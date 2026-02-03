/**
 * MongoLake Change Streams
 *
 * MongoDB-compatible change streams for real-time updates.
 * Enables applications to subscribe to data changes in collections.
 */

import type { Document, WithId, Filter, AggregationStage } from '@types';
import { matchesFilter } from '@utils/filter.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Operation types for change events
 */
export type OperationType = 'insert' | 'update' | 'delete' | 'replace';

/**
 * Resume token for resuming change streams
 */
export interface ResumeToken {
  /** Sequence number */
  _data: string;
  /** Timestamp of the operation */
  clusterTime: Date;
}

/**
 * Update description for update events
 */
export interface UpdateDescription {
  /** Fields that were updated with their new values */
  updatedFields: Record<string, unknown>;
  /** Fields that were removed */
  removedFields: string[];
  /** Truncated arrays (for array updates) */
  truncatedArrays?: Array<{ field: string; newSize: number }>;
}

/**
 * Namespace identifier
 */
export interface ChangeStreamNamespace {
  /** Database name */
  db: string;
  /** Collection name */
  coll: string;
}

/**
 * Change event document
 */
export interface ChangeStreamDocument<T extends Document = Document> {
  /** Unique identifier for the event */
  _id: ResumeToken;
  /** Type of operation */
  operationType: OperationType;
  /** Full document (available for insert, replace, and optionally update) */
  fullDocument?: WithId<T>;
  /** Document before the change (if configured) */
  fullDocumentBeforeChange?: WithId<T>;
  /** Namespace (database and collection) */
  ns: ChangeStreamNamespace;
  /** Document key (contains _id) */
  documentKey: { _id: string };
  /** Update description (for update operations) */
  updateDescription?: UpdateDescription;
  /** Cluster time of the operation */
  clusterTime: Date;
  /** Wall clock time when the event was recorded */
  wallTime: Date;
}

/**
 * Options for watch() method
 */
export interface ChangeStreamOptions {
  /**
   * Specifies whether to return the full document for update operations.
   * - 'default': Does not return the full document for update operations
   * - 'updateLookup': Returns the most current majority-committed version of the updated document
   * - 'whenAvailable': Returns the post-image if available
   * - 'required': Returns the post-image; error if not available
   */
  fullDocument?: 'default' | 'updateLookup' | 'whenAvailable' | 'required';

  /**
   * Specifies whether to return the document before the change.
   * - 'off': Does not return the pre-image
   * - 'whenAvailable': Returns the pre-image if available
   * - 'required': Returns the pre-image; error if not available
   */
  fullDocumentBeforeChange?: 'off' | 'whenAvailable' | 'required';

  /**
   * Resume the change stream from a specific resume token.
   * Mutually exclusive with startAtOperationTime.
   */
  resumeAfter?: ResumeToken;

  /**
   * Start the change stream from a specific operation time.
   * Mutually exclusive with resumeAfter.
   */
  startAtOperationTime?: Date;

  /**
   * Maximum number of events to buffer before the iterator is read.
   * @default 1000
   */
  maxAwaitTimeMS?: number;

  /**
   * Batch size for fetching events.
   * @default 100
   */
  batchSize?: number;
}

/**
 * Callback type for change event handlers
 */
export type ChangeEventHandler<T extends Document = Document> = (
  event: ChangeStreamDocument<T>
) => void | Promise<void>;

// ============================================================================
// ChangeStream Class
// ============================================================================

/**
 * Change Stream for monitoring data changes in a collection.
 *
 * Implements AsyncIterable to support for-await-of loops.
 *
 * @example
 * ```typescript
 * const changeStream = collection.watch();
 *
 * for await (const event of changeStream) {
 *   console.log('Change:', event.operationType, event.documentKey);
 * }
 * ```
 */
export class ChangeStream<T extends Document = Document> implements AsyncIterable<ChangeStreamDocument<T>> {
  private closed = false;
  private eventBuffer: ChangeStreamDocument<T>[] = [];
  private eventWaiters: Array<{
    resolve: (value: ChangeStreamDocument<T> | null) => void;
    reject: (error: Error) => void;
  }> = [];
  private lastResumeToken: ResumeToken | null = null;
  private sequenceNumber = 0;
  private matchFilter: Filter<Document> | null = null;

  /**
   * Create a new ChangeStream
   * @param namespace - Database and collection namespace
   * @param pipeline - Aggregation pipeline stages for filtering
   * @param options - Change stream options
   */
  constructor(
    private namespace: ChangeStreamNamespace,
    pipeline: AggregationStage[] = [],
    private options: ChangeStreamOptions = {}
  ) {
    // Set sequence number from resumeAfter token if provided
    if (options.resumeAfter) {
      this.sequenceNumber = parseInt(options.resumeAfter._data, 10) || 0;
    }

    // Extract $match stage from pipeline for filtering
    for (const stage of pipeline) {
      if ('$match' in stage) {
        this.matchFilter = stage.$match as Filter<Document>;
        break;
      }
    }
  }

  /**
   * Check if the change stream is closed
   */
  get isClosed(): boolean {
    return this.closed;
  }

  /**
   * Get the current resume token
   */
  get resumeToken(): ResumeToken | null {
    return this.lastResumeToken;
  }

  /**
   * Close the change stream
   */
  close(): void {
    this.closed = true;
    // Resolve all waiting promises with null to signal closure
    for (const waiter of this.eventWaiters) {
      waiter.resolve(null);
    }
    this.eventWaiters = [];
  }

  /**
   * Check if an event matches the configured filter
   */
  private matchesEvent(event: ChangeStreamDocument<T>): boolean {
    if (!this.matchFilter) {
      return true;
    }

    // For the $match stage, we match against the change event document
    // Common patterns:
    // - { operationType: 'insert' } - filter by operation type
    // - { 'fullDocument.status': 'active' } - filter by document fields
    // - { 'ns.coll': 'users' } - filter by namespace
    const eventDoc: Record<string, unknown> = {
      operationType: event.operationType,
      ns: event.ns,
      documentKey: event.documentKey,
      fullDocument: event.fullDocument,
      updateDescription: event.updateDescription,
    };

    return matchesFilter(eventDoc as Document, this.matchFilter);
  }

  /**
   * Generate a resume token for an event
   */
  private generateResumeToken(): ResumeToken {
    this.sequenceNumber++;
    return {
      _data: String(this.sequenceNumber),
      clusterTime: new Date(),
    };
  }

  /**
   * Push a change event to the stream (called by collection when changes occur)
   * @internal
   */
  pushEvent(
    operationType: OperationType,
    documentKey: { _id: string },
    fullDocument?: WithId<T>,
    updateDescription?: UpdateDescription,
    fullDocumentBeforeChange?: WithId<T>
  ): void {
    if (this.closed) {
      return;
    }

    const resumeToken = this.generateResumeToken();
    const now = new Date();

    const event: ChangeStreamDocument<T> = {
      _id: resumeToken,
      operationType,
      ns: this.namespace,
      documentKey,
      clusterTime: now,
      wallTime: now,
    };

    // Add fullDocument based on operation type and options
    if (operationType === 'insert' || operationType === 'replace') {
      event.fullDocument = fullDocument;
    } else if (operationType === 'update') {
      // For updates, only include fullDocument if configured
      if (
        this.options.fullDocument === 'updateLookup' ||
        this.options.fullDocument === 'whenAvailable' ||
        this.options.fullDocument === 'required'
      ) {
        event.fullDocument = fullDocument;
      }
      event.updateDescription = updateDescription;
    }

    // Add fullDocumentBeforeChange if configured and available
    if (
      fullDocumentBeforeChange &&
      (this.options.fullDocumentBeforeChange === 'whenAvailable' ||
        this.options.fullDocumentBeforeChange === 'required')
    ) {
      event.fullDocumentBeforeChange = fullDocumentBeforeChange;
    }

    // Check if event matches the filter
    if (!this.matchesEvent(event)) {
      return;
    }

    this.lastResumeToken = resumeToken;

    // If there are waiters, resolve the first one
    if (this.eventWaiters.length > 0) {
      const waiter = this.eventWaiters.shift()!;
      waiter.resolve(event);
    } else {
      // Otherwise, buffer the event
      this.eventBuffer.push(event);
    }
  }

  /**
   * Get the next change event
   * @returns The next change event or null if closed
   */
  async next(): Promise<ChangeStreamDocument<T> | null> {
    if (this.closed) {
      return null;
    }

    // If there are buffered events, return the first one
    if (this.eventBuffer.length > 0) {
      return this.eventBuffer.shift()!;
    }

    // Wait for the next event
    return new Promise<ChangeStreamDocument<T> | null>((resolve, reject) => {
      this.eventWaiters.push({ resolve, reject });
    });
  }

  /**
   * Check if there are events available without blocking
   */
  hasNext(): boolean {
    return this.eventBuffer.length > 0;
  }

  /**
   * Try to get the next event without blocking
   * @returns The next event if available, undefined otherwise
   */
  tryNext(): ChangeStreamDocument<T> | undefined {
    if (this.eventBuffer.length > 0) {
      return this.eventBuffer.shift();
    }
    return undefined;
  }

  /**
   * Register an event handler
   * @param event - Event type ('change', 'close', 'error')
   * @param handler - Event handler function
   */
  on(event: 'change', handler: ChangeEventHandler<T>): this;
  on(event: 'close', handler: () => void): this;
  on(event: 'error', handler: (error: Error) => void): this;
  on(
    event: 'change' | 'close' | 'error',
    handler: ChangeEventHandler<T> | (() => void) | ((error: Error) => void)
  ): this {
    if (event === 'change') {
      // Start polling for changes
      this.startPolling(handler as ChangeEventHandler<T>);
    }
    return this;
  }

  /**
   * Start polling for changes and calling the handler
   */
  private async startPolling(handler: ChangeEventHandler<T>): Promise<void> {
    while (!this.closed) {
      const event = await this.next();
      if (event === null) {
        break;
      }
      await handler(event);
    }
  }

  /**
   * Async iterator implementation
   */
  async *[Symbol.asyncIterator](): AsyncIterableIterator<ChangeStreamDocument<T>> {
    while (!this.closed) {
      const event = await this.next();
      if (event === null) {
        break;
      }
      yield event;
    }
  }

  /**
   * Convert to array (useful for collecting a limited number of events)
   * @param limit - Maximum number of events to collect
   */
  async toArray(limit?: number): Promise<ChangeStreamDocument<T>[]> {
    const results: ChangeStreamDocument<T>[] = [];
    let count = 0;

    for await (const event of this) {
      results.push(event);
      count++;
      if (limit !== undefined && count >= limit) {
        break;
      }
    }

    return results;
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Compute the update description from old and new documents
 * @param oldDoc - The document before the update
 * @param newDoc - The document after the update
 * @returns Update description with updatedFields and removedFields
 */
export function computeUpdateDescription<T extends Document>(
  oldDoc: WithId<T>,
  newDoc: WithId<T>
): UpdateDescription {
  const updatedFields: Record<string, unknown> = {};
  const removedFields: string[] = [];

  // Find updated and new fields
  for (const key of Object.keys(newDoc)) {
    if (key === '_id') continue;

    const oldValue = (oldDoc as Record<string, unknown>)[key];
    const newValue = (newDoc as Record<string, unknown>)[key];

    if (oldValue === undefined) {
      // New field
      updatedFields[key] = newValue;
    } else if (JSON.stringify(oldValue) !== JSON.stringify(newValue)) {
      // Changed field
      updatedFields[key] = newValue;
    }
  }

  // Find removed fields
  for (const key of Object.keys(oldDoc)) {
    if (key === '_id') continue;

    if ((newDoc as Record<string, unknown>)[key] === undefined) {
      removedFields.push(key);
    }
  }

  return { updatedFields, removedFields };
}

/**
 * Create a change stream for a collection
 * @param namespace - Database and collection namespace
 * @param pipeline - Aggregation pipeline stages for filtering
 * @param options - Change stream options
 * @returns A new ChangeStream instance
 */
export function createChangeStream<T extends Document = Document>(
  namespace: ChangeStreamNamespace,
  pipeline: AggregationStage[] = [],
  options: ChangeStreamOptions = {}
): ChangeStream<T> {
  return new ChangeStream<T>(namespace, pipeline, options);
}

// ============================================================================
// Exports
// ============================================================================

export type {
  Document,
  WithId,
  Filter,
  AggregationStage,
} from '../types.js';
