/**
 * Change Stream Service
 *
 * Encapsulates change stream functionality for MongoDB collections.
 * Manages the lifecycle of change streams and notification of events.
 */

import type { Document, WithId, AggregationStage } from '@types';
import {
  ChangeStream,
  computeUpdateDescription,
  type ChangeStreamOptions,
  type OperationType,
} from '@mongolake/change-stream/index.js';

/**
 * Namespace identifier for change streams.
 */
export interface ChangeStreamNamespace {
  /** Database name */
  db: string;
  /** Collection name */
  coll: string;
}

/**
 * Service for managing change streams on a collection.
 * Handles stream creation, event notification, and lifecycle management.
 */
export class ChangeStreamService<T extends Document = Document> {
  private changeStreams: Set<ChangeStream<T>> = new Set();

  constructor(private readonly namespace: ChangeStreamNamespace) {}

  /**
   * Create and watch for changes in the collection.
   *
   * @param pipeline - Aggregation pipeline stages for filtering change events
   * @param options - Change stream options
   * @returns A ChangeStream that can be iterated to receive change events
   *
   * @example
   * ```typescript
   * // Watch all changes
   * const changeStream = changeStreamService.watch();
   * for await (const event of changeStream) {
   *   console.log('Change:', event.operationType, event.documentKey);
   * }
   *
   * // Filter for insert events only
   * const insertStream = changeStreamService.watch([
   *   { $match: { operationType: 'insert' } }
   * ]);
   *
   * // Get full document on updates
   * const updateStream = changeStreamService.watch([], {
   *   fullDocument: 'updateLookup'
   * });
   * ```
   */
  watch(
    pipeline: AggregationStage[] = [],
    options: ChangeStreamOptions = {}
  ): ChangeStream<T> {
    const changeStream = new ChangeStream<T>(this.namespace, pipeline, options);
    this.changeStreams.add(changeStream);
    return changeStream;
  }

  /**
   * Notify all active change streams of an event.
   *
   * @param operationType - Type of operation (insert, update, delete, replace)
   * @param documentKey - Document identifier
   * @param fullDocument - The full document after the operation (if applicable)
   * @param oldDocument - The document before the operation (for updates/deletes)
   */
  notifyChangeStreams(
    operationType: OperationType,
    documentKey: { _id: string },
    fullDocument?: WithId<T>,
    oldDocument?: WithId<T>
  ): void {
    // Clean up closed streams
    for (const stream of this.changeStreams) {
      if (stream.isClosed) {
        this.changeStreams.delete(stream);
        continue;
      }

      if (operationType === 'update' && oldDocument && fullDocument) {
        const updateDescription = computeUpdateDescription(oldDocument, fullDocument);
        stream.pushEvent(operationType, documentKey, fullDocument, updateDescription, oldDocument);
      } else {
        stream.pushEvent(operationType, documentKey, fullDocument);
      }
    }
  }

  /**
   * Get the count of active change streams.
   */
  get activeStreamCount(): number {
    // Clean up closed streams before counting
    for (const stream of this.changeStreams) {
      if (stream.isClosed) {
        this.changeStreams.delete(stream);
      }
    }
    return this.changeStreams.size;
  }

  /**
   * Close all active change streams.
   */
  closeAll(): void {
    for (const stream of this.changeStreams) {
      stream.close();
    }
    this.changeStreams.clear();
  }
}
