/**
 * MongoLake Find Cursors
 *
 * FindCursor, StreamingFindCursor, and TimeTravelFindCursor implementations.
 */

import type {
  Document,
  WithId,
  Filter,
  FindOptions,
} from '@types';
import {
  Cursor,
  StreamingCursor,
  type CursorOptions,
  type DocumentSource,
} from '@mongolake/cursor/index.js';
import type { Collection } from './collection.js';
import type { TimeTravelCollection } from './time-travel.js';

// ============================================================================
// FindCursor
// ============================================================================

/**
 * FindCursor - Cursor for find() operations.
 *
 * Extends the base Cursor class with MongoDB-compatible API.
 * Supports batching, async iteration, and chainable modifiers.
 *
 * @example
 * ```typescript
 * const cursor = collection.find({ status: 'active' });
 *
 * // Chain modifiers
 * cursor.sort({ name: 1 }).limit(10).skip(5);
 *
 * // Iterate with for-await
 * for await (const doc of cursor) {
 *   console.log(doc);
 * }
 *
 * // Or use cursor methods
 * while (await cursor.hasNext()) {
 *   const doc = await cursor.next();
 * }
 *
 * // Or collect all at once
 * const docs = await cursor.toArray();
 * ```
 */
export class FindCursor<T extends Document = Document> extends Cursor<T> {
  private _collection: Collection<T>;

  constructor(
    collection: Collection<T>,
    filter?: Filter<T>,
    options?: FindOptions
  ) {
    // Create a document source adapter for the collection
    const source: DocumentSource<T> = {
      readDocuments: (f, o) => collection.readDocuments(f, o),
    };

    // Build namespace from collection
    const namespace = `${collection.name}`;

    // Convert FindOptions to CursorOptions (including corruption handling and cache options)
    const cursorOptions: CursorOptions = {
      limit: options?.limit,
      skip: options?.skip,
      sort: options?.sort,
      projection: options?.projection,
      skipCorruptedFiles: options?.skipCorruptedFiles,
      onCorruptedFile: options?.onCorruptedFile,
      useCache: options?.useCache,
      noCache: options?.noCache,
    };

    super(source, namespace, filter, cursorOptions);
    this._collection = collection;
  }

  /**
   * Get the collection this cursor is operating on.
   */
  get collection(): Collection<T> {
    return this._collection;
  }
}

// ============================================================================
// StreamingFindCursor
// ============================================================================

/**
 * StreamingFindCursor - Memory-efficient cursor for large result sets.
 *
 * Uses the StreamingCursor internally with the collection's streaming API
 * to avoid loading entire collections into memory.
 *
 * @example
 * ```typescript
 * // Use streaming for large collections
 * const cursor = collection.findStream({ status: 'active' });
 *
 * // Process documents one at a time without memory pressure
 * for await (const doc of cursor) {
 *   await processDocument(doc);
 * }
 *
 * // Or process in batches for better throughput
 * for await (const batch of cursor.batches()) {
 *   await Promise.all(batch.map(processDocument));
 * }
 * ```
 */
export class StreamingFindCursor<T extends Document = Document> extends StreamingCursor<T> {
  private _collection: Collection<T>;

  constructor(
    collection: Collection<T>,
    filter?: Filter<T>,
    options?: FindOptions
  ) {
    // Create a document source adapter that supports streaming
    const source: DocumentSource<T> = {
      readDocuments: (f, o) => collection.readDocuments(f, o),
      readDocumentsStream: (f, o) => collection.readDocumentsStream(f, o),
    };

    // Build namespace from collection
    const namespace = `${collection.name}`;

    // Convert FindOptions to CursorOptions (including cache options)
    const cursorOptions: CursorOptions = {
      limit: options?.limit,
      skip: options?.skip,
      sort: options?.sort,
      projection: options?.projection,
      batchSize: (options as FindOptions & { batchSize?: number })?.batchSize,
      skipCorruptedFiles: options?.skipCorruptedFiles,
      onCorruptedFile: options?.onCorruptedFile,
      useCache: options?.useCache,
      noCache: options?.noCache,
    };

    super(source, namespace, filter, cursorOptions);
    this._collection = collection;
  }

  /**
   * Get the collection this cursor is operating on.
   */
  get collection(): Collection<T> {
    return this._collection;
  }
}

// ============================================================================
// TimeTravelFindCursor
// ============================================================================

export class TimeTravelFindCursor<T extends Document = Document> {
  private _filter?: Filter<T>;
  private _options: FindOptions;
  private _executed: boolean = false;
  private _results: WithId<T>[] = [];

  constructor(
    private collection: TimeTravelCollection<T>,
    filter?: Filter<T>,
    options?: FindOptions
  ) {
    this._filter = filter;
    this._options = { ...options };
  }

  /**
   * Set sort order
   */
  sort(spec: { [key: string]: 1 | -1 }): this {
    this._options.sort = spec;
    return this;
  }

  /**
   * Limit results
   */
  limit(n: number): this {
    this._options.limit = n;
    return this;
  }

  /**
   * Skip results
   */
  skip(n: number): this {
    this._options.skip = n;
    return this;
  }

  /**
   * Set projection
   */
  project(spec: { [key: string]: 0 | 1 }): this {
    this._options.projection = spec;
    return this;
  }

  /**
   * Execute and return all results
   */
  async toArray(): Promise<WithId<T>[]> {
    if (!this._executed) {
      this._results = await this.collection.readDocuments(this._filter, this._options);
      this._executed = true;
    }
    return this._results;
  }

  /**
   * Execute and iterate
   */
  async *[Symbol.asyncIterator](): AsyncIterableIterator<WithId<T>> {
    const results = await this.toArray();
    for (const doc of results) {
      yield doc;
    }
  }

  /**
   * Execute and call function for each document
   */
  async forEach(fn: (doc: WithId<T>) => void): Promise<void> {
    const results = await this.toArray();
    for (const doc of results) {
      fn(doc);
    }
  }

  /**
   * Map results
   */
  async map<R>(fn: (doc: WithId<T>) => R): Promise<R[]> {
    const results = await this.toArray();
    return results.map(fn);
  }

  /**
   * Check if cursor has more results
   */
  async hasNext(): Promise<boolean> {
    const results = await this.toArray();
    return results.length > 0;
  }

  /**
   * Get next document
   */
  async next(): Promise<WithId<T> | null> {
    const results = await this.toArray();
    return results.shift() || null;
  }
}
