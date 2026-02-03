/**
 * MongoLake Cursor
 *
 * MongoDB-compatible cursor implementation for iterating over large result sets.
 * Supports batching, async iteration, and various cursor operations.
 */

import type { Document, WithId, Filter, FindOptions, CorruptionCallback } from '@types';

// ============================================================================
// Types
// ============================================================================

/**
 * Cursor options for configuring cursor behavior.
 */
export interface CursorOptions {
  /** Batch size for fetching documents */
  batchSize?: number;
  /** Cursor timeout in milliseconds (default: 10 minutes) */
  timeoutMs?: number;
  /** Maximum number of documents to return */
  limit?: number;
  /** Number of documents to skip */
  skip?: number;
  /** Sort specification */
  sort?: { [key: string]: 1 | -1 };
  /** Projection specification */
  projection?: { [key: string]: 0 | 1 };
  /**
   * If true, skip corrupted Parquet files instead of failing.
   * Warning: This may cause data loss. Use onCorruptedFile callback
   * or check metadata.corruptedFiles to audit skipped files.
   * @default false
   */
  skipCorruptedFiles?: boolean;
  /**
   * Callback invoked when a corrupted file is skipped.
   * Only called when skipCorruptedFiles is true.
   * Use this for real-time logging or alerting on data corruption.
   */
  onCorruptedFile?: CorruptionCallback;
  /**
   * If true, use query result cache if available.
   * Cached results are returned for identical queries within the TTL.
   * Cache is automatically invalidated on writes to the collection.
   * @default true
   */
  useCache?: boolean;
  /**
   * If true, bypass the cache and always execute the query.
   * The result will still be cached for future queries.
   * @default false
   */
  noCache?: boolean;
}

/**
 * Internal cursor state.
 */
export interface CursorState<T> {
  /** Unique cursor ID */
  cursorId: bigint;
  /** Namespace (database.collection) */
  namespace: string;
  /** Current position in the result set */
  position: number;
  /** Buffered documents from the current batch */
  buffer: WithId<T>[];
  /** Whether the cursor has been exhausted */
  exhausted: boolean;
  /** Whether the cursor has been closed */
  closed: boolean;
  /** Timestamp when the cursor was created */
  createdAt: number;
  /** Timestamp of last activity (for timeout) */
  lastActivityAt: number;
  /** Total documents processed */
  totalProcessed: number;
}

/**
 * Document source interface for cursor data retrieval.
 * This abstraction allows cursors to work with different backends.
 */
export interface DocumentSource<T extends Document = Document> {
  /**
   * Fetch a batch of documents.
   * @param filter - Query filter
   * @param options - Find options including skip/limit for batching
   * @returns Array of documents
   */
  readDocuments(filter?: Filter<T>, options?: FindOptions): Promise<WithId<T>[]>;

  /**
   * Stream documents in batches for memory-efficient processing.
   * @param filter - Query filter
   * @param options - Find options including batchSize for controlling batch yield size
   * @returns AsyncGenerator yielding batches of documents
   */
  readDocumentsStream?(
    filter?: Filter<T>,
    options?: FindOptions & { batchSize?: number }
  ): AsyncGenerator<WithId<T>[], void, undefined>;
}

// ============================================================================
// Cursor ID Generation
// ============================================================================

/**
 * Generate a unique cursor ID using cryptographically secure random values.
 * Format: 32 bits timestamp (seconds) + 32 bits random
 *
 * This approach avoids race conditions from non-atomic counter increments
 * by using crypto.getRandomValues() for the random portion. The 32-bit
 * random component provides sufficient entropy to avoid birthday paradox
 * collisions even with high-volume cursor creation.
 */
export function generateCursorId(): bigint {
  // Use seconds instead of milliseconds for the timestamp (fits in 32 bits until 2106)
  const timestamp = BigInt(Math.floor(Date.now() / 1000)) << 32n;
  const randomBytes = new Uint32Array(1);
  crypto.getRandomValues(randomBytes);
  // Safe access: Uint32Array(1) guarantees index 0 exists, default to 0 satisfies type checker
  const random = BigInt(randomBytes[0] ?? 0);
  return timestamp | random;
}

// ============================================================================
// Cursor Class
// ============================================================================

/**
 * Cursor for iterating over query results.
 *
 * Cursors provide lazy iteration over large result sets with:
 * - Batch fetching for memory efficiency
 * - Async iterator support
 * - Chainable cursor modifiers (limit, skip, sort, etc.)
 * - Automatic timeout and cleanup
 *
 * @example
 * ```typescript
 * const cursor = collection.find({ status: 'active' });
 *
 * // Use async iteration
 * for await (const doc of cursor) {
 *   console.log(doc);
 * }
 *
 * // Or use cursor methods
 * while (await cursor.hasNext()) {
 *   const doc = await cursor.next();
 *   console.log(doc);
 * }
 *
 * // Or collect all results
 * const docs = await cursor.toArray();
 * ```
 */
export class Cursor<T extends Document = Document> implements AsyncIterable<WithId<T>> {
  private source: DocumentSource<T>;
  private filter?: Filter<T>;
  private options: CursorOptions;
  private state: CursorState<T>;

  // Cursor modifiers (applied lazily)
  private _limit?: number;
  private _skip?: number;
  private _sort?: { [key: string]: 1 | -1 };
  private _projection?: { [key: string]: 0 | 1 };
  private _batchSize: number;

  // Execution state
  private executed = false;
  private allResults: WithId<T>[] | null = null;

  /**
   * Get the executed results, asserting the query has been executed.
   * Call this after ensureExecuted() to get properly typed access to allResults.
   * @throws Error if query has not been executed
   */
  private getResults(): WithId<T>[] {
    if (this.allResults === null) {
      throw new Error('Cursor: query not executed. Call ensureExecuted() first.');
    }
    return this.allResults;
  }

  constructor(
    source: DocumentSource<T>,
    namespace: string,
    filter?: Filter<T>,
    options: CursorOptions = {}
  ) {
    this.source = source;
    this.filter = filter;
    this.options = options;

    // Initialize cursor state
    this.state = {
      cursorId: generateCursorId(),
      namespace,
      position: 0,
      buffer: [],
      exhausted: false,
      closed: false,
      createdAt: Date.now(),
      lastActivityAt: Date.now(),
      totalProcessed: 0,
    };

    // Apply initial options
    this._batchSize = options.batchSize ?? 101;
    this._limit = options.limit;
    this._skip = options.skip;
    this._sort = options.sort;
    this._projection = options.projection;
  }

  // ==========================================================================
  // Cursor Properties
  // ==========================================================================

  /**
   * Get the cursor ID.
   */
  get cursorId(): bigint {
    return this.state.cursorId;
  }

  /**
   * Get the namespace (database.collection).
   */
  get namespace(): string {
    return this.state.namespace;
  }

  /**
   * Check if the cursor is closed.
   */
  get isClosed(): boolean {
    return this.state.closed;
  }

  /**
   * Check if the cursor is exhausted (no more documents).
   */
  get isExhausted(): boolean {
    return this.state.exhausted;
  }

  // ==========================================================================
  // Cursor Modifiers (Chainable)
  // ==========================================================================

  /**
   * Set the maximum number of documents to return.
   *
   * @param n - Maximum number of documents
   * @returns This cursor for chaining
   * @throws Error if cursor has already been executed
   */
  limit(n: number): this {
    this.ensureNotExecuted('limit');
    this._limit = n;
    return this;
  }

  /**
   * Set the number of documents to skip.
   *
   * @param n - Number of documents to skip
   * @returns This cursor for chaining
   * @throws Error if cursor has already been executed
   */
  skip(n: number): this {
    this.ensureNotExecuted('skip');
    this._skip = n;
    return this;
  }

  /**
   * Set the sort order.
   *
   * @param spec - Sort specification (e.g., { name: 1, age: -1 })
   * @returns This cursor for chaining
   * @throws Error if cursor has already been executed
   */
  sort(spec: { [key: string]: 1 | -1 }): this {
    this.ensureNotExecuted('sort');
    this._sort = spec;
    return this;
  }

  /**
   * Set the projection (fields to include/exclude).
   *
   * @param spec - Projection specification
   * @returns This cursor for chaining
   * @throws Error if cursor has already been executed
   */
  project(spec: { [key: string]: 0 | 1 }): this {
    this.ensureNotExecuted('project');
    this._projection = spec;
    return this;
  }

  /**
   * Set the batch size for fetching documents.
   *
   * @param n - Number of documents per batch
   * @returns This cursor for chaining
   */
  batchSize(n: number): this {
    if (n < 1) {
      throw new Error('Batch size must be at least 1');
    }
    this._batchSize = n;
    return this;
  }

  // ==========================================================================
  // Iteration Methods
  // ==========================================================================

  /**
   * Check if there are more documents to iterate.
   *
   * @returns True if there are more documents
   */
  async hasNext(): Promise<boolean> {
    if (this.state.closed) {
      return false;
    }

    await this.ensureExecuted();
    const results = this.getResults();
    this.updateActivity();

    // Check if we have buffered documents
    if (this.state.position < results.length) {
      return true;
    }

    // Check if limit has been reached
    if (this._limit !== undefined && this.state.totalProcessed >= this._limit) {
      this.state.exhausted = true;
      return false;
    }

    this.state.exhausted = true;
    return false;
  }

  /**
   * Get the next document.
   *
   * @returns The next document, or null if no more documents
   */
  async next(): Promise<WithId<T> | null> {
    if (this.state.closed) {
      return null;
    }

    await this.ensureExecuted();
    const results = this.getResults();
    this.updateActivity();

    // Check if we have more documents
    if (this.state.position >= results.length) {
      this.state.exhausted = true;
      return null;
    }

    // Check limit
    if (this._limit !== undefined && this.state.totalProcessed >= this._limit) {
      this.state.exhausted = true;
      return null;
    }

    // Get next document
    const doc = results[this.state.position++];
    this.state.totalProcessed++;
    return doc ?? null;
  }

  /**
   * Get all remaining documents as an array.
   *
   * @returns Array of all remaining documents
   */
  async toArray(): Promise<WithId<T>[]> {
    if (this.state.closed) {
      return [];
    }

    await this.ensureExecuted();
    const results = this.getResults();
    this.updateActivity();

    // Get remaining documents
    const remaining = results.slice(this.state.position);

    // Apply limit if needed
    const result = this._limit !== undefined
      ? remaining.slice(0, this._limit - this.state.totalProcessed)
      : remaining;

    // Update state
    this.state.position = results.length;
    this.state.totalProcessed += result.length;
    this.state.exhausted = true;

    return result;
  }

  /**
   * Execute a callback for each document.
   *
   * @param callback - Function to call for each document
   */
  async forEach(callback: (doc: WithId<T>) => void | Promise<void>): Promise<void> {
    for await (const doc of this) {
      await callback(doc);
    }
  }

  /**
   * Map documents to a new array.
   *
   * @param fn - Mapping function
   * @returns Array of mapped values
   */
  async map<R>(fn: (doc: WithId<T>) => R): Promise<R[]> {
    const results: R[] = [];
    for await (const doc of this) {
      results.push(fn(doc));
    }
    return results;
  }

  /**
   * Count the number of documents matching the filter.
   * Note: This exhausts the cursor.
   *
   * @returns Number of documents
   */
  async count(): Promise<number> {
    const docs = await this.toArray();
    return docs.length;
  }

  // ==========================================================================
  // Async Iterator Implementation
  // ==========================================================================

  /**
   * Async iterator implementation for for-await-of loops.
   */
  async *[Symbol.asyncIterator](): AsyncIterableIterator<WithId<T>> {
    while (await this.hasNext()) {
      const doc = await this.next();
      if (doc !== null) {
        yield doc;
      }
    }
  }

  // ==========================================================================
  // Cursor Lifecycle
  // ==========================================================================

  /**
   * Close the cursor and release resources.
   */
  async close(): Promise<void> {
    this.state.closed = true;
    this.state.buffer = [];
    this.allResults = null;
  }

  /**
   * Rewind the cursor to the beginning.
   * Note: This re-executes the query.
   */
  async rewind(): Promise<void> {
    this.state.position = 0;
    this.state.buffer = [];
    this.state.exhausted = false;
    this.state.totalProcessed = 0;
    this.executed = false;
    this.allResults = null;
    this.updateActivity();
  }

  // ==========================================================================
  // Batch Operations (for wire protocol)
  // ==========================================================================

  /**
   * Get the first batch of documents.
   * Used by wire protocol for initial find response.
   *
   * @param size - Maximum batch size (optional, uses cursor batchSize if not specified)
   * @returns First batch of documents
   */
  async getFirstBatch(size?: number): Promise<WithId<T>[]> {
    await this.ensureExecuted();
    const results = this.getResults();
    const batchSize = size ?? this._batchSize;

    // Apply limit if smaller than batch size
    const effectiveSize = this._limit !== undefined
      ? Math.min(batchSize, this._limit)
      : batchSize;

    const batch = results.slice(0, effectiveSize);
    this.state.position = batch.length;
    this.state.totalProcessed = batch.length;

    // Check if cursor is exhausted
    if (this.state.position >= results.length) {
      this.state.exhausted = true;
    }

    return batch;
  }

  /**
   * Get the next batch of documents.
   * Used by wire protocol for getMore responses.
   *
   * @param size - Maximum batch size (optional, uses cursor batchSize if not specified)
   * @returns Next batch of documents
   */
  async getNextBatch(size?: number): Promise<WithId<T>[]> {
    if (this.state.closed || this.state.exhausted) {
      return [];
    }

    await this.ensureExecuted();
    const results = this.getResults();
    this.updateActivity();

    const batchSize = size ?? this._batchSize;
    const startPos = this.state.position;

    // Calculate end position considering limit
    let endPos = startPos + batchSize;
    if (this._limit !== undefined) {
      endPos = Math.min(endPos, this._limit);
    }
    endPos = Math.min(endPos, results.length);

    const batch = results.slice(startPos, endPos);
    this.state.position = endPos;
    this.state.totalProcessed += batch.length;

    // Check if cursor is exhausted
    if (this.state.position >= results.length ||
        (this._limit !== undefined && this.state.totalProcessed >= this._limit)) {
      this.state.exhausted = true;
    }

    return batch;
  }

  // ==========================================================================
  // Internal Methods
  // ==========================================================================

  /**
   * Ensure the cursor query has been executed.
   */
  private async ensureExecuted(): Promise<void> {
    if (this.executed) {
      return;
    }

    // Build find options, including corruption handling and cache options
    const findOptions: FindOptions = {
      skip: this._skip,
      limit: this._limit,
      sort: this._sort,
      projection: this._projection,
      skipCorruptedFiles: this.options.skipCorruptedFiles,
      onCorruptedFile: this.options.onCorruptedFile,
      useCache: this.options.useCache,
      noCache: this.options.noCache,
    };

    // Execute the query
    this.allResults = await this.source.readDocuments(this.filter, findOptions);
    this.executed = true;
  }

  /**
   * Ensure cursor has not been executed (for modifiers).
   */
  private ensureNotExecuted(operation: string): void {
    if (this.executed) {
      throw new Error(`Cannot call ${operation}() after cursor has been executed`);
    }
  }

  /**
   * Update last activity timestamp.
   */
  private updateActivity(): void {
    this.state.lastActivityAt = Date.now();
  }

  /**
   * Check if cursor has timed out.
   */
  isTimedOut(timeoutMs?: number): boolean {
    const timeout = timeoutMs ?? this.options.timeoutMs ?? 600000; // 10 minutes default
    return Date.now() - this.state.lastActivityAt > timeout;
  }
}

// ============================================================================
// Streaming Cursor Class
// ============================================================================

/**
 * StreamingCursor for memory-efficient iteration over large result sets.
 *
 * Unlike the standard Cursor which loads all results into memory before iteration,
 * StreamingCursor uses an AsyncGenerator to process documents in batches,
 * significantly reducing memory usage for large collections.
 *
 * Key differences from standard Cursor:
 * - Documents are fetched and processed in batches via AsyncGenerator
 * - Memory usage is O(batchSize) instead of O(total_documents)
 * - Some operations like rewind() require re-executing the query
 * - toArray() collects all documents (defeats the purpose of streaming)
 *
 * @example
 * ```typescript
 * // Process large collection without loading all into memory
 * const cursor = new StreamingCursor(source, 'test.collection', {}, { batchSize: 1000 });
 *
 * for await (const doc of cursor) {
 *   await processDocument(doc);
 * }
 *
 * // Or process in batches
 * for await (const batch of cursor.batches()) {
 *   await processBatch(batch);
 * }
 * ```
 */
export class StreamingCursor<T extends Document = Document> implements AsyncIterable<WithId<T>> {
  private source: DocumentSource<T>;
  private filter?: Filter<T>;
  private options: CursorOptions;
  private state: CursorState<T>;

  // Cursor modifiers (applied lazily)
  private _limit?: number;
  private _skip?: number;
  private _sort?: { [key: string]: 1 | -1 };
  private _projection?: { [key: string]: 0 | 1 };
  private _batchSize: number;

  // Streaming state
  private streamGenerator: AsyncGenerator<WithId<T>[], void, undefined> | null = null;
  private currentBatch: WithId<T>[] = [];
  private batchIndex = 0;
  private started = false;

  constructor(
    source: DocumentSource<T>,
    namespace: string,
    filter?: Filter<T>,
    options: CursorOptions = {}
  ) {
    this.source = source;
    this.filter = filter;
    this.options = options;

    // Initialize cursor state
    this.state = {
      cursorId: generateCursorId(),
      namespace,
      position: 0,
      buffer: [],
      exhausted: false,
      closed: false,
      createdAt: Date.now(),
      lastActivityAt: Date.now(),
      totalProcessed: 0,
    };

    // Apply initial options
    this._batchSize = options.batchSize ?? 1000;
    this._limit = options.limit;
    this._skip = options.skip;
    this._sort = options.sort;
    this._projection = options.projection;
  }

  // ==========================================================================
  // Cursor Properties
  // ==========================================================================

  get cursorId(): bigint {
    return this.state.cursorId;
  }

  get namespace(): string {
    return this.state.namespace;
  }

  get isClosed(): boolean {
    return this.state.closed;
  }

  get isExhausted(): boolean {
    return this.state.exhausted;
  }

  // ==========================================================================
  // Cursor Modifiers (Chainable)
  // ==========================================================================

  limit(n: number): this {
    this.ensureNotStarted('limit');
    this._limit = n;
    return this;
  }

  skip(n: number): this {
    this.ensureNotStarted('skip');
    this._skip = n;
    return this;
  }

  sort(spec: { [key: string]: 1 | -1 }): this {
    this.ensureNotStarted('sort');
    this._sort = spec;
    return this;
  }

  project(spec: { [key: string]: 0 | 1 }): this {
    this.ensureNotStarted('project');
    this._projection = spec;
    return this;
  }

  batchSize(n: number): this {
    if (n < 1) {
      throw new Error('Batch size must be at least 1');
    }
    this._batchSize = n;
    return this;
  }

  // ==========================================================================
  // Streaming Methods
  // ==========================================================================

  /**
   * Get an async iterator over batches of documents.
   * This is the most memory-efficient way to process large result sets.
   */
  async *batches(): AsyncGenerator<WithId<T>[], void, undefined> {
    if (this.state.closed) return;

    await this.ensureStreamStarted();

    if (!this.streamGenerator) {
      // Fallback to non-streaming if source doesn't support streaming
      return;
    }

    let yielded = 0;
    const limit = this._limit ?? Infinity;

    for await (const batch of this.streamGenerator) {
      if (yielded >= limit) break;

      // Trim batch if it would exceed limit
      const remaining = limit - yielded;
      const outputBatch = batch.length <= remaining ? batch : batch.slice(0, remaining);

      yielded += outputBatch.length;
      this.state.totalProcessed += outputBatch.length;
      this.updateActivity();

      yield outputBatch;
    }

    this.state.exhausted = true;
  }

  // ==========================================================================
  // Iteration Methods
  // ==========================================================================

  async hasNext(): Promise<boolean> {
    if (this.state.closed) return false;

    await this.ensureStreamStarted();
    this.updateActivity();

    // Check limit
    if (this._limit !== undefined && this.state.totalProcessed >= this._limit) {
      this.state.exhausted = true;
      return false;
    }

    // Check if we have documents in current batch
    if (this.batchIndex < this.currentBatch.length) {
      return true;
    }

    // Try to fetch next batch
    if (this.streamGenerator) {
      const result = await this.streamGenerator.next();
      if (result.done) {
        this.state.exhausted = true;
        return false;
      }
      this.currentBatch = result.value;
      this.batchIndex = 0;
      return this.currentBatch.length > 0;
    }

    this.state.exhausted = true;
    return false;
  }

  async next(): Promise<WithId<T> | null> {
    if (this.state.closed) return null;

    const hasMore = await this.hasNext();
    if (!hasMore) return null;

    // Check limit
    if (this._limit !== undefined && this.state.totalProcessed >= this._limit) {
      this.state.exhausted = true;
      return null;
    }

    const doc = this.currentBatch[this.batchIndex++];
    this.state.totalProcessed++;
    return doc ?? null;
  }

  /**
   * Get all remaining documents as an array.
   * Warning: This loads all documents into memory, defeating the purpose of streaming.
   * Use for-await-of or batches() for memory-efficient processing.
   */
  async toArray(): Promise<WithId<T>[]> {
    if (this.state.closed) return [];

    const results: WithId<T>[] = [];

    for await (const doc of this) {
      results.push(doc);
    }

    return results;
  }

  async forEach(callback: (doc: WithId<T>) => void | Promise<void>): Promise<void> {
    for await (const doc of this) {
      await callback(doc);
    }
  }

  async map<R>(fn: (doc: WithId<T>) => R): Promise<R[]> {
    const results: R[] = [];
    for await (const doc of this) {
      results.push(fn(doc));
    }
    return results;
  }

  async count(): Promise<number> {
    let count = 0;
    for await (const _doc of this) {
      count++;
    }
    return count;
  }

  // ==========================================================================
  // Async Iterator Implementation
  // ==========================================================================

  async *[Symbol.asyncIterator](): AsyncIterableIterator<WithId<T>> {
    while (await this.hasNext()) {
      const doc = await this.next();
      if (doc !== null) {
        yield doc;
      }
    }
  }

  // ==========================================================================
  // Cursor Lifecycle
  // ==========================================================================

  async close(): Promise<void> {
    this.state.closed = true;
    this.currentBatch = [];
    this.streamGenerator = null;
  }

  async rewind(): Promise<void> {
    this.state.position = 0;
    this.state.exhausted = false;
    this.state.totalProcessed = 0;
    this.currentBatch = [];
    this.batchIndex = 0;
    this.started = false;
    this.streamGenerator = null;
    this.updateActivity();
  }

  // ==========================================================================
  // Internal Methods
  // ==========================================================================

  private async ensureStreamStarted(): Promise<void> {
    if (this.started) return;

    this.started = true;

    // Check if source supports streaming
    if (!this.source.readDocumentsStream) {
      // Fallback: load all documents and iterate
      const findOptions: FindOptions = {
        skip: this._skip,
        limit: this._limit,
        sort: this._sort,
        projection: this._projection,
        skipCorruptedFiles: this.options.skipCorruptedFiles,
        onCorruptedFile: this.options.onCorruptedFile,
        useCache: this.options.useCache,
        noCache: this.options.noCache,
      };

      const allDocs = await this.source.readDocuments(this.filter, findOptions);
      this.streamGenerator = this.createFallbackGenerator(allDocs);
      return;
    }

    // Use streaming
    const streamOptions = {
      skip: this._skip,
      sort: this._sort,
      projection: this._projection,
      batchSize: this._batchSize,
      skipCorruptedFiles: this.options.skipCorruptedFiles,
      onCorruptedFile: this.options.onCorruptedFile,
      useCache: this.options.useCache,
      noCache: this.options.noCache,
    };

    this.streamGenerator = this.source.readDocumentsStream(this.filter, streamOptions);
  }

  private async *createFallbackGenerator(docs: WithId<T>[]): AsyncGenerator<WithId<T>[], void, undefined> {
    for (let i = 0; i < docs.length; i += this._batchSize) {
      yield docs.slice(i, i + this._batchSize);
    }
  }

  private ensureNotStarted(operation: string): void {
    if (this.started) {
      throw new Error(`Cannot call ${operation}() after cursor has started streaming`);
    }
  }

  private updateActivity(): void {
    this.state.lastActivityAt = Date.now();
  }

  isTimedOut(timeoutMs?: number): boolean {
    const timeout = timeoutMs ?? this.options.timeoutMs ?? 600000;
    return Date.now() - this.state.lastActivityAt > timeout;
  }
}

// ============================================================================
// Cursor Store (for wire protocol)
// ============================================================================

/**
 * Cursor store for managing active cursors.
 * Used by wire protocol handlers to track cursors across getMore requests.
 */
export class CursorStore {
  private cursors: Map<string, Cursor<Document>> = new Map();
  private timeoutMs: number;
  private cleanupInterval: ReturnType<typeof setInterval> | null = null;

  constructor(options: { timeoutMs?: number; cleanupIntervalMs?: number } = {}) {
    this.timeoutMs = options.timeoutMs ?? 600000; // 10 minutes default

    // Start cleanup interval
    const cleanupIntervalMs = options.cleanupIntervalMs ?? 60000; // 1 minute default
    this.cleanupInterval = setInterval(() => {
      this.cleanupExpiredCursors();
    }, cleanupIntervalMs);
  }

  /**
   * Add a cursor to the store.
   */
  add(cursor: Cursor<Document>): void {
    this.cursors.set(cursor.cursorId.toString(), cursor);
  }

  /**
   * Get a cursor by ID.
   */
  get(cursorId: bigint): Cursor<Document> | undefined {
    return this.cursors.get(cursorId.toString());
  }

  /**
   * Remove a cursor from the store.
   */
  remove(cursorId: bigint): boolean {
    const cursor = this.cursors.get(cursorId.toString());
    if (cursor) {
      cursor.close();
      return this.cursors.delete(cursorId.toString());
    }
    return false;
  }

  /**
   * Get the number of active cursors.
   */
  get size(): number {
    return this.cursors.size;
  }

  /**
   * Clean up expired cursors.
   */
  cleanupExpiredCursors(): number {
    let cleaned = 0;
    for (const [id, cursor] of this.cursors) {
      if (cursor.isTimedOut(this.timeoutMs) || cursor.isClosed || cursor.isExhausted) {
        cursor.close();
        this.cursors.delete(id);
        cleaned++;
      }
    }
    return cleaned;
  }

  /**
   * Close all cursors and stop cleanup.
   */
  close(): void {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
    }
    for (const cursor of this.cursors.values()) {
      cursor.close();
    }
    this.cursors.clear();
  }
}

// ============================================================================
// Exports (types already exported above via interfaces)
// ============================================================================
