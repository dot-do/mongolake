/**
 * MongoLake Collection
 *
 * MongoDB-compatible collection class with CRUD operations.
 * Uses composition pattern with services for index management,
 * change streams, validation, and auditing.
 */

import type {
  Document,
  WithId,
  Filter,
  Update,
  AggregationStage,
  FindOptions,
  UpdateOptions,
  DeleteOptions,
  AggregateOptions,
  InsertOneResult,
  InsertManyResult,
  UpdateResult,
  DeleteResult,
  IndexSpec,
  IndexOptions,
  ObjectId,
  CollectionManifest,
  CollectionSchema,
  QueryMetadata,
  FilterQuery,
  DocumentFields,
} from '@types';
import type { StorageBackend } from '@storage/index.js';
import { writeParquet, readParquet } from '@parquet/io.js';
import { sortDocuments } from '@utils/sort.js';
import { matchesFilter } from '@utils/filter.js';
import { applyUpdate, createUpsertDocument, extractFilterFields } from '@utils/update.js';
import { applyProjection } from '@utils/projection.js';
import {
  validateFilter,
  validateProjection,
  validateUpdate,
  validateDocument,
  validateAggregationPipeline,
} from '@utils/validation.js';
import { IndexManager } from '@mongolake/index/index-manager.js';
import {
  ChangeStream,
  type ChangeStreamOptions,
} from '@mongolake/change-stream/index.js';
import type { Database } from './database.js';
import { extractDocumentId } from './helpers.js';
import { FindCursor, StreamingFindCursor } from './cursors.js';
import { AggregationCursor } from './aggregation.js';
import { TimeTravelCollection } from './time-travel.js';
import {
  IndexService,
  ChangeStreamService,
  ValidationService,
  AuditService,
} from './services/index.js';
import { QueryResultCache, type QueryCacheOptions, type QueryCacheStats } from '../query/result-cache.js';

// ============================================================================
// Collection
// ============================================================================

export class Collection<T extends Document = Document> {
  private manifest: CollectionManifest | null = null;
  private currentSeq: number = 0;
  private _lastQueryMetadata: QueryMetadata | null = null;

  // Services (lazy initialized)
  private _indexService: IndexService | null = null;
  private _changeStreamService: ChangeStreamService<T> | null = null;
  private _validationService: ValidationService | null = null;
  private _auditService: AuditService | null = null;
  private _queryCache: QueryResultCache<T> | null = null;

  // IndexManager is kept for internal use and readDocuments
  private indexManager: IndexManager;

  // Query cache configuration
  private queryCacheOptions?: QueryCacheOptions;

  constructor(
    public readonly name: string,
    protected db: Database,
    protected storage: StorageBackend,
    protected schema?: CollectionSchema,
    queryCacheOptions?: QueryCacheOptions
  ) {
    this.indexManager = new IndexManager(db.getPath(), name, storage);
    this.queryCacheOptions = queryCacheOptions;
  }

  // --------------------------------------------------------------------------
  // Service Accessors (Lazy Initialization)
  // --------------------------------------------------------------------------

  /**
   * Get the index service for index operations.
   */
  protected get indexService(): IndexService {
    if (!this._indexService) {
      this._indexService = new IndexService(this.indexManager);
    }
    return this._indexService;
  }

  /**
   * Get the change stream service for watch operations.
   */
  protected get changeStreamService(): ChangeStreamService<T> {
    if (!this._changeStreamService) {
      this._changeStreamService = new ChangeStreamService<T>({
        db: this.db.name,
        coll: this.name,
      });
    }
    return this._changeStreamService;
  }

  /**
   * Get the validation service for batch operations.
   */
  protected get validationService(): ValidationService {
    if (!this._validationService) {
      this._validationService = new ValidationService();
    }
    return this._validationService;
  }

  /**
   * Get the audit service for corruption tracking.
   */
  protected get auditService(): AuditService {
    if (!this._auditService) {
      this._auditService = new AuditService();
    }
    return this._auditService;
  }

  /**
   * Get the query result cache (lazy initialized).
   */
  protected get queryCache(): QueryResultCache<T> {
    if (!this._queryCache) {
      this._queryCache = new QueryResultCache<T>(this.name, this.queryCacheOptions);
    }
    return this._queryCache;
  }

  // --------------------------------------------------------------------------
  // Internal Helpers
  // --------------------------------------------------------------------------

  /**
   * Get the manifest, asserting it is loaded.
   * Call this after ensureManifest() to get properly typed access to manifest.
   * @throws Error if manifest is not initialized
   */
  protected getManifest(): CollectionManifest {
    if (!this.manifest) {
      throw new Error('Collection: manifest not initialized. Call ensureManifest() first.');
    }
    return this.manifest;
  }

  /**
   * Get metadata from the last query operation.
   * Includes information about corrupted files that were skipped.
   *
   * @returns Query metadata including corruption reports, or null if no query has been executed
   *
   * @example
   * ```typescript
   * const docs = await collection.find({}, { skipCorruptedFiles: true }).toArray();
   * const metadata = collection.getQueryMetadata();
   * if (metadata?.hasDataLoss) {
   *   console.warn(`Skipped ${metadata.skippedCount} corrupted files`);
   *   for (const report of metadata.corruptedFiles) {
   *     console.warn(`  - ${report.filename}: ${report.error}`);
   *   }
   * }
   * ```
   */
  getQueryMetadata(): QueryMetadata | null {
    return this._lastQueryMetadata;
  }

  /**
   * Get the branch name this collection is operating on.
   * Returns undefined for main branch collections.
   */
  get branch(): string | undefined {
    return undefined;
  }

  /**
   * Check if this collection is on a branch (not main).
   */
  isOnBranch(): boolean {
    return false;
  }

  /**
   * Get a sibling collection from the same database.
   * Used internally for $lookup operations.
   * @internal
   */
  getSiblingCollection<U extends Document = Document>(name: string): Collection<U> {
    return this.db.collection<U>(name);
  }

  // --------------------------------------------------------------------------
  // Time Travel Operations
  // --------------------------------------------------------------------------

  /**
   * Get a read-only view of the collection at a specific timestamp.
   * Uses Iceberg snapshot time-travel to query historical data.
   *
   * @param timestamp - The point in time to query the collection at
   * @returns A read-only collection view at the specified timestamp
   *
   * @example
   * ```typescript
   * // Query data as it was yesterday
   * const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000);
   * const historicalUsers = collection.asOf(yesterday);
   * const docs = await historicalUsers.find().toArray();
   * ```
   */
  asOf(timestamp: Date): TimeTravelCollection<T> {
    return new TimeTravelCollection<T>(
      this.name,
      this.db,
      this.storage,
      { timestamp: timestamp.getTime() },
      this.schema
    );
  }

  /**
   * Get a read-only view of the collection at a specific snapshot ID.
   * Uses Iceberg snapshot time-travel to query historical data.
   *
   * @param snapshotId - The snapshot ID to query the collection at
   * @returns A read-only collection view at the specified snapshot
   *
   * @example
   * ```typescript
   * // Query data at a specific snapshot
   * const historicalUsers = collection.atSnapshot(12345n);
   * const docs = await historicalUsers.find().toArray();
   * ```
   */
  atSnapshot(snapshotId: bigint): TimeTravelCollection<T> {
    return new TimeTravelCollection<T>(
      this.name,
      this.db,
      this.storage,
      { snapshotId: Number(snapshotId) },
      this.schema
    );
  }

  // --------------------------------------------------------------------------
  // Write Operations
  // --------------------------------------------------------------------------

  /**
   * Insert a single document
   */
  async insertOne(doc: T): Promise<InsertOneResult> {
    const result = await this.insertMany([doc]);
    return {
      acknowledged: result.acknowledged,
      insertedId: result.insertedIds[0]!,
    };
  }

  /**
   * Insert multiple documents
   */
  async insertMany(docs: T[]): Promise<InsertManyResult> {
    // Validate batch size limits
    this.validationService.validateBatchLimits(docs, 'insertMany');

    // Validate all documents before inserting
    for (const doc of docs) {
      validateDocument(doc);
    }

    await this.ensureManifest();

    const insertedIds: { [key: number]: string | ObjectId } = {};
    const rows: Array<{ _id: string; _seq: number; _op: 'i'; doc: T }> = [];

    for (let i = 0; i < docs.length; i++) {
      const doc = { ...docs[i] };

      // Generate _id if not provided
      if (!doc._id) {
        doc._id = crypto.randomUUID();
      }

      const id = extractDocumentId(doc);
      insertedIds[i] = doc._id;

      rows.push({
        _id: id,
        _seq: ++this.currentSeq,
        _op: 'i',
        doc: doc as T,
      });
    }

    // Write to delta file
    await this.writeDelta(rows);

    // Notify change streams of insert events
    for (const row of rows) {
      this.changeStreamService.notifyChangeStreams('insert', { _id: row._id }, row.doc as WithId<T>);
    }

    return {
      acknowledged: true,
      insertedCount: docs.length,
      insertedIds,
    };
  }

  /**
   * Update a single document
   */
  async updateOne(filter: Filter<T>, update: Update<T>, options?: UpdateOptions): Promise<UpdateResult> {
    // Validate inputs
    validateFilter(filter);
    validateUpdate(update);

    const doc = await this.findOne(filter);

    if (!doc) {
      if (options?.upsert) {
        // Upsert: create new document combining filter fields + update operations
        // MongoDB behavior: equality fields from filter are used as initial document values
        const newDoc = createUpsertDocument<T>(filter as FilterQuery, update);
        const result = await this.insertOne(newDoc);
        return {
          acknowledged: true,
          matchedCount: 0,
          modifiedCount: 0,
          upsertedCount: 1,
          upsertedId: result.insertedId,
        };
      }
      return {
        acknowledged: true,
        matchedCount: 0,
        modifiedCount: 0,
        upsertedCount: 0,
      };
    }

    const updated = applyUpdate(doc, update);
    const id = extractDocumentId(doc);

    // Create map of old documents for index updates
    const oldDocs = new Map<string, T>();
    oldDocs.set(id, doc as T);

    await this.writeDelta([
      {
        _id: id,
        _seq: ++this.currentSeq,
        _op: 'u' as const,
        doc: updated,
      },
    ], oldDocs);

    // Notify change streams of update event
    this.changeStreamService.notifyChangeStreams('update', { _id: id }, updated as WithId<T>, doc);

    return {
      acknowledged: true,
      matchedCount: 1,
      modifiedCount: 1,
      upsertedCount: 0,
    };
  }

  /**
   * Update multiple documents
   */
  async updateMany(filter: Filter<T>, update: Update<T>, options?: UpdateOptions): Promise<UpdateResult> {
    // Validate inputs
    validateFilter(filter);
    validateUpdate(update);

    const docs = await this.find(filter).toArray();

    // Validate batch size limits for matching documents
    if (docs.length > 0) {
      this.validationService.validateBatchLimits(docs, 'updateMany');
    }

    if (docs.length === 0) {
      if (options?.upsert) {
        // Upsert: create new document combining filter fields + update operations
        // MongoDB behavior: equality fields from filter are used as initial document values
        // Note: updateMany with upsert only creates one document (same as MongoDB)
        const newDoc = createUpsertDocument<T>(filter as FilterQuery, update);
        const result = await this.insertOne(newDoc);
        return {
          acknowledged: true,
          matchedCount: 0,
          modifiedCount: 0,
          upsertedCount: 1,
          upsertedId: result.insertedId,
        };
      }
      return {
        acknowledged: true,
        matchedCount: 0,
        modifiedCount: 0,
        upsertedCount: 0,
      };
    }

    const rows: Array<{ _id: string; _seq: number; _op: 'u'; doc: T }> = [];

    // Create map of old documents for index updates
    const oldDocsMap = new Map<string, T>();
    const oldDocs: WithId<T>[] = [];
    for (const doc of docs) {
      const oldDoc = { ...doc } as WithId<T>;
      oldDocs.push(oldDoc);
      const updated = applyUpdate(doc, update);
      const id = extractDocumentId(doc);

      oldDocsMap.set(id, oldDoc as T);
      rows.push({
        _id: id,
        _seq: ++this.currentSeq,
        _op: 'u',
        doc: updated,
      });
    }

    await this.writeDelta(rows, oldDocsMap);

    // Notify change streams of update events
    for (let i = 0; i < rows.length; i++) {
      this.changeStreamService.notifyChangeStreams('update', { _id: rows[i]!._id }, rows[i]!.doc as WithId<T>, oldDocs[i]);
    }

    return {
      acknowledged: true,
      matchedCount: docs.length,
      modifiedCount: docs.length,
      upsertedCount: 0,
    };
  }

  /**
   * Replace a single document
   */
  async replaceOne(filter: Filter<T>, replacement: T, options?: UpdateOptions): Promise<UpdateResult> {
    // Validate inputs
    validateFilter(filter);
    validateDocument(replacement);

    const doc = await this.findOne(filter);

    if (!doc) {
      if (options?.upsert) {
        // Extract _id from filter if present (MongoDB behavior for replaceOne upsert)
        const filterFields = extractFilterFields(filter as FilterQuery);
        const docToInsert = { ...replacement } as T;
        if (filterFields._id !== undefined) {
          (docToInsert as DocumentFields)._id = filterFields._id;
        }
        const result = await this.insertOne(docToInsert);
        return {
          acknowledged: true,
          matchedCount: 0,
          modifiedCount: 0,
          upsertedCount: 1,
          upsertedId: result.insertedId,
        };
      }
      return {
        acknowledged: true,
        matchedCount: 0,
        modifiedCount: 0,
        upsertedCount: 0,
      };
    }

    const id = extractDocumentId(doc);
    const newDoc = { ...replacement, _id: doc._id };

    // Create map of old documents for index updates
    const oldDocs = new Map<string, T>();
    oldDocs.set(id, doc as T);

    await this.writeDelta([
      {
        _id: id,
        _seq: ++this.currentSeq,
        _op: 'u' as const,
        doc: newDoc as T,
      },
    ], oldDocs);

    // Notify change streams of replace event
    this.changeStreamService.notifyChangeStreams('replace', { _id: id }, newDoc as WithId<T>, doc);

    return {
      acknowledged: true,
      matchedCount: 1,
      modifiedCount: 1,
      upsertedCount: 0,
    };
  }

  /**
   * Delete a single document
   */
  async deleteOne(filter: Filter<T>, _options?: DeleteOptions): Promise<DeleteResult> {
    // Validate filter
    validateFilter(filter);

    const doc = await this.findOne(filter);

    if (!doc) {
      return {
        acknowledged: true,
        deletedCount: 0,
      };
    }

    const id = extractDocumentId(doc);

    await this.writeDelta([
      {
        _id: id,
        _seq: ++this.currentSeq,
        _op: 'd' as const,
        doc: {} as T,
      },
    ]);

    // Notify change streams of delete event
    this.changeStreamService.notifyChangeStreams('delete', { _id: id }, undefined, doc);

    return {
      acknowledged: true,
      deletedCount: 1,
    };
  }

  /**
   * Delete multiple documents
   */
  async deleteMany(filter: Filter<T>, _options?: DeleteOptions): Promise<DeleteResult> {
    // Validate filter
    validateFilter(filter);

    const docs = await this.find(filter).toArray();

    // Validate batch size limits for matching documents
    if (docs.length > 0) {
      this.validationService.validateBatchLimits(docs, 'deleteMany');
    }

    if (docs.length === 0) {
      return {
        acknowledged: true,
        deletedCount: 0,
      };
    }

    const rows: Array<{ _id: string; _seq: number; _op: 'd'; doc: T }> = [];

    for (const doc of docs) {
      const id = extractDocumentId(doc);

      rows.push({
        _id: id,
        _seq: ++this.currentSeq,
        _op: 'd',
        doc: {} as T,
      });
    }

    await this.writeDelta(rows);

    // Notify change streams of delete events
    for (let i = 0; i < docs.length; i++) {
      this.changeStreamService.notifyChangeStreams('delete', { _id: rows[i]!._id }, undefined, docs[i]);
    }

    return {
      acknowledged: true,
      deletedCount: docs.length,
    };
  }

  // --------------------------------------------------------------------------
  // Read Operations
  // --------------------------------------------------------------------------

  /**
   * Find a single document
   */
  async findOne(filter?: Filter<T>, options?: FindOptions): Promise<WithId<T> | null> {
    const cursor = this.find(filter, { ...options, limit: 1 });
    const results = await cursor.toArray();
    return results[0] || null;
  }

  /**
   * Find documents
   */
  find(filter?: Filter<T>, options?: FindOptions): FindCursor<T> {
    // Validate filter and options if provided
    if (filter) {
      validateFilter(filter);
    }
    if (options?.projection) {
      validateProjection(options.projection);
    }
    return new FindCursor<T>(this, filter, options);
  }

  /**
   * Find documents with streaming for memory-efficient processing.
   *
   * This is the recommended method for processing large collections as it
   * avoids loading all documents into memory. Documents are yielded in batches
   * controlled by the batchSize option.
   *
   * @param filter - Query filter to select documents
   * @param options - Find options including batchSize for controlling batch yield size
   * @returns StreamingFindCursor for memory-efficient iteration
   *
   * @example
   * \`\`\`typescript
   * // Process large collection without loading all into memory
   * const cursor = collection.findStream({ status: 'active' }, { batchSize: 1000 });
   *
   * // Process documents one at a time
   * for await (const doc of cursor) {
   *   await processDocument(doc);
   * }
   *
   * // Or process in batches for better throughput
   * for await (const batch of cursor.batches()) {
   *   await Promise.all(batch.map(processDocument));
   * }
   * \`\`\`
   */
  findStream(
    filter?: Filter<T>,
    options?: FindOptions & { batchSize?: number }
  ): StreamingFindCursor<T> {
    // Validate filter and options if provided
    if (filter) {
      validateFilter(filter);
    }
    if (options?.projection) {
      validateProjection(options.projection);
    }
    return new StreamingFindCursor<T>(this, filter, options);
  }

  /**
   * Count documents
   */
  async countDocuments(filter?: Filter<T>): Promise<number> {
    const docs = await this.find(filter).toArray();
    return docs.length;
  }

  /**
   * Estimated document count (fast, approximate)
   */
  async estimatedDocumentCount(): Promise<number> {
    await this.ensureManifest();
    // Sum row counts from manifest
    return this.manifest?.files.reduce((sum, f) => sum + f.rowCount, 0) || 0;
  }

  /**
   * Get distinct values for a field
   */
  async distinct<K extends keyof T & keyof WithId<T>>(field: K, filter?: Filter<T>): Promise<T[K][]> {
    const docs = await this.find(filter).toArray();
    const values = new Set<T[K]>();

    for (const doc of docs) {
      const value = doc[field] as T[K];
      if (value !== undefined) {
        values.add(value);
      }
    }

    return Array.from(values);
  }

  /**
   * Run aggregation pipeline
   */
  aggregate<R extends Document = Document>(
    pipeline: AggregationStage[],
    options?: AggregateOptions
  ): AggregationCursor<R> {
    // Validate pipeline
    validateAggregationPipeline(pipeline);
    // AggregationCursor needs Collection<Document> for its source operations.
    // Double cast is required due to TypeScript's strict variance checking on generic types.
    // T extends Document, so this is safe at runtime.
    return new AggregationCursor<R>(this as unknown as Collection<Document>, pipeline, options);
  }

  // --------------------------------------------------------------------------
  // Change Stream Operations (delegated to ChangeStreamService)
  // --------------------------------------------------------------------------

  /**
   * Watch for changes in the collection
   *
   * @param pipeline - Aggregation pipeline stages for filtering change events
   * @param options - Change stream options
   * @returns A ChangeStream that can be iterated to receive change events
   *
   * @example
   * ```typescript
   * // Watch all changes
   * const changeStream = collection.watch();
   * for await (const event of changeStream) {
   *   console.log('Change:', event.operationType, event.documentKey);
   * }
   *
   * // Filter for insert events only
   * const insertStream = collection.watch([
   *   { $match: { operationType: 'insert' } }
   * ]);
   *
   * // Get full document on updates
   * const updateStream = collection.watch([], {
   *   fullDocument: 'updateLookup'
   * });
   * ```
   */
  watch(
    pipeline: AggregationStage[] = [],
    options: ChangeStreamOptions = {}
  ): ChangeStream<T> {
    return this.changeStreamService.watch(pipeline, options);
  }

  // --------------------------------------------------------------------------
  // Index Operations (delegated to IndexService)
  // --------------------------------------------------------------------------

  /**
   * Create an index
   *
   * @param spec - Index specification (e.g., { age: 1 } for ascending index on age)
   * @param options - Index options (name, unique, sparse)
   * @returns The index name
   *
   * @example
   * ```typescript
   * // Create ascending index on 'age' field
   * await collection.createIndex({ age: 1 });
   *
   * // Create unique index with custom name
   * await collection.createIndex({ email: 1 }, { unique: true, name: 'email_unique' });
   *
   * // Create text index for full-text search
   * await collection.createIndex({ title: 'text', body: 'text' }, {
   *   name: 'content_text',
   *   weights: { title: 10, body: 1 }
   * });
   * ```
   */
  async createIndex(spec: IndexSpec, options?: IndexOptions): Promise<string> {
    await this.ensureManifest();
    return this.indexService.createIndex(spec, options, () => this.readDocuments());
  }

  /**
   * Create multiple indexes
   */
  async createIndexes(specs: Array<{ key: IndexSpec; options?: IndexOptions }>): Promise<string[]> {
    await this.ensureManifest();
    return this.indexService.createIndexes(specs, () => this.readDocuments());
  }

  /**
   * Drop an index
   */
  async dropIndex(name: string): Promise<void> {
    return this.indexService.dropIndex(name);
  }

  /**
   * List indexes
   */
  async listIndexes(): Promise<Array<{ name: string; key: IndexSpec }>> {
    return this.indexService.listIndexes();
  }

  /**
   * Get the index manager for advanced operations
   * @internal
   */
  getIndexManager(): IndexManager {
    return this.indexManager;
  }

  // --------------------------------------------------------------------------
  // Query Cache Operations
  // --------------------------------------------------------------------------

  /**
   * Get statistics about the query result cache.
   *
   * @returns Cache statistics including hit rate, entry count, and memory usage
   *
   * @example
   * ```typescript
   * const stats = collection.getCacheStats();
   * console.log(`Cache hit rate: ${stats.hitRate}%`);
   * console.log(`Cached queries: ${stats.entries}`);
   * ```
   */
  getCacheStats(): QueryCacheStats {
    return this.queryCache.getStats();
  }

  /**
   * Clear the query result cache for this collection.
   * Use this to force fresh queries after external data changes.
   *
   * @example
   * ```typescript
   * // After bulk import via external tool
   * collection.clearCache();
   * ```
   */
  clearCache(): void {
    this.queryCache.clear();
  }

  /**
   * Enable or disable the query result cache.
   *
   * @param enabled - Whether to enable the cache
   *
   * @example
   * ```typescript
   * // Disable caching for real-time queries
   * collection.setCacheEnabled(false);
   * ```
   */
  setCacheEnabled(enabled: boolean): void {
    this.queryCache.setEnabled(enabled);
  }

  /**
   * Check if the query result cache is enabled.
   */
  isCacheEnabled(): boolean {
    return this.queryCache.isEnabled();
  }

  // --------------------------------------------------------------------------
  // Internal Methods
  // --------------------------------------------------------------------------

  /**
   * Ensure manifest exists
   * @internal
   */
  async ensureManifest(): Promise<void> {
    if (this.manifest) return;

    const manifestPath = `${this.db.getPath()}/${this.name}/_manifest.json`;
    const data = await this.storage.get(manifestPath);

    if (data) {
      this.manifest = JSON.parse(new TextDecoder().decode(data));
      const manifest = this.getManifest();
      this.currentSeq = manifest.currentSeq;
    } else {
      this.manifest = {
        name: this.name,
        files: [],
        schema: this.schema || {},
        currentSeq: 0,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      };
    }

    // Ensure the _id index exists
    await this.indexManager.ensureIdIndex();
  }

  /**
   * Stream documents in batches to avoid loading entire collection into memory.
   *
   * This is the memory-efficient alternative to readDocuments() for large collections.
   * Documents are yielded in batches after deduplication and filtering.
   *
   * Note: Due to Parquet's append-only nature, deduplication requires tracking
   * seen document IDs in memory. The memory footprint is O(unique_docs) for IDs
   * rather than O(all_docs) for full documents, which is significantly smaller.
   *
   * @param filter - Optional filter to apply to documents
   * @param options - Find options including batchSize for controlling batch yield size
   * @returns AsyncGenerator yielding batches of documents
   * @internal
   *
   * @example
   * ```typescript
   * // Process large collection without loading all into memory
   * for await (const batch of collection.readDocumentsStream({}, { batchSize: 1000 })) {
   *   for (const doc of batch) {
   *     await processDocument(doc);
   *   }
   * }
   * ```
   */
  async *readDocumentsStream(
    filter?: Filter<T>,
    options?: FindOptions & { batchSize?: number }
  ): AsyncGenerator<WithId<T>[], void, undefined> {
    await this.ensureManifest();

    // Reset audit service for this operation
    this.auditService.reset();

    const batchSize = options?.batchSize ?? 1000;

    // Try to use an index for the query
    let candidateDocIds: Set<string> | null = null;

    if (filter && Object.keys(filter).length > 0) {
      const queryPlan = await this.indexManager.analyzeQuery(filter as Filter<Document>);

      if (queryPlan.useIndex && queryPlan.indexName && queryPlan.field) {
        const scanResult = await this.indexManager.scanIndex(
          queryPlan.indexName,
          queryPlan.field,
          filter as Filter<Document>
        );
        candidateDocIds = new Set(scanResult.docIds);
      }
    }

    // Get all Parquet files for this collection
    const dbPath = this.db.getPath();
    const collectionPrefix = `${dbPath}/${this.name}`;
    const files = await this.storage.list(dbPath);

    const parquetFiles = files.filter(
      (f) => f.startsWith(collectionPrefix) && f.endsWith('.parquet') && !f.includes('/_')
    );

    // For streaming with deduplication, we need to track seen IDs.
    // We use a two-pass approach for sorted queries, or single-pass for unsorted.
    //
    // The challenge: Parquet files may contain multiple versions of the same document.
    // We must read ALL files first to know the final state of each document.
    // However, we can yield documents in batches as we process them.

    // Track document states: id -> { seq, op, doc }
    // This is necessary for deduplication but uses less memory than storing all docs
    // because we only keep the latest version of each document.
    const docsById = new Map<string, { seq: number; op: string; doc: T }>();

    // First pass: read all files to build deduplicated state
    for (const file of parquetFiles) {
      this.auditService.recordFileProcessed();

      const data = await this.storage.get(file);
      if (!data) continue;

      try {
        const rows = await readParquet<T>(data);

        for (const row of rows) {
          const existing = docsById.get(row._id);
          if (!existing || row._seq > existing.seq) {
            docsById.set(row._id, {
              seq: row._seq,
              op: row._op,
              doc: row.doc,
            });
          }
        }
      } catch (error) {
        const handled = this.auditService.handleCorruptedFile(file, error, {
          collection: this.name,
          database: this.db.name,
          options,
        });

        if (!handled) {
          throw new Error(`Failed to read Parquet file ${file}: ${error instanceof Error ? error.message : String(error)}`);
        }
      }
    }

    // Update query metadata with corruption information
    this._lastQueryMetadata = this.auditService.getQueryMetadata();

    // If sorting is required, we need to collect all matching docs first, sort, then yield in batches
    if (options?.sort) {
      // Collect all matching documents
      const allDocs: WithId<T>[] = [];

      for (const [id, { op, doc }] of docsById) {
        if (op === 'd') continue;
        if (candidateDocIds && !candidateDocIds.has(id)) continue;

        const fullDoc = { ...doc, _id: id } as WithId<T>;
        if (!filter || matchesFilter(fullDoc, filter as Filter<WithId<T>>)) {
          allDocs.push(fullDoc);
        }
      }

      // Sort all documents
      let sortedDocs = this.sortDocumentsInternal(allDocs, options.sort);

      // Apply skip
      if (options?.skip) {
        sortedDocs = sortedDocs.slice(options.skip);
      }

      // Yield in batches, respecting limit
      let yielded = 0;
      const limit = options?.limit ?? Infinity;

      for (let i = 0; i < sortedDocs.length && yielded < limit; i += batchSize) {
        const remaining = limit - yielded;
        const batchEnd = Math.min(i + batchSize, sortedDocs.length, i + remaining);
        let batch = sortedDocs.slice(i, batchEnd);

        // Apply projection
        if (options?.projection) {
          batch = batch.map((doc) => applyProjection(doc, options.projection!) as WithId<T>);
        }

        yielded += batch.length;
        yield batch;
      }
    } else {
      // No sorting required - can yield batches as we iterate through the map
      let batch: WithId<T>[] = [];
      let skipped = 0;
      let yielded = 0;
      const skipCount = options?.skip ?? 0;
      const limit = options?.limit ?? Infinity;

      for (const [id, { op, doc }] of docsById) {
        if (yielded >= limit) break;
        if (op === 'd') continue;
        if (candidateDocIds && !candidateDocIds.has(id)) continue;

        const fullDoc = { ...doc, _id: id } as WithId<T>;
        if (!filter || matchesFilter(fullDoc, filter as Filter<WithId<T>>)) {
          // Handle skip
          if (skipped < skipCount) {
            skipped++;
            continue;
          }

          // Apply projection if needed
          let outputDoc = fullDoc;
          if (options?.projection) {
            outputDoc = applyProjection(fullDoc, options.projection) as WithId<T>;
          }

          batch.push(outputDoc);
          yielded++;

          // Yield batch when it reaches batchSize
          if (batch.length >= batchSize) {
            yield batch;
            batch = [];
          }
        }
      }

      // Yield remaining documents
      if (batch.length > 0) {
        yield batch;
      }
    }
  }

  /**
   * Read all documents (internal)
   * @internal
   */
  async readDocuments(filter?: Filter<T>, options?: FindOptions): Promise<WithId<T>[]> {
    await this.ensureManifest();

    // Check query cache first (unless explicitly disabled)
    // useCache: false - completely disable caching for this query
    // noCache: true - bypass cache read but still cache results
    // skipCorruptedFiles: true - bypass cache to ensure corruption metadata is accurate
    const cacheEnabled = options?.useCache !== false;
    const bypassCacheRead = options?.noCache === true || options?.skipCorruptedFiles === true;

    if (cacheEnabled && !bypassCacheRead) {
      const cached = this.queryCache.get(filter, options);
      if (cached) {
        return cached;
      }
    }

    // Reset audit service for this operation
    this.auditService.reset();

    // Try to use an index for the query
    let candidateDocIds: Set<string> | null = null;

    if (filter && Object.keys(filter).length > 0) {
      const queryPlan = await this.indexManager.analyzeQuery(filter as Filter<Document>);

      if (queryPlan.useIndex && queryPlan.indexName && queryPlan.field) {
        // Use index to get candidate document IDs
        const scanResult = await this.indexManager.scanIndex(
          queryPlan.indexName,
          queryPlan.field,
          filter as Filter<Document>
        );
        candidateDocIds = new Set(scanResult.docIds);
      }
    }

    // Get all Parquet files for this collection
    // Files are stored as: {db}/{collection}_{timestamp}_{seq}.parquet
    // So we list from the database directory and filter by collection prefix
    const dbPath = this.db.getPath();
    const collectionPrefix = `${dbPath}/${this.name}`;
    const files = await this.storage.list(dbPath);

    const parquetFiles = files.filter(
      (f) => f.startsWith(collectionPrefix) && f.endsWith('.parquet') && !f.includes('/_')
    );

    // Read and deduplicate
    const docsById = new Map<string, { seq: number; op: string; doc: T }>();

    for (const file of parquetFiles) {
      this.auditService.recordFileProcessed();

      const data = await this.storage.get(file);
      if (!data) continue;

      // Read Parquet file (handles both new binary format and legacy JSON)
      try {
        const rows = await readParquet<T>(data);

        for (const row of rows) {
          const existing = docsById.get(row._id);
          if (!existing || row._seq > existing.seq) {
            docsById.set(row._id, {
              seq: row._seq,
              op: row._op,
              doc: row.doc,
            });
          }
        }
      } catch (error) {
        const handled = this.auditService.handleCorruptedFile(file, error, {
          collection: this.name,
          database: this.db.name,
          options,
        });

        if (!handled) {
          // Propagate error by default to avoid silent data loss
          throw new Error(`Failed to read Parquet file ${file}: ${error instanceof Error ? error.message : String(error)}`);
        }
      }
    }

    // Update query metadata with corruption information
    this._lastQueryMetadata = this.auditService.getQueryMetadata();

    // Filter out deletes and apply filter
    const results: WithId<T>[] = [];

    for (const [id, { op, doc }] of docsById) {
      if (op === 'd') continue;

      // If we have candidate IDs from index, skip docs not in the set
      if (candidateDocIds && !candidateDocIds.has(id)) {
        continue;
      }

      const fullDoc = { ...doc, _id: id } as WithId<T>;

      // Still apply the full filter for complex conditions not handled by index
      if (!filter || matchesFilter(fullDoc, filter as Filter<WithId<T>>)) {
        results.push(fullDoc);
      }
    }

    // Apply options
    let output = results;

    if (options?.sort) {
      output = this.sortDocumentsInternal(output, options.sort);
    }

    if (options?.skip) {
      output = output.slice(options.skip);
    }

    if (options?.limit) {
      output = output.slice(0, options.limit);
    }

    if (options?.projection) {
      output = output.map((doc) => applyProjection(doc, options.projection!) as WithId<T>);
    }

    // Cache the results (unless explicitly disabled with useCache: false)
    if (cacheEnabled) {
      this.queryCache.set(filter, options, output);
    }

    return output;
  }

  /**
   * Write delta file
   * @internal
   * @param rows - Array of rows to write
   * @param oldDocs - Optional map of _id to old document (for update operations)
   */
  private async writeDelta(
    rows: Array<{ _id: string; _seq: number; _op: 'i' | 'u' | 'd'; doc: T }>,
    oldDocs?: Map<string, T>
  ): Promise<void> {
    await this.ensureManifest();

    // Use both timestamp and sequence number to ensure unique file names
    // This prevents overwrites when multiple writes occur in the same millisecond
    const deltaPath = `${this.db.getPath()}/${this.name}_${Date.now()}_${this.currentSeq}.parquet`;

    // Write as proper binary Parquet format using Variant encoding
    // No compression by default - benchmarks show overhead often exceeds benefit
    const parquetData = writeParquet(rows);

    await this.storage.put(deltaPath, parquetData);

    // Update indexes for each row
    for (const row of rows) {
      if (row._op === 'i') {
        // Insert: add to all indexes
        await this.indexManager.indexDocument(row.doc as Document);
      } else if (row._op === 'u') {
        // Update: handle non-_id index updates properly
        // We need both the old document (for unindexing) and new document (for indexing)
        const oldDoc = oldDocs?.get(row._id);
        if (oldDoc) {
          // Use the optimized updateDocumentIndexes method that handles
          // unindexing old values and indexing new values for all non-_id indexes
          await this.indexManager.updateDocumentIndexes(oldDoc as Document, row.doc as Document);
        }
        // If oldDoc is not provided, we can't update indexes properly
        // This maintains backward compatibility but indexes may be stale
      } else if (row._op === 'd') {
        // Delete: remove from all indexes
        // We need to construct a minimal doc with just the _id for unindexing
        await this.indexManager.unindexDocument({ _id: row._id } as Document);
      }
    }

    // Flush index changes to storage
    await this.indexManager.flush();

    // Update manifest
    const manifest = this.getManifest();
    manifest.currentSeq = this.currentSeq;
    manifest.updatedAt = new Date().toISOString();

    const manifestPath = `${this.db.getPath()}/${this.name}/_manifest.json`;
    await this.storage.put(manifestPath, new TextEncoder().encode(JSON.stringify(manifest)));

    // Invalidate query cache on any write operation
    this.queryCache.invalidate();
  }

  /**
   * Sort documents
   * @internal
   */
  private sortDocumentsInternal(docs: WithId<T>[], sort: { [key: string]: 1 | -1 }): WithId<T>[] {
    return sortDocuments(docs, sort);
  }

}
