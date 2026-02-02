/**
 * MongoLake Client
 *
 * MongoDB-compatible client API
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
  MongoLakeConfig,
  ObjectId,
  CollectionManifest,
  CollectionSchema,
} from '../types.js';
import {
  Cursor,
  type CursorOptions,
  type DocumentSource,
} from '../cursor/index.js';
import {
  ClientSession,
  SessionStore,
  type SessionOptions,
  type SessionOperationOptions,
  type BufferedOperation,
  extractSession,
} from '../session/index.js';
import {
  SnapshotManager,
  type Snapshot,
  type TableMetadata,
} from '@dotdo/iceberg';
import { createStorage, type StorageBackend } from '../storage/index.js';
import { writeParquet, readParquet } from '../parquet/io.js';
import { sortDocuments } from '../utils/sort.js';
import { matchesFilter } from '../utils/filter.js';
import { applyUpdate } from '../utils/update.js';
import { applyProjection } from '../utils/projection.js';
import {
  validateDatabaseName,
  validateCollectionName,
  validateFilter,
  validateProjection,
  validateUpdate,
  validateDocument,
  validateAggregationPipeline,
} from '../utils/validation.js';
import { getNestedValue } from '../utils/nested.js';
import { IndexManager } from '../index/index-manager.js';
import {
  ChangeStream,
  computeUpdateDescription,
  type ChangeStreamOptions,
  type OperationType,
} from '../change-stream/index.js';
import {
  BranchStore,
  DEFAULT_BRANCH,
  type BranchMetadata,
} from '../branching/index.js';
import {
  DiffGenerator,
  type DiffResult,
  type DiffOptions,
} from '../branching/diff.js';

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Extract document ID as a string
 * Handles both ObjectId (objects with toString()) and primitive IDs
 */
function extractDocumentId(doc: { _id?: unknown }): string {
  if (doc._id === undefined) {
    throw new Error('Document must have _id field');
  }
  return typeof doc._id === 'object' && doc._id !== null
    ? doc._id.toString()
    : String(doc._id);
}

// ============================================================================
// Factory Functions
// ============================================================================

/**
 * Create a new MongoLake client
 *
 * Factory function for creating MongoLake clients without using singletons.
 * This is the recommended approach for:
 * - Testing (each test gets its own isolated client)
 * - Multi-tenant scenarios (each tenant gets its own client)
 * - Explicit dependency injection
 *
 * @example
 * ```typescript
 * import { createClient } from 'mongolake';
 *
 * const client = createClient({ local: '.mongolake' });
 * const users = client.db('myapp').collection('users');
 * await users.insertOne({ name: 'Alice' });
 * ```
 */
export function createClient(config: MongoLakeConfig = {}): MongoLake {
  return new MongoLake(config);
}

/**
 * Create a database instance directly
 *
 * Convenience factory function that creates a new client and returns
 * the database instance. Each call creates a new client, avoiding
 * global singleton state.
 *
 * For production use where you need to share a client across multiple
 * database accesses, use `createClient()` instead and call `.db()` on it.
 *
 * @example
 * ```typescript
 * import { createDatabase } from 'mongolake';
 *
 * // Each call creates a fresh client - good for isolated operations
 * const users = createDatabase('myapp').collection('users');
 * await users.insertOne({ name: 'Alice' });
 *
 * // For shared client scenarios, prefer createClient():
 * const client = createClient();
 * const db1 = client.db('app1');
 * const db2 = client.db('app2');
 * ```
 */
export function createDatabase(name?: string, config: MongoLakeConfig = {}): Database {
  const client = createClient(config);
  return client.db(name);
}

// ============================================================================
// MongoLake Client
// ============================================================================

/**
 * MongoLake client - MongoDB-compatible interface
 *
 * @example
 * ```typescript
 * // Local development
 * const lake = new MongoLake({ local: '.mongolake' });
 *
 * // Cloudflare Workers
 * const lake = new MongoLake({ bucket: env.R2_BUCKET });
 *
 * // S3-compatible
 * const lake = new MongoLake({
 *   endpoint: 'https://s3.amazonaws.com',
 *   accessKeyId: '...',
 *   secretAccessKey: '...',
 *   bucketName: 'my-bucket',
 * });
 * ```
 */
export class MongoLake {
  private config: MongoLakeConfig;
  private storage: StorageBackend;
  private databases: Map<string, Database> = new Map();
  private sessionStore: SessionStore;

  constructor(config: MongoLakeConfig = {}) {
    this.config = {
      database: 'default',
      ...config,
    };
    this.storage = createStorage(this.config);
    this.sessionStore = new SessionStore({
      timeoutMs: 30 * 60 * 1000, // 30 minutes
      cleanupIntervalMs: 60000,  // 1 minute
    });
  }

  /**
   * Start a new client session for transaction support.
   *
   * @param options - Session options
   * @returns A new ClientSession instance
   *
   * @example
   * ```typescript
   * const session = client.startSession();
   * session.startTransaction();
   *
   * try {
   *   await collection.insertOne(doc, { session });
   *   await session.commitTransaction();
   * } catch (error) {
   *   await session.abortTransaction();
   * } finally {
   *   await session.endSession();
   * }
   * ```
   */
  startSession(options?: SessionOptions): ClientSession {
    const session = new ClientSession(options);

    // Set up the commit handler to process buffered operations
    session.setCommitHandler(async (_session, operations) => {
      await this.executeTransactionOperations(operations);
    });

    this.sessionStore.add(session);
    return session;
  }

  /**
   * Execute buffered transaction operations atomically.
   * @internal
   */
  private async executeTransactionOperations(
    operations: BufferedOperation[]
  ): Promise<void> {
    // Group operations by database/collection for efficiency
    for (const op of operations) {
      const db = this.db(op.database);
      const collection = db.collection(op.collection);

      switch (op.type) {
        case 'insert':
          if (op.document) {
            await collection.insertOne(op.document as Document);
          }
          break;
        case 'update':
          if (op.filter && op.update) {
            await collection.updateOne(
              op.filter as Filter<Document>,
              op.update as Update<Document>,
              op.options as UpdateOptions
            );
          }
          break;
        case 'replace':
          if (op.filter && op.replacement) {
            await collection.replaceOne(
              op.filter as Filter<Document>,
              op.replacement as Document,
              op.options as UpdateOptions
            );
          }
          break;
        case 'delete':
          if (op.filter) {
            await collection.deleteOne(
              op.filter as Filter<Document>,
              op.options as DeleteOptions
            );
          }
          break;
      }
    }
  }

  /**
   * Get a session by ID.
   * @internal
   */
  getSession(sessionId: string): ClientSession | undefined {
    return this.sessionStore.get(sessionId);
  }

  /**
   * Get a database
   */
  db(name?: string): Database {
    const dbName = name || this.config.database || 'default';

    // Validate database name to prevent path traversal attacks
    validateDatabaseName(dbName);

    if (!this.databases.has(dbName)) {
      this.databases.set(dbName, new Database(dbName, this.storage, this.config));
    }

    return this.databases.get(dbName)!;
  }

  /**
   * List all databases
   */
  async listDatabases(): Promise<string[]> {
    const files = await this.storage.list('');
    const databases = new Set<string>();

    for (const file of files) {
      const parts = file.split('/');
      if (parts.length > 0 && parts[0]) {
        databases.add(parts[0]);
      }
    }

    return Array.from(databases);
  }

  /**
   * Drop a database
   */
  async dropDatabase(name: string): Promise<void> {
    // Validate database name to prevent path traversal attacks
    validateDatabaseName(name);

    const files = await this.storage.list(`${name}/`);
    for (const file of files) {
      await this.storage.delete(file);
    }
    this.databases.delete(name);
  }

  /**
   * Close client (cleanup)
   */
  async close(): Promise<void> {
    this.databases.clear();
  }
}

// ============================================================================
// Collection Options
// ============================================================================

/**
 * Options for getting a collection
 */
export interface CollectionOptions {
  /** Branch to access the collection on */
  branch?: string;
}

// ============================================================================
// Database
// ============================================================================

export class Database {
  private collections: Map<string, Collection<Document>> = new Map();
  private branchCollections: Map<string, BranchCollection<Document>> = new Map();
  private branchStore: BranchStore;

  constructor(
    public readonly name: string,
    private storage: StorageBackend,
    private config: MongoLakeConfig
  ) {
    this.branchStore = new BranchStore(storage, name);
  }

  /**
   * Get the branch store for this database
   * @internal
   */
  getBranchStore(): BranchStore {
    return this.branchStore;
  }

  /**
   * Get a collection
   */
  collection<T extends Document = Document>(name: string, options?: CollectionOptions): Collection<T> | BranchCollection<T> {
    // Validate collection name to prevent path traversal attacks
    validateCollectionName(name);

    // If branch option is provided, return a branch-aware collection
    if (options?.branch) {
      const branchKey = `${name}:${options.branch}`;
      if (!this.branchCollections.has(branchKey)) {
        const schema = this.config.schema?.[name];
        this.branchCollections.set(
          branchKey,
          new BranchCollection<Document>(name, this, this.storage, options.branch, schema)
        );
      }
      return this.branchCollections.get(branchKey) as unknown as BranchCollection<T>;
    }

    // Return regular collection for main branch
    if (!this.collections.has(name)) {
      const schema = this.config.schema?.[name];
      this.collections.set(name, new Collection<Document>(name, this, this.storage, schema));
    }
    return this.collections.get(name) as unknown as Collection<T>;
  }

  /**
   * List all collections
   */
  async listCollections(): Promise<string[]> {
    const files = await this.storage.list(`${this.name}/`);
    const collections = new Set<string>();

    for (const file of files) {
      // Extract collection name from path
      const match = file.match(new RegExp(`^${this.name}/([^/_][^/]*?)(?:_\\d+)?\\.parquet$`));
      if (match) {
        collections.add(match[1]);
      }
    }

    return Array.from(collections);
  }

  /**
   * Create a collection
   */
  async createCollection<T extends Document = Document>(
    name: string,
    _options?: { schema?: CollectionSchema }
  ): Promise<Collection<T>> {
    const collection = this.collection<T>(name);
    // Initialize manifest if needed
    await collection.ensureManifest();
    return collection;
  }

  /**
   * Drop a collection
   */
  async dropCollection(name: string): Promise<boolean> {
    // Validate collection name to prevent path traversal attacks
    validateCollectionName(name);

    const files = await this.storage.list(`${this.name}/`);
    let dropped = false;

    for (const file of files) {
      if (file.startsWith(`${this.name}/${name}.`) || file.startsWith(`${this.name}/${name}_`)) {
        await this.storage.delete(file);
        dropped = true;
      }
    }

    this.collections.delete(name);
    return dropped;
  }

  /**
   * Create a branch
   */
  async branch(_branchName: string): Promise<void> {
    // TODO: Implement branching
    throw new Error('Branching not yet implemented');
  }

  /**
   * Merge a branch
   */
  async merge(_branchName: string): Promise<void> {
    // TODO: Implement merging
    throw new Error('Merging not yet implemented');
  }

  /**
   * Get storage path for this database
   */
  getPath(): string {
    return this.name;
  }

  /**
   * Generate a diff between a branch and its base.
   *
   * Shows all documents that were inserted, updated, or deleted on the branch
   * compared to the state at the time the branch was created.
   *
   * @param branchName - The branch to diff
   * @param options - Options for filtering the diff
   * @returns The diff result containing all changes
   *
   * @example
   * ```typescript
   * const diff = await db.diff('feature-branch');
   *
   * console.log(`Inserted: ${diff.summary.insertedCount}`);
   * console.log(`Updated: ${diff.summary.updatedCount}`);
   * console.log(`Deleted: ${diff.summary.deletedCount}`);
   *
   * for (const change of diff.updated) {
   *   console.log(`${change.documentId}: ${change.changedFields.join(', ')}`);
   * }
   * ```
   */
  async diff<T extends Document = Document>(
    branchName: string,
    options?: DiffOptions
  ): Promise<DiffResult<T>> {
    const diffGenerator = new DiffGenerator(this.storage, this.name, this.branchStore);
    return diffGenerator.diff<T>(branchName, options);
  }
}

// ============================================================================
// Collection
// ============================================================================

export class Collection<T extends Document = Document> {
  private manifest: CollectionManifest | null = null;
  private currentSeq: number = 0;
  private indexManager: IndexManager;
  private changeStreams: Set<ChangeStream<T>> = new Set();

  constructor(
    public readonly name: string,
    protected db: Database,
    protected storage: StorageBackend,
    protected schema?: CollectionSchema
  ) {
    this.indexManager = new IndexManager(db.getPath(), name, storage);
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
      insertedId: result.insertedIds[0],
    };
  }

  /**
   * Insert multiple documents
   */
  async insertMany(docs: T[]): Promise<InsertManyResult> {
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
      this.notifyChangeStreams('insert', { _id: row._id }, row.doc as WithId<T>);
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
        // Upsert: create new document
        const newDoc = applyUpdate({} as T, update as Update<Document>) as T;
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

    const updated = applyUpdate(doc as Document, update as Update<Document>) as T;
    const id = extractDocumentId(doc);

    await this.writeDelta([
      {
        _id: id,
        _seq: ++this.currentSeq,
        _op: 'u' as const,
        doc: updated,
      },
    ]);

    // Notify change streams of update event
    this.notifyChangeStreams('update', { _id: id }, updated as WithId<T>, doc);

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

    if (docs.length === 0) {
      if (options?.upsert) {
        const newDoc = applyUpdate({} as T, update as Update<Document>) as T;
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

    const oldDocs: WithId<T>[] = [];
    for (const doc of docs) {
      oldDocs.push({ ...doc } as WithId<T>);
      const updated = applyUpdate(doc as Document, update as Update<Document>) as T;
      const id = extractDocumentId(doc);

      rows.push({
        _id: id,
        _seq: ++this.currentSeq,
        _op: 'u',
        doc: updated,
      });
    }

    await this.writeDelta(rows);

    // Notify change streams of update events
    for (let i = 0; i < rows.length; i++) {
      this.notifyChangeStreams('update', { _id: rows[i]._id }, rows[i].doc as WithId<T>, oldDocs[i]);
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
        const result = await this.insertOne(replacement);
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

    await this.writeDelta([
      {
        _id: id,
        _seq: ++this.currentSeq,
        _op: 'u' as const,
        doc: newDoc as T,
      },
    ]);

    // Notify change streams of replace event
    this.notifyChangeStreams('replace', { _id: id }, newDoc as WithId<T>, doc);

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
    this.notifyChangeStreams('delete', { _id: id }, undefined, doc);

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
      this.notifyChangeStreams('delete', { _id: rows[i]._id }, undefined, docs[i]);
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
    return new AggregationCursor<R>(this as unknown as Collection<Document>, pipeline, options);
  }

  // --------------------------------------------------------------------------
  // Change Stream Operations
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
    const namespace = {
      db: this.db.name,
      coll: this.name,
    };

    const changeStream = new ChangeStream<T>(namespace, pipeline, options);
    this.changeStreams.add(changeStream);

    return changeStream;
  }

  /**
   * Notify all change streams of an event
   * @internal
   */
  private notifyChangeStreams(
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

  // --------------------------------------------------------------------------
  // Index Operations
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
   * ```
   */
  async createIndex(spec: IndexSpec, options?: IndexOptions): Promise<string> {
    await this.ensureManifest();

    // Create the index in the manager
    const indexName = await this.indexManager.createIndex(spec, options);

    // Build the index from existing documents
    const docs = await this.readDocuments();
    for (const doc of docs) {
      await this.indexManager.indexDocument(doc);
    }

    // Persist index changes
    await this.indexManager.flush();

    return indexName;
  }

  /**
   * Create multiple indexes
   */
  async createIndexes(specs: Array<{ key: IndexSpec; options?: IndexOptions }>): Promise<string[]> {
    return Promise.all(specs.map((s) => this.createIndex(s.key, s.options)));
  }

  /**
   * Drop an index
   */
  async dropIndex(name: string): Promise<void> {
    await this.indexManager.dropIndex(name);
  }

  /**
   * List indexes
   */
  async listIndexes(): Promise<Array<{ name: string; key: IndexSpec }>> {
    const indexes = await this.indexManager.listIndexes();
    return indexes.map((idx) => ({
      name: idx.name,
      key: { [idx.field]: 1 } as IndexSpec,
    }));
  }

  /**
   * Get the index manager for advanced operations
   * @internal
   */
  getIndexManager(): IndexManager {
    return this.indexManager;
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
      this.currentSeq = this.manifest!.currentSeq;
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
   * Read all documents (internal)
   * @internal
   */
  async readDocuments(filter?: Filter<T>, options?: FindOptions): Promise<WithId<T>[]> {
    await this.ensureManifest();

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
        if (options?.skipCorruptedFiles) {
          // Skip corrupted file but log a warning
          console.warn(`Skipping corrupted Parquet file ${file}:`, error);
        } else {
          // Propagate error by default to avoid silent data loss
          throw new Error(`Failed to read Parquet file ${file}: ${error instanceof Error ? error.message : String(error)}`);
        }
      }
    }

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

    return output;
  }

  /**
   * Write delta file
   * @internal
   */
  private async writeDelta(
    rows: Array<{ _id: string; _seq: number; _op: 'i' | 'u' | 'd'; doc: T }>
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
        // Update: For non-_id indexes, we'd need to unindex old values and index new values
        // But for _id index, _id doesn't change on update, so we skip re-indexing _id
        // For other indexes, we would need the old document to properly handle this
        // TODO: Handle non-_id index updates properly when those are implemented
      } else if (row._op === 'd') {
        // Delete: remove from all indexes
        // We need to construct a minimal doc with just the _id for unindexing
        await this.indexManager.unindexDocument({ _id: row._id } as Document);
      }
    }

    // Flush index changes to storage
    await this.indexManager.flush();

    // Update manifest
    this.manifest!.currentSeq = this.currentSeq;
    this.manifest!.updatedAt = new Date().toISOString();

    const manifestPath = `${this.db.getPath()}/${this.name}/_manifest.json`;
    await this.storage.put(manifestPath, new TextEncoder().encode(JSON.stringify(this.manifest)));
  }

  /**
   * Sort documents
   * @internal
   */
  private sortDocumentsInternal(docs: WithId<T>[], sort: { [key: string]: 1 | -1 }): WithId<T>[] {
    return sortDocuments(docs, sort);
  }

}

// ============================================================================
// Branch Collection
// ============================================================================

/**
 * Branch-aware collection that reads from and writes to a specific branch.
 *
 * Reads layer branch changes on top of base snapshot data.
 * Writes are isolated to the branch and don't affect main.
 *
 * @example
 * ```typescript
 * // Get a collection on a feature branch
 * const users = db.collection('users', { branch: 'feature-branch' });
 *
 * // Writes go to the branch, not main
 * await users.insertOne({ name: 'Alice' });
 *
 * // Reads see branch changes layered on base
 * const docs = await users.find().toArray();
 * ```
 */
export class BranchCollection<T extends Document = Document> extends Collection<T> {
  private branchMetadata: BranchMetadata | null = null;
  private branchManifest: CollectionManifest | null = null;
  private branchCurrentSeq: number = 0;
  private baseSnapshotTimestamp: number | null = null;

  /** Cache for base documents to avoid re-reading on every query */
  private baseDocsCacheTimestamp: number = 0;
  private static readonly CACHE_TTL_MS = 5000; // 5 second TTL for testing

  constructor(
    name: string,
    db: Database,
    storage: StorageBackend,
    private readonly branchName: string,
    schema?: CollectionSchema
  ) {
    super(name, db, storage, schema);
  }

  /**
   * Get the branch name this collection is operating on.
   */
  override get branch(): string | undefined {
    return this.branchName;
  }

  /**
   * Check if this collection is on a branch (not main).
   */
  override isOnBranch(): boolean {
    return true;
  }

  /**
   * Get the branch metadata for this collection's branch.
   * Useful for inspecting branch state.
   */
  async getBranchMetadata(): Promise<BranchMetadata> {
    await this.ensureBranch();
    return this.branchMetadata!;
  }

  /**
   * Get the parent branch name (or 'main' if at root).
   */
  async getParentBranch(): Promise<string> {
    const metadata = await this.getBranchMetadata();
    return metadata.parentBranch || DEFAULT_BRANCH;
  }

  /**
   * Ensure branch metadata is loaded and validated.
   * @internal
   */
  private async ensureBranch(): Promise<void> {
    if (this.branchMetadata) return;

    const branchStore = this.db.getBranchStore();
    const branch = await branchStore.getBranch(this.branchName);

    if (!branch) {
      throw new Error(`Branch "${this.branchName}" not found`);
    }

    this.branchMetadata = branch;

    // Calculate base snapshot timestamp from branch creation
    // This is used to filter which files from main are visible
    this.baseSnapshotTimestamp = new Date(branch.createdAt).getTime();
  }

  /**
   * Ensure branch manifest exists.
   * @internal
   */
  private async ensureBranchManifest(): Promise<void> {
    if (this.branchManifest) return;

    await this.ensureBranch();

    const manifestPath = this.getBranchPath(`${this.name}/_manifest.json`);
    const data = await this.storage.get(manifestPath);

    if (data) {
      this.branchManifest = JSON.parse(new TextDecoder().decode(data));
      this.branchCurrentSeq = this.branchManifest!.currentSeq;
    } else {
      this.branchManifest = {
        name: this.name,
        files: [],
        schema: this.schema || {},
        currentSeq: 0,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      };
    }
  }

  /**
   * Get the storage path for branch-specific data.
   * @internal
   */
  private getBranchPath(relativePath: string): string {
    // Branch data is stored in: {db}/branches/{branchName}/{relativePath}
    return `${this.db.getPath()}/branches/${this.branchName}/${relativePath}`;
  }

  /**
   * Get the base (main) storage path.
   * @internal
   */
  private getBasePath(): string {
    return this.db.getPath();
  }

  // --------------------------------------------------------------------------
  // Read Operations (Layer branch on base)
  // --------------------------------------------------------------------------

  /**
   * Read all documents, layering branch changes on top of base.
   * @internal
   */
  override async readDocuments(filter?: Filter<T>, options?: FindOptions): Promise<WithId<T>[]> {
    await this.ensureBranch();
    await this.ensureBranchManifest();

    // Step 1: Read base documents (from main, filtered by branch creation time)
    const baseDocsById = await this.readBaseDocuments();

    // Step 2: Read branch-specific documents
    const branchDocsById = await this.readBranchDocuments();

    // Step 3: Layer branch changes on top of base
    // Branch data always takes precedence over base data for the same document ID
    const mergedDocsById = new Map<string, { seq: number; op: string; doc: T; isBranch: boolean }>();

    // Start with base documents
    for (const [id, entry] of baseDocsById) {
      mergedDocsById.set(id, { ...entry, isBranch: false });
    }

    // Apply branch changes (always overwrites base for same ID)
    for (const [id, entry] of branchDocsById) {
      const existing = mergedDocsById.get(id);
      // Branch entry always wins if document ID exists, or it's a new branch document
      // For branch entries with the same ID, use sequence number to determine latest
      if (!existing || !existing.isBranch || entry.seq > existing.seq) {
        mergedDocsById.set(id, { ...entry, isBranch: true });
      }
    }

    // Step 4: Filter out deletes and apply query filter
    const results: WithId<T>[] = [];

    for (const [id, { op, doc }] of mergedDocsById) {
      if (op === 'd') continue;

      const fullDoc = { ...doc, _id: id } as WithId<T>;

      if (!filter || matchesFilter(fullDoc, filter as Filter<WithId<T>>)) {
        results.push(fullDoc);
      }
    }

    // Step 5: Apply options
    let output = results;

    if (options?.sort) {
      output = sortDocuments(output, options.sort);
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

    return output;
  }

  /**
   * Read documents from base (main) that existed at branch creation time.
   * @internal
   */
  private async readBaseDocuments(): Promise<Map<string, { seq: number; op: string; doc: T }>> {
    const basePath = this.getBasePath();
    const collectionPrefix = `${basePath}/${this.name}`;
    const files = await this.storage.list(basePath);

    // Filter to parquet files that existed at branch creation time
    const parquetFiles = files.filter((f) => {
      if (!f.startsWith(collectionPrefix) || !f.endsWith('.parquet') || f.includes('/_')) {
        return false;
      }
      // Also exclude branch-specific files
      if (f.includes('/branches/')) {
        return false;
      }
      // Filter by timestamp if we have a base snapshot timestamp
      if (this.baseSnapshotTimestamp !== null) {
        const match = f.match(/_(\d+)_\d+\.parquet$/);
        if (match) {
          const fileTimestamp = parseInt(match[1], 10);
          return fileTimestamp <= this.baseSnapshotTimestamp;
        }
      }
      return true;
    });

    const docsById = new Map<string, { seq: number; op: string; doc: T }>();

    for (const file of parquetFiles) {
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
        // Skip corrupted files
        console.warn(`Skipping corrupted Parquet file ${file}:`, error);
      }
    }

    return docsById;
  }

  /**
   * Read documents that were written on this branch.
   * @internal
   */
  private async readBranchDocuments(): Promise<Map<string, { seq: number; op: string; doc: T }>> {
    const branchBasePath = this.getBranchPath('');
    const collectionPrefix = this.getBranchPath(this.name);

    const files = await this.storage.list(branchBasePath);

    const parquetFiles = files.filter(
      (f) => f.startsWith(collectionPrefix) && f.endsWith('.parquet') && !f.includes('/_')
    );

    const docsById = new Map<string, { seq: number; op: string; doc: T }>();

    for (const file of parquetFiles) {
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
        console.warn(`Skipping corrupted Parquet file ${file}:`, error);
      }
    }

    return docsById;
  }

  // --------------------------------------------------------------------------
  // Write Operations (Write to branch only)
  // --------------------------------------------------------------------------

  /**
   * Insert a single document into the branch.
   */
  override async insertOne(doc: T): Promise<InsertOneResult> {
    const result = await this.insertMany([doc]);
    return {
      acknowledged: result.acknowledged,
      insertedId: result.insertedIds[0],
    };
  }

  /**
   * Insert multiple documents into the branch.
   */
  override async insertMany(docs: T[]): Promise<InsertManyResult> {
    // Validate all documents before inserting
    for (const doc of docs) {
      validateDocument(doc);
    }

    await this.ensureBranch();
    await this.ensureBranchManifest();

    const insertedIds: { [key: number]: string | ObjectId } = {};
    const rows: Array<{ _id: string; _seq: number; _op: 'i'; doc: T }> = [];

    for (let i = 0; i < docs.length; i++) {
      const doc = { ...docs[i] };

      // Generate _id if not provided
      if (!doc._id) {
        doc._id = crypto.randomUUID();
      }

      const id = this.extractDocumentId(doc);
      insertedIds[i] = doc._id;

      rows.push({
        _id: id,
        _seq: ++this.branchCurrentSeq,
        _op: 'i',
        doc: doc as T,
      });
    }

    // Write to branch-specific delta file
    await this.writeBranchDelta(rows);

    return {
      acknowledged: true,
      insertedCount: docs.length,
      insertedIds,
    };
  }

  /**
   * Update a single document on the branch.
   */
  override async updateOne(filter: Filter<T>, update: Update<T>, options?: UpdateOptions): Promise<UpdateResult> {
    validateFilter(filter);
    validateUpdate(update);

    const doc = await this.findOne(filter);

    if (!doc) {
      if (options?.upsert) {
        const newDoc = applyUpdate({} as T, update as Update<Document>) as T;
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

    const updated = applyUpdate(doc as Document, update as Update<Document>) as T;
    const id = this.extractDocumentId(doc);

    await this.writeBranchDelta([
      {
        _id: id,
        _seq: ++this.branchCurrentSeq,
        _op: 'u' as const,
        doc: updated,
      },
    ]);

    return {
      acknowledged: true,
      matchedCount: 1,
      modifiedCount: 1,
      upsertedCount: 0,
    };
  }

  /**
   * Update multiple documents on the branch.
   */
  override async updateMany(filter: Filter<T>, update: Update<T>, options?: UpdateOptions): Promise<UpdateResult> {
    validateFilter(filter);
    validateUpdate(update);

    const docs = await this.find(filter).toArray();

    if (docs.length === 0) {
      if (options?.upsert) {
        const newDoc = applyUpdate({} as T, update as Update<Document>) as T;
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

    for (const doc of docs) {
      const updated = applyUpdate(doc as Document, update as Update<Document>) as T;
      const id = this.extractDocumentId(doc);

      rows.push({
        _id: id,
        _seq: ++this.branchCurrentSeq,
        _op: 'u',
        doc: updated,
      });
    }

    await this.writeBranchDelta(rows);

    return {
      acknowledged: true,
      matchedCount: docs.length,
      modifiedCount: docs.length,
      upsertedCount: 0,
    };
  }

  /**
   * Replace a single document on the branch.
   */
  override async replaceOne(filter: Filter<T>, replacement: T, options?: UpdateOptions): Promise<UpdateResult> {
    validateFilter(filter);
    validateDocument(replacement);

    const doc = await this.findOne(filter);

    if (!doc) {
      if (options?.upsert) {
        const result = await this.insertOne(replacement);
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

    const id = this.extractDocumentId(doc);
    const newDoc = { ...replacement, _id: doc._id };

    await this.writeBranchDelta([
      {
        _id: id,
        _seq: ++this.branchCurrentSeq,
        _op: 'u' as const,
        doc: newDoc as T,
      },
    ]);

    return {
      acknowledged: true,
      matchedCount: 1,
      modifiedCount: 1,
      upsertedCount: 0,
    };
  }

  /**
   * Delete a single document on the branch.
   */
  override async deleteOne(filter: Filter<T>, _options?: DeleteOptions): Promise<DeleteResult> {
    validateFilter(filter);

    const doc = await this.findOne(filter);

    if (!doc) {
      return {
        acknowledged: true,
        deletedCount: 0,
      };
    }

    const id = this.extractDocumentId(doc);

    await this.writeBranchDelta([
      {
        _id: id,
        _seq: ++this.branchCurrentSeq,
        _op: 'd' as const,
        doc: {} as T,
      },
    ]);

    return {
      acknowledged: true,
      deletedCount: 1,
    };
  }

  /**
   * Delete multiple documents on the branch.
   */
  override async deleteMany(filter: Filter<T>, _options?: DeleteOptions): Promise<DeleteResult> {
    validateFilter(filter);

    const docs = await this.find(filter).toArray();

    if (docs.length === 0) {
      return {
        acknowledged: true,
        deletedCount: 0,
      };
    }

    const rows: Array<{ _id: string; _seq: number; _op: 'd'; doc: T }> = [];

    for (const doc of docs) {
      const id = this.extractDocumentId(doc);

      rows.push({
        _id: id,
        _seq: ++this.branchCurrentSeq,
        _op: 'd',
        doc: {} as T,
      });
    }

    await this.writeBranchDelta(rows);

    return {
      acknowledged: true,
      deletedCount: docs.length,
    };
  }

  // --------------------------------------------------------------------------
  // Internal Methods
  // --------------------------------------------------------------------------

  /**
   * Write delta file to branch-specific storage.
   * @internal
   */
  private async writeBranchDelta(
    rows: Array<{ _id: string; _seq: number; _op: 'i' | 'u' | 'd'; doc: T }>
  ): Promise<void> {
    await this.ensureBranchManifest();

    const deltaPath = this.getBranchPath(`${this.name}_${Date.now()}_${this.branchCurrentSeq}.parquet`);
    const parquetData = writeParquet(rows);
    await this.storage.put(deltaPath, parquetData);

    // Update branch manifest
    this.branchManifest!.currentSeq = this.branchCurrentSeq;
    this.branchManifest!.updatedAt = new Date().toISOString();

    const manifestPath = this.getBranchPath(`${this.name}/_manifest.json`);
    await this.storage.put(manifestPath, new TextEncoder().encode(JSON.stringify(this.branchManifest)));
  }

  /**
   * Extract document ID as a string.
   * @internal
   */
  private extractDocumentId(doc: { _id?: unknown }): string {
    if (doc._id === undefined) {
      throw new Error('Document must have _id field');
    }
    return typeof doc._id === 'object' && doc._id !== null
      ? doc._id.toString()
      : String(doc._id);
  }
}

// ============================================================================
// Time Travel Collection
// ============================================================================

/** Options for time travel query */
export interface TimeTravelOptions {
  /** Query at specific timestamp (milliseconds since epoch) */
  timestamp?: number;
  /** Query at specific snapshot ID */
  snapshotId?: number;
}

/**
 * Read-only view of a collection at a specific point in time.
 *
 * TimeTravelCollection provides the same read APIs as Collection but
 * queries data as it existed at a specific snapshot or timestamp.
 * Write operations are not supported on time travel views.
 *
 * @example
 * ```typescript
 * // Get collection view at a specific timestamp
 * const historicalView = collection.asOf(new Date('2024-01-01'));
 *
 * // Query historical data
 * const oldDocs = await historicalView.find({ status: 'active' }).toArray();
 *
 * // Count historical documents
 * const count = await historicalView.countDocuments();
 * ```
 */
export class TimeTravelCollection<T extends Document = Document> {
  private manifest: CollectionManifest | null = null;
  private snapshot: Snapshot | null = null;
  private snapshotDataFiles: Set<string> | null = null;

  constructor(
    public readonly name: string,
    private db: Database,
    private storage: StorageBackend,
    private timeTravelOptions: TimeTravelOptions,
    private schema?: CollectionSchema
  ) {}

  // --------------------------------------------------------------------------
  // Read Operations (Same API as Collection)
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
  find(filter?: Filter<T>, options?: FindOptions): TimeTravelFindCursor<T> {
    return new TimeTravelFindCursor<T>(this, filter, options);
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
    await this.ensureSnapshot();
    // Use snapshot summary if available
    if (this.snapshot) {
      const totalRecords = this.snapshot.summary['total-records'];
      if (totalRecords) {
        return parseInt(totalRecords, 10);
      }
    }
    // Fall back to counting documents
    const docs = await this.readDocuments();
    return docs.length;
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
  ): TimeTravelAggregationCursor<R> {
    return new TimeTravelAggregationCursor<R>(this as unknown as TimeTravelCollection<Document>, pipeline, options);
  }

  /**
   * Get the snapshot this time travel view is based on.
   * Returns null if no snapshot was found for the given time/ID.
   */
  async getSnapshot(): Promise<Snapshot | null> {
    await this.ensureSnapshot();
    return this.snapshot;
  }

  /**
   * Get the timestamp of the snapshot this view is based on.
   */
  async getSnapshotTimestamp(): Promise<Date | null> {
    await this.ensureSnapshot();
    if (!this.snapshot) {
      return null;
    }
    return new Date(this.snapshot['timestamp-ms']);
  }

  /**
   * Get a sibling time travel collection from the same database at the same point in time.
   * Used internally for $lookup operations.
   * @internal
   */
  getSiblingCollection<U extends Document = Document>(name: string): TimeTravelCollection<U> {
    return new TimeTravelCollection<U>(
      name,
      this.db,
      this.storage,
      this.timeTravelOptions
    );
  }

  // --------------------------------------------------------------------------
  // Internal Methods
  // --------------------------------------------------------------------------

  /**
   * Ensure snapshot is loaded
   * @internal
   */
  private async ensureSnapshot(): Promise<void> {
    if (this.snapshot !== undefined && this.snapshotDataFiles !== null) {
      return;
    }

    // Load Iceberg metadata
    const metadataPath = `${this.db.getPath()}/${this.name}/_iceberg/metadata/v1.metadata.json`;
    const metadataData = await this.storage.get(metadataPath);

    if (!metadataData) {
      // No Iceberg metadata, fall back to manifest-based filtering
      await this.ensureManifest();
      this.snapshot = null;
      this.snapshotDataFiles = null;
      return;
    }

    try {
      const tableMetadata = JSON.parse(new TextDecoder().decode(metadataData)) as TableMetadata;
      const snapshotManager = new SnapshotManager(tableMetadata);

      // Find the target snapshot
      if (this.timeTravelOptions.snapshotId !== undefined) {
        this.snapshot = snapshotManager.getSnapshotById(this.timeTravelOptions.snapshotId) ?? null;
      } else if (this.timeTravelOptions.timestamp !== undefined) {
        this.snapshot = snapshotManager.getSnapshotAtTimestamp(this.timeTravelOptions.timestamp) ?? null;
      }

      if (this.snapshot) {
        // Load data files from the snapshot's manifest list
        await this.loadSnapshotDataFiles();
      } else {
        this.snapshotDataFiles = null;
      }
    } catch {
      // If parsing fails, fall back to timestamp-based filtering
      this.snapshot = null;
      this.snapshotDataFiles = null;
    }
  }

  /**
   * Load data files from the snapshot's manifest list
   * @internal
   */
  private async loadSnapshotDataFiles(): Promise<void> {
    if (!this.snapshot) {
      this.snapshotDataFiles = new Set();
      return;
    }

    const manifestListPath = this.snapshot['manifest-list'];
    const manifestData = await this.storage.get(manifestListPath);

    if (!manifestData) {
      // If manifest list is not found, try to parse file names from manifest path
      // or fall back to timestamp-based filtering
      this.snapshotDataFiles = null;
      return;
    }

    try {
      // Try to parse as JSON manifest list (simpler format for testing)
      const manifestList = JSON.parse(new TextDecoder().decode(manifestData));
      this.snapshotDataFiles = new Set<string>();

      // Iterate through manifest files and collect data file paths
      for (const manifest of manifestList) {
        const manifestPath = manifest['manifest-path'];
        const manifestFileData = await this.storage.get(manifestPath);

        if (manifestFileData) {
          try {
            const manifestEntries = JSON.parse(new TextDecoder().decode(manifestFileData));
            for (const entry of manifestEntries.entries || []) {
              if (entry.status !== 2) {
                // Not deleted
                this.snapshotDataFiles.add(entry['data-file']['file-path']);
              }
            }
          } catch {
            // Binary Avro format - would need Avro parser
            // For now, fall back to timestamp filtering
          }
        }
      }
    } catch {
      // Binary Avro format - fall back to timestamp filtering
      this.snapshotDataFiles = null;
    }
  }

  /**
   * Ensure manifest exists
   * @internal
   */
  private async ensureManifest(): Promise<void> {
    if (this.manifest) return;

    const manifestPath = `${this.db.getPath()}/${this.name}/_manifest.json`;
    const data = await this.storage.get(manifestPath);

    if (data) {
      this.manifest = JSON.parse(new TextDecoder().decode(data));
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
  }

  /**
   * Read all documents at the specified snapshot/timestamp
   * @internal
   */
  async readDocuments(filter?: Filter<T>, options?: FindOptions): Promise<WithId<T>[]> {
    await this.ensureSnapshot();
    await this.ensureManifest();

    // Get all Parquet files for this collection
    const dbPath = this.db.getPath();
    const collectionPrefix = `${dbPath}/${this.name}`;
    const files = await this.storage.list(dbPath);

    let parquetFiles = files.filter(
      (f) => f.startsWith(collectionPrefix) && f.endsWith('.parquet') && !f.includes('/_')
    );

    // Filter files based on snapshot or timestamp
    if (this.snapshotDataFiles !== null && this.snapshotDataFiles.size > 0) {
      // Use exact file list from snapshot manifest
      parquetFiles = parquetFiles.filter((f) => this.snapshotDataFiles!.has(f));
    } else if (this.timeTravelOptions.timestamp !== undefined) {
      // Fall back to timestamp-based filtering using file naming convention
      // Files are named: {collection}_{timestamp}_{seq}.parquet
      const targetTimestamp = this.timeTravelOptions.timestamp;
      parquetFiles = parquetFiles.filter((f) => {
        const match = f.match(/_(\d+)_\d+\.parquet$/);
        if (match) {
          const fileTimestamp = parseInt(match[1], 10);
          return fileTimestamp <= targetTimestamp;
        }
        return true; // Include files without timestamp in name
      });
    } else if (this.timeTravelOptions.snapshotId !== undefined && this.snapshot) {
      // Filter by snapshot timestamp if we found the snapshot
      const snapshotTimestamp = this.snapshot['timestamp-ms'];
      parquetFiles = parquetFiles.filter((f) => {
        const match = f.match(/_(\d+)_\d+\.parquet$/);
        if (match) {
          const fileTimestamp = parseInt(match[1], 10);
          return fileTimestamp <= snapshotTimestamp;
        }
        return true;
      });
    }

    // Read and deduplicate
    const docsById = new Map<string, { seq: number; op: string; doc: T }>();

    for (const file of parquetFiles) {
      const data = await this.storage.get(file);
      if (!data) continue;

      try {
        const rows = await readParquet<T>(data);

        for (const row of rows) {
          // For time travel, also filter by sequence number if available
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
        if (options?.skipCorruptedFiles) {
          console.warn(`Skipping corrupted Parquet file ${file}:`, error);
        } else {
          throw new Error(`Failed to read Parquet file ${file}: ${error instanceof Error ? error.message : String(error)}`);
        }
      }
    }

    // Filter out deletes and apply filter
    const results: WithId<T>[] = [];

    for (const [id, { op, doc }] of docsById) {
      if (op === 'd') continue;

      const fullDoc = { ...doc, _id: id } as WithId<T>;

      if (!filter || matchesFilter(fullDoc, filter as Filter<WithId<T>>)) {
        results.push(fullDoc);
      }
    }

    // Apply options
    let output = results;

    if (options?.sort) {
      output = sortDocuments(output, options.sort);
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

    return output;
  }
}

// ============================================================================
// Time Travel Cursors
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

export class TimeTravelAggregationCursor<T extends Document = Document> {
  constructor(
    private collection: TimeTravelCollection<Document>,
    private pipeline: AggregationStage[],
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _options?: AggregateOptions
  ) {}

  /**
   * Helper to extract field value from document using $ notation
   */
  private getFieldValue(doc: Record<string, unknown>, expr: unknown): unknown {
    if (typeof expr === 'string' && expr.startsWith('$')) {
      return getNestedValue(doc, expr.slice(1));
    }
    return expr;
  }

  /**
   * Execute and return all results
   */
  async toArray(): Promise<T[]> {
    // Get all documents first
    let docs = await this.collection.readDocuments();

    // Process pipeline stages
    for (const stage of this.pipeline) {
      docs = await this.processStage(docs, stage);
    }

    return docs as T[];
  }

  /**
   * Process a single aggregation pipeline stage
   */
  private async processStage(docs: WithId<Document>[], stage: AggregationStage): Promise<WithId<Document>[]> {
    if ('$match' in stage) {
      return docs.filter((doc) => matchesFilter(doc, stage.$match as Filter<WithId<Document>>));
    }

    if ('$sort' in stage) {
      return sortDocuments([...docs], stage.$sort);
    }

    if ('$limit' in stage) {
      return docs.slice(0, stage.$limit);
    }

    if ('$skip' in stage) {
      return docs.slice(stage.$skip);
    }

    if ('$project' in stage) {
      return docs.map((doc) => applyProjection(doc, stage.$project as { [key: string]: 0 | 1 }) as WithId<Document>);
    }

    if ('$count' in stage) {
      return [{ _id: null, [stage.$count]: docs.length } as unknown as WithId<Document>];
    }

    if ('$addFields' in stage || '$set' in stage) {
      const fieldsToAdd = '$addFields' in stage ? stage.$addFields : ('$set' in stage ? stage.$set : {});
      return docs.map((doc) => {
        const newDoc = { ...doc } as Record<string, unknown>;
        for (const [field, expr] of Object.entries(fieldsToAdd)) {
          newDoc[field] = this.getFieldValue(newDoc, expr);
        }
        return newDoc as WithId<Document>;
      });
    }

    if ('$unset' in stage) {
      const fieldsToRemove = Array.isArray(stage.$unset) ? stage.$unset : [stage.$unset];
      return docs.map((doc) => {
        const newDoc = { ...doc } as Record<string, unknown>;
        for (const field of fieldsToRemove) {
          delete newDoc[field];
        }
        return newDoc as WithId<Document>;
      });
    }

    return docs;
  }

  /**
   * Execute and iterate
   */
  async *[Symbol.asyncIterator](): AsyncIterableIterator<T> {
    const results = await this.toArray();
    for (const doc of results) {
      yield doc;
    }
  }
}

// ============================================================================
// Cursors
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

    // Convert FindOptions to CursorOptions
    const cursorOptions: CursorOptions = {
      limit: options?.limit,
      skip: options?.skip,
      sort: options?.sort,
      projection: options?.projection,
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

export class AggregationCursor<T extends Document = Document> {
  constructor(
    private collection: Collection<Document>,
    private pipeline: AggregationStage[],
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _options?: AggregateOptions
  ) {}

  /**
   * Helper to extract field value from document using $ notation
   */
  private getFieldValue(doc: Record<string, unknown>, expr: unknown): unknown {
    if (typeof expr === 'string' && expr.startsWith('$')) {
      return getNestedValue(doc, expr.slice(1));
    }
    return expr;
  }

  /**
   * Evaluate _id expression for $group stage
   */
  private evaluateGroupId(doc: Record<string, unknown>, idExpr: unknown): string {
    if (idExpr === null) {
      return '__all__';
    }

    if (typeof idExpr === 'string' && idExpr.startsWith('$')) {
      const value = getNestedValue(doc, idExpr.slice(1));
      return value === null || value === undefined ? '__null__' : String(value);
    }

    if (typeof idExpr === 'object' && idExpr !== null) {
      // Compound _id expression (e.g., { year: "$year", month: "$month" })
      const idObj: Record<string, unknown> = {};
      for (const [key, value] of Object.entries(idExpr)) {
        idObj[key] = this.getFieldValue(doc, value);
      }
      return JSON.stringify(idObj);
    }

    return String(idExpr);
  }

  /**
   * Parse compound _id back to object
   */
  private parseGroupId(groupId: string, idExpr: unknown): unknown {
    if (groupId === '__all__') {
      return null;
    }
    if (groupId === '__null__') {
      return null;
    }
    if (typeof idExpr === 'object' && idExpr !== null && !Array.isArray(idExpr)) {
      try {
        return JSON.parse(groupId);
      } catch {
        return groupId;
      }
    }
    return groupId;
  }

  /**
   * Execute and return all results
   */
  async toArray(): Promise<T[]> {
    // Get all documents first
    let docs = await this.collection.readDocuments();

    // Process pipeline stages
    for (const stage of this.pipeline) {
      docs = await this.processStage(docs, stage);
    }

    return docs as T[];
  }

  /**
   * Process a single aggregation pipeline stage
   */
  private async processStage(docs: WithId<Document>[], stage: AggregationStage): Promise<WithId<Document>[]> {
    if ('$match' in stage) {
      return this.processMatch(docs, stage.$match);
    }

    if ('$sort' in stage) {
      return this.processSort(docs, stage.$sort);
    }

    if ('$limit' in stage) {
      return docs.slice(0, stage.$limit);
    }

    if ('$skip' in stage) {
      return docs.slice(stage.$skip);
    }

    if ('$project' in stage) {
      return docs.map((doc) => applyProjection(doc, stage.$project as { [key: string]: 0 | 1 }) as WithId<Document>);
    }

    if ('$group' in stage) {
      return this.processGroup(docs, stage.$group);
    }

    if ('$unwind' in stage) {
      return this.processUnwind(docs, stage.$unwind);
    }

    if ('$lookup' in stage) {
      return await this.processLookup(docs, stage.$lookup);
    }

    if ('$count' in stage) {
      return [{ _id: null, [stage.$count]: docs.length } as unknown as WithId<Document>];
    }

    if ('$addFields' in stage || '$set' in stage) {
      const fieldsToAdd = '$addFields' in stage ? stage.$addFields : ('$set' in stage ? stage.$set : {});
      return this.processAddFields(docs, fieldsToAdd);
    }

    if ('$unset' in stage) {
      return this.processUnset(docs, stage.$unset);
    }

    return docs;
  }

  /**
   * Process $match stage
   */
  private processMatch(docs: WithId<Document>[], matchFilter: unknown): WithId<Document>[] {
    return docs.filter((doc) => matchesFilter(doc, matchFilter as Filter<WithId<Document>>));
  }

  /**
   * Process $sort stage
   */
  private processSort(docs: WithId<Document>[], sortSpec: { [key: string]: 1 | -1 }): WithId<Document>[] {
    return [...docs].sort((a, b) => {
      for (const [key, direction] of Object.entries(sortSpec)) {
        const aVal = getNestedValue(a, key) as string | number | boolean | null | undefined;
        const bVal = getNestedValue(b, key) as string | number | boolean | null | undefined;
        if (aVal != null && bVal != null && aVal < bVal) return -direction;
        if (aVal != null && bVal != null && aVal > bVal) return direction;
      }
      return 0;
    });
  }

  /**
   * Process $group stage
   */
  private processGroup(docs: WithId<Document>[], groupSpec: { _id: unknown; [key: string]: unknown }): WithId<Document>[] {
    const groups = new Map<string, Record<string, unknown>[]>();

    for (const doc of docs) {
      const groupKey = this.evaluateGroupId(doc as Record<string, unknown>, groupSpec._id);

      if (!groups.has(groupKey)) {
        groups.set(groupKey, []);
      }
      groups.get(groupKey)!.push(doc as Record<string, unknown>);
    }

    const result: WithId<Document>[] = [];
    for (const [groupId, groupDocs] of groups) {
      const groupResult: Record<string, unknown> = {
        _id: this.parseGroupId(groupId, groupSpec._id),
      };

      for (const [field, expr] of Object.entries(groupSpec)) {
        if (field === '_id') continue;
        groupResult[field] = this.evaluateAccumulator(groupDocs, expr as Record<string, unknown>);
      }

      result.push(groupResult as WithId<Document>);
    }

    return result;
  }

  /**
   * Evaluate a group accumulator expression
   */
  private evaluateAccumulator(groupDocs: Record<string, unknown>[], accExpr: Record<string, unknown>): unknown {
    if ('$sum' in accExpr) {
      if (typeof accExpr.$sum === 'number') {
        return groupDocs.length * accExpr.$sum;
      }
      const sumField = String(accExpr.$sum).replace('$', '');
      return groupDocs.reduce((sum, d) => sum + (Number(getNestedValue(d, sumField)) || 0), 0);
    }

    if ('$avg' in accExpr) {
      const avgField = String(accExpr.$avg).replace('$', '');
      const values = groupDocs.map((d) => Number(getNestedValue(d, avgField)) || 0);
      return values.length > 0 ? values.reduce((a, b) => a + b, 0) / values.length : 0;
    }

    if ('$min' in accExpr) {
      const minField = String(accExpr.$min).replace('$', '');
      const values = groupDocs.map((d) => getNestedValue(d, minField)).filter((v) => v !== undefined && v !== null);
      return values.length > 0 ? values.reduce((min, v) => (v < min ? v : min)) : null;
    }

    if ('$max' in accExpr) {
      const maxField = String(accExpr.$max).replace('$', '');
      const values = groupDocs.map((d) => getNestedValue(d, maxField)).filter((v) => v !== undefined && v !== null);
      return values.length > 0 ? values.reduce((max, v) => (v > max ? v : max)) : null;
    }

    if ('$first' in accExpr) {
      const firstField = String(accExpr.$first).replace('$', '');
      return groupDocs.length > 0 ? getNestedValue(groupDocs[0], firstField) : null;
    }

    if ('$last' in accExpr) {
      const lastField = String(accExpr.$last).replace('$', '');
      return groupDocs.length > 0 ? getNestedValue(groupDocs[groupDocs.length - 1], lastField) : null;
    }

    if ('$push' in accExpr) {
      const pushField = String(accExpr.$push).replace('$', '');
      return groupDocs.map((d) => getNestedValue(d, pushField));
    }

    if ('$addToSet' in accExpr) {
      const addToSetField = String(accExpr.$addToSet).replace('$', '');
      const values = groupDocs.map((d) => getNestedValue(d, addToSetField));
      const seen = new Set<string>();
      const uniqueValues: unknown[] = [];
      for (const v of values) {
        const key = JSON.stringify(v);
        if (!seen.has(key)) {
          seen.add(key);
          uniqueValues.push(v);
        }
      }
      return uniqueValues;
    }

    if ('$count' in accExpr) {
      return groupDocs.length;
    }

    return null;
  }

  /**
   * Process $unwind stage
   */
  private processUnwind(docs: WithId<Document>[], unwindSpec: string | { path: string; preserveNullAndEmptyArrays?: boolean }): WithId<Document>[] {
    let path: string;
    let preserveNullAndEmptyArrays = false;

    if (typeof unwindSpec === 'string') {
      path = unwindSpec.startsWith('$') ? unwindSpec.slice(1) : unwindSpec;
    } else {
      path = unwindSpec.path.startsWith('$') ? unwindSpec.path.slice(1) : unwindSpec.path;
      preserveNullAndEmptyArrays = unwindSpec.preserveNullAndEmptyArrays ?? false;
    }

    const unwoundDocs: WithId<Document>[] = [];

    for (const doc of docs) {
      const arrayValue = getNestedValue(doc as Record<string, unknown>, path);

      if (Array.isArray(arrayValue) && arrayValue.length > 0) {
        for (const item of arrayValue) {
          const newDoc = this.deepClone(doc) as Record<string, unknown>;
          this.setNestedValue(newDoc, path, item);
          unwoundDocs.push(newDoc as WithId<Document>);
        }
      } else if (preserveNullAndEmptyArrays) {
        const newDoc = this.deepClone(doc) as Record<string, unknown>;
        if (arrayValue === undefined || (Array.isArray(arrayValue) && arrayValue.length === 0)) {
          this.setNestedValue(newDoc, path, null);
        }
        unwoundDocs.push(newDoc as WithId<Document>);
      }
    }

    return unwoundDocs;
  }

  /**
   * Process $lookup stage
   */
  private async processLookup(
    docs: WithId<Document>[],
    lookupSpec: { from: string; localField?: string; foreignField?: string; as: string; let?: Record<string, unknown>; pipeline?: AggregationStage[] }
  ): Promise<WithId<Document>[]> {
    const { from, localField, foreignField, as } = lookupSpec;

    const foreignCollection = this.collection.getSiblingCollection<Document>(from);
    const foreignDocs = await foreignCollection.readDocuments();

    if (lookupSpec.pipeline) {
      return this.processLookupWithPipeline(docs, foreignDocs, lookupSpec.let || {}, lookupSpec.pipeline, as);
    } else if (localField && foreignField) {
      return this.processLookupEquality(docs, foreignDocs, localField, foreignField, as);
    } else {
      return docs.map((doc) => {
        (doc as Record<string, unknown>)[as] = [];
        return doc;
      });
    }
  }

  /**
   * Process $lookup with pipeline
   */
  private processLookupWithPipeline(
    docs: WithId<Document>[],
    foreignDocs: WithId<Document>[],
    letVars: Record<string, unknown>,
    pipeline: AggregationStage[],
    as: string
  ): WithId<Document>[] {
    for (let i = 0; i < docs.length; i++) {
      const doc = docs[i] as Record<string, unknown>;

      const varContext: Record<string, unknown> = {};
      for (const [varName, varExpr] of Object.entries(letVars)) {
        varContext[varName] = this.getFieldValue(doc, varExpr);
      }

      let pipelineDocs = [...foreignDocs] as Document[];

      for (const pipelineStage of pipeline) {
        if ('$match' in pipelineStage) {
          const resolvedMatch = this.resolveVariables(pipelineStage.$match, varContext);
          pipelineDocs = pipelineDocs.filter((d) => matchesFilter(d as WithId<Document>, resolvedMatch as Filter<WithId<Document>>));
        }
      }

      (doc as Record<string, unknown>)[as] = pipelineDocs;
      docs[i] = doc as WithId<Document>;
    }

    return docs;
  }

  /**
   * Process $lookup with equality match
   */
  private processLookupEquality(
    docs: WithId<Document>[],
    foreignDocs: WithId<Document>[],
    localField: string,
    foreignField: string,
    as: string
  ): WithId<Document>[] {
    for (let i = 0; i < docs.length; i++) {
      const doc = docs[i] as Record<string, unknown>;
      const localValue = getNestedValue(doc, localField);

      const matchingDocs = foreignDocs.filter((foreignDoc) => {
        const foreignValue = getNestedValue(foreignDoc as Record<string, unknown>, foreignField);
        return localValue === foreignValue;
      });

      (doc as Record<string, unknown>)[as] = matchingDocs;
      docs[i] = doc as WithId<Document>;
    }

    return docs;
  }

  /**
   * Process $addFields or $set stage
   */
  private processAddFields(docs: WithId<Document>[], fieldsToAdd: Record<string, unknown>): WithId<Document>[] {
    return docs.map((doc) => {
      const newDoc = { ...doc } as Record<string, unknown>;
      for (const [field, expr] of Object.entries(fieldsToAdd)) {
        newDoc[field] = this.getFieldValue(newDoc, expr);
      }
      return newDoc as WithId<Document>;
    });
  }

  /**
   * Process $unset stage
   */
  private processUnset(docs: WithId<Document>[], unsetSpec: string | string[]): WithId<Document>[] {
    const fieldsToRemove = Array.isArray(unsetSpec) ? unsetSpec : [unsetSpec];
    return docs.map((doc) => {
      const newDoc = { ...doc } as Record<string, unknown>;
      for (const field of fieldsToRemove) {
        delete newDoc[field];
      }
      return newDoc as WithId<Document>;
    });
  }

  /**
   * Deep clone an object
   */
  private deepClone<U>(obj: U): U {
    if (obj === null || typeof obj !== 'object') {
      return obj;
    }

    if (Array.isArray(obj)) {
      return obj.map((item) => this.deepClone(item)) as U;
    }

    if (obj instanceof Date) {
      return new Date(obj.getTime()) as U;
    }

    if (obj instanceof Uint8Array) {
      return new Uint8Array(obj) as U;
    }

    const cloned: Record<string, unknown> = {};
    for (const key in obj) {
      if (Object.prototype.hasOwnProperty.call(obj, key)) {
        cloned[key] = this.deepClone((obj as Record<string, unknown>)[key]);
      }
    }
    return cloned as U;
  }

  /**
   * Set a nested value in an object using dot notation
   */
  private setNestedValue(obj: Record<string, unknown>, path: string, value: unknown): void {
    const parts = path.split('.');
    let current = obj;

    for (let i = 0; i < parts.length - 1; i++) {
      const part = parts[i];
      if (!(part in current) || typeof current[part] !== 'object' || current[part] === null) {
        current[part] = {};
      }
      current = current[part] as Record<string, unknown>;
    }

    current[parts[parts.length - 1]] = value;
  }

  /**
   * Resolve $$variables in an expression
   */
  private resolveVariables(expr: unknown, varContext: Record<string, unknown>): unknown {
    if (typeof expr === 'string' && expr.startsWith('$$')) {
      const varName = expr.slice(2);
      return varContext[varName];
    }

    if (typeof expr === 'object' && expr !== null) {
      if (Array.isArray(expr)) {
        return expr.map((item) => this.resolveVariables(item, varContext));
      }

      const resolved: Record<string, unknown> = {};
      for (const [key, value] of Object.entries(expr)) {
        if (key === '$expr') {
          resolved[key] = this.resolveExprVariables(value, varContext);
        } else {
          resolved[key] = this.resolveVariables(value, varContext);
        }
      }
      return resolved;
    }

    return expr;
  }

  /**
   * Resolve $$variables in $expr expressions
   */
  private resolveExprVariables(expr: unknown, varContext: Record<string, unknown>): unknown {
    if (typeof expr === 'string' && expr.startsWith('$$')) {
      const varName = expr.slice(2);
      return varContext[varName];
    }

    if (typeof expr === 'object' && expr !== null) {
      if (Array.isArray(expr)) {
        return expr.map((item) => this.resolveExprVariables(item, varContext));
      }

      const resolved: Record<string, unknown> = {};
      for (const [key, value] of Object.entries(expr)) {
        resolved[key] = this.resolveExprVariables(value, varContext);
      }
      return resolved;
    }

    return expr;
  }

  /**
   * Execute and iterate
   */
  async *[Symbol.asyncIterator](): AsyncIterableIterator<T> {
    const results = await this.toArray();
    for (const doc of results) {
      yield doc;
    }
  }
}

// ============================================================================
// Exports
// ============================================================================

export { ObjectId } from '../types.js';
export {
  ValidationError,
  validateDatabaseName,
  validateCollectionName,
  validateFilter,
  validateProjection,
  validateUpdate,
  validateDocument,
  validateAggregationPipeline,
  validateFieldName,
  validateInputs,
  VALID_QUERY_OPERATORS,
  VALID_UPDATE_OPERATORS,
  VALID_AGGREGATION_STAGES,
} from '../utils/validation.js';
export { ChangeStream, computeUpdateDescription, createChangeStream } from '../change-stream/index.js';
export type {
  OperationType,
  ResumeToken,
  UpdateDescription,
  ChangeStreamNamespace,
  ChangeStreamDocument,
  ChangeStreamOptions,
  ChangeEventHandler,
} from '../change-stream/index.js';
export type {
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
  MongoLakeConfig,
} from '../types.js';

// Cursor exports
export { Cursor, CursorStore, generateCursorId } from '../cursor/index.js';
export type { CursorOptions, CursorState, DocumentSource } from '../cursor/index.js';

// Session and Transaction exports
export {
  ClientSession,
  SessionStore,
  TransactionError,
  SessionError,
  generateSessionId,
  hasSession,
  extractSession,
} from '../session/index.js';
export type {
  TransactionState,
  ReadConcernLevel,
  WriteConcern,
  TransactionOptions,
  SessionOptions,
  SessionOperationOptions,
  BufferedOperation,
  SessionId,
} from '../session/index.js';

// Transaction Manager exports
export {
  TransactionManager,
  runTransaction,
} from '../transaction/index.js';
export type {
  TransactionWrite,
  TransactionSnapshot,
  TransactionCommitResult,
  RunTransactionOptions,
} from '../transaction/index.js';
