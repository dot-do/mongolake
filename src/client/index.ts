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
import { createStorage, type StorageBackend } from '../storage/index.js';

// ============================================================================
// Singleton & Factory
// ============================================================================

let defaultClient: MongoLake | null = null;

/**
 * Get a database using default configuration
 *
 * @example
 * ```typescript
 * import { db } from 'mongolake';
 *
 * const users = db('myapp').collection('users');
 * await users.insertOne({ name: 'Alice' });
 * ```
 */
export function db(name?: string): Database {
  if (!defaultClient) {
    // Auto-detect configuration
    defaultClient = new MongoLake({
      // Will use .mongolake by default in Node.js
      // Will need bucket binding in Workers
    });
  }
  return defaultClient.db(name);
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

  constructor(config: MongoLakeConfig = {}) {
    this.config = {
      database: 'default',
      ...config,
    };
    this.storage = createStorage(this.config);
  }

  /**
   * Get a database
   */
  db(name?: string): Database {
    const dbName = name || this.config.database || 'default';

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
// Database
// ============================================================================

export class Database {
  private collections: Map<string, Collection<Document>> = new Map();

  constructor(
    public readonly name: string,
    private storage: StorageBackend,
    private config: MongoLakeConfig
  ) {}

  /**
   * Get a collection
   */
  collection<T extends Document = Document>(name: string): Collection<T> {
    if (!this.collections.has(name)) {
      const schema = this.config.schema?.[name];
      this.collections.set(name, new Collection<T>(name, this, this.storage, schema));
    }
    return this.collections.get(name) as Collection<T>;
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
    options?: { schema?: CollectionSchema }
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
  async branch(branchName: string): Promise<void> {
    // TODO: Implement branching
    throw new Error('Branching not yet implemented');
  }

  /**
   * Merge a branch
   */
  async merge(branchName: string): Promise<void> {
    // TODO: Implement merging
    throw new Error('Merging not yet implemented');
  }

  /**
   * Get storage path for this database
   */
  getPath(): string {
    return this.name;
  }
}

// ============================================================================
// Collection
// ============================================================================

export class Collection<T extends Document = Document> {
  private manifest: CollectionManifest | null = null;
  private currentSeq: number = 0;

  constructor(
    public readonly name: string,
    private db: Database,
    private storage: StorageBackend,
    private schema?: CollectionSchema
  ) {}

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
    await this.ensureManifest();

    const insertedIds: { [key: number]: string | ObjectId } = {};
    const rows: Array<{ _id: string; _seq: number; _op: 'i'; doc: T }> = [];

    for (let i = 0; i < docs.length; i++) {
      const doc = { ...docs[i] };

      // Generate _id if not provided
      if (!doc._id) {
        doc._id = crypto.randomUUID();
      }

      const id = typeof doc._id === 'object' ? doc._id.toString() : String(doc._id);
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
    const doc = await this.findOne(filter);

    if (!doc) {
      if (options?.upsert) {
        // Upsert: create new document
        const newDoc = this.applyUpdate({} as T, update);
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

    const updated = this.applyUpdate(doc, update);
    const id = typeof doc._id === 'object' ? doc._id.toString() : String(doc._id);

    await this.writeDelta([
      {
        _id: id,
        _seq: ++this.currentSeq,
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
   * Update multiple documents
   */
  async updateMany(filter: Filter<T>, update: Update<T>, options?: UpdateOptions): Promise<UpdateResult> {
    const docs = await this.find(filter).toArray();

    if (docs.length === 0) {
      if (options?.upsert) {
        const newDoc = this.applyUpdate({} as T, update);
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
      const updated = this.applyUpdate(doc, update);
      const id = typeof doc._id === 'object' ? doc._id.toString() : String(doc._id);

      rows.push({
        _id: id,
        _seq: ++this.currentSeq,
        _op: 'u',
        doc: updated,
      });
    }

    await this.writeDelta(rows);

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

    const id = typeof doc._id === 'object' ? doc._id.toString() : String(doc._id);
    const newDoc = { ...replacement, _id: doc._id };

    await this.writeDelta([
      {
        _id: id,
        _seq: ++this.currentSeq,
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
   * Delete a single document
   */
  async deleteOne(filter: Filter<T>, options?: DeleteOptions): Promise<DeleteResult> {
    const doc = await this.findOne(filter);

    if (!doc) {
      return {
        acknowledged: true,
        deletedCount: 0,
      };
    }

    const id = typeof doc._id === 'object' ? doc._id.toString() : String(doc._id);

    await this.writeDelta([
      {
        _id: id,
        _seq: ++this.currentSeq,
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
   * Delete multiple documents
   */
  async deleteMany(filter: Filter<T>, options?: DeleteOptions): Promise<DeleteResult> {
    const docs = await this.find(filter).toArray();

    if (docs.length === 0) {
      return {
        acknowledged: true,
        deletedCount: 0,
      };
    }

    const rows: Array<{ _id: string; _seq: number; _op: 'd'; doc: T }> = [];

    for (const doc of docs) {
      const id = typeof doc._id === 'object' ? doc._id.toString() : String(doc._id);

      rows.push({
        _id: id,
        _seq: ++this.currentSeq,
        _op: 'd',
        doc: {} as T,
      });
    }

    await this.writeDelta(rows);

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
  async distinct<K extends keyof T>(field: K, filter?: Filter<T>): Promise<T[K][]> {
    const docs = await this.find(filter).toArray();
    const values = new Set<T[K]>();

    for (const doc of docs) {
      const value = doc[field];
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
    return new AggregationCursor<R>(this as unknown as Collection<Document>, pipeline, options);
  }

  // --------------------------------------------------------------------------
  // Index Operations
  // --------------------------------------------------------------------------

  /**
   * Create an index
   */
  async createIndex(spec: IndexSpec, options?: IndexOptions): Promise<string> {
    // TODO: Implement indexes
    const name = options?.name || Object.keys(spec).join('_');
    console.warn(`Index creation not yet implemented: ${name}`);
    return name;
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
    // TODO: Implement
    console.warn(`Index drop not yet implemented: ${name}`);
  }

  /**
   * List indexes
   */
  async listIndexes(): Promise<Array<{ name: string; key: IndexSpec }>> {
    // TODO: Implement
    return [{ name: '_id_', key: { _id: 1 } }];
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
  }

  /**
   * Read all documents (internal)
   * @internal
   */
  async readDocuments(filter?: Filter<T>, options?: FindOptions): Promise<WithId<T>[]> {
    await this.ensureManifest();

    // Get all Parquet files for this collection
    const prefix = `${this.db.getPath()}/${this.name}`;
    const files = await this.storage.list(prefix);

    const parquetFiles = files.filter(
      (f) => f.endsWith('.parquet') && !f.includes('/_')
    );

    // Read and deduplicate
    const docsById = new Map<string, { seq: number; op: string; doc: T }>();

    for (const file of parquetFiles) {
      const data = await this.storage.get(file);
      if (!data) continue;

      // TODO: Implement proper Parquet reading
      // For now, use JSON fallback stored as .parquet
      try {
        const rows = JSON.parse(new TextDecoder().decode(data)) as Array<{
          _id: string;
          _seq: number;
          _op: string;
          _data: T;
        }>;

        for (const row of rows) {
          const existing = docsById.get(row._id);
          if (!existing || row._seq > existing.seq) {
            docsById.set(row._id, {
              seq: row._seq,
              op: row._op,
              doc: row._data,
            });
          }
        }
      } catch {
        // Skip invalid files
      }
    }

    // Filter out deletes and apply filter
    const results: WithId<T>[] = [];

    for (const [id, { op, doc }] of docsById) {
      if (op === 'd') continue;

      const fullDoc = { ...doc, _id: id } as WithId<T>;

      if (!filter || this.matchesFilter(fullDoc, filter)) {
        results.push(fullDoc);
      }
    }

    // Apply options
    let output = results;

    if (options?.sort) {
      output = this.sortDocuments(output, options.sort);
    }

    if (options?.skip) {
      output = output.slice(options.skip);
    }

    if (options?.limit) {
      output = output.slice(0, options.limit);
    }

    if (options?.projection) {
      output = output.map((doc) => this.applyProjection(doc, options.projection!));
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

    const deltaPath = `${this.db.getPath()}/${this.name}_${Date.now()}.parquet`;

    // TODO: Implement proper Parquet writing
    // For now, use JSON fallback
    const data = JSON.stringify(
      rows.map((r) => ({
        _id: r._id,
        _seq: r._seq,
        _op: r._op,
        _data: r.doc,
      }))
    );

    await this.storage.put(deltaPath, new TextEncoder().encode(data));

    // Update manifest
    this.manifest!.currentSeq = this.currentSeq;
    this.manifest!.updatedAt = new Date().toISOString();

    const manifestPath = `${this.db.getPath()}/${this.name}/_manifest.json`;
    await this.storage.put(manifestPath, new TextEncoder().encode(JSON.stringify(this.manifest)));
  }

  /**
   * Check if document matches filter
   * @internal
   */
  private matchesFilter(doc: WithId<T>, filter: Filter<T>): boolean {
    for (const [key, condition] of Object.entries(filter)) {
      if (key === '$and') {
        if (!((condition as Filter<T>[]).every((f) => this.matchesFilter(doc, f)))) {
          return false;
        }
        continue;
      }

      if (key === '$or') {
        if (!((condition as Filter<T>[]).some((f) => this.matchesFilter(doc, f)))) {
          return false;
        }
        continue;
      }

      const value = (doc as Record<string, unknown>)[key];

      if (typeof condition === 'object' && condition !== null && !Array.isArray(condition)) {
        // Operator condition
        const ops = condition as Record<string, unknown>;

        if ('$eq' in ops && value !== ops.$eq) return false;
        if ('$ne' in ops && value === ops.$ne) return false;
        if ('$gt' in ops && !(value > (ops.$gt as number))) return false;
        if ('$gte' in ops && !(value >= (ops.$gte as number))) return false;
        if ('$lt' in ops && !(value < (ops.$lt as number))) return false;
        if ('$lte' in ops && !(value <= (ops.$lte as number))) return false;
        if ('$in' in ops && !(ops.$in as unknown[]).includes(value)) return false;
        if ('$nin' in ops && (ops.$nin as unknown[]).includes(value)) return false;
        if ('$exists' in ops) {
          const exists = value !== undefined;
          if (ops.$exists !== exists) return false;
        }
      } else {
        // Direct equality
        if (value !== condition) return false;
      }
    }

    return true;
  }

  /**
   * Sort documents
   * @internal
   */
  private sortDocuments(docs: WithId<T>[], sort: { [key: string]: 1 | -1 }): WithId<T>[] {
    return [...docs].sort((a, b) => {
      for (const [key, direction] of Object.entries(sort)) {
        const aVal = (a as Record<string, unknown>)[key];
        const bVal = (b as Record<string, unknown>)[key];

        if (aVal < bVal) return -direction;
        if (aVal > bVal) return direction;
      }
      return 0;
    });
  }

  /**
   * Apply projection
   * @internal
   */
  private applyProjection(doc: WithId<T>, projection: { [key: string]: 0 | 1 }): WithId<T> {
    const hasInclusions = Object.values(projection).some((v) => v === 1);

    if (hasInclusions) {
      // Inclusion mode
      const result: Record<string, unknown> = { _id: doc._id };
      for (const [key, include] of Object.entries(projection)) {
        if (include === 1 && key !== '_id') {
          result[key] = (doc as Record<string, unknown>)[key];
        }
      }
      return result as WithId<T>;
    } else {
      // Exclusion mode
      const result = { ...doc };
      for (const [key, exclude] of Object.entries(projection)) {
        if (exclude === 0) {
          delete (result as Record<string, unknown>)[key];
        }
      }
      return result;
    }
  }

  /**
   * Apply update operators
   * @internal
   */
  private applyUpdate(doc: T, update: Update<T>): T {
    const result = { ...doc };

    if (update.$set) {
      Object.assign(result, update.$set);
    }

    if (update.$unset) {
      for (const key of Object.keys(update.$unset)) {
        delete (result as Record<string, unknown>)[key];
      }
    }

    if (update.$inc) {
      for (const [key, amount] of Object.entries(update.$inc)) {
        const current = ((result as Record<string, unknown>)[key] as number) || 0;
        (result as Record<string, unknown>)[key] = current + (amount as number);
      }
    }

    if (update.$push) {
      for (const [key, value] of Object.entries(update.$push)) {
        const arr = ((result as Record<string, unknown>)[key] as unknown[]) || [];
        if (typeof value === 'object' && value !== null && '$each' in value) {
          arr.push(...(value.$each as unknown[]));
        } else {
          arr.push(value);
        }
        (result as Record<string, unknown>)[key] = arr;
      }
    }

    if (update.$pull) {
      for (const [key, value] of Object.entries(update.$pull)) {
        const arr = ((result as Record<string, unknown>)[key] as unknown[]) || [];
        (result as Record<string, unknown>)[key] = arr.filter((item) => item !== value);
      }
    }

    if (update.$addToSet) {
      for (const [key, value] of Object.entries(update.$addToSet)) {
        const arr = ((result as Record<string, unknown>)[key] as unknown[]) || [];
        const values = typeof value === 'object' && value !== null && '$each' in value
          ? (value.$each as unknown[])
          : [value];
        for (const v of values) {
          if (!arr.includes(v)) {
            arr.push(v);
          }
        }
        (result as Record<string, unknown>)[key] = arr;
      }
    }

    return result;
  }
}

// ============================================================================
// Cursors
// ============================================================================

export class FindCursor<T extends Document = Document> {
  private _filter?: Filter<T>;
  private _options: FindOptions;
  private _executed: boolean = false;
  private _results: WithId<T>[] = [];

  constructor(
    private collection: Collection<T>,
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

export class AggregationCursor<T extends Document = Document> {
  constructor(
    private collection: Collection<Document>,
    private pipeline: AggregationStage[],
    private options?: AggregateOptions
  ) {}

  /**
   * Execute and return all results
   */
  async toArray(): Promise<T[]> {
    // Get all documents first
    let docs = await this.collection.readDocuments();

    // Process pipeline stages
    for (const stage of this.pipeline) {
      if ('$match' in stage) {
        docs = docs.filter((doc) => {
          // Simplified filter matching
          for (const [key, value] of Object.entries(stage.$match)) {
            if ((doc as Record<string, unknown>)[key] !== value) return false;
          }
          return true;
        });
      }

      if ('$sort' in stage) {
        docs = [...docs].sort((a, b) => {
          for (const [key, direction] of Object.entries(stage.$sort)) {
            const aVal = (a as Record<string, unknown>)[key];
            const bVal = (b as Record<string, unknown>)[key];
            if (aVal < bVal) return -direction;
            if (aVal > bVal) return direction;
          }
          return 0;
        });
      }

      if ('$limit' in stage) {
        docs = docs.slice(0, stage.$limit);
      }

      if ('$skip' in stage) {
        docs = docs.slice(stage.$skip);
      }

      if ('$group' in stage) {
        const groups = new Map<string, Record<string, unknown>[]>();

        for (const doc of docs) {
          const groupKey =
            stage.$group._id === null
              ? '__all__'
              : String((doc as Record<string, unknown>)[String(stage.$group._id).replace('$', '')]);

          if (!groups.has(groupKey)) {
            groups.set(groupKey, []);
          }
          groups.get(groupKey)!.push(doc as Record<string, unknown>);
        }

        docs = [];
        for (const [groupId, groupDocs] of groups) {
          const result: Record<string, unknown> = {
            _id: groupId === '__all__' ? null : groupId,
          };

          for (const [field, expr] of Object.entries(stage.$group)) {
            if (field === '_id') continue;

            const accExpr = expr as Record<string, unknown>;
            if ('$sum' in accExpr) {
              const sumField = String(accExpr.$sum).replace('$', '');
              result[field] =
                accExpr.$sum === 1
                  ? groupDocs.length
                  : groupDocs.reduce((sum, d) => sum + (Number(d[sumField]) || 0), 0);
            }
            if ('$avg' in accExpr) {
              const avgField = String(accExpr.$avg).replace('$', '');
              const values = groupDocs.map((d) => Number(d[avgField]) || 0);
              result[field] = values.reduce((a, b) => a + b, 0) / values.length;
            }
            if ('$min' in accExpr) {
              const minField = String(accExpr.$min).replace('$', '');
              result[field] = Math.min(...groupDocs.map((d) => Number(d[minField])));
            }
            if ('$max' in accExpr) {
              const maxField = String(accExpr.$max).replace('$', '');
              result[field] = Math.max(...groupDocs.map((d) => Number(d[maxField])));
            }
            if ('$count' in accExpr) {
              result[field] = groupDocs.length;
            }
          }

          docs.push(result as WithId<Document>);
        }
      }

      if ('$count' in stage) {
        docs = [{ _id: null, [stage.$count]: docs.length } as WithId<Document>];
      }
    }

    return docs as T[];
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
