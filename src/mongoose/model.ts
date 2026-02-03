/**
 * MongoLake Mongoose Model
 *
 * Provides Mongoose-compatible Model class with:
 * - Document creation and management
 * - CRUD operations
 * - Population / references
 * - Query building
 * - Middleware execution
 */

import { Schema, type SchemaDefinition } from './schema.js';
import { MongooseDocument } from './document.js';
import { Query } from './query.js';
import { ObjectId, type Document as BaseDocument, type Filter, type Update } from '../types.js';
import type { Collection } from '../client/index.js';

// ============================================================================
// Model Types
// ============================================================================

/**
 * Model query result type
 */
export type QueryResult<T> = T | T[] | null;

/**
 * Model options
 */
export interface ModelOptions {
  collection?: string;
  connection?: ModelConnection;
  skipInit?: boolean;
}

/**
 * Connection interface for models
 */
export interface ModelConnection {
  collection<T extends BaseDocument = BaseDocument>(name: string): Collection<T>;
}

/**
 * Population options
 */
export interface PopulateOptions {
  path: string;
  select?: string | Record<string, 0 | 1>;
  match?: Filter<BaseDocument>;
  model?: string | Model<BaseDocument>;
  options?: Record<string, unknown>;
  populate?: PopulateOptions | PopulateOptions[] | string;
  justOne?: boolean;
  localField?: string;
  foreignField?: string;
  perDocumentLimit?: number;
}

/**
 * Lean options
 */
export interface LeanOptions {
  virtuals?: boolean;
  getters?: boolean;
  defaults?: boolean;
  transform?: (doc: unknown) => unknown;
}

/**
 * Create options for Model.create()
 */
export interface CreateOptions {
  validateBeforeSave?: boolean;
}

/**
 * Save options
 */
export interface SaveOptions {
  validateBeforeSave?: boolean;
  validateModifiedOnly?: boolean;
  timestamps?: boolean;
  session?: unknown;
}

/**
 * Aggregate options
 */
export interface AggregateOptions {
  allowDiskUse?: boolean;
  maxTimeMS?: number;
  readPreference?: string;
  session?: unknown;
}

// ============================================================================
// Model Factory
// ============================================================================

/**
 * Model registry for population lookups
 */
const modelRegistry = new Map<string, Model<BaseDocument>>();

/**
 * Get a registered model by name
 */
export function getModel(name: string): Model<BaseDocument> | undefined {
  return modelRegistry.get(name);
}

/**
 * Register a model
 */
export function registerModel(name: string, model: Model<BaseDocument>): void {
  modelRegistry.set(name, model);
}

/**
 * Get all registered model names
 */
export function modelNames(): string[] {
  return Array.from(modelRegistry.keys());
}

/**
 * Delete a model from the registry
 */
export function deleteModel(name: string): boolean {
  return modelRegistry.delete(name);
}

// ============================================================================
// Model Class
// ============================================================================

/**
 * Mongoose-compatible Model class
 */
export class Model<T extends BaseDocument = BaseDocument> {
  public readonly modelName: string;
  public readonly schema: Schema<T>;
  public readonly collection: Collection<T>;

  private _connection?: ModelConnection;
  private _discriminators: Map<string, Model<T>> = new Map();

  constructor(name: string, schema: Schema<T>, options?: ModelOptions) {
    this.modelName = name;
    this.schema = schema;

    // Get collection name
    const collectionName = options?.collection || schema.options.collection || name.toLowerCase() + 's';

    // Get connection and collection
    this._connection = options?.connection;
    if (this._connection) {
      this.collection = this._connection.collection<T>(collectionName);
    } else {
      // Create a stub collection for when no connection is provided
      this.collection = this.createStubCollection(collectionName);
    }

    // Register the model. Model<T> where T extends BaseDocument should be assignable to Model<BaseDocument>,
    // but TypeScript's strict variance checking requires the double cast.
    registerModel(name, this as unknown as Model<BaseDocument>);

    // Apply static methods from schema
    for (const [methodName, fn] of schema.statics) {
      (this as unknown as Record<string, Function>)[methodName] = fn.bind(this);
    }
  }

  private createStubCollection(name: string): Collection<T> {
    // Create a stub that throws helpful errors
    const stub = {
      name,
      insertOne: async () => { throw new Error('No connection. Call mongoose.connect() first.'); },
      insertMany: async () => { throw new Error('No connection. Call mongoose.connect() first.'); },
      findOne: async () => { throw new Error('No connection. Call mongoose.connect() first.'); },
      find: () => { throw new Error('No connection. Call mongoose.connect() first.'); },
      updateOne: async () => { throw new Error('No connection. Call mongoose.connect() first.'); },
      updateMany: async () => { throw new Error('No connection. Call mongoose.connect() first.'); },
      deleteOne: async () => { throw new Error('No connection. Call mongoose.connect() first.'); },
      deleteMany: async () => { throw new Error('No connection. Call mongoose.connect() first.'); },
      countDocuments: async () => { throw new Error('No connection. Call mongoose.connect() first.'); },
      aggregate: () => { throw new Error('No connection. Call mongoose.connect() first.'); },
    };
    return stub as unknown as Collection<T>;
  }

  /**
   * Create a new document instance
   */
  new(doc?: Partial<T>): MongooseDocument<T> & T {
    return this.createDocument(doc);
  }

  /**
   * Create a document instance (internal)
   */
  private createDocument(doc?: Partial<T>): MongooseDocument<T> & T {
    const instance = new MongooseDocument<T>(doc, this.schema, this);

    // Apply instance methods from schema
    for (const [name, fn] of this.schema.methods) {
      (instance as unknown as Record<string, Function>)[name] = fn.bind(instance);
    }

    // Create a proxy to handle both document methods and data access
    return new Proxy(instance, {
      get(target, prop: string | symbol) {
        // Handle symbol properties
        if (typeof prop === 'symbol') {
          return (target as unknown as Record<symbol, unknown>)[prop];
        }

        // Check for document methods first
        if (prop in target) {
          const value = (target as unknown as Record<string, unknown>)[prop];
          if (typeof value === 'function') {
            return value.bind(target);
          }
          return value;
        }

        // Check for virtuals
        const virtual = target.schema.virtuals.get(prop);
        if (virtual && virtual.hasGetter()) {
          return virtual.applyGetters(target._doc);
        }

        // Get from document data
        return target.get(prop);
      },
      set(target, prop: string | symbol, value) {
        if (typeof prop === 'symbol') {
          (target as unknown as Record<symbol, unknown>)[prop] = value;
          return true;
        }

        // Allow setting document instance properties directly
        const documentProps = ['isNew', 'errors', '$op', '$locals', '$where', '_doc'];
        if (documentProps.includes(prop)) {
          (target as unknown as Record<string, unknown>)[prop] = value;
          return true;
        }

        // Check for virtuals
        const virtual = target.schema.virtuals.get(prop);
        if (virtual && virtual.hasSetter()) {
          virtual.applySetters(target._doc, value);
          return true;
        }

        // Set in document data
        target.set(prop, value);
        return true;
      },
      has(target, prop: string | symbol) {
        if (typeof prop === 'symbol') return false;
        return prop in target || target.schema.paths.has(prop) || target.schema.virtuals.has(prop);
      },
      ownKeys(target) {
        return [...Object.keys(target._doc), ...target.schema.virtuals.keys()];
      },
      getOwnPropertyDescriptor(target, prop: string | symbol) {
        if (typeof prop === 'symbol') return undefined;
        if (prop in target._doc || target.schema.virtuals.has(prop)) {
          return { configurable: true, enumerable: true, writable: true };
        }
        return undefined;
      },
    }) as unknown as MongooseDocument<T> & T;
  }

  // ============================================================================
  // Static CRUD Methods
  // ============================================================================

  /**
   * Create one or more documents
   */
  async create(doc: Partial<T>, options?: CreateOptions): Promise<MongooseDocument<T> & T>;
  async create(docs: Partial<T>[], options?: CreateOptions): Promise<Array<MongooseDocument<T> & T>>;
  async create(
    docOrDocs: Partial<T> | Partial<T>[],
    options?: CreateOptions
  ): Promise<MongooseDocument<T> & T | Array<MongooseDocument<T> & T>> {
    const validateBeforeSave = options?.validateBeforeSave ?? true;

    if (Array.isArray(docOrDocs)) {
      const documents = docOrDocs.map((d) => this.createDocument(d));

      // Run pre middleware
      for (const doc of documents) {
        await this.schema.runPreMiddleware('save', doc);

        // Validate
        if (validateBeforeSave) {
          await doc.validate();
        }
      }

      // Insert all
      const rawDocs = documents.map((d) => d.toObject() as T);
      await this.collection.insertMany(rawDocs);

      // Mark as saved and run post middleware
      for (const doc of documents) {
        doc.isNew = false;
        await this.schema.runPostMiddleware('save', doc, doc);
      }

      return documents;
    } else {
      const document = this.createDocument(docOrDocs);

      // Run pre middleware
      await this.schema.runPreMiddleware('save', document);

      // Validate
      if (validateBeforeSave) {
        await document.validate();
      }

      // Insert
      await this.collection.insertOne(document.toObject() as T);

      // Mark as saved and run post middleware
      document.isNew = false;
      await this.schema.runPostMiddleware('save', document, document);

      return document;
    }
  }

  /**
   * Insert many documents (bypasses validation and middleware)
   */
  async insertMany(docs: Partial<T>[], _options?: { ordered?: boolean; rawResult?: boolean }): Promise<Array<MongooseDocument<T> & T>> {
    // Run insertMany pre middleware
    await this.schema.runPreMiddleware('insertMany', docs);

    // Apply defaults and create documents
    const documents = docs.map((d) => {
      const doc = this.createDocument(d);
      return doc;
    });

    // Insert raw documents
    const rawDocs = documents.map((d) => d.toObject() as T);
    await this.collection.insertMany(rawDocs);

    // Mark all as saved
    for (const doc of documents) {
      doc.isNew = false;
    }

    return documents;
  }

  /**
   * Find documents
   */
  find(filter?: Filter<T>, projection?: Record<string, 0 | 1>, options?: Record<string, unknown>): Query<T[], T> {
    return new Query<T[], T>(this, 'find', filter, projection, options);
  }

  /**
   * Find one document
   */
  findOne(filter?: Filter<T>, projection?: Record<string, 0 | 1>, options?: Record<string, unknown>): Query<T | null, T> {
    return new Query<T | null, T>(this, 'findOne', filter, projection, options);
  }

  /**
   * Find by ID
   */
  findById(id: string | ObjectId, projection?: Record<string, 0 | 1>, options?: Record<string, unknown>): Query<T | null, T> {
    const filter = { _id: id instanceof ObjectId ? id : new ObjectId(id) } as unknown as Filter<T>;
    return new Query<T | null, T>(this, 'findOne', filter, projection, options);
  }

  /**
   * Find one and update
   */
  findOneAndUpdate(
    filter: Filter<T>,
    update: Update<T>,
    options?: { new?: boolean; upsert?: boolean; projection?: Record<string, 0 | 1>; sort?: Record<string, 1 | -1> }
  ): Query<T | null, T> {
    return new Query<T | null, T>(this, 'findOneAndUpdate', filter, options?.projection, { ...options, update });
  }

  /**
   * Find by ID and update
   */
  findByIdAndUpdate(
    id: string | ObjectId,
    update: Update<T>,
    options?: { new?: boolean; upsert?: boolean; projection?: Record<string, 0 | 1> }
  ): Query<T | null, T> {
    const filter = { _id: id instanceof ObjectId ? id : new ObjectId(id) } as unknown as Filter<T>;
    return new Query<T | null, T>(this, 'findOneAndUpdate', filter, options?.projection, { ...options, update });
  }

  /**
   * Find one and delete
   */
  findOneAndDelete(
    filter: Filter<T>,
    options?: { projection?: Record<string, 0 | 1>; sort?: Record<string, 1 | -1> }
  ): Query<T | null, T> {
    return new Query<T | null, T>(this, 'findOneAndDelete', filter, options?.projection, options);
  }

  /**
   * Find by ID and delete
   */
  findByIdAndDelete(id: string | ObjectId, options?: { projection?: Record<string, 0 | 1> }): Query<T | null, T> {
    const filter = { _id: id instanceof ObjectId ? id : new ObjectId(id) } as unknown as Filter<T>;
    return new Query<T | null, T>(this, 'findOneAndDelete', filter, options?.projection, options);
  }

  /**
   * Update one document
   */
  updateOne(
    filter: Filter<T>,
    update: Update<T>,
    options?: { upsert?: boolean }
  ): Query<{ acknowledged: boolean; matchedCount: number; modifiedCount: number; upsertedId?: string | ObjectId }, T> {
    return new Query(this, 'updateOne', filter, undefined, { ...options, update });
  }

  /**
   * Update many documents
   */
  updateMany(
    filter: Filter<T>,
    update: Update<T>,
    options?: { upsert?: boolean }
  ): Query<{ acknowledged: boolean; matchedCount: number; modifiedCount: number }, T> {
    return new Query(this, 'updateMany', filter, undefined, { ...options, update });
  }

  /**
   * Replace one document
   */
  replaceOne(
    filter: Filter<T>,
    replacement: T,
    options?: { upsert?: boolean }
  ): Query<{ acknowledged: boolean; matchedCount: number; modifiedCount: number; upsertedId?: string | ObjectId }, T> {
    return new Query(this, 'replaceOne', filter, undefined, { ...options, replacement });
  }

  /**
   * Delete one document
   */
  deleteOne(filter: Filter<T>): Query<{ acknowledged: boolean; deletedCount: number }, T> {
    return new Query(this, 'deleteOne', filter);
  }

  /**
   * Delete many documents
   */
  deleteMany(filter?: Filter<T>): Query<{ acknowledged: boolean; deletedCount: number }, T> {
    return new Query(this, 'deleteMany', filter);
  }

  /**
   * Count documents
   */
  countDocuments(filter?: Filter<T>): Query<number, T> {
    return new Query(this, 'countDocuments', filter);
  }

  /**
   * Estimated document count
   */
  estimatedDocumentCount(): Query<number, T> {
    return new Query(this, 'estimatedDocumentCount');
  }

  /**
   * Get distinct values
   */
  distinct<K extends keyof T>(field: K, filter?: Filter<T>): Query<Array<T[K]>, T> {
    return new Query(this, 'distinct', filter, undefined, { field: field as string });
  }

  /**
   * Aggregation pipeline
   */
  aggregate<R = unknown>(pipeline: unknown[], options?: AggregateOptions): AggregationBuilder<R> {
    return new AggregationBuilder<R>(this.collection as unknown as Collection<BaseDocument>, pipeline, options);
  }

  /**
   * Check if documents exist
   */
  async exists(filter: Filter<T>): Promise<{ _id: string | ObjectId } | null> {
    const doc = await this.collection.findOne(filter, { projection: { _id: 1 } });
    return doc ? { _id: doc._id! } : null;
  }

  /**
   * Populate documents
   */
  async populate<DocType extends T | T[]>(
    docs: DocType,
    paths: string | PopulateOptions | (string | PopulateOptions)[]
  ): Promise<DocType> {
    if (!docs) return docs;

    const docsArray = (Array.isArray(docs) ? docs : [docs]) as T[];
    const pathsArray = Array.isArray(paths) ? paths : [paths];

    for (const pathOpt of pathsArray) {
      const opt = typeof pathOpt === 'string' ? { path: pathOpt } : pathOpt;
      await this.populatePath(docsArray, opt);
    }

    return docs;
  }

  private async populatePath(docs: T[], options: PopulateOptions): Promise<void> {
    const { path, model: modelName, localField, foreignField, select, match, justOne } = options;

    // Determine the model to use for population
    let targetModel: Model<BaseDocument> | undefined;
    if (modelName) {
      targetModel = typeof modelName === 'string'
        ? getModel(modelName) as Model<BaseDocument> | undefined
        : modelName as unknown as Model<BaseDocument>;
    } else {
      // Try to get from schema ref
      const schemaPath = this.schema.paths.get(path);
      if (schemaPath?.options.ref) {
        targetModel = getModel(schemaPath.options.ref) as Model<BaseDocument> | undefined;
      }
    }

    if (!targetModel) {
      throw new Error(`Cannot populate path "${path}": no model found`);
    }

    // Collect all IDs to populate
    const local = localField || path;
    const foreign = foreignField || '_id';
    const ids = new Set<string>();

    for (const doc of docs) {
      const value = (doc as unknown as Record<string, unknown>)[local];
      if (value) {
        if (Array.isArray(value)) {
          for (const v of value) {
            ids.add(String(v));
          }
        } else {
          ids.add(String(value));
        }
      }
    }

    if (ids.size === 0) return;

    // Build filter
    const filter: Filter<BaseDocument> = {
      [foreign]: { $in: Array.from(ids) },
      ...match,
    } as Filter<BaseDocument>;

    // Fetch referenced documents
    const projection = select
      ? (typeof select === 'string'
        ? select.split(' ').reduce((acc, field) => {
            const isExclude = field.startsWith('-');
            acc[isExclude ? field.slice(1) : field] = isExclude ? 0 : 1;
            return acc;
          }, {} as Record<string, 0 | 1>)
        : select)
      : undefined;

    const cursor = targetModel.collection.find(filter, { projection });
    const refDocs = await cursor.toArray();

    // Create lookup map
    const refMap = new Map<string, BaseDocument | BaseDocument[]>();
    for (const refDoc of refDocs) {
      const key = String((refDoc as unknown as Record<string, unknown>)[foreign]);
      if (justOne === false) {
        const existing = refMap.get(key) || [];
        (existing as BaseDocument[]).push(refDoc);
        refMap.set(key, existing);
      } else {
        refMap.set(key, refDoc);
      }
    }

    // Populate documents
    for (const doc of docs) {
      const value = (doc as unknown as Record<string, unknown>)[local];
      if (value) {
        if (Array.isArray(value)) {
          const populated = value.map((v) => refMap.get(String(v))).filter(Boolean);
          (doc as unknown as Record<string, unknown>)[path] = populated.flat();
        } else {
          (doc as unknown as Record<string, unknown>)[path] = refMap.get(String(value)) || null;
        }
      }
    }
  }

  /**
   * Create a discriminator model
   */
  discriminator<D extends T>(name: string, schema: Schema<D>, value?: string): Model<D> {
    const discriminatorKey = this.schema.options.discriminatorKey || '__t';

    // Clone and extend the base schema
    const discriminatorSchema = this.schema.clone() as unknown as Schema<D>;

    // Add discriminator paths
    for (const [pathName, path] of schema.paths) {
      discriminatorSchema.path(pathName, path);
    }

    // Add discriminator virtuals
    for (const [name, virtual] of schema.virtuals) {
      discriminatorSchema.virtuals.set(name, virtual);
    }

    // Add discriminator methods
    for (const [name, method] of schema.methods) {
      discriminatorSchema.methods.set(name, method);
    }

    // Add discriminator key default
    discriminatorSchema.path(discriminatorKey, {
      type: 'String',
      default: value || name,
    });

    // Create the discriminator model sharing the same collection
    const discriminatorModel = new Model<D>(name, discriminatorSchema, {
      collection: this.collection.name,
      connection: this._connection,
    });

    this._discriminators.set(name, discriminatorModel as unknown as Model<T>);

    return discriminatorModel;
  }

  /**
   * Get discriminators
   */
  get discriminators(): Map<string, Model<T>> {
    return this._discriminators;
  }

  /**
   * Watch for changes
   */
  watch(_pipeline?: unknown[], _options?: { fullDocument?: 'default' | 'updateLookup' }): unknown {
    // Return a basic change stream interface
    // Full implementation would integrate with MongoLake change streams
    return {
      on: (_event: string, _callback: Function) => {},
      close: () => {},
    };
  }

  /**
   * Create indexes defined in schema
   */
  async createIndexes(): Promise<void> {
    for (const index of this.schema.indexes) {
      await this.collection.createIndex(
        index.fields as { [key: string]: 1 | -1 },
        index.options
      );
    }
  }

  /**
   * Sync indexes with schema
   */
  async syncIndexes(): Promise<string[]> {
    // Get existing indexes
    const existing = await this.collection.listIndexes();
    const existingNames = new Set(existing.map((i) => i.name));

    // Create missing indexes
    const created: string[] = [];
    for (const index of this.schema.indexes) {
      const indexName = (index.options?.name as string | undefined) || Object.keys(index.fields).join('_');
      if (!existingNames.has(indexName)) {
        await this.collection.createIndex(
          index.fields as { [key: string]: 1 | -1 },
          { name: indexName, ...index.options }
        );
        created.push(indexName);
      }
    }

    return created;
  }

  /**
   * Ensure indexes exist
   */
  async ensureIndexes(): Promise<void> {
    await this.createIndexes();
  }

  /**
   * List indexes
   */
  async listIndexes(): Promise<Array<{ name: string; key: Record<string, 1 | -1> }>> {
    const indexes = await this.collection.listIndexes();
    return indexes.map(idx => ({
      name: idx.name,
      key: idx.key as Record<string, 1 | -1>
    }));
  }

  /**
   * Hydrate a plain object into a document
   */
  hydrate(obj: Partial<T>): MongooseDocument<T> & T {
    const doc = this.createDocument(obj);
    doc.isNew = false;
    return doc;
  }

  /**
   * Compile model from schema (static factory)
   */
  static compile<T extends BaseDocument>(
    name: string,
    schema: Schema<T>,
    connection?: ModelConnection
  ): Model<T> {
    return new Model<T>(name, schema, { connection });
  }
}

// ============================================================================
// Aggregation Builder
// ============================================================================

/**
 * Aggregation pipeline builder
 */
export class AggregationBuilder<R = unknown> {
  private _pipeline: unknown[];
  private _options: AggregateOptions;
  private _collection: Collection<BaseDocument>;

  constructor(collection: Collection<BaseDocument>, pipeline: unknown[] = [], options: AggregateOptions = {}) {
    this._collection = collection;
    this._pipeline = pipeline;
    this._options = options;
  }

  /**
   * Add a stage to the pipeline
   */
  append(...stages: unknown[]): this {
    this._pipeline.push(...stages);
    return this;
  }

  /**
   * Add $match stage
   */
  match(filter: Filter<BaseDocument>): this {
    this._pipeline.push({ $match: filter });
    return this;
  }

  /**
   * Add $group stage
   */
  group(spec: { _id: unknown; [key: string]: unknown }): this {
    this._pipeline.push({ $group: spec });
    return this;
  }

  /**
   * Add $sort stage
   */
  sort(spec: Record<string, 1 | -1>): this {
    this._pipeline.push({ $sort: spec });
    return this;
  }

  /**
   * Add $limit stage
   */
  limit(n: number): this {
    this._pipeline.push({ $limit: n });
    return this;
  }

  /**
   * Add $skip stage
   */
  skip(n: number): this {
    this._pipeline.push({ $skip: n });
    return this;
  }

  /**
   * Add $project stage
   */
  project(spec: Record<string, unknown>): this {
    this._pipeline.push({ $project: spec });
    return this;
  }

  /**
   * Add $unwind stage
   */
  unwind(path: string | { path: string; preserveNullAndEmptyArrays?: boolean }): this {
    this._pipeline.push({ $unwind: path });
    return this;
  }

  /**
   * Add $lookup stage
   */
  lookup(spec: {
    from: string;
    localField?: string;
    foreignField?: string;
    as: string;
    let?: Record<string, unknown>;
    pipeline?: unknown[];
  }): this {
    this._pipeline.push({ $lookup: spec });
    return this;
  }

  /**
   * Add $count stage
   */
  count(field: string): this {
    this._pipeline.push({ $count: field });
    return this;
  }

  /**
   * Add $addFields stage
   */
  addFields(spec: Record<string, unknown>): this {
    this._pipeline.push({ $addFields: spec });
    return this;
  }

  /**
   * Add $facet stage
   */
  facet(spec: Record<string, unknown[]>): this {
    this._pipeline.push({ $facet: spec });
    return this;
  }

  /**
   * Add $sample stage
   */
  sample(size: number): this {
    this._pipeline.push({ $sample: { size } });
    return this;
  }

  /**
   * Set allow disk use
   */
  allowDiskUse(enable: boolean = true): this {
    this._options.allowDiskUse = enable;
    return this;
  }

  /**
   * Execute the aggregation
   */
  async exec(): Promise<R[]> {
    const cursor = this._collection.aggregate<R & BaseDocument>(
      this._pipeline as Array<{ $match: Filter<BaseDocument> }>,
      this._options as Parameters<typeof this._collection.aggregate>[1]
    );
    return cursor.toArray();
  }

  /**
   * Convert to array (alias for exec)
   */
  toArray(): Promise<R[]> {
    return this.exec();
  }

  /**
   * Get cursor
   */
  cursor(): AsyncIterable<R> {
    return this._collection.aggregate<R & BaseDocument>(
      this._pipeline as Array<{ $match: Filter<BaseDocument> }>,
      this._options as Parameters<typeof this._collection.aggregate>[1]
    );
  }

  /**
   * Then handler for promise interface
   */
  then<TResult1 = R[], TResult2 = never>(
    onfulfilled?: ((value: R[]) => TResult1 | PromiseLike<TResult1>) | null,
    onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
  ): Promise<TResult1 | TResult2> {
    return this.exec().then(onfulfilled, onrejected);
  }
}

// ============================================================================
// Model Factory Function
// ============================================================================

/**
 * Create a model from a schema
 */
export function model<T extends BaseDocument = BaseDocument>(
  name: string,
  schema?: Schema<T> | SchemaDefinition,
  options?: ModelOptions
): Model<T> {
  // Check if model already exists
  const existing = getModel(name);
  if (existing && !options?.connection) {
    return existing as unknown as Model<T>;
  }

  // Create schema if needed
  const schemaInstance = schema instanceof Schema
    ? schema
    : new Schema<T>(schema as SchemaDefinition);

  return new Model<T>(name, schemaInstance, options);
}
