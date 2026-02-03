/**
 * MongoLake Mongoose Integration
 *
 * Provides full Mongoose-compatible ORM layer with:
 * 1. Schema definition with type support
 * 2. Model class with CRUD operations
 * 3. Document class with validation
 * 4. Middleware hooks (pre/post)
 * 5. Virtual fields
 * 6. Instance and static methods
 * 7. Plugin support
 * 8. Population / references
 * 9. Connection events
 * 10. Proper error handling
 *
 * @example
 * ```typescript
 * // Using the full Mongoose-like API
 * import { Schema, model, connect } from 'mongolake/mongoose'
 *
 * const userSchema = new Schema({
 *   name: { type: String, required: true },
 *   email: { type: String, required: true, unique: true },
 *   age: { type: Number, min: 0 }
 * }, { timestamps: true })
 *
 * // Add virtual
 * userSchema.virtual('isAdult').get(function() {
 *   return this.age >= 18
 * })
 *
 * // Add instance method
 * userSchema.method('greet', function() {
 *   return `Hello, I'm ${this.name}`
 * })
 *
 * // Add pre-save middleware
 * userSchema.pre('save', function(next) {
 *   console.log('Saving user:', this.name)
 *   next()
 * })
 *
 * const User = model('User', userSchema)
 *
 * // Connect and use
 * await connect('mongolake://localhost/mydb?local=.mongolake')
 *
 * const user = await User.create({ name: 'Alice', email: 'alice@example.com', age: 25 })
 * console.log(user.isAdult) // true
 * console.log(user.greet()) // "Hello, I'm Alice"
 * ```
 *
 * @example
 * ```typescript
 * // Using as a driver for official mongoose
 * import mongoose from 'mongoose'
 * import { createDriver } from 'mongolake/mongoose'
 *
 * mongoose.setDriver(createDriver({ local: '.mongolake' }))
 * await mongoose.connect('mongolake://localhost/mydb')
 * ```
 */

import {
  MongoLake,
  Database,
  Collection,
  FindCursor,
  AggregationCursor,
  ObjectId,
  type Document as BaseDocumentType,
  type WithId,
  type Filter,
  type Update,
  type FindOptions,
  type UpdateOptions,
  type DeleteOptions,
  type AggregateOptions as BaseAggregateOptions,
  type AggregationStage,
  type InsertOneResult,
  type InsertManyResult,
  type UpdateResult,
  type DeleteResult,
  type MongoLakeConfig,
} from '../client/index.js';

// Import Mongoose adapter classes
import { Schema, SchemaPath, Virtual } from './schema.js';
import { Model, model, getModel, deleteModel, modelNames, registerModel, AggregationBuilder } from './model.js';
import { MongooseDocument } from './document.js';
import { Query } from './query.js';
import { Connection, ConnectionPool, ConnectionStates } from './connection.js';
import * as errors from './errors.js';

// Re-export schema types
export type {
  SchemaType,
  SchemaTypeDefinition,
  SchemaDefinition,
  SchemaOptions,
  ValidatorFunction,
  ValidatorDefinition,
  ToObjectOptions,
  MiddlewareHookType,
  QueryMiddlewareType,
  PreMiddlewareFunction,
  PostMiddlewareFunction,
  VirtualDefinition,
  VirtualOptions,
  PluginFunction,
} from './schema.js';

// Re-export model types
export type {
  ModelOptions,
  ModelConnection,
  PopulateOptions,
  LeanOptions,
  CreateOptions,
  SaveOptions,
  AggregateOptions,
} from './model.js';

// Re-export query types
export type {
  QueryOperation,
  QueryOptions,
} from './query.js';

// Re-export connection types
export type {
  ConnectionOptions,
  ConnectionEvent,
} from './connection.js';

// Re-export document types
export type {
  DocumentMethods,
  SaveOptions as DocSaveOptions,
  ToObjectOptions as DocToObjectOptions,
} from './document.js';

// Re-export client classes for direct use
export {
  MongoLake,
  Database,
  Collection,
  FindCursor,
  AggregationCursor,
  ObjectId,
  type BaseDocumentType,
  type WithId,
  type Filter,
  type Update,
  type FindOptions,
  type UpdateOptions,
  type DeleteOptions,
  type BaseAggregateOptions,
  type AggregationStage,
  type InsertOneResult,
  type InsertManyResult,
  type UpdateResult,
  type DeleteResult,
  type MongoLakeConfig,
};

// Re-export Mongoose adapter classes
export {
  Schema,
  SchemaPath,
  Virtual,
  Model,
  model,
  getModel,
  deleteModel,
  modelNames,
  registerModel,
  MongooseDocument,
  Query,
  AggregationBuilder,
  Connection,
  ConnectionPool,
  ConnectionStates,
  errors,
};

// Alias Document class
export { MongooseDocument as Document };

// Re-export specific errors
export {
  ValidationError,
  ValidatorError,
  CastError,
  DocumentNotFoundError,
  VersionError,
  ParallelSaveError,
  DisconnectedError,
  MissingSchemaError,
  DuplicateKeyError,
  StrictModeError,
  DivergentArrayError,
  ObjectExpectedError,
  ObjectParameterError,
  OverwriteModelError,
  MongooseError,
} from './errors.js';

// Alias for clarity
export { MongoLake as MongoClient };

// ============================================================================
// Global Connection Instance
// ============================================================================

const globalConnection = new Connection();

/**
 * Connect to MongoLake database
 *
 * @example
 * ```typescript
 * await connect('mongolake://localhost/mydb?local=.mongolake')
 * ```
 */
export async function connect(uri: string, options?: MongoLakeConfig): Promise<Connection> {
  return globalConnection.openUri(uri, options);
}

/**
 * Disconnect from database
 */
export async function disconnect(): Promise<void> {
  await globalConnection.close();
}

/**
 * Get the default connection
 */
export function getConnection(): Connection {
  return globalConnection;
}

/**
 * Create a new connection
 */
export function createConnection(uri?: string, options?: MongoLakeConfig): Connection {
  const conn = new Connection();
  if (uri) {
    conn.openUri(uri, options);
  }
  return conn;
}

/**
 * Get connection ready state
 */
export function getReadyState(): ConnectionStates {
  return globalConnection.readyState;
}

// ============================================================================
// Mongoose Driver Interface (for official mongoose compatibility)
// ============================================================================

/**
 * Mongoose driver connection wrapper
 */
export class MongoLakeConnection {
  private lake: MongoLake;
  private _db: Database | null = null;

  constructor(config: MongoLakeConfig = {}) {
    this.lake = new MongoLake(config);
  }

  get client(): MongoLake {
    return this.lake;
  }

  get db(): Database | null {
    return this._db;
  }

  /**
   * Connect to a database
   */
  async connect(uri?: string): Promise<this> {
    // Parse URI if provided (mongolake://host/database)
    let dbName = 'default';

    if (uri) {
      const match = uri.match(/mongolake:\/\/[^/]*\/([^?]+)/);
      if (match) {
        dbName = match[1]!;
      }
    }

    this._db = this.lake.db(dbName);
    return this;
  }

  /**
   * Close connection
   */
  async close(): Promise<void> {
    await this.lake.close();
    this._db = null;
  }

  /**
   * Get a collection
   */
  collection<T extends BaseDocumentType = BaseDocumentType>(name: string): Collection<T> {
    if (!this._db) {
      throw new Error('Not connected. Call connect() first.');
    }
    return this._db.collection<T>(name);
  }
}

/**
 * Mongoose driver wrapper for MongoLake collections
 *
 * Implements the interface expected by mongoose's internal driver
 */
export class MongoLakeDriverCollection<T extends BaseDocumentType = BaseDocumentType> {
  private _collection: Collection<T>;

  constructor(collection: Collection<T>) {
    this._collection = collection;
  }

  get collectionName(): string {
    return this._collection.name;
  }

  // ---- Write Operations ----

  async insertOne(doc: T, _options?: { session?: unknown }) {
    return this._collection.insertOne(doc);
  }

  async insertMany(docs: T[], _options?: { session?: unknown }) {
    return this._collection.insertMany(docs);
  }

  async updateOne(
    filter: Filter<T>,
    update: Update<T>,
    options?: UpdateOptions & { session?: unknown }
  ) {
    return this._collection.updateOne(filter, update, options);
  }

  async updateMany(
    filter: Filter<T>,
    update: Update<T>,
    options?: UpdateOptions & { session?: unknown }
  ) {
    return this._collection.updateMany(filter, update, options);
  }

  async replaceOne(
    filter: Filter<T>,
    replacement: T,
    options?: UpdateOptions & { session?: unknown }
  ) {
    return this._collection.replaceOne(filter, replacement, options);
  }

  async deleteOne(filter: Filter<T>, options?: DeleteOptions & { session?: unknown }) {
    return this._collection.deleteOne(filter, options);
  }

  async deleteMany(filter: Filter<T>, options?: DeleteOptions & { session?: unknown }) {
    return this._collection.deleteMany(filter, options);
  }

  // ---- Read Operations ----

  async findOne(filter?: Filter<T>, options?: FindOptions & { session?: unknown }) {
    return this._collection.findOne(filter, options);
  }

  find(filter?: Filter<T>, options?: FindOptions & { session?: unknown }) {
    return this._collection.find(filter, options);
  }

  async countDocuments(filter?: Filter<T>, _options?: { session?: unknown }) {
    return this._collection.countDocuments(filter);
  }

  async estimatedDocumentCount(_options?: { session?: unknown }) {
    return this._collection.estimatedDocumentCount();
  }

  async distinct<K extends keyof T>(
    field: K,
    filter?: Filter<T>,
    _options?: { session?: unknown }
  ) {
    return this._collection.distinct(field, filter);
  }

  aggregate<R extends BaseDocumentType = BaseDocumentType>(
    pipeline: AggregationStage[],
    options?: BaseAggregateOptions & { session?: unknown }
  ) {
    return this._collection.aggregate<R>(pipeline, options);
  }

  // ---- Index Operations ----

  async createIndex(spec: { [key: string]: 1 | -1 }, options?: { name?: string }) {
    return this._collection.createIndex(spec, options);
  }

  async createIndexes(specs: Array<{ key: { [key: string]: 1 | -1 }; options?: { name?: string } }>) {
    return this._collection.createIndexes(specs);
  }

  async dropIndex(name: string) {
    return this._collection.dropIndex(name);
  }

  async listIndexes() {
    return this._collection.listIndexes();
  }
}

/**
 * Mongoose driver for MongoLake
 *
 * This implements the interface expected by mongoose.setDriver()
 */
export interface MongoLakeDriver {
  Connection: typeof MongoLakeConnection;
  Collection: typeof MongoLakeDriverCollection;
  ObjectId: typeof ObjectId;
}

/**
 * Create a mongoose driver instance
 *
 * @example
 * ```typescript
 * import mongoose from 'mongoose'
 * import { createDriver } from 'mongolake/mongoose'
 *
 * mongoose.setDriver(createDriver())
 *
 * // Then connect using mongolake:// URI
 * await mongoose.connect('mongolake://localhost/mydb')
 *
 * // Or with R2 bucket
 * mongoose.setDriver(createDriver({ bucket: env.R2_BUCKET }))
 * ```
 */
export function createDriver(config?: MongoLakeConfig): MongoLakeDriver {
  // Create a connection class bound to this config
  class BoundConnection extends MongoLakeConnection {
    constructor() {
      super(config);
    }
  }

  const ConnectionClass = BoundConnection as typeof MongoLakeConnection;

  return {
    Connection: ConnectionClass,
    Collection: MongoLakeDriverCollection,
    ObjectId,
  };
}

// ============================================================================
// Session Support (Stub)
// ============================================================================

/**
 * Session stub for transaction support
 *
 * MongoLake doesn't yet support full transactions, but this provides
 * the interface for compatibility with mongoose session APIs
 */
export class MongoLakeSession {
  private _inTransaction = false;
  private _id = crypto.randomUUID();

  get id(): string {
    return this._id;
  }

  inTransaction(): boolean {
    return this._inTransaction;
  }

  startTransaction(): void {
    this._inTransaction = true;
  }

  async commitTransaction(): Promise<void> {
    this._inTransaction = false;
  }

  async abortTransaction(): Promise<void> {
    this._inTransaction = false;
  }

  async endSession(): Promise<void> {
    this._inTransaction = false;
  }

  async withTransaction<T>(fn: () => Promise<T>): Promise<T> {
    this.startTransaction();
    try {
      const result = await fn();
      await this.commitTransaction();
      return result;
    } catch (error) {
      await this.abortTransaction();
      throw error;
    }
  }
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Create a MongoLake client configured from a connection string
 *
 * @example
 * ```typescript
 * import { fromConnectionString } from 'mongolake/mongoose'
 *
 * // Local storage
 * const lake = fromConnectionString('mongolake://localhost/mydb?local=.mongolake')
 *
 * // R2 storage (in Workers)
 * const lake = fromConnectionString('mongolake://r2/mydb', { bucket: env.R2_BUCKET })
 * ```
 */
export function fromConnectionString(
  uri: string,
  overrides?: Partial<MongoLakeConfig>
): MongoLake {
  const url = new URL(uri);
  const config: MongoLakeConfig = { ...overrides };

  // Parse database from path
  const dbName = url.pathname.slice(1); // Remove leading /
  if (dbName) {
    config.database = dbName;
  }

  // Parse query params
  const local = url.searchParams.get('local');
  if (local) {
    config.local = local;
  }

  const branch = url.searchParams.get('branch');
  if (branch) {
    config.branch = branch;
  }

  const asOf = url.searchParams.get('asOf');
  if (asOf) {
    config.asOf = asOf;
  }

  return new MongoLake(config);
}

// ============================================================================
// Schema Type Helpers
// ============================================================================

/**
 * Schema Types - constructors for schema type definitions
 */
export const Types = {
  ObjectId,
  String,
  Number,
  Boolean,
  Date,
  Buffer: Uint8Array,
  Array,
  Map,
  Mixed: Object,
  Decimal128: 'Decimal128' as const,
  UUID: 'UUID' as const,
  BigInt: 'BigInt' as const,
};

/**
 * Built-in validators
 */
export const SchemaTypes = {
  String: 'String' as const,
  Number: 'Number' as const,
  Boolean: 'Boolean' as const,
  Date: 'Date' as const,
  Buffer: 'Buffer' as const,
  ObjectId: 'ObjectId' as const,
  Mixed: 'Mixed' as const,
  Array: 'Array' as const,
  Map: 'Map' as const,
  Decimal128: 'Decimal128' as const,
  UUID: 'UUID' as const,
  BigInt: 'BigInt' as const,
};

// ============================================================================
// Plugin System
// ============================================================================

/**
 * Global plugins registry
 */
const globalPlugins: Array<{ fn: (schema: Schema) => void; options?: Record<string, unknown> }> = [];

/**
 * Register a global plugin
 */
export function plugin(fn: (schema: Schema) => void, options?: Record<string, unknown>): void {
  globalPlugins.push({ fn, options });
}

/**
 * Get all global plugins
 */
export function getGlobalPlugins(): Array<{ fn: (schema: Schema) => void; options?: Record<string, unknown> }> {
  return globalPlugins;
}

// ============================================================================
// Set Global Options
// ============================================================================

/**
 * Global options
 */
let globalOptions: Record<string, unknown> = {};

/**
 * Set global options
 */
export function set(key: string, value: unknown): void {
  globalOptions[key] = value;
}

/**
 * Get global option
 */
export function get(key: string): unknown {
  return globalOptions[key];
}

// ============================================================================
// Default Export
// ============================================================================

export default {
  // Client classes
  MongoLake,
  MongoClient: MongoLake,
  Database,
  Collection,
  ObjectId,

  // Mongoose adapter classes
  Schema,
  Model,
  model,
  Document: MongooseDocument,
  Query,
  Connection,
  Virtual,

  // Driver classes (for official mongoose)
  MongoLakeConnection,
  MongoLakeDriverCollection,
  MongoLakeSession,
  createDriver,

  // Connection functions
  connect,
  disconnect,
  createConnection,
  getConnection,

  // Model management
  getModel,
  deleteModel,
  modelNames,

  // Utility functions
  fromConnectionString,

  // Type helpers
  Types,
  SchemaTypes,

  // Error classes
  errors,
  ValidationError: errors.ValidationError,
  CastError: errors.CastError,

  // Plugin system
  plugin,

  // Global options
  set,
  get,

  // Connection states
  ConnectionStates,
  STATES: ConnectionStates,
};
