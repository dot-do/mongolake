/**
 * MongoLake Mongoose Connection
 *
 * Provides Mongoose-compatible connection management with:
 * - Connection events (connected, disconnected, error, etc.)
 * - Connection state tracking
 * - Model management
 * - Multiple connection support
 */

import { MongoLake, Database } from '../client/index.js';
import type { MongoLakeConfig, Document } from '../types.js';
import { Model, deleteModel } from './model.js';
import { Schema, type SchemaDefinition } from './schema.js';
import { DisconnectedError } from './errors.js';
import { logger } from '../utils/logger.js';

// ============================================================================
// Connection Types
// ============================================================================

/**
 * Connection ready states
 */
export enum ConnectionStates {
  disconnected = 0,
  connected = 1,
  connecting = 2,
  disconnecting = 3,
  uninitialized = 99,
}

/**
 * Connection options
 */
export interface ConnectionOptions extends MongoLakeConfig {
  /** Auto-index on connect */
  autoIndex?: boolean;
  /** Auto-create collections */
  autoCreate?: boolean;
  /** Buffer commands when disconnected */
  bufferCommands?: boolean;
  /** Max time to wait for buffered commands */
  bufferTimeoutMS?: number;
}

/**
 * Connection events
 */
export type ConnectionEvent =
  | 'connected'
  | 'disconnected'
  | 'error'
  | 'reconnected'
  | 'close'
  | 'open'
  | 'connecting'
  | 'disconnecting'
  | 'fullsetup'
  | 'all'
  | 'reconnectFailed';

/**
 * Event listener type
 */
export type EventListener = (...args: unknown[]) => void;

// ============================================================================
// Connection Class
// ============================================================================

/**
 * Mongoose-compatible Connection class
 */
export class Connection {
  public name: string = '';
  public host: string = '';
  public port: number = 0;
  public readyState: ConnectionStates = ConnectionStates.disconnected;
  public config: ConnectionOptions;
  public db: Database | null = null;

  private _lake: MongoLake | null = null;
  private _models: Map<string, Model<Document>> = new Map();
  private _eventListeners: Map<ConnectionEvent, Set<EventListener>> = new Map();
  // Reserved for future async operation tracking
  // @ts-expect-error Reserved for future use
  private readonly _pendingPromises: Array<{ resolve: Function; reject: Function }> = [];
  private _bufferCommands: boolean = true;
  private _bufferedCommands: Array<{ fn: Function; args: unknown[] }> = [];

  constructor(_base?: Connection) {
    this.config = {};

    // Initialize event listener maps
    const events: ConnectionEvent[] = [
      'connected', 'disconnected', 'error', 'reconnected',
      'close', 'open', 'connecting', 'disconnecting',
      'fullsetup', 'all', 'reconnectFailed'
    ];
    for (const event of events) {
      this._eventListeners.set(event, new Set());
    }
  }

  /**
   * Get the underlying MongoLake client
   */
  get client(): MongoLake | null {
    return this._lake;
  }

  /**
   * Get all registered models
   */
  get models(): Record<string, Model<Document>> {
    const result: Record<string, Model<Document>> = {};
    for (const [name, model] of this._models) {
      result[name] = model;
    }
    return result;
  }

  /**
   * Get all model names
   */
  modelNames(): string[] {
    return Array.from(this._models.keys());
  }

  /**
   * Open a connection
   */
  async openUri(uri: string, options?: ConnectionOptions): Promise<this> {
    this.readyState = ConnectionStates.connecting;
    this._emit('connecting');

    try {
      const config = this.parseUri(uri, options);
      this.config = { ...this.config, ...config };

      this._lake = new MongoLake(this.config);
      this.db = this._lake.db(this.config.database);

      this.name = this.config.database || 'default';
      this.host = 'mongolake';
      this.port = 0;

      this.readyState = ConnectionStates.connected;
      this._emit('connected');
      this._emit('open');

      // Process buffered commands
      this._processBufferedCommands();

      // Auto-create indexes if enabled
      if (this.config.autoIndex !== false) {
        await this.syncIndexes();
      }

      return this;
    } catch (error) {
      this.readyState = ConnectionStates.disconnected;
      this._emit('error', error);
      throw error;
    }
  }

  /**
   * Connect (alias for openUri)
   */
  async connect(uri?: string, options?: ConnectionOptions): Promise<this> {
    if (uri) {
      return this.openUri(uri, options);
    }
    throw new Error('Connection string required');
  }

  /**
   * Close the connection
   */
  async close(_force?: boolean): Promise<void> {
    if (this.readyState === ConnectionStates.disconnected) {
      return;
    }

    this.readyState = ConnectionStates.disconnecting;
    this._emit('disconnecting');

    try {
      if (this._lake) {
        await this._lake.close();
        this._lake = null;
      }
      this.db = null;

      this.readyState = ConnectionStates.disconnected;
      this._emit('disconnected');
      this._emit('close');
    } catch (error) {
      this._emit('error', error);
      throw error;
    }
  }

  /**
   * Destroy the connection (alias for close with force)
   */
  async destroy(): Promise<void> {
    await this.close(true);
  }

  /**
   * Get a collection
   */
  collection<T extends Document = Document>(name: string) {
    if (!this.db) {
      throw new DisconnectedError();
    }
    return this.db.collection<T>(name);
  }

  /**
   * Create a collection
   */
  async createCollection<T extends Document = Document>(
    name: string,
    _options?: { capped?: boolean; size?: number; max?: number }
  ) {
    if (!this.db) {
      throw new DisconnectedError();
    }
    return this.db.createCollection<T>(name);
  }

  /**
   * Drop a collection
   */
  async dropCollection(name: string): Promise<boolean> {
    if (!this.db) {
      throw new DisconnectedError();
    }
    return this.db.dropCollection(name);
  }

  /**
   * Drop the database
   */
  async dropDatabase(): Promise<void> {
    if (!this._lake || !this.name) {
      throw new DisconnectedError();
    }
    await this._lake.dropDatabase(this.name);
  }

  /**
   * List collections
   */
  async listCollections(): Promise<string[]> {
    if (!this.db) {
      throw new DisconnectedError();
    }
    return this.db.listCollections();
  }

  // ============================================================================
  // Model Management
  // ============================================================================

  /**
   * Define or retrieve a model
   */
  model<T extends Document = Document>(
    name: string,
    schema?: Schema<T> | SchemaDefinition,
    options?: { collection?: string; skipInit?: boolean }
  ): Model<T> {
    // Return existing model
    if (!schema) {
      const existing = this._models.get(name);
      if (existing) {
        return existing as unknown as Model<T>;
      }
      throw new Error(`Model '${name}' not found. Use mongoose.model(name, schema) to define it.`);
    }

    // Create new model
    const schemaInstance = schema instanceof Schema
      ? schema
      : new Schema<T>(schema as SchemaDefinition);

    const modelInstance = new Model<T>(name, schemaInstance, {
      collection: options?.collection,
      connection: this,
    });

    this._models.set(name, modelInstance as unknown as Model<Document>);
    return modelInstance;
  }

  /**
   * Delete a model
   */
  deleteModel(name: string): this {
    this._models.delete(name);
    deleteModel(name);
    return this;
  }

  /**
   * Sync all model indexes
   */
  async syncIndexes(): Promise<void> {
    for (const model of this._models.values()) {
      await model.syncIndexes();
    }
  }

  // ============================================================================
  // Event Handling
  // ============================================================================

  /**
   * Add event listener
   */
  on(event: ConnectionEvent, listener: EventListener): this {
    const listeners = this._eventListeners.get(event);
    if (listeners) {
      listeners.add(listener);
    }
    return this;
  }

  /**
   * Add one-time event listener
   */
  once(event: ConnectionEvent, listener: EventListener): this {
    const onceListener: EventListener = (...args) => {
      this.off(event, onceListener);
      listener(...args);
    };
    return this.on(event, onceListener);
  }

  /**
   * Remove event listener
   */
  off(event: ConnectionEvent, listener: EventListener): this {
    const listeners = this._eventListeners.get(event);
    if (listeners) {
      listeners.delete(listener);
    }
    return this;
  }

  /**
   * Remove event listener (alias)
   */
  removeListener(event: ConnectionEvent, listener: EventListener): this {
    return this.off(event, listener);
  }

  /**
   * Remove all listeners for an event
   */
  removeAllListeners(event?: ConnectionEvent): this {
    if (event) {
      const listeners = this._eventListeners.get(event);
      if (listeners) {
        listeners.clear();
      }
    } else {
      for (const listeners of this._eventListeners.values()) {
        listeners.clear();
      }
    }
    return this;
  }

  /**
   * Emit an event
   */
  private _emit(event: ConnectionEvent, ...args: unknown[]): void {
    const listeners = this._eventListeners.get(event);
    if (listeners) {
      for (const listener of listeners) {
        try {
          listener(...args);
        } catch (err) {
          logger.error('Error in event listener', {
            eventName: event,
            error: err,
          });
        }
      }
    }

    // Also emit to 'all' listeners
    if (event !== 'all') {
      const allListeners = this._eventListeners.get('all');
      if (allListeners) {
        for (const listener of allListeners) {
          try {
            listener(event, ...args);
          } catch (err) {
            logger.error('Error in all event listener', {
              eventName: event,
              error: err,
            });
          }
        }
      }
    }
  }

  // ============================================================================
  // Command Buffering
  // ============================================================================

  /**
   * Buffer a command when disconnected
   * Reserved for future implementation of command buffering during disconnection
   */
  // @ts-expect-error Reserved for future use
  private _bufferCommand(fn: Function, args: unknown[]): void {
    if (this._bufferCommands) {
      this._bufferedCommands.push({ fn, args });
    }
  }

  /**
   * Process buffered commands
   */
  private async _processBufferedCommands(): Promise<void> {
    const commands = this._bufferedCommands;
    this._bufferedCommands = [];

    for (const { fn, args } of commands) {
      try {
        await fn.apply(this, args);
      } catch (err) {
        logger.error('Error processing buffered command', {
          error: err,
        });
      }
    }
  }

  // ============================================================================
  // Utilities
  // ============================================================================

  /**
   * Parse connection URI
   */
  private parseUri(uri: string, options?: ConnectionOptions): ConnectionOptions {
    const config: ConnectionOptions = { ...options };

    try {
      const url = new URL(uri);

      // Extract database from path
      const path = url.pathname.slice(1); // Remove leading /
      if (path) {
        config.database = path.split('?')[0]; // Remove query string
      }

      // Parse query parameters
      for (const [key, value] of url.searchParams) {
        switch (key) {
          case 'local':
            config.local = value;
            break;
          case 'branch':
            config.branch = value;
            break;
          case 'asOf':
            config.asOf = value;
            break;
          case 'autoIndex':
            config.autoIndex = value === 'true';
            break;
          case 'bufferCommands':
            config.bufferCommands = value === 'true';
            break;
        }
      }
    } catch {
      // Not a valid URL, treat as simple connection string
      if (uri.includes('/')) {
        const parts = uri.split('/');
        config.database = parts[parts.length - 1]?.split('?')[0];
      }
    }

    return config;
  }

  /**
   * Use a plugin on all schemas
   */
  plugin(fn: (schema: Schema) => void, _options?: Record<string, unknown>): this {
    // Apply to existing models
    for (const model of this._models.values()) {
      fn(model.schema);
    }
    return this;
  }

  /**
   * Get connection state as string
   */
  get state(): string {
    switch (this.readyState) {
      case ConnectionStates.disconnected:
        return 'disconnected';
      case ConnectionStates.connected:
        return 'connected';
      case ConnectionStates.connecting:
        return 'connecting';
      case ConnectionStates.disconnecting:
        return 'disconnecting';
      default:
        return 'uninitialized';
    }
  }

  /**
   * Promise that resolves when connected
   */
  asPromise(): Promise<this> {
    if (this.readyState === ConnectionStates.connected) {
      return Promise.resolve(this);
    }

    return new Promise((resolve, reject) => {
      this.once('connected', () => resolve(this));
      this.once('error', reject);
    });
  }

  /**
   * Start a session
   */
  async startSession(): Promise<unknown> {
    // Return a basic session interface
    // Full implementation would integrate with MongoLake sessions
    return {
      id: crypto.randomUUID(),
      inTransaction: () => false,
      startTransaction: () => {},
      commitTransaction: async () => {},
      abortTransaction: async () => {},
      endSession: async () => {},
    };
  }

  /**
   * Set buffer commands mode
   */
  setClient(client: MongoLake): void {
    this._lake = client;
    if (this.config.database) {
      this.db = client.db(this.config.database);
    }
  }

  /**
   * Transaction helper
   */
  async transaction<T>(fn: (session: unknown) => Promise<T>): Promise<T> {
    const session = await this.startSession();
    try {
      (session as { startTransaction: () => void }).startTransaction();
      const result = await fn(session);
      await (session as { commitTransaction: () => Promise<void> }).commitTransaction();
      return result;
    } catch (error) {
      await (session as { abortTransaction: () => Promise<void> }).abortTransaction();
      throw error;
    } finally {
      await (session as { endSession: () => Promise<void> }).endSession();
    }
  }

  /**
   * Watch for changes
   */
  watch(_pipeline?: unknown[], _options?: Record<string, unknown>): unknown {
    // Return a basic change stream interface
    return {
      on: (_event: string, _callback: Function) => {},
      close: () => {},
    };
  }
}

// ============================================================================
// Connection Pool (for multiple named connections)
// ============================================================================

/**
 * Connection pool for managing multiple connections
 */
export class ConnectionPool {
  private _connections: Map<string, Connection> = new Map();
  private _default: Connection;

  constructor() {
    this._default = new Connection();
  }

  /**
   * Get the default connection
   */
  get connection(): Connection {
    return this._default;
  }

  /**
   * Create a new connection
   */
  createConnection(uri?: string, options?: ConnectionOptions): Connection {
    const conn = new Connection();
    if (uri) {
      conn.openUri(uri, options);
    }
    return conn;
  }

  /**
   * Get or create a named connection
   */
  getConnection(name: string): Connection {
    let conn = this._connections.get(name);
    if (!conn) {
      conn = new Connection();
      this._connections.set(name, conn);
    }
    return conn;
  }

  /**
   * Get all connections
   */
  get connections(): Connection[] {
    return [this._default, ...Array.from(this._connections.values())];
  }

  /**
   * Connect the default connection
   */
  async connect(uri: string, options?: ConnectionOptions): Promise<Connection> {
    return this._default.openUri(uri, options);
  }

  /**
   * Close all connections
   */
  async disconnect(): Promise<void> {
    await this._default.close();
    for (const conn of this._connections.values()) {
      await conn.close();
    }
    this._connections.clear();
  }
}
