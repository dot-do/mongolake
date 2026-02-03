/**
 * MongoLake Client
 *
 * MongoDB-compatible client API
 */

import type {
  Document,
  Filter,
  Update,
  DeleteOptions,
  UpdateOptions,
  MongoLakeConfig,
} from '@types';
import {
  ClientSession,
  SessionStore,
  type SessionOptions,
  type BufferedOperation,
} from '@mongolake/session/index.js';
import { createStorage, type StorageBackend } from '@storage/index.js';
import { validateDatabaseName } from '@utils/validation.js';
import {
  parseConnectionString,
  isConnectionString,
  type ParsedConnectionString,
} from '@utils/connection-string.js';
import { logger } from '@utils/logger.js';
import { validate as validateConfig, mergeWithDefaults } from '@config/validator.js';
import { extractDocumentId } from './helpers.js';
import { Database } from './database.js';

// ============================================================================
// Transaction Rollback Types
// ============================================================================

/**
 * Represents the original state of a document before modification.
 * Used for rolling back partial transaction failures.
 */
interface DocumentSnapshot {
  /** Database name */
  database: string;
  /** Collection name */
  collection: string;
  /** Document ID */
  documentId: string;
  /** Original document state (null if document didn't exist before insert) */
  originalDocument: Document | null;
  /** Type of operation that was applied */
  operationType: 'insert' | 'update' | 'replace' | 'delete';
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
 * // With config object
 * const client = createClient({ local: '.mongolake' });
 *
 * // With MongoDB connection string
 * const client = createClient('mongodb://user:pass@localhost:27017/mydb?authSource=admin');
 *
 * const users = client.db('myapp').collection('users');
 * await users.insertOne({ name: 'Alice' });
 * ```
 */
export function createClient(configOrUri: MongoLakeConfig | string = {}): MongoLake {
  return new MongoLake(configOrUri);
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
 *
 * // MongoDB connection string
 * const lake = new MongoLake('mongodb://user:pass@localhost:27017/mydb?authSource=admin');
 * ```
 */
export class MongoLake {
  private config: MongoLakeConfig;
  private storage: StorageBackend;
  private databases: Map<string, Database> = new Map();
  private sessionStore: SessionStore;
  private parsedConnectionString?: ParsedConnectionString;

  /**
   * Create a new MongoLake client
   *
   * @param configOrUri - Configuration object or MongoDB connection string
   *
   * When a connection string is provided, the following mappings are applied:
   * - Database name from the URI path
   * - Credentials (username/password) are stored for potential future use
   * - Connection options are parsed and stored
   *
   * Note: MongoLake is a lakehouse, not a direct MongoDB replacement.
   * Connection strings are supported for compatibility, but storage
   * is configured separately via the config object or defaults to local.
   */
  constructor(configOrUri: MongoLakeConfig | string = {}) {
    // Parse connection string if provided
    if (typeof configOrUri === 'string') {
      if (!isConnectionString(configOrUri)) {
        throw new Error(
          'Invalid connection string. Must start with mongodb:// or mongodb+srv://'
        );
      }
      this.parsedConnectionString = parseConnectionString(configOrUri);
      const parsedConfig: MongoLakeConfig = {
        database: this.parsedConnectionString.database || 'default',
        // Store connection info for potential use by storage backends
        connectionString: {
          hosts: this.parsedConnectionString.hosts,
          username: this.parsedConnectionString.username,
          password: this.parsedConnectionString.password,
          options: this.parsedConnectionString.options,
        },
      };
      // Validate the parsed configuration
      validateConfig(parsedConfig);
      this.config = mergeWithDefaults(parsedConfig);
    } else {
      // Validate the configuration object
      validateConfig(configOrUri);
      this.config = mergeWithDefaults(configOrUri);
    }

    this.storage = createStorage(this.config);
    this.sessionStore = new SessionStore({
      timeoutMs: 30 * 60 * 1000, // 30 minutes
      cleanupIntervalMs: 60000,  // 1 minute
    });
  }

  /**
   * Get the parsed connection string if the client was created with one
   *
   * @returns Parsed connection string or undefined
   */
  getConnectionInfo(): ParsedConnectionString | undefined {
    return this.parsedConnectionString;
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
   * Execute buffered transaction operations atomically with rollback support.
   *
   * This method tracks the original state of documents before modification.
   * If any operation fails, all previously applied changes are rolled back
   * to ensure atomicity.
   *
   * @internal
   */
  private async executeTransactionOperations(
    operations: BufferedOperation[]
  ): Promise<void> {
    // Track original document states for potential rollback
    const snapshots: DocumentSnapshot[] = [];

    try {
      for (const op of operations) {
        const db = this.db(op.database);
        const collection = db.collection(op.collection);

        switch (op.type) {
          case 'insert':
            if (op.document) {
              // Create a copy of the document to insert
              const docToInsert = { ...op.document } as Document;

              // Generate _id if not provided (matching insertMany behavior)
              if (!docToInsert._id) {
                docToInsert._id = crypto.randomUUID();
              }

              const docId = extractDocumentId(docToInsert);
              // Record that this document didn't exist before
              snapshots.push({
                database: op.database,
                collection: op.collection,
                documentId: docId,
                originalDocument: null,
                operationType: 'insert',
              });
              await collection.insertOne(docToInsert);
            }
            break;

          case 'update':
            if (op.filter && op.update) {
              // Capture original document state before update
              const originalDoc = await collection.findOne(
                op.filter as Filter<Document>
              );
              if (originalDoc) {
                const docId = extractDocumentId(originalDoc);
                snapshots.push({
                  database: op.database,
                  collection: op.collection,
                  documentId: docId,
                  originalDocument: { ...originalDoc },
                  operationType: 'update',
                });
              }
              await collection.updateOne(
                op.filter as Filter<Document>,
                op.update as Update<Document>,
                op.options as UpdateOptions
              );
            }
            break;

          case 'replace':
            if (op.filter && op.replacement) {
              // Capture original document state before replace
              const originalDoc = await collection.findOne(
                op.filter as Filter<Document>
              );
              if (originalDoc) {
                const docId = extractDocumentId(originalDoc);
                snapshots.push({
                  database: op.database,
                  collection: op.collection,
                  documentId: docId,
                  originalDocument: { ...originalDoc },
                  operationType: 'replace',
                });
              }
              await collection.replaceOne(
                op.filter as Filter<Document>,
                op.replacement as Document,
                op.options as UpdateOptions
              );
            }
            break;

          case 'delete':
            if (op.filter) {
              // Capture original document state before delete
              const originalDoc = await collection.findOne(
                op.filter as Filter<Document>
              );
              if (originalDoc) {
                const docId = extractDocumentId(originalDoc);
                snapshots.push({
                  database: op.database,
                  collection: op.collection,
                  documentId: docId,
                  originalDocument: { ...originalDoc },
                  operationType: 'delete',
                });
              }
              await collection.deleteOne(
                op.filter as Filter<Document>,
                op.options as DeleteOptions
              );
            }
            break;
        }
      }
    } catch (error) {
      // Rollback all applied changes in reverse order
      await this.rollbackSnapshots(snapshots);
      throw error;
    }
  }

  /**
   * Rollback document changes using captured snapshots.
   * Processes snapshots in reverse order to properly undo changes.
   *
   * @param snapshots - Array of document snapshots to restore
   * @internal
   */
  private async rollbackSnapshots(snapshots: DocumentSnapshot[]): Promise<void> {
    // Process in reverse order to undo changes correctly
    for (let i = snapshots.length - 1; i >= 0; i--) {
      const snapshot = snapshots[i]!;
      const db = this.db(snapshot.database);
      const collection = db.collection(snapshot.collection);

      try {
        switch (snapshot.operationType) {
          case 'insert':
            // Remove the inserted document
            await collection.deleteOne({ _id: snapshot.documentId });
            break;

          case 'update':
          case 'replace':
            // Restore original document state
            if (snapshot.originalDocument) {
              await collection.replaceOne(
                { _id: snapshot.documentId },
                snapshot.originalDocument
              );
            }
            break;

          case 'delete':
            // Re-insert the deleted document
            if (snapshot.originalDocument) {
              await collection.insertOne(snapshot.originalDocument);
            }
            break;
        }
      } catch (rollbackError) {
        // Log rollback errors but continue with remaining rollbacks
        logger.error('Rollback failed', {
          database: snapshot.database,
          collection: snapshot.collection,
          documentId: snapshot.documentId,
          error: rollbackError instanceof Error ? rollbackError : String(rollbackError),
        });
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
        const dbName = parts[0];
        try {
          validateDatabaseName(dbName);
          databases.add(dbName);
        } catch {
          // Skip invalid database names (system files, etc.)
        }
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
