/**
 * Dual-Run Test Framework
 *
 * FerretDB-style compatibility testing pattern:
 * Run identical operations on both MongoLake and real MongoDB,
 * then compare results for exact match.
 *
 * This is the most valuable pattern for finding subtle incompatibilities.
 */

import { MongoClient, Db, Collection as MongoCollection, Document as MongoDocument } from 'mongodb';
import { MongoMemoryServer } from 'mongodb-memory-server';
import { MongoLake, Database, Collection, type Document } from '../../../src/client/index.js';
import { MemoryStorage } from '../../../src/storage/index.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Result from running an operation on both databases
 */
export interface DualRunResult<T = unknown> {
  /** Result from MongoLake */
  mongolake: T;
  /** Result from real MongoDB */
  mongodb: T;
  /** Whether results match exactly */
  match: boolean;
  /** Detailed diff if results don't match */
  diff?: ResultDiff;
}

/**
 * Detailed difference between two results
 */
export interface ResultDiff {
  /** Type of difference */
  type: 'value' | 'type' | 'array_length' | 'object_keys' | 'missing_key' | 'extra_key';
  /** Path to the differing value (dot notation) */
  path: string;
  /** Value from MongoLake */
  mongolakeValue: unknown;
  /** Value from MongoDB */
  mongodbValue: unknown;
  /** Human-readable description */
  description: string;
}

/**
 * Operation function signature for dual-run tests
 */
export type DualOperation<T> = (db: {
  collection: <D extends Document = Document>(name: string) => DualCollection<D>;
  dropCollection: (name: string) => Promise<void>;
}) => Promise<T>;

/**
 * Collection interface that works with both MongoLake and MongoDB
 */
export interface DualCollection<T extends Document = Document> {
  insertOne(doc: T): Promise<{ insertedId: unknown }>;
  insertMany(docs: T[]): Promise<{ insertedIds: Record<number, unknown>; insertedCount: number }>;
  findOne(filter: Record<string, unknown>): Promise<T | null>;
  find(filter?: Record<string, unknown>): Promise<T[]>;
  updateOne(filter: Record<string, unknown>, update: Record<string, unknown>): Promise<{ matchedCount: number; modifiedCount: number }>;
  updateMany(filter: Record<string, unknown>, update: Record<string, unknown>): Promise<{ matchedCount: number; modifiedCount: number }>;
  deleteOne(filter: Record<string, unknown>): Promise<{ deletedCount: number }>;
  deleteMany(filter: Record<string, unknown>): Promise<{ deletedCount: number }>;
  countDocuments(filter?: Record<string, unknown>): Promise<number>;
}

// ============================================================================
// Dual Database Context
// ============================================================================

/**
 * Holds connections to both MongoLake and MongoDB
 */
export class DualDatabaseContext {
  private mongoServer: MongoMemoryServer | null = null;
  private mongoClient: MongoClient | null = null;
  private mongoDb: Db | null = null;
  private mongolakeClient: MongoLake | null = null;
  private mongolakeDb: Database | null = null;
  private dbName: string;

  constructor(dbName: string = 'compat_test') {
    this.dbName = dbName;
  }

  /**
   * Initialize both databases
   */
  async setup(): Promise<void> {
    // Start MongoDB Memory Server
    this.mongoServer = await MongoMemoryServer.create();
    const mongoUri = this.mongoServer.getUri();

    // Connect to real MongoDB
    this.mongoClient = new MongoClient(mongoUri);
    await this.mongoClient.connect();
    this.mongoDb = this.mongoClient.db(this.dbName);

    // Create MongoLake with MemoryStorage
    const storage = new MemoryStorage();
    this.mongolakeClient = new MongoLake({ database: this.dbName });
    // @ts-expect-error - accessing private field for testing
    this.mongolakeClient.storage = storage;
    this.mongolakeDb = this.mongolakeClient.db(this.dbName);
  }

  /**
   * Clean up both databases
   */
  async teardown(): Promise<void> {
    if (this.mongoClient) {
      await this.mongoClient.close();
      this.mongoClient = null;
    }

    if (this.mongoServer) {
      await this.mongoServer.stop();
      this.mongoServer = null;
    }

    if (this.mongolakeClient) {
      await this.mongolakeClient.close();
      this.mongolakeClient = null;
    }

    this.mongoDb = null;
    this.mongolakeDb = null;
  }

  /**
   * Get collection wrappers for both databases
   */
  getCollections<T extends Document = Document>(name: string): {
    mongolake: Collection<T>;
    mongodb: MongoCollection<MongoDocument>;
  } {
    if (!this.mongolakeDb || !this.mongoDb) {
      throw new Error('Databases not initialized. Call setup() first.');
    }

    return {
      mongolake: this.mongolakeDb.collection<T>(name),
      mongodb: this.mongoDb.collection(name),
    };
  }

  /**
   * Drop a collection from both databases
   */
  async dropCollection(name: string): Promise<void> {
    if (!this.mongoDb || !this.mongolakeDb) {
      throw new Error('Databases not initialized. Call setup() first.');
    }

    try {
      await this.mongoDb.dropCollection(name);
    } catch {
      // Ignore if collection doesn't exist
    }

    // MongoLake drop is via database method
    // For now, just clear the collection
    const collection = this.mongolakeDb.collection(name);
    await collection.deleteMany({});
  }

  /**
   * Get MongoDB database (for direct access in tests)
   */
  getMongoDB(): Db {
    if (!this.mongoDb) {
      throw new Error('MongoDB not initialized. Call setup() first.');
    }
    return this.mongoDb;
  }

  /**
   * Get MongoLake database (for direct access in tests)
   */
  getMongoLake(): Database {
    if (!this.mongolakeDb) {
      throw new Error('MongoLake not initialized. Call setup() first.');
    }
    return this.mongolakeDb;
  }
}

// ============================================================================
// Comparison Functions
// ============================================================================

/**
 * Deep compare two values, returning differences
 */
export function compareResults<T>(
  mongolakeResult: T,
  mongodbResult: T,
  path: string = ''
): ResultDiff[] {
  const diffs: ResultDiff[] = [];

  // Handle null/undefined
  if (mongolakeResult === null && mongodbResult === null) {
    return diffs;
  }
  if (mongolakeResult === undefined && mongodbResult === undefined) {
    return diffs;
  }
  if (mongolakeResult === null || mongolakeResult === undefined ||
      mongodbResult === null || mongodbResult === undefined) {
    diffs.push({
      type: 'value',
      path: path || '(root)',
      mongolakeValue: mongolakeResult,
      mongodbValue: mongodbResult,
      description: `Null/undefined mismatch at ${path || 'root'}`,
    });
    return diffs;
  }

  // Type comparison
  const mongolakeType = Array.isArray(mongolakeResult) ? 'array' : typeof mongolakeResult;
  const mongodbType = Array.isArray(mongodbResult) ? 'array' : typeof mongodbResult;

  if (mongolakeType !== mongodbType) {
    diffs.push({
      type: 'type',
      path: path || '(root)',
      mongolakeValue: mongolakeType,
      mongodbValue: mongodbType,
      description: `Type mismatch at ${path || 'root'}: MongoLake=${mongolakeType}, MongoDB=${mongodbType}`,
    });
    return diffs;
  }

  // Array comparison
  if (Array.isArray(mongolakeResult) && Array.isArray(mongodbResult)) {
    if (mongolakeResult.length !== mongodbResult.length) {
      diffs.push({
        type: 'array_length',
        path: path || '(root)',
        mongolakeValue: mongolakeResult.length,
        mongodbValue: mongodbResult.length,
        description: `Array length mismatch at ${path || 'root'}: MongoLake=${mongolakeResult.length}, MongoDB=${mongodbResult.length}`,
      });
    }

    const minLen = Math.min(mongolakeResult.length, mongodbResult.length);
    for (let i = 0; i < minLen; i++) {
      diffs.push(...compareResults(mongolakeResult[i], mongodbResult[i], `${path}[${i}]`));
    }
    return diffs;
  }

  // Object comparison
  if (typeof mongolakeResult === 'object' && typeof mongodbResult === 'object') {
    const mongolakeObj = mongolakeResult as Record<string, unknown>;
    const mongodbObj = mongodbResult as Record<string, unknown>;

    // Normalize ObjectId-like values for comparison
    const normalizedMongolake = normalizeForComparison(mongolakeObj);
    const normalizedMongodb = normalizeForComparison(mongodbObj);

    const allKeys = new Set([
      ...Object.keys(normalizedMongolake),
      ...Object.keys(normalizedMongodb),
    ]);

    for (const key of allKeys) {
      const currentPath = path ? `${path}.${key}` : key;

      if (!(key in normalizedMongolake)) {
        diffs.push({
          type: 'missing_key',
          path: currentPath,
          mongolakeValue: undefined,
          mongodbValue: normalizedMongodb[key],
          description: `Key missing in MongoLake: ${currentPath}`,
        });
      } else if (!(key in normalizedMongodb)) {
        diffs.push({
          type: 'extra_key',
          path: currentPath,
          mongolakeValue: normalizedMongolake[key],
          mongodbValue: undefined,
          description: `Extra key in MongoLake: ${currentPath}`,
        });
      } else {
        diffs.push(...compareResults(normalizedMongolake[key], normalizedMongodb[key], currentPath));
      }
    }

    return diffs;
  }

  // Primitive comparison
  if (mongolakeResult !== mongodbResult) {
    diffs.push({
      type: 'value',
      path: path || '(root)',
      mongolakeValue: mongolakeResult,
      mongodbValue: mongodbResult,
      description: `Value mismatch at ${path || 'root'}: MongoLake=${JSON.stringify(mongolakeResult)}, MongoDB=${JSON.stringify(mongodbResult)}`,
    });
  }

  return diffs;
}

/**
 * Normalize values for comparison (handles ObjectId, Date, etc.)
 */
function normalizeForComparison(value: unknown): unknown {
  if (value === null || value === undefined) {
    return value;
  }

  // Handle ObjectId-like values (convert to string)
  if (typeof value === 'object' && value !== null) {
    const obj = value as Record<string, unknown>;

    // Check for ObjectId (MongoDB returns ObjectId, MongoLake may return string)
    if ('toHexString' in obj && typeof obj.toHexString === 'function') {
      return (obj.toHexString as () => string)();
    }
    if ('toString' in obj && typeof obj.toString === 'function' &&
        obj.constructor?.name === 'ObjectId') {
      return obj.toString();
    }

    // Handle Date objects
    if (value instanceof Date) {
      return value.toISOString();
    }

    // Recursively normalize arrays
    if (Array.isArray(value)) {
      return value.map(normalizeForComparison);
    }

    // Recursively normalize objects
    const normalized: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(obj)) {
      normalized[k] = normalizeForComparison(v);
    }
    return normalized;
  }

  return value;
}

/**
 * Check if two results match exactly
 */
export function resultsMatch<T>(mongolakeResult: T, mongodbResult: T): boolean {
  const diffs = compareResults(mongolakeResult, mongodbResult);
  return diffs.length === 0;
}

/**
 * Format differences for readable output
 */
export function formatDiffs(diffs: ResultDiff[]): string {
  if (diffs.length === 0) {
    return 'No differences found';
  }

  const lines = ['Differences found:'];
  for (const diff of diffs) {
    lines.push(`  - [${diff.type}] ${diff.description}`);
    if (diff.mongolakeValue !== undefined) {
      lines.push(`      MongoLake: ${JSON.stringify(diff.mongolakeValue)}`);
    }
    if (diff.mongodbValue !== undefined) {
      lines.push(`      MongoDB:   ${JSON.stringify(diff.mongodbValue)}`);
    }
  }
  return lines.join('\n');
}

// ============================================================================
// Dual-Run Test Helpers
// ============================================================================

/**
 * Setup dual databases before tests
 */
export async function setupDualDatabases(): Promise<DualDatabaseContext> {
  const context = new DualDatabaseContext();
  await context.setup();
  return context;
}

/**
 * Run an operation on both databases and compare results
 */
export async function runOnBoth<T>(
  context: DualDatabaseContext,
  collectionName: string,
  operation: (collection: DualCollection) => Promise<T>
): Promise<DualRunResult<T>> {
  const { mongolake, mongodb } = context.getCollections(collectionName);

  // Create unified collection interface for MongoLake
  const mongolakeWrapper: DualCollection = {
    async insertOne(doc) {
      const result = await mongolake.insertOne(doc);
      return { insertedId: result.insertedId };
    },
    async insertMany(docs) {
      const result = await mongolake.insertMany(docs);
      return {
        insertedIds: result.insertedIds,
        insertedCount: result.insertedCount,
      };
    },
    async findOne(filter) {
      return (await mongolake.findOne(filter)) as Document | null;
    },
    async find(filter = {}) {
      return await mongolake.find(filter).toArray();
    },
    async updateOne(filter, update) {
      const result = await mongolake.updateOne(filter, update);
      return {
        matchedCount: result.matchedCount,
        modifiedCount: result.modifiedCount,
      };
    },
    async updateMany(filter, update) {
      const result = await mongolake.updateMany(filter, update);
      return {
        matchedCount: result.matchedCount,
        modifiedCount: result.modifiedCount,
      };
    },
    async deleteOne(filter) {
      const result = await mongolake.deleteOne(filter);
      return { deletedCount: result.deletedCount };
    },
    async deleteMany(filter) {
      const result = await mongolake.deleteMany(filter);
      return { deletedCount: result.deletedCount };
    },
    async countDocuments(filter = {}) {
      return await mongolake.countDocuments(filter);
    },
  };

  // Create unified collection interface for MongoDB
  const mongodbWrapper: DualCollection = {
    async insertOne(doc) {
      const result = await mongodb.insertOne(doc as MongoDocument);
      return { insertedId: result.insertedId };
    },
    async insertMany(docs) {
      const result = await mongodb.insertMany(docs as MongoDocument[]);
      const insertedIds: Record<number, unknown> = {};
      for (let i = 0; i < docs.length; i++) {
        insertedIds[i] = result.insertedIds[i];
      }
      return {
        insertedIds,
        insertedCount: result.insertedCount,
      };
    },
    async findOne(filter) {
      return (await mongodb.findOne(filter)) as Document | null;
    },
    async find(filter = {}) {
      return (await mongodb.find(filter).toArray()) as Document[];
    },
    async updateOne(filter, update) {
      const result = await mongodb.updateOne(filter, update);
      return {
        matchedCount: result.matchedCount,
        modifiedCount: result.modifiedCount,
      };
    },
    async updateMany(filter, update) {
      const result = await mongodb.updateMany(filter, update);
      return {
        matchedCount: result.matchedCount,
        modifiedCount: result.modifiedCount,
      };
    },
    async deleteOne(filter) {
      const result = await mongodb.deleteOne(filter);
      return { deletedCount: result.deletedCount };
    },
    async deleteMany(filter) {
      const result = await mongodb.deleteMany(filter);
      return { deletedCount: result.deletedCount };
    },
    async countDocuments(filter = {}) {
      return await mongodb.countDocuments(filter);
    },
  };

  // Run operation on both
  const mongolakeResult = await operation(mongolakeWrapper);
  const mongodbResult = await operation(mongodbWrapper);

  // Compare results
  const diffs = compareResults(mongolakeResult, mongodbResult);

  return {
    mongolake: mongolakeResult,
    mongodb: mongodbResult,
    match: diffs.length === 0,
    diff: diffs.length > 0 ? diffs[0] : undefined,
  };
}

/**
 * Teardown dual databases after tests
 */
export async function teardownDualDatabases(context: DualDatabaseContext): Promise<void> {
  await context.teardown();
}

/**
 * Run a dual-run test with automatic setup/teardown
 */
export async function dualRunTest<T>(
  collectionName: string,
  operation: (collection: DualCollection) => Promise<T>
): Promise<DualRunResult<T>> {
  const context = await setupDualDatabases();
  try {
    return await runOnBoth(context, collectionName, operation);
  } finally {
    await teardownDualDatabases(context);
  }
}

/**
 * Assert that both databases produce the same result
 */
export function assertDualRunMatch<T>(result: DualRunResult<T>): void {
  if (!result.match) {
    const diffs = compareResults(result.mongolake, result.mongodb);
    throw new Error(
      `MongoLake and MongoDB results do not match!\n${formatDiffs(diffs)}\n\n` +
      `MongoLake result: ${JSON.stringify(result.mongolake, null, 2)}\n\n` +
      `MongoDB result: ${JSON.stringify(result.mongodb, null, 2)}`
    );
  }
}
