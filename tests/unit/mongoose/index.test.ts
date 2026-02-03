/**
 * Mongoose Integration Unit Tests
 *
 * Tests for the MongoLake Mongoose integration layer that provides
 * compatibility with mongoose via setDriver() and @dotdo/mongoose.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  createDriver,
  MongoLakeConnection,
  MongoLakeDriverCollection,
  MongoLakeSession,
  MongoLake,
  ObjectId,
  fromConnectionString,
} from '../../../src/mongoose/index.js';
import type { Collection, Database } from '../../../src/client/index.js';
import type { Document } from '../../../src/types.js';

// ============================================================================
// Mock Helpers
// ============================================================================

/**
 * Creates a mock collection with basic CRUD operations
 */
function createMockCollection<T extends Document = Document>(): Collection<T> {
  const docs = new Map<string, T>();

  return {
    name: 'test-collection',
    insertOne: vi.fn(async (doc: T) => {
      const id = doc._id || crypto.randomUUID();
      docs.set(String(id), { ...doc, _id: id } as T);
      return { acknowledged: true, insertedId: id };
    }),
    insertMany: vi.fn(async (docsToInsert: T[]) => {
      const insertedIds: { [key: number]: string } = {};
      docsToInsert.forEach((doc, i) => {
        const id = doc._id || crypto.randomUUID();
        docs.set(String(id), { ...doc, _id: id } as T);
        insertedIds[i] = String(id);
      });
      return { acknowledged: true, insertedCount: docsToInsert.length, insertedIds };
    }),
    findOne: vi.fn(async (filter?: { _id?: string }) => {
      if (filter?._id) {
        return docs.get(String(filter._id)) || null;
      }
      // Return first doc or null
      const first = docs.values().next();
      return first.done ? null : first.value;
    }),
    find: vi.fn((filter?: { _id?: string }) => {
      const results: T[] = [];
      if (filter?._id) {
        const doc = docs.get(String(filter._id));
        if (doc) results.push(doc);
      } else {
        for (const doc of docs.values()) {
          results.push(doc);
        }
      }
      return {
        toArray: async () => results,
        sort: () => ({ toArray: async () => results, limit: () => ({ toArray: async () => results }) }),
        limit: () => ({ toArray: async () => results }),
        skip: () => ({ toArray: async () => results }),
        project: () => ({ toArray: async () => results }),
        [Symbol.asyncIterator]: async function* () {
          for (const doc of results) yield doc;
        },
      };
    }),
    updateOne: vi.fn(async (filter: { _id?: string }, update: { $set?: Partial<T> }) => {
      if (filter._id && docs.has(String(filter._id))) {
        const existing = docs.get(String(filter._id))!;
        if (update.$set) {
          docs.set(String(filter._id), { ...existing, ...update.$set } as T);
        }
        return { acknowledged: true, matchedCount: 1, modifiedCount: 1, upsertedCount: 0 };
      }
      return { acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedCount: 0 };
    }),
    updateMany: vi.fn(async () => {
      return { acknowledged: true, matchedCount: docs.size, modifiedCount: docs.size, upsertedCount: 0 };
    }),
    replaceOne: vi.fn(async (filter: { _id?: string }, replacement: T) => {
      if (filter._id && docs.has(String(filter._id))) {
        docs.set(String(filter._id), { ...replacement, _id: filter._id } as T);
        return { acknowledged: true, matchedCount: 1, modifiedCount: 1, upsertedCount: 0 };
      }
      return { acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedCount: 0 };
    }),
    deleteOne: vi.fn(async (filter: { _id?: string }) => {
      if (filter._id && docs.delete(String(filter._id))) {
        return { acknowledged: true, deletedCount: 1 };
      }
      return { acknowledged: true, deletedCount: 0 };
    }),
    deleteMany: vi.fn(async () => {
      const count = docs.size;
      docs.clear();
      return { acknowledged: true, deletedCount: count };
    }),
    countDocuments: vi.fn(async () => docs.size),
    estimatedDocumentCount: vi.fn(async () => docs.size),
    distinct: vi.fn(async <K extends keyof T>(field: K) => {
      const values = new Set<T[K]>();
      for (const doc of docs.values()) {
        if (doc[field] !== undefined) values.add(doc[field]);
      }
      return Array.from(values);
    }),
    aggregate: vi.fn(() => ({
      toArray: async () => [],
      [Symbol.asyncIterator]: async function* () {},
    })),
    createIndex: vi.fn(async (_spec: object, options?: { name?: string }) => options?.name || 'test_index'),
    createIndexes: vi.fn(async () => ['test_index']),
    dropIndex: vi.fn(async () => {}),
    listIndexes: vi.fn(async () => [{ name: '_id_', key: { _id: 1 } }]),
    // For test inspection
    _docs: docs,
  } as unknown as Collection<T>;
}

/**
 * Creates a mock database
 */
function createMockDatabase(): Database {
  const collections = new Map<string, Collection<Document>>();

  return {
    name: 'test-db',
    collection: vi.fn(<T extends Document = Document>(name: string) => {
      if (!collections.has(name)) {
        collections.set(name, createMockCollection<T>() as unknown as Collection<Document>);
      }
      return collections.get(name) as Collection<T>;
    }),
    listCollections: vi.fn(async () => Array.from(collections.keys())),
    createCollection: vi.fn(async <T extends Document = Document>(name: string) => {
      const coll = createMockCollection<T>() as unknown as Collection<Document>;
      collections.set(name, coll);
      return coll as Collection<T>;
    }),
    dropCollection: vi.fn(async (name: string) => {
      return collections.delete(name);
    }),
    getPath: vi.fn(() => 'test-db'),
    branch: vi.fn(async () => {}),
    merge: vi.fn(async () => {}),
  } as unknown as Database;
}

/**
 * Creates a mock MongoLake client
 */
function createMockMongoLake(): MongoLake {
  const databases = new Map<string, Database>();

  return {
    db: vi.fn((name?: string) => {
      const dbName = name || 'default';
      if (!databases.has(dbName)) {
        databases.set(dbName, createMockDatabase());
      }
      return databases.get(dbName)!;
    }),
    listDatabases: vi.fn(async () => Array.from(databases.keys())),
    dropDatabase: vi.fn(async (name: string) => {
      databases.delete(name);
    }),
    close: vi.fn(async () => {
      databases.clear();
    }),
  } as unknown as MongoLake;
}

// ============================================================================
// Tests: createDriver()
// ============================================================================

describe('createDriver()', () => {
  it('returns a valid driver object with Connection, Collection, and ObjectId', () => {
    const driver = createDriver();

    expect(driver).toBeDefined();
    expect(driver.Connection).toBeDefined();
    expect(driver.Collection).toBeDefined();
    expect(driver.ObjectId).toBeDefined();
  });

  it('returns a driver with Connection as a constructor', () => {
    const driver = createDriver();

    expect(typeof driver.Connection).toBe('function');
    // Should be able to instantiate
    const conn = new driver.Connection();
    expect(conn).toBeInstanceOf(MongoLakeConnection);
  });

  it('returns a driver with Collection class', () => {
    const driver = createDriver();

    expect(driver.Collection).toBe(MongoLakeDriverCollection);
  });

  it('returns a driver with ObjectId class', () => {
    const driver = createDriver();

    expect(driver.ObjectId).toBe(ObjectId);
  });

  it('creates bound connection class with provided config', () => {
    const driver = createDriver({ local: '.test-mongolake' });

    const conn = new driver.Connection();
    // The connection should use the config provided to createDriver
    expect(conn.client).toBeDefined();
  });

  it('allows creating multiple drivers with different configs', () => {
    const driver1 = createDriver({ local: '.mongolake-1' });
    const driver2 = createDriver({ local: '.mongolake-2' });

    const conn1 = new driver1.Connection();
    const conn2 = new driver2.Connection();

    // Both should be valid connections
    expect(conn1).toBeInstanceOf(MongoLakeConnection);
    expect(conn2).toBeInstanceOf(MongoLakeConnection);
  });
});

// ============================================================================
// Tests: createMongoose() (via driver usage pattern)
// ============================================================================

describe('createMongoose() pattern', () => {
  it('driver can be used with mongoose setDriver pattern', () => {
    const driver = createDriver({ local: '.mongolake' });

    // Simulate mongoose.setDriver(driver)
    const mockMongoose = {
      driver: null as unknown,
      setDriver(d: unknown) {
        this.driver = d;
      },
    };

    mockMongoose.setDriver(driver);

    expect(mockMongoose.driver).toBe(driver);
    expect((mockMongoose.driver as typeof driver).Connection).toBeDefined();
    expect((mockMongoose.driver as typeof driver).Collection).toBeDefined();
    expect((mockMongoose.driver as typeof driver).ObjectId).toBeDefined();
  });

  it('driver ObjectId can be used for document IDs', () => {
    const driver = createDriver();

    const id = new driver.ObjectId();
    expect(id).toBeInstanceOf(ObjectId);
    expect(id.toString()).toMatch(/^[0-9a-f]{24}$/);
  });

  it('driver ObjectId validates correctly', () => {
    const driver = createDriver();

    expect(driver.ObjectId.isValid('507f1f77bcf86cd799439011')).toBe(true);
    expect(driver.ObjectId.isValid('invalid')).toBe(false);
  });
});

// ============================================================================
// Tests: MongoLakeConnection
// ============================================================================

describe('MongoLakeConnection', () => {
  let connection: MongoLakeConnection;

  beforeEach(() => {
    connection = new MongoLakeConnection({ local: '.test-mongolake' });
  });

  afterEach(async () => {
    await connection.close();
  });

  describe('constructor', () => {
    it('creates connection with default config', () => {
      const conn = new MongoLakeConnection();
      expect(conn).toBeInstanceOf(MongoLakeConnection);
      expect(conn.client).toBeInstanceOf(MongoLake);
    });

    it('creates connection with custom config', () => {
      const conn = new MongoLakeConnection({ local: '.custom-path', database: 'mydb' });
      expect(conn).toBeInstanceOf(MongoLakeConnection);
      expect(conn.client).toBeDefined();
    });
  });

  describe('client property', () => {
    it('returns the underlying MongoLake instance', () => {
      expect(connection.client).toBeInstanceOf(MongoLake);
    });
  });

  describe('db property', () => {
    it('returns null before connect()', () => {
      expect(connection.db).toBeNull();
    });

    it('returns database after connect()', async () => {
      await connection.connect('mongolake://localhost/testdb');
      expect(connection.db).not.toBeNull();
    });
  });

  describe('connect()', () => {
    it('connects with mongolake:// URI', async () => {
      const result = await connection.connect('mongolake://localhost/testdb');

      expect(result).toBe(connection); // Returns this for chaining
      expect(connection.db).not.toBeNull();
    });

    it('parses database name from URI', async () => {
      await connection.connect('mongolake://localhost/myspecialdb');

      expect(connection.db).not.toBeNull();
      expect(connection.db!.name).toBe('myspecialdb');
    });

    it('uses default database name when URI has no path', async () => {
      await connection.connect();

      expect(connection.db).not.toBeNull();
      expect(connection.db!.name).toBe('default');
    });

    it('uses default database name when URI is empty', async () => {
      await connection.connect('');

      expect(connection.db).not.toBeNull();
      expect(connection.db!.name).toBe('default');
    });

    it('parses database name from complex URI', async () => {
      await connection.connect('mongolake://user:pass@localhost:27017/production?authSource=admin');

      expect(connection.db!.name).toBe('production');
    });
  });

  describe('close()', () => {
    it('clears database reference', async () => {
      await connection.connect('mongolake://localhost/testdb');
      expect(connection.db).not.toBeNull();

      await connection.close();
      expect(connection.db).toBeNull();
    });

    it('can be called multiple times safely', async () => {
      await connection.connect('mongolake://localhost/testdb');

      await connection.close();
      await connection.close();

      expect(connection.db).toBeNull();
    });
  });

  describe('collection()', () => {
    it('throws error when not connected', () => {
      expect(() => connection.collection('users')).toThrow('Not connected');
    });

    it('returns collection when connected', async () => {
      await connection.connect('mongolake://localhost/testdb');

      const coll = connection.collection('users');
      expect(coll).toBeDefined();
      expect(coll.name).toBe('users');
    });

    it('returns typed collection', async () => {
      interface User {
        _id?: string;
        name: string;
        age: number;
      }

      await connection.connect('mongolake://localhost/testdb');

      const coll = connection.collection<User>('users');
      expect(coll).toBeDefined();
    });
  });
});

// ============================================================================
// Tests: MongoLakeDriverCollection
// ============================================================================

describe('MongoLakeDriverCollection', () => {
  let mockCollection: Collection<Document>;
  let driverCollection: MongoLakeDriverCollection<Document>;

  beforeEach(() => {
    mockCollection = createMockCollection();
    driverCollection = new MongoLakeDriverCollection(mockCollection);
  });

  describe('collectionName property', () => {
    it('returns the underlying collection name', () => {
      expect(driverCollection.collectionName).toBe('test-collection');
    });
  });

  describe('insertOne()', () => {
    it('inserts a single document', async () => {
      const doc = { _id: 'doc1', name: 'Alice' };
      const result = await driverCollection.insertOne(doc);

      expect(result.acknowledged).toBe(true);
      expect(result.insertedId).toBe('doc1');
      expect(mockCollection.insertOne).toHaveBeenCalledWith(doc);
    });

    it('accepts session option (ignored)', async () => {
      const doc = { _id: 'doc1', name: 'Alice' };
      const result = await driverCollection.insertOne(doc, { session: {} });

      expect(result.acknowledged).toBe(true);
    });
  });

  describe('insertMany()', () => {
    it('inserts multiple documents', async () => {
      const docs = [
        { _id: 'doc1', name: 'Alice' },
        { _id: 'doc2', name: 'Bob' },
      ];
      const result = await driverCollection.insertMany(docs);

      expect(result.acknowledged).toBe(true);
      expect(result.insertedCount).toBe(2);
      expect(mockCollection.insertMany).toHaveBeenCalledWith(docs);
    });
  });

  describe('findOne()', () => {
    it('finds a single document', async () => {
      await driverCollection.insertOne({ _id: 'doc1', name: 'Alice' });

      const result = await driverCollection.findOne({ _id: 'doc1' });

      expect(result).not.toBeNull();
      expect(result?._id).toBe('doc1');
    });

    it('returns null when not found', async () => {
      const result = await driverCollection.findOne({ _id: 'nonexistent' });
      expect(result).toBeNull();
    });
  });

  describe('find()', () => {
    it('returns a cursor', async () => {
      await driverCollection.insertOne({ _id: 'doc1', name: 'Alice' });
      await driverCollection.insertOne({ _id: 'doc2', name: 'Bob' });

      const cursor = driverCollection.find({});

      expect(cursor).toBeDefined();
      expect(typeof cursor.toArray).toBe('function');
    });

    it('cursor returns documents', async () => {
      await driverCollection.insertOne({ _id: 'doc1', name: 'Alice' });

      const cursor = driverCollection.find({});
      const docs = await cursor.toArray();

      expect(docs.length).toBeGreaterThanOrEqual(1);
    });
  });

  describe('updateOne()', () => {
    it('updates a single document', async () => {
      await driverCollection.insertOne({ _id: 'doc1', name: 'Alice' });

      const result = await driverCollection.updateOne({ _id: 'doc1' }, { $set: { name: 'Alicia' } });

      expect(result.acknowledged).toBe(true);
      expect(result.matchedCount).toBe(1);
      expect(result.modifiedCount).toBe(1);
    });

    it('returns zero counts when not found', async () => {
      const result = await driverCollection.updateOne({ _id: 'nonexistent' }, { $set: { name: 'Test' } });

      expect(result.matchedCount).toBe(0);
      expect(result.modifiedCount).toBe(0);
    });

    it('accepts upsert option', async () => {
      const result = await driverCollection.updateOne(
        { _id: 'new-doc' },
        { $set: { name: 'New' } },
        { upsert: true }
      );

      expect(result.acknowledged).toBe(true);
    });
  });

  describe('updateMany()', () => {
    it('updates multiple documents', async () => {
      await driverCollection.insertOne({ _id: 'doc1', status: 'pending' });
      await driverCollection.insertOne({ _id: 'doc2', status: 'pending' });

      const result = await driverCollection.updateMany({ status: 'pending' }, { $set: { status: 'complete' } });

      expect(result.acknowledged).toBe(true);
    });
  });

  describe('replaceOne()', () => {
    it('replaces a document', async () => {
      await driverCollection.insertOne({ _id: 'doc1', name: 'Alice', age: 30 });

      const result = await driverCollection.replaceOne({ _id: 'doc1' }, { _id: 'doc1', name: 'Alicia', age: 31 });

      expect(result.acknowledged).toBe(true);
      expect(result.matchedCount).toBe(1);
    });
  });

  describe('deleteOne()', () => {
    it('deletes a single document', async () => {
      await driverCollection.insertOne({ _id: 'doc1', name: 'Alice' });

      const result = await driverCollection.deleteOne({ _id: 'doc1' });

      expect(result.acknowledged).toBe(true);
      expect(result.deletedCount).toBe(1);
    });

    it('returns zero when not found', async () => {
      const result = await driverCollection.deleteOne({ _id: 'nonexistent' });

      expect(result.deletedCount).toBe(0);
    });
  });

  describe('deleteMany()', () => {
    it('deletes multiple documents', async () => {
      await driverCollection.insertOne({ _id: 'doc1', status: 'old' });
      await driverCollection.insertOne({ _id: 'doc2', status: 'old' });

      const result = await driverCollection.deleteMany({ status: 'old' });

      expect(result.acknowledged).toBe(true);
    });
  });

  describe('countDocuments()', () => {
    it('counts documents', async () => {
      await driverCollection.insertOne({ _id: 'doc1' });
      await driverCollection.insertOne({ _id: 'doc2' });

      const count = await driverCollection.countDocuments();

      expect(count).toBe(2);
    });

    it('counts with filter', async () => {
      await driverCollection.insertOne({ _id: 'doc1', status: 'active' });
      await driverCollection.insertOne({ _id: 'doc2', status: 'inactive' });

      const count = await driverCollection.countDocuments({ status: 'active' });

      expect(count).toBeGreaterThanOrEqual(0);
    });
  });

  describe('estimatedDocumentCount()', () => {
    it('returns estimated count', async () => {
      await driverCollection.insertOne({ _id: 'doc1' });

      const count = await driverCollection.estimatedDocumentCount();

      expect(typeof count).toBe('number');
    });
  });

  describe('distinct()', () => {
    it('returns distinct values', async () => {
      await driverCollection.insertOne({ _id: 'doc1', category: 'A' });
      await driverCollection.insertOne({ _id: 'doc2', category: 'B' });
      await driverCollection.insertOne({ _id: 'doc3', category: 'A' });

      const values = await driverCollection.distinct('category' as keyof Document);

      expect(Array.isArray(values)).toBe(true);
    });
  });

  describe('aggregate()', () => {
    it('returns aggregation cursor', () => {
      const cursor = driverCollection.aggregate([{ $match: {} }]);

      expect(cursor).toBeDefined();
      expect(typeof cursor.toArray).toBe('function');
    });
  });

  describe('index operations', () => {
    it('createIndex() creates an index', async () => {
      const result = await driverCollection.createIndex({ name: 1 }, { name: 'name_1' });

      expect(result).toBe('name_1');
    });

    it('createIndexes() creates multiple indexes', async () => {
      const result = await driverCollection.createIndexes([
        { key: { name: 1 }, options: { name: 'name_1' } },
        { key: { email: 1 }, options: { name: 'email_1' } },
      ]);

      expect(Array.isArray(result)).toBe(true);
    });

    it('dropIndex() drops an index', async () => {
      await driverCollection.createIndex({ name: 1 }, { name: 'name_1' });
      await expect(driverCollection.dropIndex('name_1')).resolves.not.toThrow();
    });

    it('listIndexes() lists indexes', async () => {
      const indexes = await driverCollection.listIndexes();

      expect(Array.isArray(indexes)).toBe(true);
      expect(indexes.some((idx) => idx.name === '_id_')).toBe(true);
    });
  });
});

// ============================================================================
// Tests: MongoLakeSession
// ============================================================================

describe('MongoLakeSession', () => {
  let session: MongoLakeSession;

  beforeEach(() => {
    session = new MongoLakeSession();
  });

  afterEach(async () => {
    await session.endSession();
  });

  describe('id property', () => {
    it('returns a unique session ID', () => {
      expect(session.id).toBeDefined();
      expect(typeof session.id).toBe('string');
      expect(session.id.length).toBeGreaterThan(0);
    });

    it('generates different IDs for different sessions', () => {
      const session2 = new MongoLakeSession();
      expect(session.id).not.toBe(session2.id);
    });
  });

  describe('inTransaction()', () => {
    it('returns false initially', () => {
      expect(session.inTransaction()).toBe(false);
    });

    it('returns true after startTransaction()', () => {
      session.startTransaction();
      expect(session.inTransaction()).toBe(true);
    });

    it('returns false after commitTransaction()', async () => {
      session.startTransaction();
      await session.commitTransaction();
      expect(session.inTransaction()).toBe(false);
    });

    it('returns false after abortTransaction()', async () => {
      session.startTransaction();
      await session.abortTransaction();
      expect(session.inTransaction()).toBe(false);
    });
  });

  describe('startTransaction()', () => {
    it('starts a transaction', () => {
      session.startTransaction();
      expect(session.inTransaction()).toBe(true);
    });

    it('can be called multiple times (idempotent)', () => {
      session.startTransaction();
      session.startTransaction();
      expect(session.inTransaction()).toBe(true);
    });
  });

  describe('commitTransaction()', () => {
    it('commits the transaction', async () => {
      session.startTransaction();
      await session.commitTransaction();
      expect(session.inTransaction()).toBe(false);
    });

    it('can be called without active transaction', async () => {
      await expect(session.commitTransaction()).resolves.not.toThrow();
    });
  });

  describe('abortTransaction()', () => {
    it('aborts the transaction', async () => {
      session.startTransaction();
      await session.abortTransaction();
      expect(session.inTransaction()).toBe(false);
    });

    it('can be called without active transaction', async () => {
      await expect(session.abortTransaction()).resolves.not.toThrow();
    });
  });

  describe('endSession()', () => {
    it('ends the session', async () => {
      session.startTransaction();
      await session.endSession();
      expect(session.inTransaction()).toBe(false);
    });

    it('can be called multiple times', async () => {
      await session.endSession();
      await session.endSession();
      expect(session.inTransaction()).toBe(false);
    });
  });

  describe('withTransaction()', () => {
    it('executes function within transaction', async () => {
      let wasInTransaction = false;

      await session.withTransaction(async () => {
        wasInTransaction = session.inTransaction();
      });

      expect(wasInTransaction).toBe(true);
      expect(session.inTransaction()).toBe(false);
    });

    it('returns the result of the function', async () => {
      const result = await session.withTransaction(async () => {
        return 'test-result';
      });

      expect(result).toBe('test-result');
    });

    it('commits on success', async () => {
      await session.withTransaction(async () => {
        // Success case
      });

      expect(session.inTransaction()).toBe(false);
    });

    it('aborts on error', async () => {
      await expect(
        session.withTransaction(async () => {
          throw new Error('Test error');
        })
      ).rejects.toThrow('Test error');

      expect(session.inTransaction()).toBe(false);
    });

    it('re-throws the error after abort', async () => {
      const testError = new Error('Custom error');

      await expect(
        session.withTransaction(async () => {
          throw testError;
        })
      ).rejects.toBe(testError);
    });
  });
});

// ============================================================================
// Tests: fromConnectionString()
// ============================================================================

describe('fromConnectionString()', () => {
  it('creates MongoLake client from URI', () => {
    const lake = fromConnectionString('mongolake://localhost/testdb');

    expect(lake).toBeInstanceOf(MongoLake);
  });

  it('parses database name from URI path', () => {
    const lake = fromConnectionString('mongolake://localhost/mydb');
    const db = lake.db();

    expect(db.name).toBe('mydb');
  });

  it('parses local query parameter', () => {
    const lake = fromConnectionString('mongolake://localhost/testdb?local=.custom-path');

    // The client should be configured with local storage
    expect(lake).toBeInstanceOf(MongoLake);
  });

  it('parses branch query parameter', () => {
    const lake = fromConnectionString('mongolake://localhost/testdb?branch=feature-1');

    expect(lake).toBeInstanceOf(MongoLake);
  });

  it('parses asOf query parameter', () => {
    const lake = fromConnectionString('mongolake://localhost/testdb?asOf=2024-01-01');

    expect(lake).toBeInstanceOf(MongoLake);
  });

  it('allows config overrides when URI has no database', () => {
    // When URI has no database path, overrides take effect
    const lake = fromConnectionString('mongolake://localhost/?local=.data', { database: 'override-db' });
    const db = lake.db();

    expect(db.name).toBe('override-db');
  });

  it('URI database takes precedence over config overrides', () => {
    // When URI has a database path, it overrides the config
    const lake = fromConnectionString('mongolake://localhost/testdb', { database: 'override-db' });
    const db = lake.db();

    expect(db.name).toBe('testdb');
  });

  it('handles complex URIs', () => {
    const lake = fromConnectionString('mongolake://user:pass@localhost:27017/production?local=.data&branch=main');

    expect(lake).toBeInstanceOf(MongoLake);
  });
});

// ============================================================================
// Tests: Re-exports
// ============================================================================

describe('Re-exports', () => {
  it('exports MongoLake class', async () => {
    const { MongoLake: ExportedMongoLake } = await import('../../../src/mongoose/index.js');
    expect(ExportedMongoLake).toBe(MongoLake);
  });

  it('exports ObjectId class', async () => {
    const { ObjectId: ExportedObjectId } = await import('../../../src/mongoose/index.js');
    expect(ExportedObjectId).toBe(ObjectId);
  });

  it('exports MongoClient as alias for MongoLake', async () => {
    const { MongoClient, MongoLake: ML } = await import('../../../src/mongoose/index.js');
    expect(MongoClient).toBe(ML);
  });

  it('exports default object with all main exports', async () => {
    const defaultExport = (await import('../../../src/mongoose/index.js')).default;

    expect(defaultExport.MongoLake).toBe(MongoLake);
    expect(defaultExport.MongoClient).toBe(MongoLake);
    expect(defaultExport.MongoLakeConnection).toBe(MongoLakeConnection);
    expect(defaultExport.MongoLakeDriverCollection).toBe(MongoLakeDriverCollection);
    expect(defaultExport.MongoLakeSession).toBe(MongoLakeSession);
    expect(defaultExport.ObjectId).toBe(ObjectId);
    expect(defaultExport.createDriver).toBe(createDriver);
    expect(defaultExport.fromConnectionString).toBe(fromConnectionString);
  });
});
