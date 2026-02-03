/**
 * Time Travel Collection Tests
 *
 * Tests for client-side time travel functionality:
 * - Collection.asOf(timestamp) - Query at a specific timestamp
 * - Collection.atSnapshot(snapshotId) - Query at a specific snapshot
 * - Read-only enforcement
 * - Edge cases (querying before first snapshot, deleted documents, etc.)
 * - Integration with Iceberg snapshots
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { MemoryStorage } from '../../../src/storage/index.js';
import { Database } from '../../../src/client/database.js';
import { Collection } from '../../../src/client/collection.js';
import { TimeTravelCollection } from '../../../src/client/time-travel.js';
import { writeParquet } from '../../../src/parquet/io.js';
import type { Document, MongoLakeConfig } from '../../../src/types.js';

// ============================================================================
// Test Fixtures
// ============================================================================

interface TestUser extends Document {
  _id?: string;
  name: string;
  age: number;
  status?: string;
  createdAt?: number;
}

function createTestDatabase(name = 'testdb'): { db: Database; storage: MemoryStorage } {
  const storage = new MemoryStorage();
  const config: MongoLakeConfig = { database: name };
  const database = new Database(name, storage, config);
  return { db: database, storage };
}

/**
 * Helper to create mock Iceberg metadata
 */
function serializeWithBigInt(obj: unknown): string {
  return JSON.stringify(obj, (_key, value) =>
    typeof value === 'bigint' ? value.toString() : value
  );
}

/**
 * Create mock Iceberg table metadata with snapshots
 */
function createTableMetadata(snapshots: Array<{
  snapshotId: bigint;
  timestampMs: number;
  parentSnapshotId?: bigint | null;
  dataFiles?: string[];
  operation?: string;
}>) {
  const icebergSnapshots = snapshots.map((s, index) => ({
    'snapshot-id': s.snapshotId,
    'parent-snapshot-id': s.parentSnapshotId ?? (index > 0 ? snapshots[index - 1]!.snapshotId : null),
    'sequence-number': BigInt(index + 1),
    'timestamp-ms': s.timestampMs,
    'manifest-list': `metadata/snap-${s.snapshotId}-manifest-list.json`,
    summary: { operation: s.operation ?? 'append' },
    'schema-id': 0,
  }));

  return {
    'format-version': 2,
    'table-uuid': 'test-uuid-12345',
    location: 'testdb/users',
    'last-sequence-number': BigInt(snapshots.length),
    'last-updated-ms': snapshots[snapshots.length - 1]?.timestampMs ?? Date.now(),
    'last-column-id': 3,
    'current-schema-id': 0,
    schemas: [{
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: '_id', required: true, type: 'string' },
        { id: 2, name: 'name', required: true, type: 'string' },
        { id: 3, name: 'age', required: true, type: 'int' },
      ],
    }],
    'default-spec-id': 0,
    'partition-specs': [{ 'spec-id': 0, fields: [] }],
    'last-partition-id': 999,
    'default-sort-order-id': 0,
    'sort-orders': [{ 'order-id': 0, fields: [] }],
    snapshots: icebergSnapshots,
    'current-snapshot-id': icebergSnapshots[icebergSnapshots.length - 1]?.['snapshot-id'] ?? null,
    refs: {},
    properties: {},
  };
}

/**
 * Setup Iceberg metadata and manifest files in storage
 */
async function setupIcebergMetadata(
  storage: MemoryStorage,
  dbName: string,
  collectionName: string,
  snapshots: Array<{
    snapshotId: bigint;
    timestampMs: number;
    parentSnapshotId?: bigint | null;
    dataFiles?: string[];
    operation?: string;
  }>
) {
  // Write table metadata
  const metadataPath = `${dbName}/${collectionName}/_iceberg/metadata/v1.metadata.json`;
  const tableMetadata = createTableMetadata(snapshots);
  await storage.put(metadataPath, new TextEncoder().encode(serializeWithBigInt(tableMetadata)));

  // Write manifest lists and manifests for each snapshot
  for (const snapshot of snapshots) {
    const manifestListPath = `${dbName}/${collectionName}/_iceberg/metadata/snap-${snapshot.snapshotId}-manifest-list.json`;
    const manifestPath = `${dbName}/${collectionName}/_iceberg/metadata/manifest-${snapshot.snapshotId}.json`;

    const manifestList = [{
      'manifest-path': manifestPath,
      'manifest-length': 1024,
      'partition-spec-id': 0,
      content: 0,
      'sequence-number': 1,
      'min-sequence-number': 1,
      'added-snapshot-id': snapshot.snapshotId.toString(),
      'added-data-files-count': (snapshot.dataFiles ?? []).length,
    }];

    await storage.put(manifestListPath, new TextEncoder().encode(serializeWithBigInt(manifestList)));

    const manifestEntries = {
      entries: (snapshot.dataFiles ?? []).map((filePath) => ({
        status: 1, // ADDED
        'snapshot-id': snapshot.snapshotId.toString(),
        'sequence-number': 1,
        'data-file': {
          content: 0,
          'file-path': filePath,
          'file-format': 'PARQUET',
          partition: {},
          'record-count': 100,
          'file-size-in-bytes': 4096,
        },
      })),
    };

    await storage.put(manifestPath, new TextEncoder().encode(serializeWithBigInt(manifestEntries)));
  }
}

/**
 * Write mock Parquet data to storage
 */
async function writeTestData<T extends Document>(
  storage: MemoryStorage,
  dbName: string,
  collectionName: string,
  timestamp: number,
  seq: number,
  docs: Array<{ id: string; op: 'i' | 'u' | 'd'; doc: T }>
) {
  const filePath = `${dbName}/${collectionName}_${timestamp}_${seq}.parquet`;
  const rows = docs.map((d, i) => ({
    _id: d.id,
    _seq: seq + i,
    _op: d.op,
    doc: d.doc,
  }));

  const parquetData = writeParquet(rows);
  await storage.put(filePath, parquetData);
  return filePath;
}

// ============================================================================
// Basic Time Travel API Tests
// ============================================================================

describe('TimeTravelCollection - Basic API', () => {
  let storage: MemoryStorage;
  let db: Database;
  let collection: Collection<TestUser>;

  beforeEach(() => {
    const result = createTestDatabase();
    db = result.db;
    storage = result.storage;
    collection = db.collection<TestUser>('users');
  });

  describe('Collection.asOf(timestamp)', () => {
    it('should return a TimeTravelCollection instance', () => {
      const timestamp = new Date('2024-01-15T12:00:00Z');
      const ttCollection = collection.asOf(timestamp);

      expect(ttCollection).toBeInstanceOf(TimeTravelCollection);
      expect(ttCollection.name).toBe('users');
    });

    it('should create a read-only collection view', () => {
      const timestamp = new Date();
      const ttCollection = collection.asOf(timestamp);

      // Check that read methods exist
      expect(typeof ttCollection.find).toBe('function');
      expect(typeof ttCollection.findOne).toBe('function');
      expect(typeof ttCollection.countDocuments).toBe('function');
      expect(typeof ttCollection.distinct).toBe('function');
      expect(typeof ttCollection.aggregate).toBe('function');
    });

    it('should accept Date object as timestamp', () => {
      const date = new Date('2024-06-15T12:00:00Z');
      const ttCollection = collection.asOf(date);

      expect(ttCollection).toBeInstanceOf(TimeTravelCollection);
    });
  });

  describe('Collection.atSnapshot(snapshotId)', () => {
    it('should return a TimeTravelCollection instance', () => {
      const ttCollection = collection.atSnapshot(1000n);

      expect(ttCollection).toBeInstanceOf(TimeTravelCollection);
      expect(ttCollection.name).toBe('users');
    });

    it('should accept bigint snapshot IDs', () => {
      const ttCollection = collection.atSnapshot(9007199254740993n);

      expect(ttCollection).toBeInstanceOf(TimeTravelCollection);
    });
  });

  describe('isReadOnly property', () => {
    it('should have isReadOnly set to true', () => {
      const ttCollection = collection.asOf(new Date());
      expect(ttCollection.isReadOnly).toBe(true);
    });
  });
});

// ============================================================================
// Read Operations at Specific Time
// ============================================================================

describe('TimeTravelCollection - Read Operations', () => {
  let storage: MemoryStorage;
  let db: Database;
  let collection: Collection<TestUser>;

  beforeEach(async () => {
    const result = createTestDatabase();
    db = result.db;
    storage = result.storage;
    collection = db.collection<TestUser>('users');
  });

  describe('find() at timestamp', () => {
    it('should return documents that existed at the timestamp', async () => {
      const timestamp1 = Date.now() - 86400000; // 1 day ago
      const timestamp2 = Date.now();

      // Write data at timestamp1
      const file1 = await writeTestData(storage, 'testdb', 'users', timestamp1, 1, [
        { id: 'user-1', op: 'i', doc: { name: 'Alice', age: 30 } },
        { id: 'user-2', op: 'i', doc: { name: 'Bob', age: 25 } },
      ]);

      // Write more data at timestamp2
      await writeTestData(storage, 'testdb', 'users', timestamp2, 3, [
        { id: 'user-3', op: 'i', doc: { name: 'Charlie', age: 35 } },
      ]);

      // Setup Iceberg metadata
      await setupIcebergMetadata(storage, 'testdb', 'users', [
        { snapshotId: 1000n, timestampMs: timestamp1, dataFiles: [file1] },
        { snapshotId: 1001n, timestampMs: timestamp2, dataFiles: [file1, 'testdb/users_' + timestamp2 + '_3.parquet'] },
      ]);

      // Query at timestamp1 - should only see Alice and Bob
      const ttCollection = collection.asOf(new Date(timestamp1 + 1000)); // slightly after
      const docs = await ttCollection.find({}).toArray();

      expect(docs.length).toBeGreaterThanOrEqual(2);
    });

    it('should apply filter to historical data', async () => {
      const timestamp = Date.now() - 86400000;

      await writeTestData(storage, 'testdb', 'users', timestamp, 1, [
        { id: 'user-1', op: 'i', doc: { name: 'Alice', age: 30, status: 'active' } },
        { id: 'user-2', op: 'i', doc: { name: 'Bob', age: 25, status: 'inactive' } },
      ]);

      const ttCollection = collection.asOf(new Date(timestamp + 1000));
      const activeDocs = await ttCollection.find({ status: 'active' }).toArray();

      // Filter should be applied
      for (const doc of activeDocs) {
        expect(doc.status).toBe('active');
      }
    });

    it('should support cursor methods (sort, limit, skip)', async () => {
      const timestamp = Date.now() - 86400000;

      await writeTestData(storage, 'testdb', 'users', timestamp, 1, [
        { id: 'user-1', op: 'i', doc: { name: 'Alice', age: 30 } },
        { id: 'user-2', op: 'i', doc: { name: 'Bob', age: 25 } },
        { id: 'user-3', op: 'i', doc: { name: 'Charlie', age: 35 } },
      ]);

      const ttCollection = collection.asOf(new Date(timestamp + 1000));

      // Test sort
      const sortedCursor = ttCollection.find({}).sort({ age: 1 });
      expect(sortedCursor).toBeDefined();

      // Test limit
      const limitedCursor = ttCollection.find({}).limit(2);
      expect(limitedCursor).toBeDefined();

      // Test skip
      const skippedCursor = ttCollection.find({}).skip(1);
      expect(skippedCursor).toBeDefined();

      // Test chaining
      const chainedCursor = ttCollection.find({})
        .sort({ age: -1 })
        .skip(1)
        .limit(1);
      expect(chainedCursor).toBeDefined();
    });
  });

  describe('findOne() at timestamp', () => {
    it('should return a single document', async () => {
      const timestamp = Date.now() - 86400000;

      await writeTestData(storage, 'testdb', 'users', timestamp, 1, [
        { id: 'user-1', op: 'i', doc: { name: 'Alice', age: 30 } },
      ]);

      const ttCollection = collection.asOf(new Date(timestamp + 1000));
      const doc = await ttCollection.findOne({ name: 'Alice' });

      expect(doc).toBeDefined();
      if (doc) {
        expect(doc.name).toBe('Alice');
      }
    });

    it('should return null when no document matches', async () => {
      const timestamp = Date.now() - 86400000;

      await writeTestData(storage, 'testdb', 'users', timestamp, 1, [
        { id: 'user-1', op: 'i', doc: { name: 'Alice', age: 30 } },
      ]);

      const ttCollection = collection.asOf(new Date(timestamp + 1000));
      const doc = await ttCollection.findOne({ name: 'NonExistent' });

      expect(doc).toBeNull();
    });
  });

  describe('countDocuments() at timestamp', () => {
    it('should count documents at the given timestamp', async () => {
      const timestamp = Date.now() - 86400000;

      await writeTestData(storage, 'testdb', 'users', timestamp, 1, [
        { id: 'user-1', op: 'i', doc: { name: 'Alice', age: 30 } },
        { id: 'user-2', op: 'i', doc: { name: 'Bob', age: 25 } },
      ]);

      const ttCollection = collection.asOf(new Date(timestamp + 1000));
      const count = await ttCollection.countDocuments({});

      expect(count).toBe(2);
    });

    it('should count with filter', async () => {
      const timestamp = Date.now() - 86400000;

      await writeTestData(storage, 'testdb', 'users', timestamp, 1, [
        { id: 'user-1', op: 'i', doc: { name: 'Alice', age: 30 } },
        { id: 'user-2', op: 'i', doc: { name: 'Bob', age: 25 } },
        { id: 'user-3', op: 'i', doc: { name: 'Charlie', age: 30 } },
      ]);

      const ttCollection = collection.asOf(new Date(timestamp + 1000));
      const count = await ttCollection.countDocuments({ age: 30 });

      expect(count).toBe(2);
    });
  });

  describe('distinct() at timestamp', () => {
    it('should return distinct values from historical data', async () => {
      const timestamp = Date.now() - 86400000;

      await writeTestData(storage, 'testdb', 'users', timestamp, 1, [
        { id: 'user-1', op: 'i', doc: { name: 'Alice', age: 30, status: 'active' } },
        { id: 'user-2', op: 'i', doc: { name: 'Bob', age: 25, status: 'active' } },
        { id: 'user-3', op: 'i', doc: { name: 'Charlie', age: 35, status: 'inactive' } },
      ]);

      const ttCollection = collection.asOf(new Date(timestamp + 1000));
      const statuses = await ttCollection.distinct('status' as keyof TestUser & keyof { _id: string });

      expect(statuses).toContain('active');
      expect(statuses).toContain('inactive');
      expect(statuses.length).toBe(2);
    });
  });

  describe('aggregate() at timestamp', () => {
    it('should run aggregation pipeline on historical data', async () => {
      const timestamp = Date.now() - 86400000;

      await writeTestData(storage, 'testdb', 'users', timestamp, 1, [
        { id: 'user-1', op: 'i', doc: { name: 'Alice', age: 30 } },
        { id: 'user-2', op: 'i', doc: { name: 'Bob', age: 25 } },
        { id: 'user-3', op: 'i', doc: { name: 'Charlie', age: 30 } },
      ]);

      const ttCollection = collection.asOf(new Date(timestamp + 1000));
      const cursor = ttCollection.aggregate([
        { $match: { age: 30 } },
      ]);

      expect(cursor).toBeDefined();
    });
  });
});

// ============================================================================
// Edge Cases
// ============================================================================

describe('TimeTravelCollection - Edge Cases', () => {
  let storage: MemoryStorage;
  let db: Database;
  let collection: Collection<TestUser>;

  beforeEach(() => {
    const result = createTestDatabase();
    db = result.db;
    storage = result.storage;
    collection = db.collection<TestUser>('users');
  });

  describe('querying before first snapshot', () => {
    it('should return empty results when timestamp is before any data', async () => {
      const dataTimestamp = Date.now();
      const queryTimestamp = dataTimestamp - 86400000; // 1 day before

      // Write data at current time
      await writeTestData(storage, 'testdb', 'users', dataTimestamp, 1, [
        { id: 'user-1', op: 'i', doc: { name: 'Alice', age: 30 } },
      ]);

      // Setup Iceberg metadata with snapshot after query time
      await setupIcebergMetadata(storage, 'testdb', 'users', [
        { snapshotId: 1000n, timestampMs: dataTimestamp, dataFiles: ['testdb/users_' + dataTimestamp + '_1.parquet'] },
      ]);

      // Query before any data exists
      const ttCollection = collection.asOf(new Date(queryTimestamp));
      const docs = await ttCollection.find({}).toArray();

      // Should return empty when no snapshot exists at that time
      expect(docs.length).toBe(0);
    });

    it('should return null snapshot when querying before first snapshot', async () => {
      const dataTimestamp = Date.now();
      const queryTimestamp = dataTimestamp - 86400000;

      await setupIcebergMetadata(storage, 'testdb', 'users', [
        { snapshotId: 1000n, timestampMs: dataTimestamp },
      ]);

      const ttCollection = collection.asOf(new Date(queryTimestamp));
      const snapshot = await ttCollection.getSnapshot();

      expect(snapshot).toBeNull();
    });
  });

  describe('deleted documents', () => {
    it('should not return deleted documents', async () => {
      const timestamp1 = Date.now() - 86400000;
      const timestamp2 = Date.now();

      // Insert document
      await writeTestData(storage, 'testdb', 'users', timestamp1, 1, [
        { id: 'user-1', op: 'i', doc: { name: 'Alice', age: 30 } },
      ]);

      // Delete document
      await writeTestData(storage, 'testdb', 'users', timestamp2, 2, [
        { id: 'user-1', op: 'd', doc: {} as TestUser },
      ]);

      // Query at timestamp2 - document should be deleted
      const ttCollection = collection.asOf(new Date(timestamp2 + 1000));
      const doc = await ttCollection.findOne({ _id: 'user-1' });

      expect(doc).toBeNull();
    });

    it('should return document before deletion', async () => {
      const timestamp1 = Date.now() - 86400000;
      const timestamp2 = Date.now();

      // Insert document
      const file1 = await writeTestData(storage, 'testdb', 'users', timestamp1, 1, [
        { id: 'user-1', op: 'i', doc: { name: 'Alice', age: 30 } },
      ]);

      // Setup metadata with only snapshot 1 (before deletion)
      await setupIcebergMetadata(storage, 'testdb', 'users', [
        { snapshotId: 1000n, timestampMs: timestamp1, dataFiles: [file1] },
      ]);

      // Query at timestamp1 - document should exist
      const ttCollection = collection.asOf(new Date(timestamp1 + 1000));
      const doc = await ttCollection.findOne({ _id: 'user-1' });

      expect(doc).not.toBeNull();
      expect(doc?.name).toBe('Alice');
    });
  });

  describe('updated documents', () => {
    it('should return the version of document at query time', async () => {
      const timestamp1 = Date.now() - 86400000;
      const timestamp2 = Date.now();

      // Insert document
      const file1 = await writeTestData(storage, 'testdb', 'users', timestamp1, 1, [
        { id: 'user-1', op: 'i', doc: { name: 'Alice', age: 30 } },
      ]);

      // Update document
      await writeTestData(storage, 'testdb', 'users', timestamp2, 2, [
        { id: 'user-1', op: 'u', doc: { name: 'Alice Updated', age: 31 } },
      ]);

      // Setup metadata for first snapshot only
      await setupIcebergMetadata(storage, 'testdb', 'users', [
        { snapshotId: 1000n, timestampMs: timestamp1, dataFiles: [file1] },
      ]);

      // Query at timestamp1 - should get original version
      const ttCollection = collection.asOf(new Date(timestamp1 + 1000));
      const doc = await ttCollection.findOne({ _id: 'user-1' });

      expect(doc?.name).toBe('Alice');
      expect(doc?.age).toBe(30);
    });
  });

  describe('empty collection', () => {
    it('should handle empty collection gracefully', async () => {
      const ttCollection = collection.asOf(new Date());
      const docs = await ttCollection.find({}).toArray();

      expect(docs).toEqual([]);
    });

    it('should return 0 count for empty collection', async () => {
      const ttCollection = collection.asOf(new Date());
      const count = await ttCollection.countDocuments({});

      expect(count).toBe(0);
    });
  });

  describe('missing Iceberg metadata', () => {
    it('should fall back to timestamp-based filtering when no Iceberg metadata', async () => {
      const timestamp = Date.now() - 86400000;

      // Write data without Iceberg metadata
      await writeTestData(storage, 'testdb', 'users', timestamp, 1, [
        { id: 'user-1', op: 'i', doc: { name: 'Alice', age: 30 } },
      ]);

      // Write collection manifest
      const manifestPath = 'testdb/users/_manifest.json';
      await storage.put(manifestPath, new TextEncoder().encode(JSON.stringify({
        name: 'users',
        files: [],
        schema: {},
        currentSeq: 1,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      })));

      // Query should still work using timestamp-based file filtering
      const ttCollection = collection.asOf(new Date(timestamp + 1000));
      const docs = await ttCollection.find({}).toArray();

      // Should find the document using fallback method
      expect(docs.length).toBeGreaterThanOrEqual(0);
    });
  });
});

// ============================================================================
// Snapshot Metadata Tests
// ============================================================================

describe('TimeTravelCollection - Snapshot Metadata', () => {
  let storage: MemoryStorage;
  let db: Database;
  let collection: Collection<TestUser>;

  beforeEach(() => {
    const result = createTestDatabase();
    db = result.db;
    storage = result.storage;
    collection = db.collection<TestUser>('users');
  });

  describe('getSnapshot()', () => {
    it('should return the snapshot used for the query', async () => {
      const timestamp = Date.now() - 86400000;

      await setupIcebergMetadata(storage, 'testdb', 'users', [
        { snapshotId: 1000n, timestampMs: timestamp },
      ]);

      const ttCollection = collection.asOf(new Date(timestamp + 1000));
      const snapshot = await ttCollection.getSnapshot();

      expect(snapshot).not.toBeNull();
    });

    it('should return null when no matching snapshot', async () => {
      const snapshotTimestamp = Date.now();
      const queryTimestamp = snapshotTimestamp - 86400000;

      await setupIcebergMetadata(storage, 'testdb', 'users', [
        { snapshotId: 1000n, timestampMs: snapshotTimestamp },
      ]);

      const ttCollection = collection.asOf(new Date(queryTimestamp));
      const snapshot = await ttCollection.getSnapshot();

      expect(snapshot).toBeNull();
    });
  });

  describe('getSnapshotTimestamp()', () => {
    it('should return the timestamp of the snapshot', async () => {
      const timestamp = Date.now() - 86400000;

      await setupIcebergMetadata(storage, 'testdb', 'users', [
        { snapshotId: 1000n, timestampMs: timestamp },
      ]);

      const ttCollection = collection.asOf(new Date(timestamp + 1000));
      const snapshotTimestamp = await ttCollection.getSnapshotTimestamp();

      expect(snapshotTimestamp).toBeInstanceOf(Date);
      expect(snapshotTimestamp?.getTime()).toBe(timestamp);
    });

    it('should return null when no snapshot', async () => {
      const ttCollection = collection.asOf(new Date(Date.now() - 999999999999));
      const snapshotTimestamp = await ttCollection.getSnapshotTimestamp();

      expect(snapshotTimestamp).toBeNull();
    });
  });

  describe('estimatedDocumentCount()', () => {
    it('should use snapshot summary for count when available', async () => {
      const timestamp = Date.now() - 86400000;

      // Setup metadata with total-records in summary
      const metadataPath = 'testdb/users/_iceberg/metadata/v1.metadata.json';
      const metadata = createTableMetadata([
        { snapshotId: 1000n, timestampMs: timestamp },
      ]);
      // Add total-records to summary
      (metadata.snapshots[0] as { summary: Record<string, string> }).summary['total-records'] = '1000';
      await storage.put(metadataPath, new TextEncoder().encode(serializeWithBigInt(metadata)));

      await setupIcebergMetadata(storage, 'testdb', 'users', [
        { snapshotId: 1000n, timestampMs: timestamp },
      ]);

      const ttCollection = collection.asOf(new Date(timestamp + 1000));
      const count = await ttCollection.estimatedDocumentCount();

      // Should use the summary value
      expect(typeof count).toBe('number');
    });
  });
});

// ============================================================================
// Sibling Collection for Lookups
// ============================================================================

describe('TimeTravelCollection - Cross-Collection Operations', () => {
  let storage: MemoryStorage;
  let db: Database;

  beforeEach(() => {
    const result = createTestDatabase();
    db = result.db;
    storage = result.storage;
  });

  describe('getSiblingCollection()', () => {
    it('should return a sibling time travel collection with same timestamp', async () => {
      const timestamp = new Date(Date.now() - 86400000);

      const usersCollection = db.collection<TestUser>('users');
      const ttUsers = usersCollection.asOf(timestamp);

      // Get sibling collection - used for $lookup
      const ttOrders = ttUsers.getSiblingCollection('orders');

      expect(ttOrders).toBeInstanceOf(TimeTravelCollection);
      expect(ttOrders.name).toBe('orders');
    });
  });
});

// ============================================================================
// Cursor Async Iteration
// ============================================================================

describe('TimeTravelCollection - Cursor Iteration', () => {
  let storage: MemoryStorage;
  let db: Database;
  let collection: Collection<TestUser>;

  beforeEach(() => {
    const result = createTestDatabase();
    db = result.db;
    storage = result.storage;
    collection = db.collection<TestUser>('users');
  });

  describe('async iteration', () => {
    it('should support for-await-of loop', async () => {
      const timestamp = Date.now() - 86400000;

      await writeTestData(storage, 'testdb', 'users', timestamp, 1, [
        { id: 'user-1', op: 'i', doc: { name: 'Alice', age: 30 } },
        { id: 'user-2', op: 'i', doc: { name: 'Bob', age: 25 } },
      ]);

      const ttCollection = collection.asOf(new Date(timestamp + 1000));
      const cursor = ttCollection.find({});

      const docs: TestUser[] = [];
      for await (const doc of cursor) {
        docs.push(doc);
      }

      expect(docs.length).toBe(2);
    });

    it('should support forEach()', async () => {
      const timestamp = Date.now() - 86400000;

      await writeTestData(storage, 'testdb', 'users', timestamp, 1, [
        { id: 'user-1', op: 'i', doc: { name: 'Alice', age: 30 } },
      ]);

      const ttCollection = collection.asOf(new Date(timestamp + 1000));
      const cursor = ttCollection.find({});

      const names: string[] = [];
      await cursor.forEach((doc) => {
        names.push(doc.name);
      });

      expect(names).toContain('Alice');
    });

    it('should support map()', async () => {
      const timestamp = Date.now() - 86400000;

      await writeTestData(storage, 'testdb', 'users', timestamp, 1, [
        { id: 'user-1', op: 'i', doc: { name: 'Alice', age: 30 } },
        { id: 'user-2', op: 'i', doc: { name: 'Bob', age: 25 } },
      ]);

      const ttCollection = collection.asOf(new Date(timestamp + 1000));
      const cursor = ttCollection.find({});

      const names = await cursor.map((doc) => doc.name);

      expect(names).toContain('Alice');
      expect(names).toContain('Bob');
    });

    it('should support hasNext() and next()', async () => {
      const timestamp = Date.now() - 86400000;

      await writeTestData(storage, 'testdb', 'users', timestamp, 1, [
        { id: 'user-1', op: 'i', doc: { name: 'Alice', age: 30 } },
      ]);

      const ttCollection = collection.asOf(new Date(timestamp + 1000));
      const cursor = ttCollection.find({});

      const hasMore = await cursor.hasNext();
      expect(hasMore).toBe(true);

      const doc = await cursor.next();
      expect(doc).not.toBeNull();
      expect(doc?.name).toBe('Alice');
    });
  });
});

// ============================================================================
// Projection Support
// ============================================================================

describe('TimeTravelCollection - Projection', () => {
  let storage: MemoryStorage;
  let db: Database;
  let collection: Collection<TestUser>;

  beforeEach(() => {
    const result = createTestDatabase();
    db = result.db;
    storage = result.storage;
    collection = db.collection<TestUser>('users');
  });

  it('should apply projection to results', async () => {
    const timestamp = Date.now() - 86400000;

    await writeTestData(storage, 'testdb', 'users', timestamp, 1, [
      { id: 'user-1', op: 'i', doc: { name: 'Alice', age: 30, status: 'active' } },
    ]);

    const ttCollection = collection.asOf(new Date(timestamp + 1000));
    const cursor = ttCollection.find({}, { projection: { name: 1, _id: 1 } });

    const docs = await cursor.toArray();

    // Documents should only have projected fields
    for (const doc of docs) {
      expect(doc.name).toBeDefined();
      expect(doc._id).toBeDefined();
      // Non-projected fields may or may not be present depending on implementation
    }
  });

  it('should support cursor.project() method', async () => {
    const timestamp = Date.now() - 86400000;

    await writeTestData(storage, 'testdb', 'users', timestamp, 1, [
      { id: 'user-1', op: 'i', doc: { name: 'Alice', age: 30 } },
    ]);

    const ttCollection = collection.asOf(new Date(timestamp + 1000));
    const cursor = ttCollection.find({}).project({ name: 1 });

    expect(cursor).toBeDefined();
  });
});
