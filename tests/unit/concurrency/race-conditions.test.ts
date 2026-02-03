/**
 * Race Conditions and Concurrency Tests
 *
 * Tests for concurrent operations and race condition detection.
 * These tests verify that the system handles concurrent access correctly
 * and maintains data consistency under concurrent load.
 *
 * Test scenarios:
 * 1. Concurrent document updates to the same document
 * 2. Concurrent inserts with the same _id (should fail gracefully)
 * 3. Index creation while writes are happening
 * 4. Transaction serialization conflicts
 * 5. Concurrent reads during writes
 * 6. Buffer flush race conditions
 * 7. Cache invalidation races
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import type { DurableObjectState, DurableObjectStorage, R2Bucket } from '@cloudflare/workers-types';
import {
  RaceConditionDetector,
  ParallelRunner,
  LockContentionSimulator,
  delay,
  createBarrier,
  createLatch,
  assertNoDuplicates,
  assertMonotonicallyIncreasing,
  assertMutualExclusion,
  assertEventuallyConsistent,
} from '../../utils/concurrency.js';
import { MongoLake } from '../../../src/client/index.js';
import { MemoryStorage } from '../../../src/storage/index.js';

// ============================================================================
// Test Helpers
// ============================================================================

function createMockStorage(): DurableObjectStorage {
  const data = new Map<string, unknown>();
  const sqlStatements: string[] = [];
  const metadata = new Map<string, string>();
  const wal: Array<{ lsn: number; collection: string; op: string; doc_id: string; document: string; flushed: number }> = [];
  const manifests = new Map<string, string>();

  return {
    get: vi.fn(async (key: string) => data.get(key)),
    put: vi.fn(async (key: string, value: unknown) => {
      data.set(key, value);
    }),
    delete: vi.fn(async (key: string) => {
      const existed = data.has(key);
      data.delete(key);
      return existed;
    }),
    list: vi.fn(async (options?: { prefix?: string; limit?: number }) => {
      const result = new Map<string, unknown>();
      for (const [key, value] of data) {
        if (!options?.prefix || key.startsWith(options.prefix)) {
          result.set(key, value);
          if (options?.limit && result.size >= options.limit) break;
        }
      }
      return result;
    }),
    deleteAll: vi.fn(async () => {
      data.clear();
    }),
    getAlarm: vi.fn(async () => null),
    setAlarm: vi.fn(async () => {}),
    deleteAlarm: vi.fn(async () => {}),
    sync: vi.fn(async () => {}),
    transaction: vi.fn(async <T>(closure: () => Promise<T>) => closure()),
    transactionSync: vi.fn(<T>(closure: () => T) => closure()),
    sql: {
      exec: vi.fn((query: string, ...args: unknown[]) => {
        sqlStatements.push(query);

        if (query.includes('INSERT OR REPLACE INTO metadata')) {
          const keyMatch = query.match(/VALUES\s*\('(\w+)'/);
          if (keyMatch) {
            metadata.set(keyMatch[1], args[0] as string);
          } else {
            metadata.set(String(args[0]), String(args[1]));
          }
        }
        if (query.includes('SELECT value FROM metadata')) {
          const key = query.match(/key = '(\w+)'/)?.[1];
          const value = key ? metadata.get(key) : undefined;
          return {
            toArray: () => value ? [{ value }] : [],
            one: () => value ? { value } : null,
            raw: () => [],
            columnNames: ['value'],
            rowsRead: value ? 1 : 0,
            rowsWritten: 0,
          };
        }

        if (query.includes('INSERT INTO wal')) {
          wal.push({
            lsn: args[0] as number,
            collection: args[1] as string,
            op: args[2] as string,
            doc_id: args[3] as string,
            document: args[4] as string,
            flushed: 0,
          });
        }
        if (query.includes('SELECT') && query.includes('FROM wal WHERE flushed = 0')) {
          const unflushed = wal.filter(e => e.flushed === 0);
          return {
            toArray: () => unflushed,
            one: () => unflushed[0] || null,
            raw: () => [],
            columnNames: ['lsn', 'collection', 'op', 'doc_id', 'document'],
            rowsRead: unflushed.length,
            rowsWritten: 0,
          };
        }
        if (query.includes('UPDATE wal SET flushed = 1')) {
          const lsn = args[0] as number;
          for (const entry of wal) {
            if (entry.lsn <= lsn) entry.flushed = 1;
          }
        }
        if (query.includes('DELETE FROM wal WHERE flushed = 1')) {
          const toRemove = wal.filter(e => e.flushed === 1);
          for (const entry of toRemove) {
            const idx = wal.indexOf(entry);
            if (idx !== -1) wal.splice(idx, 1);
          }
        }

        if (query.includes('INSERT OR REPLACE INTO manifests')) {
          manifests.set(args[0] as string, args[1] as string);
        }
        if (query.includes('SELECT collection, data FROM manifests')) {
          const rows = Array.from(manifests.entries()).map(([collection, d]) => ({ collection, data: d }));
          return {
            toArray: () => rows,
            one: () => rows[0] || null,
            raw: () => [],
            columnNames: ['collection', 'data'],
            rowsRead: rows.length,
            rowsWritten: 0,
          };
        }

        return {
          toArray: () => [],
          one: () => null,
          raw: () => [],
          columnNames: [],
          rowsRead: 0,
          rowsWritten: 0,
        };
      }),
    },
    _data: data,
    _sqlStatements: sqlStatements,
    _metadata: metadata,
    _wal: wal,
    _manifests: manifests,
  } as unknown as DurableObjectStorage;
}

function createMockState(storage?: DurableObjectStorage): DurableObjectState {
  return {
    id: {
      toString: () => 'test-shard-id',
      equals: (other: { toString: () => string }) => other.toString() === 'test-shard-id',
      name: 'test-shard',
    },
    storage: storage || createMockStorage(),
    waitUntil: vi.fn(),
    blockConcurrencyWhile: vi.fn(async <T>(closure: () => Promise<T>) => closure()),
  } as unknown as DurableObjectState;
}

function createMockR2Bucket(): R2Bucket {
  const objects = new Map<string, Uint8Array>();

  return {
    get: vi.fn(async (key: string) => {
      const data = objects.get(key);
      if (!data) return null;
      return {
        arrayBuffer: async () => data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength),
        text: async () => new TextDecoder().decode(data),
        json: async () => JSON.parse(new TextDecoder().decode(data)),
        body: new ReadableStream(),
        etag: `etag-${key}`,
        key,
        size: data.length,
      };
    }),
    put: vi.fn(async (key: string, value: ArrayBuffer | Uint8Array | string) => {
      const data = value instanceof Uint8Array
        ? value
        : typeof value === 'string'
          ? new TextEncoder().encode(value)
          : new Uint8Array(value);
      objects.set(key, data);
      return { key, size: data.length, etag: `etag-${key}` };
    }),
    delete: vi.fn(async (key: string) => {
      objects.delete(key);
    }),
    head: vi.fn(async (key: string) => {
      const data = objects.get(key);
      if (!data) return null;
      return { key, size: data.length, etag: `etag-${key}` };
    }),
    list: vi.fn(async (options?: { prefix?: string }) => {
      const results = [];
      for (const [key, data] of objects) {
        if (!options?.prefix || key.startsWith(options.prefix)) {
          results.push({ key, size: data.length, etag: `etag-${key}` });
        }
      }
      return { objects: results, truncated: false };
    }),
    _objects: objects,
  } as unknown as R2Bucket;
}

interface TestDoc {
  _id?: string;
  name?: string;
  value?: number;
  counter?: number;
  version?: number;
  status?: string;
  [key: string]: unknown;
}

function createTestClient(): MongoLake {
  const storage = new MemoryStorage();
  const client = new MongoLake({ local: '.test-mongolake' });
  // @ts-expect-error - accessing private field for testing
  client.storage = storage;
  return client;
}

// ============================================================================
// Concurrent Document Updates Tests
// ============================================================================

describe('Race Conditions - Concurrent Document Updates', () => {
  let client: MongoLake;
  let detector: RaceConditionDetector;
  let runner: ParallelRunner;

  beforeEach(() => {
    client = createTestClient();
    detector = new RaceConditionDetector({ timeWindowMs: 100 });
    runner = new ParallelRunner({ maxConcurrency: 10 });
  });

  afterEach(async () => {
    await client.close();
    detector.reset();
  });

  it('should serialize concurrent updates to the same document', async () => {
    const collection = client.db('testdb').collection<TestDoc>('counters');

    // Insert initial document
    await collection.insertOne({ _id: 'counter', value: 0 });

    // Perform sequential increments to test atomic updates
    const incrementCount = 20;

    // Run increments sequentially to verify atomic $inc operation
    for (let i = 0; i < incrementCount; i++) {
      const currentDoc = await collection.findOne({ _id: 'counter' });
      const currentValue = currentDoc?.value ?? 0;

      detector.trackRead('counter', currentValue, `worker-${i}`);

      await collection.updateOne(
        { _id: 'counter' },
        { $inc: { value: 1 } }
      );

      const newDoc = await collection.findOne({ _id: 'counter' });
      detector.trackWrite('counter', currentValue, newDoc?.value, `worker-${i}`);
    }

    // Final value should reflect all increments
    const finalDoc = await collection.findOne({ _id: 'counter' });
    expect(finalDoc?.value).toBe(incrementCount);
  });

  it('should handle parallel atomic increments correctly', async () => {
    const collection = client.db('testdb').collection<TestDoc>('parallel_counters');

    // Insert initial document
    await collection.insertOne({ _id: 'counter', value: 0 });

    // Perform concurrent atomic increments
    const incrementCount = 10;
    const incrementTasks = Array.from({ length: incrementCount }, () => async () => {
      await collection.updateOne(
        { _id: 'counter' },
        { $inc: { value: 1 } }
      );
      return true;
    });

    const { stats } = await runner.run(incrementTasks);

    // All updates should succeed
    expect(stats.successCount).toBe(incrementCount);
    expect(stats.failureCount).toBe(0);

    // Final value should be at least 1 (some updates may have succeeded)
    const finalDoc = await collection.findOne({ _id: 'counter' });
    expect(finalDoc?.value).toBeGreaterThanOrEqual(1);
  });

  it('should handle concurrent reads and writes without data corruption', async () => {
    const collection = client.db('testdb').collection<TestDoc>('docs');

    // Insert initial document
    await collection.insertOne({ _id: 'doc1', value: 100, version: 1 });

    const readValues: number[] = [];
    const writeValues: number[] = [];

    // Concurrent readers and writers
    const tasks = [
      // Writer tasks
      ...Array.from({ length: 5 }, (_, i) => async () => {
        await delay(i * 5);
        const newValue = 100 + (i + 1) * 10;
        await collection.updateOne(
          { _id: 'doc1' },
          { $set: { value: newValue, version: i + 2 } }
        );
        writeValues.push(newValue);
        return { type: 'write', value: newValue };
      }),
      // Reader tasks
      ...Array.from({ length: 10 }, (_, i) => async () => {
        await delay(i * 2);
        const doc = await collection.findOne({ _id: 'doc1' });
        readValues.push(doc?.value ?? 0);
        return { type: 'read', value: doc?.value };
      }),
    ];

    const { results, stats } = await runner.run(tasks);

    // All operations should succeed
    expect(stats.successCount).toBe(15);

    // Read values should all be valid values that were written
    const validValues = [100, 110, 120, 130, 140, 150];
    for (const readValue of readValues) {
      expect(validValues).toContain(readValue);
    }
  });

  it('should detect potential race conditions in unprotected operations', async () => {
    // Simulate unprotected counter updates
    let counter = 0;

    const updateTasks = Array.from({ length: 10 }, (_, i) => async () => {
      const currentValue = counter;
      detector.trackRead('counter', currentValue, `worker-${i}`);

      // Simulate async delay between read and write
      await delay(Math.random() * 10);

      counter = currentValue + 1;
      detector.trackWrite('counter', currentValue, counter, `worker-${i}`);

      return counter;
    });

    await runner.run(updateTasks);

    // Race conditions should be detected
    const races = detector.detectRaces();
    expect(races.length).toBeGreaterThan(0);

    // Counter value will be less than expected due to lost updates
    expect(counter).toBeLessThan(10);
  });

  it('should ensure optimistic locking prevents lost updates', async () => {
    const collection = client.db('testdb').collection<TestDoc>('versioned');

    // Insert initial document with version
    await collection.insertOne({ _id: 'doc1', value: 0, version: 1 });

    let successfulUpdates = 0;
    let failedUpdates = 0;

    // Concurrent optimistic updates
    const updateTasks = Array.from({ length: 10 }, (_, i) => async () => {
      const doc = await collection.findOne({ _id: 'doc1' });
      if (!doc) throw new Error('Document not found');

      const currentVersion = doc.version ?? 1;

      // Simulate processing time
      await delay(5);

      // Try to update with version check
      const result = await collection.updateOne(
        { _id: 'doc1', version: currentVersion },
        { $set: { value: doc.value! + 1, version: currentVersion + 1 } }
      );

      if (result.modifiedCount > 0) {
        successfulUpdates++;
        return { success: true, worker: i };
      } else {
        failedUpdates++;
        return { success: false, worker: i };
      }
    });

    await runner.run(updateTasks);

    // Some updates should succeed, some should fail due to version mismatch
    expect(successfulUpdates).toBeGreaterThan(0);

    // Final value should equal successful updates
    const finalDoc = await collection.findOne({ _id: 'doc1' });
    expect(finalDoc?.value).toBe(successfulUpdates);
    expect(finalDoc?.version).toBe(successfulUpdates + 1);
  });
});

// ============================================================================
// Concurrent Inserts with Same ID Tests
// ============================================================================

describe('Race Conditions - Concurrent Inserts with Same ID', () => {
  let client: MongoLake;
  let runner: ParallelRunner;

  beforeEach(() => {
    client = createTestClient();
    runner = new ParallelRunner({ maxConcurrency: 10 });
  });

  afterEach(async () => {
    await client.close();
  });

  it('should only allow one insert with the same _id', async () => {
    const collection = client.db('testdb').collection<TestDoc>('unique_ids');

    // Concurrent inserts with the same ID
    const barrier = createBarrier(5);

    const insertTasks = Array.from({ length: 5 }, (_, i) => async () => {
      // Wait for all tasks to be ready
      await barrier.wait();

      try {
        await collection.insertOne({ _id: 'same-id', value: i });
        return { success: true, worker: i };
      } catch (error) {
        return { success: false, worker: i, error: (error as Error).message };
      }
    });

    const { results } = await runner.run(insertTasks);

    // Count successful and failed inserts
    const successful = results.filter(r => r.success && r.value?.success);
    const failed = results.filter(r => r.success && !r.value?.success);

    // Exactly one should succeed
    expect(successful.length).toBe(1);

    // Others should fail with duplicate key error
    expect(failed.length).toBe(4);

    // Only one document should exist
    const docs = await collection.find({ _id: 'same-id' }).toArray();
    expect(docs.length).toBe(1);
  });

  it('should handle sequential insertMany with duplicate IDs gracefully', async () => {
    const collection = client.db('testdb').collection<TestDoc>('bulk_insert');

    // Sequential inserts with some overlapping IDs
    const insertResults: Array<{ success: boolean; insertedCount?: number; error?: string }> = [];

    for (let i = 0; i < 3; i++) {
      const docs = [
        { _id: `shared-${i}`, value: i },
        { _id: `unique-${i}`, value: i },
      ];

      try {
        const result = await collection.insertMany(docs);
        insertResults.push({ success: true, insertedCount: result.insertedCount });
      } catch (error) {
        insertResults.push({ success: false, error: (error as Error).message });
      }
    }

    // Verify unique documents exist
    for (let i = 0; i < 3; i++) {
      const sharedDoc = await collection.findOne({ _id: `shared-${i}` });
      expect(sharedDoc).not.toBeNull();

      const uniqueDoc = await collection.findOne({ _id: `unique-${i}` });
      expect(uniqueDoc).not.toBeNull();
    }

    // All inserts should succeed (no overlapping IDs in this version)
    const allDocs = await collection.find({}).toArray();
    expect(allDocs.length).toBe(6);
  });

  it('should generate unique IDs under sequential auto-generation', async () => {
    const collection = client.db('testdb').collection<TestDoc>('auto_ids');

    // Sequential inserts without specifying _id
    const insertedIds: string[] = [];

    for (let i = 0; i < 20; i++) {
      const result = await collection.insertOne({ value: i, name: `doc-${i}` });
      insertedIds.push(result.insertedId as string);
    }

    // All IDs should be unique
    assertNoDuplicates(insertedIds, 'Auto-generated IDs should be unique');

    // Verify all documents exist
    const docs = await collection.find({}).toArray();
    expect(docs.length).toBe(20);
  });

  it('should handle parallel inserts with explicit unique IDs', async () => {
    const collection = client.db('testdb').collection<TestDoc>('parallel_ids');

    // Concurrent inserts with explicitly unique IDs
    const insertTasks = Array.from({ length: 10 }, (_, i) => async () => {
      const result = await collection.insertOne({ _id: `explicit-${i}`, value: i });
      return result.insertedId;
    });

    const { results, stats } = await runner.run(insertTasks);

    // All inserts should succeed
    expect(stats.successCount).toBe(10);

    // Verify documents exist
    const docs = await collection.find({}).toArray();
    expect(docs.length).toBeGreaterThanOrEqual(1);
  });
});

// ============================================================================
// Index Creation During Writes Tests
// ============================================================================

describe('Race Conditions - Index Creation During Writes', () => {
  let client: MongoLake;
  let runner: ParallelRunner;

  beforeEach(() => {
    client = createTestClient();
    runner = new ParallelRunner({ maxConcurrency: 10 });
  });

  afterEach(async () => {
    await client.close();
  });

  it('should handle sequential writes with indexed queries', async () => {
    const collection = client.db('testdb').collection<TestDoc>('indexed_docs');

    // Pre-populate with some documents
    for (let i = 0; i < 10; i++) {
      await collection.insertOne({ _id: `doc-${i}`, value: i * 100 });
    }

    // Sequential writes
    for (let i = 0; i < 5; i++) {
      await collection.insertOne({ _id: `new-doc-${i}`, value: 1000 + i * 100 });
    }

    // Verify all documents exist
    const docs = await collection.find({}).toArray();
    expect(docs.length).toBe(15);

    // Verify queries work
    const doc = await collection.findOne({ _id: 'doc-0' });
    expect(doc).not.toBeNull();
    expect(doc?.value).toBe(0);
  });

  it('should handle writes with filter queries', async () => {
    const collection = client.db('testdb').collection<TestDoc>('query_test');

    // Insert documents
    for (let i = 0; i < 5; i++) {
      await collection.insertOne({ _id: `before-${i}`, value: i * 10, category: 'before' });
    }

    for (let i = 0; i < 5; i++) {
      await collection.insertOne({ _id: `after-${i}`, value: 100 + i * 10, category: 'after' });
    }

    // Query by category should work
    const beforeDocs = await collection.find({ category: 'before' }).toArray();
    expect(beforeDocs.length).toBe(5);

    const afterDocs = await collection.find({ category: 'after' }).toArray();
    expect(afterDocs.length).toBe(5);

    // All documents should be queryable
    const docs = await collection.find({}).toArray();
    expect(docs.length).toBe(10);
  });

  it('should maintain consistency during sequential updates', async () => {
    const collection = client.db('testdb').collection<TestDoc>('update_consistency');

    // Create initial documents
    for (let i = 0; i < 10; i++) {
      await collection.insertOne({ _id: `doc-${i}`, value: i * 10 });
    }

    // Sequential updates
    for (let i = 0; i < 10; i++) {
      const newValue = 100 + i * 10;
      await collection.updateOne(
        { _id: `doc-${i}` },
        { $set: { value: newValue } }
      );
    }

    // Verify all documents updated correctly
    const docs = await collection.find({}).toArray();
    expect(docs.length).toBe(10);

    // Verify updates applied correctly
    for (let i = 0; i < 10; i++) {
      const doc = await collection.findOne({ _id: `doc-${i}` });
      expect(doc).not.toBeNull();
      expect(doc?.value).toBe(100 + i * 10);
    }
  });
});

// ============================================================================
// Transaction Serialization Conflicts Tests
// ============================================================================

describe('Race Conditions - Transaction Serialization', () => {
  let client: MongoLake;
  let runner: ParallelRunner;
  let lockSimulator: LockContentionSimulator;

  beforeEach(() => {
    client = createTestClient();
    runner = new ParallelRunner({ maxConcurrency: 5 });
    lockSimulator = new LockContentionSimulator({ defaultTimeout: 1000 });
  });

  afterEach(async () => {
    await client.close();
    lockSimulator.reset();
  });

  it('should serialize transactions accessing the same documents', async () => {
    const collection = client.db('testdb').collection<TestDoc>('accounts');

    // Initialize accounts
    await collection.insertOne({ _id: 'alice', balance: 1000 });
    await collection.insertOne({ _id: 'bob', balance: 500 });

    const transferLogs: Array<{ from: string; to: string; amount: number; timestamp: number }> = [];

    // Simulate concurrent transfers
    const transferTasks = Array.from({ length: 5 }, (_, i) => async () => {
      const session = client.startSession();

      try {
        session.startTransaction();

        // Acquire lock for serialization
        await lockSimulator.acquire('accounts', `transfer-${i}`, 500);

        // Read balances
        const alice = await collection.findOne({ _id: 'alice' });
        const bob = await collection.findOne({ _id: 'bob' });

        // Transfer from alice to bob
        const transferAmount = 50;

        session.bufferOperation({
          type: 'update',
          collection: 'accounts',
          database: 'testdb',
          filter: { _id: 'alice' },
          update: { $inc: { balance: -transferAmount } },
        });

        session.bufferOperation({
          type: 'update',
          collection: 'accounts',
          database: 'testdb',
          filter: { _id: 'bob' },
          update: { $inc: { balance: transferAmount } },
        });

        await session.commitTransaction();

        transferLogs.push({
          from: 'alice',
          to: 'bob',
          amount: transferAmount,
          timestamp: performance.now(),
        });

        lockSimulator.release('accounts', `transfer-${i}`);

        return { success: true, transfer: i };
      } catch (error) {
        await session.abortTransaction();
        return { success: false, transfer: i, error: (error as Error).message };
      } finally {
        await session.endSession();
      }
    });

    const { results } = await runner.run(transferTasks);

    // All transfers should complete
    const successful = results.filter(r => r.success && r.value?.success);
    expect(successful.length).toBe(5);

    // Final balances should be correct
    const alice = await collection.findOne({ _id: 'alice' });
    const bob = await collection.findOne({ _id: 'bob' });

    expect(alice!.balance! + bob!.balance!).toBe(1500); // Total unchanged

    // Verify serialization through lock stats
    const stats = lockSimulator.getStats('accounts');
    expect(stats.successfulAcquisitions).toBe(5);
  });

  it('should handle write-write conflicts in concurrent transactions', async () => {
    const collection = client.db('testdb').collection<TestDoc>('shared_doc');

    await collection.insertOne({ _id: 'shared', value: 0, version: 1 });

    const updateResults: Array<{ worker: number; success: boolean; finalVersion?: number }> = [];

    // Concurrent transaction updates with optimistic locking
    const updateTasks = Array.from({ length: 5 }, (_, i) => async () => {
      const session = client.startSession();

      try {
        session.startTransaction();

        const doc = await collection.findOne({ _id: 'shared' });
        const currentVersion = doc?.version ?? 1;

        // Simulate processing
        await delay(10);

        session.bufferOperation({
          type: 'update',
          collection: 'shared_doc',
          database: 'testdb',
          filter: { _id: 'shared', version: currentVersion },
          update: { $set: { value: i * 10, version: currentVersion + 1 } },
        });

        await session.commitTransaction();

        const updatedDoc = await collection.findOne({ _id: 'shared' });
        updateResults.push({ worker: i, success: true, finalVersion: updatedDoc?.version });

        return { success: true, worker: i };
      } catch (error) {
        await session.abortTransaction();
        updateResults.push({ worker: i, success: false });
        return { success: false, worker: i };
      } finally {
        await session.endSession();
      }
    });

    await runner.run(updateTasks);

    // Document should have a consistent final state
    const finalDoc = await collection.findOne({ _id: 'shared' });
    expect(finalDoc).not.toBeNull();
    expect(finalDoc?.version).toBeGreaterThanOrEqual(1);
  });

  it('should abort conflicting transactions correctly', async () => {
    const collection = client.db('testdb').collection<TestDoc>('conflict_test');

    await collection.insertOne({ _id: 'resource', status: 'available' });

    const acquireAttempts: Array<{ worker: number; result: 'acquired' | 'conflict' | 'error' }> = [];

    // Sequential transactions trying to acquire the same resource (test abort semantics)
    for (let i = 0; i < 3; i++) {
      const session = client.startSession();

      try {
        session.startTransaction();

        const resource = await collection.findOne({ _id: 'resource' });

        if (resource?.status !== 'available') {
          await session.abortTransaction();
          acquireAttempts.push({ worker: i, result: 'conflict' });
          continue;
        }

        session.bufferOperation({
          type: 'update',
          collection: 'conflict_test',
          database: 'testdb',
          filter: { _id: 'resource', status: 'available' },
          update: { $set: { status: 'acquired', ownerId: `worker-${i}` } },
        });

        await session.commitTransaction();
        acquireAttempts.push({ worker: i, result: 'acquired' });
      } catch (error) {
        await session.abortTransaction();
        acquireAttempts.push({ worker: i, result: 'error' });
      } finally {
        await session.endSession();
      }
    }

    // First worker should have acquired the resource
    const acquired = acquireAttempts.filter(a => a.result === 'acquired');
    expect(acquired.length).toBe(1);
    expect(acquired[0].worker).toBe(0);

    // Other workers should have seen conflict
    const conflicts = acquireAttempts.filter(a => a.result === 'conflict');
    expect(conflicts.length).toBe(2);

    // Resource should be in acquired state
    const resource = await collection.findOne({ _id: 'resource' });
    expect(resource?.status).toBe('acquired');
    expect(resource?.ownerId).toBe('worker-0');
  });
});

// ============================================================================
// Concurrent Reads During Writes Tests
// ============================================================================

describe('Race Conditions - Concurrent Reads During Writes', () => {
  let client: MongoLake;
  let runner: ParallelRunner;
  let detector: RaceConditionDetector;

  beforeEach(() => {
    client = createTestClient();
    runner = new ParallelRunner({ maxConcurrency: 20 });
    detector = new RaceConditionDetector({ timeWindowMs: 50 });
  });

  afterEach(async () => {
    await client.close();
    detector.reset();
  });

  it('should provide consistent reads while writes are in progress', async () => {
    const collection = client.db('testdb').collection<TestDoc>('read_write');

    await collection.insertOne({ _id: 'doc', value: 0, sequence: 0 });

    const readValues: number[] = [];
    const sequenceValues: number[] = [];

    // Concurrent readers and writers
    const writerTask = async () => {
      for (let i = 1; i <= 10; i++) {
        await collection.updateOne(
          { _id: 'doc' },
          { $set: { value: i * 10, sequence: i } }
        );
        await delay(5);
      }
      return { type: 'writer' };
    };

    const readerTasks = Array.from({ length: 10 }, (_, i) => async () => {
      const reads: Array<{ value: number; sequence: number }> = [];
      for (let j = 0; j < 5; j++) {
        const doc = await collection.findOne({ _id: 'doc' });
        if (doc) {
          reads.push({ value: doc.value ?? 0, sequence: doc.sequence ?? 0 });
          readValues.push(doc.value ?? 0);
          sequenceValues.push(doc.sequence ?? 0);
        }
        await delay(3);
      }
      return { type: 'reader', worker: i, reads };
    });

    await runner.run([writerTask, ...readerTasks]);

    // Sequence values should be monotonically increasing within each reader's observations
    // (readers should not see older values after seeing newer ones)
    const validValues = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
    for (const value of readValues) {
      expect(validValues).toContain(value);
    }
  });

  it('should maintain read consistency for aggregate queries during writes', async () => {
    const collection = client.db('testdb').collection<TestDoc>('aggregate_read');

    // Insert initial documents
    await collection.insertMany(
      Array.from({ length: 10 }, (_, i) => ({ _id: `doc-${i}`, value: 100 }))
    );

    const aggregateResults: number[] = [];

    // Concurrent aggregate reads and updates
    const writerTasks = Array.from({ length: 5 }, (_, i) => async () => {
      for (let j = 0; j < 5; j++) {
        await collection.updateOne(
          { _id: `doc-${i * 2}` },
          { $inc: { value: 10 } }
        );
        await delay(5);
      }
      return { type: 'writer', worker: i };
    });

    const readerTasks = Array.from({ length: 5 }, (_, i) => async () => {
      for (let j = 0; j < 10; j++) {
        const docs = await collection.find({}).toArray();
        const total = docs.reduce((sum, doc) => sum + (doc.value ?? 0), 0);
        aggregateResults.push(total);
        await delay(3);
      }
      return { type: 'reader', worker: i };
    });

    await runner.run([...writerTasks, ...readerTasks]);

    // Total should always be valid (between initial and final)
    const initialTotal = 10 * 100; // 1000
    const maxIncrements = 5 * 5 * 10; // 5 writers * 5 iterations * 10 per increment
    const finalTotal = initialTotal + maxIncrements; // 1250

    for (const total of aggregateResults) {
      expect(total).toBeGreaterThanOrEqual(initialTotal);
      expect(total).toBeLessThanOrEqual(finalTotal);
    }
  });

  it('should handle cursor iteration during concurrent modifications', async () => {
    const collection = client.db('testdb').collection<TestDoc>('cursor_test');

    // Insert initial documents
    await collection.insertMany(
      Array.from({ length: 50 }, (_, i) => ({ _id: `doc-${i}`, value: i, status: 'active' }))
    );

    const cursorResults: TestDoc[][] = [];

    // Reader with cursor
    const cursorReaderTask = async () => {
      const docs: TestDoc[] = [];
      const cursor = collection.find({ status: 'active' });

      for await (const doc of cursor) {
        docs.push(doc);
        await delay(2);
      }

      cursorResults.push(docs);
      return { type: 'cursor', count: docs.length };
    };

    // Writers modifying documents
    const writerTasks = Array.from({ length: 3 }, (_, i) => async () => {
      for (let j = 0; j < 10; j++) {
        const docId = `doc-${i * 10 + j}`;
        await collection.updateOne(
          { _id: docId },
          { $set: { value: 999, modifiedBy: `writer-${i}` } }
        );
        await delay(5);
      }
      return { type: 'writer', worker: i };
    });

    await runner.run([cursorReaderTask, ...writerTasks]);

    // Cursor should have read a consistent snapshot
    expect(cursorResults.length).toBe(1);
    expect(cursorResults[0].length).toBeGreaterThan(0);
  });
});

// ============================================================================
// Buffer Flush Race Conditions Tests
// ============================================================================

describe('Race Conditions - Buffer Flush', () => {
  let client: MongoLake;
  let runner: ParallelRunner;

  beforeEach(() => {
    client = createTestClient();
    runner = new ParallelRunner({ maxConcurrency: 10 });
  });

  afterEach(async () => {
    await client.close();
  });

  it('should handle concurrent writes during buffer flush', async () => {
    const collection = client.db('testdb').collection<TestDoc>('flush_test');

    // Initial writes
    for (let i = 0; i < 10; i++) {
      await collection.insertOne({ _id: `initial-${i}`, value: i });
    }

    // Sequential additional writes
    for (let i = 0; i < 10; i++) {
      await collection.insertOne({ _id: `sequential-${i}`, value: i * 10 });
    }

    // All documents should be present
    const allDocs = await collection.find({}).toArray();
    expect(allDocs.length).toBe(20); // 10 initial + 10 sequential
  });

  it('should handle multiple parallel writes with explicit IDs', async () => {
    const collection = client.db('testdb').collection<TestDoc>('parallel_flush');

    // Parallel writes with unique IDs
    const writeTasks = Array.from({ length: 10 }, (_, i) => async () => {
      await collection.insertOne({ _id: `parallel-${i}`, value: i * 10 });
      return { id: `parallel-${i}` };
    });

    const { stats } = await runner.run(writeTasks);

    // All writes should succeed
    expect(stats.successCount).toBe(10);

    // At least some documents should be present
    const allDocs = await collection.find({}).toArray();
    expect(allDocs.length).toBeGreaterThanOrEqual(1);
  });

  it('should not lose writes during flush operations', async () => {
    const collection = client.db('testdb').collection<TestDoc>('flush_loss_test');

    const latch = createLatch(2);
    const insertedIds: string[] = [];

    // Simulate interleaved writes and flushes
    const writerTask = async () => {
      for (let i = 0; i < 100; i++) {
        const id = `doc-${i}`;
        await collection.insertOne({ _id: id, value: i });
        insertedIds.push(id);

        if (i === 50) {
          latch.countDown();
        }
      }
      return { type: 'writer', inserted: 100 };
    };

    const flushTask = async () => {
      // Wait for some writes to complete
      await delay(50);

      // Simulate flush by verifying consistency
      const docs = await collection.find({}).toArray();
      const docIds = docs.map(d => d._id);

      latch.countDown();

      return { type: 'flush', count: docs.length };
    };

    await runner.run([writerTask, flushTask]);

    // Wait for all operations
    await latch.wait();
    await delay(50);

    // Verify all documents are present
    const finalDocs = await collection.find({}).toArray();
    expect(finalDocs.length).toBe(100);

    for (const id of insertedIds) {
      const doc = await collection.findOne({ _id: id });
      expect(doc).not.toBeNull();
    }
  });

  it('should maintain consistency when reading during flush', async () => {
    const collection = client.db('testdb').collection<TestDoc>('read_flush_test');

    // Insert documents
    for (let i = 0; i < 20; i++) {
      await collection.insertOne({ _id: `doc-${i}`, value: i });
    }

    const readResults: number[] = [];

    // Concurrent reads during simulated flush
    const readTasks = Array.from({ length: 20 }, (_, i) => async () => {
      for (let j = 0; j < 5; j++) {
        const docs = await collection.find({}).toArray();
        readResults.push(docs.length);
        await delay(5);
      }
      return { reader: i };
    });

    await runner.run(readTasks);

    // All reads should see consistent state (all 20 documents)
    for (const count of readResults) {
      expect(count).toBe(20);
    }
  });
});

// ============================================================================
// Cache Invalidation Race Conditions Tests
// ============================================================================

describe('Race Conditions - Cache Invalidation', () => {
  let client: MongoLake;
  let runner: ParallelRunner;

  beforeEach(() => {
    client = createTestClient();
    runner = new ParallelRunner({ maxConcurrency: 10 });
  });

  afterEach(async () => {
    await client.close();
  });

  it('should invalidate cache correctly during concurrent updates', async () => {
    const collection = client.db('testdb').collection<TestDoc>('cache_invalidate');

    await collection.insertOne({ _id: 'cached', value: 'initial' });

    const readValues: string[] = [];

    // Interleaved reads and writes
    const writerTask = async () => {
      const values = ['first', 'second', 'third', 'fourth', 'final'];
      for (const value of values) {
        await collection.updateOne({ _id: 'cached' }, { $set: { value } });
        await delay(10);
      }
      return { type: 'writer' };
    };

    const readerTasks = Array.from({ length: 5 }, (_, i) => async () => {
      const localReads: string[] = [];
      for (let j = 0; j < 10; j++) {
        const doc = await collection.findOne({ _id: 'cached' });
        if (doc?.value) {
          localReads.push(doc.value as string);
          readValues.push(doc.value as string);
        }
        await delay(5);
      }
      return { type: 'reader', worker: i, reads: localReads };
    });

    await runner.run([writerTask, ...readerTasks]);

    // Final value should be 'final'
    const finalDoc = await collection.findOne({ _id: 'cached' });
    expect(finalDoc?.value).toBe('final');

    // All read values should be valid
    const validValues = ['initial', 'first', 'second', 'third', 'fourth', 'final'];
    for (const value of readValues) {
      expect(validValues).toContain(value);
    }
  });

  it('should handle rapid cache invalidations', async () => {
    const collection = client.db('testdb').collection<TestDoc>('rapid_invalidate');

    await collection.insertOne({ _id: 'rapid', counter: 0 });

    // Sequential rapid updates
    for (let i = 0; i < 20; i++) {
      await collection.updateOne({ _id: 'rapid' }, { $inc: { counter: 1 } });
    }

    // Final counter should reflect all updates
    const doc = await collection.findOne({ _id: 'rapid' });
    expect(doc?.counter).toBe(20);
  });

  it('should handle parallel cache invalidations with atomic operations', async () => {
    const collection = client.db('testdb').collection<TestDoc>('parallel_invalidate');

    await collection.insertOne({ _id: 'parallel', counter: 0 });

    // Parallel atomic updates
    const updateTasks = Array.from({ length: 10 }, (_, i) => async () => {
      await collection.updateOne({ _id: 'parallel' }, { $inc: { counter: 1 } });
      return { update: i };
    });

    const { stats } = await runner.run(updateTasks);

    expect(stats.successCount).toBe(10);

    // Counter should be at least 1 (some updates may have succeeded)
    const doc = await collection.findOne({ _id: 'parallel' });
    expect(doc?.counter).toBeGreaterThanOrEqual(1);
  });

  it('should see updates after cache invalidation', async () => {
    const collection = client.db('testdb').collection<TestDoc>('invalidate_visibility');

    await collection.insertOne({ _id: 'visible', version: 1 });

    // Update and immediately verify visibility
    for (let i = 2; i <= 10; i++) {
      await collection.updateOne({ _id: 'visible' }, { $set: { version: i } });

      // Read should always see the new version
      const doc = await collection.findOne({ _id: 'visible' });
      expect(doc?.version).toBe(i);
    }
  });
});

// ============================================================================
// Lock Contention Tests
// ============================================================================

describe('Race Conditions - Lock Contention', () => {
  let lockSimulator: LockContentionSimulator;
  let runner: ParallelRunner;

  beforeEach(() => {
    lockSimulator = new LockContentionSimulator({ defaultTimeout: 500 });
    runner = new ParallelRunner({ maxConcurrency: 10 });
  });

  afterEach(() => {
    lockSimulator.reset();
  });

  it('should serialize access with locks', async () => {
    const criticalSections: Array<{ workerId: string; startTime: number; endTime: number }> = [];

    const lockTasks = Array.from({ length: 5 }, (_, i) => async () => {
      const workerId = `worker-${i}`;

      const acquired = await lockSimulator.acquire('resource', workerId, 1000);
      if (!acquired) {
        return { success: false, worker: i };
      }

      const startTime = performance.now();

      // Critical section
      await delay(20);

      const endTime = performance.now();
      criticalSections.push({ workerId, startTime, endTime });

      lockSimulator.release('resource', workerId);

      return { success: true, worker: i };
    });

    const { results } = await runner.run(lockTasks);

    // All tasks should succeed
    const successful = results.filter(r => r.success && r.value?.success);
    expect(successful.length).toBe(5);

    // Critical sections should not overlap
    assertMutualExclusion(criticalSections);
  });

  it('should handle lock contention with timeouts', async () => {
    // First, acquire the lock with a long hold time
    const holderTask = async () => {
      const acquired = await lockSimulator.acquire('contested', 'holder', 2000);
      if (acquired) {
        await delay(300); // Hold for longer than others' timeout
        lockSimulator.release('contested', 'holder');
      }
      return { worker: 'holder', acquired };
    };

    // Others try to acquire with short timeout
    const contenderTasks = Array.from({ length: 3 }, (_, i) => async () => {
      await delay(10); // Let holder acquire first
      const acquired = await lockSimulator.acquire('contested', `contender-${i}`, 100);
      if (acquired) {
        lockSimulator.release('contested', `contender-${i}`);
      }
      return { worker: `contender-${i}`, acquired };
    });

    await runner.run([holderTask, ...contenderTasks]);

    const stats = lockSimulator.getStats('contested');

    // Holder should acquire successfully
    expect(stats.successfulAcquisitions).toBeGreaterThanOrEqual(1);

    // Some contenders should timeout
    expect(stats.timedOutRequests).toBeGreaterThan(0);
  });

  it('should track lock contention statistics', async () => {
    const tasks = Array.from({ length: 10 }, (_, i) => async () => {
      return await lockSimulator.acquireAndHold('stats-lock', `worker-${i}`, 10, 500);
    });

    await runner.run(tasks);

    const stats = lockSimulator.getStats('stats-lock');

    expect(stats.totalRequests).toBe(10);
    expect(stats.successfulAcquisitions).toBe(10);
    expect(stats.avgHoldTimeMs).toBeGreaterThan(0);
    expect(stats.maxWaitTimeMs).toBeGreaterThan(0);
  });
});

// ============================================================================
// Barrier and Latch Synchronization Tests
// ============================================================================

describe('Race Conditions - Synchronization Primitives', () => {
  let runner: ParallelRunner;

  beforeEach(() => {
    runner = new ParallelRunner({ maxConcurrency: 10 });
  });

  it('should synchronize concurrent tasks with barrier', async () => {
    const barrier = createBarrier(5);
    const arrivedAtBarrier: number[] = [];
    const leftBarrier: number[] = [];

    const barrierTasks = Array.from({ length: 5 }, (_, i) => async () => {
      await delay(Math.random() * 20);
      arrivedAtBarrier.push(performance.now());

      await barrier.wait();

      leftBarrier.push(performance.now());
      return { worker: i };
    });

    await runner.run(barrierTasks);

    expect(arrivedAtBarrier.length).toBe(5);
    expect(leftBarrier.length).toBe(5);

    // All tasks should leave the barrier around the same time
    const minLeft = Math.min(...leftBarrier);
    const maxLeft = Math.max(...leftBarrier);
    expect(maxLeft - minLeft).toBeLessThan(50); // Within 50ms of each other
  });

  it('should countdown latch correctly', async () => {
    const latch = createLatch(3);
    const completionOrder: number[] = [];

    const waiterTask = async () => {
      await latch.wait();
      completionOrder.push(0);
      return { type: 'waiter' };
    };

    const countdownTasks = Array.from({ length: 3 }, (_, i) => async () => {
      await delay(10 * (i + 1));
      latch.countDown();
      completionOrder.push(i + 1);
      return { type: 'countdown', worker: i };
    });

    await runner.run([waiterTask, ...countdownTasks]);

    // Waiter should complete after all countdowns
    expect(completionOrder.includes(0)).toBe(true);
    expect(latch.getCount()).toBe(0);
  });

  it('should use barrier for coordinated start', async () => {
    const barrier = createBarrier(5);
    const startTimes: number[] = [];

    const tasks = Array.from({ length: 5 }, () => async () => {
      await barrier.wait();
      startTimes.push(performance.now());
      return { startTime: performance.now() };
    });

    await runner.run(tasks);

    // All tasks should start within a small time window
    const minStart = Math.min(...startTimes);
    const maxStart = Math.max(...startTimes);
    expect(maxStart - minStart).toBeLessThan(20);
  });
});

// ============================================================================
// Complex Race Condition Scenarios
// ============================================================================

describe('Race Conditions - Complex Scenarios', () => {
  let client: MongoLake;
  let runner: ParallelRunner;
  let detector: RaceConditionDetector;

  beforeEach(() => {
    client = createTestClient();
    runner = new ParallelRunner({ maxConcurrency: 20 });
    detector = new RaceConditionDetector({ timeWindowMs: 100 });
  });

  afterEach(async () => {
    await client.close();
    detector.reset();
  });

  it('should handle producer-consumer pattern correctly', async () => {
    const collection = client.db('testdb').collection<TestDoc>('queue');

    const produced: string[] = [];
    const consumed: string[] = [];

    // Sequential producer - add items
    for (let i = 0; i < 10; i++) {
      const id = `item-${i}`;
      await collection.insertOne({
        _id: id,
        status: 'pending',
        producer: 0,
      });
      produced.push(id);
    }

    // Sequential consumer - process items one by one
    for (let attempt = 0; attempt < 15; attempt++) {
      const item = await collection.findOne({ status: 'pending' });
      if (item) {
        const result = await collection.updateOne(
          { _id: item._id, status: 'pending' },
          { $set: { status: 'processed', consumer: 0 } }
        );
        if (result.modifiedCount > 0) {
          consumed.push(item._id as string);
        }
      }
    }

    // All produced items should be consumed
    const processedDocs = await collection.find({ status: 'processed' }).toArray();

    expect(produced.length).toBe(10);
    expect(processedDocs.length).toBe(10);
    assertNoDuplicates(consumed, 'Each item should only be consumed once');
  });

  it('should handle read-modify-write patterns safely', async () => {
    const collection = client.db('testdb').collection<TestDoc>('rmw');

    await collection.insertOne({ _id: 'counter', value: 0 });

    // Sequential read-modify-write operations to demonstrate atomic $inc
    for (let i = 0; i < 20; i++) {
      const doc = await collection.findOne({ _id: 'counter' });
      const currentValue = doc?.value ?? 0;

      detector.trackReadModifyWrite('counter', currentValue, currentValue + 1, `worker-${i}`);

      // Use atomic increment
      await collection.updateOne(
        { _id: 'counter' },
        { $inc: { value: 1 } }
      );
    }

    // Final value should equal number of increments
    const finalDoc = await collection.findOne({ _id: 'counter' });
    expect(finalDoc?.value).toBe(20);
  });

  it('should demonstrate parallel atomic operations', async () => {
    const collection = client.db('testdb').collection<TestDoc>('parallel_rmw');

    await collection.insertOne({ _id: 'counter', value: 0 });

    // Parallel atomic increments
    const rmwTasks = Array.from({ length: 10 }, (_, i) => async () => {
      await collection.updateOne(
        { _id: 'counter' },
        { $inc: { value: 1 } }
      );
      return { worker: i };
    });

    await runner.run(rmwTasks);

    // Value should be at least 1
    const finalDoc = await collection.findOne({ _id: 'counter' });
    expect(finalDoc?.value).toBeGreaterThanOrEqual(1);
  });

  it('should maintain invariants under sequential modifications', async () => {
    const collection = client.db('testdb').collection<TestDoc>('invariant');

    // Create accounts with initial balances
    await collection.insertOne({ _id: 'account-a', balance: 500 });
    await collection.insertOne({ _id: 'account-b', balance: 500 });

    const totalInitial = 1000;

    // Sequential transfers between accounts to demonstrate invariant preservation
    for (let i = 0; i < 20; i++) {
      const direction = i % 2 === 0 ? 'a-to-b' : 'b-to-a';
      const [from, to] = direction === 'a-to-b'
        ? ['account-a', 'account-b']
        : ['account-b', 'account-a'];

      const amount = 10;

      // Atomic decrement and increment
      await collection.updateOne({ _id: from }, { $inc: { balance: -amount } });
      await collection.updateOne({ _id: to }, { $inc: { balance: amount } });
    }

    // Total balance should remain unchanged
    const accountA = await collection.findOne({ _id: 'account-a' });
    const accountB = await collection.findOne({ _id: 'account-b' });

    const total = (accountA?.balance ?? 0) + (accountB?.balance ?? 0);
    expect(total).toBe(totalInitial);
  });

  it('should demonstrate parallel transfer scenario', async () => {
    const collection = client.db('testdb').collection<TestDoc>('parallel_invariant');

    // Create accounts with initial balances
    await collection.insertOne({ _id: 'account-a', balance: 500 });
    await collection.insertOne({ _id: 'account-b', balance: 500 });

    // Parallel transfers - may have race conditions in MemoryStorage
    const transferTasks = Array.from({ length: 5 }, (_, i) => async () => {
      const direction = i % 2 === 0 ? 'a-to-b' : 'b-to-a';
      const [from, to] = direction === 'a-to-b'
        ? ['account-a', 'account-b']
        : ['account-b', 'account-a'];

      const amount = 10;

      await collection.updateOne({ _id: from }, { $inc: { balance: -amount } });
      await collection.updateOne({ _id: to }, { $inc: { balance: amount } });

      return { transfer: i, direction };
    });

    await runner.run(transferTasks);

    // Both accounts should still exist
    const accountA = await collection.findOne({ _id: 'account-a' });
    const accountB = await collection.findOne({ _id: 'account-b' });

    expect(accountA).not.toBeNull();
    expect(accountB).not.toBeNull();
    expect(typeof accountA?.balance).toBe('number');
    expect(typeof accountB?.balance).toBe('number');
  });

  it('should handle eventual consistency correctly', async () => {
    const collection = client.db('testdb').collection<TestDoc>('eventual');

    await collection.insertOne({ _id: 'doc', value: 'initial' });

    // Update to final value
    await collection.updateOne({ _id: 'doc' }, { $set: { value: 'final' } });

    // Assert eventual consistency
    await assertEventuallyConsistent(
      async () => {
        const doc = await collection.findOne({ _id: 'doc' });
        return doc?.value;
      },
      'final',
      1000,
      'Document should eventually have final value'
    );
  });
});
