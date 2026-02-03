/**
 * Concurrency Stress Tests
 *
 * RED phase tests for concurrent operations:
 * - Parallel read/write operations
 * - Lock contention scenarios
 * - Race condition detection
 * - Deadlock detection
 * - High-throughput concurrent operations
 *
 * Issue: mongolake-uzt7
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import type { DurableObjectState, DurableObjectStorage, R2Bucket } from '@cloudflare/workers-types';
import { Semaphore, MemoryStorage } from '../../../src/storage/index.js';

// ============================================================================
// Mock Helpers
// ============================================================================

function createMockStorage(): DurableObjectStorage {
  const data = new Map<string, unknown>();
  const sqlStatements: string[] = [];
  const metadata = new Map<string, string>();
  const wal: Array<{
    lsn: number;
    collection: string;
    op: string;
    doc_id: string;
    document: string;
    flushed: number;
  }> = [];
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
          let key: string;
          const keyMatch = query.match(/VALUES\s*\('(\w+)'/);
          if (keyMatch) {
            key = keyMatch[1];
            metadata.set(key, args[0] as string);
          } else {
            key = String(args[0]);
            metadata.set(key, String(args[1]));
          }
        }
        if (query.includes('SELECT value FROM metadata')) {
          const key = query.match(/key = '(\w+)'/)?.[1];
          const value = key ? metadata.get(key) : undefined;
          return {
            toArray: () => (value ? [{ value }] : []),
            one: () => (value ? { value } : null),
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
          const unflushed = wal.filter((e) => e.flushed === 0);
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
          const toRemove = wal.filter((e) => e.flushed === 1);
          for (const entry of toRemove) {
            const idx = wal.indexOf(entry);
            if (idx !== -1) wal.splice(idx, 1);
          }
        }

        if (query.includes('INSERT OR REPLACE INTO manifests')) {
          manifests.set(args[0] as string, args[1] as string);
        }
        if (query.includes('SELECT collection, data FROM manifests')) {
          const rows = Array.from(manifests.entries()).map(([collection, data]) => ({
            collection,
            data,
          }));
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
      const data =
        value instanceof Uint8Array
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
    createMultipartUpload: vi.fn(),
    resumeMultipartUpload: vi.fn(),
    _objects: objects,
  } as unknown as R2Bucket;
}

// ============================================================================
// Concurrent Document Store for Testing
// ============================================================================

/**
 * A simple concurrent document store for testing concurrency patterns.
 * This simulates the core operations of ShardDO without the full complexity.
 */
class ConcurrentDocumentStore {
  private documents: Map<string, Map<string, unknown>> = new Map();
  private locks: Map<string, Promise<void>> = new Map();
  private lockResolvers: Map<string, () => void> = new Map();
  private lsn = 0;

  async insert(collection: string, document: { _id: string; [key: string]: unknown }): Promise<{ lsn: number }> {
    await this.acquireLock(collection);
    try {
      if (!this.documents.has(collection)) {
        this.documents.set(collection, new Map());
      }
      const coll = this.documents.get(collection)!;
      if (coll.has(document._id)) {
        throw new Error(`Duplicate key: ${document._id}`);
      }
      coll.set(document._id, { ...document });
      return { lsn: ++this.lsn };
    } finally {
      this.releaseLock(collection);
    }
  }

  async update(
    collection: string,
    filter: { _id: string },
    update: { $set?: Record<string, unknown>; $inc?: Record<string, number> }
  ): Promise<{ modifiedCount: number; lsn: number }> {
    await this.acquireLock(collection);
    try {
      const coll = this.documents.get(collection);
      if (!coll) return { modifiedCount: 0, lsn: ++this.lsn };

      const doc = coll.get(filter._id) as Record<string, unknown> | undefined;
      if (!doc) return { modifiedCount: 0, lsn: ++this.lsn };

      if (update.$set) {
        for (const [key, value] of Object.entries(update.$set)) {
          doc[key] = value;
        }
      }
      if (update.$inc) {
        for (const [key, value] of Object.entries(update.$inc)) {
          doc[key] = ((doc[key] as number) || 0) + value;
        }
      }

      return { modifiedCount: 1, lsn: ++this.lsn };
    } finally {
      this.releaseLock(collection);
    }
  }

  async find(collection: string, filter: Record<string, unknown> = {}): Promise<unknown[]> {
    const coll = this.documents.get(collection);
    if (!coll) return [];

    const results: unknown[] = [];
    for (const doc of coll.values()) {
      let matches = true;
      for (const [key, value] of Object.entries(filter)) {
        if ((doc as Record<string, unknown>)[key] !== value) {
          matches = false;
          break;
        }
      }
      if (matches) {
        results.push({ ...doc });
      }
    }
    return results;
  }

  async findOne(collection: string, filter: { _id: string }): Promise<unknown | null> {
    const coll = this.documents.get(collection);
    if (!coll) return null;
    const doc = coll.get(filter._id);
    return doc ? { ...doc } : null;
  }

  async delete(collection: string, filter: { _id: string }): Promise<{ deletedCount: number; lsn: number }> {
    await this.acquireLock(collection);
    try {
      const coll = this.documents.get(collection);
      if (!coll) return { deletedCount: 0, lsn: ++this.lsn };
      const existed = coll.delete(filter._id);
      return { deletedCount: existed ? 1 : 0, lsn: ++this.lsn };
    } finally {
      this.releaseLock(collection);
    }
  }

  private async acquireLock(collection: string): Promise<void> {
    while (this.locks.has(collection)) {
      await this.locks.get(collection);
    }
    let resolver: () => void;
    const promise = new Promise<void>((resolve) => {
      resolver = resolve;
    });
    this.locks.set(collection, promise);
    this.lockResolvers.set(collection, resolver!);
  }

  private releaseLock(collection: string): void {
    const resolver = this.lockResolvers.get(collection);
    this.locks.delete(collection);
    this.lockResolvers.delete(collection);
    resolver?.();
  }

  getLsn(): number {
    return this.lsn;
  }
}

// ============================================================================
// Parallel Read/Write Operations Tests
// ============================================================================

describe('Concurrency Stress Tests - Parallel Read/Write Operations', () => {
  let store: ConcurrentDocumentStore;

  beforeEach(() => {
    store = new ConcurrentDocumentStore();
  });

  it('should handle parallel inserts to same collection without data loss', async () => {
    const numDocuments = 100;
    const collection = 'users';

    const insertPromises = Array.from({ length: numDocuments }, (_, i) =>
      store.insert(collection, { _id: `user-${i}`, name: `User ${i}`, index: i })
    );

    const results = await Promise.all(insertPromises);

    // All inserts should succeed
    expect(results).toHaveLength(numDocuments);

    // All LSNs should be unique
    const lsns = new Set(results.map((r) => r.lsn));
    expect(lsns.size).toBe(numDocuments);

    // All documents should be retrievable
    const documents = await store.find(collection);
    expect(documents).toHaveLength(numDocuments);
  });

  it('should handle parallel inserts to different collections', async () => {
    const collections = ['users', 'orders', 'products', 'logs', 'events'];
    const docsPerCollection = 20;

    const insertPromises = collections.flatMap((collection) =>
      Array.from({ length: docsPerCollection }, (_, i) =>
        store.insert(collection, { _id: `${collection}-${i}`, collection, index: i })
      )
    );

    await Promise.all(insertPromises);

    // Verify each collection has correct document count
    for (const collection of collections) {
      const docs = await store.find(collection);
      expect(docs).toHaveLength(docsPerCollection);
    }
  });

  it('should maintain consistency under concurrent read/write', async () => {
    const collection = 'items';
    const writeCount = 50;

    // Writer task
    const writer = async () => {
      for (let i = 0; i < writeCount; i++) {
        await store.insert(collection, { _id: `item-${i}`, value: i });
      }
    };

    // Reader task
    const reader = async () => {
      const counts: number[] = [];
      for (let i = 0; i < 30; i++) {
        const items = await store.find(collection);
        counts.push(items.length);
        await new Promise((r) => setTimeout(r, 1));
      }
      return counts;
    };

    const [, readerCounts] = await Promise.all([writer(), reader()]);

    // Reader should see monotonically increasing counts (no data loss)
    for (let i = 1; i < readerCounts.length; i++) {
      expect(readerCounts[i]).toBeGreaterThanOrEqual(readerCounts[i - 1]);
    }

    // Final count should be writeCount
    const finalDocs = await store.find(collection);
    expect(finalDocs).toHaveLength(writeCount);
  });

  it('should handle concurrent updates to same document correctly', async () => {
    const collection = 'counters';
    await store.insert(collection, { _id: 'counter', value: 0 });

    const incrementCount = 100;
    const incrementPromises = Array.from({ length: incrementCount }, () =>
      store.update(collection, { _id: 'counter' }, { $inc: { value: 1 } })
    );

    await Promise.all(incrementPromises);

    const result = (await store.findOne(collection, { _id: 'counter' })) as { value: number };
    expect(result.value).toBe(incrementCount);
  });

  it('should handle mixed read/write/delete operations concurrently', async () => {
    const collection = 'mixed';

    // Pre-populate
    for (let i = 0; i < 50; i++) {
      await store.insert(collection, { _id: `doc-${i}`, value: i });
    }

    const operations: Promise<unknown>[] = [];

    // Concurrent reads
    for (let i = 0; i < 20; i++) {
      operations.push(store.find(collection));
    }

    // Concurrent updates
    for (let i = 0; i < 20; i++) {
      operations.push(store.update(collection, { _id: `doc-${i}` }, { $set: { updated: true } }));
    }

    // Concurrent deletes
    for (let i = 40; i < 50; i++) {
      operations.push(store.delete(collection, { _id: `doc-${i}` }));
    }

    // Concurrent inserts
    for (let i = 50; i < 60; i++) {
      operations.push(store.insert(collection, { _id: `doc-${i}`, value: i }));
    }

    await Promise.all(operations);

    // Verify final state
    const docs = await store.find(collection);
    expect(docs.length).toBe(50); // 50 - 10 deleted + 10 inserted
  });
});

// ============================================================================
// Lock Contention Scenarios Tests
// ============================================================================

describe('Concurrency Stress Tests - Lock Contention Scenarios', () => {
  let store: ConcurrentDocumentStore;

  beforeEach(() => {
    store = new ConcurrentDocumentStore();
  });

  it('should serialize writes to same collection under high contention', async () => {
    const collection = 'contended';
    const writeCount = 50;
    const operationOrder: number[] = [];

    const writes = Array.from({ length: writeCount }, (_, i) =>
      store.insert(collection, { _id: `doc-${i}`, order: i }).then((result) => {
        operationOrder.push(result.lsn);
        return result;
      })
    );

    await Promise.all(writes);

    // All writes should complete
    expect(operationOrder).toHaveLength(writeCount);

    // LSNs should be sequential (serialized execution)
    const sortedLsns = [...operationOrder].sort((a, b) => a - b);
    for (let i = 1; i < sortedLsns.length; i++) {
      expect(sortedLsns[i]).toBe(sortedLsns[i - 1] + 1);
    }
  });

  it('should handle semaphore-based concurrency limiting', async () => {
    const semaphore = new Semaphore(3);
    let currentConcurrency = 0;
    let maxConcurrency = 0;
    const concurrencyHistory: number[] = [];

    const operation = async () => {
      await semaphore.acquire();
      try {
        currentConcurrency++;
        maxConcurrency = Math.max(maxConcurrency, currentConcurrency);
        concurrencyHistory.push(currentConcurrency);
        // Simulate work
        await new Promise((r) => setTimeout(r, 5));
      } finally {
        currentConcurrency--;
        semaphore.release();
      }
    };

    await Promise.all(Array.from({ length: 20 }, () => operation()));

    expect(maxConcurrency).toBe(3);
    expect(concurrencyHistory.every((c) => c <= 3)).toBe(true);
  });

  it('should maintain FIFO ordering under semaphore contention', async () => {
    const semaphore = new Semaphore(1);
    const executionOrder: number[] = [];

    // Hold the semaphore
    await semaphore.acquire();

    // Queue up operations
    const operations = Array.from({ length: 5 }, (_, i) =>
      semaphore.acquire().then(() => {
        executionOrder.push(i);
        semaphore.release();
      })
    );

    // Release initial hold after a small delay
    await new Promise((r) => setTimeout(r, 10));
    semaphore.release();

    await Promise.all(operations);

    // Operations should execute in FIFO order
    expect(executionOrder).toEqual([0, 1, 2, 3, 4]);
  });

  it('should handle multiple hot spots (popular keys) concurrently', async () => {
    const hotKeys = ['hot-1', 'hot-2', 'hot-3'];
    const collection = 'hotspot';

    // Initialize hot documents
    for (const key of hotKeys) {
      await store.insert(collection, { _id: key, accessCount: 0 });
    }

    // High contention on hot keys
    const operations = [];
    for (let i = 0; i < 100; i++) {
      const key = hotKeys[i % hotKeys.length];
      operations.push(store.update(collection, { _id: key }, { $inc: { accessCount: 1 } }));
    }

    await Promise.all(operations);

    // Verify each hot key has correct count
    for (const key of hotKeys) {
      const doc = (await store.findOne(collection, { _id: key })) as { accessCount: number };
      // Each key should have been updated ~33 times (100 / 3)
      expect(doc.accessCount).toBeGreaterThanOrEqual(33);
    }

    // Total access count should be 100
    let totalAccess = 0;
    for (const key of hotKeys) {
      const doc = (await store.findOne(collection, { _id: key })) as { accessCount: number };
      totalAccess += doc.accessCount;
    }
    expect(totalAccess).toBe(100);
  });
});

// ============================================================================
// Race Condition Detection Tests
// ============================================================================

describe('Concurrency Stress Tests - Race Condition Detection', () => {
  let store: ConcurrentDocumentStore;

  beforeEach(() => {
    store = new ConcurrentDocumentStore();
  });

  it('should detect lost update race condition (if present)', async () => {
    const collection = 'race';
    await store.insert(collection, { _id: 'shared', value: 0 });

    // Concurrent increments that might race
    const incrementCount = 100;
    const results = await Promise.all(
      Array.from({ length: incrementCount }, () =>
        store.update(collection, { _id: 'shared' }, { $inc: { value: 1 } })
      )
    );

    // All updates should succeed
    expect(results.filter((r) => r.modifiedCount === 1)).toHaveLength(incrementCount);

    // Final value should match increment count (no lost updates)
    const doc = (await store.findOne(collection, { _id: 'shared' })) as { value: number };
    expect(doc.value).toBe(incrementCount);
  });

  it('should handle read-modify-write patterns safely', async () => {
    const collection = 'rmw';
    await store.insert(collection, { _id: 'balance', amount: 1000 });

    // Simulate concurrent transfers that read, modify, then write
    const transfers = 10;
    const transferAmount = 10;

    // Each transfer reads current balance, deducts, and writes back
    const transferPromises = Array.from({ length: transfers }, async () => {
      const current = (await store.findOne(collection, { _id: 'balance' })) as { amount: number };
      if (current && current.amount >= transferAmount) {
        await store.update(collection, { _id: 'balance' }, { $inc: { amount: -transferAmount } });
        return true;
      }
      return false;
    });

    const results = await Promise.all(transferPromises);
    const successfulTransfers = results.filter((r) => r).length;

    const finalBalance = (await store.findOne(collection, { _id: 'balance' })) as { amount: number };

    // Note: Without proper isolation, we might have more successful transfers than expected
    // This test documents the current behavior - with proper locking, this should be safe
    expect(finalBalance.amount).toBe(1000 - successfulTransfers * transferAmount);
  });

  it('should handle check-then-act race conditions', async () => {
    const collection = 'check-act';

    // Multiple writers trying to create a unique document
    const createOnce = async (id: string): Promise<boolean> => {
      const existing = await store.findOne(collection, { _id: id });
      if (existing) return false;
      try {
        await store.insert(collection, { _id: id, createdBy: Math.random() });
        return true;
      } catch {
        return false; // Duplicate key error
      }
    };

    // Concurrent attempts to create same document
    const attempts = await Promise.all(
      Array.from({ length: 10 }, () => createOnce('unique-doc'))
    );

    // Only one should succeed due to duplicate key protection
    const successes = attempts.filter((r) => r).length;
    expect(successes).toBe(1);

    // Document should exist
    const doc = await store.findOne(collection, { _id: 'unique-doc' });
    expect(doc).not.toBeNull();
  });

  it('should maintain monotonic LSN sequence under concurrent writes', async () => {
    const collection = 'lsn-test';
    const writeCount = 100;

    const writes = await Promise.all(
      Array.from({ length: writeCount }, (_, i) =>
        store.insert(collection, { _id: `doc-${i}`, index: i })
      )
    );

    const lsns = writes.map((w) => w.lsn);

    // All LSNs should be unique
    const uniqueLsns = new Set(lsns);
    expect(uniqueLsns.size).toBe(writeCount);

    // LSNs should form a contiguous sequence
    const sortedLsns = [...lsns].sort((a, b) => a - b);
    for (let i = 1; i < sortedLsns.length; i++) {
      expect(sortedLsns[i] - sortedLsns[i - 1]).toBe(1);
    }
  });
});

// ============================================================================
// Deadlock Detection Tests
// ============================================================================

describe('Concurrency Stress Tests - Deadlock Detection', () => {
  it('should complete without deadlock when accessing multiple resources in same order', async () => {
    const semaphoreA = new Semaphore(1);
    const semaphoreB = new Semaphore(1);

    const operation = async (id: number) => {
      // Always acquire in same order to avoid deadlock
      await semaphoreA.acquire();
      try {
        await semaphoreB.acquire();
        try {
          // Simulate work with both resources
          await new Promise((r) => setTimeout(r, 1));
          return id;
        } finally {
          semaphoreB.release();
        }
      } finally {
        semaphoreA.release();
      }
    };

    const results = await Promise.all(Array.from({ length: 10 }, (_, i) => operation(i)));

    expect(results).toHaveLength(10);
  });

  it('should detect potential deadlock with timeout', async () => {
    const semaphoreA = new Semaphore(1);
    const semaphoreB = new Semaphore(1);

    // Helper to acquire with timeout
    const acquireWithTimeout = async (semaphore: Semaphore, timeoutMs: number): Promise<boolean> => {
      const timeoutPromise = new Promise<false>((resolve) => setTimeout(() => resolve(false), timeoutMs));
      const acquirePromise = semaphore.acquire().then(() => true);
      return Promise.race([acquirePromise, timeoutPromise]);
    };

    // Pre-acquire both semaphores
    await semaphoreA.acquire();
    await semaphoreB.acquire();

    // Try to acquire in opposite order (simulated potential deadlock)
    const [resultA, resultB] = await Promise.all([
      acquireWithTimeout(semaphoreB, 50), // This will timeout
      acquireWithTimeout(semaphoreA, 50), // This will also timeout
    ]);

    // Both should timeout (simulating deadlock detection)
    expect(resultA).toBe(false);
    expect(resultB).toBe(false);

    // Clean up
    semaphoreA.release();
    semaphoreB.release();
  });

  it('should handle resource hierarchy correctly', async () => {
    // Create resources with hierarchy (must be acquired in order)
    const resources = [new Semaphore(1), new Semaphore(1), new Semaphore(1)];

    const acquireAll = async (indices: number[]) => {
      const sortedIndices = [...indices].sort((a, b) => a - b);
      const acquired: number[] = [];

      try {
        for (const i of sortedIndices) {
          await resources[i].acquire();
          acquired.push(i);
        }
        // Simulate work
        await new Promise((r) => setTimeout(r, 1));
      } finally {
        // Release in reverse order
        for (const i of acquired.reverse()) {
          resources[i].release();
        }
      }
    };

    // Multiple operations acquiring different subsets
    await Promise.all([
      acquireAll([0, 1]),
      acquireAll([1, 2]),
      acquireAll([0, 2]),
      acquireAll([0, 1, 2]),
    ]);

    // If we get here without hanging, no deadlock occurred
    expect(true).toBe(true);
  });

  it('should complete circular operations without deadlock via single lock', async () => {
    const store = new ConcurrentDocumentStore();
    const collection = 'circular';

    // Insert initial documents
    await store.insert(collection, { _id: 'A', next: 'B' });
    await store.insert(collection, { _id: 'B', next: 'C' });
    await store.insert(collection, { _id: 'C', next: 'A' });

    // Operations that would create circular dependencies without proper locking
    const operations = [
      store.update(collection, { _id: 'A' }, { $set: { value: 1 } }),
      store.update(collection, { _id: 'B' }, { $set: { value: 2 } }),
      store.update(collection, { _id: 'C' }, { $set: { value: 3 } }),
    ];

    await Promise.all(operations);

    // Verify all updates completed
    const docA = (await store.findOne(collection, { _id: 'A' })) as { value: number };
    const docB = (await store.findOne(collection, { _id: 'B' })) as { value: number };
    const docC = (await store.findOne(collection, { _id: 'C' })) as { value: number };

    expect(docA.value).toBe(1);
    expect(docB.value).toBe(2);
    expect(docC.value).toBe(3);
  });
});

// ============================================================================
// High-Throughput Concurrent Operations Tests
// ============================================================================

describe('Concurrency Stress Tests - High-Throughput Operations', () => {
  let store: ConcurrentDocumentStore;

  beforeEach(() => {
    store = new ConcurrentDocumentStore();
  });

  it('should handle 1000 concurrent inserts', async () => {
    const collection = 'high-throughput';
    const documentCount = 1000;

    const startTime = Date.now();
    const results = await Promise.all(
      Array.from({ length: documentCount }, (_, i) =>
        store.insert(collection, {
          _id: `doc-${i}`,
          timestamp: Date.now(),
          index: i,
          data: 'x'.repeat(100), // 100 byte payload
        })
      )
    );
    const endTime = Date.now();

    // All inserts should succeed
    expect(results).toHaveLength(documentCount);

    // Verify all documents exist
    const docs = await store.find(collection);
    expect(docs).toHaveLength(documentCount);

    // Log throughput (informational)
    const durationMs = endTime - startTime;
    const throughput = (documentCount / durationMs) * 1000;
    console.log(`Throughput: ${throughput.toFixed(0)} inserts/second`);
  });

  it('should handle burst write patterns', async () => {
    const collection = 'burst';
    const burstSize = 100;
    const burstCount = 5;
    const results: number[] = [];

    for (let burst = 0; burst < burstCount; burst++) {
      const burstResults = await Promise.all(
        Array.from({ length: burstSize }, (_, i) =>
          store.insert(collection, {
            _id: `burst-${burst}-doc-${i}`,
            burst,
            index: i,
          })
        )
      );
      results.push(burstResults.length);
      // Small pause between bursts
      await new Promise((r) => setTimeout(r, 10));
    }

    expect(results).toEqual(Array(burstCount).fill(burstSize));

    const totalDocs = await store.find(collection);
    expect(totalDocs).toHaveLength(burstSize * burstCount);
  });

  it('should handle sustained concurrent read/write load', async () => {
    const collection = 'sustained';
    const duration = 200; // ms
    let writeCount = 0;
    let readCount = 0;
    let running = true;

    // Pre-populate some data
    for (let i = 0; i < 100; i++) {
      await store.insert(collection, { _id: `initial-${i}`, value: i });
    }

    // Writer loop
    const writer = async () => {
      while (running) {
        try {
          await store.insert(collection, {
            _id: `write-${Date.now()}-${Math.random()}`,
            timestamp: Date.now(),
          });
          writeCount++;
        } catch {
          // Ignore errors and continue
        }
        await new Promise((r) => setTimeout(r, 1));
      }
    };

    // Reader loop
    const reader = async () => {
      while (running) {
        try {
          await store.find(collection);
          readCount++;
        } catch {
          // Ignore errors and continue
        }
        await new Promise((r) => setTimeout(r, 1));
      }
    };

    // Start concurrent operations
    const writerPromise = writer();
    const readerPromise = reader();

    // Run for duration
    await new Promise((r) => setTimeout(r, duration));
    running = false;

    await Promise.all([writerPromise, readerPromise]);

    console.log(`Sustained load: ${writeCount} writes, ${readCount} reads in ${duration}ms`);
    expect(writeCount).toBeGreaterThan(0);
    expect(readCount).toBeGreaterThan(0);
  });

  it('should handle concurrent operations across multiple collections', async () => {
    const collectionCount = 10;
    const operationsPerCollection = 50;

    const allOperations = [];

    for (let c = 0; c < collectionCount; c++) {
      const collection = `collection-${c}`;
      for (let i = 0; i < operationsPerCollection; i++) {
        allOperations.push(
          store.insert(collection, { _id: `doc-${i}`, collection: c, index: i })
        );
      }
    }

    await Promise.all(allOperations);

    // Verify each collection has correct count
    for (let c = 0; c < collectionCount; c++) {
      const docs = await store.find(`collection-${c}`);
      expect(docs).toHaveLength(operationsPerCollection);
    }
  });

  it('should maintain data integrity under high contention', async () => {
    const collection = 'integrity';
    const initialBalance = 10000;

    // Create accounts
    await store.insert(collection, { _id: 'account-A', balance: initialBalance });
    await store.insert(collection, { _id: 'account-B', balance: initialBalance });

    // Concurrent transfers between accounts
    const transferCount = 100;
    const transferAmount = 10;

    const transfers = Array.from({ length: transferCount }, async (_, i) => {
      const from = i % 2 === 0 ? 'account-A' : 'account-B';
      const to = i % 2 === 0 ? 'account-B' : 'account-A';

      await store.update(collection, { _id: from }, { $inc: { balance: -transferAmount } });
      await store.update(collection, { _id: to }, { $inc: { balance: transferAmount } });
    });

    await Promise.all(transfers);

    // Total balance should be conserved
    const accountA = (await store.findOne(collection, { _id: 'account-A' })) as { balance: number };
    const accountB = (await store.findOne(collection, { _id: 'account-B' })) as { balance: number };

    expect(accountA.balance + accountB.balance).toBe(initialBalance * 2);
  });
});

// ============================================================================
// Cursor Iteration During Writes Tests
// ============================================================================

describe('Concurrency Stress Tests - Cursor Iteration During Writes', () => {
  let store: ConcurrentDocumentStore;

  beforeEach(() => {
    store = new ConcurrentDocumentStore();
  });

  it('should handle reads during active writes', async () => {
    const collection = 'cursor-writes';

    // Pre-populate
    for (let i = 0; i < 100; i++) {
      await store.insert(collection, { _id: `initial-${i}`, value: i });
    }

    // Start concurrent reads and writes
    const readResults: number[] = [];
    const writePromises: Promise<void>[] = [];

    // Multiple read snapshots
    for (let i = 0; i < 10; i++) {
      readResults.push((await store.find(collection)).length);

      // Insert some documents between reads
      writePromises.push(
        store.insert(collection, { _id: `new-${i}`, value: i }).then(() => {})
      );
    }

    await Promise.all(writePromises);

    // Read counts should be monotonically non-decreasing
    // (snapshot isolation would maintain consistent view)
    for (let i = 1; i < readResults.length; i++) {
      expect(readResults[i]).toBeGreaterThanOrEqual(readResults[i - 1]);
    }
  });

  it('should not miss documents during concurrent iteration', async () => {
    const collection = 'no-miss';

    // Writer adds documents
    const writerPromise = (async () => {
      for (let i = 0; i < 50; i++) {
        await store.insert(collection, { _id: `doc-${i}`, index: i });
        await new Promise((r) => setTimeout(r, 1));
      }
    })();

    // Reader iterates multiple times
    const allReadResults: Set<string>[] = [];
    const readerPromise = (async () => {
      for (let iteration = 0; iteration < 10; iteration++) {
        const docs = (await store.find(collection)) as Array<{ _id: string }>;
        allReadResults.push(new Set(docs.map((d) => d._id)));
        await new Promise((r) => setTimeout(r, 5));
      }
    })();

    await Promise.all([writerPromise, readerPromise]);

    // Final read should contain all documents
    const finalDocs = (await store.find(collection)) as Array<{ _id: string }>;
    expect(finalDocs).toHaveLength(50);

    // Documents seen in later reads should be superset of earlier reads
    for (let i = 1; i < allReadResults.length; i++) {
      const prev = allReadResults[i - 1];
      const curr = allReadResults[i];
      for (const id of prev) {
        // Any document seen earlier should still exist
        // (no phantom deletes in this test)
        expect(curr.has(id) || finalDocs.some((d) => d._id === id)).toBe(true);
      }
    }
  });

  it('should handle concurrent updates during read', async () => {
    const collection = 'update-during-read';

    // Pre-populate with documents
    for (let i = 0; i < 100; i++) {
      await store.insert(collection, { _id: `doc-${i}`, version: 0 });
    }

    // Concurrent updates
    const updatePromises = Array.from({ length: 100 }, (_, i) =>
      store.update(collection, { _id: `doc-${i}` }, { $inc: { version: 1 } })
    );

    // Concurrent reads
    const readPromises = Array.from({ length: 20 }, () => store.find(collection));

    await Promise.all([...updatePromises, ...readPromises]);

    // All documents should have version 1
    const finalDocs = (await store.find(collection)) as Array<{ version: number }>;
    expect(finalDocs.every((d) => d.version === 1)).toBe(true);
  });

  it('should handle deletes during iteration', async () => {
    const collection = 'delete-during-iter';

    // Pre-populate
    for (let i = 0; i < 100; i++) {
      await store.insert(collection, { _id: `doc-${i}`, keep: i < 50 });
    }

    // Concurrent deletes and reads
    const deletePromises = Array.from({ length: 50 }, (_, i) =>
      store.delete(collection, { _id: `doc-${i + 50}` })
    );

    const readSnapshots: number[] = [];
    const readPromises = Array.from({ length: 10 }, async () => {
      const docs = await store.find(collection);
      readSnapshots.push(docs.length);
      return docs;
    });

    await Promise.all([...deletePromises, ...readPromises]);

    // Final state should have exactly 50 documents
    const finalDocs = await store.find(collection);
    expect(finalDocs).toHaveLength(50);
  });
});

// ============================================================================
// MemoryStorage Concurrent Access Tests
// ============================================================================

describe('Concurrency Stress Tests - MemoryStorage', () => {
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
  });

  it('should handle concurrent puts to different keys', async () => {
    const keyCount = 100;
    const puts = Array.from({ length: keyCount }, (_, i) =>
      storage.put(`key-${i}`, new Uint8Array([i % 256]))
    );

    await Promise.all(puts);

    // Verify all keys exist
    const keys = await storage.list('');
    expect(keys).toHaveLength(keyCount);
  });

  it('should handle concurrent gets', async () => {
    // Pre-populate
    for (let i = 0; i < 50; i++) {
      await storage.put(`key-${i}`, new Uint8Array([i]));
    }

    // Concurrent gets
    const gets = Array.from({ length: 100 }, (_, i) => storage.get(`key-${i % 50}`));

    const results = await Promise.all(gets);

    expect(results.every((r) => r !== null)).toBe(true);
  });

  it('should handle concurrent put/get/delete operations', async () => {
    const operations: Promise<unknown>[] = [];

    // Puts
    for (let i = 0; i < 50; i++) {
      operations.push(storage.put(`key-${i}`, new Uint8Array([i])));
    }

    // Execute puts first
    await Promise.all(operations);
    operations.length = 0;

    // Mixed operations
    for (let i = 0; i < 50; i++) {
      operations.push(storage.get(`key-${i}`));
      if (i % 2 === 0) {
        operations.push(storage.delete(`key-${i}`));
      }
      operations.push(storage.put(`new-key-${i}`, new Uint8Array([i + 100])));
    }

    await Promise.all(operations);

    // Verify state
    const keys = await storage.list('');
    // 25 original (odd numbers) + 50 new = 75
    expect(keys.length).toBe(75);
  });

  it('should handle high-frequency updates to same key', async () => {
    const key = 'hot-key';
    const updateCount = 100;

    // Concurrent updates to same key
    const updates = Array.from({ length: updateCount }, (_, i) =>
      storage.put(key, new Uint8Array([i % 256]))
    );

    await Promise.all(updates);

    // Key should exist with some value
    const result = await storage.get(key);
    expect(result).not.toBeNull();
    expect(result!.length).toBe(1);
  });

  it('should handle multipart upload concurrency', async () => {
    const upload = await storage.createMultipartUpload('multipart-test');

    // Concurrent part uploads
    const partCount = 10;
    const partPromises = Array.from({ length: partCount }, (_, i) =>
      upload.uploadPart(i + 1, new Uint8Array(Array(100).fill(i)))
    );

    const parts = await Promise.all(partPromises);

    // Complete upload
    await upload.complete(parts);

    // Verify the assembled file
    const result = await storage.get('multipart-test');
    expect(result).not.toBeNull();
    expect(result!.length).toBe(partCount * 100);
  });
});
