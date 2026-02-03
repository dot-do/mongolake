/**
 * Concurrent Operations Load Test
 *
 * Tests multiple concurrent readers/writers and connection pool behavior under load.
 * Simulates real-world concurrent access patterns.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';

// ============================================================================
// Types
// ============================================================================

interface Document {
  _id: string;
  [key: string]: unknown;
}

interface WriteResult {
  acknowledged: boolean;
  insertedId: string;
  lsn: number;
}

interface ReadResult {
  documents: Document[];
  readLsn: number;
}

interface ConcurrencyMetrics {
  totalOperations: number;
  successfulOperations: number;
  failedOperations: number;
  averageLatencyMs: number;
  maxLatencyMs: number;
  minLatencyMs: number;
  operationsPerSecond: number;
}

interface ConnectionPoolStats {
  activeConnections: number;
  idleConnections: number;
  waitingRequests: number;
  totalConnectionsCreated: number;
}

// ============================================================================
// Mock Connection Pool
// ============================================================================

class MockConnectionPool {
  private maxConnections: number;
  private activeConnections = 0;
  private idleConnections: number;
  private waitingQueue: Array<{
    resolve: (conn: MockConnection) => void;
    reject: (err: Error) => void;
  }> = [];
  private totalCreated = 0;

  constructor(options: { maxConnections?: number; initialConnections?: number } = {}) {
    this.maxConnections = options.maxConnections ?? 10;
    this.idleConnections = options.initialConnections ?? 2;
    this.totalCreated = this.idleConnections;
  }

  async acquire(): Promise<MockConnection> {
    if (this.idleConnections > 0) {
      this.idleConnections--;
      this.activeConnections++;
      return new MockConnection(this);
    }

    if (this.activeConnections < this.maxConnections) {
      this.activeConnections++;
      this.totalCreated++;
      return new MockConnection(this);
    }

    // Wait for a connection to become available
    return new Promise((resolve, reject) => {
      this.waitingQueue.push({ resolve, reject });
    });
  }

  release(conn: MockConnection): void {
    this.activeConnections--;

    if (this.waitingQueue.length > 0) {
      const waiting = this.waitingQueue.shift()!;
      this.activeConnections++;
      waiting.resolve(conn);
    } else {
      this.idleConnections++;
    }
  }

  getStats(): ConnectionPoolStats {
    return {
      activeConnections: this.activeConnections,
      idleConnections: this.idleConnections,
      waitingRequests: this.waitingQueue.length,
      totalConnectionsCreated: this.totalCreated,
    };
  }

  async drain(): Promise<void> {
    // Reject all waiting requests
    while (this.waitingQueue.length > 0) {
      const waiting = this.waitingQueue.shift()!;
      waiting.reject(new Error('Pool drained'));
    }
  }
}

class MockConnection {
  private pool: MockConnectionPool;
  private inUse = false;

  constructor(pool: MockConnectionPool) {
    this.pool = pool;
  }

  async execute<T>(operation: () => Promise<T>): Promise<T> {
    if (this.inUse) {
      throw new Error('Connection already in use');
    }
    this.inUse = true;
    try {
      return await operation();
    } finally {
      this.inUse = false;
    }
  }

  release(): void {
    this.pool.release(this);
  }
}

// ============================================================================
// Mock Data Store
// ============================================================================

class MockDataStore {
  private data: Map<string, Document> = new Map();
  private lsn = 0;
  private readDelay: number;
  private writeDelay: number;
  private lock: Promise<void> = Promise.resolve();

  constructor(options: { readDelayMs?: number; writeDelayMs?: number } = {}) {
    this.readDelay = options.readDelayMs ?? 1;
    this.writeDelay = options.writeDelayMs ?? 2;
  }

  async write(doc: Document): Promise<WriteResult> {
    // Serialize writes
    const release = await this.acquireLock();
    try {
      await this.simulateDelay(this.writeDelay);
      this.lsn++;
      this.data.set(doc._id, { ...doc, _lsn: this.lsn });
      return {
        acknowledged: true,
        insertedId: doc._id,
        lsn: this.lsn,
      };
    } finally {
      release();
    }
  }

  async read(filter: Record<string, unknown> = {}): Promise<ReadResult> {
    await this.simulateDelay(this.readDelay);
    const currentLsn = this.lsn;

    let documents = Array.from(this.data.values());

    // Simple filtering
    if (Object.keys(filter).length > 0) {
      documents = documents.filter((doc) => {
        return Object.entries(filter).every(([key, value]) => doc[key] === value);
      });
    }

    return {
      documents,
      readLsn: currentLsn,
    };
  }

  async readOne(id: string): Promise<Document | null> {
    await this.simulateDelay(this.readDelay);
    return this.data.get(id) || null;
  }

  getCurrentLsn(): number {
    return this.lsn;
  }

  getDocumentCount(): number {
    return this.data.size;
  }

  clear(): void {
    this.data.clear();
    this.lsn = 0;
  }

  private async acquireLock(): Promise<() => void> {
    let release: () => void;
    const newLock = new Promise<void>((resolve) => {
      release = resolve;
    });
    const previousLock = this.lock;
    this.lock = newLock;
    await previousLock;
    return release!;
  }

  private simulateDelay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

// ============================================================================
// Test Utilities
// ============================================================================

function createTestDocument(index: number): Document {
  return {
    _id: `doc-${index.toString().padStart(6, '0')}`,
    value: index,
    name: `Document ${index}`,
    category: `cat-${index % 10}`,
    timestamp: Date.now(),
  };
}

async function measureOperation<T>(
  operation: () => Promise<T>
): Promise<{ result: T; latencyMs: number }> {
  const start = Date.now();
  const result = await operation();
  return { result, latencyMs: Date.now() - start };
}

function calculateMetrics(
  latencies: number[],
  startTime: number,
  endTime: number,
  failures: number
): ConcurrencyMetrics {
  const sorted = [...latencies].sort((a, b) => a - b);
  const total = latencies.length + failures;
  const durationSec = (endTime - startTime) / 1000;

  return {
    totalOperations: total,
    successfulOperations: latencies.length,
    failedOperations: failures,
    averageLatencyMs: latencies.length > 0
      ? latencies.reduce((a, b) => a + b, 0) / latencies.length
      : 0,
    maxLatencyMs: sorted[sorted.length - 1] ?? 0,
    minLatencyMs: sorted[0] ?? 0,
    operationsPerSecond: durationSec > 0 ? total / durationSec : 0,
  };
}

// ============================================================================
// Tests
// ============================================================================

describe('Concurrent Operations Load Tests', () => {
  let store: MockDataStore;
  let pool: MockConnectionPool;

  beforeEach(() => {
    store = new MockDataStore({ readDelayMs: 1, writeDelayMs: 2 });
    pool = new MockConnectionPool({ maxConnections: 10, initialConnections: 2 });
  });

  afterEach(async () => {
    await pool.drain();
    store.clear();
  });

  describe('Multiple Concurrent Readers', () => {
    it('should handle multiple concurrent read operations', async () => {
      // Pre-populate data
      for (let i = 0; i < 100; i++) {
        await store.write(createTestDocument(i));
      }

      const concurrentReaders = 20;
      const readsPerReader = 10;
      const latencies: number[] = [];

      const startTime = Date.now();

      const readers = Array.from({ length: concurrentReaders }, async (_, readerIndex) => {
        for (let i = 0; i < readsPerReader; i++) {
          const { latencyMs } = await measureOperation(() =>
            store.read({ category: `cat-${(readerIndex + i) % 10}` })
          );
          latencies.push(latencyMs);
        }
      });

      await Promise.all(readers);

      const endTime = Date.now();
      const metrics = calculateMetrics(latencies, startTime, endTime, 0);

      expect(metrics.successfulOperations).toBe(concurrentReaders * readsPerReader);
      expect(metrics.failedOperations).toBe(0);

      // Average latency should be reasonable
      expect(metrics.averageLatencyMs).toBeLessThan(50);

      console.log('Concurrent Readers Metrics:');
      console.log(`  Total: ${metrics.totalOperations} ops`);
      console.log(`  Throughput: ${metrics.operationsPerSecond.toFixed(1)} ops/sec`);
      console.log(`  Latency - Avg: ${metrics.averageLatencyMs.toFixed(2)}ms, Max: ${metrics.maxLatencyMs}ms`);
    });

    it('should maintain read consistency under concurrent access', async () => {
      // Pre-populate
      for (let i = 0; i < 50; i++) {
        await store.write(createTestDocument(i));
      }

      const concurrentReaders = 10;
      const results: ReadResult[] = [];

      const readers = Array.from({ length: concurrentReaders }, async () => {
        const result = await store.read({});
        results.push(result);
      });

      await Promise.all(readers);

      // All readers should see the same LSN (no writes during reads)
      const lsns = results.map((r) => r.readLsn);
      const uniqueLsns = new Set(lsns);
      expect(uniqueLsns.size).toBe(1);

      // All should see same document count
      const counts = results.map((r) => r.documents.length);
      expect(new Set(counts).size).toBe(1);
    });
  });

  describe('Multiple Concurrent Writers', () => {
    it('should handle multiple concurrent write operations', async () => {
      const concurrentWriters = 10;
      const writesPerWriter = 20;
      const latencies: number[] = [];
      let failures = 0;

      const startTime = Date.now();

      const writers = Array.from({ length: concurrentWriters }, async (_, writerIndex) => {
        for (let i = 0; i < writesPerWriter; i++) {
          try {
            const { latencyMs } = await measureOperation(() =>
              store.write(createTestDocument(writerIndex * writesPerWriter + i))
            );
            latencies.push(latencyMs);
          } catch {
            failures++;
          }
        }
      });

      await Promise.all(writers);

      const endTime = Date.now();
      const metrics = calculateMetrics(latencies, startTime, endTime, failures);

      expect(metrics.successfulOperations).toBe(concurrentWriters * writesPerWriter);
      expect(metrics.failedOperations).toBe(0);

      // All documents should be written
      expect(store.getDocumentCount()).toBe(concurrentWriters * writesPerWriter);

      console.log('Concurrent Writers Metrics:');
      console.log(`  Total: ${metrics.totalOperations} ops`);
      console.log(`  Throughput: ${metrics.operationsPerSecond.toFixed(1)} ops/sec`);
      console.log(`  Latency - Avg: ${metrics.averageLatencyMs.toFixed(2)}ms, Max: ${metrics.maxLatencyMs}ms`);
    });

    it('should assign unique LSNs to all concurrent writes', async () => {
      const concurrentWriters = 20;
      const results: WriteResult[] = [];

      const writers = Array.from({ length: concurrentWriters }, async (_, i) => {
        const result = await store.write(createTestDocument(i));
        results.push(result);
      });

      await Promise.all(writers);

      // All LSNs should be unique
      const lsns = results.map((r) => r.lsn);
      const uniqueLsns = new Set(lsns);
      expect(uniqueLsns.size).toBe(concurrentWriters);

      // LSNs should be sequential (1 to N)
      const sortedLsns = [...lsns].sort((a, b) => a - b);
      for (let i = 0; i < sortedLsns.length; i++) {
        expect(sortedLsns[i]).toBe(i + 1);
      }
    });
  });

  describe('Mixed Readers and Writers', () => {
    it('should handle concurrent reads and writes', async () => {
      const writerCount = 5;
      const readerCount = 10;
      const opsPerActor = 20;

      const writeLatencies: number[] = [];
      const readLatencies: number[] = [];
      let writeFailures = 0;
      let readFailures = 0;

      const startTime = Date.now();

      // Writers
      const writers = Array.from({ length: writerCount }, async (_, writerIndex) => {
        for (let i = 0; i < opsPerActor; i++) {
          try {
            const { latencyMs } = await measureOperation(() =>
              store.write(createTestDocument(writerIndex * 1000 + i))
            );
            writeLatencies.push(latencyMs);
          } catch {
            writeFailures++;
          }
          // Small delay to interleave with reads
          await new Promise((r) => setTimeout(r, 1));
        }
      });

      // Readers
      const readers = Array.from({ length: readerCount }, async () => {
        for (let i = 0; i < opsPerActor; i++) {
          try {
            const { latencyMs } = await measureOperation(() => store.read({}));
            readLatencies.push(latencyMs);
          } catch {
            readFailures++;
          }
          await new Promise((r) => setTimeout(r, 1));
        }
      });

      await Promise.all([...writers, ...readers]);

      const endTime = Date.now();

      const writeMetrics = calculateMetrics(writeLatencies, startTime, endTime, writeFailures);
      const readMetrics = calculateMetrics(readLatencies, startTime, endTime, readFailures);

      expect(writeMetrics.failedOperations).toBe(0);
      expect(readMetrics.failedOperations).toBe(0);

      console.log('Mixed Read/Write Metrics:');
      console.log(`  Writes: ${writeMetrics.totalOperations} ops, ${writeMetrics.operationsPerSecond.toFixed(1)} ops/sec`);
      console.log(`  Reads: ${readMetrics.totalOperations} ops, ${readMetrics.operationsPerSecond.toFixed(1)} ops/sec`);
    });

    it('should allow readers to see writes eventually', async () => {
      const writerCount = 3;
      const writesPerWriter = 10;
      const totalExpected = writerCount * writesPerWriter;

      // Start writers
      const writerPromises = Array.from({ length: writerCount }, async (_, writerIndex) => {
        for (let i = 0; i < writesPerWriter; i++) {
          await store.write(createTestDocument(writerIndex * writesPerWriter + i));
          await new Promise((r) => setTimeout(r, 5));
        }
      });

      // Reader that polls until all documents are visible
      let readAttempts = 0;
      const maxAttempts = 100;
      let finalCount = 0;

      const readerPromise = (async () => {
        while (readAttempts < maxAttempts) {
          const result = await store.read({});
          finalCount = result.documents.length;
          if (finalCount >= totalExpected) {
            break;
          }
          readAttempts++;
          await new Promise((r) => setTimeout(r, 10));
        }
      })();

      await Promise.all([...writerPromises, readerPromise]);

      // Reader should eventually see all documents
      expect(finalCount).toBe(totalExpected);
    });
  });

  describe('Connection Pool Behavior Under Load', () => {
    it('should efficiently reuse connections under load', async () => {
      const operationCount = 100;
      const poolStatsSamples: ConnectionPoolStats[] = [];

      // Capture pool stats during operations
      const sampleInterval = setInterval(() => {
        poolStatsSamples.push(pool.getStats());
      }, 10);

      const operations = Array.from({ length: operationCount }, async (_, i) => {
        const conn = await pool.acquire();
        try {
          await conn.execute(() => store.write(createTestDocument(i)));
        } finally {
          conn.release();
        }
      });

      await Promise.all(operations);

      clearInterval(sampleInterval);
      poolStatsSamples.push(pool.getStats());

      const finalStats = pool.getStats();

      // Pool should not have created excessive connections
      expect(finalStats.totalConnectionsCreated).toBeLessThanOrEqual(pool.getStats().activeConnections + 10);

      // No waiting requests at end
      expect(finalStats.waitingRequests).toBe(0);

      // Max concurrent connections used
      const maxActive = Math.max(...poolStatsSamples.map((s) => s.activeConnections));
      console.log(`Connection Pool - Max active: ${maxActive}, Total created: ${finalStats.totalConnectionsCreated}`);
    });

    it('should queue requests when pool is exhausted', async () => {
      // Small pool
      const smallPool = new MockConnectionPool({ maxConnections: 3, initialConnections: 1 });
      const concurrentOps = 10;
      let maxWaiting = 0;

      const operations = Array.from({ length: concurrentOps }, async (_, i) => {
        const stats = smallPool.getStats();
        maxWaiting = Math.max(maxWaiting, stats.waitingRequests);

        const conn = await smallPool.acquire();
        try {
          await conn.execute(async () => {
            await new Promise((r) => setTimeout(r, 20)); // Simulate work
            return store.write(createTestDocument(i));
          });
        } finally {
          conn.release();
        }
      });

      await Promise.all(operations);

      // Should have had some requests waiting
      expect(maxWaiting).toBeGreaterThan(0);

      // All operations should complete
      expect(store.getDocumentCount()).toBe(concurrentOps);

      await smallPool.drain();
    });

    it('should handle connection pool drain gracefully', async () => {
      const operationsStarted = { count: 0 };
      const operationsCompleted = { count: 0 };

      // Start some operations
      const operations = Array.from({ length: 20 }, async (_, i) => {
        operationsStarted.count++;
        try {
          const conn = await pool.acquire();
          try {
            await conn.execute(async () => {
              await new Promise((r) => setTimeout(r, 10));
              return store.write(createTestDocument(i));
            });
            operationsCompleted.count++;
          } finally {
            conn.release();
          }
        } catch (e) {
          // Expected for operations that were waiting when pool drained
        }
      });

      // Drain pool after a short delay
      await new Promise((r) => setTimeout(r, 50));
      await pool.drain();

      // Wait for all operations to settle
      await Promise.allSettled(operations);

      // Some operations should have completed
      expect(operationsCompleted.count).toBeGreaterThan(0);
    });
  });

  describe('Stress Test Scenarios', () => {
    it('should handle rapid successive operations', async () => {
      const rapidOps = 500;
      const results: WriteResult[] = [];

      const startTime = Date.now();

      // Fire off operations as fast as possible
      const promises = Array.from({ length: rapidOps }, (_, i) =>
        store.write(createTestDocument(i)).then((r) => results.push(r))
      );

      await Promise.all(promises);

      const duration = Date.now() - startTime;
      const opsPerSec = (rapidOps / duration) * 1000;

      expect(results.length).toBe(rapidOps);
      expect(store.getDocumentCount()).toBe(rapidOps);

      console.log(`Rapid operations: ${rapidOps} ops in ${duration}ms (${opsPerSec.toFixed(0)} ops/sec)`);
    });

    it('should maintain data integrity under high concurrency', async () => {
      const writerCount = 10;
      const writesPerWriter = 50;
      const allWriteResults: WriteResult[] = [];

      const writers = Array.from({ length: writerCount }, async (_, writerIndex) => {
        const results: WriteResult[] = [];
        for (let i = 0; i < writesPerWriter; i++) {
          const doc = {
            _id: `writer-${writerIndex}-doc-${i}`,
            writerIndex,
            docIndex: i,
            value: writerIndex * 1000 + i,
          };
          const result = await store.write(doc);
          results.push(result);
        }
        return results;
      });

      const writerResults = await Promise.all(writers);
      writerResults.forEach((results) => allWriteResults.push(...results));

      // Verify all writes acknowledged
      expect(allWriteResults.length).toBe(writerCount * writesPerWriter);
      expect(allWriteResults.every((r) => r.acknowledged)).toBe(true);

      // Verify all documents stored
      expect(store.getDocumentCount()).toBe(writerCount * writesPerWriter);

      // Verify LSN uniqueness
      const lsns = allWriteResults.map((r) => r.lsn);
      expect(new Set(lsns).size).toBe(allWriteResults.length);

      // Verify data integrity by reading back
      const { documents } = await store.read({});
      expect(documents.length).toBe(writerCount * writesPerWriter);

      // Each writer's documents should be present
      for (let w = 0; w < writerCount; w++) {
        const writerDocs = documents.filter((d) => d.writerIndex === w);
        expect(writerDocs.length).toBe(writesPerWriter);
      }
    });

    it('should recover from temporary slowdowns', async () => {
      // Simulate a store with variable latency
      const variableStore = new MockDataStore({ writeDelayMs: 5 });

      const batches = 5;
      const opsPerBatch = 20;
      const batchMetrics: { duration: number; opsPerSec: number }[] = [];

      for (let batch = 0; batch < batches; batch++) {
        const startTime = Date.now();

        const ops = Array.from({ length: opsPerBatch }, (_, i) =>
          variableStore.write(createTestDocument(batch * opsPerBatch + i))
        );
        await Promise.all(ops);

        const duration = Date.now() - startTime;
        batchMetrics.push({
          duration,
          opsPerSec: (opsPerBatch / duration) * 1000,
        });
      }

      // Later batches should not be significantly slower than earlier ones
      const firstBatchOps = batchMetrics[0].opsPerSec;
      const lastBatchOps = batchMetrics[batches - 1].opsPerSec;

      expect(lastBatchOps).toBeGreaterThan(firstBatchOps * 0.5);

      console.log('Batch throughput over time:');
      batchMetrics.forEach((m, i) => {
        console.log(`  Batch ${i + 1}: ${m.opsPerSec.toFixed(1)} ops/sec`);
      });
    });
  });
});
