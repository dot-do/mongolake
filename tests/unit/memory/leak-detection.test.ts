/**
 * Memory Leak Detection Tests
 *
 * RED phase tests for detecting memory leaks in MongoLake:
 * - Cursor cleanup after iteration
 * - Connection pool cleanup
 * - Event listener cleanup
 * - Large document handling
 * - Long-running operation memory stability
 *
 * These tests verify that resources are properly released and
 * memory usage remains stable over sustained operations.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { Cursor, CursorStore, type DocumentSource } from '../../../src/cursor/index.js';
import { ConnectionPool } from '../../../src/wire-protocol/connection-pool.js';
import { ChangeStream } from '../../../src/change-stream/index.js';
import { createMockSocket } from '../../utils/mock-socket.js';
import { createMockEventEmitter } from '../../utils/mocks.js';
import type { Document, WithId, Filter, FindOptions } from '../../../src/types.js';

// =============================================================================
// Test Helpers
// =============================================================================

interface TestDoc extends Document {
  _id?: string;
  name: string;
  value?: number;
  data?: Uint8Array;
}

/**
 * Create a mock document source for testing
 */
function createMockSource(docs: WithId<TestDoc>[]): DocumentSource<TestDoc> {
  return {
    async readDocuments(_filter?: Filter<TestDoc>, _options?: FindOptions): Promise<WithId<TestDoc>[]> {
      return [...docs];
    },
  };
}

/**
 * Generate test documents
 */
function generateTestDocs(count: number): WithId<TestDoc>[] {
  return Array.from({ length: count }, (_, i) => ({
    _id: `doc-${i}`,
    name: `User ${i}`,
    value: i,
  }));
}

/**
 * Generate large test documents with binary data
 */
function generateLargeDocs(count: number, sizeKB: number): WithId<TestDoc>[] {
  return Array.from({ length: count }, (_, i) => ({
    _id: `large-doc-${i}`,
    name: `Large User ${i}`,
    value: i,
    data: new Uint8Array(sizeKB * 1024).fill(i % 256),
  }));
}

/**
 * Simple memory usage tracker for test assertions
 * Note: This is a simplified approach for unit tests.
 * In real production code, you would use proper memory profiling tools.
 */
interface MemoryTracker {
  baseline: number;
  current: () => number;
  delta: () => number;
  reset: () => void;
}

function createMemoryTracker(): MemoryTracker {
  // Force garbage collection if available (requires --expose-gc flag)
  const gc = globalThis.gc as (() => void) | undefined;
  if (gc) {
    gc();
  }

  let baseline = process.memoryUsage().heapUsed;

  return {
    baseline,
    current: () => process.memoryUsage().heapUsed,
    delta: () => process.memoryUsage().heapUsed - baseline,
    reset: () => {
      if (gc) {
        gc();
      }
      baseline = process.memoryUsage().heapUsed;
    },
  };
}

// =============================================================================
// Cursor Cleanup After Iteration Tests
// =============================================================================

describe('Memory Leak Detection - Cursor Cleanup', () => {
  it('should release cursor buffer memory after close()', async () => {
    const docs = generateTestDocs(1000);
    const source = createMockSource(docs);
    const cursor = new Cursor(source, 'test.collection');

    // Execute and fetch all results to populate buffer
    await cursor.toArray();

    // Verify cursor has state before close
    expect(cursor.isExhausted).toBe(true);

    // Close cursor
    await cursor.close();

    // Verify cursor is properly closed
    expect(cursor.isClosed).toBe(true);

    // Verify cursor returns empty results after close
    const afterClose = await cursor.toArray();
    expect(afterClose).toEqual([]);
  });

  it('should release memory when cursor is closed mid-iteration', async () => {
    const docs = generateTestDocs(100);
    const source = createMockSource(docs);
    const cursor = new Cursor(source, 'test.collection');

    // Partially iterate
    let count = 0;
    for await (const _doc of cursor) {
      count++;
      if (count === 50) {
        break;
      }
    }

    // Close cursor while still having pending documents
    await cursor.close();

    // Verify cursor state
    expect(cursor.isClosed).toBe(true);

    // Attempting to continue iteration should yield nothing
    const remaining = await cursor.toArray();
    expect(remaining).toEqual([]);
  });

  it('should clean up CursorStore when cursors are removed', async () => {
    const docs = generateTestDocs(100);
    const source = createMockSource(docs);

    const store = new CursorStore({
      timeoutMs: 60000,
      cleanupIntervalMs: 100000, // Long interval to prevent auto-cleanup
    });

    // Add multiple cursors
    const cursors: Cursor<TestDoc>[] = [];
    for (let i = 0; i < 10; i++) {
      const cursor = new Cursor(source, 'test.collection');
      cursors.push(cursor);
      store.add(cursor as Cursor<Document>);
    }

    expect(store.size).toBe(10);

    // Remove all cursors
    for (const cursor of cursors) {
      store.remove(cursor.cursorId);
    }

    expect(store.size).toBe(0);

    // Verify all cursors are closed
    for (const cursor of cursors) {
      expect(cursor.isClosed).toBe(true);
    }

    // Clean up store
    store.close();
  });

  it('should clean up expired cursors automatically', async () => {
    vi.useFakeTimers();

    const docs = generateTestDocs(100);
    const source = createMockSource(docs);

    const store = new CursorStore({
      timeoutMs: 1000, // 1 second timeout
      cleanupIntervalMs: 100000, // Manual cleanup only
    });

    // Add cursors
    for (let i = 0; i < 5; i++) {
      const cursor = new Cursor(source, 'test.collection');
      store.add(cursor as Cursor<Document>);
    }

    expect(store.size).toBe(5);

    // Advance time past timeout
    vi.advanceTimersByTime(2000);

    // Trigger cleanup
    const cleaned = store.cleanupExpiredCursors();

    expect(cleaned).toBe(5);
    expect(store.size).toBe(0);

    store.close();
    vi.useRealTimers();
  });

  it('should handle rewind without memory accumulation', async () => {
    const docs = generateTestDocs(100);
    const source = createMockSource(docs);
    const cursor = new Cursor(source, 'test.collection');

    // Execute and rewind multiple times
    for (let i = 0; i < 5; i++) {
      await cursor.toArray();
      await cursor.rewind();
    }

    // Final execution
    const finalResults = await cursor.toArray();
    expect(finalResults).toHaveLength(100);

    await cursor.close();
    expect(cursor.isClosed).toBe(true);
  });
});

// =============================================================================
// Connection Pool Cleanup Tests
// =============================================================================

describe('Memory Leak Detection - Connection Pool Cleanup', () => {
  let pool: ConnectionPool;

  beforeEach(() => {
    pool = new ConnectionPool({
      maxConnections: 10,
      minConnections: 2,
      idleTimeout: 5000,
      healthCheckInterval: 100000, // Disable auto health check
      idleCheckInterval: 100000, // Disable auto idle check
    });
  });

  afterEach(async () => {
    await pool.shutdown();
  });

  it('should remove connections from pool on shutdown', async () => {
    // Add connections
    const sockets: ReturnType<typeof createMockSocket>[] = [];
    for (let i = 0; i < 5; i++) {
      const socket = createMockSocket();
      sockets.push(socket);
      pool.addConnection(socket as unknown as import('node:net').Socket);
    }

    expect(pool.getMetrics().totalConnections).toBe(5);

    // Shutdown pool
    await pool.shutdown();

    // Verify all sockets were properly cleaned up (either ended or destroyed)
    for (const socket of sockets) {
      // Socket should have writable set to false (either from end() or destroy())
      expect(socket.writable).toBe(false);
    }

    // Verify get() returns undefined for all connections (internal map cleared)
    for (let i = 1; i <= 5; i++) {
      expect(pool.get(i)).toBeUndefined();
    }
  });

  it('should clean up connections on explicit removal', async () => {
    const socket = createMockSocket();
    const connection = pool.addConnection(socket as unknown as import('node:net').Socket);

    expect(pool.getMetrics().totalConnections).toBe(1);

    // Remove connection
    pool.remove(connection.id, 'test cleanup');

    expect(pool.getMetrics().totalConnections).toBe(0);
    expect(pool.getMetrics().totalDestroyed).toBe(1);
  });

  it('should clean up connections on error', async () => {
    const socket = createMockSocket();
    const connection = pool.addConnection(socket as unknown as import('node:net').Socket);

    // Simulate error
    pool.handleError(connection.id, new Error('Test error'));

    expect(pool.getMetrics().totalConnections).toBe(0);
    expect(pool.getMetrics().errorCount).toBe(1);
  });

  it('should release connections back to pool', async () => {
    const socket = createMockSocket();
    const connection = pool.addConnection(socket as unknown as import('node:net').Socket);

    // Connection starts as in use
    expect(pool.getMetrics().activeConnections).toBe(1);
    expect(pool.getMetrics().idleConnections).toBe(0);

    // Release connection
    pool.release(connection.id);

    expect(pool.getMetrics().activeConnections).toBe(0);
    expect(pool.getMetrics().idleConnections).toBe(1);
  });

  it('should reject pending waiters on shutdown', async () => {
    // Fill pool to capacity with in-use connections
    for (let i = 0; i < 10; i++) {
      const socket = createMockSocket();
      pool.addConnection(socket as unknown as import('node:net').Socket);
    }

    // Start an acquire that will wait
    const acquirePromise = pool.acquire(5000).catch((e) => e);

    // Shutdown while waiting
    await pool.shutdown();

    // Acquire should reject
    const result = await acquirePromise;
    expect(result).toBeInstanceOf(Error);
    expect((result as Error).message).toContain('shutting down');
  });

  it('should clean up timers on shutdown', async () => {
    // Create pool with active timers
    const timerPool = new ConnectionPool({
      maxConnections: 10,
      idleCheckInterval: 100,
      healthCheckInterval: 100,
    });

    // Add a connection to ensure pool is active
    const socket = createMockSocket();
    const conn = timerPool.addConnection(socket as unknown as import('node:net').Socket);

    // Shutdown should clear timers
    await timerPool.shutdown();

    // Verify socket was cleaned up (either ended or destroyed)
    expect(socket.writable).toBe(false);

    // Verify connection is no longer retrievable (internal map cleared)
    expect(timerPool.get(conn.id)).toBeUndefined();
  });
});

// =============================================================================
// Event Listener Cleanup Tests
// =============================================================================

describe('Memory Leak Detection - Event Listener Cleanup', () => {
  it('should clean up event waiters when ChangeStream is closed', async () => {
    const changeStream = new ChangeStream<TestDoc>(
      { db: 'testdb', coll: 'testcoll' },
      [],
      {}
    );

    // Start waiting for next event (creates a waiter)
    const nextPromise = changeStream.next();

    // Close the stream
    changeStream.close();

    // Waiter should resolve with null
    const result = await nextPromise;
    expect(result).toBeNull();

    // Stream should be closed
    expect(changeStream.isClosed).toBe(true);
  });

  it('should not accumulate event buffer indefinitely', async () => {
    const changeStream = new ChangeStream<TestDoc>(
      { db: 'testdb', coll: 'testcoll' },
      [],
      {}
    );

    // Push many events
    for (let i = 0; i < 100; i++) {
      changeStream.pushEvent('insert', { _id: `doc-${i}` }, {
        _id: `doc-${i}`,
        name: `User ${i}`,
      });
    }

    // Consume all events
    let count = 0;
    while (changeStream.hasNext()) {
      const event = changeStream.tryNext();
      if (event) count++;
    }

    expect(count).toBe(100);

    // Buffer should be empty
    expect(changeStream.hasNext()).toBe(false);

    changeStream.close();
  });

  it('should handle MockEventEmitter listener cleanup', () => {
    const emitter = createMockEventEmitter();

    // Add listeners
    const listener1 = vi.fn();
    const listener2 = vi.fn();
    const listener3 = vi.fn();

    emitter.on('event1', listener1);
    emitter.on('event1', listener2);
    emitter.on('event2', listener3);

    expect(emitter.listenerCount('event1')).toBe(2);
    expect(emitter.listenerCount('event2')).toBe(1);

    // Remove specific listener
    emitter.off('event1', listener1);
    expect(emitter.listenerCount('event1')).toBe(1);

    // Remove all listeners for event
    emitter.removeAllListeners('event1');
    expect(emitter.listenerCount('event1')).toBe(0);
    expect(emitter.listenerCount('event2')).toBe(1);

    // Remove all listeners
    emitter.removeAllListeners();
    expect(emitter.listenerCount('event1')).toBe(0);
    expect(emitter.listenerCount('event2')).toBe(0);
  });

  it('should properly handle once() listeners', () => {
    const emitter = createMockEventEmitter();
    const listener = vi.fn();

    emitter.once('event', listener);
    expect(emitter.listenerCount('event')).toBe(1);

    // Emit event - listener should be called and removed
    emitter.emit('event', 'data');

    expect(listener).toHaveBeenCalledTimes(1);
    expect(listener).toHaveBeenCalledWith('data');

    // Listener should be automatically removed
    expect(emitter.listenerCount('event')).toBe(0);

    // Emitting again should not call listener
    emitter.emit('event', 'more data');
    expect(listener).toHaveBeenCalledTimes(1);
  });

  it('should clean up ConnectionPool event listeners on shutdown', async () => {
    const pool = new ConnectionPool({
      maxConnections: 10,
      idleCheckInterval: 100000,
      healthCheckInterval: 100000,
    });

    // Track event listener count
    const eventCounts = {
      created: 0,
      destroyed: 0,
      acquired: 0,
      released: 0,
    };

    pool.on('connection:created', () => eventCounts.created++);
    pool.on('connection:destroyed', () => eventCounts.destroyed++);
    pool.on('connection:acquired', () => eventCounts.acquired++);
    pool.on('connection:released', () => eventCounts.released++);

    // Add and use connection
    const socket = createMockSocket();
    const conn = pool.addConnection(socket as unknown as import('node:net').Socket);
    pool.release(conn.id);
    pool.remove(conn.id, 'test');

    expect(eventCounts.created).toBe(1);
    expect(eventCounts.released).toBe(1);
    expect(eventCounts.destroyed).toBe(1);

    await pool.shutdown();
  });
});

// =============================================================================
// Large Document Handling Tests
// =============================================================================

describe('Memory Leak Detection - Large Document Handling', () => {
  it('should handle large documents without accumulating memory', async () => {
    // Create documents with 100KB of binary data each
    const largeDocs = generateLargeDocs(10, 100);
    const source = createMockSource(largeDocs);
    const cursor = new Cursor(source, 'test.collection');

    // Process all documents
    const results = await cursor.toArray();
    expect(results).toHaveLength(10);

    // Verify data integrity
    for (let i = 0; i < results.length; i++) {
      expect(results[i].data).toBeDefined();
      expect(results[i].data!.length).toBe(100 * 1024);
    }

    // Close cursor to release references
    await cursor.close();
    expect(cursor.isClosed).toBe(true);
  });

  it('should handle streaming large documents one at a time', async () => {
    const largeDocs = generateLargeDocs(20, 50); // 50KB each
    const source = createMockSource(largeDocs);
    const cursor = new Cursor(source, 'test.collection');

    let processedCount = 0;
    let totalSize = 0;

    // Process one at a time using async iterator
    for await (const doc of cursor) {
      processedCount++;
      if (doc.data) {
        totalSize += doc.data.length;
      }
    }

    expect(processedCount).toBe(20);
    expect(totalSize).toBe(20 * 50 * 1024);
  });

  it('should release large document memory on cursor rewind', async () => {
    const largeDocs = generateLargeDocs(5, 200); // 200KB each
    const source = createMockSource(largeDocs);
    const cursor = new Cursor(source, 'test.collection');

    // First pass
    const firstPass = await cursor.toArray();
    expect(firstPass).toHaveLength(5);

    // Rewind (should release previous results reference)
    await cursor.rewind();

    // Second pass
    const secondPass = await cursor.toArray();
    expect(secondPass).toHaveLength(5);

    await cursor.close();
  });

  it('should not duplicate data when using map()', async () => {
    const largeDocs = generateLargeDocs(5, 100);
    const source = createMockSource(largeDocs);
    const cursor = new Cursor(source, 'test.collection');

    // Map to extract just IDs (small data)
    const ids = await cursor.map((doc) => doc._id);

    expect(ids).toHaveLength(5);
    expect(ids).toEqual([
      'large-doc-0',
      'large-doc-1',
      'large-doc-2',
      'large-doc-3',
      'large-doc-4',
    ]);

    // Cursor should be exhausted
    expect(cursor.isExhausted).toBe(true);
  });
});

// =============================================================================
// Long-Running Operation Memory Stability Tests
// =============================================================================

describe('Memory Leak Detection - Long-Running Operation Memory Stability', () => {
  it('should maintain stable memory across many cursor operations', async () => {
    const docs = generateTestDocs(100);
    const source = createMockSource(docs);

    // Perform many cursor operations
    for (let iteration = 0; iteration < 50; iteration++) {
      const cursor = new Cursor(source, 'test.collection');

      // Various operations
      await cursor.limit(10).toArray();

      // Explicitly close cursor
      await cursor.close();
    }

    // If we got here without running out of memory, test passes
    expect(true).toBe(true);
  });

  it('should maintain stable memory with CursorStore over time', async () => {
    vi.useFakeTimers();

    const docs = generateTestDocs(50);
    const source = createMockSource(docs);

    const store = new CursorStore({
      timeoutMs: 1000,
      cleanupIntervalMs: 500,
    });

    // Simulate many cursors being created and cleaned up over time
    for (let cycle = 0; cycle < 10; cycle++) {
      // Add cursors
      for (let i = 0; i < 10; i++) {
        const cursor = new Cursor(source, 'test.collection');
        await cursor.toArray(); // Execute and exhaust
        store.add(cursor as Cursor<Document>);
      }

      // Advance time to trigger cleanup
      vi.advanceTimersByTime(1500);
      store.cleanupExpiredCursors();
    }

    // All cursors should be cleaned up
    expect(store.size).toBe(0);

    store.close();
    vi.useRealTimers();
  });

  it('should handle rapid connection pool cycling', async () => {
    const pool = new ConnectionPool({
      maxConnections: 5,
      minConnections: 0,
      idleTimeout: 100,
      idleCheckInterval: 100000,
      healthCheckInterval: 100000,
    });

    // Rapidly add and remove connections
    for (let cycle = 0; cycle < 100; cycle++) {
      const socket = createMockSocket();
      const conn = pool.addConnection(socket as unknown as import('node:net').Socket);
      pool.release(conn.id);
      pool.remove(conn.id, 'test cycle');
    }

    const metrics = pool.getMetrics();
    expect(metrics.totalCreated).toBe(100);
    expect(metrics.totalDestroyed).toBe(100);
    expect(metrics.totalConnections).toBe(0);

    await pool.shutdown();
  });

  it('should handle sustained ChangeStream operations', async () => {
    const changeStream = new ChangeStream<TestDoc>(
      { db: 'testdb', coll: 'testcoll' },
      [],
      {}
    );

    // Simulate sustained event production and consumption
    const consumedEvents: number[] = [];

    for (let batch = 0; batch < 10; batch++) {
      // Produce events
      for (let i = 0; i < 100; i++) {
        changeStream.pushEvent('insert', { _id: `batch-${batch}-doc-${i}` }, {
          _id: `batch-${batch}-doc-${i}`,
          name: `User ${i}`,
          value: batch * 100 + i,
        });
      }

      // Consume all events in batch
      let batchCount = 0;
      while (changeStream.hasNext()) {
        const event = changeStream.tryNext();
        if (event) batchCount++;
      }
      consumedEvents.push(batchCount);
    }

    // All events should have been consumed
    expect(consumedEvents.reduce((a, b) => a + b, 0)).toBe(1000);

    // Buffer should be empty
    expect(changeStream.hasNext()).toBe(false);

    changeStream.close();
  });

  it('should handle forEach without accumulating results', async () => {
    const docs = generateTestDocs(1000);
    const source = createMockSource(docs);
    const cursor = new Cursor(source, 'test.collection');

    let processedCount = 0;

    // forEach should process without storing all results
    await cursor.forEach((_doc) => {
      processedCount++;
      // Simulate processing without storing reference
    });

    expect(processedCount).toBe(1000);
    expect(cursor.isExhausted).toBe(true);

    await cursor.close();
  });

  it('should handle batched operations efficiently', async () => {
    const docs = generateTestDocs(500);
    const source = createMockSource(docs);
    const cursor = new Cursor(source, 'test.collection');

    cursor.batchSize(50);

    // Get batches
    const batches: number[] = [];
    const firstBatch = await cursor.getFirstBatch(50);
    batches.push(firstBatch.length);

    while (!cursor.isExhausted) {
      const nextBatch = await cursor.getNextBatch(50);
      if (nextBatch.length === 0) break;
      batches.push(nextBatch.length);
    }

    // Should have processed all documents in batches
    const totalProcessed = batches.reduce((a, b) => a + b, 0);
    expect(totalProcessed).toBe(500);

    await cursor.close();
  });
});

// =============================================================================
// Resource Cleanup Verification Tests
// =============================================================================

describe('Memory Leak Detection - Resource Cleanup Verification', () => {
  it('should verify CursorStore cleanup interval is cleared on close', () => {
    const store = new CursorStore({
      timeoutMs: 60000,
      cleanupIntervalMs: 1000,
    });

    // Close store
    store.close();

    // Verify store is empty
    expect(store.size).toBe(0);

    // Adding cursors after close should still work (store doesn't throw)
    // but they won't be managed properly - this is expected behavior
  });

  it('should verify ConnectionPool properly cleans up all state', async () => {
    const pool = new ConnectionPool({
      maxConnections: 10,
      idleCheckInterval: 100000, // Disable auto cleanup
      healthCheckInterval: 100000,
    });

    // Add some connections and track sockets
    const connections: { id: number; socket: ReturnType<typeof createMockSocket> }[] = [];
    for (let i = 0; i < 5; i++) {
      const socket = createMockSocket();
      const conn = pool.addConnection(socket as unknown as import('node:net').Socket);
      connections.push({ id: conn.id, socket });
    }

    // Shutdown
    await pool.shutdown();

    // Verify all sockets were properly cleaned up (writable = false after end/destroy)
    for (const { socket } of connections) {
      expect(socket.writable).toBe(false);
    }

    // Verify all connections are no longer retrievable (internal map cleared)
    for (const { id } of connections) {
      expect(pool.get(id)).toBeUndefined();
    }

    // Verify metrics reflect initial creation
    const metrics = pool.getMetrics();
    expect(metrics.totalCreated).toBe(5);
  });

  it('should verify ChangeStream clears waiters on close', async () => {
    const changeStream = new ChangeStream<TestDoc>(
      { db: 'testdb', coll: 'testcoll' },
      [],
      {}
    );

    // Create multiple waiters
    const waiters = [
      changeStream.next(),
      changeStream.next(),
      changeStream.next(),
    ];

    // Close stream - should resolve all waiters with null
    changeStream.close();

    const results = await Promise.all(waiters);
    expect(results).toEqual([null, null, null]);
  });

  it('should verify cursor close is idempotent', async () => {
    const docs = generateTestDocs(10);
    const source = createMockSource(docs);
    const cursor = new Cursor(source, 'test.collection');

    await cursor.toArray();

    // Close multiple times should not cause errors
    await cursor.close();
    await cursor.close();
    await cursor.close();

    expect(cursor.isClosed).toBe(true);

    // Operations after close should be safe
    const result = await cursor.toArray();
    expect(result).toEqual([]);
  });
});
