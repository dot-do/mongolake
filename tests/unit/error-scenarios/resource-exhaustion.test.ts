/**
 * Resource Exhaustion Error Scenario Tests
 *
 * Comprehensive tests for resource exhaustion error handling:
 * - Memory limits
 * - Connection limits
 * - Storage quota limits
 * - Concurrent operation limits
 * - Buffer overflow scenarios
 * - Lock contention
 *
 * These tests verify that resource exhaustion conditions are properly
 * detected and reported with informative error messages.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  MemoryStorage,
  Semaphore,
  ManagedMultipartUpload,
  createBufferedMultipartUpload,
  DEFAULT_MULTIPART_TIMEOUT_MS,
} from '../../../src/storage/index.js';
import { LockManager } from '../../../src/transaction/participant.js';
import type { BufferedOperation } from '../../../src/session/index.js';

// ============================================================================
// Memory Limit Simulation Tests
// ============================================================================

describe('Resource Exhaustion - Memory Limits', () => {
  /**
   * Simulates a memory-limited storage backend
   */
  class MemoryLimitedStorage extends MemoryStorage {
    private memoryUsed = 0;
    private readonly maxMemory: number;

    constructor(maxMemoryBytes: number) {
      super();
      this.maxMemory = maxMemoryBytes;
    }

    async put(key: string, data: Uint8Array): Promise<void> {
      const newUsage = this.memoryUsed + data.length;
      if (newUsage > this.maxMemory) {
        throw new MemoryLimitError(
          `Memory limit exceeded: ${newUsage} bytes requested, ${this.maxMemory} bytes available`,
          { requested: newUsage, available: this.maxMemory }
        );
      }
      this.memoryUsed += data.length;
      return super.put(key, data);
    }

    async delete(key: string): Promise<void> {
      const data = await this.get(key);
      if (data) {
        this.memoryUsed -= data.length;
      }
      return super.delete(key);
    }

    getMemoryUsed(): number {
      return this.memoryUsed;
    }
  }

  class MemoryLimitError extends Error {
    constructor(
      message: string,
      public readonly details: { requested: number; available: number }
    ) {
      super(message);
      this.name = 'MemoryLimitError';
    }
  }

  it('should throw MemoryLimitError when storage limit exceeded', async () => {
    const storage = new MemoryLimitedStorage(1000);

    await storage.put('key1', new Uint8Array(500));
    await storage.put('key2', new Uint8Array(400));

    // This should exceed the limit
    await expect(storage.put('key3', new Uint8Array(200))).rejects.toThrow(MemoryLimitError);
  });

  it('should include memory usage details in error', async () => {
    const storage = new MemoryLimitedStorage(1000);

    await storage.put('key1', new Uint8Array(900));

    try {
      await storage.put('key2', new Uint8Array(200));
    } catch (error) {
      expect(error).toBeInstanceOf(MemoryLimitError);
      const memError = error as MemoryLimitError;
      expect(memError.details.requested).toBe(1100);
      expect(memError.details.available).toBe(1000);
    }
  });

  it('should recover after freeing memory', async () => {
    const storage = new MemoryLimitedStorage(1000);

    await storage.put('key1', new Uint8Array(800));

    // Should fail
    await expect(storage.put('key2', new Uint8Array(300))).rejects.toThrow(MemoryLimitError);

    // Free some memory
    await storage.delete('key1');

    // Should now succeed
    await expect(storage.put('key2', new Uint8Array(300))).resolves.not.toThrow();
  });

  it('should track memory usage correctly', async () => {
    const storage = new MemoryLimitedStorage(10000);

    await storage.put('key1', new Uint8Array(1000));
    expect(storage.getMemoryUsed()).toBe(1000);

    await storage.put('key2', new Uint8Array(2000));
    expect(storage.getMemoryUsed()).toBe(3000);

    await storage.delete('key1');
    expect(storage.getMemoryUsed()).toBe(2000);
  });
});

// ============================================================================
// Connection/Concurrency Limit Tests
// ============================================================================

describe('Resource Exhaustion - Connection Limits', () => {
  /**
   * Simulates a connection pool with limited connections
   */
  class ConnectionPool {
    private availableConnections: number;
    private readonly maxConnections: number;
    private waitingRequests: Array<() => void> = [];

    constructor(maxConnections: number) {
      this.maxConnections = maxConnections;
      this.availableConnections = maxConnections;
    }

    async acquire(): Promise<Connection> {
      if (this.availableConnections > 0) {
        this.availableConnections--;
        return new Connection(this);
      }

      // Wait for a connection to become available
      return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
          const idx = this.waitingRequests.indexOf(releaseHandler);
          if (idx !== -1) {
            this.waitingRequests.splice(idx, 1);
          }
          reject(new ConnectionPoolExhaustedError(
            `Connection pool exhausted: all ${this.maxConnections} connections in use`,
            { maxConnections: this.maxConnections, waiting: this.waitingRequests.length }
          ));
        }, 100);

        const releaseHandler = () => {
          clearTimeout(timeout);
          this.availableConnections--;
          resolve(new Connection(this));
        };

        this.waitingRequests.push(releaseHandler);
      });
    }

    release(): void {
      const waiting = this.waitingRequests.shift();
      if (waiting) {
        waiting();
      } else {
        this.availableConnections++;
      }
    }

    getStats(): { available: number; max: number; waiting: number } {
      return {
        available: this.availableConnections,
        max: this.maxConnections,
        waiting: this.waitingRequests.length,
      };
    }
  }

  class Connection {
    constructor(private pool: ConnectionPool) {}

    release(): void {
      this.pool.release();
    }
  }

  class ConnectionPoolExhaustedError extends Error {
    constructor(
      message: string,
      public readonly details: { maxConnections: number; waiting: number }
    ) {
      super(message);
      this.name = 'ConnectionPoolExhaustedError';
    }
  }

  it('should throw error when connection pool exhausted', async () => {
    const pool = new ConnectionPool(2);

    const conn1 = await pool.acquire();
    const conn2 = await pool.acquire();

    // Third should fail
    await expect(pool.acquire()).rejects.toThrow(ConnectionPoolExhaustedError);

    conn1.release();
    conn2.release();
  });

  it('should include pool details in error', async () => {
    const pool = new ConnectionPool(3);

    await pool.acquire();
    await pool.acquire();
    await pool.acquire();

    try {
      await pool.acquire();
    } catch (error) {
      expect(error).toBeInstanceOf(ConnectionPoolExhaustedError);
      const poolError = error as ConnectionPoolExhaustedError;
      expect(poolError.details.maxConnections).toBe(3);
    }
  });

  it('should recover when connection released', async () => {
    const pool = new ConnectionPool(2);

    const conn1 = await pool.acquire();
    const conn2 = await pool.acquire();

    // Start waiting for connection
    const conn3Promise = pool.acquire();

    // Release one
    conn1.release();

    // Should now succeed
    const conn3 = await conn3Promise;
    expect(conn3).toBeDefined();

    conn2.release();
    conn3.release();
  });

  it('should track pool statistics', async () => {
    const pool = new ConnectionPool(5);

    expect(pool.getStats().available).toBe(5);

    const conn1 = await pool.acquire();
    expect(pool.getStats().available).toBe(4);

    const conn2 = await pool.acquire();
    expect(pool.getStats().available).toBe(3);

    conn1.release();
    expect(pool.getStats().available).toBe(4);

    conn2.release();
    expect(pool.getStats().available).toBe(5);
  });
});

// ============================================================================
// Semaphore Concurrency Tests
// ============================================================================

describe('Resource Exhaustion - Semaphore Limits', () => {
  it('should throw error for invalid permit count', () => {
    expect(() => new Semaphore(0)).toThrow('Semaphore permits must be at least 1');
    expect(() => new Semaphore(-1)).toThrow('Semaphore permits must be at least 1');
  });

  it('should limit concurrent operations', async () => {
    const semaphore = new Semaphore(2);
    let concurrentOps = 0;
    let maxConcurrent = 0;

    const operation = async () => {
      await semaphore.acquire();
      try {
        concurrentOps++;
        maxConcurrent = Math.max(maxConcurrent, concurrentOps);
        await new Promise((resolve) => setTimeout(resolve, 10));
      } finally {
        concurrentOps--;
        semaphore.release();
      }
    };

    await Promise.all([
      operation(),
      operation(),
      operation(),
      operation(),
      operation(),
    ]);

    expect(maxConcurrent).toBeLessThanOrEqual(2);
  });

  it('should track waiting count', async () => {
    const semaphore = new Semaphore(1);

    await semaphore.acquire();
    expect(semaphore.availablePermits).toBe(0);
    expect(semaphore.waitingCount).toBe(0);

    // Start a waiting operation
    const waitingPromise = semaphore.acquire();
    expect(semaphore.waitingCount).toBe(1);

    semaphore.release();
    await waitingPromise;
    semaphore.release();
  });

  it('should handle rapid acquire/release', async () => {
    const semaphore = new Semaphore(10);

    const operations = Array.from({ length: 100 }, async () => {
      await semaphore.acquire();
      await new Promise((resolve) => setTimeout(resolve, 1));
      semaphore.release();
    });

    await expect(Promise.all(operations)).resolves.not.toThrow();
    expect(semaphore.availablePermits).toBe(10);
  });
});

// ============================================================================
// Multipart Upload Resource Tests
// ============================================================================

describe('Resource Exhaustion - Multipart Upload Limits', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should timeout abandoned multipart uploads', async () => {
    let timedOut = false;

    const upload = createBufferedMultipartUpload(
      async () => {},
      {
        managed: true,
        timeoutMs: 100,
        onTimeout: () => {
          timedOut = true;
        },
      }
    );

    // Don't complete the upload, let it timeout
    await vi.advanceTimersByTimeAsync(150);

    expect(timedOut).toBe(true);
  });

  it('should throw error when uploading to finalized upload', async () => {
    const upload = createBufferedMultipartUpload(
      async () => {},
      { managed: true, timeoutMs: 1000 }
    );

    await upload.complete([]);

    await expect(upload.uploadPart(1, new Uint8Array([1, 2, 3]))).rejects.toThrow(/finalized/i);
  });

  it('should throw error when completing already finalized upload', async () => {
    const upload = createBufferedMultipartUpload(
      async () => {},
      { managed: true, timeoutMs: 1000 }
    );

    await upload.complete([]);

    await expect(upload.complete([])).rejects.toThrow(/finalized/i);
  });

  it('should clear timeout on successful complete', async () => {
    let timedOut = false;

    const upload = createBufferedMultipartUpload(
      async () => {},
      {
        managed: true,
        timeoutMs: 100,
        onTimeout: () => {
          timedOut = true;
        },
      }
    );

    await upload.complete([]);

    // Advance past timeout - should not trigger
    await vi.advanceTimersByTimeAsync(200);

    expect(timedOut).toBe(false);
  });

  it('should clear timeout on abort', async () => {
    let timedOut = false;

    const upload = createBufferedMultipartUpload(
      async () => {},
      {
        managed: true,
        timeoutMs: 100,
        onTimeout: () => {
          timedOut = true;
        },
      }
    );

    await upload.abort();

    // Advance past timeout - should not trigger
    await vi.advanceTimersByTimeAsync(200);

    expect(timedOut).toBe(false);
  });
});

// ============================================================================
// Lock Contention Tests
// ============================================================================

describe('Resource Exhaustion - Lock Contention', () => {
  let lockManager: LockManager;

  beforeEach(() => {
    lockManager = new LockManager(5000);
  });

  it('should fail to acquire lock held by another transaction', () => {
    const op1: BufferedOperation = {
      type: 'update',
      database: 'db',
      collection: 'users',
      filter: { _id: 'user1' },
      update: { $set: { name: 'Updated' } },
      timestamp: Date.now(),
    };

    // First transaction acquires lock
    const result1 = lockManager.acquireLocks('txn1', [op1]);
    expect(result1.success).toBe(true);

    // Second transaction should fail
    const result2 = lockManager.acquireLocks('txn2', [op1]);
    expect(result2.success).toBe(false);
    expect(result2.errors[0]).toContain('is locked by transaction txn1');
  });

  it('should include lock holder in error message', () => {
    const op: BufferedOperation = {
      type: 'update',
      database: 'db',
      collection: 'users',
      filter: { _id: 'user123' },
      update: { $set: { name: 'Test' } },
      timestamp: Date.now(),
    };

    lockManager.acquireLocks('txn-holder-abc', [op]);

    const result = lockManager.acquireLocks('txn-requester-xyz', [op]);
    expect(result.success).toBe(false);
    expect(result.errors[0]).toContain('txn-holder-abc');
    expect(result.errors[0]).toContain('user123');
  });

  it('should allow same transaction to re-acquire its own lock', () => {
    const op: BufferedOperation = {
      type: 'update',
      database: 'db',
      collection: 'users',
      filter: { _id: 'user1' },
      update: { $set: { name: 'Test' } },
      timestamp: Date.now(),
    };

    const result1 = lockManager.acquireLocks('txn1', [op]);
    expect(result1.success).toBe(true);

    // Same transaction should succeed
    const result2 = lockManager.acquireLocks('txn1', [op]);
    expect(result2.success).toBe(true);
  });

  it('should release locks allowing other transactions', () => {
    const op: BufferedOperation = {
      type: 'update',
      database: 'db',
      collection: 'users',
      filter: { _id: 'user1' },
      update: { $set: { name: 'Test' } },
      timestamp: Date.now(),
    };

    lockManager.acquireLocks('txn1', [op]);

    // Should fail while locked
    expect(lockManager.acquireLocks('txn2', [op]).success).toBe(false);

    // Release locks
    lockManager.releaseLocks('txn1');

    // Should succeed now
    expect(lockManager.acquireLocks('txn2', [op]).success).toBe(true);
  });

  it('should track lock statistics', () => {
    const ops: BufferedOperation[] = [
      {
        type: 'update',
        database: 'db',
        collection: 'users',
        filter: { _id: 'user1' },
        update: { $set: { name: 'Test' } },
        timestamp: Date.now(),
      },
      {
        type: 'update',
        database: 'db',
        collection: 'users',
        filter: { _id: 'user2' },
        update: { $set: { name: 'Test' } },
        timestamp: Date.now(),
      },
    ];

    lockManager.acquireLocks('txn1', ops);

    const stats = lockManager.getStats();
    expect(stats.totalLocks).toBe(2);
    expect(stats.transactionCount).toBe(1);
  });

  it('should extend lock timeout', () => {
    vi.useFakeTimers();

    const op: BufferedOperation = {
      type: 'update',
      database: 'db',
      collection: 'users',
      filter: { _id: 'user1' },
      update: { $set: { name: 'Test' } },
      timestamp: Date.now(),
    };

    lockManager.acquireLocks('txn1', [op]);
    lockManager.extendLocks('txn1', 10000);

    // Advance time but not past extended timeout
    vi.advanceTimersByTime(7000);

    // Lock should still prevent acquisition
    const result = lockManager.acquireLocks('txn2', [op]);
    expect(result.success).toBe(false);

    vi.useRealTimers();
  });
});

// ============================================================================
// Buffer Overflow Simulation Tests
// ============================================================================

describe('Resource Exhaustion - Buffer Overflow', () => {
  /**
   * Simulates a bounded buffer
   */
  class BoundedBuffer<T> {
    private items: T[] = [];
    private readonly maxSize: number;

    constructor(maxSize: number) {
      this.maxSize = maxSize;
    }

    push(item: T): void {
      if (this.items.length >= this.maxSize) {
        throw new BufferOverflowError(
          `Buffer overflow: cannot add item, buffer is full (${this.maxSize} items)`,
          { currentSize: this.items.length, maxSize: this.maxSize }
        );
      }
      this.items.push(item);
    }

    pop(): T | undefined {
      return this.items.shift();
    }

    size(): number {
      return this.items.length;
    }

    isFull(): boolean {
      return this.items.length >= this.maxSize;
    }
  }

  class BufferOverflowError extends Error {
    constructor(
      message: string,
      public readonly details: { currentSize: number; maxSize: number }
    ) {
      super(message);
      this.name = 'BufferOverflowError';
    }
  }

  it('should throw BufferOverflowError when buffer full', () => {
    const buffer = new BoundedBuffer<number>(3);

    buffer.push(1);
    buffer.push(2);
    buffer.push(3);

    expect(() => buffer.push(4)).toThrow(BufferOverflowError);
  });

  it('should include buffer details in error', () => {
    const buffer = new BoundedBuffer<number>(5);

    for (let i = 0; i < 5; i++) {
      buffer.push(i);
    }

    try {
      buffer.push(100);
    } catch (error) {
      expect(error).toBeInstanceOf(BufferOverflowError);
      const bufError = error as BufferOverflowError;
      expect(bufError.details.maxSize).toBe(5);
      expect(bufError.details.currentSize).toBe(5);
    }
  });

  it('should recover after draining buffer', () => {
    const buffer = new BoundedBuffer<number>(3);

    buffer.push(1);
    buffer.push(2);
    buffer.push(3);

    expect(() => buffer.push(4)).toThrow(BufferOverflowError);

    buffer.pop();

    expect(() => buffer.push(4)).not.toThrow();
    expect(buffer.size()).toBe(3);
  });

  it('should track fullness correctly', () => {
    const buffer = new BoundedBuffer<string>(2);

    expect(buffer.isFull()).toBe(false);

    buffer.push('a');
    expect(buffer.isFull()).toBe(false);

    buffer.push('b');
    expect(buffer.isFull()).toBe(true);

    buffer.pop();
    expect(buffer.isFull()).toBe(false);
  });
});

// ============================================================================
// Rate Limiting Simulation Tests
// ============================================================================

describe('Resource Exhaustion - Rate Limiting', () => {
  class RateLimiter {
    private tokens: number;
    private readonly maxTokens: number;
    private readonly refillRate: number; // tokens per second
    private lastRefill: number;

    constructor(maxTokens: number, refillRate: number) {
      this.maxTokens = maxTokens;
      this.tokens = maxTokens;
      this.refillRate = refillRate;
      this.lastRefill = Date.now();
    }

    private refill(): void {
      const now = Date.now();
      const elapsed = (now - this.lastRefill) / 1000;
      const newTokens = elapsed * this.refillRate;
      this.tokens = Math.min(this.maxTokens, this.tokens + newTokens);
      this.lastRefill = now;
    }

    tryAcquire(): boolean {
      this.refill();
      if (this.tokens >= 1) {
        this.tokens--;
        return true;
      }
      return false;
    }

    acquire(): void {
      if (!this.tryAcquire()) {
        throw new RateLimitExceededError(
          `Rate limit exceeded: ${this.maxTokens} requests per second allowed`,
          {
            limit: this.maxTokens,
            availableTokens: Math.floor(this.tokens),
          }
        );
      }
    }

    getAvailableTokens(): number {
      this.refill();
      return Math.floor(this.tokens);
    }
  }

  class RateLimitExceededError extends Error {
    constructor(
      message: string,
      public readonly details: { limit: number; availableTokens: number }
    ) {
      super(message);
      this.name = 'RateLimitExceededError';
    }
  }

  it('should throw RateLimitExceededError when rate exceeded', () => {
    const limiter = new RateLimiter(3, 1);

    limiter.acquire();
    limiter.acquire();
    limiter.acquire();

    expect(() => limiter.acquire()).toThrow(RateLimitExceededError);
  });

  it('should include rate limit details in error', () => {
    const limiter = new RateLimiter(5, 1);

    // Exhaust all tokens
    for (let i = 0; i < 5; i++) {
      limiter.acquire();
    }

    try {
      limiter.acquire();
    } catch (error) {
      expect(error).toBeInstanceOf(RateLimitExceededError);
      const rateError = error as RateLimitExceededError;
      expect(rateError.details.limit).toBe(5);
    }
  });

  it('should allow operations after token refill', async () => {
    vi.useFakeTimers();

    const limiter = new RateLimiter(2, 2); // 2 tokens, refill 2 per second

    limiter.acquire();
    limiter.acquire();

    expect(() => limiter.acquire()).toThrow(RateLimitExceededError);

    // Wait for refill
    vi.advanceTimersByTime(1000);

    expect(() => limiter.acquire()).not.toThrow();

    vi.useRealTimers();
  });

  it('should track available tokens', () => {
    const limiter = new RateLimiter(10, 1);

    expect(limiter.getAvailableTokens()).toBe(10);

    limiter.acquire();
    expect(limiter.getAvailableTokens()).toBe(9);

    limiter.acquire();
    limiter.acquire();
    expect(limiter.getAvailableTokens()).toBe(7);
  });
});

// ============================================================================
// Error Recovery and Resilience Tests
// ============================================================================

describe('Resource Exhaustion - Error Recovery', () => {
  it('should handle multiple consecutive resource errors gracefully', async () => {
    const storage = new MemoryStorage();
    const errors: Error[] = [];

    // Simulate multiple operations that might fail
    for (let i = 0; i < 10; i++) {
      try {
        await storage.put(`key${i}`, new Uint8Array(100));
      } catch (error) {
        errors.push(error as Error);
      }
    }

    // All should succeed for basic memory storage
    expect(errors).toHaveLength(0);
  });

  it('should not leak resources on error', async () => {
    const semaphore = new Semaphore(5);

    const operationWithError = async () => {
      await semaphore.acquire();
      try {
        throw new Error('Simulated error');
      } finally {
        semaphore.release();
      }
    };

    // Run multiple operations that error
    for (let i = 0; i < 10; i++) {
      try {
        await operationWithError();
      } catch {
        // Expected
      }
    }

    // Semaphore should be fully available
    expect(semaphore.availablePermits).toBe(5);
  });

  it('should handle rapid resource allocation/deallocation', async () => {
    const semaphore = new Semaphore(3);
    const results: boolean[] = [];

    const operations = Array.from({ length: 50 }, async () => {
      await semaphore.acquire();
      try {
        await new Promise((resolve) => setTimeout(resolve, 1));
        results.push(true);
      } finally {
        semaphore.release();
      }
    });

    await Promise.all(operations);

    expect(results).toHaveLength(50);
    expect(semaphore.availablePermits).toBe(3);
  });
});
