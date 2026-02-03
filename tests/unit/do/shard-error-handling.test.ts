/**
 * ShardDO Error Handling Tests
 *
 * Tests for error handling, timeouts, resource exhaustion, and invalid operations.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import type { DurableObjectState, DurableObjectStorage, R2Bucket } from '@cloudflare/workers-types';
import {
  ShardDO,
  type ShardDOEnv,
  createMockState,
  createMockR2Bucket,
  createMockEnv,
} from './test-helpers.js';

describe('ShardDO - Error Handling', () => {
  let shard: ShardDO;
  let state: DurableObjectState;
  let env: ShardDOEnv;
  let bucket: R2Bucket;

  beforeEach(() => {
    state = createMockState();
    bucket = createMockR2Bucket();
    env = createMockEnv(bucket);
    shard = new ShardDO(state, env);
  });

  it('should reject write without collection', async () => {
    await expect(
      shard.write({
        collection: '',
        op: 'insert',
        document: { _id: 'doc1' },
      })
    ).rejects.toThrow();
  });

  it('should reject insert without document', async () => {
    await expect(
      shard.write({
        collection: 'users',
        op: 'insert',
        document: undefined as unknown as object,
      })
    ).rejects.toThrow();
  });

  it('should reject update without filter', async () => {
    await expect(
      shard.write({
        collection: 'users',
        op: 'update',
        filter: undefined as unknown as object,
        update: { $set: { x: 1 } },
      })
    ).rejects.toThrow();
  });

  it('should handle R2 write failure', async () => {
    await shard.configure({ flushThresholdDocs: 1 });

    (bucket.put as ReturnType<typeof vi.fn>).mockRejectedValue(new Error('R2 unavailable'));

    // Write should still succeed (buffered in memory and WAL)
    const result = await shard.write({
      collection: 'users',
      op: 'insert',
      document: { _id: 'doc1' },
    });

    expect(result.acknowledged).toBe(true);

    // But manual flush should throw
    await expect(shard.flush()).rejects.toThrow('R2 unavailable');
  });

  it('should handle R2 read failure gracefully', async () => {
    // First write and flush successfully
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1' } });
    await shard.flush();

    // Then make R2 fail
    (bucket.get as ReturnType<typeof vi.fn>).mockRejectedValue(new Error('R2 unavailable'));

    // Query should throw or return partial results
    await expect(shard.find('users', {})).rejects.toThrow();
  });

  it('should validate document _id', async () => {
    // _id with invalid characters
    await expect(
      shard.write({
        collection: 'users',
        op: 'insert',
        document: { _id: null } as unknown as object,
      })
    ).rejects.toThrow();
  });
});

describe('ShardDO - Timeout and Retry Scenarios', () => {
  let shard: ShardDO;
  let state: DurableObjectState;
  let env: ShardDOEnv;
  let bucket: R2Bucket;

  beforeEach(() => {
    state = createMockState();
    bucket = createMockR2Bucket();
    env = createMockEnv(bucket);
    shard = new ShardDO(state, env);
  });

  it('should handle slow R2 operations during flush', async () => {
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1' } });

    // Simulate slow R2 response
    let resolvePromise: () => void;
    const slowPromise = new Promise<void>((resolve) => {
      resolvePromise = resolve;
    });
    (bucket.put as ReturnType<typeof vi.fn>).mockImplementation(async () => {
      await slowPromise;
      return { key: 'test' };
    });

    // Start flush (will be slow)
    const flushPromise = shard.flush();

    // Resolve after short delay to simulate slow but successful operation
    setTimeout(() => resolvePromise!(), 10);

    // Should eventually complete
    await expect(flushPromise).resolves.not.toThrow();
  });

  it('should handle intermittent R2 failures with eventual success', async () => {
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1' } });

    let callCount = 0;
    (bucket.put as ReturnType<typeof vi.fn>).mockImplementation(async () => {
      callCount++;
      if (callCount < 2) {
        throw new Error('Temporary R2 failure');
      }
      return { key: 'test' };
    });

    // First flush should fail
    await expect(shard.flush()).rejects.toThrow('Temporary R2 failure');

    // Second flush should succeed (data still in WAL)
    await expect(shard.flush()).resolves.not.toThrow();
  });

  it('should preserve data in WAL after failed flush', async () => {
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1', name: 'Alice' } });

    // Make R2 fail
    (bucket.put as ReturnType<typeof vi.fn>).mockRejectedValue(new Error('R2 down'));

    // Flush should fail
    await expect(shard.flush()).rejects.toThrow('R2 down');

    // Data should still be readable from memory/WAL
    const result = await shard.findOne('users', { _id: 'doc1' });
    expect(result).toBeDefined();
    expect(result?.name).toBe('Alice');
  });

  it('should handle R2 timeout during multipart upload', async () => {
    // Write enough data to trigger multipart upload
    const largeDoc = { _id: 'large1', data: 'x'.repeat(10000) };
    await shard.write({ collection: 'users', op: 'insert', document: largeDoc });

    (bucket.put as ReturnType<typeof vi.fn>).mockImplementation(() => {
      return new Promise((_, reject) => {
        setTimeout(() => reject(new Error('Request timeout')), 50);
      });
    });

    await expect(shard.flush()).rejects.toThrow('timeout');
  });

  it('should handle alarm scheduling failures gracefully', async () => {
    const storage = state.storage as DurableObjectStorage;
    (storage.setAlarm as ReturnType<typeof vi.fn>).mockRejectedValue(new Error('Alarm scheduling failed'));

    // Write should still succeed even if alarm scheduling fails
    const result = await shard.write({
      collection: 'users',
      op: 'insert',
      document: { _id: 'doc1' },
    });

    expect(result.acknowledged).toBe(true);
  });
});

describe('ShardDO - Resource Exhaustion Scenarios', () => {
  let shard: ShardDO;
  let state: DurableObjectState;
  let env: ShardDOEnv;
  let bucket: R2Bucket;

  beforeEach(() => {
    state = createMockState();
    bucket = createMockR2Bucket();
    env = createMockEnv(bucket);
    shard = new ShardDO(state, env);
  });

  it('should handle SQLite storage full error', async () => {
    const storage = state.storage as DurableObjectStorage;
    (storage.sql.exec as ReturnType<typeof vi.fn>).mockImplementation(() => {
      throw new Error('SQLITE_FULL: database or disk is full');
    });

    await expect(
      shard.write({
        collection: 'users',
        op: 'insert',
        document: { _id: 'doc1' },
      })
    ).rejects.toThrow();
  });

  it('should handle R2 quota exceeded error', async () => {
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1' } });

    (bucket.put as ReturnType<typeof vi.fn>).mockRejectedValue(
      new Error('QuotaExceeded: R2 storage quota exceeded')
    );

    await expect(shard.flush()).rejects.toThrow('QuotaExceeded');
  });

  it('should handle large batch writes without memory issues', async () => {
    // Write many small documents
    for (let i = 0; i < 100; i++) {
      await shard.write({
        collection: 'users',
        op: 'insert',
        document: { _id: `doc${i}`, value: i },
      });
    }

    // Verify all documents are readable
    const results = await shard.find('users', {});
    expect(results.length).toBe(100);
  });

  it('should handle deep document nesting', async () => {
    // Create deeply nested document
    let nested: Record<string, unknown> = { value: 'leaf' };
    for (let i = 0; i < 20; i++) {
      nested = { nested };
    }

    const result = await shard.write({
      collection: 'users',
      op: 'insert',
      document: { _id: 'nested1', ...nested },
    });

    expect(result.acknowledged).toBe(true);
  });

  it('should handle document with many keys', async () => {
    const manyKeys: Record<string, unknown> = { _id: 'manykeys1' };
    for (let i = 0; i < 500; i++) {
      manyKeys[`key${i}`] = `value${i}`;
    }

    const result = await shard.write({
      collection: 'users',
      op: 'insert',
      document: manyKeys,
    });

    expect(result.acknowledged).toBe(true);

    const retrieved = await shard.findOne('users', { _id: 'manykeys1' });
    expect(retrieved?.key250).toBe('value250');
  });
});

describe('ShardDO - Invalid Operation Scenarios', () => {
  let shard: ShardDO;
  let state: DurableObjectState;
  let env: ShardDOEnv;

  beforeEach(() => {
    state = createMockState();
    env = createMockEnv();
    shard = new ShardDO(state, env);
  });

  it('should reject unknown operation type', async () => {
    await expect(
      shard.write({
        collection: 'users',
        op: 'invalid_op' as unknown as 'insert',
        document: { _id: 'doc1' },
      })
    ).rejects.toThrow();
  });

  it('should reject update with unknown operators', async () => {
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1', x: 1 } });

    // Unknown operators are rejected with validation error
    await expect(
      shard.write({
        collection: 'users',
        op: 'update',
        filter: { _id: 'doc1' },
        update: { $invalidOperator: { x: 2 } },
      })
    ).rejects.toThrow(/invalid update operator/);
  });

  it('should reject delete without filter', async () => {
    await expect(
      shard.write({
        collection: 'users',
        op: 'delete',
        filter: undefined as unknown as object,
      })
    ).rejects.toThrow();
  });

  it('should handle concurrent writes to same document', async () => {
    // Write same document twice concurrently
    const write1 = shard.write({
      collection: 'users',
      op: 'insert',
      document: { _id: 'concurrent1', value: 1 },
    });

    const write2 = shard.write({
      collection: 'users',
      op: 'insert',
      document: { _id: 'concurrent1', value: 2 },
    });

    // One should succeed, one should fail with duplicate key error
    const results = await Promise.allSettled([write1, write2]);

    const successes = results.filter((r) => r.status === 'fulfilled');
    expect(successes.length).toBeGreaterThanOrEqual(1);
  });

  it('should reject null collection name', async () => {
    await expect(
      shard.write({
        collection: null as unknown as string,
        op: 'insert',
        document: { _id: 'doc1' },
      })
    ).rejects.toThrow();
  });

  it('should reject undefined document on insert', async () => {
    await expect(
      shard.write({
        collection: 'users',
        op: 'insert',
        document: undefined as unknown as object,
      })
    ).rejects.toThrow();
  });

  it('should reject array as document', async () => {
    await expect(
      shard.write({
        collection: 'users',
        op: 'insert',
        document: [{ _id: 'doc1' }] as unknown as object,
      })
    ).rejects.toThrow();
  });

  it('should reject document with circular reference', async () => {
    const circular: Record<string, unknown> = { _id: 'circular1' };
    circular.self = circular;

    await expect(
      shard.write({
        collection: 'users',
        op: 'insert',
        document: circular,
      })
    ).rejects.toThrow();
  });
});
