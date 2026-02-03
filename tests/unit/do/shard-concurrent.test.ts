/**
 * ShardDO Concurrent Writes Tests
 *
 * Tests for concurrent write handling and serialization.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import type { DurableObjectState } from '@cloudflare/workers-types';
import {
  ShardDO,
  type ShardDOEnv,
  createMockState,
  createMockEnv,
} from './test-helpers.js';

describe('ShardDO - Concurrent Writes', () => {
  let shard: ShardDO;
  let state: DurableObjectState;
  let env: ShardDOEnv;

  beforeEach(() => {
    state = createMockState();
    env = createMockEnv();
    shard = new ShardDO(state, env);
  });

  it('should handle concurrent inserts to same collection', async () => {
    const writes = Array.from({ length: 100 }, (_, i) =>
      shard.write({
        collection: 'users',
        op: 'insert',
        document: { _id: `doc${i}`, index: i },
      })
    );

    const results = await Promise.all(writes);

    // All writes should succeed
    expect(results).toHaveLength(100);
    results.forEach((r) => expect(r.acknowledged).toBe(true));

    // All LSNs should be unique
    const lsns = results.map((r) => r.lsn);
    const uniqueLsns = new Set(lsns);
    expect(uniqueLsns.size).toBe(100);
  });

  it('should serialize concurrent updates to same document', async () => {
    await shard.write({ collection: 'counters', op: 'insert', document: { _id: 'counter', value: 0 } });

    // Concurrent increments
    const increments = Array.from({ length: 10 }, () =>
      shard.write({
        collection: 'counters',
        op: 'update',
        filter: { _id: 'counter' },
        update: { $inc: { value: 1 } },
      })
    );

    await Promise.all(increments);

    const result = await shard.findOne('counters', { _id: 'counter' });
    expect(result?.value).toBe(10);
  });

  it('should maintain consistency under concurrent read/write', async () => {
    // Writer task
    const writer = async () => {
      for (let i = 0; i < 50; i++) {
        await shard.write({
          collection: 'items',
          op: 'insert',
          document: { _id: `item${i}`, value: i },
        });
      }
    };

    // Reader task
    const reader = async () => {
      const results: number[] = [];
      for (let i = 0; i < 20; i++) {
        const items = await shard.find('items', {});
        results.push(items.length);
        await new Promise((r) => setTimeout(r, 1));
      }
      return results;
    };

    const [, readerResults] = await Promise.all([writer(), reader()]);

    // Reader should see monotonically increasing counts
    for (let i = 1; i < readerResults.length; i++) {
      expect(readerResults[i]).toBeGreaterThanOrEqual(readerResults[i - 1]);
    }
  });

  it('should handle concurrent writes to different collections', async () => {
    const writes = [
      shard.write({ collection: 'users', op: 'insert', document: { _id: 'user1' } }),
      shard.write({ collection: 'orders', op: 'insert', document: { _id: 'order1' } }),
      shard.write({ collection: 'products', op: 'insert', document: { _id: 'product1' } }),
      shard.write({ collection: 'users', op: 'insert', document: { _id: 'user2' } }),
      shard.write({ collection: 'orders', op: 'insert', document: { _id: 'order2' } }),
    ];

    const results = await Promise.all(writes);

    results.forEach((r) => expect(r.acknowledged).toBe(true));

    expect(await shard.find('users', {})).toHaveLength(2);
    expect(await shard.find('orders', {})).toHaveLength(2);
    expect(await shard.find('products', {})).toHaveLength(1);
  });

  it('should use blockConcurrencyWhile for critical sections', async () => {
    const blockCalls: number[] = [];
    (state.blockConcurrencyWhile as ReturnType<typeof vi.fn>).mockImplementation(async <T>(fn: () => Promise<T>) => {
      blockCalls.push(Date.now());
      return fn();
    });

    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1' } });
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc2' } });

    // blockConcurrencyWhile should be called for write operations
    expect(blockCalls.length).toBeGreaterThanOrEqual(2);
  });
});
