/**
 * ShardDO Buffer Tests
 *
 * Tests for buffering documents in memory and flushing to R2.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import type { DurableObjectState, R2Bucket } from '@cloudflare/workers-types';
import {
  ShardDO,
  type ShardDOEnv,
  createMockStorage,
  createMockState,
  createMockR2Bucket,
  createMockEnv,
  createTestDocument,
} from './test-helpers.js';

describe('ShardDO - Buffer Documents in Memory', () => {
  let shard: ShardDO;
  let state: DurableObjectState;
  let env: ShardDOEnv;

  beforeEach(() => {
    state = createMockState();
    env = createMockEnv();
    shard = new ShardDO(state, env);
  });

  it('should accept a single document write', async () => {
    const doc = createTestDocument({ _id: 'doc1', name: 'Alice' });

    const result = await shard.write({
      collection: 'users',
      op: 'insert',
      document: doc,
    });

    expect(result).toBeDefined();
    expect(result.acknowledged).toBe(true);
    expect(result.insertedId).toBe('doc1');
  });

  it('should accept multiple document writes', async () => {
    const docs = [
      createTestDocument({ _id: 'doc1', name: 'Alice' }),
      createTestDocument({ _id: 'doc2', name: 'Bob' }),
      createTestDocument({ _id: 'doc3', name: 'Charlie' }),
    ];

    const results = await Promise.all(
      docs.map((doc) =>
        shard.write({
          collection: 'users',
          op: 'insert',
          document: doc,
        })
      )
    );

    expect(results).toHaveLength(3);
    results.forEach((result) => {
      expect(result.acknowledged).toBe(true);
    });
  });

  it('should buffer documents before flushing to R2', async () => {
    const bucket = createMockR2Bucket();
    env = createMockEnv(bucket);
    shard = new ShardDO(state, env);

    const doc = createTestDocument({ _id: 'doc1' });
    await shard.write({
      collection: 'users',
      op: 'insert',
      document: doc,
    });

    const bufferSize = await shard.getBufferSize();
    expect(bufferSize).toBeGreaterThan(0);
    expect(bucket.put).not.toHaveBeenCalled();
  });

  it('should track buffer size in bytes', async () => {
    const smallDoc = createTestDocument({ _id: 'small', name: 'A' });
    const largeDoc = createTestDocument({
      _id: 'large',
      name: 'A'.repeat(10000),
      tags: Array.from({ length: 100 }, (_, i) => `tag${i}`),
    });

    await shard.write({ collection: 'users', op: 'insert', document: smallDoc });
    const sizeAfterSmall = await shard.getBufferSize();

    await shard.write({ collection: 'users', op: 'insert', document: largeDoc });
    const sizeAfterLarge = await shard.getBufferSize();

    expect(sizeAfterLarge).toBeGreaterThan(sizeAfterSmall);
  });

  it('should track buffer document count', async () => {
    expect(await shard.getBufferDocCount()).toBe(0);

    await shard.write({ collection: 'users', op: 'insert', document: createTestDocument({ _id: 'doc1' }) });
    expect(await shard.getBufferDocCount()).toBe(1);

    await shard.write({ collection: 'users', op: 'insert', document: createTestDocument({ _id: 'doc2' }) });
    expect(await shard.getBufferDocCount()).toBe(2);

    await shard.write({ collection: 'users', op: 'insert', document: createTestDocument({ _id: 'doc3' }) });
    expect(await shard.getBufferDocCount()).toBe(3);
  });

  it('should assign monotonically increasing LSN to each write', async () => {
    const results = await Promise.all([
      shard.write({ collection: 'users', op: 'insert', document: createTestDocument({ _id: 'doc1' }) }),
      shard.write({ collection: 'users', op: 'insert', document: createTestDocument({ _id: 'doc2' }) }),
      shard.write({ collection: 'users', op: 'insert', document: createTestDocument({ _id: 'doc3' }) }),
    ]);

    const lsns = results.map((r) => r.lsn);
    expect(lsns[0]).toBeLessThan(lsns[1]);
    expect(lsns[1]).toBeLessThan(lsns[2]);
  });
});

describe('ShardDO - Flush Buffer to R2', () => {
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

  it('should flush buffer when size threshold is reached', async () => {
    await shard.configure({ flushThresholdBytes: 1000 });

    for (let i = 0; i < 100; i++) {
      await shard.write({
        collection: 'users',
        op: 'insert',
        document: createTestDocument({ _id: `doc${i}`, name: `User ${i}` }),
      });
    }

    expect(bucket.put).toHaveBeenCalled();
  });

  it('should flush buffer when document count threshold is reached', async () => {
    await shard.configure({ flushThresholdDocs: 10 });

    for (let i = 0; i < 15; i++) {
      await shard.write({
        collection: 'users',
        op: 'insert',
        document: createTestDocument({ _id: `doc${i}` }),
      });
    }

    expect(bucket.put).toHaveBeenCalled();
  });

  it('should write Parquet format to R2', async () => {
    await shard.configure({ flushThresholdDocs: 5 });

    for (let i = 0; i < 10; i++) {
      await shard.write({
        collection: 'users',
        op: 'insert',
        document: createTestDocument({ _id: `doc${i}` }),
      });
    }

    const putCalls = (bucket.put as ReturnType<typeof vi.fn>).mock.calls;
    expect(putCalls.length).toBeGreaterThan(0);

    const parquetCall = putCalls.find(([key]: [string, unknown]) => key.endsWith('.parquet'));
    expect(parquetCall).toBeDefined();

    const [key, data] = parquetCall as [string, Uint8Array];
    expect(key).toMatch(/\.parquet$/);

    const magic = new TextDecoder().decode(data.slice(0, 4));
    expect(magic).toBe('PAR1');
  });

  it('should organize files by collection and time partition', async () => {
    await shard.configure({ flushThresholdDocs: 5 });

    for (let i = 0; i < 10; i++) {
      await shard.write({
        collection: 'users',
        op: 'insert',
        document: createTestDocument({ _id: `doc${i}` }),
      });
    }

    const putCalls = (bucket.put as ReturnType<typeof vi.fn>).mock.calls;
    const parquetCall = putCalls.find(([key]: [string, unknown]) => key.endsWith('.parquet'));
    expect(parquetCall).toBeDefined();

    const [key] = parquetCall as [string, Uint8Array];
    expect(key).toMatch(/^users\/\d{4}-\d{2}-\d{2}\/[^/]+\.parquet$/);
  });

  it('should clear buffer after successful flush', async () => {
    await shard.configure({ flushThresholdDocs: 5 });

    for (let i = 0; i < 10; i++) {
      await shard.write({
        collection: 'users',
        op: 'insert',
        document: createTestDocument({ _id: `doc${i}` }),
      });
    }

    const bufferSize = await shard.getBufferSize();
    expect(bufferSize).toBeLessThan(10 * 100);
  });

  it('should support manual flush', async () => {
    await shard.write({
      collection: 'users',
      op: 'insert',
      document: createTestDocument({ _id: 'doc1' }),
    });

    expect(bucket.put).not.toHaveBeenCalled();

    await shard.flush();

    expect(bucket.put).toHaveBeenCalled();
  });

  it('should update LSN watermark after flush', async () => {
    await shard.configure({ flushThresholdDocs: 5 });

    const initialWatermark = await shard.getFlushedLSN();

    for (let i = 0; i < 10; i++) {
      await shard.write({
        collection: 'users',
        op: 'insert',
        document: createTestDocument({ _id: `doc${i}` }),
      });
    }

    const finalWatermark = await shard.getFlushedLSN();
    expect(finalWatermark).toBeGreaterThan(initialWatermark);
  });
});
