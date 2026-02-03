/**
 * ShardDO WAL and Recovery Tests
 *
 * Tests for Write-Ahead Log persistence and state recovery.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import type { DurableObjectState, DurableObjectStorage, R2Bucket } from '@cloudflare/workers-types';
import {
  ShardDO,
  type ShardDOEnv,
  createMockStorage,
  createMockState,
  createMockR2Bucket,
  createMockEnv,
} from './test-helpers.js';

describe('ShardDO - WAL Persistence to SQLite', () => {
  let shard: ShardDO;
  let state: DurableObjectState;
  let storage: DurableObjectStorage;
  let env: ShardDOEnv;

  beforeEach(() => {
    storage = createMockStorage();
    state = createMockState(storage);
    env = createMockEnv();
    shard = new ShardDO(state, env);
  });

  it('should create WAL table on initialization', async () => {
    await shard.initialize();

    const sqlStatements = (storage as unknown as { _sqlStatements: string[] })._sqlStatements;
    expect(sqlStatements.some((s) => s.includes('CREATE TABLE') && s.includes('wal'))).toBe(true);
  });

  it('should persist write operations to WAL', async () => {
    await shard.write({
      collection: 'users',
      op: 'insert',
      document: { _id: 'doc1', name: 'Test' },
    });

    const sqlStatements = (storage as unknown as { _sqlStatements: string[] })._sqlStatements;
    expect(sqlStatements.some((s) => s.includes('INSERT INTO') && s.includes('wal'))).toBe(true);
  });

  it('should store LSN in WAL entries', async () => {
    const result = await shard.write({
      collection: 'users',
      op: 'insert',
      document: { _id: 'doc1', name: 'Test' },
    });

    expect(result.lsn).toBeDefined();
    expect(typeof result.lsn).toBe('number');
  });

  it('should store collection name in WAL entries', async () => {
    await shard.write({
      collection: 'users',
      op: 'insert',
      document: { _id: 'doc1' },
    });

    await shard.write({
      collection: 'orders',
      op: 'insert',
      document: { _id: 'order1' },
    });

    const sqlStatements = (storage as unknown as { _sqlStatements: string[] })._sqlStatements;
    const insertStatements = sqlStatements.filter((s) => s.includes('INSERT INTO'));
    expect(insertStatements.length).toBeGreaterThanOrEqual(2);
  });

  it('should store operation type in WAL entries', async () => {
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1' } });
    await shard.write({ collection: 'users', op: 'update', filter: { _id: 'doc1' }, update: { $set: { x: 1 } } });
    await shard.write({ collection: 'users', op: 'delete', filter: { _id: 'doc1' } });

    const sqlStatements = (storage as unknown as { _sqlStatements: string[] })._sqlStatements;
    const insertStatements = sqlStatements.filter((s) => s.includes('INSERT INTO') && s.includes('wal'));
    expect(insertStatements.length).toBe(3);
  });

  it('should mark WAL entries as flushed after R2 write', async () => {
    await shard.configure({ flushThresholdDocs: 2 });

    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1' } });
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc2' } });
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc3' } });

    const sqlStatements = (storage as unknown as { _sqlStatements: string[] })._sqlStatements;
    expect(sqlStatements.some((s) => s.includes('UPDATE') && s.includes('flushed'))).toBe(true);
  });

  it('should support WAL truncation after checkpoint', async () => {
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1' } });
    await shard.flush();

    await shard.checkpoint();

    const sqlStatements = (storage as unknown as { _sqlStatements: string[] })._sqlStatements;
    expect(sqlStatements.some((s) => s.includes('DELETE FROM') && s.includes('wal'))).toBe(true);
  });
});

describe('ShardDO - State Recovery from SQLite', () => {
  let storage: DurableObjectStorage;
  let env: ShardDOEnv;

  beforeEach(() => {
    storage = createMockStorage();
    env = createMockEnv();
  });

  it('should recover LSN counter on restart', async () => {
    // Simulate previous session
    const shard1 = new ShardDO(createMockState(storage), env);
    await shard1.write({ collection: 'users', op: 'insert', document: { _id: 'doc1' } });
    await shard1.write({ collection: 'users', op: 'insert', document: { _id: 'doc2' } });
    const lastResult = await shard1.write({ collection: 'users', op: 'insert', document: { _id: 'doc3' } });
    const lastLSN = lastResult.lsn;

    // Simulate restart - new instance with same storage
    const shard2 = new ShardDO(createMockState(storage), env);
    await shard2.initialize();

    // New writes should continue from last LSN
    const newResult = await shard2.write({ collection: 'users', op: 'insert', document: { _id: 'doc4' } });
    expect(newResult.lsn).toBeGreaterThan(lastLSN);
  });

  it('should recover unflushed writes from WAL', async () => {
    // Simulate previous session with unflushed writes
    const shard1 = new ShardDO(createMockState(storage), env);
    await shard1.write({ collection: 'users', op: 'insert', document: { _id: 'doc1', name: 'Alice' } });

    // Simulate restart
    const shard2 = new ShardDO(createMockState(storage), env);
    await shard2.initialize();

    // Should be able to query recovered documents
    const result = await shard2.findOne('users', { _id: 'doc1' });
    expect(result).toBeDefined();
    expect(result?.name).toBe('Alice');
  });

  it('should recover flushed LSN watermark', async () => {
    // Simulate previous session with flush
    const bucket = createMockR2Bucket();
    env = createMockEnv(bucket);

    const shard1 = new ShardDO(createMockState(storage), env);
    await shard1.write({ collection: 'users', op: 'insert', document: { _id: 'doc1' } });
    await shard1.flush();
    const flushedLSN = await shard1.getFlushedLSN();

    // Simulate restart
    const shard2 = new ShardDO(createMockState(storage), env);
    await shard2.initialize();

    const recoveredFlushedLSN = await shard2.getFlushedLSN();
    expect(recoveredFlushedLSN).toBe(flushedLSN);
  });

  it('should replay operations in LSN order during recovery', async () => {
    const shard1 = new ShardDO(createMockState(storage), env);
    await shard1.write({ collection: 'users', op: 'insert', document: { _id: 'doc1', value: 1 } });
    await shard1.write({ collection: 'users', op: 'update', filter: { _id: 'doc1' }, update: { $set: { value: 2 } } });
    await shard1.write({ collection: 'users', op: 'update', filter: { _id: 'doc1' }, update: { $set: { value: 3 } } });

    // Simulate restart
    const shard2 = new ShardDO(createMockState(storage), env);
    await shard2.initialize();

    // Final value should reflect all updates in order
    const result = await shard2.findOne('users', { _id: 'doc1' });
    expect(result?.value).toBe(3);
  });

  it('should not replay already-flushed operations from cold start', async () => {
    const bucket = createMockR2Bucket();
    env = createMockEnv(bucket);

    const shard1 = new ShardDO(createMockState(storage), env);
    await shard1.write({ collection: 'users', op: 'insert', document: { _id: 'doc1' } });
    await shard1.flush();
    await shard1.checkpoint();

    // Add more unflushed writes
    await shard1.write({ collection: 'users', op: 'insert', document: { _id: 'doc2' } });

    // Simulate restart
    const shard2 = new ShardDO(createMockState(storage), env);
    await shard2.initialize();

    // doc1 should come from R2, doc2 from recovered WAL
    const bufferDocCount = await shard2.getBufferDocCount();
    expect(bufferDocCount).toBe(1);
  });
});

describe('ShardDO - WAL Size Limits', () => {
  let storage: DurableObjectStorage;
  let env: ShardDOEnv;
  let bucket: R2Bucket;

  beforeEach(() => {
    storage = createMockStorage();
    bucket = createMockR2Bucket();
    env = createMockEnv(bucket);
  });

  it('should track WAL size in bytes', async () => {
    const shard = new ShardDO(createMockState(storage), env);
    await shard.initialize();

    // Initial WAL size should be 0
    expect(await shard.getWalSizeBytes()).toBe(0);

    // Write a document
    await shard.write({
      collection: 'users',
      op: 'insert',
      document: { _id: 'doc1', name: 'Test User', email: 'test@example.com' },
    });

    // WAL size should have increased
    const walSize = await shard.getWalSizeBytes();
    expect(walSize).toBeGreaterThan(0);
  });

  it('should track WAL entry count', async () => {
    const shard = new ShardDO(createMockState(storage), env);
    await shard.initialize();

    // Initial entry count should be 0
    expect(await shard.getWalEntryCount()).toBe(0);

    // Write multiple documents
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1' } });
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc2' } });
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc3' } });

    // Entry count should be 3
    expect(await shard.getWalEntryCount()).toBe(3);
  });

  it('should expose WAL limits', async () => {
    const shard = new ShardDO(createMockState(storage), env);
    await shard.initialize();

    const limits = await shard.getWalLimits();
    expect(limits.maxSizeBytes).toBe(10 * 1024 * 1024); // 10MB default
    expect(limits.maxEntries).toBe(10_000); // 10,000 default
  });

  it('should reduce WAL size after flush', async () => {
    const shard = new ShardDO(createMockState(storage), env);
    await shard.initialize();

    // Write documents
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1', data: 'x'.repeat(100) } });
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc2', data: 'y'.repeat(100) } });

    const walSizeBefore = await shard.getWalSizeBytes();
    expect(walSizeBefore).toBeGreaterThan(0);

    // Flush
    await shard.flush();

    // WAL size should be reduced after flush
    const walSizeAfter = await shard.getWalSizeBytes();
    expect(walSizeAfter).toBeLessThan(walSizeBefore);
  });

  it('should reset WAL size after checkpoint', async () => {
    const shard = new ShardDO(createMockState(storage), env);
    await shard.initialize();

    // Write and flush
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1', data: 'test' } });
    await shard.flush();

    // Checkpoint should remove flushed entries
    await shard.checkpoint();

    // WAL should be empty
    expect(await shard.getWalEntryCount()).toBe(0);
  });

  it('should recover WAL size on restart', async () => {
    const shard1 = new ShardDO(createMockState(storage), env);

    // Write some documents
    await shard1.write({ collection: 'users', op: 'insert', document: { _id: 'doc1', data: 'a'.repeat(50) } });
    await shard1.write({ collection: 'users', op: 'insert', document: { _id: 'doc2', data: 'b'.repeat(50) } });

    const walSizeBefore = await shard1.getWalSizeBytes();
    expect(walSizeBefore).toBeGreaterThan(0);

    // Check the underlying storage has 2 entries
    const mockWal = (storage as unknown as { _wal: Array<{lsn: number; flushed: number}> })._wal;
    const unflushedMockWal = mockWal.filter((e) => e.flushed === 0);
    expect(unflushedMockWal.length).toBe(2);

    // Simulate restart - create new shard with same storage
    // The constructor auto-initializes via blockConcurrencyWhile, so we need to wait for that
    const shard2 = new ShardDO(createMockState(storage), env);

    // Wait a tick to allow constructor's async initialization to complete
    await new Promise(resolve => setTimeout(resolve, 0));

    // WAL size and entry count should be recovered from the 2 persisted entries
    const walSizeAfter = await shard2.getWalSizeBytes();
    const entryCountAfter = await shard2.getWalEntryCount();

    // Size should be approximately the same (may differ slightly due to recalculation)
    expect(walSizeAfter).toBeGreaterThan(0);
    // Entry count should match the 2 entries in storage
    expect(entryCountAfter).toBe(2);
  });

  it('should include WAL info in status endpoint', async () => {
    const shard = new ShardDO(createMockState(storage), env);
    await shard.initialize();

    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1' } });

    const request = new Request('http://localhost/status', { method: 'GET' });
    const response = await shard.fetch(request);
    const status = await response.json() as {
      walSizeBytes: number;
      walEntryCount: number;
      walLimits: { maxSizeBytes: number; maxEntries: number };
    };

    expect(status.walSizeBytes).toBeGreaterThan(0);
    expect(status.walEntryCount).toBe(1);
    expect(status.walLimits).toBeDefined();
    expect(status.walLimits.maxSizeBytes).toBe(10 * 1024 * 1024);
    expect(status.walLimits.maxEntries).toBe(10_000);
  });
});
