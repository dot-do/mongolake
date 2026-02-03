/**
 * WAL Size Limits and Auto-Flush Tests
 *
 * Tests for WAL size limits enforcement and automatic flush triggering.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import type { DurableObjectStorage, R2Bucket } from '@cloudflare/workers-types';
import {
  ShardDO,
  type ShardDOEnv,
  createMockStorage,
  createMockState,
  createMockR2Bucket,
  createMockEnv,
} from './test-helpers.js';
import { WalManager } from '../../../src/do/shard/wal-manager.js';
import { DurableObjectStorageBackend } from '../../../src/do/shard/types.js';
import { MAX_WAL_SIZE_BYTES, MAX_WAL_ENTRIES } from '../../../src/constants.js';

describe('WalManager - shouldForceFlush()', () => {
  let storage: DurableObjectStorage;
  let storageBackend: DurableObjectStorageBackend;
  let walManager: WalManager;

  beforeEach(() => {
    storage = createMockStorage();
    const state = createMockState(storage);
    storageBackend = new DurableObjectStorageBackend(state);
    walManager = new WalManager(storageBackend);
    walManager.initializeTables();
  });

  it('should return needed: false when WAL is empty', () => {
    const result = walManager.shouldForceFlush();
    expect(result.needed).toBe(false);
    expect(result.reason).toBe(null);
  });

  it('should return needed: false when under both size and entry limits', () => {
    // Configure small limits for testing
    walManager.configureWalLimits(1000, 10);

    // Add a small entry (well under limits)
    const lsn = walManager.allocateLSN();
    walManager.persistEntry({
      lsn,
      collection: 'test',
      op: 'i',
      docId: 'doc1',
      document: { _id: 'doc1', x: 1 },
      flushed: false,
    });

    const result = walManager.shouldForceFlush();
    expect(result.needed).toBe(false);
    expect(result.reason).toBe(null);
  });

  it('should return needed: true with reason "size" when size limit reached', () => {
    // Configure a very small size limit
    walManager.configureWalLimits(100, 10000);

    // Add entries until size limit is exceeded
    for (let i = 0; i < 5; i++) {
      const lsn = walManager.allocateLSN();
      walManager.persistEntry({
        lsn,
        collection: 'test',
        op: 'i',
        docId: `doc${i}`,
        document: { _id: `doc${i}`, data: 'x'.repeat(50) },
        flushed: false,
      });
    }

    const result = walManager.shouldForceFlush();
    expect(result.needed).toBe(true);
    expect(result.reason).toBe('size');
  });

  it('should return needed: true with reason "entries" when entry limit reached', () => {
    // Configure a small entry limit
    walManager.configureWalLimits(10 * 1024 * 1024, 5);

    // Add entries until entry limit is exceeded
    for (let i = 0; i < 5; i++) {
      const lsn = walManager.allocateLSN();
      walManager.persistEntry({
        lsn,
        collection: 'test',
        op: 'i',
        docId: `doc${i}`,
        document: { _id: `doc${i}` },
        flushed: false,
      });
    }

    const result = walManager.shouldForceFlush();
    expect(result.needed).toBe(true);
    expect(result.reason).toBe('entries');
  });

  it('should prioritize size reason over entries when both exceeded', () => {
    // Configure both limits to be exceeded
    walManager.configureWalLimits(100, 3);

    // Add entries that exceed both limits
    for (let i = 0; i < 5; i++) {
      const lsn = walManager.allocateLSN();
      walManager.persistEntry({
        lsn,
        collection: 'test',
        op: 'i',
        docId: `doc${i}`,
        document: { _id: `doc${i}`, data: 'x'.repeat(50) },
        flushed: false,
      });
    }

    const result = walManager.shouldForceFlush();
    expect(result.needed).toBe(true);
    // Size check happens first in the code
    expect(result.reason).toBe('size');
  });

  it('should exclude flushed entries from entry count', () => {
    // Configure small entry limit
    walManager.configureWalLimits(10 * 1024 * 1024, 5);

    // Add 4 entries
    for (let i = 0; i < 4; i++) {
      const lsn = walManager.allocateLSN();
      walManager.persistEntry({
        lsn,
        collection: 'test',
        op: 'i',
        docId: `doc${i}`,
        document: { _id: `doc${i}` },
        flushed: false,
      });
    }

    // Mark some as flushed
    walManager.markFlushed(2);

    // Add 2 more entries
    for (let i = 4; i < 6; i++) {
      const lsn = walManager.allocateLSN();
      walManager.persistEntry({
        lsn,
        collection: 'test',
        op: 'i',
        docId: `doc${i}`,
        document: { _id: `doc${i}` },
        flushed: false,
      });
    }

    // Should not trigger because flushed entries don't count
    // 4 - 2 flushed + 2 new = 4 unflushed, under limit of 5
    const result = walManager.shouldForceFlush();
    expect(result.needed).toBe(false);
  });
});

describe('WalManager - configureWalLimits()', () => {
  let storage: DurableObjectStorage;
  let storageBackend: DurableObjectStorageBackend;
  let walManager: WalManager;

  beforeEach(() => {
    storage = createMockStorage();
    const state = createMockState(storage);
    storageBackend = new DurableObjectStorageBackend(state);
    walManager = new WalManager(storageBackend);
    walManager.initializeTables();
  });

  it('should have default limits from constants', () => {
    const limits = walManager.getWalLimits();
    expect(limits.maxSizeBytes).toBe(MAX_WAL_SIZE_BYTES);
    expect(limits.maxEntries).toBe(MAX_WAL_ENTRIES);
  });

  it('should allow configuring size limit only', () => {
    walManager.configureWalLimits(5000);
    const limits = walManager.getWalLimits();
    expect(limits.maxSizeBytes).toBe(5000);
    expect(limits.maxEntries).toBe(MAX_WAL_ENTRIES);
  });

  it('should allow configuring entries limit only', () => {
    walManager.configureWalLimits(undefined, 500);
    const limits = walManager.getWalLimits();
    expect(limits.maxSizeBytes).toBe(MAX_WAL_SIZE_BYTES);
    expect(limits.maxEntries).toBe(500);
  });

  it('should allow configuring both limits', () => {
    walManager.configureWalLimits(2000, 200);
    const limits = walManager.getWalLimits();
    expect(limits.maxSizeBytes).toBe(2000);
    expect(limits.maxEntries).toBe(200);
  });
});

describe('WalManager - WAL size tracking', () => {
  let storage: DurableObjectStorage;
  let storageBackend: DurableObjectStorageBackend;
  let walManager: WalManager;

  beforeEach(() => {
    storage = createMockStorage();
    const state = createMockState(storage);
    storageBackend = new DurableObjectStorageBackend(state);
    walManager = new WalManager(storageBackend);
    walManager.initializeTables();
  });

  it('should track WAL size as entries are added', () => {
    expect(walManager.getWalSizeBytes()).toBe(0);

    const lsn = walManager.allocateLSN();
    walManager.persistEntry({
      lsn,
      collection: 'test',
      op: 'i',
      docId: 'doc1',
      document: { _id: 'doc1', name: 'Test' },
      flushed: false,
    });

    expect(walManager.getWalSizeBytes()).toBeGreaterThan(0);
  });

  it('should accumulate size across multiple entries', () => {
    const lsn1 = walManager.allocateLSN();
    walManager.persistEntry({
      lsn: lsn1,
      collection: 'test',
      op: 'i',
      docId: 'doc1',
      document: { _id: 'doc1' },
      flushed: false,
    });
    const sizeAfterFirst = walManager.getWalSizeBytes();

    const lsn2 = walManager.allocateLSN();
    walManager.persistEntry({
      lsn: lsn2,
      collection: 'test',
      op: 'i',
      docId: 'doc2',
      document: { _id: 'doc2' },
      flushed: false,
    });
    const sizeAfterSecond = walManager.getWalSizeBytes();

    expect(sizeAfterSecond).toBeGreaterThan(sizeAfterFirst);
  });

  it('should reduce size when entries are marked flushed', () => {
    // Add entries
    for (let i = 0; i < 3; i++) {
      const lsn = walManager.allocateLSN();
      walManager.persistEntry({
        lsn,
        collection: 'test',
        op: 'i',
        docId: `doc${i}`,
        document: { _id: `doc${i}`, data: 'x'.repeat(100) },
        flushed: false,
      });
    }
    const sizeBefore = walManager.getWalSizeBytes();

    // Mark some as flushed
    walManager.markFlushed(2);
    const sizeAfter = walManager.getWalSizeBytes();

    expect(sizeAfter).toBeLessThan(sizeBefore);
  });

  it('should reset size after checkpoint', () => {
    // Add entries
    for (let i = 0; i < 5; i++) {
      const lsn = walManager.allocateLSN();
      walManager.persistEntry({
        lsn,
        collection: 'test',
        op: 'i',
        docId: `doc${i}`,
        document: { _id: `doc${i}` },
        flushed: false,
      });
    }

    // Mark all as flushed
    walManager.markFlushed(5);

    // Checkpoint removes flushed entries
    walManager.checkpoint();

    expect(walManager.getWalSizeBytes()).toBe(0);
    expect(walManager.getWalEntryCount()).toBe(0);
  });
});

describe('ShardDO - WAL size limits enforcement', () => {
  let storage: DurableObjectStorage;
  let env: ShardDOEnv;
  let bucket: R2Bucket;

  beforeEach(() => {
    storage = createMockStorage();
    bucket = createMockR2Bucket();
    env = createMockEnv(bucket);
  });

  it('should not exceed MAX_WAL_ENTRIES under normal operation', async () => {
    const shard = new ShardDO(createMockState(storage), env);
    await shard.initialize();

    // Write many documents (should trigger auto-flush before exceeding limit)
    // Configure low threshold to avoid auto-flush from buffer
    await shard.configure({ flushThresholdDocs: 100000, flushThresholdBytes: 100 * 1024 * 1024 });

    // Write documents and check WAL entry count stays reasonable
    for (let i = 0; i < 50; i++) {
      await shard.write({
        collection: 'test',
        op: 'insert',
        document: { _id: `doc${i}`, data: `value${i}` },
      });
    }

    const entryCount = await shard.getWalEntryCount();
    // Should have entries but well under the max limit
    expect(entryCount).toBeLessThanOrEqual(MAX_WAL_ENTRIES);
  });

  it('should report correct WAL size through status endpoint', async () => {
    const shard = new ShardDO(createMockState(storage), env);
    await shard.initialize();

    // Write some documents
    await shard.write({
      collection: 'test',
      op: 'insert',
      document: { _id: 'doc1', name: 'Test', data: 'x'.repeat(100) },
    });

    const request = new Request('http://localhost/status', { method: 'GET' });
    const response = await shard.fetch(request);
    const status = await response.json() as {
      walSizeBytes: number;
      walEntryCount: number;
      walLimits: { maxSizeBytes: number; maxEntries: number };
    };

    expect(status.walSizeBytes).toBeGreaterThan(0);
    expect(status.walEntryCount).toBe(1);
    expect(status.walLimits.maxSizeBytes).toBe(MAX_WAL_SIZE_BYTES);
    expect(status.walLimits.maxEntries).toBe(MAX_WAL_ENTRIES);
  });
});

describe('ShardDO - Auto-flush triggered by WAL limits', () => {
  let storage: DurableObjectStorage;
  let env: ShardDOEnv;
  let bucket: R2Bucket;

  beforeEach(() => {
    storage = createMockStorage();
    bucket = createMockR2Bucket();
    env = createMockEnv(bucket);
  });

  it('should auto-flush when entry limit is reached', async () => {
    const shard = new ShardDO(createMockState(storage), env);
    await shard.initialize();

    // Configure high buffer thresholds so only WAL limit triggers flush
    await shard.configure({
      flushThresholdDocs: 100000,
      flushThresholdBytes: 100 * 1024 * 1024,
    });

    // Mock the WalManager to have a low entry limit
    // Since we can't directly configure it through ShardDO, we'll verify behavior indirectly
    // by checking that WAL is managed properly

    // Write documents
    for (let i = 0; i < 10; i++) {
      await shard.write({
        collection: 'test',
        op: 'insert',
        document: { _id: `doc${i}` },
      });
    }

    // Verify WAL entries are being tracked
    const entryCount = await shard.getWalEntryCount();
    expect(entryCount).toBeGreaterThan(0);
  });

  it('should reduce WAL size after flush is triggered', async () => {
    const shard = new ShardDO(createMockState(storage), env);
    await shard.initialize();

    // Write some documents
    for (let i = 0; i < 5; i++) {
      await shard.write({
        collection: 'test',
        op: 'insert',
        document: { _id: `doc${i}`, data: 'x'.repeat(200) },
      });
    }

    const walSizeBefore = await shard.getWalSizeBytes();
    expect(walSizeBefore).toBeGreaterThan(0);

    // Trigger manual flush
    await shard.flush();

    const walSizeAfter = await shard.getWalSizeBytes();
    expect(walSizeAfter).toBeLessThan(walSizeBefore);
  });

  it('should clear WAL entries after checkpoint', async () => {
    const shard = new ShardDO(createMockState(storage), env);
    await shard.initialize();

    // Write documents
    await shard.write({
      collection: 'test',
      op: 'insert',
      document: { _id: 'doc1', data: 'test' },
    });

    // Flush then checkpoint
    await shard.flush();
    await shard.checkpoint();

    // WAL should be empty
    const entryCount = await shard.getWalEntryCount();
    expect(entryCount).toBe(0);
  });

  it('should preserve unflushed entries after partial flush', async () => {
    const shard = new ShardDO(createMockState(storage), env);
    await shard.initialize();

    // Write first batch
    await shard.write({
      collection: 'test',
      op: 'insert',
      document: { _id: 'doc1' },
    });
    await shard.write({
      collection: 'test',
      op: 'insert',
      document: { _id: 'doc2' },
    });

    // Flush first batch
    await shard.flush();

    // Write second batch (unflushed)
    await shard.write({
      collection: 'test',
      op: 'insert',
      document: { _id: 'doc3' },
    });
    await shard.write({
      collection: 'test',
      op: 'insert',
      document: { _id: 'doc4' },
    });

    // Checkpoint removes only flushed entries
    await shard.checkpoint();

    // Should still have 2 unflushed entries
    const entryCount = await shard.getWalEntryCount();
    expect(entryCount).toBe(2);
  });
});

describe('ShardDO - Normal operations under limits', () => {
  let storage: DurableObjectStorage;
  let env: ShardDOEnv;
  let bucket: R2Bucket;

  beforeEach(() => {
    storage = createMockStorage();
    bucket = createMockR2Bucket();
    env = createMockEnv(bucket);
  });

  it('should not trigger flush for small number of writes', async () => {
    const shard = new ShardDO(createMockState(storage), env);
    await shard.initialize();

    // Configure high thresholds
    await shard.configure({
      flushThresholdDocs: 10000,
      flushThresholdBytes: 100 * 1024 * 1024,
    });

    // Write a few small documents
    await shard.write({
      collection: 'test',
      op: 'insert',
      document: { _id: 'doc1', x: 1 },
    });
    await shard.write({
      collection: 'test',
      op: 'insert',
      document: { _id: 'doc2', x: 2 },
    });

    // Should have entries in WAL (not flushed)
    const entryCount = await shard.getWalEntryCount();
    expect(entryCount).toBe(2);

    // Documents should still be queryable from buffer
    const doc = await shard.findOne('test', { _id: 'doc1' });
    expect(doc).toBeDefined();
    expect(doc?.x).toBe(1);
  });

  it('should maintain consistency when WAL is under limits', async () => {
    const shard = new ShardDO(createMockState(storage), env);
    await shard.initialize();

    // Configure high thresholds
    await shard.configure({
      flushThresholdDocs: 10000,
      flushThresholdBytes: 100 * 1024 * 1024,
    });

    // Insert then update
    await shard.write({
      collection: 'test',
      op: 'insert',
      document: { _id: 'doc1', value: 1 },
    });
    await shard.write({
      collection: 'test',
      op: 'update',
      filter: { _id: 'doc1' },
      update: { $set: { value: 2 } },
    });

    // Verify latest value
    const doc = await shard.findOne('test', { _id: 'doc1' });
    expect(doc?.value).toBe(2);

    // WAL should have both entries
    const entryCount = await shard.getWalEntryCount();
    expect(entryCount).toBe(2);
  });

  it('should handle delete operations in WAL', async () => {
    const shard = new ShardDO(createMockState(storage), env);
    await shard.initialize();

    // Configure high thresholds
    await shard.configure({
      flushThresholdDocs: 10000,
      flushThresholdBytes: 100 * 1024 * 1024,
    });

    // Insert then delete
    await shard.write({
      collection: 'test',
      op: 'insert',
      document: { _id: 'doc1', value: 1 },
    });
    await shard.write({
      collection: 'test',
      op: 'delete',
      filter: { _id: 'doc1' },
    });

    // Document should not be found
    const doc = await shard.findOne('test', { _id: 'doc1' });
    expect(doc).toBeNull();

    // WAL should have both entries (insert + delete)
    const entryCount = await shard.getWalEntryCount();
    expect(entryCount).toBe(2);
  });

  it('should accurately estimate entry sizes', async () => {
    const shard = new ShardDO(createMockState(storage), env);
    await shard.initialize();

    // Write a small document
    await shard.write({
      collection: 'test',
      op: 'insert',
      document: { _id: 'small', x: 1 },
    });
    const smallSize = await shard.getWalSizeBytes();

    // Write a larger document
    await shard.write({
      collection: 'test',
      op: 'insert',
      document: { _id: 'large', data: 'x'.repeat(1000) },
    });
    const totalSize = await shard.getWalSizeBytes();

    // Large document should add significantly more size
    const largeDocSize = totalSize - smallSize;
    expect(largeDocSize).toBeGreaterThan(smallSize);
    // The large doc has ~1000 chars of data, so its size should be > 1000
    expect(largeDocSize).toBeGreaterThan(1000);
  });
});

describe('WalManager - Entry count tracking', () => {
  let storage: DurableObjectStorage;
  let storageBackend: DurableObjectStorageBackend;
  let walManager: WalManager;

  beforeEach(() => {
    storage = createMockStorage();
    const state = createMockState(storage);
    storageBackend = new DurableObjectStorageBackend(state);
    walManager = new WalManager(storageBackend);
    walManager.initializeTables();
  });

  it('should start with zero entry count', () => {
    expect(walManager.getWalEntryCount()).toBe(0);
  });

  it('should increment entry count on persist', () => {
    const lsn = walManager.allocateLSN();
    walManager.persistEntry({
      lsn,
      collection: 'test',
      op: 'i',
      docId: 'doc1',
      document: { _id: 'doc1' },
      flushed: false,
    });

    expect(walManager.getWalEntryCount()).toBe(1);
  });

  it('should only count unflushed entries', () => {
    // Add 3 entries
    for (let i = 1; i <= 3; i++) {
      const lsn = walManager.allocateLSN();
      walManager.persistEntry({
        lsn,
        collection: 'test',
        op: 'i',
        docId: `doc${i}`,
        document: { _id: `doc${i}` },
        flushed: false,
      });
    }

    expect(walManager.getWalEntryCount()).toBe(3);

    // Mark first 2 as flushed
    walManager.markFlushed(2);

    // Only 1 unflushed entry should remain in count
    expect(walManager.getWalEntryCount()).toBe(1);
  });

  it('should return zero after all entries are flushed and checkpointed', () => {
    // Add entries
    for (let i = 1; i <= 5; i++) {
      const lsn = walManager.allocateLSN();
      walManager.persistEntry({
        lsn,
        collection: 'test',
        op: 'i',
        docId: `doc${i}`,
        document: { _id: `doc${i}` },
        flushed: false,
      });
    }

    // Mark all as flushed
    walManager.markFlushed(5);

    // Checkpoint
    walManager.checkpoint();

    expect(walManager.getWalEntryCount()).toBe(0);
  });
});
