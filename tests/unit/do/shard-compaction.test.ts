/**
 * ShardDO Compaction Tests
 *
 * Tests for compaction via alarm and file management.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import type { DurableObjectState, DurableObjectStorage, R2Bucket } from '@cloudflare/workers-types';
import {
  ShardDO,
  type ShardDOEnv,
  createMockStorage,
  createMockState,
  createMockR2Bucket,
  createMockEnv,
} from './test-helpers.js';

describe('ShardDO - Compaction via Alarm', () => {
  let shard: ShardDO;
  let state: DurableObjectState;
  let storage: DurableObjectStorage;
  let env: ShardDOEnv;
  let bucket: R2Bucket;

  beforeEach(() => {
    storage = createMockStorage();
    state = createMockState(storage);
    bucket = createMockR2Bucket();
    env = createMockEnv(bucket);
    shard = new ShardDO(state, env);
  });

  it('should schedule compaction alarm after flush', async () => {
    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1' } });
    await shard.flush();

    expect(storage.setAlarm).toHaveBeenCalled();
  });

  it('should execute compaction on alarm', async () => {
    // Setup: write and flush multiple times to create multiple files
    for (let batch = 0; batch < 5; batch++) {
      for (let i = 0; i < 10; i++) {
        await shard.write({
          collection: 'users',
          op: 'insert',
          document: { _id: `doc${batch}_${i}` },
        });
      }
      await shard.flush();
    }

    const fileCountBefore = ((bucket as unknown as { _objects: Map<string, unknown> })._objects).size;

    // Trigger alarm (compaction)
    await shard.alarm();

    const fileCountAfter = ((bucket as unknown as { _objects: Map<string, unknown> })._objects).size;

    // Compaction should reduce file count
    expect(fileCountAfter).toBeLessThan(fileCountBefore);
  });

  it('should not compact recently written files', async () => {
    await shard.configure({ compactionMinAge: 3600000 }); // 1 hour

    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1' } });
    await shard.flush();

    const fileCountBefore = ((bucket as unknown as { _objects: Map<string, unknown> })._objects).size;

    await shard.alarm();

    const fileCountAfter = ((bucket as unknown as { _objects: Map<string, unknown> })._objects).size;

    // No compaction should occur for recent files
    expect(fileCountAfter).toBe(fileCountBefore);
  });

  it('should merge small files during compaction', async () => {
    await shard.configure({ flushThresholdDocs: 1, compactionMinAge: 0 });

    // Create many small files
    for (let i = 0; i < 20; i++) {
      await shard.write({ collection: 'users', op: 'insert', document: { _id: `doc${i}` } });
      await shard.flush();
    }

    await shard.alarm();

    const files = await bucket.list({ prefix: 'users/' });
    // Should have fewer files after compaction
    expect(files.objects.length).toBeLessThan(20);
  });

  it('should preserve data integrity during compaction', async () => {
    await shard.configure({ flushThresholdDocs: 2, compactionMinAge: 0 });

    // Write data
    for (let i = 0; i < 10; i++) {
      await shard.write({ collection: 'users', op: 'insert', document: { _id: `doc${i}`, value: i } });
    }
    await shard.flush();

    // Compact
    await shard.alarm();

    // Verify all data is still accessible
    const results = await shard.find('users', {});
    expect(results).toHaveLength(10);
    for (let i = 0; i < 10; i++) {
      const doc = results.find((r) => r._id === `doc${i}`);
      expect(doc).toBeDefined();
      expect(doc?.value).toBe(i);
    }
  });

  it('should delete old files after successful compaction', async () => {
    await shard.configure({ flushThresholdDocs: 1, compactionMinAge: 0 });

    for (let i = 0; i < 5; i++) {
      await shard.write({ collection: 'users', op: 'insert', document: { _id: `doc${i}` } });
      await shard.flush();
    }

    const filesBefore = await bucket.list({ prefix: 'users/' });
    const keysBefore = new Set(filesBefore.objects.map((o) => o.key));

    await shard.alarm();

    const filesAfter = await bucket.list({ prefix: 'users/' });
    const keysAfter = new Set(filesAfter.objects.map((o) => o.key));

    // Some old files should be deleted
    const deletedKeys = [...keysBefore].filter((k) => !keysAfter.has(k));
    expect(deletedKeys.length).toBeGreaterThan(0);
  });

  it('should reschedule alarm if more compaction needed', async () => {
    await shard.configure({ flushThresholdDocs: 1, compactionMinAge: 0, compactionBatchSize: 2 });

    // Create many files
    for (let i = 0; i < 10; i++) {
      await shard.write({ collection: 'users', op: 'insert', document: { _id: `doc${i}` } });
      await shard.flush();
    }

    (storage.setAlarm as ReturnType<typeof vi.fn>).mockClear();

    await shard.alarm();

    // Should reschedule alarm for more compaction
    expect(storage.setAlarm).toHaveBeenCalled();
  });

  it('should update manifest after compaction', async () => {
    await shard.configure({ flushThresholdDocs: 1, compactionMinAge: 0 });

    for (let i = 0; i < 5; i++) {
      await shard.write({ collection: 'users', op: 'insert', document: { _id: `doc${i}` } });
      await shard.flush();
    }

    await shard.alarm();

    // Manifest should reflect compacted file structure
    const manifest = await shard.getManifest('users');
    expect(manifest.files.length).toBeLessThan(5);
  });

  it('should handle compaction failure gracefully', async () => {
    await shard.configure({ flushThresholdDocs: 1, compactionMinAge: 0 });

    await shard.write({ collection: 'users', op: 'insert', document: { _id: 'doc1' } });
    await shard.flush();

    // Make R2 put fail
    (bucket.put as ReturnType<typeof vi.fn>).mockRejectedValueOnce(new Error('R2 error'));

    // Alarm should not throw
    await expect(shard.alarm()).resolves.not.toThrow();

    // Data should still be accessible
    const result = await shard.findOne('users', { _id: 'doc1' });
    expect(result).toBeDefined();
  });
});
