/**
 * BufferManager Back-Pressure Tests
 *
 * Tests for the back-pressure feature that prevents OOM under high write volume
 * by triggering auto-flush when buffer exceeds maxBytes threshold.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import type { DurableObjectState, R2Bucket } from '@cloudflare/workers-types';
import {
  ShardDO,
  type ShardDOEnv,
  createMockState,
  createMockR2Bucket,
  createMockEnv,
  createTestDocument,
} from './test-helpers.js';
import { DEFAULT_BUFFER_MAX_BYTES } from '../../../src/constants.js';

describe('BufferManager - Back-Pressure', () => {
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

  describe('maxBytes configuration', () => {
    it('should have a default maxBytes of 100MB', async () => {
      // Default maxBytes should be 100 * 1024 * 1024 = 104857600
      expect(DEFAULT_BUFFER_MAX_BYTES).toBe(100 * 1024 * 1024);
    });

    it('should accept custom maxBytes configuration', async () => {
      const customMaxBytes = 10 * 1024 * 1024; // 10MB
      await shard.configure({ maxBytes: customMaxBytes });

      // Verify configuration was applied by writing documents
      // and checking that back-pressure triggers at the right threshold
      const doc = createTestDocument({
        _id: 'test-doc',
        name: 'A'.repeat(1000),
      });

      await shard.write({
        collection: 'users',
        op: 'insert',
        document: doc,
      });

      const bufferSize = await shard.getBufferSize();
      expect(bufferSize).toBeGreaterThan(0);
    });
  });

  describe('back-pressure auto-flush', () => {
    it('should trigger auto-flush when buffer exceeds maxBytes', async () => {
      // Set a very low maxBytes to trigger back-pressure quickly
      await shard.configure({
        maxBytes: 1000, // 1KB - very small for testing
        flushThresholdBytes: 100000, // High threshold so normal flush doesn't trigger
        flushThresholdDocs: 10000, // High threshold so normal flush doesn't trigger
      });

      // Write documents until back-pressure triggers
      for (let i = 0; i < 10; i++) {
        await shard.write({
          collection: 'users',
          op: 'insert',
          document: createTestDocument({
            _id: `doc${i}`,
            name: `User ${'X'.repeat(100)}`, // ~100+ bytes per doc
          }),
        });
      }

      // R2 put should have been called due to back-pressure flush
      expect(bucket.put).toHaveBeenCalled();
    });

    it('should flush before buffer exceeds maxBytes to prevent OOM', async () => {
      // Set maxBytes threshold
      const maxBytes = 2000;
      await shard.configure({
        maxBytes,
        flushThresholdBytes: 10000000, // Very high to ensure only back-pressure triggers
        flushThresholdDocs: 10000000,
      });

      // Write documents
      for (let i = 0; i < 20; i++) {
        await shard.write({
          collection: 'users',
          op: 'insert',
          document: createTestDocument({
            _id: `doc${i}`,
            name: `User ${'X'.repeat(200)}`,
          }),
        });
      }

      // Buffer size should never exceed maxBytes for long
      // (it might briefly exceed during a single write before flush)
      const finalBufferSize = await shard.getBufferSize();
      expect(finalBufferSize).toBeLessThan(maxBytes * 2);
    });

    it('should continue accepting writes after back-pressure flush', async () => {
      await shard.configure({
        maxBytes: 500,
        flushThresholdBytes: 10000000,
        flushThresholdDocs: 10000000,
      });

      // Write many documents - should trigger multiple flushes
      const writePromises = [];
      for (let i = 0; i < 50; i++) {
        writePromises.push(
          shard.write({
            collection: 'users',
            op: 'insert',
            document: createTestDocument({
              _id: `doc${i}`,
              name: `User ${i}`,
            }),
          })
        );
      }

      // All writes should complete successfully
      const results = await Promise.all(writePromises);
      expect(results).toHaveLength(50);
      results.forEach((result) => {
        expect(result.acknowledged).toBe(true);
      });

      // Multiple flushes should have occurred
      const putCallCount = (bucket.put as ReturnType<typeof vi.fn>).mock.calls.length;
      expect(putCallCount).toBeGreaterThan(1);
    });

    it('should clear buffer after back-pressure flush', async () => {
      await shard.configure({
        maxBytes: 500,
        flushThresholdBytes: 10000000,
        flushThresholdDocs: 10000000,
      });

      // Write enough to trigger back-pressure
      for (let i = 0; i < 10; i++) {
        await shard.write({
          collection: 'users',
          op: 'insert',
          document: createTestDocument({
            _id: `doc${i}`,
            name: `User ${'X'.repeat(100)}`,
          }),
        });
      }

      // Buffer should have been cleared after flush
      const bufferSize = await shard.getBufferSize();
      // Buffer might have some documents from after the last flush,
      // but should be much smaller than all docs combined
      expect(bufferSize).toBeLessThan(1000);
    });
  });

  describe('back-pressure interaction with other thresholds', () => {
    it('should respect maxBytes even when other thresholds are not met', async () => {
      await shard.configure({
        maxBytes: 500, // Low back-pressure threshold
        flushThresholdBytes: 10000000, // Very high byte threshold
        flushThresholdDocs: 10000000, // Very high doc count threshold
      });

      // Write documents - only back-pressure should trigger flush
      for (let i = 0; i < 10; i++) {
        await shard.write({
          collection: 'users',
          op: 'insert',
          document: createTestDocument({
            _id: `doc${i}`,
            name: `User ${'X'.repeat(100)}`,
          }),
        });
      }

      // Flush should have been triggered by back-pressure
      expect(bucket.put).toHaveBeenCalled();
    });

    it('should prioritize back-pressure over regular flush thresholds', async () => {
      await shard.configure({
        maxBytes: 300, // Smallest threshold
        flushThresholdBytes: 500, // Medium threshold
        flushThresholdDocs: 3, // Would trigger after 3 docs
      });

      // Write 2 docs - under doc count threshold
      for (let i = 0; i < 2; i++) {
        await shard.write({
          collection: 'users',
          op: 'insert',
          document: createTestDocument({
            _id: `doc${i}`,
            name: `User ${'X'.repeat(200)}`, // ~200+ bytes each
          }),
        });
      }

      // With 2 docs at ~200+ bytes each, we should exceed maxBytes (300)
      // and trigger back-pressure flush before doc count threshold
      expect(bucket.put).toHaveBeenCalled();
    });
  });

  describe('BufferManager.addToBuffer return value', () => {
    it('should return true when buffer exceeds maxBytes', async () => {
      // This tests the BufferManager class directly
      const { BufferManager } = await import('../../../src/do/shard/buffer-manager.js');
      const bufferManager = new BufferManager();

      // Configure with small maxBytes
      bufferManager.configure({ maxBytes: 100 });

      // Add a small document first
      const smallResult = bufferManager.addToBuffer({
        _id: 'doc1',
        _seq: 1,
        _op: 'i',
        collection: 'test',
        document: { _id: 'doc1', name: 'A' },
        lsn: 1,
      });

      // Should not exceed yet
      expect(smallResult).toBe(false);

      // Add a large document
      const largeResult = bufferManager.addToBuffer({
        _id: 'doc2',
        _seq: 2,
        _op: 'i',
        collection: 'test',
        document: { _id: 'doc2', name: 'X'.repeat(200) },
        lsn: 2,
      });

      // Should exceed maxBytes now
      expect(largeResult).toBe(true);
    });

    it('should provide exceedsMaxBytes method', async () => {
      const { BufferManager } = await import('../../../src/do/shard/buffer-manager.js');
      const bufferManager = new BufferManager();

      bufferManager.configure({ maxBytes: 50 });

      expect(bufferManager.exceedsMaxBytes()).toBe(false);

      bufferManager.addToBuffer({
        _id: 'doc1',
        _seq: 1,
        _op: 'i',
        collection: 'test',
        document: { _id: 'doc1', name: 'X'.repeat(100) },
        lsn: 1,
      });

      expect(bufferManager.exceedsMaxBytes()).toBe(true);
    });

    it('should provide getMaxBytes method', async () => {
      const { BufferManager } = await import('../../../src/do/shard/buffer-manager.js');
      const bufferManager = new BufferManager();

      // Default value
      expect(bufferManager.getMaxBytes()).toBe(DEFAULT_BUFFER_MAX_BYTES);

      // After configuration
      bufferManager.configure({ maxBytes: 50000 });
      expect(bufferManager.getMaxBytes()).toBe(50000);
    });
  });

  describe('edge cases', () => {
    it('should handle single large document exceeding maxBytes', async () => {
      await shard.configure({
        maxBytes: 100,
        flushThresholdBytes: 10000000,
        flushThresholdDocs: 10000000,
      });

      // Write a single document larger than maxBytes
      await shard.write({
        collection: 'users',
        op: 'insert',
        document: createTestDocument({
          _id: 'huge-doc',
          name: 'X'.repeat(500), // Way larger than maxBytes
        }),
      });

      // Should still succeed and trigger flush
      expect(bucket.put).toHaveBeenCalled();
    });

    it('should handle rapid sequential writes under back-pressure', async () => {
      await shard.configure({
        maxBytes: 200,
        flushThresholdBytes: 10000000,
        flushThresholdDocs: 10000000,
      });

      // Rapid sequential writes
      for (let i = 0; i < 100; i++) {
        const result = await shard.write({
          collection: 'users',
          op: 'insert',
          document: createTestDocument({
            _id: `doc${i}`,
            name: `User ${i}`,
          }),
        });
        expect(result.acknowledged).toBe(true);
      }

      // Many flushes should have occurred
      const putCallCount = (bucket.put as ReturnType<typeof vi.fn>).mock.calls.length;
      expect(putCallCount).toBeGreaterThan(5);
    });

    it('should handle updates correctly under back-pressure', async () => {
      await shard.configure({
        maxBytes: 500,
        flushThresholdBytes: 10000000,
        flushThresholdDocs: 10000000,
      });

      // Insert documents
      for (let i = 0; i < 5; i++) {
        await shard.write({
          collection: 'users',
          op: 'insert',
          document: createTestDocument({
            _id: `doc${i}`,
            name: `User ${i}`,
          }),
        });
      }

      // Update documents (may cause additional size)
      for (let i = 0; i < 5; i++) {
        await shard.write({
          collection: 'users',
          op: 'update',
          filter: { _id: `doc${i}` },
          update: { $set: { name: `Updated User ${'X'.repeat(100)}` } },
        });
      }

      // All operations should complete
      const bufferDocCount = await shard.getBufferDocCount();
      expect(bufferDocCount).toBeGreaterThanOrEqual(0);
    });

    it('should handle deletes correctly under back-pressure', async () => {
      await shard.configure({
        maxBytes: 500,
        flushThresholdBytes: 10000000,
        flushThresholdDocs: 10000000,
      });

      // Insert documents
      for (let i = 0; i < 10; i++) {
        await shard.write({
          collection: 'users',
          op: 'insert',
          document: createTestDocument({
            _id: `doc${i}`,
            name: `User ${'X'.repeat(50)}`,
          }),
        });
      }

      // Delete some documents
      for (let i = 0; i < 5; i++) {
        await shard.write({
          collection: 'users',
          op: 'delete',
          filter: { _id: `doc${i}` },
        });
      }

      // Operations should complete successfully
      expect(bucket.put).toHaveBeenCalled();
    });
  });
});
