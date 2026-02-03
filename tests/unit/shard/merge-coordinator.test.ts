/**
 * Merge Coordinator Tests
 *
 * Tests for shard merge coordination and state management.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';

import {
  SplitCoordinator,
  createSplitCoordinator,
  type MergeOperation,
} from '../../../src/shard/split-coordinator';
import { createShardRouter } from '../../../src/shard/router';
import { createShardMonitor } from '../../../src/shard/monitor';
import type { ShardRouter } from '../../../src/shard/router';
import type { ShardMonitor } from '../../../src/shard/monitor';

describe('MergeCoordinator', () => {
  let coordinator: SplitCoordinator;
  let router: ShardRouter;
  let monitor: ShardMonitor;

  beforeEach(() => {
    vi.useFakeTimers();

    router = createShardRouter({ shardCount: 16 });
    monitor = createShardMonitor({
      shardCount: 16,
      thresholds: {
        maxDocuments: 1000,
        sustainedThresholdMs: 0,
        checkIntervalMs: 0,
      },
    });

    coordinator = createSplitCoordinator({
      router,
      monitor,
      minSplitIntervalMs: 0,
      minMergeIntervalMs: 0, // Disable for testing
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe('requestMerge', () => {
    it('should accept a valid merge request', async () => {
      const result = await coordinator.requestMerge([0, 1]);

      expect(result.accepted).toBe(true);
      expect(result.mergeId).toBeDefined();
    });

    it('should reject merge with less than 2 shards', async () => {
      const result = await coordinator.requestMerge([0]);

      expect(result.accepted).toBe(false);
      expect(result.rejectionReason).toMatch(/at least 2 shards/);
    });

    it('should reject when max concurrent merges reached', async () => {
      const singleConcurrentCoordinator = createSplitCoordinator({
        router,
        monitor,
        maxConcurrentMerges: 1,
        minMergeIntervalMs: 0,
      });

      // First merge should succeed
      const first = await singleConcurrentCoordinator.requestMerge([0, 1]);
      expect(first.accepted).toBe(true);

      // Second merge should be rejected (first still in progress)
      const second = await singleConcurrentCoordinator.requestMerge([2, 3]);
      expect(second.accepted).toBe(false);
      expect(second.rejectionReason).toMatch(/Maximum concurrent merges/);
    });

    it('should reject if shard is already being merged', async () => {
      const multiCoordinator = createSplitCoordinator({
        router,
        monitor,
        maxConcurrentMerges: 10,
        minMergeIntervalMs: 0,
      });

      const first = await multiCoordinator.requestMerge([0, 1]);
      expect(first.accepted).toBe(true);

      // Try to merge with same shard again
      const second = await multiCoordinator.requestMerge([0, 2]);
      expect(second.accepted).toBe(false);
      expect(second.rejectionReason).toMatch(/already involved in a merge/);
    });

    it('should respect minimum merge interval', async () => {
      const intervalCoordinator = createSplitCoordinator({
        router,
        monitor,
        minMergeIntervalMs: 60 * 60 * 1000, // 1 hour
      });

      // Wait for merge to complete
      const first = await intervalCoordinator.requestMerge([0, 1]);
      expect(first.accepted).toBe(true);

      // Let it complete
      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      // Try to merge same shard immediately
      const second = await intervalCoordinator.requestMerge([0, 2]);
      expect(second.accepted).toBe(false);
      expect(second.rejectionReason).toMatch(/Wait \d+ minutes/);
    });

    it('should create unique merge IDs', async () => {
      const multiCoordinator = createSplitCoordinator({
        router,
        monitor,
        maxConcurrentMerges: 10,
        minMergeIntervalMs: 0,
      });

      const result1 = await multiCoordinator.requestMerge([0, 1]);
      vi.advanceTimersByTime(100);
      const result2 = await multiCoordinator.requestMerge([2, 3]);

      expect(result1.mergeId).not.toBe(result2.mergeId);
    });

    it('should use first shard as target', async () => {
      const result = await coordinator.requestMerge([5, 3, 7]);

      const status = coordinator.getMergeStatus(result.mergeId!);
      expect(status!.targetShardId).toBe(5);
      expect(status!.sourceShardIds).toEqual([3, 7]);
    });
  });

  describe('merge lifecycle', () => {
    it('should transition through merge states', async () => {
      const stateChanges: string[] = [];

      const trackingCoordinator = createSplitCoordinator({
        router,
        monitor,
        minMergeIntervalMs: 0,
        onMergeStateChange: (op) => {
          stateChanges.push(op.state);
        },
      });

      await trackingCoordinator.requestMerge([0, 1]);

      // Let the async merge complete
      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      expect(stateChanges).toContain('pending');
      expect(stateChanges).toContain('preparing');
      expect(stateChanges).toContain('draining');
      expect(stateChanges).toContain('migrating');
      expect(stateChanges).toContain('validating');
      expect(stateChanges).toContain('completing');
      expect(stateChanges).toContain('completed');
    });

    it('should update progress during merge', async () => {
      const progressValues: number[] = [];

      const trackingCoordinator = createSplitCoordinator({
        router,
        monitor,
        minMergeIntervalMs: 0,
        onMergeStateChange: (op) => {
          progressValues.push(op.progress);
        },
      });

      await trackingCoordinator.requestMerge([0, 1]);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      // Progress should increase
      expect(progressValues[0]).toBe(0); // Initial
      expect(progressValues[progressValues.length - 1]).toBe(100); // Final

      // Should generally increase
      for (let i = 1; i < progressValues.length; i++) {
        expect(progressValues[i]).toBeGreaterThanOrEqual(progressValues[i - 1]);
      }
    });

    it('should call migrateDataForMerge if provided', async () => {
      const migratedCounts: Array<{ source: number; target: number }> = [];

      const migratingCoordinator = createSplitCoordinator({
        router,
        monitor,
        minMergeIntervalMs: 0,
        migrateDataForMerge: async (_op, sourceShard, targetShard) => {
          migratedCounts.push({ source: sourceShard, target: targetShard });
          return 100; // Migrated 100 documents
        },
      });

      const result = await migratingCoordinator.requestMerge([0, 1, 2]);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      // Should have migrated from each source shard to target
      expect(migratedCounts).toContainEqual({ source: 1, target: 0 });
      expect(migratedCounts).toContainEqual({ source: 2, target: 0 });

      const status = migratingCoordinator.getMergeStatus(result.mergeId!);
      expect(status!.migratedCount).toBe(200); // 100 from each source
    });

    it('should call drainShard if provided', async () => {
      const drainedShards: number[] = [];

      const drainingCoordinator = createSplitCoordinator({
        router,
        monitor,
        minMergeIntervalMs: 0,
        drainShard: async (shardId) => {
          drainedShards.push(shardId);
        },
      });

      await drainingCoordinator.requestMerge([0, 1, 2]);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      // Should have drained source shards (not target)
      expect(drainedShards).toContain(1);
      expect(drainedShards).toContain(2);
      expect(drainedShards).not.toContain(0); // Target is not drained
    });

    it('should call decommissionShard if provided', async () => {
      const decommissionedShards: number[] = [];

      const decommissioningCoordinator = createSplitCoordinator({
        router,
        monitor,
        minMergeIntervalMs: 0,
        decommissionShard: async (shardId) => {
          decommissionedShards.push(shardId);
        },
      });

      await decommissioningCoordinator.requestMerge([0, 1]);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      // Should have decommissioned source shards
      expect(decommissionedShards).toContain(1);
    });
  });

  describe('getMergeStatus', () => {
    it('should return status for active merge', async () => {
      const result = await coordinator.requestMerge([0, 1]);

      const status = coordinator.getMergeStatus(result.mergeId!);

      expect(status).toBeDefined();
      expect(status!.mergeId).toBe(result.mergeId);
      expect(status!.targetShardId).toBe(0);
    });

    it('should return status for completed merge', async () => {
      const result = await coordinator.requestMerge([0, 1]);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      const status = coordinator.getMergeStatus(result.mergeId!);

      expect(status).toBeDefined();
      expect(status!.state).toBe('completed');
    });

    it('should return undefined for unknown merge ID', () => {
      const status = coordinator.getMergeStatus('unknown_id');
      expect(status).toBeUndefined();
    });
  });

  describe('getActiveMerges', () => {
    it('should return empty array when no merges active', () => {
      const active = coordinator.getActiveMerges();
      expect(active).toHaveLength(0);
    });

    it('should return active merges', async () => {
      const multiCoordinator = createSplitCoordinator({
        router,
        monitor,
        maxConcurrentMerges: 10,
        minMergeIntervalMs: 0,
      });

      await multiCoordinator.requestMerge([0, 1]);
      await multiCoordinator.requestMerge([2, 3]);

      const active = multiCoordinator.getActiveMerges();
      expect(active.length).toBeGreaterThanOrEqual(1);
    });

    it('should not include completed merges', async () => {
      await coordinator.requestMerge([0, 1]);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      const active = coordinator.getActiveMerges();
      expect(active).toHaveLength(0);
    });
  });

  describe('getMergeHistory', () => {
    it('should return completed merges', async () => {
      await coordinator.requestMerge([0, 1]);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      const history = coordinator.getMergeHistory();
      expect(history.length).toBeGreaterThan(0);
      expect(history[0].state).toBe('completed');
    });

    it('should respect limit parameter', async () => {
      const multiCoordinator = createSplitCoordinator({
        router,
        monitor,
        maxConcurrentMerges: 10,
        minMergeIntervalMs: 0,
      });

      // Complete multiple merges
      for (let i = 0; i < 5; i++) {
        await multiCoordinator.requestMerge([i * 2, i * 2 + 1]);
        vi.advanceTimersByTime(1000);
        await vi.runAllTimersAsync();
      }

      const history = multiCoordinator.getMergeHistory(2);
      expect(history).toHaveLength(2);
    });
  });

  describe('cancelMerge', () => {
    it('should cancel a pending merge', async () => {
      const result = await coordinator.requestMerge([0, 1]);

      // Cancel immediately (before it completes)
      const cancelled = await coordinator.cancelMerge(result.mergeId!);

      expect(cancelled).toBe(true);
    });

    it('should return false for unknown merge ID', async () => {
      const cancelled = await coordinator.cancelMerge('unknown_id');
      expect(cancelled).toBe(false);
    });

    it('should not cancel completed merge', async () => {
      const result = await coordinator.requestMerge([0, 1]);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      const cancelled = await coordinator.cancelMerge(result.mergeId!);
      expect(cancelled).toBe(false);
    });
  });

  describe('checkMergeNeeded', () => {
    it('should return empty when not enough shards', () => {
      const smallMonitor = createShardMonitor({
        shardCount: 2,
        thresholds: { maxDocuments: 1000, sustainedThresholdMs: 0, checkIntervalMs: 0 },
      });

      const smallCoordinator = createSplitCoordinator({
        router,
        monitor: smallMonitor,
        lowUtilizationThreshold: 0.2,
      });

      const recommendations = smallCoordinator.checkMergeNeeded();
      expect(recommendations).toHaveLength(0);
    });

    it('should recommend merge for underutilized shards', () => {
      // Create fresh monitor with 4 shards
      const freshMonitor = createShardMonitor({
        shardCount: 4,
        thresholds: { maxDocuments: 1000000, sustainedThresholdMs: 0, checkIntervalMs: 0 },
      });

      // Shards 0 and 1 have significant data AND multiple writes
      // Shards 2 and 3 have very little data AND only 1 write each
      // This ensures both doc count AND write rate are low for shards 2,3
      //
      // Writes: shard 0 = 10 writes, shard 1 = 10 writes, shard 2 = 1 write, shard 3 = 1 write
      // Average write rate = (10 + 10 + 1 + 1) / 4 = 5.5
      // Shard 2,3 write util = 1 / 5.5 = 0.18 (< 0.2)
      for (let i = 0; i < 10; i++) {
        freshMonitor.recordWrite(0, 'users', 100, 1000);
        freshMonitor.recordWrite(1, 'users', 100, 1000);
      }
      freshMonitor.recordWrite(2, 'users', 10, 100);
      freshMonitor.recordWrite(3, 'users', 10, 100);

      // Need a router that matches the 4-shard count
      const freshRouter = createShardRouter({ shardCount: 4 });

      const lowUtilCoordinator = createSplitCoordinator({
        router: freshRouter,
        monitor: freshMonitor,
        lowUtilizationThreshold: 0.2,
      });

      const recommendations = lowUtilCoordinator.checkMergeNeeded();

      expect(recommendations.length).toBeGreaterThan(0);
      // Should recommend merging the underutilized shards (2 and 3)
      const rec = recommendations.find(r =>
        r.sourceShards.includes(2) || r.sourceShards.includes(3)
      );
      expect(rec).toBeDefined();
    });

    it('should not recommend merge if combined load too high', () => {
      // All shards have significant data - merging any two would exceed 1.5x average
      for (let i = 0; i < 16; i++) {
        monitor.recordWrite(i, 'users', 1000, 10000);
      }

      const recommendations = coordinator.checkMergeNeeded();

      // Should not recommend any merges
      expect(recommendations).toHaveLength(0);
    });

    it('should prioritize lower combined utilization', () => {
      // Create several underutilized shards with varying levels
      for (let i = 0; i < 12; i++) {
        monitor.recordWrite(i, 'users', 1000, 10000);
      }
      // Create pairs with different utilization levels
      monitor.recordWrite(12, 'users', 50, 500);  // Very low
      monitor.recordWrite(13, 'users', 50, 500);  // Very low
      monitor.recordWrite(14, 'users', 100, 1000); // Low
      monitor.recordWrite(15, 'users', 100, 1000); // Low

      const lowUtilCoordinator = createSplitCoordinator({
        router,
        monitor,
        lowUtilizationThreshold: 0.2,
      });

      const recommendations = lowUtilCoordinator.checkMergeNeeded();

      if (recommendations.length >= 2) {
        // First recommendation should have higher priority (lower combined utilization)
        expect(recommendations[0].priority).toBeGreaterThanOrEqual(recommendations[1].priority);
      }
    });
  });

  describe('executeMergeRecommendations', () => {
    it('should execute merge recommendations', async () => {
      // Create fresh monitor with 4 shards
      const freshMonitor = createShardMonitor({
        shardCount: 4,
        thresholds: { maxDocuments: 1000000, sustainedThresholdMs: 0, checkIntervalMs: 0 },
      });

      // Create underutilized shards - same as checkMergeNeeded test
      // Multiple writes to shards 0,1 to create write rate differential
      for (let i = 0; i < 10; i++) {
        freshMonitor.recordWrite(0, 'users', 100, 1000);
        freshMonitor.recordWrite(1, 'users', 100, 1000);
      }
      freshMonitor.recordWrite(2, 'users', 10, 100);
      freshMonitor.recordWrite(3, 'users', 10, 100);

      // Need a router that matches the 4-shard count
      const freshRouter = createShardRouter({ shardCount: 4 });

      const lowUtilCoordinator = createSplitCoordinator({
        router: freshRouter,
        monitor: freshMonitor,
        minMergeIntervalMs: 0,
        lowUtilizationThreshold: 0.2,
      });

      const results = await lowUtilCoordinator.executeMergeRecommendations();

      expect(results.length).toBeGreaterThan(0);
      expect(results[0].accepted).toBe(true);
    });
  });

  describe('getSummary with merge stats', () => {
    it('should include merge statistics in summary', async () => {
      await coordinator.requestMerge([0, 1]);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      const summary = coordinator.getSummary();

      expect(summary).toHaveProperty('activeMerges');
      expect(summary).toHaveProperty('completedMerges');
      expect(summary).toHaveProperty('failedMerges');
      expect(summary).toHaveProperty('totalShardsMerged');
      expect(summary).toHaveProperty('lastMergeTime');

      expect(summary.completedMerges).toBe(1);
      expect(summary.totalShardsMerged).toBe(1); // One source shard was merged
    });
  });

  describe('toJSON with merge data', () => {
    it('should include merge data in JSON output', async () => {
      await coordinator.requestMerge([0, 1]);

      const json = coordinator.toJSON();

      expect(json).toHaveProperty('activeMerges');
      expect(json).toHaveProperty('recentMerges');
      expect(json).toHaveProperty('mergeRecommendations');
    });
  });

  describe('error handling', () => {
    it('should mark merge as failed on error', async () => {
      const failingCoordinator = createSplitCoordinator({
        router,
        monitor,
        minMergeIntervalMs: 0,
        migrateDataForMerge: async () => {
          throw new Error('Migration failed');
        },
      });

      const result = await failingCoordinator.requestMerge([0, 1]);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      const status = failingCoordinator.getMergeStatus(result.mergeId!);

      expect(['failed', 'rolled_back']).toContain(status!.state);
      expect(status!.error).toBeDefined();
    });
  });

  describe('merge/split interaction', () => {
    it('should reject merge if shard is being split', async () => {
      const multiCoordinator = createSplitCoordinator({
        router,
        monitor,
        maxConcurrentSplits: 10,
        maxConcurrentMerges: 10,
        minSplitIntervalMs: 0,
        minMergeIntervalMs: 0,
      });

      // Start a split on shard 0
      const splitResult = await multiCoordinator.splitShard(0, 2);
      expect(splitResult.accepted).toBe(true);

      // Try to merge shard 0 - should be rejected
      const mergeResult = await multiCoordinator.requestMerge([0, 1]);
      expect(mergeResult.accepted).toBe(false);
      expect(mergeResult.rejectionReason).toMatch(/active split operation/);
    });

    it('should allow merge of different shards while split is active', async () => {
      const multiCoordinator = createSplitCoordinator({
        router,
        monitor,
        maxConcurrentSplits: 10,
        maxConcurrentMerges: 10,
        minSplitIntervalMs: 0,
        minMergeIntervalMs: 0,
      });

      // Start a split on shard 0
      await multiCoordinator.splitShard(0, 2);

      // Merge different shards - should succeed
      const mergeResult = await multiCoordinator.requestMerge([2, 3]);
      expect(mergeResult.accepted).toBe(true);
    });
  });

  describe('getMergeRecommendations', () => {
    it('should return merge recommendations without executing', () => {
      // Create fresh monitor with 4 shards
      const freshMonitor = createShardMonitor({
        shardCount: 4,
        thresholds: { maxDocuments: 1000000, sustainedThresholdMs: 0, checkIntervalMs: 0 },
      });

      // Create underutilized shards - same as checkMergeNeeded test
      // Multiple writes to shards 0,1 to create write rate differential
      for (let i = 0; i < 10; i++) {
        freshMonitor.recordWrite(0, 'users', 100, 1000);
        freshMonitor.recordWrite(1, 'users', 100, 1000);
      }
      freshMonitor.recordWrite(2, 'users', 10, 100);
      freshMonitor.recordWrite(3, 'users', 10, 100);

      // Need a router that matches the 4-shard count
      const freshRouter = createShardRouter({ shardCount: 4 });

      const lowUtilCoordinator = createSplitCoordinator({
        router: freshRouter,
        monitor: freshMonitor,
        lowUtilizationThreshold: 0.2,
      });

      const recommendations = lowUtilCoordinator.getMergeRecommendations();

      expect(recommendations.length).toBeGreaterThan(0);
      // Verify no merges were started
      expect(lowUtilCoordinator.getActiveMerges()).toHaveLength(0);
    });
  });
});
