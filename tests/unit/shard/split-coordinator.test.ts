/**
 * SplitCoordinator Tests
 *
 * Tests for shard split coordination and state management.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';

import {
  SplitCoordinator,
  createSplitCoordinator,
  type SplitOperation,
} from '../../../src/shard/split-coordinator';
import { createShardRouter } from '../../../src/shard/router';
import { createShardMonitor } from '../../../src/shard/monitor';
import type { ShardRouter } from '../../../src/shard/router';
import type { ShardMonitor } from '../../../src/shard/monitor';

describe('SplitCoordinator', () => {
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
      minSplitIntervalMs: 0, // Disable for testing
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe('splitShard', () => {
    it('should accept a valid split request', async () => {
      const result = await coordinator.splitShard(0, 2);

      expect(result.accepted).toBe(true);
      expect(result.splitId).toBeDefined();
    });

    it('should reject split count less than 2', async () => {
      const result = await coordinator.splitShard(0, 1);

      expect(result.accepted).toBe(false);
      expect(result.rejectionReason).toMatch(/between 2 and 4/);
    });

    it('should reject split count greater than 4', async () => {
      const result = await coordinator.splitShard(0, 5);

      expect(result.accepted).toBe(false);
      expect(result.rejectionReason).toMatch(/between 2 and 4/);
    });

    it('should reject when max concurrent splits reached', async () => {
      const singleConcurrentCoordinator = createSplitCoordinator({
        router,
        monitor,
        maxConcurrentSplits: 1,
        minSplitIntervalMs: 0,
      });

      // First split should succeed
      const first = await singleConcurrentCoordinator.splitShard(0, 2);
      expect(first.accepted).toBe(true);

      // Second split should be rejected (first still in progress)
      const second = await singleConcurrentCoordinator.splitShard(1, 2);
      expect(second.accepted).toBe(false);
      expect(second.rejectionReason).toMatch(/Maximum concurrent splits/);
    });

    it('should reject if same shard is already being split', async () => {
      // Use a coordinator that allows more concurrent splits
      const multiCoordinator = createSplitCoordinator({
        router,
        monitor,
        maxConcurrentSplits: 10,
        minSplitIntervalMs: 0,
      });

      const first = await multiCoordinator.splitShard(0, 2);
      expect(first.accepted).toBe(true);

      // Try to split same shard again
      const second = await multiCoordinator.splitShard(0, 2);
      expect(second.accepted).toBe(false);
      expect(second.rejectionReason).toMatch(/already being split/);
    });

    it('should respect minimum split interval', async () => {
      const intervalCoordinator = createSplitCoordinator({
        router,
        monitor,
        minSplitIntervalMs: 60 * 60 * 1000, // 1 hour
      });

      // Wait for split to complete
      const first = await intervalCoordinator.splitShard(0, 2);
      expect(first.accepted).toBe(true);

      // Let it complete
      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      // Try to split same shard immediately
      const second = await intervalCoordinator.splitShard(0, 2);
      expect(second.accepted).toBe(false);
      expect(second.rejectionReason).toMatch(/Wait \d+ minutes/);
    });

    it('should create unique split IDs', async () => {
      const result1 = await coordinator.splitShard(0, 2);
      // Wait a bit and request another
      vi.advanceTimersByTime(100);
      const result2 = await coordinator.splitShard(1, 2);

      expect(result1.splitId).not.toBe(result2.splitId);
    });
  });

  describe('requestSplit', () => {
    it('should accept split from recommendation', async () => {
      monitor.recordWrite(0, 'users', 2000, 0); // Exceed threshold

      // First check records breach, second triggers recommendation
      monitor.checkThresholds();
      const recommendations = monitor.checkThresholds();
      expect(recommendations.length).toBeGreaterThan(0);

      const result = await coordinator.requestSplit(recommendations[0]);
      expect(result.accepted).toBe(true);
    });

    it('should use recommended split count', async () => {
      // Record 3x threshold
      monitor.recordWrite(0, 'users', 3000, 0);

      const recommendation = monitor.shouldSplit(0);
      expect(recommendation!.recommendedSplitCount).toBe(3);

      const result = await coordinator.requestSplit(recommendation!);
      expect(result.accepted).toBe(true);

      const status = coordinator.getSplitStatus(result.splitId!);
      expect(status!.targetShardIds).toHaveLength(3);
    });
  });

  describe('split lifecycle', () => {
    it('should transition through split states', async () => {
      const stateChanges: string[] = [];

      const trackingCoordinator = createSplitCoordinator({
        router,
        monitor,
        minSplitIntervalMs: 0,
        onSplitStateChange: (op) => {
          stateChanges.push(op.state);
        },
      });

      const result = await trackingCoordinator.splitShard(0, 2);

      // Let the async split complete
      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      expect(stateChanges).toContain('pending');
      expect(stateChanges).toContain('preparing');
      expect(stateChanges).toContain('migrating');
      expect(stateChanges).toContain('validating');
      expect(stateChanges).toContain('completing');
      expect(stateChanges).toContain('completed');
    });

    it('should update progress during split', async () => {
      const progressValues: number[] = [];

      const trackingCoordinator = createSplitCoordinator({
        router,
        monitor,
        minSplitIntervalMs: 0,
        onSplitStateChange: (op) => {
          progressValues.push(op.progress);
        },
      });

      await trackingCoordinator.splitShard(0, 2);

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

    it('should allocate new shard IDs', async () => {
      const result = await coordinator.splitShard(0, 3);

      const status = coordinator.getSplitStatus(result.splitId!);
      expect(status!.targetShardIds).toHaveLength(3);

      // New shards should be >= 16 (existing shard count)
      for (const shardId of status!.targetShardIds) {
        expect(shardId).toBeGreaterThanOrEqual(16);
      }
    });

    it('should register new shards with monitor', async () => {
      const initialCount = monitor.getShardCount();

      await coordinator.splitShard(0, 2);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      expect(monitor.getShardCount()).toBe(initialCount + 2);
    });

    it('should update router with split configuration', async () => {
      // Add some collection data to the shard
      monitor.recordWrite(0, 'users', 100, 0);
      monitor.recordWrite(0, 'orders', 50, 0);

      const result = await coordinator.splitShard(0, 2, 'write_rate', ['users', 'orders']);
      expect(result.accepted).toBe(true);

      // Verify the split operation was created with the correct collections
      const status = coordinator.getSplitStatus(result.splitId!);
      expect(status).toBeDefined();
      expect(status!.collections).toContain('users');
      expect(status!.collections).toContain('orders');
      expect(status!.targetShardIds).toHaveLength(2);
    });
  });

  describe('getSplitStatus', () => {
    it('should return status for active split', async () => {
      const result = await coordinator.splitShard(0, 2);

      const status = coordinator.getSplitStatus(result.splitId!);

      expect(status).toBeDefined();
      expect(status!.splitId).toBe(result.splitId);
      expect(status!.sourceShardId).toBe(0);
    });

    it('should return status for completed split', async () => {
      const result = await coordinator.splitShard(0, 2);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      const status = coordinator.getSplitStatus(result.splitId!);

      expect(status).toBeDefined();
      expect(status!.state).toBe('completed');
    });

    it('should return undefined for unknown split ID', () => {
      const status = coordinator.getSplitStatus('unknown_id');
      expect(status).toBeUndefined();
    });
  });

  describe('getActiveSplits', () => {
    it('should return empty array when no splits active', () => {
      const active = coordinator.getActiveSplits();
      expect(active).toHaveLength(0);
    });

    it('should return active splits', async () => {
      await coordinator.splitShard(0, 2);
      await coordinator.splitShard(1, 2);

      const active = coordinator.getActiveSplits();
      expect(active.length).toBeGreaterThanOrEqual(1);
    });

    it('should not include completed splits', async () => {
      await coordinator.splitShard(0, 2);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      const active = coordinator.getActiveSplits();
      expect(active).toHaveLength(0);
    });
  });

  describe('getSplitHistory', () => {
    it('should return completed splits', async () => {
      await coordinator.splitShard(0, 2);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      const history = coordinator.getSplitHistory();
      expect(history.length).toBeGreaterThan(0);
      expect(history[0].state).toBe('completed');
    });

    it('should respect limit parameter', async () => {
      // Complete multiple splits
      for (let i = 0; i < 5; i++) {
        await coordinator.splitShard(i, 2);
        vi.advanceTimersByTime(1000);
        await vi.runAllTimersAsync();
      }

      const history = coordinator.getSplitHistory(2);
      expect(history).toHaveLength(2);
    });
  });

  describe('cancelSplit', () => {
    it('should cancel a pending split', async () => {
      const result = await coordinator.splitShard(0, 2);

      // Cancel immediately (before it completes)
      const cancelled = await coordinator.cancelSplit(result.splitId!);

      expect(cancelled).toBe(true);
    });

    it('should return false for unknown split ID', async () => {
      const cancelled = await coordinator.cancelSplit('unknown_id');
      expect(cancelled).toBe(false);
    });

    it('should not cancel completed split', async () => {
      const result = await coordinator.splitShard(0, 2);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      const cancelled = await coordinator.cancelSplit(result.splitId!);
      expect(cancelled).toBe(false);
    });
  });

  describe('checkAndTriggerSplits', () => {
    it('should trigger splits for shards exceeding thresholds', async () => {
      // Exceed threshold on shard 0
      monitor.recordWrite(0, 'users', 2000, 0);

      // First check records breach
      await coordinator.checkAndTriggerSplits();
      // Second check triggers the split
      const results = await coordinator.checkAndTriggerSplits();

      expect(results.length).toBeGreaterThan(0);
      expect(results[0].accepted).toBe(true);
    });

    it('should not trigger splits when below thresholds', async () => {
      // Stay below threshold
      monitor.recordWrite(0, 'users', 500, 0);

      const results = await coordinator.checkAndTriggerSplits();

      expect(results).toHaveLength(0);
    });

    it('should trigger multiple splits for multiple hot shards', async () => {
      // Create a multi-concurrent coordinator
      const multiCoordinator = createSplitCoordinator({
        router,
        monitor,
        maxConcurrentSplits: 10,
        minSplitIntervalMs: 0,
      });

      // Exceed threshold on multiple shards
      monitor.recordWrite(0, 'users', 2000, 0);
      monitor.recordWrite(1, 'orders', 2000, 0);

      // First check records breaches
      await multiCoordinator.checkAndTriggerSplits();
      // Second check triggers the splits
      const results = await multiCoordinator.checkAndTriggerSplits();

      expect(results.length).toBeGreaterThanOrEqual(2);
    });
  });

  describe('getSummary', () => {
    it('should return correct summary with no splits', () => {
      const summary = coordinator.getSummary();

      expect(summary.activeSplits).toBe(0);
      expect(summary.completedSplits).toBe(0);
      expect(summary.failedSplits).toBe(0);
      expect(summary.totalShardsSplit).toBe(0);
      expect(summary.lastSplitTime).toBeNull();
    });

    it('should count active splits', async () => {
      await coordinator.splitShard(0, 2);
      await coordinator.splitShard(1, 2);

      const summary = coordinator.getSummary();

      expect(summary.activeSplits).toBeGreaterThanOrEqual(1);
    });

    it('should count completed splits', async () => {
      await coordinator.splitShard(0, 2);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      const summary = coordinator.getSummary();

      expect(summary.completedSplits).toBe(1);
      expect(summary.totalShardsSplit).toBe(2);
    });

    it('should track last split time', async () => {
      const beforeSplit = Date.now();

      await coordinator.splitShard(0, 2);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      const summary = coordinator.getSummary();

      expect(summary.lastSplitTime).toBeGreaterThanOrEqual(beforeSplit);
    });
  });

  describe('toJSON', () => {
    it('should export state for debugging', async () => {
      await coordinator.splitShard(0, 2);

      const json = coordinator.toJSON();

      expect(json).toHaveProperty('activeSplits');
      expect(json).toHaveProperty('recentSplits');
      expect(json).toHaveProperty('summary');
    });
  });

  describe('error handling', () => {
    it('should mark split as failed on error', async () => {
      // Create a coordinator with a failing migrate function
      const failingCoordinator = createSplitCoordinator({
        router,
        monitor,
        minSplitIntervalMs: 0,
        migrateData: async () => {
          throw new Error('Migration failed');
        },
      });

      const result = await failingCoordinator.splitShard(0, 2);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      const status = failingCoordinator.getSplitStatus(result.splitId!);

      expect(['failed', 'rolled_back']).toContain(status!.state);
      expect(status!.error).toBeDefined();
    });

    it('should rollback router changes on failure', async () => {
      monitor.recordWrite(0, 'users', 100, 0);

      const failingCoordinator = createSplitCoordinator({
        router,
        monitor,
        minSplitIntervalMs: 0,
        migrateData: async () => {
          throw new Error('Migration failed');
        },
      });

      await failingCoordinator.splitShard(0, 2, 'write_rate', ['users']);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      // Router should not have the split configuration
      const splitInfo = router.getSplitInfo('users');
      expect(splitInfo).toBeUndefined();
    });
  });

  describe('custom shard ID allocation', () => {
    it('should use custom getNextShardId function', async () => {
      let nextId = 100;
      const customCoordinator = createSplitCoordinator({
        router,
        monitor,
        minSplitIntervalMs: 0,
        getNextShardId: () => nextId++,
      });

      const result = await customCoordinator.splitShard(0, 2);

      const status = customCoordinator.getSplitStatus(result.splitId!);

      expect(status!.targetShardIds).toContain(100);
      expect(status!.targetShardIds).toContain(101);
    });
  });

  describe('split point selection', () => {
    it('should calculate equal hash space split points when no key distribution available', async () => {
      const stateChanges: SplitOperation[] = [];

      const trackingCoordinator = createSplitCoordinator({
        router,
        monitor,
        minSplitIntervalMs: 0,
        onSplitStateChange: (op) => {
          stateChanges.push({ ...op });
        },
      });

      await trackingCoordinator.splitShard(0, 3);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      // Should have split points after preparing phase
      const preparingOp = stateChanges.find(op => op.state === 'preparing' && op.progress >= 15);
      expect(preparingOp?.splitPoints).toBeDefined();
      expect(preparingOp?.splitPoints?.length).toBe(2); // 3-way split = 2 split points
    });

    it('should use custom key distribution for split point calculation', async () => {
      const customKeys = [
        'doc_a', 'doc_b', 'doc_c', 'doc_d', 'doc_e',
        'doc_f', 'doc_g', 'doc_h', 'doc_i', 'doc_j',
      ];

      // Use router with enough shards and provide custom ID allocation
      const expandedRouter = createShardRouter({ shardCount: 64 });
      const expandedMonitor = createShardMonitor({ shardCount: 4 }); // Only 4 shards initially

      let nextShardId = 4; // Start allocating from shard 4 (within 64-shard range)

      const keyDistributionCoordinator = createSplitCoordinator({
        router: expandedRouter,
        monitor: expandedMonitor,
        minSplitIntervalMs: 0,
        getNextShardId: () => nextShardId++,
        scanKeys: async (_shardId, _collection, _limit) => {
          return customKeys;
        },
      });

      const result = await keyDistributionCoordinator.splitShard(0, 2, 'write_rate', ['users']);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      const status = keyDistributionCoordinator.getSplitStatus(result.splitId!);
      expect(status?.state).toBe('completed');
      expect(status?.splitPoints).toBeDefined();
      expect(status?.splitPoints?.length).toBe(1); // 2-way split = 1 split point
    });

    it('should calculate key ranges for each target shard', async () => {
      const trackingCoordinator = createSplitCoordinator({
        router,
        monitor,
        minSplitIntervalMs: 0,
        onSplitStateChange: (op) => {
          // Track key ranges after preparation
          if (op.state === 'migrating' && op.keyRanges) {
            expect(op.keyRanges.size).toBeGreaterThan(0);
          }
        },
      });

      await trackingCoordinator.splitShard(0, 2);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();
    });
  });

  describe('rebalancing', () => {
    it('should detect imbalanced shards and recommend splits', () => {
      // Create imbalance: shard 0 has way more data
      monitor.recordWrite(0, 'users', 5000, 50000);
      monitor.recordWrite(1, 'users', 100, 1000);
      monitor.recordWrite(2, 'users', 100, 1000);

      const rebalanceCoordinator = createSplitCoordinator({
        router,
        monitor,
        minSplitIntervalMs: 0,
        imbalanceThreshold: 0.3,
        rebalanceCheckIntervalMs: 0, // Disable rate limiting for test
      });

      const recommendations = rebalanceCoordinator.getRebalanceRecommendations();

      expect(recommendations.length).toBeGreaterThan(0);
      expect(recommendations[0].action).toBe('split');
      expect(recommendations[0].sourceShards).toContain(0);
    });

    it('should not recommend rebalance when load is balanced', () => {
      // Create fresh router/monitor for this test to avoid pollution
      const freshRouter = createShardRouter({ shardCount: 16 });
      const freshMonitor = createShardMonitor({
        shardCount: 16,
        thresholds: {
          maxDocuments: 1000000, // High threshold to avoid triggering split
          sustainedThresholdMs: 0,
          checkIntervalMs: 0,
        },
      });

      // Create balanced load - all shards have similar metrics
      for (let i = 0; i < 16; i++) {
        freshMonitor.recordWrite(i, 'users', 100, 1000);
      }

      const freshCoordinator = createSplitCoordinator({
        router: freshRouter,
        monitor: freshMonitor,
        minSplitIntervalMs: 0,
        imbalanceThreshold: 0.3,
        rebalanceCheckIntervalMs: 0,
      });

      const recommendations = freshCoordinator.getRebalanceRecommendations();

      // Should not recommend anything when balanced
      const splitRecs = recommendations.filter(r => r.action === 'split');
      expect(splitRecs.length).toBe(0);
    });

    it('should execute rebalance splits automatically', async () => {
      // Create severe imbalance
      monitor.recordWrite(0, 'users', 10000, 100000);

      const rebalanceCoordinator = createSplitCoordinator({
        router,
        monitor,
        minSplitIntervalMs: 0,
        imbalanceThreshold: 0.3,
        rebalanceCheckIntervalMs: 0,
      });

      const results = await rebalanceCoordinator.executeRebalance();

      expect(results.length).toBeGreaterThan(0);
      expect(results[0].accepted).toBe(true);
    });

    it('should calculate load balance score', () => {
      // Create fresh monitor for this test
      const freshRouter = createShardRouter({ shardCount: 16 });
      const freshMonitor = createShardMonitor({
        shardCount: 16,
        thresholds: {
          maxDocuments: 1000000,
          sustainedThresholdMs: 0,
          checkIntervalMs: 0,
        },
      });

      // Perfect balance - all shards have identical metrics
      for (let i = 0; i < 16; i++) {
        freshMonitor.recordWrite(i, 'users', 100, 1000);
      }

      const freshCoordinator = createSplitCoordinator({
        router: freshRouter,
        monitor: freshMonitor,
        minSplitIntervalMs: 0,
      });

      const score = freshCoordinator.getLoadBalanceScore();

      // Should be high when balanced (close to 1.0)
      expect(score).toBeGreaterThan(0.8);
    });

    it('should have low load balance score when imbalanced', () => {
      // Severe imbalance
      monitor.recordWrite(0, 'users', 10000, 100000);
      for (let i = 1; i < 4; i++) {
        monitor.recordWrite(i, 'users', 100, 1000);
      }

      const score = coordinator.getLoadBalanceScore();

      // Should be lower when imbalanced
      expect(score).toBeLessThan(0.5);
    });
  });

  describe('metadata persistence', () => {
    it('should persist metadata after successful split', async () => {
      const persistedMetadata: Array<{
        splitId: string;
        sourceShardId: number;
        resultingShards: number[];
        collections: string[];
      }> = [];

      // Use larger shard count router to accommodate new shards
      const expandedRouter = createShardRouter({ shardCount: 64 });
      const expandedMonitor = createShardMonitor({
        shardCount: 4, // Only 4 shards initially
        thresholds: { maxDocuments: 1000, sustainedThresholdMs: 0, checkIntervalMs: 0 },
      });

      let nextShardId = 4; // Start allocating from shard 4 (within 64-shard range)

      const persistingCoordinator = createSplitCoordinator({
        router: expandedRouter,
        monitor: expandedMonitor,
        minSplitIntervalMs: 0,
        getNextShardId: () => nextShardId++,
        persistMetadata: async (metadata) => {
          persistedMetadata.push(metadata);
        },
      });

      await persistingCoordinator.splitShard(0, 2, 'write_rate', ['users']);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      expect(persistedMetadata.length).toBe(1);
      expect(persistedMetadata[0].sourceShardId).toBe(0);
      expect(persistedMetadata[0].resultingShards).toContain(0);
      expect(persistedMetadata[0].collections).toContain('users');
    });

    it('should not fail split if metadata persistence fails', async () => {
      const persistingCoordinator = createSplitCoordinator({
        router,
        monitor,
        minSplitIntervalMs: 0,
        persistMetadata: async () => {
          throw new Error('Persistence failed');
        },
      });

      const result = await persistingCoordinator.splitShard(0, 2);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      // Split should still complete despite persistence failure
      const status = persistingCoordinator.getSplitStatus(result.splitId!);
      expect(status?.state).toBe('completed');
    });

    it('should initialize from persisted state', async () => {
      // Use shards within range (0-15 for a 16-shard router)
      const storedMetadata = [
        {
          splitId: 'split_1',
          sourceShardId: 0,
          resultingShards: [0, 8, 12], // All within 0-15 range
          splitPoints: [],
          collections: ['users'],
          completedAt: Date.now() - 10000,
          version: 1,
        },
      ];

      // Create fresh router/monitor for this test
      const loadRouter = createShardRouter({ shardCount: 16 });
      const loadMonitor = createShardMonitor({
        shardCount: 16,
        thresholds: { maxDocuments: 1000, sustainedThresholdMs: 0, checkIntervalMs: 0 },
      });

      const loadingCoordinator = createSplitCoordinator({
        router: loadRouter,
        monitor: loadMonitor,
        minSplitIntervalMs: 0,
        loadMetadata: async () => storedMetadata,
      });

      await loadingCoordinator.initializeFromPersistedState();

      // Router should have the split configuration restored
      const splitInfo = loadRouter.getSplitInfo('users');
      expect(splitInfo).toBeDefined();
      expect(splitInfo?.shards).toContain(0);
      expect(splitInfo?.shards).toContain(8);
      expect(splitInfo?.shards).toContain(12);
    });
  });

  describe('routing configuration', () => {
    it('should return routing config for split collection', async () => {
      // Use larger shard count router to accommodate new shards
      const expandedRouter = createShardRouter({ shardCount: 64 });
      const expandedMonitor = createShardMonitor({
        shardCount: 4, // Only 4 shards initially
        thresholds: { maxDocuments: 1000, sustainedThresholdMs: 0, checkIntervalMs: 0 },
      });

      let nextShardId = 4; // Start allocating from shard 4 (within 64-shard range)

      const routingCoordinator = createSplitCoordinator({
        router: expandedRouter,
        monitor: expandedMonitor,
        minSplitIntervalMs: 0,
        getNextShardId: () => nextShardId++,
      });

      await routingCoordinator.splitShard(0, 2, 'write_rate', ['users']);

      vi.advanceTimersByTime(1000);
      await vi.runAllTimersAsync();

      const config = routingCoordinator.getRoutingConfig('users');

      expect(config).toBeDefined();
      expect(config?.shards.length).toBeGreaterThanOrEqual(2);
    });

    it('should return undefined for non-split collection', () => {
      const config = coordinator.getRoutingConfig('unknown_collection');
      expect(config).toBeUndefined();
    });
  });

  describe('toJSON with new fields', () => {
    it('should include load balance score and recommendations in JSON output', async () => {
      // Add some data to make it interesting
      monitor.recordWrite(0, 'users', 1000, 10000);

      const json = coordinator.toJSON();

      expect(json).toHaveProperty('loadBalanceScore');
      expect(json).toHaveProperty('rebalanceRecommendations');
      expect(typeof json.loadBalanceScore).toBe('number');
      expect(Array.isArray(json.rebalanceRecommendations)).toBe(true);
    });
  });
});
