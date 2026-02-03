/**
 * ShardMonitor Tests
 *
 * Tests for shard metrics collection and threshold detection.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';

import {
  ShardMonitor,
  createShardMonitor,
  DEFAULT_SPLIT_THRESHOLDS,
  type SplitRecommendation,
} from '../../../src/shard/monitor';

describe('ShardMonitor', () => {
  let monitor: ShardMonitor;

  beforeEach(() => {
    vi.useFakeTimers();
    monitor = createShardMonitor({ shardCount: 16 });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe('initialization', () => {
    it('should initialize with correct shard count', () => {
      expect(monitor.getShardCount()).toBe(16);
    });

    it('should initialize metrics for all shards', () => {
      for (let i = 0; i < 16; i++) {
        const metrics = monitor.getShardMetrics(i);
        expect(metrics).toBeDefined();
        expect(metrics!.shardId).toBe(i);
        expect(metrics!.documentCount).toBe(0);
        expect(metrics!.sizeBytes).toBe(0);
        expect(metrics!.writeRate).toBe(0);
      }
    });

    it('should use default thresholds when not specified', () => {
      // Verify thresholds are applied by checking behavior
      // A shard with less than 1M docs should not trigger split
      monitor.recordWrite(0, 'users', 500_000, 1024 * 1024 * 1024);
      const recommendations = monitor.checkThresholds();
      expect(recommendations).toHaveLength(0);
    });

    it('should accept custom thresholds', () => {
      const customMonitor = createShardMonitor({
        shardCount: 4,
        thresholds: {
          maxDocuments: 100,
          sustainedThresholdMs: 0, // Immediate trigger
          checkIntervalMs: 0,
        },
      });

      customMonitor.recordWrite(0, 'users', 150, 1024);

      // First check records the breach time
      customMonitor.checkThresholds();
      // Second check detects sustained breach
      const recommendations = customMonitor.checkThresholds();
      expect(recommendations.length).toBeGreaterThan(0);
    });
  });

  describe('recordWrite', () => {
    it('should increment document count', () => {
      monitor.recordWrite(0, 'users', 10, 0);

      const metrics = monitor.getShardMetrics(0);
      expect(metrics!.documentCount).toBe(10);
    });

    it('should increment size bytes', () => {
      monitor.recordWrite(0, 'users', 1, 256);

      const metrics = monitor.getShardMetrics(0);
      expect(metrics!.sizeBytes).toBe(256);
    });

    it('should increment write count', () => {
      monitor.recordWrite(0, 'users', 1, 0);
      monitor.recordWrite(0, 'users', 1, 0);
      monitor.recordWrite(0, 'users', 1, 0);

      const metrics = monitor.getShardMetrics(0);
      expect(metrics!.writeCount).toBe(3);
    });

    it('should track per-collection metrics', () => {
      monitor.recordWrite(0, 'users', 5, 100);
      monitor.recordWrite(0, 'orders', 10, 200);
      monitor.recordWrite(0, 'users', 3, 50);

      const metrics = monitor.getShardMetrics(0);
      const usersMetrics = metrics!.collections.get('users');
      const ordersMetrics = metrics!.collections.get('orders');

      expect(usersMetrics!.documentCount).toBe(8);
      expect(usersMetrics!.sizeBytes).toBe(150);
      expect(ordersMetrics!.documentCount).toBe(10);
      expect(ordersMetrics!.sizeBytes).toBe(200);
    });

    it('should throw for unknown shard', () => {
      expect(() => monitor.recordWrite(99, 'users', 1, 0)).toThrow(/Unknown shard/);
    });

    it('should calculate write rate over time', () => {
      // Record 100 writes over 10 seconds
      for (let i = 0; i < 100; i++) {
        monitor.recordWrite(0, 'users', 1, 0);
        vi.advanceTimersByTime(100); // 100ms between each
      }

      const metrics = monitor.getShardMetrics(0);
      // ~100 writes over 10 seconds = ~10 writes/second
      expect(metrics!.writeRate).toBeGreaterThan(5);
      expect(metrics!.writeRate).toBeLessThan(15);
    });
  });

  describe('recordDelete', () => {
    it('should decrement document count', () => {
      monitor.recordWrite(0, 'users', 10, 1000);
      monitor.recordDelete(0, 'users', 3, 300);

      const metrics = monitor.getShardMetrics(0);
      expect(metrics!.documentCount).toBe(7);
      expect(metrics!.sizeBytes).toBe(700);
    });

    it('should not go below zero', () => {
      monitor.recordWrite(0, 'users', 5, 100);
      monitor.recordDelete(0, 'users', 10, 200);

      const metrics = monitor.getShardMetrics(0);
      expect(metrics!.documentCount).toBe(0);
      expect(metrics!.sizeBytes).toBe(0);
    });
  });

  describe('updateMetrics', () => {
    it('should update document count directly', () => {
      monitor.updateMetrics(0, { documentCount: 50000 });

      const metrics = monitor.getShardMetrics(0);
      expect(metrics!.documentCount).toBe(50000);
    });

    it('should update size bytes directly', () => {
      monitor.updateMetrics(0, { sizeBytes: 1024 * 1024 * 100 });

      const metrics = monitor.getShardMetrics(0);
      expect(metrics!.sizeBytes).toBe(1024 * 1024 * 100);
    });

    it('should update collection metrics', () => {
      monitor.updateMetrics(0, {
        collections: [
          { collection: 'users', documentCount: 1000, sizeBytes: 50000 },
          { collection: 'orders', documentCount: 2000, sizeBytes: 100000 },
        ],
      });

      const metrics = monitor.getShardMetrics(0);
      expect(metrics!.collections.get('users')!.documentCount).toBe(1000);
      expect(metrics!.collections.get('orders')!.documentCount).toBe(2000);
    });
  });

  describe('checkThresholds', () => {
    it('should return empty array when below thresholds', () => {
      monitor.recordWrite(0, 'users', 100, 1024);

      const recommendations = monitor.checkThresholds();
      expect(recommendations).toHaveLength(0);
    });

    it('should detect document count threshold breach', () => {
      const customMonitor = createShardMonitor({
        shardCount: 4,
        thresholds: {
          maxDocuments: 1000,
          sustainedThresholdMs: 0,
          checkIntervalMs: 0,
        },
      });

      customMonitor.recordWrite(0, 'users', 1500, 0);

      // First check records the breach time
      customMonitor.checkThresholds();
      // Second check detects sustained breach
      const recommendations = customMonitor.checkThresholds();
      expect(recommendations.length).toBeGreaterThan(0);
      expect(recommendations[0].reason).toBe('document_count');
      expect(recommendations[0].currentValue).toBe(1500);
      expect(recommendations[0].threshold).toBe(1000);
    });

    it('should detect size threshold breach', () => {
      const customMonitor = createShardMonitor({
        shardCount: 4,
        thresholds: {
          maxSizeBytes: 1024, // 1KB
          sustainedThresholdMs: 0,
          checkIntervalMs: 0,
        },
      });

      customMonitor.recordWrite(0, 'users', 1, 2048); // 2KB

      // First check records the breach time
      customMonitor.checkThresholds();
      // Second check detects sustained breach
      const recommendations = customMonitor.checkThresholds();
      expect(recommendations.length).toBeGreaterThan(0);
      expect(recommendations[0].reason).toBe('size');
    });

    it('should respect check interval', () => {
      const customMonitor = createShardMonitor({
        shardCount: 4,
        thresholds: {
          maxDocuments: 100,
          sustainedThresholdMs: 0,
          checkIntervalMs: 60000, // 1 minute
        },
      });

      customMonitor.recordWrite(0, 'users', 150, 0);

      // First check records breach time
      customMonitor.checkThresholds();

      // After interval (to pass rate limit), check should return recommendation
      vi.advanceTimersByTime(60001);
      const first = customMonitor.checkThresholds();
      expect(first.length).toBeGreaterThan(0);

      // Immediate second check should be rate-limited
      const second = customMonitor.checkThresholds();
      expect(second).toHaveLength(0);

      // After interval, check should work again
      vi.advanceTimersByTime(60001);
      const third = customMonitor.checkThresholds();
      expect(third.length).toBeGreaterThan(0);
    });

    it('should require sustained threshold breach', () => {
      const customMonitor = createShardMonitor({
        shardCount: 4,
        thresholds: {
          maxDocuments: 100,
          sustainedThresholdMs: 5000, // 5 seconds
          checkIntervalMs: 0,
        },
      });

      customMonitor.recordWrite(0, 'users', 150, 0);

      // First check - threshold just exceeded
      const first = customMonitor.checkThresholds();
      expect(first).toHaveLength(0); // Not sustained yet

      // After 3 seconds - still not sustained
      vi.advanceTimersByTime(3000);
      const second = customMonitor.checkThresholds();
      expect(second).toHaveLength(0);

      // After 5+ seconds - now sustained
      vi.advanceTimersByTime(3000);
      const third = customMonitor.checkThresholds();
      expect(third.length).toBeGreaterThan(0);
    });

    it('should reset sustained timer when threshold no longer exceeded', () => {
      const customMonitor = createShardMonitor({
        shardCount: 4,
        thresholds: {
          maxDocuments: 100,
          sustainedThresholdMs: 5000,
          checkIntervalMs: 0,
        },
      });

      // Exceed threshold
      customMonitor.recordWrite(0, 'users', 150, 0);
      customMonitor.checkThresholds();

      // Wait 3 seconds
      vi.advanceTimersByTime(3000);

      // Drop below threshold
      customMonitor.recordDelete(0, 'users', 100, 0);
      customMonitor.checkThresholds();

      // Exceed again
      customMonitor.recordWrite(0, 'users', 100, 0);

      // Wait 3 more seconds - should NOT trigger because timer reset
      vi.advanceTimersByTime(3000);
      const result = customMonitor.checkThresholds();
      expect(result).toHaveLength(0);
    });

    it('should call onSplitRecommended callback', () => {
      const callback = vi.fn();
      const customMonitor = createShardMonitor({
        shardCount: 4,
        thresholds: {
          maxDocuments: 100,
          sustainedThresholdMs: 0,
          checkIntervalMs: 0,
        },
        onSplitRecommended: callback,
      });

      customMonitor.recordWrite(0, 'users', 150, 0);
      // First check records breach time
      customMonitor.checkThresholds();
      // Second check detects sustained breach and triggers callback
      customMonitor.checkThresholds();

      expect(callback).toHaveBeenCalledTimes(1);
      expect(callback).toHaveBeenCalledWith(
        expect.objectContaining({
          shardId: 0,
          reason: 'document_count',
        })
      );
    });
  });

  describe('shouldSplit', () => {
    it('should return null when below all thresholds', () => {
      const result = monitor.shouldSplit(0);
      expect(result).toBeNull();
    });

    it('should return recommendation when above threshold', () => {
      const customMonitor = createShardMonitor({
        shardCount: 4,
        thresholds: { maxDocuments: 100 },
      });

      customMonitor.recordWrite(0, 'users', 150, 0);

      const result = customMonitor.shouldSplit(0);
      expect(result).not.toBeNull();
      expect(result!.shardId).toBe(0);
      expect(result!.reason).toBe('document_count');
    });

    it('should calculate recommended split count based on ratio', () => {
      const customMonitor = createShardMonitor({
        shardCount: 4,
        thresholds: { maxDocuments: 100 },
      });

      // 3x threshold = recommend 3-way split
      customMonitor.recordWrite(0, 'users', 300, 0);

      const result = customMonitor.shouldSplit(0);
      expect(result!.recommendedSplitCount).toBe(3);
    });

    it('should cap recommended split count at 4', () => {
      const customMonitor = createShardMonitor({
        shardCount: 4,
        thresholds: { maxDocuments: 100 },
      });

      // 10x threshold - should still cap at 4
      customMonitor.recordWrite(0, 'users', 1000, 0);

      const result = customMonitor.shouldSplit(0);
      expect(result!.recommendedSplitCount).toBe(4);
    });

    it('should include hot collections in recommendation', () => {
      const customMonitor = createShardMonitor({
        shardCount: 4,
        thresholds: { maxDocuments: 100 },
      });

      // Write to multiple collections - orders has most write operations (3 calls)
      customMonitor.recordWrite(0, 'users', 50, 0); // 1 write op
      customMonitor.recordWrite(0, 'orders', 30, 0); // 1 write op
      customMonitor.recordWrite(0, 'orders', 30, 0); // 2 write ops total
      customMonitor.recordWrite(0, 'orders', 40, 0); // 3 write ops total (100 docs)
      customMonitor.recordWrite(0, 'products', 25, 0); // 1 write op

      const result = customMonitor.shouldSplit(0);
      expect(result!.hotCollections).toContain('orders');
      expect(result!.hotCollections[0]).toBe('orders'); // Should be first (most write operations)
    });
  });

  describe('getHotCollections', () => {
    it('should return collections sorted by write count', () => {
      monitor.recordWrite(0, 'users', 1, 0);
      monitor.recordWrite(0, 'users', 1, 0);
      monitor.recordWrite(0, 'orders', 1, 0);
      monitor.recordWrite(0, 'orders', 1, 0);
      monitor.recordWrite(0, 'orders', 1, 0);
      monitor.recordWrite(0, 'products', 1, 0);

      const hot = monitor.getHotCollections(0);

      expect(hot[0].collection).toBe('orders');
      expect(hot[1].collection).toBe('users');
      expect(hot[2].collection).toBe('products');
    });

    it('should respect limit parameter', () => {
      monitor.recordWrite(0, 'a', 1, 0);
      monitor.recordWrite(0, 'b', 1, 0);
      monitor.recordWrite(0, 'c', 1, 0);
      monitor.recordWrite(0, 'd', 1, 0);
      monitor.recordWrite(0, 'e', 1, 0);

      const hot = monitor.getHotCollections(0, 2);
      expect(hot).toHaveLength(2);
    });

    it('should return empty array for unknown shard', () => {
      const hot = monitor.getHotCollections(99);
      expect(hot).toHaveLength(0);
    });
  });

  describe('addShard', () => {
    it('should add a new shard', () => {
      monitor.addShard(16);

      expect(monitor.getShardCount()).toBe(17);
      expect(monitor.getShardMetrics(16)).toBeDefined();
    });

    it('should throw if shard already exists', () => {
      expect(() => monitor.addShard(0)).toThrow(/already exists/);
    });

    it('should initialize new shard with zero metrics', () => {
      monitor.addShard(20);

      const metrics = monitor.getShardMetrics(20);
      expect(metrics!.documentCount).toBe(0);
      expect(metrics!.sizeBytes).toBe(0);
      expect(metrics!.writeRate).toBe(0);
    });
  });

  describe('resetShardMetrics', () => {
    it('should reset all metrics for a shard', () => {
      monitor.recordWrite(0, 'users', 100, 1000);
      monitor.recordWrite(0, 'orders', 50, 500);

      monitor.resetShardMetrics(0);

      const metrics = monitor.getShardMetrics(0);
      expect(metrics!.documentCount).toBe(0);
      expect(metrics!.sizeBytes).toBe(0);
      expect(metrics!.writeCount).toBe(0);
      expect(metrics!.collections.size).toBe(0);
    });
  });

  describe('getSummary', () => {
    it('should return correct totals', () => {
      monitor.recordWrite(0, 'users', 100, 1000);
      monitor.recordWrite(1, 'users', 200, 2000);
      monitor.recordWrite(2, 'orders', 50, 500);

      const summary = monitor.getSummary();

      expect(summary.totalShards).toBe(16);
      expect(summary.totalDocuments).toBe(350);
      expect(summary.totalSizeBytes).toBe(3500);
    });

    it('should calculate averages correctly', () => {
      monitor.recordWrite(0, 'users', 160, 1600);

      const summary = monitor.getSummary();

      expect(summary.avgDocumentsPerShard).toBe(10); // 160 / 16
      expect(summary.avgSizePerShard).toBe(100); // 1600 / 16
    });

    it('should identify hot shards', () => {
      const customMonitor = createShardMonitor({
        shardCount: 4,
        thresholds: { maxDocuments: 100 },
      });

      // Shard 1 is at 85% of threshold
      customMonitor.recordWrite(1, 'users', 85, 0);

      const summary = customMonitor.getSummary();

      expect(summary.hotShards).toContain(1);
      expect(summary.hotShards).not.toContain(0);
    });
  });

  describe('getAllMetrics', () => {
    it('should return metrics for all shards', () => {
      const allMetrics = monitor.getAllMetrics();
      expect(allMetrics).toHaveLength(16);
    });

    it('should include updated metrics', () => {
      monitor.recordWrite(5, 'users', 42, 420);

      const allMetrics = monitor.getAllMetrics();
      const shard5 = allMetrics.find(m => m.shardId === 5);

      expect(shard5!.documentCount).toBe(42);
    });
  });

  describe('toJSON', () => {
    it('should export metrics in JSON format', () => {
      monitor.recordWrite(0, 'users', 10, 100);

      const json = monitor.toJSON();

      expect(json).toHaveProperty('shardCount', 16);
      expect(json).toHaveProperty('thresholds');
      expect(json).toHaveProperty('metrics');
      expect(json).toHaveProperty('summary');
      expect(Array.isArray(json.metrics)).toBe(true);
    });

    it('should serialize collection data', () => {
      monitor.recordWrite(0, 'users', 10, 100);

      const json = monitor.toJSON();
      const metricsArray = json.metrics as Array<{ collections: unknown[] }>;

      expect(metricsArray[0].collections).toBeDefined();
      expect(Array.isArray(metricsArray[0].collections)).toBe(true);
    });
  });

  describe('write rate calculation', () => {
    it('should handle window cleanup', () => {
      // Write 50 entries
      for (let i = 0; i < 50; i++) {
        monitor.recordWrite(0, 'users', 1, 0);
      }

      // Advance past the window
      vi.advanceTimersByTime(120_000); // 2 minutes

      // Write 10 more
      for (let i = 0; i < 10; i++) {
        monitor.recordWrite(0, 'users', 1, 0);
      }

      const metrics = monitor.getShardMetrics(0);

      // Rate should be based only on recent writes
      expect(metrics!.writeCount).toBe(60); // Total writes
      // But rate should only consider last 10 in window
      expect(metrics!.writeRate).toBeLessThan(20);
    });
  });
});
