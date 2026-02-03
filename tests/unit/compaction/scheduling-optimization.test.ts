/**
 * Compaction Scheduling Optimization Tests (TDD RED Phase)
 *
 * Tests for advanced scheduling optimizations including:
 * 1. Optimal timing for compaction
 * 2. Load-based scheduling
 * 3. Priority queue for compaction tasks
 * 4. Adaptive thresholds based on workload
 * 5. Predictive triggering based on write patterns
 * 6. Resource-aware scheduling
 * 7. Backpressure handling
 *
 * These tests follow TDD RED phase - they define expected behavior
 * for features not yet implemented.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  CompactionScheduler,
  type BlockMetadata,
} from '../../../src/compaction/scheduler.js';
import { MemoryStorage } from '../../../src/storage/index.js';

// ============================================================================
// Test Helpers
// ============================================================================

interface TestBlock extends BlockMetadata {
  id: string;
  path: string;
  size: number;
  rowCount: number;
  minSeq: number;
  maxSeq: number;
  createdAt: Date;
}

function createBlock(
  id: string,
  size: number,
  options: Partial<Omit<TestBlock, 'id' | 'size'>> = {}
): TestBlock {
  return {
    id,
    path: `blocks/${id}.parquet`,
    size,
    rowCount: options.rowCount ?? Math.floor(size / 100),
    minSeq: options.minSeq ?? 1,
    maxSeq: options.maxSeq ?? options.rowCount ?? Math.floor(size / 100),
    createdAt: options.createdAt ?? new Date(),
  };
}

function createSmallBlock(id: string, size: number = 500_000): TestBlock {
  return createBlock(id, size);
}

// ============================================================================
// 1. Optimal Timing for Compaction
// ============================================================================

describe('CompactionScheduler - Optimal Timing', () => {
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it.todo('should defer compaction during peak write activity');

  it.fails('should identify low-activity windows for compaction', async () => {
    // Feature: Scheduler should analyze activity patterns and
    // identify optimal windows (e.g., off-peak hours) for compaction
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // activityAnalyzer: true,
    });

    // Simulate activity history
    const activityHistory = [
      { hour: 0, writeRate: 10 },   // Low
      { hour: 6, writeRate: 50 },   // Medium
      { hour: 12, writeRate: 200 }, // Peak
      { hour: 18, writeRate: 100 }, // High
    ];

    // Should identify hour 0 as optimal compaction window
    const optimalWindow = (scheduler as any).identifyOptimalWindow?.(activityHistory);
    expect(optimalWindow).toBeDefined();
    expect(optimalWindow.hour).toBe(0);
  });

  it.fails('should schedule compaction for identified low-activity windows', async () => {
    // Feature: Once an optimal window is identified, schedule compaction
    // to run during that window rather than immediately
    const mockAlarmScheduler = { schedule: vi.fn() };
    const scheduler = new CompactionScheduler({
      storage,
      alarmScheduler: mockAlarmScheduler,
      // Expected future API:
      // useOptimalTiming: true,
    });

    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    // Mock current time as peak hour (12:00)
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2024-01-15T12:00:00Z'));

    await scheduler.runCompaction(blocks);

    // Should defer to low-activity window instead of immediate scheduling
    const scheduledCall = mockAlarmScheduler.schedule.mock.calls[0]?.[0];
    expect(scheduledCall?.deferToWindow).toBe(true);
  });

  it.fails('should respect minimum interval between compaction runs', async () => {
    // Feature: Prevent compaction from running too frequently
    // even if conditions are met
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // minCompactionInterval: 60_000, // 1 minute minimum
    });

    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    // First run should succeed
    const result1 = await scheduler.runCompaction(blocks);
    expect(result1.skipped).toBe(false);

    // Create new blocks for second run
    const newBlocks: TestBlock[] = [
      createSmallBlock('block-3'),
      createSmallBlock('block-4'),
    ];
    for (const block of newBlocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    // Immediate second run should be deferred
    const result2 = await scheduler.runCompaction(newBlocks);
    expect(result2.skipped).toBe(true);
    expect(result2.reason).toMatch(/minimum.*interval/i);
  });

  it.fails('should track compaction timing statistics', async () => {
    // Feature: Track historical timing data to improve scheduling decisions
    const scheduler = new CompactionScheduler({ storage });

    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    await scheduler.runCompaction(blocks);

    // Should track when compaction ran and how long it took
    const timingStats = (scheduler as any).getTimingStatistics?.();
    expect(timingStats).toBeDefined();
    expect(timingStats.lastRunTime).toBeDefined();
    expect(timingStats.averageDuration).toBeGreaterThan(0);
    expect(timingStats.runsPerHour).toBeDefined();
  });
});

// ============================================================================
// 2. Load-Based Scheduling
// ============================================================================

describe('CompactionScheduler - Load-Based Scheduling', () => {
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
  });

  it.fails('should monitor current system load', async () => {
    // Feature: Scheduler should be aware of current system load
    // (CPU, memory, I/O) to make informed scheduling decisions
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // loadMonitor: mockLoadMonitor,
    });

    const loadMetrics = (scheduler as any).getCurrentLoad?.();
    expect(loadMetrics).toBeDefined();
    expect(loadMetrics).toHaveProperty('cpuUsage');
    expect(loadMetrics).toHaveProperty('memoryUsage');
    expect(loadMetrics).toHaveProperty('ioUtilization');
  });

  it.fails('should throttle compaction under high load', async () => {
    // Feature: When system load is high, reduce compaction intensity
    const mockLoadMonitor = {
      getCpuUsage: () => 0.9, // 90% CPU usage
      getMemoryUsage: () => 0.85,
      getIoUtilization: () => 0.7,
    };

    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // loadMonitor: mockLoadMonitor,
      // loadThreshold: { cpu: 0.8, memory: 0.8, io: 0.8 },
    });

    const blocks: TestBlock[] = Array.from({ length: 20 }, (_, i) =>
      createSmallBlock(`block-${i}`, 300_000)
    );

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.runCompaction(blocks);

    // Should process fewer blocks due to high load
    expect(result.processedBlocks).toBeLessThan(10);
    expect(result.stats).toHaveProperty('throttledDueToLoad', true);
  });

  it.fails('should increase compaction throughput under low load', async () => {
    // Feature: When system load is low, increase compaction batch size
    const mockLoadMonitor = {
      getCpuUsage: () => 0.2, // 20% CPU usage
      getMemoryUsage: () => 0.3,
      getIoUtilization: () => 0.1,
    };

    const scheduler = new CompactionScheduler({
      storage,
      maxBlocksPerRun: 10,
      // Expected future API:
      // loadMonitor: mockLoadMonitor,
      // adaptiveBatchSize: true,
    });

    const blocks: TestBlock[] = Array.from({ length: 30 }, (_, i) =>
      createSmallBlock(`block-${i}`, 300_000)
    );

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.runCompaction(blocks);

    // Should process more blocks due to low load (up to 2x base)
    expect(result.processedBlocks).toBeGreaterThan(10);
  });

  it.fails('should pause compaction when load exceeds critical threshold', async () => {
    // Feature: Completely pause compaction when system is overwhelmed
    const mockLoadMonitor = {
      getCpuUsage: () => 0.98, // Critical CPU usage
      getMemoryUsage: () => 0.95,
      getIoUtilization: () => 0.95,
    };

    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // loadMonitor: mockLoadMonitor,
      // criticalLoadThreshold: 0.95,
    });

    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.runCompaction(blocks);

    expect(result.skipped).toBe(true);
    expect(result.reason).toMatch(/critical.*load/i);
  });

  it.fails('should gradually resume compaction as load decreases', async () => {
    // Feature: After pausing due to high load, gradually resume
    // rather than immediately processing at full capacity
    let cpuUsage = 0.95;
    const mockLoadMonitor = {
      getCpuUsage: () => cpuUsage,
      getMemoryUsage: () => 0.5,
      getIoUtilization: () => 0.5,
    };

    const scheduler = new CompactionScheduler({
      storage,
      maxBlocksPerRun: 10,
      // Expected future API:
      // loadMonitor: mockLoadMonitor,
      // gradualResume: true,
    });

    const blocks: TestBlock[] = Array.from({ length: 20 }, (_, i) =>
      createSmallBlock(`block-${i}`, 300_000)
    );

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    // First run at high load - should skip or minimal processing
    const result1 = await scheduler.runCompaction(blocks);
    expect(result1.processedBlocks).toBeLessThanOrEqual(2);

    // Load decreases
    cpuUsage = 0.7;

    // Second run - should process more but not full capacity
    const result2 = await scheduler.runCompaction(blocks);
    expect(result2.processedBlocks).toBeGreaterThan(result1.processedBlocks);
    expect(result2.processedBlocks).toBeLessThan(10);
  });
});

// ============================================================================
// 3. Priority Queue for Compaction Tasks
// ============================================================================

describe('CompactionScheduler - Priority Queue', () => {
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
  });

  it.fails('should prioritize collections with most small blocks', async () => {
    // Feature: When multiple collections need compaction,
    // prioritize the one with the most small blocks
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // priorityMode: 'block-count',
    });

    const collectionA = Array.from({ length: 5 }, (_, i) =>
      createSmallBlock(`coll-a/block-${i}`, 300_000)
    );
    const collectionB = Array.from({ length: 20 }, (_, i) =>
      createSmallBlock(`coll-b/block-${i}`, 300_000)
    );

    // Enqueue both collections
    const priority = (scheduler as any).calculatePriority?.([
      { collection: 'a', blocks: collectionA },
      { collection: 'b', blocks: collectionB },
    ]);

    expect(priority[0].collection).toBe('b'); // More blocks = higher priority
  });

  it.fails('should prioritize collections with higher write activity', async () => {
    // Feature: Collections with higher write rates should be
    // compacted more frequently to maintain read performance
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // priorityMode: 'write-activity',
    });

    const collections = [
      { collection: 'low-activity', writeRate: 10, blocks: 5 },
      { collection: 'high-activity', writeRate: 1000, blocks: 5 },
    ];

    const priority = (scheduler as any).calculatePriority?.(collections);

    expect(priority[0].collection).toBe('high-activity');
  });

  it.fails('should support custom priority functions', async () => {
    // Feature: Allow users to define custom priority calculation
    const customPriorityFn = (task: { blocks: TestBlock[]; collection: string }) => {
      // Prioritize by total bytes (larger total = higher priority)
      return task.blocks.reduce((sum, b) => sum + b.size, 0);
    };

    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // priorityFunction: customPriorityFn,
    });

    const collectionA = [
      createSmallBlock('coll-a/block-1', 100_000),
      createSmallBlock('coll-a/block-2', 100_000),
    ]; // Total: 200KB

    const collectionB = [
      createSmallBlock('coll-b/block-1', 500_000),
      createSmallBlock('coll-b/block-2', 500_000),
    ]; // Total: 1MB

    const priority = (scheduler as any).calculatePriority?.([
      { collection: 'a', blocks: collectionA },
      { collection: 'b', blocks: collectionB },
    ]);

    expect(priority[0].collection).toBe('b'); // Larger total size
  });

  it.fails('should maintain priority queue across scheduler runs', async () => {
    // Feature: Priority queue state should persist across runs
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // persistPriorityQueue: true,
    });

    // Enqueue tasks
    await (scheduler as any).enqueueCompaction?.({
      collection: 'test-collection',
      blocks: [createSmallBlock('block-1'), createSmallBlock('block-2')],
      priority: 10,
    });

    // Get queue state
    const queueState = (scheduler as any).getQueueState?.();
    expect(queueState.pendingTasks).toBe(1);
    expect(queueState.tasks[0].collection).toBe('test-collection');
  });

  it.fails('should support task aging to prevent starvation', async () => {
    // Feature: Lower priority tasks should eventually be processed
    // by increasing their priority over time
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // taskAgingEnabled: true,
      // agingIntervalMs: 60_000,
      // agingIncrement: 1,
    });

    // Enqueue low priority task
    await (scheduler as any).enqueueCompaction?.({
      collection: 'low-priority',
      blocks: [createSmallBlock('block-1')],
      priority: 1,
      enqueuedAt: Date.now() - 300_000, // 5 minutes ago
    });

    // After aging, priority should have increased
    const queueState = (scheduler as any).getQueueState?.();
    expect(queueState.tasks[0].effectivePriority).toBeGreaterThan(1);
  });

  it.fails('should dequeue tasks by priority', async () => {
    // Feature: Tasks should be dequeued in priority order
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // usePriorityQueue: true,
    });

    // Enqueue tasks in random order
    await (scheduler as any).enqueueCompaction?.({
      collection: 'medium',
      priority: 5,
    });
    await (scheduler as any).enqueueCompaction?.({
      collection: 'high',
      priority: 10,
    });
    await (scheduler as any).enqueueCompaction?.({
      collection: 'low',
      priority: 1,
    });

    // Dequeue should return highest priority first
    const task1 = await (scheduler as any).dequeueCompaction?.();
    expect(task1.collection).toBe('high');

    const task2 = await (scheduler as any).dequeueCompaction?.();
    expect(task2.collection).toBe('medium');

    const task3 = await (scheduler as any).dequeueCompaction?.();
    expect(task3.collection).toBe('low');
  });
});

// ============================================================================
// 4. Adaptive Thresholds Based on Workload
// ============================================================================

describe('CompactionScheduler - Adaptive Thresholds', () => {
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
  });

  it.fails('should adjust min block size threshold based on write patterns', async () => {
    // Feature: For high-write workloads, increase threshold to reduce
    // compaction frequency; for low-write, decrease to keep blocks optimal
    const scheduler = new CompactionScheduler({
      storage,
      minBlockSize: 2_000_000, // Base 2MB
      // Expected future API:
      // adaptiveThresholds: true,
    });

    // Simulate high write activity
    await (scheduler as any).recordWriteActivity?.(10_000); // 10k writes/sec

    const adjustedThreshold = (scheduler as any).getEffectiveMinBlockSize?.();
    expect(adjustedThreshold).toBeGreaterThan(2_000_000); // Should increase
  });

  it.fails('should decrease threshold during low activity periods', async () => {
    // Feature: During low activity, be more aggressive about compaction
    const scheduler = new CompactionScheduler({
      storage,
      minBlockSize: 2_000_000,
      // Expected future API:
      // adaptiveThresholds: true,
    });

    // Simulate low write activity
    await (scheduler as any).recordWriteActivity?.(10); // 10 writes/sec

    const adjustedThreshold = (scheduler as any).getEffectiveMinBlockSize?.();
    expect(adjustedThreshold).toBeLessThan(2_000_000); // Should decrease
  });

  it.fails('should adapt max blocks per run based on block sizes', async () => {
    // Feature: Process fewer blocks if they are larger, more if smaller
    const scheduler = new CompactionScheduler({
      storage,
      maxBlocksPerRun: 10,
      // Expected future API:
      // adaptiveBatchSize: true,
    });

    const largeBlocks: TestBlock[] = Array.from({ length: 10 }, (_, i) =>
      createSmallBlock(`block-${i}`, 1_500_000) // 1.5MB each
    );

    const smallBlocks: TestBlock[] = Array.from({ length: 10 }, (_, i) =>
      createSmallBlock(`block-${i}`, 200_000) // 200KB each
    );

    const largeBlockBatchSize = (scheduler as any).calculateAdaptiveBatchSize?.(largeBlocks);
    const smallBlockBatchSize = (scheduler as any).calculateAdaptiveBatchSize?.(smallBlocks);

    expect(smallBlockBatchSize).toBeGreaterThan(largeBlockBatchSize);
  });

  it.fails('should learn optimal thresholds from historical performance', async () => {
    // Feature: Use ML/statistics to learn optimal threshold values
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // learningEnabled: true,
    });

    // Record historical performance data
    await (scheduler as any).recordCompactionPerformance?.({
      minBlockSize: 2_000_000,
      processedBlocks: 10,
      durationMs: 500,
      readLatencyImpact: 0.1, // 10% read latency increase
    });

    await (scheduler as any).recordCompactionPerformance?.({
      minBlockSize: 1_500_000,
      processedBlocks: 15,
      durationMs: 800,
      readLatencyImpact: 0.05, // 5% read latency increase
    });

    // Should recommend threshold that minimizes latency impact
    const recommendedThreshold = (scheduler as any).getRecommendedThreshold?.();
    expect(recommendedThreshold).toBe(1_500_000);
  });

  it.fails('should respect threshold bounds', async () => {
    // Feature: Adaptive thresholds should stay within reasonable bounds
    const scheduler = new CompactionScheduler({
      storage,
      minBlockSize: 2_000_000,
      // Expected future API:
      // adaptiveThresholds: true,
      // minBlockSizeBounds: { min: 500_000, max: 10_000_000 },
    });

    // Extreme write activity
    await (scheduler as any).recordWriteActivity?.(1_000_000);

    const adjustedThreshold = (scheduler as any).getEffectiveMinBlockSize?.();
    expect(adjustedThreshold).toBeLessThanOrEqual(10_000_000);
    expect(adjustedThreshold).toBeGreaterThanOrEqual(500_000);
  });

  it.fails('should adapt compaction frequency based on query patterns', async () => {
    // Feature: If read queries frequently hit small blocks,
    // increase compaction frequency
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // queryPatternAware: true,
    });

    // Record query patterns showing many small block hits
    await (scheduler as any).recordQueryPattern?.({
      blocksAccessed: ['small-block-1', 'small-block-2', 'small-block-3'],
      smallBlockHitRate: 0.8, // 80% of queries hit small blocks
    });

    const compactionUrgency = (scheduler as any).getCompactionUrgency?.();
    expect(compactionUrgency).toBe('high');
  });
});

// ============================================================================
// 5. Predictive Triggering Based on Write Patterns
// ============================================================================

describe('CompactionScheduler - Predictive Triggering', () => {
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
  });

  it.fails('should predict upcoming write surge from historical patterns', async () => {
    // Feature: Analyze historical write patterns to predict surges
    // and proactively compact before they happen
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // predictiveScheduling: true,
    });

    // Record pattern: daily surge at 9 AM
    const history = Array.from({ length: 7 }, (_, day) => ({
      timestamp: new Date(`2024-01-${15 + day}T09:00:00Z`),
      writeRate: 5000,
    }));

    for (const entry of history) {
      await (scheduler as any).recordWritePattern?.(entry);
    }

    // At 8 AM, should predict upcoming surge
    const prediction = (scheduler as any).predictWriteActivity?.(
      new Date('2024-01-22T08:00:00Z')
    );

    expect(prediction.expectedSurge).toBe(true);
    expect(prediction.surgeTime.getHours()).toBe(9);
  });

  it.fails('should trigger preemptive compaction before predicted surge', async () => {
    // Feature: Automatically trigger compaction before predicted high-activity
    const mockAlarmScheduler = { schedule: vi.fn() };
    const scheduler = new CompactionScheduler({
      storage,
      alarmScheduler: mockAlarmScheduler,
      // Expected future API:
      // preemptiveCompaction: true,
      // predictionHorizon: 3600_000, // 1 hour ahead
    });

    // Set up prediction for surge in 1 hour
    (scheduler as any).setPrediction?.({
      surgeTime: new Date(Date.now() + 3600_000),
      expectedWriteRate: 10000,
    });

    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    // Should schedule preemptive compaction
    await (scheduler as any).evaluatePreemptiveCompaction?.(blocks);

    expect(mockAlarmScheduler.schedule).toHaveBeenCalledWith(
      expect.objectContaining({
        reason: 'preemptive',
      })
    );
  });

  it.fails('should detect seasonal patterns in write activity', async () => {
    // Feature: Identify weekly/monthly patterns for better prediction
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // patternDetection: true,
    });

    // Record weekly pattern: high on weekdays, low on weekends
    const patterns = [];
    for (let week = 0; week < 4; week++) {
      for (let day = 0; day < 7; day++) {
        const isWeekend = day === 0 || day === 6;
        patterns.push({
          dayOfWeek: day,
          writeRate: isWeekend ? 100 : 1000,
        });
      }
    }

    for (const pattern of patterns) {
      await (scheduler as any).recordWritePattern?.(pattern);
    }

    const detectedPatterns = (scheduler as any).getDetectedPatterns?.();
    expect(detectedPatterns.weekly).toBeDefined();
    expect(detectedPatterns.weekly.lowActivityDays).toContain(0); // Sunday
    expect(detectedPatterns.weekly.lowActivityDays).toContain(6); // Saturday
  });

  it.fails('should adjust prediction based on recent deviations', async () => {
    // Feature: If actual activity deviates from prediction,
    // adjust future predictions accordingly
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // adaptivePrediction: true,
    });

    // Initial prediction
    (scheduler as any).setPrediction?.({
      timestamp: new Date('2024-01-15T09:00:00Z'),
      expectedWriteRate: 5000,
    });

    // Actual was higher
    await (scheduler as any).recordActualActivity?.({
      timestamp: new Date('2024-01-15T09:00:00Z'),
      actualWriteRate: 7500,
    });

    // Future predictions should be adjusted
    const adjustedPrediction = (scheduler as any).predictWriteActivity?.(
      new Date('2024-01-16T09:00:00Z')
    );

    expect(adjustedPrediction.expectedWriteRate).toBeGreaterThan(5000);
  });

  it.fails('should support real-time pattern updates', async () => {
    // Feature: Update predictions in real-time as new data arrives
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // realtimePatternUpdates: true,
    });

    // Initial state
    const initialPrediction = (scheduler as any).predictWriteActivity?.(
      new Date(Date.now() + 3600_000)
    );

    // Stream new activity data
    await (scheduler as any).streamActivityUpdate?.({ writeRate: 8000 });
    await (scheduler as any).streamActivityUpdate?.({ writeRate: 8500 });
    await (scheduler as any).streamActivityUpdate?.({ writeRate: 9000 });

    // Prediction should update based on trend
    const updatedPrediction = (scheduler as any).predictWriteActivity?.(
      new Date(Date.now() + 3600_000)
    );

    expect(updatedPrediction.expectedWriteRate).toBeGreaterThan(
      initialPrediction?.expectedWriteRate ?? 0
    );
  });
});

// ============================================================================
// 6. Resource-Aware Scheduling
// ============================================================================

describe('CompactionScheduler - Resource-Aware Scheduling', () => {
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
  });

  it.fails('should track memory usage during compaction', async () => {
    // Feature: Monitor memory consumption during compaction
    // to prevent OOM situations
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // memoryTracking: true,
    });

    const blocks: TestBlock[] = Array.from({ length: 10 }, (_, i) =>
      createSmallBlock(`block-${i}`, 500_000)
    );

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.runCompaction(blocks);

    expect(result.stats).toHaveProperty('peakMemoryUsage');
    expect(result.stats).toHaveProperty('averageMemoryUsage');
  });

  it.fails('should limit concurrent compaction based on available memory', async () => {
    // Feature: Reduce batch size when memory is constrained
    const mockResourceMonitor = {
      getAvailableMemory: () => 50 * 1024 * 1024, // 50MB available
    };

    const scheduler = new CompactionScheduler({
      storage,
      maxBlocksPerRun: 20,
      // Expected future API:
      // resourceMonitor: mockResourceMonitor,
      // maxMemoryPerCompaction: 100 * 1024 * 1024, // 100MB limit
    });

    // Create small blocks (under 2MB threshold) that will be selected for compaction
    const blocks: TestBlock[] = Array.from({ length: 20 }, (_, i) =>
      createSmallBlock(`block-${i}`, 500_000) // 500KB each, under 2MB threshold
    );

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.runCompaction(blocks);

    // Without memory-aware scheduling, scheduler will process up to maxBlocksPerRun (20)
    // With memory-aware scheduling, should process fewer blocks due to constraint
    // This test expects the memory-aware feature which would limit processing
    expect(result.stats).toHaveProperty('memoryLimited', true);
    expect(result.processedBlocks).toBeLessThan(10);
  });

  it.fails('should track I/O bandwidth usage', async () => {
    // Feature: Monitor I/O bandwidth to avoid saturating disk/network
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // ioBandwidthTracking: true,
    });

    const blocks: TestBlock[] = Array.from({ length: 5 }, (_, i) =>
      createSmallBlock(`block-${i}`, 500_000)
    );

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.runCompaction(blocks);

    expect(result.stats).toHaveProperty('ioBytesRead');
    expect(result.stats).toHaveProperty('ioBytesWritten');
    expect(result.stats).toHaveProperty('ioThroughput');
  });

  it.fails('should throttle I/O during compaction', async () => {
    // Feature: Limit I/O rate to prevent impact on concurrent operations
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // maxIoRateBytesPerSecond: 50 * 1024 * 1024, // 50MB/s limit
    });

    const blocks: TestBlock[] = Array.from({ length: 10 }, (_, i) =>
      createSmallBlock(`block-${i}`, 10 * 1024 * 1024) // 10MB each = 100MB total
    );

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const startTime = Date.now();
    await scheduler.runCompaction(blocks);
    const duration = Date.now() - startTime;

    // 100MB at 50MB/s should take at least 2 seconds
    expect(duration).toBeGreaterThanOrEqual(2000);
  });

  it.fails('should coordinate with other resource consumers', async () => {
    // Feature: Scheduler should coordinate with other processes
    // consuming resources (e.g., query engine, write path)
    const mockResourceCoordinator = {
      requestResources: vi.fn().mockResolvedValue({
        granted: true,
        allocation: { memory: 100 * 1024 * 1024, ioBandwidth: 50 * 1024 * 1024 },
      }),
      releaseResources: vi.fn(),
    };

    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // resourceCoordinator: mockResourceCoordinator,
    });

    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    await scheduler.runCompaction(blocks);

    expect(mockResourceCoordinator.requestResources).toHaveBeenCalled();
    expect(mockResourceCoordinator.releaseResources).toHaveBeenCalled();
  });

  it.fails('should yield resources during long-running compaction', async () => {
    // Feature: Periodically yield resources during long compaction
    // to allow other operations to proceed
    const mockResourceCoordinator = {
      requestResources: vi.fn().mockResolvedValue({ granted: true }),
      releaseResources: vi.fn(),
      yieldResources: vi.fn(),
    };

    const scheduler = new CompactionScheduler({
      storage,
      maxBlocksPerRun: 100,
      // Expected future API:
      // resourceCoordinator: mockResourceCoordinator,
      // yieldIntervalMs: 100,
    });

    const blocks: TestBlock[] = Array.from({ length: 50 }, (_, i) =>
      createSmallBlock(`block-${i}`, 500_000)
    );

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    await scheduler.runCompaction(blocks);

    // Should have yielded multiple times during long operation
    expect(mockResourceCoordinator.yieldResources.mock.calls.length).toBeGreaterThan(0);
  });
});

// ============================================================================
// 7. Backpressure Handling
// ============================================================================

describe('CompactionScheduler - Backpressure Handling', () => {
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
  });

  it.fails('should detect write backpressure', async () => {
    // Feature: Detect when incoming writes are backing up
    // due to compaction or other bottlenecks
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // backpressureDetection: true,
    });

    // Simulate write queue buildup
    await (scheduler as any).recordWriteQueueDepth?.(1000);
    await (scheduler as any).recordWriteQueueDepth?.(2000);
    await (scheduler as any).recordWriteQueueDepth?.(5000);

    const backpressureStatus = (scheduler as any).getBackpressureStatus?.();
    expect(backpressureStatus.detected).toBe(true);
    expect(backpressureStatus.severity).toBe('high');
  });

  it.fails('should pause compaction during high backpressure', async () => {
    // Feature: Stop compaction to free resources when
    // write backpressure is detected
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // backpressureHandling: true,
    });

    // Set high backpressure state
    (scheduler as any).setBackpressureState?.({ severity: 'critical' });

    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.runCompaction(blocks);

    expect(result.skipped).toBe(true);
    expect(result.reason).toMatch(/backpressure/i);
  });

  it.fails('should resume compaction as backpressure decreases', async () => {
    // Feature: Gradually resume compaction when backpressure eases
    let backpressure = 'critical';
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // backpressureHandling: true,
      // getBackpressure: () => backpressure,
    });

    const blocks: TestBlock[] = Array.from({ length: 10 }, (_, i) =>
      createSmallBlock(`block-${i}`, 300_000)
    );

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    // High backpressure - should skip
    const result1 = await scheduler.runCompaction(blocks);
    expect(result1.skipped).toBe(true);

    // Backpressure decreases
    backpressure = 'low';

    // Should resume compaction
    const result2 = await scheduler.runCompaction(blocks);
    expect(result2.skipped).toBe(false);
  });

  it.fails('should signal backpressure to write path', async () => {
    // Feature: When compaction is falling behind, signal
    // the write path to slow down
    const mockWritePathController = {
      setThrottle: vi.fn(),
    };

    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // writePathController: mockWritePathController,
    });

    // Simulate compaction falling behind (many small blocks accumulating)
    const blocks: TestBlock[] = Array.from({ length: 100 }, (_, i) =>
      createSmallBlock(`block-${i}`, 100_000)
    );

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    await scheduler.runCompaction(blocks);

    // Should signal write path to throttle
    expect(mockWritePathController.setThrottle).toHaveBeenCalledWith(
      expect.objectContaining({
        reason: 'compaction-backlog',
      })
    );
  });

  it.fails('should track compaction lag metrics', async () => {
    // Feature: Track how far behind compaction is relative to writes
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // lagTracking: true,
    });

    // Record writes faster than compaction can keep up
    await (scheduler as any).recordWriteActivity?.({ blocksCreated: 100 });
    await (scheduler as any).recordCompactionActivity?.({ blocksCompacted: 20 });

    const lagMetrics = (scheduler as any).getCompactionLag?.();
    expect(lagMetrics.pendingBlocks).toBe(80);
    expect(lagMetrics.estimatedCatchupTime).toBeGreaterThan(0);
  });

  it.fails('should implement flow control for compaction queue', async () => {
    // Feature: Limit the number of pending compaction tasks
    // to prevent unbounded queue growth
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // maxPendingTasks: 10,
    });

    // Try to enqueue more than max pending
    for (let i = 0; i < 15; i++) {
      await (scheduler as any).enqueueCompaction?.({
        collection: `collection-${i}`,
        blocks: [createSmallBlock(`block-${i}`)],
      });
    }

    const queueState = (scheduler as any).getQueueState?.();
    expect(queueState.pendingTasks).toBeLessThanOrEqual(10);
    expect(queueState.droppedTasks).toBe(5);
  });

  it.fails('should support configurable backpressure thresholds', async () => {
    // Feature: Allow users to configure backpressure sensitivity
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // backpressureThresholds: {
      //   low: 100,      // Queue depth for low backpressure
      //   medium: 500,   // Queue depth for medium backpressure
      //   high: 1000,    // Queue depth for high backpressure
      //   critical: 5000 // Queue depth for critical backpressure
      // }
    });

    // Test threshold detection
    await (scheduler as any).recordWriteQueueDepth?.(200);
    expect((scheduler as any).getBackpressureStatus?.().severity).toBe('low');

    await (scheduler as any).recordWriteQueueDepth?.(700);
    expect((scheduler as any).getBackpressureStatus?.().severity).toBe('medium');

    await (scheduler as any).recordWriteQueueDepth?.(2000);
    expect((scheduler as any).getBackpressureStatus?.().severity).toBe('high');

    await (scheduler as any).recordWriteQueueDepth?.(6000);
    expect((scheduler as any).getBackpressureStatus?.().severity).toBe('critical');
  });

  it.fails('should emit backpressure events for monitoring', async () => {
    // Feature: Emit events when backpressure state changes
    // for external monitoring systems
    const eventHandler = vi.fn();
    const scheduler = new CompactionScheduler({
      storage,
      // Expected future API:
      // onBackpressureChange: eventHandler,
    });

    // Trigger backpressure state changes
    await (scheduler as any).recordWriteQueueDepth?.(100); // low
    await (scheduler as any).recordWriteQueueDepth?.(5000); // critical

    expect(eventHandler).toHaveBeenCalledTimes(2);
    expect(eventHandler).toHaveBeenCalledWith(
      expect.objectContaining({
        previousState: 'none',
        newState: 'low',
      })
    );
    expect(eventHandler).toHaveBeenCalledWith(
      expect.objectContaining({
        previousState: 'low',
        newState: 'critical',
      })
    );
  });
});
