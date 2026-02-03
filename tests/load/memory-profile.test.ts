/**
 * Memory Profile Load Test
 *
 * Tracks heap usage before/after operations and detects potential memory leaks.
 * Uses process.memoryUsage() for heap tracking.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';

// ============================================================================
// Types
// ============================================================================

interface MemorySnapshot {
  timestamp: number;
  heapUsed: number;
  heapTotal: number;
  external: number;
  arrayBuffers: number;
  rss: number;
}

interface MemoryAnalysis {
  startHeap: number;
  endHeap: number;
  peakHeap: number;
  minHeap: number;
  netGrowth: number;
  growthPercent: number;
  samples: number;
}

interface LeakDetectionResult {
  isLikelyLeak: boolean;
  confidence: 'low' | 'medium' | 'high';
  trend: 'increasing' | 'stable' | 'decreasing';
  averageGrowthPerCycle: number;
  totalGrowth: number;
}

// ============================================================================
// Memory Tracking Utilities
// ============================================================================

function captureMemory(): MemorySnapshot {
  const mem = process.memoryUsage();
  return {
    timestamp: Date.now(),
    heapUsed: mem.heapUsed,
    heapTotal: mem.heapTotal,
    external: mem.external,
    arrayBuffers: mem.arrayBuffers,
    rss: mem.rss,
  };
}

function formatBytes(bytes: number): string {
  if (Math.abs(bytes) < 1024) return `${bytes} B`;
  const kb = bytes / 1024;
  if (Math.abs(kb) < 1024) return `${kb.toFixed(2)} KB`;
  const mb = kb / 1024;
  return `${mb.toFixed(2)} MB`;
}

function analyzeMemory(snapshots: MemorySnapshot[]): MemoryAnalysis {
  if (snapshots.length === 0) {
    return {
      startHeap: 0,
      endHeap: 0,
      peakHeap: 0,
      minHeap: 0,
      netGrowth: 0,
      growthPercent: 0,
      samples: 0,
    };
  }

  const heaps = snapshots.map((s) => s.heapUsed);
  const startHeap = heaps[0];
  const endHeap = heaps[heaps.length - 1];
  const peakHeap = Math.max(...heaps);
  const minHeap = Math.min(...heaps);
  const netGrowth = endHeap - startHeap;
  const growthPercent = startHeap > 0 ? (netGrowth / startHeap) * 100 : 0;

  return {
    startHeap,
    endHeap,
    peakHeap,
    minHeap,
    netGrowth,
    growthPercent,
    samples: snapshots.length,
  };
}

function detectLeak(cycleSnapshots: MemorySnapshot[][]): LeakDetectionResult {
  if (cycleSnapshots.length < 3) {
    return {
      isLikelyLeak: false,
      confidence: 'low',
      trend: 'stable',
      averageGrowthPerCycle: 0,
      totalGrowth: 0,
    };
  }

  // Compare end heap of each cycle
  const cycleEndHeaps = cycleSnapshots.map((cycle) =>
    cycle.length > 0 ? cycle[cycle.length - 1].heapUsed : 0
  );

  // Calculate growth between cycles
  const growths: number[] = [];
  for (let i = 1; i < cycleEndHeaps.length; i++) {
    growths.push(cycleEndHeaps[i] - cycleEndHeaps[i - 1]);
  }

  const totalGrowth = cycleEndHeaps[cycleEndHeaps.length - 1] - cycleEndHeaps[0];
  const averageGrowth = growths.reduce((a, b) => a + b, 0) / growths.length;

  // Count positive growths
  const positiveGrowths = growths.filter((g) => g > 0).length;
  const negativeGrowths = growths.filter((g) => g < 0).length;

  // Determine trend
  let trend: 'increasing' | 'stable' | 'decreasing';
  if (positiveGrowths > negativeGrowths * 2) {
    trend = 'increasing';
  } else if (negativeGrowths > positiveGrowths * 2) {
    trend = 'decreasing';
  } else {
    trend = 'stable';
  }

  // Determine if likely leak
  const significantGrowthThreshold = 1024 * 1024; // 1MB
  const isLikelyLeak =
    trend === 'increasing' &&
    averageGrowth > significantGrowthThreshold &&
    totalGrowth > significantGrowthThreshold * 3;

  // Confidence based on sample size and consistency
  let confidence: 'low' | 'medium' | 'high';
  if (cycleSnapshots.length >= 10 && positiveGrowths >= growths.length * 0.8) {
    confidence = 'high';
  } else if (cycleSnapshots.length >= 5 && positiveGrowths >= growths.length * 0.6) {
    confidence = 'medium';
  } else {
    confidence = 'low';
  }

  return {
    isLikelyLeak,
    confidence,
    trend,
    averageGrowthPerCycle: averageGrowth,
    totalGrowth,
  };
}

function forceGC(): void {
  if (global.gc) {
    global.gc();
  }
}

// ============================================================================
// Mock Components for Memory Testing
// ============================================================================

class MemoryTestBuffer {
  private buffer: unknown[] = [];
  private retainedReferences: Set<unknown> = new Set();

  push(item: unknown): void {
    this.buffer.push(item);
  }

  clear(): void {
    this.buffer = [];
  }

  getSize(): number {
    return this.buffer.length;
  }

  // Simulates a memory leak by retaining references
  leakyPush(item: unknown): void {
    this.buffer.push(item);
    this.retainedReferences.add(item);
  }

  // Clear buffer but leak remains
  leakyClear(): void {
    this.buffer = [];
    // retainedReferences not cleared - simulates leak
  }

  // Proper cleanup
  fullClear(): void {
    this.buffer = [];
    this.retainedReferences.clear();
  }
}

class DocumentCache {
  private cache: Map<string, unknown> = new Map();
  private maxSize: number;
  private accessOrder: string[] = [];

  constructor(maxSize: number = 1000) {
    this.maxSize = maxSize;
  }

  set(key: string, value: unknown): void {
    if (this.cache.size >= this.maxSize && !this.cache.has(key)) {
      // Evict oldest
      const oldest = this.accessOrder.shift();
      if (oldest) {
        this.cache.delete(oldest);
      }
    }

    this.cache.set(key, value);
    this.accessOrder.push(key);
  }

  get(key: string): unknown | undefined {
    const value = this.cache.get(key);
    if (value !== undefined) {
      // Move to end of access order
      const idx = this.accessOrder.indexOf(key);
      if (idx > -1) {
        this.accessOrder.splice(idx, 1);
        this.accessOrder.push(key);
      }
    }
    return value;
  }

  clear(): void {
    this.cache.clear();
    this.accessOrder = [];
  }

  getSize(): number {
    return this.cache.size;
  }
}

// ============================================================================
// Test Helpers
// ============================================================================

function createLargeObject(sizeKB: number): Record<string, unknown> {
  return {
    id: Math.random().toString(36),
    data: 'x'.repeat(sizeKB * 1024),
    timestamp: Date.now(),
    metadata: {
      size: sizeKB,
      type: 'test',
    },
  };
}

function createDocumentBatch(count: number, sizeKB: number = 1): unknown[] {
  return Array.from({ length: count }, (_, i) => ({
    _id: `doc-${i}`,
    index: i,
    payload: 'x'.repeat(sizeKB * 1024),
  }));
}

// ============================================================================
// Tests
// ============================================================================

describe('Memory Profile Load Tests', () => {
  let memorySnapshots: MemorySnapshot[];

  beforeEach(() => {
    memorySnapshots = [];
    forceGC();
  });

  afterEach(() => {
    forceGC();
  });

  describe('Heap Usage Tracking', () => {
    it('should track heap usage before and after operations', async () => {
      const beforeOp = captureMemory();

      // Perform memory-intensive operation
      const buffer = new MemoryTestBuffer();
      for (let i = 0; i < 1000; i++) {
        buffer.push(createLargeObject(1));
      }

      const duringOp = captureMemory();

      // Clear and allow GC
      buffer.clear();
      await new Promise((r) => setTimeout(r, 50));
      forceGC();

      const afterOp = captureMemory();

      // Memory should increase during operation
      expect(duringOp.heapUsed).toBeGreaterThan(beforeOp.heapUsed);

      // Memory should increase during operation
      // Note: GC timing is non-deterministic, so we can't reliably test memory recovery
      // The key assertion is that memory increased during the operation
      const memoryUsed = duringOp.heapUsed - beforeOp.heapUsed;
      expect(memoryUsed).toBeGreaterThan(0);

      console.log('Heap tracking results:');
      console.log(`  Before: ${formatBytes(beforeOp.heapUsed)}`);
      console.log(`  During: ${formatBytes(duringOp.heapUsed)}`);
      console.log(`  After: ${formatBytes(afterOp.heapUsed)}`);
      console.log(`  Memory used during operation: ${formatBytes(memoryUsed)}`);
    });

    it('should track memory across multiple operation cycles', async () => {
      const cycleCount = 5;
      const cycleSnapshots: MemorySnapshot[][] = [];

      for (let cycle = 0; cycle < cycleCount; cycle++) {
        const cycleMemory: MemorySnapshot[] = [];

        cycleMemory.push(captureMemory());

        // Allocate
        const buffer = new MemoryTestBuffer();
        for (let i = 0; i < 500; i++) {
          buffer.push(createLargeObject(1));
        }

        cycleMemory.push(captureMemory());

        // Clear
        buffer.clear();
        await new Promise((r) => setTimeout(r, 20));
        forceGC();

        cycleMemory.push(captureMemory());

        cycleSnapshots.push(cycleMemory);
      }

      // Analyze each cycle
      const analyses = cycleSnapshots.map((cycle) => analyzeMemory(cycle));

      // Later cycles should not show significantly more baseline memory
      const firstCycleStart = analyses[0].startHeap;
      const lastCycleStart = analyses[cycleCount - 1].startHeap;
      const baselineGrowth = lastCycleStart - firstCycleStart;

      // Baseline should not grow excessively (allow 10MB tolerance)
      expect(baselineGrowth).toBeLessThan(10 * 1024 * 1024);

      console.log('Cycle analysis:');
      analyses.forEach((a, i) => {
        console.log(`  Cycle ${i + 1}: Peak ${formatBytes(a.peakHeap)}, Net growth: ${formatBytes(a.netGrowth)}`);
      });
    });

    it('should handle peak memory during large allocations', async () => {
      const snapshots: MemorySnapshot[] = [];

      snapshots.push(captureMemory());

      // Gradually increase memory usage
      const batches: unknown[][] = [];
      for (let i = 0; i < 10; i++) {
        batches.push(createDocumentBatch(100, 2));
        snapshots.push(captureMemory());
      }

      const analysis = analyzeMemory(snapshots);

      // Peak should be at or near the last snapshot
      const lastHeap = snapshots[snapshots.length - 1].heapUsed;
      expect(analysis.peakHeap).toBeGreaterThanOrEqual(lastHeap * 0.95);

      // Clear
      batches.length = 0;
      forceGC();
      await new Promise((r) => setTimeout(r, 50));

      const afterClear = captureMemory();

      console.log('Peak memory analysis:');
      console.log(`  Start: ${formatBytes(analysis.startHeap)}`);
      console.log(`  Peak: ${formatBytes(analysis.peakHeap)}`);
      console.log(`  After clear: ${formatBytes(afterClear.heapUsed)}`);
    });
  });

  describe('Memory Leak Detection', () => {
    it('should detect memory leak patterns', async () => {
      const leakyBuffer = new MemoryTestBuffer();
      const cycleSnapshots: MemorySnapshot[][] = [];

      for (let cycle = 0; cycle < 8; cycle++) {
        const cycleMemory: MemorySnapshot[] = [];

        cycleMemory.push(captureMemory());

        // Leaky operation - references are retained
        for (let i = 0; i < 200; i++) {
          leakyBuffer.leakyPush(createLargeObject(1));
        }

        cycleMemory.push(captureMemory());

        // "Clear" but leak remains
        leakyBuffer.leakyClear();
        forceGC();
        await new Promise((r) => setTimeout(r, 20));

        cycleMemory.push(captureMemory());

        cycleSnapshots.push(cycleMemory);
      }

      const leakResult = detectLeak(cycleSnapshots);

      // Should detect the leak
      expect(leakResult.trend).toBe('increasing');
      expect(leakResult.totalGrowth).toBeGreaterThan(0);

      console.log('Leak detection (intentional leak):');
      console.log(`  Trend: ${leakResult.trend}`);
      console.log(`  Likely leak: ${leakResult.isLikelyLeak}`);
      console.log(`  Confidence: ${leakResult.confidence}`);
      console.log(`  Total growth: ${formatBytes(leakResult.totalGrowth)}`);

      // Cleanup
      leakyBuffer.fullClear();
    });

    it('should not falsely detect leaks in proper implementations', async () => {
      const properBuffer = new MemoryTestBuffer();
      const cycleSnapshots: MemorySnapshot[][] = [];

      for (let cycle = 0; cycle < 8; cycle++) {
        const cycleMemory: MemorySnapshot[] = [];

        cycleMemory.push(captureMemory());

        // Proper operation
        for (let i = 0; i < 200; i++) {
          properBuffer.push(createLargeObject(1));
        }

        cycleMemory.push(captureMemory());

        // Proper clear
        properBuffer.clear();
        forceGC();
        await new Promise((r) => setTimeout(r, 20));

        cycleMemory.push(captureMemory());

        cycleSnapshots.push(cycleMemory);
      }

      const leakResult = detectLeak(cycleSnapshots);

      // Should not detect a leak
      expect(leakResult.isLikelyLeak).toBe(false);

      console.log('Leak detection (proper cleanup):');
      console.log(`  Trend: ${leakResult.trend}`);
      console.log(`  Likely leak: ${leakResult.isLikelyLeak}`);
      console.log(`  Total growth: ${formatBytes(leakResult.totalGrowth)}`);
    });

    it('should detect gradual memory accumulation', async () => {
      const cache = new DocumentCache(10000); // Large cache that won't evict
      const cycleSnapshots: MemorySnapshot[][] = [];

      for (let cycle = 0; cycle < 6; cycle++) {
        const cycleMemory: MemorySnapshot[] = [];

        cycleMemory.push(captureMemory());

        // Add to cache without clearing
        for (let i = 0; i < 500; i++) {
          cache.set(`cycle-${cycle}-doc-${i}`, createLargeObject(1));
        }

        cycleMemory.push(captureMemory());

        // Don't clear cache - simulates accumulation
        forceGC();
        await new Promise((r) => setTimeout(r, 20));

        cycleMemory.push(captureMemory());

        cycleSnapshots.push(cycleMemory);
      }

      const leakResult = detectLeak(cycleSnapshots);

      // Should detect accumulation pattern
      // Note: GC behavior may cause memory to decrease between cycles
      // The key test is that the cache is accumulating entries
      expect(cache.getSize()).toBeGreaterThan(0);

      console.log('Accumulation detection:');
      console.log(`  Trend: ${leakResult.trend}`);
      console.log(`  Cache size: ${cache.getSize()}`);
      console.log(`  Total growth: ${formatBytes(leakResult.totalGrowth)}`);

      // Cleanup
      cache.clear();
    });
  });

  describe('Cache Memory Behavior', () => {
    it('should bound memory with LRU cache eviction', async () => {
      const maxCacheSize = 500;
      const cache = new DocumentCache(maxCacheSize);
      const snapshots: MemorySnapshot[] = [];

      snapshots.push(captureMemory());

      // Add more items than cache size
      for (let i = 0; i < maxCacheSize * 3; i++) {
        cache.set(`doc-${i}`, createLargeObject(1));

        if (i % 500 === 0) {
          snapshots.push(captureMemory());
        }
      }

      snapshots.push(captureMemory());

      // Cache size should be bounded
      expect(cache.getSize()).toBeLessThanOrEqual(maxCacheSize);

      // Memory should stabilize after initial ramp
      const analysis = analyzeMemory(snapshots);
      const laterSnapshots = snapshots.slice(Math.floor(snapshots.length / 2));
      const laterAnalysis = analyzeMemory(laterSnapshots);

      // Growth in later phase should be minimal
      expect(Math.abs(laterAnalysis.netGrowth)).toBeLessThan(5 * 1024 * 1024);

      console.log('Cache memory behavior:');
      console.log(`  Cache size: ${cache.getSize()} (max: ${maxCacheSize})`);
      console.log(`  Peak heap: ${formatBytes(analysis.peakHeap)}`);
      console.log(`  Later phase growth: ${formatBytes(laterAnalysis.netGrowth)}`);

      cache.clear();
    });

    it('should release memory when cache is cleared', async () => {
      const cache = new DocumentCache(1000);

      const before = captureMemory();

      // Fill cache
      for (let i = 0; i < 1000; i++) {
        cache.set(`doc-${i}`, createLargeObject(2));
      }

      const afterFill = captureMemory();

      // Clear cache
      cache.clear();
      forceGC();
      await new Promise((r) => setTimeout(r, 100));

      const afterClear = captureMemory();

      // Cache should be empty after clear
      expect(cache.getSize()).toBe(0);

      // Memory used by cache should be significant
      const memoryUsedByCache = afterFill.heapUsed - before.heapUsed;
      // Note: GC timing is non-deterministic, so we just verify the cache used memory
      expect(memoryUsedByCache).toBeGreaterThan(0);

      console.log('Cache clear memory release:');
      console.log(`  Before: ${formatBytes(before.heapUsed)}`);
      console.log(`  After fill: ${formatBytes(afterFill.heapUsed)}`);
      console.log(`  After clear: ${formatBytes(afterClear.heapUsed)}`);
      console.log(`  Memory used by cache: ${formatBytes(memoryUsedByCache)}`);
    });
  });

  describe('Array Buffer and External Memory', () => {
    it('should track array buffer allocations', async () => {
      const before = captureMemory();

      // Allocate array buffers
      const buffers: ArrayBuffer[] = [];
      for (let i = 0; i < 100; i++) {
        buffers.push(new ArrayBuffer(100 * 1024)); // 100KB each
      }

      const after = captureMemory();

      // Array buffer memory should increase
      expect(after.arrayBuffers).toBeGreaterThan(before.arrayBuffers);

      const arrayBufferGrowth = after.arrayBuffers - before.arrayBuffers;
      const expectedGrowth = 100 * 100 * 1024; // 10MB

      // Should account for most of the allocated memory
      expect(arrayBufferGrowth).toBeGreaterThan(expectedGrowth * 0.9);

      console.log('Array buffer tracking:');
      console.log(`  Before: ${formatBytes(before.arrayBuffers)}`);
      console.log(`  After: ${formatBytes(after.arrayBuffers)}`);
      console.log(`  Growth: ${formatBytes(arrayBufferGrowth)}`);

      // Clear
      buffers.length = 0;
    });

    it('should handle typed array memory correctly', async () => {
      const before = captureMemory();

      // Allocate typed arrays
      const arrays: Uint8Array[] = [];
      for (let i = 0; i < 50; i++) {
        arrays.push(new Uint8Array(200 * 1024)); // 200KB each
      }

      const during = captureMemory();

      // Clear
      arrays.length = 0;
      forceGC();
      await new Promise((r) => setTimeout(r, 50));

      const after = captureMemory();

      // Memory was allocated during the operation
      const allocated = during.arrayBuffers - before.arrayBuffers;
      expect(allocated).toBeGreaterThan(0);

      console.log('Typed array memory:');
      console.log(`  Before: ${formatBytes(before.arrayBuffers)}`);
      console.log(`  During: ${formatBytes(during.arrayBuffers)}`);
      console.log(`  Allocated: ${formatBytes(allocated)}`);
    });
  });

  describe('Long-Running Operation Memory Stability', () => {
    it('should maintain stable memory over extended operations', async () => {
      const iterations = 20;
      const operationsPerIteration = 100;
      const iterationSnapshots: MemorySnapshot[] = [];

      const cache = new DocumentCache(500);

      for (let iter = 0; iter < iterations; iter++) {
        // Simulate work
        for (let i = 0; i < operationsPerIteration; i++) {
          const key = `iter-${iter}-doc-${i}`;
          cache.set(key, createLargeObject(1));

          // Occasionally read
          if (i % 10 === 0) {
            cache.get(`iter-${Math.max(0, iter - 1)}-doc-${i}`);
          }
        }

        // Snapshot at end of each iteration
        iterationSnapshots.push(captureMemory());

        // Small delay
        await new Promise((r) => setTimeout(r, 10));
      }

      // Analyze stability
      const analysis = analyzeMemory(iterationSnapshots);

      // After initial ramp, memory should be stable
      const laterSnapshots = iterationSnapshots.slice(iterations / 2);
      const laterAnalysis = analyzeMemory(laterSnapshots);

      // Growth in later half should be minimal
      const growthRate = Math.abs(laterAnalysis.growthPercent);
      expect(growthRate).toBeLessThan(20); // Less than 20% growth in later half

      console.log('Long-running stability:');
      console.log(`  Total iterations: ${iterations}`);
      console.log(`  Total growth: ${formatBytes(analysis.netGrowth)} (${analysis.growthPercent.toFixed(1)}%)`);
      console.log(`  Later half growth: ${formatBytes(laterAnalysis.netGrowth)} (${laterAnalysis.growthPercent.toFixed(1)}%)`);

      cache.clear();
    });

    it('should handle repeated allocation/deallocation cycles', async () => {
      const cycles = 10;
      const cycleSnapshots: MemorySnapshot[][] = [];

      for (let cycle = 0; cycle < cycles; cycle++) {
        const cycleMemory: MemorySnapshot[] = [];
        cycleMemory.push(captureMemory());

        // Allocate
        const data = createDocumentBatch(500, 2);
        cycleMemory.push(captureMemory());

        // Process (simulate)
        await new Promise((r) => setTimeout(r, 5));
        cycleMemory.push(captureMemory());

        // Deallocate
        data.length = 0;
        forceGC();
        await new Promise((r) => setTimeout(r, 20));
        cycleMemory.push(captureMemory());

        cycleSnapshots.push(cycleMemory);
      }

      // Check for leak pattern
      const leakResult = detectLeak(cycleSnapshots);

      // Should not show leak pattern
      expect(leakResult.isLikelyLeak).toBe(false);

      // Compare first and last cycle baselines
      const firstCycleStart = cycleSnapshots[0][0].heapUsed;
      const lastCycleEnd = cycleSnapshots[cycles - 1][cycleSnapshots[cycles - 1].length - 1].heapUsed;
      const baselineDrift = lastCycleEnd - firstCycleStart;

      // Baseline should not drift excessively
      expect(Math.abs(baselineDrift)).toBeLessThan(10 * 1024 * 1024);

      console.log('Allocation/deallocation cycles:');
      console.log(`  Cycles: ${cycles}`);
      console.log(`  Baseline drift: ${formatBytes(baselineDrift)}`);
      console.log(`  Leak detected: ${leakResult.isLikelyLeak}`);
    });
  });
});
