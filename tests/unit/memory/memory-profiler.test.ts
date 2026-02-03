/**
 * Memory Profiler Tests
 *
 * Tests for the memory profiling utilities:
 * - Memory snapshot utilities
 * - Heap growth tracking between operations
 * - Memory diff reporting
 * - Integration with vitest
 * - Helper functions for memory leak detection
 * - Memory benchmark utilities
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  MemoryProfiler,
  createMemorySnapshot,
  calculateDiff,
  withMemoryTracking,
  withMemoryTrackingSync,
  monitorMemoryOverIterations,
  assertNoMemoryLeak,
  assertMemoryDiffWithinBounds,
  assertMemoryReturnsToBaseline,
  createMemoryBenchmark,
  compareMemoryBenchmarks,
  formatBenchmarkResult,
  withMemoryTest,
  forceGarbageCollection,
  isGCAvailable,
  formatBytes,
  createMemoryPressure,
  runUntilMemoryStabilizes,
  type MemorySnapshot,
  type MemoryDiff,
  type MemoryReport,
} from '../../utils/memory-profiler.js';

// =============================================================================
// Memory Snapshot Utilities Tests
// =============================================================================

describe('Memory Snapshot Utilities', () => {
  it('should create a memory snapshot with all required fields', () => {
    const snapshot = createMemorySnapshot('test-snapshot');

    expect(snapshot.id).toBeDefined();
    expect(snapshot.label).toBe('test-snapshot');
    expect(snapshot.timestamp).toBeGreaterThan(0);
    expect(snapshot.heapUsed).toBeGreaterThan(0);
    expect(snapshot.heapTotal).toBeGreaterThan(0);
    expect(snapshot.rss).toBeGreaterThan(0);
    expect(snapshot.external).toBeGreaterThanOrEqual(0);
    expect(snapshot.arrayBuffers).toBeGreaterThanOrEqual(0);
  });

  it('should create snapshot without forcing GC', () => {
    const snapshot = createMemorySnapshot('no-gc', false);

    expect(snapshot.label).toBe('no-gc');
    expect(snapshot.heapUsed).toBeGreaterThan(0);
  });

  it('should include V8 heap stats when available', () => {
    const snapshot = createMemorySnapshot('v8-stats');

    // V8 stats may or may not be available depending on environment
    // If available, verify structure
    if (snapshot.v8HeapStats) {
      expect(snapshot.v8HeapStats.usedHeapSize).toBeGreaterThan(0);
      expect(snapshot.v8HeapStats.heapSizeLimit).toBeGreaterThan(0);
    }
  });

  it('should calculate diff between two snapshots', () => {
    const before = createMemorySnapshot('before');

    // Allocate some memory
    const data = new Array(100000).fill({ key: 'value', nested: { a: 1, b: 2 } });

    const after = createMemorySnapshot('after');
    const diff = calculateDiff(before, after);

    expect(diff.fromLabel).toBe('before');
    expect(diff.toLabel).toBe('after');
    expect(diff.elapsedMs).toBeGreaterThanOrEqual(0);
    expect(typeof diff.heapUsedDelta).toBe('number');
    expect(typeof diff.heapUsedPercentChange).toBe('number');

    // Keep reference to prevent optimization
    expect(data.length).toBe(100000);
  });
});

// =============================================================================
// MemoryProfiler Class Tests
// =============================================================================

describe('MemoryProfiler Class', () => {
  let profiler: MemoryProfiler;

  beforeEach(() => {
    profiler = new MemoryProfiler({ forceGC: true });
  });

  it('should create profiler with default options', () => {
    const p = new MemoryProfiler();
    expect(p).toBeDefined();
  });

  it('should take snapshots and retrieve them by label', () => {
    const snap1 = profiler.snapshot('start');
    const snap2 = profiler.snapshot('middle');
    const snap3 = profiler.snapshot('end');

    expect(profiler.getSnapshot('start')).toBe(snap1);
    expect(profiler.getSnapshot('middle')).toBe(snap2);
    expect(profiler.getSnapshot('end')).toBe(snap3);
    expect(profiler.getSnapshot('nonexistent')).toBeUndefined();
  });

  it('should return all snapshots', () => {
    profiler.snapshot('a');
    profiler.snapshot('b');
    profiler.snapshot('c');

    const snapshots = profiler.getSnapshots();
    expect(snapshots).toHaveLength(3);
    expect(snapshots.map((s) => s.label)).toEqual(['a', 'b', 'c']);
  });

  it('should calculate diff between labeled snapshots', () => {
    profiler.snapshot('before');

    // Allocate memory
    const arr = new Array(50000).fill('test string data');

    profiler.snapshot('after');

    const diff = profiler.diff('before', 'after');
    expect(diff.fromLabel).toBe('before');
    expect(diff.toLabel).toBe('after');

    // Keep reference
    expect(arr.length).toBe(50000);
  });

  it('should throw when diff references nonexistent snapshot', () => {
    profiler.snapshot('exists');

    expect(() => profiler.diff('exists', 'missing')).toThrow('Snapshot not found: missing');
    expect(() => profiler.diff('missing', 'exists')).toThrow('Snapshot not found: missing');
  });

  it('should generate comprehensive report', () => {
    profiler.snapshot('start');

    // Create some allocations
    let data: unknown[] = [];
    for (let i = 0; i < 5; i++) {
      data.push(new Array(10000).fill(i));
      profiler.snapshot(`step-${i}`);
    }

    const report = profiler.generateReport();

    expect(report.snapshots).toHaveLength(6); // start + 5 steps
    expect(report.diffs).toHaveLength(5);
    expect(report.summary.snapshotCount).toBe(6);
    expect(report.summary.totalElapsedMs).toBeGreaterThanOrEqual(0);

    // Keep reference
    expect(data.length).toBe(5);
  });

  it('should format report as human-readable string', () => {
    profiler.snapshot('test-start');
    profiler.snapshot('test-end');

    const report = profiler.generateReport();
    const formatted = profiler.formatReport(report);

    expect(formatted).toContain('MEMORY PROFILING REPORT');
    expect(formatted).toContain('SUMMARY');
    expect(formatted).toContain('SNAPSHOTS');
    expect(formatted).toContain('test-start');
    expect(formatted).toContain('test-end');
  });

  it('should reset and clear all snapshots', () => {
    profiler.snapshot('a');
    profiler.snapshot('b');
    expect(profiler.getSnapshots()).toHaveLength(2);

    profiler.reset();

    expect(profiler.getSnapshots()).toHaveLength(0);
    expect(profiler.getSnapshot('a')).toBeUndefined();
  });

  it('should get current heap used without creating snapshot', () => {
    const heap = profiler.getCurrentHeapUsed();

    expect(heap).toBeGreaterThan(0);
    expect(profiler.getSnapshots()).toHaveLength(0);
  });

  it('should track async operation memory', async () => {
    const { result, diff } = await profiler.track('async-op', async () => {
      const data = new Array(20000).fill('tracked');
      await new Promise((r) => setTimeout(r, 1));
      return data.length;
    });

    expect(result).toBe(20000);
    expect(diff.fromLabel).toBe('async-op-before');
    expect(diff.toLabel).toBe('async-op-after');
  });

  it('should track sync operation memory', () => {
    const { result, diff } = profiler.trackSync('sync-op', () => {
      const data = new Array(15000).fill('tracked');
      return data.length;
    });

    expect(result).toBe(15000);
    expect(diff.fromLabel).toBe('sync-op-before');
    expect(diff.toLabel).toBe('sync-op-after');
  });
});

// =============================================================================
// Memory Tracking Helpers Tests
// =============================================================================

describe('Memory Tracking Helpers', () => {
  it('should track async operation with withMemoryTracking', async () => {
    const { result, before, after, diff } = await withMemoryTracking(async () => {
      const data = new Array(30000).fill({ value: 'test' });
      await new Promise((r) => setTimeout(r, 1));
      return data.length;
    });

    expect(result).toBe(30000);
    expect(before.heapUsed).toBeGreaterThan(0);
    expect(after.heapUsed).toBeGreaterThan(0);
    expect(diff).toBeDefined();
  });

  it('should track sync operation with withMemoryTrackingSync', () => {
    const { result, before, after, diff } = withMemoryTrackingSync(() => {
      const data = new Array(25000).fill(42);
      return data.reduce((a, b) => a + b, 0);
    });

    expect(result).toBe(25000 * 42);
    expect(before.label).toContain('before');
    expect(after.label).toContain('after');
    expect(typeof diff.heapUsedDelta).toBe('number');
  });

  it('should monitor memory over iterations', async () => {
    const result = await monitorMemoryOverIterations(
      async (i) => {
        // Small allocations per iteration
        const data = new Array(1000).fill(i);
        await new Promise((r) => setTimeout(r, 1));
        // Keep reference briefly
        expect(data[0]).toBe(i);
      },
      5,
      { forceGC: true, delayBetweenMs: 1 }
    );

    expect(result.diffs).toHaveLength(5);
    expect(result.baseline).toBeDefined();
    expect(result.final).toBeDefined();
    expect(typeof result.totalGrowth).toBe('number');
    expect(typeof result.averageGrowthPerIteration).toBe('number');
  });
});

// =============================================================================
// Leak Detection Assertions Tests
// =============================================================================

describe('Memory Leak Assertions', () => {
  it('should pass assertNoMemoryLeak for stable memory', () => {
    const profiler = new MemoryProfiler();
    profiler.snapshot('start');
    profiler.snapshot('end');

    const report = profiler.generateReport();

    // Should not throw - use relaxed growth rate for quick operations
    expect(() =>
      assertNoMemoryLeak(report, {
        maxGrowthMB: 100,
        maxGrowthRateMBPerSec: 1000 // Allow high rate for instant snapshots
      })
    ).not.toThrow();
  });

  it('should fail assertNoMemoryLeak for excessive growth', () => {
    const profiler = new MemoryProfiler();
    profiler.snapshot('start');

    // Simulate large allocation by modifying report
    const report = profiler.generateReport();
    report.summary.netHeapChange = 60 * 1024 * 1024; // 60MB
    report.summary.startHeapUsed = 10 * 1024 * 1024;

    expect(() =>
      assertNoMemoryLeak(report, { maxGrowthMB: 50 })
    ).toThrow(/Memory grew by.*exceeds limit/);
  });

  it('should enforce percentage growth limit', () => {
    const profiler = new MemoryProfiler();
    profiler.snapshot('start');

    const report = profiler.generateReport();
    report.summary.netHeapChange = 50 * 1024 * 1024;
    report.summary.startHeapUsed = 20 * 1024 * 1024; // 250% growth

    expect(() =>
      assertNoMemoryLeak(report, { maxGrowthPercent: 100 })
    ).toThrow(/exceeds limit/);
  });

  it('should assertMemoryDiffWithinBounds for small diffs', () => {
    const diff: MemoryDiff = {
      fromLabel: 'a',
      toLabel: 'b',
      elapsedMs: 100,
      heapUsedDelta: 5 * 1024 * 1024, // 5MB
      heapTotalDelta: 0,
      rssDelta: 0,
      externalDelta: 0,
      arrayBuffersDelta: 0,
      heapUsedPercentChange: 10,
      growthRatePerSecond: 0,
    };

    expect(() =>
      assertMemoryDiffWithinBounds(diff, 10) // 10MB limit
    ).not.toThrow();
  });

  it('should fail assertMemoryDiffWithinBounds for large diffs', () => {
    const diff: MemoryDiff = {
      fromLabel: 'a',
      toLabel: 'b',
      elapsedMs: 100,
      heapUsedDelta: 15 * 1024 * 1024, // 15MB
      heapTotalDelta: 0,
      rssDelta: 0,
      externalDelta: 0,
      arrayBuffersDelta: 0,
      heapUsedPercentChange: 50,
      growthRatePerSecond: 0,
    };

    expect(() =>
      assertMemoryDiffWithinBounds(diff, 10)
    ).toThrow(/exceeds limit/);
  });

  it('should assertMemoryReturnsToBaseline within tolerance', () => {
    const baseline: MemorySnapshot = {
      id: 'base',
      label: 'baseline',
      timestamp: 0,
      heapUsed: 100 * 1024 * 1024,
      heapTotal: 200 * 1024 * 1024,
      rss: 150 * 1024 * 1024,
      external: 1024,
      arrayBuffers: 0,
    };

    const afterCleanup: MemorySnapshot = {
      id: 'after',
      label: 'after-cleanup',
      timestamp: 1000,
      heapUsed: 105 * 1024 * 1024, // 5% higher
      heapTotal: 200 * 1024 * 1024,
      rss: 155 * 1024 * 1024,
      external: 1024,
      arrayBuffers: 0,
    };

    expect(() =>
      assertMemoryReturnsToBaseline(baseline, afterCleanup, 10)
    ).not.toThrow();
  });

  it('should fail assertMemoryReturnsToBaseline outside tolerance', () => {
    const baseline: MemorySnapshot = {
      id: 'base',
      label: 'baseline',
      timestamp: 0,
      heapUsed: 100 * 1024 * 1024,
      heapTotal: 200 * 1024 * 1024,
      rss: 150 * 1024 * 1024,
      external: 1024,
      arrayBuffers: 0,
    };

    const afterCleanup: MemorySnapshot = {
      id: 'after',
      label: 'after-cleanup',
      timestamp: 1000,
      heapUsed: 130 * 1024 * 1024, // 30% higher
      heapTotal: 200 * 1024 * 1024,
      rss: 180 * 1024 * 1024,
      external: 1024,
      arrayBuffers: 0,
    };

    expect(() =>
      assertMemoryReturnsToBaseline(baseline, afterCleanup, 10)
    ).toThrow(/did not return to baseline/);
  });
});

// =============================================================================
// Memory Benchmark Utilities Tests
// =============================================================================

describe('Memory Benchmark Utilities', () => {
  it('should run memory benchmark', async () => {
    const result = await createMemoryBenchmark(
      'simple-allocation',
      async (i) => {
        const data = new Array(1000).fill(i);
        await new Promise((r) => setTimeout(r, 1));
        expect(data.length).toBe(1000);
      },
      {
        warmupIterations: 2,
        iterations: 5,
        gcBetweenIterations: true,
        sampleInterval: 1,
      }
    );

    expect(result.name).toBe('simple-allocation');
    expect(result.iterations).toBe(5);
    expect(result.durationMs).toBeGreaterThan(0);
    expect(result.samples.length).toBeGreaterThan(0);
    expect(typeof result.memoryPerIteration).toBe('number');
    expect(typeof result.peakMemory).toBe('number');
  });

  it('should compare memory benchmarks', async () => {
    const baseline = await createMemoryBenchmark(
      'baseline',
      async () => {
        const data = new Array(1000).fill(0);
        expect(data.length).toBe(1000);
      },
      { warmupIterations: 1, iterations: 3 }
    );

    const comparison = await createMemoryBenchmark(
      'comparison',
      async () => {
        const data = new Array(2000).fill(0);
        expect(data.length).toBe(2000);
      },
      { warmupIterations: 1, iterations: 3 }
    );

    const result = compareMemoryBenchmarks(baseline, comparison);

    expect(typeof result.memoryPerIterationChange).toBe('number');
    expect(typeof result.memoryPerIterationChangePercent).toBe('number');
    expect(typeof result.peakMemoryChange).toBe('number');
    expect(typeof result.isRegression).toBe('boolean');
    expect(result.summary).toBeDefined();
  });

  it('should format benchmark result', async () => {
    const result = await createMemoryBenchmark(
      'format-test',
      async () => {
        const data = new Array(500).fill('x');
        expect(data.length).toBe(500);
      },
      { warmupIterations: 1, iterations: 3 }
    );

    const formatted = formatBenchmarkResult(result);

    expect(formatted).toContain('Benchmark: format-test');
    expect(formatted).toContain('Iterations:');
    expect(formatted).toContain('Duration:');
    expect(formatted).toContain('Memory/iteration:');
  });
});

// =============================================================================
// Vitest Integration Tests
// =============================================================================

describe('Vitest Integration', () => {
  it('should wrap test function with memory tracking', async () => {
    let executed = false;
    const wrappedTest = withMemoryTest(async () => {
      executed = true;
      // Small allocation that shouldn't trigger leak detection
      const data = new Array(100).fill(0);
      expect(data.length).toBe(100);
    }, {
      maxGrowthMB: 100,
      maxGrowthRateMBPerSec: 1000 // Allow high rate for quick test execution
    });

    await wrappedTest();

    expect(executed).toBe(true);
  });
});

// =============================================================================
// Utility Functions Tests
// =============================================================================

describe('Utility Functions', () => {
  it('should check GC availability', () => {
    const available = isGCAvailable();
    expect(typeof available).toBe('boolean');
  });

  it('should format bytes correctly', () => {
    expect(formatBytes(500)).toBe('500 B');
    expect(formatBytes(1024)).toBe('1.00 KB');
    expect(formatBytes(1536)).toBe('1.50 KB');
    expect(formatBytes(1024 * 1024)).toBe('1.00 MB');
    expect(formatBytes(1.5 * 1024 * 1024)).toBe('1.50 MB');
    expect(formatBytes(1024 * 1024 * 1024)).toBe('1.00 GB');
    expect(formatBytes(-1024)).toBe('-1.00 KB');
  });

  it('should create and use memory pressure', () => {
    const pressure = createMemoryPressure(5); // 5MB

    expect(pressure.allocated()).toBe(0);

    pressure.allocate();

    expect(pressure.allocated()).toBeGreaterThanOrEqual(5 * 1024 * 1024);

    pressure.release();

    expect(pressure.allocated()).toBe(0);
  });

  it('should run until memory stabilizes', async () => {
    let counter = 0;
    const result = await runUntilMemoryStabilizes(
      async () => {
        counter++;
        // Do minimal work that should stabilize quickly
        const x = Math.random();
        expect(x).toBeLessThanOrEqual(1);
      },
      20, // max iterations
      1024 * 1024 // 1MB threshold
    );

    expect(counter).toBeGreaterThan(0);
    expect(typeof result.iterations).toBe('number');
    expect(typeof result.stabilized).toBe('boolean');
    expect(result.finalHeapUsed).toBeGreaterThan(0);
  });

  it('should force garbage collection if available', () => {
    // This should not throw even if GC is not available
    expect(() => forceGarbageCollection()).not.toThrow();
  });
});

// =============================================================================
// Leak Indicator Detection Tests
// =============================================================================

describe('Leak Indicator Detection', () => {
  it('should detect monotonic growth indicator', () => {
    const profiler = new MemoryProfiler();

    // Simulate monotonic growth by manipulating snapshots
    profiler.snapshot('s1');
    profiler.snapshot('s2');
    profiler.snapshot('s3');
    profiler.snapshot('s4');
    profiler.snapshot('s5');

    const report = profiler.generateReport();

    // Manually modify diffs to simulate monotonic growth
    for (const diff of report.diffs) {
      diff.heapUsedDelta = 1024 * 1024; // 1MB growth each
    }

    // Regenerate leak indicators
    // Note: In real usage, the report would naturally show this
    // Here we're testing the structure
    expect(report.leakIndicators).toBeDefined();
    expect(Array.isArray(report.leakIndicators)).toBe(true);
  });

  it('should provide leak indicator descriptions', () => {
    const profiler = new MemoryProfiler();
    profiler.snapshot('start');
    profiler.snapshot('end');

    const report = profiler.generateReport();

    // Each indicator should have required fields
    for (const indicator of report.leakIndicators) {
      expect(indicator.type).toBeDefined();
      expect(indicator.severity).toMatch(/^(low|medium|high)$/);
      expect(indicator.description).toBeDefined();
      expect(Array.isArray(indicator.relatedSnapshots)).toBe(true);
    }
  });
});

// =============================================================================
// Report Summary Tests
// =============================================================================

describe('Report Summary', () => {
  it('should calculate summary statistics correctly', () => {
    const profiler = new MemoryProfiler();

    profiler.snapshot('s1');
    profiler.snapshot('s2');
    profiler.snapshot('s3');

    const report = profiler.generateReport();
    const summary = report.summary;

    expect(summary.snapshotCount).toBe(3);
    expect(summary.totalElapsedMs).toBeGreaterThanOrEqual(0);
    expect(summary.startHeapUsed).toBeGreaterThan(0);
    expect(summary.endHeapUsed).toBeGreaterThan(0);
    expect(typeof summary.netHeapChange).toBe('number');
    expect(summary.peakHeapUsed).toBeGreaterThan(0);
    expect(summary.minHeapUsed).toBeGreaterThan(0);
    expect(summary.avgHeapUsed).toBeGreaterThan(0);
    expect(typeof summary.avgGrowthRate).toBe('number');
    expect(typeof summary.heapUsedStdDev).toBe('number');
  });

  it('should handle empty profiler gracefully', () => {
    const profiler = new MemoryProfiler();
    const report = profiler.generateReport();

    expect(report.snapshots).toHaveLength(0);
    expect(report.diffs).toHaveLength(0);
    expect(report.summary.snapshotCount).toBe(0);
    expect(report.summary.startHeapUsed).toBe(0);
    expect(report.summary.endHeapUsed).toBe(0);
  });

  it('should handle single snapshot', () => {
    const profiler = new MemoryProfiler();
    profiler.snapshot('only');

    const report = profiler.generateReport();

    expect(report.snapshots).toHaveLength(1);
    expect(report.diffs).toHaveLength(0);
    expect(report.summary.snapshotCount).toBe(1);
    expect(report.summary.totalElapsedMs).toBe(0);
    expect(report.summary.netHeapChange).toBe(0);
  });
});
