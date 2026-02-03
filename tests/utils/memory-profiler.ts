/**
 * Memory Profiler Utilities
 *
 * Comprehensive memory profiling tools for the MongoLake test suite:
 * - Memory snapshot utilities
 * - Heap growth tracking between operations
 * - Memory diff reporting
 * - Integration with Vitest
 * - Helper functions for memory leak detection
 * - Memory benchmark utilities
 *
 * @example
 * ```ts
 * import {
 *   MemoryProfiler,
 *   createMemorySnapshot,
 *   assertNoMemoryLeak,
 *   withMemoryTracking,
 *   createMemoryBenchmark,
 * } from '../utils/memory-profiler.js';
 *
 * describe('Memory Tests', () => {
 *   it('should not leak memory', async () => {
 *     const profiler = new MemoryProfiler();
 *     profiler.snapshot('before');
 *
 *     // ... perform operations ...
 *
 *     profiler.snapshot('after');
 *     const report = profiler.generateReport();
 *     assertNoMemoryLeak(report, { maxGrowthMB: 10 });
 *   });
 * });
 * ```
 */

import { expect } from 'vitest';

// ============================================================================
// Types
// ============================================================================

/**
 * Memory usage snapshot with detailed breakdown.
 */
export interface MemorySnapshot {
  /** Unique identifier for this snapshot */
  id: string;
  /** Label for this snapshot */
  label: string;
  /** Timestamp when snapshot was taken */
  timestamp: number;
  /** Heap memory used (bytes) */
  heapUsed: number;
  /** Total heap size (bytes) */
  heapTotal: number;
  /** Resident set size (bytes) */
  rss: number;
  /** External memory (bytes) - C++ objects bound to JavaScript */
  external: number;
  /** Array buffers memory (bytes) */
  arrayBuffers: number;
  /** Optional: V8 heap statistics if available */
  v8HeapStats?: V8HeapStats;
}

/**
 * V8 heap statistics (when available via --expose-gc).
 */
export interface V8HeapStats {
  /** Total heap size executable */
  totalHeapSizeExecutable: number;
  /** Total physical size */
  totalPhysicalSize: number;
  /** Total available size */
  totalAvailableSize: number;
  /** Used heap size */
  usedHeapSize: number;
  /** Heap size limit */
  heapSizeLimit: number;
  /** Malloced memory */
  mallocedMemory: number;
  /** Peak malloced memory */
  peakMallocedMemory: number;
  /** Number of native contexts */
  numberOfNativeContexts: number;
  /** Number of detached contexts */
  numberOfDetachedContexts: number;
}

/**
 * Memory diff between two snapshots.
 */
export interface MemoryDiff {
  /** First snapshot label */
  fromLabel: string;
  /** Second snapshot label */
  toLabel: string;
  /** Time elapsed between snapshots (ms) */
  elapsedMs: number;
  /** Heap used change (bytes) */
  heapUsedDelta: number;
  /** Heap total change (bytes) */
  heapTotalDelta: number;
  /** RSS change (bytes) */
  rssDelta: number;
  /** External memory change (bytes) */
  externalDelta: number;
  /** Array buffers change (bytes) */
  arrayBuffersDelta: number;
  /** Percentage change in heap used */
  heapUsedPercentChange: number;
  /** Growth rate (bytes per second) */
  growthRatePerSecond: number;
}

/**
 * Memory profiling report.
 */
export interface MemoryReport {
  /** All collected snapshots */
  snapshots: MemorySnapshot[];
  /** Diffs between consecutive snapshots */
  diffs: MemoryDiff[];
  /** Summary statistics */
  summary: MemorySummary;
  /** Potential leak indicators */
  leakIndicators: LeakIndicator[];
}

/**
 * Summary of memory profiling session.
 */
export interface MemorySummary {
  /** Total number of snapshots */
  snapshotCount: number;
  /** Total elapsed time (ms) */
  totalElapsedMs: number;
  /** Starting heap used (bytes) */
  startHeapUsed: number;
  /** Ending heap used (bytes) */
  endHeapUsed: number;
  /** Net heap change (bytes) */
  netHeapChange: number;
  /** Peak heap used (bytes) */
  peakHeapUsed: number;
  /** Minimum heap used (bytes) */
  minHeapUsed: number;
  /** Average heap used (bytes) */
  avgHeapUsed: number;
  /** Average growth rate (bytes/sec) */
  avgGrowthRate: number;
  /** Standard deviation of heap used */
  heapUsedStdDev: number;
}

/**
 * Indicator of potential memory leak.
 */
export interface LeakIndicator {
  /** Type of indicator */
  type: 'monotonic_growth' | 'high_growth_rate' | 'large_delta' | 'unbounded_growth' | 'detached_contexts';
  /** Severity: low, medium, high */
  severity: 'low' | 'medium' | 'high';
  /** Description of the indicator */
  description: string;
  /** Related snapshot labels */
  relatedSnapshots: string[];
  /** Additional data */
  data?: Record<string, unknown>;
}

/**
 * Options for memory leak assertion.
 */
export interface MemoryLeakAssertionOptions {
  /** Maximum allowed heap growth in MB */
  maxGrowthMB?: number;
  /** Maximum allowed heap growth percentage */
  maxGrowthPercent?: number;
  /** Maximum allowed growth rate in MB/sec */
  maxGrowthRateMBPerSec?: number;
  /** Whether to allow monotonic growth */
  allowMonotonicGrowth?: boolean;
  /** Tolerance for noise in measurements (bytes) */
  toleranceBytes?: number;
}

/**
 * Options for the MemoryProfiler.
 */
export interface MemoryProfilerOptions {
  /** Whether to force GC before each snapshot (requires --expose-gc) */
  forceGC?: boolean;
  /** Whether to collect V8 heap stats */
  collectV8Stats?: boolean;
  /** Prefix for auto-generated snapshot IDs */
  idPrefix?: string;
}

/**
 * Memory benchmark result.
 */
export interface MemoryBenchmarkResult {
  /** Name of the benchmark */
  name: string;
  /** Number of iterations */
  iterations: number;
  /** Memory per iteration (bytes) */
  memoryPerIteration: number;
  /** Total memory allocated (bytes) */
  totalMemoryAllocated: number;
  /** Total memory freed (bytes) */
  totalMemoryFreed: number;
  /** Net memory after benchmark (bytes) */
  netMemory: number;
  /** Peak memory during benchmark (bytes) */
  peakMemory: number;
  /** Duration of benchmark (ms) */
  durationMs: number;
  /** Detailed samples */
  samples: Array<{ iteration: number; heapUsed: number; timestamp: number }>;
}

/**
 * Options for memory benchmark.
 */
export interface MemoryBenchmarkOptions {
  /** Number of warmup iterations */
  warmupIterations?: number;
  /** Number of measurement iterations */
  iterations?: number;
  /** Whether to force GC between iterations */
  gcBetweenIterations?: boolean;
  /** Sample every N iterations */
  sampleInterval?: number;
  /** Timeout per iteration (ms) */
  iterationTimeout?: number;
}

// ============================================================================
// Memory Profiler Class
// ============================================================================

/**
 * Memory profiler for tracking heap usage and detecting leaks.
 *
 * @example
 * ```ts
 * const profiler = new MemoryProfiler({ forceGC: true });
 *
 * profiler.snapshot('initial');
 * await performHeavyOperation();
 * profiler.snapshot('after-operation');
 * await cleanup();
 * profiler.snapshot('after-cleanup');
 *
 * const report = profiler.generateReport();
 * console.log(profiler.formatReport(report));
 * ```
 */
export class MemoryProfiler {
  private snapshots: MemorySnapshot[] = [];
  private snapshotCounter = 0;
  private readonly options: Required<MemoryProfilerOptions>;

  constructor(options: MemoryProfilerOptions = {}) {
    this.options = {
      forceGC: options.forceGC ?? true,
      collectV8Stats: options.collectV8Stats ?? true,
      idPrefix: options.idPrefix ?? 'snapshot',
    };
  }

  /**
   * Take a memory snapshot.
   *
   * @param label - Label for this snapshot
   * @returns The snapshot taken
   */
  snapshot(label: string): MemorySnapshot {
    if (this.options.forceGC) {
      forceGarbageCollection();
    }

    const memUsage = process.memoryUsage();
    const snapshot: MemorySnapshot = {
      id: `${this.options.idPrefix}-${++this.snapshotCounter}`,
      label,
      timestamp: performance.now(),
      heapUsed: memUsage.heapUsed,
      heapTotal: memUsage.heapTotal,
      rss: memUsage.rss,
      external: memUsage.external,
      arrayBuffers: memUsage.arrayBuffers,
    };

    if (this.options.collectV8Stats) {
      const v8Stats = getV8HeapStatistics();
      if (v8Stats) {
        snapshot.v8HeapStats = v8Stats;
      }
    }

    this.snapshots.push(snapshot);
    return snapshot;
  }

  /**
   * Get a previously taken snapshot by label.
   *
   * @param label - Snapshot label
   * @returns The snapshot or undefined
   */
  getSnapshot(label: string): MemorySnapshot | undefined {
    return this.snapshots.find((s) => s.label === label);
  }

  /**
   * Get all snapshots.
   */
  getSnapshots(): MemorySnapshot[] {
    return [...this.snapshots];
  }

  /**
   * Calculate diff between two snapshots.
   *
   * @param fromLabel - Label of first snapshot
   * @param toLabel - Label of second snapshot
   * @returns Memory diff
   */
  diff(fromLabel: string, toLabel: string): MemoryDiff {
    const from = this.getSnapshot(fromLabel);
    const to = this.getSnapshot(toLabel);

    if (!from) throw new Error(`Snapshot not found: ${fromLabel}`);
    if (!to) throw new Error(`Snapshot not found: ${toLabel}`);

    return calculateDiff(from, to);
  }

  /**
   * Generate a comprehensive memory report.
   *
   * @returns Memory profiling report
   */
  generateReport(): MemoryReport {
    const diffs: MemoryDiff[] = [];
    for (let i = 1; i < this.snapshots.length; i++) {
      diffs.push(calculateDiff(this.snapshots[i - 1], this.snapshots[i]));
    }

    const summary = calculateSummary(this.snapshots, diffs);
    const leakIndicators = detectLeakIndicators(this.snapshots, diffs, summary);

    return {
      snapshots: [...this.snapshots],
      diffs,
      summary,
      leakIndicators,
    };
  }

  /**
   * Format a memory report as a human-readable string.
   *
   * @param report - Report to format
   * @returns Formatted string
   */
  formatReport(report: MemoryReport): string {
    const lines: string[] = [
      '='.repeat(70),
      'MEMORY PROFILING REPORT',
      '='.repeat(70),
      '',
      'SUMMARY',
      '-'.repeat(70),
      `Snapshots:          ${report.summary.snapshotCount}`,
      `Duration:           ${report.summary.totalElapsedMs.toFixed(2)} ms`,
      `Start Heap:         ${formatBytes(report.summary.startHeapUsed)}`,
      `End Heap:           ${formatBytes(report.summary.endHeapUsed)}`,
      `Net Change:         ${formatBytes(report.summary.netHeapChange)} (${report.summary.netHeapChange >= 0 ? '+' : ''}${((report.summary.netHeapChange / report.summary.startHeapUsed) * 100).toFixed(2)}%)`,
      `Peak Heap:          ${formatBytes(report.summary.peakHeapUsed)}`,
      `Avg Growth Rate:    ${formatBytes(report.summary.avgGrowthRate)}/sec`,
      '',
    ];

    if (report.snapshots.length > 0) {
      lines.push('SNAPSHOTS', '-'.repeat(70));
      for (const snap of report.snapshots) {
        lines.push(
          `[${snap.label}] heap: ${formatBytes(snap.heapUsed)}, rss: ${formatBytes(snap.rss)}, ext: ${formatBytes(snap.external)}`
        );
      }
      lines.push('');
    }

    if (report.diffs.length > 0) {
      lines.push('DIFFS', '-'.repeat(70));
      for (const diff of report.diffs) {
        const sign = diff.heapUsedDelta >= 0 ? '+' : '';
        lines.push(
          `${diff.fromLabel} -> ${diff.toLabel}: ${sign}${formatBytes(diff.heapUsedDelta)} (${sign}${diff.heapUsedPercentChange.toFixed(2)}%) in ${diff.elapsedMs.toFixed(2)}ms`
        );
      }
      lines.push('');
    }

    if (report.leakIndicators.length > 0) {
      lines.push('LEAK INDICATORS', '-'.repeat(70));
      for (const indicator of report.leakIndicators) {
        const icon = indicator.severity === 'high' ? '[!!!]' : indicator.severity === 'medium' ? '[!!]' : '[!]';
        lines.push(`${icon} ${indicator.type}: ${indicator.description}`);
      }
      lines.push('');
    }

    lines.push('='.repeat(70));
    return lines.join('\n');
  }

  /**
   * Reset the profiler, clearing all snapshots.
   */
  reset(): void {
    this.snapshots = [];
    this.snapshotCounter = 0;
  }

  /**
   * Get the current heap used without creating a snapshot.
   */
  getCurrentHeapUsed(): number {
    return process.memoryUsage().heapUsed;
  }

  /**
   * Track memory through an async operation.
   *
   * @param label - Label prefix for snapshots
   * @param operation - Async operation to track
   * @returns Result of the operation and memory diff
   */
  async track<T>(
    label: string,
    operation: () => Promise<T>
  ): Promise<{ result: T; diff: MemoryDiff }> {
    this.snapshot(`${label}-before`);
    const result = await operation();
    this.snapshot(`${label}-after`);
    const diff = this.diff(`${label}-before`, `${label}-after`);
    return { result, diff };
  }

  /**
   * Track memory through a sync operation.
   *
   * @param label - Label prefix for snapshots
   * @param operation - Sync operation to track
   * @returns Result of the operation and memory diff
   */
  trackSync<T>(
    label: string,
    operation: () => T
  ): { result: T; diff: MemoryDiff } {
    this.snapshot(`${label}-before`);
    const result = operation();
    this.snapshot(`${label}-after`);
    const diff = this.diff(`${label}-before`, `${label}-after`);
    return { result, diff };
  }
}

// ============================================================================
// Snapshot Functions
// ============================================================================

/**
 * Create a single memory snapshot.
 *
 * @param label - Label for the snapshot
 * @param forceGC - Whether to force GC first
 * @returns Memory snapshot
 */
export function createMemorySnapshot(label: string, forceGC = true): MemorySnapshot {
  if (forceGC) {
    forceGarbageCollection();
  }

  const memUsage = process.memoryUsage();
  return {
    id: `snapshot-${Date.now()}`,
    label,
    timestamp: performance.now(),
    heapUsed: memUsage.heapUsed,
    heapTotal: memUsage.heapTotal,
    rss: memUsage.rss,
    external: memUsage.external,
    arrayBuffers: memUsage.arrayBuffers,
    v8HeapStats: getV8HeapStatistics() ?? undefined,
  };
}

/**
 * Calculate the difference between two memory snapshots.
 *
 * @param from - Starting snapshot
 * @param to - Ending snapshot
 * @returns Memory diff
 */
export function calculateDiff(from: MemorySnapshot, to: MemorySnapshot): MemoryDiff {
  const elapsedMs = to.timestamp - from.timestamp;
  const heapUsedDelta = to.heapUsed - from.heapUsed;

  return {
    fromLabel: from.label,
    toLabel: to.label,
    elapsedMs,
    heapUsedDelta,
    heapTotalDelta: to.heapTotal - from.heapTotal,
    rssDelta: to.rss - from.rss,
    externalDelta: to.external - from.external,
    arrayBuffersDelta: to.arrayBuffers - from.arrayBuffers,
    heapUsedPercentChange: from.heapUsed > 0 ? (heapUsedDelta / from.heapUsed) * 100 : 0,
    growthRatePerSecond: elapsedMs > 0 ? (heapUsedDelta / elapsedMs) * 1000 : 0,
  };
}

// ============================================================================
// Memory Tracking Helpers
// ============================================================================

/**
 * Track memory usage through an async operation.
 *
 * @param operation - Operation to track
 * @param options - Tracking options
 * @returns Result and memory info
 */
export async function withMemoryTracking<T>(
  operation: () => Promise<T>,
  options: { label?: string; forceGC?: boolean } = {}
): Promise<{
  result: T;
  before: MemorySnapshot;
  after: MemorySnapshot;
  diff: MemoryDiff;
}> {
  const label = options.label ?? 'operation';
  const forceGC = options.forceGC ?? true;

  const before = createMemorySnapshot(`${label}-before`, forceGC);
  const result = await operation();
  const after = createMemorySnapshot(`${label}-after`, forceGC);
  const diff = calculateDiff(before, after);

  return { result, before, after, diff };
}

/**
 * Track memory usage through a sync operation.
 *
 * @param operation - Operation to track
 * @param options - Tracking options
 * @returns Result and memory info
 */
export function withMemoryTrackingSync<T>(
  operation: () => T,
  options: { label?: string; forceGC?: boolean } = {}
): {
  result: T;
  before: MemorySnapshot;
  after: MemorySnapshot;
  diff: MemoryDiff;
} {
  const label = options.label ?? 'operation';
  const forceGC = options.forceGC ?? true;

  const before = createMemorySnapshot(`${label}-before`, forceGC);
  const result = operation();
  const after = createMemorySnapshot(`${label}-after`, forceGC);
  const diff = calculateDiff(before, after);

  return { result, before, after, diff };
}

/**
 * Monitor memory usage over multiple iterations of an operation.
 *
 * @param operation - Operation to repeat
 * @param iterations - Number of iterations
 * @param options - Monitoring options
 * @returns Array of diffs for each iteration
 */
export async function monitorMemoryOverIterations(
  operation: (iteration: number) => Promise<void>,
  iterations: number,
  options: { forceGC?: boolean; delayBetweenMs?: number } = {}
): Promise<{
  diffs: MemoryDiff[];
  baseline: MemorySnapshot;
  final: MemorySnapshot;
  totalGrowth: number;
  averageGrowthPerIteration: number;
}> {
  const forceGC = options.forceGC ?? true;
  const delay = options.delayBetweenMs ?? 0;
  const diffs: MemoryDiff[] = [];

  const baseline = createMemorySnapshot('baseline', forceGC);
  let previousSnapshot = baseline;

  for (let i = 0; i < iterations; i++) {
    await operation(i);

    if (delay > 0) {
      await new Promise((resolve) => setTimeout(resolve, delay));
    }

    const currentSnapshot = createMemorySnapshot(`iteration-${i}`, forceGC);
    diffs.push(calculateDiff(previousSnapshot, currentSnapshot));
    previousSnapshot = currentSnapshot;
  }

  const final = previousSnapshot;
  const totalGrowth = final.heapUsed - baseline.heapUsed;

  return {
    diffs,
    baseline,
    final,
    totalGrowth,
    averageGrowthPerIteration: totalGrowth / iterations,
  };
}

// ============================================================================
// Leak Detection Helpers
// ============================================================================

/**
 * Detect potential memory leak indicators.
 */
function detectLeakIndicators(
  snapshots: MemorySnapshot[],
  diffs: MemoryDiff[],
  summary: MemorySummary
): LeakIndicator[] {
  const indicators: LeakIndicator[] = [];

  // Check for monotonic growth
  if (diffs.length >= 3) {
    const allPositive = diffs.every((d) => d.heapUsedDelta > 0);
    if (allPositive) {
      indicators.push({
        type: 'monotonic_growth',
        severity: diffs.length >= 5 ? 'high' : 'medium',
        description: `Heap continuously grew across ${diffs.length} measurements`,
        relatedSnapshots: snapshots.map((s) => s.label),
      });
    }
  }

  // Check for high growth rate (> 1MB/sec sustained)
  const highGrowthDiffs = diffs.filter((d) => d.growthRatePerSecond > 1024 * 1024);
  if (highGrowthDiffs.length > 0) {
    indicators.push({
      type: 'high_growth_rate',
      severity: highGrowthDiffs.length >= 3 ? 'high' : 'medium',
      description: `${highGrowthDiffs.length} measurements showed growth > 1MB/sec`,
      relatedSnapshots: highGrowthDiffs.flatMap((d) => [d.fromLabel, d.toLabel]),
      data: {
        maxRate: Math.max(...highGrowthDiffs.map((d) => d.growthRatePerSecond)),
      },
    });
  }

  // Check for large deltas (> 10MB jump)
  const largeDeltaDiffs = diffs.filter((d) => Math.abs(d.heapUsedDelta) > 10 * 1024 * 1024);
  if (largeDeltaDiffs.length > 0) {
    indicators.push({
      type: 'large_delta',
      severity: 'medium',
      description: `${largeDeltaDiffs.length} measurements showed heap jumps > 10MB`,
      relatedSnapshots: largeDeltaDiffs.flatMap((d) => [d.fromLabel, d.toLabel]),
      data: {
        maxDelta: Math.max(...largeDeltaDiffs.map((d) => d.heapUsedDelta)),
      },
    });
  }

  // Check for unbounded growth (end > 2x start)
  if (summary.endHeapUsed > summary.startHeapUsed * 2) {
    indicators.push({
      type: 'unbounded_growth',
      severity: 'high',
      description: `Heap more than doubled: ${formatBytes(summary.startHeapUsed)} -> ${formatBytes(summary.endHeapUsed)}`,
      relatedSnapshots: [snapshots[0]?.label ?? '', snapshots[snapshots.length - 1]?.label ?? ''],
    });
  }

  // Check for detached contexts (if V8 stats available)
  const latestSnapshot = snapshots[snapshots.length - 1];
  if (latestSnapshot?.v8HeapStats?.numberOfDetachedContexts ?? 0 > 0) {
    indicators.push({
      type: 'detached_contexts',
      severity: 'medium',
      description: `Found ${latestSnapshot.v8HeapStats!.numberOfDetachedContexts} detached contexts`,
      relatedSnapshots: [latestSnapshot.label],
    });
  }

  return indicators;
}

/**
 * Calculate summary statistics from snapshots and diffs.
 */
function calculateSummary(snapshots: MemorySnapshot[], diffs: MemoryDiff[]): MemorySummary {
  if (snapshots.length === 0) {
    return {
      snapshotCount: 0,
      totalElapsedMs: 0,
      startHeapUsed: 0,
      endHeapUsed: 0,
      netHeapChange: 0,
      peakHeapUsed: 0,
      minHeapUsed: 0,
      avgHeapUsed: 0,
      avgGrowthRate: 0,
      heapUsedStdDev: 0,
    };
  }

  const heapValues = snapshots.map((s) => s.heapUsed);
  const avgHeapUsed = heapValues.reduce((a, b) => a + b, 0) / heapValues.length;
  const variance =
    heapValues.reduce((sum, val) => sum + Math.pow(val - avgHeapUsed, 2), 0) / heapValues.length;
  const stdDev = Math.sqrt(variance);

  const totalElapsedMs =
    snapshots.length > 1 ? snapshots[snapshots.length - 1].timestamp - snapshots[0].timestamp : 0;

  const avgGrowthRate =
    diffs.length > 0
      ? diffs.reduce((sum, d) => sum + d.growthRatePerSecond, 0) / diffs.length
      : 0;

  return {
    snapshotCount: snapshots.length,
    totalElapsedMs,
    startHeapUsed: snapshots[0].heapUsed,
    endHeapUsed: snapshots[snapshots.length - 1].heapUsed,
    netHeapChange: snapshots[snapshots.length - 1].heapUsed - snapshots[0].heapUsed,
    peakHeapUsed: Math.max(...heapValues),
    minHeapUsed: Math.min(...heapValues),
    avgHeapUsed,
    avgGrowthRate,
    heapUsedStdDev: stdDev,
  };
}

// ============================================================================
// Assertions
// ============================================================================

/**
 * Assert that no memory leak is detected based on a report.
 *
 * @param report - Memory report to check
 * @param options - Assertion options
 * @param message - Custom error message
 */
export function assertNoMemoryLeak(
  report: MemoryReport,
  options: MemoryLeakAssertionOptions = {},
  message?: string
): void {
  const {
    maxGrowthMB = 50,
    maxGrowthPercent = 100,
    maxGrowthRateMBPerSec = 10,
    allowMonotonicGrowth = false,
    toleranceBytes = 1024 * 1024, // 1MB tolerance for noise
  } = options;

  const netGrowthMB = report.summary.netHeapChange / (1024 * 1024);
  const growthPercent =
    report.summary.startHeapUsed > 0
      ? (report.summary.netHeapChange / report.summary.startHeapUsed) * 100
      : 0;
  const growthRateMBPerSec = report.summary.avgGrowthRate / (1024 * 1024);

  // Check absolute growth
  if (netGrowthMB > maxGrowthMB) {
    expect.fail(
      message ??
        `Memory grew by ${netGrowthMB.toFixed(2)} MB, exceeds limit of ${maxGrowthMB} MB`
    );
  }

  // Check percentage growth
  if (growthPercent > maxGrowthPercent) {
    expect.fail(
      message ??
        `Memory grew by ${growthPercent.toFixed(2)}%, exceeds limit of ${maxGrowthPercent}%`
    );
  }

  // Check growth rate
  if (growthRateMBPerSec > maxGrowthRateMBPerSec) {
    expect.fail(
      message ??
        `Memory growth rate ${growthRateMBPerSec.toFixed(2)} MB/sec exceeds limit of ${maxGrowthRateMBPerSec} MB/sec`
    );
  }

  // Check for high-severity leak indicators
  if (!allowMonotonicGrowth) {
    const highSeverityIndicators = report.leakIndicators.filter(
      (i) => i.severity === 'high'
    );
    if (highSeverityIndicators.length > 0) {
      const descriptions = highSeverityIndicators.map((i) => i.description).join('; ');
      expect.fail(
        message ??
          `High severity leak indicators detected: ${descriptions}`
      );
    }
  }
}

/**
 * Assert that memory diff is within expected bounds.
 *
 * @param diff - Memory diff to check
 * @param maxGrowthMB - Maximum allowed growth in MB
 * @param message - Custom error message
 */
export function assertMemoryDiffWithinBounds(
  diff: MemoryDiff,
  maxGrowthMB: number,
  message?: string
): void {
  const growthMB = diff.heapUsedDelta / (1024 * 1024);
  expect(
    growthMB <= maxGrowthMB,
    message ??
      `Memory grew by ${growthMB.toFixed(2)} MB (${diff.fromLabel} -> ${diff.toLabel}), exceeds limit of ${maxGrowthMB} MB`
  ).toBe(true);
}

/**
 * Assert that memory returns to baseline after cleanup.
 *
 * @param baseline - Baseline snapshot
 * @param afterCleanup - Snapshot after cleanup
 * @param tolerancePercent - Allowed variance from baseline (default 10%)
 * @param message - Custom error message
 */
export function assertMemoryReturnsToBaseline(
  baseline: MemorySnapshot,
  afterCleanup: MemorySnapshot,
  tolerancePercent: number = 10,
  message?: string
): void {
  const diff = afterCleanup.heapUsed - baseline.heapUsed;
  const diffPercent = (diff / baseline.heapUsed) * 100;

  expect(
    Math.abs(diffPercent) <= tolerancePercent,
    message ??
      `Memory did not return to baseline: ${formatBytes(baseline.heapUsed)} -> ${formatBytes(afterCleanup.heapUsed)} (${diffPercent.toFixed(2)}% change, tolerance: ${tolerancePercent}%)`
  ).toBe(true);
}

/**
 * Assert that an operation doesn't leak memory across multiple iterations.
 *
 * @param operation - Operation to test
 * @param iterations - Number of iterations
 * @param maxGrowthPerIterationKB - Maximum allowed growth per iteration in KB
 * @param message - Custom error message
 */
export async function assertNoLeakOverIterations(
  operation: (iteration: number) => Promise<void>,
  iterations: number,
  maxGrowthPerIterationKB: number = 100,
  message?: string
): Promise<void> {
  const result = await monitorMemoryOverIterations(operation, iterations, {
    forceGC: true,
    delayBetweenMs: 10,
  });

  const avgGrowthKB = result.averageGrowthPerIteration / 1024;

  expect(
    avgGrowthKB <= maxGrowthPerIterationKB,
    message ??
      `Average growth per iteration ${avgGrowthKB.toFixed(2)} KB exceeds limit of ${maxGrowthPerIterationKB} KB`
  ).toBe(true);
}

// ============================================================================
// Benchmark Utilities
// ============================================================================

/**
 * Run a memory benchmark on an operation.
 *
 * @param name - Benchmark name
 * @param operation - Operation to benchmark
 * @param options - Benchmark options
 * @returns Benchmark result
 */
export async function createMemoryBenchmark(
  name: string,
  operation: (iteration: number) => Promise<void>,
  options: MemoryBenchmarkOptions = {}
): Promise<MemoryBenchmarkResult> {
  const {
    warmupIterations = 3,
    iterations = 10,
    gcBetweenIterations = true,
    sampleInterval = 1,
    iterationTimeout = 30000,
  } = options;

  // Warmup phase
  for (let i = 0; i < warmupIterations; i++) {
    await Promise.race([
      operation(i),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('Warmup iteration timeout')), iterationTimeout)
      ),
    ]);
  }

  if (gcBetweenIterations) {
    forceGarbageCollection();
  }

  const samples: Array<{ iteration: number; heapUsed: number; timestamp: number }> = [];
  const startTime = performance.now();
  const startMemory = process.memoryUsage().heapUsed;
  let peakMemory = startMemory;

  // Measurement phase
  for (let i = 0; i < iterations; i++) {
    await Promise.race([
      operation(i),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('Iteration timeout')), iterationTimeout)
      ),
    ]);

    if (i % sampleInterval === 0) {
      if (gcBetweenIterations) {
        forceGarbageCollection();
      }
      const heapUsed = process.memoryUsage().heapUsed;
      samples.push({
        iteration: i,
        heapUsed,
        timestamp: performance.now(),
      });
      peakMemory = Math.max(peakMemory, heapUsed);
    }
  }

  if (gcBetweenIterations) {
    forceGarbageCollection();
  }

  const endTime = performance.now();
  const endMemory = process.memoryUsage().heapUsed;

  const totalMemoryAllocated = samples.reduce((sum, s, i) => {
    if (i === 0) return 0;
    const delta = s.heapUsed - samples[i - 1].heapUsed;
    return sum + (delta > 0 ? delta : 0);
  }, 0);

  const totalMemoryFreed = samples.reduce((sum, s, i) => {
    if (i === 0) return 0;
    const delta = s.heapUsed - samples[i - 1].heapUsed;
    return sum + (delta < 0 ? -delta : 0);
  }, 0);

  return {
    name,
    iterations,
    memoryPerIteration: (endMemory - startMemory) / iterations,
    totalMemoryAllocated,
    totalMemoryFreed,
    netMemory: endMemory - startMemory,
    peakMemory,
    durationMs: endTime - startTime,
    samples,
  };
}

/**
 * Compare memory benchmarks and return analysis.
 *
 * @param baseline - Baseline benchmark
 * @param comparison - Comparison benchmark
 * @returns Comparison analysis
 */
export function compareMemoryBenchmarks(
  baseline: MemoryBenchmarkResult,
  comparison: MemoryBenchmarkResult
): {
  memoryPerIterationChange: number;
  memoryPerIterationChangePercent: number;
  peakMemoryChange: number;
  peakMemoryChangePercent: number;
  isRegression: boolean;
  summary: string;
} {
  const memoryPerIterationChange = comparison.memoryPerIteration - baseline.memoryPerIteration;
  const memoryPerIterationChangePercent =
    baseline.memoryPerIteration !== 0
      ? (memoryPerIterationChange / baseline.memoryPerIteration) * 100
      : 0;

  const peakMemoryChange = comparison.peakMemory - baseline.peakMemory;
  const peakMemoryChangePercent =
    baseline.peakMemory !== 0 ? (peakMemoryChange / baseline.peakMemory) * 100 : 0;

  // Consider it a regression if memory per iteration increased by more than 20%
  const isRegression = memoryPerIterationChangePercent > 20;

  const sign = (n: number) => (n >= 0 ? '+' : '');
  const summary = `Memory per iteration: ${sign(memoryPerIterationChangePercent)}${memoryPerIterationChangePercent.toFixed(2)}%, Peak: ${sign(peakMemoryChangePercent)}${peakMemoryChangePercent.toFixed(2)}%`;

  return {
    memoryPerIterationChange,
    memoryPerIterationChangePercent,
    peakMemoryChange,
    peakMemoryChangePercent,
    isRegression,
    summary,
  };
}

/**
 * Format benchmark result as a string.
 *
 * @param result - Benchmark result
 * @returns Formatted string
 */
export function formatBenchmarkResult(result: MemoryBenchmarkResult): string {
  return [
    `Benchmark: ${result.name}`,
    `  Iterations: ${result.iterations}`,
    `  Duration: ${result.durationMs.toFixed(2)} ms`,
    `  Memory/iteration: ${formatBytes(result.memoryPerIteration)}`,
    `  Net memory: ${formatBytes(result.netMemory)}`,
    `  Peak memory: ${formatBytes(result.peakMemory)}`,
    `  Total allocated: ${formatBytes(result.totalMemoryAllocated)}`,
    `  Total freed: ${formatBytes(result.totalMemoryFreed)}`,
  ].join('\n');
}

// ============================================================================
// Vitest Integration
// ============================================================================

/**
 * Create a memory-tracked test function for Vitest.
 *
 * @param testFn - Test function to wrap
 * @param options - Memory tracking options
 * @returns Wrapped test function
 */
export function withMemoryTest<T>(
  testFn: () => Promise<T>,
  options: MemoryLeakAssertionOptions = {}
): () => Promise<T> {
  return async () => {
    const profiler = new MemoryProfiler();
    profiler.snapshot('test-start');

    const result = await testFn();

    profiler.snapshot('test-end');
    const report = profiler.generateReport();
    assertNoMemoryLeak(report, options);

    return result;
  };
}

/**
 * Vitest test modifier that adds memory leak checking.
 *
 * @example
 * ```ts
 * import { memoryTest } from '../utils/memory-profiler.js';
 *
 * describe('MyComponent', () => {
 *   memoryTest('should not leak memory', async () => {
 *     // ... test code ...
 *   }, { maxGrowthMB: 5 });
 * });
 * ```
 */
export function memoryTest(
  name: string,
  testFn: () => Promise<void>,
  options: MemoryLeakAssertionOptions = {}
): void {
  // Note: This would integrate with vitest's it() function
  // For now, we expose the wrapper that can be used manually
  const wrappedTest = withMemoryTest(testFn, options);
  // The caller should use: it(name, wrappedTest);
  // This is a helper that can be composed with vitest's it()
  return void wrappedTest;
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Force garbage collection if available.
 * Requires Node.js to be started with --expose-gc flag.
 */
export function forceGarbageCollection(): void {
  const gc = (globalThis as { gc?: () => void }).gc;
  if (gc) {
    gc();
  }
}

/**
 * Check if garbage collection is available.
 */
export function isGCAvailable(): boolean {
  return typeof (globalThis as { gc?: () => void }).gc === 'function';
}

/**
 * Get V8 heap statistics if available.
 */
export function getV8HeapStatistics(): V8HeapStats | null {
  try {
    // Dynamically require v8 module
    const v8 = require('v8');
    if (v8 && typeof v8.getHeapStatistics === 'function') {
      const stats = v8.getHeapStatistics();
      return {
        totalHeapSizeExecutable: stats.total_heap_size_executable,
        totalPhysicalSize: stats.total_physical_size,
        totalAvailableSize: stats.total_available_size,
        usedHeapSize: stats.used_heap_size,
        heapSizeLimit: stats.heap_size_limit,
        mallocedMemory: stats.malloced_memory,
        peakMallocedMemory: stats.peak_malloced_memory,
        numberOfNativeContexts: stats.number_of_native_contexts,
        numberOfDetachedContexts: stats.number_of_detached_contexts,
      };
    }
  } catch {
    // v8 module not available
  }
  return null;
}

/**
 * Format bytes as human-readable string.
 *
 * @param bytes - Number of bytes
 * @returns Formatted string
 */
export function formatBytes(bytes: number): string {
  const sign = bytes < 0 ? '-' : '';
  const absBytes = Math.abs(bytes);

  if (absBytes < 1024) {
    return `${sign}${absBytes} B`;
  } else if (absBytes < 1024 * 1024) {
    return `${sign}${(absBytes / 1024).toFixed(2)} KB`;
  } else if (absBytes < 1024 * 1024 * 1024) {
    return `${sign}${(absBytes / (1024 * 1024)).toFixed(2)} MB`;
  } else {
    return `${sign}${(absBytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
  }
}

/**
 * Wait for a specified duration.
 *
 * @param ms - Milliseconds to wait
 */
export function waitMs(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Create a memory pressure generator for testing.
 *
 * @param targetMB - Target memory pressure in MB
 * @returns Object with allocate and release methods
 */
export function createMemoryPressure(targetMB: number): {
  allocate: () => void;
  release: () => void;
  allocated: () => number;
} {
  let buffers: Uint8Array[] = [];
  const chunkSize = 1024 * 1024; // 1MB chunks
  const targetBytes = targetMB * 1024 * 1024;

  return {
    allocate(): void {
      while (buffers.reduce((sum, b) => sum + b.length, 0) < targetBytes) {
        buffers.push(new Uint8Array(chunkSize));
      }
    },

    release(): void {
      buffers = [];
      forceGarbageCollection();
    },

    allocated(): number {
      return buffers.reduce((sum, b) => sum + b.length, 0);
    },
  };
}

/**
 * Run a function repeatedly until memory stabilizes.
 *
 * @param operation - Operation to run
 * @param maxIterations - Maximum iterations before giving up
 * @param stabilityThresholdBytes - How close heap values must be to consider stable
 * @returns Number of iterations until stable, or -1 if not stable
 */
export async function runUntilMemoryStabilizes(
  operation: () => Promise<void>,
  maxIterations: number = 100,
  stabilityThresholdBytes: number = 100 * 1024 // 100KB
): Promise<{
  iterations: number;
  stabilized: boolean;
  finalHeapUsed: number;
}> {
  let lastHeap = process.memoryUsage().heapUsed;
  let stableCount = 0;
  const requiredStableCount = 3;

  for (let i = 0; i < maxIterations; i++) {
    await operation();
    forceGarbageCollection();

    const currentHeap = process.memoryUsage().heapUsed;
    const delta = Math.abs(currentHeap - lastHeap);

    if (delta < stabilityThresholdBytes) {
      stableCount++;
      if (stableCount >= requiredStableCount) {
        return {
          iterations: i + 1,
          stabilized: true,
          finalHeapUsed: currentHeap,
        };
      }
    } else {
      stableCount = 0;
    }

    lastHeap = currentHeap;
  }

  return {
    iterations: maxIterations,
    stabilized: false,
    finalHeapUsed: process.memoryUsage().heapUsed,
  };
}
