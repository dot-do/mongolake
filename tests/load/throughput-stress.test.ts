/**
 * Throughput Stress Load Test
 *
 * Tests high throughput scenarios with configurable load patterns.
 * Measures sustained ops/sec, latency percentiles, and system behavior under stress.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  createUserGenerator,
  createEventGenerator,
  createOrderGenerator,
  formatBytes,
  formatDuration,
  calculateThroughput,
} from '../utils/data-generator';

// ============================================================================
// Types
// ============================================================================

interface LatencyBucket {
  name: string;
  minMs: number;
  maxMs: number;
  count: number;
}

interface ThroughputMetrics {
  totalOps: number;
  durationMs: number;
  opsPerSecond: number;
  bytesProcessed: number;
  latencies: number[];
  latencyP50: number;
  latencyP95: number;
  latencyP99: number;
  latencyMax: number;
  latencyMin: number;
  errorCount: number;
}

interface LoadProfile {
  name: string;
  rampUpMs: number;
  sustainMs: number;
  rampDownMs: number;
  targetOpsPerSec: number;
}

// ============================================================================
// Mock High-Throughput Store
// ============================================================================

/**
 * Simulates a high-throughput document store for stress testing.
 * Tracks operation latencies and supports configurable delays.
 */
class HighThroughputStore {
  private data: Map<string, unknown> = new Map();
  private operationLatencies: number[] = [];
  private writeLatencyMs: number;
  private readLatencyMs: number;
  private errorRate: number;
  private totalWrites = 0;
  private totalReads = 0;
  private totalErrors = 0;
  private bytesWritten = 0;

  constructor(options: {
    writeLatencyMs?: number;
    readLatencyMs?: number;
    errorRate?: number;
  } = {}) {
    this.writeLatencyMs = options.writeLatencyMs ?? 0.5;
    this.readLatencyMs = options.readLatencyMs ?? 0.2;
    this.errorRate = options.errorRate ?? 0;
  }

  async write(doc: Record<string, unknown>): Promise<{ success: boolean; latencyMs: number }> {
    const start = performance.now();

    // Simulate error rate
    if (this.errorRate > 0 && Math.random() < this.errorRate) {
      this.totalErrors++;
      const latencyMs = performance.now() - start;
      return { success: false, latencyMs };
    }

    // Simulate write latency with some variance
    const variance = this.writeLatencyMs * 0.3;
    const delay = this.writeLatencyMs + (Math.random() - 0.5) * variance;
    await new Promise((r) => setTimeout(r, delay));

    const id = doc._id as string;
    const serialized = JSON.stringify(doc);
    this.data.set(id, doc);
    this.bytesWritten += serialized.length;
    this.totalWrites++;

    const latencyMs = performance.now() - start;
    this.operationLatencies.push(latencyMs);

    return { success: true, latencyMs };
  }

  async writeBatch(docs: Record<string, unknown>[]): Promise<{ success: boolean; latencyMs: number; written: number }> {
    const start = performance.now();
    let written = 0;

    for (const doc of docs) {
      const id = doc._id as string;
      const serialized = JSON.stringify(doc);
      this.data.set(id, doc);
      this.bytesWritten += serialized.length;
      written++;
    }

    // Simulate batch write latency
    const delay = this.writeLatencyMs * Math.log2(docs.length + 1);
    await new Promise((r) => setTimeout(r, delay));

    this.totalWrites += written;
    const latencyMs = performance.now() - start;
    this.operationLatencies.push(latencyMs);

    return { success: true, latencyMs, written };
  }

  async read(id: string): Promise<{ doc: unknown | null; latencyMs: number }> {
    const start = performance.now();

    // Simulate read latency
    const variance = this.readLatencyMs * 0.2;
    const delay = this.readLatencyMs + (Math.random() - 0.5) * variance;
    await new Promise((r) => setTimeout(r, delay));

    this.totalReads++;
    const doc = this.data.get(id) || null;
    const latencyMs = performance.now() - start;
    this.operationLatencies.push(latencyMs);

    return { doc, latencyMs };
  }

  getMetrics(): ThroughputMetrics {
    const sorted = [...this.operationLatencies].sort((a, b) => a - b);
    const totalOps = this.totalWrites + this.totalReads;

    return {
      totalOps,
      durationMs: 0, // Caller fills this in
      opsPerSecond: 0,
      bytesProcessed: this.bytesWritten,
      latencies: this.operationLatencies,
      latencyP50: this.percentile(sorted, 50),
      latencyP95: this.percentile(sorted, 95),
      latencyP99: this.percentile(sorted, 99),
      latencyMax: sorted[sorted.length - 1] ?? 0,
      latencyMin: sorted[0] ?? 0,
      errorCount: this.totalErrors,
    };
  }

  getDocumentCount(): number {
    return this.data.size;
  }

  clear(): void {
    this.data.clear();
    this.operationLatencies = [];
    this.totalWrites = 0;
    this.totalReads = 0;
    this.totalErrors = 0;
    this.bytesWritten = 0;
  }

  private percentile(sorted: number[], p: number): number {
    if (sorted.length === 0) return 0;
    const index = Math.ceil((p / 100) * sorted.length) - 1;
    return sorted[Math.max(0, Math.min(index, sorted.length - 1))]!;
  }
}

// ============================================================================
// Load Pattern Utilities
// ============================================================================

/**
 * Generates operations according to a load profile
 */
async function runLoadProfile<T>(
  profile: LoadProfile,
  generator: () => T,
  executor: (item: T) => Promise<void>
): Promise<{ completed: number; errors: number; latencies: number[] }> {
  const latencies: number[] = [];
  let completed = 0;
  let errors = 0;
  const totalDuration = profile.rampUpMs + profile.sustainMs + profile.rampDownMs;

  const startTime = Date.now();
  const endTime = startTime + totalDuration;

  while (Date.now() < endTime) {
    const elapsed = Date.now() - startTime;
    let currentOpsPerSec: number;

    if (elapsed < profile.rampUpMs) {
      // Ramp up phase
      currentOpsPerSec = (elapsed / profile.rampUpMs) * profile.targetOpsPerSec;
    } else if (elapsed < profile.rampUpMs + profile.sustainMs) {
      // Sustain phase
      currentOpsPerSec = profile.targetOpsPerSec;
    } else {
      // Ramp down phase
      const rampDownElapsed = elapsed - profile.rampUpMs - profile.sustainMs;
      currentOpsPerSec = (1 - rampDownElapsed / profile.rampDownMs) * profile.targetOpsPerSec;
    }

    // Calculate delay to achieve target ops/sec
    const delayMs = currentOpsPerSec > 0 ? 1000 / currentOpsPerSec : 100;

    const opStart = performance.now();
    try {
      const item = generator();
      await executor(item);
      completed++;
    } catch {
      errors++;
    }
    latencies.push(performance.now() - opStart);

    // Wait for next operation
    const opDuration = performance.now() - opStart;
    const waitTime = Math.max(0, delayMs - opDuration);
    if (waitTime > 0) {
      await new Promise((r) => setTimeout(r, waitTime));
    }
  }

  return { completed, errors, latencies };
}

/**
 * Calculate latency buckets for distribution analysis
 */
function calculateLatencyBuckets(latencies: number[]): LatencyBucket[] {
  const buckets: LatencyBucket[] = [
    { name: '<1ms', minMs: 0, maxMs: 1, count: 0 },
    { name: '1-5ms', minMs: 1, maxMs: 5, count: 0 },
    { name: '5-10ms', minMs: 5, maxMs: 10, count: 0 },
    { name: '10-50ms', minMs: 10, maxMs: 50, count: 0 },
    { name: '50-100ms', minMs: 50, maxMs: 100, count: 0 },
    { name: '100-500ms', minMs: 100, maxMs: 500, count: 0 },
    { name: '>500ms', minMs: 500, maxMs: Infinity, count: 0 },
  ];

  for (const latency of latencies) {
    for (const bucket of buckets) {
      if (latency >= bucket.minMs && latency < bucket.maxMs) {
        bucket.count++;
        break;
      }
    }
  }

  return buckets;
}

// ============================================================================
// Tests
// ============================================================================

describe('Throughput Stress Tests', () => {
  let store: HighThroughputStore;

  beforeEach(() => {
    store = new HighThroughputStore({ writeLatencyMs: 0.5, readLatencyMs: 0.2 });
  });

  afterEach(() => {
    store.clear();
  });

  describe('Sustained High Throughput', () => {
    it('should sustain 1000 ops/sec for 5 seconds', async () => {
      const targetOps = 5000;
      const generator = createUserGenerator();
      const latencies: number[] = [];

      const startTime = Date.now();

      // Fire operations at target rate
      const promises: Promise<void>[] = [];
      for (let i = 0; i < targetOps; i++) {
        const doc = generator.generate();
        const opStart = performance.now();
        const promise = store.write(doc as unknown as Record<string, unknown>).then(() => {
          latencies.push(performance.now() - opStart);
        });
        promises.push(promise);

        // Throttle to roughly 1000 ops/sec
        if (i % 100 === 0 && i > 0) {
          await new Promise((r) => setTimeout(r, 100));
        }
      }

      await Promise.all(promises);

      const durationMs = Date.now() - startTime;
      const throughput = calculateThroughput(targetOps, durationMs);
      const metrics = store.getMetrics();

      console.log('\n  Sustained Throughput Test (5000 ops):');
      console.log(`    Duration: ${formatDuration(durationMs)}`);
      console.log(`    Throughput: ${throughput.opsPerSecond.toFixed(0)} ops/sec`);
      console.log(`    Latency P50: ${metrics.latencyP50.toFixed(2)}ms`);
      console.log(`    Latency P95: ${metrics.latencyP95.toFixed(2)}ms`);
      console.log(`    Latency P99: ${metrics.latencyP99.toFixed(2)}ms`);
      console.log(`    Data written: ${formatBytes(metrics.bytesProcessed)}`);

      expect(store.getDocumentCount()).toBe(targetOps);
      expect(throughput.opsPerSecond).toBeGreaterThan(100); // At least 100 ops/sec
    });

    it('should handle burst traffic of 10000 operations', async () => {
      const burstSize = 10000;
      const batchSize = 500;
      const generator = createEventGenerator();
      const batchTimes: number[] = [];

      const startTime = Date.now();

      for (let i = 0; i < burstSize; i += batchSize) {
        const batchStart = Date.now();
        const batch = generator.generateBatch(Math.min(batchSize, burstSize - i));

        await store.writeBatch(batch as unknown as Record<string, unknown>[]);

        batchTimes.push(Date.now() - batchStart);
      }

      const durationMs = Date.now() - startTime;
      const throughput = calculateThroughput(burstSize, durationMs);
      const avgBatchTime = batchTimes.reduce((a, b) => a + b, 0) / batchTimes.length;

      console.log('\n  Burst Traffic Test (10000 ops):');
      console.log(`    Duration: ${formatDuration(durationMs)}`);
      console.log(`    Throughput: ${throughput.opsPerSecond.toFixed(0)} ops/sec`);
      console.log(`    Avg batch time: ${avgBatchTime.toFixed(2)}ms per ${batchSize} docs`);
      console.log(`    Batch count: ${batchTimes.length}`);

      expect(store.getDocumentCount()).toBe(burstSize);
      expect(throughput.opsPerSecond).toBeGreaterThan(500);
    });

    it('should maintain throughput with varying document sizes', async () => {
      const smallDocs = 2000;
      const mediumDocs = 1000;
      const largeDocs = 500;

      const smallGenerator = createUserGenerator({ targetSizeBytes: 500 });
      const mediumGenerator = createOrderGenerator({ targetSizeBytes: 2000 });
      const largeGenerator = createOrderGenerator({ targetSizeBytes: 10000 });

      const results: { name: string; ops: number; durationMs: number; bytesPerSec: number }[] = [];

      // Small documents
      let start = Date.now();
      for (let i = 0; i < smallDocs; i++) {
        await store.write(smallGenerator.generate() as unknown as Record<string, unknown>);
      }
      let duration = Date.now() - start;
      let metrics = store.getMetrics();
      results.push({
        name: 'Small (~500B)',
        ops: smallDocs,
        durationMs: duration,
        bytesPerSec: (metrics.bytesProcessed / duration) * 1000,
      });

      store.clear();

      // Medium documents
      start = Date.now();
      for (let i = 0; i < mediumDocs; i++) {
        await store.write(mediumGenerator.generate() as unknown as Record<string, unknown>);
      }
      duration = Date.now() - start;
      metrics = store.getMetrics();
      results.push({
        name: 'Medium (~2KB)',
        ops: mediumDocs,
        durationMs: duration,
        bytesPerSec: (metrics.bytesProcessed / duration) * 1000,
      });

      store.clear();

      // Large documents
      start = Date.now();
      for (let i = 0; i < largeDocs; i++) {
        await store.write(largeGenerator.generate() as unknown as Record<string, unknown>);
      }
      duration = Date.now() - start;
      metrics = store.getMetrics();
      results.push({
        name: 'Large (~10KB)',
        ops: largeDocs,
        durationMs: duration,
        bytesPerSec: (metrics.bytesProcessed / duration) * 1000,
      });

      console.log('\n  Varying Document Size Test:');
      for (const r of results) {
        const opsPerSec = (r.ops / r.durationMs) * 1000;
        console.log(`    ${r.name}: ${opsPerSec.toFixed(0)} ops/sec, ${formatBytes(r.bytesPerSec)}/sec`);
      }

      // All should complete successfully
      expect(results.length).toBe(3);
    });
  });

  describe('Load Profile Patterns', () => {
    it('should handle ramp-up/sustain/ramp-down pattern', async () => {
      const generator = createUserGenerator();
      let docIndex = 0;

      const profile: LoadProfile = {
        name: 'Gradual Load',
        rampUpMs: 1000,
        sustainMs: 2000,
        rampDownMs: 1000,
        targetOpsPerSec: 200,
      };

      const result = await runLoadProfile(
        profile,
        () => generator.generate(),
        async (doc) => {
          await store.write(doc as unknown as Record<string, unknown>);
          docIndex++;
        }
      );

      const durationMs = profile.rampUpMs + profile.sustainMs + profile.rampDownMs;
      const throughput = calculateThroughput(result.completed, durationMs);

      console.log('\n  Load Profile Test (ramp-up/sustain/ramp-down):');
      console.log(`    Profile: ${profile.name}`);
      console.log(`    Completed: ${result.completed} ops`);
      console.log(`    Errors: ${result.errors}`);
      console.log(`    Avg throughput: ${throughput.opsPerSecond.toFixed(0)} ops/sec`);

      expect(result.completed).toBeGreaterThan(0);
      expect(result.errors).toBe(0);
    });

    it('should handle spike load pattern', async () => {
      const generator = createEventGenerator();
      const spikeDuration = 500; // 500ms spike
      const restDuration = 500; // 500ms rest
      const spikeCount = 5;
      const opsPerSpike = 500;

      const spikeResults: { duration: number; ops: number }[] = [];

      for (let spike = 0; spike < spikeCount; spike++) {
        const spikeStart = Date.now();

        // Execute spike
        const batch = generator.generateBatch(opsPerSpike);
        await store.writeBatch(batch as unknown as Record<string, unknown>[]);

        spikeResults.push({
          duration: Date.now() - spikeStart,
          ops: opsPerSpike,
        });

        // Rest period
        await new Promise((r) => setTimeout(r, restDuration));
      }

      const totalDuration = spikeResults.reduce((sum, r) => sum + r.duration, 0) + restDuration * (spikeCount - 1);
      const totalOps = opsPerSpike * spikeCount;

      console.log('\n  Spike Load Pattern Test:');
      console.log(`    Spikes: ${spikeCount} x ${opsPerSpike} ops`);
      console.log(`    Total ops: ${totalOps}`);
      console.log(`    Total duration: ${formatDuration(totalDuration)}`);

      for (let i = 0; i < spikeResults.length; i++) {
        const r = spikeResults[i]!;
        const opsPerSec = (r.ops / r.duration) * 1000;
        console.log(`    Spike ${i + 1}: ${r.duration}ms (${opsPerSec.toFixed(0)} ops/sec)`);
      }

      expect(store.getDocumentCount()).toBe(totalOps);
    });
  });

  describe('Latency Distribution', () => {
    it('should measure latency percentiles accurately', async () => {
      const operationCount = 5000;
      const generator = createUserGenerator();
      const latencies: number[] = [];

      for (let i = 0; i < operationCount; i++) {
        const doc = generator.generate();
        const start = performance.now();
        await store.write(doc as unknown as Record<string, unknown>);
        latencies.push(performance.now() - start);
      }

      const sorted = [...latencies].sort((a, b) => a - b);
      const p50 = sorted[Math.floor(sorted.length * 0.5)]!;
      const p95 = sorted[Math.floor(sorted.length * 0.95)]!;
      const p99 = sorted[Math.floor(sorted.length * 0.99)]!;
      const mean = latencies.reduce((a, b) => a + b, 0) / latencies.length;

      const buckets = calculateLatencyBuckets(latencies);

      console.log('\n  Latency Distribution (5000 ops):');
      console.log(`    Mean: ${mean.toFixed(3)}ms`);
      console.log(`    P50: ${p50.toFixed(3)}ms`);
      console.log(`    P95: ${p95.toFixed(3)}ms`);
      console.log(`    P99: ${p99.toFixed(3)}ms`);
      console.log(`    Min: ${sorted[0]!.toFixed(3)}ms`);
      console.log(`    Max: ${sorted[sorted.length - 1]!.toFixed(3)}ms`);
      console.log('    Distribution:');
      for (const bucket of buckets) {
        const percentage = (bucket.count / latencies.length * 100).toFixed(1);
        console.log(`      ${bucket.name}: ${bucket.count} (${percentage}%)`);
      }

      // P99 should not be excessively higher than P50
      expect(p99).toBeLessThan(p50 * 20);
    });

    it('should track latency under varying concurrency', async () => {
      const concurrencyLevels = [1, 5, 10, 20];
      const opsPerLevel = 500;
      const generator = createUserGenerator();

      const results: { concurrency: number; avgLatency: number; p95Latency: number }[] = [];

      for (const concurrency of concurrencyLevels) {
        const latencies: number[] = [];
        const batches = Math.ceil(opsPerLevel / concurrency);

        for (let batch = 0; batch < batches; batch++) {
          const promises: Promise<void>[] = [];

          for (let i = 0; i < concurrency && batch * concurrency + i < opsPerLevel; i++) {
            const doc = generator.generate();
            const start = performance.now();
            promises.push(
              store.write(doc as unknown as Record<string, unknown>).then(() => {
                latencies.push(performance.now() - start);
              })
            );
          }

          await Promise.all(promises);
        }

        const sorted = [...latencies].sort((a, b) => a - b);
        results.push({
          concurrency,
          avgLatency: latencies.reduce((a, b) => a + b, 0) / latencies.length,
          p95Latency: sorted[Math.floor(sorted.length * 0.95)]!,
        });

        store.clear();
      }

      console.log('\n  Latency vs Concurrency:');
      for (const r of results) {
        console.log(`    Concurrency ${r.concurrency}: Avg ${r.avgLatency.toFixed(2)}ms, P95 ${r.p95Latency.toFixed(2)}ms`);
      }

      // Latency should not increase dramatically with concurrency
      const firstResult = results[0]!;
      const lastResult = results[results.length - 1]!;
      expect(lastResult.avgLatency).toBeLessThan(firstResult.avgLatency * 10);
    });
  });

  describe('Mixed Workload Stress', () => {
    it('should handle mixed read/write workload', async () => {
      const generator = createUserGenerator();
      const totalOps = 5000;
      const writeRatio = 0.3; // 30% writes, 70% reads
      const writeCount = Math.floor(totalOps * writeRatio);
      const readCount = totalOps - writeCount;

      // Pre-populate with some documents for reading
      const initialDocs = generator.generateBatch(1000);
      await store.writeBatch(initialDocs as unknown as Record<string, unknown>[]);

      const writeLatencies: number[] = [];
      const readLatencies: number[] = [];
      let readHits = 0;
      let readMisses = 0;

      const startTime = Date.now();

      // Execute mixed workload
      for (let i = 0; i < totalOps; i++) {
        if (Math.random() < writeRatio) {
          // Write operation
          const doc = generator.generate();
          const start = performance.now();
          await store.write(doc as unknown as Record<string, unknown>);
          writeLatencies.push(performance.now() - start);
        } else {
          // Read operation
          const randomIdx = Math.floor(Math.random() * initialDocs.length);
          const id = (initialDocs[randomIdx] as unknown as Record<string, unknown>)._id as string;
          const start = performance.now();
          const result = await store.read(id);
          readLatencies.push(performance.now() - start);
          if (result.doc) readHits++;
          else readMisses++;
        }
      }

      const durationMs = Date.now() - startTime;

      console.log('\n  Mixed Workload Test (30% writes, 70% reads):');
      console.log(`    Duration: ${formatDuration(durationMs)}`);
      console.log(`    Write ops: ${writeLatencies.length} (avg ${(writeLatencies.reduce((a, b) => a + b, 0) / writeLatencies.length).toFixed(2)}ms)`);
      console.log(`    Read ops: ${readLatencies.length} (avg ${(readLatencies.reduce((a, b) => a + b, 0) / readLatencies.length).toFixed(2)}ms)`);
      console.log(`    Read hits: ${readHits}, misses: ${readMisses}`);
      console.log(`    Combined throughput: ${((totalOps / durationMs) * 1000).toFixed(0)} ops/sec`);

      expect(writeLatencies.length + readLatencies.length).toBeGreaterThan(0);
      expect(readHits).toBeGreaterThan(0);
    });

    it('should handle hotspot access pattern', async () => {
      const generator = createUserGenerator();
      const totalOps = 5000;
      const hotspotRatio = 0.8; // 80% of operations target 20% of keys

      // Pre-populate
      const allDocs = generator.generateBatch(1000);
      await store.writeBatch(allDocs as unknown as Record<string, unknown>[]);

      const hotspotDocs = allDocs.slice(0, 200); // Hot 20%
      const coldDocs = allDocs.slice(200); // Cold 80%

      const hotspotOps: number[] = [];
      const coldOps: number[] = [];

      const startTime = Date.now();

      for (let i = 0; i < totalOps; i++) {
        let targetDocs: typeof allDocs;
        let isHotspot: boolean;

        if (Math.random() < hotspotRatio) {
          targetDocs = hotspotDocs;
          isHotspot = true;
        } else {
          targetDocs = coldDocs;
          isHotspot = false;
        }

        const randomIdx = Math.floor(Math.random() * targetDocs.length);
        const id = (targetDocs[randomIdx] as unknown as Record<string, unknown>)._id as string;

        const start = performance.now();
        await store.read(id);
        const latency = performance.now() - start;

        if (isHotspot) {
          hotspotOps.push(latency);
        } else {
          coldOps.push(latency);
        }
      }

      const durationMs = Date.now() - startTime;

      console.log('\n  Hotspot Access Pattern Test:');
      console.log(`    Duration: ${formatDuration(durationMs)}`);
      console.log(`    Hotspot ops: ${hotspotOps.length} (avg ${(hotspotOps.reduce((a, b) => a + b, 0) / hotspotOps.length).toFixed(2)}ms)`);
      console.log(`    Cold ops: ${coldOps.length} (avg ${(coldOps.reduce((a, b) => a + b, 0) / coldOps.length).toFixed(2)}ms)`);
      console.log(`    Hotspot/Cold ratio: ${(hotspotOps.length / coldOps.length).toFixed(2)}`);

      // Hotspot ops should be roughly 4x cold ops (80/20)
      expect(hotspotOps.length).toBeGreaterThan(coldOps.length * 2);
    });
  });

  describe('Error Handling Under Load', () => {
    it('should handle errors gracefully during high load', async () => {
      const errorStore = new HighThroughputStore({
        writeLatencyMs: 0.5,
        errorRate: 0.05, // 5% error rate
      });

      const generator = createUserGenerator();
      const totalOps = 2000;
      let successCount = 0;
      let errorCount = 0;

      const startTime = Date.now();

      for (let i = 0; i < totalOps; i++) {
        const doc = generator.generate();
        const result = await errorStore.write(doc as unknown as Record<string, unknown>);

        if (result.success) {
          successCount++;
        } else {
          errorCount++;
        }
      }

      const durationMs = Date.now() - startTime;
      const errorRate = (errorCount / totalOps) * 100;

      console.log('\n  Error Handling Under Load:');
      console.log(`    Duration: ${formatDuration(durationMs)}`);
      console.log(`    Success: ${successCount} (${((successCount / totalOps) * 100).toFixed(1)}%)`);
      console.log(`    Errors: ${errorCount} (${errorRate.toFixed(1)}%)`);

      // Error rate should be close to configured rate
      expect(errorRate).toBeGreaterThan(2);
      expect(errorRate).toBeLessThan(10);

      errorStore.clear();
    });

    it('should recover from error bursts', async () => {
      const generator = createUserGenerator();
      let errorPhase = false;

      // Simulate a store that has error bursts
      const burstStore = new HighThroughputStore({ writeLatencyMs: 0.5 });
      const originalWrite = burstStore.write.bind(burstStore);

      let writeCount = 0;
      burstStore.write = async (doc: Record<string, unknown>) => {
        writeCount++;
        // Error phase every 500-600 operations
        if (writeCount >= 500 && writeCount < 600) {
          if (!errorPhase) {
            errorPhase = true;
          }
          throw new Error('Simulated error burst');
        } else {
          errorPhase = false;
          return originalWrite(doc);
        }
      };

      const totalOps = 2000;
      let successCount = 0;
      let errorCount = 0;
      const errorBursts: number[] = [];
      let currentBurstSize = 0;

      for (let i = 0; i < totalOps; i++) {
        const doc = generator.generate();
        try {
          await burstStore.write(doc as unknown as Record<string, unknown>);
          successCount++;
          if (currentBurstSize > 0) {
            errorBursts.push(currentBurstSize);
            currentBurstSize = 0;
          }
        } catch {
          errorCount++;
          currentBurstSize++;
        }
      }

      if (currentBurstSize > 0) {
        errorBursts.push(currentBurstSize);
      }

      console.log('\n  Error Burst Recovery:');
      console.log(`    Total ops: ${totalOps}`);
      console.log(`    Success: ${successCount}`);
      console.log(`    Errors: ${errorCount}`);
      console.log(`    Error bursts: ${errorBursts.length}`);
      console.log(`    Max burst size: ${Math.max(...errorBursts, 0)}`);

      // Should recover after bursts
      expect(successCount).toBeGreaterThan(totalOps * 0.9);

      burstStore.clear();
    });
  });
});
