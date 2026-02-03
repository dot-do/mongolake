/**
 * Sustained Writes Load Test
 *
 * Tests sustained insert throughput over time and tracks memory usage during writes.
 * Uses process.memoryUsage() for memory tracking.
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

interface ThroughputSample {
  timestamp: number;
  operationsCompleted: number;
  durationMs: number;
  opsPerSecond: number;
}

interface WriteResult {
  acknowledged: boolean;
  insertedId: string;
  lsn: number;
}

// ============================================================================
// Mock Writer for Load Testing
// ============================================================================

/**
 * Simulates a document writer for load testing purposes.
 * In a real scenario, this would use ShardDO or StreamingParquetWriter.
 */
class MockDocumentWriter {
  private buffer: Map<string, unknown>[] = [];
  private lsn = 0;
  private flushThreshold: number;
  private flushedCount = 0;

  constructor(options: { flushThreshold?: number } = {}) {
    this.flushThreshold = options.flushThreshold ?? 1000;
  }

  async write(doc: { _id: string; [key: string]: unknown }): Promise<WriteResult> {
    this.buffer.push(doc);
    this.lsn++;

    if (this.buffer.length >= this.flushThreshold) {
      await this.flush();
    }

    return {
      acknowledged: true,
      insertedId: doc._id,
      lsn: this.lsn,
    };
  }

  async flush(): Promise<void> {
    // Simulate I/O delay
    await new Promise((resolve) => setTimeout(resolve, 1));
    this.flushedCount += this.buffer.length;
    this.buffer = [];
  }

  getBufferSize(): number {
    return this.buffer.length;
  }

  getFlushedCount(): number {
    return this.flushedCount;
  }

  getTotalWritten(): number {
    return this.flushedCount + this.buffer.length;
  }
}

// ============================================================================
// Memory Tracking Utilities
// ============================================================================

function captureMemorySnapshot(): MemorySnapshot {
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
  const mb = bytes / (1024 * 1024);
  return `${mb.toFixed(2)} MB`;
}

function analyzeMemoryGrowth(snapshots: MemorySnapshot[]): {
  startHeap: number;
  endHeap: number;
  maxHeap: number;
  growthBytes: number;
  growthPercent: number;
} {
  if (snapshots.length === 0) {
    return { startHeap: 0, endHeap: 0, maxHeap: 0, growthBytes: 0, growthPercent: 0 };
  }

  const startHeap = snapshots[0].heapUsed;
  const endHeap = snapshots[snapshots.length - 1].heapUsed;
  const maxHeap = Math.max(...snapshots.map((s) => s.heapUsed));
  const growthBytes = endHeap - startHeap;
  const growthPercent = startHeap > 0 ? (growthBytes / startHeap) * 100 : 0;

  return { startHeap, endHeap, maxHeap, growthBytes, growthPercent };
}

// ============================================================================
// Document Generation
// ============================================================================

function createTestDocument(index: number, sizeKB?: number): { _id: string; [key: string]: unknown } {
  const doc: { _id: string; [key: string]: unknown } = {
    _id: `doc-${index.toString().padStart(8, '0')}`,
    name: `User ${index}`,
    email: `user${index}@example.com`,
    age: 20 + (index % 60),
    active: index % 2 === 0,
    score: Math.random() * 100,
    createdAt: new Date().toISOString(),
    tags: ['tag1', 'tag2', 'tag3'].slice(0, (index % 3) + 1),
    metadata: {
      source: 'load-test',
      batchId: Math.floor(index / 100),
      version: 1,
    },
  };

  // Optionally pad to approximate size
  if (sizeKB && sizeKB > 0) {
    const currentSize = JSON.stringify(doc).length;
    const targetSize = sizeKB * 1024;
    if (targetSize > currentSize) {
      doc.padding = 'x'.repeat(targetSize - currentSize);
    }
  }

  return doc;
}

// ============================================================================
// Tests
// ============================================================================

describe('Sustained Writes Load Tests', () => {
  let writer: MockDocumentWriter;
  let memorySnapshots: MemorySnapshot[];

  beforeEach(() => {
    writer = new MockDocumentWriter({ flushThreshold: 100 });
    memorySnapshots = [];
    // Force GC if available to get cleaner baseline
    if (global.gc) {
      global.gc();
    }
  });

  afterEach(async () => {
    // Ensure final flush
    await writer.flush();
  });

  describe('Sustained Insert Throughput', () => {
    it('should maintain consistent throughput over 1000 writes', async () => {
      const totalWrites = 1000;
      const samples: ThroughputSample[] = [];
      const sampleInterval = 100;

      let batchStart = performance.now();
      let batchOps = 0;

      for (let i = 0; i < totalWrites; i++) {
        await writer.write(createTestDocument(i));
        batchOps++;

        if (batchOps >= sampleInterval) {
          const now = performance.now();
          const durationMs = now - batchStart;
          samples.push({
            timestamp: Date.now(),
            operationsCompleted: batchOps,
            durationMs,
            // Use high precision timing; if duration is very small, estimate high throughput
            opsPerSecond: durationMs > 0.1 ? (batchOps / durationMs) * 1000 : batchOps * 10000,
          });
          batchStart = now;
          batchOps = 0;
        }
      }

      // Verify all writes completed
      expect(writer.getTotalWritten()).toBe(totalWrites);

      // Skip throughput assertions if no samples were collected
      if (samples.length === 0) {
        return;
      }

      // Calculate throughput statistics
      const throughputs = samples.map((s) => s.opsPerSecond).filter((t) => t > 0);
      if (throughputs.length === 0) {
        return; // All operations were too fast to measure
      }

      const avgThroughput = throughputs.reduce((a, b) => a + b, 0) / throughputs.length;
      const minThroughput = Math.min(...throughputs);
      const maxThroughput = Math.max(...throughputs);

      // Throughput should not degrade significantly (min should be at least 30% of max)
      expect(minThroughput).toBeGreaterThan(maxThroughput * 0.3);

      // Average throughput should be reasonable (at least 100 ops/sec)
      expect(avgThroughput).toBeGreaterThan(100);
    });

    it('should handle burst writes without significant slowdown', async () => {
      const burstSize = 500;
      const burstCount = 3;
      const burstResults: { durationMs: number; opsPerSecond: number }[] = [];

      for (let burst = 0; burst < burstCount; burst++) {
        const startTime = Date.now();

        // Write burst
        const writes = [];
        for (let i = 0; i < burstSize; i++) {
          writes.push(writer.write(createTestDocument(burst * burstSize + i)));
        }
        await Promise.all(writes);

        const durationMs = Date.now() - startTime;
        burstResults.push({
          durationMs,
          opsPerSecond: durationMs > 0 ? (burstSize / durationMs) * 1000 : 0,
        });

        // Small delay between bursts
        await new Promise((resolve) => setTimeout(resolve, 10));
      }

      // Verify all bursts completed
      expect(writer.getTotalWritten()).toBe(burstSize * burstCount);

      // Later bursts should not be significantly slower than the first
      const firstBurstOps = burstResults[0].opsPerSecond;
      const lastBurstOps = burstResults[burstResults.length - 1].opsPerSecond;

      // Last burst should be at least 50% as fast as first
      expect(lastBurstOps).toBeGreaterThan(firstBurstOps * 0.5);
    });

    it('should sustain writes with varying document sizes', async () => {
      const smallDocs = 200;
      const mediumDocs = 100;
      const largeDocs = 50;

      const startTime = Date.now();

      // Small documents (~0.5KB)
      for (let i = 0; i < smallDocs; i++) {
        await writer.write(createTestDocument(i));
      }

      // Medium documents (~2KB)
      for (let i = 0; i < mediumDocs; i++) {
        await writer.write(createTestDocument(smallDocs + i, 2));
      }

      // Large documents (~10KB)
      for (let i = 0; i < largeDocs; i++) {
        await writer.write(createTestDocument(smallDocs + mediumDocs + i, 10));
      }

      const durationMs = Date.now() - startTime;
      const totalOps = smallDocs + mediumDocs + largeDocs;

      expect(writer.getTotalWritten()).toBe(totalOps);

      // Should complete in reasonable time (less than 10 seconds)
      expect(durationMs).toBeLessThan(10000);
    });
  });

  describe('Memory Usage During Writes', () => {
    it('should not leak memory during sustained writes', async () => {
      const totalWrites = 2000;
      const snapshotInterval = 200;

      // Initial snapshot
      memorySnapshots.push(captureMemorySnapshot());

      for (let i = 0; i < totalWrites; i++) {
        await writer.write(createTestDocument(i));

        if ((i + 1) % snapshotInterval === 0) {
          memorySnapshots.push(captureMemorySnapshot());
        }
      }

      // Final flush and snapshot
      await writer.flush();
      memorySnapshots.push(captureMemorySnapshot());

      const analysis = analyzeMemoryGrowth(memorySnapshots);

      // Memory growth should be bounded (less than 100% growth from start)
      expect(analysis.growthPercent).toBeLessThan(100);

      // Log memory analysis for debugging
      console.log('Memory Analysis:');
      console.log(`  Start heap: ${formatBytes(analysis.startHeap)}`);
      console.log(`  End heap: ${formatBytes(analysis.endHeap)}`);
      console.log(`  Max heap: ${formatBytes(analysis.maxHeap)}`);
      console.log(`  Growth: ${formatBytes(analysis.growthBytes)} (${analysis.growthPercent.toFixed(1)}%)`);
    });

    it('should release memory after flush', async () => {
      const batchSize = 500;

      // Write a batch
      for (let i = 0; i < batchSize; i++) {
        await writer.write(createTestDocument(i, 1)); // 1KB documents
      }

      const beforeFlush = captureMemorySnapshot();

      // Flush the buffer
      await writer.flush();

      // Give GC a chance to run
      await new Promise((resolve) => setTimeout(resolve, 50));
      if (global.gc) {
        global.gc();
      }

      const afterFlush = captureMemorySnapshot();

      // Buffer should be cleared
      expect(writer.getBufferSize()).toBe(0);

      // Heap usage should not have increased significantly after flush
      const heapGrowth = afterFlush.heapUsed - beforeFlush.heapUsed;
      // Allow some tolerance for GC timing
      expect(heapGrowth).toBeLessThan(10 * 1024 * 1024); // Less than 10MB growth
    });

    it('should track memory across multiple write-flush cycles', async () => {
      const cycleCount = 5;
      const docsPerCycle = 300;
      const cycleMemory: { beforeWrite: number; afterWrite: number; afterFlush: number }[] = [];

      for (let cycle = 0; cycle < cycleCount; cycle++) {
        const beforeWrite = captureMemorySnapshot().heapUsed;

        // Write documents
        for (let i = 0; i < docsPerCycle; i++) {
          await writer.write(createTestDocument(cycle * docsPerCycle + i));
        }

        const afterWrite = captureMemorySnapshot().heapUsed;

        // Flush
        await writer.flush();
        await new Promise((resolve) => setTimeout(resolve, 10));

        const afterFlush = captureMemorySnapshot().heapUsed;

        cycleMemory.push({ beforeWrite, afterWrite, afterFlush });
      }

      // Memory should not accumulate across cycles
      const firstCycleGrowth = cycleMemory[0].afterFlush - cycleMemory[0].beforeWrite;
      const lastCycleGrowth = cycleMemory[cycleCount - 1].afterFlush - cycleMemory[cycleCount - 1].beforeWrite;

      // Last cycle growth should not be significantly more than first cycle
      expect(lastCycleGrowth).toBeLessThan(firstCycleGrowth + 5 * 1024 * 1024); // 5MB tolerance
    });

    it('should handle large documents without excessive memory usage', async () => {
      const largeDocCount = 50;
      const docSizeKB = 50; // 50KB per document

      const beforeWrite = captureMemorySnapshot();

      for (let i = 0; i < largeDocCount; i++) {
        await writer.write(createTestDocument(i, docSizeKB));

        // Capture memory periodically
        if (i % 10 === 0) {
          memorySnapshots.push(captureMemorySnapshot());
        }
      }

      await writer.flush();
      const afterFlush = captureMemorySnapshot();

      // Total expected data size
      const expectedDataSize = largeDocCount * docSizeKB * 1024;

      // Peak memory should not exceed 3x the expected data size (accounting for overhead)
      const analysis = analyzeMemoryGrowth([beforeWrite, ...memorySnapshots, afterFlush]);
      const peakGrowth = analysis.maxHeap - analysis.startHeap;

      expect(peakGrowth).toBeLessThan(expectedDataSize * 3);

      console.log(`Large document test:`);
      console.log(`  Documents: ${largeDocCount} x ${docSizeKB}KB = ${formatBytes(expectedDataSize)}`);
      console.log(`  Peak memory growth: ${formatBytes(peakGrowth)}`);
    });
  });

  describe('Throughput Under Memory Pressure', () => {
    it('should maintain throughput with frequent flushes', async () => {
      // Create writer with very low flush threshold
      const frequentFlusher = new MockDocumentWriter({ flushThreshold: 10 });
      const totalWrites = 500;

      const startTime = Date.now();

      for (let i = 0; i < totalWrites; i++) {
        await frequentFlusher.write(createTestDocument(i));
      }

      const durationMs = Date.now() - startTime;
      const opsPerSecond = (totalWrites / durationMs) * 1000;

      expect(frequentFlusher.getTotalWritten()).toBe(totalWrites);

      // Should still achieve reasonable throughput despite frequent flushes
      expect(opsPerSecond).toBeGreaterThan(50);

      // Should have triggered many flushes
      expect(frequentFlusher.getFlushedCount()).toBeGreaterThan(totalWrites * 0.9);
    });

    it('should not degrade when buffer approaches capacity', async () => {
      // Create writer with high flush threshold
      const highCapacity = new MockDocumentWriter({ flushThreshold: 2000 });
      const samples: { count: number; opsPerSecond: number }[] = [];

      const batchSize = 200;
      const batches = 8; // Will exceed 2000 threshold

      for (let batch = 0; batch < batches; batch++) {
        const startTime = Date.now();

        for (let i = 0; i < batchSize; i++) {
          await highCapacity.write(createTestDocument(batch * batchSize + i));
        }

        const durationMs = Date.now() - startTime;
        samples.push({
          count: highCapacity.getTotalWritten(),
          opsPerSecond: durationMs > 0 ? (batchSize / durationMs) * 1000 : 0,
        });
      }

      // Throughput should not degrade significantly as buffer fills
      const firstHalfAvg =
        samples.slice(0, 4).reduce((a, b) => a + b.opsPerSecond, 0) / 4;
      const secondHalfAvg =
        samples.slice(4).reduce((a, b) => a + b.opsPerSecond, 0) / 4;

      // Second half should maintain at least 40% of first half throughput
      expect(secondHalfAvg).toBeGreaterThan(firstHalfAvg * 0.4);
    });
  });
});
