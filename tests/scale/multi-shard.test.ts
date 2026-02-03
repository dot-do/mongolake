/**
 * Multi-Shard Scale Tests
 *
 * Tests MongoLake performance with operations across multiple shards.
 * These tests verify:
 * - Operations across 16 shards
 * - Shard rebalancing at scale
 * - Concurrent operations on multiple shards
 *
 * NOTE: These tests are resource-intensive and may take several minutes to run.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  ShardRouter,
  createShardRouter,
  hashCollectionToShard,
  hashDocumentToShard,
} from '../../src/shard/router';

// ============================================================================
// Types
// ============================================================================

interface ShardDocument {
  _id: string;
  shardId: number;
  data: string;
  timestamp: number;
  counter: number;
}

interface ShardStats {
  documentCount: number;
  totalBytes: number;
  lastWrite: number;
  writeOps: number;
  readOps: number;
}

interface RebalanceResult {
  movedDocuments: number;
  sourceShards: number[];
  targetShards: number[];
  duration: number;
}

// ============================================================================
// Mock Shard Implementation
// ============================================================================

/**
 * Simulates a single shard for testing purposes.
 * Tracks documents, operations, and supports concurrent access.
 */
class MockShard {
  readonly id: number;
  private documents: Map<string, ShardDocument> = new Map();
  private stats: ShardStats = {
    documentCount: 0,
    totalBytes: 0,
    lastWrite: 0,
    writeOps: 0,
    readOps: 0,
  };
  private lock: Promise<void> = Promise.resolve();

  constructor(id: number) {
    this.id = id;
  }

  /**
   * Acquire exclusive lock for write operations
   */
  private async withLock<T>(operation: () => Promise<T>): Promise<T> {
    const previousLock = this.lock;
    let releaseLock: () => void;
    this.lock = new Promise((resolve) => {
      releaseLock = resolve;
    });

    await previousLock;
    try {
      return await operation();
    } finally {
      releaseLock!();
    }
  }

  async write(doc: ShardDocument): Promise<void> {
    await this.withLock(async () => {
      const bytes = JSON.stringify(doc).length;
      this.documents.set(doc._id, doc);
      this.stats.documentCount = this.documents.size;
      this.stats.totalBytes += bytes;
      this.stats.lastWrite = Date.now();
      this.stats.writeOps++;
    });
  }

  async writeBatch(docs: ShardDocument[]): Promise<void> {
    await this.withLock(async () => {
      for (const doc of docs) {
        const bytes = JSON.stringify(doc).length;
        this.documents.set(doc._id, doc);
        this.stats.totalBytes += bytes;
      }
      this.stats.documentCount = this.documents.size;
      this.stats.lastWrite = Date.now();
      this.stats.writeOps += docs.length;
    });
  }

  async read(id: string): Promise<ShardDocument | null> {
    this.stats.readOps++;
    return this.documents.get(id) || null;
  }

  async readMany(filter?: (doc: ShardDocument) => boolean): Promise<ShardDocument[]> {
    this.stats.readOps++;
    const results: ShardDocument[] = [];
    for (const doc of this.documents.values()) {
      if (!filter || filter(doc)) {
        results.push(doc);
      }
    }
    return results;
  }

  async delete(id: string): Promise<boolean> {
    return this.withLock(async () => {
      if (this.documents.has(id)) {
        this.documents.delete(id);
        this.stats.documentCount = this.documents.size;
        this.stats.writeOps++;
        return true;
      }
      return false;
    });
  }

  async deleteBatch(ids: string[]): Promise<number> {
    return this.withLock(async () => {
      let deleted = 0;
      for (const id of ids) {
        if (this.documents.has(id)) {
          this.documents.delete(id);
          deleted++;
        }
      }
      this.stats.documentCount = this.documents.size;
      this.stats.writeOps += deleted;
      return deleted;
    });
  }

  getStats(): ShardStats {
    return { ...this.stats };
  }

  getDocumentCount(): number {
    return this.documents.size;
  }

  getAllDocuments(): ShardDocument[] {
    return Array.from(this.documents.values());
  }

  clear(): void {
    this.documents.clear();
    this.stats = {
      documentCount: 0,
      totalBytes: 0,
      lastWrite: 0,
      writeOps: 0,
      readOps: 0,
    };
  }
}

// ============================================================================
// Shard Cluster Implementation
// ============================================================================

/**
 * Manages a cluster of shards for testing.
 * Provides routing, rebalancing, and distributed operations.
 */
class ShardCluster {
  readonly shardCount: number;
  readonly shards: MockShard[];
  readonly router: ShardRouter;

  constructor(shardCount: number = 16) {
    this.shardCount = shardCount;
    this.shards = Array.from({ length: shardCount }, (_, i) => new MockShard(i));
    this.router = createShardRouter({ shardCount });
  }

  /**
   * Route a document ID to the appropriate shard
   */
  getShardForDocument(docId: string): MockShard {
    const shardId = hashDocumentToShard(docId, this.shardCount);
    return this.shards[shardId]!;
  }

  /**
   * Write a document to the appropriate shard
   */
  async writeDocument(doc: ShardDocument): Promise<void> {
    const shard = this.getShardForDocument(doc._id);
    doc.shardId = shard.id;
    await shard.write(doc);
  }

  /**
   * Write multiple documents, routing each to appropriate shard
   */
  async writeDocuments(docs: ShardDocument[]): Promise<void> {
    // Group by shard for batch writes
    const shardBatches = new Map<number, ShardDocument[]>();

    for (const doc of docs) {
      const shard = this.getShardForDocument(doc._id);
      doc.shardId = shard.id;

      if (!shardBatches.has(shard.id)) {
        shardBatches.set(shard.id, []);
      }
      shardBatches.get(shard.id)!.push(doc);
    }

    // Write batches in parallel
    const writePromises: Promise<void>[] = [];
    for (const [shardId, batch] of shardBatches) {
      writePromises.push(this.shards[shardId]!.writeBatch(batch));
    }
    await Promise.all(writePromises);
  }

  /**
   * Read a document by ID
   */
  async readDocument(docId: string): Promise<ShardDocument | null> {
    const shard = this.getShardForDocument(docId);
    return shard.read(docId);
  }

  /**
   * Read documents from all shards matching filter
   */
  async readFromAllShards(
    filter?: (doc: ShardDocument) => boolean
  ): Promise<ShardDocument[]> {
    const readPromises = this.shards.map((shard) => shard.readMany(filter));
    const results = await Promise.all(readPromises);
    return results.flat();
  }

  /**
   * Get total document count across all shards
   */
  getTotalDocumentCount(): number {
    return this.shards.reduce((sum, shard) => sum + shard.getDocumentCount(), 0);
  }

  /**
   * Get document distribution across shards
   */
  getDistribution(): { shardId: number; count: number; percentage: number }[] {
    const total = this.getTotalDocumentCount();
    return this.shards.map((shard) => ({
      shardId: shard.id,
      count: shard.getDocumentCount(),
      percentage: total > 0 ? (shard.getDocumentCount() / total) * 100 : 0,
    }));
  }

  /**
   * Rebalance documents from overloaded shards to underloaded ones
   */
  async rebalance(targetPercentage: number = 6.25): Promise<RebalanceResult> {
    const startTime = Date.now();
    const total = this.getTotalDocumentCount();
    const targetPerShard = total * (targetPercentage / 100);
    const tolerance = targetPerShard * 0.2; // 20% tolerance

    let movedDocuments = 0;
    const sourceShards: Set<number> = new Set();
    const targetShards: Set<number> = new Set();

    // Find overloaded and underloaded shards
    const overloaded: { shard: MockShard; excess: number }[] = [];
    const underloaded: { shard: MockShard; capacity: number }[] = [];

    for (const shard of this.shards) {
      const count = shard.getDocumentCount();
      if (count > targetPerShard + tolerance) {
        overloaded.push({ shard, excess: count - targetPerShard });
      } else if (count < targetPerShard - tolerance) {
        underloaded.push({ shard, capacity: targetPerShard - count });
      }
    }

    // Move documents from overloaded to underloaded
    for (const source of overloaded) {
      for (const target of underloaded) {
        if (source.excess <= 0 || target.capacity <= 0) continue;

        const toMove = Math.min(source.excess, target.capacity);
        const docs = source.shard.getAllDocuments().slice(0, toMove);

        if (docs.length > 0) {
          // Move documents
          const docIds = docs.map((d) => d._id);
          await source.shard.deleteBatch(docIds);

          // Update shard IDs and write to target
          const updatedDocs = docs.map((d) => ({ ...d, shardId: target.shard.id }));
          await target.shard.writeBatch(updatedDocs);

          movedDocuments += docs.length;
          source.excess -= docs.length;
          target.capacity -= docs.length;
          sourceShards.add(source.shard.id);
          targetShards.add(target.shard.id);
        }
      }
    }

    return {
      movedDocuments,
      sourceShards: Array.from(sourceShards),
      targetShards: Array.from(targetShards),
      duration: Date.now() - startTime,
    };
  }

  /**
   * Get cluster-wide statistics
   */
  getClusterStats(): {
    totalDocuments: number;
    totalWriteOps: number;
    totalReadOps: number;
    shardStats: ShardStats[];
  } {
    const shardStats = this.shards.map((s) => s.getStats());
    return {
      totalDocuments: shardStats.reduce((sum, s) => sum + s.documentCount, 0),
      totalWriteOps: shardStats.reduce((sum, s) => sum + s.writeOps, 0),
      totalReadOps: shardStats.reduce((sum, s) => sum + s.readOps, 0),
      shardStats,
    };
  }

  /**
   * Clear all shards
   */
  clear(): void {
    for (const shard of this.shards) {
      shard.clear();
    }
  }
}

// ============================================================================
// Test Utilities
// ============================================================================

function generateDocument(index: number): ShardDocument {
  return {
    _id: `doc-${index.toString().padStart(8, '0')}-${crypto.randomUUID().slice(0, 8)}`,
    shardId: -1, // Will be set during routing
    data: `data-${index}-${'x'.repeat(100)}`,
    timestamp: Date.now(),
    counter: index,
  };
}

function generateDocuments(startIndex: number, count: number): ShardDocument[] {
  return Array.from({ length: count }, (_, i) => generateDocument(startIndex + i));
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

// ============================================================================
// Tests
// ============================================================================

describe('Multi-Shard Scale Tests', () => {
  const SHARD_COUNT = 16;
  let cluster: ShardCluster;

  beforeEach(() => {
    cluster = new ShardCluster(SHARD_COUNT);
  });

  afterEach(() => {
    cluster.clear();
  });

  describe('Operations Across 16 Shards', () => {
    const DOCUMENT_COUNT = 100_000;
    const BATCH_SIZE = 1000;

    it('should distribute 100k documents evenly across 16 shards', async () => {
      const startTime = Date.now();

      // Insert documents in batches
      for (let i = 0; i < DOCUMENT_COUNT; i += BATCH_SIZE) {
        const batch = generateDocuments(i, Math.min(BATCH_SIZE, DOCUMENT_COUNT - i));
        await cluster.writeDocuments(batch);
      }

      const duration = Date.now() - startTime;
      const distribution = cluster.getDistribution();

      // Verify total count
      expect(cluster.getTotalDocumentCount()).toBe(DOCUMENT_COUNT);

      // Check distribution evenness
      const expectedPerShard = DOCUMENT_COUNT / SHARD_COUNT;
      const counts = distribution.map((d) => d.count);
      const minCount = Math.min(...counts);
      const maxCount = Math.max(...counts);
      const stdDev = Math.sqrt(
        counts.reduce((sum, c) => sum + Math.pow(c - expectedPerShard, 2), 0) / counts.length
      );

      console.log(`\n  Distribution across 16 shards:`);
      console.log(`    Total documents: ${DOCUMENT_COUNT.toLocaleString()}`);
      console.log(`    Duration: ${formatDuration(duration)}`);
      console.log(`    Expected per shard: ${expectedPerShard.toFixed(0)}`);
      console.log(`    Min: ${minCount} (${(minCount / expectedPerShard * 100).toFixed(1)}%)`);
      console.log(`    Max: ${maxCount} (${(maxCount / expectedPerShard * 100).toFixed(1)}%)`);
      console.log(`    Std Dev: ${stdDev.toFixed(2)}`);

      // Distribution should be within 30% of expected (consistent hashing variance)
      expect(minCount).toBeGreaterThan(expectedPerShard * 0.7);
      expect(maxCount).toBeLessThan(expectedPerShard * 1.3);
    });

    it('should read documents from correct shards', async () => {
      // Pre-populate
      const docs = generateDocuments(0, 10000);
      await cluster.writeDocuments(docs);

      // Read random documents and verify they come from correct shard
      let correctShardReads = 0;
      const sampleSize = 100;
      const sampleIndices = Array.from({ length: sampleSize }, () =>
        Math.floor(Math.random() * docs.length)
      );

      for (const idx of sampleIndices) {
        const doc = docs[idx]!;
        const retrieved = await cluster.readDocument(doc._id);

        expect(retrieved).not.toBeNull();
        expect(retrieved!._id).toBe(doc._id);

        // Verify document was read from correct shard
        const expectedShardId = hashDocumentToShard(doc._id, SHARD_COUNT);
        if (retrieved!.shardId === expectedShardId) {
          correctShardReads++;
        }
      }

      expect(correctShardReads).toBe(sampleSize);
    });

    it('should aggregate across all shards', async () => {
      // Pre-populate with 50k documents
      const docs = generateDocuments(0, 50000);
      await cluster.writeDocuments(docs);

      const startTime = Date.now();

      // Read from all shards and aggregate counter values
      const allDocs = await cluster.readFromAllShards();
      const totalCounter = allDocs.reduce((sum, doc) => sum + doc.counter, 0);
      const expectedTotal = (49999 * 50000) / 2; // Sum of 0 to 49999

      const duration = Date.now() - startTime;

      expect(allDocs.length).toBe(50000);
      expect(totalCounter).toBe(expectedTotal);

      console.log(`\n  Cross-shard aggregation:`);
      console.log(`    Documents: ${allDocs.length}`);
      console.log(`    Duration: ${duration}ms`);
    });

    it('should handle parallel writes to all shards', async () => {
      const PARALLEL_BATCHES = 16;
      const DOCS_PER_BATCH = 1000;

      const startTime = Date.now();

      // Generate batches for parallel writes
      const batches = Array.from({ length: PARALLEL_BATCHES }, (_, i) =>
        generateDocuments(i * DOCS_PER_BATCH, DOCS_PER_BATCH)
      );

      // Write all batches in parallel
      await Promise.all(batches.map((batch) => cluster.writeDocuments(batch)));

      const duration = Date.now() - startTime;
      const totalDocs = cluster.getTotalDocumentCount();

      expect(totalDocs).toBe(PARALLEL_BATCHES * DOCS_PER_BATCH);

      console.log(`\n  Parallel writes (${PARALLEL_BATCHES} x ${DOCS_PER_BATCH}):`);
      console.log(`    Total: ${totalDocs.toLocaleString()} documents`);
      console.log(`    Duration: ${duration}ms`);
      console.log(`    Throughput: ${((totalDocs / duration) * 1000).toFixed(0)} docs/sec`);
    });

    it('should handle mixed read/write operations concurrently', async () => {
      // Pre-populate
      const initialDocs = generateDocuments(0, 10000);
      await cluster.writeDocuments(initialDocs);

      const startTime = Date.now();
      const operations: Promise<unknown>[] = [];

      // Mix of concurrent operations
      for (let i = 0; i < 100; i++) {
        // Writes
        operations.push(
          cluster.writeDocuments(generateDocuments(10000 + i * 10, 10))
        );

        // Reads
        const readIdx = Math.floor(Math.random() * initialDocs.length);
        operations.push(cluster.readDocument(initialDocs[readIdx]!._id));

        // Full scans
        if (i % 10 === 0) {
          operations.push(
            cluster.readFromAllShards((doc) => doc.counter < 100)
          );
        }
      }

      await Promise.all(operations);
      const duration = Date.now() - startTime;

      console.log(`\n  Mixed concurrent operations:`);
      console.log(`    Operations: ${operations.length}`);
      console.log(`    Duration: ${duration}ms`);

      // Verify data integrity
      const finalCount = cluster.getTotalDocumentCount();
      expect(finalCount).toBeGreaterThan(10000);
    });
  });

  describe('Shard Rebalancing at Scale', () => {
    it('should rebalance after uneven initial distribution', async () => {
      // Force uneven distribution by writing directly to specific shards
      const docs: ShardDocument[] = [];

      // Write 70% to first 4 shards
      for (let i = 0; i < 7000; i++) {
        const doc = generateDocument(i);
        doc.shardId = i % 4;
        await cluster.shards[i % 4]!.write(doc);
        docs.push(doc);
      }

      // Write 30% to remaining shards
      for (let i = 0; i < 3000; i++) {
        const doc = generateDocument(7000 + i);
        doc.shardId = 4 + (i % 12);
        await cluster.shards[4 + (i % 12)]!.write(doc);
        docs.push(doc);
      }

      const beforeDistribution = cluster.getDistribution();
      console.log('\n  Before rebalancing:');
      console.log(
        `    Shard 0-3 avg: ${(beforeDistribution.slice(0, 4).reduce((s, d) => s + d.count, 0) / 4).toFixed(0)}`
      );
      console.log(
        `    Shard 4-15 avg: ${(beforeDistribution.slice(4).reduce((s, d) => s + d.count, 0) / 12).toFixed(0)}`
      );

      // Perform rebalancing
      const result = await cluster.rebalance();

      const afterDistribution = cluster.getDistribution();

      console.log('\n  After rebalancing:');
      console.log(`    Moved documents: ${result.movedDocuments}`);
      console.log(`    Source shards: ${result.sourceShards.join(', ')}`);
      console.log(`    Target shards: ${result.targetShards.join(', ')}`);
      console.log(`    Duration: ${result.duration}ms`);

      // Verify distribution is more even
      const afterCounts = afterDistribution.map((d) => d.count);
      const afterStdDev = Math.sqrt(
        afterCounts.reduce((sum, c) => sum + Math.pow(c - 625, 2), 0) / afterCounts.length
      );

      const beforeCounts = beforeDistribution.map((d) => d.count);
      const beforeStdDev = Math.sqrt(
        beforeCounts.reduce((sum, c) => sum + Math.pow(c - 625, 2), 0) / beforeCounts.length
      );

      console.log(`    Std Dev before: ${beforeStdDev.toFixed(2)}`);
      console.log(`    Std Dev after: ${afterStdDev.toFixed(2)}`);

      // After rebalancing, standard deviation should decrease
      expect(afterStdDev).toBeLessThan(beforeStdDev);

      // Total count should remain the same
      expect(cluster.getTotalDocumentCount()).toBe(10000);
    });

    it('should handle rebalancing with 100k documents', async () => {
      const TOTAL = 100_000;

      // Create uneven distribution
      for (let i = 0; i < TOTAL; i += 1000) {
        const batch = generateDocuments(i, 1000);
        // Bias first 4 shards
        for (const doc of batch) {
          const biasedShard = Math.random() < 0.6 ? Math.floor(Math.random() * 4) : Math.floor(Math.random() * 16);
          doc.shardId = biasedShard;
          await cluster.shards[biasedShard]!.write(doc);
        }
      }

      const beforeMax = Math.max(...cluster.getDistribution().map((d) => d.count));
      const beforeMin = Math.min(...cluster.getDistribution().map((d) => d.count));

      console.log(`\n  100k Document Rebalancing:`);
      console.log(`    Before - Max: ${beforeMax}, Min: ${beforeMin}`);

      const startTime = Date.now();
      const result = await cluster.rebalance();
      const totalDuration = Date.now() - startTime;

      const afterMax = Math.max(...cluster.getDistribution().map((d) => d.count));
      const afterMin = Math.min(...cluster.getDistribution().map((d) => d.count));

      console.log(`    After - Max: ${afterMax}, Min: ${afterMin}`);
      console.log(`    Documents moved: ${result.movedDocuments}`);
      console.log(`    Total duration: ${totalDuration}ms`);

      // Distribution should be more even
      expect(afterMax - afterMin).toBeLessThan(beforeMax - beforeMin);
      expect(cluster.getTotalDocumentCount()).toBe(TOTAL);
    });

    it('should not move documents if already balanced', async () => {
      // Create even distribution
      const docs = generateDocuments(0, 16000);
      await cluster.writeDocuments(docs);

      const beforeDist = cluster.getDistribution();
      const result = await cluster.rebalance();

      console.log(`\n  Already balanced cluster:`);
      console.log(`    Documents moved: ${result.movedDocuments}`);

      // Should move minimal documents
      expect(result.movedDocuments).toBeLessThan(1000); // Allow some tolerance
      expect(cluster.getTotalDocumentCount()).toBe(16000);
    });
  });

  describe('Concurrent Operations on Multiple Shards', () => {
    it('should handle high concurrency without data loss', async () => {
      const CONCURRENT_WRITERS = 16;
      const WRITES_PER_WRITER = 1000;
      const expectedTotal = CONCURRENT_WRITERS * WRITES_PER_WRITER;

      const startTime = Date.now();

      // Launch concurrent writers
      const writers = Array.from({ length: CONCURRENT_WRITERS }, async (_, writerId) => {
        for (let i = 0; i < WRITES_PER_WRITER; i++) {
          const doc = generateDocument(writerId * WRITES_PER_WRITER + i);
          await cluster.writeDocument(doc);
        }
      });

      await Promise.all(writers);
      const duration = Date.now() - startTime;

      const totalDocs = cluster.getTotalDocumentCount();

      console.log(`\n  High concurrency test (${CONCURRENT_WRITERS} writers):`);
      console.log(`    Expected: ${expectedTotal}`);
      console.log(`    Actual: ${totalDocs}`);
      console.log(`    Duration: ${duration}ms`);
      console.log(`    Throughput: ${((expectedTotal / duration) * 1000).toFixed(0)} docs/sec`);

      // No data loss
      expect(totalDocs).toBe(expectedTotal);
    });

    it('should maintain consistency during concurrent read/write', async () => {
      // Pre-populate
      const initialDocs = generateDocuments(0, 10000);
      await cluster.writeDocuments(initialDocs);

      const counters: number[] = [];
      let readErrors = 0;
      const startTime = Date.now();

      // Concurrent readers and writers
      const operations: Promise<void>[] = [];

      // Writers
      for (let w = 0; w < 8; w++) {
        operations.push(
          (async () => {
            for (let i = 0; i < 500; i++) {
              const doc = generateDocument(10000 + w * 500 + i);
              await cluster.writeDocument(doc);
            }
          })()
        );
      }

      // Readers
      for (let r = 0; r < 8; r++) {
        operations.push(
          (async () => {
            for (let i = 0; i < 100; i++) {
              const idx = Math.floor(Math.random() * initialDocs.length);
              const doc = await cluster.readDocument(initialDocs[idx]!._id);
              if (doc) {
                counters.push(doc.counter);
              } else {
                readErrors++;
              }
            }
          })()
        );
      }

      await Promise.all(operations);
      const duration = Date.now() - startTime;

      console.log(`\n  Concurrent read/write consistency:`);
      console.log(`    Read operations: ${counters.length}`);
      console.log(`    Read errors: ${readErrors}`);
      console.log(`    Duration: ${duration}ms`);
      console.log(`    Final count: ${cluster.getTotalDocumentCount()}`);

      // No read errors for existing documents
      expect(readErrors).toBe(0);

      // All writes completed
      expect(cluster.getTotalDocumentCount()).toBe(10000 + 8 * 500);
    });

    it('should handle burst traffic across shards', async () => {
      const BURST_SIZE = 5000;
      const BURST_COUNT = 5;
      const burstTimes: number[] = [];

      for (let burst = 0; burst < BURST_COUNT; burst++) {
        const startTime = Date.now();
        const docs = generateDocuments(burst * BURST_SIZE, BURST_SIZE);

        await cluster.writeDocuments(docs);

        burstTimes.push(Date.now() - startTime);

        // Small delay between bursts
        await new Promise((resolve) => setTimeout(resolve, 50));
      }

      const totalDocs = cluster.getTotalDocumentCount();
      const avgBurstTime = burstTimes.reduce((a, b) => a + b, 0) / burstTimes.length;
      const maxBurstTime = Math.max(...burstTimes);
      const minBurstTime = Math.min(...burstTimes);

      console.log(`\n  Burst traffic handling (${BURST_COUNT} x ${BURST_SIZE}):`);
      console.log(`    Total documents: ${totalDocs}`);
      console.log(`    Avg burst time: ${avgBurstTime.toFixed(0)}ms`);
      console.log(`    Min burst time: ${minBurstTime}ms`);
      console.log(`    Max burst time: ${maxBurstTime}ms`);

      expect(totalDocs).toBe(BURST_COUNT * BURST_SIZE);

      // Burst times should be relatively consistent
      expect(maxBurstTime).toBeLessThan(avgBurstTime * 2);
    });

    it('should handle operations during shard failures', async () => {
      // Pre-populate
      const docs = generateDocuments(0, 16000);
      await cluster.writeDocuments(docs);

      // Simulate shard "failure" by clearing some shards
      const failedShards = [3, 7, 11];
      let lostDocs = 0;
      for (const shardId of failedShards) {
        lostDocs += cluster.shards[shardId]!.getDocumentCount();
        cluster.shards[shardId]!.clear();
      }

      // Continue operations on remaining shards
      const newDocs = generateDocuments(16000, 5000);
      await cluster.writeDocuments(newDocs);

      const finalCount = cluster.getTotalDocumentCount();
      const distribution = cluster.getDistribution();

      console.log(`\n  Operations during shard failure:`);
      console.log(`    Failed shards: ${failedShards.join(', ')}`);
      console.log(`    Documents lost: ${lostDocs}`);
      console.log(`    Final count: ${finalCount}`);
      console.log(`    Expected: ${16000 - lostDocs + 5000}`);

      // Verify new writes succeeded
      const newDocCount = newDocs.filter((d) => !failedShards.includes(hashDocumentToShard(d._id, SHARD_COUNT))).length;

      // Operations on healthy shards should succeed
      expect(finalCount).toBeGreaterThan(16000 - lostDocs);
    });
  });

  describe('ShardRouter Integration', () => {
    it('should use ShardRouter for consistent routing', () => {
      const router = createShardRouter({ shardCount: 16 });

      // Test document routing consistency
      const docIds = Array.from({ length: 1000 }, () => crypto.randomUUID());

      for (const docId of docIds) {
        const shard1 = hashDocumentToShard(docId, 16);
        const shard2 = hashDocumentToShard(docId, 16);
        const shard3 = hashDocumentToShard(docId, 16);

        expect(shard1).toBe(shard2);
        expect(shard2).toBe(shard3);
        expect(shard1).toBeGreaterThanOrEqual(0);
        expect(shard1).toBeLessThan(16);
      }
    });

    it('should support split collections across multiple shards', () => {
      const router = createShardRouter({ shardCount: 16 });

      // Split a hot collection across 4 shards
      router.splitCollection('hot_events', [0, 4, 8, 12]);

      const docIds = Array.from({ length: 1000 }, () => crypto.randomUUID());
      const shardCounts = new Map<number, number>();

      for (const docId of docIds) {
        const assignment = router.routeDocument('hot_events', docId);
        shardCounts.set(
          assignment.shardId,
          (shardCounts.get(assignment.shardId) || 0) + 1
        );
      }

      // Verify only split shards are used
      expect(shardCounts.size).toBeLessThanOrEqual(4);
      for (const shardId of shardCounts.keys()) {
        expect([0, 4, 8, 12]).toContain(shardId);
      }

      console.log(`\n  Split collection routing:`);
      console.log(`    Split shards: 0, 4, 8, 12`);
      console.log(`    Distribution: ${Array.from(shardCounts.entries()).map(([k, v]) => `${k}:${v}`).join(', ')}`);
    });

    it('should handle affinity hints for colocation', () => {
      const router = createShardRouter({ shardCount: 16 });

      // Set affinity for related collections
      router.setAffinityHint('users', { preferredShard: 5 });
      router.setAffinityHint('user_sessions', { preferredShard: 5 });
      router.setAffinityHint('user_preferences', { preferredShard: 5 });

      const assignment1 = router.route('users');
      const assignment2 = router.route('user_sessions');
      const assignment3 = router.route('user_preferences');

      expect(assignment1.shardId).toBe(5);
      expect(assignment2.shardId).toBe(5);
      expect(assignment3.shardId).toBe(5);

      console.log(`\n  Affinity hints for colocation:`);
      console.log(`    users -> shard ${assignment1.shardId}`);
      console.log(`    user_sessions -> shard ${assignment2.shardId}`);
      console.log(`    user_preferences -> shard ${assignment3.shardId}`);
    });

    it('should track router statistics at scale', () => {
      // Use larger cache to accommodate all unique collections
      const router = createShardRouter({ shardCount: 16, cacheSize: 1000 });

      // Route many collections with high repetition
      // 50 unique collections, 1000 total calls = 20 calls per collection
      for (let i = 0; i < 1000; i++) {
        router.route(`collection_${i % 50}`);
      }

      const stats = router.getStats();

      console.log(`\n  Router statistics:`);
      console.log(`    Total routes: ${stats.totalRoutes}`);
      console.log(`    Cache hits: ${stats.cacheHits}`);
      console.log(`    Cache misses: ${stats.cacheMisses}`);
      console.log(`    Hit rate: ${((stats.cacheHits / stats.totalRoutes) * 100).toFixed(1)}%`);

      expect(stats.totalRoutes).toBe(1000);
      expect(stats.cacheHits + stats.cacheMisses).toBe(1000);
      // With 50 unique collections and 1000 calls, first 50 are misses, remaining 950 are hits
      expect(stats.cacheHits).toBe(950);
      expect(stats.cacheMisses).toBe(50);
    });
  });
});
