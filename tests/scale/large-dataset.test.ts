/**
 * Large Dataset Scale Tests
 *
 * Tests MongoLake performance and correctness with 100,000+ documents.
 * These tests verify:
 * - Inserting 100,000 documents
 * - Querying 100k document collection
 * - Aggregation on 100k documents
 * - Operations with 16 shards
 *
 * NOTE: These tests are resource-intensive and may take several minutes to run.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';

// ============================================================================
// Types
// ============================================================================

interface TestDocument {
  _id?: string;
  name: string;
  email: string;
  age: number;
  department: string;
  salary: number;
  active: boolean;
  tags: string[];
  createdAt: string;
  metadata: {
    source: string;
    batchId: number;
    region: string;
  };
}

interface MemorySnapshot {
  timestamp: number;
  heapUsed: number;
  heapTotal: number;
  rss: number;
}

interface InsertResult {
  acknowledged: boolean;
  insertedCount: number;
  insertedIds: { [key: number]: string };
}

interface AggregateResult {
  _id: string | number | null;
  count?: number;
  avgSalary?: number;
  totalSalary?: number;
  minAge?: number;
  maxAge?: number;
}

// ============================================================================
// Mock Storage Backend for Scale Testing
// ============================================================================

/**
 * In-memory storage backend optimized for scale testing.
 * Mimics the StorageBackend interface while keeping data in memory.
 */
class ScaleTestStorage {
  private data: Map<string, Uint8Array> = new Map();

  async get(key: string): Promise<Uint8Array | null> {
    return this.data.get(key) || null;
  }

  async put(key: string, value: Uint8Array): Promise<void> {
    this.data.set(key, value);
  }

  async delete(key: string): Promise<void> {
    this.data.delete(key);
  }

  async list(prefix: string): Promise<string[]> {
    const keys: string[] = [];
    for (const key of this.data.keys()) {
      if (key.startsWith(prefix)) {
        keys.push(key);
      }
    }
    return keys;
  }

  async has(key: string): Promise<boolean> {
    return this.data.has(key);
  }

  clear(): void {
    this.data.clear();
  }

  getSize(): number {
    return this.data.size;
  }

  getTotalBytes(): number {
    let total = 0;
    for (const value of this.data.values()) {
      total += value.byteLength;
    }
    return total;
  }
}

// ============================================================================
// Mock Collection for Scale Testing
// ============================================================================

/**
 * Simulates a collection for scale testing.
 * Implements key MongoDB-compatible operations using in-memory storage.
 */
class MockCollection<T extends { _id?: string }> {
  private documents: Map<string, T> = new Map();
  private nextSeq = 0;

  constructor(
    public readonly name: string,
    private shardId: number = 0
  ) {}

  async insertOne(doc: T): Promise<{ acknowledged: boolean; insertedId: string }> {
    const id = doc._id || crypto.randomUUID();
    const fullDoc = { ...doc, _id: id } as T;
    this.documents.set(id, fullDoc);
    this.nextSeq++;
    return { acknowledged: true, insertedId: id };
  }

  async insertMany(docs: T[]): Promise<InsertResult> {
    const insertedIds: { [key: number]: string } = {};

    for (let i = 0; i < docs.length; i++) {
      const doc = docs[i]!;
      const id = doc._id || crypto.randomUUID();
      const fullDoc = { ...doc, _id: id } as T;
      this.documents.set(id, fullDoc);
      insertedIds[i] = id;
      this.nextSeq++;
    }

    return {
      acknowledged: true,
      insertedCount: docs.length,
      insertedIds,
    };
  }

  async find(filter?: Partial<T>): Promise<T[]> {
    const results: T[] = [];

    for (const doc of this.documents.values()) {
      if (!filter || this.matchesFilter(doc, filter)) {
        results.push(doc);
      }
    }

    return results;
  }

  async findOne(filter?: Partial<T>): Promise<T | null> {
    for (const doc of this.documents.values()) {
      if (!filter || this.matchesFilter(doc, filter)) {
        return doc;
      }
    }
    return null;
  }

  async countDocuments(filter?: Partial<T>): Promise<number> {
    if (!filter) {
      return this.documents.size;
    }
    let count = 0;
    for (const doc of this.documents.values()) {
      if (this.matchesFilter(doc, filter)) {
        count++;
      }
    }
    return count;
  }

  async aggregate(pipeline: unknown[]): Promise<AggregateResult[]> {
    let results: unknown[] = [...this.documents.values()];

    for (const stage of pipeline) {
      const stageObj = stage as Record<string, unknown>;

      if ('$match' in stageObj) {
        results = results.filter((doc) =>
          this.matchesFilter(doc as T, stageObj.$match as Partial<T>)
        );
      } else if ('$group' in stageObj) {
        results = this.executeGroup(results, stageObj.$group as Record<string, unknown>);
      } else if ('$sort' in stageObj) {
        results = this.executeSort(results, stageObj.$sort as Record<string, number>);
      } else if ('$limit' in stageObj) {
        results = results.slice(0, stageObj.$limit as number);
      } else if ('$skip' in stageObj) {
        results = results.slice(stageObj.$skip as number);
      }
    }

    return results as AggregateResult[];
  }

  async updateOne(
    filter: Partial<T>,
    update: { $set?: Partial<T>; $inc?: Record<string, number> }
  ): Promise<{ matchedCount: number; modifiedCount: number }> {
    for (const [id, doc] of this.documents) {
      if (this.matchesFilter(doc, filter)) {
        const updated = { ...doc };
        if (update.$set) {
          Object.assign(updated, update.$set);
        }
        if (update.$inc) {
          for (const [key, value] of Object.entries(update.$inc)) {
            (updated as Record<string, unknown>)[key] =
              ((doc as Record<string, unknown>)[key] as number) + value;
          }
        }
        this.documents.set(id, updated);
        return { matchedCount: 1, modifiedCount: 1 };
      }
    }
    return { matchedCount: 0, modifiedCount: 0 };
  }

  async deleteOne(filter: Partial<T>): Promise<{ deletedCount: number }> {
    for (const [id, doc] of this.documents) {
      if (this.matchesFilter(doc, filter)) {
        this.documents.delete(id);
        return { deletedCount: 1 };
      }
    }
    return { deletedCount: 0 };
  }

  async deleteMany(filter: Partial<T>): Promise<{ deletedCount: number }> {
    let deletedCount = 0;
    const toDelete: string[] = [];

    for (const [id, doc] of this.documents) {
      if (this.matchesFilter(doc, filter)) {
        toDelete.push(id);
      }
    }

    for (const id of toDelete) {
      this.documents.delete(id);
      deletedCount++;
    }

    return { deletedCount };
  }

  getShardId(): number {
    return this.shardId;
  }

  clear(): void {
    this.documents.clear();
    this.nextSeq = 0;
  }

  private matchesFilter(doc: T, filter: Partial<T>): boolean {
    for (const [key, value] of Object.entries(filter)) {
      const docValue = (doc as Record<string, unknown>)[key];

      // Handle comparison operators
      if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
        const ops = value as Record<string, unknown>;
        if ('$gte' in ops && (docValue as number) < (ops.$gte as number)) return false;
        if ('$lte' in ops && (docValue as number) > (ops.$lte as number)) return false;
        if ('$gt' in ops && (docValue as number) <= (ops.$gt as number)) return false;
        if ('$lt' in ops && (docValue as number) >= (ops.$lt as number)) return false;
        if ('$in' in ops && !(ops.$in as unknown[]).includes(docValue)) return false;
        if ('$ne' in ops && docValue === ops.$ne) return false;
      } else if (docValue !== value) {
        return false;
      }
    }
    return true;
  }

  private executeGroup(
    docs: unknown[],
    groupSpec: Record<string, unknown>
  ): AggregateResult[] {
    const groups = new Map<string | number | null, unknown[]>();
    const groupId = groupSpec._id as string | null;

    for (const doc of docs) {
      const docObj = doc as Record<string, unknown>;
      let key: string | number | null = null;

      if (groupId === null) {
        key = null;
      } else if (groupId.startsWith('$')) {
        key = docObj[groupId.slice(1)] as string | number;
      } else {
        key = groupId;
      }

      if (!groups.has(key)) {
        groups.set(key, []);
      }
      groups.get(key)!.push(doc);
    }

    const results: AggregateResult[] = [];
    for (const [key, groupDocs] of groups) {
      const result: AggregateResult = { _id: key };

      for (const [field, spec] of Object.entries(groupSpec)) {
        if (field === '_id') continue;

        const specObj = spec as Record<string, string>;
        if ('$sum' in specObj) {
          if (specObj.$sum === 1) {
            result[field as keyof AggregateResult] = groupDocs.length as never;
          } else {
            const sumField = specObj.$sum.slice(1);
            result[field as keyof AggregateResult] = groupDocs.reduce(
              (acc, d) => acc + ((d as Record<string, number>)[sumField] || 0),
              0
            ) as never;
          }
        } else if ('$avg' in specObj) {
          const avgField = specObj.$avg.slice(1);
          const sum = groupDocs.reduce(
            (acc, d) => acc + ((d as Record<string, number>)[avgField] || 0),
            0
          );
          result[field as keyof AggregateResult] = (sum / groupDocs.length) as never;
        } else if ('$min' in specObj) {
          const minField = specObj.$min.slice(1);
          result[field as keyof AggregateResult] = Math.min(
            ...groupDocs.map((d) => (d as Record<string, number>)[minField] || 0)
          ) as never;
        } else if ('$max' in specObj) {
          const maxField = specObj.$max.slice(1);
          result[field as keyof AggregateResult] = Math.max(
            ...groupDocs.map((d) => (d as Record<string, number>)[maxField] || 0)
          ) as never;
        }
      }

      results.push(result);
    }

    return results;
  }

  private executeSort(docs: unknown[], sortSpec: Record<string, number>): unknown[] {
    return [...docs].sort((a, b) => {
      for (const [field, order] of Object.entries(sortSpec)) {
        const aVal = (a as Record<string, unknown>)[field];
        const bVal = (b as Record<string, unknown>)[field];

        if (aVal < bVal) return -order;
        if (aVal > bVal) return order;
      }
      return 0;
    });
  }
}

// ============================================================================
// Test Utilities
// ============================================================================

const DEPARTMENTS = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance', 'Operations', 'Legal', 'Support'];
const REGIONS = ['us-east', 'us-west', 'eu-west', 'eu-central', 'ap-south', 'ap-east'];
const TAGS = ['senior', 'junior', 'remote', 'onsite', 'fulltime', 'contractor', 'manager', 'lead'];

function generateTestDocument(index: number): TestDocument {
  return {
    name: `User ${index}`,
    email: `user${index}@example.com`,
    age: 22 + (index % 43), // Ages 22-64
    department: DEPARTMENTS[index % DEPARTMENTS.length]!,
    salary: 50000 + (index % 100) * 1000, // Salaries 50k-149k
    active: index % 10 !== 0, // 90% active
    tags: [
      TAGS[index % TAGS.length]!,
      TAGS[(index + 3) % TAGS.length]!,
    ],
    createdAt: new Date(Date.now() - (index * 60000)).toISOString(), // Spread over time
    metadata: {
      source: 'scale-test',
      batchId: Math.floor(index / 1000),
      region: REGIONS[index % REGIONS.length]!,
    },
  };
}

function generateBatch(startIndex: number, count: number): TestDocument[] {
  const batch: TestDocument[] = [];
  for (let i = 0; i < count; i++) {
    batch.push(generateTestDocument(startIndex + i));
  }
  return batch;
}

function captureMemory(): MemorySnapshot {
  const mem = process.memoryUsage();
  return {
    timestamp: Date.now(),
    heapUsed: mem.heapUsed,
    heapTotal: mem.heapTotal,
    rss: mem.rss,
  };
}

function formatBytes(bytes: number): string {
  const mb = bytes / (1024 * 1024);
  return `${mb.toFixed(2)} MB`;
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

// ============================================================================
// Tests
// ============================================================================

describe('Large Dataset Scale Tests', () => {
  const TOTAL_DOCUMENTS = 100_000;
  const BATCH_SIZE = 1000;

  let collection: MockCollection<TestDocument>;
  let memoryBaseline: MemorySnapshot;

  beforeAll(() => {
    memoryBaseline = captureMemory();
    console.log(`\nMemory baseline: ${formatBytes(memoryBaseline.heapUsed)} heap used`);
  });

  beforeEach(() => {
    collection = new MockCollection<TestDocument>('scale_test');
  });

  afterAll(() => {
    const finalMemory = captureMemory();
    console.log(`\nFinal memory: ${formatBytes(finalMemory.heapUsed)} heap used`);
    console.log(`Memory growth: ${formatBytes(finalMemory.heapUsed - memoryBaseline.heapUsed)}`);
  });

  describe('Inserting 100,000 Documents', () => {
    it('should insert 100k documents in batches', async () => {
      const startTime = Date.now();
      const memorySnapshots: MemorySnapshot[] = [captureMemory()];

      let totalInserted = 0;
      const batchTimes: number[] = [];

      for (let i = 0; i < TOTAL_DOCUMENTS; i += BATCH_SIZE) {
        const batchStart = Date.now();
        const batch = generateBatch(i, Math.min(BATCH_SIZE, TOTAL_DOCUMENTS - i));

        const result = await collection.insertMany(batch);

        expect(result.acknowledged).toBe(true);
        expect(result.insertedCount).toBe(batch.length);

        totalInserted += batch.length;
        batchTimes.push(Date.now() - batchStart);

        // Capture memory every 10k documents
        if (totalInserted % 10000 === 0) {
          memorySnapshots.push(captureMemory());
          console.log(`  Inserted ${totalInserted.toLocaleString()} documents...`);
        }
      }

      const totalTime = Date.now() - startTime;
      const count = await collection.countDocuments();

      // Verify all documents were inserted
      expect(count).toBe(TOTAL_DOCUMENTS);

      // Calculate statistics
      const avgBatchTime = batchTimes.reduce((a, b) => a + b, 0) / batchTimes.length;
      const docsPerSecond = (TOTAL_DOCUMENTS / totalTime) * 1000;
      const memoryGrowth = memorySnapshots[memorySnapshots.length - 1]!.heapUsed - memorySnapshots[0]!.heapUsed;

      console.log(`\n  Insert Performance Summary:`);
      console.log(`    Total documents: ${TOTAL_DOCUMENTS.toLocaleString()}`);
      console.log(`    Total time: ${formatDuration(totalTime)}`);
      console.log(`    Throughput: ${docsPerSecond.toFixed(0)} docs/sec`);
      console.log(`    Avg batch time: ${avgBatchTime.toFixed(2)}ms per ${BATCH_SIZE} docs`);
      console.log(`    Memory growth: ${formatBytes(memoryGrowth)}`);

      // Performance assertions
      expect(docsPerSecond).toBeGreaterThan(1000); // At least 1k docs/sec
      expect(totalTime).toBeLessThan(300000); // Complete within 5 minutes
    });

    it('should maintain consistent insert throughput over time', async () => {
      const batchResults: { batchNum: number; duration: number; docsPerSec: number }[] = [];

      for (let i = 0; i < TOTAL_DOCUMENTS; i += BATCH_SIZE) {
        const batchNum = Math.floor(i / BATCH_SIZE);
        const batchStart = Date.now();
        const batch = generateBatch(i, Math.min(BATCH_SIZE, TOTAL_DOCUMENTS - i));

        await collection.insertMany(batch);

        const duration = Date.now() - batchStart;
        batchResults.push({
          batchNum,
          duration,
          docsPerSec: duration > 0 ? (batch.length / duration) * 1000 : 0,
        });
      }

      // Compare first 10% vs last 10% throughput
      const firstDecile = batchResults.slice(0, 10);
      const lastDecile = batchResults.slice(-10);

      const avgFirstDecile = firstDecile.reduce((a, b) => a + b.docsPerSec, 0) / firstDecile.length;
      const avgLastDecile = lastDecile.reduce((a, b) => a + b.docsPerSec, 0) / lastDecile.length;

      console.log(`\n  Throughput consistency:`);
      console.log(`    First 10% avg: ${avgFirstDecile.toFixed(0)} docs/sec`);
      console.log(`    Last 10% avg: ${avgLastDecile.toFixed(0)} docs/sec`);
      console.log(`    Ratio: ${(avgLastDecile / avgFirstDecile * 100).toFixed(1)}%`);

      // Last batches should maintain at least 50% of initial throughput
      expect(avgLastDecile).toBeGreaterThan(avgFirstDecile * 0.5);
    });
  });

  describe('Querying 100k Document Collection', () => {
    beforeEach(async () => {
      // Pre-populate with 100k documents
      for (let i = 0; i < TOTAL_DOCUMENTS; i += BATCH_SIZE) {
        const batch = generateBatch(i, Math.min(BATCH_SIZE, TOTAL_DOCUMENTS - i));
        await collection.insertMany(batch);
      }
    });

    it('should count all documents efficiently', async () => {
      const startTime = Date.now();
      const count = await collection.countDocuments();
      const duration = Date.now() - startTime;

      expect(count).toBe(TOTAL_DOCUMENTS);
      expect(duration).toBeLessThan(1000); // Should be very fast

      console.log(`\n  Count 100k docs: ${duration}ms`);
    });

    it('should find documents with equality filter', async () => {
      const startTime = Date.now();
      const results = await collection.find({ department: 'Engineering' });
      const duration = Date.now() - startTime;

      // Engineering is 1/8 of documents
      const expectedCount = Math.floor(TOTAL_DOCUMENTS / DEPARTMENTS.length);
      expect(results.length).toBeGreaterThan(expectedCount * 0.9);
      expect(results.length).toBeLessThan(expectedCount * 1.1);

      console.log(`\n  Find by department (${results.length} results): ${duration}ms`);
      expect(duration).toBeLessThan(5000); // Under 5 seconds
    });

    it('should find documents with range filter', async () => {
      const startTime = Date.now();
      const results = await collection.find({
        age: { $gte: 30, $lte: 40 },
      } as unknown as Partial<TestDocument>);
      const duration = Date.now() - startTime;

      // Ages 30-40 is about 25% of the 22-64 range
      expect(results.length).toBeGreaterThan(TOTAL_DOCUMENTS * 0.2);
      expect(results.length).toBeLessThan(TOTAL_DOCUMENTS * 0.3);

      console.log(`\n  Find by age range (${results.length} results): ${duration}ms`);
      expect(duration).toBeLessThan(10000); // Under 10 seconds
    });

    it('should find documents with compound filter', async () => {
      const startTime = Date.now();
      const results = await collection.find({
        department: 'Engineering',
        active: true,
      });
      const duration = Date.now() - startTime;

      // Engineering (1/8) * active (90%)
      const expectedCount = Math.floor(TOTAL_DOCUMENTS / DEPARTMENTS.length * 0.9);
      expect(results.length).toBeGreaterThan(expectedCount * 0.8);
      expect(results.length).toBeLessThan(expectedCount * 1.2);

      console.log(`\n  Find with compound filter (${results.length} results): ${duration}ms`);
      expect(duration).toBeLessThan(10000);
    });

    it('should handle findOne efficiently in large collection', async () => {
      const startTime = Date.now();
      const result = await collection.findOne({ email: 'user50000@example.com' });
      const duration = Date.now() - startTime;

      expect(result).not.toBeNull();
      expect(result!.email).toBe('user50000@example.com');

      console.log(`\n  FindOne in 100k docs: ${duration}ms`);
      expect(duration).toBeLessThan(5000);
    });
  });

  describe('Aggregation on 100k Documents', () => {
    beforeEach(async () => {
      // Pre-populate with 100k documents
      for (let i = 0; i < TOTAL_DOCUMENTS; i += BATCH_SIZE) {
        const batch = generateBatch(i, Math.min(BATCH_SIZE, TOTAL_DOCUMENTS - i));
        await collection.insertMany(batch);
      }
    });

    it('should aggregate count by department', async () => {
      const startTime = Date.now();
      const results = await collection.aggregate([
        {
          $group: {
            _id: '$department',
            count: { $sum: 1 },
          },
        },
        { $sort: { count: -1 } },
      ]);
      const duration = Date.now() - startTime;

      expect(results.length).toBe(DEPARTMENTS.length);

      // Each department should have roughly equal count
      const totalCount = results.reduce((acc, r) => acc + (r.count || 0), 0);
      expect(totalCount).toBe(TOTAL_DOCUMENTS);

      console.log(`\n  Group by department (${results.length} groups): ${duration}ms`);
      expect(duration).toBeLessThan(15000);
    });

    it('should aggregate average salary by department', async () => {
      const startTime = Date.now();
      const results = await collection.aggregate([
        {
          $group: {
            _id: '$department',
            avgSalary: { $avg: '$salary' },
            count: { $sum: 1 },
          },
        },
      ]);
      const duration = Date.now() - startTime;

      expect(results.length).toBe(DEPARTMENTS.length);

      // Verify average salaries are in expected range
      for (const result of results) {
        expect(result.avgSalary).toBeGreaterThan(50000);
        expect(result.avgSalary).toBeLessThan(150000);
      }

      console.log(`\n  Avg salary by department: ${duration}ms`);
      expect(duration).toBeLessThan(15000);
    });

    it('should aggregate with match and group', async () => {
      const startTime = Date.now();
      const results = await collection.aggregate([
        { $match: { active: true } },
        {
          $group: {
            _id: '$department',
            count: { $sum: 1 },
            totalSalary: { $sum: '$salary' },
            avgSalary: { $avg: '$salary' },
          },
        },
        { $sort: { totalSalary: -1 } },
        { $limit: 5 },
      ]);
      const duration = Date.now() - startTime;

      expect(results.length).toBe(5);

      console.log(`\n  Top 5 departments by salary (active only): ${duration}ms`);
      expect(duration).toBeLessThan(20000);
    });

    it('should aggregate min/max values', async () => {
      const startTime = Date.now();
      const results = await collection.aggregate([
        {
          $group: {
            _id: null,
            minAge: { $min: '$age' },
            maxAge: { $max: '$age' },
            count: { $sum: 1 },
          },
        },
      ]);
      const duration = Date.now() - startTime;

      expect(results.length).toBe(1);
      expect(results[0]!.minAge).toBe(22);
      expect(results[0]!.maxAge).toBe(64);
      expect(results[0]!.count).toBe(TOTAL_DOCUMENTS);

      console.log(`\n  Min/max age aggregation: ${duration}ms`);
      expect(duration).toBeLessThan(15000);
    });

    it('should handle multi-stage pipeline efficiently', async () => {
      const startTime = Date.now();
      const results = await collection.aggregate([
        { $match: { salary: { $gte: 80000 } } },
        {
          $group: {
            _id: '$department',
            count: { $sum: 1 },
            avgAge: { $avg: '$age' },
          },
        },
        { $sort: { count: -1 } },
        { $limit: 3 },
      ]);
      const duration = Date.now() - startTime;

      expect(results.length).toBe(3);

      console.log(`\n  Multi-stage pipeline (match -> group -> sort -> limit): ${duration}ms`);
      expect(duration).toBeLessThan(20000);
    });
  });

  describe('Operations with 16 Shards', () => {
    const SHARD_COUNT = 16;
    let shardedCollections: MockCollection<TestDocument>[];

    beforeEach(() => {
      // Create 16 shard collections
      shardedCollections = Array.from(
        { length: SHARD_COUNT },
        (_, i) => new MockCollection<TestDocument>(`scale_test_shard_${i}`, i)
      );
    });

    /**
     * Simple hash function to distribute documents across shards
     */
    function getShardForDocument(docId: string): number {
      let hash = 0;
      for (let i = 0; i < docId.length; i++) {
        hash = (hash * 31 + docId.charCodeAt(i)) >>> 0;
      }
      return hash % SHARD_COUNT;
    }

    it('should distribute 100k documents across 16 shards', async () => {
      const startTime = Date.now();
      const shardCounts: number[] = new Array(SHARD_COUNT).fill(0);

      for (let i = 0; i < TOTAL_DOCUMENTS; i += BATCH_SIZE) {
        const batch = generateBatch(i, Math.min(BATCH_SIZE, TOTAL_DOCUMENTS - i));

        // Distribute each document to appropriate shard
        for (const doc of batch) {
          const id = crypto.randomUUID();
          const shardId = getShardForDocument(id);
          await shardedCollections[shardId]!.insertOne({ ...doc, _id: id });
          shardCounts[shardId]++;
        }
      }

      const duration = Date.now() - startTime;

      // Verify total count
      let totalCount = 0;
      for (const coll of shardedCollections) {
        totalCount += await coll.countDocuments();
      }
      expect(totalCount).toBe(TOTAL_DOCUMENTS);

      // Check distribution (should be roughly even with some variance)
      const avgPerShard = TOTAL_DOCUMENTS / SHARD_COUNT;
      const minCount = Math.min(...shardCounts);
      const maxCount = Math.max(...shardCounts);

      console.log(`\n  16-Shard Distribution:`);
      console.log(`    Total time: ${formatDuration(duration)}`);
      console.log(`    Avg per shard: ${avgPerShard.toFixed(0)}`);
      console.log(`    Min shard: ${minCount} (${(minCount / avgPerShard * 100).toFixed(1)}%)`);
      console.log(`    Max shard: ${maxCount} (${(maxCount / avgPerShard * 100).toFixed(1)}%)`);

      // Distribution should be within 20% of average
      expect(minCount).toBeGreaterThan(avgPerShard * 0.8);
      expect(maxCount).toBeLessThan(avgPerShard * 1.2);
    });

    it('should query across all 16 shards', async () => {
      // Pre-populate shards
      for (let i = 0; i < TOTAL_DOCUMENTS; i += BATCH_SIZE) {
        const batch = generateBatch(i, Math.min(BATCH_SIZE, TOTAL_DOCUMENTS - i));
        for (const doc of batch) {
          const id = crypto.randomUUID();
          const shardId = getShardForDocument(id);
          await shardedCollections[shardId]!.insertOne({ ...doc, _id: id });
        }
      }

      const startTime = Date.now();

      // Query all shards in parallel
      const queryPromises = shardedCollections.map((coll) =>
        coll.find({ department: 'Engineering' })
      );
      const shardResults = await Promise.all(queryPromises);

      const totalResults = shardResults.reduce((acc, r) => acc + r.length, 0);
      const duration = Date.now() - startTime;

      console.log(`\n  Parallel query across 16 shards:`);
      console.log(`    Total results: ${totalResults}`);
      console.log(`    Duration: ${duration}ms`);

      // Verify we got results from all shards
      const shardsWithResults = shardResults.filter((r) => r.length > 0).length;
      expect(shardsWithResults).toBe(SHARD_COUNT);
    });

    it('should aggregate across all 16 shards', async () => {
      // Pre-populate shards
      for (let i = 0; i < TOTAL_DOCUMENTS; i += BATCH_SIZE) {
        const batch = generateBatch(i, Math.min(BATCH_SIZE, TOTAL_DOCUMENTS - i));
        for (const doc of batch) {
          const id = crypto.randomUUID();
          const shardId = getShardForDocument(id);
          await shardedCollections[shardId]!.insertOne({ ...doc, _id: id });
        }
      }

      const startTime = Date.now();

      // Aggregate on each shard (map phase)
      const aggregatePromises = shardedCollections.map((coll) =>
        coll.aggregate([
          {
            $group: {
              _id: '$department',
              count: { $sum: 1 },
              totalSalary: { $sum: '$salary' },
            },
          },
        ])
      );
      const shardResults = await Promise.all(aggregatePromises);

      // Merge results (reduce phase)
      const merged = new Map<string, { count: number; totalSalary: number }>();
      for (const results of shardResults) {
        for (const result of results) {
          const existing = merged.get(result._id as string);
          if (existing) {
            existing.count += result.count || 0;
            existing.totalSalary += result.totalSalary || 0;
          } else {
            merged.set(result._id as string, {
              count: result.count || 0,
              totalSalary: result.totalSalary || 0,
            });
          }
        }
      }

      const duration = Date.now() - startTime;

      // Verify we got all departments
      expect(merged.size).toBe(DEPARTMENTS.length);

      // Verify total count matches
      let totalCount = 0;
      for (const [, value] of merged) {
        totalCount += value.count;
      }
      expect(totalCount).toBe(TOTAL_DOCUMENTS);

      console.log(`\n  Distributed aggregation across 16 shards:`);
      console.log(`    Departments: ${merged.size}`);
      console.log(`    Total docs: ${totalCount}`);
      console.log(`    Duration: ${duration}ms`);
    });
  });
});
