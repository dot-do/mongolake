/**
 * Many Collections Scale Test
 *
 * Tests MongoLake performance with many collections and cross-collection operations.
 * These tests verify:
 * - Creating and managing 1000+ collections
 * - Cross-collection queries and aggregations
 * - Collection-level operations at scale
 * - Namespace management under load
 */

import { describe, it, expect, beforeEach, afterEach, beforeAll } from 'vitest';
import {
  createUserGenerator,
  createOrderGenerator,
  createEventGenerator,
  formatBytes,
  formatDuration,
  calculateThroughput,
} from '../utils/data-generator';

// ============================================================================
// Types
// ============================================================================

interface CollectionStats {
  name: string;
  documentCount: number;
  bytesUsed: number;
  indexCount: number;
}

interface DatabaseStats {
  collectionCount: number;
  totalDocuments: number;
  totalBytes: number;
  collections: CollectionStats[];
}

interface QueryResult {
  documents: unknown[];
  collectionName: string;
  durationMs: number;
}

// ============================================================================
// Mock Collection Manager
// ============================================================================

/**
 * Simulates a database with multiple collections for scale testing.
 */
class MockCollectionManager {
  private collections: Map<string, Map<string, unknown>> = new Map();
  private collectionMetadata: Map<string, { createdAt: number; indexNames: string[] }> = new Map();

  createCollection(name: string): void {
    if (this.collections.has(name)) {
      throw new Error(`Collection ${name} already exists`);
    }
    this.collections.set(name, new Map());
    this.collectionMetadata.set(name, {
      createdAt: Date.now(),
      indexNames: ['_id'],
    });
  }

  dropCollection(name: string): boolean {
    if (!this.collections.has(name)) {
      return false;
    }
    this.collections.delete(name);
    this.collectionMetadata.delete(name);
    return true;
  }

  listCollections(): string[] {
    return Array.from(this.collections.keys());
  }

  hasCollection(name: string): boolean {
    return this.collections.has(name);
  }

  async insertOne(collectionName: string, doc: Record<string, unknown>): Promise<{ insertedId: string }> {
    const collection = this.getOrCreateCollection(collectionName);
    const id = doc._id as string || crypto.randomUUID();
    collection.set(id, { ...doc, _id: id });
    return { insertedId: id };
  }

  async insertMany(collectionName: string, docs: Record<string, unknown>[]): Promise<{ insertedCount: number }> {
    const collection = this.getOrCreateCollection(collectionName);
    for (const doc of docs) {
      const id = doc._id as string || crypto.randomUUID();
      collection.set(id, { ...doc, _id: id });
    }
    return { insertedCount: docs.length };
  }

  async find(collectionName: string, filter?: Record<string, unknown>): Promise<unknown[]> {
    const collection = this.collections.get(collectionName);
    if (!collection) return [];

    const results: unknown[] = [];
    for (const doc of collection.values()) {
      if (!filter || this.matchesFilter(doc as Record<string, unknown>, filter)) {
        results.push(doc);
      }
    }
    return results;
  }

  async findOne(collectionName: string, filter?: Record<string, unknown>): Promise<unknown | null> {
    const results = await this.find(collectionName, filter);
    return results[0] || null;
  }

  async countDocuments(collectionName: string, filter?: Record<string, unknown>): Promise<number> {
    const results = await this.find(collectionName, filter);
    return results.length;
  }

  async aggregate(collectionName: string, pipeline: unknown[]): Promise<unknown[]> {
    let results = await this.find(collectionName);

    for (const stage of pipeline) {
      const stageObj = stage as Record<string, unknown>;

      if ('$match' in stageObj) {
        results = results.filter((doc) =>
          this.matchesFilter(doc as Record<string, unknown>, stageObj.$match as Record<string, unknown>)
        );
      } else if ('$limit' in stageObj) {
        results = results.slice(0, stageObj.$limit as number);
      } else if ('$skip' in stageObj) {
        results = results.slice(stageObj.$skip as number);
      }
    }

    return results;
  }

  getStats(): DatabaseStats {
    const collections: CollectionStats[] = [];
    let totalDocuments = 0;
    let totalBytes = 0;

    for (const [name, collection] of this.collections) {
      const metadata = this.collectionMetadata.get(name);
      let bytesUsed = 0;
      for (const doc of collection.values()) {
        bytesUsed += JSON.stringify(doc).length;
      }

      collections.push({
        name,
        documentCount: collection.size,
        bytesUsed,
        indexCount: metadata?.indexNames.length ?? 1,
      });

      totalDocuments += collection.size;
      totalBytes += bytesUsed;
    }

    return {
      collectionCount: this.collections.size,
      totalDocuments,
      totalBytes,
      collections,
    };
  }

  getCollectionStats(name: string): CollectionStats | null {
    const collection = this.collections.get(name);
    if (!collection) return null;

    const metadata = this.collectionMetadata.get(name);
    let bytesUsed = 0;
    for (const doc of collection.values()) {
      bytesUsed += JSON.stringify(doc).length;
    }

    return {
      name,
      documentCount: collection.size,
      bytesUsed,
      indexCount: metadata?.indexNames.length ?? 1,
    };
  }

  clear(): void {
    this.collections.clear();
    this.collectionMetadata.clear();
  }

  private getOrCreateCollection(name: string): Map<string, unknown> {
    if (!this.collections.has(name)) {
      this.createCollection(name);
    }
    return this.collections.get(name)!;
  }

  private matchesFilter(doc: Record<string, unknown>, filter: Record<string, unknown>): boolean {
    for (const [key, value] of Object.entries(filter)) {
      if (doc[key] !== value) {
        return false;
      }
    }
    return true;
  }
}

// ============================================================================
// Test Utilities
// ============================================================================

function generateCollectionName(prefix: string, index: number): string {
  return `${prefix}_${index.toString().padStart(4, '0')}`;
}

function captureMemory(): { heapUsed: number; heapTotal: number; rss: number } {
  const mem = process.memoryUsage();
  return {
    heapUsed: mem.heapUsed,
    heapTotal: mem.heapTotal,
    rss: mem.rss,
  };
}

// ============================================================================
// Tests
// ============================================================================

describe('Many Collections Scale Tests', () => {
  let manager: MockCollectionManager;

  beforeEach(() => {
    manager = new MockCollectionManager();
  });

  afterEach(() => {
    manager.clear();
  });

  describe('Creating Many Collections', () => {
    it('should create 1000 collections efficiently', async () => {
      const collectionCount = 1000;
      const creationTimes: number[] = [];

      const startTime = Date.now();

      for (let i = 0; i < collectionCount; i++) {
        const opStart = Date.now();
        const name = generateCollectionName('test_collection', i);
        manager.createCollection(name);
        creationTimes.push(Date.now() - opStart);
      }

      const durationMs = Date.now() - startTime;
      const collections = manager.listCollections();

      expect(collections.length).toBe(collectionCount);

      const avgCreationTime = creationTimes.reduce((a, b) => a + b, 0) / creationTimes.length;

      console.log('\n  Create 1000 Collections:');
      console.log(`    Duration: ${formatDuration(durationMs)}`);
      console.log(`    Avg creation time: ${avgCreationTime.toFixed(3)}ms`);
      console.log(`    Collections/sec: ${((collectionCount / durationMs) * 1000).toFixed(0)}`);

      // Should complete in reasonable time
      expect(durationMs).toBeLessThan(5000);
    });

    it('should handle collection creation with concurrent inserts', async () => {
      const collectionCount = 100;
      const docsPerCollection = 100;
      const userGenerator = createUserGenerator();

      const startTime = Date.now();

      const operations: Promise<void>[] = [];

      for (let i = 0; i < collectionCount; i++) {
        const name = generateCollectionName('concurrent', i);
        manager.createCollection(name);

        const docs = userGenerator.generateBatch(docsPerCollection);
        operations.push(
          manager.insertMany(name, docs as unknown as Record<string, unknown>[]).then(() => {})
        );
      }

      await Promise.all(operations);

      const durationMs = Date.now() - startTime;
      const stats = manager.getStats();

      console.log('\n  Concurrent Collection Creation with Inserts:');
      console.log(`    Collections: ${stats.collectionCount}`);
      console.log(`    Total documents: ${stats.totalDocuments}`);
      console.log(`    Total data: ${formatBytes(stats.totalBytes)}`);
      console.log(`    Duration: ${formatDuration(durationMs)}`);

      expect(stats.collectionCount).toBe(collectionCount);
      expect(stats.totalDocuments).toBe(collectionCount * docsPerCollection);
    });

    it('should list collections efficiently at scale', async () => {
      const collectionCount = 2000;

      // Create collections
      for (let i = 0; i < collectionCount; i++) {
        manager.createCollection(generateCollectionName('list_test', i));
      }

      // Measure listing performance
      const listTimes: number[] = [];
      const iterations = 100;

      for (let i = 0; i < iterations; i++) {
        const start = Date.now();
        const collections = manager.listCollections();
        listTimes.push(Date.now() - start);
        expect(collections.length).toBe(collectionCount);
      }

      const avgListTime = listTimes.reduce((a, b) => a + b, 0) / listTimes.length;

      console.log('\n  List 2000 Collections:');
      console.log(`    Avg list time: ${avgListTime.toFixed(3)}ms`);
      console.log(`    Iterations: ${iterations}`);

      // Listing should be fast
      expect(avgListTime).toBeLessThan(10);
    });
  });

  describe('Operations Across Many Collections', () => {
    const COLLECTION_COUNT = 500;
    const DOCS_PER_COLLECTION = 100;

    beforeEach(async () => {
      const userGenerator = createUserGenerator();

      // Pre-populate collections
      for (let i = 0; i < COLLECTION_COUNT; i++) {
        const name = generateCollectionName('ops_test', i);
        manager.createCollection(name);

        const docs = userGenerator.generateBatch(DOCS_PER_COLLECTION);
        await manager.insertMany(name, docs as unknown as Record<string, unknown>[]);
      }
    });

    it('should query across 500 collections', async () => {
      const queryTimes: number[] = [];
      const resultsPerCollection: number[] = [];

      const startTime = Date.now();

      for (let i = 0; i < COLLECTION_COUNT; i++) {
        const name = generateCollectionName('ops_test', i);
        const queryStart = Date.now();

        const results = await manager.find(name, { active: true });

        queryTimes.push(Date.now() - queryStart);
        resultsPerCollection.push(results.length);
      }

      const durationMs = Date.now() - startTime;
      const avgQueryTime = queryTimes.reduce((a, b) => a + b, 0) / queryTimes.length;
      const totalResults = resultsPerCollection.reduce((a, b) => a + b, 0);

      console.log('\n  Query Across 500 Collections:');
      console.log(`    Total duration: ${formatDuration(durationMs)}`);
      console.log(`    Avg query time: ${avgQueryTime.toFixed(3)}ms`);
      console.log(`    Total results: ${totalResults}`);
      console.log(`    Queries/sec: ${((COLLECTION_COUNT / durationMs) * 1000).toFixed(0)}`);

      expect(queryTimes.length).toBe(COLLECTION_COUNT);
    });

    it('should aggregate across all collections in parallel', async () => {
      const startTime = Date.now();

      const aggregatePromises = Array.from({ length: COLLECTION_COUNT }, async (_, i) => {
        const name = generateCollectionName('ops_test', i);
        return {
          collection: name,
          count: await manager.countDocuments(name),
        };
      });

      const results = await Promise.all(aggregatePromises);

      const durationMs = Date.now() - startTime;
      const totalCount = results.reduce((sum, r) => sum + r.count, 0);

      console.log('\n  Parallel Aggregate Across 500 Collections:');
      console.log(`    Duration: ${formatDuration(durationMs)}`);
      console.log(`    Total documents: ${totalCount}`);
      console.log(`    Collections: ${results.length}`);

      expect(totalCount).toBe(COLLECTION_COUNT * DOCS_PER_COLLECTION);
    });

    it('should handle random access pattern across collections', async () => {
      const accessCount = 5000;
      const accessTimes: number[] = [];
      const accessedCollections = new Set<string>();

      const startTime = Date.now();

      for (let i = 0; i < accessCount; i++) {
        const collectionIndex = Math.floor(Math.random() * COLLECTION_COUNT);
        const name = generateCollectionName('ops_test', collectionIndex);
        accessedCollections.add(name);

        const accessStart = Date.now();
        await manager.findOne(name);
        accessTimes.push(Date.now() - accessStart);
      }

      const durationMs = Date.now() - startTime;
      const avgAccessTime = accessTimes.reduce((a, b) => a + b, 0) / accessTimes.length;

      console.log('\n  Random Access Across Collections:');
      console.log(`    Access count: ${accessCount}`);
      console.log(`    Unique collections accessed: ${accessedCollections.size}`);
      console.log(`    Duration: ${formatDuration(durationMs)}`);
      console.log(`    Avg access time: ${avgAccessTime.toFixed(3)}ms`);
      console.log(`    Accesses/sec: ${((accessCount / durationMs) * 1000).toFixed(0)}`);

      expect(accessedCollections.size).toBeGreaterThan(COLLECTION_COUNT * 0.9);
    });
  });

  describe('Collection Lifecycle at Scale', () => {
    it('should handle create/drop cycles for 500 collections', async () => {
      const collectionCount = 500;
      const cycles = 3;
      const cycleTimes: { create: number; drop: number }[] = [];

      for (let cycle = 0; cycle < cycles; cycle++) {
        // Create phase
        const createStart = Date.now();
        for (let i = 0; i < collectionCount; i++) {
          manager.createCollection(generateCollectionName(`cycle_${cycle}`, i));
        }
        const createDuration = Date.now() - createStart;

        // Verify creation
        expect(manager.listCollections().length).toBe(collectionCount);

        // Drop phase
        const dropStart = Date.now();
        for (let i = 0; i < collectionCount; i++) {
          manager.dropCollection(generateCollectionName(`cycle_${cycle}`, i));
        }
        const dropDuration = Date.now() - dropStart;

        // Verify drop
        expect(manager.listCollections().length).toBe(0);

        cycleTimes.push({ create: createDuration, drop: dropDuration });
      }

      console.log('\n  Collection Create/Drop Cycles:');
      for (let i = 0; i < cycleTimes.length; i++) {
        console.log(`    Cycle ${i + 1}: Create ${cycleTimes[i]!.create}ms, Drop ${cycleTimes[i]!.drop}ms`);
      }

      // Performance should be consistent across cycles
      // Add 1ms buffer to handle very fast operations where timing is 0ms
      const lastCycle = cycleTimes[cycleTimes.length - 1]!;
      const firstCycle = cycleTimes[0]!;
      expect(lastCycle.create).toBeLessThan((firstCycle.create + 1) * 2);
    });

    it('should handle mixed create/drop/query operations', async () => {
      const operationCount = 2000;
      const maxCollections = 200;
      let createOps = 0;
      let dropOps = 0;
      let queryOps = 0;
      let existingCollections: string[] = [];

      const startTime = Date.now();

      for (let i = 0; i < operationCount; i++) {
        const operation = Math.random();
        const collectionName = generateCollectionName('mixed', i % maxCollections);

        if (operation < 0.4 && existingCollections.length < maxCollections) {
          // Create (40% of ops when under limit)
          if (!manager.hasCollection(collectionName)) {
            manager.createCollection(collectionName);
            existingCollections.push(collectionName);
            createOps++;
          }
        } else if (operation < 0.5 && existingCollections.length > 0) {
          // Drop (10% of ops when collections exist)
          const randomIdx = Math.floor(Math.random() * existingCollections.length);
          const toDrop = existingCollections[randomIdx]!;
          if (manager.dropCollection(toDrop)) {
            existingCollections = existingCollections.filter((c) => c !== toDrop);
            dropOps++;
          }
        } else if (existingCollections.length > 0) {
          // Query (remaining ops)
          const randomIdx = Math.floor(Math.random() * existingCollections.length);
          await manager.find(existingCollections[randomIdx]!);
          queryOps++;
        }
      }

      const durationMs = Date.now() - startTime;
      const finalCollections = manager.listCollections().length;

      console.log('\n  Mixed Collection Operations:');
      console.log(`    Duration: ${formatDuration(durationMs)}`);
      console.log(`    Create ops: ${createOps}`);
      console.log(`    Drop ops: ${dropOps}`);
      console.log(`    Query ops: ${queryOps}`);
      console.log(`    Final collections: ${finalCollections}`);
      console.log(`    Ops/sec: ${((operationCount / durationMs) * 1000).toFixed(0)}`);

      expect(createOps + dropOps + queryOps).toBeGreaterThan(0);
    });
  });

  describe('Cross-Collection Queries', () => {
    it('should perform cross-collection join-like operations', async () => {
      // Create users and orders collections
      const userGenerator = createUserGenerator();
      const orderGenerator = createOrderGenerator();

      const userCollections = 10;
      const orderCollections = 10;
      const docsPerCollection = 100;

      // Create user collections
      for (let i = 0; i < userCollections; i++) {
        const name = `users_${i}`;
        manager.createCollection(name);
        await manager.insertMany(name, userGenerator.generateBatch(docsPerCollection) as unknown as Record<string, unknown>[]);
      }

      // Create order collections with references to users
      for (let i = 0; i < orderCollections; i++) {
        const name = `orders_${i}`;
        manager.createCollection(name);
        await manager.insertMany(name, orderGenerator.generateBatch(docsPerCollection) as unknown as Record<string, unknown>[]);
      }

      const startTime = Date.now();

      // Simulate a cross-collection query (orders with user lookup)
      const orderResults: unknown[] = [];
      for (let i = 0; i < orderCollections; i++) {
        const orders = await manager.find(`orders_${i}`);
        orderResults.push(...orders);
      }

      const userLookups = new Map<string, unknown>();
      for (const order of orderResults) {
        const customerId = (order as Record<string, unknown>).customerId as string;
        if (!userLookups.has(customerId)) {
          // Find user across all user collections
          for (let i = 0; i < userCollections; i++) {
            const user = await manager.findOne(`users_${i}`, { _id: customerId });
            if (user) {
              userLookups.set(customerId, user);
              break;
            }
          }
        }
      }

      const durationMs = Date.now() - startTime;

      console.log('\n  Cross-Collection Join Simulation:');
      console.log(`    User collections: ${userCollections}`);
      console.log(`    Order collections: ${orderCollections}`);
      console.log(`    Total orders: ${orderResults.length}`);
      console.log(`    User lookups performed: ${userLookups.size}`);
      console.log(`    Duration: ${formatDuration(durationMs)}`);

      expect(orderResults.length).toBe(orderCollections * docsPerCollection);
    });

    it('should aggregate data from multiple collections', async () => {
      const eventGenerator = createEventGenerator();
      const collectionCount = 20;
      const docsPerCollection = 500;

      // Create event collections partitioned by type
      const eventTypes = ['page_view', 'click', 'form_submit', 'purchase'];

      for (const eventType of eventTypes) {
        for (let i = 0; i < collectionCount / eventTypes.length; i++) {
          const name = `events_${eventType}_${i}`;
          manager.createCollection(name);

          const docs = eventGenerator.generateBatch(docsPerCollection);
          // Set eventType for all docs in this collection
          for (const doc of docs) {
            (doc as unknown as Record<string, unknown>).eventType = eventType;
          }
          await manager.insertMany(name, docs as unknown as Record<string, unknown>[]);
        }
      }

      const startTime = Date.now();

      // Aggregate counts by event type across all collections
      const aggregateResults: { eventType: string; count: number }[] = [];

      for (const eventType of eventTypes) {
        let totalCount = 0;
        for (let i = 0; i < collectionCount / eventTypes.length; i++) {
          const name = `events_${eventType}_${i}`;
          totalCount += await manager.countDocuments(name);
        }
        aggregateResults.push({ eventType, count: totalCount });
      }

      const durationMs = Date.now() - startTime;
      const totalEvents = aggregateResults.reduce((sum, r) => sum + r.count, 0);

      console.log('\n  Multi-Collection Aggregation:');
      console.log(`    Collections: ${collectionCount}`);
      console.log(`    Duration: ${formatDuration(durationMs)}`);
      console.log(`    Total events: ${totalEvents}`);
      for (const r of aggregateResults) {
        console.log(`    ${r.eventType}: ${r.count}`);
      }

      expect(totalEvents).toBe(collectionCount * docsPerCollection);
    });
  });

  describe('Memory and Performance Under Collection Load', () => {
    it('should track memory usage with many collections', async () => {
      const memorySnapshots: { collections: number; heapUsed: number }[] = [];
      const collectionCount = 500;
      const docsPerCollection = 50;
      const userGenerator = createUserGenerator();

      memorySnapshots.push({ collections: 0, heapUsed: captureMemory().heapUsed });

      for (let i = 0; i < collectionCount; i++) {
        const name = generateCollectionName('memory_test', i);
        manager.createCollection(name);

        const docs = userGenerator.generateBatch(docsPerCollection);
        await manager.insertMany(name, docs as unknown as Record<string, unknown>[]);

        if ((i + 1) % 100 === 0) {
          memorySnapshots.push({
            collections: i + 1,
            heapUsed: captureMemory().heapUsed,
          });
        }
      }

      const stats = manager.getStats();
      const startHeap = memorySnapshots[0]!.heapUsed;
      const endHeap = memorySnapshots[memorySnapshots.length - 1]!.heapUsed;
      const heapGrowth = endHeap - startHeap;

      console.log('\n  Memory Usage with Many Collections:');
      console.log(`    Collections: ${stats.collectionCount}`);
      console.log(`    Total documents: ${stats.totalDocuments}`);
      console.log(`    Data size: ${formatBytes(stats.totalBytes)}`);
      console.log(`    Start heap: ${formatBytes(startHeap)}`);
      console.log(`    End heap: ${formatBytes(endHeap)}`);
      console.log(`    Heap growth: ${formatBytes(heapGrowth)}`);

      // Memory growth should be proportional to data size
      expect(heapGrowth).toBeLessThan(stats.totalBytes * 5);
    });

    it('should maintain consistent performance as collection count grows', async () => {
      const phases = [100, 200, 400, 800];
      const queryCount = 100;
      const docsPerCollection = 20;
      const userGenerator = createUserGenerator();

      const phaseResults: { collections: number; avgQueryTime: number }[] = [];

      let totalCollections = 0;

      for (const targetCollections of phases) {
        // Add more collections
        while (totalCollections < targetCollections) {
          const name = generateCollectionName('perf_test', totalCollections);
          manager.createCollection(name);
          await manager.insertMany(name, userGenerator.generateBatch(docsPerCollection) as unknown as Record<string, unknown>[]);
          totalCollections++;
        }

        // Measure query performance
        const queryTimes: number[] = [];
        for (let i = 0; i < queryCount; i++) {
          const randomCollection = generateCollectionName('perf_test', Math.floor(Math.random() * totalCollections));
          const start = Date.now();
          await manager.find(randomCollection);
          queryTimes.push(Date.now() - start);
        }

        const avgQueryTime = queryTimes.reduce((a, b) => a + b, 0) / queryTimes.length;
        phaseResults.push({ collections: totalCollections, avgQueryTime });
      }

      console.log('\n  Query Performance vs Collection Count:');
      for (const r of phaseResults) {
        console.log(`    ${r.collections} collections: avg ${r.avgQueryTime.toFixed(3)}ms/query`);
      }

      // Query time should not degrade significantly (less than 5x slowdown)
      const firstPhase = phaseResults[0]!;
      const lastPhase = phaseResults[phaseResults.length - 1]!;
      expect(lastPhase.avgQueryTime).toBeLessThan(firstPhase.avgQueryTime * 5 + 5);
    });
  });

  describe('Namespace Patterns', () => {
    it('should handle hierarchical collection names', async () => {
      const databases = ['app', 'analytics', 'logs'];
      const schemas = ['v1', 'v2'];
      const tables = ['users', 'events', 'sessions'];

      const namespacedCollections: string[] = [];

      // Create hierarchical collections: db.schema.table
      for (const db of databases) {
        for (const schema of schemas) {
          for (const table of tables) {
            const name = `${db}.${schema}.${table}`;
            manager.createCollection(name);
            namespacedCollections.push(name);
          }
        }
      }

      expect(manager.listCollections().length).toBe(namespacedCollections.length);

      // Test prefix-based listing (simulated)
      const appCollections = namespacedCollections.filter((c) => c.startsWith('app.'));
      const v1Collections = namespacedCollections.filter((c) => c.includes('.v1.'));

      console.log('\n  Hierarchical Namespace Test:');
      console.log(`    Total collections: ${namespacedCollections.length}`);
      console.log(`    'app.*' collections: ${appCollections.length}`);
      console.log(`    '*.v1.*' collections: ${v1Collections.length}`);

      expect(appCollections.length).toBe(schemas.length * tables.length);
      expect(v1Collections.length).toBe(databases.length * tables.length);
    });

    it('should support sharded collection distribution', async () => {
      const collectionCount = 100;
      const shardCount = 16;
      const docsPerCollection = 50;
      const userGenerator = createUserGenerator();

      // Simulate shard distribution
      const shardAssignments = new Map<number, string[]>();
      for (let i = 0; i < shardCount; i++) {
        shardAssignments.set(i, []);
      }

      for (let i = 0; i < collectionCount; i++) {
        const name = generateCollectionName('sharded', i);
        manager.createCollection(name);
        await manager.insertMany(name, userGenerator.generateBatch(docsPerCollection) as unknown as Record<string, unknown>[]);

        // Hash-based shard assignment
        const shardId = i % shardCount;
        shardAssignments.get(shardId)!.push(name);
      }

      console.log('\n  Sharded Collection Distribution:');
      console.log(`    Total collections: ${collectionCount}`);
      console.log(`    Shards: ${shardCount}`);

      const shardStats: { shardId: number; collections: number }[] = [];
      for (const [shardId, collections] of shardAssignments) {
        shardStats.push({ shardId, collections: collections.length });
      }

      // All shards should have roughly equal distribution
      const avgPerShard = collectionCount / shardCount;
      const minCollections = Math.min(...shardStats.map((s) => s.collections));
      const maxCollections = Math.max(...shardStats.map((s) => s.collections));

      console.log(`    Avg per shard: ${avgPerShard.toFixed(1)}`);
      console.log(`    Min: ${minCollections}, Max: ${maxCollections}`);

      expect(minCollections).toBeGreaterThan(avgPerShard * 0.5);
      expect(maxCollections).toBeLessThan(avgPerShard * 1.5);
    });
  });
});
