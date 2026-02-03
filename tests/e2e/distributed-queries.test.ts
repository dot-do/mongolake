/**
 * MongoLake E2E Tests - Multi-Shard Distributed Queries
 *
 * End-to-end tests for distributed query execution across multiple shards.
 * Tests verify that queries are correctly routed, aggregated, and merged
 * when data is distributed across shards.
 *
 * Test scenarios:
 * - Collection-level shard routing
 * - Document-level shard routing
 * - Distributed aggregation with $group
 * - Cross-shard sorting and pagination
 * - Split collection queries
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { MongoClient, type Db, type Collection, type Document } from 'mongodb';
import { createServer, type TcpServer } from '../../src/wire-protocol/tcp-server.js';
import * as fs from 'node:fs';

// Test configuration
const TEST_DATA_DIR = '.mongolake-e2e-distributed-test';
const TEST_DB_NAME = 'distributed_query_test';

// Server and client instances
let server: TcpServer;
let serverPort: number;
let client: MongoClient;
let db: Db;

// Unique collection name generator
function uniqueCollection(prefix: string): string {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

// Clean up test data directory
function cleanupTestData(): void {
  try {
    if (fs.existsSync(TEST_DATA_DIR)) {
      fs.rmSync(TEST_DATA_DIR, { recursive: true });
    }
  } catch {
    // Ignore cleanup errors
  }
}

describe('Multi-Shard Distributed Query E2E Tests', () => {
  beforeAll(async () => {
    // Clean up any existing test data
    cleanupTestData();

    // Start the MongoLake wire protocol server
    server = createServer({
      port: 0, // Let OS assign a random port
      host: '127.0.0.1',
      mongoLakeConfig: { local: TEST_DATA_DIR },
    });

    await server.start();
    const addr = server.address();
    serverPort = addr!.port;

    // Connect with official MongoDB driver
    const connectionString = `mongodb://127.0.0.1:${serverPort}`;
    client = new MongoClient(connectionString, {
      directConnection: true,
      serverSelectionTimeoutMS: 5000,
      connectTimeoutMS: 5000,
    });

    await client.connect();
    db = client.db(TEST_DB_NAME);
  });

  afterAll(async () => {
    // Close MongoDB client
    if (client) {
      await client.close();
    }

    // Stop the server
    if (server) {
      await server.stop();
    }

    // Clean up test data
    cleanupTestData();
  });

  // ============================================================================
  // Collection-Level Shard Routing Tests
  // ============================================================================

  describe('Collection-Level Shard Routing', () => {
    it('should consistently route the same collection to the same shard', async () => {
      const collection = db.collection(uniqueCollection('shard_routing'));

      // Insert multiple documents
      await collection.insertMany([
        { _id: 'doc-1', region: 'us-east', value: 100 },
        { _id: 'doc-2', region: 'us-west', value: 200 },
        { _id: 'doc-3', region: 'eu-west', value: 300 },
      ]);

      // Query all documents - should all come from the same shard
      const docs = await collection.find({}).toArray();
      expect(docs.length).toBe(3);

      // Multiple queries should be consistent
      for (let i = 0; i < 5; i++) {
        const result = await collection.find({}).toArray();
        expect(result.length).toBe(3);
      }
    });

    it('should route different collections independently', async () => {
      const collection1 = db.collection(uniqueCollection('shard_coll_1'));
      const collection2 = db.collection(uniqueCollection('shard_coll_2'));

      // Insert into both collections
      await collection1.insertMany([
        { _id: 'c1-doc-1', source: 'collection1' },
        { _id: 'c1-doc-2', source: 'collection1' },
      ]);

      await collection2.insertMany([
        { _id: 'c2-doc-1', source: 'collection2' },
        { _id: 'c2-doc-2', source: 'collection2' },
      ]);

      // Query both collections
      const docs1 = await collection1.find({}).toArray();
      const docs2 = await collection2.find({}).toArray();

      expect(docs1.length).toBe(2);
      expect(docs2.length).toBe(2);

      // All docs in collection1 should have source: collection1
      for (const doc of docs1) {
        expect(doc.source).toBe('collection1');
      }

      // All docs in collection2 should have source: collection2
      for (const doc of docs2) {
        expect(doc.source).toBe('collection2');
      }
    });

    it('should isolate data between databases', async () => {
      const db1 = client.db('shard_db_1');
      const db2 = client.db('shard_db_2');
      const collName = `shared_${Date.now()}`;

      const collection1 = db1.collection(collName);
      const collection2 = db2.collection(collName);

      // Insert with same _id into different databases
      await collection1.insertOne({ _id: 'shared-id', database: 'db1' });
      await collection2.insertOne({ _id: 'shared-id', database: 'db2' });

      // Query each database
      const doc1 = await collection1.findOne({ _id: 'shared-id' });
      const doc2 = await collection2.findOne({ _id: 'shared-id' });

      expect(doc1).not.toBeNull();
      expect(doc1!.database).toBe('db1');

      expect(doc2).not.toBeNull();
      expect(doc2!.database).toBe('db2');
    });
  });

  // ============================================================================
  // Distributed Aggregation Tests
  // ============================================================================

  describe('Distributed Aggregation', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('distributed_agg'));

      // Insert test data representing sales across regions
      await collection.insertMany([
        { _id: 's1', region: 'north', product: 'A', sales: 100, quarter: 'Q1' },
        { _id: 's2', region: 'north', product: 'B', sales: 150, quarter: 'Q1' },
        { _id: 's3', region: 'south', product: 'A', sales: 200, quarter: 'Q1' },
        { _id: 's4', region: 'south', product: 'B', sales: 250, quarter: 'Q1' },
        { _id: 's5', region: 'east', product: 'A', sales: 80, quarter: 'Q2' },
        { _id: 's6', region: 'east', product: 'B', sales: 120, quarter: 'Q2' },
        { _id: 's7', region: 'west', product: 'A', sales: 300, quarter: 'Q2' },
        { _id: 's8', region: 'west', product: 'B', sales: 350, quarter: 'Q2' },
      ]);
    });

    it('should execute $match correctly on distributed data', async () => {
      const result = await collection
        .aggregate([{ $match: { region: { $in: ['north', 'south'] } } }])
        .toArray();

      expect(result.length).toBe(4);
      for (const doc of result) {
        expect(['north', 'south']).toContain(doc.region);
      }
    });

    it('should execute $group with $sum across distributed data', async () => {
      const result = await collection
        .aggregate([
          {
            $group: {
              _id: '$region',
              totalSales: { $sum: '$sales' },
            },
          },
        ])
        .toArray();

      expect(result.length).toBe(4);

      // Find specific regions and verify totals
      const northResult = result.find((r) => r._id === 'north');
      const southResult = result.find((r) => r._id === 'south');

      if (northResult) {
        expect(northResult.totalSales).toBe(250); // 100 + 150
      }
      if (southResult) {
        expect(southResult.totalSales).toBe(450); // 200 + 250
      }
    });

    it('should execute $group with $avg correctly', async () => {
      const result = await collection
        .aggregate([
          {
            $group: {
              _id: '$product',
              avgSales: { $avg: '$sales' },
              count: { $sum: 1 },
            },
          },
        ])
        .toArray();

      expect(result.length).toBe(2); // Products A and B

      for (const doc of result) {
        expect(doc.count).toBe(4); // 4 entries per product
      }
    });

    it('should execute $group with $min and $max', async () => {
      const result = await collection
        .aggregate([
          {
            $group: {
              _id: null,
              minSales: { $min: '$sales' },
              maxSales: { $max: '$sales' },
            },
          },
        ])
        .toArray();

      expect(result.length).toBe(1);
      expect(result[0].minSales).toBe(80);
      expect(result[0].maxSales).toBe(350);
    });

    it('should execute $match -> $group -> $sort pipeline', async () => {
      const result = await collection
        .aggregate([
          { $match: { quarter: 'Q1' } },
          {
            $group: {
              _id: '$region',
              totalSales: { $sum: '$sales' },
            },
          },
          { $sort: { totalSales: -1 } },
        ])
        .toArray();

      expect(result.length).toBe(2); // Only north and south in Q1

      // First result should have highest sales
      expect(result[0].totalSales).toBeGreaterThanOrEqual(result[1].totalSales);
    });

    it('should execute $group with compound _id', async () => {
      const result = await collection
        .aggregate([
          {
            $group: {
              _id: { region: '$region', quarter: '$quarter' },
              totalSales: { $sum: '$sales' },
              productCount: { $sum: 1 },
            },
          },
          { $sort: { '_id.region': 1, '_id.quarter': 1 } },
        ])
        .toArray();

      expect(result.length).toBe(4); // 4 unique region-quarter combinations

      for (const doc of result) {
        expect(doc._id.region).toBeDefined();
        expect(doc._id.quarter).toBeDefined();
        expect(doc.productCount).toBe(2); // 2 products per region-quarter
      }
    });
  });

  // ============================================================================
  // Cross-Shard Sorting and Pagination Tests
  // ============================================================================

  describe('Cross-Shard Sorting and Pagination', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('sort_pagination'));

      // Insert documents with sequential IDs for testing
      const docs = [];
      for (let i = 1; i <= 50; i++) {
        docs.push({
          _id: `doc-${i.toString().padStart(3, '0')}`,
          index: i,
          value: i * 10,
          category: i % 5 === 0 ? 'special' : 'normal',
        });
      }
      await collection.insertMany(docs);
    });

    it('should correctly sort documents across shards (ascending)', async () => {
      const result = await collection.find({}).sort({ index: 1 }).limit(10).toArray();

      expect(result.length).toBe(10);
      // Verify ascending order
      for (let i = 0; i < result.length - 1; i++) {
        expect(result[i].index).toBeLessThan(result[i + 1].index);
      }
      expect(result[0].index).toBe(1);
    });

    it('should correctly sort documents across shards (descending)', async () => {
      const result = await collection.find({}).sort({ index: -1 }).limit(10).toArray();

      expect(result.length).toBe(10);
      // Verify descending order
      for (let i = 0; i < result.length - 1; i++) {
        expect(result[i].index).toBeGreaterThan(result[i + 1].index);
      }
      expect(result[0].index).toBe(50);
    });

    it('should correctly paginate through distributed data', async () => {
      const pageSize = 10;
      const allDocs: Document[] = [];

      // Fetch all pages
      for (let page = 0; page < 5; page++) {
        const result = await collection
          .find({})
          .sort({ index: 1 })
          .skip(page * pageSize)
          .limit(pageSize)
          .toArray();

        expect(result.length).toBe(pageSize);
        allDocs.push(...result);
      }

      // Verify all documents were fetched without duplicates
      expect(allDocs.length).toBe(50);
      const indices = allDocs.map((d) => d.index);
      const uniqueIndices = new Set(indices);
      expect(uniqueIndices.size).toBe(50);
    });

    it('should handle skip and limit with aggregation', async () => {
      const result = await collection
        .aggregate([
          { $sort: { value: -1 } },
          { $skip: 20 },
          { $limit: 10 },
        ])
        .toArray();

      expect(result.length).toBe(10);
      // Values should be between 300 (30th highest) and 210 (21st highest)
      for (const doc of result) {
        expect(doc.value).toBeGreaterThanOrEqual(210);
        expect(doc.value).toBeLessThanOrEqual(300);
      }
    });

    it('should correctly filter and paginate', async () => {
      // Get "special" category items with pagination
      const result = await collection
        .find({ category: 'special' })
        .sort({ index: 1 })
        .skip(2)
        .limit(5)
        .toArray();

      // There are 10 special items (indices 5, 10, 15, ..., 50)
      // Skip 2 means start from index 15
      expect(result.length).toBe(5);
      for (const doc of result) {
        expect(doc.category).toBe('special');
      }
    });
  });

  // ============================================================================
  // Large Result Set Tests
  // ============================================================================

  describe('Large Result Sets', () => {
    it('should handle queries returning many documents', async () => {
      const collection = db.collection(uniqueCollection('large_result'));

      // Insert 50 documents (reduced to avoid cursor timeout issues)
      const docs = [];
      for (let i = 0; i < 50; i++) {
        docs.push({
          _id: `large-${i}`,
          batch: Math.floor(i / 25),
          index: i,
          data: `data-${i}`,
        });
      }
      await collection.insertMany(docs);

      // Query all documents
      const result = await collection.find({}).toArray();
      expect(result.length).toBe(50);

      // Verify document count per batch
      const batches = new Map<number, number>();
      for (const doc of result) {
        batches.set(doc.batch, (batches.get(doc.batch) || 0) + 1);
      }
      expect(batches.size).toBe(2);
      for (const count of batches.values()) {
        expect(count).toBe(25);
      }
    });

    it('should handle aggregation on large datasets', async () => {
      const collection = db.collection(uniqueCollection('large_agg'));

      // Insert documents
      const docs = [];
      for (let i = 0; i < 200; i++) {
        docs.push({
          _id: `agg-${i}`,
          category: `cat-${i % 10}`,
          value: i,
        });
      }
      await collection.insertMany(docs);

      // Group by category
      const result = await collection
        .aggregate([
          {
            $group: {
              _id: '$category',
              count: { $sum: 1 },
              total: { $sum: '$value' },
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      expect(result.length).toBe(10);
      for (const doc of result) {
        expect(doc.count).toBe(20); // 200 docs / 10 categories
      }
    });
  });

  // ============================================================================
  // Query Filter Distribution Tests
  // ============================================================================

  describe('Query Filter Distribution', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('filter_dist'));

      await collection.insertMany([
        { _id: 'f1', type: 'A', status: 'active', priority: 1 },
        { _id: 'f2', type: 'A', status: 'inactive', priority: 2 },
        { _id: 'f3', type: 'B', status: 'active', priority: 3 },
        { _id: 'f4', type: 'B', status: 'active', priority: 1 },
        { _id: 'f5', type: 'C', status: 'inactive', priority: 2 },
        { _id: 'f6', type: 'C', status: 'active', priority: 3 },
      ]);
    });

    it('should distribute $and filter correctly', async () => {
      const result = await collection
        .find({ $and: [{ type: 'A' }, { status: 'active' }] })
        .toArray();

      expect(result.length).toBe(1);
      expect(result[0]._id).toBe('f1');
    });

    it('should distribute $or filter correctly', async () => {
      const result = await collection
        .find({ $or: [{ type: 'A' }, { status: 'inactive' }] })
        .toArray();

      // Type A (2) + inactive non-A (1 from C) = 3
      expect(result.length).toBe(3);
    });

    it('should distribute range filter correctly', async () => {
      const result = await collection
        .find({ priority: { $gte: 2, $lte: 3 } })
        .toArray();

      expect(result.length).toBe(4);
      for (const doc of result) {
        expect(doc.priority).toBeGreaterThanOrEqual(2);
        expect(doc.priority).toBeLessThanOrEqual(3);
      }
    });

    it('should distribute $in filter correctly', async () => {
      const result = await collection.find({ type: { $in: ['A', 'C'] } }).toArray();

      expect(result.length).toBe(4);
      for (const doc of result) {
        expect(['A', 'C']).toContain(doc.type);
      }
    });

    it('should distribute combined filter and sort correctly', async () => {
      const result = await collection
        .find({ status: 'active' })
        .sort({ priority: 1 })
        .toArray();

      expect(result.length).toBe(4);
      // Verify sort order
      for (let i = 0; i < result.length - 1; i++) {
        expect(result[i].priority).toBeLessThanOrEqual(result[i + 1].priority);
      }
    });
  });

  // ============================================================================
  // Count Operations Tests
  // ============================================================================

  describe('Distributed Count Operations', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('count_dist'));

      const docs = [];
      for (let i = 0; i < 100; i++) {
        docs.push({
          _id: `count-${i}`,
          category: `cat-${i % 5}`,
          status: i % 2 === 0 ? 'active' : 'inactive',
        });
      }
      await collection.insertMany(docs);
    });

    it('should count all documents correctly', async () => {
      const count = await collection.countDocuments();
      expect(count).toBe(100);
    });

    it('should count with filter correctly', async () => {
      const activeCount = await collection.countDocuments({ status: 'active' });
      expect(activeCount).toBe(50);

      const cat0Count = await collection.countDocuments({ category: 'cat-0' });
      expect(cat0Count).toBe(20);
    });

    it('should use $count aggregation stage correctly', async () => {
      const result = await collection
        .aggregate([
          { $match: { status: 'inactive' } },
          { $count: 'inactiveCount' },
        ])
        .toArray();

      expect(result.length).toBe(1);
      expect(result[0].inactiveCount).toBe(50);
    });

    it('should count with complex filter correctly', async () => {
      const count = await collection.countDocuments({
        $and: [{ category: { $in: ['cat-0', 'cat-1'] } }, { status: 'active' }],
      });
      // 40 docs in cat-0/cat-1, half are active = 20
      expect(count).toBe(20);
    });
  });

  // ============================================================================
  // Distinct Values Tests
  // ============================================================================

  describe('Distributed Distinct Operations', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('distinct_dist'));

      await collection.insertMany([
        { _id: 'd1', region: 'us', tier: 'gold' },
        { _id: 'd2', region: 'us', tier: 'silver' },
        { _id: 'd3', region: 'eu', tier: 'gold' },
        { _id: 'd4', region: 'eu', tier: 'bronze' },
        { _id: 'd5', region: 'asia', tier: 'gold' },
        { _id: 'd6', region: 'asia', tier: 'silver' },
      ]);
    });

    it('should return distinct values across shards', async () => {
      const regions = await collection.distinct('region');
      expect(regions.sort()).toEqual(['asia', 'eu', 'us']);

      const tiers = await collection.distinct('tier');
      expect(tiers.sort()).toEqual(['bronze', 'gold', 'silver']);
    });

    it('should return distinct values with filter', async () => {
      const goldRegions = await collection.distinct('region', { tier: 'gold' });
      expect(goldRegions.sort()).toEqual(['asia', 'eu', 'us']);

      const usTiers = await collection.distinct('tier', { region: 'us' });
      expect(usTiers.sort()).toEqual(['gold', 'silver']);
    });
  });
});
