/**
 * MongoLake Aggregation Pipeline Compatibility Tests
 *
 * Tests aggregation pipeline operations for MongoDB compatibility.
 * Based on MongoDB specification tests from:
 * https://github.com/mongodb/specifications/tree/master/source/crud/tests/unified
 *
 * Tests the following pipeline stages:
 * - $match, $project, $sort, $limit, $skip (basic stages)
 * - $group with accumulators ($sum, $avg, $min, $max, $first, $last, $push, $addToSet, $count)
 * - $unwind (array expansion)
 * - $lookup (basic cases)
 * - $facet (parallel pipelines)
 * - $bucket (document bucketing)
 * - $count, $addFields, $set, $unset (utility stages)
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { MongoClient, type Db, type Collection, type Document } from 'mongodb';
import { createServer, type TcpServer } from '../../../src/wire-protocol/tcp-server.js';
import * as fs from 'node:fs';

// ============================================================================
// Test Configuration
// ============================================================================

const TEST_DATA_DIR = '.mongolake-compat-agg-test';
const TEST_DB_NAME = 'compat_aggregation_test';

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

// ============================================================================
// Compatibility Matrix Tracking
// ============================================================================

interface StageCompatibility {
  stage: string;
  supported: boolean;
  tests: number;
  passed: number;
  notes?: string;
}

const compatibilityMatrix: StageCompatibility[] = [];

function trackStageResult(stage: string, passed: boolean, notes?: string): void {
  let entry = compatibilityMatrix.find((e) => e.stage === stage);
  if (!entry) {
    entry = { stage, supported: true, tests: 0, passed: 0 };
    compatibilityMatrix.push(entry);
  }
  entry.tests++;
  if (passed) {
    entry.passed++;
  } else {
    entry.supported = false;
  }
  if (notes) {
    entry.notes = notes;
  }
}

// ============================================================================
// Test Setup
// ============================================================================

describe('MongoLake Aggregation Compatibility Tests', () => {
  beforeAll(async () => {
    cleanupTestData();

    server = createServer({
      port: 0,
      host: '127.0.0.1',
      mongoLakeConfig: { local: TEST_DATA_DIR },
    });

    await server.start();
    const addr = server.address();
    serverPort = addr!.port;

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
    if (client) {
      await client.close();
    }

    if (server) {
      await server.stop();
    }

    cleanupTestData();

    // Print compatibility matrix
    console.log('\n========================================');
    console.log('AGGREGATION STAGE COMPATIBILITY MATRIX');
    console.log('========================================\n');
    for (const entry of compatibilityMatrix.sort((a, b) => a.stage.localeCompare(b.stage))) {
      const status = entry.supported ? 'SUPPORTED' : 'PARTIAL';
      const ratio = `${entry.passed}/${entry.tests}`;
      console.log(`${entry.stage.padEnd(20)} ${status.padEnd(12)} ${ratio.padEnd(8)} ${entry.notes || ''}`);
    }
    console.log('\n========================================\n');
  });

  // ============================================================================
  // $match Stage Tests
  // ============================================================================

  describe('$match Stage', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('match'));
      await collection.insertMany([
        { _id: '1', category: 'electronics', price: 500, inStock: true, tags: ['sale', 'new'] },
        { _id: '2', category: 'electronics', price: 800, inStock: false, tags: ['premium'] },
        { _id: '3', category: 'clothing', price: 50, inStock: true, tags: ['sale'] },
        { _id: '4', category: 'clothing', price: 80, inStock: true, tags: [] },
        { _id: '5', category: 'books', price: 20, inStock: true, tags: ['sale', 'bestseller'] },
        { _id: '6', category: 'books', price: 35, inStock: false, tags: null },
      ]);
    });

    it('should filter with field equality', async () => {
      const result = await collection
        .aggregate([{ $match: { category: 'electronics' } }])
        .toArray();

      const passed = result.length === 2 && result.every((doc) => doc.category === 'electronics');
      trackStageResult('$match', passed);
      expect(passed).toBe(true);
    });

    it('should filter with $gt comparison', async () => {
      const result = await collection
        .aggregate([{ $match: { price: { $gt: 100 } } }])
        .toArray();

      const passed = result.length === 2 && result.every((doc) => (doc.price as number) > 100);
      trackStageResult('$match', passed);
      expect(passed).toBe(true);
    });

    it('should filter with $gte comparison', async () => {
      const result = await collection
        .aggregate([{ $match: { price: { $gte: 50 } } }])
        .toArray();

      const passed = result.length === 4 && result.every((doc) => (doc.price as number) >= 50);
      trackStageResult('$match', passed);
      expect(passed).toBe(true);
    });

    it('should filter with $lt comparison', async () => {
      const result = await collection
        .aggregate([{ $match: { price: { $lt: 50 } } }])
        .toArray();

      const passed = result.length === 2 && result.every((doc) => (doc.price as number) < 50);
      trackStageResult('$match', passed);
      expect(passed).toBe(true);
    });

    it('should filter with $lte comparison', async () => {
      const result = await collection
        .aggregate([{ $match: { price: { $lte: 50 } } }])
        .toArray();

      const passed = result.length === 3 && result.every((doc) => (doc.price as number) <= 50);
      trackStageResult('$match', passed);
      expect(passed).toBe(true);
    });

    it('should filter with $in operator', async () => {
      const result = await collection
        .aggregate([{ $match: { category: { $in: ['electronics', 'books'] } } }])
        .toArray();

      const passed = result.length === 4 && result.every((doc) => ['electronics', 'books'].includes(doc.category as string));
      trackStageResult('$match', passed);
      expect(passed).toBe(true);
    });

    it('should filter with $ne operator', async () => {
      const result = await collection
        .aggregate([{ $match: { category: { $ne: 'clothing' } } }])
        .toArray();

      const passed = result.length === 4 && result.every((doc) => doc.category !== 'clothing');
      trackStageResult('$match', passed);
      expect(passed).toBe(true);
    });

    it('should filter with $nin operator', async () => {
      const result = await collection
        .aggregate([{ $match: { category: { $nin: ['electronics', 'books'] } } }])
        .toArray();

      const passed = result.length === 2 && result.every((doc) => !['electronics', 'books'].includes(doc.category as string));
      trackStageResult('$match', passed);
      expect(passed).toBe(true);
    });

    it('should filter with multiple conditions (implicit AND)', async () => {
      const result = await collection
        .aggregate([{ $match: { category: 'clothing', inStock: true } }])
        .toArray();

      const passed = result.length === 2 && result.every((doc) => doc.category === 'clothing' && doc.inStock === true);
      trackStageResult('$match', passed);
      expect(passed).toBe(true);
    });

    it('should filter with $or operator', async () => {
      const result = await collection
        .aggregate([{ $match: { $or: [{ category: 'electronics' }, { price: { $lt: 30 } }] } }])
        .toArray();

      const passed = result.length === 3;
      trackStageResult('$match', passed);
      expect(passed).toBe(true);
    });

    it('should filter with $and operator', async () => {
      const result = await collection
        .aggregate([{ $match: { $and: [{ category: 'electronics' }, { inStock: true }] } }])
        .toArray();

      const passed = result.length === 1 && result[0]._id === '1';
      trackStageResult('$match', passed);
      expect(passed).toBe(true);
    });

    it('should filter with empty match (returns all)', async () => {
      const result = await collection.aggregate([{ $match: {} }]).toArray();

      const passed = result.length === 6;
      trackStageResult('$match', passed);
      expect(passed).toBe(true);
    });

    it('should return empty array when no matches', async () => {
      const result = await collection
        .aggregate([{ $match: { category: 'nonexistent' } }])
        .toArray();

      const passed = result.length === 0;
      trackStageResult('$match', passed);
      expect(passed).toBe(true);
    });
  });

  // ============================================================================
  // $project Stage Tests
  // ============================================================================

  describe('$project Stage', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('project'));
      await collection.insertMany([
        { _id: '1', name: 'Product A', price: 100, cost: 60, metadata: { supplier: 'Acme', category: 'electronics' } },
        { _id: '2', name: 'Product B', price: 200, cost: 120, metadata: { supplier: 'Beta', category: 'clothing' } },
      ]);
    });

    it('should include specified fields', async () => {
      const result = await collection
        .aggregate([{ $project: { name: 1, price: 1 } }])
        .toArray();

      const passed =
        result.length === 2 &&
        result.every((doc) => doc.name !== undefined && doc.price !== undefined && doc._id !== undefined);
      trackStageResult('$project', passed);
      expect(passed).toBe(true);
    });

    it('should exclude _id when set to 0', async () => {
      const result = await collection
        .aggregate([{ $project: { _id: 0, name: 1, price: 1 } }])
        .toArray();

      const passed = result.every((doc) => doc._id === undefined && doc.name !== undefined);
      trackStageResult('$project', passed);
      expect(passed).toBe(true);
    });

    it('should exclude specified fields', async () => {
      const result = await collection
        .aggregate([{ $project: { cost: 0, metadata: 0 } }])
        .toArray();

      const passed = result.every((doc) => doc.cost === undefined && doc.metadata === undefined && doc.name !== undefined);
      trackStageResult('$project', passed);
      expect(passed).toBe(true);
    });

    it('should include nested objects', async () => {
      const result = await collection
        .aggregate([{ $project: { name: 1, metadata: 1 } }])
        .toArray();

      const passed = result.every((doc) => doc.metadata !== undefined && (doc.metadata as Record<string, unknown>).supplier !== undefined);
      trackStageResult('$project', passed);
      expect(passed).toBe(true);
    });
  });

  // ============================================================================
  // $sort Stage Tests
  // ============================================================================

  describe('$sort Stage', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('sort'));
      await collection.insertMany([
        { _id: '1', name: 'Charlie', score: 75, priority: 2 },
        { _id: '2', name: 'Alice', score: 90, priority: 1 },
        { _id: '3', name: 'Bob', score: 85, priority: 3 },
        { _id: '4', name: 'Diana', score: 95, priority: 1 },
        { _id: '5', name: 'Eve', score: 80, priority: 2 },
      ]);
    });

    it('should sort ascending by number', async () => {
      const result = await collection.aggregate([{ $sort: { score: 1 } }]).toArray();

      let passed = true;
      for (let i = 1; i < result.length; i++) {
        if ((result[i].score as number) < (result[i - 1].score as number)) {
          passed = false;
          break;
        }
      }
      trackStageResult('$sort', passed);
      expect(passed).toBe(true);
    });

    it('should sort descending by number', async () => {
      const result = await collection.aggregate([{ $sort: { score: -1 } }]).toArray();

      let passed = true;
      for (let i = 1; i < result.length; i++) {
        if ((result[i].score as number) > (result[i - 1].score as number)) {
          passed = false;
          break;
        }
      }
      trackStageResult('$sort', passed);
      expect(passed).toBe(true);
    });

    it('should sort alphabetically by string', async () => {
      const result = await collection.aggregate([{ $sort: { name: 1 } }]).toArray();

      const names = result.map((r) => r.name);
      const passed = names[0] === 'Alice' && names[1] === 'Bob' && names[2] === 'Charlie';
      trackStageResult('$sort', passed);
      expect(passed).toBe(true);
    });

    it('should support multi-field sort', async () => {
      const result = await collection
        .aggregate([{ $sort: { priority: 1, score: -1 } }])
        .toArray();

      // Priority 1 items should come first, sorted by score descending
      const priority1 = result.filter((r) => r.priority === 1);
      const passed = priority1[0].name === 'Diana' && priority1[1].name === 'Alice';
      trackStageResult('$sort', passed);
      expect(passed).toBe(true);
    });
  });

  // ============================================================================
  // $limit and $skip Stage Tests
  // ============================================================================

  describe('$limit and $skip Stages', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('limit_skip'));
      const docs = [];
      for (let i = 1; i <= 10; i++) {
        docs.push({ _id: `doc-${i}`, index: i, value: i * 10 });
      }
      await collection.insertMany(docs);
    });

    it('should limit results', async () => {
      const result = await collection.aggregate([{ $limit: 3 }]).toArray();

      const passed = result.length === 3;
      trackStageResult('$limit', passed);
      expect(passed).toBe(true);
    });

    it('should skip documents', async () => {
      const all = await collection.find({}).toArray();
      const result = await collection.aggregate([{ $skip: 5 }]).toArray();

      const passed = result.length === all.length - 5;
      trackStageResult('$skip', passed);
      expect(passed).toBe(true);
    });

    it('should combine skip and limit for pagination', async () => {
      const page2 = await collection
        .aggregate([{ $sort: { index: 1 } }, { $skip: 3 }, { $limit: 3 }])
        .toArray();

      const passed = page2.length === 3 && (page2[0].index as number) === 4 && (page2[2].index as number) === 6;
      trackStageResult('$skip', passed);
      trackStageResult('$limit', passed);
      expect(passed).toBe(true);
    });

    it('should return empty array when skip exceeds count', async () => {
      const result = await collection.aggregate([{ $skip: 100 }]).toArray();

      const passed = result.length === 0;
      trackStageResult('$skip', passed);
      expect(passed).toBe(true);
    });
  });

  // ============================================================================
  // $group Stage Tests
  // ============================================================================

  describe('$group Stage', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('group'));
      await collection.insertMany([
        { _id: '1', category: 'electronics', price: 500, quantity: 10, name: 'Phone' },
        { _id: '2', category: 'electronics', price: 800, quantity: 5, name: 'Laptop' },
        { _id: '3', category: 'clothing', price: 50, quantity: 100, name: 'Shirt' },
        { _id: '4', category: 'clothing', price: 80, quantity: 50, name: 'Pants' },
        { _id: '5', category: 'books', price: 20, quantity: 200, name: 'Novel' },
      ]);
    });

    it('should group by field', async () => {
      const result = await collection
        .aggregate([{ $group: { _id: '$category' } }])
        .toArray();

      const categories = result.map((r) => r._id).sort();
      const passed = categories.length === 3 && categories.includes('electronics') && categories.includes('clothing') && categories.includes('books');
      trackStageResult('$group', passed);
      expect(passed).toBe(true);
    });

    it('should group all with _id: null', async () => {
      const result = await collection
        .aggregate([{ $group: { _id: null, count: { $sum: 1 } } }])
        .toArray();

      const passed = result.length === 1 && result[0]._id === null && result[0].count === 5;
      trackStageResult('$group', passed);
      expect(passed).toBe(true);
    });

    it('should calculate $sum accumulator', async () => {
      const result = await collection
        .aggregate([{ $group: { _id: '$category', total: { $sum: '$quantity' } } }])
        .toArray();

      const electronics = result.find((r) => r._id === 'electronics');
      const passed = electronics !== undefined && electronics.total === 15;
      trackStageResult('$group ($sum)', passed);
      expect(passed).toBe(true);
    });

    it('should calculate $avg accumulator', async () => {
      const result = await collection
        .aggregate([{ $group: { _id: '$category', avgPrice: { $avg: '$price' } } }])
        .toArray();

      const electronics = result.find((r) => r._id === 'electronics');
      const passed = electronics !== undefined && electronics.avgPrice === 650;
      trackStageResult('$group ($avg)', passed);
      expect(passed).toBe(true);
    });

    it('should calculate $min accumulator', async () => {
      const result = await collection
        .aggregate([{ $group: { _id: '$category', minPrice: { $min: '$price' } } }])
        .toArray();

      const electronics = result.find((r) => r._id === 'electronics');
      const passed = electronics !== undefined && electronics.minPrice === 500;
      trackStageResult('$group ($min)', passed);
      expect(passed).toBe(true);
    });

    it('should calculate $max accumulator', async () => {
      const result = await collection
        .aggregate([{ $group: { _id: '$category', maxPrice: { $max: '$price' } } }])
        .toArray();

      const electronics = result.find((r) => r._id === 'electronics');
      const passed = electronics !== undefined && electronics.maxPrice === 800;
      trackStageResult('$group ($max)', passed);
      expect(passed).toBe(true);
    });

    it('should calculate $first accumulator', async () => {
      const result = await collection
        .aggregate([
          { $sort: { price: 1 } },
          { $group: { _id: '$category', firstName: { $first: '$name' } } },
        ])
        .toArray();

      const electronics = result.find((r) => r._id === 'electronics');
      const passed = electronics !== undefined && electronics.firstName === 'Phone';
      trackStageResult('$group ($first)', passed);
      expect(passed).toBe(true);
    });

    it('should calculate $last accumulator', async () => {
      const result = await collection
        .aggregate([
          { $sort: { price: 1 } },
          { $group: { _id: '$category', lastName: { $last: '$name' } } },
        ])
        .toArray();

      const electronics = result.find((r) => r._id === 'electronics');
      const passed = electronics !== undefined && electronics.lastName === 'Laptop';
      trackStageResult('$group ($last)', passed);
      expect(passed).toBe(true);
    });

    it('should calculate $push accumulator', async () => {
      const result = await collection
        .aggregate([{ $group: { _id: '$category', names: { $push: '$name' } } }])
        .toArray();

      const electronics = result.find((r) => r._id === 'electronics');
      const names = electronics?.names as string[];
      const passed = electronics !== undefined && names.length === 2 && names.includes('Phone') && names.includes('Laptop');
      trackStageResult('$group ($push)', passed);
      expect(passed).toBe(true);
    });

    it('should calculate $addToSet accumulator', async () => {
      await collection.insertOne({ _id: '6', category: 'electronics', price: 500, quantity: 3, name: 'Phone' });

      const result = await collection
        .aggregate([{ $group: { _id: '$category', uniqueNames: { $addToSet: '$name' } } }])
        .toArray();

      const electronics = result.find((r) => r._id === 'electronics');
      const uniqueNames = electronics?.uniqueNames as string[];
      // Should have unique values only
      const passed = electronics !== undefined && uniqueNames.length === 2;
      trackStageResult('$group ($addToSet)', passed);
      expect(passed).toBe(true);
    });
  });

  // ============================================================================
  // $unwind Stage Tests
  // ============================================================================

  describe('$unwind Stage', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('unwind'));
      await collection.insertMany([
        { _id: '1', name: 'Product A', tags: ['sale', 'new', 'featured'] },
        { _id: '2', name: 'Product B', tags: ['premium'] },
        { _id: '3', name: 'Product C', tags: [] },
        { _id: '4', name: 'Product D', tags: null },
        { _id: '5', name: 'Product E' },
      ]);
    });

    it('should unwind array field (string syntax)', async () => {
      const result = await collection
        .aggregate([{ $unwind: '$tags' }])
        .toArray();

      const passed = result.length === 4; // 3 from Product A + 1 from Product B
      trackStageResult('$unwind', passed);
      expect(passed).toBe(true);
    });

    it('should expand array into multiple documents', async () => {
      const result = await collection
        .aggregate([{ $match: { _id: '1' } }, { $unwind: '$tags' }])
        .toArray();

      const passed = result.length === 3 && result.every((doc) => typeof doc.tags === 'string');
      trackStageResult('$unwind', passed);
      expect(passed).toBe(true);
    });

    it('should preserve null and empty arrays with option', async () => {
      const result = await collection
        .aggregate([{ $unwind: { path: '$tags', preserveNullAndEmptyArrays: true } }])
        .toArray();

      const passed = result.length === 7; // 3 + 1 + 1 (empty) + 1 (null) + 1 (missing)
      trackStageResult('$unwind', passed, 'preserveNullAndEmptyArrays');
      expect(passed).toBe(true);
    });
  });

  // ============================================================================
  // $lookup Stage Tests
  // ============================================================================

  describe('$lookup Stage', () => {
    let orders: Collection<Document>;
    let products: Collection<Document>;

    beforeEach(async () => {
      const suffix = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
      orders = db.collection(`orders_${suffix}`);
      products = db.collection(`products_${suffix}`);

      await products.insertMany([
        { _id: 'prod1', name: 'Phone', price: 500 },
        { _id: 'prod2', name: 'Laptop', price: 1000 },
        { _id: 'prod3', name: 'Tablet', price: 300 },
      ]);

      await orders.insertMany([
        { _id: 'order1', productId: 'prod1', quantity: 2 },
        { _id: 'order2', productId: 'prod2', quantity: 1 },
        { _id: 'order3', productId: 'prod1', quantity: 5 },
        { _id: 'order4', productId: 'prod99', quantity: 1 }, // No matching product
      ]);
    });

    it('should join with equality match', async () => {
      const result = await orders
        .aggregate([
          {
            $lookup: {
              from: products.collectionName,
              localField: 'productId',
              foreignField: '_id',
              as: 'product',
            },
          },
        ])
        .toArray();

      const order1 = result.find((r) => r._id === 'order1');
      const passed =
        result.length === 4 &&
        order1 !== undefined &&
        Array.isArray(order1.product) &&
        order1.product.length === 1 &&
        order1.product[0].name === 'Phone';
      trackStageResult('$lookup', passed);
      expect(passed).toBe(true);
    });

    it('should return empty array when no match', async () => {
      const result = await orders
        .aggregate([
          { $match: { _id: 'order4' } },
          {
            $lookup: {
              from: products.collectionName,
              localField: 'productId',
              foreignField: '_id',
              as: 'product',
            },
          },
        ])
        .toArray();

      const passed = result.length === 1 && Array.isArray(result[0].product) && result[0].product.length === 0;
      trackStageResult('$lookup', passed);
      expect(passed).toBe(true);
    });
  });

  // ============================================================================
  // $facet Stage Tests
  // ============================================================================

  describe('$facet Stage', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('facet'));
      await collection.insertMany([
        { _id: '1', category: 'electronics', price: 500, rating: 4.5 },
        { _id: '2', category: 'electronics', price: 800, rating: 4.8 },
        { _id: '3', category: 'clothing', price: 50, rating: 4.0 },
        { _id: '4', category: 'clothing', price: 80, rating: 3.5 },
        { _id: '5', category: 'books', price: 20, rating: 4.9 },
      ]);
    });

    it('should run multiple pipelines in parallel', async () => {
      const result = await collection
        .aggregate([
          {
            $facet: {
              byCategory: [{ $group: { _id: '$category' } }],
              topRated: [{ $match: { rating: { $gte: 4.5 } } }, { $sort: { rating: -1 } }],
              priceStats: [{ $group: { _id: null, avgPrice: { $avg: '$price' } } }],
            },
          },
        ])
        .toArray();

      const passed =
        result.length === 1 &&
        Array.isArray(result[0].byCategory) &&
        result[0].byCategory.length === 3 &&
        Array.isArray(result[0].topRated) &&
        result[0].topRated.length === 3 &&
        Array.isArray(result[0].priceStats) &&
        result[0].priceStats.length === 1;
      trackStageResult('$facet', passed);
      expect(passed).toBe(true);
    });

    it('should return independent results for each facet', async () => {
      const result = await collection
        .aggregate([
          {
            $facet: {
              electronics: [{ $match: { category: 'electronics' } }],
              cheap: [{ $match: { price: { $lt: 100 } } }],
            },
          },
        ])
        .toArray();

      // cheap should have 3 items: clothing (50), clothing (80), books (20)
      // electronics has prices 500 and 800 which are >= 100
      const passed =
        result.length === 1 &&
        result[0].electronics.length === 2 &&
        result[0].cheap.length === 3;
      trackStageResult('$facet', passed);
      expect(passed).toBe(true);
    });
  });

  // ============================================================================
  // $bucket Stage Tests
  // ============================================================================

  describe('$bucket Stage', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('bucket'));
      await collection.insertMany([
        { _id: '1', price: 5 },
        { _id: '2', price: 15 },
        { _id: '3', price: 25 },
        { _id: '4', price: 35 },
        { _id: '5', price: 55 },
        { _id: '6', price: 75 },
        { _id: '7', price: 150 },
      ]);
    });

    it('should bucket documents by boundaries', async () => {
      const result = await collection
        .aggregate([
          {
            $bucket: {
              groupBy: '$price',
              boundaries: [0, 25, 50, 100],
              default: 'Other',
            },
          },
        ])
        .toArray();

      // Should have buckets: 0-25 (2 docs), 25-50 (2 docs), 50-100 (2 docs), Other (1 doc)
      const passed = result.length >= 3;
      trackStageResult('$bucket', passed);
      expect(passed).toBe(true);
    });

    it('should support output accumulators', async () => {
      const result = await collection
        .aggregate([
          {
            $bucket: {
              groupBy: '$price',
              boundaries: [0, 50, 100],
              default: 'Other',
              output: {
                count: { $sum: 1 },
                avgPrice: { $avg: '$price' },
              },
            },
          },
        ])
        .toArray();

      const bucket0to50 = result.find((r) => r._id === 0);
      const passed = bucket0to50 !== undefined && bucket0to50.count === 4;
      trackStageResult('$bucket', passed);
      expect(passed).toBe(true);
    });

    it('should place out-of-range docs in default bucket', async () => {
      const result = await collection
        .aggregate([
          {
            $bucket: {
              groupBy: '$price',
              boundaries: [0, 50, 100],
              default: 'Other',
            },
          },
        ])
        .toArray();

      const otherBucket = result.find((r) => r._id === 'Other');
      const passed = otherBucket !== undefined && otherBucket.count === 1;
      trackStageResult('$bucket', passed);
      expect(passed).toBe(true);
    });
  });

  // ============================================================================
  // $count Stage Tests
  // ============================================================================

  describe('$count Stage', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('count'));
      await collection.insertMany([
        { _id: '1', status: 'active' },
        { _id: '2', status: 'active' },
        { _id: '3', status: 'inactive' },
        { _id: '4', status: 'active' },
        { _id: '5', status: 'inactive' },
      ]);
    });

    it('should count all documents', async () => {
      const result = await collection
        .aggregate([{ $count: 'totalCount' }])
        .toArray();

      const passed = result.length === 1 && result[0].totalCount === 5;
      trackStageResult('$count', passed);
      expect(passed).toBe(true);
    });

    it('should count after $match', async () => {
      const result = await collection
        .aggregate([{ $match: { status: 'active' } }, { $count: 'activeCount' }])
        .toArray();

      const passed = result.length === 1 && result[0].activeCount === 3;
      trackStageResult('$count', passed);
      expect(passed).toBe(true);
    });
  });

  // ============================================================================
  // $addFields and $set Stage Tests
  // ============================================================================

  describe('$addFields and $set Stages', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('addfields'));
      await collection.insertMany([
        { _id: '1', price: 100, quantity: 5 },
        { _id: '2', price: 200, quantity: 3 },
      ]);
    });

    it('should add new fields with $addFields', async () => {
      const result = await collection
        .aggregate([{ $addFields: { newField: 'added' } }])
        .toArray();

      const passed = result.every((doc) => doc.newField === 'added');
      trackStageResult('$addFields', passed);
      expect(passed).toBe(true);
    });

    it('should add field from existing field with $addFields', async () => {
      const result = await collection
        .aggregate([{ $addFields: { priceCopy: '$price' } }])
        .toArray();

      const passed = result.every((doc) => doc.priceCopy === doc.price);
      trackStageResult('$addFields', passed);
      expect(passed).toBe(true);
    });

    it('should add new fields with $set (alias)', async () => {
      const result = await collection
        .aggregate([{ $set: { newField: 'set' } }])
        .toArray();

      const passed = result.every((doc) => doc.newField === 'set');
      trackStageResult('$set', passed);
      expect(passed).toBe(true);
    });
  });

  // ============================================================================
  // $unset Stage Tests
  // ============================================================================

  describe('$unset Stage', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('unset'));
      await collection.insertMany([
        { _id: '1', name: 'A', price: 100, secret: 'hidden' },
        { _id: '2', name: 'B', price: 200, secret: 'hidden' },
      ]);
    });

    it('should remove single field', async () => {
      const result = await collection
        .aggregate([{ $unset: 'secret' }])
        .toArray();

      const passed = result.every((doc) => doc.secret === undefined && doc.name !== undefined);
      trackStageResult('$unset', passed);
      expect(passed).toBe(true);
    });

    it('should remove multiple fields', async () => {
      const result = await collection
        .aggregate([{ $unset: ['secret', 'price'] }])
        .toArray();

      const passed = result.every((doc) => doc.secret === undefined && doc.price === undefined && doc.name !== undefined);
      trackStageResult('$unset', passed);
      expect(passed).toBe(true);
    });
  });

  // ============================================================================
  // Combined Pipeline Tests
  // ============================================================================

  describe('Combined Pipeline Operations', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('combined'));
      await collection.insertMany([
        { _id: '1', region: 'north', product: 'A', sales: 100 },
        { _id: '2', region: 'north', product: 'B', sales: 150 },
        { _id: '3', region: 'south', product: 'A', sales: 200 },
        { _id: '4', region: 'south', product: 'B', sales: 250 },
        { _id: '5', region: 'east', product: 'A', sales: 80 },
        { _id: '6', region: 'east', product: 'B', sales: 120 },
        { _id: '7', region: 'west', product: 'A', sales: 300 },
        { _id: '8', region: 'west', product: 'B', sales: 350 },
      ]);
    });

    it('should execute $match -> $group -> $sort pipeline', async () => {
      const result = await collection
        .aggregate([
          { $match: { product: 'A' } },
          { $group: { _id: '$region', totalSales: { $sum: '$sales' } } },
          { $sort: { totalSales: -1 } },
        ])
        .toArray();

      const passed =
        result.length === 4 &&
        result[0]._id === 'west' &&
        result[0].totalSales === 300;
      trackStageResult('combined', passed, 'match->group->sort');
      expect(passed).toBe(true);
    });

    it('should execute $match -> $group -> $sort -> $limit pipeline', async () => {
      const result = await collection
        .aggregate([
          { $match: { sales: { $gte: 100 } } },
          { $group: { _id: '$product', totalSales: { $sum: '$sales' } } },
          { $sort: { totalSales: -1 } },
          { $limit: 1 },
        ])
        .toArray();

      const passed = result.length === 1 && result[0]._id === 'B';
      trackStageResult('combined', passed, 'match->group->sort->limit');
      expect(passed).toBe(true);
    });

    it('should handle multiple $match stages', async () => {
      const result = await collection
        .aggregate([
          { $match: { sales: { $gte: 100 } } },
          { $match: { sales: { $lte: 200 } } },
        ])
        .toArray();

      const passed = result.length === 4 && result.every((doc) => (doc.sales as number) >= 100 && (doc.sales as number) <= 200);
      trackStageResult('combined', passed, 'multiple-match');
      expect(passed).toBe(true);
    });
  });

  // ============================================================================
  // MongoDB Spec Test Cases
  // ============================================================================

  describe('MongoDB Specification Tests', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('spec'));
      await collection.insertMany([
        { _id: 1, x: 11 },
        { _id: 2, x: 22 },
        { _id: 3, x: 33 },
        { _id: 4, x: 44 },
        { _id: 5, x: 55 },
        { _id: 6, x: 66 },
      ]);
    });

    it('aggregate with $match and $gt filter', async () => {
      const result = await collection
        .aggregate([{ $match: { _id: { $gt: 1 } } }])
        .toArray();

      const passed =
        result.length === 5 &&
        result.every((doc) => (doc._id as number) > 1);
      trackStageResult('spec:$match', passed);
      expect(passed).toBe(true);
    });

    it('aggregate with multiple batches works', async () => {
      const result = await collection
        .aggregate([{ $match: { _id: { $gt: 1 } } }])
        .toArray();

      const expected = [
        { _id: 2, x: 22 },
        { _id: 3, x: 33 },
        { _id: 4, x: 44 },
        { _id: 5, x: 55 },
        { _id: 6, x: 66 },
      ];

      const passed = result.length === expected.length;
      trackStageResult('spec:batching', passed);
      expect(passed).toBe(true);
    });
  });
});
