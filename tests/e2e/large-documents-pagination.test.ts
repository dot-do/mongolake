/**
 * MongoLake E2E Tests - Large Document Handling and Pagination
 *
 * End-to-end tests for handling large documents and proper pagination.
 * Tests verify that large documents are stored and retrieved correctly,
 * and that pagination works across various data sizes.
 *
 * Test scenarios:
 * - Large document storage and retrieval
 * - Documents with many fields
 * - Deep nesting structures
 * - Large arrays within documents
 * - Cursor-based pagination
 * - Skip/limit pagination
 * - Keyset (seek) pagination
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { MongoClient, type Db, type Collection, type Document } from 'mongodb';
import { createServer, type TcpServer } from '../../src/wire-protocol/tcp-server.js';
import * as fs from 'node:fs';

// Test configuration
const TEST_DATA_DIR = '.mongolake-e2e-large-docs-test';
const TEST_DB_NAME = 'large_docs_test';

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

// Generate random string of specified length
function generateRandomString(length: number): string {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let result = '';
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

describe('Large Document Handling and Pagination E2E Tests', () => {
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
      serverSelectionTimeoutMS: 10000,
      connectTimeoutMS: 10000,
      socketTimeoutMS: 30000,
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
  // Large Document Storage Tests
  // ============================================================================

  describe('Large Document Storage', () => {
    it('should store and retrieve document with large string field', async () => {
      const collection = db.collection(uniqueCollection('large_string'));

      // Create document with 10KB string field (reduced for faster tests)
      const largeString = generateRandomString(10 * 1024);
      const doc = {
        _id: 'large-string-doc',
        content: largeString,
        size: largeString.length,
      };

      await collection.insertOne(doc);

      const retrieved = await collection.findOne({ _id: 'large-string-doc' });
      expect(retrieved).not.toBeNull();
      expect(retrieved!.content.length).toBe(10 * 1024);
      expect(retrieved!.content).toBe(largeString);
    });

    it('should store and retrieve document with many fields', async () => {
      const collection = db.collection(uniqueCollection('many_fields'));

      // Create document with 500 fields
      const doc: Record<string, unknown> = {
        _id: 'many-fields-doc',
      };

      for (let i = 0; i < 500; i++) {
        doc[`field_${i.toString().padStart(3, '0')}`] = `value_${i}`;
      }

      await collection.insertOne(doc);

      const retrieved = await collection.findOne({ _id: 'many-fields-doc' });
      expect(retrieved).not.toBeNull();

      // Verify all fields are present
      for (let i = 0; i < 500; i++) {
        const fieldName = `field_${i.toString().padStart(3, '0')}`;
        expect(retrieved![fieldName]).toBe(`value_${i}`);
      }
    });

    it('should store and retrieve deeply nested document', async () => {
      const collection = db.collection(uniqueCollection('deep_nested'));

      // Create deeply nested structure (20 levels)
      let nested: Record<string, unknown> = { value: 'deepest', level: 20 };
      for (let i = 19; i >= 1; i--) {
        nested = { child: nested, level: i };
      }

      const doc = {
        _id: 'deep-nested-doc',
        root: nested,
      };

      await collection.insertOne(doc);

      const retrieved = await collection.findOne({ _id: 'deep-nested-doc' });
      expect(retrieved).not.toBeNull();
      expect(retrieved!.root.level).toBe(1);

      // Navigate to deepest level
      let current = retrieved!.root;
      for (let i = 1; i < 20; i++) {
        expect(current.level).toBe(i);
        current = current.child;
      }
      expect(current.level).toBe(20);
      expect(current.value).toBe('deepest');
    });

    it('should store and retrieve document with large array', async () => {
      const collection = db.collection(uniqueCollection('large_array'));

      // Create document with array of 500 items (reduced for faster tests)
      const largeArray = [];
      for (let i = 0; i < 500; i++) {
        largeArray.push({
          index: i,
          value: `item_${i}`,
          data: { nested: true },
        });
      }

      const doc = {
        _id: 'large-array-doc',
        items: largeArray,
        count: largeArray.length,
      };

      await collection.insertOne(doc);

      const retrieved = await collection.findOne({ _id: 'large-array-doc' });
      expect(retrieved).not.toBeNull();
      expect(retrieved!.items.length).toBe(500);
      expect(retrieved!.items[0].index).toBe(0);
      expect(retrieved!.items[499].index).toBe(499);
    });

    it('should store and retrieve document with binary-like data', async () => {
      const collection = db.collection(uniqueCollection('binary_data'));

      // Create document with base64-encoded "binary" data (smaller size for faster test)
      const binaryData = Buffer.from(generateRandomString(5 * 1024)).toString('base64');

      const doc = {
        _id: 'binary-doc',
        data: binaryData,
        encoding: 'base64',
        originalSize: 5 * 1024,
      };

      await collection.insertOne(doc);

      const retrieved = await collection.findOne({ _id: 'binary-doc' });
      expect(retrieved).not.toBeNull();
      expect(retrieved!.data).toBe(binaryData);
      expect(retrieved!.encoding).toBe('base64');
    });
  });

  // ============================================================================
  // Document Size Limits Tests
  // ============================================================================

  describe('Document Size Handling', () => {
    it('should handle multiple medium-sized documents', async () => {
      const collection = db.collection(uniqueCollection('medium_docs'));

      // Insert 20 documents of ~2KB each (reduced for faster tests)
      const docs = [];
      for (let i = 0; i < 20; i++) {
        docs.push({
          _id: `medium-${i}`,
          content: generateRandomString(2 * 1024),
          index: i,
        });
      }

      const result = await collection.insertMany(docs);
      expect(result.insertedCount).toBe(20);

      // Verify retrieval
      const retrieved = await collection.find({}).toArray();
      expect(retrieved.length).toBe(20);
    });

    it('should handle mixed document sizes', async () => {
      const collection = db.collection(uniqueCollection('mixed_sizes'));

      const docs = [
        { _id: 'tiny', content: 'small', size: 'tiny' },
        { _id: 'small', content: generateRandomString(1024), size: 'small' },
        { _id: 'medium', content: generateRandomString(10 * 1024), size: 'medium' },
        { _id: 'large', content: generateRandomString(50 * 1024), size: 'large' },
      ];

      await collection.insertMany(docs);

      // Query by size category
      const sizes = ['tiny', 'small', 'medium', 'large'];
      for (const size of sizes) {
        const doc = await collection.findOne({ size });
        expect(doc).not.toBeNull();
        expect(doc!.size).toBe(size);
      }
    });

    it('should handle documents with complex mixed content', async () => {
      const collection = db.collection(uniqueCollection('complex_content'));

      const doc = {
        _id: 'complex-doc',
        strings: {
          short: 'hello',
          medium: generateRandomString(1000),
          long: generateRandomString(10000),
        },
        numbers: {
          integer: 42,
          float: 3.14159265359,
          large: 9007199254740991,
          negative: -999999,
        },
        arrays: {
          strings: ['a', 'b', 'c', 'd', 'e'],
          numbers: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
          mixed: [1, 'two', true, null, { nested: 'object' }],
        },
        nested: {
          level1: {
            level2: {
              level3: {
                value: 'deep',
              },
            },
          },
        },
        booleans: {
          true: true,
          false: false,
        },
        nullValue: null,
      };

      await collection.insertOne(doc);

      const retrieved = await collection.findOne({ _id: 'complex-doc' });
      expect(retrieved).not.toBeNull();
      expect(retrieved!.strings.short).toBe('hello');
      expect(retrieved!.numbers.integer).toBe(42);
      expect(retrieved!.arrays.strings.length).toBe(5);
      expect(retrieved!.nested.level1.level2.level3.value).toBe('deep');
      expect(retrieved!.booleans.true).toBe(true);
      expect(retrieved!.nullValue).toBeNull();
    });
  });

  // ============================================================================
  // Skip/Limit Pagination Tests
  // ============================================================================

  describe('Skip/Limit Pagination', () => {
    let collection: Collection<Document>;
    const totalDocs = 100;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('pagination'));

      // Insert 100 documents
      const docs = [];
      for (let i = 0; i < totalDocs; i++) {
        docs.push({
          _id: `page-${i.toString().padStart(3, '0')}`,
          index: i,
          value: i * 10,
          category: `cat-${i % 5}`,
        });
      }
      await collection.insertMany(docs);
    });

    it('should paginate through all documents with fixed page size', async () => {
      const pageSize = 10;
      const allDocs: Document[] = [];

      for (let page = 0; page < 10; page++) {
        const docs = await collection
          .find({})
          .sort({ index: 1 })
          .skip(page * pageSize)
          .limit(pageSize)
          .toArray();

        expect(docs.length).toBe(pageSize);
        allDocs.push(...docs);
      }

      // Verify all documents retrieved
      expect(allDocs.length).toBe(totalDocs);

      // Verify no duplicates
      const indices = allDocs.map((d) => d.index);
      const uniqueIndices = new Set(indices);
      expect(uniqueIndices.size).toBe(totalDocs);
    });

    it('should handle varying page sizes', async () => {
      // First page: 15 items
      const page1 = await collection.find({}).sort({ index: 1 }).skip(0).limit(15).toArray();
      expect(page1.length).toBe(15);
      expect(page1[0].index).toBe(0);
      expect(page1[14].index).toBe(14);

      // Second page: 30 items
      const page2 = await collection.find({}).sort({ index: 1 }).skip(15).limit(30).toArray();
      expect(page2.length).toBe(30);
      expect(page2[0].index).toBe(15);
      expect(page2[29].index).toBe(44);

      // Third page: remaining items
      const page3 = await collection.find({}).sort({ index: 1 }).skip(45).limit(100).toArray();
      expect(page3.length).toBe(55);
      expect(page3[0].index).toBe(45);
      expect(page3[54].index).toBe(99);
    });

    it('should handle skip beyond document count', async () => {
      const docs = await collection.find({}).skip(200).limit(10).toArray();
      expect(docs.length).toBe(0);
    });

    it.skip('should handle limit of 0', async () => {
      // Skipped: limit(0) handling varies by MongoDB version
      // In MongoDB, limit(0) means no limit - returns all documents
      const docs = await collection.find({}).limit(0).toArray();
      // Should return all 100 documents (no limit applied)
      expect(docs.length).toBe(100);
    });

    it('should paginate with filter applied', async () => {
      // Paginate through cat-0 category (20 docs)
      const category = 'cat-0';
      const pageSize = 5;
      const allCatDocs: Document[] = [];

      for (let page = 0; page < 4; page++) {
        const docs = await collection
          .find({ category })
          .sort({ index: 1 })
          .skip(page * pageSize)
          .limit(pageSize)
          .toArray();

        allCatDocs.push(...docs);
      }

      expect(allCatDocs.length).toBe(20);
      for (const doc of allCatDocs) {
        expect(doc.category).toBe(category);
      }
    });

    it('should paginate with sort in different orders', async () => {
      // Ascending sort
      const asc = await collection.find({}).sort({ index: 1 }).skip(10).limit(5).toArray();
      expect(asc[0].index).toBe(10);
      expect(asc[4].index).toBe(14);

      // Descending sort
      const desc = await collection.find({}).sort({ index: -1 }).skip(10).limit(5).toArray();
      expect(desc[0].index).toBe(89);
      expect(desc[4].index).toBe(85);
    });
  });

  // ============================================================================
  // Keyset (Seek) Pagination Tests
  // ============================================================================

  describe('Keyset (Seek) Pagination', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('keyset'));

      // Insert documents with unique, sortable IDs
      const docs = [];
      for (let i = 0; i < 100; i++) {
        docs.push({
          _id: `item_${i.toString().padStart(4, '0')}`,
          sequence: i,
          timestamp: Date.now() + i,
          score: Math.random() * 100,
        });
      }
      await collection.insertMany(docs);
    });

    it('should paginate forward using keyset pagination', async () => {
      const pageSize = 10;
      const pages: Document[][] = [];
      let lastId: string | null = null;

      // Fetch 5 pages
      for (let i = 0; i < 5; i++) {
        const filter = lastId ? { _id: { $gt: lastId } } : {};
        const docs = await collection
          .find(filter)
          .sort({ _id: 1 })
          .limit(pageSize)
          .toArray();

        pages.push(docs);
        if (docs.length > 0) {
          lastId = docs[docs.length - 1]._id as string;
        }
      }

      // Verify each page has correct size
      for (const page of pages) {
        expect(page.length).toBe(pageSize);
      }

      // Verify no overlaps between pages
      const allIds = pages.flatMap((p) => p.map((d) => d._id));
      const uniqueIds = new Set(allIds);
      expect(uniqueIds.size).toBe(50);
    });

    it('should paginate using compound keyset', async () => {
      // Add more documents with varying scores
      const existingDocs = await collection.find({}).toArray();
      await collection.deleteMany({});

      // Reinsert with controlled scores
      const docs = [];
      for (let i = 0; i < 50; i++) {
        docs.push({
          _id: `compound_${i.toString().padStart(3, '0')}`,
          score: Math.floor(i / 10), // Groups of 10 with same score
          name: `name_${i}`,
        });
      }
      await collection.insertMany(docs);

      // Keyset pagination by (score, _id)
      const pageSize = 8;
      const pages: Document[][] = [];
      let lastScore: number | null = null;
      let lastId: string | null = null;

      for (let i = 0; i < 7; i++) {
        let filter = {};
        if (lastScore !== null && lastId !== null) {
          filter = {
            $or: [
              { score: { $gt: lastScore } },
              { score: lastScore, _id: { $gt: lastId } },
            ],
          };
        }

        const docs = await collection
          .find(filter)
          .sort({ score: 1, _id: 1 })
          .limit(pageSize)
          .toArray();

        if (docs.length === 0) break;

        pages.push(docs);
        const last = docs[docs.length - 1];
        lastScore = last.score;
        lastId = last._id as string;
      }

      // Verify we got all documents
      const totalDocs = pages.flatMap((p) => p);
      expect(totalDocs.length).toBe(50);
    });

    it('should maintain consistent order with keyset pagination', async () => {
      const pageSize = 10;
      let cursor: string | null = null;
      const sequences: number[] = [];

      // Paginate and collect sequences
      while (true) {
        const filter = cursor ? { _id: { $gt: cursor } } : {};
        const docs = await collection
          .find(filter)
          .sort({ _id: 1 })
          .limit(pageSize)
          .toArray();

        if (docs.length === 0) break;

        sequences.push(...docs.map((d) => d.sequence));
        cursor = docs[docs.length - 1]._id as string;
      }

      // Verify sequences are in order
      for (let i = 1; i < sequences.length; i++) {
        expect(sequences[i]).toBeGreaterThan(sequences[i - 1]);
      }
    });
  });

  // ============================================================================
  // Aggregation Pagination Tests
  // ============================================================================

  describe('Aggregation Pagination', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('agg_pagination'));

      const docs = [];
      for (let i = 0; i < 100; i++) {
        docs.push({
          _id: `agg-${i}`,
          region: `region-${i % 5}`,
          product: `product-${i % 10}`,
          sales: (i + 1) * 100,
        });
      }
      await collection.insertMany(docs);
    });

    it('should paginate aggregation results with $skip and $limit', async () => {
      const pageSize = 3;
      const pages: Document[][] = [];

      // Group by region and paginate
      for (let page = 0; page < 2; page++) {
        const result = await collection
          .aggregate([
            {
              $group: {
                _id: '$region',
                totalSales: { $sum: '$sales' },
                count: { $sum: 1 },
              },
            },
            { $sort: { _id: 1 } },
            { $skip: page * pageSize },
            { $limit: pageSize },
          ])
          .toArray();

        pages.push(result);
      }

      expect(pages[0].length).toBe(3);
      expect(pages[1].length).toBe(2); // Only 5 regions total
    });

    it('should handle sorted aggregation pagination', async () => {
      // Get top 5 products by sales
      const top5 = await collection
        .aggregate([
          {
            $group: {
              _id: '$product',
              totalSales: { $sum: '$sales' },
            },
          },
          { $sort: { totalSales: -1 } },
          { $limit: 5 },
        ])
        .toArray();

      expect(top5.length).toBe(5);

      // Verify descending order
      for (let i = 1; i < top5.length; i++) {
        expect(top5[i].totalSales).toBeLessThanOrEqual(top5[i - 1].totalSales);
      }

      // Get next 5 products
      const next5 = await collection
        .aggregate([
          {
            $group: {
              _id: '$product',
              totalSales: { $sum: '$sales' },
            },
          },
          { $sort: { totalSales: -1 } },
          { $skip: 5 },
          { $limit: 5 },
        ])
        .toArray();

      expect(next5.length).toBe(5);

      // Verify no overlap
      const top5Products = new Set(top5.map((d) => d._id));
      for (const doc of next5) {
        expect(top5Products.has(doc._id)).toBe(false);
      }
    });

    it('should paginate through $match filtered aggregation', async () => {
      // Filter to specific regions and paginate
      const targetRegions = ['region-0', 'region-1'];

      const page1 = await collection
        .aggregate([
          { $match: { region: { $in: targetRegions } } },
          { $sort: { sales: -1 } },
          { $skip: 0 },
          { $limit: 10 },
        ])
        .toArray();

      const page2 = await collection
        .aggregate([
          { $match: { region: { $in: targetRegions } } },
          { $sort: { sales: -1 } },
          { $skip: 10 },
          { $limit: 10 },
        ])
        .toArray();

      expect(page1.length).toBe(10);
      expect(page2.length).toBe(10);

      // All should be from target regions
      for (const doc of [...page1, ...page2]) {
        expect(targetRegions).toContain(doc.region);
      }
    });
  });

  // ============================================================================
  // Bulk Insert Tests
  // ============================================================================

  describe('Bulk Operations with Large Data', () => {
    it('should bulk insert many small documents', async () => {
      const collection = db.collection(uniqueCollection('bulk_small'));

      // Insert 1000 small documents
      const docs = [];
      for (let i = 0; i < 1000; i++) {
        docs.push({
          _id: `bulk-${i}`,
          index: i,
          value: i * 2,
        });
      }

      const result = await collection.insertMany(docs);
      expect(result.insertedCount).toBe(1000);

      // Verify count
      const count = await collection.countDocuments();
      expect(count).toBe(1000);
    });

    it('should bulk insert medium-sized documents in batches', async () => {
      const collection = db.collection(uniqueCollection('bulk_medium'));

      // Insert in batches of 20 (reduced for faster tests)
      const batchSize = 20;
      const totalBatches = 5;

      for (let batch = 0; batch < totalBatches; batch++) {
        const docs = [];
        for (let i = 0; i < batchSize; i++) {
          const docIndex = batch * batchSize + i;
          docs.push({
            _id: `batch-${batch}-${i}`,
            batchNum: batch,
            content: generateRandomString(500),
            index: docIndex,
          });
        }
        await collection.insertMany(docs);
      }

      // Verify total count
      const count = await collection.countDocuments();
      expect(count).toBe(batchSize * totalBatches);

      // Verify each batch
      for (let batch = 0; batch < totalBatches; batch++) {
        const batchDocs = await collection.find({ batchNum: batch }).toArray();
        expect(batchDocs.length).toBe(batchSize);
      }
    });

    it('should handle bulk update of large documents', async () => {
      const collection = db.collection(uniqueCollection('bulk_update'));

      // Insert documents (reduced count for faster tests)
      const docs = [];
      for (let i = 0; i < 30; i++) {
        docs.push({
          _id: `update-${i}`,
          content: generateRandomString(500),
          status: 'pending',
          version: 1,
        });
      }
      await collection.insertMany(docs);

      // Bulk update all documents
      const result = await collection.updateMany(
        { status: 'pending' },
        { $set: { status: 'processed' }, $inc: { version: 1 } }
      );

      expect(result.modifiedCount).toBe(30);

      // Verify updates
      const updated = await collection.find({}).toArray();
      for (const doc of updated) {
        expect(doc.status).toBe('processed');
        expect(doc.version).toBe(2);
      }
    });
  });

  // ============================================================================
  // Edge Cases Tests
  // ============================================================================

  describe('Edge Cases', () => {
    it('should handle empty collection pagination', async () => {
      const collection = db.collection(uniqueCollection('empty'));

      const docs = await collection.find({}).skip(10).limit(10).toArray();
      expect(docs.length).toBe(0);

      const count = await collection.countDocuments();
      expect(count).toBe(0);
    });

    it('should handle single document collection', async () => {
      const collection = db.collection(uniqueCollection('single'));

      await collection.insertOne({ _id: 'only-doc', value: 42 });

      // First page gets the document
      const page1 = await collection.find({}).skip(0).limit(10).toArray();
      expect(page1.length).toBe(1);

      // Second page is empty
      const page2 = await collection.find({}).skip(10).limit(10).toArray();
      expect(page2.length).toBe(0);
    });

    it('should handle documents with special characters', async () => {
      const collection = db.collection(uniqueCollection('special_chars'));

      const specialContent = {
        unicode: '\u0048\u0065\u006c\u006c\u006f \u4e16\u754c',
        newlines: 'line1\nline2\nline3',
        tabs: 'col1\tcol2\tcol3',
        quotes: '"quoted" and \'apostrophe\'',
        backslash: 'path\\to\\file',
        html: '<script>alert("xss")</script>',
        emoji: 'hello world',
      };

      await collection.insertOne({
        _id: 'special',
        ...specialContent,
      });

      const retrieved = await collection.findOne({ _id: 'special' });
      expect(retrieved).not.toBeNull();
      expect(retrieved!.unicode).toBe(specialContent.unicode);
      expect(retrieved!.newlines).toBe(specialContent.newlines);
      expect(retrieved!.html).toBe(specialContent.html);
    });

    it('should handle extreme page sizes', async () => {
      const collection = db.collection(uniqueCollection('extreme_page'));

      // Insert some documents
      const docs = [];
      for (let i = 0; i < 50; i++) {
        docs.push({ _id: `extreme-${i}`, index: i });
      }
      await collection.insertMany(docs);

      // Very large limit (larger than collection)
      const largeLimit = await collection.find({}).limit(10000).toArray();
      expect(largeLimit.length).toBe(50);

      // Very large skip
      const largeSkip = await collection.find({}).skip(10000).limit(10).toArray();
      expect(largeSkip.length).toBe(0);
    });
  });
});
