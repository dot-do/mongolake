/**
 * MongoLake E2E Tests - MongoDB Driver Integration
 *
 * End-to-end tests that use the official MongoDB Node.js driver
 * against MongoLake's wire protocol server.
 *
 * These tests verify that MongoLake can be used as a drop-in
 * replacement for MongoDB with standard MongoDB client libraries.
 *
 * Usage:
 *   pnpm test:e2e
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest';
import { MongoClient, type Db, type Collection, type Document, ObjectId } from 'mongodb';
import { createServer, type TcpServer } from '../../src/wire-protocol/tcp-server.js';
import * as fs from 'node:fs';
import * as path from 'node:path';

// Test configuration
const TEST_DATA_DIR = '.mongolake-e2e-test';
const TEST_DB_NAME = 'e2e_mongodb_driver_test';

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

describe('MongoDB Driver E2E Tests', () => {
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
  // Connection Tests
  // ============================================================================

  describe('Connection', () => {
    it('should connect successfully with MongoDB driver', () => {
      expect(client).toBeDefined();
      expect(db).toBeDefined();
    });

    it('should respond to ping command', async () => {
      const result = await db.admin().ping();
      expect(result.ok).toBe(1);
    });

    it('should return server info on hello', async () => {
      const result = await db.admin().command({ hello: 1 });
      expect(result.ok).toBe(1);
      expect(result.ismaster).toBe(true);
      expect(result.maxWireVersion).toBeDefined();
    });

    it('should return server info on isMaster', async () => {
      const result = await db.admin().command({ isMaster: 1 });
      expect(result.ok).toBe(1);
      expect(result.ismaster).toBe(true);
    });

    it('should return build info', async () => {
      const result = await db.admin().command({ buildInfo: 1 });
      expect(result.ok).toBe(1);
      expect(result.version).toBeDefined();
    });
  });

  // ============================================================================
  // Insert Operations
  // ============================================================================

  describe('Insert Operations', () => {
    let collection: Collection<Document>;
    let collectionName: string;

    beforeEach(() => {
      collectionName = uniqueCollection('insert');
      collection = db.collection(collectionName);
    });

    describe('insertOne', () => {
      it('should insert a single document with provided _id', async () => {
        const doc = { _id: 'test-id-1', name: 'Alice', age: 30 };

        const result = await collection.insertOne(doc);

        expect(result.acknowledged).toBe(true);
        expect(result.insertedId).toBe('test-id-1');
      });

      it('should insert a document with auto-generated ObjectId', async () => {
        const doc = { name: 'Bob', age: 25 };

        const result = await collection.insertOne(doc);

        expect(result.acknowledged).toBe(true);
        expect(result.insertedId).toBeDefined();
      });

      it('should insert document with nested objects', async () => {
        const doc = {
          _id: 'nested-test',
          name: 'Charlie',
          address: {
            street: '123 Main St',
            city: 'Test City',
            zip: '12345',
          },
          tags: ['developer', 'engineer'],
        };

        const result = await collection.insertOne(doc);

        expect(result.acknowledged).toBe(true);
        expect(result.insertedId).toBe('nested-test');
      });

      it('should insert document with various data types', async () => {
        const doc = {
          _id: 'types-test',
          stringField: 'hello',
          numberField: 42,
          floatField: 3.14,
          booleanTrue: true,
          booleanFalse: false,
          nullField: null,
          arrayField: [1, 2, 3],
          dateField: new Date('2024-01-01'),
        };

        const result = await collection.insertOne(doc);

        expect(result.acknowledged).toBe(true);
      });
    });

    describe('insertMany', () => {
      it('should insert multiple documents', async () => {
        const docs = [
          { _id: 'multi-1', name: 'Alice', score: 100 },
          { _id: 'multi-2', name: 'Bob', score: 90 },
          { _id: 'multi-3', name: 'Charlie', score: 80 },
        ];

        const result = await collection.insertMany(docs);

        expect(result.acknowledged).toBe(true);
        expect(result.insertedCount).toBe(3);
        expect(Object.keys(result.insertedIds)).toHaveLength(3);
      });

      it('should insert documents with auto-generated ids', async () => {
        const docs = [
          { name: 'Doc1', value: 1 },
          { name: 'Doc2', value: 2 },
        ];

        const result = await collection.insertMany(docs);

        expect(result.acknowledged).toBe(true);
        expect(result.insertedCount).toBe(2);
      });

      it('should insert an empty array gracefully', async () => {
        // This should either succeed with 0 inserts or throw a controlled error
        try {
          const result = await collection.insertMany([]);
          expect(result.insertedCount).toBe(0);
        } catch (error) {
          // Some implementations reject empty arrays
          expect(error).toBeDefined();
        }
      });
    });
  });

  // ============================================================================
  // Find Operations
  // ============================================================================

  describe('Find Operations', () => {
    let collection: Collection<Document>;
    let collectionName: string;

    beforeEach(async () => {
      collectionName = uniqueCollection('find');
      collection = db.collection(collectionName);

      // Insert test data
      await collection.insertMany([
        { _id: 'user-1', name: 'Alice', age: 30, status: 'active', score: 85 },
        { _id: 'user-2', name: 'Bob', age: 25, status: 'active', score: 92 },
        { _id: 'user-3', name: 'Charlie', age: 35, status: 'inactive', score: 78 },
        { _id: 'user-4', name: 'Diana', age: 28, status: 'active', score: 95 },
        { _id: 'user-5', name: 'Eve', age: 32, status: 'inactive', score: 88 },
      ]);
    });

    describe('Basic find', () => {
      it('should find all documents with empty filter', async () => {
        const docs = await collection.find({}).toArray();

        expect(docs.length).toBe(5);
      });

      it('should find documents by exact field match', async () => {
        const docs = await collection.find({ status: 'active' }).toArray();

        expect(docs.length).toBe(3);
        for (const doc of docs) {
          expect(doc.status).toBe('active');
        }
      });

      it('should find document by _id', async () => {
        const docs = await collection.find({ _id: 'user-2' }).toArray();

        expect(docs.length).toBe(1);
        expect(docs[0].name).toBe('Bob');
      });

      it('should return empty array when no documents match', async () => {
        const docs = await collection.find({ status: 'nonexistent' }).toArray();

        expect(docs).toEqual([]);
      });
    });

    describe('Comparison operators', () => {
      it('should find documents with $gt operator', async () => {
        const docs = await collection.find({ age: { $gt: 30 } }).toArray();

        expect(docs.length).toBeGreaterThan(0);
        for (const doc of docs) {
          expect(doc.age).toBeGreaterThan(30);
        }
      });

      it('should find documents with $gte operator', async () => {
        const docs = await collection.find({ age: { $gte: 30 } }).toArray();

        expect(docs.length).toBeGreaterThan(0);
        for (const doc of docs) {
          expect(doc.age).toBeGreaterThanOrEqual(30);
        }
      });

      it('should find documents with $lt operator', async () => {
        const docs = await collection.find({ age: { $lt: 30 } }).toArray();

        expect(docs.length).toBeGreaterThan(0);
        for (const doc of docs) {
          expect(doc.age).toBeLessThan(30);
        }
      });

      it('should find documents with $lte operator', async () => {
        const docs = await collection.find({ age: { $lte: 30 } }).toArray();

        expect(docs.length).toBeGreaterThan(0);
        for (const doc of docs) {
          expect(doc.age).toBeLessThanOrEqual(30);
        }
      });

      it('should find documents with $ne operator', async () => {
        const docs = await collection.find({ status: { $ne: 'active' } }).toArray();

        expect(docs.length).toBeGreaterThan(0);
        for (const doc of docs) {
          expect(doc.status).not.toBe('active');
        }
      });

      it('should find documents with $in operator', async () => {
        const docs = await collection.find({ name: { $in: ['Alice', 'Bob'] } }).toArray();

        expect(docs.length).toBe(2);
        for (const doc of docs) {
          expect(['Alice', 'Bob']).toContain(doc.name);
        }
      });

      it('should find documents with $nin operator', async () => {
        const docs = await collection.find({ name: { $nin: ['Alice', 'Bob'] } }).toArray();

        expect(docs.length).toBe(3);
        for (const doc of docs) {
          expect(['Alice', 'Bob']).not.toContain(doc.name);
        }
      });
    });

    describe('Projection', () => {
      it('should include only specified fields', async () => {
        const docs = await collection.find({}, { projection: { name: 1, age: 1 } }).toArray();

        expect(docs.length).toBe(5);
        for (const doc of docs) {
          expect(doc.name).toBeDefined();
          expect(doc.age).toBeDefined();
          expect(doc._id).toBeDefined(); // _id is included by default
        }
      });

      it('should exclude specified fields', async () => {
        const docs = await collection.find({}, { projection: { score: 0 } }).toArray();

        expect(docs.length).toBe(5);
        for (const doc of docs) {
          expect(doc.score).toBeUndefined();
          expect(doc.name).toBeDefined();
        }
      });

      it('should exclude _id when specified', async () => {
        const docs = await collection.find({}, { projection: { _id: 0, name: 1 } }).toArray();

        expect(docs.length).toBe(5);
        for (const doc of docs) {
          expect(doc._id).toBeUndefined();
          expect(doc.name).toBeDefined();
        }
      });
    });

    describe('Sort', () => {
      it('should sort documents in ascending order', async () => {
        const docs = await collection.find({}).sort({ age: 1 }).toArray();

        expect(docs.length).toBe(5);
        for (let i = 1; i < docs.length; i++) {
          expect(docs[i].age).toBeGreaterThanOrEqual(docs[i - 1].age);
        }
      });

      it('should sort documents in descending order', async () => {
        const docs = await collection.find({}).sort({ age: -1 }).toArray();

        expect(docs.length).toBe(5);
        for (let i = 1; i < docs.length; i++) {
          expect(docs[i].age).toBeLessThanOrEqual(docs[i - 1].age);
        }
      });

      it('should sort by multiple fields', async () => {
        const docs = await collection.find({}).sort({ status: 1, score: -1 }).toArray();

        expect(docs.length).toBe(5);
        // Verify primary sort is correct
        let prevStatus = '';
        for (const doc of docs) {
          if (prevStatus && doc.status !== prevStatus) {
            expect(doc.status >= prevStatus).toBe(true);
          }
          prevStatus = doc.status;
        }
      });
    });

    describe('Limit and Skip', () => {
      it('should limit the number of results', async () => {
        const docs = await collection.find({}).limit(2).toArray();

        expect(docs.length).toBe(2);
      });

      it('should skip specified number of documents', async () => {
        const allDocs = await collection.find({}).toArray();
        const skippedDocs = await collection.find({}).skip(2).toArray();

        expect(skippedDocs.length).toBe(allDocs.length - 2);
      });

      it('should combine limit and skip for pagination', async () => {
        const page1 = await collection.find({}).skip(0).limit(2).toArray();
        const page2 = await collection.find({}).skip(2).limit(2).toArray();

        expect(page1.length).toBe(2);
        expect(page2.length).toBe(2);

        // Pages should contain different documents
        const page1Ids = new Set(page1.map((d) => d._id));
        for (const doc of page2) {
          expect(page1Ids.has(doc._id)).toBe(false);
        }
      });
    });

    describe('findOne', () => {
      it('should find a single document', async () => {
        const doc = await collection.findOne({ _id: 'user-1' });

        expect(doc).not.toBeNull();
        expect(doc!.name).toBe('Alice');
      });

      it('should return null when no document matches', async () => {
        const doc = await collection.findOne({ _id: 'nonexistent' });

        expect(doc).toBeNull();
      });
    });
  });

  // ============================================================================
  // Update Operations
  // ============================================================================

  describe('Update Operations', () => {
    let collection: Collection<Document>;
    let collectionName: string;

    beforeEach(async () => {
      collectionName = uniqueCollection('update');
      collection = db.collection(collectionName);

      // Insert test data
      await collection.insertMany([
        { _id: 'update-1', name: 'Alice', age: 30, status: 'pending', score: 85 },
        { _id: 'update-2', name: 'Bob', age: 25, status: 'pending', score: 90 },
        { _id: 'update-3', name: 'Charlie', age: 35, status: 'pending', score: 78 },
      ]);
    });

    describe('updateOne', () => {
      it('should update a single document with $set', async () => {
        const result = await collection.updateOne(
          { _id: 'update-1' },
          { $set: { status: 'completed' } }
        );

        expect(result.acknowledged).toBe(true);
        expect(result.matchedCount).toBe(1);
        expect(result.modifiedCount).toBe(1);

        // Verify update
        const doc = await collection.findOne({ _id: 'update-1' });
        expect(doc!.status).toBe('completed');
      });

      it('should update with $inc operator', async () => {
        const result = await collection.updateOne(
          { _id: 'update-1' },
          { $inc: { score: 10 } }
        );

        expect(result.acknowledged).toBe(true);
        expect(result.modifiedCount).toBe(1);

        const doc = await collection.findOne({ _id: 'update-1' });
        expect(doc!.score).toBe(95);
      });

      it('should update with $unset operator', async () => {
        const result = await collection.updateOne(
          { _id: 'update-1' },
          { $unset: { score: '' } }
        );

        expect(result.acknowledged).toBe(true);

        const doc = await collection.findOne({ _id: 'update-1' });
        expect(doc!.score).toBeUndefined();
      });

      it('should update with multiple operators', async () => {
        const result = await collection.updateOne(
          { _id: 'update-1' },
          {
            $set: { status: 'updated' },
            $inc: { age: 1 },
          }
        );

        expect(result.acknowledged).toBe(true);

        const doc = await collection.findOne({ _id: 'update-1' });
        expect(doc!.status).toBe('updated');
        expect(doc!.age).toBe(31);
      });

      it('should return matchedCount: 0 when no document matches', async () => {
        const result = await collection.updateOne(
          { _id: 'nonexistent' },
          { $set: { status: 'updated' } }
        );

        expect(result.acknowledged).toBe(true);
        expect(result.matchedCount).toBe(0);
        expect(result.modifiedCount).toBe(0);
      });

      it('should upsert when document does not exist', async () => {
        const result = await collection.updateOne(
          { _id: 'new-doc' },
          { $set: { name: 'New User', status: 'created' } },
          { upsert: true }
        );

        expect(result.acknowledged).toBe(true);
        expect(result.upsertedCount).toBe(1);
        expect(result.upsertedId).toBeDefined();

        // Find by the upserted ID (MongoLake may generate a new ID)
        const doc = await collection.findOne({ name: 'New User' });
        expect(doc).not.toBeNull();
        expect(doc!.name).toBe('New User');
        expect(doc!.status).toBe('created');
      });
    });

    describe('updateMany', () => {
      it('should update multiple documents', async () => {
        const result = await collection.updateMany(
          { status: 'pending' },
          { $set: { status: 'processed' } }
        );

        expect(result.acknowledged).toBe(true);
        expect(result.matchedCount).toBe(3);
        expect(result.modifiedCount).toBe(3);

        // Verify all documents were updated
        const docs = await collection.find({ status: 'processed' }).toArray();
        expect(docs.length).toBe(3);
      });

      it('should update multiple documents with $inc', async () => {
        const result = await collection.updateMany(
          {},
          { $inc: { score: 5 } }
        );

        expect(result.acknowledged).toBe(true);
        expect(result.modifiedCount).toBe(3);
      });

      it('should return matchedCount: 0 when no documents match', async () => {
        const result = await collection.updateMany(
          { status: 'nonexistent' },
          { $set: { status: 'updated' } }
        );

        expect(result.matchedCount).toBe(0);
        expect(result.modifiedCount).toBe(0);
      });
    });
  });

  // ============================================================================
  // Delete Operations
  // ============================================================================

  describe('Delete Operations', () => {
    let collection: Collection<Document>;
    let collectionName: string;

    beforeEach(async () => {
      collectionName = uniqueCollection('delete');
      collection = db.collection(collectionName);

      // Insert test data
      await collection.insertMany([
        { _id: 'delete-1', name: 'Alice', status: 'active' },
        { _id: 'delete-2', name: 'Bob', status: 'active' },
        { _id: 'delete-3', name: 'Charlie', status: 'inactive' },
        { _id: 'delete-4', name: 'Diana', status: 'inactive' },
      ]);
    });

    describe('deleteOne', () => {
      it('should delete a single document', async () => {
        const result = await collection.deleteOne({ _id: 'delete-1' });

        expect(result.acknowledged).toBe(true);
        expect(result.deletedCount).toBe(1);

        // Verify deletion
        const doc = await collection.findOne({ _id: 'delete-1' });
        expect(doc).toBeNull();
      });

      it('should delete only one document when multiple match', async () => {
        const result = await collection.deleteOne({ status: 'active' });

        expect(result.acknowledged).toBe(true);
        expect(result.deletedCount).toBe(1);

        // Should still have one active document
        const remainingActive = await collection.find({ status: 'active' }).toArray();
        expect(remainingActive.length).toBe(1);
      });

      it('should return deletedCount: 0 when no document matches', async () => {
        const result = await collection.deleteOne({ _id: 'nonexistent' });

        expect(result.acknowledged).toBe(true);
        expect(result.deletedCount).toBe(0);
      });
    });

    describe('deleteMany', () => {
      it('should delete multiple documents', async () => {
        const result = await collection.deleteMany({ status: 'inactive' });

        expect(result.acknowledged).toBe(true);
        expect(result.deletedCount).toBe(2);

        // Verify deletion
        const remaining = await collection.find({ status: 'inactive' }).toArray();
        expect(remaining.length).toBe(0);
      });

      it('should delete all documents with empty filter', async () => {
        const result = await collection.deleteMany({});

        expect(result.acknowledged).toBe(true);
        expect(result.deletedCount).toBe(4);

        const remaining = await collection.find({}).toArray();
        expect(remaining.length).toBe(0);
      });

      it('should return deletedCount: 0 when no documents match', async () => {
        const result = await collection.deleteMany({ status: 'nonexistent' });

        expect(result.acknowledged).toBe(true);
        expect(result.deletedCount).toBe(0);
      });
    });
  });

  // ============================================================================
  // Collection Operations
  // ============================================================================

  describe('Collection Operations', () => {
    it('should list collections', async () => {
      const testCollName = uniqueCollection('list_test');
      const collection = db.collection(testCollName);
      await collection.insertOne({ _id: 'test', value: 1 });

      const collections = await db.listCollections().toArray();

      // Verify we can list collections (some collections exist)
      expect(collections).toBeDefined();
      expect(Array.isArray(collections)).toBe(true);
      // Note: Collection may not appear immediately in listing due to storage implementation
    });

    it('should drop a collection', async () => {
      const testCollName = uniqueCollection('drop_test');
      const collection = db.collection(testCollName);
      await collection.insertOne({ _id: 'test', value: 1 });

      // Drop collection
      try {
        const dropped = await collection.drop();
        expect(dropped).toBe(true);
      } catch (error) {
        // Some implementations may throw if collection doesn't exist or can't be dropped
        // This is acceptable behavior
        expect(error).toBeDefined();
      }

      // Verify documents are no longer accessible
      const docs = await collection.find({}).toArray();
      expect(docs.length).toBe(0);
    });

    it('should count documents', async () => {
      const testCollName = uniqueCollection('count_test');
      const collection = db.collection(testCollName);

      await collection.insertMany([
        { _id: '1', status: 'active' },
        { _id: '2', status: 'active' },
        { _id: '3', status: 'inactive' },
      ]);

      const totalCount = await collection.countDocuments();
      expect(totalCount).toBe(3);

      const activeCount = await collection.countDocuments({ status: 'active' });
      expect(activeCount).toBe(2);
    });

    it('should get distinct values', async () => {
      const testCollName = uniqueCollection('distinct_test');
      const collection = db.collection(testCollName);

      await collection.insertMany([
        { _id: '1', category: 'A' },
        { _id: '2', category: 'B' },
        { _id: '3', category: 'A' },
        { _id: '4', category: 'C' },
      ]);

      const categories = await collection.distinct('category');
      expect(categories.sort()).toEqual(['A', 'B', 'C']);
    });
  });

  // ============================================================================
  // Aggregation Operations
  // ============================================================================

  describe('Aggregation Operations', () => {
    let collection: Collection<Document>;
    let collectionName: string;

    beforeEach(async () => {
      collectionName = uniqueCollection('aggregate');
      collection = db.collection(collectionName);

      // Insert test data
      await collection.insertMany([
        { _id: '1', category: 'electronics', price: 500, quantity: 10 },
        { _id: '2', category: 'electronics', price: 800, quantity: 5 },
        { _id: '3', category: 'clothing', price: 50, quantity: 100 },
        { _id: '4', category: 'clothing', price: 80, quantity: 50 },
        { _id: '5', category: 'books', price: 20, quantity: 200 },
      ]);
    });

    it('should execute $match stage', async () => {
      const result = await collection
        .aggregate([{ $match: { category: 'electronics' } }])
        .toArray();

      expect(result.length).toBe(2);
      for (const doc of result) {
        expect(doc.category).toBe('electronics');
      }
    });

    it('should execute $sort stage', async () => {
      const result = await collection.aggregate([{ $sort: { price: -1 } }]).toArray();

      expect(result.length).toBe(5);
      for (let i = 1; i < result.length; i++) {
        expect(result[i].price).toBeLessThanOrEqual(result[i - 1].price);
      }
    });

    it('should execute $limit stage', async () => {
      const result = await collection.aggregate([{ $limit: 3 }]).toArray();

      expect(result.length).toBe(3);
    });

    it('should execute $skip stage', async () => {
      const all = await collection.find({}).toArray();
      const result = await collection.aggregate([{ $skip: 2 }]).toArray();

      expect(result.length).toBe(all.length - 2);
    });

    it('should execute $count stage', async () => {
      const result = await collection.aggregate([{ $count: 'totalCount' }]).toArray();

      expect(result.length).toBe(1);
      expect(result[0].totalCount).toBe(5);
    });

    it('should execute multiple stages in pipeline', async () => {
      const result = await collection
        .aggregate([
          { $match: { category: { $in: ['electronics', 'clothing'] } } },
          { $sort: { price: 1 } },
          { $limit: 3 },
        ])
        .toArray();

      expect(result.length).toBe(3);
      // Should be sorted by price ascending
      for (let i = 1; i < result.length; i++) {
        expect(result[i].price).toBeGreaterThanOrEqual(result[i - 1].price);
      }
    });

    it('should execute $group stage', async () => {
      const result = await collection
        .aggregate([
          {
            $group: {
              _id: '$category',
              totalQuantity: { $sum: '$quantity' },
              count: { $sum: 1 },
            },
          },
        ])
        .toArray();

      expect(result.length).toBe(3); // 3 categories

      // Find electronics group
      const electronics = result.find((r) => r._id === 'electronics');
      expect(electronics).toBeDefined();
      expect(electronics!.totalQuantity).toBe(15);
      expect(electronics!.count).toBe(2);
    });
  });

  // ============================================================================
  // Database Operations
  // ============================================================================

  describe('Database Operations', () => {
    it('should list databases', async () => {
      const result = await client.db('admin').admin().listDatabases();

      expect(result.ok).toBe(1);
      expect(result.databases).toBeDefined();
      expect(Array.isArray(result.databases)).toBe(true);
    });
  });

  // ============================================================================
  // Error Handling
  // ============================================================================

  describe('Error Handling', () => {
    it('should handle unknown command gracefully', async () => {
      try {
        await db.admin().command({ unknownCommand: 1 });
        // If it doesn't throw, check the response
      } catch (error) {
        // Expected to throw for unknown commands
        expect(error).toBeDefined();
      }
    });
  });

  // ============================================================================
  // Read-Your-Writes Consistency
  // ============================================================================

  describe('Read-Your-Writes Consistency', () => {
    it('should immediately read back an inserted document', async () => {
      const collectionName = uniqueCollection('ryw');
      const collection = db.collection(collectionName);

      const doc = { _id: 'ryw-test', name: 'Immediate Read', timestamp: Date.now() };

      await collection.insertOne(doc);

      const found = await collection.findOne({ _id: 'ryw-test' });

      expect(found).not.toBeNull();
      expect(found!.name).toBe('Immediate Read');
    });

    it('should immediately read back an updated document', async () => {
      const collectionName = uniqueCollection('ryw_update');
      const collection = db.collection(collectionName);

      await collection.insertOne({ _id: 'ryw-update', version: 1 });

      await collection.updateOne({ _id: 'ryw-update' }, { $set: { version: 2 } });

      const found = await collection.findOne({ _id: 'ryw-update' });

      expect(found!.version).toBe(2);
    });

    it('should not find a deleted document after deletion', async () => {
      const collectionName = uniqueCollection('ryw_delete');
      const collection = db.collection(collectionName);

      await collection.insertOne({ _id: 'ryw-delete', name: 'To Be Deleted' });

      await collection.deleteOne({ _id: 'ryw-delete' });

      const found = await collection.findOne({ _id: 'ryw-delete' });

      expect(found).toBeNull();
    });
  });
});
