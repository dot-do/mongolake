/**
 * MongoLake E2E Tests - CRUD Lifecycle
 *
 * Basic CRUD smoke tests that validate the system works end-to-end.
 * Uses local storage (.mongolake/) for testing without deployment.
 *
 * Tests cover:
 * - Insert a document and verify it exists
 * - Find documents with various filters
 * - Update a document and verify changes
 * - Delete a document and verify removal
 * - insertMany and deleteMany operations
 * - findOne vs find().toArray() comparison
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest';
import { MongoClient, type Db, type Collection, type Document } from 'mongodb';
import { createServer, type TcpServer } from '../../src/wire-protocol/tcp-server.js';
import * as fs from 'node:fs';

// Test configuration
const TEST_DATA_DIR = '.mongolake-e2e-crud-test';
const TEST_DB_NAME = 'crud_lifecycle_test';

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

describe('CRUD Lifecycle E2E Tests', () => {
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
  // Insert and Verify Tests
  // ============================================================================

  describe('Insert and Verify Document Exists', () => {
    let collection: Collection<Document>;

    beforeEach(() => {
      collection = db.collection(uniqueCollection('insert_verify'));
    });

    it('should insert a document and immediately find it by _id', async () => {
      const doc = {
        _id: 'crud-test-1',
        name: 'Test Document',
        value: 42,
        timestamp: new Date().toISOString(),
      };

      // Insert the document
      const insertResult = await collection.insertOne(doc);
      expect(insertResult.acknowledged).toBe(true);
      expect(insertResult.insertedId).toBe('crud-test-1');

      // Verify it exists by finding it
      const found = await collection.findOne({ _id: 'crud-test-1' });
      expect(found).not.toBeNull();
      expect(found!.name).toBe('Test Document');
      expect(found!.value).toBe(42);
    });

    it('should insert a document with auto-generated _id and verify it exists', async () => {
      const doc = {
        name: 'Auto ID Document',
        category: 'test',
      };

      const insertResult = await collection.insertOne(doc);
      expect(insertResult.acknowledged).toBe(true);
      expect(insertResult.insertedId).toBeDefined();

      // Find by the generated ID
      const found = await collection.findOne({ _id: insertResult.insertedId });
      expect(found).not.toBeNull();
      expect(found!.name).toBe('Auto ID Document');
    });

    it('should insert document with nested structure and verify all fields', async () => {
      const doc = {
        _id: 'nested-doc',
        user: {
          name: 'John Doe',
          email: 'john@example.com',
          profile: {
            age: 30,
            interests: ['coding', 'reading'],
          },
        },
        metadata: {
          created: new Date().toISOString(),
          version: 1,
        },
      };

      await collection.insertOne(doc);

      const found = await collection.findOne({ _id: 'nested-doc' });
      expect(found).not.toBeNull();
      expect(found!.user.name).toBe('John Doe');
      expect(found!.user.profile.age).toBe(30);
      expect(found!.user.profile.interests).toEqual(['coding', 'reading']);
    });
  });

  // ============================================================================
  // Find with Various Filters
  // ============================================================================

  describe('Find Documents with Various Filters', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('find_filters'));

      // Insert test data
      await collection.insertMany([
        { _id: 'user-1', name: 'Alice', age: 25, status: 'active', score: 85 },
        { _id: 'user-2', name: 'Bob', age: 30, status: 'active', score: 92 },
        { _id: 'user-3', name: 'Charlie', age: 35, status: 'inactive', score: 78 },
        { _id: 'user-4', name: 'Diana', age: 28, status: 'active', score: 95 },
        { _id: 'user-5', name: 'Eve', age: 32, status: 'inactive', score: 88 },
      ]);
    });

    it('should find all documents with empty filter', async () => {
      const docs = await collection.find({}).toArray();
      expect(docs.length).toBe(5);
    });

    it('should find documents by exact field match', async () => {
      const docs = await collection.find({ status: 'active' }).toArray();
      expect(docs.length).toBe(3);
      docs.forEach((doc) => {
        expect(doc.status).toBe('active');
      });
    });

    it('should find documents with $gt comparison', async () => {
      const docs = await collection.find({ age: { $gt: 30 } }).toArray();
      expect(docs.length).toBeGreaterThan(0);
      docs.forEach((doc) => {
        expect(doc.age).toBeGreaterThan(30);
      });
    });

    it('should find documents with $lt comparison', async () => {
      const docs = await collection.find({ age: { $lt: 30 } }).toArray();
      expect(docs.length).toBeGreaterThan(0);
      docs.forEach((doc) => {
        expect(doc.age).toBeLessThan(30);
      });
    });

    it('should find documents with $gte and $lte range', async () => {
      const docs = await collection.find({
        age: { $gte: 28, $lte: 32 },
      }).toArray();

      expect(docs.length).toBeGreaterThan(0);
      docs.forEach((doc) => {
        expect(doc.age).toBeGreaterThanOrEqual(28);
        expect(doc.age).toBeLessThanOrEqual(32);
      });
    });

    it('should find documents with $in operator', async () => {
      const docs = await collection.find({
        name: { $in: ['Alice', 'Bob', 'Charlie'] },
      }).toArray();

      expect(docs.length).toBe(3);
      docs.forEach((doc) => {
        expect(['Alice', 'Bob', 'Charlie']).toContain(doc.name);
      });
    });

    it('should find documents with $ne (not equal)', async () => {
      const docs = await collection.find({ status: { $ne: 'active' } }).toArray();
      expect(docs.length).toBeGreaterThan(0);
      docs.forEach((doc) => {
        expect(doc.status).not.toBe('active');
      });
    });

    it('should find documents with combined filters', async () => {
      const docs = await collection.find({
        status: 'active',
        age: { $gte: 28 },
      }).toArray();

      expect(docs.length).toBeGreaterThan(0);
      docs.forEach((doc) => {
        expect(doc.status).toBe('active');
        expect(doc.age).toBeGreaterThanOrEqual(28);
      });
    });

    it('should return empty array when no documents match', async () => {
      const docs = await collection.find({ status: 'nonexistent' }).toArray();
      expect(docs).toEqual([]);
    });
  });

  // ============================================================================
  // Update and Verify Changes
  // ============================================================================

  describe('Update Document and Verify Changes', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('update_verify'));

      await collection.insertOne({
        _id: 'update-test',
        name: 'Original Name',
        status: 'pending',
        counter: 10,
        tags: ['initial'],
      });
    });

    it('should update with $set and verify changes', async () => {
      const updateResult = await collection.updateOne(
        { _id: 'update-test' },
        { $set: { status: 'completed', name: 'Updated Name' } }
      );

      expect(updateResult.acknowledged).toBe(true);
      expect(updateResult.matchedCount).toBe(1);
      expect(updateResult.modifiedCount).toBe(1);

      // Verify the update
      const doc = await collection.findOne({ _id: 'update-test' });
      expect(doc!.status).toBe('completed');
      expect(doc!.name).toBe('Updated Name');
      expect(doc!.counter).toBe(10); // Unchanged fields preserved
    });

    it('should update with $inc and verify numeric increment', async () => {
      const updateResult = await collection.updateOne(
        { _id: 'update-test' },
        { $inc: { counter: 5 } }
      );

      expect(updateResult.modifiedCount).toBe(1);

      const doc = await collection.findOne({ _id: 'update-test' });
      expect(doc!.counter).toBe(15);
    });

    it('should update with $unset and verify field removal', async () => {
      await collection.updateOne(
        { _id: 'update-test' },
        { $unset: { tags: '' } }
      );

      const doc = await collection.findOne({ _id: 'update-test' });
      expect(doc!.tags).toBeUndefined();
    });

    it('should update with multiple operators simultaneously', async () => {
      await collection.updateOne(
        { _id: 'update-test' },
        {
          $set: { status: 'processed' },
          $inc: { counter: -2 },
        }
      );

      const doc = await collection.findOne({ _id: 'update-test' });
      expect(doc!.status).toBe('processed');
      expect(doc!.counter).toBe(8);
    });

    it('should return matchedCount: 0 when document does not exist', async () => {
      const result = await collection.updateOne(
        { _id: 'nonexistent' },
        { $set: { status: 'failed' } }
      );

      expect(result.matchedCount).toBe(0);
      expect(result.modifiedCount).toBe(0);
    });

    it('should upsert document when it does not exist', async () => {
      const result = await collection.updateOne(
        { _id: 'new-upsert-doc' },
        { $set: { name: 'Upserted', created: true } },
        { upsert: true }
      );

      expect(result.upsertedCount).toBe(1);

      // Verify upserted document
      const doc = await collection.findOne({ name: 'Upserted' });
      expect(doc).not.toBeNull();
      expect(doc!.name).toBe('Upserted');
      expect(doc!.created).toBe(true);
    });
  });

  // ============================================================================
  // Delete and Verify Removal
  // ============================================================================

  describe('Delete Document and Verify Removal', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('delete_verify'));

      await collection.insertMany([
        { _id: 'delete-1', name: 'Doc 1', status: 'active' },
        { _id: 'delete-2', name: 'Doc 2', status: 'active' },
        { _id: 'delete-3', name: 'Doc 3', status: 'inactive' },
      ]);
    });

    it('should delete a document and verify it no longer exists', async () => {
      // Verify document exists before delete
      const before = await collection.findOne({ _id: 'delete-1' });
      expect(before).not.toBeNull();

      // Delete the document
      const deleteResult = await collection.deleteOne({ _id: 'delete-1' });
      expect(deleteResult.acknowledged).toBe(true);
      expect(deleteResult.deletedCount).toBe(1);

      // Verify document no longer exists
      const after = await collection.findOne({ _id: 'delete-1' });
      expect(after).toBeNull();
    });

    it('should return deletedCount: 0 when document does not exist', async () => {
      const result = await collection.deleteOne({ _id: 'nonexistent' });
      expect(result.deletedCount).toBe(0);
    });

    it('should delete document by field filter', async () => {
      const result = await collection.deleteOne({ name: 'Doc 2' });
      expect(result.deletedCount).toBe(1);

      const doc = await collection.findOne({ name: 'Doc 2' });
      expect(doc).toBeNull();
    });
  });

  // ============================================================================
  // insertMany and deleteMany
  // ============================================================================

  describe('Bulk Operations: insertMany and deleteMany', () => {
    let collection: Collection<Document>;

    beforeEach(() => {
      collection = db.collection(uniqueCollection('bulk_ops'));
    });

    it('should insertMany documents and verify all exist', async () => {
      const docs = [
        { _id: 'bulk-1', name: 'Bulk Doc 1', category: 'A' },
        { _id: 'bulk-2', name: 'Bulk Doc 2', category: 'A' },
        { _id: 'bulk-3', name: 'Bulk Doc 3', category: 'B' },
        { _id: 'bulk-4', name: 'Bulk Doc 4', category: 'B' },
        { _id: 'bulk-5', name: 'Bulk Doc 5', category: 'C' },
      ];

      const insertResult = await collection.insertMany(docs);
      expect(insertResult.acknowledged).toBe(true);
      expect(insertResult.insertedCount).toBe(5);
      expect(Object.keys(insertResult.insertedIds)).toHaveLength(5);

      // Verify all documents exist
      const allDocs = await collection.find({}).toArray();
      expect(allDocs.length).toBe(5);

      // Verify each document by ID
      for (const doc of docs) {
        const found = await collection.findOne({ _id: doc._id });
        expect(found).not.toBeNull();
        expect(found!.name).toBe(doc.name);
      }
    });

    it('should insertMany with auto-generated IDs', async () => {
      const docs = [
        { name: 'Auto 1', value: 1 },
        { name: 'Auto 2', value: 2 },
        { name: 'Auto 3', value: 3 },
      ];

      const result = await collection.insertMany(docs);
      expect(result.insertedCount).toBe(3);

      // All inserted documents should be findable
      const allDocs = await collection.find({}).toArray();
      expect(allDocs.length).toBe(3);
    });

    it('should deleteMany documents by filter', async () => {
      // Insert test data
      await collection.insertMany([
        { _id: 'd1', status: 'pending' },
        { _id: 'd2', status: 'pending' },
        { _id: 'd3', status: 'completed' },
        { _id: 'd4', status: 'pending' },
        { _id: 'd5', status: 'completed' },
      ]);

      // Delete all pending documents
      const deleteResult = await collection.deleteMany({ status: 'pending' });
      expect(deleteResult.acknowledged).toBe(true);
      expect(deleteResult.deletedCount).toBe(3);

      // Verify only completed documents remain
      const remaining = await collection.find({}).toArray();
      expect(remaining.length).toBe(2);
      remaining.forEach((doc) => {
        expect(doc.status).toBe('completed');
      });
    });

    it('should deleteMany with empty filter to clear collection', async () => {
      await collection.insertMany([
        { _id: 'c1', value: 1 },
        { _id: 'c2', value: 2 },
        { _id: 'c3', value: 3 },
      ]);

      // Delete all documents
      const result = await collection.deleteMany({});
      expect(result.deletedCount).toBe(3);

      // Verify collection is empty
      const docs = await collection.find({}).toArray();
      expect(docs.length).toBe(0);
    });

    it('should return deletedCount: 0 when no documents match filter', async () => {
      await collection.insertMany([
        { _id: 'x1', status: 'active' },
        { _id: 'x2', status: 'active' },
      ]);

      const result = await collection.deleteMany({ status: 'nonexistent' });
      expect(result.deletedCount).toBe(0);
    });
  });

  // ============================================================================
  // findOne vs find().toArray() Comparison
  // ============================================================================

  describe('findOne vs find().toArray() Comparison', () => {
    let collection: Collection<Document>;

    beforeEach(async () => {
      collection = db.collection(uniqueCollection('find_compare'));

      await collection.insertMany([
        { _id: 'find-1', name: 'Alice', role: 'admin' },
        { _id: 'find-2', name: 'Bob', role: 'user' },
        { _id: 'find-3', name: 'Charlie', role: 'user' },
      ]);
    });

    it('findOne should return single document, find().toArray() should return array', async () => {
      // findOne returns a single document or null
      const oneDoc = await collection.findOne({ role: 'admin' });
      expect(oneDoc).not.toBeNull();
      expect(oneDoc!.name).toBe('Alice');

      // find().toArray() returns an array
      const allAdmins = await collection.find({ role: 'admin' }).toArray();
      expect(Array.isArray(allAdmins)).toBe(true);
      expect(allAdmins.length).toBe(1);
      expect(allAdmins[0].name).toBe('Alice');
    });

    it('findOne returns null when not found, find().toArray() returns empty array', async () => {
      const oneDoc = await collection.findOne({ role: 'superadmin' });
      expect(oneDoc).toBeNull();

      const docs = await collection.find({ role: 'superadmin' }).toArray();
      expect(docs).toEqual([]);
    });

    it('findOne returns only first match, find().toArray() returns all matches', async () => {
      // findOne returns just one user document (order may vary)
      const oneUser = await collection.findOne({ role: 'user' });
      expect(oneUser).not.toBeNull();
      expect(oneUser!.role).toBe('user');

      // find().toArray() returns all users
      const allUsers = await collection.find({ role: 'user' }).toArray();
      expect(allUsers.length).toBe(2);
      allUsers.forEach((doc) => {
        expect(doc.role).toBe('user');
      });
    });

    it('both methods work correctly with _id lookup', async () => {
      const byFindOne = await collection.findOne({ _id: 'find-2' });
      const byFind = await collection.find({ _id: 'find-2' }).toArray();

      expect(byFindOne).not.toBeNull();
      expect(byFindOne!.name).toBe('Bob');

      expect(byFind.length).toBe(1);
      expect(byFind[0].name).toBe('Bob');
    });

    it('find() supports chaining with sort/limit, findOne does not need limit', async () => {
      // find() with sort and limit
      const sorted = await collection
        .find({})
        .sort({ name: 1 })
        .limit(2)
        .toArray();

      expect(sorted.length).toBe(2);
      // Alphabetically: Alice, Bob, Charlie -> first 2 are Alice, Bob
      expect(sorted[0].name).toBe('Alice');
      expect(sorted[1].name).toBe('Bob');

      // findOne naturally returns first match (with optional sort)
      const first = await collection.findOne({}, { sort: { name: -1 } });
      expect(first!.name).toBe('Charlie'); // Reverse alphabetical
    });
  });

  // ============================================================================
  // Full CRUD Lifecycle Test
  // ============================================================================

  describe('Complete CRUD Lifecycle', () => {
    it('should perform full create-read-update-delete lifecycle', async () => {
      const collection = db.collection(uniqueCollection('full_lifecycle'));

      // CREATE
      const doc = {
        _id: 'lifecycle-doc',
        title: 'Original Title',
        version: 1,
        tags: ['new'],
      };
      const createResult = await collection.insertOne(doc);
      expect(createResult.acknowledged).toBe(true);

      // READ - verify creation
      let current = await collection.findOne({ _id: 'lifecycle-doc' });
      expect(current).not.toBeNull();
      expect(current!.title).toBe('Original Title');
      expect(current!.version).toBe(1);

      // UPDATE - modify the document
      await collection.updateOne(
        { _id: 'lifecycle-doc' },
        {
          $set: { title: 'Updated Title' },
          $inc: { version: 1 },
        }
      );

      // READ - verify update
      current = await collection.findOne({ _id: 'lifecycle-doc' });
      expect(current!.title).toBe('Updated Title');
      expect(current!.version).toBe(2);

      // DELETE - remove the document
      const deleteResult = await collection.deleteOne({ _id: 'lifecycle-doc' });
      expect(deleteResult.deletedCount).toBe(1);

      // READ - verify deletion
      current = await collection.findOne({ _id: 'lifecycle-doc' });
      expect(current).toBeNull();
    });
  });
});
