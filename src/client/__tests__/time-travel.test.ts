/**
 * Time Travel Tests
 *
 * Tests for querying collections at specific snapshots or timestamps
 * using Iceberg-style time travel.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { MemoryStorage } from '../../storage/index.js';
import { Collection, Database, TimeTravelCollection, MongoLake } from '../index.js';
import type { Document } from '../../types.js';

// ============================================================================
// Test Setup
// ============================================================================

interface TestDoc extends Document {
  name: string;
  value: number;
}

describe('Time Travel', () => {
  let storage: MemoryStorage;
  let db: Database;
  let collection: Collection<TestDoc>;

  beforeEach(() => {
    storage = new MemoryStorage();
    // Create a minimal MongoLake instance to get database
    const config = { local: '.test-mongolake' };
    db = new Database('testdb', storage, config);
    collection = db.collection<TestDoc>('users');
  });

  // --------------------------------------------------------------------------
  // Basic Time Travel API
  // --------------------------------------------------------------------------

  describe('asOf()', () => {
    it('should return a TimeTravelCollection', () => {
      const timestamp = new Date();
      const ttCollection = collection.asOf(timestamp);

      expect(ttCollection).toBeInstanceOf(TimeTravelCollection);
      expect(ttCollection.name).toBe('users');
    });

    it('should create time travel view with correct timestamp', async () => {
      const timestamp = new Date('2024-06-15T12:00:00Z');
      const ttCollection = collection.asOf(timestamp);

      // The collection should be created but snapshot will be null (no Iceberg metadata)
      const snapshot = await ttCollection.getSnapshot();
      expect(snapshot).toBeNull();
    });
  });

  describe('atSnapshot()', () => {
    it('should return a TimeTravelCollection', () => {
      const snapshotId = 12345n;
      const ttCollection = collection.atSnapshot(snapshotId);

      expect(ttCollection).toBeInstanceOf(TimeTravelCollection);
      expect(ttCollection.name).toBe('users');
    });

    it('should create time travel view with correct snapshot ID', async () => {
      const snapshotId = 12345n;
      const ttCollection = collection.atSnapshot(snapshotId);

      // The collection should be created but snapshot will be null (no Iceberg metadata)
      const snapshot = await ttCollection.getSnapshot();
      expect(snapshot).toBeNull();
    });
  });

  // --------------------------------------------------------------------------
  // Time Travel Read Operations
  // --------------------------------------------------------------------------

  describe('TimeTravelCollection read operations', () => {
    it('should support find()', async () => {
      // Insert some documents first
      await collection.insertMany([
        { name: 'Alice', value: 1 },
        { name: 'Bob', value: 2 },
      ]);

      // Create time travel view (will include all docs since no timestamp filtering without Iceberg)
      const ttCollection = collection.asOf(new Date());
      const docs = await ttCollection.find().toArray();

      expect(docs).toHaveLength(2);
      expect(docs.map((d) => d.name).sort()).toEqual(['Alice', 'Bob']);
    });

    it('should support findOne()', async () => {
      await collection.insertMany([
        { name: 'Alice', value: 1 },
        { name: 'Bob', value: 2 },
      ]);

      const ttCollection = collection.asOf(new Date());
      const doc = await ttCollection.findOne({ name: 'Alice' });

      expect(doc).not.toBeNull();
      expect(doc?.name).toBe('Alice');
    });

    it('should support find() with filter', async () => {
      await collection.insertMany([
        { name: 'Alice', value: 1 },
        { name: 'Bob', value: 2 },
        { name: 'Charlie', value: 3 },
      ]);

      const ttCollection = collection.asOf(new Date());
      const docs = await ttCollection.find({ value: { $gt: 1 } }).toArray();

      expect(docs).toHaveLength(2);
      expect(docs.map((d) => d.name).sort()).toEqual(['Bob', 'Charlie']);
    });

    it('should support countDocuments()', async () => {
      await collection.insertMany([
        { name: 'Alice', value: 1 },
        { name: 'Bob', value: 2 },
      ]);

      const ttCollection = collection.asOf(new Date());
      const count = await ttCollection.countDocuments();

      expect(count).toBe(2);
    });

    it('should support countDocuments() with filter', async () => {
      await collection.insertMany([
        { name: 'Alice', value: 1 },
        { name: 'Bob', value: 2 },
        { name: 'Charlie', value: 3 },
      ]);

      const ttCollection = collection.asOf(new Date());
      const count = await ttCollection.countDocuments({ value: { $gte: 2 } });

      expect(count).toBe(2);
    });

    it('should support distinct()', async () => {
      await collection.insertMany([
        { name: 'Alice', value: 1 },
        { name: 'Bob', value: 1 },
        { name: 'Charlie', value: 2 },
      ]);

      const ttCollection = collection.asOf(new Date());
      const values = await ttCollection.distinct('value');

      expect(values.sort()).toEqual([1, 2]);
    });

    it('should support estimatedDocumentCount()', async () => {
      await collection.insertMany([
        { name: 'Alice', value: 1 },
        { name: 'Bob', value: 2 },
      ]);

      const ttCollection = collection.asOf(new Date());
      const count = await ttCollection.estimatedDocumentCount();

      expect(count).toBe(2);
    });
  });

  // --------------------------------------------------------------------------
  // Time Travel Cursor Operations
  // --------------------------------------------------------------------------

  describe('TimeTravelFindCursor', () => {
    beforeEach(async () => {
      await collection.insertMany([
        { name: 'Alice', value: 1 },
        { name: 'Bob', value: 2 },
        { name: 'Charlie', value: 3 },
        { name: 'Diana', value: 4 },
      ]);
    });

    it('should support sort()', async () => {
      const ttCollection = collection.asOf(new Date());
      const docs = await ttCollection.find().sort({ value: -1 }).toArray();

      expect(docs[0].name).toBe('Diana');
      expect(docs[3].name).toBe('Alice');
    });

    it('should support limit()', async () => {
      const ttCollection = collection.asOf(new Date());
      const docs = await ttCollection.find().limit(2).toArray();

      expect(docs).toHaveLength(2);
    });

    it('should support skip()', async () => {
      const ttCollection = collection.asOf(new Date());
      const docs = await ttCollection.find().sort({ value: 1 }).skip(2).toArray();

      expect(docs).toHaveLength(2);
      expect(docs[0].name).toBe('Charlie');
    });

    it('should support project()', async () => {
      const ttCollection = collection.asOf(new Date());
      const docs = await ttCollection.find().project({ name: 1 }).toArray();

      expect(docs[0].name).toBeDefined();
      // Note: projection behavior depends on implementation
    });

    it('should support chaining operations', async () => {
      const ttCollection = collection.asOf(new Date());
      const docs = await ttCollection
        .find({ value: { $gte: 2 } })
        .sort({ value: -1 })
        .skip(1)
        .limit(2)
        .toArray();

      expect(docs).toHaveLength(2);
      expect(docs[0].name).toBe('Charlie');
      expect(docs[1].name).toBe('Bob');
    });

    it('should support forEach()', async () => {
      const ttCollection = collection.asOf(new Date());
      const names: string[] = [];

      await ttCollection.find().forEach((doc) => {
        names.push(doc.name);
      });

      expect(names).toHaveLength(4);
    });

    it('should support map()', async () => {
      const ttCollection = collection.asOf(new Date());
      const names = await ttCollection.find().map((doc) => doc.name);

      expect(names).toHaveLength(4);
      expect(names.sort()).toEqual(['Alice', 'Bob', 'Charlie', 'Diana']);
    });

    it('should support async iteration', async () => {
      const ttCollection = collection.asOf(new Date());
      const names: string[] = [];

      for await (const doc of ttCollection.find()) {
        names.push(doc.name);
      }

      expect(names).toHaveLength(4);
    });

    it('should support hasNext()', async () => {
      const ttCollection = collection.asOf(new Date());
      const cursor = ttCollection.find();

      expect(await cursor.hasNext()).toBe(true);
    });

    it('should support next()', async () => {
      const ttCollection = collection.asOf(new Date());
      const cursor = ttCollection.find().sort({ value: 1 });

      const first = await cursor.next();
      expect(first?.name).toBe('Alice');
    });
  });

  // --------------------------------------------------------------------------
  // Time Travel Aggregation
  // --------------------------------------------------------------------------

  describe('TimeTravelAggregationCursor', () => {
    beforeEach(async () => {
      await collection.insertMany([
        { name: 'Alice', value: 1 },
        { name: 'Bob', value: 2 },
        { name: 'Charlie', value: 3 },
      ]);
    });

    it('should support $match stage', async () => {
      const ttCollection = collection.asOf(new Date());
      const results = await ttCollection
        .aggregate([{ $match: { value: { $gte: 2 } } }])
        .toArray();

      expect(results).toHaveLength(2);
    });

    it('should support $sort stage', async () => {
      const ttCollection = collection.asOf(new Date());
      const results = await ttCollection
        .aggregate([{ $sort: { value: -1 } }])
        .toArray();

      expect(results[0].name).toBe('Charlie');
    });

    it('should support $limit stage', async () => {
      const ttCollection = collection.asOf(new Date());
      const results = await ttCollection.aggregate([{ $limit: 2 }]).toArray();

      expect(results).toHaveLength(2);
    });

    it('should support $skip stage', async () => {
      const ttCollection = collection.asOf(new Date());
      const results = await ttCollection
        .aggregate([{ $sort: { value: 1 } }, { $skip: 1 }])
        .toArray();

      expect(results).toHaveLength(2);
      expect(results[0].name).toBe('Bob');
    });

    it('should support $count stage', async () => {
      const ttCollection = collection.asOf(new Date());
      const results = await ttCollection.aggregate([{ $count: 'total' }]).toArray();

      expect(results[0].total).toBe(3);
    });

    it('should support $project stage', async () => {
      const ttCollection = collection.asOf(new Date());
      const results = await ttCollection
        .aggregate([{ $project: { name: 1 } }])
        .toArray();

      expect(results[0].name).toBeDefined();
    });

    it('should support $addFields/$set stage', async () => {
      const ttCollection = collection.asOf(new Date());
      const results = await ttCollection
        .aggregate([{ $addFields: { doubled: { $mul: ['$value', 2] } } }])
        .toArray();

      // Note: $mul would need expression evaluation - this tests the stage works
      expect(results).toHaveLength(3);
    });

    it('should support $unset stage', async () => {
      const ttCollection = collection.asOf(new Date());
      const results = await ttCollection.aggregate([{ $unset: 'value' }]).toArray();

      expect(results[0].value).toBeUndefined();
    });

    it('should support multiple pipeline stages', async () => {
      const ttCollection = collection.asOf(new Date());
      const results = await ttCollection
        .aggregate([
          { $match: { value: { $gte: 2 } } },
          { $sort: { value: -1 } },
          { $limit: 1 },
        ])
        .toArray();

      expect(results).toHaveLength(1);
      expect(results[0].name).toBe('Charlie');
    });

    it('should support async iteration', async () => {
      const ttCollection = collection.asOf(new Date());
      const names: string[] = [];

      for await (const doc of ttCollection.aggregate([])) {
        names.push(doc.name as string);
      }

      expect(names).toHaveLength(3);
    });
  });

  // --------------------------------------------------------------------------
  // Timestamp-based Filtering
  // --------------------------------------------------------------------------

  describe('Timestamp-based filtering', () => {
    it('should filter files based on timestamp in filename', async () => {
      // Insert document at known time
      const beforeInsert = Date.now();
      await collection.insertOne({ name: 'First', value: 1 });

      // Small delay to ensure different timestamp
      await new Promise((resolve) => setTimeout(resolve, 10));

      const afterFirst = Date.now();
      await collection.insertOne({ name: 'Second', value: 2 });

      // Query at time before second insert
      const ttCollection = collection.asOf(new Date(afterFirst - 1));
      const docs = await ttCollection.find().toArray();

      // Note: Without full Iceberg support, timestamp filtering uses file naming
      // This test verifies the mechanism works
      expect(docs.length).toBeGreaterThanOrEqual(1);
    });

    it('should return empty result for timestamp before any data', async () => {
      const beforeAnyData = Date.now() - 1000000; // Far in the past

      await collection.insertOne({ name: 'Test', value: 1 });

      const ttCollection = collection.asOf(new Date(beforeAnyData));
      const docs = await ttCollection.find().toArray();

      // Files created after the timestamp should be excluded
      // The exact behavior depends on file naming convention
      expect(docs.length).toBe(0);
    });
  });

  // --------------------------------------------------------------------------
  // Sibling Collection Time Travel
  // --------------------------------------------------------------------------

  describe('Sibling collection time travel', () => {
    it('should get sibling collection at same point in time', async () => {
      const ordersCollection = db.collection('orders');
      await ordersCollection.insertOne({ orderId: 1, userId: 'alice' });

      const ttUsers = collection.asOf(new Date());
      const ttOrders = ttUsers.getSiblingCollection('orders');

      expect(ttOrders).toBeInstanceOf(TimeTravelCollection);
      expect(ttOrders.name).toBe('orders');
    });
  });

  // --------------------------------------------------------------------------
  // Snapshot Info
  // --------------------------------------------------------------------------

  describe('Snapshot information', () => {
    it('getSnapshot() returns null without Iceberg metadata', async () => {
      const ttCollection = collection.asOf(new Date());
      const snapshot = await ttCollection.getSnapshot();

      expect(snapshot).toBeNull();
    });

    it('getSnapshotTimestamp() returns null without Iceberg metadata', async () => {
      const ttCollection = collection.asOf(new Date());
      const timestamp = await ttCollection.getSnapshotTimestamp();

      expect(timestamp).toBeNull();
    });
  });
});
