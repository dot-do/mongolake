/**
 * MongoLake Client Tests
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { MongoLake, db, Collection, ObjectId } from '../client/index.js';
import { MemoryStorage } from '../storage/index.js';

describe('MongoLake Client', () => {
  let lake: MongoLake;
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
    lake = new MongoLake({ database: 'test' });
    // Inject memory storage for testing
    (lake as any).storage = storage;
  });

  afterEach(() => {
    storage.clear();
  });

  describe('Database', () => {
    it('should get a database', () => {
      const testDb = lake.db('mydb');
      expect(testDb.name).toBe('mydb');
    });

    it('should use default database', () => {
      const defaultDb = lake.db();
      expect(defaultDb.name).toBe('test');
    });
  });

  describe('Collection', () => {
    it('should get a collection', () => {
      const users = lake.db('mydb').collection('users');
      expect(users.name).toBe('users');
    });
  });

  describe('Insert Operations', () => {
    it('should insert one document', async () => {
      const users = lake.db().collection('users');
      const result = await users.insertOne({ name: 'Alice', email: 'alice@example.com' });

      expect(result.acknowledged).toBe(true);
      expect(result.insertedId).toBeDefined();
    });

    it('should insert many documents', async () => {
      const users = lake.db().collection('users');
      const result = await users.insertMany([
        { name: 'Alice', email: 'alice@example.com' },
        { name: 'Bob', email: 'bob@example.com' },
      ]);

      expect(result.acknowledged).toBe(true);
      expect(result.insertedCount).toBe(2);
      expect(Object.keys(result.insertedIds)).toHaveLength(2);
    });

    it('should use provided _id', async () => {
      const users = lake.db().collection('users');
      const result = await users.insertOne({ _id: 'custom-id', name: 'Alice' });

      expect(result.insertedId).toBe('custom-id');
    });
  });

  describe('Find Operations', () => {
    it('should find one document', async () => {
      const users = lake.db().collection('users');
      await users.insertOne({ _id: '1', name: 'Alice', email: 'alice@example.com' });

      const user = await users.findOne({ _id: '1' });

      expect(user).not.toBeNull();
      expect(user?.name).toBe('Alice');
    });

    it('should find documents with filter', async () => {
      const users = lake.db().collection('users');
      await users.insertMany([
        { name: 'Alice', status: 'active' },
        { name: 'Bob', status: 'inactive' },
        { name: 'Charlie', status: 'active' },
      ]);

      const activeUsers = await users.find({ status: 'active' }).toArray();

      expect(activeUsers).toHaveLength(2);
      expect(activeUsers.map((u) => u.name).sort()).toEqual(['Alice', 'Charlie']);
    });

    it('should support comparison operators', async () => {
      const products = lake.db().collection('products');
      await products.insertMany([
        { name: 'A', price: 10 },
        { name: 'B', price: 20 },
        { name: 'C', price: 30 },
      ]);

      const expensive = await products.find({ price: { $gt: 15 } }).toArray();

      expect(expensive).toHaveLength(2);
    });

    it('should support sort', async () => {
      const users = lake.db().collection('users');
      await users.insertMany([
        { name: 'Charlie', age: 30 },
        { name: 'Alice', age: 25 },
        { name: 'Bob', age: 35 },
      ]);

      const sorted = await users.find().sort({ age: 1 }).toArray();

      expect(sorted.map((u) => u.name)).toEqual(['Alice', 'Charlie', 'Bob']);
    });

    it('should support limit and skip', async () => {
      const users = lake.db().collection('users');
      await users.insertMany([
        { name: 'A' },
        { name: 'B' },
        { name: 'C' },
        { name: 'D' },
        { name: 'E' },
      ]);

      const page = await users.find().skip(1).limit(2).toArray();

      expect(page).toHaveLength(2);
    });
  });

  describe('Update Operations', () => {
    it('should update one document', async () => {
      const users = lake.db().collection('users');
      await users.insertOne({ _id: '1', name: 'Alice', status: 'pending' });

      const result = await users.updateOne({ _id: '1' }, { $set: { status: 'active' } });

      expect(result.matchedCount).toBe(1);
      expect(result.modifiedCount).toBe(1);

      const updated = await users.findOne({ _id: '1' });
      expect(updated?.status).toBe('active');
    });

    it('should support $inc operator', async () => {
      const users = lake.db().collection('users');
      await users.insertOne({ _id: '1', name: 'Alice', score: 10 });

      await users.updateOne({ _id: '1' }, { $inc: { score: 5 } });

      const updated = await users.findOne({ _id: '1' });
      expect(updated?.score).toBe(15);
    });

    it('should support $unset operator', async () => {
      const users = lake.db().collection('users');
      await users.insertOne({ _id: '1', name: 'Alice', temp: true });

      await users.updateOne({ _id: '1' }, { $unset: { temp: '' } });

      const updated = await users.findOne({ _id: '1' });
      expect(updated?.temp).toBeUndefined();
    });

    it('should support upsert', async () => {
      const users = lake.db().collection('users');

      const result = await users.updateOne(
        { _id: 'new' },
        { $set: { name: 'New User' } },
        { upsert: true }
      );

      expect(result.upsertedCount).toBe(1);
      expect(result.upsertedId).toBeDefined();
    });
  });

  describe('Delete Operations', () => {
    it('should delete one document', async () => {
      const users = lake.db().collection('users');
      await users.insertMany([
        { _id: '1', name: 'Alice' },
        { _id: '2', name: 'Bob' },
      ]);

      const result = await users.deleteOne({ _id: '1' });

      expect(result.deletedCount).toBe(1);

      const remaining = await users.find().toArray();
      expect(remaining).toHaveLength(1);
      expect(remaining[0].name).toBe('Bob');
    });

    it('should delete many documents', async () => {
      const users = lake.db().collection('users');
      await users.insertMany([
        { name: 'Alice', status: 'inactive' },
        { name: 'Bob', status: 'active' },
        { name: 'Charlie', status: 'inactive' },
      ]);

      const result = await users.deleteMany({ status: 'inactive' });

      expect(result.deletedCount).toBe(2);

      const remaining = await users.find().toArray();
      expect(remaining).toHaveLength(1);
    });
  });

  describe('Aggregation', () => {
    it('should support $match stage', async () => {
      const orders = lake.db().collection('orders');
      await orders.insertMany([
        { product: 'A', amount: 100 },
        { product: 'B', amount: 200 },
        { product: 'A', amount: 150 },
      ]);

      const results = await orders.aggregate([{ $match: { product: 'A' } }]).toArray();

      expect(results).toHaveLength(2);
    });

    it('should support $group stage', async () => {
      const orders = lake.db().collection('orders');
      await orders.insertMany([
        { product: 'A', amount: 100 },
        { product: 'B', amount: 200 },
        { product: 'A', amount: 150 },
      ]);

      const results = await orders
        .aggregate([{ $group: { _id: '$product', total: { $sum: '$amount' } } }])
        .toArray();

      expect(results).toHaveLength(2);

      const productA = results.find((r) => r._id === 'A');
      expect(productA?.total).toBe(250);
    });

    it('should support $count stage', async () => {
      const users = lake.db().collection('users');
      await users.insertMany([{ name: 'A' }, { name: 'B' }, { name: 'C' }]);

      const results = await users.aggregate([{ $count: 'total' }]).toArray();

      expect(results[0].total).toBe(3);
    });
  });

  describe('ObjectId', () => {
    it('should generate valid ObjectId', () => {
      const id = new ObjectId();
      expect(id.toString()).toMatch(/^[0-9a-f]{24}$/);
    });

    it('should parse ObjectId from string', () => {
      const hex = '507f1f77bcf86cd799439011';
      const id = new ObjectId(hex);
      expect(id.toString()).toBe(hex);
    });

    it('should extract timestamp', () => {
      const id = new ObjectId();
      const timestamp = id.getTimestamp();
      expect(timestamp).toBeInstanceOf(Date);
      expect(Math.abs(timestamp.getTime() - Date.now())).toBeLessThan(1000);
    });
  });
});

describe('db() helper', () => {
  it('should provide simple database access', async () => {
    // This test verifies the API shape
    const users = db('test').collection('users');
    expect(users).toBeDefined();
    expect(typeof users.insertOne).toBe('function');
    expect(typeof users.find).toBe('function');
  });
});
