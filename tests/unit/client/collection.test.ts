/**
 * Collection CRUD Tests
 *
 * Tests for Collection class CRUD operations:
 * - insertOne, insertMany
 * - findOne, find
 * - updateOne, updateMany, replaceOne
 * - deleteOne, deleteMany
 * - countDocuments, distinct
 */

import { describe, it, expect } from 'vitest';
import { FindCursor } from '../../../src/client/index.js';
import { createTestCollection } from './test-helpers.js';

describe('Collection', () => {
  describe('insertOne()', () => {
    it('should insert a document and return result', async () => {
      const { collection } = createTestCollection();

      const result = await collection.insertOne({ name: 'Alice', age: 30 });

      expect(result.acknowledged).toBe(true);
      expect(result.insertedId).toBeDefined();
    });

    it('should generate _id if not provided', async () => {
      const { collection } = createTestCollection();

      const result = await collection.insertOne({ name: 'Bob' });

      expect(result.insertedId).toBeDefined();
      expect(typeof result.insertedId).toBe('string');
    });

    it('should use provided _id', async () => {
      const { collection } = createTestCollection();

      const result = await collection.insertOne({ _id: 'custom-id', name: 'Charlie' });

      expect(result.insertedId).toBe('custom-id');
    });

    it('should be retrievable after insert', async () => {
      const { collection } = createTestCollection();

      await collection.insertOne({ _id: 'test-1', name: 'Alice', age: 30 });
      const doc = await collection.findOne({ _id: 'test-1' });

      expect(doc).not.toBeNull();
      expect(doc?.name).toBe('Alice');
      expect(doc?.age).toBe(30);
    });
  });

  describe('insertMany()', () => {
    it('should insert multiple documents', async () => {
      const { collection } = createTestCollection();

      const result = await collection.insertMany([
        { name: 'Alice' },
        { name: 'Bob' },
        { name: 'Charlie' },
      ]);

      expect(result.acknowledged).toBe(true);
      expect(result.insertedCount).toBe(3);
      expect(Object.keys(result.insertedIds)).toHaveLength(3);
    });

    it('should generate unique ids for each document', async () => {
      const { collection } = createTestCollection();

      const result = await collection.insertMany([{ name: 'A' }, { name: 'B' }, { name: 'C' }]);

      const ids = Object.values(result.insertedIds);
      const uniqueIds = new Set(ids);
      expect(uniqueIds.size).toBe(3);
    });

    it('should preserve provided ids', async () => {
      const { collection } = createTestCollection();

      const result = await collection.insertMany([
        { _id: 'id-1', name: 'A' },
        { _id: 'id-2', name: 'B' },
      ]);

      expect(result.insertedIds[0]).toBe('id-1');
      expect(result.insertedIds[1]).toBe('id-2');
    });

    it('should handle empty array gracefully', async () => {
      const { collection } = createTestCollection();
      await expect(collection.insertMany([])).rejects.toThrow();
    });

    it('should be retrievable after insertMany', async () => {
      const { collection } = createTestCollection();

      await collection.insertMany([
        { _id: 'a', name: 'Alice' },
        { _id: 'b', name: 'Bob' },
      ]);

      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(2);
    });
  });

  describe('findOne()', () => {
    it('should return null for empty collection', async () => {
      const { collection } = createTestCollection();
      const doc = await collection.findOne({});
      expect(doc).toBeNull();
    });

    it('should find document by _id', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: 'user-1', name: 'Alice' });

      const doc = await collection.findOne({ _id: 'user-1' });

      expect(doc).not.toBeNull();
      expect(doc?.name).toBe('Alice');
    });

    it('should find document by field equality', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', name: 'Alice', role: 'admin' },
        { _id: '2', name: 'Bob', role: 'user' },
      ]);

      const doc = await collection.findOne({ role: 'admin' });

      expect(doc?.name).toBe('Alice');
    });

    it('should return first matching document', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', status: 'active' },
        { _id: '2', status: 'active' },
      ]);

      const doc = await collection.findOne({ status: 'active' });

      expect(doc).not.toBeNull();
      expect(['1', '2']).toContain(doc?._id);
    });

    it('should return null when no match found', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', name: 'Alice' });

      const doc = await collection.findOne({ name: 'Bob' });

      expect(doc).toBeNull();
    });
  });

  describe('find()', () => {
    it('should return FindCursor', () => {
      const { collection } = createTestCollection();
      const cursor = collection.find({});
      expect(cursor).toBeInstanceOf(FindCursor);
    });

    it('should find all documents with empty filter', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', name: 'A' },
        { _id: '2', name: 'B' },
        { _id: '3', name: 'C' },
      ]);

      const docs = await collection.find({}).toArray();

      expect(docs).toHaveLength(3);
    });

    it('should filter by field value', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', status: 'active' },
        { _id: '2', status: 'inactive' },
        { _id: '3', status: 'active' },
      ]);

      const docs = await collection.find({ status: 'active' }).toArray();

      expect(docs).toHaveLength(2);
      expect(docs.every((d) => d.status === 'active')).toBe(true);
    });

    it('should filter with $gt operator', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', age: 20 },
        { _id: '2', age: 30 },
        { _id: '3', age: 40 },
      ]);

      const docs = await collection.find({ age: { $gt: 25 } }).toArray();

      expect(docs).toHaveLength(2);
      expect(docs.every((d) => (d.age as number) > 25)).toBe(true);
    });

    it('should filter with $gte operator', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', age: 20 },
        { _id: '2', age: 30 },
        { _id: '3', age: 40 },
      ]);

      const docs = await collection.find({ age: { $gte: 30 } }).toArray();

      expect(docs).toHaveLength(2);
    });

    it('should filter with $lt operator', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', age: 20 },
        { _id: '2', age: 30 },
        { _id: '3', age: 40 },
      ]);

      const docs = await collection.find({ age: { $lt: 35 } }).toArray();

      expect(docs).toHaveLength(2);
    });

    it('should filter with $lte operator', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', age: 20 },
        { _id: '2', age: 30 },
        { _id: '3', age: 40 },
      ]);

      const docs = await collection.find({ age: { $lte: 30 } }).toArray();

      expect(docs).toHaveLength(2);
    });

    it('should filter with $in operator', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', status: 'active' },
        { _id: '2', status: 'pending' },
        { _id: '3', status: 'inactive' },
      ]);

      const docs = await collection.find({ status: { $in: ['active', 'pending'] } }).toArray();

      expect(docs).toHaveLength(2);
    });

    it('should filter with $nin operator', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', status: 'active' },
        { _id: '2', status: 'pending' },
        { _id: '3', status: 'inactive' },
      ]);

      const docs = await collection.find({ status: { $nin: ['inactive'] } }).toArray();

      expect(docs).toHaveLength(2);
    });

    it('should filter with $ne operator', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', name: 'Alice' },
        { _id: '2', name: 'Bob' },
        { _id: '3', name: 'Alice' },
      ]);

      const docs = await collection.find({ name: { $ne: 'Alice' } }).toArray();

      expect(docs).toHaveLength(1);
      expect(docs[0].name).toBe('Bob');
    });

    it('should filter with $exists operator', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', name: 'Alice', email: 'alice@test.com' },
        { _id: '2', name: 'Bob' },
      ]);

      const docs = await collection.find({ email: { $exists: true } }).toArray();

      expect(docs).toHaveLength(1);
      expect(docs[0].name).toBe('Alice');
    });

    it('should filter with $and operator', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', age: 30, status: 'active' },
        { _id: '2', age: 25, status: 'active' },
        { _id: '3', age: 30, status: 'inactive' },
      ]);

      const docs = await collection
        .find({
          $and: [{ age: 30 }, { status: 'active' }],
        })
        .toArray();

      expect(docs).toHaveLength(1);
      expect(docs[0]._id).toBe('1');
    });

    it('should filter with $or operator', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', name: 'Alice' },
        { _id: '2', name: 'Bob' },
        { _id: '3', name: 'Charlie' },
      ]);

      const docs = await collection
        .find({
          $or: [{ name: 'Alice' }, { name: 'Charlie' }],
        })
        .toArray();

      expect(docs).toHaveLength(2);
    });

    it('should support multiple conditions on same field', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', age: 20 },
        { _id: '2', age: 30 },
        { _id: '3', age: 40 },
      ]);

      const docs = await collection.find({ age: { $gte: 25, $lte: 35 } }).toArray();

      expect(docs).toHaveLength(1);
      expect(docs[0].age).toBe(30);
    });
  });

  describe('updateOne()', () => {
    it('should update a single document', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', name: 'Alice', age: 30 });

      const result = await collection.updateOne({ _id: '1' }, { $set: { age: 31 } });

      expect(result.acknowledged).toBe(true);
      expect(result.matchedCount).toBe(1);
      expect(result.modifiedCount).toBe(1);

      const doc = await collection.findOne({ _id: '1' });
      expect(doc?.age).toBe(31);
    });

    it('should return matchedCount 0 when no document matches', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', name: 'Alice' });

      const result = await collection.updateOne({ _id: 'nonexistent' }, { $set: { name: 'Bob' } });

      expect(result.matchedCount).toBe(0);
      expect(result.modifiedCount).toBe(0);
    });

    it('should support $set operator', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', name: 'Alice' });

      await collection.updateOne({ _id: '1' }, { $set: { name: 'Alicia', email: 'a@test.com' } });

      const doc = await collection.findOne({ _id: '1' });
      expect(doc?.name).toBe('Alicia');
      expect(doc?.email).toBe('a@test.com');
    });

    it('should support $inc operator', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', views: 10 });

      await collection.updateOne({ _id: '1' }, { $inc: { views: 5 } });

      const doc = await collection.findOne({ _id: '1' });
      expect(doc?.views).toBe(15);
    });

    it('should support $unset operator', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', name: 'Alice', temp: 'value' });

      await collection.updateOne({ _id: '1' }, { $unset: { temp: '' } });

      const doc = await collection.findOne({ _id: '1' });
      expect(doc?.temp).toBeUndefined();
    });

    it('should support $push operator', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', tags: ['a', 'b'] });

      await collection.updateOne({ _id: '1' }, { $push: { tags: 'c' } });

      const doc = await collection.findOne({ _id: '1' });
      expect(doc?.tags).toEqual(['a', 'b', 'c']);
    });

    it('should support $pull operator', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', tags: ['a', 'b', 'c'] });

      await collection.updateOne({ _id: '1' }, { $pull: { tags: 'b' } });

      const doc = await collection.findOne({ _id: '1' });
      expect(doc?.tags).toEqual(['a', 'c']);
    });

    it('should support upsert option - insert new document', async () => {
      const { collection } = createTestCollection();

      const result = await collection.updateOne(
        { _id: 'new-id' },
        { $set: { name: 'New User' } },
        { upsert: true }
      );

      expect(result.upsertedCount).toBe(1);
      expect(result.upsertedId).toBeDefined();

      const doc = await collection.findOne({ name: 'New User' });
      expect(doc).not.toBeNull();
    });

    it('should support upsert option - update existing document', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', name: 'Alice' });

      const result = await collection.updateOne(
        { _id: '1' },
        { $set: { name: 'Alicia' } },
        { upsert: true }
      );

      expect(result.matchedCount).toBe(1);
      expect(result.modifiedCount).toBe(1);
      expect(result.upsertedCount).toBe(0);
    });

    it('should include filter equality fields in upserted document', async () => {
      const { collection } = createTestCollection();

      await collection.updateOne(
        { category: 'electronics', brand: 'Acme' },
        { $set: { price: 99.99 } },
        { upsert: true }
      );

      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(1);
      const doc = docs[0];
      expect(doc.category).toBe('electronics');
      expect(doc.brand).toBe('Acme');
      expect(doc.price).toBe(99.99);
    });

    it('should handle $eq operator in filter for upsert', async () => {
      const { collection } = createTestCollection();

      await collection.updateOne(
        { status: { $eq: 'active' } },
        { $set: { count: 1 } },
        { upsert: true }
      );

      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(1);
      expect(docs[0].status).toBe('active');
      expect(docs[0].count).toBe(1);
    });

    it('should not include comparison operators in upserted document', async () => {
      const { collection } = createTestCollection();

      await collection.updateOne(
        { age: { $gt: 18 }, status: 'active' },
        { $set: { name: 'User' } },
        { upsert: true }
      );

      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(1);
      // status equality should be included
      expect(docs[0].status).toBe('active');
      // age with $gt should NOT be included
      expect(docs[0].age).toBeUndefined();
      expect(docs[0].name).toBe('User');
    });

    it('should handle $set overriding filter fields in upsert', async () => {
      const { collection } = createTestCollection();

      await collection.updateOne(
        { name: 'Original' },
        { $set: { name: 'Updated', extra: 'field' } },
        { upsert: true }
      );

      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(1);
      // $set should override the filter value
      expect(docs[0].name).toBe('Updated');
      expect(docs[0].extra).toBe('field');
    });

    it('should handle $inc on new field in upsert', async () => {
      const { collection } = createTestCollection();

      await collection.updateOne(
        { name: 'Counter' },
        { $inc: { count: 5 } },
        { upsert: true }
      );

      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(1);
      expect(docs[0].name).toBe('Counter');
      expect(docs[0].count).toBe(5);
    });

    it('should apply $setOnInsert only during upsert insert', async () => {
      const { collection } = createTestCollection();

      // Upsert insert - $setOnInsert should be applied
      await collection.updateOne(
        { name: 'NewUser' },
        { $set: { status: 'active' }, $setOnInsert: { createdAt: '2024-01-01' } },
        { upsert: true }
      );

      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(1);
      expect(docs[0].name).toBe('NewUser');
      expect(docs[0].status).toBe('active');
      expect(docs[0].createdAt).toBe('2024-01-01');
    });

    it('should NOT apply $setOnInsert when updating existing document', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', name: 'ExistingUser', status: 'inactive' });

      // Upsert update (document exists) - $setOnInsert should NOT be applied
      await collection.updateOne(
        { _id: '1' },
        { $set: { status: 'active' }, $setOnInsert: { createdAt: '2024-01-01' } },
        { upsert: true }
      );

      const doc = await collection.findOne({ _id: '1' });
      expect(doc).not.toBeNull();
      expect(doc!.status).toBe('active');
      // createdAt should NOT be set because document already existed
      expect(doc!.createdAt).toBeUndefined();
    });

    it('should allow $set to override $setOnInsert fields', async () => {
      const { collection } = createTestCollection();

      await collection.updateOne(
        { name: 'User' },
        { $setOnInsert: { role: 'guest' }, $set: { role: 'admin' } },
        { upsert: true }
      );

      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(1);
      // $set should override $setOnInsert
      expect(docs[0].role).toBe('admin');
    });

    it('should handle nested fields in $setOnInsert', async () => {
      const { collection } = createTestCollection();

      await collection.updateOne(
        { username: 'john' },
        { $setOnInsert: { 'profile.level': 1, 'profile.verified': false } },
        { upsert: true }
      );

      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(1);
      expect(docs[0].username).toBe('john');
      expect((docs[0] as { profile?: { level?: number; verified?: boolean } }).profile?.level).toBe(1);
      expect((docs[0] as { profile?: { level?: number; verified?: boolean } }).profile?.verified).toBe(false);
    });
  });

  describe('updateMany()', () => {
    it('should update multiple documents matching filter', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: 'um-1', status: 'pending', processed: false },
        { _id: 'um-2', status: 'pending', processed: false },
      ]);

      const result = await collection.updateMany(
        { status: 'pending' },
        { $set: { processed: true } }
      );

      expect(result.matchedCount).toBe(2);
      expect(result.modifiedCount).toBe(2);

      const processedDocs = await collection.find({ processed: true }).toArray();
      expect(processedDocs).toHaveLength(2);
    });

    it('should not update documents not matching filter', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: 'um-no-update', status: 'done', value: 'original' },
        { _id: 'um-update', status: 'pending', value: 'original' },
      ]);

      await collection.updateMany({ status: 'pending' }, { $set: { value: 'updated' } });

      const doneDoc = await collection.findOne({ _id: 'um-no-update' });
      expect(doneDoc).not.toBeNull();
      expect(doneDoc?.value).toBe('original');
    });

    it('should return matchedCount 0 when no documents match', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', status: 'active' });

      const result = await collection.updateMany(
        { status: 'pending' },
        { $set: { processed: true } }
      );

      expect(result.matchedCount).toBe(0);
      expect(result.modifiedCount).toBe(0);
    });

    it('should support upsert option', async () => {
      const { collection } = createTestCollection();

      const result = await collection.updateMany(
        { category: 'new' },
        { $set: { count: 0 } },
        { upsert: true }
      );

      expect(result.upsertedCount).toBe(1);
    });

    it('should include filter equality fields in upserted document', async () => {
      const { collection } = createTestCollection();

      await collection.updateMany(
        { type: 'report', department: 'sales' },
        { $set: { processed: false } },
        { upsert: true }
      );

      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(1);
      expect(docs[0].type).toBe('report');
      expect(docs[0].department).toBe('sales');
      expect(docs[0].processed).toBe(false);
    });

    it('should only create one document even with updateMany upsert', async () => {
      const { collection } = createTestCollection();

      const result = await collection.updateMany(
        { status: 'new' },
        { $set: { count: 0 } },
        { upsert: true }
      );

      expect(result.upsertedCount).toBe(1);
      expect(result.matchedCount).toBe(0);

      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(1);
    });
  });

  describe('replaceOne()', () => {
    it('should replace entire document', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', name: 'Alice', age: 30, email: 'a@test.com' });

      const result = await collection.replaceOne({ _id: '1' }, { name: 'Bob', age: 25 });

      expect(result.matchedCount).toBe(1);
      expect(result.modifiedCount).toBe(1);

      const doc = await collection.findOne({ _id: '1' });
      expect(doc?.name).toBe('Bob');
      expect(doc?.age).toBe(25);
      expect(doc?.email).toBeUndefined();
    });

    it('should preserve _id', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', name: 'Alice' });

      await collection.replaceOne({ _id: '1' }, { name: 'Bob' });

      const doc = await collection.findOne({ _id: '1' });
      expect(doc?._id).toBe('1');
    });

    it('should support upsert option', async () => {
      const { collection } = createTestCollection();

      const result = await collection.replaceOne(
        { _id: 'new' },
        { name: 'New User' },
        { upsert: true }
      );

      expect(result.upsertedCount).toBe(1);
    });
  });

  describe('deleteOne()', () => {
    it('should delete a single document', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: 'delete-test-1', name: 'ToDelete' });

      const beforeDoc = await collection.findOne({ _id: 'delete-test-1' });
      expect(beforeDoc).not.toBeNull();

      const result = await collection.deleteOne({ _id: 'delete-test-1' });

      expect(result.acknowledged).toBe(true);
      expect(result.deletedCount).toBe(1);

      const afterDoc = await collection.findOne({ _id: 'delete-test-1' });
      expect(afterDoc).toBeNull();
    });

    it('should keep other documents when deleting one', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: 'keep-1', name: 'Keeper' });
      await collection.insertOne({ _id: 'delete-1', name: 'ToDelete' });

      await collection.deleteOne({ _id: 'delete-1' });

      const keptDoc = await collection.findOne({ _id: 'keep-1' });
      expect(keptDoc).not.toBeNull();
      expect(keptDoc?.name).toBe('Keeper');
    });

    it('should return deletedCount 0 when no match', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', name: 'Alice' });

      const result = await collection.deleteOne({ _id: 'nonexistent' });

      expect(result.deletedCount).toBe(0);
    });

    it('should only delete first matching document', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: 'do-first-1', status: 'matchMe' },
        { _id: 'do-first-2', status: 'matchMe' },
      ]);

      const result = await collection.deleteOne({ status: 'matchMe' });

      expect(result.deletedCount).toBe(1);

      const docs = await collection.find({ status: 'matchMe' }).toArray();
      expect(docs).toHaveLength(1);
    });
  });

  describe('deleteMany()', () => {
    it('should delete multiple documents matching filter', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: 'dm-1', category: 'toDelete' },
        { _id: 'dm-2', category: 'toDelete' },
      ]);

      const result = await collection.deleteMany({ category: 'toDelete' });

      expect(result.acknowledged).toBe(true);
      expect(result.deletedCount).toBe(2);

      const remainingDocs = await collection.find({ category: 'toDelete' }).toArray();
      expect(remainingDocs).toHaveLength(0);
    });

    it('should keep documents not matching filter', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: 'dm-keep', status: 'keep' },
        { _id: 'dm-del-1', status: 'toDelete' },
        { _id: 'dm-del-2', status: 'toDelete' },
      ]);

      await collection.deleteMany({ status: 'toDelete' });

      const keptDoc = await collection.findOne({ _id: 'dm-keep' });
      expect(keptDoc).not.toBeNull();
      expect(keptDoc?.status).toBe('keep');
    });

    it('should return deletedCount 0 when no match', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', status: 'done' });

      const result = await collection.deleteMany({ status: 'pending' });

      expect(result.deletedCount).toBe(0);
    });

    it('should delete all documents with empty filter', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', name: 'A' },
        { _id: '2', name: 'B' },
        { _id: '3', name: 'C' },
      ]);

      const result = await collection.deleteMany({});

      expect(result.deletedCount).toBe(3);

      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(0);
    });
  });

  describe('countDocuments()', () => {
    it('should return 0 for empty collection', async () => {
      const { collection } = createTestCollection();
      const count = await collection.countDocuments();
      expect(count).toBe(0);
    });

    it('should count all documents without filter', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([{ _id: '1' }, { _id: '2' }, { _id: '3' }]);

      const count = await collection.countDocuments();

      expect(count).toBe(3);
    });

    it('should count documents matching filter', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', status: 'active' },
        { _id: '2', status: 'inactive' },
        { _id: '3', status: 'active' },
      ]);

      const count = await collection.countDocuments({ status: 'active' });

      expect(count).toBe(2);
    });
  });

  describe('distinct()', () => {
    it('should return empty array for empty collection', async () => {
      const { collection } = createTestCollection();
      const values = await collection.distinct('status');
      expect(values).toEqual([]);
    });

    it('should return distinct values', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', status: 'active' },
        { _id: '2', status: 'pending' },
        { _id: '3', status: 'active' },
        { _id: '4', status: 'done' },
      ]);

      const values = await collection.distinct('status');

      expect(values).toHaveLength(3);
      expect(values).toContain('active');
      expect(values).toContain('pending');
      expect(values).toContain('done');
    });

    it('should apply filter before getting distinct', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', type: 'A', category: 'cat1' },
        { _id: '2', type: 'B', category: 'cat1' },
        { _id: '3', type: 'A', category: 'cat2' },
      ]);

      const values = await collection.distinct('type', { category: 'cat1' });

      expect(values).toHaveLength(2);
      expect(values).toContain('A');
      expect(values).toContain('B');
    });
  });
});
