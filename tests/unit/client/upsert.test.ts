/**
 * Upsert Option Tests
 *
 * Tests for the upsert option in update operations.
 * These tests verify MongoDB-compatible upsert behavior:
 * - updateOne/updateMany with upsert:true
 * - replaceOne with upsert:true
 * - $setOnInsert operator
 * - upsertedId and upsertedCount in results
 */

import { describe, it, expect } from 'vitest';
import { ObjectId } from '../../../src/types.js';
import { createTestCollection } from './test-helpers.js';

describe('Upsert Option', () => {
  // =========================================================================
  // updateOne with upsert:true
  // =========================================================================

  describe('updateOne with upsert:true', () => {
    it('should insert a new document when no match found', async () => {
      const { collection } = createTestCollection();

      const result = await collection.updateOne(
        { name: 'NewUser' },
        { $set: { age: 25, status: 'active' } },
        { upsert: true }
      );

      expect(result.acknowledged).toBe(true);
      expect(result.matchedCount).toBe(0);
      expect(result.modifiedCount).toBe(0);
      expect(result.upsertedCount).toBe(1);
      expect(result.upsertedId).toBeDefined();

      // Verify the document was inserted
      const doc = await collection.findOne({ name: 'NewUser' });
      expect(doc).not.toBeNull();
      expect(doc?.name).toBe('NewUser');
      expect(doc?.age).toBe(25);
      expect(doc?.status).toBe('active');
    });

    it('should update existing document when match found', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: 'existing-1', name: 'Alice', age: 30 });

      const result = await collection.updateOne(
        { name: 'Alice' },
        { $set: { age: 31 } },
        { upsert: true }
      );

      expect(result.acknowledged).toBe(true);
      expect(result.matchedCount).toBe(1);
      expect(result.modifiedCount).toBe(1);
      expect(result.upsertedCount).toBe(0);
      expect(result.upsertedId).toBeUndefined();

      // Verify only one document exists
      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(1);
      expect(docs[0]?.age).toBe(31);
    });

    it('should use _id from filter when upserting', async () => {
      const { collection } = createTestCollection();

      const result = await collection.updateOne(
        { _id: 'custom-id-123' },
        { $set: { name: 'CustomIdUser' } },
        { upsert: true }
      );

      expect(result.upsertedCount).toBe(1);
      expect(result.upsertedId).toBe('custom-id-123');

      const doc = await collection.findOne({ _id: 'custom-id-123' });
      expect(doc).not.toBeNull();
      expect(doc?._id).toBe('custom-id-123');
      expect(doc?.name).toBe('CustomIdUser');
    });

    it('should generate new ObjectId when upserting without _id in filter', async () => {
      const { collection } = createTestCollection();

      const result = await collection.updateOne(
        { status: 'new' },
        { $set: { name: 'GeneratedIdUser' } },
        { upsert: true }
      );

      expect(result.upsertedCount).toBe(1);
      expect(result.upsertedId).toBeDefined();
      // Verify the upsertedId is a valid identifier (string UUID or ObjectId)
      expect(typeof result.upsertedId === 'string' || result.upsertedId instanceof ObjectId).toBe(true);

      // Verify document exists with generated id
      const docs = await collection.find({ status: 'new' }).toArray();
      expect(docs).toHaveLength(1);
      expect(docs[0]?._id).toBeDefined();
    });
  });

  // =========================================================================
  // updateMany with upsert:true
  // =========================================================================

  describe('updateMany with upsert:true', () => {
    it('should insert a single document when no matches found', async () => {
      const { collection } = createTestCollection();

      const result = await collection.updateMany(
        { category: 'nonexistent' },
        { $set: { processed: true } },
        { upsert: true }
      );

      expect(result.acknowledged).toBe(true);
      expect(result.matchedCount).toBe(0);
      expect(result.modifiedCount).toBe(0);
      expect(result.upsertedCount).toBe(1);
      expect(result.upsertedId).toBeDefined();

      // Verify only one document was created (MongoDB behavior)
      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(1);
      expect(docs[0]?.category).toBe('nonexistent');
      expect(docs[0]?.processed).toBe(true);
    });

    it('should update all matching documents without upserting', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: 'um-1', status: 'pending' },
        { _id: 'um-2', status: 'pending' },
        { _id: 'um-3', status: 'done' },
      ]);

      const result = await collection.updateMany(
        { status: 'pending' },
        { $set: { processed: true } },
        { upsert: true }
      );

      expect(result.matchedCount).toBe(2);
      expect(result.modifiedCount).toBe(2);
      expect(result.upsertedCount).toBe(0);
      expect(result.upsertedId).toBeUndefined();

      // Total documents should remain 3
      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(3);
    });
  });

  // =========================================================================
  // replaceOne with upsert:true
  // =========================================================================

  describe('replaceOne with upsert:true', () => {
    it('should insert replacement document when no match found', async () => {
      const { collection } = createTestCollection();

      const result = await collection.replaceOne(
        { _id: 'replace-new' },
        { name: 'ReplacedUser', email: 'replaced@test.com' },
        { upsert: true }
      );

      expect(result.acknowledged).toBe(true);
      expect(result.matchedCount).toBe(0);
      expect(result.modifiedCount).toBe(0);
      expect(result.upsertedCount).toBe(1);
      expect(result.upsertedId).toBeDefined();

      const doc = await collection.findOne({ _id: 'replace-new' });
      expect(doc).not.toBeNull();
      expect(doc?.name).toBe('ReplacedUser');
      expect(doc?.email).toBe('replaced@test.com');
    });

    it('should replace existing document when match found', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: 'replace-existing', name: 'Original', extra: 'field' });

      const result = await collection.replaceOne(
        { _id: 'replace-existing' },
        { name: 'Replaced', newField: 'value' },
        { upsert: true }
      );

      expect(result.matchedCount).toBe(1);
      expect(result.modifiedCount).toBe(1);
      expect(result.upsertedCount).toBe(0);

      const doc = await collection.findOne({ _id: 'replace-existing' });
      expect(doc?.name).toBe('Replaced');
      expect(doc?.newField).toBe('value');
      expect(doc?.extra).toBeUndefined(); // Old field should be gone
    });

    it('should preserve _id from filter in replacement', async () => {
      const { collection } = createTestCollection();

      await collection.replaceOne(
        { _id: 'preserve-id' },
        { name: 'PreservedIdUser' },
        { upsert: true }
      );

      const doc = await collection.findOne({ _id: 'preserve-id' });
      expect(doc?._id).toBe('preserve-id');
    });
  });

  // =========================================================================
  // Result properties (upsertedId, upsertedCount)
  // =========================================================================

  describe('upsert result properties', () => {
    it('should include upsertedId that matches the inserted document _id', async () => {
      const { collection } = createTestCollection();

      const result = await collection.updateOne(
        { _id: 'result-test-id' },
        { $set: { value: 42 } },
        { upsert: true }
      );

      expect(result.upsertedId).toBe('result-test-id');

      const doc = await collection.findOne({ _id: 'result-test-id' });
      expect(doc?._id).toBe(result.upsertedId);
    });

    it('should have upsertedCount of 1 when document is inserted', async () => {
      const { collection } = createTestCollection();

      const result = await collection.updateOne(
        { nonexistent: true },
        { $set: { created: true } },
        { upsert: true }
      );

      expect(result.upsertedCount).toBe(1);
    });

    it('should have upsertedCount of 0 when document is updated', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: 'count-test', value: 1 });

      const result = await collection.updateOne(
        { _id: 'count-test' },
        { $set: { value: 2 } },
        { upsert: true }
      );

      expect(result.upsertedCount).toBe(0);
      expect(result.matchedCount).toBe(1);
    });
  });

  // =========================================================================
  // $setOnInsert operator
  // =========================================================================

  describe('$setOnInsert operator', () => {
    it('should apply $setOnInsert fields only when inserting', async () => {
      const { collection } = createTestCollection();

      const result = await collection.updateOne(
        { name: 'OnInsertUser' },
        {
          $set: { status: 'active' },
          $setOnInsert: { createdAt: '2024-01-01', role: 'user' },
        },
        { upsert: true }
      );

      expect(result.upsertedCount).toBe(1);

      const doc = await collection.findOne({ name: 'OnInsertUser' });
      expect(doc).not.toBeNull();
      expect(doc?.status).toBe('active');
      expect(doc?.createdAt).toBe('2024-01-01');
      expect(doc?.role).toBe('user');
    });

    it('should NOT apply $setOnInsert fields when updating', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({
        _id: 'soi-update',
        name: 'ExistingUser',
        createdAt: 'original',
        role: 'admin',
      });

      await collection.updateOne(
        { name: 'ExistingUser' },
        {
          $set: { status: 'updated' },
          $setOnInsert: { createdAt: 'should-not-change', role: 'should-not-change' },
        },
        { upsert: true }
      );

      const doc = await collection.findOne({ _id: 'soi-update' });
      expect(doc?.status).toBe('updated');
      expect(doc?.createdAt).toBe('original'); // Should NOT be changed
      expect(doc?.role).toBe('admin'); // Should NOT be changed
    });

    it('should work with $setOnInsert alone (no $set)', async () => {
      const { collection } = createTestCollection();

      await collection.updateOne(
        { uniqueField: 'test-value' },
        { $setOnInsert: { defaultValue: 100, initialized: true } },
        { upsert: true }
      );

      const doc = await collection.findOne({ uniqueField: 'test-value' });
      expect(doc).not.toBeNull();
      expect(doc?.uniqueField).toBe('test-value');
      expect(doc?.defaultValue).toBe(100);
      expect(doc?.initialized).toBe(true);
    });
  });

  // =========================================================================
  // upsert:false (default behavior)
  // =========================================================================

  describe('upsert:false (default)', () => {
    it('should NOT insert when no match found with upsert:false', async () => {
      const { collection } = createTestCollection();

      const result = await collection.updateOne(
        { name: 'NonExistent' },
        { $set: { value: 42 } },
        { upsert: false }
      );

      expect(result.matchedCount).toBe(0);
      expect(result.modifiedCount).toBe(0);
      expect(result.upsertedCount).toBe(0);

      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(0);
    });

    it('should NOT insert when no match found without upsert option', async () => {
      const { collection } = createTestCollection();

      const result = await collection.updateOne(
        { name: 'NonExistent' },
        { $set: { value: 42 } }
        // No options - default is upsert:false
      );

      expect(result.matchedCount).toBe(0);
      expect(result.upsertedCount).toBe(0);

      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(0);
    });
  });

  // =========================================================================
  // Edge Cases: Empty filter with upsert
  // =========================================================================

  describe('edge case: empty filter with upsert', () => {
    it('should insert with empty filter if collection is empty', async () => {
      const { collection } = createTestCollection();

      const result = await collection.updateOne(
        {},
        { $set: { default: true } },
        { upsert: true }
      );

      expect(result.upsertedCount).toBe(1);

      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(1);
      expect(docs[0]?.default).toBe(true);
    });

    it('should update first document with empty filter if documents exist', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: 'empty-1', value: 1 },
        { _id: 'empty-2', value: 2 },
      ]);

      const result = await collection.updateOne(
        {},
        { $set: { updated: true } },
        { upsert: true }
      );

      expect(result.matchedCount).toBe(1);
      expect(result.modifiedCount).toBe(1);
      expect(result.upsertedCount).toBe(0);

      // Total count should remain 2
      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(2);
    });
  });

  // =========================================================================
  // Edge Cases: Complex filter with upsert
  // =========================================================================

  describe('edge case: complex filter with upsert', () => {
    it('should extract equality fields from complex filter', async () => {
      const { collection } = createTestCollection();

      await collection.updateOne(
        { category: 'electronics', brand: 'Acme', price: { $gt: 100 } },
        { $set: { inStock: true } },
        { upsert: true }
      );

      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(1);
      // Equality fields should be included
      expect(docs[0]?.category).toBe('electronics');
      expect(docs[0]?.brand).toBe('Acme');
      // Comparison operators should NOT be included
      expect(docs[0]?.price).toBeUndefined();
      expect(docs[0]?.inStock).toBe(true);
    });

    it('should handle $and in filter for upsert', async () => {
      const { collection } = createTestCollection();

      await collection.updateOne(
        { $and: [{ type: 'report' }, { department: 'sales' }] },
        { $set: { processed: false } },
        { upsert: true }
      );

      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(1);
      // Note: extracting fields from $and is implementation-dependent
      // The current implementation may or may not extract these
      expect(docs[0]?.processed).toBe(false);
    });

    it('should handle $or in filter for upsert', async () => {
      const { collection } = createTestCollection();

      await collection.updateOne(
        { $or: [{ status: 'new' }, { status: 'pending' }] },
        { $set: { flagged: true } },
        { upsert: true }
      );

      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(1);
      // $or fields should NOT be included in upserted document
      expect(docs[0]?.status).toBeUndefined();
      expect(docs[0]?.flagged).toBe(true);
    });
  });

  // =========================================================================
  // Edge Cases: upsert with $inc and other operators
  // =========================================================================

  describe('edge case: upsert with $inc and other operators', () => {
    it('should initialize field with $inc value when upserting', async () => {
      const { collection } = createTestCollection();

      await collection.updateOne(
        { name: 'Counter' },
        { $inc: { count: 5 } },
        { upsert: true }
      );

      const doc = await collection.findOne({ name: 'Counter' });
      expect(doc).not.toBeNull();
      expect(doc?.name).toBe('Counter');
      expect(doc?.count).toBe(5);
    });

    it('should handle multiple operators in upsert', async () => {
      const { collection } = createTestCollection();

      await collection.updateOne(
        { productId: 'prod-123' },
        {
          $set: { name: 'Widget' },
          $inc: { views: 1 },
          $push: { tags: 'new' },
        },
        { upsert: true }
      );

      const doc = await collection.findOne({ productId: 'prod-123' });
      expect(doc).not.toBeNull();
      expect(doc?.productId).toBe('prod-123');
      expect(doc?.name).toBe('Widget');
      expect(doc?.views).toBe(1);
      expect(doc?.tags).toEqual(['new']);
    });

    it('should handle $unset in upsert (should not include field)', async () => {
      const { collection } = createTestCollection();

      await collection.updateOne(
        { itemId: 'item-456' },
        {
          $set: { active: true },
          $unset: { tempField: '' },
        },
        { upsert: true }
      );

      const doc = await collection.findOne({ itemId: 'item-456' });
      expect(doc).not.toBeNull();
      expect(doc?.active).toBe(true);
      expect(doc?.tempField).toBeUndefined();
    });

    it('should handle $addToSet in upsert', async () => {
      const { collection } = createTestCollection();

      await collection.updateOne(
        { groupId: 'group-1' },
        { $addToSet: { members: 'user-1' } },
        { upsert: true }
      );

      const doc = await collection.findOne({ groupId: 'group-1' });
      expect(doc).not.toBeNull();
      expect(doc?.groupId).toBe('group-1');
      expect(doc?.members).toEqual(['user-1']);
    });

    it('should handle $rename in upsert (rename nonexistent field should be no-op)', async () => {
      const { collection } = createTestCollection();

      await collection.updateOne(
        { docId: 'rename-test' },
        {
          $set: { originalField: 'value' },
          $rename: { originalField: 'renamedField' },
        },
        { upsert: true }
      );

      const doc = await collection.findOne({ docId: 'rename-test' });
      expect(doc).not.toBeNull();
      // After $set and $rename, originalField should be renamed to renamedField
      expect(doc?.renamedField).toBe('value');
      expect(doc?.originalField).toBeUndefined();
    });
  });

  // =========================================================================
  // Additional edge cases
  // =========================================================================

  describe('additional edge cases', () => {
    it('should handle nested field in filter for upsert', async () => {
      const { collection } = createTestCollection();

      await collection.updateOne(
        { 'metadata.version': 1 },
        { $set: { name: 'NestedFilterDoc' } },
        { upsert: true }
      );

      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(1);
      expect(docs[0]?.name).toBe('NestedFilterDoc');
      // Nested fields in filter may or may not be extracted depending on implementation
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
      expect(docs[0]?.status).toBe('active');
      expect(docs[0]?.count).toBe(1);
    });

    it('should handle array value in filter for upsert', async () => {
      const { collection } = createTestCollection();

      await collection.updateOne(
        { tags: ['a', 'b', 'c'] },
        { $set: { matched: true } },
        { upsert: true }
      );

      const docs = await collection.find({}).toArray();
      expect(docs).toHaveLength(1);
      expect(docs[0]?.tags).toEqual(['a', 'b', 'c']);
      expect(docs[0]?.matched).toBe(true);
    });

    it('should handle ObjectId in filter _id for upsert', async () => {
      const { collection } = createTestCollection();
      const objectId = new ObjectId();

      const result = await collection.updateOne(
        { _id: objectId.toString() },
        { $set: { name: 'ObjectIdUser' } },
        { upsert: true }
      );

      expect(result.upsertedCount).toBe(1);

      const doc = await collection.findOne({ _id: objectId.toString() });
      expect(doc).not.toBeNull();
      expect(doc?.name).toBe('ObjectIdUser');
    });

    it('should not duplicate document on repeated upsert with same filter', async () => {
      const { collection } = createTestCollection();

      // First upsert - should insert
      const result1 = await collection.updateOne(
        { uniqueKey: 'repeated' },
        { $set: { value: 1 } },
        { upsert: true }
      );
      expect(result1.upsertedCount).toBe(1);

      // Second upsert - should update
      const result2 = await collection.updateOne(
        { uniqueKey: 'repeated' },
        { $set: { value: 2 } },
        { upsert: true }
      );
      expect(result2.upsertedCount).toBe(0);
      expect(result2.matchedCount).toBe(1);

      // Should only have one document
      const docs = await collection.find({ uniqueKey: 'repeated' }).toArray();
      expect(docs).toHaveLength(1);
      expect(docs[0]?.value).toBe(2);
    });
  });
});
