/**
 * Tests for Non-_id Index Updates
 *
 * These tests verify that when documents are updated, non-_id indexes
 * are properly maintained by:
 * - Removing old index entries for changed fields
 * - Adding new index entries for updated values
 * - Handling partial updates ($set, $unset, etc.)
 *
 * @see src/client/collection.ts - writeDelta method
 * @see src/index/index-manager.ts - updateDocumentIndexes method
 */

import { describe, it, expect } from 'vitest';
import { createTestCollection } from './test-helpers.js';

describe('Non-_id Index Updates', () => {
  describe('Single Field Index Updates', () => {
    it('should update single field index when indexed field changes', async () => {
      const { collection } = createTestCollection();

      // Create index on 'email' field
      await collection.createIndex({ email: 1 });

      // Insert document
      await collection.insertOne({ _id: '1', email: 'old@test.com', name: 'Alice' });

      // Verify initial state - document can be found by old email
      let doc = await collection.findOne({ email: 'old@test.com' });
      expect(doc).not.toBeNull();
      expect(doc?.email).toBe('old@test.com');

      // Update the email
      await collection.updateOne({ _id: '1' }, { $set: { email: 'new@test.com' } });

      // Verify old email no longer finds the document
      doc = await collection.findOne({ email: 'old@test.com' });
      expect(doc).toBeNull();

      // Verify new email finds the document
      doc = await collection.findOne({ email: 'new@test.com' });
      expect(doc).not.toBeNull();
      expect(doc?.email).toBe('new@test.com');
    });

    it('should not modify index when non-indexed field changes', async () => {
      const { collection } = createTestCollection();

      // Create index on 'email' field
      await collection.createIndex({ email: 1 });

      // Insert document
      await collection.insertOne({ _id: '1', email: 'test@test.com', name: 'Old Name' });

      // Update only the name field (non-indexed)
      await collection.updateOne({ _id: '1' }, { $set: { name: 'New Name' } });

      // Verify email index still works
      const doc = await collection.findOne({ email: 'test@test.com' });
      expect(doc).not.toBeNull();
      expect(doc?.name).toBe('New Name');
    });

    it('should handle setting indexed field to null', async () => {
      const { collection } = createTestCollection();

      // Create index on 'email' field
      await collection.createIndex({ email: 1 });

      // Insert document with email
      await collection.insertOne({ _id: '1', email: 'test@test.com' });

      // Verify initial lookup works
      let doc = await collection.findOne({ email: 'test@test.com' });
      expect(doc).not.toBeNull();

      // Set email to null
      await collection.updateOne({ _id: '1' }, { $set: { email: null } });

      // Old value should not be in index anymore
      doc = await collection.findOne({ email: 'test@test.com' });
      expect(doc).toBeNull();

      // Document should still exist and have null email
      doc = await collection.findOne({ _id: '1' });
      expect(doc).not.toBeNull();
      expect(doc?.email).toBeNull();
    });

    it('should handle unsetting indexed field', async () => {
      const { collection } = createTestCollection();

      // Create index on 'email' field
      await collection.createIndex({ email: 1 });

      // Insert document with email
      await collection.insertOne({ _id: '1', email: 'test@test.com' });

      // Verify initial lookup works
      let doc = await collection.findOne({ email: 'test@test.com' });
      expect(doc).not.toBeNull();

      // Unset the email field
      await collection.updateOne({ _id: '1' }, { $unset: { email: '' } });

      // Old value should not be in index anymore
      doc = await collection.findOne({ email: 'test@test.com' });
      expect(doc).toBeNull();

      // Document should still exist without email field
      doc = await collection.findOne({ _id: '1' });
      expect(doc).not.toBeNull();
      expect(doc?.email).toBeUndefined();
    });
  });

  describe('Compound Index Updates', () => {
    it('should update compound index when first field changes', async () => {
      const { collection } = createTestCollection();

      // Create compound index on ['firstName', 'lastName']
      await collection.createIndex({ firstName: 1, lastName: 1 });

      // Insert document
      await collection.insertOne({ _id: '1', firstName: 'John', lastName: 'Doe' });

      // Verify initial lookup
      let doc = await collection.findOne({ firstName: 'John', lastName: 'Doe' });
      expect(doc).not.toBeNull();

      // Update first name
      await collection.updateOne({ _id: '1' }, { $set: { firstName: 'Jane' } });

      // Old compound key should not find document
      doc = await collection.findOne({ firstName: 'John', lastName: 'Doe' });
      expect(doc).toBeNull();

      // New compound key should find document
      doc = await collection.findOne({ firstName: 'Jane', lastName: 'Doe' });
      expect(doc).not.toBeNull();
    });

    it('should update compound index when second field changes', async () => {
      const { collection } = createTestCollection();

      // Create compound index
      await collection.createIndex({ firstName: 1, lastName: 1 });

      // Insert document
      await collection.insertOne({ _id: '1', firstName: 'John', lastName: 'Doe' });

      // Update last name
      await collection.updateOne({ _id: '1' }, { $set: { lastName: 'Smith' } });

      // Old compound key should not find document
      const oldDoc = await collection.findOne({ firstName: 'John', lastName: 'Doe' });
      expect(oldDoc).toBeNull();

      // New compound key should find document
      const newDoc = await collection.findOne({ firstName: 'John', lastName: 'Smith' });
      expect(newDoc).not.toBeNull();
    });

    it('should update compound index when both fields change', async () => {
      const { collection } = createTestCollection();

      // Create compound index
      await collection.createIndex({ firstName: 1, lastName: 1 });

      // Insert document
      await collection.insertOne({ _id: '1', firstName: 'John', lastName: 'Doe' });

      // Update both fields
      await collection.updateOne({ _id: '1' }, { $set: { firstName: 'Jane', lastName: 'Smith' } });

      // Old compound key should not find document
      const oldDoc = await collection.findOne({ firstName: 'John', lastName: 'Doe' });
      expect(oldDoc).toBeNull();

      // New compound key should find document
      const newDoc = await collection.findOne({ firstName: 'Jane', lastName: 'Smith' });
      expect(newDoc).not.toBeNull();
    });
  });

  describe('Multiple Index Updates', () => {
    it('should update all affected indexes on document update', async () => {
      const { collection } = createTestCollection();

      // Create multiple indexes
      await collection.createIndex({ email: 1 });
      await collection.createIndex({ phone: 1 });

      // Insert document
      await collection.insertOne({
        _id: '1',
        email: 'old@test.com',
        phone: '111-111-1111',
      });

      // Update both indexed fields
      await collection.updateOne({ _id: '1' }, {
        $set: { email: 'new@test.com', phone: '222-222-2222' },
      });

      // Old values should not find document
      expect(await collection.findOne({ email: 'old@test.com' })).toBeNull();
      expect(await collection.findOne({ phone: '111-111-1111' })).toBeNull();

      // New values should find document
      expect(await collection.findOne({ email: 'new@test.com' })).not.toBeNull();
      expect(await collection.findOne({ phone: '222-222-2222' })).not.toBeNull();
    });

    it('should handle update affecting some but not all indexes', async () => {
      const { collection } = createTestCollection();

      // Create indexes on email, phone, and name
      await collection.createIndex({ email: 1 });
      await collection.createIndex({ phone: 1 });
      await collection.createIndex({ name: 1 });

      // Insert document
      await collection.insertOne({
        _id: '1',
        email: 'test@test.com',
        phone: '111-111-1111',
        name: 'Alice',
      });

      // Update only email and name (phone unchanged)
      await collection.updateOne({ _id: '1' }, {
        $set: { email: 'new@test.com', name: 'Bob' },
      });

      // Phone index should still work (unchanged)
      const byPhone = await collection.findOne({ phone: '111-111-1111' });
      expect(byPhone).not.toBeNull();
      expect(byPhone?.name).toBe('Bob');

      // Email and name indexes should be updated
      expect(await collection.findOne({ email: 'test@test.com' })).toBeNull();
      expect(await collection.findOne({ email: 'new@test.com' })).not.toBeNull();
      expect(await collection.findOne({ name: 'Alice' })).toBeNull();
      expect(await collection.findOne({ name: 'Bob' })).not.toBeNull();
    });
  });

  describe('Update Operators and Index Handling', () => {
    it('should handle $set operator updating indexed field', async () => {
      const { collection } = createTestCollection();

      await collection.createIndex({ status: 1 });
      await collection.insertOne({ _id: '1', status: 'pending' });

      await collection.updateOne({ _id: '1' }, { $set: { status: 'active' } });

      expect(await collection.findOne({ status: 'pending' })).toBeNull();
      expect(await collection.findOne({ status: 'active' })).not.toBeNull();
    });

    it('should handle $inc operator on indexed numeric field', async () => {
      const { collection } = createTestCollection();

      await collection.createIndex({ count: 1 });
      await collection.insertOne({ _id: '1', count: 5 });

      // Verify initial value is indexed
      let doc = await collection.findOne({ count: 5 });
      expect(doc).not.toBeNull();

      // Increment the count
      await collection.updateOne({ _id: '1' }, { $inc: { count: 3 } });

      // Old value should not find document
      doc = await collection.findOne({ count: 5 });
      expect(doc).toBeNull();

      // New value should find document
      doc = await collection.findOne({ count: 8 });
      expect(doc).not.toBeNull();
    });

    it('should handle $push on indexed array field (multikey index)', async () => {
      const { collection } = createTestCollection();

      await collection.createIndex({ tags: 1 });
      await collection.insertOne({ _id: '1', tags: ['nodejs', 'typescript'] });

      // Push a new tag
      await collection.updateOne({ _id: '1' }, { $push: { tags: 'mongodb' } });

      // Verify document was updated correctly
      const doc = await collection.findOne({ _id: '1' });
      expect(doc).not.toBeNull();
      expect(doc?.tags).toEqual(['nodejs', 'typescript', 'mongodb']);

      // Verify index is updated correctly by accessing it directly
      // @ts-expect-error - accessing private property for testing
      const indexManager = collection.indexManager;
      const index = await indexManager.getIndex('tags_1');
      expect(index).not.toBeUndefined();

      // All three tags should be indexed
      expect(index!.search('nodejs')).toContain('1');
      expect(index!.search('typescript')).toContain('1');
      expect(index!.search('mongodb')).toContain('1');
    });

    it('should handle $pull on indexed array field', async () => {
      const { collection } = createTestCollection();

      await collection.createIndex({ tags: 1 });
      await collection.insertOne({ _id: '1', tags: ['nodejs', 'typescript', 'oldTag'] });

      // Pull oldTag
      await collection.updateOne({ _id: '1' }, { $pull: { tags: 'oldTag' } });

      // Verify document was updated
      const doc = await collection.findOne({ _id: '1' });
      expect(doc).not.toBeNull();
      expect(doc?.tags).toEqual(['nodejs', 'typescript']);

      // Verify index is updated correctly
      // @ts-expect-error - accessing private property for testing
      const indexManager = collection.indexManager;
      const index = await indexManager.getIndex('tags_1');

      // Removed tag should not be in index
      expect(index!.search('oldTag')).not.toContain('1');

      // Remaining tags should still be in index
      expect(index!.search('nodejs')).toContain('1');
      expect(index!.search('typescript')).toContain('1');
    });
  });

  describe('updateMany Index Updates', () => {
    it('should update indexes for all matched documents in updateMany', async () => {
      const { collection } = createTestCollection();

      await collection.createIndex({ status: 1 });

      // Insert multiple documents with status: 'pending'
      await collection.insertMany([
        { _id: '1', status: 'pending', name: 'Doc1' },
        { _id: '2', status: 'pending', name: 'Doc2' },
        { _id: '3', status: 'pending', name: 'Doc3' },
        { _id: '4', status: 'active', name: 'Doc4' },
      ]);

      // Update all pending to completed
      const result = await collection.updateMany(
        { status: 'pending' },
        { $set: { status: 'completed' } }
      );

      expect(result.matchedCount).toBe(3);
      expect(result.modifiedCount).toBe(3);

      // No more pending documents
      const pending = await collection.find({ status: 'pending' }).toArray();
      expect(pending).toHaveLength(0);

      // Three completed documents
      const completed = await collection.find({ status: 'completed' }).toArray();
      expect(completed).toHaveLength(3);

      // Active document unchanged
      const active = await collection.find({ status: 'active' }).toArray();
      expect(active).toHaveLength(1);
    });
  });

  describe('replaceOne Index Updates', () => {
    it('should update indexes when replacing a document', async () => {
      const { collection } = createTestCollection();

      await collection.createIndex({ email: 1 });
      await collection.createIndex({ age: 1 });

      // Insert document
      await collection.insertOne({ _id: '1', email: 'old@test.com', age: 25 });

      // Replace entire document
      await collection.replaceOne(
        { _id: '1' },
        { email: 'new@test.com', age: 30 }
      );

      // Old values should not find document
      expect(await collection.findOne({ email: 'old@test.com' })).toBeNull();
      expect(await collection.findOne({ age: 25 })).toBeNull();

      // New values should find document
      expect(await collection.findOne({ email: 'new@test.com' })).not.toBeNull();
      expect(await collection.findOne({ age: 30 })).not.toBeNull();
    });
  });

  describe('Unique Index Constraint Enforcement', () => {
    it('should throw on update that would violate unique index', async () => {
      const { collection } = createTestCollection();

      // Create unique index on email
      await collection.createIndex({ email: 1 }, { unique: true });

      // Insert two documents with different emails
      await collection.insertOne({ _id: '1', email: 'alice@test.com' });
      await collection.insertOne({ _id: '2', email: 'bob@test.com' });

      // Try to update bob's email to alice's - should throw
      // Note: Due to current implementation, the parquet data may be written
      // before the index check fails, so this tests that an error IS thrown
      await expect(
        collection.updateOne({ _id: '2' }, { $set: { email: 'alice@test.com' } })
      ).rejects.toThrow(/[Dd]uplicate/);
    });

    it('should allow update to same value on unique indexed field', async () => {
      const { collection } = createTestCollection();

      // Create unique index on email
      await collection.createIndex({ email: 1 }, { unique: true });

      // Insert document
      await collection.insertOne({ _id: '1', email: 'alice@test.com' });

      // Update document to set email to its current value (should succeed)
      // Since the value hasn't changed, valuesEqual() returns true and
      // no index update is needed
      await expect(
        collection.updateOne({ _id: '1' }, { $set: { email: 'alice@test.com' } })
      ).resolves.not.toThrow();

      const doc = await collection.findOne({ _id: '1' });
      expect(doc?.email).toBe('alice@test.com');
    });
  });

  describe('Sparse Index Updates', () => {
    it('should add to sparse index when indexed field is added', async () => {
      const { collection } = createTestCollection();

      // Create sparse index on optional field
      await collection.createIndex({ optionalField: 1 }, { sparse: true });

      // Insert document without the optional field
      await collection.insertOne({ _id: '1', name: 'Test' });

      // Document should not be found by optionalField (not in sparse index)
      let doc = await collection.findOne({ optionalField: 'value' });
      expect(doc).toBeNull();

      // Add the optional field
      await collection.updateOne({ _id: '1' }, { $set: { optionalField: 'value' } });

      // Now document should be in sparse index
      doc = await collection.findOne({ optionalField: 'value' });
      expect(doc).not.toBeNull();
    });

    it('should remove from sparse index when indexed field is removed', async () => {
      const { collection } = createTestCollection();

      // Create sparse index on optional field
      await collection.createIndex({ optionalField: 1 }, { sparse: true });

      // Insert document with the optional field
      await collection.insertOne({ _id: '1', optionalField: 'value' });

      // Document should be found
      let doc = await collection.findOne({ optionalField: 'value' });
      expect(doc).not.toBeNull();

      // Remove the optional field
      await collection.updateOne({ _id: '1' }, { $unset: { optionalField: '' } });

      // Document should no longer be in sparse index
      doc = await collection.findOne({ optionalField: 'value' });
      expect(doc).toBeNull();

      // But document still exists
      doc = await collection.findOne({ _id: '1' });
      expect(doc).not.toBeNull();
    });
  });
});
