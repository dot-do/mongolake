/**
 * Dual-Run CRUD Tests
 *
 * These tests run identical CRUD operations on both MongoLake and real MongoDB,
 * comparing results for exact match. This is the most valuable pattern for
 * finding subtle incompatibilities.
 *
 * Based on FerretDB's compatibility testing pattern.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import {
  DualDatabaseContext,
  setupDualDatabases,
  teardownDualDatabases,
  runOnBoth,
  compareResults,
  formatDiffs,
  assertDualRunMatch,
} from './framework.js';

describe('Dual-Run CRUD Compatibility Tests', () => {
  let context: DualDatabaseContext;
  const testCollection = 'dual_run_test';

  beforeAll(async () => {
    context = await setupDualDatabases();
  });

  afterAll(async () => {
    await teardownDualDatabases(context);
  });

  beforeEach(async () => {
    // Clean up collection before each test
    await context.dropCollection(testCollection);
  });

  // ==========================================================================
  // Insert Operations
  // ==========================================================================

  describe('Insert Operations', () => {
    it('should insert a single document and return insertedId', async () => {
      const doc = { _id: 'test-1', name: 'Alice', age: 30 };

      const result = await runOnBoth(context, testCollection, async (collection) => {
        const insertResult = await collection.insertOne(doc);
        return { insertedId: String(insertResult.insertedId) };
      });

      expect(result.match).toBe(true);
      if (!result.match) {
        console.error(formatDiffs(compareResults(result.mongolake, result.mongodb)));
      }
    });

    it('should insert multiple documents', async () => {
      const docs = [
        { _id: 'multi-1', name: 'Bob', age: 25 },
        { _id: 'multi-2', name: 'Carol', age: 35 },
        { _id: 'multi-3', name: 'Dave', age: 45 },
      ];

      const result = await runOnBoth(context, testCollection, async (collection) => {
        const insertResult = await collection.insertMany(docs);
        return { insertedCount: insertResult.insertedCount };
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
    });

    it('should insert document with nested objects', async () => {
      const doc = {
        _id: 'nested-1',
        user: {
          profile: {
            firstName: 'Emma',
            lastName: 'Smith',
            address: {
              street: '123 Main St',
              city: 'Boston',
            },
          },
        },
        tags: ['developer', 'team-lead'],
      };

      const result = await runOnBoth(context, testCollection, async (collection) => {
        await collection.insertOne(doc);
        const found = await collection.findOne({ _id: 'nested-1' });
        return found;
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
    });

    it('should insert document with various data types', async () => {
      const doc = {
        _id: 'types-1',
        stringField: 'hello',
        numberInt: 42,
        numberFloat: 3.14159,
        boolTrue: true,
        boolFalse: false,
        nullField: null,
        arrayField: [1, 2, 3],
        nestedArray: [[1, 2], [3, 4]],
        emptyObject: {},
        emptyArray: [],
      };

      const result = await runOnBoth(context, testCollection, async (collection) => {
        await collection.insertOne(doc);
        return await collection.findOne({ _id: 'types-1' });
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
    });
  });

  // ==========================================================================
  // Find Operations
  // ==========================================================================

  describe('Find Operations', () => {
    beforeEach(async () => {
      // Seed test data in both databases
      const seedDocs = [
        { _id: 'find-1', name: 'Alice', age: 30, status: 'active' },
        { _id: 'find-2', name: 'Bob', age: 25, status: 'inactive' },
        { _id: 'find-3', name: 'Carol', age: 35, status: 'active' },
        { _id: 'find-4', name: 'Dave', age: 40, status: 'active' },
        { _id: 'find-5', name: 'Eve', age: 28, status: 'pending' },
      ];

      const { mongolake, mongodb } = context.getCollections(testCollection);
      await mongolake.insertMany(seedDocs);
      await mongodb.insertMany(seedDocs);
    });

    it('should find all documents', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        const docs = await collection.find({});
        return docs.sort((a, b) => String(a._id).localeCompare(String(b._id)));
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
    });

    it('should find by exact field match', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        return await collection.findOne({ name: 'Alice' });
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
    });

    it('should find by _id', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        return await collection.findOne({ _id: 'find-3' });
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
    });

    it('should find with $gt operator', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        const docs = await collection.find({ age: { $gt: 30 } });
        return docs.sort((a, b) => String(a._id).localeCompare(String(b._id)));
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
    });

    it('should find with $gte operator', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        const docs = await collection.find({ age: { $gte: 30 } });
        return docs.sort((a, b) => String(a._id).localeCompare(String(b._id)));
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
    });

    it('should find with $lt operator', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        const docs = await collection.find({ age: { $lt: 30 } });
        return docs.sort((a, b) => String(a._id).localeCompare(String(b._id)));
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
    });

    it('should find with $lte operator', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        const docs = await collection.find({ age: { $lte: 30 } });
        return docs.sort((a, b) => String(a._id).localeCompare(String(b._id)));
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
    });

    it('should find with $ne operator', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        const docs = await collection.find({ status: { $ne: 'active' } });
        return docs.sort((a, b) => String(a._id).localeCompare(String(b._id)));
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
    });

    it('should find with $in operator', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        const docs = await collection.find({ status: { $in: ['active', 'pending'] } });
        return docs.sort((a, b) => String(a._id).localeCompare(String(b._id)));
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
    });

    it('should find with $nin operator', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        const docs = await collection.find({ status: { $nin: ['inactive'] } });
        return docs.sort((a, b) => String(a._id).localeCompare(String(b._id)));
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
    });

    it('should find with multiple conditions (implicit $and)', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        const docs = await collection.find({ status: 'active', age: { $gte: 35 } });
        return docs.sort((a, b) => String(a._id).localeCompare(String(b._id)));
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
    });

    it('should return null for non-existent document', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        return await collection.findOne({ _id: 'non-existent-id' });
      });

      expect(result.match).toBe(true);
      expect(result.mongolake).toBeNull();
      expect(result.mongodb).toBeNull();
    });

    it('should count documents', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        return await collection.countDocuments({ status: 'active' });
      });

      expect(result.match).toBe(true);
      expect(result.mongolake).toBe(3);
      expect(result.mongodb).toBe(3);
    });
  });

  // ==========================================================================
  // Update Operations
  // ==========================================================================

  describe('Update Operations', () => {
    beforeEach(async () => {
      const seedDocs = [
        { _id: 'update-1', name: 'Alice', age: 30, score: 100 },
        { _id: 'update-2', name: 'Bob', age: 25, score: 85 },
        { _id: 'update-3', name: 'Carol', age: 35, score: 90 },
      ];

      const { mongolake, mongodb } = context.getCollections(testCollection);
      await mongolake.insertMany(seedDocs);
      await mongodb.insertMany(seedDocs);
    });

    it('should update a single document with $set', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        const updateResult = await collection.updateOne(
          { _id: 'update-1' },
          { $set: { name: 'Alicia', status: 'updated' } }
        );
        const doc = await collection.findOne({ _id: 'update-1' });
        return {
          matchedCount: updateResult.matchedCount,
          modifiedCount: updateResult.modifiedCount,
          document: doc,
        };
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
    });

    it('should update with $inc operator', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        await collection.updateOne(
          { _id: 'update-1' },
          { $inc: { age: 1, score: 10 } }
        );
        return await collection.findOne({ _id: 'update-1' });
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
      expect(result.mongolake?.age).toBe(31);
      expect(result.mongolake?.score).toBe(110);
    });

    it('should update with $unset operator', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        await collection.updateOne(
          { _id: 'update-1' },
          { $unset: { score: '' } }
        );
        return await collection.findOne({ _id: 'update-1' });
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
      expect(result.mongolake?.score).toBeUndefined();
    });

    it('should update multiple documents', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        const updateResult = await collection.updateMany(
          { age: { $gte: 30 } },
          { $set: { category: 'senior' } }
        );
        const docs = await collection.find({ category: 'senior' });
        return {
          matchedCount: updateResult.matchedCount,
          modifiedCount: updateResult.modifiedCount,
          documentCount: docs.length,
        };
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
      expect(result.mongolake.matchedCount).toBe(2);
    });

    it('should return matchedCount 0 for non-matching filter', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        return await collection.updateOne(
          { _id: 'non-existent' },
          { $set: { name: 'Nobody' } }
        );
      });

      expect(result.match).toBe(true);
      expect(result.mongolake.matchedCount).toBe(0);
      expect(result.mongodb.matchedCount).toBe(0);
    });

    it('should update nested fields with $set', async () => {
      // First insert a document with nested structure
      const { mongolake, mongodb } = context.getCollections(testCollection);
      const doc = { _id: 'nested-update', profile: { name: 'Test', level: 1 } };
      await mongolake.insertOne(doc);
      await mongodb.insertOne(doc);

      const result = await runOnBoth(context, testCollection, async (collection) => {
        await collection.updateOne(
          { _id: 'nested-update' },
          { $set: { 'profile.level': 2, 'profile.badge': 'gold' } }
        );
        return await collection.findOne({ _id: 'nested-update' });
      });

      // Log detailed diff on failure for debugging
      if (!result.match) {
        console.log('=== INCOMPATIBILITY DETECTED ===');
        console.log('MongoLake result:', JSON.stringify(result.mongolake, null, 2));
        console.log('MongoDB result:', JSON.stringify(result.mongodb, null, 2));
        console.log('Differences:', formatDiffs(compareResults(result.mongolake, result.mongodb)));
      }

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
    });
  });

  // ==========================================================================
  // Delete Operations
  // ==========================================================================

  describe('Delete Operations', () => {
    beforeEach(async () => {
      const seedDocs = [
        { _id: 'delete-1', name: 'Alice', status: 'active' },
        { _id: 'delete-2', name: 'Bob', status: 'inactive' },
        { _id: 'delete-3', name: 'Carol', status: 'active' },
        { _id: 'delete-4', name: 'Dave', status: 'active' },
      ];

      const { mongolake, mongodb } = context.getCollections(testCollection);
      await mongolake.insertMany(seedDocs);
      await mongodb.insertMany(seedDocs);
    });

    it('should delete a single document', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        const deleteResult = await collection.deleteOne({ _id: 'delete-1' });
        const remaining = await collection.countDocuments({});
        return {
          deletedCount: deleteResult.deletedCount,
          remainingCount: remaining,
        };
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
      expect(result.mongolake.deletedCount).toBe(1);
      expect(result.mongolake.remainingCount).toBe(3);
    });

    it('should delete multiple documents', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        const deleteResult = await collection.deleteMany({ status: 'active' });
        const remaining = await collection.countDocuments({});
        return {
          deletedCount: deleteResult.deletedCount,
          remainingCount: remaining,
        };
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
      expect(result.mongolake.deletedCount).toBe(3);
      expect(result.mongolake.remainingCount).toBe(1);
    });

    it('should return deletedCount 0 for non-existent document', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        return await collection.deleteOne({ _id: 'non-existent' });
      });

      expect(result.match).toBe(true);
      expect(result.mongolake.deletedCount).toBe(0);
      expect(result.mongodb.deletedCount).toBe(0);
    });

    it('should delete all documents with empty filter', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        const deleteResult = await collection.deleteMany({});
        const remaining = await collection.countDocuments({});
        return {
          deletedCount: deleteResult.deletedCount,
          remainingCount: remaining,
        };
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
      expect(result.mongolake.deletedCount).toBe(4);
      expect(result.mongolake.remainingCount).toBe(0);
    });

    it('should verify document is gone after delete', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        await collection.deleteOne({ _id: 'delete-2' });
        return await collection.findOne({ _id: 'delete-2' });
      });

      expect(result.match).toBe(true);
      expect(result.mongolake).toBeNull();
      expect(result.mongodb).toBeNull();
    });
  });

  // ==========================================================================
  // Combined Operations
  // ==========================================================================

  describe('Combined CRUD Operations', () => {
    it('should handle insert-find-update-find-delete workflow', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        // Insert
        const doc = { _id: 'workflow-1', name: 'Test', version: 1 };
        await collection.insertOne(doc);

        // Find and verify
        const afterInsert = await collection.findOne({ _id: 'workflow-1' });

        // Update
        await collection.updateOne(
          { _id: 'workflow-1' },
          { $set: { version: 2, modified: true } }
        );

        // Find and verify update
        const afterUpdate = await collection.findOne({ _id: 'workflow-1' });

        // Delete
        const deleteResult = await collection.deleteOne({ _id: 'workflow-1' });

        // Verify deleted
        const afterDelete = await collection.findOne({ _id: 'workflow-1' });

        return {
          afterInsert,
          afterUpdate,
          deletedCount: deleteResult.deletedCount,
          afterDelete,
        };
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
      expect(result.mongolake.afterInsert?.version).toBe(1);
      expect(result.mongolake.afterUpdate?.version).toBe(2);
      expect(result.mongolake.afterUpdate?.modified).toBe(true);
      expect(result.mongolake.deletedCount).toBe(1);
      expect(result.mongolake.afterDelete).toBeNull();
    });

    it('should handle batch operations correctly', async () => {
      const result = await runOnBoth(context, testCollection, async (collection) => {
        // Batch insert
        const docs = Array.from({ length: 10 }, (_, i) => ({
          _id: `batch-${i}`,
          index: i,
          category: i % 2 === 0 ? 'even' : 'odd',
        }));
        await collection.insertMany(docs);

        // Count by category
        const evenCount = await collection.countDocuments({ category: 'even' });
        const oddCount = await collection.countDocuments({ category: 'odd' });

        // Update all even
        await collection.updateMany(
          { category: 'even' },
          { $set: { processed: true } }
        );

        // Delete all odd
        const deleteResult = await collection.deleteMany({ category: 'odd' });

        // Final count
        const finalCount = await collection.countDocuments({});

        return {
          evenCount,
          oddCount,
          deletedCount: deleteResult.deletedCount,
          finalCount,
        };
      });

      expect(result.match).toBe(true);
      assertDualRunMatch(result);
      expect(result.mongolake.evenCount).toBe(5);
      expect(result.mongolake.oddCount).toBe(5);
      expect(result.mongolake.deletedCount).toBe(5);
      expect(result.mongolake.finalCount).toBe(5);
    });
  });
});
