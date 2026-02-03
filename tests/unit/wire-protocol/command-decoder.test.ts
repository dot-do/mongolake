/**
 * Command Decoder Tests - RED Phase (TDD)
 *
 * These tests define expected behavior for command decoding that is not yet
 * fully implemented. Tests should fail until the implementation is complete.
 *
 * Tests cover:
 * 1. Find command extraction (filter, projection, sort, limit, skip)
 * 2. Insert command (documents array)
 * 3. Update command (filter, update, upsert, multi)
 * 4. Delete command (filter, limit)
 * 5. Aggregate command (pipeline, options)
 *
 * Wire Protocol Reference: https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/
 */

import { describe, it, expect } from 'vitest';
import {
  decodeCommand,
  CommandValidationError,
  COLLECTION_COMMANDS,
  ADMIN_COMMANDS,
  CURSOR_COMMANDS,
  // Type guards
  isCollectionCommand,
  isFindCommand,
  isInsertCommand,
  isUpdateCommand,
  isDeleteCommand,
  isAggregateCommand,
  isCursorCommand,
  // Builders
  FindCommandBuilder,
  InsertCommandBuilder,
  AggregateCommandBuilder,
  findCommand,
  insertCommand,
  aggregateCommand,
  // Types
  type DecodedCommand,
  type FindCommand,
  type InsertCommand,
  type UpdateCommand,
  type DeleteCommand,
  type AggregateCommand,
  type UpdateSpec,
  type DeleteSpec,
} from '../../../src/wire-protocol/command-decoder.js';
import type { Document, Filter } from '../../../src/types.js';

// ============================================================================
// Test Helpers
// ============================================================================

function createCommandBody(
  command: Record<string, unknown>,
  database: string = 'test'
): Document {
  return {
    ...command,
    $db: database,
  } as Document;
}

// ============================================================================
// Find Command Extraction Tests (RED - TDD)
// ============================================================================

describe('Find Command Extraction', () => {
  describe('Filter Extraction', () => {
    it('should extract simple equality filter', () => {
      const body = createCommandBody({
        find: 'users',
        filter: { status: 'active' },
      });

      const cmd = decodeCommand(body);
      expect(isFindCommand(cmd)).toBe(true);
      if (isFindCommand(cmd)) {
        expect(cmd.filter).toEqual({ status: 'active' });
      }
    });

    it('should extract filter with comparison operators', () => {
      const body = createCommandBody({
        find: 'products',
        filter: { price: { $gt: 100, $lt: 500 } },
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.filter).toEqual({ price: { $gt: 100, $lt: 500 } });
      }
    });

    it('should extract filter with logical operators', () => {
      const body = createCommandBody({
        find: 'orders',
        filter: {
          $and: [
            { status: 'pending' },
            { $or: [{ priority: 'high' }, { amount: { $gt: 1000 } }] },
          ],
        },
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.filter).toHaveProperty('$and');
        expect((cmd.filter as { $and: unknown[] }).$and).toHaveLength(2);
      }
    });

    it('should extract filter with array operators', () => {
      const body = createCommandBody({
        find: 'documents',
        filter: {
          tags: { $in: ['mongodb', 'database'] },
          categories: { $all: ['tech', 'nosql'] },
          scores: { $elemMatch: { value: { $gt: 80 } } },
        },
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.filter).toHaveProperty('tags');
        expect(cmd.filter).toHaveProperty('categories');
        expect(cmd.filter).toHaveProperty('scores');
      }
    });

    it('should extract filter with $regex operator', () => {
      const body = createCommandBody({
        find: 'users',
        filter: { email: { $regex: '^admin@', $options: 'i' } },
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.filter).toHaveProperty('email');
        const emailFilter = (cmd.filter as { email: { $regex: string; $options: string } }).email;
        expect(emailFilter.$regex).toBe('^admin@');
        expect(emailFilter.$options).toBe('i');
      }
    });

    it('should extract filter with nested document path', () => {
      const body = createCommandBody({
        find: 'users',
        filter: { 'address.city': 'New York', 'address.zip': { $exists: true } },
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.filter).toHaveProperty('address.city');
        expect(cmd.filter).toHaveProperty('address.zip');
      }
    });

    it('should handle empty filter as match all', () => {
      const body = createCommandBody({
        find: 'users',
        filter: {},
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.filter).toEqual({});
      }
    });

    it('should handle undefined filter as match all', () => {
      const body = createCommandBody({
        find: 'users',
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.filter).toBeUndefined();
      }
    });
  });

  describe('Projection Extraction', () => {
    it('should extract inclusion projection', () => {
      const body = createCommandBody({
        find: 'users',
        projection: { name: 1, email: 1, _id: 0 },
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.projection).toEqual({ name: 1, email: 1, _id: 0 });
      }
    });

    it('should extract exclusion projection', () => {
      const body = createCommandBody({
        find: 'users',
        projection: { password: 0, internalNotes: 0 },
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.projection).toEqual({ password: 0, internalNotes: 0 });
      }
    });

    it('should extract projection with nested fields', () => {
      const body = createCommandBody({
        find: 'users',
        projection: { 'address.city': 1, 'address.zip': 1, name: 1 },
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.projection).toHaveProperty('address.city');
        expect(cmd.projection).toHaveProperty('address.zip');
      }
    });

    it('should extract $slice projection operator', () => {
      const body = createCommandBody({
        find: 'posts',
        projection: { title: 1, comments: { $slice: 5 } },
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.projection).toHaveProperty('comments');
        // $slice should be preserved even if not typed as 0 | 1
        expect((cmd.projection as Record<string, unknown>).comments).toEqual({ $slice: 5 });
      }
    });

    it('should extract $elemMatch projection operator', () => {
      const body = createCommandBody({
        find: 'students',
        projection: { grades: { $elemMatch: { score: { $gt: 85 } } } },
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.projection).toHaveProperty('grades');
      }
    });

    it('should extract $meta projection for text search', () => {
      const body = createCommandBody({
        find: 'articles',
        filter: { $text: { $search: 'mongodb' } },
        projection: { title: 1, score: { $meta: 'textScore' } },
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.projection).toHaveProperty('score');
      }
    });
  });

  describe('Sort Extraction', () => {
    it('should extract ascending sort', () => {
      const body = createCommandBody({
        find: 'users',
        sort: { createdAt: 1 },
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.sort).toEqual({ createdAt: 1 });
      }
    });

    it('should extract descending sort', () => {
      const body = createCommandBody({
        find: 'users',
        sort: { updatedAt: -1 },
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.sort).toEqual({ updatedAt: -1 });
      }
    });

    it('should extract compound sort', () => {
      const body = createCommandBody({
        find: 'users',
        sort: { status: 1, priority: -1, createdAt: -1 },
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.sort).toEqual({ status: 1, priority: -1, createdAt: -1 });
        // Verify order is preserved
        const sortKeys = Object.keys(cmd.sort!);
        expect(sortKeys[0]).toBe('status');
        expect(sortKeys[1]).toBe('priority');
        expect(sortKeys[2]).toBe('createdAt');
      }
    });

    it('should extract sort on nested fields', () => {
      const body = createCommandBody({
        find: 'orders',
        sort: { 'customer.name': 1, 'items.0.price': -1 },
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.sort).toHaveProperty('customer.name');
        expect(cmd.sort).toHaveProperty('items.0.price');
      }
    });

    it('should extract $meta sort for text search', () => {
      const body = createCommandBody({
        find: 'articles',
        filter: { $text: { $search: 'mongodb' } },
        sort: { score: { $meta: 'textScore' } },
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.sort).toHaveProperty('score');
      }
    });
  });

  describe('Limit Extraction', () => {
    it('should extract positive limit', () => {
      const body = createCommandBody({
        find: 'users',
        limit: 10,
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.limit).toBe(10);
      }
    });

    it('should extract zero limit (all documents)', () => {
      const body = createCommandBody({
        find: 'users',
        limit: 0,
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.limit).toBe(0);
      }
    });

    it('should extract large limit value', () => {
      const body = createCommandBody({
        find: 'logs',
        limit: 1000000,
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.limit).toBe(1000000);
      }
    });

    it('should reject negative limit with validation error', () => {
      const body = createCommandBody({
        find: 'users',
        limit: -5,
      });

      // This test expects validation to reject negative limit
      // Currently the decoder may not validate this - should fail
      expect(() => decodeCommand(body)).toThrow(CommandValidationError);
    });
  });

  describe('Skip Extraction', () => {
    it('should extract positive skip', () => {
      const body = createCommandBody({
        find: 'users',
        skip: 20,
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.skip).toBe(20);
      }
    });

    it('should extract zero skip (no skip)', () => {
      const body = createCommandBody({
        find: 'users',
        skip: 0,
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.skip).toBe(0);
      }
    });

    it('should extract large skip for pagination', () => {
      const body = createCommandBody({
        find: 'products',
        skip: 10000,
        limit: 50,
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.skip).toBe(10000);
        expect(cmd.limit).toBe(50);
      }
    });

    it('should reject negative skip with validation error', () => {
      const body = createCommandBody({
        find: 'users',
        skip: -10,
      });

      // This test expects validation to reject negative skip
      // Currently the decoder may not validate this - should fail
      expect(() => decodeCommand(body)).toThrow(CommandValidationError);
    });
  });

  describe('Combined Find Options', () => {
    it('should extract all find options together', () => {
      const body = createCommandBody({
        find: 'orders',
        filter: { status: 'pending', amount: { $gte: 100 } },
        projection: { orderId: 1, customer: 1, amount: 1, _id: 0 },
        sort: { createdAt: -1, priority: -1 },
        limit: 25,
        skip: 50,
        batchSize: 100,
        singleBatch: false,
        hint: { status: 1, createdAt: -1 },
        maxTimeMS: 5000,
      });

      const cmd = decodeCommand(body);
      expect(isFindCommand(cmd)).toBe(true);
      if (isFindCommand(cmd)) {
        expect(cmd.collection).toBe('orders');
        expect(cmd.filter).toHaveProperty('status');
        expect(cmd.filter).toHaveProperty('amount');
        expect(cmd.projection).toEqual({ orderId: 1, customer: 1, amount: 1, _id: 0 });
        expect(cmd.sort).toEqual({ createdAt: -1, priority: -1 });
        expect(cmd.limit).toBe(25);
        expect(cmd.skip).toBe(50);
        expect(cmd.batchSize).toBe(100);
        expect(cmd.singleBatch).toBe(false);
        expect(cmd.hint).toEqual({ status: 1, createdAt: -1 });
        expect(cmd.maxTimeMS).toBe(5000);
      }
    });

    it('should extract find with string hint (index name)', () => {
      const body = createCommandBody({
        find: 'users',
        filter: { status: 'active' },
        hint: 'status_1_createdAt_-1',
      });

      const cmd = decodeCommand(body);
      if (isFindCommand(cmd)) {
        expect(cmd.hint).toBe('status_1_createdAt_-1');
      }
    });
  });
});

// ============================================================================
// Insert Command Tests (RED - TDD)
// ============================================================================

describe('Insert Command Extraction', () => {
  describe('Documents Array', () => {
    it('should extract single document', () => {
      const body = createCommandBody({
        insert: 'users',
        documents: [{ name: 'Alice', email: 'alice@example.com' }],
      });

      const cmd = decodeCommand(body);
      expect(isInsertCommand(cmd)).toBe(true);
      if (isInsertCommand(cmd)) {
        expect(cmd.documents).toHaveLength(1);
        expect(cmd.documents[0]).toEqual({ name: 'Alice', email: 'alice@example.com' });
      }
    });

    it('should extract multiple documents', () => {
      const body = createCommandBody({
        insert: 'users',
        documents: [
          { name: 'Alice', email: 'alice@example.com' },
          { name: 'Bob', email: 'bob@example.com' },
          { name: 'Charlie', email: 'charlie@example.com' },
        ],
      });

      const cmd = decodeCommand(body);
      if (isInsertCommand(cmd)) {
        expect(cmd.documents).toHaveLength(3);
        expect(cmd.documents[0].name).toBe('Alice');
        expect(cmd.documents[1].name).toBe('Bob');
        expect(cmd.documents[2].name).toBe('Charlie');
      }
    });

    it('should extract documents with nested objects', () => {
      const body = createCommandBody({
        insert: 'orders',
        documents: [
          {
            orderId: 'ORD-001',
            customer: { name: 'Alice', address: { city: 'NYC', zip: '10001' } },
            items: [
              { product: 'Widget', quantity: 2, price: 19.99 },
              { product: 'Gadget', quantity: 1, price: 49.99 },
            ],
          },
        ],
      });

      const cmd = decodeCommand(body);
      if (isInsertCommand(cmd)) {
        expect(cmd.documents[0]).toHaveProperty('customer.address.city');
        expect(cmd.documents[0].items).toHaveLength(2);
      }
    });

    it('should extract documents with arrays', () => {
      const body = createCommandBody({
        insert: 'posts',
        documents: [
          { title: 'Post 1', tags: ['mongodb', 'database', 'nosql'] },
        ],
      });

      const cmd = decodeCommand(body);
      if (isInsertCommand(cmd)) {
        expect(cmd.documents[0].tags).toEqual(['mongodb', 'database', 'nosql']);
      }
    });

    it('should extract documents from document sequence section', () => {
      const body = createCommandBody({
        insert: 'products',
      });
      const documents = [
        { name: 'Widget', price: 19.99 },
        { name: 'Gadget', price: 49.99 },
      ] as Document[];

      const cmd = decodeCommand(body, documents);
      if (isInsertCommand(cmd)) {
        expect(cmd.documents).toEqual(documents);
      }
    });

    it('should prefer sequence documents over body documents', () => {
      const body = createCommandBody({
        insert: 'products',
        documents: [{ name: 'FromBody' }],
      });
      const sequenceDocuments = [{ name: 'FromSequence' }] as Document[];

      const cmd = decodeCommand(body, sequenceDocuments);
      if (isInsertCommand(cmd)) {
        expect(cmd.documents[0].name).toBe('FromSequence');
      }
    });

    it('should handle large document batches', () => {
      const docs = Array.from({ length: 1000 }, (_, i) => ({
        index: i,
        data: `Document ${i}`,
      }));

      const body = createCommandBody({
        insert: 'bulk_data',
        documents: docs,
      });

      const cmd = decodeCommand(body);
      if (isInsertCommand(cmd)) {
        expect(cmd.documents).toHaveLength(1000);
        expect(cmd.documents[500].index).toBe(500);
      }
    });

    it('should validate documents array is not empty at execution time', () => {
      // Note: Per the implementation, empty array is allowed at decode time
      // This test verifies that behavior
      const body = createCommandBody({
        insert: 'users',
        documents: [],
      });

      // Should not throw at decode time
      const cmd = decodeCommand(body);
      expect(isInsertCommand(cmd)).toBe(true);
      if (isInsertCommand(cmd)) {
        expect(cmd.documents).toEqual([]);
      }
    });
  });

  describe('Ordered Option', () => {
    it('should extract ordered=true (default)', () => {
      const body = createCommandBody({
        insert: 'users',
        documents: [{ name: 'Test' }],
        ordered: true,
      });

      const cmd = decodeCommand(body);
      if (isInsertCommand(cmd)) {
        expect(cmd.ordered).toBe(true);
      }
    });

    it('should extract ordered=false for unordered insert', () => {
      const body = createCommandBody({
        insert: 'logs',
        documents: [{ event: 'test' }],
        ordered: false,
      });

      const cmd = decodeCommand(body);
      if (isInsertCommand(cmd)) {
        expect(cmd.ordered).toBe(false);
      }
    });

    it('should have undefined ordered when not specified', () => {
      const body = createCommandBody({
        insert: 'users',
        documents: [{ name: 'Test' }],
      });

      const cmd = decodeCommand(body);
      if (isInsertCommand(cmd)) {
        expect(cmd.ordered).toBeUndefined();
      }
    });
  });

  describe('Insert Validation', () => {
    it('should reject non-array documents field', () => {
      const body = createCommandBody({
        insert: 'users',
        documents: { name: 'NotAnArray' },
      });

      // This should fail validation - documents must be an array
      expect(() => decodeCommand(body)).toThrow(CommandValidationError);
    });

    it('should reject documents with invalid BSON types', () => {
      const body = createCommandBody({
        insert: 'users',
        documents: [{ name: 'Test', callback: () => {} }],
      });

      // Functions cannot be serialized to BSON - should fail
      expect(() => decodeCommand(body)).toThrow(CommandValidationError);
    });
  });
});

// ============================================================================
// Update Command Tests (RED - TDD)
// ============================================================================

describe('Update Command Extraction', () => {
  describe('Filter Extraction', () => {
    it('should extract update filter (q field)', () => {
      const body = createCommandBody({
        update: 'users',
        updates: [
          { q: { _id: 'user123' }, u: { $set: { status: 'active' } } },
        ],
      });

      const cmd = decodeCommand(body);
      expect(isUpdateCommand(cmd)).toBe(true);
      if (isUpdateCommand(cmd)) {
        expect(cmd.updates[0].q).toEqual({ _id: 'user123' });
      }
    });

    it('should extract complex filter with operators', () => {
      const body = createCommandBody({
        update: 'orders',
        updates: [
          {
            q: {
              status: 'pending',
              createdAt: { $lt: new Date('2024-01-01') },
              $or: [{ priority: 'high' }, { amount: { $gt: 1000 } }],
            },
            u: { $set: { status: 'expired' } },
          },
        ],
      });

      const cmd = decodeCommand(body);
      if (isUpdateCommand(cmd)) {
        expect(cmd.updates[0].q).toHaveProperty('status');
        expect(cmd.updates[0].q).toHaveProperty('createdAt');
        expect(cmd.updates[0].q).toHaveProperty('$or');
      }
    });

    it('should extract empty filter (match all)', () => {
      const body = createCommandBody({
        update: 'settings',
        updates: [
          { q: {}, u: { $set: { version: 2 } }, multi: true },
        ],
      });

      const cmd = decodeCommand(body);
      if (isUpdateCommand(cmd)) {
        expect(cmd.updates[0].q).toEqual({});
      }
    });
  });

  describe('Update Document Extraction', () => {
    it('should extract $set update operator', () => {
      const body = createCommandBody({
        update: 'users',
        updates: [
          { q: { _id: 'user1' }, u: { $set: { name: 'Alice', email: 'alice@example.com' } } },
        ],
      });

      const cmd = decodeCommand(body);
      if (isUpdateCommand(cmd)) {
        expect(cmd.updates[0].u).toEqual({ $set: { name: 'Alice', email: 'alice@example.com' } });
      }
    });

    it('should extract $unset update operator', () => {
      const body = createCommandBody({
        update: 'users',
        updates: [
          { q: { _id: 'user1' }, u: { $unset: { temporaryField: '' } } },
        ],
      });

      const cmd = decodeCommand(body);
      if (isUpdateCommand(cmd)) {
        expect(cmd.updates[0].u).toHaveProperty('$unset');
      }
    });

    it('should extract $inc update operator', () => {
      const body = createCommandBody({
        update: 'counters',
        updates: [
          { q: { name: 'pageViews' }, u: { $inc: { value: 1 } } },
        ],
      });

      const cmd = decodeCommand(body);
      if (isUpdateCommand(cmd)) {
        expect(cmd.updates[0].u).toEqual({ $inc: { value: 1 } });
      }
    });

    it('should extract $push update operator', () => {
      const body = createCommandBody({
        update: 'users',
        updates: [
          {
            q: { _id: 'user1' },
            u: { $push: { tags: { $each: ['new', 'tags'], $position: 0 } } },
          },
        ],
      });

      const cmd = decodeCommand(body);
      if (isUpdateCommand(cmd)) {
        expect(cmd.updates[0].u).toHaveProperty('$push');
      }
    });

    it('should extract $pull update operator', () => {
      const body = createCommandBody({
        update: 'users',
        updates: [
          { q: { _id: 'user1' }, u: { $pull: { tags: 'deprecated' } } },
        ],
      });

      const cmd = decodeCommand(body);
      if (isUpdateCommand(cmd)) {
        expect(cmd.updates[0].u).toHaveProperty('$pull');
      }
    });

    it('should extract replacement document (no operators)', () => {
      const body = createCommandBody({
        update: 'users',
        updates: [
          { q: { _id: 'user1' }, u: { name: 'Alice', email: 'alice@example.com', active: true } },
        ],
      });

      const cmd = decodeCommand(body);
      if (isUpdateCommand(cmd)) {
        expect(cmd.updates[0].u).toEqual({ name: 'Alice', email: 'alice@example.com', active: true });
      }
    });

    it('should extract aggregation pipeline update', () => {
      const body = createCommandBody({
        update: 'users',
        updates: [
          {
            q: { status: 'new' },
            u: [
              { $set: { status: 'processed', processedAt: '$$NOW' } },
              { $unset: ['temporaryField'] },
            ],
          },
        ],
      });

      const cmd = decodeCommand(body);
      if (isUpdateCommand(cmd)) {
        expect(Array.isArray(cmd.updates[0].u)).toBe(true);
        expect((cmd.updates[0].u as Document[])).toHaveLength(2);
      }
    });
  });

  describe('Upsert Option', () => {
    it('should extract upsert=true', () => {
      const body = createCommandBody({
        update: 'users',
        updates: [
          { q: { email: 'new@example.com' }, u: { $set: { name: 'New User' } }, upsert: true },
        ],
      });

      const cmd = decodeCommand(body);
      if (isUpdateCommand(cmd)) {
        expect(cmd.updates[0].upsert).toBe(true);
      }
    });

    it('should extract upsert=false', () => {
      const body = createCommandBody({
        update: 'users',
        updates: [
          { q: { _id: 'user1' }, u: { $set: { name: 'Alice' } }, upsert: false },
        ],
      });

      const cmd = decodeCommand(body);
      if (isUpdateCommand(cmd)) {
        expect(cmd.updates[0].upsert).toBe(false);
      }
    });

    it('should have undefined upsert when not specified', () => {
      const body = createCommandBody({
        update: 'users',
        updates: [
          { q: { _id: 'user1' }, u: { $set: { name: 'Alice' } } },
        ],
      });

      const cmd = decodeCommand(body);
      if (isUpdateCommand(cmd)) {
        expect(cmd.updates[0].upsert).toBeUndefined();
      }
    });
  });

  describe('Multi Option', () => {
    it('should extract multi=true', () => {
      const body = createCommandBody({
        update: 'users',
        updates: [
          { q: { status: 'inactive' }, u: { $set: { archived: true } }, multi: true },
        ],
      });

      const cmd = decodeCommand(body);
      if (isUpdateCommand(cmd)) {
        expect(cmd.updates[0].multi).toBe(true);
      }
    });

    it('should extract multi=false (update one)', () => {
      const body = createCommandBody({
        update: 'users',
        updates: [
          { q: { status: 'inactive' }, u: { $set: { status: 'active' } }, multi: false },
        ],
      });

      const cmd = decodeCommand(body);
      if (isUpdateCommand(cmd)) {
        expect(cmd.updates[0].multi).toBe(false);
      }
    });

    it('should have undefined multi when not specified', () => {
      const body = createCommandBody({
        update: 'users',
        updates: [
          { q: { _id: 'user1' }, u: { $set: { name: 'Alice' } } },
        ],
      });

      const cmd = decodeCommand(body);
      if (isUpdateCommand(cmd)) {
        expect(cmd.updates[0].multi).toBeUndefined();
      }
    });
  });

  describe('Multiple Updates', () => {
    it('should extract multiple update specifications', () => {
      const body = createCommandBody({
        update: 'users',
        updates: [
          { q: { _id: 'user1' }, u: { $set: { name: 'Alice' } } },
          { q: { _id: 'user2' }, u: { $set: { name: 'Bob' } }, upsert: true },
          { q: { status: 'old' }, u: { $set: { status: 'archived' } }, multi: true },
        ],
      });

      const cmd = decodeCommand(body);
      if (isUpdateCommand(cmd)) {
        expect(cmd.updates).toHaveLength(3);
        expect(cmd.updates[0].q).toEqual({ _id: 'user1' });
        expect(cmd.updates[1].upsert).toBe(true);
        expect(cmd.updates[2].multi).toBe(true);
      }
    });

    it('should extract updates from document sequence section', () => {
      const body = createCommandBody({
        update: 'users',
      });
      const updates = [
        { q: { _id: 'user1' }, u: { $set: { name: 'Alice' } } },
        { q: { _id: 'user2' }, u: { $set: { name: 'Bob' } } },
      ] as Document[];

      const cmd = decodeCommand(body, updates);
      if (isUpdateCommand(cmd)) {
        expect(cmd.updates).toHaveLength(2);
      }
    });
  });

  describe('Array Filters', () => {
    it('should extract arrayFilters option', () => {
      const body = createCommandBody({
        update: 'students',
        updates: [
          {
            q: { _id: 'student1' },
            u: { $set: { 'grades.$[elem].passed': true } },
            arrayFilters: [{ 'elem.score': { $gte: 60 } }],
          },
        ],
      });

      const cmd = decodeCommand(body);
      if (isUpdateCommand(cmd)) {
        expect(cmd.updates[0].arrayFilters).toBeDefined();
        expect(cmd.updates[0].arrayFilters).toHaveLength(1);
      }
    });

    it('should extract multiple arrayFilters', () => {
      const body = createCommandBody({
        update: 'students',
        updates: [
          {
            q: { _id: 'student1' },
            u: {
              $set: {
                'grades.$[pass].status': 'passed',
                'grades.$[fail].status': 'failed',
              },
            },
            arrayFilters: [
              { 'pass.score': { $gte: 60 } },
              { 'fail.score': { $lt: 60 } },
            ],
          },
        ],
      });

      const cmd = decodeCommand(body);
      if (isUpdateCommand(cmd)) {
        expect(cmd.updates[0].arrayFilters).toHaveLength(2);
      }
    });
  });

  describe('Update Hint', () => {
    it('should extract hint as string (index name)', () => {
      const body = createCommandBody({
        update: 'users',
        updates: [
          {
            q: { status: 'active' },
            u: { $set: { lastChecked: new Date() } },
            hint: 'status_1',
          },
        ],
      });

      const cmd = decodeCommand(body);
      if (isUpdateCommand(cmd)) {
        expect(cmd.updates[0].hint).toBe('status_1');
      }
    });

    it('should extract hint as document (index spec)', () => {
      const body = createCommandBody({
        update: 'users',
        updates: [
          {
            q: { status: 'active' },
            u: { $set: { lastChecked: new Date() } },
            hint: { status: 1, createdAt: -1 },
          },
        ],
      });

      const cmd = decodeCommand(body);
      if (isUpdateCommand(cmd)) {
        expect(cmd.updates[0].hint).toEqual({ status: 1, createdAt: -1 });
      }
    });
  });
});

// ============================================================================
// Delete Command Tests (RED - TDD)
// ============================================================================

describe('Delete Command Extraction', () => {
  describe('Filter Extraction', () => {
    it('should extract delete filter (q field)', () => {
      const body = createCommandBody({
        delete: 'sessions',
        deletes: [
          { q: { expired: true }, limit: 0 },
        ],
      });

      const cmd = decodeCommand(body);
      expect(isDeleteCommand(cmd)).toBe(true);
      if (isDeleteCommand(cmd)) {
        expect(cmd.deletes[0].q).toEqual({ expired: true });
      }
    });

    it('should extract complex filter with operators', () => {
      const body = createCommandBody({
        delete: 'logs',
        deletes: [
          {
            q: {
              timestamp: { $lt: new Date('2023-01-01') },
              $or: [{ level: 'debug' }, { level: 'trace' }],
            },
            limit: 0,
          },
        ],
      });

      const cmd = decodeCommand(body);
      if (isDeleteCommand(cmd)) {
        expect(cmd.deletes[0].q).toHaveProperty('timestamp');
        expect(cmd.deletes[0].q).toHaveProperty('$or');
      }
    });

    it('should extract empty filter (delete all - DANGEROUS)', () => {
      const body = createCommandBody({
        delete: 'temp_data',
        deletes: [
          { q: {}, limit: 0 },
        ],
      });

      const cmd = decodeCommand(body);
      if (isDeleteCommand(cmd)) {
        expect(cmd.deletes[0].q).toEqual({});
      }
    });

    it('should extract filter with _id', () => {
      const body = createCommandBody({
        delete: 'users',
        deletes: [
          { q: { _id: 'user123' }, limit: 1 },
        ],
      });

      const cmd = decodeCommand(body);
      if (isDeleteCommand(cmd)) {
        expect(cmd.deletes[0].q).toEqual({ _id: 'user123' });
      }
    });
  });

  describe('Limit Extraction', () => {
    it('should extract limit=0 (delete all matching)', () => {
      const body = createCommandBody({
        delete: 'logs',
        deletes: [
          { q: { level: 'debug' }, limit: 0 },
        ],
      });

      const cmd = decodeCommand(body);
      if (isDeleteCommand(cmd)) {
        expect(cmd.deletes[0].limit).toBe(0);
      }
    });

    it('should extract limit=1 (delete one)', () => {
      const body = createCommandBody({
        delete: 'users',
        deletes: [
          { q: { _id: 'user1' }, limit: 1 },
        ],
      });

      const cmd = decodeCommand(body);
      if (isDeleteCommand(cmd)) {
        expect(cmd.deletes[0].limit).toBe(1);
      }
    });

    it('should reject invalid limit values', () => {
      const body = createCommandBody({
        delete: 'users',
        deletes: [
          { q: { _id: 'user1' }, limit: 5 },
        ],
      });

      // MongoDB only allows limit 0 or 1 for delete operations
      // This test expects validation to reject other values
      expect(() => decodeCommand(body)).toThrow(CommandValidationError);
    });

    it('should reject negative limit', () => {
      const body = createCommandBody({
        delete: 'users',
        deletes: [
          { q: { _id: 'user1' }, limit: -1 },
        ],
      });

      expect(() => decodeCommand(body)).toThrow(CommandValidationError);
    });
  });

  describe('Multiple Deletes', () => {
    it('should extract multiple delete specifications', () => {
      const body = createCommandBody({
        delete: 'users',
        deletes: [
          { q: { _id: 'user1' }, limit: 1 },
          { q: { _id: 'user2' }, limit: 1 },
          { q: { status: 'deleted' }, limit: 0 },
        ],
      });

      const cmd = decodeCommand(body);
      if (isDeleteCommand(cmd)) {
        expect(cmd.deletes).toHaveLength(3);
        expect(cmd.deletes[0].limit).toBe(1);
        expect(cmd.deletes[1].limit).toBe(1);
        expect(cmd.deletes[2].limit).toBe(0);
      }
    });

    it('should extract deletes from document sequence section', () => {
      const body = createCommandBody({
        delete: 'users',
      });
      const deletes = [
        { q: { _id: 'user1' }, limit: 1 },
        { q: { _id: 'user2' }, limit: 1 },
      ] as Document[];

      const cmd = decodeCommand(body, deletes);
      if (isDeleteCommand(cmd)) {
        expect(cmd.deletes).toHaveLength(2);
      }
    });
  });

  describe('Delete Hint', () => {
    it('should extract hint as string', () => {
      const body = createCommandBody({
        delete: 'logs',
        deletes: [
          { q: { timestamp: { $lt: new Date() } }, limit: 0, hint: 'timestamp_1' },
        ],
      });

      const cmd = decodeCommand(body);
      if (isDeleteCommand(cmd)) {
        expect(cmd.deletes[0].hint).toBe('timestamp_1');
      }
    });

    it('should extract hint as document', () => {
      const body = createCommandBody({
        delete: 'logs',
        deletes: [
          { q: { timestamp: { $lt: new Date() } }, limit: 0, hint: { timestamp: 1 } },
        ],
      });

      const cmd = decodeCommand(body);
      if (isDeleteCommand(cmd)) {
        expect(cmd.deletes[0].hint).toEqual({ timestamp: 1 });
      }
    });
  });

  describe('Ordered Option', () => {
    it('should extract ordered=true', () => {
      const body = createCommandBody({
        delete: 'users',
        deletes: [
          { q: { _id: 'user1' }, limit: 1 },
          { q: { _id: 'user2' }, limit: 1 },
        ],
        ordered: true,
      });

      const cmd = decodeCommand(body);
      if (isDeleteCommand(cmd)) {
        expect(cmd.ordered).toBe(true);
      }
    });

    it('should extract ordered=false', () => {
      const body = createCommandBody({
        delete: 'logs',
        deletes: [
          { q: { level: 'debug' }, limit: 0 },
          { q: { level: 'trace' }, limit: 0 },
        ],
        ordered: false,
      });

      const cmd = decodeCommand(body);
      if (isDeleteCommand(cmd)) {
        expect(cmd.ordered).toBe(false);
      }
    });
  });
});

// ============================================================================
// Aggregate Command Tests (RED - TDD)
// ============================================================================

describe('Aggregate Command Extraction', () => {
  describe('Pipeline Extraction', () => {
    it('should extract empty pipeline', () => {
      const body = createCommandBody({
        aggregate: 'users',
        pipeline: [],
        cursor: {},
      });

      const cmd = decodeCommand(body);
      expect(isAggregateCommand(cmd)).toBe(true);
      if (isAggregateCommand(cmd)) {
        expect(cmd.pipeline).toEqual([]);
      }
    });

    it('should extract $match stage', () => {
      const body = createCommandBody({
        aggregate: 'orders',
        pipeline: [
          { $match: { status: 'completed', amount: { $gte: 100 } } },
        ],
        cursor: {},
      });

      const cmd = decodeCommand(body);
      if (isAggregateCommand(cmd)) {
        expect(cmd.pipeline).toHaveLength(1);
        expect(cmd.pipeline[0]).toHaveProperty('$match');
      }
    });

    it('should extract $group stage', () => {
      const body = createCommandBody({
        aggregate: 'orders',
        pipeline: [
          {
            $group: {
              _id: '$customer',
              totalAmount: { $sum: '$amount' },
              orderCount: { $sum: 1 },
              avgAmount: { $avg: '$amount' },
            },
          },
        ],
        cursor: {},
      });

      const cmd = decodeCommand(body);
      if (isAggregateCommand(cmd)) {
        expect(cmd.pipeline[0]).toHaveProperty('$group');
        const group = cmd.pipeline[0].$group as Record<string, unknown>;
        expect(group._id).toBe('$customer');
        expect(group.totalAmount).toEqual({ $sum: '$amount' });
      }
    });

    it('should extract $project stage', () => {
      const body = createCommandBody({
        aggregate: 'users',
        pipeline: [
          {
            $project: {
              fullName: { $concat: ['$firstName', ' ', '$lastName'] },
              email: 1,
              _id: 0,
            },
          },
        ],
        cursor: {},
      });

      const cmd = decodeCommand(body);
      if (isAggregateCommand(cmd)) {
        expect(cmd.pipeline[0]).toHaveProperty('$project');
      }
    });

    it('should extract $sort stage', () => {
      const body = createCommandBody({
        aggregate: 'products',
        pipeline: [
          { $sort: { price: -1, name: 1 } },
        ],
        cursor: {},
      });

      const cmd = decodeCommand(body);
      if (isAggregateCommand(cmd)) {
        expect(cmd.pipeline[0]).toHaveProperty('$sort');
        expect((cmd.pipeline[0] as { $sort: Record<string, number> }).$sort).toEqual({ price: -1, name: 1 });
      }
    });

    it('should extract $limit stage', () => {
      const body = createCommandBody({
        aggregate: 'products',
        pipeline: [
          { $limit: 10 },
        ],
        cursor: {},
      });

      const cmd = decodeCommand(body);
      if (isAggregateCommand(cmd)) {
        expect(cmd.pipeline[0]).toEqual({ $limit: 10 });
      }
    });

    it('should extract $skip stage', () => {
      const body = createCommandBody({
        aggregate: 'products',
        pipeline: [
          { $skip: 100 },
        ],
        cursor: {},
      });

      const cmd = decodeCommand(body);
      if (isAggregateCommand(cmd)) {
        expect(cmd.pipeline[0]).toEqual({ $skip: 100 });
      }
    });

    it('should extract $lookup stage', () => {
      const body = createCommandBody({
        aggregate: 'orders',
        pipeline: [
          {
            $lookup: {
              from: 'customers',
              localField: 'customerId',
              foreignField: '_id',
              as: 'customer',
            },
          },
        ],
        cursor: {},
      });

      const cmd = decodeCommand(body);
      if (isAggregateCommand(cmd)) {
        expect(cmd.pipeline[0]).toHaveProperty('$lookup');
        const lookup = (cmd.pipeline[0] as { $lookup: Record<string, unknown> }).$lookup;
        expect(lookup.from).toBe('customers');
        expect(lookup.as).toBe('customer');
      }
    });

    it('should extract $unwind stage', () => {
      const body = createCommandBody({
        aggregate: 'orders',
        pipeline: [
          { $unwind: { path: '$items', preserveNullAndEmptyArrays: true } },
        ],
        cursor: {},
      });

      const cmd = decodeCommand(body);
      if (isAggregateCommand(cmd)) {
        expect(cmd.pipeline[0]).toHaveProperty('$unwind');
      }
    });

    it('should extract $addFields stage', () => {
      const body = createCommandBody({
        aggregate: 'orders',
        pipeline: [
          { $addFields: { totalWithTax: { $multiply: ['$total', 1.08] } } },
        ],
        cursor: {},
      });

      const cmd = decodeCommand(body);
      if (isAggregateCommand(cmd)) {
        expect(cmd.pipeline[0]).toHaveProperty('$addFields');
      }
    });

    it('should extract $facet stage', () => {
      const body = createCommandBody({
        aggregate: 'products',
        pipeline: [
          {
            $facet: {
              byCategory: [{ $group: { _id: '$category', count: { $sum: 1 } } }],
              byPrice: [{ $bucket: { groupBy: '$price', boundaries: [0, 50, 100, 500] } }],
            },
          },
        ],
        cursor: {},
      });

      const cmd = decodeCommand(body);
      if (isAggregateCommand(cmd)) {
        expect(cmd.pipeline[0]).toHaveProperty('$facet');
      }
    });

    it('should extract $out stage', () => {
      const body = createCommandBody({
        aggregate: 'orders',
        pipeline: [
          { $match: { status: 'completed' } },
          { $out: 'completed_orders' },
        ],
        cursor: {},
      });

      const cmd = decodeCommand(body);
      if (isAggregateCommand(cmd)) {
        expect(cmd.pipeline).toHaveLength(2);
        expect(cmd.pipeline[1]).toEqual({ $out: 'completed_orders' });
      }
    });

    it('should extract $merge stage', () => {
      const body = createCommandBody({
        aggregate: 'orders',
        pipeline: [
          { $match: { status: 'completed' } },
          {
            $merge: {
              into: 'order_summary',
              on: '_id',
              whenMatched: 'replace',
              whenNotMatched: 'insert',
            },
          },
        ],
        cursor: {},
      });

      const cmd = decodeCommand(body);
      if (isAggregateCommand(cmd)) {
        expect(cmd.pipeline[1]).toHaveProperty('$merge');
      }
    });

    it('should extract complex multi-stage pipeline', () => {
      const body = createCommandBody({
        aggregate: 'orders',
        pipeline: [
          { $match: { status: 'completed' } },
          { $unwind: '$items' },
          {
            $group: {
              _id: '$items.productId',
              totalQuantity: { $sum: '$items.quantity' },
              totalRevenue: { $sum: { $multiply: ['$items.quantity', '$items.price'] } },
            },
          },
          {
            $lookup: {
              from: 'products',
              localField: '_id',
              foreignField: '_id',
              as: 'product',
            },
          },
          { $unwind: '$product' },
          { $project: { productName: '$product.name', totalQuantity: 1, totalRevenue: 1 } },
          { $sort: { totalRevenue: -1 } },
          { $limit: 10 },
        ],
        cursor: { batchSize: 100 },
      });

      const cmd = decodeCommand(body);
      if (isAggregateCommand(cmd)) {
        expect(cmd.pipeline).toHaveLength(8);
      }
    });
  });

  describe('Cursor Options', () => {
    it('should extract cursor with batchSize', () => {
      const body = createCommandBody({
        aggregate: 'users',
        pipeline: [],
        cursor: { batchSize: 100 },
      });

      const cmd = decodeCommand(body);
      if (isAggregateCommand(cmd)) {
        expect(cmd.cursor).toEqual({ batchSize: 100 });
      }
    });

    it('should extract empty cursor (use default batch size)', () => {
      const body = createCommandBody({
        aggregate: 'users',
        pipeline: [],
        cursor: {},
      });

      const cmd = decodeCommand(body);
      if (isAggregateCommand(cmd)) {
        expect(cmd.cursor).toEqual({});
      }
    });

    it('should handle missing cursor field', () => {
      const body = createCommandBody({
        aggregate: 'users',
        pipeline: [],
      });

      const cmd = decodeCommand(body);
      if (isAggregateCommand(cmd)) {
        expect(cmd.cursor).toBeUndefined();
      }
    });
  });

  describe('Aggregate Options', () => {
    it('should extract allowDiskUse=true', () => {
      const body = createCommandBody({
        aggregate: 'large_collection',
        pipeline: [{ $sort: { field: 1 } }],
        cursor: {},
        allowDiskUse: true,
      });

      const cmd = decodeCommand(body);
      if (isAggregateCommand(cmd)) {
        expect(cmd.allowDiskUse).toBe(true);
      }
    });

    it('should extract allowDiskUse=false', () => {
      const body = createCommandBody({
        aggregate: 'users',
        pipeline: [],
        cursor: {},
        allowDiskUse: false,
      });

      const cmd = decodeCommand(body);
      if (isAggregateCommand(cmd)) {
        expect(cmd.allowDiskUse).toBe(false);
      }
    });

    it('should extract maxTimeMS', () => {
      const body = createCommandBody({
        aggregate: 'users',
        pipeline: [{ $match: { status: 'active' } }],
        cursor: {},
        maxTimeMS: 30000,
      });

      const cmd = decodeCommand(body);
      if (isAggregateCommand(cmd)) {
        expect(cmd.maxTimeMS).toBe(30000);
      }
    });

    it('should extract hint as string', () => {
      const body = createCommandBody({
        aggregate: 'orders',
        pipeline: [{ $match: { status: 'pending' } }],
        cursor: {},
        hint: 'status_1_createdAt_-1',
      });

      const cmd = decodeCommand(body);
      if (isAggregateCommand(cmd)) {
        expect(cmd.hint).toBe('status_1_createdAt_-1');
      }
    });

    it('should extract hint as document', () => {
      const body = createCommandBody({
        aggregate: 'orders',
        pipeline: [{ $match: { status: 'pending' } }],
        cursor: {},
        hint: { status: 1, createdAt: -1 },
      });

      const cmd = decodeCommand(body);
      if (isAggregateCommand(cmd)) {
        expect(cmd.hint).toEqual({ status: 1, createdAt: -1 });
      }
    });

    it('should extract all aggregate options together', () => {
      const body = createCommandBody({
        aggregate: 'analytics',
        pipeline: [
          { $match: { timestamp: { $gte: new Date('2024-01-01') } } },
          { $group: { _id: '$eventType', count: { $sum: 1 } } },
        ],
        cursor: { batchSize: 500 },
        allowDiskUse: true,
        maxTimeMS: 60000,
        hint: { timestamp: 1 },
      });

      const cmd = decodeCommand(body);
      if (isAggregateCommand(cmd)) {
        expect(cmd.collection).toBe('analytics');
        expect(cmd.pipeline).toHaveLength(2);
        expect(cmd.cursor).toEqual({ batchSize: 500 });
        expect(cmd.allowDiskUse).toBe(true);
        expect(cmd.maxTimeMS).toBe(60000);
        expect(cmd.hint).toEqual({ timestamp: 1 });
      }
    });
  });

  describe('Aggregate Validation', () => {
    it('should require pipeline to be an array', () => {
      const body = createCommandBody({
        aggregate: 'users',
        pipeline: { $match: { status: 'active' } },
        cursor: {},
      });

      expect(() => decodeCommand(body)).toThrow(CommandValidationError);
    });

    it('should validate pipeline stages are objects', () => {
      const body = createCommandBody({
        aggregate: 'users',
        pipeline: ['not an object', 123],
        cursor: {},
      });

      // This test expects validation of pipeline stage types
      expect(() => decodeCommand(body)).toThrow(CommandValidationError);
    });

    it('should validate $out/$merge is last stage', () => {
      const body = createCommandBody({
        aggregate: 'orders',
        pipeline: [
          { $out: 'temp' },
          { $match: { status: 'active' } }, // Invalid: $out must be last
        ],
        cursor: {},
      });

      // This test expects validation that $out must be the last stage
      expect(() => decodeCommand(body)).toThrow(CommandValidationError);
    });
  });
});

// ============================================================================
// Basic Command Decoding Tests (Existing tests - kept for completeness)
// ============================================================================

describe('Basic Command Decoding', () => {
  it('should decode find command', () => {
    const body = createCommandBody({
      find: 'users',
      filter: { status: 'active' },
      projection: { name: 1, email: 1 },
      sort: { createdAt: -1 },
      limit: 10,
      skip: 5,
    });

    const cmd = decodeCommand(body);

    expect(cmd.name).toBe('find');
    expect(cmd.database).toBe('test');
    if (isFindCommand(cmd)) {
      expect(cmd.collection).toBe('users');
      expect(cmd.filter).toEqual({ status: 'active' });
      expect(cmd.projection).toEqual({ name: 1, email: 1 });
      expect(cmd.sort).toEqual({ createdAt: -1 });
      expect(cmd.limit).toBe(10);
      expect(cmd.skip).toBe(5);
    } else {
      throw new Error('Expected find command');
    }
  });

  it('should decode insert command', () => {
    const body = createCommandBody({
      insert: 'products',
      documents: [{ name: 'Widget' }],
      ordered: false,
    });

    const cmd = decodeCommand(body);

    expect(cmd.name).toBe('insert');
    if (isInsertCommand(cmd)) {
      expect(cmd.collection).toBe('products');
      expect(cmd.documents).toEqual([{ name: 'Widget' }]);
      expect(cmd.ordered).toBe(false);
    } else {
      throw new Error('Expected insert command');
    }
  });

  it('should decode update command', () => {
    const body = createCommandBody({
      update: 'users',
      updates: [
        { q: { status: 'inactive' }, u: { $set: { status: 'active' } }, multi: true },
      ],
      ordered: true,
    });

    const cmd = decodeCommand(body);

    expect(cmd.name).toBe('update');
    if (isUpdateCommand(cmd)) {
      expect(cmd.collection).toBe('users');
      expect(cmd.updates).toHaveLength(1);
      expect(cmd.ordered).toBe(true);
    } else {
      throw new Error('Expected update command');
    }
  });

  it('should decode delete command', () => {
    const body = createCommandBody({
      delete: 'sessions',
      deletes: [{ q: { expired: true }, limit: 0 }],
    });

    const cmd = decodeCommand(body);

    expect(cmd.name).toBe('delete');
    if (isDeleteCommand(cmd)) {
      expect(cmd.collection).toBe('sessions');
      expect(cmd.deletes).toHaveLength(1);
    } else {
      throw new Error('Expected delete command');
    }
  });

  it('should decode aggregate command', () => {
    const body = createCommandBody({
      aggregate: 'orders',
      pipeline: [
        { $match: { status: 'completed' } },
        { $group: { _id: '$customer', total: { $sum: '$amount' } } },
      ],
      cursor: { batchSize: 100 },
      allowDiskUse: true,
    });

    const cmd = decodeCommand(body);

    expect(cmd.name).toBe('aggregate');
    if (isAggregateCommand(cmd)) {
      expect(cmd.collection).toBe('orders');
      expect(cmd.pipeline).toHaveLength(2);
      expect(cmd.cursor).toEqual({ batchSize: 100 });
      expect(cmd.allowDiskUse).toBe(true);
    } else {
      throw new Error('Expected aggregate command');
    }
  });
});

// ============================================================================
// Validation Tests
// ============================================================================

describe('Validation', () => {
  it('should require $db field', () => {
    const body = { find: 'users' } as Document;

    expect(() => decodeCommand(body)).toThrow(CommandValidationError);
    expect(() => decodeCommand(body)).toThrow(/\$db.*required/i);
  });

  it('should require non-empty collection name', () => {
    const body = createCommandBody({ find: '' });

    expect(() => decodeCommand(body)).toThrow(CommandValidationError);
    expect(() => decodeCommand(body)).toThrow(/cannot be empty/i);
  });

  it('should require collection name to be a string', () => {
    const body = createCommandBody({ find: 123 });

    expect(() => decodeCommand(body)).toThrow(CommandValidationError);
    expect(() => decodeCommand(body)).toThrow(/must be a string/i);
  });

  it('should require pipeline for aggregate', () => {
    const body = createCommandBody({ aggregate: 'users' });

    expect(() => decodeCommand(body)).toThrow(CommandValidationError);
    expect(() => decodeCommand(body)).toThrow(/pipeline.*array/i);
  });
});

// ============================================================================
// Type Guard Tests
// ============================================================================

describe('Type Guards', () => {
  it('should identify collection commands', () => {
    const findBody = createCommandBody({ find: 'users' });
    const findCmd = decodeCommand(findBody);
    expect(isCollectionCommand(findCmd)).toBe(true);

    const pingBody = createCommandBody({ ping: 1 }, 'admin');
    const pingCmd = decodeCommand(pingBody);
    expect(isCollectionCommand(pingCmd)).toBe(false);
  });

  it('should identify find commands', () => {
    const findBody = createCommandBody({ find: 'users' });
    const findCmd = decodeCommand(findBody);
    expect(isFindCommand(findCmd)).toBe(true);

    const insertBody = createCommandBody({ insert: 'users', documents: [] });
    const insertCmd = decodeCommand(insertBody);
    expect(isFindCommand(insertCmd)).toBe(false);
  });

  it('should identify insert commands', () => {
    const insertBody = createCommandBody({ insert: 'users', documents: [] });
    const insertCmd = decodeCommand(insertBody);
    expect(isInsertCommand(insertCmd)).toBe(true);

    const findBody = createCommandBody({ find: 'users' });
    const findCmd = decodeCommand(findBody);
    expect(isInsertCommand(findCmd)).toBe(false);
  });

  it('should identify update commands', () => {
    const updateBody = createCommandBody({ update: 'users', updates: [] });
    const updateCmd = decodeCommand(updateBody);
    expect(isUpdateCommand(updateCmd)).toBe(true);

    const findBody = createCommandBody({ find: 'users' });
    const findCmd = decodeCommand(findBody);
    expect(isUpdateCommand(findCmd)).toBe(false);
  });

  it('should identify delete commands', () => {
    const deleteBody = createCommandBody({ delete: 'users', deletes: [] });
    const deleteCmd = decodeCommand(deleteBody);
    expect(isDeleteCommand(deleteCmd)).toBe(true);

    const findBody = createCommandBody({ find: 'users' });
    const findCmd = decodeCommand(findBody);
    expect(isDeleteCommand(findCmd)).toBe(false);
  });

  it('should identify aggregate commands', () => {
    const aggBody = createCommandBody({ aggregate: 'users', pipeline: [] });
    const aggCmd = decodeCommand(aggBody);
    expect(isAggregateCommand(aggCmd)).toBe(true);

    const findBody = createCommandBody({ find: 'users' });
    const findCmd = decodeCommand(findBody);
    expect(isAggregateCommand(findCmd)).toBe(false);
  });

  it('should identify cursor commands', () => {
    const getMoreBody = createCommandBody({
      getMore: 12345n,
      collection: 'users',
    });
    const getMoreCmd = decodeCommand(getMoreBody);
    expect(isCursorCommand(getMoreCmd)).toBe(true);

    const findBody = createCommandBody({ find: 'users' });
    const findCmd = decodeCommand(findBody);
    expect(isCursorCommand(findCmd)).toBe(false);
  });
});

// ============================================================================
// Command Constants Tests
// ============================================================================

describe('Command Constants', () => {
  it('should define collection commands', () => {
    expect(COLLECTION_COMMANDS).toContain('find');
    expect(COLLECTION_COMMANDS).toContain('insert');
    expect(COLLECTION_COMMANDS).toContain('update');
    expect(COLLECTION_COMMANDS).toContain('delete');
    expect(COLLECTION_COMMANDS).toContain('aggregate');
    expect(COLLECTION_COMMANDS).toContain('count');
    expect(COLLECTION_COMMANDS).toContain('distinct');
  });

  it('should define admin commands', () => {
    expect(ADMIN_COMMANDS).toContain('ping');
    expect(ADMIN_COMMANDS).toContain('hello');
    expect(ADMIN_COMMANDS).toContain('isMaster');
    expect(ADMIN_COMMANDS).toContain('ismaster');
    expect(ADMIN_COMMANDS).toContain('listDatabases');
    expect(ADMIN_COMMANDS).toContain('listCollections');
  });

  it('should define cursor commands', () => {
    expect(CURSOR_COMMANDS).toContain('getMore');
    expect(CURSOR_COMMANDS).toContain('killCursors');
  });
});

// ============================================================================
// Edge Cases
// ============================================================================

describe('Edge Cases', () => {
  it('should handle unknown commands as generic admin commands', () => {
    const body = createCommandBody({ customCommand: 1 }, 'admin');
    const cmd = decodeCommand(body);

    expect(cmd.name).toBe('customCommand');
    expect(cmd.database).toBe('admin');
  });

  it('should preserve full body in decoded command', () => {
    const body = createCommandBody({
      find: 'users',
      filter: { status: 'active' },
      customOption: 'value',
    });

    const cmd = decodeCommand(body);

    expect(cmd.body).toEqual(body);
    expect(cmd.body.customOption).toBe('value');
  });

  it('should handle bigint cursor IDs', () => {
    const body = createCommandBody({
      getMore: BigInt('9007199254740993'),
      collection: 'users',
    });

    const cmd = decodeCommand(body);

    expect(cmd.name).toBe('getMore');
    expect((cmd as { cursorId: bigint }).cursorId).toBe(BigInt('9007199254740993'));
  });

  it('should handle number cursor IDs', () => {
    const body = createCommandBody({
      getMore: 12345,
      collection: 'users',
    });

    const cmd = decodeCommand(body);

    expect(cmd.name).toBe('getMore');
    expect((cmd as { cursorId: number }).cursorId).toBe(12345);
  });

  it('should handle special characters in collection names', () => {
    const body = createCommandBody({
      find: 'my-collection.with.dots',
    });

    const cmd = decodeCommand(body);
    if (isFindCommand(cmd)) {
      expect(cmd.collection).toBe('my-collection.with.dots');
    }
  });

  it('should handle unicode in filter values', () => {
    const body = createCommandBody({
      find: 'users',
      filter: { name: 'Javier Garcia' },
    });

    const cmd = decodeCommand(body);
    if (isFindCommand(cmd)) {
      expect(cmd.filter).toEqual({ name: 'Javier Garcia' });
    }
  });

  it('should handle null values in filter', () => {
    const body = createCommandBody({
      find: 'users',
      filter: { deletedAt: null },
    });

    const cmd = decodeCommand(body);
    if (isFindCommand(cmd)) {
      expect(cmd.filter).toEqual({ deletedAt: null });
    }
  });

  it('should handle Date objects in filter', () => {
    const date = new Date('2024-01-01T00:00:00Z');
    const body = createCommandBody({
      find: 'events',
      filter: { timestamp: { $gte: date } },
    });

    const cmd = decodeCommand(body);
    if (isFindCommand(cmd)) {
      expect(cmd.filter).toHaveProperty('timestamp');
    }
  });
});
