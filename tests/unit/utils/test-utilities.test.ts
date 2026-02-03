/**
 * Test Utilities Tests
 *
 * Tests to verify that the test utility functions work correctly.
 * Also serves as documentation for how to use the utilities.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  // Factories
  createObjectId,
  createObjectIdString,
  createObjectIdFromDate,
  createObjectIds,
  createDate,
  createPastDate,
  createFutureDate,
  createDateAt,
  createUser,
  createUsers,
  createOrder,
  createProduct,
  createDeduplicationDoc,
  createDeduplicationSequence,
  createAddress,
  createNestedDocument,
  createDocumentWithPath,
  createBatch,
  createDocumentWithManyKeys,
  createLargeDocument,
  resetDocumentCounter,
  // Mocks
  createMockStorage,
  createMockR2Bucket,
  createMockFetch,
  createMockEnv,
  createMockRequest,
  // Assertions
  assertValidObjectId,
  assertDocumentId,
  assertDocumentFields,
  assertContainsDocumentWithId,
  assertSortedBy,
  assertInsertSuccess,
  assertUpdateSuccess,
  assertDeduplicationStats,
  assertCompletesWithin,
  // Fixtures
  OBJECT_IDS,
  USERS,
  PRODUCTS,
  FILTERS,
  UPDATES,
  DATES,
  getAllUsers,
  generateLargeUserDataset,
} from '../../../tests/utils/index.js';
import { ObjectId } from '../../../src/types.js';

// ============================================================================
// Factory Tests
// ============================================================================

describe('Test Factories', () => {
  beforeEach(() => {
    resetDocumentCounter();
  });

  describe('ObjectId Factories', () => {
    it('should create valid ObjectId', () => {
      const oid = createObjectId();
      expect(oid).toBeInstanceOf(ObjectId);
      expect(oid.toString()).toHaveLength(24);
    });

    it('should create valid ObjectId string', () => {
      const hex = createObjectIdString();
      expect(typeof hex).toBe('string');
      expect(hex).toHaveLength(24);
      expect(ObjectId.isValid(hex)).toBe(true);
    });

    it('should create ObjectId from date', () => {
      const date = new Date('2024-01-01T00:00:00Z');
      const oid = createObjectIdFromDate(date);
      const timestamp = oid.getTimestamp();
      expect(timestamp.getTime()).toBe(date.getTime());
    });

    it('should create multiple unique ObjectIds', () => {
      const oids = createObjectIds(10);
      expect(oids).toHaveLength(10);
      const uniqueIds = new Set(oids.map((o) => o.toString()));
      expect(uniqueIds.size).toBe(10);
    });
  });

  describe('Date Factories', () => {
    it('should create date with offset', () => {
      const before = Date.now();
      const date = createDate(1000);
      const after = Date.now();
      expect(date.getTime()).toBeGreaterThanOrEqual(before + 1000);
      expect(date.getTime()).toBeLessThanOrEqual(after + 1000);
    });

    it('should create past date', () => {
      const date = createPastDate(7);
      const expected = Date.now() - 7 * 24 * 60 * 60 * 1000;
      expect(Math.abs(date.getTime() - expected)).toBeLessThan(1000);
    });

    it('should create future date', () => {
      const date = createFutureDate(7);
      const expected = Date.now() + 7 * 24 * 60 * 60 * 1000;
      expect(Math.abs(date.getTime() - expected)).toBeLessThan(1000);
    });

    it('should create date at specific time', () => {
      const date = createDateAt(2024, 6, 15, 12, 30, 45);
      expect(date.getFullYear()).toBe(2024);
      expect(date.getMonth()).toBe(5); // 0-indexed
      expect(date.getDate()).toBe(15);
      expect(date.getHours()).toBe(12);
      expect(date.getMinutes()).toBe(30);
      expect(date.getSeconds()).toBe(45);
    });
  });

  describe('Document Factories', () => {
    it('should create user with defaults', () => {
      const user = createUser();
      expect(user._id).toBeDefined();
      expect(user.name).toBeDefined();
      expect(user.email).toBeDefined();
      expect(user.status).toBe('active');
    });

    it('should create user with overrides', () => {
      const user = createUser({ name: 'Custom Name', age: 99 });
      expect(user.name).toBe('Custom Name');
      expect(user.age).toBe(99);
    });

    it('should create multiple users', () => {
      const users = createUsers(5);
      expect(users).toHaveLength(5);
      const uniqueIds = new Set(users.map((u) => u._id));
      expect(uniqueIds.size).toBe(5);
    });

    it('should create order with items', () => {
      const order = createOrder();
      expect(order._id).toBeDefined();
      expect(order.items.length).toBeGreaterThan(0);
      expect(order.total).toBeGreaterThan(0);
      expect(order.status).toBe('pending');
    });

    it('should create product', () => {
      const product = createProduct({ price: 29.99, category: 'electronics' });
      expect(product.price).toBe(29.99);
      expect(product.category).toBe('electronics');
    });

    it('should create deduplication document', () => {
      const doc = createDeduplicationDoc('doc1', 5, 'u', { name: 'Test' });
      expect(doc._id).toBe('doc1');
      expect(doc._seq).toBe(5);
      expect(doc._op).toBe('u');
      expect(doc.name).toBe('Test');
    });

    it('should create deduplication sequence', () => {
      const docs = createDeduplicationSequence('doc1', [1, 3, 5], 'd');
      expect(docs).toHaveLength(3);
      expect(docs[0]._op).toBe('i');
      expect(docs[1]._op).toBe('u');
      expect(docs[2]._op).toBe('d');
    });

    it('should create address', () => {
      const address = createAddress({ city: 'Boston' });
      expect(address.city).toBe('Boston');
      expect(address.country).toBe('USA');
    });
  });

  describe('Nested Document Factories', () => {
    it('should create nested document', () => {
      const doc = createNestedDocument(3, 'deep');
      expect(doc.nested.nested.nested.value).toBe('deep');
    });

    it('should create document with path', () => {
      const doc = createDocumentWithPath('a.b.c', 'value');
      expect(doc.a.b.c).toBe('value');
    });
  });

  describe('Bulk Factories', () => {
    it('should create batch', () => {
      const items = createBatch(5, (i) => ({ id: i, name: `Item ${i}` }));
      expect(items).toHaveLength(5);
      expect(items[2].id).toBe(2);
      expect(items[2].name).toBe('Item 2');
    });

    it('should create document with many keys', () => {
      const doc = createDocumentWithManyKeys(100);
      expect(Object.keys(doc).length).toBe(101); // 100 keys + _id
      expect(doc.key50).toBe('value50');
    });

    it('should create large document', () => {
      const doc = createLargeDocument(10000);
      expect(doc.data.length).toBe(10000);
    });
  });
});

// ============================================================================
// Mock Tests
// ============================================================================

describe('Test Mocks', () => {
  describe('MockStorage', () => {
    it('should store and retrieve data', async () => {
      const storage = createMockStorage();
      await storage.put('key1', new Uint8Array([1, 2, 3]));
      const result = await storage.get('key1');
      expect(result).toEqual(new Uint8Array([1, 2, 3]));
    });

    it('should return null for missing key', async () => {
      const storage = createMockStorage();
      const result = await storage.get('missing');
      expect(result).toBeNull();
    });

    it('should list keys by prefix', async () => {
      const storage = createMockStorage();
      await storage.put('prefix/a', new Uint8Array([1]));
      await storage.put('prefix/b', new Uint8Array([2]));
      await storage.put('other/c', new Uint8Array([3]));
      const keys = await storage.list('prefix/');
      expect(keys).toHaveLength(2);
      expect(keys).toContain('prefix/a');
      expect(keys).toContain('prefix/b');
    });

    it('should delete key', async () => {
      const storage = createMockStorage();
      await storage.put('key1', new Uint8Array([1]));
      await storage.delete('key1');
      expect(await storage.exists('key1')).toBe(false);
    });
  });

  describe('MockR2Bucket', () => {
    it('should simulate R2 operations', async () => {
      const bucket = createMockR2Bucket();
      await bucket.put('test.txt', 'Hello, World!');
      const obj = await bucket.get('test.txt');
      expect(obj).not.toBeNull();
      expect(await obj!.text()).toBe('Hello, World!');
    });

    it('should handle multipart upload', async () => {
      const bucket = createMockR2Bucket();
      const upload = await bucket.createMultipartUpload('large.bin');
      const part1 = await upload.uploadPart(1, new Uint8Array([1, 2]));
      const part2 = await upload.uploadPart(2, new Uint8Array([3, 4]));
      await upload.complete([part1, part2]);
      const result = await bucket.get('large.bin');
      const buffer = await result!.arrayBuffer();
      expect(new Uint8Array(buffer)).toEqual(new Uint8Array([1, 2, 3, 4]));
    });
  });

  describe('MockFetch', () => {
    it('should mock fetch responses', async () => {
      const mockFetch = createMockFetch();
      mockFetch.mockJsonResponse({ success: true }, 200);
      const response = await mockFetch.fn('https://api.example.com/data');
      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.success).toBe(true);
    });

    it('should mock fetch errors', async () => {
      const mockFetch = createMockFetch();
      mockFetch.mockError(new Error('Network failure'));
      await expect(mockFetch.fn('https://api.example.com')).rejects.toThrow('Network failure');
    });

    it('should track calls', async () => {
      const mockFetch = createMockFetch();
      mockFetch.mockResponse(200);
      await mockFetch.fn('https://api.example.com/endpoint', { method: 'POST' });
      expect(mockFetch.calls).toHaveLength(1);
      expect(mockFetch.calls[0].url).toBe('https://api.example.com/endpoint');
    });
  });

  describe('MockEnv', () => {
    it('should create valid worker environment', () => {
      const env = createMockEnv();
      expect(env.BUCKET).toBeDefined();
      expect(env.RPC_NAMESPACE).toBeDefined();
      expect(env.OAUTH_SECRET).toBe('test-secret');
    });

    it('should support options', () => {
      const env = createMockEnv({ requireAuth: true, environment: 'production' });
      expect(env.REQUIRE_AUTH).toBe(true);
      expect(env.ENVIRONMENT).toBe('production');
    });
  });

  describe('MockRequest', () => {
    it('should create GET request', () => {
      const req = createMockRequest('GET', '/api/users');
      expect(req.method).toBe('GET');
      expect(new URL(req.url).pathname).toBe('/api/users');
    });

    it('should create POST request with body', async () => {
      const req = createMockRequest('POST', '/api/users', { name: 'Alice' });
      expect(req.method).toBe('POST');
      const body = await req.json();
      expect(body.name).toBe('Alice');
    });

    it('should include custom headers', () => {
      const req = createMockRequest('GET', '/api/users', undefined, { Authorization: 'Bearer token' });
      expect(req.headers.get('Authorization')).toBe('Bearer token');
    });
  });
});

// ============================================================================
// Assertion Tests
// ============================================================================

describe('Custom Assertions', () => {
  describe('ObjectId Assertions', () => {
    it('assertValidObjectId should pass for valid hex', () => {
      assertValidObjectId('507f1f77bcf86cd799439011');
    });

    it('assertValidObjectId should fail for invalid hex', () => {
      expect(() => assertValidObjectId('invalid')).toThrow();
    });
  });

  describe('Document Assertions', () => {
    it('assertDocumentId should verify _id', () => {
      const doc = { _id: 'doc1', name: 'Test' };
      assertDocumentId(doc, 'doc1');
    });

    it('assertDocumentFields should verify fields', () => {
      const doc = { _id: 'doc1', name: 'Alice', age: 30 };
      assertDocumentFields(doc, { name: 'Alice', age: 30 });
    });

    it('assertContainsDocumentWithId should find document', () => {
      const docs = [
        { _id: 'doc1', name: 'Alice' },
        { _id: 'doc2', name: 'Bob' },
      ];
      assertContainsDocumentWithId(docs, 'doc2');
    });

    it('assertSortedBy should verify sort order', () => {
      const docs = [
        { _id: 'a', age: 20 },
        { _id: 'b', age: 25 },
        { _id: 'c', age: 30 },
      ];
      assertSortedBy(docs, 'age', 'asc');
    });
  });

  describe('Operation Result Assertions', () => {
    it('assertInsertSuccess should verify insert result', () => {
      const result = { acknowledged: true, insertedId: 'doc1' };
      assertInsertSuccess(result, 'doc1');
    });

    it('assertUpdateSuccess should verify update result', () => {
      const result = { acknowledged: true, matchedCount: 1, modifiedCount: 1 };
      assertUpdateSuccess(result, 1, 1);
    });

    it('assertDeduplicationStats should verify stats', () => {
      const result = {
        documents: [],
        stats: {
          inputCount: 10,
          outputCount: 5,
          duplicatesRemoved: 4,
          deletesFiltered: 1,
        },
      };
      assertDeduplicationStats(result, {
        inputCount: 10,
        outputCount: 5,
        duplicatesRemoved: 4,
        deletesFiltered: 1,
      });
    });
  });

  describe('Timing Assertions', () => {
    it('assertCompletesWithin should pass for fast operations', async () => {
      const result = await assertCompletesWithin(async () => {
        return 'done';
      }, 1000);
      expect(result).toBe('done');
    });
  });
});

// ============================================================================
// Fixture Tests
// ============================================================================

describe('Test Fixtures', () => {
  describe('ObjectId Fixtures', () => {
    it('should provide valid ObjectIds', () => {
      expect(ObjectId.isValid(OBJECT_IDS.TEST_1)).toBe(true);
      expect(ObjectId.isValid(OBJECT_IDS.TEST_2)).toBe(true);
    });

    it('should identify invalid ObjectIds', () => {
      expect(ObjectId.isValid(OBJECT_IDS.INVALID_SHORT)).toBe(false);
      expect(ObjectId.isValid(OBJECT_IDS.INVALID_CHARS)).toBe(false);
    });
  });

  describe('User Fixtures', () => {
    it('should provide complete user data', () => {
      expect(USERS.alice._id).toBe('user-alice');
      expect(USERS.alice.name).toBe('Alice Smith');
      expect(USERS.alice.status).toBe('active');
    });

    it('should provide users with different statuses', () => {
      expect(USERS.alice.status).toBe('active');
      expect(USERS.charlie.status).toBe('inactive');
      expect(USERS.diana.status).toBe('pending');
    });

    it('getAllUsers should return all fixtures', () => {
      const users = getAllUsers();
      expect(users.length).toBe(4);
    });
  });

  describe('Product Fixtures', () => {
    it('should provide product data', () => {
      expect(PRODUCTS.laptop.price).toBe(1299.99);
      expect(PRODUCTS.laptop.category).toBe('electronics');
    });

    it('should include out of stock product', () => {
      expect(PRODUCTS.outOfStock.inventory).toBe(0);
    });
  });

  describe('Filter Fixtures', () => {
    it('should provide filter helpers', () => {
      expect(FILTERS.matchAll).toEqual({});
      expect(FILTERS.byId('test')).toEqual({ _id: 'test' });
      expect(FILTERS.comparison.greaterThan('age', 21)).toEqual({ age: { $gt: 21 } });
    });

    it('should support complex filters', () => {
      const filter = FILTERS.logical.and([
        FILTERS.byField('status', 'active'),
        FILTERS.comparison.between('age', 18, 65),
      ]);
      expect(filter).toEqual({
        $and: [{ status: 'active' }, { age: { $gte: 18, $lte: 65 } }],
      });
    });
  });

  describe('Update Fixtures', () => {
    it('should provide update operators', () => {
      expect(UPDATES.set({ name: 'New' })).toEqual({ $set: { name: 'New' } });
      expect(UPDATES.inc('count', 5)).toEqual({ $inc: { count: 5 } });
      expect(UPDATES.push('tags', 'new')).toEqual({ $push: { tags: 'new' } });
    });

    it('should support complex updates', () => {
      const update = UPDATES.complex(
        { name: 'Updated' },
        { views: 1 },
        ['temp']
      );
      expect(update).toEqual({
        $set: { name: 'Updated' },
        $inc: { views: 1 },
        $unset: { temp: '' },
      });
    });
  });

  describe('Date Fixtures', () => {
    it('should provide well-known dates', () => {
      expect(DATES.epoch.getTime()).toBe(0);
      expect(DATES.y2k.getUTCFullYear()).toBe(2000);
      expect(DATES.start2024.getUTCFullYear()).toBe(2024);
    });
  });

  describe('Large Dataset Generators', () => {
    it('should generate large user dataset', () => {
      const users = generateLargeUserDataset(100);
      expect(users).toHaveLength(100);
      expect(users[0]._id).toBe('user-0');
      expect(users[99]._id).toBe('user-99');
    });
  });
});
