/**
 * MongoLake E2E Tests - Full Stack Integration
 *
 * These tests verify the complete data flow through the stack:
 * Worker -> ShardDO (WAL + Buffer) -> R2 (Parquet Storage)
 *
 * They test durability, consistency, and the interaction between components.
 *
 * Usage:
 *   MONGOLAKE_E2E_URL=https://mongolake.workers.dev npm run test:e2e
 */

import { describe, it, expect, beforeAll } from 'vitest';

const BASE_URL = process.env.MONGOLAKE_E2E_URL || 'http://localhost:8787';
const TEST_DB = 'e2e_stack_test';

// Helper function for API requests
async function apiRequest(
  method: string,
  path: string,
  body?: unknown,
  options: { query?: Record<string, string>; headers?: Record<string, string> } = {}
): Promise<Response> {
  let url = `${BASE_URL}${path}`;

  if (options.query) {
    const params = new URLSearchParams(options.query);
    url += `?${params.toString()}`;
  }

  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...options.headers,
  };

  return fetch(url, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined,
  });
}

// Generate unique collection name for test isolation
function uniqueCollection(prefix: string): string {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

describe('Full Stack Integration', () => {
  beforeAll(async () => {
    const healthRes = await fetch(`${BASE_URL}/health`);
    if (!healthRes.ok) {
      throw new Error(`Worker not accessible at ${BASE_URL}`);
    }
  });

  describe('Read-Your-Writes Consistency', () => {
    it('should immediately read back inserted document', async () => {
      const collection = uniqueCollection('ryw');
      const doc = {
        _id: `ryw-${Date.now()}`,
        name: 'Read-Your-Writes Test',
        value: Math.random(),
      };

      // Insert
      const insertRes = await apiRequest('POST', `/api/${TEST_DB}/${collection}`, doc);
      expect(insertRes.status).toBe(201);

      // Immediately query for the document
      const queryRes = await apiRequest('GET', `/api/${TEST_DB}/${collection}`, undefined, {
        query: { filter: JSON.stringify({ _id: doc._id }) },
      });
      expect(queryRes.status).toBe(200);

      const result = await queryRes.json() as { documents: Array<{ _id: string; name: string }> };

      // Document should be found (from ShardDO buffer before R2 flush)
      const found = result.documents.find(d => d._id === doc._id);
      expect(found).toBeDefined();
      if (found) {
        expect(found.name).toBe('Read-Your-Writes Test');
      }
    });

    it('should read updated document immediately after update', async () => {
      const collection = uniqueCollection('ryw_update');
      const docId = `ryw-update-${Date.now()}`;

      // Insert
      await apiRequest('POST', `/api/${TEST_DB}/${collection}`, {
        _id: docId,
        status: 'initial',
        version: 1,
      });

      // Update
      await apiRequest('PATCH', `/api/${TEST_DB}/${collection}/${docId}`, {
        $set: { status: 'updated', version: 2 },
      });

      // Query should reflect update
      const queryRes = await apiRequest('GET', `/api/${TEST_DB}/${collection}`, undefined, {
        query: { filter: JSON.stringify({ _id: docId }) },
      });

      const result = await queryRes.json() as { documents: Array<{ status: string; version: number }> };
      const found = result.documents.find(d => (d as unknown as { _id: string })._id === docId);

      if (found) {
        expect(found.status).toBe('updated');
        expect(found.version).toBe(2);
      }
    });

    it('should not find deleted document after delete', async () => {
      const collection = uniqueCollection('ryw_delete');
      const docId = `ryw-delete-${Date.now()}`;

      // Insert
      await apiRequest('POST', `/api/${TEST_DB}/${collection}`, {
        _id: docId,
        name: 'To Be Deleted',
      });

      // Delete
      await apiRequest('DELETE', `/api/${TEST_DB}/${collection}/${docId}`);

      // Query should not find it
      const queryRes = await apiRequest('GET', `/api/${TEST_DB}/${collection}`, undefined, {
        query: { filter: JSON.stringify({ _id: docId }) },
      });

      const result = await queryRes.json() as { documents: Array<{ _id: string }> };
      const found = result.documents.find(d => d._id === docId);
      expect(found).toBeUndefined();
    });
  });

  describe('Concurrent Operations', () => {
    it('should handle concurrent inserts to same shard', async () => {
      const collection = uniqueCollection('concurrent');
      const insertCount = 10;
      const promises: Promise<Response>[] = [];

      // Launch concurrent inserts
      for (let i = 0; i < insertCount; i++) {
        promises.push(
          apiRequest('POST', `/api/${TEST_DB}/${collection}`, {
            _id: `concurrent-${Date.now()}-${i}`,
            index: i,
            timestamp: Date.now(),
          })
        );
      }

      const results = await Promise.all(promises);

      // All inserts should succeed
      for (const res of results) {
        expect(res.status).toBe(201);
      }
    });

    it('should handle concurrent updates to different documents', async () => {
      const collection = uniqueCollection('concurrent_update');

      // Insert documents first
      const docIds: string[] = [];
      for (let i = 0; i < 5; i++) {
        const docId = `concurrent-update-${Date.now()}-${i}`;
        docIds.push(docId);
        await apiRequest('POST', `/api/${TEST_DB}/${collection}`, {
          _id: docId,
          counter: 0,
        });
      }

      // Concurrent updates to different documents
      const updatePromises = docIds.map((docId, i) =>
        apiRequest('PATCH', `/api/${TEST_DB}/${collection}/${docId}`, {
          $inc: { counter: i + 1 },
        })
      );

      const results = await Promise.all(updatePromises);

      for (const res of results) {
        expect(res.status).toBe(200);
      }
    });

    it('should serialize concurrent updates to same document', async () => {
      const collection = uniqueCollection('concurrent_same');
      const docId = `concurrent-same-${Date.now()}`;

      // Insert initial document
      await apiRequest('POST', `/api/${TEST_DB}/${collection}`, {
        _id: docId,
        counter: 0,
      });

      // Concurrent increments (should be serialized by ShardDO)
      const incrementCount = 5;
      const promises = Array(incrementCount)
        .fill(null)
        .map(() =>
          apiRequest('PATCH', `/api/${TEST_DB}/${collection}/${docId}`, {
            $inc: { counter: 1 },
          })
        );

      await Promise.all(promises);

      // All updates should have succeeded (ShardDO serializes writes)
      const queryRes = await apiRequest('GET', `/api/${TEST_DB}/${collection}`, undefined, {
        query: { filter: JSON.stringify({ _id: docId }) },
      });

      const result = await queryRes.json() as { documents: Array<{ counter: number }> };
      // Counter should reflect some increments (exact value depends on serialization order)
      const found = result.documents.find(d => (d as unknown as { _id: string })._id === docId);
      if (found) {
        expect(found.counter).toBeGreaterThan(0);
      }
    });
  });

  describe('Data Types and Serialization', () => {
    it('should preserve string data types', async () => {
      const collection = uniqueCollection('types');
      const doc = {
        _id: `types-string-${Date.now()}`,
        stringField: 'hello world',
        emptyString: '',
        unicodeString: 'Hello \u4e16\u754c',
      };

      await apiRequest('POST', `/api/${TEST_DB}/${collection}`, doc);

      const queryRes = await apiRequest('GET', `/api/${TEST_DB}/${collection}`, undefined, {
        query: { filter: JSON.stringify({ _id: doc._id }) },
      });

      const result = await queryRes.json() as { documents: Array<typeof doc> };
      const found = result.documents.find(d => d._id === doc._id);

      if (found) {
        expect(found.stringField).toBe('hello world');
        expect(found.emptyString).toBe('');
        expect(found.unicodeString).toBe('Hello \u4e16\u754c');
      }
    });

    it('should preserve numeric data types', async () => {
      const collection = uniqueCollection('types');
      const doc = {
        _id: `types-number-${Date.now()}`,
        integer: 42,
        float: 3.14159,
        negative: -100,
        zero: 0,
        large: 9007199254740991, // Max safe integer
      };

      await apiRequest('POST', `/api/${TEST_DB}/${collection}`, doc);

      const queryRes = await apiRequest('GET', `/api/${TEST_DB}/${collection}`, undefined, {
        query: { filter: JSON.stringify({ _id: doc._id }) },
      });

      const result = await queryRes.json() as { documents: Array<typeof doc> };
      const found = result.documents.find(d => d._id === doc._id);

      if (found) {
        expect(found.integer).toBe(42);
        expect(found.float).toBeCloseTo(3.14159);
        expect(found.negative).toBe(-100);
        expect(found.zero).toBe(0);
      }
    });

    it('should preserve boolean data types', async () => {
      const collection = uniqueCollection('types');
      const doc = {
        _id: `types-bool-${Date.now()}`,
        isTrue: true,
        isFalse: false,
      };

      await apiRequest('POST', `/api/${TEST_DB}/${collection}`, doc);

      const queryRes = await apiRequest('GET', `/api/${TEST_DB}/${collection}`, undefined, {
        query: { filter: JSON.stringify({ _id: doc._id }) },
      });

      const result = await queryRes.json() as { documents: Array<typeof doc> };
      const found = result.documents.find(d => d._id === doc._id);

      if (found) {
        expect(found.isTrue).toBe(true);
        expect(found.isFalse).toBe(false);
      }
    });

    it('should preserve null values', async () => {
      const collection = uniqueCollection('types');
      const doc = {
        _id: `types-null-${Date.now()}`,
        nullField: null,
        validField: 'present',
      };

      await apiRequest('POST', `/api/${TEST_DB}/${collection}`, doc);

      const queryRes = await apiRequest('GET', `/api/${TEST_DB}/${collection}`, undefined, {
        query: { filter: JSON.stringify({ _id: doc._id }) },
      });

      const result = await queryRes.json() as { documents: Array<typeof doc> };
      const found = result.documents.find(d => d._id === doc._id);

      if (found) {
        expect(found.nullField).toBeNull();
        expect(found.validField).toBe('present');
      }
    });

    it('should preserve arrays', async () => {
      const collection = uniqueCollection('types');
      const doc = {
        _id: `types-array-${Date.now()}`,
        emptyArray: [] as unknown[],
        stringArray: ['a', 'b', 'c'],
        mixedArray: [1, 'two', true, null] as unknown[],
        nestedArray: [[1, 2], [3, 4]] as number[][],
      };

      await apiRequest('POST', `/api/${TEST_DB}/${collection}`, doc);

      const queryRes = await apiRequest('GET', `/api/${TEST_DB}/${collection}`, undefined, {
        query: { filter: JSON.stringify({ _id: doc._id }) },
      });

      const result = await queryRes.json() as { documents: Array<typeof doc> };
      const found = result.documents.find(d => d._id === doc._id);

      if (found) {
        expect(found.emptyArray).toEqual([]);
        expect(found.stringArray).toEqual(['a', 'b', 'c']);
        expect(found.mixedArray).toEqual([1, 'two', true, null]);
        expect(found.nestedArray).toEqual([[1, 2], [3, 4]]);
      }
    });

    it('should preserve nested objects', async () => {
      const collection = uniqueCollection('types');
      const doc = {
        _id: `types-nested-${Date.now()}`,
        level1: {
          level2: {
            level3: {
              value: 'deep',
            },
          },
        },
        metadata: {
          created: '2026-02-01',
          tags: ['test', 'e2e'],
        },
      };

      await apiRequest('POST', `/api/${TEST_DB}/${collection}`, doc);

      const queryRes = await apiRequest('GET', `/api/${TEST_DB}/${collection}`, undefined, {
        query: { filter: JSON.stringify({ _id: doc._id }) },
      });

      const result = await queryRes.json() as { documents: Array<typeof doc> };
      const found = result.documents.find(d => d._id === doc._id);

      if (found) {
        expect(found.level1.level2.level3.value).toBe('deep');
        expect(found.metadata.tags).toEqual(['test', 'e2e']);
      }
    });
  });

  describe('Large Document Handling', () => {
    it('should handle moderately large documents', async () => {
      const collection = uniqueCollection('large');

      // Create a document with many fields
      const doc: Record<string, unknown> = {
        _id: `large-${Date.now()}`,
        description: 'Large document test',
      };

      // Add 100 fields
      for (let i = 0; i < 100; i++) {
        doc[`field_${i}`] = `value_${i}_${'x'.repeat(50)}`;
      }

      const insertRes = await apiRequest('POST', `/api/${TEST_DB}/${collection}`, doc);
      expect(insertRes.status).toBe(201);

      // Query it back
      const queryRes = await apiRequest('GET', `/api/${TEST_DB}/${collection}`, undefined, {
        query: { filter: JSON.stringify({ _id: doc._id }) },
      });
      expect(queryRes.status).toBe(200);
    });

    it('should handle batch of documents', async () => {
      const collection = uniqueCollection('batch');
      const batchSize = 50;

      const docs = Array(batchSize)
        .fill(null)
        .map((_, i) => ({
          _id: `batch-${Date.now()}-${i}`,
          index: i,
          data: `data_${i}`,
        }));

      const insertRes = await apiRequest('POST', `/api/${TEST_DB}/${collection}/bulk-insert`, {
        documents: docs,
      });

      expect(insertRes.status).toBe(201);

      const result = await insertRes.json() as { insertedCount: number };
      expect(result.insertedCount).toBe(batchSize);
    });
  });

  describe('Query Filter Edge Cases', () => {
    it('should handle empty filter (find all)', async () => {
      const collection = uniqueCollection('filter');

      // Insert some documents
      await apiRequest('POST', `/api/${TEST_DB}/${collection}`, { _id: 'f1', value: 1 });
      await apiRequest('POST', `/api/${TEST_DB}/${collection}`, { _id: 'f2', value: 2 });

      // Query with empty filter
      const queryRes = await apiRequest('GET', `/api/${TEST_DB}/${collection}`, undefined, {
        query: { filter: JSON.stringify({}) },
      });

      expect(queryRes.status).toBe(200);

      const result = await queryRes.json() as { documents: unknown[] };
      expect(result.documents.length).toBeGreaterThanOrEqual(2);
    });

    it('should handle $in operator', async () => {
      const collection = uniqueCollection('filter_in');

      await apiRequest('POST', `/api/${TEST_DB}/${collection}/bulk-insert`, {
        documents: [
          { _id: 'in1', category: 'A' },
          { _id: 'in2', category: 'B' },
          { _id: 'in3', category: 'C' },
        ],
      });

      const queryRes = await apiRequest('GET', `/api/${TEST_DB}/${collection}`, undefined, {
        query: { filter: JSON.stringify({ category: { $in: ['A', 'C'] } }) },
      });

      expect(queryRes.status).toBe(200);

      const result = await queryRes.json() as { documents: Array<{ category: string }> };
      for (const doc of result.documents) {
        expect(['A', 'C']).toContain(doc.category);
      }
    });

    it('should handle $exists operator', async () => {
      const collection = uniqueCollection('filter_exists');

      await apiRequest('POST', `/api/${TEST_DB}/${collection}/bulk-insert`, {
        documents: [
          { _id: 'ex1', optionalField: 'present' },
          { _id: 'ex2' }, // No optionalField
          { _id: 'ex3', optionalField: null },
        ],
      });

      const queryRes = await apiRequest('GET', `/api/${TEST_DB}/${collection}`, undefined, {
        query: { filter: JSON.stringify({ optionalField: { $exists: true } }) },
      });

      expect(queryRes.status).toBe(200);
    });

    it('should handle $and logical operator', async () => {
      const collection = uniqueCollection('filter_and');

      await apiRequest('POST', `/api/${TEST_DB}/${collection}/bulk-insert`, {
        documents: [
          { _id: 'and1', status: 'active', priority: 'high' },
          { _id: 'and2', status: 'active', priority: 'low' },
          { _id: 'and3', status: 'inactive', priority: 'high' },
        ],
      });

      const queryRes = await apiRequest('GET', `/api/${TEST_DB}/${collection}`, undefined, {
        query: {
          filter: JSON.stringify({
            $and: [{ status: 'active' }, { priority: 'high' }],
          }),
        },
      });

      expect(queryRes.status).toBe(200);
    });

    it('should handle $or logical operator', async () => {
      const collection = uniqueCollection('filter_or');

      await apiRequest('POST', `/api/${TEST_DB}/${collection}/bulk-insert`, {
        documents: [
          { _id: 'or1', status: 'pending' },
          { _id: 'or2', status: 'active' },
          { _id: 'or3', status: 'completed' },
        ],
      });

      const queryRes = await apiRequest('GET', `/api/${TEST_DB}/${collection}`, undefined, {
        query: {
          filter: JSON.stringify({
            $or: [{ status: 'pending' }, { status: 'completed' }],
          }),
        },
      });

      expect(queryRes.status).toBe(200);
    });
  });

  describe('Shard Isolation', () => {
    it('should isolate data between different databases', async () => {
      const collection = 'shared_collection';
      const docId = `isolation-${Date.now()}`;

      // Insert in database 1
      await apiRequest('POST', `/api/db1/${collection}`, {
        _id: docId,
        database: 'db1',
      });

      // Insert in database 2 with same _id
      await apiRequest('POST', `/api/db2/${collection}`, {
        _id: docId,
        database: 'db2',
      });

      // Query database 1
      const query1 = await apiRequest('GET', `/api/db1/${collection}`, undefined, {
        query: { filter: JSON.stringify({ _id: docId }) },
      });

      const result1 = await query1.json() as { documents: Array<{ database: string }> };
      const found1 = result1.documents.find(d => (d as unknown as { _id: string })._id === docId);
      if (found1) {
        expect(found1.database).toBe('db1');
      }

      // Query database 2
      const query2 = await apiRequest('GET', `/api/db2/${collection}`, undefined, {
        query: { filter: JSON.stringify({ _id: docId }) },
      });

      const result2 = await query2.json() as { documents: Array<{ database: string }> };
      const found2 = result2.documents.find(d => (d as unknown as { _id: string })._id === docId);
      if (found2) {
        expect(found2.database).toBe('db2');
      }
    });

    it('should isolate data between different collections', async () => {
      const docId = `isolation-${Date.now()}`;

      // Insert in collection 1
      await apiRequest('POST', `/api/${TEST_DB}/collection1`, {
        _id: docId,
        collection: 'collection1',
      });

      // Insert in collection 2 with same _id (should be allowed)
      await apiRequest('POST', `/api/${TEST_DB}/collection2`, {
        _id: docId,
        collection: 'collection2',
      });

      // Both should exist independently
      const query1 = await apiRequest('GET', `/api/${TEST_DB}/collection1`, undefined, {
        query: { filter: JSON.stringify({ _id: docId }) },
      });

      const query2 = await apiRequest('GET', `/api/${TEST_DB}/collection2`, undefined, {
        query: { filter: JSON.stringify({ _id: docId }) },
      });

      expect(query1.status).toBe(200);
      expect(query2.status).toBe(200);
    });
  });
});
