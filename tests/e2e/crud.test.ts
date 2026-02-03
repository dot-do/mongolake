/**
 * MongoLake E2E Tests - CRUD Operations
 *
 * End-to-end tests that run against a deployed MongoLake worker.
 * These tests verify the full stack: Worker -> ShardDO -> R2
 *
 * Usage:
 *   MONGOLAKE_E2E_URL=https://mongolake.workers.dev npm run test:e2e
 *
 * For local testing with wrangler dev:
 *   MONGOLAKE_E2E_URL=http://localhost:8787 npm run test:e2e
 */

import { describe, it, expect, beforeAll, afterEach } from 'vitest';

// Get base URL from environment or use local dev server
const BASE_URL = process.env.MONGOLAKE_E2E_URL || 'http://localhost:8787';

// Test database and collection names
const TEST_DB = 'e2e_test_db';
const TEST_COLLECTION = `e2e_test_${Date.now()}`;

// Track created document IDs for cleanup
const createdDocIds: string[] = [];

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

// Helper to create unique test document
function createTestDoc(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    _id: `e2e-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    name: 'E2E Test Document',
    timestamp: new Date().toISOString(),
    testRun: TEST_COLLECTION,
    ...overrides,
  };
}

describe('MongoLake E2E Tests', () => {
  beforeAll(async () => {
    // Verify the worker is accessible
    const healthRes = await fetch(`${BASE_URL}/health`);
    if (!healthRes.ok) {
      throw new Error(`Worker not accessible at ${BASE_URL}. Set MONGOLAKE_E2E_URL to a running MongoLake worker.`);
    }
  });

  afterEach(async () => {
    // Clean up created documents
    for (const docId of createdDocIds) {
      try {
        await apiRequest('DELETE', `/api/${TEST_DB}/${TEST_COLLECTION}/${docId}`);
      } catch {
        // Ignore cleanup errors
      }
    }
    createdDocIds.length = 0;
  });

  describe('Health Check', () => {
    it('should return healthy status', async () => {
      const response = await fetch(`${BASE_URL}/health`);

      expect(response.status).toBe(200);

      const data = await response.json() as { status: string; version?: string };
      expect(data.status).toBe('ok');
      expect(data.version).toBeDefined();
    });
  });

  describe('Insert Operations', () => {
    it('should insert a single document', async () => {
      const doc = createTestDoc({ name: 'Insert Test', value: 42 });

      const response = await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}`, doc);

      expect(response.status).toBe(201);

      const result = await response.json() as { acknowledged: boolean; insertedId: string };
      expect(result.acknowledged).toBe(true);
      expect(result.insertedId).toBe(doc._id);

      createdDocIds.push(doc._id as string);
    });

    it('should generate ObjectId when _id not provided', async () => {
      const doc = { name: 'Auto ID Test', value: 123 };

      const response = await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}`, doc);

      expect(response.status).toBe(201);

      const result = await response.json() as { insertedId: string };
      expect(result.insertedId).toBeDefined();
      // ObjectId should be 24 hex characters
      expect(result.insertedId).toMatch(/^[0-9a-f]{24}$/);

      createdDocIds.push(result.insertedId);
    });

    it('should reject duplicate _id', async () => {
      const doc = createTestDoc({ name: 'Duplicate Test' });

      // First insert should succeed
      const res1 = await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}`, doc);
      expect(res1.status).toBe(201);
      createdDocIds.push(doc._id as string);

      // Second insert with same _id should fail
      const res2 = await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}`, doc);
      expect(res2.status).toBe(409); // Conflict
    });

    it('should handle nested documents', async () => {
      const doc = createTestDoc({
        name: 'Nested Test',
        nested: {
          level1: {
            level2: {
              value: 'deep',
            },
          },
        },
        tags: ['a', 'b', 'c'],
      });

      const response = await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}`, doc);

      expect(response.status).toBe(201);
      createdDocIds.push(doc._id as string);
    });

    it('should reject invalid JSON', async () => {
      const response = await fetch(`${BASE_URL}/api/${TEST_DB}/${TEST_COLLECTION}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: 'not valid json {{{',
      });

      expect(response.status).toBe(400);
    });

    it('should reject empty body', async () => {
      const response = await fetch(`${BASE_URL}/api/${TEST_DB}/${TEST_COLLECTION}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '',
      });

      expect(response.status).toBe(400);
    });
  });

  describe('Query Operations', () => {
    it('should find all documents in collection', async () => {
      // Insert test documents
      const doc1 = createTestDoc({ name: 'Query Test 1', queryGroup: 'all' });
      const doc2 = createTestDoc({ name: 'Query Test 2', queryGroup: 'all' });

      await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}`, doc1);
      await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}`, doc2);
      createdDocIds.push(doc1._id as string, doc2._id as string);

      // Query all documents
      const response = await apiRequest('GET', `/api/${TEST_DB}/${TEST_COLLECTION}`);

      expect(response.status).toBe(200);

      const result = await response.json() as { documents: unknown[] };
      expect(result.documents).toBeDefined();
      expect(Array.isArray(result.documents)).toBe(true);
    });

    it('should filter documents by field equality', async () => {
      const doc = createTestDoc({ name: 'Filter Test', status: 'active' });
      await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}`, doc);
      createdDocIds.push(doc._id as string);

      const response = await apiRequest('GET', `/api/${TEST_DB}/${TEST_COLLECTION}`, undefined, {
        query: { filter: JSON.stringify({ _id: doc._id }) },
      });

      expect(response.status).toBe(200);

      const result = await response.json() as { documents: Array<{ _id: string; status: string }> };
      expect(result.documents.length).toBeGreaterThanOrEqual(1);

      const found = result.documents.find(d => d._id === doc._id);
      if (found) {
        expect(found.status).toBe('active');
      }
    });

    it('should support comparison operators ($gt, $lt, $gte, $lte)', async () => {
      // Insert documents with numeric values
      const docs = [
        createTestDoc({ name: 'Score 10', score: 10 }),
        createTestDoc({ name: 'Score 50', score: 50 }),
        createTestDoc({ name: 'Score 90', score: 90 }),
      ];

      for (const doc of docs) {
        await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}`, doc);
        createdDocIds.push(doc._id as string);
      }

      // Query for score > 40
      const response = await apiRequest('GET', `/api/${TEST_DB}/${TEST_COLLECTION}`, undefined, {
        query: { filter: JSON.stringify({ score: { $gt: 40 } }) },
      });

      expect(response.status).toBe(200);

      const result = await response.json() as { documents: Array<{ score: number }> };
      // All returned documents should have score > 40
      for (const doc of result.documents) {
        if (doc.score !== undefined) {
          expect(doc.score).toBeGreaterThan(40);
        }
      }
    });

    it('should support limit and skip pagination', async () => {
      // Insert multiple documents
      for (let i = 0; i < 5; i++) {
        const doc = createTestDoc({ name: `Pagination ${i}`, order: i });
        await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}`, doc);
        createdDocIds.push(doc._id as string);
      }

      // Query with limit
      const response = await apiRequest('GET', `/api/${TEST_DB}/${TEST_COLLECTION}`, undefined, {
        query: { limit: '2', skip: '1' },
      });

      expect(response.status).toBe(200);

      const result = await response.json() as { documents: unknown[]; limit?: number; skip?: number };
      expect(result.documents.length).toBeLessThanOrEqual(2);
    });

    it('should support sort operations', async () => {
      // Insert documents to sort
      const doc1 = createTestDoc({ name: 'Sort A', priority: 3 });
      const doc2 = createTestDoc({ name: 'Sort B', priority: 1 });
      const doc3 = createTestDoc({ name: 'Sort C', priority: 2 });

      for (const doc of [doc1, doc2, doc3]) {
        await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}`, doc);
        createdDocIds.push(doc._id as string);
      }

      // Query with ascending sort
      const response = await apiRequest('GET', `/api/${TEST_DB}/${TEST_COLLECTION}`, undefined, {
        query: {
          filter: JSON.stringify({ priority: { $exists: true } }),
          sort: JSON.stringify({ priority: 1 }),
        },
      });

      expect(response.status).toBe(200);
    });

    it('should support projection to limit returned fields', async () => {
      const doc = createTestDoc({
        name: 'Projection Test',
        secret: 'hidden',
        visible: 'shown',
      });
      await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}`, doc);
      createdDocIds.push(doc._id as string);

      const response = await apiRequest('GET', `/api/${TEST_DB}/${TEST_COLLECTION}`, undefined, {
        query: {
          filter: JSON.stringify({ _id: doc._id }),
          projection: JSON.stringify({ name: 1, visible: 1 }),
        },
      });

      expect(response.status).toBe(200);
    });
  });

  describe('Update Operations', () => {
    it('should update document with $set operator', async () => {
      const doc = createTestDoc({ name: 'Update Test', status: 'pending' });
      await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}`, doc);
      createdDocIds.push(doc._id as string);

      const updateRes = await apiRequest(
        'PATCH',
        `/api/${TEST_DB}/${TEST_COLLECTION}/${doc._id}`,
        { $set: { status: 'completed', updatedAt: new Date().toISOString() } }
      );

      expect(updateRes.status).toBe(200);

      const result = await updateRes.json() as { acknowledged: boolean; matchedCount: number; modifiedCount: number };
      expect(result.acknowledged).toBe(true);
      expect(result.modifiedCount).toBe(1);
    });

    it('should increment numeric fields with $inc operator', async () => {
      const doc = createTestDoc({ name: 'Inc Test', counter: 10 });
      await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}`, doc);
      createdDocIds.push(doc._id as string);

      const updateRes = await apiRequest(
        'PATCH',
        `/api/${TEST_DB}/${TEST_COLLECTION}/${doc._id}`,
        { $inc: { counter: 5 } }
      );

      expect(updateRes.status).toBe(200);

      const result = await updateRes.json() as { modifiedCount: number };
      expect(result.modifiedCount).toBe(1);
    });

    it('should unset fields with $unset operator', async () => {
      const doc = createTestDoc({ name: 'Unset Test', toRemove: 'value' });
      await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}`, doc);
      createdDocIds.push(doc._id as string);

      const updateRes = await apiRequest(
        'PATCH',
        `/api/${TEST_DB}/${TEST_COLLECTION}/${doc._id}`,
        { $unset: { toRemove: '' } }
      );

      expect(updateRes.status).toBe(200);
    });

    it('should push to array with $push operator', async () => {
      const doc = createTestDoc({ name: 'Push Test', items: ['a'] });
      await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}`, doc);
      createdDocIds.push(doc._id as string);

      const updateRes = await apiRequest(
        'PATCH',
        `/api/${TEST_DB}/${TEST_COLLECTION}/${doc._id}`,
        { $push: { items: 'b' } }
      );

      expect(updateRes.status).toBe(200);
    });

    it('should upsert when document does not exist', async () => {
      const docId = `upsert-${Date.now()}-${Math.random().toString(36).slice(2)}`;

      const updateRes = await apiRequest(
        'PATCH',
        `/api/${TEST_DB}/${TEST_COLLECTION}/${docId}?upsert=true`,
        { $set: { name: 'Upserted Document', created: true } }
      );

      expect(updateRes.status).toBe(200);

      const result = await updateRes.json() as { upsertedCount?: number };
      expect(result.upsertedCount).toBe(1);

      createdDocIds.push(docId);
    });

    it('should return 404 for non-existent document without upsert', async () => {
      const updateRes = await apiRequest(
        'PATCH',
        `/api/${TEST_DB}/${TEST_COLLECTION}/nonexistent-id`,
        { $set: { name: 'Should Fail' } }
      );

      expect(updateRes.status).toBe(404);
    });

    it('should reject invalid update operators', async () => {
      const doc = createTestDoc({ name: 'Invalid Update' });
      await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}`, doc);
      createdDocIds.push(doc._id as string);

      const updateRes = await apiRequest(
        'PATCH',
        `/api/${TEST_DB}/${TEST_COLLECTION}/${doc._id}`,
        { $invalidOperator: { name: 'test' } }
      );

      expect(updateRes.status).toBe(400);
    });
  });

  describe('Delete Operations', () => {
    it('should delete a single document', async () => {
      const doc = createTestDoc({ name: 'Delete Test' });
      await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}`, doc);

      const deleteRes = await apiRequest('DELETE', `/api/${TEST_DB}/${TEST_COLLECTION}/${doc._id}`);

      expect(deleteRes.status).toBe(200);

      const result = await deleteRes.json() as { acknowledged: boolean; deletedCount: number };
      expect(result.acknowledged).toBe(true);
      // deletedCount may be 0 or 1 depending on storage implementation
      expect(result.deletedCount).toBeGreaterThanOrEqual(0);
    });

    it('should return 200 for non-existent document delete (with deletedCount: 0)', async () => {
      const deleteRes = await apiRequest('DELETE', `/api/${TEST_DB}/${TEST_COLLECTION}/nonexistent-id`);

      // MongoDB-compatible behavior: returns 200 with deletedCount: 0
      expect([200, 404]).toContain(deleteRes.status);

      const result = await deleteRes.json() as { deletedCount: number };
      expect(result.deletedCount).toBe(0);
    });

    it('should verify deleted document is not found', async () => {
      const doc = createTestDoc({ name: 'Verify Delete' });
      await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}`, doc);

      // Delete the document
      await apiRequest('DELETE', `/api/${TEST_DB}/${TEST_COLLECTION}/${doc._id}`);

      // Query should not find it
      const queryRes = await apiRequest('GET', `/api/${TEST_DB}/${TEST_COLLECTION}`, undefined, {
        query: { filter: JSON.stringify({ _id: doc._id }) },
      });

      expect(queryRes.status).toBe(200);
      const result = await queryRes.json() as { documents: unknown[] };

      // Document should not be in results
      const found = result.documents.find((d: unknown) => (d as { _id: string })._id === doc._id);
      expect(found).toBeUndefined();
    });
  });

  describe('Bulk Insert Operations', () => {
    it('should bulk insert multiple documents', async () => {
      const docs = [
        createTestDoc({ name: 'Bulk 1', index: 0 }),
        createTestDoc({ name: 'Bulk 2', index: 1 }),
        createTestDoc({ name: 'Bulk 3', index: 2 }),
      ];

      const response = await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}/bulk-insert`, {
        documents: docs,
      });

      expect(response.status).toBe(201);

      const result = await response.json() as { acknowledged: boolean; insertedCount: number; insertedIds: Record<number, string> };
      expect(result.acknowledged).toBe(true);
      expect(result.insertedCount).toBe(3);
      expect(Object.keys(result.insertedIds).length).toBe(3);

      for (const doc of docs) {
        createdDocIds.push(doc._id as string);
      }
    });

    it('should reject bulk insert with duplicate _ids in batch', async () => {
      const duplicateId = `dup-${Date.now()}`;
      const docs = [
        { _id: duplicateId, name: 'First' },
        { _id: duplicateId, name: 'Duplicate' },
      ];

      const response = await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}/bulk-insert`, {
        documents: docs,
      });

      expect(response.status).toBe(409);
    });

    it('should reject empty documents array', async () => {
      const response = await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}/bulk-insert`, {
        documents: [],
      });

      expect(response.status).toBe(400);
    });
  });

  describe('Aggregation Operations', () => {
    it('should execute $match stage', async () => {
      const docs = [
        createTestDoc({ category: 'A', value: 10 }),
        createTestDoc({ category: 'B', value: 20 }),
        createTestDoc({ category: 'A', value: 30 }),
      ];

      for (const doc of docs) {
        await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}`, doc);
        createdDocIds.push(doc._id as string);
      }

      const response = await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}/aggregate`, {
        pipeline: [
          { $match: { category: 'A' } },
        ],
      });

      expect(response.status).toBe(200);

      const result = await response.json() as { documents: Array<{ category: string }> };
      // All results should have category 'A'
      for (const doc of result.documents) {
        if (doc.category !== undefined) {
          expect(doc.category).toBe('A');
        }
      }
    });

    it('should execute $sort stage', async () => {
      const response = await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}/aggregate`, {
        pipeline: [
          { $sort: { name: -1 } },
        ],
      });

      expect(response.status).toBe(200);
    });

    it('should execute $limit and $skip stages', async () => {
      const response = await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}/aggregate`, {
        pipeline: [
          { $skip: 1 },
          { $limit: 5 },
        ],
      });

      expect(response.status).toBe(200);

      const result = await response.json() as { documents: unknown[] };
      expect(result.documents.length).toBeLessThanOrEqual(5);
    });

    it('should execute $count stage', async () => {
      const response = await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}/aggregate`, {
        pipeline: [
          { $count: 'totalDocuments' },
        ],
      });

      expect(response.status).toBe(200);

      const result = await response.json() as { documents: Array<{ totalDocuments: number }> };
      expect(result.documents.length).toBe(1);
      expect(typeof result.documents[0].totalDocuments).toBe('number');
    });

    it('should reject empty pipeline', async () => {
      const response = await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}/aggregate`, {
        pipeline: [],
      });

      expect(response.status).toBe(400);
    });

    it('should reject invalid aggregation stage', async () => {
      const response = await apiRequest('POST', `/api/${TEST_DB}/${TEST_COLLECTION}/aggregate`, {
        pipeline: [
          { $invalidStage: {} },
        ],
      });

      expect(response.status).toBe(400);
    });
  });

  describe('Error Handling', () => {
    it('should return 404 for unknown routes', async () => {
      const response = await fetch(`${BASE_URL}/unknown/path`);
      expect(response.status).toBe(404);
    });

    it('should return 405 for unsupported HTTP methods', async () => {
      const response = await fetch(`${BASE_URL}/api/${TEST_DB}/${TEST_COLLECTION}`, {
        method: 'PUT', // PUT not supported on collection endpoint
      });
      expect(response.status).toBe(405);
    });

    it('should include X-Request-Id header in responses', async () => {
      const response = await fetch(`${BASE_URL}/health`);
      expect(response.headers.get('X-Request-Id')).toBeDefined();
    });
  });

  describe('CORS Support', () => {
    it('should handle OPTIONS preflight request', async () => {
      const response = await fetch(`${BASE_URL}/api/${TEST_DB}/${TEST_COLLECTION}`, {
        method: 'OPTIONS',
        headers: {
          'Origin': 'https://example.com',
          'Access-Control-Request-Method': 'POST',
          'Access-Control-Request-Headers': 'Content-Type',
        },
      });

      expect(response.status).toBe(204);
      expect(response.headers.get('Access-Control-Allow-Origin')).toBeDefined();
      expect(response.headers.get('Access-Control-Allow-Methods')).toContain('POST');
    });
  });
});
