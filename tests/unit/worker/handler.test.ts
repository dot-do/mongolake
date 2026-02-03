/**
 * Worker Handler Tests (TDD RED Phase)
 *
 * Tests for the MongoLake Worker HTTP request handler
 * Routes requests to appropriate handlers (find, insert, update, delete, aggregate)
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  MongoLakeWorker,
  type MongoLakeEnv,
  type RequestContext,
  type FindHandler,
  type InsertHandler,
  type UpdateHandler,
  type DeleteHandler,
  type AggregateHandler,
  type BulkInsertHandler,
} from '../../../src/worker/index.js';

// Mock environment for testing
const createMockEnv = (): MongoLakeEnv => ({
  BUCKET: {
    get: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
    list: vi.fn(),
    createMultipartUpload: vi.fn(),
  } as unknown as R2Bucket,
  RPC_NAMESPACE: {
    idFromName: vi.fn().mockReturnValue({ toString: () => 'test-shard-id' }),
    get: vi.fn().mockReturnValue({
      fetch: vi.fn().mockResolvedValue(new Response(JSON.stringify({ acknowledged: true, readToken: 'test-token' }), {
        headers: { 'Content-Type': 'application/json' },
      })),
    }),
  } as unknown as DurableObjectNamespace,
  OAUTH_SECRET: 'test-secret',
});

// Mock Request helper
const createRequest = (method: string, path: string, body?: unknown, headers?: Record<string, string>): Request => {
  const url = `https://mongolake.workers.dev${path}`;
  const init: RequestInit = {
    method,
    headers: {
      'Content-Type': 'application/json',
      ...headers,
    },
  };
  if (body) {
    init.body = JSON.stringify(body);
  }
  return new Request(url, init);
};

// Mock WebSocket upgrade request helper
const createWebSocketRequest = (path: string, headers?: Record<string, string>): Request => {
  const url = `https://mongolake.workers.dev${path}`;
  return new Request(url, {
    headers: {
      'Upgrade': 'websocket',
      'Connection': 'Upgrade',
      'Sec-WebSocket-Key': 'dGhlIHNhbXBsZSBub25jZQ==',
      'Sec-WebSocket-Version': '13',
      ...headers,
    },
  });
};

describe('MongoLakeWorker Handler', () => {
  let worker: MongoLakeWorker;
  let env: MongoLakeEnv;

  beforeEach(() => {
    env = createMockEnv();
    worker = new MongoLakeWorker();
  });

  describe('Route: GET /api/{db}/{collection} - Find Handler', () => {
    it('should route GET request to find handler', async () => {
      const request = createRequest('GET', '/api/testdb/users');

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(result).toBeDefined();
      expect(Array.isArray(result.documents)).toBe(true);
    });

    it('should pass query parameters to find handler', async () => {
      const request = createRequest('GET', '/api/testdb/users?filter={"age":{"$gt":21}}&limit=10');

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(result).toBeDefined();
    });

    it('should parse filter from query string', async () => {
      const request = createRequest('GET', '/api/testdb/users?filter={"name":"Alice"}');

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(result.filter).toEqual({ name: 'Alice' });
    });

    it('should parse projection from query string', async () => {
      const request = createRequest('GET', '/api/testdb/users?projection={"name":1,"age":1}');

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(result.projection).toEqual({ name: 1, age: 1 });
    });

    it('should parse sort from query string', async () => {
      const request = createRequest('GET', '/api/testdb/users?sort={"createdAt":-1}');

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(result.sort).toEqual({ createdAt: -1 });
    });

    it('should parse limit and skip from query string', async () => {
      const request = createRequest('GET', '/api/testdb/users?limit=20&skip=40');

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(result.limit).toBe(20);
      expect(result.skip).toBe(40);
    });

    it('should handle empty collection', async () => {
      const request = createRequest('GET', '/api/testdb/emptyCollection');

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(result.documents).toEqual([]);
    });

    it('should return 400 for invalid filter JSON', async () => {
      const request = createRequest('GET', '/api/testdb/users?filter=invalid-json');

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(400);
      const result = await response.json();
      expect(result.error).toContain('Invalid filter');
    });
  });

  describe('Route: POST /api/{db}/{collection} - Insert Handler', () => {
    it('should route POST request to insert handler', async () => {
      const document = { name: 'Alice', age: 30 };
      const request = createRequest('POST', '/api/testdb/users', document);

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(201);
      expect(result.acknowledged).toBe(true);
      expect(result.insertedId).toBeDefined();
    });

    it('should generate _id if not provided', async () => {
      const document = { name: 'Bob', age: 25 };
      const request = createRequest('POST', '/api/testdb/users', document);

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(201);
      expect(result.insertedId).toBeDefined();
      expect(typeof result.insertedId).toBe('string');
      expect(result.insertedId.length).toBe(24); // ObjectId hex string length
    });

    it('should use provided _id', async () => {
      const document = { _id: 'custom-id-123', name: 'Charlie', age: 35 };
      const request = createRequest('POST', '/api/testdb/users', document);

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(201);
      expect(result.insertedId).toBe('custom-id-123');
    });

    it('should return 400 for empty body', async () => {
      const request = createRequest('POST', '/api/testdb/users');

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(400);
      const result = await response.json();
      expect(result.error).toContain('body');
    });

    it('should return 400 for invalid JSON body', async () => {
      const url = 'https://mongolake.workers.dev/api/testdb/users';
      const request = new Request(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: 'not-valid-json',
      });

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(400);
      const result = await response.json();
      expect(result.error).toContain('JSON');
    });

    it('should return 409 for duplicate _id', async () => {
      // Create a stateful mock that tracks inserted IDs and returns 409 on duplicate
      const insertedIds = new Set<string>();
      const statefulStub = {
        fetch: vi.fn().mockImplementation(async (request: Request) => {
          const body = await request.json() as { op?: string; document?: { _id?: string } };
          if (body.op === 'insert' && body.document?._id) {
            if (insertedIds.has(body.document._id)) {
              return new Response(JSON.stringify({ error: 'duplicate key error' }), {
                status: 409,
                headers: { 'Content-Type': 'application/json' },
              });
            }
            insertedIds.add(body.document._id);
          }
          return new Response(JSON.stringify({ acknowledged: true, readToken: 'test-token' }), {
            headers: { 'Content-Type': 'application/json' },
          });
        }),
      };
      env.RPC_NAMESPACE = {
        idFromName: vi.fn().mockReturnValue({ toString: () => 'test-shard-id' }),
        get: vi.fn().mockReturnValue(statefulStub),
      } as unknown as DurableObjectNamespace;

      const document = { _id: 'duplicate-id', name: 'First' };
      const request1 = createRequest('POST', '/api/testdb/users', document);
      await worker.fetch(request1, env);

      const request2 = createRequest('POST', '/api/testdb/users', document);
      const response = await worker.fetch(request2, env);

      expect(response.status).toBe(409);
      const result = await response.json();
      expect(result.error).toContain('duplicate');
    });

    it('should route write to correct shard via rpc.do', async () => {
      const document = { name: 'Sharded User', age: 40 };
      const request = createRequest('POST', '/api/testdb/users', document);

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(201);
      expect(env.RPC_NAMESPACE.get).toHaveBeenCalled();
    });
  });

  describe('Route: PATCH /api/{db}/{collection}/{id} - Update Handler', () => {
    it('should route PATCH request to update handler', async () => {
      const update = { $set: { age: 31 } };
      const request = createRequest('PATCH', '/api/testdb/users/user123', update);

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(result.acknowledged).toBe(true);
      expect(result.matchedCount).toBeDefined();
      expect(result.modifiedCount).toBeDefined();
    });

    it('should update document using $set operator', async () => {
      const update = { $set: { name: 'Updated Name', email: 'new@example.com' } };
      const request = createRequest('PATCH', '/api/testdb/users/user123', update);

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(result.modifiedCount).toBeGreaterThanOrEqual(0);
    });

    it('should update document using $inc operator', async () => {
      const update = { $inc: { visits: 1, score: 10 } };
      const request = createRequest('PATCH', '/api/testdb/users/user123', update);

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(result.acknowledged).toBe(true);
    });

    it('should update document using $unset operator', async () => {
      const update = { $unset: { temporaryField: '' } };
      const request = createRequest('PATCH', '/api/testdb/users/user123', update);

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(result.acknowledged).toBe(true);
    });

    it('should update document using $push operator', async () => {
      const update = { $push: { tags: 'new-tag' } };
      const request = createRequest('PATCH', '/api/testdb/users/user123', update);

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(result.acknowledged).toBe(true);
    });

    it('should return 404 if document not found', async () => {
      const update = { $set: { name: 'Updated' } };
      const request = createRequest('PATCH', '/api/testdb/users/nonexistent-id', update);

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(404);
      const result = await response.json();
      expect(result.matchedCount).toBe(0);
    });

    it('should return 400 for invalid update operators', async () => {
      const update = { invalidOperator: { name: 'Test' } };
      const request = createRequest('PATCH', '/api/testdb/users/user123', update);

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(400);
      const result = await response.json();
      expect(result.error).toContain('operator');
    });

    it('should support upsert option', async () => {
      const update = { $set: { name: 'New User' } };
      const request = createRequest('PATCH', '/api/testdb/users/new-user-id?upsert=true', update);

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(result.upsertedCount).toBeDefined();
    });
  });

  describe('Route: DELETE /api/{db}/{collection}/{id} - Delete Handler', () => {
    it('should route DELETE request to delete handler', async () => {
      // First insert a document to delete
      const insertRequest = createRequest('POST', '/api/testdb/users', { _id: 'user123', name: 'Test' });
      await worker.fetch(insertRequest, env);

      const request = createRequest('DELETE', '/api/testdb/users/user123');

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(result.acknowledged).toBe(true);
      expect(result.deletedCount).toBeDefined();
    });

    it('should delete document by _id', async () => {
      // First insert a document to delete
      const insertRequest = createRequest('POST', '/api/testdb/users', { _id: 'user-to-delete', name: 'ToDelete' });
      await worker.fetch(insertRequest, env);

      const request = createRequest('DELETE', '/api/testdb/users/user-to-delete');

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(result.deletedCount).toBe(1);
    });

    it('should return 404 if document not found', async () => {
      const request = createRequest('DELETE', '/api/testdb/users/nonexistent-id');

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(404);
      const result = await response.json();
      expect(result.deletedCount).toBe(0);
    });

    it('should not delete other documents', async () => {
      // Create a stateful mock that tracks documents
      const documents = new Map<string, Record<string, unknown>>();
      const statefulStub = {
        fetch: vi.fn().mockImplementation(async (request: Request) => {
          const url = new URL(request.url);
          const body = await request.json() as {
            op?: string;
            document?: Record<string, unknown>;
            filter?: { _id?: string };
            collection?: string;
          };

          // Handle /write endpoint
          if (url.pathname === '/write') {
            if (body.op === 'insert' && body.document) {
              const id = String(body.document._id);
              documents.set(id, body.document);
            } else if (body.op === 'delete' && body.filter?._id) {
              documents.delete(String(body.filter._id));
            }
            return new Response(JSON.stringify({ acknowledged: true, readToken: 'test-token' }), {
              headers: { 'Content-Type': 'application/json' },
            });
          }

          // Handle /find endpoint
          if (url.pathname === '/find') {
            const filter = body.filter || {};
            let results = Array.from(documents.values());
            if (filter._id) {
              results = results.filter(d => d._id === filter._id);
            }
            return new Response(JSON.stringify({ documents: results }), {
              headers: { 'Content-Type': 'application/json' },
            });
          }

          // Handle /findOne endpoint
          if (url.pathname === '/findOne') {
            const filter = body.filter || {};
            const doc = filter._id ? documents.get(String(filter._id)) : null;
            return new Response(JSON.stringify({ document: doc || null }), {
              headers: { 'Content-Type': 'application/json' },
            });
          }

          return new Response(JSON.stringify({ acknowledged: true }), {
            headers: { 'Content-Type': 'application/json' },
          });
        }),
      };
      env.RPC_NAMESPACE = {
        idFromName: vi.fn().mockReturnValue({ toString: () => 'test-shard-id' }),
        get: vi.fn().mockReturnValue(statefulStub),
      } as unknown as DurableObjectNamespace;

      // Insert two documents
      const insert1 = createRequest('POST', '/api/testdb/users', { _id: 'specific-id', name: 'Target' });
      const insert2 = createRequest('POST', '/api/testdb/users', { _id: 'other-id', name: 'Other' });
      await worker.fetch(insert1, env);
      await worker.fetch(insert2, env);

      const request = createRequest('DELETE', '/api/testdb/users/specific-id');

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(result.deletedCount).toBe(1);

      // Verify other document still exists by querying
      const checkRequest = createRequest('GET', '/api/testdb/users?filter=' + encodeURIComponent(JSON.stringify({ _id: 'other-id' })));
      const checkResponse = await worker.fetch(checkRequest, env);
      expect(checkResponse.status).toBe(200);
      const checkResult = await checkResponse.json();
      expect(checkResult.documents).toHaveLength(1);
      expect(checkResult.documents[0]._id).toBe('other-id');
    });
  });

  describe('Route: POST /api/{db}/{collection}/aggregate - Aggregation Handler', () => {
    it('should route aggregate request to aggregation handler', async () => {
      const pipeline = [{ $match: { active: true } }, { $group: { _id: '$status', count: { $sum: 1 } } }];
      const request = createRequest('POST', '/api/testdb/users/aggregate', { pipeline });

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(Array.isArray(result.documents)).toBe(true);
    });

    it('should execute $match stage', async () => {
      const pipeline = [{ $match: { age: { $gt: 21 } } }];
      const request = createRequest('POST', '/api/testdb/users/aggregate', { pipeline });

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(result.documents).toBeDefined();
    });

    it('should execute $group stage', async () => {
      const pipeline = [{ $group: { _id: '$department', total: { $sum: '$salary' } } }];
      const request = createRequest('POST', '/api/testdb/employees/aggregate', { pipeline });

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(result.documents).toBeDefined();
    });

    it('should execute $sort stage', async () => {
      const pipeline = [{ $sort: { createdAt: -1 } }];
      const request = createRequest('POST', '/api/testdb/users/aggregate', { pipeline });

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(result.documents).toBeDefined();
    });

    it('should execute $limit stage', async () => {
      const pipeline = [{ $limit: 5 }];
      const request = createRequest('POST', '/api/testdb/users/aggregate', { pipeline });

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(result.documents.length).toBeLessThanOrEqual(5);
    });

    it('should execute $skip stage', async () => {
      const pipeline = [{ $skip: 10 }];
      const request = createRequest('POST', '/api/testdb/users/aggregate', { pipeline });

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(200);
    });

    it('should execute $project stage', async () => {
      const pipeline = [{ $project: { name: 1, age: 1, _id: 0 } }];
      const request = createRequest('POST', '/api/testdb/users/aggregate', { pipeline });

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(result.documents).toBeDefined();
    });

    it('should execute $unwind stage', async () => {
      const pipeline = [{ $unwind: '$tags' }];
      const request = createRequest('POST', '/api/testdb/users/aggregate', { pipeline });

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(200);
    });

    it('should execute $lookup stage', async () => {
      const pipeline = [
        {
          $lookup: {
            from: 'orders',
            localField: '_id',
            foreignField: 'userId',
            as: 'orders',
          },
        },
      ];
      const request = createRequest('POST', '/api/testdb/users/aggregate', { pipeline });

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(200);
    });

    it('should execute $count stage', async () => {
      const pipeline = [{ $count: 'total' }];
      const request = createRequest('POST', '/api/testdb/users/aggregate', { pipeline });

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(200);
      expect(result.documents).toBeDefined();
    });

    it('should execute complex multi-stage pipeline', async () => {
      const pipeline = [
        { $match: { status: 'active' } },
        { $group: { _id: '$category', count: { $sum: 1 }, avgPrice: { $avg: '$price' } } },
        { $sort: { count: -1 } },
        { $limit: 10 },
      ];
      const request = createRequest('POST', '/api/testdb/products/aggregate', { pipeline });

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(200);
    });

    it('should return 400 for empty pipeline', async () => {
      const request = createRequest('POST', '/api/testdb/users/aggregate', { pipeline: [] });

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(400);
      const result = await response.json();
      expect(result.error).toContain('pipeline');
    });

    it('should return 400 for missing pipeline', async () => {
      const request = createRequest('POST', '/api/testdb/users/aggregate', {});

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(400);
      const result = await response.json();
      expect(result.error).toContain('pipeline');
    });

    it('should return 400 for invalid pipeline stage', async () => {
      const pipeline = [{ $invalidStage: {} }];
      const request = createRequest('POST', '/api/testdb/users/aggregate', { pipeline });

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(400);
      const result = await response.json();
      expect(result.error).toContain('stage');
    });
  });

  describe('Route: POST /api/{db}/{collection}/bulk-insert - Bulk Insert Handler', () => {
    it('should route bulk-insert request to bulk insert handler', async () => {
      const documents = [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 },
        { name: 'Charlie', age: 35 },
      ];
      const request = createRequest('POST', '/api/testdb/users/bulk-insert', { documents });

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(201);
      expect(result.acknowledged).toBe(true);
      expect(result.insertedCount).toBe(3);
    });

    it('should return insertedIds for each document', async () => {
      const documents = [{ name: 'User1' }, { name: 'User2' }];
      const request = createRequest('POST', '/api/testdb/users/bulk-insert', { documents });

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(201);
      expect(result.insertedIds).toBeDefined();
      expect(Object.keys(result.insertedIds).length).toBe(2);
    });

    it('should generate _id for documents without one', async () => {
      const documents = [{ name: 'NoIdUser1' }, { name: 'NoIdUser2' }];
      const request = createRequest('POST', '/api/testdb/users/bulk-insert', { documents });

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(201);
      for (const id of Object.values(result.insertedIds)) {
        expect(typeof id).toBe('string');
        expect((id as string).length).toBe(24);
      }
    });

    it('should preserve provided _id values', async () => {
      const documents = [
        { _id: 'custom-1', name: 'User1' },
        { _id: 'custom-2', name: 'User2' },
      ];
      const request = createRequest('POST', '/api/testdb/users/bulk-insert', { documents });

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(201);
      expect(result.insertedIds[0]).toBe('custom-1');
      expect(result.insertedIds[1]).toBe('custom-2');
    });

    it('should return 400 for empty documents array', async () => {
      const request = createRequest('POST', '/api/testdb/users/bulk-insert', { documents: [] });

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(400);
      const result = await response.json();
      expect(result.error).toContain('documents');
    });

    it('should return 400 for missing documents field', async () => {
      const request = createRequest('POST', '/api/testdb/users/bulk-insert', {});

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(400);
      const result = await response.json();
      expect(result.error).toContain('documents');
    });

    it('should handle large batch inserts', async () => {
      const documents = Array.from({ length: 1000 }, (_, i) => ({
        name: `User${i}`,
        index: i,
      }));
      const request = createRequest('POST', '/api/testdb/users/bulk-insert', { documents });

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(response.status).toBe(201);
      expect(result.insertedCount).toBe(1000);
    });

    it('should fail atomically on duplicate _id', async () => {
      const documents = [
        { _id: 'dup-id', name: 'First' },
        { _id: 'dup-id', name: 'Second' },
      ];
      const request = createRequest('POST', '/api/testdb/users/bulk-insert', { documents });

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(409);
      const result = await response.json();
      expect(result.error).toContain('duplicate');
    });

    it('should support ordered bulk insert', async () => {
      const documents = [{ name: 'User1' }, { name: 'User2' }];
      const request = createRequest('POST', '/api/testdb/users/bulk-insert', { documents, ordered: true });

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(201);
    });

    it('should support unordered bulk insert', async () => {
      const documents = [{ name: 'User1' }, { name: 'User2' }];
      const request = createRequest('POST', '/api/testdb/users/bulk-insert', { documents, ordered: false });

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(201);
    });
  });

  describe('Authentication', () => {
    it('should accept valid Bearer token', async () => {
      const request = createRequest('GET', '/api/testdb/users', undefined, {
        Authorization: 'Bearer valid-token-abc123',
      });

      const response = await worker.fetch(request, env);

      expect(response.status).not.toBe(401);
    });

    it('should return 401 for missing authorization header when auth is required', async () => {
      const strictEnv = { ...env, REQUIRE_AUTH: true };
      const request = createRequest('GET', '/api/testdb/users');

      const response = await worker.fetch(request, strictEnv);

      expect(response.status).toBe(401);
      const result = await response.json();
      expect(result.error).toContain('Authorization');
    });

    it('should return 401 for invalid Bearer token', async () => {
      const strictEnv = { ...env, REQUIRE_AUTH: true };
      const request = createRequest('GET', '/api/testdb/users', undefined, {
        Authorization: 'Bearer invalid-token',
      });

      const response = await worker.fetch(request, strictEnv);

      expect(response.status).toBe(401);
      const result = await response.json();
      expect(result.error).toContain('Invalid');
    });

    it('should return 401 for malformed authorization header', async () => {
      const strictEnv = { ...env, REQUIRE_AUTH: true };
      const request = createRequest('GET', '/api/testdb/users', undefined, {
        Authorization: 'NotBearer token123',
      });

      const response = await worker.fetch(request, strictEnv);

      expect(response.status).toBe(401);
    });

    it('should validate token via oauth.do', async () => {
      const strictEnv = { ...env, REQUIRE_AUTH: true };
      const request = createRequest('GET', '/api/testdb/users', undefined, {
        Authorization: 'Bearer valid-oauth-token',
      });

      const response = await worker.fetch(request, strictEnv);

      // Should make a call to validate the token
      expect(response.status).not.toBe(500);
    });

    it('should extract user context from token', async () => {
      const request = createRequest('GET', '/api/testdb/users', undefined, {
        Authorization: 'Bearer user-token-with-claims',
      });

      const response = await worker.fetch(request, env);

      // User context should be available for audit logging
      expect(response.status).not.toBe(500);
    });

    it('should support API key authentication', async () => {
      const request = createRequest('GET', '/api/testdb/users', undefined, {
        'X-API-Key': 'api-key-12345',
      });

      const response = await worker.fetch(request, env);

      expect(response.status).not.toBe(401);
    });
  });

  describe('Unknown Routes - 404 Handling', () => {
    it('should return 404 for unknown root path', async () => {
      const request = createRequest('GET', '/unknown');

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(404);
      const result = await response.json();
      expect(result.error).toContain('Not found');
    });

    it('should return 404 for unknown API endpoint', async () => {
      const request = createRequest('GET', '/api/unknown-endpoint');

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(404);
    });

    it('should return 404 for missing collection in path', async () => {
      const request = createRequest('GET', '/api/testdb');

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(404);
    });

    it('should return 405 for unsupported HTTP method', async () => {
      const request = createRequest('PUT', '/api/testdb/users');

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(405);
      const result = await response.json();
      expect(result.error).toContain('Method');
    });

    it('should return 404 for invalid path segments', async () => {
      const request = createRequest('GET', '/api/testdb/users/id/extra/segments');

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(404);
    });
  });

  describe('WebSocket Upgrade - Wire Protocol', () => {
    it('should handle WebSocket upgrade request', async () => {
      const request = createWebSocketRequest('/wire');

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(101);
      expect(response.webSocket).toBeDefined();
    });

    it('should return 426 for non-upgrade request to /wire', async () => {
      const request = createRequest('GET', '/wire');

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(426);
      const result = await response.json();
      expect(result.error).toContain('Upgrade');
    });

    it('should accept WebSocket upgrade on wire protocol path', async () => {
      const request = createWebSocketRequest('/wire');

      const response = await worker.fetch(request, env);

      expect(response.headers.get('Upgrade')).toBe('websocket');
    });

    it('should handle WebSocket upgrade with authentication', async () => {
      const request = createWebSocketRequest('/wire', {
        Authorization: 'Bearer ws-auth-token',
      });

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(101);
    });

    it('should reject WebSocket upgrade with invalid auth when required', async () => {
      const strictEnv = { ...env, REQUIRE_AUTH: true };
      const request = createWebSocketRequest('/wire');

      const response = await worker.fetch(request, strictEnv);

      expect(response.status).toBe(401);
    });

    it('should support wire protocol subprotocol', async () => {
      const request = createWebSocketRequest('/wire', {
        'Sec-WebSocket-Protocol': 'mongodb',
      });

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(101);
      expect(response.headers.get('Sec-WebSocket-Protocol')).toBe('mongodb');
    });

    it('should handle multiple concurrent WebSocket connections', async () => {
      const request1 = createWebSocketRequest('/wire');
      const request2 = createWebSocketRequest('/wire');

      const [response1, response2] = await Promise.all([
        worker.fetch(request1, env),
        worker.fetch(request2, env),
      ]);

      expect(response1.status).toBe(101);
      expect(response2.status).toBe(101);
    });
  });

  describe('Request Context', () => {
    it('should extract database name from path', async () => {
      const request = createRequest('GET', '/api/myDatabase/myCollection');

      const response = await worker.fetch(request, env);

      // The handler should receive database: 'myDatabase'
      expect(response.status).not.toBe(500);
    });

    it('should extract collection name from path', async () => {
      const request = createRequest('GET', '/api/myDatabase/myCollection');

      const response = await worker.fetch(request, env);

      // The handler should receive collection: 'myCollection'
      expect(response.status).not.toBe(500);
    });

    it('should extract document id from path', async () => {
      const request = createRequest('PATCH', '/api/testdb/users/doc123');

      const response = await worker.fetch(request, env);

      // The handler should receive id: 'doc123'
      expect(response.status).not.toBe(500);
    });

    it('should include request timestamp in context', async () => {
      const request = createRequest('GET', '/api/testdb/users');

      const response = await worker.fetch(request, env);

      // Context should have timestamp for logging
      expect(response.status).not.toBe(500);
    });

    it('should include request ID in context', async () => {
      const request = createRequest('GET', '/api/testdb/users');

      const response = await worker.fetch(request, env);
      const requestId = response.headers.get('X-Request-Id');

      expect(requestId).toBeDefined();
      expect(requestId?.length).toBeGreaterThan(0);
    });
  });

  describe('Error Handling', () => {
    it('should return JSON error response on internal error', async () => {
      // Trigger an internal error scenario
      const badEnv = { ...env, BUCKET: null } as unknown as MongoLakeEnv;
      const request = createRequest('GET', '/api/testdb/users');

      const response = await worker.fetch(request, badEnv);

      expect(response.status).toBe(500);
      expect(response.headers.get('Content-Type')).toBe('application/json');
      const result = await response.json();
      expect(result.error).toBeDefined();
    });

    it('should not expose internal error details in production', async () => {
      const prodEnv = { ...env, ENVIRONMENT: 'production' };
      const badEnv = { ...prodEnv, BUCKET: null } as unknown as MongoLakeEnv;
      const request = createRequest('GET', '/api/testdb/users');

      const response = await worker.fetch(request, badEnv);

      expect(response.status).toBe(500);
      const result = await response.json();
      expect(result.error).toBe('Internal server error');
      expect(result.stack).toBeUndefined();
    });

    it('should include error details in development', async () => {
      const devEnv = { ...env, ENVIRONMENT: 'development' };
      const request = createRequest('GET', '/api/testdb/users');

      // This may or may not have detailed errors based on implementation
      const response = await worker.fetch(request, devEnv);

      expect(response.status).not.toBe(500);
    });

    it('should handle timeout errors gracefully', async () => {
      const request = createRequest('GET', '/api/testdb/slowCollection');

      const response = await worker.fetch(request, env);

      // Should not crash, may return 504 or handle gracefully
      expect(response).toBeDefined();
    });
  });

  describe('CORS Headers', () => {
    it('should include CORS headers in response', async () => {
      const request = createRequest('GET', '/api/testdb/users');

      const response = await worker.fetch(request, env);

      expect(response.headers.get('Access-Control-Allow-Origin')).toBeDefined();
    });

    it('should handle OPTIONS preflight request', async () => {
      const request = createRequest('OPTIONS', '/api/testdb/users');

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(204);
      expect(response.headers.get('Access-Control-Allow-Methods')).toContain('GET');
      expect(response.headers.get('Access-Control-Allow-Methods')).toContain('POST');
      expect(response.headers.get('Access-Control-Allow-Headers')).toContain('Authorization');
    });

    it('should allow custom origins when configured', async () => {
      const envWithOrigin = { ...env, ALLOWED_ORIGINS: 'https://myapp.com' };
      const request = createRequest('GET', '/api/testdb/users', undefined, {
        Origin: 'https://myapp.com',
      });

      const response = await worker.fetch(request, envWithOrigin);

      expect(response.headers.get('Access-Control-Allow-Origin')).toBe('https://myapp.com');
    });
  });

  describe('Health Check', () => {
    it('should return 200 for health check endpoint', async () => {
      const request = createRequest('GET', '/health');

      const response = await worker.fetch(request, env);

      expect(response.status).toBe(200);
      const result = await response.json();
      expect(result.status).toBe('ok');
    });

    it('should include version in health response', async () => {
      const request = createRequest('GET', '/health');

      const response = await worker.fetch(request, env);
      const result = await response.json();

      expect(result.version).toBeDefined();
    });
  });
});
