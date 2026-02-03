/**
 * Tests for the Abstraction Layer
 *
 * Verifies that the abstraction interfaces and Cloudflare implementations
 * work correctly.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { R2Bucket, R2Object, R2ObjectBody, R2Objects } from '../../../src/types.js';
import type { DurableObjectState, DurableObjectId, DurableObjectNamespace, DurableObjectStub } from '@cloudflare/workers-types';

// Import the abstraction types
import type {
  ObjectStorageBackend,
  ObjectStorageObject,
  CoordinatorState,
  CoordinatorNamespace,
  CacheBackend,
} from '../../../src/abstractions/index.js';

// Import the Cloudflare implementations
import {
  CloudflareR2Storage,
  createCloudflareStorage,
  createCloudflareCoordination,
  createCloudflareNamespace,
  CloudflareKVCache,
  createCloudflareCache,
} from '../../../src/abstractions/index.js';

// Import DO types to verify compatibility
import type { ObjectStorage, StorageBackend } from '../../../src/do/shard/types.js';

describe('Abstraction Layer', () => {
  describe('Storage Abstraction', () => {
    let mockBucket: R2Bucket;

    beforeEach(() => {
      mockBucket = {
        get: vi.fn(),
        head: vi.fn(),
        put: vi.fn(),
        delete: vi.fn(),
        list: vi.fn(),
        createMultipartUpload: vi.fn(),
      } as unknown as R2Bucket;
    });

    it('should create CloudflareR2Storage from R2Bucket', () => {
      const storage = new CloudflareR2Storage(mockBucket);
      expect(storage).toBeDefined();
      expect(storage.getR2Bucket()).toBe(mockBucket);
    });

    it('should implement ObjectStorageBackend interface', () => {
      const storage = createCloudflareStorage(mockBucket);

      // Verify it has all required methods
      expect(typeof storage.get).toBe('function');
      expect(typeof storage.head).toBe('function');
      expect(typeof storage.put).toBe('function');
      expect(typeof storage.delete).toBe('function');
      expect(typeof storage.list).toBe('function');
      expect(typeof storage.createMultipartUpload).toBe('function');
    });

    it('should delegate get to underlying R2Bucket', async () => {
      const mockBody: R2ObjectBody = {
        arrayBuffer: vi.fn().mockResolvedValue(new ArrayBuffer(10)),
        text: vi.fn().mockResolvedValue('test'),
        json: vi.fn().mockResolvedValue({ test: true }),
        body: new ReadableStream(),
        etag: 'etag-123',
      };
      (mockBucket.get as ReturnType<typeof vi.fn>).mockResolvedValue(mockBody);

      const storage = createCloudflareStorage(mockBucket);
      const result = await storage.get('test-key');

      expect(mockBucket.get).toHaveBeenCalledWith('test-key');
      expect(result).not.toBeNull();
      expect(result?.metadata.etag).toBe('etag-123');
    });

    it('should return null for non-existent objects', async () => {
      (mockBucket.get as ReturnType<typeof vi.fn>).mockResolvedValue(null);

      const storage = createCloudflareStorage(mockBucket);
      const result = await storage.get('non-existent');

      expect(result).toBeNull();
    });

    it('should delegate put to underlying R2Bucket', async () => {
      const mockResult: R2Object = {
        key: 'test-key',
        size: 10,
        etag: 'etag-456',
      };
      (mockBucket.put as ReturnType<typeof vi.fn>).mockResolvedValue(mockResult);

      const storage = createCloudflareStorage(mockBucket);
      const result = await storage.put('test-key', new Uint8Array([1, 2, 3]));

      expect(mockBucket.put).toHaveBeenCalledWith('test-key', new Uint8Array([1, 2, 3]));
      expect(result.key).toBe('test-key');
      expect(result.etag).toBe('etag-456');
    });

    it('should delegate list to underlying R2Bucket', async () => {
      const mockListResult: R2Objects = {
        objects: [
          { key: 'file1.txt', size: 100, etag: 'etag1' },
          { key: 'file2.txt', size: 200, etag: 'etag2' },
        ],
        truncated: false,
        cursor: undefined,
      };
      (mockBucket.list as ReturnType<typeof vi.fn>).mockResolvedValue(mockListResult);

      const storage = createCloudflareStorage(mockBucket);
      const result = await storage.list({ prefix: 'files/' });

      expect(mockBucket.list).toHaveBeenCalledWith({
        prefix: 'files/',
        limit: undefined,
        cursor: undefined,
      });
      expect(result.objects).toHaveLength(2);
      expect(result.truncated).toBe(false);
    });
  });

  describe('Coordination Abstraction', () => {
    let mockState: DurableObjectState;

    beforeEach(() => {
      mockState = {
        id: {
          toString: () => 'test-shard-id',
        } as DurableObjectId,
        storage: {
          sql: {
            exec: vi.fn().mockReturnValue({
              toArray: () => [],
            }),
          },
          setAlarm: vi.fn(),
          getAlarm: vi.fn().mockResolvedValue(null),
          deleteAlarm: vi.fn(),
          get: vi.fn(),
          put: vi.fn(),
          delete: vi.fn(),
        },
        blockConcurrencyWhile: vi.fn().mockImplementation((fn) => fn()),
      } as unknown as DurableObjectState;
    });

    it('should create CoordinatorState from DurableObjectState', () => {
      const state = createCloudflareCoordination(mockState);
      expect(state).toBeDefined();
      expect(state.id).toBe(mockState.id);
    });

    it('should provide SQL storage interface', () => {
      const state = createCloudflareCoordination(mockState);

      expect(typeof state.sql.exec).toBe('function');

      // Execute a query
      const cursor = state.sql.exec('SELECT * FROM test');
      expect(mockState.storage.sql.exec).toHaveBeenCalledWith('SELECT * FROM test');
      expect(cursor.toArray()).toEqual([]);
    });

    it('should provide alarm scheduling interface', async () => {
      const state = createCloudflareCoordination(mockState);

      expect(typeof state.alarms.setAlarm).toBe('function');

      await state.alarms.setAlarm(Date.now() + 1000);
      expect(mockState.storage.setAlarm).toHaveBeenCalled();
    });

    it('should provide blockConcurrencyWhile', async () => {
      const state = createCloudflareCoordination(mockState);

      const result = await state.blockConcurrencyWhile(async () => {
        return 'test-result';
      });

      expect(mockState.blockConcurrencyWhile).toHaveBeenCalled();
      expect(result).toBe('test-result');
    });

    it('should provide KV storage interface', async () => {
      const state = createCloudflareCoordination(mockState);

      expect(state.storage).toBeDefined();
      expect(typeof state.storage?.get).toBe('function');
      expect(typeof state.storage?.put).toBe('function');
      expect(typeof state.storage?.delete).toBe('function');

      await state.storage?.put('key', 'value');
      expect(mockState.storage.put).toHaveBeenCalledWith('key', 'value');
    });
  });

  describe('Namespace Abstraction', () => {
    let mockNamespace: DurableObjectNamespace;
    let mockStub: DurableObjectStub;

    beforeEach(() => {
      mockStub = {
        fetch: vi.fn().mockResolvedValue(new Response('OK')),
      } as unknown as DurableObjectStub;

      mockNamespace = {
        idFromName: vi.fn().mockReturnValue({ toString: () => 'id-123' }),
        get: vi.fn().mockReturnValue(mockStub),
      } as unknown as DurableObjectNamespace;
    });

    it('should create CoordinatorNamespace from DurableObjectNamespace', () => {
      const namespace = createCloudflareNamespace(mockNamespace);
      expect(namespace).toBeDefined();
    });

    it('should resolve coordinator IDs from names', () => {
      const namespace = createCloudflareNamespace(mockNamespace);
      const id = namespace.idFromName('test-shard');

      expect(mockNamespace.idFromName).toHaveBeenCalledWith('test-shard');
      expect(id).toBeDefined();
    });

    it('should get coordinator stubs by ID', () => {
      const namespace = createCloudflareNamespace(mockNamespace);
      const id = namespace.idFromName('test-shard');
      const stub = namespace.get(id);

      expect(mockNamespace.get).toHaveBeenCalled();
      expect(typeof stub.fetch).toBe('function');
    });

    it('should allow RPC via stub.fetch', async () => {
      const namespace = createCloudflareNamespace(mockNamespace);
      const id = namespace.idFromName('test-shard');
      const stub = namespace.get(id);

      const response = await stub.fetch(new Request('https://shard/status'));

      expect(mockStub.fetch).toHaveBeenCalled();
      expect(response.ok).toBe(true);
    });
  });

  describe('Cache Abstraction', () => {
    let mockKV: {
      get: ReturnType<typeof vi.fn>;
      getWithMetadata: ReturnType<typeof vi.fn>;
      put: ReturnType<typeof vi.fn>;
      delete: ReturnType<typeof vi.fn>;
      list: ReturnType<typeof vi.fn>;
    };

    beforeEach(() => {
      mockKV = {
        get: vi.fn(),
        getWithMetadata: vi.fn(),
        put: vi.fn(),
        delete: vi.fn(),
        list: vi.fn(),
      };
    });

    it('should create CloudflareKVCache from KVNamespace', () => {
      const cache = new CloudflareKVCache(mockKV);
      expect(cache).toBeDefined();
      expect(cache.getKVNamespace()).toBe(mockKV);
    });

    it('should implement CacheBackend interface', () => {
      const cache = createCloudflareCache(mockKV);

      expect(typeof cache.get).toBe('function');
      expect(typeof cache.getJson).toBe('function');
      expect(typeof cache.put).toBe('function');
      expect(typeof cache.putJson).toBe('function');
      expect(typeof cache.delete).toBe('function');
      expect(typeof cache.list).toBe('function');
    });

    it('should delegate get to underlying KV', async () => {
      mockKV.get.mockResolvedValue('test-value');

      const cache = createCloudflareCache(mockKV);
      const result = await cache.get('test-key');

      expect(mockKV.get).toHaveBeenCalledWith('test-key', {
        type: undefined,
        cacheTtl: undefined,
      });
      expect(result).toBe('test-value');
    });

    it('should support JSON get/put', async () => {
      mockKV.get.mockResolvedValue({ name: 'test' });

      const cache = createCloudflareCache(mockKV);
      const result = await cache.getJson<{ name: string }>('test-key');

      expect(mockKV.get).toHaveBeenCalledWith('test-key', { type: 'json' });
      expect(result?.name).toBe('test');
    });

    it('should serialize JSON on putJson', async () => {
      const cache = createCloudflareCache(mockKV);
      await cache.putJson('test-key', { count: 42 }, { expirationTtl: 300 });

      expect(mockKV.put).toHaveBeenCalledWith(
        'test-key',
        '{"count":42}',
        { expirationTtl: 300, expiration: undefined, metadata: undefined }
      );
    });
  });

  describe('Type Compatibility', () => {
    it('ObjectStorage interface should be compatible with R2Bucket methods', () => {
      // This is a compile-time check - if it compiles, the types are compatible
      const mockR2: R2Bucket = {
        get: vi.fn(),
        head: vi.fn(),
        put: vi.fn(),
        delete: vi.fn(),
        list: vi.fn(),
        createMultipartUpload: vi.fn(),
      } as unknown as R2Bucket;

      // The ObjectStorage interface uses a subset of R2Bucket methods
      // This verifies they're compatible
      const objectStorage: ObjectStorage = {
        get: mockR2.get as ObjectStorage['get'],
        head: mockR2.head as ObjectStorage['head'],
        put: mockR2.put as ObjectStorage['put'],
        delete: mockR2.delete,
        list: mockR2.list as ObjectStorage['list'],
        createMultipartUpload: mockR2.createMultipartUpload as ObjectStorage['createMultipartUpload'],
      };

      expect(objectStorage).toBeDefined();
    });

it('StorageBackend interface should match DurableObjectStorageBackend', async () => {
      // This verifies the StorageBackend interface is correctly abstracted
      const mockState = {
        id: { toString: () => 'id' },
        storage: {
          sql: { exec: vi.fn().mockReturnValue({ toArray: () => [] }) },
          setAlarm: vi.fn(),
        },
        blockConcurrencyWhile: vi.fn().mockImplementation((fn: () => Promise<unknown>) => fn()),
      } as unknown as DurableObjectState;

      // Import the actual implementation dynamically
      const { DurableObjectStorageBackend } = await import('../../../src/do/shard/types.js');

      const backend: StorageBackend = new DurableObjectStorageBackend(mockState);

      expect(typeof backend.sqlExec).toBe('function');
      expect(typeof backend.getShardId).toBe('function');
      expect(typeof backend.blockConcurrencyWhile).toBe('function');
      expect(typeof backend.setAlarm).toBe('function');
    });
  });
});
