/**
 * Client -> Storage Pipeline Integration Tests
 *
 * Tests the complete data flow from client operations through the storage layer:
 * - MongoLake client initialization with different storage backends
 * - Document CRUD operations through the storage layer
 * - Storage backend operations (MemoryStorage, FileSystemStorage patterns)
 * - Data persistence and retrieval verification
 * - Multipart upload handling
 * - Storage key validation
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  MemoryStorage,
  FileSystemStorage,
  createStorage,
  validateStorageKey,
  validateStoragePrefix,
  InvalidStorageKeyError,
  Semaphore,
  createBufferedMultipartUpload,
  concatenateParts,
  type StorageBackend,
} from '../../../src/storage/index.js';
import { MongoLake, Collection, Database } from '../../../src/client/index.js';
import { resetDocumentCounter, createUser, createUsers } from '../../utils/factories.js';

// ============================================================================
// Test Types
// ============================================================================

interface TestDocument {
  _id: string;
  name: string;
  email?: string;
  age?: number;
  status?: string;
  tags?: string[];
  metadata?: Record<string, unknown>;
}

// ============================================================================
// Test Helpers
// ============================================================================

function createTestClient(): MongoLake {
  const storage = new MemoryStorage();
  const client = new MongoLake({ local: '.test-mongolake' });
  // @ts-expect-error - accessing private field for testing
  client.storage = storage;
  return client;
}

// ============================================================================
// Storage Key Validation Tests
// ============================================================================

describe('Storage Key Validation', () => {
  it('should accept valid storage keys', () => {
    expect(() => validateStorageKey('data/collection/file.parquet')).not.toThrow();
    expect(() => validateStorageKey('simple-key')).not.toThrow();
    expect(() => validateStorageKey('nested/path/to/file.txt')).not.toThrow();
    expect(() => validateStorageKey('key_with_underscores')).not.toThrow();
  });

  it('should reject empty storage keys', () => {
    expect(() => validateStorageKey('')).toThrow(InvalidStorageKeyError);
    expect(() => validateStorageKey('   ')).toThrow(InvalidStorageKeyError);
  });

  it('should reject absolute paths', () => {
    expect(() => validateStorageKey('/absolute/path')).toThrow(InvalidStorageKeyError);
    expect(() => validateStorageKey('/etc/passwd')).toThrow(InvalidStorageKeyError);
  });

  it('should reject path traversal attempts', () => {
    expect(() => validateStorageKey('../etc/passwd')).toThrow(InvalidStorageKeyError);
    expect(() => validateStorageKey('data/../../../etc/passwd')).toThrow(InvalidStorageKeyError);
    expect(() => validateStorageKey('data/nested/../../../secret')).toThrow(InvalidStorageKeyError);
  });

  it('should normalize backslashes and reject traversal', () => {
    expect(() => validateStorageKey('data\\..\\..\\etc\\passwd')).toThrow(InvalidStorageKeyError);
  });

  it('should validate storage prefixes', () => {
    // Empty prefix is allowed for listing all
    expect(() => validateStoragePrefix('')).not.toThrow();

    // Valid prefixes
    expect(() => validateStoragePrefix('data/')).not.toThrow();
    expect(() => validateStoragePrefix('collection/2024-01-01/')).not.toThrow();

    // Invalid prefixes (same rules as keys)
    expect(() => validateStoragePrefix('../')).toThrow(InvalidStorageKeyError);
    expect(() => validateStoragePrefix('/absolute/')).toThrow(InvalidStorageKeyError);
  });
});

// ============================================================================
// MemoryStorage Backend Tests
// ============================================================================

describe('MemoryStorage Backend Integration', () => {
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
  });

  afterEach(() => {
    storage.clear();
  });

  it('should store and retrieve data', async () => {
    const key = 'test/data.bin';
    const data = new Uint8Array([1, 2, 3, 4, 5]);

    await storage.put(key, data);
    const retrieved = await storage.get(key);

    expect(retrieved).not.toBeNull();
    expect(retrieved).toEqual(data);
  });

  it('should return null for non-existent keys', async () => {
    const result = await storage.get('non-existent-key');
    expect(result).toBeNull();
  });

  it('should delete data', async () => {
    const key = 'test/to-delete.bin';
    const data = new Uint8Array([1, 2, 3]);

    await storage.put(key, data);
    expect(await storage.exists(key)).toBe(true);

    await storage.delete(key);
    expect(await storage.exists(key)).toBe(false);
  });

  it('should list keys by prefix', async () => {
    await storage.put('prefix/file1.txt', new Uint8Array([1]));
    await storage.put('prefix/file2.txt', new Uint8Array([2]));
    await storage.put('prefix/subdir/file3.txt', new Uint8Array([3]));
    await storage.put('other/file4.txt', new Uint8Array([4]));

    const prefixedKeys = await storage.list('prefix/');

    expect(prefixedKeys).toContain('prefix/file1.txt');
    expect(prefixedKeys).toContain('prefix/file2.txt');
    expect(prefixedKeys).toContain('prefix/subdir/file3.txt');
    expect(prefixedKeys).not.toContain('other/file4.txt');
  });

  it('should check key existence', async () => {
    const key = 'test/exists.bin';

    expect(await storage.exists(key)).toBe(false);

    await storage.put(key, new Uint8Array([1, 2, 3]));

    expect(await storage.exists(key)).toBe(true);
  });

  it('should get object metadata (head)', async () => {
    const key = 'test/metadata.bin';
    const data = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    await storage.put(key, data);
    const head = await storage.head(key);

    expect(head).not.toBeNull();
    expect(head!.size).toBe(10);
  });

  it('should return null for head of non-existent key', async () => {
    const head = await storage.head('non-existent');
    expect(head).toBeNull();
  });

  it('should support stream operations', async () => {
    const key = 'test/stream.bin';
    const data = new Uint8Array([1, 2, 3, 4, 5]);

    await storage.put(key, data);

    const stream = await storage.getStream(key);
    expect(stream).not.toBeNull();

    // Read from stream
    const reader = stream!.getReader();
    const { value } = await reader.read();
    reader.releaseLock();

    expect(value).toEqual(data);
  });

  it('should put data from stream', async () => {
    const key = 'test/put-stream.bin';
    const data = new Uint8Array([10, 20, 30, 40, 50]);

    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(data);
        controller.close();
      },
    });

    await storage.putStream(key, stream);

    const retrieved = await storage.get(key);
    expect(retrieved).toEqual(data);
  });

  it('should clear all data', async () => {
    await storage.put('key1', new Uint8Array([1]));
    await storage.put('key2', new Uint8Array([2]));
    await storage.put('key3', new Uint8Array([3]));

    const keysBefore = await storage.list('');
    expect(keysBefore.length).toBe(3);

    storage.clear();

    const keysAfter = await storage.list('');
    expect(keysAfter.length).toBe(0);
  });
});

// ============================================================================
// Multipart Upload Tests
// ============================================================================

describe('Multipart Upload Integration', () => {
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
  });

  afterEach(() => {
    storage.clear();
  });

  it('should handle multipart upload through storage', async () => {
    const key = 'test/multipart.bin';

    const upload = await storage.createMultipartUpload(key);

    // Upload parts
    const part1 = await upload.uploadPart(1, new Uint8Array([1, 2, 3]));
    const part2 = await upload.uploadPart(2, new Uint8Array([4, 5, 6]));
    const part3 = await upload.uploadPart(3, new Uint8Array([7, 8, 9]));

    // Complete upload
    await upload.complete([part1, part2, part3]);

    // Verify result
    const result = await storage.get(key);
    expect(result).toEqual(new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9]));
  });

  it('should handle out-of-order part uploads', async () => {
    const key = 'test/out-of-order.bin';

    const upload = await storage.createMultipartUpload(key);

    // Upload parts in random order
    const part3 = await upload.uploadPart(3, new Uint8Array([7, 8, 9]));
    const part1 = await upload.uploadPart(1, new Uint8Array([1, 2, 3]));
    const part2 = await upload.uploadPart(2, new Uint8Array([4, 5, 6]));

    // Complete with correct order
    await upload.complete([part1, part2, part3]);

    const result = await storage.get(key);
    expect(result).toEqual(new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9]));
  });

  it('should abort multipart upload', async () => {
    const key = 'test/aborted.bin';

    const upload = await storage.createMultipartUpload(key);

    await upload.uploadPart(1, new Uint8Array([1, 2, 3]));
    await upload.uploadPart(2, new Uint8Array([4, 5, 6]));

    // Abort instead of complete
    await upload.abort();

    // Key should not exist
    expect(await storage.exists(key)).toBe(false);
  });

  it('should create buffered multipart upload directly', async () => {
    let completedData: Uint8Array | null = null;

    const upload = createBufferedMultipartUpload(async (data) => {
      completedData = data;
    });

    const part1 = await upload.uploadPart(1, new Uint8Array([10, 20]));
    const part2 = await upload.uploadPart(2, new Uint8Array([30, 40]));

    await upload.complete([part1, part2]);

    expect(completedData).toEqual(new Uint8Array([10, 20, 30, 40]));
  });
});

// ============================================================================
// Semaphore Concurrency Control Tests
// ============================================================================

describe('Semaphore Concurrency Control', () => {
  it('should limit concurrent operations', async () => {
    const semaphore = new Semaphore(2);
    let concurrentCount = 0;
    let maxConcurrent = 0;

    const tasks = Array.from({ length: 10 }, async () => {
      await semaphore.acquire();
      concurrentCount++;
      maxConcurrent = Math.max(maxConcurrent, concurrentCount);

      // Simulate work
      await new Promise((resolve) => setTimeout(resolve, 10));

      concurrentCount--;
      semaphore.release();
    });

    await Promise.all(tasks);

    // Should never exceed semaphore limit
    expect(maxConcurrent).toBeLessThanOrEqual(2);
  });

  it('should track available permits', () => {
    const semaphore = new Semaphore(5);

    expect(semaphore.availablePermits).toBe(5);
  });

  it('should track waiting count', async () => {
    const semaphore = new Semaphore(1);

    // Acquire the only permit
    await semaphore.acquire();

    // Start tasks that will wait
    const waitingTasks = [
      semaphore.acquire(),
      semaphore.acquire(),
    ];

    // Give time for tasks to queue
    await new Promise((resolve) => setTimeout(resolve, 5));

    expect(semaphore.waitingCount).toBe(2);

    // Release to allow tasks to complete
    semaphore.release();
    semaphore.release();
    semaphore.release();

    await Promise.all(waitingTasks);
  });

  it('should throw for invalid permit count', () => {
    expect(() => new Semaphore(0)).toThrow();
    expect(() => new Semaphore(-1)).toThrow();
  });
});

// ============================================================================
// Part Concatenation Tests
// ============================================================================

describe('Part Concatenation', () => {
  it('should concatenate multiple parts', () => {
    const parts = [
      new Uint8Array([1, 2, 3]),
      new Uint8Array([4, 5]),
      new Uint8Array([6, 7, 8, 9]),
    ];

    const result = concatenateParts(parts);

    expect(result).toEqual(new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9]));
  });

  it('should handle empty parts array', () => {
    const result = concatenateParts([]);
    expect(result).toEqual(new Uint8Array([]));
  });

  it('should handle single part', () => {
    const part = new Uint8Array([1, 2, 3]);
    const result = concatenateParts([part]);
    expect(result).toEqual(part);
  });
});

// ============================================================================
// Client -> Storage Integration Tests
// ============================================================================

describe('MongoLake Client Storage Integration', () => {
  let client: MongoLake;
  let collection: Collection<TestDocument>;

  beforeEach(() => {
    resetDocumentCounter();
    client = createTestClient();
    collection = client.db('testdb').collection<TestDocument>('users');
  });

  afterEach(async () => {
    await client.close();
  });

  it('should insert and retrieve documents', async () => {
    const doc: TestDocument = {
      _id: 'user-1',
      name: 'Alice',
      email: 'alice@example.com',
      age: 30,
    };

    const result = await collection.insertOne(doc);
    expect(result.insertedId).toBe('user-1');

    const found = await collection.findOne({ _id: 'user-1' });
    expect(found).toBeDefined();
    expect(found?.name).toBe('Alice');
    expect(found?.email).toBe('alice@example.com');
  });

  it('should handle bulk inserts', async () => {
    const docs: TestDocument[] = [
      { _id: 'bulk-1', name: 'User 1' },
      { _id: 'bulk-2', name: 'User 2' },
      { _id: 'bulk-3', name: 'User 3' },
    ];

    const result = await collection.insertMany(docs);
    expect(result.insertedCount).toBe(3);
    expect(result.insertedIds).toEqual({
      0: 'bulk-1',
      1: 'bulk-2',
      2: 'bulk-3',
    });
  });

  it('should update documents', async () => {
    await collection.insertOne({
      _id: 'update-test',
      name: 'Original',
      status: 'active',
    });

    const updateResult = await collection.updateOne(
      { _id: 'update-test' },
      { $set: { name: 'Updated', status: 'modified' } }
    );

    expect(updateResult.modifiedCount).toBe(1);

    const updated = await collection.findOne({ _id: 'update-test' });
    expect(updated?.name).toBe('Updated');
    expect(updated?.status).toBe('modified');
  });

  it('should delete documents', async () => {
    await collection.insertOne({ _id: 'delete-test', name: 'To Delete' });

    const deleteResult = await collection.deleteOne({ _id: 'delete-test' });
    expect(deleteResult.deletedCount).toBe(1);

    const found = await collection.findOne({ _id: 'delete-test' });
    expect(found).toBeNull();
  });

  it('should find documents with filters', async () => {
    await collection.insertMany([
      { _id: 'filter-1', name: 'Alice', age: 30, status: 'active' },
      { _id: 'filter-2', name: 'Bob', age: 25, status: 'inactive' },
      { _id: 'filter-3', name: 'Charlie', age: 35, status: 'active' },
      { _id: 'filter-4', name: 'Diana', age: 28, status: 'active' },
    ]);

    // Find by status
    const activeUsers = await collection.find({ status: 'active' }).toArray();
    expect(activeUsers.length).toBe(3);

    // Find by age range
    const youngUsers = await collection.find({ age: { $lt: 30 } }).toArray();
    expect(youngUsers.length).toBe(2);
  });

  it('should handle nested document storage', async () => {
    const nestedDoc: TestDocument = {
      _id: 'nested-1',
      name: 'Nested Test',
      metadata: {
        created: '2024-01-01',
        tags: ['test', 'nested'],
        nested: {
          level2: {
            value: 42,
          },
        },
      },
    };

    await collection.insertOne(nestedDoc);

    const found = await collection.findOne({ _id: 'nested-1' });
    expect(found?.metadata?.created).toBe('2024-01-01');
    // @ts-expect-error - accessing nested structure
    expect(found?.metadata?.nested?.level2?.value).toBe(42);
  });

  it('should count documents', async () => {
    await collection.insertMany([
      { _id: 'count-1', name: 'A', status: 'active' },
      { _id: 'count-2', name: 'B', status: 'active' },
      { _id: 'count-3', name: 'C', status: 'inactive' },
    ]);

    const totalCount = await collection.countDocuments({});
    expect(totalCount).toBe(3);

    const activeCount = await collection.countDocuments({ status: 'active' });
    expect(activeCount).toBe(2);
  });
});

// ============================================================================
// Database Operations Tests
// ============================================================================

describe('Database Operations Integration', () => {
  let client: MongoLake;

  beforeEach(() => {
    resetDocumentCounter();
    client = createTestClient();
  });

  afterEach(async () => {
    await client.close();
  });

  it('should create and access multiple databases', async () => {
    const db1 = client.db('database1');
    const db2 = client.db('database2');

    const coll1 = db1.collection('shared_name');
    const coll2 = db2.collection('shared_name');

    await coll1.insertOne({ _id: 'doc-1', source: 'db1' });
    await coll2.insertOne({ _id: 'doc-1', source: 'db2' });

    const found1 = await coll1.findOne({ _id: 'doc-1' });
    const found2 = await coll2.findOne({ _id: 'doc-1' });

    expect(found1?.source).toBe('db1');
    expect(found2?.source).toBe('db2');
  });

  it('should list collections in database', async () => {
    const db = client.db('listtest');

    await db.collection('collection_a').insertOne({ _id: '1' });
    await db.collection('collection_b').insertOne({ _id: '1' });
    await db.collection('collection_c').insertOne({ _id: '1' });

    // Collection listing would be implementation-dependent
    // This tests that operations work without errors
    const collA = db.collection('collection_a');
    const found = await collA.findOne({ _id: '1' });
    expect(found).toBeDefined();
  });
});

// ============================================================================
// Error Handling in Storage Pipeline Tests
// ============================================================================

describe('Error Handling in Storage Pipeline', () => {
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
  });

  afterEach(() => {
    storage.clear();
  });

  it('should reject operations with invalid keys', async () => {
    await expect(storage.put('../invalid', new Uint8Array([1]))).rejects.toThrow(
      InvalidStorageKeyError
    );

    await expect(storage.get('/absolute/path')).rejects.toThrow(InvalidStorageKeyError);

    await expect(storage.delete('path/../../escape')).rejects.toThrow(
      InvalidStorageKeyError
    );
  });

  it('should handle concurrent operations safely', async () => {
    const key = 'concurrent/key';
    const operations: Promise<void>[] = [];

    // Concurrent writes
    for (let i = 0; i < 100; i++) {
      operations.push(storage.put(key, new Uint8Array([i])));
    }

    await Promise.all(operations);

    // Key should exist with some value
    const exists = await storage.exists(key);
    expect(exists).toBe(true);
  });

  it('should handle large data chunks', async () => {
    const key = 'large/data';
    const largeData = new Uint8Array(1024 * 1024); // 1MB
    for (let i = 0; i < largeData.length; i++) {
      largeData[i] = i % 256;
    }

    await storage.put(key, largeData);

    const retrieved = await storage.get(key);
    expect(retrieved?.length).toBe(largeData.length);
    expect(retrieved?.[0]).toBe(0);
    expect(retrieved?.[255]).toBe(255);
    expect(retrieved?.[256]).toBe(0);
  });
});

// ============================================================================
// Storage Factory Tests
// ============================================================================

describe('Storage Factory', () => {
  it('should create MemoryStorage for testing', () => {
    const storage = new MemoryStorage();
    expect(storage).toBeInstanceOf(MemoryStorage);
  });

  it('should create storage from config with local path', () => {
    const storage = createStorage({ local: '.test-mongolake' });
    expect(storage).toBeDefined();
    expect(storage).toBeInstanceOf(FileSystemStorage);
  });

  it('should default to FileSystemStorage with .mongolake path', () => {
    const storage = createStorage({});
    expect(storage).toBeInstanceOf(FileSystemStorage);
  });
});
