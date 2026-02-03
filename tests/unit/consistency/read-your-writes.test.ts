/**
 * Read-Your-Writes Consistency Tests
 *
 * These tests verify that reads immediately see writes that were just performed.
 * This is achieved by routing all operations through ShardDO which maintains
 * an in-memory buffer of unflushed writes merged with R2 storage.
 *
 * Key consistency guarantees:
 * 1. After insert, subsequent reads see the inserted document
 * 2. After update, subsequent reads see the updated document
 * 3. After delete, subsequent reads do not see the deleted document
 * 4. Read tokens can be used to ensure consistency across requests
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import type { DurableObjectState, DurableObjectStorage, R2Bucket } from '@cloudflare/workers-types';
import { ShardDO, type ShardDOEnv, type WriteOperation } from '../../../src/do/shard.js';

// ============================================================================
// Mock Helpers
// ============================================================================

function createMockStorage(): DurableObjectStorage {
  const data = new Map<string, unknown>();
  const sqlStatements: string[] = [];

  // SQLite tables storage
  const metadata = new Map<string, string>();
  const wal: Array<{ lsn: number; collection: string; op: string; doc_id: string; document: string; flushed: number }> = [];
  const manifests = new Map<string, string>();

  return {
    get: vi.fn(async (key: string) => data.get(key)),
    put: vi.fn(async (key: string, value: unknown) => {
      data.set(key, value);
    }),
    delete: vi.fn(async (key: string) => {
      const existed = data.has(key);
      data.delete(key);
      return existed;
    }),
    list: vi.fn(async (options?: { prefix?: string; limit?: number }) => {
      const result = new Map<string, unknown>();
      for (const [key, value] of data) {
        if (!options?.prefix || key.startsWith(options.prefix)) {
          result.set(key, value);
          if (options?.limit && result.size >= options.limit) break;
        }
      }
      return result;
    }),
    deleteAll: vi.fn(async () => {
      data.clear();
    }),
    getAlarm: vi.fn(async () => null),
    setAlarm: vi.fn(async () => {}),
    deleteAlarm: vi.fn(async () => {}),
    sync: vi.fn(async () => {}),
    transaction: vi.fn(async <T>(closure: () => Promise<T>) => closure()),
    transactionSync: vi.fn(<T>(closure: () => T) => closure()),
    // SQL interface for SQLite
    sql: {
      exec: vi.fn((query: string, ...args: unknown[]) => {
        sqlStatements.push(query);

        // Handle metadata queries
        if (query.includes('INSERT OR REPLACE INTO metadata')) {
          const keyMatch = query.match(/VALUES\s*\('(\w+)'/);
          const key = keyMatch?.[1] || '';
          const value = args[0] as string;
          metadata.set(key, value);
        }
        if (query.includes('SELECT value FROM metadata')) {
          const key = query.match(/key = '(\w+)'/)?.[1];
          const value = key ? metadata.get(key) : undefined;
          return {
            toArray: () => value ? [{ value }] : [],
            one: () => value ? { value } : null,
            raw: () => [],
            columnNames: ['value'],
            rowsRead: value ? 1 : 0,
            rowsWritten: 0,
          };
        }

        // Handle WAL queries
        if (query.includes('INSERT INTO wal')) {
          wal.push({
            lsn: args[0] as number,
            collection: args[1] as string,
            op: args[2] as string,
            doc_id: args[3] as string,
            document: args[4] as string,
            flushed: 0,
          });
        }
        if (query.includes('SELECT') && query.includes('FROM wal WHERE flushed = 0')) {
          const unflushed = wal.filter(e => e.flushed === 0);
          return {
            toArray: () => unflushed,
            one: () => unflushed[0] || null,
            raw: () => [],
            columnNames: ['lsn', 'collection', 'op', 'doc_id', 'document'],
            rowsRead: unflushed.length,
            rowsWritten: 0,
          };
        }
        if (query.includes('UPDATE wal SET flushed = 1')) {
          const lsn = args[0] as number;
          for (const entry of wal) {
            if (entry.lsn <= lsn) entry.flushed = 1;
          }
        }
        if (query.includes('DELETE FROM wal WHERE flushed = 1')) {
          const toRemove = wal.filter(e => e.flushed === 1);
          for (const entry of toRemove) {
            const idx = wal.indexOf(entry);
            if (idx !== -1) wal.splice(idx, 1);
          }
        }

        // Handle manifests queries
        if (query.includes('INSERT OR REPLACE INTO manifests')) {
          manifests.set(args[0] as string, args[1] as string);
        }
        if (query.includes('SELECT collection, data FROM manifests')) {
          const rows = Array.from(manifests.entries()).map(([collection, data]) => ({ collection, data }));
          return {
            toArray: () => rows,
            one: () => rows[0] || null,
            raw: () => [],
            columnNames: ['collection', 'data'],
            rowsRead: rows.length,
            rowsWritten: 0,
          };
        }

        // Return default mock cursor
        return {
          toArray: () => [],
          one: () => null,
          raw: () => [],
          columnNames: [],
          rowsRead: 0,
          rowsWritten: 0,
        };
      }),
    },
    // For test inspection
    _data: data,
    _sqlStatements: sqlStatements,
    _metadata: metadata,
    _wal: wal,
    _manifests: manifests,
  } as unknown as DurableObjectStorage;
}

function createMockState(storage?: DurableObjectStorage): DurableObjectState {
  return {
    id: {
      toString: () => 'test-shard-id',
      equals: (other: { toString: () => string }) => other.toString() === 'test-shard-id',
      name: 'test-shard',
    },
    storage: storage || createMockStorage(),
    waitUntil: vi.fn(),
    blockConcurrencyWhile: vi.fn(async <T>(closure: () => Promise<T>) => closure()),
  } as unknown as DurableObjectState;
}

function createMockR2Bucket(): R2Bucket {
  const objects = new Map<string, Uint8Array>();

  return {
    get: vi.fn(async (key: string) => {
      const data = objects.get(key);
      if (!data) return null;
      return {
        arrayBuffer: async () => data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength),
        text: async () => new TextDecoder().decode(data),
        json: async () => JSON.parse(new TextDecoder().decode(data)),
        body: new ReadableStream(),
        etag: `etag-${key}`,
        key,
        size: data.length,
      };
    }),
    put: vi.fn(async (key: string, value: ArrayBuffer | Uint8Array | string) => {
      const data = value instanceof Uint8Array
        ? value
        : typeof value === 'string'
          ? new TextEncoder().encode(value)
          : new Uint8Array(value);
      objects.set(key, data);
      return { key, size: data.length, etag: `etag-${key}` };
    }),
    delete: vi.fn(async (key: string) => {
      objects.delete(key);
    }),
    head: vi.fn(async (key: string) => {
      const data = objects.get(key);
      if (!data) return null;
      return { key, size: data.length, etag: `etag-${key}` };
    }),
    list: vi.fn(async (options?: { prefix?: string }) => {
      const results = [];
      for (const [key, data] of objects) {
        if (!options?.prefix || key.startsWith(options.prefix)) {
          results.push({ key, size: data.length, etag: `etag-${key}` });
        }
      }
      return { objects: results, truncated: false };
    }),
    _objects: objects, // For test inspection
  } as unknown as R2Bucket;
}

function createMockEnv(bucket?: R2Bucket): ShardDOEnv {
  return {
    DATA_BUCKET: bucket || createMockR2Bucket(),
    SHARD_DO: {} as unknown as DurableObjectNamespace,
  };
}

// ============================================================================
// Tests
// ============================================================================

describe('Read-Your-Writes Consistency', () => {
  let shard: ShardDO;
  let env: ShardDOEnv;

  beforeEach(async () => {
    const storage = createMockStorage();
    const state = createMockState(storage);
    env = createMockEnv();
    shard = new ShardDO(state, env);
    await shard.initialize();
  });

  describe('Insert then Read', () => {
    it('should see inserted document immediately after insert', async () => {
      // Insert a document
      const insertResult = await shard.write({
        collection: 'users',
        op: 'insert',
        document: { _id: 'user-1', name: 'Alice', email: 'alice@example.com' },
      });

      expect(insertResult.acknowledged).toBe(true);
      expect(insertResult.insertedId).toBe('user-1');
      expect(insertResult.readToken).toBeDefined();

      // Read immediately after insert - should see the document
      const documents = await shard.find('users', { _id: 'user-1' });

      expect(documents.length).toBe(1);
      expect(documents[0]._id).toBe('user-1');
      expect(documents[0].name).toBe('Alice');
      expect(documents[0].email).toBe('alice@example.com');
    });

    it('should see multiple inserted documents immediately', async () => {
      // Insert multiple documents
      await shard.write({
        collection: 'products',
        op: 'insert',
        document: { _id: 'prod-1', name: 'Widget', price: 9.99 },
      });
      await shard.write({
        collection: 'products',
        op: 'insert',
        document: { _id: 'prod-2', name: 'Gadget', price: 19.99 },
      });
      await shard.write({
        collection: 'products',
        op: 'insert',
        document: { _id: 'prod-3', name: 'Gizmo', price: 29.99 },
      });

      // Read all - should see all documents
      const documents = await shard.find('products', {});

      expect(documents.length).toBe(3);
      const ids = documents.map(d => d._id);
      expect(ids).toContain('prod-1');
      expect(ids).toContain('prod-2');
      expect(ids).toContain('prod-3');
    });

    it('should see inserted document with read token validation', async () => {
      // Insert a document and get read token
      const insertResult = await shard.write({
        collection: 'orders',
        op: 'insert',
        document: { _id: 'order-1', total: 100.00, status: 'pending' },
      });

      const readToken = insertResult.readToken;
      expect(readToken).toMatch(/^test-shard-id:\d+$/);

      // Read with the token to ensure consistency
      const documents = await shard.find('orders', { _id: 'order-1' }, { afterToken: readToken });

      expect(documents.length).toBe(1);
      expect(documents[0].status).toBe('pending');
    });
  });

  describe('Update then Read', () => {
    it('should see updated document immediately after update', async () => {
      // Insert a document first
      await shard.write({
        collection: 'users',
        op: 'insert',
        document: { _id: 'user-2', name: 'Bob', score: 100 },
      });

      // Update the document
      const updateResult = await shard.write({
        collection: 'users',
        op: 'update',
        filter: { _id: 'user-2' },
        update: { $set: { score: 150 } },
      });

      expect(updateResult.acknowledged).toBe(true);
      expect(updateResult.readToken).toBeDefined();

      // Read immediately - should see the update
      const documents = await shard.find('users', { _id: 'user-2' });

      expect(documents.length).toBe(1);
      expect(documents[0].score).toBe(150);
    });

    it('should see multiple updates applied in order', async () => {
      // Insert a document
      await shard.write({
        collection: 'counters',
        op: 'insert',
        document: { _id: 'counter-1', value: 0 },
      });

      // Perform multiple updates
      await shard.write({
        collection: 'counters',
        op: 'update',
        filter: { _id: 'counter-1' },
        update: { $inc: { value: 10 } },
      });
      await shard.write({
        collection: 'counters',
        op: 'update',
        filter: { _id: 'counter-1' },
        update: { $inc: { value: 5 } },
      });
      await shard.write({
        collection: 'counters',
        op: 'update',
        filter: { _id: 'counter-1' },
        update: { $inc: { value: 3 } },
      });

      // Read - should see final value
      const documents = await shard.find('counters', { _id: 'counter-1' });

      expect(documents.length).toBe(1);
      expect(documents[0].value).toBe(18); // 0 + 10 + 5 + 3
    });
  });

  describe('Delete then Read', () => {
    it('should not see deleted document immediately after delete', async () => {
      // Insert a document
      await shard.write({
        collection: 'temp',
        op: 'insert',
        document: { _id: 'temp-1', data: 'temporary' },
      });

      // Verify it exists
      let documents = await shard.find('temp', { _id: 'temp-1' });
      expect(documents.length).toBe(1);

      // Delete the document
      const deleteResult = await shard.write({
        collection: 'temp',
        op: 'delete',
        filter: { _id: 'temp-1' },
      });

      expect(deleteResult.acknowledged).toBe(true);
      expect(deleteResult.readToken).toBeDefined();

      // Read immediately - should NOT see the document
      documents = await shard.find('temp', { _id: 'temp-1' });

      expect(documents.length).toBe(0);
    });

    it('should not see deleted document in findOne either', async () => {
      // Insert a document
      await shard.write({
        collection: 'temp',
        op: 'insert',
        document: { _id: 'temp-2', data: 'will be deleted' },
      });

      // Delete it
      await shard.write({
        collection: 'temp',
        op: 'delete',
        filter: { _id: 'temp-2' },
      });

      // findOne should return null
      const document = await shard.findOne('temp', { _id: 'temp-2' });

      expect(document).toBeNull();
    });
  });

  describe('Mixed Operations', () => {
    it('should handle insert-update-delete sequence correctly', async () => {
      // Insert
      await shard.write({
        collection: 'lifecycle',
        op: 'insert',
        document: { _id: 'doc-1', state: 'created' },
      });

      let docs = await shard.find('lifecycle', { _id: 'doc-1' });
      expect(docs.length).toBe(1);
      expect(docs[0].state).toBe('created');

      // Update
      await shard.write({
        collection: 'lifecycle',
        op: 'update',
        filter: { _id: 'doc-1' },
        update: { $set: { state: 'updated' } },
      });

      docs = await shard.find('lifecycle', { _id: 'doc-1' });
      expect(docs.length).toBe(1);
      expect(docs[0].state).toBe('updated');

      // Delete
      await shard.write({
        collection: 'lifecycle',
        op: 'delete',
        filter: { _id: 'doc-1' },
      });

      docs = await shard.find('lifecycle', { _id: 'doc-1' });
      expect(docs.length).toBe(0);
    });

    it('should handle operations on multiple documents correctly', async () => {
      // Insert multiple documents
      await shard.write({
        collection: 'multi',
        op: 'insert',
        document: { _id: 'a', value: 1 },
      });
      await shard.write({
        collection: 'multi',
        op: 'insert',
        document: { _id: 'b', value: 2 },
      });
      await shard.write({
        collection: 'multi',
        op: 'insert',
        document: { _id: 'c', value: 3 },
      });

      // Update one
      await shard.write({
        collection: 'multi',
        op: 'update',
        filter: { _id: 'b' },
        update: { $set: { value: 20 } },
      });

      // Delete one
      await shard.write({
        collection: 'multi',
        op: 'delete',
        filter: { _id: 'c' },
      });

      // Read all - should see 2 documents with correct values
      const docs = await shard.find('multi', {});

      expect(docs.length).toBe(2);

      const docA = docs.find(d => d._id === 'a');
      const docB = docs.find(d => d._id === 'b');
      const docC = docs.find(d => d._id === 'c');

      expect(docA).toBeDefined();
      expect(docA!.value).toBe(1);

      expect(docB).toBeDefined();
      expect(docB!.value).toBe(20);

      expect(docC).toBeUndefined();
    });
  });

  describe('Read Token Semantics', () => {
    it('should generate monotonically increasing LSN in read tokens', async () => {
      const tokens: string[] = [];

      // Perform multiple writes and collect tokens
      for (let i = 0; i < 5; i++) {
        const result = await shard.write({
          collection: 'tokens',
          op: 'insert',
          document: { _id: `doc-${i}`, index: i },
        });
        tokens.push(result.readToken);
      }

      // Extract LSNs and verify they're monotonically increasing
      const lsns = tokens.map(token => {
        const [, lsn] = token.split(':');
        return parseInt(lsn, 10);
      });

      for (let i = 1; i < lsns.length; i++) {
        expect(lsns[i]).toBeGreaterThan(lsns[i - 1]);
      }
    });

    it('should reject read tokens with wrong shard ID', async () => {
      // Insert a document
      await shard.write({
        collection: 'test',
        op: 'insert',
        document: { _id: 'test-1', data: 'test' },
      });

      // Try to read with a token from a different shard
      const wrongShardToken = 'wrong-shard-id:1';

      await expect(
        shard.find('test', {}, { afterToken: wrongShardToken })
      ).rejects.toThrow(/shard ID mismatch/);
    });

    it('should reject read tokens referencing future LSN', async () => {
      // Insert a document (LSN will be 1)
      await shard.write({
        collection: 'test',
        op: 'insert',
        document: { _id: 'test-1', data: 'test' },
      });

      // Try to read with a token referencing a future LSN
      const futureToken = 'test-shard-id:9999';

      await expect(
        shard.find('test', {}, { afterToken: futureToken })
      ).rejects.toThrow(/future LSN/);
    });
  });

  describe('Consistency After Flush', () => {
    it('should maintain consistency after flush to R2', async () => {
      // Insert documents
      await shard.write({
        collection: 'durable',
        op: 'insert',
        document: { _id: 'doc-1', value: 'before flush' },
      });

      // Verify visible before flush
      let docs = await shard.find('durable', { _id: 'doc-1' });
      expect(docs.length).toBe(1);

      // Flush to R2
      await shard.flush();

      // Still visible after flush
      docs = await shard.find('durable', { _id: 'doc-1' });
      expect(docs.length).toBe(1);
      expect(docs[0].value).toBe('before flush');
    });

    it('should see new writes after flush', async () => {
      // Insert and flush
      await shard.write({
        collection: 'durable',
        op: 'insert',
        document: { _id: 'doc-2', value: 'first' },
      });
      await shard.flush();

      // Insert more
      await shard.write({
        collection: 'durable',
        op: 'insert',
        document: { _id: 'doc-3', value: 'second' },
      });

      // Should see both - one from R2, one from buffer
      const docs = await shard.find('durable', {});
      expect(docs.length).toBe(2);
    });

    it('should see updates to flushed documents', async () => {
      // Insert and flush
      await shard.write({
        collection: 'durable',
        op: 'insert',
        document: { _id: 'doc-4', status: 'original' },
      });
      await shard.flush();

      // Update the flushed document
      await shard.write({
        collection: 'durable',
        op: 'update',
        filter: { _id: 'doc-4' },
        update: { $set: { status: 'modified' } },
      });

      // Should see the updated version
      const docs = await shard.find('durable', { _id: 'doc-4' });
      expect(docs.length).toBe(1);
      expect(docs[0].status).toBe('modified');
    });

    it('should not see deleted flushed documents', async () => {
      // Insert and flush
      await shard.write({
        collection: 'durable',
        op: 'insert',
        document: { _id: 'doc-5', data: 'to delete' },
      });
      await shard.flush();

      // Delete the flushed document
      await shard.write({
        collection: 'durable',
        op: 'delete',
        filter: { _id: 'doc-5' },
      });

      // Should NOT see the deleted document
      const docs = await shard.find('durable', { _id: 'doc-5' });
      expect(docs.length).toBe(0);
    });
  });
});
