/**
 * Shared test helpers for ShardDO tests
 */

import { vi } from 'vitest';
import type { DurableObjectState, DurableObjectStorage, R2Bucket } from '@cloudflare/workers-types';

// @ts-expect-error - ShardDO does not exist yet
import { ShardDO, type ShardDOEnv } from '../../../src/do/shard.js';

export { ShardDO };
export type { ShardDOEnv };

export function createMockStorage(): DurableObjectStorage {
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
          // Extract key from VALUES clause - handles both ('key', ?) and (?, ?) patterns
          let key: string;
          const keyMatch = query.match(/VALUES\s*\('(\w+)'/);
          if (keyMatch) {
            key = keyMatch[1];
            metadata.set(key, args[0] as string);
          } else {
            // For dynamic keys passed as parameters
            key = String(args[0]);
            metadata.set(key, String(args[1]));
          }
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

export function createMockState(storage?: DurableObjectStorage): DurableObjectState {
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

export function createMockR2Bucket(): R2Bucket {
  const objects = new Map<string, Uint8Array>();

  return {
    get: vi.fn(async (key: string) => {
      const data = objects.get(key);
      if (!data) return null;
      return {
        arrayBuffer: async () => data.buffer,
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
    list: vi.fn(async (options?: { prefix?: string; limit?: number; cursor?: string }) => {
      const result: Array<{ key: string; size: number; etag: string }> = [];
      for (const [key, data] of objects) {
        if (!options?.prefix || key.startsWith(options.prefix)) {
          result.push({ key, size: data.length, etag: `etag-${key}` });
        }
      }
      return { objects: result, truncated: false };
    }),
    head: vi.fn(async (key: string) => {
      const data = objects.get(key);
      if (!data) return null;
      return { key, size: data.length, etag: `etag-${key}` };
    }),
    createMultipartUpload: vi.fn(),
    resumeMultipartUpload: vi.fn(),
    // For test inspection
    _objects: objects,
  } as unknown as R2Bucket;
}

export function createMockEnv(bucket?: R2Bucket): ShardDOEnv {
  return {
    DATA_BUCKET: bucket || createMockR2Bucket(),
    SHARD_DO: {} as DurableObjectNamespace,
  };
}

export function createTestDocument(overrides: Partial<{ _id: string; name: string; age: number; tags: string[] }> = {}) {
  return {
    _id: overrides._id || `doc-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    name: overrides.name || 'Test User',
    age: overrides.age || 25,
    tags: overrides.tags || ['test'],
  };
}
