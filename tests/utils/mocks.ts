/**
 * Test Mocks
 *
 * Common mock implementations for storage, network, and timer operations.
 * Provides reusable mocks to reduce boilerplate in tests.
 */

import { vi, type MockedFunction } from 'vitest';
import type {
  R2Bucket,
  R2Object,
  R2ObjectBody,
  R2ListOptions,
  R2Objects,
  R2MultipartUpload,
  R2UploadedPart,
} from '../../src/types.js';
import { createBufferedMultipartUpload } from '../../src/storage/index.js';

// ============================================================================
// Storage Mocks
// ============================================================================

/**
 * In-memory storage implementation for testing.
 * Simulates R2/S3 bucket behavior.
 */
export interface MockStorage {
  data: Map<string, Uint8Array>;
  get(key: string): Promise<Uint8Array | null>;
  put(key: string, value: Uint8Array | ArrayBuffer | string): Promise<void>;
  delete(key: string): Promise<void>;
  list(prefix?: string): Promise<string[]>;
  head(key: string): Promise<{ key: string; size: number } | null>;
  exists(key: string): Promise<boolean>;
  clear(): void;
}

/**
 * Create an in-memory storage mock.
 */
export function createMockStorage(): MockStorage {
  const data = new Map<string, Uint8Array>();

  return {
    data,

    async get(key: string): Promise<Uint8Array | null> {
      return data.get(key) ?? null;
    },

    async put(key: string, value: Uint8Array | ArrayBuffer | string): Promise<void> {
      const bytes =
        value instanceof Uint8Array
          ? value
          : typeof value === 'string'
            ? new TextEncoder().encode(value)
            : new Uint8Array(value);
      data.set(key, bytes);
    },

    async delete(key: string): Promise<void> {
      data.delete(key);
    },

    async list(prefix?: string): Promise<string[]> {
      const keys: string[] = [];
      for (const key of data.keys()) {
        if (!prefix || key.startsWith(prefix)) {
          keys.push(key);
        }
      }
      return keys;
    },

    async head(key: string): Promise<{ key: string; size: number } | null> {
      const value = data.get(key);
      if (!value) return null;
      return { key, size: value.length };
    },

    async exists(key: string): Promise<boolean> {
      return data.has(key);
    },

    clear(): void {
      data.clear();
    },
  };
}

// ============================================================================
// R2 Bucket Mock
// ============================================================================

/**
 * Mock R2 multipart upload.
 */
export interface MockMultipartUpload extends R2MultipartUpload {
  parts: Map<number, Uint8Array>;
  completed: boolean;
  aborted: boolean;
}

/**
 * Create a mock R2 bucket for testing.
 */
export function createMockR2Bucket(): R2Bucket & { _objects: Map<string, Uint8Array> } {
  const objects = new Map<string, Uint8Array>();

  const bucket: R2Bucket & { _objects: Map<string, Uint8Array> } = {
    _objects: objects,

    async get(key: string): Promise<R2ObjectBody | null> {
      const data = objects.get(key);
      if (!data) return null;
      return {
        arrayBuffer: async () => data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength),
        text: async () => new TextDecoder().decode(data),
        json: async <T>() => JSON.parse(new TextDecoder().decode(data)) as T,
        body: new ReadableStream({
          start(controller) {
            controller.enqueue(data);
            controller.close();
          },
        }),
        etag: `etag-${key}`,
      };
    },

    async head(key: string): Promise<R2Object | null> {
      const data = objects.get(key);
      if (!data) return null;
      return {
        key,
        size: data.length,
        etag: `etag-${key}`,
      };
    },

    async put(key: string, value: ArrayBuffer | Uint8Array | string): Promise<R2Object> {
      const data =
        value instanceof Uint8Array
          ? value
          : typeof value === 'string'
            ? new TextEncoder().encode(value)
            : new Uint8Array(value);
      objects.set(key, data);
      return {
        key,
        size: data.length,
        etag: `etag-${key}`,
      };
    },

    async delete(key: string): Promise<void> {
      objects.delete(key);
    },

    async list(options?: R2ListOptions): Promise<R2Objects> {
      const result: R2Object[] = [];
      for (const [key, data] of objects) {
        if (!options?.prefix || key.startsWith(options.prefix)) {
          result.push({
            key,
            size: data.length,
            etag: `etag-${key}`,
          });
          if (options?.limit && result.length >= options.limit) {
            break;
          }
        }
      }
      return {
        objects: result,
        truncated: false,
      };
    },

    async createMultipartUpload(key: string): Promise<R2MultipartUpload> {
      let completed = false;
      let aborted = false;

      // Create a buffered upload that stores to our mock objects map
      const bufferedUpload = createBufferedMultipartUpload(async (data) => {
        objects.set(key, data);
      });

      return {
        async uploadPart(partNumber: number, value: ArrayBuffer | Uint8Array): Promise<R2UploadedPart> {
          if (completed || aborted) {
            throw new Error('Upload already finalized');
          }
          const data = value instanceof Uint8Array ? value : new Uint8Array(value);
          const result = await bufferedUpload.uploadPart(partNumber, data);
          return {
            partNumber: result.partNumber,
            etag: `etag-part-${partNumber}`,
          };
        },

        async complete(uploadedParts: R2UploadedPart[]): Promise<R2Object> {
          if (completed || aborted) {
            throw new Error('Upload already finalized');
          }
          completed = true;

          // Convert R2UploadedPart[] to UploadedPart[] format
          await bufferedUpload.complete(uploadedParts.map(p => ({
            partNumber: p.partNumber,
            etag: p.etag,
          })));

          const combined = objects.get(key)!;
          return {
            key,
            size: combined.length,
            etag: `etag-${key}`,
          };
        },

        async abort(): Promise<void> {
          if (completed) {
            throw new Error('Cannot abort completed upload');
          }
          aborted = true;
          await bufferedUpload.abort();
        },
      };
    },
  };

  return bucket;
}

/**
 * Create a mock R2 bucket with spied methods.
 */
export function createSpiedR2Bucket() {
  const bucket = createMockR2Bucket();
  return {
    ...bucket,
    get: vi.fn(bucket.get.bind(bucket)),
    head: vi.fn(bucket.head.bind(bucket)),
    put: vi.fn(bucket.put.bind(bucket)),
    delete: vi.fn(bucket.delete.bind(bucket)),
    list: vi.fn(bucket.list.bind(bucket)),
    createMultipartUpload: vi.fn(bucket.createMultipartUpload.bind(bucket)),
    _objects: bucket._objects,
  };
}

// ============================================================================
// Durable Object Storage Mock
// ============================================================================

export interface MockDurableObjectStorage {
  data: Map<string, unknown>;
  alarms: number[];
  sqlStatements: string[];
  get<T = unknown>(key: string): Promise<T | undefined>;
  put<T>(key: string, value: T): Promise<void>;
  delete(key: string): Promise<boolean>;
  deleteAll(): Promise<void>;
  list<T = unknown>(options?: { prefix?: string; limit?: number }): Promise<Map<string, T>>;
  getAlarm(): Promise<number | null>;
  setAlarm(scheduledTime: number | Date): Promise<void>;
  deleteAlarm(): Promise<void>;
  sync(): Promise<void>;
  transaction<T>(closure: () => Promise<T>): Promise<T>;
  transactionSync<T>(closure: () => T): T;
  sql: {
    exec: MockedFunction<(query: string, ...args: unknown[]) => {
      toArray: () => unknown[];
      one: () => unknown | null;
      raw: () => unknown[];
      columnNames: string[];
      rowsRead: number;
      rowsWritten: number;
    }>;
  };
}

/**
 * Create a mock Durable Object storage.
 */
export function createMockDurableObjectStorage(): MockDurableObjectStorage {
  const data = new Map<string, unknown>();
  const alarms: number[] = [];
  const sqlStatements: string[] = [];

  return {
    data,
    alarms,
    sqlStatements,

    async get<T = unknown>(key: string): Promise<T | undefined> {
      return data.get(key) as T | undefined;
    },

    async put<T>(key: string, value: T): Promise<void> {
      data.set(key, value);
    },

    async delete(key: string): Promise<boolean> {
      const existed = data.has(key);
      data.delete(key);
      return existed;
    },

    async deleteAll(): Promise<void> {
      data.clear();
    },

    async list<T = unknown>(options?: { prefix?: string; limit?: number }): Promise<Map<string, T>> {
      const result = new Map<string, T>();
      for (const [key, value] of data) {
        if (!options?.prefix || key.startsWith(options.prefix)) {
          result.set(key, value as T);
          if (options?.limit && result.size >= options.limit) {
            break;
          }
        }
      }
      return result;
    },

    async getAlarm(): Promise<number | null> {
      return alarms.length > 0 ? alarms[0] : null;
    },

    async setAlarm(scheduledTime: number | Date): Promise<void> {
      const time = typeof scheduledTime === 'number' ? scheduledTime : scheduledTime.getTime();
      alarms.push(time);
    },

    async deleteAlarm(): Promise<void> {
      alarms.shift();
    },

    async sync(): Promise<void> {
      // No-op
    },

    async transaction<T>(closure: () => Promise<T>): Promise<T> {
      return closure();
    },

    transactionSync<T>(closure: () => T): T {
      return closure();
    },

    sql: {
      exec: vi.fn((query: string) => {
        sqlStatements.push(query);
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
  };
}

// ============================================================================
// Network Mocks
// ============================================================================

export interface MockFetch {
  fn: MockedFunction<typeof fetch>;
  mockResponse(status: number, body?: unknown, headers?: Record<string, string>): void;
  mockJsonResponse<T>(data: T, status?: number): void;
  mockError(error: Error): void;
  mockTimeout(delayMs: number): void;
  reset(): void;
  calls: Array<{ url: string; init?: RequestInit }>;
}

/**
 * Create a mock fetch function.
 */
export function createMockFetch(): MockFetch {
  const calls: Array<{ url: string; init?: RequestInit }> = [];

  const fn = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === 'string' ? input : input.toString();
    calls.push({ url, init });
    return new Response(null, { status: 200 });
  }) as MockedFunction<typeof fetch>;

  const trackingWrapper = (impl: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>) => {
    return async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input.toString();
      calls.push({ url, init });
      return impl(input, init);
    };
  };

  return {
    fn,
    calls,

    mockResponse(status: number, body?: unknown, headers?: Record<string, string>): void {
      fn.mockImplementationOnce(trackingWrapper(async () =>
        new Response(
          body !== undefined ? JSON.stringify(body) : null,
          { status, headers: { 'Content-Type': 'application/json', ...headers } }
        )
      ));
    },

    mockJsonResponse<T>(data: T, status: number = 200): void {
      fn.mockImplementationOnce(trackingWrapper(async () =>
        new Response(JSON.stringify(data), {
          status,
          headers: { 'Content-Type': 'application/json' },
        })
      ));
    },

    mockError(error: Error): void {
      fn.mockImplementationOnce(trackingWrapper(async () => {
        throw error;
      }));
    },

    mockTimeout(delayMs: number): void {
      fn.mockImplementationOnce(trackingWrapper(() =>
        new Promise((_, reject) => {
          setTimeout(() => reject(new Error('Request timeout')), delayMs);
        })
      ));
    },

    reset(): void {
      fn.mockReset();
      calls.length = 0;
    },
  };
}

/**
 * Install global fetch mock.
 */
export function installFetchMock(): MockFetch {
  const mock = createMockFetch();
  vi.stubGlobal('fetch', mock.fn);
  return mock;
}

/**
 * Restore global fetch.
 */
export function restoreFetch(): void {
  vi.unstubAllGlobals();
}

// ============================================================================
// Timer Mocks
// ============================================================================

export interface MockTimers {
  advanceTime(ms: number): Promise<void>;
  advanceToNextTimer(): Promise<void>;
  runAllTimers(): Promise<void>;
  getTimerCount(): number;
  clearAllTimers(): void;
  useRealTimers(): void;
  useFakeTimers(): void;
}

/**
 * Create timer mocks using Vitest.
 */
export function createMockTimers(): MockTimers {
  return {
    async advanceTime(ms: number): Promise<void> {
      await vi.advanceTimersByTimeAsync(ms);
    },

    async advanceToNextTimer(): Promise<void> {
      await vi.advanceTimersToNextTimerAsync();
    },

    async runAllTimers(): Promise<void> {
      await vi.runAllTimersAsync();
    },

    getTimerCount(): number {
      return vi.getTimerCount();
    },

    clearAllTimers(): void {
      vi.clearAllTimers();
    },

    useRealTimers(): void {
      vi.useRealTimers();
    },

    useFakeTimers(): void {
      vi.useFakeTimers();
    },
  };
}

// ============================================================================
// Worker Environment Mocks
// ============================================================================

export interface MockMongoLakeEnv {
  BUCKET: ReturnType<typeof createSpiedR2Bucket>;
  RPC_NAMESPACE: {
    idFromName: MockedFunction<(name: string) => { toString: () => string }>;
    get: MockedFunction<(id: { toString: () => string }) => {
      fetch: MockedFunction<(request: Request) => Promise<Response>>;
    }>;
  };
  OAUTH_SECRET: string;
  REQUIRE_AUTH?: boolean;
  ENVIRONMENT?: 'development' | 'production';
  ALLOWED_ORIGINS?: string;
}

/**
 * Create a mock MongoLake worker environment.
 */
export function createMockEnv(options: {
  requireAuth?: boolean;
  environment?: 'development' | 'production';
  allowedOrigins?: string;
} = {}): MockMongoLakeEnv {
  const stubFetch = vi.fn().mockResolvedValue(
    new Response(JSON.stringify({ acknowledged: true, readToken: 'test-token' }), {
      headers: { 'Content-Type': 'application/json' },
    })
  );

  const stub = {
    fetch: stubFetch,
  };

  return {
    BUCKET: createSpiedR2Bucket(),
    RPC_NAMESPACE: {
      idFromName: vi.fn().mockReturnValue({ toString: () => 'test-shard-id' }),
      get: vi.fn().mockReturnValue(stub),
    },
    OAUTH_SECRET: 'test-secret',
    REQUIRE_AUTH: options.requireAuth,
    ENVIRONMENT: options.environment,
    ALLOWED_ORIGINS: options.allowedOrigins,
  };
}

// ============================================================================
// Request/Response Helpers
// ============================================================================

/**
 * Create a mock HTTP request.
 */
export function createMockRequest(
  method: string,
  path: string,
  body?: unknown,
  headers?: Record<string, string>
): Request {
  const url = `https://mongolake.workers.dev${path}`;
  const init: RequestInit = {
    method,
    headers: {
      'Content-Type': 'application/json',
      ...headers,
    },
  };
  if (body !== undefined) {
    init.body = JSON.stringify(body);
  }
  return new Request(url, init);
}

/**
 * Create a WebSocket upgrade request.
 */
export function createWebSocketRequest(
  path: string,
  headers?: Record<string, string>
): Request {
  const url = `https://mongolake.workers.dev${path}`;
  return new Request(url, {
    headers: {
      Upgrade: 'websocket',
      Connection: 'Upgrade',
      'Sec-WebSocket-Key': 'dGhlIHNhbXBsZSBub25jZQ==',
      'Sec-WebSocket-Version': '13',
      ...headers,
    },
  });
}

/**
 * Parse JSON response body.
 */
export async function parseJsonResponse<T>(response: Response): Promise<T> {
  return response.json() as Promise<T>;
}

// ============================================================================
// Event Emitter Mock
// ============================================================================

export interface MockEventEmitter {
  on(event: string, callback: (...args: unknown[]) => void): void;
  off(event: string, callback: (...args: unknown[]) => void): void;
  emit(event: string, ...args: unknown[]): void;
  once(event: string, callback: (...args: unknown[]) => void): void;
  listenerCount(event: string): number;
  removeAllListeners(event?: string): void;
}

/**
 * Create a mock event emitter.
 */
export function createMockEventEmitter(): MockEventEmitter {
  const listeners = new Map<string, Set<(...args: unknown[]) => void>>();

  return {
    on(event: string, callback: (...args: unknown[]) => void): void {
      if (!listeners.has(event)) {
        listeners.set(event, new Set());
      }
      listeners.get(event)!.add(callback);
    },

    off(event: string, callback: (...args: unknown[]) => void): void {
      listeners.get(event)?.delete(callback);
    },

    emit(event: string, ...args: unknown[]): void {
      for (const callback of listeners.get(event) ?? []) {
        callback(...args);
      }
    },

    once(event: string, callback: (...args: unknown[]) => void): void {
      const wrapper = (...args: unknown[]) => {
        this.off(event, wrapper);
        callback(...args);
      };
      this.on(event, wrapper);
    },

    listenerCount(event: string): number {
      return listeners.get(event)?.size ?? 0;
    },

    removeAllListeners(event?: string): void {
      if (event) {
        listeners.delete(event);
      } else {
        listeners.clear();
      }
    },
  };
}

// ============================================================================
// Socket Mock (re-exported from dedicated module)
// ============================================================================

/**
 * Re-export socket mocks from the dedicated mock-socket module.
 * For new code, prefer importing directly from './mock-socket.js'.
 *
 * @deprecated Import from './mock-socket.js' instead for new code.
 */
export {
  MockSocketImpl as MockSocket,
  createMockSocket,
  type MockSocket as MockSocketInterface,
} from './mock-socket.js';
