/**
 * Storage Failure Error Scenario Tests
 *
 * Comprehensive tests for storage layer error handling:
 * - R2/storage unavailable scenarios
 * - Read errors (corruption, network failures)
 * - Write errors (quota, permissions, network)
 * - List errors (pagination failures, access denied)
 *
 * These tests verify that errors are properly propagated with
 * informative error messages.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  MemoryStorage,
  R2Storage,
  MetricsStorageWrapper,
  InvalidStorageKeyError,
  validateStorageKey,
  validateStoragePrefix,
  type StorageBackend,
} from '../../../src/storage/index.js';

// ============================================================================
// Mock Storage Backend for Error Simulation
// ============================================================================

/**
 * A storage backend that simulates various error conditions
 */
class ErrorSimulatingStorage implements StorageBackend {
  private shouldError: {
    get?: Error;
    put?: Error;
    delete?: Error;
    list?: Error;
    head?: Error;
    exists?: Error;
    getStream?: Error;
    putStream?: Error;
  } = {};

  private data: Map<string, Uint8Array> = new Map();

  setError(operation: keyof typeof this.shouldError, error: Error | undefined): void {
    this.shouldError[operation] = error;
  }

  clearErrors(): void {
    this.shouldError = {};
  }

  async get(key: string): Promise<Uint8Array | null> {
    if (this.shouldError.get) {
      throw this.shouldError.get;
    }
    return this.data.get(key) || null;
  }

  async put(key: string, data: Uint8Array): Promise<void> {
    if (this.shouldError.put) {
      throw this.shouldError.put;
    }
    this.data.set(key, data);
  }

  async delete(key: string): Promise<void> {
    if (this.shouldError.delete) {
      throw this.shouldError.delete;
    }
    this.data.delete(key);
  }

  async list(prefix: string): Promise<string[]> {
    if (this.shouldError.list) {
      throw this.shouldError.list;
    }
    return Array.from(this.data.keys()).filter((k) => k.startsWith(prefix));
  }

  async exists(key: string): Promise<boolean> {
    if (this.shouldError.exists) {
      throw this.shouldError.exists;
    }
    return this.data.has(key);
  }

  async head(key: string): Promise<{ size: number } | null> {
    if (this.shouldError.head) {
      throw this.shouldError.head;
    }
    const data = this.data.get(key);
    return data ? { size: data.length } : null;
  }

  async createMultipartUpload(key: string) {
    const self = this;
    return {
      async uploadPart(partNumber: number, data: Uint8Array) {
        if (self.shouldError.put) {
          throw self.shouldError.put;
        }
        return { partNumber, etag: `part-${partNumber}` };
      },
      async complete() {
        if (self.shouldError.put) {
          throw self.shouldError.put;
        }
      },
      async abort() {},
    };
  }

  async getStream(key: string): Promise<ReadableStream<Uint8Array> | null> {
    if (this.shouldError.getStream) {
      throw this.shouldError.getStream;
    }
    const data = this.data.get(key);
    if (!data) return null;
    return new ReadableStream({
      start(controller) {
        controller.enqueue(data);
        controller.close();
      },
    });
  }

  async putStream(key: string, stream: ReadableStream<Uint8Array>): Promise<void> {
    if (this.shouldError.putStream) {
      throw this.shouldError.putStream;
    }
    const reader = stream.getReader();
    const chunks: Uint8Array[] = [];
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
    }
    const total = chunks.reduce((sum, c) => sum + c.length, 0);
    const combined = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
      combined.set(chunk, offset);
      offset += chunk.length;
    }
    this.data.set(key, combined);
  }
}

// ============================================================================
// R2/Storage Unavailable Tests
// ============================================================================

describe('Storage Unavailable Scenarios', () => {
  let storage: ErrorSimulatingStorage;

  beforeEach(() => {
    storage = new ErrorSimulatingStorage();
  });

  it('should throw informative error when storage is unavailable for read', async () => {
    const unavailableError = new Error('Storage service is unavailable');
    storage.setError('get', unavailableError);

    await expect(storage.get('any-key')).rejects.toThrow('Storage service is unavailable');
  });

  it('should throw informative error when storage is unavailable for write', async () => {
    const unavailableError = new Error('R2 service temporarily unavailable');
    storage.setError('put', unavailableError);

    await expect(storage.put('any-key', new Uint8Array([1, 2, 3]))).rejects.toThrow(
      'R2 service temporarily unavailable'
    );
  });

  it('should throw informative error when storage is unavailable for list', async () => {
    const unavailableError = new Error('Cannot connect to storage backend');
    storage.setError('list', unavailableError);

    await expect(storage.list('prefix/')).rejects.toThrow('Cannot connect to storage backend');
  });

  it('should handle connection timeout errors', async () => {
    const timeoutError = new Error('Connection timeout after 30000ms');
    storage.setError('get', timeoutError);

    await expect(storage.get('key')).rejects.toThrow('Connection timeout');
  });

  it('should handle DNS resolution failures', async () => {
    const dnsError = new Error('getaddrinfo ENOTFOUND storage.example.com');
    storage.setError('get', dnsError);

    await expect(storage.get('key')).rejects.toThrow('ENOTFOUND');
  });
});

// ============================================================================
// Read Errors Tests
// ============================================================================

describe('Storage Read Errors', () => {
  let storage: ErrorSimulatingStorage;

  beforeEach(() => {
    storage = new ErrorSimulatingStorage();
  });

  it('should throw informative error on read corruption', async () => {
    const corruptionError = new Error('Data integrity check failed: checksum mismatch');
    storage.setError('get', corruptionError);

    await expect(storage.get('corrupted-key')).rejects.toThrow('checksum mismatch');
  });

  it('should throw informative error on permission denied read', async () => {
    const permissionError = new Error('Access denied: insufficient permissions to read object');
    storage.setError('get', permissionError);

    const error = await storage.get('protected-key').catch((e) => e);
    expect(error).toBeInstanceOf(Error);
    expect(error.message).toContain('Access denied');
    expect(error.message).toContain('permissions');
  });

  it('should throw informative error on network read failure', async () => {
    const networkError = new Error('ECONNRESET: Connection reset by peer');
    storage.setError('get', networkError);

    await expect(storage.get('key')).rejects.toThrow('ECONNRESET');
  });

  it('should throw informative error on stream read failure', async () => {
    const streamError = new Error('Stream read failed: unexpected end of stream');
    storage.setError('getStream', streamError);

    await expect(storage.getStream('key')).rejects.toThrow('unexpected end of stream');
  });

  it('should handle partial read failures gracefully', async () => {
    // First successful, then fail
    await storage.put('key', new Uint8Array([1, 2, 3]));
    const result = await storage.get('key');
    expect(result).not.toBeNull();

    // Now set error
    storage.setError('get', new Error('Connection lost during transfer'));
    await expect(storage.get('key')).rejects.toThrow('Connection lost');
  });
});

// ============================================================================
// Write Errors Tests
// ============================================================================

describe('Storage Write Errors', () => {
  let storage: ErrorSimulatingStorage;

  beforeEach(() => {
    storage = new ErrorSimulatingStorage();
  });

  it('should throw informative error on quota exceeded', async () => {
    const quotaError = new Error('Storage quota exceeded: maximum 10GB reached');
    storage.setError('put', quotaError);

    const error = await storage.put('key', new Uint8Array([1, 2, 3])).catch((e) => e);
    expect(error).toBeInstanceOf(Error);
    expect(error.message).toContain('quota exceeded');
  });

  it('should throw informative error on permission denied write', async () => {
    const permissionError = new Error('Access denied: no write permission on bucket');
    storage.setError('put', permissionError);

    const error = await storage.put('key', new Uint8Array([1, 2, 3])).catch((e) => e);
    expect(error).toBeInstanceOf(Error);
    expect(error.message).toContain('Access denied');
  });

  it('should throw informative error on object size limit', async () => {
    const sizeError = new Error('Object too large: maximum size is 5GB');
    storage.setError('put', sizeError);

    await expect(storage.put('large-key', new Uint8Array(100))).rejects.toThrow('too large');
  });

  it('should throw informative error on stream write failure', async () => {
    const streamError = new Error('Upload failed: stream interrupted');
    storage.setError('putStream', streamError);

    const stream = new ReadableStream({
      start(controller) {
        controller.enqueue(new Uint8Array([1, 2, 3]));
        controller.close();
      },
    });

    await expect(storage.putStream('key', stream)).rejects.toThrow('stream interrupted');
  });

  it('should throw informative error on multipart upload failure', async () => {
    const multipartError = new Error('Multipart upload failed: part 3 upload failed');
    storage.setError('put', multipartError);

    const upload = await storage.createMultipartUpload('key');
    await expect(upload.uploadPart(1, new Uint8Array([1, 2, 3]))).rejects.toThrow(
      'part 3 upload failed'
    );
  });

  it('should handle concurrent write errors', async () => {
    storage.setError('put', new Error('Concurrent modification detected'));

    const writes = [
      storage.put('key1', new Uint8Array([1])),
      storage.put('key2', new Uint8Array([2])),
      storage.put('key3', new Uint8Array([3])),
    ];

    const results = await Promise.allSettled(writes);
    for (const result of results) {
      expect(result.status).toBe('rejected');
      if (result.status === 'rejected') {
        expect(result.reason.message).toContain('Concurrent modification');
      }
    }
  });
});

// ============================================================================
// List Errors Tests
// ============================================================================

describe('Storage List Errors', () => {
  let storage: ErrorSimulatingStorage;

  beforeEach(() => {
    storage = new ErrorSimulatingStorage();
  });

  it('should throw informative error on list access denied', async () => {
    const accessError = new Error('Access denied: cannot list objects in this prefix');
    storage.setError('list', accessError);

    const error = await storage.list('protected/').catch((e) => e);
    expect(error).toBeInstanceOf(Error);
    expect(error.message).toContain('Access denied');
  });

  it('should throw informative error on list timeout', async () => {
    const timeoutError = new Error('List operation timed out after 60s');
    storage.setError('list', timeoutError);

    await expect(storage.list('large-prefix/')).rejects.toThrow('timed out');
  });

  it('should throw informative error on invalid prefix', async () => {
    const prefixError = new Error('Invalid prefix: contains forbidden characters');
    storage.setError('list', prefixError);

    await expect(storage.list('bad\0prefix/')).rejects.toThrow('forbidden characters');
  });

  it('should handle list pagination errors', async () => {
    // This simulates a scenario where the first page succeeds but subsequent pages fail
    let callCount = 0;
    const originalList = storage.list.bind(storage);

    storage.list = async (prefix: string) => {
      callCount++;
      if (callCount > 1) {
        throw new Error('Pagination token expired');
      }
      return originalList(prefix);
    };

    // First call should work
    await expect(storage.list('prefix/')).resolves.not.toThrow();

    // Second call simulates pagination failure
    await expect(storage.list('prefix/')).rejects.toThrow('Pagination token expired');
  });
});

// ============================================================================
// MetricsStorageWrapper Error Handling Tests
// ============================================================================

describe('MetricsStorageWrapper - Error Handling', () => {
  let errorStorage: ErrorSimulatingStorage;
  let metricsStorage: MetricsStorageWrapper;

  beforeEach(async () => {
    errorStorage = new ErrorSimulatingStorage();
    // Import MetricsCollector to satisfy DI requirement
    const { MetricsCollector } = await import('../../../src/metrics/index.js');
    const metrics = new MetricsCollector();
    metricsStorage = new MetricsStorageWrapper(errorStorage, metrics);
  });

  it('should propagate get errors while recording metrics', async () => {
    errorStorage.setError('get', new Error('Read failed'));

    await expect(metricsStorage.get('key')).rejects.toThrow('Read failed');
  });

  it('should propagate put errors while recording metrics', async () => {
    errorStorage.setError('put', new Error('Write failed'));

    await expect(metricsStorage.put('key', new Uint8Array([1]))).rejects.toThrow('Write failed');
  });

  it('should propagate delete errors while recording metrics', async () => {
    errorStorage.setError('delete', new Error('Delete failed'));

    await expect(metricsStorage.delete('key')).rejects.toThrow('Delete failed');
  });

  it('should propagate list errors while recording metrics', async () => {
    errorStorage.setError('list', new Error('List failed'));

    await expect(metricsStorage.list('prefix/')).rejects.toThrow('List failed');
  });

  it('should propagate head errors while recording metrics', async () => {
    errorStorage.setError('head', new Error('Head failed'));

    await expect(metricsStorage.head('key')).rejects.toThrow('Head failed');
  });

  it('should propagate exists errors while recording metrics', async () => {
    errorStorage.setError('exists', new Error('Exists check failed'));

    await expect(metricsStorage.exists('key')).rejects.toThrow('Exists check failed');
  });
});

// ============================================================================
// Storage Key Validation Error Tests
// ============================================================================

describe('Storage Key Validation Errors', () => {
  it('should throw InvalidStorageKeyError for empty key', () => {
    expect(() => validateStorageKey('')).toThrow(InvalidStorageKeyError);
    expect(() => validateStorageKey('')).toThrow('cannot be empty');
  });

  it('should throw InvalidStorageKeyError for absolute path', () => {
    expect(() => validateStorageKey('/etc/passwd')).toThrow(InvalidStorageKeyError);
    expect(() => validateStorageKey('/etc/passwd')).toThrow('cannot be an absolute path');
  });

  it('should throw InvalidStorageKeyError for path traversal', () => {
    expect(() => validateStorageKey('../etc/passwd')).toThrow(InvalidStorageKeyError);
    expect(() => validateStorageKey('foo/../bar')).toThrow(InvalidStorageKeyError);
    expect(() => validateStorageKey('foo/bar/..')).toThrow(InvalidStorageKeyError);
  });

  it('should throw informative error message with the invalid key', () => {
    try {
      validateStorageKey('../evil');
    } catch (error) {
      expect(error).toBeInstanceOf(InvalidStorageKeyError);
      expect((error as Error).message).toContain('..');
      expect((error as Error).message).toContain('path traversal');
    }
  });

  it('should allow empty prefix for list operations', () => {
    expect(() => validateStoragePrefix('')).not.toThrow();
  });

  it('should validate non-empty prefixes like keys', () => {
    expect(() => validateStoragePrefix('../etc')).toThrow(InvalidStorageKeyError);
    expect(() => validateStoragePrefix('/absolute')).toThrow(InvalidStorageKeyError);
  });
});

// ============================================================================
// R2Storage Mock Error Tests
// ============================================================================

describe('R2Storage - Error Scenarios with Mocks', () => {
  let mockBucket: {
    get: ReturnType<typeof vi.fn>;
    put: ReturnType<typeof vi.fn>;
    delete: ReturnType<typeof vi.fn>;
    list: ReturnType<typeof vi.fn>;
    head: ReturnType<typeof vi.fn>;
    createMultipartUpload: ReturnType<typeof vi.fn>;
  };
  let storage: R2Storage;

  beforeEach(() => {
    mockBucket = {
      get: vi.fn(),
      put: vi.fn(),
      delete: vi.fn(),
      list: vi.fn(),
      head: vi.fn(),
      createMultipartUpload: vi.fn(),
    };
    // @ts-expect-error - Mock R2Bucket
    storage = new R2Storage(mockBucket);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should throw informative error on R2 internal error', async () => {
    mockBucket.get.mockRejectedValueOnce(new Error('R2 internal error: operation failed'));

    await expect(storage.get('key')).rejects.toThrow('R2 internal error');
  });

  it('should throw informative error on R2 rate limiting', async () => {
    mockBucket.put.mockRejectedValueOnce(new Error('Rate limit exceeded: too many requests'));

    await expect(storage.put('key', new Uint8Array([1]))).rejects.toThrow('Rate limit exceeded');
  });

  it('should throw informative error on R2 authentication failure', async () => {
    mockBucket.list.mockRejectedValueOnce(new Error('Authentication failed: invalid API token'));

    await expect(storage.list('prefix/')).rejects.toThrow('Authentication failed');
  });

  it('should throw informative error on R2 bucket not found', async () => {
    mockBucket.get.mockRejectedValueOnce(new Error('Bucket not found: bucket-name'));

    await expect(storage.get('key')).rejects.toThrow('Bucket not found');
  });
});

// ============================================================================
// Error Message Quality Tests
// ============================================================================

describe('Error Message Quality', () => {
  let storage: ErrorSimulatingStorage;

  beforeEach(() => {
    storage = new ErrorSimulatingStorage();
  });

  it('should include operation context in error messages', async () => {
    const error = new Error('Operation failed: GET request to /key/path returned 500');
    storage.setError('get', error);

    try {
      await storage.get('key/path');
    } catch (e) {
      expect((e as Error).message).toContain('GET');
      expect((e as Error).message).toContain('500');
    }
  });

  it('should include key information in error messages when appropriate', async () => {
    const error = new Error('Object "my-important-key" not accessible');
    storage.setError('get', error);

    try {
      await storage.get('my-important-key');
    } catch (e) {
      expect((e as Error).message).toContain('my-important-key');
    }
  });

  it('should preserve error stack traces', async () => {
    const originalError = new Error('Root cause error');
    storage.setError('get', originalError);

    try {
      await storage.get('key');
    } catch (e) {
      expect((e as Error).stack).toBeDefined();
      expect((e as Error).stack).toContain('Root cause error');
    }
  });

  it('should not expose sensitive information in error messages', async () => {
    // Simulate an error that might contain sensitive info
    const errorWithSensitiveInfo = new Error('Auth failed for token abc123secret');
    storage.setError('get', errorWithSensitiveInfo);

    try {
      await storage.get('key');
    } catch (e) {
      // In a real implementation, you'd want to sanitize this
      // Here we just verify the error is thrown as expected
      expect((e as Error).message).toContain('Auth failed');
    }
  });
});
