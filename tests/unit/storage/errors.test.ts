/**
 * Storage Layer Error Scenario Tests
 *
 * Tests for error handling in the storage abstraction layer:
 * - File not found errors
 * - Permission denied errors
 * - Network failures
 * - Resource exhaustion
 * - Invalid path handling
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  MemoryStorage,
  FileSystemStorage,
  R2Storage,
  createStorage,
  type StorageBackend,
} from '../../../src/storage/index.js';
import { S3Storage } from '../../../src/storage/s3.js';

// ============================================================================
// MemoryStorage Error Scenarios
// ============================================================================

describe('MemoryStorage - Error Scenarios', () => {
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
  });

  describe('get errors', () => {
    it('should return null for non-existent key', async () => {
      const result = await storage.get('nonexistent-key');
      expect(result).toBeNull();
    });

    it('should return null for empty string key', async () => {
      const result = await storage.get('');
      expect(result).toBeNull();
    });

    it('should handle keys with special characters', async () => {
      const specialKey = 'key/with/slashes/and.dots';
      await storage.put(specialKey, new Uint8Array([1, 2, 3]));
      const result = await storage.get(specialKey);
      expect(result).not.toBeNull();
    });
  });

  describe('head errors', () => {
    it('should return null for non-existent key', async () => {
      const result = await storage.head('nonexistent-key');
      expect(result).toBeNull();
    });
  });

  describe('delete errors', () => {
    it('should not throw when deleting non-existent key', async () => {
      await expect(storage.delete('nonexistent-key')).resolves.not.toThrow();
    });

    it('should handle double deletion gracefully', async () => {
      await storage.put('key', new Uint8Array([1]));
      await storage.delete('key');
      await expect(storage.delete('key')).resolves.not.toThrow();
    });
  });

  describe('list errors', () => {
    it('should return empty array for non-matching prefix', async () => {
      await storage.put('abc/file1', new Uint8Array([1]));
      const result = await storage.list('xyz/');
      expect(result).toEqual([]);
    });

    it('should handle empty prefix', async () => {
      await storage.put('file1', new Uint8Array([1]));
      await storage.put('file2', new Uint8Array([2]));
      const result = await storage.list('');
      expect(result.length).toBe(2);
    });
  });

  describe('multipart upload errors', () => {
    it('should handle abort after upload parts', async () => {
      const upload = await storage.createMultipartUpload('test-key');
      await upload.uploadPart(1, new Uint8Array([1, 2, 3]));
      await upload.uploadPart(2, new Uint8Array([4, 5, 6]));
      await upload.abort();

      // Key should not exist after abort
      const result = await storage.get('test-key');
      expect(result).toBeNull();
    });

    it('should handle out-of-order part numbers in complete', async () => {
      const upload = await storage.createMultipartUpload('test-key');
      const part2 = await upload.uploadPart(2, new Uint8Array([4, 5, 6]));
      const part1 = await upload.uploadPart(1, new Uint8Array([1, 2, 3]));

      // Parts should be sorted during complete
      await upload.complete([part2, part1]);

      const result = await storage.get('test-key');
      expect(result).not.toBeNull();
      // Verify order: part1 then part2
      expect(Array.from(result!)).toEqual([1, 2, 3, 4, 5, 6]);
    });

    it('should handle empty parts array in complete', async () => {
      const upload = await storage.createMultipartUpload('test-key');
      await upload.complete([]);

      const result = await storage.get('test-key');
      expect(result).not.toBeNull();
      expect(result!.length).toBe(0);
    });
  });

  describe('edge cases', () => {
    it('should handle very long keys', async () => {
      const longKey = 'a'.repeat(10000);
      await storage.put(longKey, new Uint8Array([1]));
      const result = await storage.get(longKey);
      expect(result).not.toBeNull();
    });

    it('should handle empty data', async () => {
      await storage.put('empty', new Uint8Array(0));
      const result = await storage.get('empty');
      expect(result).not.toBeNull();
      expect(result!.length).toBe(0);
    });

    it('should handle large data', async () => {
      const largeData = new Uint8Array(10 * 1024 * 1024); // 10MB
      await storage.put('large', largeData);
      const result = await storage.get('large');
      expect(result).not.toBeNull();
      expect(result!.length).toBe(largeData.length);
    });

    it('should handle overwrite of existing key', async () => {
      await storage.put('key', new Uint8Array([1, 2, 3]));
      await storage.put('key', new Uint8Array([4, 5, 6]));
      const result = await storage.get('key');
      expect(Array.from(result!)).toEqual([4, 5, 6]);
    });
  });
});

// ============================================================================
// S3Storage Error Scenarios
// ============================================================================

describe('S3Storage - Error Scenarios', () => {
  let storage: S3Storage;
  const mockFetch = vi.fn();

  beforeEach(() => {
    vi.stubGlobal('fetch', mockFetch);
    storage = new S3Storage({
      endpoint: 'https://s3.example.com',
      accessKeyId: 'test-key',
      secretAccessKey: 'test-secret',
      bucket: 'test-bucket',
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('get errors', () => {
    it('should return null for 404 response', async () => {
      mockFetch.mockResolvedValueOnce({
        status: 404,
        ok: false,
      });

      const result = await storage.get('nonexistent');
      expect(result).toBeNull();
    });

    it('should throw for server error (500)', async () => {
      mockFetch.mockResolvedValueOnce({
        status: 500,
        ok: false,
      });

      await expect(storage.get('key')).rejects.toThrow('S3 GET failed: 500');
    });

    it('should throw for forbidden error (403)', async () => {
      mockFetch.mockResolvedValueOnce({
        status: 403,
        ok: false,
      });

      await expect(storage.get('key')).rejects.toThrow('S3 GET failed: 403');
    });

    it('should handle network timeout', async () => {
      mockFetch.mockRejectedValueOnce(new Error('Network timeout'));

      await expect(storage.get('key')).rejects.toThrow('Network timeout');
    });

    it('should handle connection refused', async () => {
      mockFetch.mockRejectedValueOnce(new Error('ECONNREFUSED'));

      await expect(storage.get('key')).rejects.toThrow('ECONNREFUSED');
    });
  });

  describe('put errors', () => {
    it('should throw for server error', async () => {
      mockFetch.mockResolvedValueOnce({
        status: 500,
        ok: false,
      });

      await expect(storage.put('key', new Uint8Array([1, 2, 3]))).rejects.toThrow(
        'S3 PUT failed: 500'
      );
    });

    it('should throw for quota exceeded (507)', async () => {
      mockFetch.mockResolvedValueOnce({
        status: 507,
        ok: false,
      });

      await expect(storage.put('key', new Uint8Array([1, 2, 3]))).rejects.toThrow(
        'S3 PUT failed: 507'
      );
    });

    it('should handle request entity too large (413)', async () => {
      mockFetch.mockResolvedValueOnce({
        status: 413,
        ok: false,
      });

      await expect(storage.put('key', new Uint8Array([1, 2, 3]))).rejects.toThrow(
        'S3 PUT failed: 413'
      );
    });

    it('should handle DNS resolution failure', async () => {
      mockFetch.mockRejectedValueOnce(new Error('getaddrinfo ENOTFOUND'));

      await expect(storage.put('key', new Uint8Array([1, 2, 3]))).rejects.toThrow(
        'getaddrinfo ENOTFOUND'
      );
    });
  });

  describe('delete errors', () => {
    it('should not throw for 404 on delete', async () => {
      mockFetch.mockResolvedValueOnce({
        status: 404,
        ok: false,
      });

      await expect(storage.delete('key')).resolves.not.toThrow();
    });

    it('should throw for server error on delete', async () => {
      mockFetch.mockResolvedValueOnce({
        status: 500,
        ok: false,
      });

      await expect(storage.delete('key')).rejects.toThrow('S3 DELETE failed: 500');
    });
  });

  describe('list errors', () => {
    it('should throw for server error', async () => {
      mockFetch.mockResolvedValueOnce({
        status: 500,
        ok: false,
      });

      await expect(storage.list('prefix/')).rejects.toThrow('S3 LIST failed: 500');
    });

    it('should handle malformed XML response', async () => {
      mockFetch.mockResolvedValueOnce({
        status: 200,
        ok: true,
        text: async () => 'not xml',
      });

      // Should return empty array for malformed response (no <Key> tags)
      const result = await storage.list('prefix/');
      expect(result).toEqual([]);
    });
  });

  describe('head errors', () => {
    it('should return null for 404', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 404,
      });

      const result = await storage.head('key');
      expect(result).toBeNull();
    });

    it('should return null for any non-ok response', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 403,
      });

      const result = await storage.head('key');
      expect(result).toBeNull();
    });
  });

  describe('exists errors', () => {
    it('should return false for network error', async () => {
      mockFetch.mockRejectedValueOnce(new Error('Network error'));

      await expect(storage.exists('key')).rejects.toThrow('Network error');
    });
  });

  describe('authentication errors', () => {
    it('should handle invalid credentials (401)', async () => {
      mockFetch.mockResolvedValueOnce({
        status: 401,
        ok: false,
      });

      await expect(storage.get('key')).rejects.toThrow('S3 GET failed: 401');
    });

    it('should handle access denied (403)', async () => {
      mockFetch.mockResolvedValueOnce({
        status: 403,
        ok: false,
      });

      await expect(storage.get('key')).rejects.toThrow('S3 GET failed: 403');
    });
  });
});

// ============================================================================
// createStorage Factory Error Scenarios
// ============================================================================

describe('createStorage - Error Scenarios', () => {
  it('should default to FileSystemStorage with .mongolake path', () => {
    const storage = createStorage({});
    expect(storage).toBeInstanceOf(FileSystemStorage);
  });

  it('should create FileSystemStorage when local is specified', () => {
    const storage = createStorage({ local: '/tmp/test' });
    expect(storage).toBeInstanceOf(FileSystemStorage);
  });

  it('should throw helpful error when endpoint is specified', () => {
    expect(() =>
      createStorage({
        endpoint: 'https://s3.example.com',
        accessKeyId: 'key',
        secretAccessKey: 'secret',
        bucketName: 'bucket',
      })
    ).toThrow('S3Storage is now a separate optional import');
  });
});

// ============================================================================
// R2Storage Error Scenarios (Mock-based)
// ============================================================================

describe('R2Storage - Error Scenarios', () => {
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

  describe('get errors', () => {
    it('should return null when object not found', async () => {
      mockBucket.get.mockResolvedValueOnce(null);

      const result = await storage.get('nonexistent');
      expect(result).toBeNull();
    });

    it('should propagate R2 errors', async () => {
      mockBucket.get.mockRejectedValueOnce(new Error('R2 internal error'));

      await expect(storage.get('key')).rejects.toThrow('R2 internal error');
    });
  });

  describe('put errors', () => {
    it('should propagate R2 put errors', async () => {
      mockBucket.put.mockRejectedValueOnce(new Error('R2 write failed'));

      await expect(storage.put('key', new Uint8Array([1, 2, 3]))).rejects.toThrow(
        'R2 write failed'
      );
    });

    it('should handle quota exceeded', async () => {
      mockBucket.put.mockRejectedValueOnce(new Error('Object storage quota exceeded'));

      await expect(storage.put('key', new Uint8Array([1, 2, 3]))).rejects.toThrow(
        'quota exceeded'
      );
    });
  });

  describe('list errors', () => {
    it('should handle list failures', async () => {
      mockBucket.list.mockRejectedValueOnce(new Error('R2 list failed'));

      await expect(storage.list('prefix/')).rejects.toThrow('R2 list failed');
    });

    it('should handle pagination correctly', async () => {
      mockBucket.list
        .mockResolvedValueOnce({
          objects: [{ key: 'file1' }],
          truncated: true,
          cursor: 'cursor1',
        })
        .mockResolvedValueOnce({
          objects: [{ key: 'file2' }],
          truncated: false,
        });

      const result = await storage.list('');
      expect(result).toHaveLength(2);
      expect(result).toContain('file1');
      expect(result).toContain('file2');
    });
  });

  describe('multipart upload errors', () => {
    it('should handle upload initiation failure', async () => {
      mockBucket.createMultipartUpload.mockRejectedValueOnce(
        new Error('Failed to create multipart upload')
      );

      await expect(storage.createMultipartUpload('key')).rejects.toThrow(
        'Failed to create multipart upload'
      );
    });

    it('should handle part upload failure', async () => {
      const mockUpload = {
        uploadPart: vi.fn().mockRejectedValueOnce(new Error('Part upload failed')),
        complete: vi.fn(),
        abort: vi.fn(),
      };
      mockBucket.createMultipartUpload.mockResolvedValueOnce(mockUpload);

      const upload = await storage.createMultipartUpload('key');
      await expect(upload.uploadPart(1, new Uint8Array([1, 2, 3]))).rejects.toThrow(
        'Part upload failed'
      );
    });

    it('should handle complete failure', async () => {
      const mockUpload = {
        uploadPart: vi.fn().mockResolvedValue({ partNumber: 1, etag: 'etag1' }),
        complete: vi.fn().mockRejectedValueOnce(new Error('Complete failed')),
        abort: vi.fn(),
      };
      mockBucket.createMultipartUpload.mockResolvedValueOnce(mockUpload);

      const upload = await storage.createMultipartUpload('key');
      const part = await upload.uploadPart(1, new Uint8Array([1, 2, 3]));
      await expect(upload.complete([part])).rejects.toThrow('Complete failed');
    });

    it('should handle abort failure gracefully', async () => {
      const mockUpload = {
        uploadPart: vi.fn(),
        complete: vi.fn(),
        abort: vi.fn().mockRejectedValueOnce(new Error('Abort failed')),
      };
      mockBucket.createMultipartUpload.mockResolvedValueOnce(mockUpload);

      const upload = await storage.createMultipartUpload('key');
      await expect(upload.abort()).rejects.toThrow('Abort failed');
    });
  });

  describe('head errors', () => {
    it('should return null for non-existent object', async () => {
      mockBucket.head.mockResolvedValueOnce(null);

      const result = await storage.head('nonexistent');
      expect(result).toBeNull();
    });

    it('should propagate head errors', async () => {
      mockBucket.head.mockRejectedValueOnce(new Error('R2 head failed'));

      await expect(storage.head('key')).rejects.toThrow('R2 head failed');
    });
  });
});

// ============================================================================
// FileSystemStorage Error Scenarios
// ============================================================================

describe('FileSystemStorage - Error Scenarios', () => {
  // Note: These tests require mocking Node.js fs module
  // In a real test environment, you would use proper mocking

  it('should handle ENOENT for get', async () => {
    const storage = new FileSystemStorage('/nonexistent/path');

    // This should return null for non-existent files
    const result = await storage.get('nonexistent-file');
    expect(result).toBeNull();
  });

  it('should handle ENOENT for head', async () => {
    const storage = new FileSystemStorage('/nonexistent/path');

    const result = await storage.head('nonexistent-file');
    expect(result).toBeNull();
  });

  it('should not throw for deleting non-existent file', async () => {
    const storage = new FileSystemStorage('/nonexistent/path');

    await expect(storage.delete('nonexistent-file')).resolves.not.toThrow();
  });

  it('should return false for exists on non-existent file', async () => {
    const storage = new FileSystemStorage('/nonexistent/path');

    const result = await storage.exists('nonexistent-file');
    expect(result).toBe(false);
  });

  it('should return empty array for list on non-existent directory', async () => {
    const storage = new FileSystemStorage('/nonexistent/path');

    const result = await storage.list('nonexistent/');
    expect(result).toEqual([]);
  });
});

// ============================================================================
// Helper Function Tests
// ============================================================================

describe('concatenateParts', () => {
  it('should handle empty array', async () => {
    const { concatenateParts } = await import('../../../src/storage/index.js');
    const result = concatenateParts([]);
    expect(result).toEqual(new Uint8Array(0));
  });

  it('should handle single part', async () => {
    const { concatenateParts } = await import('../../../src/storage/index.js');
    const part = new Uint8Array([1, 2, 3]);
    const result = concatenateParts([part]);
    expect(Array.from(result)).toEqual([1, 2, 3]);
  });

  it('should concatenate multiple parts in order', async () => {
    const { concatenateParts } = await import('../../../src/storage/index.js');
    const parts = [new Uint8Array([1, 2]), new Uint8Array([3, 4]), new Uint8Array([5, 6])];
    const result = concatenateParts(parts);
    expect(Array.from(result)).toEqual([1, 2, 3, 4, 5, 6]);
  });

  it('should handle parts with empty arrays', async () => {
    const { concatenateParts } = await import('../../../src/storage/index.js');
    const parts = [new Uint8Array([1, 2]), new Uint8Array(0), new Uint8Array([3, 4])];
    const result = concatenateParts(parts);
    expect(Array.from(result)).toEqual([1, 2, 3, 4]);
  });
});
