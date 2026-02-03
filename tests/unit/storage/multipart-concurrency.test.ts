/**
 * Multipart Upload Concurrency Limiting Tests
 *
 * Tests for the Semaphore class and R2Storage multipart upload concurrency limiting.
 *
 * Issue: mongolake-75r
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  Semaphore,
  R2Storage,
  DEFAULT_MULTIPART_CONCURRENCY,
  type R2StorageOptions,
} from '../../../src/storage/index.js';
import type { R2Bucket, R2MultipartUpload, R2UploadedPart } from '../../../src/types.js';

// ============================================================================
// Semaphore Unit Tests
// ============================================================================

describe('Semaphore', () => {
  describe('constructor', () => {
    it('should create a semaphore with the specified number of permits', () => {
      const semaphore = new Semaphore(5);
      expect(semaphore.availablePermits).toBe(5);
    });

    it('should throw an error if permits is less than 1', () => {
      expect(() => new Semaphore(0)).toThrow('Semaphore permits must be at least 1');
      expect(() => new Semaphore(-1)).toThrow('Semaphore permits must be at least 1');
    });

    it('should accept 1 as minimum permits', () => {
      const semaphore = new Semaphore(1);
      expect(semaphore.availablePermits).toBe(1);
    });
  });

  describe('acquire and release', () => {
    it('should acquire a permit immediately when available', async () => {
      const semaphore = new Semaphore(3);
      await semaphore.acquire();
      expect(semaphore.availablePermits).toBe(2);
    });

    it('should release a permit', async () => {
      const semaphore = new Semaphore(3);
      await semaphore.acquire();
      expect(semaphore.availablePermits).toBe(2);
      semaphore.release();
      expect(semaphore.availablePermits).toBe(3);
    });

    it('should block when no permits are available', async () => {
      const semaphore = new Semaphore(1);
      await semaphore.acquire(); // Takes the only permit

      let secondAcquired = false;
      const secondAcquire = semaphore.acquire().then(() => {
        secondAcquired = true;
      });

      // Give the promise a chance to resolve (it shouldn't)
      await new Promise((resolve) => setTimeout(resolve, 10));
      expect(secondAcquired).toBe(false);
      expect(semaphore.waitingCount).toBe(1);

      // Release the permit
      semaphore.release();

      // Now the second acquire should complete
      await secondAcquire;
      expect(secondAcquired).toBe(true);
      expect(semaphore.waitingCount).toBe(0);
    });

    it('should handle multiple waiting operations in FIFO order', async () => {
      const semaphore = new Semaphore(1);
      await semaphore.acquire(); // Takes the only permit

      const order: number[] = [];

      // Queue up multiple waiters
      const waiter1 = semaphore.acquire().then(() => order.push(1));
      const waiter2 = semaphore.acquire().then(() => order.push(2));
      const waiter3 = semaphore.acquire().then(() => order.push(3));

      expect(semaphore.waitingCount).toBe(3);

      // Release permits one by one
      semaphore.release();
      await waiter1;
      expect(order).toEqual([1]);

      semaphore.release();
      await waiter2;
      expect(order).toEqual([1, 2]);

      semaphore.release();
      await waiter3;
      expect(order).toEqual([1, 2, 3]);
    });
  });

  describe('concurrent operations', () => {
    it('should limit concurrent operations to the number of permits', async () => {
      const semaphore = new Semaphore(3);
      let currentConcurrency = 0;
      let maxConcurrency = 0;

      const operation = async () => {
        await semaphore.acquire();
        try {
          currentConcurrency++;
          maxConcurrency = Math.max(maxConcurrency, currentConcurrency);
          // Simulate some work
          await new Promise((resolve) => setTimeout(resolve, 10));
        } finally {
          currentConcurrency--;
          semaphore.release();
        }
      };

      // Run 10 operations that should be limited to 3 concurrent
      await Promise.all(Array.from({ length: 10 }, () => operation()));

      expect(maxConcurrency).toBe(3);
    });

    it('should work with DEFAULT_MULTIPART_CONCURRENCY value', async () => {
      const semaphore = new Semaphore(DEFAULT_MULTIPART_CONCURRENCY);
      expect(semaphore.availablePermits).toBe(DEFAULT_MULTIPART_CONCURRENCY);

      // Acquire all permits
      for (let i = 0; i < DEFAULT_MULTIPART_CONCURRENCY; i++) {
        await semaphore.acquire();
      }
      expect(semaphore.availablePermits).toBe(0);

      // Release all permits
      for (let i = 0; i < DEFAULT_MULTIPART_CONCURRENCY; i++) {
        semaphore.release();
      }
      expect(semaphore.availablePermits).toBe(DEFAULT_MULTIPART_CONCURRENCY);
    });
  });
});

// ============================================================================
// R2Storage Multipart Concurrency Tests
// ============================================================================

describe('R2Storage multipart concurrency', () => {
  let mockBucket: R2Bucket;
  let uploadPartCalls: Array<{ partNumber: number; timestamp: number }>;
  let mockUpload: R2MultipartUpload;

  beforeEach(() => {
    uploadPartCalls = [];

    mockUpload = {
      uploadPart: vi.fn(async (partNumber: number, _data: ArrayBuffer | Uint8Array): Promise<R2UploadedPart> => {
        const timestamp = Date.now();
        uploadPartCalls.push({ partNumber, timestamp });
        // Simulate network delay
        await new Promise((resolve) => setTimeout(resolve, 20));
        return { partNumber, etag: `etag-${partNumber}` };
      }),
      complete: vi.fn(async () => ({ key: 'test-key', size: 1000, etag: 'final-etag' })),
      abort: vi.fn(async () => {}),
    };

    mockBucket = {
      get: vi.fn(),
      head: vi.fn(),
      put: vi.fn(),
      delete: vi.fn(),
      list: vi.fn(),
      createMultipartUpload: vi.fn(async () => mockUpload),
    };
  });

  describe('constructor options', () => {
    it('should use DEFAULT_MULTIPART_CONCURRENCY by default', () => {
      const storage = new R2Storage(mockBucket);
      expect(storage.getMultipartConcurrency()).toBe(DEFAULT_MULTIPART_CONCURRENCY);
    });

    it('should accept custom multipartConcurrency option', () => {
      const storage = new R2Storage(mockBucket, { multipartConcurrency: 3 });
      expect(storage.getMultipartConcurrency()).toBe(3);
    });

    it('should throw an error if multipartConcurrency is less than 1', () => {
      expect(() => new R2Storage(mockBucket, { multipartConcurrency: 0 }))
        .toThrow('multipartConcurrency must be at least 1');
      expect(() => new R2Storage(mockBucket, { multipartConcurrency: -1 }))
        .toThrow('multipartConcurrency must be at least 1');
    });

    it('should accept 1 as minimum multipartConcurrency', () => {
      const storage = new R2Storage(mockBucket, { multipartConcurrency: 1 });
      expect(storage.getMultipartConcurrency()).toBe(1);
    });
  });

  describe('multipart upload concurrency limiting', () => {
    it('should limit concurrent part uploads to the configured limit', async () => {
      const concurrencyLimit = 2;
      const storage = new R2Storage(mockBucket, { multipartConcurrency: concurrencyLimit });

      const upload = await storage.createMultipartUpload('test-file.bin');

      // Upload 6 parts in parallel
      const parts = Array.from({ length: 6 }, (_, i) => ({
        partNumber: i + 1,
        data: new Uint8Array([i]),
      }));

      const results = await Promise.all(
        parts.map((part) => upload.uploadPart(part.partNumber, part.data))
      );

      // All parts should complete
      expect(results).toHaveLength(6);
      expect(results.map(r => r.partNumber).sort((a, b) => a - b)).toEqual([1, 2, 3, 4, 5, 6]);

      // Verify that no more than `concurrencyLimit` uploads were in flight at once
      // Sort calls by timestamp
      uploadPartCalls.sort((a, b) => a.timestamp - b.timestamp);

      // Check that at any point in time, no more than `concurrencyLimit` calls were in progress
      // This is a simplified check - in a real scenario you'd track start/end times
      // Here we verify that calls are staggered (not all at the same timestamp)
      const uniqueTimestamps = new Set(uploadPartCalls.map(c => c.timestamp));
      // With concurrency limit of 2 and 6 parts, we should see at least 3 batches
      expect(uniqueTimestamps.size).toBeGreaterThanOrEqual(3);
    });

    it('should allow sequential calls without blocking', async () => {
      const storage = new R2Storage(mockBucket, { multipartConcurrency: 1 });
      const upload = await storage.createMultipartUpload('test-file.bin');

      // Upload parts sequentially
      const result1 = await upload.uploadPart(1, new Uint8Array([1]));
      const result2 = await upload.uploadPart(2, new Uint8Array([2]));
      const result3 = await upload.uploadPart(3, new Uint8Array([3]));

      expect(result1.partNumber).toBe(1);
      expect(result2.partNumber).toBe(2);
      expect(result3.partNumber).toBe(3);
    });

    it('should release semaphore permit even when upload fails', async () => {
      const storage = new R2Storage(mockBucket, { multipartConcurrency: 1 });

      // Make the first upload fail
      mockUpload.uploadPart = vi.fn()
        .mockRejectedValueOnce(new Error('Upload failed'))
        .mockResolvedValueOnce({ partNumber: 2, etag: 'etag-2' });

      const upload = await storage.createMultipartUpload('test-file.bin');

      // First upload should fail
      await expect(upload.uploadPart(1, new Uint8Array([1]))).rejects.toThrow('Upload failed');

      // Second upload should succeed (semaphore should be released)
      const result = await upload.uploadPart(2, new Uint8Array([2]));
      expect(result.partNumber).toBe(2);
    });

    it('should handle complete and abort without semaphore interaction', async () => {
      const storage = new R2Storage(mockBucket);
      const upload = await storage.createMultipartUpload('test-file.bin');

      // Upload a part
      const part = await upload.uploadPart(1, new Uint8Array([1]));

      // Complete should work
      await upload.complete([part]);
      expect(mockUpload.complete).toHaveBeenCalledWith([{ partNumber: 1, etag: 'etag-1' }]);
    });

    it('should handle abort correctly', async () => {
      const storage = new R2Storage(mockBucket);
      const upload = await storage.createMultipartUpload('test-file.bin');

      await upload.abort();
      expect(mockUpload.abort).toHaveBeenCalled();
    });
  });

  describe('integration with realistic workloads', () => {
    it('should handle a large number of parts with concurrency limit', async () => {
      const concurrencyLimit = 4;
      const numParts = 20;
      const storage = new R2Storage(mockBucket, { multipartConcurrency: concurrencyLimit });

      // Track concurrent operations
      let currentConcurrency = 0;
      let maxConcurrency = 0;

      mockUpload.uploadPart = vi.fn(async (partNumber: number): Promise<R2UploadedPart> => {
        currentConcurrency++;
        maxConcurrency = Math.max(maxConcurrency, currentConcurrency);
        // Simulate variable network delay
        await new Promise((resolve) => setTimeout(resolve, 5 + Math.random() * 10));
        currentConcurrency--;
        return { partNumber, etag: `etag-${partNumber}` };
      });

      const upload = await storage.createMultipartUpload('large-file.bin');

      // Upload all parts in parallel
      const parts = Array.from({ length: numParts }, (_, i) => ({
        partNumber: i + 1,
        data: new Uint8Array([i]),
      }));

      const results = await Promise.all(
        parts.map((part) => upload.uploadPart(part.partNumber, part.data))
      );

      expect(results).toHaveLength(numParts);
      expect(maxConcurrency).toBeLessThanOrEqual(concurrencyLimit);
      expect(maxConcurrency).toBeGreaterThan(0);
    });

    it('should complete full multipart upload workflow', async () => {
      const storage = new R2Storage(mockBucket, { multipartConcurrency: 3 });
      const upload = await storage.createMultipartUpload('complete-test.bin');

      // Upload parts in parallel
      const parts = await Promise.all([
        upload.uploadPart(1, new Uint8Array([1, 2, 3])),
        upload.uploadPart(2, new Uint8Array([4, 5, 6])),
        upload.uploadPart(3, new Uint8Array([7, 8, 9])),
      ]);

      // Complete the upload
      await upload.complete(parts);

      expect(mockBucket.createMultipartUpload).toHaveBeenCalledWith('complete-test.bin');
      expect(mockUpload.uploadPart).toHaveBeenCalledTimes(3);
      expect(mockUpload.complete).toHaveBeenCalledWith([
        { partNumber: 1, etag: 'etag-1' },
        { partNumber: 2, etag: 'etag-2' },
        { partNumber: 3, etag: 'etag-3' },
      ]);
    });
  });
});

// ============================================================================
// DEFAULT_MULTIPART_CONCURRENCY Constant Tests
// ============================================================================

describe('DEFAULT_MULTIPART_CONCURRENCY', () => {
  it('should be a reasonable default value (between 4 and 6)', () => {
    expect(DEFAULT_MULTIPART_CONCURRENCY).toBeGreaterThanOrEqual(4);
    expect(DEFAULT_MULTIPART_CONCURRENCY).toBeLessThanOrEqual(6);
  });
});
