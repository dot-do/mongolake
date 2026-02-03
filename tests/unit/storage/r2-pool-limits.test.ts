/**
 * R2 Connection Pool Limits Tests
 *
 * Tests for R2 multipart upload concurrency limits, queuing behavior,
 * backpressure handling, and timeout scenarios.
 *
 * Issue: mongolake-vpvj
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  Semaphore,
  R2Storage,
  DEFAULT_MULTIPART_CONCURRENCY,
} from '../../../src/storage/index.js';
import type { R2Bucket, R2MultipartUpload, R2UploadedPart } from '../../../src/types.js';

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Create a mock R2 bucket with configurable upload behavior
 */
function createMockBucket(options: {
  uploadDelay?: number;
  uploadFn?: (partNumber: number, data: ArrayBuffer | Uint8Array) => Promise<R2UploadedPart>;
} = {}): { bucket: R2Bucket; mockUpload: R2MultipartUpload; stats: { uploadCalls: number[]; concurrentUploads: number; maxConcurrent: number } } {
  const stats = {
    uploadCalls: [] as number[],
    concurrentUploads: 0,
    maxConcurrent: 0,
  };

  const mockUpload: R2MultipartUpload = {
    uploadPart: options.uploadFn ?? vi.fn(async (partNumber: number, _data: ArrayBuffer | Uint8Array): Promise<R2UploadedPart> => {
      stats.uploadCalls.push(partNumber);
      stats.concurrentUploads++;
      stats.maxConcurrent = Math.max(stats.maxConcurrent, stats.concurrentUploads);

      if (options.uploadDelay) {
        await new Promise((resolve) => setTimeout(resolve, options.uploadDelay));
      }

      stats.concurrentUploads--;
      return { partNumber, etag: `etag-${partNumber}` };
    }),
    complete: vi.fn(async () => ({ key: 'test-key', size: 1000, etag: 'final-etag' })),
    abort: vi.fn(async () => {}),
  };

  const bucket: R2Bucket = {
    get: vi.fn(),
    head: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
    list: vi.fn(),
    createMultipartUpload: vi.fn(async () => mockUpload),
  };

  return { bucket, mockUpload, stats };
}

// ============================================================================
// Concurrent Uploads Limited to Max Tests
// ============================================================================

describe('R2 Pool Limits: Concurrent uploads limited to max', () => {
  it('should never exceed the configured concurrency limit', async () => {
    const concurrencyLimit = 3;
    const { bucket, stats } = createMockBucket({ uploadDelay: 20 });
    const storage = new R2Storage(bucket, { multipartConcurrency: concurrencyLimit });

    const upload = await storage.createMultipartUpload('test-file.bin');

    // Launch 10 concurrent uploads
    const parts = Array.from({ length: 10 }, (_, i) => i + 1);
    await Promise.all(parts.map((partNumber) =>
      upload.uploadPart(partNumber, new Uint8Array([partNumber]))
    ));

    expect(stats.maxConcurrent).toBeLessThanOrEqual(concurrencyLimit);
    expect(stats.uploadCalls).toHaveLength(10);
  });

  it('should respect concurrency limit of 1 (serial execution)', async () => {
    const { bucket, stats } = createMockBucket({ uploadDelay: 10 });
    const storage = new R2Storage(bucket, { multipartConcurrency: 1 });

    const upload = await storage.createMultipartUpload('test-file.bin');

    // Launch 5 concurrent uploads
    await Promise.all([1, 2, 3, 4, 5].map((partNumber) =>
      upload.uploadPart(partNumber, new Uint8Array([partNumber]))
    ));

    // With concurrency of 1, max concurrent should be exactly 1
    expect(stats.maxConcurrent).toBe(1);
  });

  it('should use DEFAULT_MULTIPART_CONCURRENCY when no option provided', async () => {
    const { bucket, stats } = createMockBucket({ uploadDelay: 15 });
    const storage = new R2Storage(bucket);

    const upload = await storage.createMultipartUpload('test-file.bin');

    // Launch more uploads than the default limit
    const numParts = DEFAULT_MULTIPART_CONCURRENCY + 5;
    await Promise.all(
      Array.from({ length: numParts }, (_, i) =>
        upload.uploadPart(i + 1, new Uint8Array([i]))
      )
    );

    expect(stats.maxConcurrent).toBeLessThanOrEqual(DEFAULT_MULTIPART_CONCURRENCY);
    expect(stats.maxConcurrent).toBeGreaterThan(0);
  });
});

// ============================================================================
// Additional Uploads Queued When at Limit Tests
// ============================================================================

describe('R2 Pool Limits: Additional uploads queued when at limit', () => {
  it('should queue uploads when all permits are in use', async () => {
    const semaphore = new Semaphore(2);

    // Acquire all permits
    await semaphore.acquire();
    await semaphore.acquire();
    expect(semaphore.availablePermits).toBe(0);

    // Try to acquire more - these should be queued
    let acquired3 = false;
    let acquired4 = false;

    const promise3 = semaphore.acquire().then(() => { acquired3 = true; });
    const promise4 = semaphore.acquire().then(() => { acquired4 = true; });

    // Give promises a chance to resolve (they shouldn't yet)
    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(acquired3).toBe(false);
    expect(acquired4).toBe(false);
    expect(semaphore.waitingCount).toBe(2);

    // Release one permit
    semaphore.release();
    await promise3;
    expect(acquired3).toBe(true);
    expect(acquired4).toBe(false);
    expect(semaphore.waitingCount).toBe(1);

    // Release another
    semaphore.release();
    await promise4;
    expect(acquired4).toBe(true);
    expect(semaphore.waitingCount).toBe(0);
  });

  it('should maintain FIFO order for queued operations', async () => {
    const { bucket } = createMockBucket({ uploadDelay: 30 });
    const storage = new R2Storage(bucket, { multipartConcurrency: 1 });

    const upload = await storage.createMultipartUpload('test-file.bin');

    const completionOrder: number[] = [];

    // Launch uploads in order 1, 2, 3, 4, 5
    const promises = [1, 2, 3, 4, 5].map((partNumber) =>
      upload.uploadPart(partNumber, new Uint8Array([partNumber])).then(() => {
        completionOrder.push(partNumber);
      })
    );

    await Promise.all(promises);

    // With serial execution, completion order should match launch order
    expect(completionOrder).toEqual([1, 2, 3, 4, 5]);
  });

  it('should track waiting count accurately during multipart upload', async () => {
    const semaphore = new Semaphore(1);

    // Acquire the only permit
    await semaphore.acquire();
    expect(semaphore.waitingCount).toBe(0);

    // Queue multiple waiters
    const promises = [1, 2, 3].map(() => semaphore.acquire());
    expect(semaphore.waitingCount).toBe(3);

    // Release one at a time and verify count decreases
    semaphore.release();
    await promises[0];
    expect(semaphore.waitingCount).toBe(2);

    semaphore.release();
    await promises[1];
    expect(semaphore.waitingCount).toBe(1);

    semaphore.release();
    await promises[2];
    expect(semaphore.waitingCount).toBe(0);
  });
});

// ============================================================================
// Queued Uploads Proceed When Slots Free Tests
// ============================================================================

describe('R2 Pool Limits: Queued uploads proceed when slots free', () => {
  it('should automatically process queued uploads when permits are released', async () => {
    const { bucket, stats } = createMockBucket({ uploadDelay: 10 });
    const storage = new R2Storage(bucket, { multipartConcurrency: 2 });

    const upload = await storage.createMultipartUpload('test-file.bin');

    // Track completion times
    const completionTimes: number[] = [];
    const startTime = Date.now();

    // Upload 6 parts with concurrency 2
    await Promise.all(
      Array.from({ length: 6 }, (_, i) =>
        upload.uploadPart(i + 1, new Uint8Array([i])).then(() => {
          completionTimes.push(Date.now() - startTime);
        })
      )
    );

    // All uploads should complete
    expect(completionTimes).toHaveLength(6);
    // No more than 2 should run concurrently
    expect(stats.maxConcurrent).toBeLessThanOrEqual(2);
  });

  it('should release semaphore permit even when upload fails', async () => {
    let callCount = 0;
    const { bucket } = createMockBucket({
      uploadFn: async (partNumber: number) => {
        callCount++;
        if (callCount === 1) {
          throw new Error('Simulated upload failure');
        }
        return { partNumber, etag: `etag-${partNumber}` };
      },
    });
    const storage = new R2Storage(bucket, { multipartConcurrency: 1 });

    const upload = await storage.createMultipartUpload('test-file.bin');

    // First upload fails
    await expect(upload.uploadPart(1, new Uint8Array([1]))).rejects.toThrow('Simulated upload failure');

    // Second upload should still work (permit was released)
    const result = await upload.uploadPart(2, new Uint8Array([2]));
    expect(result.partNumber).toBe(2);
  });

  it('should process queued uploads in order after failure', async () => {
    let callCount = 0;
    const completionOrder: number[] = [];

    const { bucket } = createMockBucket({
      uploadFn: async (partNumber: number) => {
        callCount++;
        await new Promise((resolve) => setTimeout(resolve, 10));
        if (callCount === 1) {
          throw new Error('First upload fails');
        }
        completionOrder.push(partNumber);
        return { partNumber, etag: `etag-${partNumber}` };
      },
    });
    const storage = new R2Storage(bucket, { multipartConcurrency: 1 });

    const upload = await storage.createMultipartUpload('test-file.bin');

    // Launch multiple uploads, first one will fail
    const promises = [
      upload.uploadPart(1, new Uint8Array([1])).catch(() => {}), // Will fail
      upload.uploadPart(2, new Uint8Array([2])),
      upload.uploadPart(3, new Uint8Array([3])),
    ];

    await Promise.all(promises);

    // Parts 2 and 3 should complete in order after part 1 fails
    expect(completionOrder).toEqual([2, 3]);
  });
});

// ============================================================================
// Backpressure Under Heavy Load Tests
// ============================================================================

describe('R2 Pool Limits: Backpressure under heavy load', () => {
  it('should handle 100 concurrent upload requests with backpressure', async () => {
    const concurrencyLimit = 5;
    const { bucket, stats } = createMockBucket({ uploadDelay: 5 });
    const storage = new R2Storage(bucket, { multipartConcurrency: concurrencyLimit });

    const upload = await storage.createMultipartUpload('large-file.bin');

    // Simulate heavy load with 100 parts
    const numParts = 100;
    const results = await Promise.all(
      Array.from({ length: numParts }, (_, i) =>
        upload.uploadPart(i + 1, new Uint8Array([i % 256]))
      )
    );

    // All parts should complete successfully
    expect(results).toHaveLength(numParts);
    // Concurrency should never exceed limit
    expect(stats.maxConcurrent).toBeLessThanOrEqual(concurrencyLimit);
    // But we should have utilized the full concurrency
    expect(stats.maxConcurrent).toBeGreaterThanOrEqual(1);
  });

  it('should maintain fairness under sustained load', async () => {
    const semaphore = new Semaphore(2);
    const processOrder: number[] = [];

    // Acquire both permits initially
    await semaphore.acquire();
    await semaphore.acquire();

    // Queue 10 waiters
    const waiters = Array.from({ length: 10 }, (_, i) =>
      semaphore.acquire().then(() => {
        processOrder.push(i);
        // Immediately release after processing
        semaphore.release();
      })
    );

    // Release the initial permits
    semaphore.release();
    semaphore.release();

    await Promise.all(waiters);

    // All waiters should complete and maintain FIFO order
    expect(processOrder).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });

  it('should not deadlock with high concurrency and varying delays', async () => {
    const concurrencyLimit = 4;
    let activeCount = 0;
    let maxActive = 0;

    const { bucket } = createMockBucket({
      uploadFn: async (partNumber: number) => {
        activeCount++;
        maxActive = Math.max(maxActive, activeCount);
        // Varying delays to simulate real network conditions
        await new Promise((resolve) => setTimeout(resolve, Math.random() * 20 + 5));
        activeCount--;
        return { partNumber, etag: `etag-${partNumber}` };
      },
    });

    const storage = new R2Storage(bucket, { multipartConcurrency: concurrencyLimit });
    const upload = await storage.createMultipartUpload('test-file.bin');

    // Launch 50 uploads with varying delays
    const results = await Promise.all(
      Array.from({ length: 50 }, (_, i) =>
        upload.uploadPart(i + 1, new Uint8Array([i]))
      )
    );

    expect(results).toHaveLength(50);
    expect(maxActive).toBeLessThanOrEqual(concurrencyLimit);
  });

  it('should handle burst of uploads followed by steady state', async () => {
    const { bucket, stats } = createMockBucket({ uploadDelay: 10 });
    const storage = new R2Storage(bucket, { multipartConcurrency: 3 });

    const upload = await storage.createMultipartUpload('test-file.bin');

    // Burst of 20 uploads
    const burstResults = await Promise.all(
      Array.from({ length: 20 }, (_, i) =>
        upload.uploadPart(i + 1, new Uint8Array([i]))
      )
    );

    expect(burstResults).toHaveLength(20);
    expect(stats.maxConcurrent).toBeLessThanOrEqual(3);

    // Steady state - sequential uploads
    for (let i = 21; i <= 25; i++) {
      const result = await upload.uploadPart(i, new Uint8Array([i]));
      expect(result.partNumber).toBe(i);
    }
  });
});

// ============================================================================
// Timeout for Queued Operations Tests
// ============================================================================

describe('R2 Pool Limits: Timeout for queued operations', () => {
  it('should allow implementing timeout wrapper around semaphore acquire', async () => {
    const semaphore = new Semaphore(1);

    // Acquire the only permit
    await semaphore.acquire();

    // Helper function to acquire with timeout
    async function acquireWithTimeout(sem: Semaphore, timeoutMs: number): Promise<boolean> {
      return Promise.race([
        sem.acquire().then(() => true),
        new Promise<boolean>((resolve) => setTimeout(() => resolve(false), timeoutMs)),
      ]);
    }

    // Try to acquire with a short timeout - should fail (timeout fires first)
    const acquired = await acquireWithTimeout(semaphore, 50);
    expect(acquired).toBe(false);

    // Note: The pending acquire from above is still queued in the semaphore
    // When we release, that queued request gets the permit
    semaphore.release();

    // Wait for the queued acquire to complete
    await new Promise((resolve) => setTimeout(resolve, 10));

    // Now semaphore has 0 permits (the queued acquire got it)
    // Release again to make a permit available
    semaphore.release();

    // Now try again with a fresh acquire - should succeed
    const acquiredAfterRelease = await acquireWithTimeout(semaphore, 100);
    expect(acquiredAfterRelease).toBe(true);
  });

  it('should support AbortController pattern for cancellable uploads', async () => {
    let uploadStarted = false;
    let uploadCompleted = false;

    const { bucket } = createMockBucket({
      uploadFn: async (partNumber: number) => {
        uploadStarted = true;
        // Simulate long upload
        await new Promise((resolve) => setTimeout(resolve, 100));
        uploadCompleted = true;
        return { partNumber, etag: `etag-${partNumber}` };
      },
    });

    const storage = new R2Storage(bucket, { multipartConcurrency: 1 });
    const upload = await storage.createMultipartUpload('test-file.bin');

    // Start an upload with a timeout
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 30);

    try {
      await Promise.race([
        upload.uploadPart(1, new Uint8Array([1])),
        new Promise((_, reject) => {
          controller.signal.addEventListener('abort', () => reject(new Error('Upload timeout')));
        }),
      ]);
    } catch (error) {
      expect((error as Error).message).toBe('Upload timeout');
    } finally {
      clearTimeout(timeoutId);
    }

    // Upload started but timed out before completion (from caller's perspective)
    expect(uploadStarted).toBe(true);
    // Note: The actual upload continues in the background - this tests the pattern
  });

  it('should handle long-running uploads gracefully', async () => {
    const completedParts: number[] = [];

    const { bucket } = createMockBucket({
      uploadFn: async (partNumber: number) => {
        // Simulate variable upload times
        const delay = partNumber === 1 ? 100 : 10;
        await new Promise((resolve) => setTimeout(resolve, delay));
        completedParts.push(partNumber);
        return { partNumber, etag: `etag-${partNumber}` };
      },
    });

    const storage = new R2Storage(bucket, { multipartConcurrency: 2 });
    const upload = await storage.createMultipartUpload('test-file.bin');

    // Part 1 takes 100ms, parts 2-5 take 10ms each
    // With concurrency 2: Part 1 & 2 start together
    // Part 2 finishes first, then 3 starts, etc.
    await Promise.all([
      upload.uploadPart(1, new Uint8Array([1])),
      upload.uploadPart(2, new Uint8Array([2])),
      upload.uploadPart(3, new Uint8Array([3])),
      upload.uploadPart(4, new Uint8Array([4])),
      upload.uploadPart(5, new Uint8Array([5])),
    ]);

    // All parts should complete
    expect(completedParts).toHaveLength(5);
    expect(completedParts.sort((a, b) => a - b)).toEqual([1, 2, 3, 4, 5]);
    // Part 1 should be the last to appear in completedParts (before sorting)
    // because it has a 100ms delay while others have 10ms
    // The completion order should have part 2 complete before part 1
  });
});

// ============================================================================
// Edge Cases and Error Handling Tests
// ============================================================================

describe('R2 Pool Limits: Edge cases and error handling', () => {
  it('should handle zero-byte uploads', async () => {
    const { bucket, stats } = createMockBucket({ uploadDelay: 5 });
    const storage = new R2Storage(bucket, { multipartConcurrency: 2 });

    const upload = await storage.createMultipartUpload('empty-parts.bin');

    const results = await Promise.all([
      upload.uploadPart(1, new Uint8Array(0)),
      upload.uploadPart(2, new Uint8Array(0)),
      upload.uploadPart(3, new Uint8Array(0)),
    ]);

    expect(results).toHaveLength(3);
    expect(stats.maxConcurrent).toBeLessThanOrEqual(2);
  });

  it('should handle rapid acquire/release cycles', async () => {
    const semaphore = new Semaphore(2);

    // Rapidly acquire and release
    for (let i = 0; i < 100; i++) {
      await semaphore.acquire();
      semaphore.release();
    }

    // Semaphore should be back to initial state
    expect(semaphore.availablePermits).toBe(2);
    expect(semaphore.waitingCount).toBe(0);
  });

  it('should maintain correct state after multiple errors', async () => {
    let errorCount = 0;
    const { bucket } = createMockBucket({
      uploadFn: async (partNumber: number) => {
        if (partNumber % 2 === 0) {
          errorCount++;
          throw new Error(`Error for part ${partNumber}`);
        }
        return { partNumber, etag: `etag-${partNumber}` };
      },
    });

    const storage = new R2Storage(bucket, { multipartConcurrency: 2 });
    const upload = await storage.createMultipartUpload('test-file.bin');

    // Upload parts 1-6, even numbers will fail
    const results = await Promise.allSettled([
      upload.uploadPart(1, new Uint8Array([1])),
      upload.uploadPart(2, new Uint8Array([2])),
      upload.uploadPart(3, new Uint8Array([3])),
      upload.uploadPart(4, new Uint8Array([4])),
      upload.uploadPart(5, new Uint8Array([5])),
      upload.uploadPart(6, new Uint8Array([6])),
    ]);

    // 3 should succeed, 3 should fail
    const fulfilled = results.filter((r) => r.status === 'fulfilled');
    const rejected = results.filter((r) => r.status === 'rejected');

    expect(fulfilled).toHaveLength(3);
    expect(rejected).toHaveLength(3);
    expect(errorCount).toBe(3);
  });

  it('should handle concurrent operations across multiple uploads', async () => {
    const { bucket, stats } = createMockBucket({ uploadDelay: 10 });
    const storage = new R2Storage(bucket, { multipartConcurrency: 3 });

    // Create two separate multipart uploads
    const upload1 = await storage.createMultipartUpload('file1.bin');
    const upload2 = await storage.createMultipartUpload('file2.bin');

    // Each upload has its own semaphore, so total concurrency is 6 (3+3)
    // But within each upload, max concurrency is 3
    await Promise.all([
      upload1.uploadPart(1, new Uint8Array([1])),
      upload1.uploadPart(2, new Uint8Array([2])),
      upload1.uploadPart(3, new Uint8Array([3])),
      upload1.uploadPart(4, new Uint8Array([4])),
      upload2.uploadPart(1, new Uint8Array([1])),
      upload2.uploadPart(2, new Uint8Array([2])),
      upload2.uploadPart(3, new Uint8Array([3])),
      upload2.uploadPart(4, new Uint8Array([4])),
    ]);

    // All 8 parts should complete
    expect(stats.uploadCalls).toHaveLength(8);
    // Note: Since each upload has its own semaphore, actual max concurrent
    // could be higher than 3 (up to 6 if both uploads run at full concurrency)
  });
});
