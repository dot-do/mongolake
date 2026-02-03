/**
 * Storage Failure Recovery Tests
 *
 * Tests for storage layer failure scenarios and recovery mechanisms:
 * - Transient R2 failures with retry
 * - Partial flush failure data integrity
 * - Incomplete multipart upload cleanup
 * - Network timeout mid-upload
 * - Storage quota exceeded recovery
 * - Corrupted file handling
 *
 * Issue: mongolake-ump7
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  R2Storage,
  MemoryStorage,
  type StorageBackend,
  type MultipartUpload,
  type UploadedPart,
} from '../../../src/storage/index.js';
import type { R2Bucket, R2MultipartUpload, R2UploadedPart, R2Object, R2ObjectBody } from '../../../src/types.js';

// ============================================================================
// Mock R2 Bucket with Failure Simulation
// ============================================================================

interface FailureConfig {
  /** Number of times to fail before succeeding */
  failCount?: number;
  /** Error to throw */
  error?: Error;
  /** Delay before response (for timeout simulation) */
  delayMs?: number;
  /** Simulate partial data corruption */
  corruptData?: boolean;
  /** Simulate quota exceeded */
  quotaExceeded?: boolean;
}

interface MockR2BucketOptions {
  getFailures?: FailureConfig;
  putFailures?: FailureConfig;
  deleteFailures?: FailureConfig;
  listFailures?: FailureConfig;
  multipartFailures?: {
    createFailures?: FailureConfig;
    uploadPartFailures?: FailureConfig;
    completeFailures?: FailureConfig;
    abortFailures?: FailureConfig;
  };
}

function createFailableR2Bucket(options: MockR2BucketOptions = {}): R2Bucket & {
  _objects: Map<string, Uint8Array>;
  _incompleteUploads: Map<string, { key: string; parts: Map<number, Uint8Array> }>;
  _callCounts: {
    get: number;
    put: number;
    delete: number;
    list: number;
    createMultipartUpload: number;
    uploadPart: number;
    complete: number;
    abort: number;
  };
  _resetCallCounts: () => void;
} {
  const objects = new Map<string, Uint8Array>();
  const incompleteUploads = new Map<string, { key: string; parts: Map<number, Uint8Array> }>();
  const callCounts = {
    get: 0,
    put: 0,
    delete: 0,
    list: 0,
    createMultipartUpload: 0,
    uploadPart: 0,
    complete: 0,
    abort: 0,
  };

  const shouldFail = (config: FailureConfig | undefined, callCount: number): boolean => {
    if (!config) return false;
    // corruptData doesn't throw errors, it just corrupts the response
    if (config.corruptData && !config.error && !config.quotaExceeded) {
      return false;
    }
    return callCount <= (config.failCount ?? 1);
  };

  const shouldCorruptData = (config: FailureConfig | undefined, callCount: number): boolean => {
    if (!config?.corruptData) return false;
    return callCount <= (config.failCount ?? 1);
  };

  const applyFailure = async (config: FailureConfig | undefined, callCount: number): Promise<void> => {
    if (!shouldFail(config, callCount)) return;

    if (config?.delayMs) {
      await new Promise((resolve) => setTimeout(resolve, config.delayMs));
    }

    if (config?.quotaExceeded) {
      throw new Error('Storage quota exceeded');
    }

    if (config?.error) {
      throw config.error;
    }

    throw new Error('Simulated transient failure');
  };

  const bucket: R2Bucket & {
    _objects: Map<string, Uint8Array>;
    _incompleteUploads: Map<string, { key: string; parts: Map<number, Uint8Array> }>;
    _callCounts: typeof callCounts;
    _resetCallCounts: () => void;
  } = {
    _objects: objects,
    _incompleteUploads: incompleteUploads,
    _callCounts: callCounts,
    _resetCallCounts: () => {
      callCounts.get = 0;
      callCounts.put = 0;
      callCounts.delete = 0;
      callCounts.list = 0;
      callCounts.createMultipartUpload = 0;
      callCounts.uploadPart = 0;
      callCounts.complete = 0;
      callCounts.abort = 0;
    },

    get: vi.fn(async (key: string) => {
      callCounts.get++;
      await applyFailure(options.getFailures, callCounts.get);

      const data = objects.get(key);
      if (!data) return null;

      let resultData = data;
      if (shouldCorruptData(options.getFailures, callCounts.get)) {
        // Return corrupted data
        resultData = new Uint8Array([0xff, 0xfe, 0xfd, ...Array(Math.max(0, data.length - 3)).fill(0)]);
      }

      return {
        arrayBuffer: async () => resultData.buffer.slice(resultData.byteOffset, resultData.byteOffset + resultData.byteLength),
        text: async () => new TextDecoder().decode(resultData),
        json: async () => JSON.parse(new TextDecoder().decode(resultData)),
        body: new ReadableStream(),
        etag: `etag-${key}`,
        key,
        size: resultData.length,
      } as R2ObjectBody;
    }),

    put: vi.fn(async (key: string, value: ArrayBuffer | Uint8Array | string) => {
      callCounts.put++;
      await applyFailure(options.putFailures, callCounts.put);

      const data = value instanceof Uint8Array
        ? value
        : typeof value === 'string'
          ? new TextEncoder().encode(value)
          : new Uint8Array(value);
      objects.set(key, data);
      return { key, size: data.length, etag: `etag-${key}` };
    }),

    delete: vi.fn(async (key: string | string[]) => {
      callCounts.delete++;
      await applyFailure(options.deleteFailures, callCounts.delete);

      const keys = Array.isArray(key) ? key : [key];
      for (const k of keys) {
        objects.delete(k);
      }
    }),

    list: vi.fn(async (listOptions?: { prefix?: string; limit?: number; cursor?: string }) => {
      callCounts.list++;
      await applyFailure(options.listFailures, callCounts.list);

      const result: Array<{ key: string; size: number; etag: string }> = [];
      for (const [key, data] of objects) {
        if (!listOptions?.prefix || key.startsWith(listOptions.prefix)) {
          result.push({ key, size: data.length, etag: `etag-${key}` });
        }
      }
      return { objects: result, truncated: false };
    }),

    head: vi.fn(async (key: string) => {
      const data = objects.get(key);
      if (!data) return null;
      return { key, size: data.length, etag: `etag-${key}` } as R2Object;
    }),

    createMultipartUpload: vi.fn(async (key: string) => {
      callCounts.createMultipartUpload++;
      await applyFailure(options.multipartFailures?.createFailures, callCounts.createMultipartUpload);

      const uploadId = `upload-${Date.now()}-${Math.random().toString(36).slice(2)}`;
      incompleteUploads.set(uploadId, { key, parts: new Map() });

      const upload: R2MultipartUpload = {
        uploadPart: vi.fn(async (partNumber: number, data: ArrayBuffer | Uint8Array): Promise<R2UploadedPart> => {
          callCounts.uploadPart++;
          await applyFailure(options.multipartFailures?.uploadPartFailures, callCounts.uploadPart);

          const uploadData = incompleteUploads.get(uploadId);
          if (!uploadData) {
            throw new Error('Upload not found');
          }

          const partData = data instanceof Uint8Array ? data : new Uint8Array(data);
          uploadData.parts.set(partNumber, partData);

          return { partNumber, etag: `etag-part-${partNumber}` };
        }),

        complete: vi.fn(async (uploadedParts: R2UploadedPart[]) => {
          callCounts.complete++;
          await applyFailure(options.multipartFailures?.completeFailures, callCounts.complete);

          const uploadData = incompleteUploads.get(uploadId);
          if (!uploadData) {
            throw new Error('Upload not found');
          }

          // Combine parts in order
          const sortedParts = uploadedParts.sort((a, b) => a.partNumber - b.partNumber);
          const totalSize = sortedParts.reduce((sum, p) => {
            const part = uploadData.parts.get(p.partNumber);
            return sum + (part?.length ?? 0);
          }, 0);

          const combined = new Uint8Array(totalSize);
          let offset = 0;
          for (const p of sortedParts) {
            const part = uploadData.parts.get(p.partNumber);
            if (part) {
              combined.set(part, offset);
              offset += part.length;
            }
          }

          objects.set(uploadData.key, combined);
          incompleteUploads.delete(uploadId);

          return { key: uploadData.key, size: totalSize, etag: `etag-${uploadData.key}` };
        }),

        abort: vi.fn(async () => {
          callCounts.abort++;
          await applyFailure(options.multipartFailures?.abortFailures, callCounts.abort);

          incompleteUploads.delete(uploadId);
        }),
      };

      return upload;
    }),

    resumeMultipartUpload: vi.fn(),
  } as unknown as R2Bucket & {
    _objects: Map<string, Uint8Array>;
    _incompleteUploads: Map<string, { key: string; parts: Map<number, Uint8Array> }>;
    _callCounts: typeof callCounts;
    _resetCallCounts: () => void;
  };

  return bucket;
}

// ============================================================================
// Retry Helper with Exponential Backoff
// ============================================================================

interface RetryOptions {
  maxRetries: number;
  initialDelayMs: number;
  maxDelayMs: number;
  backoffMultiplier: number;
}

async function withRetry<T>(
  operation: () => Promise<T>,
  options: RetryOptions = {
    maxRetries: 3,
    initialDelayMs: 10,
    maxDelayMs: 100,
    backoffMultiplier: 2,
  }
): Promise<T> {
  let lastError: Error | undefined;
  let delay = options.initialDelayMs;

  for (let attempt = 0; attempt <= options.maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error as Error;

      if (attempt === options.maxRetries) {
        break;
      }

      // Check if error is retryable (transient)
      const isRetryable =
        error instanceof Error &&
        (error.message.includes('transient') ||
          error.message.includes('timeout') ||
          error.message.includes('network') ||
          error.message.includes('ECONNRESET') ||
          error.message.includes('503'));

      if (!isRetryable) {
        throw error;
      }

      await new Promise((resolve) => setTimeout(resolve, delay));
      delay = Math.min(delay * options.backoffMultiplier, options.maxDelayMs);
    }
  }

  throw lastError;
}

// ============================================================================
// Test: Retry on Transient R2 Failures
// ============================================================================

describe('Storage Failure Recovery - Transient R2 Failures', () => {
  it('should retry on transient R2 failures', async () => {
    const bucket = createFailableR2Bucket({
      putFailures: {
        failCount: 2,
        error: new Error('Simulated transient failure'),
      },
    });
    const storage = new R2Storage(bucket);

    const testData = new Uint8Array([1, 2, 3, 4, 5]);
    const key = 'retry-test/data.bin';

    // Direct call should fail
    await expect(storage.put(key, testData)).rejects.toThrow('transient');

    // Reset and try with retry wrapper
    bucket._resetCallCounts();

    await withRetry(
      () => storage.put(key, testData),
      { maxRetries: 3, initialDelayMs: 5, maxDelayMs: 50, backoffMultiplier: 2 }
    );

    // Verify data was eventually written
    const retrieved = await storage.get(key);
    expect(retrieved).not.toBeNull();
    expect(Array.from(retrieved!)).toEqual([1, 2, 3, 4, 5]);

    // Should have attempted multiple times
    expect(bucket._callCounts.put).toBeGreaterThan(1);
  });

  it('should eventually succeed after transient get failures', async () => {
    const bucket = createFailableR2Bucket({
      getFailures: {
        failCount: 2,
        error: new Error('Simulated transient network error'),
      },
    });
    const storage = new R2Storage(bucket);

    // Pre-populate data
    const testData = new Uint8Array([10, 20, 30]);
    bucket._objects.set('test-key', testData);

    // With retry, should eventually succeed
    const result = await withRetry(
      () => storage.get('test-key'),
      { maxRetries: 3, initialDelayMs: 5, maxDelayMs: 50, backoffMultiplier: 2 }
    );

    expect(result).not.toBeNull();
    expect(Array.from(result!)).toEqual([10, 20, 30]);
    expect(bucket._callCounts.get).toBe(3); // 2 failures + 1 success
  });

  it('should give up after max retries exceeded', async () => {
    const bucket = createFailableR2Bucket({
      putFailures: {
        failCount: 10, // Always fail
        error: new Error('Simulated transient failure'),
      },
    });
    const storage = new R2Storage(bucket);

    await expect(
      withRetry(
        () => storage.put('key', new Uint8Array([1])),
        { maxRetries: 3, initialDelayMs: 5, maxDelayMs: 50, backoffMultiplier: 2 }
      )
    ).rejects.toThrow('transient');

    // Should have attempted 4 times (1 initial + 3 retries)
    expect(bucket._callCounts.put).toBe(4);
  });
});

// ============================================================================
// Test: Data Integrity on Partial Flush Failure
// ============================================================================

describe('Storage Failure Recovery - Partial Flush Failure', () => {
  it('should not lose data on partial flush failure', async () => {
    // Scenario: Write multiple documents, some succeed, some fail
    // Recovery: Track which writes succeeded, retry failed ones

    const bucket = createFailableR2Bucket({
      putFailures: {
        failCount: 1, // First put fails
        error: new Error('Partial flush failure'),
      },
    });
    const storage = new R2Storage(bucket);

    const documents = [
      { key: 'doc1.json', data: new TextEncoder().encode('{"id": 1}') },
      { key: 'doc2.json', data: new TextEncoder().encode('{"id": 2}') },
      { key: 'doc3.json', data: new TextEncoder().encode('{"id": 3}') },
    ];

    const successfulWrites: string[] = [];
    const failedWrites: Array<{ key: string; data: Uint8Array }> = [];

    // Attempt to write all documents
    for (const doc of documents) {
      try {
        await storage.put(doc.key, doc.data);
        successfulWrites.push(doc.key);
      } catch {
        failedWrites.push(doc);
      }
    }

    // First write failed, others succeeded
    expect(failedWrites).toHaveLength(1);
    expect(failedWrites[0].key).toBe('doc1.json');
    expect(successfulWrites).toHaveLength(2);

    // Retry failed writes
    for (const doc of failedWrites) {
      await storage.put(doc.key, doc.data);
      successfulWrites.push(doc.key);
    }

    // Verify all data is present
    expect(successfulWrites).toHaveLength(3);

    for (const doc of documents) {
      const retrieved = await storage.get(doc.key);
      expect(retrieved).not.toBeNull();
      expect(new TextDecoder().decode(retrieved!)).toBe(new TextDecoder().decode(doc.data));
    }
  });

  it('should maintain consistency with write-ahead log pattern', async () => {
    // Simulate WAL: write to log first, then flush to storage
    const bucket = createFailableR2Bucket({
      putFailures: {
        failCount: 1,
        error: new Error('Flush failed'),
      },
    });
    const storage = new R2Storage(bucket);

    // WAL entries (simulated in memory for this test)
    const walEntries: Array<{ lsn: number; key: string; data: Uint8Array; flushed: boolean }> = [
      { lsn: 1, key: 'entry1', data: new Uint8Array([1]), flushed: false },
      { lsn: 2, key: 'entry2', data: new Uint8Array([2]), flushed: false },
    ];

    // Attempt to flush WAL entries
    for (const entry of walEntries) {
      try {
        await storage.put(entry.key, entry.data);
        entry.flushed = true;
      } catch {
        // Entry not flushed, will retry
      }
    }

    // First entry failed to flush
    expect(walEntries[0].flushed).toBe(false);
    expect(walEntries[1].flushed).toBe(true);

    // Retry unflushed entries (recovery)
    const unflushed = walEntries.filter((e) => !e.flushed);
    for (const entry of unflushed) {
      await storage.put(entry.key, entry.data);
      entry.flushed = true;
    }

    // All entries should now be flushed
    expect(walEntries.every((e) => e.flushed)).toBe(true);

    // Verify data integrity
    const data1 = await storage.get('entry1');
    const data2 = await storage.get('entry2');
    expect(Array.from(data1!)).toEqual([1]);
    expect(Array.from(data2!)).toEqual([2]);
  });
});

// ============================================================================
// Test: Incomplete Multipart Upload Cleanup
// ============================================================================

describe('Storage Failure Recovery - Incomplete Multipart Upload Cleanup', () => {
  it('should clean up incomplete multipart uploads', async () => {
    const bucket = createFailableR2Bucket({
      multipartFailures: {
        completeFailures: {
          failCount: 1,
          error: new Error('Complete failed - network error'),
        },
      },
    });
    const storage = new R2Storage(bucket);

    const upload = await storage.createMultipartUpload('large-file.bin');

    // Upload some parts
    const part1 = await upload.uploadPart(1, new Uint8Array([1, 2, 3]));
    const part2 = await upload.uploadPart(2, new Uint8Array([4, 5, 6]));

    // Complete fails
    await expect(upload.complete([part1, part2])).rejects.toThrow('Complete failed');

    // Incomplete upload should exist
    expect(bucket._incompleteUploads.size).toBe(1);

    // Cleanup: abort the incomplete upload
    await upload.abort();

    // Incomplete upload should be cleaned up
    expect(bucket._incompleteUploads.size).toBe(0);

    // File should not exist in storage
    const retrieved = await storage.get('large-file.bin');
    expect(retrieved).toBeNull();
  });

  it('should list and clean up stale incomplete uploads', async () => {
    const bucket = createFailableR2Bucket();

    // Create multiple incomplete uploads
    const upload1 = await bucket.createMultipartUpload('file1.bin');
    const upload2 = await bucket.createMultipartUpload('file2.bin');
    const upload3 = await bucket.createMultipartUpload('file3.bin');

    // Upload parts but don't complete
    await upload1.uploadPart(1, new Uint8Array([1]));
    await upload2.uploadPart(1, new Uint8Array([2]));
    await upload3.uploadPart(1, new Uint8Array([3]));

    expect(bucket._incompleteUploads.size).toBe(3);

    // Cleanup stale uploads (simulating a cleanup routine)
    const incompleteUploadIds = Array.from(bucket._incompleteUploads.keys());
    for (const _uploadId of incompleteUploadIds) {
      // In a real scenario, we'd check age and abort old ones
      // Here we just abort all for testing
    }

    // Abort all incomplete uploads
    await upload1.abort();
    await upload2.abort();
    await upload3.abort();

    expect(bucket._incompleteUploads.size).toBe(0);
  });

  it('should handle abort failure gracefully during cleanup', async () => {
    const bucket = createFailableR2Bucket({
      multipartFailures: {
        abortFailures: {
          failCount: 1,
          error: new Error('Abort failed'),
        },
      },
    });
    const storage = new R2Storage(bucket);

    const upload = await storage.createMultipartUpload('test.bin');
    await upload.uploadPart(1, new Uint8Array([1]));

    // First abort fails
    await expect(upload.abort()).rejects.toThrow('Abort failed');

    // Retry abort should succeed
    await upload.abort();

    // Verify cleanup
    expect(bucket._incompleteUploads.size).toBe(0);
  });
});

// ============================================================================
// Test: Network Timeout Mid-Upload
// ============================================================================

describe('Storage Failure Recovery - Network Timeout Mid-Upload', () => {
  it('should handle network timeout mid-upload', async () => {
    const bucket = createFailableR2Bucket({
      multipartFailures: {
        uploadPartFailures: {
          failCount: 1,
          delayMs: 100,
          error: new Error('Network timeout during upload'),
        },
      },
    });
    const storage = new R2Storage(bucket);

    const upload = await storage.createMultipartUpload('timeout-test.bin');
    const parts: UploadedPart[] = [];

    // Part 1 should fail with timeout
    await expect(upload.uploadPart(1, new Uint8Array([1, 2, 3]))).rejects.toThrow('timeout');

    // Retry part 1 - should succeed now
    const part1 = await upload.uploadPart(1, new Uint8Array([1, 2, 3]));
    parts.push(part1);

    // Part 2 should succeed
    const part2 = await upload.uploadPart(2, new Uint8Array([4, 5, 6]));
    parts.push(part2);

    // Complete should succeed
    await upload.complete(parts);

    // Verify data
    const retrieved = await storage.get('timeout-test.bin');
    expect(retrieved).not.toBeNull();
    expect(Array.from(retrieved!)).toEqual([1, 2, 3, 4, 5, 6]);
  });

  it('should track upload progress and resume after timeout', async () => {
    const bucket = createFailableR2Bucket({
      multipartFailures: {
        uploadPartFailures: {
          failCount: 1,
          error: new Error('Network timeout'),
        },
      },
    });
    const storage = new R2Storage(bucket);

    const upload = await storage.createMultipartUpload('resume-test.bin');
    const completedParts: Map<number, UploadedPart> = new Map();

    const partsToUpload = [
      { partNumber: 1, data: new Uint8Array([1, 2]) },
      { partNumber: 2, data: new Uint8Array([3, 4]) },
      { partNumber: 3, data: new Uint8Array([5, 6]) },
    ];

    // Attempt uploads, tracking progress
    for (const part of partsToUpload) {
      if (completedParts.has(part.partNumber)) {
        continue; // Skip already completed parts
      }

      try {
        const result = await upload.uploadPart(part.partNumber, part.data);
        completedParts.set(part.partNumber, result);
      } catch {
        // Part 1 fails on first attempt
      }
    }

    // Part 1 failed, parts 2 and 3 succeeded
    expect(completedParts.has(1)).toBe(false);
    expect(completedParts.has(2)).toBe(true);
    expect(completedParts.has(3)).toBe(true);

    // Retry failed parts
    for (const part of partsToUpload) {
      if (!completedParts.has(part.partNumber)) {
        const result = await upload.uploadPart(part.partNumber, part.data);
        completedParts.set(part.partNumber, result);
      }
    }

    // All parts should be completed
    expect(completedParts.size).toBe(3);

    // Complete upload
    const allParts = Array.from(completedParts.values());
    await upload.complete(allParts);

    const retrieved = await storage.get('resume-test.bin');
    expect(Array.from(retrieved!)).toEqual([1, 2, 3, 4, 5, 6]);
  });
});

// ============================================================================
// Test: Storage Quota Exceeded Recovery
// ============================================================================

describe('Storage Failure Recovery - Storage Quota Exceeded', () => {
  it('should recover from storage quota exceeded', async () => {
    const bucket = createFailableR2Bucket({
      putFailures: {
        failCount: 1,
        quotaExceeded: true,
      },
    });
    const storage = new R2Storage(bucket);

    const testData = new Uint8Array([1, 2, 3, 4, 5]);

    // First write fails due to quota
    await expect(storage.put('quota-test.bin', testData)).rejects.toThrow('quota exceeded');

    // Simulate: free up space by deleting old files
    bucket._objects.set('old-file-1.bin', new Uint8Array(1000));
    bucket._objects.set('old-file-2.bin', new Uint8Array(1000));

    // Delete old files to free space
    await storage.delete('old-file-1.bin');
    await storage.delete('old-file-2.bin');

    // Retry write - should succeed now (quota failure was only for first attempt)
    await storage.put('quota-test.bin', testData);

    const retrieved = await storage.get('quota-test.bin');
    expect(retrieved).not.toBeNull();
    expect(Array.from(retrieved!)).toEqual([1, 2, 3, 4, 5]);
  });

  it('should implement LRU eviction strategy on quota exceeded', async () => {
    const bucket = createFailableR2Bucket({
      putFailures: {
        failCount: 1,
        quotaExceeded: true,
      },
    });
    const storage = new R2Storage(bucket);

    // Simulate existing files with access timestamps
    const existingFiles = [
      { key: 'old-file.bin', data: new Uint8Array(100), lastAccess: Date.now() - 100000 },
      { key: 'recent-file.bin', data: new Uint8Array(100), lastAccess: Date.now() - 1000 },
      { key: 'newest-file.bin', data: new Uint8Array(100), lastAccess: Date.now() },
    ];

    for (const file of existingFiles) {
      bucket._objects.set(file.key, file.data);
    }

    // Attempt write - fails due to quota
    await expect(storage.put('new-data.bin', new Uint8Array([1, 2, 3]))).rejects.toThrow('quota');

    // LRU eviction: delete oldest accessed file
    const fileToEvict = existingFiles.sort((a, b) => a.lastAccess - b.lastAccess)[0];
    await storage.delete(fileToEvict.key);

    expect(bucket._objects.has('old-file.bin')).toBe(false);
    expect(bucket._objects.has('recent-file.bin')).toBe(true);
    expect(bucket._objects.has('newest-file.bin')).toBe(true);

    // Retry write - should succeed now
    await storage.put('new-data.bin', new Uint8Array([1, 2, 3]));

    const retrieved = await storage.get('new-data.bin');
    expect(Array.from(retrieved!)).toEqual([1, 2, 3]);
  });

  it('should report quota status for monitoring', async () => {
    const bucket = createFailableR2Bucket({
      putFailures: {
        failCount: 2,
        quotaExceeded: true,
      },
    });
    const storage = new R2Storage(bucket);

    let quotaExceededCount = 0;

    // Attempt multiple writes, tracking quota errors
    for (let i = 0; i < 3; i++) {
      try {
        await storage.put(`file-${i}.bin`, new Uint8Array([i]));
      } catch (error) {
        if (error instanceof Error && error.message.includes('quota')) {
          quotaExceededCount++;
        }
      }
    }

    // First two attempts failed with quota exceeded
    expect(quotaExceededCount).toBe(2);

    // Third attempt succeeded (quota failure was only for first 2 attempts)
    const retrieved = await storage.get('file-2.bin');
    expect(retrieved).not.toBeNull();
  });
});

// ============================================================================
// Test: Corrupted File Handling
// ============================================================================

describe('Storage Failure Recovery - Corrupted File Handling', () => {
  it('should handle corrupted file gracefully', async () => {
    const bucket = createFailableR2Bucket({
      getFailures: {
        corruptData: true,
      },
    });
    const storage = new R2Storage(bucket);

    // Store valid JSON data
    const validData = new TextEncoder().encode('{"id": 123, "name": "test"}');
    bucket._objects.set('data.json', validData);

    // First read returns corrupted data
    const corrupted = await storage.get('data.json');
    expect(corrupted).not.toBeNull();

    // Attempt to parse as JSON - should fail
    let parseError: Error | null = null;
    try {
      JSON.parse(new TextDecoder().decode(corrupted!));
    } catch (error) {
      parseError = error as Error;
    }
    expect(parseError).not.toBeNull();

    // Second read returns valid data (corruption was only on first read)
    const valid = await storage.get('data.json');
    expect(valid).not.toBeNull();

    const parsed = JSON.parse(new TextDecoder().decode(valid!));
    expect(parsed).toEqual({ id: 123, name: 'test' });
  });

  it('should detect corruption via checksum validation', async () => {
    const bucket = createFailableR2Bucket();
    const storage = new R2Storage(bucket);

    // Helper to compute simple checksum
    const computeChecksum = (data: Uint8Array): number => {
      return data.reduce((sum, byte) => (sum + byte) & 0xffffffff, 0);
    };

    // Store data with checksum
    const originalData = new Uint8Array([1, 2, 3, 4, 5]);
    const checksum = computeChecksum(originalData);

    await storage.put('data.bin', originalData);
    await storage.put('data.bin.checksum', new TextEncoder().encode(checksum.toString()));

    // Read and validate
    const retrievedData = await storage.get('data.bin');
    const retrievedChecksumStr = await storage.get('data.bin.checksum');

    expect(retrievedData).not.toBeNull();
    expect(retrievedChecksumStr).not.toBeNull();

    const expectedChecksum = parseInt(new TextDecoder().decode(retrievedChecksumStr!), 10);
    const actualChecksum = computeChecksum(retrievedData!);

    expect(actualChecksum).toBe(expectedChecksum);
  });

  it('should attempt recovery from backup on corruption', async () => {
    const bucket = createFailableR2Bucket({
      getFailures: {
        corruptData: true,
      },
    });
    const storage = new R2Storage(bucket);

    // Store primary and backup data
    const validData = new Uint8Array([10, 20, 30]);
    bucket._objects.set('data.bin', validData);
    bucket._objects.set('data.bin.backup', validData);

    // First read from primary is corrupted
    const primary = await storage.get('data.bin');
    const isCorrupted = primary && primary[0] === 0xff; // Check for corruption marker

    if (isCorrupted) {
      // Fall back to backup
      const backup = await storage.get('data.bin.backup');
      expect(backup).not.toBeNull();
      expect(Array.from(backup!)).toEqual([10, 20, 30]);

      // Restore primary from backup
      await storage.put('data.bin', backup!);

      // Verify restoration
      const restored = await storage.get('data.bin');
      expect(Array.from(restored!)).toEqual([10, 20, 30]);
    }
  });

  it('should handle partial file corruption detection', async () => {
    const bucket = createFailableR2Bucket();
    const storage = new R2Storage(bucket);

    // Store a multi-chunk file with per-chunk checksums
    const chunk1 = new Uint8Array([1, 2, 3]);
    const chunk2 = new Uint8Array([4, 5, 6]);
    const chunk3 = new Uint8Array([7, 8, 9]);

    // Store chunks
    await storage.put('file/chunk-0', chunk1);
    await storage.put('file/chunk-1', chunk2);
    await storage.put('file/chunk-2', chunk3);

    // Store manifest with chunk checksums
    const manifest = {
      chunks: [
        { id: 0, checksum: chunk1.reduce((a, b) => a + b, 0) },
        { id: 1, checksum: chunk2.reduce((a, b) => a + b, 0) },
        { id: 2, checksum: chunk3.reduce((a, b) => a + b, 0) },
      ],
    };
    await storage.put('file/manifest.json', new TextEncoder().encode(JSON.stringify(manifest)));

    // Validate all chunks
    const manifestData = await storage.get('file/manifest.json');
    const parsedManifest = JSON.parse(new TextDecoder().decode(manifestData!));

    const corruptedChunks: number[] = [];
    for (const chunkInfo of parsedManifest.chunks) {
      const chunkData = await storage.get(`file/chunk-${chunkInfo.id}`);
      if (chunkData) {
        const actualChecksum = chunkData.reduce((a: number, b: number) => a + b, 0);
        if (actualChecksum !== chunkInfo.checksum) {
          corruptedChunks.push(chunkInfo.id);
        }
      }
    }

    expect(corruptedChunks).toHaveLength(0);
  });
});

// ============================================================================
// Integration Test: Full Recovery Workflow
// ============================================================================

describe('Storage Failure Recovery - Integration', () => {
  it('should handle complex failure scenario with full recovery', async () => {
    // Scenario: Multiple failures during a batch write operation
    const bucket = createFailableR2Bucket({
      putFailures: {
        failCount: 2,
        error: new Error('Simulated transient failure'),
      },
    });
    const storage = new R2Storage(bucket);

    const documents = Array.from({ length: 5 }, (_, i) => ({
      key: `doc-${i}.json`,
      data: new TextEncoder().encode(JSON.stringify({ id: i, timestamp: Date.now() })),
    }));

    const results = {
      succeeded: [] as string[],
      failed: [] as { key: string; data: Uint8Array; error: string }[],
    };

    // First pass: attempt all writes
    for (const doc of documents) {
      try {
        await storage.put(doc.key, doc.data);
        results.succeeded.push(doc.key);
      } catch (error) {
        results.failed.push({
          key: doc.key,
          data: doc.data,
          error: (error as Error).message,
        });
      }
    }

    // First two writes failed
    expect(results.failed).toHaveLength(2);
    expect(results.succeeded).toHaveLength(3);

    // Retry failed writes
    const retryResults = {
      succeeded: [] as string[],
      failed: [] as string[],
    };

    for (const failed of results.failed) {
      try {
        await storage.put(failed.key, failed.data);
        retryResults.succeeded.push(failed.key);
      } catch {
        retryResults.failed.push(failed.key);
      }
    }

    // All retries should succeed (failures were transient)
    expect(retryResults.succeeded).toHaveLength(2);
    expect(retryResults.failed).toHaveLength(0);

    // Verify all documents are stored
    for (const doc of documents) {
      const retrieved = await storage.get(doc.key);
      expect(retrieved).not.toBeNull();
      expect(new TextDecoder().decode(retrieved!)).toBe(new TextDecoder().decode(doc.data));
    }
  });
});
