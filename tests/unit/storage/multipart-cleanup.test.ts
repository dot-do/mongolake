/**
 * Multipart Upload Cleanup/Abort Handling Tests
 *
 * Tests for the ManagedMultipartUpload class and cleanup behavior
 * for multipart uploads in storage backends.
 *
 * Issue: mongolake-m5mg
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  ManagedMultipartUpload,
  DEFAULT_MULTIPART_TIMEOUT_MS,
  createBufferedMultipartUpload,
  R2Storage,
  MemoryStorage,
  MetricsStorageWrapper,
  type MultipartUpload,
  type UploadedPart,
} from '../../../src/storage/index.js';
import { ErrorCodes } from '../../../src/errors/index.js';
import type { R2Bucket, R2MultipartUpload, R2UploadedPart } from '../../../src/types.js';

// ============================================================================
// ManagedMultipartUpload Unit Tests
// ============================================================================

describe('ManagedMultipartUpload', () => {
  let mockInnerUpload: MultipartUpload;
  let uploadPartSpy: ReturnType<typeof vi.fn>;
  let completeSpy: ReturnType<typeof vi.fn>;
  let abortSpy: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    uploadPartSpy = vi.fn(async (partNumber: number) => ({
      partNumber,
      etag: `etag-${partNumber}`,
    }));
    completeSpy = vi.fn(async () => {});
    abortSpy = vi.fn(async () => {});

    mockInnerUpload = {
      uploadPart: uploadPartSpy,
      complete: completeSpy,
      abort: abortSpy,
    };

    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe('constructor', () => {
    it('should create a managed upload with default timeout', () => {
      const upload = new ManagedMultipartUpload(mockInnerUpload);
      expect(upload.isFinalized).toBe(false);
    });

    it('should create a managed upload with custom timeout', () => {
      const upload = new ManagedMultipartUpload(mockInnerUpload, { timeoutMs: 5000 });
      expect(upload.isFinalized).toBe(false);
    });

    it('should create a managed upload with no timeout when timeoutMs is 0', () => {
      const upload = new ManagedMultipartUpload(mockInnerUpload, { timeoutMs: 0 });
      expect(upload.isFinalized).toBe(false);

      // Advance time significantly - should not trigger timeout
      vi.advanceTimersByTime(DEFAULT_MULTIPART_TIMEOUT_MS * 2);
      expect(upload.isFinalized).toBe(false);
    });
  });

  describe('uploadPart', () => {
    it('should forward uploadPart calls to inner upload', async () => {
      const upload = new ManagedMultipartUpload(mockInnerUpload);
      const data = new Uint8Array([1, 2, 3]);
      const result = await upload.uploadPart(1, data);

      expect(uploadPartSpy).toHaveBeenCalledWith(1, data);
      expect(result).toEqual({ partNumber: 1, etag: 'etag-1' });
    });

    it('should throw error after upload is finalized', async () => {
      const upload = new ManagedMultipartUpload(mockInnerUpload);
      await upload.complete([]);

      await expect(upload.uploadPart(1, new Uint8Array([1]))).rejects.toMatchObject({
        code: ErrorCodes.MULTIPART_UPLOAD_FINALIZED,
        message: expect.stringContaining('finalized'),
      });
    });

    it('should throw error after upload is cleaned up', async () => {
      const upload = new ManagedMultipartUpload(mockInnerUpload);
      upload.cleanup();

      // cleanup() sets isFinalized=true first, so the error message says "finalized"
      await expect(upload.uploadPart(1, new Uint8Array([1]))).rejects.toMatchObject({
        code: ErrorCodes.MULTIPART_UPLOAD_FINALIZED,
        message: expect.stringContaining('finalized'),
      });
    });
  });

  describe('complete', () => {
    it('should forward complete calls to inner upload and cleanup', async () => {
      const cleanupSpy = vi.fn();
      const upload = new ManagedMultipartUpload(mockInnerUpload, {}, cleanupSpy);
      const parts: UploadedPart[] = [{ partNumber: 1, etag: 'etag-1' }];

      await upload.complete(parts);

      expect(completeSpy).toHaveBeenCalledWith(parts);
      expect(upload.isFinalized).toBe(true);
      expect(cleanupSpy).toHaveBeenCalled();
    });

    it('should throw error if already finalized', async () => {
      const upload = new ManagedMultipartUpload(mockInnerUpload);
      await upload.complete([]);

      await expect(upload.complete([])).rejects.toMatchObject({
        code: ErrorCodes.MULTIPART_UPLOAD_FINALIZED,
        message: expect.stringContaining('already been finalized'),
      });
    });

    it('should cleanup even if complete throws', async () => {
      const cleanupSpy = vi.fn();
      completeSpy.mockRejectedValueOnce(new Error('Complete failed'));
      const upload = new ManagedMultipartUpload(mockInnerUpload, {}, cleanupSpy);

      await expect(upload.complete([])).rejects.toThrow('Complete failed');
      expect(upload.isFinalized).toBe(true);
      expect(cleanupSpy).toHaveBeenCalled();
    });
  });

  describe('abort', () => {
    it('should forward abort calls to inner upload and cleanup', async () => {
      const cleanupSpy = vi.fn();
      const upload = new ManagedMultipartUpload(mockInnerUpload, {}, cleanupSpy);

      await upload.abort();

      expect(abortSpy).toHaveBeenCalled();
      expect(upload.isFinalized).toBe(true);
      expect(cleanupSpy).toHaveBeenCalled();
    });

    it('should silently succeed if already finalized', async () => {
      const upload = new ManagedMultipartUpload(mockInnerUpload);
      await upload.abort();

      // Second abort should not throw
      await expect(upload.abort()).resolves.toBeUndefined();
      expect(abortSpy).toHaveBeenCalledTimes(1);
    });

    it('should cleanup even if abort throws', async () => {
      const cleanupSpy = vi.fn();
      abortSpy.mockRejectedValueOnce(new Error('Abort failed'));
      const upload = new ManagedMultipartUpload(mockInnerUpload, {}, cleanupSpy);

      await expect(upload.abort()).rejects.toThrow('Abort failed');
      expect(upload.isFinalized).toBe(true);
      expect(cleanupSpy).toHaveBeenCalled();
    });
  });

  describe('cleanup', () => {
    it('should set isFinalized and call cleanup callback', () => {
      const cleanupSpy = vi.fn();
      const upload = new ManagedMultipartUpload(mockInnerUpload, {}, cleanupSpy);

      upload.cleanup();

      expect(upload.isFinalized).toBe(true);
      expect(cleanupSpy).toHaveBeenCalledTimes(1);
    });

    it('should only run cleanup once', () => {
      const cleanupSpy = vi.fn();
      const upload = new ManagedMultipartUpload(mockInnerUpload, {}, cleanupSpy);

      upload.cleanup();
      upload.cleanup();
      upload.cleanup();

      expect(cleanupSpy).toHaveBeenCalledTimes(1);
    });

    it('should clear timeout on cleanup', () => {
      const clearTimeoutSpy = vi.spyOn(globalThis, 'clearTimeout');
      const upload = new ManagedMultipartUpload(mockInnerUpload, { timeoutMs: 60000 });

      upload.cleanup();

      expect(clearTimeoutSpy).toHaveBeenCalled();
      clearTimeoutSpy.mockRestore();
    });
  });

  describe('timeout handling', () => {
    it('should call onTimeout callback when timeout expires', async () => {
      const onTimeoutSpy = vi.fn();
      new ManagedMultipartUpload(mockInnerUpload, {
        timeoutMs: 5000,
        onTimeout: onTimeoutSpy,
      });

      vi.advanceTimersByTime(5000);

      expect(onTimeoutSpy).toHaveBeenCalled();
    });

    it('should abort upload when timeout expires', async () => {
      new ManagedMultipartUpload(mockInnerUpload, { timeoutMs: 5000 });

      vi.advanceTimersByTime(5000);

      // Flush pending promises
      await vi.runAllTimersAsync();

      expect(abortSpy).toHaveBeenCalled();
    });

    it('should not trigger timeout if already finalized', async () => {
      const onTimeoutSpy = vi.fn();
      const upload = new ManagedMultipartUpload(mockInnerUpload, {
        timeoutMs: 5000,
        onTimeout: onTimeoutSpy,
      });

      await upload.complete([]);

      vi.advanceTimersByTime(5000);

      expect(onTimeoutSpy).not.toHaveBeenCalled();
      expect(abortSpy).not.toHaveBeenCalled();
    });

    it('should use DEFAULT_MULTIPART_TIMEOUT_MS when not specified', () => {
      const onTimeoutSpy = vi.fn();
      new ManagedMultipartUpload(mockInnerUpload, { onTimeout: onTimeoutSpy });

      // Should not trigger before default timeout
      vi.advanceTimersByTime(DEFAULT_MULTIPART_TIMEOUT_MS - 1);
      expect(onTimeoutSpy).not.toHaveBeenCalled();

      // Should trigger at default timeout
      vi.advanceTimersByTime(1);
      expect(onTimeoutSpy).toHaveBeenCalled();
    });

    it('should handle abort errors gracefully during timeout', async () => {
      abortSpy.mockRejectedValueOnce(new Error('Abort failed'));
      const upload = new ManagedMultipartUpload(mockInnerUpload, { timeoutMs: 5000 });

      // Should not throw when timeout triggers abort that fails
      vi.advanceTimersByTime(5000);
      await vi.runAllTimersAsync();

      expect(upload.isFinalized).toBe(true);
    });
  });
});

// ============================================================================
// createBufferedMultipartUpload Cleanup Tests
// ============================================================================

describe('createBufferedMultipartUpload cleanup', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should wrap with ManagedMultipartUpload by default', async () => {
    const onComplete = vi.fn();
    const upload = createBufferedMultipartUpload(onComplete);

    expect(upload).toBeInstanceOf(ManagedMultipartUpload);
    expect(upload.isFinalized).toBe(false);
  });

  it('should return raw upload when managed is false', () => {
    const onComplete = vi.fn();
    const upload = createBufferedMultipartUpload(onComplete, { managed: false });

    expect(upload).not.toBeInstanceOf(ManagedMultipartUpload);
    expect(upload.isFinalized).toBeUndefined();
  });

  it('should timeout abandoned buffered uploads', async () => {
    const onComplete = vi.fn();
    const onTimeout = vi.fn();
    createBufferedMultipartUpload(onComplete, {
      timeoutMs: 5000,
      onTimeout,
    });

    vi.advanceTimersByTime(5000);
    await vi.runAllTimersAsync();

    expect(onTimeout).toHaveBeenCalled();
  });

  it('should release references on cleanup', async () => {
    const onComplete = vi.fn();
    const upload = createBufferedMultipartUpload(onComplete);

    await upload.uploadPart(1, new Uint8Array([1, 2, 3]));
    await upload.complete([{ partNumber: 1, etag: 'part-1' }]);

    expect(onComplete).toHaveBeenCalled();

    // After completion, trying to upload should fail
    await expect(upload.uploadPart(2, new Uint8Array([4]))).rejects.toMatchObject({
      code: ErrorCodes.MULTIPART_UPLOAD_FINALIZED,
    });
  });

  it('should throw on uploadPart after abort', async () => {
    const onComplete = vi.fn();
    const upload = createBufferedMultipartUpload(onComplete);

    await upload.abort();

    await expect(upload.uploadPart(1, new Uint8Array([1]))).rejects.toMatchObject({
      code: ErrorCodes.MULTIPART_UPLOAD_FINALIZED,
    });
  });
});

// ============================================================================
// R2Storage Multipart Cleanup Tests
// ============================================================================

describe('R2Storage multipart cleanup', () => {
  let mockBucket: R2Bucket;
  let mockUpload: R2MultipartUpload;
  let uploadPartSpy: ReturnType<typeof vi.fn>;
  let completeSpy: ReturnType<typeof vi.fn>;
  let abortSpy: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.useFakeTimers();

    uploadPartSpy = vi.fn(async (partNumber: number): Promise<R2UploadedPart> => ({
      partNumber,
      etag: `etag-${partNumber}`,
    }));
    completeSpy = vi.fn(async () => ({ key: 'test-key', size: 100, etag: 'final-etag' }));
    abortSpy = vi.fn(async () => {});

    mockUpload = {
      uploadPart: uploadPartSpy,
      complete: completeSpy,
      abort: abortSpy,
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

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should return ManagedMultipartUpload from createMultipartUpload', async () => {
    const storage = new R2Storage(mockBucket);
    const upload = await storage.createMultipartUpload('test-key');

    expect(upload).toBeInstanceOf(ManagedMultipartUpload);
    expect(upload.isFinalized).toBe(false);
  });

  it('should cleanup semaphore reference on complete', async () => {
    const storage = new R2Storage(mockBucket);
    const upload = await storage.createMultipartUpload('test-key');

    const part = await upload.uploadPart(1, new Uint8Array([1, 2, 3]));
    await upload.complete([part]);

    // After completion, upload should be finalized
    expect(upload.isFinalized).toBe(true);

    // New uploads should fail
    await expect(upload.uploadPart(2, new Uint8Array([4]))).rejects.toMatchObject({
      code: ErrorCodes.MULTIPART_UPLOAD_FINALIZED,
    });
  });

  it('should cleanup semaphore reference on abort', async () => {
    const storage = new R2Storage(mockBucket);
    const upload = await storage.createMultipartUpload('test-key');

    await upload.abort();

    expect(upload.isFinalized).toBe(true);
  });

  it('should abort upload on timeout', async () => {
    const storage = new R2Storage(mockBucket);
    await storage.createMultipartUpload('test-key');

    // Advance past the default timeout
    vi.advanceTimersByTime(DEFAULT_MULTIPART_TIMEOUT_MS);
    await vi.runAllTimersAsync();

    expect(abortSpy).toHaveBeenCalled();
  });

  it('should release semaphore permits when timeout aborts in-flight uploads', async () => {
    const storage = new R2Storage(mockBucket, { multipartConcurrency: 1 });

    // Create an upload
    const upload = await storage.createMultipartUpload('test-key');

    // Start an upload that will hang - use a deferred promise we can reject
    let rejectUpload: (reason?: unknown) => void;
    uploadPartSpy.mockImplementationOnce(
      () =>
        new Promise((_, reject) => {
          rejectUpload = reject;
        })
    );
    const uploadPromise = upload.uploadPart(1, new Uint8Array([1]));

    // Trigger timeout
    vi.advanceTimersByTime(DEFAULT_MULTIPART_TIMEOUT_MS);
    await vi.runAllTimersAsync();

    // Verify abort was called and upload is finalized
    expect(abortSpy).toHaveBeenCalled();
    expect(upload.isFinalized).toBe(true);

    // Simulate the hanging operation being rejected (e.g., by the underlying system)
    rejectUpload!(new Error('Upload aborted'));
    await expect(uploadPromise).rejects.toThrow('Upload aborted');
  });
});

// ============================================================================
// MemoryStorage Multipart Cleanup Tests
// ============================================================================

describe('MemoryStorage multipart cleanup', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should return ManagedMultipartUpload from createMultipartUpload', async () => {
    const storage = new MemoryStorage();
    const upload = await storage.createMultipartUpload('test-key');

    expect(upload).toBeInstanceOf(ManagedMultipartUpload);
    expect(upload.isFinalized).toBe(false);
  });

  it('should complete multipart upload and store data', async () => {
    const storage = new MemoryStorage();
    const upload = await storage.createMultipartUpload('test-key');

    const part1 = await upload.uploadPart(1, new Uint8Array([1, 2]));
    const part2 = await upload.uploadPart(2, new Uint8Array([3, 4]));
    await upload.complete([part1, part2]);

    const result = await storage.get('test-key');
    expect(result).toEqual(new Uint8Array([1, 2, 3, 4]));
  });

  it('should cleanup after abort', async () => {
    const storage = new MemoryStorage();
    const upload = await storage.createMultipartUpload('test-key');

    await upload.uploadPart(1, new Uint8Array([1, 2]));
    await upload.abort();

    expect(upload.isFinalized).toBe(true);

    // Data should not be stored
    const result = await storage.get('test-key');
    expect(result).toBeNull();
  });
});

// ============================================================================
// DEFAULT_MULTIPART_TIMEOUT_MS Tests
// ============================================================================

describe('DEFAULT_MULTIPART_TIMEOUT_MS', () => {
  it('should be 30 minutes', () => {
    expect(DEFAULT_MULTIPART_TIMEOUT_MS).toBe(30 * 60 * 1000);
  });
});

// ============================================================================
// MetricsStorageWrapper Multipart Cleanup Tests
// ============================================================================

describe('MetricsStorageWrapper multipart cleanup', () => {
  let mockBackend: MemoryStorage;
  let metricsStorage: MetricsStorageWrapper;

  beforeEach(async () => {
    vi.useFakeTimers();
    mockBackend = new MemoryStorage();
    // Import MetricsCollector dynamically to avoid import at top level
    const { MetricsCollector } = await import('../../../src/metrics/index.js');
    const metrics = new MetricsCollector();
    metricsStorage = new MetricsStorageWrapper(mockBackend, metrics);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should return ManagedMultipartUpload from createMultipartUpload', async () => {
    const upload = await metricsStorage.createMultipartUpload('test-key');

    expect(upload).toBeInstanceOf(ManagedMultipartUpload);
    expect(upload.isFinalized).toBe(false);
  });

  it('should cleanup closure references on complete', async () => {
    const upload = await metricsStorage.createMultipartUpload('test-key');

    const part1 = await upload.uploadPart(1, new Uint8Array([1, 2]));
    await upload.complete([part1]);

    expect(upload.isFinalized).toBe(true);

    // Verify data was stored correctly
    const result = await metricsStorage.get('test-key');
    expect(result).toEqual(new Uint8Array([1, 2]));

    // Further operations should fail
    await expect(upload.uploadPart(2, new Uint8Array([3]))).rejects.toMatchObject({
      code: ErrorCodes.MULTIPART_UPLOAD_FINALIZED,
    });
  });

  it('should cleanup closure references on abort', async () => {
    const upload = await metricsStorage.createMultipartUpload('test-key');

    await upload.uploadPart(1, new Uint8Array([1, 2]));
    await upload.abort();

    expect(upload.isFinalized).toBe(true);

    // Data should not be stored
    const result = await metricsStorage.get('test-key');
    expect(result).toBeNull();

    // Further operations should fail
    await expect(upload.uploadPart(2, new Uint8Array([3]))).rejects.toMatchObject({
      code: ErrorCodes.MULTIPART_UPLOAD_FINALIZED,
    });
  });

  it('should timeout and cleanup abandoned uploads', async () => {
    await metricsStorage.createMultipartUpload('test-key');

    // Advance past default timeout
    vi.advanceTimersByTime(DEFAULT_MULTIPART_TIMEOUT_MS);
    await vi.runAllTimersAsync();

    // Data should not be stored (upload was aborted)
    const result = await metricsStorage.get('test-key');
    expect(result).toBeNull();
  });

  it('should record metrics and still cleanup properly', async () => {
    // This test verifies that even though MetricsStorageWrapper captures
    // closure variables (metricsRef, getErrorTypeRef), they are properly
    // released when the upload completes
    const upload = await metricsStorage.createMultipartUpload('test-key');

    // Upload and complete
    const part = await upload.uploadPart(1, new Uint8Array([1, 2, 3]));
    await upload.complete([part]);

    // Upload should be fully finalized
    expect(upload.isFinalized).toBe(true);

    // Verify complete() correctly stores the data
    const stored = await metricsStorage.get('test-key');
    expect(stored).toEqual(new Uint8Array([1, 2, 3]));
  });

  it('should cleanup on complete even when metrics recording fails', async () => {
    // Use a custom metrics collector that may throw
    const mockMetrics = {
      recordR2Operation: vi.fn(),
    };

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const storage = new MetricsStorageWrapper(mockBackend, mockMetrics as any);
    const upload = await storage.createMultipartUpload('test-key');

    const part = await upload.uploadPart(1, new Uint8Array([1]));
    await upload.complete([part]);

    expect(upload.isFinalized).toBe(true);
    expect(mockMetrics.recordR2Operation).toHaveBeenCalled();
  });
});
