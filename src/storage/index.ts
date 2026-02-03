/**
 * Storage Abstraction Layer
 *
 * Supports multiple backends:
 * - Local filesystem (.mongolake/)
 * - Cloudflare R2
 * - In-memory (for testing)
 *
 * For S3-compatible storage, import separately:
 * ```ts
 * import { S3Storage } from 'mongolake/storage/s3';
 * ```
 */

import type { R2Bucket, MongoLakeConfig } from '@types';
import {
  MetricsCollector,
} from '@mongolake/metrics/index.js';
import {
  InvalidStorageKeyError as BaseInvalidStorageKeyError,
  StorageError,
  TransientError,
  ErrorCodes,
} from '@errors/index.js';
import { STORAGE_MULTIPART_CONCURRENCY } from '@mongolake/constants.js';

// ============================================================================
// Storage Interface
// ============================================================================

export interface StorageBackend {
  /** Get object by key */
  get(key: string): Promise<Uint8Array | null>;

  /** Put object */
  put(key: string, data: Uint8Array): Promise<void>;

  /** Delete object */
  delete(key: string): Promise<void>;

  /** List objects by prefix */
  list(prefix: string): Promise<string[]>;

  /** Check if object exists */
  exists(key: string): Promise<boolean>;

  /** Get object metadata (size) */
  head(key: string): Promise<{ size: number } | null>;

  /** Multipart upload for large files */
  createMultipartUpload(key: string): Promise<MultipartUpload>;

  /** Get object as a readable stream for efficient large file handling */
  getStream(key: string): Promise<ReadableStream<Uint8Array> | null>;

  /** Put object from a readable stream for efficient large file handling */
  putStream(key: string, stream: ReadableStream<Uint8Array>): Promise<void>;
}

export interface MultipartUpload {
  uploadPart(partNumber: number, data: Uint8Array): Promise<UploadedPart>;
  complete(parts: UploadedPart[]): Promise<void>;
  abort(): Promise<void>;
  /** Whether the upload has been completed or aborted */
  readonly isFinalized?: boolean;
  /** Explicit cleanup to release resources (called automatically on complete/abort) */
  cleanup?(): void;
}

/** Default timeout for abandoned multipart uploads (30 minutes) */
export const DEFAULT_MULTIPART_TIMEOUT_MS = 30 * 60 * 1000;

/**
 * Configuration options for managed multipart uploads.
 */
export interface ManagedMultipartUploadOptions {
  /** Timeout in milliseconds after which the upload is automatically aborted (default: 30 minutes) */
  timeoutMs?: number;
  /** Callback invoked when the upload times out */
  onTimeout?: () => void;
}

export interface UploadedPart {
  partNumber: number;
  etag: string;
}

// ============================================================================
// Storage Key Validation
// ============================================================================

/**
 * Error thrown when a storage key is invalid or attempts path traversal.
 * Extends the base InvalidStorageKeyError from the errors module.
 */
export class InvalidStorageKeyError extends BaseInvalidStorageKeyError {
  constructor(message: string, key?: string) {
    super(message, key);
  }
}

/**
 * Validates and sanitizes a storage key to prevent path traversal attacks.
 *
 * This function ensures that:
 * 1. Keys do not start with an absolute path (/)
 * 2. Keys do not contain '..' path components that could escape the base directory
 * 3. The normalized path stays within the intended base directory
 *
 * @param key - The storage key to validate
 * @throws InvalidStorageKeyError if the key is invalid or attempts path traversal
 * @returns The validated key (unchanged if valid)
 *
 * @example
 * validateStorageKey('data/file.txt');       // OK
 * validateStorageKey('../etc/passwd');       // Throws InvalidStorageKeyError
 * validateStorageKey('/etc/passwd');         // Throws InvalidStorageKeyError
 * validateStorageKey('data/../other/file');  // Throws InvalidStorageKeyError
 */
export function validateStorageKey(key: string): string {
  // Reject empty keys
  if (!key || key.trim() === '') {
    throw new InvalidStorageKeyError('Storage key cannot be empty');
  }

  // Reject absolute paths (starting with /)
  if (key.startsWith('/')) {
    throw new InvalidStorageKeyError(
      `Storage key cannot be an absolute path: "${key}"`
    );
  }

  // Normalize backslashes to forward slashes to handle Windows-style paths
  // This must be done before checking for '..' segments
  const normalized = key.replace(/\\/g, '/');

  // Reject keys containing '..' path components
  // This catches: '..', '../foo', 'foo/../bar', 'foo/..', etc.
  const segments = normalized.split('/');
  for (const segment of segments) {
    if (segment === '..') {
      throw new InvalidStorageKeyError(
        `Storage key cannot contain path traversal sequences (..): "${key}"`
      );
    }
  }

  return key;
}

/**
 * Validates a storage prefix for list operations.
 * Prefixes have slightly different rules - they can be empty.
 *
 * @param prefix - The prefix to validate
 * @throws InvalidStorageKeyError if the prefix is invalid
 * @returns The validated prefix
 */
export function validateStoragePrefix(prefix: string): string {
  // Empty prefix is allowed for listing all keys
  if (prefix === '') {
    return prefix;
  }

  // Apply same validation as keys for non-empty prefixes
  return validateStorageKey(prefix);
}

// ============================================================================
// Concurrency Control
// ============================================================================

/**
 * Default limit for concurrent multipart upload operations.
 * Re-exported from constants.ts for backward compatibility.
 */
export const DEFAULT_MULTIPART_CONCURRENCY = STORAGE_MULTIPART_CONCURRENCY;

/**
 * Semaphore for limiting concurrent async operations.
 *
 * Used to prevent exhausting connection pools or memory when uploading
 * multiple parts in parallel during multipart uploads.
 *
 * @example
 * ```typescript
 * const semaphore = new Semaphore(4);
 *
 * // Only 4 uploads can run concurrently
 * await Promise.all(parts.map(async (part) => {
 *   await semaphore.acquire();
 *   try {
 *     await upload.uploadPart(part.number, part.data);
 *   } finally {
 *     semaphore.release();
 *   }
 * }));
 * ```
 */
export class Semaphore {
  private permits: number;
  private waitQueue: Array<() => void> = [];

  constructor(permits: number) {
    if (permits < 1) {
      throw new Error('Semaphore permits must be at least 1');
    }
    this.permits = permits;
  }

  /**
   * Acquire a permit, waiting if necessary until one is available.
   */
  async acquire(): Promise<void> {
    if (this.permits > 0) {
      this.permits--;
      return;
    }

    return new Promise<void>((resolve) => {
      this.waitQueue.push(() => {
        this.permits--;
        resolve();
      });
    });
  }

  /**
   * Release a permit, allowing a waiting operation to proceed.
   */
  release(): void {
    this.permits++;
    const next = this.waitQueue.shift();
    if (next) {
      next();
    }
  }

  /**
   * Get the number of available permits.
   */
  get availablePermits(): number {
    return this.permits;
  }

  /**
   * Get the number of operations waiting for a permit.
   */
  get waitingCount(): number {
    return this.waitQueue.length;
  }
}

/**
 * Options for R2Storage multipart uploads.
 */
export interface R2StorageOptions {
  /**
   * Maximum number of concurrent part uploads.
   * Prevents exhausting connection pool or memory under load.
   * @default 5
   */
  multipartConcurrency?: number;
}

// ============================================================================
// Helpers
// ============================================================================

/**
 * Concatenates an array of Uint8Array parts into a single Uint8Array.
 * Parts are concatenated in the order provided.
 */
export function concatenateParts(parts: Uint8Array[]): Uint8Array {
  const totalSize = parts.reduce((sum, part) => sum + part.length, 0);
  const combined = new Uint8Array(totalSize);
  let offset = 0;
  for (const part of parts) {
    combined.set(part, offset);
    offset += part.length;
  }
  return combined;
}

/**
 * Options for createBufferedMultipartUpload.
 */
export interface BufferedMultipartUploadOptions {
  /** Whether to wrap with ManagedMultipartUpload for automatic cleanup (default: true) */
  managed?: boolean;
  /** Timeout for abandoned uploads when managed is true (default: DEFAULT_MULTIPART_TIMEOUT_MS) */
  timeoutMs?: number;
  /** Callback invoked when the upload times out */
  onTimeout?: () => void;
}

/**
 * Creates a buffered multipart upload that stores parts in memory and
 * concatenates them on completion. Used by storage backends that don't
 * have native multipart upload support (FileSystem, S3 simplified, Memory).
 *
 * @param onComplete - Callback invoked with the final concatenated data when complete() is called
 * @param options - Configuration options
 * @returns A MultipartUpload implementation
 */
export function createBufferedMultipartUpload(
  onComplete: (data: Uint8Array) => Promise<void>,
  options: BufferedMultipartUploadOptions = {}
): MultipartUpload {
  let parts: Map<number, Uint8Array> | null = new Map();
  let onCompleteRef: ((data: Uint8Array) => Promise<void>) | null = onComplete;

  const innerUpload: MultipartUpload = {
    async uploadPart(partNumber: number, data: Uint8Array): Promise<UploadedPart> {
      if (!parts) {
        throw new StorageError(
          'Cannot upload part: multipart upload has been cleaned up',
          ErrorCodes.MULTIPART_UPLOAD_FINALIZED
        );
      }
      parts.set(partNumber, data);
      return { partNumber, etag: `part-${partNumber}` };
    },

    async complete(uploadedParts: UploadedPart[]): Promise<void> {
      if (!parts || !onCompleteRef) {
        throw new StorageError(
          'Cannot complete: multipart upload has been cleaned up',
          ErrorCodes.MULTIPART_UPLOAD_FINALIZED
        );
      }
      // Sort parts by number and concatenate
      const sortedParts = uploadedParts.sort((a, b) => a.partNumber - b.partNumber);
      const sortedData = sortedParts.map((p) => {
        const data = parts?.get(p.partNumber);
        if (!data) {
          throw new Error(`Missing data for part ${p.partNumber}. Part was not uploaded.`);
        }
        return data;
      });
      const combined = concatenateParts(sortedData);
      await onCompleteRef(combined);
    },

    async abort(): Promise<void> {
      if (parts) {
        parts.clear();
      }
    },
  };

  // By default, wrap with ManagedMultipartUpload for proper cleanup
  const managed = options.managed ?? true;
  if (!managed) {
    return innerUpload;
  }

  return new ManagedMultipartUpload(
    innerUpload,
    {
      timeoutMs: options.timeoutMs ?? DEFAULT_MULTIPART_TIMEOUT_MS,
      onTimeout: options.onTimeout,
    },
    // Cleanup callback to release references
    () => {
      parts = null;
      onCompleteRef = null;
    }
  );
}

/**
 * ManagedMultipartUpload wraps a MultipartUpload to add:
 * - Automatic cleanup when complete/abort is called
 * - Timeout handling for abandoned uploads
 * - Proper closure release
 *
 * This ensures that resources are properly released even if the caller
 * forgets to call complete() or abort().
 */
export class ManagedMultipartUpload implements MultipartUpload {
  private _isFinalized = false;
  private timeoutHandle: ReturnType<typeof setTimeout> | null = null;
  private innerUpload: MultipartUpload | null;
  private onCleanup: (() => void) | null;

  constructor(
    upload: MultipartUpload,
    options: ManagedMultipartUploadOptions = {},
    onCleanup?: () => void
  ) {
    this.innerUpload = upload;
    this.onCleanup = onCleanup ?? null;

    const timeoutMs = options.timeoutMs ?? DEFAULT_MULTIPART_TIMEOUT_MS;
    if (timeoutMs > 0) {
      this.timeoutHandle = setTimeout(() => {
        if (!this._isFinalized) {
          options.onTimeout?.();
          // Attempt to abort the upload on timeout
          this.abort().catch(() => {
            // Ignore abort errors on timeout - best effort cleanup
          });
        }
      }, timeoutMs);
    }
  }

  get isFinalized(): boolean {
    return this._isFinalized;
  }

  async uploadPart(partNumber: number, data: Uint8Array): Promise<UploadedPart> {
    if (this._isFinalized) {
      throw new StorageError(
        'Cannot upload part: multipart upload has been finalized',
        ErrorCodes.MULTIPART_UPLOAD_FINALIZED
      );
    }
    if (!this.innerUpload) {
      throw new StorageError(
        'Cannot upload part: multipart upload has been cleaned up',
        ErrorCodes.MULTIPART_UPLOAD_FINALIZED
      );
    }
    return this.innerUpload.uploadPart(partNumber, data);
  }

  async complete(parts: UploadedPart[]): Promise<void> {
    if (this._isFinalized) {
      throw new StorageError(
        'Cannot complete: multipart upload has already been finalized',
        ErrorCodes.MULTIPART_UPLOAD_FINALIZED
      );
    }
    if (!this.innerUpload) {
      throw new StorageError(
        'Cannot complete: multipart upload has been cleaned up',
        ErrorCodes.MULTIPART_UPLOAD_FINALIZED
      );
    }
    try {
      await this.innerUpload.complete(parts);
    } finally {
      this.cleanup();
    }
  }

  async abort(): Promise<void> {
    if (this._isFinalized) {
      // Already finalized, nothing to do
      return;
    }
    if (!this.innerUpload) {
      // Already cleaned up, nothing to do
      return;
    }
    try {
      await this.innerUpload.abort();
    } finally {
      this.cleanup();
    }
  }

  cleanup(): void {
    if (this._isFinalized) {
      return;
    }

    this._isFinalized = true;

    // Clear the timeout
    if (this.timeoutHandle !== null) {
      clearTimeout(this.timeoutHandle);
      this.timeoutHandle = null;
    }

    // Release references to allow garbage collection
    this.innerUpload = null;

    // Invoke cleanup callback
    if (this.onCleanup) {
      this.onCleanup();
      this.onCleanup = null;
    }
  }
}

// ============================================================================
// Factory
// ============================================================================

export function createStorage(config: MongoLakeConfig): StorageBackend {
  if (config.local) {
    return new FileSystemStorage(config.local);
  }

  if (config.bucket) {
    return new R2Storage(config.bucket);
  }

  if (config.endpoint) {
    throw new Error(
      'S3Storage is now a separate optional import. ' +
      'To use S3-compatible storage, import S3Storage from "mongolake/storage/s3":\n\n' +
      '  import { S3Storage } from "mongolake/storage/s3";\n\n' +
      '  const storage = new S3Storage({\n' +
      '    endpoint: config.endpoint,\n' +
      '    accessKeyId: config.accessKeyId,\n' +
      '    secretAccessKey: config.secretAccessKey,\n' +
      '    bucket: config.bucketName,\n' +
      '  });'
    );
  }

  // Default to local .mongolake folder
  return new FileSystemStorage('.mongolake');
}

// ============================================================================
// Filesystem Storage (Local Development)
// ============================================================================

export class FileSystemStorage implements StorageBackend {
  private basePath: string;
  private fs: typeof import('node:fs/promises') | null = null;
  private fsSync: typeof import('node:fs') | null = null;
  private path: typeof import('node:path') | null = null;

  constructor(basePath: string) {
    this.basePath = basePath;
  }

  private async ensureModules() {
    if (!this.fs) {
      this.fs = await import('node:fs/promises');
      this.fsSync = await import('node:fs');
      this.path = await import('node:path');
    }
  }

  /**
   * Get the loaded modules, asserting they are initialized.
   * Call this after ensureModules() to get properly typed access to the loaded modules.
   * @throws Error if modules are not initialized
   */
  private getModules(): {
    fs: typeof import('node:fs/promises');
    fsSync: typeof import('node:fs');
    path: typeof import('node:path');
  } {
    if (!this.fs || !this.fsSync || !this.path) {
      throw new Error('FileSystemStorage: modules not initialized. Call ensureModules() first.');
    }
    return { fs: this.fs, fsSync: this.fsSync, path: this.path };
  }

  private getFullPath(key: string): string {
    const { path } = this.getModules();
    return path.join(this.basePath, key);
  }

  async get(key: string): Promise<Uint8Array | null> {
    validateStorageKey(key);
    await this.ensureModules();
    const { fs } = this.getModules();
    try {
      const buffer = await fs.readFile(this.getFullPath(key));
      return new Uint8Array(buffer);
    } catch (e: unknown) {
      if ((e as NodeJS.ErrnoException).code === 'ENOENT') return null;
      throw e;
    }
  }

  async put(key: string, data: Uint8Array): Promise<void> {
    validateStorageKey(key);
    await this.ensureModules();
    const { fs, path } = this.getModules();
    const fullPath = this.getFullPath(key);
    await fs.mkdir(path.dirname(fullPath), { recursive: true });
    await fs.writeFile(fullPath, data);
  }

  async delete(key: string): Promise<void> {
    validateStorageKey(key);
    await this.ensureModules();
    const { fs } = this.getModules();
    try {
      await fs.unlink(this.getFullPath(key));
    } catch (e: unknown) {
      if ((e as NodeJS.ErrnoException).code !== 'ENOENT') throw e;
    }
  }

  async list(prefix: string): Promise<string[]> {
    validateStoragePrefix(prefix);
    await this.ensureModules();
    const { fs, path } = this.getModules();
    const results: string[] = [];
    const basePath = this.getFullPath(prefix);

    async function walk(dir: string, base: string) {
      try {
        const entries = await fs.readdir(dir, { withFileTypes: true });
        for (const entry of entries) {
          const fullPath = path.join(dir, entry.name);
          const relativePath = path.join(base, entry.name);
          if (entry.isDirectory()) {
            await walk(fullPath, relativePath);
          } else {
            results.push(relativePath);
          }
        }
      } catch (e: unknown) {
        if ((e as NodeJS.ErrnoException).code !== 'ENOENT') throw e;
      }
    }

    await walk(basePath, prefix);
    return results;
  }

  async exists(key: string): Promise<boolean> {
    validateStorageKey(key);
    await this.ensureModules();
    const { fs } = this.getModules();
    try {
      await fs.access(this.getFullPath(key));
      return true;
    } catch {
      return false;
    }
  }

  async head(key: string): Promise<{ size: number } | null> {
    validateStorageKey(key);
    await this.ensureModules();
    const { fs } = this.getModules();
    try {
      const stats = await fs.stat(this.getFullPath(key));
      return { size: stats.size };
    } catch (e: unknown) {
      if ((e as NodeJS.ErrnoException).code === 'ENOENT') return null;
      throw e;
    }
  }

  async createMultipartUpload(key: string): Promise<MultipartUpload> {
    validateStorageKey(key);
    await this.ensureModules();
    return createBufferedMultipartUpload((data) => this.put(key, data));
  }

  async getStream(key: string): Promise<ReadableStream<Uint8Array> | null> {
    validateStorageKey(key);
    await this.ensureModules();
    const { fs, fsSync } = this.getModules();
    const fullPath = this.getFullPath(key);

    try {
      await fs.access(fullPath);
    } catch {
      return null;
    }

    const nodeStream = fsSync.createReadStream(fullPath);

    return new ReadableStream<Uint8Array>({
      start(controller) {
        nodeStream.on('data', (chunk: string | Buffer) => {
          const buffer = typeof chunk === 'string' ? Buffer.from(chunk) : chunk;
          controller.enqueue(new Uint8Array(buffer));
        });
        nodeStream.on('end', () => {
          controller.close();
        });
        nodeStream.on('error', (err) => {
          controller.error(err);
        });
      },
      cancel() {
        nodeStream.destroy();
      },
    });
  }

  async putStream(key: string, stream: ReadableStream<Uint8Array>): Promise<void> {
    validateStorageKey(key);
    await this.ensureModules();
    const { fs, fsSync, path } = this.getModules();
    const fullPath = this.getFullPath(key);
    await fs.mkdir(path.dirname(fullPath), { recursive: true });

    const writeStream = fsSync.createWriteStream(fullPath);
    const reader = stream.getReader();

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        await new Promise<void>((resolve, reject) => {
          const canContinue = writeStream.write(value, (err) => {
            if (err) reject(err);
          });
          if (canContinue) {
            resolve();
          } else {
            writeStream.once('drain', resolve);
          }
        });
      }
    } finally {
      reader.releaseLock();
      await new Promise<void>((resolve, reject) => {
        writeStream.end((err: Error | null | undefined) => {
          if (err) reject(err);
          else resolve();
        });
      });
    }
  }
}

// ============================================================================
// R2 Error Handling Helpers
// ============================================================================

/**
 * Check if an error is a rate limit error (429 status)
 */
function isRateLimitError(error: unknown): boolean {
  if (error && typeof error === 'object') {
    const status = (error as { status?: number }).status;
    if (status === 429) return true;
    if (error instanceof Error) {
      const msg = error.message.toLowerCase();
      return msg.includes('too many requests') || msg.includes('rate limit');
    }
  }
  return false;
}

/**
 * Wrap an R2 error with appropriate StorageError or TransientError
 */
function wrapR2Error(
  error: unknown,
  operation: 'get' | 'put' | 'delete' | 'list' | 'head' | 'exists' | 'createMultipartUpload',
  key?: string,
  details?: Record<string, unknown>
): Error {
  // If already a StorageError or TransientError, return as-is
  if (error instanceof StorageError || error instanceof TransientError) {
    return error;
  }

  // Check for rate limiting (429)
  if (isRateLimitError(error)) {
    return new TransientError(`R2 rate limited during ${operation}${key ? ` for key: ${key}` : ''}`, {
      originalError: error instanceof Error ? error : new Error(String(error)),
      retryCount: 0,
    });
  }

  // Determine the appropriate error code based on operation
  let errorCode: string;
  switch (operation) {
    case 'get':
      errorCode = ErrorCodes.STORAGE_READ_FAILED;
      break;
    case 'put':
      errorCode = ErrorCodes.STORAGE_WRITE_FAILED;
      break;
    case 'delete':
      errorCode = ErrorCodes.STORAGE_DELETE_FAILED;
      break;
    default:
      errorCode = ErrorCodes.STORAGE_ERROR;
  }

  const originalError = error instanceof Error ? error : new Error(String(error));
  const message = `R2 ${operation} failed${key ? ` for key: ${key}` : ''}: ${originalError.message}`;

  return new StorageError(message, errorCode, {
    key,
    cause: originalError,
    operation,
    ...details,
  });
}

// ============================================================================
// R2 Storage (Cloudflare)
// ============================================================================

export class R2Storage implements StorageBackend {
  private multipartConcurrency: number;

  constructor(
    private bucket: R2Bucket,
    options?: R2StorageOptions
  ) {
    this.multipartConcurrency = options?.multipartConcurrency ?? DEFAULT_MULTIPART_CONCURRENCY;
    if (this.multipartConcurrency < 1) {
      throw new Error('multipartConcurrency must be at least 1');
    }
  }

  async get(key: string): Promise<Uint8Array | null> {
    validateStorageKey(key);
    try {
      const obj = await this.bucket.get(key);
      if (!obj) return null;
      const buffer = await obj.arrayBuffer();
      return new Uint8Array(buffer);
    } catch (error) {
      throw wrapR2Error(error, 'get', key);
    }
  }

  async put(key: string, data: Uint8Array): Promise<void> {
    validateStorageKey(key);
    try {
      await this.bucket.put(key, data);
    } catch (error) {
      throw wrapR2Error(error, 'put', key, { size: data.length });
    }
  }

  async delete(key: string): Promise<void> {
    validateStorageKey(key);
    try {
      await this.bucket.delete(key);
    } catch (error) {
      throw wrapR2Error(error, 'delete', key);
    }
  }

  async list(prefix: string): Promise<string[]> {
    validateStoragePrefix(prefix);
    try {
      const results: string[] = [];
      let cursor: string | undefined;

      do {
        const response = await this.bucket.list({ prefix, cursor });
        for (const obj of response.objects) {
          results.push(obj.key);
        }
        cursor = response.truncated ? response.cursor : undefined;
      } while (cursor);

      return results;
    } catch (error) {
      throw wrapR2Error(error, 'list', undefined, { prefix });
    }
  }

  async exists(key: string): Promise<boolean> {
    validateStorageKey(key);
    try {
      const obj = await this.bucket.get(key);
      return obj !== null;
    } catch (error) {
      throw wrapR2Error(error, 'exists', key);
    }
  }

  async head(key: string): Promise<{ size: number } | null> {
    validateStorageKey(key);
    try {
      const obj = await this.bucket.head(key);
      if (!obj) return null;
      return { size: obj.size };
    } catch (error) {
      throw wrapR2Error(error, 'head', key);
    }
  }

  async createMultipartUpload(key: string): Promise<MultipartUpload> {
    validateStorageKey(key);
    try {
      const upload = await this.bucket.createMultipartUpload(key);
      let semaphore: Semaphore | null = new Semaphore(this.multipartConcurrency);

      const innerUpload: MultipartUpload = {
        async uploadPart(partNumber: number, data: Uint8Array): Promise<UploadedPart> {
          if (!semaphore) {
            throw new StorageError(
              'Cannot upload part: multipart upload has been cleaned up',
              ErrorCodes.MULTIPART_UPLOAD_FINALIZED
            );
          }
          await semaphore.acquire();
          try {
            const part = await upload.uploadPart(partNumber, data);
            return { partNumber: part.partNumber, etag: part.etag };
          } finally {
            semaphore?.release();
          }
        },

        async complete(parts: UploadedPart[]): Promise<void> {
          await upload.complete(parts.map((p) => ({ partNumber: p.partNumber, etag: p.etag })));
        },

        async abort(): Promise<void> {
          await upload.abort();
        },
      };

      // Wrap with ManagedMultipartUpload for proper cleanup and timeout handling
      return new ManagedMultipartUpload(
        innerUpload,
        { timeoutMs: DEFAULT_MULTIPART_TIMEOUT_MS },
        // Cleanup callback to release the semaphore reference
        () => {
          semaphore = null;
        }
      );
    } catch (error) {
      throw wrapR2Error(error, 'createMultipartUpload', key);
    }
  }

  async getStream(key: string): Promise<ReadableStream<Uint8Array> | null> {
    validateStorageKey(key);
    try {
      const obj = await this.bucket.get(key);
      if (!obj) return null;
      // R2ObjectBody has a native ReadableStream body
      return obj.body as ReadableStream<Uint8Array>;
    } catch (error) {
      throw wrapR2Error(error, 'get', key);
    }
  }

  async putStream(key: string, stream: ReadableStream<Uint8Array>): Promise<void> {
    validateStorageKey(key);
    try {
      // R2 bucket.put natively accepts ReadableStream
      await this.bucket.put(key, stream);
    } catch (error) {
      throw wrapR2Error(error, 'put', key);
    }
  }

  /**
   * Get the configured multipart upload concurrency limit.
   */
  getMultipartConcurrency(): number {
    return this.multipartConcurrency;
  }
}

// ============================================================================
// Memory Storage (Testing)
// ============================================================================

export class MemoryStorage implements StorageBackend {
  private data: Map<string, Uint8Array> = new Map();

  async get(key: string): Promise<Uint8Array | null> {
    validateStorageKey(key);
    return this.data.get(key) || null;
  }

  async put(key: string, data: Uint8Array): Promise<void> {
    validateStorageKey(key);
    this.data.set(key, data);
  }

  async delete(key: string): Promise<void> {
    validateStorageKey(key);
    this.data.delete(key);
  }

  async list(prefix: string): Promise<string[]> {
    validateStoragePrefix(prefix);
    return Array.from(this.data.keys()).filter((k) => k.startsWith(prefix));
  }

  async exists(key: string): Promise<boolean> {
    validateStorageKey(key);
    return this.data.has(key);
  }

  async head(key: string): Promise<{ size: number } | null> {
    validateStorageKey(key);
    const data = this.data.get(key);
    if (!data) return null;
    return { size: data.length };
  }

  async createMultipartUpload(key: string): Promise<MultipartUpload> {
    validateStorageKey(key);
    return createBufferedMultipartUpload((data) => this.put(key, data));
  }

  async getStream(key: string): Promise<ReadableStream<Uint8Array> | null> {
    validateStorageKey(key);
    const data = this.data.get(key);
    if (!data) return null;

    // Convert Uint8Array to a ReadableStream
    return new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(data);
        controller.close();
      },
    });
  }

  async putStream(key: string, stream: ReadableStream<Uint8Array>): Promise<void> {
    validateStorageKey(key);
    // Collect all chunks from the stream and concatenate into a single Uint8Array
    const reader = stream.getReader();
    const chunks: Uint8Array[] = [];

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        chunks.push(value);
      }
    } finally {
      reader.releaseLock();
    }

    const combined = concatenateParts(chunks);
    this.data.set(key, combined);
  }

  /** Clear all data (for testing) */
  clear(): void {
    this.data.clear();
  }
}

// ============================================================================
// Metrics-Enabled Storage Wrapper
// ============================================================================

/**
 * MetricsStorageWrapper wraps a StorageBackend to add metrics collection.
 *
 * Tracks:
 * - Read/write operation counts
 * - Bytes transferred
 * - Operation latencies
 * - Errors
 *
 * @example
 * ```typescript
 * const storage = new R2Storage(bucket);
 * const metrics = new MetricsCollector();
 * const metricsStorage = new MetricsStorageWrapper(storage, metrics);
 * // All operations now record metrics
 * await metricsStorage.get('key');
 * ```
 */
export class MetricsStorageWrapper implements StorageBackend {
  private metrics: MetricsCollector;

  /**
   * Create a MetricsStorageWrapper.
   *
   * @param backend - The underlying storage backend to wrap
   * @param metrics - MetricsCollector instance for recording metrics (required for explicit dependency injection)
   */
  constructor(
    private backend: StorageBackend,
    metrics: MetricsCollector
  ) {
    this.metrics = metrics;
  }

  async get(key: string): Promise<Uint8Array | null> {
    const start = performance.now();
    try {
      const result = await this.backend.get(key);
      const durationMs = performance.now() - start;
      const bytes = result?.length ?? 0;

      this.metrics.recordR2Operation('get', durationMs, bytes);
      return result;
    } catch (error) {
      const durationMs = performance.now() - start;
      this.metrics.recordR2Operation('get', durationMs, 0, this.getErrorType(error));
      throw error;
    }
  }

  async put(key: string, data: Uint8Array): Promise<void> {
    const start = performance.now();
    try {
      await this.backend.put(key, data);
      const durationMs = performance.now() - start;

      this.metrics.recordR2Operation('put', durationMs, data.length);
    } catch (error) {
      const durationMs = performance.now() - start;
      this.metrics.recordR2Operation('put', durationMs, data.length, this.getErrorType(error));
      throw error;
    }
  }

  async delete(key: string): Promise<void> {
    const start = performance.now();
    try {
      await this.backend.delete(key);
      const durationMs = performance.now() - start;

      this.metrics.recordR2Operation('delete', durationMs);
    } catch (error) {
      const durationMs = performance.now() - start;
      this.metrics.recordR2Operation('delete', durationMs, 0, this.getErrorType(error));
      throw error;
    }
  }

  async list(prefix: string): Promise<string[]> {
    const start = performance.now();
    try {
      const result = await this.backend.list(prefix);
      const durationMs = performance.now() - start;

      this.metrics.recordR2Operation('list', durationMs);
      return result;
    } catch (error) {
      const durationMs = performance.now() - start;
      this.metrics.recordR2Operation('list', durationMs, 0, this.getErrorType(error));
      throw error;
    }
  }

  async exists(key: string): Promise<boolean> {
    const start = performance.now();
    try {
      const result = await this.backend.exists(key);
      const durationMs = performance.now() - start;

      this.metrics.recordR2Operation('exists', durationMs);
      return result;
    } catch (error) {
      const durationMs = performance.now() - start;
      this.metrics.recordR2Operation('exists', durationMs, 0, this.getErrorType(error));
      throw error;
    }
  }

  async head(key: string): Promise<{ size: number } | null> {
    const start = performance.now();
    try {
      const result = await this.backend.head(key);
      const durationMs = performance.now() - start;

      this.metrics.recordR2Operation('head', durationMs);
      return result;
    } catch (error) {
      const durationMs = performance.now() - start;
      this.metrics.recordR2Operation('head', durationMs, 0, this.getErrorType(error));
      throw error;
    }
  }

  async createMultipartUpload(key: string): Promise<MultipartUpload> {
    const upload = await this.backend.createMultipartUpload(key);

    // Capture references in local scope that can be nullified on cleanup
    let metricsRef: MetricsCollector | null = this.metrics;
    let getErrorTypeRef: ((error: unknown) => string | undefined) | null = this.getErrorType.bind(this);

    // Create metrics-wrapped upload
    const metricsWrappedUpload: MultipartUpload = {
      async uploadPart(partNumber: number, data: Uint8Array): Promise<UploadedPart> {
        const start = performance.now();
        try {
          const result = await upload.uploadPart(partNumber, data);
          const durationMs = performance.now() - start;
          metricsRef?.recordR2Operation('uploadPart', durationMs, data.length);
          return result;
        } catch (error) {
          const durationMs = performance.now() - start;
          metricsRef?.recordR2Operation('uploadPart', durationMs, data.length, getErrorTypeRef?.(error));
          throw error;
        }
      },

      async complete(parts: UploadedPart[]): Promise<void> {
        const start = performance.now();
        try {
          await upload.complete(parts);
          const durationMs = performance.now() - start;
          metricsRef?.recordR2Operation('completeMultipart', durationMs);
        } catch (error) {
          const durationMs = performance.now() - start;
          metricsRef?.recordR2Operation('completeMultipart', durationMs, 0, getErrorTypeRef?.(error));
          throw error;
        }
      },

      async abort(): Promise<void> {
        const start = performance.now();
        try {
          await upload.abort();
          const durationMs = performance.now() - start;
          metricsRef?.recordR2Operation('abortMultipart', durationMs);
        } catch (error) {
          const durationMs = performance.now() - start;
          metricsRef?.recordR2Operation('abortMultipart', durationMs, 0, getErrorTypeRef?.(error));
          throw error;
        }
      },
    };

    // Wrap with ManagedMultipartUpload for proper cleanup and timeout handling
    return new ManagedMultipartUpload(
      metricsWrappedUpload,
      {
        timeoutMs: DEFAULT_MULTIPART_TIMEOUT_MS,
        onTimeout: () => {
          metricsRef?.recordR2Operation('multipartTimeout', 0, 0, 'timeout');
        },
      },
      // Cleanup callback to release captured closure references
      () => {
        metricsRef = null;
        getErrorTypeRef = null;
      }
    );
  }

  async getStream(key: string): Promise<ReadableStream<Uint8Array> | null> {
    const start = performance.now();
    try {
      const result = await this.backend.getStream(key);
      const durationMs = performance.now() - start;

      this.metrics.recordR2Operation('getStream', durationMs);
      return result;
    } catch (error) {
      const durationMs = performance.now() - start;
      this.metrics.recordR2Operation('getStream', durationMs, 0, this.getErrorType(error));
      throw error;
    }
  }

  async putStream(key: string, stream: ReadableStream<Uint8Array>): Promise<void> {
    const start = performance.now();
    try {
      await this.backend.putStream(key, stream);
      const durationMs = performance.now() - start;

      this.metrics.recordR2Operation('putStream', durationMs);
    } catch (error) {
      const durationMs = performance.now() - start;
      this.metrics.recordR2Operation('putStream', durationMs, 0, this.getErrorType(error));
      throw error;
    }
  }

  /**
   * Extract error type for metrics categorization.
   */
  private getErrorType(error: unknown): string {
    if (error instanceof Error) {
      if (error.name === 'InvalidStorageKeyError') return 'invalid_key';
      if (error.message.includes('not found') || error.message.includes('NotFound')) return 'not_found';
      if (error.message.includes('timeout') || error.message.includes('Timeout')) return 'timeout';
      if (error.message.includes('permission') || error.message.includes('forbidden')) return 'permission';
      return 'unknown';
    }
    return 'unknown';
  }

  /**
   * Get the underlying storage backend.
   */
  getBackend(): StorageBackend {
    return this.backend;
  }
}

/**
 * Create a metrics-enabled storage backend.
 *
 * @param config - MongoLake configuration
 * @param metrics - MetricsCollector instance for recording metrics (required for explicit dependency injection)
 * @returns Storage backend with metrics collection
 */
export function createStorageWithMetrics(
  config: MongoLakeConfig,
  metrics: MetricsCollector
): StorageBackend {
  const backend = createStorage(config);
  return new MetricsStorageWrapper(backend, metrics);
}
