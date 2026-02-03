/**
 * Cloudflare R2 Storage Implementation
 *
 * Implements ObjectStorageBackend using Cloudflare R2.
 *
 * @module abstractions/cloudflare/storage
 */

import type {
  ObjectStorageBackend,
  ObjectStorageMetadata,
  ObjectStorageListOptions,
  ObjectStorageListResult,
  ObjectStorageObject,
  MultipartUploadBackend,
  UploadedPartInfo,
} from '../storage.js';

// Use the R2 types from the project's types definition
// These are compatible with @cloudflare/workers-types but defined locally
import type {
  R2Bucket,
  R2ObjectBody,
  R2MultipartUpload,
} from '../../types.js';

/**
 * Wraps an R2 object in the ObjectStorageObject interface.
 */
class R2ObjectWrapper implements ObjectStorageObject {
  metadata: ObjectStorageMetadata;
  body: ReadableStream<Uint8Array>;

  private r2Object: R2ObjectBody;

  constructor(r2Object: R2ObjectBody, key: string) {
    this.r2Object = r2Object;
    this.body = r2Object.body;
    this.metadata = {
      key,
      size: 0, // R2ObjectBody doesn't expose size, need to get from head
      etag: r2Object.etag,
    };
  }

  async arrayBuffer(): Promise<ArrayBuffer> {
    return this.r2Object.arrayBuffer();
  }

  async text(): Promise<string> {
    return this.r2Object.text();
  }

  async json<T>(): Promise<T> {
    return this.r2Object.json<T>();
  }
}

/**
 * Wraps an R2 multipart upload.
 */
class R2MultipartUploadWrapper implements MultipartUploadBackend {
  constructor(private upload: R2MultipartUpload) {}

  async uploadPart(partNumber: number, data: ArrayBuffer | Uint8Array): Promise<UploadedPartInfo> {
    const part = await this.upload.uploadPart(partNumber, data);
    return {
      partNumber: part.partNumber,
      etag: part.etag,
    };
  }

  async complete(parts: UploadedPartInfo[]): Promise<void> {
    await this.upload.complete(
      parts.map((p) => ({ partNumber: p.partNumber, etag: p.etag }))
    );
  }

  async abort(): Promise<void> {
    await this.upload.abort();
  }
}

/**
 * Cloudflare R2 implementation of ObjectStorageBackend.
 *
 * This implementation wraps Cloudflare R2 to provide a platform-agnostic
 * storage interface. It can be replaced with S3, GCS, or other implementations
 * without changing the MongoLake core code.
 *
 * ## Configuration
 *
 * The R2 bucket is passed in via the constructor, typically from the
 * Cloudflare Worker environment bindings:
 *
 * ```typescript
 * const storage = new CloudflareR2Storage(env.DATA_BUCKET);
 * ```
 *
 * ## R2-Specific Behavior
 *
 * - Strong read-after-write consistency
 * - Automatic retry with exponential backoff
 * - Support for conditional operations (ETags)
 */
export class CloudflareR2Storage implements ObjectStorageBackend {
  constructor(private bucket: R2Bucket) {}

  async get(key: string): Promise<ObjectStorageObject | null> {
    const obj = await this.bucket.get(key);
    if (!obj) return null;
    return new R2ObjectWrapper(obj, key);
  }

  async head(key: string): Promise<ObjectStorageMetadata | null> {
    const obj = await this.bucket.head(key);
    if (!obj) return null;

    return {
      key: obj.key,
      size: obj.size,
      etag: obj.etag,
    };
  }

  async put(
    key: string,
    value: ReadableStream<Uint8Array> | ArrayBuffer | Uint8Array | string | Blob | null,
    _options?: { customMetadata?: Record<string, string> }
  ): Promise<ObjectStorageMetadata> {
    const result = await this.bucket.put(key, value);
    return {
      key: result.key,
      size: result.size,
      etag: result.etag,
    };
  }

  async delete(key: string): Promise<void> {
    await this.bucket.delete(key);
  }

  async list(options?: ObjectStorageListOptions): Promise<ObjectStorageListResult> {
    const result = await this.bucket.list({
      prefix: options?.prefix,
      limit: options?.limit,
      cursor: options?.cursor,
    });

    return {
      objects: result.objects.map((obj) => ({
        key: obj.key,
        size: obj.size,
        etag: obj.etag,
      })),
      truncated: result.truncated,
      cursor: result.cursor,
    };
  }

  async createMultipartUpload(key: string): Promise<MultipartUploadBackend> {
    const upload = await this.bucket.createMultipartUpload(key);
    return new R2MultipartUploadWrapper(upload);
  }

  /**
   * Get the underlying R2Bucket.
   *
   * Use this for R2-specific operations not covered by the abstraction.
   * Note: Using this breaks portability.
   */
  getR2Bucket(): R2Bucket {
    return this.bucket;
  }
}

/**
 * Factory function to create Cloudflare R2 storage.
 *
 * @param bucket - R2Bucket from environment bindings
 * @returns ObjectStorageBackend implementation
 */
export function createCloudflareStorage(bucket: R2Bucket): ObjectStorageBackend {
  return new CloudflareR2Storage(bucket);
}
