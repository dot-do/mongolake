/**
 * Object Storage Abstraction
 *
 * Platform-agnostic interface for object storage operations.
 * This abstraction supports:
 * - Cloudflare R2
 * - AWS S3
 * - Google Cloud Storage
 * - Azure Blob Storage
 * - Local filesystem (for development)
 * - In-memory (for testing)
 *
 * @module abstractions/storage
 */

/**
 * Metadata about a stored object.
 */
export interface ObjectStorageMetadata {
  /** Object key/path */
  key: string;

  /** Size in bytes */
  size: number;

  /** ETag for cache validation */
  etag: string;

  /** Last modified timestamp */
  lastModified?: Date;

  /** Custom metadata */
  customMetadata?: Record<string, string>;
}

/**
 * Options for listing objects.
 */
export interface ObjectStorageListOptions {
  /** Prefix to filter by */
  prefix?: string;

  /** Maximum number of objects to return */
  limit?: number;

  /** Cursor for pagination */
  cursor?: string;

  /** Delimiter for hierarchical listing */
  delimiter?: string;
}

/**
 * Result from listing objects.
 */
export interface ObjectStorageListResult {
  /** List of objects */
  objects: ObjectStorageMetadata[];

  /** Whether there are more results */
  truncated: boolean;

  /** Cursor for next page */
  cursor?: string;

  /** Common prefixes when using delimiter */
  delimitedPrefixes?: string[];
}

/**
 * A retrieved object with body and metadata.
 */
export interface ObjectStorageObject {
  /** Object metadata */
  metadata: ObjectStorageMetadata;

  /** Get the body as an ArrayBuffer */
  arrayBuffer(): Promise<ArrayBuffer>;

  /** Get the body as text */
  text(): Promise<string>;

  /** Get the body as JSON */
  json<T>(): Promise<T>;

  /** Get the body as a ReadableStream */
  body: ReadableStream<Uint8Array>;
}

/**
 * Information about an uploaded part in a multipart upload.
 */
export interface UploadedPartInfo {
  /** Part number (1-based) */
  partNumber: number;

  /** ETag of the uploaded part */
  etag: string;
}

/**
 * Interface for multipart uploads.
 *
 * Multipart uploads allow uploading large objects in parts,
 * improving reliability and enabling parallel uploads.
 */
export interface MultipartUploadBackend {
  /**
   * Upload a single part.
   *
   * @param partNumber - Part number (1-based, must be sequential)
   * @param data - Part data (minimum 5MB except for last part in most implementations)
   * @returns Information about the uploaded part
   */
  uploadPart(partNumber: number, data: ArrayBuffer | Uint8Array): Promise<UploadedPartInfo>;

  /**
   * Complete the multipart upload.
   *
   * @param parts - List of uploaded parts (must be in order)
   */
  complete(parts: UploadedPartInfo[]): Promise<void>;

  /**
   * Abort the multipart upload and delete uploaded parts.
   */
  abort(): Promise<void>;
}

/**
 * Platform-agnostic object storage interface.
 *
 * This interface abstracts the common operations needed for storing
 * and retrieving objects (Parquet files, manifests, etc.) in MongoLake.
 *
 * ## Implementation Notes
 *
 * When implementing this interface for a new platform:
 *
 * 1. **Keys** - Keys are paths like "collection/date/file.parquet". Implementations
 *    should handle path normalization as needed.
 *
 * 2. **Consistency** - MongoLake requires strong read-after-write consistency.
 *    Most cloud object stores provide this natively.
 *
 * 3. **Atomic Operations** - The put operation should be atomic (all-or-nothing).
 *    MongoLake uses two-phase commit for additional safety.
 *
 * 4. **Error Handling** - Implementations should throw appropriate errors that
 *    can be caught and handled by the storage layer.
 *
 * ## Example Implementation (S3)
 *
 * ```typescript
 * class S3Storage implements ObjectStorageBackend {
 *   constructor(private client: S3Client, private bucket: string) {}
 *
 *   async get(key: string): Promise<ObjectStorageObject | null> {
 *     const command = new GetObjectCommand({ Bucket: this.bucket, Key: key });
 *     const response = await this.client.send(command);
 *     // ... wrap response in ObjectStorageObject
 *   }
 *
 *   // ... implement other methods
 * }
 * ```
 */
export interface ObjectStorageBackend {
  /**
   * Get an object by key.
   *
   * @param key - Object key/path
   * @returns The object if found, null otherwise
   */
  get(key: string): Promise<ObjectStorageObject | null>;

  /**
   * Get only object metadata without the body.
   *
   * Use this for existence checks or when you only need metadata.
   *
   * @param key - Object key/path
   * @returns Metadata if found, null otherwise
   */
  head(key: string): Promise<ObjectStorageMetadata | null>;

  /**
   * Store an object.
   *
   * @param key - Object key/path
   * @param value - Object data
   * @param options - Optional metadata
   * @returns Metadata of the stored object
   */
  put(
    key: string,
    value: ReadableStream<Uint8Array> | ArrayBuffer | Uint8Array | string | Blob | null,
    options?: { customMetadata?: Record<string, string> }
  ): Promise<ObjectStorageMetadata>;

  /**
   * Delete an object.
   *
   * Implementations should be idempotent (no error if object doesn't exist).
   *
   * @param key - Object key/path
   */
  delete(key: string): Promise<void>;

  /**
   * List objects with optional filtering.
   *
   * @param options - Listing options
   * @returns List result with pagination support
   */
  list(options?: ObjectStorageListOptions): Promise<ObjectStorageListResult>;

  /**
   * Create a multipart upload for large objects.
   *
   * Use multipart uploads for objects larger than ~5MB for better reliability.
   *
   * @param key - Object key/path
   * @returns Multipart upload interface
   */
  createMultipartUpload(key: string): Promise<MultipartUploadBackend>;
}
