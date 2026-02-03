/**
 * ShardDO Shared Types
 *
 * Type definitions shared across shard modules.
 *
 * ## Portability
 *
 * This module uses Cloudflare-specific types for the environment bindings
 * (R2Bucket, DurableObjectNamespace) to maintain backward compatibility.
 * For portable code, use the abstraction layer:
 *
 * ```typescript
 * import type { ObjectStorageBackend, CoordinatorNamespace } from '@mongolake/abstractions';
 * ```
 *
 * The StorageBackend interface in this module is already abstracted and can be
 * implemented for non-Cloudflare platforms.
 */

import type { DurableObjectState, R2Bucket, DurableObjectNamespace } from '@cloudflare/workers-types';
import type { AnalyticsEngineDataset } from '../../metrics/index.js';

// Re-export abstraction types for convenience
export type {
  ObjectStorageBackend,
  CoordinatorState,
  CoordinatorNamespace,
  CoordinatorStub,
} from '../../abstractions/index.js';

// ============================================================================
// Environment & Configuration Types
// ============================================================================

/**
 * Environment bindings required by ShardDO.
 *
 * These bindings must be configured in wrangler.toml:
 *
 * ```toml
 * [[r2_buckets]]
 * binding = "DATA_BUCKET"
 * bucket_name = "mongolake-data"
 *
 * [[durable_objects.bindings]]
 * name = "SHARD_DO"
 * class_name = "ShardDO"
 * ```
 */
export interface ShardDOEnv {
  /**
   * R2 bucket for storing Parquet data files.
   * Files are organized as: `{collection}/{date}/{fileId}.parquet`
   */
  DATA_BUCKET: R2Bucket;

  /**
   * Durable Object namespace for shard instances.
   * Used for routing and cross-shard operations.
   */
  SHARD_DO: DurableObjectNamespace;

  /**
   * Optional Workers Analytics Engine dataset for metrics.
   */
  ANALYTICS?: AnalyticsEngineDataset;
}

/**
 * Configuration options for ShardDO behavior.
 *
 * These settings control when automatic flushing and compaction occur.
 */
export interface ShardConfig {
  /**
   * Buffer size in bytes that triggers automatic flush to R2.
   * @default 1048576 (1MB)
   */
  flushThresholdBytes?: number;

  /**
   * Document count that triggers automatic flush to R2.
   * @default 10000
   */
  flushThresholdDocs?: number;

  /**
   * Maximum buffer size in bytes before back-pressure triggers auto-flush.
   * When the buffer exceeds this limit, writes will automatically trigger
   * a flush before adding more data to prevent OOM conditions.
   * @default 104857600 (100MB)
   */
  maxBytes?: number;

  /**
   * Minimum age in milliseconds before a file is eligible for compaction.
   * Set to 0 for immediate eligibility.
   * @default 0
   */
  compactionMinAge?: number;

  /**
   * Maximum number of files to compact in a single cycle.
   * Larger values improve read performance but increase compaction time.
   * @default 10
   */
  compactionBatchSize?: number;
}

// ============================================================================
// Write Operation Types
// ============================================================================

/**
 * Represents a write operation to be executed against a collection.
 *
 * Supports three operation types:
 * - `insert`: Add a new document (requires `document` with `_id`)
 * - `update`: Modify an existing document (requires `filter` and `update`)
 * - `delete`: Remove a document (requires `filter`)
 */
export interface WriteOperation {
  /** Target collection name */
  collection: string;

  /** Operation type: insert, update, or delete */
  op: 'insert' | 'update' | 'delete';

  /**
   * Document to insert (required for insert operations).
   * Must include an `_id` field.
   */
  document?: Record<string, unknown>;

  /**
   * Filter to identify target document (required for update/delete).
   * Currently supports equality matching on `_id`.
   */
  filter?: Record<string, unknown>;

  /**
   * Update operators to apply (required for update operations).
   * Supports MongoDB-style operators: $set, $unset, $inc, $push, $pull, etc.
   */
  update?: Record<string, unknown>;
}

/**
 * Result returned from a write operation.
 *
 * Contains acknowledgment status, the assigned document ID (for inserts),
 * and a read token for causal consistency.
 */
export interface WriteResult {
  /** Whether the write was successfully acknowledged */
  acknowledged: boolean;

  /** Document ID assigned (only present for insert operations) */
  insertedId?: string;

  /** Log Sequence Number assigned to this write */
  lsn: number;

  /**
   * Read token for causal consistency.
   * Format: `{shardId}:{lsn}`
   * Pass to subsequent reads via `afterToken` option.
   */
  readToken: string;
}

// ============================================================================
// Read Token Types
// ============================================================================

/**
 * Parsed read token containing shard and LSN information.
 *
 * Read tokens are used to ensure causal consistency - a read with
 * an afterToken will only see data at or after that LSN.
 */
export interface ReadToken {
  /** Shard identifier this token belongs to */
  shardId: string;

  /** Log Sequence Number position */
  lsn: number;
}

// ============================================================================
// Query Types
// ============================================================================

/**
 * Options for find and findOne query operations.
 */
export interface FindOptions {
  /**
   * Field projection to include (1) or exclude (0) fields.
   * Cannot mix inclusion and exclusion (except for _id).
   */
  projection?: Record<string, 0 | 1>;

  /**
   * Sort order for results. Use 1 for ascending, -1 for descending.
   */
  sort?: Record<string, 1 | -1>;

  /** Maximum number of documents to return */
  limit?: number;

  /** Number of documents to skip (for pagination) */
  skip?: number;

  /**
   * Read token for causal consistency.
   * Ensures the read sees all writes up to this token's LSN.
   */
  afterToken?: string;
}

// ============================================================================
// Storage Types
// ============================================================================

/**
 * Zone map statistics for a single field in a file.
 * Tracks min/max values to enable predicate pushdown filtering.
 */
export interface FileZoneMapEntry {
  /** Field path (supports dot notation for nested fields) */
  field: string;
  /** Minimum value in the file for this field */
  min: string | number | boolean | null;
  /** Maximum value in the file for this field */
  max: string | number | boolean | null;
  /** Number of null values */
  nullCount: number;
  /** Total number of rows */
  rowCount: number;
}

/**
 * Metadata for a Parquet file stored in R2.
 *
 * Used by the manifest to track files and their LSN ranges.
 */
export interface FileMetadata {
  /** R2 object path: `{collection}/{date}/{fileId}.parquet` */
  path: string;

  /** File size in bytes */
  size: number;

  /** Number of documents (rows) in the file */
  rowCount: number;

  /** Minimum LSN of documents in this file */
  minLSN: number;

  /** Maximum LSN of documents in this file */
  maxLSN: number;

  /** Unix timestamp when file was created */
  createdAt: number;

  /**
   * Zone map statistics for key fields (optional).
   * Enables predicate pushdown to skip files that can't contain matching documents.
   */
  zoneMap?: FileZoneMapEntry[];
}

/**
 * Pending flush record for two-phase commit pattern.
 *
 * Written to R2 (`_pending/{flushId}.json`) before the data block
 * and manifest update to ensure atomicity. During recovery, if a
 * pending flush exists:
 * - If data block exists: complete the manifest update
 * - If data block missing: discard (rollback)
 */
export interface PendingFlush {
  /** Unique identifier for this flush operation */
  flushId: string;

  /** Collection being flushed */
  collection: string;

  /** Metadata for the file being written */
  file: FileMetadata;

  /** Unix timestamp when flush started */
  timestamp: number;
}

/**
 * Manifest tracking all Parquet files for a collection.
 *
 * Stored in SQLite for durability and used to locate files
 * during queries and compaction.
 */
export interface CollectionManifest {
  /** Collection name */
  collection: string;

  /** List of Parquet files in this collection */
  files: FileMetadata[];

  /** Unix timestamp of last manifest update */
  updatedAt: number;
}

// ============================================================================
// Internal Types
// ============================================================================

/**
 * Internal representation of a document in the in-memory buffer.
 *
 * Contains both the document data and metadata needed for
 * ordering, deduplication, and operation type tracking.
 *
 * @internal
 */
export interface BufferedDoc {
  /** Document identifier */
  _id: string;

  /** Sequence number (same as LSN) for ordering */
  _seq: number;

  /** Operation type: insert (i), update (u), or delete (d) */
  _op: 'i' | 'u' | 'd';

  /** Collection this document belongs to */
  collection: string;

  /** The actual document data */
  document: Record<string, unknown>;

  /** Log Sequence Number when this version was written */
  lsn: number;
}

/**
 * Write-Ahead Log entry for durability.
 *
 * WAL entries are persisted to SQLite before writes are acknowledged,
 * ensuring durability across DO restarts.
 *
 * @internal
 */
export interface WalEntry {
  /** Log Sequence Number - unique, monotonically increasing */
  lsn: number;

  /** Collection this entry belongs to */
  collection: string;

  /** Operation type: insert (i), update (u), or delete (d) */
  op: 'i' | 'u' | 'd';

  /** Document identifier */
  docId: string;

  /** Document data at this version */
  document: Record<string, unknown>;

  /** Whether this entry has been flushed to R2 */
  flushed: boolean;
}

// ============================================================================
// Object Storage Interface (for Parquet files)
// ============================================================================

/**
 * Object storage interface for Parquet file operations.
 *
 * This interface abstracts R2/S3-style object storage for storing
 * Parquet files and manifests. It can be implemented using:
 * - Cloudflare R2
 * - AWS S3
 * - Google Cloud Storage
 * - Local filesystem
 *
 * For the full abstraction with additional methods, see:
 * `@mongolake/abstractions/ObjectStorageBackend`
 */
export interface ObjectStorage {
  /** Get object by key, returns body for reading */
  get(key: string): Promise<ObjectStorageBody | null>;

  /** Get object metadata without body */
  head(key: string): Promise<{ key: string; size: number; etag: string } | null>;

  /** Put an object */
  put(key: string, value: ReadableStream | ArrayBuffer | Uint8Array | string | Blob | null): Promise<{ key: string; size: number; etag: string }>;

  /** Delete an object */
  delete(key: string): Promise<void>;

  /** List objects with optional prefix */
  list(options?: { prefix?: string; limit?: number; cursor?: string }): Promise<{
    objects: Array<{ key: string; size: number; etag: string }>;
    truncated: boolean;
    cursor?: string;
  }>;

  /** Create a multipart upload for large files */
  createMultipartUpload(key: string): Promise<ObjectStorageMultipartUpload>;
}

/**
 * Body of a retrieved object.
 */
export interface ObjectStorageBody {
  /** Get as ArrayBuffer */
  arrayBuffer(): Promise<ArrayBuffer>;

  /** Get as text string */
  text(): Promise<string>;

  /** Get as parsed JSON */
  json<T>(): Promise<T>;

  /** Streaming body */
  body: ReadableStream;

  /** ETag for caching */
  etag: string;
}

/**
 * Multipart upload interface.
 */
export interface ObjectStorageMultipartUpload {
  /** Upload a part */
  uploadPart(partNumber: number, value: ArrayBuffer | Uint8Array): Promise<{ partNumber: number; etag: string }>;

  /** Complete the upload */
  complete(parts: Array<{ partNumber: number; etag: string }>): Promise<{ key: string; size: number; etag: string }>;

  /** Abort and cleanup */
  abort(): Promise<void>;
}

// ============================================================================
// Storage Backend Interface (for SQLite/WAL)
// ============================================================================

/**
 * Storage backend interface for dependency injection.
 *
 * Abstracts the Durable Object state to allow for testing
 * and alternative storage implementations.
 */
export interface StorageBackend {
  /** Execute a SQL query */
  sqlExec(query: string, ...args: unknown[]): SqlCursor;

  /** Get the shard ID */
  getShardId(): string;

  /** Block concurrent operations */
  blockConcurrencyWhile<T>(fn: () => Promise<T>): Promise<T>;

  /** Set an alarm for delayed execution */
  setAlarm(timestamp: number): Promise<void>;
}

/**
 * SQL cursor result from query execution.
 */
export interface SqlCursor {
  toArray(): Array<Record<string, unknown>>;
}

/**
 * Adapter to wrap DurableObjectState as StorageBackend.
 *
 * This is the Cloudflare-specific implementation of StorageBackend.
 * For alternative platforms, implement StorageBackend directly using
 * your platform's SQLite and alarm capabilities.
 *
 * ## Alternative Implementations
 *
 * To support a non-Cloudflare platform:
 *
 * 1. Implement SqlCursor with your SQL results
 * 2. Implement sqlExec with your SQLite/SQL database
 * 3. Implement alarm scheduling with your platform's scheduler
 * 4. Implement concurrency control (mutex/lock for single-threaded execution)
 *
 * Example for a Redis-based implementation:
 *
 * ```typescript
 * class RedisStorageBackend implements StorageBackend {
 *   constructor(
 *     private redis: Redis,
 *     private shardId: string,
 *     private sqlite: BetterSqlite3.Database
 *   ) {}
 *
 *   sqlExec(query: string, ...args: unknown[]): SqlCursor {
 *     const stmt = this.sqlite.prepare(query);
 *     return { toArray: () => stmt.all(...args) };
 *   }
 *
 *   getShardId(): string { return this.shardId; }
 *
 *   async blockConcurrencyWhile<T>(fn: () => Promise<T>): Promise<T> {
 *     const lock = await this.redis.lock(`shard:${this.shardId}`);
 *     try { return await fn(); }
 *     finally { await lock.release(); }
 *   }
 *
 *   async setAlarm(timestamp: number): Promise<void> {
 *     await this.redis.zadd('alarms', timestamp, this.shardId);
 *   }
 * }
 * ```
 */
export class DurableObjectStorageBackend implements StorageBackend {
  constructor(private state: DurableObjectState) {}

  sqlExec(query: string, ...args: unknown[]): SqlCursor {
    return this.state.storage.sql.exec(query, ...args);
  }

  getShardId(): string {
    return this.state.id.toString();
  }

  blockConcurrencyWhile<T>(fn: () => Promise<T>): Promise<T> {
    return this.state.blockConcurrencyWhile(fn);
  }

  async setAlarm(timestamp: number): Promise<void> {
    await this.state.storage.setAlarm(timestamp);
  }

  /**
   * Get the underlying DurableObjectState.
   *
   * Use this for Cloudflare-specific operations not covered by the abstraction.
   * Note: Using this breaks portability.
   */
  getDurableObjectState(): DurableObjectState {
    return this.state;
  }
}
