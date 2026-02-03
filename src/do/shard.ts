/**
 * ShardDO - Shard Durable Object (Re-export)
 *
 * This file re-exports from the modular implementation in src/do/shard/
 * for backwards compatibility with existing imports.
 *
 * The ShardDO is the write coordinator for a shard in MongoLake. It provides
 * a MongoDB-like document storage interface built on Cloudflare's Durable Objects,
 * R2 object storage, and SQLite.
 *
 * ## Module Structure
 *
 * The implementation is split into focused modules:
 * - `shard/types.ts` - Shared type definitions
 * - `shard/wal-manager.ts` - WAL operations and durability
 * - `shard/buffer-manager.ts` - In-memory buffer management
 * - `shard/index-manager.ts` - Manifest and file operations
 * - `shard/query-executor.ts` - Query execution
 * - `shard/compaction-service.ts` - Background compaction
 * - `shard/index.ts` - Main ShardDO class that composes all modules
 *
 * ## Architecture
 *
 * ShardDO implements a Log-Structured Merge (LSM) tree-inspired architecture:
 * - **In-memory buffer**: Fast writes accumulate in memory
 * - **Write-Ahead Log (WAL)**: SQLite-backed durability for crash recovery
 * - **R2 Storage**: Parquet files for persistent, queryable storage
 * - **Compaction**: Background process merges small files into larger ones
 *
 * ## Data Flow
 *
 * ```
 * Write Request
 *      |
 *      v
 * +------------------+
 * |  In-Memory       | <-- Fast path for reads
 * |  Buffer          |
 * +--------+---------+
 *          | (threshold exceeded)
 *          v
 * +------------------+
 * |  SQLite WAL      | <-- Durability guarantee
 * +--------+---------+
 *          | (flush)
 *          v
 * +------------------+
 * |  R2 Parquet      | <-- Long-term storage
 * |  Files           |
 * +--------+---------+
 *          | (compaction)
 *          v
 * +------------------+
 * |  Merged Parquet  | <-- Optimized for reads
 * |  Files           |
 * +------------------+
 * ```
 *
 * ## Usage Examples
 *
 * ### Basic CRUD Operations
 *
 * ```typescript
 * // Get shard stub from Durable Object namespace
 * const shardId = env.SHARD_DO.idFromName('shard-1');
 * const shard = env.SHARD_DO.get(shardId);
 *
 * // Insert a document
 * const insertResult = await shard.write({
 *   collection: 'users',
 *   op: 'insert',
 *   document: { _id: 'user-1', name: 'Alice', age: 30 }
 * });
 *
 * // Update a document
 * const updateResult = await shard.write({
 *   collection: 'users',
 *   op: 'update',
 *   filter: { _id: 'user-1' },
 *   update: { $set: { age: 31 } }
 * });
 *
 * // Query documents
 * const users = await shard.find('users', { age: { $gte: 18 } }, {
 *   projection: { name: 1, age: 1 },
 *   sort: { age: -1 },
 *   limit: 10
 * });
 * ```
 *
 * @see {@link ShardConfig} for configuration options
 * @see {@link WriteOperation} for write operation structure
 * @see {@link FindOptions} for query options
 * @see {@link WriteResult} for write operation results
 */

// Re-export everything from the modular implementation
export { ShardDO, DurableObjectStorageBackend } from './shard/index.js';
export type {
  ShardDOEnv,
  ShardConfig,
  WriteOperation,
  WriteResult,
  ReadToken,
  FindOptions,
  FileMetadata,
  PendingFlush,
  CollectionManifest,
  BufferedDoc,
  WalEntry,
  StorageBackend,
  SqlCursor,
} from './shard/index.js';

// Re-export replica components
export { ReplicaShardDO } from './shard/replica.js';
export { ReplicaSyncManager } from './shard/replica-sync-manager.js';
export { ReplicaBuffer } from './shard/replica-buffer.js';
export type {
  ReplicaShardDOEnv,
  ReplicaConfig,
  ReplicaFindOptions,
  ReplicaReadResult,
  ReplicationState,
  ReplicationWalEntry,
  WalFetchResponse,
  SyncResult,
  CachedDocument,
  ReplicaStatus,
} from './shard/replica-types.js';
export { DEFAULT_REPLICA_CONFIG } from './shard/replica-types.js';
