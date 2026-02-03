/**
 * Replica ShardDO Types
 *
 * Type definitions for read replica Durable Objects that sync from primary ShardDOs.
 *
 * ## Portability
 *
 * This module uses Cloudflare-specific types for backward compatibility.
 * For portable code, use the abstraction layer types which can be implemented
 * for any platform:
 *
 * ```typescript
 * import type { ObjectStorageBackend, CoordinatorNamespace } from '@mongolake/abstractions';
 * ```
 */

import type { R2Bucket, DurableObjectNamespace } from '@cloudflare/workers-types';
import type { AnalyticsEngineDataset } from '../../metrics/index.js';
import type { ObjectStorage } from './types.js';

// Re-export for consumers who want portable types
export type { ObjectStorage } from './types.js';

// ============================================================================
// Environment & Configuration Types
// ============================================================================

/**
 * Environment bindings required by ReplicaShardDO.
 *
 * This interface uses Cloudflare-specific types for backward compatibility.
 * The actual implementation can work with any ObjectStorage implementation.
 */
export interface ReplicaShardDOEnv {
  /**
   * Object storage for reading Parquet data files.
   * Cloudflare: R2Bucket
   * Portable: Any ObjectStorage implementation
   */
  DATA_BUCKET: R2Bucket | ObjectStorage;

  /**
   * Coordinator namespace for fetching WAL updates from primary.
   * Cloudflare: DurableObjectNamespace
   * Portable: Any CoordinatorNamespace implementation
   */
  SHARD_DO: DurableObjectNamespace;

  /** Optional Workers Analytics Engine dataset for metrics */
  ANALYTICS?: AnalyticsEngineDataset;
}

/**
 * Configuration options for ReplicaShardDO behavior.
 */
export interface ReplicaConfig {
  /** How often to poll primary for WAL updates (milliseconds) */
  syncIntervalMs?: number;

  /** Maximum staleness allowed before failing reads (milliseconds) */
  maxStalenessMs?: number;

  /** Maximum number of WAL entries to fetch per sync */
  maxWalBatchSize?: number;

  /** Whether to enable local caching of documents */
  enableLocalCache?: boolean;

  /** Cache TTL for documents (milliseconds) */
  cacheTtlMs?: number;
}

/**
 * Default replica configuration values.
 */
export const DEFAULT_REPLICA_CONFIG: Required<ReplicaConfig> = {
  syncIntervalMs: 1000,
  maxStalenessMs: 5000,
  maxWalBatchSize: 1000,
  enableLocalCache: true,
  cacheTtlMs: 5000,
};

// ============================================================================
// Replication State Types
// ============================================================================

/**
 * Represents the replication state of a replica.
 */
export interface ReplicationState {
  /** ID of the primary shard being replicated */
  primaryShardId: string;

  /** Last applied LSN from primary WAL */
  appliedLSN: number;

  /** Timestamp of last successful sync */
  lastSyncTimestamp: number;

  /** Number of consecutive sync failures */
  consecutiveFailures: number;

  /** Current replication lag in milliseconds */
  lagMs: number;

  /** Replica health status */
  status: ReplicaStatus;
}

/**
 * Replica health status.
 */
export type ReplicaStatus = 'healthy' | 'lagging' | 'stale' | 'disconnected';

/**
 * WAL entry received from primary for replication.
 */
export interface ReplicationWalEntry {
  /** Log Sequence Number */
  lsn: number;

  /** Collection name */
  collection: string;

  /** Operation type: insert (i), update (u), or delete (d) */
  op: 'i' | 'u' | 'd';

  /** Document identifier */
  docId: string;

  /** Document data at this version */
  document: Record<string, unknown>;

  /** Timestamp when the entry was created */
  timestamp: number;
}

/**
 * Response from primary when fetching WAL entries.
 */
export interface WalFetchResponse {
  /** WAL entries since requested LSN */
  entries: ReplicationWalEntry[];

  /** Current LSN at primary (may be higher than last entry) */
  currentLSN: number;

  /** Primary shard's timestamp for lag calculation */
  timestamp: number;
}

// ============================================================================
// Query Types
// ============================================================================

/**
 * Options for replica find operations.
 */
export interface ReplicaFindOptions {
  /** Field projection */
  projection?: Record<string, 0 | 1>;

  /** Sort order */
  sort?: Record<string, 1 | -1>;

  /** Maximum number of documents to return */
  limit?: number;

  /** Number of documents to skip */
  skip?: number;

  /** Maximum staleness acceptable in milliseconds */
  maxStalenessMs?: number;

  /** If true, allow stale reads even when lagging */
  allowStale?: boolean;
}

/**
 * Result of a replica read operation.
 */
export interface ReplicaReadResult<T> {
  /** The documents or document returned */
  data: T;

  /** The LSN this read was served at */
  atLSN: number;

  /** Current staleness in milliseconds */
  stalenessMs: number;

  /** Whether the result may be stale */
  isStale: boolean;

  /** Replica status at time of read */
  status: ReplicaStatus;
}

// ============================================================================
// Internal Types
// ============================================================================

/**
 * Cached document entry.
 */
export interface CachedDocument {
  /** The document data */
  document: Record<string, unknown>;

  /** LSN when this version was cached */
  lsn: number;

  /** Timestamp when cached */
  cachedAt: number;
}

/**
 * Sync result from a replication cycle.
 */
export interface SyncResult {
  /** Whether sync was successful */
  success: boolean;

  /** Number of entries applied */
  entriesApplied: number;

  /** New applied LSN after sync */
  newLSN: number;

  /** Error message if sync failed */
  error?: string;
}
