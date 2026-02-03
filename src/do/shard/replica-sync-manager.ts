/**
 * Replica Sync Manager
 *
 * Handles WAL synchronization between primary and replica ShardDOs.
 * Implements the core replication protocol using WAL-based streaming.
 *
 * ## Portability
 *
 * This module uses CoordinatorNamespace which abstracts the underlying
 * coordinator system. While it currently accepts DurableObjectNamespace
 * for backward compatibility, any CoordinatorNamespace implementation works.
 *
 * The sync protocol is HTTP-based, making it portable across platforms:
 * - Cloudflare Durable Objects (via stub.fetch)
 * - HTTP services (direct fetch)
 * - gRPC (with adapter)
 */

import type { DurableObjectNamespace, DurableObjectStub, DurableObjectId } from '@cloudflare/workers-types';
import type {
  ReplicationState,
  ReplicationWalEntry,
  WalFetchResponse,
  SyncResult,
  ReplicaConfig,
  ReplicaStatus,
} from './replica-types.js';
import { DEFAULT_REPLICA_CONFIG } from './replica-types.js';
import { logger } from '../../utils/logger.js';
import type { CoordinatorNamespace, CoordinatorStub } from '../../abstractions/coordination.js';

/**
 * Union type for coordinator namespace (portable or Cloudflare-specific).
 */
export type SyncManagerNamespace = DurableObjectNamespace | CoordinatorNamespace;

/**
 * Manages WAL synchronization for a replica shard.
 *
 * Responsibilities:
 * - Fetching WAL entries from primary
 * - Tracking replication state and lag
 * - Handling sync failures and reconnection
 * - Computing replica health status
 *
 * ## Architecture
 *
 * The sync manager uses a pull-based replication model:
 *
 * 1. Replica calls sync() periodically (via alarms)
 * 2. Sync manager fetches WAL entries from primary via HTTP
 * 3. Entries are applied locally via callback
 * 4. State (applied LSN, lag) is updated
 *
 * This model works across platforms because it only requires
 * HTTP-style communication (Request/Response).
 */
export class ReplicaSyncManager {
  /** Replication state */
  private state: ReplicationState;

  /** Configuration */
  private config: Required<ReplicaConfig>;

  /** Stub to primary ShardDO (portable interface) */
  private primaryStub: (DurableObjectStub | CoordinatorStub) | null = null;

  /** Pending sync in progress */
  private syncInProgress: boolean = false;

  /**
   * Create a ReplicaSyncManager.
   *
   * @param primaryShardId - ID of the primary shard to sync from
   * @param shardNamespace - Coordinator namespace (DurableObjectNamespace or CoordinatorNamespace)
   * @param config - Replica configuration
   */
  constructor(
    private readonly primaryShardId: string,
    private readonly shardNamespace: SyncManagerNamespace,
    config: ReplicaConfig = {}
  ) {
    this.config = { ...DEFAULT_REPLICA_CONFIG, ...config };
    this.state = {
      primaryShardId,
      appliedLSN: 0,
      lastSyncTimestamp: 0,
      consecutiveFailures: 0,
      lagMs: 0,
      status: 'disconnected',
    };
  }

  // ============================================================================
  // State Access
  // ============================================================================

  /**
   * Get current replication state.
   */
  getState(): ReplicationState {
    return { ...this.state };
  }

  /**
   * Get the current applied LSN.
   */
  getAppliedLSN(): number {
    return this.state.appliedLSN;
  }

  /**
   * Get current replica status.
   */
  getStatus(): ReplicaStatus {
    return this.computeStatus();
  }

  /**
   * Check if replica is healthy for serving reads.
   */
  isHealthy(maxStalenessMs?: number): boolean {
    const effectiveMaxStaleness = maxStalenessMs ?? this.config.maxStalenessMs;
    return this.state.lagMs <= effectiveMaxStaleness && this.state.status !== 'disconnected';
  }

  // ============================================================================
  // Sync Operations
  // ============================================================================

  /**
   * Perform a sync cycle with the primary.
   *
   * Fetches WAL entries since the last applied LSN and applies them locally.
   *
   * @param applyEntry - Callback to apply each WAL entry locally
   * @returns Sync result
   */
  async sync(
    applyEntry: (entry: ReplicationWalEntry) => void
  ): Promise<SyncResult> {
    // Prevent concurrent syncs
    if (this.syncInProgress) {
      return {
        success: false,
        entriesApplied: 0,
        newLSN: this.state.appliedLSN,
        error: 'Sync already in progress',
      };
    }

    this.syncInProgress = true;

    try {
      // Ensure we have a connection to primary
      const stub = this.getPrimaryStub();

      // Fetch WAL entries from primary
      const response = await this.fetchWalEntries(stub, this.state.appliedLSN);

      // Apply entries
      let entriesApplied = 0;
      for (const entry of response.entries) {
        try {
          applyEntry(entry);
          entriesApplied++;
          this.state.appliedLSN = entry.lsn;
        } catch (error) {
          logger.error('Failed to apply WAL entry during replica sync', {
            operation: 'ReplicaSyncManager.sync',
            lsn: entry.lsn,
            collection: entry.collection,
            error,
          });
          // Continue applying other entries
        }
      }

      // Update replication state
      this.state.lastSyncTimestamp = Date.now();
      this.state.lagMs = Date.now() - response.timestamp;
      this.state.consecutiveFailures = 0;
      this.state.status = this.computeStatus();

      return {
        success: true,
        entriesApplied,
        newLSN: this.state.appliedLSN,
      };
    } catch (error) {
      this.state.consecutiveFailures++;
      this.state.status = this.computeStatus();

      return {
        success: false,
        entriesApplied: 0,
        newLSN: this.state.appliedLSN,
        error: error instanceof Error ? error.message : String(error),
      };
    } finally {
      this.syncInProgress = false;
    }
  }

  /**
   * Force a full resync from a specific LSN.
   *
   * Use this to recover from corruption or reset replica state.
   */
  async resyncFrom(
    fromLSN: number,
    applyEntry: (entry: ReplicationWalEntry) => void
  ): Promise<SyncResult> {
    // Reset state
    this.state.appliedLSN = fromLSN;
    this.state.consecutiveFailures = 0;

    // Perform sync
    return this.sync(applyEntry);
  }

  // ============================================================================
  // Primary Communication
  // ============================================================================

  /**
   * Get or create stub to primary coordinator.
   *
   * Works with both DurableObjectNamespace and CoordinatorNamespace.
   */
  private getPrimaryStub(): DurableObjectStub | CoordinatorStub {
    if (!this.primaryStub) {
      const id = this.shardNamespace.idFromName(this.primaryShardId);
      // Cast through unknown since the namespace could be either type
      // Both provide the same get() interface
      this.primaryStub = (this.shardNamespace as DurableObjectNamespace).get(id as DurableObjectId) as DurableObjectStub | CoordinatorStub;
    }
    return this.primaryStub;
  }

  /**
   * Fetch WAL entries from primary since the given LSN.
   *
   * Uses HTTP-style communication that works with any coordinator type.
   */
  private async fetchWalEntries(
    stub: DurableObjectStub | CoordinatorStub,
    afterLSN: number
  ): Promise<WalFetchResponse> {
    const request = new Request('https://primary/wal', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        afterLSN,
        limit: this.config.maxWalBatchSize,
      }),
    });

    const response = await stub.fetch(request);

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Failed to fetch WAL: ${response.status} ${errorText}`);
    }

    const data = await response.json() as WalFetchResponse;
    return data;
  }

  // ============================================================================
  // Status Computation
  // ============================================================================

  /**
   * Compute current replica status based on state.
   */
  private computeStatus(): ReplicaStatus {
    // Check for disconnection
    if (this.state.consecutiveFailures >= 5) {
      return 'disconnected';
    }

    // Check staleness
    if (this.state.lastSyncTimestamp === 0) {
      return 'disconnected';
    }

    const timeSinceSync = Date.now() - this.state.lastSyncTimestamp;
    if (timeSinceSync > this.config.maxStalenessMs * 2) {
      return 'stale';
    }

    if (this.state.lagMs > this.config.maxStalenessMs) {
      return 'lagging';
    }

    return 'healthy';
  }

  // ============================================================================
  // Configuration
  // ============================================================================

  /**
   * Update configuration.
   */
  configure(config: Partial<ReplicaConfig>): void {
    this.config = { ...this.config, ...config };
  }

  /**
   * Get current configuration.
   */
  getConfig(): Required<ReplicaConfig> {
    return { ...this.config };
  }

  /**
   * Reset connection to primary (for reconnection).
   */
  resetConnection(): void {
    this.primaryStub = null;
    this.state.consecutiveFailures = 0;
  }

  /**
   * Get the primary shard ID.
   */
  getPrimaryShardId(): string {
    return this.primaryShardId;
  }

  /**
   * Check if a sync is currently in progress.
   */
  isSyncInProgress(): boolean {
    return this.syncInProgress;
  }
}
