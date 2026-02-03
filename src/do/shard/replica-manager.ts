/**
 * ReplicaManager - Primary ShardDO Replica Tracking
 *
 * Manages read replicas for a primary ShardDO, including:
 * - Replica registration and deregistration
 * - Read preference routing
 * - Health monitoring and lag detection
 * - WAL-based replication coordination
 */

import type { DurableObjectNamespace, DurableObjectStub } from '@cloudflare/workers-types';

// ============================================================================
// Types
// ============================================================================

/**
 * Information about a registered replica.
 */
export interface ReplicaInfo {
  /** Unique replica identifier */
  id: string;

  /** Endpoint URL for the replica DO */
  endpoint: string;

  /** When the replica was registered */
  registeredAt: number;

  /** Last known LSN applied by the replica */
  lastKnownLSN: number;

  /** Timestamp of last heartbeat from replica */
  lastHeartbeat: number;

  /** Current lag in milliseconds (estimated) */
  lagMs: number;

  /** Current replica health status */
  status: ReplicaHealthStatus;

  /** Number of consecutive failed health checks */
  consecutiveFailures: number;

  /** Replica metadata (region, priority, etc.) */
  metadata?: Record<string, unknown>;
}

/**
 * Replica health status.
 */
export type ReplicaHealthStatus = 'healthy' | 'lagging' | 'stale' | 'unhealthy' | 'unknown';

/**
 * Read preference options for query routing.
 */
export type ReadPreference =
  | 'primary'           // Only read from primary
  | 'primaryPreferred'  // Prefer primary, fallback to secondary
  | 'secondary'         // Only read from secondaries
  | 'secondaryPreferred' // Prefer secondary, fallback to primary
  | 'nearest';          // Read from lowest latency node

/**
 * Options for selecting a replica for reads.
 */
export interface ReplicaSelectionOptions {
  /** Read preference mode */
  preference: ReadPreference;

  /** Maximum acceptable staleness in milliseconds */
  maxStalenessMs?: number;

  /** Tag set for filtering replicas */
  tags?: Record<string, string>;

  /** Whether to allow reads from unhealthy replicas */
  allowUnhealthy?: boolean;
}

/**
 * Configuration for ReplicaManager.
 */
export interface ReplicaManagerConfig {
  /** Interval for health checks in milliseconds */
  healthCheckIntervalMs?: number;

  /** Maximum lag before replica is considered "lagging" */
  lagThresholdMs?: number;

  /** Maximum lag before replica is considered "stale" */
  staleThresholdMs?: number;

  /** Heartbeat timeout before marking replica unhealthy */
  heartbeatTimeoutMs?: number;

  /** Maximum number of consecutive failures before removal */
  maxConsecutiveFailures?: number;

  /** Whether to auto-remove unhealthy replicas */
  autoRemoveUnhealthy?: boolean;
}

/**
 * Result of a replica sync operation.
 */
export interface ReplicaSyncResult {
  /** Whether sync was successful */
  success: boolean;

  /** Number of WAL entries sent */
  entriesSent: number;

  /** Starting LSN of sent entries */
  fromLSN: number;

  /** Ending LSN of sent entries */
  toLSN: number;

  /** Error message if sync failed */
  error?: string;
}

// ============================================================================
// Default Configuration
// ============================================================================

export const DEFAULT_REPLICA_MANAGER_CONFIG: Required<ReplicaManagerConfig> = {
  healthCheckIntervalMs: 5000,
  lagThresholdMs: 1000,
  staleThresholdMs: 10000,
  heartbeatTimeoutMs: 30000,
  maxConsecutiveFailures: 5,
  autoRemoveUnhealthy: false,
};

// ============================================================================
// ReplicaManager Implementation
// ============================================================================

/**
 * ReplicaManager tracks and coordinates read replicas for a primary ShardDO.
 *
 * ## Architecture
 *
 * The primary ShardDO maintains a ReplicaManager that:
 * 1. Tracks registered replicas and their health
 * 2. Routes read requests based on read preference
 * 3. Coordinates WAL-based replication
 * 4. Monitors replica lag and health
 *
 * ## Usage
 *
 * ```typescript
 * const manager = new ReplicaManager(env.REPLICA_DO);
 *
 * // Register a replica
 * manager.registerReplica('replica-1', 'https://replica-1.example.com');
 *
 * // Get a replica for reading
 * const replicaId = manager.getReplicaForRead({ preference: 'secondary' });
 *
 * // Sync a replica with WAL entries
 * await manager.syncReplica('replica-1', 100);
 * ```
 */
export class ReplicaManager {
  /** Map of replica ID to replica info */
  private replicas: Map<string, ReplicaInfo> = new Map();

  /** Configuration */
  private config: Required<ReplicaManagerConfig>;

  /** Current primary LSN (updated externally) */
  private currentPrimaryLSN: number = 0;

  /** Timestamp of last health check */
  private lastHealthCheck: number = 0;

  /** Optional replica namespace for DO stubs */
  private replicaNamespace: DurableObjectNamespace | null = null;

  /** Cached replica stubs */
  private replicaStubs: Map<string, DurableObjectStub> = new Map();

  constructor(
    replicaNamespace?: DurableObjectNamespace,
    config: ReplicaManagerConfig = {}
  ) {
    this.replicaNamespace = replicaNamespace ?? null;
    this.config = { ...DEFAULT_REPLICA_MANAGER_CONFIG, ...config };
  }

  // ============================================================================
  // Replica Registration
  // ============================================================================

  /**
   * Register a new replica.
   *
   * @param id - Unique replica identifier
   * @param endpoint - Endpoint URL for the replica
   * @param metadata - Optional metadata (region, priority, etc.)
   */
  registerReplica(
    id: string,
    endpoint: string,
    metadata?: Record<string, unknown>
  ): void {
    if (this.replicas.has(id)) {
      // Update existing replica
      const existing = this.replicas.get(id);
      if (existing) {
        existing.endpoint = endpoint;
        existing.metadata = metadata;
        existing.lastHeartbeat = Date.now();
      }
      return;
    }

    const now = Date.now();
    const replica: ReplicaInfo = {
      id,
      endpoint,
      registeredAt: now,
      lastKnownLSN: 0,
      lastHeartbeat: now,
      lagMs: 0,
      status: 'unknown',
      consecutiveFailures: 0,
      metadata,
    };

    this.replicas.set(id, replica);
  }

  /**
   * Deregister a replica.
   *
   * @param id - Replica identifier to remove
   * @returns Whether the replica was found and removed
   */
  deregisterReplica(id: string): boolean {
    // Clean up cached stub
    this.replicaStubs.delete(id);
    return this.replicas.delete(id);
  }

  /**
   * Get information about a specific replica.
   *
   * @param id - Replica identifier
   * @returns Replica info or null if not found
   */
  getReplica(id: string): ReplicaInfo | null {
    const replica = this.replicas.get(id);
    return replica ? { ...replica } : null;
  }

  /**
   * Get all registered replicas.
   *
   * @returns Array of replica info objects
   */
  getAllReplicas(): ReplicaInfo[] {
    return Array.from(this.replicas.values()).map(r => ({ ...r }));
  }

  /**
   * Get count of registered replicas.
   */
  getReplicaCount(): number {
    return this.replicas.size;
  }

  // ============================================================================
  // Read Preference Routing
  // ============================================================================

  /**
   * Select a replica for read operations based on read preference.
   *
   * @param options - Selection options including preference and staleness
   * @returns Replica ID or null if no suitable replica found
   */
  getReplicaForRead(options: ReplicaSelectionOptions): string | null {
    const { preference, maxStalenessMs, tags, allowUnhealthy } = options;
    const effectiveMaxStaleness = maxStalenessMs ?? this.config.staleThresholdMs;

    // Filter replicas based on criteria
    const candidates = this.filterReplicas(effectiveMaxStaleness, tags, allowUnhealthy);

    switch (preference) {
      case 'primary':
        // Primary only - return null (caller should use primary)
        return null;

      case 'primaryPreferred':
        // Prefer primary, but return a secondary if primary is unavailable
        // Since this is called on the primary, return null to use primary
        // In a real implementation, this would check primary health
        return null;

      case 'secondary':
        // Secondary only - must have a candidate
        if (candidates.length === 0) {
          return null;
        }
        return this.selectBestReplica(candidates);

      case 'secondaryPreferred':
        // Prefer secondary, fallback to primary
        if (candidates.length === 0) {
          return null; // Caller should use primary
        }
        return this.selectBestReplica(candidates);

      case 'nearest':
        // Return lowest latency node (including primary as option)
        if (candidates.length === 0) {
          return null; // Use primary
        }
        return this.selectNearestReplica(candidates);

      default:
        return null;
    }
  }

  /**
   * Filter replicas based on staleness, tags, and health.
   */
  private filterReplicas(
    maxStalenessMs: number,
    tags?: Record<string, string>,
    allowUnhealthy?: boolean
  ): ReplicaInfo[] {
    const candidates: ReplicaInfo[] = [];

    for (const replica of this.replicas.values()) {
      // Check health status
      if (!allowUnhealthy && replica.status === 'unhealthy') {
        continue;
      }

      // Check staleness
      if (replica.lagMs > maxStalenessMs) {
        continue;
      }

      // Check tags
      if (tags) {
        // If tags are required but replica has no metadata, skip it
        if (!replica.metadata) {
          continue;
        }
        const matchesTags = Object.entries(tags).every(
          ([key, value]) => replica.metadata?.[key] === value
        );
        if (!matchesTags) {
          continue;
        }
      }

      candidates.push(replica);
    }

    return candidates;
  }

  /**
   * Select the best replica from candidates (lowest lag).
   */
  private selectBestReplica(candidates: ReplicaInfo[]): string {
    // Sort by lag (ascending) and pick the best
    candidates.sort((a, b) => {
      // Prefer healthy replicas
      if (a.status === 'healthy' && b.status !== 'healthy') return -1;
      if (b.status === 'healthy' && a.status !== 'healthy') return 1;
      // Then by lag
      return a.lagMs - b.lagMs;
    });

    const best = candidates[0];
    if (!best) {
      throw new Error('selectBestReplica called with empty candidates array');
    }
    return best.id;
  }

  /**
   * Select the nearest replica (lowest estimated latency).
   */
  private selectNearestReplica(candidates: ReplicaInfo[]): string {
    // For now, use lag as a proxy for latency
    // In a real implementation, this would use actual RTT measurements
    return this.selectBestReplica(candidates);
  }

  // ============================================================================
  // WAL Synchronization
  // ============================================================================

  /**
   * Sync a replica with WAL entries from the specified LSN.
   *
   * This method coordinates with the WalManager to send entries to the replica.
   *
   * @param id - Replica identifier
   * @param fromLSN - Starting LSN to sync from
   * @param getWalEntries - Callback to get WAL entries from primary
   * @returns Sync result
   */
  async syncReplica(
    id: string,
    fromLSN: number,
    getWalEntries?: (afterLSN: number, limit: number) => Promise<{
      entries: Array<{
        lsn: number;
        collection: string;
        op: 'i' | 'u' | 'd';
        docId: string;
        document: Record<string, unknown>;
        timestamp: number;
      }>;
      currentLSN: number;
      timestamp: number;
    }>
  ): Promise<ReplicaSyncResult> {
    const replica = this.replicas.get(id);
    if (!replica) {
      return {
        success: false,
        entriesSent: 0,
        fromLSN,
        toLSN: fromLSN,
        error: `Replica not found: ${id}`,
      };
    }

    try {
      // If no getWalEntries callback, we can't sync
      if (!getWalEntries) {
        return {
          success: false,
          entriesSent: 0,
          fromLSN,
          toLSN: fromLSN,
          error: 'No WAL entries provider available',
        };
      }

      // Get WAL entries
      const walData = await getWalEntries(fromLSN, 1000);

      if (walData.entries.length === 0) {
        // No new entries - replica is up to date
        this.updateReplicaLSN(id, fromLSN);
        return {
          success: true,
          entriesSent: 0,
          fromLSN,
          toLSN: fromLSN,
        };
      }

      // Send entries to replica
      const stub = this.getReplicaStub(id);
      if (stub) {
        const response = await stub.fetch(
          new Request('https://replica/sync', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ entries: walData.entries }),
          })
        );

        if (!response.ok) {
          throw new Error(`Replica sync failed: ${response.status}`);
        }
      }

      // Update replica state
      const lastEntry = walData.entries[walData.entries.length - 1];
      const toLSN = lastEntry ? lastEntry.lsn : fromLSN;
      if (lastEntry) {
        this.updateReplicaLSN(id, lastEntry.lsn);
      }

      return {
        success: true,
        entriesSent: walData.entries.length,
        fromLSN,
        toLSN,
      };
    } catch (error) {
      // Record failure
      replica.consecutiveFailures++;
      if (replica.consecutiveFailures >= this.config.maxConsecutiveFailures) {
        replica.status = 'unhealthy';
      }

      return {
        success: false,
        entriesSent: 0,
        fromLSN,
        toLSN: fromLSN,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  /**
   * Get or create a stub for a replica DO.
   */
  private getReplicaStub(id: string): DurableObjectStub | null {
    if (!this.replicaNamespace) {
      return null;
    }

    let stub = this.replicaStubs.get(id);
    if (!stub) {
      const doId = this.replicaNamespace.idFromName(id);
      stub = this.replicaNamespace.get(doId);
      this.replicaStubs.set(id, stub);
    }
    return stub;
  }

  /**
   * Update the last known LSN for a replica.
   *
   * @param id - Replica identifier
   * @param lsn - New LSN value
   */
  updateReplicaLSN(id: string, lsn: number): void {
    const replica = this.replicas.get(id);
    if (replica) {
      replica.lastKnownLSN = lsn;
      replica.lastHeartbeat = Date.now();
      this.updateReplicaLag(replica);
    }
  }

  // ============================================================================
  // Health Monitoring
  // ============================================================================

  /**
   * Record a heartbeat from a replica.
   *
   * @param id - Replica identifier
   * @param lsn - Current LSN at the replica
   * @param lagMs - Replica-reported lag in milliseconds
   */
  recordHeartbeat(id: string, lsn: number, lagMs?: number): void {
    const replica = this.replicas.get(id);
    if (!replica) return;

    replica.lastHeartbeat = Date.now();
    replica.lastKnownLSN = lsn;
    if (lagMs !== undefined) {
      replica.lagMs = lagMs;
    } else {
      this.updateReplicaLag(replica);
    }
    replica.consecutiveFailures = 0;
    replica.status = this.computeReplicaStatus(replica);
  }

  /**
   * Update the primary LSN for lag calculations.
   *
   * @param lsn - Current LSN at primary
   */
  updatePrimaryLSN(lsn: number): void {
    this.currentPrimaryLSN = lsn;

    // Update lag for all replicas
    for (const replica of this.replicas.values()) {
      this.updateReplicaLag(replica);
    }
  }

  /**
   * Run a health check on all replicas.
   *
   * Updates status based on heartbeat timeout and lag thresholds.
   */
  runHealthCheck(): void {
    const now = Date.now();
    this.lastHealthCheck = now;

    for (const replica of this.replicas.values()) {
      // Check heartbeat timeout
      const timeSinceHeartbeat = now - replica.lastHeartbeat;
      if (timeSinceHeartbeat > this.config.heartbeatTimeoutMs) {
        replica.status = 'unhealthy';
        replica.consecutiveFailures++;
        continue;
      }

      // Update status based on lag
      replica.status = this.computeReplicaStatus(replica);
    }

    // Auto-remove unhealthy replicas if configured
    if (this.config.autoRemoveUnhealthy) {
      for (const [id, replica] of this.replicas.entries()) {
        if (replica.consecutiveFailures >= this.config.maxConsecutiveFailures) {
          this.deregisterReplica(id);
        }
      }
    }
  }

  /**
   * Get health status summary for all replicas.
   */
  getHealthSummary(): {
    total: number;
    healthy: number;
    lagging: number;
    stale: number;
    unhealthy: number;
    unknown: number;
  } {
    const summary = {
      total: this.replicas.size,
      healthy: 0,
      lagging: 0,
      stale: 0,
      unhealthy: 0,
      unknown: 0,
    };

    for (const replica of this.replicas.values()) {
      switch (replica.status) {
        case 'healthy':
          summary.healthy++;
          break;
        case 'lagging':
          summary.lagging++;
          break;
        case 'stale':
          summary.stale++;
          break;
        case 'unhealthy':
          summary.unhealthy++;
          break;
        case 'unknown':
          summary.unknown++;
          break;
      }
    }

    return summary;
  }

  /**
   * Update lag for a replica based on LSN difference.
   */
  private updateReplicaLag(replica: ReplicaInfo): void {
    // Estimate lag based on LSN difference
    // This is a rough estimate - real lag depends on write rate
    const lsnDiff = this.currentPrimaryLSN - replica.lastKnownLSN;

    // Assume ~100 writes/second as baseline for lag estimation
    // In practice, this would be calibrated based on actual write rate
    const estimatedLagMs = lsnDiff * 10; // 10ms per LSN diff

    replica.lagMs = Math.max(0, estimatedLagMs);
    replica.status = this.computeReplicaStatus(replica);
  }

  /**
   * Compute replica status based on current state.
   */
  private computeReplicaStatus(replica: ReplicaInfo): ReplicaHealthStatus {
    if (replica.consecutiveFailures >= this.config.maxConsecutiveFailures) {
      return 'unhealthy';
    }

    const timeSinceHeartbeat = Date.now() - replica.lastHeartbeat;
    if (timeSinceHeartbeat > this.config.heartbeatTimeoutMs) {
      return 'unhealthy';
    }

    if (replica.lagMs > this.config.staleThresholdMs) {
      return 'stale';
    }

    if (replica.lagMs > this.config.lagThresholdMs) {
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
  configure(config: Partial<ReplicaManagerConfig>): void {
    this.config = { ...this.config, ...config };
  }

  /**
   * Get current configuration.
   */
  getConfig(): Required<ReplicaManagerConfig> {
    return { ...this.config };
  }

  // ============================================================================
  // Serialization
  // ============================================================================

  /**
   * Export replica state for persistence.
   */
  exportState(): {
    replicas: ReplicaInfo[];
    currentPrimaryLSN: number;
    lastHealthCheck: number;
  } {
    return {
      replicas: this.getAllReplicas(),
      currentPrimaryLSN: this.currentPrimaryLSN,
      lastHealthCheck: this.lastHealthCheck,
    };
  }

  /**
   * Import replica state from persistence.
   */
  importState(state: {
    replicas: ReplicaInfo[];
    currentPrimaryLSN: number;
    lastHealthCheck: number;
  }): void {
    this.replicas.clear();
    for (const replica of state.replicas) {
      this.replicas.set(replica.id, { ...replica });
    }
    this.currentPrimaryLSN = state.currentPrimaryLSN;
    this.lastHealthCheck = state.lastHealthCheck;
  }

  /**
   * Clear all replicas.
   */
  clear(): void {
    this.replicas.clear();
    this.replicaStubs.clear();
    this.currentPrimaryLSN = 0;
    this.lastHealthCheck = 0;
  }
}
