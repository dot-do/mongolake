/**
 * Replica Router
 *
 * Routes read operations between primary ShardDO and ReplicaShardDO
 * based on read preference and consistency requirements.
 */

import type { Document, Filter, FindOptions } from '../types.js';
import type { DurableObjectNamespace, DurableObjectStub } from '@cloudflare/workers-types';
import { LRUCache } from '../utils/lru-cache.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Read preference modes (MongoDB-compatible).
 */
export type ReadPreference =
  | 'primary'           // Always read from primary - strong consistency
  | 'primaryPreferred'  // Prefer primary, fallback to replica if unavailable
  | 'secondary'         // Read from replica only
  | 'secondaryPreferred' // Prefer replica, fallback to primary if unavailable
  | 'nearest';          // Read from lowest-latency node

/**
 * Read options with replica routing.
 */
export interface ReplicaReadOptions extends FindOptions {
  /** Read preference for routing */
  readPreference?: ReadPreference;

  /** Maximum staleness acceptable (for replica reads) */
  maxStalenessMs?: number;

  /** Allow stale reads even when exceeding staleness bounds */
  allowStale?: boolean;
}

/**
 * Result of a routed read operation.
 */
export interface RoutedReadResult<T> {
  /** The data returned */
  data: T;

  /** Source of the read */
  source: 'primary' | 'replica';

  /** Staleness in milliseconds (for replica reads) */
  stalenessMs?: number;

  /** Whether the data may be stale */
  isStale?: boolean;
}

/**
 * Replica pool configuration.
 */
export interface ReplicaPoolConfig {
  /** Number of replicas per shard */
  replicasPerShard: number;

  /** Whether replica routing is enabled */
  enabled: boolean;

  /** Default read preference */
  defaultReadPreference: ReadPreference;

  /** Default max staleness in milliseconds */
  defaultMaxStalenessMs: number;
}

/**
 * Default replica pool configuration.
 */
export const DEFAULT_REPLICA_POOL_CONFIG: ReplicaPoolConfig = {
  replicasPerShard: 2,
  enabled: false,
  defaultReadPreference: 'primaryPreferred',
  defaultMaxStalenessMs: 5000,
};

// ============================================================================
// Replica Router Implementation
// ============================================================================

/**
 * Routes read operations to primary or replica based on configuration.
 */
export class ReplicaRouter {
  private config: ReplicaPoolConfig;
  private primaryNamespace: DurableObjectNamespace;
  private replicaNamespace: DurableObjectNamespace | null = null;

  /** Cache for replica stubs */
  private replicaStubs: LRUCache<string, DurableObjectStub>;

  /** Health status of replicas */
  private replicaHealth: Map<string, { healthy: boolean; lastCheck: number }> = new Map();

  /** Round-robin index per shard */
  private roundRobinIndex: Map<string, number> = new Map();

  constructor(
    primaryNamespace: DurableObjectNamespace,
    replicaNamespace?: DurableObjectNamespace,
    config: Partial<ReplicaPoolConfig> = {}
  ) {
    this.primaryNamespace = primaryNamespace;
    this.replicaNamespace = replicaNamespace ?? null;
    this.config = { ...DEFAULT_REPLICA_POOL_CONFIG, ...config };
    this.replicaStubs = new LRUCache({ maxSize: 100 });
  }

  // ============================================================================
  // Routing Logic
  // ============================================================================

  /**
   * Route a find operation based on read preference.
   */
  async find(
    shardId: string,
    collection: string,
    filter: Filter<Document>,
    options: ReplicaReadOptions = {}
  ): Promise<RoutedReadResult<Document[]>> {
    const preference = options.readPreference ?? this.config.defaultReadPreference;

    // If replicas not enabled or configured, always use primary
    if (!this.config.enabled || !this.replicaNamespace) {
      return this.readFromPrimary(shardId, collection, filter, options);
    }

    switch (preference) {
      case 'primary':
        return this.readFromPrimary(shardId, collection, filter, options);

      case 'secondary':
        return this.readFromReplica(shardId, collection, filter, options);

      case 'primaryPreferred':
        try {
          return await this.readFromPrimary(shardId, collection, filter, options);
        } catch {
          return this.readFromReplica(shardId, collection, filter, options);
        }

      case 'secondaryPreferred':
        try {
          const result = await this.readFromReplica(shardId, collection, filter, options);
          // If too stale, fallback to primary
          if (result.isStale && !options.allowStale) {
            return this.readFromPrimary(shardId, collection, filter, options);
          }
          return result;
        } catch {
          return this.readFromPrimary(shardId, collection, filter, options);
        }

      case 'nearest':
        // For now, use secondaryPreferred as a proxy for nearest
        // In production, this could use latency-based routing
        return this.find(shardId, collection, filter, {
          ...options,
          readPreference: 'secondaryPreferred',
        });

      default:
        return this.readFromPrimary(shardId, collection, filter, options);
    }
  }

  /**
   * Read from the primary ShardDO.
   */
  private async readFromPrimary(
    shardId: string,
    collection: string,
    filter: Filter<Document>,
    options: FindOptions
  ): Promise<RoutedReadResult<Document[]>> {
    const id = this.primaryNamespace.idFromName(shardId);
    const stub = this.primaryNamespace.get(id);

    const request = new Request('https://primary/find', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ collection, filter, ...options }),
    });

    const response = await stub.fetch(request);

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Primary read failed: ${response.status} ${errorText}`);
    }

    const data = await response.json() as { documents: Document[] };

    return {
      data: data.documents,
      source: 'primary',
    };
  }

  /**
   * Read from a replica ShardDO.
   */
  private async readFromReplica(
    shardId: string,
    collection: string,
    filter: Filter<Document>,
    options: ReplicaReadOptions
  ): Promise<RoutedReadResult<Document[]>> {
    if (!this.replicaNamespace) {
      throw new Error('Replica namespace not configured');
    }

    // Select a replica using round-robin
    const replicaIndex = this.getNextReplicaIndex(shardId);
    const replicaId = `${shardId}-replica-${replicaIndex}`;

    // Get or create replica stub
    let stub = this.replicaStubs.get(replicaId);
    if (!stub) {
      const id = this.replicaNamespace.idFromName(replicaId);
      stub = this.replicaNamespace.get(id);
      this.replicaStubs.set(replicaId, stub);
    }

    const request = new Request('https://replica/find', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        collection,
        filter,
        maxStalenessMs: options.maxStalenessMs ?? this.config.defaultMaxStalenessMs,
        allowStale: options.allowStale,
        projection: options.projection,
        sort: options.sort,
        limit: options.limit,
        skip: options.skip,
      }),
    });

    const response = await stub.fetch(request);

    if (!response.ok) {
      this.markReplicaUnhealthy(replicaId);
      const errorText = await response.text();
      throw new Error(`Replica read failed: ${response.status} ${errorText}`);
    }

    this.markReplicaHealthy(replicaId);

    const result = await response.json() as {
      data: Document[];
      stalenessMs: number;
      isStale: boolean;
    };

    return {
      data: result.data,
      source: 'replica',
      stalenessMs: result.stalenessMs,
      isStale: result.isStale,
    };
  }

  // ============================================================================
  // Replica Selection
  // ============================================================================

  /**
   * Get next replica index using round-robin.
   */
  private getNextReplicaIndex(shardId: string): number {
    const currentIndex = this.roundRobinIndex.get(shardId) ?? 0;
    const nextIndex = (currentIndex + 1) % this.config.replicasPerShard;
    this.roundRobinIndex.set(shardId, nextIndex);
    return currentIndex;
  }

  /**
   * Mark a replica as healthy.
   */
  private markReplicaHealthy(replicaId: string): void {
    this.replicaHealth.set(replicaId, { healthy: true, lastCheck: Date.now() });
  }

  /**
   * Mark a replica as unhealthy.
   */
  private markReplicaUnhealthy(replicaId: string): void {
    this.replicaHealth.set(replicaId, { healthy: false, lastCheck: Date.now() });
  }

  /**
   * Check if a replica is healthy.
   */
  isReplicaHealthy(replicaId: string): boolean {
    const health = this.replicaHealth.get(replicaId);
    if (!health) return true; // Assume healthy if unknown
    // Consider unhealthy for 30 seconds after failure
    if (!health.healthy && Date.now() - health.lastCheck < 30000) {
      return false;
    }
    return true;
  }

  // ============================================================================
  // Configuration
  // ============================================================================

  /**
   * Update configuration.
   */
  configure(config: Partial<ReplicaPoolConfig>): void {
    this.config = { ...this.config, ...config };
  }

  /**
   * Check if replica routing is enabled.
   */
  isEnabled(): boolean {
    return this.config.enabled && this.replicaNamespace !== null;
  }

  /**
   * Get pool status.
   */
  getPoolStatus(): {
    enabled: boolean;
    replicasPerShard: number;
    healthyReplicas: number;
    unhealthyReplicas: number;
  } {
    let healthy = 0;
    let unhealthy = 0;

    for (const [_, status] of this.replicaHealth) {
      if (status.healthy) {
        healthy++;
      } else {
        unhealthy++;
      }
    }

    return {
      enabled: this.isEnabled(),
      replicasPerShard: this.config.replicasPerShard,
      healthyReplicas: healthy,
      unhealthyReplicas: unhealthy,
    };
  }
}
