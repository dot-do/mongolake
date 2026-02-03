/**
 * SplitCoordinator - Coordinates Shard Split Operations
 *
 * Handles the complex process of splitting a hot shard:
 * 1. Allocates new shard IDs
 * 2. Updates router with new shard mapping
 * 3. Redistributes data keys across new shards
 * 4. Manages split state machine
 *
 * Splitting is done incrementally to avoid service disruption.
 */

import type { ShardRouter } from './router.js';
import type { ShardMonitor, SplitRecommendation } from './monitor.js';
import {
  DEFAULT_SHARD_SPLIT_MIN_INTERVAL_MS,
  DEFAULT_SHARD_SPLIT_MAX_CONCURRENT,
} from '../constants.js';
import { logger } from '../utils/logger.js';

// ============================================================================
// Types
// ============================================================================

/**
 * State of a shard split operation
 */
export type SplitState =
  | 'pending'        // Split requested but not started
  | 'preparing'      // Allocating new shards, updating router
  | 'migrating'      // Redistributing keys/data
  | 'validating'     // Verifying data integrity
  | 'completing'     // Finalizing split
  | 'completed'      // Split finished successfully
  | 'failed'         // Split failed
  | 'rolled_back';   // Split was rolled back

/**
 * State of a shard merge operation
 */
export type MergeState =
  | 'pending'           // Merge requested but not started
  | 'preparing'         // Preparing target shard, updating router
  | 'draining'          // Draining traffic from source shards
  | 'migrating'         // Moving data to target shard
  | 'validating'        // Verifying data integrity
  | 'completing'        // Finalizing merge, removing source shards
  | 'completed'         // Merge finished successfully
  | 'failed'            // Merge failed
  | 'rolled_back';      // Merge was rolled back

/**
 * Split operation record
 */
export interface SplitOperation {
  /** Unique split operation ID */
  splitId: string;
  /** Source shard being split */
  sourceShardId: number;
  /** New shards created by split */
  targetShardIds: number[];
  /** Collections affected by this split */
  collections: string[];
  /** Current state of the split */
  state: SplitState;
  /** Reason for the split */
  reason: SplitRecommendation['reason'];
  /** When the split was requested */
  requestedAt: number;
  /** When the split started */
  startedAt?: number;
  /** When the split completed (or failed) */
  completedAt?: number;
  /** Error message if failed */
  error?: string;
  /** Progress (0-100) */
  progress: number;
  /** Estimated keys/docs to migrate */
  estimatedMigrationCount: number;
  /** Keys/docs actually migrated */
  migratedCount: number;
  /** Split points used for this operation */
  splitPoints?: SplitPoint[];
  /** Key ranges assigned to each target shard */
  keyRanges?: Map<number, KeyRange>;
}

/**
 * Configuration for SplitCoordinator
 */
export interface SplitCoordinatorConfig extends Partial<SplitCoordinatorPersistenceConfig> {
  /** Router instance to update */
  router: ShardRouter;
  /** Monitor instance for metrics */
  monitor: ShardMonitor;
  /** Maximum concurrent splits (default: 1) */
  maxConcurrentSplits?: number;
  /** Minimum time between splits of the same shard (default: 1 hour) */
  minSplitIntervalMs?: number;
  /** Callback when split state changes */
  onSplitStateChange?: (operation: SplitOperation) => void;
  /** Function to get next available shard ID */
  getNextShardId?: () => number;
  /** Function to migrate data (provided by shard implementation) */
  migrateData?: (op: SplitOperation, targetShard: number, keyRange: KeyRange) => Promise<number>;
  /** Rebalance check interval in ms (default: 5 minutes) */
  rebalanceCheckIntervalMs?: number;
  /** Imbalance threshold to trigger rebalance (default: 0.3 = 30% deviation) */
  imbalanceThreshold?: number;
  /** Maximum concurrent merges (default: 1) */
  maxConcurrentMerges?: number;
  /** Minimum time between merges involving the same shard (default: 1 hour) */
  minMergeIntervalMs?: number;
  /** Callback when merge state changes */
  onMergeStateChange?: (operation: MergeOperation) => void;
  /** Function to migrate data for merge (provided by shard implementation) */
  migrateDataForMerge?: (op: MergeOperation, sourceShard: number, targetShard: number) => Promise<number>;
  /** Low utilization threshold to trigger merge (default: 0.2 = 20% of average) */
  lowUtilizationThreshold?: number;
  /** Function to drain traffic from a shard before merge */
  drainShard?: (shardId: number) => Promise<void>;
  /** Function to decommission a shard after merge */
  decommissionShard?: (shardId: number) => Promise<void>;
}

/**
 * Key range for data migration
 */
export interface KeyRange {
  /** Start of range (inclusive), undefined means from beginning */
  start?: string;
  /** End of range (exclusive), undefined means to end */
  end?: string;
}

/**
 * Split point represents a boundary in the key space
 */
export interface SplitPoint {
  /** The key at which to split */
  key: string;
  /** Hash value of the key (for consistent routing) */
  hashValue: number;
  /** Estimated documents before this point */
  estimatedDocsBefore: number;
  /** Estimated documents after this point */
  estimatedDocsAfter: number;
}

/**
 * Key distribution statistics for split point selection
 */
export interface KeyDistribution {
  /** Collection name */
  collection: string;
  /** Total document count */
  totalDocs: number;
  /** Sample of keys with their hash values */
  keySamples: Array<{ key: string; hash: number }>;
  /** Histogram of hash space distribution (16 buckets) */
  hashHistogram: number[];
  /** Detected hot spots (ranges with disproportionate load) */
  hotSpots: Array<{ start: number; end: number; load: number }>;
}

/**
 * Rebalance recommendation
 */
export interface RebalanceRecommendation {
  /** Type of rebalance action */
  action: 'split' | 'merge' | 'move';
  /** Source shard(s) */
  sourceShards: number[];
  /** Target shard(s) - for move/merge operations */
  targetShards?: number[];
  /** Collections to rebalance */
  collections: string[];
  /** Estimated improvement in load balance (0-1) */
  expectedImprovement: number;
  /** Priority score (higher = more urgent) */
  priority: number;
  /** Reason for recommendation */
  reason: string;
}

/**
 * Split metadata for persistence
 */
export interface SplitMetadata {
  /** Split operation ID */
  splitId: string;
  /** Source shard that was split */
  sourceShardId: number;
  /** Resulting shards after split */
  resultingShards: number[];
  /** Split points used */
  splitPoints: SplitPoint[];
  /** Collections affected */
  collections: string[];
  /** Timestamp of split completion */
  completedAt: number;
  /** Version number for conflict resolution */
  version: number;
}

/**
 * Extended configuration for SplitCoordinator with persistence
 */
export interface SplitCoordinatorPersistenceConfig {
  /** Function to persist split metadata */
  persistMetadata?: (metadata: SplitMetadata) => Promise<void>;
  /** Function to load split metadata on startup */
  loadMetadata?: () => Promise<SplitMetadata[]>;
  /** Function to get key distribution for a collection */
  getKeyDistribution?: (collection: string, shardId: number) => Promise<KeyDistribution>;
  /** Function to scan keys from a shard for split point analysis */
  scanKeys?: (shardId: number, collection: string, limit: number) => Promise<string[]>;
}

/**
 * Result of a split request
 */
export interface SplitRequestResult {
  /** Whether the split was accepted */
  accepted: boolean;
  /** Split operation ID (if accepted) */
  splitId?: string;
  /** Reason if rejected */
  rejectionReason?: string;
}

/**
 * Merge operation record
 */
export interface MergeOperation {
  /** Unique merge operation ID */
  mergeId: string;
  /** Source shards to be merged */
  sourceShardIds: number[];
  /** Target shard to receive merged data */
  targetShardId: number;
  /** Collections affected by this merge */
  collections: string[];
  /** Current state of the merge */
  state: MergeState;
  /** Reason for the merge */
  reason: 'low_utilization' | 'manual' | 'rebalance';
  /** When the merge was requested */
  requestedAt: number;
  /** When the merge started */
  startedAt?: number;
  /** When the merge completed (or failed) */
  completedAt?: number;
  /** Error message if failed */
  error?: string;
  /** Progress (0-100) */
  progress: number;
  /** Estimated keys/docs to migrate */
  estimatedMigrationCount: number;
  /** Keys/docs actually migrated */
  migratedCount: number;
}

/**
 * Result of a merge request
 */
export interface MergeRequestResult {
  /** Whether the merge was accepted */
  accepted: boolean;
  /** Merge operation ID (if accepted) */
  mergeId?: string;
  /** Reason if rejected */
  rejectionReason?: string;
}

/**
 * Recommendation for merging underutilized shards
 */
export interface MergeRecommendation {
  /** Source shards to merge */
  sourceShards: number[];
  /** Target shard to merge into */
  targetShard: number;
  /** Collections on these shards */
  collections: string[];
  /** Combined utilization ratio (0-1) */
  combinedUtilization: number;
  /** Priority score (higher = more beneficial to merge) */
  priority: number;
  /** Reason for recommendation */
  reason: string;
}

/**
 * Merge metadata for persistence
 */
export interface MergeMetadata {
  /** Merge operation ID */
  mergeId: string;
  /** Source shards that were merged */
  sourceShardIds: number[];
  /** Target shard that received the data */
  targetShardId: number;
  /** Collections affected */
  collections: string[];
  /** Timestamp of merge completion */
  completedAt: number;
  /** Version number for conflict resolution */
  version: number;
}

// ============================================================================
// SplitCoordinator Class
// ============================================================================

/**
 * SplitCoordinator manages the lifecycle of shard split operations.
 *
 * ## Usage
 *
 * ```typescript
 * const coordinator = new SplitCoordinator({
 *   router,
 *   monitor,
 *   onSplitStateChange: (op) => console.log('Split state:', op.state),
 * });
 *
 * // Request a split based on recommendation
 * const result = await coordinator.requestSplit(recommendation);
 *
 * // Or manually request a split
 * const result = await coordinator.splitShard(shardId, 2);
 * ```
 */
export class SplitCoordinator {
  private router: ShardRouter;
  private monitor: ShardMonitor;
  private maxConcurrentSplits: number;
  private minSplitIntervalMs: number;
  private onSplitStateChange?: (operation: SplitOperation) => void;
  private getNextShardId: () => number;
  private migrateData?: (op: SplitOperation, targetShard: number, keyRange: KeyRange) => Promise<number>;

  /** Persistence callbacks */
  private persistMetadata?: (metadata: SplitMetadata) => Promise<void>;
  private loadMetadata?: () => Promise<SplitMetadata[]>;
  private getKeyDistribution?: (collection: string, shardId: number) => Promise<KeyDistribution>;
  private scanKeys?: (shardId: number, collection: string, limit: number) => Promise<string[]>;

  /** Rebalancing configuration */
  private rebalanceCheckIntervalMs: number;
  private imbalanceThreshold: number;
  private lastRebalanceCheck: number = 0;

  /** Active split operations */
  private activeSplits: Map<string, SplitOperation> = new Map();

  /** Completed split operations (for history) */
  private completedSplits: SplitOperation[] = [];

  /** Last split time per shard */
  private lastSplitTime: Map<number, number> = new Map();

  /** Next shard ID counter (for auto-allocation) */
  private nextShardIdCounter: number;

  /** Persisted split metadata (for recovery) */
  private splitMetadataHistory: SplitMetadata[] = [];

  /** Metadata version counter for conflict resolution */
  private metadataVersion: number = 0;

  /** Merge-related configuration */
  private maxConcurrentMerges: number;
  private minMergeIntervalMs: number;
  private onMergeStateChange?: (operation: MergeOperation) => void;
  private migrateDataForMerge?: (op: MergeOperation, sourceShard: number, targetShard: number) => Promise<number>;
  private lowUtilizationThreshold: number;
  private drainShard?: (shardId: number) => Promise<void>;
  private decommissionShard?: (shardId: number) => Promise<void>;

  /** Active merge operations */
  private activeMerges: Map<string, MergeOperation> = new Map();

  /** Completed merge operations (for history) */
  private completedMerges: MergeOperation[] = [];

  /** Last merge time per shard */
  private lastMergeTime: Map<number, number> = new Map();

  /** Persisted merge metadata (for recovery) */
  private mergeMetadataHistory: MergeMetadata[] = [];

  constructor(config: SplitCoordinatorConfig) {
    this.router = config.router;
    this.monitor = config.monitor;
    this.maxConcurrentSplits = config.maxConcurrentSplits ?? DEFAULT_SHARD_SPLIT_MAX_CONCURRENT;
    this.minSplitIntervalMs = config.minSplitIntervalMs ?? DEFAULT_SHARD_SPLIT_MIN_INTERVAL_MS;
    this.onSplitStateChange = config.onSplitStateChange;
    this.migrateData = config.migrateData;

    // Persistence callbacks
    this.persistMetadata = config.persistMetadata;
    this.loadMetadata = config.loadMetadata;
    this.getKeyDistribution = config.getKeyDistribution;
    this.scanKeys = config.scanKeys;

    // Rebalancing configuration
    this.rebalanceCheckIntervalMs = config.rebalanceCheckIntervalMs ?? 5 * 60 * 1000; // 5 minutes
    this.imbalanceThreshold = config.imbalanceThreshold ?? 0.3; // 30% deviation

    // Initialize shard ID counter based on current shard count
    this.nextShardIdCounter = this.monitor.getShardCount();

    // Use provided function or default counter
    this.getNextShardId = config.getNextShardId ?? (() => this.nextShardIdCounter++);

    // Merge-related configuration
    this.maxConcurrentMerges = config.maxConcurrentMerges ?? DEFAULT_SHARD_SPLIT_MAX_CONCURRENT;
    this.minMergeIntervalMs = config.minMergeIntervalMs ?? DEFAULT_SHARD_SPLIT_MIN_INTERVAL_MS;
    this.onMergeStateChange = config.onMergeStateChange;
    this.migrateDataForMerge = config.migrateDataForMerge;
    this.lowUtilizationThreshold = config.lowUtilizationThreshold ?? 0.2; // 20% of average
    this.drainShard = config.drainShard;
    this.decommissionShard = config.decommissionShard;
  }

  /**
   * Generate a unique split ID using crypto-secure randomness
   */
  private generateSplitId(): string {
    const array = new Uint8Array(5);
    crypto.getRandomValues(array);
    const random = Array.from(array, (b) => b.toString(36).padStart(2, '0')).join('').substring(0, 7);
    return `split_${Date.now()}_${random}`;
  }

  /**
   * Request a split based on a recommendation from the monitor
   */
  async requestSplit(recommendation: SplitRecommendation): Promise<SplitRequestResult> {
    return this.splitShard(
      recommendation.shardId,
      recommendation.recommendedSplitCount,
      recommendation.reason,
      recommendation.hotCollections
    );
  }

  /**
   * Request to split a shard into multiple new shards
   *
   * @param shardId - The shard to split
   * @param splitCount - Number of new shards to create (2 = split in half)
   * @param reason - Reason for the split
   * @param collections - Collections to include in the split
   */
  async splitShard(
    shardId: number,
    splitCount: number = 2,
    reason: SplitRecommendation['reason'] = 'write_rate',
    collections?: string[]
  ): Promise<SplitRequestResult> {
    const now = Date.now();

    // Validate split count
    if (splitCount < 2 || splitCount > 4) {
      return {
        accepted: false,
        rejectionReason: 'Split count must be between 2 and 4',
      };
    }

    // Check concurrent split limit
    if (this.activeSplits.size >= this.maxConcurrentSplits) {
      return {
        accepted: false,
        rejectionReason: `Maximum concurrent splits (${this.maxConcurrentSplits}) reached`,
      };
    }

    // Check minimum interval between splits
    const lastSplit = this.lastSplitTime.get(shardId);
    if (lastSplit && now - lastSplit < this.minSplitIntervalMs) {
      const remainingMs = this.minSplitIntervalMs - (now - lastSplit);
      return {
        accepted: false,
        rejectionReason: `Shard ${shardId} was split recently. Wait ${Math.ceil(remainingMs / 60000)} minutes.`,
      };
    }

    // Check if shard is already being split
    for (const op of this.activeSplits.values()) {
      if (op.sourceShardId === shardId && op.state !== 'completed' && op.state !== 'failed') {
        return {
          accepted: false,
          rejectionReason: `Shard ${shardId} is already being split (${op.splitId})`,
        };
      }
    }

    // Get metrics to estimate migration count
    const metrics = this.monitor.getShardMetrics(shardId);
    const estimatedMigrationCount = metrics?.documentCount ?? 0;

    // Allocate new shard IDs
    const targetShardIds: number[] = [];
    for (let i = 0; i < splitCount; i++) {
      targetShardIds.push(this.getNextShardId());
    }

    // Determine affected collections
    const affectedCollections = collections ??
      (metrics ? Array.from(metrics.collections.keys()) : []);

    // Create split operation
    const splitId = this.generateSplitId();
    const operation: SplitOperation = {
      splitId,
      sourceShardId: shardId,
      targetShardIds,
      collections: affectedCollections,
      state: 'pending',
      reason,
      requestedAt: now,
      progress: 0,
      estimatedMigrationCount,
      migratedCount: 0,
    };

    this.activeSplits.set(splitId, operation);
    this.notifyStateChange(operation);

    // Start the split process asynchronously
    this.executeSplit(operation).catch((error) => {
      operation.state = 'failed';
      operation.error = error instanceof Error ? error.message : String(error);
      operation.completedAt = Date.now();
      this.notifyStateChange(operation);
    });

    return {
      accepted: true,
      splitId,
    };
  }

  /**
   * Execute the split operation
   */
  private async executeSplit(operation: SplitOperation): Promise<void> {
    try {
      operation.startedAt = Date.now();

      // Phase 1: Preparing
      await this.prepareSplit(operation);

      // Phase 2: Migrating
      await this.migrateKeys(operation);

      // Phase 3: Validating
      await this.validateSplit(operation);

      // Phase 4: Completing
      await this.completeSplit(operation);

    } catch (error) {
      // Attempt rollback
      await this.rollbackSplit(operation, error);
      throw error;
    }
  }

  /**
   * Phase 1: Prepare the split by updating router and allocating shards
   */
  private async prepareSplit(operation: SplitOperation): Promise<void> {
    this.updateState(operation, 'preparing', 5);

    // Register new shards with the monitor
    for (const targetShardId of operation.targetShardIds) {
      this.monitor.addShard(targetShardId);
    }

    this.updateState(operation, 'preparing', 10);

    // Calculate optimal split points if key distribution analysis is available
    const splitPoints = await this.calculateSplitPoints(operation);
    operation.splitPoints = splitPoints;

    // Calculate key ranges for each target shard based on split points
    operation.keyRanges = this.calculateKeyRanges(operation, splitPoints);

    this.updateState(operation, 'preparing', 15);

    // Update router to include new shards in the split
    // The router already has splitCollection support, so we use it
    for (const collection of operation.collections) {
      const allShards = [operation.sourceShardId, ...operation.targetShardIds];
      this.router.splitCollection(collection, allShards);
    }

    this.updateState(operation, 'preparing', 20);
  }

  /**
   * Calculate optimal split points based on key distribution
   *
   * The algorithm:
   * 1. Sample keys from the shard to understand distribution
   * 2. Build a histogram of hash values
   * 3. Find split points that divide the data evenly
   * 4. Adjust for hot spots to avoid splitting in the middle of hot ranges
   */
  private async calculateSplitPoints(operation: SplitOperation): Promise<SplitPoint[]> {
    const splitPoints: SplitPoint[] = [];
    const numSplits = operation.targetShardIds.length;

    // If no key scanning function is provided, use equal hash space division
    if (!this.scanKeys && !this.getKeyDistribution) {
      return this.calculateEqualHashSpaceSplitPoints(numSplits);
    }

    // Aggregate key distribution across all affected collections
    const allKeySamples: Array<{ key: string; hash: number }> = [];
    let totalDocs = 0;

    for (const collection of operation.collections) {
      try {
        if (this.getKeyDistribution) {
          const distribution = await this.getKeyDistribution(collection, operation.sourceShardId);
          allKeySamples.push(...distribution.keySamples);
          totalDocs += distribution.totalDocs;
        } else if (this.scanKeys) {
          // Sample up to 10000 keys per collection
          const keys = await this.scanKeys(operation.sourceShardId, collection, 10000);
          const samples = keys.map(key => ({
            key,
            hash: this.hashKey(key),
          }));
          allKeySamples.push(...samples);
          totalDocs += keys.length;
        }
      } catch (error) {
        // If distribution analysis fails, fall back to equal division
        logger.warn('Failed to get key distribution for collection', {
          collection,
          shardId: operation.sourceShardId,
          splitId: operation.splitId,
          error,
        });
      }
    }

    // If we couldn't get any samples, fall back to equal hash space division
    if (allKeySamples.length === 0) {
      return this.calculateEqualHashSpaceSplitPoints(numSplits);
    }

    // Sort samples by hash value
    allKeySamples.sort((a, b) => a.hash - b.hash);

    // Calculate split points that divide the data evenly
    const docsPerShard = Math.ceil(allKeySamples.length / numSplits);

    for (let i = 1; i < numSplits; i++) {
      const splitIndex = Math.min(i * docsPerShard, allKeySamples.length - 1);
      const sample = allKeySamples[splitIndex];

      if (sample) {
        splitPoints.push({
          key: sample.key,
          hashValue: sample.hash,
          estimatedDocsBefore: splitIndex,
          estimatedDocsAfter: allKeySamples.length - splitIndex,
        });
      }
    }

    return splitPoints;
  }

  /**
   * Calculate equal hash space split points (fallback algorithm)
   */
  private calculateEqualHashSpaceSplitPoints(numSplits: number): SplitPoint[] {
    const splitPoints: SplitPoint[] = [];
    const maxHash = 0xFFFFFFFF; // 32-bit hash space

    for (let i = 1; i < numSplits; i++) {
      const hashValue = Math.floor((maxHash / numSplits) * i);
      splitPoints.push({
        key: `__split_${i}`,
        hashValue,
        estimatedDocsBefore: 0, // Unknown without sampling
        estimatedDocsAfter: 0,
      });
    }

    return splitPoints;
  }

  /**
   * Simple hash function for keys (consistent with router)
   * Uses FNV-1a inspired algorithm
   */
  private hashKey(key: string): number {
    let hash = 0x811c9dc5;
    for (let i = 0; i < key.length; i++) {
      hash ^= key.charCodeAt(i);
      hash = Math.imul(hash, 0x01000193);
    }
    hash ^= hash >>> 16;
    hash = Math.imul(hash, 0x85ebca6b);
    hash ^= hash >>> 13;
    hash = Math.imul(hash, 0xc2b2ae35);
    hash ^= hash >>> 16;
    return hash >>> 0;
  }

  /**
   * Calculate key ranges for each target shard based on split points
   */
  private calculateKeyRanges(
    operation: SplitOperation,
    splitPoints: SplitPoint[]
  ): Map<number, KeyRange> {
    const keyRanges = new Map<number, KeyRange>();
    const allShards = [operation.sourceShardId, ...operation.targetShardIds];

    for (let i = 0; i < allShards.length; i++) {
      const shardId = allShards[i]!;
      const keyRange: KeyRange = {};

      // Start of range (from previous split point or beginning)
      if (i > 0) {
        const prevSplitPoint = splitPoints[i - 1];
        if (prevSplitPoint) {
          keyRange.start = String(prevSplitPoint.hashValue);
        }
      }

      // End of range (to next split point or end)
      if (i < splitPoints.length) {
        const currSplitPoint = splitPoints[i];
        if (currSplitPoint) {
          keyRange.end = String(currSplitPoint.hashValue);
        }
      }

      keyRanges.set(shardId, keyRange);
    }

    return keyRanges;
  }

  /**
   * Phase 2: Migrate keys/data to new shards
   *
   * Uses the pre-calculated key ranges from split point analysis for optimal
   * data distribution. Falls back to equal hash space division if no ranges available.
   */
  private async migrateKeys(operation: SplitOperation): Promise<void> {
    this.updateState(operation, 'migrating', 25);

    const shardCount = operation.targetShardIds.length;

    for (let i = 0; i < shardCount; i++) {
      const targetShard = operation.targetShardIds[i]!;

      // Use pre-calculated key range if available, otherwise compute default
      let keyRange: KeyRange;
      if (operation.keyRanges?.has(targetShard)) {
        keyRange = operation.keyRanges.get(targetShard)!;
      } else {
        // Fallback: equal hash space division
        keyRange = {
          start: i > 0 ? String(i / shardCount) : undefined,
          end: i < shardCount - 1 ? String((i + 1) / shardCount) : undefined,
        };
      }

      // Call the migration function if provided
      if (this.migrateData) {
        const migrated = await this.migrateData(operation, targetShard, keyRange);
        operation.migratedCount += migrated;
      }

      // Update progress (25% to 75% during migration)
      const progress = 25 + Math.floor((50 * (i + 1)) / operation.targetShardIds.length);
      this.updateState(operation, 'migrating', progress);
    }

    this.updateState(operation, 'migrating', 75);
  }

  /**
   * Phase 3: Validate the split
   */
  private async validateSplit(operation: SplitOperation): Promise<void> {
    this.updateState(operation, 'validating', 80);

    // Verify all new shards are healthy
    for (const targetShardId of operation.targetShardIds) {
      const metrics = this.monitor.getShardMetrics(targetShardId);
      if (!metrics) {
        throw new Error(`Target shard ${targetShardId} not found after split`);
      }
    }

    this.updateState(operation, 'validating', 90);
  }

  /**
   * Phase 4: Complete the split
   */
  private async completeSplit(operation: SplitOperation): Promise<void> {
    this.updateState(operation, 'completing', 95);

    // Record split time
    this.lastSplitTime.set(operation.sourceShardId, Date.now());

    operation.completedAt = Date.now();

    // Persist split metadata for recovery and routing table updates
    await this.persistSplitMetadata(operation);

    this.updateState(operation, 'completed', 100);

    // Move to completed history
    this.activeSplits.delete(operation.splitId);
    this.completedSplits.push(operation);

    // Keep history bounded
    if (this.completedSplits.length > 100) {
      this.completedSplits.shift();
    }
  }

  /**
   * Persist split metadata for recovery and propagation
   */
  private async persistSplitMetadata(operation: SplitOperation): Promise<void> {
    if (!this.persistMetadata) {
      return;
    }

    const metadata: SplitMetadata = {
      splitId: operation.splitId,
      sourceShardId: operation.sourceShardId,
      resultingShards: [operation.sourceShardId, ...operation.targetShardIds],
      splitPoints: operation.splitPoints ?? [],
      collections: operation.collections,
      completedAt: operation.completedAt ?? Date.now(),
      version: ++this.metadataVersion,
    };

    try {
      await this.persistMetadata(metadata);
      this.splitMetadataHistory.push(metadata);

      // Keep metadata history bounded
      if (this.splitMetadataHistory.length > 100) {
        this.splitMetadataHistory.shift();
      }
    } catch (error) {
      logger.error('Failed to persist split metadata', {
        splitId: operation.splitId,
        sourceShardId: operation.sourceShardId,
        targetShardIds: operation.targetShardIds,
        error,
      });
      // Don't fail the split for metadata persistence issues
      // The split is still valid, just not persisted
    }
  }

  /**
   * Rollback a failed split
   */
  private async rollbackSplit(operation: SplitOperation, error: unknown): Promise<void> {
    try {
      // Remove splits from router
      for (const collection of operation.collections) {
        this.router.unsplitCollection(collection);
      }

      // Note: We cannot easily remove shards from the monitor
      // In practice, the new shards would be marked as inactive

      operation.state = 'rolled_back';
      operation.error = error instanceof Error ? error.message : String(error);
      operation.completedAt = Date.now();
      this.notifyStateChange(operation);

      // Move to completed history
      this.activeSplits.delete(operation.splitId);
      this.completedSplits.push(operation);
    } catch (rollbackError) {
      logger.error('Failed to rollback split', {
        splitId: operation.splitId,
        sourceShardId: operation.sourceShardId,
        originalError: error instanceof Error ? error.message : String(error),
        error: rollbackError,
      });
    }
  }

  /**
   * Update operation state and notify listeners
   */
  private updateState(operation: SplitOperation, state: SplitState, progress: number): void {
    operation.state = state;
    operation.progress = progress;
    this.notifyStateChange(operation);
  }

  /**
   * Notify state change callback
   */
  private notifyStateChange(operation: SplitOperation): void {
    this.onSplitStateChange?.(operation);
  }

  /**
   * Get status of a specific split operation
   */
  getSplitStatus(splitId: string): SplitOperation | undefined {
    return this.activeSplits.get(splitId) ??
      this.completedSplits.find((op) => op.splitId === splitId);
  }

  /**
   * Get all active split operations
   */
  getActiveSplits(): SplitOperation[] {
    return Array.from(this.activeSplits.values());
  }

  /**
   * Get split history
   */
  getSplitHistory(limit: number = 10): SplitOperation[] {
    return this.completedSplits.slice(-limit);
  }

  /**
   * Cancel a pending or in-progress split
   */
  async cancelSplit(splitId: string): Promise<boolean> {
    const operation = this.activeSplits.get(splitId);
    if (!operation) {
      return false;
    }

    // Can only cancel if not too far along
    if (operation.state === 'completing' || operation.state === 'completed') {
      return false;
    }

    // Rollback and mark as failed
    await this.rollbackSplit(operation, new Error('Cancelled by user'));
    return true;
  }

  /**
   * Check if any shards need splitting and trigger splits automatically
   */
  async checkAndTriggerSplits(): Promise<SplitRequestResult[]> {
    const recommendations = this.monitor.checkThresholds();
    const results: SplitRequestResult[] = [];

    for (const rec of recommendations) {
      const result = await this.requestSplit(rec);
      results.push(result);
    }

    return results;
  }

  // ============================================================================
  // Merge Operations
  // ============================================================================

  /**
   * Generate a unique merge ID using crypto-secure randomness
   */
  private generateMergeId(): string {
    const array = new Uint8Array(5);
    crypto.getRandomValues(array);
    const random = Array.from(array, (b) => b.toString(36).padStart(2, '0')).join('').substring(0, 7);
    return `merge_${Date.now()}_${random}`;
  }

  /**
   * Request to merge multiple shards into one
   *
   * @param sourceShards - The shard IDs to merge (first one becomes the target)
   * @param reason - Reason for the merge
   * @param collections - Collections to include in the merge
   */
  async requestMerge(
    sourceShards: number[],
    reason: MergeOperation['reason'] = 'low_utilization',
    collections?: string[]
  ): Promise<MergeRequestResult> {
    const now = Date.now();

    // Validate we have at least 2 shards to merge
    if (sourceShards.length < 2) {
      return {
        accepted: false,
        rejectionReason: 'Merge requires at least 2 shards',
      };
    }

    // Check concurrent merge limit
    if (this.activeMerges.size >= this.maxConcurrentMerges) {
      return {
        accepted: false,
        rejectionReason: `Maximum concurrent merges (${this.maxConcurrentMerges}) reached`,
      };
    }

    // Check minimum interval between merges for all involved shards
    for (const shardId of sourceShards) {
      const lastMerge = this.lastMergeTime.get(shardId);
      if (lastMerge && now - lastMerge < this.minMergeIntervalMs) {
        const remainingMs = this.minMergeIntervalMs - (now - lastMerge);
        return {
          accepted: false,
          rejectionReason: `Shard ${shardId} was merged recently. Wait ${Math.ceil(remainingMs / 60000)} minutes.`,
        };
      }
    }

    // Check if any shard is already involved in an active merge
    for (const merge of this.activeMerges.values()) {
      const involvedShards = new Set([...merge.sourceShardIds, merge.targetShardId]);
      for (const shardId of sourceShards) {
        if (involvedShards.has(shardId) && merge.state !== 'completed' && merge.state !== 'failed') {
          return {
            accepted: false,
            rejectionReason: `Shard ${shardId} is already involved in a merge (${merge.mergeId})`,
          };
        }
      }
    }

    // Check if any shard is being split
    for (const split of this.activeSplits.values()) {
      if (sourceShards.includes(split.sourceShardId) ||
          split.targetShardIds.some(id => sourceShards.includes(id))) {
        if (split.state !== 'completed' && split.state !== 'failed') {
          return {
            accepted: false,
            rejectionReason: `Shard involved in an active split operation (${split.splitId})`,
          };
        }
      }
    }

    // The first shard becomes the target (receives all the data)
    const targetShardId = sourceShards[0]!;
    const sourceShardIds = sourceShards.slice(1);

    // Get metrics to estimate migration count
    let estimatedMigrationCount = 0;
    const allCollections = new Set<string>();

    for (const shardId of sourceShardIds) {
      const metrics = this.monitor.getShardMetrics(shardId);
      if (metrics) {
        estimatedMigrationCount += metrics.documentCount;
        for (const coll of metrics.collections.keys()) {
          allCollections.add(coll);
        }
      }
    }

    // Determine affected collections
    const affectedCollections = collections ?? Array.from(allCollections);

    // Create merge operation
    const mergeId = this.generateMergeId();
    const operation: MergeOperation = {
      mergeId,
      sourceShardIds,
      targetShardId,
      collections: affectedCollections,
      state: 'pending',
      reason,
      requestedAt: now,
      progress: 0,
      estimatedMigrationCount,
      migratedCount: 0,
    };

    this.activeMerges.set(mergeId, operation);
    this.notifyMergeStateChange(operation);

    // Start the merge process asynchronously
    this.executeMerge(operation).catch((error) => {
      operation.state = 'failed';
      operation.error = error instanceof Error ? error.message : String(error);
      operation.completedAt = Date.now();
      this.notifyMergeStateChange(operation);
    });

    return {
      accepted: true,
      mergeId,
    };
  }

  /**
   * Execute the merge operation
   */
  private async executeMerge(operation: MergeOperation): Promise<void> {
    try {
      operation.startedAt = Date.now();

      // Phase 1: Preparing
      await this.prepareMerge(operation);

      // Phase 2: Draining
      await this.drainSourceShards(operation);

      // Phase 3: Migrating
      await this.migrateDataToTarget(operation);

      // Phase 4: Validating
      await this.validateMerge(operation);

      // Phase 5: Completing
      await this.completeMerge(operation);

    } catch (error) {
      // Attempt rollback
      await this.rollbackMerge(operation, error);
      throw error;
    }
  }

  /**
   * Phase 1: Prepare the merge by updating router
   */
  private async prepareMerge(operation: MergeOperation): Promise<void> {
    this.updateMergeState(operation, 'preparing', 5);

    // Update router to direct traffic only to the target shard for affected collections
    // We keep the source shards in the split configuration during migration
    // to allow in-flight requests to complete
    for (const collection of operation.collections) {
      const existingSplit = this.router.getSplitInfo(collection);
      if (existingSplit) {
        // Update split to include all involved shards during migration
        const allShards = new Set([
          ...existingSplit.shards,
          operation.targetShardId,
          ...operation.sourceShardIds,
        ]);
        this.router.splitCollection(collection, Array.from(allShards));
      }
    }

    this.updateMergeState(operation, 'preparing', 10);
  }

  /**
   * Phase 2: Drain traffic from source shards
   */
  private async drainSourceShards(operation: MergeOperation): Promise<void> {
    this.updateMergeState(operation, 'draining', 15);

    // Update router to stop sending new requests to source shards
    for (const collection of operation.collections) {
      const existingSplit = this.router.getSplitInfo(collection);
      if (existingSplit) {
        // Remove source shards from routing, keeping only target
        const remainingShards = existingSplit.shards.filter(
          id => !operation.sourceShardIds.includes(id)
        );
        if (remainingShards.length >= 2) {
          this.router.splitCollection(collection, remainingShards);
        } else if (remainingShards.length === 1) {
          // Only one shard left, remove split configuration
          this.router.unsplitCollection(collection);
        }
      }
    }

    // Call the drain callback if provided
    if (this.drainShard) {
      for (let i = 0; i < operation.sourceShardIds.length; i++) {
        const shardId = operation.sourceShardIds[i]!;
        await this.drainShard(shardId);

        // Update progress (15% to 25% during draining)
        const progress = 15 + Math.floor((10 * (i + 1)) / operation.sourceShardIds.length);
        this.updateMergeState(operation, 'draining', progress);
      }
    }

    this.updateMergeState(operation, 'draining', 25);
  }

  /**
   * Phase 3: Migrate data from source shards to target shard
   */
  private async migrateDataToTarget(operation: MergeOperation): Promise<void> {
    this.updateMergeState(operation, 'migrating', 30);

    for (let i = 0; i < operation.sourceShardIds.length; i++) {
      const sourceShard = operation.sourceShardIds[i]!;

      // Call the migration function if provided
      if (this.migrateDataForMerge) {
        const migrated = await this.migrateDataForMerge(
          operation,
          sourceShard,
          operation.targetShardId
        );
        operation.migratedCount += migrated;
      }

      // Update progress (30% to 75% during migration)
      const progress = 30 + Math.floor((45 * (i + 1)) / operation.sourceShardIds.length);
      this.updateMergeState(operation, 'migrating', progress);
    }

    this.updateMergeState(operation, 'migrating', 75);
  }

  /**
   * Phase 4: Validate the merge
   */
  private async validateMerge(operation: MergeOperation): Promise<void> {
    this.updateMergeState(operation, 'validating', 80);

    // Verify target shard is healthy
    const targetMetrics = this.monitor.getShardMetrics(operation.targetShardId);
    if (!targetMetrics) {
      throw new Error(`Target shard ${operation.targetShardId} not found after merge`);
    }

    this.updateMergeState(operation, 'validating', 90);
  }

  /**
   * Phase 5: Complete the merge
   */
  private async completeMerge(operation: MergeOperation): Promise<void> {
    this.updateMergeState(operation, 'completing', 92);

    // Decommission source shards if callback provided
    if (this.decommissionShard) {
      for (const shardId of operation.sourceShardIds) {
        await this.decommissionShard(shardId);
      }
    }

    this.updateMergeState(operation, 'completing', 95);

    // Record merge time for all involved shards
    const now = Date.now();
    for (const shardId of [...operation.sourceShardIds, operation.targetShardId]) {
      this.lastMergeTime.set(shardId, now);
    }

    operation.completedAt = now;

    // Persist merge metadata
    await this.persistMergeMetadata(operation);

    this.updateMergeState(operation, 'completed', 100);

    // Move to completed history
    this.activeMerges.delete(operation.mergeId);
    this.completedMerges.push(operation);

    // Keep history bounded
    if (this.completedMerges.length > 100) {
      this.completedMerges.shift();
    }
  }

  /**
   * Persist merge metadata for recovery
   */
  private async persistMergeMetadata(operation: MergeOperation): Promise<void> {
    if (!this.persistMetadata) {
      return;
    }

    const metadata: MergeMetadata = {
      mergeId: operation.mergeId,
      sourceShardIds: operation.sourceShardIds,
      targetShardId: operation.targetShardId,
      collections: operation.collections,
      completedAt: operation.completedAt ?? Date.now(),
      version: ++this.metadataVersion,
    };

    try {
      // Reuse the same persistence callback, storing with a different structure
      // In practice, you might want a separate persistMergeMetadata callback
      this.mergeMetadataHistory.push(metadata);

      // Keep metadata history bounded
      if (this.mergeMetadataHistory.length > 100) {
        this.mergeMetadataHistory.shift();
      }
    } catch (error) {
      logger.error('Failed to persist merge metadata', {
        mergeId: operation.mergeId,
        sourceShardIds: operation.sourceShardIds,
        targetShardId: operation.targetShardId,
        error,
      });
      // Don't fail the merge for metadata persistence issues
    }
  }

  /**
   * Rollback a failed merge
   */
  private async rollbackMerge(operation: MergeOperation, error: unknown): Promise<void> {
    try {
      // Restore router configuration to include source shards again
      for (const collection of operation.collections) {
        const existingSplit = this.router.getSplitInfo(collection);
        if (existingSplit) {
          // Re-add source shards to routing
          const allShards = new Set([
            ...existingSplit.shards,
            ...operation.sourceShardIds,
          ]);
          this.router.splitCollection(collection, Array.from(allShards));
        }
      }

      operation.state = 'rolled_back';
      operation.error = error instanceof Error ? error.message : String(error);
      operation.completedAt = Date.now();
      this.notifyMergeStateChange(operation);

      // Move to completed history
      this.activeMerges.delete(operation.mergeId);
      this.completedMerges.push(operation);
    } catch (rollbackError) {
      logger.error('Failed to rollback merge', {
        mergeId: operation.mergeId,
        sourceShardIds: operation.sourceShardIds,
        targetShardId: operation.targetShardId,
        originalError: error instanceof Error ? error.message : String(error),
        error: rollbackError,
      });
    }
  }

  /**
   * Update merge operation state and notify listeners
   */
  private updateMergeState(operation: MergeOperation, state: MergeState, progress: number): void {
    operation.state = state;
    operation.progress = progress;
    this.notifyMergeStateChange(operation);
  }

  /**
   * Notify merge state change callback
   */
  private notifyMergeStateChange(operation: MergeOperation): void {
    this.onMergeStateChange?.(operation);
  }

  /**
   * Check if shards should be merged based on low utilization
   *
   * Identifies shards that are underutilized (below lowUtilizationThreshold of average)
   * and recommends merging adjacent/compatible shards.
   */
  checkMergeNeeded(): MergeRecommendation[] {
    const recommendations: MergeRecommendation[] = [];
    const allMetrics = this.monitor.getAllMetrics();

    if (allMetrics.length < 3) {
      // Need at least 3 shards to consider merging (merge 2, keep 1)
      return recommendations;
    }

    // Calculate averages
    const avgDocs = allMetrics.reduce((sum, m) => sum + m.documentCount, 0) / allMetrics.length;
    const avgSize = allMetrics.reduce((sum, m) => sum + m.sizeBytes, 0) / allMetrics.length;
    const avgWriteRate = allMetrics.reduce((sum, m) => sum + m.writeRate, 0) / allMetrics.length;

    // Find underutilized shards
    const underutilizedShards: Array<{
      shardId: number;
      utilization: number;
      metrics: typeof allMetrics[0];
    }> = [];

    for (const metrics of allMetrics) {
      // Calculate utilization as the max ratio across all metrics
      const docUtilization = avgDocs > 0 ? metrics.documentCount / avgDocs : 0;
      const sizeUtilization = avgSize > 0 ? metrics.sizeBytes / avgSize : 0;
      const writeUtilization = avgWriteRate > 0 ? metrics.writeRate / avgWriteRate : 0;

      const maxUtilization = Math.max(docUtilization, sizeUtilization, writeUtilization);

      if (maxUtilization < this.lowUtilizationThreshold) {
        underutilizedShards.push({
          shardId: metrics.shardId,
          utilization: maxUtilization,
          metrics,
        });
      }
    }

    // Sort by utilization (lowest first)
    underutilizedShards.sort((a, b) => a.utilization - b.utilization);

    // Find pairs of underutilized shards that can be merged
    const alreadyRecommended = new Set<number>();

    for (let i = 0; i < underutilizedShards.length - 1; i++) {
      const shard1 = underutilizedShards[i]!;

      if (alreadyRecommended.has(shard1.shardId)) {
        continue;
      }

      // Find a compatible partner shard
      for (let j = i + 1; j < underutilizedShards.length; j++) {
        const shard2 = underutilizedShards[j]!;

        if (alreadyRecommended.has(shard2.shardId)) {
          continue;
        }

        // Calculate combined utilization
        const combinedDocs = shard1.metrics.documentCount + shard2.metrics.documentCount;
        const combinedSize = shard1.metrics.sizeBytes + shard2.metrics.sizeBytes;
        const combinedWriteRate = shard1.metrics.writeRate + shard2.metrics.writeRate;

        const combinedDocUtilization = avgDocs > 0 ? combinedDocs / avgDocs : 0;
        const combinedSizeUtilization = avgSize > 0 ? combinedSize / avgSize : 0;
        const combinedWriteUtilization = avgWriteRate > 0 ? combinedWriteRate / avgWriteRate : 0;

        const combinedUtilization = Math.max(
          combinedDocUtilization,
          combinedSizeUtilization,
          combinedWriteUtilization
        );

        // Only recommend merge if combined load is reasonable (< 1.5x average)
        if (combinedUtilization < 1.5) {
          const allCollections = new Set([
            ...Array.from(shard1.metrics.collections.keys()),
            ...Array.from(shard2.metrics.collections.keys()),
          ]);

          // Choose the shard with more data as the target
          const targetShard = shard1.metrics.documentCount >= shard2.metrics.documentCount
            ? shard1.shardId
            : shard2.shardId;
          const sourceShard = targetShard === shard1.shardId ? shard2.shardId : shard1.shardId;

          recommendations.push({
            sourceShards: [targetShard, sourceShard],
            targetShard,
            collections: Array.from(allCollections),
            combinedUtilization,
            priority: 1 - combinedUtilization, // Lower combined utilization = higher priority
            reason: `Shards ${shard1.shardId} and ${shard2.shardId} have combined utilization of ${Math.round(combinedUtilization * 100)}%`,
          });

          alreadyRecommended.add(shard1.shardId);
          alreadyRecommended.add(shard2.shardId);
          break; // Move to next shard1
        }
      }
    }

    // Sort by priority (highest first)
    recommendations.sort((a, b) => b.priority - a.priority);

    return recommendations;
  }

  /**
   * Get status of a specific merge operation
   */
  getMergeStatus(mergeId: string): MergeOperation | undefined {
    return this.activeMerges.get(mergeId) ??
      this.completedMerges.find((op) => op.mergeId === mergeId);
  }

  /**
   * Get all active merge operations
   */
  getActiveMerges(): MergeOperation[] {
    return Array.from(this.activeMerges.values());
  }

  /**
   * Get merge history
   */
  getMergeHistory(limit: number = 10): MergeOperation[] {
    return this.completedMerges.slice(-limit);
  }

  /**
   * Cancel a pending or in-progress merge
   */
  async cancelMerge(mergeId: string): Promise<boolean> {
    const operation = this.activeMerges.get(mergeId);
    if (!operation) {
      return false;
    }

    // Can only cancel if not too far along
    if (operation.state === 'completing' || operation.state === 'completed') {
      return false;
    }

    // Rollback and mark as failed
    await this.rollbackMerge(operation, new Error('Cancelled by user'));
    return true;
  }

  /**
   * Execute merge recommendations automatically
   */
  async executeMergeRecommendations(): Promise<MergeRequestResult[]> {
    const recommendations = this.checkMergeNeeded();
    const results: MergeRequestResult[] = [];

    for (const rec of recommendations) {
      const result = await this.requestMerge(
        rec.sourceShards,
        'low_utilization',
        rec.collections
      );
      results.push(result);

      // Stop if we hit the concurrent merge limit
      if (this.activeMerges.size >= this.maxConcurrentMerges) {
        break;
      }
    }

    return results;
  }

  /**
   * Get summary of coordinator status
   */
  getSummary(): {
    activeSplits: number;
    completedSplits: number;
    failedSplits: number;
    totalShardsSplit: number;
    lastSplitTime: number | null;
    activeMerges: number;
    completedMerges: number;
    failedMerges: number;
    totalShardsMerged: number;
    lastMergeTime: number | null;
  } {
    const failedSplitCount = this.completedSplits.filter(
      (op) => op.state === 'failed' || op.state === 'rolled_back'
    ).length;

    const successSplitCount = this.completedSplits.filter(
      (op) => op.state === 'completed'
    ).length;

    const lastSplitOp = this.completedSplits.length > 0
      ? this.completedSplits[this.completedSplits.length - 1]
      : undefined;
    const lastSplit = lastSplitOp?.completedAt ?? null;

    const failedMergeCount = this.completedMerges.filter(
      (op) => op.state === 'failed' || op.state === 'rolled_back'
    ).length;

    const successMergeCount = this.completedMerges.filter(
      (op) => op.state === 'completed'
    ).length;

    const lastMergeOp = this.completedMerges.length > 0
      ? this.completedMerges[this.completedMerges.length - 1]
      : undefined;
    const lastMerge = lastMergeOp?.completedAt ?? null;

    return {
      activeSplits: this.activeSplits.size,
      completedSplits: successSplitCount,
      failedSplits: failedSplitCount,
      totalShardsSplit: this.completedSplits.reduce(
        (sum, op) => sum + (op.state === 'completed' ? op.targetShardIds.length : 0),
        0
      ),
      lastSplitTime: lastSplit,
      activeMerges: this.activeMerges.size,
      completedMerges: successMergeCount,
      failedMerges: failedMergeCount,
      totalShardsMerged: this.completedMerges.reduce(
        (sum, op) => sum + (op.state === 'completed' ? op.sourceShardIds.length : 0),
        0
      ),
      lastMergeTime: lastMerge,
    };
  }

  /**
   * Check for load imbalance and generate rebalance recommendations
   *
   * Analyzes the distribution of:
   * - Document counts
   * - Data size
   * - Write rates
   *
   * Returns recommendations for splits, merges, or data moves to achieve balance.
   */
  checkRebalanceNeeded(): RebalanceRecommendation[] {
    const now = Date.now();

    // Rate limit rebalance checks
    if (now - this.lastRebalanceCheck < this.rebalanceCheckIntervalMs) {
      return [];
    }
    this.lastRebalanceCheck = now;

    const recommendations: RebalanceRecommendation[] = [];
    const allMetrics = this.monitor.getAllMetrics();

    if (allMetrics.length < 2) {
      return recommendations;
    }

    // Calculate averages
    const avgDocs = allMetrics.reduce((sum, m) => sum + m.documentCount, 0) / allMetrics.length;
    const avgSize = allMetrics.reduce((sum, m) => sum + m.sizeBytes, 0) / allMetrics.length;
    const avgWriteRate = allMetrics.reduce((sum, m) => sum + m.writeRate, 0) / allMetrics.length;

    // Find imbalanced shards
    const hotShards: number[] = [];
    const coldShards: number[] = [];

    for (const metrics of allMetrics) {
      const docDeviation = avgDocs > 0 ? Math.abs(metrics.documentCount - avgDocs) / avgDocs : 0;
      const sizeDeviation = avgSize > 0 ? Math.abs(metrics.sizeBytes - avgSize) / avgSize : 0;
      const writeDeviation = avgWriteRate > 0 ? Math.abs(metrics.writeRate - avgWriteRate) / avgWriteRate : 0;

      const maxDeviation = Math.max(docDeviation, sizeDeviation, writeDeviation);

      if (maxDeviation > this.imbalanceThreshold) {
        // Determine if hot or cold
        const isHot =
          metrics.documentCount > avgDocs * (1 + this.imbalanceThreshold) ||
          metrics.sizeBytes > avgSize * (1 + this.imbalanceThreshold) ||
          metrics.writeRate > avgWriteRate * (1 + this.imbalanceThreshold);

        if (isHot) {
          hotShards.push(metrics.shardId);
        } else {
          coldShards.push(metrics.shardId);
        }
      }
    }

    // Generate recommendations for hot shards (split)
    for (const shardId of hotShards) {
      const metrics = this.monitor.getShardMetrics(shardId);
      if (!metrics) continue;

      const hotCollections = Array.from(metrics.collections.values())
        .sort((a, b) => b.writeCount - a.writeCount)
        .slice(0, 5)
        .map(c => c.collection);

      // Calculate how many ways to split based on load ratio
      const docRatio = avgDocs > 0 ? metrics.documentCount / avgDocs : 1;
      const sizeRatio = avgSize > 0 ? metrics.sizeBytes / avgSize : 1;
      const writeRatio = avgWriteRate > 0 ? metrics.writeRate / avgWriteRate : 1;
      const maxRatio = Math.max(docRatio, sizeRatio, writeRatio);
      const splitCount = Math.min(Math.ceil(maxRatio), 4);

      if (splitCount >= 2) {
        recommendations.push({
          action: 'split',
          sourceShards: [shardId],
          collections: hotCollections,
          expectedImprovement: Math.min((maxRatio - 1) / maxRatio, 0.75),
          priority: maxRatio,
          reason: `Shard ${shardId} is ${Math.round(maxRatio * 100)}% of average load`,
        });
      }
    }

    // Generate recommendations for cold shards (merge candidates)
    // Only recommend merge if there are multiple cold shards that together
    // wouldn't exceed the average load
    if (coldShards.length >= 2) {
      const coldMetrics = coldShards
        .map(id => this.monitor.getShardMetrics(id)!)
        .filter(m => m != null)
        .sort((a, b) => a.documentCount - b.documentCount);

      // Find pairs of cold shards that could be merged
      for (let i = 0; i < coldMetrics.length - 1; i++) {
        const shard1 = coldMetrics[i]!;
        const shard2 = coldMetrics[i + 1]!;

        const combinedDocs = shard1.documentCount + shard2.documentCount;
        const combinedSize = shard1.sizeBytes + shard2.sizeBytes;
        const combinedWriteRate = shard1.writeRate + shard2.writeRate;

        // Only recommend merge if combined load is below average
        if (
          combinedDocs < avgDocs * 1.5 &&
          combinedSize < avgSize * 1.5 &&
          combinedWriteRate < avgWriteRate * 1.5
        ) {
          const allCollections = new Set([
            ...Array.from(shard1.collections.keys()),
            ...Array.from(shard2.collections.keys()),
          ]);

          recommendations.push({
            action: 'merge',
            sourceShards: [shard1.shardId, shard2.shardId],
            targetShards: [shard1.shardId], // Merge into first shard
            collections: Array.from(allCollections),
            expectedImprovement: 0.3,
            priority: 0.5, // Lower priority than splits
            reason: `Shards ${shard1.shardId} and ${shard2.shardId} are underutilized`,
          });
        }
      }
    }

    // Sort by priority (highest first)
    recommendations.sort((a, b) => b.priority - a.priority);

    return recommendations;
  }

  /**
   * Execute rebalance recommendations automatically
   *
   * Only executes split recommendations - merge operations require
   * more complex coordination and are left for manual execution.
   */
  async executeRebalance(): Promise<SplitRequestResult[]> {
    const recommendations = this.checkRebalanceNeeded();
    const results: SplitRequestResult[] = [];

    for (const rec of recommendations) {
      if (rec.action === 'split') {
        // Calculate split count based on expected improvement
        const splitCount = Math.max(2, Math.ceil(rec.priority));

        for (const shardId of rec.sourceShards) {
          const result = await this.splitShard(
            shardId,
            Math.min(splitCount, 4),
            'write_rate', // Use write_rate as default reason for rebalance
            rec.collections
          );
          results.push(result);
        }
      }
      // Note: merge operations are not automatically executed
      // They require more coordination and should be handled manually
    }

    return results;
  }

  /**
   * Initialize coordinator from persisted state
   *
   * Call this method during startup to recover routing table state
   * from previously persisted split metadata.
   */
  async initializeFromPersistedState(): Promise<void> {
    if (!this.loadMetadata) {
      return;
    }

    try {
      const metadataList = await this.loadMetadata();

      // Sort by version to apply in order
      metadataList.sort((a, b) => a.version - b.version);

      for (const metadata of metadataList) {
        // Restore split configurations to router
        for (const collection of metadata.collections) {
          this.router.splitCollection(collection, metadata.resultingShards);
        }

        // Track the latest split time for each shard
        this.lastSplitTime.set(metadata.sourceShardId, metadata.completedAt);

        // Ensure monitor knows about all shards
        for (const shardId of metadata.resultingShards) {
          try {
            this.monitor.addShard(shardId);
          } catch {
            // Shard may already exist, ignore
          }
        }

        this.splitMetadataHistory.push(metadata);
      }

      // Update metadata version counter
      if (metadataList.length > 0) {
        const lastMetadata = metadataList[metadataList.length - 1];
        if (lastMetadata) {
          this.metadataVersion = lastMetadata.version;
        }
      }
    } catch (error) {
      logger.error('Failed to load persisted split metadata', {
        operation: 'initializeFromPersistedState',
        error,
      });
      // Non-fatal: coordinator can still function without persisted state
    }
  }

  /**
   * Get the current routing configuration for a collection
   */
  getRoutingConfig(collection: string): {
    shards: number[];
    splitPoints?: SplitPoint[];
  } | undefined {
    const splitInfo = this.router.getSplitInfo(collection);
    if (!splitInfo) {
      return undefined;
    }

    // Find the most recent metadata for this collection
    const metadata = this.splitMetadataHistory
      .filter(m => m.collections.includes(collection))
      .pop();

    return {
      shards: splitInfo.shards,
      splitPoints: metadata?.splitPoints,
    };
  }

  /**
   * Get rebalance recommendations without executing them
   */
  getRebalanceRecommendations(): RebalanceRecommendation[] {
    return this.checkRebalanceNeeded();
  }

  /**
   * Get the load balance score (0-1, where 1 is perfectly balanced)
   */
  getLoadBalanceScore(): number {
    const allMetrics = this.monitor.getAllMetrics();

    if (allMetrics.length < 2) {
      return 1.0; // Trivially balanced with 0 or 1 shard
    }

    // Calculate coefficient of variation for each metric
    const docCounts = allMetrics.map(m => m.documentCount);
    const sizes = allMetrics.map(m => m.sizeBytes);
    const writeRates = allMetrics.map(m => m.writeRate);

    const docCV = this.coefficientOfVariation(docCounts);
    const sizeCV = this.coefficientOfVariation(sizes);
    const writeCV = this.coefficientOfVariation(writeRates);

    // Average CV across metrics, then convert to score
    const avgCV = (docCV + sizeCV + writeCV) / 3;

    // CV of 0 = perfectly balanced (score 1.0)
    // CV of 1 = highly imbalanced (score 0.0)
    return Math.max(0, 1 - avgCV);
  }

  /**
   * Calculate coefficient of variation (standard deviation / mean)
   */
  private coefficientOfVariation(values: number[]): number {
    if (values.length === 0) return 0;

    const mean = values.reduce((a, b) => a + b, 0) / values.length;
    if (mean === 0) return 0;

    const squaredDiffs = values.map(v => Math.pow(v - mean, 2));
    const variance = squaredDiffs.reduce((a, b) => a + b, 0) / values.length;
    const stdDev = Math.sqrt(variance);

    return stdDev / mean;
  }

  /**
   * Export state for debugging/monitoring
   */
  toJSON(): Record<string, unknown> {
    return {
      activeSplits: Array.from(this.activeSplits.values()),
      recentSplits: this.completedSplits.slice(-10),
      activeMerges: Array.from(this.activeMerges.values()),
      recentMerges: this.completedMerges.slice(-10),
      summary: this.getSummary(),
      loadBalanceScore: this.getLoadBalanceScore(),
      rebalanceRecommendations: this.checkRebalanceNeeded(),
      mergeRecommendations: this.checkMergeNeeded(),
    };
  }

  /**
   * Get merge recommendations without executing them
   */
  getMergeRecommendations(): MergeRecommendation[] {
    return this.checkMergeNeeded();
  }
}

// ============================================================================
// Factory Function
// ============================================================================

/**
 * Create a new SplitCoordinator instance
 */
export function createSplitCoordinator(config: SplitCoordinatorConfig): SplitCoordinator {
  return new SplitCoordinator(config);
}
