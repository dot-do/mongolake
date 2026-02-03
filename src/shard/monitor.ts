/**
 * ShardMonitor - Shard Metrics Collection and Threshold Detection
 *
 * Tracks per-shard metrics for dynamic shard splitting:
 * - Write rate (operations per second)
 * - Document count
 * - Data size in bytes
 *
 * Detects when shards exceed configurable thresholds and triggers split events.
 */

import {
  DEFAULT_SHARD_SPLIT_MAX_DOCUMENTS,
  DEFAULT_SHARD_SPLIT_MAX_SIZE_BYTES,
  DEFAULT_SHARD_SPLIT_MAX_WRITE_RATE,
  DEFAULT_SHARD_SPLIT_CHECK_INTERVAL_MS,
  DEFAULT_SHARD_SPLIT_SUSTAINED_THRESHOLD_MS,
  DEFAULT_SHARD_WRITE_RATE_WINDOW_MS,
} from '../constants.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Metrics for a single shard
 */
export interface ShardMetrics {
  /** Shard identifier */
  shardId: number;
  /** Total document count across all collections */
  documentCount: number;
  /** Total data size in bytes */
  sizeBytes: number;
  /** Writes in the current window */
  writeCount: number;
  /** Write rate (operations per second, computed from sliding window) */
  writeRate: number;
  /** Timestamp of last metric update */
  lastUpdated: number;
  /** Per-collection breakdown */
  collections: Map<string, CollectionMetrics>;
}

/**
 * Metrics for a collection within a shard
 */
export interface CollectionMetrics {
  /** Collection name */
  collection: string;
  /** Document count in this collection */
  documentCount: number;
  /** Data size in bytes */
  sizeBytes: number;
  /** Write count in current window */
  writeCount: number;
}

/**
 * Thresholds for triggering shard splits
 */
export interface SplitThresholds {
  /** Maximum documents per shard before split (default: 1,000,000) */
  maxDocuments?: number;
  /** Maximum size in bytes per shard before split (default: 10GB) */
  maxSizeBytes?: number;
  /** Maximum write rate per second before split (default: 10,000) */
  maxWriteRate?: number;
  /** Minimum time between split checks in ms (default: 60000) */
  checkIntervalMs?: number;
  /** Minimum time a shard must exceed threshold before split (default: 300000 = 5 min) */
  sustainedThresholdMs?: number;
}

/**
 * Default thresholds
 */
export const DEFAULT_SPLIT_THRESHOLDS: Required<SplitThresholds> = {
  maxDocuments: DEFAULT_SHARD_SPLIT_MAX_DOCUMENTS,
  maxSizeBytes: DEFAULT_SHARD_SPLIT_MAX_SIZE_BYTES,
  maxWriteRate: DEFAULT_SHARD_SPLIT_MAX_WRITE_RATE,
  checkIntervalMs: DEFAULT_SHARD_SPLIT_CHECK_INTERVAL_MS,
  sustainedThresholdMs: DEFAULT_SHARD_SPLIT_SUSTAINED_THRESHOLD_MS,
};

/**
 * Event emitted when a shard exceeds thresholds
 */
export interface SplitRecommendation {
  /** Shard that should be split */
  shardId: number;
  /** Reason for the split recommendation */
  reason: 'document_count' | 'size' | 'write_rate';
  /** Current value that exceeded threshold */
  currentValue: number;
  /** Threshold that was exceeded */
  threshold: number;
  /** Collections contributing most to the threshold breach */
  hotCollections: string[];
  /** Recommended number of new shards */
  recommendedSplitCount: number;
  /** Timestamp of recommendation */
  timestamp: number;
}

/**
 * Write operation record for rate calculation
 */
interface WriteRecord {
  timestamp: number;
  count: number;
}

/**
 * Configuration for ShardMonitor
 */
export interface ShardMonitorConfig {
  /** Initial shard count */
  shardCount: number;
  /** Split thresholds */
  thresholds?: SplitThresholds;
  /** Sliding window size for write rate calculation (default: 60000ms = 1 minute) */
  writeRateWindowMs?: number;
  /** Callback when split is recommended */
  onSplitRecommended?: (recommendation: SplitRecommendation) => void;
}

// ============================================================================
// ShardMonitor Class
// ============================================================================

/**
 * ShardMonitor tracks per-shard metrics and detects when splits are needed.
 *
 * ## Usage
 *
 * ```typescript
 * const monitor = new ShardMonitor({
 *   shardCount: 16,
 *   thresholds: {
 *     maxDocuments: 1_000_000,
 *     maxSizeBytes: 10 * 1024 * 1024 * 1024,
 *   },
 *   onSplitRecommended: (rec) => console.log('Split recommended:', rec),
 * });
 *
 * // Record writes
 * monitor.recordWrite(shardId, 'users', 1, 256);
 *
 * // Check for split recommendations
 * const recommendations = monitor.checkThresholds();
 * ```
 */
export class ShardMonitor {
  private shardCount: number;
  private thresholds: Required<SplitThresholds>;
  private writeRateWindowMs: number;
  private onSplitRecommended?: (recommendation: SplitRecommendation) => void;

  /** Per-shard metrics */
  private shardMetrics: Map<number, ShardMetrics> = new Map();

  /** Write history for rate calculation (per shard) */
  private writeHistory: Map<number, WriteRecord[]> = new Map();

  /** Tracks when shards first exceeded thresholds (for sustained threshold detection) */
  private thresholdExceededAt: Map<number, Map<string, number>> = new Map();

  /** Last time thresholds were checked */
  private lastCheckTime: number = 0;

  constructor(config: ShardMonitorConfig) {
    this.shardCount = config.shardCount;
    this.thresholds = { ...DEFAULT_SPLIT_THRESHOLDS, ...config.thresholds };
    this.writeRateWindowMs = config.writeRateWindowMs ?? DEFAULT_SHARD_WRITE_RATE_WINDOW_MS;
    this.onSplitRecommended = config.onSplitRecommended;

    // Initialize metrics for all shards
    for (let i = 0; i < this.shardCount; i++) {
      this.initializeShard(i);
    }
  }

  /**
   * Initialize metrics for a shard
   */
  private initializeShard(shardId: number): void {
    this.shardMetrics.set(shardId, {
      shardId,
      documentCount: 0,
      sizeBytes: 0,
      writeCount: 0,
      writeRate: 0,
      lastUpdated: Date.now(),
      collections: new Map(),
    });
    this.writeHistory.set(shardId, []);
    this.thresholdExceededAt.set(shardId, new Map());
  }

  /**
   * Get current shard count
   */
  getShardCount(): number {
    return this.shardCount;
  }

  /**
   * Add a new shard (after split)
   */
  addShard(shardId: number): void {
    if (this.shardMetrics.has(shardId)) {
      throw new Error(`Shard ${shardId} already exists`);
    }
    this.initializeShard(shardId);
    this.shardCount++;
  }

  /**
   * Record a write operation
   *
   * @param shardId - The shard that received the write
   * @param collection - The collection written to
   * @param documentCount - Number of documents written
   * @param sizeBytes - Size of the write in bytes
   */
  recordWrite(
    shardId: number,
    collection: string,
    documentCount: number = 1,
    sizeBytes: number = 0
  ): void {
    const metrics = this.shardMetrics.get(shardId);
    if (!metrics) {
      throw new Error(`Unknown shard: ${shardId}`);
    }

    const now = Date.now();

    // Update shard-level metrics
    metrics.documentCount += documentCount;
    metrics.sizeBytes += sizeBytes;
    metrics.writeCount += 1;
    metrics.lastUpdated = now;

    // Update collection-level metrics
    let collMetrics = metrics.collections.get(collection);
    if (!collMetrics) {
      collMetrics = {
        collection,
        documentCount: 0,
        sizeBytes: 0,
        writeCount: 0,
      };
      metrics.collections.set(collection, collMetrics);
    }
    collMetrics.documentCount += documentCount;
    collMetrics.sizeBytes += sizeBytes;
    collMetrics.writeCount += 1;

    // Record write for rate calculation
    const history = this.writeHistory.get(shardId)!;
    history.push({ timestamp: now, count: 1 });

    // Prune old entries from history
    this.pruneWriteHistory(shardId, now);

    // Calculate current write rate
    metrics.writeRate = this.calculateWriteRate(shardId, now);
  }

  /**
   * Record a delete operation (decrements document count)
   */
  recordDelete(
    shardId: number,
    collection: string,
    documentCount: number = 1,
    sizeBytes: number = 0
  ): void {
    const metrics = this.shardMetrics.get(shardId);
    if (!metrics) {
      throw new Error(`Unknown shard: ${shardId}`);
    }

    // Decrement counts (don't go below 0)
    metrics.documentCount = Math.max(0, metrics.documentCount - documentCount);
    metrics.sizeBytes = Math.max(0, metrics.sizeBytes - sizeBytes);
    metrics.lastUpdated = Date.now();

    // Update collection-level metrics
    const collMetrics = metrics.collections.get(collection);
    if (collMetrics) {
      collMetrics.documentCount = Math.max(0, collMetrics.documentCount - documentCount);
      collMetrics.sizeBytes = Math.max(0, collMetrics.sizeBytes - sizeBytes);
    }
  }

  /**
   * Update shard metrics directly (e.g., from a status endpoint)
   */
  updateMetrics(
    shardId: number,
    update: {
      documentCount?: number;
      sizeBytes?: number;
      collections?: Array<{ collection: string; documentCount: number; sizeBytes: number }>;
    }
  ): void {
    const metrics = this.shardMetrics.get(shardId);
    if (!metrics) {
      throw new Error(`Unknown shard: ${shardId}`);
    }

    if (update.documentCount !== undefined) {
      metrics.documentCount = update.documentCount;
    }
    if (update.sizeBytes !== undefined) {
      metrics.sizeBytes = update.sizeBytes;
    }
    if (update.collections) {
      for (const coll of update.collections) {
        let collMetrics = metrics.collections.get(coll.collection);
        if (!collMetrics) {
          collMetrics = {
            collection: coll.collection,
            documentCount: 0,
            sizeBytes: 0,
            writeCount: 0,
          };
          metrics.collections.set(coll.collection, collMetrics);
        }
        collMetrics.documentCount = coll.documentCount;
        collMetrics.sizeBytes = coll.sizeBytes;
      }
    }
    metrics.lastUpdated = Date.now();
  }

  /**
   * Prune old entries from write history
   */
  private pruneWriteHistory(shardId: number, now: number): void {
    const history = this.writeHistory.get(shardId)!;
    const cutoff = now - this.writeRateWindowMs;

    // Remove entries older than the window
    while (history.length > 0 && history[0]!.timestamp < cutoff) {
      history.shift();
    }
  }

  /**
   * Calculate write rate for a shard (writes per second)
   */
  private calculateWriteRate(shardId: number, now: number): number {
    const history = this.writeHistory.get(shardId)!;
    if (history.length === 0) {
      return 0;
    }

    // Sum all writes in the window
    const totalWrites = history.reduce((sum, record) => sum + record.count, 0);

    // Calculate the actual window duration (from oldest record to now)
    const oldestTimestamp = history[0]!.timestamp;
    const windowDuration = (now - oldestTimestamp) / 1000; // in seconds

    if (windowDuration < 1) {
      return totalWrites; // Less than 1 second, return raw count
    }

    return totalWrites / windowDuration;
  }

  /**
   * Get metrics for a specific shard
   */
  getShardMetrics(shardId: number): ShardMetrics | undefined {
    return this.shardMetrics.get(shardId);
  }

  /**
   * Get metrics for all shards
   */
  getAllMetrics(): ShardMetrics[] {
    return Array.from(this.shardMetrics.values());
  }

  /**
   * Get the hottest collections for a shard (by write count)
   */
  getHotCollections(shardId: number, limit: number = 5): CollectionMetrics[] {
    const metrics = this.shardMetrics.get(shardId);
    if (!metrics) {
      return [];
    }

    return Array.from(metrics.collections.values())
      .sort((a, b) => b.writeCount - a.writeCount)
      .slice(0, limit);
  }

  /**
   * Check all shards against thresholds and return split recommendations
   */
  checkThresholds(): SplitRecommendation[] {
    const now = Date.now();

    // Rate limit threshold checks
    if (now - this.lastCheckTime < this.thresholds.checkIntervalMs) {
      return [];
    }
    this.lastCheckTime = now;

    const recommendations: SplitRecommendation[] = [];

    for (const [shardId, metrics] of this.shardMetrics) {
      const exceededThresholds = this.thresholdExceededAt.get(shardId)!;

      // Check document count
      if (metrics.documentCount > this.thresholds.maxDocuments) {
        const rec = this.checkSustainedThreshold(
          shardId,
          'document_count',
          metrics.documentCount,
          this.thresholds.maxDocuments,
          exceededThresholds,
          now
        );
        if (rec) recommendations.push(rec);
      } else {
        exceededThresholds.delete('document_count');
      }

      // Check size
      if (metrics.sizeBytes > this.thresholds.maxSizeBytes) {
        const rec = this.checkSustainedThreshold(
          shardId,
          'size',
          metrics.sizeBytes,
          this.thresholds.maxSizeBytes,
          exceededThresholds,
          now
        );
        if (rec) recommendations.push(rec);
      } else {
        exceededThresholds.delete('size');
      }

      // Check write rate
      if (metrics.writeRate > this.thresholds.maxWriteRate) {
        const rec = this.checkSustainedThreshold(
          shardId,
          'write_rate',
          metrics.writeRate,
          this.thresholds.maxWriteRate,
          exceededThresholds,
          now
        );
        if (rec) recommendations.push(rec);
      } else {
        exceededThresholds.delete('write_rate');
      }
    }

    // Notify callback for each recommendation
    for (const rec of recommendations) {
      this.onSplitRecommended?.(rec);
    }

    return recommendations;
  }

  /**
   * Check if a threshold has been exceeded for the sustained duration
   */
  private checkSustainedThreshold(
    shardId: number,
    reason: 'document_count' | 'size' | 'write_rate',
    currentValue: number,
    threshold: number,
    exceededThresholds: Map<string, number>,
    now: number
  ): SplitRecommendation | null {
    // Record when threshold was first exceeded
    if (!exceededThresholds.has(reason)) {
      exceededThresholds.set(reason, now);
      return null; // Not sustained yet
    }

    const firstExceeded = exceededThresholds.get(reason)!;
    if (now - firstExceeded < this.thresholds.sustainedThresholdMs) {
      return null; // Not sustained long enough
    }

    // Calculate recommended split count based on how much threshold is exceeded
    const ratio = currentValue / threshold;
    const recommendedSplitCount = Math.min(
      Math.ceil(ratio),
      4 // Cap at 4x split to avoid over-splitting
    );

    const hotCollections = this.getHotCollections(shardId, 5).map((c) => c.collection);

    return {
      shardId,
      reason,
      currentValue,
      threshold,
      hotCollections,
      recommendedSplitCount,
      timestamp: now,
    };
  }

  /**
   * Check if a specific shard should be split (for on-demand checking)
   */
  shouldSplit(shardId: number): SplitRecommendation | null {
    const metrics = this.shardMetrics.get(shardId);
    if (!metrics) {
      return null;
    }

    const now = Date.now();
    const hotCollections = this.getHotCollections(shardId, 5).map((c) => c.collection);

    // Check document count
    if (metrics.documentCount > this.thresholds.maxDocuments) {
      const ratio = metrics.documentCount / this.thresholds.maxDocuments;
      return {
        shardId,
        reason: 'document_count',
        currentValue: metrics.documentCount,
        threshold: this.thresholds.maxDocuments,
        hotCollections,
        recommendedSplitCount: Math.min(Math.ceil(ratio), 4),
        timestamp: now,
      };
    }

    // Check size
    if (metrics.sizeBytes > this.thresholds.maxSizeBytes) {
      const ratio = metrics.sizeBytes / this.thresholds.maxSizeBytes;
      return {
        shardId,
        reason: 'size',
        currentValue: metrics.sizeBytes,
        threshold: this.thresholds.maxSizeBytes,
        hotCollections,
        recommendedSplitCount: Math.min(Math.ceil(ratio), 4),
        timestamp: now,
      };
    }

    // Check write rate
    if (metrics.writeRate > this.thresholds.maxWriteRate) {
      const ratio = metrics.writeRate / this.thresholds.maxWriteRate;
      return {
        shardId,
        reason: 'write_rate',
        currentValue: metrics.writeRate,
        threshold: this.thresholds.maxWriteRate,
        hotCollections,
        recommendedSplitCount: Math.min(Math.ceil(ratio), 4),
        timestamp: now,
      };
    }

    return null;
  }

  /**
   * Reset metrics for a shard (after split or for testing)
   */
  resetShardMetrics(shardId: number): void {
    const metrics = this.shardMetrics.get(shardId);
    if (metrics) {
      metrics.documentCount = 0;
      metrics.sizeBytes = 0;
      metrics.writeCount = 0;
      metrics.writeRate = 0;
      metrics.collections.clear();
      metrics.lastUpdated = Date.now();
    }
    this.writeHistory.set(shardId, []);
    this.thresholdExceededAt.get(shardId)?.clear();
  }

  /**
   * Get summary statistics across all shards
   */
  getSummary(): {
    totalShards: number;
    totalDocuments: number;
    totalSizeBytes: number;
    avgDocumentsPerShard: number;
    avgSizePerShard: number;
    hotShards: number[];
  } {
    const allMetrics = this.getAllMetrics();
    const totalDocuments = allMetrics.reduce((sum, m) => sum + m.documentCount, 0);
    const totalSizeBytes = allMetrics.reduce((sum, m) => sum + m.sizeBytes, 0);

    const hotShards = allMetrics
      .filter(
        (m) =>
          m.documentCount > this.thresholds.maxDocuments * 0.8 ||
          m.sizeBytes > this.thresholds.maxSizeBytes * 0.8 ||
          m.writeRate > this.thresholds.maxWriteRate * 0.8
      )
      .map((m) => m.shardId);

    return {
      totalShards: this.shardCount,
      totalDocuments,
      totalSizeBytes,
      avgDocumentsPerShard: this.shardCount > 0 ? totalDocuments / this.shardCount : 0,
      avgSizePerShard: this.shardCount > 0 ? totalSizeBytes / this.shardCount : 0,
      hotShards,
    };
  }

  /**
   * Export metrics in a format suitable for logging/monitoring
   */
  toJSON(): Record<string, unknown> {
    const metricsArray = this.getAllMetrics().map((m) => ({
      shardId: m.shardId,
      documentCount: m.documentCount,
      sizeBytes: m.sizeBytes,
      writeRate: m.writeRate,
      lastUpdated: m.lastUpdated,
      collections: Array.from(m.collections.values()),
    }));

    return {
      shardCount: this.shardCount,
      thresholds: this.thresholds,
      metrics: metricsArray,
      summary: this.getSummary(),
    };
  }
}

// ============================================================================
// Factory Function
// ============================================================================

/**
 * Create a new ShardMonitor instance with optional configuration
 */
export function createShardMonitor(config: ShardMonitorConfig): ShardMonitor {
  return new ShardMonitor(config);
}
