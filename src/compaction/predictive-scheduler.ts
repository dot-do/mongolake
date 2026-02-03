/**
 * Predictive Compaction Scheduler
 *
 * Implements cost-based compaction scheduling that:
 * - Tracks write rates over configurable time windows
 * - Predicts when compaction will be needed based on current write patterns
 * - Proactively schedules compaction before thresholds are exceeded
 * - Learns from historical patterns to improve prediction accuracy
 * - Avoids scheduling during high-write periods to prevent stalls
 */

import type { CompactionScheduler, BlockMetadata, CompactionState, CompactionResult } from './scheduler.js';

// ============================================================================
// Types
// ============================================================================

/**
 * A single write event recorded for tracking purposes
 */
export interface WriteEvent {
  timestamp: number;
  bytes: number;
  blockCount: number;
}

/**
 * Statistics calculated over a time window
 */
export interface WindowStats {
  totalBytes: number;
  totalBlocks: number;
  eventCount: number;
  averageWriteRate: number; // bytes per second
  peakWriteRate: number; // bytes per second
  windowDurationMs: number;
}

/**
 * Prediction result for when compaction will be needed
 */
export interface CompactionPrediction {
  predictedTimeToThreshold: number; // milliseconds until threshold exceeded
  confidence: number; // 0-1 confidence in prediction
  currentBlockCount: number;
  currentTotalSize: number;
  estimatedBlocksAtCompaction: number;
  recommendedScheduleTime: number; // timestamp when to schedule
  shouldScheduleNow: boolean;
  reason: string;
}

/**
 * Historical pattern data point
 */
export interface HistoricalPattern {
  hourOfDay: number; // 0-23
  dayOfWeek: number; // 0-6 (Sunday = 0)
  averageWriteRate: number;
  sampleCount: number;
}

/**
 * Compaction outcome for learning
 */
export interface CompactionOutcome {
  timestamp: number;
  predictedTime: number;
  actualTime: number;
  wasProactive: boolean;
  blocksCompacted: number;
  durationMs: number;
  writeRateAtStart: number;
}

/**
 * Options for the predictive compaction scheduler
 */
export interface PredictiveSchedulerOptions {
  /** The underlying compaction scheduler to use */
  compactionScheduler: CompactionScheduler;

  /** Time window for write rate tracking (ms), default 5 minutes */
  trackingWindowMs?: number;

  /** Granularity for time buckets (ms), default 10 seconds */
  bucketSizeMs?: number;

  /** Threshold block count before compaction is needed */
  blockCountThreshold?: number;

  /** Threshold total size before compaction is needed (bytes) */
  sizeThreshold?: number;

  /** How far ahead to predict (ms), default 30 minutes */
  predictionHorizonMs?: number;

  /** Minimum confidence required to schedule proactively (0-1) */
  minPredictionConfidence?: number;

  /** Avoid scheduling when write rate exceeds this multiplier of average */
  highWriteMultiplier?: number;

  /** Buffer time before predicted threshold (ms), default 2 minutes */
  scheduleBufferMs?: number;

  /** Whether to enable historical pattern learning */
  enableLearning?: boolean;

  /** Maximum historical patterns to retain per hour/day slot */
  maxHistoricalSamples?: number;
}

/**
 * Proactive scheduling result
 */
export interface ProactiveScheduleResult {
  scheduled: boolean;
  scheduledFor?: number;
  reason: string;
  prediction?: CompactionPrediction;
}

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_TRACKING_WINDOW_MS = 5 * 60 * 1000; // 5 minutes
const DEFAULT_BUCKET_SIZE_MS = 10 * 1000; // 10 seconds
const DEFAULT_BLOCK_COUNT_THRESHOLD = 50;
const DEFAULT_SIZE_THRESHOLD = 100 * 1024 * 1024; // 100MB
const DEFAULT_PREDICTION_HORIZON_MS = 30 * 60 * 1000; // 30 minutes
const DEFAULT_MIN_PREDICTION_CONFIDENCE = 0.6;
const DEFAULT_HIGH_WRITE_MULTIPLIER = 2.0;
const DEFAULT_SCHEDULE_BUFFER_MS = 2 * 60 * 1000; // 2 minutes
const DEFAULT_MAX_HISTORICAL_SAMPLES = 100;

// ============================================================================
// WriteRateTracker Implementation
// ============================================================================

/**
 * Tracks write rates over configurable time windows using a sliding window approach
 */
export class WriteRateTracker {
  private events: WriteEvent[] = [];
  private windowMs: number;
  private bucketSizeMs: number;

  constructor(windowMs: number = DEFAULT_TRACKING_WINDOW_MS, bucketSizeMs: number = DEFAULT_BUCKET_SIZE_MS) {
    this.windowMs = windowMs;
    this.bucketSizeMs = bucketSizeMs;
  }

  /**
   * Record a write event
   */
  recordWrite(bytes: number, blockCount: number = 1): void {
    const now = Date.now();
    this.events.push({
      timestamp: now,
      bytes,
      blockCount,
    });
    this.pruneOldEvents(now);
  }

  /**
   * Get statistics for the current time window
   */
  getWindowStats(): WindowStats {
    const now = Date.now();
    this.pruneOldEvents(now);

    if (this.events.length === 0) {
      return {
        totalBytes: 0,
        totalBlocks: 0,
        eventCount: 0,
        averageWriteRate: 0,
        peakWriteRate: 0,
        windowDurationMs: this.windowMs,
      };
    }

    const totalBytes = this.events.reduce((sum, e) => sum + e.bytes, 0);
    const totalBlocks = this.events.reduce((sum, e) => sum + e.blockCount, 0);

    // Calculate effective window duration
    const oldestEvent = this.events[0]!;
    const effectiveDuration = Math.max(now - oldestEvent.timestamp, 1000);

    // Calculate average write rate (bytes per second)
    const averageWriteRate = (totalBytes / effectiveDuration) * 1000;

    // Calculate peak write rate by looking at buckets
    const peakWriteRate = this.calculatePeakWriteRate(now);

    return {
      totalBytes,
      totalBlocks,
      eventCount: this.events.length,
      averageWriteRate,
      peakWriteRate,
      windowDurationMs: this.windowMs,
    };
  }

  /**
   * Get write rate for a specific time range
   */
  getWriteRateForRange(startMs: number, endMs: number): number {
    const eventsInRange = this.events.filter(
      (e) => e.timestamp >= startMs && e.timestamp <= endMs
    );

    if (eventsInRange.length === 0) {
      return 0;
    }

    const totalBytes = eventsInRange.reduce((sum, e) => sum + e.bytes, 0);
    const duration = endMs - startMs;
    return duration > 0 ? (totalBytes / duration) * 1000 : 0;
  }

  /**
   * Get the trend in write rate (positive = increasing, negative = decreasing)
   */
  getWriteRateTrend(): number {
    const now = Date.now();
    const halfWindow = this.windowMs / 2;

    const firstHalfRate = this.getWriteRateForRange(now - this.windowMs, now - halfWindow);
    const secondHalfRate = this.getWriteRateForRange(now - halfWindow, now);

    if (firstHalfRate === 0) {
      return secondHalfRate > 0 ? 1 : 0;
    }

    return (secondHalfRate - firstHalfRate) / firstHalfRate;
  }

  /**
   * Get raw events (for testing/debugging)
   */
  getEvents(): WriteEvent[] {
    return [...this.events];
  }

  /**
   * Clear all events
   */
  clear(): void {
    this.events = [];
  }

  /**
   * Calculate peak write rate by bucketing events
   */
  private calculatePeakWriteRate(_now: number): number {
    if (this.events.length === 0) {
      return 0;
    }

    const buckets = new Map<number, number>();

    for (const event of this.events) {
      const bucketKey = Math.floor(event.timestamp / this.bucketSizeMs);
      buckets.set(bucketKey, (buckets.get(bucketKey) ?? 0) + event.bytes);
    }

    let peakBytesPerBucket = 0;
    for (const bytes of buckets.values()) {
      if (bytes > peakBytesPerBucket) {
        peakBytesPerBucket = bytes;
      }
    }

    // Convert to bytes per second
    return (peakBytesPerBucket / this.bucketSizeMs) * 1000;
  }

  /**
   * Remove events older than the tracking window
   */
  private pruneOldEvents(now: number): void {
    const cutoff = now - this.windowMs;
    this.events = this.events.filter((e) => e.timestamp >= cutoff);
  }
}

// ============================================================================
// HistoricalPatternLearner Implementation
// ============================================================================

/**
 * Learns and stores historical write patterns for improved predictions
 */
export class HistoricalPatternLearner {
  private patterns: Map<string, HistoricalPattern> = new Map();
  private outcomes: CompactionOutcome[] = [];
  private maxSamples: number;
  private maxOutcomes: number = 1000;

  constructor(maxSamples: number = DEFAULT_MAX_HISTORICAL_SAMPLES) {
    this.maxSamples = maxSamples;
  }

  /**
   * Record a write rate observation for pattern learning
   */
  recordObservation(writeRate: number): void {
    const now = new Date();
    const key = this.getPatternKey(now);

    const existing = this.patterns.get(key);
    if (existing) {
      // Update running average
      const totalRate = existing.averageWriteRate * existing.sampleCount + writeRate;
      existing.sampleCount = Math.min(existing.sampleCount + 1, this.maxSamples);
      existing.averageWriteRate = totalRate / existing.sampleCount;
    } else {
      this.patterns.set(key, {
        hourOfDay: now.getHours(),
        dayOfWeek: now.getDay(),
        averageWriteRate: writeRate,
        sampleCount: 1,
      });
    }
  }

  /**
   * Record a compaction outcome for learning
   */
  recordOutcome(outcome: CompactionOutcome): void {
    this.outcomes.push(outcome);
    if (this.outcomes.length > this.maxOutcomes) {
      this.outcomes.shift();
    }
  }

  /**
   * Get predicted write rate for a given time
   */
  getPredictedWriteRate(date: Date): number | null {
    const key = this.getPatternKey(date);
    const pattern = this.patterns.get(key);
    return pattern ? pattern.averageWriteRate : null;
  }

  /**
   * Get pattern for current hour/day slot
   */
  getCurrentPattern(): HistoricalPattern | null {
    const now = new Date();
    const key = this.getPatternKey(now);
    return this.patterns.get(key) ?? null;
  }

  /**
   * Calculate prediction accuracy based on past outcomes
   */
  getPredictionAccuracy(): number {
    if (this.outcomes.length < 5) {
      return 0.5; // Default when insufficient data
    }

    const recentOutcomes = this.outcomes.slice(-50);
    let totalError = 0;

    for (const outcome of recentOutcomes) {
      const error = Math.abs(outcome.predictedTime - outcome.actualTime);
      const normalizedError = error / Math.max(outcome.actualTime, 1);
      totalError += Math.min(normalizedError, 1); // Cap at 100% error
    }

    const averageError = totalError / recentOutcomes.length;
    return Math.max(0, 1 - averageError);
  }

  /**
   * Get all patterns (for persistence/debugging)
   */
  getPatterns(): Map<string, HistoricalPattern> {
    return new Map(this.patterns);
  }

  /**
   * Get recent outcomes (for analysis)
   */
  getRecentOutcomes(count: number = 10): CompactionOutcome[] {
    return this.outcomes.slice(-count);
  }

  /**
   * Clear all learned data
   */
  clear(): void {
    this.patterns.clear();
    this.outcomes = [];
  }

  /**
   * Generate pattern key from date
   */
  private getPatternKey(date: Date): string {
    return `${date.getDay()}-${date.getHours()}`;
  }
}

// ============================================================================
// PredictiveCompactionScheduler Implementation
// ============================================================================

/**
 * Predictive compaction scheduler that integrates with the existing CompactionScheduler
 */
export class PredictiveCompactionScheduler {
  private compactionScheduler: CompactionScheduler;
  private writeTracker: WriteRateTracker;
  private patternLearner: HistoricalPatternLearner;

  private blockCountThreshold: number;
  private sizeThreshold: number;
  private predictionHorizonMs: number;
  private minPredictionConfidence: number;
  private highWriteMultiplier: number;
  private scheduleBufferMs: number;
  private enableLearning: boolean;

  private scheduledCompaction: number | null = null;
  private lastPrediction: CompactionPrediction | null = null;

  constructor(options: PredictiveSchedulerOptions) {
    this.compactionScheduler = options.compactionScheduler;

    this.writeTracker = new WriteRateTracker(
      options.trackingWindowMs ?? DEFAULT_TRACKING_WINDOW_MS,
      options.bucketSizeMs ?? DEFAULT_BUCKET_SIZE_MS
    );

    this.patternLearner = new HistoricalPatternLearner(
      options.maxHistoricalSamples ?? DEFAULT_MAX_HISTORICAL_SAMPLES
    );

    this.blockCountThreshold = options.blockCountThreshold ?? DEFAULT_BLOCK_COUNT_THRESHOLD;
    this.sizeThreshold = options.sizeThreshold ?? DEFAULT_SIZE_THRESHOLD;
    this.predictionHorizonMs = options.predictionHorizonMs ?? DEFAULT_PREDICTION_HORIZON_MS;
    this.minPredictionConfidence = options.minPredictionConfidence ?? DEFAULT_MIN_PREDICTION_CONFIDENCE;
    this.highWriteMultiplier = options.highWriteMultiplier ?? DEFAULT_HIGH_WRITE_MULTIPLIER;
    this.scheduleBufferMs = options.scheduleBufferMs ?? DEFAULT_SCHEDULE_BUFFER_MS;
    this.enableLearning = options.enableLearning ?? true;
  }

  /**
   * Record a write operation for tracking
   */
  recordWrite(bytes: number, blockCount: number = 1): void {
    this.writeTracker.recordWrite(bytes, blockCount);

    if (this.enableLearning) {
      const stats = this.writeTracker.getWindowStats();
      this.patternLearner.recordObservation(stats.averageWriteRate);
    }
  }

  /**
   * Predict when compaction will be needed based on current state
   */
  predictCompactionNeed<T extends BlockMetadata>(blocks: T[]): CompactionPrediction {
    const now = Date.now();
    const stats = this.writeTracker.getWindowStats();

    // Current state
    const currentBlockCount = blocks.length;
    const currentTotalSize = blocks.reduce((sum, b) => sum + b.size, 0);

    // Calculate how far from thresholds
    const blocksToThreshold = this.blockCountThreshold - currentBlockCount;
    const bytesToThreshold = this.sizeThreshold - currentTotalSize;

    // If already at or past threshold
    if (blocksToThreshold <= 0 || bytesToThreshold <= 0) {
      this.lastPrediction = {
        predictedTimeToThreshold: 0,
        confidence: 1.0,
        currentBlockCount,
        currentTotalSize,
        estimatedBlocksAtCompaction: currentBlockCount,
        recommendedScheduleTime: now,
        shouldScheduleNow: true,
        reason: 'Threshold already exceeded',
      };
      return this.lastPrediction;
    }

    // Predict based on write rate
    const writeRate = stats.averageWriteRate;
    if (writeRate <= 0) {
      this.lastPrediction = {
        predictedTimeToThreshold: this.predictionHorizonMs * 2, // Far in the future
        confidence: 0.3,
        currentBlockCount,
        currentTotalSize,
        estimatedBlocksAtCompaction: currentBlockCount,
        recommendedScheduleTime: now + this.predictionHorizonMs,
        shouldScheduleNow: false,
        reason: 'No write activity detected',
      };
      return this.lastPrediction;
    }

    // Estimate time to size threshold
    const timeToSizeThreshold = (bytesToThreshold / writeRate) * 1000;

    // Estimate time to block count threshold (assuming average block size)
    const avgBlockSize = currentTotalSize > 0 && currentBlockCount > 0
      ? currentTotalSize / currentBlockCount
      : 1024 * 1024; // Default 1MB
    const blocksPerSecond = writeRate / avgBlockSize;
    const timeToBlockThreshold = blocksPerSecond > 0
      ? (blocksToThreshold / blocksPerSecond) * 1000
      : timeToSizeThreshold;

    // Use the earlier threshold
    const predictedTimeToThreshold = Math.min(timeToSizeThreshold, timeToBlockThreshold);

    // Calculate confidence based on:
    // 1. Amount of data in the tracking window
    // 2. Stability of write rate (trend)
    // 3. Historical accuracy
    const trend = this.writeTracker.getWriteRateTrend();
    const trendStability = Math.max(0, 1 - Math.abs(trend));
    const dataConfidence = Math.min(1, stats.eventCount / 10);
    const historicalAccuracy = this.enableLearning
      ? this.patternLearner.getPredictionAccuracy()
      : 0.5;

    const confidence = (trendStability * 0.3 + dataConfidence * 0.3 + historicalAccuracy * 0.4);

    // Determine if we should schedule now
    const isWithinHorizon = predictedTimeToThreshold <= this.predictionHorizonMs;
    const isHighConfidence = confidence >= this.minPredictionConfidence;
    const isNotHighWritePeriod = !this.isHighWritePeriod(stats);

    // Calculate recommended schedule time
    const recommendedScheduleTime = now + Math.max(0, predictedTimeToThreshold - this.scheduleBufferMs);

    // Estimate blocks at compaction time
    const estimatedBlocksAtCompaction = Math.min(
      this.blockCountThreshold,
      currentBlockCount + Math.ceil(blocksPerSecond * (predictedTimeToThreshold / 1000))
    );

    const shouldScheduleNow = isWithinHorizon && isHighConfidence && isNotHighWritePeriod;

    let reason = '';
    if (!isWithinHorizon) {
      reason = `Threshold not expected within prediction horizon (${Math.round(predictedTimeToThreshold / 1000)}s > ${Math.round(this.predictionHorizonMs / 1000)}s)`;
    } else if (!isHighConfidence) {
      reason = `Prediction confidence too low (${(confidence * 100).toFixed(1)}% < ${(this.minPredictionConfidence * 100).toFixed(1)}%)`;
    } else if (!isNotHighWritePeriod) {
      reason = 'Currently in high-write period, deferring compaction';
    } else {
      reason = `Proactive compaction recommended in ${Math.round((recommendedScheduleTime - now) / 1000)}s`;
    }

    this.lastPrediction = {
      predictedTimeToThreshold,
      confidence,
      currentBlockCount,
      currentTotalSize,
      estimatedBlocksAtCompaction,
      recommendedScheduleTime,
      shouldScheduleNow,
      reason,
    };

    return this.lastPrediction;
  }

  /**
   * Schedule proactive compaction if prediction warrants it
   */
  scheduleProactiveCompaction<T extends BlockMetadata>(blocks: T[]): ProactiveScheduleResult {
    const prediction = this.predictCompactionNeed(blocks);

    if (!prediction.shouldScheduleNow) {
      return {
        scheduled: false,
        reason: prediction.reason,
        prediction,
      };
    }

    this.scheduledCompaction = prediction.recommendedScheduleTime;

    return {
      scheduled: true,
      scheduledFor: prediction.recommendedScheduleTime,
      reason: prediction.reason,
      prediction,
    };
  }

  /**
   * Run compaction if scheduled or threshold exceeded
   */
  async runCompactionIfNeeded<T extends BlockMetadata>(
    blocks: T[],
    options?: { continuationState?: CompactionState; signal?: AbortSignal }
  ): Promise<CompactionResult | null> {
    const now = Date.now();
    const prediction = this.predictCompactionNeed(blocks);

    // Check if we should run compaction
    const shouldRun = prediction.shouldScheduleNow ||
      (this.scheduledCompaction !== null && now >= this.scheduledCompaction) ||
      prediction.predictedTimeToThreshold === 0;

    if (!shouldRun) {
      return null;
    }

    const startTime = Date.now();
    const writeRateAtStart = this.writeTracker.getWindowStats().averageWriteRate;

    // Run the actual compaction
    const result = await this.compactionScheduler.runCompaction(blocks, options);

    // Record outcome for learning
    if (this.enableLearning && !result.skipped) {
      this.patternLearner.recordOutcome({
        timestamp: startTime,
        predictedTime: this.lastPrediction?.predictedTimeToThreshold ?? 0,
        actualTime: Date.now() - startTime,
        wasProactive: this.scheduledCompaction !== null,
        blocksCompacted: result.processedBlocks,
        durationMs: result.stats.durationMs,
        writeRateAtStart,
      });
    }

    // Clear scheduled compaction
    this.scheduledCompaction = null;

    return result;
  }

  /**
   * Get current write rate statistics
   */
  getWriteStats(): WindowStats {
    return this.writeTracker.getWindowStats();
  }

  /**
   * Get the last prediction made
   */
  getLastPrediction(): CompactionPrediction | null {
    return this.lastPrediction;
  }

  /**
   * Get the scheduled compaction time (if any)
   */
  getScheduledCompactionTime(): number | null {
    return this.scheduledCompaction;
  }

  /**
   * Cancel any scheduled proactive compaction
   */
  cancelScheduledCompaction(): void {
    this.scheduledCompaction = null;
  }

  /**
   * Check if currently in a high-write period
   */
  isHighWritePeriod(stats?: WindowStats): boolean {
    const currentStats = stats ?? this.writeTracker.getWindowStats();

    // Use historical pattern if available
    const historicalRate = this.enableLearning
      ? this.patternLearner.getPredictedWriteRate(new Date())
      : null;

    const baselineRate = historicalRate ?? currentStats.averageWriteRate;

    // Current rate significantly exceeds baseline
    return currentStats.peakWriteRate > baselineRate * this.highWriteMultiplier;
  }

  /**
   * Get prediction accuracy from historical data
   */
  getPredictionAccuracy(): number {
    return this.patternLearner.getPredictionAccuracy();
  }

  /**
   * Get access to the underlying compaction scheduler
   */
  getCompactionScheduler(): CompactionScheduler {
    return this.compactionScheduler;
  }

  /**
   * Get the write rate tracker (for testing)
   */
  getWriteTracker(): WriteRateTracker {
    return this.writeTracker;
  }

  /**
   * Get the pattern learner (for testing)
   */
  getPatternLearner(): HistoricalPatternLearner {
    return this.patternLearner;
  }
}
