/**
 * Auto-Promote Manager for Schema Evolution
 *
 * Automatically evaluates fields for promotion to Parquet columns based on
 * frequency thresholds, type consistency, and configurable exclusion patterns.
 *
 * Key features:
 * - Threshold-based promotion detection
 * - Type consistency evaluation
 * - Glob pattern exclusions
 * - Batch size limits with priority ordering
 * - Cooldown periods between evaluations
 * - Promotion history tracking
 * - Configurable promotion strategies
 * - Dry-run mode for testing
 * - Promotion impact analysis
 * - Rollback support
 *
 * @module schema/auto-promote
 */

import type { ParquetType } from '../types.js';
import type { ParsedColumn, ParsedCollectionSchema } from './config.js';
import type { FieldAnalyzer, FieldStats, DetectedType } from './analyzer.js';

// ============================================================================
// Type Definitions
// ============================================================================

/**
 * Promotion strategy determines how fields are selected for promotion
 */
export type PromotionStrategy =
  | 'frequency'     // Promote based on frequency threshold (default)
  | 'conservative'  // Only promote fields with very high frequency (>95%) and consistency (>99%)
  | 'aggressive'    // Promote more liberally with lower thresholds
  | 'manual'        // Only promote fields explicitly specified
  | 'adaptive';     // Adjust thresholds based on data characteristics

/**
 * Strategy-specific configuration
 */
export interface StrategyConfig {
  /** Strategy type */
  type: PromotionStrategy;
  /** Custom threshold overrides for the strategy */
  thresholdOverrides?: {
    frequency?: number;
    consistency?: number;
  };
  /** For manual strategy: explicitly included fields */
  manualFields?: string[];
  /** For adaptive strategy: learning rate for threshold adjustment */
  adaptiveLearningRate?: number;
}

/**
 * Configuration for auto-promotion behavior
 */
export interface AutoPromoteConfig {
  /** Minimum frequency threshold (0-1) for promotion (default: 0.8) */
  threshold: number;
  /** Minimum type consistency (0-1) for promotion (default: 0.9) */
  minConsistency?: number;
  /** Maximum number of fields to promote in one batch */
  maxBatchSize?: number;
  /** Field patterns to exclude from auto-promotion */
  excludePatterns?: string[];
  /** Field patterns to include (overrides excludePatterns) */
  includePatterns?: string[];
  /** Whether to auto-promote nested fields (default: true) */
  includeNested?: boolean;
  /** Maximum nesting depth for auto-promotion (default: 5) */
  maxNestingDepth?: number;
  /** Whether to auto-promote array fields (default: false) */
  includeArrays?: boolean;
  /** Minimum document sample size before considering promotion */
  minSampleSize?: number;
  /** Cooldown period in ms before re-evaluating a field */
  cooldownMs?: number;
  /** Promotion strategy configuration */
  strategy?: StrategyConfig;
  /** Enable dry-run mode (no actual promotions applied) */
  dryRun?: boolean;
}

/**
 * Impact analysis for a promotion
 */
export interface PromotionImpact {
  /** Field path */
  path: string;
  /** Estimated storage impact in bytes (positive = increase, negative = decrease) */
  storageImpactBytes: number;
  /** Estimated query performance improvement factor (1.0 = no change, >1 = faster) */
  querySpeedupFactor: number;
  /** Risk level for the promotion */
  riskLevel: 'low' | 'medium' | 'high';
  /** Human-readable impact summary */
  summary: string;
  /** Affected queries or patterns */
  affectedPatterns: string[];
}

/**
 * Rollback information for a promotion
 */
export interface PromotionRollback {
  /** Unique rollback identifier */
  rollbackId: string;
  /** Original schema state before promotion */
  originalSchema: ParsedCollectionSchema;
  /** Promotions that were applied */
  appliedPromotions: AutoPromoteCandidate[];
  /** When the promotion was applied */
  appliedAt: Date;
  /** Whether rollback is still possible */
  canRollback: boolean;
  /** Reason if rollback is not possible */
  rollbackBlockedReason?: string;
}

/**
 * Result of auto-promote evaluation
 */
export interface AutoPromoteResult {
  /** Fields that should be promoted */
  promotions: AutoPromoteCandidate[];
  /** Fields that were evaluated but not promoted */
  skipped: SkippedField[];
  /** Fields that were excluded by pattern */
  excluded: string[];
  /** Total fields evaluated */
  totalEvaluated: number;
  /** Timestamp of evaluation */
  evaluatedAt: Date;
  /** Impact analysis for each promotion (available in dry-run mode) */
  impactAnalysis?: PromotionImpact[];
  /** Whether this was a dry-run evaluation */
  isDryRun: boolean;
  /** Strategy used for this evaluation */
  strategyUsed: PromotionStrategy;
}

/**
 * A candidate for automatic promotion
 */
export interface AutoPromoteCandidate {
  /** Field path */
  path: string;
  /** Suggested Parquet type */
  suggestedType: ParquetType;
  /** Field frequency (0-1) */
  frequency: number;
  /** Type consistency (0-1) */
  consistency: number;
  /** Priority score for batch ordering */
  priority: number;
  /** Reason for promotion */
  reason: string;
}

/**
 * A field that was evaluated but not promoted
 */
export interface SkippedField {
  /** Field path */
  path: string;
  /** Reason for skipping */
  reason: string;
  /** Current frequency */
  frequency: number;
  /** Current consistency */
  consistency: number;
}

/**
 * History entry for promotion tracking
 */
export interface PromotionHistoryEntry {
  /** Field path */
  path: string;
  /** When promotion was considered */
  timestamp: Date;
  /** Whether it was promoted */
  promoted: boolean;
  /** Reason for decision */
  reason: string;
  /** Stats at time of decision */
  stats: {
    frequency: number;
    consistency: number;
    sampleSize: number;
  };
  /** Strategy used for this decision */
  strategy?: PromotionStrategy;
  /** Rollback ID if this promotion can be rolled back */
  rollbackId?: string;
}

// ============================================================================
// Constants
// ============================================================================

/** Default minimum frequency threshold */
const DEFAULT_THRESHOLD = 0.8;

/** Default minimum type consistency */
const DEFAULT_MIN_CONSISTENCY = 0.9;

/** Default maximum nesting depth */
const DEFAULT_MAX_NESTING_DEPTH = 5;

/** Conservative strategy thresholds */
const CONSERVATIVE_THRESHOLD = 0.95;
const CONSERVATIVE_CONSISTENCY = 0.99;

/** Aggressive strategy thresholds */
const AGGRESSIVE_THRESHOLD = 0.5;
const AGGRESSIVE_CONSISTENCY = 0.7;

/** Parquet type mappings from detected types */
const DETECTED_TO_PARQUET: Readonly<Record<DetectedType, ParquetType>> = {
  string: 'string',
  number: 'double',
  boolean: 'boolean',
  date: 'timestamp',
  binary: 'binary',
  objectId: 'string',
  array: 'variant',
  object: 'variant',
  null: 'variant',
  mixed: 'variant',
};

/** Estimated bytes per type for storage impact calculation */
const BYTES_PER_TYPE: Readonly<Record<ParquetType, number>> = {
  string: 50,      // Average string length estimate
  int32: 4,
  int64: 8,
  float: 4,
  double: 8,
  boolean: 1,
  timestamp: 8,
  date: 4,
  binary: 100,     // Average binary size estimate
  variant: 200,    // Variant is larger due to type metadata
};

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Convert a glob pattern to a regex for field matching
 */
function globToRegex(pattern: string): RegExp {
  // Handle ** for recursive matching
  let escaped = pattern
    .replace(/[.+^${}()|[\]\\]/g, '\\$&')
    .replace(/\*\*/g, '<<<DOUBLE_STAR>>>')
    .replace(/\*/g, '[^.]*')
    .replace(/<<<DOUBLE_STAR>>>/g, '.*')
    .replace(/\?/g, '.');

  return new RegExp(`^${escaped}$`);
}

/**
 * Check if a field path matches any pattern in a list
 */
function matchesAnyPattern(path: string, patterns: string[]): boolean {
  return patterns.some((pattern) => globToRegex(pattern).test(path));
}

/**
 * Get the nesting depth of a field path
 */
function getNestingDepth(path: string): number {
  return (path.match(/\./g) || []).length;
}

/**
 * Calculate type consistency from field stats
 */
function calculateConsistency(stats: FieldStats): number {
  if (stats.count === 0) {
    return 0;
  }

  // Calculate total non-null occurrences
  let totalNonNull = 0;
  let dominantCount = 0;

  for (const [type, count] of stats.types) {
    if (type !== 'null') {
      totalNonNull += count;
      if (count > dominantCount) {
        dominantCount = count;
      }
    }
  }

  // If all values are null, consistency is 1.0
  if (totalNonNull === 0) {
    return 1.0;
  }

  return dominantCount / totalNonNull;
}

/**
 * Get the dominant (most common) non-null type
 */
function getDominantType(stats: FieldStats): DetectedType {
  let dominant: DetectedType = 'null';
  let dominantCount = 0;

  for (const [type, count] of stats.types) {
    if (type !== 'null' && count > dominantCount) {
      dominantCount = count;
      dominant = type;
    }
  }

  return dominant;
}

/**
 * Convert detected type to Parquet type
 */
function toParquetType(type: DetectedType): ParquetType {
  return DETECTED_TO_PARQUET[type] ?? 'variant';
}

/**
 * Get effective thresholds based on strategy
 */
function getStrategyThresholds(
  strategy: StrategyConfig | undefined,
  baseThreshold: number,
  baseConsistency: number
): { threshold: number; consistency: number } {
  if (!strategy) {
    return { threshold: baseThreshold, consistency: baseConsistency };
  }

  let threshold = baseThreshold;
  let consistency = baseConsistency;

  switch (strategy.type) {
    case 'conservative':
      threshold = CONSERVATIVE_THRESHOLD;
      consistency = CONSERVATIVE_CONSISTENCY;
      break;
    case 'aggressive':
      threshold = AGGRESSIVE_THRESHOLD;
      consistency = AGGRESSIVE_CONSISTENCY;
      break;
    case 'manual':
      // Manual strategy ignores thresholds - handled separately
      threshold = 0;
      consistency = 0;
      break;
    case 'adaptive':
      // Adaptive starts with base and will be adjusted
      break;
    case 'frequency':
    default:
      // Use base thresholds
      break;
  }

  // Apply any overrides
  if (strategy.thresholdOverrides) {
    if (strategy.thresholdOverrides.frequency !== undefined) {
      threshold = strategy.thresholdOverrides.frequency;
    }
    if (strategy.thresholdOverrides.consistency !== undefined) {
      consistency = strategy.thresholdOverrides.consistency;
    }
  }

  return { threshold, consistency };
}

/**
 * Calculate promotion impact for a field
 */
function calculatePromotionImpact(
  path: string,
  stats: FieldStats,
  suggestedType: ParquetType,
  documentCount: number
): PromotionImpact {
  // Estimate storage change: promoted column vs variant storage
  const promotedBytes = BYTES_PER_TYPE[suggestedType] * documentCount * stats.frequency;
  const variantBytes = BYTES_PER_TYPE['variant'] * documentCount * stats.frequency;
  const storageImpactBytes = Math.round(promotedBytes - variantBytes);

  // Estimate query speedup based on type (native types are faster to query)
  let querySpeedupFactor = 1.0;
  if (suggestedType !== 'variant') {
    // Native types are typically 2-10x faster depending on query type
    querySpeedupFactor = suggestedType === 'boolean' ? 5.0 :
                         suggestedType === 'int32' || suggestedType === 'int64' ? 4.0 :
                         suggestedType === 'double' || suggestedType === 'float' ? 3.5 :
                         suggestedType === 'timestamp' || suggestedType === 'date' ? 3.0 :
                         suggestedType === 'string' ? 2.5 :
                         suggestedType === 'binary' ? 2.0 : 1.5;
  }

  // Calculate risk level based on consistency and frequency
  const consistency = calculateConsistency(stats);
  let riskLevel: 'low' | 'medium' | 'high' = 'low';
  if (consistency < 0.9 || stats.frequency < 0.7) {
    riskLevel = 'high';
  } else if (consistency < 0.95 || stats.frequency < 0.85) {
    riskLevel = 'medium';
  }

  // Generate affected patterns (common query patterns for this field)
  const affectedPatterns: string[] = [];
  if (path.includes('.')) {
    affectedPatterns.push(`Nested field queries: db.find({ "${path}": ... })`);
  } else {
    affectedPatterns.push(`Direct field queries: db.find({ ${path}: ... })`);
  }
  if (suggestedType === 'string') {
    affectedPatterns.push(`Text search queries on ${path}`);
  }
  if (['int32', 'int64', 'double', 'float', 'timestamp', 'date'].includes(suggestedType)) {
    affectedPatterns.push(`Range queries: { ${path}: { $gte: ..., $lte: ... } }`);
  }

  // Generate summary
  const storageImpactStr = storageImpactBytes > 0
    ? `+${(storageImpactBytes / 1024).toFixed(1)}KB`
    : `${(storageImpactBytes / 1024).toFixed(1)}KB`;
  const summary = `Promoting "${path}" to ${suggestedType}: Storage impact ${storageImpactStr}, ` +
                  `Query speedup ~${querySpeedupFactor.toFixed(1)}x, Risk: ${riskLevel}`;

  return {
    path,
    storageImpactBytes,
    querySpeedupFactor,
    riskLevel,
    summary,
    affectedPatterns,
  };
}

/**
 * Generate a unique rollback ID
 */
function generateRollbackId(): string {
  return `rb_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
}

// ============================================================================
// Auto-Promote Manager Class
// ============================================================================

/**
 * Manages automatic field promotion based on configurable thresholds and patterns.
 *
 * The AutoPromoteManager analyzes field statistics from a FieldAnalyzer and
 * generates recommendations for which fields should be promoted to native
 * Parquet columns for efficient querying.
 *
 * New features:
 * - Configurable promotion strategies (conservative, aggressive, manual, adaptive)
 * - Dry-run mode for testing promotions without applying them
 * - Promotion impact analysis with storage and query performance estimates
 * - Rollback support to undo promotions
 *
 * @example
 * ```typescript
 * const manager = new AutoPromoteManager();
 * manager.configure({
 *   threshold: 0.8,
 *   minConsistency: 0.9,
 *   excludePatterns: ['_*', '*.password'],
 *   strategy: { type: 'conservative' },
 *   dryRun: true, // Test before applying
 * });
 *
 * // Evaluate with impact analysis
 * const result = manager.evaluate(analyzer);
 * console.log(result.impactAnalysis); // View impact before applying
 *
 * // Apply promotions with rollback support
 * const { schema, rollbackInfo } = manager.applyPromotionsWithRollback(schema, result.promotions);
 *
 * // Rollback if needed
 * const restoredSchema = manager.rollback(rollbackInfo.rollbackId);
 * ```
 */
export class AutoPromoteManager {
  private config: AutoPromoteConfig = { threshold: DEFAULT_THRESHOLD };
  private history: Map<string, PromotionHistoryEntry[]> = new Map();
  private cooldowns: Map<string, number> = new Map();
  private promotedFields: Set<string> = new Set();
  private rollbackRegistry: Map<string, PromotionRollback> = new Map();
  private adaptiveThresholds: { threshold: number; consistency: number } | null = null;

  /**
   * Configure auto-promotion settings
   */
  configure(config: AutoPromoteConfig): void {
    this.config = { ...config };
  }

  /**
   * Get current configuration
   */
  getConfig(): AutoPromoteConfig {
    return { ...this.config };
  }

  /**
   * Evaluate fields for auto-promotion based on current analysis
   */
  evaluate(analyzer: FieldAnalyzer): AutoPromoteResult {
    const now = new Date();
    const promotions: AutoPromoteCandidate[] = [];
    const skipped: SkippedField[] = [];
    const excluded: string[] = [];
    const impactAnalysis: PromotionImpact[] = [];
    const fieldStats = analyzer.getFieldStats();
    let totalEvaluated = 0;

    // Get effective thresholds based on strategy
    const strategy = this.config.strategy;
    const strategyType = strategy?.type ?? 'frequency';
    const isDryRun = this.config.dryRun ?? false;

    const { threshold: effectiveThreshold, consistency: effectiveConsistency } =
      this.getEffectiveThresholds();

    const minSampleSize = this.config.minSampleSize ?? 0;
    const includeNested = this.config.includeNested ?? true;
    const maxNestingDepth = this.config.maxNestingDepth ?? DEFAULT_MAX_NESTING_DEPTH;
    const includeArrays = this.config.includeArrays ?? false;
    const excludePatterns = this.config.excludePatterns ?? [];
    const includePatterns = this.config.includePatterns ?? [];

    // For manual strategy, get the list of fields to include
    const manualFields = new Set(strategy?.manualFields ?? []);

    for (const [path, stats] of fieldStats) {
      // For manual strategy, only process explicitly specified fields
      if (strategyType === 'manual' && !manualFields.has(path)) {
        continue;
      }

      // Check exclusion patterns first (this handles _id via patterns like '_*')
      const isExcluded = matchesAnyPattern(path, excludePatterns);
      const isIncluded = matchesAnyPattern(path, includePatterns);

      if (isExcluded && !isIncluded) {
        excluded.push(path);
        continue;
      }

      // Skip _id field unless explicitly included via patterns
      if (path === '_id' && !isIncluded) {
        continue;
      }

      // Skip already promoted fields
      if (this.promotedFields.has(path)) {
        continue;
      }

      // Count this field as evaluated
      totalEvaluated++;

      // Check nesting depth
      const depth = getNestingDepth(path);
      if (!includeNested && depth > 0) {
        skipped.push({
          path,
          reason: 'Nested fields excluded',
          frequency: stats.frequency,
          consistency: calculateConsistency(stats),
        });
        continue;
      }

      if (depth > maxNestingDepth) {
        skipped.push({
          path,
          reason: `Exceeds max nesting depth (${maxNestingDepth})`,
          frequency: stats.frequency,
          consistency: calculateConsistency(stats),
        });
        continue;
      }

      // Check sample size
      const documentCount = analyzer.getDocumentCount();
      if (documentCount < minSampleSize) {
        skipped.push({
          path,
          reason: `Insufficient sample size (${documentCount} < ${minSampleSize})`,
          frequency: stats.frequency,
          consistency: calculateConsistency(stats),
        });
        this.recordHistory(path, false, `Insufficient sample size (${documentCount} < ${minSampleSize})`, stats, documentCount, now, strategyType);
        continue;
      }

      // Check dominant type
      const dominantType = getDominantType(stats);

      // Skip object/null/mixed types (they're stored as variant anyway)
      if (dominantType === 'object' || dominantType === 'null' || dominantType === 'mixed') {
        const consistency = calculateConsistency(stats);
        skipped.push({
          path,
          reason: `Non-promotable type: ${dominantType}`,
          frequency: stats.frequency,
          consistency,
        });
        continue;
      }

      // Check array type
      if (dominantType === 'array' && !includeArrays) {
        skipped.push({
          path,
          reason: 'Array fields excluded',
          frequency: stats.frequency,
          consistency: calculateConsistency(stats),
        });
        continue;
      }

      // Calculate consistency
      const consistency = calculateConsistency(stats);

      // For manual strategy, skip threshold checks
      if (strategyType !== 'manual') {
        // Check frequency threshold
        if (stats.frequency < effectiveThreshold) {
          skipped.push({
            path,
            reason: `Below frequency threshold (${(stats.frequency * 100).toFixed(1)}% < ${(effectiveThreshold * 100).toFixed(1)}%)`,
            frequency: stats.frequency,
            consistency,
          });
          this.recordHistory(path, false, `Below frequency threshold`, stats, documentCount, now, strategyType);
          continue;
        }

        // Check consistency threshold
        if (consistency < effectiveConsistency) {
          skipped.push({
            path,
            reason: `Below consistency threshold (${(consistency * 100).toFixed(1)}% < ${(effectiveConsistency * 100).toFixed(1)}%)`,
            frequency: stats.frequency,
            consistency,
          });
          this.recordHistory(path, false, `Below consistency threshold`, stats, documentCount, now, strategyType);
          continue;
        }
      }

      // Field qualifies for promotion
      const suggestedType = toParquetType(dominantType);
      const priority = this.calculatePriority(stats.frequency, consistency);
      const reason = `Field appears in ${(stats.frequency * 100).toFixed(1)}% of documents with ${(consistency * 100).toFixed(1)}% ${dominantType} consistency`;

      promotions.push({
        path,
        suggestedType,
        frequency: stats.frequency,
        consistency,
        priority,
        reason,
      });

      // Calculate impact analysis for all promotions (especially useful in dry-run mode)
      const impact = calculatePromotionImpact(path, stats, suggestedType, documentCount);
      impactAnalysis.push(impact);

      this.recordHistory(path, true, reason, stats, documentCount, now, strategyType);

      // Set cooldown (skip in dry-run mode)
      if (!isDryRun && this.config.cooldownMs !== undefined && this.config.cooldownMs > 0) {
        this.cooldowns.set(path, now.getTime() + this.config.cooldownMs);
      }
    }

    // For adaptive strategy, update thresholds based on results
    if (strategyType === 'adaptive') {
      this.updateAdaptiveThresholds(promotions, skipped);
    }

    // Sort by priority (descending)
    promotions.sort((a, b) => b.priority - a.priority);

    // Apply batch size limit
    const maxBatchSize = this.config.maxBatchSize;
    const limitedPromotions = maxBatchSize !== undefined
      ? promotions.slice(0, maxBatchSize)
      : promotions;

    // Limit impact analysis to match limited promotions
    const limitedImpact = maxBatchSize !== undefined
      ? impactAnalysis.filter(impact =>
          limitedPromotions.some(p => p.path === impact.path)
        )
      : impactAnalysis;

    return {
      promotions: limitedPromotions,
      skipped,
      excluded,
      totalEvaluated,
      evaluatedAt: now,
      impactAnalysis: limitedImpact,
      isDryRun,
      strategyUsed: strategyType,
    };
  }

  /**
   * Get effective thresholds based on strategy and adaptive learning
   */
  private getEffectiveThresholds(): { threshold: number; consistency: number } {
    const baseThreshold = this.config.threshold;
    const baseConsistency = this.config.minConsistency ?? DEFAULT_MIN_CONSISTENCY;

    // Use adaptive thresholds if available
    if (this.adaptiveThresholds) {
      return this.adaptiveThresholds;
    }

    return getStrategyThresholds(this.config.strategy, baseThreshold, baseConsistency);
  }

  /**
   * Update adaptive thresholds based on evaluation results
   */
  private updateAdaptiveThresholds(
    promotions: AutoPromoteCandidate[],
    skipped: SkippedField[]
  ): void {
    if (!this.config.strategy || this.config.strategy.type !== 'adaptive') {
      return;
    }

    const learningRate = this.config.strategy.adaptiveLearningRate ?? 0.1;
    const current = this.adaptiveThresholds ?? {
      threshold: this.config.threshold,
      consistency: this.config.minConsistency ?? DEFAULT_MIN_CONSISTENCY,
    };

    // If too few promotions, lower thresholds; if too many, raise them
    const totalCandidates = promotions.length + skipped.length;
    if (totalCandidates === 0) {
      return;
    }

    const promotionRate = promotions.length / totalCandidates;

    // Target promotion rate of 20-40%
    let frequencyAdjustment = 0;
    let consistencyAdjustment = 0;

    if (promotionRate < 0.2) {
      // Too few promotions - lower thresholds
      frequencyAdjustment = -learningRate;
      consistencyAdjustment = -learningRate * 0.5;
    } else if (promotionRate > 0.4) {
      // Too many promotions - raise thresholds
      frequencyAdjustment = learningRate;
      consistencyAdjustment = learningRate * 0.5;
    }

    this.adaptiveThresholds = {
      threshold: Math.max(0.3, Math.min(0.99, current.threshold + frequencyAdjustment)),
      consistency: Math.max(0.5, Math.min(0.99, current.consistency + consistencyAdjustment)),
    };
  }

  /**
   * Get promotion candidates without committing
   */
  getCandidates(analyzer: FieldAnalyzer): AutoPromoteCandidate[] {
    return this.evaluate(analyzer).promotions;
  }

  /**
   * Check if a specific field should be promoted
   */
  shouldPromote(analyzer: FieldAnalyzer, fieldPath: string): boolean {
    const stats = analyzer.getFieldStat(fieldPath);
    if (!stats) {
      return false;
    }

    const threshold = this.config.threshold;
    const minConsistency = this.config.minConsistency ?? DEFAULT_MIN_CONSISTENCY;
    const minSampleSize = this.config.minSampleSize ?? 0;
    const excludePatterns = this.config.excludePatterns ?? [];
    const includePatterns = this.config.includePatterns ?? [];
    const includeArrays = this.config.includeArrays ?? false;
    const includeNested = this.config.includeNested ?? true;
    const maxNestingDepth = this.config.maxNestingDepth ?? DEFAULT_MAX_NESTING_DEPTH;

    // Check exclusion patterns
    const isExcluded = matchesAnyPattern(fieldPath, excludePatterns);
    const isIncluded = matchesAnyPattern(fieldPath, includePatterns);
    if (isExcluded && !isIncluded) {
      return false;
    }

    // Check nesting
    const depth = getNestingDepth(fieldPath);
    if (!includeNested && depth > 0) {
      return false;
    }
    if (depth > maxNestingDepth) {
      return false;
    }

    // Check sample size
    if (analyzer.getDocumentCount() < minSampleSize) {
      return false;
    }

    // Check dominant type
    const dominantType = getDominantType(stats);
    if (dominantType === 'object' || dominantType === 'null' || dominantType === 'mixed') {
      return false;
    }
    if (dominantType === 'array' && !includeArrays) {
      return false;
    }

    // Check frequency
    if (stats.frequency < threshold) {
      return false;
    }

    // Check consistency
    const consistency = calculateConsistency(stats);
    if (consistency < minConsistency) {
      return false;
    }

    return true;
  }

  /**
   * Get the priority score for a field
   */
  getPriority(analyzer: FieldAnalyzer, fieldPath: string): number {
    const stats = analyzer.getFieldStat(fieldPath);
    if (!stats) {
      return 0;
    }

    const consistency = calculateConsistency(stats);
    return this.calculatePriority(stats.frequency, consistency);
  }

  /**
   * Apply promotions to a schema
   */
  applyPromotions(
    schema: ParsedCollectionSchema,
    promotions: AutoPromoteCandidate[]
  ): ParsedCollectionSchema {
    // Check if in dry-run mode
    if (this.config.dryRun) {
      // In dry-run mode, return the schema unchanged but still validate promotions
      return schema;
    }

    // Create new columns list and map
    const columns = [...schema.columns];
    const columnMap = new Map(schema.columnMap);

    for (const promotion of promotions) {
      // Skip if column already exists
      if (columnMap.has(promotion.path)) {
        continue;
      }

      const column: ParsedColumn = {
        path: promotion.path,
        segments: promotion.path.split('.'),
        type: promotion.suggestedType,
        isArray: false,
        isStruct: false,
      };

      columns.push(column);
      columnMap.set(promotion.path, column);

      // Track this field as promoted
      this.promotedFields.add(promotion.path);
    }

    return {
      ...schema,
      columns,
      columnMap,
    };
  }

  /**
   * Apply promotions to a schema with rollback support
   */
  applyPromotionsWithRollback(
    schema: ParsedCollectionSchema,
    promotions: AutoPromoteCandidate[]
  ): { schema: ParsedCollectionSchema; rollbackInfo: PromotionRollback } {
    // Check if in dry-run mode
    if (this.config.dryRun) {
      const rollbackInfo: PromotionRollback = {
        rollbackId: 'dry_run_' + generateRollbackId(),
        originalSchema: schema,
        appliedPromotions: promotions,
        appliedAt: new Date(),
        canRollback: false,
        rollbackBlockedReason: 'Dry-run mode - no promotions were applied',
      };
      return { schema, rollbackInfo };
    }

    // Create rollback entry before applying
    const rollbackId = generateRollbackId();
    const originalSchema: ParsedCollectionSchema = {
      ...schema,
      columns: [...schema.columns],
      columnMap: new Map(schema.columnMap),
    };

    // Apply promotions
    const newSchema = this.applyPromotions(schema, promotions);

    // Create rollback info
    const rollbackInfo: PromotionRollback = {
      rollbackId,
      originalSchema,
      appliedPromotions: promotions,
      appliedAt: new Date(),
      canRollback: true,
    };

    // Store in registry
    this.rollbackRegistry.set(rollbackId, rollbackInfo);

    // Update history with rollback ID
    for (const promotion of promotions) {
      const history = this.history.get(promotion.path);
      if (history && history.length > 0) {
        history[history.length - 1]!.rollbackId = rollbackId;
      }
    }

    return { schema: newSchema, rollbackInfo };
  }

  /**
   * Rollback a promotion operation
   */
  rollback(rollbackId: string): ParsedCollectionSchema {
    const rollbackInfo = this.rollbackRegistry.get(rollbackId);

    if (!rollbackInfo) {
      throw new Error(`Rollback not found: ${rollbackId}`);
    }

    if (!rollbackInfo.canRollback) {
      throw new Error(`Cannot rollback: ${rollbackInfo.rollbackBlockedReason ?? 'Unknown reason'}`);
    }

    // Remove promoted fields from tracking
    for (const promotion of rollbackInfo.appliedPromotions) {
      this.promotedFields.delete(promotion.path);
    }

    // Mark rollback as used
    rollbackInfo.canRollback = false;
    rollbackInfo.rollbackBlockedReason = 'Rollback already executed';

    return rollbackInfo.originalSchema;
  }

  /**
   * Get rollback information by ID
   */
  getRollbackInfo(rollbackId: string): PromotionRollback | undefined {
    return this.rollbackRegistry.get(rollbackId);
  }

  /**
   * Get all available rollbacks
   */
  getAvailableRollbacks(): PromotionRollback[] {
    return Array.from(this.rollbackRegistry.values()).filter(r => r.canRollback);
  }

  /**
   * Clear old rollback entries
   */
  cleanupRollbacks(maxAge: number = 3600000): number {
    const now = Date.now();
    let cleaned = 0;

    for (const [_id, info] of this.rollbackRegistry) {
      const age = now - info.appliedAt.getTime();
      if (age > maxAge) {
        // Mark as expired instead of deleting to preserve history
        if (info.canRollback) {
          info.canRollback = false;
          info.rollbackBlockedReason = 'Rollback expired';
          cleaned++;
        }
      }
    }

    return cleaned;
  }

  /**
   * Perform a dry-run evaluation and return detailed impact analysis
   */
  dryRunEvaluate(analyzer: FieldAnalyzer): AutoPromoteResult {
    // Temporarily enable dry-run
    const originalDryRun = this.config.dryRun;
    this.config.dryRun = true;

    try {
      return this.evaluate(analyzer);
    } finally {
      // Restore original setting
      this.config.dryRun = originalDryRun;
    }
  }

  /**
   * Get a summary of promotion impact
   */
  getImpactSummary(impactAnalysis: PromotionImpact[]): {
    totalStorageImpact: number;
    avgQuerySpeedup: number;
    riskDistribution: { low: number; medium: number; high: number };
    recommendations: string[];
  } {
    if (impactAnalysis.length === 0) {
      return {
        totalStorageImpact: 0,
        avgQuerySpeedup: 1.0,
        riskDistribution: { low: 0, medium: 0, high: 0 },
        recommendations: ['No promotions to analyze'],
      };
    }

    const totalStorageImpact = impactAnalysis.reduce((sum, i) => sum + i.storageImpactBytes, 0);
    const avgQuerySpeedup = impactAnalysis.reduce((sum, i) => sum + i.querySpeedupFactor, 0) / impactAnalysis.length;

    const riskDistribution = { low: 0, medium: 0, high: 0 };
    for (const impact of impactAnalysis) {
      riskDistribution[impact.riskLevel]++;
    }

    const recommendations: string[] = [];

    if (riskDistribution.high > 0) {
      recommendations.push(`Consider reviewing ${riskDistribution.high} high-risk promotion(s) before applying`);
    }

    if (totalStorageImpact > 1024 * 1024) {
      recommendations.push(`Storage impact is significant (${(totalStorageImpact / 1024 / 1024).toFixed(1)}MB). Consider batching promotions.`);
    }

    if (avgQuerySpeedup > 2.0) {
      recommendations.push(`Expected query performance improvement: ${avgQuerySpeedup.toFixed(1)}x average speedup`);
    }

    if (recommendations.length === 0) {
      recommendations.push('All promotions appear safe to apply');
    }

    return {
      totalStorageImpact,
      avgQuerySpeedup,
      riskDistribution,
      recommendations,
    };
  }

  /**
   * Set the promotion strategy
   */
  setStrategy(strategy: StrategyConfig): void {
    this.config.strategy = strategy;
    // Reset adaptive thresholds when strategy changes
    if (strategy.type !== 'adaptive') {
      this.adaptiveThresholds = null;
    }
  }

  /**
   * Get the current strategy
   */
  getStrategy(): StrategyConfig | undefined {
    return this.config.strategy;
  }

  /**
   * Get current adaptive thresholds (if using adaptive strategy)
   */
  getAdaptiveThresholds(): { threshold: number; consistency: number } | null {
    return this.adaptiveThresholds;
  }

  /**
   * Get promotion history for a field
   */
  getPromotionHistory(fieldPath: string): PromotionHistoryEntry[] {
    return this.history.get(fieldPath) ?? [];
  }

  /**
   * Reset cooldown for a field
   */
  resetCooldown(fieldPath: string): void {
    this.cooldowns.delete(fieldPath);
  }

  /**
   * Check if a field is in cooldown
   */
  isInCooldown(fieldPath: string): boolean {
    const cooldownEnd = this.cooldowns.get(fieldPath);
    if (cooldownEnd === undefined) {
      return false;
    }
    return Date.now() < cooldownEnd;
  }

  // ============================================================================
  // Private Methods
  // ============================================================================

  /**
   * Calculate priority score from frequency and consistency
   */
  private calculatePriority(frequency: number, consistency: number): number {
    // Weight frequency higher than consistency
    return frequency * 0.7 + consistency * 0.3;
  }

  /**
   * Record a decision in the history
   */
  private recordHistory(
    path: string,
    promoted: boolean,
    reason: string,
    stats: FieldStats,
    sampleSize: number,
    timestamp: Date,
    strategy?: PromotionStrategy
  ): void {
    const entry: PromotionHistoryEntry = {
      path,
      timestamp,
      promoted,
      reason,
      stats: {
        frequency: stats.frequency,
        consistency: calculateConsistency(stats),
        sampleSize,
      },
      strategy,
    };

    const existing = this.history.get(path) ?? [];
    existing.push(entry);
    this.history.set(path, existing);
  }

  /**
   * Clear all state (useful for testing)
   */
  reset(): void {
    this.history.clear();
    this.cooldowns.clear();
    this.promotedFields.clear();
    this.rollbackRegistry.clear();
    this.adaptiveThresholds = null;
  }
}

// ============================================================================
// Factory Functions
// ============================================================================

/**
 * Create a new AutoPromoteManager instance
 */
export function createAutoPromoteManager(config?: AutoPromoteConfig): AutoPromoteManager {
  const manager = new AutoPromoteManager();
  if (config) {
    manager.configure(config);
  }
  return manager;
}

/**
 * Create a conservative auto-promote manager
 * Only promotes fields with very high frequency and consistency
 */
export function createConservativeManager(baseConfig?: Partial<AutoPromoteConfig>): AutoPromoteManager {
  return createAutoPromoteManager({
    threshold: CONSERVATIVE_THRESHOLD,
    minConsistency: CONSERVATIVE_CONSISTENCY,
    ...baseConfig,
    strategy: { type: 'conservative' },
  });
}

/**
 * Create an aggressive auto-promote manager
 * Promotes fields more liberally with lower thresholds
 */
export function createAggressiveManager(baseConfig?: Partial<AutoPromoteConfig>): AutoPromoteManager {
  return createAutoPromoteManager({
    threshold: AGGRESSIVE_THRESHOLD,
    minConsistency: AGGRESSIVE_CONSISTENCY,
    ...baseConfig,
    strategy: { type: 'aggressive' },
  });
}

/**
 * Create a manual auto-promote manager
 * Only promotes explicitly specified fields
 */
export function createManualManager(fields: string[], baseConfig?: Partial<AutoPromoteConfig>): AutoPromoteManager {
  return createAutoPromoteManager({
    threshold: 0, // Ignored for manual
    ...baseConfig,
    strategy: { type: 'manual', manualFields: fields },
  });
}

/**
 * Create an adaptive auto-promote manager
 * Adjusts thresholds based on data characteristics
 */
export function createAdaptiveManager(baseConfig?: Partial<AutoPromoteConfig>): AutoPromoteManager {
  return createAutoPromoteManager({
    threshold: DEFAULT_THRESHOLD,
    ...baseConfig,
    strategy: { type: 'adaptive', adaptiveLearningRate: 0.1 },
  });
}

/**
 * Create a dry-run auto-promote manager for testing
 */
export function createDryRunManager(config?: AutoPromoteConfig): AutoPromoteManager {
  return createAutoPromoteManager({
    ...config,
    threshold: config?.threshold ?? DEFAULT_THRESHOLD,
    dryRun: true,
  });
}

// ============================================================================
// Global Registration for Tests
// ============================================================================

// Extend globalThis type for test compatibility
declare global {
  // eslint-disable-next-line no-var
  var AutoPromoteManager: typeof import('./auto-promote.js').AutoPromoteManager;
}

// Register AutoPromoteManager on globalThis for test compatibility
globalThis.AutoPromoteManager = AutoPromoteManager;
