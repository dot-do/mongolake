/**
 * Field Analyzer for Schema Evolution
 *
 * Analyzes documents to detect field types and track field frequency
 * for schema evolution and field promotion decisions.
 *
 * Key features:
 * - Analyze documents to detect field types
 * - Track field frequency across documents
 * - Identify candidates for promotion based on frequency threshold
 * - Support nested field analysis with dot notation paths
 * - Memory-efficient sampling with deduplication
 * - Streaming-compatible incremental analysis
 *
 * @module schema/analyzer
 */

import type { ParquetType } from '../types.js';

// ============================================================================
// Type Definitions
// ============================================================================

/**
 * Detected field type from document analysis.
 * These map to MongoDB/BSON types for compatibility.
 */
export type DetectedType =
  | 'string'
  | 'number'
  | 'boolean'
  | 'date'
  | 'binary'
  | 'null'
  | 'array'
  | 'object'
  | 'objectId'
  | 'mixed';

/**
 * Statistics for a single field.
 * Provides comprehensive information about field presence, types, and samples.
 */
export interface FieldStats {
  /** Field path using dot notation (e.g., 'user.profile.name') */
  path: string;
  /** Number of documents containing this field */
  count: number;
  /** Frequency as a ratio (0-1) of documents containing this field */
  frequency: number;
  /** Detected types for this field with occurrence counts */
  types: Map<DetectedType, number>;
  /** The most common non-null type */
  dominantType: DetectedType;
  /** Whether this field has consistent typing (single non-null type) */
  isConsistent: boolean;
  /** Whether this is a nested field (has parent object) */
  isNested: boolean;
  /** Sample values (up to configured limit, excludes null/undefined) */
  sampleValues: unknown[];
}

/**
 * Promotion suggestion for a field.
 * Used to recommend fields for Parquet column promotion.
 */
export interface PromotionSuggestion {
  /** Field path */
  path: string;
  /** Frequency of the field across documents */
  frequency: number;
  /** Suggested Parquet type for the field */
  suggestedType: ParquetType;
  /** Confidence level (0-1) based on type consistency */
  confidence: number;
  /** Whether this is an array type */
  isArray: boolean;
  /** Human-readable reason for the suggestion */
  reason: string;
}

/**
 * Options for analyzing documents.
 */
export interface AnalyzeOptions {
  /** Maximum nesting depth to analyze (default: 10) */
  maxDepth?: number;
  /** Maximum number of sample values to keep per field (default: 5) */
  maxSamples?: number;
  /** Fields to exclude from analysis (supports glob patterns) */
  excludeFields?: string[];
}

/**
 * Options for promotion suggestions.
 */
export interface PromotionOptions {
  /** Minimum frequency threshold (0-1) for promotion (default: 0.8) */
  threshold?: number;
  /** Minimum type consistency (0-1) for promotion (default: 0.9) */
  minConsistency?: number;
  /** Maximum number of suggestions to return */
  maxSuggestions?: number;
  /** Exclude array fields from suggestions */
  excludeArrays?: boolean;
  /** Exclude nested object fields from suggestions */
  excludeObjects?: boolean;
}

/**
 * Statistics about array elements for a field.
 * Provides aggregate information about array length and element types.
 */
export interface ArrayElementStats {
  /** Average length of arrays */
  avgLength: number;
  /** Minimum length of arrays */
  minLength: number;
  /** Maximum length of arrays */
  maxLength: number;
  /** Dominant type of array elements */
  elementType: DetectedType;
}

/**
 * Serialized form of FieldAnalyzer state.
 * Used for persisting and restoring analyzer state.
 */
export interface SerializedFieldAnalyzer {
  /** Number of documents analyzed */
  documentCount: number;
  /** Field statistics keyed by path */
  fields: Record<string, SerializedFieldData>;
  /** Configuration options */
  options: SerializedOptions;
}

/**
 * Serialized field data for a single field.
 */
interface SerializedFieldData {
  count: number;
  types: Record<string, number>;
  samples: unknown[];
  isNested: boolean;
}

/**
 * Serialized analyzer options.
 */
interface SerializedOptions {
  maxSamples: number;
  maxDepth: number;
  excludePatterns: string[];
}

// ============================================================================
// Internal Types
// ============================================================================

/**
 * Internal accumulator for field statistics.
 * Tracks mutable state during analysis.
 */
interface FieldAccumulator {
  count: number;
  types: Map<DetectedType, number>;
  samples: unknown[];
  isNested: boolean;
  arrayLengths?: number[];
  arrayElementTypes?: Map<DetectedType, number>;
}

/**
 * Result of dominant type calculation.
 */
interface DominantTypeResult {
  dominantType: DetectedType;
  consistency: number;
  isConsistent: boolean;
}

// ============================================================================
// Constants
// ============================================================================

/** Default maximum nesting depth for analysis */
const DEFAULT_MAX_DEPTH = 10;

/** Default maximum sample values to keep per field */
const DEFAULT_MAX_SAMPLES = 5;

/** Parquet type mappings from detected types */
const PARQUET_TYPE_MAP: Readonly<Record<DetectedType, ParquetType>> = {
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

// ============================================================================
// Field Analyzer Class
// ============================================================================

/**
 * Analyzes documents to detect field types and track frequency for schema evolution.
 *
 * The FieldAnalyzer processes batches of documents and maintains statistics
 * about field occurrence and types, which can be used to suggest fields
 * for promotion to native Parquet columns.
 *
 * Supports incremental analysis for streaming large datasets and can be
 * serialized/deserialized for distributed processing.
 *
 * @example
 * ```typescript
 * const analyzer = new FieldAnalyzer();
 *
 * // Analyze a batch of documents
 * analyzer.analyzeDocuments([
 *   { name: 'Alice', age: 30, email: 'alice@example.com' },
 *   { name: 'Bob', age: 25 },
 *   { name: 'Charlie', age: 35, email: 'charlie@example.com' },
 * ]);
 *
 * // Get statistics for each field
 * const stats = analyzer.getFieldStats();
 * console.log(stats.get('email')?.frequency); // 0.667
 *
 * // Get promotion suggestions
 * const suggestions = analyzer.suggestPromotions(0.5);
 * // Returns fields appearing in >= 50% of documents
 * ```
 */
export class FieldAnalyzer {
  private readonly fields: Map<string, FieldAccumulator> = new Map();
  private documentCount = 0;
  private readonly maxSamples: number;
  private readonly maxDepth: number;
  private readonly excludePatterns: readonly RegExp[];

  constructor(options: AnalyzeOptions = {}) {
    this.maxSamples = options.maxSamples ?? DEFAULT_MAX_SAMPLES;
    this.maxDepth = options.maxDepth ?? DEFAULT_MAX_DEPTH;
    this.excludePatterns = Object.freeze(
      (options.excludeFields ?? []).map((pattern) => globToRegex(pattern))
    );
  }

  /**
   * Analyze a batch of documents.
   * Supports incremental analysis - can be called multiple times.
   *
   * @param documents - Array of documents to analyze
   * @param options - Analysis options (override constructor options for this batch)
   */
  analyzeDocuments(
    documents: Record<string, unknown>[],
    options?: AnalyzeOptions
  ): void {
    const effectiveMaxDepth = options?.maxDepth ?? this.maxDepth;
    const effectiveExcludePatterns = options?.excludeFields
      ? options.excludeFields.map((p) => globToRegex(p))
      : this.excludePatterns;

    for (const doc of documents) {
      this.documentCount++;
      this.analyzeObject(doc, '', 0, effectiveMaxDepth, effectiveExcludePatterns);
    }
  }

  /**
   * Get statistics for all analyzed fields.
   *
   * @returns Map of field path to FieldStats
   */
  getFieldStats(): Map<string, FieldStats> {
    const stats = new Map<string, FieldStats>();

    for (const [path, accumulator] of this.fields) {
      stats.set(path, this.buildFieldStats(path, accumulator));
    }

    return stats;
  }

  /**
   * Get statistics for a specific field.
   *
   * @param path - Field path using dot notation
   * @returns FieldStats or undefined if field not found
   */
  getFieldStat(path: string): FieldStats | undefined {
    const accumulator = this.fields.get(path);
    if (!accumulator) {
      return undefined;
    }

    return this.buildFieldStats(path, accumulator);
  }

  /**
   * Build FieldStats from an accumulator.
   * Internal helper to avoid code duplication.
   */
  private buildFieldStats(path: string, accumulator: FieldAccumulator): FieldStats {
    const frequency = this.documentCount > 0
      ? accumulator.count / this.documentCount
      : 0;

    const { dominantType, isConsistent } = this.getDominantType(accumulator.types);

    return {
      path,
      count: accumulator.count,
      frequency,
      types: new Map(accumulator.types),
      dominantType,
      isConsistent,
      isNested: accumulator.isNested,
      sampleValues: [...accumulator.samples],
    };
  }

  /**
   * Suggest fields for promotion based on frequency threshold.
   *
   * @param threshold - Minimum frequency (0-1) for promotion (default: 0.8)
   * @param options - Additional options for filtering suggestions
   * @returns Array of promotion suggestions sorted by frequency (descending)
   */
  suggestPromotions(
    threshold = 0.8,
    options: PromotionOptions = {}
  ): PromotionSuggestion[] {
    const {
      minConsistency = 0.9,
      maxSuggestions,
      excludeArrays = false,
      excludeObjects = true,
    } = options;

    const suggestions: PromotionSuggestion[] = [];

    for (const [path, accumulator] of this.fields) {
      const suggestion = this.evaluateFieldForPromotion(
        path,
        accumulator,
        threshold,
        minConsistency,
        excludeArrays,
        excludeObjects
      );

      if (suggestion !== null) {
        suggestions.push(suggestion);
      }
    }

    // Sort by frequency (descending), then by confidence (descending)
    suggestions.sort(compareSuggestions);

    // Limit results if requested
    return maxSuggestions !== undefined && suggestions.length > maxSuggestions
      ? suggestions.slice(0, maxSuggestions)
      : suggestions;
  }

  /**
   * Evaluate a single field for promotion eligibility.
   * Returns null if the field should not be promoted.
   */
  private evaluateFieldForPromotion(
    path: string,
    accumulator: FieldAccumulator,
    threshold: number,
    minConsistency: number,
    excludeArrays: boolean,
    excludeObjects: boolean
  ): PromotionSuggestion | null {
    const frequency = this.documentCount > 0
      ? accumulator.count / this.documentCount
      : 0;

    // Skip fields below frequency threshold
    if (frequency < threshold) {
      return null;
    }

    const { dominantType, consistency } = this.getDominantType(accumulator.types);

    // Skip fields that don't meet consistency requirements
    if (consistency < minConsistency) {
      return null;
    }

    // Skip excluded types
    if (excludeArrays && dominantType === 'array') {
      return null;
    }
    if (excludeObjects && dominantType === 'object') {
      return null;
    }

    // Skip non-promotable types
    if (dominantType === 'null' || dominantType === 'mixed') {
      return null;
    }

    return {
      path,
      frequency,
      suggestedType: PARQUET_TYPE_MAP[dominantType],
      confidence: consistency,
      isArray: dominantType === 'array',
      reason: generatePromotionReason(path, frequency, consistency, dominantType),
    };
  }

  /**
   * Get the total number of documents analyzed.
   */
  getDocumentCount(): number {
    return this.documentCount;
  }

  /**
   * Get all unique field paths.
   */
  getFieldPaths(): string[] {
    return Array.from(this.fields.keys());
  }

  /**
   * Reset the analyzer, clearing all accumulated statistics.
   */
  reset(): void {
    this.fields.clear();
    this.documentCount = 0;
  }

  /**
   * Merge statistics from another analyzer.
   * Useful for distributed/parallel analysis where batches are processed separately.
   *
   * @param other - Another FieldAnalyzer to merge from
   */
  merge(other: FieldAnalyzer): void {
    this.documentCount += other.documentCount;

    for (const [path, otherAcc] of other.fields) {
      const existing = this.fields.get(path);

      if (existing) {
        this.mergeAccumulators(existing, otherAcc);
      } else {
        // Clone the accumulator from other analyzer
        this.fields.set(path, {
          count: otherAcc.count,
          types: new Map(otherAcc.types),
          samples: [...otherAcc.samples].slice(0, this.maxSamples),
          isNested: otherAcc.isNested,
        });
      }
    }
  }

  /**
   * Merge two field accumulators.
   */
  private mergeAccumulators(target: FieldAccumulator, source: FieldAccumulator): void {
    target.count += source.count;
    target.isNested = target.isNested || source.isNested;

    // Merge type counts
    for (const [type, count] of source.types) {
      incrementTypeCount(target.types, type, count);
    }

    // Merge samples with deduplication
    const combinedSamples = [...target.samples, ...source.samples];
    target.samples = deduplicateSamples(combinedSamples).slice(0, this.maxSamples);
  }

  // ============================================================================
  // Private Methods
  // ============================================================================

  /**
   * Recursively analyze an object's fields.
   */
  private analyzeObject(
    obj: Record<string, unknown>,
    prefix: string,
    depth: number,
    maxDepth: number,
    excludePatterns: readonly RegExp[]
  ): void {
    if (depth > maxDepth) {
      return;
    }

    for (const [key, value] of Object.entries(obj)) {
      const path = prefix ? `${prefix}.${key}` : key;

      // Check exclusion patterns
      if (isExcluded(path, excludePatterns)) {
        continue;
      }

      const isNested = prefix !== '';
      this.recordField(path, value, isNested);

      // Recursively analyze nested objects
      if (isPlainObject(value)) {
        this.analyzeObject(
          value as Record<string, unknown>,
          path,
          depth + 1,
          maxDepth,
          excludePatterns
        );
      }

      // Analyze array elements for type detection (first element only)
      if (Array.isArray(value) && value.length > 0) {
        const firstElement = value[0];
        if (isPlainObject(firstElement)) {
          this.analyzeObject(
            firstElement as Record<string, unknown>,
            `${path}[]`,
            depth + 1,
            maxDepth,
            excludePatterns
          );
        }
      }
    }
  }

  /**
   * Record a field occurrence with its value.
   */
  private recordField(path: string, value: unknown, isNested: boolean): void {
    const accumulator = this.getOrCreateAccumulator(path, isNested);

    accumulator.count++;
    accumulator.isNested = accumulator.isNested || isNested;

    const detectedType = detectType(value);
    incrementTypeCount(accumulator.types, detectedType);

    // Track array statistics
    if (Array.isArray(value)) {
      this.recordArrayStats(accumulator, value);
    }

    // Add sample value (excluding null/undefined)
    this.addSampleValue(accumulator, value);
  }

  /**
   * Get existing accumulator or create a new one.
   */
  private getOrCreateAccumulator(path: string, isNested: boolean): FieldAccumulator {
    let accumulator = this.fields.get(path);

    if (!accumulator) {
      accumulator = {
        count: 0,
        types: new Map(),
        samples: [],
        isNested,
      };
      this.fields.set(path, accumulator);
    }

    return accumulator;
  }

  /**
   * Record array length and element type statistics.
   */
  private recordArrayStats(accumulator: FieldAccumulator, value: unknown[]): void {
    if (!accumulator.arrayLengths) {
      accumulator.arrayLengths = [];
    }
    accumulator.arrayLengths.push(value.length);

    if (!accumulator.arrayElementTypes) {
      accumulator.arrayElementTypes = new Map();
    }
    for (const element of value) {
      const elementType = detectType(element);
      incrementTypeCount(accumulator.arrayElementTypes, elementType);
    }
  }

  /**
   * Add a sample value if within limits and not null/undefined.
   */
  private addSampleValue(accumulator: FieldAccumulator, value: unknown): void {
    if (accumulator.samples.length >= this.maxSamples) {
      return;
    }

    // Skip null/undefined values
    if (value === null || value === undefined) {
      return;
    }

    // Check for duplicates
    if (typeof value === 'object') {
      const valueStr = JSON.stringify(value);
      if (!accumulator.samples.some(s => JSON.stringify(s) === valueStr)) {
        accumulator.samples.push(value);
      }
    } else {
      if (!accumulator.samples.includes(value)) {
        accumulator.samples.push(value);
      }
    }
  }

  /**
   * Get the dominant type and consistency ratio.
   */
  private getDominantType(types: Map<DetectedType, number>): DominantTypeResult {
    if (types.size === 0) {
      return { dominantType: 'null', consistency: 0, isConsistent: false };
    }

    // Find dominant non-null type and total non-null count
    let totalNonNull = 0;
    let dominant: DetectedType = 'null';
    let dominantCount = 0;

    for (const [type, count] of types) {
      if (type !== 'null') {
        totalNonNull += count;
        if (count > dominantCount) {
          dominantCount = count;
          dominant = type;
        }
      }
    }

    // Handle null-only case
    if (totalNonNull === 0) {
      const nullCount = types.get('null') ?? 0;
      return {
        dominantType: 'null',
        consistency: nullCount > 0 ? 1 : 0,
        isConsistent: true,
      };
    }

    // Calculate consistency as ratio of dominant to total non-null
    const consistency = dominantCount / totalNonNull;

    return {
      dominantType: dominant,
      consistency,
      isConsistent: consistency === 1,
    };
  }

  // ============================================================================
  // New Methods for RED Phase Tests
  // ============================================================================

  /**
   * Get type distribution percentages for a field.
   *
   * @param path - Field path using dot notation
   * @returns Map of type to percentage (0-1), or undefined if field not found
   */
  getTypeDistribution(path: string): Map<DetectedType, number> | undefined {
    const accumulator = this.fields.get(path);
    if (!accumulator) {
      return undefined;
    }

    const total = accumulator.count;
    if (total === 0) {
      return new Map();
    }

    const distribution = new Map<DetectedType, number>();
    for (const [type, count] of accumulator.types) {
      distribution.set(type, count / total);
    }

    return distribution;
  }

  /**
   * Get the ratio of null values for a field.
   *
   * @param path - Field path using dot notation
   * @returns Ratio of null values (0-1), or undefined if field not found
   */
  getNullRatio(path: string): number | undefined {
    const accumulator = this.fields.get(path);
    if (!accumulator) {
      return undefined;
    }

    if (accumulator.count === 0) {
      return 0;
    }

    const nullCount = accumulator.types.get('null') ?? 0;
    return nullCount / accumulator.count;
  }

  /**
   * Get the ratio of documents missing a field.
   *
   * @param path - Field path using dot notation
   * @returns Ratio of missing documents (0-1)
   */
  getMissingRatio(path: string): number {
    const accumulator = this.fields.get(path);
    if (!accumulator) {
      return 1; // Field doesn't exist, so it's missing in all documents
    }

    if (this.documentCount === 0) {
      return 0;
    }

    return 1 - (accumulator.count / this.documentCount);
  }

  /**
   * Get fields matching a glob pattern.
   *
   * @param pattern - Glob pattern (e.g., "user.*", "*.name")
   * @returns Array of matching field paths
   */
  getFieldsByPattern(pattern: string): string[] {
    const regex = globToRegex(pattern);
    const result: string[] = [];

    for (const path of this.fields.keys()) {
      if (regex.test(path)) {
        result.push(path);
      }
    }

    return result;
  }

  /**
   * Get only nested fields (fields with dots in their path).
   *
   * @returns Array of nested field paths
   */
  getNestedFields(): string[] {
    const result: string[] = [];

    for (const [path, accumulator] of this.fields) {
      if (accumulator.isNested) {
        result.push(path);
      }
    }

    return result;
  }

  /**
   * Get only root-level fields (fields without dots in their path).
   *
   * @returns Array of root-level field paths
   */
  getRootFields(): string[] {
    const result: string[] = [];

    for (const [path, accumulator] of this.fields) {
      if (!accumulator.isNested) {
        result.push(path);
      }
    }

    return result;
  }

  /**
   * Get fields at a specific nesting depth.
   *
   * @param depth - Nesting depth (0 = root level)
   * @returns Array of field paths at the specified depth
   */
  getFieldsAtDepth(depth: number): string[] {
    const result: string[] = [];

    for (const path of this.fields.keys()) {
      // Count dots to determine depth
      // Root fields have depth 0, "a.b" has depth 1, "a.b.c" has depth 2
      const fieldDepth = (path.match(/\./g) || []).length;
      if (fieldDepth === depth) {
        result.push(path);
      }
    }

    return result;
  }

  /**
   * Get statistics about array elements for a field.
   *
   * @param path - Field path using dot notation
   * @returns ArrayElementStats or undefined if field is not an array
   */
  getArrayElementStats(path: string): ArrayElementStats | undefined {
    const accumulator = this.fields.get(path);
    if (!accumulator || !accumulator.arrayLengths || accumulator.arrayLengths.length === 0) {
      return undefined;
    }

    const lengths = accumulator.arrayLengths;
    const avgLength = lengths.reduce((a, b) => a + b, 0) / lengths.length;
    const minLength = Math.min(...lengths);
    const maxLength = Math.max(...lengths);

    // Determine dominant element type
    let elementType: DetectedType = 'mixed';
    if (accumulator.arrayElementTypes && accumulator.arrayElementTypes.size > 0) {
      let maxCount = 0;
      for (const [type, count] of accumulator.arrayElementTypes) {
        if (count > maxCount) {
          maxCount = count;
          elementType = type;
        }
      }
    }

    return {
      avgLength,
      minLength,
      maxLength,
      elementType,
    };
  }

  /**
   * Serialize the analyzer state to a JSON-serializable object.
   *
   * @returns Serializable representation of the analyzer state
   */
  toJSON(): SerializedFieldAnalyzer {
    const fields: SerializedFieldAnalyzer['fields'] = {};

    for (const [path, accumulator] of this.fields) {
      const types: Record<string, number> = {};
      for (const [type, count] of accumulator.types) {
        types[type] = count;
      }

      fields[path] = {
        count: accumulator.count,
        types,
        samples: [...accumulator.samples],
        isNested: accumulator.isNested,
      };
    }

    return {
      documentCount: this.documentCount,
      fields,
      options: {
        maxSamples: this.maxSamples,
        maxDepth: this.maxDepth,
        excludePatterns: this.excludePatterns.map((r) => r.source),
      },
    };
  }

  /**
   * Restore analyzer state from a serialized object.
   *
   * @param data - Serialized analyzer state
   * @returns New FieldAnalyzer instance with restored state
   */
  static fromJSON(data: SerializedFieldAnalyzer): FieldAnalyzer {
    const analyzer = new FieldAnalyzer({
      maxSamples: data.options.maxSamples,
      maxDepth: data.options.maxDepth,
    });

    analyzer.documentCount = data.documentCount;

    for (const [path, fieldData] of Object.entries(data.fields)) {
      const types = new Map<DetectedType, number>();
      for (const [type, count] of Object.entries(fieldData.types)) {
        types.set(type as DetectedType, count);
      }

      analyzer.fields.set(path, {
        count: fieldData.count,
        types,
        samples: [...fieldData.samples],
        isNested: fieldData.isNested,
      });
    }

    return analyzer;
  }
}

// ============================================================================
// Module-Level Helper Functions
// ============================================================================

/**
 * Convert a glob pattern to a regex.
 * Supports * (any characters) and ? (single character) wildcards.
 */
function globToRegex(pattern: string): RegExp {
  const escaped = pattern
    .replace(/[.+^${}()|[\]\\]/g, '\\$&')
    .replace(/\*/g, '.*')
    .replace(/\?/g, '.');

  return new RegExp(`^${escaped}$`);
}

/**
 * Check if a path matches any exclusion pattern.
 */
function isExcluded(path: string, patterns: readonly RegExp[]): boolean {
  return patterns.some((pattern) => pattern.test(path));
}

/**
 * Detect the type of a value.
 */
function detectType(value: unknown): DetectedType {
  if (value === null || value === undefined) {
    return 'null';
  }

  switch (typeof value) {
    case 'string':
      return 'string';
    case 'number':
      return 'number';
    case 'boolean':
      return 'boolean';
  }

  if (value instanceof Date) {
    return 'date';
  }

  if (value instanceof Uint8Array) {
    return 'binary';
  }

  if (Array.isArray(value)) {
    return 'array';
  }

  if (isObjectId(value)) {
    return 'objectId';
  }

  if (typeof value === 'object') {
    return 'object';
  }

  return 'mixed';
}

/**
 * Check if a value looks like an ObjectId.
 */
function isObjectId(value: unknown): boolean {
  if (typeof value !== 'object' || value === null) {
    return false;
  }

  // Check for ObjectId class instance by constructor name
  const proto = Object.getPrototypeOf(value);
  if (proto?.constructor?.name === 'ObjectId') {
    return true;
  }

  // Check for ObjectId-like shape (has toString and toHexString returning 24 hex chars)
  const obj = value as { toString?: () => string; toHexString?: () => string };
  if (typeof obj.toString === 'function' && typeof obj.toHexString === 'function') {
    const str = obj.toString();
    return /^[0-9a-fA-F]{24}$/.test(str);
  }

  return false;
}

/**
 * Check if a value is a plain object (not an array, Date, etc.).
 */
function isPlainObject(value: unknown): boolean {
  if (typeof value !== 'object' || value === null) {
    return false;
  }

  if (Array.isArray(value)) {
    return false;
  }

  if (value instanceof Date || value instanceof Uint8Array) {
    return false;
  }

  if (isObjectId(value)) {
    return false;
  }

  return true;
}

/**
 * Increment a type count in a type map.
 */
function incrementTypeCount(
  types: Map<DetectedType, number>,
  type: DetectedType,
  amount = 1
): void {
  types.set(type, (types.get(type) ?? 0) + amount);
}

/**
 * Deduplicate sample values using JSON serialization.
 */
function deduplicateSamples(samples: unknown[]): unknown[] {
  const seen = new Set<string>();
  const result: unknown[] = [];

  for (const sample of samples) {
    const key = JSON.stringify(sample);
    if (!seen.has(key)) {
      seen.add(key);
      result.push(sample);
    }
  }

  return result;
}

/**
 * Generate a human-readable reason for a promotion suggestion.
 */
function generatePromotionReason(
  path: string,
  frequency: number,
  consistency: number,
  type: DetectedType
): string {
  const freqPct = (frequency * 100).toFixed(1);

  if (consistency === 1) {
    return `Field '${path}' appears in ${freqPct}% of documents with consistent ${type} type`;
  }

  const consistencyPct = (consistency * 100).toFixed(1);
  return `Field '${path}' appears in ${freqPct}% of documents, ${consistencyPct}% are ${type}`;
}

/**
 * Compare two promotion suggestions for sorting.
 * Sorts by frequency (descending), then by confidence (descending).
 */
function compareSuggestions(a: PromotionSuggestion, b: PromotionSuggestion): number {
  if (b.frequency !== a.frequency) {
    return b.frequency - a.frequency;
  }
  return b.confidence - a.confidence;
}

// ============================================================================
// Exported Utility Functions
// ============================================================================

/**
 * Create a new FieldAnalyzer instance.
 */
export function createFieldAnalyzer(options?: AnalyzeOptions): FieldAnalyzer {
  return new FieldAnalyzer(options);
}

/**
 * Analyze documents and return field statistics.
 *
 * @param documents - Documents to analyze
 * @param options - Analysis options
 * @returns Map of field path to FieldStats
 */
export function analyzeDocuments(
  documents: Record<string, unknown>[],
  options?: AnalyzeOptions
): Map<string, FieldStats> {
  const analyzer = new FieldAnalyzer(options);
  analyzer.analyzeDocuments(documents);
  return analyzer.getFieldStats();
}

/**
 * Get promotion suggestions for a set of documents.
 *
 * @param documents - Documents to analyze
 * @param threshold - Minimum frequency threshold (default: 0.8)
 * @param options - Analysis and promotion options
 * @returns Array of promotion suggestions
 */
export function suggestFieldPromotions(
  documents: Record<string, unknown>[],
  threshold: number = 0.8,
  options?: AnalyzeOptions & PromotionOptions
): PromotionSuggestion[] {
  const analyzer = new FieldAnalyzer(options);
  analyzer.analyzeDocuments(documents);
  return analyzer.suggestPromotions(threshold, options);
}
