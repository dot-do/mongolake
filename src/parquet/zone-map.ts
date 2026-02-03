/**
 * Zone Map Generator
 *
 * Zone maps in MongoLake:
 * - Track min/max values per row group for each column
 * - Enable predicate pushdown (skip row groups that can't match)
 * - Work with MongoDB document fields
 * - Handle nested field paths
 *
 * Zone maps are a critical optimization for query performance in analytical
 * workloads. By tracking column statistics at the row group level, we can
 * skip entire row groups that cannot possibly contain matching data.
 */

// ============================================================================
// Type Definitions
// ============================================================================

/** Supported zone map value types */
export type ZoneMapValue = string | number | bigint | Date | boolean | null;

/** Field types for zone maps */
export type ZoneMapFieldType = 'string' | 'number' | 'bigint' | 'date' | 'boolean' | 'mixed';

/** Result of predicate evaluation */
export type PredicateResult = 'MATCH' | 'NO_MATCH' | 'UNKNOWN';

/** Range predicate operators */
export interface RangePredicate {
  op: '$eq' | '$ne' | '$gt' | '$gte' | '$lt' | '$lte' | '$in' | '$nin';
  value: ZoneMapValue | ZoneMapValue[];
}

/** Zone map entry for a row group */
export interface ZoneMapEntry {
  rowGroupId: string;
  min: ZoneMapValue;
  max: ZoneMapValue;
  nullCount: number;
  hasNull: boolean;
  allNull: boolean;
  rowCount: number;
  // Mixed type support - used when field contains values of different types
  hasMixedTypes?: boolean;
  numericMin?: number | bigint;
  numericMax?: number | bigint;
  stringMin?: string;
  stringMax?: string;
  hasBoolean?: boolean;
  hasObject?: boolean;
  hasArray?: boolean;
  // Count of each type present in mixed-type fields
  typeCounts?: {
    number?: number;
    string?: number;
    boolean?: number;
    object?: number;
    array?: number;
    date?: number;
    bigint?: number;
  };
}

/** Zone map for a column */
export interface ZoneMap {
  fieldPath: string;
  fieldType: ZoneMapFieldType;
  entries: ZoneMapEntry[];
}

/** Column zone map (alias for compatibility) */
export type ColumnZoneMap = ZoneMap;

/** Zone map metadata for all columns */
export interface ZoneMapMetadata {
  columns: { [fieldPath: string]: ZoneMap };
}

/** Options for processing row groups */
export interface ProcessRowGroupOptions {
  parseAsDate?: boolean;
  flattenArrays?: boolean;
}

/** Options for merging zone maps */
export interface MergeOptions {
  preserveRowGroups?: boolean;
}

/** Options for serialization */
export interface SerializeOptions {
  format?: 'binary' | 'json';
}

// ============================================================================
// Zone Map Generator Implementation
// ============================================================================

import { LRUCache } from '../utils/lru-cache.js';

/**
 * Generator for creating and managing zone maps
 */
export class ZoneMapGenerator {
  /** Zone map cache with LRU eviction (max 1000 fields per generator) */
  private zoneMaps: LRUCache<string, ZoneMap> = new LRUCache({ maxSize: 1000 });

  /**
   * Process a row group and update the zone map for a specific field.
   * Extracts field values, computes min/max bounds, and tracks statistics.
   */
  processRowGroup<T extends Record<string, unknown>>(
    rowGroupId: string,
    rows: T[],
    fieldPath: string,
    options?: ProcessRowGroupOptions
  ): void {
    const values: unknown[] = [];
    const typeCounts: NonNullable<ZoneMapEntry['typeCounts']> = {};

    // Step 1: Extract field values from all rows
    for (const row of rows) {
      const value = this.getFieldValue(row, fieldPath, options?.flattenArrays);
      values.push(value);
    }

    // Step 2: Create zone map entry with bounds and statistics
    const entry = this.createEntry(rowGroupId, values, options, typeCounts);

    // Step 3: Get or create zone map for this field
    let zoneMap = this.zoneMaps.get(fieldPath);
    if (!zoneMap) {
      zoneMap = {
        fieldPath,
        fieldType: this.inferFieldType(values, entry.hasMixedTypes),
        entries: [],
      };
      this.zoneMaps.set(fieldPath, zoneMap);
    }

    // Step 4: Update or insert entry for this row group
    const existingIndex = zoneMap.entries.findIndex((e) => e.rowGroupId === rowGroupId);
    if (existingIndex >= 0) {
      // Replace existing entry (e.g., if processing same row group again)
      zoneMap.entries[existingIndex] = entry;
    } else {
      // Add new entry
      zoneMap.entries.push(entry);
    }
  }

  /**
   * Process multiple fields in a batch
   */
  processRowGroupBatch<T extends Record<string, unknown>>(
    rowGroupId: string,
    rows: T[],
    fieldPaths: string[],
    options?: ProcessRowGroupOptions
  ): void {
    for (const fieldPath of fieldPaths) {
      this.processRowGroup(rowGroupId, rows, fieldPath, options);
    }
  }

  /**
   * Get the zone map for a specific field
   */
  getZoneMap(fieldPath: string): ZoneMap | undefined {
    return this.zoneMaps.get(fieldPath);
  }

  /**
   * Get metadata for all zone maps
   */
  getMetadata(): ZoneMapMetadata {
    const columns: { [fieldPath: string]: ZoneMap } = {};
    for (const [fieldPath, zoneMap] of this.zoneMaps.entries()) {
      columns[fieldPath] = zoneMap;
    }
    return { columns };
  }

  /**
   * Extract field value from a row, supporting nested paths and escaped field names.
   * Handles dot-notation field paths (e.g., "user.address.city") and escaped dots (e.g., "field\.name").
   * When flattenArrays is enabled, recursively flattens arrays in the path.
   */
  private getFieldValue(row: Record<string, unknown>, fieldPath: string, flattenArrays = false): unknown {
    // Handle escaped dots in field names (e.g., "field\.name" = literal field named "field.name")
    if (fieldPath.includes('\\.')) {
      const unescapedFieldName = fieldPath.replace(/\\\./g, '.');
      return row[unescapedFieldName];
    }

    const pathParts = fieldPath.split('.');
    let current: unknown = row;

    for (let i = 0; i < pathParts.length; i++) {
      const part = pathParts[i]!;

      // Stop traversal if we hit null/undefined
      if (current === null || current === undefined) {
        return undefined;
      }

      // Handle array flattening: when we encounter an array and flattenArrays is true,
      // recursively extract the remaining path from each array element
      if (Array.isArray(current) && flattenArrays) {
        const remainingPath = pathParts.slice(i).join('.');
        const flattenedResults: unknown[] = [];
        for (const item of current) {
          const itemValue = this.getFieldValue(item as Record<string, unknown>, remainingPath, flattenArrays);
          // Recursively flatten nested arrays
          if (Array.isArray(itemValue)) {
            flattenedResults.push(...itemValue);
          } else {
            flattenedResults.push(itemValue);
          }
        }
        return flattenedResults;
      }

      // Can't traverse further if current is not an object
      if (typeof current !== 'object') {
        return undefined;
      }

      current = (current as Record<string, unknown>)[part];
    }

    return current;
  }

  /**
   * Create a zone map entry from a row group's values.
   * Handles type detection, null counting, and computing min/max bounds.
   */
  private createEntry(
    rowGroupId: string,
    values: unknown[],
    options?: ProcessRowGroupOptions,
    typeCounts: NonNullable<ZoneMapEntry['typeCounts']> = {}
  ): ZoneMapEntry {
    let nullCount = 0;
    const nonNullValues: unknown[] = [];
    let hasMixedTypes = false;
    let hasBoolean = false;
    let hasObject = false;
    let hasArray = false;
    let firstType: string | null = null;

    // Step 1: Flatten arrays if enabled (values may already be partially flattened from getFieldValue)
    const processedValues: unknown[] = [];
    for (const val of values) {
      if (options?.flattenArrays && Array.isArray(val)) {
        // Recursively flatten nested arrays into a flat list
        for (const item of val) {
          processedValues.push(item);
        }
      } else {
        processedValues.push(val);
      }
    }

    // Step 2: Separate nulls and analyze types of non-null values
    for (let val of processedValues) {
      if (val === null || val === undefined) {
        nullCount++;
        continue;
      }

      // Parse string to Date if requested
      if (options?.parseAsDate && typeof val === 'string') {
        val = new Date(val);
      }

      // Skip NaN values (they don't contribute to min/max bounds)
      if (typeof val === 'number' && Number.isNaN(val)) {
        continue;
      }

      const valType = this.getValueType(val);

      // Track type counts for mixed-type fields
      typeCounts[valType as keyof typeof typeCounts] = (typeCounts[valType as keyof typeof typeCounts] || 0) + 1;

      // Mark which complex types are present
      if (valType === 'boolean') hasBoolean = true;
      if (valType === 'object') hasObject = true;
      if (valType === 'array') hasArray = true;

      // Detect type diversity
      if (firstType === null) {
        firstType = valType;
      } else if (firstType !== valType) {
        hasMixedTypes = true;
      }

      nonNullValues.push(val);
    }

    // Step 3: Build base entry with counts
    const entry: ZoneMapEntry = {
      rowGroupId,
      min: null,
      max: null,
      nullCount,
      hasNull: nullCount > 0,
      allNull: nonNullValues.length === 0,
      rowCount: values.length,
    };

    // Early return for all-null entries
    if (nonNullValues.length === 0) {
      return entry;
    }

    // Step 4: Compute min/max bounds
    if (hasMixedTypes) {
      // For mixed types, track type-specific bounds separately
      entry.hasMixedTypes = true;
      entry.hasBoolean = hasBoolean;
      entry.hasObject = hasObject;
      entry.hasArray = hasArray;
      entry.typeCounts = typeCounts;

      const numbers = nonNullValues.filter((v) => typeof v === 'number' || typeof v === 'bigint');
      const strings = nonNullValues.filter((v) => typeof v === 'string');

      if (numbers.length > 0) {
        const { min, max } = this.findNumericMinMax(numbers as (number | bigint)[]);
        entry.numericMin = min;
        entry.numericMax = max;
      }

      if (strings.length > 0) {
        const { min, max } = this.findStringMinMax(strings as string[]);
        entry.stringMin = min;
        entry.stringMax = max;
      }
    } else {
      // For homogeneous types, use unified min/max bounds
      const { min, max } = this.findMinMax(nonNullValues);
      entry.min = min as ZoneMapValue;
      entry.max = max as ZoneMapValue;
    }

    return entry;
  }

  /**
   * Determine the JavaScript type of a value.
   * Used for type detection and tracking in zone map entries.
   */
  private getValueType(val: unknown): string {
    if (val === null || val === undefined) return 'null';
    if (val instanceof Date) return 'date';
    if (typeof val === 'bigint') return 'bigint';
    if (Array.isArray(val)) return 'array';
    if (typeof val === 'object') return 'object';
    // typeof returns: 'string', 'number', 'boolean', 'function', 'symbol', 'undefined'
    return typeof val;
  }

  /**
   * Find min/max bounds for a homogeneous set of values.
   * Dispatches to type-specific min/max functions based on value type.
   */
  private findMinMax(values: unknown[]): { min: unknown; max: unknown } {
    if (values.length === 0) {
      return { min: null, max: null };
    }

    const first = values[0];

    // Dispatch to type-specific handler
    if (typeof first === 'number' || typeof first === 'bigint') {
      return this.findNumericMinMax(values as (number | bigint)[]);
    }

    if (typeof first === 'string') {
      return this.findStringMinMax(values as string[]);
    }

    if (first instanceof Date) {
      return this.findDateMinMax(values as Date[]);
    }

    // Fallback for other types: use first element as both min and max
    // (conservative approach when min/max comparison is not applicable)
    return { min: first, max: first };
  }

  /**
   * Find min/max for numeric values, handling both number and bigint types.
   * For mixed number/bigint cases, converts to number for comparison (may lose precision on very large bigints).
   */
  private findNumericMinMax(values: (number | bigint)[]): { min: number | bigint; max: number | bigint } {
    const numbers = values.filter((v): v is number => typeof v === 'number');
    const bigints = values.filter((v): v is bigint => typeof v === 'bigint');

    // Case 1: Only bigints - use bigint comparison (no precision loss)
    if (bigints.length > 0 && numbers.length === 0) {
      const firstBigint = bigints[0];
      if (firstBigint === undefined) {
        throw new Error('Expected at least one bigint value');
      }
      let min = firstBigint;
      let max = firstBigint;
      for (const v of bigints) {
        if (v < min) min = v;
        if (v > max) max = v;
      }
      return { min, max };
    }

    // Case 2: Only numbers - use numeric comparison
    if (numbers.length > 0 && bigints.length === 0) {
      const firstNumber = numbers[0];
      if (firstNumber === undefined) {
        throw new Error('Expected at least one number value');
      }
      let min = firstNumber;
      let max = firstNumber;
      for (const v of numbers) {
        if (v < min) min = v;
        if (v > max) max = v;
      }
      return { min, max };
    }

    // Case 3: Mixed number and bigint - convert to number for comparison
    // NOTE: Large bigints lose precision when converted to number
    const firstValue = values[0];
    if (firstValue === undefined) {
      throw new Error('Expected at least one numeric value');
    }
    let min = firstValue;
    let max = firstValue;
    for (const v of values) {
      const vNum = typeof v === 'bigint' ? Number(v) : v;
      const minNum = typeof min === 'bigint' ? Number(min) : min;
      const maxNum = typeof max === 'bigint' ? Number(max) : max;
      if (vNum < minNum) min = v;
      if (vNum > maxNum) max = v;
    }
    return { min, max };
  }

  /**
   * Find min/max for string values using lexicographic comparison.
   */
  private findStringMinMax(values: string[]): { min: string; max: string } {
    const firstValue = values[0];
    if (firstValue === undefined) {
      throw new Error('Expected at least one string value');
    }
    let min = firstValue;
    let max = firstValue;
    for (const v of values) {
      if (v < min) min = v;
      if (v > max) max = v;
    }
    return { min, max };
  }

  /**
   * Find min/max for date values using timestamp comparison.
   */
  private findDateMinMax(values: Date[]): { min: Date; max: Date } {
    const firstValue = values[0];
    if (firstValue === undefined) {
      throw new Error('Expected at least one date value');
    }
    let min = firstValue;
    let max = firstValue;
    for (const v of values) {
      if (v.getTime() < min.getTime()) min = v;
      if (v.getTime() > max.getTime()) max = v;
    }
    return { min, max };
  }

  /**
   * Infer the field type from values.
   * Returns the most common type found, or 'string' as default fallback.
   */
  private inferFieldType(values: unknown[], hasMixedTypes?: boolean): ZoneMapFieldType {
    // Mixed type field - explicitly marked
    if (hasMixedTypes) return 'mixed';

    // Scan values and return first detected type (skipping nulls)
    for (const val of values) {
      if (val === null || val === undefined) continue;
      if (val instanceof Date) return 'date';
      if (typeof val === 'bigint') return 'bigint';
      if (typeof val === 'number') return 'number';
      if (typeof val === 'string') return 'string';
      if (typeof val === 'boolean') return 'boolean';
    }

    // Fallback for all-null fields or empty value set
    return 'string';
  }
}

// ============================================================================
// Zone Map Merge Functions
// ============================================================================

/**
 * Merge two zone maps for the same field.
 * Combines statistics from multiple zone maps while optionally preserving row group boundaries.
 * Throws if zone maps are for different fields.
 */
export function mergeZoneMaps(map1: ZoneMap, map2: ZoneMap, options?: MergeOptions): ZoneMap {
  // Validate: both zone maps must be for the same field
  if (map1.fieldPath !== map2.fieldPath) {
    throw new Error(
      `Cannot merge zone maps: field paths must match. ` +
      `Got "${map1.fieldPath}" and "${map2.fieldPath}"`
    );
  }

  if (options?.preserveRowGroups) {
    // Keep all entries separate: concatenate entry lists
    return {
      fieldPath: map1.fieldPath,
      fieldType: map1.fieldType,
      entries: [...map1.entries, ...map2.entries],
    };
  }

  // Merge mode: combine entries into a single aggregated entry
  const allEntries = [...map1.entries, ...map2.entries];

  if (allEntries.length === 0) {
    return {
      fieldPath: map1.fieldPath,
      fieldType: map1.fieldType,
      entries: [],
    };
  }

  // Compute global bounds across all entries
  const mergedEntry = mergeEntries(allEntries, map1.fieldType);

  return {
    fieldPath: map1.fieldPath,
    fieldType: map1.fieldType,
    entries: [mergedEntry],
  };
}

/**
 * Merge multiple zone map entries into a single aggregated entry.
 * Combines statistics across all entries while computing global min/max bounds.
 */
function mergeEntries(entries: ZoneMapEntry[], fieldType: ZoneMapFieldType): ZoneMapEntry {
  let totalNullCount = 0;
  let totalRowCount = 0;
  let hasNull = false;
  let allNull = true;

  // Separate entries with values from all-null entries
  const nonNullEntries = entries.filter((e) => !e.allNull);

  // Aggregate statistics across all entries
  for (const entry of entries) {
    totalNullCount += entry.nullCount;
    totalRowCount += entry.rowCount;
    if (entry.hasNull) hasNull = true;
    if (!entry.allNull) allNull = false;
  }

  // Handle all-null case: no bounds available
  if (nonNullEntries.length === 0) {
    return {
      rowGroupId: 'merged',
      min: null,
      max: null,
      nullCount: totalNullCount,
      hasNull: true,
      allNull: true,
      rowCount: totalRowCount,
    };
  }

  // Compute overall bounds by finding global min/max across all entries
  const firstEntry = nonNullEntries[0];
  if (!firstEntry) {
    // This shouldn't happen since we checked nonNullEntries.length above
    return {
      rowGroupId: 'merged',
      min: null,
      max: null,
      nullCount: totalNullCount,
      hasNull: true,
      allNull: true,
      rowCount: totalRowCount,
    };
  }
  let min: ZoneMapValue = firstEntry.min;
  let max: ZoneMapValue = firstEntry.max;

  for (const entry of nonNullEntries) {
    // Update min if we find a smaller value
    if (compareValues(entry.min, min, fieldType) < 0) {
      min = entry.min;
    }
    // Update max if we find a larger value
    if (compareValues(entry.max, max, fieldType) > 0) {
      max = entry.max;
    }
  }

  return {
    rowGroupId: 'merged',
    min,
    max,
    nullCount: totalNullCount,
    hasNull,
    allNull,
    rowCount: totalRowCount,
  };
}

/**
 * Compare two values in a type-aware manner.
 * Returns: < 0 if a < b, 0 if a == b, > 0 if a > b
 * Handles null values (treated as less than any non-null value).
 * Supports type coercion between compatible types:
 * - number <-> bigint (both converted to number for comparison)
 * - Date <-> ISO string (string parsed as Date)
 * - Date <-> timestamp number (number treated as milliseconds since epoch)
 */
function compareValues(a: ZoneMapValue, b: ZoneMapValue, fieldType: ZoneMapFieldType): number {
  // Null handling: null is treated as less than any non-null value
  if (a === null && b === null) return 0;
  if (a === null) return -1;
  if (b === null) return 1;

  // Type coercion for number/bigint comparisons
  const aIsNumeric = typeof a === 'number' || typeof a === 'bigint';
  const bIsNumeric = typeof b === 'number' || typeof b === 'bigint';
  if (aIsNumeric && bIsNumeric) {
    // Convert both to number for comparison (may lose precision for very large bigints)
    const aNum = typeof a === 'bigint' ? Number(a) : (a as number);
    const bNum = typeof b === 'bigint' ? Number(b) : (b as number);
    return aNum - bNum;
  }

  // Type coercion for date comparisons
  // Handles: Date <-> Date, Date <-> ISO string, Date <-> timestamp number
  const aIsDate = a instanceof Date;
  const bIsDate = b instanceof Date;
  const aIsDateLike = aIsDate || (fieldType === 'date' && (typeof a === 'string' || typeof a === 'number'));
  const bIsDateLike = bIsDate || (fieldType === 'date' && (typeof b === 'string' || typeof b === 'number'));

  if (aIsDate || bIsDate || (aIsDateLike && bIsDateLike)) {
    const aTime = coerceToTimestamp(a);
    const bTime = coerceToTimestamp(b);
    if (aTime !== null && bTime !== null) {
      return aTime - bTime;
    }
  }

  // Type-specific comparisons using field type hint or runtime type detection
  if (fieldType === 'date' || (a instanceof Date && b instanceof Date)) {
    return (a as Date).getTime() - (b as Date).getTime();
  }

  if (fieldType === 'bigint' || (typeof a === 'bigint' && typeof b === 'bigint')) {
    const diff = (a as bigint) - (b as bigint);
    return diff < 0n ? -1 : diff > 0n ? 1 : 0;
  }

  if (fieldType === 'number' || (typeof a === 'number' && typeof b === 'number')) {
    return (a as number) - (b as number);
  }

  if (fieldType === 'string' || (typeof a === 'string' && typeof b === 'string')) {
    return (a as string) < (b as string) ? -1 : (a as string) > (b as string) ? 1 : 0;
  }

  // Fallback: lexicographic comparison on string representations
  return String(a) < String(b) ? -1 : String(a) > String(b) ? 1 : 0;
}

/**
 * Coerce a value to a timestamp (milliseconds since epoch).
 * Supports: Date objects, ISO date strings, and numeric timestamps.
 * Returns null if the value cannot be coerced to a valid timestamp.
 */
function coerceToTimestamp(value: ZoneMapValue): number | null {
  if (value instanceof Date) {
    return value.getTime();
  }
  if (typeof value === 'number') {
    // Assume numeric value is already a timestamp in milliseconds
    return value;
  }
  if (typeof value === 'string') {
    // Try to parse as ISO date string
    const parsed = Date.parse(value);
    if (!Number.isNaN(parsed)) {
      return parsed;
    }
  }
  return null;
}

// ============================================================================
// Predicate Evaluation
// ============================================================================

/**
 * Evaluate a predicate against a zone map entry to determine if the row group might contain matching data.
 * Returns:
 * - MATCH: Row group may contain matching data (cannot exclude)
 * - NO_MATCH: Row group definitely doesn't contain matching data (safe to skip)
 * - UNKNOWN: Cannot determine from zone map statistics alone
 */
export function evaluatePredicate(
  zoneMap: ZoneMap,
  rowGroupId: string,
  predicate: RangePredicate
): PredicateResult {
  const entry = zoneMap.entries.find((e) => e.rowGroupId === rowGroupId);

  // Row group not found in zone map
  if (!entry) {
    return 'UNKNOWN';
  }

  // All-null row groups cannot match any predicate except null-specific ones
  if (entry.allNull) {
    return 'UNKNOWN';
  }

  const { min, max } = entry;
  const { op, value } = predicate;

  // Special handling for null predicate values
  if (value === null) {
    if (op === '$eq') {
      // $eq: null matches if the row group contains null values
      return entry.hasNull ? 'MATCH' : 'NO_MATCH';
    }
    if (op === '$ne') {
      // $ne: null matches if the row group contains non-null values
      return entry.allNull ? 'NO_MATCH' : 'MATCH';
    }
    // For other operators with null value, we can't determine from zone map
    return 'UNKNOWN';
  }

  // Dispatch to operator-specific evaluator
  switch (op) {
    case '$eq':
      return evaluateEq(min, max, value as ZoneMapValue, zoneMap.fieldType, entry.hasNull);
    case '$ne':
      return evaluateNe(min, max, value as ZoneMapValue, zoneMap.fieldType);
    case '$gt':
      return evaluateGt(min, max, value as ZoneMapValue, zoneMap.fieldType);
    case '$gte':
      return evaluateGte(min, max, value as ZoneMapValue, zoneMap.fieldType);
    case '$lt':
      return evaluateLt(min, max, value as ZoneMapValue, zoneMap.fieldType);
    case '$lte':
      return evaluateLte(min, max, value as ZoneMapValue, zoneMap.fieldType);
    case '$in':
      return evaluateIn(min, max, value as ZoneMapValue[], zoneMap.fieldType);
    case '$nin':
      // Cannot definitively exclude row groups using $nin with zone maps
      // (must scan to know if any excluded values are present)
      return 'MATCH';
    default:
      return 'UNKNOWN';
  }
}

/** Check if value falls within zone map range [min, max] */
function evaluateEq(
  min: ZoneMapValue,
  max: ZoneMapValue,
  value: ZoneMapValue,
  fieldType: ZoneMapFieldType,
  _hasNull?: boolean
): PredicateResult {
  // Predicate $eq: value may exist if it falls within [min, max]
  const gtMin = compareValues(value, min, fieldType) >= 0;
  const ltMax = compareValues(value, max, fieldType) <= 0;
  return gtMin && ltMax ? 'MATCH' : 'NO_MATCH';
}

/** Check if row group might contain values != value */
function evaluateNe(
  min: ZoneMapValue,
  max: ZoneMapValue,
  value: ZoneMapValue,
  fieldType: ZoneMapFieldType
): PredicateResult {
  // Predicate $ne: can only exclude if field is constant and equals the value
  // i.e., only exclude when min == max == value
  if (compareValues(min, max, fieldType) === 0 && compareValues(min, value, fieldType) === 0) {
    return 'NO_MATCH';
  }
  return 'MATCH';
}

/** Check if row group might contain values > value */
function evaluateGt(
  _min: ZoneMapValue,
  max: ZoneMapValue,
  value: ZoneMapValue,
  fieldType: ZoneMapFieldType
): PredicateResult {
  // Predicate $gt: match if max > value
  return compareValues(max, value, fieldType) > 0 ? 'MATCH' : 'NO_MATCH';
}

/** Check if row group might contain values >= value */
function evaluateGte(
  _min: ZoneMapValue,
  max: ZoneMapValue,
  value: ZoneMapValue,
  fieldType: ZoneMapFieldType
): PredicateResult {
  // Predicate $gte: match if max >= value
  return compareValues(max, value, fieldType) >= 0 ? 'MATCH' : 'NO_MATCH';
}

/** Check if row group might contain values < value */
function evaluateLt(
  min: ZoneMapValue,
  _max: ZoneMapValue,
  value: ZoneMapValue,
  fieldType: ZoneMapFieldType
): PredicateResult {
  // Predicate $lt: match if min < value
  return compareValues(min, value, fieldType) < 0 ? 'MATCH' : 'NO_MATCH';
}

/** Check if row group might contain values <= value */
function evaluateLte(
  min: ZoneMapValue,
  _max: ZoneMapValue,
  value: ZoneMapValue,
  fieldType: ZoneMapFieldType
): PredicateResult {
  // Predicate $lte: match if min <= value
  return compareValues(min, value, fieldType) <= 0 ? 'MATCH' : 'NO_MATCH';
}

/** Check if row group might contain any value from the set */
function evaluateIn(
  min: ZoneMapValue,
  max: ZoneMapValue,
  values: ZoneMapValue[],
  fieldType: ZoneMapFieldType
): PredicateResult {
  // Predicate $in: match if at least one value falls within [min, max]
  for (const value of values) {
    if (compareValues(value, min, fieldType) >= 0 && compareValues(value, max, fieldType) <= 0) {
      return 'MATCH';
    }
  }
  return 'NO_MATCH';
}

// ============================================================================
// Row Group Filtering
// ============================================================================

/**
 * Filter row groups based on a predicate.
 * Returns an array of row group IDs that potentially match the predicate.
 * Row groups that definitely don't match are excluded.
 */
export function filterRowGroups(
  zoneMap: ZoneMap,
  predicate: RangePredicate
): string[] {
  const matchingGroups: string[] = [];

  for (const entry of zoneMap.entries) {
    const result = evaluatePredicate(zoneMap, entry.rowGroupId, predicate);
    // Include row groups that MATCH or are UNKNOWN (conservative approach)
    if (result !== 'NO_MATCH') {
      matchingGroups.push(entry.rowGroupId);
    }
  }

  return matchingGroups;
}

// ============================================================================
// Compound Predicate Evaluation
// ============================================================================

/**
 * Compound predicate type supporting $and and $or operators.
 * Allows nesting of range predicates and other compound predicates.
 */
export interface CompoundPredicate {
  $and?: Array<RangePredicate | CompoundPredicate>;
  $or?: Array<RangePredicate | CompoundPredicate>;
}

/**
 * Type guard to check if a predicate is a RangePredicate
 */
function isRangePredicate(predicate: RangePredicate | CompoundPredicate): predicate is RangePredicate {
  return 'op' in predicate && 'value' in predicate;
}

/**
 * Evaluate a compound predicate ($and/$or) against a zone map entry.
 * Supports nested compound predicates for complex filter expressions.
 *
 * Returns:
 * - MATCH: Row group may contain matching data
 * - NO_MATCH: Row group definitely doesn't contain matching data
 * - UNKNOWN: Cannot determine from zone map statistics alone
 */
export function evaluateCompoundPredicate(
  zoneMap: ZoneMap,
  rowGroupId: string,
  predicate: CompoundPredicate
): PredicateResult {
  // Handle $and predicates
  if (predicate.$and && predicate.$and.length > 0) {
    let hasUnknown = false;

    for (const subPredicate of predicate.$and) {
      const result = isRangePredicate(subPredicate)
        ? evaluatePredicate(zoneMap, rowGroupId, subPredicate)
        : evaluateCompoundPredicate(zoneMap, rowGroupId, subPredicate);

      // For AND, if any predicate is NO_MATCH, the whole thing is NO_MATCH
      if (result === 'NO_MATCH') {
        return 'NO_MATCH';
      }
      if (result === 'UNKNOWN') {
        hasUnknown = true;
      }
    }

    // If we got here, no predicates were NO_MATCH
    // Return UNKNOWN if any were unknown, otherwise MATCH
    return hasUnknown ? 'UNKNOWN' : 'MATCH';
  }

  // Handle $or predicates
  if (predicate.$or && predicate.$or.length > 0) {
    let hasUnknown = false;

    for (const subPredicate of predicate.$or) {
      const result = isRangePredicate(subPredicate)
        ? evaluatePredicate(zoneMap, rowGroupId, subPredicate)
        : evaluateCompoundPredicate(zoneMap, rowGroupId, subPredicate);

      // For OR, if any predicate is MATCH, the whole thing is MATCH
      if (result === 'MATCH') {
        return 'MATCH';
      }
      if (result === 'UNKNOWN') {
        hasUnknown = true;
      }
    }

    // If we got here, no predicates were MATCH
    // Return UNKNOWN if any were unknown, otherwise NO_MATCH
    return hasUnknown ? 'UNKNOWN' : 'NO_MATCH';
  }

  // Empty compound predicate - cannot determine
  return 'UNKNOWN';
}

// ============================================================================
// Serialization/Deserialization
// ============================================================================

const ZONE_MAP_MAGIC = 0x5a4d4150; // "ZMAP"
const ZONE_MAP_VERSION = 1;

/**
 * Serialize a zone map to binary or JSON format.
 * Defaults to binary format for space efficiency.
 */
export function serializeZoneMap(zoneMap: ZoneMap, options?: SerializeOptions): Uint8Array {
  const format = options?.format ?? 'binary';
  return format === 'json' ? serializeZoneMapJson(zoneMap) : serializeZoneMapBinary(zoneMap);
}

/**
 * Serialize zone map to JSON format with custom handling for Date and BigInt.
 * Uses special marker objects to preserve type information during serialization.
 */
function serializeZoneMapJson(zoneMap: ZoneMap): Uint8Array {
  const json = JSON.stringify(zoneMap, (_key, value) => {
    // Convert BigInt to marker object (JSON doesn't support BigInt)
    if (typeof value === 'bigint') {
      return { __bigint__: value.toString() };
    }
    // Convert Date to marker object with ISO string
    if (value instanceof Date) {
      return { __date__: value.toISOString() };
    }
    return value;
  });
  return new TextEncoder().encode(json);
}

/**
 * Serialize zone map to binary format for compact storage.
 * Binary format: [magic(4)][version(4)][fieldPath(4+len)][fieldType(1)][entryCount(4)][entries...]
 */
function serializeZoneMapBinary(zoneMap: ZoneMap): Uint8Array {
  const parts: Uint8Array[] = [];
  const encoder = new TextEncoder();

  // Header: magic number and version for validation and compatibility
  const header = new Uint8Array(8);
  const headerView = new DataView(header.buffer);
  headerView.setUint32(0, ZONE_MAP_MAGIC, true);   // "ZMAP" magic bytes
  headerView.setUint32(4, ZONE_MAP_VERSION, true); // Format version
  parts.push(header);

  // Field path: length-prefixed UTF-8 string
  const fieldPathBytes = encoder.encode(zoneMap.fieldPath);
  const fieldPathHeader = new Uint8Array(4);
  new DataView(fieldPathHeader.buffer).setUint32(0, fieldPathBytes.length, true);
  parts.push(fieldPathHeader);
  parts.push(fieldPathBytes);

  // Field type: single byte enum
  const fieldTypeMap: Record<ZoneMapFieldType, number> = {
    string: 1,
    number: 2,
    bigint: 3,
    date: 4,
    boolean: 5,
    mixed: 6,
  };
  parts.push(new Uint8Array([fieldTypeMap[zoneMap.fieldType] || 1]));

  // Entry count: number of row group entries
  const entryCountHeader = new Uint8Array(4);
  new DataView(entryCountHeader.buffer).setUint32(0, zoneMap.entries.length, true);
  parts.push(entryCountHeader);

  // Serialize each row group entry
  for (const entry of zoneMap.entries) {
    parts.push(serializeEntry(entry, zoneMap.fieldType));
  }

  // Combine all parts into final byte array
  const totalLength = parts.reduce((sum, p) => sum + p.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.length;
  }

  return result;
}

/**
 * Serialize a single zone map entry.
 * Format: [rowGroupId(4+len)][min][max][stats(14)][typeSpecificValues...]
 */
function serializeEntry(entry: ZoneMapEntry, fieldType: ZoneMapFieldType): Uint8Array {
  const parts: Uint8Array[] = [];
  const encoder = new TextEncoder();

  // Row group ID: length-prefixed UTF-8 string
  const rowGroupIdBytes = encoder.encode(entry.rowGroupId);
  const rowGroupIdHeader = new Uint8Array(4);
  new DataView(rowGroupIdHeader.buffer).setUint32(0, rowGroupIdBytes.length, true);
  parts.push(rowGroupIdHeader);
  parts.push(rowGroupIdBytes);

  // Min/max bounds
  parts.push(serializeValue(entry.min, fieldType));
  parts.push(serializeValue(entry.max, fieldType));

  // Statistics: packed into 14 bytes for compactness
  // Layout: nullCount(4B) + hasNull(1B) + allNull(1B) + rowCount(4B) + type-flags(4B)
  const stats = new Uint8Array(14);
  const statsView = new DataView(stats.buffer);
  statsView.setUint32(0, entry.nullCount, true);
  statsView.setUint8(4, entry.hasNull ? 1 : 0);
  statsView.setUint8(5, entry.allNull ? 1 : 0);
  statsView.setUint32(6, entry.rowCount, true);
  statsView.setUint8(10, entry.hasMixedTypes ? 1 : 0);
  statsView.setUint8(11, entry.hasBoolean ? 1 : 0);
  statsView.setUint8(12, entry.hasObject ? 1 : 0);
  statsView.setUint8(13, entry.hasArray ? 1 : 0);
  parts.push(stats);

  // Type-specific min/max for mixed-type entries
  if (entry.hasMixedTypes) {
    parts.push(serializeValue(entry.numericMin ?? null, 'number'));
    parts.push(serializeValue(entry.numericMax ?? null, 'number'));
    parts.push(serializeValue(entry.stringMin ?? null, 'string'));
    parts.push(serializeValue(entry.stringMax ?? null, 'string'));
  }

  // Combine all parts into final array
  const totalLength = parts.reduce((sum, p) => sum + p.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.length;
  }

  return result;
}

/**
 * Serialize a single value with type marker.
 * Format: [typeMarker(1)][data...]
 * Type markers: 0=null, 1=string, 2=number, 3=bigint, 4=date, 5=boolean
 */
function serializeValue(value: ZoneMapValue | undefined, _fieldType: ZoneMapFieldType): Uint8Array {
  const encoder = new TextEncoder();

  // Null/undefined values
  if (value === null || value === undefined) {
    return new Uint8Array([0]);
  }

  // Date: timestamp as big int64
  if (value instanceof Date) {
    const result = new Uint8Array(9);
    result[0] = 4;
    new DataView(result.buffer).setBigInt64(1, BigInt(value.getTime()), true);
    return result;
  }

  // BigInt: as big int64
  if (typeof value === 'bigint') {
    const result = new Uint8Array(9);
    result[0] = 3;
    new DataView(result.buffer).setBigInt64(1, value, true);
    return result;
  }

  // Number: as float64
  if (typeof value === 'number') {
    const result = new Uint8Array(9);
    result[0] = 2;
    new DataView(result.buffer).setFloat64(1, value, true);
    return result;
  }

  // String: length-prefixed UTF-8 bytes
  if (typeof value === 'string') {
    const bytes = encoder.encode(value);
    const result = new Uint8Array(5 + bytes.length);
    result[0] = 1;
    new DataView(result.buffer).setUint32(1, bytes.length, true);
    result.set(bytes, 5);
    return result;
  }

  // Boolean: single byte (0 or 1)
  if (typeof value === 'boolean') {
    return new Uint8Array([5, value ? 1 : 0]);
  }

  // Fallback: null
  return new Uint8Array([0]);
}

/**
 * Deserialize a zone map from binary or JSON format.
 * Automatically detects format based on first byte.
 */
export function deserializeZoneMap(data: Uint8Array): ZoneMap {
  // Detect format: JSON starts with '{' (0x7b), binary starts with magic number
  if (data[0] === 0x7b) {
    return deserializeZoneMapJson(data);
  }
  return deserializeZoneMapBinary(data);
}

/**
 * Deserialize zone map from JSON format with custom reviver for Date and BigInt.
 */
function deserializeZoneMapJson(data: Uint8Array): ZoneMap {
  const json = new TextDecoder().decode(data);
  return JSON.parse(json, (_key, value) => {
    if (value && typeof value === 'object') {
      // Reconstruct BigInt from serialized marker
      if ('__bigint__' in value) {
        return BigInt(value.__bigint__);
      }
      // Reconstruct Date from ISO string marker
      if ('__date__' in value) {
        return new Date(value.__date__);
      }
    }
    return value;
  });
}

/**
 * Deserialize zone map from binary format.
 * Validates magic number and version for integrity.
 */
function deserializeZoneMapBinary(data: Uint8Array): ZoneMap {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const decoder = new TextDecoder();
  let offset = 0;

  // Validate magic number (should be "ZMAP")
  const magic = view.getUint32(offset, true);
  offset += 4;
  if (magic !== ZONE_MAP_MAGIC) {
    throw new Error(
      `Invalid zone map data: incorrect magic number. ` +
      `Expected 0x${ZONE_MAP_MAGIC.toString(16)}, got 0x${magic.toString(16)}`
    );
  }

  // Validate version for compatibility
  const version = view.getUint32(offset, true);
  offset += 4;
  if (version !== ZONE_MAP_VERSION) {
    throw new Error(
      `Cannot deserialize zone map: unsupported version ${version}. ` +
      `This code supports version ${ZONE_MAP_VERSION}`
    );
  }

  // Read field path (length-prefixed string)
  const fieldPathLength = view.getUint32(offset, true);
  offset += 4;
  const fieldPath = decoder.decode(data.subarray(offset, offset + fieldPathLength));
  offset += fieldPathLength;

  // Read field type (single byte enum)
  const fieldTypeMap: Record<number, ZoneMapFieldType> = {
    1: 'string',
    2: 'number',
    3: 'bigint',
    4: 'date',
    5: 'boolean',
    6: 'mixed',
  };
  const fieldType = fieldTypeMap[data[offset]!] || 'string';
  offset += 1;

  // Read entry count
  const entryCount = view.getUint32(offset, true);
  offset += 4;

  // Deserialize each row group entry
  const entries: ZoneMapEntry[] = [];
  for (let i = 0; i < entryCount; i++) {
    const { entry, bytesRead } = deserializeEntry(data, offset, fieldType);
    entries.push(entry);
    offset += bytesRead;
  }

  return {
    fieldPath,
    fieldType,
    entries,
  };
}

/**
 * Deserialize a single zone map entry starting at the given offset.
 * Returns the entry and number of bytes consumed.
 */
function deserializeEntry(
  data: Uint8Array,
  startOffset: number,
  fieldType: ZoneMapFieldType
): { entry: ZoneMapEntry; bytesRead: number } {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const decoder = new TextDecoder();
  let offset = startOffset;

  // Read row group ID (length-prefixed string)
  const rowGroupIdLength = view.getUint32(offset, true);
  offset += 4;
  const rowGroupId = decoder.decode(data.subarray(offset, offset + rowGroupIdLength));
  offset += rowGroupIdLength;

  // Read min/max bounds
  const { value: min, bytesRead: minBytesRead } = deserializeValue(data, offset, fieldType);
  offset += minBytesRead;

  const { value: max, bytesRead: maxBytesRead } = deserializeValue(data, offset, fieldType);
  offset += maxBytesRead;

  // Read packed statistics (14 bytes total)
  const nullCount = view.getUint32(offset, true);
  offset += 4;
  const hasNull = view.getUint8(offset) === 1;
  offset += 1;
  const allNull = view.getUint8(offset) === 1;
  offset += 1;
  const rowCount = view.getUint32(offset, true);
  offset += 4;
  // Type information flags
  const hasMixedTypes = view.getUint8(offset) === 1;
  offset += 1;
  const hasBoolean = view.getUint8(offset) === 1;
  offset += 1;
  const hasObject = view.getUint8(offset) === 1;
  offset += 1;
  const hasArray = view.getUint8(offset) === 1;
  offset += 1;

  const entry: ZoneMapEntry = {
    rowGroupId,
    min,
    max,
    nullCount,
    hasNull,
    allNull,
    rowCount,
  };

  // Read type-specific bounds for mixed-type entries
  if (hasMixedTypes) {
    entry.hasMixedTypes = true;
    entry.hasBoolean = hasBoolean;
    entry.hasObject = hasObject;
    entry.hasArray = hasArray;

    const { value: numericMin, bytesRead: numMinBytes } = deserializeValue(data, offset, 'number');
    offset += numMinBytes;
    const { value: numericMax, bytesRead: numMaxBytes } = deserializeValue(data, offset, 'number');
    offset += numMaxBytes;
    const { value: stringMin, bytesRead: strMinBytes } = deserializeValue(data, offset, 'string');
    offset += strMinBytes;
    const { value: stringMax, bytesRead: strMaxBytes } = deserializeValue(data, offset, 'string');
    offset += strMaxBytes;

    if (numericMin !== null) entry.numericMin = numericMin as number | bigint;
    if (numericMax !== null) entry.numericMax = numericMax as number | bigint;
    if (stringMin !== null) entry.stringMin = stringMin as string;
    if (stringMax !== null) entry.stringMax = stringMax as string;
  }

  return {
    entry,
    bytesRead: offset - startOffset,
  };
}

/**
 * Deserialize a single typed value starting at the given offset.
 * Returns the deserialized value and number of bytes consumed.
 * Type markers: 0=null, 1=string, 2=number, 3=bigint, 4=date, 5=boolean
 */
function deserializeValue(
  data: Uint8Array,
  offset: number,
  _fieldType: ZoneMapFieldType
): { value: ZoneMapValue; bytesRead: number } {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const decoder = new TextDecoder();

  const typeMarker = data[offset];
  offset += 1;

  switch (typeMarker) {
    case 0: // Null marker
      return { value: null, bytesRead: 1 };

    case 1: // String: length-prefixed UTF-8
      {
        const length = view.getUint32(offset, true);
        offset += 4;
        const value = decoder.decode(data.subarray(offset, offset + length));
        return { value, bytesRead: 5 + length };
      }

    case 2: // Number: float64
      {
        const value = view.getFloat64(offset, true);
        return { value, bytesRead: 9 };
      }

    case 3: // BigInt: int64
      {
        const value = view.getBigInt64(offset, true);
        return { value, bytesRead: 9 };
      }

    case 4: // Date: timestamp as int64
      {
        const timestamp = view.getBigInt64(offset, true);
        return { value: new Date(Number(timestamp)), bytesRead: 9 };
      }

    case 5: // Boolean: single byte (0=false, 1=true)
      {
        const value = data[offset] === 1;
        return { value, bytesRead: 2 };
      }

    default:
      // Unknown type marker - treat as null
      return { value: null, bytesRead: 1 };
  }
}
