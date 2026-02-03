/**
 * Zone Map Filtering Utilities
 *
 * Provides functions to extract predicates from MongoDB filters and
 * evaluate them against zone map statistics for predicate pushdown.
 *
 * Zone map filtering allows skipping entire Parquet files during queries
 * when we can determine from min/max statistics that no documents in the
 * file could possibly match the query filter.
 */

import type { FileZoneMapEntry } from '@mongolake/do/shard/types.js';

/**
 * Predicate extracted from a MongoDB filter for zone map evaluation.
 */
export interface ZoneMapPredicate {
  field: string;
  op: '$eq' | '$ne' | '$gt' | '$gte' | '$lt' | '$lte' | '$in' | '$nin';
  value: unknown;
}

/**
 * Extract zone map predicates from a MongoDB-style filter.
 *
 * Only extracts predicates that can be evaluated against zone maps:
 * - Simple equality: { field: value }
 * - Comparison operators: $eq, $ne, $gt, $gte, $lt, $lte
 * - Set operators: $in, $nin
 *
 * Does NOT extract:
 * - Regex patterns ($regex)
 * - Existence checks ($exists)
 * - Logical operators at the top level ($and, $or, $nor)
 * - Nested logical operators
 *
 * @param filter - MongoDB-style filter object
 * @returns Array of extracted predicates
 */
export function extractZoneMapPredicates(
  filter: Record<string, unknown>
): ZoneMapPredicate[] {
  const predicates: ZoneMapPredicate[] = [];

  for (const [key, condition] of Object.entries(filter)) {
    // Skip logical operators - they require more complex evaluation
    if (key === '$and' || key === '$or' || key === '$nor') {
      continue;
    }

    // Handle operator object conditions
    if (typeof condition === 'object' && condition !== null && !Array.isArray(condition)) {
      const ops = condition as Record<string, unknown>;

      // Extract supported operators
      if ('$eq' in ops) {
        predicates.push({ field: key, op: '$eq', value: ops.$eq });
      }
      if ('$ne' in ops) {
        predicates.push({ field: key, op: '$ne', value: ops.$ne });
      }
      if ('$gt' in ops) {
        predicates.push({ field: key, op: '$gt', value: ops.$gt });
      }
      if ('$gte' in ops) {
        predicates.push({ field: key, op: '$gte', value: ops.$gte });
      }
      if ('$lt' in ops) {
        predicates.push({ field: key, op: '$lt', value: ops.$lt });
      }
      if ('$lte' in ops) {
        predicates.push({ field: key, op: '$lte', value: ops.$lte });
      }
      if ('$in' in ops && Array.isArray(ops.$in)) {
        predicates.push({ field: key, op: '$in', value: ops.$in });
      }
      if ('$nin' in ops && Array.isArray(ops.$nin)) {
        predicates.push({ field: key, op: '$nin', value: ops.$nin });
      }
    } else {
      // Direct equality: { field: value }
      predicates.push({ field: key, op: '$eq', value: condition });
    }
  }

  return predicates;
}

/**
 * Compare two values for ordering.
 * Returns negative if a < b, positive if a > b, 0 if equal.
 *
 * @param a - First value
 * @param b - Second value
 * @returns Comparison result
 */
function compareValues(a: unknown, b: unknown): number {
  if (a === b) return 0;
  if (a === null || a === undefined) return -1;
  if (b === null || b === undefined) return 1;

  // Numeric comparison
  if (typeof a === 'number' && typeof b === 'number') {
    return a - b;
  }

  // String comparison
  if (typeof a === 'string' && typeof b === 'string') {
    return a < b ? -1 : a > b ? 1 : 0;
  }

  // Date comparison (stored as ISO strings or timestamps)
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() - b.getTime();
  }

  // Boolean comparison (false < true)
  if (typeof a === 'boolean' && typeof b === 'boolean') {
    return a === b ? 0 : a ? 1 : -1;
  }

  // Mixed types - convert to string for comparison
  return String(a) < String(b) ? -1 : String(a) > String(b) ? 1 : 0;
}

/**
 * Evaluate a single predicate against a zone map entry.
 *
 * Returns:
 * - true: The file MAY contain matching documents (cannot exclude)
 * - false: The file CANNOT contain matching documents (safe to skip)
 *
 * @param predicate - The predicate to evaluate
 * @param entry - Zone map entry for the field
 * @returns Whether the file may contain matches
 */
function evaluatePredicate(
  predicate: ZoneMapPredicate,
  entry: FileZoneMapEntry
): boolean {
  const { op, value } = predicate;
  const { min, max, nullCount, rowCount } = entry;

  // If all values are null and predicate is looking for null, may match
  if (nullCount === rowCount && value === null && op === '$eq') {
    return true;
  }

  // If all values are null and predicate is for non-null, cannot match
  if (nullCount === rowCount && value !== null && op === '$eq') {
    return false;
  }

  // Handle null predicate value
  if (value === null) {
    if (op === '$eq') {
      // Match if file has any null values
      return nullCount > 0;
    }
    if (op === '$ne') {
      // Match if file has any non-null values
      return nullCount < rowCount;
    }
    // Other operators with null value - cannot determine
    return true;
  }

  // Skip evaluation if min/max are null (all values in file are null)
  if (min === null || max === null) {
    return true;
  }

  switch (op) {
    case '$eq':
      // Value must be within [min, max] range
      return compareValues(value, min) >= 0 && compareValues(value, max) <= 0;

    case '$ne':
      // Can only exclude if all values in file equal the predicate value
      // This happens when min === max === value
      if (compareValues(min, max) === 0 && compareValues(min, value) === 0) {
        return false;
      }
      return true;

    case '$gt':
      // File may contain values > predicate if max > value
      return compareValues(max, value) > 0;

    case '$gte':
      // File may contain values >= predicate if max >= value
      return compareValues(max, value) >= 0;

    case '$lt':
      // File may contain values < predicate if min < value
      return compareValues(min, value) < 0;

    case '$lte':
      // File may contain values <= predicate if min <= value
      return compareValues(min, value) <= 0;

    case '$in':
      // File may match if any value in the set falls within [min, max]
      if (!Array.isArray(value)) return true;
      for (const v of value) {
        if (v === null && nullCount > 0) return true;
        if (v !== null && compareValues(v, min) >= 0 && compareValues(v, max) <= 0) {
          return true;
        }
      }
      return false;

    case '$nin':
      // Cannot reliably exclude files with $nin using zone maps
      // The file may contain values not in the exclusion set
      return true;

    default:
      // Unknown operator - cannot exclude
      return true;
  }
}

/**
 * Check if a file may contain documents matching the filter based on zone map statistics.
 *
 * This function performs predicate pushdown - it evaluates the filter against
 * the file's zone map statistics to determine if the file can be skipped.
 *
 * @param zoneMap - Zone map entries for the file (may be undefined for older files)
 * @param filter - MongoDB-style filter object
 * @returns true if the file MAY contain matching documents, false if it can be skipped
 */
export function canFileMatchFilter(
  zoneMap: FileZoneMapEntry[] | undefined,
  filter: Record<string, unknown>
): boolean {
  // No zone map data - cannot exclude the file
  if (!zoneMap || zoneMap.length === 0) {
    return true;
  }

  // Empty filter matches everything
  if (Object.keys(filter).length === 0) {
    return true;
  }

  // Extract predicates from filter
  const predicates = extractZoneMapPredicates(filter);

  // No extractable predicates - cannot exclude
  if (predicates.length === 0) {
    return true;
  }

  // Build a map of zone entries by field for quick lookup
  const zoneMapByField = new Map<string, FileZoneMapEntry>();
  for (const entry of zoneMap) {
    zoneMapByField.set(entry.field, entry);
  }

  // Evaluate each predicate - if ANY predicate definitely doesn't match,
  // the entire file can be skipped (predicates are AND-ed in MongoDB)
  for (const predicate of predicates) {
    const entry = zoneMapByField.get(predicate.field);

    // No zone map for this field - cannot exclude based on this predicate
    if (!entry) {
      continue;
    }

    // Evaluate the predicate against zone map
    if (!evaluatePredicate(predicate, entry)) {
      // This predicate definitely doesn't match - skip the file
      return false;
    }
  }

  // All predicates may match - file cannot be excluded
  return true;
}

/**
 * Generate zone map entries from a set of documents.
 *
 * Extracts min/max statistics for commonly filtered fields:
 * - _id (document identifier)
 * - _seq (sequence number for ordering)
 * - Any top-level numeric, string, or boolean fields
 *
 * @param docs - Array of documents to analyze
 * @param maxFields - Maximum number of fields to track (default: 20)
 * @returns Array of zone map entries
 */
export function generateZoneMapEntries(
  docs: Record<string, unknown>[],
  maxFields: number = 20
): FileZoneMapEntry[] {
  if (docs.length === 0) {
    return [];
  }

  // Collect statistics for each field
  const fieldStats = new Map<string, {
    min: string | number | boolean | null;
    max: string | number | boolean | null;
    nullCount: number;
    type: 'string' | 'number' | 'boolean' | 'mixed' | null;
  }>();

  // Priority fields that should always be tracked
  const priorityFields = new Set(['_id', '_seq']);

  for (const doc of docs) {
    for (const [field, value] of Object.entries(doc)) {
      // Skip complex types (objects, arrays) - they can't be easily compared
      if (typeof value === 'object' && value !== null && !(value instanceof Date)) {
        continue;
      }

      let stats = fieldStats.get(field);
      if (!stats) {
        stats = { min: null, max: null, nullCount: 0, type: null };
        fieldStats.set(field, stats);
      }

      // Handle null/undefined values
      if (value === null || value === undefined) {
        stats.nullCount++;
        continue;
      }

      // Normalize value for comparison
      let normalizedValue: string | number | boolean;
      let valueType: 'string' | 'number' | 'boolean';

      if (value instanceof Date) {
        normalizedValue = value.getTime();
        valueType = 'number';
      } else if (typeof value === 'string') {
        normalizedValue = value;
        valueType = 'string';
      } else if (typeof value === 'number') {
        normalizedValue = value;
        valueType = 'number';
      } else if (typeof value === 'boolean') {
        normalizedValue = value;
        valueType = 'boolean';
      } else {
        // Skip unsupported types
        continue;
      }

      // Track type consistency
      if (stats.type === null) {
        stats.type = valueType;
      } else if (stats.type !== valueType) {
        stats.type = 'mixed';
      }

      // Update min/max
      if (stats.min === null || compareValues(normalizedValue, stats.min) < 0) {
        stats.min = normalizedValue;
      }
      if (stats.max === null || compareValues(normalizedValue, stats.max) > 0) {
        stats.max = normalizedValue;
      }
    }
  }

  // Convert to array and limit fields
  const entries: FileZoneMapEntry[] = [];

  // First add priority fields
  for (const field of priorityFields) {
    const stats = fieldStats.get(field);
    if (stats && stats.type !== 'mixed') {
      entries.push({
        field,
        min: stats.min,
        max: stats.max,
        nullCount: stats.nullCount,
        rowCount: docs.length,
      });
      fieldStats.delete(field);
    }
  }

  // Then add other fields up to the limit
  for (const [field, stats] of fieldStats) {
    if (entries.length >= maxFields) break;

    // Skip mixed-type fields (can't reliably compare)
    if (stats.type === 'mixed') continue;

    entries.push({
      field,
      min: stats.min,
      max: stats.max,
      nullCount: stats.nullCount,
      rowCount: docs.length,
    });
  }

  return entries;
}
