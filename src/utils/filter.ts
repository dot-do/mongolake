/**
 * Shared filter matching logic for MongoDB-style query filters
 * @module utils/filter
 */

import type {
  Document,
  Filter,
  AnyDocument,
  DocumentFields,
  FilterQuery,
  OperatorCondition,
} from '@types';
import { getNestedValue } from './nested.js';
import { TextIndex } from '@mongolake/index/text-index.js';
import { GeoIndex } from '@mongolake/index/geo-index.js';
import { isGeoQuery, matchesGeoCondition, type GeoQuery } from '@mongolake/query/geospatial.js';

/**
 * Maximum allowed length for regex patterns to prevent ReDoS attacks.
 * Patterns longer than this will be rejected.
 */
export const MAX_REGEX_PATTERN_LENGTH = 1000;

/**
 * Patterns that indicate potentially dangerous regex constructs.
 * These patterns can cause catastrophic backtracking (ReDoS).
 */
const DANGEROUS_REGEX_PATTERNS = [
  // Nested quantifiers: (a+)+, (a*)+, (a+)*, (a*)*, etc.
  /\([^)]*[+*][^)]*\)[+*]/,
  // Overlapping alternations with quantifiers: (a|a)+, (a|aa)+
  /\(([^)|]+)\|(\1)+[^)]*\)[+*]/,
  // Repeated groups with quantifiers inside: (.+)+, (.*)+, (.+)*, (.*)*
  /\(\.[+*][^)]*\)[+*]/,
  // Nested quantifiers with character classes: ([a-z]+)+
  /\(\[[^\]]*\][+*][^)]*\)[+*]/,
  // Quantifier on quantified group: (a{1,})+, (a{2,})*
  /\([^)]*\{\d+,?\d*\}[^)]*\)[+*]/,
  // Multiple adjacent quantifiers (invalid but can cause issues in some engines)
  /[+*?]\{/,
  // Backreference with quantifier in a way that can cause exponential backtracking
  /\(.*\).*\\1[+*]/,
];

/**
 * Error thrown when a regex pattern is rejected for security reasons.
 */
export class RegexSecurityError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'RegexSecurityError';
  }
}

/**
 * Validates a regex pattern for security issues that could cause ReDoS attacks.
 *
 * @param pattern - The regex pattern to validate (string or RegExp)
 * @throws RegexSecurityError if the pattern is potentially dangerous
 * @returns true if the pattern is safe to use
 */
export function validateRegexPattern(pattern: unknown): boolean {
  let patternStr: string;

  if (pattern instanceof RegExp) {
    patternStr = pattern.source;
  } else if (typeof pattern === 'string') {
    patternStr = pattern;
  } else {
    // Non-string/RegExp patterns will fail in matchesRegex anyway
    return true;
  }

  // Check pattern length
  if (patternStr.length > MAX_REGEX_PATTERN_LENGTH) {
    throw new RegexSecurityError(
      `Regex pattern exceeds maximum allowed length of ${MAX_REGEX_PATTERN_LENGTH} characters`
    );
  }

  // Check for dangerous patterns that could cause catastrophic backtracking
  for (const dangerousPattern of DANGEROUS_REGEX_PATTERNS) {
    if (dangerousPattern.test(patternStr)) {
      throw new RegexSecurityError(
        'Regex pattern contains potentially dangerous constructs that could cause ReDoS attacks'
      );
    }
  }

  return true;
}

/**
 * Context for text search operations.
 * When provided, enables $text query support.
 */
export interface TextSearchContext {
  /** Text index to use for searching */
  textIndex: TextIndex;
  /** Pre-computed matching document IDs from text search */
  matchingDocIds?: Set<string>;
  /** Pre-computed scores for $meta: "textScore" */
  scores?: Map<string, number>;
}

/**
 * Context for geospatial query operations.
 * When provided, enables $near, $geoWithin, etc. query support.
 */
export interface GeoSearchContext {
  /** Geo index to use for querying */
  geoIndex: GeoIndex;
  /** Pre-computed matching document IDs from geo query */
  matchingDocIds?: Set<string>;
  /** Pre-computed distances for $near queries */
  distances?: Map<string, number>;
}

export type { GeoQueryContext } from '@mongolake/query/geospatial.js';

/**
 * Check if a value is comparable (can be used with comparison operators)
 * @param value - The value to check
 * @returns True if the value is a string, number, boolean, or Date
 */
function isComparable(value: unknown): value is string | number | boolean | Date {
  return typeof value === 'string' || typeof value === 'number' ||
         typeof value === 'boolean' || value instanceof Date;
}

/**
 * Compare two values for ordering
 * @param a - First value
 * @param b - Second value
 * @returns Negative if a < b, positive if a > b, 0 if equal
 */
function compareValues(a: unknown, b: unknown): number {
  if (a === b) return 0;
  if (a === null || a === undefined) return -1;
  if (b === null || b === undefined) return 1;
  if (typeof a === 'number' && typeof b === 'number') return a - b;
  if (typeof a === 'string' && typeof b === 'string') return a.localeCompare(b);
  if (a instanceof Date && b instanceof Date) return a.getTime() - b.getTime();
  return String(a).localeCompare(String(b));
}

/**
 * Check if a value matches a regex pattern
 * @param value - The value to test
 * @param pattern - The regex pattern (string or RegExp)
 * @returns True if the value matches the pattern
 * @throws RegexSecurityError if the pattern is potentially dangerous
 */
function matchesRegex(value: unknown, pattern: unknown): boolean {
  if (typeof value !== 'string') {
    return false;
  }

  // Validate the pattern for security issues before executing
  validateRegexPattern(pattern);

  if (pattern instanceof RegExp) {
    return pattern.test(value);
  }

  if (typeof pattern === 'string') {
    return new RegExp(pattern).test(value);
  }

  return false;
}

/**
 * Check if a single field value matches a condition
 * @param value - The field value from the document
 * @param condition - The filter condition (direct value or operator object)
 * @param doc - The full document (needed for recursive $not evaluation)
 * @param key - The field key (needed for recursive $not evaluation)
 * @returns True if the value matches the condition
 */
function matchesCondition(
  value: unknown,
  condition: unknown,
  doc: DocumentFields,
  key: string
): boolean {
  // Handle null condition (match null values)
  if (condition === null) {
    return value === null;
  }

  // Handle operator object conditions
  if (typeof condition === 'object' && condition !== null && !Array.isArray(condition)) {
    const ops = condition as OperatorCondition;

    // $eq - equality
    if ('$eq' in ops && value !== ops.$eq) {
      return false;
    }

    // $ne - not equal
    if ('$ne' in ops && value === ops.$ne) {
      return false;
    }

    // $gt - greater than
    if ('$gt' in ops && !(isComparable(value) && compareValues(value, ops.$gt) > 0)) {
      return false;
    }

    // $gte - greater than or equal
    if ('$gte' in ops && !(isComparable(value) && compareValues(value, ops.$gte) >= 0)) {
      return false;
    }

    // $lt - less than
    if ('$lt' in ops && !(isComparable(value) && compareValues(value, ops.$lt) < 0)) {
      return false;
    }

    // $lte - less than or equal
    if ('$lte' in ops && !(isComparable(value) && compareValues(value, ops.$lte) <= 0)) {
      return false;
    }

    // $in - value in array
    if ('$in' in ops && !(ops.$in as unknown[]).includes(value)) {
      return false;
    }

    // $nin - value not in array
    if ('$nin' in ops && (ops.$nin as unknown[]).includes(value)) {
      return false;
    }

    // $exists - field existence check
    if ('$exists' in ops) {
      const exists = value !== undefined;
      if (ops.$exists !== exists) {
        return false;
      }
    }

    // $regex - regular expression match
    if ('$regex' in ops) {
      if (!matchesRegex(value, ops.$regex)) {
        return false;
      }
    }

    // $not - negation of a condition
    if ('$not' in ops) {
      const notCondition = ops.$not;
      if (matchesCondition(value, notCondition, doc, key)) {
        return false;
      }
    }

    // Geospatial operators - $near, $nearSphere, $geoWithin, $geoIntersects
    if (isGeoQuery(ops)) {
      if (!matchesGeoCondition(value, ops as GeoQuery)) {
        return false;
      }
    }

    return true;
  }

  // Direct equality comparison
  return value === condition;
}

/**
 * Check if a document matches a MongoDB-style filter
 *
 * Supports the following operators:
 * - Comparison: $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin
 * - Element: $exists
 * - Evaluation: $regex
 * - Logical: $and, $or, $not
 *
 * @param doc - The document to check
 * @param filter - The MongoDB-style filter
 * @returns True if the document matches the filter
 *
 * @example
 * ```typescript
 * // Simple equality
 * matchesFilter({ name: 'Alice', age: 30 }, { name: 'Alice' }); // true
 *
 * // Comparison operators
 * matchesFilter({ age: 30 }, { age: { $gt: 25 } }); // true
 *
 * // Logical operators
 * matchesFilter(
 *   { status: 'active', age: 30 },
 *   { $and: [{ status: 'active' }, { age: { $gte: 18 } }] }
 * ); // true
 * ```
 */
export function matchesFilter<T extends Document>(
  doc: T,
  filter: Filter<T>
): boolean;

/**
 * Overload for AnyDocument (looser typing for internal use).
 * Allows matching against documents that may not strictly conform to BSONValue constraints.
 */
export function matchesFilter(
  doc: AnyDocument,
  filter: FilterQuery
): boolean;

export function matchesFilter(
  doc: Document | AnyDocument,
  filter: Filter<Document> | FilterQuery
): boolean {
  const docRecord = doc as DocumentFields;
  const filterRecord = filter as FilterQuery;

  for (const [key, condition] of Object.entries(filterRecord)) {
    // Handle $and - all conditions must match
    if (key === '$and') {
      const andConditions = condition as Array<Filter<Document> | FilterQuery>;
      if (!andConditions.every((f) => matchesFilter(doc as AnyDocument, f))) {
        return false;
      }
      continue;
    }

    // Handle $or - at least one condition must match
    if (key === '$or') {
      const orConditions = condition as Array<Filter<Document> | FilterQuery>;
      if (!orConditions.some((f) => matchesFilter(doc as AnyDocument, f))) {
        return false;
      }
      continue;
    }

    // Handle $nor - none of the conditions must match
    if (key === '$nor') {
      const norConditions = condition as Array<Filter<Document> | FilterQuery>;
      if (norConditions.some((f) => matchesFilter(doc as AnyDocument, f))) {
        return false;
      }
      continue;
    }

    // Handle $text - full-text search (requires text index context)
    // Note: $text queries are typically pre-processed at a higher level
    // where the text index is available. This handles inline $text filters.
    if (key === '$text') {
      // $text queries require external text index support
      // When matchesFilter is called directly without text index context,
      // we skip $text validation (it should be handled by the collection layer)
      continue;
    }

    // Get the value from the document (supports dot notation for nested fields)
    const value = getNestedValue(docRecord, key);

    // Check if the value matches the condition
    if (!matchesCondition(value, condition, docRecord, key)) {
      return false;
    }
  }

  return true;
}

/**
 * Check if a document matches a filter with text search support.
 *
 * This variant of matchesFilter accepts a text search context
 * that enables $text query operator support.
 *
 * @param doc - The document to check
 * @param filter - The MongoDB-style filter
 * @param textContext - Optional text search context with index and cached results
 * @returns True if the document matches the filter
 *
 * @example
 * ```typescript
 * const textIndex = new TextIndex('text_index', ['title', 'body']);
 * textIndex.indexDocument('doc1', { title: 'Hello World', body: 'Test content' });
 *
 * const matchingIds = new Set(textIndex.getMatchingDocIds('hello'));
 *
 * matchesFilterWithText(
 *   { _id: 'doc1', title: 'Hello World' },
 *   { $text: { $search: 'hello' } },
 *   { textIndex, matchingDocIds: matchingIds }
 * ); // true
 * ```
 */
export function matchesFilterWithText(
  doc: Document | AnyDocument,
  filter: Filter<Document> | FilterQuery,
  textContext?: TextSearchContext
): boolean {
  const docRecord = doc as DocumentFields;
  const filterRecord = filter as FilterQuery;

  for (const [key, condition] of Object.entries(filterRecord)) {
    // Handle $and - all conditions must match
    if (key === '$and') {
      const andConditions = condition as Array<Filter<Document> | FilterQuery>;
      if (!andConditions.every((f) => matchesFilterWithText(doc as AnyDocument, f, textContext))) {
        return false;
      }
      continue;
    }

    // Handle $or - at least one condition must match
    if (key === '$or') {
      const orConditions = condition as Array<Filter<Document> | FilterQuery>;
      if (!orConditions.some((f) => matchesFilterWithText(doc as AnyDocument, f, textContext))) {
        return false;
      }
      continue;
    }

    // Handle $nor - none of the conditions must match
    if (key === '$nor') {
      const norConditions = condition as Array<Filter<Document> | FilterQuery>;
      if (norConditions.some((f) => matchesFilterWithText(doc as AnyDocument, f, textContext))) {
        return false;
      }
      continue;
    }

    // Handle $text - full-text search
    if (key === '$text') {
      const textQuery = condition as { $search: string; $language?: string };

      if (!textQuery || !textQuery.$search) {
        // Invalid $text query
        return false;
      }

      // Get document ID for lookup
      const docId = extractDocId(docRecord);

      if (textContext?.matchingDocIds) {
        // Use pre-computed results
        if (!textContext.matchingDocIds.has(docId)) {
          return false;
        }
      } else if (textContext?.textIndex) {
        // Compute on the fly (less efficient)
        const matchingIds = textContext.textIndex.getMatchingDocIds(textQuery.$search);
        if (!matchingIds.includes(docId)) {
          return false;
        }
      } else {
        // No text index available - $text queries cannot be evaluated
        throw new Error('$text query requires a text index. Create a text index on the collection first.');
      }

      continue;
    }

    // Get the value from the document (supports dot notation for nested fields)
    const value = getNestedValue(docRecord, key);

    // Check if the value matches the condition
    if (!matchesCondition(value, condition, docRecord, key)) {
      return false;
    }
  }

  return true;
}

/**
 * Extract document ID as string for text search lookups.
 */
function extractDocId(doc: DocumentFields): string {
  const id = doc._id;
  if (id === undefined || id === null) {
    return '';
  }
  if (typeof id === 'object' && id !== null && 'toString' in id) {
    return (id as { toString: () => string }).toString();
  }
  return String(id);
}

/**
 * Check if a filter contains a $text query operator.
 *
 * @param filter - The filter to check
 * @returns True if the filter contains a $text operator
 */
export function hasTextQuery(filter: FilterQuery): boolean {
  if ('$text' in filter) {
    return true;
  }

  // Check in $and conditions
  if ('$and' in filter && Array.isArray(filter.$and)) {
    return filter.$and.some((f) => hasTextQuery(f as FilterQuery));
  }

  // Check in $or conditions
  if ('$or' in filter && Array.isArray(filter.$or)) {
    return filter.$or.some((f) => hasTextQuery(f as FilterQuery));
  }

  return false;
}

/**
 * Extract the $text query from a filter.
 *
 * @param filter - The filter to extract from
 * @returns The $text query object or null if not found
 */
export function extractTextQuery(
  filter: FilterQuery
): { $search: string; $language?: string } | null {
  if ('$text' in filter) {
    return filter.$text as { $search: string; $language?: string };
  }

  // Check in $and conditions
  if ('$and' in filter && Array.isArray(filter.$and)) {
    for (const f of filter.$and) {
      const textQuery = extractTextQuery(f as FilterQuery);
      if (textQuery) {
        return textQuery;
      }
    }
  }

  return null;
}

/**
 * Check if a filter contains a geospatial query operator.
 *
 * @param filter - The filter to check
 * @returns True if the filter contains a geo operator ($near, $nearSphere, $geoWithin, $geoIntersects)
 */
export function hasGeoQuery(filter: FilterQuery): boolean {
  for (const [key, value] of Object.entries(filter)) {
    // Skip logical operators - check recursively
    if (key === '$and' && Array.isArray(value)) {
      if (value.some(f => hasGeoQuery(f as FilterQuery))) {
        return true;
      }
      continue;
    }
    if (key === '$or' && Array.isArray(value)) {
      if (value.some(f => hasGeoQuery(f as FilterQuery))) {
        return true;
      }
      continue;
    }

    // Check field-level conditions
    if (isGeoQuery(value)) {
      return true;
    }
  }

  return false;
}

/**
 * Extract the geo query from a filter.
 *
 * @param filter - The filter to extract from
 * @returns The geo query field and options, or null if not found
 */
export function extractGeoQuery(
  filter: FilterQuery
): { field: string; query: GeoQuery } | null {
  for (const [key, value] of Object.entries(filter)) {
    if (key.startsWith('$')) continue;

    if (isGeoQuery(value)) {
      return { field: key, query: value as GeoQuery };
    }
  }

  // Check in $and conditions
  if ('$and' in filter && Array.isArray(filter.$and)) {
    for (const f of filter.$and) {
      const geoQuery = extractGeoQuery(f as FilterQuery);
      if (geoQuery) {
        return geoQuery;
      }
    }
  }

  return null;
}
