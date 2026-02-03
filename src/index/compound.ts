/**
 * Compound Index Implementation
 *
 * Provides multi-field index functionality for MongoLake collections.
 * Supports:
 * - Compound index metadata storage
 * - Index key generation for multiple fields
 * - B-tree structure for compound lookups
 * - Query planner integration
 * - Field ordering support (ascending/descending)
 * - Unique constraint enforcement
 * - Sparse index behavior
 * - Index intersection logic
 */

import { BTree, type CompareFn, type SerializedBTree } from './btree.js';
import { getNestedValue } from '../utils/nested.js';
import type { Document, IndexSpec } from '../types.js';

// ============================================================================
// Types
// ============================================================================

/** Field specification in a compound index */
export interface CompoundIndexField {
  /** Field name (supports dot notation) */
  field: string;
  /** Sort direction: 1 = ascending, -1 = descending */
  direction: 1 | -1;
}

/** Compound index metadata */
export interface CompoundIndexMetadata {
  /** Index name */
  name: string;
  /** Fields in order with directions */
  fields: CompoundIndexField[];
  /** Whether this is a unique index */
  unique: boolean;
  /** Whether this is a sparse index */
  sparse: boolean;
  /** Creation timestamp */
  createdAt: string;
}

/** Serialized compound index format */
export interface SerializedCompoundIndex {
  metadata: CompoundIndexMetadata;
  btree: SerializedBTree<string>;
}

/** Compound key value - represents the combined key for lookups */
export type CompoundKeyValue = (unknown | null | undefined)[];

/** Query condition for a compound index prefix */
export interface CompoundPrefixCondition {
  /** Field conditions in index order */
  conditions: Array<{
    field: string;
    value?: unknown;
    op?: 'eq' | 'gt' | 'gte' | 'lt' | 'lte' | 'in';
    values?: unknown[]; // For $in operator
  }>;
  /** Number of fields covered by equality conditions */
  equalityPrefix: number;
}

// ============================================================================
// Constants
// ============================================================================

/** Separator used in compound key serialization */
const KEY_SEPARATOR = '\x00';

/** Null marker for compound keys */
const NULL_MARKER = '\x01NULL\x01';

/** Undefined marker for compound keys */
const UNDEFINED_MARKER = '\x01UNDEF\x01';

/** Type markers for proper ordering */
const TYPE_MARKERS = {
  null: '\x00',
  undefined: '\x00',
  boolean: '\x01',
  number: '\x02',
  string: '\x03',
  date: '\x04',
  array: '\x05',
  object: '\x06',
} as const;

// ============================================================================
// Compound Index
// ============================================================================

/**
 * CompoundIndex provides multi-field indexing for efficient compound queries.
 *
 * @example
 * ```typescript
 * // Create a compound index on { name: 1, age: -1 }
 * const index = new CompoundIndex(
 *   'name_1_age_-1',
 *   [
 *     { field: 'name', direction: 1 },
 *     { field: 'age', direction: -1 }
 *   ]
 * );
 *
 * // Index documents
 * index.indexDocument('doc1', { _id: 'doc1', name: 'Alice', age: 30 });
 * index.indexDocument('doc2', { _id: 'doc2', name: 'Alice', age: 25 });
 * index.indexDocument('doc3', { _id: 'doc3', name: 'Bob', age: 35 });
 *
 * // Query using prefix
 * index.searchByPrefix([{ field: 'name', value: 'Alice', op: 'eq' }]);
 * // Returns: ['doc1', 'doc2']
 *
 * // Full compound query
 * index.searchByPrefix([
 *   { field: 'name', value: 'Alice', op: 'eq' },
 *   { field: 'age', value: 30, op: 'gte' }
 * ]);
 * // Returns: ['doc1']
 * ```
 */
export class CompoundIndex {
  /** Index name */
  readonly name: string;

  /** Fields with directions in index order */
  readonly fields: CompoundIndexField[];

  /** Whether this is a unique index */
  readonly unique: boolean;

  /** Whether this is a sparse index */
  readonly sparse: boolean;

  /** Internal B-tree using serialized compound keys */
  private btree: BTree<string>;

  /** Map from serialized key to original key values (for reverse lookup) */
  private keyValueMap: Map<string, CompoundKeyValue> = new Map();

  /**
   * Create a new compound index
   *
   * @param name - Index name
   * @param fields - Fields in index order with directions
   * @param unique - Whether to enforce uniqueness
   * @param sparse - Whether to skip documents missing indexed fields
   */
  constructor(
    name: string,
    fields: CompoundIndexField[],
    unique: boolean = false,
    sparse: boolean = false
  ) {
    if (fields.length === 0) {
      throw new Error('Compound index must have at least one field');
    }

    this.name = name;
    this.fields = fields;
    this.unique = unique;
    this.sparse = sparse;

    // Create B-tree with compound key comparison
    this.btree = new BTree<string>(
      name,
      fields.map((f) => f.field).join('_'),
      64,
      this.createCompareFunction(),
      unique
    );
  }

  /**
   * Create comparison function that respects field directions
   */
  private createCompareFunction(): CompareFn<string> {
    return (a: string, b: string): number => {
      const aValues = this.deserializeKey(a);
      const bValues = this.deserializeKey(b);

      for (let i = 0; i < this.fields.length; i++) {
        const aVal = aValues[i];
        const bVal = bValues[i];
        const direction = this.fields[i]!.direction;

        const cmp = this.compareValues(aVal, bVal);
        if (cmp !== 0) {
          return cmp * direction;
        }
      }

      return 0;
    };
  }

  /**
   * Compare two values using MongoDB ordering rules
   */
  private compareValues(a: unknown, b: unknown): number {
    // Get type order
    const aType = this.getTypeOrder(a);
    const bType = this.getTypeOrder(b);

    if (aType !== bType) {
      return aType - bType;
    }

    // Same type comparison
    if (a === null || a === undefined) {
      return 0;
    }

    if (typeof a === 'boolean' && typeof b === 'boolean') {
      return a === b ? 0 : a ? 1 : -1;
    }

    if (typeof a === 'number' && typeof b === 'number') {
      if (Number.isNaN(a) && Number.isNaN(b)) return 0;
      if (Number.isNaN(a)) return -1;
      if (Number.isNaN(b)) return 1;
      return a - b;
    }

    if (typeof a === 'string' && typeof b === 'string') {
      return a.localeCompare(b);
    }

    if (a instanceof Date && b instanceof Date) {
      return a.getTime() - b.getTime();
    }

    // Fallback to string comparison
    return String(a).localeCompare(String(b));
  }

  /**
   * Get type order for MongoDB comparison
   */
  private getTypeOrder(value: unknown): number {
    if (value === null) return 0;
    if (value === undefined) return 0;
    if (typeof value === 'boolean') return 1;
    if (typeof value === 'number') return 2;
    if (typeof value === 'string') return 3;
    if (value instanceof Date) return 4;
    if (Array.isArray(value)) return 5;
    if (typeof value === 'object') return 6;
    return 7;
  }

  // --------------------------------------------------------------------------
  // Key Serialization
  // --------------------------------------------------------------------------

  /**
   * Serialize a compound key to a string for B-tree storage.
   * Handles proper ordering across types.
   */
  serializeKey(values: CompoundKeyValue): string {
    const parts: string[] = [];

    for (let i = 0; i < this.fields.length; i++) {
      const value = values[i];
      parts.push(this.serializeValue(value, this.fields[i]!.direction));
    }

    return parts.join(KEY_SEPARATOR);
  }

  /**
   * Serialize a single value to a sortable string
   */
  private serializeValue(value: unknown, direction: 1 | -1): string {
    if (value === null) {
      return direction === 1 ? NULL_MARKER : this.invertString(NULL_MARKER);
    }
    if (value === undefined) {
      return direction === 1 ? UNDEFINED_MARKER : this.invertString(UNDEFINED_MARKER);
    }

    const typeMarker = this.getTypeMarker(value);
    let serialized: string;

    if (typeof value === 'boolean') {
      serialized = value ? '1' : '0';
    } else if (typeof value === 'number') {
      // Pad numbers for proper string ordering
      serialized = this.serializeNumber(value);
    } else if (typeof value === 'string') {
      serialized = value;
    } else if (value instanceof Date) {
      serialized = this.serializeNumber(value.getTime());
    } else {
      // Fallback to JSON for complex types
      serialized = JSON.stringify(value);
    }

    const result = typeMarker + serialized;
    return direction === 1 ? result : this.invertString(result);
  }

  /**
   * Get type marker for proper cross-type ordering
   */
  private getTypeMarker(value: unknown): string {
    if (value === null) return TYPE_MARKERS.null;
    if (value === undefined) return TYPE_MARKERS.undefined;
    if (typeof value === 'boolean') return TYPE_MARKERS.boolean;
    if (typeof value === 'number') return TYPE_MARKERS.number;
    if (typeof value === 'string') return TYPE_MARKERS.string;
    if (value instanceof Date) return TYPE_MARKERS.date;
    if (Array.isArray(value)) return TYPE_MARKERS.array;
    if (typeof value === 'object') return TYPE_MARKERS.object;
    return '\x07';
  }

  /**
   * Serialize a number to a sortable string
   */
  private serializeNumber(n: number): string {
    if (Number.isNaN(n)) return 'NaN';
    if (!Number.isFinite(n)) return n > 0 ? '+Inf' : '-Inf';

    // Handle negative numbers by inverting for proper sort order
    const isNegative = n < 0;
    const absValue = Math.abs(n);

    // Convert to exponential notation for consistent comparison
    const exp = absValue === 0 ? 0 : Math.floor(Math.log10(absValue));
    const expStr = (exp + 1000).toString().padStart(4, '0'); // Offset to handle negatives
    const mantissa = absValue === 0 ? '0' : (absValue / Math.pow(10, exp)).toFixed(15);

    if (isNegative) {
      // Invert for negative numbers to sort correctly
      return '-' + this.invertDigits(expStr) + this.invertDigits(mantissa);
    } else {
      return '+' + expStr + mantissa;
    }
  }

  /**
   * Invert digits for descending sort (9-digit)
   */
  private invertDigits(s: string): string {
    return s
      .split('')
      .map((c) => {
        if (c >= '0' && c <= '9') {
          return String(9 - parseInt(c, 10));
        }
        return c;
      })
      .join('');
  }

  /**
   * Invert a string for descending sort
   */
  private invertString(s: string): string {
    return s
      .split('')
      .map((c) => String.fromCharCode(0xffff - c.charCodeAt(0)))
      .join('');
  }

  /**
   * Deserialize a compound key from string
   */
  deserializeKey(serialized: string): CompoundKeyValue {
    // First check the cache
    const cached = this.keyValueMap.get(serialized);
    if (cached) {
      return cached;
    }

    // Deserialize manually (this is a simplified version)
    const parts = serialized.split(KEY_SEPARATOR);
    const values: CompoundKeyValue = [];

    for (let i = 0; i < this.fields.length && i < parts.length; i++) {
      const part = parts[i]!;
      const direction = this.fields[i]!.direction;
      values.push(this.deserializeValue(part, direction));
    }

    return values;
  }

  /**
   * Deserialize a single value
   */
  private deserializeValue(serialized: string, direction: 1 | -1): unknown {
    // Un-invert if descending
    const s = direction === 1 ? serialized : this.invertString(serialized);

    if (s === NULL_MARKER) return null;
    if (s === UNDEFINED_MARKER) return undefined;

    const typeMarker = s[0];
    const value = s.slice(1);

    switch (typeMarker) {
      case TYPE_MARKERS.boolean:
        return value === '1';
      case TYPE_MARKERS.number:
        return this.deserializeNumber(value);
      case TYPE_MARKERS.string:
        return value;
      case TYPE_MARKERS.date:
        return new Date(this.deserializeNumber(value));
      default:
        try {
          return JSON.parse(value);
        } catch {
          return value;
        }
    }
  }

  /**
   * Deserialize a number from string
   */
  private deserializeNumber(s: string): number {
    if (s === 'NaN') return NaN;
    if (s === '+Inf') return Infinity;
    if (s === '-Inf') return -Infinity;

    const isNegative = s[0] === '-';
    let expStr = s.slice(1, 5);
    let mantissa = s.slice(5);

    if (isNegative) {
      expStr = this.invertDigits(expStr);
      mantissa = this.invertDigits(mantissa);
    }

    const exp = parseInt(expStr, 10) - 1000;
    const mant = parseFloat(mantissa);

    return isNegative ? -mant * Math.pow(10, exp) : mant * Math.pow(10, exp);
  }

  // --------------------------------------------------------------------------
  // Index Operations
  // --------------------------------------------------------------------------

  /**
   * Extract compound key from a document
   *
   * @param doc - Document to extract key from
   * @returns Compound key values or null if sparse and any field is missing
   */
  extractKey(doc: Document): CompoundKeyValue | null {
    const values: CompoundKeyValue = [];
    let hasUndefined = false;

    for (const fieldSpec of this.fields) {
      const value = getNestedValue(doc, fieldSpec.field);
      values.push(value);

      if (value === undefined || value === null) {
        hasUndefined = true;
      }
    }

    // For sparse indexes, skip documents with any undefined/null field
    if (this.sparse && hasUndefined) {
      return null;
    }

    return values;
  }

  /**
   * Index a document
   *
   * @param docId - Document ID
   * @param doc - Document to index
   * @throws Error if unique constraint is violated
   */
  indexDocument(docId: string, doc: Document): void {
    const key = this.extractKey(doc);

    if (key === null) {
      // Sparse index skips this document
      return;
    }

    const serializedKey = this.serializeKey(key);

    // Store key mapping for reverse lookup
    this.keyValueMap.set(serializedKey, key);

    // Insert into B-tree (will throw on unique violation)
    this.btree.insert(serializedKey, docId);
  }

  /**
   * Remove a document from the index
   *
   * @param docId - Document ID
   * @param doc - Document being removed (for key extraction)
   */
  unindexDocument(docId: string, doc: Document): void {
    const key = this.extractKey(doc);

    if (key === null) {
      return;
    }

    const serializedKey = this.serializeKey(key);
    this.btree.delete(serializedKey, docId);

    // Check if any other documents use this key
    if (this.btree.search(serializedKey).length === 0) {
      this.keyValueMap.delete(serializedKey);
    }
  }

  /**
   * Search using exact compound key match
   *
   * @param keyValues - Exact values for all fields
   * @returns Matching document IDs
   */
  search(keyValues: CompoundKeyValue): string[] {
    const serializedKey = this.serializeKey(keyValues);
    return this.btree.search(serializedKey);
  }

  /**
   * Check if a compound key exists
   */
  has(keyValues: CompoundKeyValue): boolean {
    const serializedKey = this.serializeKey(keyValues);
    return this.btree.has(serializedKey);
  }

  /**
   * Search using a prefix of the compound key (leftmost fields)
   *
   * This is the primary method for compound index queries.
   * It supports equality conditions on a prefix of fields,
   * with an optional range condition on the next field.
   *
   * @param condition - Prefix conditions in index order
   * @returns Matching document IDs
   */
  searchByPrefix(
    conditions: Array<{
      field: string;
      value?: unknown;
      op?: 'eq' | 'gt' | 'gte' | 'lt' | 'lte' | 'in';
      values?: unknown[];
    }>
  ): string[] {
    // Validate conditions match index prefix
    for (let i = 0; i < conditions.length; i++) {
      const cond = conditions[i]!;
      if (i >= this.fields.length) {
        break;
      }
      if (cond.field !== this.fields[i]!.field) {
        throw new Error(
          `Condition field '${cond.field}' does not match index field '${this.fields[i]!.field}' at position ${i}`
        );
      }
    }

    // Build prefix key for equality conditions
    const equalityConditions = conditions.filter(
      (c) => c.op === 'eq' || c.op === undefined
    );

    // Handle $in operator
    const inCondition = conditions.find((c) => c.op === 'in');
    if (inCondition && inCondition.values) {
      const results = new Set<string>();
      for (const val of inCondition.values) {
        const modifiedConditions = conditions.map((c) =>
          c === inCondition ? { ...c, value: val, op: 'eq' as const } : c
        );
        for (const docId of this.searchByPrefix(modifiedConditions)) {
          results.add(docId);
        }
      }
      return Array.from(results);
    }

    // Handle range conditions
    const rangeCondition = conditions.find(
      (c): c is { field: string; value?: unknown; op: 'gt' | 'gte' | 'lt' | 'lte' } =>
        c.op !== undefined && ['gt', 'gte', 'lt', 'lte'].includes(c.op)
    );

    if (rangeCondition) {
      return this.searchByPrefixWithRange(equalityConditions, rangeCondition);
    }

    // All equality conditions - exact prefix match
    if (equalityConditions.length === this.fields.length) {
      // Exact match on all fields
      const key = equalityConditions.map((c) => c.value);
      return this.search(key);
    }

    // Prefix match - range scan
    return this.searchByEqualityPrefix(equalityConditions);
  }

  /**
   * Search with equality prefix
   */
  private searchByEqualityPrefix(
    equalityConditions: Array<{ field: string; value?: unknown }>
  ): string[] {
    if (equalityConditions.length === 0) {
      // Return all entries
      return this.btree.entries().flatMap(([, docIds]) => docIds);
    }

    const prefixValues = equalityConditions.map((c) => c.value);

    // Build min and max keys for range scan
    const minKey = this.buildPrefixKey(prefixValues, 'min');
    const maxKey = this.buildPrefixKey(prefixValues, 'max');

    const entries = this.btree.range(minKey, maxKey);

    // Filter to only include entries that match the prefix exactly
    const results: string[] = [];
    for (const [serializedKey, docIds] of entries) {
      const keyValues = this.deserializeKey(serializedKey);
      let matches = true;
      for (let i = 0; i < equalityConditions.length && i < keyValues.length; i++) {
        if (!this.valuesEqual(keyValues[i], equalityConditions[i]?.value)) {
          matches = false;
          break;
        }
      }
      if (matches) {
        results.push(...docIds);
      }
    }

    return results;
  }

  /**
   * Search with equality prefix and range condition
   */
  private searchByPrefixWithRange(
    equalityConditions: Array<{ field: string; value?: unknown }>,
    rangeCondition: {
      field: string;
      value?: unknown;
      op?: 'gt' | 'gte' | 'lt' | 'lte';
    }
  ): string[] {
    const prefixValues = equalityConditions.map((c) => c.value);

    // Build range bounds
    let minKey: string | null = null;
    let maxKey: string | null = null;

    if (rangeCondition.op === 'gt' || rangeCondition.op === 'gte') {
      const rangeValues = [...prefixValues, rangeCondition.value];
      minKey = this.buildPrefixKey(rangeValues, 'min');
      maxKey = this.buildPrefixKey(prefixValues, 'max');
    }

    if (rangeCondition.op === 'lt' || rangeCondition.op === 'lte') {
      if (minKey === null) {
        minKey = this.buildPrefixKey(prefixValues, 'min');
      }
      const rangeValues = [...prefixValues, rangeCondition.value];
      maxKey = this.buildPrefixKey(rangeValues, 'max');
    }

    const entries = this.btree.range(minKey, maxKey);

    // Filter results based on inclusivity and prefix match
    const results: string[] = [];
    const rangeFieldIndex = equalityConditions.length;

    for (const [serializedKey, docIds] of entries) {
      const keyValues = this.deserializeKey(serializedKey);

      // Check equality prefix
      let prefixMatches = true;
      for (let i = 0; i < equalityConditions.length && i < keyValues.length; i++) {
        if (!this.valuesEqual(keyValues[i], equalityConditions[i]?.value)) {
          prefixMatches = false;
          break;
        }
      }

      if (!prefixMatches) continue;

      // Check range condition
      const rangeValue = keyValues[rangeFieldIndex];
      const cmp = this.compareValues(rangeValue, rangeCondition.value);

      if (rangeCondition.op === 'gt' && cmp <= 0) continue;
      if (rangeCondition.op === 'gte' && cmp < 0) continue;
      if (rangeCondition.op === 'lt' && cmp >= 0) continue;
      if (rangeCondition.op === 'lte' && cmp > 0) continue;

      results.push(...docIds);
    }

    return results;
  }

  /**
   * Build a prefix key with min or max padding for remaining fields
   */
  private buildPrefixKey(prefixValues: unknown[], mode: 'min' | 'max'): string {
    const fullKey: CompoundKeyValue = [...prefixValues];

    // Pad with min or max values for remaining fields
    for (let i = prefixValues.length; i < this.fields.length; i++) {
      // Use null for min (sorts first) or max number for max
      fullKey.push(mode === 'min' ? null : '\uffff');
    }

    return this.serializeKey(fullKey);
  }

  /**
   * Check if two values are equal for index purposes
   */
  private valuesEqual(a: unknown, b: unknown): boolean {
    if (a === b) return true;
    if (a === null && b === null) return true;
    if (a === undefined && b === undefined) return true;
    if (a instanceof Date && b instanceof Date) {
      return a.getTime() === b.getTime();
    }
    return false;
  }

  // --------------------------------------------------------------------------
  // Index Analysis
  // --------------------------------------------------------------------------

  /**
   * Check if this index can be used for a filter.
   *
   * An index can be used if the filter includes equality conditions
   * on a prefix of the index fields, optionally followed by a range
   * condition on the next field.
   *
   * @param filter - Query filter
   * @returns Object with usability and covered fields
   */
  canSupportFilter(filter: Record<string, unknown>): {
    canUse: boolean;
    coveredFields: string[];
    equalityFields: string[];
    rangeField?: string;
  } {
    const result = {
      canUse: false,
      coveredFields: [] as string[],
      equalityFields: [] as string[],
      rangeField: undefined as string | undefined,
    };

    // Check each index field in order
    for (const fieldSpec of this.fields) {
      const fieldName = fieldSpec.field;
      const condition = filter[fieldName];

      if (condition === undefined) {
        // Field not in filter - can't use any more fields
        break;
      }

      // Check if this is an equality condition
      if (this.isEqualityCondition(condition)) {
        result.equalityFields.push(fieldName);
        result.coveredFields.push(fieldName);
        result.canUse = true;
        continue;
      }

      // Check if this is a range condition
      if (this.isRangeCondition(condition)) {
        result.rangeField = fieldName;
        result.coveredFields.push(fieldName);
        result.canUse = true;
        // Can't use any more fields after a range
        break;
      }

      // Check if this is an $in condition (treated as equality)
      if (this.isInCondition(condition)) {
        result.equalityFields.push(fieldName);
        result.coveredFields.push(fieldName);
        result.canUse = true;
        continue;
      }

      // Unsupported condition type - can't use any more fields
      break;
    }

    return result;
  }

  /**
   * Check if a condition is an equality condition
   */
  private isEqualityCondition(condition: unknown): boolean {
    if (condition === null || typeof condition !== 'object') {
      // Simple value = equality
      return true;
    }

    const cond = condition as Record<string, unknown>;
    return '$eq' in cond;
  }

  /**
   * Check if a condition is a range condition
   */
  private isRangeCondition(condition: unknown): boolean {
    if (condition === null || typeof condition !== 'object') {
      return false;
    }

    const cond = condition as Record<string, unknown>;
    return '$gt' in cond || '$gte' in cond || '$lt' in cond || '$lte' in cond;
  }

  /**
   * Check if a condition is an $in condition
   */
  private isInCondition(condition: unknown): boolean {
    if (condition === null || typeof condition !== 'object') {
      return false;
    }

    const cond = condition as Record<string, unknown>;
    return '$in' in cond && Array.isArray(cond.$in);
  }

  /**
   * Get field names in index order
   */
  getFieldNames(): string[] {
    return this.fields.map((f) => f.field);
  }

  /**
   * Check if this index covers a set of fields (for index-only queries)
   */
  coversFields(fields: string[]): boolean {
    const indexFields = new Set(this.fields.map((f) => f.field));
    return fields.every((f) => indexFields.has(f));
  }

  // --------------------------------------------------------------------------
  // Utility Methods
  // --------------------------------------------------------------------------

  /**
   * Get the number of indexed keys
   */
  get size(): number {
    return this.btree.size;
  }

  /**
   * Check if the index is empty
   */
  get isEmpty(): boolean {
    return this.btree.isEmpty;
  }

  /**
   * Clear all entries from the index
   */
  clear(): void {
    this.btree.clear();
    this.keyValueMap.clear();
  }

  /**
   * Get all entries in the index
   */
  entries(): Array<[CompoundKeyValue, string[]]> {
    return this.btree.entries().map(([serializedKey, docIds]) => [
      this.deserializeKey(serializedKey),
      docIds,
    ]);
  }

  // --------------------------------------------------------------------------
  // Serialization
  // --------------------------------------------------------------------------

  /**
   * Serialize the index for persistence
   */
  serialize(): SerializedCompoundIndex {
    return {
      metadata: {
        name: this.name,
        fields: this.fields,
        unique: this.unique,
        sparse: this.sparse,
        createdAt: new Date().toISOString(),
      },
      btree: this.btree.serialize(),
    };
  }

  /**
   * Deserialize an index from storage
   */
  static deserialize(data: SerializedCompoundIndex): CompoundIndex {
    const index = new CompoundIndex(
      data.metadata.name,
      data.metadata.fields,
      data.metadata.unique,
      data.metadata.sparse
    );

    // Deserialize B-tree
    index.btree = BTree.deserialize(data.btree, index.createCompareFunction());

    // Rebuild key value map
    for (const [serializedKey] of index.btree.entries()) {
      const values = index.deserializeKey(serializedKey);
      index.keyValueMap.set(serializedKey, values);
    }

    return index;
  }

  /**
   * Convert to JSON string
   */
  toJSON(): string {
    return JSON.stringify(this.serialize());
  }

  /**
   * Create from JSON string
   */
  static fromJSON(json: string): CompoundIndex {
    return CompoundIndex.deserialize(JSON.parse(json));
  }
}

// ============================================================================
// Index Intersection
// ============================================================================

/**
 * Intersect results from multiple indexes
 *
 * @param indexResults - Array of document ID arrays from different indexes
 * @returns Document IDs that appear in all results
 */
export function intersectIndexResults(indexResults: string[][]): string[] {
  if (indexResults.length === 0) {
    return [];
  }

  if (indexResults.length === 1) {
    return indexResults[0]!;
  }

  // Start with smallest result set for efficiency
  const sorted = [...indexResults].sort((a, b) => a.length - b.length);
  let result = new Set(sorted[0]!);

  for (let i = 1; i < sorted.length; i++) {
    const nextSet = new Set(sorted[i]!);
    result = new Set([...result].filter((id) => nextSet.has(id)));

    // Early exit if result is empty
    if (result.size === 0) {
      return [];
    }
  }

  return Array.from(result);
}

/**
 * Union results from multiple indexes (for $or queries)
 *
 * @param indexResults - Array of document ID arrays from different indexes
 * @returns Unique document IDs from all results
 */
export function unionIndexResults(indexResults: string[][]): string[] {
  const result = new Set<string>();

  for (const ids of indexResults) {
    for (const id of ids) {
      result.add(id);
    }
  }

  return Array.from(result);
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Parse an IndexSpec into CompoundIndexField array
 *
 * @param spec - MongoDB-style index specification
 * @returns Array of field specifications
 */
export function parseIndexSpec(spec: IndexSpec): CompoundIndexField[] {
  const fields: CompoundIndexField[] = [];

  for (const [field, direction] of Object.entries(spec)) {
    if (direction === 'text' || direction === '2dsphere' || direction === 'hashed') {
      // Skip non-standard index types
      continue;
    }

    fields.push({
      field,
      direction: direction as 1 | -1,
    });
  }

  return fields;
}

/**
 * Generate a name for a compound index from its spec
 *
 * @param spec - Index specification
 * @returns Generated index name
 */
export function generateCompoundIndexName(spec: IndexSpec): string {
  return Object.entries(spec)
    .map(([field, dir]) => `${field}_${dir}`)
    .join('_');
}

// ============================================================================
// Exports
// ============================================================================

export default CompoundIndex;
