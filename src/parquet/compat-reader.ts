/**
 * Backward Compatibility Reader
 *
 * Reads Parquet files written with older schema versions using the current schema.
 * Handles:
 * - Missing columns (new fields in current schema)
 * - Removed columns (fields in old file not in current schema)
 * - Type widening (int32 -> int64, float -> double, etc.)
 * - Default values for new required fields
 *
 * Key optimizations:
 * - Cached column mappings for repeated reads with same schema pair
 * - Pre-compiled type transformations
 * - Efficient schema reconciliation with hash-based lookups
 *
 * @module parquet/compat-reader
 */

import { RowGroupReader } from './row-group-reader.js';
import { isTypeWideningAllowed } from '@mongolake/schema/versioning.js';

import type { SerializedRowGroup } from './row-group.js';
import type { SchemaField } from '@mongolake/schema/versioning.js';

// ============================================================================
// Type Definitions
// ============================================================================

/**
 * A type transformation function for column values
 */
export interface TypeTransform {
  /** Name of the transform */
  name: string;
  /** Transform function */
  apply: (value: unknown) => unknown;
}

/**
 * Mapping for a single column between source and target schemas
 */
export interface ColumnMapping {
  /** Source column name (null if new field) */
  sourceColumn: string | null;
  /** Target column name */
  targetColumn: string;
  /** Type transformation to apply (null if no transform needed) */
  transform: TypeTransform | null;
  /** Whether to use default value for this column */
  useDefault?: boolean;
}

/**
 * Type change information
 */
export interface TypeChange {
  /** Column name */
  column: string;
  /** Original type */
  fromType: string;
  /** New type */
  toType: string;
  /** Whether the change is a safe widening */
  isWidening: boolean;
}

/**
 * Widened column information
 */
export interface WidenedColumn {
  /** Column name */
  column: string;
  /** Original type */
  fromType: string;
  /** New type */
  toType: string;
}

/**
 * Schema mapping result from reconciliation
 */
export interface SchemaMapping {
  /** Columns that exist in target but not in source */
  missingColumns: string[];
  /** Columns that exist in source but not in target */
  removedColumns: string[];
  /** Columns that had their types widened */
  widenedColumns: WidenedColumn[];
  /** Columns that received default values */
  columnsWithDefaults: string[];
}

/**
 * Compatibility warning
 */
export interface CompatibilityWarning {
  /** Warning type */
  type: 'removed_column' | 'type_narrowing' | 'missing_required' | 'other';
  /** Column name (if applicable) */
  column?: string;
  /** Warning message */
  message: string;
}

/**
 * Schema reconciliation result
 */
export interface SchemaReconciliation {
  /** Columns added in target schema */
  addedColumns: string[];
  /** Columns removed from source schema */
  removedColumns: string[];
  /** Type changes between schemas */
  typeChanges: TypeChange[];
  /** Whether the schemas are compatible */
  isCompatible: boolean;
}

/**
 * Result of reading with the compat reader
 */
export interface ReadResult<T> {
  /** Reconstructed documents */
  documents: T[];
  /** Schema mapping information */
  schemaMapping: SchemaMapping;
  /** Whether the read was fully compatible */
  isCompatible: boolean;
  /** Any compatibility warnings */
  compatibilityWarnings: CompatibilityWarning[];
}

/**
 * Default value definition - can be a static value or a function
 */
export type DefaultValue<T = unknown> = T | ((doc: Record<string, unknown>) => T);

/**
 * Forward compatibility hints for schema evolution
 */
export interface ForwardCompatibilityHints {
  /** Fields that may be added in future versions */
  expectedNewFields?: string[];
  /** Fields that may be removed in future versions */
  deprecatedFields?: string[];
  /** Fields that may have their types widened */
  expectedTypeWidenings?: Array<{ field: string; fromType: string; toType: string }>;
}

/**
 * Options for creating a CompatReader
 */
export interface CompatReaderOptions {
  /** Target schema to read into */
  targetSchema: Record<string, SchemaField>;
  /** Source schema (optional - inferred from file if not provided) */
  sourceSchema?: Record<string, SchemaField>;
  /** Default values for new fields */
  defaults?: Record<string, DefaultValue>;
  /** Whether to use schema-defined defaults */
  useSchemaDefaults?: boolean;
  /** Strict mode - throw on incompatible changes */
  strictMode?: boolean;
  /** Preserve columns that exist in source but not in target */
  preserveRemovedColumns?: boolean;
  /** Forward compatibility hints */
  forwardCompatibilityHints?: ForwardCompatibilityHints;
  /** Enable column mapping cache for repeated reads */
  enableCache?: boolean;
}

/**
 * Cached schema pair mapping
 */
interface CachedMapping {
  /** Column mappings */
  mappings: Map<string, ColumnMapping>;
  /** Schema reconciliation result */
  reconciliation: SchemaReconciliation;
  /** Target schema column list (for fast iteration) */
  targetColumns: Array<[string, SchemaField]>;
}

// ============================================================================
// Type Transform Registry (Optimized)
// ============================================================================

/**
 * Pre-defined type transformations for common widening operations.
 * Using a registry pattern for efficient lookup.
 */
const TYPE_TRANSFORM_REGISTRY: Map<string, TypeTransform> = new Map([
  // int32 -> int64
  ['int32->int64', {
    name: 'widen_int32_to_int64',
    apply: (value: unknown) => value, // No-op in JS (numbers are already 64-bit)
  }],

  // int32 -> double
  ['int32->double', {
    name: 'widen_int32_to_double',
    apply: (value: unknown) => typeof value === 'number' ? value : value,
  }],

  // int64 -> double
  ['int64->double', {
    name: 'widen_int64_to_double',
    apply: (value: unknown) => {
      if (typeof value === 'bigint') {
        return Number(value);
      }
      return value;
    },
  }],

  // float -> double
  ['float->double', {
    name: 'widen_float_to_double',
    apply: (value: unknown) => value, // No transformation needed in JS
  }],

  // date -> timestamp
  ['date->timestamp', {
    name: 'widen_date_to_timestamp',
    apply: (value: unknown) => {
      if (value instanceof Date) {
        return value;
      }
      if (typeof value === 'number' || typeof value === 'string') {
        return new Date(value);
      }
      return value;
    },
  }],
]);

// ============================================================================
// Schema Fingerprint for Caching
// ============================================================================

/**
 * Generate a fingerprint for a schema to use as cache key.
 * Uses a simple FNV-1a hash for speed.
 */
function generateSchemaFingerprint(schema: Record<string, SchemaField>): string {
  const sortedKeys = Object.keys(schema).sort();
  const normalized = sortedKeys.map((key) => {
    const field = schema[key]!;
    return `${key}:${field.type}:${field.required}`;
  }).join('|');

  // FNV-1a hash
  let hash = 2166136261;
  for (let i = 0; i < normalized.length; i++) {
    hash ^= normalized.charCodeAt(i);
    hash = (hash * 16777619) >>> 0;
  }

  return hash.toString(16).padStart(8, '0');
}

// ============================================================================
// CompatReader Implementation
// ============================================================================

/**
 * Reads Parquet files with schema evolution support.
 *
 * Handles backward compatibility when reading files written with older
 * schema versions, applying type widening, default values, and column mapping.
 *
 * Features:
 * - Automatic schema reconciliation
 * - Type widening (int32->int64, float->double, etc.)
 * - Default value application for new fields
 * - Strict mode for enforcing schema compatibility
 * - Column mapping caching for repeated reads
 * - Forward compatibility hints for planned schema changes
 *
 * @example
 * ```typescript
 * const reader = new CompatReader({
 *   targetSchema: newSchema,
 *   sourceSchema: oldSchema,
 *   defaults: { email: 'unknown@example.com' },
 * });
 *
 * const result = reader.read<UserDocument>(serializedRowGroup);
 * console.log(result.documents);
 * ```
 */
export class CompatReader {
  private readonly targetSchema: Record<string, SchemaField>;
  private readonly sourceSchema: Record<string, SchemaField> | undefined;
  private readonly defaults: Record<string, DefaultValue>;
  private readonly useSchemaDefaults: boolean;
  private readonly strictMode: boolean;
  private readonly preserveRemovedColumns: boolean;
  private readonly forwardCompatibilityHints: ForwardCompatibilityHints;
  private readonly enableCache: boolean;
  private readonly baseReader: RowGroupReader;

  // Caching
  private readonly mappingCache: Map<string, CachedMapping> = new Map();
  private readonly targetSchemaFingerprint: string;
  private readonly precomputedTargetColumns: Array<[string, SchemaField]>;

  constructor(options: CompatReaderOptions) {
    this.targetSchema = options.targetSchema;
    this.sourceSchema = options.sourceSchema;
    this.defaults = options.defaults ?? {};
    this.useSchemaDefaults = options.useSchemaDefaults ?? false;
    this.strictMode = options.strictMode ?? false;
    this.preserveRemovedColumns = options.preserveRemovedColumns ?? false;
    this.forwardCompatibilityHints = options.forwardCompatibilityHints ?? {};
    this.enableCache = options.enableCache ?? true;
    this.baseReader = new RowGroupReader();

    // Pre-compute target schema data for efficiency
    this.targetSchemaFingerprint = generateSchemaFingerprint(this.targetSchema);
    this.precomputedTargetColumns = Object.entries(this.targetSchema);

    // Pre-cache mapping if source schema is provided
    if (this.sourceSchema && this.enableCache) {
      this.getCachedMapping(this.sourceSchema);
    }
  }

  /**
   * Get the target schema.
   */
  getTargetSchema(): Record<string, SchemaField> {
    return this.targetSchema;
  }

  /**
   * Get the configured default values.
   */
  getDefaults(): Record<string, DefaultValue> {
    return this.defaults;
  }

  /**
   * Check if strict mode is enabled.
   */
  isStrictMode(): boolean {
    return this.strictMode;
  }

  /**
   * Clear the column mapping cache.
   */
  clearCache(): void {
    this.mappingCache.clear();
  }

  /**
   * Get cache statistics for monitoring.
   */
  getCacheStats(): { size: number; hits: number } {
    return {
      size: this.mappingCache.size,
      hits: 0, // Would need to track this separately
    };
  }

  /**
   * Validate compatibility between two schemas without reading data.
   *
   * @param sourceSchema - The source schema to validate
   * @returns Validation result with compatibility status and details
   */
  validateCompatibility(sourceSchema: Record<string, SchemaField>): {
    isCompatible: boolean;
    errors: string[];
    warnings: string[];
  } {
    const reconciliation = reconcileSchemas(sourceSchema, this.targetSchema);
    const errors: string[] = [];
    const warnings: string[] = [];

    // Check for removed columns
    for (const column of reconciliation.removedColumns) {
      if (this.strictMode) {
        errors.push(`Removed column '${column}' is not allowed in strict mode`);
      } else {
        warnings.push(`Column '${column}' will be dropped`);
      }
    }

    // Check for type narrowing
    for (const typeChange of reconciliation.typeChanges) {
      if (!typeChange.isWidening) {
        if (this.strictMode) {
          errors.push(
            `Type narrowing for '${typeChange.column}': ${typeChange.fromType} -> ${typeChange.toType}`
          );
        } else {
          warnings.push(
            `Type narrowing for '${typeChange.column}': ${typeChange.fromType} -> ${typeChange.toType}`
          );
        }
      }
    }

    // Check forward compatibility hints
    if (this.forwardCompatibilityHints.expectedNewFields) {
      for (const field of this.forwardCompatibilityHints.expectedNewFields) {
        if (reconciliation.addedColumns.includes(field)) {
          // Expected new field found - good
        }
      }
    }

    return {
      isCompatible: errors.length === 0,
      errors,
      warnings,
    };
  }

  /**
   * Read documents from a serialized row group with schema compatibility handling.
   *
   * @param serialized - The serialized row group data
   * @returns Read result with documents and schema mapping info
   */
  read<T extends Record<string, unknown>>(serialized: SerializedRowGroup): ReadResult<T> {
    // Handle empty row groups
    if (serialized.rowCount === 0) {
      return this.createEmptyResult<T>();
    }

    // Infer source schema from serialized data if not provided
    const sourceSchema = this.sourceSchema ?? this.inferSourceSchema(serialized);

    // Get cached or compute mapping
    const cached = this.getCachedMapping(sourceSchema);

    // Validate and collect warnings
    const warnings = this.validateAndCollectWarnings(cached.reconciliation);

    // Read raw documents using base reader
    const rawDocuments = this.baseReader.read<Record<string, unknown>>(serialized);

    // Transform documents according to schema mapping (optimized)
    const documents = this.transformDocuments<T>(rawDocuments, cached, sourceSchema);

    // Build widened columns list
    const widenedColumns = this.buildWidenedColumnsList(cached.reconciliation);

    return {
      documents: documents.transformed,
      schemaMapping: {
        missingColumns: cached.reconciliation.addedColumns,
        removedColumns: cached.reconciliation.removedColumns,
        widenedColumns,
        columnsWithDefaults: documents.columnsWithDefaults,
      },
      isCompatible: cached.reconciliation.isCompatible && warnings.length === 0,
      compatibilityWarnings: warnings,
    };
  }

  /**
   * Create empty result for empty row groups.
   */
  private createEmptyResult<T>(): ReadResult<T> {
    return {
      documents: [],
      schemaMapping: {
        missingColumns: [],
        removedColumns: [],
        widenedColumns: [],
        columnsWithDefaults: [],
      },
      isCompatible: true,
      compatibilityWarnings: [],
    };
  }

  /**
   * Get cached mapping or compute and cache it.
   */
  private getCachedMapping(sourceSchema: Record<string, SchemaField>): CachedMapping {
    const sourceFingerprint = generateSchemaFingerprint(sourceSchema);
    const cacheKey = `${sourceFingerprint}->${this.targetSchemaFingerprint}`;

    if (this.enableCache) {
      const cached = this.mappingCache.get(cacheKey);
      if (cached) {
        return cached;
      }
    }

    // Compute mapping
    const reconciliation = reconcileSchemas(sourceSchema, this.targetSchema);
    const mappings = createColumnMapping(sourceSchema, this.targetSchema);

    const result: CachedMapping = {
      mappings,
      reconciliation,
      targetColumns: this.precomputedTargetColumns,
    };

    // Cache result
    if (this.enableCache) {
      this.mappingCache.set(cacheKey, result);
    }

    return result;
  }

  /**
   * Validate reconciliation and collect warnings.
   */
  private validateAndCollectWarnings(reconciliation: SchemaReconciliation): CompatibilityWarning[] {
    const warnings: CompatibilityWarning[] = [];

    // Check for removed columns
    for (const column of reconciliation.removedColumns) {
      if (this.strictMode) {
        throw new Error(`Removed column '${column}' detected in strict mode`);
      }
      warnings.push({
        type: 'removed_column',
        column,
        message: `Column '${column}' exists in source but not in target schema`,
      });
    }

    // Check for type narrowing
    for (const typeChange of reconciliation.typeChanges) {
      if (!typeChange.isWidening) {
        if (this.strictMode) {
          throw new Error(
            `Type narrowing detected for column '${typeChange.column}': ${typeChange.fromType} -> ${typeChange.toType}`
          );
        }
        warnings.push({
          type: 'type_narrowing',
          column: typeChange.column,
          message: `Column '${typeChange.column}' type narrowed from ${typeChange.fromType} to ${typeChange.toType}`,
        });
      }
    }

    return warnings;
  }

  /**
   * Transform documents according to schema mapping.
   * Optimized for batch processing.
   */
  private transformDocuments<T extends Record<string, unknown>>(
    rawDocuments: Record<string, unknown>[],
    cached: CachedMapping,
    _sourceSchema: Record<string, SchemaField>
  ): { transformed: T[]; columnsWithDefaults: string[] } {
    const documents: T[] = [];
    const columnsWithDefaultsSet = new Set<string>();

    // Pre-extract mapping data for hot path
    const mappings = cached.mappings;
    const targetColumns = cached.targetColumns;

    for (const rawDoc of rawDocuments) {
      const transformedDoc: Record<string, unknown> = {};

      // Process target schema columns
      for (const [columnName] of targetColumns) {
        const mapping = mappings.get(columnName);

        if (mapping && mapping.sourceColumn !== null) {
          // Column exists in source
          let value = rawDoc[mapping.sourceColumn];

          // Apply type transformation if needed
          if (value !== undefined && value !== null && mapping.transform) {
            value = mapping.transform.apply(value);
          }

          if (value !== undefined && value !== null) {
            transformedDoc[columnName] = value;
          } else if (mapping.useDefault) {
            const defaultValue = this.getDefaultValue(columnName, transformedDoc);
            if (defaultValue !== undefined) {
              transformedDoc[columnName] = defaultValue;
              columnsWithDefaultsSet.add(columnName);
            }
          }
        } else {
          // Column doesn't exist in source - apply default
          const defaultValue = this.getDefaultValue(columnName, { ...transformedDoc, ...rawDoc });
          if (defaultValue !== undefined) {
            transformedDoc[columnName] = defaultValue;
            columnsWithDefaultsSet.add(columnName);
          }
        }
      }

      // Preserve removed columns if requested
      if (this.preserveRemovedColumns) {
        for (const [key, value] of Object.entries(rawDoc)) {
          if (!(key in transformedDoc) && value !== undefined && value !== null) {
            transformedDoc[key] = value;
          }
        }
      }

      documents.push(transformedDoc as T);
    }

    return {
      transformed: documents,
      columnsWithDefaults: Array.from(columnsWithDefaultsSet),
    };
  }

  /**
   * Build widened columns list from reconciliation.
   */
  private buildWidenedColumnsList(reconciliation: SchemaReconciliation): WidenedColumn[] {
    return reconciliation.typeChanges
      .filter((tc) => tc.isWidening)
      .map((tc) => ({
        column: tc.column,
        fromType: tc.fromType,
        toType: tc.toType,
      }));
  }

  /**
   * Get default value for a column.
   */
  private getDefaultValue(
    columnName: string,
    doc: Record<string, unknown>
  ): unknown {
    // Check configured defaults first
    const configuredDefault = this.defaults[columnName];
    if (configuredDefault !== undefined) {
      if (typeof configuredDefault === 'function') {
        return configuredDefault(doc);
      }
      return configuredDefault;
    }

    // Check schema defaults if enabled
    if (this.useSchemaDefaults) {
      const field = this.targetSchema[columnName] as SchemaField & { default?: unknown };
      if (field?.default !== undefined) {
        return field.default;
      }
    }

    return undefined;
  }

  /**
   * Infer source schema from serialized data.
   */
  private inferSourceSchema(serialized: SerializedRowGroup): Record<string, SchemaField> {
    const schema: Record<string, SchemaField> = {};

    for (const column of serialized.metadata.columns) {
      schema[column.name] = {
        type: column.type,
        required: false, // Cannot infer required from data
      };
    }

    return schema;
  }
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Reconcile two schemas and identify differences.
 *
 * Uses Set-based lookups for O(1) column membership checks.
 *
 * @param sourceSchema - The source (old) schema
 * @param targetSchema - The target (new) schema
 * @returns Reconciliation result with added, removed, and changed columns
 */
export function reconcileSchemas(
  sourceSchema: Record<string, SchemaField>,
  targetSchema: Record<string, SchemaField>
): SchemaReconciliation {
  const sourceColumns = new Set(Object.keys(sourceSchema));
  const targetColumns = new Set(Object.keys(targetSchema));

  // Find added columns (in target but not in source)
  const addedColumns: string[] = [];
  for (const column of targetColumns) {
    if (!sourceColumns.has(column)) {
      addedColumns.push(column);
    }
  }

  // Find removed columns (in source but not in target)
  const removedColumns: string[] = [];
  for (const column of sourceColumns) {
    if (!targetColumns.has(column)) {
      removedColumns.push(column);
    }
  }

  // Find type changes
  const typeChanges: TypeChange[] = [];
  for (const column of sourceColumns) {
    if (!targetColumns.has(column)) {
      continue;
    }

    const sourceField = sourceSchema[column]!;
    const targetField = targetSchema[column]!;

    if (sourceField.type !== targetField.type) {
      const isWidening = isTypeWideningAllowed(
        sourceField.type as string,
        targetField.type as string
      );

      typeChanges.push({
        column,
        fromType: sourceField.type as string,
        toType: targetField.type as string,
        isWidening,
      });
    }
  }

  // Determine overall compatibility
  // Compatible if: no removed columns and no type narrowing
  const hasTypeNarrowing = typeChanges.some((tc) => !tc.isWidening);
  const isCompatible = removedColumns.length === 0 && !hasTypeNarrowing;

  return {
    addedColumns,
    removedColumns,
    typeChanges,
    isCompatible,
  };
}

/**
 * Create column mappings between source and target schemas.
 *
 * Uses the type transform registry for efficient transform lookup.
 *
 * @param sourceSchema - The source schema
 * @param targetSchema - The target schema
 * @returns Map of target column name to column mapping
 */
export function createColumnMapping(
  sourceSchema: Record<string, SchemaField>,
  targetSchema: Record<string, SchemaField>
): Map<string, ColumnMapping> {
  const mappings = new Map<string, ColumnMapping>();
  const sourceColumns = new Set(Object.keys(sourceSchema));

  for (const [columnName, targetField] of Object.entries(targetSchema)) {
    if (sourceColumns.has(columnName)) {
      // Column exists in source
      const sourceField = sourceSchema[columnName]!;
      let transform: TypeTransform | null = null;

      // Check if type transformation is needed
      if (sourceField.type !== targetField.type) {
        transform = getTypeTransform(
          sourceField.type as string,
          targetField.type as string
        );
      }

      mappings.set(columnName, {
        sourceColumn: columnName,
        targetColumn: columnName,
        transform,
      });
    } else {
      // Column doesn't exist in source - new field
      mappings.set(columnName, {
        sourceColumn: null,
        targetColumn: columnName,
        transform: null,
        useDefault: true,
      });
    }
  }

  return mappings;
}

/**
 * Get a type transformation from the registry or create one.
 */
function getTypeTransform(fromType: string, toType: string): TypeTransform | null {
  // Check registry first
  const registryKey = `${fromType}->${toType}`;
  const registered = TYPE_TRANSFORM_REGISTRY.get(registryKey);
  if (registered) {
    return registered;
  }

  // Handle any -> variant widening
  if (toType === 'variant') {
    return {
      name: `widen_${fromType}_to_variant`,
      apply: (value: unknown) => value, // Variant accepts any type
    };
  }

  return null;
}

/**
 * Apply default values to a document.
 *
 * Only applies defaults for undefined fields, preserving existing values.
 *
 * @param doc - The document to apply defaults to
 * @param defaults - Default value definitions
 * @returns Document with defaults applied
 */
export function applyDefaults<T extends Record<string, unknown>>(
  doc: T,
  defaults: Record<string, DefaultValue>
): T {
  const result: Record<string, unknown> = { ...doc };

  for (const [key, defaultValue] of Object.entries(defaults)) {
    // Only apply default if value is undefined
    if (result[key] === undefined) {
      if (typeof defaultValue === 'function') {
        result[key] = defaultValue(result);
      } else {
        result[key] = defaultValue;
      }
    }
  }

  return result as T;
}

// ============================================================================
// Batch Processing Utilities
// ============================================================================

/**
 * Options for batch compat reading
 */
export interface BatchReadOptions {
  /** Chunk size for processing (default: 1000) */
  chunkSize?: number;
  /** Progress callback */
  onProgress?: (processed: number, total: number) => void;
}

/**
 * Read multiple row groups with the same compat reader.
 *
 * Optimized for processing multiple files with the same schema pair.
 *
 * @param reader - The compat reader instance
 * @param serializedGroups - Array of serialized row groups
 * @param options - Batch read options
 * @returns Combined read result
 */
export function batchRead<T extends Record<string, unknown>>(
  reader: CompatReader,
  serializedGroups: SerializedRowGroup[],
  options: BatchReadOptions = {}
): ReadResult<T> {
  const allDocuments: T[] = [];
  let firstMapping: SchemaMapping | null = null;
  let isCompatible = true;
  const allWarnings: CompatibilityWarning[] = [];

  const total = serializedGroups.length;

  for (let i = 0; i < serializedGroups.length; i++) {
    const result = reader.read<T>(serializedGroups[i]!);

    allDocuments.push(...result.documents);

    if (!firstMapping) {
      firstMapping = result.schemaMapping;
    }

    if (!result.isCompatible) {
      isCompatible = false;
    }

    // Collect unique warnings
    for (const warning of result.compatibilityWarnings) {
      const exists = allWarnings.some(
        (w) => w.type === warning.type && w.column === warning.column
      );
      if (!exists) {
        allWarnings.push(warning);
      }
    }

    // Report progress
    if (options.onProgress) {
      options.onProgress(i + 1, total);
    }
  }

  return {
    documents: allDocuments,
    schemaMapping: firstMapping ?? {
      missingColumns: [],
      removedColumns: [],
      widenedColumns: [],
      columnsWithDefaults: [],
    },
    isCompatible,
    compatibilityWarnings: allWarnings,
  };
}
