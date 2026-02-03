/**
 * Schema Promoter
 *
 * Handles automatic type promotion and schema migration for evolving schemas.
 * When new documents contain fields with different types than the existing schema,
 * this module detects the need for promotion and generates migration strategies.
 *
 * Key features:
 * - Detect when a field needs type promotion
 * - Generate schema migration plans
 * - Handle backward compatibility
 * - Support type widening (int32 -> int64 -> double)
 *
 * @module schema/promoter
 */

import type { ParquetType } from '../types.js';
import type { ParsedColumn, ParsedCollectionSchema } from './config.js';
import type { DetectedType } from './analyzer.js';

// ============================================================================
// Type Definitions
// ============================================================================

/**
 * Represents a type promotion from one type to another
 */
export interface TypePromotion {
  /** Field path (e.g., 'user.age') */
  field: string;
  /** Original type */
  from: ParquetType;
  /** Target type after promotion */
  to: ParquetType;
  /** Whether this promotion is safe (lossless) */
  isSafe: boolean;
  /** Human-readable reason for the promotion */
  reason: string;
}

/**
 * Schema migration plan containing all promotions
 */
export interface SchemaMigration {
  /** Schema version this migration upgrades from */
  fromVersion: number;
  /** Schema version this migration upgrades to */
  toVersion: number;
  /** List of type promotions in this migration */
  promotions: TypePromotion[];
  /** New fields added in this schema version */
  newFields: NewFieldDefinition[];
  /** Fields removed in this schema version */
  removedFields: string[];
  /** Whether the migration is backward compatible */
  isBackwardCompatible: boolean;
  /** Timestamp when migration was created */
  createdAt: Date;
}

/**
 * Definition for a new field being added to the schema
 */
export interface NewFieldDefinition {
  /** Field path */
  path: string;
  /** Parquet type for the field */
  type: ParquetType;
  /** Whether the field is optional (nullable) */
  isOptional: boolean;
  /** Default value for the field when reading old data */
  defaultValue?: unknown;
}

/**
 * Schema with version information
 */
export interface VersionedSchema {
  /** Schema version number */
  version: number;
  /** Column definitions */
  columns: Map<string, ParsedColumn>;
  /** Creation timestamp */
  createdAt: Date;
  /** Previous schema version (for tracking lineage) */
  previousVersion?: number;
}

/**
 * Result of schema comparison
 */
export interface SchemaComparison {
  /** Whether schemas are compatible */
  compatible: boolean;
  /** Required promotions to make schemas compatible */
  promotions: TypePromotion[];
  /** Fields that exist in new but not in old */
  addedFields: string[];
  /** Fields that exist in old but not in new */
  removedFields: string[];
  /** Fields with type conflicts that cannot be automatically resolved */
  conflicts: TypeConflict[];
}

/**
 * Represents a type conflict that cannot be automatically resolved
 */
export interface TypeConflict {
  /** Field path */
  field: string;
  /** Type in existing schema */
  existingType: ParquetType;
  /** Type in new data */
  newType: ParquetType;
  /** Reason why automatic promotion is not possible */
  reason: string;
}

// ============================================================================
// Type Promotion Rules
// ============================================================================

/**
 * Type promotion hierarchy - types can be safely promoted upward in this list.
 * Lower indices can be promoted to higher indices without data loss.
 */
const NUMERIC_PROMOTION_HIERARCHY: ParquetType[] = [
  'int32',
  'int64',
  'float',
  'double',
];

/**
 * Map of safe type promotions: from -> [valid targets]
 * Each type maps to all types it can be safely promoted to.
 */
const SAFE_PROMOTIONS: Map<ParquetType, ParquetType[]> = new Map([
  ['int32', ['int64', 'float', 'double', 'variant']],
  ['int64', ['double', 'variant']], // float loses precision for large int64
  ['float', ['double', 'variant']],
  ['double', ['variant']],
  ['boolean', ['variant']],
  ['string', ['variant']],
  ['date', ['timestamp', 'variant']],
  ['timestamp', ['variant']],
  ['binary', ['variant']],
]);

/**
 * Maps detected types from analyzer to Parquet types
 */
const DETECTED_TO_PARQUET: Map<DetectedType, ParquetType> = new Map([
  ['string', 'string'],
  ['number', 'double'], // JavaScript numbers are doubles
  ['boolean', 'boolean'],
  ['date', 'timestamp'],
  ['binary', 'binary'],
  ['null', 'variant'],
  ['array', 'variant'],
  ['object', 'variant'],
  ['objectId', 'string'],
  ['mixed', 'variant'],
]);

// ============================================================================
// Type Promotion Functions
// ============================================================================

/**
 * Check if a type can be safely promoted to another type
 *
 * @param from - Source type
 * @param to - Target type
 * @returns True if promotion is safe (lossless)
 */
export function canPromoteSafely(from: ParquetType, to: ParquetType): boolean {
  if (from === to) {
    return true;
  }

  const validTargets = SAFE_PROMOTIONS.get(from);
  return validTargets?.includes(to) ?? false;
}

/**
 * Get the promoted type when combining two types.
 * Returns the wider type that can safely represent both.
 *
 * @param type1 - First type
 * @param type2 - Second type
 * @returns The promoted type, or 'variant' if types are incompatible
 */
export function getPromotedType(type1: ParquetType, type2: ParquetType): ParquetType {
  // Same type - no promotion needed
  if (type1 === type2) {
    return type1;
  }

  // Variant absorbs everything
  if (type1 === 'variant' || type2 === 'variant') {
    return 'variant';
  }

  // Check numeric promotion hierarchy
  const idx1 = NUMERIC_PROMOTION_HIERARCHY.indexOf(type1);
  const idx2 = NUMERIC_PROMOTION_HIERARCHY.indexOf(type2);

  if (idx1 >= 0 && idx2 >= 0) {
    // Both are numeric types - use the wider one
    return NUMERIC_PROMOTION_HIERARCHY[Math.max(idx1, idx2)]!;
  }

  // Check if one can be safely promoted to the other
  if (canPromoteSafely(type1, type2)) {
    return type2;
  }
  if (canPromoteSafely(type2, type1)) {
    return type1;
  }

  // Incompatible types - fall back to variant
  return 'variant';
}

/**
 * Convert detected type to Parquet type
 *
 * @param detected - Detected type from analyzer
 * @returns Corresponding Parquet type
 */
export function detectedTypeToParquet(detected: DetectedType): ParquetType {
  return DETECTED_TO_PARQUET.get(detected) ?? 'variant';
}

// ============================================================================
// Schema Promotion Detection
// ============================================================================

/**
 * Detect promotions needed when a new document doesn't match existing schema.
 *
 * @example
 * ```typescript
 * const existingSchema = { count: 'int32' };
 * const newDocument = { count: 9007199254740993n }; // BigInt > MAX_SAFE_INTEGER
 * const promotion = detectPromotion(existingSchema, newDocument);
 * // { field: 'count', from: 'int32', to: 'int64' }
 * ```
 *
 * @param existingSchema - Current schema columns
 * @param document - New document to check
 * @returns Array of required type promotions
 */
export function detectPromotion(
  existingSchema: ParsedCollectionSchema | Map<string, ParsedColumn>,
  document: Record<string, unknown>
): TypePromotion[] {
  const promotions: TypePromotion[] = [];

  // Get column map
  const columns = existingSchema instanceof Map
    ? existingSchema
    : existingSchema.columnMap;

  // Check each field in the document
  for (const [path, value] of flattenDocument(document)) {
    const column = columns.get(path);

    if (!column) {
      // New field - not a promotion, skip
      continue;
    }

    const detectedType = detectValueType(value);
    const parquetType = detectedTypeToParquet(detectedType);

    // Check if types match
    if (column.type !== parquetType && value !== null && value !== undefined) {
      const promotedType = getPromotedType(column.type, parquetType);

      if (promotedType !== column.type) {
        const isSafe = canPromoteSafely(column.type, promotedType);

        promotions.push({
          field: path,
          from: column.type,
          to: promotedType,
          isSafe,
          reason: isSafe
            ? `Field '${path}' needs widening from ${column.type} to ${promotedType}`
            : `Field '${path}' has incompatible types, promoting to ${promotedType}`,
        });
      }
    }
  }

  return promotions;
}

/**
 * Compare two schemas and determine required promotions for compatibility
 *
 * @param oldSchema - Existing schema
 * @param newSchema - New schema to compare against
 * @returns Comparison result with promotions and conflicts
 */
export function compareSchemas(
  oldSchema: ParsedCollectionSchema | Map<string, ParsedColumn>,
  newSchema: ParsedCollectionSchema | Map<string, ParsedColumn>
): SchemaComparison {
  const oldColumns = oldSchema instanceof Map ? oldSchema : oldSchema.columnMap;
  const newColumns = newSchema instanceof Map ? newSchema : newSchema.columnMap;

  const promotions: TypePromotion[] = [];
  const conflicts: TypeConflict[] = [];
  const addedFields: string[] = [];
  const removedFields: string[] = [];

  // Check fields in new schema
  for (const [path, newColumn] of newColumns) {
    const oldColumn = oldColumns.get(path);

    if (!oldColumn) {
      // New field
      addedFields.push(path);
      continue;
    }

    // Compare types
    if (oldColumn.type !== newColumn.type) {
      const promotedType = getPromotedType(oldColumn.type, newColumn.type);
      const isSafe = canPromoteSafely(oldColumn.type, promotedType);

      if (promotedType === 'variant' && oldColumn.type !== 'variant' && newColumn.type !== 'variant') {
        // Types are incompatible - record as conflict
        conflicts.push({
          field: path,
          existingType: oldColumn.type,
          newType: newColumn.type,
          reason: `Cannot safely promote ${oldColumn.type} to ${newColumn.type}`,
        });
      } else {
        // Promotion is possible
        promotions.push({
          field: path,
          from: oldColumn.type,
          to: promotedType,
          isSafe,
          reason: `Type changed from ${oldColumn.type} to ${newColumn.type}`,
        });
      }
    }
  }

  // Check for removed fields
  for (const path of oldColumns.keys()) {
    if (!newColumns.has(path)) {
      removedFields.push(path);
    }
  }

  // Schema is compatible if there are no conflicts and all promotions are safe
  const compatible = conflicts.length === 0 && promotions.every(p => p.isSafe);

  return {
    compatible,
    promotions,
    addedFields,
    removedFields,
    conflicts,
  };
}

// ============================================================================
// Schema Migration Generation
// ============================================================================

/**
 * Generate a schema migration from comparison results
 *
 * @param comparison - Schema comparison result
 * @param fromVersion - Source schema version
 * @param toVersion - Target schema version
 * @param newSchema - New schema for field definitions
 * @returns Schema migration plan
 */
export function generateMigration(
  comparison: SchemaComparison,
  fromVersion: number,
  toVersion: number,
  newSchema?: ParsedCollectionSchema | Map<string, ParsedColumn>
): SchemaMigration {
  const newFields: NewFieldDefinition[] = [];

  // Generate definitions for new fields
  if (newSchema) {
    const columns = newSchema instanceof Map ? newSchema : newSchema.columnMap;

    for (const fieldPath of comparison.addedFields) {
      const column = columns.get(fieldPath);
      if (column) {
        newFields.push({
          path: fieldPath,
          type: column.type,
          isOptional: true, // New fields are always optional for backward compatibility
          defaultValue: null,
        });
      }
    }
  }

  return {
    fromVersion,
    toVersion,
    promotions: comparison.promotions,
    newFields,
    removedFields: comparison.removedFields,
    isBackwardCompatible: comparison.compatible && comparison.removedFields.length === 0,
    createdAt: new Date(),
  };
}

/**
 * Apply a migration to a schema, producing a new schema version
 *
 * @param schema - Current schema
 * @param migration - Migration to apply
 * @returns New schema with migration applied
 */
export function applyMigration(
  schema: VersionedSchema,
  migration: SchemaMigration
): VersionedSchema {
  // Clone the column map
  const newColumns = new Map(schema.columns);

  // Apply promotions
  for (const promotion of migration.promotions) {
    const column = newColumns.get(promotion.field);
    if (column) {
      newColumns.set(promotion.field, {
        ...column,
        type: promotion.to,
      });
    }
  }

  // Add new fields
  for (const newField of migration.newFields) {
    newColumns.set(newField.path, {
      path: newField.path,
      segments: newField.path.split('.'),
      type: newField.type,
      isArray: false,
      isStruct: false,
    });
  }

  // Remove fields (mark them, but keep for backward compatibility)
  for (const removedPath of migration.removedFields) {
    newColumns.delete(removedPath);
  }

  return {
    version: migration.toVersion,
    columns: newColumns,
    createdAt: migration.createdAt,
    previousVersion: migration.fromVersion,
  };
}

// ============================================================================
// Value Type Detection
// ============================================================================

/**
 * Detect the type of a JavaScript value
 *
 * @param value - Value to detect type of
 * @returns Detected type
 */
export function detectValueType(value: unknown): DetectedType {
  if (value === null) {
    return 'null';
  }

  if (value === undefined) {
    return 'null';
  }

  if (typeof value === 'string') {
    return 'string';
  }

  if (typeof value === 'number') {
    return 'number';
  }

  if (typeof value === 'bigint') {
    return 'number'; // BigInt is still numeric
  }

  if (typeof value === 'boolean') {
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

  // Check for ObjectId-like
  if (isObjectIdLike(value)) {
    return 'objectId';
  }

  if (typeof value === 'object') {
    return 'object';
  }

  return 'mixed';
}

/**
 * Check if a value looks like a MongoDB ObjectId
 */
function isObjectIdLike(value: unknown): boolean {
  if (typeof value !== 'object' || value === null) {
    return false;
  }

  const proto = Object.getPrototypeOf(value);
  if (proto && proto.constructor && proto.constructor.name === 'ObjectId') {
    return true;
  }

  if (
    typeof (value as { toString?: () => string }).toString === 'function' &&
    typeof (value as { toHexString?: () => string }).toHexString === 'function'
  ) {
    const str = (value as { toString: () => string }).toString();
    return /^[0-9a-fA-F]{24}$/.test(str);
  }

  return false;
}

// ============================================================================
// Document Flattening Utility
// ============================================================================

/**
 * Flatten a nested document into dot-notation paths
 *
 * @param obj - Object to flatten
 * @param prefix - Current path prefix
 * @param maxDepth - Maximum nesting depth
 * @yields [path, value] pairs
 */
export function* flattenDocument(
  obj: Record<string, unknown>,
  prefix: string = '',
  maxDepth: number = 10
): Generator<[string, unknown]> {
  if (maxDepth <= 0) {
    return;
  }

  for (const [key, value] of Object.entries(obj)) {
    const path = prefix ? `${prefix}.${key}` : key;

    yield [path, value];

    // Recurse into plain objects
    if (
      value !== null &&
      typeof value === 'object' &&
      !Array.isArray(value) &&
      !(value instanceof Date) &&
      !(value instanceof Uint8Array) &&
      !isObjectIdLike(value)
    ) {
      yield* flattenDocument(value as Record<string, unknown>, path, maxDepth - 1);
    }
  }
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Check if a type requires numeric value coercion
 *
 * @param type - Type to check
 * @returns True if type is numeric
 */
export function isNumericType(type: ParquetType): boolean {
  return ['int32', 'int64', 'float', 'double'].includes(type);
}

/**
 * Get the numeric precision of a type (higher = more precise/wider range)
 *
 * @param type - Type to check
 * @returns Precision level (0-3) or -1 if not numeric
 */
export function getNumericPrecision(type: ParquetType): number {
  return NUMERIC_PROMOTION_HIERARCHY.indexOf(type);
}

/**
 * Determine if a migration is safe to apply automatically
 *
 * @param migration - Migration to check
 * @returns True if all changes are safe
 */
export function isSafeMigration(migration: SchemaMigration): boolean {
  return (
    migration.isBackwardCompatible &&
    migration.promotions.every(p => p.isSafe) &&
    migration.removedFields.length === 0
  );
}

/**
 * Create a versioned schema from a parsed collection schema
 *
 * @param schema - Parsed collection schema
 * @param version - Version number
 * @returns Versioned schema
 */
export function createVersionedSchema(
  schema: ParsedCollectionSchema,
  version: number = 1
): VersionedSchema {
  return {
    version,
    columns: schema.columnMap,
    createdAt: new Date(),
  };
}

// ============================================================================
// Schema History Tracking
// ============================================================================

/**
 * Schema history entry recording a schema version and its migration
 */
export interface SchemaHistoryEntry {
  /** Schema version at this point in history */
  schema: VersionedSchema;
  /** Migration that led to this version (null for initial version) */
  migration: SchemaMigration | null;
}

/**
 * Serializable schema history for persistence
 */
export interface SerializedSchemaHistory {
  /** Collection name */
  collectionName: string;
  /** Current schema version */
  currentVersion: number;
  /** All schema versions in history */
  versions: Array<{
    version: number;
    columns: Array<{
      path: string;
      type: ParquetType;
      isArray: boolean;
      isStruct: boolean;
    }>;
    createdAt: string;
    previousVersion?: number;
  }>;
  /** All migrations between versions */
  migrations: Array<{
    fromVersion: number;
    toVersion: number;
    promotions: TypePromotion[];
    newFields: NewFieldDefinition[];
    removedFields: string[];
    isBackwardCompatible: boolean;
    createdAt: string;
  }>;
}

/**
 * Tracks schema versions and migrations over time.
 *
 * SchemaHistory maintains a complete record of all schema changes,
 * enabling:
 * - Schema evolution auditing
 * - Backward compatibility checking
 * - Rollback to previous versions
 * - Migration path computation
 *
 * @example
 * ```typescript
 * const history = new SchemaHistory('users');
 *
 * // Initialize with first schema
 * const initialSchema = createVersionedSchema(parsedSchema, 1);
 * history.addVersion(initialSchema);
 *
 * // Later, when promoting a field
 * const migration = generateMigration(comparison, 1, 2, newSchema);
 * const newVersionedSchema = applyMigration(initialSchema, migration);
 * history.addVersion(newVersionedSchema, migration);
 *
 * // Query history
 * console.log(history.getCurrentVersion()); // 2
 * console.log(history.getMigrationPath(1, 2)); // [migration]
 * ```
 */
export class SchemaHistory {
  private collectionName: string;
  private versions: Map<number, VersionedSchema> = new Map();
  private migrations: Map<string, SchemaMigration> = new Map();
  private currentVersion: number = 0;

  constructor(collectionName: string) {
    this.collectionName = collectionName;
  }

  /**
   * Get the collection name
   */
  getCollectionName(): string {
    return this.collectionName;
  }

  /**
   * Get the current schema version number
   */
  getCurrentVersion(): number {
    return this.currentVersion;
  }

  /**
   * Get the current schema
   */
  getCurrentSchema(): VersionedSchema | undefined {
    return this.versions.get(this.currentVersion);
  }

  /**
   * Get a specific schema version
   */
  getVersion(version: number): VersionedSchema | undefined {
    return this.versions.get(version);
  }

  /**
   * Get all version numbers in order
   */
  getAllVersions(): number[] {
    return Array.from(this.versions.keys()).sort((a, b) => a - b);
  }

  /**
   * Add a new schema version to history
   *
   * @param schema - The new versioned schema
   * @param migration - The migration that produced this version (null for initial)
   */
  addVersion(schema: VersionedSchema, migration: SchemaMigration | null = null): void {
    // Validate version sequence
    if (migration) {
      if (migration.fromVersion !== this.currentVersion) {
        throw new Error(
          `Migration fromVersion (${migration.fromVersion}) must match current version (${this.currentVersion})`
        );
      }
      if (migration.toVersion !== schema.version) {
        throw new Error(
          `Migration toVersion (${migration.toVersion}) must match schema version (${schema.version})`
        );
      }
    } else if (this.versions.size > 0) {
      // Non-initial version must have a migration
      throw new Error('Non-initial schema version must have a migration');
    }

    // Store the version
    this.versions.set(schema.version, schema);
    this.currentVersion = schema.version;

    // Store the migration
    if (migration) {
      const migrationKey = `${migration.fromVersion}->${migration.toVersion}`;
      this.migrations.set(migrationKey, migration);
    }
  }

  /**
   * Get a migration between two versions
   */
  getMigration(fromVersion: number, toVersion: number): SchemaMigration | undefined {
    return this.migrations.get(`${fromVersion}->${toVersion}`);
  }

  /**
   * Get the full migration path between two versions
   *
   * @param fromVersion - Starting version
   * @param toVersion - Target version
   * @returns Array of migrations to apply in order, or null if no path exists
   */
  getMigrationPath(fromVersion: number, toVersion: number): SchemaMigration[] | null {
    if (fromVersion === toVersion) {
      return [];
    }

    if (fromVersion > toVersion) {
      // Downgrade not supported
      return null;
    }

    const path: SchemaMigration[] = [];
    let current = fromVersion;

    while (current < toVersion) {
      const nextVersion = current + 1;
      const migration = this.getMigration(current, nextVersion);

      if (!migration) {
        // No direct path, check if there's a skip migration
        const skipMigration = this.migrations.get(`${current}->${toVersion}`);
        if (skipMigration) {
          path.push(skipMigration);
          return path;
        }
        return null;
      }

      path.push(migration);
      current = nextVersion;
    }

    return path;
  }

  /**
   * Check if upgrading from one version to another is safe
   */
  isSafeUpgrade(fromVersion: number, toVersion: number): boolean {
    const path = this.getMigrationPath(fromVersion, toVersion);
    if (!path) {
      return false;
    }
    return path.every(m => isSafeMigration(m));
  }

  /**
   * Get history of a specific field across all versions
   */
  getFieldHistory(fieldPath: string): Array<{ version: number; type: ParquetType | null }> {
    const history: Array<{ version: number; type: ParquetType | null }> = [];

    for (const version of this.getAllVersions()) {
      const schema = this.versions.get(version)!;
      const column = schema.columns.get(fieldPath);
      history.push({
        version,
        type: column?.type ?? null,
      });
    }

    return history;
  }

  /**
   * Serialize the history for persistence
   */
  serialize(): SerializedSchemaHistory {
    const versions: SerializedSchemaHistory['versions'] = [];

    for (const [version, schema] of this.versions) {
      const columns: SerializedSchemaHistory['versions'][0]['columns'] = [];

      for (const [path, column] of schema.columns) {
        columns.push({
          path,
          type: column.type,
          isArray: column.isArray,
          isStruct: column.isStruct,
        });
      }

      versions.push({
        version,
        columns,
        createdAt: schema.createdAt.toISOString(),
        previousVersion: schema.previousVersion,
      });
    }

    const migrations: SerializedSchemaHistory['migrations'] = [];

    for (const migration of this.migrations.values()) {
      migrations.push({
        fromVersion: migration.fromVersion,
        toVersion: migration.toVersion,
        promotions: migration.promotions,
        newFields: migration.newFields,
        removedFields: migration.removedFields,
        isBackwardCompatible: migration.isBackwardCompatible,
        createdAt: migration.createdAt.toISOString(),
      });
    }

    return {
      collectionName: this.collectionName,
      currentVersion: this.currentVersion,
      versions,
      migrations,
    };
  }

  /**
   * Deserialize history from stored format
   */
  static deserialize(data: SerializedSchemaHistory): SchemaHistory {
    const history = new SchemaHistory(data.collectionName);

    // Restore versions
    for (const versionData of data.versions) {
      const columns = new Map<string, ParsedColumn>();

      for (const col of versionData.columns) {
        columns.set(col.path, {
          path: col.path,
          segments: col.path.split('.'),
          type: col.type,
          isArray: col.isArray,
          isStruct: col.isStruct,
        });
      }

      const schema: VersionedSchema = {
        version: versionData.version,
        columns,
        createdAt: new Date(versionData.createdAt),
        previousVersion: versionData.previousVersion,
      };

      history.versions.set(versionData.version, schema);
    }

    // Restore migrations
    for (const migrationData of data.migrations) {
      const migration: SchemaMigration = {
        fromVersion: migrationData.fromVersion,
        toVersion: migrationData.toVersion,
        promotions: migrationData.promotions,
        newFields: migrationData.newFields,
        removedFields: migrationData.removedFields,
        isBackwardCompatible: migrationData.isBackwardCompatible,
        createdAt: new Date(migrationData.createdAt),
      };

      const key = `${migration.fromVersion}->${migration.toVersion}`;
      history.migrations.set(key, migration);
    }

    history.currentVersion = data.currentVersion;

    return history;
  }

  /**
   * Get a summary of all promotions that have occurred
   */
  getPromotionSummary(): Map<string, TypePromotion[]> {
    const summary = new Map<string, TypePromotion[]>();

    for (const migration of this.migrations.values()) {
      for (const promotion of migration.promotions) {
        if (!summary.has(promotion.field)) {
          summary.set(promotion.field, []);
        }
        summary.get(promotion.field)!.push(promotion);
      }
    }

    return summary;
  }
}
