/**
 * Schema Migration Module
 *
 * Provides tools for defining and executing schema migrations on document data.
 * Supports forward migrations (up) and rollback migrations (down).
 *
 * Key features:
 * - MigrationDefinition interface for declarative migrations
 * - MigrationRunner for applying/rolling back migrations
 * - Built-in migration operations (add field, rename field, remove field, change type)
 * - Schema version tracking in metadata
 * - Type upcasting support with configurable converters
 *
 * @example
 * ```typescript
 * import { MigrationRunner, addField, renameField, changeFieldType } from './migration.js';
 *
 * const runner = new MigrationRunner('users');
 *
 * // Register migrations
 * runner.register({
 *   version: 1,
 *   description: 'Add email field',
 *   up: addField('email', null),
 *   down: removeField('email'),
 * });
 *
 * runner.register({
 *   version: 2,
 *   description: 'Rename name to fullName',
 *   up: renameField('name', 'fullName'),
 *   down: renameField('fullName', 'name'),
 * });
 *
 * // Apply migrations
 * const doc = { _id: '1', name: 'Alice' };
 * const migrated = runner.migrateDocument(doc, 0, 2);
 * // Result: { _id: '1', fullName: 'Alice', email: null, _schemaVersion: 2 }
 * ```
 *
 * @module schema/migration
 */

import type { ParquetType } from '../types.js';

// ============================================================================
// Type Definitions
// ============================================================================

/**
 * Document type for migration operations
 */
export type MigratableDocument = Record<string, unknown>;

/**
 * Function that transforms a document during migration
 */
export type MigrationTransform = (doc: MigratableDocument) => MigratableDocument;

/**
 * Definition of a single migration
 */
export interface MigrationDefinition {
  /** Migration version number (must be unique and sequential) */
  version: number;

  /** Human-readable description of the migration */
  description: string;

  /** Function to apply the migration (upgrade) */
  up: MigrationTransform;

  /** Function to reverse the migration (downgrade) */
  down: MigrationTransform;

  /** Optional metadata for the migration */
  metadata?: {
    /** Author of the migration */
    author?: string;
    /** Creation timestamp */
    createdAt?: Date;
    /** Related ticket or issue */
    ticket?: string;
    /** Additional notes */
    notes?: string;
  };
}

/**
 * Options for type conversion during field type changes
 */
export interface TypeConversionOptions {
  /** How to handle conversion failures */
  onError: 'throw' | 'skip' | 'default';
  /** Default value when onError is 'default' */
  defaultValue?: unknown;
  /** Custom conversion function */
  converter?: (value: unknown) => unknown;
}

/**
 * Result of a migration operation
 */
export interface MigrationResult {
  /** Original document version */
  fromVersion: number;
  /** Target document version */
  toVersion: number;
  /** Number of migrations applied */
  migrationsApplied: number;
  /** Whether the migration was successful */
  success: boolean;
  /** Error message if failed */
  error?: string;
  /** The migrated document */
  document: MigratableDocument;
}

/**
 * Batch migration result
 */
export interface BatchMigrationResult {
  /** Total documents processed */
  total: number;
  /** Successfully migrated documents */
  succeeded: number;
  /** Failed documents */
  failed: number;
  /** Individual results (only failures if detailed=false) */
  results: MigrationResult[];
}

/**
 * Options for batch migration
 */
export interface BatchMigrationOptions {
  /** Whether to stop on first error */
  stopOnError?: boolean;
  /** Include all results, not just failures */
  detailed?: boolean;
  /** Callback for progress reporting */
  onProgress?: (processed: number, total: number) => void;
}

/**
 * Schema version metadata stored with documents
 */
export interface SchemaVersionMetadata {
  /** Current schema version of the document */
  _schemaVersion: number;
  /** Timestamp of last migration */
  _migratedAt?: string;
  /** Collection name */
  _collection?: string;
}

/**
 * Serializable migration registry for persistence
 */
export interface SerializedMigrationRegistry {
  /** Collection name */
  collectionName: string;
  /** Current schema version */
  currentVersion: number;
  /** Registered migrations */
  migrations: Array<{
    version: number;
    description: string;
    metadata?: MigrationDefinition['metadata'];
  }>;
}

// ============================================================================
// Migration Runner Class
// ============================================================================

/**
 * Manages and executes schema migrations for a collection.
 *
 * The MigrationRunner maintains a registry of migrations and provides
 * methods to apply migrations to individual documents or batches.
 *
 * @example
 * ```typescript
 * const runner = new MigrationRunner('users');
 *
 * runner.register({
 *   version: 1,
 *   description: 'Add email field with default',
 *   up: addField('email', 'unknown@example.com'),
 *   down: removeField('email'),
 * });
 *
 * // Migrate a single document
 * const migrated = runner.migrateDocument(doc, 0, 1);
 *
 * // Migrate a batch
 * const results = runner.migrateBatch(docs, 0, 1);
 * ```
 */
export class MigrationRunner {
  private readonly collectionName: string;
  private readonly migrations: Map<number, MigrationDefinition> = new Map();
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
   * Get the current schema version
   */
  getCurrentVersion(): number {
    return this.currentVersion;
  }

  /**
   * Get all registered migration versions in order
   */
  getMigrationVersions(): number[] {
    return Array.from(this.migrations.keys()).sort((a, b) => a - b);
  }

  /**
   * Get a specific migration by version
   */
  getMigration(version: number): MigrationDefinition | undefined {
    return this.migrations.get(version);
  }

  /**
   * Register a migration definition
   *
   * @param migration - The migration to register
   * @throws Error if version is invalid or already registered
   */
  register(migration: MigrationDefinition): void {
    if (migration.version <= 0) {
      throw new Error(`Migration version must be positive, got ${migration.version}`);
    }

    if (this.migrations.has(migration.version)) {
      throw new Error(`Migration version ${migration.version} is already registered`);
    }

    this.migrations.set(migration.version, migration);

    // Update current version if this is higher
    if (migration.version > this.currentVersion) {
      this.currentVersion = migration.version;
    }
  }

  /**
   * Register multiple migrations at once
   *
   * @param migrations - Array of migrations to register
   */
  registerAll(migrations: MigrationDefinition[]): void {
    for (const migration of migrations) {
      this.register(migration);
    }
  }

  /**
   * Migrate a single document from one version to another
   *
   * @param doc - Document to migrate
   * @param fromVersion - Starting version (or read from doc._schemaVersion)
   * @param toVersion - Target version
   * @returns Migration result with transformed document
   */
  migrateDocument(
    doc: MigratableDocument,
    fromVersion?: number,
    toVersion?: number
  ): MigrationResult {
    // Determine starting version
    const startVersion = fromVersion ?? this.getDocumentVersion(doc);
    // Determine target version
    const endVersion = toVersion ?? this.currentVersion;

    // Validate versions
    if (startVersion < 0 || endVersion < 0) {
      return {
        fromVersion: startVersion,
        toVersion: endVersion,
        migrationsApplied: 0,
        success: false,
        error: 'Version numbers must be non-negative',
        document: doc,
      };
    }

    // Same version, no migration needed
    if (startVersion === endVersion) {
      return {
        fromVersion: startVersion,
        toVersion: endVersion,
        migrationsApplied: 0,
        success: true,
        document: this.setDocumentVersion(doc, endVersion),
      };
    }

    // Determine direction
    const isUpgrade = endVersion > startVersion;
    const versions = this.getMigrationVersions();

    // Get migrations to apply
    const migrationsToApply: MigrationDefinition[] = [];

    if (isUpgrade) {
      // Forward migrations: apply versions > startVersion and <= endVersion
      for (const version of versions) {
        if (version > startVersion && version <= endVersion) {
          const migration = this.migrations.get(version);
          if (!migration) {
            return {
              fromVersion: startVersion,
              toVersion: endVersion,
              migrationsApplied: migrationsToApply.length,
              success: false,
              error: `Missing migration for version ${version}`,
              document: doc,
            };
          }
          migrationsToApply.push(migration);
        }
      }
    } else {
      // Backward migrations: apply versions <= startVersion and > endVersion (in reverse)
      for (let i = versions.length - 1; i >= 0; i--) {
        const version = versions[i]!;
        if (version <= startVersion && version > endVersion) {
          const migration = this.migrations.get(version);
          if (!migration) {
            return {
              fromVersion: startVersion,
              toVersion: endVersion,
              migrationsApplied: migrationsToApply.length,
              success: false,
              error: `Missing migration for version ${version}`,
              document: doc,
            };
          }
          migrationsToApply.push(migration);
        }
      }
    }

    // Apply migrations
    let currentDoc = { ...doc };
    let appliedCount = 0;

    try {
      for (const migration of migrationsToApply) {
        const transform = isUpgrade ? migration.up : migration.down;
        currentDoc = transform(currentDoc);
        appliedCount++;
      }

      // Set final version
      currentDoc = this.setDocumentVersion(currentDoc, endVersion);

      return {
        fromVersion: startVersion,
        toVersion: endVersion,
        migrationsApplied: appliedCount,
        success: true,
        document: currentDoc,
      };
    } catch (error) {
      return {
        fromVersion: startVersion,
        toVersion: endVersion,
        migrationsApplied: appliedCount,
        success: false,
        error: error instanceof Error ? error.message : String(error),
        document: currentDoc,
      };
    }
  }

  /**
   * Migrate a batch of documents
   *
   * @param docs - Documents to migrate
   * @param fromVersion - Starting version for all docs (or read from each doc)
   * @param toVersion - Target version
   * @param options - Batch options
   * @returns Batch migration result
   */
  migrateBatch(
    docs: MigratableDocument[],
    fromVersion?: number,
    toVersion?: number,
    options: BatchMigrationOptions = {}
  ): BatchMigrationResult {
    const { stopOnError = false, detailed = false, onProgress } = options;

    const results: MigrationResult[] = [];
    let succeeded = 0;
    let failed = 0;

    for (let i = 0; i < docs.length; i++) {
      const result = this.migrateDocument(docs[i]!, fromVersion, toVersion);

      if (result.success) {
        succeeded++;
        if (detailed) {
          results.push(result);
        }
      } else {
        failed++;
        results.push(result);

        if (stopOnError) {
          break;
        }
      }

      if (onProgress) {
        onProgress(i + 1, docs.length);
      }
    }

    return {
      total: docs.length,
      succeeded,
      failed,
      results,
    };
  }

  /**
   * Check if migrations are needed for a document
   */
  needsMigration(doc: MigratableDocument, targetVersion?: number): boolean {
    const docVersion = this.getDocumentVersion(doc);
    const target = targetVersion ?? this.currentVersion;
    return docVersion !== target;
  }

  /**
   * Get the schema version of a document
   */
  getDocumentVersion(doc: MigratableDocument): number {
    const version = doc._schemaVersion;
    if (typeof version === 'number') {
      return version;
    }
    return 0; // Default to version 0 for documents without version
  }

  /**
   * Set the schema version on a document
   */
  private setDocumentVersion(doc: MigratableDocument, version: number): MigratableDocument {
    return {
      ...doc,
      _schemaVersion: version,
      _migratedAt: new Date().toISOString(),
    };
  }

  /**
   * Validate that all migrations are sequential
   */
  validateMigrations(): string[] {
    const errors: string[] = [];
    const versions = this.getMigrationVersions();

    // Check for gaps
    for (let i = 1; i < versions.length; i++) {
      if (versions[i]! !== versions[i - 1]! + 1) {
        errors.push(
          `Gap in migration versions: ${versions[i - 1]} to ${versions[i]}`
        );
      }
    }

    // Check that first version is 1
    if (versions.length > 0 && versions[0]! !== 1) {
      errors.push(`Migrations should start at version 1, but first is ${versions[0]}`);
    }

    return errors;
  }

  /**
   * Serialize the migration registry (migrations themselves are not serialized)
   */
  serialize(): SerializedMigrationRegistry {
    const migrations: SerializedMigrationRegistry['migrations'] = [];

    for (const [version, migration] of this.migrations) {
      migrations.push({
        version,
        description: migration.description,
        metadata: migration.metadata,
      });
    }

    // Sort by version
    migrations.sort((a, b) => a.version - b.version);

    return {
      collectionName: this.collectionName,
      currentVersion: this.currentVersion,
      migrations,
    };
  }
}

// ============================================================================
// Built-in Migration Operations
// ============================================================================

/**
 * Create a migration transform that adds a field with a default value
 *
 * @param fieldPath - Dot-notation path to the field
 * @param defaultValue - Value to set for the field
 * @returns Migration transform function
 *
 * @example
 * ```typescript
 * const migration = {
 *   version: 1,
 *   description: 'Add email field',
 *   up: addField('email', null),
 *   down: removeField('email'),
 * };
 * ```
 */
export function addField(fieldPath: string, defaultValue: unknown): MigrationTransform {
  return (doc: MigratableDocument): MigratableDocument => {
    const result = { ...doc };
    setNestedValue(result, fieldPath, defaultValue);
    return result;
  };
}

/**
 * Create a migration transform that removes a field
 *
 * @param fieldPath - Dot-notation path to the field
 * @returns Migration transform function
 *
 * @example
 * ```typescript
 * const migration = {
 *   version: 2,
 *   description: 'Remove legacy field',
 *   up: removeField('legacyData'),
 *   down: addField('legacyData', null),
 * };
 * ```
 */
export function removeField(fieldPath: string): MigrationTransform {
  return (doc: MigratableDocument): MigratableDocument => {
    const result = { ...doc };
    deleteNestedValue(result, fieldPath);
    return result;
  };
}

/**
 * Create a migration transform that renames a field
 *
 * @param oldPath - Current field path
 * @param newPath - New field path
 * @returns Migration transform function
 *
 * @example
 * ```typescript
 * const migration = {
 *   version: 3,
 *   description: 'Rename name to fullName',
 *   up: renameField('name', 'fullName'),
 *   down: renameField('fullName', 'name'),
 * };
 * ```
 */
export function renameField(oldPath: string, newPath: string): MigrationTransform {
  return (doc: MigratableDocument): MigratableDocument => {
    const result = { ...doc };
    const value = getNestedValue(result, oldPath);

    if (value !== undefined) {
      setNestedValue(result, newPath, value);
      deleteNestedValue(result, oldPath);
    }

    return result;
  };
}

/**
 * Create a migration transform that changes a field's type
 *
 * @param fieldPath - Dot-notation path to the field
 * @param toType - Target Parquet type
 * @param options - Conversion options
 * @returns Migration transform function
 *
 * @example
 * ```typescript
 * const migration = {
 *   version: 4,
 *   description: 'Convert age from string to number',
 *   up: changeFieldType('age', 'int32', {
 *     onError: 'default',
 *     defaultValue: 0,
 *     converter: (val) => parseInt(String(val), 10),
 *   }),
 *   down: changeFieldType('age', 'string', {
 *     onError: 'skip',
 *     converter: (val) => String(val),
 *   }),
 * };
 * ```
 */
export function changeFieldType(
  fieldPath: string,
  toType: ParquetType,
  options: TypeConversionOptions = { onError: 'throw' }
): MigrationTransform {
  return (doc: MigratableDocument): MigratableDocument => {
    const result = { ...doc };
    const currentValue = getNestedValue(result, fieldPath);

    // Skip if field doesn't exist
    if (currentValue === undefined) {
      return result;
    }

    try {
      let convertedValue: unknown;

      // Use custom converter if provided
      if (options.converter) {
        convertedValue = options.converter(currentValue);
      } else {
        // Use built-in type converters
        convertedValue = convertToType(currentValue, toType);
      }

      setNestedValue(result, fieldPath, convertedValue);
    } catch (error) {
      switch (options.onError) {
        case 'throw':
          throw new Error(
            `Failed to convert field '${fieldPath}' to ${toType}: ${
              error instanceof Error ? error.message : String(error)
            }`
          );
        case 'default':
          setNestedValue(result, fieldPath, options.defaultValue);
          break;
        case 'skip':
          // Leave the value unchanged
          break;
      }
    }

    return result;
  };
}

/**
 * Create a composite migration transform that applies multiple transforms in order
 *
 * @param transforms - Array of transforms to apply
 * @returns Combined migration transform function
 *
 * @example
 * ```typescript
 * const migration = {
 *   version: 5,
 *   description: 'Add email and rename name',
 *   up: compose([
 *     addField('email', null),
 *     renameField('name', 'fullName'),
 *   ]),
 *   down: compose([
 *     renameField('fullName', 'name'),
 *     removeField('email'),
 *   ]),
 * };
 * ```
 */
export function compose(transforms: MigrationTransform[]): MigrationTransform {
  return (doc: MigratableDocument): MigratableDocument => {
    let result = doc;
    for (const transform of transforms) {
      result = transform(result);
    }
    return result;
  };
}

/**
 * Create a conditional migration transform that only applies if condition is met
 *
 * @param condition - Function that returns true if transform should apply
 * @param transform - Transform to apply if condition is true
 * @returns Conditional migration transform function
 *
 * @example
 * ```typescript
 * const migration = {
 *   version: 6,
 *   description: 'Add premium flag for high-value users',
 *   up: conditional(
 *     (doc) => (doc.totalPurchases as number) > 1000,
 *     addField('isPremium', true)
 *   ),
 *   down: removeField('isPremium'),
 * };
 * ```
 */
export function conditional(
  condition: (doc: MigratableDocument) => boolean,
  transform: MigrationTransform
): MigrationTransform {
  return (doc: MigratableDocument): MigratableDocument => {
    if (condition(doc)) {
      return transform(doc);
    }
    return doc;
  };
}

// ============================================================================
// Type Conversion Utilities
// ============================================================================

/**
 * Convert a value to a target Parquet type
 *
 * @param value - Value to convert
 * @param toType - Target type
 * @returns Converted value
 * @throws Error if conversion is not possible
 */
export function convertToType(value: unknown, toType: ParquetType): unknown {
  if (value === null || value === undefined) {
    return value;
  }

  switch (toType) {
    case 'string':
      return convertToString(value);

    case 'int32':
      return convertToInt32(value);

    case 'int64':
      return convertToInt64(value);

    case 'float':
    case 'double':
      return convertToDouble(value);

    case 'boolean':
      return convertToBoolean(value);

    case 'timestamp':
      return convertToTimestamp(value);

    case 'date':
      return convertToDate(value);

    case 'binary':
      return convertToBinary(value);

    case 'variant':
      // Variant accepts anything
      return value;

    default:
      throw new Error(`Unknown target type: ${toType}`);
  }
}

function convertToString(value: unknown): string {
  if (typeof value === 'string') {
    return value;
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (value instanceof Uint8Array) {
    return new TextDecoder().decode(value);
  }
  return String(value);
}

function convertToInt32(value: unknown): number {
  if (typeof value === 'number') {
    if (!Number.isInteger(value)) {
      throw new Error('Cannot convert non-integer to int32');
    }
    if (value < -2147483648 || value > 2147483647) {
      throw new Error('Value out of int32 range');
    }
    return value;
  }
  if (typeof value === 'string') {
    const num = parseInt(value, 10);
    if (isNaN(num)) {
      throw new Error(`Cannot parse '${value}' as int32`);
    }
    return convertToInt32(num);
  }
  if (typeof value === 'boolean') {
    return value ? 1 : 0;
  }
  throw new Error(`Cannot convert ${typeof value} to int32`);
}

function convertToInt64(value: unknown): number | bigint {
  if (typeof value === 'number') {
    if (!Number.isInteger(value)) {
      throw new Error('Cannot convert non-integer to int64');
    }
    return value;
  }
  if (typeof value === 'bigint') {
    return value;
  }
  if (typeof value === 'string') {
    // Try BigInt for large numbers
    try {
      const bigNum = BigInt(value);
      // Return as number if safe, otherwise BigInt
      if (bigNum >= Number.MIN_SAFE_INTEGER && bigNum <= Number.MAX_SAFE_INTEGER) {
        return Number(bigNum);
      }
      return bigNum;
    } catch {
      throw new Error(`Cannot parse '${value}' as int64`);
    }
  }
  if (typeof value === 'boolean') {
    return value ? 1 : 0;
  }
  throw new Error(`Cannot convert ${typeof value} to int64`);
}

function convertToDouble(value: unknown): number {
  if (typeof value === 'number') {
    return value;
  }
  if (typeof value === 'string') {
    const num = parseFloat(value);
    if (isNaN(num)) {
      throw new Error(`Cannot parse '${value}' as double`);
    }
    return num;
  }
  if (typeof value === 'boolean') {
    return value ? 1.0 : 0.0;
  }
  if (typeof value === 'bigint') {
    return Number(value);
  }
  throw new Error(`Cannot convert ${typeof value} to double`);
}

function convertToBoolean(value: unknown): boolean {
  if (typeof value === 'boolean') {
    return value;
  }
  if (typeof value === 'number') {
    return value !== 0;
  }
  if (typeof value === 'string') {
    const lower = value.toLowerCase();
    if (lower === 'true' || lower === '1' || lower === 'yes') {
      return true;
    }
    if (lower === 'false' || lower === '0' || lower === 'no' || lower === '') {
      return false;
    }
    throw new Error(`Cannot parse '${value}' as boolean`);
  }
  throw new Error(`Cannot convert ${typeof value} to boolean`);
}

function convertToTimestamp(value: unknown): Date {
  if (value instanceof Date) {
    return value;
  }
  if (typeof value === 'number') {
    return new Date(value);
  }
  if (typeof value === 'string') {
    const date = new Date(value);
    if (isNaN(date.getTime())) {
      throw new Error(`Cannot parse '${value}' as timestamp`);
    }
    return date;
  }
  throw new Error(`Cannot convert ${typeof value} to timestamp`);
}

function convertToDate(value: unknown): Date {
  // Same as timestamp for now, but could be date-only in future
  return convertToTimestamp(value);
}

function convertToBinary(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (typeof value === 'string') {
    return new TextEncoder().encode(value);
  }
  if (Array.isArray(value)) {
    return new Uint8Array(value as number[]);
  }
  throw new Error(`Cannot convert ${typeof value} to binary`);
}

// ============================================================================
// Nested Value Utilities
// ============================================================================

/**
 * Get a nested value from an object using dot notation
 */
function getNestedValue(obj: MigratableDocument, path: string): unknown {
  const segments = path.split('.');
  let current: unknown = obj;

  for (const segment of segments) {
    if (current === null || current === undefined) {
      return undefined;
    }
    if (typeof current !== 'object') {
      return undefined;
    }
    current = (current as Record<string, unknown>)[segment];
  }

  return current;
}

/**
 * Set a nested value in an object using dot notation
 */
function setNestedValue(obj: MigratableDocument, path: string, value: unknown): void {
  const segments = path.split('.');
  let current: Record<string, unknown> = obj;

  for (let i = 0; i < segments.length - 1; i++) {
    const segment = segments[i]!;
    if (!(segment in current) || typeof current[segment] !== 'object' || current[segment] === null) {
      current[segment] = {};
    }
    current = current[segment] as Record<string, unknown>;
  }

  current[segments[segments.length - 1]!] = value;
}

/**
 * Delete a nested value from an object using dot notation
 */
function deleteNestedValue(obj: MigratableDocument, path: string): void {
  const segments = path.split('.');
  let current: Record<string, unknown> = obj;

  for (let i = 0; i < segments.length - 1; i++) {
    const segment = segments[i]!;
    if (!(segment in current) || typeof current[segment] !== 'object' || current[segment] === null) {
      return; // Path doesn't exist, nothing to delete
    }
    current = current[segment] as Record<string, unknown>;
  }

  delete current[segments[segments.length - 1]!];
}

// ============================================================================
// Factory Functions
// ============================================================================

/**
 * Create a new MigrationRunner for a collection
 */
export function createMigrationRunner(collectionName: string): MigrationRunner {
  return new MigrationRunner(collectionName);
}

/**
 * Create a migration definition with type checking
 */
export function defineMigration(migration: MigrationDefinition): MigrationDefinition {
  return migration;
}

// ============================================================================
// Database Migration Types
// ============================================================================

/**
 * Database interface for migration operations.
 * This is a minimal interface to avoid circular dependencies with the full Database class.
 */
export interface MigrationDatabase {
  /**
   * Get a collection by name
   */
  collection<T extends Record<string, unknown> = Record<string, unknown>>(name: string): MigrationCollection<T>;

  /**
   * Database name
   */
  readonly name: string;
}

/**
 * Collection interface for migration operations
 */
export interface MigrationCollection<T extends Record<string, unknown> = Record<string, unknown>> {
  insertOne(doc: T): Promise<{ insertedId: unknown }>;
  insertMany(docs: T[]): Promise<{ insertedCount: number }>;
  updateOne(filter: Record<string, unknown>, update: Record<string, unknown>): Promise<{ modifiedCount: number }>;
  updateMany(filter: Record<string, unknown>, update: Record<string, unknown>): Promise<{ modifiedCount: number }>;
  deleteOne(filter: Record<string, unknown>): Promise<{ deletedCount: number }>;
  deleteMany(filter: Record<string, unknown>): Promise<{ deletedCount: number }>;
  findOne(filter?: Record<string, unknown>): Promise<T | null>;
  find(filter?: Record<string, unknown>): MigrationCursor<T>;
  createIndex(spec: Record<string, unknown>, options?: Record<string, unknown>): Promise<string>;
  dropIndex(name: string): Promise<void>;
}

/**
 * Cursor interface for migration operations
 */
export interface MigrationCursor<T> {
  toArray(): Promise<T[]>;
  sort(spec: Record<string, 1 | -1>): MigrationCursor<T>;
  limit(n: number): MigrationCursor<T>;
}

/**
 * Database-level migration definition.
 *
 * Unlike MigrationDefinition (which transforms individual documents),
 * this interface defines migrations that operate on the database level,
 * such as creating collections, adding indexes, or performing bulk data updates.
 */
export interface Migration {
  /** Unique version number for this migration */
  version: number;

  /** Human-readable name for the migration */
  name: string;

  /** Function to apply the migration (upgrade) */
  up: (db: MigrationDatabase) => Promise<void>;

  /** Function to reverse the migration (downgrade) */
  down: (db: MigrationDatabase) => Promise<void>;

  /** Optional description */
  description?: string;

  /** Optional metadata */
  metadata?: Record<string, unknown>;
}

/**
 * Record of an executed migration stored in the _migrations collection
 */
export interface MigrationRecord extends Record<string, unknown> {
  /** Migration version */
  version: number;

  /** Migration name */
  name: string;

  /** When the migration was applied */
  appliedAt: Date;

  /** How long the migration took (milliseconds) */
  duration: number;

  /** Direction of migration: 'up' or 'down' */
  direction: 'up' | 'down';

  /** Whether the migration was successful */
  success: boolean;

  /** Error message if failed */
  error?: string;

  /** Optional checksum for verification */
  checksum?: string;
}

/**
 * Options for running migrations
 */
export interface RunMigrationOptions {
  /** Target version to migrate to (default: latest) */
  targetVersion?: number;

  /** Stop on first error (default: true) */
  stopOnError?: boolean;

  /** Callback for progress reporting */
  onProgress?: (migration: Migration, direction: 'up' | 'down') => void;

  /** Dry run - don't actually apply migrations */
  dryRun?: boolean;
}

/**
 * Result of a migration run
 */
export interface MigrationRunResult {
  /** Whether all migrations succeeded */
  success: boolean;

  /** Starting version */
  fromVersion: number;

  /** Ending version */
  toVersion: number;

  /** Number of migrations applied */
  migrationsApplied: number;

  /** Individual migration records */
  records: MigrationRecord[];

  /** Error if failed */
  error?: string;
}

// ============================================================================
// Migration Manager Class
// ============================================================================

/**
 * Manages database-level migrations.
 *
 * MigrationManager tracks and applies database migrations such as:
 * - Creating or dropping collections
 * - Adding or removing indexes
 * - Bulk data transformations
 * - Schema changes
 *
 * Migration state is stored in the `_migrations` collection.
 *
 * @example
 * ```typescript
 * import { MigrationManager, type Migration } from './migration.js';
 *
 * const migrations: Migration[] = [
 *   {
 *     version: 1,
 *     name: 'create-users-collection',
 *     up: async (db) => {
 *       await db.collection('users').createIndex({ email: 1 }, { unique: true });
 *     },
 *     down: async (db) => {
 *       await db.collection('users').dropIndex('email_1');
 *     },
 *   },
 *   {
 *     version: 2,
 *     name: 'add-created-at-index',
 *     up: async (db) => {
 *       await db.collection('users').createIndex({ createdAt: -1 });
 *     },
 *     down: async (db) => {
 *       await db.collection('users').dropIndex('createdAt_-1');
 *     },
 *   },
 * ];
 *
 * const manager = new MigrationManager(database);
 * await manager.runMigrations(migrations);
 *
 * // Check current version
 * const version = await manager.getCurrentVersion();
 * console.log(`Database at version ${version}`);
 *
 * // Rollback one step
 * await manager.rollback(1);
 *
 * // Get history
 * const history = await manager.getHistory();
 * ```
 */
export class MigrationManager {
  private readonly db: MigrationDatabase;
  private readonly migrationsCollectionName: string;

  constructor(db: MigrationDatabase, migrationsCollectionName: string = '_migrations') {
    this.db = db;
    this.migrationsCollectionName = migrationsCollectionName;
  }

  /**
   * Get the current migration version from the database.
   *
   * Returns 0 if no migrations have been applied.
   */
  async getCurrentVersion(): Promise<number> {
    const collection = this.db.collection<MigrationRecord>(this.migrationsCollectionName);

    // Find the most recent successful 'up' migration that hasn't been rolled back
    const records = await collection
      .find({ success: true })
      .sort({ appliedAt: -1 })
      .toArray();

    if (records.length === 0) {
      return 0;
    }

    // Calculate the effective version by tracking up/down migrations
    // Start from 0 and replay all migrations in order
    const sortedRecords = [...records].sort(
      (a, b) => new Date(a.appliedAt).getTime() - new Date(b.appliedAt).getTime()
    );

    let currentVersion = 0;
    for (const record of sortedRecords) {
      if (record.direction === 'up' && record.success) {
        currentVersion = Math.max(currentVersion, record.version);
      } else if (record.direction === 'down' && record.success) {
        // If we rolled back, the current version is one less than the rolled back version
        if (record.version <= currentVersion) {
          currentVersion = record.version - 1;
        }
      }
    }

    return Math.max(0, currentVersion);
  }

  /**
   * Run pending migrations.
   *
   * Applies all migrations with versions greater than the current database version,
   * up to the optional target version.
   *
   * @param migrations - Array of migrations to potentially apply
   * @param options - Migration options
   * @returns Result of the migration run
   */
  async runMigrations(
    migrations: Migration[],
    options: RunMigrationOptions = {}
  ): Promise<MigrationRunResult> {
    const { targetVersion, stopOnError = true, onProgress, dryRun = false } = options;

    // Validate migrations
    this.validateMigrations(migrations);

    const currentVersion = await this.getCurrentVersion();
    const target = targetVersion ?? Math.max(...migrations.map((m) => m.version), currentVersion);

    // Sort migrations by version
    const sortedMigrations = [...migrations].sort((a, b) => a.version - b.version);

    // Determine which migrations to apply
    const toApply = sortedMigrations.filter(
      (m) => m.version > currentVersion && m.version <= target
    );

    if (toApply.length === 0) {
      return {
        success: true,
        fromVersion: currentVersion,
        toVersion: currentVersion,
        migrationsApplied: 0,
        records: [],
      };
    }

    const records: MigrationRecord[] = [];
    let lastSuccessfulVersion = currentVersion;

    for (const migration of toApply) {
      if (onProgress) {
        onProgress(migration, 'up');
      }

      const startTime = Date.now();
      const record: MigrationRecord = {
        version: migration.version,
        name: migration.name,
        appliedAt: new Date(),
        duration: 0,
        direction: 'up',
        success: false,
      };

      try {
        if (!dryRun) {
          await migration.up(this.db);
        }

        record.success = true;
        record.duration = Date.now() - startTime;
        lastSuccessfulVersion = migration.version;
      } catch (error) {
        record.success = false;
        record.duration = Date.now() - startTime;
        record.error = error instanceof Error ? error.message : String(error);

        if (!dryRun) {
          await this.saveRecord(record);
        }
        records.push(record);

        if (stopOnError) {
          return {
            success: false,
            fromVersion: currentVersion,
            toVersion: lastSuccessfulVersion,
            migrationsApplied: records.filter((r) => r.success).length,
            records,
            error: record.error,
          };
        }
        continue;
      }

      if (!dryRun) {
        await this.saveRecord(record);
      }
      records.push(record);
    }

    return {
      success: records.every((r) => r.success),
      fromVersion: currentVersion,
      toVersion: lastSuccessfulVersion,
      migrationsApplied: records.filter((r) => r.success).length,
      records,
    };
  }

  /**
   * Rollback migrations.
   *
   * @param steps - Number of migrations to rollback (default: 1)
   * @returns Result of the rollback
   */
  async rollback(steps: number = 1): Promise<MigrationRunResult> {
    if (steps < 1) {
      throw new Error('Steps must be at least 1');
    }

    const currentVersion = await this.getCurrentVersion();

    if (currentVersion === 0) {
      return {
        success: true,
        fromVersion: 0,
        toVersion: 0,
        migrationsApplied: 0,
        records: [],
      };
    }

    // Get the history of successful 'up' migrations in reverse order
    const collection = this.db.collection<MigrationRecord>(this.migrationsCollectionName);
    const upMigrations = await collection
      .find({ direction: 'up', success: true })
      .sort({ version: -1 })
      .toArray();

    // Get the migrations we need to rollback
    // Filter to only include versions up to current version and take the last N
    const toRollback = upMigrations
      .filter((m) => m.version <= currentVersion)
      .slice(0, steps);

    if (toRollback.length === 0) {
      return {
        success: true,
        fromVersion: currentVersion,
        toVersion: currentVersion,
        migrationsApplied: 0,
        records: [],
      };
    }

    // We need the actual migration functions to rollback
    // For now, we return an error explaining this limitation
    // In a real implementation, migrations would be registered or stored
    throw new Error(
      `Cannot rollback: Migration definitions are required. ` +
      `Use rollbackWithMigrations() method instead, providing the migration definitions.`
    );
  }

  /**
   * Rollback migrations with provided migration definitions.
   *
   * @param migrations - Array of migration definitions
   * @param steps - Number of migrations to rollback (default: 1)
   * @param options - Options for the rollback
   * @returns Result of the rollback
   */
  async rollbackWithMigrations(
    migrations: Migration[],
    steps: number = 1,
    options: { onProgress?: (migration: Migration, direction: 'up' | 'down') => void; dryRun?: boolean } = {}
  ): Promise<MigrationRunResult> {
    const { onProgress, dryRun = false } = options;

    if (steps < 1) {
      throw new Error('Steps must be at least 1');
    }

    const currentVersion = await this.getCurrentVersion();

    if (currentVersion === 0) {
      return {
        success: true,
        fromVersion: 0,
        toVersion: 0,
        migrationsApplied: 0,
        records: [],
      };
    }

    // Create a map of migrations by version
    const migrationMap = new Map(migrations.map((m) => [m.version, m]));

    // Get versions to rollback (from current down to current - steps + 1)
    const versionsToRollback: number[] = [];
    for (let v = currentVersion; v > currentVersion - steps && v > 0; v--) {
      versionsToRollback.push(v);
    }

    const records: MigrationRecord[] = [];
    let lastVersion = currentVersion;

    for (const version of versionsToRollback) {
      const migration = migrationMap.get(version);

      if (!migration) {
        return {
          success: false,
          fromVersion: currentVersion,
          toVersion: lastVersion,
          migrationsApplied: records.filter((r) => r.success).length,
          records,
          error: `Migration version ${version} not found in provided migrations`,
        };
      }

      if (onProgress) {
        onProgress(migration, 'down');
      }

      const startTime = Date.now();
      const record: MigrationRecord = {
        version: migration.version,
        name: migration.name,
        appliedAt: new Date(),
        duration: 0,
        direction: 'down',
        success: false,
      };

      try {
        if (!dryRun) {
          await migration.down(this.db);
        }

        record.success = true;
        record.duration = Date.now() - startTime;
        lastVersion = version - 1;
      } catch (error) {
        record.success = false;
        record.duration = Date.now() - startTime;
        record.error = error instanceof Error ? error.message : String(error);

        if (!dryRun) {
          await this.saveRecord(record);
        }
        records.push(record);

        return {
          success: false,
          fromVersion: currentVersion,
          toVersion: lastVersion,
          migrationsApplied: records.filter((r) => r.success).length,
          records,
          error: record.error,
        };
      }

      if (!dryRun) {
        await this.saveRecord(record);
      }
      records.push(record);
    }

    return {
      success: true,
      fromVersion: currentVersion,
      toVersion: lastVersion,
      migrationsApplied: records.filter((r) => r.success).length,
      records,
    };
  }

  /**
   * Get the complete migration history.
   *
   * @returns Array of migration records, newest first
   */
  async getHistory(): Promise<MigrationRecord[]> {
    const collection = this.db.collection<MigrationRecord>(this.migrationsCollectionName);
    return collection.find({}).sort({ appliedAt: -1 }).toArray();
  }

  /**
   * Get pending migrations that haven't been applied yet.
   *
   * @param migrations - All available migrations
   * @returns Migrations that need to be applied
   */
  async getPendingMigrations(migrations: Migration[]): Promise<Migration[]> {
    const currentVersion = await this.getCurrentVersion();
    return migrations
      .filter((m) => m.version > currentVersion)
      .sort((a, b) => a.version - b.version);
  }

  /**
   * Check if any migrations are pending.
   *
   * @param migrations - All available migrations
   * @returns True if there are pending migrations
   */
  async hasPendingMigrations(migrations: Migration[]): Promise<boolean> {
    const pending = await this.getPendingMigrations(migrations);
    return pending.length > 0;
  }

  /**
   * Validate a set of migrations.
   *
   * @param migrations - Migrations to validate
   * @throws Error if validations fail
   */
  private validateMigrations(migrations: Migration[]): void {
    const versions = new Set<number>();

    for (const migration of migrations) {
      // Check for positive version
      if (migration.version <= 0) {
        throw new Error(`Migration version must be positive, got ${migration.version}`);
      }

      // Check for duplicate versions
      if (versions.has(migration.version)) {
        throw new Error(`Duplicate migration version: ${migration.version}`);
      }
      versions.add(migration.version);

      // Check for required fields
      if (!migration.name || typeof migration.name !== 'string') {
        throw new Error(`Migration version ${migration.version} must have a name`);
      }

      if (typeof migration.up !== 'function') {
        throw new Error(`Migration version ${migration.version} must have an up function`);
      }

      if (typeof migration.down !== 'function') {
        throw new Error(`Migration version ${migration.version} must have a down function`);
      }
    }
  }

  /**
   * Save a migration record to the database.
   *
   * @param record - Migration record to save
   */
  private async saveRecord(record: MigrationRecord): Promise<void> {
    const collection = this.db.collection<MigrationRecord>(this.migrationsCollectionName);
    await collection.insertOne(record);
  }
}

// ============================================================================
// Factory Functions for MigrationManager
// ============================================================================

/**
 * Create a new MigrationManager for a database
 */
export function createMigrationManager(
  db: MigrationDatabase,
  migrationsCollectionName?: string
): MigrationManager {
  return new MigrationManager(db, migrationsCollectionName);
}

/**
 * Define a database migration with type checking
 */
export function defineDatabaseMigration(migration: Migration): Migration {
  return migration;
}
