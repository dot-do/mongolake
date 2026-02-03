/**
 * Schema Versioning Module
 *
 * Tracks schema changes over time with version numbers.
 * Stores schema history and supports schema evolution metadata
 * for Parquet file footers.
 *
 * Key features:
 * - Version management with unique hashes
 * - Schema comparison and diff generation
 * - Migration path calculation between versions
 * - Parquet metadata integration for file footers
 * - Version pruning strategies (count-based, age-based)
 * - Backwards compatibility detection
 *
 * @example
 * ```typescript
 * import { SchemaVersionManager } from './versioning.js';
 *
 * const manager = new SchemaVersionManager({ collectionName: 'users' });
 *
 * // Track schema evolution
 * manager.createVersion({
 *   _id: { type: 'string', required: true },
 *   name: { type: 'string', required: false },
 * });
 *
 * manager.createVersion({
 *   _id: { type: 'string', required: true },
 *   name: { type: 'string', required: false },
 *   email: { type: 'string', required: false },
 * });
 *
 * // Analyze changes
 * const diff = manager.compareVersions(1, 2);
 * console.log(diff.addedFields); // ['email']
 * console.log(diff.isBackwardsCompatible); // true
 * ```
 *
 * @module schema/versioning
 */

import type { ParquetType } from '../types.js';

// ============================================================================
// Type Definitions
// ============================================================================

/**
 * Schema field definition
 */
export interface SchemaField {
  /** Parquet type for the field */
  type: ParquetType | string;
  /** Whether the field is required */
  required: boolean;
  /** Optional documentation */
  doc?: string;
}

/**
 * Version metadata for tracking schema changes
 */
export interface VersionMetadata {
  /** Author of the schema change */
  author?: string;
  /** Message describing the change */
  message?: string;
  /** Source of the change (e.g., 'migration', 'auto', 'manual') */
  source?: string;
  /** Additional custom metadata */
  [key: string]: unknown;
}

/**
 * A single schema version
 */
export interface SchemaVersion {
  /** Version number (1-based) */
  version: number;
  /** Schema definition (field path -> field definition) */
  schema: Record<string, SchemaField>;
  /** Unique hash of the schema content */
  hash: string;
  /** When this version was created */
  createdAt: Date;
  /** Parent version number (if any) */
  parentVersion?: number;
  /** Optional metadata about this version */
  metadata?: VersionMetadata;
}

/**
 * Represents a field that changed between versions
 */
export interface ChangedField {
  /** Field path */
  path: string;
  /** Old type (if type changed) */
  oldType?: string;
  /** New type (if type changed) */
  newType?: string;
  /** Old required status (if changed) */
  oldRequired?: boolean;
  /** New required status (if changed) */
  newRequired?: boolean;
}

/**
 * Schema diff result
 */
export interface SchemaDiff {
  /** Fields added in the new version */
  addedFields: string[];
  /** Fields removed in the new version */
  removedFields: string[];
  /** Fields with changed types or required status */
  changedFields: ChangedField[];
  /** Whether the change is backwards compatible */
  isBackwardsCompatible: boolean;
  /** Human-readable summary of changes */
  summary: string;
}

/**
 * A single step in a migration path
 */
export interface MigrationStep {
  /** Starting version */
  fromVersion: number;
  /** Target version */
  toVersion: number;
  /** Diff between versions */
  diff: SchemaDiff;
}

/**
 * Options for creating a version
 */
export interface CreateVersionOptions {
  /** Metadata to attach to the version */
  metadata?: VersionMetadata;
  /** Force creation even if schema is identical */
  force?: boolean;
}

/**
 * Options for getting version history
 */
export interface VersionHistoryOptions {
  /** Return in reverse order (newest first) */
  reverse?: boolean;
  /** Limit number of versions returned */
  limit?: number;
  /** Only return versions since this date */
  since?: Date;
  /** Only return versions until this date */
  until?: Date;
}

/**
 * Options for pruning versions
 */
export interface PruneOptions {
  /** Keep only this many versions */
  keepCount?: number;
  /** Remove versions older than this date */
  olderThan?: Date;
}

/**
 * Options for creating a SchemaVersionManager
 */
export interface SchemaVersionManagerOptions {
  /** Collection name */
  collectionName?: string;
  /** Initial schema to create first version from */
  initialSchema?: Record<string, SchemaField>;
}

// ============================================================================
// Schema Version Manager Class
// ============================================================================

/**
 * Manages schema versions for a collection.
 *
 * Tracks schema changes over time, allowing comparison between versions,
 * migration path calculation, and integration with Parquet file metadata.
 *
 * @example
 * ```typescript
 * const manager = new SchemaVersionManager({ collectionName: 'users' });
 *
 * // Create initial version
 * manager.createVersion({
 *   _id: { type: 'string', required: true },
 *   name: { type: 'string', required: false },
 * });
 *
 * // Evolve schema
 * manager.createVersion({
 *   _id: { type: 'string', required: true },
 *   name: { type: 'string', required: false },
 *   email: { type: 'string', required: false },
 * });
 *
 * // Compare versions
 * const diff = manager.compareVersions(1, 2);
 * console.log(diff.addedFields); // ['email']
 * ```
 */
export class SchemaVersionManager {
  private versions: Map<number, SchemaVersion> = new Map();
  private hashIndex: Map<string, number> = new Map();
  private currentVersionNumber: number = 0;
  private collectionName: string;

  constructor(options: SchemaVersionManagerOptions = {}) {
    this.collectionName = options.collectionName ?? 'unknown';

    if (options.initialSchema) {
      this.createVersion(options.initialSchema);
    }
  }

  /**
   * Create a SchemaVersionManager from serialized JSON.
   */
  static fromJSON(json: string): SchemaVersionManager {
    const data = JSON.parse(json);
    const manager = new SchemaVersionManager({
      collectionName: data.collectionName,
    });

    // Restore versions
    for (const versionData of data.versions) {
      const version: SchemaVersion = {
        version: versionData.version,
        schema: versionData.schema,
        hash: versionData.hash,
        createdAt: new Date(versionData.createdAt),
        parentVersion: versionData.parentVersion,
        metadata: versionData.metadata,
      };
      manager.versions.set(version.version, version);
      manager.hashIndex.set(version.hash, version.version);
    }

    manager.currentVersionNumber = data.currentVersion;

    return manager;
  }

  /**
   * Get the collection name.
   */
  getCollectionName(): string {
    return this.collectionName;
  }

  /**
   * Get the current schema version.
   */
  getCurrentVersion(): SchemaVersion | undefined {
    return this.versions.get(this.currentVersionNumber);
  }

  /**
   * Get the total number of versions.
   */
  getVersionCount(): number {
    return this.versions.size;
  }

  /**
   * Get a specific version by version number.
   */
  getVersion(versionNumber: number): SchemaVersion | undefined {
    return this.versions.get(versionNumber);
  }

  /**
   * Get a version by its hash.
   */
  getVersionByHash(hash: string): SchemaVersion | undefined {
    const versionNumber = this.hashIndex.get(hash);
    if (versionNumber === undefined) {
      return undefined;
    }
    return this.versions.get(versionNumber);
  }

  /**
   * Get version history with optional filtering.
   */
  getVersionHistory(options: VersionHistoryOptions = {}): SchemaVersion[] {
    let versions = Array.from(this.versions.values());

    // Filter by date range
    if (options.since) {
      versions = versions.filter((v) => v.createdAt >= options.since!);
    }
    if (options.until) {
      versions = versions.filter((v) => v.createdAt <= options.until!);
    }

    // Sort by version number
    versions.sort((a, b) => {
      if (options.reverse) {
        return b.version - a.version;
      }
      return a.version - b.version;
    });

    // Apply limit
    if (options.limit !== undefined && options.limit < versions.length) {
      versions = versions.slice(0, options.limit);
    }

    return versions;
  }

  /**
   * Create a new schema version.
   *
   * If the schema is identical to the current version (same hash),
   * returns the existing version unless `force` is true.
   */
  createVersion(
    schema: Record<string, SchemaField>,
    options: CreateVersionOptions = {}
  ): SchemaVersion {
    const hash = this.computeSchemaHash(schema);

    // Check if schema already exists (unless force is true)
    if (!options.force) {
      const existingVersionNumber = this.hashIndex.get(hash);
      if (existingVersionNumber !== undefined) {
        return this.versions.get(existingVersionNumber)!;
      }
    }

    // Create new version
    const versionNumber = this.currentVersionNumber + 1;
    const version: SchemaVersion = {
      version: versionNumber,
      schema: { ...schema },
      hash,
      createdAt: new Date(),
      parentVersion: this.currentVersionNumber > 0 ? this.currentVersionNumber : undefined,
      metadata: options.metadata,
    };

    // Store version
    this.versions.set(versionNumber, version);
    this.hashIndex.set(hash, versionNumber);
    this.currentVersionNumber = versionNumber;

    return version;
  }

  /**
   * Compare two versions and return a diff.
   */
  compareVersions(fromVersion: number, toVersion: number): SchemaDiff {
    const from = this.versions.get(fromVersion);
    const to = this.versions.get(toVersion);

    if (!from) {
      throw new Error(`Version ${fromVersion} not found`);
    }
    if (!to) {
      throw new Error(`Version ${toVersion} not found`);
    }

    return compareSchemaVersions(from.schema, to.schema);
  }

  /**
   * Calculate the migration path between two versions.
   */
  getMigrationPath(fromVersion: number, toVersion: number): MigrationStep[] {
    if (fromVersion === toVersion) {
      return [];
    }

    const versions = this.getVersionHistory();
    return calculateMigrationPath(versions, fromVersion, toVersion);
  }

  /**
   * Serialize to JSON.
   */
  toJSON(): string {
    const versions = this.getVersionHistory().map((v) => ({
      version: v.version,
      schema: v.schema,
      hash: v.hash,
      createdAt: v.createdAt.toISOString(),
      parentVersion: v.parentVersion,
      metadata: v.metadata,
    }));

    return JSON.stringify({
      collectionName: this.collectionName,
      currentVersion: this.currentVersionNumber,
      versions,
    });
  }

  /**
   * Export schema metadata for Parquet file footer.
   */
  toParquetMetadata(): Record<string, string> {
    const current = this.getCurrentVersion();
    if (!current) {
      return {
        'mongolake.collection': this.collectionName,
        'mongolake.schema.version': '0',
      };
    }

    return {
      'mongolake.collection': this.collectionName,
      'mongolake.schema.version': String(current.version),
      'mongolake.schema.hash': current.hash,
      'mongolake.schema.created_at': current.createdAt.toISOString(),
    };
  }

  /**
   * Prune old versions according to options.
   *
   * Always keeps at least the current version.
   */
  pruneVersions(options: PruneOptions): void {
    const currentVersion = this.currentVersionNumber;

    // Get versions to potentially remove
    let versionsToCheck = this.getVersionHistory();

    // Never remove the current version
    versionsToCheck = versionsToCheck.filter((v) => v.version !== currentVersion);

    // Filter by date if specified
    if (options.olderThan) {
      versionsToCheck = versionsToCheck.filter((v) => v.createdAt < options.olderThan!);
    }

    // If keepCount is specified, calculate which to remove
    if (options.keepCount !== undefined) {
      const allVersions = this.getVersionHistory({ reverse: true });
      const toKeep = new Set(
        allVersions.slice(0, Math.max(1, options.keepCount)).map((v) => v.version)
      );

      versionsToCheck = versionsToCheck.filter((v) => !toKeep.has(v.version));
    }

    // Remove versions
    for (const version of versionsToCheck) {
      this.versions.delete(version.version);
      this.hashIndex.delete(version.hash);
    }
  }

  /**
   * Compute a hash for a schema.
   *
   * The hash is deterministic and based on the schema content,
   * so identical schemas will have the same hash.
   */
  private computeSchemaHash(schema: Record<string, SchemaField>): string {
    // Sort keys for deterministic ordering
    const sortedKeys = Object.keys(schema).sort();
    const normalized = sortedKeys.map((key) => {
      const field = schema[key]!;
      return `${key}:${field.type}:${field.required}`;
    }).join('|');

    // Simple hash function (FNV-1a)
    let hash = 2166136261;
    for (let i = 0; i < normalized.length; i++) {
      hash ^= normalized.charCodeAt(i);
      hash = (hash * 16777619) >>> 0;
    }

    return hash.toString(16).padStart(8, '0');
  }
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Create a SchemaVersionManager with options.
 */
export function createSchemaVersionManager(
  options: SchemaVersionManagerOptions
): SchemaVersionManager {
  return new SchemaVersionManager(options);
}

/**
 * Compare two schemas directly (without version manager).
 */
export function compareSchemaVersions(
  oldSchema: Record<string, SchemaField>,
  newSchema: Record<string, SchemaField>
): SchemaDiff {
  const oldFields = new Set(Object.keys(oldSchema));
  const newFields = new Set(Object.keys(newSchema));

  // Find added fields
  const addedFields: string[] = [];
  for (const field of newFields) {
    if (!oldFields.has(field)) {
      addedFields.push(field);
    }
  }

  // Find removed fields
  const removedFields: string[] = [];
  for (const field of oldFields) {
    if (!newFields.has(field)) {
      removedFields.push(field);
    }
  }

  // Find changed fields
  const changedFields: ChangedField[] = [];
  for (const field of oldFields) {
    if (!newFields.has(field)) {
      continue; // Already tracked as removed
    }

    const oldField = oldSchema[field]!;
    const newField = newSchema[field]!;

    const typeChanged = oldField.type !== newField.type;
    const requiredChanged = oldField.required !== newField.required;

    if (typeChanged || requiredChanged) {
      const change: ChangedField = { path: field };
      if (typeChanged) {
        change.oldType = oldField.type as string;
        change.newType = newField.type as string;
      }
      if (requiredChanged) {
        change.oldRequired = oldField.required;
        change.newRequired = newField.required;
      }
      changedFields.push(change);
    }
  }

  // Determine backwards compatibility
  // Breaking changes:
  // - Removing fields
  // - Making fields required that were optional
  // - Narrowing types (detected via type hierarchy)
  let isBackwardsCompatible = true;

  if (removedFields.length > 0) {
    isBackwardsCompatible = false;
  }

  for (const change of changedFields) {
    // Making optional field required is breaking
    if (change.oldRequired === false && change.newRequired === true) {
      isBackwardsCompatible = false;
      break;
    }

    // Type narrowing is breaking (e.g., int64 -> int32)
    if (change.oldType && change.newType) {
      if (!isTypeWideningAllowed(change.oldType, change.newType)) {
        isBackwardsCompatible = false;
        break;
      }
    }
  }

  // Generate summary
  const summaryParts: string[] = [];
  if (addedFields.length > 0) {
    summaryParts.push(`Added ${addedFields.length} field(s): ${addedFields.join(', ')}`);
  }
  if (removedFields.length > 0) {
    summaryParts.push(`Removed ${removedFields.length} field(s): ${removedFields.join(', ')}`);
  }
  if (changedFields.length > 0) {
    summaryParts.push(`Changed ${changedFields.length} field(s): ${changedFields.map((c) => c.path).join(', ')}`);
  }
  if (summaryParts.length === 0) {
    summaryParts.push('No changes');
  }

  return {
    addedFields,
    removedFields,
    changedFields,
    isBackwardsCompatible,
    summary: summaryParts.join('; '),
  };
}

/**
 * Generate a detailed schema diff.
 */
export function generateSchemaDiff(
  oldSchema: Record<string, SchemaField>,
  newSchema: Record<string, SchemaField>
): SchemaDiff {
  return compareSchemaVersions(oldSchema, newSchema);
}

/**
 * Calculate migration path between versions.
 *
 * Computes the sequence of schema changes needed to migrate
 * from one version to another, including forward and backward
 * migrations.
 *
 * @param versions - Array of schema versions
 * @param fromVersion - Starting version number
 * @param toVersion - Target version number
 * @returns Array of migration steps with diffs
 */
export function calculateMigrationPath(
  versions: SchemaVersion[],
  fromVersion: number,
  toVersion: number
): MigrationStep[] {
  if (fromVersion === toVersion) {
    return [];
  }

  // Create version map for quick lookup
  const versionMap = new Map<number, SchemaVersion>();
  for (const v of versions) {
    versionMap.set(v.version, v);
  }

  const steps: MigrationStep[] = [];
  const isForward = toVersion > fromVersion;

  let current = fromVersion;
  while (current !== toVersion) {
    const next = isForward ? current + 1 : current - 1;

    const fromSchema = versionMap.get(current);
    const toSchema = versionMap.get(next);

    if (!fromSchema || !toSchema) {
      break;
    }

    const diff = compareSchemaVersions(fromSchema.schema, toSchema.schema);

    steps.push({
      fromVersion: current,
      toVersion: next,
      diff,
    });

    current = next;
  }

  return steps;
}

// ============================================================================
// Type Widening Rules
// ============================================================================

/**
 * Type promotion hierarchy for safe type widening.
 *
 * Defines which type changes are backwards compatible.
 * A type can safely be widened to any type in its promotion set.
 */
const TYPE_WIDENING_RULES: Record<string, Set<string>> = {
  // Integer promotions
  'int32': new Set(['int64', 'double', 'variant']),
  'int64': new Set(['double', 'variant']),

  // Float promotions
  'float': new Set(['double', 'variant']),
  'double': new Set(['variant']),

  // String promotions
  'string': new Set(['variant']),

  // Boolean promotions
  'boolean': new Set(['variant']),

  // Date/time promotions
  'date': new Set(['timestamp', 'variant']),
  'timestamp': new Set(['variant']),

  // Binary promotions
  'binary': new Set(['variant']),
};

/**
 * Check if a type change represents safe type widening.
 *
 * Type widening is allowed when the new type can represent
 * all values of the old type without loss of precision.
 *
 * @param oldType - Original type
 * @param newType - New type
 * @returns true if the change is a safe widening
 */
export function isTypeWideningAllowed(oldType: string, newType: string): boolean {
  // Same type is always allowed
  if (oldType === newType) {
    return true;
  }

  // Check widening rules
  const allowedWidenings = TYPE_WIDENING_RULES[oldType];
  if (allowedWidenings) {
    return allowedWidenings.has(newType);
  }

  // Variant can widen to variant (no change)
  if (oldType === 'variant' && newType === 'variant') {
    return true;
  }

  // Unknown types - assume not compatible
  return false;
}

/**
 * Get the common supertype for two types.
 *
 * Returns the most specific type that can represent values
 * from both input types, or 'variant' if no common type exists.
 *
 * @param type1 - First type
 * @param type2 - Second type
 * @returns Common supertype
 */
export function getCommonSupertype(type1: string, type2: string): string {
  if (type1 === type2) {
    return type1;
  }

  // Check if type1 can widen to type2
  if (isTypeWideningAllowed(type1, type2)) {
    return type2;
  }

  // Check if type2 can widen to type1
  if (isTypeWideningAllowed(type2, type1)) {
    return type1;
  }

  // Fall back to variant (can represent any type)
  return 'variant';
}
