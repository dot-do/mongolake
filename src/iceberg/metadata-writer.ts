/**
 * Iceberg Metadata Writer
 *
 * Generates Iceberg v2 metadata.json files for MongoLake tables.
 * The metadata.json file is the heart of an Iceberg table, containing
 * all necessary information for query engines to understand the table structure.
 *
 * Key components of metadata.json:
 * - Table identification (UUID, location)
 * - Schema definitions with field IDs (supports schema evolution)
 * - Partition specifications (supports partition evolution)
 * - Sort orders (for optimized data layout)
 * - Snapshot history (for time-travel queries)
 * - Table properties (custom configuration)
 *
 * @module iceberg/metadata-writer
 * @see {@link https://iceberg.apache.org/spec/ Iceberg Specification}
 */

// ============================================================================
// Error Types
// ============================================================================

/**
 * Base error class for all metadata-related operations.
 *
 * @example
 * ```typescript
 * try {
 *   writer.generate(options);
 * } catch (error) {
 *   if (error instanceof MetadataError) {
 *     console.error('Metadata error:', error.message);
 *   }
 * }
 * ```
 */
export class MetadataError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'MetadataError';
  }
}

/**
 * Error thrown when an unsupported Iceberg format version is specified.
 * Currently supports versions 1 and 2.
 */
export class FormatVersionError extends MetadataError {
  constructor(version: number) {
    super(`Unsupported Iceberg format version: ${version}. Supported versions: 1, 2`);
    this.name = 'FormatVersionError';
  }
}

/**
 * Error thrown when schema validation fails.
 * Common causes include: empty schema, duplicate field IDs/names, invalid types.
 */
export class InvalidSchemaError extends MetadataError {
  constructor(message: string) {
    super(`Invalid schema: ${message}`);
    this.name = 'InvalidSchemaError';
  }
}

/**
 * Error thrown when partition specification validation fails.
 * Common causes include: invalid source-id references, unsupported transforms.
 */
export class InvalidPartitionSpecError extends MetadataError {
  constructor(message: string) {
    super(`Invalid partition spec: ${message}`);
    this.name = 'InvalidPartitionSpecError';
  }
}

/**
 * Error thrown when sort order validation fails.
 * Common causes include: invalid source-id references, invalid direction/null-order.
 */
export class InvalidSortOrderError extends MetadataError {
  constructor(message: string) {
    super(`Invalid sort order: ${message}`);
    this.name = 'InvalidSortOrderError';
  }
}

/**
 * Error thrown when snapshot validation fails.
 * Common causes include: negative IDs, duplicate IDs, missing manifest-list.
 */
export class InvalidSnapshotError extends MetadataError {
  constructor(message: string) {
    super(`Invalid snapshot: ${message}`);
    this.name = 'InvalidSnapshotError';
  }
}

/**
 * Error thrown when metadata serialization fails.
 * Typically indicates missing required fields like location.
 */
export class MetadataSerializationError extends MetadataError {
  constructor(message: string) {
    super(`Metadata serialization failed: ${message}`);
    this.name = 'MetadataSerializationError';
  }
}

// ============================================================================
// Type Definitions
// ============================================================================

/**
 * Iceberg primitive data types as defined in the Iceberg specification.
 *
 * @see {@link https://iceberg.apache.org/spec/#primitive-types}
 */
export type IcebergPrimitiveType =
  | 'boolean'
  | 'int'
  | 'long'
  | 'float'
  | 'double'
  | 'decimal'
  | 'date'
  | 'time'
  | 'timestamp'
  | 'timestamptz'
  | 'string'
  | 'uuid'
  | 'fixed'
  | 'binary';

/**
 * Iceberg list (array) type definition.
 *
 * @example
 * ```typescript
 * const tagsField: IcebergListType = {
 *   type: 'list',
 *   'element-id': 3,
 *   element: 'string',
 *   'element-required': false,
 * };
 * ```
 */
export interface IcebergListType {
  /** Type discriminator */
  type: 'list';
  /** Unique ID for the element field */
  'element-id': number;
  /** Type of list elements */
  element: IcebergType;
  /** Whether list elements are required (non-nullable) */
  'element-required': boolean;
}

/**
 * Iceberg map type definition.
 *
 * @example
 * ```typescript
 * const propsField: IcebergMapType = {
 *   type: 'map',
 *   'key-id': 3,
 *   key: 'string',
 *   'value-id': 4,
 *   value: 'string',
 *   'value-required': false,
 * };
 * ```
 */
export interface IcebergMapType {
  /** Type discriminator */
  type: 'map';
  /** Unique ID for the key field */
  'key-id': number;
  /** Type of map keys (must be primitive) */
  key: IcebergType;
  /** Unique ID for the value field */
  'value-id': number;
  /** Type of map values */
  value: IcebergType;
  /** Whether map values are required (non-nullable) */
  'value-required': boolean;
}

/**
 * Iceberg struct (nested record) type definition.
 */
export interface IcebergStructType {
  /** Type discriminator */
  type: 'struct';
  /** Nested fields within the struct */
  fields: IcebergSchemaField[];
}

/**
 * Union of all Iceberg types (primitive and complex).
 */
export type IcebergType =
  | IcebergPrimitiveType
  | IcebergListType
  | IcebergMapType
  | IcebergStructType;

/**
 * Schema field definition within an Iceberg schema.
 *
 * Each field has a unique ID that is preserved across schema evolution.
 */
export interface IcebergSchemaField {
  /** Unique field ID (must be positive, preserved across evolution) */
  id: number;
  /** Field name */
  name: string;
  /** Whether the field is required (non-nullable) */
  required: boolean;
  /** Field data type */
  type: IcebergType;
  /** Optional documentation for the field */
  doc?: string;
}

/**
 * Iceberg schema definition.
 *
 * Schemas are versioned by schema-id to support schema evolution.
 */
export interface IcebergSchema {
  /** Type discriminator (always 'struct' for top-level schema) */
  type: 'struct';
  /** Unique schema version ID */
  'schema-id': number;
  /** Top-level fields in the schema */
  fields: IcebergSchemaField[];
  /** Field IDs that form the row identifier (for MERGE INTO) */
  'identifier-field-ids'?: number[];
}

/**
 * Partition field definition within a partition spec.
 */
export interface PartitionField {
  /** Source field ID from the schema */
  'source-id': number;
  /** Unique partition field ID (in partition field ID space) */
  'field-id': number;
  /** Partition field name */
  name: string;
  /** Transform function (identity, year, month, day, hour, bucket[N], truncate[N], void) */
  transform: string;
}

/**
 * Partition specification defining how data is partitioned.
 *
 * Partition specs are versioned by spec-id to support partition evolution.
 */
export interface PartitionSpec {
  /** Unique partition spec ID */
  'spec-id': number;
  /** Partition fields */
  fields: PartitionField[];
}

/**
 * Sort field definition within a sort order.
 */
export interface SortField {
  /** Transform to apply before sorting */
  transform: string;
  /** Source field ID from the schema */
  'source-id': number;
  /** Sort direction */
  direction: 'asc' | 'desc';
  /** Where to place nulls in sort order */
  'null-order': 'nulls-first' | 'nulls-last';
}

/**
 * Sort order specification defining how data is sorted within files.
 *
 * Sort orders are versioned by order-id to support sort order evolution.
 */
export interface SortOrder {
  /** Unique sort order ID (0 = unsorted) */
  'order-id': number;
  /** Sort fields */
  fields: SortField[];
}

/**
 * Snapshot summary containing operation statistics.
 *
 * All numeric values are serialized as strings per Iceberg spec.
 */
export interface SnapshotSummary {
  /** Type of operation that created this snapshot */
  operation: 'append' | 'replace' | 'overwrite' | 'delete';
  /** Number of data files added in this snapshot */
  'added-data-files'?: string;
  /** Number of data files deleted in this snapshot */
  'deleted-data-files'?: string;
  /** Number of records added in this snapshot */
  'added-records'?: string;
  /** Number of records deleted in this snapshot */
  'deleted-records'?: string;
  /** Total data files after this snapshot */
  'total-data-files'?: string;
  /** Total delete files after this snapshot */
  'total-delete-files'?: string;
  /** Total records after this snapshot */
  'total-records'?: string;
  /** Total equality deletes after this snapshot */
  'total-equality-deletes'?: string;
  /** Total position deletes after this snapshot */
  'total-position-deletes'?: string;
  /** Additional custom summary properties */
  [key: string]: string | undefined;
}

/**
 * Snapshot representing a point-in-time state of the table.
 */
export interface Snapshot {
  /** Unique snapshot ID */
  'snapshot-id': number;
  /** Parent snapshot ID (for lineage) */
  'parent-snapshot-id'?: number;
  /** Sequence number (v2 only, for ordering) */
  'sequence-number'?: number;
  /** Timestamp when snapshot was created */
  'timestamp-ms': number;
  /** Location of the manifest list file */
  'manifest-list': string;
  /** Summary statistics for this snapshot */
  summary: SnapshotSummary;
  /** Schema ID used when writing this snapshot */
  'schema-id'?: number;
}

/**
 * Snapshot reference (branch or tag) for named access to snapshots.
 */
export interface SnapshotRef {
  /** Snapshot ID this reference points to */
  'snapshot-id': number;
  /** Reference type: branch (mutable) or tag (immutable) */
  type: 'branch' | 'tag';
  /** Max age in ms before ref is deleted (for expiration) */
  'max-ref-age-ms'?: number;
  /** Max age in ms for snapshots on this branch */
  'max-snapshot-age-ms'?: number;
  /** Min snapshots to keep on this branch */
  'min-snapshots-to-keep'?: number;
}

/**
 * Entry in the snapshot log tracking snapshot changes over time.
 */
export interface ManifestListLocation {
  /** Timestamp when current-snapshot-id was changed */
  'timestamp-ms': number;
  /** Snapshot ID that was current at this timestamp */
  'snapshot-id': number;
}

/**
 * Entry in the metadata log tracking metadata file changes.
 */
export interface MetadataLogEntry {
  /** Timestamp when metadata file was written */
  'timestamp-ms': number;
  /** Location of the previous metadata file */
  'metadata-file': string;
}

/**
 * Complete Iceberg table metadata structure.
 *
 * This is the root structure serialized to metadata.json files.
 */
export interface TableMetadata {
  /** Iceberg format version (1 or 2) */
  'format-version': number;
  /** Unique table UUID */
  'table-uuid': string;
  /** Base location for table data and metadata */
  location: string;
  /** Last assigned sequence number (v2 only) */
  'last-sequence-number'?: number;
  /** Timestamp of last metadata update */
  'last-updated-ms': number;
  /** Last assigned column ID across all schemas */
  'last-column-id': number;
  /** Current schema ID */
  'current-schema-id': number;
  /** All schema versions (for evolution) */
  schemas: IcebergSchema[];
  /** Current partition spec ID */
  'default-spec-id': number;
  /** All partition spec versions (for evolution) */
  'partition-specs': PartitionSpec[];
  /** Last assigned partition field ID (v2 only) */
  'last-partition-id'?: number;
  /** Current sort order ID */
  'default-sort-order-id': number;
  /** All sort order versions (for evolution) */
  'sort-orders': SortOrder[];
  /** Table properties (custom key-value configuration) */
  properties: Record<string, string>;
  /** Current snapshot ID (-1 if no snapshots) */
  'current-snapshot-id': number;
  /** All snapshots in the table */
  snapshots: Snapshot[];
  /** Named references (branches and tags) */
  refs?: Record<string, SnapshotRef>;
  /** Log of snapshot changes */
  'snapshot-log'?: ManifestListLocation[];
  /** Log of metadata file changes */
  'metadata-log'?: MetadataLogEntry[];
}

// ============================================================================
// Writer Options
// ============================================================================

/**
 * Configuration options for the MetadataWriter constructor.
 */
export interface MetadataWriterOptions {
  /**
   * Iceberg format version (1 or 2).
   * Version 2 adds sequence numbers and improved delete handling.
   * @default 2
   */
  formatVersion?: number;
  /**
   * Pretty-print JSON output with indentation.
   * Set to false for compact output.
   * @default true
   */
  prettyPrint?: boolean;
}

/**
 * Options for generating table metadata.
 *
 * Required fields are `location` and `schema`. All other fields have sensible defaults.
 *
 * @example
 * ```typescript
 * const options: GenerateMetadataOptions = {
 *   location: 's3://bucket/db/users',
 *   schema: {
 *     type: 'struct',
 *     'schema-id': 0,
 *     fields: [
 *       { id: 1, name: '_id', required: true, type: 'string' },
 *       { id: 2, name: 'name', required: false, type: 'string' },
 *     ],
 *   },
 *   properties: {
 *     'mongolake.database': 'mydb',
 *     'mongolake.collection': 'users',
 *   },
 * };
 * ```
 */
export interface GenerateMetadataOptions {
  /** Base location (URI) for the table data and metadata */
  location: string;
  /** Current schema definition */
  schema: IcebergSchema;
  /** All schema versions for schema evolution history */
  schemas?: IcebergSchema[];
  /** Table UUID (auto-generated if not provided) */
  tableUuid?: string;
  /** Last updated timestamp in milliseconds (defaults to current time) */
  lastUpdatedMs?: number;
  /** Last sequence number for v2 format (defaults to 0) */
  lastSequenceNumber?: number;
  /** Current partition specification */
  partitionSpec?: PartitionSpec;
  /** All partition specs for partition evolution history */
  partitionSpecs?: PartitionSpec[];
  /** Current sort order */
  sortOrder?: SortOrder;
  /** All sort orders for sort order evolution history */
  sortOrders?: SortOrder[];
  /** Table properties (key-value configuration) */
  properties?: Record<string, string>;
  /** Current snapshot ID (-1 if no snapshots) */
  currentSnapshotId?: number;
  /** All snapshots in the table history */
  snapshots?: Snapshot[];
  /** Named snapshot references (branches and tags) */
  refs?: Record<string, SnapshotRef>;
  /** Previous metadata file entries for metadata log */
  metadataLog?: MetadataLogEntry[];
}

// ============================================================================
// Constants
// ============================================================================

/** Valid Iceberg primitive type names for schema validation */
const VALID_PRIMITIVE_TYPES: ReadonlySet<string> = new Set([
  'boolean',
  'int',
  'long',
  'float',
  'double',
  'decimal',
  'date',
  'time',
  'timestamp',
  'timestamptz',
  'string',
  'uuid',
  'fixed',
  'binary',
]);

/** Valid non-parameterized partition transforms */
const VALID_TRANSFORMS: ReadonlySet<string> = new Set([
  'identity',
  'year',
  'month',
  'day',
  'hour',
  'void',
]);

/** Pattern for parameterized transforms: bucket[N] or truncate[N] */
const PARAMETERIZED_TRANSFORM_PATTERN = /^(bucket|truncate)\[\d+\]$/;

/** Supported Iceberg format versions */
const SUPPORTED_FORMAT_VERSIONS = { MIN: 1, MAX: 2 } as const;

/** Default snapshot ID for tables with no snapshots */
const NO_SNAPSHOT_ID = -1;

// ============================================================================
// Type Guards
// ============================================================================

/**
 * Type guard to check if a type is a primitive type string.
 */
function isPrimitiveType(type: IcebergType): type is IcebergPrimitiveType {
  return typeof type === 'string';
}

/**
 * Type guard to check if a type is a complex type (struct, list, or map).
 */
function isComplexType(
  type: IcebergType
): type is IcebergStructType | IcebergListType | IcebergMapType {
  return typeof type === 'object' && type !== null && 'type' in type;
}

/**
 * Type guard to check if a type is a struct type.
 */
function isStructType(type: IcebergType): type is IcebergStructType {
  return isComplexType(type) && type.type === 'struct';
}

/**
 * Type guard to check if a type is a list type.
 */
function isListType(type: IcebergType): type is IcebergListType {
  return isComplexType(type) && type.type === 'list';
}

/**
 * Type guard to check if a type is a map type.
 */
function isMapType(type: IcebergType): type is IcebergMapType {
  return isComplexType(type) && type.type === 'map';
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Validates that a partition transform is valid.
 *
 * @param transform - The transform string to validate
 * @returns True if the transform is valid
 */
function isValidTransform(transform: string): boolean {
  return (
    VALID_TRANSFORMS.has(transform) ||
    PARAMETERIZED_TRANSFORM_PATTERN.test(transform)
  );
}

/**
 * Creates a default unsorted sort order.
 *
 * @returns A sort order with order-id 0 and no fields
 */
function createDefaultSortOrder(): SortOrder {
  return { 'order-id': 0, fields: [] };
}

/**
 * Creates a default unpartitioned partition spec.
 *
 * @returns A partition spec with spec-id 0 and no fields
 */
function createDefaultPartitionSpec(): PartitionSpec {
  return { 'spec-id': 0, fields: [] };
}

/**
 * Generates a UUID v4 string.
 *
 * @returns A UUID string in the format xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
 */
function generateUuidV4(): string {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

// ============================================================================
// MetadataWriter Class
// ============================================================================

/**
 * Generates Iceberg v2 metadata.json files for MongoLake tables.
 *
 * The MetadataWriter handles all aspects of metadata generation including:
 * - Schema validation and serialization
 * - Partition specification handling
 * - Sort order configuration
 * - Snapshot management
 * - Format version compatibility (v1 and v2)
 *
 * @example Basic usage
 * ```typescript
 * const writer = new MetadataWriter();
 * const metadata = writer.generate({
 *   location: 's3://bucket/db/table',
 *   schema: {
 *     type: 'struct',
 *     'schema-id': 0,
 *     fields: [
 *       { id: 1, name: '_id', required: true, type: 'string' },
 *     ],
 *   },
 * });
 * ```
 *
 * @example With custom options
 * ```typescript
 * const writer = new MetadataWriter({ formatVersion: 2, prettyPrint: false });
 * const metadata = writer.generate({
 *   location: 's3://bucket/db/users',
 *   schema: userSchema,
 *   partitionSpec: bucketPartitionSpec,
 *   sortOrder: timestampSortOrder,
 *   properties: { 'mongolake.database': 'mydb' },
 * });
 * ```
 */
export class MetadataWriter {
  private readonly formatVersion: number;
  private readonly prettyPrint: boolean;

  /**
   * Creates a new MetadataWriter instance.
   *
   * @param options - Configuration options
   * @throws {FormatVersionError} If format version is not 1 or 2
   */
  constructor(options: MetadataWriterOptions = {}) {
    this.formatVersion = options.formatVersion ?? 2;
    this.prettyPrint = options.prettyPrint ?? true;

    if (
      this.formatVersion < SUPPORTED_FORMAT_VERSIONS.MIN ||
      this.formatVersion > SUPPORTED_FORMAT_VERSIONS.MAX
    ) {
      throw new FormatVersionError(this.formatVersion);
    }
  }

  /**
   * Generates metadata.json content for an Iceberg table.
   *
   * @param options - Generation options including location, schema, and optional configurations
   * @returns JSON string containing the complete table metadata
   * @throws {MetadataSerializationError} If location is missing or empty
   * @throws {InvalidSchemaError} If schema validation fails
   * @throws {InvalidPartitionSpecError} If partition spec validation fails
   * @throws {InvalidSortOrderError} If sort order validation fails
   * @throws {InvalidSnapshotError} If snapshot validation fails
   */
  generate(options: GenerateMetadataOptions): string {
    // Validate all inputs
    this.validateInputs(options);

    // Build component arrays
    const schemas = this.buildSchemasArray(options);
    const partitionSpecs = this.buildPartitionSpecsArray(options);
    const sortOrders = this.buildSortOrdersArray(options);

    // Calculate derived values
    const lastColumnId = this.calculateLastColumnId(options.schema);
    const lastPartitionId = this.calculateLastPartitionId(partitionSpecs);

    // Build the core metadata object
    const metadata = this.buildCoreMetadata(options, {
      schemas,
      partitionSpecs,
      sortOrders,
      lastColumnId,
    });

    // Add format-version-specific fields
    this.addFormatVersionFields(metadata, options, lastPartitionId);

    // Add snapshot-related fields
    this.addSnapshotFields(metadata, options);

    // Serialize to JSON
    return this.serializeMetadata(metadata);
  }

  /**
   * Generates a metadata file name for a given version number.
   *
   * @param version - Metadata version number (positive integer)
   * @returns File name in the format "v{version}.metadata.json"
   *
   * @example
   * ```typescript
   * MetadataWriter.generateMetadataFileName(1); // "v1.metadata.json"
   * MetadataWriter.generateMetadataFileName(42); // "v42.metadata.json"
   * ```
   */
  static generateMetadataFileName(version: number): string {
    return `v${version}.metadata.json`;
  }

  // ==========================================================================
  // Input Validation
  // ==========================================================================

  /**
   * Validates all input options before generating metadata.
   */
  private validateInputs(options: GenerateMetadataOptions): void {
    this.validateLocation(options.location);
    this.validateSchema(options.schema);
    this.validateOptionalSnapshots(options);
    this.validateOptionalPartitionSpec(options);
    this.validateOptionalSortOrder(options);
  }

  /**
   * Validates the table location is present and non-empty.
   */
  private validateLocation(location: string): void {
    if (!location || location.trim() === '') {
      throw new MetadataSerializationError('location is required');
    }
  }

  /**
   * Validates the schema structure and field definitions.
   */
  private validateSchema(schema: IcebergSchema): void {
    if (!schema) {
      throw new InvalidSchemaError('schema is required');
    }

    if (!schema.fields || schema.fields.length === 0) {
      throw new InvalidSchemaError('schema must have at least one field');
    }

    const fieldIds = new Set<number>();
    const fieldNames = new Set<string>();
    this.validateSchemaFields(schema.fields, fieldIds, fieldNames);
  }

  /**
   * Validates snapshots if provided, including current snapshot reference.
   */
  private validateOptionalSnapshots(options: GenerateMetadataOptions): void {
    if (options.snapshots) {
      this.validateSnapshots(options.snapshots);
    }

    // Validate current snapshot ID references an existing snapshot
    if (
      options.currentSnapshotId !== undefined &&
      options.currentSnapshotId !== NO_SNAPSHOT_ID
    ) {
      const snapshotExists = options.snapshots?.some(
        (s) => s['snapshot-id'] === options.currentSnapshotId
      );
      if (!snapshotExists) {
        throw new InvalidSnapshotError(
          `current-snapshot-id ${options.currentSnapshotId} references non-existent snapshot`
        );
      }
    }
  }

  /**
   * Validates partition spec if provided.
   */
  private validateOptionalPartitionSpec(
    options: GenerateMetadataOptions
  ): void {
    if (options.partitionSpec) {
      this.validatePartitionSpec(options.partitionSpec, options.schema);
    }
  }

  /**
   * Validates sort order if provided.
   */
  private validateOptionalSortOrder(options: GenerateMetadataOptions): void {
    if (options.sortOrder) {
      this.validateSortOrder(options.sortOrder, options.schema);
    }
  }

  /**
   * Recursively validates schema fields for valid types and unique IDs/names.
   */
  private validateSchemaFields(
    fields: IcebergSchemaField[],
    fieldIds: Set<number>,
    fieldNames: Set<string>,
    prefix: string = ''
  ): void {
    for (const field of fields) {
      this.validateFieldId(field.id, fieldIds);
      this.validateFieldName(field.name, fieldNames, prefix);
      this.validateFieldType(field.type, fieldIds);
    }
  }

  /**
   * Validates a field ID is non-negative and unique.
   */
  private validateFieldId(id: number, fieldIds: Set<number>): void {
    if (id < 0) {
      throw new InvalidSchemaError(`field ID cannot be negative: ${id}`);
    }
    if (fieldIds.has(id)) {
      throw new InvalidSchemaError(`duplicate field ID: ${id}`);
    }
    fieldIds.add(id);
  }

  /**
   * Validates a field name is unique at its level.
   */
  private validateFieldName(
    name: string,
    fieldNames: Set<string>,
    prefix: string
  ): void {
    const fullName = prefix ? `${prefix}.${name}` : name;
    if (fieldNames.has(fullName)) {
      throw new InvalidSchemaError(`duplicate field name: ${name}`);
    }
    fieldNames.add(fullName);
  }

  /**
   * Validates a field type (primitive or complex).
   */
  private validateFieldType(type: IcebergType, fieldIds: Set<number>): void {
    if (isPrimitiveType(type)) {
      this.validatePrimitiveType(type);
    } else if (isComplexType(type)) {
      this.validateComplexType(type, fieldIds);
    }
  }

  /**
   * Validates a primitive type is recognized.
   */
  private validatePrimitiveType(type: string): void {
    if (!VALID_PRIMITIVE_TYPES.has(type)) {
      throw new InvalidSchemaError(`invalid field type: ${type}`);
    }
  }

  /**
   * Validates a complex type (struct, list, or map).
   */
  private validateComplexType(
    type: IcebergStructType | IcebergListType | IcebergMapType,
    fieldIds: Set<number>
  ): void {
    if (isStructType(type)) {
      const nestedNames = new Set<string>();
      this.validateSchemaFields(type.fields, fieldIds, nestedNames);
    } else if (isListType(type)) {
      this.validateListType(type, fieldIds);
    } else if (isMapType(type)) {
      this.validateMapType(type, fieldIds);
    }
  }

  /**
   * Validates a list type element.
   */
  private validateListType(type: IcebergListType, fieldIds: Set<number>): void {
    if (isPrimitiveType(type.element) && !VALID_PRIMITIVE_TYPES.has(type.element)) {
      throw new InvalidSchemaError(`invalid list element type: ${type.element}`);
    }
    if (type['element-id'] !== undefined) {
      fieldIds.add(type['element-id']);
    }
  }

  /**
   * Validates a map type key and value.
   */
  private validateMapType(type: IcebergMapType, fieldIds: Set<number>): void {
    if (isPrimitiveType(type.key) && !VALID_PRIMITIVE_TYPES.has(type.key)) {
      throw new InvalidSchemaError(`invalid map key type: ${type.key}`);
    }
    if (isPrimitiveType(type.value) && !VALID_PRIMITIVE_TYPES.has(type.value)) {
      throw new InvalidSchemaError(`invalid map value type: ${type.value}`);
    }
    if (type['key-id'] !== undefined) {
      fieldIds.add(type['key-id']);
    }
    if (type['value-id'] !== undefined) {
      fieldIds.add(type['value-id']);
    }
  }

  /**
   * Validates a partition specification against the schema.
   */
  private validatePartitionSpec(
    spec: PartitionSpec,
    schema: IcebergSchema
  ): void {
    const schemaFieldIds = this.collectFieldIds(schema);

    for (const field of spec.fields) {
      if (!schemaFieldIds.has(field['source-id'])) {
        throw new InvalidPartitionSpecError(
          `source-id ${field['source-id']} not found in schema`
        );
      }

      if (!isValidTransform(field.transform)) {
        throw new InvalidPartitionSpecError(
          `invalid transform: ${field.transform}`
        );
      }
    }
  }

  /**
   * Validates a sort order against the schema.
   */
  private validateSortOrder(order: SortOrder, schema: IcebergSchema): void {
    const schemaFieldIds = this.collectFieldIds(schema);

    for (const field of order.fields) {
      if (!schemaFieldIds.has(field['source-id'])) {
        throw new InvalidSortOrderError(
          `source-id ${field['source-id']} not found in schema`
        );
      }

      if (field.direction !== 'asc' && field.direction !== 'desc') {
        throw new InvalidSortOrderError(
          `invalid direction: ${field.direction}`
        );
      }

      if (
        field['null-order'] !== 'nulls-first' &&
        field['null-order'] !== 'nulls-last'
      ) {
        throw new InvalidSortOrderError(
          `invalid null-order: ${field['null-order']}`
        );
      }
    }
  }

  /**
   * Validates an array of snapshots.
   */
  private validateSnapshots(snapshots: Snapshot[]): void {
    const snapshotIds = new Set<number>();

    for (const snapshot of snapshots) {
      if (snapshot['snapshot-id'] < 0) {
        throw new InvalidSnapshotError(
          `snapshot ID cannot be negative: ${snapshot['snapshot-id']}`
        );
      }

      if (snapshotIds.has(snapshot['snapshot-id'])) {
        throw new InvalidSnapshotError(
          `duplicate snapshot ID: ${snapshot['snapshot-id']}`
        );
      }
      snapshotIds.add(snapshot['snapshot-id']);

      if (!snapshot['manifest-list']) {
        throw new InvalidSnapshotError(
          `snapshot ${snapshot['snapshot-id']} missing manifest-list`
        );
      }
    }
  }

  // ==========================================================================
  // Array Builders
  // ==========================================================================

  /**
   * Builds the schemas array from options.
   */
  private buildSchemasArray(options: GenerateMetadataOptions): IcebergSchema[] {
    return options.schemas ?? [options.schema];
  }

  /**
   * Builds the partition specs array from options.
   */
  private buildPartitionSpecsArray(
    options: GenerateMetadataOptions
  ): PartitionSpec[] {
    return (
      options.partitionSpecs ?? [
        options.partitionSpec ?? createDefaultPartitionSpec(),
      ]
    );
  }

  /**
   * Builds the sort orders array from options.
   */
  private buildSortOrdersArray(options: GenerateMetadataOptions): SortOrder[] {
    if (options.sortOrders) {
      return options.sortOrders;
    }
    if (options.sortOrder) {
      // Include both the default unsorted and the custom sort order
      return [createDefaultSortOrder(), options.sortOrder];
    }
    return [createDefaultSortOrder()];
  }

  // ==========================================================================
  // Metadata Builders
  // ==========================================================================

  /**
   * Builds the core metadata object without optional fields.
   */
  private buildCoreMetadata(
    options: GenerateMetadataOptions,
    computed: {
      schemas: IcebergSchema[];
      partitionSpecs: PartitionSpec[];
      sortOrders: SortOrder[];
      lastColumnId: number;
    }
  ): TableMetadata {
    return {
      'format-version': this.formatVersion,
      'table-uuid': options.tableUuid ?? generateUuidV4(),
      location: options.location,
      'last-updated-ms': options.lastUpdatedMs ?? Date.now(),
      'last-column-id': computed.lastColumnId,
      'current-schema-id': options.schema['schema-id'],
      schemas: computed.schemas,
      'default-spec-id': this.determineDefaultSpecId(options),
      'partition-specs': computed.partitionSpecs,
      'default-sort-order-id': options.sortOrder?.['order-id'] ?? 0,
      'sort-orders': computed.sortOrders,
      properties: options.properties ?? {},
      'current-snapshot-id': options.currentSnapshotId ?? NO_SNAPSHOT_ID,
      snapshots: options.snapshots ?? [],
    };
  }

  /**
   * Determines the default partition spec ID.
   */
  private determineDefaultSpecId(options: GenerateMetadataOptions): number {
    if (options.partitionSpec) {
      return options.partitionSpec['spec-id'];
    }
    if (options.partitionSpecs && options.partitionSpecs.length > 0) {
      return options.partitionSpecs[options.partitionSpecs.length - 1]![
        'spec-id'
      ];
    }
    return 0;
  }

  /**
   * Adds format-version-specific fields (v2: sequence number, partition ID).
   */
  private addFormatVersionFields(
    metadata: TableMetadata,
    options: GenerateMetadataOptions,
    lastPartitionId: number
  ): void {
    if (this.formatVersion === 2) {
      metadata['last-sequence-number'] = options.lastSequenceNumber ?? 0;
      metadata['last-partition-id'] = lastPartitionId;
    }
  }

  /**
   * Adds snapshot-related fields (refs, snapshot-log, metadata-log).
   */
  private addSnapshotFields(
    metadata: TableMetadata,
    options: GenerateMetadataOptions
  ): void {
    const hasSnapshots = options.snapshots && options.snapshots.length > 0;

    if (hasSnapshots) {
      metadata.refs = this.buildRefs(options);
      metadata['snapshot-log'] = this.buildSnapshotLog(options.snapshots!);
    }

    metadata['metadata-log'] = options.metadataLog ?? [];
  }

  /**
   * Builds the refs (branch/tag references) object.
   */
  private buildRefs(
    options: GenerateMetadataOptions
  ): Record<string, SnapshotRef> {
    return (
      options.refs ?? {
        main: {
          'snapshot-id': options.currentSnapshotId!,
          type: 'branch',
        },
      }
    );
  }

  /**
   * Builds the snapshot log from snapshots.
   */
  private buildSnapshotLog(snapshots: Snapshot[]): ManifestListLocation[] {
    return snapshots.map((s) => ({
      'timestamp-ms': s['timestamp-ms'],
      'snapshot-id': s['snapshot-id'],
    }));
  }

  /**
   * Serializes metadata to JSON string.
   */
  private serializeMetadata(metadata: TableMetadata): string {
    return this.prettyPrint
      ? JSON.stringify(metadata, null, 2)
      : JSON.stringify(metadata);
  }

  // ==========================================================================
  // Field ID Collection and Calculation
  // ==========================================================================

  /**
   * Collects all field IDs from a schema (including nested types).
   */
  private collectFieldIds(schema: IcebergSchema): Set<number> {
    const ids = new Set<number>();
    this.collectFieldIdsRecursive(schema.fields, ids);
    return ids;
  }

  /**
   * Recursively collects field IDs from fields array.
   */
  private collectFieldIdsRecursive(
    fields: IcebergSchemaField[],
    ids: Set<number>
  ): void {
    for (const field of fields) {
      ids.add(field.id);

      if (isStructType(field.type)) {
        this.collectFieldIdsRecursive(field.type.fields, ids);
      } else if (isListType(field.type)) {
        ids.add(field.type['element-id']);
      } else if (isMapType(field.type)) {
        ids.add(field.type['key-id']);
        ids.add(field.type['value-id']);
      }
    }
  }

  /**
   * Calculates the maximum column ID used in the schema.
   */
  private calculateLastColumnId(schema: IcebergSchema): number {
    let maxId = 0;
    this.traverseFieldIds(schema.fields, (id) => {
      if (id > maxId) maxId = id;
    });
    return maxId;
  }

  /**
   * Traverses all field IDs in a fields array, calling callback for each.
   */
  private traverseFieldIds(
    fields: IcebergSchemaField[],
    callback: (id: number) => void
  ): void {
    for (const field of fields) {
      callback(field.id);

      if (isStructType(field.type)) {
        this.traverseFieldIds(field.type.fields, callback);
      } else if (isListType(field.type)) {
        callback(field.type['element-id']);
      } else if (isMapType(field.type)) {
        callback(field.type['key-id']);
        callback(field.type['value-id']);
      }
    }
  }

  /**
   * Calculates the maximum partition field ID across all partition specs.
   */
  private calculateLastPartitionId(specs: PartitionSpec[]): number {
    let maxId = 0;
    for (const spec of specs) {
      for (const field of spec.fields) {
        if (field['field-id'] > maxId) {
          maxId = field['field-id'];
        }
      }
    }
    return maxId;
  }
}
