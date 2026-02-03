/**
 * Iceberg Manifest Writer
 *
 * Generates Iceberg manifest files in Avro format.
 * Manifest files contain entries describing data files in an Iceberg table.
 *
 * @see https://iceberg.apache.org/spec/#manifests
 */

import {
  AvroEncoder,
  AvroFileWriter,
  type AvroRecord,
  type AvroType,
} from '@dotdo/iceberg';

// ============================================================================
// Type Definitions
// ============================================================================

/** File format for data files */
export type FileFormat = 'PARQUET' | 'AVRO' | 'ORC';

/** Content type for manifest files */
export type ManifestContent = 'DATA' | 'DELETES';

/** Status of a manifest entry */
export type ManifestEntryStatus = 'ADDED' | 'EXISTING' | 'DELETED';

/** Content type for data files */
export type DataFileContent = 'DATA' | 'POSITION_DELETES' | 'EQUALITY_DELETES';

/** Partition value types */
export type PartitionValue = string | number | bigint | boolean | Uint8Array | null;

/** Sort order for data files */
export interface SortOrder {
  orderId: number;
  fields: Array<{
    sourceId: number;
    transform: string;
    direction: 'asc' | 'desc';
    nullOrder: 'nulls-first' | 'nulls-last';
  }>;
}

/** Column statistics */
export interface ColumnStats {
  columnId: number;
  lowerBound?: Uint8Array;
  upperBound?: Uint8Array;
  nullCount?: number;
  valueCount?: number;
  nanCount?: number;
}

/** Data file metadata */
export interface DataFile {
  /** Content type: DATA, POSITION_DELETES, or EQUALITY_DELETES */
  content: DataFileContent;
  /** Path to the data file */
  filePath: string;
  /** File format */
  fileFormat: FileFormat;
  /** Partition values as a map of field name to value */
  partitionValues: Record<string, PartitionValue>;
  /** Number of records in the file */
  recordCount: number;
  /** Size of the file in bytes */
  fileSizeBytes: number;
  /** Map of column ID to column size in bytes */
  columnSizes?: Record<number, number>;
  /** Map of column ID to value count */
  valueCounts?: Record<number, number>;
  /** Map of column ID to null value count */
  nullValueCounts?: Record<number, number>;
  /** Map of column ID to NaN value count */
  nanValueCounts?: Record<number, number>;
  /** Map of column ID to lower bound value (binary encoded) */
  lowerBounds?: Record<number, Uint8Array>;
  /** Map of column ID to upper bound value (binary encoded) */
  upperBounds?: Record<number, Uint8Array>;
  /** Split offsets for the file */
  splitOffsets?: number[];
  /** Equality field IDs for equality delete files */
  equalityIds?: number[];
  /** Sort order ID */
  sortOrderId?: number;
  /** Referenced data file (for position deletes) */
  referencedDataFile?: string;
}

/** Delete file metadata (extends DataFile) */
export interface DeleteFile extends DataFile {
  content: 'POSITION_DELETES' | 'EQUALITY_DELETES';
}

/** Partition field summary */
export interface PartitionFieldSummary {
  /** Whether any partition value is null */
  containsNull: boolean;
  /** Whether any partition value is NaN */
  containsNaN?: boolean;
  /** Lower bound for partition values */
  lowerBound?: PartitionValue;
  /** Upper bound for partition values */
  upperBound?: PartitionValue;
}

/** Manifest entry */
export interface ManifestEntry {
  /** Entry status */
  status: ManifestEntryStatus;
  /** Snapshot ID when the entry was added */
  snapshotId?: bigint;
  /** Sequence number (v2 only) */
  sequenceNumber?: bigint;
  /** File sequence number (v2 only) */
  fileSequenceNumber?: bigint;
  /** Data file metadata */
  dataFile?: DataFile;
}

/** Manifest metadata */
export interface ManifestMetadata {
  /** Schema ID */
  schemaId: number;
  /** Partition spec ID */
  partitionSpecId: number;
  /** Format version */
  formatVersion: number;
  /** Content type */
  contentType: ManifestContent;
  /** Added files count */
  addedFilesCount: number;
  /** Existing files count */
  existingFilesCount: number;
  /** Deleted files count */
  deletedFilesCount: number;
  /** Added rows count */
  addedRowsCount: number;
  /** Existing rows count */
  existingRowsCount: number;
  /** Deleted rows count */
  deletedRowsCount: number;
  /** Minimum sequence number */
  minSequenceNumber?: bigint;
  /** Partition field summaries */
  partitionFieldSummaries?: PartitionFieldSummary[];
  /** Manifest path */
  manifestPath?: string;
  /** Manifest length in bytes */
  manifestLength?: number;
}

/** Manifest summary statistics */
export interface ManifestSummary {
  /** Total record count */
  totalRecordCount: number;
  /** Added files count */
  addedFilesCount: number;
  /** Existing files count */
  existingFilesCount: number;
  /** Deleted files count */
  deletedFilesCount: number;
  /** Position delete count */
  positionDeleteCount: number;
  /** Equality delete count */
  equalityDeleteCount: number;
  /** Deleted row count */
  deletedRowCount: number;
}

/** Manifest writer options */
export interface ManifestWriterOptions {
  /** Schema ID */
  schemaId?: number;
  /** Partition spec ID */
  partitionSpecId?: number;
  /** Format version (1 or 2) */
  formatVersion?: number;
  /** Content type (DATA or DELETES) */
  contentType?: ManifestContent;
  /** Compression codec for Avro */
  compressionCodec?: 'null' | 'deflate' | 'snappy';
}

/** Entry options for adding files */
export interface EntryOptions {
  /** Snapshot ID */
  snapshotId?: bigint;
  /** Sequence number */
  sequenceNumber?: bigint;
  /** File sequence number */
  fileSequenceNumber?: bigint;
}

// ============================================================================
// Constants
// ============================================================================

/** Set of valid file formats */
const VALID_FILE_FORMATS: ReadonlySet<string> = new Set(['PARQUET', 'AVRO', 'ORC']);

/** Set of valid entry statuses */
const VALID_STATUSES: ReadonlySet<string> = new Set(['ADDED', 'EXISTING', 'DELETED']);

/** Set of supported format versions */
const SUPPORTED_FORMAT_VERSIONS: ReadonlySet<number> = new Set([1, 2]);

/** Maps entry status to Avro integer value */
const STATUS_TO_INT: Readonly<Record<ManifestEntryStatus, number>> = {
  EXISTING: 0,
  ADDED: 1,
  DELETED: 2,
};

/** Maps content type to Avro integer value */
const CONTENT_TO_INT: Readonly<Record<DataFileContent, number>> = {
  DATA: 0,
  POSITION_DELETES: 1,
  EQUALITY_DELETES: 2,
};

// ============================================================================
// Validation Functions
// ============================================================================

/**
 * Validates the format version is supported.
 * @throws Error if format version is not 1 or 2
 */
function validateFormatVersion(version: number): void {
  if (!SUPPORTED_FORMAT_VERSIONS.has(version)) {
    throw new Error(
      `Unsupported format version: ${version}. Only versions 1 and 2 are supported.`
    );
  }
}

/**
 * Validates the entry status is valid.
 * @throws Error if status is not ADDED, EXISTING, or DELETED
 */
function validateEntryStatus(status: string): asserts status is ManifestEntryStatus {
  if (!VALID_STATUSES.has(status)) {
    throw new Error(`Invalid status: ${status}. Must be ADDED, EXISTING, or DELETED.`);
  }
}

/**
 * Validates the file format is valid.
 * @throws Error if format is not PARQUET, AVRO, or ORC
 */
function validateFileFormat(format: string): asserts format is FileFormat {
  if (!VALID_FILE_FORMATS.has(format)) {
    throw new Error(`Invalid file format: ${format}. Must be PARQUET, AVRO, or ORC.`);
  }
}

/**
 * Validates a data file has all required fields with valid values.
 * @throws Error if any validation fails
 */
function validateDataFile(dataFile: DataFile): void {
  validateFileFormat(dataFile.fileFormat);

  if (!dataFile.filePath || dataFile.filePath.trim() === '') {
    throw new Error('File path is required and cannot be empty');
  }

  if (dataFile.recordCount < 0) {
    throw new Error('Negative record count is not allowed');
  }

  if (dataFile.fileSizeBytes < 0) {
    throw new Error('Negative file size is not allowed');
  }
}

/**
 * Validates equality delete files have required equality IDs.
 * @throws Error if equality delete is missing equality IDs
 */
function validateEqualityDelete(deleteFile: DeleteFile): void {
  if (deleteFile.content === 'EQUALITY_DELETES') {
    if (!deleteFile.equalityIds || deleteFile.equalityIds.length === 0) {
      throw new Error('Equality delete files require equalityIds');
    }
  }
}

// ============================================================================
// Avro Schema Creation
// ============================================================================

/**
 * Creates the data file schema for Avro encoding.
 */
function createDataFileSchema(): AvroRecord {
  const partitionDataSchema: AvroRecord = {
    type: 'record',
    name: 'r102',
    fields: [],
  };

  const dataFileFields: Array<{ name: string; type: AvroType; 'field-id'?: number }> = [
    { name: 'content', type: 'int', 'field-id': 134 },
    { name: 'file_path', type: 'string', 'field-id': 100 },
    { name: 'file_format', type: 'string', 'field-id': 101 },
    { name: 'partition', type: partitionDataSchema, 'field-id': 102 },
    { name: 'record_count', type: 'long', 'field-id': 103 },
    { name: 'file_size_in_bytes', type: 'long', 'field-id': 104 },
    { name: 'column_sizes', type: ['null', { type: 'map', values: 'long' }], 'field-id': 108 },
    { name: 'value_counts', type: ['null', { type: 'map', values: 'long' }], 'field-id': 109 },
    { name: 'null_value_counts', type: ['null', { type: 'map', values: 'long' }], 'field-id': 110 },
    { name: 'nan_value_counts', type: ['null', { type: 'map', values: 'long' }], 'field-id': 137 },
    { name: 'lower_bounds', type: ['null', { type: 'map', values: 'bytes' }], 'field-id': 125 },
    { name: 'upper_bounds', type: ['null', { type: 'map', values: 'bytes' }], 'field-id': 128 },
    { name: 'key_metadata', type: ['null', 'bytes'], 'field-id': 131 },
    { name: 'split_offsets', type: ['null', { type: 'array', items: 'long' }], 'field-id': 132 },
    { name: 'equality_ids', type: ['null', { type: 'array', items: 'int' }], 'field-id': 135 },
    { name: 'sort_order_id', type: ['null', 'int'], 'field-id': 140 },
  ];

  return {
    type: 'record',
    name: 'r2',
    fields: dataFileFields,
  };
}

/**
 * Creates the manifest entry schema for Avro encoding.
 */
function createManifestEntrySchema(formatVersion: number): AvroRecord {
  const dataFileSchema = createDataFileSchema();

  const manifestEntryFields: Array<{ name: string; type: AvroType; 'field-id'?: number }> = [
    { name: 'status', type: 'int', 'field-id': 0 },
    { name: 'snapshot_id', type: ['null', 'long'], 'field-id': 1 },
  ];

  // Add v2-specific fields
  if (formatVersion >= 2) {
    manifestEntryFields.push(
      { name: 'sequence_number', type: ['null', 'long'], 'field-id': 3 },
      { name: 'file_sequence_number', type: ['null', 'long'], 'field-id': 4 }
    );
  }

  manifestEntryFields.push({ name: 'data_file', type: dataFileSchema, 'field-id': 2 });

  return {
    type: 'record',
    name: 'manifest_entry',
    fields: manifestEntryFields,
  };
}

// ============================================================================
// Avro Encoding Helpers
// ============================================================================

/**
 * Encodes an optional long value to Avro format.
 */
function encodeOptionalLong(encoder: AvroEncoder, value: bigint | undefined): void {
  if (value !== undefined) {
    encoder.writeUnionIndex(1);
    encoder.writeLong(value);
  } else {
    encoder.writeUnionIndex(0);
  }
}

/**
 * Encodes an optional int value to Avro format.
 */
function encodeOptionalInt(encoder: AvroEncoder, value: number | undefined): void {
  if (value !== undefined) {
    encoder.writeUnionIndex(1);
    encoder.writeInt(value);
  } else {
    encoder.writeUnionIndex(0);
  }
}

/**
 * Encodes an optional map of int to long to Avro format.
 */
function encodeOptionalIntLongMap(
  encoder: AvroEncoder,
  map: Record<number, number> | undefined
): void {
  if (map && Object.keys(map).length > 0) {
    encoder.writeUnionIndex(1);
    encoder.writeMap(
      Object.fromEntries(Object.entries(map)),
      (value) => encoder.writeLong(value)
    );
  } else {
    encoder.writeUnionIndex(0);
  }
}

/**
 * Encodes an optional map of int to bytes to Avro format.
 */
function encodeOptionalIntBytesMap(
  encoder: AvroEncoder,
  map: Record<number, Uint8Array> | undefined
): void {
  if (map && Object.keys(map).length > 0) {
    encoder.writeUnionIndex(1);
    encoder.writeMap(
      Object.fromEntries(Object.entries(map)),
      (value) => encoder.writeBytes(value)
    );
  } else {
    encoder.writeUnionIndex(0);
  }
}

/**
 * Encodes an optional array of longs to Avro format.
 */
function encodeOptionalLongArray(
  encoder: AvroEncoder,
  values: number[] | undefined
): void {
  if (values && values.length > 0) {
    encoder.writeUnionIndex(1);
    encoder.writeArray(values, (offset) => encoder.writeLong(offset));
  } else {
    encoder.writeUnionIndex(0);
  }
}

/**
 * Encodes an optional array of ints to Avro format.
 */
function encodeOptionalIntArray(
  encoder: AvroEncoder,
  values: number[] | undefined
): void {
  if (values && values.length > 0) {
    encoder.writeUnionIndex(1);
    encoder.writeArray(values, (id) => encoder.writeInt(id));
  } else {
    encoder.writeUnionIndex(0);
  }
}

/**
 * Encodes a data file record to Avro format.
 */
function encodeDataFile(encoder: AvroEncoder, dataFile: DataFile): void {
  // Content type (int)
  encoder.writeInt(CONTENT_TO_INT[dataFile.content]);

  // File path (string)
  encoder.writeString(dataFile.filePath);

  // File format (string)
  encoder.writeString(dataFile.fileFormat);

  // Partition values (empty record for unpartitioned tables)
  // Full partition encoding would require schema knowledge

  // Record count (long)
  encoder.writeLong(dataFile.recordCount);

  // File size in bytes (long)
  encoder.writeLong(dataFile.fileSizeBytes);

  // Column sizes (optional map)
  encodeOptionalIntLongMap(encoder, dataFile.columnSizes);

  // Value counts (optional map)
  encodeOptionalIntLongMap(encoder, dataFile.valueCounts);

  // Null value counts (optional map)
  encodeOptionalIntLongMap(encoder, dataFile.nullValueCounts);

  // NaN value counts (optional map)
  encodeOptionalIntLongMap(encoder, dataFile.nanValueCounts);

  // Lower bounds (optional map of bytes)
  encodeOptionalIntBytesMap(encoder, dataFile.lowerBounds);

  // Upper bounds (optional map of bytes)
  encodeOptionalIntBytesMap(encoder, dataFile.upperBounds);

  // Key metadata (optional bytes) - null for now
  encoder.writeUnionIndex(0);

  // Split offsets (optional array)
  encodeOptionalLongArray(encoder, dataFile.splitOffsets);

  // Equality IDs (optional array)
  encodeOptionalIntArray(encoder, dataFile.equalityIds);

  // Sort order ID (optional int)
  encodeOptionalInt(encoder, dataFile.sortOrderId);
}

/**
 * Encodes a manifest entry to Avro format.
 */
function encodeManifestEntry(
  encoder: AvroEncoder,
  entry: ManifestEntry,
  formatVersion: number
): void {
  // Status (int)
  encoder.writeInt(STATUS_TO_INT[entry.status]);

  // Snapshot ID (optional long)
  encodeOptionalLong(encoder, entry.snapshotId);

  // V2 sequence numbers
  if (formatVersion >= 2) {
    encodeOptionalLong(encoder, entry.sequenceNumber);
    encodeOptionalLong(encoder, entry.fileSequenceNumber);
  }

  // Data file (always present when entry has data)
  if (entry.dataFile) {
    encodeDataFile(encoder, entry.dataFile);
  }
}

// ============================================================================
// Partition Field Tracking
// ============================================================================

/**
 * Tracks partition field values for computing field summaries.
 */
class PartitionFieldTracker {
  private readonly fieldNames: Set<string> = new Set();
  private readonly fieldValues: Map<string, PartitionValue[]> = new Map();

  /**
   * Records partition values from a data file.
   */
  track(partitionValues: Record<string, PartitionValue>): void {
    for (const [fieldName, value] of Object.entries(partitionValues)) {
      this.fieldNames.add(fieldName);
      const values = this.fieldValues.get(fieldName) ?? [];
      values.push(value);
      this.fieldValues.set(fieldName, values);
    }
  }

  /**
   * Computes partition field summaries from tracked values.
   */
  getSummaries(): PartitionFieldSummary[] {
    const summaries: PartitionFieldSummary[] = [];

    for (const fieldName of this.fieldNames) {
      const values = this.fieldValues.get(fieldName) ?? [];
      summaries.push(this.computeFieldSummary(values));
    }

    return summaries;
  }

  /**
   * Computes a single partition field summary.
   */
  private computeFieldSummary(values: PartitionValue[]): PartitionFieldSummary {
    const containsNull = values.some((v) => v === null);
    const containsNaN = values.some((v) => typeof v === 'number' && Number.isNaN(v));

    const nonNullValues = values.filter(
      (v): v is Exclude<PartitionValue, null> =>
        v !== null && !(typeof v === 'number' && Number.isNaN(v))
    );

    const { lowerBound, upperBound } = this.computeBounds(nonNullValues);

    return {
      containsNull,
      containsNaN,
      lowerBound,
      upperBound,
    };
  }

  /**
   * Computes lower and upper bounds for partition values.
   */
  private computeBounds(
    values: Exclude<PartitionValue, null>[]
  ): { lowerBound?: PartitionValue; upperBound?: PartitionValue } {
    if (values.length === 0) {
      return {};
    }

    const sorted = [...values].sort((a, b) => {
      if (typeof a === 'string' && typeof b === 'string') {
        return a.localeCompare(b);
      }
      if (typeof a === 'number' && typeof b === 'number') {
        return a - b;
      }
      if (typeof a === 'bigint' && typeof b === 'bigint') {
        return a < b ? -1 : a > b ? 1 : 0;
      }
      return 0;
    });

    return {
      lowerBound: sorted[0],
      upperBound: sorted[sorted.length - 1],
    };
  }
}

// ============================================================================
// ManifestWriter Class
// ============================================================================

/**
 * Manifest writer for generating Iceberg manifest files.
 *
 * Creates valid Avro manifest files that conform to the Apache Iceberg specification.
 *
 * @example
 * ```ts
 * const writer = new ManifestWriter({
 *   schemaId: 1,
 *   partitionSpecId: 0,
 *   formatVersion: 2,
 * });
 *
 * writer.addDataFile({
 *   content: 'DATA',
 *   filePath: 's3://bucket/data/file.parquet',
 *   fileFormat: 'PARQUET',
 *   partitionValues: {},
 *   recordCount: 1000,
 *   fileSizeBytes: 100000,
 * }, 'ADDED');
 *
 * const avroData = await writer.toAvro();
 * ```
 */
export class ManifestWriter {
  private readonly schemaId: number;
  private readonly partitionSpecId: number;
  private readonly formatVersion: number;
  private readonly contentType: ManifestContent;
  private readonly compressionCodec: 'null' | 'deflate' | 'snappy';
  private readonly entries: ManifestEntry[] = [];
  private readonly partitionTracker = new PartitionFieldTracker();

  private manifestPath?: string;
  private manifestLength?: number;
  private serialized = false;

  // Cached schema to avoid recreation
  private cachedSchema?: AvroRecord;

  constructor(options: ManifestWriterOptions = {}) {
    const formatVersion = options.formatVersion ?? 2;
    validateFormatVersion(formatVersion);

    this.schemaId = options.schemaId ?? 0;
    this.partitionSpecId = options.partitionSpecId ?? 0;
    this.formatVersion = formatVersion;
    this.contentType = options.contentType ?? 'DATA';
    this.compressionCodec = options.compressionCodec ?? 'null';
  }

  /** Get the format version */
  getFormatVersion(): number {
    return this.formatVersion;
  }

  /** Get the schema ID */
  getSchemaId(): number {
    return this.schemaId;
  }

  /** Get the partition spec ID */
  getPartitionSpecId(): number {
    return this.partitionSpecId;
  }

  /** Get the content type */
  getContentType(): ManifestContent {
    return this.contentType;
  }

  /**
   * Add a data file entry to the manifest.
   *
   * @param dataFile - The data file metadata
   * @param status - The entry status (ADDED, EXISTING, or DELETED)
   * @param options - Optional entry metadata
   * @throws Error if manifest has already been serialized
   * @throws Error if validation fails
   */
  addDataFile(dataFile: DataFile, status: ManifestEntryStatus, options?: EntryOptions): void {
    this.ensureNotSerialized();
    validateEntryStatus(status);
    validateDataFile(dataFile);

    // Track partition field values for summaries
    this.partitionTracker.track(dataFile.partitionValues);

    // Create the entry
    const entry = this.createEntry(dataFile, status, options);
    this.entries.push(entry);
  }

  /**
   * Add a delete file entry to the manifest.
   *
   * @param deleteFile - The delete file metadata
   * @param status - The entry status (ADDED, EXISTING, or DELETED)
   * @param options - Optional entry metadata
   * @throws Error if content type is not DELETES
   * @throws Error if equality delete is missing equality IDs
   */
  addDeleteFile(
    deleteFile: DeleteFile,
    status: ManifestEntryStatus,
    options?: EntryOptions
  ): void {
    if (this.contentType !== 'DELETES') {
      throw new Error('Cannot add delete files to a DATA manifest');
    }

    validateEqualityDelete(deleteFile);

    // Use addDataFile for the actual addition (DeleteFile extends DataFile)
    this.addDataFile(deleteFile as DataFile, status, options);
  }

  /** Get all entries in the manifest */
  getEntries(): ManifestEntry[] {
    return [...this.entries];
  }

  /** Get summary statistics for the manifest */
  getSummary(): ManifestSummary {
    return this.computeSummary();
  }

  /** Get manifest metadata */
  getManifestMetadata(): ManifestMetadata {
    return this.computeMetadata();
  }

  /** Set the manifest path */
  setManifestPath(path: string): void {
    this.manifestPath = path;
  }

  /**
   * Serialize the manifest to Avro format.
   *
   * @returns The manifest as an Avro byte array
   */
  async toAvro(): Promise<Uint8Array> {
    const schema = this.getOrCreateSchema();
    const metadata = this.createAvroMetadata();
    const fileWriter = new AvroFileWriter(schema, metadata);

    // Encode all entries into a single block
    if (this.entries.length > 0) {
      const blockData = this.encodeEntries();
      fileWriter.addBlock(this.entries.length, blockData);
    }

    // Generate the Avro file
    const avroData = fileWriter.toBuffer();

    // Mark as serialized and record length
    this.serialized = true;
    this.manifestLength = avroData.byteLength;

    return avroData;
  }

  // ============================================================================
  // Private Methods
  // ============================================================================

  /**
   * Throws if the manifest has already been serialized.
   */
  private ensureNotSerialized(): void {
    if (this.serialized) {
      throw new Error('Manifest has already been serialized');
    }
  }

  /**
   * Creates a manifest entry from a data file.
   */
  private createEntry(
    dataFile: DataFile,
    status: ManifestEntryStatus,
    options?: EntryOptions
  ): ManifestEntry {
    const entry: ManifestEntry = {
      status,
      dataFile: { ...dataFile },
    };

    if (options?.snapshotId !== undefined) {
      entry.snapshotId = options.snapshotId;
    }

    // Only include sequence numbers for v2
    if (this.formatVersion >= 2) {
      if (options?.sequenceNumber !== undefined) {
        entry.sequenceNumber = options.sequenceNumber;
      }
      if (options?.fileSequenceNumber !== undefined) {
        entry.fileSequenceNumber = options.fileSequenceNumber;
      }
    }

    return entry;
  }

  /**
   * Computes summary statistics for the manifest.
   */
  private computeSummary(): ManifestSummary {
    let totalRecordCount = 0;
    let addedFilesCount = 0;
    let existingFilesCount = 0;
    let deletedFilesCount = 0;
    let positionDeleteCount = 0;
    let equalityDeleteCount = 0;
    let deletedRowCount = 0;

    for (const entry of this.entries) {
      if (!entry.dataFile) continue;

      totalRecordCount += entry.dataFile.recordCount;

      switch (entry.status) {
        case 'ADDED':
          addedFilesCount++;
          break;
        case 'EXISTING':
          existingFilesCount++;
          break;
        case 'DELETED':
          deletedFilesCount++;
          break;
      }

      // Track delete file types
      if (entry.dataFile.content === 'POSITION_DELETES') {
        positionDeleteCount++;
        deletedRowCount += entry.dataFile.recordCount;
      } else if (entry.dataFile.content === 'EQUALITY_DELETES') {
        equalityDeleteCount++;
      }
    }

    return {
      totalRecordCount,
      addedFilesCount,
      existingFilesCount,
      deletedFilesCount,
      positionDeleteCount,
      equalityDeleteCount,
      deletedRowCount,
    };
  }

  /**
   * Computes full manifest metadata.
   */
  private computeMetadata(): ManifestMetadata {
    const summary = this.computeSummary();
    const rowCounts = this.computeRowCountsByStatus();
    const minSequenceNumber = this.findMinSequenceNumber();
    const partitionFieldSummaries = this.partitionTracker.getSummaries();

    return {
      schemaId: this.schemaId,
      partitionSpecId: this.partitionSpecId,
      formatVersion: this.formatVersion,
      contentType: this.contentType,
      addedFilesCount: summary.addedFilesCount,
      existingFilesCount: summary.existingFilesCount,
      deletedFilesCount: summary.deletedFilesCount,
      addedRowsCount: rowCounts.added,
      existingRowsCount: rowCounts.existing,
      deletedRowsCount: rowCounts.deleted,
      minSequenceNumber,
      partitionFieldSummaries:
        partitionFieldSummaries.length > 0 ? partitionFieldSummaries : undefined,
      manifestPath: this.manifestPath,
      manifestLength: this.manifestLength,
    };
  }

  /**
   * Computes row counts grouped by entry status.
   */
  private computeRowCountsByStatus(): {
    added: number;
    existing: number;
    deleted: number;
  } {
    let added = 0;
    let existing = 0;
    let deleted = 0;

    for (const entry of this.entries) {
      if (!entry.dataFile) continue;

      switch (entry.status) {
        case 'ADDED':
          added += entry.dataFile.recordCount;
          break;
        case 'EXISTING':
          existing += entry.dataFile.recordCount;
          break;
        case 'DELETED':
          deleted += entry.dataFile.recordCount;
          break;
      }
    }

    return { added, existing, deleted };
  }

  /**
   * Finds the minimum sequence number across all entries.
   */
  private findMinSequenceNumber(): bigint | undefined {
    let minSeq: bigint | undefined;

    for (const entry of this.entries) {
      if (entry.sequenceNumber !== undefined) {
        if (minSeq === undefined || entry.sequenceNumber < minSeq) {
          minSeq = entry.sequenceNumber;
        }
      }
    }

    return minSeq;
  }

  /**
   * Gets or creates the cached Avro schema.
   */
  private getOrCreateSchema(): AvroRecord {
    if (!this.cachedSchema) {
      this.cachedSchema = createManifestEntrySchema(this.formatVersion);
    }
    return this.cachedSchema;
  }

  /**
   * Creates Avro file metadata.
   */
  private createAvroMetadata(): Map<string, string> {
    const metadata = new Map<string, string>();
    metadata.set('schema', JSON.stringify(this.schemaId));
    metadata.set('schema-id', String(this.schemaId));
    metadata.set('partition-spec-id', String(this.partitionSpecId));
    metadata.set('format-version', String(this.formatVersion));
    metadata.set('content', this.contentType === 'DATA' ? 'data' : 'deletes');
    metadata.set('avro.codec', this.compressionCodec);
    return metadata;
  }

  /**
   * Encodes all entries to Avro binary format.
   */
  private encodeEntries(): Uint8Array {
    const encoder = new AvroEncoder();
    for (const entry of this.entries) {
      encodeManifestEntry(encoder, entry, this.formatVersion);
    }
    return encoder.toBuffer();
  }
}
