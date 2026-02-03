/**
 * Iceberg Manifest List Writer
 *
 * Generates manifest-list.avro files that track all manifests in a snapshot.
 * Implements the Apache Iceberg specification for manifest list file format.
 *
 * @see https://iceberg.apache.org/spec/#manifest-lists
 */

import {
  AvroEncoder,
  AvroFileWriter,
  createManifestListSchema,
  generateUUID,
  MANIFEST_CONTENT_DATA as ICEBERG_MANIFEST_CONTENT_DATA,
  MANIFEST_CONTENT_DELETES as ICEBERG_MANIFEST_CONTENT_DELETES,
} from '@dotdo/iceberg';
import type { StorageBackend } from '../storage/index.js';

// ============================================================================
// Constants
// ============================================================================

/** Content type for data file manifests */
export const MANIFEST_CONTENT_DATA = ICEBERG_MANIFEST_CONTENT_DATA;

/** Content type for delete file manifests */
export const MANIFEST_CONTENT_DELETES = ICEBERG_MANIFEST_CONTENT_DELETES;

/** Valid manifest content types */
const VALID_CONTENT_TYPES: ReadonlySet<number> = new Set([
  MANIFEST_CONTENT_DATA,
  MANIFEST_CONTENT_DELETES,
]);

// ============================================================================
// Types
// ============================================================================

/** Content type: DATA (0) for data files, DELETES (1) for delete files */
export type ManifestContent = typeof MANIFEST_CONTENT_DATA | typeof MANIFEST_CONTENT_DELETES;

/**
 * Partition field summary for a manifest file.
 * Tracks statistics about partition values within the manifest.
 */
export interface PartitionFieldSummary {
  /** Whether the partition field contains null values */
  contains_null: boolean;
  /** Whether the partition field contains NaN values */
  contains_nan?: boolean;
  /** Lower bound of partition values (binary-encoded) */
  lower_bound?: Uint8Array;
  /** Upper bound of partition values (binary-encoded) */
  upper_bound?: Uint8Array;
}

/**
 * Manifest file entry in a manifest list.
 * Represents metadata about a single manifest file.
 */
export interface ManifestFileEntry {
  /** Location of the manifest file */
  manifest_path: string;
  /** Length of the manifest file in bytes */
  manifest_length: number;
  /** ID of the partition spec used to write the manifest */
  partition_spec_id: number;
  /** Content type: DATA (0) or DELETES (1) */
  content: ManifestContent;
  /** Sequence number when the manifest was added to the table */
  sequence_number: bigint;
  /** Minimum data sequence number of all live data or delete files */
  min_sequence_number: bigint;
  /** ID of the snapshot that added the manifest */
  added_snapshot_id: bigint;
  /** Number of entries with status ADDED */
  added_data_files_count: number;
  /** Number of entries with status EXISTING */
  existing_data_files_count: number;
  /** Number of entries with status DELETED */
  deleted_data_files_count: number;
  /** Total number of rows in all files with status ADDED */
  added_rows_count: bigint;
  /** Total number of rows in all files with status EXISTING */
  existing_rows_count: bigint;
  /** Total number of rows in all files with status DELETED */
  deleted_rows_count: bigint;
  /** Partition field summaries, one per partition field */
  partitions?: PartitionFieldSummary[];
  /** Encryption key metadata (binary) */
  key_metadata?: Uint8Array;
}

/**
 * Metadata about the manifest list file.
 */
export interface ManifestListMetadata {
  /** ID of the snapshot this manifest list belongs to */
  snapshotId?: bigint;
  /** ID of the parent snapshot */
  parentSnapshotId?: bigint;
  /** Sequence number of the snapshot */
  sequenceNumber?: bigint;
  /** Format version (1 or 2) */
  formatVersion?: number;
  /** ID of the current schema */
  schemaId?: number;
  /** JSON representation of the schema */
  schema?: unknown;
  /** Compression codec used (e.g., 'snappy', 'deflate') */
  codec?: string;
  /** Number of manifest entries */
  manifestCount?: number;
  /** Timestamp when the manifest list was written */
  timestampMs?: number;
  /** Custom metadata properties */
  customProperties?: Record<string, string>;
}

/**
 * Options for creating a ManifestListWriter.
 */
export interface ManifestListWriterOptions {
  /** ID of the snapshot being written */
  snapshotId?: bigint;
  /** ID of the parent snapshot */
  parentSnapshotId?: bigint;
  /** Sequence number for the snapshot */
  sequenceNumber?: bigint;
  /** Iceberg format version (default: 2) */
  formatVersion?: number;
  /** Table schema for metadata */
  tableSchema?: unknown;
  /** Schema ID */
  schemaId?: number;
  /** Compression codec (default: 'snappy') */
  codec?: 'snappy' | 'deflate' | 'null';
  /** Custom metadata to include in Avro file */
  customMetadata?: Record<string, string>;
}

/**
 * Options for writing the manifest list.
 */
export interface WriteOptions {
  /** Whether to sort delete manifests before data manifests */
  sortDeletesFirst?: boolean;
}

/**
 * Result of writing a manifest list.
 */
export interface ManifestListWriteResult {
  /** Path where the manifest list was written */
  path: string;
  /** Avro schema used for the manifest list */
  avroSchema: {
    type: string;
    name: string;
    fields: Array<{ name: string; type: unknown }>;
  };
  /** Metadata about the manifest list */
  metadata: ManifestListMetadata;
}

/**
 * Statistics about the manifest list.
 */
export interface ManifestListStatistics {
  /** Total number of manifest entries */
  totalManifestCount: number;
  /** Number of data manifest entries */
  dataManifestCount: number;
  /** Number of delete manifest entries */
  deleteManifestCount: number;
  /** Total added data files across all manifests */
  totalAddedDataFiles: number;
  /** Total existing data files across all manifests */
  totalExistingDataFiles: number;
  /** Total deleted data files across all manifests */
  totalDeletedDataFiles: number;
  /** Total added rows across all manifests */
  totalAddedRows: bigint;
  /** Total existing rows across all manifests */
  totalExistingRows: bigint;
  /** Total deleted rows across all manifests */
  totalDeletedRows: bigint;
  /** Bytes written to storage */
  bytesWritten: number;
}

/** Writer state */
export type ManifestListWriterState = 'initialized' | 'writing' | 'written' | 'closed';

// ============================================================================
// Error Classes
// ============================================================================

/**
 * Base error class for manifest list operations.
 */
export class ManifestListError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ManifestListError';
  }
}

/**
 * Error thrown when manifest entry validation fails.
 */
export class ManifestEntryValidationError extends ManifestListError {
  constructor(message: string) {
    super(`Validation error: ${message}`);
    this.name = 'ManifestEntryValidationError';
  }
}

/**
 * Error thrown when writer is in an invalid state.
 */
export class ManifestListWriterStateError extends ManifestListError {
  constructor(message: string) {
    super(message);
    this.name = 'ManifestListWriterStateError';
  }
}

/**
 * Error thrown when storage operations fail.
 */
export class ManifestListStorageError extends ManifestListError {
  constructor(operation: 'read' | 'write', cause: string) {
    super(`Storage ${operation} failed: ${cause}`);
    this.name = 'ManifestListStorageError';
  }
}

// ============================================================================
// Validation Utilities
// ============================================================================

/**
 * Validates a manifest file entry.
 *
 * @param entry - The manifest entry to validate
 * @throws {ManifestEntryValidationError} If validation fails
 */
function validateManifestEntry(entry: ManifestFileEntry): void {
  // Validate manifest_path
  if (!entry.manifest_path || entry.manifest_path.trim() === '') {
    throw new ManifestEntryValidationError('manifest_path cannot be empty');
  }

  // Validate manifest_length
  if (entry.manifest_length <= 0) {
    throw new ManifestEntryValidationError('manifest_length must be positive');
  }

  // Validate content type
  if (!VALID_CONTENT_TYPES.has(entry.content)) {
    throw new ManifestEntryValidationError('content value is invalid (must be 0 or 1)');
  }

  // Validate sequence numbers
  if (entry.min_sequence_number > entry.sequence_number) {
    throw new ManifestEntryValidationError('min_sequence_number cannot be greater than sequence_number');
  }

  // Validate file counts are non-negative
  validateNonNegativeInt(entry.added_data_files_count, 'added_data_files_count');
  validateNonNegativeInt(entry.existing_data_files_count, 'existing_data_files_count');
  validateNonNegativeInt(entry.deleted_data_files_count, 'deleted_data_files_count');

  // Validate row counts are non-negative
  validateNonNegativeBigInt(entry.added_rows_count, 'added_rows_count');
  validateNonNegativeBigInt(entry.existing_rows_count, 'existing_rows_count');
  validateNonNegativeBigInt(entry.deleted_rows_count, 'deleted_rows_count');
}

/**
 * Validates that a number is non-negative.
 */
function validateNonNegativeInt(value: number, fieldName: string): void {
  if (value < 0) {
    throw new ManifestEntryValidationError(`${fieldName} cannot be negative`);
  }
}

/**
 * Validates that a bigint is non-negative.
 */
function validateNonNegativeBigInt(value: bigint, fieldName: string): void {
  if (value < BigInt(0)) {
    throw new ManifestEntryValidationError(`${fieldName} cannot be negative`);
  }
}

// ============================================================================
// Avro Parsing Utilities
// ============================================================================

/**
 * Result of reading a variable-length integer.
 */
interface VarIntResult {
  value: number;
  newOffset: number;
}

/**
 * Result of reading a variable-length long.
 */
interface VarLongResult {
  value: bigint;
  newOffset: number;
}

/**
 * Result of reading a string.
 */
interface StringResult {
  value: string;
  newOffset: number;
}

/**
 * Result of reading bytes.
 */
interface BytesResult {
  value: Uint8Array;
  newOffset: number;
}

/**
 * Read a variable-length integer (zig-zag encoded) from Avro data.
 */
function readVarInt(data: Uint8Array, offset: number): VarIntResult {
  let result = 0;
  let shift = 0;

  while (true) {
    const byte = data[offset++]!;
    result |= (byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) break;
    shift += 7;
  }

  // Zig-zag decode
  const decoded = (result >>> 1) ^ -(result & 1);
  return { value: decoded, newOffset: offset };
}

/**
 * Read a variable-length long (zig-zag encoded) from Avro data.
 */
function readVarLong(data: Uint8Array, offset: number): VarLongResult {
  let result = BigInt(0);
  let shift = BigInt(0);

  while (true) {
    const byte = data[offset++]!;
    result |= BigInt(byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) break;
    shift += BigInt(7);
  }

  // Zig-zag decode
  const decoded = (result >> BigInt(1)) ^ -(result & BigInt(1));
  return { value: decoded, newOffset: offset };
}

/**
 * Read a string (length-prefixed UTF-8) from Avro data.
 */
function readString(data: Uint8Array, offset: number): StringResult {
  const lenResult = readVarLong(data, offset);
  const length = Number(lenResult.value);
  offset = lenResult.newOffset;

  const strBytes = data.slice(offset, offset + length);
  const value = new TextDecoder().decode(strBytes);

  return { value, newOffset: offset + length };
}

/**
 * Read bytes (length-prefixed) from Avro data.
 */
function readBytes(data: Uint8Array, offset: number): BytesResult {
  const lenResult = readVarLong(data, offset);
  const length = Number(lenResult.value);
  offset = lenResult.newOffset;

  const value = data.slice(offset, offset + length);
  return { value, newOffset: offset + length };
}

/**
 * Read an array of items from Avro data.
 */
function readArray<T>(
  data: Uint8Array,
  offset: number,
  readItem: (data: Uint8Array, offset: number) => { item: T; newOffset: number }
): { items: T[]; newOffset: number } {
  const items: T[] = [];

  while (true) {
    const countResult = readVarLong(data, offset);
    offset = countResult.newOffset;

    let count = countResult.value;
    if (count === BigInt(0)) break;

    // Negative count indicates byte size follows
    if (count < BigInt(0)) {
      count = -count;
      // Skip block size
      const sizeResult = readVarLong(data, offset);
      offset = sizeResult.newOffset;
    }

    for (let i = BigInt(0); i < count; i++) {
      const result = readItem(data, offset);
      items.push(result.item);
      offset = result.newOffset;
    }
  }

  return { items, newOffset: offset };
}

// ============================================================================
// Implementation
// ============================================================================

/**
 * Writer for Iceberg manifest-list.avro files.
 *
 * Usage:
 * ```typescript
 * const writer = new ManifestListWriter(storage, { snapshotId: 1n });
 * await writer.addManifest(manifestEntry);
 * const result = await writer.write('metadata/snap-1-uuid.avro');
 * await writer.close();
 * ```
 */
export class ManifestListWriter {
  private readonly storage: StorageBackend;
  private readonly options: ManifestListWriterOptions;
  private state: ManifestListWriterState = 'initialized';
  private readonly manifests: ManifestFileEntry[] = [];
  private bytesWritten: number = 0;

  constructor(storage: StorageBackend, options: ManifestListWriterOptions = {}) {
    this.storage = storage;
    this.options = {
      formatVersion: 2,
      codec: 'snappy',
      ...options,
    };
  }

  /**
   * Get the current state of the writer.
   */
  getState(): ManifestListWriterState {
    return this.state;
  }

  /**
   * Add a manifest entry to the manifest list.
   *
   * @param entry - The manifest file entry to add
   * @throws {ManifestListWriterStateError} If the writer has already been written or closed
   * @throws {ManifestEntryValidationError} If the entry is invalid
   */
  async addManifest(entry: ManifestFileEntry): Promise<void> {
    this.ensureWritable();
    validateManifestEntry(entry);

    // Apply default sequence number from options if not provided
    const entryToAdd = this.applyDefaultSequenceNumbers(entry);

    this.manifests.push(entryToAdd);
    this.state = 'writing';
  }

  /**
   * Ensures the writer is in a writable state.
   * @throws {ManifestListWriterStateError} If the writer has already been written or closed
   */
  private ensureWritable(): void {
    if (this.state === 'written' || this.state === 'closed') {
      throw new ManifestListWriterStateError('Cannot add manifest: writer has already written');
    }
  }

  /**
   * Apply default sequence numbers from options if not provided in entry.
   */
  private applyDefaultSequenceNumbers(entry: ManifestFileEntry): ManifestFileEntry {
    const entryToAdd: ManifestFileEntry = { ...entry };
    if (entryToAdd.sequence_number === undefined && this.options.sequenceNumber !== undefined) {
      entryToAdd.sequence_number = this.options.sequenceNumber;
    }
    if (entryToAdd.min_sequence_number === undefined && this.options.sequenceNumber !== undefined) {
      entryToAdd.min_sequence_number = this.options.sequenceNumber;
    }
    return entryToAdd;
  }

  /**
   * Write the manifest list to storage.
   *
   * @param path - Optional path to write to. If not provided, generates a path.
   * @param options - Write options
   * @returns Result containing the path and metadata
   * @throws {ManifestListWriterStateError} If the writer has already been written
   * @throws {ManifestListStorageError} If storage write fails
   */
  async write(path?: string, options?: WriteOptions): Promise<ManifestListWriteResult> {
    if (this.state === 'written' || this.state === 'closed') {
      throw new ManifestListWriterStateError('Cannot write: writer has already written');
    }

    const writePath = path ?? this.generateManifestListPath();
    const manifestsToWrite = this.prepareManifestsForWrite(options);
    const avroData = this.serializeManifests(manifestsToWrite);

    await this.writeToStorage(writePath, avroData);

    this.bytesWritten = avroData.length;
    this.state = 'written';

    return this.buildWriteResult(writePath, manifestsToWrite.length);
  }

  /**
   * Prepare manifests for writing, optionally sorting them.
   */
  private prepareManifestsForWrite(options?: WriteOptions): ManifestFileEntry[] {
    const manifestsToWrite = [...this.manifests];
    if (options?.sortDeletesFirst) {
      manifestsToWrite.sort((a, b) => {
        // Delete manifests first (content=1), then data manifests (content=0)
        if (a.content !== b.content) {
          return b.content - a.content;
        }
        return 0;
      });
    }
    return manifestsToWrite;
  }

  /**
   * Serialize manifests to Avro format.
   */
  private serializeManifests(manifests: ManifestFileEntry[]): Uint8Array {
    const avroSchema = createManifestListSchema();
    const metadata = this.buildAvroMetadata();
    const fileWriter = new AvroFileWriter(avroSchema, metadata);

    if (manifests.length > 0) {
      const encoder = new AvroEncoder();
      for (const manifest of manifests) {
        this.encodeManifestEntry(encoder, manifest);
      }
      fileWriter.addBlock(manifests.length, encoder.toBuffer());
    }

    return fileWriter.toBuffer();
  }

  /**
   * Build Avro file metadata from writer options.
   */
  private buildAvroMetadata(): Map<string, string> {
    const metadata = new Map<string, string>();

    metadata.set('avro.codec', this.options.codec ?? 'snappy');

    if (this.options.formatVersion !== undefined) {
      metadata.set('format-version', String(this.options.formatVersion));
    }
    if (this.options.snapshotId !== undefined) {
      metadata.set('snapshot-id', String(this.options.snapshotId));
    }
    if (this.options.parentSnapshotId !== undefined) {
      metadata.set('parent-snapshot-id', String(this.options.parentSnapshotId));
    }
    if (this.options.sequenceNumber !== undefined) {
      metadata.set('sequence-number', String(this.options.sequenceNumber));
    }
    if (this.options.schemaId !== undefined) {
      metadata.set('schema-id', String(this.options.schemaId));
    }
    if (this.options.tableSchema !== undefined) {
      metadata.set('schema', JSON.stringify(this.options.tableSchema));
    }
    if (this.options.customMetadata) {
      for (const [key, value] of Object.entries(this.options.customMetadata)) {
        metadata.set(key, value);
      }
    }

    return metadata;
  }

  /**
   * Write data to storage.
   * @throws {ManifestListStorageError} If storage write fails
   */
  private async writeToStorage(path: string, data: Uint8Array): Promise<void> {
    try {
      await this.storage.put(path, data);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      throw new ManifestListStorageError('write', message);
    }
  }

  /**
   * Build the write result object.
   */
  private buildWriteResult(writePath: string, manifestCount: number): ManifestListWriteResult {
    const avroSchema = createManifestListSchema();
    const timestampMs = Date.now();

    return {
      path: writePath,
      avroSchema: {
        type: avroSchema.type,
        name: avroSchema.name,
        fields: avroSchema.fields.map((f) => ({ name: f.name, type: f.type })),
      },
      metadata: {
        snapshotId: this.options.snapshotId,
        parentSnapshotId: this.options.parentSnapshotId,
        sequenceNumber: this.options.sequenceNumber,
        formatVersion: this.options.formatVersion,
        schemaId: this.options.schemaId,
        schema: this.options.tableSchema,
        codec: this.options.codec ?? 'snappy',
        manifestCount,
        timestampMs,
        customProperties: this.options.customMetadata,
      },
    };
  }

  /**
   * Read a manifest list from storage.
   *
   * @param path - Path to the manifest list file
   * @returns Array of manifest file entries
   * @throws {ManifestListStorageError} If the file cannot be read
   * @throws {ManifestListError} If the file cannot be parsed
   */
  async readManifestList(path: string): Promise<ManifestFileEntry[]> {
    const data = await this.readFromStorage(path);
    return this.parseAvroManifestList(data);
  }

  /**
   * Read data from storage.
   * @throws {ManifestListStorageError} If storage read fails or file not found
   */
  private async readFromStorage(path: string): Promise<Uint8Array> {
    let data: Uint8Array | null;
    try {
      data = await this.storage.get(path);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      throw new ManifestListStorageError('read', message);
    }

    if (!data) {
      throw new ManifestListStorageError('read', `File not found: ${path}`);
    }

    return data;
  }

  /**
   * Get statistics about the manifest list.
   *
   * @returns Statistics about manifests, files, and rows
   */
  getStatistics(): ManifestListStatistics {
    let totalAddedDataFiles = 0;
    let totalExistingDataFiles = 0;
    let totalDeletedDataFiles = 0;
    let totalAddedRows = BigInt(0);
    let totalExistingRows = BigInt(0);
    let totalDeletedRows = BigInt(0);
    let dataManifestCount = 0;
    let deleteManifestCount = 0;

    for (const manifest of this.manifests) {
      totalAddedDataFiles += manifest.added_data_files_count;
      totalExistingDataFiles += manifest.existing_data_files_count;
      totalDeletedDataFiles += manifest.deleted_data_files_count;
      totalAddedRows += manifest.added_rows_count;
      totalExistingRows += manifest.existing_rows_count;
      totalDeletedRows += manifest.deleted_rows_count;

      if (manifest.content === MANIFEST_CONTENT_DATA) {
        dataManifestCount++;
      } else {
        deleteManifestCount++;
      }
    }

    return {
      totalManifestCount: this.manifests.length,
      dataManifestCount,
      deleteManifestCount,
      totalAddedDataFiles,
      totalExistingDataFiles,
      totalDeletedDataFiles,
      totalAddedRows,
      totalExistingRows,
      totalDeletedRows,
      bytesWritten: this.bytesWritten,
    };
  }

  /**
   * Close the writer and release resources.
   */
  async close(): Promise<void> {
    this.state = 'closed';
  }

  /**
   * Generate a manifest list path following Iceberg conventions.
   */
  private generateManifestListPath(): string {
    const snapshotId = this.options.snapshotId ?? BigInt(Date.now());
    const uuid = generateUUID();
    return `metadata/snap-${snapshotId}-${uuid}.avro`;
  }

  /**
   * Encode a manifest entry to Avro format.
   */
  private encodeManifestEntry(encoder: AvroEncoder, entry: ManifestFileEntry): void {
    // manifest_path (string)
    encoder.writeString(entry.manifest_path);

    // manifest_length (long)
    encoder.writeLong(entry.manifest_length);

    // partition_spec_id (int)
    encoder.writeInt(entry.partition_spec_id);

    // content (int)
    encoder.writeInt(entry.content);

    // sequence_number (long)
    encoder.writeLong(entry.sequence_number);

    // min_sequence_number (long)
    encoder.writeLong(entry.min_sequence_number);

    // added_snapshot_id (long)
    encoder.writeLong(entry.added_snapshot_id);

    // added_data_files_count (int)
    encoder.writeInt(entry.added_data_files_count);

    // existing_data_files_count (int)
    encoder.writeInt(entry.existing_data_files_count);

    // deleted_data_files_count (int)
    encoder.writeInt(entry.deleted_data_files_count);

    // added_rows_count (long)
    encoder.writeLong(entry.added_rows_count);

    // existing_rows_count (long)
    encoder.writeLong(entry.existing_rows_count);

    // deleted_rows_count (long)
    encoder.writeLong(entry.deleted_rows_count);

    // partitions (optional array)
    if (entry.partitions !== undefined && entry.partitions.length > 0) {
      encoder.writeUnionIndex(1);
      encoder.writeArray(entry.partitions, (partition) => {
        // contains_null (boolean)
        encoder.writeBoolean(partition.contains_null);

        // contains_nan (optional boolean)
        if (partition.contains_nan !== undefined) {
          encoder.writeUnionIndex(1);
          encoder.writeBoolean(partition.contains_nan);
        } else {
          encoder.writeUnionIndex(0);
        }

        // lower_bound (optional bytes)
        if (partition.lower_bound !== undefined) {
          encoder.writeUnionIndex(1);
          encoder.writeBytes(partition.lower_bound);
        } else {
          encoder.writeUnionIndex(0);
        }

        // upper_bound (optional bytes)
        if (partition.upper_bound !== undefined) {
          encoder.writeUnionIndex(1);
          encoder.writeBytes(partition.upper_bound);
        } else {
          encoder.writeUnionIndex(0);
        }
      });
    } else if (entry.partitions !== undefined) {
      // Empty array case
      encoder.writeUnionIndex(1);
      encoder.writeArray([], () => {});
    } else {
      encoder.writeUnionIndex(0);
    }

    // key_metadata (optional bytes)
    if (entry.key_metadata !== undefined) {
      encoder.writeUnionIndex(1);
      encoder.writeBytes(entry.key_metadata);
    } else {
      encoder.writeUnionIndex(0);
    }
  }

  /**
   * Parse Avro manifest list data into manifest entries.
   * @throws {ManifestListError} If the file format is invalid
   */
  private parseAvroManifestList(data: Uint8Array): ManifestFileEntry[] {
    this.validateAvroMagic(data);

    const entries: ManifestFileEntry[] = [];
    let offset = this.skipAvroHeader(data);

    // Read data blocks
    while (offset < data.length) {
      // Check if we've reached the end
      if (offset >= data.length - 16) break;

      // Read block count
      const blockCount = readVarLong(data, offset);
      offset = blockCount.newOffset;

      if (blockCount.value === 0n) break;

      // Read block size
      const blockSize = readVarLong(data, offset);
      offset = blockSize.newOffset;

      // Parse records in block
      const recordCount = Number(blockCount.value);
      for (let i = 0; i < recordCount; i++) {
        const result = this.parseManifestEntry(data, offset);
        entries.push(result.entry);
        offset = result.newOffset;
      }

      // Skip sync marker
      offset += 16;
    }

    return entries;
  }

  /**
   * Validate Avro file magic bytes.
   * @throws {ManifestListError} If magic bytes are missing or invalid
   */
  private validateAvroMagic(data: Uint8Array): void {
    const magic = new TextDecoder().decode(data.slice(0, 3));
    if (magic !== 'Obj' || data[3] !== 1) {
      throw new ManifestListError('Invalid Avro file: missing magic bytes');
    }
  }

  /**
   * Skip the Avro header and return the offset to the first data block.
   */
  private skipAvroHeader(data: Uint8Array): number {
    let offset = 4; // Skip magic

    // Read metadata block count
    const metadataBlockCount = readVarLong(data, offset);
    offset = metadataBlockCount.newOffset;

    // Skip metadata entries
    for (let i = 0; i < Math.abs(Number(metadataBlockCount.value)); i++) {
      // Read key
      const keyLen = readVarLong(data, offset);
      offset = keyLen.newOffset + Number(keyLen.value);
      // Read value
      const valLen = readVarLong(data, offset);
      offset = valLen.newOffset + Number(valLen.value);
    }

    // Read terminating zero for metadata block
    if (metadataBlockCount.value !== 0n) {
      const zero = readVarLong(data, offset);
      offset = zero.newOffset;
    }

    // Skip sync marker (16 bytes)
    offset += 16;

    return offset;
  }

  /**
   * Parse a single manifest entry from Avro data.
   */
  private parseManifestEntry(data: Uint8Array, offset: number): { entry: ManifestFileEntry; newOffset: number } {
    // manifest_path (string)
    const manifestPath = readString(data, offset);
    offset = manifestPath.newOffset;

    // manifest_length (long)
    const manifestLength = readVarLong(data, offset);
    offset = manifestLength.newOffset;

    // partition_spec_id (int)
    const partitionSpecId = readVarInt(data, offset);
    offset = partitionSpecId.newOffset;

    // content (int)
    const content = readVarInt(data, offset);
    offset = content.newOffset;

    // sequence_number (long)
    const sequenceNumber = readVarLong(data, offset);
    offset = sequenceNumber.newOffset;

    // min_sequence_number (long)
    const minSequenceNumber = readVarLong(data, offset);
    offset = minSequenceNumber.newOffset;

    // added_snapshot_id (long)
    const addedSnapshotId = readVarLong(data, offset);
    offset = addedSnapshotId.newOffset;

    // added_data_files_count (int)
    const addedDataFilesCount = readVarInt(data, offset);
    offset = addedDataFilesCount.newOffset;

    // existing_data_files_count (int)
    const existingDataFilesCount = readVarInt(data, offset);
    offset = existingDataFilesCount.newOffset;

    // deleted_data_files_count (int)
    const deletedDataFilesCount = readVarInt(data, offset);
    offset = deletedDataFilesCount.newOffset;

    // added_rows_count (long)
    const addedRowsCount = readVarLong(data, offset);
    offset = addedRowsCount.newOffset;

    // existing_rows_count (long)
    const existingRowsCount = readVarLong(data, offset);
    offset = existingRowsCount.newOffset;

    // deleted_rows_count (long)
    const deletedRowsCount = readVarLong(data, offset);
    offset = deletedRowsCount.newOffset;

    // partitions (optional array)
    let partitions: PartitionFieldSummary[] | undefined;
    const partitionsUnionIndex = readVarInt(data, offset);
    offset = partitionsUnionIndex.newOffset;

    if (partitionsUnionIndex.value === 1) {
      const arrayResult = readArray(data, offset, (d, o) => this.parsePartitionSummary(d, o));
      partitions = arrayResult.items;
      offset = arrayResult.newOffset;
    }

    // key_metadata (optional bytes)
    let keyMetadata: Uint8Array | undefined;
    const keyMetadataUnionIndex = readVarInt(data, offset);
    offset = keyMetadataUnionIndex.newOffset;

    if (keyMetadataUnionIndex.value === 1) {
      const bytes = readBytes(data, offset);
      keyMetadata = bytes.value;
      offset = bytes.newOffset;
    }

    const entry: ManifestFileEntry = {
      manifest_path: manifestPath.value,
      manifest_length: Number(manifestLength.value),
      partition_spec_id: partitionSpecId.value,
      content: content.value as ManifestContent,
      sequence_number: sequenceNumber.value,
      min_sequence_number: minSequenceNumber.value,
      added_snapshot_id: addedSnapshotId.value,
      added_data_files_count: addedDataFilesCount.value,
      existing_data_files_count: existingDataFilesCount.value,
      deleted_data_files_count: deletedDataFilesCount.value,
      added_rows_count: addedRowsCount.value,
      existing_rows_count: existingRowsCount.value,
      deleted_rows_count: deletedRowsCount.value,
      partitions,
      key_metadata: keyMetadata,
    };

    return { entry, newOffset: offset };
  }

  /**
   * Parse a partition field summary from Avro data.
   */
  private parsePartitionSummary(data: Uint8Array, offset: number): { item: PartitionFieldSummary; newOffset: number } {
    // contains_null (boolean)
    const containsNull = data[offset] !== 0;
    offset++;

    // contains_nan (optional boolean)
    let containsNan: boolean | undefined;
    const containsNanUnionIndex = readVarInt(data, offset);
    offset = containsNanUnionIndex.newOffset;
    if (containsNanUnionIndex.value === 1) {
      containsNan = data[offset] !== 0;
      offset++;
    }

    // lower_bound (optional bytes)
    let lowerBound: Uint8Array | undefined;
    const lowerBoundUnionIndex = readVarInt(data, offset);
    offset = lowerBoundUnionIndex.newOffset;
    if (lowerBoundUnionIndex.value === 1) {
      const bytes = readBytes(data, offset);
      lowerBound = bytes.value;
      offset = bytes.newOffset;
    }

    // upper_bound (optional bytes)
    let upperBound: Uint8Array | undefined;
    const upperBoundUnionIndex = readVarInt(data, offset);
    offset = upperBoundUnionIndex.newOffset;
    if (upperBoundUnionIndex.value === 1) {
      const bytes = readBytes(data, offset);
      upperBound = bytes.value;
      offset = bytes.newOffset;
    }

    const item: PartitionFieldSummary = {
      contains_null: containsNull,
      contains_nan: containsNan,
      lower_bound: lowerBound,
      upper_bound: upperBound,
    };

    return { item, newOffset: offset };
  }
}
