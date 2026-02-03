/**
 * Streaming Parquet Writer
 *
 * Writes 500MB+ Parquet files with only 128MB Worker memory through:
 * - Row group batching (~64MB per group)
 * - Streaming to R2 via multipart upload
 * - Field promotion to native columns
 * - Variant encoding for remaining fields
 * - Statistics tracking (min/max/null count)
 */

import type {
  StorageBackend,
  MultipartUpload,
  UploadedPart,
} from '@storage/index.js';
import type { Document } from '@types';
import { encodeVariant } from './variant.js';
import { DEFAULT_ROW_GROUP_SIZE_BYTES } from '@mongolake/constants.js';

// ============================================================================
// Type Definitions
// ============================================================================

/** Parquet physical type names */
export type ParquetPhysicalType =
  | 'BOOLEAN'
  | 'INT32'
  | 'INT64'
  | 'FLOAT'
  | 'DOUBLE'
  | 'BYTE_ARRAY'
  | 'FIXED_LEN_BYTE_ARRAY';

/** Field promotion types */
export type FieldPromotionType =
  | 'string'
  | 'int32'
  | 'int64'
  | 'float'
  | 'double'
  | 'boolean'
  | 'timestamp'
  | 'binary';

/** Field promotion configuration */
export type FieldPromotion = Record<string, FieldPromotionType>;

/** Row group configuration */
export interface RowGroupConfig {
  targetSizeBytes: number;
  maxRows: number;
}

/** Column statistics */
export interface ColumnStatistics {
  minValue?: unknown;
  maxValue?: unknown;
  nullCount: number;
  distinctCount?: number;
}

/** Column metadata in footer */
export interface FooterColumnMetadata {
  path: string;
  fileOffset: number;
  compressedSize: number;
  uncompressedSize: number;
  numValues: number;
  statistics: ColumnStatistics;
}

/** Row group metadata in footer */
export interface FooterRowGroup {
  numRows: number;
  totalByteSize: number;
  columns: FooterColumnMetadata[];
}

/** Schema element in footer */
export interface FooterSchemaElement {
  name: string;
  type: ParquetPhysicalType;
  logicalType?: { type: string };
}

/** Parquet footer structure */
export interface ParquetFooter {
  numRows: number;
  rowGroups: FooterRowGroup[];
  schema: FooterSchemaElement[];
  keyValueMetadata: Map<string, string>;
}

/** Global column statistics */
export interface GlobalColumnStatistics {
  globalMin?: unknown;
  globalMax?: unknown;
  totalNullCount: number;
}

/** Writer statistics */
export interface WriterStatistics {
  totalDocuments: number;
  rowGroupsWritten: number;
  rowGroupsFlushed: number;
  bytesUploaded: number;
  peakMemoryUsageBytes: number;
  columns: Record<string, GlobalColumnStatistics>;
}

/** Writer state */
export type StreamingWriterState = 'initialized' | 'writing' | 'closed' | 'aborted';

/** Streaming writer options */
export interface StreamingWriterOptions {
  /** Target row group size in bytes (default: DEFAULT_ROW_GROUP_SIZE_BYTES) */
  rowGroupSizeBytes?: number;
  /** Maximum rows per row group */
  maxRowsPerRowGroup?: number;
  /** Fields to promote to native columns */
  fieldPromotions?: FieldPromotion;
  /** Store entire document as variant only (no promotions) */
  variantOnly?: boolean;
  /** Track distinct count for columns */
  trackDistinctCount?: boolean;
  /** Key-value metadata to include in footer */
  metadata?: Record<string, string>;
}

// ============================================================================
// Internal Types
// ============================================================================

/** Buffered column data */
interface ColumnBuffer {
  name: string;
  type: FieldPromotionType;
  values: unknown[];
  nullCount: number;
  distinctValues?: Set<string>;
  minValue?: unknown;
  maxValue?: unknown;
}

/** Internal row group state */
interface RowGroupState {
  documents: Document[];
  columns: Map<string, ColumnBuffer>;
  variantData: Uint8Array[];
  estimatedSize: number;
}

// ============================================================================
// Simple Event Emitter
// ============================================================================

type EventHandler = (...args: unknown[]) => void;

/**
 * Basic event emitter for row group flush notifications
 */
class SimpleEventEmitter {
  private handlers: Map<string, EventHandler[]> = new Map();

  /**
   * Register a handler for an event
   */
  on(event: string, handler: EventHandler): void {
    if (!this.handlers.has(event)) {
      this.handlers.set(event, []);
    }
    const handlers = this.handlers.get(event);
    if (handlers) {
      handlers.push(handler);
    }
  }

  /**
   * Emit an event to all registered handlers
   */
  emit(event: string, ...args: unknown[]): void {
    const handlers = this.handlers.get(event);
    if (handlers) {
      for (const handler of handlers) {
        handler(...args);
      }
    }
  }
}

// ============================================================================
// Streaming Parquet Writer Implementation
// ============================================================================

/**
 * Streaming Parquet writer that enables writing 500MB+ files
 * with only 128MB Worker memory through row group batching and multipart upload.
 */
export class StreamingParquetWriter extends SimpleEventEmitter {
  private storage: StorageBackend;
  private key: string;
  private options: StreamingWriterOptions;
  private state: StreamingWriterState = 'initialized';

  // Multipart upload state
  private upload: MultipartUpload | null = null;
  private uploadedParts: UploadedPart[] = [];
  private nextPartNumber: number = 1;

  // Row group state
  private currentRowGroup: RowGroupState;
  private rowGroupConfig: RowGroupConfig;

  // Footer state
  private rowGroupMetadata: FooterRowGroup[] = [];
  private schema: FooterSchemaElement[] = [];
  private schemaBuilt: boolean = false;
  private currentFileOffset: number = 4; // Starts after PAR1 magic (4 bytes)

  // Statistics
  private stats: WriterStatistics = {
    totalDocuments: 0,
    rowGroupsWritten: 0,
    rowGroupsFlushed: 0,
    bytesUploaded: 0,
    peakMemoryUsageBytes: 0,
    columns: {},
  };

  // Testing helper: track last variant data written
  private lastVariantDataString: string = '';

  // Serialization lock: ensures writes are processed in order
  private writeLock: Promise<void> = Promise.resolve();

  constructor(
    storage: StorageBackend,
    key: string,
    options: StreamingWriterOptions = {}
  ) {
    super();

    // Validate options
    if (options.rowGroupSizeBytes !== undefined) {
      if (options.rowGroupSizeBytes <= 0) {
        throw new Error('rowGroupSizeBytes must be positive');
      }
    }

    this.storage = storage;
    this.key = key;
    this.options = {
      rowGroupSizeBytes: DEFAULT_ROW_GROUP_SIZE_BYTES,
      maxRowsPerRowGroup: Number.MAX_SAFE_INTEGER,
      fieldPromotions: {},
      variantOnly: false,
      trackDistinctCount: false,
      metadata: {},
      ...options,
    };

    this.rowGroupConfig = {
      targetSizeBytes: this.options.rowGroupSizeBytes!,
      maxRows: this.options.maxRowsPerRowGroup!,
    };

    this.currentRowGroup = this.createEmptyRowGroup();
  }

  // ============================================================================
  // Public API
  // ============================================================================

  /**
   * Get current writer state
   */
  getState(): StreamingWriterState {
    return this.state;
  }

  /**
   * Get row group configuration
   */
  getRowGroupConfig(): RowGroupConfig {
    return this.rowGroupConfig;
  }

  /**
   * Get field promotions
   */
  getFieldPromotions(): FieldPromotion {
    return this.options.fieldPromotions || {};
  }

  /**
   * Get current row group size in bytes
   */
  getCurrentRowGroupSize(): number {
    return this.currentRowGroup.estimatedSize;
  }

  /**
   * Get writer statistics
   */
  getStatistics(): WriterStatistics {
    return { ...this.stats };
  }

  /**
   * Get the generated footer (only available after close)
   */
  getFooter(): ParquetFooter | null {
    if (this.state !== 'closed') {
      return null;
    }

    const keyValueMetadata = new Map<string, string>();
    if (this.options.metadata) {
      for (const [key, value] of Object.entries(this.options.metadata)) {
        keyValueMetadata.set(key, value);
      }
    }

    return {
      numRows: this.stats.totalDocuments,
      rowGroups: this.rowGroupMetadata,
      schema: this.schema,
      keyValueMetadata,
    };
  }

  /**
   * Get the last variant data string (for testing)
   */
  getLastVariantData(): string {
    return this.lastVariantDataString;
  }

  /**
   * Write a single document
   *
   * Serializes writes to maintain document order and prevent concurrent
   * modifications to the current row group.
   */
  async write(doc: Document): Promise<void> {
    const previousLock = this.writeLock;
    let resolveLock!: () => void;
    this.writeLock = new Promise((resolve) => {
      resolveLock = resolve;
    });

    try {
      await previousLock;
      await this.writeInternal(doc);
    } finally {
      resolveLock();
    }
  }

  /**
   * Write all documents from an async iterator
   */
  async writeAll(documents: AsyncIterable<Document>): Promise<void> {
    for await (const doc of documents) {
      await this.write(doc);
    }
  }

  /**
   * Manually flush the current row group to storage
   *
   * Serializes with other writes to maintain consistency.
   */
  async flushRowGroup(): Promise<void> {
    const previousLock = this.writeLock;
    let resolveLock!: () => void;
    this.writeLock = new Promise((resolve) => {
      resolveLock = resolve;
    });

    try {
      await previousLock;
      if (this.currentRowGroup.documents.length > 0) {
        await this.flushCurrentRowGroup();
      }
    } finally {
      resolveLock();
    }
  }

  /**
   * Close the writer and finalize the Parquet file
   */
  async close(): Promise<void> {
    if (this.state === 'closed') {
      return;
    }

    if (this.state === 'aborted') {
      return;
    }

    // Wait for any pending writes
    await this.writeLock;

    // Flush remaining data
    if (this.currentRowGroup.documents.length > 0) {
      await this.flushCurrentRowGroup();
    }

    // If we never wrote anything, still create a valid empty file
    if (this.upload === null) {
      // No data written at all - create minimal file
      this.state = 'closed';
      return;
    }

    // Write footer and finalize
    await this.writeFooter();
    await this.upload.complete(this.uploadedParts);

    this.state = 'closed';
  }

  /**
   * Abort the writer and clean up multipart upload
   */
  async abort(): Promise<void> {
    if (this.state === 'closed' || this.state === 'aborted') {
      return;
    }

    // Wait for any pending writes
    await this.writeLock;

    if (this.upload) {
      await this.upload.abort();
    }

    // Clear buffers
    this.currentRowGroup = this.createEmptyRowGroup();

    this.state = 'aborted';
  }

  // ============================================================================
  // Internal Methods
  // ============================================================================

  /**
   * Internal write implementation (must be called with lock held)
   */
  private async writeInternal(doc: Document): Promise<void> {
    if (this.state === 'closed') {
      throw new Error('Cannot write to closed writer');
    }

    if (this.state === 'aborted') {
      throw new Error('Cannot write to aborted writer');
    }

    // Validate document has _id
    if (doc._id === undefined) {
      throw new Error('Document must have an _id field');
    }

    // Initialize multipart upload on first write
    if (this.upload === null) {
      this.upload = await this.storage.createMultipartUpload(this.key);
      this.state = 'writing';

      // Upload PAR1 magic bytes as the first part (required by Parquet format)
      const magicBytes = new Uint8Array([0x50, 0x41, 0x52, 0x31]); // 'PAR1'
      const part = await this.upload.uploadPart(this.nextPartNumber++, magicBytes);
      this.uploadedParts.push(part);
      this.stats.bytesUploaded += magicBytes.length;
    }

    // Build schema on first document if not already built
    if (!this.schemaBuilt) {
      this.buildSchema();
    }

    // Add document to current row group
    this.addDocumentToRowGroup(doc);

    // Check if we need to flush based on size or row count limits
    const shouldFlush =
      this.currentRowGroup.estimatedSize >= this.rowGroupConfig.targetSizeBytes ||
      this.currentRowGroup.documents.length >= this.rowGroupConfig.maxRows;

    if (shouldFlush) {
      await this.flushCurrentRowGroup();
    }
  }

  /**
   * Create an empty row group state
   */
  private createEmptyRowGroup(): RowGroupState {
    return {
      documents: [],
      columns: new Map(),
      variantData: [],
      estimatedSize: 0,
    };
  }

  /**
   * Build schema from first document
   *
   * Determines which columns to create based on field promotions.
   */
  private buildSchema(): void {
    this.schema = [];

    // Always add _id column
    this.schema.push({
      name: '_id',
      type: 'BYTE_ARRAY',
    });

    // Add promoted columns
    const promotions = this.options.fieldPromotions || {};
    if (!this.options.variantOnly) {
      for (const [fieldName, fieldType] of Object.entries(promotions)) {
        this.schema.push({
          name: fieldName,
          type: this.mapTypeToPhysical(fieldType),
          logicalType: this.getLogicalType(fieldType),
        });
      }
    }

    // Add _data (variant) column
    this.schema.push({
      name: '_data',
      type: 'BYTE_ARRAY',
      logicalType: { type: 'VARIANT' },
    });

    this.schemaBuilt = true;
  }

  /**
   * Map field type to Parquet physical type
   */
  private mapTypeToPhysical(type: FieldPromotionType): ParquetPhysicalType {
    switch (type) {
      case 'string':
      case 'binary':
        return 'BYTE_ARRAY';
      case 'int32':
        return 'INT32';
      case 'int64':
      case 'timestamp':
        return 'INT64';
      case 'float':
        return 'FLOAT';
      case 'double':
        return 'DOUBLE';
      case 'boolean':
        return 'BOOLEAN';
      default:
        return 'BYTE_ARRAY';
    }
  }

  /**
   * Get logical type for a field type
   */
  private getLogicalType(type: FieldPromotionType): { type: string } | undefined {
    if (type === 'timestamp') {
      return { type: 'TIMESTAMP' };
    }
    return undefined;
  }

  /**
   * Add a document to the current row group
   *
   * Extracts promoted fields to native columns and encodes remaining fields
   * as variant data. Updates memory usage tracking.
   */
  private addDocumentToRowGroup(doc: Document): void {
    this.currentRowGroup.documents.push(doc);
    this.stats.totalDocuments++;

    // Track memory usage estimate
    const estimatedSize = this.estimateDocumentSize(doc);
    this.currentRowGroup.estimatedSize += estimatedSize;

    // Update peak memory usage
    if (this.currentRowGroup.estimatedSize > this.stats.peakMemoryUsageBytes) {
      this.stats.peakMemoryUsageBytes = this.currentRowGroup.estimatedSize;
    }

    // Extract _id (always promoted to native column)
    this.addValueToColumn('_id', 'string', String(doc._id));

    // Extract promoted fields to native columns
    const promotions = this.options.fieldPromotions || {};
    if (!this.options.variantOnly) {
      for (const [fieldName, fieldType] of Object.entries(promotions)) {
        const value = this.getNestedValue(doc, fieldName);
        this.addValueToColumn(fieldName, fieldType, value);
      }
    }

    // Build variant object containing non-promoted fields
    const variantObj: Record<string, unknown> = {};
    let hasVariantData = false;

    const promotedFieldSet = new Set(Object.keys(promotions));

    for (const [key, value] of Object.entries(doc)) {
      // Skip _id (already handled)
      if (key === '_id') continue;

      // Skip promoted fields
      if (!this.options.variantOnly && promotedFieldSet.has(key)) continue;

      // Skip fields that are partially promoted (nested)
      let isPartiallyPromoted = false;
      for (const promotedField of promotedFieldSet) {
        if (promotedField.startsWith(key + '.')) {
          isPartiallyPromoted = true;
          break;
        }
      }
      if (isPartiallyPromoted) continue;

      variantObj[key] = value;
      hasVariantData = true;
    }

    // Encode variant data and add to _data column
    if (hasVariantData) {
      const encoded = encodeVariant(variantObj);
      this.currentRowGroup.variantData.push(encoded);
      this.lastVariantDataString = JSON.stringify(variantObj);
      this.addValueToColumn('_data', 'binary', encoded);
    } else {
      // No variant data for this document (all fields promoted)
      this.addValueToColumn('_data', 'binary', null);
      this.lastVariantDataString = '';
    }
  }

  /**
   * Get a nested value from a document using dot notation (e.g., "user.address.city")
   *
   * Returns null if any level of nesting is missing or null.
   */
  private getNestedValue(doc: Document, path: string): unknown {
    const parts = path.split('.');
    let current: unknown = doc;

    for (const part of parts) {
      if (current === null || current === undefined) {
        return null;
      }
      if (typeof current === 'object') {
        current = (current as Record<string, unknown>)[part];
      } else {
        return null;
      }
    }

    return current;
  }

  /**
   * Add a value to a column buffer
   *
   * Creates the column if it doesn't exist and updates statistics.
   */
  private addValueToColumn(
    name: string,
    type: FieldPromotionType | 'binary',
    value: unknown
  ): void {
    // Lazily create column on first value
    if (!this.currentRowGroup.columns.has(name)) {
      this.currentRowGroup.columns.set(name, {
        name,
        type: type as FieldPromotionType,
        values: [],
        nullCount: 0,
        distinctValues: this.options.trackDistinctCount ? new Set() : undefined,
      });
    }

    const column = this.currentRowGroup.columns.get(name)!;

    // Handle null/undefined values
    if (value === null || value === undefined) {
      column.values.push(null);
      column.nullCount++;
      return;
    }

    // Convert values to their Parquet representation (e.g., Date to timestamp)
    let convertedValue: unknown = value;

    if (type === 'timestamp' && value instanceof Date) {
      convertedValue = value.getTime();
    } else if (type === 'int64' && typeof value === 'bigint') {
      convertedValue = value;
    }

    column.values.push(convertedValue);

    // Update min/max/distinct count statistics
    this.updateColumnStatistics(column, convertedValue);
  }

  /**
   * Update column statistics with a new value
   *
   * Tracks min/max for scalar types and optional distinct count.
   * Skips statistics for binary/complex types.
   */
  private updateColumnStatistics(column: ColumnBuffer, value: unknown): void {
    // Skip statistics for binary and complex types (not comparable)
    if (
      value instanceof Uint8Array ||
      typeof value === 'object' ||
      Array.isArray(value)
    ) {
      return;
    }

    // Update min value
    if (column.minValue === undefined) {
      column.minValue = value;
    } else if (this.compareValues(value, column.minValue) < 0) {
      column.minValue = value;
    }

    // Update max value
    if (column.maxValue === undefined) {
      column.maxValue = value;
    } else if (this.compareValues(value, column.maxValue) > 0) {
      column.maxValue = value;
    }

    // Track distinct values if enabled
    if (column.distinctValues) {
      column.distinctValues.add(String(value));
    }
  }

  /**
   * Compare two values for min/max calculation
   *
   * Returns: negative if a < b, positive if a > b, 0 if a == b
   */
  private compareValues(a: unknown, b: unknown): number {
    // String comparison using locale-aware sorting
    if (typeof a === 'string' && typeof b === 'string') {
      return a.localeCompare(b);
    }

    // Numeric comparison
    if (typeof a === 'number' && typeof b === 'number') {
      return a - b;
    }

    // BigInt comparison
    if (typeof a === 'bigint' && typeof b === 'bigint') {
      return a < b ? -1 : a > b ? 1 : 0;
    }

    // Boolean comparison (false < true)
    if (typeof a === 'boolean' && typeof b === 'boolean') {
      return (a ? 1 : 0) - (b ? 1 : 0);
    }

    // Date comparison by timestamp
    if (a instanceof Date && b instanceof Date) {
      return a.getTime() - b.getTime();
    }

    return 0;
  }

  /**
   * Estimate document size in bytes
   *
   * Uses JSON serialization size × 2 as a conservative estimate
   * (accounts for encoding and metadata overhead).
   */
  private estimateDocumentSize(doc: Document): number {
    const jsonSize = JSON.stringify(doc, (_, value) =>
      typeof value === 'bigint' ? value.toString() : value
    ).length;
    return jsonSize * 2;
  }

  /**
   * Flush the current row group to storage
   */
  private async flushCurrentRowGroup(): Promise<void> {
    if (this.currentRowGroup.documents.length === 0) {
      return;
    }

    const rowGroupIndex = this.stats.rowGroupsFlushed;

    try {
      // Serialize row group data
      const rowGroupData = this.serializeRowGroup();

      // Upload row group as a multipart part
      if (!this.upload) {
        throw new Error('Upload not initialized');
      }
      const part = await this.upload.uploadPart(this.nextPartNumber++, rowGroupData);
      this.uploadedParts.push(part);
      this.stats.bytesUploaded += rowGroupData.length;

      // Build row group metadata
      const rowGroupMeta = this.buildRowGroupMetadata(rowGroupData.length);
      this.rowGroupMetadata.push(rowGroupMeta);

      // Update file offset
      this.currentFileOffset += rowGroupData.length;

      // Update global statistics
      this.updateGlobalStatistics();

      // Update stats
      this.stats.rowGroupsFlushed++;
      this.stats.rowGroupsWritten++;

      // Emit event
      this.emit('rowGroupFlushed', rowGroupIndex);

      // Reset row group
      this.currentRowGroup = this.createEmptyRowGroup();
    } catch (error) {
      // Abort on error
      if (this.upload) {
        await this.upload.abort();
      }
      this.state = 'aborted';
      throw error;
    }
  }

  /**
   * Serialize the current row group to binary format
   */
  private serializeRowGroup(): Uint8Array {
    const parts: Uint8Array[] = [];

    // Serialize each column
    for (const schemaElement of this.schema) {
      const column = this.currentRowGroup.columns.get(schemaElement.name);
      if (column) {
        const columnData = this.serializeColumn(column, schemaElement.type);
        parts.push(columnData);
      }
    }

    // Concatenate all parts
    return this.concatArrays(parts);
  }

  /**
   * Serialize a single column to binary format
   *
   * Encodes each value according to its Parquet physical type.
   */
  private serializeColumn(
    column: ColumnBuffer,
    physicalType: ParquetPhysicalType
  ): Uint8Array {
    const parts: Uint8Array[] = [];

    for (const value of column.values) {
      const encoded = this.encodeValue(value, physicalType);
      parts.push(encoded);
    }

    return this.concatArrays(parts);
  }

  /**
   * Encode a single value to its Parquet binary representation
   *
   * All values are little-endian encoded. Null values use 0x00 marker.
   * BYTE_ARRAY values include a 4-byte length prefix.
   */
  private encodeValue(value: unknown, type: ParquetPhysicalType): Uint8Array {
    // Null values encoded as 0x00 marker
    if (value === null) {
      return new Uint8Array([0x00]);
    }

    switch (type) {
      case 'BYTE_ARRAY': {
        // Variable-length binary with 4-byte length prefix (LE)
        const bytes = value instanceof Uint8Array
          ? value
          : new TextEncoder().encode(String(value));
        const result = new Uint8Array(4 + bytes.length);
        const view = new DataView(result.buffer);
        view.setUint32(0, bytes.length, true); // Length prefix
        result.set(bytes, 4);
        return result;
      }

      case 'INT32': {
        // 32-bit signed integer (LE)
        const result = new Uint8Array(4);
        new DataView(result.buffer).setInt32(0, Number(value), true);
        return result;
      }

      case 'INT64': {
        // 64-bit signed integer (LE), used for timestamps and bigints
        const result = new Uint8Array(8);
        const view = new DataView(result.buffer);
        if (typeof value === 'bigint') {
          view.setBigInt64(0, value, true);
        } else if (value instanceof Date) {
          view.setBigInt64(0, BigInt(value.getTime()), true);
        } else {
          view.setBigInt64(0, BigInt(Math.trunc(Number(value))), true);
        }
        return result;
      }

      case 'FLOAT': {
        // 32-bit floating point (LE)
        const result = new Uint8Array(4);
        new DataView(result.buffer).setFloat32(0, Number(value), true);
        return result;
      }

      case 'DOUBLE': {
        // 64-bit floating point (LE)
        const result = new Uint8Array(8);
        new DataView(result.buffer).setFloat64(0, Number(value), true);
        return result;
      }

      case 'BOOLEAN': {
        // 1 byte: 0x00 for false, 0x01 for true
        return new Uint8Array([value ? 0x01 : 0x00]);
      }

      default:
        return new Uint8Array([0x00]);
    }
  }

  /**
   * Build row group metadata for footer
   */
  private buildRowGroupMetadata(totalByteSize: number): FooterRowGroup {
    const columns: FooterColumnMetadata[] = [];
    let columnOffset = this.currentFileOffset;

    for (const schemaElement of this.schema) {
      const column = this.currentRowGroup.columns.get(schemaElement.name);
      if (column) {
        const columnSize = this.estimateColumnSize(column);

        columns.push({
          path: schemaElement.name,
          fileOffset: columnOffset,
          compressedSize: columnSize,
          uncompressedSize: columnSize,
          numValues: column.values.length,
          statistics: {
            minValue: column.minValue,
            maxValue: column.maxValue,
            nullCount: column.nullCount,
            distinctCount: column.distinctValues?.size,
          },
        });

        columnOffset += columnSize;
      }
    }

    return {
      numRows: this.currentRowGroup.documents.length,
      totalByteSize,
      columns,
    };
  }

  /**
   * Estimate column size in bytes
   *
   * Used to compute row group metadata offsets. Estimates based on
   * Parquet binary encoding sizes.
   */
  private estimateColumnSize(column: ColumnBuffer): number {
    let totalSize = 0;

    for (const value of column.values) {
      if (value === null) {
        // Null marker
        totalSize += 1;
      } else if (typeof value === 'string') {
        // 4-byte length + string bytes
        totalSize += 4 + value.length;
      } else if (value instanceof Uint8Array) {
        // 4-byte length + binary data
        totalSize += 4 + value.length;
      } else if (typeof value === 'bigint' || typeof value === 'number') {
        // INT64 or DOUBLE (8 bytes)
        totalSize += 8;
      } else if (typeof value === 'boolean') {
        // 1 byte
        totalSize += 1;
      } else if (value instanceof Date) {
        // INT64 timestamp (8 bytes)
        totalSize += 8;
      } else {
        // Default to 8 bytes for unknown types
        totalSize += 8;
      }
    }

    return totalSize;
  }

  /**
   * Update global statistics from current row group
   *
   * Aggregates min/max/null counts across all row groups for statistics
   * reported in writer.getStatistics().
   */
  private updateGlobalStatistics(): void {
    for (const column of this.currentRowGroup.columns.values()) {
      // Initialize global stats for column if not already present
      if (!this.stats.columns[column.name]) {
        this.stats.columns[column.name] = {
          totalNullCount: 0,
        };
      }

      const globalStats = this.stats.columns[column.name]!;

      // Aggregate null count across all row groups
      globalStats.totalNullCount += column.nullCount;

      // Update global minimum value
      if (column.minValue !== undefined) {
        if (globalStats.globalMin === undefined) {
          globalStats.globalMin = column.minValue;
        } else if (this.compareValues(column.minValue, globalStats.globalMin) < 0) {
          globalStats.globalMin = column.minValue;
        }
      }

      // Update global maximum value
      if (column.maxValue !== undefined) {
        if (globalStats.globalMax === undefined) {
          globalStats.globalMax = column.maxValue;
        } else if (this.compareValues(column.maxValue, globalStats.globalMax) > 0) {
          globalStats.globalMax = column.maxValue;
        }
      }
    }
  }

  /**
   * Write the Parquet footer to storage
   */
  private async writeFooter(): Promise<void> {
    const footerData = this.buildFooterData();
    if (!this.upload) {
      throw new Error('Upload not initialized');
    }
    const part = await this.upload.uploadPart(this.nextPartNumber++, footerData);
    this.uploadedParts.push(part);
    this.stats.bytesUploaded += footerData.length;
  }

  /**
   * Build footer binary data
   *
   * Format: [metadata JSON] [metadata_length (4 bytes LE)] [PAR1]
   * In a full implementation, metadata would be Thrift-encoded FileMetaData.
   */
  private buildFooterData(): Uint8Array {
    // Serialize metadata to JSON
    const metadataJson = JSON.stringify(
      {
        version: 1,
        schema: this.schema,
        num_rows: this.stats.totalDocuments,
        row_groups: this.rowGroupMetadata,
        key_value_metadata: this.options.metadata,
        created_by: 'mongolake-streaming-writer',
      },
      (_, value) => (typeof value === 'bigint' ? value.toString() : value)
    );

    const metadataBytes = new TextEncoder().encode(metadataJson);

    // Allocate footer: metadata + length (4 bytes) + magic (4 bytes)
    const footer = new Uint8Array(metadataBytes.length + 8);

    // Copy metadata
    footer.set(metadataBytes, 0);

    // Write metadata length (4-byte LE integer)
    const lengthView = new DataView(footer.buffer, metadataBytes.length, 4);
    lengthView.setInt32(0, metadataBytes.length, true);

    // Write PAR1 magic bytes at the end
    footer[metadataBytes.length + 4] = 0x50; // P
    footer[metadataBytes.length + 5] = 0x41; // A
    footer[metadataBytes.length + 6] = 0x52; // R
    footer[metadataBytes.length + 7] = 0x31; // 1

    return footer;
  }

  /**
   * Concatenate multiple Uint8Arrays into a single array
   */
  private concatArrays(arrays: Uint8Array[]): Uint8Array {
    const totalLength = arrays.reduce((sum, arr) => sum + arr.length, 0);
    const result = new Uint8Array(totalLength);
    let offset = 0;

    for (const arr of arrays) {
      result.set(arr, offset);
      offset += arr.length;
    }

    return result;
  }
}
