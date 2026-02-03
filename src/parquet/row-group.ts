/**
 * Row Group Serializer
 *
 * Serializes a batch of documents into a Parquet row group format.
 * - Organizes data by columns
 * - Tracks row group metadata (row count, byte size)
 * - Supports promoted columns (native Parquet types)
 * - Encodes remaining fields as variant
 * - Generates column statistics for zone maps
 */

import {
  PARQUET_MAGIC_BYTES,
  PARQUET_MAGIC_SIZE,
} from '../constants.js';

import {
  compress,
  supportedCodecs,
  type CompressionCodec,
} from './compression.js';

import type {
  ParquetPhysicalType,
  RepetitionType,
  ColumnStatistics,
} from './types.js';

// Re-export compression types for backwards compatibility
export type { CompressionCodec } from './compression.js';

// Re-export types from types.ts for backwards compatibility
export type { ParquetPhysicalType, RepetitionType, ColumnStatistics } from './types.js';

// ============================================================================
// Type Definitions
// ============================================================================

/** Encoding type for columns */
export type ColumnEncoding = 'PLAIN' | 'RLE' | 'DELTA_BINARY_PACKED' | 'DELTA_LENGTH_BYTE_ARRAY';

/** Data types for columns */
export type ColumnDataType =
  | 'string'
  | 'int64'
  | 'double'
  | 'boolean'
  | 'timestamp'
  | 'binary'
  | 'variant';

/** Column chunk metadata */
export interface ColumnChunk {
  columnName: string;
  dataType: ColumnDataType;
  fileOffset: number;
  compressedSize: number;
  uncompressedSize: number;
  numValues: number;
  encoding: ColumnEncoding;
  compression: CompressionCodec;
  statistics?: ColumnStatistics;
}

/** Schema element for row group metadata */
export interface RowGroupSchemaElement {
  name: string;
  type: ParquetPhysicalType;
  repetitionType: RepetitionType;
}

/** Column metadata */
export interface ColumnMetadata {
  name: string;
  type: ColumnDataType;
}

/** Row group metadata */
export interface RowGroupMetadata {
  numRows: number;
  totalByteSize: number;
  columns: ColumnMetadata[];
  schema: RowGroupSchemaElement[];
  sortingColumns?: string[];
}

/** Serialized row group result */
export interface SerializedRowGroup {
  rowCount: number;
  columnChunks: ColumnChunk[];
  data: Uint8Array;
  compression: CompressionCodec;
  metadata: RowGroupMetadata;
}

/** Row group serializer options */
export interface RowGroupSerializerOptions {
  compression?: CompressionCodec;
  sortingColumns?: string[];
}

// ============================================================================
// Helper Types
// ============================================================================

interface ColumnData {
  name: string;
  type: ColumnDataType;
  values: unknown[];
  nullCount: number;
  distinctValues: Set<unknown>;
  /** True if type was set to variant due to a conflict between concrete types */
  typeIsCoerced: boolean;
}

// ============================================================================
// Row Group Serializer Implementation
// ============================================================================

/**
 * Serializes documents into Parquet row group format
 */
export class RowGroupSerializer {
  private readonly compression: CompressionCodec;
  private readonly sortingColumns?: string[];

  constructor(options?: RowGroupSerializerOptions) {
    const codec = options?.compression ?? 'snappy';

    // Validate that the compression codec is supported
    if (!supportedCodecs.includes(codec)) {
      throw new Error(`Unsupported compression codec '${codec}'. Must be one of: ${supportedCodecs.join(', ')}`);
    }

    this.compression = codec;
    this.sortingColumns = options?.sortingColumns;
  }

  /**
   * Serialize an array of documents into a Parquet row group
   */
  serialize<T extends Record<string, unknown>>(documents: T[]): SerializedRowGroup {
    // Handle empty documents
    if (documents.length === 0) {
      return this.serializeEmpty();
    }

    // Validate required fields
    this.validateDocuments(documents);

    // Collect column data
    const columns = this.collectColumns(documents);

    // Serialize column data
    const { columnChunks, data } = this.serializeColumns(columns);

    // Build metadata
    const metadata = this.buildMetadata(documents.length, columnChunks, columns);

    return {
      rowCount: documents.length,
      columnChunks,
      data,
      compression: this.compression,
      metadata,
    };
  }

  /**
   * Serialize empty row group
   */
  private serializeEmpty(): SerializedRowGroup {
    // Create minimal Parquet header with PAR1 magic
    const encoder = new TextEncoder();
    const data = encoder.encode(PARQUET_MAGIC_BYTES);

    return {
      rowCount: 0,
      columnChunks: [],
      data,
      compression: this.compression,
      metadata: {
        numRows: 0,
        totalByteSize: data.length,
        columns: [],
        schema: [],
        sortingColumns: this.sortingColumns,
      },
    };
  }

  /**
   * Validate that all documents have required fields and valid values.
   * Throws descriptive error on first validation failure.
   */
  private validateDocuments<T extends Record<string, unknown>>(documents: T[]): void {
    const requiredFields = ['_id', '_seq', '_op'] as const;
    const validOps = ['i', 'u', 'd'];

    for (let i = 0; i < documents.length; i++) {
      const document = documents[i]!;

      // Check required system fields
      for (const field of requiredFields) {
        if (document[field] === undefined) {
          throw new Error(`Document at index ${i} is missing required field '${field}'`);
        }
      }

      // Validate _op value is one of the allowed operations
      if (!validOps.includes(String(document._op))) {
        throw new Error(
          `Document at index ${i} has invalid '_op' value '${document._op}'. Must be one of: ${validOps.join(', ')}`
        );
      }
    }
  }

  /**
   * Collect and organize column data from documents.
   * Returns a map of column name to column data with inferred types and values.
   * Two passes: first to discover fields and infer types, second to populate values.
   */
  private collectColumns<T extends Record<string, unknown>>(documents: T[]): Map<string, ColumnData> {
    const columns = new Map<string, ColumnData>();

    // First pass: discover all fields and infer their types
    for (const document of documents) {
      for (const [fieldName, fieldValue] of Object.entries(document)) {
        if (!columns.has(fieldName)) {
          columns.set(fieldName, {
            name: fieldName,
            type: this.inferType(fieldValue),
            values: [],
            nullCount: 0,
            distinctValues: new Set(),
            typeIsCoerced: false,
          });
        }

        const column = columns.get(fieldName)!;
        const valueType = this.inferType(fieldValue);

        if (fieldValue !== null && fieldValue !== undefined) {
          // Refine 'variant' type based on actual non-null value
          if (column.type === 'variant' && valueType !== 'variant' && !column.typeIsCoerced) {
            column.type = valueType;
          }
          // Handle type conflicts: int64/double can be coerced to double, others become variant
          else if (
            column.type !== 'variant' &&
            valueType !== column.type &&
            valueType !== 'variant'
          ) {
            const canCoerceToDouble =
              (column.type === 'int64' && valueType === 'double') ||
              (column.type === 'double' && valueType === 'int64');

            if (canCoerceToDouble) {
              column.type = 'double';
            } else {
              column.type = 'variant';
              column.typeIsCoerced = true;
            }
          }
        }
      }
    }

    // Second pass: populate column values and track statistics
    for (const document of documents) {
      for (const column of columns.values()) {
        const value = document[column.name];

        if (value === null || value === undefined) {
          column.values.push(null);
          column.nullCount++;
        } else {
          column.values.push(value);
          // Track distinct values for statistics calculation
          column.distinctValues.add(this.getDistinctValueKey(value));
        }
      }
    }

    return columns;
  }

  /**
   * Get a unique key for tracking distinct values
   * Uses JSON for complex types, getTime() for Dates, and direct value otherwise
   */
  private getDistinctValueKey(value: unknown): unknown {
    if (typeof value === 'object' && !(value instanceof Date) && !(value instanceof Uint8Array)) {
      return JSON.stringify(value);
    }
    if (value instanceof Date) {
      return value.getTime();
    }
    return value;
  }

  /**
   * Infer the Parquet data type from a JavaScript value.
   * Returns 'variant' for unknown or complex types.
   * Note: Numbers are distinguished by integer vs decimal: int64 vs double.
   */
  private inferType(value: unknown): ColumnDataType {
    // Null/undefined handled as variant to allow later type refinement
    if (value === null || value === undefined) {
      return 'variant';
    }

    if (typeof value === 'string') {
      return 'string';
    }

    if (typeof value === 'boolean') {
      return 'boolean';
    }

    if (typeof value === 'bigint') {
      return 'int64';
    }

    if (typeof value === 'number') {
      // Distinguish between integer and decimal numbers
      return Number.isInteger(value) ? 'int64' : 'double';
    }

    if (value instanceof Date) {
      return 'timestamp';
    }

    if (value instanceof Uint8Array) {
      return 'binary';
    }

    // Arrays, objects, and other complex types encoded as JSON via variant
    return 'variant';
  }

  /**
   * Serialize all columns to binary format with PAR1 magic header.
   * Produces binary data with magic bytes followed by column data in sequence.
   */
  private serializeColumns(columns: Map<string, ColumnData>): {
    columnChunks: ColumnChunk[];
    data: Uint8Array;
  } {
    const chunks: { chunk: ColumnChunk; data: Uint8Array }[] = [];
    let currentFileOffset = PARQUET_MAGIC_SIZE;

    // Serialize each column and track file offsets
    for (const column of columns.values()) {
      const { chunk, data } = this.serializeColumn(column, currentFileOffset);
      chunks.push({ chunk, data });
      currentFileOffset += chunk.compressedSize;
    }

    // Build final buffer with PAR1 header and all column data
    const totalSize = PARQUET_MAGIC_SIZE + chunks.reduce((sum, c) => sum + c.data.length, 0);
    const buffer = new Uint8Array(totalSize);

    // Write PAR1 magic bytes at offset 0
    buffer[0] = 0x50; // 'P'
    buffer[1] = 0x41; // 'A'
    buffer[2] = 0x52; // 'R'
    buffer[3] = 0x31; // '1'

    // Write column data sequentially after magic bytes
    let bufferOffset = PARQUET_MAGIC_SIZE;
    for (const { data } of chunks) {
      buffer.set(data, bufferOffset);
      bufferOffset += data.length;
    }

    return {
      columnChunks: chunks.map((c) => c.chunk),
      data: buffer,
    };
  }

  /**
   * Serialize a single column and generate its metadata chunk
   */
  private serializeColumn(
    column: ColumnData,
    fileOffset: number
  ): { chunk: ColumnChunk; data: Uint8Array } {
    // Encode column values to binary format
    const uncompressedData = this.encodeColumnValues(column);

    // Apply compression codec
    const compressedData = compress(uncompressedData, this.compression);

    // Generate statistics for zone mapping
    const statistics = this.calculateStatistics(column);

    // Select appropriate encoding based on data type
    const encoding = this.selectEncoding(column.type);

    const chunk: ColumnChunk = {
      columnName: column.name,
      dataType: column.type,
      fileOffset,
      compressedSize: compressedData.length,
      uncompressedSize: uncompressedData.length,
      numValues: column.values.length,
      encoding,
      compression: this.compression,
      statistics,
    };

    return { chunk, data: compressedData };
  }

  /**
   * Encode all column values to binary format.
   * Format: [null bitmap] [non-null values only]
   *
   * The null bitmap has ceil(numValues / 8) bytes, where each bit indicates
   * if the value at that position is present (1) or null (0).
   * Only non-null values are encoded after the bitmap.
   */
  private encodeColumnValues(column: ColumnData): Uint8Array {
    const numValues = column.values.length;
    const bitmapSize = Math.ceil(numValues / 8);

    // Build null bitmap: bit i is set if value i is NOT null
    const bitmap = new Uint8Array(bitmapSize);
    for (let i = 0; i < numValues; i++) {
      if (column.values[i] !== null && column.values[i] !== undefined) {
        const byteIndex = Math.floor(i / 8);
        const bitIndex = i % 8;
        bitmap[byteIndex]! |= 1 << bitIndex;
      }
    }

    // Encode only non-null values
    const encodedParts: Uint8Array[] = [];
    for (const value of column.values) {
      if (value !== null && value !== undefined) {
        encodedParts.push(this.encodeValue(value, column.type));
      }
    }

    // Calculate total buffer size: bitmap + encoded values
    const valuesSize = encodedParts.reduce((sum, part) => sum + part.length, 0);
    const buffer = new Uint8Array(bitmapSize + valuesSize);

    // Write bitmap first
    buffer.set(bitmap, 0);

    // Copy all encoded parts into the buffer after the bitmap
    let bufferOffset = bitmapSize;
    for (const encodedPart of encodedParts) {
      buffer.set(encodedPart, bufferOffset);
      bufferOffset += encodedPart.length;
    }

    return buffer;
  }

  /**
   * Encode a single non-null value to binary format based on its type.
   * Variable-length types: [length:4 bytes LE][data]
   * Fixed-length types: direct encoding (8 bytes for numbers, 1 byte for boolean)
   *
   * Note: Nulls are handled by the bitmap in encodeColumnValues, so this method
   * should only be called for non-null values.
   */
  private encodeValue(value: unknown, type: ColumnDataType): Uint8Array {
    switch (type) {
      case 'string': {
        const encoder = new TextEncoder();
        const bytes = encoder.encode(String(value));
        const result = new Uint8Array(4 + bytes.length);
        const view = new DataView(result.buffer);
        // Store length as 4-byte little-endian integer
        view.setUint32(0, bytes.length, true);
        result.set(bytes, 4);
        return result;
      }

      case 'int64': {
        // Convert to BigInt if needed, truncate fractional part if necessary
        const num = typeof value === 'bigint' ? value : BigInt(Math.trunc(Number(value)));
        const result = new Uint8Array(8);
        const view = new DataView(result.buffer);
        view.setBigInt64(0, num, true);
        return result;
      }

      case 'double': {
        const result = new Uint8Array(8);
        const view = new DataView(result.buffer);
        view.setFloat64(0, Number(value), true);
        return result;
      }

      case 'boolean': {
        return new Uint8Array([value ? 0x01 : 0x00]);
      }

      case 'timestamp': {
        // Encode as milliseconds since epoch in 8 bytes
        const time = value instanceof Date ? value.getTime() : Number(value);
        const result = new Uint8Array(8);
        const view = new DataView(result.buffer);
        view.setBigInt64(0, BigInt(time), true);
        return result;
      }

      case 'binary': {
        // Encode binary data with 4-byte length prefix
        const bytes = value instanceof Uint8Array ? value : new Uint8Array(0);
        const result = new Uint8Array(4 + bytes.length);
        const view = new DataView(result.buffer);
        view.setUint32(0, bytes.length, true);
        result.set(bytes, 4);
        return result;
      }

      case 'variant': {
        // Complex types are JSON-encoded as strings with 4-byte length prefix
        const encoder = new TextEncoder();
        const bytes = encoder.encode(JSON.stringify(value));
        const result = new Uint8Array(4 + bytes.length);
        const view = new DataView(result.buffer);
        view.setUint32(0, bytes.length, true);
        result.set(bytes, 4);
        return result;
      }

      default:
        // Fallback for unknown types
        return new Uint8Array([0x00]);
    }
  }

  /**
   * Calculate column statistics for zone mapping and pruning.
   * Includes min/max values, null count, and distinct value count.
   * Only non-null values are used for min/max calculations.
   */
  private calculateStatistics(column: ColumnData): ColumnStatistics {
    const nonNullValues = column.values.filter((v) => v !== null && v !== undefined);

    const stats: ColumnStatistics = {
      nullCount: column.nullCount,
      distinctCount: column.distinctValues.size,
    };

    // Without non-null values, only null count is meaningful
    if (nonNullValues.length === 0) {
      return stats;
    }

    // Calculate min/max based on column type for zone map pruning
    switch (column.type) {
      case 'string':
        stats.minValue = this.findMin(nonNullValues as string[], (a, b) => a.localeCompare(b));
        stats.maxValue = this.findMax(nonNullValues as string[], (a, b) => a.localeCompare(b));
        break;

      case 'int64': {
        // Normalize BigInt and number values to numeric comparison
        const numericValues = (nonNullValues as (number | bigint)[]).map((v) =>
          typeof v === 'bigint' ? Number(v) : v
        );
        stats.minValue = Math.min(...numericValues);
        stats.maxValue = Math.max(...numericValues);
        break;
      }

      case 'double':
        stats.minValue = Math.min(...(nonNullValues as number[]));
        stats.maxValue = Math.max(...(nonNullValues as number[]));
        break;

      case 'boolean':
        // For booleans, track which values exist (useful for filtering)
        stats.minValue = nonNullValues.some((v) => v === false) ? false : true;
        stats.maxValue = nonNullValues.some((v) => v === true) ? true : false;
        break;

      case 'timestamp': {
        // Convert to Date objects for consistent statistics representation
        const times = (nonNullValues as Date[]).map((d) => d.getTime());
        stats.minValue = new Date(Math.min(...times));
        stats.maxValue = new Date(Math.max(...times));
        break;
      }

      default:
        // Variant and binary types don't provide meaningful min/max
        break;
    }

    return stats;
  }

  /**
   * Find minimum value in array using custom comparator.
   * Comparator should return negative if first arg < second arg.
   */
  private findMin<T>(values: T[], compare: (a: T, b: T) => number): T {
    return values.reduce((min, val) => (compare(val, min) < 0 ? val : min));
  }

  /**
   * Find maximum value in array using custom comparator.
   * Comparator should return positive if first arg > second arg.
   */
  private findMax<T>(values: T[], compare: (a: T, b: T) => number): T {
    return values.reduce((max, val) => (compare(val, max) > 0 ? val : max));
  }

  /**
   * Select encoding based on data type
   */
  private selectEncoding(type: ColumnDataType): ColumnEncoding {
    switch (type) {
      case 'boolean':
        return 'RLE';
      case 'int64':
        return 'DELTA_BINARY_PACKED';
      case 'string':
        return 'DELTA_LENGTH_BYTE_ARRAY';
      default:
        return 'PLAIN';
    }
  }

  /**
   * Build metadata for the row group.
   * Combines column chunks, schema information, and sorting configuration.
   */
  private buildMetadata(
    rowCount: number,
    columnChunks: ColumnChunk[],
    columns: Map<string, ColumnData>
  ): RowGroupMetadata {
    // Sum compressed sizes of all columns for total row group size
    const totalByteSize = columnChunks.reduce((sum, chunk) => sum + chunk.compressedSize, 0);

    // Extract column metadata from chunks
    const columnMetadata: ColumnMetadata[] = columnChunks.map((chunk) => ({
      name: chunk.columnName,
      type: chunk.dataType,
    }));

    // Build schema with Parquet physical types and repetition rules
    const schema: RowGroupSchemaElement[] = Array.from(columns.values()).map((column) => ({
      name: column.name,
      type: this.mapToParquetType(column.type),
      repetitionType: this.isRequiredColumn(column.name) ? 'REQUIRED' : 'OPTIONAL',
    }));

    return {
      numRows: rowCount,
      totalByteSize,
      columns: columnMetadata,
      schema,
      sortingColumns: this.sortingColumns,
    };
  }

  /**
   * Map internal data type to Parquet physical type.
   * String, binary, and variant all map to BYTE_ARRAY in Parquet.
   */
  private mapToParquetType(type: ColumnDataType): ParquetPhysicalType {
    switch (type) {
      case 'boolean':
        return 'BOOLEAN';
      case 'int64':
        return 'INT64';
      case 'double':
        return 'DOUBLE';
      case 'string':
      case 'binary':
      case 'variant':
        // Variable-length data is always BYTE_ARRAY in Parquet
        return 'BYTE_ARRAY';
      case 'timestamp':
        // Timestamps stored as int64 (milliseconds since epoch)
        return 'INT64';
      default:
        return 'BYTE_ARRAY';
    }
  }

  /**
   * Check if a column is required (non-nullable) in Parquet schema.
   * System columns (_id, _seq, _op) are always required by MongoLake.
   */
  private isRequiredColumn(columnName: string): boolean {
    return columnName === '_id' || columnName === '_seq' || columnName === '_op';
  }
}
