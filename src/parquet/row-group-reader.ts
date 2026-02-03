/**
 * Row Group Reader
 *
 * Reads and deserializes Parquet row groups back to documents.
 * This is the inverse of RowGroupSerializer:
 * - Decompresses column data (snappy, zstd)
 * - Decodes values from binary format
 * - Reconstructs documents from columnar data
 */

import { PARQUET_MAGIC_SIZE } from '../constants.js';

import { decompress } from './compression.js';

import type {
  ColumnChunk,
  ColumnDataType,
  SerializedRowGroup,
} from './row-group.js';

// ============================================================================
// Value Decoding Functions
// ============================================================================

/**
 * Get the fixed size for a data type, or -1 if variable-length.
 */
function getFixedSize(type: ColumnDataType): number {
  switch (type) {
    case 'int64':
    case 'double':
    case 'timestamp':
      return 8;
    case 'boolean':
      return 1;
    default:
      return -1; // Variable-length
  }
}

/**
 * Decode all values from a column's binary data.
 * Format: [null bitmap] [non-null values]
 *
 * The null bitmap has ceil(numValues / 8) bytes, where each bit indicates
 * if the value at that position is present (1) or null (0).
 * Only non-null values follow after the bitmap.
 *
 * @param data - The binary column data
 * @param type - The column data type
 * @param numValues - The number of values (including nulls)
 * @returns Array of decoded values
 */
function decodeColumnValues(
  data: Uint8Array,
  type: ColumnDataType,
  numValues: number
): unknown[] {
  if (numValues === 0) {
    return [];
  }

  // Read the null bitmap
  const bitmapSize = Math.ceil(numValues / 8);
  const bitmap = data.slice(0, bitmapSize);
  const valueData = data.slice(bitmapSize);

  // Decode based on type
  const fixedSize = getFixedSize(type);

  if (fixedSize < 0) {
    return decodeVariableLengthValuesWithBitmap(valueData, type, numValues, bitmap);
  }

  return decodeFixedSizeValuesWithBitmap(valueData, type, numValues, fixedSize, bitmap);
}

/**
 * Check if a value at position i is present (not null) according to the bitmap.
 * Bit i is set if value i is NOT null.
 */
function isValuePresent(bitmap: Uint8Array, index: number): boolean {
  const byteIndex = Math.floor(index / 8);
  const bitIndex = index % 8;
  return (bitmap[byteIndex]! & (1 << bitIndex)) !== 0;
}

/**
 * Decode fixed-size column values using the null bitmap.
 *
 * The bitmap indicates which values are present (non-null).
 * Only non-null values are encoded in the data, in sequence.
 */
function decodeFixedSizeValuesWithBitmap(
  data: Uint8Array,
  type: ColumnDataType,
  numValues: number,
  fixedSize: number,
  bitmap: Uint8Array
): unknown[] {
  const values: unknown[] = [];
  let offset = 0;

  for (let i = 0; i < numValues; i++) {
    if (isValuePresent(bitmap, i)) {
      // Value is present - decode it
      const value = decodeFixedValue(data, offset, type, fixedSize);
      values.push(value);
      offset += fixedSize;
    } else {
      // Value is null
      values.push(null);
    }
  }

  return values;
}

/**
 * Decode a single fixed-size value from binary data.
 */
function decodeFixedValue(
  data: Uint8Array,
  offset: number,
  type: ColumnDataType,
  fixedSize: number
): unknown {
  const view = new DataView(data.buffer, data.byteOffset + offset, fixedSize);

  switch (type) {
    case 'int64': {
      const bigIntValue = view.getBigInt64(0, true);
      if (
        bigIntValue >= BigInt(Number.MIN_SAFE_INTEGER) &&
        bigIntValue <= BigInt(Number.MAX_SAFE_INTEGER)
      ) {
        return Number(bigIntValue);
      }
      return bigIntValue;
    }

    case 'double':
      return view.getFloat64(0, true);

    case 'timestamp': {
      const timestamp = view.getBigInt64(0, true);
      return new Date(Number(timestamp));
    }

    case 'boolean':
      return data[offset] !== 0x00;

    default:
      throw new Error(`Unexpected fixed-size type: ${type}`);
  }
}

/**
 * Decode variable-length column values (string, binary, variant) using the null bitmap.
 *
 * Format:
 * - Non-null: [length:4 bytes LE] [data:length bytes]
 * - Nulls are indicated by the bitmap, not encoded in the data
 */
function decodeVariableLengthValuesWithBitmap(
  data: Uint8Array,
  type: ColumnDataType,
  numValues: number,
  bitmap: Uint8Array
): unknown[] {
  const values: unknown[] = [];
  let offset = 0;

  for (let i = 0; i < numValues; i++) {
    if (!isValuePresent(bitmap, i)) {
      // Value is null according to bitmap
      values.push(null);
      continue;
    }

    // Value is present - decode it
    if (offset + 4 > data.length) {
      // Not enough bytes for length prefix - shouldn't happen with valid data
      values.push(null);
      continue;
    }

    const view = new DataView(data.buffer, data.byteOffset + offset, 4);
    const length = view.getUint32(0, true);

    if (offset + 4 + length > data.length) {
      // Length exceeds remaining data - shouldn't happen with valid data
      values.push(null);
      offset += 4;
      continue;
    }

    const valueBytes = data.slice(offset + 4, offset + 4 + length);
    offset += 4 + length;

    if (type === 'binary') {
      values.push(new Uint8Array(valueBytes));
    } else {
      const decoder = new TextDecoder();
      const str = decoder.decode(valueBytes);

      if (type === 'variant') {
        try {
          values.push(JSON.parse(str));
        } catch {
          values.push(str);
        }
      } else {
        values.push(str);
      }
    }
  }

  return values;
}

// ============================================================================
// Row Group Reader Implementation
// ============================================================================

/**
 * Options for RowGroupReader
 */
export interface RowGroupReaderOptions {
  /** Whether to validate the PAR1 magic bytes (default: true) */
  validateMagic?: boolean;
}

/**
 * Reads and deserializes Parquet row groups back to documents.
 * Inverse of RowGroupSerializer.
 */
export class RowGroupReader {
  private readonly validateMagic: boolean;

  constructor(options?: RowGroupReaderOptions) {
    this.validateMagic = options?.validateMagic ?? true;
  }

  /**
   * Read documents from a serialized row group.
   *
   * @param serialized - The serialized row group data
   * @returns Array of reconstructed documents
   */
  read<T extends Record<string, unknown>>(serialized: SerializedRowGroup): T[] {
    const { rowCount, columnChunks, data } = serialized;

    // Handle empty row groups
    if (rowCount === 0 || columnChunks.length === 0) {
      return [];
    }

    // Validate magic bytes if requested
    if (this.validateMagic && data.length >= PARQUET_MAGIC_SIZE) {
      const magic = new TextDecoder().decode(data.slice(0, PARQUET_MAGIC_SIZE));
      if (magic !== 'PAR1') {
        throw new Error('Invalid Parquet file: missing PAR1 magic bytes');
      }
    }

    // Decode each column
    const columnData = new Map<string, unknown[]>();

    for (const chunk of columnChunks) {
      const columnValues = this.readColumn(data, chunk);
      columnData.set(chunk.columnName, columnValues);
    }

    // Reconstruct documents row by row
    const documents: T[] = [];

    for (let row = 0; row < rowCount; row++) {
      const doc: Record<string, unknown> = {};

      for (const [columnName, values] of columnData) {
        const value = values[row];
        // Only include non-null values in the document
        // (sparse documents - fields with null are not included)
        if (value !== null && value !== undefined) {
          doc[columnName] = value;
        }
      }

      documents.push(doc as T);
    }

    return documents;
  }

  /**
   * Read and decode a single column from the row group data.
   */
  private readColumn(data: Uint8Array, chunk: ColumnChunk): unknown[] {
    // Extract the column's compressed data from the file
    const compressedData = data.slice(
      chunk.fileOffset,
      chunk.fileOffset + chunk.compressedSize
    );

    // Decompress the data
    const uncompressedData = decompress(
      compressedData,
      chunk.compression,
      chunk.uncompressedSize
    );

    // Decode the values
    return decodeColumnValues(uncompressedData, chunk.dataType, chunk.numValues);
  }

  /**
   * Read documents from raw binary data and metadata.
   * Use this when you have the data and metadata separately.
   *
   * @param data - The raw Parquet row group binary data
   * @param columnChunks - Column chunk metadata
   * @param rowCount - Number of rows in the row group
   * @returns Array of reconstructed documents
   */
  readFromBinary<T extends Record<string, unknown>>(
    data: Uint8Array,
    columnChunks: ColumnChunk[],
    rowCount: number
  ): T[] {
    // Create a minimal SerializedRowGroup to reuse the main read method
    const serialized: SerializedRowGroup = {
      rowCount,
      columnChunks,
      data,
      compression: columnChunks[0]?.compression ?? 'none',
      metadata: {
        numRows: rowCount,
        totalByteSize: data.length,
        columns: columnChunks.map((c) => ({ name: c.columnName, type: c.dataType })),
        schema: [],
      },
    };

    return this.read<T>(serialized);
  }
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Verify that serialized data can be read back correctly.
 * Useful for testing round-trip correctness.
 *
 * @param serialized - The serialized row group
 * @returns true if the data appears valid
 */
export function validateRowGroup(serialized: SerializedRowGroup): boolean {
  try {
    const reader = new RowGroupReader();
    const documents = reader.read(serialized);
    return documents.length === serialized.rowCount;
  } catch {
    return false;
  }
}
