/**
 * Parquet Column Writer
 *
 * Writes column data in Parquet format with support for:
 * - Primitive types (string, int32, int64, float, double, boolean, timestamp, binary)
 * - Null handling and definition levels
 * - Dictionary encoding for strings
 * - Statistics generation (min, max, null count, distinct count)
 * - Compression (none, snappy, zstd)
 * - Repetition/definition levels for nested types
 * - Nested structs and arrays/lists
 */

import { compress, type CompressionCodec } from './compression.js';

// Re-export for backwards compatibility
export type { CompressionCodec } from './compression.js';

// ============================================================================
// Type Definitions
// ============================================================================

/** Parquet encoding types */
export type Encoding =
  | 'PLAIN'
  | 'PLAIN_DICTIONARY'
  | 'RLE'
  | 'BIT_PACKED'
  | 'DELTA_BINARY_PACKED'
  | 'DELTA_LENGTH_BYTE_ARRAY'
  | 'DELTA_BYTE_ARRAY'
  | 'RLE_DICTIONARY'
  | 'BYTE_STREAM_SPLIT';

/** Column statistics */
export interface ColumnStatistics {
  minValue?: unknown;
  maxValue?: unknown;
  nullCount?: number;
  distinctCount?: number;
  minByteLength?: number;
  maxByteLength?: number;
  minListLength?: number;
  maxListLength?: number;
  totalElements?: number;
}

/** Parquet data page header */
export interface DataPageHeader {
  type: 'DATA_PAGE' | 'DICTIONARY_PAGE';
  uncompressedPageSize: number;
  compressedPageSize: number;
  numValues: number;
  encoding?: Encoding;
  definitionLevelEncoding?: Encoding;
  repetitionLevelEncoding?: Encoding;
}

/** Written column result */
export interface WrittenColumn {
  columnName: string;
  dataType: string;
  logicalType?: string;
  typeLength?: number;
  numValues: number;
  data: Uint8Array;
  encoding: Encoding;
  compression: CompressionCodec;
  compressedSize: number;
  uncompressedSize: number;
  statistics: ColumnStatistics;
  definitionLevels?: number[];
  repetitionLevels?: number[];
  maxDefinitionLevel?: number;
  maxRepetitionLevel?: number;
  dictionaryPageData?: Uint8Array;
  dictionarySize?: number;
  totalByteSize?: number;
  pageHeader: DataPageHeader;
  children?: WrittenColumn[];
  elementChild?: WrittenColumn;
}

/** Field definition for struct types */
export interface FieldDefinition {
  name: string;
  type: string;
  fields?: FieldDefinition[];
  elementType?: string;
  elementOptions?: ColumnWriterOptions;
}

/** Column writer options */
export interface ColumnWriterOptions {
  compression?: CompressionCodec;
  encoding?: Encoding;
  useDictionary?: boolean;
  nullable?: boolean;
  fixedLength?: number;
  unit?: 'millis' | 'micros' | 'nanos';
  fields?: FieldDefinition[];
  elementType?: string;
  elementOptions?: ColumnWriterOptions;
  optional?: boolean;
}

// ============================================================================
// Base Column Writer
// ============================================================================

/**
 * Abstract base class for column writers
 */
export abstract class ColumnWriter {
  protected columnName: string;
  protected options: ColumnWriterOptions;
  protected values: unknown[] = [];
  protected nullFlags: boolean[] = [];

  constructor(columnName: string, options: ColumnWriterOptions = {}) {
    this.columnName = columnName;
    this.options = {
      ...options,
      compression: options.compression ?? 'none',
      nullable: options.nullable ?? true,
    };
  }

  /**
   * Write a value to the column
   */
  abstract write(value: unknown): void;

  /**
   * Finish writing and return the column data
   */
  abstract finish(): WrittenColumn;

  /**
   * Factory method to create a column writer for a specific type
   */
  static create(
    columnName: string,
    type: string,
    options: ColumnWriterOptions = {}
  ): ColumnWriter {
    switch (type) {
      case 'string':
        return new StringColumnWriter(columnName, options);
      case 'int32':
        return new Int32ColumnWriter(columnName, options);
      case 'int64':
        return new Int64ColumnWriter(columnName, options);
      case 'float':
        return new FloatColumnWriter(columnName, options);
      case 'double':
        return new DoubleColumnWriter(columnName, options);
      case 'boolean':
        return new BooleanColumnWriter(columnName, options);
      case 'timestamp':
        return new TimestampColumnWriter(columnName, options);
      case 'binary':
        return new BinaryColumnWriter(columnName, options);
      case 'struct':
        return new StructColumnWriter(columnName, options);
      case 'list':
        return new ListColumnWriter(columnName, options);
      default:
        throw new Error(`Unknown column type: ${type}`);
    }
  }
}

// ============================================================================
// String Column Writer
// ============================================================================

export class StringColumnWriter extends ColumnWriter {
  private dictionary: Map<string, number> = new Map();
  private dictionaryValues: string[] = [];
  private dictionaryIndices: number[] = [];
  private useDictionary: boolean;

  constructor(columnName: string, options: ColumnWriterOptions = {}) {
    super(columnName, options);
    this.useDictionary = options.useDictionary ?? true;
  }

  write(value: string | null): void {
    this.values.push(value);
    this.nullFlags.push(value === null);

    if (value !== null && this.useDictionary) {
      // Add to dictionary if not seen before
      if (!this.dictionary.has(value)) {
        this.dictionary.set(value, this.dictionaryValues.length);
        this.dictionaryValues.push(value);
      }
      this.dictionaryIndices.push(this.dictionary.get(value)!);
    }
  }

  finish(): WrittenColumn {
    const nullCount = this.nullFlags.filter(Boolean).length;
    const nonNullValues = this.values.filter((v) => v !== null) as string[];

    // Calculate statistics
    let minValue: string | undefined;
    let maxValue: string | undefined;
    for (const v of nonNullValues) {
      if (minValue === undefined || v < minValue) minValue = v;
      if (maxValue === undefined || v > maxValue) maxValue = v;
    }

    const useDictEncoding = this.useDictionary && this.dictionaryValues.length > 0;
    const encoding: Encoding = useDictEncoding ? 'PLAIN_DICTIONARY' : 'PLAIN';

    // Encode data based on encoding type
    let uncompressedData: Uint8Array;
    let dictionaryPageData: Uint8Array | undefined;

    if (useDictEncoding) {
      dictionaryPageData = this.encodeDictionary();
      uncompressedData = this.encodeDictionaryIndices();
    } else {
      uncompressedData = this.encodePlainStrings();
    }

    const uncompressedSize = uncompressedData.length;
    const compressedData = compress(uncompressedData, this.options.compression!);
    const compressedSize = compressedData.length;

    const definitionLevels =
      this.options.nullable !== false ? this.nullFlags.map((isNull) => (isNull ? 0 : 1)) : undefined;

    return {
      columnName: this.columnName,
      dataType: 'BYTE_ARRAY',
      numValues: this.values.length,
      data: compressedData,
      encoding,
      compression: this.options.compression!,
      compressedSize,
      uncompressedSize,
      statistics: {
        minValue,
        maxValue,
        nullCount,
        distinctCount: new Set(nonNullValues).size,
      },
      definitionLevels,
      dictionaryPageData: useDictEncoding ? dictionaryPageData : undefined,
      dictionarySize: useDictEncoding ? this.dictionaryValues.length : undefined,
      pageHeader: {
        type: 'DATA_PAGE',
        uncompressedPageSize: uncompressedSize,
        compressedPageSize: compressedSize,
        numValues: this.values.length,
        encoding,
      },
    };
  }

  private encodeDictionary(): Uint8Array {
    const parts: Uint8Array[] = [];
    for (const val of this.dictionaryValues) {
      const encoded = new TextEncoder().encode(val);
      const lenBuf = new Uint8Array(4);
      new DataView(lenBuf.buffer).setUint32(0, encoded.length, true);
      parts.push(lenBuf, encoded);
    }
    return concatArrays(parts);
  }

  private encodeDictionaryIndices(): Uint8Array {
    const indexData = new Uint8Array(this.values.length * 4);
    const view = new DataView(indexData.buffer);
    let dictIdx = 0;
    for (let i = 0; i < this.values.length; i++) {
      if (this.values[i] !== null) {
        view.setUint32(i * 4, this.dictionaryIndices[dictIdx++]!, true);
      } else {
        view.setUint32(i * 4, 0, true);
      }
    }
    return indexData;
  }

  private encodePlainStrings(): Uint8Array {
    const parts: Uint8Array[] = [];
    for (const v of this.values) {
      const encoded = v !== null ? new TextEncoder().encode(v as string) : new Uint8Array(0);
      const lenBuf = new Uint8Array(4);
      new DataView(lenBuf.buffer).setUint32(0, encoded.length, true);
      parts.push(lenBuf, encoded);
    }
    return concatArrays(parts);
  }
}

// ============================================================================
// Int32 Column Writer
// ============================================================================

const INT32_MIN = -2147483648;
const INT32_MAX = 2147483647;

export class Int32ColumnWriter extends ColumnWriter {
  private encoding: Encoding;

  constructor(columnName: string, options: ColumnWriterOptions = {}) {
    super(columnName, options);
    this.encoding = options.encoding ?? 'PLAIN';
  }

  write(value: number | null): void {
    if (value !== null) {
      if (value < INT32_MIN || value > INT32_MAX) {
        throw new Error(
          `INT32 value out of range: ${value}. Expected range: [${INT32_MIN}, ${INT32_MAX}]`
        );
      }
    }
    this.values.push(value);
    this.nullFlags.push(value === null);
  }

  finish(): WrittenColumn {
    const nullCount = this.nullFlags.filter(Boolean).length;
    const nonNullValues = this.values.filter((v) => v !== null) as number[];

    // Calculate min/max statistics
    let minValue: number | undefined;
    let maxValue: number | undefined;
    for (const v of nonNullValues) {
      if (minValue === undefined || v < minValue) minValue = v;
      if (maxValue === undefined || v > maxValue) maxValue = v;
    }

    // Encode 32-bit integers in little-endian format
    const data = new Uint8Array(this.values.length * 4);
    const view = new DataView(data.buffer);
    for (let i = 0; i < this.values.length; i++) {
      const v = this.values[i];
      view.setInt32(i * 4, v !== null ? (v as number) : 0, true);
    }

    const uncompressedSize = data.length;
    const compressedData = compress(data, this.options.compression!);
    const compressedSize = compressedData.length;

    const definitionLevels =
      this.options.nullable !== false ? this.nullFlags.map((isNull) => (isNull ? 0 : 1)) : undefined;

    return {
      columnName: this.columnName,
      dataType: 'INT32',
      numValues: this.values.length,
      data: compressedData,
      encoding: this.encoding,
      compression: this.options.compression!,
      compressedSize,
      uncompressedSize,
      statistics: {
        minValue,
        maxValue,
        nullCount,
      },
      definitionLevels,
      pageHeader: {
        type: 'DATA_PAGE',
        uncompressedPageSize: uncompressedSize,
        compressedPageSize: compressedSize,
        numValues: this.values.length,
        encoding: this.encoding,
      },
    };
  }
}

// ============================================================================
// Int64 Column Writer
// ============================================================================

export class Int64ColumnWriter extends ColumnWriter {
  private encoding: Encoding;

  constructor(columnName: string, options: ColumnWriterOptions = {}) {
    super(columnName, options);
    this.encoding = options.encoding ?? 'PLAIN';
  }

  write(value: bigint | number | null): void {
    if (value !== null) {
      // Convert number to bigint if needed
      const bigVal = typeof value === 'bigint' ? value : BigInt(value);
      this.values.push(bigVal);
    } else {
      this.values.push(null);
    }
    this.nullFlags.push(value === null);
  }

  finish(): WrittenColumn {
    const nullCount = this.nullFlags.filter(Boolean).length;
    const nonNullValues = this.values.filter((v) => v !== null) as bigint[];

    // Calculate min/max statistics
    let minValue: bigint | undefined;
    let maxValue: bigint | undefined;
    for (const v of nonNullValues) {
      if (minValue === undefined || v < minValue) minValue = v;
      if (maxValue === undefined || v > maxValue) maxValue = v;
    }

    // Encode 64-bit integers in little-endian format
    const data = new Uint8Array(this.values.length * 8);
    const view = new DataView(data.buffer);
    for (let i = 0; i < this.values.length; i++) {
      const v = this.values[i];
      view.setBigInt64(i * 8, v !== null ? (v as bigint) : BigInt(0), true);
    }

    const uncompressedSize = data.length;
    const compressedData = compress(data, this.options.compression!);
    const compressedSize = compressedData.length;

    const definitionLevels =
      this.options.nullable !== false ? this.nullFlags.map((isNull) => (isNull ? 0 : 1)) : undefined;

    return {
      columnName: this.columnName,
      dataType: 'INT64',
      numValues: this.values.length,
      data: compressedData,
      encoding: this.encoding,
      compression: this.options.compression!,
      compressedSize,
      uncompressedSize,
      statistics: {
        minValue,
        maxValue,
        nullCount,
      },
      definitionLevels,
      pageHeader: {
        type: 'DATA_PAGE',
        uncompressedPageSize: uncompressedSize,
        compressedPageSize: compressedSize,
        numValues: this.values.length,
        encoding: this.encoding,
      },
    };
  }
}

// ============================================================================
// Float Column Writer
// ============================================================================

export class FloatColumnWriter extends ColumnWriter {
  constructor(columnName: string, options: ColumnWriterOptions = {}) {
    super(columnName, options);
  }

  write(value: number | null): void {
    this.values.push(value);
    this.nullFlags.push(value === null);
  }

  finish(): WrittenColumn {
    const nullCount = this.nullFlags.filter(Boolean).length;
    const nonNullValues = this.values.filter(
      (v) => v !== null && !Number.isNaN(v as number)
    ) as number[];

    // Calculate min/max, filtering out non-finite values
    let minValue: number | undefined;
    let maxValue: number | undefined;
    for (const v of nonNullValues) {
      if (!Number.isFinite(v)) continue;
      if (minValue === undefined || v < minValue) minValue = v;
      if (maxValue === undefined || v > maxValue) maxValue = v;
    }

    // Normalize min/max to actual float32 precision
    const float32View = new Float32Array(1);
    if (minValue !== undefined) {
      float32View[0] = minValue;
      minValue = float32View[0];
    }
    if (maxValue !== undefined) {
      float32View[0] = maxValue;
      maxValue = float32View[0];
    }

    // Encode 32-bit floating point values in little-endian format
    const data = new Uint8Array(this.values.length * 4);
    const view = new DataView(data.buffer);
    for (let i = 0; i < this.values.length; i++) {
      const v = this.values[i];
      view.setFloat32(i * 4, v !== null ? (v as number) : 0, true);
    }

    const uncompressedSize = data.length;
    const compressedData = compress(data, this.options.compression!);
    const compressedSize = compressedData.length;

    const definitionLevels =
      this.options.nullable !== false ? this.nullFlags.map((isNull) => (isNull ? 0 : 1)) : undefined;

    return {
      columnName: this.columnName,
      dataType: 'FLOAT',
      numValues: this.values.length,
      data: compressedData,
      encoding: 'PLAIN',
      compression: this.options.compression!,
      compressedSize,
      uncompressedSize,
      statistics: {
        minValue,
        maxValue,
        nullCount,
      },
      definitionLevels,
      pageHeader: {
        type: 'DATA_PAGE',
        uncompressedPageSize: uncompressedSize,
        compressedPageSize: compressedSize,
        numValues: this.values.length,
        encoding: 'PLAIN',
      },
    };
  }
}

// ============================================================================
// Double Column Writer
// ============================================================================

export class DoubleColumnWriter extends ColumnWriter {
  constructor(columnName: string, options: ColumnWriterOptions = {}) {
    super(columnName, options);
  }

  write(value: number | null): void {
    this.values.push(value);
    this.nullFlags.push(value === null);
  }

  finish(): WrittenColumn {
    const nullCount = this.nullFlags.filter(Boolean).length;
    const nonNullValues = this.values.filter(
      (v) => v !== null && !Number.isNaN(v as number)
    ) as number[];

    // Calculate min/max, filtering out non-finite values
    let minValue: number | undefined;
    let maxValue: number | undefined;
    for (const v of nonNullValues) {
      if (!Number.isFinite(v)) continue;
      if (minValue === undefined || v < minValue) minValue = v;
      if (maxValue === undefined || v > maxValue) maxValue = v;
    }

    // Encode 64-bit floating point values in little-endian format
    const data = new Uint8Array(this.values.length * 8);
    const view = new DataView(data.buffer);
    for (let i = 0; i < this.values.length; i++) {
      const v = this.values[i];
      view.setFloat64(i * 8, v !== null ? (v as number) : 0, true);
    }

    const uncompressedSize = data.length;
    const compressedData = compress(data, this.options.compression!);
    const compressedSize = compressedData.length;

    const definitionLevels =
      this.options.nullable !== false ? this.nullFlags.map((isNull) => (isNull ? 0 : 1)) : undefined;

    return {
      columnName: this.columnName,
      dataType: 'DOUBLE',
      numValues: this.values.length,
      data: compressedData,
      encoding: 'PLAIN',
      compression: this.options.compression!,
      compressedSize,
      uncompressedSize,
      statistics: {
        minValue,
        maxValue,
        nullCount,
      },
      definitionLevels,
      pageHeader: {
        type: 'DATA_PAGE',
        uncompressedPageSize: uncompressedSize,
        compressedPageSize: compressedSize,
        numValues: this.values.length,
        encoding: 'PLAIN',
      },
    };
  }
}

// ============================================================================
// Boolean Column Writer
// ============================================================================

export class BooleanColumnWriter extends ColumnWriter {
  constructor(columnName: string, options: ColumnWriterOptions = {}) {
    super(columnName, options);
  }

  write(value: boolean | null): void {
    this.values.push(value);
    this.nullFlags.push(value === null);
  }

  finish(): WrittenColumn {
    const nullCount = this.nullFlags.filter(Boolean).length;
    const nonNullValues = this.values.filter((v) => v !== null) as boolean[];

    // Calculate min/max (false < true)
    let minValue: boolean | undefined;
    let maxValue: boolean | undefined;
    for (const v of nonNullValues) {
      if (minValue === undefined || (v === false && minValue === true)) minValue = v;
      if (maxValue === undefined || (v === true && maxValue === false)) maxValue = v;
    }

    // Bit-pack boolean values into bytes
    const numBytes = Math.ceil(this.values.length / 8);
    const data = new Uint8Array(numBytes);
    for (let i = 0; i < this.values.length; i++) {
      if (this.values[i] === true) {
        data[Math.floor(i / 8)]! |= 1 << (i % 8);
      }
    }

    const uncompressedSize = data.length;
    const compressedData = compress(data, this.options.compression!);
    const compressedSize = compressedData.length;

    const definitionLevels =
      this.options.nullable !== false ? this.nullFlags.map((isNull) => (isNull ? 0 : 1)) : undefined;

    return {
      columnName: this.columnName,
      dataType: 'BOOLEAN',
      numValues: this.values.length,
      data: compressedData,
      encoding: 'RLE',
      compression: this.options.compression!,
      compressedSize,
      uncompressedSize,
      statistics: {
        minValue,
        maxValue,
        nullCount,
      },
      definitionLevels,
      pageHeader: {
        type: 'DATA_PAGE',
        uncompressedPageSize: uncompressedSize,
        compressedPageSize: compressedSize,
        numValues: this.values.length,
        encoding: 'RLE',
      },
    };
  }
}

// ============================================================================
// Timestamp Column Writer
// ============================================================================

export class TimestampColumnWriter extends ColumnWriter {
  private unit: 'millis' | 'micros' | 'nanos';

  constructor(columnName: string, options: ColumnWriterOptions = {}) {
    super(columnName, options);
    this.unit = options.unit ?? 'millis';
  }

  write(value: Date | number | null): void {
    if (value === null) {
      this.values.push(null);
    } else if (value instanceof Date) {
      this.values.push(value.getTime());
    } else {
      this.values.push(value);
    }
    this.nullFlags.push(value === null);
  }

  finish(): WrittenColumn {
    const nullCount = this.nullFlags.filter(Boolean).length;
    const nonNullValues = this.values.filter((v) => v !== null) as number[];

    // Calculate min/max timestamp values
    let minValue: number | undefined;
    let maxValue: number | undefined;
    for (const v of nonNullValues) {
      if (minValue === undefined || v < minValue) minValue = v;
      if (maxValue === undefined || v > maxValue) maxValue = v;
    }

    // Encode timestamps as 64-bit integers with unit conversion
    const data = new Uint8Array(this.values.length * 8);
    const view = new DataView(data.buffer);
    for (let i = 0; i < this.values.length; i++) {
      let ts = this.values[i] !== null ? (this.values[i] as number) : 0;
      // Convert from milliseconds to the configured unit
      if (this.unit === 'micros') {
        ts *= 1000;
      } else if (this.unit === 'nanos') {
        ts *= 1000000;
      }
      view.setBigInt64(i * 8, BigInt(ts), true);
    }

    const uncompressedSize = data.length;
    const compressedData = compress(data, this.options.compression!);
    const compressedSize = compressedData.length;

    const definitionLevels =
      this.options.nullable !== false ? this.nullFlags.map((isNull) => (isNull ? 0 : 1)) : undefined;

    const logicalType =
      this.unit === 'micros'
        ? 'TIMESTAMP_MICROS'
        : this.unit === 'nanos'
          ? 'TIMESTAMP_NANOS'
          : 'TIMESTAMP_MILLIS';

    return {
      columnName: this.columnName,
      dataType: 'INT64',
      logicalType,
      numValues: this.values.length,
      data: compressedData,
      encoding: 'PLAIN',
      compression: this.options.compression!,
      compressedSize,
      uncompressedSize,
      statistics: {
        minValue,
        maxValue,
        nullCount,
      },
      definitionLevels,
      pageHeader: {
        type: 'DATA_PAGE',
        uncompressedPageSize: uncompressedSize,
        compressedPageSize: compressedSize,
        numValues: this.values.length,
        encoding: 'PLAIN',
      },
    };
  }
}

// ============================================================================
// Binary Column Writer
// ============================================================================

export class BinaryColumnWriter extends ColumnWriter {
  private fixedLength?: number;
  private totalBytes = 0;

  constructor(columnName: string, options: ColumnWriterOptions = {}) {
    super(columnName, options);
    this.fixedLength = options.fixedLength;
  }

  write(value: Uint8Array | ArrayBuffer | null): void {
    if (value === null) {
      this.values.push(null);
      this.nullFlags.push(true);
      return;
    }

    const data = value instanceof ArrayBuffer ? new Uint8Array(value) : value;

    if (this.fixedLength !== undefined && data.length !== this.fixedLength) {
      throw new Error(
        `Binary data length mismatch: got ${data.length} bytes, expected ${this.fixedLength} bytes`
      );
    }

    this.values.push(data);
    this.nullFlags.push(false);
    this.totalBytes += data.length;
  }

  finish(): WrittenColumn {
    const nullCount = this.nullFlags.filter(Boolean).length;
    const nonNullValues = this.values.filter((v) => v !== null) as Uint8Array[];

    // Calculate min/max byte lengths
    let minByteLength: number | undefined;
    let maxByteLength: number | undefined;
    for (const v of nonNullValues) {
      if (minByteLength === undefined || v.length < minByteLength) minByteLength = v.length;
      if (maxByteLength === undefined || v.length > maxByteLength) maxByteLength = v.length;
    }

    // Encode binary data, prefixing with length unless fixed-length
    const parts: Uint8Array[] = [];
    for (const v of this.values) {
      if (v !== null) {
        const data = v as Uint8Array;
        // Variable-length format requires 4-byte length prefix
        if (this.fixedLength === undefined) {
          const lenBuf = new Uint8Array(4);
          new DataView(lenBuf.buffer).setUint32(0, data.length, true);
          parts.push(lenBuf);
        }
        parts.push(data);
      } else if (this.fixedLength === undefined) {
        // Null values need length prefix of 0
        const lenBuf = new Uint8Array(4);
        new DataView(lenBuf.buffer).setUint32(0, 0, true);
        parts.push(lenBuf);
      }
    }
    const uncompressedData = concatArrays(parts);

    const uncompressedSize = uncompressedData.length;
    const compressedData = compress(uncompressedData, this.options.compression!);
    const compressedSize = compressedData.length;

    const definitionLevels =
      this.options.nullable !== false ? this.nullFlags.map((isNull) => (isNull ? 0 : 1)) : undefined;

    const dataType = this.fixedLength !== undefined ? 'FIXED_LEN_BYTE_ARRAY' : 'BYTE_ARRAY';

    return {
      columnName: this.columnName,
      dataType,
      typeLength: this.fixedLength,
      numValues: this.values.length,
      data: compressedData,
      encoding: 'PLAIN',
      compression: this.options.compression!,
      compressedSize,
      uncompressedSize,
      totalByteSize: this.totalBytes,
      statistics: {
        minByteLength,
        maxByteLength,
        nullCount,
      },
      definitionLevels,
      pageHeader: {
        type: 'DATA_PAGE',
        uncompressedPageSize: uncompressedSize,
        compressedPageSize: compressedSize,
        numValues: this.values.length,
        encoding: 'PLAIN',
      },
    };
  }
}

// ============================================================================
// Struct Column Writer
// ============================================================================

/**
 * Writes nested struct (group) columns for Parquet format.
 *
 * Supports arbitrary nesting depth by recursively creating child writers.
 * Handles definition levels for optional structs and null values.
 *
 * Parquet struct schema follows the format:
 * ```
 * optional group address {
 *   optional binary street (STRING);
 *   optional binary city (STRING);
 *   optional int32 zip;
 * }
 * ```
 *
 * Column names are flattened with dot notation: `address.street`, `address.city`
 */
export class StructColumnWriter extends ColumnWriter {
  private readonly fields: FieldDefinition[];
  private readonly childWriters: Map<string, ColumnWriter> = new Map();
  private readonly isOptional: boolean;

  constructor(columnName: string, options: ColumnWriterOptions = {}) {
    super(columnName, options);
    this.fields = options.fields || [];
    this.isOptional = options.optional ?? false;

    // Validate field definitions
    this.validateFieldDefinitions(this.fields);

    // Create child writers for each field
    for (const field of this.fields) {
      const childWriter = this.createChildWriter(field, options.compression);
      this.childWriters.set(field.name, childWriter);
    }
  }

  /**
   * Validate field definitions for common issues
   */
  private validateFieldDefinitions(fields: FieldDefinition[]): void {
    const seenNames = new Set<string>();
    for (const field of fields) {
      if (!field.name || field.name.trim() === '') {
        throw new Error('Struct field name cannot be empty');
      }
      if (seenNames.has(field.name)) {
        throw new Error(`Duplicate field name in struct: ${field.name}`);
      }
      seenNames.add(field.name);

      // Validate nested struct fields
      if (field.type === 'struct' && field.fields) {
        this.validateFieldDefinitions(field.fields);
      }
    }
  }

  /**
   * Create a child writer for a field definition
   */
  private createChildWriter(field: FieldDefinition, compression?: CompressionCodec): ColumnWriter {
    const childOptions: ColumnWriterOptions = {
      compression,
      nullable: true,
    };

    // Configure options based on field type
    if (field.type === 'struct' && field.fields) {
      childOptions.fields = field.fields;
    } else if (field.type === 'list') {
      if (!field.elementType) {
        throw new Error(`List field '${field.name}' must specify elementType`);
      }
      childOptions.elementType = field.elementType;
      childOptions.elementOptions = field.elementOptions;
    }

    return ColumnWriter.create(field.name, field.type, childOptions);
  }

  /**
   * Recursively update column names to include full parent path.
   * This ensures nested structures have fully-qualified column names
   * like `root.level1.level2.field` for proper Parquet schema representation.
   */
  private updateColumnNamesRecursively(column: WrittenColumn, fullPath: string): void {
    column.columnName = fullPath;

    // Recursively update nested struct children
    if (column.children) {
      for (const child of column.children) {
        // Extract the field name from the current column name (last segment)
        const fieldName = child.columnName.split('.').pop()!;
        const childPath = `${fullPath}.${fieldName}`;
        this.updateColumnNamesRecursively(child, childPath);
      }
    }

    // Update element child for lists (maintains .element suffix convention)
    if (column.elementChild) {
      const elementPath = `${fullPath}.element`;
      this.updateColumnNamesRecursively(column.elementChild, elementPath);
    }
  }

  /**
   * Write a struct value. Pass null for null structs.
   * @param value The struct object or null
   */
  write(value: Record<string, unknown> | null): void {
    this.values.push(value);
    this.nullFlags.push(value === null);

    // Propagate values (or nulls) to all child writers
    for (const field of this.fields) {
      const childWriter = this.childWriters.get(field.name)!;
      const fieldValue = value !== null ? value[field.name] ?? null : null;
      childWriter.write(fieldValue as never);
    }
  }

  finish(): WrittenColumn {
    const nullCount = this.countNulls();

    // Finish all child writers and update their column names
    const children = this.finishChildWriters();

    // Calculate maximum definition level across all descendants
    const maxDefinitionLevel = this.calculateMaxDefinitionLevel(children);

    // Generate definition levels for this struct
    const definitionLevels = this.generateDefinitionLevels();

    // Combine child data (each child is independently encoded)
    const data = this.combineChildData(children);
    const compressedData = compress(data, this.options.compression!);

    return {
      columnName: this.columnName,
      dataType: 'STRUCT',
      numValues: this.values.length,
      data: compressedData,
      encoding: 'PLAIN',
      compression: this.options.compression!,
      compressedSize: compressedData.length,
      uncompressedSize: data.length,
      statistics: { nullCount },
      children,
      definitionLevels,
      maxDefinitionLevel,
      pageHeader: {
        type: 'DATA_PAGE',
        uncompressedPageSize: data.length,
        compressedPageSize: compressedData.length,
        numValues: this.values.length,
        encoding: 'PLAIN',
      },
    };
  }

  private countNulls(): number {
    return this.nullFlags.filter(Boolean).length;
  }

  private finishChildWriters(): WrittenColumn[] {
    const children: WrittenColumn[] = [];
    for (const field of this.fields) {
      const childWriter = this.childWriters.get(field.name)!;
      const childResult = childWriter.finish();
      // Apply full path prefix to column name hierarchy
      this.updateColumnNamesRecursively(childResult, `${this.columnName}.${field.name}`);
      children.push(childResult);
    }
    return children;
  }

  private calculateMaxDefinitionLevel(children: WrittenColumn[]): number {
    let maxLevel = this.isOptional ? 1 : 0;
    for (const child of children) {
      const childMax = child.maxDefinitionLevel ?? 1;
      maxLevel = Math.max(maxLevel, childMax + (this.isOptional ? 1 : 0));
    }
    return maxLevel;
  }

  private generateDefinitionLevels(): number[] | undefined {
    if (!this.isOptional) {
      return undefined;
    }

    const definitionLevels: number[] = [];
    for (let i = 0; i < this.values.length; i++) {
      const value = this.values[i] as Record<string, unknown> | null;
      if (value === null) {
        definitionLevels.push(0); // Struct is null
      } else {
        // Check if first field is present (simplified heuristic)
        const firstField = this.fields[0];
        if (firstField && value[firstField.name] === null) {
          definitionLevels.push(1); // Struct present, field null
        } else {
          definitionLevels.push(2); // Struct and field present
        }
      }
    }
    return definitionLevels;
  }

  private combineChildData(children: WrittenColumn[]): Uint8Array {
    return concatArrays(children.map((c) => c.data));
  }
}

// ============================================================================
// List Column Writer
// ============================================================================

/**
 * Writes list (repeated) columns for Parquet format.
 *
 * Supports nested lists (2D arrays, 3D arrays, etc.) and lists of structs.
 * Handles repetition and definition levels per Parquet spec.
 *
 * Parquet list schema follows the 3-level format:
 * ```
 * optional group tags (LIST) {
 *   repeated group list {
 *     optional binary element (STRING);
 *   }
 * }
 * ```
 *
 * Repetition levels indicate list structure:
 * - 0 = start of a new top-level list
 * - 1 = continuation element in same list
 * - Higher levels for nested lists
 *
 * Definition levels indicate presence:
 * - 0 = list itself is null
 * - 1 = list is present but empty
 * - 2 = element in list is null
 * - 3 = element in list is present
 */
export class ListColumnWriter extends ColumnWriter {
  private readonly elementType: string;
  private readonly elementOptions: ColumnWriterOptions;
  private readonly elementWriter: ColumnWriter;
  private readonly listLengths: number[] = [];
  private readonly isNullable: boolean;
  private totalElements = 0;

  constructor(columnName: string, options: ColumnWriterOptions = {}) {
    super(columnName, options);

    // Validate and set element type
    this.elementType = options.elementType || 'string';
    this.elementOptions = options.elementOptions || {};
    this.isNullable = options.nullable ?? false;

    // Create the element writer for list items
    this.elementWriter = this.createElementWriter(options.compression);
  }

  /**
   * Create a writer for list elements based on element type
   */
  private createElementWriter(compression?: CompressionCodec): ColumnWriter {
    const elementWriterOptions: ColumnWriterOptions = {
      compression,
      nullable: true,
      ...this.elementOptions,
    };

    return ColumnWriter.create('element', this.elementType, elementWriterOptions);
  }

  /**
   * Write a list value. Pass null for null lists, empty array for empty lists.
   * @param value The array of elements or null
   */
  write(value: unknown[] | null): void {
    this.values.push(value);
    this.nullFlags.push(value === null);

    if (value !== null) {
      this.listLengths.push(value.length);
      this.totalElements += value.length;

      // Write each element to the element writer
      for (const elem of value) {
        this.elementWriter.write(elem as never);
      }
    } else {
      // Track null list with zero length
      this.listLengths.push(0);
    }
  }

  finish(): WrittenColumn {
    const nullCount = this.countNulls();
    const { minListLength, maxListLength } = this.calculateListLengthStatistics();

    // Finish the element writer to get encoded element data
    const elementChild = this.elementWriter.finish();

    // Generate repetition and definition levels
    const repetitionLevels = this.generateRepetitionLevels();
    const definitionLevels = this.generateDefinitionLevels();

    // Calculate max repetition level (increases with nesting depth)
    const maxRepetitionLevel = this.calculateMaxRepetitionLevel(elementChild);

    // Apply compression to element data
    const data = elementChild.data;
    const compressedData = compress(data, this.options.compression!);

    return {
      columnName: this.columnName,
      dataType: 'LIST',
      numValues: this.values.length,
      data: compressedData,
      encoding: 'PLAIN',
      compression: this.options.compression!,
      compressedSize: compressedData.length,
      uncompressedSize: data.length,
      statistics: {
        nullCount,
        minListLength,
        maxListLength,
        totalElements: this.totalElements,
      },
      repetitionLevels,
      definitionLevels,
      maxRepetitionLevel,
      elementChild,
      pageHeader: {
        type: 'DATA_PAGE',
        uncompressedPageSize: data.length,
        compressedPageSize: compressedData.length,
        numValues: this.values.length,
        encoding: 'PLAIN',
      },
    };
  }

  private countNulls(): number {
    return this.nullFlags.filter(Boolean).length;
  }

  private calculateListLengthStatistics(): { minListLength?: number; maxListLength?: number } {
    const nonNullLengths = this.listLengths.filter((_, i) => !this.nullFlags[i]);

    if (nonNullLengths.length === 0) {
      return {};
    }

    let minListLength = nonNullLengths[0]!;
    let maxListLength = nonNullLengths[0]!;

    for (const len of nonNullLengths) {
      if (len < minListLength!) minListLength = len;
      if (len > maxListLength!) maxListLength = len;
    }

    return { minListLength, maxListLength };
  }

  /**
   * Generate repetition levels for all elements.
   *
   * Repetition level indicates at what level we're repeating:
   * - 0 = new top-level record (first element of a new list)
   * - 1 = repeating at list level (subsequent elements in same list)
   */
  private generateRepetitionLevels(): number[] {
    const levels: number[] = [];

    for (let i = 0; i < this.values.length; i++) {
      const list = this.values[i] as unknown[] | null;
      if (list !== null && list.length > 0) {
        for (let j = 0; j < list.length; j++) {
          // First element starts a new list (0), others continue (1)
          levels.push(j === 0 ? 0 : 1);
        }
      }
    }

    return levels;
  }

  /**
   * Generate definition levels for nullable lists.
   *
   * Definition level indicates how much of the path is defined:
   * - 0 = list is null
   * - 1 = list is present but empty
   * - 2 = list element is null
   * - 3 = list element is present and non-null
   */
  private generateDefinitionLevels(): number[] | undefined {
    if (!this.isNullable) {
      return undefined;
    }

    const levels: number[] = [];

    for (let i = 0; i < this.values.length; i++) {
      const list = this.values[i] as unknown[] | null;

      if (list === null) {
        levels.push(0); // List is null
      } else if (list.length === 0) {
        levels.push(1); // List exists but is empty
      } else {
        // Generate a level for each element
        for (const elem of list) {
          levels.push(elem === null ? 2 : 3);
        }
      }
    }

    return levels;
  }

  /**
   * Calculate max repetition level, accounting for nested lists.
   * Each level of list nesting adds 1 to the max repetition level.
   */
  private calculateMaxRepetitionLevel(elementChild: WrittenColumn): number {
    let maxLevel = 1; // Base level for this list

    // If element is also a list, add its repetition level
    if (elementChild.maxRepetitionLevel !== undefined) {
      maxLevel += elementChild.maxRepetitionLevel;
    }

    return maxLevel;
  }
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Concatenate multiple Uint8Arrays into a single Uint8Array
 */
function concatArrays(arrays: Uint8Array[]): Uint8Array {
  const totalLength = arrays.reduce((sum, arr) => sum + arr.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const arr of arrays) {
    result.set(arr, offset);
    offset += arr.length;
  }
  return result;
}
