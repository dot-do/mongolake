/**
 * Parquet Footer Generator
 *
 * Generates Parquet file footer metadata including:
 * - File schema
 * - Row group metadata
 * - Column chunk metadata
 * - Column statistics
 *
 * Serialized using Thrift compact protocol with PAR1 magic bytes.
 */

// ============================================================================
// Type Definitions
// ============================================================================

/** Parquet physical types */
export type ParquetType =
  | 'BOOLEAN'
  | 'INT32'
  | 'INT64'
  | 'INT96'
  | 'FLOAT'
  | 'DOUBLE'
  | 'BYTE_ARRAY'
  | 'FIXED_LEN_BYTE_ARRAY';

/** Parquet converted (logical) types */
export type ConvertedType =
  | 'UTF8'
  | 'MAP'
  | 'MAP_KEY_VALUE'
  | 'LIST'
  | 'ENUM'
  | 'DECIMAL'
  | 'DATE'
  | 'TIME_MILLIS'
  | 'TIME_MICROS'
  | 'TIMESTAMP_MILLIS'
  | 'TIMESTAMP_MICROS'
  | 'UINT_8'
  | 'UINT_16'
  | 'UINT_32'
  | 'UINT_64'
  | 'INT_8'
  | 'INT_16'
  | 'INT_32'
  | 'INT_64'
  | 'JSON'
  | 'BSON'
  | 'INTERVAL';

/** Repetition type */
export type RepetitionType = 'REQUIRED' | 'OPTIONAL' | 'REPEATED';

/** Encoding types */
export type Encoding =
  | 'PLAIN'
  | 'RLE'
  | 'BIT_PACKED'
  | 'DELTA_BINARY_PACKED'
  | 'DELTA_LENGTH_BYTE_ARRAY'
  | 'DELTA_BYTE_ARRAY'
  | 'RLE_DICTIONARY'
  | 'BYTE_STREAM_SPLIT';

/** Compression codecs */
export type CompressionCodec = 'none' | 'snappy' | 'gzip' | 'lzo' | 'brotli' | 'lz4' | 'zstd';

/** Column statistics */
export interface ColumnStatistics {
  minValue?: unknown;
  maxValue?: unknown;
  nullCount?: number;
  distinctCount?: number;
}

/** Schema element */
export interface SchemaElement {
  name: string;
  type?: ParquetType;
  typeLength?: number;
  repetitionType?: RepetitionType;
  convertedType?: ConvertedType;
  numChildren?: number;
  fieldId?: number;
  children?: SchemaElement[];
}

/** Column chunk metadata */
export interface ColumnChunkMetadata {
  columnName: string;
  fileOffset: number;
  dataPageOffset?: number;
  dictionaryPageOffset?: number;
  compressedSize: number;
  uncompressedSize: number;
  numValues: number;
  encoding: Encoding;
  compression: CompressionCodec;
  statistics?: ColumnStatistics;
}

/** Row group metadata */
export interface RowGroupMetadata {
  columns: ColumnChunkMetadata[];
  numRows: number;
  totalByteSize: number;
  fileOffset: number;
}

/** Key-value metadata */
export interface KeyValueMetadata {
  key: string;
  value: string;
}

/** Column order */
export interface ColumnOrder {
  columnOrderType: 'TYPE_DEFINED_ORDER' | 'UNDEFINED';
}

/** Footer generator input */
export interface FooterInput {
  schema: SchemaElement[];
  rowGroups: RowGroupMetadata[];
  encryptionAlgorithm?: string;
  columnOrders?: ColumnOrder[];
}

/** Generated footer output */
export interface ParquetFooter {
  data: Uint8Array;
  version: number;
  schema: SchemaElement[];
  numRows: number;
  rowGroups: RowGroupMetadata[];
  createdBy: string;
  keyValueMetadata?: KeyValueMetadata[];
  encryptionAlgorithm?: string;
  columnOrders?: ColumnOrder[];
}

/** Footer generator options */
export interface FooterGeneratorOptions {
  version?: number;
  createdBy?: string;
  keyValueMetadata?: KeyValueMetadata[];
}

// ============================================================================
// Constants
// ============================================================================

const PARQUET_MAGIC = new Uint8Array([0x50, 0x41, 0x52, 0x31]); // "PAR1"

const VALID_PARQUET_TYPES: Set<string> = new Set([
  'BOOLEAN',
  'INT32',
  'INT64',
  'INT96',
  'FLOAT',
  'DOUBLE',
  'BYTE_ARRAY',
  'FIXED_LEN_BYTE_ARRAY',
]);

const PARQUET_TYPE_ENUM: Record<ParquetType, number> = {
  BOOLEAN: 0,
  INT32: 1,
  INT64: 2,
  INT96: 3,
  FLOAT: 4,
  DOUBLE: 5,
  BYTE_ARRAY: 6,
  FIXED_LEN_BYTE_ARRAY: 7,
};

const REPETITION_TYPE_ENUM: Record<RepetitionType, number> = {
  REQUIRED: 0,
  OPTIONAL: 1,
  REPEATED: 2,
};

const CONVERTED_TYPE_ENUM: Record<ConvertedType, number> = {
  UTF8: 0,
  MAP: 1,
  MAP_KEY_VALUE: 2,
  LIST: 3,
  ENUM: 4,
  DECIMAL: 5,
  DATE: 6,
  TIME_MILLIS: 7,
  TIME_MICROS: 8,
  TIMESTAMP_MILLIS: 9,
  TIMESTAMP_MICROS: 10,
  UINT_8: 11,
  UINT_16: 12,
  UINT_32: 13,
  UINT_64: 14,
  INT_8: 15,
  INT_16: 16,
  INT_32: 17,
  INT_64: 18,
  JSON: 19,
  BSON: 20,
  INTERVAL: 21,
};

const ENCODING_ENUM: Record<Encoding, number> = {
  PLAIN: 0,
  RLE: 3,
  BIT_PACKED: 4,
  DELTA_BINARY_PACKED: 5,
  DELTA_LENGTH_BYTE_ARRAY: 6,
  DELTA_BYTE_ARRAY: 7,
  RLE_DICTIONARY: 8,
  BYTE_STREAM_SPLIT: 9,
};

const COMPRESSION_ENUM: Record<CompressionCodec, number> = {
  none: 0,
  snappy: 1,
  gzip: 2,
  lzo: 3,
  brotli: 4,
  lz4: 5,
  zstd: 6,
};

// ============================================================================
// Thrift Compact Protocol Writer
// ============================================================================

/** Thrift compact protocol type IDs */
const THRIFT_TYPE = {
  STOP: 0,
  TRUE: 1,
  FALSE: 2,
  BYTE: 3,
  I16: 4,
  I32: 5,
  I64: 6,
  DOUBLE: 7,
  BINARY: 8,
  LIST: 9,
  SET: 10,
  MAP: 11,
  STRUCT: 12,
};

/**
 * Thrift compact protocol writer for Parquet metadata serialization.
 *
 * Implements the Thrift compact binary protocol with field ID delta compression
 * and zigzag variable-length integer encoding. This is used exclusively for
 * serializing Parquet footer metadata structures.
 */
class ThriftCompactWriter {
  private buffer: number[] = [];
  private lastFieldId: number = 0;
  private fieldIdStack: number[] = [];

  /** Serialize buffer and return as Uint8Array. */
  getBytes(): Uint8Array {
    return new Uint8Array(this.buffer);
  }

  /** Push current field ID context for nested struct. */
  writeStructBegin(): void {
    this.fieldIdStack.push(this.lastFieldId);
    this.lastFieldId = 0;
  }

  /** Pop field ID context and write STOP marker. */
  writeStructEnd(): void {
    this.buffer.push(THRIFT_TYPE.STOP);
    this.lastFieldId = this.fieldIdStack.pop() ?? 0;
  }

  /**
   * Write field header with delta-compressed field ID.
   * Uses 1 byte for small deltas (0-15), 3 bytes for larger ones.
   */
  writeFieldBegin(fieldId: number, thriftType: number): void {
    const delta = fieldId - this.lastFieldId;
    if (delta > 0 && delta <= 15) {
      // Compact form: encode both delta and type in one byte
      this.buffer.push((delta << 4) | thriftType);
    } else {
      // Long form: type byte followed by zigzag i16 for field ID
      this.buffer.push(thriftType);
      this.writeI16(fieldId);
    }
    this.lastFieldId = fieldId;
  }

  /** Write boolean field (encoded as TRUE or FALSE type marker). */
  writeBool(fieldId: number, value: boolean): void {
    const thriftType = value ? THRIFT_TYPE.TRUE : THRIFT_TYPE.FALSE;
    this.writeFieldBegin(fieldId, thriftType);
  }

  /** Write single-byte integer field. */
  writeByte(fieldId: number, value: number): void {
    this.writeFieldBegin(fieldId, THRIFT_TYPE.BYTE);
    this.buffer.push(value & 0xff);
  }

  /** Write 16-bit integer field (zigzag encoded). */
  writeI16Field(fieldId: number, value: number): void {
    this.writeFieldBegin(fieldId, THRIFT_TYPE.I16);
    this.writeI16(value);
  }

  /** Write 32-bit integer field (zigzag encoded). */
  writeI32Field(fieldId: number, value: number): void {
    this.writeFieldBegin(fieldId, THRIFT_TYPE.I32);
    this.writeVarInt(this.zigzagEncode32(value));
  }

  /** Write 64-bit integer field (zigzag encoded). */
  writeI64Field(fieldId: number, value: bigint | number): void {
    this.writeFieldBegin(fieldId, THRIFT_TYPE.I64);
    this.writeVarInt64(this.zigzagEncode64(BigInt(value)));
  }

  /** Write double-precision float field (8 bytes, little-endian). */
  writeDoubleField(fieldId: number, value: number): void {
    this.writeFieldBegin(fieldId, THRIFT_TYPE.DOUBLE);
    const buf = new ArrayBuffer(8);
    const view = new DataView(buf);
    view.setFloat64(0, value, true);
    for (let i = 0; i < 8; i++) {
      this.buffer.push(view.getUint8(i));
    }
  }

  /** Write binary/string field (length-prefixed with varint). */
  writeBinaryField(fieldId: number, value: Uint8Array | string): void {
    this.writeFieldBegin(fieldId, THRIFT_TYPE.BINARY);
    const bytes = typeof value === 'string' ? new TextEncoder().encode(value) : value;
    this.writeVarInt(bytes.length);
    for (let i = 0; i < bytes.length; i++) {
      this.buffer.push(bytes[i]);
    }
  }

  /** Write list field header with element type and size. */
  writeListBegin(fieldId: number, elementType: number, size: number): void {
    this.writeFieldBegin(fieldId, THRIFT_TYPE.LIST);
    if (size < 15) {
      // Compact form: size and element type in one byte
      this.buffer.push((size << 4) | elementType);
    } else {
      // Long form: 0xF0 marker followed by varint size
      this.buffer.push(0xf0 | elementType);
      this.writeVarInt(size);
    }
  }

  /** Write struct field header (used for nested structures). */
  writeStructFieldBegin(fieldId: number): void {
    this.writeFieldBegin(fieldId, THRIFT_TYPE.STRUCT);
  }

  /** Encode i16 value as zigzag-encoded varint. */
  private writeI16(value: number): void {
    this.writeVarInt(this.zigzagEncode32(value));
  }

  /** Encode i32 as variable-length integer (1-5 bytes). */
  private writeVarInt(value: number): void {
    while ((value & ~0x7f) !== 0) {
      this.buffer.push((value & 0x7f) | 0x80);
      value >>>= 7;
    }
    this.buffer.push(value & 0x7f);
  }

  /** Encode i64 as variable-length integer (1-10 bytes). */
  private writeVarInt64(value: bigint): void {
    while ((value & ~0x7fn) !== 0n) {
      this.buffer.push(Number(value & 0x7fn) | 0x80);
      value >>= 7n;
    }
    this.buffer.push(Number(value & 0x7fn));
  }

  /** Convert signed 32-bit int to unsigned via zigzag encoding. */
  private zigzagEncode32(n: number): number {
    return (n << 1) ^ (n >> 31);
  }

  /** Convert signed 64-bit int to unsigned via zigzag encoding. */
  private zigzagEncode64(n: bigint): bigint {
    return (n << 1n) ^ (n >> 63n);
  }

  /** Write raw byte to buffer. */
  writeRawByte(value: number): void {
    this.buffer.push(value & 0xff);
  }

  /** Write raw varint (no field header). */
  writeRawVarInt(value: number): void {
    this.writeVarInt(value);
  }

  /** Write raw varint64 (no field header). */
  writeRawVarInt64(value: bigint): void {
    this.writeVarInt64(value);
  }

  /** Write raw binary data with varint length prefix. */
  writeRawBinary(value: Uint8Array | string): void {
    const bytes = typeof value === 'string' ? new TextEncoder().encode(value) : value;
    this.writeVarInt(bytes.length);
    for (let i = 0; i < bytes.length; i++) {
      this.buffer.push(bytes[i]);
    }
  }

  /** Write zigzag-encoded i32 as varint (no field header). */
  writeZigzagI32(value: number): void {
    this.writeVarInt(this.zigzagEncode32(value));
  }

  /** Write zigzag-encoded i64 as varint (no field header). */
  writeZigzagI64(value: bigint | number): void {
    this.writeVarInt64(this.zigzagEncode64(BigInt(value)));
  }
}

// ============================================================================
// Footer Generator Implementation
// ============================================================================

/**
 * Generates Parquet file footer metadata serialized in Thrift compact protocol.
 *
 * The footer contains critical metadata read before processing file data:
 * - Version number
 * - Complete schema definition (flattened)
 * - Row group and column chunk metadata
 * - Key-value metadata pairs
 * - Created-by attribution
 *
 * Binary format: [Thrift metadata] [length: 4 LE bytes] [magic: "PAR1"]
 */
export class FooterGenerator {
  private readonly version: number;
  private readonly createdBy: string;
  private readonly keyValueMetadata?: KeyValueMetadata[];

  constructor(options?: FooterGeneratorOptions) {
    this.version = options?.version ?? 1;
    this.createdBy = options?.createdBy ?? 'mongolake-parquet';
    this.keyValueMetadata = options?.keyValueMetadata;
  }

  /**
   * Generate and serialize footer metadata to binary Parquet format.
   *
   * Performs validation, flattens schema, serializes via Thrift compact protocol,
   * and wraps with length field and magic marker.
   */
  generate(input: FooterInput): ParquetFooter {
    this.validate(input);

    const flattenedSchema = this.flattenSchema(input.schema);
    const numRows = input.rowGroups.reduce((sum, rg) => sum + rg.numRows, 0);

    // Serialize metadata to Thrift compact protocol
    const writer = new ThriftCompactWriter();
    this.writeFileMetaData(writer, input, flattenedSchema, numRows);
    const thriftBytes = writer.getBytes();

    // Assemble final footer: metadata + length field + magic bytes
    const footerLength = thriftBytes.length;
    const totalSize = footerLength + 8; // 4 bytes for length, 4 for magic
    const data = new Uint8Array(totalSize);

    data.set(thriftBytes, 0);

    // Write footer length as 4-byte little-endian integer
    const lengthView = new DataView(data.buffer, footerLength, 4);
    lengthView.setUint32(0, footerLength, true);

    // Write PAR1 magic marker
    data.set(PARQUET_MAGIC, footerLength + 4);

    return {
      data,
      version: this.version,
      schema: flattenedSchema,
      numRows,
      rowGroups: input.rowGroups,
      createdBy: this.createdBy,
      keyValueMetadata: this.keyValueMetadata,
      encryptionAlgorithm: input.encryptionAlgorithm,
      columnOrders: input.columnOrders,
    };
  }

  /** Validate input schema and row group metadata. */
  private validate(input: FooterInput): void {
    const schemaColumnNames = this.collectLeafColumnNames(input.schema);

    // Validate all schema elements
    for (const element of input.schema) {
      this.validateSchemaElement(element);
    }

    // Validate row groups and columns
    for (const rowGroup of input.rowGroups) {
      if (rowGroup.numRows < 0) {
        throw new Error('Row count cannot be negative');
      }
      if (rowGroup.fileOffset < 0) {
        throw new Error('Row group file offset cannot be negative');
      }

      // Ensure row group has exactly the expected number of columns
      if (rowGroup.columns.length !== schemaColumnNames.size) {
        throw new Error(
          `Column count mismatch in row group: expected ${schemaColumnNames.size}, got ${rowGroup.columns.length}`
        );
      }

      // Validate each column exists in schema
      for (const column of rowGroup.columns) {
        if (column.fileOffset < 0) {
          throw new Error(`Column "${column.columnName}" has negative file offset`);
        }
        if (!schemaColumnNames.has(column.columnName)) {
          throw new Error(`Column "${column.columnName}" not found in schema`);
        }
      }
    }
  }

  /** Validate schema element type and recursively validate children. */
  private validateSchemaElement(element: SchemaElement): void {
    if (element.type !== undefined && !VALID_PARQUET_TYPES.has(element.type)) {
      throw new Error(`Invalid Parquet physical type: ${element.type}`);
    }

    // Recursively validate nested elements
    if (element.children) {
      for (const child of element.children) {
        this.validateSchemaElement(child);
      }
    }
  }

  /**
   * Collect all leaf column names from schema with full paths (e.g., "user.name").
   * Groups (intermediate nodes without type) are not included.
   */
  private collectLeafColumnNames(
    schema: SchemaElement[],
    prefix: string = ''
  ): Set<string> {
    const names = new Set<string>();

    for (const element of schema) {
      const fullName = prefix ? `${prefix}.${element.name}` : element.name;

      if (element.children?.length ?? 0 > 0) {
        // Recursively collect from children
        const childNames = this.collectLeafColumnNames(element.children!, fullName);
        childNames.forEach(name => names.add(name));
      } else if (element.type !== undefined) {
        // Leaf node: has a physical type
        names.add(fullName);
      }
    }

    return names;
  }

  /**
   * Flatten nested schema into linear list with root element first.
   * Required by Parquet format for metadata storage.
   */
  private flattenSchema(schema: SchemaElement[]): SchemaElement[] {
    const result: SchemaElement[] = [
      {
        name: 'schema',
        numChildren: schema.length,
      },
    ];

    for (const element of schema) {
      this.flattenSchemaElement(element, result);
    }

    return result;
  }

  /**
   * Flatten a single schema element and recursively flatten its children.
   * Removes the children array and sets numChildren count instead.
   */
  private flattenSchemaElement(element: SchemaElement, result: SchemaElement[]): void {
    // Copy element without children array (Parquet uses index-based structure)
    const flattened: SchemaElement = { name: element.name };

    if (element.type !== undefined) flattened.type = element.type;
    if (element.typeLength !== undefined) flattened.typeLength = element.typeLength;
    if (element.repetitionType !== undefined) flattened.repetitionType = element.repetitionType;
    if (element.convertedType !== undefined) flattened.convertedType = element.convertedType;
    if (element.fieldId !== undefined) flattened.fieldId = element.fieldId;

    // Count children for group nodes
    if ((element.children?.length ?? 0) > 0) {
      flattened.numChildren = element.children!.length;
    }

    result.push(flattened);

    // Recursively flatten children immediately after parent
    if (element.children?.length ?? 0 > 0) {
      for (const child of element.children!) {
        this.flattenSchemaElement(child, result);
      }
    }
  }

  /** Write FileMetaData struct (root Parquet metadata). */
  private writeFileMetaData(
    writer: ThriftCompactWriter,
    input: FooterInput,
    flattenedSchema: SchemaElement[],
    numRows: number
  ): void {
    writer.writeStructBegin();

    writer.writeI32Field(1, this.version);

    // Field 2: schema list (flattened with root element)
    writer.writeListBegin(2, THRIFT_TYPE.STRUCT, flattenedSchema.length);
    for (const element of flattenedSchema) {
      this.writeSchemaElement(writer, element);
    }

    writer.writeI64Field(3, numRows);

    // Field 4: row_groups list
    writer.writeListBegin(4, THRIFT_TYPE.STRUCT, input.rowGroups.length);
    for (const rowGroup of input.rowGroups) {
      this.writeRowGroup(writer, rowGroup, input.schema);
    }

    // Field 5: key_value_metadata (optional)
    if (this.keyValueMetadata?.length ?? 0 > 0) {
      writer.writeListBegin(5, THRIFT_TYPE.STRUCT, this.keyValueMetadata!.length);
      for (const kv of this.keyValueMetadata!) {
        this.writeKeyValue(writer, kv);
      }
    }

    // Field 6: created_by attribution
    writer.writeBinaryField(6, this.createdBy);

    // Field 7: column_orders (optional)
    if (input.columnOrders?.length ?? 0 > 0) {
      writer.writeListBegin(7, THRIFT_TYPE.STRUCT, input.columnOrders!.length);
      for (const order of input.columnOrders!) {
        this.writeColumnOrder(writer, order);
      }
    }

    // Field 8: encryption_algorithm (optional)
    if (input.encryptionAlgorithm) {
      writer.writeBinaryField(8, input.encryptionAlgorithm);
    }

    writer.writeStructEnd();
  }

  /** Write SchemaElement struct. */
  private writeSchemaElement(writer: ThriftCompactWriter, element: SchemaElement): void {
    writer.writeStructBegin();

    // Field 1: type (optional) - physical type for leaf nodes
    if (element.type !== undefined) {
      writer.writeI32Field(1, PARQUET_TYPE_ENUM[element.type]);
    }

    // Field 2: type_length (optional) - length for FIXED_LEN_BYTE_ARRAY
    if (element.typeLength !== undefined) {
      writer.writeI32Field(2, element.typeLength);
    }

    // Field 3: repetition_type (optional)
    if (element.repetitionType !== undefined) {
      writer.writeI32Field(3, REPETITION_TYPE_ENUM[element.repetitionType]);
    }

    // Field 4: name (required)
    writer.writeBinaryField(4, element.name);

    // Field 5: num_children (optional) - count for group nodes
    if (element.numChildren !== undefined) {
      writer.writeI32Field(5, element.numChildren);
    }

    // Field 6: converted_type (optional) - logical type annotation
    if (element.convertedType !== undefined) {
      writer.writeI32Field(6, CONVERTED_TYPE_ENUM[element.convertedType]);
    }

    // Field 9: field_id (optional) - for column selection projection
    if (element.fieldId !== undefined) {
      writer.writeI32Field(9, element.fieldId);
    }

    writer.writeStructEnd();
  }

  /** Write RowGroup struct. */
  private writeRowGroup(
    writer: ThriftCompactWriter,
    rowGroup: RowGroupMetadata,
    schema: SchemaElement[]
  ): void {
    writer.writeStructBegin();

    // Field 1: columns list
    writer.writeListBegin(1, THRIFT_TYPE.STRUCT, rowGroup.columns.length);
    for (const column of rowGroup.columns) {
      this.writeColumnChunk(writer, column, schema);
    }

    // Field 2: total_byte_size
    writer.writeI64Field(2, rowGroup.totalByteSize);

    // Field 3: num_rows
    writer.writeI64Field(3, rowGroup.numRows);

    // Field 5: file_offset (row group data location in file)
    writer.writeI64Field(5, rowGroup.fileOffset);

    writer.writeStructEnd();
  }

  /** Write ColumnChunk struct. */
  private writeColumnChunk(
    writer: ThriftCompactWriter,
    column: ColumnChunkMetadata,
    schema: SchemaElement[]
  ): void {
    writer.writeStructBegin();

    // Field 2: file_offset (column chunk data location)
    writer.writeI64Field(2, column.fileOffset);

    // Field 3: meta_data (ColumnMetaData struct)
    writer.writeStructFieldBegin(3);
    this.writeColumnMetaData(writer, column, schema);

    writer.writeStructEnd();
  }

  /** Write ColumnMetaData struct. */
  private writeColumnMetaData(
    writer: ThriftCompactWriter,
    column: ColumnChunkMetadata,
    schema: SchemaElement[]
  ): void {
    writer.writeStructBegin();

    const columnType = this.findColumnType(column.columnName, schema);

    // Field 1: type (physical type)
    if (columnType !== undefined) {
      writer.writeI32Field(1, PARQUET_TYPE_ENUM[columnType]);
    }

    // Field 2: encodings list (typically single element)
    writer.writeListBegin(2, THRIFT_TYPE.I32, 1);
    writer.writeZigzagI32(ENCODING_ENUM[column.encoding]);

    // Field 3: path_in_schema (hierarchical path as list of strings)
    const pathParts = column.columnName.split('.');
    writer.writeListBegin(3, THRIFT_TYPE.BINARY, pathParts.length);
    for (const part of pathParts) {
      writer.writeRawBinary(part);
    }

    // Field 4: codec (compression algorithm)
    writer.writeI32Field(4, COMPRESSION_ENUM[column.compression]);

    // Field 5: num_values (total values in this column chunk)
    writer.writeI64Field(5, column.numValues);

    // Field 6: total_uncompressed_size (uncompressed byte size)
    writer.writeI64Field(6, column.uncompressedSize);

    // Field 7: total_compressed_size (compressed byte size)
    writer.writeI64Field(7, column.compressedSize);

    // Field 9: data_page_offset (location of first data page)
    const dataPageOffset = column.dataPageOffset ?? column.fileOffset;
    writer.writeI64Field(9, dataPageOffset);

    // Field 11: dictionary_page_offset (optional, for dictionary encoding)
    if (column.dictionaryPageOffset !== undefined) {
      writer.writeI64Field(11, column.dictionaryPageOffset);
    }

    // Field 12: statistics (optional min/max/null counts)
    if (column.statistics) {
      writer.writeStructFieldBegin(12);
      this.writeStatistics(writer, column.statistics, columnType);
    }

    writer.writeStructEnd();
  }

  /** Write Statistics struct with min/max values and counts. */
  private writeStatistics(
    writer: ThriftCompactWriter,
    stats: ColumnStatistics,
    columnType?: ParquetType
  ): void {
    writer.writeStructBegin();

    // Field 3: null_count (optional)
    if (stats.nullCount !== undefined) {
      writer.writeI64Field(3, stats.nullCount);
    }

    // Field 4: distinct_count (optional, for filtering optimization)
    if (stats.distinctCount !== undefined) {
      writer.writeI64Field(4, stats.distinctCount);
    }

    // Field 5: max_value (optional, serialized as binary)
    if (stats.maxValue !== undefined && stats.maxValue !== null) {
      const maxBytes = this.serializeStatValue(stats.maxValue, columnType);
      writer.writeBinaryField(5, maxBytes);
    }

    // Field 6: min_value (optional, serialized as binary)
    if (stats.minValue !== undefined && stats.minValue !== null) {
      const minBytes = this.serializeStatValue(stats.minValue, columnType);
      writer.writeBinaryField(6, minBytes);
    }

    writer.writeStructEnd();
  }

  /**
   * Convert statistics value to binary representation based on type.
   * Handles strings, BigInt, and numeric types with appropriate encoding.
   */
  private serializeStatValue(value: unknown, columnType?: ParquetType): Uint8Array {
    // Already binary
    if (value instanceof Uint8Array) {
      return value;
    }

    // String types (UTF8)
    if (typeof value === 'string') {
      return new TextEncoder().encode(value);
    }

    // BigInt (INT64)
    if (typeof value === 'bigint') {
      const buf = new ArrayBuffer(8);
      const view = new DataView(buf);
      view.setBigInt64(0, value, true);
      return new Uint8Array(buf);
    }

    // Numeric types
    if (typeof value === 'number') {
      // Use column type hint if available
      if (columnType === 'INT32') {
        const buf = new ArrayBuffer(4);
        new DataView(buf).setInt32(0, value, true);
        return new Uint8Array(buf);
      }
      if (columnType === 'INT64') {
        const buf = new ArrayBuffer(8);
        new DataView(buf).setBigInt64(0, BigInt(Math.round(value)), true);
        return new Uint8Array(buf);
      }
      if (columnType === 'FLOAT') {
        const buf = new ArrayBuffer(4);
        new DataView(buf).setFloat32(0, value, true);
        return new Uint8Array(buf);
      }
      if (columnType === 'DOUBLE') {
        const buf = new ArrayBuffer(8);
        new DataView(buf).setFloat64(0, value, true);
        return new Uint8Array(buf);
      }
      // Default: 4-byte signed integer
      const buf = new ArrayBuffer(4);
      new DataView(buf).setInt32(0, value, true);
      return new Uint8Array(buf);
    }

    // Unsupported type: return empty
    return new Uint8Array(0);
  }

  /**
   * Find physical type of a column by traversing schema hierarchy.
   * Handles nested column paths (e.g., "user.name").
   */
  private findColumnType(
    columnName: string,
    schema: SchemaElement[]
  ): ParquetType | undefined {
    const parts = columnName.split('.');

    const traverse = (
      elements: SchemaElement[],
      pathIndex: number
    ): ParquetType | undefined => {
      for (const element of elements) {
        if (element.name === parts[pathIndex]) {
          if (pathIndex === parts.length - 1) {
            return element.type; // Found leaf
          }
          // Continue traversing children
          if (element.children?.length ?? 0 > 0) {
            return traverse(element.children!, pathIndex + 1);
          }
          return undefined;
        }
      }
      return undefined;
    };

    return traverse(schema, 0);
  }

  /** Write KeyValue metadata pair. */
  private writeKeyValue(writer: ThriftCompactWriter, kv: KeyValueMetadata): void {
    writer.writeStructBegin();
    writer.writeBinaryField(1, kv.key);
    writer.writeBinaryField(2, kv.value);
    writer.writeStructEnd();
  }

  /** Write ColumnOrder (union type for sort order). */
  private writeColumnOrder(writer: ThriftCompactWriter, order: ColumnOrder): void {
    writer.writeStructBegin();

    // ColumnOrder is a union; TYPE_DEFINED_ORDER is the only supported variant
    if (order.columnOrderType === 'TYPE_DEFINED_ORDER') {
      // Field 1: TYPE_ORDER (empty struct as union discriminator)
      writer.writeStructFieldBegin(1);
      writer.writeStructBegin();
      writer.writeStructEnd();
    }

    writer.writeStructEnd();
  }
}
