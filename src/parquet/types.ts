/**
 * Parquet Type Definitions
 *
 * Consolidated type definitions for Parquet file format structures.
 * These types are used across footer generation, parsing, row groups, and zone maps.
 */

// ============================================================================
// Parquet Physical Types
// ============================================================================

/**
 * Parquet physical types define how data is stored at the binary level.
 * These correspond to the primitive types in the Parquet specification.
 */
export type ParquetType =
  | 'BOOLEAN'
  | 'INT32'
  | 'INT64'
  | 'INT96'
  | 'FLOAT'
  | 'DOUBLE'
  | 'BYTE_ARRAY'
  | 'FIXED_LEN_BYTE_ARRAY';

/** Alias for ParquetType for backwards compatibility */
export type ParquetPhysicalType = ParquetType;

// ============================================================================
// Parquet Logical Types
// ============================================================================

/**
 * Parquet converted (logical) types provide semantic meaning on top of physical types.
 * For example, a BYTE_ARRAY physical type with UTF8 converted type represents a string.
 */
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

/**
 * Logical type annotation (Parquet 2.0+)
 * Provides more detailed type information than ConvertedType.
 */
export interface LogicalType {
  type: string;
  precision?: number;
  scale?: number;
  isAdjustedToUTC?: boolean;
  unit?: string;
}

// ============================================================================
// Repetition Types
// ============================================================================

/**
 * Field repetition type defines nullability and cardinality.
 * - REQUIRED: Field must have exactly one value (not nullable)
 * - OPTIONAL: Field may have zero or one value (nullable)
 * - REPEATED: Field may have zero or more values (array-like)
 */
export type RepetitionType = 'REQUIRED' | 'OPTIONAL' | 'REPEATED';

/** Alias for RepetitionType for backwards compatibility */
export type FieldRepetitionType = RepetitionType;

// ============================================================================
// Encoding Types
// ============================================================================

/**
 * Parquet encoding types define how values are encoded within pages.
 * Different encodings optimize for different data patterns.
 */
export type Encoding =
  | 'PLAIN'
  | 'RLE'
  | 'BIT_PACKED'
  | 'DELTA_BINARY_PACKED'
  | 'DELTA_LENGTH_BYTE_ARRAY'
  | 'DELTA_BYTE_ARRAY'
  | 'RLE_DICTIONARY'
  | 'BYTE_STREAM_SPLIT'
  | 'PLAIN_DICTIONARY';

// ============================================================================
// Compression Types
// ============================================================================

/**
 * Compression codec for column data (lowercase convention used by footer generator).
 * Used when writing Parquet files.
 */
export type CompressionCodec = 'none' | 'snappy' | 'gzip' | 'lzo' | 'brotli' | 'lz4' | 'zstd';

/**
 * Compression codec enum values (uppercase convention used by footer parser).
 * Used when reading Parquet files.
 */
export type CompressionCodecUppercase =
  | 'UNCOMPRESSED'
  | 'SNAPPY'
  | 'GZIP'
  | 'LZO'
  | 'BROTLI'
  | 'LZ4'
  | 'ZSTD';

// ============================================================================
// Statistics Types
// ============================================================================

/**
 * Column statistics used for zone maps and predicate pushdown.
 * Statistics enable skipping row groups that cannot contain matching data.
 */
export interface ColumnStatistics {
  minValue?: unknown;
  maxValue?: unknown;
  nullCount?: number;
  distinctCount?: number;
}

// ============================================================================
// Schema Types
// ============================================================================

/**
 * Schema element representing a field in the Parquet schema.
 * Can represent either a primitive column or a group (nested structure).
 */
export interface SchemaElement {
  /** Field name */
  name: string;
  /** Physical type (only for leaf/primitive columns) */
  type?: ParquetType;
  /** Length for FIXED_LEN_BYTE_ARRAY type */
  typeLength?: number;
  /** Nullability/cardinality */
  repetitionType?: RepetitionType;
  /** Logical type annotation (legacy) */
  convertedType?: ConvertedType;
  /** Logical type annotation (Parquet 2.0+) */
  logicalType?: LogicalType | null;
  /** Number of children for group nodes */
  numChildren?: number;
  /** Optional field ID for column projection */
  fieldId?: number;
  /** Decimal scale (for DECIMAL type) */
  scale?: number;
  /** Decimal precision (for DECIMAL type) */
  precision?: number;
  /** Children elements (used during schema construction, flattened for storage) */
  children?: SchemaElement[];
}

/**
 * Parquet schema container with element list.
 */
export interface ParquetSchema {
  elements: SchemaElement[];
}

// ============================================================================
// Column Chunk Types
// ============================================================================

/**
 * Column chunk metadata for footer generator (write path).
 * Contains information needed to serialize column data to Parquet format.
 */
export interface ColumnChunkMetadata {
  /** Column name (may include path for nested columns, e.g., "user.name") */
  columnName: string;
  /** Byte offset of column chunk in file */
  fileOffset: number;
  /** Byte offset of first data page */
  dataPageOffset?: number;
  /** Byte offset of dictionary page (if present) */
  dictionaryPageOffset?: number;
  /** Compressed size in bytes */
  compressedSize: number;
  /** Uncompressed size in bytes */
  uncompressedSize: number;
  /** Total number of values (including nulls) */
  numValues: number;
  /** Encoding used for data pages */
  encoding: Encoding;
  /** Compression codec used */
  compression: CompressionCodec;
  /** Column statistics for zone maps */
  statistics?: ColumnStatistics;
}

/**
 * Column chunk metadata for footer parser (read path).
 * Uses different field names and supports multiple encodings.
 */
export interface ParsedColumnChunkMetadata {
  /** Column path (dot-separated for nested columns) */
  columnPath: string;
  /** Byte offset of column chunk in file */
  fileOffset: number;
  /** Byte offset of first data page */
  dataPageOffset: number;
  /** Byte offset of dictionary page (if present) */
  dictionaryPageOffset?: number;
  /** Compressed size in bytes */
  compressedSize: number;
  /** Uncompressed size in bytes */
  uncompressedSize: number;
  /** Total number of values */
  numValues: number;
  /** List of encodings used */
  encodings: Encoding[];
  /** Compression codec (uppercase enum) */
  codec: CompressionCodecUppercase;
  /** Column statistics */
  statistics?: ColumnStatistics;
  /** Physical type of the column */
  type?: ParquetType;
}

// ============================================================================
// Row Group Types
// ============================================================================

/**
 * Sorting column specification for row group ordering.
 */
export interface SortingColumn {
  columnIdx: number;
  descending: boolean;
  nullsFirst: boolean;
}

/**
 * Row group metadata for footer generator (write path).
 */
export interface RowGroupMetadata {
  /** Column chunks in this row group */
  columns: ColumnChunkMetadata[];
  /** Number of rows in this row group */
  numRows: number;
  /** Total byte size of all column data */
  totalByteSize: number;
  /** Byte offset of row group in file */
  fileOffset: number;
}

/**
 * Row group metadata for footer parser (read path).
 * Uses ParsedColumnChunkMetadata and includes sorting information.
 */
export interface ParsedRowGroupMetadata {
  /** Column chunks in this row group */
  columns: ParsedColumnChunkMetadata[];
  /** Number of rows in this row group */
  numRows: number;
  /** Total byte size of all column data */
  totalByteSize: number;
  /** Byte offset of row group in file */
  fileOffset: number;
  /** Sorting column specifications */
  sortingColumns?: SortingColumn[];
}

// ============================================================================
// Key-Value Metadata
// ============================================================================

/**
 * Key-value metadata pair stored in Parquet footer.
 * Used for custom application-specific metadata.
 */
export interface KeyValueMetadata {
  key: string;
  value: string;
}

// ============================================================================
// Column Order Types
// ============================================================================

/**
 * Column order specification for sort ordering semantics.
 */
export interface ColumnOrder {
  columnOrderType: 'TYPE_DEFINED_ORDER' | 'UNDEFINED';
}

// ============================================================================
// Footer Types
// ============================================================================

/**
 * Footer generator input configuration.
 */
export interface FooterInput {
  schema: SchemaElement[];
  rowGroups: RowGroupMetadata[];
  encryptionAlgorithm?: string;
  columnOrders?: ColumnOrder[];
}

/**
 * Generated footer output from FooterGenerator.
 */
export interface GeneratedParquetFooter {
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

/**
 * Parsed footer result from FooterParser.
 */
export interface ParsedParquetFooter {
  version: number;
  formatVersion: string;
  schema: ParquetSchema;
  numRows: number;
  rowGroups: ParsedRowGroupMetadata[];
  createdBy?: string;
  keyValueMetadata?: KeyValueMetadata[];
  footerLength: number;
  footerOffset: number;
}

/**
 * Footer generator options.
 */
export interface FooterGeneratorOptions {
  version?: number;
  createdBy?: string;
  keyValueMetadata?: KeyValueMetadata[];
}
