/**
 * Parquet Module
 *
 * Provides Parquet file reading and writing capabilities for MongoLake.
 * Uses hyparquet libraries for actual binary Parquet format.
 */

// Main I/O functions
export {
  writeParquet,
  readParquet,
  createAsyncBufferFromBytes,
  isParquetFile,
  getParquetMetadata,
  type WriteOptions,
  type ParquetRow,
  type AsyncBuffer,
} from './io.js';

// Variant encoding (for custom Parquet workflows)
export {
  encodeVariant,
  decodeVariant,
  VariantType,
} from './variant.js';

// Compression utilities
export {
  compress,
  decompress,
  getCodec,
  isCodecSupported,
  supportedCodecs,
  snappyCodec,
  zstdCodec,
  noneCodec,
  type CompressionCodec,
  type CompressionCodecInterface,
} from './compression.js';

// Type mapping (BSON to Parquet)
export {
  BSONType,
  ParquetPhysicalType,
  ParquetLogicalType,
  bsonToParquet,
  inferBSONType,
  canPromoteType,
  getPromotedMapping,
  mapSchemaToParquet,
  mapValueToParquet,
  getDefaultValue,
  isNumericPhysicalType,
  getPhysicalTypeByteSize,
  type BSONTypeValue,
  type ParquetPhysicalTypeValue,
  type ParquetLogicalTypeValue,
  type TypeMapping,
  type SchemaField,
  type ParquetSchemaField,
  type ParquetSchema,
  type MappedValue,
} from './type-mapper.js';

// Backward Compatibility Reader
export {
  CompatReader,
  reconcileSchemas,
  createColumnMapping,
  applyDefaults,
  batchRead,
  type CompatReaderOptions,
  type SchemaMapping,
  type SchemaReconciliation,
  type ColumnMapping,
  type TypeTransform,
  type TypeChange,
  type WidenedColumn,
  type CompatibilityWarning,
  type ReadResult,
  type DefaultValue,
  type ForwardCompatibilityHints,
  type BatchReadOptions,
} from './compat-reader.js';
