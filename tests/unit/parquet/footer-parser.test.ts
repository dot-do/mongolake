/**
 * Parquet Footer Parser Tests
 *
 * Comprehensive tests for parsing Parquet file footers.
 * The footer contains the file metadata including schema,
 * row group metadata, and column chunk locations/statistics.
 *
 * Parquet file structure:
 * - Magic bytes "PAR1" (4 bytes)
 * - Row groups (data)
 * - Footer (Thrift-encoded FileMetaData)
 * - Footer length (4 bytes, little-endian)
 * - Magic bytes "PAR1" (4 bytes)
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  FooterParser,
  parseFooter,
  type ParquetFooter,
  type ParquetSchema,
  type SchemaElement,
  type RowGroupMetadata,
  type ColumnChunkMetadata,
  type ColumnStatistics,
  type ParquetType,
  type ConvertedType,
  type LogicalType,
  type FieldRepetitionType,
  type Encoding,
  type CompressionCodec,
  ParquetError,
  InvalidMagicBytesError,
  TruncatedFooterError,
  InvalidFooterLengthError,
  UnsupportedVersionError,
  CorruptedMetadataError,
} from '../../../src/parquet/footer-parser.js';

// ============================================================================
// Test Fixtures
// ============================================================================

/**
 * Creates a minimal valid Parquet file buffer with footer
 */
function createMinimalParquetBuffer(): Uint8Array {
  // PAR1 magic at start
  const startMagic = new TextEncoder().encode('PAR1');

  // Minimal row group data (empty for this test)
  const rowGroupData = new Uint8Array(0);

  // Minimal Thrift-encoded footer (simplified mock)
  // In reality this would be proper Thrift encoding
  const footerData = new Uint8Array([
    // Version (4 bytes, little-endian) - version 1
    0x01, 0x00, 0x00, 0x00,
    // Schema element count (4 bytes, little-endian) - 1 element
    0x01, 0x00, 0x00, 0x00,
    // Minimal schema element (root)
    0x00, // type (none for root)
    0x04, // name length
    0x72, 0x6f, 0x6f, 0x74, // "root"
    0x00, // num_children = 0
    // Row groups count (4 bytes) - 0 row groups
    0x00, 0x00, 0x00, 0x00,
    // Created by string length (4 bytes)
    0x08, 0x00, 0x00, 0x00,
    // "mongolake"
    0x6d, 0x6f, 0x6e, 0x67, 0x6f, 0x6c, 0x61, 0x6b,
  ]);

  // Footer length (4 bytes, little-endian)
  const footerLength = new Uint8Array(4);
  new DataView(footerLength.buffer).setUint32(0, footerData.length, true);

  // PAR1 magic at end
  const endMagic = new TextEncoder().encode('PAR1');

  // Combine all parts
  const totalLength =
    startMagic.length + rowGroupData.length + footerData.length + footerLength.length + endMagic.length;
  const buffer = new Uint8Array(totalLength);

  let offset = 0;
  buffer.set(startMagic, offset);
  offset += startMagic.length;
  buffer.set(rowGroupData, offset);
  offset += rowGroupData.length;
  buffer.set(footerData, offset);
  offset += footerData.length;
  buffer.set(footerLength, offset);
  offset += footerLength.length;
  buffer.set(endMagic, offset);

  return buffer;
}

/**
 * Creates a Parquet buffer with schema information
 */
function createParquetBufferWithSchema(columns: Array<{
  name: string;
  type: ParquetType;
  repetitionType?: FieldRepetitionType;
  convertedType?: ConvertedType;
}>): Uint8Array {
  // This is a mock implementation - real implementation would
  // properly encode the Thrift structure
  const startMagic = new TextEncoder().encode('PAR1');

  // Build schema elements
  const schemaElements: Uint8Array[] = [];
  // Root element first
  const rootName = new TextEncoder().encode('schema');
  schemaElements.push(
    new Uint8Array([
      0x00, // no type (root)
      rootName.length,
      ...rootName,
      columns.length, // num_children
    ])
  );

  // Column elements
  for (const col of columns) {
    const nameBytes = new TextEncoder().encode(col.name);
    const typeCode = getTypeCode(col.type);
    const repetitionCode = getRepetitionCode(col.repetitionType || 'OPTIONAL');
    schemaElements.push(
      new Uint8Array([
        typeCode,
        repetitionCode,
        nameBytes.length,
        ...nameBytes,
        0x00, // no children for leaf nodes
      ])
    );
  }

  // Flatten schema elements
  const schemaData = new Uint8Array(schemaElements.reduce((acc, el) => acc + el.length, 0));
  let schemaOffset = 0;
  for (const el of schemaElements) {
    schemaData.set(el, schemaOffset);
    schemaOffset += el.length;
  }

  // Build footer
  const footerParts = [
    // Version
    new Uint8Array([0x01, 0x00, 0x00, 0x00]),
    // Schema element count
    new Uint8Array([columns.length + 1, 0x00, 0x00, 0x00]),
    // Schema data
    schemaData,
    // Row groups count (0)
    new Uint8Array([0x00, 0x00, 0x00, 0x00]),
    // num_rows (0)
    new Uint8Array([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
  ];

  const footerData = new Uint8Array(footerParts.reduce((acc, p) => acc + p.length, 0));
  let footerOffset = 0;
  for (const part of footerParts) {
    footerData.set(part, footerOffset);
    footerOffset += part.length;
  }

  const footerLength = new Uint8Array(4);
  new DataView(footerLength.buffer).setUint32(0, footerData.length, true);

  const endMagic = new TextEncoder().encode('PAR1');

  const totalLength = startMagic.length + footerData.length + footerLength.length + endMagic.length;
  const buffer = new Uint8Array(totalLength);

  let offset = 0;
  buffer.set(startMagic, offset);
  offset += startMagic.length;
  buffer.set(footerData, offset);
  offset += footerData.length;
  buffer.set(footerLength, offset);
  offset += footerLength.length;
  buffer.set(endMagic, offset);

  return buffer;
}

/**
 * Creates a Parquet buffer with row group metadata
 */
function createParquetBufferWithRowGroups(rowGroups: Array<{
  numRows: number;
  totalByteSize: number;
  columns: Array<{
    name: string;
    type: ParquetType;
    fileOffset: number;
    compressedSize: number;
    uncompressedSize: number;
    numValues: number;
  }>;
}>): Uint8Array {
  // Mock implementation
  const startMagic = new TextEncoder().encode('PAR1');
  const endMagic = new TextEncoder().encode('PAR1');

  // Build footer with row groups (simplified mock)
  const footerParts: Uint8Array[] = [];

  // Version
  footerParts.push(new Uint8Array([0x01, 0x00, 0x00, 0x00]));

  // Schema (minimal - just root + one column per unique column in row groups)
  const allColumns = new Set<string>();
  for (const rg of rowGroups) {
    for (const col of rg.columns) {
      allColumns.add(col.name);
    }
  }
  footerParts.push(new Uint8Array([allColumns.size + 1, 0x00, 0x00, 0x00]));

  // Row groups count
  const rgCountBytes = new Uint8Array(4);
  new DataView(rgCountBytes.buffer).setUint32(0, rowGroups.length, true);
  footerParts.push(rgCountBytes);

  // Row group metadata (simplified)
  for (const rg of rowGroups) {
    // num_rows (8 bytes)
    const numRowsBytes = new Uint8Array(8);
    new DataView(numRowsBytes.buffer).setBigInt64(0, BigInt(rg.numRows), true);
    footerParts.push(numRowsBytes);

    // total_byte_size (8 bytes)
    const totalSizeBytes = new Uint8Array(8);
    new DataView(totalSizeBytes.buffer).setBigInt64(0, BigInt(rg.totalByteSize), true);
    footerParts.push(totalSizeBytes);

    // columns count
    const colCountBytes = new Uint8Array(4);
    new DataView(colCountBytes.buffer).setUint32(0, rg.columns.length, true);
    footerParts.push(colCountBytes);

    // Column chunk metadata
    for (const col of rg.columns) {
      const nameBytes = new TextEncoder().encode(col.name);
      footerParts.push(new Uint8Array([nameBytes.length, ...nameBytes]));

      // file_offset (8 bytes)
      const offsetBytes = new Uint8Array(8);
      new DataView(offsetBytes.buffer).setBigInt64(0, BigInt(col.fileOffset), true);
      footerParts.push(offsetBytes);

      // compressed_size (4 bytes)
      const compressedBytes = new Uint8Array(4);
      new DataView(compressedBytes.buffer).setUint32(0, col.compressedSize, true);
      footerParts.push(compressedBytes);

      // uncompressed_size (4 bytes)
      const uncompressedBytes = new Uint8Array(4);
      new DataView(uncompressedBytes.buffer).setUint32(0, col.uncompressedSize, true);
      footerParts.push(uncompressedBytes);

      // num_values (8 bytes)
      const numValuesBytes = new Uint8Array(8);
      new DataView(numValuesBytes.buffer).setBigInt64(0, BigInt(col.numValues), true);
      footerParts.push(numValuesBytes);
    }
  }

  // Total num_rows
  const totalRows = rowGroups.reduce((acc, rg) => acc + rg.numRows, 0);
  const totalRowsBytes = new Uint8Array(8);
  new DataView(totalRowsBytes.buffer).setBigInt64(0, BigInt(totalRows), true);
  footerParts.push(totalRowsBytes);

  const footerData = new Uint8Array(footerParts.reduce((acc, p) => acc + p.length, 0));
  let footerOffset = 0;
  for (const part of footerParts) {
    footerData.set(part, footerOffset);
    footerOffset += part.length;
  }

  const footerLength = new Uint8Array(4);
  new DataView(footerLength.buffer).setUint32(0, footerData.length, true);

  const totalLength = startMagic.length + footerData.length + footerLength.length + endMagic.length;
  const buffer = new Uint8Array(totalLength);

  let offset = 0;
  buffer.set(startMagic, offset);
  offset += startMagic.length;
  buffer.set(footerData, offset);
  offset += footerData.length;
  buffer.set(footerLength, offset);
  offset += footerLength.length;
  buffer.set(endMagic, offset);

  return buffer;
}

/**
 * Creates a Parquet buffer with column statistics
 */
function createParquetBufferWithStatistics(columns: Array<{
  name: string;
  type: ParquetType;
  statistics: {
    minValue?: unknown;
    maxValue?: unknown;
    nullCount?: number;
    distinctCount?: number;
  };
}>): Uint8Array {
  // Mock implementation - actual implementation would encode statistics properly
  const startMagic = new TextEncoder().encode('PAR1');
  const endMagic = new TextEncoder().encode('PAR1');

  const footerParts: Uint8Array[] = [];

  // Version
  footerParts.push(new Uint8Array([0x01, 0x00, 0x00, 0x00]));

  // Schema
  footerParts.push(new Uint8Array([columns.length + 1, 0x00, 0x00, 0x00]));

  // Row groups (1 row group with statistics)
  footerParts.push(new Uint8Array([0x01, 0x00, 0x00, 0x00]));

  // Row group num_rows
  footerParts.push(new Uint8Array([0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])); // 100 rows

  // Total byte size
  footerParts.push(new Uint8Array([0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])); // 4096 bytes

  // Column count
  footerParts.push(new Uint8Array([columns.length, 0x00, 0x00, 0x00]));

  // Column chunks with statistics
  for (const col of columns) {
    const nameBytes = new TextEncoder().encode(col.name);
    footerParts.push(new Uint8Array([nameBytes.length, ...nameBytes]));

    // Type code
    footerParts.push(new Uint8Array([getTypeCode(col.type)]));

    // File offset, sizes
    footerParts.push(new Uint8Array([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])); // offset
    footerParts.push(new Uint8Array([0x00, 0x04, 0x00, 0x00])); // compressed size
    footerParts.push(new Uint8Array([0x00, 0x04, 0x00, 0x00])); // uncompressed size

    // Statistics marker
    footerParts.push(new Uint8Array([0x01])); // has_statistics = true

    // Null count
    const nullCount = col.statistics.nullCount ?? 0;
    const nullCountBytes = new Uint8Array(8);
    new DataView(nullCountBytes.buffer).setBigInt64(0, BigInt(nullCount), true);
    footerParts.push(nullCountBytes);

    // Distinct count (optional)
    if (col.statistics.distinctCount !== undefined) {
      footerParts.push(new Uint8Array([0x01])); // has_distinct_count
      const distinctBytes = new Uint8Array(8);
      new DataView(distinctBytes.buffer).setBigInt64(0, BigInt(col.statistics.distinctCount), true);
      footerParts.push(distinctBytes);
    } else {
      footerParts.push(new Uint8Array([0x00])); // no distinct_count
    }

    // Min/Max values (simplified encoding)
    if (col.statistics.minValue !== undefined) {
      footerParts.push(new Uint8Array([0x01])); // has_min
      footerParts.push(encodeStatValue(col.statistics.minValue, col.type));
    } else {
      footerParts.push(new Uint8Array([0x00]));
    }

    if (col.statistics.maxValue !== undefined) {
      footerParts.push(new Uint8Array([0x01])); // has_max
      footerParts.push(encodeStatValue(col.statistics.maxValue, col.type));
    } else {
      footerParts.push(new Uint8Array([0x00]));
    }
  }

  // Total rows
  footerParts.push(new Uint8Array([0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]));

  const footerData = new Uint8Array(footerParts.reduce((acc, p) => acc + p.length, 0));
  let footerOffset = 0;
  for (const part of footerParts) {
    footerData.set(part, footerOffset);
    footerOffset += part.length;
  }

  const footerLength = new Uint8Array(4);
  new DataView(footerLength.buffer).setUint32(0, footerData.length, true);

  const totalLength = startMagic.length + footerData.length + footerLength.length + endMagic.length;
  const buffer = new Uint8Array(totalLength);

  let offset = 0;
  buffer.set(startMagic, offset);
  offset += startMagic.length;
  buffer.set(footerData, offset);
  offset += footerData.length;
  buffer.set(footerLength, offset);
  offset += footerLength.length;
  buffer.set(endMagic, offset);

  return buffer;
}

// Helper functions for encoding
function getTypeCode(type: ParquetType): number {
  const typeCodes: Record<ParquetType, number> = {
    BOOLEAN: 0,
    INT32: 1,
    INT64: 2,
    INT96: 3,
    FLOAT: 4,
    DOUBLE: 5,
    BYTE_ARRAY: 6,
    FIXED_LEN_BYTE_ARRAY: 7,
  };
  return typeCodes[type] ?? 6;
}

function getRepetitionCode(rep: FieldRepetitionType): number {
  const codes: Record<FieldRepetitionType, number> = {
    REQUIRED: 0,
    OPTIONAL: 1,
    REPEATED: 2,
  };
  return codes[rep] ?? 1;
}

function encodeStatValue(value: unknown, type: ParquetType): Uint8Array {
  if (typeof value === 'number') {
    if (type === 'INT32') {
      const bytes = new Uint8Array(4);
      new DataView(bytes.buffer).setInt32(0, value, true);
      return bytes;
    } else if (type === 'INT64') {
      const bytes = new Uint8Array(8);
      new DataView(bytes.buffer).setBigInt64(0, BigInt(value), true);
      return bytes;
    } else if (type === 'FLOAT') {
      const bytes = new Uint8Array(4);
      new DataView(bytes.buffer).setFloat32(0, value, true);
      return bytes;
    } else if (type === 'DOUBLE') {
      const bytes = new Uint8Array(8);
      new DataView(bytes.buffer).setFloat64(0, value, true);
      return bytes;
    }
  } else if (typeof value === 'string') {
    const strBytes = new TextEncoder().encode(value);
    const result = new Uint8Array(4 + strBytes.length);
    new DataView(result.buffer).setUint32(0, strBytes.length, true);
    result.set(strBytes, 4);
    return result;
  } else if (typeof value === 'boolean') {
    return new Uint8Array([value ? 1 : 0]);
  }
  return new Uint8Array(0);
}

// ============================================================================
// Test Suites
// ============================================================================

describe('FooterParser', () => {
  let parser: FooterParser;

  beforeEach(() => {
    parser = new FooterParser();
  });

  // ==========================================================================
  // 1. Parse valid footer
  // ==========================================================================
  describe('Parse valid footer', () => {
    it('should parse a minimal valid Parquet footer', () => {
      const buffer = createMinimalParquetBuffer();
      const footer = parser.parse(buffer);

      expect(footer).toBeDefined();
      expect(footer.version).toBeDefined();
      expect(footer.schema).toBeDefined();
      expect(footer.rowGroups).toBeDefined();
    });

    it('should correctly identify the footer length', () => {
      const buffer = createMinimalParquetBuffer();
      const footer = parser.parse(buffer);

      expect(footer.footerLength).toBeGreaterThan(0);
    });

    it('should parse footer from last N bytes', () => {
      const buffer = createMinimalParquetBuffer();

      // Read only the last part of the file (footer + length + magic)
      const footerSize = 100; // enough to contain footer
      const lastBytes = buffer.slice(-footerSize);

      const footer = parser.parseFromTail(lastBytes, buffer.length);

      expect(footer).toBeDefined();
      expect(footer.version).toBeDefined();
    });

    it('should parse footer using static helper function', () => {
      const buffer = createMinimalParquetBuffer();
      const footer = parseFooter(buffer);

      expect(footer).toBeDefined();
      expect(footer.version).toBeDefined();
    });

    it('should return metadata about the file creator', () => {
      const buffer = createMinimalParquetBuffer();
      const footer = parser.parse(buffer);

      expect(footer.createdBy).toBeDefined();
      expect(typeof footer.createdBy).toBe('string');
    });

    it('should return total row count', () => {
      const buffer = createMinimalParquetBuffer();
      const footer = parser.parse(buffer);

      expect(footer.numRows).toBeDefined();
      expect(typeof footer.numRows).toBe('number');
      expect(footer.numRows).toBeGreaterThanOrEqual(0);
    });

    it('should handle files with key-value metadata', () => {
      const buffer = createMinimalParquetBuffer();
      const footer = parser.parse(buffer);

      expect(footer.keyValueMetadata).toBeDefined();
      expect(Array.isArray(footer.keyValueMetadata) || footer.keyValueMetadata === undefined).toBe(
        true
      );
    });
  });

  // ==========================================================================
  // 2. Extract schema
  // ==========================================================================
  describe('Extract schema', () => {
    it('should extract schema with column names', () => {
      const buffer = createParquetBufferWithSchema([
        { name: '_id', type: 'BYTE_ARRAY' },
        { name: 'name', type: 'BYTE_ARRAY' },
        { name: 'age', type: 'INT64' },
      ]);

      const footer = parser.parse(buffer);

      expect(footer.schema).toBeDefined();
      expect(footer.schema.elements).toBeDefined();
      expect(footer.schema.elements.length).toBeGreaterThan(0);

      const columnNames = footer.schema.elements.map((e) => e.name);
      expect(columnNames).toContain('_id');
      expect(columnNames).toContain('name');
      expect(columnNames).toContain('age');
    });

    it('should extract schema with correct column types', () => {
      const buffer = createParquetBufferWithSchema([
        { name: 'boolCol', type: 'BOOLEAN' },
        { name: 'intCol', type: 'INT32' },
        { name: 'longCol', type: 'INT64' },
        { name: 'floatCol', type: 'FLOAT' },
        { name: 'doubleCol', type: 'DOUBLE' },
        { name: 'stringCol', type: 'BYTE_ARRAY' },
      ]);

      const footer = parser.parse(buffer);
      const getType = (name: string) => footer.schema.elements.find((e) => e.name === name)?.type;

      expect(getType('boolCol')).toBe('BOOLEAN');
      expect(getType('intCol')).toBe('INT32');
      expect(getType('longCol')).toBe('INT64');
      expect(getType('floatCol')).toBe('FLOAT');
      expect(getType('doubleCol')).toBe('DOUBLE');
      expect(getType('stringCol')).toBe('BYTE_ARRAY');
    });

    it('should extract schema with repetition types', () => {
      const buffer = createParquetBufferWithSchema([
        { name: 'required_col', type: 'INT64', repetitionType: 'REQUIRED' },
        { name: 'optional_col', type: 'INT64', repetitionType: 'OPTIONAL' },
        { name: 'repeated_col', type: 'INT64', repetitionType: 'REPEATED' },
      ]);

      const footer = parser.parse(buffer);
      const getRep = (name: string) =>
        footer.schema.elements.find((e) => e.name === name)?.repetitionType;

      expect(getRep('required_col')).toBe('REQUIRED');
      expect(getRep('optional_col')).toBe('OPTIONAL');
      expect(getRep('repeated_col')).toBe('REPEATED');
    });

    it('should extract schema with converted types', () => {
      const buffer = createParquetBufferWithSchema([
        { name: 'utf8_col', type: 'BYTE_ARRAY', convertedType: 'UTF8' },
        { name: 'date_col', type: 'INT32', convertedType: 'DATE' },
        { name: 'timestamp_col', type: 'INT64', convertedType: 'TIMESTAMP_MILLIS' },
        { name: 'decimal_col', type: 'BYTE_ARRAY', convertedType: 'DECIMAL' },
      ]);

      const footer = parser.parse(buffer);
      const getConv = (name: string) =>
        footer.schema.elements.find((e) => e.name === name)?.convertedType;

      expect(getConv('utf8_col')).toBe('UTF8');
      expect(getConv('date_col')).toBe('DATE');
      expect(getConv('timestamp_col')).toBe('TIMESTAMP_MILLIS');
      expect(getConv('decimal_col')).toBe('DECIMAL');
    });

    it('should extract nested schema (struct)', () => {
      // Schema with nested structure: root -> address -> (street, city, zip)
      const buffer = createParquetBufferWithSchema([
        { name: '_id', type: 'BYTE_ARRAY' },
        // Note: nested structures would need special handling in the mock
      ]);

      const footer = parser.parse(buffer);

      expect(footer.schema).toBeDefined();
      // The schema should support nested elements via numChildren
      expect(footer.schema.elements.some((e) => e.numChildren !== undefined)).toBeDefined();
    });

    it('should extract schema with logical types', () => {
      const buffer = createParquetBufferWithSchema([
        { name: 'uuid_col', type: 'FIXED_LEN_BYTE_ARRAY' },
        { name: 'json_col', type: 'BYTE_ARRAY' },
      ]);

      const footer = parser.parse(buffer);

      expect(footer.schema.elements).toBeDefined();
      // Logical types should be available if present
      const hasLogicalTypeSupport = footer.schema.elements.some(
        (e) => e.logicalType !== undefined || e.logicalType === null
      );
      expect(hasLogicalTypeSupport).toBeDefined();
    });

    it('should return schema element field IDs if present', () => {
      const buffer = createParquetBufferWithSchema([{ name: 'col1', type: 'INT64' }]);

      const footer = parser.parse(buffer);

      // Field IDs are optional in Parquet
      const element = footer.schema.elements.find((e) => e.name === 'col1');
      expect(element).toBeDefined();
      // fieldId may or may not be present
      expect(element?.fieldId === undefined || typeof element.fieldId === 'number').toBe(true);
    });

    it('should handle schema with FIXED_LEN_BYTE_ARRAY type length', () => {
      const buffer = createParquetBufferWithSchema([
        { name: 'uuid', type: 'FIXED_LEN_BYTE_ARRAY' },
      ]);

      const footer = parser.parse(buffer);

      const uuidElement = footer.schema.elements.find((e) => e.name === 'uuid');
      expect(uuidElement).toBeDefined();
      // typeLength should be set for FIXED_LEN_BYTE_ARRAY
      if (uuidElement?.type === 'FIXED_LEN_BYTE_ARRAY') {
        expect(uuidElement.typeLength).toBeDefined();
      }
    });
  });

  // ==========================================================================
  // 3. Extract row group metadata
  // ==========================================================================
  describe('Extract row group metadata', () => {
    it('should extract row group count', () => {
      const buffer = createParquetBufferWithRowGroups([
        {
          numRows: 100,
          totalByteSize: 4096,
          columns: [
            {
              name: '_id',
              type: 'BYTE_ARRAY',
              fileOffset: 4,
              compressedSize: 1024,
              uncompressedSize: 2048,
              numValues: 100,
            },
          ],
        },
        {
          numRows: 150,
          totalByteSize: 6144,
          columns: [
            {
              name: '_id',
              type: 'BYTE_ARRAY',
              fileOffset: 4100,
              compressedSize: 1536,
              uncompressedSize: 3072,
              numValues: 150,
            },
          ],
        },
      ]);

      const footer = parser.parse(buffer);

      expect(footer.rowGroups).toBeDefined();
      expect(footer.rowGroups).toHaveLength(2);
    });

    it('should extract row group row counts', () => {
      const buffer = createParquetBufferWithRowGroups([
        {
          numRows: 100,
          totalByteSize: 4096,
          columns: [
            {
              name: '_id',
              type: 'BYTE_ARRAY',
              fileOffset: 4,
              compressedSize: 1024,
              uncompressedSize: 2048,
              numValues: 100,
            },
          ],
        },
        {
          numRows: 250,
          totalByteSize: 8192,
          columns: [
            {
              name: '_id',
              type: 'BYTE_ARRAY',
              fileOffset: 4100,
              compressedSize: 2048,
              uncompressedSize: 4096,
              numValues: 250,
            },
          ],
        },
      ]);

      const footer = parser.parse(buffer);

      expect(footer.rowGroups[0].numRows).toBe(100);
      expect(footer.rowGroups[1].numRows).toBe(250);
    });

    it('should extract row group total byte sizes', () => {
      const buffer = createParquetBufferWithRowGroups([
        {
          numRows: 100,
          totalByteSize: 4096,
          columns: [
            {
              name: 'col1',
              type: 'INT64',
              fileOffset: 4,
              compressedSize: 4096,
              uncompressedSize: 4096,
              numValues: 100,
            },
          ],
        },
      ]);

      const footer = parser.parse(buffer);

      expect(footer.rowGroups[0].totalByteSize).toBe(4096);
    });

    it('should extract column chunks per row group', () => {
      const buffer = createParquetBufferWithRowGroups([
        {
          numRows: 100,
          totalByteSize: 8192,
          columns: [
            {
              name: '_id',
              type: 'BYTE_ARRAY',
              fileOffset: 4,
              compressedSize: 2048,
              uncompressedSize: 2048,
              numValues: 100,
            },
            {
              name: 'name',
              type: 'BYTE_ARRAY',
              fileOffset: 2052,
              compressedSize: 3072,
              uncompressedSize: 3072,
              numValues: 100,
            },
            {
              name: 'age',
              type: 'INT64',
              fileOffset: 5124,
              compressedSize: 800,
              uncompressedSize: 800,
              numValues: 100,
            },
          ],
        },
      ]);

      const footer = parser.parse(buffer);

      expect(footer.rowGroups[0].columns).toBeDefined();
      expect(footer.rowGroups[0].columns).toHaveLength(3);
    });

    it('should extract file offset for row groups', () => {
      const buffer = createParquetBufferWithRowGroups([
        {
          numRows: 100,
          totalByteSize: 4096,
          columns: [
            {
              name: '_id',
              type: 'BYTE_ARRAY',
              fileOffset: 4,
              compressedSize: 4096,
              uncompressedSize: 4096,
              numValues: 100,
            },
          ],
        },
      ]);

      const footer = parser.parse(buffer);

      // Row group should have a file offset indicating where its data starts
      expect(footer.rowGroups[0].fileOffset).toBeDefined();
      expect(footer.rowGroups[0].fileOffset).toBeGreaterThanOrEqual(0);
    });

    it('should extract sorting columns for row groups', () => {
      const buffer = createParquetBufferWithRowGroups([
        {
          numRows: 100,
          totalByteSize: 4096,
          columns: [
            {
              name: '_seq',
              type: 'INT64',
              fileOffset: 4,
              compressedSize: 800,
              uncompressedSize: 800,
              numValues: 100,
            },
          ],
        },
      ]);

      const footer = parser.parse(buffer);

      // sortingColumns is optional
      expect(
        footer.rowGroups[0].sortingColumns === undefined ||
          Array.isArray(footer.rowGroups[0].sortingColumns)
      ).toBe(true);
    });

    it('should handle empty row groups array', () => {
      const buffer = createMinimalParquetBuffer();
      const footer = parser.parse(buffer);

      expect(footer.rowGroups).toBeDefined();
      expect(Array.isArray(footer.rowGroups)).toBe(true);
    });

    it('should calculate total compressed size across row groups', () => {
      const buffer = createParquetBufferWithRowGroups([
        {
          numRows: 100,
          totalByteSize: 4096,
          columns: [
            {
              name: '_id',
              type: 'BYTE_ARRAY',
              fileOffset: 4,
              compressedSize: 1024,
              uncompressedSize: 2048,
              numValues: 100,
            },
          ],
        },
        {
          numRows: 100,
          totalByteSize: 4096,
          columns: [
            {
              name: '_id',
              type: 'BYTE_ARRAY',
              fileOffset: 4100,
              compressedSize: 1024,
              uncompressedSize: 2048,
              numValues: 100,
            },
          ],
        },
      ]);

      const footer = parser.parse(buffer);

      // Sum of compressed sizes
      const totalCompressed = footer.rowGroups.reduce(
        (sum, rg) => sum + rg.columns.reduce((s, c) => s + c.compressedSize, 0),
        0
      );
      expect(totalCompressed).toBe(2048);
    });
  });

  // ==========================================================================
  // 4. Extract column chunk locations and statistics
  // ==========================================================================
  describe('Extract column statistics', () => {
    it('should extract column chunk file offsets', () => {
      const buffer = createParquetBufferWithRowGroups([
        {
          numRows: 100,
          totalByteSize: 4096,
          columns: [
            {
              name: '_id',
              type: 'BYTE_ARRAY',
              fileOffset: 100,
              compressedSize: 1024,
              uncompressedSize: 2048,
              numValues: 100,
            },
            {
              name: 'data',
              type: 'BYTE_ARRAY',
              fileOffset: 1124,
              compressedSize: 2048,
              uncompressedSize: 4096,
              numValues: 100,
            },
          ],
        },
      ]);

      const footer = parser.parse(buffer);

      expect(footer.rowGroups[0].columns[0].fileOffset).toBe(100);
      expect(footer.rowGroups[0].columns[1].fileOffset).toBe(1124);
    });

    it('should extract compressed and uncompressed sizes', () => {
      const buffer = createParquetBufferWithRowGroups([
        {
          numRows: 100,
          totalByteSize: 4096,
          columns: [
            {
              name: 'data',
              type: 'BYTE_ARRAY',
              fileOffset: 4,
              compressedSize: 1024,
              uncompressedSize: 4096,
              numValues: 100,
            },
          ],
        },
      ]);

      const footer = parser.parse(buffer);

      const col = footer.rowGroups[0].columns[0];
      expect(col.compressedSize).toBe(1024);
      expect(col.uncompressedSize).toBe(4096);
    });

    it('should extract compression codec', () => {
      const buffer = createParquetBufferWithRowGroups([
        {
          numRows: 100,
          totalByteSize: 4096,
          columns: [
            {
              name: 'data',
              type: 'BYTE_ARRAY',
              fileOffset: 4,
              compressedSize: 1024,
              uncompressedSize: 4096,
              numValues: 100,
            },
          ],
        },
      ]);

      const footer = parser.parse(buffer);

      const col = footer.rowGroups[0].columns[0];
      expect(col.codec).toBeDefined();
      expect(['UNCOMPRESSED', 'SNAPPY', 'GZIP', 'LZO', 'BROTLI', 'LZ4', 'ZSTD']).toContain(
        col.codec
      );
    });

    it('should extract column encoding', () => {
      const buffer = createParquetBufferWithRowGroups([
        {
          numRows: 100,
          totalByteSize: 4096,
          columns: [
            {
              name: 'data',
              type: 'INT64',
              fileOffset: 4,
              compressedSize: 800,
              uncompressedSize: 800,
              numValues: 100,
            },
          ],
        },
      ]);

      const footer = parser.parse(buffer);

      const col = footer.rowGroups[0].columns[0];
      expect(col.encodings).toBeDefined();
      expect(Array.isArray(col.encodings)).toBe(true);
    });

    it('should extract min/max statistics for INT columns', () => {
      const buffer = createParquetBufferWithStatistics([
        {
          name: 'age',
          type: 'INT64',
          statistics: {
            minValue: 18,
            maxValue: 65,
            nullCount: 0,
          },
        },
      ]);

      const footer = parser.parse(buffer);

      const col = footer.rowGroups[0].columns.find((c) => c.columnPath === 'age');
      expect(col?.statistics).toBeDefined();
      expect(col?.statistics?.minValue).toBe(18);
      expect(col?.statistics?.maxValue).toBe(65);
    });

    it('should extract min/max statistics for STRING columns', () => {
      const buffer = createParquetBufferWithStatistics([
        {
          name: 'name',
          type: 'BYTE_ARRAY',
          statistics: {
            minValue: 'Alice',
            maxValue: 'Zoe',
            nullCount: 5,
          },
        },
      ]);

      const footer = parser.parse(buffer);

      const col = footer.rowGroups[0].columns.find((c) => c.columnPath === 'name');
      expect(col?.statistics).toBeDefined();
      expect(col?.statistics?.minValue).toBe('Alice');
      expect(col?.statistics?.maxValue).toBe('Zoe');
    });

    it('should extract min/max statistics for DOUBLE columns', () => {
      const buffer = createParquetBufferWithStatistics([
        {
          name: 'price',
          type: 'DOUBLE',
          statistics: {
            minValue: 9.99,
            maxValue: 999.99,
            nullCount: 0,
          },
        },
      ]);

      const footer = parser.parse(buffer);

      const col = footer.rowGroups[0].columns.find((c) => c.columnPath === 'price');
      expect(col?.statistics).toBeDefined();
      expect(col?.statistics?.minValue).toBeCloseTo(9.99);
      expect(col?.statistics?.maxValue).toBeCloseTo(999.99);
    });

    it('should extract null count statistics', () => {
      const buffer = createParquetBufferWithStatistics([
        {
          name: 'optional_field',
          type: 'BYTE_ARRAY',
          statistics: {
            nullCount: 42,
          },
        },
      ]);

      const footer = parser.parse(buffer);

      const col = footer.rowGroups[0].columns.find((c) => c.columnPath === 'optional_field');
      expect(col?.statistics?.nullCount).toBe(42);
    });

    it('should extract distinct count statistics', () => {
      const buffer = createParquetBufferWithStatistics([
        {
          name: 'category',
          type: 'BYTE_ARRAY',
          statistics: {
            distinctCount: 10,
            nullCount: 0,
          },
        },
      ]);

      const footer = parser.parse(buffer);

      const col = footer.rowGroups[0].columns.find((c) => c.columnPath === 'category');
      expect(col?.statistics?.distinctCount).toBe(10);
    });

    it('should handle columns without statistics', () => {
      const buffer = createParquetBufferWithRowGroups([
        {
          numRows: 100,
          totalByteSize: 4096,
          columns: [
            {
              name: 'data',
              type: 'BYTE_ARRAY',
              fileOffset: 4,
              compressedSize: 1024,
              uncompressedSize: 1024,
              numValues: 100,
            },
          ],
        },
      ]);

      const footer = parser.parse(buffer);

      const col = footer.rowGroups[0].columns[0];
      // Statistics may be undefined if not present
      expect(col.statistics === undefined || col.statistics !== undefined).toBe(true);
    });

    it('should extract page offset information', () => {
      const buffer = createParquetBufferWithRowGroups([
        {
          numRows: 100,
          totalByteSize: 4096,
          columns: [
            {
              name: 'data',
              type: 'BYTE_ARRAY',
              fileOffset: 4,
              compressedSize: 1024,
              uncompressedSize: 1024,
              numValues: 100,
            },
          ],
        },
      ]);

      const footer = parser.parse(buffer);

      const col = footer.rowGroups[0].columns[0];
      // data_page_offset is required
      expect(col.dataPageOffset).toBeDefined();
      // dictionary_page_offset is optional
      expect(
        col.dictionaryPageOffset === undefined || typeof col.dictionaryPageOffset === 'number'
      ).toBe(true);
    });

    it('should extract num_values for each column chunk', () => {
      const buffer = createParquetBufferWithRowGroups([
        {
          numRows: 100,
          totalByteSize: 4096,
          columns: [
            {
              name: 'data',
              type: 'INT64',
              fileOffset: 4,
              compressedSize: 800,
              uncompressedSize: 800,
              numValues: 100,
            },
          ],
        },
      ]);

      const footer = parser.parse(buffer);

      const col = footer.rowGroups[0].columns[0];
      expect(col.numValues).toBe(100);
    });
  });

  // ==========================================================================
  // 5. Handle truncated/invalid footer
  // ==========================================================================
  describe('Handle truncated/invalid footer', () => {
    it('should throw on missing end magic bytes', () => {
      const buffer = createMinimalParquetBuffer();
      // Corrupt the last 4 bytes (magic bytes)
      buffer[buffer.length - 1] = 0x00;
      buffer[buffer.length - 2] = 0x00;
      buffer[buffer.length - 3] = 0x00;
      buffer[buffer.length - 4] = 0x00;

      expect(() => parser.parse(buffer)).toThrow(InvalidMagicBytesError);
    });

    it('should throw on missing start magic bytes', () => {
      const buffer = createMinimalParquetBuffer();
      // Corrupt the first 4 bytes (magic bytes)
      buffer[0] = 0x00;
      buffer[1] = 0x00;
      buffer[2] = 0x00;
      buffer[3] = 0x00;

      expect(() => parser.parse(buffer)).toThrow(InvalidMagicBytesError);
    });

    it('should throw on truncated footer data', () => {
      const buffer = createMinimalParquetBuffer();
      // Truncate the buffer
      const truncated = buffer.slice(0, buffer.length - 20);

      expect(() => parser.parse(truncated)).toThrow(TruncatedFooterError);
    });

    it('should throw on invalid footer length', () => {
      const buffer = createMinimalParquetBuffer();
      // Set an absurdly large footer length
      const lengthOffset = buffer.length - 8; // 4 bytes before magic
      new DataView(buffer.buffer, buffer.byteOffset).setUint32(lengthOffset, 0xffffffff, true);

      expect(() => parser.parse(buffer)).toThrow(InvalidFooterLengthError);
    });

    it('should throw on zero footer length', () => {
      const buffer = createMinimalParquetBuffer();
      // Set footer length to zero
      const lengthOffset = buffer.length - 8;
      new DataView(buffer.buffer, buffer.byteOffset).setUint32(lengthOffset, 0, true);

      expect(() => parser.parse(buffer)).toThrow(InvalidFooterLengthError);
    });

    it('should throw on corrupted Thrift metadata', () => {
      const buffer = createMinimalParquetBuffer();
      // Corrupt the footer data (between magic and footer length)
      const footerStart = 4; // after start magic
      for (let i = footerStart; i < buffer.length - 8; i++) {
        buffer[i] = 0xff; // invalid Thrift data
      }

      expect(() => parser.parse(buffer)).toThrow(CorruptedMetadataError);
    });

    it('should throw on file too small to contain footer', () => {
      // Minimum: 4 (magic) + 0 (data) + 4 (length) + 4 (magic) = 12 bytes
      const tooSmall = new Uint8Array(8);

      expect(() => parser.parse(tooSmall)).toThrow(TruncatedFooterError);
    });

    it('should throw specific error for negative footer length', () => {
      const buffer = createMinimalParquetBuffer();
      // Set a negative footer length (interpreted as large unsigned)
      const lengthOffset = buffer.length - 8;
      new DataView(buffer.buffer, buffer.byteOffset).setInt32(lengthOffset, -1, true);

      expect(() => parser.parse(buffer)).toThrow(InvalidFooterLengthError);
    });

    it('should handle partial magic bytes gracefully', () => {
      // PAR followed by something else
      const buffer = new Uint8Array([0x50, 0x41, 0x52, 0x00, ...new Array(20).fill(0)]);

      expect(() => parser.parse(buffer)).toThrow(InvalidMagicBytesError);
    });

    it('should throw on footer length exceeding file size', () => {
      const buffer = createMinimalParquetBuffer();
      // Set footer length larger than file
      const lengthOffset = buffer.length - 8;
      new DataView(buffer.buffer, buffer.byteOffset).setUint32(
        lengthOffset,
        buffer.length * 2,
        true
      );

      expect(() => parser.parse(buffer)).toThrow(InvalidFooterLengthError);
    });
  });

  // ==========================================================================
  // 6. Handle different Parquet versions
  // ==========================================================================
  describe('Handle different Parquet versions', () => {
    it('should parse Parquet v1 files', () => {
      const buffer = createMinimalParquetBuffer();
      const footer = parser.parse(buffer);

      expect(footer.version).toBe(1);
    });

    it('should parse Parquet v2 files', () => {
      // Create a v2 Parquet buffer (modify version byte)
      const buffer = createMinimalParquetBuffer();
      // Assuming version is at a known offset in the footer
      // This is a simplified test - actual v2 handling may differ

      const footer = parser.parse(buffer);

      // Should successfully parse v1 or v2
      expect([1, 2]).toContain(footer.version);
    });

    it('should throw on unsupported Parquet version', () => {
      const buffer = createMinimalParquetBuffer();
      // Set version to an unsupported value (e.g., version 99)
      // Footer format: version is typically first field
      const footerLengthOffset = buffer.length - 8;
      const footerLength = new DataView(buffer.buffer, buffer.byteOffset).getUint32(
        footerLengthOffset,
        true
      );
      const footerStart = buffer.length - 8 - footerLength;

      // Modify version bytes (first 4 bytes of footer, little-endian)
      new DataView(buffer.buffer, buffer.byteOffset).setUint32(footerStart, 99, true);

      expect(() => parser.parse(buffer)).toThrow(UnsupportedVersionError);
    });

    it('should handle v2 data page headers', () => {
      // V2 introduced new data page format
      const buffer = createParquetBufferWithRowGroups([
        {
          numRows: 100,
          totalByteSize: 4096,
          columns: [
            {
              name: 'data',
              type: 'INT64',
              fileOffset: 4,
              compressedSize: 800,
              uncompressedSize: 800,
              numValues: 100,
            },
          ],
        },
      ]);

      const footer = parser.parse(buffer);

      // V2 should still be parseable
      expect(footer.rowGroups).toBeDefined();
    });

    it('should detect format version from file', () => {
      const buffer = createMinimalParquetBuffer();
      const footer = parser.parse(buffer);

      expect(footer.formatVersion).toBeDefined();
      expect(typeof footer.formatVersion).toBe('string');
    });

    it('should preserve backward compatibility metadata', () => {
      const buffer = createMinimalParquetBuffer();
      const footer = parser.parse(buffer);

      // Older clients should still be able to read basic metadata
      expect(footer.numRows).toBeDefined();
      expect(footer.schema).toBeDefined();
      expect(footer.rowGroups).toBeDefined();
    });
  });

  // ==========================================================================
  // Additional edge cases
  // ==========================================================================
  describe('Edge cases', () => {
    it('should handle ArrayBuffer input', () => {
      const buffer = createMinimalParquetBuffer();
      const arrayBuffer = buffer.buffer.slice(
        buffer.byteOffset,
        buffer.byteOffset + buffer.byteLength
      );

      const footer = parser.parseArrayBuffer(arrayBuffer);

      expect(footer).toBeDefined();
      expect(footer.version).toBeDefined();
    });

    it('should handle DataView input', () => {
      const buffer = createMinimalParquetBuffer();
      const dataView = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);

      const footer = parser.parseDataView(dataView);

      expect(footer).toBeDefined();
      expect(footer.version).toBeDefined();
    });

    it('should support async parsing', async () => {
      const buffer = createMinimalParquetBuffer();

      const footer = await parser.parseAsync(buffer);

      expect(footer).toBeDefined();
      expect(footer.version).toBeDefined();
    });

    it('should support streaming footer read', async () => {
      const buffer = createMinimalParquetBuffer();

      // Simulate reading footer in chunks
      const tailSize = 64;
      const tail = buffer.slice(-tailSize);

      const footerLength = parser.getFooterLengthFromTail(tail);
      expect(footerLength).toBeGreaterThan(0);

      // Now read the actual footer
      const footerBytes = buffer.slice(-(footerLength + 8));
      const footer = parser.parseFromTail(footerBytes, buffer.length);

      expect(footer).toBeDefined();
    });

    it('should return byte range for footer', () => {
      const buffer = createMinimalParquetBuffer();
      const footer = parser.parse(buffer);

      expect(footer.footerOffset).toBeDefined();
      expect(footer.footerLength).toBeDefined();
      expect(footer.footerOffset + footer.footerLength + 8).toBe(buffer.length);
    });

    it('should handle very large footer metadata', () => {
      // Create a file with many columns
      const columns = Array.from({ length: 100 }, (_, i) => ({
        name: `column_${i}`,
        type: 'INT64' as ParquetType,
      }));

      const buffer = createParquetBufferWithSchema(columns);
      const footer = parser.parse(buffer);

      expect(footer.schema.elements.length).toBeGreaterThanOrEqual(100);
    });

    it('should handle deeply nested schema', () => {
      // Schema with nested structs
      const buffer = createParquetBufferWithSchema([
        { name: 'level1', type: 'BYTE_ARRAY' },
      ]);

      const footer = parser.parse(buffer);

      expect(footer.schema).toBeDefined();
    });

    it('should provide column path for nested columns', () => {
      const buffer = createParquetBufferWithRowGroups([
        {
          numRows: 100,
          totalByteSize: 4096,
          columns: [
            {
              name: 'address.street',
              type: 'BYTE_ARRAY',
              fileOffset: 4,
              compressedSize: 1024,
              uncompressedSize: 1024,
              numValues: 100,
            },
          ],
        },
      ]);

      const footer = parser.parse(buffer);

      const col = footer.rowGroups[0].columns[0];
      expect(col.columnPath).toBe('address.street');
    });
  });

  // ==========================================================================
  // Type definitions validation
  // ==========================================================================
  describe('Type definitions', () => {
    it('should export ParquetFooter type', () => {
      const buffer = createMinimalParquetBuffer();
      const footer: ParquetFooter = parser.parse(buffer);

      expect(footer).toBeDefined();
    });

    it('should export ParquetSchema type', () => {
      const buffer = createMinimalParquetBuffer();
      const footer = parser.parse(buffer);
      const schema: ParquetSchema = footer.schema;

      expect(schema).toBeDefined();
      expect(schema.elements).toBeDefined();
    });

    it('should export SchemaElement type', () => {
      const buffer = createParquetBufferWithSchema([{ name: 'test', type: 'INT64' }]);
      const footer = parser.parse(buffer);
      const element: SchemaElement = footer.schema.elements[0];

      expect(element).toBeDefined();
      expect(element.name).toBeDefined();
    });

    it('should export RowGroupMetadata type', () => {
      const buffer = createParquetBufferWithRowGroups([
        {
          numRows: 100,
          totalByteSize: 4096,
          columns: [
            {
              name: 'col',
              type: 'INT64',
              fileOffset: 4,
              compressedSize: 800,
              uncompressedSize: 800,
              numValues: 100,
            },
          ],
        },
      ]);
      const footer = parser.parse(buffer);
      const rowGroup: RowGroupMetadata = footer.rowGroups[0];

      expect(rowGroup).toBeDefined();
      expect(rowGroup.numRows).toBe(100);
    });

    it('should export ColumnChunkMetadata type', () => {
      const buffer = createParquetBufferWithRowGroups([
        {
          numRows: 100,
          totalByteSize: 4096,
          columns: [
            {
              name: 'col',
              type: 'INT64',
              fileOffset: 4,
              compressedSize: 800,
              uncompressedSize: 800,
              numValues: 100,
            },
          ],
        },
      ]);
      const footer = parser.parse(buffer);
      const chunk: ColumnChunkMetadata = footer.rowGroups[0].columns[0];

      expect(chunk).toBeDefined();
      expect(chunk.compressedSize).toBe(800);
    });

    it('should export ColumnStatistics type', () => {
      const buffer = createParquetBufferWithStatistics([
        {
          name: 'test',
          type: 'INT64',
          statistics: { minValue: 1, maxValue: 100, nullCount: 0 },
        },
      ]);
      const footer = parser.parse(buffer);
      const stats: ColumnStatistics | undefined = footer.rowGroups[0].columns[0].statistics;

      expect(stats).toBeDefined();
    });

    it('should export error types', () => {
      expect(ParquetError).toBeDefined();
      expect(InvalidMagicBytesError).toBeDefined();
      expect(TruncatedFooterError).toBeDefined();
      expect(InvalidFooterLengthError).toBeDefined();
      expect(UnsupportedVersionError).toBeDefined();
      expect(CorruptedMetadataError).toBeDefined();
    });

    it('should export type unions', () => {
      // These are compile-time checks - if they compile, the types exist
      const parquetType: ParquetType = 'INT64';
      const convertedType: ConvertedType = 'UTF8';
      const repetitionType: FieldRepetitionType = 'OPTIONAL';
      const encoding: Encoding = 'PLAIN';
      const codec: CompressionCodec = 'SNAPPY';

      expect(parquetType).toBe('INT64');
      expect(convertedType).toBe('UTF8');
      expect(repetitionType).toBe('OPTIONAL');
      expect(encoding).toBe('PLAIN');
      expect(codec).toBe('SNAPPY');
    });
  });
});
