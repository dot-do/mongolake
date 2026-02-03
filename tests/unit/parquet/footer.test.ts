/**
 * Parquet Footer Generator Tests
 *
 * Tests for generating Parquet file footers with schema, row group metadata,
 * column chunk metadata, and statistics.
 *
 * The footer is the critical metadata section at the end of a Parquet file that
 * allows readers to understand the file structure without scanning the entire file.
 *
 * Binary format:
 * [row groups data...] [footer metadata] [footer length (4 bytes LE)] [magic "PAR1"]
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  FooterGenerator,
  type FooterGeneratorOptions,
  type ParquetFooter,
  type RowGroupMetadata,
  type ColumnChunkMetadata,
  type ColumnStatistics,
  type SchemaElement,
  type ParquetType,
  type ConvertedType,
  type RepetitionType,
  type Encoding,
  type CompressionCodec,
} from '../../../src/parquet/footer.js';

// ============================================================================
// Constants
// ============================================================================

const PARQUET_MAGIC = new Uint8Array([0x50, 0x41, 0x52, 0x31]); // "PAR1"
const FOOTER_LENGTH_BYTES = 4;

// ============================================================================
// Empty File Footer Tests
// ============================================================================

describe('FooterGenerator - Empty file footer', () => {
  let generator: FooterGenerator;

  beforeEach(() => {
    generator = new FooterGenerator();
  });

  it('should generate footer for an empty file with no row groups', () => {
    const footer = generator.generate({
      schema: [],
      rowGroups: [],
    });

    expect(footer).toBeDefined();
    expect(footer.data).toBeInstanceOf(Uint8Array);
    expect(footer.data.byteLength).toBeGreaterThan(0);
  });

  it('should include magic bytes at the end of footer', () => {
    const footer = generator.generate({
      schema: [],
      rowGroups: [],
    });

    const data = footer.data;
    const magic = data.slice(data.byteLength - 4);
    expect(magic).toEqual(PARQUET_MAGIC);
  });

  it('should include footer length before magic bytes', () => {
    const footer = generator.generate({
      schema: [],
      rowGroups: [],
    });

    const data = footer.data;
    // Footer length is stored as 4-byte little-endian before the magic
    const lengthBytes = data.slice(
      data.byteLength - FOOTER_LENGTH_BYTES - 4,
      data.byteLength - 4
    );
    const footerLength = new DataView(lengthBytes.buffer, lengthBytes.byteOffset).getUint32(
      0,
      true
    );

    // The footer length should not include the length field or magic bytes themselves
    expect(footerLength).toBe(data.byteLength - FOOTER_LENGTH_BYTES - 4);
  });

  it('should set numRows to 0 for empty file', () => {
    const footer = generator.generate({
      schema: [],
      rowGroups: [],
    });

    expect(footer.numRows).toBe(0);
  });

  it('should have empty row groups array', () => {
    const footer = generator.generate({
      schema: [],
      rowGroups: [],
    });

    expect(footer.rowGroups).toEqual([]);
  });

  it('should include version in metadata', () => {
    const footer = generator.generate({
      schema: [],
      rowGroups: [],
    });

    expect(footer.version).toBeDefined();
    expect(footer.version).toBeGreaterThanOrEqual(1);
  });

  it('should include created_by in metadata', () => {
    const footer = generator.generate({
      schema: [],
      rowGroups: [],
    });

    expect(footer.createdBy).toBeDefined();
    expect(typeof footer.createdBy).toBe('string');
    expect(footer.createdBy.length).toBeGreaterThan(0);
  });
});

// ============================================================================
// Single Row Group Footer Tests
// ============================================================================

describe('FooterGenerator - Single row group footer', () => {
  let generator: FooterGenerator;

  beforeEach(() => {
    generator = new FooterGenerator();
  });

  it('should generate footer with single row group', () => {
    const footer = generator.generate({
      schema: [
        { name: 'id', type: 'INT64', repetitionType: 'REQUIRED' },
        { name: 'name', type: 'BYTE_ARRAY', repetitionType: 'OPTIONAL' },
      ],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'id',
              fileOffset: 4, // after magic bytes
              compressedSize: 100,
              uncompressedSize: 120,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
            },
            {
              columnName: 'name',
              fileOffset: 104,
              compressedSize: 200,
              uncompressedSize: 250,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
            },
          ],
          numRows: 10,
          totalByteSize: 300,
          fileOffset: 4,
        },
      ],
    });

    expect(footer).toBeDefined();
    expect(footer.rowGroups).toHaveLength(1);
    expect(footer.numRows).toBe(10);
  });

  it('should include row group metadata with correct offsets', () => {
    const footer = generator.generate({
      schema: [{ name: 'value', type: 'INT32', repetitionType: 'REQUIRED' }],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'value',
              fileOffset: 4,
              compressedSize: 500,
              uncompressedSize: 500,
              numValues: 100,
              encoding: 'PLAIN',
              compression: 'none',
            },
          ],
          numRows: 100,
          totalByteSize: 500,
          fileOffset: 4,
        },
      ],
    });

    const rowGroup = footer.rowGroups[0];
    expect(rowGroup.fileOffset).toBe(4);
    expect(rowGroup.totalByteSize).toBe(500);
    expect(rowGroup.numRows).toBe(100);
  });

  it('should include column chunk metadata in row group', () => {
    const footer = generator.generate({
      schema: [
        { name: 'id', type: 'INT64', repetitionType: 'REQUIRED' },
        { name: 'score', type: 'DOUBLE', repetitionType: 'OPTIONAL' },
      ],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'id',
              fileOffset: 4,
              compressedSize: 80,
              uncompressedSize: 80,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
            },
            {
              columnName: 'score',
              fileOffset: 84,
              compressedSize: 80,
              uncompressedSize: 80,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
            },
          ],
          numRows: 10,
          totalByteSize: 160,
          fileOffset: 4,
        },
      ],
    });

    expect(footer.rowGroups[0].columns).toHaveLength(2);
    expect(footer.rowGroups[0].columns[0].columnName).toBe('id');
    expect(footer.rowGroups[0].columns[1].columnName).toBe('score');
  });

  it('should include compression codec in column metadata', () => {
    const footer = generator.generate({
      schema: [{ name: 'data', type: 'BYTE_ARRAY', repetitionType: 'REQUIRED' }],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'data',
              fileOffset: 4,
              compressedSize: 300,
              uncompressedSize: 500,
              numValues: 50,
              encoding: 'PLAIN',
              compression: 'snappy',
            },
          ],
          numRows: 50,
          totalByteSize: 300,
          fileOffset: 4,
        },
      ],
    });

    expect(footer.rowGroups[0].columns[0].compression).toBe('snappy');
    expect(footer.rowGroups[0].columns[0].compressedSize).toBe(300);
    expect(footer.rowGroups[0].columns[0].uncompressedSize).toBe(500);
  });

  it('should include encoding information in column metadata', () => {
    const footer = generator.generate({
      schema: [{ name: 'id', type: 'INT64', repetitionType: 'REQUIRED' }],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'id',
              fileOffset: 4,
              compressedSize: 100,
              uncompressedSize: 100,
              numValues: 100,
              encoding: 'DELTA_BINARY_PACKED',
              compression: 'none',
            },
          ],
          numRows: 100,
          totalByteSize: 100,
          fileOffset: 4,
        },
      ],
    });

    expect(footer.rowGroups[0].columns[0].encoding).toBe('DELTA_BINARY_PACKED');
  });

  it('should track data page offset in column metadata', () => {
    const footer = generator.generate({
      schema: [{ name: 'value', type: 'INT32', repetitionType: 'REQUIRED' }],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'value',
              fileOffset: 4,
              dataPageOffset: 4,
              compressedSize: 200,
              uncompressedSize: 200,
              numValues: 50,
              encoding: 'PLAIN',
              compression: 'none',
            },
          ],
          numRows: 50,
          totalByteSize: 200,
          fileOffset: 4,
        },
      ],
    });

    expect(footer.rowGroups[0].columns[0].dataPageOffset).toBe(4);
  });

  it('should track dictionary page offset when present', () => {
    const footer = generator.generate({
      schema: [{ name: 'category', type: 'BYTE_ARRAY', repetitionType: 'REQUIRED' }],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'category',
              fileOffset: 4,
              dataPageOffset: 104,
              dictionaryPageOffset: 4,
              compressedSize: 300,
              uncompressedSize: 300,
              numValues: 100,
              encoding: 'RLE_DICTIONARY',
              compression: 'none',
            },
          ],
          numRows: 100,
          totalByteSize: 300,
          fileOffset: 4,
        },
      ],
    });

    expect(footer.rowGroups[0].columns[0].dictionaryPageOffset).toBe(4);
    expect(footer.rowGroups[0].columns[0].dataPageOffset).toBe(104);
  });
});

// ============================================================================
// Multiple Row Groups Footer Tests
// ============================================================================

describe('FooterGenerator - Multiple row groups', () => {
  let generator: FooterGenerator;

  beforeEach(() => {
    generator = new FooterGenerator();
  });

  it('should generate footer with multiple row groups', () => {
    const footer = generator.generate({
      schema: [{ name: 'id', type: 'INT64', repetitionType: 'REQUIRED' }],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'id',
              fileOffset: 4,
              compressedSize: 100,
              uncompressedSize: 100,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
            },
          ],
          numRows: 10,
          totalByteSize: 100,
          fileOffset: 4,
        },
        {
          columns: [
            {
              columnName: 'id',
              fileOffset: 104,
              compressedSize: 100,
              uncompressedSize: 100,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
            },
          ],
          numRows: 10,
          totalByteSize: 100,
          fileOffset: 104,
        },
        {
          columns: [
            {
              columnName: 'id',
              fileOffset: 204,
              compressedSize: 100,
              uncompressedSize: 100,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
            },
          ],
          numRows: 10,
          totalByteSize: 100,
          fileOffset: 204,
        },
      ],
    });

    expect(footer.rowGroups).toHaveLength(3);
    expect(footer.numRows).toBe(30);
  });

  it('should track correct offsets for each row group', () => {
    const footer = generator.generate({
      schema: [{ name: 'value', type: 'DOUBLE', repetitionType: 'REQUIRED' }],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'value',
              fileOffset: 4,
              compressedSize: 800,
              uncompressedSize: 800,
              numValues: 100,
              encoding: 'PLAIN',
              compression: 'none',
            },
          ],
          numRows: 100,
          totalByteSize: 800,
          fileOffset: 4,
        },
        {
          columns: [
            {
              columnName: 'value',
              fileOffset: 804,
              compressedSize: 400,
              uncompressedSize: 400,
              numValues: 50,
              encoding: 'PLAIN',
              compression: 'none',
            },
          ],
          numRows: 50,
          totalByteSize: 400,
          fileOffset: 804,
        },
      ],
    });

    expect(footer.rowGroups[0].fileOffset).toBe(4);
    expect(footer.rowGroups[0].totalByteSize).toBe(800);
    expect(footer.rowGroups[1].fileOffset).toBe(804);
    expect(footer.rowGroups[1].totalByteSize).toBe(400);
  });

  it('should calculate total row count across all row groups', () => {
    const footer = generator.generate({
      schema: [{ name: 'id', type: 'INT32', repetitionType: 'REQUIRED' }],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'id',
              fileOffset: 4,
              compressedSize: 40,
              uncompressedSize: 40,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
            },
          ],
          numRows: 10,
          totalByteSize: 40,
          fileOffset: 4,
        },
        {
          columns: [
            {
              columnName: 'id',
              fileOffset: 44,
              compressedSize: 120,
              uncompressedSize: 120,
              numValues: 30,
              encoding: 'PLAIN',
              compression: 'none',
            },
          ],
          numRows: 30,
          totalByteSize: 120,
          fileOffset: 44,
        },
        {
          columns: [
            {
              columnName: 'id',
              fileOffset: 164,
              compressedSize: 200,
              uncompressedSize: 200,
              numValues: 50,
              encoding: 'PLAIN',
              compression: 'none',
            },
          ],
          numRows: 50,
          totalByteSize: 200,
          fileOffset: 164,
        },
      ],
    });

    expect(footer.numRows).toBe(90);
  });

  it('should handle row groups with different compressions', () => {
    const footer = generator.generate({
      schema: [{ name: 'data', type: 'BYTE_ARRAY', repetitionType: 'REQUIRED' }],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'data',
              fileOffset: 4,
              compressedSize: 500,
              uncompressedSize: 1000,
              numValues: 100,
              encoding: 'PLAIN',
              compression: 'snappy',
            },
          ],
          numRows: 100,
          totalByteSize: 500,
          fileOffset: 4,
        },
        {
          columns: [
            {
              columnName: 'data',
              fileOffset: 504,
              compressedSize: 400,
              uncompressedSize: 1000,
              numValues: 100,
              encoding: 'PLAIN',
              compression: 'zstd',
            },
          ],
          numRows: 100,
          totalByteSize: 400,
          fileOffset: 504,
        },
      ],
    });

    expect(footer.rowGroups[0].columns[0].compression).toBe('snappy');
    expect(footer.rowGroups[1].columns[0].compression).toBe('zstd');
  });

  it('should handle row groups with multiple columns', () => {
    const footer = generator.generate({
      schema: [
        { name: 'id', type: 'INT64', repetitionType: 'REQUIRED' },
        { name: 'name', type: 'BYTE_ARRAY', repetitionType: 'OPTIONAL' },
        { name: 'score', type: 'DOUBLE', repetitionType: 'OPTIONAL' },
      ],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'id',
              fileOffset: 4,
              compressedSize: 80,
              uncompressedSize: 80,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
            },
            {
              columnName: 'name',
              fileOffset: 84,
              compressedSize: 100,
              uncompressedSize: 100,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
            },
            {
              columnName: 'score',
              fileOffset: 184,
              compressedSize: 80,
              uncompressedSize: 80,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
            },
          ],
          numRows: 10,
          totalByteSize: 260,
          fileOffset: 4,
        },
        {
          columns: [
            {
              columnName: 'id',
              fileOffset: 264,
              compressedSize: 160,
              uncompressedSize: 160,
              numValues: 20,
              encoding: 'PLAIN',
              compression: 'none',
            },
            {
              columnName: 'name',
              fileOffset: 424,
              compressedSize: 200,
              uncompressedSize: 200,
              numValues: 20,
              encoding: 'PLAIN',
              compression: 'none',
            },
            {
              columnName: 'score',
              fileOffset: 624,
              compressedSize: 160,
              uncompressedSize: 160,
              numValues: 20,
              encoding: 'PLAIN',
              compression: 'none',
            },
          ],
          numRows: 20,
          totalByteSize: 520,
          fileOffset: 264,
        },
      ],
    });

    expect(footer.rowGroups).toHaveLength(2);
    expect(footer.rowGroups[0].columns).toHaveLength(3);
    expect(footer.rowGroups[1].columns).toHaveLength(3);
    expect(footer.numRows).toBe(30);
  });
});

// ============================================================================
// Column Statistics Serialization Tests
// ============================================================================

describe('FooterGenerator - Column statistics serialization', () => {
  let generator: FooterGenerator;

  beforeEach(() => {
    generator = new FooterGenerator();
  });

  it('should include min/max statistics for INT32 columns', () => {
    const footer = generator.generate({
      schema: [{ name: 'age', type: 'INT32', repetitionType: 'REQUIRED' }],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'age',
              fileOffset: 4,
              compressedSize: 40,
              uncompressedSize: 40,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
              statistics: {
                minValue: 18,
                maxValue: 65,
                nullCount: 0,
                distinctCount: 10,
              },
            },
          ],
          numRows: 10,
          totalByteSize: 40,
          fileOffset: 4,
        },
      ],
    });

    const stats = footer.rowGroups[0].columns[0].statistics;
    expect(stats).toBeDefined();
    expect(stats?.minValue).toBe(18);
    expect(stats?.maxValue).toBe(65);
  });

  it('should include min/max statistics for INT64 columns', () => {
    const footer = generator.generate({
      schema: [{ name: 'id', type: 'INT64', repetitionType: 'REQUIRED' }],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'id',
              fileOffset: 4,
              compressedSize: 80,
              uncompressedSize: 80,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
              statistics: {
                minValue: BigInt('1000000000000'),
                maxValue: BigInt('9999999999999'),
                nullCount: 0,
                distinctCount: 10,
              },
            },
          ],
          numRows: 10,
          totalByteSize: 80,
          fileOffset: 4,
        },
      ],
    });

    const stats = footer.rowGroups[0].columns[0].statistics;
    expect(stats).toBeDefined();
    expect(stats?.minValue).toBe(BigInt('1000000000000'));
    expect(stats?.maxValue).toBe(BigInt('9999999999999'));
  });

  it('should include min/max statistics for DOUBLE columns', () => {
    const footer = generator.generate({
      schema: [{ name: 'score', type: 'DOUBLE', repetitionType: 'OPTIONAL' }],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'score',
              fileOffset: 4,
              compressedSize: 80,
              uncompressedSize: 80,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
              statistics: {
                minValue: 0.0,
                maxValue: 100.0,
                nullCount: 2,
                distinctCount: 8,
              },
            },
          ],
          numRows: 10,
          totalByteSize: 80,
          fileOffset: 4,
        },
      ],
    });

    const stats = footer.rowGroups[0].columns[0].statistics;
    expect(stats?.minValue).toBeCloseTo(0.0, 10);
    expect(stats?.maxValue).toBeCloseTo(100.0, 10);
  });

  it('should include min/max statistics for BYTE_ARRAY (string) columns', () => {
    const footer = generator.generate({
      schema: [{ name: 'name', type: 'BYTE_ARRAY', repetitionType: 'REQUIRED' }],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'name',
              fileOffset: 4,
              compressedSize: 100,
              uncompressedSize: 100,
              numValues: 5,
              encoding: 'PLAIN',
              compression: 'none',
              statistics: {
                minValue: 'Alice',
                maxValue: 'Zoe',
                nullCount: 0,
                distinctCount: 5,
              },
            },
          ],
          numRows: 5,
          totalByteSize: 100,
          fileOffset: 4,
        },
      ],
    });

    const stats = footer.rowGroups[0].columns[0].statistics;
    expect(stats?.minValue).toBe('Alice');
    expect(stats?.maxValue).toBe('Zoe');
  });

  it('should include null count in statistics', () => {
    const footer = generator.generate({
      schema: [{ name: 'optional_field', type: 'INT32', repetitionType: 'OPTIONAL' }],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'optional_field',
              fileOffset: 4,
              compressedSize: 40,
              uncompressedSize: 40,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
              statistics: {
                minValue: 1,
                maxValue: 100,
                nullCount: 3,
              },
            },
          ],
          numRows: 10,
          totalByteSize: 40,
          fileOffset: 4,
        },
      ],
    });

    expect(footer.rowGroups[0].columns[0].statistics?.nullCount).toBe(3);
  });

  it('should include distinct count in statistics', () => {
    const footer = generator.generate({
      schema: [{ name: 'category', type: 'BYTE_ARRAY', repetitionType: 'REQUIRED' }],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'category',
              fileOffset: 4,
              compressedSize: 50,
              uncompressedSize: 50,
              numValues: 100,
              encoding: 'RLE_DICTIONARY',
              compression: 'none',
              statistics: {
                minValue: 'A',
                maxValue: 'Z',
                nullCount: 0,
                distinctCount: 5,
              },
            },
          ],
          numRows: 100,
          totalByteSize: 50,
          fileOffset: 4,
        },
      ],
    });

    expect(footer.rowGroups[0].columns[0].statistics?.distinctCount).toBe(5);
  });

  it('should handle statistics with all nulls', () => {
    const footer = generator.generate({
      schema: [{ name: 'nullable', type: 'INT32', repetitionType: 'OPTIONAL' }],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'nullable',
              fileOffset: 4,
              compressedSize: 10,
              uncompressedSize: 10,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
              statistics: {
                minValue: null,
                maxValue: null,
                nullCount: 10,
                distinctCount: 0,
              },
            },
          ],
          numRows: 10,
          totalByteSize: 10,
          fileOffset: 4,
        },
      ],
    });

    const stats = footer.rowGroups[0].columns[0].statistics;
    expect(stats?.minValue).toBeNull();
    expect(stats?.maxValue).toBeNull();
    expect(stats?.nullCount).toBe(10);
  });

  it('should serialize binary min/max values for BYTE_ARRAY', () => {
    const footer = generator.generate({
      schema: [{ name: 'binary_data', type: 'BYTE_ARRAY', repetitionType: 'REQUIRED' }],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'binary_data',
              fileOffset: 4,
              compressedSize: 200,
              uncompressedSize: 200,
              numValues: 5,
              encoding: 'PLAIN',
              compression: 'none',
              statistics: {
                minValue: new Uint8Array([0x00, 0x01, 0x02]),
                maxValue: new Uint8Array([0xff, 0xfe, 0xfd]),
                nullCount: 0,
              },
            },
          ],
          numRows: 5,
          totalByteSize: 200,
          fileOffset: 4,
        },
      ],
    });

    const stats = footer.rowGroups[0].columns[0].statistics;
    expect(stats?.minValue).toEqual(new Uint8Array([0x00, 0x01, 0x02]));
    expect(stats?.maxValue).toEqual(new Uint8Array([0xff, 0xfe, 0xfd]));
  });

  it('should omit statistics when not provided', () => {
    const footer = generator.generate({
      schema: [{ name: 'value', type: 'INT32', repetitionType: 'REQUIRED' }],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'value',
              fileOffset: 4,
              compressedSize: 40,
              uncompressedSize: 40,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
              // No statistics provided
            },
          ],
          numRows: 10,
          totalByteSize: 40,
          fileOffset: 4,
        },
      ],
    });

    expect(footer.rowGroups[0].columns[0].statistics).toBeUndefined();
  });
});

// ============================================================================
// Schema Serialization Tests
// ============================================================================

describe('FooterGenerator - Schema serialization', () => {
  let generator: FooterGenerator;

  beforeEach(() => {
    generator = new FooterGenerator();
  });

  it('should serialize simple flat schema', () => {
    const footer = generator.generate({
      schema: [
        { name: 'id', type: 'INT64', repetitionType: 'REQUIRED' },
        { name: 'name', type: 'BYTE_ARRAY', repetitionType: 'OPTIONAL' },
        { name: 'score', type: 'DOUBLE', repetitionType: 'REQUIRED' },
      ],
      rowGroups: [],
    });

    expect(footer.schema).toHaveLength(4); // root + 3 columns
    // First element is the root schema element
    expect(footer.schema[0].name).toBe('schema');
    expect(footer.schema[1].name).toBe('id');
    expect(footer.schema[2].name).toBe('name');
    expect(footer.schema[3].name).toBe('score');
  });

  it('should include repetition type in schema elements', () => {
    const footer = generator.generate({
      schema: [
        { name: 'required_field', type: 'INT32', repetitionType: 'REQUIRED' },
        { name: 'optional_field', type: 'INT32', repetitionType: 'OPTIONAL' },
        { name: 'repeated_field', type: 'INT32', repetitionType: 'REPEATED' },
      ],
      rowGroups: [],
    });

    expect(footer.schema[1].repetitionType).toBe('REQUIRED');
    expect(footer.schema[2].repetitionType).toBe('OPTIONAL');
    expect(footer.schema[3].repetitionType).toBe('REPEATED');
  });

  it('should include physical type in schema elements', () => {
    const footer = generator.generate({
      schema: [
        { name: 'bool_col', type: 'BOOLEAN', repetitionType: 'REQUIRED' },
        { name: 'int32_col', type: 'INT32', repetitionType: 'REQUIRED' },
        { name: 'int64_col', type: 'INT64', repetitionType: 'REQUIRED' },
        { name: 'float_col', type: 'FLOAT', repetitionType: 'REQUIRED' },
        { name: 'double_col', type: 'DOUBLE', repetitionType: 'REQUIRED' },
        { name: 'binary_col', type: 'BYTE_ARRAY', repetitionType: 'REQUIRED' },
        { name: 'fixed_col', type: 'FIXED_LEN_BYTE_ARRAY', repetitionType: 'REQUIRED', typeLength: 16 },
      ],
      rowGroups: [],
    });

    expect(footer.schema[1].type).toBe('BOOLEAN');
    expect(footer.schema[2].type).toBe('INT32');
    expect(footer.schema[3].type).toBe('INT64');
    expect(footer.schema[4].type).toBe('FLOAT');
    expect(footer.schema[5].type).toBe('DOUBLE');
    expect(footer.schema[6].type).toBe('BYTE_ARRAY');
    expect(footer.schema[7].type).toBe('FIXED_LEN_BYTE_ARRAY');
    expect(footer.schema[7].typeLength).toBe(16);
  });

  it('should include converted type for logical types', () => {
    const footer = generator.generate({
      schema: [
        { name: 'utf8_string', type: 'BYTE_ARRAY', repetitionType: 'OPTIONAL', convertedType: 'UTF8' },
        { name: 'date', type: 'INT32', repetitionType: 'OPTIONAL', convertedType: 'DATE' },
        {
          name: 'timestamp',
          type: 'INT64',
          repetitionType: 'OPTIONAL',
          convertedType: 'TIMESTAMP_MILLIS',
        },
        { name: 'decimal', type: 'BYTE_ARRAY', repetitionType: 'OPTIONAL', convertedType: 'DECIMAL' },
      ],
      rowGroups: [],
    });

    expect(footer.schema[1].convertedType).toBe('UTF8');
    expect(footer.schema[2].convertedType).toBe('DATE');
    expect(footer.schema[3].convertedType).toBe('TIMESTAMP_MILLIS');
    expect(footer.schema[4].convertedType).toBe('DECIMAL');
  });

  it('should serialize nested schema (struct)', () => {
    const footer = generator.generate({
      schema: [
        { name: 'id', type: 'INT64', repetitionType: 'REQUIRED' },
        {
          name: 'address',
          repetitionType: 'OPTIONAL',
          children: [
            { name: 'street', type: 'BYTE_ARRAY', repetitionType: 'REQUIRED' },
            { name: 'city', type: 'BYTE_ARRAY', repetitionType: 'REQUIRED' },
            { name: 'zip', type: 'BYTE_ARRAY', repetitionType: 'OPTIONAL' },
          ],
        },
      ],
      rowGroups: [],
    });

    // Should flatten nested schema with num_children
    expect(footer.schema.some((s) => s.name === 'address')).toBe(true);
    expect(footer.schema.some((s) => s.name === 'street')).toBe(true);
    expect(footer.schema.some((s) => s.name === 'city')).toBe(true);
    expect(footer.schema.some((s) => s.name === 'zip')).toBe(true);

    const addressElement = footer.schema.find((s) => s.name === 'address');
    expect(addressElement?.numChildren).toBe(3);
  });

  it('should serialize list schema (repeated elements)', () => {
    const footer = generator.generate({
      schema: [
        { name: 'id', type: 'INT64', repetitionType: 'REQUIRED' },
        {
          name: 'tags',
          repetitionType: 'OPTIONAL',
          convertedType: 'LIST',
          children: [
            {
              name: 'list',
              repetitionType: 'REPEATED',
              children: [{ name: 'element', type: 'BYTE_ARRAY', repetitionType: 'OPTIONAL' }],
            },
          ],
        },
      ],
      rowGroups: [],
    });

    const tagsElement = footer.schema.find((s) => s.name === 'tags');
    expect(tagsElement?.convertedType).toBe('LIST');
  });

  it('should serialize map schema', () => {
    const footer = generator.generate({
      schema: [
        { name: 'id', type: 'INT64', repetitionType: 'REQUIRED' },
        {
          name: 'metadata',
          repetitionType: 'OPTIONAL',
          convertedType: 'MAP',
          children: [
            {
              name: 'key_value',
              repetitionType: 'REPEATED',
              children: [
                { name: 'key', type: 'BYTE_ARRAY', repetitionType: 'REQUIRED' },
                { name: 'value', type: 'BYTE_ARRAY', repetitionType: 'OPTIONAL' },
              ],
            },
          ],
        },
      ],
      rowGroups: [],
    });

    const metadataElement = footer.schema.find((s) => s.name === 'metadata');
    expect(metadataElement?.convertedType).toBe('MAP');
  });

  it('should include field id when provided', () => {
    const footer = generator.generate({
      schema: [
        { name: 'id', type: 'INT64', repetitionType: 'REQUIRED', fieldId: 1 },
        { name: 'name', type: 'BYTE_ARRAY', repetitionType: 'OPTIONAL', fieldId: 2 },
      ],
      rowGroups: [],
    });

    expect(footer.schema[1].fieldId).toBe(1);
    expect(footer.schema[2].fieldId).toBe(2);
  });

  it('should include root schema element with num_children', () => {
    const footer = generator.generate({
      schema: [
        { name: 'col1', type: 'INT32', repetitionType: 'REQUIRED' },
        { name: 'col2', type: 'INT64', repetitionType: 'REQUIRED' },
        { name: 'col3', type: 'DOUBLE', repetitionType: 'REQUIRED' },
      ],
      rowGroups: [],
    });

    // Root element should be first
    expect(footer.schema[0].name).toBe('schema');
    expect(footer.schema[0].numChildren).toBe(3);
    expect(footer.schema[0].type).toBeUndefined(); // Root has no type
  });
});

// ============================================================================
// Footer Binary Format Validation Tests
// ============================================================================

describe('FooterGenerator - Footer binary format validation', () => {
  let generator: FooterGenerator;

  beforeEach(() => {
    generator = new FooterGenerator();
  });

  it('should end with PAR1 magic bytes', () => {
    const footer = generator.generate({
      schema: [{ name: 'id', type: 'INT64', repetitionType: 'REQUIRED' }],
      rowGroups: [],
    });

    const data = footer.data;
    const lastFour = data.slice(data.byteLength - 4);
    expect(lastFour[0]).toBe(0x50); // 'P'
    expect(lastFour[1]).toBe(0x41); // 'A'
    expect(lastFour[2]).toBe(0x52); // 'R'
    expect(lastFour[3]).toBe(0x31); // '1'
  });

  it('should have valid footer length field', () => {
    const footer = generator.generate({
      schema: [
        { name: 'id', type: 'INT64', repetitionType: 'REQUIRED' },
        { name: 'name', type: 'BYTE_ARRAY', repetitionType: 'OPTIONAL' },
      ],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'id',
              fileOffset: 4,
              compressedSize: 80,
              uncompressedSize: 80,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
            },
            {
              columnName: 'name',
              fileOffset: 84,
              compressedSize: 100,
              uncompressedSize: 100,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
            },
          ],
          numRows: 10,
          totalByteSize: 180,
          fileOffset: 4,
        },
      ],
    });

    const data = footer.data;
    // Read the 4-byte little-endian length before magic bytes
    const lengthOffset = data.byteLength - 8;
    const lengthView = new DataView(data.buffer, data.byteOffset + lengthOffset, 4);
    const footerLength = lengthView.getUint32(0, true);

    // Footer length should be positive and less than total size minus overhead
    expect(footerLength).toBeGreaterThan(0);
    expect(footerLength).toBeLessThan(data.byteLength);

    // The footer metadata should be exactly footerLength bytes
    expect(footerLength).toBe(data.byteLength - 8); // minus length field and magic
  });

  it('should use Thrift compact protocol encoding', () => {
    const footer = generator.generate({
      schema: [{ name: 'value', type: 'INT32', repetitionType: 'REQUIRED' }],
      rowGroups: [],
    });

    // Thrift compact protocol typically starts with specific byte patterns
    // The FileMetaData struct in Thrift compact format starts with field type indicators
    const data = footer.data;
    const metadataStart = data.slice(0, data.byteLength - 8);

    // Should have valid Thrift structure (non-empty)
    expect(metadataStart.byteLength).toBeGreaterThan(0);
  });

  it('should produce deterministic output for same input', () => {
    const input = {
      schema: [{ name: 'id', type: 'INT32' as ParquetType, repetitionType: 'REQUIRED' as RepetitionType }],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'id',
              fileOffset: 4,
              compressedSize: 40,
              uncompressedSize: 40,
              numValues: 10,
              encoding: 'PLAIN' as Encoding,
              compression: 'none' as CompressionCodec,
            },
          ],
          numRows: 10,
          totalByteSize: 40,
          fileOffset: 4,
        },
      ],
    };

    const footer1 = generator.generate(input);
    const footer2 = generator.generate(input);

    // Should produce identical binary output
    expect(footer1.data).toEqual(footer2.data);
  });

  it('should include all required FileMetaData fields', () => {
    const footer = generator.generate({
      schema: [{ name: 'id', type: 'INT64', repetitionType: 'REQUIRED' }],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'id',
              fileOffset: 4,
              compressedSize: 80,
              uncompressedSize: 80,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
            },
          ],
          numRows: 10,
          totalByteSize: 80,
          fileOffset: 4,
        },
      ],
    });

    // FileMetaData required fields per Parquet spec
    expect(footer.version).toBeDefined();
    expect(footer.schema).toBeDefined();
    expect(footer.schema.length).toBeGreaterThan(0);
    expect(footer.numRows).toBeDefined();
    expect(footer.rowGroups).toBeDefined();
  });

  it('should handle large footers efficiently', () => {
    // Generate a footer with many columns and row groups
    const columns = Array.from({ length: 50 }, (_, i) => ({
      name: `column_${i}`,
      type: 'INT64' as ParquetType,
      repetitionType: 'OPTIONAL' as RepetitionType,
    }));

    const rowGroups = Array.from({ length: 100 }, (_, rgIdx) => ({
      columns: columns.map((col, colIdx) => ({
        columnName: col.name,
        fileOffset: rgIdx * 50000 + colIdx * 1000,
        compressedSize: 900,
        uncompressedSize: 1000,
        numValues: 1000,
        encoding: 'PLAIN' as Encoding,
        compression: 'snappy' as CompressionCodec,
        statistics: {
          minValue: colIdx,
          maxValue: colIdx + 1000,
          nullCount: 10,
          distinctCount: 990,
        },
      })),
      numRows: 1000,
      totalByteSize: 45000,
      fileOffset: rgIdx * 50000,
    }));

    const startTime = performance.now();
    const footer = generator.generate({ schema: columns, rowGroups });
    const elapsed = performance.now() - startTime;

    expect(footer.data).toBeInstanceOf(Uint8Array);
    expect(footer.numRows).toBe(100000); // 100 row groups * 1000 rows
    expect(elapsed).toBeLessThan(1000); // Should complete in under 1 second
  });

  it('should correctly encode column path for nested schemas', () => {
    const footer = generator.generate({
      schema: [
        { name: 'id', type: 'INT64', repetitionType: 'REQUIRED' },
        {
          name: 'user',
          repetitionType: 'OPTIONAL',
          children: [
            { name: 'name', type: 'BYTE_ARRAY', repetitionType: 'REQUIRED' },
            { name: 'email', type: 'BYTE_ARRAY', repetitionType: 'OPTIONAL' },
          ],
        },
      ],
      rowGroups: [
        {
          columns: [
            {
              columnName: 'id',
              fileOffset: 4,
              compressedSize: 80,
              uncompressedSize: 80,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
            },
            {
              columnName: 'user.name',
              fileOffset: 84,
              compressedSize: 100,
              uncompressedSize: 100,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
            },
            {
              columnName: 'user.email',
              fileOffset: 184,
              compressedSize: 100,
              uncompressedSize: 100,
              numValues: 10,
              encoding: 'PLAIN',
              compression: 'none',
            },
          ],
          numRows: 10,
          totalByteSize: 280,
          fileOffset: 4,
        },
      ],
    });

    // Column paths should be preserved
    expect(footer.rowGroups[0].columns[1].columnName).toBe('user.name');
    expect(footer.rowGroups[0].columns[2].columnName).toBe('user.email');
  });
});

// ============================================================================
// Error Handling Tests
// ============================================================================

describe('FooterGenerator - Error handling', () => {
  let generator: FooterGenerator;

  beforeEach(() => {
    generator = new FooterGenerator();
  });

  it('should throw on invalid schema type', () => {
    expect(() =>
      generator.generate({
        schema: [{ name: 'bad', type: 'INVALID_TYPE' as ParquetType, repetitionType: 'REQUIRED' }],
        rowGroups: [],
      })
    ).toThrow();
  });

  it('should throw on mismatched column count in row group', () => {
    expect(() =>
      generator.generate({
        schema: [
          { name: 'col1', type: 'INT32', repetitionType: 'REQUIRED' },
          { name: 'col2', type: 'INT32', repetitionType: 'REQUIRED' },
        ],
        rowGroups: [
          {
            columns: [
              {
                columnName: 'col1',
                fileOffset: 4,
                compressedSize: 40,
                uncompressedSize: 40,
                numValues: 10,
                encoding: 'PLAIN',
                compression: 'none',
              },
              // Missing col2
            ],
            numRows: 10,
            totalByteSize: 40,
            fileOffset: 4,
          },
        ],
      })
    ).toThrow();
  });

  it('should throw on negative file offset', () => {
    expect(() =>
      generator.generate({
        schema: [{ name: 'value', type: 'INT32', repetitionType: 'REQUIRED' }],
        rowGroups: [
          {
            columns: [
              {
                columnName: 'value',
                fileOffset: -1,
                compressedSize: 40,
                uncompressedSize: 40,
                numValues: 10,
                encoding: 'PLAIN',
                compression: 'none',
              },
            ],
            numRows: 10,
            totalByteSize: 40,
            fileOffset: -1,
          },
        ],
      })
    ).toThrow();
  });

  it('should throw on negative row count', () => {
    expect(() =>
      generator.generate({
        schema: [{ name: 'value', type: 'INT32', repetitionType: 'REQUIRED' }],
        rowGroups: [
          {
            columns: [
              {
                columnName: 'value',
                fileOffset: 4,
                compressedSize: 40,
                uncompressedSize: 40,
                numValues: 10,
                encoding: 'PLAIN',
                compression: 'none',
              },
            ],
            numRows: -5,
            totalByteSize: 40,
            fileOffset: 4,
          },
        ],
      })
    ).toThrow();
  });

  it('should throw on column name not in schema', () => {
    expect(() =>
      generator.generate({
        schema: [{ name: 'known_column', type: 'INT32', repetitionType: 'REQUIRED' }],
        rowGroups: [
          {
            columns: [
              {
                columnName: 'unknown_column',
                fileOffset: 4,
                compressedSize: 40,
                uncompressedSize: 40,
                numValues: 10,
                encoding: 'PLAIN',
                compression: 'none',
              },
            ],
            numRows: 10,
            totalByteSize: 40,
            fileOffset: 4,
          },
        ],
      })
    ).toThrow();
  });
});

// ============================================================================
// Options and Configuration Tests
// ============================================================================

describe('FooterGenerator - Options and configuration', () => {
  it('should accept custom created_by string', () => {
    const generator = new FooterGenerator({ createdBy: 'mongolake v0.1.0' });

    const footer = generator.generate({
      schema: [{ name: 'id', type: 'INT64', repetitionType: 'REQUIRED' }],
      rowGroups: [],
    });

    expect(footer.createdBy).toBe('mongolake v0.1.0');
  });

  it('should accept custom version', () => {
    const generator = new FooterGenerator({ version: 2 });

    const footer = generator.generate({
      schema: [{ name: 'id', type: 'INT64', repetitionType: 'REQUIRED' }],
      rowGroups: [],
    });

    expect(footer.version).toBe(2);
  });

  it('should include key-value metadata when provided', () => {
    const generator = new FooterGenerator({
      keyValueMetadata: [
        { key: 'writer.model.name', value: 'mongolake' },
        { key: 'writer.version', value: '0.1.0' },
        { key: 'iceberg.schema', value: '{"type":"struct","fields":[]}' },
      ],
    });

    const footer = generator.generate({
      schema: [{ name: 'id', type: 'INT64', repetitionType: 'REQUIRED' }],
      rowGroups: [],
    });

    expect(footer.keyValueMetadata).toBeDefined();
    expect(footer.keyValueMetadata).toHaveLength(3);
    expect(footer.keyValueMetadata?.find((kv) => kv.key === 'writer.model.name')?.value).toBe(
      'mongolake'
    );
  });

  it('should support encryption metadata placeholder', () => {
    const generator = new FooterGenerator();

    const footer = generator.generate({
      schema: [{ name: 'id', type: 'INT64', repetitionType: 'REQUIRED' }],
      rowGroups: [],
      encryptionAlgorithm: 'AES_GCM_V1',
    });

    expect(footer.encryptionAlgorithm).toBe('AES_GCM_V1');
  });

  it('should support column order specification', () => {
    const generator = new FooterGenerator();

    const footer = generator.generate({
      schema: [
        { name: 'id', type: 'INT64', repetitionType: 'REQUIRED' },
        { name: 'name', type: 'BYTE_ARRAY', repetitionType: 'OPTIONAL' },
      ],
      rowGroups: [],
      columnOrders: [
        { columnOrderType: 'TYPE_DEFINED_ORDER' },
        { columnOrderType: 'TYPE_DEFINED_ORDER' },
      ],
    });

    expect(footer.columnOrders).toBeDefined();
    expect(footer.columnOrders).toHaveLength(2);
  });
});
