/**
 * Parquet Column Writer Tests
 *
 * Tests for writing native Parquet column types with support for:
 * - Primitive types (string, int32, int64, float, double, boolean, timestamp, binary)
 * - Null handling and definition levels
 * - Dictionary encoding for strings
 * - Statistics generation (min, max, null count, distinct count)
 * - Compression (none, snappy, zstd)
 * - Repetition/definition levels for nested types
 * - Nested structs and arrays/lists
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  ColumnWriter,
  StringColumnWriter,
  Int32ColumnWriter,
  Int64ColumnWriter,
  FloatColumnWriter,
  DoubleColumnWriter,
  BooleanColumnWriter,
  TimestampColumnWriter,
  BinaryColumnWriter,
  StructColumnWriter,
  ListColumnWriter,
  type ColumnWriterOptions,
  type ColumnStatistics,
  type CompressionCodec,
  type Encoding,
  type WrittenColumn,
} from '../../../src/parquet/column-writer.js';

// ============================================================================
// String Column Writer
// ============================================================================

describe('StringColumnWriter', () => {
  let writer: StringColumnWriter;

  beforeEach(() => {
    writer = new StringColumnWriter('name');
  });

  describe('Basic writing', () => {
    it('should write a single string value', () => {
      writer.write('hello');
      const result = writer.finish();

      expect(result).toBeDefined();
      expect(result.columnName).toBe('name');
      expect(result.numValues).toBe(1);
      expect(result.data).toBeInstanceOf(Uint8Array);
      expect(result.data.byteLength).toBeGreaterThan(0);
    });

    it('should write multiple string values', () => {
      writer.write('alice');
      writer.write('bob');
      writer.write('charlie');
      const result = writer.finish();

      expect(result.numValues).toBe(3);
    });

    it('should write empty string', () => {
      writer.write('');
      const result = writer.finish();

      expect(result.numValues).toBe(1);
      expect(result.statistics.minValue).toBe('');
      expect(result.statistics.maxValue).toBe('');
    });

    it('should write unicode strings', () => {
      writer.write('\u4e16\u754c'); // Chinese characters
      writer.write('\ud83c\udf0d'); // Globe emoji
      const result = writer.finish();

      expect(result.numValues).toBe(2);
    });

    it('should write long strings', () => {
      const longString = 'a'.repeat(100000);
      writer.write(longString);
      const result = writer.finish();

      expect(result.numValues).toBe(1);
    });
  });

  describe('Null handling', () => {
    it('should write null values', () => {
      writer.write('alice');
      writer.write(null);
      writer.write('charlie');
      const result = writer.finish();

      expect(result.numValues).toBe(3);
      expect(result.statistics.nullCount).toBe(1);
    });

    it('should write all null values', () => {
      writer.write(null);
      writer.write(null);
      writer.write(null);
      const result = writer.finish();

      expect(result.numValues).toBe(3);
      expect(result.statistics.nullCount).toBe(3);
      expect(result.statistics.minValue).toBeUndefined();
      expect(result.statistics.maxValue).toBeUndefined();
    });

    it('should include definition levels for nullable column', () => {
      const nullableWriter = new StringColumnWriter('name', { nullable: true });
      nullableWriter.write('alice');
      nullableWriter.write(null);
      nullableWriter.write('bob');
      const result = nullableWriter.finish();

      expect(result.definitionLevels).toBeDefined();
      expect(result.definitionLevels).toEqual([1, 0, 1]);
    });
  });

  describe('Dictionary encoding', () => {
    it('should use dictionary encoding by default for low cardinality', () => {
      for (let i = 0; i < 1000; i++) {
        writer.write(i % 10 === 0 ? 'category_a' : 'category_b');
      }
      const result = writer.finish();

      expect(result.encoding).toBe('PLAIN_DICTIONARY');
      expect(result.dictionaryPageData).toBeDefined();
    });

    it('should support explicit dictionary encoding option', () => {
      const dictWriter = new StringColumnWriter('category', { useDictionary: true });
      dictWriter.write('red');
      dictWriter.write('green');
      dictWriter.write('red');
      dictWriter.write('blue');
      dictWriter.write('red');
      const result = dictWriter.finish();

      expect(result.encoding).toBe('PLAIN_DICTIONARY');
      expect(result.dictionaryPageData).toBeDefined();
    });

    it('should fall back to plain encoding for high cardinality', () => {
      const noDictWriter = new StringColumnWriter('uuid', { useDictionary: false });
      for (let i = 0; i < 100; i++) {
        noDictWriter.write(`unique-value-${i}`);
      }
      const result = noDictWriter.finish();

      expect(result.encoding).toBe('PLAIN');
      expect(result.dictionaryPageData).toBeUndefined();
    });

    it('should track dictionary size', () => {
      const dictWriter = new StringColumnWriter('tag', { useDictionary: true });
      dictWriter.write('a');
      dictWriter.write('b');
      dictWriter.write('a');
      dictWriter.write('c');
      const result = dictWriter.finish();

      expect(result.dictionarySize).toBe(3); // 'a', 'b', 'c'
    });
  });

  describe('Statistics', () => {
    it('should compute min/max values', () => {
      writer.write('charlie');
      writer.write('alice');
      writer.write('bob');
      const result = writer.finish();

      expect(result.statistics.minValue).toBe('alice');
      expect(result.statistics.maxValue).toBe('charlie');
    });

    it('should compute null count', () => {
      writer.write('a');
      writer.write(null);
      writer.write('b');
      writer.write(null);
      const result = writer.finish();

      expect(result.statistics.nullCount).toBe(2);
    });

    it('should compute distinct count', () => {
      writer.write('a');
      writer.write('b');
      writer.write('a');
      writer.write('c');
      writer.write('b');
      const result = writer.finish();

      expect(result.statistics.distinctCount).toBe(3);
    });

    it('should exclude nulls from min/max', () => {
      writer.write(null);
      writer.write('zebra');
      writer.write(null);
      writer.write('apple');
      const result = writer.finish();

      expect(result.statistics.minValue).toBe('apple');
      expect(result.statistics.maxValue).toBe('zebra');
      expect(result.statistics.nullCount).toBe(2);
    });
  });

  describe('Compression', () => {
    it('should support no compression', () => {
      const uncompressed = new StringColumnWriter('name', { compression: 'none' });
      uncompressed.write('test data');
      const result = uncompressed.finish();

      expect(result.compression).toBe('none');
    });

    it('should support snappy compression', () => {
      const compressed = new StringColumnWriter('name', { compression: 'snappy' });
      for (let i = 0; i < 100; i++) {
        compressed.write('repeated string data that compresses well');
      }
      const result = compressed.finish();

      expect(result.compression).toBe('snappy');
      expect(result.compressedSize).toBeLessThan(result.uncompressedSize);
    });

    it('should support zstd compression', () => {
      const compressed = new StringColumnWriter('name', { compression: 'zstd' });
      for (let i = 0; i < 100; i++) {
        compressed.write('repeated string data that compresses well');
      }
      const result = compressed.finish();

      expect(result.compression).toBe('zstd');
      expect(result.compressedSize).toBeLessThan(result.uncompressedSize);
    });

    it('should track uncompressed and compressed sizes', () => {
      const compressed = new StringColumnWriter('name', { compression: 'snappy' });
      compressed.write('some test data');
      const result = compressed.finish();

      expect(result.uncompressedSize).toBeGreaterThan(0);
      expect(result.compressedSize).toBeGreaterThan(0);
    });
  });
});

// ============================================================================
// Integer Column Writers (Int32 and Int64)
// ============================================================================

describe('Int32ColumnWriter', () => {
  let writer: Int32ColumnWriter;

  beforeEach(() => {
    writer = new Int32ColumnWriter('count');
  });

  describe('Basic writing', () => {
    it('should write int32 values', () => {
      writer.write(42);
      writer.write(-100);
      writer.write(0);
      const result = writer.finish();

      expect(result.numValues).toBe(3);
      expect(result.dataType).toBe('INT32');
    });

    it('should handle int32 min/max range', () => {
      writer.write(-2147483648); // INT32_MIN
      writer.write(2147483647); // INT32_MAX
      const result = writer.finish();

      expect(result.statistics.minValue).toBe(-2147483648);
      expect(result.statistics.maxValue).toBe(2147483647);
    });

    it('should throw on overflow', () => {
      expect(() => writer.write(2147483648)).toThrow();
    });

    it('should throw on underflow', () => {
      expect(() => writer.write(-2147483649)).toThrow();
    });
  });

  describe('Null handling', () => {
    it('should write null values', () => {
      writer.write(1);
      writer.write(null);
      writer.write(3);
      const result = writer.finish();

      expect(result.statistics.nullCount).toBe(1);
    });
  });

  describe('Statistics', () => {
    it('should compute min/max for int32', () => {
      writer.write(100);
      writer.write(-50);
      writer.write(75);
      const result = writer.finish();

      expect(result.statistics.minValue).toBe(-50);
      expect(result.statistics.maxValue).toBe(100);
    });
  });

  describe('Encoding', () => {
    it('should use PLAIN encoding by default', () => {
      writer.write(1);
      const result = writer.finish();

      expect(result.encoding).toBe('PLAIN');
    });

    it('should support DELTA_BINARY_PACKED encoding', () => {
      const deltaWriter = new Int32ColumnWriter('seq', { encoding: 'DELTA_BINARY_PACKED' });
      for (let i = 0; i < 100; i++) {
        deltaWriter.write(i);
      }
      const result = deltaWriter.finish();

      expect(result.encoding).toBe('DELTA_BINARY_PACKED');
    });
  });
});

describe('Int64ColumnWriter', () => {
  let writer: Int64ColumnWriter;

  beforeEach(() => {
    writer = new Int64ColumnWriter('bigcount');
  });

  describe('Basic writing', () => {
    it('should write int64 values', () => {
      writer.write(BigInt(42));
      writer.write(BigInt(-100));
      const result = writer.finish();

      expect(result.numValues).toBe(2);
      expect(result.dataType).toBe('INT64');
    });

    it('should write large int64 values', () => {
      writer.write(BigInt('9223372036854775807')); // INT64_MAX
      writer.write(BigInt('-9223372036854775808')); // INT64_MIN
      const result = writer.finish();

      expect(result.statistics.minValue).toBe(BigInt('-9223372036854775808'));
      expect(result.statistics.maxValue).toBe(BigInt('9223372036854775807'));
    });

    it('should accept regular numbers within safe integer range', () => {
      writer.write(Number.MAX_SAFE_INTEGER);
      writer.write(Number.MIN_SAFE_INTEGER);
      const result = writer.finish();

      expect(result.numValues).toBe(2);
    });
  });

  describe('Null handling', () => {
    it('should write null values', () => {
      writer.write(BigInt(1));
      writer.write(null);
      const result = writer.finish();

      expect(result.statistics.nullCount).toBe(1);
    });
  });

  describe('Encoding', () => {
    it('should support DELTA_BINARY_PACKED encoding', () => {
      const deltaWriter = new Int64ColumnWriter('seq', { encoding: 'DELTA_BINARY_PACKED' });
      for (let i = 0; i < 100; i++) {
        deltaWriter.write(BigInt(i));
      }
      const result = deltaWriter.finish();

      expect(result.encoding).toBe('DELTA_BINARY_PACKED');
    });
  });
});

// ============================================================================
// Float Column Writers (Float and Double)
// ============================================================================

describe('FloatColumnWriter', () => {
  let writer: FloatColumnWriter;

  beforeEach(() => {
    writer = new FloatColumnWriter('temperature');
  });

  describe('Basic writing', () => {
    it('should write float values', () => {
      writer.write(3.14);
      writer.write(-273.15);
      writer.write(0.0);
      const result = writer.finish();

      expect(result.numValues).toBe(3);
      expect(result.dataType).toBe('FLOAT');
    });

    it('should handle float precision (32-bit)', () => {
      writer.write(1.23456789);
      const result = writer.finish();

      // Float32 has ~7 decimal digits of precision
      expect(result.statistics.minValue).toBeCloseTo(1.234568, 5);
    });

    it('should handle special float values', () => {
      writer.write(Infinity);
      writer.write(-Infinity);
      writer.write(NaN);
      const result = writer.finish();

      expect(result.numValues).toBe(3);
    });
  });

  describe('Null handling', () => {
    it('should write null values', () => {
      writer.write(1.5);
      writer.write(null);
      writer.write(2.5);
      const result = writer.finish();

      expect(result.statistics.nullCount).toBe(1);
    });
  });

  describe('Statistics', () => {
    it('should compute min/max for floats', () => {
      writer.write(100.5);
      writer.write(-50.25);
      writer.write(75.75);
      const result = writer.finish();

      expect(result.statistics.minValue).toBeCloseTo(-50.25, 2);
      expect(result.statistics.maxValue).toBeCloseTo(100.5, 2);
    });

    it('should handle NaN in statistics', () => {
      writer.write(1.0);
      writer.write(NaN);
      writer.write(2.0);
      const result = writer.finish();

      // NaN should not affect min/max
      expect(result.statistics.minValue).toBeCloseTo(1.0, 5);
      expect(result.statistics.maxValue).toBeCloseTo(2.0, 5);
    });
  });
});

describe('DoubleColumnWriter', () => {
  let writer: DoubleColumnWriter;

  beforeEach(() => {
    writer = new DoubleColumnWriter('price');
  });

  describe('Basic writing', () => {
    it('should write double values', () => {
      writer.write(3.141592653589793);
      writer.write(-1e308);
      writer.write(1e-308);
      const result = writer.finish();

      expect(result.numValues).toBe(3);
      expect(result.dataType).toBe('DOUBLE');
    });

    it('should maintain double precision (64-bit)', () => {
      const precise = 1.2345678901234567;
      writer.write(precise);
      const result = writer.finish();

      expect(result.statistics.minValue).toBe(precise);
    });
  });

  describe('Null handling', () => {
    it('should write null values', () => {
      writer.write(1.5);
      writer.write(null);
      const result = writer.finish();

      expect(result.statistics.nullCount).toBe(1);
    });
  });

  describe('Statistics', () => {
    it('should compute min/max for doubles', () => {
      writer.write(1e100);
      writer.write(-1e100);
      writer.write(0.0);
      const result = writer.finish();

      expect(result.statistics.minValue).toBe(-1e100);
      expect(result.statistics.maxValue).toBe(1e100);
    });
  });
});

// ============================================================================
// Boolean Column Writer
// ============================================================================

describe('BooleanColumnWriter', () => {
  let writer: BooleanColumnWriter;

  beforeEach(() => {
    writer = new BooleanColumnWriter('active');
  });

  describe('Basic writing', () => {
    it('should write boolean values', () => {
      writer.write(true);
      writer.write(false);
      writer.write(true);
      const result = writer.finish();

      expect(result.numValues).toBe(3);
      expect(result.dataType).toBe('BOOLEAN');
    });

    it('should use bit-packed encoding for booleans', () => {
      for (let i = 0; i < 100; i++) {
        writer.write(i % 2 === 0);
      }
      const result = writer.finish();

      // Boolean columns should use RLE encoding
      expect(result.encoding).toBe('RLE');
      // Bit-packed: 100 bools should be ~13 bytes (100/8 = 12.5)
      expect(result.data.byteLength).toBeLessThan(20);
    });
  });

  describe('Null handling', () => {
    it('should write null values', () => {
      writer.write(true);
      writer.write(null);
      writer.write(false);
      const result = writer.finish();

      expect(result.statistics.nullCount).toBe(1);
    });
  });

  describe('Statistics', () => {
    it('should compute min/max for booleans', () => {
      writer.write(true);
      writer.write(true);
      const result = writer.finish();

      expect(result.statistics.minValue).toBe(true);
      expect(result.statistics.maxValue).toBe(true);
    });

    it('should handle mixed booleans', () => {
      writer.write(false);
      writer.write(true);
      writer.write(false);
      const result = writer.finish();

      expect(result.statistics.minValue).toBe(false);
      expect(result.statistics.maxValue).toBe(true);
    });
  });
});

// ============================================================================
// Timestamp Column Writer
// ============================================================================

describe('TimestampColumnWriter', () => {
  let writer: TimestampColumnWriter;

  beforeEach(() => {
    writer = new TimestampColumnWriter('createdAt');
  });

  describe('Basic writing', () => {
    it('should write Date values', () => {
      writer.write(new Date('2024-01-15T12:30:00Z'));
      const result = writer.finish();

      expect(result.numValues).toBe(1);
      expect(result.dataType).toBe('INT64');
      expect(result.logicalType).toBe('TIMESTAMP_MILLIS');
    });

    it('should write timestamp as milliseconds since epoch', () => {
      const date = new Date('2024-01-15T12:30:00.123Z');
      writer.write(date);
      const result = writer.finish();

      expect(result.statistics.minValue).toBe(date.getTime());
    });

    it('should write number timestamps (milliseconds)', () => {
      writer.write(1705321800000); // 2024-01-15T12:30:00Z
      const result = writer.finish();

      expect(result.numValues).toBe(1);
    });

    it('should handle epoch timestamp', () => {
      writer.write(new Date(0));
      const result = writer.finish();

      expect(result.statistics.minValue).toBe(0);
    });

    it('should handle pre-epoch timestamps', () => {
      const preEpoch = new Date('1969-07-20T20:17:40Z');
      writer.write(preEpoch);
      const result = writer.finish();

      expect(result.statistics.minValue).toBe(preEpoch.getTime());
    });
  });

  describe('Timestamp units', () => {
    it('should support millisecond precision by default', () => {
      const msWriter = new TimestampColumnWriter('ts');
      msWriter.write(new Date('2024-01-15T12:30:00.123Z'));
      const result = msWriter.finish();

      expect(result.logicalType).toBe('TIMESTAMP_MILLIS');
    });

    it('should support microsecond precision', () => {
      const usWriter = new TimestampColumnWriter('ts', { unit: 'micros' });
      usWriter.write(new Date('2024-01-15T12:30:00.123Z'));
      const result = usWriter.finish();

      expect(result.logicalType).toBe('TIMESTAMP_MICROS');
    });

    it('should support nanosecond precision', () => {
      const nsWriter = new TimestampColumnWriter('ts', { unit: 'nanos' });
      nsWriter.write(new Date('2024-01-15T12:30:00.123Z'));
      const result = nsWriter.finish();

      expect(result.logicalType).toBe('TIMESTAMP_NANOS');
    });
  });

  describe('Null handling', () => {
    it('should write null values', () => {
      writer.write(new Date());
      writer.write(null);
      const result = writer.finish();

      expect(result.statistics.nullCount).toBe(1);
    });
  });

  describe('Statistics', () => {
    it('should compute min/max for timestamps', () => {
      const early = new Date('2024-01-01');
      const late = new Date('2024-12-31');
      const middle = new Date('2024-06-15');

      writer.write(middle);
      writer.write(early);
      writer.write(late);
      const result = writer.finish();

      expect(result.statistics.minValue).toBe(early.getTime());
      expect(result.statistics.maxValue).toBe(late.getTime());
    });
  });
});

// ============================================================================
// Binary Column Writer
// ============================================================================

describe('BinaryColumnWriter', () => {
  let writer: BinaryColumnWriter;

  beforeEach(() => {
    writer = new BinaryColumnWriter('data');
  });

  describe('Basic writing', () => {
    it('should write Uint8Array values', () => {
      writer.write(new Uint8Array([1, 2, 3, 4]));
      const result = writer.finish();

      expect(result.numValues).toBe(1);
      expect(result.dataType).toBe('BYTE_ARRAY');
    });

    it('should write ArrayBuffer values', () => {
      const buffer = new ArrayBuffer(4);
      new Uint8Array(buffer).set([1, 2, 3, 4]);
      writer.write(buffer);
      const result = writer.finish();

      expect(result.numValues).toBe(1);
    });

    it('should write empty binary', () => {
      writer.write(new Uint8Array(0));
      const result = writer.finish();

      expect(result.numValues).toBe(1);
    });

    it('should write large binary data', () => {
      const largeData = new Uint8Array(1000000); // 1MB
      for (let i = 0; i < largeData.length; i++) {
        largeData[i] = i % 256;
      }
      writer.write(largeData);
      const result = writer.finish();

      expect(result.numValues).toBe(1);
    });
  });

  describe('Null handling', () => {
    it('should write null values', () => {
      writer.write(new Uint8Array([1, 2]));
      writer.write(null);
      writer.write(new Uint8Array([3, 4]));
      const result = writer.finish();

      expect(result.statistics.nullCount).toBe(1);
    });
  });

  describe('Statistics', () => {
    it('should compute byte length statistics', () => {
      writer.write(new Uint8Array([1, 2, 3]));
      writer.write(new Uint8Array([1, 2, 3, 4, 5]));
      writer.write(new Uint8Array([1]));
      const result = writer.finish();

      expect(result.statistics.minByteLength).toBe(1);
      expect(result.statistics.maxByteLength).toBe(5);
    });

    it('should track total byte size', () => {
      writer.write(new Uint8Array([1, 2, 3]));
      writer.write(new Uint8Array([4, 5]));
      const result = writer.finish();

      expect(result.totalByteSize).toBe(5);
    });
  });

  describe('Fixed length binary', () => {
    it('should support fixed length binary columns', () => {
      const fixedWriter = new BinaryColumnWriter('hash', { fixedLength: 32 });
      fixedWriter.write(new Uint8Array(32).fill(0xab));
      const result = fixedWriter.finish();

      expect(result.dataType).toBe('FIXED_LEN_BYTE_ARRAY');
      expect(result.typeLength).toBe(32);
    });

    it('should throw on incorrect fixed length', () => {
      const fixedWriter = new BinaryColumnWriter('hash', { fixedLength: 32 });
      expect(() => fixedWriter.write(new Uint8Array(16))).toThrow();
    });
  });
});

// ============================================================================
// Nested Struct Column Writer
// ============================================================================

describe('StructColumnWriter', () => {
  let writer: StructColumnWriter;

  beforeEach(() => {
    writer = new StructColumnWriter('address', {
      fields: [
        { name: 'street', type: 'string' },
        { name: 'city', type: 'string' },
        { name: 'zip', type: 'int32' },
      ],
    });
  });

  describe('Basic writing', () => {
    it('should write struct values', () => {
      writer.write({
        street: '123 Main St',
        city: 'NYC',
        zip: 10001,
      });
      const result = writer.finish();

      expect(result.numValues).toBe(1);
      expect(result.dataType).toBe('STRUCT');
    });

    it('should write multiple struct values', () => {
      writer.write({ street: '123 Main St', city: 'NYC', zip: 10001 });
      writer.write({ street: '456 Oak Ave', city: 'LA', zip: 90001 });
      const result = writer.finish();

      expect(result.numValues).toBe(2);
    });

    it('should create child column writers', () => {
      writer.write({ street: '123 Main St', city: 'NYC', zip: 10001 });
      const result = writer.finish();

      expect(result.children).toHaveLength(3);
      expect(result.children[0].columnName).toBe('address.street');
      expect(result.children[1].columnName).toBe('address.city');
      expect(result.children[2].columnName).toBe('address.zip');
    });
  });

  describe('Null handling', () => {
    it('should write null struct values', () => {
      writer.write({ street: '123 Main St', city: 'NYC', zip: 10001 });
      writer.write(null);
      const result = writer.finish();

      expect(result.statistics.nullCount).toBe(1);
    });

    it('should write struct with null fields', () => {
      writer.write({ street: '123 Main St', city: null, zip: 10001 });
      const result = writer.finish();

      const cityChild = result.children.find((c) => c.columnName === 'address.city');
      expect(cityChild?.statistics.nullCount).toBe(1);
    });
  });

  describe('Repetition and definition levels', () => {
    it('should generate correct definition levels for optional struct', () => {
      const optionalWriter = new StructColumnWriter('profile', {
        fields: [{ name: 'bio', type: 'string' }],
        optional: true,
      });

      optionalWriter.write({ bio: 'Hello' });
      optionalWriter.write(null);
      optionalWriter.write({ bio: null });
      const result = optionalWriter.finish();

      // Definition levels: 2 (struct present, field present), 0 (struct null), 1 (struct present, field null)
      expect(result.definitionLevels).toEqual([2, 0, 1]);
    });

    it('should set max definition level based on nesting depth', () => {
      const nestedWriter = new StructColumnWriter('outer', {
        fields: [
          {
            name: 'inner',
            type: 'struct',
            fields: [{ name: 'value', type: 'string' }],
          },
        ],
      });

      nestedWriter.write({ inner: { value: 'test' } });
      const result = nestedWriter.finish();

      // Definition levels track optional fields, not nesting depth
      // With non-optional structs, only the inner string field is nullable (default)
      expect(result.maxDefinitionLevel).toBe(1);
    });
  });

  describe('Nested structs', () => {
    it('should support deeply nested structs', () => {
      const deepWriter = new StructColumnWriter('person', {
        fields: [
          { name: 'name', type: 'string' },
          {
            name: 'address',
            type: 'struct',
            fields: [
              { name: 'street', type: 'string' },
              {
                name: 'location',
                type: 'struct',
                fields: [
                  { name: 'lat', type: 'double' },
                  { name: 'lng', type: 'double' },
                ],
              },
            ],
          },
        ],
      });

      deepWriter.write({
        name: 'Alice',
        address: {
          street: '123 Main St',
          location: { lat: 40.7128, lng: -74.006 },
        },
      });

      const result = deepWriter.finish();

      expect(result.children).toHaveLength(2);
      const addressChild = result.children.find((c) => c.columnName === 'person.address');
      expect(addressChild?.children).toHaveLength(2);
    });
  });
});

// ============================================================================
// List/Array Column Writer
// ============================================================================

describe('ListColumnWriter', () => {
  let writer: ListColumnWriter;

  beforeEach(() => {
    writer = new ListColumnWriter('tags', { elementType: 'string' });
  });

  describe('Basic writing', () => {
    it('should write array values', () => {
      writer.write(['red', 'green', 'blue']);
      const result = writer.finish();

      expect(result.numValues).toBe(1);
      expect(result.dataType).toBe('LIST');
    });

    it('should write multiple arrays', () => {
      writer.write(['a', 'b']);
      writer.write(['c', 'd', 'e']);
      writer.write(['f']);
      const result = writer.finish();

      expect(result.numValues).toBe(3);
    });

    it('should write empty arrays', () => {
      writer.write([]);
      writer.write(['a']);
      writer.write([]);
      const result = writer.finish();

      expect(result.numValues).toBe(3);
    });
  });

  describe('Element types', () => {
    it('should support int32 element type', () => {
      const intListWriter = new ListColumnWriter('scores', { elementType: 'int32' });
      intListWriter.write([1, 2, 3]);
      const result = intListWriter.finish();

      expect(result.elementChild.dataType).toBe('INT32');
    });

    it('should support double element type', () => {
      const doubleListWriter = new ListColumnWriter('prices', { elementType: 'double' });
      doubleListWriter.write([1.5, 2.5, 3.5]);
      const result = doubleListWriter.finish();

      expect(result.elementChild.dataType).toBe('DOUBLE');
    });

    it('should support nested list element type', () => {
      const nestedListWriter = new ListColumnWriter('matrix', {
        elementType: 'list',
        elementOptions: { elementType: 'int32' },
      });

      nestedListWriter.write([
        [1, 2],
        [3, 4],
      ]);
      const result = nestedListWriter.finish();

      expect(result.dataType).toBe('LIST');
      expect(result.elementChild.dataType).toBe('LIST');
    });

    it('should support struct element type', () => {
      const structListWriter = new ListColumnWriter('users', {
        elementType: 'struct',
        elementOptions: {
          fields: [
            { name: 'name', type: 'string' },
            { name: 'age', type: 'int32' },
          ],
        },
      });

      structListWriter.write([
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 },
      ]);
      const result = structListWriter.finish();

      expect(result.elementChild.dataType).toBe('STRUCT');
    });
  });

  describe('Null handling', () => {
    it('should write null arrays', () => {
      writer.write(['a']);
      writer.write(null);
      writer.write(['b']);
      const result = writer.finish();

      expect(result.statistics.nullCount).toBe(1);
    });

    it('should write arrays with null elements', () => {
      writer.write(['a', null, 'b']);
      const result = writer.finish();

      expect(result.elementChild.statistics.nullCount).toBe(1);
    });
  });

  describe('Repetition and definition levels', () => {
    it('should generate correct repetition levels', () => {
      writer.write(['a', 'b', 'c']);
      writer.write(['d']);
      const result = writer.finish();

      // Rep levels: 0 (new list), 1 (continuation), 1 (continuation), 0 (new list)
      expect(result.repetitionLevels).toEqual([0, 1, 1, 0]);
    });

    it('should generate correct definition levels for nullable list', () => {
      const nullableWriter = new ListColumnWriter('items', {
        elementType: 'string',
        nullable: true,
      });

      nullableWriter.write(['a']);
      nullableWriter.write(null);
      nullableWriter.write([null]);
      nullableWriter.write([]);
      const result = nullableWriter.finish();

      // Def levels encode: list present+element present, list null, list present+element null, list present+empty
      expect(result.definitionLevels).toEqual([3, 0, 2, 1]);
    });

    it('should set max repetition level correctly', () => {
      writer.write(['a', 'b']);
      const result = writer.finish();

      expect(result.maxRepetitionLevel).toBe(1);
    });

    it('should handle nested list repetition levels', () => {
      const nestedWriter = new ListColumnWriter('nested', {
        elementType: 'list',
        elementOptions: { elementType: 'int32' },
      });

      nestedWriter.write([
        [1, 2],
        [3, 4, 5],
      ]);
      const result = nestedWriter.finish();

      expect(result.maxRepetitionLevel).toBe(2);
    });
  });

  describe('Statistics', () => {
    it('should track list lengths', () => {
      writer.write(['a', 'b', 'c']);
      writer.write(['d']);
      writer.write(['e', 'f']);
      const result = writer.finish();

      expect(result.statistics.minListLength).toBe(1);
      expect(result.statistics.maxListLength).toBe(3);
    });

    it('should track total element count', () => {
      writer.write(['a', 'b']);
      writer.write(['c', 'd', 'e']);
      const result = writer.finish();

      expect(result.statistics.totalElements).toBe(5);
    });
  });
});

// ============================================================================
// Generic Column Writer Factory
// ============================================================================

describe('ColumnWriter factory', () => {
  it('should create string writer', () => {
    const writer = ColumnWriter.create('name', 'string');
    expect(writer).toBeInstanceOf(StringColumnWriter);
  });

  it('should create int32 writer', () => {
    const writer = ColumnWriter.create('count', 'int32');
    expect(writer).toBeInstanceOf(Int32ColumnWriter);
  });

  it('should create int64 writer', () => {
    const writer = ColumnWriter.create('bigcount', 'int64');
    expect(writer).toBeInstanceOf(Int64ColumnWriter);
  });

  it('should create float writer', () => {
    const writer = ColumnWriter.create('temp', 'float');
    expect(writer).toBeInstanceOf(FloatColumnWriter);
  });

  it('should create double writer', () => {
    const writer = ColumnWriter.create('price', 'double');
    expect(writer).toBeInstanceOf(DoubleColumnWriter);
  });

  it('should create boolean writer', () => {
    const writer = ColumnWriter.create('active', 'boolean');
    expect(writer).toBeInstanceOf(BooleanColumnWriter);
  });

  it('should create timestamp writer', () => {
    const writer = ColumnWriter.create('createdAt', 'timestamp');
    expect(writer).toBeInstanceOf(TimestampColumnWriter);
  });

  it('should create binary writer', () => {
    const writer = ColumnWriter.create('data', 'binary');
    expect(writer).toBeInstanceOf(BinaryColumnWriter);
  });

  it('should create struct writer with schema', () => {
    const writer = ColumnWriter.create('address', 'struct', {
      fields: [
        { name: 'street', type: 'string' },
        { name: 'zip', type: 'int32' },
      ],
    });
    expect(writer).toBeInstanceOf(StructColumnWriter);
  });

  it('should create list writer with element type', () => {
    const writer = ColumnWriter.create('tags', 'list', { elementType: 'string' });
    expect(writer).toBeInstanceOf(ListColumnWriter);
  });

  it('should throw for unknown type', () => {
    expect(() => ColumnWriter.create('field', 'unknown' as any)).toThrow();
  });

  it('should pass options to created writer', () => {
    const writer = ColumnWriter.create('name', 'string', {
      compression: 'zstd',
      useDictionary: true,
    });
    writer.write('test');
    const result = writer.finish();

    expect(result.compression).toBe('zstd');
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('Column Writer Integration', () => {
  it('should write a complete column with all features', () => {
    const writer = new StringColumnWriter('category', {
      compression: 'snappy',
      useDictionary: true,
      nullable: true,
    });

    // Write varied data
    for (let i = 0; i < 1000; i++) {
      if (i % 50 === 0) {
        writer.write(null);
      } else {
        writer.write(['electronics', 'clothing', 'food', 'books'][i % 4]);
      }
    }

    const result = writer.finish();

    // Verify all aspects
    expect(result.numValues).toBe(1000);
    expect(result.statistics.nullCount).toBe(20);
    expect(result.statistics.distinctCount).toBe(4);
    expect(result.encoding).toBe('PLAIN_DICTIONARY');
    expect(result.compression).toBe('snappy');
    expect(result.compressedSize).toBeLessThan(result.uncompressedSize);
    expect(result.definitionLevels).toHaveLength(1000);
  });

  it('should write complex nested structure', () => {
    const writer = new StructColumnWriter('order', {
      fields: [
        { name: 'id', type: 'string' },
        { name: 'total', type: 'double' },
        {
          name: 'items',
          type: 'list',
          elementType: 'struct',
          elementOptions: {
            fields: [
              { name: 'sku', type: 'string' },
              { name: 'qty', type: 'int32' },
              { name: 'price', type: 'double' },
            ],
          },
        },
        {
          name: 'shipping',
          type: 'struct',
          fields: [
            { name: 'address', type: 'string' },
            { name: 'zip', type: 'string' },
          ],
        },
      ],
    });

    writer.write({
      id: 'order-001',
      total: 99.99,
      items: [
        { sku: 'WIDGET-A', qty: 2, price: 29.99 },
        { sku: 'GADGET-B', qty: 1, price: 39.99 },
      ],
      shipping: {
        address: '123 Main St',
        zip: '10001',
      },
    });

    const result = writer.finish();

    expect(result.dataType).toBe('STRUCT');
    expect(result.children.length).toBe(4);

    // Check nested list of structs
    const itemsChild = result.children.find((c) => c.columnName === 'order.items');
    expect(itemsChild?.dataType).toBe('LIST');
  });

  it('should handle batch writing efficiently', () => {
    const writer = new Int64ColumnWriter('seq');

    const startTime = performance.now();

    for (let i = 0; i < 100000; i++) {
      writer.write(BigInt(i));
    }

    const result = writer.finish();
    const elapsed = performance.now() - startTime;

    expect(result.numValues).toBe(100000);
    expect(elapsed).toBeLessThan(1000); // Should complete in under 1 second
  });

  it('should produce valid Parquet page headers', () => {
    const writer = new StringColumnWriter('test');
    writer.write('hello');
    writer.write('world');
    const result = writer.finish();

    // Verify page header structure
    expect(result.pageHeader).toBeDefined();
    expect(result.pageHeader.type).toBe('DATA_PAGE');
    expect(result.pageHeader.uncompressedPageSize).toBeGreaterThan(0);
    expect(result.pageHeader.compressedPageSize).toBeGreaterThan(0);
    expect(result.pageHeader.numValues).toBe(2);
  });
});
