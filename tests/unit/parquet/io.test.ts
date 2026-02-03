/**
 * Tests for Parquet I/O Module
 *
 * Verifies that writeParquet/readParquet produce valid binary Parquet format
 * and can round-trip documents correctly.
 */

import { describe, it, expect } from 'vitest';
import { writeParquet, readParquet, isParquetFile } from '../../../src/parquet/io.js';

describe('Parquet I/O', () => {
  describe('writeParquet', () => {
    it('creates valid Parquet binary with magic bytes', () => {
      const rows = [
        { _id: 'doc-1', _seq: 1, _op: 'i' as const, doc: { name: 'Alice' } },
      ];

      const data = writeParquet(rows);

      // Check Parquet magic bytes at start and end
      expect(data[0]).toBe(0x50); // 'P'
      expect(data[1]).toBe(0x41); // 'A'
      expect(data[2]).toBe(0x52); // 'R'
      expect(data[3]).toBe(0x31); // '1'

      expect(data[data.length - 4]).toBe(0x50); // 'P'
      expect(data[data.length - 3]).toBe(0x41); // 'A'
      expect(data[data.length - 2]).toBe(0x52); // 'R'
      expect(data[data.length - 1]).toBe(0x31); // '1'
    });

    it('creates file recognized by isParquetFile', () => {
      const rows = [
        { _id: 'doc-1', _seq: 1, _op: 'i' as const, doc: { name: 'Alice' } },
      ];

      const data = writeParquet(rows);
      expect(isParquetFile(data)).toBe(true);
    });

    it('creates valid Parquet for multiple rows', () => {
      const rows = Array.from({ length: 100 }, (_, i) => ({
        _id: `doc-${i}`,
        _seq: i + 1,
        _op: 'i' as const,
        doc: { name: `User ${i}`, value: i },
      }));

      const data = writeParquet(rows);

      // Should be valid Parquet
      expect(isParquetFile(data)).toBe(true);
      // Size should be reasonable (variant + base64 encoding)
      expect(data.length).toBeGreaterThan(0);
    });
  });

  describe('readParquet', () => {
    it('round-trips simple documents', async () => {
      const rows = [
        { _id: 'doc-1', _seq: 1, _op: 'i' as const, doc: { name: 'Alice', age: 30 } },
        { _id: 'doc-2', _seq: 2, _op: 'i' as const, doc: { name: 'Bob', age: 25 } },
      ];

      const data = writeParquet(rows);
      const result = await readParquet(data);

      expect(result).toHaveLength(2);
      expect(result[0]._id).toBe('doc-1');
      expect(result[0]._seq).toBe(1);
      expect(result[0]._op).toBe('i');
      expect(result[0].doc).toEqual({ name: 'Alice', age: 30 });
      expect(result[1]._id).toBe('doc-2');
      expect(result[1].doc).toEqual({ name: 'Bob', age: 25 });
    });

    it('round-trips nested documents', async () => {
      const rows = [
        {
          _id: 'doc-1',
          _seq: 1,
          _op: 'i' as const,
          doc: {
            name: 'Alice',
            address: {
              street: '123 Main St',
              city: 'San Francisco',
            },
            tags: ['admin', 'user'],
          },
        },
      ];

      const data = writeParquet(rows);
      const result = await readParquet(data);

      expect(result[0].doc.address).toEqual({
        street: '123 Main St',
        city: 'San Francisco',
      });
      expect(result[0].doc.tags).toEqual(['admin', 'user']);
    });

    it('round-trips various data types', async () => {
      const now = new Date();
      const rows = [
        {
          _id: 'doc-1',
          _seq: 1,
          _op: 'i' as const,
          doc: {
            string: 'hello',
            number: 42,
            float: 3.14,
            boolean: true,
            null: null,
            array: [1, 2, 3],
            object: { nested: true },
            date: now,
          },
        },
      ];

      const data = writeParquet(rows);
      const result = await readParquet(data);

      expect(result[0].doc.string).toBe('hello');
      expect(result[0].doc.number).toBe(42);
      expect(result[0].doc.float).toBeCloseTo(3.14);
      expect(result[0].doc.boolean).toBe(true);
      expect(result[0].doc.null).toBeNull();
      expect(result[0].doc.array).toEqual([1, 2, 3]);
      expect(result[0].doc.object).toEqual({ nested: true });
      expect(new Date(result[0].doc.date as Date).getTime()).toBe(now.getTime());
    });

    it('handles empty documents', async () => {
      const rows = [
        { _id: 'doc-1', _seq: 1, _op: 'i' as const, doc: {} },
      ];

      const data = writeParquet(rows);
      const result = await readParquet(data);

      expect(result[0].doc).toEqual({});
    });

    it('handles all operation types', async () => {
      const rows = [
        { _id: 'doc-1', _seq: 1, _op: 'i' as const, doc: { value: 1 } },
        { _id: 'doc-1', _seq: 2, _op: 'u' as const, doc: { value: 2 } },
        { _id: 'doc-1', _seq: 3, _op: 'd' as const, doc: {} },
      ];

      const data = writeParquet(rows);
      const result = await readParquet(data);

      expect(result[0]._op).toBe('i');
      expect(result[1]._op).toBe('u');
      expect(result[2]._op).toBe('d');
    });
  });

  describe('legacy JSON compatibility', () => {
    it('reads legacy JSON format', async () => {
      // Simulate the old JSON format that was stored in .parquet files
      const legacyData = JSON.stringify([
        { _id: 'doc-1', _seq: 1, _op: 'i', _data: { name: 'Alice' } },
        { _id: 'doc-2', _seq: 2, _op: 'i', _data: { name: 'Bob' } },
      ]);

      const result = await readParquet(new TextEncoder().encode(legacyData));

      expect(result).toHaveLength(2);
      expect(result[0]._id).toBe('doc-1');
      expect(result[0].doc).toEqual({ name: 'Alice' });
    });
  });

  describe('isParquetFile', () => {
    it('returns true for valid Parquet files', () => {
      const rows = [{ _id: '1', _seq: 1, _op: 'i' as const, doc: {} }];
      const data = writeParquet(rows);
      expect(isParquetFile(data)).toBe(true);
    });

    it('returns false for JSON data', () => {
      const jsonData = new TextEncoder().encode('{"test": true}');
      expect(isParquetFile(jsonData)).toBe(false);
    });

    it('returns false for empty data', () => {
      expect(isParquetFile(new Uint8Array(0))).toBe(false);
      expect(isParquetFile(new Uint8Array(4))).toBe(false);
    });
  });

  // ==========================================================================
  // Error Scenarios
  // ==========================================================================

  describe('writeParquet - Error Scenarios', () => {
    it('handles empty rows array', () => {
      // The underlying hyparquet-writer throws when it cannot determine column types
      // from empty data. This is expected behavior.
      expect(() => writeParquet([])).toThrow();
    });

    it('handles rows with null _id', () => {
      // null _id cannot be processed by hyparquet-writer because it cannot determine type
      const rows = [
        { _id: null as unknown as string, _seq: 1, _op: 'i' as const, doc: { name: 'Test' } },
      ];

      // Should throw because null _id prevents type inference
      expect(() => writeParquet(rows)).toThrow();
    });

    it('handles rows with undefined doc', () => {
      const rows = [
        { _id: 'doc-1', _seq: 1, _op: 'i' as const, doc: undefined as unknown as object },
      ];

      // undefined is not a valid Variant value, so it throws
      expect(() => writeParquet(rows)).toThrow();
    });

    it('handles rows with very large documents', () => {
      const largeDoc = {
        data: 'x'.repeat(1000000), // 1MB string
      };
      const rows = [
        { _id: 'doc-1', _seq: 1, _op: 'i' as const, doc: largeDoc },
      ];

      const data = writeParquet(rows);
      expect(isParquetFile(data)).toBe(true);
    });

    it('handles special characters in document values', () => {
      const rows = [
        {
          _id: 'doc-1',
          _seq: 1,
          _op: 'i' as const,
          doc: {
            nullByte: 'test\0null',
            unicode: '\u0000\u001f\u007f',
            emoji: '\uD83D\uDE00',
            newlines: 'line1\nline2\rline3',
          },
        },
      ];

      const data = writeParquet(rows);
      expect(isParquetFile(data)).toBe(true);
    });

    it('handles circular reference detection', () => {
      const circular: Record<string, unknown> = { name: 'test' };
      circular.self = circular;

      const rows = [
        { _id: 'doc-1', _seq: 1, _op: 'i' as const, doc: circular },
      ];

      // Should throw due to circular reference in JSON.stringify
      expect(() => writeParquet(rows)).toThrow();
    });

    it('handles BigInt values (not JSON serializable)', () => {
      const rows = [
        { _id: 'doc-1', _seq: 1, _op: 'i' as const, doc: { value: BigInt(9007199254740991) } },
      ];

      // BigInt cannot be serialized to JSON by default
      expect(() => writeParquet(rows)).toThrow();
    });
  });

  describe('readParquet - Error Scenarios', () => {
    it('throws for completely invalid data', async () => {
      const invalidData = new TextEncoder().encode('not valid data at all');

      // Should throw or return empty array depending on implementation
      await expect(readParquet(invalidData)).rejects.toThrow();
    });

    it('throws for truncated Parquet file', async () => {
      const rows = [{ _id: '1', _seq: 1, _op: 'i' as const, doc: { name: 'test' } }];
      const validData = writeParquet(rows);

      // Truncate the data
      const truncated = validData.slice(0, validData.length / 2);

      await expect(readParquet(truncated)).rejects.toThrow();
    });

    it('throws for corrupted magic bytes', async () => {
      const rows = [{ _id: '1', _seq: 1, _op: 'i' as const, doc: { name: 'test' } }];
      const data = writeParquet(rows);

      // Corrupt the start magic bytes
      data[0] = 0x00;
      data[1] = 0x00;
      data[2] = 0x00;
      data[3] = 0x00;

      await expect(readParquet(data)).rejects.toThrow();
    });

    it('throws for corrupted end magic bytes', async () => {
      const rows = [{ _id: '1', _seq: 1, _op: 'i' as const, doc: { name: 'test' } }];
      const data = writeParquet(rows);

      // Corrupt the end magic bytes
      data[data.length - 4] = 0x00;
      data[data.length - 3] = 0x00;
      data[data.length - 2] = 0x00;
      data[data.length - 1] = 0x00;

      await expect(readParquet(data)).rejects.toThrow();
    });

    it('handles malformed JSON in legacy format', async () => {
      const malformedJson = new TextEncoder().encode('[{"_id": "doc-1", invalid json');

      await expect(readParquet(malformedJson)).rejects.toThrow();
    });

    it('handles empty Uint8Array', async () => {
      await expect(readParquet(new Uint8Array(0))).rejects.toThrow();
    });

    it('handles ArrayBuffer input', async () => {
      const rows = [{ _id: '1', _seq: 1, _op: 'i' as const, doc: { name: 'test' } }];
      const data = writeParquet(rows);

      // Convert to ArrayBuffer and back to Uint8Array
      const arrayBuffer = data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
      const result = await readParquet(new Uint8Array(arrayBuffer));

      expect(result).toHaveLength(1);
      expect(result[0]._id).toBe('1');
    });
  });

  describe('isParquetFile - Error Scenarios', () => {
    it('returns false for partial magic bytes', () => {
      // Only start magic, no end magic
      const partial = new Uint8Array([0x50, 0x41, 0x52, 0x31, 0x00, 0x00, 0x00, 0x00]);
      expect(isParquetFile(partial)).toBe(false);
    });

    it('returns false for reversed magic bytes', () => {
      const reversed = new Uint8Array([0x31, 0x52, 0x41, 0x50, 0x31, 0x52, 0x41, 0x50]);
      expect(isParquetFile(reversed)).toBe(false);
    });

    it('returns false for data shorter than 8 bytes', () => {
      expect(isParquetFile(new Uint8Array(7))).toBe(false);
    });

    it('handles null/undefined gracefully', () => {
      // @ts-expect-error - Testing error case
      expect(() => isParquetFile(null)).toThrow();
      // @ts-expect-error - Testing error case
      expect(() => isParquetFile(undefined)).toThrow();
    });

    it('returns false for random binary data', () => {
      const random = new Uint8Array(1024);
      for (let i = 0; i < random.length; i++) {
        random[i] = Math.floor(Math.random() * 256);
      }
      expect(isParquetFile(random)).toBe(false);
    });
  });

  describe('Edge cases and boundary conditions', () => {
    it('handles documents with deeply nested structures', async () => {
      let nested: Record<string, unknown> = { value: 'leaf' };
      for (let i = 0; i < 50; i++) {
        nested = { nested };
      }

      const rows = [{ _id: '1', _seq: 1, _op: 'i' as const, doc: nested }];
      const data = writeParquet(rows);
      const result = await readParquet(data);

      expect(result).toHaveLength(1);
    });

    it('handles documents with many keys', async () => {
      const manyKeys: Record<string, number> = {};
      for (let i = 0; i < 1000; i++) {
        manyKeys[`key_${i}`] = i;
      }

      const rows = [{ _id: '1', _seq: 1, _op: 'i' as const, doc: manyKeys }];
      const data = writeParquet(rows);
      const result = await readParquet(data);

      expect(result[0].doc.key_0).toBe(0);
      expect(result[0].doc.key_999).toBe(999);
    });

    it('handles documents with very long string values', async () => {
      const longString = 'a'.repeat(100000);

      const rows = [{ _id: '1', _seq: 1, _op: 'i' as const, doc: { text: longString } }];
      const data = writeParquet(rows);
      const result = await readParquet(data);

      expect(result[0].doc.text).toBe(longString);
    });

    it('handles documents with very large arrays', async () => {
      const largeArray = Array.from({ length: 10000 }, (_, i) => i);

      const rows = [{ _id: '1', _seq: 1, _op: 'i' as const, doc: { items: largeArray } }];
      const data = writeParquet(rows);
      const result = await readParquet(data);

      expect(result[0].doc.items).toHaveLength(10000);
    });

    it('handles documents with special number values', async () => {
      const rows = [
        {
          _id: '1',
          _seq: 1,
          _op: 'i' as const,
          doc: {
            zero: 0,
            negZero: -0,
            infinity: Infinity,
            negInfinity: -Infinity,
            maxSafe: Number.MAX_SAFE_INTEGER,
            minSafe: Number.MIN_SAFE_INTEGER,
            epsilon: Number.EPSILON,
          },
        },
      ];

      const data = writeParquet(rows);
      const result = await readParquet(data);

      expect(result[0].doc.zero).toBe(0);
      expect(result[0].doc.maxSafe).toBe(Number.MAX_SAFE_INTEGER);
    });

    it('handles NaN values', async () => {
      const rows = [
        { _id: '1', _seq: 1, _op: 'i' as const, doc: { nan: NaN } },
      ];

      const data = writeParquet(rows);
      const result = await readParquet(data);

      // NaN is preserved as NaN (float type in Parquet)
      expect(result[0].doc.nan).toBeNaN();
    });

    it('handles undefined values in documents', async () => {
      const rows = [
        { _id: '1', _seq: 1, _op: 'i' as const, doc: { value: undefined } },
      ];

      const data = writeParquet(rows);
      const result = await readParquet(data);

      // undefined is converted to null in Variant encoding
      // and may be preserved or converted depending on implementation
      // Check if value exists and is null/undefined
      const value = result[0].doc.value;
      expect(value === null || value === undefined).toBe(true);
    });
  });
});
