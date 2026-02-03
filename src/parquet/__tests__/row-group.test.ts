/**
 * Row Group Serializer Tests
 *
 * Tests for converting documents to Parquet row group format
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  RowGroupSerializer,
  type RowGroupSerializerOptions,
  type SerializedRowGroup,
  type ColumnChunk,
  type ColumnStatistics,
  type CompressionCodec,
} from '../row-group.js';

// Test document type
interface TestDocument {
  _id: string;
  _seq: number;
  _op: 'i' | 'u' | 'd';
  name?: string;
  age?: number;
  active?: boolean;
  score?: number;
  tags?: string[];
  metadata?: Record<string, unknown>;
  createdAt?: Date;
}

describe('RowGroupSerializer', () => {
  let serializer: RowGroupSerializer;

  beforeEach(() => {
    serializer = new RowGroupSerializer();
  });

  describe('Empty row group', () => {
    it('should serialize an empty row group', () => {
      const result = serializer.serialize([]);

      expect(result).toBeDefined();
      expect(result.rowCount).toBe(0);
      expect(result.columnChunks).toHaveLength(0);
      expect(result.data).toBeInstanceOf(Uint8Array);
      expect(result.data.byteLength).toBeGreaterThan(0);
    });

    it('should include valid metadata for empty row group', () => {
      const result = serializer.serialize([]);

      expect(result.metadata).toBeDefined();
      expect(result.metadata.numRows).toBe(0);
      expect(result.metadata.totalByteSize).toBeGreaterThanOrEqual(0);
    });
  });

  describe('Single document serialization', () => {
    it('should serialize a single document with required fields', () => {
      const doc: TestDocument = {
        _id: 'doc1',
        _seq: 1,
        _op: 'i',
      };

      const result = serializer.serialize([doc]);

      expect(result.rowCount).toBe(1);
      expect(result.columnChunks.length).toBeGreaterThanOrEqual(3); // _id, _seq, _op
    });

    it('should serialize a document with all field types', () => {
      const doc: TestDocument = {
        _id: 'doc1',
        _seq: 1,
        _op: 'i',
        name: 'Alice',
        age: 30,
        active: true,
        score: 95.5,
        createdAt: new Date('2024-01-15T10:30:00Z'),
      };

      const result = serializer.serialize([doc]);

      expect(result.rowCount).toBe(1);

      // Should have column chunks for each field
      const columnNames = result.columnChunks.map((c) => c.columnName);
      expect(columnNames).toContain('_id');
      expect(columnNames).toContain('_seq');
      expect(columnNames).toContain('_op');
      expect(columnNames).toContain('name');
      expect(columnNames).toContain('age');
      expect(columnNames).toContain('active');
      expect(columnNames).toContain('score');
      expect(columnNames).toContain('createdAt');
    });

    it('should include correct data types in column chunks', () => {
      const doc: TestDocument = {
        _id: 'doc1',
        _seq: 1,
        _op: 'i',
        name: 'Alice',
        age: 30,
        active: true,
        score: 95.5,
      };

      const result = serializer.serialize([doc]);

      const getColumnChunk = (name: string) =>
        result.columnChunks.find((c) => c.columnName === name);

      expect(getColumnChunk('_id')?.dataType).toBe('string');
      expect(getColumnChunk('_seq')?.dataType).toBe('int64');
      expect(getColumnChunk('_op')?.dataType).toBe('string');
      expect(getColumnChunk('name')?.dataType).toBe('string');
      expect(getColumnChunk('age')?.dataType).toBe('int64');
      expect(getColumnChunk('active')?.dataType).toBe('boolean');
      expect(getColumnChunk('score')?.dataType).toBe('double');
    });

    it('should produce valid binary output', () => {
      const doc: TestDocument = {
        _id: 'doc1',
        _seq: 1,
        _op: 'i',
        name: 'Test',
      };

      const result = serializer.serialize([doc]);

      expect(result.data).toBeInstanceOf(Uint8Array);
      expect(result.data.byteLength).toBeGreaterThan(0);

      // Check Parquet magic bytes (PAR1)
      const magic = new TextDecoder().decode(result.data.slice(0, 4));
      expect(magic).toBe('PAR1');
    });
  });

  describe('Multiple documents serialization', () => {
    it('should serialize multiple documents', () => {
      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i', name: 'Alice' },
        { _id: 'doc2', _seq: 2, _op: 'i', name: 'Bob' },
        { _id: 'doc3', _seq: 3, _op: 'u', name: 'Charlie' },
      ];

      const result = serializer.serialize(docs);

      expect(result.rowCount).toBe(3);
    });

    it('should handle documents with different fields', () => {
      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i', name: 'Alice', age: 25 },
        { _id: 'doc2', _seq: 2, _op: 'i', name: 'Bob' }, // no age
        { _id: 'doc3', _seq: 3, _op: 'i', score: 88.5 }, // no name or age
      ];

      const result = serializer.serialize(docs);

      expect(result.rowCount).toBe(3);

      // All discovered columns should be present
      const columnNames = result.columnChunks.map((c) => c.columnName);
      expect(columnNames).toContain('name');
      expect(columnNames).toContain('age');
      expect(columnNames).toContain('score');
    });

    it('should handle null values correctly', () => {
      const docs = [
        { _id: 'doc1', _seq: 1, _op: 'i' as const, name: 'Alice', age: 25 },
        { _id: 'doc2', _seq: 2, _op: 'i' as const, name: null, age: null },
        { _id: 'doc3', _seq: 3, _op: 'i' as const, name: 'Charlie', age: 35 },
      ];

      const result = serializer.serialize(docs);

      expect(result.rowCount).toBe(3);

      // Column chunks should have null count
      const nameChunk = result.columnChunks.find((c) => c.columnName === 'name');
      const ageChunk = result.columnChunks.find((c) => c.columnName === 'age');

      expect(nameChunk?.statistics?.nullCount).toBe(1);
      expect(ageChunk?.statistics?.nullCount).toBe(1);
    });

    it('should preserve document order', () => {
      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i' },
        { _id: 'doc2', _seq: 2, _op: 'i' },
        { _id: 'doc3', _seq: 3, _op: 'i' },
        { _id: 'doc4', _seq: 4, _op: 'i' },
      ];

      const result = serializer.serialize(docs);

      // The _seq column should have values in order 1, 2, 3, 4
      const seqChunk = result.columnChunks.find((c) => c.columnName === '_seq');
      expect(seqChunk?.statistics?.minValue).toBe(1);
      expect(seqChunk?.statistics?.maxValue).toBe(4);
    });

    it('should serialize large document batches efficiently', () => {
      const docs: TestDocument[] = Array.from({ length: 10000 }, (_, i) => ({
        _id: `doc${i}`,
        _seq: i + 1,
        _op: 'i' as const,
        name: `User ${i}`,
        age: 20 + (i % 50),
        score: Math.random() * 100,
      }));

      const startTime = performance.now();
      const result = serializer.serialize(docs);
      const elapsedTime = performance.now() - startTime;

      expect(result.rowCount).toBe(10000);
      expect(elapsedTime).toBeLessThan(5000); // Should complete within 5 seconds
    });
  });

  describe('Column statistics generation', () => {
    it('should generate min/max statistics for string columns', () => {
      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i', name: 'Charlie' },
        { _id: 'doc2', _seq: 2, _op: 'i', name: 'Alice' },
        { _id: 'doc3', _seq: 3, _op: 'i', name: 'Bob' },
      ];

      const result = serializer.serialize(docs);

      const nameChunk = result.columnChunks.find((c) => c.columnName === 'name');
      expect(nameChunk?.statistics).toBeDefined();
      expect(nameChunk?.statistics?.minValue).toBe('Alice');
      expect(nameChunk?.statistics?.maxValue).toBe('Charlie');
    });

    it('should generate min/max statistics for numeric columns', () => {
      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i', age: 25 },
        { _id: 'doc2', _seq: 2, _op: 'i', age: 45 },
        { _id: 'doc3', _seq: 3, _op: 'i', age: 30 },
      ];

      const result = serializer.serialize(docs);

      const ageChunk = result.columnChunks.find((c) => c.columnName === 'age');
      expect(ageChunk?.statistics).toBeDefined();
      expect(ageChunk?.statistics?.minValue).toBe(25);
      expect(ageChunk?.statistics?.maxValue).toBe(45);
    });

    it('should generate min/max statistics for double columns', () => {
      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i', score: 72.5 },
        { _id: 'doc2', _seq: 2, _op: 'i', score: 98.3 },
        { _id: 'doc3', _seq: 3, _op: 'i', score: 85.0 },
      ];

      const result = serializer.serialize(docs);

      const scoreChunk = result.columnChunks.find((c) => c.columnName === 'score');
      expect(scoreChunk?.statistics).toBeDefined();
      expect(scoreChunk?.statistics?.minValue).toBe(72.5);
      expect(scoreChunk?.statistics?.maxValue).toBe(98.3);
    });

    it('should generate min/max statistics for boolean columns', () => {
      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i', active: false },
        { _id: 'doc2', _seq: 2, _op: 'i', active: true },
        { _id: 'doc3', _seq: 3, _op: 'i', active: false },
      ];

      const result = serializer.serialize(docs);

      const activeChunk = result.columnChunks.find((c) => c.columnName === 'active');
      expect(activeChunk?.statistics).toBeDefined();
      expect(activeChunk?.statistics?.minValue).toBe(false);
      expect(activeChunk?.statistics?.maxValue).toBe(true);
    });

    it('should generate min/max statistics for date columns', () => {
      const date1 = new Date('2024-01-01');
      const date2 = new Date('2024-06-15');
      const date3 = new Date('2024-03-10');

      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i', createdAt: date1 },
        { _id: 'doc2', _seq: 2, _op: 'i', createdAt: date2 },
        { _id: 'doc3', _seq: 3, _op: 'i', createdAt: date3 },
      ];

      const result = serializer.serialize(docs);

      const createdAtChunk = result.columnChunks.find((c) => c.columnName === 'createdAt');
      expect(createdAtChunk?.statistics).toBeDefined();
      expect(createdAtChunk?.statistics?.minValue).toEqual(date1);
      expect(createdAtChunk?.statistics?.maxValue).toEqual(date2);
    });

    it('should count distinct values', () => {
      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i', name: 'Alice' },
        { _id: 'doc2', _seq: 2, _op: 'i', name: 'Bob' },
        { _id: 'doc3', _seq: 3, _op: 'i', name: 'Alice' },
        { _id: 'doc4', _seq: 4, _op: 'i', name: 'Charlie' },
      ];

      const result = serializer.serialize(docs);

      const nameChunk = result.columnChunks.find((c) => c.columnName === 'name');
      expect(nameChunk?.statistics?.distinctCount).toBe(3);
    });

    it('should track null count in statistics', () => {
      const docs = [
        { _id: 'doc1', _seq: 1, _op: 'i' as const, name: 'Alice' },
        { _id: 'doc2', _seq: 2, _op: 'i' as const, name: undefined },
        { _id: 'doc3', _seq: 3, _op: 'i' as const, name: null },
        { _id: 'doc4', _seq: 4, _op: 'i' as const, name: 'Bob' },
      ];

      const result = serializer.serialize(docs);

      const nameChunk = result.columnChunks.find((c) => c.columnName === 'name');
      expect(nameChunk?.statistics?.nullCount).toBe(2);
    });

    it('should exclude nulls from min/max calculations', () => {
      const docs = [
        { _id: 'doc1', _seq: 1, _op: 'i' as const, age: 25 },
        { _id: 'doc2', _seq: 2, _op: 'i' as const, age: null },
        { _id: 'doc3', _seq: 3, _op: 'i' as const, age: 45 },
      ];

      const result = serializer.serialize(docs);

      const ageChunk = result.columnChunks.find((c) => c.columnName === 'age');
      expect(ageChunk?.statistics?.minValue).toBe(25);
      expect(ageChunk?.statistics?.maxValue).toBe(45);
    });
  });

  describe('Data type handling', () => {
    it('should handle string type', () => {
      const doc = { _id: 'doc1', _seq: 1, _op: 'i' as const, value: 'hello world' };
      const result = serializer.serialize([doc]);

      const chunk = result.columnChunks.find((c) => c.columnName === 'value');
      expect(chunk?.dataType).toBe('string');
    });

    it('should handle integer type', () => {
      const doc = { _id: 'doc1', _seq: 1, _op: 'i' as const, value: 42 };
      const result = serializer.serialize([doc]);

      const chunk = result.columnChunks.find((c) => c.columnName === 'value');
      expect(chunk?.dataType).toBe('int64');
    });

    it('should handle float type', () => {
      const doc = { _id: 'doc1', _seq: 1, _op: 'i' as const, value: 3.14159 };
      const result = serializer.serialize([doc]);

      const chunk = result.columnChunks.find((c) => c.columnName === 'value');
      expect(chunk?.dataType).toBe('double');
    });

    it('should handle boolean type', () => {
      const doc = { _id: 'doc1', _seq: 1, _op: 'i' as const, value: true };
      const result = serializer.serialize([doc]);

      const chunk = result.columnChunks.find((c) => c.columnName === 'value');
      expect(chunk?.dataType).toBe('boolean');
    });

    it('should handle Date type', () => {
      const doc = { _id: 'doc1', _seq: 1, _op: 'i' as const, value: new Date() };
      const result = serializer.serialize([doc]);

      const chunk = result.columnChunks.find((c) => c.columnName === 'value');
      expect(chunk?.dataType).toBe('timestamp');
    });

    it('should handle binary (Uint8Array) type', () => {
      const doc = { _id: 'doc1', _seq: 1, _op: 'i' as const, value: new Uint8Array([1, 2, 3, 4]) };
      const result = serializer.serialize([doc]);

      const chunk = result.columnChunks.find((c) => c.columnName === 'value');
      expect(chunk?.dataType).toBe('binary');
    });

    it('should handle array type as variant', () => {
      const doc = { _id: 'doc1', _seq: 1, _op: 'i' as const, tags: ['a', 'b', 'c'] };
      const result = serializer.serialize([doc]);

      const chunk = result.columnChunks.find((c) => c.columnName === 'tags');
      expect(chunk?.dataType).toBe('variant');
    });

    it('should handle nested object type as variant', () => {
      const doc = {
        _id: 'doc1',
        _seq: 1,
        _op: 'i' as const,
        metadata: { key: 'value', nested: { deep: true } },
      };
      const result = serializer.serialize([doc]);

      const chunk = result.columnChunks.find((c) => c.columnName === 'metadata');
      expect(chunk?.dataType).toBe('variant');
    });

    it('should handle BigInt type', () => {
      const doc = { _id: 'doc1', _seq: 1, _op: 'i' as const, value: BigInt('9007199254740993') };
      const result = serializer.serialize([doc]);

      const chunk = result.columnChunks.find((c) => c.columnName === 'value');
      expect(chunk?.dataType).toBe('int64');
    });

    it('should handle null type', () => {
      const doc = { _id: 'doc1', _seq: 1, _op: 'i' as const, value: null };
      const result = serializer.serialize([doc]);

      // Null-only column should still be present
      const chunk = result.columnChunks.find((c) => c.columnName === 'value');
      expect(chunk).toBeDefined();
      expect(chunk?.statistics?.nullCount).toBe(1);
    });

    it('should handle mixed types with type coercion', () => {
      const docs = [
        { _id: 'doc1', _seq: 1, _op: 'i' as const, value: 42 },
        { _id: 'doc2', _seq: 2, _op: 'i' as const, value: '42' }, // string instead of number
        { _id: 'doc3', _seq: 3, _op: 'i' as const, value: 43 },
      ];

      const result = serializer.serialize(docs);

      // Should coerce to variant when types conflict
      const chunk = result.columnChunks.find((c) => c.columnName === 'value');
      expect(chunk?.dataType).toBe('variant');
    });
  });

  describe('Compression options', () => {
    it('should support no compression', () => {
      const options: RowGroupSerializerOptions = { compression: 'none' };
      const serializer = new RowGroupSerializer(options);

      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i', name: 'Test' },
      ];

      const result = serializer.serialize(docs);

      expect(result.compression).toBe('none');
      expect(result.data).toBeInstanceOf(Uint8Array);
    });

    it('should support snappy compression', () => {
      const options: RowGroupSerializerOptions = { compression: 'snappy' };
      const serializer = new RowGroupSerializer(options);

      const docs: TestDocument[] = Array.from({ length: 100 }, (_, i) => ({
        _id: `doc${i}`,
        _seq: i + 1,
        _op: 'i' as const,
        name: 'This is a repeated string that should compress well',
      }));

      const result = serializer.serialize(docs);

      expect(result.compression).toBe('snappy');

      // Compare with uncompressed
      const uncompressedSerializer = new RowGroupSerializer({ compression: 'none' });
      const uncompressedResult = uncompressedSerializer.serialize(docs);

      // Compressed should be smaller for repetitive data
      expect(result.data.byteLength).toBeLessThan(uncompressedResult.data.byteLength);
    });

    it('should support zstd compression', () => {
      const options: RowGroupSerializerOptions = { compression: 'zstd' };
      const serializer = new RowGroupSerializer(options);

      const docs: TestDocument[] = Array.from({ length: 100 }, (_, i) => ({
        _id: `doc${i}`,
        _seq: i + 1,
        _op: 'i' as const,
        name: 'This is a repeated string that should compress well',
      }));

      const result = serializer.serialize(docs);

      expect(result.compression).toBe('zstd');

      // Compare with uncompressed
      const uncompressedSerializer = new RowGroupSerializer({ compression: 'none' });
      const uncompressedResult = uncompressedSerializer.serialize(docs);

      // Compressed should be smaller for repetitive data
      expect(result.data.byteLength).toBeLessThan(uncompressedResult.data.byteLength);
    });

    it('should apply compression per column', () => {
      const options: RowGroupSerializerOptions = { compression: 'snappy' };
      const serializer = new RowGroupSerializer(options);

      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i', name: 'Alice', age: 25 },
      ];

      const result = serializer.serialize(docs);

      // Each column chunk should indicate its compression
      for (const chunk of result.columnChunks) {
        expect(chunk.compression).toBe('snappy');
      }
    });

    it('should include uncompressed size in column metadata', () => {
      const options: RowGroupSerializerOptions = { compression: 'snappy' };
      const serializer = new RowGroupSerializer(options);

      const docs: TestDocument[] = Array.from({ length: 100 }, (_, i) => ({
        _id: `doc${i}`,
        _seq: i + 1,
        _op: 'i' as const,
        name: 'Repeated data for compression',
      }));

      const result = serializer.serialize(docs);

      for (const chunk of result.columnChunks) {
        expect(chunk.uncompressedSize).toBeDefined();
        expect(chunk.uncompressedSize).toBeGreaterThan(0);
        expect(chunk.compressedSize).toBeDefined();
        expect(chunk.compressedSize).toBeLessThanOrEqual(chunk.uncompressedSize);
      }
    });

    it('should default to snappy compression', () => {
      const serializer = new RowGroupSerializer();

      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i', name: 'Test' },
      ];

      const result = serializer.serialize(docs);

      expect(result.compression).toBe('snappy');
    });
  });

  describe('Row group metadata generation', () => {
    it('should include total byte size', () => {
      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i', name: 'Alice' },
        { _id: 'doc2', _seq: 2, _op: 'i', name: 'Bob' },
      ];

      const result = serializer.serialize(docs);

      expect(result.metadata.totalByteSize).toBeDefined();
      expect(result.metadata.totalByteSize).toBeGreaterThan(0);
    });

    it('should include row count in metadata', () => {
      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i' },
        { _id: 'doc2', _seq: 2, _op: 'i' },
        { _id: 'doc3', _seq: 3, _op: 'i' },
      ];

      const result = serializer.serialize(docs);

      expect(result.metadata.numRows).toBe(3);
    });

    it('should include column metadata', () => {
      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i', name: 'Alice', age: 25 },
      ];

      const result = serializer.serialize(docs);

      expect(result.metadata.columns).toBeDefined();
      expect(Array.isArray(result.metadata.columns)).toBe(true);
      expect(result.metadata.columns.length).toBeGreaterThan(0);

      const nameColumn = result.metadata.columns.find((c) => c.name === 'name');
      expect(nameColumn).toBeDefined();
      expect(nameColumn?.type).toBe('string');
    });

    it('should include file offset for each column chunk', () => {
      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i', name: 'Alice', age: 25 },
      ];

      const result = serializer.serialize(docs);

      let lastOffset = 0;
      for (const chunk of result.columnChunks) {
        expect(chunk.fileOffset).toBeDefined();
        expect(chunk.fileOffset).toBeGreaterThanOrEqual(lastOffset);
        lastOffset = chunk.fileOffset + chunk.compressedSize;
      }
    });

    it('should include encoding information', () => {
      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i', name: 'Alice', age: 25 },
      ];

      const result = serializer.serialize(docs);

      for (const chunk of result.columnChunks) {
        expect(chunk.encoding).toBeDefined();
        expect(['PLAIN', 'RLE', 'DELTA_BINARY_PACKED', 'DELTA_LENGTH_BYTE_ARRAY']).toContain(
          chunk.encoding
        );
      }
    });

    it('should include schema information in metadata', () => {
      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i', name: 'Alice', age: 25, active: true },
      ];

      const result = serializer.serialize(docs);

      expect(result.metadata.schema).toBeDefined();
      expect(Array.isArray(result.metadata.schema)).toBe(true);

      const idSchema = result.metadata.schema.find((s) => s.name === '_id');
      expect(idSchema).toBeDefined();
      expect(idSchema?.type).toBe('BYTE_ARRAY');
      expect(idSchema?.repetitionType).toBe('REQUIRED');

      const nameSchema = result.metadata.schema.find((s) => s.name === 'name');
      expect(nameSchema).toBeDefined();
      expect(nameSchema?.type).toBe('BYTE_ARRAY');
      expect(nameSchema?.repetitionType).toBe('OPTIONAL');
    });

    it('should include sorting columns if specified', () => {
      const options: RowGroupSerializerOptions = {
        sortingColumns: ['_seq'],
      };
      const serializer = new RowGroupSerializer(options);

      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i' },
        { _id: 'doc2', _seq: 2, _op: 'i' },
      ];

      const result = serializer.serialize(docs);

      expect(result.metadata.sortingColumns).toBeDefined();
      expect(result.metadata.sortingColumns).toContain('_seq');
    });
  });

  describe('Error handling', () => {
    it('should throw on invalid document without _id', () => {
      const docs = [{ _seq: 1, _op: 'i' as const, name: 'Invalid' }] as unknown as TestDocument[];

      expect(() => serializer.serialize(docs)).toThrow();
    });

    it('should throw on invalid document without _seq', () => {
      const docs = [{ _id: 'doc1', _op: 'i' as const }] as unknown as TestDocument[];

      expect(() => serializer.serialize(docs)).toThrow();
    });

    it('should throw on invalid document without _op', () => {
      const docs = [{ _id: 'doc1', _seq: 1 }] as unknown as TestDocument[];

      expect(() => serializer.serialize(docs)).toThrow();
    });

    it('should throw on invalid _op value', () => {
      const docs = [{ _id: 'doc1', _seq: 1, _op: 'x' }] as unknown as TestDocument[];

      expect(() => serializer.serialize(docs)).toThrow();
    });

    it('should throw on unsupported compression codec', () => {
      const options = { compression: 'lz4' as CompressionCodec };

      expect(() => new RowGroupSerializer(options)).toThrow();
    });
  });

  describe('Serialization round-trip', () => {
    it('should produce consistent output for same input', () => {
      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i', name: 'Alice', age: 25 },
        { _id: 'doc2', _seq: 2, _op: 'i', name: 'Bob', age: 30 },
      ];

      const result1 = serializer.serialize(docs);
      const result2 = serializer.serialize(docs);

      expect(result1.rowCount).toBe(result2.rowCount);
      expect(result1.columnChunks.length).toBe(result2.columnChunks.length);
      expect(result1.metadata.numRows).toBe(result2.metadata.numRows);
    });
  });
});
