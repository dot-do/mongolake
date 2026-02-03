/**
 * Row Group Reader Tests
 *
 * Comprehensive tests for reading and deserializing Parquet row groups.
 * Tests cover:
 * - Column chunk reading
 * - Decompression (snappy, zstd)
 * - Value decoding
 * - Row reconstruction
 * - Round-trip serialization/deserialization
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { RowGroupReader, validateRowGroup } from '../../../src/parquet/row-group-reader.js';
import { RowGroupSerializer, type SerializedRowGroup } from '../../../src/parquet/row-group.js';

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
  data?: Uint8Array;
}

describe('RowGroupReader', () => {
  let serializer: RowGroupSerializer;
  let reader: RowGroupReader;

  beforeEach(() => {
    serializer = new RowGroupSerializer({ compression: 'none' });
    reader = new RowGroupReader();
  });

  describe('Empty row group', () => {
    it('should read an empty row group', () => {
      const serialized = serializer.serialize([]);
      const documents = reader.read(serialized);

      expect(documents).toEqual([]);
    });
  });

  describe('Single document round-trip', () => {
    it('should read a single document with required fields', () => {
      const doc: TestDocument = {
        _id: 'doc1',
        _seq: 1,
        _op: 'i',
      };

      const serialized = serializer.serialize([doc]);
      const documents = reader.read<TestDocument>(serialized);

      expect(documents).toHaveLength(1);
      expect(documents[0]._id).toBe('doc1');
      expect(documents[0]._seq).toBe(1);
      expect(documents[0]._op).toBe('i');
    });

    it('should read a document with string fields', () => {
      const doc: TestDocument = {
        _id: 'doc1',
        _seq: 1,
        _op: 'i',
        name: 'Alice',
      };

      const serialized = serializer.serialize([doc]);
      const documents = reader.read<TestDocument>(serialized);

      expect(documents).toHaveLength(1);
      expect(documents[0].name).toBe('Alice');
    });

    it('should read a document with integer fields', () => {
      const doc: TestDocument = {
        _id: 'doc1',
        _seq: 1,
        _op: 'i',
        age: 30,
      };

      const serialized = serializer.serialize([doc]);
      const documents = reader.read<TestDocument>(serialized);

      expect(documents).toHaveLength(1);
      expect(documents[0].age).toBe(30);
    });

    it('should read a document with double fields', () => {
      const doc: TestDocument = {
        _id: 'doc1',
        _seq: 1,
        _op: 'i',
        score: 95.5,
      };

      const serialized = serializer.serialize([doc]);
      const documents = reader.read<TestDocument>(serialized);

      expect(documents).toHaveLength(1);
      expect(documents[0].score).toBe(95.5);
    });

    it('should read a document with boolean fields', () => {
      const doc: TestDocument = {
        _id: 'doc1',
        _seq: 1,
        _op: 'i',
        active: true,
      };

      const serialized = serializer.serialize([doc]);
      const documents = reader.read<TestDocument>(serialized);

      expect(documents).toHaveLength(1);
      expect(documents[0].active).toBe(true);
    });

    it('should read a document with timestamp fields', () => {
      const date = new Date('2024-01-15T10:30:00Z');
      const doc: TestDocument = {
        _id: 'doc1',
        _seq: 1,
        _op: 'i',
        createdAt: date,
      };

      const serialized = serializer.serialize([doc]);
      const documents = reader.read<TestDocument>(serialized);

      expect(documents).toHaveLength(1);
      expect(documents[0].createdAt).toEqual(date);
    });

    it('should read a document with all field types', () => {
      const date = new Date('2024-01-15T10:30:00Z');
      const doc: TestDocument = {
        _id: 'doc1',
        _seq: 1,
        _op: 'i',
        name: 'Alice',
        age: 30,
        active: true,
        score: 95.5,
        createdAt: date,
      };

      const serialized = serializer.serialize([doc]);
      const documents = reader.read<TestDocument>(serialized);

      expect(documents).toHaveLength(1);
      expect(documents[0]._id).toBe('doc1');
      expect(documents[0]._seq).toBe(1);
      expect(documents[0]._op).toBe('i');
      expect(documents[0].name).toBe('Alice');
      expect(documents[0].age).toBe(30);
      expect(documents[0].active).toBe(true);
      expect(documents[0].score).toBe(95.5);
      expect(documents[0].createdAt).toEqual(date);
    });
  });

  describe('Multiple documents round-trip', () => {
    it('should read multiple documents', () => {
      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i', name: 'Alice' },
        { _id: 'doc2', _seq: 2, _op: 'i', name: 'Bob' },
        { _id: 'doc3', _seq: 3, _op: 'u', name: 'Charlie' },
      ];

      const serialized = serializer.serialize(docs);
      const documents = reader.read<TestDocument>(serialized);

      expect(documents).toHaveLength(3);
      expect(documents[0]._id).toBe('doc1');
      expect(documents[0].name).toBe('Alice');
      expect(documents[1]._id).toBe('doc2');
      expect(documents[1].name).toBe('Bob');
      expect(documents[2]._id).toBe('doc3');
      expect(documents[2].name).toBe('Charlie');
    });

    it('should handle documents with different fields (sparse)', () => {
      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i', name: 'Alice', age: 25 },
        { _id: 'doc2', _seq: 2, _op: 'i', name: 'Bob' }, // no age
        { _id: 'doc3', _seq: 3, _op: 'i', score: 88.5 }, // no name or age
      ];

      const serialized = serializer.serialize(docs);
      const documents = reader.read<TestDocument>(serialized);

      expect(documents).toHaveLength(3);
      expect(documents[0].name).toBe('Alice');
      expect(documents[0].age).toBe(25);
      expect(documents[1].name).toBe('Bob');
      expect(documents[1].age).toBeUndefined(); // null becomes undefined (omitted)
      expect(documents[2].name).toBeUndefined();
      expect(documents[2].score).toBe(88.5);
    });

    it('should preserve document order', () => {
      const docs: TestDocument[] = [
        { _id: 'doc4', _seq: 4, _op: 'i' },
        { _id: 'doc2', _seq: 2, _op: 'i' },
        { _id: 'doc1', _seq: 1, _op: 'i' },
        { _id: 'doc3', _seq: 3, _op: 'i' },
      ];

      const serialized = serializer.serialize(docs);
      const documents = reader.read<TestDocument>(serialized);

      expect(documents).toHaveLength(4);
      expect(documents[0]._id).toBe('doc4');
      expect(documents[1]._id).toBe('doc2');
      expect(documents[2]._id).toBe('doc1');
      expect(documents[3]._id).toBe('doc3');
    });
  });

  describe('Variant type handling', () => {
    it('should read array fields as variant', () => {
      const doc = {
        _id: 'doc1',
        _seq: 1,
        _op: 'i' as const,
        tags: ['a', 'b', 'c'],
      };

      const serialized = serializer.serialize([doc]);
      const documents = reader.read<typeof doc>(serialized);

      expect(documents).toHaveLength(1);
      expect(documents[0].tags).toEqual(['a', 'b', 'c']);
    });

    it('should read nested object fields as variant', () => {
      const doc = {
        _id: 'doc1',
        _seq: 1,
        _op: 'i' as const,
        metadata: { key: 'value', nested: { deep: true } },
      };

      const serialized = serializer.serialize([doc]);
      const documents = reader.read<typeof doc>(serialized);

      expect(documents).toHaveLength(1);
      expect(documents[0].metadata).toEqual({ key: 'value', nested: { deep: true } });
    });
  });

  describe('Compression support', () => {
    it('should read snappy-compressed data', () => {
      const snappySerializer = new RowGroupSerializer({ compression: 'snappy' });
      const docs: TestDocument[] = Array.from({ length: 100 }, (_, i) => ({
        _id: `doc${i}`,
        _seq: i + 1,
        _op: 'i' as const,
        name: 'This is a repeated string that should compress well',
      }));

      const serialized = snappySerializer.serialize(docs);
      expect(serialized.compression).toBe('snappy');

      const documents = reader.read<TestDocument>(serialized);

      expect(documents).toHaveLength(100);
      expect(documents[0]._id).toBe('doc0');
      expect(documents[0].name).toBe('This is a repeated string that should compress well');
      expect(documents[99]._id).toBe('doc99');
    });

    it('should read zstd-compressed data', () => {
      const zstdSerializer = new RowGroupSerializer({ compression: 'zstd' });
      const docs: TestDocument[] = Array.from({ length: 100 }, (_, i) => ({
        _id: `doc${i}`,
        _seq: i + 1,
        _op: 'i' as const,
        name: 'This is a repeated string that should compress well',
      }));

      const serialized = zstdSerializer.serialize(docs);
      expect(serialized.compression).toBe('zstd');

      const documents = reader.read<TestDocument>(serialized);

      expect(documents).toHaveLength(100);
      expect(documents[0]._id).toBe('doc0');
      expect(documents[99]._id).toBe('doc99');
    });
  });

  describe('Binary data handling', () => {
    it('should read binary (Uint8Array) fields', () => {
      const binaryData = new Uint8Array([1, 2, 3, 4, 5]);
      const doc = {
        _id: 'doc1',
        _seq: 1,
        _op: 'i' as const,
        data: binaryData,
      };

      const serialized = serializer.serialize([doc]);
      const documents = reader.read<typeof doc>(serialized);

      expect(documents).toHaveLength(1);
      expect(documents[0].data).toBeInstanceOf(Uint8Array);
      expect(documents[0].data).toEqual(binaryData);
    });
  });

  describe('Large dataset handling', () => {
    it('should read large document batches efficiently', () => {
      const docs: TestDocument[] = Array.from({ length: 1000 }, (_, i) => ({
        _id: `doc${i}`,
        _seq: i + 1,
        _op: 'i' as const,
        name: `User ${i}`,
        age: 20 + (i % 50),
        score: Math.random() * 100,
      }));

      const serialized = serializer.serialize(docs);

      const startTime = performance.now();
      const documents = reader.read<TestDocument>(serialized);
      const elapsedTime = performance.now() - startTime;

      expect(documents).toHaveLength(1000);
      expect(elapsedTime).toBeLessThan(2000); // Should complete within 2 seconds
    });
  });

  describe('Validation', () => {
    it('should validate PAR1 magic bytes by default', () => {
      const doc: TestDocument = {
        _id: 'doc1',
        _seq: 1,
        _op: 'i',
      };

      const serialized = serializer.serialize([doc]);

      // Corrupt the magic bytes
      const corruptData = new Uint8Array(serialized.data);
      corruptData[0] = 0x00;
      serialized.data = corruptData;

      expect(() => reader.read(serialized)).toThrow('Invalid Parquet file');
    });

    it('should skip magic validation when disabled', () => {
      const permissiveReader = new RowGroupReader({ validateMagic: false });
      const doc: TestDocument = {
        _id: 'doc1',
        _seq: 1,
        _op: 'i',
      };

      const serialized = serializer.serialize([doc]);

      // Corrupt the magic bytes
      const corruptData = new Uint8Array(serialized.data);
      corruptData[0] = 0x00;
      serialized.data = corruptData;

      // Should not throw
      const documents = permissiveReader.read(serialized);
      expect(documents).toHaveLength(1);
    });
  });

  describe('validateRowGroup utility', () => {
    it('should return true for valid row groups', () => {
      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i', name: 'Alice' },
        { _id: 'doc2', _seq: 2, _op: 'i', name: 'Bob' },
      ];

      const serialized = serializer.serialize(docs);
      expect(validateRowGroup(serialized)).toBe(true);
    });

    it('should return true for empty row groups', () => {
      const serialized = serializer.serialize([]);
      expect(validateRowGroup(serialized)).toBe(true);
    });
  });

  describe('readFromBinary', () => {
    it('should read from raw binary data with metadata', () => {
      const docs: TestDocument[] = [
        { _id: 'doc1', _seq: 1, _op: 'i', name: 'Alice' },
        { _id: 'doc2', _seq: 2, _op: 'i', name: 'Bob' },
      ];

      const serialized = serializer.serialize(docs);
      const documents = reader.readFromBinary<TestDocument>(
        serialized.data,
        serialized.columnChunks,
        serialized.rowCount
      );

      expect(documents).toHaveLength(2);
      expect(documents[0]._id).toBe('doc1');
      expect(documents[1]._id).toBe('doc2');
    });
  });
});
