/**
 * Streaming Parquet Writer Tests (TDD RED Phase)
 *
 * Tests for the streaming Parquet writer that enables writing 500MB+ files
 * with only 128MB Worker memory through row group batching and multipart upload.
 *
 * Key capabilities:
 * - Write documents exceeding memory limit via streaming
 * - Flush row groups at configured threshold (~64MB per group)
 * - Stream row groups to R2 via multipart upload
 * - Complete multipart upload on close
 * - Abort multipart upload on error
 * - Generate valid Parquet footer
 * - Support field promotion to native columns
 * - Encode remaining fields as variant
 * - Track field statistics (min/max/null count)
 * - Handle nested document structures
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  StreamingParquetWriter,
  type StreamingWriterOptions,
  type RowGroupConfig,
  type FieldPromotion,
  type WriterStatistics,
  type StreamingWriterState,
} from '../../../src/parquet/streaming-writer.js';
import {
  MemoryStorage,
  type StorageBackend,
  type MultipartUpload,
  type UploadedPart,
} from '../../../src/storage/index.js';
import type { Document, CollectionSchema } from '../../../src/types.js';

// ============================================================================
// Mock Storage for Testing
// ============================================================================

class MockMultipartUpload implements MultipartUpload {
  public parts: Map<number, Uint8Array> = new Map();
  public completed = false;
  public aborted = false;
  public completedParts: UploadedPart[] = [];

  async uploadPart(partNumber: number, data: Uint8Array): Promise<UploadedPart> {
    this.parts.set(partNumber, new Uint8Array(data));
    return { partNumber, etag: `etag-${partNumber}` };
  }

  async complete(parts: UploadedPart[]): Promise<void> {
    this.completed = true;
    this.completedParts = parts;
  }

  async abort(): Promise<void> {
    this.aborted = true;
    this.parts.clear();
  }

  getTotalSize(): number {
    let total = 0;
    for (const part of this.parts.values()) {
      total += part.byteLength;
    }
    return total;
  }
}

class MockStorage implements StorageBackend {
  public data: Map<string, Uint8Array> = new Map();
  public multipartUploads: Map<string, MockMultipartUpload> = new Map();
  public createMultipartUploadCalls: string[] = [];

  async get(key: string): Promise<Uint8Array | null> {
    return this.data.get(key) || null;
  }

  async put(key: string, data: Uint8Array): Promise<void> {
    this.data.set(key, data);
  }

  async delete(key: string): Promise<void> {
    this.data.delete(key);
  }

  async list(prefix: string): Promise<string[]> {
    return Array.from(this.data.keys()).filter((k) => k.startsWith(prefix));
  }

  async exists(key: string): Promise<boolean> {
    return this.data.has(key);
  }

  async head(key: string): Promise<{ size: number } | null> {
    const data = this.data.get(key);
    if (!data) return null;
    return { size: data.length };
  }

  async createMultipartUpload(key: string): Promise<MultipartUpload> {
    this.createMultipartUploadCalls.push(key);
    const upload = new MockMultipartUpload();
    this.multipartUploads.set(key, upload);
    return upload;
  }

  getMultipartUpload(key: string): MockMultipartUpload | undefined {
    return this.multipartUploads.get(key);
  }
}

// ============================================================================
// Test Helpers
// ============================================================================

function createTestDocument(index: number, sizeBytes?: number): Document {
  const doc: Document = {
    _id: `doc-${index.toString().padStart(8, '0')}`,
    name: `User ${index}`,
    age: 20 + (index % 50),
    email: `user${index}@example.com`,
    active: index % 2 === 0,
    score: Math.random() * 100,
    createdAt: new Date(`2024-01-${(index % 28) + 1}`),
    tags: ['tag1', 'tag2', 'tag3'].slice(0, (index % 3) + 1),
    metadata: {
      source: 'test',
      version: index % 10,
    },
  };

  // Optionally pad to approximate size
  if (sizeBytes && sizeBytes > 0) {
    const currentSize = JSON.stringify(doc).length;
    if (sizeBytes > currentSize) {
      doc.padding = 'x'.repeat(sizeBytes - currentSize);
    }
  }

  return doc;
}

function createLargeDocument(index: number, approximateSizeKB: number): Document {
  const doc: Document = {
    _id: `large-doc-${index}`,
    name: `Large Document ${index}`,
    content: 'x'.repeat(approximateSizeKB * 1024),
    metadata: {
      size: approximateSizeKB,
      index,
    },
  };
  return doc;
}

function createNestedDocument(depth: number, breadth: number): Document {
  function createNested(currentDepth: number): Record<string, unknown> {
    if (currentDepth === 0) {
      return { value: `leaf-${currentDepth}` };
    }
    const result: Record<string, unknown> = {};
    for (let i = 0; i < breadth; i++) {
      result[`child_${i}`] = createNested(currentDepth - 1);
    }
    return result;
  }

  return {
    _id: `nested-${depth}-${breadth}`,
    structure: createNested(depth),
    timestamp: new Date(),
  };
}

// ============================================================================
// Writer Initialization
// ============================================================================

describe('StreamingParquetWriter', () => {
  let storage: MockStorage;
  let writer: StreamingParquetWriter;

  beforeEach(() => {
    storage = new MockStorage();
  });

  afterEach(async () => {
    // Ensure writer is closed if still open
    if (writer && writer.getState() !== 'closed') {
      try {
        await writer.abort();
      } catch {
        // Ignore cleanup errors
      }
    }
  });

  describe('Initialization', () => {
    it('should create a writer with default options', () => {
      writer = new StreamingParquetWriter(storage, 'test/output.parquet');

      expect(writer).toBeDefined();
      expect(writer.getState()).toBe('initialized');
    });

    it('should create a writer with custom row group size', () => {
      const options: StreamingWriterOptions = {
        rowGroupSizeBytes: 32 * 1024 * 1024, // 32MB
      };
      writer = new StreamingParquetWriter(storage, 'test/output.parquet', options);

      expect(writer).toBeDefined();
      expect(writer.getRowGroupConfig().targetSizeBytes).toBe(32 * 1024 * 1024);
    });

    it('should create a writer with field promotion configuration', () => {
      const options: StreamingWriterOptions = {
        fieldPromotions: {
          name: 'string',
          age: 'int32',
          score: 'double',
          active: 'boolean',
          createdAt: 'timestamp',
        },
      };
      writer = new StreamingParquetWriter(storage, 'test/output.parquet', options);

      expect(writer).toBeDefined();
      expect(writer.getFieldPromotions()).toEqual(options.fieldPromotions);
    });

    it('should initialize multipart upload on first write', async () => {
      writer = new StreamingParquetWriter(storage, 'test/output.parquet');

      await writer.write(createTestDocument(1));

      expect(storage.createMultipartUploadCalls).toContain('test/output.parquet');
      expect(writer.getState()).toBe('writing');
    });

    it('should reject invalid row group size', () => {
      expect(() => {
        new StreamingParquetWriter(storage, 'test/output.parquet', {
          rowGroupSizeBytes: 0,
        });
      }).toThrow();
    });

    it('should reject negative row group size', () => {
      expect(() => {
        new StreamingParquetWriter(storage, 'test/output.parquet', {
          rowGroupSizeBytes: -1,
        });
      }).toThrow();
    });
  });

  // ============================================================================
  // Writing Documents Exceeding Memory Limit
  // ============================================================================

  describe('Write documents exceeding memory limit', () => {
    it('should write many small documents without running out of memory', async () => {
      writer = new StreamingParquetWriter(storage, 'test/large.parquet', {
        rowGroupSizeBytes: 1 * 1024 * 1024, // 1MB row groups for testing
      });

      // Write 10,000 documents (simulating large dataset)
      for (let i = 0; i < 10000; i++) {
        await writer.write(createTestDocument(i));
      }
      await writer.close();

      const stats = writer.getStatistics();
      expect(stats.totalDocuments).toBe(10000);
      expect(stats.rowGroupsWritten).toBeGreaterThan(1);
    });

    it('should handle documents larger than row group threshold', async () => {
      writer = new StreamingParquetWriter(storage, 'test/large-docs.parquet', {
        rowGroupSizeBytes: 100 * 1024, // 100KB row groups
      });

      // Write documents that exceed row group size individually
      await writer.write(createLargeDocument(1, 200)); // 200KB document
      await writer.write(createLargeDocument(2, 200));
      await writer.close();

      const stats = writer.getStatistics();
      expect(stats.totalDocuments).toBe(2);
      // Each large doc should trigger its own row group
      expect(stats.rowGroupsWritten).toBeGreaterThanOrEqual(2);
    });

    it('should not buffer entire file in memory', async () => {
      writer = new StreamingParquetWriter(storage, 'test/memory-test.parquet', {
        rowGroupSizeBytes: 1 * 1024 * 1024, // 1MB
      });

      // Write enough to trigger multiple flushes
      for (let i = 0; i < 100; i++) {
        await writer.write(createLargeDocument(i, 50)); // 50KB each = 5MB total
      }
      await writer.close();

      // Current memory usage should be bounded by row group size
      const stats = writer.getStatistics();
      expect(stats.peakMemoryUsageBytes).toBeLessThan(2 * 1024 * 1024); // < 2x row group size
    });

    it('should stream documents from async iterator', async () => {
      writer = new StreamingParquetWriter(storage, 'test/iterator.parquet', {
        rowGroupSizeBytes: 1 * 1024 * 1024,
      });

      async function* generateDocuments(): AsyncGenerator<Document> {
        for (let i = 0; i < 1000; i++) {
          yield createTestDocument(i);
        }
      }

      await writer.writeAll(generateDocuments());
      await writer.close();

      const stats = writer.getStatistics();
      expect(stats.totalDocuments).toBe(1000);
    });
  });

  // ============================================================================
  // Row Group Flushing at Configured Threshold
  // ============================================================================

  describe('Flush row groups at configured threshold', () => {
    it('should flush row group when size threshold is reached', async () => {
      writer = new StreamingParquetWriter(storage, 'test/flush-test.parquet', {
        rowGroupSizeBytes: 100 * 1024, // 100KB
      });

      // Write documents until we exceed threshold
      for (let i = 0; i < 50; i++) {
        await writer.write(createLargeDocument(i, 10)); // 10KB each
      }

      const stats = writer.getStatistics();
      expect(stats.rowGroupsFlushed).toBeGreaterThan(0);
    });

    it('should respect custom row group row count limit', async () => {
      writer = new StreamingParquetWriter(storage, 'test/row-limit.parquet', {
        rowGroupSizeBytes: 100 * 1024 * 1024, // Large size limit
        maxRowsPerRowGroup: 100, // But small row limit
      });

      // Write 250 small documents
      for (let i = 0; i < 250; i++) {
        await writer.write(createTestDocument(i));
      }
      await writer.close();

      const stats = writer.getStatistics();
      // Should have at least 2 row groups (250 / 100 = 2.5)
      expect(stats.rowGroupsWritten).toBeGreaterThanOrEqual(2);
    });

    it('should track current row group size accurately', async () => {
      writer = new StreamingParquetWriter(storage, 'test/size-tracking.parquet', {
        rowGroupSizeBytes: 1 * 1024 * 1024,
      });

      await writer.write(createTestDocument(1));
      const sizeAfterOne = writer.getCurrentRowGroupSize();
      expect(sizeAfterOne).toBeGreaterThan(0);

      await writer.write(createTestDocument(2));
      const sizeAfterTwo = writer.getCurrentRowGroupSize();
      expect(sizeAfterTwo).toBeGreaterThan(sizeAfterOne);

      await writer.close();
    });

    it('should emit event when row group is flushed', async () => {
      writer = new StreamingParquetWriter(storage, 'test/flush-events.parquet', {
        rowGroupSizeBytes: 50 * 1024, // 50KB
      });

      const flushEvents: number[] = [];
      writer.on('rowGroupFlushed', (rowGroupIndex: number) => {
        flushEvents.push(rowGroupIndex);
      });

      for (let i = 0; i < 30; i++) {
        await writer.write(createLargeDocument(i, 10)); // 10KB each
      }
      await writer.close();

      expect(flushEvents.length).toBeGreaterThan(0);
      expect(flushEvents[0]).toBe(0); // First row group index
    });

    it('should allow manual flush before threshold', async () => {
      writer = new StreamingParquetWriter(storage, 'test/manual-flush.parquet', {
        rowGroupSizeBytes: 100 * 1024 * 1024, // Very large threshold
      });

      await writer.write(createTestDocument(1));
      await writer.write(createTestDocument(2));

      await writer.flushRowGroup();

      expect(writer.getCurrentRowGroupSize()).toBe(0);

      const stats = writer.getStatistics();
      expect(stats.rowGroupsFlushed).toBe(1);
    });
  });

  // ============================================================================
  // Stream Row Groups to R2 Multipart Upload
  // ============================================================================

  describe('Stream row groups to R2 multipart upload', () => {
    it('should upload each row group as a multipart part', async () => {
      writer = new StreamingParquetWriter(storage, 'test/multipart.parquet', {
        rowGroupSizeBytes: 50 * 1024, // 50KB
      });

      for (let i = 0; i < 30; i++) {
        await writer.write(createLargeDocument(i, 10)); // 10KB each
      }
      await writer.close();

      const upload = storage.getMultipartUpload('test/multipart.parquet');
      expect(upload).toBeDefined();
      expect(upload!.parts.size).toBeGreaterThan(1);
    });

    it('should upload parts with sequential part numbers', async () => {
      writer = new StreamingParquetWriter(storage, 'test/sequential-parts.parquet', {
        rowGroupSizeBytes: 50 * 1024,
      });

      for (let i = 0; i < 30; i++) {
        await writer.write(createLargeDocument(i, 10));
      }
      await writer.close();

      const upload = storage.getMultipartUpload('test/sequential-parts.parquet');
      const partNumbers = Array.from(upload!.parts.keys()).sort((a, b) => a - b);

      // Part numbers should be sequential starting from 1
      for (let i = 0; i < partNumbers.length; i++) {
        expect(partNumbers[i]).toBe(i + 1);
      }
    });

    it('should track bytes uploaded during streaming', async () => {
      writer = new StreamingParquetWriter(storage, 'test/bytes-tracked.parquet', {
        rowGroupSizeBytes: 50 * 1024,
      });

      for (let i = 0; i < 20; i++) {
        await writer.write(createLargeDocument(i, 10));
      }

      const bytesBeforeClose = writer.getStatistics().bytesUploaded;
      expect(bytesBeforeClose).toBeGreaterThan(0);

      await writer.close();

      const bytesAfterClose = writer.getStatistics().bytesUploaded;
      expect(bytesAfterClose).toBeGreaterThan(bytesBeforeClose);
    });

    it('should handle concurrent writes correctly', async () => {
      writer = new StreamingParquetWriter(storage, 'test/concurrent.parquet', {
        rowGroupSizeBytes: 100 * 1024,
      });

      // Queue up multiple writes (they should be serialized internally)
      const writes = [];
      for (let i = 0; i < 100; i++) {
        writes.push(writer.write(createTestDocument(i)));
      }
      await Promise.all(writes);
      await writer.close();

      const stats = writer.getStatistics();
      expect(stats.totalDocuments).toBe(100);
    });
  });

  // ============================================================================
  // Complete Multipart Upload on Close
  // ============================================================================

  describe('Complete multipart upload on close', () => {
    it('should complete multipart upload when writer is closed', async () => {
      writer = new StreamingParquetWriter(storage, 'test/complete.parquet', {
        rowGroupSizeBytes: 50 * 1024,
      });

      for (let i = 0; i < 10; i++) {
        await writer.write(createLargeDocument(i, 10));
      }
      await writer.close();

      const upload = storage.getMultipartUpload('test/complete.parquet');
      expect(upload).toBeDefined();
      expect(upload!.completed).toBe(true);
      expect(upload!.aborted).toBe(false);
    });

    it('should complete with all parts in order', async () => {
      writer = new StreamingParquetWriter(storage, 'test/parts-order.parquet', {
        rowGroupSizeBytes: 50 * 1024,
      });

      for (let i = 0; i < 20; i++) {
        await writer.write(createLargeDocument(i, 10));
      }
      await writer.close();

      const upload = storage.getMultipartUpload('test/parts-order.parquet');
      const completedPartNumbers = upload!.completedParts.map((p) => p.partNumber);

      // Parts should be in ascending order
      for (let i = 1; i < completedPartNumbers.length; i++) {
        expect(completedPartNumbers[i]).toBeGreaterThan(completedPartNumbers[i - 1]);
      }
    });

    it('should flush remaining buffered data before completing', async () => {
      writer = new StreamingParquetWriter(storage, 'test/flush-before-complete.parquet', {
        rowGroupSizeBytes: 100 * 1024 * 1024, // Very large threshold
      });

      // Write data that won't trigger automatic flush
      for (let i = 0; i < 10; i++) {
        await writer.write(createTestDocument(i));
      }

      expect(writer.getCurrentRowGroupSize()).toBeGreaterThan(0);

      await writer.close();

      // Data should have been flushed
      const upload = storage.getMultipartUpload('test/flush-before-complete.parquet');
      expect(upload!.completed).toBe(true);
      expect(upload!.getTotalSize()).toBeGreaterThan(0);
    });

    it('should not allow writes after close', async () => {
      writer = new StreamingParquetWriter(storage, 'test/no-write-after-close.parquet');

      await writer.write(createTestDocument(1));
      await writer.close();

      await expect(writer.write(createTestDocument(2))).rejects.toThrow();
    });

    it('should allow multiple close calls (idempotent)', async () => {
      writer = new StreamingParquetWriter(storage, 'test/idempotent-close.parquet');

      await writer.write(createTestDocument(1));
      await writer.close();

      // Second close should not throw
      await expect(writer.close()).resolves.not.toThrow();
    });

    it('should set state to closed after completion', async () => {
      writer = new StreamingParquetWriter(storage, 'test/state-closed.parquet');

      await writer.write(createTestDocument(1));
      await writer.close();

      expect(writer.getState()).toBe('closed');
    });
  });

  // ============================================================================
  // Abort Multipart Upload on Error
  // ============================================================================

  describe('Abort multipart upload on error', () => {
    it('should abort upload when abort() is called', async () => {
      writer = new StreamingParquetWriter(storage, 'test/abort.parquet', {
        rowGroupSizeBytes: 50 * 1024,
      });

      for (let i = 0; i < 10; i++) {
        await writer.write(createLargeDocument(i, 10));
      }
      await writer.abort();

      const upload = storage.getMultipartUpload('test/abort.parquet');
      expect(upload).toBeDefined();
      expect(upload!.aborted).toBe(true);
      expect(upload!.completed).toBe(false);
    });

    it('should abort upload on write error', async () => {
      const failingStorage = new MockStorage();
      let writeCount = 0;
      const originalUploadPart = MockMultipartUpload.prototype.uploadPart;
      MockMultipartUpload.prototype.uploadPart = async function (
        partNumber: number,
        data: Uint8Array
      ) {
        writeCount++;
        if (writeCount > 2) {
          throw new Error('Simulated upload failure');
        }
        return originalUploadPart.call(this, partNumber, data);
      };

      writer = new StreamingParquetWriter(failingStorage, 'test/write-error.parquet', {
        rowGroupSizeBytes: 10 * 1024, // Small threshold to trigger flushes
      });

      try {
        for (let i = 0; i < 100; i++) {
          await writer.write(createLargeDocument(i, 5));
        }
        await writer.close();
        // Should not reach here
        expect(true).toBe(false);
      } catch (error) {
        expect((error as Error).message).toContain('upload failure');
      } finally {
        // Restore original method
        MockMultipartUpload.prototype.uploadPart = originalUploadPart;
      }

      const upload = failingStorage.getMultipartUpload('test/write-error.parquet');
      expect(upload!.aborted).toBe(true);
    });

    it('should clean up resources on abort', async () => {
      writer = new StreamingParquetWriter(storage, 'test/cleanup.parquet');

      await writer.write(createTestDocument(1));
      await writer.write(createTestDocument(2));
      await writer.abort();

      expect(writer.getState()).toBe('aborted');
      expect(writer.getCurrentRowGroupSize()).toBe(0);
    });

    it('should not allow writes after abort', async () => {
      writer = new StreamingParquetWriter(storage, 'test/no-write-after-abort.parquet');

      await writer.write(createTestDocument(1));
      await writer.abort();

      await expect(writer.write(createTestDocument(2))).rejects.toThrow();
    });

    it('should set state to aborted after abort', async () => {
      writer = new StreamingParquetWriter(storage, 'test/state-aborted.parquet');

      await writer.write(createTestDocument(1));
      await writer.abort();

      expect(writer.getState()).toBe('aborted');
    });
  });

  // ============================================================================
  // Generate Valid Parquet Footer
  // ============================================================================

  describe('Generate valid Parquet footer', () => {
    it('should write PAR1 magic bytes at start and end', async () => {
      writer = new StreamingParquetWriter(storage, 'test/magic-bytes.parquet');

      await writer.write(createTestDocument(1));
      await writer.close();

      const upload = storage.getMultipartUpload('test/magic-bytes.parquet');
      const allData = concatenateParts(upload!);

      // Check start magic
      const startMagic = new TextDecoder().decode(allData.slice(0, 4));
      expect(startMagic).toBe('PAR1');

      // Check end magic
      const endMagic = new TextDecoder().decode(allData.slice(-4));
      expect(endMagic).toBe('PAR1');
    });

    it('should include file metadata in footer', async () => {
      writer = new StreamingParquetWriter(storage, 'test/metadata.parquet');

      for (let i = 0; i < 100; i++) {
        await writer.write(createTestDocument(i));
      }
      await writer.close();

      const footer = writer.getFooter();
      expect(footer).toBeDefined();
      expect(footer!.numRows).toBe(100);
      expect(footer!.rowGroups.length).toBeGreaterThan(0);
    });

    it('should include schema in footer', async () => {
      writer = new StreamingParquetWriter(storage, 'test/schema-footer.parquet', {
        fieldPromotions: {
          name: 'string',
          age: 'int32',
        },
      });

      await writer.write(createTestDocument(1));
      await writer.close();

      const footer = writer.getFooter();
      expect(footer!.schema).toBeDefined();

      const schemaFields = footer!.schema.map((s: { name: string }) => s.name);
      expect(schemaFields).toContain('name');
      expect(schemaFields).toContain('age');
      expect(schemaFields).toContain('_data'); // Variant column
    });

    it('should include row group metadata in footer', async () => {
      writer = new StreamingParquetWriter(storage, 'test/row-group-meta.parquet', {
        rowGroupSizeBytes: 50 * 1024,
      });

      for (let i = 0; i < 30; i++) {
        await writer.write(createLargeDocument(i, 10));
      }
      await writer.close();

      const footer = writer.getFooter();
      expect(footer!.rowGroups.length).toBeGreaterThan(1);

      for (const rowGroup of footer!.rowGroups) {
        expect(rowGroup.numRows).toBeGreaterThan(0);
        expect(rowGroup.totalByteSize).toBeGreaterThan(0);
        expect(rowGroup.columns).toBeDefined();
        expect(rowGroup.columns.length).toBeGreaterThan(0);
      }
    });

    it('should include column chunk offsets in footer', async () => {
      writer = new StreamingParquetWriter(storage, 'test/column-offsets.parquet', {
        fieldPromotions: {
          name: 'string',
          age: 'int32',
        },
      });

      for (let i = 0; i < 10; i++) {
        await writer.write(createTestDocument(i));
      }
      await writer.close();

      const footer = writer.getFooter();
      for (const rowGroup of footer!.rowGroups) {
        for (const column of rowGroup.columns) {
          expect(column.fileOffset).toBeDefined();
          expect(column.fileOffset).toBeGreaterThanOrEqual(4); // After magic bytes
          expect(column.compressedSize).toBeGreaterThan(0);
        }
      }
    });

    it('should include key-value metadata', async () => {
      writer = new StreamingParquetWriter(storage, 'test/kv-metadata.parquet', {
        metadata: {
          'created_by': 'mongolake',
          'mongolake.version': '0.1.0',
          'mongolake.collection': 'users',
        },
      });

      await writer.write(createTestDocument(1));
      await writer.close();

      const footer = writer.getFooter();
      expect(footer!.keyValueMetadata).toBeDefined();
      expect(footer!.keyValueMetadata.get('created_by')).toBe('mongolake');
      expect(footer!.keyValueMetadata.get('mongolake.version')).toBe('0.1.0');
    });

    it('should write footer length before final magic bytes', async () => {
      writer = new StreamingParquetWriter(storage, 'test/footer-length.parquet');

      await writer.write(createTestDocument(1));
      await writer.close();

      const upload = storage.getMultipartUpload('test/footer-length.parquet');
      const allData = concatenateParts(upload!);

      // Last 4 bytes are PAR1
      // 4 bytes before that are footer length (little-endian int32)
      const footerLengthBytes = allData.slice(-8, -4);
      const footerLength = new DataView(footerLengthBytes.buffer).getInt32(0, true);

      expect(footerLength).toBeGreaterThan(0);
    });
  });

  // ============================================================================
  // Support Field Promotion to Native Columns
  // ============================================================================

  describe('Support field promotion to native columns', () => {
    it('should promote string fields to string columns', async () => {
      writer = new StreamingParquetWriter(storage, 'test/promote-string.parquet', {
        fieldPromotions: {
          name: 'string',
        },
      });

      await writer.write({ _id: '1', name: 'Alice' });
      await writer.write({ _id: '2', name: 'Bob' });
      await writer.close();

      const footer = writer.getFooter();
      const nameColumn = footer!.schema.find((s: { name: string }) => s.name === 'name');
      expect(nameColumn).toBeDefined();
      expect(nameColumn!.type).toBe('BYTE_ARRAY');
    });

    it('should promote integer fields to int32/int64 columns', async () => {
      writer = new StreamingParquetWriter(storage, 'test/promote-int.parquet', {
        fieldPromotions: {
          age: 'int32',
          bigNumber: 'int64',
        },
      });

      await writer.write({ _id: '1', age: 25, bigNumber: 9007199254740993n });
      await writer.close();

      const footer = writer.getFooter();
      const ageColumn = footer!.schema.find((s: { name: string }) => s.name === 'age');
      expect(ageColumn!.type).toBe('INT32');

      const bigNumColumn = footer!.schema.find((s: { name: string }) => s.name === 'bigNumber');
      expect(bigNumColumn!.type).toBe('INT64');
    });

    it('should promote float/double fields to float/double columns', async () => {
      writer = new StreamingParquetWriter(storage, 'test/promote-float.parquet', {
        fieldPromotions: {
          temperature: 'float',
          preciseValue: 'double',
        },
      });

      await writer.write({ _id: '1', temperature: 98.6, preciseValue: 3.141592653589793 });
      await writer.close();

      const footer = writer.getFooter();
      const tempColumn = footer!.schema.find((s: { name: string }) => s.name === 'temperature');
      expect(tempColumn!.type).toBe('FLOAT');

      const preciseColumn = footer!.schema.find(
        (s: { name: string }) => s.name === 'preciseValue'
      );
      expect(preciseColumn!.type).toBe('DOUBLE');
    });

    it('should promote boolean fields to boolean columns', async () => {
      writer = new StreamingParquetWriter(storage, 'test/promote-bool.parquet', {
        fieldPromotions: {
          active: 'boolean',
        },
      });

      await writer.write({ _id: '1', active: true });
      await writer.write({ _id: '2', active: false });
      await writer.close();

      const footer = writer.getFooter();
      const activeColumn = footer!.schema.find((s: { name: string }) => s.name === 'active');
      expect(activeColumn!.type).toBe('BOOLEAN');
    });

    it('should promote timestamp fields to timestamp columns', async () => {
      writer = new StreamingParquetWriter(storage, 'test/promote-timestamp.parquet', {
        fieldPromotions: {
          createdAt: 'timestamp',
        },
      });

      await writer.write({ _id: '1', createdAt: new Date('2024-01-15T12:00:00Z') });
      await writer.close();

      const footer = writer.getFooter();
      const createdAtColumn = footer!.schema.find(
        (s: { name: string }) => s.name === 'createdAt'
      );
      expect(createdAtColumn!.type).toBe('INT64');
      expect(createdAtColumn!.logicalType?.type).toBe('TIMESTAMP');
    });

    it('should promote binary fields to binary columns', async () => {
      writer = new StreamingParquetWriter(storage, 'test/promote-binary.parquet', {
        fieldPromotions: {
          data: 'binary',
        },
      });

      await writer.write({ _id: '1', data: new Uint8Array([1, 2, 3, 4]) });
      await writer.close();

      const footer = writer.getFooter();
      const dataColumn = footer!.schema.find((s: { name: string }) => s.name === 'data');
      expect(dataColumn!.type).toBe('BYTE_ARRAY');
    });

    it('should handle null values in promoted columns', async () => {
      writer = new StreamingParquetWriter(storage, 'test/promote-nulls.parquet', {
        fieldPromotions: {
          name: 'string',
          age: 'int32',
        },
      });

      await writer.write({ _id: '1', name: 'Alice', age: 25 });
      await writer.write({ _id: '2', name: null, age: null });
      await writer.write({ _id: '3', name: 'Charlie' }); // age missing
      await writer.close();

      const footer = writer.getFooter();
      const nameColumn = footer!.rowGroups[0].columns.find(
        (c: { path: string }) => c.path === 'name'
      );
      expect(nameColumn!.statistics.nullCount).toBe(1);

      const ageColumn = footer!.rowGroups[0].columns.find(
        (c: { path: string }) => c.path === 'age'
      );
      expect(ageColumn!.statistics.nullCount).toBe(2); // null + missing
    });

    it('should support nested field promotion with dot notation', async () => {
      writer = new StreamingParquetWriter(storage, 'test/promote-nested.parquet', {
        fieldPromotions: {
          'address.city': 'string',
          'address.zip': 'string',
        },
      });

      await writer.write({
        _id: '1',
        address: {
          city: 'New York',
          zip: '10001',
          street: '123 Main St',
        },
      });
      await writer.close();

      const footer = writer.getFooter();
      const cityColumn = footer!.schema.find(
        (s: { name: string }) => s.name === 'address.city'
      );
      expect(cityColumn).toBeDefined();
    });
  });

  // ============================================================================
  // Encode Remaining Fields as Variant
  // ============================================================================

  describe('Encode remaining fields as variant', () => {
    it('should encode non-promoted fields as variant', async () => {
      writer = new StreamingParquetWriter(storage, 'test/variant-remaining.parquet', {
        fieldPromotions: {
          name: 'string',
        },
      });

      await writer.write({
        _id: '1',
        name: 'Alice',
        age: 25,
        tags: ['admin', 'user'],
        metadata: { source: 'api' },
      });
      await writer.close();

      const footer = writer.getFooter();
      const variantColumn = footer!.schema.find((s: { name: string }) => s.name === '_data');
      expect(variantColumn).toBeDefined();
      expect(variantColumn!.type).toBe('BYTE_ARRAY');
      expect(variantColumn!.logicalType?.type).toBe('VARIANT');
    });

    it('should encode entire document as variant when no promotions configured', async () => {
      writer = new StreamingParquetWriter(storage, 'test/variant-only.parquet', {
        // No field promotions
      });

      await writer.write({
        _id: '1',
        name: 'Alice',
        age: 25,
        nested: { deep: { value: 42 } },
      });
      await writer.close();

      const footer = writer.getFooter();
      // Should only have _id and _data columns
      const columnNames = footer!.schema.map((s: { name: string }) => s.name);
      expect(columnNames).toContain('_id');
      expect(columnNames).toContain('_data');
      expect(columnNames).not.toContain('name');
      expect(columnNames).not.toContain('age');
    });

    it('should exclude promoted fields from variant column', async () => {
      writer = new StreamingParquetWriter(storage, 'test/variant-exclude.parquet', {
        fieldPromotions: {
          name: 'string',
          age: 'int32',
        },
      });

      const doc = {
        _id: '1',
        name: 'Alice',
        age: 25,
        email: 'alice@example.com',
      };
      await writer.write(doc);
      await writer.close();

      // The variant data should only contain email, not name or age
      const variantData = writer.getLastVariantData();
      expect(variantData).toBeDefined();
      expect(variantData).not.toContain('Alice');
      expect(variantData).not.toContain('25');
      expect(variantData).toContain('alice@example.com');
    });

    it('should handle documents with only promoted fields', async () => {
      writer = new StreamingParquetWriter(storage, 'test/variant-empty.parquet', {
        fieldPromotions: {
          name: 'string',
          age: 'int32',
        },
      });

      // Document only has promoted fields (plus _id)
      await writer.write({ _id: '1', name: 'Alice', age: 25 });
      await writer.close();

      // Variant column should exist but be minimal
      const footer = writer.getFooter();
      const variantColumn = footer!.rowGroups[0].columns.find(
        (c: { path: string }) => c.path === '_data'
      );
      expect(variantColumn!.statistics.nullCount).toBe(1);
    });

    it('should support variant-only mode for schema flexibility', async () => {
      writer = new StreamingParquetWriter(storage, 'test/variant-flexible.parquet', {
        variantOnly: true,
      });

      // Documents with completely different schemas
      await writer.write({ _id: '1', type: 'user', name: 'Alice' });
      await writer.write({ _id: '2', type: 'event', action: 'click', target: 'button' });
      await writer.write({ _id: '3', type: 'log', level: 'info', message: 'test' });
      await writer.close();

      const footer = writer.getFooter();
      // Should only have _id and _data columns
      expect(footer!.schema.length).toBe(2);
    });
  });

  // ============================================================================
  // Track Field Statistics (min/max/null count)
  // ============================================================================

  describe('Track field statistics (min/max/null count)', () => {
    it('should track min/max for string columns', async () => {
      writer = new StreamingParquetWriter(storage, 'test/stats-string.parquet', {
        fieldPromotions: {
          name: 'string',
        },
      });

      await writer.write({ _id: '1', name: 'Charlie' });
      await writer.write({ _id: '2', name: 'Alice' });
      await writer.write({ _id: '3', name: 'Bob' });
      await writer.close();

      const footer = writer.getFooter();
      const nameColumn = footer!.rowGroups[0].columns.find(
        (c: { path: string }) => c.path === 'name'
      );
      expect(nameColumn!.statistics.minValue).toBe('Alice');
      expect(nameColumn!.statistics.maxValue).toBe('Charlie');
    });

    it('should track min/max for numeric columns', async () => {
      writer = new StreamingParquetWriter(storage, 'test/stats-numeric.parquet', {
        fieldPromotions: {
          age: 'int32',
          score: 'double',
        },
      });

      await writer.write({ _id: '1', age: 30, score: 85.5 });
      await writer.write({ _id: '2', age: 25, score: 92.0 });
      await writer.write({ _id: '3', age: 45, score: 78.3 });
      await writer.close();

      const footer = writer.getFooter();

      const ageColumn = footer!.rowGroups[0].columns.find(
        (c: { path: string }) => c.path === 'age'
      );
      expect(ageColumn!.statistics.minValue).toBe(25);
      expect(ageColumn!.statistics.maxValue).toBe(45);

      const scoreColumn = footer!.rowGroups[0].columns.find(
        (c: { path: string }) => c.path === 'score'
      );
      expect(scoreColumn!.statistics.minValue).toBeCloseTo(78.3, 1);
      expect(scoreColumn!.statistics.maxValue).toBeCloseTo(92.0, 1);
    });

    it('should track min/max for boolean columns', async () => {
      writer = new StreamingParquetWriter(storage, 'test/stats-bool.parquet', {
        fieldPromotions: {
          active: 'boolean',
        },
      });

      await writer.write({ _id: '1', active: false });
      await writer.write({ _id: '2', active: true });
      await writer.write({ _id: '3', active: false });
      await writer.close();

      const footer = writer.getFooter();
      const activeColumn = footer!.rowGroups[0].columns.find(
        (c: { path: string }) => c.path === 'active'
      );
      expect(activeColumn!.statistics.minValue).toBe(false);
      expect(activeColumn!.statistics.maxValue).toBe(true);
    });

    it('should track min/max for timestamp columns', async () => {
      writer = new StreamingParquetWriter(storage, 'test/stats-timestamp.parquet', {
        fieldPromotions: {
          createdAt: 'timestamp',
        },
      });

      const date1 = new Date('2024-01-01');
      const date2 = new Date('2024-06-15');
      const date3 = new Date('2024-03-10');

      await writer.write({ _id: '1', createdAt: date1 });
      await writer.write({ _id: '2', createdAt: date2 });
      await writer.write({ _id: '3', createdAt: date3 });
      await writer.close();

      const footer = writer.getFooter();
      const createdAtColumn = footer!.rowGroups[0].columns.find(
        (c: { path: string }) => c.path === 'createdAt'
      );
      expect(new Date(createdAtColumn!.statistics.minValue)).toEqual(date1);
      expect(new Date(createdAtColumn!.statistics.maxValue)).toEqual(date2);
    });

    it('should track null count for columns', async () => {
      writer = new StreamingParquetWriter(storage, 'test/stats-nulls.parquet', {
        fieldPromotions: {
          name: 'string',
        },
      });

      await writer.write({ _id: '1', name: 'Alice' });
      await writer.write({ _id: '2', name: null });
      await writer.write({ _id: '3' }); // name missing
      await writer.write({ _id: '4', name: 'David' });
      await writer.close();

      const footer = writer.getFooter();
      const nameColumn = footer!.rowGroups[0].columns.find(
        (c: { path: string }) => c.path === 'name'
      );
      expect(nameColumn!.statistics.nullCount).toBe(2);
    });

    it('should track distinct count for columns', async () => {
      writer = new StreamingParquetWriter(storage, 'test/stats-distinct.parquet', {
        fieldPromotions: {
          category: 'string',
        },
        trackDistinctCount: true,
      });

      await writer.write({ _id: '1', category: 'A' });
      await writer.write({ _id: '2', category: 'B' });
      await writer.write({ _id: '3', category: 'A' });
      await writer.write({ _id: '4', category: 'C' });
      await writer.write({ _id: '5', category: 'A' });
      await writer.close();

      const footer = writer.getFooter();
      const categoryColumn = footer!.rowGroups[0].columns.find(
        (c: { path: string }) => c.path === 'category'
      );
      expect(categoryColumn!.statistics.distinctCount).toBe(3);
    });

    it('should aggregate statistics across row groups', async () => {
      writer = new StreamingParquetWriter(storage, 'test/stats-aggregate.parquet', {
        fieldPromotions: {
          value: 'int32',
        },
        rowGroupSizeBytes: 1024, // Very small to force multiple row groups
      });

      for (let i = 1; i <= 100; i++) {
        await writer.write({ _id: `${i}`, value: i });
      }
      await writer.close();

      const stats = writer.getStatistics();
      expect(stats.columns.value.globalMin).toBe(1);
      expect(stats.columns.value.globalMax).toBe(100);
      expect(stats.columns.value.totalNullCount).toBe(0);
    });

    it('should exclude nulls from min/max calculations', async () => {
      writer = new StreamingParquetWriter(storage, 'test/stats-exclude-nulls.parquet', {
        fieldPromotions: {
          value: 'int32',
        },
      });

      await writer.write({ _id: '1', value: null });
      await writer.write({ _id: '2', value: 50 });
      await writer.write({ _id: '3', value: null });
      await writer.write({ _id: '4', value: 100 });
      await writer.close();

      const footer = writer.getFooter();
      const valueColumn = footer!.rowGroups[0].columns.find(
        (c: { path: string }) => c.path === 'value'
      );
      expect(valueColumn!.statistics.minValue).toBe(50);
      expect(valueColumn!.statistics.maxValue).toBe(100);
    });
  });

  // ============================================================================
  // Handle Nested Document Structures
  // ============================================================================

  describe('Handle nested document structures', () => {
    it('should handle flat documents', async () => {
      writer = new StreamingParquetWriter(storage, 'test/nested-flat.parquet');

      await writer.write({ _id: '1', name: 'Alice', age: 25 });
      await writer.close();

      const stats = writer.getStatistics();
      expect(stats.totalDocuments).toBe(1);
    });

    it('should handle single level nesting', async () => {
      writer = new StreamingParquetWriter(storage, 'test/nested-one-level.parquet');

      await writer.write({
        _id: '1',
        user: {
          name: 'Alice',
          email: 'alice@example.com',
        },
      });
      await writer.close();

      const stats = writer.getStatistics();
      expect(stats.totalDocuments).toBe(1);
    });

    it('should handle deeply nested documents', async () => {
      writer = new StreamingParquetWriter(storage, 'test/nested-deep.parquet');

      await writer.write(createNestedDocument(5, 3));
      await writer.close();

      const stats = writer.getStatistics();
      expect(stats.totalDocuments).toBe(1);
    });

    it('should handle arrays of primitives', async () => {
      writer = new StreamingParquetWriter(storage, 'test/nested-array-primitives.parquet');

      await writer.write({
        _id: '1',
        tags: ['a', 'b', 'c'],
        scores: [1, 2, 3, 4, 5],
        flags: [true, false, true],
      });
      await writer.close();

      const stats = writer.getStatistics();
      expect(stats.totalDocuments).toBe(1);
    });

    it('should handle arrays of objects', async () => {
      writer = new StreamingParquetWriter(storage, 'test/nested-array-objects.parquet');

      await writer.write({
        _id: '1',
        items: [
          { name: 'Widget', qty: 5, price: 9.99 },
          { name: 'Gadget', qty: 2, price: 19.99 },
          { name: 'Doohickey', qty: 10, price: 4.99 },
        ],
      });
      await writer.close();

      const stats = writer.getStatistics();
      expect(stats.totalDocuments).toBe(1);
    });

    it('should handle mixed nested structures', async () => {
      writer = new StreamingParquetWriter(storage, 'test/nested-mixed.parquet');

      await writer.write({
        _id: '1',
        user: {
          name: 'Alice',
          addresses: [
            { type: 'home', city: 'NYC', coords: { lat: 40.7, lng: -74.0 } },
            { type: 'work', city: 'Boston', coords: { lat: 42.3, lng: -71.0 } },
          ],
        },
        tags: ['admin', 'user'],
        metadata: {
          counts: [1, 2, 3],
          nested: {
            deep: {
              value: 42,
            },
          },
        },
      });
      await writer.close();

      const stats = writer.getStatistics();
      expect(stats.totalDocuments).toBe(1);
    });

    it('should handle empty arrays', async () => {
      writer = new StreamingParquetWriter(storage, 'test/nested-empty-array.parquet');

      await writer.write({
        _id: '1',
        tags: [],
        items: [],
      });
      await writer.close();

      const stats = writer.getStatistics();
      expect(stats.totalDocuments).toBe(1);
    });

    it('should handle empty nested objects', async () => {
      writer = new StreamingParquetWriter(storage, 'test/nested-empty-object.parquet');

      await writer.write({
        _id: '1',
        metadata: {},
        config: {},
      });
      await writer.close();

      const stats = writer.getStatistics();
      expect(stats.totalDocuments).toBe(1);
    });

    it('should handle null nested fields', async () => {
      writer = new StreamingParquetWriter(storage, 'test/nested-null.parquet');

      await writer.write({
        _id: '1',
        user: null,
        items: null,
        metadata: {
          config: null,
        },
      });
      await writer.close();

      const stats = writer.getStatistics();
      expect(stats.totalDocuments).toBe(1);
    });

    it('should promote fields from nested structures', async () => {
      writer = new StreamingParquetWriter(storage, 'test/nested-promote.parquet', {
        fieldPromotions: {
          'user.name': 'string',
          'user.address.city': 'string',
        },
      });

      await writer.write({
        _id: '1',
        user: {
          name: 'Alice',
          email: 'alice@example.com',
          address: {
            city: 'NYC',
            zip: '10001',
          },
        },
      });
      await writer.close();

      const footer = writer.getFooter();
      const schemaFields = footer!.schema.map((s: { name: string }) => s.name);
      expect(schemaFields).toContain('user.name');
      expect(schemaFields).toContain('user.address.city');
    });

    it('should handle documents with varying nesting depths', async () => {
      writer = new StreamingParquetWriter(storage, 'test/nested-varying.parquet');

      // Shallow document
      await writer.write({ _id: '1', value: 'flat' });

      // Medium depth document
      await writer.write({
        _id: '2',
        level1: {
          level2: {
            value: 'medium',
          },
        },
      });

      // Deep document
      await writer.write(createNestedDocument(4, 2));

      await writer.close();

      const stats = writer.getStatistics();
      expect(stats.totalDocuments).toBe(3);
    });
  });

  // ============================================================================
  // Edge Cases and Error Handling
  // ============================================================================

  describe('Edge cases and error handling', () => {
    it('should handle writing zero documents', async () => {
      writer = new StreamingParquetWriter(storage, 'test/empty.parquet');

      await writer.close();

      const stats = writer.getStatistics();
      expect(stats.totalDocuments).toBe(0);
      expect(stats.rowGroupsWritten).toBe(0);
    });

    it('should handle document with only _id', async () => {
      writer = new StreamingParquetWriter(storage, 'test/id-only.parquet');

      await writer.write({ _id: '1' });
      await writer.close();

      const stats = writer.getStatistics();
      expect(stats.totalDocuments).toBe(1);
    });

    it('should handle very large single document', async () => {
      writer = new StreamingParquetWriter(storage, 'test/large-doc.parquet', {
        rowGroupSizeBytes: 10 * 1024 * 1024, // 10MB row groups
      });

      // 5MB document
      await writer.write(createLargeDocument(1, 5 * 1024));
      await writer.close();

      const stats = writer.getStatistics();
      expect(stats.totalDocuments).toBe(1);
    });

    it('should reject document without _id', async () => {
      writer = new StreamingParquetWriter(storage, 'test/no-id.parquet');

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      await expect(writer.write({ name: 'No ID' } as any)).rejects.toThrow();
    });

    it('should handle special characters in field names', async () => {
      writer = new StreamingParquetWriter(storage, 'test/special-fields.parquet');

      await writer.write({
        _id: '1',
        'field.with.dots': 'value1',
        'field-with-dashes': 'value2',
        'field with spaces': 'value3',
        'field/with/slashes': 'value4',
      });
      await writer.close();

      const stats = writer.getStatistics();
      expect(stats.totalDocuments).toBe(1);
    });

    it('should handle unicode field names and values', async () => {
      writer = new StreamingParquetWriter(storage, 'test/unicode.parquet');

      await writer.write({
        _id: '1',
        '\u540d\u524d': '\u7530\u4e2d\u592a\u90ce',
        '\u5e74\u9f61': 30,
        '\u30bf\u30b0': ['\u65e5\u672c', '\u6771\u4eac'],
      });
      await writer.close();

      const stats = writer.getStatistics();
      expect(stats.totalDocuments).toBe(1);
    });

    it('should handle concurrent writes and flushes', async () => {
      writer = new StreamingParquetWriter(storage, 'test/concurrent.parquet', {
        rowGroupSizeBytes: 10 * 1024,
      });

      // Simulate concurrent operations
      const operations = [];
      for (let i = 0; i < 100; i++) {
        operations.push(writer.write(createTestDocument(i)));
        if (i % 20 === 0) {
          operations.push(writer.flushRowGroup());
        }
      }
      await Promise.all(operations);
      await writer.close();

      const stats = writer.getStatistics();
      expect(stats.totalDocuments).toBe(100);
    });
  });
});

// ============================================================================
// Helper Functions
// ============================================================================

function concatenateParts(upload: MockMultipartUpload): Uint8Array {
  const sortedParts = Array.from(upload.parts.entries())
    .sort(([a], [b]) => a - b)
    .map(([_, data]) => data);

  const totalSize = sortedParts.reduce((sum, part) => sum + part.byteLength, 0);
  const result = new Uint8Array(totalSize);
  let offset = 0;

  for (const part of sortedParts) {
    result.set(part, offset);
    offset += part.byteLength;
  }

  return result;
}
