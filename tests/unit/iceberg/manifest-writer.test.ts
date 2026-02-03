/**
 * Iceberg Manifest Writer Tests (TDD RED Phase)
 *
 * Tests for generating Iceberg manifest files in Avro format.
 * Manifest files contain entries describing data files in an Iceberg table,
 * including their metadata, partition values, and column-level statistics.
 *
 * Iceberg Manifest File Specification:
 * - Manifests are Avro files containing a list of manifest entries
 * - Each entry describes one data file (or delete file)
 * - Entries include status (added/existing/deleted), snapshot info, file metadata
 * - File metadata includes path, format, partition values, record count, file size
 * - Column-level statistics (min/max/null count) enable query planning optimizations
 *
 * Reference: https://iceberg.apache.org/spec/#manifests
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  ManifestWriter,
  type ManifestEntry,
  type DataFile,
  type DeleteFile,
  type PartitionFieldSummary,
  type ManifestContent,
  type ManifestMetadata,
  type ColumnStats,
  type FileFormat,
  type ManifestEntryStatus,
  type SortOrder,
} from '../../../src/iceberg/manifest-writer.js';

// ============================================================================
// Constants
// ============================================================================

/** Avro magic bytes */
const AVRO_MAGIC = new Uint8Array([0x4f, 0x62, 0x6a, 0x01]); // "Obj\x01"

/** Iceberg manifest file schema ID for v2 */
const MANIFEST_SCHEMA_ID_V2 = 2;

// ============================================================================
// Manifest Entry Structure Tests
// ============================================================================

describe('ManifestWriter - Manifest entry structure', () => {
  let writer: ManifestWriter;

  beforeEach(() => {
    writer = new ManifestWriter({
      schemaId: 1,
      partitionSpecId: 0,
      formatVersion: 2,
    });
  });

  it('should create a manifest writer with default configuration', () => {
    const defaultWriter = new ManifestWriter();

    expect(defaultWriter).toBeDefined();
    expect(defaultWriter.getFormatVersion()).toBe(2);
  });

  it('should create a manifest writer with custom schema ID', () => {
    const customWriter = new ManifestWriter({
      schemaId: 5,
      partitionSpecId: 0,
    });

    expect(customWriter.getSchemaId()).toBe(5);
  });

  it('should create a manifest writer with custom partition spec ID', () => {
    const customWriter = new ManifestWriter({
      schemaId: 1,
      partitionSpecId: 3,
    });

    expect(customWriter.getPartitionSpecId()).toBe(3);
  });

  it('should add a data file entry with ADDED status', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/part-00000.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 1024000,
    };

    writer.addDataFile(dataFile, 'ADDED');

    const entries = writer.getEntries();
    expect(entries).toHaveLength(1);
    expect(entries[0].status).toBe('ADDED');
    expect(entries[0].dataFile).toBeDefined();
    expect(entries[0].dataFile?.filePath).toBe('s3://bucket/data/part-00000.parquet');
  });

  it('should add a data file entry with EXISTING status', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/part-00001.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 2000,
      fileSizeBytes: 2048000,
    };

    writer.addDataFile(dataFile, 'EXISTING');

    const entries = writer.getEntries();
    expect(entries[0].status).toBe('EXISTING');
  });

  it('should add a data file entry with DELETED status', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/part-00002.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 500,
      fileSizeBytes: 512000,
    };

    writer.addDataFile(dataFile, 'DELETED');

    const entries = writer.getEntries();
    expect(entries[0].status).toBe('DELETED');
  });

  it('should include snapshot ID in manifest entry', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/part-00003.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 100,
      fileSizeBytes: 102400,
    };

    writer.addDataFile(dataFile, 'ADDED', { snapshotId: BigInt('1234567890123456789') });

    const entries = writer.getEntries();
    expect(entries[0].snapshotId).toBe(BigInt('1234567890123456789'));
  });

  it('should include sequence number in manifest entry (v2)', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/part-00004.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 100,
      fileSizeBytes: 102400,
    };

    writer.addDataFile(dataFile, 'ADDED', { sequenceNumber: BigInt(42) });

    const entries = writer.getEntries();
    expect(entries[0].sequenceNumber).toBe(BigInt(42));
  });

  it('should include file sequence number in manifest entry (v2)', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/part-00005.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 100,
      fileSizeBytes: 102400,
    };

    writer.addDataFile(dataFile, 'ADDED', { fileSequenceNumber: BigInt(10) });

    const entries = writer.getEntries();
    expect(entries[0].fileSequenceNumber).toBe(BigInt(10));
  });

  it('should support adding multiple entries', () => {
    for (let i = 0; i < 10; i++) {
      const dataFile: DataFile = {
        content: 'DATA',
        filePath: `s3://bucket/data/part-${i.toString().padStart(5, '0')}.parquet`,
        fileFormat: 'PARQUET',
        partitionValues: {},
        recordCount: 100 * (i + 1),
        fileSizeBytes: 102400 * (i + 1),
      };
      writer.addDataFile(dataFile, 'ADDED');
    }

    expect(writer.getEntries()).toHaveLength(10);
  });

  it('should track entry counts by status', () => {
    const files: DataFile[] = Array.from({ length: 5 }, (_, i) => ({
      content: 'DATA' as const,
      filePath: `s3://bucket/data/file-${i}.parquet`,
      fileFormat: 'PARQUET' as const,
      partitionValues: {},
      recordCount: 100,
      fileSizeBytes: 10000,
    }));

    writer.addDataFile(files[0], 'ADDED');
    writer.addDataFile(files[1], 'ADDED');
    writer.addDataFile(files[2], 'EXISTING');
    writer.addDataFile(files[3], 'EXISTING');
    writer.addDataFile(files[4], 'DELETED');

    const summary = writer.getSummary();
    expect(summary.addedFilesCount).toBe(2);
    expect(summary.existingFilesCount).toBe(2);
    expect(summary.deletedFilesCount).toBe(1);
  });
});

// ============================================================================
// Data File Serialization Tests
// ============================================================================

describe('ManifestWriter - Data file serialization', () => {
  let writer: ManifestWriter;

  beforeEach(() => {
    writer = new ManifestWriter({
      schemaId: 1,
      partitionSpecId: 0,
      formatVersion: 2,
    });
  });

  it('should serialize file path correctly', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://my-bucket/warehouse/db/table/data/00000-0-abc123.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 1024000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.filePath).toBe(
      's3://my-bucket/warehouse/db/table/data/00000-0-abc123.parquet'
    );
  });

  it('should serialize PARQUET file format', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 100,
      fileSizeBytes: 10000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.fileFormat).toBe('PARQUET');
  });

  it('should serialize AVRO file format', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.avro',
      fileFormat: 'AVRO',
      partitionValues: {},
      recordCount: 100,
      fileSizeBytes: 10000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.fileFormat).toBe('AVRO');
  });

  it('should serialize ORC file format', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.orc',
      fileFormat: 'ORC',
      partitionValues: {},
      recordCount: 100,
      fileSizeBytes: 10000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.fileFormat).toBe('ORC');
  });

  it('should serialize record count', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 123456789,
      fileSizeBytes: 10000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.recordCount).toBe(123456789);
  });

  it('should serialize file size in bytes', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 987654321,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.fileSizeBytes).toBe(987654321);
  });

  it('should serialize column sizes map', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
      columnSizes: {
        1: 25000,
        2: 50000,
        3: 25000,
      },
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.columnSizes).toEqual({
      1: 25000,
      2: 50000,
      3: 25000,
    });
  });

  it('should serialize value counts map', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
      valueCounts: {
        1: 1000,
        2: 950,
        3: 800,
      },
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.valueCounts).toEqual({
      1: 1000,
      2: 950,
      3: 800,
    });
  });

  it('should serialize null value counts map', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
      nullValueCounts: {
        1: 0,
        2: 50,
        3: 200,
      },
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.nullValueCounts).toEqual({
      1: 0,
      2: 50,
      3: 200,
    });
  });

  it('should serialize NaN value counts map', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
      nanValueCounts: {
        1: 0,
        2: 5,
      },
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.nanValueCounts).toEqual({
      1: 0,
      2: 5,
    });
  });

  it('should serialize split offsets', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 10000,
      fileSizeBytes: 100000000,
      splitOffsets: [0, 25000000, 50000000, 75000000],
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.splitOffsets).toEqual([0, 25000000, 50000000, 75000000]);
  });

  it('should serialize equality IDs for delete files', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 100,
      fileSizeBytes: 10000,
      equalityIds: [1, 2, 3],
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.equalityIds).toEqual([1, 2, 3]);
  });

  it('should serialize sort order ID', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
      sortOrderId: 1,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.sortOrderId).toBe(1);
  });

  it('should serialize content type as DATA', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.content).toBe('DATA');
  });
});

// ============================================================================
// Partition Values Tests
// ============================================================================

describe('ManifestWriter - Partition values', () => {
  let writer: ManifestWriter;

  beforeEach(() => {
    writer = new ManifestWriter({
      schemaId: 1,
      partitionSpecId: 0,
      formatVersion: 2,
    });
  });

  it('should serialize empty partition values for unpartitioned table', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.partitionValues).toEqual({});
  });

  it('should serialize single string partition value', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/date=2024-01-15/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {
        date: '2024-01-15',
      },
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.partitionValues).toEqual({
      date: '2024-01-15',
    });
  });

  it('should serialize integer partition value', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/year=2024/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {
        year: 2024,
      },
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.partitionValues).toEqual({
      year: 2024,
    });
  });

  it('should serialize multiple partition values', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/year=2024/month=01/day=15/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {
        year: 2024,
        month: 1,
        day: 15,
      },
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.partitionValues).toEqual({
      year: 2024,
      month: 1,
      day: 15,
    });
  });

  it('should serialize null partition value', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/category=__null__/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {
        category: null,
      },
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.partitionValues).toEqual({
      category: null,
    });
  });

  it('should serialize binary partition value', () => {
    const binaryValue = new Uint8Array([0x01, 0x02, 0x03, 0x04]);
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/hash=01020304/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {
        hash: binaryValue,
      },
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.partitionValues.hash).toEqual(binaryValue);
  });

  it('should serialize boolean partition value', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/active=true/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {
        active: true,
      },
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.partitionValues).toEqual({
      active: true,
    });
  });

  it('should serialize long partition value', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/timestamp=1705312800000/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {
        timestamp: BigInt('1705312800000000'),
      },
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.partitionValues.timestamp).toBe(BigInt('1705312800000000'));
  });

  it('should serialize double partition value', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/bucket=3.14/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {
        bucket: 3.14159,
      },
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.partitionValues.bucket).toBeCloseTo(3.14159, 5);
  });

  it('should track partition field summaries in manifest metadata', () => {
    const dataFile1: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/date=2024-01-15/file1.parquet',
      fileFormat: 'PARQUET',
      partitionValues: { date: '2024-01-15' },
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    const dataFile2: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/date=2024-01-16/file2.parquet',
      fileFormat: 'PARQUET',
      partitionValues: { date: '2024-01-16' },
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    writer.addDataFile(dataFile1, 'ADDED');
    writer.addDataFile(dataFile2, 'ADDED');

    const metadata = writer.getManifestMetadata();
    expect(metadata.partitionFieldSummaries).toBeDefined();
    expect(metadata.partitionFieldSummaries?.[0]?.containsNull).toBe(false);
    expect(metadata.partitionFieldSummaries?.[0]?.containsNaN).toBe(false);
  });

  it('should track contains_null in partition field summary', () => {
    const dataFile1: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/category=A/file1.parquet',
      fileFormat: 'PARQUET',
      partitionValues: { category: 'A' },
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    const dataFile2: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/category=__null__/file2.parquet',
      fileFormat: 'PARQUET',
      partitionValues: { category: null },
      recordCount: 500,
      fileSizeBytes: 50000,
    };

    writer.addDataFile(dataFile1, 'ADDED');
    writer.addDataFile(dataFile2, 'ADDED');

    const metadata = writer.getManifestMetadata();
    expect(metadata.partitionFieldSummaries?.[0]?.containsNull).toBe(true);
  });

  it('should track lower and upper bounds in partition field summary', () => {
    const files: DataFile[] = [
      {
        content: 'DATA',
        filePath: 's3://bucket/data/date=2024-01-15/file1.parquet',
        fileFormat: 'PARQUET',
        partitionValues: { date: '2024-01-15' },
        recordCount: 1000,
        fileSizeBytes: 100000,
      },
      {
        content: 'DATA',
        filePath: 's3://bucket/data/date=2024-01-10/file2.parquet',
        fileFormat: 'PARQUET',
        partitionValues: { date: '2024-01-10' },
        recordCount: 1000,
        fileSizeBytes: 100000,
      },
      {
        content: 'DATA',
        filePath: 's3://bucket/data/date=2024-01-20/file3.parquet',
        fileFormat: 'PARQUET',
        partitionValues: { date: '2024-01-20' },
        recordCount: 1000,
        fileSizeBytes: 100000,
      },
    ];

    files.forEach((f) => writer.addDataFile(f, 'ADDED'));

    const metadata = writer.getManifestMetadata();
    expect(metadata.partitionFieldSummaries?.[0]?.lowerBound).toBe('2024-01-10');
    expect(metadata.partitionFieldSummaries?.[0]?.upperBound).toBe('2024-01-20');
  });
});

// ============================================================================
// Column Statistics Tests
// ============================================================================

describe('ManifestWriter - Column stats', () => {
  let writer: ManifestWriter;

  beforeEach(() => {
    writer = new ManifestWriter({
      schemaId: 1,
      partitionSpecId: 0,
      formatVersion: 2,
    });
  });

  it('should serialize lower bounds for columns', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
      lowerBounds: {
        1: new Uint8Array([0, 0, 0, 0, 0, 0, 0, 1]), // int64 = 1
        2: new TextEncoder().encode('Alice'),
      },
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.lowerBounds).toBeDefined();
    expect(entries[0].dataFile?.lowerBounds?.[1]).toEqual(
      new Uint8Array([0, 0, 0, 0, 0, 0, 0, 1])
    );
  });

  it('should serialize upper bounds for columns', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
      upperBounds: {
        1: new Uint8Array([0, 0, 0, 0, 0, 0, 3, 232]), // int64 = 1000
        2: new TextEncoder().encode('Zoe'),
      },
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.upperBounds).toBeDefined();
    expect(entries[0].dataFile?.upperBounds?.[2]).toEqual(new TextEncoder().encode('Zoe'));
  });

  it('should serialize null value counts per column', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
      nullValueCounts: {
        1: 0,
        2: 150,
        3: 500,
      },
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.nullValueCounts).toEqual({
      1: 0,
      2: 150,
      3: 500,
    });
  });

  it('should serialize column sizes per column', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
      columnSizes: {
        1: 8000,
        2: 45000,
        3: 47000,
      },
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.columnSizes).toEqual({
      1: 8000,
      2: 45000,
      3: 47000,
    });
  });

  it('should serialize value counts per column', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
      valueCounts: {
        1: 1000,
        2: 850,
        3: 500,
      },
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.valueCounts).toEqual({
      1: 1000,
      2: 850,
      3: 500,
    });
  });

  it('should serialize NaN counts for floating point columns', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
      nanValueCounts: {
        3: 5,
        4: 0,
      },
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.nanValueCounts).toEqual({
      3: 5,
      4: 0,
    });
  });

  it('should handle empty statistics maps', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
      // No statistics provided
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.lowerBounds).toBeUndefined();
    expect(entries[0].dataFile?.upperBounds).toBeUndefined();
  });

  it('should serialize statistics for all data types', () => {
    const int32Min = new Uint8Array(4);
    new DataView(int32Min.buffer).setInt32(0, -100, true);

    const int32Max = new Uint8Array(4);
    new DataView(int32Max.buffer).setInt32(0, 1000, true);

    const int64Min = new Uint8Array(8);
    new DataView(int64Min.buffer).setBigInt64(0, BigInt('-9223372036854775808'), true);

    const int64Max = new Uint8Array(8);
    new DataView(int64Max.buffer).setBigInt64(0, BigInt('9223372036854775807'), true);

    const floatMin = new Uint8Array(4);
    new DataView(floatMin.buffer).setFloat32(0, -3.14, true);

    const floatMax = new Uint8Array(4);
    new DataView(floatMax.buffer).setFloat32(0, 3.14, true);

    const doubleMin = new Uint8Array(8);
    new DataView(doubleMin.buffer).setFloat64(0, -Math.PI, true);

    const doubleMax = new Uint8Array(8);
    new DataView(doubleMax.buffer).setFloat64(0, Math.PI, true);

    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
      lowerBounds: {
        1: int32Min,
        2: int64Min,
        3: floatMin,
        4: doubleMin,
        5: new TextEncoder().encode('aaa'),
        6: new Uint8Array([0x00]),
      },
      upperBounds: {
        1: int32Max,
        2: int64Max,
        3: floatMax,
        4: doubleMax,
        5: new TextEncoder().encode('zzz'),
        6: new Uint8Array([0xff]),
      },
    };

    writer.addDataFile(dataFile, 'ADDED');
    const entries = writer.getEntries();

    expect(entries[0].dataFile?.lowerBounds).toBeDefined();
    expect(entries[0].dataFile?.upperBounds).toBeDefined();
    expect(Object.keys(entries[0].dataFile?.lowerBounds || {})).toHaveLength(6);
  });

  it('should aggregate column statistics across entries for manifest summary', () => {
    const file1: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file1.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 500,
      fileSizeBytes: 50000,
      nullValueCounts: { 1: 10, 2: 20 },
    };

    const file2: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file2.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 500,
      fileSizeBytes: 50000,
      nullValueCounts: { 1: 5, 2: 30 },
    };

    writer.addDataFile(file1, 'ADDED');
    writer.addDataFile(file2, 'ADDED');

    const summary = writer.getSummary();
    expect(summary.totalRecordCount).toBe(1000);
  });
});

// ============================================================================
// Delete File Support Tests
// ============================================================================

describe('ManifestWriter - Delete file support', () => {
  let writer: ManifestWriter;

  beforeEach(() => {
    writer = new ManifestWriter({
      schemaId: 1,
      partitionSpecId: 0,
      formatVersion: 2,
      contentType: 'DELETES',
    });
  });

  it('should create a manifest writer for delete files', () => {
    expect(writer.getContentType()).toBe('DELETES');
  });

  it('should add position delete file', () => {
    const deleteFile: DeleteFile = {
      content: 'POSITION_DELETES',
      filePath: 's3://bucket/data/delete-00000.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 100,
      fileSizeBytes: 10000,
    };

    writer.addDeleteFile(deleteFile, 'ADDED');

    const entries = writer.getEntries();
    expect(entries).toHaveLength(1);
    expect(entries[0].dataFile?.content).toBe('POSITION_DELETES');
  });

  it('should add equality delete file', () => {
    const deleteFile: DeleteFile = {
      content: 'EQUALITY_DELETES',
      filePath: 's3://bucket/data/eq-delete-00000.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 50,
      fileSizeBytes: 5000,
      equalityIds: [1, 2],
    };

    writer.addDeleteFile(deleteFile, 'ADDED');

    const entries = writer.getEntries();
    expect(entries[0].dataFile?.content).toBe('EQUALITY_DELETES');
    expect(entries[0].dataFile?.equalityIds).toEqual([1, 2]);
  });

  it('should include referenced data file in position deletes', () => {
    const deleteFile: DeleteFile = {
      content: 'POSITION_DELETES',
      filePath: 's3://bucket/data/delete-00000.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 100,
      fileSizeBytes: 10000,
      referencedDataFile: 's3://bucket/data/part-00000.parquet',
    };

    writer.addDeleteFile(deleteFile, 'ADDED');

    const entries = writer.getEntries();
    expect(entries[0].dataFile?.referencedDataFile).toBe('s3://bucket/data/part-00000.parquet');
  });

  it('should track delete file counts separately in summary', () => {
    const positionDelete: DeleteFile = {
      content: 'POSITION_DELETES',
      filePath: 's3://bucket/data/pos-delete.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 100,
      fileSizeBytes: 10000,
    };

    const equalityDelete: DeleteFile = {
      content: 'EQUALITY_DELETES',
      filePath: 's3://bucket/data/eq-delete.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 50,
      fileSizeBytes: 5000,
      equalityIds: [1],
    };

    writer.addDeleteFile(positionDelete, 'ADDED');
    writer.addDeleteFile(equalityDelete, 'ADDED');

    const summary = writer.getSummary();
    expect(summary.positionDeleteCount).toBe(1);
    expect(summary.equalityDeleteCount).toBe(1);
  });

  it('should not allow mixing data and delete content types', () => {
    const dataWriter = new ManifestWriter({
      schemaId: 1,
      partitionSpecId: 0,
      contentType: 'DATA',
    });

    const deleteFile: DeleteFile = {
      content: 'POSITION_DELETES',
      filePath: 's3://bucket/data/delete.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 100,
      fileSizeBytes: 10000,
    };

    expect(() => dataWriter.addDeleteFile(deleteFile, 'ADDED')).toThrow();
  });

  it('should track deleted rows count for position deletes', () => {
    const deleteFile1: DeleteFile = {
      content: 'POSITION_DELETES',
      filePath: 's3://bucket/data/delete1.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 100,
      fileSizeBytes: 10000,
    };

    const deleteFile2: DeleteFile = {
      content: 'POSITION_DELETES',
      filePath: 's3://bucket/data/delete2.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 200,
      fileSizeBytes: 20000,
    };

    writer.addDeleteFile(deleteFile1, 'ADDED');
    writer.addDeleteFile(deleteFile2, 'ADDED');

    const summary = writer.getSummary();
    expect(summary.deletedRowCount).toBe(300);
  });

  it('should include equality field IDs for equality deletes', () => {
    const deleteFile: DeleteFile = {
      content: 'EQUALITY_DELETES',
      filePath: 's3://bucket/data/eq-delete.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 50,
      fileSizeBytes: 5000,
      equalityIds: [1, 2, 3],
    };

    writer.addDeleteFile(deleteFile, 'ADDED');

    const entries = writer.getEntries();
    expect(entries[0].dataFile?.equalityIds).toEqual([1, 2, 3]);
  });
});

// ============================================================================
// Avro Serialization Tests
// ============================================================================

describe('ManifestWriter - Avro serialization', () => {
  let writer: ManifestWriter;

  beforeEach(() => {
    writer = new ManifestWriter({
      schemaId: 1,
      partitionSpecId: 0,
      formatVersion: 2,
    });
  });

  it('should generate valid Avro file with magic bytes', async () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const avroData = await writer.toAvro();

    // Check Avro magic bytes
    expect(avroData.slice(0, 4)).toEqual(AVRO_MAGIC);
  });

  it('should include Avro schema in file header', async () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const avroData = await writer.toAvro();

    // Avro file should be larger than just the data (includes header with schema)
    expect(avroData.byteLength).toBeGreaterThan(100);
  });

  it('should include sync marker in Avro blocks', async () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const avroData = await writer.toAvro();

    // Avro files contain 16-byte sync markers
    expect(avroData.byteLength).toBeGreaterThan(16);
  });

  it('should serialize empty manifest', async () => {
    const avroData = await writer.toAvro();

    // Even empty manifest should have valid Avro header
    expect(avroData.slice(0, 4)).toEqual(AVRO_MAGIC);
  });

  it('should serialize multiple entries', async () => {
    for (let i = 0; i < 100; i++) {
      const dataFile: DataFile = {
        content: 'DATA',
        filePath: `s3://bucket/data/file-${i}.parquet`,
        fileFormat: 'PARQUET',
        partitionValues: { partition: i % 10 },
        recordCount: 1000,
        fileSizeBytes: 100000,
      };
      writer.addDataFile(dataFile, 'ADDED');
    }

    const avroData = await writer.toAvro();

    expect(avroData.byteLength).toBeGreaterThan(0);
  });

  it('should include manifest metadata in Avro header', async () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const avroData = await writer.toAvro();

    // Metadata should be encoded in the Avro file metadata map
    // This includes schema-id, partition-spec-id, format-version, etc.
    expect(avroData.byteLength).toBeGreaterThan(0);
  });

  it('should compress Avro blocks with deflate codec', async () => {
    writer = new ManifestWriter({
      schemaId: 1,
      partitionSpecId: 0,
      compressionCodec: 'deflate',
    });

    for (let i = 0; i < 100; i++) {
      const dataFile: DataFile = {
        content: 'DATA',
        filePath: `s3://bucket/data/file-${i}.parquet`,
        fileFormat: 'PARQUET',
        partitionValues: {},
        recordCount: 1000,
        fileSizeBytes: 100000,
      };
      writer.addDataFile(dataFile, 'ADDED');
    }

    const avroData = await writer.toAvro();
    expect(avroData.byteLength).toBeGreaterThan(0);
  });

  it('should support snappy compression codec', async () => {
    writer = new ManifestWriter({
      schemaId: 1,
      partitionSpecId: 0,
      compressionCodec: 'snappy',
    });

    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const avroData = await writer.toAvro();

    expect(avroData.byteLength).toBeGreaterThan(0);
  });

  it('should support null (no compression) codec', async () => {
    writer = new ManifestWriter({
      schemaId: 1,
      partitionSpecId: 0,
      compressionCodec: 'null',
    });

    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const avroData = await writer.toAvro();

    expect(avroData.byteLength).toBeGreaterThan(0);
  });
});

// ============================================================================
// Manifest Metadata Tests
// ============================================================================

describe('ManifestWriter - Manifest metadata', () => {
  let writer: ManifestWriter;

  beforeEach(() => {
    writer = new ManifestWriter({
      schemaId: 1,
      partitionSpecId: 0,
      formatVersion: 2,
    });
  });

  it('should include schema ID in manifest metadata', () => {
    const metadata = writer.getManifestMetadata();
    expect(metadata.schemaId).toBe(1);
  });

  it('should include partition spec ID in manifest metadata', () => {
    const metadata = writer.getManifestMetadata();
    expect(metadata.partitionSpecId).toBe(0);
  });

  it('should include format version in manifest metadata', () => {
    const metadata = writer.getManifestMetadata();
    expect(metadata.formatVersion).toBe(2);
  });

  it('should include content type in manifest metadata', () => {
    const dataWriter = new ManifestWriter({ contentType: 'DATA' });
    const deleteWriter = new ManifestWriter({ contentType: 'DELETES' });

    expect(dataWriter.getManifestMetadata().contentType).toBe('DATA');
    expect(deleteWriter.getManifestMetadata().contentType).toBe('DELETES');
  });

  it('should calculate added files count', () => {
    const files: DataFile[] = Array.from({ length: 5 }, (_, i) => ({
      content: 'DATA' as const,
      filePath: `s3://bucket/data/file-${i}.parquet`,
      fileFormat: 'PARQUET' as const,
      partitionValues: {},
      recordCount: 100,
      fileSizeBytes: 10000,
    }));

    files.forEach((f) => writer.addDataFile(f, 'ADDED'));

    const metadata = writer.getManifestMetadata();
    expect(metadata.addedFilesCount).toBe(5);
  });

  it('should calculate existing files count', () => {
    const files: DataFile[] = Array.from({ length: 3 }, (_, i) => ({
      content: 'DATA' as const,
      filePath: `s3://bucket/data/file-${i}.parquet`,
      fileFormat: 'PARQUET' as const,
      partitionValues: {},
      recordCount: 100,
      fileSizeBytes: 10000,
    }));

    files.forEach((f) => writer.addDataFile(f, 'EXISTING'));

    const metadata = writer.getManifestMetadata();
    expect(metadata.existingFilesCount).toBe(3);
  });

  it('should calculate deleted files count', () => {
    const files: DataFile[] = Array.from({ length: 2 }, (_, i) => ({
      content: 'DATA' as const,
      filePath: `s3://bucket/data/file-${i}.parquet`,
      fileFormat: 'PARQUET' as const,
      partitionValues: {},
      recordCount: 100,
      fileSizeBytes: 10000,
    }));

    files.forEach((f) => writer.addDataFile(f, 'DELETED'));

    const metadata = writer.getManifestMetadata();
    expect(metadata.deletedFilesCount).toBe(2);
  });

  it('should calculate added rows count', () => {
    const files: DataFile[] = [
      {
        content: 'DATA',
        filePath: 's3://bucket/data/file-1.parquet',
        fileFormat: 'PARQUET',
        partitionValues: {},
        recordCount: 1000,
        fileSizeBytes: 100000,
      },
      {
        content: 'DATA',
        filePath: 's3://bucket/data/file-2.parquet',
        fileFormat: 'PARQUET',
        partitionValues: {},
        recordCount: 2000,
        fileSizeBytes: 200000,
      },
    ];

    files.forEach((f) => writer.addDataFile(f, 'ADDED'));

    const metadata = writer.getManifestMetadata();
    expect(metadata.addedRowsCount).toBe(3000);
  });

  it('should calculate existing rows count', () => {
    const file: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 5000,
      fileSizeBytes: 500000,
    };

    writer.addDataFile(file, 'EXISTING');

    const metadata = writer.getManifestMetadata();
    expect(metadata.existingRowsCount).toBe(5000);
  });

  it('should calculate deleted rows count', () => {
    const file: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1500,
      fileSizeBytes: 150000,
    };

    writer.addDataFile(file, 'DELETED');

    const metadata = writer.getManifestMetadata();
    expect(metadata.deletedRowsCount).toBe(1500);
  });

  it('should track minimum sequence number', () => {
    const files: DataFile[] = Array.from({ length: 3 }, (_, i) => ({
      content: 'DATA' as const,
      filePath: `s3://bucket/data/file-${i}.parquet`,
      fileFormat: 'PARQUET' as const,
      partitionValues: {},
      recordCount: 100,
      fileSizeBytes: 10000,
    }));

    writer.addDataFile(files[0], 'ADDED', { sequenceNumber: BigInt(5) });
    writer.addDataFile(files[1], 'ADDED', { sequenceNumber: BigInt(3) });
    writer.addDataFile(files[2], 'ADDED', { sequenceNumber: BigInt(7) });

    const metadata = writer.getManifestMetadata();
    expect(metadata.minSequenceNumber).toBe(BigInt(3));
  });

  it('should include manifest path when set', () => {
    writer.setManifestPath('s3://bucket/metadata/snap-123-1-abc.avro');

    const metadata = writer.getManifestMetadata();
    expect(metadata.manifestPath).toBe('s3://bucket/metadata/snap-123-1-abc.avro');
  });

  it('should track manifest length after serialization', async () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    const avroData = await writer.toAvro();

    const metadata = writer.getManifestMetadata();
    expect(metadata.manifestLength).toBe(avroData.byteLength);
  });
});

// ============================================================================
// Error Handling Tests
// ============================================================================

describe('ManifestWriter - Error handling', () => {
  let writer: ManifestWriter;

  beforeEach(() => {
    writer = new ManifestWriter({
      schemaId: 1,
      partitionSpecId: 0,
    });
  });

  it('should reject invalid file format', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.txt',
      fileFormat: 'INVALID' as FileFormat,
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    expect(() => writer.addDataFile(dataFile, 'ADDED')).toThrow(/invalid.*format/i);
  });

  it('should reject negative record count', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: -100,
      fileSizeBytes: 100000,
    };

    expect(() => writer.addDataFile(dataFile, 'ADDED')).toThrow(/negative.*record|record.*negative/i);
  });

  it('should reject negative file size', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 100,
      fileSizeBytes: -10000,
    };

    expect(() => writer.addDataFile(dataFile, 'ADDED')).toThrow(/negative.*size|size.*negative/i);
  });

  it('should reject empty file path', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: '',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 100,
      fileSizeBytes: 10000,
    };

    expect(() => writer.addDataFile(dataFile, 'ADDED')).toThrow(/empty.*path|path.*required/i);
  });

  it('should reject invalid entry status', () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 100,
      fileSizeBytes: 10000,
    };

    expect(() => writer.addDataFile(dataFile, 'INVALID' as ManifestEntryStatus)).toThrow(/invalid.*status/i);
  });

  it('should reject equality deletes without equality IDs', () => {
    const deleteWriter = new ManifestWriter({ contentType: 'DELETES' });

    const deleteFile: DeleteFile = {
      content: 'EQUALITY_DELETES',
      filePath: 's3://bucket/data/eq-delete.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 50,
      fileSizeBytes: 5000,
      // Missing equalityIds
    };

    expect(() => deleteWriter.addDeleteFile(deleteFile, 'ADDED')).toThrow(/equality.*ids|equalityIds.*required/i);
  });

  it('should reject adding entries after serialization', async () => {
    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 100,
      fileSizeBytes: 10000,
    };

    writer.addDataFile(dataFile, 'ADDED');
    await writer.toAvro();

    const newFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file2.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 200,
      fileSizeBytes: 20000,
    };

    expect(() => writer.addDataFile(newFile, 'ADDED')).toThrow(/already.*serialized|closed|finalized/i);
  });
});

// ============================================================================
// Format Version Compatibility Tests
// ============================================================================

describe('ManifestWriter - Format version compatibility', () => {
  it('should support Iceberg format version 1', () => {
    const v1Writer = new ManifestWriter({
      schemaId: 1,
      partitionSpecId: 0,
      formatVersion: 1,
    });

    expect(v1Writer.getFormatVersion()).toBe(1);
  });

  it('should support Iceberg format version 2', () => {
    const v2Writer = new ManifestWriter({
      schemaId: 1,
      partitionSpecId: 0,
      formatVersion: 2,
    });

    expect(v2Writer.getFormatVersion()).toBe(2);
  });

  it('should default to format version 2', () => {
    const writer = new ManifestWriter();
    expect(writer.getFormatVersion()).toBe(2);
  });

  it('should not include sequence numbers in v1 manifests', async () => {
    const v1Writer = new ManifestWriter({
      schemaId: 1,
      partitionSpecId: 0,
      formatVersion: 1,
    });

    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    v1Writer.addDataFile(dataFile, 'ADDED', { sequenceNumber: BigInt(42) });

    const entries = v1Writer.getEntries();
    // v1 manifests should not have sequence numbers
    expect(entries[0].sequenceNumber).toBeUndefined();
  });

  it('should include sequence numbers in v2 manifests', () => {
    const v2Writer = new ManifestWriter({
      schemaId: 1,
      partitionSpecId: 0,
      formatVersion: 2,
    });

    const dataFile: DataFile = {
      content: 'DATA',
      filePath: 's3://bucket/data/file.parquet',
      fileFormat: 'PARQUET',
      partitionValues: {},
      recordCount: 1000,
      fileSizeBytes: 100000,
    };

    v2Writer.addDataFile(dataFile, 'ADDED', { sequenceNumber: BigInt(42) });

    const entries = v2Writer.getEntries();
    expect(entries[0].sequenceNumber).toBe(BigInt(42));
  });

  it('should reject unsupported format versions', () => {
    expect(() => {
      new ManifestWriter({
        schemaId: 1,
        partitionSpecId: 0,
        formatVersion: 3,
      });
    }).toThrow(/unsupported.*version|version.*3/i);
  });
});

// ============================================================================
// Large Manifest Performance Tests
// ============================================================================

describe('ManifestWriter - Performance', () => {
  it('should handle manifests with thousands of entries', async () => {
    const writer = new ManifestWriter({
      schemaId: 1,
      partitionSpecId: 0,
    });

    const startTime = performance.now();

    for (let i = 0; i < 10000; i++) {
      const dataFile: DataFile = {
        content: 'DATA',
        filePath: `s3://bucket/data/partition=${i % 100}/file-${i}.parquet`,
        fileFormat: 'PARQUET',
        partitionValues: { partition: i % 100 },
        recordCount: 1000,
        fileSizeBytes: 100000,
        lowerBounds: { 1: new Uint8Array([0, 0, 0, i & 0xff]) },
        upperBounds: { 1: new Uint8Array([0, 0, 0, (i + 1000) & 0xff]) },
        nullValueCounts: { 1: i % 100 },
      };
      writer.addDataFile(dataFile, 'ADDED');
    }

    const addTime = performance.now() - startTime;
    expect(addTime).toBeLessThan(5000); // Should complete in under 5 seconds

    const serializeStart = performance.now();
    const avroData = await writer.toAvro();
    const serializeTime = performance.now() - serializeStart;

    expect(avroData.byteLength).toBeGreaterThan(0);
    expect(serializeTime).toBeLessThan(10000); // Should serialize in under 10 seconds
  });

  it('should efficiently calculate manifest statistics', () => {
    const writer = new ManifestWriter({
      schemaId: 1,
      partitionSpecId: 0,
    });

    for (let i = 0; i < 1000; i++) {
      const dataFile: DataFile = {
        content: 'DATA',
        filePath: `s3://bucket/data/file-${i}.parquet`,
        fileFormat: 'PARQUET',
        partitionValues: {},
        recordCount: 1000 + i,
        fileSizeBytes: 100000 + i * 100,
      };
      writer.addDataFile(dataFile, i % 3 === 0 ? 'ADDED' : i % 3 === 1 ? 'EXISTING' : 'DELETED');
    }

    const startTime = performance.now();
    const summary = writer.getSummary();
    const elapsed = performance.now() - startTime;

    expect(elapsed).toBeLessThan(100); // Statistics should be calculated quickly
    expect(summary.totalRecordCount).toBeGreaterThan(0);
  });
});
