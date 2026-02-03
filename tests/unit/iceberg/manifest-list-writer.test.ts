/**
 * Manifest List Writer Tests (TDD RED Phase)
 *
 * Tests for the Iceberg manifest-list.avro writer that generates
 * manifest list files tracking all manifests in a snapshot.
 *
 * Key capabilities tested:
 * - Manifest file entry structure (manifest_file struct)
 * - Snapshot ID tracking
 * - Added/deleted file counts
 * - Partition field summaries
 * - Avro format compliance
 * - Content type handling (DATA vs DELETES)
 * - Sequence number tracking
 * - Key metadata support
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  ManifestListWriter,
  type ManifestListWriterOptions,
  type ManifestFileEntry,
  type PartitionFieldSummary,
  type ManifestContent,
  type ManifestListMetadata,
  MANIFEST_CONTENT_DATA,
  MANIFEST_CONTENT_DELETES,
} from '../../../src/iceberg/manifest-list-writer.js';
import { MemoryStorage, type StorageBackend } from '../../../src/storage/index.js';

// ============================================================================
// Test Types
// ============================================================================

interface TestManifestEntry extends ManifestFileEntry {
  manifest_path: string;
  manifest_length: number;
  partition_spec_id: number;
  content: ManifestContent;
  sequence_number: bigint;
  min_sequence_number: bigint;
  added_snapshot_id: bigint;
  added_data_files_count: number;
  existing_data_files_count: number;
  deleted_data_files_count: number;
  added_rows_count: bigint;
  existing_rows_count: bigint;
  deleted_rows_count: bigint;
  partitions?: PartitionFieldSummary[];
  key_metadata?: Uint8Array;
}

// ============================================================================
// Helper Functions
// ============================================================================

function createTestManifestEntry(
  id: number,
  options: Partial<TestManifestEntry> = {}
): ManifestFileEntry {
  return {
    manifest_path: options.manifest_path ?? `s3://bucket/metadata/manifest-${id}.avro`,
    manifest_length: options.manifest_length ?? 4096 + id * 100,
    partition_spec_id: options.partition_spec_id ?? 0,
    content: options.content ?? MANIFEST_CONTENT_DATA,
    sequence_number: options.sequence_number ?? BigInt(id),
    min_sequence_number: options.min_sequence_number ?? BigInt(id),
    added_snapshot_id: options.added_snapshot_id ?? BigInt(1000 + id),
    added_data_files_count: options.added_data_files_count ?? 10,
    existing_data_files_count: options.existing_data_files_count ?? 0,
    deleted_data_files_count: options.deleted_data_files_count ?? 0,
    added_rows_count: options.added_rows_count ?? BigInt(1000),
    existing_rows_count: options.existing_rows_count ?? BigInt(0),
    deleted_rows_count: options.deleted_rows_count ?? BigInt(0),
    partitions: options.partitions,
    key_metadata: options.key_metadata,
  };
}

function createTestPartitionSummary(
  options: Partial<PartitionFieldSummary> = {}
): PartitionFieldSummary {
  return {
    contains_null: options.contains_null ?? false,
    contains_nan: options.contains_nan ?? false,
    lower_bound: options.lower_bound,
    upper_bound: options.upper_bound,
  };
}

// ============================================================================
// 1. Manifest File Entry Structure
// ============================================================================

describe('ManifestListWriter - Manifest File Entry Structure', () => {
  let writer: ManifestListWriter;
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
    writer = new ManifestListWriter(storage);
  });

  afterEach(async () => {
    if (writer) {
      await writer.close().catch(() => {});
    }
  });

  it('should write manifest entry with required manifest_path field', async () => {
    const entry = createTestManifestEntry(1, {
      manifest_path: 's3://bucket/metadata/00000-abc123.avro',
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].manifest_path).toBe('s3://bucket/metadata/00000-abc123.avro');
  });

  it('should write manifest entry with manifest_length field', async () => {
    const entry = createTestManifestEntry(1, {
      manifest_length: 8192,
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].manifest_length).toBe(8192);
  });

  it('should write manifest entry with partition_spec_id field', async () => {
    const entry = createTestManifestEntry(1, {
      partition_spec_id: 5,
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].partition_spec_id).toBe(5);
  });

  it('should write manifest entry with content field (DATA=0)', async () => {
    const entry = createTestManifestEntry(1, {
      content: MANIFEST_CONTENT_DATA,
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].content).toBe(0);
  });

  it('should write manifest entry with content field (DELETES=1)', async () => {
    const entry = createTestManifestEntry(1, {
      content: MANIFEST_CONTENT_DELETES,
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].content).toBe(1);
  });

  it('should write all manifest_file struct fields according to Iceberg spec', async () => {
    const entry = createTestManifestEntry(1, {
      manifest_path: 's3://bucket/manifest-1.avro',
      manifest_length: 4096,
      partition_spec_id: 0,
      content: MANIFEST_CONTENT_DATA,
      sequence_number: BigInt(10),
      min_sequence_number: BigInt(5),
      added_snapshot_id: BigInt(12345),
      added_data_files_count: 100,
      existing_data_files_count: 50,
      deleted_data_files_count: 10,
      added_rows_count: BigInt(10000),
      existing_rows_count: BigInt(5000),
      deleted_rows_count: BigInt(1000),
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    const readEntry = entries[0];

    expect(readEntry.manifest_path).toBe('s3://bucket/manifest-1.avro');
    expect(readEntry.manifest_length).toBe(4096);
    expect(readEntry.partition_spec_id).toBe(0);
    expect(readEntry.content).toBe(0);
    expect(readEntry.sequence_number).toBe(BigInt(10));
    expect(readEntry.min_sequence_number).toBe(BigInt(5));
    expect(readEntry.added_snapshot_id).toBe(BigInt(12345));
    expect(readEntry.added_data_files_count).toBe(100);
    expect(readEntry.existing_data_files_count).toBe(50);
    expect(readEntry.deleted_data_files_count).toBe(10);
    expect(readEntry.added_rows_count).toBe(BigInt(10000));
    expect(readEntry.existing_rows_count).toBe(BigInt(5000));
    expect(readEntry.deleted_rows_count).toBe(BigInt(1000));
  });

  it('should validate manifest_path is not empty', async () => {
    const entry = createTestManifestEntry(1, {
      manifest_path: '',
    });

    await expect(writer.addManifest(entry)).rejects.toThrow(/manifest_path.*empty/i);
  });

  it('should validate manifest_length is positive', async () => {
    const entry = createTestManifestEntry(1, {
      manifest_length: 0,
    });

    await expect(writer.addManifest(entry)).rejects.toThrow(/manifest_length.*positive/i);
  });

  it('should validate content is a valid value (0 or 1)', async () => {
    const entry = createTestManifestEntry(1, {
      content: 2 as ManifestContent,
    });

    await expect(writer.addManifest(entry)).rejects.toThrow(/content.*invalid/i);
  });
});

// ============================================================================
// 2. Snapshot ID Tracking
// ============================================================================

describe('ManifestListWriter - Snapshot ID Tracking', () => {
  let writer: ManifestListWriter;
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
    writer = new ManifestListWriter(storage);
  });

  afterEach(async () => {
    if (writer) {
      await writer.close().catch(() => {});
    }
  });

  it('should track added_snapshot_id for each manifest entry', async () => {
    const entry1 = createTestManifestEntry(1, { added_snapshot_id: BigInt(100) });
    const entry2 = createTestManifestEntry(2, { added_snapshot_id: BigInt(200) });

    await writer.addManifest(entry1);
    await writer.addManifest(entry2);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].added_snapshot_id).toBe(BigInt(100));
    expect(entries[1].added_snapshot_id).toBe(BigInt(200));
  });

  it('should associate manifest list with snapshot ID in metadata', async () => {
    const options: ManifestListWriterOptions = {
      snapshotId: BigInt(999),
    };
    writer = new ManifestListWriter(storage, options);

    await writer.addManifest(createTestManifestEntry(1));
    const result = await writer.write('test/snap-999/manifest-list.avro');

    expect(result.metadata.snapshotId).toBe(BigInt(999));
  });

  it('should store parent snapshot ID in metadata', async () => {
    const options: ManifestListWriterOptions = {
      snapshotId: BigInt(999),
      parentSnapshotId: BigInt(998),
    };
    writer = new ManifestListWriter(storage, options);

    await writer.addManifest(createTestManifestEntry(1));
    const result = await writer.write('test/snap-999/manifest-list.avro');

    expect(result.metadata.parentSnapshotId).toBe(BigInt(998));
  });

  it('should track sequence_number for manifest entries', async () => {
    const entry = createTestManifestEntry(1, {
      sequence_number: BigInt(42),
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].sequence_number).toBe(BigInt(42));
  });

  it('should track min_sequence_number for manifest entries', async () => {
    const entry = createTestManifestEntry(1, {
      sequence_number: BigInt(50),
      min_sequence_number: BigInt(30),
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].min_sequence_number).toBe(BigInt(30));
  });

  it('should validate min_sequence_number <= sequence_number', async () => {
    const entry = createTestManifestEntry(1, {
      sequence_number: BigInt(30),
      min_sequence_number: BigInt(50), // Invalid: min > seq
    });

    await expect(writer.addManifest(entry)).rejects.toThrow(/min_sequence_number.*sequence_number/i);
  });

  it('should inherit sequence number from snapshot when not specified', async () => {
    const options: ManifestListWriterOptions = {
      snapshotId: BigInt(999),
      sequenceNumber: BigInt(100),
    };
    writer = new ManifestListWriter(storage, options);

    const entry = createTestManifestEntry(1);
    // Remove sequence_number to test inheritance
    delete (entry as Partial<ManifestFileEntry>).sequence_number;

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-999/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].sequence_number).toBe(BigInt(100));
  });

  it('should generate monotonically increasing snapshot IDs', async () => {
    const writer1 = new ManifestListWriter(storage, { snapshotId: BigInt(100) });
    await writer1.addManifest(createTestManifestEntry(1));
    const result1 = await writer1.write('test/snap-100/manifest-list.avro');

    const writer2 = new ManifestListWriter(storage, { snapshotId: BigInt(101) });
    await writer2.addManifest(createTestManifestEntry(2));
    const result2 = await writer2.write('test/snap-101/manifest-list.avro');

    expect(result2.metadata.snapshotId).toBeGreaterThan(result1.metadata.snapshotId);
  });
});

// ============================================================================
// 3. Added/Deleted File Counts
// ============================================================================

describe('ManifestListWriter - Added/Deleted File Counts', () => {
  let writer: ManifestListWriter;
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
    writer = new ManifestListWriter(storage);
  });

  afterEach(async () => {
    if (writer) {
      await writer.close().catch(() => {});
    }
  });

  it('should track added_data_files_count per manifest', async () => {
    const entry = createTestManifestEntry(1, {
      added_data_files_count: 150,
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].added_data_files_count).toBe(150);
  });

  it('should track existing_data_files_count per manifest', async () => {
    const entry = createTestManifestEntry(1, {
      existing_data_files_count: 75,
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].existing_data_files_count).toBe(75);
  });

  it('should track deleted_data_files_count per manifest', async () => {
    const entry = createTestManifestEntry(1, {
      deleted_data_files_count: 25,
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].deleted_data_files_count).toBe(25);
  });

  it('should track added_rows_count per manifest', async () => {
    const entry = createTestManifestEntry(1, {
      added_rows_count: BigInt(100000),
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].added_rows_count).toBe(BigInt(100000));
  });

  it('should track existing_rows_count per manifest', async () => {
    const entry = createTestManifestEntry(1, {
      existing_rows_count: BigInt(50000),
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].existing_rows_count).toBe(BigInt(50000));
  });

  it('should track deleted_rows_count per manifest', async () => {
    const entry = createTestManifestEntry(1, {
      deleted_rows_count: BigInt(10000),
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].deleted_rows_count).toBe(BigInt(10000));
  });

  it('should compute total file counts across all manifests', async () => {
    await writer.addManifest(createTestManifestEntry(1, {
      added_data_files_count: 100,
      existing_data_files_count: 50,
      deleted_data_files_count: 10,
    }));
    await writer.addManifest(createTestManifestEntry(2, {
      added_data_files_count: 200,
      existing_data_files_count: 100,
      deleted_data_files_count: 20,
    }));

    const stats = writer.getStatistics();
    expect(stats.totalAddedDataFiles).toBe(300);
    expect(stats.totalExistingDataFiles).toBe(150);
    expect(stats.totalDeletedDataFiles).toBe(30);
  });

  it('should compute total row counts across all manifests', async () => {
    await writer.addManifest(createTestManifestEntry(1, {
      added_rows_count: BigInt(10000),
      existing_rows_count: BigInt(5000),
      deleted_rows_count: BigInt(1000),
    }));
    await writer.addManifest(createTestManifestEntry(2, {
      added_rows_count: BigInt(20000),
      existing_rows_count: BigInt(10000),
      deleted_rows_count: BigInt(2000),
    }));

    const stats = writer.getStatistics();
    expect(stats.totalAddedRows).toBe(BigInt(30000));
    expect(stats.totalExistingRows).toBe(BigInt(15000));
    expect(stats.totalDeletedRows).toBe(BigInt(3000));
  });

  it('should validate file counts are non-negative', async () => {
    const entry = createTestManifestEntry(1, {
      added_data_files_count: -1,
    });

    await expect(writer.addManifest(entry)).rejects.toThrow(/added_data_files_count.*negative/i);
  });

  it('should validate row counts are non-negative', async () => {
    const entry = createTestManifestEntry(1, {
      added_rows_count: BigInt(-1),
    });

    await expect(writer.addManifest(entry)).rejects.toThrow(/added_rows_count.*negative/i);
  });

  it('should handle zero counts correctly', async () => {
    const entry = createTestManifestEntry(1, {
      added_data_files_count: 0,
      existing_data_files_count: 0,
      deleted_data_files_count: 0,
      added_rows_count: BigInt(0),
      existing_rows_count: BigInt(0),
      deleted_rows_count: BigInt(0),
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].added_data_files_count).toBe(0);
    expect(entries[0].added_rows_count).toBe(BigInt(0));
  });
});

// ============================================================================
// 4. Partition Field Summaries
// ============================================================================

describe('ManifestListWriter - Partition Field Summaries', () => {
  let writer: ManifestListWriter;
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
    writer = new ManifestListWriter(storage);
  });

  afterEach(async () => {
    if (writer) {
      await writer.close().catch(() => {});
    }
  });

  it('should store partition field summaries with contains_null', async () => {
    const entry = createTestManifestEntry(1, {
      partitions: [
        createTestPartitionSummary({ contains_null: true }),
        createTestPartitionSummary({ contains_null: false }),
      ],
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].partitions).toHaveLength(2);
    expect(entries[0].partitions![0].contains_null).toBe(true);
    expect(entries[0].partitions![1].contains_null).toBe(false);
  });

  it('should store partition field summaries with contains_nan', async () => {
    const entry = createTestManifestEntry(1, {
      partitions: [
        createTestPartitionSummary({ contains_nan: true }),
        createTestPartitionSummary({ contains_nan: false }),
      ],
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].partitions![0].contains_nan).toBe(true);
    expect(entries[0].partitions![1].contains_nan).toBe(false);
  });

  it('should store partition field summaries with lower_bound', async () => {
    const lowerBound = new TextEncoder().encode('2024-01-01');
    const entry = createTestManifestEntry(1, {
      partitions: [
        createTestPartitionSummary({ lower_bound: lowerBound }),
      ],
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].partitions![0].lower_bound).toEqual(lowerBound);
  });

  it('should store partition field summaries with upper_bound', async () => {
    const upperBound = new TextEncoder().encode('2024-12-31');
    const entry = createTestManifestEntry(1, {
      partitions: [
        createTestPartitionSummary({ upper_bound: upperBound }),
      ],
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].partitions![0].upper_bound).toEqual(upperBound);
  });

  it('should handle multiple partition fields', async () => {
    const entry = createTestManifestEntry(1, {
      partitions: [
        createTestPartitionSummary({
          contains_null: false,
          lower_bound: new TextEncoder().encode('A'),
          upper_bound: new TextEncoder().encode('Z'),
        }),
        createTestPartitionSummary({
          contains_null: true,
          lower_bound: new TextEncoder().encode('2024-01-01'),
          upper_bound: new TextEncoder().encode('2024-12-31'),
        }),
        createTestPartitionSummary({
          contains_null: false,
          contains_nan: true,
        }),
      ],
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].partitions).toHaveLength(3);
  });

  it('should handle manifest with no partitions (null partitions)', async () => {
    const entry = createTestManifestEntry(1, {
      partitions: undefined,
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].partitions).toBeUndefined();
  });

  it('should handle manifest with empty partitions array', async () => {
    const entry = createTestManifestEntry(1, {
      partitions: [],
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].partitions).toEqual([]);
  });

  it('should handle partition with null bounds', async () => {
    const entry = createTestManifestEntry(1, {
      partitions: [
        createTestPartitionSummary({
          contains_null: true,
          lower_bound: undefined,
          upper_bound: undefined,
        }),
      ],
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].partitions![0].lower_bound).toBeUndefined();
    expect(entries[0].partitions![0].upper_bound).toBeUndefined();
  });

  it('should store binary partition bounds correctly', async () => {
    const intBound = new Uint8Array([0x00, 0x00, 0x00, 0x01]); // int 1 in big-endian
    const entry = createTestManifestEntry(1, {
      partitions: [
        createTestPartitionSummary({
          lower_bound: intBound,
          upper_bound: new Uint8Array([0x00, 0x00, 0x00, 0xFF]),
        }),
      ],
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].partitions![0].lower_bound).toEqual(intBound);
  });
});

// ============================================================================
// 5. Avro Format Compliance
// ============================================================================

describe('ManifestListWriter - Avro Format Compliance', () => {
  let writer: ManifestListWriter;
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
    writer = new ManifestListWriter(storage);
  });

  afterEach(async () => {
    if (writer) {
      await writer.close().catch(() => {});
    }
  });

  it('should write valid Avro file with magic bytes', async () => {
    await writer.addManifest(createTestManifestEntry(1));
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const data = await storage.get(result.path);
    expect(data).not.toBeNull();

    // Avro files start with "Obj" followed by version byte (1)
    const magic = new TextDecoder().decode(data!.slice(0, 3));
    expect(magic).toBe('Obj');
    expect(data![3]).toBe(1);
  });

  it('should include Avro schema in file metadata', async () => {
    await writer.addManifest(createTestManifestEntry(1));
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const schema = result.avroSchema;
    expect(schema).toBeDefined();
    expect(schema.type).toBe('record');
    expect(schema.name).toBe('manifest_file');
  });

  it('should include iceberg schema in Avro metadata', async () => {
    const options: ManifestListWriterOptions = {
      tableSchema: {
        type: 'struct',
        fields: [
          { id: 1, name: 'id', required: true, type: 'long' },
          { id: 2, name: 'data', required: false, type: 'string' },
        ],
      },
    };
    writer = new ManifestListWriter(storage, options);

    await writer.addManifest(createTestManifestEntry(1));
    const result = await writer.write('test/snap-1/manifest-list.avro');

    expect(result.metadata.schema).toBeDefined();
  });

  it('should include format-version in Avro metadata', async () => {
    const options: ManifestListWriterOptions = {
      formatVersion: 2,
    };
    writer = new ManifestListWriter(storage, options);

    await writer.addManifest(createTestManifestEntry(1));
    const result = await writer.write('test/snap-1/manifest-list.avro');

    expect(result.metadata.formatVersion).toBe(2);
  });

  it('should write manifest entries as Avro records', async () => {
    await writer.addManifest(createTestManifestEntry(1));
    await writer.addManifest(createTestManifestEntry(2));
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries).toHaveLength(2);
  });

  it('should use correct Avro types for long fields (bigint)', async () => {
    const entry = createTestManifestEntry(1, {
      sequence_number: BigInt('9007199254740993'), // > MAX_SAFE_INTEGER
      added_rows_count: BigInt('9007199254740993'),
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].sequence_number).toBe(BigInt('9007199254740993'));
    expect(entries[0].added_rows_count).toBe(BigInt('9007199254740993'));
  });

  it('should handle optional fields as Avro union with null', async () => {
    const entry = createTestManifestEntry(1, {
      partitions: undefined,
      key_metadata: undefined,
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const schema = result.avroSchema;
    const partitionsField = schema.fields.find((f: { name: string }) => f.name === 'partitions');
    expect(partitionsField.type).toContain('null');
  });

  it('should write file with .avro extension', async () => {
    await writer.addManifest(createTestManifestEntry(1));
    const result = await writer.write('test/snap-1/manifest-list.avro');

    expect(result.path.endsWith('.avro')).toBe(true);
  });

  it('should use snappy compression by default', async () => {
    await writer.addManifest(createTestManifestEntry(1));
    const result = await writer.write('test/snap-1/manifest-list.avro');

    expect(result.metadata.codec).toBe('snappy');
  });

  it('should support configurable compression codec', async () => {
    const options: ManifestListWriterOptions = {
      codec: 'deflate',
    };
    writer = new ManifestListWriter(storage, options);

    await writer.addManifest(createTestManifestEntry(1));
    const result = await writer.write('test/snap-1/manifest-list.avro');

    expect(result.metadata.codec).toBe('deflate');
  });
});

// ============================================================================
// 6. Content Type Handling
// ============================================================================

describe('ManifestListWriter - Content Type Handling', () => {
  let writer: ManifestListWriter;
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
    writer = new ManifestListWriter(storage);
  });

  afterEach(async () => {
    if (writer) {
      await writer.close().catch(() => {});
    }
  });

  it('should differentiate DATA manifests from DELETE manifests', async () => {
    const dataEntry = createTestManifestEntry(1, {
      content: MANIFEST_CONTENT_DATA,
      manifest_path: 's3://bucket/data-manifest.avro',
    });
    const deleteEntry = createTestManifestEntry(2, {
      content: MANIFEST_CONTENT_DELETES,
      manifest_path: 's3://bucket/delete-manifest.avro',
    });

    await writer.addManifest(dataEntry);
    await writer.addManifest(deleteEntry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    const dataManifest = entries.find(e => e.manifest_path.includes('data-manifest'));
    const deleteManifest = entries.find(e => e.manifest_path.includes('delete-manifest'));

    expect(dataManifest?.content).toBe(MANIFEST_CONTENT_DATA);
    expect(deleteManifest?.content).toBe(MANIFEST_CONTENT_DELETES);
  });

  it('should track counts for DATA content type manifests', async () => {
    await writer.addManifest(createTestManifestEntry(1, {
      content: MANIFEST_CONTENT_DATA,
      added_data_files_count: 100,
    }));
    await writer.addManifest(createTestManifestEntry(2, {
      content: MANIFEST_CONTENT_DATA,
      added_data_files_count: 200,
    }));

    const stats = writer.getStatistics();
    expect(stats.dataManifestCount).toBe(2);
    expect(stats.totalAddedDataFiles).toBe(300);
  });

  it('should track counts for DELETES content type manifests', async () => {
    await writer.addManifest(createTestManifestEntry(1, {
      content: MANIFEST_CONTENT_DELETES,
      added_data_files_count: 50, // Delete files count
    }));
    await writer.addManifest(createTestManifestEntry(2, {
      content: MANIFEST_CONTENT_DELETES,
      added_data_files_count: 75,
    }));

    const stats = writer.getStatistics();
    expect(stats.deleteManifestCount).toBe(2);
  });

  it('should process delete manifests before data manifests during planning', async () => {
    // Delete manifests should be listed first in manifest list for efficient query planning
    await writer.addManifest(createTestManifestEntry(1, {
      content: MANIFEST_CONTENT_DATA,
      manifest_path: 's3://bucket/data-1.avro',
    }));
    await writer.addManifest(createTestManifestEntry(2, {
      content: MANIFEST_CONTENT_DELETES,
      manifest_path: 's3://bucket/delete-1.avro',
    }));

    const result = await writer.write('test/snap-1/manifest-list.avro', {
      sortDeletesFirst: true,
    });

    const entries = await writer.readManifestList(result.path);
    // Delete manifests should come first
    expect(entries[0].content).toBe(MANIFEST_CONTENT_DELETES);
    expect(entries[1].content).toBe(MANIFEST_CONTENT_DATA);
  });

  it('should handle mixed content types correctly', async () => {
    await writer.addManifest(createTestManifestEntry(1, {
      content: MANIFEST_CONTENT_DATA,
      added_data_files_count: 100,
      added_rows_count: BigInt(10000),
    }));
    await writer.addManifest(createTestManifestEntry(2, {
      content: MANIFEST_CONTENT_DELETES,
      added_data_files_count: 20,
      added_rows_count: BigInt(500),
    }));
    await writer.addManifest(createTestManifestEntry(3, {
      content: MANIFEST_CONTENT_DATA,
      added_data_files_count: 150,
      added_rows_count: BigInt(15000),
    }));

    const stats = writer.getStatistics();
    expect(stats.dataManifestCount).toBe(2);
    expect(stats.deleteManifestCount).toBe(1);
    expect(stats.totalManifestCount).toBe(3);
  });
});

// ============================================================================
// 7. Key Metadata Support
// ============================================================================

describe('ManifestListWriter - Key Metadata Support', () => {
  let writer: ManifestListWriter;
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
    writer = new ManifestListWriter(storage);
  });

  afterEach(async () => {
    if (writer) {
      await writer.close().catch(() => {});
    }
  });

  it('should store key_metadata as binary field', async () => {
    const keyMetadata = new Uint8Array([0x01, 0x02, 0x03, 0x04, 0x05]);
    const entry = createTestManifestEntry(1, {
      key_metadata: keyMetadata,
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].key_metadata).toEqual(keyMetadata);
  });

  it('should handle null key_metadata', async () => {
    const entry = createTestManifestEntry(1, {
      key_metadata: undefined,
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].key_metadata).toBeUndefined();
  });

  it('should support encryption key reference in key_metadata', async () => {
    // Key metadata is typically used to store encryption key information
    const keyReference = new TextEncoder().encode('key-id-12345');
    const entry = createTestManifestEntry(1, {
      key_metadata: keyReference,
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    const decodedKey = new TextDecoder().decode(entries[0].key_metadata);
    expect(decodedKey).toBe('key-id-12345');
  });

  it('should preserve key_metadata across read/write cycles', async () => {
    const originalKey = new Uint8Array(256);
    for (let i = 0; i < 256; i++) {
      originalKey[i] = i;
    }

    const entry = createTestManifestEntry(1, {
      key_metadata: originalKey,
    });

    await writer.addManifest(entry);
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].key_metadata).toEqual(originalKey);
  });
});

// ============================================================================
// 8. Writer Lifecycle and Error Handling
// ============================================================================

describe('ManifestListWriter - Lifecycle and Error Handling', () => {
  let writer: ManifestListWriter;
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
    writer = new ManifestListWriter(storage);
  });

  afterEach(async () => {
    if (writer) {
      await writer.close().catch(() => {});
    }
  });

  it('should track writer state', async () => {
    expect(writer.getState()).toBe('initialized');

    await writer.addManifest(createTestManifestEntry(1));
    expect(writer.getState()).toBe('writing');

    await writer.write('test/snap-1/manifest-list.avro');
    expect(writer.getState()).toBe('written');

    await writer.close();
    expect(writer.getState()).toBe('closed');
  });

  it('should prevent adding manifests after write', async () => {
    await writer.addManifest(createTestManifestEntry(1));
    await writer.write('test/snap-1/manifest-list.avro');

    await expect(writer.addManifest(createTestManifestEntry(2)))
      .rejects.toThrow(/already written/i);
  });

  it('should prevent writing twice', async () => {
    await writer.addManifest(createTestManifestEntry(1));
    await writer.write('test/snap-1/manifest-list.avro');

    await expect(writer.write('test/snap-2/manifest-list.avro'))
      .rejects.toThrow(/already written/i);
  });

  it('should handle empty manifest list', async () => {
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries).toHaveLength(0);
  });

  it('should handle storage write failure', async () => {
    const failingStorage = {
      ...storage,
      put: vi.fn().mockRejectedValue(new Error('Storage write failed')),
    } as unknown as StorageBackend;

    const failingWriter = new ManifestListWriter(failingStorage);
    await failingWriter.addManifest(createTestManifestEntry(1));

    await expect(failingWriter.write('test/snap-1/manifest-list.avro'))
      .rejects.toThrow(/storage write failed/i);
  });

  it('should handle storage read failure', async () => {
    await writer.addManifest(createTestManifestEntry(1));
    await writer.write('test/snap-1/manifest-list.avro');

    const failingStorage = {
      ...storage,
      get: vi.fn().mockRejectedValue(new Error('Storage read failed')),
    } as unknown as StorageBackend;

    const failingWriter = new ManifestListWriter(failingStorage);

    await expect(failingWriter.readManifestList('test/snap-1/manifest-list.avro'))
      .rejects.toThrow(/storage read failed/i);
  });

  it('should provide statistics after write', async () => {
    await writer.addManifest(createTestManifestEntry(1, {
      added_data_files_count: 100,
      added_rows_count: BigInt(10000),
    }));
    await writer.addManifest(createTestManifestEntry(2, {
      added_data_files_count: 200,
      added_rows_count: BigInt(20000),
    }));
    await writer.write('test/snap-1/manifest-list.avro');

    const stats = writer.getStatistics();
    expect(stats.totalManifestCount).toBe(2);
    expect(stats.totalAddedDataFiles).toBe(300);
    expect(stats.totalAddedRows).toBe(BigInt(30000));
    expect(stats.bytesWritten).toBeGreaterThan(0);
  });

  it('should close idempotently', async () => {
    await writer.addManifest(createTestManifestEntry(1));
    await writer.write('test/snap-1/manifest-list.avro');

    await expect(writer.close()).resolves.not.toThrow();
    await expect(writer.close()).resolves.not.toThrow();
  });
});

// ============================================================================
// 9. Multiple Manifests and Large Files
// ============================================================================

describe('ManifestListWriter - Multiple Manifests and Scale', () => {
  let writer: ManifestListWriter;
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
    writer = new ManifestListWriter(storage);
  });

  afterEach(async () => {
    if (writer) {
      await writer.close().catch(() => {});
    }
  });

  it('should handle many manifest entries', async () => {
    for (let i = 0; i < 100; i++) {
      await writer.addManifest(createTestManifestEntry(i));
    }

    const result = await writer.write('test/snap-1/manifest-list.avro');
    const entries = await writer.readManifestList(result.path);

    expect(entries).toHaveLength(100);
  });

  it('should maintain manifest order', async () => {
    for (let i = 0; i < 10; i++) {
      await writer.addManifest(createTestManifestEntry(i, {
        manifest_path: `s3://bucket/manifest-${i.toString().padStart(3, '0')}.avro`,
      }));
    }

    const result = await writer.write('test/snap-1/manifest-list.avro');
    const entries = await writer.readManifestList(result.path);

    for (let i = 0; i < 10; i++) {
      expect(entries[i].manifest_path).toContain(`manifest-${i.toString().padStart(3, '0')}`);
    }
  });

  it('should handle manifests with diverse partition summaries', async () => {
    for (let i = 0; i < 10; i++) {
      await writer.addManifest(createTestManifestEntry(i, {
        partitions: Array.from({ length: i + 1 }, (_, j) =>
          createTestPartitionSummary({
            contains_null: j % 2 === 0,
            lower_bound: new TextEncoder().encode(`${j}-lower`),
            upper_bound: new TextEncoder().encode(`${j}-upper`),
          })
        ),
      }));
    }

    const result = await writer.write('test/snap-1/manifest-list.avro');
    const entries = await writer.readManifestList(result.path);

    expect(entries[0].partitions).toHaveLength(1);
    expect(entries[9].partitions).toHaveLength(10);
  });

  it('should efficiently serialize large manifest lists', async () => {
    const startTime = Date.now();

    for (let i = 0; i < 1000; i++) {
      await writer.addManifest(createTestManifestEntry(i));
    }

    const result = await writer.write('test/snap-1/manifest-list.avro');
    const endTime = Date.now();

    // Should complete in reasonable time (< 5 seconds)
    expect(endTime - startTime).toBeLessThan(5000);
    expect(result.metadata.manifestCount).toBe(1000);
  });

  it('should compute accurate statistics for large manifest lists', async () => {
    let expectedFiles = 0;
    let expectedRows = BigInt(0);

    for (let i = 0; i < 100; i++) {
      const files = (i + 1) * 10;
      const rows = BigInt((i + 1) * 1000);
      expectedFiles += files;
      expectedRows += rows;

      await writer.addManifest(createTestManifestEntry(i, {
        added_data_files_count: files,
        added_rows_count: rows,
      }));
    }

    const stats = writer.getStatistics();
    expect(stats.totalAddedDataFiles).toBe(expectedFiles);
    expect(stats.totalAddedRows).toBe(expectedRows);
  });
});

// ============================================================================
// 10. Integration with Table Metadata
// ============================================================================

describe('ManifestListWriter - Table Metadata Integration', () => {
  let writer: ManifestListWriter;
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
  });

  afterEach(async () => {
    if (writer) {
      await writer.close().catch(() => {});
    }
  });

  it('should include table schema reference in manifest list', async () => {
    const options: ManifestListWriterOptions = {
      schemaId: 1,
    };
    writer = new ManifestListWriter(storage, options);

    await writer.addManifest(createTestManifestEntry(1));
    const result = await writer.write('test/snap-1/manifest-list.avro');

    expect(result.metadata.schemaId).toBe(1);
  });

  it('should include partition spec reference in manifest entries', async () => {
    writer = new ManifestListWriter(storage);
    await writer.addManifest(createTestManifestEntry(1, {
      partition_spec_id: 0,
    }));
    await writer.addManifest(createTestManifestEntry(2, {
      partition_spec_id: 1, // Different partition spec (schema evolution)
    }));

    const result = await writer.write('test/snap-1/manifest-list.avro');

    const entries = await writer.readManifestList(result.path);
    expect(entries[0].partition_spec_id).toBe(0);
    expect(entries[1].partition_spec_id).toBe(1);
  });

  it('should support custom Avro metadata properties', async () => {
    const options: ManifestListWriterOptions = {
      customMetadata: {
        'mongolake.version': '0.1.0',
        'mongolake.created_by': 'compaction',
      },
    };
    writer = new ManifestListWriter(storage, options);

    await writer.addManifest(createTestManifestEntry(1));
    const result = await writer.write('test/snap-1/manifest-list.avro');

    expect(result.metadata.customProperties?.['mongolake.version']).toBe('0.1.0');
    expect(result.metadata.customProperties?.['mongolake.created_by']).toBe('compaction');
  });

  it('should generate path in Iceberg metadata directory structure', async () => {
    const options: ManifestListWriterOptions = {
      snapshotId: BigInt(12345),
    };
    writer = new ManifestListWriter(storage, options);

    await writer.addManifest(createTestManifestEntry(1));
    const result = await writer.write();

    // Path should follow Iceberg convention: metadata/snap-{snapshot_id}-{uuid}.avro
    expect(result.path).toMatch(/metadata\/snap-\d+-[a-f0-9-]+\.avro/);
  });

  it('should include timestamp in manifest list metadata', async () => {
    const beforeWrite = Date.now();

    writer = new ManifestListWriter(storage);
    await writer.addManifest(createTestManifestEntry(1));
    const result = await writer.write('test/snap-1/manifest-list.avro');

    const afterWrite = Date.now();

    expect(result.metadata.timestampMs).toBeGreaterThanOrEqual(beforeWrite);
    expect(result.metadata.timestampMs).toBeLessThanOrEqual(afterWrite);
  });

  it('should track format version for Iceberg spec compliance', async () => {
    const options: ManifestListWriterOptions = {
      formatVersion: 2,
    };
    writer = new ManifestListWriter(storage, options);

    await writer.addManifest(createTestManifestEntry(1));
    const result = await writer.write('test/snap-1/manifest-list.avro');

    expect(result.metadata.formatVersion).toBe(2);
  });

  it('should generate unique manifest list file names', async () => {
    const writer1 = new ManifestListWriter(storage, { snapshotId: BigInt(100) });
    await writer1.addManifest(createTestManifestEntry(1));
    const result1 = await writer1.write();

    const writer2 = new ManifestListWriter(storage, { snapshotId: BigInt(100) });
    await writer2.addManifest(createTestManifestEntry(2));
    const result2 = await writer2.write();

    expect(result1.path).not.toBe(result2.path);
  });
});
