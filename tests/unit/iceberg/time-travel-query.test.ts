/**
 * Time Travel Query Tests (TDD RED Phase)
 *
 * Tests for querying data at specific snapshots or timestamps using Iceberg time travel.
 * These tests should FAIL initially - they define the expected API.
 *
 * Requirements from mongolake-qkk.6.1:
 * - Query by snapshot ID
 * - Query by timestamp
 * - Snapshot history traversal
 * - As-of queries
 *
 * Iceberg Specification Reference:
 * https://iceberg.apache.org/spec/#time-travel
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { MemoryStorage } from '../../../src/storage/index.js';
import type { Document, MongoLakeConfig } from '../../../src/types.js';

// These imports will need to be implemented - TDD RED phase
// @ts-expect-error - TimeTravelReader does not exist yet
import {
  TimeTravelReader,
  type TimeTravelOptions,
  type TimeTravelResult,
  type SnapshotQueryResult,
} from '../../../src/iceberg/time-travel-reader.js';

// @ts-expect-error - Mock Iceberg types for testing
import type {
  Snapshot,
  TableMetadata,
  ManifestFile,
  ManifestEntry,
  DataFile,
} from '@dotdo/iceberg';

// ============================================================================
// Test Fixtures
// ============================================================================

interface TestDoc extends Document {
  _id: string;
  name: string;
  value: number;
  createdAt?: string;
}

function createTestStorage(): MemoryStorage {
  return new MemoryStorage();
}

/**
 * Helper to create mock table metadata with snapshots
 */
function createMockTableMetadata(snapshots: Partial<Snapshot>[]): TableMetadata {
  const fullSnapshots = snapshots.map((s, index) => ({
    'snapshot-id': s['snapshot-id'] ?? BigInt(1000 + index),
    // Use 'parent-snapshot-id' in s explicitly - check for undefined to allow explicit null
    'parent-snapshot-id': 'parent-snapshot-id' in s ? s['parent-snapshot-id'] : (index > 0 ? BigInt(999 + index) : null),
    'sequence-number': s['sequence-number'] ?? BigInt(index + 1),
    'timestamp-ms': s['timestamp-ms'] ?? Date.now() - (snapshots.length - index) * 86400000,
    'manifest-list': s['manifest-list'] ?? `metadata/snap-${1000 + index}-manifest-list.avro`,
    summary: s.summary ?? { operation: 'append' },
    'schema-id': 0,
  }));

  return {
    'format-version': 2,
    'table-uuid': '550e8400-e29b-41d4-a716-446655440000',
    location: 's3://bucket/warehouse/db/table',
    'last-sequence-number': BigInt(fullSnapshots.length),
    'last-updated-ms': Date.now(),
    'last-column-id': 3,
    'current-schema-id': 0,
    schemas: [
      {
        type: 'struct',
        'schema-id': 0,
        fields: [
          { id: 1, name: '_id', required: true, type: 'string' },
          { id: 2, name: 'name', required: true, type: 'string' },
          { id: 3, name: 'value', required: true, type: 'int' },
        ],
      },
    ],
    'default-spec-id': 0,
    'partition-specs': [{ 'spec-id': 0, fields: [] }],
    'last-partition-id': 999,
    'default-sort-order-id': 0,
    'sort-orders': [{ 'order-id': 0, fields: [] }],
    snapshots: fullSnapshots as Snapshot[],
    'current-snapshot-id': fullSnapshots[fullSnapshots.length - 1]?.['snapshot-id'] ?? null,
    'snapshot-log': fullSnapshots.map((s) => ({
      'timestamp-ms': s['timestamp-ms'],
      'snapshot-id': s['snapshot-id'],
    })),
    'metadata-log': [],
    refs: {
      main: {
        'snapshot-id': fullSnapshots[fullSnapshots.length - 1]?.['snapshot-id'] ?? 0n,
        type: 'branch',
      },
    },
    properties: {},
  } as unknown as TableMetadata;
}

/**
 * Custom JSON stringifier that handles BigInt
 */
function serializeWithBigInt(obj: unknown): string {
  return JSON.stringify(obj, (_key, value) =>
    typeof value === 'bigint' ? value.toString() : value
  );
}

/**
 * Helper to set up mock Iceberg metadata in storage
 */
async function setupMockIcebergMetadata(
  storage: MemoryStorage,
  dbName: string,
  collectionName: string,
  tableMetadata: TableMetadata
): Promise<void> {
  const metadataPath = `${dbName}/${collectionName}/_iceberg/metadata/v1.metadata.json`;
  await storage.put(metadataPath, new TextEncoder().encode(serializeWithBigInt(tableMetadata)));
}

/**
 * Helper to set up mock manifest files
 */
async function setupMockManifests(
  storage: MemoryStorage,
  dbName: string,
  collectionName: string,
  snapshotId: bigint,
  dataFiles: string[]
): Promise<void> {
  // Create manifest list
  const manifestListPath = `${dbName}/${collectionName}/_iceberg/metadata/snap-${snapshotId}-manifest-list.avro`;
  const manifestPath = `${dbName}/${collectionName}/_iceberg/metadata/manifest-${snapshotId}.json`;

  const manifestList = [
    {
      'manifest-path': manifestPath,
      'manifest-length': 1024,
      'partition-spec-id': 0,
      content: 0,
      'sequence-number': 1,
      'min-sequence-number': 1,
      'added-snapshot-id': snapshotId.toString(),
      'added-data-files-count': dataFiles.length,
      'existing-data-files-count': 0,
      'deleted-data-files-count': 0,
      'added-rows-count': 100,
      'existing-rows-count': 0,
      'deleted-rows-count': 0,
    },
  ];

  await storage.put(manifestListPath, new TextEncoder().encode(serializeWithBigInt(manifestList)));

  // Create manifest with entries
  const manifestEntries = {
    entries: dataFiles.map((filePath) => ({
      status: 1, // ADDED
      'snapshot-id': snapshotId.toString(),
      'sequence-number': 1,
      'data-file': {
        content: 0,
        'file-path': filePath,
        'file-format': 'PARQUET',
        partition: {},
        'record-count': 100,
        'file-size-in-bytes': 4096,
        'column-sizes': {},
        'value-counts': {},
        'null-value-counts': {},
        'nan-value-counts': {},
        'lower-bounds': {},
        'upper-bounds': {},
        'key-metadata': null,
        'split-offsets': null,
        'equality-ids': null,
        'sort-order-id': 0,
      },
    })),
  };

  await storage.put(manifestPath, new TextEncoder().encode(serializeWithBigInt(manifestEntries)));
}

// ============================================================================
// 1. Query by Snapshot ID
// ============================================================================

describe('TimeTravelReader - Query by Snapshot ID', () => {
  let storage: MemoryStorage;
  let reader: TimeTravelReader;

  beforeEach(async () => {
    storage = createTestStorage();
    reader = new TimeTravelReader(storage);
  });

  describe('basic snapshot ID queries', () => {
    it('should read data from a specific snapshot ID', async () => {
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, 'timestamp-ms': Date.now() - 86400000 },
        { 'snapshot-id': 1001n, 'timestamp-ms': Date.now() - 43200000 },
        { 'snapshot-id': 1002n, 'timestamp-ms': Date.now() },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1001n, [
        'testdb/users/data-1001-1.parquet',
      ]);

      // Write test data file
      await storage.put(
        'testdb/users/data-1001-1.parquet',
        new TextEncoder().encode('mock parquet data')
      );

      const result = await reader.readAtSnapshot('testdb', 'users', 1001n);

      expect(result.snapshotId).toBe(1001n);
      expect(result.dataFiles).toContain('testdb/users/data-1001-1.parquet');
    });

    it('should return only files that existed in the specified snapshot', async () => {
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n },
        { 'snapshot-id': 1001n },
        { 'snapshot-id': 1002n },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);

      // Snapshot 1000 has file-a
      await setupMockManifests(storage, 'testdb', 'users', 1000n, [
        'testdb/users/file-a.parquet',
      ]);

      // Snapshot 1001 has file-a and file-b
      await setupMockManifests(storage, 'testdb', 'users', 1001n, [
        'testdb/users/file-a.parquet',
        'testdb/users/file-b.parquet',
      ]);

      const result1000 = await reader.readAtSnapshot('testdb', 'users', 1000n);
      const result1001 = await reader.readAtSnapshot('testdb', 'users', 1001n);

      expect(result1000.dataFiles).toHaveLength(1);
      expect(result1000.dataFiles).toContain('testdb/users/file-a.parquet');
      expect(result1000.dataFiles).not.toContain('testdb/users/file-b.parquet');

      expect(result1001.dataFiles).toHaveLength(2);
      expect(result1001.dataFiles).toContain('testdb/users/file-a.parquet');
      expect(result1001.dataFiles).toContain('testdb/users/file-b.parquet');
    });

    it('should throw error for non-existent snapshot ID', async () => {
      const tableMetadata = createMockTableMetadata([{ 'snapshot-id': 1000n }]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);

      await expect(reader.readAtSnapshot('testdb', 'users', 9999n)).rejects.toThrow(
        /snapshot.*not found/i
      );
    });

    it('should handle snapshot with no data files', async () => {
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, summary: { operation: 'delete' } },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1000n, []);

      const result = await reader.readAtSnapshot('testdb', 'users', 1000n);

      expect(result.dataFiles).toHaveLength(0);
    });

    it('should include deleted files marker when querying snapshot after deletes', async () => {
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, summary: { operation: 'append' } },
        { 'snapshot-id': 1001n, summary: { operation: 'delete' } },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1001n, []);

      const result = await reader.readAtSnapshot('testdb', 'users', 1001n);

      // The snapshot should have delete metadata
      expect(result.snapshot?.summary?.operation).toBe('delete');
    });
  });

  describe('snapshot ID validation', () => {
    it('should accept bigint snapshot IDs', async () => {
      const largeId = 9007199254740993n;
      const tableMetadata = createMockTableMetadata([
        {
          'snapshot-id': largeId,
          'manifest-list': `metadata/snap-${largeId}-manifest-list.avro`, // Match setupMockManifests path
        }, // Larger than Number.MAX_SAFE_INTEGER
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', largeId, []);

      const result = await reader.readAtSnapshot('testdb', 'users', largeId);

      expect(result.snapshotId).toBe(largeId);
    });

    it('should accept number snapshot IDs and convert to bigint', async () => {
      const tableMetadata = createMockTableMetadata([{ 'snapshot-id': 1000n }]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1000n, []);

      // Pass number instead of bigint
      const result = await reader.readAtSnapshot('testdb', 'users', 1000 as unknown as bigint);

      expect(result.snapshotId).toBe(1000n);
    });

    it('should reject negative snapshot IDs', async () => {
      await expect(reader.readAtSnapshot('testdb', 'users', -1n)).rejects.toThrow(
        /invalid.*snapshot.*id/i
      );
    });
  });
});

// ============================================================================
// 2. Query by Timestamp
// ============================================================================

describe('TimeTravelReader - Query by Timestamp', () => {
  let storage: MemoryStorage;
  let reader: TimeTravelReader;

  beforeEach(async () => {
    storage = createTestStorage();
    reader = new TimeTravelReader(storage);
  });

  describe('basic timestamp queries', () => {
    it('should find snapshot at or before the given timestamp', async () => {
      const now = Date.now();
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, 'timestamp-ms': now - 86400000 }, // 1 day ago
        { 'snapshot-id': 1001n, 'timestamp-ms': now - 43200000 }, // 12 hours ago
        { 'snapshot-id': 1002n, 'timestamp-ms': now }, // now
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1001n, []);

      // Query at a time between snapshots 1001 and 1002
      const result = await reader.readAtTimestamp('testdb', 'users', now - 3600000); // 1 hour ago

      expect(result.snapshotId).toBe(1001n);
    });

    it('should return exact match when timestamp matches snapshot', async () => {
      const timestamp = Date.now() - 86400000;
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, 'timestamp-ms': timestamp },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1000n, []);

      const result = await reader.readAtTimestamp('testdb', 'users', timestamp);

      expect(result.snapshotId).toBe(1000n);
      expect(result.snapshot?.['timestamp-ms']).toBe(timestamp);
    });

    it('should return null/empty when timestamp is before all snapshots', async () => {
      const now = Date.now();
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, 'timestamp-ms': now - 86400000 },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);

      const result = await reader.readAtTimestamp('testdb', 'users', now - 172800000); // 2 days ago

      expect(result.snapshot).toBeNull();
      expect(result.dataFiles).toHaveLength(0);
    });

    it('should accept Date objects as timestamp', async () => {
      const timestamp = new Date('2024-06-15T12:00:00Z');
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, 'timestamp-ms': timestamp.getTime() - 3600000 },
        { 'snapshot-id': 1001n, 'timestamp-ms': timestamp.getTime() },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1001n, []);

      // @ts-expect-error - Testing Date input
      const result = await reader.readAtTimestamp('testdb', 'users', timestamp);

      expect(result.snapshotId).toBe(1001n);
    });

    it('should accept ISO string as timestamp', async () => {
      const isoString = '2024-06-15T12:00:00Z';
      const timestamp = Date.parse(isoString);
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, 'timestamp-ms': timestamp },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1000n, []);

      // @ts-expect-error - Testing string input
      const result = await reader.readAtTimestamp('testdb', 'users', isoString);

      expect(result.snapshotId).toBe(1000n);
    });
  });

  describe('timestamp boundary conditions', () => {
    it('should handle millisecond precision', async () => {
      const baseTime = Date.now();
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, 'timestamp-ms': baseTime },
        { 'snapshot-id': 1001n, 'timestamp-ms': baseTime + 1 }, // 1ms later
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1000n, []);
      await setupMockManifests(storage, 'testdb', 'users', 1001n, []);

      // Query at exactly baseTime should return snapshot 1000
      const result = await reader.readAtTimestamp('testdb', 'users', baseTime);
      expect(result.snapshotId).toBe(1000n);
    });

    it('should handle far future timestamps', async () => {
      const now = Date.now();
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, 'timestamp-ms': now },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1000n, []);

      // Query 1 year in the future should return latest snapshot
      const result = await reader.readAtTimestamp(
        'testdb',
        'users',
        now + 365 * 24 * 60 * 60 * 1000
      );

      expect(result.snapshotId).toBe(1000n);
    });

    it('should handle epoch timestamp (1970-01-01)', async () => {
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, 'timestamp-ms': Date.now() },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);

      const result = await reader.readAtTimestamp('testdb', 'users', 0); // Unix epoch

      expect(result.snapshot).toBeNull();
    });

    it('should reject invalid timestamps', async () => {
      await expect(reader.readAtTimestamp('testdb', 'users', -1)).rejects.toThrow(
        /invalid.*timestamp/i
      );

      await expect(reader.readAtTimestamp('testdb', 'users', NaN)).rejects.toThrow(
        /invalid.*timestamp/i
      );

      await expect(reader.readAtTimestamp('testdb', 'users', Infinity)).rejects.toThrow(
        /invalid.*timestamp/i
      );
    });
  });

  describe('as-of query semantics', () => {
    it('should return the most recent snapshot at or before timestamp', async () => {
      const now = Date.now();
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, 'timestamp-ms': now - 3600000 }, // -1hr
        { 'snapshot-id': 1001n, 'timestamp-ms': now - 3599000 }, // -59m 59s
        { 'snapshot-id': 1002n, 'timestamp-ms': now - 3598000 }, // -59m 58s
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1002n, []);

      // Query 30 minutes ago should return the most recent snapshot before that
      const result = await reader.readAtTimestamp('testdb', 'users', now - 1800000);

      expect(result.snapshotId).toBe(1002n);
    });

    it('asOf() should be an alias for readAtTimestamp()', async () => {
      const timestamp = Date.now();
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, 'timestamp-ms': timestamp },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1000n, []);

      const resultTimestamp = await reader.readAtTimestamp('testdb', 'users', timestamp);
      const resultAsOf = await reader.asOf('testdb', 'users', timestamp);

      expect(resultAsOf.snapshotId).toBe(resultTimestamp.snapshotId);
    });
  });
});

// ============================================================================
// 3. Snapshot History Traversal
// ============================================================================

describe('TimeTravelReader - Snapshot History Traversal', () => {
  let storage: MemoryStorage;
  let reader: TimeTravelReader;

  beforeEach(async () => {
    storage = createTestStorage();
    reader = new TimeTravelReader(storage);
  });

  describe('list snapshots', () => {
    it('should list all snapshots in chronological order', async () => {
      const now = Date.now();
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, 'timestamp-ms': now - 86400000 },
        { 'snapshot-id': 1001n, 'timestamp-ms': now - 43200000 },
        { 'snapshot-id': 1002n, 'timestamp-ms': now },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);

      const snapshots = await reader.listSnapshots('testdb', 'users');

      expect(snapshots).toHaveLength(3);
      expect(snapshots[0]['snapshot-id']).toBe(1000n);
      expect(snapshots[1]['snapshot-id']).toBe(1001n);
      expect(snapshots[2]['snapshot-id']).toBe(1002n);
    });

    it('should list snapshots in reverse chronological order when specified', async () => {
      const now = Date.now();
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, 'timestamp-ms': now - 86400000 },
        { 'snapshot-id': 1001n, 'timestamp-ms': now },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);

      const snapshots = await reader.listSnapshots('testdb', 'users', { reverse: true });

      expect(snapshots[0]['snapshot-id']).toBe(1001n);
      expect(snapshots[1]['snapshot-id']).toBe(1000n);
    });

    it('should support pagination with limit and offset', async () => {
      const tableMetadata = createMockTableMetadata(
        Array.from({ length: 10 }, (_, i) => ({
          'snapshot-id': BigInt(1000 + i),
          'timestamp-ms': Date.now() - (10 - i) * 3600000,
        }))
      );

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);

      const page1 = await reader.listSnapshots('testdb', 'users', { limit: 3, offset: 0 });
      const page2 = await reader.listSnapshots('testdb', 'users', { limit: 3, offset: 3 });

      expect(page1).toHaveLength(3);
      expect(page2).toHaveLength(3);
      expect(page1[0]['snapshot-id']).toBe(1000n);
      expect(page2[0]['snapshot-id']).toBe(1003n);
    });

    it('should return empty array when no snapshots exist', async () => {
      // Create metadata with no snapshots
      const tableMetadata = {
        ...createMockTableMetadata([]),
        snapshots: [],
        'current-snapshot-id': null,
      } as unknown as TableMetadata;

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);

      const snapshots = await reader.listSnapshots('testdb', 'users');

      expect(snapshots).toHaveLength(0);
    });
  });

  describe('snapshot ancestry', () => {
    it('should get parent snapshot', async () => {
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, 'parent-snapshot-id': null },
        { 'snapshot-id': 1001n, 'parent-snapshot-id': 1000n },
        { 'snapshot-id': 1002n, 'parent-snapshot-id': 1001n },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);

      const parent = await reader.getParentSnapshot('testdb', 'users', 1002n);

      expect(parent?.['snapshot-id']).toBe(1001n);
    });

    it('should return null for first snapshot (no parent)', async () => {
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, 'parent-snapshot-id': null },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);

      const parent = await reader.getParentSnapshot('testdb', 'users', 1000n);

      expect(parent).toBeNull();
    });

    it('should get full ancestry chain', async () => {
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, 'parent-snapshot-id': null },
        { 'snapshot-id': 1001n, 'parent-snapshot-id': 1000n },
        { 'snapshot-id': 1002n, 'parent-snapshot-id': 1001n },
        { 'snapshot-id': 1003n, 'parent-snapshot-id': 1002n },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);

      const ancestry = await reader.getSnapshotAncestry('testdb', 'users', 1003n);

      expect(ancestry).toHaveLength(3);
      expect(ancestry.map((s) => s['snapshot-id'])).toEqual([1002n, 1001n, 1000n]);
    });

    it('should limit ancestry depth when specified', async () => {
      const tableMetadata = createMockTableMetadata(
        Array.from({ length: 10 }, (_, i) => ({
          'snapshot-id': BigInt(1000 + i),
          'parent-snapshot-id': i > 0 ? BigInt(999 + i) : null,
        }))
      );

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);

      const ancestry = await reader.getSnapshotAncestry('testdb', 'users', 1009n, {
        maxDepth: 3,
      });

      expect(ancestry).toHaveLength(3);
    });
  });

  describe('snapshot log', () => {
    it('should get snapshot log (history)', async () => {
      const now = Date.now();
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, 'timestamp-ms': now - 86400000 },
        { 'snapshot-id': 1001n, 'timestamp-ms': now },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);

      const log = await reader.getSnapshotLog('testdb', 'users');

      expect(log).toHaveLength(2);
      expect(log[0]['snapshot-id']).toBe(1000n);
      expect(log[1]['snapshot-id']).toBe(1001n);
    });

    it('should include timestamp in snapshot log entries', async () => {
      const timestamp = Date.now();
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, 'timestamp-ms': timestamp },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);

      const log = await reader.getSnapshotLog('testdb', 'users');

      expect(log[0]['timestamp-ms']).toBe(timestamp);
    });
  });

  describe('branching support', () => {
    it('should find common ancestor between two snapshots', async () => {
      // Create a branching history:
      // 1000 -> 1001 -> 1002
      //      \-> 1003 -> 1004
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, 'parent-snapshot-id': null },
        { 'snapshot-id': 1001n, 'parent-snapshot-id': 1000n },
        { 'snapshot-id': 1002n, 'parent-snapshot-id': 1001n },
        { 'snapshot-id': 1003n, 'parent-snapshot-id': 1000n }, // Branch point
        { 'snapshot-id': 1004n, 'parent-snapshot-id': 1003n },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);

      const commonAncestor = await reader.findCommonAncestor(
        'testdb',
        'users',
        1002n,
        1004n
      );

      expect(commonAncestor?.['snapshot-id']).toBe(1000n);
    });

    it('should handle linear ancestry (common ancestor is one of the snapshots)', async () => {
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, 'parent-snapshot-id': null },
        { 'snapshot-id': 1001n, 'parent-snapshot-id': 1000n },
        { 'snapshot-id': 1002n, 'parent-snapshot-id': 1001n },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);

      const commonAncestor = await reader.findCommonAncestor(
        'testdb',
        'users',
        1001n,
        1002n
      );

      expect(commonAncestor?.['snapshot-id']).toBe(1001n);
    });

    it('should return null when snapshots have no common ancestor', async () => {
      // Two separate tables would have no common ancestor
      // For this test, we simulate with a broken chain
      const tableMetadata = {
        ...createMockTableMetadata([
          { 'snapshot-id': 1000n, 'parent-snapshot-id': null },
          { 'snapshot-id': 2000n, 'parent-snapshot-id': null }, // Different root
        ]),
      } as unknown as TableMetadata;

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);

      const commonAncestor = await reader.findCommonAncestor(
        'testdb',
        'users',
        1000n,
        2000n
      );

      expect(commonAncestor).toBeNull();
    });
  });
});

// ============================================================================
// 4. As-Of Queries (Combined with Collection API)
// ============================================================================

describe('TimeTravelReader - As-Of Queries with Documents', () => {
  let storage: MemoryStorage;
  let reader: TimeTravelReader;

  beforeEach(async () => {
    storage = createTestStorage();
    reader = new TimeTravelReader(storage);
  });

  describe('reading documents at snapshot', () => {
    it('should read documents that existed at the snapshot', async () => {
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n },
        { 'snapshot-id': 1001n },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1000n, [
        'testdb/users/data-1000.parquet',
      ]);

      // This would need actual parquet reading - for now we're testing the interface
      const result = await reader.readDocumentsAtSnapshot<TestDoc>(
        'testdb',
        'users',
        1000n
      );

      expect(result.snapshotId).toBe(1000n);
      expect(Array.isArray(result.documents)).toBe(true);
    });

    it('should apply filter when reading documents at snapshot', async () => {
      const tableMetadata = createMockTableMetadata([{ 'snapshot-id': 1000n }]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1000n, [
        'testdb/users/data-1000.parquet',
      ]);

      const result = await reader.readDocumentsAtSnapshot<TestDoc>(
        'testdb',
        'users',
        1000n,
        { name: 'Alice' }
      );

      expect(result.snapshotId).toBe(1000n);
      // Filter should be applied to documents
    });

    it('should support projection when reading documents', async () => {
      const tableMetadata = createMockTableMetadata([{ 'snapshot-id': 1000n }]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1000n, [
        'testdb/users/data-1000.parquet',
      ]);

      const result = await reader.readDocumentsAtSnapshot<TestDoc>(
        'testdb',
        'users',
        1000n,
        {},
        { projection: { name: 1, _id: 1 } }
      );

      expect(result.snapshotId).toBe(1000n);
      // Projection should be applied
    });
  });

  describe('diff between snapshots', () => {
    it('should compute diff between two snapshots', async () => {
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n },
        { 'snapshot-id': 1001n },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1000n, [
        'testdb/users/file-a.parquet',
      ]);
      await setupMockManifests(storage, 'testdb', 'users', 1001n, [
        'testdb/users/file-a.parquet',
        'testdb/users/file-b.parquet',
      ]);

      const diff = await reader.diffSnapshots('testdb', 'users', 1000n, 1001n);

      expect(diff.addedFiles).toContain('testdb/users/file-b.parquet');
      expect(diff.removedFiles).toHaveLength(0);
    });

    it('should identify removed files in diff', async () => {
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n },
        { 'snapshot-id': 1001n },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1000n, [
        'testdb/users/file-a.parquet',
        'testdb/users/file-b.parquet',
      ]);
      await setupMockManifests(storage, 'testdb', 'users', 1001n, [
        'testdb/users/file-a.parquet',
      ]);

      const diff = await reader.diffSnapshots('testdb', 'users', 1000n, 1001n);

      expect(diff.removedFiles).toContain('testdb/users/file-b.parquet');
    });

    it('should include snapshot metadata in diff result', async () => {
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, summary: { operation: 'append' } },
        { 'snapshot-id': 1001n, summary: { operation: 'delete' } },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1000n, []);
      await setupMockManifests(storage, 'testdb', 'users', 1001n, []);

      const diff = await reader.diffSnapshots('testdb', 'users', 1000n, 1001n);

      expect(diff.fromSnapshot['snapshot-id']).toBe(1000n);
      expect(diff.toSnapshot['snapshot-id']).toBe(1001n);
    });
  });

  describe('incremental reading', () => {
    it('should read only changes since a given snapshot', async () => {
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n },
        { 'snapshot-id': 1001n },
        { 'snapshot-id': 1002n },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);

      const changes = await reader.readChangesSince('testdb', 'users', 1000n);

      expect(changes.fromSnapshotId).toBe(1000n);
      expect(changes.toSnapshotId).toBe(1002n);
      expect(changes.intermediateSnapshots).toHaveLength(2); // 1001 and 1002
    });

    it('should support reading changes to a specific snapshot', async () => {
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n },
        { 'snapshot-id': 1001n },
        { 'snapshot-id': 1002n },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);

      const changes = await reader.readChangesSince('testdb', 'users', 1000n, {
        toSnapshotId: 1001n,
      });

      expect(changes.toSnapshotId).toBe(1001n);
      expect(changes.intermediateSnapshots).toHaveLength(1);
    });
  });
});

// ============================================================================
// 5. Edge Cases and Error Handling
// ============================================================================

describe('TimeTravelReader - Edge Cases', () => {
  let storage: MemoryStorage;
  let reader: TimeTravelReader;

  beforeEach(async () => {
    storage = createTestStorage();
    reader = new TimeTravelReader(storage);
  });

  describe('missing metadata', () => {
    it('should throw when table metadata does not exist', async () => {
      await expect(reader.readAtSnapshot('testdb', 'nonexistent', 1000n)).rejects.toThrow(
        /table.*not found|metadata.*not found/i
      );
    });

    it('should throw when manifest list is missing', async () => {
      const tableMetadata = createMockTableMetadata([{ 'snapshot-id': 1000n }]);
      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      // Don't set up manifests

      await expect(reader.readAtSnapshot('testdb', 'users', 1000n)).rejects.toThrow(
        /manifest.*not found/i
      );
    });

    it('should handle corrupted manifest files gracefully', async () => {
      const tableMetadata = createMockTableMetadata([{ 'snapshot-id': 1000n }]);
      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);

      // Write corrupted manifest
      const manifestListPath = 'testdb/users/_iceberg/metadata/snap-1000-manifest-list.avro';
      await storage.put(manifestListPath, new TextEncoder().encode('not valid json'));

      await expect(reader.readAtSnapshot('testdb', 'users', 1000n)).rejects.toThrow(
        /invalid.*manifest|parse.*error/i
      );
    });
  });

  describe('concurrent modifications', () => {
    it('should read consistent snapshot even during writes', async () => {
      const tableMetadata = createMockTableMetadata([{ 'snapshot-id': 1000n }]);
      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1000n, [
        'testdb/users/file-1.parquet',
      ]);

      // Start reading
      const readPromise = reader.readAtSnapshot('testdb', 'users', 1000n);

      // Simulate concurrent write (add new snapshot)
      const newMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n },
        { 'snapshot-id': 1001n },
      ]);
      await setupMockIcebergMetadata(storage, 'testdb', 'users', newMetadata);

      // Original read should still return snapshot 1000
      const result = await readPromise;
      expect(result.snapshotId).toBe(1000n);
    });
  });

  describe('large tables', () => {
    it('should handle tables with many snapshots', async () => {
      const snapshots = Array.from({ length: 1000 }, (_, i) => ({
        'snapshot-id': BigInt(1000 + i),
        'timestamp-ms': Date.now() - (1000 - i) * 60000,
      }));

      const tableMetadata = createMockTableMetadata(snapshots);
      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);

      // Should be able to query middle snapshot
      await setupMockManifests(storage, 'testdb', 'users', 1500n, []);
      const result = await reader.readAtSnapshot('testdb', 'users', 1500n);

      expect(result.snapshotId).toBe(1500n);
    });

    it('should handle tables with many data files', async () => {
      const dataFiles = Array.from({ length: 1000 }, (_, i) => `testdb/users/file-${i}.parquet`);

      const tableMetadata = createMockTableMetadata([{ 'snapshot-id': 1000n }]);
      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1000n, dataFiles);

      const result = await reader.readAtSnapshot('testdb', 'users', 1000n);

      expect(result.dataFiles).toHaveLength(1000);
    });
  });

  describe('expired snapshots', () => {
    it('should throw when querying an expired snapshot', async () => {
      // Simulate a table where old snapshots have been expired
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1002n }, // Only recent snapshot exists
        { 'snapshot-id': 1003n },
      ]);

      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);

      // Snapshot 1000 has been expired
      await expect(reader.readAtSnapshot('testdb', 'users', 1000n)).rejects.toThrow(
        /snapshot.*not found|expired/i
      );
    });
  });
});

// ============================================================================
// 6. Integration with Collection API
// ============================================================================

describe('TimeTravelReader - Collection API Integration', () => {
  let storage: MemoryStorage;
  let reader: TimeTravelReader;

  beforeEach(async () => {
    storage = createTestStorage();
    reader = new TimeTravelReader(storage);
  });

  describe('createTimeTravelCollection', () => {
    it('should create a read-only time travel collection view', async () => {
      const tableMetadata = createMockTableMetadata([{ 'snapshot-id': 1000n }]);
      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1000n, []);

      const ttCollection = await reader.createTimeTravelCollection<TestDoc>(
        'testdb',
        'users',
        { snapshotId: 1000n }
      );

      expect(ttCollection).toBeDefined();
      expect(ttCollection.name).toBe('users');
      expect(ttCollection.isReadOnly).toBe(true);
    });

    it('should create time travel collection by timestamp', async () => {
      const timestamp = Date.now();
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n, 'timestamp-ms': timestamp },
      ]);
      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1000n, []);

      const ttCollection = await reader.createTimeTravelCollection<TestDoc>(
        'testdb',
        'users',
        { timestamp }
      );

      expect(ttCollection).toBeDefined();
      const snapshot = await ttCollection.getSnapshot();
      expect(snapshot?.['snapshot-id']).toBe(1000n);
    });

    it('time travel collection should reject write operations', async () => {
      const tableMetadata = createMockTableMetadata([{ 'snapshot-id': 1000n }]);
      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1000n, []);

      const ttCollection = await reader.createTimeTravelCollection<TestDoc>(
        'testdb',
        'users',
        { snapshotId: 1000n }
      );

      // All write operations should throw
      await expect(ttCollection.insertOne({ name: 'test', value: 1 })).rejects.toThrow(
        /read.*only|write.*not.*allowed/i
      );
      await expect(
        ttCollection.updateOne({ name: 'test' }, { $set: { value: 2 } })
      ).rejects.toThrow(/read.*only|write.*not.*allowed/i);
      await expect(ttCollection.deleteOne({ name: 'test' })).rejects.toThrow(
        /read.*only|write.*not.*allowed/i
      );
    });
  });
});

// ============================================================================
// 7. Performance and Optimization
// ============================================================================

describe('TimeTravelReader - Performance', () => {
  let storage: MemoryStorage;
  let reader: TimeTravelReader;

  beforeEach(async () => {
    storage = createTestStorage();
    reader = new TimeTravelReader(storage);
  });

  describe('caching', () => {
    it('should cache table metadata', async () => {
      const tableMetadata = createMockTableMetadata([{ 'snapshot-id': 1000n }]);
      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1000n, []);

      // First read
      await reader.readAtSnapshot('testdb', 'users', 1000n);

      // Spy on storage.get to verify caching
      const getSpy = vi.spyOn(storage, 'get');

      // Second read should use cache
      await reader.readAtSnapshot('testdb', 'users', 1000n);

      // Should not have fetched metadata again
      expect(
        getSpy.mock.calls.filter((call) => call[0].includes('metadata.json')).length
      ).toBeLessThanOrEqual(1);
    });

    it('should invalidate cache when requested', async () => {
      const tableMetadata = createMockTableMetadata([{ 'snapshot-id': 1000n }]);
      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1000n, []);

      await reader.readAtSnapshot('testdb', 'users', 1000n);

      reader.invalidateCache('testdb', 'users');

      const getSpy = vi.spyOn(storage, 'get');
      await reader.readAtSnapshot('testdb', 'users', 1000n);

      // Should have fetched metadata again after cache invalidation
      expect(getSpy.mock.calls.some((call) => call[0].includes('metadata.json'))).toBe(true);
    });
  });

  describe('manifest pruning', () => {
    it('should only read relevant manifest files for snapshot', async () => {
      const tableMetadata = createMockTableMetadata([
        { 'snapshot-id': 1000n },
        { 'snapshot-id': 1001n },
        { 'snapshot-id': 1002n },
      ]);
      await setupMockIcebergMetadata(storage, 'testdb', 'users', tableMetadata);
      await setupMockManifests(storage, 'testdb', 'users', 1000n, ['file-1.parquet']);
      await setupMockManifests(storage, 'testdb', 'users', 1001n, ['file-2.parquet']);
      await setupMockManifests(storage, 'testdb', 'users', 1002n, ['file-3.parquet']);

      const getSpy = vi.spyOn(storage, 'get');

      await reader.readAtSnapshot('testdb', 'users', 1001n);

      // Should not have read manifest files for other snapshots
      const manifestCalls = getSpy.mock.calls.filter((call) =>
        call[0].includes('manifest-')
      );
      expect(manifestCalls.every((call) => call[0].includes('1001'))).toBe(true);
    });
  });
});
