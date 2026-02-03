/**
 * Iceberg Snapshot Manager Tests (TDD RED Phase)
 *
 * Tests for Iceberg-compatible snapshot management in MongoLake.
 * These tests should FAIL initially - they define the expected API.
 *
 * Requirements from mongolake-qkk.4.1:
 * - test snapshot creation
 * - test snapshot ID generation
 * - test parent tracking
 * - test summary stats
 * - test operation types (append/overwrite/delete)
 *
 * Iceberg Specification Reference:
 * https://iceberg.apache.org/spec/#snapshots
 *
 * A snapshot represents the state of a table at some point in time.
 * Each snapshot contains:
 * - snapshot-id: unique long ID
 * - parent-snapshot-id: ID of parent snapshot (null for first)
 * - sequence-number: monotonically increasing sequence number
 * - timestamp-ms: creation timestamp
 * - manifest-list: path to the manifest list
 * - summary: map of summary stats and operation type
 * - schema-id: ID of the schema used for the snapshot
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { MemoryStorage } from '../../../src/storage/index.js';
// Import SnapshotManager - the class under test (does not exist yet - TDD RED phase)
// @ts-expect-error - SnapshotManager does not exist yet
import {
  SnapshotManager,
  type Snapshot,
  type SnapshotSummary,
  type OperationType,
  type CreateSnapshotOptions,
  type SnapshotManagerConfig,
} from '../../../src/iceberg/snapshot-manager.js';

// ============================================================================
// Test Fixtures
// ============================================================================

function createTestStorage(): MemoryStorage {
  return new MemoryStorage();
}

async function createInitializedManager(
  storage: MemoryStorage,
  config?: SnapshotManagerConfig
): Promise<SnapshotManager> {
  const manager = new SnapshotManager(storage, 'test-table', config);
  await manager.initialize();
  return manager;
}

/**
 * Create a mock manifest list path for testing.
 */
function createManifestListPath(snapshotId: bigint): string {
  return `metadata/snap-${snapshotId}-manifest-list.avro`;
}

// ============================================================================
// 1. Snapshot Creation
// ============================================================================

describe('SnapshotManager - Snapshot Creation', () => {
  let storage: MemoryStorage;
  let manager: SnapshotManager;

  beforeEach(async () => {
    storage = createTestStorage();
    manager = await createInitializedManager(storage);
  });

  describe('basic snapshot creation', () => {
    it('should create a new snapshot with required fields', async () => {
      const manifestListPath = 'metadata/snap-1-manifest-list.avro';

      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath,
      });

      expect(snapshot).toBeDefined();
      expect(snapshot.snapshotId).toBeDefined();
      expect(snapshot.timestampMs).toBeDefined();
      expect(snapshot.manifestList).toBe(manifestListPath);
      expect(snapshot.summary).toBeDefined();
      expect(snapshot.summary.operation).toBe('append');
    });

    it('should create snapshot with timestamp close to current time', async () => {
      const beforeMs = Date.now();
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });
      const afterMs = Date.now();

      expect(snapshot.timestampMs).toBeGreaterThanOrEqual(beforeMs);
      expect(snapshot.timestampMs).toBeLessThanOrEqual(afterMs);
    });

    it('should allow custom timestamp', async () => {
      const customTimestamp = 1704067200000; // 2024-01-01 00:00:00 UTC

      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        timestampMs: customTimestamp,
      });

      expect(snapshot.timestampMs).toBe(customTimestamp);
    });

    it('should persist snapshot to storage', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      // Snapshot should be retrievable
      const retrieved = await manager.getSnapshot(snapshot.snapshotId);
      expect(retrieved).not.toBeNull();
      expect(retrieved!.snapshotId).toBe(snapshot.snapshotId);
    });

    it('should update current snapshot after creation', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      expect(manager.getCurrentSnapshotId()).toBe(snapshot.snapshotId);
    });

    it('should include schema-id in snapshot', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        schemaId: 1,
      });

      expect(snapshot.schemaId).toBe(1);
    });

    it('should use default schema-id when not specified', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      // Default schema ID is 0
      expect(snapshot.schemaId).toBe(0);
    });
  });

  describe('snapshot validation', () => {
    it('should reject snapshot without manifest list path', async () => {
      await expect(
        manager.createSnapshot({
          operation: 'append',
          manifestListPath: '',
        })
      ).rejects.toThrow(/manifest.*list.*required/i);
    });

    it('should reject invalid operation type', async () => {
      await expect(
        manager.createSnapshot({
          // @ts-expect-error - Testing invalid operation
          operation: 'invalid-operation',
          manifestListPath: 'metadata/snap-1-manifest-list.avro',
        })
      ).rejects.toThrow(/invalid.*operation/i);
    });

    it('should reject negative timestamp', async () => {
      await expect(
        manager.createSnapshot({
          operation: 'append',
          manifestListPath: 'metadata/snap-1-manifest-list.avro',
          timestampMs: -1,
        })
      ).rejects.toThrow(/invalid.*timestamp/i);
    });

    it('should reject invalid schema-id', async () => {
      await expect(
        manager.createSnapshot({
          operation: 'append',
          manifestListPath: 'metadata/snap-1-manifest-list.avro',
          schemaId: -1,
        })
      ).rejects.toThrow(/invalid.*schema/i);
    });
  });
});

// ============================================================================
// 2. Snapshot ID Generation
// ============================================================================

describe('SnapshotManager - Snapshot ID Generation', () => {
  let storage: MemoryStorage;
  let manager: SnapshotManager;

  beforeEach(async () => {
    storage = createTestStorage();
    manager = await createInitializedManager(storage);
  });

  describe('unique ID generation', () => {
    it('should generate unique snapshot IDs', async () => {
      const snapshot1 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      const snapshot2 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-2-manifest-list.avro',
      });

      expect(snapshot1.snapshotId).not.toBe(snapshot2.snapshotId);
    });

    it('should generate strictly increasing snapshot IDs', async () => {
      const snapshots: Snapshot[] = [];

      for (let i = 0; i < 10; i++) {
        const snapshot = await manager.createSnapshot({
          operation: 'append',
          manifestListPath: `metadata/snap-${i}-manifest-list.avro`,
        });
        snapshots.push(snapshot);
      }

      for (let i = 1; i < snapshots.length; i++) {
        expect(snapshots[i].snapshotId > snapshots[i - 1].snapshotId).toBe(true);
      }
    });

    it('should generate snapshot IDs as bigint (64-bit)', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      expect(typeof snapshot.snapshotId).toBe('bigint');
    });

    it('should handle high-volume snapshot ID generation without collision', async () => {
      const ids = new Set<bigint>();

      // Create 100 snapshots rapidly
      for (let i = 0; i < 100; i++) {
        const snapshot = await manager.createSnapshot({
          operation: 'append',
          manifestListPath: `metadata/snap-${i}-manifest-list.avro`,
        });
        ids.add(snapshot.snapshotId);
      }

      // All IDs should be unique
      expect(ids.size).toBe(100);
    });

    it('should persist ID counter across manager restarts', async () => {
      // Create some snapshots
      const snapshot1 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      // Create new manager with same storage
      const newManager = await createInitializedManager(storage);

      const snapshot2 = await newManager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-2-manifest-list.avro',
      });

      // New snapshot should have higher ID
      expect(snapshot2.snapshotId > snapshot1.snapshotId).toBe(true);
    });
  });

  describe('sequence number generation', () => {
    it('should assign monotonically increasing sequence numbers', async () => {
      const snapshot1 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      const snapshot2 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-2-manifest-list.avro',
      });

      expect(snapshot2.sequenceNumber).toBe(snapshot1.sequenceNumber + 1n);
    });

    it('should start sequence numbers from 1', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      expect(snapshot.sequenceNumber).toBe(1n);
    });

    it('should include sequence number in all snapshots', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      expect(snapshot.sequenceNumber).toBeDefined();
      expect(typeof snapshot.sequenceNumber).toBe('bigint');
    });
  });
});

// ============================================================================
// 3. Parent Tracking
// ============================================================================

describe('SnapshotManager - Parent Tracking', () => {
  let storage: MemoryStorage;
  let manager: SnapshotManager;

  beforeEach(async () => {
    storage = createTestStorage();
    manager = await createInitializedManager(storage);
  });

  describe('parent-snapshot-id', () => {
    it('should have null parent for first snapshot', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      expect(snapshot.parentSnapshotId).toBeNull();
    });

    it('should reference previous snapshot as parent', async () => {
      const snapshot1 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      const snapshot2 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-2-manifest-list.avro',
      });

      expect(snapshot2.parentSnapshotId).toBe(snapshot1.snapshotId);
    });

    it('should form a linear chain of parent references', async () => {
      const snapshots: Snapshot[] = [];

      for (let i = 0; i < 5; i++) {
        const snapshot = await manager.createSnapshot({
          operation: 'append',
          manifestListPath: `metadata/snap-${i}-manifest-list.avro`,
        });
        snapshots.push(snapshot);
      }

      // Verify chain
      for (let i = 1; i < snapshots.length; i++) {
        expect(snapshots[i].parentSnapshotId).toBe(snapshots[i - 1].snapshotId);
      }
    });

    it('should allow explicit parent specification', async () => {
      const snapshot1 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      const snapshot2 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-2-manifest-list.avro',
      });

      // Create snapshot3 with explicit parent as snapshot1 (branching)
      const snapshot3 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-3-manifest-list.avro',
        parentSnapshotId: snapshot1.snapshotId,
      });

      expect(snapshot3.parentSnapshotId).toBe(snapshot1.snapshotId);
    });

    it('should reject non-existent parent snapshot ID', async () => {
      await expect(
        manager.createSnapshot({
          operation: 'append',
          manifestListPath: 'metadata/snap-1-manifest-list.avro',
          parentSnapshotId: 999999n,
        })
      ).rejects.toThrow(/parent.*snapshot.*not found/i);
    });
  });

  describe('snapshot ancestry', () => {
    it('should retrieve snapshot ancestry chain', async () => {
      const snapshot1 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      const snapshot2 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-2-manifest-list.avro',
      });

      const snapshot3 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-3-manifest-list.avro',
      });

      const ancestry = await manager.getSnapshotAncestry(snapshot3.snapshotId);

      expect(ancestry).toHaveLength(2);
      expect(ancestry[0].snapshotId).toBe(snapshot2.snapshotId);
      expect(ancestry[1].snapshotId).toBe(snapshot1.snapshotId);
    });

    it('should return empty ancestry for first snapshot', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      const ancestry = await manager.getSnapshotAncestry(snapshot.snapshotId);

      expect(ancestry).toHaveLength(0);
    });

    it('should limit ancestry depth when specified', async () => {
      // Create 10 snapshots
      let lastSnapshot: Snapshot | null = null;
      for (let i = 0; i < 10; i++) {
        lastSnapshot = await manager.createSnapshot({
          operation: 'append',
          manifestListPath: `metadata/snap-${i}-manifest-list.avro`,
        });
      }

      // Get only 3 ancestors
      const ancestry = await manager.getSnapshotAncestry(lastSnapshot!.snapshotId, { maxDepth: 3 });

      expect(ancestry).toHaveLength(3);
    });

    it('should find common ancestor between two snapshots', async () => {
      const snapshot1 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      const snapshot2 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-2-manifest-list.avro',
      });

      // Create branch from snapshot1
      const snapshot3 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-3-manifest-list.avro',
        parentSnapshotId: snapshot1.snapshotId,
      });

      const commonAncestor = await manager.findCommonAncestor(
        snapshot2.snapshotId,
        snapshot3.snapshotId
      );

      expect(commonAncestor?.snapshotId).toBe(snapshot1.snapshotId);
    });
  });
});

// ============================================================================
// 4. Summary Stats
// ============================================================================

describe('SnapshotManager - Summary Stats', () => {
  let storage: MemoryStorage;
  let manager: SnapshotManager;

  beforeEach(async () => {
    storage = createTestStorage();
    manager = await createInitializedManager(storage);
  });

  describe('required summary fields', () => {
    it('should include operation in summary', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      expect(snapshot.summary.operation).toBe('append');
    });

    it('should include operation in summary as string map', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      // Summary is a Map<string, string> per Iceberg spec
      expect(typeof snapshot.summary).toBe('object');
      expect(typeof snapshot.summary.operation).toBe('string');
    });
  });

  describe('file statistics', () => {
    it('should track added-data-files count', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'added-data-files': '5',
        },
      });

      expect(snapshot.summary['added-data-files']).toBe('5');
    });

    it('should track deleted-data-files count', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'delete',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'deleted-data-files': '3',
        },
      });

      expect(snapshot.summary['deleted-data-files']).toBe('3');
    });

    it('should track total-data-files count', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'total-data-files': '100',
        },
      });

      expect(snapshot.summary['total-data-files']).toBe('100');
    });

    it('should track added-delete-files count', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'delete',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'added-delete-files': '2',
        },
      });

      expect(snapshot.summary['added-delete-files']).toBe('2');
    });
  });

  describe('record statistics', () => {
    it('should track added-records count', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'added-records': '1000',
        },
      });

      expect(snapshot.summary['added-records']).toBe('1000');
    });

    it('should track deleted-records count', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'delete',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'deleted-records': '50',
        },
      });

      expect(snapshot.summary['deleted-records']).toBe('50');
    });

    it('should track total-records count', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'total-records': '10000',
        },
      });

      expect(snapshot.summary['total-records']).toBe('10000');
    });
  });

  describe('size statistics', () => {
    it('should track added-files-size', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'added-files-size': '1048576', // 1MB
        },
      });

      expect(snapshot.summary['added-files-size']).toBe('1048576');
    });

    it('should track removed-files-size', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'overwrite',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'removed-files-size': '524288', // 512KB
        },
      });

      expect(snapshot.summary['removed-files-size']).toBe('524288');
    });

    it('should track total-files-size', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'total-files-size': '10485760', // 10MB
        },
      });

      expect(snapshot.summary['total-files-size']).toBe('10485760');
    });
  });

  describe('equality and partition statistics', () => {
    it('should track added-equality-delete-files', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'delete',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'added-equality-delete-files': '1',
        },
      });

      expect(snapshot.summary['added-equality-delete-files']).toBe('1');
    });

    it('should track added-position-delete-files', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'delete',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'added-position-delete-files': '2',
        },
      });

      expect(snapshot.summary['added-position-delete-files']).toBe('2');
    });

    it('should track changed-partition-count', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'changed-partition-count': '5',
        },
      });

      expect(snapshot.summary['changed-partition-count']).toBe('5');
    });

    it('should track total-equality-deletes', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'delete',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'total-equality-deletes': '100',
        },
      });

      expect(snapshot.summary['total-equality-deletes']).toBe('100');
    });

    it('should track total-position-deletes', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'delete',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'total-position-deletes': '50',
        },
      });

      expect(snapshot.summary['total-position-deletes']).toBe('50');
    });
  });

  describe('custom summary properties', () => {
    it('should allow arbitrary custom properties', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'custom-property': 'custom-value',
          'mongolake.source': 'bulk-import',
        },
      });

      expect(snapshot.summary['custom-property']).toBe('custom-value');
      expect(snapshot.summary['mongolake.source']).toBe('bulk-import');
    });

    it('should preserve all summary values as strings', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'added-records': '1000',
          'total-records': '5000',
        },
      });

      // All values should be strings per Iceberg spec
      Object.values(snapshot.summary).forEach(value => {
        expect(typeof value).toBe('string');
      });
    });
  });
});

// ============================================================================
// 5. Operation Types
// ============================================================================

describe('SnapshotManager - Operation Types', () => {
  let storage: MemoryStorage;
  let manager: SnapshotManager;

  beforeEach(async () => {
    storage = createTestStorage();
    manager = await createInitializedManager(storage);
  });

  describe('append operation', () => {
    it('should create append snapshot', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      expect(snapshot.summary.operation).toBe('append');
    });

    it('should allow append without previous data', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'added-data-files': '10',
          'added-records': '1000',
        },
      });

      expect(snapshot.summary.operation).toBe('append');
      expect(snapshot.summary['added-data-files']).toBe('10');
    });

    it('should support append after append', async () => {
      await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'total-records': '1000',
        },
      });

      const snapshot2 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-2-manifest-list.avro',
        summary: {
          'added-records': '500',
          'total-records': '1500',
        },
      });

      expect(snapshot2.summary.operation).toBe('append');
      expect(snapshot2.summary['total-records']).toBe('1500');
    });
  });

  describe('overwrite operation', () => {
    it('should create overwrite snapshot', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'overwrite',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      expect(snapshot.summary.operation).toBe('overwrite');
    });

    it('should track both added and deleted files for overwrite', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'overwrite',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'added-data-files': '5',
          'deleted-data-files': '3',
          'added-records': '500',
          'deleted-records': '300',
        },
      });

      expect(snapshot.summary.operation).toBe('overwrite');
      expect(snapshot.summary['added-data-files']).toBe('5');
      expect(snapshot.summary['deleted-data-files']).toBe('3');
    });

    it('should allow overwrite with empty result (full partition delete)', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'overwrite',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'deleted-data-files': '10',
          'deleted-records': '1000',
          'added-data-files': '0',
          'added-records': '0',
        },
      });

      expect(snapshot.summary.operation).toBe('overwrite');
      expect(snapshot.summary['deleted-data-files']).toBe('10');
    });
  });

  describe('delete operation', () => {
    it('should create delete snapshot', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'delete',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      expect(snapshot.summary.operation).toBe('delete');
    });

    it('should track deleted records and files', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'delete',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'deleted-data-files': '2',
          'deleted-records': '200',
        },
      });

      expect(snapshot.summary.operation).toBe('delete');
      expect(snapshot.summary['deleted-data-files']).toBe('2');
      expect(snapshot.summary['deleted-records']).toBe('200');
    });

    it('should support equality deletes', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'delete',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'added-equality-delete-files': '1',
          'total-equality-deletes': '50',
        },
      });

      expect(snapshot.summary.operation).toBe('delete');
      expect(snapshot.summary['added-equality-delete-files']).toBe('1');
    });

    it('should support position deletes', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'delete',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'added-position-delete-files': '2',
          'total-position-deletes': '100',
        },
      });

      expect(snapshot.summary.operation).toBe('delete');
      expect(snapshot.summary['added-position-delete-files']).toBe('2');
    });
  });

  describe('replace operation', () => {
    it('should create replace snapshot', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'replace',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      expect(snapshot.summary.operation).toBe('replace');
    });

    it('should track schema changes for replace', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'replace',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        schemaId: 2,
        summary: {
          'total-data-files': '50',
          'total-records': '5000',
        },
      });

      expect(snapshot.summary.operation).toBe('replace');
      expect(snapshot.schemaId).toBe(2);
    });
  });

  describe('operation type validation', () => {
    it('should accept all valid operation types', async () => {
      const validOperations: OperationType[] = ['append', 'overwrite', 'delete', 'replace'];

      for (const operation of validOperations) {
        const snapshot = await manager.createSnapshot({
          operation,
          manifestListPath: `metadata/snap-${operation}-manifest-list.avro`,
        });

        expect(snapshot.summary.operation).toBe(operation);
      }
    });

    it('should reject invalid operation types', async () => {
      const invalidOperations = ['insert', 'update', 'merge', 'truncate', ''];

      for (const operation of invalidOperations) {
        await expect(
          manager.createSnapshot({
            // @ts-expect-error - Testing invalid operation
            operation,
            manifestListPath: 'metadata/snap-1-manifest-list.avro',
          })
        ).rejects.toThrow(/invalid.*operation/i);
      }
    });
  });
});

// ============================================================================
// 6. Snapshot Retrieval and Listing
// ============================================================================

describe('SnapshotManager - Retrieval and Listing', () => {
  let storage: MemoryStorage;
  let manager: SnapshotManager;

  beforeEach(async () => {
    storage = createTestStorage();
    manager = await createInitializedManager(storage);
  });

  describe('get snapshot', () => {
    it('should retrieve snapshot by ID', async () => {
      const created = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      const retrieved = await manager.getSnapshot(created.snapshotId);

      expect(retrieved).not.toBeNull();
      expect(retrieved!.snapshotId).toBe(created.snapshotId);
      expect(retrieved!.manifestList).toBe(created.manifestList);
    });

    it('should return null for non-existent snapshot', async () => {
      const retrieved = await manager.getSnapshot(999999n);

      expect(retrieved).toBeNull();
    });

    it('should retrieve all snapshot fields', async () => {
      const created = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        schemaId: 1,
        summary: {
          'added-records': '100',
        },
      });

      const retrieved = await manager.getSnapshot(created.snapshotId);

      expect(retrieved!.snapshotId).toBe(created.snapshotId);
      expect(retrieved!.parentSnapshotId).toBe(created.parentSnapshotId);
      expect(retrieved!.sequenceNumber).toBe(created.sequenceNumber);
      expect(retrieved!.timestampMs).toBe(created.timestampMs);
      expect(retrieved!.manifestList).toBe(created.manifestList);
      expect(retrieved!.schemaId).toBe(created.schemaId);
      expect(retrieved!.summary['added-records']).toBe('100');
    });
  });

  describe('get current snapshot', () => {
    it('should return null when no snapshots exist', async () => {
      const current = await manager.getCurrentSnapshot();

      expect(current).toBeNull();
    });

    it('should return latest snapshot', async () => {
      await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      const latest = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-2-manifest-list.avro',
      });

      const current = await manager.getCurrentSnapshot();

      expect(current).not.toBeNull();
      expect(current!.snapshotId).toBe(latest.snapshotId);
    });
  });

  describe('list snapshots', () => {
    it('should list all snapshots', async () => {
      for (let i = 0; i < 5; i++) {
        await manager.createSnapshot({
          operation: 'append',
          manifestListPath: `metadata/snap-${i}-manifest-list.avro`,
        });
      }

      const snapshots = await manager.listSnapshots();

      expect(snapshots).toHaveLength(5);
    });

    it('should list snapshots in creation order', async () => {
      const ids: bigint[] = [];

      for (let i = 0; i < 5; i++) {
        const snapshot = await manager.createSnapshot({
          operation: 'append',
          manifestListPath: `metadata/snap-${i}-manifest-list.avro`,
        });
        ids.push(snapshot.snapshotId);
      }

      const snapshots = await manager.listSnapshots();
      const retrievedIds = snapshots.map(s => s.snapshotId);

      expect(retrievedIds).toEqual(ids);
    });

    it('should support pagination', async () => {
      for (let i = 0; i < 10; i++) {
        await manager.createSnapshot({
          operation: 'append',
          manifestListPath: `metadata/snap-${i}-manifest-list.avro`,
        });
      }

      const page1 = await manager.listSnapshots({ limit: 5, offset: 0 });
      const page2 = await manager.listSnapshots({ limit: 5, offset: 5 });

      expect(page1).toHaveLength(5);
      expect(page2).toHaveLength(5);

      // Pages should not overlap
      const page1Ids = new Set(page1.map(s => s.snapshotId));
      const page2Ids = new Set(page2.map(s => s.snapshotId));

      for (const id of page2Ids) {
        expect(page1Ids.has(id)).toBe(false);
      }
    });

    it('should return empty array when no snapshots exist', async () => {
      const snapshots = await manager.listSnapshots();

      expect(snapshots).toHaveLength(0);
    });
  });

  describe('snapshot history', () => {
    it('should get snapshot log (history)', async () => {
      for (let i = 0; i < 5; i++) {
        await manager.createSnapshot({
          operation: 'append',
          manifestListPath: `metadata/snap-${i}-manifest-list.avro`,
        });
      }

      const history = await manager.getSnapshotLog();

      expect(history).toHaveLength(5);
      // History should be in reverse chronological order (newest first)
      for (let i = 1; i < history.length; i++) {
        expect(history[i - 1].timestampMs >= history[i].timestampMs).toBe(true);
      }
    });

    it('should include snapshot metadata in log', async () => {
      await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      const history = await manager.getSnapshotLog();

      expect(history[0].snapshotId).toBeDefined();
      expect(history[0].timestampMs).toBeDefined();
      expect(history[0].manifestList).toBeDefined();
    });
  });
});

// ============================================================================
// 7. Snapshot Expiration and Cleanup
// ============================================================================

describe('SnapshotManager - Expiration and Cleanup', () => {
  let storage: MemoryStorage;
  let manager: SnapshotManager;

  beforeEach(async () => {
    storage = createTestStorage();
    manager = await createInitializedManager(storage);
  });

  describe('expire snapshots', () => {
    it('should expire snapshots older than timestamp', async () => {
      // Create snapshots at different times
      const oldTimestamp = Date.now() - 86400000; // 24 hours ago
      const recentTimestamp = Date.now() - 3600000; // 1 hour ago

      await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        timestampMs: oldTimestamp,
      });

      const recentSnapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-2-manifest-list.avro',
        timestampMs: recentTimestamp,
      });

      // Expire snapshots older than 12 hours
      const cutoffMs = Date.now() - 43200000;
      const result = await manager.expireSnapshots({ olderThanMs: cutoffMs });

      expect(result.expiredCount).toBe(1);

      // Only recent snapshot should remain accessible
      const snapshots = await manager.listSnapshots();
      expect(snapshots).toHaveLength(1);
      expect(snapshots[0].snapshotId).toBe(recentSnapshot.snapshotId);
    });

    it('should retain minimum number of snapshots', async () => {
      // Create 5 old snapshots
      const oldTimestamp = Date.now() - 86400000;
      for (let i = 0; i < 5; i++) {
        await manager.createSnapshot({
          operation: 'append',
          manifestListPath: `metadata/snap-${i}-manifest-list.avro`,
          timestampMs: oldTimestamp + i,
        });
      }

      // Expire all but keep minimum 3
      const result = await manager.expireSnapshots({
        olderThanMs: Date.now(),
        minSnapshotsToRetain: 3,
      });

      expect(result.expiredCount).toBe(2);

      const snapshots = await manager.listSnapshots();
      expect(snapshots).toHaveLength(3);
    });

    it('should not expire current snapshot', async () => {
      const oldTimestamp = Date.now() - 86400000;

      const oldSnapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        timestampMs: oldTimestamp,
      });

      // Even with aggressive expiration, current snapshot is preserved
      const result = await manager.expireSnapshots({
        olderThanMs: Date.now(),
        minSnapshotsToRetain: 0,
      });

      // Current snapshot should still exist
      const current = await manager.getCurrentSnapshot();
      expect(current?.snapshotId).toBe(oldSnapshot.snapshotId);
    });

    it('should return list of expired snapshot IDs', async () => {
      const oldTimestamp = Date.now() - 86400000;

      const snapshot1 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        timestampMs: oldTimestamp,
      });

      await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-2-manifest-list.avro',
        timestampMs: Date.now(),
      });

      const result = await manager.expireSnapshots({
        olderThanMs: Date.now() - 3600000,
      });

      expect(result.expiredSnapshots).toContain(snapshot1.snapshotId);
      expect(result.expiredSnapshots).toHaveLength(1);
    });
  });

  describe('rollback to snapshot', () => {
    it('should rollback to previous snapshot', async () => {
      const snapshot1 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-2-manifest-list.avro',
      });

      await manager.rollbackToSnapshot(snapshot1.snapshotId);

      const current = await manager.getCurrentSnapshot();
      expect(current?.snapshotId).toBe(snapshot1.snapshotId);
    });

    it('should create new snapshot for rollback operation', async () => {
      const snapshot1 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-2-manifest-list.avro',
      });

      const rollbackResult = await manager.rollbackToSnapshot(snapshot1.snapshotId);

      // Rollback should create a new snapshot
      const snapshots = await manager.listSnapshots();
      expect(snapshots.length).toBe(3);

      // The new snapshot should reference the rollback target
      expect(rollbackResult.newSnapshotId).toBeDefined();
      expect(rollbackResult.newSnapshotId).not.toBe(snapshot1.snapshotId);
    });

    it('should reject rollback to non-existent snapshot', async () => {
      await expect(manager.rollbackToSnapshot(999999n)).rejects.toThrow(
        /snapshot.*not found/i
      );
    });
  });

  describe('cherry-pick snapshot', () => {
    it('should cherry-pick changes from another snapshot', async () => {
      const snapshot1 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'added-records': '100',
        },
      });

      // Create branch by specifying parent
      const branchSnapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-branch-manifest-list.avro',
        parentSnapshotId: snapshot1.snapshotId,
        summary: {
          'added-records': '50',
        },
      });

      // Continue on main line
      const snapshot2 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-2-manifest-list.avro',
        summary: {
          'added-records': '200',
        },
      });

      // Cherry-pick from branch
      const result = await manager.cherryPick(branchSnapshot.snapshotId);

      expect(result.newSnapshotId).toBeDefined();
      expect(result.sourceSnapshotId).toBe(branchSnapshot.snapshotId);
    });
  });
});

// ============================================================================
// 8. Concurrent Operations
// ============================================================================

describe('SnapshotManager - Concurrent Operations', () => {
  let storage: MemoryStorage;
  let manager: SnapshotManager;

  beforeEach(async () => {
    storage = createTestStorage();
    manager = await createInitializedManager(storage);
  });

  describe('concurrent snapshot creation', () => {
    it('should handle concurrent snapshot creation', async () => {
      // Create 10 snapshots concurrently
      const promises = Array.from({ length: 10 }, (_, i) =>
        manager.createSnapshot({
          operation: 'append',
          manifestListPath: `metadata/snap-${i}-manifest-list.avro`,
        })
      );

      const snapshots = await Promise.all(promises);

      // All snapshots should have unique IDs
      const ids = new Set(snapshots.map(s => s.snapshotId));
      expect(ids.size).toBe(10);

      // All snapshots should be persisted
      const listed = await manager.listSnapshots();
      expect(listed).toHaveLength(10);
    });

    it('should maintain ID ordering under concurrent creation', async () => {
      const promises = Array.from({ length: 5 }, (_, i) =>
        manager.createSnapshot({
          operation: 'append',
          manifestListPath: `metadata/snap-${i}-manifest-list.avro`,
        })
      );

      const snapshots = await Promise.all(promises);
      const sortedBySequence = [...snapshots].sort(
        (a, b) => Number(a.sequenceNumber - b.sequenceNumber)
      );

      // Sequence numbers should be unique and sequential
      for (let i = 0; i < sortedBySequence.length; i++) {
        expect(sortedBySequence[i].sequenceNumber).toBe(BigInt(i + 1));
      }
    });
  });

  describe('optimistic concurrency control', () => {
    it('should support optimistic locking', async () => {
      const snapshot1 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      // Try to create with expected parent
      const snapshot2 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-2-manifest-list.avro',
        expectedParentSnapshotId: snapshot1.snapshotId,
      });

      expect(snapshot2.parentSnapshotId).toBe(snapshot1.snapshotId);
    });

    it('should reject if expected parent does not match current', async () => {
      const snapshot1 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-2-manifest-list.avro',
      });

      // Try to create with expected parent as snapshot1 (but current is snapshot2)
      await expect(
        manager.createSnapshot({
          operation: 'append',
          manifestListPath: 'metadata/snap-3-manifest-list.avro',
          expectedParentSnapshotId: snapshot1.snapshotId,
        })
      ).rejects.toThrow(/concurrent.*modification|parent.*mismatch/i);
    });
  });
});

// ============================================================================
// 9. Initialization and Configuration
// ============================================================================

describe('SnapshotManager - Initialization', () => {
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = createTestStorage();
  });

  describe('initialization', () => {
    it('should initialize empty manager', async () => {
      const manager = new SnapshotManager(storage, 'test-table');
      await manager.initialize();

      expect(manager.isInitialized()).toBe(true);
    });

    it('should be idempotent on multiple initialize calls', async () => {
      const manager = new SnapshotManager(storage, 'test-table');
      await manager.initialize();
      await manager.initialize();
      await manager.initialize();

      expect(manager.isInitialized()).toBe(true);
    });

    it('should load existing snapshots on initialize', async () => {
      const manager1 = new SnapshotManager(storage, 'test-table');
      await manager1.initialize();

      await manager1.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      // Create new manager with same storage
      const manager2 = new SnapshotManager(storage, 'test-table');
      await manager2.initialize();

      const snapshots = await manager2.listSnapshots();
      expect(snapshots).toHaveLength(1);
    });

    it('should throw when used before initialization', async () => {
      const manager = new SnapshotManager(storage, 'test-table');

      await expect(
        manager.createSnapshot({
          operation: 'append',
          manifestListPath: 'metadata/snap-1-manifest-list.avro',
        })
      ).rejects.toThrow(/not initialized/i);
    });
  });

  describe('configuration', () => {
    it('should accept custom ID generator', async () => {
      let idCounter = 1000n;
      const customIdGenerator = () => idCounter++;

      const manager = new SnapshotManager(storage, 'test-table', {
        idGenerator: customIdGenerator,
      });
      await manager.initialize();

      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      expect(snapshot.snapshotId).toBe(1000n);
    });

    it('should accept custom table location', async () => {
      const manager = new SnapshotManager(storage, 'test-table', {
        tableLocation: 's3://bucket/path/to/table',
      });
      await manager.initialize();

      expect(manager.getTableLocation()).toBe('s3://bucket/path/to/table');
    });

    it('should support metadata caching configuration', async () => {
      const manager = new SnapshotManager(storage, 'test-table', {
        cacheEnabled: true,
        cacheTtlMs: 30000,
      });
      await manager.initialize();

      // Cache config should be respected (implementation detail)
      expect(manager.isCacheEnabled()).toBe(true);
    });
  });
});

// ============================================================================
// 10. Integration Tests
// ============================================================================

describe('SnapshotManager - Integration', () => {
  let storage: MemoryStorage;
  let manager: SnapshotManager;

  beforeEach(async () => {
    storage = createTestStorage();
    manager = await createInitializedManager(storage);
  });

  describe('full workflow', () => {
    it('should support complete snapshot lifecycle', async () => {
      // 1. Create initial snapshot (append)
      const snapshot1 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        summary: {
          'added-data-files': '10',
          'added-records': '1000',
          'total-data-files': '10',
          'total-records': '1000',
        },
      });

      expect(snapshot1.parentSnapshotId).toBeNull();
      expect(snapshot1.sequenceNumber).toBe(1n);

      // 2. Add more data (append)
      const snapshot2 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-2-manifest-list.avro',
        summary: {
          'added-data-files': '5',
          'added-records': '500',
          'total-data-files': '15',
          'total-records': '1500',
        },
      });

      expect(snapshot2.parentSnapshotId).toBe(snapshot1.snapshotId);
      expect(snapshot2.sequenceNumber).toBe(2n);

      // 3. Replace partition (overwrite)
      const snapshot3 = await manager.createSnapshot({
        operation: 'overwrite',
        manifestListPath: 'metadata/snap-3-manifest-list.avro',
        summary: {
          'added-data-files': '3',
          'deleted-data-files': '2',
          'added-records': '300',
          'deleted-records': '200',
          'total-data-files': '16',
          'total-records': '1600',
        },
      });

      expect(snapshot3.parentSnapshotId).toBe(snapshot2.snapshotId);
      expect(snapshot3.summary.operation).toBe('overwrite');

      // 4. Delete some records
      const snapshot4 = await manager.createSnapshot({
        operation: 'delete',
        manifestListPath: 'metadata/snap-4-manifest-list.avro',
        summary: {
          'added-delete-files': '1',
          'deleted-records': '100',
          'total-records': '1500',
        },
      });

      expect(snapshot4.summary.operation).toBe('delete');

      // 5. Verify snapshot chain
      const ancestry = await manager.getSnapshotAncestry(snapshot4.snapshotId);
      expect(ancestry.map(s => s.snapshotId)).toEqual([
        snapshot3.snapshotId,
        snapshot2.snapshotId,
        snapshot1.snapshotId,
      ]);

      // 6. Verify all snapshots are listed
      const allSnapshots = await manager.listSnapshots();
      expect(allSnapshots).toHaveLength(4);

      // 7. Verify current snapshot
      const current = await manager.getCurrentSnapshot();
      expect(current?.snapshotId).toBe(snapshot4.snapshotId);
    });

    it('should support branching workflow', async () => {
      // Create main line
      const main1 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/main-1-manifest-list.avro',
      });

      const main2 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/main-2-manifest-list.avro',
      });

      // Create branch from main1
      const branch1 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/branch-1-manifest-list.avro',
        parentSnapshotId: main1.snapshotId,
      });

      const branch2 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/branch-2-manifest-list.avro',
        parentSnapshotId: branch1.snapshotId,
      });

      // Verify branching
      expect(branch1.parentSnapshotId).toBe(main1.snapshotId);
      expect(branch2.parentSnapshotId).toBe(branch1.snapshotId);
      expect(main2.parentSnapshotId).toBe(main1.snapshotId);

      // Find common ancestor
      const commonAncestor = await manager.findCommonAncestor(
        main2.snapshotId,
        branch2.snapshotId
      );
      expect(commonAncestor?.snapshotId).toBe(main1.snapshotId);
    });

    it('should handle time travel queries', async () => {
      const timestamps: number[] = [];

      // Create snapshots at different times
      for (let i = 0; i < 5; i++) {
        const ts = Date.now() + i * 1000; // 1 second apart
        timestamps.push(ts);

        await manager.createSnapshot({
          operation: 'append',
          manifestListPath: `metadata/snap-${i}-manifest-list.avro`,
          timestampMs: ts,
        });
      }

      // Query as of specific timestamp
      const snapshotAtTime = await manager.getSnapshotAsOf(timestamps[2]);
      expect(snapshotAtTime).not.toBeNull();
      expect(snapshotAtTime!.timestampMs).toBeLessThanOrEqual(timestamps[2]);

      // Query should return the latest snapshot at or before the timestamp
      const snapshotsBefore = await manager.listSnapshotsAsOf(timestamps[3]);
      expect(snapshotsBefore.length).toBeLessThanOrEqual(4);
    });
  });

  describe('persistence and recovery', () => {
    it('should persist all snapshot data', async () => {
      const original = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
        schemaId: 1,
        summary: {
          'added-records': '1000',
          'total-records': '1000',
          'custom-prop': 'custom-value',
        },
      });

      // Create new manager
      const newManager = await createInitializedManager(storage);
      const retrieved = await newManager.getSnapshot(original.snapshotId);

      expect(retrieved).not.toBeNull();
      expect(retrieved!.snapshotId).toBe(original.snapshotId);
      expect(retrieved!.parentSnapshotId).toBe(original.parentSnapshotId);
      expect(retrieved!.sequenceNumber).toBe(original.sequenceNumber);
      expect(retrieved!.timestampMs).toBe(original.timestampMs);
      expect(retrieved!.manifestList).toBe(original.manifestList);
      expect(retrieved!.schemaId).toBe(original.schemaId);
      expect(retrieved!.summary).toEqual(original.summary);
    });

    it('should maintain current snapshot pointer across restarts', async () => {
      const snapshot = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 'metadata/snap-1-manifest-list.avro',
      });

      const newManager = await createInitializedManager(storage);
      const currentId = newManager.getCurrentSnapshotId();

      expect(currentId).toBe(snapshot.snapshotId);
    });
  });
});
