/**
 * Conflict Resolver Tests
 *
 * Tests for applying resolved conflicts during merge operations.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { MemoryStorage } from '../../storage/index.js';
import { writeParquet, readParquet } from '../../parquet/io.js';
import { BranchStore, DEFAULT_BRANCH } from '../metadata.js';
import {
  ConflictResolutionApplier,
  MergeResultApplier,
} from '../conflict-resolver.js';
import type { ResolvedConflict } from '../merge.js';
import type { Document } from '../../types.js';

// ============================================================================
// Test Setup
// ============================================================================

describe('ConflictResolutionApplier', () => {
  let storage: MemoryStorage;
  let branchStore: BranchStore;
  let applier: ConflictResolutionApplier;

  beforeEach(async () => {
    storage = new MemoryStorage();
    branchStore = new BranchStore(storage, 'testdb');
    applier = new ConflictResolutionApplier(storage, 'testdb');

    // Initialize main branch
    await branchStore.initializeDefaultBranch('initial-commit');

    // Create feature branch
    await branchStore.createBranch({
      name: 'feature',
      baseCommit: 'initial-commit',
    });
  });

  // ==========================================================================
  // Basic Resolution Application
  // ==========================================================================

  describe('Basic resolution application', () => {
    it('should apply source resolution by using source document', async () => {
      // Set up source document on feature branch
      const sourceDoc = { _id: 'doc-1', name: 'Source Name', value: 100 };
      const sourceRows = [{ _id: 'doc-1', _seq: 1, _op: 'i' as const, doc: sourceDoc }];
      const sourceData = writeParquet(sourceRows);
      await storage.put('testdb/branches/feature/users_1_1.parquet', sourceData);

      // Set up target document on main
      const targetDoc = { _id: 'doc-1', name: 'Target Name', value: 200 };
      const targetRows = [{ _id: 'doc-1', _seq: 1, _op: 'i' as const, doc: targetDoc }];
      const targetData = writeParquet(targetRows);
      await storage.put('testdb/users_1_1.parquet', targetData);

      const resolvedConflicts: ResolvedConflict[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          field: 'name',
          sourceValue: 'Source Name',
          targetValue: 'Target Name',
          baseValue: 'Original Name',
          resolution: 'source',
          resolvedValue: 'Source Name',
        },
      ];

      const result = await applier.applyResolutions(resolvedConflicts, 'feature', DEFAULT_BRANCH);

      expect(result.appliedCount).toBe(1);
      expect(result.failedCount).toBe(0);
      expect(result.details[0]?.success).toBe(true);
    });

    it('should apply target resolution by using target document', async () => {
      // Set up documents
      const sourceDoc = { _id: 'doc-1', name: 'Source', value: 100 };
      const sourceRows = [{ _id: 'doc-1', _seq: 1, _op: 'i' as const, doc: sourceDoc }];
      await storage.put('testdb/branches/feature/users_1_1.parquet', writeParquet(sourceRows));

      const targetDoc = { _id: 'doc-1', name: 'Target', value: 200 };
      const targetRows = [{ _id: 'doc-1', _seq: 1, _op: 'i' as const, doc: targetDoc }];
      await storage.put('testdb/users_1_1.parquet', writeParquet(targetRows));

      const resolvedConflicts: ResolvedConflict[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          field: 'name',
          sourceValue: 'Source',
          targetValue: 'Target',
          baseValue: 'Original',
          resolution: 'target',
          resolvedValue: 'Target',
        },
      ];

      const result = await applier.applyResolutions(resolvedConflicts, 'feature', DEFAULT_BRANCH);

      expect(result.appliedCount).toBe(1);
      expect(result.failedCount).toBe(0);
    });

    it('should apply custom resolution with specified value', async () => {
      // Set up documents
      const sourceDoc = { _id: 'doc-1', name: 'Source', email: 'source@example.com' };
      const sourceRows = [{ _id: 'doc-1', _seq: 1, _op: 'i' as const, doc: sourceDoc }];
      await storage.put('testdb/branches/feature/users_1_1.parquet', writeParquet(sourceRows));

      const targetDoc = { _id: 'doc-1', name: 'Target', email: 'target@example.com' };
      const targetRows = [{ _id: 'doc-1', _seq: 1, _op: 'i' as const, doc: targetDoc }];
      await storage.put('testdb/users_1_1.parquet', writeParquet(targetRows));

      const resolvedConflicts: ResolvedConflict[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          field: 'email',
          sourceValue: 'source@example.com',
          targetValue: 'target@example.com',
          baseValue: 'original@example.com',
          resolution: 'custom',
          resolvedValue: 'merged@example.com',
        },
      ];

      const result = await applier.applyResolutions(resolvedConflicts, 'feature', DEFAULT_BRANCH);

      expect(result.appliedCount).toBe(1);
      expect(result.failedCount).toBe(0);
    });
  });

  // ==========================================================================
  // Multiple Conflicts
  // ==========================================================================

  describe('Multiple conflicts', () => {
    it('should apply multiple conflicts in same collection', async () => {
      // Set up multiple documents
      const doc1 = { _id: 'doc-1', name: 'User1' };
      const doc2 = { _id: 'doc-2', name: 'User2' };

      const sourceRows = [
        { _id: 'doc-1', _seq: 1, _op: 'i' as const, doc: doc1 },
        { _id: 'doc-2', _seq: 2, _op: 'i' as const, doc: doc2 },
      ];
      await storage.put('testdb/branches/feature/users_1_1.parquet', writeParquet(sourceRows));
      await storage.put('testdb/users_1_1.parquet', writeParquet(sourceRows));

      const resolvedConflicts: ResolvedConflict[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          field: 'name',
          sourceValue: 'User1 Source',
          targetValue: 'User1 Target',
          baseValue: 'User1',
          resolution: 'source',
          resolvedValue: 'User1 Source',
        },
        {
          documentId: 'doc-2',
          collection: 'users',
          field: 'name',
          sourceValue: 'User2 Source',
          targetValue: 'User2 Target',
          baseValue: 'User2',
          resolution: 'target',
          resolvedValue: 'User2 Target',
        },
      ];

      const result = await applier.applyResolutions(resolvedConflicts, 'feature', DEFAULT_BRANCH);

      expect(result.appliedCount).toBe(2);
      expect(result.failedCount).toBe(0);
    });

    it('should apply conflicts across multiple collections', async () => {
      // Set up documents in different collections
      const userDoc = { _id: 'user-1', name: 'User' };
      const orderDoc = { _id: 'order-1', status: 'pending' };

      await storage.put('testdb/branches/feature/users_1_1.parquet',
        writeParquet([{ _id: 'user-1', _seq: 1, _op: 'i' as const, doc: userDoc }]));
      await storage.put('testdb/branches/feature/orders_1_1.parquet',
        writeParquet([{ _id: 'order-1', _seq: 1, _op: 'i' as const, doc: orderDoc }]));

      await storage.put('testdb/users_1_1.parquet',
        writeParquet([{ _id: 'user-1', _seq: 1, _op: 'i' as const, doc: userDoc }]));
      await storage.put('testdb/orders_1_1.parquet',
        writeParquet([{ _id: 'order-1', _seq: 1, _op: 'i' as const, doc: orderDoc }]));

      const resolvedConflicts: ResolvedConflict[] = [
        {
          documentId: 'user-1',
          collection: 'users',
          field: 'name',
          sourceValue: 'Source User',
          targetValue: 'Target User',
          baseValue: 'User',
          resolution: 'source',
          resolvedValue: 'Source User',
        },
        {
          documentId: 'order-1',
          collection: 'orders',
          field: 'status',
          sourceValue: 'shipped',
          targetValue: 'cancelled',
          baseValue: 'pending',
          resolution: 'custom',
          resolvedValue: 'confirmed',
        },
      ];

      const result = await applier.applyResolutions(resolvedConflicts, 'feature', DEFAULT_BRANCH);

      expect(result.appliedCount).toBe(2);
      expect(result.failedCount).toBe(0);
      expect(result.details.filter(d => d.collection === 'users')).toHaveLength(1);
      expect(result.details.filter(d => d.collection === 'orders')).toHaveLength(1);
    });
  });

  // ==========================================================================
  // Edge Cases
  // ==========================================================================

  describe('Edge cases', () => {
    it('should handle empty conflict list', async () => {
      const result = await applier.applyResolutions([], 'feature', DEFAULT_BRANCH);

      expect(result.appliedCount).toBe(0);
      expect(result.failedCount).toBe(0);
      expect(result.details).toHaveLength(0);
    });

    it('should handle nested field paths in custom resolution', async () => {
      const doc = { _id: 'doc-1', profile: { settings: { theme: 'dark' } } };
      const rows = [{ _id: 'doc-1', _seq: 1, _op: 'i' as const, doc }];
      await storage.put('testdb/branches/feature/users_1_1.parquet', writeParquet(rows));
      await storage.put('testdb/users_1_1.parquet', writeParquet(rows));

      const resolvedConflicts: ResolvedConflict[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          field: 'profile.settings.theme',
          sourceValue: 'dark',
          targetValue: 'light',
          baseValue: 'auto',
          resolution: 'custom',
          resolvedValue: 'system',
        },
      ];

      const result = await applier.applyResolutions(resolvedConflicts, 'feature', DEFAULT_BRANCH);

      expect(result.appliedCount).toBe(1);
      expect(result.failedCount).toBe(0);
    });

    it('should apply resolutions to target branch when not main', async () => {
      // Create another branch as target
      await branchStore.createBranch({
        name: 'develop',
        baseCommit: 'initial-commit',
      });

      const doc = { _id: 'doc-1', name: 'Test' };
      const rows = [{ _id: 'doc-1', _seq: 1, _op: 'i' as const, doc }];
      await storage.put('testdb/branches/feature/users_1_1.parquet', writeParquet(rows));
      await storage.put('testdb/branches/develop/users_1_1.parquet', writeParquet(rows));

      const resolvedConflicts: ResolvedConflict[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          field: 'name',
          sourceValue: 'Feature',
          targetValue: 'Develop',
          baseValue: 'Test',
          resolution: 'source',
          resolvedValue: 'Feature',
        },
      ];

      const result = await applier.applyResolutions(resolvedConflicts, 'feature', 'develop');

      expect(result.appliedCount).toBe(1);
      expect(result.failedCount).toBe(0);
    });
  });
});

// ============================================================================
// MergeResultApplier Tests
// ============================================================================

describe('MergeResultApplier', () => {
  let storage: MemoryStorage;
  let branchStore: BranchStore;
  let applier: MergeResultApplier;

  beforeEach(async () => {
    storage = new MemoryStorage();
    branchStore = new BranchStore(storage, 'testdb');
    applier = new MergeResultApplier(storage, 'testdb');

    await branchStore.initializeDefaultBranch('initial-commit');
    await branchStore.createBranch({
      name: 'feature',
      baseCommit: 'initial-commit',
    });
  });

  it('should delegate to ConflictResolutionApplier', async () => {
    const doc = { _id: 'doc-1', name: 'Test' };
    const rows = [{ _id: 'doc-1', _seq: 1, _op: 'i' as const, doc }];
    await storage.put('testdb/branches/feature/users_1_1.parquet', writeParquet(rows));
    await storage.put('testdb/users_1_1.parquet', writeParquet(rows));

    const resolvedConflicts: ResolvedConflict[] = [
      {
        documentId: 'doc-1',
        collection: 'users',
        field: 'name',
        sourceValue: 'Source',
        targetValue: 'Target',
        baseValue: 'Test',
        resolution: 'source',
        resolvedValue: 'Source',
      },
    ];

    const result = await applier.applyConflictResolutions(
      resolvedConflicts,
      'feature',
      DEFAULT_BRANCH
    );

    expect(result.appliedCount).toBe(1);
    expect(result.failedCount).toBe(0);
  });
});
