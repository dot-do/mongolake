/**
 * Merge Integration Tests
 *
 * End-to-end tests for the complete merge workflow including:
 * - Branch creation
 * - Data modification
 * - Conflict detection
 * - Conflict resolution
 * - Merge completion
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { MemoryStorage } from '../../../src/storage/index.js';
import { BranchManager } from '../../../src/branching/manager.js';
import { BranchStore, DEFAULT_BRANCH } from '../../../src/branching/metadata.js';
import {
  MergeEngine,
  type MergeConflict,
  ConflictError,
} from '../../../src/branching/merge.js';
import { writeParquet, readParquet } from '../../../src/parquet/io.js';
import type { Document } from '../../../src/types.js';

// ============================================================================
// Test Setup
// ============================================================================

interface TestUser extends Document {
  name: string;
  email: string;
  role?: string;
}

describe('Merge Integration', () => {
  let storage: MemoryStorage;
  let manager: BranchManager;
  let branchStore: BranchStore;
  let engine: MergeEngine;

  beforeEach(async () => {
    storage = new MemoryStorage();
    manager = new BranchManager(storage, 'testdb');
    branchStore = new BranchStore(storage, 'testdb');
    await manager.initialize('initial-snapshot');
    engine = new MergeEngine(storage, manager, 'testdb');
  });

  // ==========================================================================
  // Complete Merge Workflow
  // ==========================================================================

  describe('Complete merge workflow', () => {
    it('should merge branch with no conflicts', async () => {
      // 1. Create feature branch
      await manager.createBranch('feature-add-users');

      // 2. Add data on feature branch
      const userDoc: TestUser = { _id: 'user-1', name: 'Alice', email: 'alice@example.com' };
      const rows = [{ _id: 'user-1', _seq: 1, _op: 'i' as const, doc: userDoc }];
      const parquetData = writeParquet(rows);
      await storage.put('testdb/branches/feature-add-users/users_1_1.parquet', parquetData);

      // 3. Record modified files
      await manager.recordModifiedFile('feature-add-users', 'users_1_1.parquet');
      await manager.advanceSnapshot('feature-snapshot-1', 'feature-add-users');

      // 4. Merge to main
      const result = await engine.merge('feature-add-users');

      expect(result.success).toBe(true);
      expect(result.autoMerged).toBe(true);
      expect(result.conflicts).toHaveLength(0);
      expect(result.mergeCommit).toBeDefined();

      // 5. Verify branch marked as merged
      const featureBranch = await manager.getBranch('feature-add-users');
      expect(featureBranch?.state).toBe('merged');
    });

    it('should handle merge with theirs strategy', async () => {
      // Setup branches with conflicting changes
      await manager.createBranch('feature-theirs');

      // Add conflicting data
      const mainDoc: TestUser = { _id: 'user-1', name: 'Main User', email: 'main@example.com' };
      const featureDoc: TestUser = { _id: 'user-1', name: 'Feature User', email: 'feature@example.com' };

      await storage.put('testdb/users_1_1.parquet',
        writeParquet([{ _id: 'user-1', _seq: 1, _op: 'i' as const, doc: mainDoc }]));
      await storage.put('testdb/branches/feature-theirs/users_1_1.parquet',
        writeParquet([{ _id: 'user-1', _seq: 2, _op: 'u' as const, doc: featureDoc }]));

      // Mock conflict detection
      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue([
          {
            documentId: 'user-1',
            collection: 'users',
            field: 'name',
            sourceValue: 'Feature User',
            targetValue: 'Main User',
            baseValue: 'Original User',
          },
        ]),
      });

      await manager.advanceSnapshot('feature-snapshot-1', 'feature-theirs');

      // Merge with theirs strategy
      const result = await engine.merge('feature-theirs', { strategy: 'theirs' });

      expect(result.success).toBe(true);
      expect(result.resolvedConflicts).toHaveLength(1);
      expect(result.resolvedConflicts![0].resolution).toBe('source');
    });

    it('should handle merge with ours strategy', async () => {
      await manager.createBranch('feature-ours');

      // Mock conflict
      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue([
          {
            documentId: 'user-1',
            collection: 'users',
            field: 'email',
            sourceValue: 'feature@example.com',
            targetValue: 'main@example.com',
            baseValue: 'original@example.com',
          },
        ]),
      });

      await manager.advanceSnapshot('feature-snapshot-1', 'feature-ours');

      // Merge with ours strategy
      const result = await engine.merge('feature-ours', { strategy: 'ours' });

      expect(result.success).toBe(true);
      expect(result.resolvedConflicts).toHaveLength(1);
      expect(result.resolvedConflicts![0].resolution).toBe('target');
    });

    it('should handle manual conflict resolution', async () => {
      await manager.createBranch('feature-manual');

      const conflicts: MergeConflict[] = [
        {
          documentId: 'user-1',
          collection: 'users',
          field: 'role',
          sourceValue: 'admin',
          targetValue: 'user',
          baseValue: 'guest',
        },
      ];

      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue(conflicts),
      });

      await manager.advanceSnapshot('feature-snapshot-1', 'feature-manual');

      // Merge with manual resolution
      const result = await engine.merge('feature-manual', {
        onConflict: (conflict) => {
          // Custom merge logic
          return {
            resolution: 'custom',
            resolvedValue: 'moderator', // Custom value
          };
        },
      });

      expect(result.success).toBe(true);
      expect(result.resolvedConflicts).toHaveLength(1);
      expect(result.resolvedConflicts![0].resolution).toBe('custom');
      expect(result.resolvedConflicts![0].resolvedValue).toBe('moderator');
    });
  });

  // ==========================================================================
  // Conflict Detection Integration
  // ==========================================================================

  describe('Conflict detection integration', () => {
    it('should fail merge when conflicts exist without resolution', async () => {
      await manager.createBranch('feature-conflict');

      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue([
          {
            documentId: 'doc-1',
            collection: 'users',
            field: 'name',
            sourceValue: 'source',
            targetValue: 'target',
            baseValue: 'base',
          },
        ]),
      });

      await manager.advanceSnapshot('feature-snapshot-1', 'feature-conflict');

      await expect(engine.merge('feature-conflict')).rejects.toThrow(ConflictError);
    });

    it('should include all conflicts in error', async () => {
      await manager.createBranch('feature-multiple-conflicts');

      const conflicts: MergeConflict[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          field: 'name',
          sourceValue: 'a',
          targetValue: 'b',
          baseValue: 'c',
        },
        {
          documentId: 'doc-2',
          collection: 'users',
          field: 'email',
          sourceValue: 'x',
          targetValue: 'y',
          baseValue: 'z',
        },
      ];

      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue(conflicts),
      });

      await manager.advanceSnapshot('feature-snapshot-1', 'feature-multiple-conflicts');

      try {
        await engine.merge('feature-multiple-conflicts');
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(ConflictError);
        const conflictError = error as ConflictError;
        expect(conflictError.conflicts).toHaveLength(2);
      }
    });
  });

  // ==========================================================================
  // Preview Mode
  // ==========================================================================

  describe('Preview mode', () => {
    it('should preview merge without applying changes', async () => {
      await manager.createBranch('feature-preview');
      await manager.recordModifiedFile('feature-preview', 'users_1.parquet');
      await manager.advanceSnapshot('preview-snapshot', 'feature-preview');

      const mainBefore = await manager.getBranch(DEFAULT_BRANCH);

      const preview = await engine.preview('feature-preview');

      // Main should be unchanged
      const mainAfter = await manager.getBranch(DEFAULT_BRANCH);
      expect(mainAfter?.headCommit).toBe(mainBefore?.headCommit);

      // Preview should have information
      expect(preview.canMerge).toBeDefined();
      expect(preview.conflicts).toBeDefined();
      expect(preview.changesCount).toBeDefined();
      expect(preview.sourceBranch.name).toBe('feature-preview');
      expect(preview.targetBranch.name).toBe(DEFAULT_BRANCH);
    });

    it('should show conflicts in preview', async () => {
      await manager.createBranch('feature-preview-conflicts');

      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue([
          {
            documentId: 'doc-1',
            collection: 'users',
            field: 'name',
            sourceValue: 'a',
            targetValue: 'b',
            baseValue: 'c',
          },
        ]),
      });

      const preview = await engine.preview('feature-preview-conflicts');

      expect(preview.canMerge).toBe(false);
      expect(preview.conflicts).toHaveLength(1);
      expect(preview.requiresResolution).toBe(true);
    });
  });

  // ==========================================================================
  // Fast-Forward Merges
  // ==========================================================================

  describe('Fast-forward merges', () => {
    it('should fast-forward when target has not advanced', async () => {
      await manager.createBranch('feature-ff');
      await manager.advanceSnapshot('ff-snapshot-1', 'feature-ff');
      await manager.advanceSnapshot('ff-snapshot-2', 'feature-ff');

      const result = await engine.merge('feature-ff');

      expect(result.success).toBe(true);
      expect(result.fastForward).toBe(true);

      // Target should point to source's head
      const mainBranch = await manager.getBranch(DEFAULT_BRANCH);
      expect(mainBranch?.headCommit).toBe('ff-snapshot-2');
    });

    it('should not fast-forward when target has advanced', async () => {
      await manager.createBranch('feature-no-ff');

      // Advance both branches
      await manager.advanceSnapshot('main-snapshot-2');
      await manager.advanceSnapshot('feature-snapshot-1', 'feature-no-ff');

      const result = await engine.merge('feature-no-ff');

      expect(result.success).toBe(true);
      expect(result.fastForward).toBe(false);
    });

    it('should force no-ff merge when requested', async () => {
      await manager.createBranch('feature-force-no-ff');
      await manager.advanceSnapshot('ff-snapshot', 'feature-force-no-ff');

      const result = await engine.merge('feature-force-no-ff', { noFastForward: true });

      expect(result.success).toBe(true);
      expect(result.fastForward).toBe(false);
    });
  });

  // ==========================================================================
  // Branch Cleanup
  // ==========================================================================

  describe('Branch cleanup', () => {
    it('should delete branch after merge when requested', async () => {
      await manager.createBranch('feature-delete');
      await manager.advanceSnapshot('delete-snapshot', 'feature-delete');

      await engine.merge('feature-delete', { deleteBranch: true });

      const branch = await manager.getBranch('feature-delete');
      expect(branch).toBeNull();
    });

    it('should keep branch when delete not requested', async () => {
      await manager.createBranch('feature-keep');
      await manager.advanceSnapshot('keep-snapshot', 'feature-keep');

      await engine.merge('feature-keep', { deleteBranch: false });

      const branch = await manager.getBranch('feature-keep');
      expect(branch).not.toBeNull();
      expect(branch?.state).toBe('merged');
    });

    it('should not delete protected branch even with deleteBranch option', async () => {
      await manager.createBranch('feature-protected');
      await branchStore.updateBranch('feature-protected', { protected: true });
      await manager.advanceSnapshot('protected-snapshot', 'feature-protected');

      const result = await engine.merge('feature-protected', { deleteBranch: true });

      expect(result.success).toBe(true);
      expect(result.deletedBranch).toBeUndefined();

      const branch = await manager.getBranch('feature-protected');
      expect(branch).not.toBeNull();
    });
  });

  // ==========================================================================
  // Merge Hooks
  // ==========================================================================

  describe('Merge hooks', () => {
    it('should call beforeMerge hook', async () => {
      const beforeMerge = vi.fn().mockReturnValue(true);
      engine.setHooks({ beforeMerge });

      await manager.createBranch('feature-hooks');
      await manager.advanceSnapshot('hooks-snapshot', 'feature-hooks');

      await engine.merge('feature-hooks');

      expect(beforeMerge).toHaveBeenCalledTimes(1);
      expect(beforeMerge).toHaveBeenCalledWith(
        expect.objectContaining({ name: 'feature-hooks' }),
        expect.objectContaining({ name: DEFAULT_BRANCH })
      );
    });

    it('should abort merge if beforeMerge returns false', async () => {
      const beforeMerge = vi.fn().mockReturnValue(false);
      engine.setHooks({ beforeMerge });

      await manager.createBranch('feature-abort');
      await manager.advanceSnapshot('abort-snapshot', 'feature-abort');

      await expect(engine.merge('feature-abort')).rejects.toThrow(/cancelled/i);
    });

    it('should call afterMerge hook on success', async () => {
      const afterMerge = vi.fn();
      engine.setHooks({ afterMerge });

      await manager.createBranch('feature-after');
      await manager.advanceSnapshot('after-snapshot', 'feature-after');

      await engine.merge('feature-after');

      expect(afterMerge).toHaveBeenCalledTimes(1);
      expect(afterMerge).toHaveBeenCalledWith(
        expect.objectContaining({
          success: true,
          sourceBranch: 'feature-after',
          targetBranch: DEFAULT_BRANCH,
        })
      );
    });
  });

  // ==========================================================================
  // Batch Merge
  // ==========================================================================

  describe('Batch merge', () => {
    it('should merge multiple branches sequentially', async () => {
      await manager.createBranch('batch-1');
      await manager.createBranch('batch-2');
      await manager.createBranch('batch-3');

      await manager.advanceSnapshot('batch-1-snapshot', 'batch-1');
      await manager.advanceSnapshot('batch-2-snapshot', 'batch-2');
      await manager.advanceSnapshot('batch-3-snapshot', 'batch-3');

      const result = await engine.mergeAll(['batch-1', 'batch-2', 'batch-3']);

      expect(result.total).toBe(3);
      expect(result.succeeded).toBe(3);
      expect(result.failed).toBe(0);
      expect(result.results.every(r => r.result?.success)).toBe(true);
    });

    it('should continue after failed merge', async () => {
      await manager.createBranch('batch-ok-1');
      await manager.createBranch('batch-fail');
      await manager.createBranch('batch-ok-2');

      // Set up conflict on batch-fail
      engine.setConflictDetector({
        detectConflicts: vi.fn().mockImplementation(async (source) => {
          if (source === 'batch-fail') {
            return [{
              documentId: 'doc-1',
              collection: 'users',
              field: 'name',
              sourceValue: 'a',
              targetValue: 'b',
              baseValue: 'c',
            }];
          }
          return [];
        }),
      });

      await manager.advanceSnapshot('ok-1-snapshot', 'batch-ok-1');
      await manager.advanceSnapshot('fail-snapshot', 'batch-fail');
      await manager.advanceSnapshot('ok-2-snapshot', 'batch-ok-2');

      const result = await engine.mergeAll(['batch-ok-1', 'batch-fail', 'batch-ok-2']);

      expect(result.total).toBe(3);
      expect(result.succeeded).toBe(2);
      expect(result.failed).toBe(1);

      const failedResult = result.results.find(r => r.branch === 'batch-fail');
      expect(failedResult?.error).toBeDefined();
    });

    it('should apply strategy to all batch merges', async () => {
      await manager.createBranch('batch-strategy-1');
      await manager.createBranch('batch-strategy-2');

      // Both have conflicts
      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue([
          {
            documentId: 'doc-1',
            collection: 'users',
            field: 'name',
            sourceValue: 'a',
            targetValue: 'b',
            baseValue: 'c',
          },
        ]),
      });

      await manager.advanceSnapshot('strategy-1-snapshot', 'batch-strategy-1');
      await manager.advanceSnapshot('strategy-2-snapshot', 'batch-strategy-2');

      const result = await engine.mergeAll(
        ['batch-strategy-1', 'batch-strategy-2'],
        { strategy: 'theirs' }
      );

      expect(result.succeeded).toBe(2);
      expect(result.results.every(r => r.result?.resolvedConflicts?.[0]?.resolution === 'source')).toBe(true);
    });
  });

  // ==========================================================================
  // Error Handling
  // ==========================================================================

  describe('Error handling', () => {
    it('should throw when source branch does not exist', async () => {
      await expect(engine.merge('non-existent')).rejects.toThrow(/not found/i);
    });

    it('should throw when target branch does not exist', async () => {
      await manager.createBranch('feature-target');

      await expect(
        engine.merge('feature-target', { targetBranch: 'non-existent' })
      ).rejects.toThrow(/not found/i);
    });

    it('should throw when merging branch into itself', async () => {
      await manager.createBranch('feature-self');

      await expect(
        engine.merge('feature-self', { targetBranch: 'feature-self' })
      ).rejects.toThrow(/into itself/i);
    });
  });
});
