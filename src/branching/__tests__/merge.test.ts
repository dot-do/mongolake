/**
 * Merge Engine Tests (TDD RED Phase)
 *
 * Tests for merging branches in MongoLake.
 * These tests should FAIL initially - they define the expected API.
 *
 * Requirements from mongolake-eg5.6.1:
 * - test db.merge('branch') applies changes
 * - test auto-merge for non-conflicting changes
 * - test merge fails on conflicts without resolution
 * - test manual conflict resolution callback
 * - test merge creates new snapshot
 * - test branch cleanup after merge
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { MemoryStorage } from '../../storage/index.js';
import { BranchManager, type BranchInfo } from '../manager.js';
import { DEFAULT_BRANCH } from '../metadata.js';
import {
  MergeEngine,
  type MergeOptions,
  type MergeResult,
  type MergeConflict,
  type ConflictResolution,
  type MergeStrategy,
  MergeError,
  ConflictError,
} from '../merge.js';

// ============================================================================
// Test Fixtures
// ============================================================================

function createTestStorage(): MemoryStorage {
  return new MemoryStorage();
}

async function createInitializedSystem(
  storage: MemoryStorage
): Promise<{ manager: BranchManager; engine: MergeEngine }> {
  const manager = new BranchManager(storage, 'testdb');
  await manager.initialize('initial-snapshot-001');

  const engine = new MergeEngine(storage, manager, 'testdb');
  return { manager, engine };
}

// ============================================================================
// Merge Engine Tests - Core API
// ============================================================================

describe('MergeEngine - Core API', () => {
  let storage: MemoryStorage;
  let manager: BranchManager;
  let engine: MergeEngine;

  beforeEach(async () => {
    storage = createTestStorage();
    const system = await createInitializedSystem(storage);
    manager = system.manager;
    engine = system.engine;
  });

  // ==========================================================================
  // db.merge('branch') applies changes
  // ==========================================================================

  describe('db.merge(branch) applies changes', () => {
    it('should merge branch changes into main', async () => {
      // Create and modify a feature branch
      await manager.createBranch('feature-branch');
      await manager.recordModifiedFile('feature-branch', 'users/data-001.parquet');
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      // Merge feature branch into main
      const result = await engine.merge('feature-branch');

      expect(result.success).toBe(true);
      expect(result.mergedChanges).toBeGreaterThan(0);
      expect(result.sourceBranch).toBe('feature-branch');
      expect(result.targetBranch).toBe(DEFAULT_BRANCH);
    });

    it('should create a merge commit on target branch', async () => {
      await manager.createBranch('feature-branch');
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      const result = await engine.merge('feature-branch');

      expect(result.mergeCommit).toBeDefined();
      expect(typeof result.mergeCommit).toBe('string');

      // Target branch should have the new merge commit
      const mainBranch = await manager.getBranch(DEFAULT_BRANCH);
      expect(mainBranch!.headCommit).toBe(result.mergeCommit);
    });

    it('should merge into a specified target branch', async () => {
      // Create development and feature branches
      await manager.createBranch('development');
      await manager.createBranch('feature-branch', { parentBranch: 'development' });
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      // Merge feature into development (not main)
      const result = await engine.merge('feature-branch', {
        targetBranch: 'development',
      });

      expect(result.success).toBe(true);
      expect(result.targetBranch).toBe('development');

      // Development branch should have the merge commit
      const devBranch = await manager.getBranch('development');
      expect(devBranch!.headCommit).toBe(result.mergeCommit);
    });

    it('should fail when source branch does not exist', async () => {
      await expect(engine.merge('non-existent-branch')).rejects.toThrow(
        MergeError
      );
      await expect(engine.merge('non-existent-branch')).rejects.toThrow(
        /branch.*not found/i
      );
    });

    it('should fail when target branch does not exist', async () => {
      await manager.createBranch('feature-branch');

      await expect(
        engine.merge('feature-branch', { targetBranch: 'non-existent' })
      ).rejects.toThrow(MergeError);
    });

    it('should fail when merging branch into itself', async () => {
      await manager.createBranch('feature-branch');

      await expect(
        engine.merge('feature-branch', { targetBranch: 'feature-branch' })
      ).rejects.toThrow(/cannot merge.*into itself/i);
    });
  });

  // ==========================================================================
  // Auto-merge for non-conflicting changes
  // ==========================================================================

  describe('auto-merge for non-conflicting changes', () => {
    it('should auto-merge when no conflicts exist', async () => {
      // Create branch with changes
      await manager.createBranch('feature-branch');
      await manager.recordModifiedFile('feature-branch', 'users/new-data.parquet');
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      // Main has no changes since branch was created
      const result = await engine.merge('feature-branch');

      expect(result.success).toBe(true);
      expect(result.conflicts).toHaveLength(0);
      expect(result.autoMerged).toBe(true);
    });

    it('should auto-merge when different documents are modified', async () => {
      // Create branch and modify document A
      await manager.createBranch('feature-branch');
      await manager.recordModifiedFile('feature-branch', 'users/doc-a.parquet');

      // Advance main with document B modification
      await manager.recordModifiedFile(DEFAULT_BRANCH, 'users/doc-b.parquet');
      await manager.advanceSnapshot('main-snapshot-002');

      // Advance feature branch
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      // Should auto-merge since different documents
      const result = await engine.merge('feature-branch');

      expect(result.success).toBe(true);
      expect(result.conflicts).toHaveLength(0);
      expect(result.autoMerged).toBe(true);
    });

    it('should auto-merge when same document has non-conflicting field changes', async () => {
      // Setup: main and branch modify different fields of same document
      await manager.createBranch('feature-branch');

      // Record that both branches modified the same file (but different fields)
      // The actual conflict detection happens at field level
      await manager.recordModifiedFile('feature-branch', 'users/doc-001.parquet');
      await manager.recordModifiedFile(DEFAULT_BRANCH, 'users/doc-001.parquet');

      // Mock the conflict detector to indicate no conflicts
      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue([]),
      });

      await manager.advanceSnapshot('main-snapshot-002');
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      const result = await engine.merge('feature-branch');

      expect(result.success).toBe(true);
      expect(result.conflicts).toHaveLength(0);
    });

    it('should apply all branch modifications to target', async () => {
      await manager.createBranch('feature-branch');

      // Multiple file modifications on branch
      await manager.recordModifiedFiles('feature-branch', [
        'users/file1.parquet',
        'users/file2.parquet',
        'orders/file1.parquet',
      ]);
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      const result = await engine.merge('feature-branch');

      expect(result.success).toBe(true);
      expect(result.mergedChanges).toBe(3);
    });
  });

  // ==========================================================================
  // Merge fails on conflicts without resolution
  // ==========================================================================

  describe('merge fails on conflicts without resolution', () => {
    it('should fail when conflicts exist and no resolution provided', async () => {
      await manager.createBranch('feature-branch');

      // Both branches modify the same document/field
      await manager.recordModifiedFile('feature-branch', 'users/doc-001.parquet');
      await manager.recordModifiedFile(DEFAULT_BRANCH, 'users/doc-001.parquet');

      // Mock conflict detector to return conflicts
      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue([
          {
            documentId: 'doc-001',
            collection: 'users',
            field: 'email',
            sourceValue: 'feature@example.com',
            targetValue: 'main@example.com',
            baseValue: 'original@example.com',
          },
        ]),
      });

      await manager.advanceSnapshot('main-snapshot-002');
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      await expect(engine.merge('feature-branch')).rejects.toThrow(ConflictError);
    });

    it('should include conflict details in error', async () => {
      await manager.createBranch('feature-branch');
      await manager.recordModifiedFile('feature-branch', 'users/doc-001.parquet');
      await manager.recordModifiedFile(DEFAULT_BRANCH, 'users/doc-001.parquet');

      const mockConflicts: MergeConflict[] = [
        {
          documentId: 'doc-001',
          collection: 'users',
          field: 'email',
          sourceValue: 'feature@example.com',
          targetValue: 'main@example.com',
          baseValue: 'original@example.com',
        },
      ];

      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue(mockConflicts),
      });

      await manager.advanceSnapshot('main-snapshot-002');
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      try {
        await engine.merge('feature-branch');
        expect.fail('Should have thrown ConflictError');
      } catch (error) {
        expect(error).toBeInstanceOf(ConflictError);
        const conflictError = error as ConflictError;
        expect(conflictError.conflicts).toHaveLength(1);
        expect(conflictError.conflicts[0].documentId).toBe('doc-001');
        expect(conflictError.conflicts[0].field).toBe('email');
      }
    });

    it('should report multiple conflicts', async () => {
      await manager.createBranch('feature-branch');

      const mockConflicts: MergeConflict[] = [
        {
          documentId: 'doc-001',
          collection: 'users',
          field: 'email',
          sourceValue: 'a@example.com',
          targetValue: 'b@example.com',
          baseValue: 'c@example.com',
        },
        {
          documentId: 'doc-002',
          collection: 'users',
          field: 'name',
          sourceValue: 'Alice',
          targetValue: 'Bob',
          baseValue: 'Charlie',
        },
        {
          documentId: 'doc-003',
          collection: 'orders',
          field: 'status',
          sourceValue: 'shipped',
          targetValue: 'cancelled',
          baseValue: 'pending',
        },
      ];

      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue(mockConflicts),
      });

      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      try {
        await engine.merge('feature-branch');
        expect.fail('Should have thrown ConflictError');
      } catch (error) {
        expect(error).toBeInstanceOf(ConflictError);
        const conflictError = error as ConflictError;
        expect(conflictError.conflicts).toHaveLength(3);
      }
    });

    it('should not modify target branch when conflicts exist', async () => {
      await manager.createBranch('feature-branch');

      const originalMain = await manager.getBranch(DEFAULT_BRANCH);
      const originalHeadCommit = originalMain!.headCommit;

      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue([
          {
            documentId: 'doc-001',
            collection: 'users',
            field: 'email',
            sourceValue: 'a@example.com',
            targetValue: 'b@example.com',
            baseValue: 'c@example.com',
          },
        ]),
      });

      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      try {
        await engine.merge('feature-branch');
      } catch {
        // Expected to throw
      }

      // Main branch should be unchanged
      const mainBranch = await manager.getBranch(DEFAULT_BRANCH);
      expect(mainBranch!.headCommit).toBe(originalHeadCommit);
    });
  });

  // ==========================================================================
  // Manual conflict resolution callback
  // ==========================================================================

  describe('manual conflict resolution callback', () => {
    it('should call resolution callback when conflicts exist', async () => {
      await manager.createBranch('feature-branch');

      const mockConflicts: MergeConflict[] = [
        {
          documentId: 'doc-001',
          collection: 'users',
          field: 'email',
          sourceValue: 'source@example.com',
          targetValue: 'target@example.com',
          baseValue: 'base@example.com',
        },
      ];

      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue(mockConflicts),
      });

      const resolveConflict = vi.fn().mockReturnValue({
        resolution: 'source' as const,
        resolvedValue: 'source@example.com',
      });

      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      const result = await engine.merge('feature-branch', {
        onConflict: resolveConflict,
      });

      expect(resolveConflict).toHaveBeenCalledTimes(1);
      expect(resolveConflict).toHaveBeenCalledWith(mockConflicts[0]);
      expect(result.success).toBe(true);
    });

    it('should support resolution strategy "source" (keep branch changes)', async () => {
      await manager.createBranch('feature-branch');

      const mockConflicts: MergeConflict[] = [
        {
          documentId: 'doc-001',
          collection: 'users',
          field: 'email',
          sourceValue: 'source@example.com',
          targetValue: 'target@example.com',
          baseValue: 'base@example.com',
        },
      ];

      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue(mockConflicts),
      });

      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      const result = await engine.merge('feature-branch', {
        onConflict: () => ({
          resolution: 'source',
          resolvedValue: 'source@example.com',
        }),
      });

      expect(result.success).toBe(true);
      expect(result.resolvedConflicts).toHaveLength(1);
      expect(result.resolvedConflicts![0].resolution).toBe('source');
    });

    it('should support resolution strategy "target" (keep main changes)', async () => {
      await manager.createBranch('feature-branch');

      const mockConflicts: MergeConflict[] = [
        {
          documentId: 'doc-001',
          collection: 'users',
          field: 'email',
          sourceValue: 'source@example.com',
          targetValue: 'target@example.com',
          baseValue: 'base@example.com',
        },
      ];

      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue(mockConflicts),
      });

      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      const result = await engine.merge('feature-branch', {
        onConflict: () => ({
          resolution: 'target',
          resolvedValue: 'target@example.com',
        }),
      });

      expect(result.success).toBe(true);
      expect(result.resolvedConflicts).toHaveLength(1);
      expect(result.resolvedConflicts![0].resolution).toBe('target');
    });

    it('should support resolution strategy "custom" (provide custom value)', async () => {
      await manager.createBranch('feature-branch');

      const mockConflicts: MergeConflict[] = [
        {
          documentId: 'doc-001',
          collection: 'users',
          field: 'email',
          sourceValue: 'source@example.com',
          targetValue: 'target@example.com',
          baseValue: 'base@example.com',
        },
      ];

      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue(mockConflicts),
      });

      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      const result = await engine.merge('feature-branch', {
        onConflict: () => ({
          resolution: 'custom',
          resolvedValue: 'merged@example.com',
        }),
      });

      expect(result.success).toBe(true);
      expect(result.resolvedConflicts![0].resolvedValue).toBe('merged@example.com');
    });

    it('should support async resolution callback', async () => {
      await manager.createBranch('feature-branch');

      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue([
          {
            documentId: 'doc-001',
            collection: 'users',
            field: 'email',
            sourceValue: 'source@example.com',
            targetValue: 'target@example.com',
            baseValue: 'base@example.com',
          },
        ]),
      });

      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      const asyncResolver = vi.fn().mockImplementation(async () => {
        // Simulate async operation (e.g., UI prompt)
        await new Promise((resolve) => setTimeout(resolve, 10));
        return {
          resolution: 'source' as const,
          resolvedValue: 'source@example.com',
        };
      });

      const result = await engine.merge('feature-branch', {
        onConflict: asyncResolver,
      });

      expect(asyncResolver).toHaveBeenCalled();
      expect(result.success).toBe(true);
    });

    it('should abort merge if resolution callback throws', async () => {
      await manager.createBranch('feature-branch');

      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue([
          {
            documentId: 'doc-001',
            collection: 'users',
            field: 'email',
            sourceValue: 'source@example.com',
            targetValue: 'target@example.com',
            baseValue: 'base@example.com',
          },
        ]),
      });

      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      const failingResolver = vi.fn().mockImplementation(() => {
        throw new Error('User cancelled');
      });

      await expect(
        engine.merge('feature-branch', { onConflict: failingResolver })
      ).rejects.toThrow('User cancelled');
    });
  });

  // ==========================================================================
  // Merge creates new snapshot
  // ==========================================================================

  describe('merge creates new snapshot', () => {
    it('should create a new snapshot after successful merge', async () => {
      await manager.createBranch('feature-branch');
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      // Force non-fast-forward to ensure a new merge commit is created
      const result = await engine.merge('feature-branch', { noFastForward: true });

      expect(result.mergeCommit).toBeDefined();
      expect(result.fastForward).toBe(false);

      // The merge commit should be different from the feature branch commit
      expect(result.mergeCommit).not.toBe('feature-snapshot-001');
    });

    it('should include merge metadata in snapshot', async () => {
      await manager.createBranch('feature-branch', {
        description: 'Feature work',
      });
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      const result = await engine.merge('feature-branch', {
        message: 'Merge feature-branch into main',
      });

      expect(result.mergeCommit).toBeDefined();
      expect(result.message).toBe('Merge feature-branch into main');
    });

    it('should update target branch head to merge commit', async () => {
      await manager.createBranch('feature-branch');
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      const mainBefore = await manager.getBranch(DEFAULT_BRANCH);
      const result = await engine.merge('feature-branch');
      const mainAfter = await manager.getBranch(DEFAULT_BRANCH);

      expect(mainAfter!.headCommit).not.toBe(mainBefore!.headCommit);
      expect(mainAfter!.headCommit).toBe(result.mergeCommit);
    });

    it('should record merge in source branch metadata', async () => {
      await manager.createBranch('feature-branch');
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      const result = await engine.merge('feature-branch');

      const featureBranch = await manager.getBranch('feature-branch');
      expect(featureBranch!.mergeCommit).toBe(result.mergeCommit);
      expect(featureBranch!.mergedInto).toBe(DEFAULT_BRANCH);
    });
  });

  // ==========================================================================
  // Branch cleanup after merge
  // ==========================================================================

  describe('branch cleanup after merge', () => {
    it('should optionally delete source branch after merge', async () => {
      await manager.createBranch('feature-branch');
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      await engine.merge('feature-branch', { deleteBranch: true });

      const featureBranch = await manager.getBranch('feature-branch');
      expect(featureBranch).toBeNull();
    });

    it('should keep source branch by default', async () => {
      await manager.createBranch('feature-branch');
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      await engine.merge('feature-branch');

      const featureBranch = await manager.getBranch('feature-branch');
      expect(featureBranch).not.toBeNull();
    });

    it('should mark branch as merged when not deleted', async () => {
      await manager.createBranch('feature-branch');
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      await engine.merge('feature-branch', { deleteBranch: false });

      const featureBranch = await manager.getBranch('feature-branch');
      expect(featureBranch!.state).toBe('merged');
    });

    it('should return deleted branch info in result', async () => {
      await manager.createBranch('feature-branch', {
        description: 'Feature work',
      });
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      const result = await engine.merge('feature-branch', { deleteBranch: true });

      expect(result.deletedBranch).toBeDefined();
      expect(result.deletedBranch!.name).toBe('feature-branch');
    });

    it('should not delete protected branch even with deleteBranch option', async () => {
      await manager.createBranch('feature-branch');
      // Protect the branch via store update
      const store = manager['store'];
      await store.updateBranch('feature-branch', { protected: true });

      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      const result = await engine.merge('feature-branch', { deleteBranch: true });

      // Branch should still exist
      const featureBranch = await manager.getBranch('feature-branch');
      expect(featureBranch).not.toBeNull();
      expect(result.deletedBranch).toBeUndefined();
    });
  });
});

// ============================================================================
// Merge Strategies Tests
// ============================================================================

describe('MergeEngine - Merge Strategies', () => {
  let storage: MemoryStorage;
  let manager: BranchManager;
  let engine: MergeEngine;

  beforeEach(async () => {
    storage = createTestStorage();
    const system = await createInitializedSystem(storage);
    manager = system.manager;
    engine = system.engine;
  });

  describe('merge strategies', () => {
    it('should support "ours" strategy (always keep target/main changes)', async () => {
      await manager.createBranch('feature-branch');

      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue([
          {
            documentId: 'doc-001',
            collection: 'users',
            field: 'email',
            sourceValue: 'source@example.com',
            targetValue: 'target@example.com',
            baseValue: 'base@example.com',
          },
        ]),
      });

      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      const result = await engine.merge('feature-branch', {
        strategy: 'ours',
      });

      expect(result.success).toBe(true);
      expect(result.resolvedConflicts![0].resolution).toBe('target');
    });

    it('should support "theirs" strategy (always keep source/branch changes)', async () => {
      await manager.createBranch('feature-branch');

      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue([
          {
            documentId: 'doc-001',
            collection: 'users',
            field: 'email',
            sourceValue: 'source@example.com',
            targetValue: 'target@example.com',
            baseValue: 'base@example.com',
          },
        ]),
      });

      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      const result = await engine.merge('feature-branch', {
        strategy: 'theirs',
      });

      expect(result.success).toBe(true);
      expect(result.resolvedConflicts![0].resolution).toBe('source');
    });

    it('should default to "manual" strategy requiring resolution', async () => {
      await manager.createBranch('feature-branch');

      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue([
          {
            documentId: 'doc-001',
            collection: 'users',
            field: 'email',
            sourceValue: 'source@example.com',
            targetValue: 'target@example.com',
            baseValue: 'base@example.com',
          },
        ]),
      });

      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      // Without onConflict callback, should fail
      await expect(engine.merge('feature-branch')).rejects.toThrow(ConflictError);
    });
  });
});

// ============================================================================
// Merge Preview Tests
// ============================================================================

describe('MergeEngine - Merge Preview', () => {
  let storage: MemoryStorage;
  let manager: BranchManager;
  let engine: MergeEngine;

  beforeEach(async () => {
    storage = createTestStorage();
    const system = await createInitializedSystem(storage);
    manager = system.manager;
    engine = system.engine;
  });

  describe('merge preview mode', () => {
    it('should preview merge without applying changes', async () => {
      await manager.createBranch('feature-branch');
      await manager.recordModifiedFile('feature-branch', 'users/new-file.parquet');
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      const mainBefore = await manager.getBranch(DEFAULT_BRANCH);
      const preview = await engine.preview('feature-branch');
      const mainAfter = await manager.getBranch(DEFAULT_BRANCH);

      // Main should be unchanged
      expect(mainAfter!.headCommit).toBe(mainBefore!.headCommit);

      // Preview should have merge info
      expect(preview.canMerge).toBeDefined();
      expect(preview.conflicts).toBeDefined();
      expect(preview.changesCount).toBeDefined();
    });

    it('should show conflicts in preview', async () => {
      await manager.createBranch('feature-branch');

      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue([
          {
            documentId: 'doc-001',
            collection: 'users',
            field: 'email',
            sourceValue: 'source@example.com',
            targetValue: 'target@example.com',
            baseValue: 'base@example.com',
          },
        ]),
      });

      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      const preview = await engine.preview('feature-branch');

      expect(preview.canMerge).toBe(false);
      expect(preview.conflicts).toHaveLength(1);
      expect(preview.requiresResolution).toBe(true);
    });

    it('should show no conflicts when merge is clean', async () => {
      await manager.createBranch('feature-branch');
      await manager.recordModifiedFile('feature-branch', 'users/new-file.parquet');

      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue([]),
      });

      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      const preview = await engine.preview('feature-branch');

      expect(preview.canMerge).toBe(true);
      expect(preview.conflicts).toHaveLength(0);
      expect(preview.requiresResolution).toBe(false);
    });
  });
});

// ============================================================================
// Fast-Forward Merge Tests
// ============================================================================

describe('MergeEngine - Fast-Forward Merge', () => {
  let storage: MemoryStorage;
  let manager: BranchManager;
  let engine: MergeEngine;

  beforeEach(async () => {
    storage = createTestStorage();
    const system = await createInitializedSystem(storage);
    manager = system.manager;
    engine = system.engine;
  });

  describe('fast-forward merge', () => {
    it('should fast-forward when target has no new commits', async () => {
      // Create branch from current main HEAD
      await manager.createBranch('feature-branch');

      // Only advance feature branch, main stays at original
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');
      await manager.advanceSnapshot('feature-snapshot-002', 'feature-branch');

      const result = await engine.merge('feature-branch');

      expect(result.success).toBe(true);
      expect(result.fastForward).toBe(true);
      // Target branch should point to same commit as source
      const mainBranch = await manager.getBranch(DEFAULT_BRANCH);
      expect(mainBranch!.headCommit).toBe('feature-snapshot-002');
    });

    it('should not fast-forward when target has new commits', async () => {
      await manager.createBranch('feature-branch');

      // Advance both branches
      await manager.advanceSnapshot('main-snapshot-002');
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      const result = await engine.merge('feature-branch');

      expect(result.success).toBe(true);
      expect(result.fastForward).toBe(false);
      // Result should be a new merge commit
      const mainBranch = await manager.getBranch(DEFAULT_BRANCH);
      expect(mainBranch!.headCommit).not.toBe('feature-snapshot-001');
      expect(mainBranch!.headCommit).not.toBe('main-snapshot-002');
    });

    it('should allow forcing no-ff merge', async () => {
      await manager.createBranch('feature-branch');
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      const result = await engine.merge('feature-branch', { noFastForward: true });

      expect(result.success).toBe(true);
      expect(result.fastForward).toBe(false);
      // Should create a merge commit even though ff was possible
      const mainBranch = await manager.getBranch(DEFAULT_BRANCH);
      expect(mainBranch!.headCommit).not.toBe('feature-snapshot-001');
    });
  });
});

// ============================================================================
// Merge Hooks Tests
// ============================================================================

describe('MergeEngine - Merge Hooks', () => {
  let storage: MemoryStorage;
  let manager: BranchManager;
  let engine: MergeEngine;

  beforeEach(async () => {
    storage = createTestStorage();
    const system = await createInitializedSystem(storage);
    manager = system.manager;
    engine = system.engine;
  });

  describe('merge hooks', () => {
    it('should call beforeMerge hook', async () => {
      const beforeMerge = vi.fn().mockReturnValue(true);
      engine.setHooks({ beforeMerge });

      await manager.createBranch('feature-branch');
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      await engine.merge('feature-branch');

      expect(beforeMerge).toHaveBeenCalledWith(
        expect.objectContaining({ name: 'feature-branch' }),
        expect.objectContaining({ name: DEFAULT_BRANCH })
      );
    });

    it('should abort merge if beforeMerge returns false', async () => {
      const beforeMerge = vi.fn().mockReturnValue(false);
      engine.setHooks({ beforeMerge });

      await manager.createBranch('feature-branch');
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      await expect(engine.merge('feature-branch')).rejects.toThrow(
        /merge cancelled/i
      );
    });

    it('should call afterMerge hook on success', async () => {
      const afterMerge = vi.fn();
      engine.setHooks({ afterMerge });

      await manager.createBranch('feature-branch');
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      await engine.merge('feature-branch');

      expect(afterMerge).toHaveBeenCalledWith(
        expect.objectContaining({
          success: true,
          sourceBranch: 'feature-branch',
          targetBranch: DEFAULT_BRANCH,
        })
      );
    });

    it('should not call afterMerge hook on failure', async () => {
      const afterMerge = vi.fn();
      engine.setHooks({ afterMerge });

      engine.setConflictDetector({
        detectConflicts: vi.fn().mockResolvedValue([
          {
            documentId: 'doc-001',
            collection: 'users',
            field: 'email',
            sourceValue: 'a@example.com',
            targetValue: 'b@example.com',
            baseValue: 'c@example.com',
          },
        ]),
      });

      await manager.createBranch('feature-branch');
      await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

      try {
        await engine.merge('feature-branch');
      } catch {
        // Expected
      }

      expect(afterMerge).not.toHaveBeenCalled();
    });
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('MergeEngine - Integration', () => {
  let storage: MemoryStorage;
  let manager: BranchManager;
  let engine: MergeEngine;

  beforeEach(async () => {
    storage = createTestStorage();
    const system = await createInitializedSystem(storage);
    manager = system.manager;
    engine = system.engine;
  });

  it('should support full merge workflow', async () => {
    // 1. Create feature branch
    await manager.createBranch('feature/new-api', {
      description: 'New API implementation',
    });

    // 2. Make changes on feature branch
    await manager.recordModifiedFiles('feature/new-api', [
      'api/endpoints.parquet',
      'api/handlers.parquet',
    ]);
    await manager.advanceSnapshot('feature-commit-1', 'feature/new-api');

    // 3. Preview merge
    engine.setConflictDetector({
      detectConflicts: vi.fn().mockResolvedValue([]),
    });
    const preview = await engine.preview('feature/new-api');
    expect(preview.canMerge).toBe(true);

    // 4. Merge
    const result = await engine.merge('feature/new-api', {
      message: 'Merge feature/new-api: New API implementation',
      deleteBranch: true,
    });

    expect(result.success).toBe(true);
    expect(result.mergedChanges).toBe(2);
    expect(result.deletedBranch).toBeDefined();

    // 5. Verify main has the merge
    const mainBranch = await manager.getBranch(DEFAULT_BRANCH);
    expect(mainBranch!.headCommit).toBe(result.mergeCommit);

    // 6. Feature branch should be deleted
    const featureBranch = await manager.getBranch('feature/new-api');
    expect(featureBranch).toBeNull();
  });

  it('should handle merge with conflict resolution', async () => {
    // 1. Create feature branch
    await manager.createBranch('feature-branch');

    // 2. Both branches modify same document
    await manager.recordModifiedFile('feature-branch', 'users/doc-001.parquet');
    await manager.recordModifiedFile(DEFAULT_BRANCH, 'users/doc-001.parquet');

    // 3. Setup conflict
    const mockConflicts: MergeConflict[] = [
      {
        documentId: 'doc-001',
        collection: 'users',
        field: 'status',
        sourceValue: 'active',
        targetValue: 'inactive',
        baseValue: 'pending',
      },
    ];

    engine.setConflictDetector({
      detectConflicts: vi.fn().mockResolvedValue(mockConflicts),
    });

    await manager.advanceSnapshot('main-commit-2');
    await manager.advanceSnapshot('feature-commit-1', 'feature-branch');

    // 4. Merge with resolution
    const result = await engine.merge('feature-branch', {
      onConflict: (conflict) => ({
        resolution: 'custom',
        resolvedValue: 'active', // Keep the feature branch value
      }),
    });

    expect(result.success).toBe(true);
    expect(result.conflicts).toHaveLength(1);
    expect(result.resolvedConflicts).toHaveLength(1);
  });

  it('should support multiple sequential merges', async () => {
    // Create multiple feature branches
    await manager.createBranch('feature-a');
    await manager.createBranch('feature-b');
    await manager.createBranch('feature-c');

    // Make changes on each
    await manager.recordModifiedFile('feature-a', 'data/a.parquet');
    await manager.recordModifiedFile('feature-b', 'data/b.parquet');
    await manager.recordModifiedFile('feature-c', 'data/c.parquet');

    await manager.advanceSnapshot('commit-a', 'feature-a');
    await manager.advanceSnapshot('commit-b', 'feature-b');
    await manager.advanceSnapshot('commit-c', 'feature-c');

    engine.setConflictDetector({
      detectConflicts: vi.fn().mockResolvedValue([]),
    });

    // Merge all three sequentially
    const resultA = await engine.merge('feature-a', { deleteBranch: true });
    const resultB = await engine.merge('feature-b', { deleteBranch: true });
    const resultC = await engine.merge('feature-c', { deleteBranch: true });

    expect(resultA.success).toBe(true);
    expect(resultB.success).toBe(true);
    expect(resultC.success).toBe(true);

    // Main should have all changes
    const mainBranch = await manager.getBranch(DEFAULT_BRANCH);
    expect(mainBranch!.headCommit).toBe(resultC.mergeCommit);
  });
});
