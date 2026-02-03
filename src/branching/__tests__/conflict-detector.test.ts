/**
 * Conflict Detector Tests
 *
 * Tests for detecting conflicts when merging branches.
 * Conflict detection identifies when the same documents have been
 * modified on both the source branch and main branch since the branch point.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { MemoryStorage } from '../../storage/index.js';
import { BranchStore, DEFAULT_BRANCH } from '../metadata.js';
import {
  ConflictDetector,
  type ConflictReport,
  type DocumentConflict,
  ConflictType,
  ConflictSeverity,
} from '../conflict-detector.js';
import type { Document } from '../../types.js';

// ============================================================================
// Test Setup
// ============================================================================

interface TestChange {
  documentId: string;
  collection: string;
  operation: 'insert' | 'update' | 'delete';
  fields?: string[];
  before?: Document;
  after?: Document;
}

describe('ConflictDetector', () => {
  let storage: MemoryStorage;
  let branchStore: BranchStore;
  let detector: ConflictDetector;

  beforeEach(async () => {
    storage = new MemoryStorage();
    branchStore = new BranchStore(storage, 'testdb');

    // Initialize main branch
    await branchStore.initializeDefaultBranch('initial-commit');

    // Create the conflict detector
    detector = new ConflictDetector(storage, branchStore, 'testdb');
  });

  // ==========================================================================
  // Same-Document Modifications
  // ==========================================================================

  describe('Same-document modifications', () => {
    it('should detect conflict when same document modified on both branches', async () => {
      // Create feature branch
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      // Simulate changes on main branch since branch point
      const mainChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Alice', age: 30 },
          after: { _id: 'doc-1', name: 'Alice Updated', age: 30 },
        },
      ];

      // Simulate changes on feature branch
      const branchChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Alice', age: 30 },
          after: { _id: 'doc-1', name: 'Alice Feature', age: 30 },
        },
      ];

      const report = await detector.detectConflicts('feature', mainChanges, branchChanges);

      expect(report.hasConflicts).toBe(true);
      expect(report.conflicts).toHaveLength(1);
      expect(report.conflicts[0].documentId).toBe('doc-1');
      expect(report.conflicts[0].collection).toBe('users');
    });

    it('should not detect conflict when different documents modified', async () => {
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const mainChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Alice' },
          after: { _id: 'doc-1', name: 'Alice Updated' },
        },
      ];

      const branchChanges: TestChange[] = [
        {
          documentId: 'doc-2',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-2', name: 'Bob' },
          after: { _id: 'doc-2', name: 'Bob Updated' },
        },
      ];

      const report = await detector.detectConflicts('feature', mainChanges, branchChanges);

      expect(report.hasConflicts).toBe(false);
      expect(report.conflicts).toHaveLength(0);
    });

    it('should detect multiple conflicts across different documents', async () => {
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const mainChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Alice' },
          after: { _id: 'doc-1', name: 'Alice Main' },
        },
        {
          documentId: 'doc-2',
          collection: 'users',
          operation: 'update',
          fields: ['email'],
          before: { _id: 'doc-2', email: 'bob@example.com' },
          after: { _id: 'doc-2', email: 'bob@main.com' },
        },
      ];

      const branchChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Alice' },
          after: { _id: 'doc-1', name: 'Alice Branch' },
        },
        {
          documentId: 'doc-2',
          collection: 'users',
          operation: 'update',
          fields: ['email'],
          before: { _id: 'doc-2', email: 'bob@example.com' },
          after: { _id: 'doc-2', email: 'bob@branch.com' },
        },
      ];

      const report = await detector.detectConflicts('feature', mainChanges, branchChanges);

      expect(report.hasConflicts).toBe(true);
      expect(report.conflicts).toHaveLength(2);
    });
  });

  // ==========================================================================
  // Field-Level Conflict Detection
  // ==========================================================================

  describe('Field-level conflict detection', () => {
    it('should detect conflict when same field modified on both branches', async () => {
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const mainChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Alice', age: 30 },
          after: { _id: 'doc-1', name: 'Alice Main', age: 30 },
        },
      ];

      const branchChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Alice', age: 30 },
          after: { _id: 'doc-1', name: 'Alice Branch', age: 30 },
        },
      ];

      const report = await detector.detectConflicts('feature', mainChanges, branchChanges);

      expect(report.hasConflicts).toBe(true);
      expect(report.conflicts[0].type).toBe(ConflictType.FIELD_CONFLICT);
      expect(report.conflicts[0].conflictingFields).toContain('name');
    });

    it('should not detect conflict when different fields modified', async () => {
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const mainChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Alice', age: 30, email: 'alice@example.com' },
          after: { _id: 'doc-1', name: 'Alice Updated', age: 30, email: 'alice@example.com' },
        },
      ];

      const branchChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['age'],
          before: { _id: 'doc-1', name: 'Alice', age: 30, email: 'alice@example.com' },
          after: { _id: 'doc-1', name: 'Alice', age: 31, email: 'alice@example.com' },
        },
      ];

      const report = await detector.detectConflicts('feature', mainChanges, branchChanges);

      // Different fields modified - should be auto-mergeable, not a conflict
      expect(report.hasConflicts).toBe(false);
      expect(report.autoMergeableChanges).toHaveLength(1);
    });

    it('should detect conflict when multiple overlapping fields modified', async () => {
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const mainChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name', 'email'],
          before: { _id: 'doc-1', name: 'Alice', email: 'a@example.com', age: 30 },
          after: { _id: 'doc-1', name: 'Alice Main', email: 'alice.main@example.com', age: 30 },
        },
      ];

      const branchChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name', 'age'],
          before: { _id: 'doc-1', name: 'Alice', email: 'a@example.com', age: 30 },
          after: { _id: 'doc-1', name: 'Alice Branch', email: 'a@example.com', age: 31 },
        },
      ];

      const report = await detector.detectConflicts('feature', mainChanges, branchChanges);

      expect(report.hasConflicts).toBe(true);
      expect(report.conflicts[0].conflictingFields).toContain('name');
      // email and age are not conflicting - different branches
    });
  });

  // ==========================================================================
  // Conflict Report Structure
  // ==========================================================================

  describe('Conflict report structure', () => {
    it('should include both versions in conflict report', async () => {
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const mainChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Original' },
          after: { _id: 'doc-1', name: 'Main Version' },
        },
      ];

      const branchChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Original' },
          after: { _id: 'doc-1', name: 'Branch Version' },
        },
      ];

      const report = await detector.detectConflicts('feature', mainChanges, branchChanges);

      expect(report.conflicts[0].mainVersion).toEqual({ _id: 'doc-1', name: 'Main Version' });
      expect(report.conflicts[0].branchVersion).toEqual({ _id: 'doc-1', name: 'Branch Version' });
      expect(report.conflicts[0].baseVersion).toEqual({ _id: 'doc-1', name: 'Original' });
    });

    it('should include branch information in report', async () => {
      await branchStore.createBranch({
        name: 'feature-xyz',
        baseCommit: 'commit-1',
      });

      const mainChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Alice' },
          after: { _id: 'doc-1', name: 'Alice Main' },
        },
      ];

      const branchChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Alice' },
          after: { _id: 'doc-1', name: 'Alice Branch' },
        },
      ];

      const report = await detector.detectConflicts('feature-xyz', mainChanges, branchChanges);

      expect(report.sourceBranch).toBe('feature-xyz');
      expect(report.targetBranch).toBe(DEFAULT_BRANCH);
    });

    it('should include summary statistics in report', async () => {
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const mainChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Alice' },
          after: { _id: 'doc-1', name: 'Alice Main' },
        },
        {
          documentId: 'doc-2',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-2', name: 'Bob' },
          after: { _id: 'doc-2', name: 'Bob Main' },
        },
      ];

      const branchChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Alice' },
          after: { _id: 'doc-1', name: 'Alice Branch' },
        },
        {
          documentId: 'doc-3',
          collection: 'users',
          operation: 'insert',
          before: undefined,
          after: { _id: 'doc-3', name: 'Charlie' },
        },
      ];

      const report = await detector.detectConflicts('feature', mainChanges, branchChanges);

      expect(report.summary.totalConflicts).toBe(1);
      expect(report.summary.mainChangesCount).toBe(2);
      expect(report.summary.branchChangesCount).toBe(2);
    });
  });

  // ==========================================================================
  // Delete vs Update Conflicts
  // ==========================================================================

  describe('Delete vs update conflicts', () => {
    it('should detect conflict when document deleted on main and updated on branch', async () => {
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const mainChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'delete',
          before: { _id: 'doc-1', name: 'Alice', age: 30 },
          after: undefined,
        },
      ];

      const branchChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['age'],
          before: { _id: 'doc-1', name: 'Alice', age: 30 },
          after: { _id: 'doc-1', name: 'Alice', age: 31 },
        },
      ];

      const report = await detector.detectConflicts('feature', mainChanges, branchChanges);

      expect(report.hasConflicts).toBe(true);
      expect(report.conflicts[0].type).toBe(ConflictType.DELETE_UPDATE);
      expect(report.conflicts[0].mainOperation).toBe('delete');
      expect(report.conflicts[0].branchOperation).toBe('update');
    });

    it('should detect conflict when document updated on main and deleted on branch', async () => {
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const mainChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Alice', age: 30 },
          after: { _id: 'doc-1', name: 'Alice Updated', age: 30 },
        },
      ];

      const branchChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'delete',
          before: { _id: 'doc-1', name: 'Alice', age: 30 },
          after: undefined,
        },
      ];

      const report = await detector.detectConflicts('feature', mainChanges, branchChanges);

      expect(report.hasConflicts).toBe(true);
      expect(report.conflicts[0].type).toBe(ConflictType.UPDATE_DELETE);
      expect(report.conflicts[0].mainOperation).toBe('update');
      expect(report.conflicts[0].branchOperation).toBe('delete');
    });

    it('should not detect conflict when same document deleted on both branches', async () => {
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const mainChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'delete',
          before: { _id: 'doc-1', name: 'Alice' },
          after: undefined,
        },
      ];

      const branchChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'delete',
          before: { _id: 'doc-1', name: 'Alice' },
          after: undefined,
        },
      ];

      const report = await detector.detectConflicts('feature', mainChanges, branchChanges);

      // Same delete on both branches - no conflict, just skip one
      expect(report.hasConflicts).toBe(false);
    });
  });

  // ==========================================================================
  // Concurrent Inserts with Same ID
  // ==========================================================================

  describe('Concurrent inserts with same ID', () => {
    it('should detect conflict when same ID inserted on both branches', async () => {
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const mainChanges: TestChange[] = [
        {
          documentId: 'new-doc',
          collection: 'users',
          operation: 'insert',
          before: undefined,
          after: { _id: 'new-doc', name: 'Main User', createdBy: 'main' },
        },
      ];

      const branchChanges: TestChange[] = [
        {
          documentId: 'new-doc',
          collection: 'users',
          operation: 'insert',
          before: undefined,
          after: { _id: 'new-doc', name: 'Branch User', createdBy: 'branch' },
        },
      ];

      const report = await detector.detectConflicts('feature', mainChanges, branchChanges);

      expect(report.hasConflicts).toBe(true);
      expect(report.conflicts[0].type).toBe(ConflictType.DUPLICATE_INSERT);
      expect(report.conflicts[0].mainOperation).toBe('insert');
      expect(report.conflicts[0].branchOperation).toBe('insert');
    });

    it('should not detect conflict when different IDs inserted on both branches', async () => {
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const mainChanges: TestChange[] = [
        {
          documentId: 'main-new-doc',
          collection: 'users',
          operation: 'insert',
          before: undefined,
          after: { _id: 'main-new-doc', name: 'Main User' },
        },
      ];

      const branchChanges: TestChange[] = [
        {
          documentId: 'branch-new-doc',
          collection: 'users',
          operation: 'insert',
          before: undefined,
          after: { _id: 'branch-new-doc', name: 'Branch User' },
        },
      ];

      const report = await detector.detectConflicts('feature', mainChanges, branchChanges);

      expect(report.hasConflicts).toBe(false);
    });
  });

  // ==========================================================================
  // Conflict Severity Levels
  // ==========================================================================

  describe('Conflict severity levels', () => {
    it('should mark field conflicts as LOW severity', async () => {
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const mainChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Alice' },
          after: { _id: 'doc-1', name: 'Alice Main' },
        },
      ];

      const branchChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Alice' },
          after: { _id: 'doc-1', name: 'Alice Branch' },
        },
      ];

      const report = await detector.detectConflicts('feature', mainChanges, branchChanges);

      expect(report.conflicts[0].severity).toBe(ConflictSeverity.LOW);
    });

    it('should mark delete/update conflicts as MEDIUM severity', async () => {
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const mainChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'delete',
          before: { _id: 'doc-1', name: 'Alice' },
          after: undefined,
        },
      ];

      const branchChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Alice' },
          after: { _id: 'doc-1', name: 'Alice Branch' },
        },
      ];

      const report = await detector.detectConflicts('feature', mainChanges, branchChanges);

      expect(report.conflicts[0].severity).toBe(ConflictSeverity.MEDIUM);
    });

    it('should mark duplicate inserts as HIGH severity', async () => {
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const mainChanges: TestChange[] = [
        {
          documentId: 'new-doc',
          collection: 'users',
          operation: 'insert',
          before: undefined,
          after: { _id: 'new-doc', name: 'Main User' },
        },
      ];

      const branchChanges: TestChange[] = [
        {
          documentId: 'new-doc',
          collection: 'users',
          operation: 'insert',
          before: undefined,
          after: { _id: 'new-doc', name: 'Branch User' },
        },
      ];

      const report = await detector.detectConflicts('feature', mainChanges, branchChanges);

      expect(report.conflicts[0].severity).toBe(ConflictSeverity.HIGH);
    });
  });

  // ==========================================================================
  // Resolution Hints
  // ==========================================================================

  describe('Resolution hints', () => {
    it('should provide resolution hint for field conflicts', async () => {
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const mainChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Alice' },
          after: { _id: 'doc-1', name: 'Alice Main' },
        },
      ];

      const branchChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Alice' },
          after: { _id: 'doc-1', name: 'Alice Branch' },
        },
      ];

      const report = await detector.detectConflicts('feature', mainChanges, branchChanges);

      expect(report.conflicts[0].resolutionHint).toBeDefined();
      expect(report.conflicts[0].resolutionHint).toContain('name');
    });

    it('should provide resolution hint for delete/update conflicts', async () => {
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const mainChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'delete',
          before: { _id: 'doc-1', name: 'Alice' },
          after: undefined,
        },
      ];

      const branchChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Alice' },
          after: { _id: 'doc-1', name: 'Alice Branch' },
        },
      ];

      const report = await detector.detectConflicts('feature', mainChanges, branchChanges);

      expect(report.conflicts[0].resolutionHint).toBeDefined();
      expect(report.conflicts[0].resolutionHint).toMatch(/delete|keep/i);
    });
  });

  // ==========================================================================
  // Edge Cases
  // ==========================================================================

  describe('Edge cases', () => {
    it('should handle empty change lists', async () => {
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const report = await detector.detectConflicts('feature', [], []);

      expect(report.hasConflicts).toBe(false);
      expect(report.conflicts).toHaveLength(0);
    });

    it('should handle changes across multiple collections', async () => {
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const mainChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Alice' },
          after: { _id: 'doc-1', name: 'Alice Main' },
        },
        {
          documentId: 'doc-1',
          collection: 'orders',
          operation: 'update',
          fields: ['status'],
          before: { _id: 'doc-1', status: 'pending' },
          after: { _id: 'doc-1', status: 'completed' },
        },
      ];

      const branchChanges: TestChange[] = [
        {
          documentId: 'doc-1',
          collection: 'users',
          operation: 'update',
          fields: ['name'],
          before: { _id: 'doc-1', name: 'Alice' },
          after: { _id: 'doc-1', name: 'Alice Branch' },
        },
      ];

      const report = await detector.detectConflicts('feature', mainChanges, branchChanges);

      // Only users collection should have conflict
      expect(report.hasConflicts).toBe(true);
      expect(report.conflicts).toHaveLength(1);
      expect(report.conflicts[0].collection).toBe('users');
    });

    it('should throw error for non-existent branch', async () => {
      await expect(
        detector.detectConflicts('non-existent', [], [])
      ).rejects.toThrow(/branch.*not found/i);
    });
  });
});
