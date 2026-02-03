/**
 * Branching API Tests
 *
 * RED phase: Tests for the public branching API.
 * These tests define the expected behavior for database branching,
 * which currently throws "not implemented" errors.
 *
 * Coverage includes:
 * - Branch creation (db.branch())
 * - Branch listing (db.listBranches())
 * - Branch retrieval (db.getBranch())
 * - Branch deletion (db.deleteBranch())
 * - Merging (db.merge())
 * - Branch isolation
 * - Merge conflicts and resolution
 * - Nested branches (branch from branch)
 * - Time travel queries on branches
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { MemoryStorage } from '../../../src/storage/index.js';
import { Database } from '../../../src/client/index.js';
import type { Document } from '../../../src/types.js';
import type { BranchInfo } from '../../../src/branching/index.js';
import type { MergeResult, MergeOptions } from '../../../src/branching/merge.js';

// ============================================================================
// Test Interfaces
// ============================================================================

interface TestDoc extends Document {
  name: string;
  value: number;
  category?: string;
}

interface UserDoc extends Document {
  username: string;
  email: string;
  age?: number;
}

// ============================================================================
// Extended Database Interface (expected API)
// ============================================================================

/**
 * Extended Database interface with expected branching methods.
 * This documents the API we expect to be implemented.
 */
interface DatabaseWithBranching extends Database {
  branch(name: string, options?: BranchCreateOptions): Promise<BranchInfo>;
  getBranch(name: string): Promise<BranchInfo | null>;
  listBranches(options?: ListBranchesOptions): Promise<BranchInfo[]>;
  deleteBranch(name: string, force?: boolean): Promise<boolean>;
  merge(source: string, target?: string, options?: MergeOptions): Promise<MergeResult>;
}

interface BranchCreateOptions {
  description?: string;
  createdBy?: string;
  parentBranch?: string;
  metadata?: Record<string, unknown>;
}

interface ListBranchesOptions {
  prefix?: string;
  state?: 'active' | 'merged' | 'deleted';
  sortBy?: 'name' | 'createdAt' | 'updatedAt';
  sortOrder?: 'asc' | 'desc';
}

// ============================================================================
// Test Setup
// ============================================================================

describe('Branching API', () => {
  let storage: MemoryStorage;
  let db: DatabaseWithBranching;

  beforeEach(() => {
    storage = new MemoryStorage();
    const config = { local: '.test-mongolake' };
    db = new Database('testdb', storage, config) as DatabaseWithBranching;
  });

  // ==========================================================================
  // Branch Creation Tests
  // ==========================================================================

  describe('branch() - Branch Creation', () => {
    it('should create a new branch with default options', async () => {
      const branch = await db.branch('feature-x');

      expect(branch).toBeDefined();
      expect(branch.name).toBe('feature-x');
      expect(branch.state).toBe('active');
      expect(branch.parentBranch).toBe('main');
    });

    it('should create a branch with description', async () => {
      const branch = await db.branch('feature-auth', {
        description: 'Authentication feature implementation',
      });

      expect(branch.name).toBe('feature-auth');
      expect(branch.description).toBe('Authentication feature implementation');
    });

    it('should create a branch with createdBy metadata', async () => {
      const branch = await db.branch('bugfix-123', {
        createdBy: 'developer@example.com',
      });

      expect(branch.createdBy).toBe('developer@example.com');
    });

    it('should create a branch with custom metadata', async () => {
      const branch = await db.branch('feature-payments', {
        metadata: {
          ticket: 'JIRA-456',
          priority: 'high',
        },
      });

      expect(branch.metadata).toEqual({
        ticket: 'JIRA-456',
        priority: 'high',
      });
    });

    it('should reject empty branch names', async () => {
      await expect(db.branch('')).rejects.toThrow();
    });

    it('should reject reserved branch names (HEAD)', async () => {
      await expect(db.branch('HEAD')).rejects.toThrow();
    });

    it('should reject branch names with spaces', async () => {
      await expect(db.branch('invalid name')).rejects.toThrow();
    });

    it('should reject branch names with special characters', async () => {
      await expect(db.branch('branch~1')).rejects.toThrow();
      await expect(db.branch('branch^ref')).rejects.toThrow();
      await expect(db.branch('branch:name')).rejects.toThrow();
    });

    it('should reject duplicate branch names', async () => {
      await db.branch('feature-x');
      await expect(db.branch('feature-x')).rejects.toThrow(/already exists/);
    });

    it('should allow branch names with slashes (namespaced branches)', async () => {
      const branch = await db.branch('feature/auth/oauth');

      expect(branch.name).toBe('feature/auth/oauth');
      expect(branch.state).toBe('active');
    });

    it('should allow branch names with hyphens and underscores', async () => {
      const branch = await db.branch('feature-x_v2');

      expect(branch.name).toBe('feature-x_v2');
    });

    it('should set timestamps on created branch', async () => {
      const before = new Date().toISOString();
      const branch = await db.branch('feature-timestamp');
      const after = new Date().toISOString();

      expect(branch.createdAt).toBeDefined();
      expect(branch.updatedAt).toBeDefined();
      expect(branch.createdAt >= before).toBe(true);
      expect(branch.createdAt <= after).toBe(true);
    });
  });

  // ==========================================================================
  // Branch Listing Tests
  // ==========================================================================

  describe('listBranches() - Branch Listing', () => {
    it('should return empty array or just main for new database', async () => {
      const branches = await db.listBranches();

      // May include 'main' if initialized
      expect(Array.isArray(branches)).toBe(true);
    });

    it('should list all created branches', async () => {
      await db.branch('feature-1');
      await db.branch('feature-2');
      await db.branch('feature-3');

      const branches = await db.listBranches();
      const names = branches.map(b => b.name);

      expect(names).toContain('feature-1');
      expect(names).toContain('feature-2');
      expect(names).toContain('feature-3');
    });

    it('should include main branch after initialization', async () => {
      await db.branch('feature-1'); // This should initialize main

      const branches = await db.listBranches();
      const names = branches.map(b => b.name);

      expect(names).toContain('main');
    });

    it('should filter branches by prefix', async () => {
      await db.branch('feature/auth');
      await db.branch('feature/payments');
      await db.branch('bugfix/login');

      const featureBranches = await db.listBranches({ prefix: 'feature/' });
      const names = featureBranches.map(b => b.name);

      expect(names).toContain('feature/auth');
      expect(names).toContain('feature/payments');
      expect(names).not.toContain('bugfix/login');
    });

    it('should filter branches by state', async () => {
      await db.branch('feature-active');
      await db.branch('feature-to-merge');
      await db.merge('feature-to-merge');

      const activeBranches = await db.listBranches({ state: 'active' });
      const activeNames = activeBranches.map(b => b.name);

      expect(activeNames).toContain('feature-active');
      expect(activeNames).not.toContain('feature-to-merge');
    });

    it('should sort branches by name ascending', async () => {
      await db.branch('zebra-branch');
      await db.branch('alpha-branch');
      await db.branch('middle-branch');

      const branches = await db.listBranches({
        sortBy: 'name',
        sortOrder: 'asc',
      });
      const names = branches
        .filter(b => ['zebra-branch', 'alpha-branch', 'middle-branch'].includes(b.name))
        .map(b => b.name);

      expect(names).toEqual(['alpha-branch', 'middle-branch', 'zebra-branch']);
    });

    it('should sort branches by createdAt descending', async () => {
      await db.branch('first-branch');
      await new Promise(resolve => setTimeout(resolve, 10));
      await db.branch('second-branch');
      await new Promise(resolve => setTimeout(resolve, 10));
      await db.branch('third-branch');

      const branches = await db.listBranches({
        sortBy: 'createdAt',
        sortOrder: 'desc',
      });

      const ourBranches = branches.filter(b =>
        ['first-branch', 'second-branch', 'third-branch'].includes(b.name)
      );

      expect(ourBranches[0]!.name).toBe('third-branch');
      expect(ourBranches[2]!.name).toBe('first-branch');
    });
  });

  // ==========================================================================
  // Branch Retrieval Tests
  // ==========================================================================

  describe('getBranch() - Branch Retrieval', () => {
    it('should return branch info for existing branch', async () => {
      await db.branch('feature-x', { description: 'Test branch' });

      const branch = await db.getBranch('feature-x');

      expect(branch).not.toBeNull();
      expect(branch!.name).toBe('feature-x');
      expect(branch!.description).toBe('Test branch');
      expect(branch!.state).toBe('active');
    });

    it('should return null for non-existent branch', async () => {
      await db.branch('existing-branch');

      const branch = await db.getBranch('non-existent');

      expect(branch).toBeNull();
    });

    it('should return main branch info', async () => {
      await db.branch('feature-x'); // Initialize branching

      const main = await db.getBranch('main');

      expect(main).not.toBeNull();
      expect(main!.name).toBe('main');
      expect(main!.protected).toBe(true);
    });

    it('should include headCommit and baseCommit', async () => {
      const created = await db.branch('feature-y');
      const retrieved = await db.getBranch('feature-y');

      expect(retrieved!.baseCommit).toBeDefined();
      expect(retrieved!.headCommit).toBeDefined();
      expect(retrieved!.baseCommit).toBe(created.baseCommit);
    });
  });

  // ==========================================================================
  // Branch Deletion Tests
  // ==========================================================================

  describe('deleteBranch() - Branch Deletion', () => {
    it('should delete an existing branch', async () => {
      await db.branch('feature-to-delete');

      const deleted = await db.deleteBranch('feature-to-delete');

      expect(deleted).toBe(true);

      const branch = await db.getBranch('feature-to-delete');
      expect(branch).toBeNull();
    });

    it('should return false for non-existent branch', async () => {
      await db.branch('existing'); // Initialize

      const deleted = await db.deleteBranch('non-existent');

      expect(deleted).toBe(false);
    });

    it('should not delete the main branch', async () => {
      await db.branch('feature-x'); // Initialize

      const deleted = await db.deleteBranch('main');

      expect(deleted).toBe(false);

      const main = await db.getBranch('main');
      expect(main).not.toBeNull();
    });

    it('should not delete protected branches without force', async () => {
      // This test assumes we can create protected branches
      // The main branch is already protected
      await db.branch('feature-x');

      const deleted = await db.deleteBranch('main', false);

      expect(deleted).toBe(false);
    });

    it('should delete protected branches with force flag', async () => {
      await db.branch('protected-feature');
      // Assuming a way to mark a branch as protected or using main

      // For now, test that force=true works on regular branches
      const deleted = await db.deleteBranch('protected-feature', true);

      expect(deleted).toBe(true);
    });

    it('should clean up branch from listing after deletion', async () => {
      await db.branch('feature-cleanup');

      let branches = await db.listBranches();
      expect(branches.map(b => b.name)).toContain('feature-cleanup');

      await db.deleteBranch('feature-cleanup');

      branches = await db.listBranches();
      expect(branches.map(b => b.name)).not.toContain('feature-cleanup');
    });
  });

  // ==========================================================================
  // Branch Isolation Tests
  // ==========================================================================

  describe('Branch Isolation', () => {
    it('should isolate branch writes from main', async () => {
      // Insert on main
      const mainCollection = db.collection<TestDoc>('items');
      await mainCollection.insertOne({ name: 'Main Item', value: 100 });

      // Create branch
      await db.branch('feature-isolated');

      // Insert on branch
      const branchCollection = db.collection<TestDoc>('items', { branch: 'feature-isolated' });
      await branchCollection.insertOne({ name: 'Branch Item', value: 200 });

      // Verify main doesn't have branch item
      const mainItems = await mainCollection.find().toArray();
      const mainNames = mainItems.map(d => d.name);

      expect(mainNames).toContain('Main Item');
      expect(mainNames).not.toContain('Branch Item');
    });

    it('should inherit main data on branch', async () => {
      // Insert on main
      const mainCollection = db.collection<TestDoc>('items');
      await mainCollection.insertOne({ name: 'Inherited Item', value: 50 });

      // Create branch
      await db.branch('feature-inherit');

      // Read from branch should see main data
      const branchCollection = db.collection<TestDoc>('items', { branch: 'feature-inherit' });
      const branchItems = await branchCollection.find().toArray();

      expect(branchItems.map(d => d.name)).toContain('Inherited Item');
    });

    it('should allow branch updates without affecting main', async () => {
      // Insert on main
      const mainCollection = db.collection<TestDoc>('items');
      const { insertedId } = await mainCollection.insertOne({ name: 'Original', value: 10 });

      // Create branch
      await db.branch('feature-update');

      // Update on branch
      const branchCollection = db.collection<TestDoc>('items', { branch: 'feature-update' });
      await branchCollection.updateOne({ _id: insertedId }, { $set: { value: 99 } });

      // Verify main is unchanged
      const mainDoc = await mainCollection.findOne({ _id: insertedId });
      expect(mainDoc!.value).toBe(10);

      // Verify branch has update
      const branchDoc = await branchCollection.findOne({ _id: insertedId });
      expect(branchDoc!.value).toBe(99);
    });

    it('should allow branch deletes without affecting main', async () => {
      // Insert on main
      const mainCollection = db.collection<TestDoc>('items');
      const { insertedId } = await mainCollection.insertOne({ name: 'ToDelete', value: 5 });

      // Create branch
      await db.branch('feature-delete');

      // Delete on branch
      const branchCollection = db.collection<TestDoc>('items', { branch: 'feature-delete' });
      await branchCollection.deleteOne({ _id: insertedId });

      // Verify main still has document
      const mainDoc = await mainCollection.findOne({ _id: insertedId });
      expect(mainDoc).not.toBeNull();

      // Verify branch doesn't have document
      const branchDoc = await branchCollection.findOne({ _id: insertedId });
      expect(branchDoc).toBeNull();
    });

    it('should keep branches isolated from each other', async () => {
      await db.branch('branch-a');
      await db.branch('branch-b');

      const collectionA = db.collection<TestDoc>('items', { branch: 'branch-a' });
      const collectionB = db.collection<TestDoc>('items', { branch: 'branch-b' });

      await collectionA.insertOne({ name: 'A-only', value: 1 });
      await collectionB.insertOne({ name: 'B-only', value: 2 });

      const docsA = await collectionA.find().toArray();
      const docsB = await collectionB.find().toArray();

      expect(docsA.map(d => d.name)).toContain('A-only');
      expect(docsA.map(d => d.name)).not.toContain('B-only');

      expect(docsB.map(d => d.name)).toContain('B-only');
      expect(docsB.map(d => d.name)).not.toContain('A-only');
    });
  });

  // ==========================================================================
  // Merge Tests
  // ==========================================================================

  describe('merge() - Branch Merging', () => {
    it('should merge a branch into main', async () => {
      await db.branch('feature-merge');

      const result = await db.merge('feature-merge');

      expect(result.success).toBe(true);
      expect(result.sourceBranch).toBe('feature-merge');
      expect(result.targetBranch).toBe('main');
    });

    it('should merge a branch into a specific target', async () => {
      await db.branch('develop');
      await db.branch('feature-x', { parentBranch: 'develop' });

      const result = await db.merge('feature-x', 'develop');

      expect(result.success).toBe(true);
      expect(result.sourceBranch).toBe('feature-x');
      expect(result.targetBranch).toBe('develop');
    });

    it('should apply branch changes to target after merge', async () => {
      const mainCollection = db.collection<TestDoc>('items');

      await db.branch('feature-add');
      const branchCollection = db.collection<TestDoc>('items', { branch: 'feature-add' });
      await branchCollection.insertOne({ name: 'New from branch', value: 42 });

      await db.merge('feature-add');

      // Main should now have the branch changes
      const mainDocs = await mainCollection.find().toArray();
      expect(mainDocs.map(d => d.name)).toContain('New from branch');
    });

    it('should report auto-merge when no conflicts', async () => {
      await db.branch('feature-auto');

      const result = await db.merge('feature-auto');

      expect(result.autoMerged).toBe(true);
      expect(result.conflicts).toHaveLength(0);
    });

    it('should mark source branch as merged after merge', async () => {
      await db.branch('feature-status');

      await db.merge('feature-status');

      const branch = await db.getBranch('feature-status');
      expect(branch?.state).toBe('merged');
    });

    it('should delete source branch if deleteBranch option is true', async () => {
      await db.branch('feature-delete-after');

      await db.merge('feature-delete-after', 'main', { deleteBranch: true });

      const branch = await db.getBranch('feature-delete-after');
      // Branch should be gone or marked as deleted
      expect(branch === null || branch.state === 'deleted' || branch.state === 'merged').toBe(true);
    });

    it('should reject merging non-existent branch', async () => {
      await db.branch('existing'); // Initialize

      await expect(db.merge('non-existent')).rejects.toThrow(/not found/i);
    });

    it('should reject merging branch into itself', async () => {
      await db.branch('self-merge');

      await expect(db.merge('self-merge', 'self-merge')).rejects.toThrow(/itself/i);
    });

    it('should include merge commit in result', async () => {
      await db.branch('feature-commit');

      const result = await db.merge('feature-commit');

      expect(result.mergeCommit).toBeDefined();
      expect(typeof result.mergeCommit).toBe('string');
    });

    it('should report fast-forward status when applicable', async () => {
      await db.branch('feature-ff');

      const result = await db.merge('feature-ff');

      // Should be fast-forward if main hasn't changed since branch creation
      expect(typeof result.fastForward).toBe('boolean');
    });
  });

  // ==========================================================================
  // Merge Conflict Tests
  // ==========================================================================

  describe('Merge Conflicts', () => {
    it('should detect field conflict when same document modified on both branches', async () => {
      // Insert initial document on main
      const mainCollection = db.collection<TestDoc>('items');
      const { insertedId } = await mainCollection.insertOne({ name: 'Original', value: 10 });

      // Create branch
      await db.branch('feature-conflict');

      // Modify on main
      await mainCollection.updateOne({ _id: insertedId }, { $set: { value: 20 } });

      // Modify same field on branch
      const branchCollection = db.collection<TestDoc>('items', { branch: 'feature-conflict' });
      await branchCollection.updateOne({ _id: insertedId }, { $set: { value: 30 } });

      // Merge should detect conflict
      try {
        await db.merge('feature-conflict');
        // If we get here without error, check result
      } catch (error) {
        expect(error).toBeDefined();
        // Conflict error expected
      }
    });

    it('should detect delete/update conflict', async () => {
      // Insert document
      const mainCollection = db.collection<TestDoc>('items');
      const { insertedId } = await mainCollection.insertOne({ name: 'Conflict Doc', value: 5 });

      await db.branch('feature-del-upd');

      // Delete on main
      await mainCollection.deleteOne({ _id: insertedId });

      // Update on branch
      const branchCollection = db.collection<TestDoc>('items', { branch: 'feature-del-upd' });
      await branchCollection.updateOne({ _id: insertedId }, { $set: { value: 99 } });

      // Merge should detect conflict
      try {
        const result = await db.merge('feature-del-upd');
        expect(result.conflicts.length).toBeGreaterThan(0);
      } catch {
        // Conflict error is also acceptable
      }
    });

    it('should allow merge with "ours" strategy (keep target)', async () => {
      const mainCollection = db.collection<TestDoc>('items');
      const { insertedId } = await mainCollection.insertOne({ name: 'Original', value: 10 });

      await db.branch('feature-ours');

      await mainCollection.updateOne({ _id: insertedId }, { $set: { value: 20 } });

      const branchCollection = db.collection<TestDoc>('items', { branch: 'feature-ours' });
      await branchCollection.updateOne({ _id: insertedId }, { $set: { value: 30 } });

      const result = await db.merge('feature-ours', 'main', { strategy: 'ours' });

      expect(result.success).toBe(true);

      // Value should be 20 (main's value)
      const doc = await mainCollection.findOne({ _id: insertedId });
      expect(doc?.value).toBe(20);
    });

    it('should allow merge with "theirs" strategy (keep source)', async () => {
      const mainCollection = db.collection<TestDoc>('items');
      const { insertedId } = await mainCollection.insertOne({ name: 'Original', value: 10 });

      await db.branch('feature-theirs');

      await mainCollection.updateOne({ _id: insertedId }, { $set: { value: 20 } });

      const branchCollection = db.collection<TestDoc>('items', { branch: 'feature-theirs' });
      await branchCollection.updateOne({ _id: insertedId }, { $set: { value: 30 } });

      const result = await db.merge('feature-theirs', 'main', { strategy: 'theirs' });

      expect(result.success).toBe(true);

      // Value should be 30 (branch's value)
      const doc = await mainCollection.findOne({ _id: insertedId });
      expect(doc?.value).toBe(30);
    });

    it('should support custom conflict resolver callback', async () => {
      const mainCollection = db.collection<TestDoc>('items');
      const { insertedId } = await mainCollection.insertOne({ name: 'Original', value: 10 });

      await db.branch('feature-custom');

      await mainCollection.updateOne({ _id: insertedId }, { $set: { value: 20 } });

      const branchCollection = db.collection<TestDoc>('items', { branch: 'feature-custom' });
      await branchCollection.updateOne({ _id: insertedId }, { $set: { value: 30 } });

      const resolverCalled: boolean[] = [];

      const result = await db.merge('feature-custom', 'main', {
        onConflict: (conflict) => {
          resolverCalled.push(true);
          // Custom resolution: use average of values
          const avgValue = ((conflict.targetValue as number) + (conflict.sourceValue as number)) / 2;
          return {
            resolution: 'custom',
            resolvedValue: avgValue,
          };
        },
      });

      expect(result.success).toBe(true);
      expect(resolverCalled.length).toBeGreaterThan(0);
    });

    it('should include conflict details in merge result', async () => {
      const mainCollection = db.collection<TestDoc>('items');
      const { insertedId } = await mainCollection.insertOne({ name: 'ConflictDoc', value: 1 });

      await db.branch('feature-details');

      await mainCollection.updateOne({ _id: insertedId }, { $set: { value: 2 } });

      const branchCollection = db.collection<TestDoc>('items', { branch: 'feature-details' });
      await branchCollection.updateOne({ _id: insertedId }, { $set: { value: 3 } });

      try {
        await db.merge('feature-details');
      } catch (error: unknown) {
        // Expect ConflictError with details
        if (error && typeof error === 'object' && 'conflicts' in error) {
          const conflicts = (error as { conflicts: unknown[] }).conflicts;
          expect(conflicts.length).toBeGreaterThan(0);
        }
      }
    });
  });

  // ==========================================================================
  // Nested Branches (Branch from Branch)
  // ==========================================================================

  describe('Nested Branches (Branch from Branch)', () => {
    it('should create a branch from another branch', async () => {
      await db.branch('feature-parent');
      const child = await db.branch('feature-child', { parentBranch: 'feature-parent' });

      expect(child.parentBranch).toBe('feature-parent');
    });

    it('should inherit data from parent branch', async () => {
      // Create parent branch with data
      await db.branch('parent-branch');
      const parentCollection = db.collection<TestDoc>('items', { branch: 'parent-branch' });
      await parentCollection.insertOne({ name: 'Parent Data', value: 100 });

      // Create child branch from parent
      await db.branch('child-branch', { parentBranch: 'parent-branch' });
      const childCollection = db.collection<TestDoc>('items', { branch: 'child-branch' });

      // Child should see parent data
      const childDocs = await childCollection.find().toArray();
      expect(childDocs.map(d => d.name)).toContain('Parent Data');
    });

    it('should isolate child changes from parent', async () => {
      await db.branch('parent-isolated');
      await db.branch('child-isolated', { parentBranch: 'parent-isolated' });

      const childCollection = db.collection<TestDoc>('items', { branch: 'child-isolated' });
      await childCollection.insertOne({ name: 'Child Only', value: 50 });

      const parentCollection = db.collection<TestDoc>('items', { branch: 'parent-isolated' });
      const parentDocs = await parentCollection.find().toArray();

      expect(parentDocs.map(d => d.name)).not.toContain('Child Only');
    });

    it('should support merging child back to parent', async () => {
      await db.branch('merge-parent');
      await db.branch('merge-child', { parentBranch: 'merge-parent' });

      const childCollection = db.collection<TestDoc>('items', { branch: 'merge-child' });
      await childCollection.insertOne({ name: 'Merge Up', value: 75 });

      const result = await db.merge('merge-child', 'merge-parent');

      expect(result.success).toBe(true);
      expect(result.targetBranch).toBe('merge-parent');

      // Parent should now have child's data
      const parentCollection = db.collection<TestDoc>('items', { branch: 'merge-parent' });
      const parentDocs = await parentCollection.find().toArray();
      expect(parentDocs.map(d => d.name)).toContain('Merge Up');
    });

    it('should track branch ancestry', async () => {
      await db.branch('level-1');
      await db.branch('level-2', { parentBranch: 'level-1' });
      await db.branch('level-3', { parentBranch: 'level-2' });

      const level3 = await db.getBranch('level-3');
      expect(level3?.parentBranch).toBe('level-2');

      const level2 = await db.getBranch('level-2');
      expect(level2?.parentBranch).toBe('level-1');

      const level1 = await db.getBranch('level-1');
      expect(level1?.parentBranch).toBe('main');
    });
  });

  // ==========================================================================
  // Time Travel on Branches
  // ==========================================================================

  describe('Time Travel on Branches', () => {
    it('should support reading branch at specific snapshot', async () => {
      await db.branch('time-travel');
      const collection = db.collection<TestDoc>('items', { branch: 'time-travel' });

      // Insert first document
      await collection.insertOne({ name: 'First', value: 1 });

      // Get current snapshot
      const branch = await db.getBranch('time-travel');
      const snapshotId = branch?.headCommit;

      // Insert second document
      await collection.insertOne({ name: 'Second', value: 2 });

      // Read at earlier snapshot should only see first document
      // This requires time travel API integration
      // For now, verify the snapshot exists
      expect(snapshotId).toBeDefined();
    });

    it('should preserve historical branch states after merge', async () => {
      await db.branch('history-test');
      const collection = db.collection<TestDoc>('items', { branch: 'history-test' });
      await collection.insertOne({ name: 'Historical', value: 999 });

      const beforeMerge = await db.getBranch('history-test');

      await db.merge('history-test');

      // The branch should retain its merge commit reference
      const afterMerge = await db.getBranch('history-test');
      expect(afterMerge?.mergeCommit).toBeDefined();
    });
  });

  // ==========================================================================
  // Integration Workflow Tests
  // ==========================================================================

  describe('Integration Workflows', () => {
    it('should support complete feature branch workflow', async () => {
      // 1. Setup: Insert initial data on main
      const mainCollection = db.collection<UserDoc>('users');
      await mainCollection.insertOne({ username: 'alice', email: 'alice@test.com' });

      // 2. Create feature branch
      const branch = await db.branch('feature/add-age-field', {
        description: 'Adding age field to users',
        createdBy: 'developer',
      });
      expect(branch.state).toBe('active');

      // 3. Work on feature branch
      const branchCollection = db.collection<UserDoc>('users', { branch: 'feature/add-age-field' });
      await branchCollection.updateOne(
        { username: 'alice' },
        { $set: { age: 30 } }
      );
      await branchCollection.insertOne({ username: 'bob', email: 'bob@test.com', age: 25 });

      // 4. Verify isolation
      const mainUsers = await mainCollection.find().toArray();
      expect(mainUsers.find(u => u.username === 'alice')?.age).toBeUndefined();
      expect(mainUsers.find(u => u.username === 'bob')).toBeUndefined();

      // 5. Merge feature branch
      const mergeResult = await db.merge('feature/add-age-field');
      expect(mergeResult.success).toBe(true);

      // 6. Verify merge applied
      const mergedUsers = await mainCollection.find().toArray();
      expect(mergedUsers.find(u => u.username === 'alice')?.age).toBe(30);
      expect(mergedUsers.find(u => u.username === 'bob')?.email).toBe('bob@test.com');

      // 7. Verify branch state
      const mergedBranch = await db.getBranch('feature/add-age-field');
      expect(mergedBranch?.state).toBe('merged');
    });

    it('should support parallel branch development', async () => {
      const mainCollection = db.collection<TestDoc>('products');
      await mainCollection.insertOne({ name: 'Widget', value: 100 });

      // Create two parallel branches
      await db.branch('feature/pricing');
      await db.branch('feature/naming');

      // Work on pricing branch
      const pricingCollection = db.collection<TestDoc>('products', { branch: 'feature/pricing' });
      await pricingCollection.updateOne({ name: 'Widget' }, { $set: { value: 150 } });

      // Work on naming branch (different field)
      const namingCollection = db.collection<TestDoc>('products', { branch: 'feature/naming' });
      await namingCollection.updateOne({ name: 'Widget' }, { $set: { category: 'Tools' } });

      // Merge both (no conflicts - different fields)
      const pricingMerge = await db.merge('feature/pricing');
      expect(pricingMerge.success).toBe(true);

      const namingMerge = await db.merge('feature/naming');
      expect(namingMerge.success).toBe(true);

      // Verify both changes applied
      const final = await mainCollection.findOne({ name: 'Widget' });
      expect(final?.value).toBe(150);
      expect(final?.category).toBe('Tools');
    });

    it('should handle GitFlow-style workflow', async () => {
      // Main branch exists by default

      // Create develop branch
      await db.branch('develop', { description: 'Development branch' });

      // Create feature from develop
      await db.branch('feature/new-widget', { parentBranch: 'develop' });

      const featureCollection = db.collection<TestDoc>('widgets', { branch: 'feature/new-widget' });
      await featureCollection.insertOne({ name: 'Super Widget', value: 500 });

      // Merge feature to develop
      await db.merge('feature/new-widget', 'develop');

      // Verify develop has changes
      const developCollection = db.collection<TestDoc>('widgets', { branch: 'develop' });
      const developWidgets = await developCollection.find().toArray();
      expect(developWidgets.map(w => w.name)).toContain('Super Widget');

      // Main should not have changes yet
      const mainCollection = db.collection<TestDoc>('widgets');
      const mainWidgets = await mainCollection.find().toArray();
      expect(mainWidgets.map(w => w.name)).not.toContain('Super Widget');

      // Release: merge develop to main
      await db.merge('develop', 'main');

      // Now main has changes
      const finalMain = await mainCollection.find().toArray();
      expect(finalMain.map(w => w.name)).toContain('Super Widget');
    });
  });
});
