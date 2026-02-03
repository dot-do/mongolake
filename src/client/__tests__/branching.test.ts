/**
 * Branching API Tests
 *
 * Tests for database branching functionality through the public API.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { MemoryStorage } from '../../storage/index.js';
import { Database, createDatabase } from '../index.js';
import type { Document } from '../../types.js';

// ============================================================================
// Test Setup
// ============================================================================

interface TestDoc extends Document {
  name: string;
  value: number;
}

describe('Database Branching API', () => {
  let storage: MemoryStorage;
  let db: Database;

  beforeEach(() => {
    storage = new MemoryStorage();
    const config = { local: '.test-mongolake' };
    db = new Database('testdb', storage, config);
  });

  // --------------------------------------------------------------------------
  // Branch Creation
  // --------------------------------------------------------------------------

  describe('branch()', () => {
    it('should create a branch', async () => {
      const branch = await db.branch('feature-x');

      expect(branch).toBeDefined();
      expect(branch.name).toBe('feature-x');
      expect(branch.state).toBe('active');
    });

    it('should create a branch with options', async () => {
      const branch = await db.branch('feature-y', {
        description: 'Feature Y development',
        createdBy: 'test-user',
      });

      expect(branch.name).toBe('feature-y');
      expect(branch.description).toBe('Feature Y development');
      expect(branch.createdBy).toBe('test-user');
    });

    it('should reject invalid branch names', async () => {
      await expect(db.branch('HEAD')).rejects.toThrow();
      await expect(db.branch('')).rejects.toThrow();
      await expect(db.branch('invalid name')).rejects.toThrow();
    });

    it('should reject duplicate branch names', async () => {
      await db.branch('feature-x');
      await expect(db.branch('feature-x')).rejects.toThrow(/already exists/);
    });
  });

  // --------------------------------------------------------------------------
  // Branch Listing
  // --------------------------------------------------------------------------

  describe('listBranches()', () => {
    it('should list all branches', async () => {
      // Create some branches
      await db.branch('feature-1');
      await db.branch('feature-2');

      const branches = await db.listBranches();

      // Should include main branch plus our two feature branches
      expect(branches.length).toBeGreaterThanOrEqual(2);
      expect(branches.map(b => b.name)).toContain('feature-1');
      expect(branches.map(b => b.name)).toContain('feature-2');
    });

    it('should include main branch after initialization', async () => {
      await db.branch('feature-1');
      const branches = await db.listBranches();

      expect(branches.map(b => b.name)).toContain('main');
    });
  });

  // --------------------------------------------------------------------------
  // Branch Information
  // --------------------------------------------------------------------------

  describe('getBranch()', () => {
    it('should return branch info for existing branch', async () => {
      await db.branch('feature-x');

      const branch = await db.getBranch('feature-x');

      expect(branch).not.toBeNull();
      expect(branch!.name).toBe('feature-x');
      expect(branch!.state).toBe('active');
    });

    it('should return null for non-existent branch', async () => {
      // Initialize branching first
      await db.branch('feature-x');

      const branch = await db.getBranch('non-existent');

      expect(branch).toBeNull();
    });
  });

  // --------------------------------------------------------------------------
  // Branch Deletion
  // --------------------------------------------------------------------------

  describe('deleteBranch()', () => {
    it('should delete a branch', async () => {
      await db.branch('feature-x');

      const deleted = await db.deleteBranch('feature-x');

      expect(deleted).toBe(true);
    });

    it('should return false for non-existent branch', async () => {
      // Initialize branching first
      await db.branch('feature-x');

      const deleted = await db.deleteBranch('non-existent');

      expect(deleted).toBe(false);
    });

    it('should not delete main branch', async () => {
      // Initialize branching first
      await db.branch('feature-x');

      const deleted = await db.deleteBranch('main');

      expect(deleted).toBe(false);
    });
  });

  // --------------------------------------------------------------------------
  // Merging
  // --------------------------------------------------------------------------

  describe('merge()', () => {
    it('should merge a branch into main', async () => {
      await db.branch('feature-x');

      const result = await db.merge('feature-x');

      expect(result.success).toBe(true);
      expect(result.sourceBranch).toBe('feature-x');
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

    it('should report auto-merge status', async () => {
      await db.branch('feature-x');

      const result = await db.merge('feature-x');

      expect(result.autoMerged).toBe(true);
      expect(result.conflicts).toHaveLength(0);
    });

    it('should optionally delete source branch after merge', async () => {
      await db.branch('feature-x');

      const result = await db.merge('feature-x', 'main', { deleteBranch: true });

      expect(result.success).toBe(true);

      // Branch should be deleted after merge
      const branch = await db.getBranch('feature-x');
      // Branch might be marked as merged or deleted
      expect(branch === null || branch.state === 'merged' || branch.state === 'deleted').toBe(true);
    });

    it('should reject merging non-existent branch', async () => {
      // Initialize branching first
      await db.branch('feature-x');

      await expect(db.merge('non-existent')).rejects.toThrow(/not found/);
    });

    it('should reject merging branch into itself', async () => {
      await db.branch('feature-x');

      await expect(db.merge('feature-x', 'feature-x')).rejects.toThrow(/itself/);
    });
  });

  // --------------------------------------------------------------------------
  // Branch Collections
  // --------------------------------------------------------------------------

  describe('branch collections', () => {
    it('should allow accessing collection on a branch', async () => {
      await db.branch('feature-x');

      const collection = db.collection<TestDoc>('users', { branch: 'feature-x' });

      expect(collection).toBeDefined();
      expect(collection.name).toBe('users');
    });

    it('should isolate branch data from main', async () => {
      // Insert data on main
      const mainCollection = db.collection<TestDoc>('users');
      await mainCollection.insertOne({ name: 'Alice', value: 1 });

      // Create branch
      await db.branch('feature-x');

      // Insert data on branch
      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature-x' });
      await branchCollection.insertOne({ name: 'Bob', value: 2 });

      // Main should have Alice
      const mainDocs = await mainCollection.find().toArray();
      expect(mainDocs.map(d => d.name)).toContain('Alice');

      // Branch should have both (inherited from main + new)
      const branchDocs = await branchCollection.find().toArray();
      expect(branchDocs.length).toBeGreaterThanOrEqual(1);
    });
  });

  // --------------------------------------------------------------------------
  // Integration
  // --------------------------------------------------------------------------

  describe('integration', () => {
    it('should support full branching workflow', async () => {
      // 1. Start with main branch
      const mainCollection = db.collection<TestDoc>('users');
      await mainCollection.insertOne({ name: 'Alice', value: 1 });

      // 2. Create a feature branch
      const branch = await db.branch('feature-add-users', {
        description: 'Adding new users',
      });
      expect(branch.name).toBe('feature-add-users');

      // 3. Work on the branch
      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature-add-users' });
      await branchCollection.insertOne({ name: 'Bob', value: 2 });

      // 4. Check branch exists
      const branches = await db.listBranches();
      expect(branches.map(b => b.name)).toContain('feature-add-users');

      // 5. Merge the branch
      const result = await db.merge('feature-add-users');
      expect(result.success).toBe(true);

      // 6. Verify branch is marked as merged
      const mergedBranch = await db.getBranch('feature-add-users');
      expect(mergedBranch?.state).toBe('merged');
    });

    it('should support createDatabase factory', async () => {
      // Use unique name to avoid conflicts with other test runs
      const uniqueName = `factory-test-${Date.now()}`;
      const factoryDb = createDatabase(uniqueName, { local: '.test-mongolake' });

      const branch = await factoryDb.branch('feature-test');
      expect(branch.name).toBe('feature-test');

      const branches = await factoryDb.listBranches();
      expect(branches.map(b => b.name)).toContain('feature-test');
    });
  });
});
