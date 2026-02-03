/**
 * Branch Metadata Storage Tests
 *
 * Tests for branch creation, listing, retrieval, and deletion.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  BranchStore,
  createBranchStore,
  createBranch,
  getBranch,
  listBranches,
  deleteBranch,
  isValidBranchName,
  normalizeBranchName,
  DEFAULT_BRANCH,
  BRANCHES_DIR,
  BRANCH_FILE_EXTENSION,
  type BranchMetadata,
} from '../metadata.js';
import { MemoryStorage } from '../../storage/index.js';

// ============================================================================
// Branch Name Validation Tests
// ============================================================================

describe('Branch Name Validation', () => {
  describe('isValidBranchName', () => {
    it('should accept valid simple names', () => {
      expect(isValidBranchName('main')).toBe(true);
      expect(isValidBranchName('develop')).toBe(true);
      expect(isValidBranchName('feature')).toBe(true);
      expect(isValidBranchName('v1')).toBe(true);
    });

    it('should accept names with hyphens', () => {
      expect(isValidBranchName('feature-branch')).toBe(true);
      expect(isValidBranchName('my-feature-branch')).toBe(true);
      expect(isValidBranchName('v1-0-0')).toBe(true);
    });

    it('should accept names with underscores', () => {
      expect(isValidBranchName('feature_branch')).toBe(true);
      expect(isValidBranchName('my_feature_branch')).toBe(true);
    });

    it('should accept names with slashes', () => {
      expect(isValidBranchName('feature/my-feature')).toBe(true);
      expect(isValidBranchName('feature/auth/login')).toBe(true);
      expect(isValidBranchName('user/john/feature')).toBe(true);
    });

    it('should accept single character names', () => {
      expect(isValidBranchName('a')).toBe(true);
      expect(isValidBranchName('1')).toBe(true);
    });

    it('should reject empty names', () => {
      expect(isValidBranchName('')).toBe(false);
    });

    it('should reject names that are too long', () => {
      const longName = 'a'.repeat(256);
      expect(isValidBranchName(longName)).toBe(false);
    });

    it('should reject reserved names', () => {
      expect(isValidBranchName('HEAD')).toBe(false);
      expect(isValidBranchName('head')).toBe(false);
      expect(isValidBranchName('refs')).toBe(false);
      expect(isValidBranchName('objects')).toBe(false);
    });

    it('should reject names starting with special characters', () => {
      expect(isValidBranchName('-feature')).toBe(false);
      expect(isValidBranchName('_feature')).toBe(false);
      expect(isValidBranchName('/feature')).toBe(false);
    });

    it('should reject names ending with slash', () => {
      expect(isValidBranchName('feature/')).toBe(false);
    });

    it('should reject names with consecutive slashes', () => {
      expect(isValidBranchName('feature//branch')).toBe(false);
    });

    it('should reject names with consecutive dots', () => {
      expect(isValidBranchName('feature..branch')).toBe(false);
    });
  });

  describe('normalizeBranchName', () => {
    it('should trim whitespace', () => {
      expect(normalizeBranchName('  main  ')).toBe('main');
      expect(normalizeBranchName('\tmain\n')).toBe('main');
    });

    it('should preserve valid names', () => {
      expect(normalizeBranchName('feature-branch')).toBe('feature-branch');
      expect(normalizeBranchName('feature/branch')).toBe('feature/branch');
    });
  });
});

// ============================================================================
// BranchStore Tests
// ============================================================================

describe('BranchStore', () => {
  let storage: MemoryStorage;
  let store: BranchStore;

  beforeEach(() => {
    storage = new MemoryStorage();
    store = new BranchStore(storage, 'testdb');
  });

  // ==========================================================================
  // createBranch Tests
  // ==========================================================================

  describe('createBranch', () => {
    it('should create a branch with all required fields', async () => {
      const branch = await store.createBranch({
        name: 'feature-branch',
        baseCommit: 'abc123',
      });

      expect(branch.name).toBe('feature-branch');
      expect(branch.baseCommit).toBe('abc123');
      expect(branch.headCommit).toBe('abc123');
      expect(branch.createdAt).toBeTruthy();
      expect(branch.updatedAt).toBeTruthy();
    });

    it('should create a branch with optional fields', async () => {
      const branch = await store.createBranch({
        name: 'feature-branch',
        baseCommit: 'abc123',
        description: 'My feature branch',
        createdBy: 'user@example.com',
        protected: true,
        metadata: { priority: 'high' },
      });

      expect(branch.description).toBe('My feature branch');
      expect(branch.createdBy).toBe('user@example.com');
      expect(branch.protected).toBe(true);
      expect(branch.metadata).toEqual({ priority: 'high' });
    });

    it('should throw error for invalid branch name', async () => {
      await expect(
        store.createBranch({
          name: 'HEAD',
          baseCommit: 'abc123',
        })
      ).rejects.toThrow('Invalid branch name');
    });

    it('should throw error if branch already exists', async () => {
      await store.createBranch({
        name: 'feature',
        baseCommit: 'abc123',
      });

      await expect(
        store.createBranch({
          name: 'feature',
          baseCommit: 'def456',
        })
      ).rejects.toThrow('already exists');
    });

    it('should throw error if no base commit provided and no default branch', async () => {
      await expect(
        store.createBranch({
          name: 'feature',
        })
      ).rejects.toThrow('No base commit specified');
    });

    it('should use current commit from default branch if no base commit specified', async () => {
      // Initialize default branch first
      await store.initializeDefaultBranch('initial-commit');

      const branch = await store.createBranch({
        name: 'feature',
      });

      expect(branch.baseCommit).toBe('initial-commit');
    });

    it('should write branch to storage', async () => {
      await store.createBranch({
        name: 'feature',
        baseCommit: 'abc123',
      });

      const data = await storage.get('testdb/branches/feature.json');
      expect(data).not.toBeNull();

      const parsed = JSON.parse(new TextDecoder().decode(data!));
      expect(parsed.name).toBe('feature');
    });
  });

  // ==========================================================================
  // getBranch Tests
  // ==========================================================================

  describe('getBranch', () => {
    it('should retrieve an existing branch', async () => {
      await store.createBranch({
        name: 'feature',
        baseCommit: 'abc123',
        description: 'Test branch',
      });

      const branch = await store.getBranch('feature');

      expect(branch).not.toBeNull();
      expect(branch!.name).toBe('feature');
      expect(branch!.description).toBe('Test branch');
    });

    it('should return null for non-existent branch', async () => {
      const branch = await store.getBranch('non-existent');

      expect(branch).toBeNull();
    });

    it('should handle branches with slashes in name', async () => {
      await store.createBranch({
        name: 'feature/auth/login',
        baseCommit: 'abc123',
      });

      const branch = await store.getBranch('feature/auth/login');

      expect(branch).not.toBeNull();
      expect(branch!.name).toBe('feature/auth/login');
    });
  });

  // ==========================================================================
  // listBranches Tests
  // ==========================================================================

  describe('listBranches', () => {
    beforeEach(async () => {
      // Create test branches with different timestamps
      await store.createBranch({
        name: 'main',
        baseCommit: 'abc123',
        protected: true,
      });

      // Small delay to ensure different timestamps
      await new Promise((r) => setTimeout(r, 10));

      await store.createBranch({
        name: 'develop',
        baseCommit: 'def456',
      });

      await new Promise((r) => setTimeout(r, 10));

      await store.createBranch({
        name: 'feature/auth',
        baseCommit: 'ghi789',
      });
    });

    it('should list all branches', async () => {
      const branches = await store.listBranches();

      expect(branches).toHaveLength(3);
      expect(branches.map((b) => b.name)).toContain('main');
      expect(branches.map((b) => b.name)).toContain('develop');
      expect(branches.map((b) => b.name)).toContain('feature/auth');
    });

    it('should filter by prefix', async () => {
      const branches = await store.listBranches({ prefix: 'feature' });

      expect(branches).toHaveLength(1);
      expect(branches[0].name).toBe('feature/auth');
    });

    it('should filter protected branches', async () => {
      const branches = await store.listBranches({ protectedOnly: true });

      expect(branches).toHaveLength(1);
      expect(branches[0].name).toBe('main');
    });

    it('should sort by name ascending', async () => {
      const branches = await store.listBranches({ sortBy: 'name', sortOrder: 'asc' });

      expect(branches[0].name).toBe('develop');
      expect(branches[1].name).toBe('feature/auth');
      expect(branches[2].name).toBe('main');
    });

    it('should sort by name descending', async () => {
      const branches = await store.listBranches({ sortBy: 'name', sortOrder: 'desc' });

      expect(branches[0].name).toBe('main');
      expect(branches[2].name).toBe('develop');
    });

    it('should sort by createdAt', async () => {
      const branches = await store.listBranches({ sortBy: 'createdAt', sortOrder: 'asc' });

      expect(branches[0].name).toBe('main');
      expect(branches[2].name).toBe('feature/auth');
    });

    it('should return empty array when no branches exist', async () => {
      const emptyStore = new BranchStore(new MemoryStorage(), 'emptydb');
      const branches = await emptyStore.listBranches();

      expect(branches).toEqual([]);
    });
  });

  // ==========================================================================
  // updateBranch Tests
  // ==========================================================================

  describe('updateBranch', () => {
    it('should update branch description', async () => {
      await store.createBranch({
        name: 'feature',
        baseCommit: 'abc123',
      });

      const updated = await store.updateBranch('feature', {
        description: 'Updated description',
      });

      expect(updated.description).toBe('Updated description');
    });

    it('should update branch headCommit', async () => {
      await store.createBranch({
        name: 'feature',
        baseCommit: 'abc123',
      });

      const updated = await store.updateBranch('feature', {
        headCommit: 'def456',
      });

      expect(updated.headCommit).toBe('def456');
      expect(updated.baseCommit).toBe('abc123'); // Should remain unchanged
    });

    it('should update branch protected status', async () => {
      await store.createBranch({
        name: 'feature',
        baseCommit: 'abc123',
      });

      const updated = await store.updateBranch('feature', {
        protected: true,
      });

      expect(updated.protected).toBe(true);
    });

    it('should merge metadata', async () => {
      await store.createBranch({
        name: 'feature',
        baseCommit: 'abc123',
        metadata: { key1: 'value1' },
      });

      const updated = await store.updateBranch('feature', {
        metadata: { key2: 'value2' },
      });

      expect(updated.metadata).toEqual({ key1: 'value1', key2: 'value2' });
    });

    it('should update updatedAt timestamp', async () => {
      await store.createBranch({
        name: 'feature',
        baseCommit: 'abc123',
      });

      const original = await store.getBranch('feature');
      await new Promise((r) => setTimeout(r, 10));

      const updated = await store.updateBranch('feature', {
        description: 'New description',
      });

      expect(new Date(updated.updatedAt).getTime()).toBeGreaterThan(
        new Date(original!.updatedAt).getTime()
      );
    });

    it('should throw error for non-existent branch', async () => {
      await expect(
        store.updateBranch('non-existent', { description: 'test' })
      ).rejects.toThrow('not found');
    });
  });

  // ==========================================================================
  // deleteBranch Tests
  // ==========================================================================

  describe('deleteBranch', () => {
    it('should delete an existing branch', async () => {
      await store.createBranch({
        name: 'feature',
        baseCommit: 'abc123',
      });

      const result = await store.deleteBranch('feature');

      expect(result.deleted).toBe(true);
      expect(result.name).toBe('feature');

      const branch = await store.getBranch('feature');
      expect(branch).toBeNull();
    });

    it('should not delete the default branch', async () => {
      await store.initializeDefaultBranch('abc123');

      const result = await store.deleteBranch(DEFAULT_BRANCH);

      expect(result.deleted).toBe(false);
      expect(result.reason).toContain('Cannot delete the default branch');
    });

    it('should not delete protected branch without force', async () => {
      await store.createBranch({
        name: 'protected-branch',
        baseCommit: 'abc123',
        protected: true,
      });

      const result = await store.deleteBranch('protected-branch');

      expect(result.deleted).toBe(false);
      expect(result.reason).toContain('protected');
    });

    it('should delete protected branch with force', async () => {
      await store.createBranch({
        name: 'protected-branch',
        baseCommit: 'abc123',
        protected: true,
      });

      const result = await store.deleteBranch('protected-branch', true);

      expect(result.deleted).toBe(true);
    });

    it('should return appropriate result for non-existent branch', async () => {
      const result = await store.deleteBranch('non-existent');

      expect(result.deleted).toBe(false);
      expect(result.reason).toContain('not found');
    });
  });

  // ==========================================================================
  // branchExists Tests
  // ==========================================================================

  describe('branchExists', () => {
    it('should return true for existing branch', async () => {
      await store.createBranch({
        name: 'feature',
        baseCommit: 'abc123',
      });

      const exists = await store.branchExists('feature');

      expect(exists).toBe(true);
    });

    it('should return false for non-existent branch', async () => {
      const exists = await store.branchExists('non-existent');

      expect(exists).toBe(false);
    });
  });

  // ==========================================================================
  // renameBranch Tests
  // ==========================================================================

  describe('renameBranch', () => {
    it('should rename a branch', async () => {
      await store.createBranch({
        name: 'old-name',
        baseCommit: 'abc123',
        description: 'Test branch',
      });

      const renamed = await store.renameBranch('old-name', 'new-name');

      expect(renamed.name).toBe('new-name');
      expect(renamed.description).toBe('Test branch');
      expect(renamed.baseCommit).toBe('abc123');

      // Old branch should not exist
      const oldBranch = await store.getBranch('old-name');
      expect(oldBranch).toBeNull();

      // New branch should exist
      const newBranch = await store.getBranch('new-name');
      expect(newBranch).not.toBeNull();
    });

    it('should not rename the default branch', async () => {
      await store.initializeDefaultBranch('abc123');

      await expect(
        store.renameBranch(DEFAULT_BRANCH, 'other')
      ).rejects.toThrow('Cannot rename the default branch');
    });

    it('should throw error for invalid new name', async () => {
      await store.createBranch({
        name: 'feature',
        baseCommit: 'abc123',
      });

      await expect(
        store.renameBranch('feature', 'HEAD')
      ).rejects.toThrow('Invalid branch name');
    });

    it('should throw error if new name already exists', async () => {
      await store.createBranch({
        name: 'branch-a',
        baseCommit: 'abc123',
      });

      await store.createBranch({
        name: 'branch-b',
        baseCommit: 'abc123',
      });

      await expect(
        store.renameBranch('branch-a', 'branch-b')
      ).rejects.toThrow('already exists');
    });

    it('should throw error if old branch does not exist', async () => {
      await expect(
        store.renameBranch('non-existent', 'new-name')
      ).rejects.toThrow('not found');
    });
  });

  // ==========================================================================
  // initializeDefaultBranch Tests
  // ==========================================================================

  describe('initializeDefaultBranch', () => {
    it('should create default branch if not exists', async () => {
      const branch = await store.initializeDefaultBranch('initial-commit');

      expect(branch.name).toBe(DEFAULT_BRANCH);
      expect(branch.baseCommit).toBe('initial-commit');
      expect(branch.protected).toBe(true);
    });

    it('should return existing default branch without modification', async () => {
      await store.createBranch({
        name: DEFAULT_BRANCH,
        baseCommit: 'abc123',
        description: 'Original',
      });

      const branch = await store.initializeDefaultBranch('different-commit');

      expect(branch.baseCommit).toBe('abc123');
      expect(branch.description).toBe('Original');
    });
  });

  // ==========================================================================
  // getStats Tests
  // ==========================================================================

  describe('getStats', () => {
    it('should return correct stats', async () => {
      await store.createBranch({
        name: 'main',
        baseCommit: 'abc123',
        protected: true,
      });

      await new Promise((r) => setTimeout(r, 10));

      await store.createBranch({
        name: 'develop',
        baseCommit: 'def456',
      });

      await new Promise((r) => setTimeout(r, 10));

      await store.createBranch({
        name: 'feature',
        baseCommit: 'ghi789',
        protected: true,
      });

      const stats = await store.getStats();

      expect(stats.totalBranches).toBe(3);
      expect(stats.protectedBranches).toBe(2);
      expect(stats.oldestBranch).toBe('main');
      expect(stats.newestBranch).toBe('feature');
    });

    it('should handle empty store', async () => {
      const emptyStore = new BranchStore(new MemoryStorage(), 'emptydb');
      const stats = await emptyStore.getStats();

      expect(stats.totalBranches).toBe(0);
      expect(stats.protectedBranches).toBe(0);
      expect(stats.oldestBranch).toBeNull();
      expect(stats.newestBranch).toBeNull();
    });
  });
});

// ============================================================================
// Convenience Function Tests
// ============================================================================

describe('Convenience Functions', () => {
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
  });

  describe('createBranchStore', () => {
    it('should create a BranchStore instance', () => {
      const store = createBranchStore(storage, 'testdb');

      expect(store).toBeInstanceOf(BranchStore);
    });
  });

  describe('createBranch', () => {
    it('should create a branch', async () => {
      const branch = await createBranch(storage, 'testdb', {
        name: 'feature',
        baseCommit: 'abc123',
      });

      expect(branch.name).toBe('feature');
    });
  });

  describe('getBranch', () => {
    it('should get a branch', async () => {
      await createBranch(storage, 'testdb', {
        name: 'feature',
        baseCommit: 'abc123',
      });

      const branch = await getBranch(storage, 'testdb', 'feature');

      expect(branch).not.toBeNull();
      expect(branch!.name).toBe('feature');
    });

    it('should return null for non-existent branch', async () => {
      const branch = await getBranch(storage, 'testdb', 'non-existent');

      expect(branch).toBeNull();
    });
  });

  describe('listBranches', () => {
    it('should list branches', async () => {
      await createBranch(storage, 'testdb', {
        name: 'branch-a',
        baseCommit: 'abc123',
      });

      await createBranch(storage, 'testdb', {
        name: 'branch-b',
        baseCommit: 'def456',
      });

      const branches = await listBranches(storage, 'testdb');

      expect(branches).toHaveLength(2);
    });
  });

  describe('deleteBranch', () => {
    it('should delete a branch', async () => {
      await createBranch(storage, 'testdb', {
        name: 'feature',
        baseCommit: 'abc123',
      });

      const result = await deleteBranch(storage, 'testdb', 'feature');

      expect(result.deleted).toBe(true);
    });
  });
});

// ============================================================================
// Constants Tests
// ============================================================================

describe('Constants', () => {
  it('should export DEFAULT_BRANCH', () => {
    expect(DEFAULT_BRANCH).toBe('main');
  });

  it('should export BRANCHES_DIR', () => {
    expect(BRANCHES_DIR).toBe('branches');
  });

  it('should export BRANCH_FILE_EXTENSION', () => {
    expect(BRANCH_FILE_EXTENSION).toBe('.json');
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('BranchStore Integration', () => {
  let storage: MemoryStorage;
  let store: BranchStore;

  beforeEach(() => {
    storage = new MemoryStorage();
    store = new BranchStore(storage, 'mydb');
  });

  it('should support full branch lifecycle', async () => {
    // 1. Initialize default branch
    const mainBranch = await store.initializeDefaultBranch('commit-1');
    expect(mainBranch.name).toBe('main');
    expect(mainBranch.protected).toBe(true);

    // 2. Create feature branch
    const featureBranch = await store.createBranch({
      name: 'feature/new-feature',
      description: 'Working on new feature',
      createdBy: 'developer@example.com',
    });
    expect(featureBranch.baseCommit).toBe('commit-1');

    // 3. List branches
    const branches = await store.listBranches();
    expect(branches).toHaveLength(2);

    // 4. Update feature branch head
    const updated = await store.updateBranch('feature/new-feature', {
      headCommit: 'commit-2',
    });
    expect(updated.headCommit).toBe('commit-2');

    // 5. Create another feature branch
    await store.createBranch({
      name: 'feature/another-feature',
      baseCommit: 'commit-1',
    });

    // 6. List feature branches only
    const featureBranches = await store.listBranches({ prefix: 'feature/' });
    expect(featureBranches).toHaveLength(2);

    // 7. Rename branch
    const renamed = await store.renameBranch('feature/another-feature', 'feature/renamed');
    expect(renamed.name).toBe('feature/renamed');

    // 8. Delete branch
    const deleteResult = await store.deleteBranch('feature/renamed');
    expect(deleteResult.deleted).toBe(true);

    // 9. Verify deletion
    const remainingBranches = await store.listBranches();
    expect(remainingBranches).toHaveLength(2);
    expect(remainingBranches.map((b) => b.name)).toContain('main');
    expect(remainingBranches.map((b) => b.name)).toContain('feature/new-feature');

    // 10. Get stats
    const stats = await store.getStats();
    expect(stats.totalBranches).toBe(2);
    expect(stats.protectedBranches).toBe(1);
  });

  it('should handle concurrent operations safely', async () => {
    await store.initializeDefaultBranch('commit-1');

    // Create multiple branches concurrently
    const createPromises = [
      store.createBranch({ name: 'branch-1', baseCommit: 'commit-1' }),
      store.createBranch({ name: 'branch-2', baseCommit: 'commit-1' }),
      store.createBranch({ name: 'branch-3', baseCommit: 'commit-1' }),
    ];

    const branches = await Promise.all(createPromises);

    expect(branches).toHaveLength(3);
    expect(branches.map((b) => b.name).sort()).toEqual(['branch-1', 'branch-2', 'branch-3']);
  });

  it('should persist branches across store instances', async () => {
    // Create branch with first store instance
    await store.createBranch({
      name: 'persistent-branch',
      baseCommit: 'abc123',
      description: 'Should persist',
    });

    // Create new store instance with same storage
    const newStore = new BranchStore(storage, 'mydb');

    // Branch should be accessible
    const branch = await newStore.getBranch('persistent-branch');
    expect(branch).not.toBeNull();
    expect(branch!.description).toBe('Should persist');
  });
});
