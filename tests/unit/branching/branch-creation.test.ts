/**
 * Branch Creation Tests (TDD RED Phase)
 *
 * Tests for Git-like branch creation in MongoLake.
 * These tests should FAIL initially - they define the expected API.
 *
 * Requirements from mongolake-eg5.2.1:
 * - test db.branch('name') creates branch
 * - test copy-on-write semantics (no data duplication)
 * - test branch from specific snapshot
 * - test branch naming validation
 * - test error on duplicate branch name
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { MemoryStorage } from '../../../src/storage/index.js';
import {
  BranchStore,
  isValidBranchName,
  DEFAULT_BRANCH,
  type BranchMetadata,
} from '../../../src/branching/index.js';
// Import BranchManager - the class under test (does not exist yet - TDD RED phase)
// @ts-expect-error - BranchManager does not exist yet
import { BranchManager, type BranchCreateOptions, type BranchInfo } from '../../../src/branching/manager.js';

// ============================================================================
// Test Fixtures
// ============================================================================

function createTestStorage(): MemoryStorage {
  return new MemoryStorage();
}

async function createInitializedBranchManager(storage: MemoryStorage): Promise<BranchManager> {
  const manager = new BranchManager(storage, 'testdb');
  // Initialize with a main branch and initial snapshot
  await manager.initialize('initial-snapshot-001');
  return manager;
}

// ============================================================================
// Branch Creation Tests - Core API
// ============================================================================

describe('BranchManager - Branch Creation', () => {
  let storage: MemoryStorage;
  let manager: BranchManager;

  beforeEach(async () => {
    storage = createTestStorage();
    manager = await createInitializedBranchManager(storage);
  });

  // ==========================================================================
  // db.branch('name') creates branch
  // ==========================================================================

  describe('db.branch(name) creates branch', () => {
    it('should create a branch with the given name', async () => {
      const result = await manager.createBranch('feature-branch');

      expect(result).toBeDefined();
      expect(result.name).toBe('feature-branch');
      expect(result.state).toBe('active');
    });

    it('should create branch from current HEAD by default', async () => {
      // Write some data to advance the snapshot
      await manager.advanceSnapshot('snapshot-002');

      const result = await manager.createBranch('feature-branch');

      // baseCommit should match the current head of the parent branch
      expect(result.baseCommit).toBe('snapshot-002');
    });

    it('should return BranchInfo with all required fields', async () => {
      const result = await manager.createBranch('feature-branch');

      // All required fields should be present
      expect(result.name).toBe('feature-branch');
      expect(result.baseCommit).toBeDefined();
      expect(result.headCommit).toBeDefined();
      expect(result.createdAt).toBeDefined();
      expect(result.state).toBe('active');
      expect(result.parentBranch).toBe(DEFAULT_BRANCH);
    });

    it('should persist branch to storage', async () => {
      await manager.createBranch('feature-branch');

      // Branch should be retrievable
      const branch = await manager.getBranch('feature-branch');
      expect(branch).not.toBeNull();
      expect(branch!.name).toBe('feature-branch');
    });

    it('should allow creating branch with description', async () => {
      const result = await manager.createBranch('feature-branch', {
        description: 'Working on new feature',
      });

      expect(result.description).toBe('Working on new feature');
    });

    it('should allow creating branch with metadata', async () => {
      const result = await manager.createBranch('feature-branch', {
        metadata: { ticket: 'JIRA-123', priority: 'high' },
      });

      expect(result.metadata).toEqual({ ticket: 'JIRA-123', priority: 'high' });
    });

    it('should track createdBy when provided', async () => {
      const result = await manager.createBranch('feature-branch', {
        createdBy: 'developer@example.com',
      });

      expect(result.createdBy).toBe('developer@example.com');
    });
  });

  // ==========================================================================
  // Copy-on-write semantics (no data duplication)
  // ==========================================================================

  describe('copy-on-write semantics', () => {
    it('should not duplicate data when creating branch', async () => {
      // Get initial storage state
      const filesBefore = await storage.list('');
      const dataFilesBefore = filesBefore.filter(f => f.endsWith('.parquet'));

      // Create branch
      await manager.createBranch('feature-branch');

      // Get storage state after
      const filesAfter = await storage.list('');
      const dataFilesAfter = filesAfter.filter(f => f.endsWith('.parquet'));

      // No new parquet files should be created - only metadata
      expect(dataFilesAfter.length).toBe(dataFilesBefore.length);
    });

    it('should reference parent snapshot instead of copying', async () => {
      const branch = await manager.createBranch('feature-branch');

      // Branch should reference the same snapshot as parent
      const mainBranch = await manager.getBranch(DEFAULT_BRANCH);
      expect(branch.baseCommit).toBe(mainBranch!.headCommit);
    });

    it('should track modified files separately per branch', async () => {
      const branch = await manager.createBranch('feature-branch');

      // Initially no modified files
      expect(branch.modifiedFiles).toEqual([]);
    });

    it('should isolate writes between branches', async () => {
      await manager.createBranch('feature-branch');

      // Simulate write on feature branch
      await manager.recordModifiedFile('feature-branch', 'users/2024-01-01/file1.parquet');

      // Main branch should not be affected
      const mainBranch = await manager.getBranch(DEFAULT_BRANCH);
      expect(mainBranch!.modifiedFiles).not.toContain('users/2024-01-01/file1.parquet');

      // Feature branch should have the modification recorded
      const featureBranch = await manager.getBranch('feature-branch');
      expect(featureBranch!.modifiedFiles).toContain('users/2024-01-01/file1.parquet');
    });

    it('should share unchanged data between branches', async () => {
      // Create branch
      const branch = await manager.createBranch('feature-branch');

      // Both branches should reference the same base files
      const mainBranch = await manager.getBranch(DEFAULT_BRANCH);

      // The shared snapshot should be the same
      expect(branch.baseCommit).toBe(mainBranch!.headCommit);
    });
  });

  // ==========================================================================
  // Branch from specific snapshot
  // ==========================================================================

  describe('branch from specific snapshot', () => {
    it('should create branch from specific snapshot ID', async () => {
      // Advance to create multiple snapshots
      await manager.advanceSnapshot('snapshot-002');
      await manager.advanceSnapshot('snapshot-003');

      // Create branch from an older snapshot (using the initial snapshot)
      const branch = await manager.createBranch('hotfix-branch', {
        fromSnapshotId: 'initial-snapshot-001',
      });

      expect(branch.baseCommit).toBe('initial-snapshot-001');
    });

    it('should fail when snapshot does not exist', async () => {
      await expect(
        manager.createBranch('feature-branch', {
          fromSnapshotId: 'non-existent-snapshot',
        })
      ).rejects.toThrow(/snapshot.*not found/i);
    });

    it('should create branch from specific commit hash', async () => {
      await manager.advanceSnapshot('snapshot-002');

      const branch = await manager.createBranch('feature-branch', {
        baseCommit: 'snapshot-002',
      });

      expect(branch.baseCommit).toBe('snapshot-002');
    });

    it('should allow branching from another branch', async () => {
      // Create first feature branch
      await manager.createBranch('feature-a');
      await manager.advanceSnapshot('snapshot-feature-a', 'feature-a');

      // Create branch from feature-a
      const branch = await manager.createBranch('feature-b', {
        parentBranch: 'feature-a',
      });

      expect(branch.parentBranch).toBe('feature-a');
      expect(branch.baseCommit).toBe('snapshot-feature-a');
    });

    it('should track branch hierarchy', async () => {
      await manager.createBranch('feature-a');
      await manager.createBranch('feature-b', { parentBranch: 'feature-a' });
      await manager.createBranch('feature-c', { parentBranch: 'feature-b' });

      const branchC = await manager.getBranch('feature-c');
      expect(branchC!.parentBranch).toBe('feature-b');

      // Get ancestry
      const ancestry = await manager.getBranchAncestry('feature-c');
      expect(ancestry).toEqual(['feature-b', 'feature-a', DEFAULT_BRANCH]);
    });
  });

  // ==========================================================================
  // Branch naming validation
  // ==========================================================================

  describe('branch naming validation', () => {
    it('should accept valid branch names', async () => {
      // Note: 'main' is already created during initialization
      // Note: Dots are not allowed in branch names per current validation
      const validNames = [
        'feature-branch',
        'feature_branch',
        'feature/my-feature',
        'user/john/feature',
        'v1',
        'release-1-0',
      ];

      for (const name of validNames) {
        // Should not throw
        const branch = await manager.createBranch(name);
        expect(branch.name).toBe(name);
        // Clean up
        await manager.deleteBranch(name, true);
      }
    });

    it('should reject empty branch name', async () => {
      await expect(manager.createBranch('')).rejects.toThrow(/invalid branch name/i);
    });

    it('should reject branch name starting with hyphen', async () => {
      await expect(manager.createBranch('-feature')).rejects.toThrow(/invalid branch name/i);
    });

    it('should reject branch name starting with slash', async () => {
      await expect(manager.createBranch('/feature')).rejects.toThrow(/invalid branch name/i);
    });

    it('should reject branch name ending with slash', async () => {
      await expect(manager.createBranch('feature/')).rejects.toThrow(/invalid branch name/i);
    });

    it('should reject branch name with consecutive slashes', async () => {
      await expect(manager.createBranch('feature//branch')).rejects.toThrow(/invalid branch name/i);
    });

    it('should reject branch name with consecutive dots', async () => {
      await expect(manager.createBranch('feature..branch')).rejects.toThrow(/invalid branch name/i);
    });

    it('should reject reserved branch names', async () => {
      const reservedNames = ['HEAD', 'head', 'refs', 'objects'];

      for (const name of reservedNames) {
        await expect(manager.createBranch(name)).rejects.toThrow(/invalid branch name|reserved/i);
      }
    });

    it('should reject branch names exceeding max length', async () => {
      const longName = 'a'.repeat(256);
      await expect(manager.createBranch(longName)).rejects.toThrow(/invalid branch name/i);
    });

    it('should trim whitespace from branch names', async () => {
      const branch = await manager.createBranch('  feature-branch  ');
      expect(branch.name).toBe('feature-branch');
    });
  });

  // ==========================================================================
  // Error on duplicate branch name
  // ==========================================================================

  describe('error on duplicate branch name', () => {
    it('should throw error when branch already exists', async () => {
      await manager.createBranch('feature-branch');

      await expect(manager.createBranch('feature-branch')).rejects.toThrow(/already exists/i);
    });

    it('should provide descriptive error message for duplicate', async () => {
      await manager.createBranch('feature-branch');

      try {
        await manager.createBranch('feature-branch');
        expect.fail('Should have thrown');
      } catch (error) {
        expect((error as Error).message).toContain('feature-branch');
        expect((error as Error).message).toContain('already exists');
      }
    });

    it('should allow creating branch after deleting one with same name', async () => {
      await manager.createBranch('feature-branch');
      await manager.deleteBranch('feature-branch', true);

      // Should succeed now
      const branch = await manager.createBranch('feature-branch');
      expect(branch.name).toBe('feature-branch');
    });

    it('should be case-sensitive for branch names', async () => {
      await manager.createBranch('Feature-Branch');

      // Different case should be allowed (case-sensitive)
      const branch = await manager.createBranch('feature-branch');
      expect(branch.name).toBe('feature-branch');
    });
  });
});

// ============================================================================
// BranchManager Initialization Tests
// ============================================================================

describe('BranchManager - Initialization', () => {
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = createTestStorage();
  });

  it('should initialize with default branch', async () => {
    const manager = new BranchManager(storage, 'testdb');
    await manager.initialize('initial-snapshot');

    const mainBranch = await manager.getBranch(DEFAULT_BRANCH);
    expect(mainBranch).not.toBeNull();
    expect(mainBranch!.name).toBe(DEFAULT_BRANCH);
    expect(mainBranch!.protected).toBe(true);
  });

  it('should not reinitialize if already initialized', async () => {
    const manager = new BranchManager(storage, 'testdb');
    await manager.initialize('snapshot-1');
    await manager.initialize('snapshot-2'); // Should be idempotent

    const mainBranch = await manager.getBranch(DEFAULT_BRANCH);
    expect(mainBranch!.baseCommit).toBe('snapshot-1'); // First init wins
  });

  it('should throw if used before initialization', async () => {
    const manager = new BranchManager(storage, 'testdb');

    await expect(manager.createBranch('feature')).rejects.toThrow(/not initialized/i);
  });

  it('should expose current branch', async () => {
    const manager = new BranchManager(storage, 'testdb');
    await manager.initialize('initial-snapshot');

    expect(manager.getCurrentBranch()).toBe(DEFAULT_BRANCH);
  });
});

// ============================================================================
// BranchManager - Branch Listing Tests
// ============================================================================

describe('BranchManager - Branch Listing', () => {
  let storage: MemoryStorage;
  let manager: BranchManager;

  beforeEach(async () => {
    storage = createTestStorage();
    manager = await createInitializedBranchManager(storage);
  });

  it('should list all branches', async () => {
    await manager.createBranch('feature-a');
    await manager.createBranch('feature-b');
    await manager.createBranch('hotfix');

    const branches = await manager.listBranches();

    expect(branches.length).toBe(4); // main + 3 created
    expect(branches.map(b => b.name)).toContain(DEFAULT_BRANCH);
    expect(branches.map(b => b.name)).toContain('feature-a');
    expect(branches.map(b => b.name)).toContain('feature-b');
    expect(branches.map(b => b.name)).toContain('hotfix');
  });

  it('should filter branches by prefix', async () => {
    await manager.createBranch('feature/auth');
    await manager.createBranch('feature/payments');
    await manager.createBranch('hotfix/urgent');

    const branches = await manager.listBranches({ prefix: 'feature/' });

    expect(branches.length).toBe(2);
    expect(branches.map(b => b.name)).toContain('feature/auth');
    expect(branches.map(b => b.name)).toContain('feature/payments');
  });

  it('should sort branches by name', async () => {
    await manager.createBranch('zebra');
    await manager.createBranch('alpha');
    await manager.createBranch('beta');

    const branches = await manager.listBranches({ sortBy: 'name', sortOrder: 'asc' });
    const names = branches.map(b => b.name);

    expect(names[0]).toBe('alpha');
    expect(names[names.length - 1]).toBe('zebra');
  });
});

// ============================================================================
// BranchManager - Branch Switching Tests
// ============================================================================

describe('BranchManager - Branch Switching', () => {
  let storage: MemoryStorage;
  let manager: BranchManager;

  beforeEach(async () => {
    storage = createTestStorage();
    manager = await createInitializedBranchManager(storage);
  });

  it('should switch to existing branch', async () => {
    await manager.createBranch('feature-branch');

    await manager.checkout('feature-branch');

    expect(manager.getCurrentBranch()).toBe('feature-branch');
  });

  it('should throw when switching to non-existent branch', async () => {
    await expect(manager.checkout('non-existent')).rejects.toThrow(/not found/i);
  });

  it('should return branch info after checkout', async () => {
    await manager.createBranch('feature-branch');

    const branch = await manager.checkout('feature-branch');

    expect(branch.name).toBe('feature-branch');
    expect(branch.state).toBe('active');
  });

  it('should create and checkout in one operation', async () => {
    const branch = await manager.createBranch('feature-branch', { checkout: true });

    expect(manager.getCurrentBranch()).toBe('feature-branch');
    expect(branch.name).toBe('feature-branch');
  });
});

// ============================================================================
// BranchManager - Snapshot Management Tests
// ============================================================================

describe('BranchManager - Snapshot Management', () => {
  let storage: MemoryStorage;
  let manager: BranchManager;

  beforeEach(async () => {
    storage = createTestStorage();
    manager = await createInitializedBranchManager(storage);
  });

  it('should track snapshots for validation', async () => {
    await manager.advanceSnapshot('snapshot-002');
    await manager.advanceSnapshot('snapshot-003');

    // Both snapshots should be valid
    expect(await manager.snapshotExists('snapshot-002')).toBe(true);
    expect(await manager.snapshotExists('snapshot-003')).toBe(true);
    expect(await manager.snapshotExists('non-existent')).toBe(false);
  });

  it('should update branch head when advancing snapshot', async () => {
    await manager.advanceSnapshot('snapshot-002');

    const mainBranch = await manager.getBranch(DEFAULT_BRANCH);
    expect(mainBranch!.headCommit).toBe('snapshot-002');
  });

  it('should advance snapshot on specific branch', async () => {
    await manager.createBranch('feature-branch');
    await manager.advanceSnapshot('feature-snapshot', 'feature-branch');

    const featureBranch = await manager.getBranch('feature-branch');
    expect(featureBranch!.headCommit).toBe('feature-snapshot');

    // Main branch should not be affected
    const mainBranch = await manager.getBranch(DEFAULT_BRANCH);
    expect(mainBranch!.headCommit).not.toBe('feature-snapshot');
  });

  it('should list snapshots', async () => {
    await manager.advanceSnapshot('snapshot-002');
    await manager.advanceSnapshot('snapshot-003');

    const snapshots = await manager.listSnapshots();

    expect(snapshots.length).toBeGreaterThanOrEqual(3); // initial + 2 advances
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('BranchManager - Integration', () => {
  let storage: MemoryStorage;
  let manager: BranchManager;

  beforeEach(async () => {
    storage = createTestStorage();
    manager = await createInitializedBranchManager(storage);
  });

  it('should support full branching workflow', async () => {
    // 1. Create feature branch
    const featureBranch = await manager.createBranch('feature/new-api', {
      description: 'New API implementation',
      createdBy: 'developer@example.com',
    });
    expect(featureBranch.name).toBe('feature/new-api');

    // 2. Checkout feature branch
    await manager.checkout('feature/new-api');
    expect(manager.getCurrentBranch()).toBe('feature/new-api');

    // 3. Make changes (advance snapshot)
    await manager.advanceSnapshot('feature-snapshot-1', 'feature/new-api');

    // 4. Record modified files
    await manager.recordModifiedFile('feature/new-api', 'data/users/file1.parquet');

    // 5. Verify branch state
    const updatedBranch = await manager.getBranch('feature/new-api');
    expect(updatedBranch!.headCommit).toBe('feature-snapshot-1');
    expect(updatedBranch!.modifiedFiles).toContain('data/users/file1.parquet');

    // 6. Main branch should be unchanged
    const mainBranch = await manager.getBranch(DEFAULT_BRANCH);
    expect(mainBranch!.headCommit).not.toBe('feature-snapshot-1');
    expect(mainBranch!.modifiedFiles).not.toContain('data/users/file1.parquet');

    // 7. Create sub-branch
    const subBranch = await manager.createBranch('feature/new-api-v2', {
      parentBranch: 'feature/new-api',
    });
    expect(subBranch.parentBranch).toBe('feature/new-api');
    expect(subBranch.baseCommit).toBe('feature-snapshot-1');

    // 8. List branches
    const branches = await manager.listBranches();
    expect(branches.length).toBe(3); // main + 2 feature branches
  });

  it('should handle concurrent branch creation', async () => {
    // Create branches concurrently
    const createPromises = [
      manager.createBranch('branch-1'),
      manager.createBranch('branch-2'),
      manager.createBranch('branch-3'),
    ];

    const branches = await Promise.all(createPromises);

    expect(branches.length).toBe(3);
    expect(branches.map(b => b.name).sort()).toEqual(['branch-1', 'branch-2', 'branch-3']);

    // All branches should be persisted
    for (const name of ['branch-1', 'branch-2', 'branch-3']) {
      const branch = await manager.getBranch(name);
      expect(branch).not.toBeNull();
    }
  });

  it('should persist state across manager instances', async () => {
    // Create branch with first instance
    await manager.createBranch('persistent-branch', {
      description: 'Should persist',
    });

    // Create new manager instance with same storage
    const newManager = new BranchManager(storage, 'testdb');
    await newManager.initialize('same-snapshot');

    // Branch should be accessible
    const branch = await newManager.getBranch('persistent-branch');
    expect(branch).not.toBeNull();
    expect(branch!.description).toBe('Should persist');
  });
});
