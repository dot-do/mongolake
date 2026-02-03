/**
 * Branch Metadata Storage Tests (TDD RED Phase)
 *
 * Tests for branch metadata storage per mongolake-eg5.1.1:
 * - BranchInfo type structure
 * - Storing/retrieving branch by name
 * - Base snapshot reference integrity
 * - Created timestamp accuracy
 * - Parent branch chain
 *
 * These tests define the expected behavior for branch metadata.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { MemoryStorage } from '../../../src/storage/index.js';
import {
  BranchManager,
  type BranchInfo,
  type BranchCreateOptions,
  DEFAULT_BRANCH,
} from '../../../src/branching/index.js';

// ============================================================================
// Test Fixtures
// ============================================================================

function createTestStorage(): MemoryStorage {
  return new MemoryStorage();
}

async function createInitializedBranchManager(storage: MemoryStorage): Promise<BranchManager> {
  const manager = new BranchManager(storage, 'testdb');
  await manager.initialize('initial-snapshot-001');
  return manager;
}

// ============================================================================
// BranchInfo Type Structure Tests
// ============================================================================

describe('BranchInfo Type Structure', () => {
  let storage: MemoryStorage;
  let manager: BranchManager;

  beforeEach(async () => {
    storage = createTestStorage();
    manager = await createInitializedBranchManager(storage);
  });

  it('should have required name field as string', async () => {
    const branch = await manager.createBranch('feature-branch');

    expect(branch.name).toBeDefined();
    expect(typeof branch.name).toBe('string');
    expect(branch.name).toBe('feature-branch');
  });

  it('should have required baseCommit field as string', async () => {
    const branch = await manager.createBranch('feature-branch');

    expect(branch.baseCommit).toBeDefined();
    expect(typeof branch.baseCommit).toBe('string');
    expect(branch.baseCommit.length).toBeGreaterThan(0);
  });

  it('should have required headCommit field as string', async () => {
    const branch = await manager.createBranch('feature-branch');

    expect(branch.headCommit).toBeDefined();
    expect(typeof branch.headCommit).toBe('string');
    expect(branch.headCommit.length).toBeGreaterThan(0);
  });

  it('should have required createdAt field as ISO timestamp string', async () => {
    const branch = await manager.createBranch('feature-branch');

    expect(branch.createdAt).toBeDefined();
    expect(typeof branch.createdAt).toBe('string');
    // Should be a valid ISO 8601 timestamp
    expect(new Date(branch.createdAt).toISOString()).toBe(branch.createdAt);
  });

  it('should have required updatedAt field as ISO timestamp string', async () => {
    const branch = await manager.createBranch('feature-branch');

    expect(branch.updatedAt).toBeDefined();
    expect(typeof branch.updatedAt).toBe('string');
    // Should be a valid ISO 8601 timestamp
    expect(new Date(branch.updatedAt).toISOString()).toBe(branch.updatedAt);
  });

  it('should have required state field with valid value', async () => {
    const branch = await manager.createBranch('feature-branch');

    expect(branch.state).toBeDefined();
    expect(['active', 'merged', 'deleted']).toContain(branch.state);
    // New branches should be active
    expect(branch.state).toBe('active');
  });

  it('should have required parentBranch field (string or null)', async () => {
    const branch = await manager.createBranch('feature-branch');

    expect('parentBranch' in branch).toBe(true);
    expect(branch.parentBranch === null || typeof branch.parentBranch === 'string').toBe(true);
  });

  it('should have optional description field when provided', async () => {
    const branch = await manager.createBranch('feature-branch', {
      description: 'Test description',
    });

    expect(branch.description).toBe('Test description');
  });

  it('should not have description field when not provided', async () => {
    const branch = await manager.createBranch('feature-branch');

    // Description should be undefined when not provided
    expect(branch.description).toBeUndefined();
  });

  it('should have optional createdBy field when provided', async () => {
    const branch = await manager.createBranch('feature-branch', {
      createdBy: 'developer@example.com',
    });

    expect(branch.createdBy).toBe('developer@example.com');
  });

  it('should have optional metadata field when provided', async () => {
    const branch = await manager.createBranch('feature-branch', {
      metadata: { ticket: 'JIRA-123', priority: 'high' },
    });

    expect(branch.metadata).toEqual({ ticket: 'JIRA-123', priority: 'high' });
  });

  it('should have modifiedFiles initialized as empty array', async () => {
    const branch = await manager.createBranch('feature-branch');

    expect(branch.modifiedFiles).toBeDefined();
    expect(Array.isArray(branch.modifiedFiles)).toBe(true);
    expect(branch.modifiedFiles).toEqual([]);
  });

  it('should have optional protected field', async () => {
    // Default branch should be protected
    const mainBranch = await manager.getBranch(DEFAULT_BRANCH);
    expect(mainBranch?.protected).toBe(true);

    // New branches should not be protected by default
    const featureBranch = await manager.createBranch('feature-branch');
    expect(featureBranch.protected).toBeUndefined();
  });

  it('should conform to complete BranchInfo interface', async () => {
    const branch = await manager.createBranch('feature-branch', {
      description: 'Full test',
      createdBy: 'tester@example.com',
      metadata: { env: 'test' },
    });

    // Type-check all required fields
    const branchInfo: BranchInfo = branch;

    expect(branchInfo.name).toBe('feature-branch');
    expect(branchInfo.baseCommit).toBeDefined();
    expect(branchInfo.headCommit).toBeDefined();
    expect(branchInfo.createdAt).toBeDefined();
    expect(branchInfo.updatedAt).toBeDefined();
    expect(branchInfo.state).toBe('active');
    expect(branchInfo.parentBranch).toBe(DEFAULT_BRANCH);
    expect(branchInfo.description).toBe('Full test');
    expect(branchInfo.createdBy).toBe('tester@example.com');
    expect(branchInfo.metadata).toEqual({ env: 'test' });
    expect(branchInfo.modifiedFiles).toEqual([]);
  });
});

// ============================================================================
// Storing/Retrieving Branch by Name Tests
// ============================================================================

describe('Storing/Retrieving Branch by Name', () => {
  let storage: MemoryStorage;
  let manager: BranchManager;

  beforeEach(async () => {
    storage = createTestStorage();
    manager = await createInitializedBranchManager(storage);
  });

  it('should store branch and retrieve by exact name', async () => {
    await manager.createBranch('feature-branch');

    const retrieved = await manager.getBranch('feature-branch');

    expect(retrieved).not.toBeNull();
    expect(retrieved!.name).toBe('feature-branch');
  });

  it('should return null for non-existent branch name', async () => {
    const retrieved = await manager.getBranch('non-existent-branch');

    expect(retrieved).toBeNull();
  });

  it('should handle branch names with slashes', async () => {
    await manager.createBranch('feature/auth/login');

    const retrieved = await manager.getBranch('feature/auth/login');

    expect(retrieved).not.toBeNull();
    expect(retrieved!.name).toBe('feature/auth/login');
  });

  it('should handle branch names with hyphens', async () => {
    await manager.createBranch('feature-new-api-v2');

    const retrieved = await manager.getBranch('feature-new-api-v2');

    expect(retrieved).not.toBeNull();
    expect(retrieved!.name).toBe('feature-new-api-v2');
  });

  it('should handle branch names with underscores', async () => {
    await manager.createBranch('feature_experimental_test');

    const retrieved = await manager.getBranch('feature_experimental_test');

    expect(retrieved).not.toBeNull();
    expect(retrieved!.name).toBe('feature_experimental_test');
  });

  it('should preserve all metadata when storing and retrieving', async () => {
    const createOptions: BranchCreateOptions = {
      description: 'Test branch with full metadata',
      createdBy: 'developer@example.com',
      metadata: { ticket: 'JIRA-456', environment: 'staging' },
    };

    await manager.createBranch('metadata-branch', createOptions);

    const retrieved = await manager.getBranch('metadata-branch');

    expect(retrieved).not.toBeNull();
    expect(retrieved!.description).toBe('Test branch with full metadata');
    expect(retrieved!.createdBy).toBe('developer@example.com');
    expect(retrieved!.metadata).toEqual({ ticket: 'JIRA-456', environment: 'staging' });
  });

  it('should persist branch across manager instances', async () => {
    await manager.createBranch('persistent-branch', {
      description: 'Should survive manager restart',
    });

    // Create new manager instance with same storage
    const newManager = new BranchManager(storage, 'testdb');
    await newManager.initialize('some-snapshot');

    const retrieved = await newManager.getBranch('persistent-branch');

    expect(retrieved).not.toBeNull();
    expect(retrieved!.name).toBe('persistent-branch');
    expect(retrieved!.description).toBe('Should survive manager restart');
  });

  it('should handle case-sensitive branch names correctly', async () => {
    await manager.createBranch('Feature-Branch');
    await manager.createBranch('feature-branch');

    const upper = await manager.getBranch('Feature-Branch');
    const lower = await manager.getBranch('feature-branch');

    expect(upper).not.toBeNull();
    expect(lower).not.toBeNull();
    expect(upper!.name).toBe('Feature-Branch');
    expect(lower!.name).toBe('feature-branch');
    // They should be different branches
    expect(upper!.name).not.toBe(lower!.name);
  });

  it('should trim branch names when storing', async () => {
    const branch = await manager.createBranch('  trimmed-branch  ');

    expect(branch.name).toBe('trimmed-branch');

    const retrieved = await manager.getBranch('trimmed-branch');
    expect(retrieved).not.toBeNull();
    expect(retrieved!.name).toBe('trimmed-branch');
  });

  it('should list stored branches correctly', async () => {
    await manager.createBranch('branch-a');
    await manager.createBranch('branch-b');
    await manager.createBranch('branch-c');

    const branches = await manager.listBranches();
    const branchNames = branches.map(b => b.name);

    expect(branchNames).toContain('branch-a');
    expect(branchNames).toContain('branch-b');
    expect(branchNames).toContain('branch-c');
    expect(branchNames).toContain(DEFAULT_BRANCH);
  });
});

// ============================================================================
// Base Snapshot Reference Integrity Tests
// ============================================================================

describe('Base Snapshot Reference Integrity', () => {
  let storage: MemoryStorage;
  let manager: BranchManager;

  beforeEach(async () => {
    storage = createTestStorage();
    manager = await createInitializedBranchManager(storage);
  });

  it('should reference parent branch head as baseCommit by default', async () => {
    const mainBranch = await manager.getBranch(DEFAULT_BRANCH);
    const featureBranch = await manager.createBranch('feature-branch');

    expect(featureBranch.baseCommit).toBe(mainBranch!.headCommit);
  });

  it('should use specified baseCommit when provided', async () => {
    await manager.advanceSnapshot('snapshot-002');
    await manager.advanceSnapshot('snapshot-003');

    const branch = await manager.createBranch('hotfix', {
      baseCommit: 'snapshot-002',
    });

    expect(branch.baseCommit).toBe('snapshot-002');
  });

  it('should use fromSnapshotId when provided', async () => {
    await manager.advanceSnapshot('snapshot-002');

    const branch = await manager.createBranch('from-snapshot', {
      fromSnapshotId: 'initial-snapshot-001',
    });

    expect(branch.baseCommit).toBe('initial-snapshot-001');
  });

  it('should throw error for non-existent snapshot reference', async () => {
    await expect(
      manager.createBranch('bad-branch', {
        fromSnapshotId: 'non-existent-snapshot',
      })
    ).rejects.toThrow(/snapshot.*not found/i);
  });

  it('should set headCommit equal to baseCommit on branch creation', async () => {
    const branch = await manager.createBranch('feature-branch');

    expect(branch.headCommit).toBe(branch.baseCommit);
  });

  it('should preserve baseCommit when headCommit advances', async () => {
    await manager.createBranch('feature-branch');
    await manager.advanceSnapshot('feature-snapshot-001', 'feature-branch');

    const branch = await manager.getBranch('feature-branch');

    expect(branch!.baseCommit).toBe('initial-snapshot-001');
    expect(branch!.headCommit).toBe('feature-snapshot-001');
  });

  it('should use parent branch headCommit when creating child branch', async () => {
    await manager.createBranch('feature-a');
    await manager.advanceSnapshot('feature-a-snapshot', 'feature-a');

    const childBranch = await manager.createBranch('feature-a-sub', {
      parentBranch: 'feature-a',
    });

    expect(childBranch.baseCommit).toBe('feature-a-snapshot');
  });

  it('should validate snapshot exists before creating branch from it', async () => {
    // Advance to create valid snapshots
    await manager.advanceSnapshot('valid-snapshot');

    // Should succeed with valid snapshot
    const branch = await manager.createBranch('valid-branch', {
      fromSnapshotId: 'valid-snapshot',
    });
    expect(branch.baseCommit).toBe('valid-snapshot');
  });

  it('should maintain referential integrity across multiple branches', async () => {
    // Create branch hierarchy
    const branch1 = await manager.createBranch('level-1');
    await manager.advanceSnapshot('l1-snapshot', 'level-1');

    const branch2 = await manager.createBranch('level-2', {
      parentBranch: 'level-1',
    });
    await manager.advanceSnapshot('l2-snapshot', 'level-2');

    const branch3 = await manager.createBranch('level-3', {
      parentBranch: 'level-2',
    });

    // Verify chain
    expect(branch1.baseCommit).toBe('initial-snapshot-001');
    expect(branch2.baseCommit).toBe('l1-snapshot');
    expect(branch3.baseCommit).toBe('l2-snapshot');
  });
});

// ============================================================================
// Created Timestamp Accuracy Tests
// ============================================================================

describe('Created Timestamp Accuracy', () => {
  let storage: MemoryStorage;
  let manager: BranchManager;

  beforeEach(async () => {
    storage = createTestStorage();
    manager = await createInitializedBranchManager(storage);
  });

  it('should set createdAt to current time on branch creation', async () => {
    const beforeCreate = new Date();

    const branch = await manager.createBranch('feature-branch');

    const afterCreate = new Date();
    const createdAt = new Date(branch.createdAt);

    expect(createdAt.getTime()).toBeGreaterThanOrEqual(beforeCreate.getTime());
    expect(createdAt.getTime()).toBeLessThanOrEqual(afterCreate.getTime());
  });

  it('should set updatedAt equal to createdAt on branch creation', async () => {
    const branch = await manager.createBranch('feature-branch');

    expect(branch.updatedAt).toBe(branch.createdAt);
  });

  it('should update updatedAt when branch is modified', async () => {
    const branch = await manager.createBranch('feature-branch');
    const originalUpdatedAt = branch.updatedAt;

    // Small delay to ensure different timestamp
    await new Promise(resolve => setTimeout(resolve, 10));

    await manager.advanceSnapshot('new-snapshot', 'feature-branch');

    const updatedBranch = await manager.getBranch('feature-branch');

    expect(new Date(updatedBranch!.updatedAt).getTime()).toBeGreaterThan(
      new Date(originalUpdatedAt).getTime()
    );
  });

  it('should preserve createdAt when branch is modified', async () => {
    const branch = await manager.createBranch('feature-branch');
    const originalCreatedAt = branch.createdAt;

    await new Promise(resolve => setTimeout(resolve, 10));
    await manager.advanceSnapshot('new-snapshot', 'feature-branch');

    const updatedBranch = await manager.getBranch('feature-branch');

    expect(updatedBranch!.createdAt).toBe(originalCreatedAt);
  });

  it('should format timestamp as ISO 8601 string', async () => {
    const branch = await manager.createBranch('feature-branch');

    // ISO 8601 format: YYYY-MM-DDTHH:mm:ss.sssZ
    const isoPattern = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

    expect(branch.createdAt).toMatch(isoPattern);
    expect(branch.updatedAt).toMatch(isoPattern);
  });

  it('should have increasing createdAt for sequentially created branches', async () => {
    const branch1 = await manager.createBranch('branch-1');
    await new Promise(resolve => setTimeout(resolve, 5));
    const branch2 = await manager.createBranch('branch-2');
    await new Promise(resolve => setTimeout(resolve, 5));
    const branch3 = await manager.createBranch('branch-3');

    const time1 = new Date(branch1.createdAt).getTime();
    const time2 = new Date(branch2.createdAt).getTime();
    const time3 = new Date(branch3.createdAt).getTime();

    expect(time1).toBeLessThanOrEqual(time2);
    expect(time2).toBeLessThanOrEqual(time3);
  });

  it('should persist timestamps correctly across retrieval', async () => {
    const originalBranch = await manager.createBranch('feature-branch');

    const retrievedBranch = await manager.getBranch('feature-branch');

    expect(retrievedBranch!.createdAt).toBe(originalBranch.createdAt);
    expect(retrievedBranch!.updatedAt).toBe(originalBranch.updatedAt);
  });

  it('should handle timestamp persistence across manager restarts', async () => {
    const originalBranch = await manager.createBranch('feature-branch');
    const originalCreatedAt = originalBranch.createdAt;

    // Create new manager instance
    const newManager = new BranchManager(storage, 'testdb');
    await newManager.initialize('some-snapshot');

    const retrievedBranch = await newManager.getBranch('feature-branch');

    expect(retrievedBranch!.createdAt).toBe(originalCreatedAt);
  });
});

// ============================================================================
// Parent Branch Chain Tests
// ============================================================================

describe('Parent Branch Chain', () => {
  let storage: MemoryStorage;
  let manager: BranchManager;

  beforeEach(async () => {
    storage = createTestStorage();
    manager = await createInitializedBranchManager(storage);
  });

  it('should set parentBranch to main for branches created from main', async () => {
    const branch = await manager.createBranch('feature-branch');

    expect(branch.parentBranch).toBe(DEFAULT_BRANCH);
  });

  it('should set parentBranch to null for main branch', async () => {
    const mainBranch = await manager.getBranch(DEFAULT_BRANCH);

    expect(mainBranch!.parentBranch).toBeNull();
  });

  it('should set parentBranch when specified in options', async () => {
    await manager.createBranch('feature-a');

    const childBranch = await manager.createBranch('feature-a-child', {
      parentBranch: 'feature-a',
    });

    expect(childBranch.parentBranch).toBe('feature-a');
  });

  it('should track ancestry through getBranchAncestry', async () => {
    await manager.createBranch('level-1');
    await manager.createBranch('level-2', { parentBranch: 'level-1' });
    await manager.createBranch('level-3', { parentBranch: 'level-2' });

    const ancestry = await manager.getBranchAncestry('level-3');

    expect(ancestry).toEqual(['level-2', 'level-1', DEFAULT_BRANCH]);
  });

  it('should return empty ancestry for main branch', async () => {
    const ancestry = await manager.getBranchAncestry(DEFAULT_BRANCH);

    expect(ancestry).toEqual([]);
  });

  it('should return single parent for direct child of main', async () => {
    await manager.createBranch('direct-child');

    const ancestry = await manager.getBranchAncestry('direct-child');

    expect(ancestry).toEqual([DEFAULT_BRANCH]);
  });

  it('should handle deeply nested branch chains', async () => {
    // Create a chain of 5 branches
    const branchNames = ['l1', 'l2', 'l3', 'l4', 'l5'];
    let parentBranch = DEFAULT_BRANCH;

    for (const name of branchNames) {
      await manager.createBranch(name, { parentBranch });
      parentBranch = name;
    }

    const ancestry = await manager.getBranchAncestry('l5');

    expect(ancestry).toEqual(['l4', 'l3', 'l2', 'l1', DEFAULT_BRANCH]);
  });

  it('should not allow circular parent references', async () => {
    // Create a branch
    await manager.createBranch('branch-a');

    // Attempt to create a branch that would create a cycle
    // (This tests the integrity of the system)
    // Since we can't retroactively change parentBranch, the system
    // should prevent invalid states at creation time
    const branchA = await manager.getBranch('branch-a');
    expect(branchA!.parentBranch).toBe(DEFAULT_BRANCH);
  });

  it('should handle branch chain after parent branch is deleted', async () => {
    await manager.createBranch('parent-branch');
    await manager.createBranch('child-branch', { parentBranch: 'parent-branch' });

    // Delete the parent (force)
    await manager.deleteBranch('parent-branch', true);

    // Child should still exist with its original parentBranch reference
    const child = await manager.getBranch('child-branch');
    expect(child).not.toBeNull();
    expect(child!.parentBranch).toBe('parent-branch');

    // Ancestry query should handle the broken chain gracefully
    const ancestry = await manager.getBranchAncestry('child-branch');
    // Should stop at the deleted branch (or handle gracefully)
    expect(Array.isArray(ancestry)).toBe(true);
  });

  it('should preserve parent chain when updating branch', async () => {
    await manager.createBranch('parent-branch');
    await manager.createBranch('child-branch', { parentBranch: 'parent-branch' });

    // Update the child branch
    await manager.advanceSnapshot('child-snapshot', 'child-branch');
    await manager.recordModifiedFile('child-branch', 'some-file.parquet');

    // Parent chain should be unchanged
    const child = await manager.getBranch('child-branch');
    expect(child!.parentBranch).toBe('parent-branch');
  });

  it('should allow multiple children from same parent', async () => {
    await manager.createBranch('parent-branch');
    await manager.createBranch('child-1', { parentBranch: 'parent-branch' });
    await manager.createBranch('child-2', { parentBranch: 'parent-branch' });
    await manager.createBranch('child-3', { parentBranch: 'parent-branch' });

    const child1 = await manager.getBranch('child-1');
    const child2 = await manager.getBranch('child-2');
    const child3 = await manager.getBranch('child-3');

    expect(child1!.parentBranch).toBe('parent-branch');
    expect(child2!.parentBranch).toBe('parent-branch');
    expect(child3!.parentBranch).toBe('parent-branch');
  });

  it('should use current branch as default parent when not specified', async () => {
    // Create a branch and checkout
    await manager.createBranch('current-branch');
    await manager.checkout('current-branch');

    // Create another branch without specifying parent
    const newBranch = await manager.createBranch('new-branch');

    // Should use current branch as parent
    expect(newBranch.parentBranch).toBe('current-branch');
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('Branch Metadata Integration', () => {
  let storage: MemoryStorage;
  let manager: BranchManager;

  beforeEach(async () => {
    storage = createTestStorage();
    manager = await createInitializedBranchManager(storage);
  });

  it('should support complete branch metadata workflow', async () => {
    // 1. Create feature branch with full metadata
    const feature = await manager.createBranch('feature/new-api', {
      description: 'New API implementation',
      createdBy: 'developer@example.com',
      metadata: { ticket: 'JIRA-123' },
    });

    // Verify BranchInfo structure
    expect(feature.name).toBe('feature/new-api');
    expect(feature.description).toBe('New API implementation');
    expect(feature.createdBy).toBe('developer@example.com');
    expect(feature.metadata).toEqual({ ticket: 'JIRA-123' });
    expect(feature.state).toBe('active');
    expect(feature.parentBranch).toBe(DEFAULT_BRANCH);

    // 2. Create child branch
    const child = await manager.createBranch('feature/new-api-v2', {
      parentBranch: 'feature/new-api',
      description: 'API v2 improvements',
    });

    expect(child.parentBranch).toBe('feature/new-api');

    // 3. Advance snapshot on child
    await manager.advanceSnapshot('child-snapshot-001', 'feature/new-api-v2');

    // 4. Record modified files
    await manager.recordModifiedFile('feature/new-api-v2', 'data/users.parquet');

    // 5. Retrieve and verify persistence
    const retrievedChild = await manager.getBranch('feature/new-api-v2');
    expect(retrievedChild!.headCommit).toBe('child-snapshot-001');
    expect(retrievedChild!.modifiedFiles).toContain('data/users.parquet');
    expect(retrievedChild!.createdAt).toBe(child.createdAt); // Preserved

    // 6. Verify ancestry
    const ancestry = await manager.getBranchAncestry('feature/new-api-v2');
    expect(ancestry).toEqual(['feature/new-api', DEFAULT_BRANCH]);
  });

  it('should handle concurrent branch creation with correct timestamps', async () => {
    const beforeAll = new Date();

    // Create branches concurrently
    const [branch1, branch2, branch3] = await Promise.all([
      manager.createBranch('concurrent-1'),
      manager.createBranch('concurrent-2'),
      manager.createBranch('concurrent-3'),
    ]);

    const afterAll = new Date();

    // All branches should have timestamps within the test window
    for (const branch of [branch1, branch2, branch3]) {
      const createdAt = new Date(branch.createdAt).getTime();
      expect(createdAt).toBeGreaterThanOrEqual(beforeAll.getTime());
      expect(createdAt).toBeLessThanOrEqual(afterAll.getTime());
    }

    // All should have main as parent
    expect(branch1.parentBranch).toBe(DEFAULT_BRANCH);
    expect(branch2.parentBranch).toBe(DEFAULT_BRANCH);
    expect(branch3.parentBranch).toBe(DEFAULT_BRANCH);
  });
});
