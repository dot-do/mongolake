/**
 * Diff Generator Tests
 *
 * Tests for the diff generator that compares two branches and generates a list of changes.
 * The diff generator is essential for the branching/merging epic, showing what has changed
 * between a branch and its base.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { MemoryStorage } from '../../storage/index.js';
import { Database, Collection, BranchCollection } from '../../client/index.js';
import { BranchStore, DEFAULT_BRANCH } from '../metadata.js';
import type { Document } from '../../types.js';

// ============================================================================
// Test Setup
// ============================================================================

interface TestDoc extends Document {
  name: string;
  value: number;
  category?: string;
}

describe('Diff Generator', () => {
  let storage: MemoryStorage;
  let db: Database;
  let collection: Collection<TestDoc>;
  let branchStore: BranchStore;

  beforeEach(async () => {
    storage = new MemoryStorage();
    const config = { local: '.test-mongolake' };
    db = new Database('testdb', storage, config);
    collection = db.collection<TestDoc>('users');
    branchStore = db.getBranchStore();

    // Initialize main branch
    await branchStore.initializeDefaultBranch('initial-commit');
  });

  // ==========================================================================
  // Basic Diff Operations
  // ==========================================================================

  describe('Basic diff operations', () => {
    it('should return empty diff for unchanged branch', async () => {
      // Set up base data on main branch
      await collection.insertMany([
        { name: 'Alice', value: 100 },
        { name: 'Bob', value: 200 },
      ]);

      // Create feature branch
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      // Get diff without making any changes
      const diff = await db.diff('feature');

      expect(diff.inserted).toHaveLength(0);
      expect(diff.updated).toHaveLength(0);
      expect(diff.deleted).toHaveLength(0);
      expect(diff.hasChanges).toBe(false);
    });

    it('should detect inserted documents', async () => {
      // Set up base data on main branch
      await collection.insertOne({ name: 'BaseUser', value: 1 });

      // Create feature branch
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      // Insert new document on branch
      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });
      await branchCollection.insertOne({ name: 'NewUser', value: 999 });

      // Get diff
      const diff = await db.diff('feature');

      expect(diff.inserted).toHaveLength(1);
      expect(diff.inserted[0].document.name).toBe('NewUser');
      expect(diff.inserted[0].document.value).toBe(999);
      expect(diff.updated).toHaveLength(0);
      expect(diff.deleted).toHaveLength(0);
      expect(diff.hasChanges).toBe(true);
    });

    it('should detect updated documents with before/after values', async () => {
      // Set up base data on main branch
      await collection.insertOne({ name: 'User', value: 100 });

      // Create feature branch
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      // Update document on branch
      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });
      await branchCollection.updateOne({ name: 'User' }, { $set: { value: 999 } });

      // Get diff
      const diff = await db.diff('feature');

      expect(diff.updated).toHaveLength(1);
      expect(diff.updated[0].before.value).toBe(100);
      expect(diff.updated[0].after.value).toBe(999);
      expect(diff.updated[0].before.name).toBe('User');
      expect(diff.updated[0].after.name).toBe('User');
      expect(diff.inserted).toHaveLength(0);
      expect(diff.deleted).toHaveLength(0);
      expect(diff.hasChanges).toBe(true);
    });

    it('should detect deleted documents', async () => {
      // Set up base data on main branch
      await collection.insertOne({ name: 'ToDelete', value: 100 });

      // Create feature branch
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      // Delete document on branch
      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });
      await branchCollection.deleteOne({ name: 'ToDelete' });

      // Get diff
      const diff = await db.diff('feature');

      expect(diff.deleted).toHaveLength(1);
      expect(diff.deleted[0].document.name).toBe('ToDelete');
      expect(diff.deleted[0].document.value).toBe(100);
      expect(diff.inserted).toHaveLength(0);
      expect(diff.updated).toHaveLength(0);
      expect(diff.hasChanges).toBe(true);
    });
  });

  // ==========================================================================
  // Complex Diff Scenarios
  // ==========================================================================

  describe('Complex diff scenarios', () => {
    it('should handle multiple changes across multiple collections', async () => {
      // Set up base data
      await collection.insertMany([
        { name: 'User1', value: 1 },
        { name: 'User2', value: 2 },
        { name: 'User3', value: 3 },
      ]);

      const orders = db.collection<Document>('orders');
      await orders.insertOne({ orderId: 'ORD-001', amount: 100 });

      // Create feature branch
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      // Make various changes on branch
      const branchUsers = db.collection<TestDoc>('users', { branch: 'feature' });
      const branchOrders = db.collection<Document>('orders', { branch: 'feature' });

      await branchUsers.insertOne({ name: 'NewUser', value: 100 });
      await branchUsers.updateOne({ name: 'User1' }, { $set: { value: 999 } });
      await branchUsers.deleteOne({ name: 'User3' });
      await branchOrders.updateOne({ orderId: 'ORD-001' }, { $set: { amount: 200 } });

      // Get diff
      const diff = await db.diff('feature');

      // Verify users collection changes
      expect(diff.inserted.filter(c => c.collection === 'users')).toHaveLength(1);
      expect(diff.updated.filter(c => c.collection === 'users')).toHaveLength(1);
      expect(diff.deleted.filter(c => c.collection === 'users')).toHaveLength(1);

      // Verify orders collection changes
      expect(diff.updated.filter(c => c.collection === 'orders')).toHaveLength(1);
    });

    it('should track document through multiple operations', async () => {
      // Set up base data
      await collection.insertOne({ name: 'User', value: 1 });

      // Create feature branch
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      // Multiple operations on same document
      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });
      await branchCollection.updateOne({ name: 'User' }, { $set: { value: 10 } });
      await branchCollection.updateOne({ name: 'User' }, { $set: { value: 100 } });
      await branchCollection.updateOne({ name: 'User' }, { $set: { value: 1000 } });

      // Diff should show net change (1 -> 1000)
      const diff = await db.diff('feature');

      expect(diff.updated).toHaveLength(1);
      expect(diff.updated[0].before.value).toBe(1);
      expect(diff.updated[0].after.value).toBe(1000);
    });

    it('should handle insert then delete as no-op', async () => {
      // Set up base data
      await collection.insertOne({ name: 'BaseUser', value: 1 });

      // Create feature branch
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      // Insert and then delete a document on branch
      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });
      const { insertedId } = await branchCollection.insertOne({ name: 'TempUser', value: 99 });
      await branchCollection.deleteOne({ _id: insertedId });

      // Diff should not show the temporary document
      const diff = await db.diff('feature');

      expect(diff.inserted).toHaveLength(0);
      expect(diff.updated).toHaveLength(0);
      expect(diff.deleted).toHaveLength(0);
      expect(diff.hasChanges).toBe(false);
    });

    it('should handle delete then reinsert with same id', async () => {
      // Set up base data
      const result = await collection.insertOne({ name: 'User', value: 100 });

      // Create feature branch
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      // Delete and reinsert with same ID but different data
      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });
      await branchCollection.deleteOne({ _id: result.insertedId });
      await branchCollection.insertOne({ _id: result.insertedId, name: 'Renamed', value: 999 });

      // Diff should show as an update
      const diff = await db.diff('feature');

      expect(diff.updated).toHaveLength(1);
      expect(diff.updated[0].before.name).toBe('User');
      expect(diff.updated[0].after.name).toBe('Renamed');
    });
  });

  // ==========================================================================
  // Diff Metadata
  // ==========================================================================

  describe('Diff metadata', () => {
    it('should include branch information in diff result', async () => {
      await collection.insertOne({ name: 'User', value: 1 });

      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const diff = await db.diff('feature');

      expect(diff.branch).toBe('feature');
      expect(diff.baseBranch).toBe(DEFAULT_BRANCH);
    });

    it('should include summary statistics', async () => {
      await collection.insertMany([
        { name: 'User1', value: 1 },
        { name: 'User2', value: 2 },
        { name: 'User3', value: 3 },
      ]);

      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });
      await branchCollection.insertOne({ name: 'NewUser', value: 100 });
      await branchCollection.updateOne({ name: 'User1' }, { $set: { value: 999 } });
      await branchCollection.deleteOne({ name: 'User3' });

      const diff = await db.diff('feature');

      expect(diff.summary.insertedCount).toBe(1);
      expect(diff.summary.updatedCount).toBe(1);
      expect(diff.summary.deletedCount).toBe(1);
      expect(diff.summary.totalChanges).toBe(3);
    });

    it('should include collection breakdown in summary', async () => {
      await collection.insertOne({ name: 'User', value: 1 });
      const orders = db.collection<Document>('orders');
      await orders.insertOne({ orderId: 'ORD-001', amount: 100 });

      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const branchUsers = db.collection<TestDoc>('users', { branch: 'feature' });
      const branchOrders = db.collection<Document>('orders', { branch: 'feature' });
      await branchUsers.insertOne({ name: 'NewUser', value: 100 });
      await branchOrders.updateOne({ orderId: 'ORD-001' }, { $set: { amount: 200 } });

      const diff = await db.diff('feature');

      expect(diff.summary.byCollection).toHaveProperty('users');
      expect(diff.summary.byCollection).toHaveProperty('orders');
      expect(diff.summary.byCollection['users'].insertedCount).toBe(1);
      expect(diff.summary.byCollection['orders'].updatedCount).toBe(1);
    });
  });

  // ==========================================================================
  // Diff Options
  // ==========================================================================

  describe('Diff options', () => {
    it('should filter diff by collection', async () => {
      await collection.insertOne({ name: 'User', value: 1 });
      const orders = db.collection<Document>('orders');
      await orders.insertOne({ orderId: 'ORD-001', amount: 100 });

      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const branchUsers = db.collection<TestDoc>('users', { branch: 'feature' });
      const branchOrders = db.collection<Document>('orders', { branch: 'feature' });
      await branchUsers.insertOne({ name: 'NewUser', value: 100 });
      await branchOrders.insertOne({ orderId: 'ORD-002', amount: 200 });

      // Filter to only users collection
      const diff = await db.diff('feature', { collections: ['users'] });

      expect(diff.inserted).toHaveLength(1);
      expect(diff.inserted[0].collection).toBe('users');
    });

    it('should support limiting number of changes returned', async () => {
      await collection.insertMany([
        { name: 'User1', value: 1 },
        { name: 'User2', value: 2 },
        { name: 'User3', value: 3 },
        { name: 'User4', value: 4 },
        { name: 'User5', value: 5 },
      ]);

      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });
      await branchCollection.deleteMany({});

      const diff = await db.diff('feature', { limit: 3 });

      expect(diff.deleted).toHaveLength(3);
      expect(diff.summary.deletedCount).toBe(5); // Summary still shows total
      expect(diff.truncated).toBe(true);
    });

    it('should support including only specific change types', async () => {
      await collection.insertMany([
        { name: 'User1', value: 1 },
        { name: 'User2', value: 2 },
      ]);

      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });
      await branchCollection.insertOne({ name: 'NewUser', value: 100 });
      await branchCollection.updateOne({ name: 'User1' }, { $set: { value: 999 } });
      await branchCollection.deleteOne({ name: 'User2' });

      // Only get inserts
      const diff = await db.diff('feature', { changeTypes: ['insert'] });

      expect(diff.inserted).toHaveLength(1);
      expect(diff.updated).toHaveLength(0);
      expect(diff.deleted).toHaveLength(0);
    });
  });

  // ==========================================================================
  // Error Handling
  // ==========================================================================

  describe('Error handling', () => {
    it('should throw error for non-existent branch', async () => {
      await expect(async () => {
        await db.diff('non-existent-branch');
      }).rejects.toThrow(/branch.*not found/i);
    });

    it('should throw error for main branch (cannot diff main against itself)', async () => {
      await expect(async () => {
        await db.diff(DEFAULT_BRANCH);
      }).rejects.toThrow(/cannot diff.*main/i);
    });
  });

  // ==========================================================================
  // Field-Level Diff
  // ==========================================================================

  describe('Field-level diff', () => {
    it('should show which fields changed in an update', async () => {
      await collection.insertOne({ name: 'User', value: 100, category: 'premium' });

      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });
      await branchCollection.updateOne({ name: 'User' }, { $set: { value: 999 } });

      const diff = await db.diff('feature');

      expect(diff.updated).toHaveLength(1);
      expect(diff.updated[0].changedFields).toContain('value');
      expect(diff.updated[0].changedFields).not.toContain('name');
      expect(diff.updated[0].changedFields).not.toContain('category');
    });

    it('should detect added fields', async () => {
      await collection.insertOne({ name: 'User', value: 100 });

      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });
      await branchCollection.updateOne({ name: 'User' }, { $set: { category: 'premium' } });

      const diff = await db.diff('feature');

      expect(diff.updated).toHaveLength(1);
      expect(diff.updated[0].addedFields).toContain('category');
    });

    it('should detect removed fields', async () => {
      await collection.insertOne({ name: 'User', value: 100, category: 'premium' });

      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });

      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });
      await branchCollection.updateOne({ name: 'User' }, { $unset: { category: '' } });

      const diff = await db.diff('feature');

      expect(diff.updated).toHaveLength(1);
      expect(diff.updated[0].removedFields).toContain('category');
    });
  });

  // ==========================================================================
  // Diff Against Specific Snapshot
  // ==========================================================================

  describe('Diff against specific snapshot', () => {
    it('should diff against a specific snapshot ID', async () => {
      // Set up initial data
      await collection.insertOne({ name: 'User', value: 100 });

      // Create branch from specific snapshot
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'snapshot-1',
        fromSnapshotId: 1,
      });

      // Make changes after branch creation on main
      await collection.insertOne({ name: 'AfterBranch', value: 200 });

      // Make changes on branch
      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });
      await branchCollection.insertOne({ name: 'BranchUser', value: 300 });

      // Diff against the branch point should not include main's later changes
      const diff = await db.diff('feature');

      // Should only see the branch's insert
      expect(diff.inserted).toHaveLength(1);
      expect(diff.inserted[0].document.name).toBe('BranchUser');
    });
  });
});
