/**
 * Branch Switching Tests
 *
 * Tests for querying data from specific branches using db('collection', { branch: 'x' }).
 * Branch switching allows reading from isolated branch data layered on top of the base snapshot.
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
}

describe('Branch Switching', () => {
  let storage: MemoryStorage;
  let db: Database;
  let collection: Collection<TestDoc>;
  let branchStore: BranchStore;

  beforeEach(async () => {
    storage = new MemoryStorage();
    // Create database directly with storage (same as time-travel tests)
    const config = { local: '.test-mongolake' };
    db = new Database('testdb', storage, config);
    collection = db.collection<TestDoc>('users');
    branchStore = db.getBranchStore();

    // Initialize main branch
    await branchStore.initializeDefaultBranch('initial-commit');
  });

  // ==========================================================================
  // Branch-Aware Collection Access
  // ==========================================================================

  describe('Branch-aware collection access', () => {
    it('should access collection on main branch by default', async () => {
      // Insert documents on main branch
      await collection.insertMany([
        { name: 'Alice', value: 1 },
        { name: 'Bob', value: 2 },
      ]);

      // Access without branch option should read from main
      const docs = await collection.find().toArray();
      expect(docs).toHaveLength(2);
      expect(docs.map((d) => d.name).sort()).toEqual(['Alice', 'Bob']);
    });

    it('should access collection on a specific branch', async () => {
      // Insert documents on main branch
      await collection.insertMany([
        { name: 'Alice', value: 1 },
        { name: 'Bob', value: 2 },
      ]);

      // Create a feature branch
      await branchStore.createBranch({
        name: 'feature-branch',
        baseCommit: 'initial-commit',
      });

      // Access collection on feature branch
      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature-branch' });
      const docs = await branchCollection.find().toArray();

      // Branch should see main branch data (inherited)
      expect(docs).toHaveLength(2);
      expect(docs.map((d) => d.name).sort()).toEqual(['Alice', 'Bob']);
    });

    it('should throw error for non-existent branch', async () => {
      await expect(async () => {
        const branchCollection = db.collection<TestDoc>('users', { branch: 'non-existent' });
        await branchCollection.find().toArray();
      }).rejects.toThrow(/branch.*not found/i);
    });
  });

  // ==========================================================================
  // Branch Reads Data from Base Snapshot
  // ==========================================================================

  describe('Branch reads base snapshot data', () => {
    beforeEach(async () => {
      // Set up base data on main branch
      await collection.insertMany([
        { name: 'MainUser1', value: 100 },
        { name: 'MainUser2', value: 200 },
      ]);

      // Create feature branch
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });
    });

    it('should read documents from base snapshot on new branch', async () => {
      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });
      const docs = await branchCollection.find().toArray();

      // Branch should see all base data
      expect(docs).toHaveLength(2);
      expect(docs.map((d) => d.name).sort()).toEqual(['MainUser1', 'MainUser2']);
    });

    it('should support findOne on branch', async () => {
      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });
      const doc = await branchCollection.findOne({ name: 'MainUser1' });

      expect(doc).not.toBeNull();
      expect(doc?.value).toBe(100);
    });

    it('should support filtered queries on branch', async () => {
      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });
      const docs = await branchCollection.find({ value: { $gte: 150 } }).toArray();

      expect(docs).toHaveLength(1);
      expect(docs[0].name).toBe('MainUser2');
    });

    it('should support countDocuments on branch', async () => {
      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });
      const count = await branchCollection.countDocuments();

      expect(count).toBe(2);
    });
  });

  // ==========================================================================
  // Branch Sees Its Own Writes
  // ==========================================================================

  describe('Branch sees its own writes', () => {
    beforeEach(async () => {
      // Set up base data
      await collection.insertMany([
        { name: 'BaseUser', value: 1 },
      ]);

      // Create feature branch
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });
    });

    it('should see new documents inserted on branch', async () => {
      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });

      // Insert on branch
      await branchCollection.insertOne({ name: 'BranchUser', value: 99 });

      // Should see both base and branch data
      const docs = await branchCollection.find().toArray();
      expect(docs).toHaveLength(2);
      expect(docs.map((d) => d.name).sort()).toEqual(['BaseUser', 'BranchUser']);
    });

    it('should see updates made on branch', async () => {
      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });

      // Update document on branch
      await branchCollection.updateOne({ name: 'BaseUser' }, { $set: { value: 999 } });

      // Should see updated value
      const doc = await branchCollection.findOne({ name: 'BaseUser' });
      expect(doc?.value).toBe(999);
    });

    it('should see documents deleted on branch as deleted', async () => {
      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });

      // Delete document on branch
      await branchCollection.deleteOne({ name: 'BaseUser' });

      // Document should not be visible on branch
      const doc = await branchCollection.findOne({ name: 'BaseUser' });
      expect(doc).toBeNull();

      const count = await branchCollection.countDocuments();
      expect(count).toBe(0);
    });

    it('should layer branch changes on top of base data', async () => {
      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });

      // Insert new document
      await branchCollection.insertOne({ name: 'NewUser', value: 50 });

      // Update existing document
      await branchCollection.updateOne({ name: 'BaseUser' }, { $set: { value: 10 } });

      // Query should show layered results
      const docs = await branchCollection.find().sort({ name: 1 }).toArray();
      expect(docs).toHaveLength(2);
      expect(docs[0]).toMatchObject({ name: 'BaseUser', value: 10 });
      expect(docs[1]).toMatchObject({ name: 'NewUser', value: 50 });
    });
  });

  // ==========================================================================
  // Branch Isolation
  // ==========================================================================

  describe('Branch isolation', () => {
    beforeEach(async () => {
      // Set up base data
      await collection.insertMany([
        { name: 'SharedUser', value: 100 },
      ]);

      // Create feature branch
      await branchStore.createBranch({
        name: 'feature',
        baseCommit: 'commit-1',
      });
    });

    it('branch writes should not affect main branch', async () => {
      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });
      const mainCollection = db.collection<TestDoc>('users');

      // Insert on branch
      await branchCollection.insertOne({ name: 'BranchOnlyUser', value: 999 });

      // Main should not see branch data
      const mainDocs = await mainCollection.find().toArray();
      expect(mainDocs).toHaveLength(1);
      expect(mainDocs[0].name).toBe('SharedUser');
    });

    it('branch updates should not affect main branch', async () => {
      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });
      const mainCollection = db.collection<TestDoc>('users');

      // Update on branch
      await branchCollection.updateOne({ name: 'SharedUser' }, { $set: { value: 999 } });

      // Main should see original value
      const mainDoc = await mainCollection.findOne({ name: 'SharedUser' });
      expect(mainDoc?.value).toBe(100);
    });

    it('branch deletes should not affect main branch', async () => {
      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });
      const mainCollection = db.collection<TestDoc>('users');

      // Delete on branch
      await branchCollection.deleteOne({ name: 'SharedUser' });

      // Main should still see document
      const mainDoc = await mainCollection.findOne({ name: 'SharedUser' });
      expect(mainDoc).not.toBeNull();
      expect(mainDoc?.value).toBe(100);
    });

    it('main writes after branch creation should not affect branch', async () => {
      // Small delay to ensure different timestamps for branch creation vs main insert
      await new Promise((r) => setTimeout(r, 10));

      const branchCollection = db.collection<TestDoc>('users', { branch: 'feature' });
      const mainCollection = db.collection<TestDoc>('users');

      // Insert on main after branch was created
      await mainCollection.insertOne({ name: 'NewMainUser', value: 500 });

      // Branch should not see new main data (branch was created from earlier snapshot)
      const branchDocs = await branchCollection.find().toArray();
      expect(branchDocs).toHaveLength(1);
      expect(branchDocs[0].name).toBe('SharedUser');
    });
  });

  // ==========================================================================
  // Switching Between Branches
  // ==========================================================================

  describe('Switching between branches', () => {
    beforeEach(async () => {
      // Set up base data
      await collection.insertMany([
        { name: 'User1', value: 1 },
      ]);

      // Create two branches
      await branchStore.createBranch({
        name: 'branch-a',
        baseCommit: 'commit-1',
      });

      await branchStore.createBranch({
        name: 'branch-b',
        baseCommit: 'commit-1',
      });
    });

    it('should read different data from different branches', async () => {
      const branchACollection = db.collection<TestDoc>('users', { branch: 'branch-a' });
      const branchBCollection = db.collection<TestDoc>('users', { branch: 'branch-b' });

      // Insert different data on each branch
      await branchACollection.insertOne({ name: 'BranchAUser', value: 100 });
      await branchBCollection.insertOne({ name: 'BranchBUser', value: 200 });

      // Each branch should see only its own data plus base
      const branchADocs = await branchACollection.find().toArray();
      expect(branchADocs.map((d) => d.name).sort()).toEqual(['BranchAUser', 'User1']);

      const branchBDocs = await branchBCollection.find().toArray();
      expect(branchBDocs.map((d) => d.name).sort()).toEqual(['BranchBUser', 'User1']);
    });

    it('should maintain independent state when switching between branches', async () => {
      const branchACollection = db.collection<TestDoc>('users', { branch: 'branch-a' });
      const branchBCollection = db.collection<TestDoc>('users', { branch: 'branch-b' });

      // Update same document differently on each branch
      await branchACollection.updateOne({ name: 'User1' }, { $set: { value: 111 } });
      await branchBCollection.updateOne({ name: 'User1' }, { $set: { value: 222 } });

      // Each branch should see its own version
      const docA = await branchACollection.findOne({ name: 'User1' });
      expect(docA?.value).toBe(111);

      const docB = await branchBCollection.findOne({ name: 'User1' });
      expect(docB?.value).toBe(222);
    });

    it('should allow accessing main branch explicitly', async () => {
      const mainCollection = db.collection<TestDoc>('users', { branch: DEFAULT_BRANCH });
      const branchACollection = db.collection<TestDoc>('users', { branch: 'branch-a' });

      // Insert on branch
      await branchACollection.insertOne({ name: 'BranchOnly', value: 99 });

      // Main should not see branch data
      const mainDocs = await mainCollection.find().toArray();
      expect(mainDocs).toHaveLength(1);
      expect(mainDocs[0].name).toBe('User1');
    });
  });

  // ==========================================================================
  // Branch Context
  // ==========================================================================

  describe('Branch context', () => {
    beforeEach(async () => {
      await branchStore.createBranch({
        name: 'test-branch',
        baseCommit: 'commit-1',
      });
    });

    it('should expose current branch name', async () => {
      const branchCollection = db.collection<TestDoc>('users', { branch: 'test-branch' });

      expect(branchCollection.branch).toBe('test-branch');
    });

    it('should return undefined branch for main collection', async () => {
      const mainCollection = db.collection<TestDoc>('users');

      expect(mainCollection.branch).toBeUndefined();
    });

    it('should support checking if collection is on a branch', async () => {
      const branchCollection = db.collection<TestDoc>('users', { branch: 'test-branch' });
      const mainCollection = db.collection<TestDoc>('users');

      expect(branchCollection.isOnBranch()).toBe(true);
      expect(mainCollection.isOnBranch()).toBe(false);
    });
  });

  // ==========================================================================
  // Edge Cases
  // ==========================================================================

  describe('Edge cases', () => {
    it('should handle empty branch (no data on main)', async () => {
      await branchStore.createBranch({
        name: 'empty-branch',
        baseCommit: 'commit-1',
      });

      const branchCollection = db.collection<TestDoc>('users', { branch: 'empty-branch' });
      const docs = await branchCollection.find().toArray();

      expect(docs).toHaveLength(0);
    });

    it('should handle branch with only deletes', async () => {
      await collection.insertOne({ name: 'ToDelete', value: 1 });

      await branchStore.createBranch({
        name: 'delete-branch',
        baseCommit: 'commit-1',
      });

      const branchCollection = db.collection<TestDoc>('users', { branch: 'delete-branch' });
      await branchCollection.deleteOne({ name: 'ToDelete' });

      const docs = await branchCollection.find().toArray();
      expect(docs).toHaveLength(0);
    });

    it('should handle multiple operations on same document in branch', async () => {
      await collection.insertOne({ name: 'MultiOp', value: 1 });

      await branchStore.createBranch({
        name: 'multi-op-branch',
        baseCommit: 'commit-1',
      });

      const branchCollection = db.collection<TestDoc>('users', { branch: 'multi-op-branch' });

      // Multiple updates
      await branchCollection.updateOne({ name: 'MultiOp' }, { $set: { value: 2 } });
      await branchCollection.updateOne({ name: 'MultiOp' }, { $set: { value: 3 } });
      await branchCollection.updateOne({ name: 'MultiOp' }, { $set: { value: 4 } });

      const doc = await branchCollection.findOne({ name: 'MultiOp' });
      expect(doc?.value).toBe(4);
    });

    it('should handle inserting document with same _id as base on branch', async () => {
      const result = await collection.insertOne({ name: 'Original', value: 1 });
      const docId = result.insertedId;

      await branchStore.createBranch({
        name: 'overwrite-branch',
        baseCommit: 'commit-1',
      });

      const branchCollection = db.collection<TestDoc>('users', { branch: 'overwrite-branch' });

      // Delete and re-insert with same ID (simulating replacement)
      await branchCollection.deleteOne({ _id: docId });
      await branchCollection.insertOne({ _id: docId, name: 'Replaced', value: 999 });

      const doc = await branchCollection.findOne({ _id: docId });
      expect(doc?.name).toBe('Replaced');
      expect(doc?.value).toBe(999);
    });
  });

  // ==========================================================================
  // Aggregation on Branch
  // ==========================================================================

  describe('Aggregation on branch', () => {
    beforeEach(async () => {
      await collection.insertMany([
        { name: 'User1', value: 10 },
        { name: 'User2', value: 20 },
      ]);

      await branchStore.createBranch({
        name: 'agg-branch',
        baseCommit: 'commit-1',
      });
    });

    it('should support aggregation pipeline on branch', async () => {
      const branchCollection = db.collection<TestDoc>('users', { branch: 'agg-branch' });

      // Add more data on branch
      await branchCollection.insertOne({ name: 'BranchUser', value: 30 });

      const results = await branchCollection
        .aggregate([
          { $match: { value: { $gte: 15 } } },
          { $sort: { value: 1 } },
        ])
        .toArray();

      expect(results).toHaveLength(2);
      expect(results[0].name).toBe('User2');
      expect(results[1].name).toBe('BranchUser');
    });

    it('should support $group on branch data', async () => {
      const branchCollection = db.collection<TestDoc>('users', { branch: 'agg-branch' });

      const results = await branchCollection
        .aggregate([
          { $group: { _id: null, total: { $sum: '$value' } } },
        ])
        .toArray();

      expect(results[0].total).toBe(30); // 10 + 20
    });
  });
});
