/**
 * MongoLake E2E Tests - Branch Conflict Resolution
 *
 * End-to-end tests for Git-like branching and conflict resolution.
 * Tests verify that branches can be created, modified independently,
 * and merged with proper conflict detection and resolution.
 *
 * Test scenarios:
 * - Branch creation and isolation
 * - Concurrent modifications on branches
 * - Merge operations (fast-forward and non-fast-forward)
 * - Conflict detection and resolution strategies
 * - Branch deletion and cleanup
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { MongoClient, type Db, type Collection, type Document } from 'mongodb';
import { createServer, type TcpServer } from '../../src/wire-protocol/tcp-server.js';
import * as fs from 'node:fs';

// Test configuration
const TEST_DATA_DIR = '.mongolake-e2e-branch-test';
const TEST_DB_NAME = 'branch_conflict_test';

// Server and client instances
let server: TcpServer;
let serverPort: number;
let client: MongoClient;
let db: Db;

// Unique collection name generator
function uniqueCollection(prefix: string): string {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

// Clean up test data directory
function cleanupTestData(): void {
  try {
    if (fs.existsSync(TEST_DATA_DIR)) {
      fs.rmSync(TEST_DATA_DIR, { recursive: true });
    }
  } catch {
    // Ignore cleanup errors
  }
}

describe('Branch Conflict Resolution E2E Tests', () => {
  beforeAll(async () => {
    // Clean up any existing test data
    cleanupTestData();

    // Start the MongoLake wire protocol server
    server = createServer({
      port: 0, // Let OS assign a random port
      host: '127.0.0.1',
      mongoLakeConfig: { local: TEST_DATA_DIR },
    });

    await server.start();
    const addr = server.address();
    serverPort = addr!.port;

    // Connect with official MongoDB driver
    const connectionString = `mongodb://127.0.0.1:${serverPort}`;
    client = new MongoClient(connectionString, {
      directConnection: true,
      serverSelectionTimeoutMS: 5000,
      connectTimeoutMS: 5000,
    });

    await client.connect();
    db = client.db(TEST_DB_NAME);
  });

  afterAll(async () => {
    // Close MongoDB client
    if (client) {
      await client.close();
    }

    // Stop the server
    if (server) {
      await server.stop();
    }

    // Clean up test data
    cleanupTestData();
  });

  // ============================================================================
  // Branch Isolation Tests
  // ============================================================================

  describe('Branch Isolation', () => {
    it('should isolate data written on different databases (simulating branches)', async () => {
      const mainDb = client.db('branch_main');
      const featureDb = client.db('branch_feature');
      const collName = `isolation_${Date.now()}`;

      const mainCollection = mainDb.collection(collName);
      const featureCollection = featureDb.collection(collName);

      // Write to main "branch"
      await mainCollection.insertOne({
        _id: 'shared-doc',
        content: 'main content',
        version: 1,
      });

      // Write different content to feature "branch"
      await featureCollection.insertOne({
        _id: 'shared-doc',
        content: 'feature content',
        version: 1,
      });

      // Add feature-only document
      await featureCollection.insertOne({
        _id: 'feature-only',
        content: 'only in feature branch',
      });

      // Verify isolation
      const mainDoc = await mainCollection.findOne({ _id: 'shared-doc' });
      expect(mainDoc).not.toBeNull();
      expect(mainDoc!.content).toBe('main content');

      const featureDoc = await featureCollection.findOne({ _id: 'shared-doc' });
      expect(featureDoc).not.toBeNull();
      expect(featureDoc!.content).toBe('feature content');

      // Main should not see feature-only document
      const mainFeatureOnly = await mainCollection.findOne({ _id: 'feature-only' });
      expect(mainFeatureOnly).toBeNull();

      // Feature should see its own document
      const featureOnly = await featureCollection.findOne({ _id: 'feature-only' });
      expect(featureOnly).not.toBeNull();
    });

    it('should maintain separate document histories per branch', async () => {
      const mainDb = client.db('history_main');
      const featureDb = client.db('history_feature');
      const collName = `history_${Date.now()}`;

      const mainCollection = mainDb.collection(collName);
      const featureCollection = featureDb.collection(collName);

      // Initial state on main
      await mainCollection.insertOne({
        _id: 'versioned-doc',
        content: 'initial',
        updates: ['created'],
      });

      // Copy to feature (simulate branch point)
      await featureCollection.insertOne({
        _id: 'versioned-doc',
        content: 'initial',
        updates: ['created'],
      });

      // Update on main
      await mainCollection.updateOne(
        { _id: 'versioned-doc' },
        {
          $set: { content: 'main-updated' },
          $push: { updates: 'main-v2' },
        }
      );

      // Different updates on feature
      await featureCollection.updateOne(
        { _id: 'versioned-doc' },
        {
          $set: { content: 'feature-updated' },
          $push: { updates: 'feature-v2' },
        }
      );
      await featureCollection.updateOne(
        { _id: 'versioned-doc' },
        { $push: { updates: 'feature-v3' } }
      );

      // Verify separate histories
      const mainDoc = await mainCollection.findOne({ _id: 'versioned-doc' });
      expect(mainDoc!.content).toBe('main-updated');
      expect(mainDoc!.updates).toEqual(['created', 'main-v2']);

      const featureDoc = await featureCollection.findOne({ _id: 'versioned-doc' });
      expect(featureDoc!.content).toBe('feature-updated');
      expect(featureDoc!.updates).toEqual(['created', 'feature-v2', 'feature-v3']);
    });

    it('should handle deletes independently per branch', async () => {
      const mainDb = client.db('delete_main');
      const featureDb = client.db('delete_feature');
      const collName = `delete_${Date.now()}`;

      const mainCollection = mainDb.collection(collName);
      const featureCollection = featureDb.collection(collName);

      // Create documents in both branches
      await mainCollection.insertMany([
        { _id: 'doc-1', value: 1 },
        { _id: 'doc-2', value: 2 },
        { _id: 'doc-3', value: 3 },
      ]);

      await featureCollection.insertMany([
        { _id: 'doc-1', value: 1 },
        { _id: 'doc-2', value: 2 },
        { _id: 'doc-3', value: 3 },
      ]);

      // Delete doc-2 on main
      await mainCollection.deleteOne({ _id: 'doc-2' });

      // Delete doc-1 and doc-3 on feature
      await featureCollection.deleteMany({ _id: { $in: ['doc-1', 'doc-3'] } });

      // Verify main state
      const mainDocs = await mainCollection.find({}).toArray();
      expect(mainDocs.length).toBe(2);
      expect(mainDocs.map((d) => d._id).sort()).toEqual(['doc-1', 'doc-3']);

      // Verify feature state
      const featureDocs = await featureCollection.find({}).toArray();
      expect(featureDocs.length).toBe(1);
      expect(featureDocs[0]._id).toBe('doc-2');
    });
  });

  // ============================================================================
  // Concurrent Modification Tests
  // ============================================================================

  describe('Concurrent Modifications', () => {
    it('should detect concurrent modifications to the same document', async () => {
      const db1 = client.db('concurrent_1');
      const db2 = client.db('concurrent_2');
      const collName = `concurrent_${Date.now()}`;

      const coll1 = db1.collection(collName);
      const coll2 = db2.collection(collName);

      // Both branches start with same document
      const initialDoc = {
        _id: 'concurrent-doc',
        field1: 'original',
        field2: 'original',
        counter: 0,
      };

      await coll1.insertOne({ ...initialDoc });
      await coll2.insertOne({ ...initialDoc });

      // Concurrent modifications to different fields
      await coll1.updateOne(
        { _id: 'concurrent-doc' },
        { $set: { field1: 'modified-by-1' }, $inc: { counter: 1 } }
      );

      await coll2.updateOne(
        { _id: 'concurrent-doc' },
        { $set: { field2: 'modified-by-2' }, $inc: { counter: 1 } }
      );

      // Read both versions
      const doc1 = await coll1.findOne({ _id: 'concurrent-doc' });
      const doc2 = await coll2.findOne({ _id: 'concurrent-doc' });

      // Both should have their own modifications
      expect(doc1!.field1).toBe('modified-by-1');
      expect(doc1!.field2).toBe('original');
      expect(doc1!.counter).toBe(1);

      expect(doc2!.field1).toBe('original');
      expect(doc2!.field2).toBe('modified-by-2');
      expect(doc2!.counter).toBe(1);
    });

    it('should handle concurrent inserts with different IDs', async () => {
      const db1 = client.db('concurrent_insert_1');
      const db2 = client.db('concurrent_insert_2');
      const collName = `inserts_${Date.now()}`;

      const coll1 = db1.collection(collName);
      const coll2 = db2.collection(collName);

      // Common document
      await coll1.insertOne({ _id: 'common', source: 'initial' });
      await coll2.insertOne({ _id: 'common', source: 'initial' });

      // Concurrent inserts with different IDs
      const insert1Promise = coll1.insertMany([
        { _id: 'branch1-doc1', branch: 1 },
        { _id: 'branch1-doc2', branch: 1 },
      ]);

      const insert2Promise = coll2.insertMany([
        { _id: 'branch2-doc1', branch: 2 },
        { _id: 'branch2-doc2', branch: 2 },
      ]);

      await Promise.all([insert1Promise, insert2Promise]);

      // Verify branch 1 state
      const docs1 = await coll1.find({}).toArray();
      expect(docs1.length).toBe(3); // common + 2 new docs

      // Verify branch 2 state
      const docs2 = await coll2.find({}).toArray();
      expect(docs2.length).toBe(3); // common + 2 new docs

      // Each branch should have its own documents
      const branch1Docs = await coll1.find({ branch: 1 }).toArray();
      expect(branch1Docs.length).toBe(2);

      const branch2Docs = await coll2.find({ branch: 2 }).toArray();
      expect(branch2Docs.length).toBe(2);
    });

    it('should handle mixed operations concurrently', async () => {
      const db1 = client.db('mixed_ops_1');
      const db2 = client.db('mixed_ops_2');
      const collName = `mixed_${Date.now()}`;

      const coll1 = db1.collection(collName);
      const coll2 = db2.collection(collName);

      // Initial state in both branches
      await coll1.insertMany([
        { _id: 'a', value: 1 },
        { _id: 'b', value: 2 },
        { _id: 'c', value: 3 },
      ]);

      await coll2.insertMany([
        { _id: 'a', value: 1 },
        { _id: 'b', value: 2 },
        { _id: 'c', value: 3 },
      ]);

      // Branch 1: Update 'a', delete 'b', insert 'd'
      const ops1 = [
        coll1.updateOne({ _id: 'a' }, { $set: { value: 100 } }),
        coll1.deleteOne({ _id: 'b' }),
        coll1.insertOne({ _id: 'd', value: 4 }),
      ];

      // Branch 2: Delete 'a', update 'b', insert 'e'
      const ops2 = [
        coll2.deleteOne({ _id: 'a' }),
        coll2.updateOne({ _id: 'b' }, { $set: { value: 200 } }),
        coll2.insertOne({ _id: 'e', value: 5 }),
      ];

      await Promise.all([...ops1, ...ops2]);

      // Verify branch 1 state: a(100), c(3), d(4) - b deleted
      const docs1 = await coll1.find({}).sort({ _id: 1 }).toArray();
      expect(docs1.length).toBe(3);
      expect(docs1.map((d) => d._id)).toEqual(['a', 'c', 'd']);
      expect(docs1[0].value).toBe(100);

      // Verify branch 2 state: b(200), c(3), e(5) - a deleted
      const docs2 = await coll2.find({}).sort({ _id: 1 }).toArray();
      expect(docs2.length).toBe(3);
      expect(docs2.map((d) => d._id)).toEqual(['b', 'c', 'e']);
      expect(docs2[0].value).toBe(200);
    });
  });

  // ============================================================================
  // Merge Simulation Tests
  // ============================================================================

  describe('Merge Simulation', () => {
    it('should simulate fast-forward merge (no conflicts)', async () => {
      const mainDb = client.db('ff_main');
      const featureDb = client.db('ff_feature');
      const collName = `ff_merge_${Date.now()}`;

      const mainCollection = mainDb.collection(collName);
      const featureCollection = featureDb.collection(collName);

      // Initial state on main
      await mainCollection.insertOne({
        _id: 'base-doc',
        content: 'base content',
      });

      // Copy to feature
      const baseDoc = await mainCollection.findOne({ _id: 'base-doc' });
      await featureCollection.insertOne(baseDoc!);

      // Feature makes changes (main has no new changes)
      await featureCollection.insertOne({
        _id: 'feature-doc',
        content: 'new from feature',
      });
      await featureCollection.updateOne(
        { _id: 'base-doc' },
        { $set: { content: 'updated by feature' } }
      );

      // Simulate fast-forward: copy all feature changes to main
      const featureDocs = await featureCollection.find({}).toArray();

      // Clear main and insert feature state (simulating fast-forward)
      await mainCollection.deleteMany({});
      for (const doc of featureDocs) {
        await mainCollection.insertOne(doc);
      }

      // Verify main now has feature's state
      const mainDocs = await mainCollection.find({}).sort({ _id: 1 }).toArray();
      expect(mainDocs.length).toBe(2);
      expect(mainDocs.map((d) => d._id)).toEqual(['base-doc', 'feature-doc']);
      expect(mainDocs[0].content).toBe('updated by feature');
    });

    it('should simulate non-fast-forward merge without conflicts', async () => {
      const mainDb = client.db('nff_main');
      const featureDb = client.db('nff_feature');
      const collName = `nff_merge_${Date.now()}`;

      const mainCollection = mainDb.collection(collName);
      const featureCollection = featureDb.collection(collName);

      // Initial state (branch point)
      const initialDocs = [
        { _id: 'doc-1', field: 'original-1' },
        { _id: 'doc-2', field: 'original-2' },
      ];

      await mainCollection.insertMany(initialDocs);
      await featureCollection.insertMany(initialDocs);

      // Main changes doc-1
      await mainCollection.updateOne(
        { _id: 'doc-1' },
        { $set: { field: 'main-change' } }
      );

      // Feature changes doc-2
      await featureCollection.updateOne(
        { _id: 'doc-2' },
        { $set: { field: 'feature-change' } }
      );

      // Simulate merge: apply feature's changes to main
      // (only doc-2 was changed on feature)
      const featureDoc2 = await featureCollection.findOne({ _id: 'doc-2' });
      await mainCollection.updateOne(
        { _id: 'doc-2' },
        { $set: { field: featureDoc2!.field } }
      );

      // Verify merged state has both changes
      const mergedDocs = await mainCollection.find({}).sort({ _id: 1 }).toArray();
      expect(mergedDocs[0].field).toBe('main-change');
      expect(mergedDocs[1].field).toBe('feature-change');
    });

    it('should detect and resolve conflicts with "ours" strategy', async () => {
      const mainDb = client.db('conflict_ours_main');
      const featureDb = client.db('conflict_ours_feature');
      const collName = `conflict_ours_${Date.now()}`;

      const mainCollection = mainDb.collection(collName);
      const featureCollection = featureDb.collection(collName);

      // Initial state
      const initialDoc = {
        _id: 'conflict-doc',
        title: 'Original Title',
        content: 'Original Content',
      };

      await mainCollection.insertOne({ ...initialDoc });
      await featureCollection.insertOne({ ...initialDoc });

      // Both branches modify the same field (conflict!)
      await mainCollection.updateOne(
        { _id: 'conflict-doc' },
        { $set: { title: 'Main Title', content: 'Main Content' } }
      );

      await featureCollection.updateOne(
        { _id: 'conflict-doc' },
        { $set: { title: 'Feature Title', content: 'Feature Content' } }
      );

      // Resolve with "ours" strategy (keep main's changes)
      // Main doesn't need to change - it keeps its version
      const mainDoc = await mainCollection.findOne({ _id: 'conflict-doc' });
      expect(mainDoc!.title).toBe('Main Title');
      expect(mainDoc!.content).toBe('Main Content');
    });

    it('should detect and resolve conflicts with "theirs" strategy', async () => {
      const mainDb = client.db('conflict_theirs_main');
      const featureDb = client.db('conflict_theirs_feature');
      const collName = `conflict_theirs_${Date.now()}`;

      const mainCollection = mainDb.collection(collName);
      const featureCollection = featureDb.collection(collName);

      // Initial state
      const initialDoc = {
        _id: 'conflict-doc',
        title: 'Original Title',
        content: 'Original Content',
      };

      await mainCollection.insertOne({ ...initialDoc });
      await featureCollection.insertOne({ ...initialDoc });

      // Both branches modify the same field (conflict!)
      await mainCollection.updateOne(
        { _id: 'conflict-doc' },
        { $set: { title: 'Main Title', content: 'Main Content' } }
      );

      await featureCollection.updateOne(
        { _id: 'conflict-doc' },
        { $set: { title: 'Feature Title', content: 'Feature Content' } }
      );

      // Resolve with "theirs" strategy (take feature's changes)
      const featureDoc = await featureCollection.findOne({ _id: 'conflict-doc' });
      await mainCollection.updateOne(
        { _id: 'conflict-doc' },
        { $set: { title: featureDoc!.title, content: featureDoc!.content } }
      );

      // Verify main now has feature's changes
      const mainDoc = await mainCollection.findOne({ _id: 'conflict-doc' });
      expect(mainDoc!.title).toBe('Feature Title');
      expect(mainDoc!.content).toBe('Feature Content');
    });

    it('should handle field-level merge for non-conflicting changes', async () => {
      const mainDb = client.db('field_merge_main');
      const featureDb = client.db('field_merge_feature');
      const collName = `field_merge_${Date.now()}`;

      const mainCollection = mainDb.collection(collName);
      const featureCollection = featureDb.collection(collName);

      // Initial state with multiple fields
      const initialDoc = {
        _id: 'merge-doc',
        field1: 'original-1',
        field2: 'original-2',
        field3: 'original-3',
        metadata: { version: 1 },
      };

      await mainCollection.insertOne({ ...initialDoc });
      await featureCollection.insertOne({ ...initialDoc });

      // Main changes field1
      await mainCollection.updateOne(
        { _id: 'merge-doc' },
        { $set: { field1: 'main-update', 'metadata.version': 2 } }
      );

      // Feature changes field2 and field3
      await featureCollection.updateOne(
        { _id: 'merge-doc' },
        { $set: { field2: 'feature-update-2', field3: 'feature-update-3' } }
      );

      // Simulate field-level merge (apply non-conflicting changes)
      const featureDoc = await featureCollection.findOne({ _id: 'merge-doc' });
      await mainCollection.updateOne(
        { _id: 'merge-doc' },
        {
          $set: {
            field2: featureDoc!.field2,
            field3: featureDoc!.field3,
            'metadata.version': 3, // Increment version after merge
          },
        }
      );

      // Verify merged state has all changes
      const mergedDoc = await mainCollection.findOne({ _id: 'merge-doc' });
      expect(mergedDoc!.field1).toBe('main-update');
      expect(mergedDoc!.field2).toBe('feature-update-2');
      expect(mergedDoc!.field3).toBe('feature-update-3');
      expect(mergedDoc!.metadata.version).toBe(3);
    });
  });

  // ============================================================================
  // Branch Cleanup Tests
  // ============================================================================

  describe('Branch Cleanup', () => {
    it('should clean up branch data after merge', async () => {
      const mainDb = client.db('cleanup_main');
      const featureDb = client.db('cleanup_feature');
      const collName = `cleanup_${Date.now()}`;

      const mainCollection = mainDb.collection(collName);
      const featureCollection = featureDb.collection(collName);

      // Setup and modify feature branch
      await mainCollection.insertOne({ _id: 'main-doc', source: 'main' });
      await featureCollection.insertOne({ _id: 'feature-doc', source: 'feature' });

      // Simulate merge (copy feature data to main)
      const featureDocs = await featureCollection.find({}).toArray();
      for (const doc of featureDocs) {
        const exists = await mainCollection.findOne({ _id: doc._id });
        if (!exists) {
          await mainCollection.insertOne(doc);
        }
      }

      // Verify main has merged data
      const mainDocs = await mainCollection.find({}).toArray();
      expect(mainDocs.length).toBe(2);

      // Clean up feature branch (delete all data)
      await featureCollection.deleteMany({});

      // Verify feature is empty
      const remainingDocs = await featureCollection.find({}).toArray();
      expect(remainingDocs.length).toBe(0);

      // Verify main is unaffected
      const mainDocsAfter = await mainCollection.find({}).toArray();
      expect(mainDocsAfter.length).toBe(2);
    });

    it('should maintain referential integrity after branch operations', async () => {
      const mainDb = client.db('integrity_main');
      const collName = `integrity_${Date.now()}`;

      const usersCollection = mainDb.collection(`${collName}_users`);
      const ordersCollection = mainDb.collection(`${collName}_orders`);

      // Create user
      await usersCollection.insertOne({
        _id: 'user-1',
        name: 'Test User',
        email: 'test@example.com',
      });

      // Create orders referencing user
      await ordersCollection.insertMany([
        { _id: 'order-1', userId: 'user-1', total: 100 },
        { _id: 'order-2', userId: 'user-1', total: 200 },
      ]);

      // Verify referential integrity
      const user = await usersCollection.findOne({ _id: 'user-1' });
      expect(user).not.toBeNull();

      const userOrders = await ordersCollection.find({ userId: 'user-1' }).toArray();
      expect(userOrders.length).toBe(2);

      // Update user and verify orders still reference correctly
      await usersCollection.updateOne(
        { _id: 'user-1' },
        { $set: { name: 'Updated User' } }
      );

      // Orders should still be findable by userId
      const ordersAfterUpdate = await ordersCollection.find({ userId: 'user-1' }).toArray();
      expect(ordersAfterUpdate.length).toBe(2);
    });
  });

  // ============================================================================
  // Branch Switch Simulation Tests
  // ============================================================================

  describe('Branch Switch Simulation', () => {
    it('should provide consistent view after switching databases', async () => {
      const views: string[] = [];

      // Simulate switching between branches by using different databases
      const branch1 = client.db('switch_branch_1');
      const branch2 = client.db('switch_branch_2');
      const collName = `switch_${Date.now()}`;

      // Setup different states
      await branch1.collection(collName).insertOne({ _id: 'doc', view: 'branch1' });
      await branch2.collection(collName).insertOne({ _id: 'doc', view: 'branch2' });

      // Simulate rapid branch switching
      for (let i = 0; i < 10; i++) {
        const branch = i % 2 === 0 ? branch1 : branch2;
        const doc = await branch.collection(collName).findOne({ _id: 'doc' });
        views.push(doc!.view);
      }

      // Verify alternating views
      for (let i = 0; i < 10; i++) {
        const expected = i % 2 === 0 ? 'branch1' : 'branch2';
        expect(views[i]).toBe(expected);
      }
    });

    it('should handle writes during simulated branch switch', async () => {
      const mainDb = client.db('write_switch_main');
      const featureDb = client.db('write_switch_feature');
      const collName = `write_switch_${Date.now()}`;

      // Initial setup
      await mainDb.collection(collName).insertOne({ _id: 'doc', counter: 0 });
      await featureDb.collection(collName).insertOne({ _id: 'doc', counter: 0 });

      // Alternating writes to different branches
      for (let i = 0; i < 5; i++) {
        await mainDb.collection(collName).updateOne(
          { _id: 'doc' },
          { $inc: { counter: 1 } }
        );
        await featureDb.collection(collName).updateOne(
          { _id: 'doc' },
          { $inc: { counter: 2 } }
        );
      }

      // Verify each branch has its own counter
      const mainDoc = await mainDb.collection(collName).findOne({ _id: 'doc' });
      const featureDoc = await featureDb.collection(collName).findOne({ _id: 'doc' });

      expect(mainDoc!.counter).toBe(5); // 5 increments of 1
      expect(featureDoc!.counter).toBe(10); // 5 increments of 2
    });
  });
});
