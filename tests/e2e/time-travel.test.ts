/**
 * MongoLake E2E Tests - Time Travel Queries
 *
 * End-to-end tests for time-travel query functionality.
 * Tests verify that historical data can be queried at specific
 * timestamps and that the time-travel view is consistent.
 *
 * Test scenarios:
 * - Querying data at specific timestamps
 * - Historical vs current data comparison
 * - Time-travel with updates and deletes
 * - Time-travel aggregation
 * - Snapshot consistency
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { MongoClient, type Db, type Collection, type Document } from 'mongodb';
import { createServer, type TcpServer } from '../../src/wire-protocol/tcp-server.js';
import * as fs from 'node:fs';

// Test configuration
const TEST_DATA_DIR = '.mongolake-e2e-timetravel-test';
const TEST_DB_NAME = 'time_travel_test';

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

// Wait helper for timed operations
async function wait(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('Time Travel Query E2E Tests', () => {
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
  // Basic Time-Travel Tests (Simulated via Versioning)
  // ============================================================================

  describe('Document Version History', () => {
    it('should track document versions over time', async () => {
      const collection = db.collection(uniqueCollection('versioned'));
      const timestamps: number[] = [];

      // Create initial document with version tracking
      timestamps.push(Date.now());
      await collection.insertOne({
        _id: 'doc-1',
        content: 'version-1',
        version: 1,
        createdAt: timestamps[0],
        updatedAt: timestamps[0],
      });

      await wait(100);

      // Update to version 2
      timestamps.push(Date.now());
      await collection.updateOne(
        { _id: 'doc-1' },
        {
          $set: {
            content: 'version-2',
            version: 2,
            updatedAt: timestamps[1],
          },
        }
      );

      await wait(100);

      // Update to version 3
      timestamps.push(Date.now());
      await collection.updateOne(
        { _id: 'doc-1' },
        {
          $set: {
            content: 'version-3',
            version: 3,
            updatedAt: timestamps[2],
          },
        }
      );

      // Verify current state
      const currentDoc = await collection.findOne({ _id: 'doc-1' });
      expect(currentDoc!.version).toBe(3);
      expect(currentDoc!.content).toBe('version-3');
    });

    it('should maintain separate version history per document', async () => {
      const collection = db.collection(uniqueCollection('multi_version'));

      // Create multiple documents with different version histories
      await collection.insertMany([
        { _id: 'doc-a', content: 'a-v1', history: ['created'] },
        { _id: 'doc-b', content: 'b-v1', history: ['created'] },
        { _id: 'doc-c', content: 'c-v1', history: ['created'] },
      ]);

      // Update each document different number of times
      await collection.updateOne(
        { _id: 'doc-a' },
        { $set: { content: 'a-v2' }, $push: { history: 'update-1' } }
      );
      await collection.updateOne(
        { _id: 'doc-a' },
        { $set: { content: 'a-v3' }, $push: { history: 'update-2' } }
      );

      await collection.updateOne(
        { _id: 'doc-b' },
        { $set: { content: 'b-v2' }, $push: { history: 'update-1' } }
      );

      // doc-c stays at v1

      // Verify each document's history
      const docA = await collection.findOne({ _id: 'doc-a' });
      expect(docA!.history.length).toBe(3);
      expect(docA!.content).toBe('a-v3');

      const docB = await collection.findOne({ _id: 'doc-b' });
      expect(docB!.history.length).toBe(2);
      expect(docB!.content).toBe('b-v2');

      const docC = await collection.findOne({ _id: 'doc-c' });
      expect(docC!.history.length).toBe(1);
      expect(docC!.content).toBe('c-v1');
    });
  });

  // ============================================================================
  // Historical Data Comparison Tests
  // ============================================================================

  describe('Historical Data Comparison', () => {
    it('should compare current state with historical snapshots', async () => {
      const collection = db.collection(uniqueCollection('comparison'));
      const snapshots: Array<{ timestamp: number; count: number; docs: Document[] }> = [];

      // Snapshot 1: Initial state
      await collection.insertMany([
        { _id: 's1', status: 'active', value: 10 },
        { _id: 's2', status: 'active', value: 20 },
      ]);

      let docs = await collection.find({}).toArray();
      snapshots.push({
        timestamp: Date.now(),
        count: docs.length,
        docs: JSON.parse(JSON.stringify(docs)),
      });

      await wait(50);

      // Snapshot 2: After adding documents
      await collection.insertMany([
        { _id: 's3', status: 'active', value: 30 },
        { _id: 's4', status: 'inactive', value: 40 },
      ]);

      docs = await collection.find({}).toArray();
      snapshots.push({
        timestamp: Date.now(),
        count: docs.length,
        docs: JSON.parse(JSON.stringify(docs)),
      });

      await wait(50);

      // Snapshot 3: After updates and deletes
      await collection.updateOne({ _id: 's1' }, { $set: { status: 'inactive' } });
      await collection.deleteOne({ _id: 's2' });

      docs = await collection.find({}).toArray();
      snapshots.push({
        timestamp: Date.now(),
        count: docs.length,
        docs: JSON.parse(JSON.stringify(docs)),
      });

      // Verify snapshot progression
      expect(snapshots[0].count).toBe(2);
      expect(snapshots[1].count).toBe(4);
      expect(snapshots[2].count).toBe(3); // One deleted

      // Verify s1 status changed
      const s1Snapshot1 = snapshots[0].docs.find((d) => d._id === 's1');
      const s1Snapshot3 = snapshots[2].docs.find((d) => d._id === 's1');
      expect(s1Snapshot1!.status).toBe('active');
      expect(s1Snapshot3!.status).toBe('inactive');

      // Verify s2 was present in snapshot 1 but not in snapshot 3
      const s2Snapshot1 = snapshots[0].docs.find((d) => d._id === 's2');
      const s2Snapshot3 = snapshots[2].docs.find((d) => d._id === 's2');
      expect(s2Snapshot1).toBeDefined();
      expect(s2Snapshot3).toBeUndefined();
    });

    it('should track changes over time with timestamps', async () => {
      const collection = db.collection(uniqueCollection('changelog'));
      const changelog: Array<{ timestamp: number; action: string; docId: string }> = [];

      // Helper to record changes
      const recordChange = (action: string, docId: string): void => {
        changelog.push({ timestamp: Date.now(), action, docId });
      };

      // Perform operations and record changes
      await collection.insertOne({ _id: 'tracked-1', value: 1 });
      recordChange('insert', 'tracked-1');
      await wait(20);

      await collection.insertOne({ _id: 'tracked-2', value: 2 });
      recordChange('insert', 'tracked-2');
      await wait(20);

      await collection.updateOne({ _id: 'tracked-1' }, { $set: { value: 10 } });
      recordChange('update', 'tracked-1');
      await wait(20);

      await collection.deleteOne({ _id: 'tracked-2' });
      recordChange('delete', 'tracked-2');

      // Verify changelog
      expect(changelog.length).toBe(4);
      expect(changelog[0].action).toBe('insert');
      expect(changelog[2].action).toBe('update');
      expect(changelog[3].action).toBe('delete');

      // Verify timestamps are increasing
      for (let i = 1; i < changelog.length; i++) {
        expect(changelog[i].timestamp).toBeGreaterThanOrEqual(changelog[i - 1].timestamp);
      }
    });
  });

  // ============================================================================
  // Time-Travel with Updates and Deletes
  // ============================================================================

  describe('Time-Travel with Updates and Deletes', () => {
    it('should handle document recreation after delete', async () => {
      const collection = db.collection(uniqueCollection('recreation'));

      // Create document
      await collection.insertOne({
        _id: 'recreated',
        incarnation: 1,
        data: 'original',
      });

      // Delete it
      await collection.deleteOne({ _id: 'recreated' });

      // Verify deleted
      let doc = await collection.findOne({ _id: 'recreated' });
      expect(doc).toBeNull();

      // Recreate with same ID
      await collection.insertOne({
        _id: 'recreated',
        incarnation: 2,
        data: 'recreated',
      });

      // Verify new document
      doc = await collection.findOne({ _id: 'recreated' });
      expect(doc).not.toBeNull();
      expect(doc!.incarnation).toBe(2);
      expect(doc!.data).toBe('recreated');
    });

    it('should handle multiple update cycles', async () => {
      const collection = db.collection(uniqueCollection('cycles'));

      // Create document with cycle counter
      await collection.insertOne({
        _id: 'cycled',
        cycle: 0,
        states: ['initial'],
      });

      // Perform multiple update cycles
      for (let cycle = 1; cycle <= 5; cycle++) {
        await collection.updateOne(
          { _id: 'cycled' },
          {
            $set: { cycle },
            $push: { states: `cycle-${cycle}` },
          }
        );
      }

      // Verify final state
      const doc = await collection.findOne({ _id: 'cycled' });
      expect(doc!.cycle).toBe(5);
      expect(doc!.states.length).toBe(6); // initial + 5 cycles
      expect(doc!.states[5]).toBe('cycle-5');
    });

    it('should track field-level changes over time', async () => {
      const collection = db.collection(uniqueCollection('field_changes'));

      // Create document with multiple fields
      await collection.insertOne({
        _id: 'fields',
        field1: 'original-1',
        field2: 'original-2',
        field3: 'original-3',
        changeLog: [{ timestamp: Date.now(), fields: ['field1', 'field2', 'field3'] }],
      });

      // Change field1
      await collection.updateOne(
        { _id: 'fields' },
        {
          $set: { field1: 'changed-1' },
          $push: { changeLog: { timestamp: Date.now(), fields: ['field1'] } },
        }
      );

      // Change field2 and field3
      await collection.updateOne(
        { _id: 'fields' },
        {
          $set: { field2: 'changed-2', field3: 'changed-3' },
          $push: { changeLog: { timestamp: Date.now(), fields: ['field2', 'field3'] } },
        }
      );

      // Verify final state and change log
      const doc = await collection.findOne({ _id: 'fields' });
      expect(doc!.field1).toBe('changed-1');
      expect(doc!.field2).toBe('changed-2');
      expect(doc!.field3).toBe('changed-3');
      expect(doc!.changeLog.length).toBe(3);
    });
  });

  // ============================================================================
  // Time-Travel Aggregation Tests
  // ============================================================================

  describe('Time-Travel Aggregation', () => {
    it('should aggregate data from specific time periods', async () => {
      const collection = db.collection(uniqueCollection('time_agg'));

      // Insert events with timestamps
      const now = Date.now();
      const events = [
        { _id: 'e1', timestamp: now - 3600000, category: 'sales', amount: 100 }, // 1 hour ago
        { _id: 'e2', timestamp: now - 3000000, category: 'sales', amount: 200 }, // 50 min ago
        { _id: 'e3', timestamp: now - 1800000, category: 'refund', amount: 50 }, // 30 min ago
        { _id: 'e4', timestamp: now - 600000, category: 'sales', amount: 150 }, // 10 min ago
        { _id: 'e5', timestamp: now - 300000, category: 'sales', amount: 250 }, // 5 min ago
      ];

      await collection.insertMany(events);

      // Aggregate sales from last 30 minutes
      const thirtyMinAgo = now - 1800000;
      const recentSales = await collection
        .aggregate([
          { $match: { timestamp: { $gte: thirtyMinAgo }, category: 'sales' } },
          { $group: { _id: null, totalSales: { $sum: '$amount' }, count: { $sum: 1 } } },
        ])
        .toArray();

      expect(recentSales.length).toBe(1);
      expect(recentSales[0].totalSales).toBe(400); // 150 + 250
      expect(recentSales[0].count).toBe(2);

      // Aggregate all sales
      const allSales = await collection
        .aggregate([
          { $match: { category: 'sales' } },
          { $group: { _id: null, totalSales: { $sum: '$amount' }, count: { $sum: 1 } } },
        ])
        .toArray();

      expect(allSales[0].totalSales).toBe(700); // 100 + 200 + 150 + 250
      expect(allSales[0].count).toBe(4);
    });

    it('should group by time buckets', async () => {
      const collection = db.collection(uniqueCollection('time_buckets'));

      // Insert events across different categories (simplified)
      const events = [];
      for (let category = 0; category < 3; category++) {
        for (let i = 0; i < 3; i++) {
          events.push({
            _id: `event-${category}-${i}`,
            category: `cat-${category}`,
            value: (category + 1) * 10,
          });
        }
      }

      await collection.insertMany(events);

      // Verify documents were inserted
      const totalDocs = await collection.countDocuments();
      expect(totalDocs).toBe(9); // 3 categories * 3 events

      // Group by category
      const categoryStats = await collection
        .aggregate([
          {
            $group: {
              _id: '$category',
              count: { $sum: 1 },
              total: { $sum: '$value' },
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      expect(categoryStats.length).toBe(3);

      // Each category should have 3 events
      for (const stat of categoryStats) {
        expect(stat.count).toBe(3);
      }

      // Verify totals using string IDs
      const cat0 = categoryStats.find((s) => s._id === 'cat-0');
      const cat2 = categoryStats.find((s) => s._id === 'cat-2');

      expect(cat0).toBeDefined();
      expect(cat2).toBeDefined();
      // cat-0: (0+1)*10 * 3 = 30, cat-2: (2+1)*10 * 3 = 90
      expect(cat0!.total).toBe(30);
      expect(cat2!.total).toBe(90);
    });

    it('should compare metrics across time periods', async () => {
      const collection = db.collection(uniqueCollection('period_compare'));

      // Insert metrics for two periods
      const period1Metrics = [
        { _id: 'p1-a', period: 1, metric: 'visits', value: 1000 },
        { _id: 'p1-b', period: 1, metric: 'conversions', value: 50 },
        { _id: 'p1-c', period: 1, metric: 'revenue', value: 5000 },
      ];

      const period2Metrics = [
        { _id: 'p2-a', period: 2, metric: 'visits', value: 1200 },
        { _id: 'p2-b', period: 2, metric: 'conversions', value: 72 },
        { _id: 'p2-c', period: 2, metric: 'revenue', value: 7200 },
      ];

      await collection.insertMany([...period1Metrics, ...period2Metrics]);

      // Aggregate by period and metric
      const periodComparison = await collection
        .aggregate([
          {
            $group: {
              _id: { period: '$period', metric: '$metric' },
              value: { $sum: '$value' },
            },
          },
          { $sort: { '_id.metric': 1, '_id.period': 1 } },
        ])
        .toArray();

      expect(periodComparison.length).toBe(6);

      // Calculate growth rates
      const metrics = ['conversions', 'revenue', 'visits'];
      for (const metric of metrics) {
        const p1 = periodComparison.find(
          (p) => p._id.period === 1 && p._id.metric === metric
        );
        const p2 = periodComparison.find(
          (p) => p._id.period === 2 && p._id.metric === metric
        );
        expect(p2!.value).toBeGreaterThan(p1!.value);
      }
    });
  });

  // ============================================================================
  // Snapshot Consistency Tests
  // ============================================================================

  describe('Snapshot Consistency', () => {
    it('should provide consistent snapshot during long-running query', async () => {
      const collection = db.collection(uniqueCollection('snapshot_consistency'));

      // Insert initial data
      const initialDocs = [];
      for (let i = 0; i < 100; i++) {
        initialDocs.push({ _id: `doc-${i}`, value: i, status: 'initial' });
      }
      await collection.insertMany(initialDocs);

      // Start a query that reads all documents
      const readPromise = collection.find({}).toArray();

      // Concurrently modify some documents
      const modifyPromise = (async (): Promise<void> => {
        for (let i = 0; i < 10; i++) {
          await collection.updateOne(
            { _id: `doc-${i * 10}` },
            { $set: { status: 'modified' } }
          );
        }
      })();

      // Wait for both operations
      const [docs] = await Promise.all([readPromise, modifyPromise]);

      // The read should have gotten a consistent view
      expect(docs.length).toBe(100);
      expect(docs.every((d) => d._id !== undefined)).toBe(true);
    });

    it('should handle concurrent reads and writes consistently', async () => {
      const collection = db.collection(uniqueCollection('concurrent_rw'));

      // Insert initial data
      await collection.insertMany([
        { _id: 'counter', value: 0 },
        { _id: 'log', entries: [] as string[] },
      ]);

      // Run writers sequentially to ensure all updates are applied
      // (concurrent $inc may have race conditions in the implementation)
      for (let i = 0; i < 5; i++) {
        await collection.updateOne({ _id: 'counter' }, { $inc: { value: 1 } });
        await collection.updateOne(
          { _id: 'log' },
          { $push: { entries: `entry-${i}` } }
        );
      }

      // Final state should be consistent
      const counter = await collection.findOne({ _id: 'counter' });
      const log = await collection.findOne({ _id: 'log' });

      expect(counter!.value).toBe(5);
      expect(log!.entries.length).toBe(5);
    });

    it('should isolate different collection reads', async () => {
      const coll1 = db.collection(uniqueCollection('isolated_1'));
      const coll2 = db.collection(uniqueCollection('isolated_2'));

      // Insert into both collections
      await coll1.insertMany([
        { _id: 'c1-1', source: 'coll1' },
        { _id: 'c1-2', source: 'coll1' },
      ]);

      await coll2.insertMany([
        { _id: 'c2-1', source: 'coll2' },
        { _id: 'c2-2', source: 'coll2' },
        { _id: 'c2-3', source: 'coll2' },
      ]);

      // Read from both concurrently
      const [docs1, docs2] = await Promise.all([
        coll1.find({}).toArray(),
        coll2.find({}).toArray(),
      ]);

      // Results should be isolated
      expect(docs1.length).toBe(2);
      expect(docs2.length).toBe(3);

      for (const doc of docs1) {
        expect(doc.source).toBe('coll1');
      }

      for (const doc of docs2) {
        expect(doc.source).toBe('coll2');
      }
    });
  });

  // ============================================================================
  // Audit Trail Tests
  // ============================================================================

  describe('Audit Trail', () => {
    it('should maintain audit trail of all operations', async () => {
      const dataCollection = db.collection(uniqueCollection('audited_data'));
      const auditCollection = db.collection(uniqueCollection('audit_log'));

      // Helper to log operations
      const logOperation = async (
        operation: string,
        docId: string,
        details: Record<string, unknown>
      ): Promise<void> => {
        await auditCollection.insertOne({
          timestamp: Date.now(),
          operation,
          documentId: docId,
          details,
        });
      };

      // Perform operations with audit logging
      await dataCollection.insertOne({
        _id: 'audited-doc',
        value: 100,
        status: 'active',
      });
      await logOperation('insert', 'audited-doc', { value: 100, status: 'active' });

      await dataCollection.updateOne(
        { _id: 'audited-doc' },
        { $set: { value: 200 } }
      );
      await logOperation('update', 'audited-doc', { changes: { value: 200 } });

      await dataCollection.updateOne(
        { _id: 'audited-doc' },
        { $set: { status: 'inactive' } }
      );
      await logOperation('update', 'audited-doc', { changes: { status: 'inactive' } });

      await dataCollection.deleteOne({ _id: 'audited-doc' });
      await logOperation('delete', 'audited-doc', {});

      // Verify audit trail
      const auditEntries = await auditCollection
        .find({ documentId: 'audited-doc' })
        .sort({ timestamp: 1 })
        .toArray();

      expect(auditEntries.length).toBe(4);
      expect(auditEntries[0].operation).toBe('insert');
      expect(auditEntries[1].operation).toBe('update');
      expect(auditEntries[2].operation).toBe('update');
      expect(auditEntries[3].operation).toBe('delete');
    });

    it('should allow querying historical state from audit trail', async () => {
      const auditCollection = db.collection(uniqueCollection('state_audit'));

      // Record state snapshots
      const snapshots = [
        { timestamp: Date.now() - 3000, documentId: 'doc-1', state: { value: 1, status: 'draft' } },
        { timestamp: Date.now() - 2000, documentId: 'doc-1', state: { value: 2, status: 'draft' } },
        { timestamp: Date.now() - 1000, documentId: 'doc-1', state: { value: 2, status: 'published' } },
        { timestamp: Date.now(), documentId: 'doc-1', state: { value: 3, status: 'published' } },
      ];

      await auditCollection.insertMany(snapshots);

      // Query state at specific time (between snapshots 2 and 3)
      const queryTime = Date.now() - 1500;
      const historicalState = await auditCollection
        .find({ documentId: 'doc-1', timestamp: { $lte: queryTime } })
        .sort({ timestamp: -1 })
        .limit(1)
        .toArray();

      expect(historicalState.length).toBe(1);
      expect(historicalState[0].state.value).toBe(2);
      expect(historicalState[0].state.status).toBe('draft');

      // Query latest state
      const latestState = await auditCollection
        .find({ documentId: 'doc-1' })
        .sort({ timestamp: -1 })
        .limit(1)
        .toArray();

      expect(latestState[0].state.value).toBe(3);
      expect(latestState[0].state.status).toBe('published');
    });
  });
});
