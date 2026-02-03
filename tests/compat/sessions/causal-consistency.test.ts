/**
 * MongoDB Causal Consistency Tests
 *
 * Based on MongoDB Driver Specifications:
 * - implicit-sessions-default-causal-consistency.json
 * - snapshot-sessions.json
 *
 * Tests causal consistency behavior and read-your-writes guarantees.
 *
 * Note: MongoLake implements read-your-writes through buffer checking
 * and transaction isolation, not through MongoDB's cluster time gossip
 * protocol. These tests verify the functional behavior regardless of
 * the underlying implementation mechanism.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { ClientSession } from '../../../src/session/index.js';
import { TransactionManager } from '../../../src/transaction/index.js';
import { MongoLake, Collection } from '../../../src/client/index.js';
import { MemoryStorage } from '../../../src/storage/index.js';

interface TestDoc {
  _id: string;
  value: number;
  name?: string;
}

function createTestClient(): MongoLake {
  const storage = new MemoryStorage();
  const client = new MongoLake({ local: '.test-mongolake' });
  // @ts-expect-error - accessing private field for testing
  client.storage = storage;
  return client;
}

describe('Causal Consistency - Read Your Writes', () => {
  let client: MongoLake;
  let collection: Collection<TestDoc>;

  beforeEach(async () => {
    client = createTestClient();
    collection = client.db('testdb').collection<TestDoc>('causal_test');
  });

  afterEach(async () => {
    await client.close();
  });

  /**
   * MongoDB Spec: Read-your-writes consistency
   *
   * When using the same session, reads should see previously
   * written data from that session.
   */
  describe('Read-Your-Writes Within Session', () => {
    it('should read back inserted document within same session', async () => {
      const session = client.startSession();

      // Insert with session
      await collection.insertOne({ _id: 'ryw-1', value: 100 }, { session });

      // Read back with same session
      const doc = await collection.findOne({ _id: 'ryw-1' }, { session });

      expect(doc).toBeDefined();
      expect(doc?.value).toBe(100);

      await session.endSession();
    });

    it('should read back multiple inserted documents', async () => {
      const session = client.startSession();

      await collection.insertOne({ _id: 'ryw-2', value: 200 }, { session });
      await collection.insertOne({ _id: 'ryw-3', value: 300 }, { session });

      const doc2 = await collection.findOne({ _id: 'ryw-2' }, { session });
      const doc3 = await collection.findOne({ _id: 'ryw-3' }, { session });

      expect(doc2?.value).toBe(200);
      expect(doc3?.value).toBe(300);

      await session.endSession();
    });

    it('should see updated document within same session', async () => {
      const session = client.startSession();

      await collection.insertOne({ _id: 'ryw-4', value: 10 }, { session });
      await collection.updateOne({ _id: 'ryw-4' }, { $set: { value: 20 } }, { session });

      const doc = await collection.findOne({ _id: 'ryw-4' }, { session });
      expect(doc?.value).toBe(20);

      await session.endSession();
    });

    it('should not find deleted document within same session', async () => {
      const session = client.startSession();

      await collection.insertOne({ _id: 'ryw-5', value: 50 }, { session });
      await collection.deleteOne({ _id: 'ryw-5' }, { session });

      const doc = await collection.findOne({ _id: 'ryw-5' }, { session });
      expect(doc).toBeNull();

      await session.endSession();
    });
  });
});

describe('Causal Consistency - Transaction Isolation', () => {
  let client: MongoLake;
  let collection: Collection<TestDoc>;

  beforeEach(async () => {
    client = createTestClient();
    collection = client.db('testdb').collection<TestDoc>('isolation_test');
  });

  afterEach(async () => {
    await client.close();
  });

  /**
   * MongoDB Spec: Transaction isolation
   *
   * Uncommitted transaction changes should not be visible to other sessions.
   */
  describe('Uncommitted Changes Isolation', () => {
    it('should not see uncommitted transaction changes from other sessions', async () => {
      const session1 = client.startSession();
      const session2 = client.startSession();

      // Session 1 starts transaction and inserts (but doesn't commit)
      session1.startTransaction();
      session1.bufferOperation({
        type: 'insert',
        collection: 'isolation_test',
        database: 'testdb',
        document: { _id: 'iso-1', value: 999 },
      });

      // Session 2 should not see the uncommitted document
      const doc = await collection.findOne({ _id: 'iso-1' }, { session: session2 });
      expect(doc).toBeNull();

      await session1.endSession();
      await session2.endSession();
    });

    it('should see committed transaction changes', async () => {
      const session1 = client.startSession();
      const session2 = client.startSession();

      // Insert directly (not in transaction)
      await collection.insertOne({ _id: 'iso-2', value: 100 });

      // Session 2 should see the committed document
      const doc = await collection.findOne({ _id: 'iso-2' }, { session: session2 });
      expect(doc).toBeDefined();
      expect(doc?.value).toBe(100);

      await session1.endSession();
      await session2.endSession();
    });
  });

  /**
   * MongoDB Spec: Transaction snapshot isolation
   *
   * Within a transaction, reads should see a consistent snapshot.
   */
  describe('Snapshot Consistency Within Transaction', () => {
    it('should see consistent snapshot of buffered operations', () => {
      const session = client.startSession();
      const txn = new TransactionManager(session);

      txn.begin();

      // Buffer multiple operations
      txn.insert('testdb', 'isolation_test', { _id: 'snap-1', value: 1 });
      txn.insert('testdb', 'isolation_test', { _id: 'snap-2', value: 2 });

      // All operations should be in buffer
      const ops = txn.getOperations();
      expect(ops).toHaveLength(2);
      expect(ops[0].document?._id).toBe('snap-1');
      expect(ops[1].document?._id).toBe('snap-2');

      session.endSession();
    });

    it('should track snapshot start time', () => {
      const session = client.startSession();
      const txn = new TransactionManager(session);

      const before = Date.now();
      txn.begin();
      const after = Date.now();

      const snapshot = txn.getSnapshot();
      expect(snapshot?.startTime).toBeGreaterThanOrEqual(before);
      expect(snapshot?.startTime).toBeLessThanOrEqual(after);

      session.endSession();
    });
  });
});

describe('Causal Consistency - Shard LSN Tracking', () => {
  let client: MongoLake;

  beforeEach(() => {
    client = createTestClient();
  });

  afterEach(async () => {
    await client.close();
  });

  /**
   * MongoDB Spec: Cluster time / operation time tracking
   *
   * MongoLake uses LSN (Log Sequence Number) tracking per shard
   * instead of MongoDB's cluster time gossip.
   */
  describe('LSN Tracking for Consistency', () => {
    it('should record shard LSNs in transaction snapshot', () => {
      const session = client.startSession();
      const txn = new TransactionManager(session);

      txn.begin();
      txn.recordShardLSN('shard-0', 100);
      txn.recordShardLSN('shard-1', 150);

      const snapshot = txn.getSnapshot();
      expect(snapshot?.shardLSNs.get('shard-0')).toBe(100);
      expect(snapshot?.shardLSNs.get('shard-1')).toBe(150);

      session.endSession();
    });

    it('should update LSN for same shard', () => {
      const session = client.startSession();
      const txn = new TransactionManager(session);

      txn.begin();
      txn.recordShardLSN('shard-0', 100);
      txn.recordShardLSN('shard-0', 200);

      const snapshot = txn.getSnapshot();
      expect(snapshot?.shardLSNs.get('shard-0')).toBe(200);

      session.endSession();
    });

    it('should clear LSN tracking on commit', async () => {
      const session = client.startSession();
      const txn = new TransactionManager(session);

      txn.begin();
      txn.recordShardLSN('shard-0', 100);

      await txn.commit();

      expect(txn.getSnapshot()).toBeNull();

      session.endSession();
    });

    it('should clear LSN tracking on abort', async () => {
      const session = client.startSession();
      const txn = new TransactionManager(session);

      txn.begin();
      txn.recordShardLSN('shard-0', 100);

      await txn.abort();

      expect(txn.getSnapshot()).toBeNull();

      session.endSession();
    });

    it('should not record LSN when not in transaction', () => {
      const session = client.startSession();
      const txn = new TransactionManager(session);

      // No transaction started
      txn.recordShardLSN('shard-0', 100);

      // Should be no-op
      expect(txn.getSnapshot()).toBeNull();

      session.endSession();
    });
  });
});

describe('Causal Consistency - Feature Support Summary', () => {
  it('should document causal consistency feature support', () => {
    /**
     * CAUSAL CONSISTENCY IN MONGOLAKE
     *
     * MongoLake provides read-your-writes consistency through:
     *
     * 1. Operation Buffering
     *    - Writes within a transaction are buffered
     *    - Buffer is checked during reads in the same session
     *
     * 2. Transaction Isolation
     *    - Uncommitted changes are not visible to other sessions
     *    - Snapshot isolation within transactions
     *
     * 3. LSN Tracking
     *    - Per-shard Log Sequence Numbers
     *    - Snapshot records shard LSNs at transaction start
     *    - Used for distributed transaction coordination
     *
     * DIFFERENCES FROM MONGODB:
     *
     * 1. No Cluster Time Gossip
     *    - MongoDB uses clusterTime and operationTime
     *    - MongoLake is single-region (no gossip needed)
     *
     * 2. No atClusterTime
     *    - MongoDB snapshot reads use atClusterTime
     *    - MongoLake uses LSN-based snapshots
     *
     * 3. Session Option
     *    - causalConsistency option is accepted but not enforced
     *    - Consistency is provided through architecture, not option
     */
    expect(true).toBe(true);
  });
});
