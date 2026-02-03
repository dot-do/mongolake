/**
 * TransactionManager Unit Tests
 *
 * Tests for the TransactionManager class that coordinates
 * multi-document ACID transactions.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  MongoLake,
  Collection,
} from '../../../src/client/index.js';
import {
  TransactionManager,
  runTransaction,
  TransactionError,
} from '../../../src/transaction/index.js';
import { ClientSession } from '../../../src/session/index.js';
import { MemoryStorage } from '../../../src/storage/index.js';

// ============================================================================
// Test Helpers
// ============================================================================

function createTestClient(): MongoLake {
  const storage = new MemoryStorage();
  const client = new MongoLake({ local: '.test-mongolake' });
  // @ts-expect-error - accessing private field for testing
  client.storage = storage;
  return client;
}

interface TestDoc {
  _id?: string;
  name: string;
  balance?: number;
  [key: string]: unknown;
}

// ============================================================================
// TransactionManager Basic Tests
// ============================================================================

describe('TransactionManager - Basic Operations', () => {
  let client: MongoLake;
  let session: ClientSession;
  let txn: TransactionManager;

  beforeEach(() => {
    client = createTestClient();
    session = client.startSession();
    txn = new TransactionManager(session);
  });

  afterEach(async () => {
    if (!session.hasEnded) {
      await session.endSession();
    }
    await client.close();
  });

  it('should begin a transaction', () => {
    txn.begin();
    expect(txn.inTransaction).toBe(true);
  });

  it('should begin with transaction options', () => {
    txn.begin({
      readConcern: { level: 'snapshot' },
      writeConcern: { w: 'majority' },
    });
    expect(txn.inTransaction).toBe(true);
  });

  it('should capture snapshot on begin', () => {
    txn.begin();
    const snapshot = txn.getSnapshot();

    expect(snapshot).not.toBeNull();
    expect(snapshot?.startTime).toBeLessThanOrEqual(Date.now());
  });

  it('should commit a transaction', async () => {
    txn.begin();
    txn.insert('testdb', 'users', { _id: '1', name: 'Test' });

    const result = await txn.commit();

    expect(result.success).toBe(true);
    expect(result.operationCount).toBe(1);
    expect(txn.inTransaction).toBe(false);
  });

  it('should abort a transaction', async () => {
    txn.begin();
    txn.insert('testdb', 'users', { _id: '1', name: 'Test' });

    await txn.abort();

    expect(txn.inTransaction).toBe(false);
    expect(txn.operationCount).toBe(0);
  });

  it('should clear snapshot on commit', async () => {
    txn.begin();
    expect(txn.getSnapshot()).not.toBeNull();

    await txn.commit();
    expect(txn.getSnapshot()).toBeNull();
  });

  it('should clear snapshot on abort', async () => {
    txn.begin();
    txn.insert('testdb', 'users', { _id: '1', name: 'Test' });
    expect(txn.getSnapshot()).not.toBeNull();

    await txn.abort();
    expect(txn.getSnapshot()).toBeNull();
  });
});

// ============================================================================
// TransactionManager Write Operations Tests
// ============================================================================

describe('TransactionManager - Write Operations', () => {
  let client: MongoLake;
  let session: ClientSession;
  let txn: TransactionManager;

  beforeEach(() => {
    client = createTestClient();
    session = client.startSession();
    txn = new TransactionManager(session);
  });

  afterEach(async () => {
    if (!session.hasEnded) {
      await session.endSession();
    }
    await client.close();
  });

  it('should buffer insert operations', () => {
    txn.begin();
    txn.insert('testdb', 'users', { _id: '1', name: 'Alice' });

    expect(txn.operationCount).toBe(1);

    const ops = txn.getOperations();
    expect(ops[0].type).toBe('insert');
    expect(ops[0].document?._id).toBe('1');
  });

  it('should buffer update operations', () => {
    txn.begin();
    txn.update('testdb', 'users', { _id: '1' }, { $set: { name: 'Bob' } });

    expect(txn.operationCount).toBe(1);

    const ops = txn.getOperations();
    expect(ops[0].type).toBe('update');
    expect(ops[0].filter?._id).toBe('1');
  });

  it('should buffer replace operations', () => {
    txn.begin();
    txn.replace('testdb', 'users', { _id: '1' }, { _id: '1', name: 'Charlie' });

    expect(txn.operationCount).toBe(1);

    const ops = txn.getOperations();
    expect(ops[0].type).toBe('replace');
  });

  it('should buffer delete operations', () => {
    txn.begin();
    txn.delete('testdb', 'users', { _id: '1' });

    expect(txn.operationCount).toBe(1);

    const ops = txn.getOperations();
    expect(ops[0].type).toBe('delete');
  });

  it('should buffer multiple operations', () => {
    txn.begin();
    txn.insert('testdb', 'users', { _id: '1', name: 'Alice' });
    txn.update('testdb', 'users', { _id: '1' }, { $set: { age: 30 } });
    txn.insert('testdb', 'orders', { _id: '1', userId: '1' });

    expect(txn.operationCount).toBe(3);
  });

  it('should preserve operation order', () => {
    txn.begin();
    txn.insert('testdb', 'users', { _id: '1', name: 'Alice' });
    txn.update('testdb', 'users', { _id: '1' }, { $set: { name: 'Updated' } });
    txn.delete('testdb', 'users', { _id: '1' });

    const ops = txn.getOperations();
    expect(ops[0].type).toBe('insert');
    expect(ops[1].type).toBe('update');
    expect(ops[2].type).toBe('delete');
  });

  it('should support operations across databases', () => {
    txn.begin();
    txn.insert('db1', 'users', { _id: '1', name: 'Alice' });
    txn.insert('db2', 'users', { _id: '1', name: 'Bob' });

    const ops = txn.getOperations();
    expect(ops[0].database).toBe('db1');
    expect(ops[1].database).toBe('db2');
  });
});

// ============================================================================
// TransactionManager Error Handling Tests
// ============================================================================

describe('TransactionManager - Error Handling', () => {
  let client: MongoLake;
  let session: ClientSession;
  let txn: TransactionManager;

  beforeEach(() => {
    client = createTestClient();
    session = client.startSession();
    txn = new TransactionManager(session);
  });

  afterEach(async () => {
    if (!session.hasEnded) {
      await session.endSession();
    }
    await client.close();
  });

  it('should throw when committing without transaction', async () => {
    await expect(txn.commit()).rejects.toThrow(TransactionError);
  });

  it('should throw when aborting without transaction', async () => {
    await expect(txn.abort()).rejects.toThrow(TransactionError);
  });

  it('should throw when writing without transaction', () => {
    expect(() => {
      txn.insert('testdb', 'users', { _id: '1', name: 'Test' });
    }).toThrow(TransactionError);
  });

  it('should throw when beginning nested transaction', () => {
    txn.begin();
    expect(() => txn.begin()).toThrow(TransactionError);
  });
});

// ============================================================================
// runTransaction Helper Tests
// ============================================================================

describe('runTransaction Helper', () => {
  let client: MongoLake;
  let collection: Collection<TestDoc>;

  beforeEach(async () => {
    client = createTestClient();
    collection = client.db('testdb').collection<TestDoc>('accounts');
    await collection.insertMany([
      { _id: 'acc1', name: 'Account 1', balance: 1000 },
      { _id: 'acc2', name: 'Account 2', balance: 500 },
    ]);
  });

  afterEach(async () => {
    await client.close();
  });

  it('should execute callback within transaction', async () => {
    const session = client.startSession();
    let callbackExecuted = false;

    try {
      await runTransaction(session, async (txn) => {
        callbackExecuted = true;
        txn.insert('testdb', 'accounts', { _id: 'acc3', name: 'New', balance: 100 });
        return 'done';
      });

      expect(callbackExecuted).toBe(true);
    } finally {
      await session.endSession();
    }
  });

  it('should return callback result', async () => {
    const session = client.startSession();

    try {
      const result = await runTransaction(session, async (txn) => {
        txn.insert('testdb', 'accounts', { _id: 'acc3', name: 'New', balance: 100 });
        return { status: 'success', count: 1 };
      });

      expect(result.status).toBe('success');
      expect(result.count).toBe(1);
    } finally {
      await session.endSession();
    }
  });

  it('should commit on success', async () => {
    const session = client.startSession();

    try {
      await runTransaction(session, async (txn) => {
        txn.insert('testdb', 'accounts', { _id: 'acc3', name: 'New', balance: 100 });
      });

      // Verify document was committed
      const acc3 = await collection.findOne({ _id: 'acc3' });
      expect(acc3).toBeDefined();
    } finally {
      await session.endSession();
    }
  });

  it('should abort on callback error', async () => {
    const session = client.startSession();

    try {
      await runTransaction(session, async (txn) => {
        txn.insert('testdb', 'accounts', { _id: 'acc3', name: 'New', balance: 100 });
        throw new Error('Simulated failure');
      });
    } catch (error) {
      expect((error as Error).message).toBe('Simulated failure');
    } finally {
      await session.endSession();
    }

    // Document should not exist
    const acc3 = await collection.findOne({ _id: 'acc3' });
    expect(acc3).toBeNull();
  });

  it('should support transaction options', async () => {
    const session = client.startSession();

    try {
      await runTransaction(
        session,
        async (txn) => {
          txn.insert('testdb', 'accounts', { _id: 'acc3', name: 'New', balance: 100 });
        },
        {
          transactionOptions: {
            readConcern: { level: 'snapshot' },
            writeConcern: { w: 'majority' },
          },
        }
      );
    } finally {
      await session.endSession();
    }
  });

  it('should retry on transient error', async () => {
    const session = client.startSession();
    let attempts = 0;

    try {
      await runTransaction(
        session,
        async (txn) => {
          attempts++;
          if (attempts < 2) {
            throw new Error('Write conflict - please retry');
          }
          txn.insert('testdb', 'accounts', { _id: 'acc3', name: 'New', balance: 100 });
        },
        { maxRetries: 3 }
      );

      expect(attempts).toBe(2);
    } finally {
      await session.endSession();
    }
  });

  it('should give up after max retries', async () => {
    const session = client.startSession();
    let attempts = 0;

    try {
      await expect(
        runTransaction(
          session,
          async () => {
            attempts++;
            throw new Error('Write conflict - please retry');
          },
          { maxRetries: 2, retryDelayMs: 1 }
        )
      ).rejects.toThrow('Write conflict');

      // Should attempt: initial + 2 retries = 3 total
      expect(attempts).toBe(3);
    } finally {
      await session.endSession();
    }
  });
});

// ============================================================================
// Snapshot LSN Tracking Tests
// ============================================================================

describe('TransactionManager - Snapshot LSN Tracking', () => {
  let client: MongoLake;
  let session: ClientSession;
  let txn: TransactionManager;

  beforeEach(() => {
    client = createTestClient();
    session = client.startSession();
    txn = new TransactionManager(session);
  });

  afterEach(async () => {
    if (!session.hasEnded) {
      await session.endSession();
    }
    await client.close();
  });

  it('should record shard LSNs in snapshot', () => {
    txn.begin();
    txn.recordShardLSN('shard-1', 100);
    txn.recordShardLSN('shard-2', 200);

    const snapshot = txn.getSnapshot();
    expect(snapshot?.shardLSNs.get('shard-1')).toBe(100);
    expect(snapshot?.shardLSNs.get('shard-2')).toBe(200);
  });

  it('should initialize empty shardLSNs map', () => {
    txn.begin();
    const snapshot = txn.getSnapshot();

    expect(snapshot?.shardLSNs).toBeInstanceOf(Map);
    expect(snapshot?.shardLSNs.size).toBe(0);
  });

  it('should not record LSN when not in transaction', () => {
    // Should not throw, just be a no-op
    txn.recordShardLSN('shard-1', 100);
    expect(txn.getSnapshot()).toBeNull();
  });
});
