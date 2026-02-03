/**
 * Session and Transaction Integration Tests
 *
 * Tests session and transaction workflows including:
 * - Session lifecycle management
 * - Transaction commit and abort
 * - Operation buffering during transactions
 * - Multi-document atomic operations
 * - Concurrent transaction handling
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  ClientSession,
  SessionStore,
  TransactionError,
  SessionError,
  generateSessionId,
} from '../../src/session/index.js';
import {
  TransactionManager,
  runTransaction,
} from '../../src/transaction/index.js';
import { MongoLake, Collection } from '../../src/client/index.js';
import { MemoryStorage } from '../../src/storage/index.js';
import { resetDocumentCounter } from '../utils/factories.js';

// ============================================================================
// Test Types
// ============================================================================

interface AccountDocument {
  _id: string;
  name: string;
  balance: number;
  status?: string;
  lastTransaction?: Date;
}

interface OrderDocument {
  _id: string;
  customerId: string;
  items: Array<{ productId: string; quantity: number; price: number }>;
  total: number;
  status: 'pending' | 'confirmed' | 'shipped' | 'cancelled';
}

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

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ============================================================================
// Session Lifecycle Tests
// ============================================================================

describe('Session - Lifecycle Management', () => {
  let client: MongoLake;

  beforeEach(() => {
    resetDocumentCounter();
    client = createTestClient();
  });

  afterEach(async () => {
    await client.close();
  });

  it('should create a new session with unique ID', () => {
    const session = client.startSession();

    expect(session).toBeInstanceOf(ClientSession);
    expect(session.id).toBeDefined();
    expect(session.id.length).toBeGreaterThan(0);
    expect(session.hasEnded).toBe(false);
  });

  it('should create multiple sessions with unique IDs', () => {
    const session1 = client.startSession();
    const session2 = client.startSession();
    const session3 = client.startSession();

    expect(session1.id).not.toBe(session2.id);
    expect(session2.id).not.toBe(session3.id);
    expect(session1.id).not.toBe(session3.id);
  });

  it('should end session gracefully', async () => {
    const session = client.startSession();

    expect(session.hasEnded).toBe(false);

    await session.endSession();

    expect(session.hasEnded).toBe(true);
  });

  it('should allow multiple end session calls', async () => {
    const session = client.startSession();

    await session.endSession();
    await session.endSession(); // Should not throw
    await session.endSession();

    expect(session.hasEnded).toBe(true);
  });

  it('should abort active transaction on end session', async () => {
    const session = client.startSession();
    session.startTransaction();

    expect(session.inTransaction).toBe(true);

    await session.endSession();

    expect(session.hasEnded).toBe(true);
    expect(session.transactionState).toBe('aborted');
  });

  it('should track session creation timestamp', () => {
    const before = new Date();
    const session = client.startSession();
    const after = new Date();

    expect(session.createdAt.getTime()).toBeGreaterThanOrEqual(before.getTime());
    expect(session.createdAt.getTime()).toBeLessThanOrEqual(after.getTime());
  });

  it('should update lastUsed timestamp on operations', async () => {
    const session = client.startSession();
    const initialLastUsed = session.lastUsed;

    await delay(10);
    session.startTransaction();

    expect(session.lastUsed.getTime()).toBeGreaterThan(initialLastUsed.getTime());
  });
});

// ============================================================================
// Session Store Tests
// ============================================================================

describe('Session - Session Store Management', () => {
  let store: SessionStore;

  beforeEach(() => {
    resetDocumentCounter();
    store = new SessionStore({ timeoutMs: 1000, cleanupIntervalMs: 0 });
  });

  afterEach(async () => {
    await store.closeAll();
  });

  it('should add and retrieve sessions', () => {
    const session = new ClientSession();
    store.add(session);

    const retrieved = store.get(session.id);

    expect(retrieved).toBe(session);
  });

  it('should track session count', () => {
    expect(store.size).toBe(0);

    store.add(new ClientSession());
    expect(store.size).toBe(1);

    store.add(new ClientSession());
    expect(store.size).toBe(2);

    store.add(new ClientSession());
    expect(store.size).toBe(3);
  });

  it('should remove sessions', () => {
    const session = new ClientSession();
    store.add(session);

    expect(store.has(session.id)).toBe(true);

    const removed = store.remove(session.id);

    expect(removed).toBe(true);
    expect(store.has(session.id)).toBe(false);
  });

  it('should return false when removing non-existent session', () => {
    const removed = store.remove('non-existent-id');
    expect(removed).toBe(false);
  });

  it('should cleanup expired sessions', async () => {
    const expiredStore = new SessionStore({ timeoutMs: 10, cleanupIntervalMs: 0 });

    const session = new ClientSession();
    expiredStore.add(session);

    expect(expiredStore.size).toBe(1);

    await delay(20);

    const cleaned = expiredStore.cleanupExpired();

    expect(cleaned).toBe(1);
    expect(expiredStore.size).toBe(0);

    await expiredStore.closeAll();
  });

  it('should close all sessions', async () => {
    const session1 = new ClientSession();
    const session2 = new ClientSession();
    const session3 = new ClientSession();

    store.add(session1);
    store.add(session2);
    store.add(session3);

    expect(store.size).toBe(3);

    await store.closeAll();

    expect(store.size).toBe(0);
    expect(session1.hasEnded).toBe(true);
    expect(session2.hasEnded).toBe(true);
    expect(session3.hasEnded).toBe(true);
  });

  it('should list all session IDs', () => {
    const session1 = new ClientSession();
    const session2 = new ClientSession();

    store.add(session1);
    store.add(session2);

    const ids = store.getSessionIds();

    expect(ids).toContain(session1.id);
    expect(ids).toContain(session2.id);
  });
});

// ============================================================================
// Transaction Basic Operations Tests
// ============================================================================

describe('Transaction - Basic Operations', () => {
  let client: MongoLake;
  let session: ClientSession;
  let txn: TransactionManager;

  beforeEach(() => {
    resetDocumentCounter();
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
    expect(session.transactionState).toBe('starting');
  });

  it('should begin transaction with options', () => {
    txn.begin({
      readConcern: { level: 'snapshot' },
      writeConcern: { w: 'majority' },
    });

    expect(txn.inTransaction).toBe(true);
    expect(session.transactionOptions).toBeDefined();
    expect(session.transactionOptions?.readConcern?.level).toBe('snapshot');
  });

  it('should capture snapshot on begin', () => {
    txn.begin();

    const snapshot = txn.getSnapshot();

    expect(snapshot).not.toBeNull();
    expect(snapshot?.startTime).toBeLessThanOrEqual(Date.now());
    expect(snapshot?.shardLSNs).toBeInstanceOf(Map);
  });

  it('should commit transaction', async () => {
    txn.begin();
    txn.insert('testdb', 'users', { _id: '1', name: 'Test' });

    const result = await txn.commit();

    expect(result.success).toBe(true);
    expect(result.operationCount).toBe(1);
    expect(txn.inTransaction).toBe(false);
  });

  it('should abort transaction', async () => {
    txn.begin();
    txn.insert('testdb', 'users', { _id: '1', name: 'Test' });

    await txn.abort();

    expect(txn.inTransaction).toBe(false);
    expect(txn.operationCount).toBe(0);
    expect(session.transactionState).toBe('aborted');
  });

  it('should clear snapshot after commit', async () => {
    txn.begin();
    expect(txn.getSnapshot()).not.toBeNull();

    await txn.commit();

    expect(txn.getSnapshot()).toBeNull();
  });

  it('should clear snapshot after abort', async () => {
    txn.begin();
    txn.insert('testdb', 'users', { _id: '1', name: 'Test' });
    expect(txn.getSnapshot()).not.toBeNull();

    await txn.abort();

    expect(txn.getSnapshot()).toBeNull();
  });
});

// ============================================================================
// Operation Buffering Tests
// ============================================================================

describe('Transaction - Operation Buffering', () => {
  let client: MongoLake;
  let session: ClientSession;
  let txn: TransactionManager;

  beforeEach(() => {
    resetDocumentCounter();
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
    txn.insert('testdb', 'users', { _id: '1', name: 'Alice', balance: 100 });

    expect(txn.operationCount).toBe(1);

    const ops = txn.getOperations();
    expect(ops[0].type).toBe('insert');
    expect(ops[0].database).toBe('testdb');
    expect(ops[0].collection).toBe('users');
    expect(ops[0].document?._id).toBe('1');
  });

  it('should buffer update operations', () => {
    txn.begin();
    txn.update('testdb', 'users', { _id: '1' }, { $set: { name: 'Bob' } });

    expect(txn.operationCount).toBe(1);

    const ops = txn.getOperations();
    expect(ops[0].type).toBe('update');
    expect(ops[0].filter?._id).toBe('1');
    expect(ops[0].update?.$set).toEqual({ name: 'Bob' });
  });

  it('should buffer replace operations', () => {
    txn.begin();
    txn.replace('testdb', 'users', { _id: '1' }, { _id: '1', name: 'Charlie', balance: 200 });

    expect(txn.operationCount).toBe(1);

    const ops = txn.getOperations();
    expect(ops[0].type).toBe('replace');
    expect(ops[0].replacement?._id).toBe('1');
  });

  it('should buffer delete operations', () => {
    txn.begin();
    txn.delete('testdb', 'users', { _id: '1' });

    expect(txn.operationCount).toBe(1);

    const ops = txn.getOperations();
    expect(ops[0].type).toBe('delete');
    expect(ops[0].filter?._id).toBe('1');
  });

  it('should buffer multiple operations', () => {
    txn.begin();
    txn.insert('testdb', 'users', { _id: '1', name: 'Alice', balance: 1000 });
    txn.update('testdb', 'users', { _id: '1' }, { $inc: { balance: -100 } });
    txn.insert('testdb', 'orders', { _id: 'order-1', userId: '1', total: 100 });

    expect(txn.operationCount).toBe(3);
  });

  it('should preserve operation order', () => {
    txn.begin();
    txn.insert('testdb', 'users', { _id: '1', name: 'Alice', balance: 1000 });
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

  it('should clear buffered operations on abort', async () => {
    txn.begin();
    txn.insert('testdb', 'users', { _id: '1', name: 'Test' });
    txn.insert('testdb', 'users', { _id: '2', name: 'Test 2' });

    expect(txn.operationCount).toBe(2);

    await txn.abort();

    expect(txn.operationCount).toBe(0);
  });
});

// ============================================================================
// Transaction Error Handling Tests
// ============================================================================

describe('Transaction - Error Handling', () => {
  let client: MongoLake;
  let session: ClientSession;
  let txn: TransactionManager;

  beforeEach(() => {
    resetDocumentCounter();
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

  it('should throw when using ended session', async () => {
    await session.endSession();

    expect(() => {
      session.startTransaction();
    }).toThrow(SessionError);
  });

  it('should throw when buffering operation on ended session', async () => {
    txn.begin();
    await session.endSession();

    expect(() => {
      session.bufferOperation({ type: 'insert', collection: 'test', database: 'test' });
    }).toThrow(SessionError);
  });
});

// ============================================================================
// runTransaction Helper Tests
// ============================================================================

describe('Transaction - runTransaction Helper', () => {
  let client: MongoLake;
  let accounts: Collection<AccountDocument>;

  beforeEach(async () => {
    resetDocumentCounter();
    client = createTestClient();
    accounts = client.db('bank').collection<AccountDocument>('accounts');

    // Setup initial accounts
    await accounts.insertMany([
      { _id: 'acc1', name: 'Account 1', balance: 1000 },
      { _id: 'acc2', name: 'Account 2', balance: 500 },
      { _id: 'acc3', name: 'Account 3', balance: 750 },
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
        txn.insert('bank', 'accounts', { _id: 'acc4', name: 'New Account', balance: 100 });
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
        txn.insert('bank', 'accounts', { _id: 'acc4', name: 'New', balance: 100 });
        return { status: 'success', newAccountId: 'acc4' };
      });

      expect(result.status).toBe('success');
      expect(result.newAccountId).toBe('acc4');
    } finally {
      await session.endSession();
    }
  });

  it('should commit on success', async () => {
    const session = client.startSession();

    try {
      await runTransaction(session, async (txn) => {
        txn.insert('bank', 'accounts', { _id: 'acc5', name: 'Committed', balance: 200 });
      });

      // Verify committed
      const acc5 = await accounts.findOne({ _id: 'acc5' });
      expect(acc5).toBeDefined();
    } finally {
      await session.endSession();
    }
  });

  it('should abort on callback error', async () => {
    const session = client.startSession();

    try {
      await runTransaction(session, async (txn) => {
        txn.insert('bank', 'accounts', { _id: 'acc6', name: 'Should Not Exist', balance: 100 });
        throw new Error('Simulated failure');
      });
    } catch (error) {
      expect((error as Error).message).toBe('Simulated failure');
    } finally {
      await session.endSession();
    }

    // Document should not exist
    const acc6 = await accounts.findOne({ _id: 'acc6' });
    expect(acc6).toBeNull();
  });

  it('should support custom transaction options', async () => {
    const session = client.startSession();

    try {
      await runTransaction(
        session,
        async (txn) => {
          txn.insert('bank', 'accounts', { _id: 'acc7', name: 'Options Test', balance: 100 });
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

  it('should retry on transient errors', async () => {
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
          txn.insert('bank', 'accounts', { _id: 'acc8', name: 'Retry Success', balance: 100 });
        },
        { maxRetries: 3, retryDelayMs: 1 }
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
// Multi-Document Transaction Tests
// ============================================================================

describe('Transaction - Multi-Document Operations', () => {
  let client: MongoLake;
  let accounts: Collection<AccountDocument>;
  let orders: Collection<OrderDocument>;

  beforeEach(async () => {
    resetDocumentCounter();
    client = createTestClient();
    accounts = client.db('bank').collection<AccountDocument>('accounts');
    orders = client.db('bank').collection<OrderDocument>('orders');

    await accounts.insertMany([
      { _id: 'buyer', name: 'Buyer Account', balance: 1000 },
      { _id: 'seller', name: 'Seller Account', balance: 500 },
    ]);
  });

  afterEach(async () => {
    await client.close();
  });

  it('should handle multi-document transfer atomically', async () => {
    const session = client.startSession();

    try {
      await runTransaction(session, async (txn) => {
        const transferAmount = 200;

        // Debit buyer
        txn.update('bank', 'accounts', { _id: 'buyer' }, { $inc: { balance: -transferAmount } });

        // Credit seller
        txn.update('bank', 'accounts', { _id: 'seller' }, { $inc: { balance: transferAmount } });

        // Create order record
        txn.insert('bank', 'orders', {
          _id: 'order-1',
          customerId: 'buyer',
          items: [{ productId: 'prod-1', quantity: 1, price: 200 }],
          total: transferAmount,
          status: 'confirmed',
        });

        return { transferred: transferAmount };
      });

      // Verify atomic updates
      const buyer = await accounts.findOne({ _id: 'buyer' });
      const seller = await accounts.findOne({ _id: 'seller' });

      expect(buyer?.balance).toBe(800);
      expect(seller?.balance).toBe(700);
    } finally {
      await session.endSession();
    }
  });

  it('should rollback all operations on failure', async () => {
    const session = client.startSession();

    try {
      await runTransaction(session, async (txn) => {
        // Debit buyer
        txn.update('bank', 'accounts', { _id: 'buyer' }, { $inc: { balance: -500 } });

        // Create order
        txn.insert('bank', 'orders', {
          _id: 'order-fail',
          customerId: 'buyer',
          items: [{ productId: 'prod-1', quantity: 1, price: 500 }],
          total: 500,
          status: 'confirmed',
        });

        // Simulate validation failure
        throw new Error('Insufficient inventory');
      });
    } catch {
      // Expected error
    } finally {
      await session.endSession();
    }

    // Verify rollback - balance should be unchanged
    const buyer = await accounts.findOne({ _id: 'buyer' });
    expect(buyer?.balance).toBe(1000);

    // Order should not exist
    const order = await orders.findOne({ _id: 'order-fail' });
    expect(order).toBeNull();
  });

  it('should handle cross-collection operations', async () => {
    const session = client.startSession();

    try {
      await runTransaction(session, async (txn) => {
        // Update account
        txn.update('bank', 'accounts', { _id: 'buyer' }, { $set: { status: 'premium' } });

        // Create multiple orders
        for (let i = 0; i < 3; i++) {
          txn.insert('bank', 'orders', {
            _id: `batch-order-${i}`,
            customerId: 'buyer',
            items: [{ productId: `prod-${i}`, quantity: 1, price: 50 }],
            total: 50,
            status: 'confirmed',
          });
        }

        // Update running total
        txn.update('bank', 'accounts', { _id: 'buyer' }, { $inc: { balance: -150 } });
      });

      const buyer = await accounts.findOne({ _id: 'buyer' });
      expect(buyer?.balance).toBe(850);
      expect(buyer?.status).toBe('premium');
    } finally {
      await session.endSession();
    }
  });
});

// ============================================================================
// Concurrent Transaction Tests
// ============================================================================

describe('Transaction - Concurrent Handling', () => {
  let client: MongoLake;

  beforeEach(() => {
    resetDocumentCounter();
    client = createTestClient();
  });

  afterEach(async () => {
    await client.close();
  });

  it('should handle concurrent sessions independently', async () => {
    const session1 = client.startSession();
    const session2 = client.startSession();

    const txn1 = new TransactionManager(session1);
    const txn2 = new TransactionManager(session2);

    txn1.begin();
    txn2.begin();

    txn1.insert('testdb', 'collection1', { _id: '1', source: 'txn1' });
    txn2.insert('testdb', 'collection2', { _id: '2', source: 'txn2' });

    // Both should be independent
    expect(txn1.operationCount).toBe(1);
    expect(txn2.operationCount).toBe(1);

    const ops1 = txn1.getOperations();
    const ops2 = txn2.getOperations();

    expect(ops1[0].document?.source).toBe('txn1');
    expect(ops2[0].document?.source).toBe('txn2');

    await session1.endSession();
    await session2.endSession();
  });

  it('should allow one session to commit while another aborts', async () => {
    const session1 = client.startSession();
    const session2 = client.startSession();

    const txn1 = new TransactionManager(session1);
    const txn2 = new TransactionManager(session2);

    txn1.begin();
    txn2.begin();

    txn1.insert('testdb', 'users', { _id: 'commit-user', name: 'Committed' });
    txn2.insert('testdb', 'users', { _id: 'abort-user', name: 'Aborted' });

    await txn1.commit();
    await txn2.abort();

    expect(session1.transactionState).toBe('committed');
    expect(session2.transactionState).toBe('aborted');

    await session1.endSession();
    await session2.endSession();
  });

  it('should handle parallel transaction execution', async () => {
    const sessions = Array.from({ length: 5 }, () => client.startSession());
    const transactions = sessions.map((s) => new TransactionManager(s));

    // Start all transactions
    transactions.forEach((txn) => txn.begin());

    // Add operations to each
    transactions.forEach((txn, i) => {
      txn.insert('testdb', 'parallel', { _id: `parallel-${i}`, index: i });
    });

    // Commit all in parallel
    const commitPromises = transactions.map((txn) => txn.commit());
    const results = await Promise.all(commitPromises);

    // All should succeed
    for (const result of results) {
      expect(result.success).toBe(true);
    }

    // Cleanup
    await Promise.all(sessions.map((s) => s.endSession()));
  });

  it('should track transaction numbers correctly', () => {
    const session = client.startSession();
    const txn = new TransactionManager(session);

    expect(session.txnNumber).toBe(0);

    txn.begin();
    expect(session.txnNumber).toBe(1);
    session.abortTransaction();

    txn.begin();
    expect(session.txnNumber).toBe(2);
    session.abortTransaction();

    txn.begin();
    expect(session.txnNumber).toBe(3);

    session.endSession();
  });
});

// ============================================================================
// Snapshot LSN Tracking Tests
// ============================================================================

describe('Transaction - Snapshot LSN Tracking', () => {
  let client: MongoLake;
  let session: ClientSession;
  let txn: TransactionManager;

  beforeEach(() => {
    resetDocumentCounter();
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
    txn.recordShardLSN('shard-0', 100);
    txn.recordShardLSN('shard-1', 200);
    txn.recordShardLSN('shard-2', 150);

    const snapshot = txn.getSnapshot();

    expect(snapshot?.shardLSNs.get('shard-0')).toBe(100);
    expect(snapshot?.shardLSNs.get('shard-1')).toBe(200);
    expect(snapshot?.shardLSNs.get('shard-2')).toBe(150);
  });

  it('should initialize empty shardLSNs map', () => {
    txn.begin();
    const snapshot = txn.getSnapshot();

    expect(snapshot?.shardLSNs).toBeInstanceOf(Map);
    expect(snapshot?.shardLSNs.size).toBe(0);
  });

  it('should not record LSN when not in transaction', () => {
    // Should be a no-op when not in transaction
    txn.recordShardLSN('shard-0', 100);

    expect(txn.getSnapshot()).toBeNull();
  });

  it('should update LSN for same shard', () => {
    txn.begin();
    txn.recordShardLSN('shard-0', 100);
    txn.recordShardLSN('shard-0', 150);

    const snapshot = txn.getSnapshot();

    expect(snapshot?.shardLSNs.get('shard-0')).toBe(150);
  });
});

// ============================================================================
// Session ID Generation Tests
// ============================================================================

describe('Session - ID Generation', () => {
  it('should generate valid UUID format', () => {
    const id = generateSessionId();

    // UUID v4 format: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    expect(id).toMatch(uuidRegex);
  });

  it('should generate unique IDs', () => {
    const ids = new Set<string>();

    for (let i = 0; i < 100; i++) {
      ids.add(generateSessionId());
    }

    expect(ids.size).toBe(100);
  });
});
