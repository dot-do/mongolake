/**
 * Transaction API Unit Tests for MongoLake Client
 *
 * Issue: mongolake-nj6b
 *
 * Comprehensive tests for the public transaction API including:
 * - Session creation and lifecycle
 * - Transaction lifecycle (start, commit, abort)
 * - ACID properties (Atomicity, Consistency, Isolation, Durability)
 * - Error scenarios (commit after abort, operations without transaction, timeout)
 * - Nested operations across multiple collections
 * - Rollback on error handling
 * - Session cleanup
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  MongoLake,
  createClient,
  Collection,
  Database,
  ClientSession,
  SessionStore,
  TransactionError,
  SessionError,
  TransactionManager,
  runTransaction,
  generateSessionId,
  hasSession,
  extractSession,
} from '../../../src/client/index.js';
import type {
  TransactionState,
  TransactionOptions,
  SessionOptions,
  SessionOperationOptions,
  BufferedOperation,
  TransactionCommitResult,
  RunTransactionOptions,
} from '../../../src/client/index.js';
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

interface TestAccount {
  _id?: string;
  name: string;
  balance: number;
  [key: string]: unknown;
}

interface TestOrder {
  _id?: string;
  accountId: string;
  amount: number;
  status: string;
  [key: string]: unknown;
}

// ============================================================================
// 1. Session Creation Tests
// ============================================================================

describe('Transaction API - client.startSession() creates a session', () => {
  let client: MongoLake;

  beforeEach(() => {
    client = createTestClient();
  });

  afterEach(async () => {
    await client.close();
  });

  it('should create a session with startSession()', () => {
    const session = client.startSession();

    expect(session).toBeInstanceOf(ClientSession);
    expect(session.id).toBeDefined();
    expect(typeof session.id).toBe('string');
  });

  it('should create sessions with unique IDs', () => {
    const session1 = client.startSession();
    const session2 = client.startSession();
    const session3 = client.startSession();

    expect(session1.id).not.toBe(session2.id);
    expect(session2.id).not.toBe(session3.id);
    expect(session1.id).not.toBe(session3.id);
  });

  it('should accept SessionOptions when creating session', () => {
    const options: SessionOptions = {
      causalConsistency: true,
      defaultTransactionOptions: {
        readConcern: { level: 'snapshot' },
        writeConcern: { w: 'majority', j: true },
        maxCommitTimeMS: 10000,
      },
    };

    const session = client.startSession(options);

    expect(session).toBeInstanceOf(ClientSession);
    expect(session.hasEnded).toBe(false);
    expect(session.inTransaction).toBe(false);
  });

  it('should initialize session with correct default state', () => {
    const session = client.startSession();

    expect(session.hasEnded).toBe(false);
    expect(session.inTransaction).toBe(false);
    expect(session.transactionState).toBe('none');
    expect(session.txnNumber).toBe(0);
    expect(session.operationCount).toBe(0);
  });

  it('should track session creation timestamp', () => {
    const beforeCreate = new Date();
    const session = client.startSession();
    const afterCreate = new Date();

    expect(session.createdAt).toBeInstanceOf(Date);
    expect(session.createdAt.getTime()).toBeGreaterThanOrEqual(beforeCreate.getTime());
    expect(session.createdAt.getTime()).toBeLessThanOrEqual(afterCreate.getTime());
  });
});

// ============================================================================
// 2. Transaction Begin Tests
// ============================================================================

describe('Transaction API - session.startTransaction() begins transaction', () => {
  let client: MongoLake;
  let session: ClientSession;

  beforeEach(() => {
    client = createTestClient();
    session = client.startSession();
  });

  afterEach(async () => {
    if (!session.hasEnded) {
      await session.endSession();
    }
    await client.close();
  });

  it('should start a transaction with startTransaction()', () => {
    session.startTransaction();

    expect(session.inTransaction).toBe(true);
    expect(session.transactionState).toBe('starting');
  });

  it('should accept TransactionOptions in startTransaction()', () => {
    const options: TransactionOptions = {
      readConcern: { level: 'majority' },
      writeConcern: { w: 1, j: false },
      maxCommitTimeMS: 5000,
    };

    session.startTransaction(options);

    expect(session.inTransaction).toBe(true);
    expect(session.transactionOptions).toMatchObject(options);
  });

  it('should increment txnNumber when starting a new transaction', () => {
    const initialTxn = session.txnNumber;

    session.startTransaction();
    expect(session.txnNumber).toBe(initialTxn + 1);

    session.abortTransaction();

    session.startTransaction();
    expect(session.txnNumber).toBe(initialTxn + 2);
  });

  it('should update lastUsed timestamp when starting transaction', async () => {
    const initialLastUsed = session.lastUsed.getTime();
    await new Promise(resolve => setTimeout(resolve, 10));

    session.startTransaction();

    expect(session.lastUsed.getTime()).toBeGreaterThan(initialLastUsed);
  });

  it('should transition from starting to in_progress on first operation', () => {
    session.startTransaction();
    expect(session.transactionState).toBe('starting');

    session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '1', name: 'test' },
    });

    expect(session.transactionState).toBe('in_progress');
  });
});

// ============================================================================
// 3. Transaction Commit Tests
// ============================================================================

describe('Transaction API - session.commitTransaction() commits changes', () => {
  let client: MongoLake;
  let session: ClientSession;
  let collection: Collection<TestAccount>;

  beforeEach(async () => {
    client = createTestClient();
    session = client.startSession();
    collection = client.db('testdb').collection<TestAccount>('accounts');
    await collection.insertMany([
      { _id: 'acc1', name: 'Account 1', balance: 1000 },
      { _id: 'acc2', name: 'Account 2', balance: 500 },
    ]);
  });

  afterEach(async () => {
    if (!session.hasEnded) {
      await session.endSession();
    }
    await client.close();
  });

  it('should commit a transaction successfully', async () => {
    session.startTransaction();

    session.bufferOperation({
      type: 'insert',
      collection: 'accounts',
      database: 'testdb',
      document: { _id: 'acc3', name: 'Account 3', balance: 750 },
    });

    await session.commitTransaction();

    expect(session.inTransaction).toBe(false);
    expect(session.transactionState).toBe('committed');
  });

  it('should apply all buffered operations on commit', async () => {
    session.startTransaction();

    session.bufferOperation({
      type: 'insert',
      collection: 'accounts',
      database: 'testdb',
      document: { _id: 'acc3', name: 'Account 3', balance: 750 },
    });

    session.bufferOperation({
      type: 'update',
      collection: 'accounts',
      database: 'testdb',
      filter: { _id: 'acc1' },
      update: { $set: { balance: 1500 } },
    });

    await session.commitTransaction();

    // Verify changes were applied
    const acc3 = await collection.findOne({ _id: 'acc3' });
    expect(acc3).toBeDefined();
    expect(acc3?.name).toBe('Account 3');

    const acc1 = await collection.findOne({ _id: 'acc1' });
    expect(acc1?.balance).toBe(1500);
  });

  it('should clear buffered operations after commit', async () => {
    session.startTransaction();

    session.bufferOperation({
      type: 'insert',
      collection: 'accounts',
      database: 'testdb',
      document: { _id: 'acc3', name: 'Account 3', balance: 750 },
    });

    expect(session.operationCount).toBe(1);

    await session.commitTransaction();

    expect(session.operationCount).toBe(0);
  });

  it('should update lastUsed timestamp on commit', async () => {
    session.startTransaction();
    const beforeCommit = session.lastUsed.getTime();
    await new Promise(resolve => setTimeout(resolve, 10));

    await session.commitTransaction();

    expect(session.lastUsed.getTime()).toBeGreaterThan(beforeCommit);
  });
});

// ============================================================================
// 4. Transaction Abort Tests
// ============================================================================

describe('Transaction API - session.abortTransaction() rolls back changes', () => {
  let client: MongoLake;
  let session: ClientSession;
  let collection: Collection<TestAccount>;

  beforeEach(async () => {
    client = createTestClient();
    session = client.startSession();
    collection = client.db('testdb').collection<TestAccount>('accounts');
    await collection.insertMany([
      { _id: 'acc1', name: 'Account 1', balance: 1000 },
      { _id: 'acc2', name: 'Account 2', balance: 500 },
    ]);
  });

  afterEach(async () => {
    if (!session.hasEnded) {
      await session.endSession();
    }
    await client.close();
  });

  it('should abort a transaction successfully', async () => {
    session.startTransaction();

    session.bufferOperation({
      type: 'update',
      collection: 'accounts',
      database: 'testdb',
      filter: { _id: 'acc1' },
      update: { $set: { balance: 0 } },
    });

    await session.abortTransaction();

    expect(session.inTransaction).toBe(false);
    expect(session.transactionState).toBe('aborted');
  });

  it('should discard all buffered operations on abort', async () => {
    session.startTransaction();

    session.bufferOperation({
      type: 'insert',
      collection: 'accounts',
      database: 'testdb',
      document: { _id: 'acc3', name: 'Account 3', balance: 750 },
    });

    session.bufferOperation({
      type: 'delete',
      collection: 'accounts',
      database: 'testdb',
      filter: { _id: 'acc1' },
    });

    await session.abortTransaction();

    // Verify no changes were applied
    const acc3 = await collection.findOne({ _id: 'acc3' });
    expect(acc3).toBeNull();

    const acc1 = await collection.findOne({ _id: 'acc1' });
    expect(acc1).toBeDefined();
    expect(acc1?.balance).toBe(1000);
  });

  it('should clear buffered operations after abort', async () => {
    session.startTransaction();

    session.bufferOperation({
      type: 'insert',
      collection: 'accounts',
      database: 'testdb',
      document: { _id: 'acc3', name: 'Account 3', balance: 750 },
    });

    expect(session.operationCount).toBe(1);

    await session.abortTransaction();

    expect(session.operationCount).toBe(0);
  });

  it('should update lastUsed timestamp on abort', async () => {
    session.startTransaction();
    const beforeAbort = session.lastUsed.getTime();
    await new Promise(resolve => setTimeout(resolve, 10));

    await session.abortTransaction();

    expect(session.lastUsed.getTime()).toBeGreaterThan(beforeAbort);
  });

  it('should allow starting a new transaction after abort', async () => {
    session.startTransaction();
    await session.abortTransaction();

    session.startTransaction();
    expect(session.inTransaction).toBe(true);
  });
});

// ============================================================================
// 5. ACID Property - Atomicity Tests
// ============================================================================

describe('Transaction API - ACID: Atomicity (all or nothing commit)', () => {
  let client: MongoLake;
  let session: ClientSession;
  let collection: Collection<TestAccount>;

  beforeEach(async () => {
    client = createTestClient();
    session = client.startSession();
    collection = client.db('testdb').collection<TestAccount>('accounts');
    await collection.insertMany([
      { _id: 'from', name: 'From Account', balance: 1000 },
      { _id: 'to', name: 'To Account', balance: 0 },
    ]);
  });

  afterEach(async () => {
    if (!session.hasEnded) {
      await session.endSession();
    }
    await client.close();
  });

  it('should apply all operations atomically on successful commit', async () => {
    const transferAmount = 500;

    session.startTransaction();

    // Debit from account
    session.bufferOperation({
      type: 'update',
      collection: 'accounts',
      database: 'testdb',
      filter: { _id: 'from' },
      update: { $inc: { balance: -transferAmount } },
    });

    // Credit to account
    session.bufferOperation({
      type: 'update',
      collection: 'accounts',
      database: 'testdb',
      filter: { _id: 'to' },
      update: { $inc: { balance: transferAmount } },
    });

    await session.commitTransaction();

    const fromAccount = await collection.findOne({ _id: 'from' });
    const toAccount = await collection.findOne({ _id: 'to' });

    expect(fromAccount?.balance).toBe(500);
    expect(toAccount?.balance).toBe(500);

    // Total balance should remain constant
    expect((fromAccount?.balance ?? 0) + (toAccount?.balance ?? 0)).toBe(1000);
  });

  it('should roll back all operations on abort', async () => {
    const transferAmount = 500;

    session.startTransaction();

    session.bufferOperation({
      type: 'update',
      collection: 'accounts',
      database: 'testdb',
      filter: { _id: 'from' },
      update: { $inc: { balance: -transferAmount } },
    });

    session.bufferOperation({
      type: 'update',
      collection: 'accounts',
      database: 'testdb',
      filter: { _id: 'to' },
      update: { $inc: { balance: transferAmount } },
    });

    await session.abortTransaction();

    const fromAccount = await collection.findOne({ _id: 'from' });
    const toAccount = await collection.findOne({ _id: 'to' });

    // Both accounts should retain original values
    expect(fromAccount?.balance).toBe(1000);
    expect(toAccount?.balance).toBe(0);
  });

  it('should not leave partial state on commit failure', async () => {
    session.startTransaction();

    session.bufferOperation({
      type: 'insert',
      collection: 'accounts',
      database: 'testdb',
      document: { _id: 'new1', name: 'New 1', balance: 100 },
    });

    session.bufferOperation({
      type: 'insert',
      collection: 'accounts',
      database: 'testdb',
      document: { _id: 'new2', name: 'New 2', balance: 200 },
    });

    // Mock commit failure
    // @ts-expect-error - accessing private field for testing
    session._commitHandler = async () => {
      throw new Error('Simulated commit failure');
    };

    await expect(session.commitTransaction()).rejects.toThrow('Simulated commit failure');

    // Neither document should exist
    const new1 = await collection.findOne({ _id: 'new1' });
    const new2 = await collection.findOne({ _id: 'new2' });

    expect(new1).toBeNull();
    expect(new2).toBeNull();
  });
});

// ============================================================================
// 6. ACID Property - Consistency Tests
// ============================================================================

describe('Transaction API - ACID: Consistency (invariants maintained)', () => {
  let client: MongoLake;
  let session: ClientSession;
  let collection: Collection<TestAccount>;

  beforeEach(async () => {
    client = createTestClient();
    session = client.startSession();
    collection = client.db('testdb').collection<TestAccount>('accounts');
    await collection.insertMany([
      { _id: 'acc1', name: 'Account 1', balance: 500 },
      { _id: 'acc2', name: 'Account 2', balance: 500 },
    ]);
  });

  afterEach(async () => {
    if (!session.hasEnded) {
      await session.endSession();
    }
    await client.close();
  });

  it('should maintain total balance invariant in transfer operation', async () => {
    // Calculate initial total balance
    const allAccounts = await collection.find({}).toArray();
    const initialTotal = allAccounts.reduce((sum, acc) => sum + acc.balance, 0);
    expect(initialTotal).toBe(1000);

    session.startTransaction();

    // Transfer 200 from acc1 to acc2
    session.bufferOperation({
      type: 'update',
      collection: 'accounts',
      database: 'testdb',
      filter: { _id: 'acc1' },
      update: { $inc: { balance: -200 } },
    });

    session.bufferOperation({
      type: 'update',
      collection: 'accounts',
      database: 'testdb',
      filter: { _id: 'acc2' },
      update: { $inc: { balance: 200 } },
    });

    await session.commitTransaction();

    // Verify total balance is preserved
    const finalAccounts = await collection.find({}).toArray();
    const finalTotal = finalAccounts.reduce((sum, acc) => sum + acc.balance, 0);
    expect(finalTotal).toBe(1000);
  });

  it('should maintain document count on failed transaction', async () => {
    const initialCount = await collection.countDocuments({});
    expect(initialCount).toBe(2);

    session.startTransaction();

    session.bufferOperation({
      type: 'insert',
      collection: 'accounts',
      database: 'testdb',
      document: { _id: 'acc3', name: 'Account 3', balance: 300 },
    });

    session.bufferOperation({
      type: 'delete',
      collection: 'accounts',
      database: 'testdb',
      filter: { _id: 'acc1' },
    });

    await session.abortTransaction();

    const finalCount = await collection.countDocuments({});
    expect(finalCount).toBe(2);
  });
});

// ============================================================================
// 7. ACID Property - Isolation Tests
// ============================================================================

describe('Transaction API - ACID: Isolation (concurrent transactions)', () => {
  let client: MongoLake;
  let session1: ClientSession;
  let session2: ClientSession;
  let collection: Collection<TestAccount>;

  beforeEach(async () => {
    client = createTestClient();
    session1 = client.startSession();
    session2 = client.startSession();
    collection = client.db('testdb').collection<TestAccount>('accounts');
    await collection.insertOne({ _id: 'shared', name: 'Shared Account', balance: 1000 });
  });

  afterEach(async () => {
    if (!session1.hasEnded) await session1.endSession();
    if (!session2.hasEnded) await session2.endSession();
    await client.close();
  });

  it('should prevent dirty reads (uncommitted changes not visible)', async () => {
    session1.startTransaction();

    // Buffer an update in session1 (not committed)
    session1.bufferOperation({
      type: 'update',
      collection: 'accounts',
      database: 'testdb',
      filter: { _id: 'shared' },
      update: { $set: { balance: 0 } },
    });

    // Session2 should not see the uncommitted change
    const balance = await collection.findOne({ _id: 'shared' });
    expect(balance?.balance).toBe(1000); // Original value, not 0

    await session1.abortTransaction();
  });

  it('should isolate buffered operations between sessions', async () => {
    session1.startTransaction();
    session2.startTransaction();

    session1.bufferOperation({
      type: 'insert',
      collection: 'accounts',
      database: 'testdb',
      document: { _id: 'session1Doc', name: 'Session 1 Doc', balance: 100 },
    });

    session2.bufferOperation({
      type: 'insert',
      collection: 'accounts',
      database: 'testdb',
      document: { _id: 'session2Doc', name: 'Session 2 Doc', balance: 200 },
    });

    // Sessions should not see each other's buffered operations
    expect(session1.operationCount).toBe(1);
    expect(session2.operationCount).toBe(1);

    const ops1 = session1.getBufferedOperations();
    const ops2 = session2.getBufferedOperations();

    expect(ops1.some(op => op.document?._id === 'session1Doc')).toBe(true);
    expect(ops1.some(op => op.document?._id === 'session2Doc')).toBe(false);

    expect(ops2.some(op => op.document?._id === 'session2Doc')).toBe(true);
    expect(ops2.some(op => op.document?._id === 'session1Doc')).toBe(false);

    await session1.abortTransaction();
    await session2.abortTransaction();
  });

  it('should handle concurrent commit and abort on different sessions', async () => {
    session1.startTransaction();
    session2.startTransaction();

    session1.bufferOperation({
      type: 'insert',
      collection: 'accounts',
      database: 'testdb',
      document: { _id: 'committed', name: 'Committed', balance: 100 },
    });

    session2.bufferOperation({
      type: 'insert',
      collection: 'accounts',
      database: 'testdb',
      document: { _id: 'aborted', name: 'Aborted', balance: 200 },
    });

    await Promise.all([
      session1.commitTransaction(),
      session2.abortTransaction(),
    ]);

    expect(session1.transactionState).toBe('committed');
    expect(session2.transactionState).toBe('aborted');

    // Only committed document should exist
    const committed = await collection.findOne({ _id: 'committed' });
    const aborted = await collection.findOne({ _id: 'aborted' });

    expect(committed).toBeDefined();
    expect(aborted).toBeNull();
  });
});

// ============================================================================
// 8. ACID Property - Durability Tests
// ============================================================================

describe('Transaction API - ACID: Durability (committed changes persist)', () => {
  let client: MongoLake;
  let session: ClientSession;
  let collection: Collection<TestAccount>;

  beforeEach(async () => {
    client = createTestClient();
    session = client.startSession();
    collection = client.db('testdb').collection<TestAccount>('accounts');
    await collection.insertOne({ _id: 'existing', name: 'Existing', balance: 100 });
  });

  afterEach(async () => {
    if (!session.hasEnded) {
      await session.endSession();
    }
    await client.close();
  });

  it('should persist committed changes after transaction completes', async () => {
    session.startTransaction();

    session.bufferOperation({
      type: 'insert',
      collection: 'accounts',
      database: 'testdb',
      document: { _id: 'persisted', name: 'Persisted', balance: 500 },
    });

    session.bufferOperation({
      type: 'update',
      collection: 'accounts',
      database: 'testdb',
      filter: { _id: 'existing' },
      update: { $set: { balance: 200 } },
    });

    await session.commitTransaction();
    await session.endSession();

    // Verify data persists after session ends
    const persisted = await collection.findOne({ _id: 'persisted' });
    const existing = await collection.findOne({ _id: 'existing' });

    expect(persisted?.balance).toBe(500);
    expect(existing?.balance).toBe(200);
  });

  it('should maintain committed state after session cleanup', async () => {
    session.startTransaction();

    session.bufferOperation({
      type: 'insert',
      collection: 'accounts',
      database: 'testdb',
      document: { _id: 'durable', name: 'Durable', balance: 1000 },
    });

    await session.commitTransaction();

    // Create a new session and verify data
    const newSession = client.startSession();

    const doc = await collection.findOne({ _id: 'durable' });
    expect(doc).toBeDefined();
    expect(doc?.name).toBe('Durable');
    expect(doc?.balance).toBe(1000);

    await newSession.endSession();
  });
});

// ============================================================================
// 9. Rollback on Error Tests
// ============================================================================

describe('Transaction API - Rollback on error', () => {
  let client: MongoLake;
  let collection: Collection<TestAccount>;

  beforeEach(async () => {
    client = createTestClient();
    collection = client.db('testdb').collection<TestAccount>('accounts');
    await collection.insertMany([
      { _id: 'acc1', name: 'Account 1', balance: 1000 },
      { _id: 'acc2', name: 'Account 2', balance: 500 },
    ]);
  });

  afterEach(async () => {
    await client.close();
  });

  it('should trigger abort when exception occurs during transaction', async () => {
    const session = client.startSession();

    try {
      session.startTransaction();

      session.bufferOperation({
        type: 'update',
        collection: 'accounts',
        database: 'testdb',
        filter: { _id: 'acc1' },
        update: { $set: { balance: 0 } },
      });

      // Simulate an error during transaction
      throw new Error('Business logic error');

      // This should not be reached
      await session.commitTransaction();
    } catch (error) {
      await session.abortTransaction();
    } finally {
      await session.endSession();
    }

    // Verify rollback occurred
    const acc1 = await collection.findOne({ _id: 'acc1' });
    expect(acc1?.balance).toBe(1000);
  });

  it('should auto-abort transaction when session ends with active transaction', async () => {
    const session = client.startSession();

    session.startTransaction();

    session.bufferOperation({
      type: 'update',
      collection: 'accounts',
      database: 'testdb',
      filter: { _id: 'acc1' },
      update: { $set: { balance: 0 } },
    });

    // End session without explicit commit/abort
    await session.endSession();

    expect(session.transactionState).toBe('aborted');

    // Verify no changes were applied
    const acc1 = await collection.findOne({ _id: 'acc1' });
    expect(acc1?.balance).toBe(1000);
  });
});

// ============================================================================
// 10. Nested Operations (Multiple Collections) Tests
// ============================================================================

describe('Transaction API - Nested operations (multiple collections)', () => {
  let client: MongoLake;
  let session: ClientSession;
  let accountsCollection: Collection<TestAccount>;
  let ordersCollection: Collection<TestOrder>;

  beforeEach(async () => {
    client = createTestClient();
    session = client.startSession();
    accountsCollection = client.db('testdb').collection<TestAccount>('accounts');
    ordersCollection = client.db('testdb').collection<TestOrder>('orders');
    await accountsCollection.insertOne({ _id: 'acc1', name: 'Account 1', balance: 1000 });
  });

  afterEach(async () => {
    if (!session.hasEnded) {
      await session.endSession();
    }
    await client.close();
  });

  it('should support operations across multiple collections', async () => {
    session.startTransaction();

    // Deduct from account
    session.bufferOperation({
      type: 'update',
      collection: 'accounts',
      database: 'testdb',
      filter: { _id: 'acc1' },
      update: { $inc: { balance: -100 } },
    });

    // Create order
    session.bufferOperation({
      type: 'insert',
      collection: 'orders',
      database: 'testdb',
      document: { _id: 'order1', accountId: 'acc1', amount: 100, status: 'completed' },
    });

    await session.commitTransaction();

    const account = await accountsCollection.findOne({ _id: 'acc1' });
    const order = await ordersCollection.findOne({ _id: 'order1' });

    expect(account?.balance).toBe(900);
    expect(order?.amount).toBe(100);
    expect(order?.status).toBe('completed');
  });

  it('should rollback all collections on abort', async () => {
    session.startTransaction();

    session.bufferOperation({
      type: 'update',
      collection: 'accounts',
      database: 'testdb',
      filter: { _id: 'acc1' },
      update: { $set: { balance: 0 } },
    });

    session.bufferOperation({
      type: 'insert',
      collection: 'orders',
      database: 'testdb',
      document: { _id: 'order1', accountId: 'acc1', amount: 1000, status: 'pending' },
    });

    await session.abortTransaction();

    const account = await accountsCollection.findOne({ _id: 'acc1' });
    const order = await ordersCollection.findOne({ _id: 'order1' });

    expect(account?.balance).toBe(1000); // Original value preserved
    expect(order).toBeNull(); // Order not created
  });

  it('should support operations across multiple databases', async () => {
    const db2Collection = client.db('testdb2').collection<TestAccount>('accounts');
    await db2Collection.insertOne({ _id: 'acc2', name: 'Account in DB2', balance: 500 });

    session.startTransaction();

    session.bufferOperation({
      type: 'update',
      collection: 'accounts',
      database: 'testdb',
      filter: { _id: 'acc1' },
      update: { $inc: { balance: -200 } },
    });

    session.bufferOperation({
      type: 'update',
      collection: 'accounts',
      database: 'testdb2',
      filter: { _id: 'acc2' },
      update: { $inc: { balance: 200 } },
    });

    await session.commitTransaction();

    const acc1 = await accountsCollection.findOne({ _id: 'acc1' });
    const acc2 = await db2Collection.findOne({ _id: 'acc2' });

    expect(acc1?.balance).toBe(800);
    expect(acc2?.balance).toBe(700);
  });
});

// ============================================================================
// 11. Session Cleanup Tests
// ============================================================================

describe('Transaction API - session.endSession() cleanup', () => {
  let client: MongoLake;

  beforeEach(() => {
    client = createTestClient();
  });

  afterEach(async () => {
    await client.close();
  });

  it('should end session properly with endSession()', async () => {
    const session = client.startSession();
    expect(session.hasEnded).toBe(false);

    await session.endSession();

    expect(session.hasEnded).toBe(true);
  });

  it('should allow calling endSession multiple times', async () => {
    const session = client.startSession();

    await session.endSession();
    await session.endSession();
    await session.endSession();

    expect(session.hasEnded).toBe(true);
  });

  it('should clear buffered operations on end session', async () => {
    const session = client.startSession();

    session.startTransaction();
    session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '1', name: 'test' },
    });

    expect(session.operationCount).toBe(1);

    await session.endSession();

    expect(session.operationCount).toBe(0);
  });

  it('should abort active transaction when ending session', async () => {
    const session = client.startSession();

    session.startTransaction();

    await session.endSession();

    expect(session.hasEnded).toBe(true);
    expect(session.transactionState).toBe('aborted');
  });
});

// ============================================================================
// 12. Error Scenarios Tests
// ============================================================================

describe('Transaction API - Error: Commit after abort', () => {
  let client: MongoLake;
  let session: ClientSession;

  beforeEach(() => {
    client = createTestClient();
    session = client.startSession();
  });

  afterEach(async () => {
    if (!session.hasEnded) {
      await session.endSession();
    }
    await client.close();
  });

  it('should throw error when committing after abort', async () => {
    session.startTransaction();
    await session.abortTransaction();

    await expect(session.commitTransaction()).rejects.toThrow(TransactionError);
  });

  it('should throw error when aborting after abort', async () => {
    session.startTransaction();
    await session.abortTransaction();

    await expect(session.abortTransaction()).rejects.toThrow(TransactionError);
  });

  it('should throw error when starting nested transaction', () => {
    session.startTransaction();

    expect(() => session.startTransaction()).toThrow(TransactionError);
  });
});

describe('Transaction API - Error: Operations without transaction', () => {
  let client: MongoLake;
  let session: ClientSession;

  beforeEach(() => {
    client = createTestClient();
    session = client.startSession();
  });

  afterEach(async () => {
    if (!session.hasEnded) {
      await session.endSession();
    }
    await client.close();
  });

  it('should throw error when committing without active transaction', async () => {
    await expect(session.commitTransaction()).rejects.toThrow(TransactionError);
  });

  it('should throw error when aborting without active transaction', async () => {
    await expect(session.abortTransaction()).rejects.toThrow(TransactionError);
  });

  it('should throw error when buffering operation without active transaction', () => {
    expect(() => session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '1', name: 'test' },
    })).toThrow(TransactionError);
  });
});

describe('Transaction API - Error: Using ended session', () => {
  let client: MongoLake;
  let session: ClientSession;

  beforeEach(async () => {
    client = createTestClient();
    session = client.startSession();
    await session.endSession();
  });

  afterEach(async () => {
    await client.close();
  });

  it('should throw error when starting transaction on ended session', () => {
    expect(() => session.startTransaction()).toThrow(SessionError);
    expect(() => session.startTransaction()).toThrow('Cannot use a session that has ended.');
  });

  it('should throw error when committing on ended session', async () => {
    await expect(session.commitTransaction()).rejects.toThrow(SessionError);
  });

  it('should throw error when aborting on ended session', async () => {
    await expect(session.abortTransaction()).rejects.toThrow(SessionError);
  });

  it('should throw error when buffering operation on ended session', () => {
    expect(() => session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '1', name: 'test' },
    })).toThrow(SessionError);
  });
});

// ============================================================================
// 13. TransactionManager API Tests
// ============================================================================

describe('Transaction API - TransactionManager', () => {
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

  it('should commit a transaction with result', async () => {
    txn.begin();
    txn.insert('testdb', 'users', { _id: '1', name: 'Test' });

    const result: TransactionCommitResult = await txn.commit();

    expect(result.success).toBe(true);
    expect(result.operationCount).toBe(1);
    expect(typeof result.commitTime).toBe('number');
  });

  it('should abort a transaction', async () => {
    txn.begin();
    txn.insert('testdb', 'users', { _id: '1', name: 'Test' });

    await txn.abort();

    expect(txn.inTransaction).toBe(false);
  });

  it('should buffer CRUD operations', () => {
    txn.begin();

    txn.insert('testdb', 'users', { _id: '1', name: 'Alice' });
    txn.update('testdb', 'users', { _id: '1' }, { $set: { active: true } });
    txn.replace('testdb', 'users', { _id: '1' }, { _id: '1', name: 'Alice Updated' });
    txn.delete('testdb', 'users', { _id: '1' });

    expect(txn.operationCount).toBe(4);

    const ops = txn.getOperations();
    expect(ops[0].type).toBe('insert');
    expect(ops[1].type).toBe('update');
    expect(ops[2].type).toBe('replace');
    expect(ops[3].type).toBe('delete');
  });

  it('should provide snapshot information', () => {
    txn.begin();
    const snapshot = txn.getSnapshot();

    expect(snapshot).not.toBeNull();
    expect(snapshot?.startTime).toBeLessThanOrEqual(Date.now());
    expect(snapshot?.shardLSNs).toBeInstanceOf(Map);
  });
});

// ============================================================================
// 14. runTransaction Helper Tests
// ============================================================================

describe('Transaction API - runTransaction helper', () => {
  let client: MongoLake;
  let collection: Collection<TestAccount>;

  beforeEach(async () => {
    client = createTestClient();
    collection = client.db('testdb').collection<TestAccount>('accounts');
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
    let executed = false;

    try {
      await runTransaction(session, async (txn) => {
        executed = true;
        txn.insert('testdb', 'accounts', { _id: 'acc3', name: 'New', balance: 100 });
      });

      expect(executed).toBe(true);
    } finally {
      await session.endSession();
    }
  });

  it('should return callback result', async () => {
    const session = client.startSession();

    try {
      const result = await runTransaction(session, async (txn) => {
        txn.insert('testdb', 'accounts', { _id: 'acc3', name: 'New', balance: 100 });
        return { success: true, insertedId: 'acc3' };
      });

      expect(result.success).toBe(true);
      expect(result.insertedId).toBe('acc3');
    } finally {
      await session.endSession();
    }
  });

  it('should accept RunTransactionOptions', async () => {
    const session = client.startSession();
    const options: RunTransactionOptions = {
      transactionOptions: {
        readConcern: { level: 'snapshot' },
        writeConcern: { w: 'majority' },
      },
      maxRetries: 5,
      retryDelayMs: 50,
    };

    try {
      await runTransaction(
        session,
        async (txn) => {
          txn.insert('testdb', 'accounts', { _id: 'acc3', name: 'New', balance: 100 });
        },
        options
      );
    } finally {
      await session.endSession();
    }
  });

  it('should abort transaction on callback error', async () => {
    const session = client.startSession();

    try {
      await expect(
        runTransaction(session, async () => {
          throw new Error('Callback error');
        })
      ).rejects.toThrow('Callback error');
    } finally {
      await session.endSession();
    }

    // Verify no changes were made
    const count = await collection.countDocuments({});
    expect(count).toBe(2);
  });
});

// ============================================================================
// 15. Utility Functions Tests
// ============================================================================

describe('Transaction API - Utility functions', () => {
  let client: MongoLake;

  beforeEach(() => {
    client = createTestClient();
  });

  afterEach(async () => {
    await client.close();
  });

  it('generateSessionId should generate unique IDs', () => {
    const ids = new Set<string>();

    for (let i = 0; i < 100; i++) {
      const id = generateSessionId();
      expect(ids.has(id)).toBe(false);
      ids.add(id);
    }

    expect(ids.size).toBe(100);
  });

  it('hasSession should return true for options with session', () => {
    const session = client.startSession();
    const options: SessionOperationOptions = { session };

    expect(hasSession(options)).toBe(true);
  });

  it('hasSession should return false for options without session', () => {
    expect(hasSession({})).toBe(false);
    expect(hasSession(undefined)).toBe(false);
  });

  it('extractSession should return session from options', () => {
    const session = client.startSession();
    const options: SessionOperationOptions = { session };

    const extracted = extractSession(options);
    expect(extracted).toBe(session);
  });

  it('extractSession should return undefined when no session', () => {
    expect(extractSession({})).toBeUndefined();
    expect(extractSession(undefined)).toBeUndefined();
  });
});

// ============================================================================
// 16. Session Store Tests
// ============================================================================

describe('Transaction API - SessionStore', () => {
  it('should add and retrieve sessions', async () => {
    const store = new SessionStore({ timeoutMs: 60000, cleanupIntervalMs: 0 });
    const session = new ClientSession();

    store.add(session);

    const retrieved = store.get(session.id);
    expect(retrieved).toBe(session);

    await store.closeAll();
  });

  it('should track session count', async () => {
    const store = new SessionStore({ timeoutMs: 60000, cleanupIntervalMs: 0 });

    expect(store.size).toBe(0);

    store.add(new ClientSession());
    expect(store.size).toBe(1);

    store.add(new ClientSession());
    expect(store.size).toBe(2);

    await store.closeAll();
    expect(store.size).toBe(0);
  });

  it('should remove sessions by ID', async () => {
    const store = new SessionStore({ timeoutMs: 60000, cleanupIntervalMs: 0 });
    const session = new ClientSession();

    store.add(session);
    expect(store.has(session.id)).toBe(true);

    const removed = store.remove(session.id);
    expect(removed).toBe(true);
    expect(store.has(session.id)).toBe(false);

    await store.closeAll();
  });

  it('should expire sessions after timeout', async () => {
    const store = new SessionStore({ timeoutMs: 50, cleanupIntervalMs: 0 });
    const session = new ClientSession();

    store.add(session);
    expect(store.has(session.id)).toBe(true);

    await new Promise(resolve => setTimeout(resolve, 100));

    const cleaned = store.cleanupExpired();
    expect(cleaned).toBe(1);
    expect(store.has(session.id)).toBe(false);

    await store.closeAll();
  });

  it('should return all session IDs', async () => {
    const store = new SessionStore({ timeoutMs: 60000, cleanupIntervalMs: 0 });

    const s1 = new ClientSession();
    const s2 = new ClientSession();
    const s3 = new ClientSession();

    store.add(s1);
    store.add(s2);
    store.add(s3);

    const ids = store.getSessionIds();
    expect(ids.length).toBe(3);
    expect(ids).toContain(s1.id);
    expect(ids).toContain(s2.id);
    expect(ids).toContain(s3.id);

    await store.closeAll();
  });
});

// ============================================================================
// 17. Complete Transaction Flow Integration Test
// ============================================================================

describe('Transaction API - Complete flow integration test', () => {
  let client: MongoLake;

  beforeEach(async () => {
    client = createTestClient();
    const collection = client.db('bank').collection<TestAccount>('accounts');
    await collection.insertMany([
      { _id: 'checking', name: 'Checking', balance: 5000 },
      { _id: 'savings', name: 'Savings', balance: 10000 },
    ]);
  });

  afterEach(async () => {
    await client.close();
  });

  it('should perform a complete bank transfer transaction', async () => {
    const accounts = client.db('bank').collection<TestAccount>('accounts');
    const transfers = client.db('bank').collection('transfers');
    const session = client.startSession();
    const transferAmount = 2500;

    try {
      session.startTransaction({
        readConcern: { level: 'snapshot' },
        writeConcern: { w: 'majority' },
      });

      // Debit from checking
      session.bufferOperation({
        type: 'update',
        collection: 'accounts',
        database: 'bank',
        filter: { _id: 'checking' },
        update: { $inc: { balance: -transferAmount } },
      });

      // Credit to savings
      session.bufferOperation({
        type: 'update',
        collection: 'accounts',
        database: 'bank',
        filter: { _id: 'savings' },
        update: { $inc: { balance: transferAmount } },
      });

      // Record the transfer
      session.bufferOperation({
        type: 'insert',
        collection: 'transfers',
        database: 'bank',
        document: {
          _id: 'transfer1',
          from: 'checking',
          to: 'savings',
          amount: transferAmount,
          timestamp: new Date().toISOString(),
        },
      });

      await session.commitTransaction();
    } catch (error) {
      await session.abortTransaction();
      throw error;
    } finally {
      await session.endSession();
    }

    // Verify the results
    const checking = await accounts.findOne({ _id: 'checking' });
    const savings = await accounts.findOne({ _id: 'savings' });
    const transfer = await transfers.findOne({ _id: 'transfer1' });

    expect(checking?.balance).toBe(2500);
    expect(savings?.balance).toBe(12500);
    expect(transfer?.amount).toBe(2500);

    // Total should still be 15000
    expect((checking?.balance ?? 0) + (savings?.balance ?? 0)).toBe(15000);
  });
});
