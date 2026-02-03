/**
 * Transaction API Public Surface Tests
 *
 * Tests that verify the transaction API is properly exposed
 * and usable through the public client interface.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  // Client and Collection
  MongoLake,
  createClient,
  Collection,
  // Session and Transaction (should all be exported from client/index.ts)
  ClientSession,
  SessionStore,
  TransactionError,
  SessionError,
  generateSessionId,
  hasSession,
  extractSession,
  // Transaction Manager
  TransactionManager,
  runTransaction,
} from '../../../src/client/index.js';
import type {
  // Session types (should all be exported from client/index.ts)
  TransactionState,
  ReadConcernLevel,
  WriteConcern,
  TransactionOptions,
  SessionOptions,
  SessionOperationOptions,
  BufferedOperation,
  SessionId,
  // Transaction types
  TransactionWrite,
  TransactionSnapshot,
  TransactionCommitResult,
  RunTransactionOptions,
  // Options with session support
  FindOptions,
  UpdateOptions,
  DeleteOptions,
  AggregateOptions,
  InsertOptions,
  SessionOption,
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

interface TestDoc {
  _id?: string;
  name: string;
  balance?: number;
  [key: string]: unknown;
}

// ============================================================================
// Public API Export Tests
// ============================================================================

describe('Transaction API - Public Exports', () => {
  it('should export ClientSession class', () => {
    expect(ClientSession).toBeDefined();
    expect(typeof ClientSession).toBe('function');
  });

  it('should export SessionStore class', () => {
    expect(SessionStore).toBeDefined();
    expect(typeof SessionStore).toBe('function');
  });

  it('should export TransactionManager class', () => {
    expect(TransactionManager).toBeDefined();
    expect(typeof TransactionManager).toBe('function');
  });

  it('should export TransactionError class', () => {
    expect(TransactionError).toBeDefined();
    expect(typeof TransactionError).toBe('function');
    const error = new TransactionError('test');
    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe('TransactionError');
  });

  it('should export SessionError class', () => {
    expect(SessionError).toBeDefined();
    expect(typeof SessionError).toBe('function');
    const error = new SessionError('test');
    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe('SessionError');
  });

  it('should export runTransaction helper function', () => {
    expect(runTransaction).toBeDefined();
    expect(typeof runTransaction).toBe('function');
  });

  it('should export generateSessionId utility', () => {
    expect(generateSessionId).toBeDefined();
    expect(typeof generateSessionId).toBe('function');
    const id = generateSessionId();
    expect(typeof id).toBe('string');
    expect(id.length).toBeGreaterThan(0);
  });

  it('should export hasSession utility', () => {
    expect(hasSession).toBeDefined();
    expect(typeof hasSession).toBe('function');
  });

  it('should export extractSession utility', () => {
    expect(extractSession).toBeDefined();
    expect(typeof extractSession).toBe('function');
  });
});

// ============================================================================
// Client Session API Tests
// ============================================================================

describe('Transaction API - Client.startSession()', () => {
  let client: MongoLake;

  beforeEach(() => {
    client = createTestClient();
  });

  afterEach(async () => {
    await client.close();
  });

  it('should start a session from the client', () => {
    const session = client.startSession();

    expect(session).toBeInstanceOf(ClientSession);
    expect(session.id).toBeDefined();
    expect(session.hasEnded).toBe(false);
    expect(session.inTransaction).toBe(false);
  });

  it('should accept session options', () => {
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
  });

  it('should create unique sessions', () => {
    const session1 = client.startSession();
    const session2 = client.startSession();

    expect(session1.id).not.toBe(session2.id);
  });
});

// ============================================================================
// Session Transaction Methods Tests
// ============================================================================

describe('Transaction API - Session Transaction Methods', () => {
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

  it('should accept transaction options in startTransaction()', () => {
    const options: TransactionOptions = {
      readConcern: { level: 'majority' },
      writeConcern: { w: 1, j: false },
      maxCommitTimeMS: 5000,
    };

    session.startTransaction(options);

    expect(session.inTransaction).toBe(true);
    expect(session.transactionOptions).toMatchObject(options);
  });

  it('should commit a transaction with commitTransaction()', async () => {
    session.startTransaction();

    await session.commitTransaction();

    expect(session.inTransaction).toBe(false);
    expect(session.transactionState).toBe('committed');
  });

  it('should abort a transaction with abortTransaction()', async () => {
    session.startTransaction();

    await session.abortTransaction();

    expect(session.inTransaction).toBe(false);
    expect(session.transactionState).toBe('aborted');
  });

  it('should end a session with endSession()', async () => {
    await session.endSession();

    expect(session.hasEnded).toBe(true);
  });

  it('should auto-abort transaction when ending session', async () => {
    session.startTransaction();

    await session.endSession();

    expect(session.hasEnded).toBe(true);
    expect(session.transactionState).toBe('aborted');
  });
});

// ============================================================================
// Session Properties Tests
// ============================================================================

describe('Transaction API - Session Properties', () => {
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

  it('should expose id property', () => {
    expect(session.id).toBeDefined();
    expect(typeof session.id).toBe('string');
  });

  it('should expose hasEnded property', () => {
    expect(session.hasEnded).toBe(false);
  });

  it('should expose inTransaction property', () => {
    expect(session.inTransaction).toBe(false);

    session.startTransaction();
    expect(session.inTransaction).toBe(true);
  });

  it('should expose transactionState property', () => {
    expect(session.transactionState).toBe('none');

    session.startTransaction();
    expect(session.transactionState).toBe('starting');
  });

  it('should expose txnNumber property', () => {
    const initialTxn = session.txnNumber;
    expect(typeof initialTxn).toBe('number');

    session.startTransaction();
    expect(session.txnNumber).toBe(initialTxn + 1);
  });

  it('should expose transactionOptions property', () => {
    expect(session.transactionOptions).toBeNull();

    const options: TransactionOptions = { readConcern: { level: 'snapshot' } };
    session.startTransaction(options);

    expect(session.transactionOptions).toMatchObject(options);
  });

  it('should expose operationCount property', () => {
    session.startTransaction();
    expect(session.operationCount).toBe(0);

    session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '1', name: 'test' },
    });

    expect(session.operationCount).toBe(1);
  });

  it('should expose createdAt property', () => {
    expect(session.createdAt).toBeInstanceOf(Date);
  });

  it('should expose lastUsed property', () => {
    expect(session.lastUsed).toBeInstanceOf(Date);
  });
});

// ============================================================================
// Transaction Manager API Tests
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

  it('should commit a transaction', async () => {
    txn.begin();
    txn.insert('testdb', 'users', { _id: '1', name: 'Test' });

    const result = await txn.commit();

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

  it('should buffer insert operations', () => {
    txn.begin();
    txn.insert('testdb', 'users', { _id: '1', name: 'Alice' });

    const ops = txn.getOperations();
    expect(ops.length).toBe(1);
    expect(ops[0].type).toBe('insert');
  });

  it('should buffer update operations', () => {
    txn.begin();
    txn.update('testdb', 'users', { _id: '1' }, { $set: { name: 'Bob' } });

    const ops = txn.getOperations();
    expect(ops.length).toBe(1);
    expect(ops[0].type).toBe('update');
  });

  it('should buffer replace operations', () => {
    txn.begin();
    txn.replace('testdb', 'users', { _id: '1' }, { _id: '1', name: 'Charlie' });

    const ops = txn.getOperations();
    expect(ops.length).toBe(1);
    expect(ops[0].type).toBe('replace');
  });

  it('should buffer delete operations', () => {
    txn.begin();
    txn.delete('testdb', 'users', { _id: '1' });

    const ops = txn.getOperations();
    expect(ops.length).toBe(1);
    expect(ops[0].type).toBe('delete');
  });

  it('should expose operationCount property', () => {
    txn.begin();
    expect(txn.operationCount).toBe(0);

    txn.insert('testdb', 'users', { _id: '1', name: 'Test' });
    expect(txn.operationCount).toBe(1);
  });

  it('should expose snapshot via getSnapshot()', () => {
    txn.begin();
    const snapshot = txn.getSnapshot();

    expect(snapshot).not.toBeNull();
    expect(snapshot?.startTime).toBeLessThanOrEqual(Date.now());
    expect(snapshot?.shardLSNs).toBeInstanceOf(Map);
  });
});

// ============================================================================
// runTransaction Helper Tests
// ============================================================================

describe('Transaction API - runTransaction Helper', () => {
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
});

// ============================================================================
// Utility Functions Tests
// ============================================================================

describe('Transaction API - Utility Functions', () => {
  let client: MongoLake;

  beforeEach(() => {
    client = createTestClient();
  });

  afterEach(async () => {
    await client.close();
  });

  it('hasSession should return true for options with session', () => {
    const session = client.startSession();
    const options: SessionOperationOptions = { session };

    expect(hasSession(options)).toBe(true);
  });

  it('hasSession should return false for options without session', () => {
    const options = {};
    expect(hasSession(options)).toBe(false);
  });

  it('hasSession should return false for undefined options', () => {
    expect(hasSession(undefined)).toBe(false);
  });

  it('extractSession should return session from options', () => {
    const session = client.startSession();
    const options: SessionOperationOptions = { session };

    const extracted = extractSession(options);
    expect(extracted).toBe(session);
  });

  it('extractSession should return undefined for options without session', () => {
    const options = {};
    expect(extractSession(options)).toBeUndefined();
  });

  it('generateSessionId should generate unique IDs', () => {
    const id1 = generateSessionId();
    const id2 = generateSessionId();

    expect(id1).not.toBe(id2);
  });
});

// ============================================================================
// Type Safety Tests (compile-time validation)
// ============================================================================

describe('Transaction API - Type Safety', () => {
  it('should accept TransactionState values', () => {
    const states: TransactionState[] = [
      'none',
      'starting',
      'in_progress',
      'committed',
      'aborted',
    ];

    expect(states.length).toBe(5);
  });

  it('should accept ReadConcernLevel values', () => {
    const levels: ReadConcernLevel[] = [
      'local',
      'majority',
      'linearizable',
      'snapshot',
    ];

    expect(levels.length).toBe(4);
  });

  it('should accept WriteConcern interface', () => {
    const writeConcerns: WriteConcern[] = [
      { w: 1 },
      { w: 'majority' },
      { w: 1, j: true },
      { w: 'majority', j: true, wtimeout: 5000 },
    ];

    expect(writeConcerns.length).toBe(4);
  });

  it('should accept TransactionOptions interface', () => {
    const options: TransactionOptions = {
      readConcern: { level: 'snapshot' },
      writeConcern: { w: 'majority', j: true },
      maxCommitTimeMS: 10000,
    };

    expect(options.readConcern?.level).toBe('snapshot');
  });

  it('should accept SessionOptions interface', () => {
    const options: SessionOptions = {
      causalConsistency: true,
      defaultTransactionOptions: {
        readConcern: { level: 'majority' },
      },
    };

    expect(options.causalConsistency).toBe(true);
  });

  it('should accept SessionOperationOptions interface', () => {
    // This is a compile-time test - if it compiles, the types are correct
    const options: SessionOperationOptions = {};
    expect(options.session).toBeUndefined();
  });

  it('should accept FindOptions with session', () => {
    const findOptions: FindOptions = {
      projection: { name: 1 },
      limit: 10,
      session: undefined, // Should be accepted
    };

    expect(findOptions.limit).toBe(10);
  });

  it('should accept UpdateOptions with session', () => {
    const updateOptions: UpdateOptions = {
      upsert: true,
      session: undefined,
    };

    expect(updateOptions.upsert).toBe(true);
  });

  it('should accept DeleteOptions with session', () => {
    const deleteOptions: DeleteOptions = {
      hint: 'idx_name',
      session: undefined,
    };

    expect(deleteOptions.hint).toBe('idx_name');
  });

  it('should accept AggregateOptions with session', () => {
    const aggregateOptions: AggregateOptions = {
      allowDiskUse: true,
      maxTimeMS: 10000,
      session: undefined,
    };

    expect(aggregateOptions.allowDiskUse).toBe(true);
  });

  it('should accept InsertOptions with session', () => {
    const insertOptions: InsertOptions = {
      session: undefined,
    };

    expect(insertOptions.session).toBeUndefined();
  });

  it('should accept SessionOption interface', () => {
    const sessionOpt: SessionOption = {
      session: undefined,
    };

    expect(sessionOpt.session).toBeUndefined();
  });
});

// ============================================================================
// Integration Tests - Full Transaction Flow
// ============================================================================

describe('Transaction API - Full Transaction Flow', () => {
  let client: MongoLake;
  let collection: Collection<TestDoc>;

  beforeEach(async () => {
    client = createTestClient();
    collection = client.db('bank').collection<TestDoc>('accounts');
    await collection.insertMany([
      { _id: 'checking', name: 'Checking Account', balance: 1000 },
      { _id: 'savings', name: 'Savings Account', balance: 5000 },
    ]);
  });

  afterEach(async () => {
    await client.close();
  });

  it('should perform a complete transfer transaction', async () => {
    const session = client.startSession();
    const transferAmount = 500;

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

      await session.commitTransaction();
    } finally {
      await session.endSession();
    }

    // Verify the transfer
    const checking = await collection.findOne({ _id: 'checking' });
    const savings = await collection.findOne({ _id: 'savings' });

    expect(checking?.balance).toBe(500);
    expect(savings?.balance).toBe(5500);

    // Total should still be 6000
    expect((checking?.balance ?? 0) + (savings?.balance ?? 0)).toBe(6000);
  });

  it('should rollback on abort', async () => {
    const session = client.startSession();

    try {
      session.startTransaction();

      session.bufferOperation({
        type: 'update',
        collection: 'accounts',
        database: 'bank',
        filter: { _id: 'checking' },
        update: { $set: { balance: 0 } },
      });

      await session.abortTransaction();
    } finally {
      await session.endSession();
    }

    // Verify no changes
    const checking = await collection.findOne({ _id: 'checking' });
    expect(checking?.balance).toBe(1000);
  });

  it('should use runTransaction for automatic error handling', async () => {
    const session = client.startSession();

    try {
      const result = await runTransaction(session, async (txn) => {
        txn.insert('bank', 'accounts', { _id: 'investment', name: 'Investment', balance: 10000 });
        txn.update('bank', 'accounts', { _id: 'checking' }, { $inc: { balance: 100 } });
        return 'Transfer complete';
      });

      expect(result).toBe('Transfer complete');
    } finally {
      await session.endSession();
    }

    // Verify changes
    const investment = await collection.findOne({ _id: 'investment' });
    const checking = await collection.findOne({ _id: 'checking' });

    expect(investment?.name).toBe('Investment');
    expect(checking?.balance).toBe(1100);
  });
});
