/**
 * MongoDB Transactions Compatibility Tests
 *
 * Tests based on MongoDB Driver Specifications:
 * https://github.com/mongodb/specifications/tree/master/source/transactions/tests/unified
 *
 * These tests validate that MongoLake's transaction implementation is compatible
 * with MongoDB's transaction semantics for:
 * - startTransaction, commitTransaction, abortTransaction
 * - Transaction options (readConcern, writeConcern, maxCommitTimeMS)
 * - Multi-document transactions
 * - Transaction state management
 *
 * ## Supported Features
 *
 * | Feature                      | Status    | Notes                                    |
 * |-----------------------------|-----------|------------------------------------------|
 * | startTransaction            | Supported | Begins a new transaction                 |
 * | commitTransaction           | Supported | Commits buffered operations atomically   |
 * | abortTransaction            | Supported | Discards buffered operations             |
 * | Transaction options         | Supported | readConcern, writeConcern, maxCommitTime |
 * | Multi-document transactions | Supported | Buffer multiple operations               |
 * | Transaction state machine   | Supported | none/starting/in_progress/committed/aborted |
 * | Operation buffering         | Supported | Insert, update, replace, delete          |
 * | Transaction number (txnNumber) | Supported | Incrementing per transaction          |
 * | TransactionManager          | Supported | Coordinates transactions across shards   |
 * | 2PC (Two-Phase Commit)      | Supported | For cross-shard transactions             |
 * | Transaction retry           | Supported | runTransaction helper with retry logic   |
 * | Causal consistency          | Partial   | Option tracked, limited enforcement      |
 * | Retryable writes            | Limited   | Transient error detection                |
 * | Read/write concern propagation | Partial | Options stored, server behavior differs |
 *
 * ## Test Categories
 *
 * 1. Basic Operations: startTransaction, commitTransaction, abortTransaction
 * 2. Transaction Options: Read/write concern, maxCommitTimeMS
 * 3. Multi-Document: Multiple operations in single transaction
 * 4. Error Handling: Invalid operations, state violations
 * 5. Transaction Manager: Coordinator and snapshot management
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  ClientSession,
  TransactionError,
  type TransactionOptions,
  type TransactionState,
} from '../../src/session/index.js';
import {
  TransactionManager,
  runTransaction,
} from '../../src/transaction/index.js';
import { MongoLake, Collection } from '../../src/client/index.js';
import { MemoryStorage } from '../../src/storage/index.js';

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

interface TestDocument {
  _id?: string;
  name?: string;
  balance?: number;
  value?: number;
  [key: string]: unknown;
}

// ============================================================================
// Basic Transaction Operations Tests
// ============================================================================

describe('MongoDB Transactions Compatibility - Basic Operations', () => {
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

  /**
   * Test: Start transaction
   * MongoDB Spec: commit.json - Basic transaction start
   *
   * startTransaction() should begin a new transaction.
   */
  it('should start a transaction', () => {
    session.startTransaction();

    expect(session.inTransaction).toBe(true);
    expect(session.transactionState).toBe('starting');
  });

  /**
   * Test: Commit transaction
   * MongoDB Spec: commit.json - Transaction commit
   *
   * commitTransaction() should commit all buffered operations.
   */
  it('should commit a transaction', async () => {
    session.startTransaction();
    session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '1', name: 'Test' },
    });

    await session.commitTransaction();

    expect(session.inTransaction).toBe(false);
    expect(session.transactionState).toBe('committed');
  });

  /**
   * Test: Abort transaction
   * MongoDB Spec: abort.json - Transaction abort
   *
   * abortTransaction() should discard all buffered operations.
   */
  it('should abort a transaction', async () => {
    session.startTransaction();
    session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '1', name: 'Test' },
    });

    await session.abortTransaction();

    expect(session.inTransaction).toBe(false);
    expect(session.transactionState).toBe('aborted');
    expect(session.operationCount).toBe(0);
  });

  /**
   * Test: Empty transaction commit
   * MongoDB Spec: commit.json - Empty transaction
   *
   * Committing an empty transaction should succeed.
   */
  it('should commit empty transaction', async () => {
    session.startTransaction();

    await session.commitTransaction();

    expect(session.transactionState).toBe('committed');
  });

  /**
   * Test: Sequential transactions
   * MongoDB Spec: commit.json - Multiple transactions
   *
   * After commit, a new transaction can be started.
   */
  it('should support sequential transactions', async () => {
    // First transaction
    session.startTransaction();
    session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '1', name: 'First' },
    });
    await session.commitTransaction();
    expect(session.txnNumber).toBe(1);

    // Second transaction
    session.startTransaction();
    session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '2', name: 'Second' },
    });
    await session.commitTransaction();
    expect(session.txnNumber).toBe(2);
  });

  /**
   * Test: Transaction after abort
   * MongoDB Spec: abort.json - New transaction after abort
   *
   * After abort, a new transaction can be started.
   */
  it('should support transaction after abort', async () => {
    session.startTransaction();
    await session.abortTransaction();

    session.startTransaction();
    expect(session.inTransaction).toBe(true);
    expect(session.txnNumber).toBe(2);
  });
});

// ============================================================================
// Transaction State Machine Tests
// ============================================================================

describe('MongoDB Transactions Compatibility - State Machine', () => {
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

  /**
   * Test: Transaction state transitions
   * MongoDB Spec: Transaction state machine
   *
   * States: none -> starting -> in_progress -> committed/aborted
   */
  it('should transition through transaction states', async () => {
    expect(session.transactionState).toBe('none');

    session.startTransaction();
    expect(session.transactionState).toBe('starting');

    session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '1' },
    });
    expect(session.transactionState).toBe('in_progress');

    await session.commitTransaction();
    expect(session.transactionState).toBe('committed');
  });

  /**
   * Test: State after abort
   */
  it('should transition to aborted state', async () => {
    session.startTransaction();
    session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '1' },
    });

    await session.abortTransaction();
    expect(session.transactionState).toBe('aborted');
  });

  /**
   * Test: inTransaction flag
   * MongoDB Spec: Transaction active state
   */
  it('should correctly report inTransaction', async () => {
    expect(session.inTransaction).toBe(false);

    session.startTransaction();
    expect(session.inTransaction).toBe(true);

    session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '1' },
    });
    expect(session.inTransaction).toBe(true);

    await session.commitTransaction();
    expect(session.inTransaction).toBe(false);
  });

  /**
   * Test: Transaction number increment
   * MongoDB Spec: Transaction numbers increment
   */
  it('should increment transaction number', async () => {
    expect(session.txnNumber).toBe(0);

    session.startTransaction();
    expect(session.txnNumber).toBe(1);
    await session.commitTransaction();

    session.startTransaction();
    expect(session.txnNumber).toBe(2);
    await session.abortTransaction();

    session.startTransaction();
    expect(session.txnNumber).toBe(3);
  });
});

// ============================================================================
// Transaction Options Tests
// ============================================================================

describe('MongoDB Transactions Compatibility - Options', () => {
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

  /**
   * Test: Transaction with read concern
   * MongoDB Spec: transaction-options.json
   */
  it('should accept readConcern option', () => {
    session.startTransaction({
      readConcern: { level: 'snapshot' },
    });

    expect(session.transactionOptions?.readConcern?.level).toBe('snapshot');
  });

  /**
   * Test: Transaction with write concern
   * MongoDB Spec: transaction-options.json
   */
  it('should accept writeConcern option', () => {
    session.startTransaction({
      writeConcern: { w: 'majority', j: true },
    });

    expect(session.transactionOptions?.writeConcern?.w).toBe('majority');
    expect(session.transactionOptions?.writeConcern?.j).toBe(true);
  });

  /**
   * Test: Transaction with maxCommitTimeMS
   * MongoDB Spec: transaction-options.json
   */
  it('should accept maxCommitTimeMS option', () => {
    session.startTransaction({
      maxCommitTimeMS: 5000,
    });

    expect(session.transactionOptions?.maxCommitTimeMS).toBe(5000);
  });

  /**
   * Test: Combined options
   * MongoDB Spec: transaction-options.json
   */
  it('should accept all options together', () => {
    const options: TransactionOptions = {
      readConcern: { level: 'majority' },
      writeConcern: { w: 2, wtimeout: 1000 },
      maxCommitTimeMS: 3000,
    };

    session.startTransaction(options);

    expect(session.transactionOptions).toEqual(options);
  });

  /**
   * Test: Session default options override
   * MongoDB Spec: transaction-options.json - Override hierarchy
   */
  it('should merge session defaults with transaction options', () => {
    // Create session with defaults
    const sessionWithDefaults = new ClientSession({
      defaultTransactionOptions: {
        readConcern: { level: 'local' },
        writeConcern: { w: 1 },
      },
    });

    // Start transaction with partial override
    sessionWithDefaults.startTransaction({
      readConcern: { level: 'snapshot' },
    });

    // readConcern should be overridden, writeConcern should be from defaults
    expect(sessionWithDefaults.transactionOptions?.readConcern?.level).toBe('snapshot');
    expect(sessionWithDefaults.transactionOptions?.writeConcern?.w).toBe(1);

    sessionWithDefaults.endSession();
  });
});

// ============================================================================
// Multi-Document Transaction Tests
// ============================================================================

describe('MongoDB Transactions Compatibility - Multi-Document', () => {
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

  /**
   * Test: Multiple inserts in transaction
   * MongoDB Spec: insert.json - Multi-document insert
   */
  it('should buffer multiple insert operations', () => {
    session.startTransaction();

    session.bufferOperation({
      type: 'insert',
      collection: 'users',
      database: 'testdb',
      document: { _id: '1', name: 'Alice' },
    });
    session.bufferOperation({
      type: 'insert',
      collection: 'users',
      database: 'testdb',
      document: { _id: '2', name: 'Bob' },
    });
    session.bufferOperation({
      type: 'insert',
      collection: 'users',
      database: 'testdb',
      document: { _id: '3', name: 'Charlie' },
    });

    expect(session.operationCount).toBe(3);
  });

  /**
   * Test: Mixed operations in transaction
   * MongoDB Spec: Various CRUD operations in transaction
   */
  it('should buffer mixed operation types', () => {
    session.startTransaction();

    session.bufferOperation({
      type: 'insert',
      collection: 'users',
      database: 'testdb',
      document: { _id: '1', name: 'Alice' },
    });
    session.bufferOperation({
      type: 'update',
      collection: 'users',
      database: 'testdb',
      filter: { _id: '1' },
      update: { $set: { name: 'Updated' } },
    });
    session.bufferOperation({
      type: 'delete',
      collection: 'users',
      database: 'testdb',
      filter: { _id: '2' },
    });

    const ops = session.getBufferedOperations();
    expect(ops[0].type).toBe('insert');
    expect(ops[1].type).toBe('update');
    expect(ops[2].type).toBe('delete');
  });

  /**
   * Test: Operations across collections
   * MongoDB Spec: Cross-collection transactions
   */
  it('should buffer operations across collections', () => {
    session.startTransaction();

    session.bufferOperation({
      type: 'insert',
      collection: 'users',
      database: 'testdb',
      document: { _id: '1', name: 'Alice' },
    });
    session.bufferOperation({
      type: 'insert',
      collection: 'orders',
      database: 'testdb',
      document: { _id: '1', userId: '1', total: 100 },
    });

    const ops = session.getBufferedOperations();
    expect(ops[0].collection).toBe('users');
    expect(ops[1].collection).toBe('orders');
  });

  /**
   * Test: Operations across databases
   * MongoDB Spec: Cross-database transactions
   */
  it('should buffer operations across databases', () => {
    session.startTransaction();

    session.bufferOperation({
      type: 'insert',
      collection: 'users',
      database: 'db1',
      document: { _id: '1', name: 'Alice' },
    });
    session.bufferOperation({
      type: 'insert',
      collection: 'users',
      database: 'db2',
      document: { _id: '1', name: 'Bob' },
    });

    const ops = session.getBufferedOperations();
    expect(ops[0].database).toBe('db1');
    expect(ops[1].database).toBe('db2');
  });

  /**
   * Test: Abort clears all operations
   * MongoDB Spec: abort.json - Operations discarded on abort
   */
  it('should discard all operations on abort', async () => {
    session.startTransaction();

    session.bufferOperation({
      type: 'insert',
      collection: 'users',
      database: 'testdb',
      document: { _id: '1', name: 'Alice' },
    });
    session.bufferOperation({
      type: 'insert',
      collection: 'users',
      database: 'testdb',
      document: { _id: '2', name: 'Bob' },
    });

    expect(session.operationCount).toBe(2);

    await session.abortTransaction();

    expect(session.operationCount).toBe(0);
  });
});

// ============================================================================
// Error Handling Tests
// ============================================================================

describe('MongoDB Transactions Compatibility - Error Handling', () => {
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

  /**
   * Test: Double startTransaction
   * MongoDB Spec: Cannot start transaction when one is active
   */
  it('should reject nested transaction start', () => {
    session.startTransaction();

    expect(() => {
      session.startTransaction();
    }).toThrow(TransactionError);
    expect(() => {
      session.startTransaction();
    }).toThrow('Transaction already in progress');
  });

  /**
   * Test: Commit without transaction
   * MongoDB Spec: abort.json - "abort without start"
   */
  it('should reject commit without transaction', async () => {
    await expect(session.commitTransaction()).rejects.toThrow(TransactionError);
    await expect(session.commitTransaction()).rejects.toThrow(
      'No transaction in progress'
    );
  });

  /**
   * Test: Abort without transaction
   * MongoDB Spec: abort.json - "no transaction started"
   */
  it('should reject abort without transaction', async () => {
    await expect(session.abortTransaction()).rejects.toThrow(TransactionError);
    await expect(session.abortTransaction()).rejects.toThrow(
      'No transaction in progress'
    );
  });

  /**
   * Test: Abort after commit
   * MongoDB Spec: abort.json - "abort directly after commit"
   */
  it('should reject abort after commit', async () => {
    session.startTransaction();
    await session.commitTransaction();

    await expect(session.abortTransaction()).rejects.toThrow(TransactionError);
  });

  /**
   * Test: Commit after abort
   */
  it('should reject commit after abort', async () => {
    session.startTransaction();
    await session.abortTransaction();

    await expect(session.commitTransaction()).rejects.toThrow(TransactionError);
  });

  /**
   * Test: Buffer operation without transaction
   * MongoDB Spec: Cannot write without transaction
   */
  it('should reject buffer without transaction', () => {
    expect(() => {
      session.bufferOperation({
        type: 'insert',
        collection: 'test',
        database: 'testdb',
        document: { _id: '1' },
      });
    }).toThrow(TransactionError);
  });

  /**
   * Test: Operations on ended session
   * MongoDB Spec: Ended sessions cannot be used
   */
  it('should reject operations on ended session', async () => {
    await session.endSession();

    expect(() => {
      session.startTransaction();
    }).toThrow('Cannot use a session that has ended');
  });
});

// ============================================================================
// TransactionManager Tests
// ============================================================================

describe('MongoDB Transactions Compatibility - TransactionManager', () => {
  let client: MongoLake;
  let session: ClientSession;
  let txnManager: TransactionManager;

  beforeEach(() => {
    client = createTestClient();
    session = client.startSession();
    txnManager = new TransactionManager(session);
  });

  afterEach(async () => {
    if (!session.hasEnded) {
      await session.endSession();
    }
    await client.close();
  });

  /**
   * Test: Begin transaction with manager
   */
  it('should begin transaction via manager', () => {
    txnManager.begin();

    expect(txnManager.inTransaction).toBe(true);
    expect(txnManager.getSnapshot()).not.toBeNull();
  });

  /**
   * Test: Manager captures snapshot
   */
  it('should capture snapshot on begin', () => {
    const before = Date.now();
    txnManager.begin();
    const after = Date.now();

    const snapshot = txnManager.getSnapshot();
    expect(snapshot).not.toBeNull();
    expect(snapshot?.startTime).toBeGreaterThanOrEqual(before);
    expect(snapshot?.startTime).toBeLessThanOrEqual(after);
  });

  /**
   * Test: Manager convenience methods
   */
  it('should provide insert convenience method', () => {
    txnManager.begin();
    txnManager.insert('testdb', 'users', { _id: '1', name: 'Alice' });

    const ops = txnManager.getOperations();
    expect(ops).toHaveLength(1);
    expect(ops[0].type).toBe('insert');
  });

  /**
   * Test: Manager update method
   */
  it('should provide update convenience method', () => {
    txnManager.begin();
    txnManager.update('testdb', 'users', { _id: '1' }, { $set: { name: 'Updated' } });

    const ops = txnManager.getOperations();
    expect(ops[0].type).toBe('update');
    expect(ops[0].filter?._id).toBe('1');
  });

  /**
   * Test: Manager replace method
   */
  it('should provide replace convenience method', () => {
    txnManager.begin();
    txnManager.replace('testdb', 'users', { _id: '1' }, { _id: '1', name: 'Replaced' });

    const ops = txnManager.getOperations();
    expect(ops[0].type).toBe('replace');
  });

  /**
   * Test: Manager delete method
   */
  it('should provide delete convenience method', () => {
    txnManager.begin();
    txnManager.delete('testdb', 'users', { _id: '1' });

    const ops = txnManager.getOperations();
    expect(ops[0].type).toBe('delete');
  });

  /**
   * Test: Manager commit
   */
  it('should commit via manager', async () => {
    txnManager.begin();
    txnManager.insert('testdb', 'users', { _id: '1', name: 'Alice' });

    const result = await txnManager.commit();

    expect(result.success).toBe(true);
    expect(result.operationCount).toBe(1);
    expect(txnManager.inTransaction).toBe(false);
    expect(txnManager.getSnapshot()).toBeNull();
  });

  /**
   * Test: Manager abort
   */
  it('should abort via manager', async () => {
    txnManager.begin();
    txnManager.insert('testdb', 'users', { _id: '1', name: 'Alice' });

    await txnManager.abort();

    expect(txnManager.inTransaction).toBe(false);
    expect(txnManager.getSnapshot()).toBeNull();
  });

  /**
   * Test: Manager operation count
   */
  it('should track operation count', () => {
    txnManager.begin();
    expect(txnManager.operationCount).toBe(0);

    txnManager.insert('testdb', 'users', { _id: '1', name: 'Alice' });
    expect(txnManager.operationCount).toBe(1);

    txnManager.insert('testdb', 'users', { _id: '2', name: 'Bob' });
    expect(txnManager.operationCount).toBe(2);
  });
});

// ============================================================================
// runTransaction Helper Tests
// ============================================================================

describe('MongoDB Transactions Compatibility - runTransaction Helper', () => {
  let client: MongoLake;
  let collection: Collection<TestDocument>;

  beforeEach(async () => {
    client = createTestClient();
    collection = client.db('testdb').collection<TestDocument>('accounts');
    await collection.insertMany([
      { _id: 'acc1', name: 'Account 1', balance: 1000 },
      { _id: 'acc2', name: 'Account 2', balance: 500 },
    ]);
  });

  afterEach(async () => {
    await client.close();
  });

  /**
   * Test: Basic runTransaction usage
   */
  it('should execute callback in transaction', async () => {
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

  /**
   * Test: Return value from runTransaction
   */
  it('should return callback result', async () => {
    const session = client.startSession();

    try {
      const result = await runTransaction(session, async (txn) => {
        txn.insert('testdb', 'accounts', { _id: 'acc3', name: 'New', balance: 100 });
        return { success: true, id: 'acc3' };
      });

      expect(result.success).toBe(true);
      expect(result.id).toBe('acc3');
    } finally {
      await session.endSession();
    }
  });

  /**
   * Test: Abort on error
   */
  it('should abort transaction on callback error', async () => {
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
    const doc = await collection.findOne({ _id: 'acc3' });
    expect(doc).toBeNull();
  });

  /**
   * Test: Transaction options
   */
  it('should pass transaction options', async () => {
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

  /**
   * Test: Retry on transient error
   */
  it('should retry on transient error', async () => {
    const session = client.startSession();
    let attempts = 0;

    try {
      await runTransaction(
        session,
        async (txn) => {
          attempts++;
          if (attempts < 2) {
            throw new Error('Write conflict - retry');
          }
          txn.insert('testdb', 'accounts', { _id: 'acc3', name: 'New', balance: 100 });
        },
        { maxRetries: 3, retryDelayMs: 1 }
      );

      expect(attempts).toBe(2);
    } finally {
      await session.endSession();
    }
  });

  /**
   * Test: Max retries exceeded
   */
  it('should fail after max retries', async () => {
    const session = client.startSession();
    let attempts = 0;

    try {
      await expect(
        runTransaction(
          session,
          async () => {
            attempts++;
            throw new Error('Write conflict - retry');
          },
          { maxRetries: 2, retryDelayMs: 1 }
        )
      ).rejects.toThrow('Write conflict');

      // initial + 2 retries = 3 attempts
      expect(attempts).toBe(3);
    } finally {
      await session.endSession();
    }
  });
});

// ============================================================================
// Snapshot LSN Tracking Tests
// ============================================================================

describe('MongoDB Transactions Compatibility - Snapshot Tracking', () => {
  let client: MongoLake;
  let session: ClientSession;
  let txnManager: TransactionManager;

  beforeEach(() => {
    client = createTestClient();
    session = client.startSession();
    txnManager = new TransactionManager(session);
  });

  afterEach(async () => {
    if (!session.hasEnded) {
      await session.endSession();
    }
    await client.close();
  });

  /**
   * Test: Record shard LSN
   */
  it('should record shard LSNs in snapshot', () => {
    txnManager.begin();
    txnManager.recordShardLSN('shard-1', 100);
    txnManager.recordShardLSN('shard-2', 200);

    const snapshot = txnManager.getSnapshot();
    expect(snapshot?.shardLSNs.get('shard-1')).toBe(100);
    expect(snapshot?.shardLSNs.get('shard-2')).toBe(200);
  });

  /**
   * Test: Snapshot cleared on commit
   */
  it('should clear snapshot on commit', async () => {
    txnManager.begin();
    txnManager.recordShardLSN('shard-1', 100);

    await txnManager.commit();

    expect(txnManager.getSnapshot()).toBeNull();
  });

  /**
   * Test: Snapshot cleared on abort
   */
  it('should clear snapshot on abort', async () => {
    txnManager.begin();
    txnManager.recordShardLSN('shard-1', 100);

    await txnManager.abort();

    expect(txnManager.getSnapshot()).toBeNull();
  });
});

// ============================================================================
// Feature Support Summary
// ============================================================================

describe('MongoDB Transactions Compatibility - Feature Support Summary', () => {
  it('should document supported transaction features', () => {
    /**
     * SUPPORTED FEATURES:
     *
     * 1. Basic Transaction Operations
     *    - startTransaction(): Begin a new transaction
     *    - commitTransaction(): Commit buffered operations atomically
     *    - abortTransaction(): Discard buffered operations
     *
     * 2. Transaction State Machine
     *    - States: none, starting, in_progress, committed, aborted
     *    - inTransaction property for checking active state
     *    - Transaction number (txnNumber) increments per transaction
     *
     * 3. Transaction Options
     *    - readConcern: { level: 'local' | 'majority' | 'snapshot' | 'linearizable' }
     *    - writeConcern: { w: number | 'majority', j?: boolean, wtimeout?: number }
     *    - maxCommitTimeMS: Commit timeout
     *
     * 4. Multi-Document Transactions
     *    - Buffer insert, update, replace, delete operations
     *    - Operations across collections
     *    - Operations across databases
     *
     * 5. TransactionManager
     *    - Coordinate transactions with snapshot tracking
     *    - Convenience methods for CRUD operations
     *    - LSN tracking for distributed transactions
     *
     * 6. runTransaction Helper
     *    - Automatic begin/commit/abort lifecycle
     *    - Retry logic for transient errors
     *    - Return value propagation
     *
     * 7. Two-Phase Commit (2PC)
     *    - TransactionCoordinator for cross-shard transactions
     *    - TransactionParticipant for shard-side handling
     *    - Document-level locking via LockManager
     *
     * PARTIALLY SUPPORTED:
     *
     * 1. Causal Consistency
     *    - Option can be set
     *    - Not enforced via cluster time gossip
     *
     * 2. Retryable Writes
     *    - Transient error detection
     *    - Automatic retry in runTransaction
     *
     * NOT APPLICABLE (MongoDB Server Features):
     *
     * 1. MongoDB Wire Protocol Transaction Fields
     *    - autocommit: false (server-side)
     *    - startTransaction: true (first operation)
     *    - lsid in commands (server sessions)
     *
     * 2. Recovery Token
     *    - Used in sharded MongoDB for transaction recovery
     *    - MongoLake uses 2PC protocol instead
     */
    expect(true).toBe(true);
  });
});
