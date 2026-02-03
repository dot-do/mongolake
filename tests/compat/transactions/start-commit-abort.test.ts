/**
 * MongoDB Transaction Start/Commit/Abort Tests
 *
 * Based on MongoDB Driver Specifications:
 * - commit.json
 * - abort.json
 *
 * Tests the core transaction lifecycle operations.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  ClientSession,
  TransactionError,
} from '../../../src/session/index.js';
import { TransactionManager } from '../../../src/transaction/index.js';
import { MongoLake } from '../../../src/client/index.js';
import { MemoryStorage } from '../../../src/storage/index.js';

function createTestClient(): MongoLake {
  const storage = new MemoryStorage();
  const client = new MongoLake({ local: '.test-mongolake' });
  // @ts-expect-error - accessing private field for testing
  client.storage = storage;
  return client;
}

describe('startTransaction - commit.json specification', () => {
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
   * MongoDB Spec: "commit" test - Basic transaction start
   */
  it('should start a transaction', () => {
    session.startTransaction();

    expect(session.inTransaction).toBe(true);
  });

  /**
   * MongoDB Spec: Transaction state after start
   */
  it('should set transaction state to "starting"', () => {
    session.startTransaction();

    expect(session.transactionState).toBe('starting');
  });

  /**
   * MongoDB Spec: Transaction number assignment
   */
  it('should assign transaction number', () => {
    expect(session.txnNumber).toBe(0);

    session.startTransaction();

    expect(session.txnNumber).toBe(1);
  });

  /**
   * MongoDB Spec: Cannot start nested transactions
   */
  it('should reject starting transaction when one is active', () => {
    session.startTransaction();

    expect(() => session.startTransaction()).toThrow(TransactionError);
    expect(() => session.startTransaction()).toThrow('Transaction already in progress');
  });

  /**
   * MongoDB Spec: Start after commit
   */
  it('should allow starting new transaction after commit', async () => {
    session.startTransaction();
    await session.commitTransaction();

    // Should be able to start a new transaction
    session.startTransaction();

    expect(session.inTransaction).toBe(true);
    expect(session.txnNumber).toBe(2);
  });

  /**
   * MongoDB Spec: Start after abort
   */
  it('should allow starting new transaction after abort', async () => {
    session.startTransaction();
    await session.abortTransaction();

    // Should be able to start a new transaction
    session.startTransaction();

    expect(session.inTransaction).toBe(true);
    expect(session.txnNumber).toBe(2);
  });
});

describe('commitTransaction - commit.json specification', () => {
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
   * MongoDB Spec: "commit" test - Basic commit
   */
  it('should commit a transaction', async () => {
    session.startTransaction();
    session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '1' },
    });

    await session.commitTransaction();

    expect(session.transactionState).toBe('committed');
  });

  /**
   * MongoDB Spec: Transaction no longer active after commit
   */
  it('should end transaction after commit', async () => {
    session.startTransaction();
    await session.commitTransaction();

    expect(session.inTransaction).toBe(false);
  });

  /**
   * MongoDB Spec: "empty transaction" - Commit without operations
   */
  it('should commit empty transaction', async () => {
    session.startTransaction();

    await session.commitTransaction();

    expect(session.transactionState).toBe('committed');
  });

  /**
   * MongoDB Spec: "commit without start" error
   */
  it('should reject commit without active transaction', async () => {
    await expect(session.commitTransaction()).rejects.toThrow(TransactionError);
    await expect(session.commitTransaction()).rejects.toThrow('No transaction in progress');
  });

  /**
   * MongoDB Spec: Cannot commit after commit
   */
  it('should reject commit after already committed', async () => {
    session.startTransaction();
    await session.commitTransaction();

    await expect(session.commitTransaction()).rejects.toThrow(TransactionError);
  });

  /**
   * MongoDB Spec: Cannot commit after abort
   */
  it('should reject commit after abort', async () => {
    session.startTransaction();
    await session.abortTransaction();

    await expect(session.commitTransaction()).rejects.toThrow(TransactionError);
  });

  /**
   * MongoDB Spec: Operations cleared after commit
   */
  it('should clear operations after commit', async () => {
    session.startTransaction();
    session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '1' },
    });

    expect(session.operationCount).toBe(1);

    await session.commitTransaction();

    expect(session.operationCount).toBe(0);
  });
});

describe('abortTransaction - abort.json specification', () => {
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
   * MongoDB Spec: "abort" test - Basic abort
   */
  it('should abort a transaction', async () => {
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
   * MongoDB Spec: Transaction no longer active after abort
   */
  it('should end transaction after abort', async () => {
    session.startTransaction();
    await session.abortTransaction();

    expect(session.inTransaction).toBe(false);
  });

  /**
   * MongoDB Spec: "abort without start" error
   */
  it('should reject abort without active transaction', async () => {
    await expect(session.abortTransaction()).rejects.toThrow(TransactionError);
    await expect(session.abortTransaction()).rejects.toThrow('No transaction in progress');
  });

  /**
   * MongoDB Spec: "two aborts" - Cannot abort twice
   * Note: MongoLake throws instead of ignoring second abort
   */
  it('should reject abort after already aborted', async () => {
    session.startTransaction();
    await session.abortTransaction();

    await expect(session.abortTransaction()).rejects.toThrow(TransactionError);
  });

  /**
   * MongoDB Spec: "abort directly after commit" error
   */
  it('should reject abort after commit', async () => {
    session.startTransaction();
    await session.commitTransaction();

    await expect(session.abortTransaction()).rejects.toThrow(TransactionError);
  });

  /**
   * MongoDB Spec: Operations discarded on abort
   */
  it('should discard all operations on abort', async () => {
    session.startTransaction();
    session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '1' },
    });
    session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '2' },
    });

    expect(session.operationCount).toBe(2);

    await session.abortTransaction();

    expect(session.operationCount).toBe(0);
  });

  /**
   * MongoDB Spec: "implicit abort" - Session end aborts active transaction
   */
  it('should abort transaction when session ends', async () => {
    session.startTransaction();
    session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '1' },
    });

    await session.endSession();

    expect(session.transactionState).toBe('aborted');
    expect(session.hasEnded).toBe(true);
  });
});

describe('Sequential Transactions - commit.json specification', () => {
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
   * MongoDB Spec: Multiple sequential transactions
   */
  it('should support multiple sequential transactions', async () => {
    // First transaction
    session.startTransaction();
    session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '1' },
    });
    await session.commitTransaction();
    expect(session.txnNumber).toBe(1);

    // Second transaction
    session.startTransaction();
    session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '2' },
    });
    await session.commitTransaction();
    expect(session.txnNumber).toBe(2);

    // Third transaction
    session.startTransaction();
    session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '3' },
    });
    await session.commitTransaction();
    expect(session.txnNumber).toBe(3);
  });

  /**
   * MongoDB Spec: Transaction after abort followed by transaction
   */
  it('should support transaction after abort', async () => {
    // First transaction - abort
    session.startTransaction();
    session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '1' },
    });
    await session.abortTransaction();
    expect(session.txnNumber).toBe(1);

    // Second transaction - commit
    session.startTransaction();
    session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '2' },
    });
    await session.commitTransaction();
    expect(session.txnNumber).toBe(2);
  });

  /**
   * Test alternating commit and abort
   */
  it('should handle alternating commit and abort', async () => {
    session.startTransaction();
    await session.commitTransaction();

    session.startTransaction();
    await session.abortTransaction();

    session.startTransaction();
    await session.commitTransaction();

    session.startTransaction();
    await session.abortTransaction();

    expect(session.txnNumber).toBe(4);
  });
});

describe('TransactionManager Lifecycle', () => {
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

  /**
   * TransactionManager begin
   */
  it('should begin transaction via manager', () => {
    txn.begin();

    expect(txn.inTransaction).toBe(true);
    expect(session.transactionState).toBe('starting');
  });

  /**
   * TransactionManager commit
   */
  it('should commit transaction via manager', async () => {
    txn.begin();
    txn.insert('testdb', 'test', { _id: '1' });

    const result = await txn.commit();

    expect(result.success).toBe(true);
    expect(result.operationCount).toBe(1);
    expect(txn.inTransaction).toBe(false);
  });

  /**
   * TransactionManager abort
   */
  it('should abort transaction via manager', async () => {
    txn.begin();
    txn.insert('testdb', 'test', { _id: '1' });

    await txn.abort();

    expect(txn.inTransaction).toBe(false);
    expect(session.transactionState).toBe('aborted');
  });

  /**
   * TransactionManager commit result
   */
  it('should return commit result with timing', async () => {
    const before = Date.now();

    txn.begin();
    txn.insert('testdb', 'test', { _id: '1' });

    const result = await txn.commit();

    expect(result.commitTime).toBeGreaterThanOrEqual(before);
    expect(result.commitTime).toBeLessThanOrEqual(Date.now());
  });
});
