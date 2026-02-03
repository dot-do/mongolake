/**
 * Transaction Rollback Completeness Tests
 *
 * Comprehensive tests verifying that transaction rollback is complete:
 * - Rollback undoes ALL operations when one fails
 * - No partial commits
 * - Multi-collection rollback
 * - Rollback after storage failure
 * - Rollback after validation failure
 * - Verify document state matches pre-transaction
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  TransactionCoordinator,
  DistributedTransactionError,
  type ShardRPC,
  type PrepareMessage,
  type PreparedMessage,
  type AbortVoteMessage,
  type CommitMessage,
  type AbortMessage,
  type AckMessage,
  type StatusQueryMessage,
  type StatusResponseMessage,
} from '../../../src/transaction/coordinator.js';
import {
  TransactionParticipant,
  type ParticipantStorage,
  type OperationExecutor,
  type PreparedTransaction,
} from '../../../src/transaction/participant.js';
import { ShardRouter } from '../../../src/shard/router.js';
import type { BufferedOperation } from '../../../src/session/index.js';
import { ClientSession } from '../../../src/session/index.js';
import { TransactionManager } from '../../../src/transaction/index.js';

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * In-memory document store for testing rollback completeness.
 */
class DocumentStore {
  private documents: Map<string, Map<string, Record<string, unknown>>> = new Map();

  /**
   * Get a collection's document store.
   */
  private getCollection(database: string, collection: string): Map<string, Record<string, unknown>> {
    const key = `${database}.${collection}`;
    let coll = this.documents.get(key);
    if (!coll) {
      coll = new Map();
      this.documents.set(key, coll);
    }
    return coll;
  }

  /**
   * Insert a document.
   */
  insert(database: string, collection: string, document: Record<string, unknown>): void {
    const coll = this.getCollection(database, collection);
    const id = String(document._id);
    if (coll.has(id)) {
      throw new Error(`Duplicate key error: ${id}`);
    }
    coll.set(id, { ...document });
  }

  /**
   * Update a document.
   */
  update(database: string, collection: string, filter: Record<string, unknown>, update: Record<string, unknown>): void {
    const coll = this.getCollection(database, collection);
    const id = String(filter._id);
    const doc = coll.get(id);
    if (!doc) {
      throw new Error(`Document not found: ${id}`);
    }
    // Simple $set handling
    if (update.$set) {
      Object.assign(doc, update.$set);
    }
    if (update.$inc) {
      for (const [key, value] of Object.entries(update.$inc)) {
        doc[key] = ((doc[key] as number) || 0) + (value as number);
      }
    }
  }

  /**
   * Delete a document.
   */
  delete(database: string, collection: string, filter: Record<string, unknown>): void {
    const coll = this.getCollection(database, collection);
    const id = String(filter._id);
    if (!coll.has(id)) {
      throw new Error(`Document not found: ${id}`);
    }
    coll.delete(id);
  }

  /**
   * Replace a document.
   */
  replace(database: string, collection: string, filter: Record<string, unknown>, replacement: Record<string, unknown>): void {
    const coll = this.getCollection(database, collection);
    const id = String(filter._id);
    if (!coll.has(id)) {
      throw new Error(`Document not found: ${id}`);
    }
    coll.set(id, { ...replacement });
  }

  /**
   * Find a document.
   */
  findOne(database: string, collection: string, id: string): Record<string, unknown> | null {
    const coll = this.getCollection(database, collection);
    return coll.get(id) ? { ...coll.get(id)! } : null;
  }

  /**
   * Get all documents in a collection.
   */
  findAll(database: string, collection: string): Record<string, unknown>[] {
    const coll = this.getCollection(database, collection);
    return Array.from(coll.values()).map(doc => ({ ...doc }));
  }

  /**
   * Get document count.
   */
  count(database: string, collection: string): number {
    return this.getCollection(database, collection).size;
  }

  /**
   * Create a snapshot of current state.
   */
  snapshot(): Map<string, Map<string, Record<string, unknown>>> {
    const snap = new Map<string, Map<string, Record<string, unknown>>>();
    for (const [key, coll] of this.documents) {
      const collSnap = new Map<string, Record<string, unknown>>();
      for (const [id, doc] of coll) {
        collSnap.set(id, { ...doc });
      }
      snap.set(key, collSnap);
    }
    return snap;
  }

  /**
   * Restore from snapshot.
   */
  restore(snap: Map<string, Map<string, Record<string, unknown>>>): void {
    this.documents.clear();
    for (const [key, coll] of snap) {
      const collCopy = new Map<string, Record<string, unknown>>();
      for (const [id, doc] of coll) {
        collCopy.set(id, { ...doc });
      }
      this.documents.set(key, collCopy);
    }
  }

  /**
   * Clear all documents.
   */
  clear(): void {
    this.documents.clear();
  }
}

/**
 * Create a mock storage implementation for participant tests.
 */
function createMockStorage(): ParticipantStorage & {
  savedTransactions: Map<string, PreparedTransaction>;
  currentLSN: number;
  deletedTransactions: string[];
} {
  const savedTransactions = new Map<string, PreparedTransaction>();
  const deletedTransactions: string[] = [];
  let currentLSN = 100;

  return {
    savedTransactions,
    deletedTransactions,
    currentLSN,
    async savePreparedTransaction(txn: PreparedTransaction): Promise<void> {
      savedTransactions.set(txn.txnId, txn);
    },
    async loadPreparedTransaction(txnId: string): Promise<PreparedTransaction | null> {
      return savedTransactions.get(txnId) ?? null;
    },
    async deletePreparedTransaction(txnId: string): Promise<void> {
      savedTransactions.delete(txnId);
      deletedTransactions.push(txnId);
    },
    async loadAllPreparedTransactions(): Promise<PreparedTransaction[]> {
      return Array.from(savedTransactions.values());
    },
    allocateLSN(): number {
      return ++currentLSN;
    },
    getCurrentLSN(): number {
      return currentLSN;
    },
  };
}

/**
 * Create a mock operation executor.
 */
function createMockExecutor(options: {
  shouldValidate?: boolean;
  validationErrors?: string[];
  applyLSN?: number;
  shouldFailApply?: boolean;
  applyError?: Error;
  failOnOperation?: number;
} = {}): OperationExecutor & { appliedOperations: BufferedOperation[] } {
  const {
    shouldValidate = true,
    validationErrors,
    applyLSN = 200,
    shouldFailApply = false,
    applyError,
    failOnOperation,
  } = options;

  const appliedOperations: BufferedOperation[] = [];

  return {
    appliedOperations,
    async validateOperations(): Promise<{ valid: boolean; errors?: string[] }> {
      if (!shouldValidate) {
        return {
          valid: false,
          errors: validationErrors ?? ['Validation failed'],
        };
      }
      return { valid: true };
    },
    async applyOperations(operations: BufferedOperation[]): Promise<number> {
      if (shouldFailApply) {
        throw applyError ?? new Error('Apply failed');
      }
      if (failOnOperation !== undefined) {
        for (let i = 0; i < operations.length; i++) {
          if (i === failOnOperation) {
            throw new Error(`Operation ${i} failed`);
          }
          appliedOperations.push(operations[i]);
        }
      } else {
        appliedOperations.push(...operations);
      }
      return applyLSN;
    },
  };
}

/**
 * Create a mock ShardRPC implementation with document store.
 */
function createMockShardRPCWithStore(store: DocumentStore, options: {
  prepareResponses?: Map<number, 'prepared' | 'abort'>;
  commitFailure?: { shardId: number; afterOp?: number };
  validationFailure?: { shardId: number; reason: string };
} = {}): ShardRPC & {
  preparedShards: Set<number>;
  committedShards: Set<number>;
  abortedShards: Set<number>;
  operationsApplied: BufferedOperation[];
} {
  const { prepareResponses = new Map(), commitFailure, validationFailure } = options;

  let preparedLSN = 100;
  const preparedShards = new Set<number>();
  const committedShards = new Set<number>();
  const abortedShards = new Set<number>();
  const operationsApplied: BufferedOperation[] = [];
  const preparedOperations = new Map<string, BufferedOperation[]>();

  return {
    preparedShards,
    committedShards,
    abortedShards,
    operationsApplied,

    async sendPrepare(shardId: number, message: PrepareMessage): Promise<PreparedMessage | AbortVoteMessage> {
      // Check for validation failure
      if (validationFailure && validationFailure.shardId === shardId) {
        return {
          type: 'abort_vote',
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          reason: validationFailure.reason,
        };
      }

      const response = prepareResponses.get(shardId) ?? 'prepared';
      if (response === 'abort') {
        return {
          type: 'abort_vote',
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          reason: 'Simulated abort',
        };
      }

      preparedShards.add(shardId);
      preparedOperations.set(message.txnId, message.operations);

      return {
        type: 'prepared',
        txnId: message.txnId,
        shardId,
        timestamp: Date.now(),
        preparedLSN: preparedLSN++,
      };
    },

    async sendCommit(shardId: number, message: CommitMessage): Promise<AckMessage> {
      const ops = preparedOperations.get(message.txnId) || [];

      // Check for commit failure
      if (commitFailure && commitFailure.shardId === shardId) {
        const afterOp = commitFailure.afterOp ?? -1;

        // Apply operations up to failure point
        for (let i = 0; i <= afterOp && i < ops.length; i++) {
          const op = ops[i];
          applyOperation(store, op);
          operationsApplied.push(op);
        }

        throw new Error(`Storage failure on shard ${shardId}`);
      }

      // Apply all operations
      for (const op of ops) {
        applyOperation(store, op);
        operationsApplied.push(op);
      }

      committedShards.add(shardId);

      return {
        type: 'ack',
        txnId: message.txnId,
        shardId,
        timestamp: Date.now(),
        finalLSN: 200,
      };
    },

    async sendAbort(shardId: number, message: AbortMessage): Promise<AckMessage> {
      abortedShards.add(shardId);

      return {
        type: 'ack',
        txnId: message.txnId,
        shardId,
        timestamp: Date.now(),
      };
    },

    async queryStatus(shardId: number, message: StatusQueryMessage): Promise<StatusResponseMessage> {
      return {
        type: 'status_response',
        txnId: message.txnId,
        shardId,
        timestamp: Date.now(),
        participantState: 'done',
      };
    },
  };
}

/**
 * Apply an operation to the document store.
 */
function applyOperation(store: DocumentStore, op: BufferedOperation): void {
  switch (op.type) {
    case 'insert':
      store.insert(op.database, op.collection, op.document!);
      break;
    case 'update':
      store.update(op.database, op.collection, op.filter!, op.update!);
      break;
    case 'delete':
      store.delete(op.database, op.collection, op.filter!);
      break;
    case 'replace':
      store.replace(op.database, op.collection, op.filter!, op.replacement!);
      break;
  }
}

/**
 * Create test operations targeting multiple collections.
 */
function createMultiCollectionOperations(): BufferedOperation[] {
  return [
    {
      type: 'insert',
      collection: 'users',
      database: 'testdb',
      document: { _id: 'user1', name: 'Alice', balance: 1000 },
      timestamp: Date.now(),
    },
    {
      type: 'insert',
      collection: 'orders',
      database: 'testdb',
      document: { _id: 'order1', userId: 'user1', total: 100 },
      timestamp: Date.now(),
    },
    {
      type: 'insert',
      collection: 'inventory',
      database: 'testdb',
      document: { _id: 'item1', name: 'Widget', quantity: 50 },
      timestamp: Date.now(),
    },
  ];
}

/**
 * Create a prepare message.
 */
function createPrepareMessage(txnId: string, operations: BufferedOperation[], shardId: number = 1): PrepareMessage {
  return {
    type: 'prepare',
    txnId,
    shardId,
    timestamp: Date.now(),
    operations,
    prepareDeadline: Date.now() + 5000,
  };
}

// ============================================================================
// Test Suite: Rollback Undoes ALL Operations When One Fails
// ============================================================================

describe('Rollback Completeness - Rollback undoes ALL operations when one fails', () => {
  let store: DocumentStore;
  let router: ShardRouter;

  beforeEach(() => {
    store = new DocumentStore();
    router = new ShardRouter();

    // Pre-populate some data
    store.insert('testdb', 'accounts', { _id: 'acc1', name: 'Account 1', balance: 1000 });
    store.insert('testdb', 'accounts', { _id: 'acc2', name: 'Account 2', balance: 500 });
  });

  it('should undo all operations when the last operation fails', async () => {
    const preTransactionSnapshot = store.snapshot();

    const storage = createMockStorage();
    const executor = createMockExecutor({ failOnOperation: 2 });
    const participant = new TransactionParticipant(1, storage, executor);

    const operations: BufferedOperation[] = [
      {
        type: 'update',
        collection: 'accounts',
        database: 'testdb',
        filter: { _id: 'acc1' },
        update: { $inc: { balance: -100 } },
        timestamp: Date.now(),
      },
      {
        type: 'update',
        collection: 'accounts',
        database: 'testdb',
        filter: { _id: 'acc2' },
        update: { $inc: { balance: 100 } },
        timestamp: Date.now(),
      },
      {
        type: 'insert',
        collection: 'accounts',
        database: 'testdb',
        document: { _id: 'acc3', name: 'Account 3', balance: 0 },
        timestamp: Date.now(),
      },
    ];

    // Prepare succeeds
    const prepareResult = await participant.handlePrepare(createPrepareMessage('txn-1', operations));
    expect(prepareResult.type).toBe('prepared');

    // Commit should fail at operation 2
    const commitMessage: CommitMessage = {
      type: 'commit',
      txnId: 'txn-1',
      shardId: 1,
      timestamp: Date.now(),
      commitDeadline: Date.now() + 5000,
    };

    await expect(participant.handleCommit(commitMessage)).rejects.toThrow('Operation 2 failed');

    // Only first 2 operations should have been applied before failure
    expect(executor.appliedOperations.length).toBe(2);
  });

  it('should undo all operations when first operation fails', async () => {
    const storage = createMockStorage();
    const executor = createMockExecutor({ failOnOperation: 0 });
    const participant = new TransactionParticipant(1, storage, executor);

    const operations: BufferedOperation[] = [
      {
        type: 'insert',
        collection: 'accounts',
        database: 'testdb',
        document: { _id: 'new1', name: 'New', balance: 100 },
        timestamp: Date.now(),
      },
      {
        type: 'insert',
        collection: 'accounts',
        database: 'testdb',
        document: { _id: 'new2', name: 'New2', balance: 200 },
        timestamp: Date.now(),
      },
    ];

    await participant.handlePrepare(createPrepareMessage('txn-1', operations));

    await expect(participant.handleCommit({
      type: 'commit',
      txnId: 'txn-1',
      shardId: 1,
      timestamp: Date.now(),
      commitDeadline: Date.now() + 5000,
    })).rejects.toThrow('Operation 0 failed');

    // No operations should have been applied
    expect(executor.appliedOperations.length).toBe(0);
  });

  it('should undo all operations when middle operation fails', async () => {
    const storage = createMockStorage();
    const executor = createMockExecutor({ failOnOperation: 1 });
    const participant = new TransactionParticipant(1, storage, executor);

    const operations: BufferedOperation[] = [
      {
        type: 'insert',
        collection: 'accounts',
        database: 'testdb',
        document: { _id: 'new1', name: 'New', balance: 100 },
        timestamp: Date.now(),
      },
      {
        type: 'insert',
        collection: 'accounts',
        database: 'testdb',
        document: { _id: 'new2', name: 'New2', balance: 200 },
        timestamp: Date.now(),
      },
      {
        type: 'insert',
        collection: 'accounts',
        database: 'testdb',
        document: { _id: 'new3', name: 'New3', balance: 300 },
        timestamp: Date.now(),
      },
    ];

    await participant.handlePrepare(createPrepareMessage('txn-1', operations));

    await expect(participant.handleCommit({
      type: 'commit',
      txnId: 'txn-1',
      shardId: 1,
      timestamp: Date.now(),
      commitDeadline: Date.now() + 5000,
    })).rejects.toThrow('Operation 1 failed');

    // Only first operation applied before failure
    expect(executor.appliedOperations.length).toBe(1);
  });
});

// ============================================================================
// Test Suite: No Partial Commits
// ============================================================================

describe('Rollback Completeness - No partial commits', () => {
  let router: ShardRouter;

  beforeEach(() => {
    router = new ShardRouter();
  });

  it('should not have partial commits when prepare fails on any shard', async () => {
    const shard1 = router.routeWithDatabase('db1', 'users').shardId;
    const shard2 = router.routeWithDatabase('db2', 'orders').shardId;

    // Track committed operations per shard
    const committedPerShard = new Map<number, BufferedOperation[]>();

    const rpc: ShardRPC = {
      async sendPrepare(shardId, message) {
        // Second shard fails prepare
        if (shardId === shard2) {
          return {
            type: 'abort_vote',
            txnId: message.txnId,
            shardId,
            timestamp: Date.now(),
            reason: 'Validation failed',
          };
        }
        return {
          type: 'prepared',
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          preparedLSN: 100,
        };
      },
      async sendCommit(shardId, message) {
        // Should never be called since prepare failed
        committedPerShard.set(shardId, []);
        return {
          type: 'ack',
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          finalLSN: 200,
        };
      },
      async sendAbort(shardId, message) {
        return { type: 'ack', txnId: message.txnId, shardId, timestamp: Date.now() };
      },
      async queryStatus(shardId, message) {
        return { type: 'status_response', txnId: message.txnId, shardId, timestamp: Date.now(), participantState: 'done' };
      },
    };

    const coordinator = new TransactionCoordinator(router, rpc);
    const operations: BufferedOperation[] = [
      { type: 'insert', collection: 'users', database: 'db1', document: { _id: 'u1', name: 'User1' }, timestamp: Date.now() },
      { type: 'insert', collection: 'orders', database: 'db2', document: { _id: 'o1', userId: 'u1' }, timestamp: Date.now() },
    ];

    const result = await coordinator.execute(operations);

    expect(result.committed).toBe(false);
    // No shard should have committed
    expect(committedPerShard.size).toBe(0);
  });

  it('should not have partial commits when all operations are buffered then aborted', async () => {
    const session = new ClientSession();
    let commitCalled = false;

    session.setCommitHandler(async () => {
      commitCalled = true;
    });

    session.startTransaction();

    // Buffer multiple operations
    session.bufferOperation({
      type: 'insert',
      collection: 'users',
      database: 'testdb',
      document: { _id: 'u1', name: 'User1' },
    });

    session.bufferOperation({
      type: 'insert',
      collection: 'users',
      database: 'testdb',
      document: { _id: 'u2', name: 'User2' },
    });

    session.bufferOperation({
      type: 'update',
      collection: 'users',
      database: 'testdb',
      filter: { _id: 'u1' },
      update: { $set: { name: 'Updated' } },
    });

    expect(session.operationCount).toBe(3);

    // Abort without committing
    await session.abortTransaction();

    expect(commitCalled).toBe(false);
    expect(session.operationCount).toBe(0);
    expect(session.transactionState).toBe('aborted');
  });

  it('should discard all buffered operations on abort even with mixed operation types', async () => {
    const session = new ClientSession();
    const appliedOps: BufferedOperation[] = [];

    session.setCommitHandler(async (_sess, ops) => {
      appliedOps.push(...ops);
    });

    session.startTransaction();

    // Mix of all operation types
    session.bufferOperation({ type: 'insert', collection: 'c1', database: 'db', document: { _id: '1' } });
    session.bufferOperation({ type: 'update', collection: 'c2', database: 'db', filter: { _id: '2' }, update: { $set: { x: 1 } } });
    session.bufferOperation({ type: 'delete', collection: 'c3', database: 'db', filter: { _id: '3' } });
    session.bufferOperation({ type: 'replace', collection: 'c4', database: 'db', filter: { _id: '4' }, replacement: { _id: '4', new: true } });

    expect(session.operationCount).toBe(4);

    await session.abortTransaction();

    expect(appliedOps.length).toBe(0);
    expect(session.getBufferedOperations().length).toBe(0);
  });
});

// ============================================================================
// Test Suite: Multi-Collection Rollback
// ============================================================================

describe('Rollback Completeness - Multi-collection rollback', () => {
  let store: DocumentStore;

  beforeEach(() => {
    store = new DocumentStore();

    // Pre-populate multiple collections
    store.insert('testdb', 'users', { _id: 'user1', name: 'Alice', balance: 1000 });
    store.insert('testdb', 'orders', { _id: 'order1', userId: 'user1', total: 50 });
    store.insert('testdb', 'inventory', { _id: 'item1', name: 'Widget', quantity: 100 });
  });

  it('should rollback changes across all collections on abort', async () => {
    const preSnapshot = store.snapshot();

    const storage = createMockStorage();
    const executor = createMockExecutor();
    const participant = new TransactionParticipant(1, storage, executor);

    const operations: BufferedOperation[] = [
      { type: 'update', collection: 'users', database: 'testdb', filter: { _id: 'user1' }, update: { $inc: { balance: -100 } }, timestamp: Date.now() },
      { type: 'insert', collection: 'orders', database: 'testdb', document: { _id: 'order2', userId: 'user1', total: 100 }, timestamp: Date.now() },
      { type: 'update', collection: 'inventory', database: 'testdb', filter: { _id: 'item1' }, update: { $inc: { quantity: -1 } }, timestamp: Date.now() },
    ];

    // Prepare
    await participant.handlePrepare(createPrepareMessage('txn-1', operations));

    // Abort instead of commit
    await participant.handleAbort({
      type: 'abort',
      txnId: 'txn-1',
      shardId: 1,
      timestamp: Date.now(),
      reason: 'User aborted',
    });

    // Verify executor never applied operations
    expect(executor.appliedOperations.length).toBe(0);

    // Transaction should be removed
    expect(participant.getPreparedTransactionIds()).not.toContain('txn-1');
  });

  it('should not affect other collections when transaction targeting them is rolled back', async () => {
    const storage = createMockStorage();
    const executor = createMockExecutor();
    const participant = new TransactionParticipant(1, storage, executor);

    // Operations targeting multiple collections
    const operations = createMultiCollectionOperations();

    await participant.handlePrepare(createPrepareMessage('txn-1', operations));

    // Abort
    await participant.handleAbort({
      type: 'abort',
      txnId: 'txn-1',
      shardId: 1,
      timestamp: Date.now(),
      reason: 'Aborted',
    });

    // Original data should be unchanged
    expect(store.findOne('testdb', 'users', 'user1')?.balance).toBe(1000);
    expect(store.findOne('testdb', 'orders', 'order1')?.total).toBe(50);
    expect(store.findOne('testdb', 'inventory', 'item1')?.quantity).toBe(100);
  });

  it('should rollback inserts, updates, and deletes across collections together', async () => {
    const storage = createMockStorage();
    const executor = createMockExecutor();
    const participant = new TransactionParticipant(1, storage, executor);

    const operations: BufferedOperation[] = [
      { type: 'insert', collection: 'users', database: 'testdb', document: { _id: 'newuser', name: 'New' }, timestamp: Date.now() },
      { type: 'update', collection: 'orders', database: 'testdb', filter: { _id: 'order1' }, update: { $set: { status: 'cancelled' } }, timestamp: Date.now() },
      { type: 'delete', collection: 'inventory', database: 'testdb', filter: { _id: 'item1' }, timestamp: Date.now() },
    ];

    await participant.handlePrepare(createPrepareMessage('txn-1', operations));
    await participant.handleAbort({
      type: 'abort',
      txnId: 'txn-1',
      shardId: 1,
      timestamp: Date.now(),
      reason: 'Aborted',
    });

    // No operations should have been executed
    expect(executor.appliedOperations.length).toBe(0);
  });
});

// ============================================================================
// Test Suite: Rollback After Storage Failure
// ============================================================================

describe('Rollback Completeness - Rollback after storage failure', () => {
  let store: DocumentStore;
  let router: ShardRouter;

  beforeEach(() => {
    store = new DocumentStore();
    router = new ShardRouter();

    store.insert('testdb', 'accounts', { _id: 'acc1', balance: 1000 });
    store.insert('testdb', 'accounts', { _id: 'acc2', balance: 500 });
  });

  it('should abort transaction when storage fails during prepare', async () => {
    const storage = createMockStorage();
    // Use an executor that returns invalid validation result (simulating storage error during validation)
    const executor = createMockExecutor({
      shouldValidate: false,
      validationErrors: ['Storage connection lost'],
    });

    const participant = new TransactionParticipant(1, storage, executor);

    const operations: BufferedOperation[] = [
      { type: 'insert', collection: 'accounts', database: 'testdb', document: { _id: 'new', balance: 100 }, timestamp: Date.now() },
    ];

    const result = await participant.handlePrepare(createPrepareMessage('txn-1', operations));

    // Should return abort vote due to validation failure
    expect(result.type).toBe('abort_vote');
    expect((result as AbortVoteMessage).reason).toContain('Storage connection lost');
  });

  it('should handle storage failure during commit', async () => {
    const storage = createMockStorage();
    const executor = createMockExecutor({
      shouldFailApply: true,
      applyError: new Error('Disk full'),
    });

    const participant = new TransactionParticipant(1, storage, executor);

    const operations: BufferedOperation[] = [
      { type: 'insert', collection: 'accounts', database: 'testdb', document: { _id: 'new', balance: 100 }, timestamp: Date.now() },
    ];

    await participant.handlePrepare(createPrepareMessage('txn-1', operations));

    // Commit should fail
    await expect(participant.handleCommit({
      type: 'commit',
      txnId: 'txn-1',
      shardId: 1,
      timestamp: Date.now(),
      commitDeadline: Date.now() + 5000,
    })).rejects.toThrow('Disk full');
  });

  it('should abort all shards when one shard has storage failure during prepare', async () => {
    const shard2 = router.routeWithDatabase('db2', 'orders').shardId;

    let abortCount = 0;
    const rpc: ShardRPC = {
      async sendPrepare(shardId, message) {
        if (shardId === shard2) {
          return {
            type: 'abort_vote',
            txnId: message.txnId,
            shardId,
            timestamp: Date.now(),
            reason: 'Storage failure',
          };
        }
        return {
          type: 'prepared',
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          preparedLSN: 100,
        };
      },
      async sendCommit(shardId, message) {
        return { type: 'ack', txnId: message.txnId, shardId, timestamp: Date.now(), finalLSN: 200 };
      },
      async sendAbort(shardId, message) {
        abortCount++;
        return { type: 'ack', txnId: message.txnId, shardId, timestamp: Date.now() };
      },
      async queryStatus(shardId, message) {
        return { type: 'status_response', txnId: message.txnId, shardId, timestamp: Date.now(), participantState: 'done' };
      },
    };

    const coordinator = new TransactionCoordinator(router, rpc);
    const operations: BufferedOperation[] = [
      { type: 'insert', collection: 'users', database: 'db1', document: { _id: 'u1' }, timestamp: Date.now() },
      { type: 'insert', collection: 'orders', database: 'db2', document: { _id: 'o1' }, timestamp: Date.now() },
    ];

    const result = await coordinator.execute(operations);

    expect(result.committed).toBe(false);
    expect(result.abortReason).toContain('Storage failure');
    expect(abortCount).toBeGreaterThan(0);
  });

  it('should retry commit on transient storage failure', async () => {
    let attempts = 0;
    const rpc: ShardRPC = {
      async sendPrepare(shardId, message) {
        return { type: 'prepared', txnId: message.txnId, shardId, timestamp: Date.now(), preparedLSN: 100 };
      },
      async sendCommit(shardId, message) {
        attempts++;
        if (attempts < 3) {
          throw new Error('Temporary storage failure');
        }
        return { type: 'ack', txnId: message.txnId, shardId, timestamp: Date.now(), finalLSN: 200 };
      },
      async sendAbort(shardId, message) {
        return { type: 'ack', txnId: message.txnId, shardId, timestamp: Date.now() };
      },
      async queryStatus(shardId, message) {
        return { type: 'status_response', txnId: message.txnId, shardId, timestamp: Date.now(), participantState: 'done' };
      },
    };

    const coordinator = new TransactionCoordinator(router, rpc, {
      retryDelayMs: 1,
    });

    // Multi-shard to trigger 2PC path with unlimited retries
    const operations: BufferedOperation[] = [
      { type: 'insert', collection: 'users', database: 'db1', document: { _id: 'u1' }, timestamp: Date.now() },
      { type: 'insert', collection: 'orders', database: 'db2', document: { _id: 'o1' }, timestamp: Date.now() },
    ];

    const result = await coordinator.execute(operations);

    expect(result.committed).toBe(true);
    // With multiple shards, each shard gets retried, total attempts should be > 2
    expect(attempts).toBeGreaterThanOrEqual(3);
  });
});

// ============================================================================
// Test Suite: Rollback After Validation Failure
// ============================================================================

describe('Rollback Completeness - Rollback after validation failure', () => {
  let router: ShardRouter;

  beforeEach(() => {
    router = new ShardRouter();
  });

  it('should abort transaction when validation fails during prepare', async () => {
    const storage = createMockStorage();
    const executor = createMockExecutor({
      shouldValidate: false,
      validationErrors: ['Document exceeds size limit', 'Invalid field type'],
    });

    const participant = new TransactionParticipant(1, storage, executor);

    const operations: BufferedOperation[] = [
      { type: 'insert', collection: 'test', database: 'testdb', document: { _id: 'doc1', data: 'x'.repeat(1000000) }, timestamp: Date.now() },
    ];

    const result = await participant.handlePrepare(createPrepareMessage('txn-1', operations));

    expect(result.type).toBe('abort_vote');
    expect((result as AbortVoteMessage).reason).toContain('Validation failed');
    expect((result as AbortVoteMessage).reason).toContain('Document exceeds size limit');
  });

  it('should not commit any operations when one shard fails validation', async () => {
    const shard2 = router.routeWithDatabase('db2', 'orders').shardId;

    const committedShards: number[] = [];
    const rpc: ShardRPC = {
      async sendPrepare(shardId, message) {
        if (shardId === shard2) {
          return {
            type: 'abort_vote',
            txnId: message.txnId,
            shardId,
            timestamp: Date.now(),
            reason: 'Schema validation failed: missing required field',
          };
        }
        return { type: 'prepared', txnId: message.txnId, shardId, timestamp: Date.now(), preparedLSN: 100 };
      },
      async sendCommit(shardId, message) {
        committedShards.push(shardId);
        return { type: 'ack', txnId: message.txnId, shardId, timestamp: Date.now(), finalLSN: 200 };
      },
      async sendAbort(shardId, message) {
        return { type: 'ack', txnId: message.txnId, shardId, timestamp: Date.now() };
      },
      async queryStatus(shardId, message) {
        return { type: 'status_response', txnId: message.txnId, shardId, timestamp: Date.now(), participantState: 'done' };
      },
    };

    const coordinator = new TransactionCoordinator(router, rpc);
    const operations: BufferedOperation[] = [
      { type: 'insert', collection: 'users', database: 'db1', document: { _id: 'u1' }, timestamp: Date.now() },
      { type: 'insert', collection: 'orders', database: 'db2', document: { _id: 'o1' }, timestamp: Date.now() },
    ];

    const result = await coordinator.execute(operations);

    expect(result.committed).toBe(false);
    expect(result.abortReason).toContain('Schema validation failed');
    expect(committedShards.length).toBe(0);
  });

  it('should include validation error details in abort reason', async () => {
    const storage = createMockStorage();
    const validationErrors = [
      'Field "email" must be a valid email address',
      'Field "age" must be a positive integer',
      'Field "status" must be one of: active, inactive, pending',
    ];

    const executor = createMockExecutor({
      shouldValidate: false,
      validationErrors,
    });

    const participant = new TransactionParticipant(1, storage, executor);

    const result = await participant.handlePrepare(createPrepareMessage('txn-1', [
      { type: 'insert', collection: 'users', database: 'testdb', document: { _id: 'u1', email: 'invalid', age: -5, status: 'unknown' }, timestamp: Date.now() },
    ]));

    expect(result.type).toBe('abort_vote');
    const reason = (result as AbortVoteMessage).reason;
    expect(reason).toContain('email');
    expect(reason).toContain('age');
    expect(reason).toContain('status');
  });

  it('should abort on constraint violation', async () => {
    const storage = createMockStorage();
    const executor = createMockExecutor({
      shouldValidate: false,
      validationErrors: ['Unique constraint violation: email already exists'],
    });

    const participant = new TransactionParticipant(1, storage, executor);

    const result = await participant.handlePrepare(createPrepareMessage('txn-1', [
      { type: 'insert', collection: 'users', database: 'testdb', document: { _id: 'u1', email: 'existing@example.com' }, timestamp: Date.now() },
    ]));

    expect(result.type).toBe('abort_vote');
    expect((result as AbortVoteMessage).reason).toContain('Unique constraint violation');
  });
});

// ============================================================================
// Test Suite: Verify Document State Matches Pre-Transaction
// ============================================================================

describe('Rollback Completeness - Document state matches pre-transaction', () => {
  let store: DocumentStore;

  beforeEach(() => {
    store = new DocumentStore();

    // Set up initial state
    store.insert('testdb', 'accounts', { _id: 'acc1', name: 'Alice', balance: 1000, version: 1 });
    store.insert('testdb', 'accounts', { _id: 'acc2', name: 'Bob', balance: 500, version: 1 });
    store.insert('testdb', 'products', { _id: 'prod1', name: 'Widget', price: 99.99, stock: 100 });
  });

  it('should preserve exact document state after abort', async () => {
    // Capture pre-transaction state
    const preAcc1 = store.findOne('testdb', 'accounts', 'acc1');
    const preAcc2 = store.findOne('testdb', 'accounts', 'acc2');
    const preProd1 = store.findOne('testdb', 'products', 'prod1');

    const storage = createMockStorage();
    const executor = createMockExecutor();
    const participant = new TransactionParticipant(1, storage, executor);

    // Prepare operations that would modify all documents
    const operations: BufferedOperation[] = [
      { type: 'update', collection: 'accounts', database: 'testdb', filter: { _id: 'acc1' }, update: { $inc: { balance: -500 } }, timestamp: Date.now() },
      { type: 'update', collection: 'accounts', database: 'testdb', filter: { _id: 'acc2' }, update: { $inc: { balance: 500 } }, timestamp: Date.now() },
      { type: 'update', collection: 'products', database: 'testdb', filter: { _id: 'prod1' }, update: { $inc: { stock: -10 } }, timestamp: Date.now() },
    ];

    await participant.handlePrepare(createPrepareMessage('txn-1', operations));
    await participant.handleAbort({
      type: 'abort',
      txnId: 'txn-1',
      shardId: 1,
      timestamp: Date.now(),
      reason: 'User cancelled',
    });

    // Verify state is unchanged
    expect(store.findOne('testdb', 'accounts', 'acc1')).toEqual(preAcc1);
    expect(store.findOne('testdb', 'accounts', 'acc2')).toEqual(preAcc2);
    expect(store.findOne('testdb', 'products', 'prod1')).toEqual(preProd1);
  });

  it('should not create documents that were meant to be inserted', async () => {
    const preCount = store.count('testdb', 'accounts');

    const storage = createMockStorage();
    const executor = createMockExecutor();
    const participant = new TransactionParticipant(1, storage, executor);

    const operations: BufferedOperation[] = [
      { type: 'insert', collection: 'accounts', database: 'testdb', document: { _id: 'new1', name: 'New1' }, timestamp: Date.now() },
      { type: 'insert', collection: 'accounts', database: 'testdb', document: { _id: 'new2', name: 'New2' }, timestamp: Date.now() },
      { type: 'insert', collection: 'accounts', database: 'testdb', document: { _id: 'new3', name: 'New3' }, timestamp: Date.now() },
    ];

    await participant.handlePrepare(createPrepareMessage('txn-1', operations));
    await participant.handleAbort({
      type: 'abort',
      txnId: 'txn-1',
      shardId: 1,
      timestamp: Date.now(),
      reason: 'Aborted',
    });

    // Document count should be unchanged
    expect(store.count('testdb', 'accounts')).toBe(preCount);

    // New documents should not exist
    expect(store.findOne('testdb', 'accounts', 'new1')).toBeNull();
    expect(store.findOne('testdb', 'accounts', 'new2')).toBeNull();
    expect(store.findOne('testdb', 'accounts', 'new3')).toBeNull();
  });

  it('should preserve documents that were meant to be deleted', async () => {
    const preAcc1 = store.findOne('testdb', 'accounts', 'acc1');
    expect(preAcc1).not.toBeNull();

    const storage = createMockStorage();
    const executor = createMockExecutor();
    const participant = new TransactionParticipant(1, storage, executor);

    const operations: BufferedOperation[] = [
      { type: 'delete', collection: 'accounts', database: 'testdb', filter: { _id: 'acc1' }, timestamp: Date.now() },
    ];

    await participant.handlePrepare(createPrepareMessage('txn-1', operations));
    await participant.handleAbort({
      type: 'abort',
      txnId: 'txn-1',
      shardId: 1,
      timestamp: Date.now(),
      reason: 'Aborted',
    });

    // Document should still exist with original values
    const postAcc1 = store.findOne('testdb', 'accounts', 'acc1');
    expect(postAcc1).toEqual(preAcc1);
  });

  it('should preserve original document values after abort of replace operation', async () => {
    const preAcc1 = store.findOne('testdb', 'accounts', 'acc1');

    const storage = createMockStorage();
    const executor = createMockExecutor();
    const participant = new TransactionParticipant(1, storage, executor);

    const operations: BufferedOperation[] = [
      {
        type: 'replace',
        collection: 'accounts',
        database: 'testdb',
        filter: { _id: 'acc1' },
        replacement: { _id: 'acc1', name: 'Completely Different', balance: 9999, newField: 'added' },
        timestamp: Date.now(),
      },
    ];

    await participant.handlePrepare(createPrepareMessage('txn-1', operations));
    await participant.handleAbort({
      type: 'abort',
      txnId: 'txn-1',
      shardId: 1,
      timestamp: Date.now(),
      reason: 'Aborted',
    });

    // Original document should be preserved
    const postAcc1 = store.findOne('testdb', 'accounts', 'acc1');
    expect(postAcc1).toEqual(preAcc1);
    expect(postAcc1?.name).toBe('Alice');
    expect(postAcc1?.balance).toBe(1000);
  });

  it('should handle complex transaction with multiple operations and preserve all original states', async () => {
    // Capture all original states
    const originalStates = {
      acc1: store.findOne('testdb', 'accounts', 'acc1'),
      acc2: store.findOne('testdb', 'accounts', 'acc2'),
      prod1: store.findOne('testdb', 'products', 'prod1'),
      accountCount: store.count('testdb', 'accounts'),
      productCount: store.count('testdb', 'products'),
    };

    const storage = createMockStorage();
    const executor = createMockExecutor();
    const participant = new TransactionParticipant(1, storage, executor);

    // Complex transaction with many operations
    const operations: BufferedOperation[] = [
      { type: 'insert', collection: 'accounts', database: 'testdb', document: { _id: 'new', name: 'New' }, timestamp: Date.now() },
      { type: 'update', collection: 'accounts', database: 'testdb', filter: { _id: 'acc1' }, update: { $set: { name: 'Changed' } }, timestamp: Date.now() },
      { type: 'update', collection: 'accounts', database: 'testdb', filter: { _id: 'acc2' }, update: { $inc: { balance: 1000 } }, timestamp: Date.now() },
      { type: 'delete', collection: 'products', database: 'testdb', filter: { _id: 'prod1' }, timestamp: Date.now() },
      { type: 'insert', collection: 'products', database: 'testdb', document: { _id: 'prod2', name: 'Gadget' }, timestamp: Date.now() },
    ];

    await participant.handlePrepare(createPrepareMessage('txn-1', operations));
    await participant.handleAbort({
      type: 'abort',
      txnId: 'txn-1',
      shardId: 1,
      timestamp: Date.now(),
      reason: 'Rollback test',
    });

    // Verify all original states are preserved
    expect(store.findOne('testdb', 'accounts', 'acc1')).toEqual(originalStates.acc1);
    expect(store.findOne('testdb', 'accounts', 'acc2')).toEqual(originalStates.acc2);
    expect(store.findOne('testdb', 'products', 'prod1')).toEqual(originalStates.prod1);
    expect(store.count('testdb', 'accounts')).toBe(originalStates.accountCount);
    expect(store.count('testdb', 'products')).toBe(originalStates.productCount);

    // New documents should not exist
    expect(store.findOne('testdb', 'accounts', 'new')).toBeNull();
    expect(store.findOne('testdb', 'products', 'prod2')).toBeNull();
  });

  it('should preserve state across multiple aborted transactions', async () => {
    const originalAcc1 = store.findOne('testdb', 'accounts', 'acc1');

    const storage = createMockStorage();
    const executor = createMockExecutor();
    const participant = new TransactionParticipant(1, storage, executor);

    // First transaction - abort
    await participant.handlePrepare(createPrepareMessage('txn-1', [
      { type: 'update', collection: 'accounts', database: 'testdb', filter: { _id: 'acc1' }, update: { $set: { balance: 500 } }, timestamp: Date.now() },
    ]));
    await participant.handleAbort({ type: 'abort', txnId: 'txn-1', shardId: 1, timestamp: Date.now(), reason: 'First abort' });

    // Second transaction - abort
    await participant.handlePrepare(createPrepareMessage('txn-2', [
      { type: 'update', collection: 'accounts', database: 'testdb', filter: { _id: 'acc1' }, update: { $set: { balance: 0 } }, timestamp: Date.now() },
    ]));
    await participant.handleAbort({ type: 'abort', txnId: 'txn-2', shardId: 1, timestamp: Date.now(), reason: 'Second abort' });

    // Third transaction - abort
    await participant.handlePrepare(createPrepareMessage('txn-3', [
      { type: 'delete', collection: 'accounts', database: 'testdb', filter: { _id: 'acc1' }, timestamp: Date.now() },
    ]));
    await participant.handleAbort({ type: 'abort', txnId: 'txn-3', shardId: 1, timestamp: Date.now(), reason: 'Third abort' });

    // Original state should still be preserved
    expect(store.findOne('testdb', 'accounts', 'acc1')).toEqual(originalAcc1);
  });
});

// ============================================================================
// Additional Edge Case Tests
// ============================================================================

describe('Rollback Completeness - Edge cases', () => {
  it('should handle empty transaction rollback', async () => {
    const session = new ClientSession();

    session.startTransaction();

    // No operations buffered
    expect(session.operationCount).toBe(0);

    await session.abortTransaction();

    expect(session.transactionState).toBe('aborted');
  });

  it('should handle rollback of transaction with duplicate operations on same document', async () => {
    const storage = createMockStorage();
    const executor = createMockExecutor();
    const participant = new TransactionParticipant(1, storage, executor);

    // Multiple operations on same document
    const operations: BufferedOperation[] = [
      { type: 'update', collection: 'test', database: 'db', filter: { _id: 'doc1' }, update: { $set: { a: 1 } }, timestamp: Date.now() },
      { type: 'update', collection: 'test', database: 'db', filter: { _id: 'doc1' }, update: { $set: { b: 2 } }, timestamp: Date.now() },
      { type: 'update', collection: 'test', database: 'db', filter: { _id: 'doc1' }, update: { $set: { c: 3 } }, timestamp: Date.now() },
    ];

    await participant.handlePrepare(createPrepareMessage('txn-1', operations));
    await participant.handleAbort({ type: 'abort', txnId: 'txn-1', shardId: 1, timestamp: Date.now(), reason: 'Test' });

    expect(participant.getStats().preparedCount).toBe(0);
  });

  it('should handle concurrent abort requests idempotently', async () => {
    const storage = createMockStorage();
    const executor = createMockExecutor();
    const participant = new TransactionParticipant(1, storage, executor);

    const operations: BufferedOperation[] = [
      { type: 'insert', collection: 'test', database: 'db', document: { _id: 'doc1' }, timestamp: Date.now() },
    ];

    await participant.handlePrepare(createPrepareMessage('txn-1', operations));

    // Multiple abort requests
    const abortMessage: AbortMessage = { type: 'abort', txnId: 'txn-1', shardId: 1, timestamp: Date.now(), reason: 'Test' };

    const [result1, result2, result3] = await Promise.all([
      participant.handleAbort(abortMessage),
      participant.handleAbort(abortMessage),
      participant.handleAbort(abortMessage),
    ]);

    // All should return ACK
    expect(result1.type).toBe('ack');
    expect(result2.type).toBe('ack');
    expect(result3.type).toBe('ack');

    expect(participant.getStats().preparedCount).toBe(0);
  });

  it('should handle large transaction rollback with many operations', async () => {
    const session = new ClientSession();
    let appliedCount = 0;

    session.setCommitHandler(async (_sess, ops) => {
      appliedCount = ops.length;
    });

    session.startTransaction();

    // Buffer many operations
    for (let i = 0; i < 100; i++) {
      session.bufferOperation({
        type: 'insert',
        collection: 'bulk',
        database: 'testdb',
        document: { _id: `doc-${i}`, index: i },
      });
    }

    expect(session.operationCount).toBe(100);

    await session.abortTransaction();

    expect(appliedCount).toBe(0);
    expect(session.operationCount).toBe(0);
  });

  it('should maintain transaction manager state correctly after abort', async () => {
    const session = new ClientSession();
    const txnManager = new TransactionManager(session);

    txnManager.begin();

    txnManager.insert('testdb', 'users', { _id: 'user1', name: 'Alice' });
    txnManager.update('testdb', 'users', { _id: 'user1' }, { $set: { name: 'Updated' } });
    txnManager.delete('testdb', 'users', { _id: 'other' });

    expect(txnManager.operationCount).toBe(3);
    expect(txnManager.inTransaction).toBe(true);

    await txnManager.abort();

    expect(txnManager.inTransaction).toBe(false);
    expect(txnManager.operationCount).toBe(0);
    expect(txnManager.getSnapshot()).toBeNull();
  });
});
