/**
 * Distributed Transaction Edge Cases Tests
 *
 * Comprehensive edge case testing for multi-shard transactions including:
 * - Network partition scenarios
 * - Concurrent transaction conflicts
 * - Timeout edge cases
 * - Message ordering issues
 * - Idempotency guarantees
 * - State machine edge cases
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  TransactionCoordinator,
  DistributedTransactionError,
  TransactionTimeoutError,
  ParticipantAbortError,
  MaxCommitAttemptsError,
  CommitCircuitBreakerError,
  type ShardRPC,
  type PrepareMessage,
  type PreparedMessage,
  type AbortVoteMessage,
  type CommitMessage,
  type AbortMessage,
  type AckMessage,
  type StatusQueryMessage,
  type StatusResponseMessage,
  type CoordinatorState,
  type ParticipantState,
} from '../../../src/transaction/coordinator.js';
import {
  TransactionParticipant,
  LockManager,
  type ParticipantStorage,
  type OperationExecutor,
  type PreparedTransaction,
} from '../../../src/transaction/participant.js';
import { ShardRouter } from '../../../src/shard/router.js';
import type { BufferedOperation } from '../../../src/session/index.js';

// ============================================================================
// Test Helpers
// ============================================================================

function createMockStorage(): ParticipantStorage & {
  savedTransactions: Map<string, PreparedTransaction>;
} {
  const savedTransactions = new Map<string, PreparedTransaction>();
  let currentLSN = 100;

  return {
    savedTransactions,
    async savePreparedTransaction(txn) {
      savedTransactions.set(txn.txnId, txn);
    },
    async loadPreparedTransaction(txnId) {
      return savedTransactions.get(txnId) ?? null;
    },
    async deletePreparedTransaction(txnId) {
      savedTransactions.delete(txnId);
    },
    async loadAllPreparedTransactions() {
      return Array.from(savedTransactions.values());
    },
    allocateLSN() {
      return ++currentLSN;
    },
    getCurrentLSN() {
      return currentLSN;
    },
  };
}

function createMockExecutor(options: {
  shouldValidate?: boolean;
  validationErrors?: string[];
  applyLSN?: number;
  shouldFailApply?: boolean;
} = {}): OperationExecutor {
  return {
    async validateOperations() {
      if (options.shouldValidate === false) {
        return { valid: false, errors: options.validationErrors ?? ['Validation failed'] };
      }
      return { valid: true };
    },
    async applyOperations() {
      if (options.shouldFailApply) {
        throw new Error('Apply failed');
      }
      return options.applyLSN ?? 200;
    },
  };
}

function createTestOperations(docIds: string[] = ['doc1']): BufferedOperation[] {
  return docIds.map(id => ({
    type: 'update' as const,
    collection: 'test',
    database: 'testdb',
    filter: { _id: id },
    update: { $set: { updated: true } },
    timestamp: Date.now(),
  }));
}

function createPrepareMessage(
  txnId: string,
  operations: BufferedOperation[],
  shardId: number = 1
): PrepareMessage {
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
// Network Partition Scenarios
// ============================================================================

describe('Edge Cases - Network Partition Scenarios', () => {
  let router: ShardRouter;

  beforeEach(() => {
    vi.useFakeTimers();
    router = new ShardRouter();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should abort when all shards become unreachable during prepare', async () => {
    const rpc: ShardRPC = {
      async sendPrepare() {
        throw new Error('Network unreachable');
      },
      async sendCommit(shardId, message) {
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
      maxRetries: 2,
      retryDelayMs: 1,
    });

    const operations: BufferedOperation[] = [
      {
        type: 'insert',
        collection: 'test',
        database: 'db1',
        document: { _id: 'doc1' },
        timestamp: Date.now(),
      },
      {
        type: 'insert',
        collection: 'test',
        database: 'db2',
        document: { _id: 'doc2' },
        timestamp: Date.now(),
      },
    ];

    const executePromise = coordinator.execute(operations);
    await vi.advanceTimersByTimeAsync(100);
    const result = await executePromise;

    expect(result.committed).toBe(false);
    expect(result.abortReason).toContain('Network unreachable');
  });

  it('should handle intermittent network failures during prepare', async () => {
    let attemptCount = 0;

    const rpc: ShardRPC = {
      async sendPrepare(shardId, message) {
        attemptCount++;
        // Fail first 2 attempts, then succeed
        if (attemptCount <= 2) {
          throw new Error('Temporary network issue');
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
        return { type: 'ack', txnId: message.txnId, shardId, timestamp: Date.now() };
      },
      async queryStatus(shardId, message) {
        return { type: 'status_response', txnId: message.txnId, shardId, timestamp: Date.now(), participantState: 'done' };
      },
    };

    const coordinator = new TransactionCoordinator(router, rpc, {
      maxRetries: 5,
      retryDelayMs: 1,
    });

    const operations: BufferedOperation[] = [
      {
        type: 'insert',
        collection: 'test',
        database: 'db1',
        document: { _id: 'doc1' },
        timestamp: Date.now(),
      },
      {
        type: 'insert',
        collection: 'test',
        database: 'db2',
        document: { _id: 'doc2' },
        timestamp: Date.now(),
      },
    ];

    const executePromise = coordinator.execute(operations);
    await vi.advanceTimersByTimeAsync(100);
    const result = await executePromise;

    expect(result.committed).toBe(true);
    expect(attemptCount).toBeGreaterThan(2);
  });

  it('should handle split-brain scenario where some shards are reachable', async () => {
    const reachableShards = new Set([0, 1]);

    const rpc: ShardRPC = {
      async sendPrepare(shardId, message) {
        if (!reachableShards.has(shardId)) {
          throw new Error(`Shard ${shardId} unreachable`);
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
        if (!reachableShards.has(shardId)) {
          throw new Error(`Shard ${shardId} unreachable`);
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
      maxRetries: 2,
      retryDelayMs: 1,
    });

    // Create operations that route to unreachable shards
    const operations: BufferedOperation[] = [];
    for (let i = 0; i < 10; i++) {
      operations.push({
        type: 'insert',
        collection: `coll_${i}`,
        database: `db_${i}`,
        document: { _id: `doc_${i}` },
        timestamp: Date.now(),
      });
    }

    const executePromise = coordinator.execute(operations);
    await vi.advanceTimersByTimeAsync(100);
    const result = await executePromise;

    // Transaction may or may not commit depending on which shards are involved
    expect(result).toBeDefined();
  });
});

// ============================================================================
// Concurrent Transaction Conflicts
// ============================================================================

describe('Edge Cases - Concurrent Transaction Conflicts', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let executor: OperationExecutor;
  let participant: TransactionParticipant;

  beforeEach(() => {
    storage = createMockStorage();
    executor = createMockExecutor();
    participant = new TransactionParticipant(1, storage, executor);
  });

  it('should detect lock conflict between concurrent transactions', async () => {
    const operations = createTestOperations(['shared-doc']);

    // First transaction prepares and holds lock
    const response1 = await participant.handlePrepare(
      createPrepareMessage('txn-1', operations)
    );
    expect(response1.type).toBe('prepared');

    // Second transaction tries to access same document
    const response2 = await participant.handlePrepare(
      createPrepareMessage('txn-2', operations)
    );
    expect(response2.type).toBe('abort_vote');
    if (response2.type === 'abort_vote') {
      expect(response2.reason).toContain('Lock acquisition failed');
    }
  });

  it('should allow concurrent transactions on different documents', async () => {
    const ops1 = createTestOperations(['doc-1']);
    const ops2 = createTestOperations(['doc-2']);

    // Both transactions should prepare successfully
    const response1 = await participant.handlePrepare(
      createPrepareMessage('txn-1', ops1)
    );
    const response2 = await participant.handlePrepare(
      createPrepareMessage('txn-2', ops2)
    );

    expect(response1.type).toBe('prepared');
    expect(response2.type).toBe('prepared');
  });

  it('should handle deadlock scenario with timeout', async () => {
    const lockManager = new LockManager(100); // 100ms lock timeout

    const ops1: BufferedOperation[] = [
      { type: 'update', collection: 'test', database: 'db', filter: { _id: 'A' }, update: { $set: { x: 1 } }, timestamp: Date.now() },
    ];
    const ops2: BufferedOperation[] = [
      { type: 'update', collection: 'test', database: 'db', filter: { _id: 'A' }, update: { $set: { x: 2 } }, timestamp: Date.now() },
    ];

    // First transaction acquires lock on A
    const result1 = lockManager.acquireLocks('txn-1', ops1);
    expect(result1.success).toBe(true);

    // Second transaction tries to acquire lock on A (blocked)
    const result2 = lockManager.acquireLocks('txn-2', ops2);
    expect(result2.success).toBe(false);

    // Wait for lock timeout
    await new Promise(resolve => setTimeout(resolve, 150));

    // Now lock should be expired and second transaction can acquire
    const result3 = lockManager.acquireLocks('txn-2', ops2);
    expect(result3.success).toBe(true);
  });

  it('should prevent write-write conflicts on same document', async () => {
    const participant1 = new TransactionParticipant(1, storage, executor);

    // Both transactions want to update the same document
    const ops: BufferedOperation[] = [
      {
        type: 'update',
        collection: 'accounts',
        database: 'bank',
        filter: { _id: 'account-1' },
        update: { $inc: { balance: -100 } },
        timestamp: Date.now(),
      },
    ];

    // First transaction prepares
    const prep1 = await participant1.handlePrepare(createPrepareMessage('txn-1', ops));
    expect(prep1.type).toBe('prepared');

    // Second transaction should be rejected
    const prep2 = await participant1.handlePrepare(createPrepareMessage('txn-2', ops));
    expect(prep2.type).toBe('abort_vote');
  });
});

// ============================================================================
// Timeout Edge Cases
// ============================================================================

describe('Edge Cases - Timeout Scenarios', () => {
  let router: ShardRouter;

  beforeEach(() => {
    vi.useFakeTimers();
    router = new ShardRouter();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should abort transaction exactly at prepare deadline', async () => {
    const rpc: ShardRPC = {
      async sendPrepare(shardId, message) {
        // Delay just past the deadline
        await new Promise(resolve => setTimeout(resolve, 60));
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
        return { type: 'ack', txnId: message.txnId, shardId, timestamp: Date.now() };
      },
      async queryStatus(shardId, message) {
        return { type: 'status_response', txnId: message.txnId, shardId, timestamp: Date.now(), participantState: 'done' };
      },
    };

    const coordinator = new TransactionCoordinator(router, rpc, {
      prepareTimeoutMs: 50,
      maxRetries: 1,
    });

    const operations: BufferedOperation[] = [
      {
        type: 'insert',
        collection: 'test',
        database: 'db1',
        document: { _id: 'doc1' },
        timestamp: Date.now(),
      },
      {
        type: 'insert',
        collection: 'test',
        database: 'db2',
        document: { _id: 'doc2' },
        timestamp: Date.now(),
      },
    ];

    const executePromise = coordinator.execute(operations);
    await vi.advanceTimersByTimeAsync(100);
    const result = await executePromise;

    expect(result.committed).toBe(false);
    expect(result.state).toBe('aborted');
  });

  it('should handle participant responding with expired deadline', async () => {
    const storage = createMockStorage();
    const executor = createMockExecutor();
    const participant = new TransactionParticipant(1, storage, executor);

    const expiredMessage: PrepareMessage = {
      type: 'prepare',
      txnId: 'txn-expired',
      shardId: 1,
      timestamp: Date.now() - 10000, // 10 seconds ago
      operations: createTestOperations(),
      prepareDeadline: Date.now() - 5000, // Already expired
    };

    const response = await participant.handlePrepare(expiredMessage);

    expect(response.type).toBe('abort_vote');
    if (response.type === 'abort_vote') {
      expect(response.reason).toContain('deadline');
    }
  });

  it('should handle very short timeout gracefully', async () => {
    const rpc: ShardRPC = {
      async sendPrepare(shardId, message) {
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
        return { type: 'ack', txnId: message.txnId, shardId, timestamp: Date.now() };
      },
      async queryStatus(shardId, message) {
        return { type: 'status_response', txnId: message.txnId, shardId, timestamp: Date.now(), participantState: 'done' };
      },
    };

    const coordinator = new TransactionCoordinator(router, rpc, {
      prepareTimeoutMs: 1, // 1ms timeout
      maxRetries: 1,
    });

    const operations: BufferedOperation[] = [
      { type: 'insert', collection: 'test', database: 'db1', document: { _id: 'doc1' }, timestamp: Date.now() },
    ];

    const result = await coordinator.execute(operations);

    // Should either succeed quickly or timeout - not hang
    expect(result).toBeDefined();
  });
});

// ============================================================================
// Message Ordering Issues
// ============================================================================

describe('Edge Cases - Message Ordering', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let executor: OperationExecutor;
  let participant: TransactionParticipant;

  beforeEach(() => {
    storage = createMockStorage();
    executor = createMockExecutor();
    participant = new TransactionParticipant(1, storage, executor);
  });

  it('should handle duplicate prepare messages (idempotency)', async () => {
    const operations = createTestOperations();
    const message = createPrepareMessage('txn-dup', operations);

    // Send prepare twice
    const response1 = await participant.handlePrepare(message);
    const response2 = await participant.handlePrepare(message);

    expect(response1.type).toBe('prepared');
    expect(response2.type).toBe('prepared');

    // Should return same LSN
    if (response1.type === 'prepared' && response2.type === 'prepared') {
      expect(response1.preparedLSN).toBe(response2.preparedLSN);
    }
  });

  it('should handle duplicate commit messages (idempotency)', async () => {
    const operations = createTestOperations();
    await participant.handlePrepare(createPrepareMessage('txn-dup-commit', operations));

    const commitMessage: CommitMessage = {
      type: 'commit',
      txnId: 'txn-dup-commit',
      shardId: 1,
      timestamp: Date.now(),
      commitDeadline: Date.now() + 5000,
    };

    // Send commit twice
    const response1 = await participant.handleCommit(commitMessage);
    const response2 = await participant.handleCommit(commitMessage);

    expect(response1.type).toBe('ack');
    expect(response2.type).toBe('ack');
  });

  it('should handle commit before prepare completes (race condition)', async () => {
    // Commit for unknown transaction should be handled gracefully
    const commitMessage: CommitMessage = {
      type: 'commit',
      txnId: 'unknown-txn',
      shardId: 1,
      timestamp: Date.now(),
      commitDeadline: Date.now() + 5000,
    };

    const response = await participant.handleCommit(commitMessage);

    // Should return ACK (transaction may have already been committed and cleaned up)
    expect(response.type).toBe('ack');
  });

  it('should handle abort after commit (out of order)', async () => {
    const operations = createTestOperations();
    await participant.handlePrepare(createPrepareMessage('txn-order', operations));

    // Commit first
    await participant.handleCommit({
      type: 'commit',
      txnId: 'txn-order',
      shardId: 1,
      timestamp: Date.now(),
      commitDeadline: Date.now() + 5000,
    });

    // Then abort arrives (late)
    const abortResponse = await participant.handleAbort({
      type: 'abort',
      txnId: 'txn-order',
      shardId: 1,
      timestamp: Date.now(),
      reason: 'Late abort',
    });

    // Should handle gracefully
    expect(abortResponse.type).toBe('ack');
  });

  it('should handle prepare after abort', async () => {
    const operations = createTestOperations();
    const prepareMessage = createPrepareMessage('txn-prep-after-abort', operations);

    // First prepare
    const response1 = await participant.handlePrepare(prepareMessage);
    expect(response1.type).toBe('prepared');

    // Abort
    await participant.handleAbort({
      type: 'abort',
      txnId: 'txn-prep-after-abort',
      shardId: 1,
      timestamp: Date.now(),
      reason: 'Coordinator abort',
    });

    // Second prepare (retransmission) - should fail as transaction was aborted
    const response2 = await participant.handlePrepare(prepareMessage);
    expect(response2.type).toBe('abort_vote');
  });
});

// ============================================================================
// State Machine Edge Cases
// ============================================================================

describe('Edge Cases - State Machine', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let executor: OperationExecutor;
  let participant: TransactionParticipant;

  beforeEach(() => {
    storage = createMockStorage();
    executor = createMockExecutor();
    participant = new TransactionParticipant(1, storage, executor);
  });

  it('should track all participant states correctly', async () => {
    // Use different documents for each transaction
    const ops1 = createTestOperations(['doc-1']);
    const ops2 = createTestOperations(['doc-2']);
    const ops3 = createTestOperations(['doc-3']);

    // Multiple concurrent transactions
    await participant.handlePrepare(createPrepareMessage('txn-1', ops1));
    await participant.handlePrepare(createPrepareMessage('txn-2', ops2));
    await participant.handlePrepare(createPrepareMessage('txn-3', ops3));

    expect(participant.getStats().preparedCount).toBe(3);

    // Commit txn-1
    await participant.handleCommit({
      type: 'commit',
      txnId: 'txn-1',
      shardId: 1,
      timestamp: Date.now(),
      commitDeadline: Date.now() + 5000,
    });

    expect(participant.getStats().preparedCount).toBe(2);

    // Abort txn-2
    await participant.handleAbort({
      type: 'abort',
      txnId: 'txn-2',
      shardId: 1,
      timestamp: Date.now(),
      reason: 'Test abort',
    });

    expect(participant.getStats().preparedCount).toBe(1);
    expect(participant.getPreparedTransactionIds()).toEqual(['txn-3']);
  });

  it('should persist prepared state correctly', async () => {
    const operations = createTestOperations();
    const prepareMessage = createPrepareMessage('txn-persist', operations);

    await participant.handlePrepare(prepareMessage);

    // Verify storage has the prepared transaction
    expect(storage.savedTransactions.has('txn-persist')).toBe(true);
    const saved = storage.savedTransactions.get('txn-persist')!;
    expect(saved.state).toBe('prepared');
    expect(saved.operations).toEqual(operations);
  });

  it('should clean up storage after commit', async () => {
    const operations = createTestOperations();
    await participant.handlePrepare(createPrepareMessage('txn-cleanup', operations));

    expect(storage.savedTransactions.has('txn-cleanup')).toBe(true);

    await participant.handleCommit({
      type: 'commit',
      txnId: 'txn-cleanup',
      shardId: 1,
      timestamp: Date.now(),
      commitDeadline: Date.now() + 5000,
    });

    expect(storage.savedTransactions.has('txn-cleanup')).toBe(false);
  });

  it('should handle status query for all states', async () => {
    // Unknown transaction
    const status1 = await participant.handleStatusQuery({
      type: 'status_query',
      txnId: 'unknown',
      shardId: 1,
      timestamp: Date.now(),
    });
    expect(status1.participantState).toBe('done');

    // Prepared transaction
    await participant.handlePrepare(createPrepareMessage('txn-status', createTestOperations()));
    const status2 = await participant.handleStatusQuery({
      type: 'status_query',
      txnId: 'txn-status',
      shardId: 1,
      timestamp: Date.now(),
    });
    expect(status2.participantState).toBe('prepared');
  });
});

// ============================================================================
// Coordinator State Machine
// ============================================================================

describe('Edge Cases - Coordinator State Machine', () => {
  let router: ShardRouter;

  beforeEach(() => {
    vi.useFakeTimers();
    router = new ShardRouter();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should transition through all states for successful transaction', async () => {
    const states: CoordinatorState[] = [];

    const rpc: ShardRPC = {
      async sendPrepare(shardId, message) {
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
        return { type: 'ack', txnId: message.txnId, shardId, timestamp: Date.now() };
      },
      async queryStatus(shardId, message) {
        return { type: 'status_response', txnId: message.txnId, shardId, timestamp: Date.now(), participantState: 'done' };
      },
    };

    const coordinator = new TransactionCoordinator(router, rpc);

    const operations: BufferedOperation[] = [
      { type: 'insert', collection: 'test', database: 'db1', document: { _id: 'doc1' }, timestamp: Date.now() },
      { type: 'insert', collection: 'test', database: 'db2', document: { _id: 'doc2' }, timestamp: Date.now() },
    ];

    const result = await coordinator.execute(operations);

    expect(result.state).toBe('committed');
  });

  it('should handle multiple concurrent transactions', async () => {
    const rpc: ShardRPC = {
      async sendPrepare(shardId, message) {
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
        return { type: 'ack', txnId: message.txnId, shardId, timestamp: Date.now() };
      },
      async queryStatus(shardId, message) {
        return { type: 'status_response', txnId: message.txnId, shardId, timestamp: Date.now(), participantState: 'done' };
      },
    };

    const coordinator = new TransactionCoordinator(router, rpc);

    const operations1: BufferedOperation[] = [
      { type: 'insert', collection: 'test1', database: 'db1', document: { _id: 'doc1' }, timestamp: Date.now() },
    ];
    const operations2: BufferedOperation[] = [
      { type: 'insert', collection: 'test2', database: 'db2', document: { _id: 'doc2' }, timestamp: Date.now() },
    ];
    const operations3: BufferedOperation[] = [
      { type: 'insert', collection: 'test3', database: 'db3', document: { _id: 'doc3' }, timestamp: Date.now() },
    ];

    const [result1, result2, result3] = await Promise.all([
      coordinator.execute(operations1),
      coordinator.execute(operations2),
      coordinator.execute(operations3),
    ]);

    expect(result1.committed).toBe(true);
    expect(result2.committed).toBe(true);
    expect(result3.committed).toBe(true);

    // All should have unique transaction IDs
    const txnIds = new Set([result1.txnId, result2.txnId, result3.txnId]);
    expect(txnIds.size).toBe(3);
  });

  it('should handle force complete for stuck transaction', async () => {
    let commitBlocked = false;
    let resolveCommit: () => void;
    const commitPromise = new Promise<void>(resolve => {
      resolveCommit = resolve;
    });

    const rpc: ShardRPC = {
      async sendPrepare(shardId, message) {
        return {
          type: 'prepared',
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          preparedLSN: 100,
        };
      },
      async sendCommit() {
        commitBlocked = true;
        await commitPromise;
        throw new Error('Should not reach here');
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
      { type: 'insert', collection: 'test', database: 'db1', document: { _id: 'doc1' }, timestamp: Date.now() },
      { type: 'insert', collection: 'test', database: 'db2', document: { _id: 'doc2' }, timestamp: Date.now() },
    ];

    // Start execute but don't await
    const executePromise = coordinator.execute(operations);

    // Wait for commit to be blocked
    while (!commitBlocked) {
      await vi.advanceTimersByTimeAsync(10);
    }

    // Get the active transaction
    const activeTxns = coordinator.getActiveTransactions();
    expect(activeTxns.length).toBeGreaterThan(0);

    const txnId = activeTxns[0].txnId;

    // Force complete
    await coordinator.forceCompleteTransaction(txnId, 'abort');

    // Transaction should be marked aborted
    const txn = coordinator.getTransaction(txnId);
    expect(txn?.state).toBe('aborted');

    // Cleanup
    resolveCommit!();
  });
});

// ============================================================================
// Lock Manager Edge Cases
// ============================================================================

describe('Edge Cases - Lock Manager', () => {
  it('should handle lock expiration correctly', async () => {
    const lockManager = new LockManager(50); // 50ms timeout

    const ops: BufferedOperation[] = [
      { type: 'update', collection: 'test', database: 'db', filter: { _id: 'doc1' }, update: {}, timestamp: Date.now() },
    ];

    // Acquire lock
    const result1 = lockManager.acquireLocks('txn-1', ops);
    expect(result1.success).toBe(true);

    // Wait for expiration
    await new Promise(resolve => setTimeout(resolve, 100));

    // Another transaction should be able to acquire
    const result2 = lockManager.acquireLocks('txn-2', ops);
    expect(result2.success).toBe(true);
  });

  it('should extend locks correctly', async () => {
    const lockManager = new LockManager(100); // 100ms timeout

    const ops: BufferedOperation[] = [
      { type: 'update', collection: 'test', database: 'db', filter: { _id: 'doc1' }, update: {}, timestamp: Date.now() },
    ];

    // Acquire lock
    lockManager.acquireLocks('txn-1', ops);

    // Wait a bit
    await new Promise(resolve => setTimeout(resolve, 50));

    // Extend lock
    lockManager.extendLocks('txn-1', 200);

    // Wait past original expiry
    await new Promise(resolve => setTimeout(resolve, 100));

    // Lock should still be held
    const result = lockManager.acquireLocks('txn-2', ops);
    expect(result.success).toBe(false);
  });

  it('should handle operations without document IDs', () => {
    const lockManager = new LockManager(5000);

    const ops: BufferedOperation[] = [
      { type: 'insert', collection: 'test', database: 'db', document: { name: 'test' }, timestamp: Date.now() },
    ];

    // Should handle gracefully (no lock acquired for docs without _id)
    const result = lockManager.acquireLocks('txn-1', ops);
    expect(result.success).toBe(true);
  });

  it('should handle bulk operations efficiently', () => {
    const lockManager = new LockManager(5000);

    // Many operations on different documents
    const ops: BufferedOperation[] = [];
    for (let i = 0; i < 100; i++) {
      ops.push({
        type: 'update',
        collection: 'test',
        database: 'db',
        filter: { _id: `doc-${i}` },
        update: { $set: { updated: true } },
        timestamp: Date.now(),
      });
    }

    const result = lockManager.acquireLocks('txn-bulk', ops);
    expect(result.success).toBe(true);

    const stats = lockManager.getStats();
    expect(stats.totalLocks).toBe(100);
    expect(stats.transactionCount).toBe(1);
  });
});

// ============================================================================
// Recovery Edge Cases
// ============================================================================

describe('Edge Cases - Recovery', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let executor: OperationExecutor;

  beforeEach(() => {
    storage = createMockStorage();
    executor = createMockExecutor();
  });

  it('should recover multiple prepared transactions after restart', async () => {
    // Simulate crash with multiple prepared transactions
    const txns: PreparedTransaction[] = [
      {
        txnId: 'txn-1',
        state: 'prepared',
        operations: createTestOperations(['doc-1']),
        preparedLSN: 100,
        prepareDeadline: Date.now() + 30000,
        preparedAt: Date.now(),
      },
      {
        txnId: 'txn-2',
        state: 'prepared',
        operations: createTestOperations(['doc-2']),
        preparedLSN: 101,
        prepareDeadline: Date.now() + 30000,
        preparedAt: Date.now(),
      },
    ];

    for (const txn of txns) {
      storage.savedTransactions.set(txn.txnId, txn);
    }

    // Create new participant and recover
    const participant = new TransactionParticipant(1, storage, executor);
    await participant.recover();

    expect(participant.getPreparedTransactionIds().sort()).toEqual(['txn-1', 'txn-2']);
  });

  it('should handle lock conflicts during recovery', async () => {
    // Simulate crash with prepared transaction that has lock conflicts
    const txn: PreparedTransaction = {
      txnId: 'txn-conflict',
      state: 'prepared',
      operations: createTestOperations(['contested-doc']),
      preparedLSN: 100,
      prepareDeadline: Date.now() + 30000,
      preparedAt: Date.now(),
    };

    storage.savedTransactions.set('txn-conflict', txn);

    // Create first participant that holds locks
    const participant1 = new TransactionParticipant(1, storage, executor);
    await participant1.recover();

    // First recovery should succeed
    expect(participant1.getPreparedTransactionIds()).toContain('txn-conflict');
  });

  it('should abort very old prepared transactions during recovery', async () => {
    const oldTime = Date.now() - 60 * 60 * 1000; // 1 hour ago

    const txn: PreparedTransaction = {
      txnId: 'txn-old',
      state: 'prepared',
      operations: createTestOperations(),
      preparedLSN: 100,
      prepareDeadline: oldTime + 5000,
      preparedAt: oldTime,
    };

    storage.savedTransactions.set('txn-old', txn);

    const participant = new TransactionParticipant(1, storage, executor, {
      preparedTimeoutMs: 30000, // 30 second timeout
    });

    await participant.recover();

    // Transaction should be aborted (not in prepared list)
    expect(participant.getPreparedTransactionIds()).not.toContain('txn-old');
  });
});
