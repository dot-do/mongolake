/**
 * TransactionParticipant Unit Tests
 *
 * Tests for the Two-Phase Commit (2PC) participant implementation.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  TransactionParticipant,
  createTransactionParticipant,
  LockManager,
  type ParticipantStorage,
  type OperationExecutor,
  type PreparedTransaction,
  type ValidationResult,
} from '../../../src/transaction/participant.js';
import type {
  PrepareMessage,
  CommitMessage,
  AbortMessage,
  StatusQueryMessage,
} from '../../../src/transaction/coordinator.js';
import type { BufferedOperation } from '../../../src/session/index.js';

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Create a mock storage implementation.
 */
function createMockStorage(): ParticipantStorage & {
  savedTransactions: Map<string, PreparedTransaction>;
  currentLSN: number;
} {
  const savedTransactions = new Map<string, PreparedTransaction>();
  let currentLSN = 100;

  return {
    savedTransactions,
    currentLSN,
    async savePreparedTransaction(txn: PreparedTransaction): Promise<void> {
      savedTransactions.set(txn.txnId, txn);
    },
    async loadPreparedTransaction(txnId: string): Promise<PreparedTransaction | null> {
      return savedTransactions.get(txnId) ?? null;
    },
    async deletePreparedTransaction(txnId: string): Promise<void> {
      savedTransactions.delete(txnId);
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
} = {}): OperationExecutor {
  const {
    shouldValidate = true,
    validationErrors,
    applyLSN = 200,
  } = options;

  return {
    async validateOperations(): Promise<ValidationResult> {
      if (!shouldValidate) {
        return {
          valid: false,
          errors: validationErrors ?? ['Validation failed'],
        };
      }
      return { valid: true };
    },
    async applyOperations(): Promise<number> {
      return applyLSN;
    },
  };
}

/**
 * Create test operations.
 */
function createTestOperations(): BufferedOperation[] {
  return [
    {
      type: 'insert',
      collection: 'users',
      database: 'testdb',
      document: { _id: 'user1', name: 'Alice' },
      timestamp: Date.now(),
    },
    {
      type: 'update',
      collection: 'users',
      database: 'testdb',
      filter: { _id: 'user2' },
      update: { $set: { name: 'Bob Updated' } },
      timestamp: Date.now(),
    },
  ];
}

/**
 * Create a prepare message.
 */
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
// Factory Function Tests
// ============================================================================

describe('createTransactionParticipant', () => {
  it('should create a TransactionParticipant instance', () => {
    const storage = createMockStorage();
    const executor = createMockExecutor();

    const participant = createTransactionParticipant(1, storage, executor);

    expect(participant).toBeInstanceOf(TransactionParticipant);
  });

  it('should accept custom options', () => {
    const storage = createMockStorage();
    const executor = createMockExecutor();

    const participant = createTransactionParticipant(1, storage, executor, {
      lockTimeoutMs: 10000,
      preparedTimeoutMs: 60000,
    });

    expect(participant).toBeInstanceOf(TransactionParticipant);
  });
});

// ============================================================================
// LockManager Tests
// ============================================================================

describe('LockManager', () => {
  let lockManager: LockManager;

  beforeEach(() => {
    lockManager = new LockManager(5000);
  });

  it('should acquire locks for operations', () => {
    const operations = createTestOperations();

    const result = lockManager.acquireLocks('txn-1', operations);

    expect(result.success).toBe(true);
    expect(result.errors).toHaveLength(0);
  });

  it('should prevent concurrent locks on same document', () => {
    const operations1: BufferedOperation[] = [
      {
        type: 'update',
        collection: 'users',
        database: 'testdb',
        filter: { _id: 'user1' },
        update: { $set: { name: 'Alice' } },
        timestamp: Date.now(),
      },
    ];

    const operations2: BufferedOperation[] = [
      {
        type: 'update',
        collection: 'users',
        database: 'testdb',
        filter: { _id: 'user1' },
        update: { $set: { name: 'Bob' } },
        timestamp: Date.now(),
      },
    ];

    // First transaction acquires lock
    const result1 = lockManager.acquireLocks('txn-1', operations1);
    expect(result1.success).toBe(true);

    // Second transaction should fail to acquire
    const result2 = lockManager.acquireLocks('txn-2', operations2);
    expect(result2.success).toBe(false);
    expect(result2.errors.length).toBeGreaterThan(0);
    expect(result2.errors[0]).toContain('locked by transaction txn-1');
  });

  it('should allow same transaction to re-acquire locks', () => {
    const operations = createTestOperations();

    // First acquisition
    const result1 = lockManager.acquireLocks('txn-1', operations);
    expect(result1.success).toBe(true);

    // Same transaction acquiring again (idempotent)
    const result2 = lockManager.acquireLocks('txn-1', operations);
    expect(result2.success).toBe(true);
  });

  it('should release locks', () => {
    const operations = createTestOperations();

    // Acquire locks
    lockManager.acquireLocks('txn-1', operations);

    // Release locks
    lockManager.releaseLocks('txn-1');

    // Another transaction should now be able to acquire
    const result = lockManager.acquireLocks('txn-2', operations);
    expect(result.success).toBe(true);
  });

  it('should track lock statistics', () => {
    const operations = createTestOperations();
    lockManager.acquireLocks('txn-1', operations);

    const stats = lockManager.getStats();

    expect(stats.totalLocks).toBeGreaterThan(0);
    expect(stats.transactionCount).toBe(1);
  });

  it('should allow locks on different documents', () => {
    const operations1: BufferedOperation[] = [
      {
        type: 'update',
        collection: 'users',
        database: 'testdb',
        filter: { _id: 'user1' },
        update: { $set: { name: 'Alice' } },
        timestamp: Date.now(),
      },
    ];

    const operations2: BufferedOperation[] = [
      {
        type: 'update',
        collection: 'users',
        database: 'testdb',
        filter: { _id: 'user2' },
        update: { $set: { name: 'Bob' } },
        timestamp: Date.now(),
      },
    ];

    const result1 = lockManager.acquireLocks('txn-1', operations1);
    const result2 = lockManager.acquireLocks('txn-2', operations2);

    expect(result1.success).toBe(true);
    expect(result2.success).toBe(true);
  });
});

// ============================================================================
// Prepare Phase Tests
// ============================================================================

describe('TransactionParticipant - Prepare Phase', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let executor: OperationExecutor;
  let participant: TransactionParticipant;

  beforeEach(() => {
    storage = createMockStorage();
    executor = createMockExecutor();
    participant = new TransactionParticipant(1, storage, executor);
  });

  it('should respond PREPARED for valid operations', async () => {
    const operations = createTestOperations();
    const message = createPrepareMessage('txn-1', operations);

    const response = await participant.handlePrepare(message);

    expect(response.type).toBe('prepared');
    if (response.type === 'prepared') {
      expect(response.preparedLSN).toBeGreaterThan(0);
    }
  });

  it('should persist prepared transaction', async () => {
    const operations = createTestOperations();
    const message = createPrepareMessage('txn-1', operations);

    await participant.handlePrepare(message);

    expect(storage.savedTransactions.has('txn-1')).toBe(true);
    const saved = storage.savedTransactions.get('txn-1')!;
    expect(saved.state).toBe('prepared');
    expect(saved.operations).toEqual(operations);
  });

  it('should respond ABORT_VOTE for validation failure', async () => {
    executor = createMockExecutor({ shouldValidate: false });
    participant = new TransactionParticipant(1, storage, executor);

    const operations = createTestOperations();
    const message = createPrepareMessage('txn-1', operations);

    const response = await participant.handlePrepare(message);

    expect(response.type).toBe('abort_vote');
    if (response.type === 'abort_vote') {
      expect(response.reason).toContain('Validation failed');
    }
  });

  it('should respond ABORT_VOTE when deadline exceeded', async () => {
    const operations = createTestOperations();
    const message: PrepareMessage = {
      type: 'prepare',
      txnId: 'txn-1',
      shardId: 1,
      timestamp: Date.now(),
      operations,
      prepareDeadline: Date.now() - 1000, // Already expired
    };

    const response = await participant.handlePrepare(message);

    expect(response.type).toBe('abort_vote');
    if (response.type === 'abort_vote') {
      expect(response.reason).toContain('deadline');
    }
  });

  it('should be idempotent for same transaction', async () => {
    const operations = createTestOperations();
    const message = createPrepareMessage('txn-1', operations);

    // First prepare
    const response1 = await participant.handlePrepare(message);
    expect(response1.type).toBe('prepared');

    // Second prepare (same transaction)
    const response2 = await participant.handlePrepare(message);
    expect(response2.type).toBe('prepared');

    // Should return same prepared LSN
    if (response1.type === 'prepared' && response2.type === 'prepared') {
      expect(response2.preparedLSN).toBe(response1.preparedLSN);
    }
  });

  it('should track prepared transaction IDs', async () => {
    // Use different operations for different transactions to avoid lock conflicts
    const operations1: BufferedOperation[] = [
      {
        type: 'insert',
        collection: 'users',
        database: 'testdb',
        document: { _id: 'user1', name: 'Alice' },
        timestamp: Date.now(),
      },
    ];

    const operations2: BufferedOperation[] = [
      {
        type: 'insert',
        collection: 'users',
        database: 'testdb',
        document: { _id: 'user3', name: 'Charlie' },
        timestamp: Date.now(),
      },
    ];

    await participant.handlePrepare(createPrepareMessage('txn-1', operations1));
    await participant.handlePrepare(createPrepareMessage('txn-2', operations2));

    const preparedIds = participant.getPreparedTransactionIds();
    expect(preparedIds).toContain('txn-1');
    expect(preparedIds).toContain('txn-2');
  });
});

// ============================================================================
// Commit Phase Tests
// ============================================================================

describe('TransactionParticipant - Commit Phase', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let executor: OperationExecutor;
  let participant: TransactionParticipant;

  beforeEach(() => {
    storage = createMockStorage();
    executor = createMockExecutor({ applyLSN: 250 });
    participant = new TransactionParticipant(1, storage, executor);
  });

  it('should apply operations and return ACK', async () => {
    const operations = createTestOperations();
    const prepareMessage = createPrepareMessage('txn-1', operations);

    // Prepare first
    await participant.handlePrepare(prepareMessage);

    // Commit
    const commitMessage: CommitMessage = {
      type: 'commit',
      txnId: 'txn-1',
      shardId: 1,
      timestamp: Date.now(),
      commitDeadline: Date.now() + 5000,
    };

    const response = await participant.handleCommit(commitMessage);

    expect(response.type).toBe('ack');
    expect(response.finalLSN).toBe(250);
  });

  it('should clean up prepared transaction after commit', async () => {
    const operations = createTestOperations();
    await participant.handlePrepare(createPrepareMessage('txn-1', operations));

    const commitMessage: CommitMessage = {
      type: 'commit',
      txnId: 'txn-1',
      shardId: 1,
      timestamp: Date.now(),
      commitDeadline: Date.now() + 5000,
    };

    await participant.handleCommit(commitMessage);

    // Transaction should be removed
    expect(storage.savedTransactions.has('txn-1')).toBe(false);
    expect(participant.getPreparedTransactionIds()).not.toContain('txn-1');
  });

  it('should be idempotent for already committed transaction', async () => {
    const operations = createTestOperations();
    await participant.handlePrepare(createPrepareMessage('txn-1', operations));

    const commitMessage: CommitMessage = {
      type: 'commit',
      txnId: 'txn-1',
      shardId: 1,
      timestamp: Date.now(),
      commitDeadline: Date.now() + 5000,
    };

    // First commit
    const response1 = await participant.handleCommit(commitMessage);
    expect(response1.type).toBe('ack');

    // Second commit (idempotent)
    const response2 = await participant.handleCommit(commitMessage);
    expect(response2.type).toBe('ack');
  });

  it('should handle commit for unknown transaction', async () => {
    const commitMessage: CommitMessage = {
      type: 'commit',
      txnId: 'unknown-txn',
      shardId: 1,
      timestamp: Date.now(),
      commitDeadline: Date.now() + 5000,
    };

    // Should return ACK anyway (idempotent)
    const response = await participant.handleCommit(commitMessage);
    expect(response.type).toBe('ack');
  });
});

// ============================================================================
// Abort Phase Tests
// ============================================================================

describe('TransactionParticipant - Abort Phase', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let executor: OperationExecutor;
  let participant: TransactionParticipant;

  beforeEach(() => {
    storage = createMockStorage();
    executor = createMockExecutor();
    participant = new TransactionParticipant(1, storage, executor);
  });

  it('should discard prepared transaction and return ACK', async () => {
    const operations = createTestOperations();
    await participant.handlePrepare(createPrepareMessage('txn-1', operations));

    const abortMessage: AbortMessage = {
      type: 'abort',
      txnId: 'txn-1',
      shardId: 1,
      timestamp: Date.now(),
      reason: 'Coordinator decided to abort',
    };

    const response = await participant.handleAbort(abortMessage);

    expect(response.type).toBe('ack');
  });

  it('should clean up prepared transaction after abort', async () => {
    const operations = createTestOperations();
    await participant.handlePrepare(createPrepareMessage('txn-1', operations));

    const abortMessage: AbortMessage = {
      type: 'abort',
      txnId: 'txn-1',
      shardId: 1,
      timestamp: Date.now(),
      reason: 'Coordinator decided to abort',
    };

    await participant.handleAbort(abortMessage);

    // Transaction should be removed
    expect(storage.savedTransactions.has('txn-1')).toBe(false);
    expect(participant.getPreparedTransactionIds()).not.toContain('txn-1');
  });

  it('should release locks after abort', async () => {
    const operations = createTestOperations();
    await participant.handlePrepare(createPrepareMessage('txn-1', operations));

    // Check locks are held
    expect(participant.getStats().lockStats.transactionCount).toBe(1);

    const abortMessage: AbortMessage = {
      type: 'abort',
      txnId: 'txn-1',
      shardId: 1,
      timestamp: Date.now(),
      reason: 'Abort',
    };

    await participant.handleAbort(abortMessage);

    // Locks should be released
    expect(participant.getStats().lockStats.transactionCount).toBe(0);
  });

  it('should handle abort for unknown transaction', async () => {
    const abortMessage: AbortMessage = {
      type: 'abort',
      txnId: 'unknown-txn',
      shardId: 1,
      timestamp: Date.now(),
      reason: 'Abort',
    };

    // Should return ACK anyway
    const response = await participant.handleAbort(abortMessage);
    expect(response.type).toBe('ack');
  });
});

// ============================================================================
// Status Query Tests
// ============================================================================

describe('TransactionParticipant - Status Query', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let executor: OperationExecutor;
  let participant: TransactionParticipant;

  beforeEach(() => {
    storage = createMockStorage();
    executor = createMockExecutor();
    participant = new TransactionParticipant(1, storage, executor);
  });

  it('should return prepared status for prepared transaction', async () => {
    const operations = createTestOperations();
    const prepareResponse = await participant.handlePrepare(
      createPrepareMessage('txn-1', operations)
    );

    const statusMessage: StatusQueryMessage = {
      type: 'status_query',
      txnId: 'txn-1',
      shardId: 1,
      timestamp: Date.now(),
    };

    const response = await participant.handleStatusQuery(statusMessage);

    expect(response.type).toBe('status_response');
    expect(response.participantState).toBe('prepared');
    if (prepareResponse.type === 'prepared') {
      expect(response.preparedLSN).toBe(prepareResponse.preparedLSN);
    }
  });

  it('should return done status for unknown transaction', async () => {
    const statusMessage: StatusQueryMessage = {
      type: 'status_query',
      txnId: 'unknown-txn',
      shardId: 1,
      timestamp: Date.now(),
    };

    const response = await participant.handleStatusQuery(statusMessage);

    expect(response.type).toBe('status_response');
    expect(response.participantState).toBe('done');
  });
});

// ============================================================================
// Recovery Tests
// ============================================================================

describe('TransactionParticipant - Recovery', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let executor: OperationExecutor;

  beforeEach(() => {
    storage = createMockStorage();
    executor = createMockExecutor();
  });

  it('should recover prepared transactions', async () => {
    // Simulate a prepared transaction from before restart
    const preparedTxn: PreparedTransaction = {
      txnId: 'txn-1',
      state: 'prepared',
      operations: createTestOperations(),
      preparedLSN: 150,
      prepareDeadline: Date.now() + 30000,
      preparedAt: Date.now(),
    };
    storage.savedTransactions.set('txn-1', preparedTxn);

    // Create new participant and recover
    const participant = new TransactionParticipant(1, storage, executor);
    await participant.recover();

    // Transaction should be recovered
    expect(participant.getPreparedTransactionIds()).toContain('txn-1');
  });

  it('should abort expired transactions during recovery', async () => {
    // Simulate an expired prepared transaction from 60 seconds ago
    const expiredTxn: PreparedTransaction = {
      txnId: 'txn-expired',
      state: 'prepared',
      operations: createTestOperations(),
      preparedLSN: 150,
      prepareDeadline: Date.now() - 60000, // Expired
      preparedAt: Date.now() - 60000, // 60 seconds ago
    };
    storage.savedTransactions.set('txn-expired', expiredTxn);

    // Create new participant with short timeout (1ms)
    // The transaction was prepared 60000ms ago, so it should exceed the timeout
    const participant = new TransactionParticipant(1, storage, executor, {
      preparedTimeoutMs: 1000, // 1 second timeout, transaction is 60s old
    });
    await participant.recover();

    // Transaction should be aborted (not in prepared list)
    expect(participant.getPreparedTransactionIds()).not.toContain('txn-expired');
  });
});

// ============================================================================
// Statistics Tests
// ============================================================================

describe('TransactionParticipant - Statistics', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let executor: OperationExecutor;
  let participant: TransactionParticipant;

  beforeEach(() => {
    storage = createMockStorage();
    executor = createMockExecutor();
    participant = new TransactionParticipant(1, storage, executor);
  });

  it('should track prepared transaction count', async () => {
    // Use different operations for different transactions to avoid lock conflicts
    const operations1: BufferedOperation[] = [
      {
        type: 'insert',
        collection: 'users',
        database: 'testdb',
        document: { _id: 'user1', name: 'Alice' },
        timestamp: Date.now(),
      },
    ];

    const operations2: BufferedOperation[] = [
      {
        type: 'insert',
        collection: 'users',
        database: 'testdb',
        document: { _id: 'user3', name: 'Charlie' },
        timestamp: Date.now(),
      },
    ];

    await participant.handlePrepare(createPrepareMessage('txn-1', operations1));
    await participant.handlePrepare(createPrepareMessage('txn-2', operations2));

    const stats = participant.getStats();
    expect(stats.preparedCount).toBe(2);
  });

  it('should track lock statistics', async () => {
    const operations = createTestOperations();
    await participant.handlePrepare(createPrepareMessage('txn-1', operations));

    const stats = participant.getStats();
    expect(stats.lockStats.totalLocks).toBeGreaterThan(0);
    expect(stats.lockStats.transactionCount).toBe(1);
  });
});

// ============================================================================
// Lock Conflict Tests
// ============================================================================

describe('TransactionParticipant - Lock Conflicts', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let executor: OperationExecutor;
  let participant: TransactionParticipant;

  beforeEach(() => {
    storage = createMockStorage();
    executor = createMockExecutor();
    participant = new TransactionParticipant(1, storage, executor);
  });

  it('should abort when locks cannot be acquired', async () => {
    // First transaction prepares and holds locks
    const operations1: BufferedOperation[] = [
      {
        type: 'update',
        collection: 'users',
        database: 'testdb',
        filter: { _id: 'user1' },
        update: { $set: { name: 'Alice' } },
        timestamp: Date.now(),
      },
    ];

    const response1 = await participant.handlePrepare(
      createPrepareMessage('txn-1', operations1)
    );
    expect(response1.type).toBe('prepared');

    // Second transaction tries same document
    const operations2: BufferedOperation[] = [
      {
        type: 'update',
        collection: 'users',
        database: 'testdb',
        filter: { _id: 'user1' },
        update: { $set: { name: 'Bob' } },
        timestamp: Date.now(),
      },
    ];

    const response2 = await participant.handlePrepare(
      createPrepareMessage('txn-2', operations2)
    );

    expect(response2.type).toBe('abort_vote');
    if (response2.type === 'abort_vote') {
      expect(response2.reason).toContain('Lock acquisition failed');
    }
  });
});
