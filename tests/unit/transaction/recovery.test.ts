/**
 * Transaction Recovery Unit Tests
 *
 * Comprehensive tests for transaction recovery scenarios including:
 * - Coordinator crash recovery
 * - Participant crash recovery
 * - Heuristic decisions for orphaned transactions
 * - Transaction log persistence
 * - Recovery with partial failures
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  TransactionRecoveryManager,
  ParticipantRecoveryManager,
  InMemoryTransactionLogStorage,
  createTransactionRecoveryManager,
  createParticipantRecoveryManager,
  type TransactionLogEntry,
  type TransactionLogStorage,
  type CoordinatorDiscovery,
  type RecoveryOptions,
} from '../../../src/transaction/recovery.js';
import type {
  ShardRPC,
  PrepareMessage,
  PreparedMessage,
  AbortVoteMessage,
  CommitMessage,
  AbortMessage,
  AckMessage,
  StatusQueryMessage,
  StatusResponseMessage,
  DistributedTransaction,
  TransactionId,
  CoordinatorState,
} from '../../../src/transaction/coordinator.js';
import type {
  PreparedTransaction,
  ParticipantStorage,
} from '../../../src/transaction/participant.js';
import type { BufferedOperation } from '../../../src/session/index.js';

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Create a mock ShardRPC for recovery tests.
 */
function createMockShardRPC(options: {
  shouldFailCommit?: boolean;
  shouldFailAbort?: boolean;
  commitDelayMs?: number;
  abortDelayMs?: number;
} = {}): ShardRPC & {
  commitCalls: Array<{ shardId: number; txnId: string }>;
  abortCalls: Array<{ shardId: number; txnId: string; reason: string }>;
} {
  const commitCalls: Array<{ shardId: number; txnId: string }> = [];
  const abortCalls: Array<{ shardId: number; txnId: string; reason: string }> = [];

  return {
    commitCalls,
    abortCalls,
    async sendPrepare(
      shardId: number,
      message: PrepareMessage
    ): Promise<PreparedMessage | AbortVoteMessage> {
      return {
        type: 'prepared',
        txnId: message.txnId,
        shardId,
        timestamp: Date.now(),
        preparedLSN: 100,
      };
    },
    async sendCommit(shardId: number, message: CommitMessage): Promise<AckMessage> {
      if (options.commitDelayMs) {
        await new Promise(resolve => setTimeout(resolve, options.commitDelayMs));
      }
      if (options.shouldFailCommit) {
        throw new Error('Commit failed');
      }
      commitCalls.push({ shardId, txnId: message.txnId });
      return {
        type: 'ack',
        txnId: message.txnId,
        shardId,
        timestamp: Date.now(),
        finalLSN: 200,
      };
    },
    async sendAbort(shardId: number, message: AbortMessage): Promise<AckMessage> {
      if (options.abortDelayMs) {
        await new Promise(resolve => setTimeout(resolve, options.abortDelayMs));
      }
      if (options.shouldFailAbort) {
        throw new Error('Abort failed');
      }
      abortCalls.push({ shardId, txnId: message.txnId, reason: message.reason });
      return {
        type: 'ack',
        txnId: message.txnId,
        shardId,
        timestamp: Date.now(),
      };
    },
    async queryStatus(
      shardId: number,
      message: StatusQueryMessage
    ): Promise<StatusResponseMessage> {
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
 * Create a mock ParticipantStorage for recovery tests.
 */
function createMockParticipantStorage(): ParticipantStorage & {
  savedTransactions: Map<string, PreparedTransaction>;
} {
  const savedTransactions = new Map<string, PreparedTransaction>();
  let currentLSN = 100;

  return {
    savedTransactions,
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
 * Create a mock CoordinatorDiscovery for participant recovery tests.
 */
function createMockCoordinatorDiscovery(options: {
  isAvailable?: boolean;
  decisions?: Map<string, 'commit' | 'abort' | null>;
} = {}): CoordinatorDiscovery {
  const { isAvailable = true, decisions = new Map() } = options;

  return {
    async isCoordinatorAvailable(): Promise<boolean> {
      return isAvailable;
    },
    async queryDecision(txnId: string): Promise<'commit' | 'abort' | null> {
      return decisions.get(txnId) ?? null;
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
  ];
}

// ============================================================================
// InMemoryTransactionLogStorage Tests
// ============================================================================

describe('InMemoryTransactionLogStorage', () => {
  let storage: InMemoryTransactionLogStorage;

  beforeEach(() => {
    storage = new InMemoryTransactionLogStorage();
  });

  it('should write and read log entries', async () => {
    const entry: TransactionLogEntry = {
      txnId: 'txn-1',
      state: 'preparing',
      participantShardIds: [0, 1, 2],
      loggedAt: Date.now(),
    };

    await storage.writeLogEntry(entry);
    const read = await storage.readLogEntry('txn-1');

    expect(read).toBeDefined();
    expect(read?.txnId).toBe('txn-1');
    expect(read?.state).toBe('preparing');
    expect(read?.participantShardIds).toEqual([0, 1, 2]);
  });

  it('should return null for non-existent entry', async () => {
    const read = await storage.readLogEntry('non-existent');
    expect(read).toBeNull();
  });

  it('should delete log entries', async () => {
    const entry: TransactionLogEntry = {
      txnId: 'txn-1',
      state: 'committed',
      participantShardIds: [0],
      loggedAt: Date.now(),
    };

    await storage.writeLogEntry(entry);
    await storage.deleteLogEntry('txn-1');

    const read = await storage.readLogEntry('txn-1');
    expect(read).toBeNull();
  });

  it('should list pending entries', async () => {
    const entries: TransactionLogEntry[] = [
      { txnId: 'txn-1', state: 'preparing', participantShardIds: [0], loggedAt: Date.now() },
      { txnId: 'txn-2', state: 'committed', participantShardIds: [1], loggedAt: Date.now() },
      { txnId: 'txn-3', state: 'committing', participantShardIds: [2], loggedAt: Date.now() },
      { txnId: 'txn-4', state: 'aborted', participantShardIds: [3], loggedAt: Date.now() },
    ];

    for (const entry of entries) {
      await storage.writeLogEntry(entry);
    }

    const pending = await storage.listPendingEntries();

    expect(pending.length).toBe(2);
    expect(pending.map(e => e.txnId)).toContain('txn-1');
    expect(pending.map(e => e.txnId)).toContain('txn-3');
  });

  it('should update log entries', async () => {
    const entry: TransactionLogEntry = {
      txnId: 'txn-1',
      state: 'preparing',
      participantShardIds: [0],
      loggedAt: Date.now(),
    };

    await storage.writeLogEntry(entry);
    await storage.updateLogEntry('txn-1', {
      state: 'committing',
      decision: 'commit',
      decisionAt: Date.now(),
    });

    const read = await storage.readLogEntry('txn-1');
    expect(read?.state).toBe('committing');
    expect(read?.decision).toBe('commit');
  });
});

// ============================================================================
// TransactionRecoveryManager Tests
// ============================================================================

describe('TransactionRecoveryManager', () => {
  let logStorage: InMemoryTransactionLogStorage;
  let rpc: ReturnType<typeof createMockShardRPC>;
  let recoveryManager: TransactionRecoveryManager;

  beforeEach(() => {
    logStorage = new InMemoryTransactionLogStorage();
    rpc = createMockShardRPC();
    recoveryManager = new TransactionRecoveryManager(logStorage, rpc, {
      maxRetries: 3,
      operationTimeoutMs: 5000,
    });
  });

  describe('Recovery of Committed Decisions', () => {
    it('should complete commit for transactions with commit decision', async () => {
      const entry: TransactionLogEntry = {
        txnId: 'txn-commit',
        state: 'committing',
        decision: 'commit',
        participantShardIds: [0, 1, 2],
        loggedAt: Date.now(),
        decisionAt: Date.now(),
      };

      await logStorage.writeLogEntry(entry);

      const results = await recoveryManager.recoverPendingTransactions();

      expect(results.length).toBe(1);
      expect(results[0].success).toBe(true);
      expect(results[0].action).toBe('commit');
      expect(results[0].completedShards).toEqual([0, 1, 2]);

      // Verify commit was sent to all shards
      expect(rpc.commitCalls.length).toBe(3);
      expect(rpc.commitCalls.map(c => c.shardId).sort()).toEqual([0, 1, 2]);

      // Verify log entry was cleaned up
      expect(await logStorage.readLogEntry('txn-commit')).toBeNull();
    });

    it('should handle partial commit failures', async () => {
      // Create RPC that fails for shard 1
      let callCount = 0;
      const failingRpc: ShardRPC = {
        ...rpc,
        async sendCommit(shardId, message) {
          callCount++;
          if (shardId === 1) {
            throw new Error('Shard 1 unavailable');
          }
          return { type: 'ack', txnId: message.txnId, shardId, timestamp: Date.now(), finalLSN: 200 };
        },
      };

      const manager = new TransactionRecoveryManager(logStorage, failingRpc, {
        maxRetries: 2,
      });

      const entry: TransactionLogEntry = {
        txnId: 'txn-partial',
        state: 'committing',
        decision: 'commit',
        participantShardIds: [0, 1, 2],
        loggedAt: Date.now(),
      };

      await logStorage.writeLogEntry(entry);

      const results = await manager.recoverPendingTransactions();

      expect(results.length).toBe(1);
      expect(results[0].success).toBe(false);
      expect(results[0].failedShards).toContain(1);
      expect(results[0].completedShards.length).toBe(2);
    });
  });

  describe('Recovery of Aborted Decisions', () => {
    it('should complete abort for transactions with abort decision', async () => {
      const entry: TransactionLogEntry = {
        txnId: 'txn-abort',
        state: 'aborting',
        decision: 'abort',
        participantShardIds: [0, 1],
        loggedAt: Date.now(),
        abortReason: 'Participant voted abort',
      };

      await logStorage.writeLogEntry(entry);

      const results = await recoveryManager.recoverPendingTransactions();

      expect(results.length).toBe(1);
      expect(results[0].success).toBe(true);
      expect(results[0].action).toBe('abort');

      // Verify abort was sent to all shards
      expect(rpc.abortCalls.length).toBe(2);
    });

    it('should use abort reason from log entry', async () => {
      const entry: TransactionLogEntry = {
        txnId: 'txn-abort-reason',
        state: 'aborting',
        decision: 'abort',
        participantShardIds: [0],
        loggedAt: Date.now(),
        abortReason: 'Custom abort reason',
      };

      await logStorage.writeLogEntry(entry);

      await recoveryManager.recoverPendingTransactions();

      expect(rpc.abortCalls.length).toBe(1);
      expect(rpc.abortCalls[0].reason).toContain('Custom abort reason');
    });
  });

  describe('Recovery of Undecided Transactions', () => {
    it('should abort transactions in preparing state (presumed abort)', async () => {
      const entry: TransactionLogEntry = {
        txnId: 'txn-preparing',
        state: 'preparing',
        participantShardIds: [0, 1],
        loggedAt: Date.now(),
      };

      await logStorage.writeLogEntry(entry);

      const results = await recoveryManager.recoverPendingTransactions();

      expect(results.length).toBe(1);
      expect(results[0].action).toBe('abort');

      // Verify abort was sent
      expect(rpc.abortCalls.length).toBe(2);
      expect(rpc.abortCalls[0].reason).toContain('crashed before decision');
    });

    it('should abort transactions in initialized state', async () => {
      const entry: TransactionLogEntry = {
        txnId: 'txn-initialized',
        state: 'initialized',
        participantShardIds: [0],
        loggedAt: Date.now(),
      };

      await logStorage.writeLogEntry(entry);

      const results = await recoveryManager.recoverPendingTransactions();

      expect(results.length).toBe(1);
      expect(results[0].action).toBe('abort');
    });

    it('should abort transactions in prepared state without decision', async () => {
      const entry: TransactionLogEntry = {
        txnId: 'txn-prepared-no-decision',
        state: 'prepared',
        participantShardIds: [0],
        loggedAt: Date.now(),
        // No decision field
      };

      await logStorage.writeLogEntry(entry);

      const results = await recoveryManager.recoverPendingTransactions();

      expect(results.length).toBe(1);
      expect(results[0].action).toBe('abort');
      expect(rpc.abortCalls[0].reason).toContain('no decision logged');
    });
  });

  describe('Transaction Age Handling', () => {
    it('should abort undecided transactions exceeding max age', async () => {
      const oldTime = Date.now() - 25 * 60 * 60 * 1000; // 25 hours ago

      // Transaction without decision (still in preparing state) exceeding max age
      const entry: TransactionLogEntry = {
        txnId: 'txn-old',
        state: 'preparing',
        // No decision - transaction was in progress when coordinator crashed
        participantShardIds: [0],
        loggedAt: oldTime,
      };

      await logStorage.writeLogEntry(entry);

      const results = await recoveryManager.recoverPendingTransactions();

      expect(results.length).toBe(1);
      expect(results[0].action).toBe('abort');
      expect(rpc.abortCalls[0].reason).toContain('exceeded max age');
    });

    it('should honor commit decision even for old transactions (2PC guarantee)', async () => {
      const oldTime = Date.now() - 25 * 60 * 60 * 1000; // 25 hours ago

      // Once a commit decision is made, we must honor it (2PC durability guarantee)
      const entry: TransactionLogEntry = {
        txnId: 'txn-old-committed',
        state: 'committing',
        decision: 'commit',
        participantShardIds: [0],
        loggedAt: oldTime,
      };

      await logStorage.writeLogEntry(entry);

      const results = await recoveryManager.recoverPendingTransactions();

      expect(results.length).toBe(1);
      expect(results[0].action).toBe('commit');
      expect(rpc.commitCalls.length).toBe(1);
    });
  });

  describe('Heuristic Decisions', () => {
    it('should apply heuristic abort when enabled and threshold met', async () => {
      const manager = new TransactionRecoveryManager(logStorage, rpc, {
        enableHeuristicDecisions: true,
        defaultHeuristicAction: 'abort',
        heuristicThresholdMs: 1000, // 1 second
      });

      const oldTime = Date.now() - 5000; // 5 seconds ago
      const entry: TransactionLogEntry = {
        txnId: 'txn-heuristic',
        state: 'prepared',
        participantShardIds: [0],
        loggedAt: oldTime,
      };

      await logStorage.writeLogEntry(entry);

      const results = await manager.recoverPendingTransactions();

      expect(results.length).toBe(1);
      expect(results[0].action).toBe('heuristic_abort');
    });

    it('should apply heuristic commit when configured', async () => {
      const manager = new TransactionRecoveryManager(logStorage, rpc, {
        enableHeuristicDecisions: true,
        defaultHeuristicAction: 'commit',
        heuristicThresholdMs: 1000,
      });

      const oldTime = Date.now() - 5000;
      const entry: TransactionLogEntry = {
        txnId: 'txn-heuristic-commit',
        state: 'prepared',
        participantShardIds: [0],
        loggedAt: oldTime,
      };

      await logStorage.writeLogEntry(entry);

      const results = await manager.recoverPendingTransactions();

      expect(results.length).toBe(1);
      expect(results[0].action).toBe('heuristic_commit');
      expect(rpc.commitCalls.length).toBe(1);
    });
  });

  describe('Transaction Logging', () => {
    it('should log new transaction', async () => {
      const txn: DistributedTransaction = {
        txnId: 'txn-new',
        state: 'preparing',
        participants: new Map([
          [0, { shardId: 0, operations: [], state: 'preparing', lastStateChange: Date.now() }],
          [1, { shardId: 1, operations: [], state: 'preparing', lastStateChange: Date.now() }],
        ]),
        startTime: Date.now(),
        prepareDeadline: Date.now() + 5000,
        commitDeadline: 0,
      };

      await recoveryManager.logTransaction(txn);

      const entry = await logStorage.readLogEntry('txn-new');
      expect(entry).toBeDefined();
      expect(entry?.state).toBe('preparing');
      expect(entry?.participantShardIds).toEqual([0, 1]);
    });

    it('should log transaction decision', async () => {
      const entry: TransactionLogEntry = {
        txnId: 'txn-decision',
        state: 'preparing',
        participantShardIds: [0],
        loggedAt: Date.now(),
      };

      await logStorage.writeLogEntry(entry);
      await recoveryManager.logDecision('txn-decision', 'commit');

      const updated = await logStorage.readLogEntry('txn-decision');
      expect(updated?.decision).toBe('commit');
      expect(updated?.state).toBe('committing');
      expect(updated?.decisionAt).toBeDefined();
    });

    it('should log abort decision with reason', async () => {
      const entry: TransactionLogEntry = {
        txnId: 'txn-abort-decision',
        state: 'preparing',
        participantShardIds: [0],
        loggedAt: Date.now(),
      };

      await logStorage.writeLogEntry(entry);
      await recoveryManager.logDecision('txn-abort-decision', 'abort', 'Validation failed');

      const updated = await logStorage.readLogEntry('txn-abort-decision');
      expect(updated?.decision).toBe('abort');
      expect(updated?.abortReason).toBe('Validation failed');
    });

    it('should complete and delete transaction log', async () => {
      const entry: TransactionLogEntry = {
        txnId: 'txn-complete',
        state: 'committing',
        decision: 'commit',
        participantShardIds: [0],
        loggedAt: Date.now(),
      };

      await logStorage.writeLogEntry(entry);
      await recoveryManager.completeTransaction('txn-complete');

      expect(await logStorage.readLogEntry('txn-complete')).toBeNull();
    });
  });

  describe('Multiple Transaction Recovery', () => {
    it('should recover multiple pending transactions', async () => {
      const entries: TransactionLogEntry[] = [
        {
          txnId: 'txn-1',
          state: 'committing',
          decision: 'commit',
          participantShardIds: [0],
          loggedAt: Date.now(),
        },
        {
          txnId: 'txn-2',
          state: 'aborting',
          decision: 'abort',
          participantShardIds: [1],
          loggedAt: Date.now(),
          abortReason: 'Test abort',
        },
        {
          txnId: 'txn-3',
          state: 'preparing',
          participantShardIds: [2],
          loggedAt: Date.now(),
        },
      ];

      for (const entry of entries) {
        await logStorage.writeLogEntry(entry);
      }

      const results = await recoveryManager.recoverPendingTransactions();

      expect(results.length).toBe(3);
      expect(results.filter(r => r.success).length).toBe(3);

      // Verify appropriate actions were taken
      const commitResults = results.filter(r => r.action === 'commit');
      const abortResults = results.filter(r => r.action === 'abort');

      expect(commitResults.length).toBe(1);
      expect(abortResults.length).toBe(2);
    });
  });
});

// ============================================================================
// ParticipantRecoveryManager Tests
// ============================================================================

describe('ParticipantRecoveryManager', () => {
  let storage: ReturnType<typeof createMockParticipantStorage>;
  let discovery: CoordinatorDiscovery;
  let commitCalls: string[];
  let abortCalls: string[];
  let recoveryManager: ParticipantRecoveryManager;

  beforeEach(() => {
    storage = createMockParticipantStorage();
    discovery = createMockCoordinatorDiscovery({ isAvailable: true });
    commitCalls = [];
    abortCalls = [];

    recoveryManager = new ParticipantRecoveryManager(
      0, // shardId
      storage,
      discovery,
      async (txnId) => { commitCalls.push(txnId); },
      async (txnId) => { abortCalls.push(txnId); },
      {
        maxRetries: 3,
        heuristicThresholdMs: 10000,
      }
    );
  });

  describe('Recovery with Coordinator Available', () => {
    it('should commit when coordinator says commit', async () => {
      const decisions = new Map<string, 'commit' | 'abort' | null>();
      decisions.set('txn-commit', 'commit');
      discovery = createMockCoordinatorDiscovery({ isAvailable: true, decisions });

      recoveryManager = new ParticipantRecoveryManager(
        0,
        storage,
        discovery,
        async (txnId) => { commitCalls.push(txnId); },
        async (txnId) => { abortCalls.push(txnId); }
      );

      const preparedTxn: PreparedTransaction = {
        txnId: 'txn-commit',
        state: 'prepared',
        operations: createTestOperations(),
        preparedLSN: 100,
        prepareDeadline: Date.now() + 30000,
        preparedAt: Date.now(),
      };

      storage.savedTransactions.set('txn-commit', preparedTxn);

      const results = await recoveryManager.recoverPreparedTransactions();

      expect(results.length).toBe(1);
      expect(results[0].action).toBe('commit');
      expect(commitCalls).toContain('txn-commit');
      expect(abortCalls).not.toContain('txn-commit');
    });

    it('should abort when coordinator says abort', async () => {
      const decisions = new Map<string, 'commit' | 'abort' | null>();
      decisions.set('txn-abort', 'abort');
      discovery = createMockCoordinatorDiscovery({ isAvailable: true, decisions });

      recoveryManager = new ParticipantRecoveryManager(
        0,
        storage,
        discovery,
        async (txnId) => { commitCalls.push(txnId); },
        async (txnId) => { abortCalls.push(txnId); }
      );

      const preparedTxn: PreparedTransaction = {
        txnId: 'txn-abort',
        state: 'prepared',
        operations: createTestOperations(),
        preparedLSN: 100,
        prepareDeadline: Date.now() + 30000,
        preparedAt: Date.now(),
      };

      storage.savedTransactions.set('txn-abort', preparedTxn);

      const results = await recoveryManager.recoverPreparedTransactions();

      expect(results.length).toBe(1);
      expect(results[0].action).toBe('abort');
      expect(abortCalls).toContain('txn-abort');
    });

    it('should abort when coordinator has no record (presumed abort)', async () => {
      // No decision in map = null returned
      discovery = createMockCoordinatorDiscovery({ isAvailable: true, decisions: new Map() });

      recoveryManager = new ParticipantRecoveryManager(
        0,
        storage,
        discovery,
        async (txnId) => { commitCalls.push(txnId); },
        async (txnId) => { abortCalls.push(txnId); }
      );

      const preparedTxn: PreparedTransaction = {
        txnId: 'txn-unknown',
        state: 'prepared',
        operations: createTestOperations(),
        preparedLSN: 100,
        prepareDeadline: Date.now() + 30000,
        preparedAt: Date.now(),
      };

      storage.savedTransactions.set('txn-unknown', preparedTxn);

      const results = await recoveryManager.recoverPreparedTransactions();

      expect(results.length).toBe(1);
      expect(results[0].action).toBe('abort');
      expect(abortCalls).toContain('txn-unknown');
    });
  });

  describe('Recovery with Coordinator Unavailable', () => {
    it('should leave transaction in prepared state when within timeout', async () => {
      discovery = createMockCoordinatorDiscovery({ isAvailable: false });

      recoveryManager = new ParticipantRecoveryManager(
        0,
        storage,
        discovery,
        async (txnId) => { commitCalls.push(txnId); },
        async (txnId) => { abortCalls.push(txnId); },
        {
          heuristicThresholdMs: 60000, // 1 minute
        }
      );

      const preparedTxn: PreparedTransaction = {
        txnId: 'txn-wait',
        state: 'prepared',
        operations: createTestOperations(),
        preparedLSN: 100,
        prepareDeadline: Date.now() + 30000,
        preparedAt: Date.now(),
      };

      storage.savedTransactions.set('txn-wait', preparedTxn);

      const results = await recoveryManager.recoverPreparedTransactions();

      expect(results.length).toBe(1);
      expect(results[0].action).toBe('query'); // Left in prepared state
      expect(commitCalls.length).toBe(0);
      expect(abortCalls.length).toBe(0);
    });

    it('should apply heuristic decision after threshold', async () => {
      discovery = createMockCoordinatorDiscovery({ isAvailable: false });

      recoveryManager = new ParticipantRecoveryManager(
        0,
        storage,
        discovery,
        async (txnId) => { commitCalls.push(txnId); },
        async (txnId) => { abortCalls.push(txnId); },
        {
          enableHeuristicDecisions: true,
          defaultHeuristicAction: 'abort',
          heuristicThresholdMs: 1000, // 1 second
        }
      );

      const oldTime = Date.now() - 5000; // 5 seconds ago
      const preparedTxn: PreparedTransaction = {
        txnId: 'txn-heuristic',
        state: 'prepared',
        operations: createTestOperations(),
        preparedLSN: 100,
        prepareDeadline: oldTime + 30000,
        preparedAt: oldTime,
      };

      storage.savedTransactions.set('txn-heuristic', preparedTxn);

      const results = await recoveryManager.recoverPreparedTransactions();

      expect(results.length).toBe(1);
      expect(results[0].action).toBe('heuristic_abort');
      expect(abortCalls).toContain('txn-heuristic');
    });
  });

  describe('Transaction Age Handling', () => {
    it('should abort transactions exceeding max age', async () => {
      const oldTime = Date.now() - 25 * 60 * 60 * 1000; // 25 hours ago

      const preparedTxn: PreparedTransaction = {
        txnId: 'txn-old',
        state: 'prepared',
        operations: createTestOperations(),
        preparedLSN: 100,
        prepareDeadline: oldTime + 30000,
        preparedAt: oldTime,
      };

      storage.savedTransactions.set('txn-old', preparedTxn);

      const results = await recoveryManager.recoverPreparedTransactions();

      expect(results.length).toBe(1);
      expect(results[0].action).toBe('abort');
      expect(abortCalls).toContain('txn-old');
    });
  });

  describe('Multiple Transaction Recovery', () => {
    it('should recover multiple prepared transactions', async () => {
      const decisions = new Map<string, 'commit' | 'abort' | null>();
      decisions.set('txn-1', 'commit');
      decisions.set('txn-2', 'abort');
      // txn-3 has no decision
      discovery = createMockCoordinatorDiscovery({ isAvailable: true, decisions });

      recoveryManager = new ParticipantRecoveryManager(
        0,
        storage,
        discovery,
        async (txnId) => { commitCalls.push(txnId); },
        async (txnId) => { abortCalls.push(txnId); }
      );

      const now = Date.now();
      const transactions: PreparedTransaction[] = [
        {
          txnId: 'txn-1',
          state: 'prepared',
          operations: createTestOperations(),
          preparedLSN: 100,
          prepareDeadline: now + 30000,
          preparedAt: now,
        },
        {
          txnId: 'txn-2',
          state: 'prepared',
          operations: createTestOperations(),
          preparedLSN: 101,
          prepareDeadline: now + 30000,
          preparedAt: now,
        },
        {
          txnId: 'txn-3',
          state: 'prepared',
          operations: createTestOperations(),
          preparedLSN: 102,
          prepareDeadline: now + 30000,
          preparedAt: now,
        },
      ];

      for (const txn of transactions) {
        storage.savedTransactions.set(txn.txnId, txn);
      }

      const results = await recoveryManager.recoverPreparedTransactions();

      expect(results.length).toBe(3);
      expect(commitCalls).toContain('txn-1');
      expect(abortCalls).toContain('txn-2');
      expect(abortCalls).toContain('txn-3'); // No decision = presumed abort
    });
  });
});

// ============================================================================
// Factory Function Tests
// ============================================================================

describe('Factory Functions', () => {
  it('should create TransactionRecoveryManager', () => {
    const storage = new InMemoryTransactionLogStorage();
    const rpc = createMockShardRPC();

    const manager = createTransactionRecoveryManager(storage, rpc, {
      maxRetries: 5,
    });

    expect(manager).toBeInstanceOf(TransactionRecoveryManager);
  });

  it('should create ParticipantRecoveryManager', () => {
    const storage = createMockParticipantStorage();
    const discovery = createMockCoordinatorDiscovery();

    const manager = createParticipantRecoveryManager(
      0,
      storage,
      discovery,
      async () => {},
      async () => {},
      { maxRetries: 5 }
    );

    expect(manager).toBeInstanceOf(ParticipantRecoveryManager);
  });
});

// ============================================================================
// Edge Cases and Error Handling
// ============================================================================

describe('Recovery Edge Cases', () => {
  let logStorage: InMemoryTransactionLogStorage;
  let rpc: ReturnType<typeof createMockShardRPC>;

  beforeEach(() => {
    logStorage = new InMemoryTransactionLogStorage();
    rpc = createMockShardRPC();
  });

  it('should handle empty pending list', async () => {
    const manager = new TransactionRecoveryManager(logStorage, rpc);

    const results = await manager.recoverPendingTransactions();

    expect(results.length).toBe(0);
  });

  it('should handle completed transactions in log', async () => {
    const entry: TransactionLogEntry = {
      txnId: 'txn-already-done',
      state: 'committed',
      decision: 'commit',
      participantShardIds: [],
      loggedAt: Date.now(),
    };

    await logStorage.writeLogEntry(entry);

    const manager = new TransactionRecoveryManager(logStorage, rpc);
    const results = await manager.recoverPendingTransactions();

    // Completed transactions should not be in pending list
    expect(results.length).toBe(0);
  });

  it('should continue recovery after individual transaction failure', async () => {
    // Create RPC that fails for specific transaction
    let failOnTxn = 'txn-fail';
    const failingRpc: ShardRPC = {
      ...rpc,
      async sendCommit(shardId, message) {
        if (message.txnId === failOnTxn) {
          throw new Error('Failed');
        }
        return { type: 'ack', txnId: message.txnId, shardId, timestamp: Date.now(), finalLSN: 200 };
      },
    };

    const manager = new TransactionRecoveryManager(logStorage, failingRpc, {
      maxRetries: 1,
    });

    const entries: TransactionLogEntry[] = [
      {
        txnId: 'txn-fail',
        state: 'committing',
        decision: 'commit',
        participantShardIds: [0],
        loggedAt: Date.now(),
      },
      {
        txnId: 'txn-success',
        state: 'committing',
        decision: 'commit',
        participantShardIds: [1],
        loggedAt: Date.now(),
      },
    ];

    for (const entry of entries) {
      await logStorage.writeLogEntry(entry);
    }

    const results = await manager.recoverPendingTransactions();

    expect(results.length).toBe(2);
    expect(results.find(r => r.txnId === 'txn-fail')?.success).toBe(false);
    expect(results.find(r => r.txnId === 'txn-success')?.success).toBe(true);
  });

  it('should handle RPC failures gracefully for abort', async () => {
    const failingRpc = createMockShardRPC({ shouldFailAbort: true });

    const manager = new TransactionRecoveryManager(logStorage, failingRpc, {
      maxRetries: 1,
    });

    const entry: TransactionLogEntry = {
      txnId: 'txn-abort-fail',
      state: 'aborting',
      decision: 'abort',
      participantShardIds: [0],
      loggedAt: Date.now(),
    };

    await logStorage.writeLogEntry(entry);

    const results = await manager.recoverPendingTransactions();

    // Abort is best-effort, should still succeed overall
    expect(results.length).toBe(1);
    expect(results[0].success).toBe(true);
  });
});
