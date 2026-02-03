/**
 * TransactionCoordinator Unit Tests
 *
 * Tests for the Two-Phase Commit (2PC) protocol implementation.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  TransactionCoordinator,
  createTransactionCoordinator,
  DistributedTransactionError,
  TransactionTimeoutError,
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
  type CommitRetryMetrics,
  type InterventionHook,
} from '../../../src/transaction/coordinator.js';
import { ShardRouter } from '../../../src/shard/router.js';
import type { BufferedOperation } from '../../../src/session/index.js';

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Create a mock ShardRPC implementation.
 */
function createMockShardRPC(options: {
  prepareResponses?: Map<number, 'prepared' | 'abort'>;
  prepareDelays?: Map<number, number>;
  commitDelays?: Map<number, number>;
  shouldFailCommit?: boolean;
} = {}): ShardRPC {
  const {
    prepareResponses = new Map(),
    prepareDelays = new Map(),
    commitDelays = new Map(),
    shouldFailCommit = false,
  } = options;

  let preparedLSN = 100;

  return {
    async sendPrepare(
      shardId: number,
      message: PrepareMessage
    ): Promise<PreparedMessage | AbortVoteMessage> {
      // Apply delay if specified
      const delay = prepareDelays.get(shardId);
      if (delay) {
        await new Promise((resolve) => setTimeout(resolve, delay));
      }

      // Check configured response
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

      return {
        type: 'prepared',
        txnId: message.txnId,
        shardId,
        timestamp: Date.now(),
        preparedLSN: preparedLSN++,
      };
    },

    async sendCommit(shardId: number, message: CommitMessage): Promise<AckMessage> {
      if (shouldFailCommit) {
        throw new Error('Commit failed');
      }

      const delay = commitDelays.get(shardId);
      if (delay) {
        await new Promise((resolve) => setTimeout(resolve, delay));
      }

      return {
        type: 'ack',
        txnId: message.txnId,
        shardId,
        timestamp: Date.now(),
        finalLSN: 200,
      };
    },

    async sendAbort(shardId: number, message: AbortMessage): Promise<AckMessage> {
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
 * Create test operations targeting multiple shards.
 */
function createMultiShardOperations(): BufferedOperation[] {
  return [
    {
      type: 'insert',
      collection: 'users',
      database: 'db1',
      document: { _id: 'user1', name: 'Alice' },
      timestamp: Date.now(),
    },
    {
      type: 'insert',
      collection: 'orders',
      database: 'db1',
      document: { _id: 'order1', userId: 'user1' },
      timestamp: Date.now(),
    },
    {
      type: 'update',
      collection: 'inventory',
      database: 'db2',
      filter: { _id: 'item1' },
      update: { $inc: { quantity: -1 } },
      timestamp: Date.now(),
    },
  ];
}

/**
 * Create test operations targeting a single shard.
 */
function createSingleShardOperations(): BufferedOperation[] {
  return [
    {
      type: 'insert',
      collection: 'users',
      database: 'db1',
      document: { _id: 'user1', name: 'Alice' },
      timestamp: Date.now(),
    },
    {
      type: 'update',
      collection: 'users',
      database: 'db1',
      filter: { _id: 'user1' },
      update: { $set: { name: 'Alice Updated' } },
      timestamp: Date.now(),
    },
  ];
}

// ============================================================================
// Factory Function Tests
// ============================================================================

describe('createTransactionCoordinator', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should create a TransactionCoordinator instance', () => {
    const router = new ShardRouter();
    const rpc = createMockShardRPC();

    const coordinator = createTransactionCoordinator(router, rpc);

    expect(coordinator).toBeInstanceOf(TransactionCoordinator);
  });

  it('should accept custom options', () => {
    const router = new ShardRouter();
    const rpc = createMockShardRPC();

    const coordinator = createTransactionCoordinator(router, rpc, {
      prepareTimeoutMs: 10000,
      commitTimeoutMs: 20000,
      maxRetries: 5,
      retryDelayMs: 200,
    });

    expect(coordinator).toBeInstanceOf(TransactionCoordinator);
  });
});

// ============================================================================
// Single-Shard Transaction Tests
// ============================================================================

describe('TransactionCoordinator - Single Shard Transactions', () => {
  let router: ShardRouter;
  let rpc: ShardRPC;
  let coordinator: TransactionCoordinator;

  beforeEach(() => {
    vi.useFakeTimers();
    router = new ShardRouter();
    rpc = createMockShardRPC();
    coordinator = new TransactionCoordinator(router, rpc);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should bypass 2PC for single-shard transactions', async () => {
    const operations = createSingleShardOperations();

    const result = await coordinator.execute(operations);

    expect(result.committed).toBe(true);
    expect(result.state).toBe('committed');
    expect(result.durationMs).toBeGreaterThanOrEqual(0);
  });

  it('should handle single-shard abort', async () => {
    // Configure shard to abort
    rpc = createMockShardRPC({
      prepareResponses: new Map([[router.routeWithDatabase('db1', 'users').shardId, 'abort']]),
    });
    coordinator = new TransactionCoordinator(router, rpc);

    const operations = createSingleShardOperations();

    const result = await coordinator.execute(operations);

    expect(result.committed).toBe(false);
    expect(result.state).toBe('aborted');
    expect(result.abortReason).toBe('Simulated abort');
  });
});

// ============================================================================
// Multi-Shard Transaction Tests (2PC)
// ============================================================================

describe('TransactionCoordinator - Multi-Shard Transactions (2PC)', () => {
  let router: ShardRouter;
  let rpc: ShardRPC;
  let coordinator: TransactionCoordinator;

  beforeEach(() => {
    vi.useFakeTimers();
    router = new ShardRouter();
    rpc = createMockShardRPC();
    coordinator = new TransactionCoordinator(router, rpc);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should commit when all participants prepare successfully', async () => {
    const operations = createMultiShardOperations();

    const result = await coordinator.execute(operations);

    expect(result.committed).toBe(true);
    expect(result.state).toBe('committed');
    expect(result.txnId).toMatch(/^txn-/);
  });

  it('should abort when any participant votes abort', async () => {
    // Get the shard for inventory collection
    const inventoryShardId = router.routeWithDatabase('db2', 'inventory').shardId;

    rpc = createMockShardRPC({
      prepareResponses: new Map([[inventoryShardId, 'abort']]),
    });
    coordinator = new TransactionCoordinator(router, rpc);

    const operations = createMultiShardOperations();

    const result = await coordinator.execute(operations);

    expect(result.committed).toBe(false);
    expect(result.state).toBe('aborted');
    expect(result.abortReason).toContain('Simulated abort');
  });

  it('should abort when prepare times out', async () => {
    // Set a very short timeout
    coordinator = new TransactionCoordinator(router, rpc, {
      prepareTimeoutMs: 10, // Very short timeout
      maxRetries: 1,
    });

    // Add delay to prepare responses
    const usersShardId = router.routeWithDatabase('db1', 'users').shardId;
    rpc = createMockShardRPC({
      prepareDelays: new Map([[usersShardId, 100]]), // 100ms delay
    });
    coordinator = new TransactionCoordinator(router, rpc, {
      prepareTimeoutMs: 10,
      maxRetries: 1,
    });

    const operations = createMultiShardOperations();

    // Start execute but don't await immediately
    const executePromise = coordinator.execute(operations);

    // Advance timers to allow prepare timeout and delays to complete
    await vi.advanceTimersByTimeAsync(200);

    const result = await executePromise;

    expect(result.committed).toBe(false);
    expect(result.state).toBe('aborted');
  });

  it('should generate unique transaction IDs', async () => {
    const txnIds = new Set<string>();

    for (let i = 0; i < 10; i++) {
      const operations = createMultiShardOperations();
      const result = await coordinator.execute(operations);
      txnIds.add(result.txnId);
    }

    expect(txnIds.size).toBe(10);
  });

  it('should track duration correctly', async () => {
    const operations = createMultiShardOperations();

    const startTime = Date.now();
    const result = await coordinator.execute(operations);
    const endTime = Date.now();

    expect(result.durationMs).toBeGreaterThanOrEqual(0);
    expect(result.durationMs).toBeLessThanOrEqual(endTime - startTime + 10);
  });
});

// ============================================================================
// Operation Partitioning Tests
// ============================================================================

describe('TransactionCoordinator - Operation Partitioning', () => {
  let router: ShardRouter;
  let rpc: ShardRPC;
  let coordinator: TransactionCoordinator;
  let prepareMessages: PrepareMessage[];

  beforeEach(() => {
    vi.useFakeTimers();
    router = new ShardRouter();
    prepareMessages = [];

    // Capture prepare messages
    rpc = {
      async sendPrepare(shardId, message) {
        prepareMessages.push(message);
        return {
          type: 'prepared' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          preparedLSN: 100,
        };
      },
      async sendCommit(shardId, message) {
        return {
          type: 'ack' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          finalLSN: 200,
        };
      },
      async sendAbort(shardId, message) {
        return {
          type: 'ack' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
        };
      },
      async queryStatus(shardId, message) {
        return {
          type: 'status_response' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          participantState: 'done' as const,
        };
      },
    };
    coordinator = new TransactionCoordinator(router, rpc);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should partition operations by shard', async () => {
    const operations = createMultiShardOperations();

    await coordinator.execute(operations);

    // Should have sent prepare to multiple shards
    expect(prepareMessages.length).toBeGreaterThan(0);

    // Each message should have operations
    for (const message of prepareMessages) {
      expect(message.operations.length).toBeGreaterThan(0);
    }
  });

  it('should group operations for the same shard', async () => {
    // All operations to the same collection/database
    const operations: BufferedOperation[] = [
      {
        type: 'insert',
        collection: 'users',
        database: 'testdb',
        document: { _id: 'user1', name: 'Alice' },
        timestamp: Date.now(),
      },
      {
        type: 'insert',
        collection: 'users',
        database: 'testdb',
        document: { _id: 'user2', name: 'Bob' },
        timestamp: Date.now(),
      },
      {
        type: 'update',
        collection: 'users',
        database: 'testdb',
        filter: { _id: 'user1' },
        update: { $set: { name: 'Alice Updated' } },
        timestamp: Date.now(),
      },
    ];

    await coordinator.execute(operations);

    // Should only send to one shard (bypassing 2PC)
    // For single-shard, we still send prepare, so we check that all ops go together
    expect(prepareMessages.length).toBe(1);
    expect(prepareMessages[0].operations.length).toBe(3);
  });
});

// ============================================================================
// Active Transaction Tracking Tests
// ============================================================================

describe('TransactionCoordinator - Transaction Tracking', () => {
  let router: ShardRouter;
  let rpc: ShardRPC;
  let coordinator: TransactionCoordinator;

  beforeEach(() => {
    vi.useFakeTimers();
    router = new ShardRouter();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should track active transactions during execution', async () => {
    let capturedActiveCount = 0;

    rpc = {
      async sendPrepare(shardId, message) {
        capturedActiveCount = coordinator.getActiveTransactions().length;
        return {
          type: 'prepared' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          preparedLSN: 100,
        };
      },
      async sendCommit(shardId, message) {
        return {
          type: 'ack' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          finalLSN: 200,
        };
      },
      async sendAbort() {
        return { type: 'ack' as const, txnId: '', shardId: 0, timestamp: 0 };
      },
      async queryStatus() {
        return {
          type: 'status_response' as const,
          txnId: '',
          shardId: 0,
          timestamp: 0,
          participantState: 'done' as const,
        };
      },
    };
    coordinator = new TransactionCoordinator(router, rpc);

    const operations = createMultiShardOperations();
    await coordinator.execute(operations);

    // Transaction should have been active during prepare
    expect(capturedActiveCount).toBeGreaterThan(0);

    // After completion, no active transactions
    expect(coordinator.getActiveTransactions().length).toBe(0);
  });

  it('should clean up transaction after abort', async () => {
    const inventoryShardId = router.routeWithDatabase('db2', 'inventory').shardId;

    rpc = createMockShardRPC({
      prepareResponses: new Map([[inventoryShardId, 'abort']]),
    });
    coordinator = new TransactionCoordinator(router, rpc);

    const operations = createMultiShardOperations();
    await coordinator.execute(operations);

    expect(coordinator.getActiveTransactions().length).toBe(0);
  });
});

// ============================================================================
// Error Handling Tests
// ============================================================================

describe('TransactionCoordinator - Error Handling', () => {
  let router: ShardRouter;
  let coordinator: TransactionCoordinator;

  beforeEach(() => {
    vi.useFakeTimers();
    router = new ShardRouter();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should handle RPC failures during prepare with retry for multi-shard', async () => {
    // Use multi-shard operations to test the 2PC retry path
    let attempts = 0;

    const rpc: ShardRPC = {
      async sendPrepare(shardId, message) {
        attempts++;
        if (attempts < 3) {
          throw new Error('Network error');
        }
        return {
          type: 'prepared' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          preparedLSN: 100,
        };
      },
      async sendCommit(shardId, message) {
        return {
          type: 'ack' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          finalLSN: 200,
        };
      },
      async sendAbort() {
        return { type: 'ack' as const, txnId: '', shardId: 0, timestamp: 0 };
      },
      async queryStatus() {
        return {
          type: 'status_response' as const,
          txnId: '',
          shardId: 0,
          timestamp: 0,
          participantState: 'done' as const,
        };
      },
    };

    coordinator = new TransactionCoordinator(router, rpc, {
      maxRetries: 5,
      retryDelayMs: 1,
    });

    const operations = createMultiShardOperations();

    // Start execute and advance timers to allow retries
    const executePromise = coordinator.execute(operations);

    // Advance timers to allow retry delays to complete (exponential backoff: 2, 4, 8, 16, 32ms)
    await vi.advanceTimersByTimeAsync(100);

    const result = await executePromise;

    // Multi-shard transaction should succeed after retries
    expect(result.committed).toBe(true);
    // Should have retried (exact count depends on shard count)
    expect(attempts).toBeGreaterThanOrEqual(3);
  });

  it('should abort after exhausting retries', async () => {
    const rpc: ShardRPC = {
      async sendPrepare() {
        throw new Error('Network error');
      },
      async sendCommit() {
        return { type: 'ack' as const, txnId: '', shardId: 0, timestamp: 0, finalLSN: 0 };
      },
      async sendAbort() {
        return { type: 'ack' as const, txnId: '', shardId: 0, timestamp: 0 };
      },
      async queryStatus() {
        return {
          type: 'status_response' as const,
          txnId: '',
          shardId: 0,
          timestamp: 0,
          participantState: 'done' as const,
        };
      },
    };

    coordinator = new TransactionCoordinator(router, rpc, {
      maxRetries: 2,
      retryDelayMs: 1,
    });

    const operations = createMultiShardOperations();

    // Start execute and advance timers to allow retry delays to complete
    const executePromise = coordinator.execute(operations);
    await vi.advanceTimersByTimeAsync(100);
    const result = await executePromise;

    expect(result.committed).toBe(false);
    expect(result.abortReason).toContain('Network error');
  });

  it('should throw DistributedTransactionError for single-shard failures', async () => {
    const rpc: ShardRPC = {
      async sendPrepare() {
        throw new Error('Fatal error');
      },
      async sendCommit() {
        return { type: 'ack' as const, txnId: '', shardId: 0, timestamp: 0, finalLSN: 0 };
      },
      async sendAbort() {
        return { type: 'ack' as const, txnId: '', shardId: 0, timestamp: 0 };
      },
      async queryStatus() {
        return {
          type: 'status_response' as const,
          txnId: '',
          shardId: 0,
          timestamp: 0,
          participantState: 'done' as const,
        };
      },
    };

    coordinator = new TransactionCoordinator(router, rpc, {
      maxRetries: 1,
      retryDelayMs: 1,
    });

    const operations = createSingleShardOperations();

    await expect(coordinator.execute(operations)).rejects.toThrow(
      DistributedTransactionError
    );
  });
});

// ============================================================================
// Edge Cases
// ============================================================================

describe('TransactionCoordinator - Edge Cases', () => {
  let router: ShardRouter;
  let rpc: ShardRPC;
  let coordinator: TransactionCoordinator;

  beforeEach(() => {
    vi.useFakeTimers();
    router = new ShardRouter();
    rpc = createMockShardRPC();
    coordinator = new TransactionCoordinator(router, rpc);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should handle empty operations list', async () => {
    const operations: BufferedOperation[] = [];

    // Empty operations should be handled gracefully
    // The coordinator should bypass since there's nothing to partition
    const result = await coordinator.execute(operations);

    // With no operations, we still get a result (empty transaction)
    expect(result).toBeDefined();
  });

  it('should handle operations with same document across shards', async () => {
    // Operations that affect related data across shards
    const operations: BufferedOperation[] = [
      {
        type: 'insert',
        collection: 'users',
        database: 'db1',
        document: { _id: 'user1', name: 'Alice' },
        timestamp: Date.now(),
      },
      {
        type: 'insert',
        collection: 'user_profiles',
        database: 'db2',
        document: { _id: 'profile1', userId: 'user1', bio: 'Hello' },
        timestamp: Date.now(),
      },
    ];

    const result = await coordinator.execute(operations);

    expect(result.committed).toBe(true);
  });
});

// ============================================================================
// Circuit Breaker Tests
// ============================================================================

describe('TransactionCoordinator - Circuit Breaker', () => {
  let router: ShardRouter;

  beforeEach(() => {
    vi.useFakeTimers();
    router = new ShardRouter();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should throw MaxCommitAttemptsError after max attempts exceeded', async () => {
    let commitAttempts = 0;

    const rpc: ShardRPC = {
      async sendPrepare(shardId, message) {
        return {
          type: 'prepared' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          preparedLSN: 100,
        };
      },
      async sendCommit() {
        commitAttempts++;
        throw new Error('Persistent failure');
      },
      async sendAbort(shardId, message) {
        return { type: 'ack' as const, txnId: message.txnId, shardId, timestamp: Date.now() };
      },
      async queryStatus(shardId, message) {
        return {
          type: 'status_response' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          participantState: 'done' as const,
        };
      },
    };

    const coordinator = new TransactionCoordinator(router, rpc, {
      maxCommitAttempts: 5,
      retryDelayMs: 1,
      circuitBreakerThreshold: 100, // High threshold to test max attempts first
    });

    const operations = createMultiShardOperations();

    // Start execute and advance timers to allow retry delays to complete
    const executePromise = coordinator.execute(operations);
    // Exponential backoff: 2, 4, 8, 16, 32... need enough time for multiple attempts per shard
    await vi.advanceTimersByTimeAsync(1000);
    const result = await executePromise;

    // Transaction should not be committed
    expect(result.committed).toBe(false);
    // Should have hit max attempts
    expect(commitAttempts).toBeGreaterThanOrEqual(5);
  });

  it('should trip circuit breaker after threshold failures', async () => {
    let commitAttempts = 0;

    const rpc: ShardRPC = {
      async sendPrepare(shardId, message) {
        return {
          type: 'prepared' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          preparedLSN: 100,
        };
      },
      async sendCommit() {
        commitAttempts++;
        throw new Error('Network failure');
      },
      async sendAbort(shardId, message) {
        return { type: 'ack' as const, txnId: message.txnId, shardId, timestamp: Date.now() };
      },
      async queryStatus(shardId, message) {
        return {
          type: 'status_response' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          participantState: 'done' as const,
        };
      },
    };

    const coordinator = new TransactionCoordinator(router, rpc, {
      maxCommitAttempts: 100,
      retryDelayMs: 1,
      circuitBreakerThreshold: 3, // Trip after 3 failures
      circuitBreakerResetMs: 60000, // Long reset time
    });

    const operations = createMultiShardOperations();

    // Start execute and advance timers to allow retry delays to complete
    const executePromise = coordinator.execute(operations);
    await vi.advanceTimersByTimeAsync(1000);
    const result = await executePromise;

    expect(result.committed).toBe(false);
    // Should have stopped relatively quickly due to circuit breaker
    // Note: Multiple shards can be committing concurrently, so total attempts may vary
    // The key is that it doesn't hit the maxCommitAttempts (100)
    expect(commitAttempts).toBeLessThan(20);
  });

  it('should track stuck transactions', async () => {
    let commitAttempts = 0;

    const rpc: ShardRPC = {
      async sendPrepare(shardId, message) {
        return {
          type: 'prepared' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          preparedLSN: 100,
        };
      },
      async sendCommit() {
        commitAttempts++;
        if (commitAttempts < 15) {
          throw new Error('Temporary failure');
        }
        // Succeed after 15 attempts
        return {
          type: 'ack' as const,
          txnId: '',
          shardId: 0,
          timestamp: Date.now(),
          finalLSN: 200,
        };
      },
      async sendAbort(shardId, message) {
        return { type: 'ack' as const, txnId: message.txnId, shardId, timestamp: Date.now() };
      },
      async queryStatus(shardId, message) {
        return {
          type: 'status_response' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          participantState: 'done' as const,
        };
      },
    };

    const coordinator = new TransactionCoordinator(router, rpc, {
      maxCommitAttempts: 100,
      retryDelayMs: 1,
      stuckTransactionThreshold: 5, // Log as stuck after 5 attempts
      circuitBreakerThreshold: 100, // Don't trip circuit breaker
    });

    const operations = createMultiShardOperations();

    // Start execute and advance timers to allow retry delays to complete
    const executePromise = coordinator.execute(operations);
    await vi.advanceTimersByTimeAsync(10000);
    const result = await executePromise;

    // Should eventually succeed
    expect(result.committed).toBe(true);
    // Stuck transactions should be cleared after success
    expect(coordinator.getStuckTransactions().length).toBe(0);
  });

  it('should call metrics on commit retries', async () => {
    const metricsRecorded: string[] = [];
    const mockMetrics: CommitRetryMetrics = {
      incCommitRetry: () => metricsRecorded.push('retry'),
      incCircuitBreakerTrip: () => metricsRecorded.push('circuit_trip'),
      incStuckTransaction: () => metricsRecorded.push('stuck'),
      incManualIntervention: () => metricsRecorded.push('intervention'),
      incMaxAttemptsExceeded: () => metricsRecorded.push('max_exceeded'),
      observeCommitAttemptDuration: () => metricsRecorded.push('duration'),
    };

    let commitAttempts = 0;
    const rpc: ShardRPC = {
      async sendPrepare(shardId, message) {
        return {
          type: 'prepared' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          preparedLSN: 100,
        };
      },
      async sendCommit(shardId, message) {
        commitAttempts++;
        if (commitAttempts < 3) {
          throw new Error('Temporary failure');
        }
        return {
          type: 'ack' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          finalLSN: 200,
        };
      },
      async sendAbort(shardId, message) {
        return { type: 'ack' as const, txnId: message.txnId, shardId, timestamp: Date.now() };
      },
      async queryStatus(shardId, message) {
        return {
          type: 'status_response' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          participantState: 'done' as const,
        };
      },
    };

    const coordinator = new TransactionCoordinator(router, rpc, {
      maxCommitAttempts: 100,
      retryDelayMs: 1,
      circuitBreakerThreshold: 100,
    }, mockMetrics);

    const operations = createMultiShardOperations();

    // Start execute and advance timers to allow retry delays to complete
    const executePromise = coordinator.execute(operations);
    await vi.advanceTimersByTimeAsync(1000);
    await executePromise;

    // Should have recorded retries
    expect(metricsRecorded.filter(m => m === 'retry').length).toBeGreaterThan(0);
    // Should have recorded successful duration
    expect(metricsRecorded.filter(m => m === 'duration').length).toBeGreaterThan(0);
  });

  it('should support manual intervention hook', async () => {
    let commitAttempts = 0;
    let interventionCalled = false;

    const interventionHook: InterventionHook = async (txnId, shardId, attempts) => {
      interventionCalled = true;
      if (attempts >= 5) {
        return {
          txnId,
          shardId,
          action: 'force_commit',
          requestedAt: Date.now(),
          reason: 'Manual override',
        };
      }
      return undefined;
    };

    const rpc: ShardRPC = {
      async sendPrepare(shardId, message) {
        return {
          type: 'prepared' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          preparedLSN: 100,
        };
      },
      async sendCommit() {
        commitAttempts++;
        throw new Error('Always fails');
      },
      async sendAbort(shardId, message) {
        return { type: 'ack' as const, txnId: message.txnId, shardId, timestamp: Date.now() };
      },
      async queryStatus(shardId, message) {
        return {
          type: 'status_response' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          participantState: 'done' as const,
        };
      },
    };

    const coordinator = new TransactionCoordinator(router, rpc, {
      maxCommitAttempts: 100,
      retryDelayMs: 1,
      stuckTransactionThreshold: 3, // Trigger intervention hook after 3 attempts
      circuitBreakerThreshold: 100,
      interventionHook,
    });

    const operations = createMultiShardOperations();

    // Start execute and advance timers to allow retry delays to complete
    const executePromise = coordinator.execute(operations);
    await vi.advanceTimersByTimeAsync(1000);
    const result = await executePromise;

    // Intervention should have been called
    expect(interventionCalled).toBe(true);
    // Force commit should succeed (with synthetic ack)
    expect(result.committed).toBe(true);
  });

  it('should reset circuit breaker after success', async () => {
    let commitAttempts = 0;
    const rpc: ShardRPC = {
      async sendPrepare(shardId, message) {
        return {
          type: 'prepared' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          preparedLSN: 100,
        };
      },
      async sendCommit(shardId, message) {
        commitAttempts++;
        // Fail first 5 times, then succeed
        if (commitAttempts <= 5) {
          throw new Error('Temporary failure');
        }
        return {
          type: 'ack' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          finalLSN: 200,
        };
      },
      async sendAbort(shardId, message) {
        return { type: 'ack' as const, txnId: message.txnId, shardId, timestamp: Date.now() };
      },
      async queryStatus(shardId, message) {
        return {
          type: 'status_response' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          participantState: 'done' as const,
        };
      },
    };

    const coordinator = new TransactionCoordinator(router, rpc, {
      maxCommitAttempts: 100,
      retryDelayMs: 1,
      circuitBreakerThreshold: 100, // Don't trip
    });

    const operations = createMultiShardOperations();

    // Start execute and advance timers to allow retry delays to complete
    const executePromise = coordinator.execute(operations);
    await vi.advanceTimersByTimeAsync(1000);
    const result = await executePromise;

    expect(result.committed).toBe(true);

    // Get shard ID to check circuit breaker
    const shardId = router.routeWithDatabase('db1', 'users').shardId;
    const cbState = coordinator.getCircuitBreakerState(shardId);

    // Circuit breaker should be reset (or not exist)
    expect(cbState === undefined || cbState.failures === 0).toBe(true);
  });

  it('should allow manual circuit breaker reset', async () => {
    const rpc: ShardRPC = {
      async sendPrepare(shardId, message) {
        return {
          type: 'prepared' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          preparedLSN: 100,
        };
      },
      async sendCommit() {
        throw new Error('Always fails');
      },
      async sendAbort(shardId, message) {
        return { type: 'ack' as const, txnId: message.txnId, shardId, timestamp: Date.now() };
      },
      async queryStatus(shardId, message) {
        return {
          type: 'status_response' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          participantState: 'done' as const,
        };
      },
    };

    const coordinator = new TransactionCoordinator(router, rpc, {
      maxCommitAttempts: 10,
      retryDelayMs: 1,
      circuitBreakerThreshold: 3, // Trip quickly
      circuitBreakerResetMs: 60000,
    });

    // Execute to trip circuit breaker
    const operations = createMultiShardOperations();

    // Start execute and advance timers to allow retry delays to complete
    const executePromise = coordinator.execute(operations);
    await vi.advanceTimersByTimeAsync(1000);
    await executePromise;

    // Get shard ID
    const shardId = router.routeWithDatabase('db1', 'users').shardId;

    // Reset circuit breaker manually
    coordinator.resetCircuitBreaker(shardId);

    // Should be cleared
    const cbState = coordinator.getCircuitBreakerState(shardId);
    expect(cbState).toBeUndefined();
  });

  it('should support force complete transaction', async () => {
    let commitStarted = false;

    const rpc: ShardRPC = {
      async sendPrepare(shardId, message) {
        return {
          type: 'prepared' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          preparedLSN: 100,
        };
      },
      async sendCommit() {
        commitStarted = true;
        // Block indefinitely
        await new Promise(() => {});
        throw new Error('Should not reach');
      },
      async sendAbort(shardId, message) {
        return { type: 'ack' as const, txnId: message.txnId, shardId, timestamp: Date.now() };
      },
      async queryStatus(shardId, message) {
        return {
          type: 'status_response' as const,
          txnId: message.txnId,
          shardId,
          timestamp: Date.now(),
          participantState: 'done' as const,
        };
      },
    };

    const coordinator = new TransactionCoordinator(router, rpc, {
      retryDelayMs: 1,
    });

    // Start execute but don't await it
    const operations = createMultiShardOperations();
    const executePromise = coordinator.execute(operations);

    // Wait for commit to start using fake timers
    while (!commitStarted) {
      await vi.advanceTimersByTimeAsync(10);
    }

    // Get active transaction
    const activeTxns = coordinator.getActiveTransactions();
    expect(activeTxns.length).toBeGreaterThan(0);

    const txnId = activeTxns[0].txnId;

    // Force complete it
    await coordinator.forceCompleteTransaction(txnId, 'abort');

    // Transaction should be marked as aborted
    const txn = coordinator.getTransaction(txnId);
    expect(txn?.state).toBe('aborted');
  });
});
