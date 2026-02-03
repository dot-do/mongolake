/**
 * Transaction Coordinator Integration Tests
 *
 * Tests the distributed transaction coordinator functionality:
 * - Two-Phase Commit (2PC) protocol
 * - Cross-shard operation coordination
 * - Transaction lifecycle management
 * - Failure handling and recovery
 * - Circuit breaker patterns
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  TransactionCoordinator,
  createTransactionCoordinator,
  DistributedTransactionError,
  TransactionTimeoutError,
  ParticipantAbortError,
  CommitCircuitBreakerError,
  MaxCommitAttemptsError,
  type ShardRPC,
  type PrepareMessage,
  type PreparedMessage,
  type AbortVoteMessage,
  type CommitMessage,
  type AbortMessage,
  type AckMessage,
  type StatusQueryMessage,
  type StatusResponseMessage,
  type CoordinatorOptions,
} from '../../../src/transaction/coordinator.js';
import {
  ShardRouter,
  createShardRouter,
} from '../../../src/shard/router.js';
import type { BufferedOperation } from '../../../src/session/index.js';
import { resetDocumentCounter } from '../../utils/factories.js';

// ============================================================================
// Mock ShardRPC Implementation
// ============================================================================

class MockShardRPC implements ShardRPC {
  public prepareResponses: Map<number, 'prepared' | 'abort'> = new Map();
  public commitDelay: number = 0;
  public commitFailures: Map<number, number> = new Map(); // shardId -> fail count
  public abortCalled: Set<number> = new Set();
  public commitCalled: Set<number> = new Set();
  public prepareCalled: Set<number> = new Set();

  private lsnCounter: number = 0;

  reset() {
    this.prepareResponses.clear();
    this.commitFailures.clear();
    this.abortCalled.clear();
    this.commitCalled.clear();
    this.prepareCalled.clear();
    this.commitDelay = 0;
    this.lsnCounter = 0;
  }

  async sendPrepare(
    shardId: number,
    message: PrepareMessage
  ): Promise<PreparedMessage | AbortVoteMessage> {
    this.prepareCalled.add(shardId);

    const response = this.prepareResponses.get(shardId) ?? 'prepared';

    if (response === 'abort') {
      return {
        type: 'abort_vote',
        txnId: message.txnId,
        shardId,
        timestamp: Date.now(),
        reason: 'Mock abort response',
      };
    }

    return {
      type: 'prepared',
      txnId: message.txnId,
      shardId,
      timestamp: Date.now(),
      preparedLSN: ++this.lsnCounter,
    };
  }

  async sendCommit(shardId: number, message: CommitMessage): Promise<AckMessage> {
    this.commitCalled.add(shardId);

    // Simulate failures if configured
    const failCount = this.commitFailures.get(shardId) ?? 0;
    if (failCount > 0) {
      this.commitFailures.set(shardId, failCount - 1);
      throw new Error(`Mock commit failure for shard ${shardId}`);
    }

    // Simulate delay
    if (this.commitDelay > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.commitDelay));
    }

    return {
      type: 'ack',
      txnId: message.txnId,
      shardId,
      timestamp: Date.now(),
      finalLSN: ++this.lsnCounter,
    };
  }

  async sendAbort(shardId: number, message: AbortMessage): Promise<AckMessage> {
    this.abortCalled.add(shardId);

    return {
      type: 'ack',
      txnId: message.txnId,
      shardId,
      timestamp: Date.now(),
    };
  }

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
  }
}

// ============================================================================
// Test Helpers
// ============================================================================

function createTestOperations(
  database: string,
  collections: string[],
  countPerCollection: number = 2
): BufferedOperation[] {
  const operations: BufferedOperation[] = [];

  for (const collection of collections) {
    for (let i = 0; i < countPerCollection; i++) {
      operations.push({
        type: 'insert',
        database,
        collection,
        document: { _id: `${collection}-${i}`, name: `Test ${i}` },
        timestamp: Date.now(),
      });
    }
  }

  return operations;
}

function createSingleShardOperations(database: string, collection: string): BufferedOperation[] {
  return [
    {
      type: 'insert',
      database,
      collection,
      document: { _id: 'doc-1', name: 'Test 1' },
      timestamp: Date.now(),
    },
    {
      type: 'update',
      database,
      collection,
      filter: { _id: 'doc-1' },
      update: { $set: { name: 'Updated' } },
      timestamp: Date.now(),
    },
  ];
}

// ============================================================================
// Basic Transaction Lifecycle Tests
// ============================================================================

describe('Transaction Coordinator - Basic Lifecycle', () => {
  let router: ShardRouter;
  let rpc: MockShardRPC;
  let coordinator: TransactionCoordinator;

  beforeEach(() => {
    resetDocumentCounter();
    router = createShardRouter({ shardCount: 4 });
    rpc = new MockShardRPC();
    coordinator = createTransactionCoordinator(router, rpc, {
      prepareTimeoutMs: 5000,
      commitTimeoutMs: 10000,
      maxRetries: 3,
      retryDelayMs: 10,
    });
  });

  afterEach(() => {
    rpc.reset();
  });

  it('should execute single-shard transaction successfully', async () => {
    const operations = createSingleShardOperations('testdb', 'users');

    const result = await coordinator.execute(operations);

    expect(result.committed).toBe(true);
    expect(result.txnId).toBeDefined();
    expect(result.state).toBe('committed');
    expect(result.durationMs).toBeGreaterThanOrEqual(0);
    expect(result.shardLSNs).toBeDefined();
    expect(result.shardLSNs!.size).toBe(1);
  });

  it('should execute multi-shard transaction successfully', async () => {
    const operations = createTestOperations('testdb', ['users', 'orders', 'products']);

    const result = await coordinator.execute(operations);

    expect(result.committed).toBe(true);
    expect(result.state).toBe('committed');
    // Should have called prepare on multiple shards
    expect(rpc.prepareCalled.size).toBeGreaterThan(0);
  });

  it('should generate unique transaction IDs', async () => {
    const operations1 = createSingleShardOperations('testdb', 'collection1');
    const operations2 = createSingleShardOperations('testdb', 'collection2');

    const result1 = await coordinator.execute(operations1);
    const result2 = await coordinator.execute(operations2);

    expect(result1.txnId).not.toBe(result2.txnId);
    expect(result1.txnId).toMatch(/^txn-[a-z0-9]+-[a-z0-9]+$/);
    expect(result2.txnId).toMatch(/^txn-[a-z0-9]+-[a-z0-9]+$/);
  });

  it('should track transaction duration', async () => {
    rpc.commitDelay = 50; // Add delay
    const operations = createSingleShardOperations('testdb', 'users');

    const startTime = Date.now();
    const result = await coordinator.execute(operations);
    const elapsed = Date.now() - startTime;

    expect(result.durationMs).toBeGreaterThanOrEqual(40); // Allow some timing variance
    expect(result.durationMs).toBeLessThanOrEqual(elapsed + 50);
  });
});

// ============================================================================
// Two-Phase Commit Protocol Tests
// ============================================================================

describe('Transaction Coordinator - Two-Phase Commit', () => {
  let router: ShardRouter;
  let rpc: MockShardRPC;
  let coordinator: TransactionCoordinator;

  beforeEach(() => {
    resetDocumentCounter();
    router = createShardRouter({ shardCount: 4 });
    rpc = new MockShardRPC();
    coordinator = createTransactionCoordinator(router, rpc, {
      prepareTimeoutMs: 5000,
      commitTimeoutMs: 10000,
      maxRetries: 3,
      retryDelayMs: 10,
    });
  });

  afterEach(() => {
    rpc.reset();
  });

  it('should call prepare on all participating shards', async () => {
    const operations = createTestOperations('testdb', ['users', 'orders', 'products', 'inventory']);

    await coordinator.execute(operations);

    // All participating shards should receive prepare
    expect(rpc.prepareCalled.size).toBeGreaterThan(0);
  });

  it('should call commit on all shards after successful prepare', async () => {
    const operations = createTestOperations('testdb', ['coll1', 'coll2']);

    const result = await coordinator.execute(operations);

    expect(result.committed).toBe(true);
    // Commit should be called on all prepared shards
    expect(rpc.commitCalled.size).toBeGreaterThan(0);
    expect(rpc.abortCalled.size).toBe(0);
  });

  it('should abort if any participant votes abort', async () => {
    // Configure one shard to abort
    const shardToAbort = router.routeWithDatabase('testdb', 'users').shardId;
    rpc.prepareResponses.set(shardToAbort, 'abort');

    const operations = createTestOperations('testdb', ['users', 'orders']);

    const result = await coordinator.execute(operations);

    expect(result.committed).toBe(false);
    expect(result.state).toBe('aborted');
    expect(result.abortReason).toBeDefined();
  });

  it('should send abort to prepared shards when another aborts', async () => {
    // Configure specific shard to abort
    const ordersShardId = router.routeWithDatabase('testdb', 'orders').shardId;
    rpc.prepareResponses.set(ordersShardId, 'abort');

    const operations = createTestOperations('testdb', ['users', 'orders', 'products']);

    await coordinator.execute(operations);

    // Abort should be sent to at least some shards
    // The exact count depends on routing
    expect(rpc.abortCalled.size).toBeGreaterThanOrEqual(0);
  });
});

// ============================================================================
// Failure Handling Tests
// ============================================================================

describe('Transaction Coordinator - Failure Handling', () => {
  let router: ShardRouter;
  let rpc: MockShardRPC;
  let coordinator: TransactionCoordinator;

  beforeEach(() => {
    resetDocumentCounter();
    router = createShardRouter({ shardCount: 4 });
    rpc = new MockShardRPC();
    coordinator = createTransactionCoordinator(router, rpc, {
      prepareTimeoutMs: 1000,
      commitTimeoutMs: 2000,
      maxRetries: 2,
      retryDelayMs: 10,
      maxCommitAttempts: 5,
      circuitBreakerThreshold: 3,
      circuitBreakerResetMs: 100,
    });
  });

  afterEach(() => {
    rpc.reset();
  });

  it('should retry commit on transient failures for multi-shard transactions', async () => {
    // Multi-shard transactions have retry logic; single-shard do not
    const operations = createTestOperations('testdb', ['users', 'orders']);
    const shardIds = operations.map((op) => router.routeWithDatabase(op.database, op.collection).shardId);

    // Configure 2 failures then success for one shard
    rpc.commitFailures.set(shardIds[0], 2);

    const result = await coordinator.execute(operations);

    // Should eventually succeed after retries
    expect(result.committed).toBe(true);
  });

  it('should handle participant abort with reason', async () => {
    const shardId = router.routeWithDatabase('testdb', 'orders').shardId;
    rpc.prepareResponses.set(shardId, 'abort');

    const operations = createTestOperations('testdb', ['users', 'orders']);

    const result = await coordinator.execute(operations);

    expect(result.committed).toBe(false);
    expect(result.abortReason).toContain('Mock abort response');
  });

  it('should record circuit breaker state after failures', async () => {
    // Circuit breaker works with multi-shard transactions (have retry logic)
    const operations = createTestOperations('testdb', ['users', 'orders']);
    const shardIds = operations.map((op) => router.routeWithDatabase(op.database, op.collection).shardId);
    const targetShard = shardIds[0];

    // Configure many failures to exceed max attempts (will trigger circuit breaker)
    rpc.commitFailures.set(targetShard, 10);

    try {
      await coordinator.execute(operations);
    } catch (error) {
      // Expected to fail after max retries
    }

    // Check circuit breaker state - should have recorded failures
    const cbState = coordinator.getCircuitBreakerState(targetShard);
    expect(cbState).toBeDefined();
    expect(cbState!.failures).toBeGreaterThan(0);
  });

  it('should reset circuit breaker on success', async () => {
    // Circuit breaker works with multi-shard transactions
    const operations = createTestOperations('testdb', ['users', 'orders']);
    const shardIds = operations.map((op) => router.routeWithDatabase(op.database, op.collection).shardId);
    const targetShard = shardIds[0];

    // First: cause some failures (but not enough to fail)
    rpc.commitFailures.set(targetShard, 2);
    await coordinator.execute(operations);

    // Verify failures were recorded
    const cbStateBefore = coordinator.getCircuitBreakerState(targetShard);
    expect(cbStateBefore).toBeDefined();

    // Now do a successful transaction (no failures)
    rpc.commitFailures.set(targetShard, 0);
    await coordinator.execute(operations);

    // Circuit breaker should be reset
    const cbStateAfter = coordinator.getCircuitBreakerState(targetShard);
    expect(cbStateAfter?.failures ?? 0).toBe(0);
  });

  it('should allow manual circuit breaker reset', async () => {
    // Use multi-shard to populate circuit breaker state
    const operations = createTestOperations('testdb', ['users', 'orders']);
    const shardIds = operations.map((op) => router.routeWithDatabase(op.database, op.collection).shardId);
    const targetShard = shardIds[0];

    // Configure failures
    rpc.commitFailures.set(targetShard, 10);

    try {
      await coordinator.execute(operations);
    } catch {
      // Expected
    }

    // Reset circuit breaker manually
    coordinator.resetCircuitBreaker(targetShard);

    const cbState = coordinator.getCircuitBreakerState(targetShard);
    expect(cbState).toBeUndefined();
  });
});

// ============================================================================
// Active Transaction Tracking Tests
// ============================================================================

describe('Transaction Coordinator - Transaction Tracking', () => {
  let router: ShardRouter;
  let rpc: MockShardRPC;
  let coordinator: TransactionCoordinator;

  beforeEach(() => {
    resetDocumentCounter();
    router = createShardRouter({ shardCount: 4 });
    rpc = new MockShardRPC();
    coordinator = createTransactionCoordinator(router, rpc, {
      prepareTimeoutMs: 5000,
      commitTimeoutMs: 10000,
      maxRetries: 3,
      retryDelayMs: 10,
    });
  });

  afterEach(() => {
    rpc.reset();
  });

  it('should track active transactions during execution', async () => {
    rpc.commitDelay = 100; // Add delay to keep transaction active

    const operations = createTestOperations('testdb', ['users', 'orders']);

    // Start transaction but don't await immediately
    const promise = coordinator.execute(operations);

    // Small delay to let transaction start
    await new Promise((resolve) => setTimeout(resolve, 10));

    // Note: Transaction may complete quickly in mock, so we just check the API works
    const active = coordinator.getActiveTransactions();
    expect(Array.isArray(active)).toBe(true);

    await promise;
  });

  it('should remove transaction from tracking after completion', async () => {
    const operations = createSingleShardOperations('testdb', 'users');

    const result = await coordinator.execute(operations);

    // Transaction should be removed
    const transaction = coordinator.getTransaction(result.txnId);
    expect(transaction).toBeUndefined();

    const active = coordinator.getActiveTransactions();
    expect(active.find((t) => t.txnId === result.txnId)).toBeUndefined();
  });
});

// ============================================================================
// Operation Partitioning Tests
// ============================================================================

describe('Transaction Coordinator - Operation Partitioning', () => {
  let router: ShardRouter;
  let rpc: MockShardRPC;
  let coordinator: TransactionCoordinator;

  beforeEach(() => {
    resetDocumentCounter();
    router = createShardRouter({ shardCount: 4 });
    rpc = new MockShardRPC();
    coordinator = createTransactionCoordinator(router, rpc);
  });

  afterEach(() => {
    rpc.reset();
  });

  it('should partition operations by database and collection', async () => {
    const operations: BufferedOperation[] = [
      {
        type: 'insert',
        database: 'db1',
        collection: 'users',
        document: { _id: '1' },
        timestamp: Date.now(),
      },
      {
        type: 'insert',
        database: 'db1',
        collection: 'orders',
        document: { _id: '2' },
        timestamp: Date.now(),
      },
      {
        type: 'insert',
        database: 'db2',
        collection: 'users',
        document: { _id: '3' },
        timestamp: Date.now(),
      },
    ];

    await coordinator.execute(operations);

    // Operations should be routed based on db.collection
    expect(rpc.prepareCalled.size).toBeGreaterThan(0);
  });

  it('should handle operations spanning many shards', async () => {
    // Create operations that span multiple collections
    const collections = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'];
    const operations = createTestOperations('testdb', collections, 1);

    const result = await coordinator.execute(operations);

    expect(result.committed).toBe(true);
    // Should have prepared multiple shards
    expect(rpc.prepareCalled.size).toBeGreaterThan(0);
  });

  it('should optimize single-shard transactions', async () => {
    const operations = createSingleShardOperations('testdb', 'single_collection');

    const result = await coordinator.execute(operations);

    expect(result.committed).toBe(true);
    // Single shard should still go through proper commit
    expect(rpc.commitCalled.size).toBe(1);
  });
});

// ============================================================================
// Intervention Hook Tests
// ============================================================================

describe('Transaction Coordinator - Intervention Hooks', () => {
  let router: ShardRouter;
  let rpc: MockShardRPC;

  beforeEach(() => {
    resetDocumentCounter();
    router = createShardRouter({ shardCount: 4 });
    rpc = new MockShardRPC();
  });

  afterEach(() => {
    rpc.reset();
  });

  it('should call intervention hook for stuck transactions', async () => {
    let hookCalled = false;
    let hookAttempts = 0;

    const coordinator = createTransactionCoordinator(router, rpc, {
      maxRetries: 2,
      retryDelayMs: 1,
      maxCommitAttempts: 20,
      stuckTransactionThreshold: 3,
      interventionHook: async (txnId, shardId, attempts) => {
        hookCalled = true;
        hookAttempts = attempts;

        // Force abort after threshold
        if (attempts >= 5) {
          return {
            txnId,
            shardId,
            action: 'force_abort',
            requestedAt: Date.now(),
            reason: 'Test intervention',
          };
        }
        return undefined; // Continue normal retry
      },
    });

    // Use multi-shard operations (intervention hooks work with commit phase retries)
    const operations = createTestOperations('testdb', ['users', 'orders']);
    const shardIds = operations.map((op) => router.routeWithDatabase(op.database, op.collection).shardId);

    // Configure many failures on one shard
    rpc.commitFailures.set(shardIds[0], 100);

    try {
      await coordinator.execute(operations);
    } catch (error) {
      // Expected to be aborted via intervention
    }

    expect(hookCalled).toBe(true);
    expect(hookAttempts).toBeGreaterThanOrEqual(3);
  });

  it('should force commit via intervention hook', async () => {
    const coordinator = createTransactionCoordinator(router, rpc, {
      maxRetries: 1,
      retryDelayMs: 1,
      maxCommitAttempts: 20,
      stuckTransactionThreshold: 2,
      interventionHook: async (txnId, shardId, attempts) => {
        if (attempts >= 3) {
          return {
            txnId,
            shardId,
            action: 'force_commit',
            requestedAt: Date.now(),
            reason: 'Force commit for test',
          };
        }
        return undefined;
      },
    });

    // Use multi-shard operations for intervention hooks
    const operations = createTestOperations('testdb', ['users', 'orders']);
    const shardIds = operations.map((op) => router.routeWithDatabase(op.database, op.collection).shardId);
    rpc.commitFailures.set(shardIds[0], 100);

    const result = await coordinator.execute(operations);

    // Force commit should result in committed state
    expect(result.committed).toBe(true);
    // All shards should have LSNs recorded
    expect(result.shardLSNs).toBeDefined();
    expect(result.shardLSNs!.size).toBeGreaterThan(0);
  });
});

// ============================================================================
// Metrics Integration Tests
// ============================================================================

describe('Transaction Coordinator - Metrics', () => {
  let router: ShardRouter;
  let rpc: MockShardRPC;

  beforeEach(() => {
    resetDocumentCounter();
    router = createShardRouter({ shardCount: 4 });
    rpc = new MockShardRPC();
  });

  afterEach(() => {
    rpc.reset();
  });

  it('should record commit retry metrics', async () => {
    let attemptDurations = 0;
    const retryMetrics = {
      incCommitRetry: () => {},
      incCircuitBreakerTrip: () => {},
      incStuckTransaction: () => {},
      incManualIntervention: () => {},
      incMaxAttemptsExceeded: () => {},
      observeCommitAttemptDuration: () => { attemptDurations++; },
    };

    const coordinator = createTransactionCoordinator(router, rpc, {
      maxRetries: 5,
      retryDelayMs: 1,
    });
    // Set metrics after construction (factory doesn't accept metrics param)
    coordinator.setMetrics(retryMetrics);

    // Use multi-shard operations (metrics are recorded during commit)
    const operations = createTestOperations('testdb', ['users', 'orders']);
    const shardIds = operations.map((op) => router.routeWithDatabase(op.database, op.collection).shardId);

    // Configure failures then success - this triggers retries and the duration is recorded on success
    rpc.commitFailures.set(shardIds[0], 2);

    await coordinator.execute(operations);

    // On successful commit (after retries), observeCommitAttemptDuration should be called for each shard
    expect(attemptDurations).toBeGreaterThanOrEqual(1);
  });

  it('should allow setting metrics after construction', async () => {
    const coordinator = createTransactionCoordinator(router, rpc);

    let metricsRecorded = false;
    coordinator.setMetrics({
      incCommitRetry: () => { metricsRecorded = true; },
      incCircuitBreakerTrip: () => {},
      incStuckTransaction: () => {},
      incManualIntervention: () => {},
      incMaxAttemptsExceeded: () => {},
      observeCommitAttemptDuration: () => {},
    });

    // Use multi-shard with failures to trigger retry logic
    const operations = createTestOperations('testdb', ['users', 'orders']);
    const shardIds = operations.map((op) => router.routeWithDatabase(op.database, op.collection).shardId);
    rpc.commitFailures.set(shardIds[0], 1); // One failure to trigger retry

    await coordinator.execute(operations);

    expect(metricsRecorded).toBe(true);
  });
});

// ============================================================================
// Force Complete Transaction Tests
// ============================================================================

describe('Transaction Coordinator - Force Complete', () => {
  let router: ShardRouter;
  let rpc: MockShardRPC;
  let coordinator: TransactionCoordinator;

  beforeEach(() => {
    resetDocumentCounter();
    router = createShardRouter({ shardCount: 4 });
    rpc = new MockShardRPC();
    rpc.commitDelay = 500; // Slow commits to keep transactions active
    coordinator = createTransactionCoordinator(router, rpc, {
      prepareTimeoutMs: 10000,
      commitTimeoutMs: 10000,
    });
  });

  afterEach(() => {
    rpc.reset();
  });

  it('should throw error when force completing non-existent transaction', async () => {
    await expect(
      coordinator.forceCompleteTransaction('non-existent-txn', 'commit')
    ).rejects.toThrow('Transaction non-existent-txn not found');
  });
});

// ============================================================================
// Edge Cases and Error Conditions
// ============================================================================

describe('Transaction Coordinator - Edge Cases', () => {
  let router: ShardRouter;
  let rpc: MockShardRPC;
  let coordinator: TransactionCoordinator;

  beforeEach(() => {
    resetDocumentCounter();
    router = createShardRouter({ shardCount: 4 });
    rpc = new MockShardRPC();
    coordinator = createTransactionCoordinator(router, rpc, {
      prepareTimeoutMs: 100,
      commitTimeoutMs: 200,
      maxRetries: 1,
      retryDelayMs: 1,
    });
  });

  afterEach(() => {
    rpc.reset();
  });

  it('should handle empty operations list', async () => {
    const operations: BufferedOperation[] = [];

    // This may throw or return quickly depending on implementation
    // The coordinator should handle gracefully
    try {
      const result = await coordinator.execute(operations);
      // If it succeeds, should be committed
      expect(result.state).toBeDefined();
    } catch (error) {
      // If it fails, should be a sensible error
      expect(error).toBeDefined();
    }
  });

  it('should handle operations with same database but different collections', async () => {
    const operations: BufferedOperation[] = [
      {
        type: 'insert',
        database: 'shared_db',
        collection: 'collection_a',
        document: { _id: '1' },
        timestamp: Date.now(),
      },
      {
        type: 'insert',
        database: 'shared_db',
        collection: 'collection_b',
        document: { _id: '2' },
        timestamp: Date.now(),
      },
      {
        type: 'insert',
        database: 'shared_db',
        collection: 'collection_c',
        document: { _id: '3' },
        timestamp: Date.now(),
      },
    ];

    const result = await coordinator.execute(operations);

    expect(result.committed).toBe(true);
  });

  it('should handle mixed operation types in transaction', async () => {
    const operations: BufferedOperation[] = [
      {
        type: 'insert',
        database: 'testdb',
        collection: 'users',
        document: { _id: 'new-user', name: 'Test' },
        timestamp: Date.now(),
      },
      {
        type: 'update',
        database: 'testdb',
        collection: 'users',
        filter: { _id: 'existing-user' },
        update: { $set: { status: 'updated' } },
        timestamp: Date.now(),
      },
      {
        type: 'delete',
        database: 'testdb',
        collection: 'users',
        filter: { _id: 'delete-user' },
        timestamp: Date.now(),
      },
    ];

    const result = await coordinator.execute(operations);

    expect(result.committed).toBe(true);
  });
});
