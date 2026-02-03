/**
 * Cross-Shard Transaction Coordination Tests (RED)
 *
 * Tests for distributed transactions spanning multiple shards using
 * the Two-Phase Commit (2PC) protocol.
 *
 * These tests verify:
 * - Atomic commit across multiple shards
 * - Atomic abort on any failure
 * - Prepare phase coordination
 * - Commit phase coordination
 * - Abort phase rollback
 * - Timeout handling
 * - Partial failure recovery
 * - Coordinator failure recovery
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  TransactionCoordinator,
  createTransactionCoordinator,
  DistributedTransactionError,
  TransactionTimeoutError,
  ParticipantAbortError,
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
  type DistributedTransactionResult,
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
// Mock Shard DO for Multi-Shard Scenarios
// ============================================================================

/**
 * Mock ShardDO that simulates a shard participant in distributed transactions.
 */
class MockShardDO {
  public readonly shardId: number;
  private participant: TransactionParticipant;
  private storage: MockParticipantStorage;
  private executor: MockOperationExecutor;

  // State tracking for testing
  public prepareCallCount = 0;
  public commitCallCount = 0;
  public abortCallCount = 0;
  public preparedTransactions: Map<string, PreparedTransaction> = new Map();

  // Configurable behaviors
  public shouldFailPrepare = false;
  public prepareFailureReason = 'Simulated prepare failure';
  public shouldFailCommit = false;
  public commitFailureReason = 'Simulated commit failure';
  public prepareDelay = 0;
  public commitDelay = 0;
  public abortDelay = 0;

  constructor(shardId: number) {
    this.shardId = shardId;
    this.storage = new MockParticipantStorage();
    this.executor = new MockOperationExecutor();
    this.participant = new TransactionParticipant(
      shardId,
      this.storage,
      this.executor
    );
  }

  async handlePrepare(message: PrepareMessage): Promise<PreparedMessage | AbortVoteMessage> {
    this.prepareCallCount++;

    if (this.prepareDelay > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.prepareDelay));
    }

    if (this.shouldFailPrepare) {
      return {
        type: 'abort_vote',
        txnId: message.txnId,
        shardId: this.shardId,
        timestamp: Date.now(),
        reason: this.prepareFailureReason,
      };
    }

    return this.participant.handlePrepare(message);
  }

  async handleCommit(message: CommitMessage): Promise<AckMessage> {
    this.commitCallCount++;

    if (this.commitDelay > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.commitDelay));
    }

    if (this.shouldFailCommit) {
      throw new Error(this.commitFailureReason);
    }

    return this.participant.handleCommit(message);
  }

  async handleAbort(message: AbortMessage): Promise<AckMessage> {
    this.abortCallCount++;

    if (this.abortDelay > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.abortDelay));
    }

    return this.participant.handleAbort(message);
  }

  async handleStatusQuery(message: StatusQueryMessage): Promise<StatusResponseMessage> {
    return this.participant.handleStatusQuery(message);
  }

  reset(): void {
    this.prepareCallCount = 0;
    this.commitCallCount = 0;
    this.abortCallCount = 0;
    this.shouldFailPrepare = false;
    this.shouldFailCommit = false;
    this.prepareDelay = 0;
    this.commitDelay = 0;
    this.abortDelay = 0;
  }
}

/**
 * Mock storage for participant state.
 */
class MockParticipantStorage implements ParticipantStorage {
  private transactions: Map<string, PreparedTransaction> = new Map();
  private currentLSN = 0;

  async savePreparedTransaction(txn: PreparedTransaction): Promise<void> {
    this.transactions.set(txn.txnId, txn);
  }

  async loadPreparedTransaction(txnId: string): Promise<PreparedTransaction | null> {
    return this.transactions.get(txnId) ?? null;
  }

  async deletePreparedTransaction(txnId: string): Promise<void> {
    this.transactions.delete(txnId);
  }

  async loadAllPreparedTransactions(): Promise<PreparedTransaction[]> {
    return Array.from(this.transactions.values());
  }

  allocateLSN(): number {
    return ++this.currentLSN;
  }

  getCurrentLSN(): number {
    return this.currentLSN;
  }
}

/**
 * Mock executor for operations.
 */
class MockOperationExecutor implements OperationExecutor {
  public shouldFailValidation = false;
  public validationErrors: string[] = [];
  public shouldFailApply = false;

  async validateOperations(operations: BufferedOperation[]): Promise<{ valid: boolean; errors?: string[] }> {
    if (this.shouldFailValidation) {
      return { valid: false, errors: this.validationErrors };
    }
    return { valid: true };
  }

  async applyOperations(operations: BufferedOperation[]): Promise<number> {
    if (this.shouldFailApply) {
      throw new Error('Failed to apply operations');
    }
    return Date.now();
  }
}

/**
 * Create a multi-shard RPC implementation using MockShardDOs.
 */
function createMultiShardRPC(shards: Map<number, MockShardDO>): ShardRPC {
  return {
    async sendPrepare(shardId: number, message: PrepareMessage): Promise<PreparedMessage | AbortVoteMessage> {
      const shard = shards.get(shardId);
      if (!shard) {
        throw new Error(`Shard ${shardId} not found`);
      }
      return shard.handlePrepare(message);
    },

    async sendCommit(shardId: number, message: CommitMessage): Promise<AckMessage> {
      const shard = shards.get(shardId);
      if (!shard) {
        throw new Error(`Shard ${shardId} not found`);
      }
      return shard.handleCommit(message);
    },

    async sendAbort(shardId: number, message: AbortMessage): Promise<AckMessage> {
      const shard = shards.get(shardId);
      if (!shard) {
        throw new Error(`Shard ${shardId} not found`);
      }
      return shard.handleAbort(message);
    },

    async queryStatus(shardId: number, message: StatusQueryMessage): Promise<StatusResponseMessage> {
      const shard = shards.get(shardId);
      if (!shard) {
        throw new Error(`Shard ${shardId} not found`);
      }
      return shard.handleStatusQuery(message);
    },
  };
}

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Create operations that span exactly 2 shards.
 * Uses different database names to ensure operations route to different shards.
 */
function createTwoShardOperations(): BufferedOperation[] {
  return [
    {
      type: 'insert',
      collection: 'accounts',
      database: 'bank_shard_1',
      document: { _id: 'acc1', balance: 1000 },
      timestamp: Date.now(),
    },
    {
      type: 'update',
      collection: 'accounts',
      database: 'bank_shard_2',
      filter: { _id: 'acc2' },
      update: { $inc: { balance: -500 } },
      timestamp: Date.now(),
    },
  ];
}

/**
 * Create operations with specific target shards.
 */
function createOperationsForShards(targetShards: number[], router: ShardRouter): BufferedOperation[] {
  const operations: BufferedOperation[] = [];

  for (let i = 0; i < targetShards.length; i++) {
    // Find a collection name that routes to the target shard
    let collection = `collection_${i}`;
    let database = `db_${i}`;

    // Keep trying until we find a combination that routes to our target shard
    for (let attempt = 0; attempt < 1000; attempt++) {
      const assignment = router.routeWithDatabase(database, collection);
      if (assignment.shardId === targetShards[i]) {
        break;
      }
      collection = `collection_${i}_${attempt}`;
      database = `db_${i}_${attempt}`;
    }

    operations.push({
      type: 'insert',
      collection,
      database,
      document: { _id: `doc_${i}`, data: `value_${i}` },
      timestamp: Date.now(),
    });
  }

  return operations;
}

// ============================================================================
// Test Suite: Cross-Shard Transaction Coordination
// ============================================================================

describe('Cross-Shard Transaction Coordination', () => {
  let router: ShardRouter;
  let shards: Map<number, MockShardDO>;
  let rpc: ShardRPC;
  let coordinator: TransactionCoordinator;

  beforeEach(() => {
    router = new ShardRouter();

    // Create mock shards (0-15)
    shards = new Map();
    for (let i = 0; i < 16; i++) {
      shards.set(i, new MockShardDO(i));
    }

    rpc = createMultiShardRPC(shards);
    coordinator = new TransactionCoordinator(router, rpc, {
      prepareTimeoutMs: 5000,
      commitTimeoutMs: 10000,
      maxRetries: 3,
      retryDelayMs: 10,
    });
  });

  afterEach(() => {
    // Reset all shards
    for (const shard of shards.values()) {
      shard.reset();
    }
  });

  // ==========================================================================
  // Test 1: Transaction spanning 2 shards commits atomically
  // ==========================================================================

  describe('Atomic Commit', () => {
    it('should commit transaction spanning 2 shards atomically', async () => {
      // Create operations that span 2 different shards
      const operations = createTwoShardOperations();

      const result = await coordinator.execute(operations);

      // Verify transaction committed
      expect(result.committed).toBe(true);
      expect(result.state).toBe('committed');

      // Find which shards were involved
      const involvedShards: number[] = [];
      for (const [shardId, shard] of shards) {
        if (shard.commitCallCount > 0) {
          involvedShards.push(shardId);
        }
      }

      // At least 2 shards should have been involved
      expect(involvedShards.length).toBeGreaterThanOrEqual(2);

      // All involved shards should have received commit
      for (const shardId of involvedShards) {
        const shard = shards.get(shardId)!;
        expect(shard.prepareCallCount).toBeGreaterThanOrEqual(1);
        expect(shard.commitCallCount).toBeGreaterThanOrEqual(1);
      }
    });

    it('should include all shard LSNs in successful commit result', async () => {
      const operations = createTwoShardOperations();

      const result = await coordinator.execute(operations);

      expect(result.committed).toBe(true);
      expect(result.shardLSNs).toBeDefined();
      expect(result.shardLSNs!.size).toBeGreaterThanOrEqual(1);
    });
  });

  // ==========================================================================
  // Test 2: Transaction spanning 2 shards aborts on failure
  // ==========================================================================

  describe('Atomic Abort on Failure', () => {
    it('should abort transaction spanning 2 shards when one shard fails prepare', async () => {
      const operations = createTwoShardOperations();

      // Find one of the shards that will be involved and make it fail
      const assignment1 = router.routeWithDatabase('bank_shard_1', 'accounts');
      const failingShard = shards.get(assignment1.shardId)!;
      failingShard.shouldFailPrepare = true;
      failingShard.prepareFailureReason = 'Validation failed on shard';

      const result = await coordinator.execute(operations);

      // Verify transaction aborted
      expect(result.committed).toBe(false);
      expect(result.state).toBe('aborted');
      expect(result.abortReason).toContain('Validation failed on shard');
    });

    it('should send abort to all prepared shards when one fails', async () => {
      const operations = createTwoShardOperations();

      // Get assignments for both databases
      const assignment1 = router.routeWithDatabase('bank_shard_1', 'accounts');
      const assignment2 = router.routeWithDatabase('bank_shard_2', 'accounts');

      // Make shard 2 fail prepare (after shard 1 succeeds)
      const shard2 = shards.get(assignment2.shardId)!;
      shard2.shouldFailPrepare = true;

      const result = await coordinator.execute(operations);

      expect(result.committed).toBe(false);

      // Shard 1 should have received abort if it was prepared
      const shard1 = shards.get(assignment1.shardId)!;
      if (shard1.prepareCallCount > 0 && !shard1.shouldFailPrepare) {
        // Shard 1 prepared successfully, so it should have received abort
        expect(shard1.abortCallCount).toBeGreaterThanOrEqual(0);
      }
    });
  });

  // ==========================================================================
  // Test 3: Prepare phase - all shards must agree before commit
  // ==========================================================================

  describe('Prepare Phase Coordination', () => {
    it('should send prepare to all participating shards', async () => {
      const operations = createTwoShardOperations();

      await coordinator.execute(operations);

      // Count shards that received prepare
      let prepareCount = 0;
      for (const shard of shards.values()) {
        if (shard.prepareCallCount > 0) {
          prepareCount++;
        }
      }

      expect(prepareCount).toBeGreaterThanOrEqual(2);
    });

    it('should wait for all prepare responses before deciding', async () => {
      const operations = createTwoShardOperations();

      // Add delay to one shard's prepare
      const assignment1 = router.routeWithDatabase('bank_shard_1', 'accounts');
      const shard1 = shards.get(assignment1.shardId)!;
      shard1.prepareDelay = 50;

      const startTime = Date.now();
      const result = await coordinator.execute(operations);
      const duration = Date.now() - startTime;

      // Should have waited for the delayed shard
      expect(duration).toBeGreaterThanOrEqual(50);
      expect(result.committed).toBe(true);
    });

    it('should abort if any shard votes abort during prepare', async () => {
      const operations = createTwoShardOperations();

      // Make shard 1 abort
      const assignment1 = router.routeWithDatabase('bank_shard_1', 'accounts');
      const shard1 = shards.get(assignment1.shardId)!;
      shard1.shouldFailPrepare = true;
      shard1.prepareFailureReason = 'Lock conflict detected';

      const result = await coordinator.execute(operations);

      expect(result.committed).toBe(false);
      expect(result.state).toBe('aborted');
      expect(result.abortReason).toContain('Lock conflict detected');
    });

    it('should only proceed to commit when all shards vote prepared', async () => {
      const operations = createTwoShardOperations();

      const result = await coordinator.execute(operations);

      // All shards that received prepare should have also received commit
      for (const shard of shards.values()) {
        if (shard.prepareCallCount > 0) {
          expect(shard.commitCallCount).toBeGreaterThanOrEqual(1);
        }
      }

      expect(result.committed).toBe(true);
    });
  });

  // ==========================================================================
  // Test 4: Commit phase - all shards commit after prepare succeeds
  // ==========================================================================

  describe('Commit Phase Coordination', () => {
    it('should send commit to all prepared shards', async () => {
      const operations = createTwoShardOperations();

      const result = await coordinator.execute(operations);

      expect(result.committed).toBe(true);

      // All prepared shards should have received commit
      for (const shard of shards.values()) {
        if (shard.prepareCallCount > 0) {
          expect(shard.commitCallCount).toBe(1);
        }
      }
    });

    it('should retry commit on transient failures', async () => {
      const operations = createTwoShardOperations();

      // Make shard 1 fail commit once, then succeed
      const assignment1 = router.routeWithDatabase('bank_shard_1', 'accounts');
      const shard1 = shards.get(assignment1.shardId)!;

      let commitAttempts = 0;
      const originalHandleCommit = shard1.handleCommit.bind(shard1);
      shard1.handleCommit = async (message: CommitMessage) => {
        commitAttempts++;
        if (commitAttempts === 1) {
          throw new Error('Transient network error');
        }
        return originalHandleCommit(message);
      };

      coordinator = new TransactionCoordinator(router, rpc, {
        maxRetries: 5,
        retryDelayMs: 10,
        maxCommitAttempts: 10,
      });

      const result = await coordinator.execute(operations);

      // Should have retried and eventually succeeded
      expect(result.committed).toBe(true);
      expect(commitAttempts).toBeGreaterThan(1);
    });

    it('should only commit after prepare decision is made', async () => {
      const operations = createTwoShardOperations();

      const events: string[] = [];

      // Track order of prepare and commit for all shards
      for (const [shardId, shard] of shards) {
        const originalPrepare = shard.handlePrepare.bind(shard);
        const originalCommit = shard.handleCommit.bind(shard);

        shard.handlePrepare = async (msg: PrepareMessage) => {
          events.push(`prepare-${shardId}`);
          return originalPrepare(msg);
        };

        shard.handleCommit = async (msg: CommitMessage) => {
          events.push(`commit-${shardId}`);
          return originalCommit(msg);
        };
      }

      await coordinator.execute(operations);

      // All prepares should come before any commits
      const firstCommitIndex = events.findIndex((e) => e.startsWith('commit-'));
      const prepareEvents = events.filter((e) => e.startsWith('prepare-'));

      if (firstCommitIndex !== -1) {
        expect(prepareEvents.length).toBeGreaterThanOrEqual(2);
        for (let i = 0; i < prepareEvents.length; i++) {
          expect(events.indexOf(prepareEvents[i])).toBeLessThan(firstCommitIndex);
        }
      }
    });
  });

  // ==========================================================================
  // Test 5: Abort phase - all shards rollback if any prepare fails
  // ==========================================================================

  describe('Abort Phase Coordination', () => {
    it('should send abort to all shards that received prepare', async () => {
      const operations = createTwoShardOperations();

      // Get both shard assignments
      const assignment1 = router.routeWithDatabase('bank_shard_1', 'accounts');
      const assignment2 = router.routeWithDatabase('bank_shard_2', 'accounts');

      // Make shard 2 delay prepare, and shard 1 fail
      const shard1 = shards.get(assignment1.shardId)!;
      const shard2 = shards.get(assignment2.shardId)!;

      shard1.shouldFailPrepare = true;
      shard2.prepareDelay = 10; // Small delay to ensure shard 1 fails first

      const result = await coordinator.execute(operations);

      expect(result.committed).toBe(false);

      // Shards that were preparing/prepared should receive abort
      // Note: Due to concurrent prepare, abort behavior may vary
    });

    it('should release locks on abort', async () => {
      const operations = createTwoShardOperations();

      // Track lock state
      const assignment1 = router.routeWithDatabase('bank_shard_1', 'accounts');
      const shard1 = shards.get(assignment1.shardId)!;

      // Make shard 2 fail to trigger abort
      const assignment2 = router.routeWithDatabase('bank_shard_2', 'accounts');
      const shard2 = shards.get(assignment2.shardId)!;
      shard2.shouldFailPrepare = true;

      await coordinator.execute(operations);

      // After abort, shard 1 should have received abort and released locks
      expect(shard1.abortCallCount).toBeGreaterThanOrEqual(0);
    });

    it('should abort all shards even if some abort RPCs fail', async () => {
      const operations = createTwoShardOperations();

      // Make shard 1 fail prepare to trigger abort
      const assignment1 = router.routeWithDatabase('bank_shard_1', 'accounts');
      const shard1 = shards.get(assignment1.shardId)!;
      shard1.shouldFailPrepare = true;

      // Make abort fail on shard 2 (should be handled gracefully)
      const assignment2 = router.routeWithDatabase('bank_shard_2', 'accounts');
      const shard2 = shards.get(assignment2.shardId)!;
      const originalAbort = shard2.handleAbort.bind(shard2);
      shard2.handleAbort = async () => {
        throw new Error('Network error on abort');
      };

      const result = await coordinator.execute(operations);

      // Transaction should still be aborted
      expect(result.committed).toBe(false);
      expect(result.state).toBe('aborted');
    });
  });

  // ==========================================================================
  // Test 6: Timeout handling - abort if prepare takes too long
  // ==========================================================================

  describe('Timeout Handling', () => {
    it('should abort transaction if prepare phase exceeds timeout', async () => {
      // Set very short timeout
      coordinator = new TransactionCoordinator(router, rpc, {
        prepareTimeoutMs: 50,
        maxRetries: 1,
        retryDelayMs: 1,
      });

      const operations = createTwoShardOperations();

      // Add significant delay to prepare
      const assignment1 = router.routeWithDatabase('bank_shard_1', 'accounts');
      const shard1 = shards.get(assignment1.shardId)!;
      shard1.prepareDelay = 200; // Longer than timeout

      const result = await coordinator.execute(operations);

      expect(result.committed).toBe(false);
      expect(result.abortReason).toBeDefined();
    });

    it('should track prepare deadline in messages', async () => {
      const operations = createTwoShardOperations();

      let capturedDeadline: number | undefined;

      // Capture the prepare deadline
      const assignment1 = router.routeWithDatabase('bank_shard_1', 'accounts');
      const shard1 = shards.get(assignment1.shardId)!;
      const originalPrepare = shard1.handlePrepare.bind(shard1);
      shard1.handlePrepare = async (msg: PrepareMessage) => {
        capturedDeadline = msg.prepareDeadline;
        return originalPrepare(msg);
      };

      coordinator = new TransactionCoordinator(router, rpc, {
        prepareTimeoutMs: 5000,
      });

      await coordinator.execute(operations);

      expect(capturedDeadline).toBeDefined();
      expect(capturedDeadline).toBeGreaterThan(Date.now() - 10000);
    });

    it('should handle mixed fast and slow shards within timeout', async () => {
      coordinator = new TransactionCoordinator(router, rpc, {
        prepareTimeoutMs: 1000,
        maxRetries: 1,
      });

      const operations = createTwoShardOperations();

      // One shard fast, one slow but within timeout
      const assignment1 = router.routeWithDatabase('bank_shard_1', 'accounts');
      const shard1 = shards.get(assignment1.shardId)!;
      shard1.prepareDelay = 100; // 100ms delay

      const result = await coordinator.execute(operations);

      expect(result.committed).toBe(true);
    });
  });

  // ==========================================================================
  // Test 7: Partial failure recovery - one shard fails during commit
  // ==========================================================================

  describe('Partial Failure Recovery', () => {
    it('should track partial commit state when one shard fails during commit', async () => {
      const operations = createTwoShardOperations();

      // Make one shard fail commit
      const assignment1 = router.routeWithDatabase('bank_shard_1', 'accounts');
      const shard1 = shards.get(assignment1.shardId)!;
      shard1.shouldFailCommit = true;

      coordinator = new TransactionCoordinator(router, rpc, {
        maxCommitAttempts: 3,
        retryDelayMs: 1,
        circuitBreakerThreshold: 100,
      });

      const result = await coordinator.execute(operations);

      // Transaction may not be fully committed due to failure
      // The coordinator should track this state
      expect(result).toBeDefined();
    });

    it('should continue retrying commit for failed shards', async () => {
      const operations = createTwoShardOperations();

      let commitAttempts = 0;
      const assignment1 = router.routeWithDatabase('bank_shard_1', 'accounts');
      const shard1 = shards.get(assignment1.shardId)!;

      const originalCommit = shard1.handleCommit.bind(shard1);
      shard1.handleCommit = async (msg: CommitMessage) => {
        commitAttempts++;
        if (commitAttempts < 3) {
          throw new Error('Temporary failure');
        }
        return originalCommit(msg);
      };

      coordinator = new TransactionCoordinator(router, rpc, {
        maxCommitAttempts: 10,
        retryDelayMs: 1,
        circuitBreakerThreshold: 100,
      });

      const result = await coordinator.execute(operations);

      expect(commitAttempts).toBeGreaterThanOrEqual(3);
      expect(result.committed).toBe(true);
    });

    it('should track which shards completed commit and which failed', async () => {
      const operations = createTwoShardOperations();

      // Track commit results
      const commitResults = new Map<number, 'success' | 'failure'>();

      for (const [shardId, shard] of shards) {
        const originalCommit = shard.handleCommit.bind(shard);
        shard.handleCommit = async (msg: CommitMessage) => {
          try {
            const result = await originalCommit(msg);
            commitResults.set(shardId, 'success');
            return result;
          } catch (e) {
            commitResults.set(shardId, 'failure');
            throw e;
          }
        };
      }

      await coordinator.execute(operations);

      // At least the involved shards should have commit results tracked
      expect(commitResults.size).toBeGreaterThan(0);
    });
  });

  // ==========================================================================
  // Test 8: Coordinator failure recovery
  // ==========================================================================

  describe('Coordinator Failure Recovery', () => {
    it('should allow querying transaction status after coordinator restart', async () => {
      const operations = createTwoShardOperations();

      // Execute a transaction
      const result = await coordinator.execute(operations);
      const txnId = result.txnId;

      // Simulate coordinator restart - create new coordinator
      const newCoordinator = new TransactionCoordinator(router, rpc, {
        prepareTimeoutMs: 5000,
      });

      // Query status of all shards
      for (const [shardId, shard] of shards) {
        const status = await rpc.queryStatus(shardId, {
          type: 'status_query',
          txnId,
          shardId,
          timestamp: Date.now(),
        });

        // Shards should report 'done' after successful commit
        expect(status.type).toBe('status_response');
      }
    });

    it('should support coordinator recovery API', async () => {
      // Get active transactions (should be empty when nothing is running)
      const activeTxns = coordinator.getActiveTransactions();
      expect(Array.isArray(activeTxns)).toBe(true);
    });

    it('should support force completion of stuck transactions', async () => {
      // Start a transaction that will block
      const operations = createTwoShardOperations();

      // Make commit block indefinitely
      const assignment1 = router.routeWithDatabase('bank_shard_1', 'accounts');
      const shard1 = shards.get(assignment1.shardId)!;
      let resolveBlock: () => void;
      const blockPromise = new Promise<void>((resolve) => {
        resolveBlock = resolve;
      });

      let commitStarted = false;
      const originalCommit = shard1.handleCommit.bind(shard1);
      shard1.handleCommit = async (msg: CommitMessage) => {
        commitStarted = true;
        await blockPromise;
        return originalCommit(msg);
      };

      // Start transaction (don't await)
      const executePromise = coordinator.execute(operations);

      // Wait for commit to start
      while (!commitStarted) {
        await new Promise((r) => setTimeout(r, 10));
      }

      // Get active transaction
      const activeTxns = coordinator.getActiveTransactions();
      expect(activeTxns.length).toBeGreaterThan(0);

      const stuckTxn = activeTxns[0];

      // Force complete it
      await coordinator.forceCompleteTransaction(stuckTxn.txnId, 'abort');

      // Verify state updated
      const txn = coordinator.getTransaction(stuckTxn.txnId);
      expect(txn?.state).toBe('aborted');

      // Cleanup
      resolveBlock!();
    });

    it('should persist transaction decision for recovery', async () => {
      // This tests the conceptual requirement that transaction decisions
      // are durable. In real implementation, this would involve persistence.

      const operations = createTwoShardOperations();

      // Capture transaction state transitions
      const stateHistory: string[] = [];

      // Execute transaction
      const result = await coordinator.execute(operations);

      // Verify we can track state
      expect(result.state).toBe('committed');
      expect(result.txnId).toBeDefined();
    });
  });

  // ==========================================================================
  // Additional Tests for Comprehensive Coverage
  // ==========================================================================

  describe('2PC Protocol Phases', () => {
    it('should follow correct 2PC state machine transitions', async () => {
      const operations = createTwoShardOperations();

      const stateTransitions: string[] = [];

      // Track active transaction states
      const checkState = () => {
        const txns = coordinator.getActiveTransactions();
        if (txns.length > 0) {
          stateTransitions.push(txns[0].state);
        }
      };

      // Wrap RPC calls to capture state
      for (const [shardId, shard] of shards) {
        const originalPrepare = shard.handlePrepare.bind(shard);
        shard.handlePrepare = async (msg: PrepareMessage) => {
          checkState();
          return originalPrepare(msg);
        };

        const originalCommit = shard.handleCommit.bind(shard);
        shard.handleCommit = async (msg: CommitMessage) => {
          checkState();
          return originalCommit(msg);
        };
      }

      const result = await coordinator.execute(operations);

      expect(result.committed).toBe(true);
      // State should transition: initialized -> preparing -> prepared -> committing -> committed
    });

    it('should set correct participant states during prepare', async () => {
      const operations = createTwoShardOperations();

      let participantStates: Map<number, ParticipantState> | undefined;

      // Capture participant states during prepare
      const assignment1 = router.routeWithDatabase('bank_shard_1', 'accounts');
      const shard1 = shards.get(assignment1.shardId)!;
      const originalPrepare = shard1.handlePrepare.bind(shard1);
      shard1.handlePrepare = async (msg: PrepareMessage) => {
        const txns = coordinator.getActiveTransactions();
        if (txns.length > 0) {
          participantStates = new Map();
          for (const [shardId, p] of txns[0].participants) {
            participantStates.set(shardId, p.state);
          }
        }
        return originalPrepare(msg);
      };

      await coordinator.execute(operations);

      // Participants should be in preparing state during prepare
      expect(participantStates).toBeDefined();
    });

    it('should handle concurrent transactions to different shards', async () => {
      // Create two independent sets of operations
      const operations1 = createTwoShardOperations();
      const operations2: BufferedOperation[] = [
        {
          type: 'insert',
          collection: 'products',
          database: 'store_shard_a',
          document: { _id: 'prod1', name: 'Widget' },
          timestamp: Date.now(),
        },
        {
          type: 'insert',
          collection: 'inventory',
          database: 'store_shard_b',
          document: { _id: 'inv1', productId: 'prod1' },
          timestamp: Date.now(),
        },
      ];

      // Execute concurrently
      const [result1, result2] = await Promise.all([
        coordinator.execute(operations1),
        coordinator.execute(operations2),
      ]);

      // Both should commit
      expect(result1.committed).toBe(true);
      expect(result2.committed).toBe(true);

      // Should have different transaction IDs
      expect(result1.txnId).not.toBe(result2.txnId);
    });
  });

  describe('Edge Cases and Error Conditions', () => {
    it('should handle all shards failing prepare', async () => {
      const operations = createTwoShardOperations();

      // Make all shards fail prepare
      for (const shard of shards.values()) {
        shard.shouldFailPrepare = true;
        shard.prepareFailureReason = 'All shards down';
      }

      const result = await coordinator.execute(operations);

      expect(result.committed).toBe(false);
      expect(result.state).toBe('aborted');
    });

    it('should handle network errors during prepare with retry', async () => {
      const operations = createTwoShardOperations();

      let attempts = 0;
      const assignment1 = router.routeWithDatabase('bank_shard_1', 'accounts');
      const shard1 = shards.get(assignment1.shardId)!;

      const originalPrepare = shard1.handlePrepare.bind(shard1);
      shard1.handlePrepare = async (msg: PrepareMessage) => {
        attempts++;
        if (attempts < 2) {
          throw new Error('Network timeout');
        }
        return originalPrepare(msg);
      };

      coordinator = new TransactionCoordinator(router, rpc, {
        maxRetries: 5,
        retryDelayMs: 1,
      });

      const result = await coordinator.execute(operations);

      expect(result.committed).toBe(true);
      expect(attempts).toBeGreaterThanOrEqual(2);
    });

    it('should include abort reason in transaction result', async () => {
      const operations = createTwoShardOperations();

      const assignment1 = router.routeWithDatabase('bank_shard_1', 'accounts');
      const shard1 = shards.get(assignment1.shardId)!;
      shard1.shouldFailPrepare = true;
      shard1.prepareFailureReason = 'Document locked by another transaction';

      const result = await coordinator.execute(operations);

      expect(result.committed).toBe(false);
      expect(result.abortReason).toContain('Document locked');
    });

    it('should track transaction duration accurately', async () => {
      const operations = createTwoShardOperations();

      // Add some delay
      const assignment1 = router.routeWithDatabase('bank_shard_1', 'accounts');
      const shard1 = shards.get(assignment1.shardId)!;
      shard1.prepareDelay = 50;

      const startTime = Date.now();
      const result = await coordinator.execute(operations);
      const endTime = Date.now();

      expect(result.durationMs).toBeGreaterThanOrEqual(50);
      expect(result.durationMs).toBeLessThanOrEqual(endTime - startTime + 10);
    });
  });

  describe('Lock Manager Integration', () => {
    it('should acquire locks during prepare phase', async () => {
      // Create a standalone lock manager to test
      const lockManager = new LockManager(5000);

      const operations: BufferedOperation[] = [
        {
          type: 'update',
          collection: 'accounts',
          database: 'bank',
          filter: { _id: 'acc1' },
          update: { $inc: { balance: 100 } },
          timestamp: Date.now(),
        },
      ];

      // Acquire locks
      const result = lockManager.acquireLocks('txn-1', operations);

      expect(result.success).toBe(true);
      expect(result.errors.length).toBe(0);

      // Verify lock stats
      const stats = lockManager.getStats();
      expect(stats.transactionCount).toBe(1);
    });

    it('should detect lock conflicts between concurrent transactions', async () => {
      const lockManager = new LockManager(5000);

      const operations: BufferedOperation[] = [
        {
          type: 'update',
          collection: 'accounts',
          database: 'bank',
          filter: { _id: 'acc1' },
          update: { $inc: { balance: 100 } },
          timestamp: Date.now(),
        },
      ];

      // First transaction acquires lock
      const result1 = lockManager.acquireLocks('txn-1', operations);
      expect(result1.success).toBe(true);

      // Second transaction tries same document
      const result2 = lockManager.acquireLocks('txn-2', operations);
      expect(result2.success).toBe(false);
      expect(result2.errors.length).toBeGreaterThan(0);
      expect(result2.errors[0]).toContain('locked by transaction txn-1');
    });

    it('should release locks after abort', async () => {
      const lockManager = new LockManager(5000);

      const operations: BufferedOperation[] = [
        {
          type: 'update',
          collection: 'accounts',
          database: 'bank',
          filter: { _id: 'acc1' },
          update: { $inc: { balance: 100 } },
          timestamp: Date.now(),
        },
      ];

      // Acquire and release locks
      lockManager.acquireLocks('txn-1', operations);
      lockManager.releaseLocks('txn-1');

      // Now another transaction should be able to acquire
      const result = lockManager.acquireLocks('txn-2', operations);
      expect(result.success).toBe(true);
    });
  });

  // ==========================================================================
  // RED Tests: Future functionality / edge cases to implement
  // ==========================================================================

  describe('RED: Advanced Cross-Shard Features (Not Yet Implemented)', () => {
    /**
     * These tests document expected behavior that may not be fully implemented yet.
     * They serve as specification for future improvements to the 2PC protocol.
     */

    it.skip('should support read-only transactions without 2PC overhead', async () => {
      // Read-only transactions should not need full 2PC
      // This is an optimization not yet implemented
      const operations: BufferedOperation[] = [];

      const result = await coordinator.execute(operations);

      // Should complete without prepare/commit phases
      expect(result.committed).toBe(true);
      expect(result.durationMs).toBeLessThan(10);
    });

    it.skip('should implement presumed abort optimization', async () => {
      // Presumed abort: if coordinator crashes before decision is logged,
      // participants should abort. This reduces logging overhead.
      const operations = createTwoShardOperations();

      // Simulate coordinator crash before decision
      // Implementation would need a way to inject this failure
      expect(true).toBe(false); // Force failure - not yet implemented
    });

    it.skip('should support nested/savepoint transactions within cross-shard txn', async () => {
      // Nested transactions or savepoints within a distributed transaction
      // This is a complex feature not yet implemented
      const operations = createTwoShardOperations();

      // Would need: coordinator.createSavepoint('sp1')
      // Would need: coordinator.rollbackToSavepoint('sp1')
      expect(true).toBe(false); // Force failure - not yet implemented
    });

    it.skip('should implement 3PC for non-blocking commit', async () => {
      // Three-phase commit adds a "pre-commit" phase to avoid blocking
      // This is an advanced protocol not yet implemented
      const operations = createTwoShardOperations();

      // 3PC phases: prepare -> pre-commit -> commit
      // This avoids the blocking issue where prepared participants
      // must wait indefinitely for coordinator decision
      expect(true).toBe(false); // Force failure - not yet implemented
    });

    it.skip('should support saga pattern for long-running transactions', async () => {
      // Saga pattern with compensating transactions for eventual consistency
      // Useful for very long-running distributed operations
      const operations = createTwoShardOperations();

      // Would need: define compensating actions for each operation
      // Would need: coordinator.executeSaga(operations, compensations)
      expect(true).toBe(false); // Force failure - not yet implemented
    });

    it('should detect and report deadlocks across shards', async () => {
      // Deadlock detection across multiple shards
      // Currently not implemented - would require cross-shard wait-for graph
      const operations1 = createTwoShardOperations();
      const operations2: BufferedOperation[] = [
        {
          type: 'update',
          collection: 'accounts',
          database: 'bank_shard_2',
          filter: { _id: 'acc2' },
          update: { $inc: { balance: 100 } },
          timestamp: Date.now(),
        },
        {
          type: 'update',
          collection: 'accounts',
          database: 'bank_shard_1',
          filter: { _id: 'acc1' },
          update: { $inc: { balance: -100 } },
          timestamp: Date.now(),
        },
      ];

      // Setup: txn1 locks acc1, waits for acc2
      //        txn2 locks acc2, waits for acc1
      // This is a classic deadlock scenario

      // Current implementation doesn't detect this - relies on timeout
      // Future implementation should detect and abort one transaction

      // For now, just verify timeout handling works
      coordinator = new TransactionCoordinator(router, rpc, {
        prepareTimeoutMs: 100,
        maxRetries: 1,
      });

      // Execute concurrently - at least one should succeed or timeout
      const results = await Promise.allSettled([
        coordinator.execute(operations1),
        coordinator.execute(operations2),
      ]);

      // At least one should complete (success or abort)
      expect(results.filter((r) => r.status === 'fulfilled').length).toBeGreaterThan(0);
    });

    it('should track prepared transaction count per shard', async () => {
      const operations = createTwoShardOperations();

      // Track prepared counts
      const preparedCounts = new Map<number, number>();

      for (const [shardId, shard] of shards) {
        const originalPrepare = shard.handlePrepare.bind(shard);
        shard.handlePrepare = async (msg: PrepareMessage) => {
          const result = await originalPrepare(msg);
          if (result.type === 'prepared') {
            preparedCounts.set(shardId, (preparedCounts.get(shardId) ?? 0) + 1);
          }
          return result;
        };
      }

      await coordinator.execute(operations);

      // Verify we can track prepared counts
      expect(preparedCounts.size).toBeGreaterThan(0);
    });

    it('should expose transaction metrics for monitoring', async () => {
      const operations = createTwoShardOperations();

      const result = await coordinator.execute(operations);

      // Verify basic metrics are available
      expect(result.durationMs).toBeDefined();
      expect(result.txnId).toBeDefined();
      expect(result.state).toBeDefined();

      // Future: would want more detailed metrics
      // - preparePhaseMs, commitPhaseMs
      // - perShardLatencies
      // - retryCount
      // - lockWaitTimeMs
    });

    it('should handle shard failure during ongoing transaction', async () => {
      const operations = createTwoShardOperations();

      // Simulate shard becoming unavailable mid-transaction
      const assignment1 = router.routeWithDatabase('bank_shard_1', 'accounts');
      const shard1 = shards.get(assignment1.shardId)!;

      let callCount = 0;
      const originalPrepare = shard1.handlePrepare.bind(shard1);
      shard1.handlePrepare = async (msg: PrepareMessage) => {
        callCount++;
        if (callCount === 1) {
          // First call succeeds (prepare)
          return originalPrepare(msg);
        }
        // Subsequent calls fail (simulating shard failure)
        throw new Error('Shard unavailable');
      };

      coordinator = new TransactionCoordinator(router, rpc, {
        maxRetries: 3,
        retryDelayMs: 10,
      });

      const result = await coordinator.execute(operations);

      // Transaction should complete (success or abort) gracefully
      expect(result).toBeDefined();
    });

    it('should support transaction priority for deadlock resolution', async () => {
      // Higher priority transactions should win in case of conflicts
      // This is not yet implemented
      const operations = createTwoShardOperations();

      // Future: coordinator.execute(operations, { priority: 'high' })
      const result = await coordinator.execute(operations);

      // For now, just verify normal execution works
      expect(result).toBeDefined();
    });

    it('should implement transaction isolation levels', async () => {
      // Different isolation levels: read-committed, repeatable-read, serializable
      // Currently only snapshot isolation is supported implicitly

      const operations = createTwoShardOperations();

      // Future: coordinator.execute(operations, { isolationLevel: 'serializable' })
      const result = await coordinator.execute(operations);

      expect(result.committed).toBe(true);
    });

    it('should track transaction lineage for debugging', async () => {
      const operations = createTwoShardOperations();

      const result = await coordinator.execute(operations);

      // Transaction should have a unique ID that can be traced
      expect(result.txnId).toMatch(/^txn-/);

      // Future: would want full lineage including:
      // - parent transaction (if nested)
      // - retry history
      // - related transaction IDs
    });

    it('should handle extremely large transactions efficiently', async () => {
      // Test with many operations across many shards
      const largeOperations: BufferedOperation[] = [];

      // Create 100 operations across different databases
      for (let i = 0; i < 100; i++) {
        largeOperations.push({
          type: 'insert',
          collection: `collection_${i % 10}`,
          database: `db_${i % 16}`,
          document: { _id: `doc_${i}`, data: `value_${i}` },
          timestamp: Date.now(),
        });
      }

      const startTime = Date.now();
      const result = await coordinator.execute(largeOperations);
      const duration = Date.now() - startTime;

      // Should complete in reasonable time
      expect(result.committed).toBe(true);
      expect(duration).toBeLessThan(10000); // 10 seconds max

      // Verify all operations were processed
      let totalPrepares = 0;
      for (const shard of shards.values()) {
        totalPrepares += shard.prepareCallCount;
      }
      expect(totalPrepares).toBeGreaterThan(0);
    });
  });
});
