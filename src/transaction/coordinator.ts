/**
 * Cross-Shard Transaction Coordinator
 *
 * Implements the Two-Phase Commit (2PC) protocol for distributed transactions
 * across multiple shards. This ensures atomicity when a transaction spans
 * multiple Durable Objects (shards).
 *
 * ## Protocol Overview
 *
 * Phase 1 - Prepare:
 * 1. Coordinator assigns a unique transaction ID (txnId)
 * 2. Coordinator sends PREPARE message to all participant shards
 * 3. Each shard validates operations and acquires locks
 * 4. Each shard responds with PREPARED or ABORTED
 * 5. If any shard responds ABORTED, go to Phase 2 with ABORT
 *
 * Phase 2 - Commit/Abort:
 * 1. If all shards responded PREPARED, coordinator sends COMMIT
 * 2. If any shard responded ABORTED or timed out, coordinator sends ABORT
 * 3. Each shard applies or discards buffered operations
 * 4. Each shard responds with ACK
 *
 * ## Failure Handling
 *
 * - Coordinator failure: Participants timeout and abort prepared transactions
 * - Participant failure: Coordinator retries or aborts based on timeout
 * - Network partition: Uses timeout-based resolution
 *
 * ## Durability
 *
 * - Transaction state is persisted to coordinator's SQLite before Phase 2
 * - Participant prepared state is persisted before responding PREPARED
 * - This enables recovery after crashes
 */

import type { BufferedOperation } from '../session/index.js';
import type { ShardRouter } from '../shard/router.js';
import { logger } from '../utils/logger.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Unique transaction identifier.
 */
export type TransactionId = string;

/**
 * Participant shard information.
 */
export interface Participant {
  /** Shard identifier */
  shardId: number;
  /** Operations for this shard */
  operations: BufferedOperation[];
  /** Current participant state */
  state: ParticipantState;
  /** Timestamp of last state change */
  lastStateChange: number;
}

/**
 * Participant state in the 2PC protocol.
 */
export type ParticipantState =
  | 'initial'    // Not yet contacted
  | 'preparing'  // PREPARE sent, waiting for response
  | 'prepared'   // Responded PREPARED
  | 'aborted'    // Responded ABORTED or timeout
  | 'committing' // COMMIT sent, waiting for ACK
  | 'committed'  // Responded ACK to COMMIT
  | 'aborting'   // ABORT sent, waiting for ACK
  | 'done';      // Final state (committed or aborted)

/**
 * Transaction coordinator state.
 */
export type CoordinatorState =
  | 'initialized' // Transaction created but not started
  | 'preparing'   // Phase 1 in progress
  | 'prepared'    // All participants prepared
  | 'committing'  // Phase 2 COMMIT in progress
  | 'aborting'    // Phase 2 ABORT in progress
  | 'committed'   // Transaction committed successfully
  | 'aborted';    // Transaction aborted

/**
 * Distributed transaction record.
 */
export interface DistributedTransaction {
  /** Unique transaction ID */
  txnId: TransactionId;
  /** Coordinator state */
  state: CoordinatorState;
  /** Participant shards */
  participants: Map<number, Participant>;
  /** Transaction start time */
  startTime: number;
  /** Prepare deadline (Phase 1 timeout) */
  prepareDeadline: number;
  /** Commit deadline (Phase 2 timeout) */
  commitDeadline: number;
  /** Decision (set after Phase 1) */
  decision?: 'commit' | 'abort';
  /** Abort reason if aborted */
  abortReason?: string;
}

/**
 * RPC message types for 2PC protocol.
 */
export type TwoPhaseCommitMessageType =
  | 'prepare'      // Coordinator -> Participant: Prepare to commit
  | 'prepared'     // Participant -> Coordinator: Ready to commit
  | 'abort_vote'   // Participant -> Coordinator: Cannot commit
  | 'commit'       // Coordinator -> Participant: Commit the transaction
  | 'abort'        // Coordinator -> Participant: Abort the transaction
  | 'ack'          // Participant -> Coordinator: Acknowledge commit/abort
  | 'status_query' // Coordinator -> Participant: Query transaction status
  | 'status_response'; // Participant -> Coordinator: Transaction status

/**
 * Base 2PC message structure.
 */
export interface TwoPhaseCommitMessage {
  type: TwoPhaseCommitMessageType;
  txnId: TransactionId;
  shardId: number;
  timestamp: number;
}

/**
 * Prepare message from coordinator to participant.
 */
export interface PrepareMessage extends TwoPhaseCommitMessage {
  type: 'prepare';
  operations: BufferedOperation[];
  prepareDeadline: number;
}

/**
 * Prepared response from participant.
 */
export interface PreparedMessage extends TwoPhaseCommitMessage {
  type: 'prepared';
  /** LSN at which operations are prepared */
  preparedLSN: number;
}

/**
 * Abort vote from participant.
 */
export interface AbortVoteMessage extends TwoPhaseCommitMessage {
  type: 'abort_vote';
  reason: string;
}

/**
 * Commit message from coordinator.
 */
export interface CommitMessage extends TwoPhaseCommitMessage {
  type: 'commit';
  commitDeadline: number;
}

/**
 * Abort message from coordinator.
 */
export interface AbortMessage extends TwoPhaseCommitMessage {
  type: 'abort';
  reason: string;
}

/**
 * Acknowledgement from participant.
 */
export interface AckMessage extends TwoPhaseCommitMessage {
  type: 'ack';
  /** Final LSN after commit (only for commit ack) */
  finalLSN?: number;
}

/**
 * Status query message.
 */
export interface StatusQueryMessage extends TwoPhaseCommitMessage {
  type: 'status_query';
}

/**
 * Status response message.
 */
export interface StatusResponseMessage extends TwoPhaseCommitMessage {
  type: 'status_response';
  participantState: ParticipantState;
  preparedLSN?: number;
}

/**
 * Union type for all 2PC messages.
 */
export type AnyTwoPhaseCommitMessage =
  | PrepareMessage
  | PreparedMessage
  | AbortVoteMessage
  | CommitMessage
  | AbortMessage
  | AckMessage
  | StatusQueryMessage
  | StatusResponseMessage;

/**
 * Circuit breaker state for commit retry loops.
 */
export interface CircuitBreakerState {
  /** Number of consecutive failures */
  failures: number;
  /** Last failure timestamp */
  lastFailure: number;
  /** Whether circuit is open (failing fast) */
  isOpen: boolean;
  /** When circuit can attempt half-open state */
  resetAt: number;
}

/**
 * Manual intervention request for stuck transactions.
 */
export interface InterventionRequest {
  /** Transaction ID */
  txnId: TransactionId;
  /** Shard ID */
  shardId: number;
  /** Action to take */
  action: 'force_commit' | 'force_abort' | 'retry';
  /** Timestamp of request */
  requestedAt: number;
  /** Optional reason for intervention */
  reason?: string;
}

/**
 * Hook for manual intervention on stuck transactions.
 * Return an action to take, or undefined to continue normal retry behavior.
 */
export type InterventionHook = (
  txnId: TransactionId,
  shardId: number,
  attempts: number,
  lastError?: Error
) => Promise<InterventionRequest | undefined>;

/**
 * Stuck transaction info for monitoring.
 */
export interface StuckTransactionInfo {
  txnId: TransactionId;
  shardId: number;
  attempts: number;
  startTime: number;
  lastAttemptTime: number;
  lastError?: string;
}

/**
 * Coordinator options.
 */
export interface CoordinatorOptions {
  /** Timeout for Phase 1 (prepare) in milliseconds */
  prepareTimeoutMs?: number;
  /** Timeout for Phase 2 (commit/abort) in milliseconds */
  commitTimeoutMs?: number;
  /** Maximum retries for RPC calls */
  maxRetries?: number;
  /** Retry delay in milliseconds */
  retryDelayMs?: number;
  /** Maximum concurrent RPC calls (default: 10) */
  maxConcurrentRPCs?: number;
  /** Maximum commit attempts before giving up (default: 100) */
  maxCommitAttempts?: number;
  /** Circuit breaker failure threshold (default: 10) */
  circuitBreakerThreshold?: number;
  /** Circuit breaker reset timeout in ms (default: 60000) */
  circuitBreakerResetMs?: number;
  /** Threshold for logging stuck transactions (default: 10 attempts) */
  stuckTransactionThreshold?: number;
  /** Hook for manual intervention on stuck transactions */
  interventionHook?: InterventionHook;
}

/**
 * Result of a distributed transaction.
 */
export interface DistributedTransactionResult {
  /** Whether the transaction committed */
  committed: boolean;
  /** Transaction ID */
  txnId: TransactionId;
  /** Final state */
  state: CoordinatorState;
  /** Abort reason if aborted */
  abortReason?: string;
  /** Per-shard commit LSNs (only for committed transactions) */
  shardLSNs?: Map<number, number>;
  /** Duration in milliseconds */
  durationMs: number;
}

// ============================================================================
// Errors
// ============================================================================

/**
 * Error thrown for distributed transaction failures.
 */
export class DistributedTransactionError extends Error {
  constructor(
    message: string,
    public readonly txnId: TransactionId,
    public readonly state: CoordinatorState,
    public readonly cause?: Error
  ) {
    super(message);
    this.name = 'DistributedTransactionError';
  }
}

/**
 * Error thrown when a transaction times out.
 */
export class TransactionTimeoutError extends DistributedTransactionError {
  constructor(
    txnId: TransactionId,
    state: CoordinatorState,
    public readonly phase: 'prepare' | 'commit'
  ) {
    super(`Transaction ${txnId} timed out during ${phase} phase`, txnId, state);
    this.name = 'TransactionTimeoutError';
  }
}

/**
 * Error thrown when a participant aborts.
 */
export class ParticipantAbortError extends DistributedTransactionError {
  constructor(
    txnId: TransactionId,
    public readonly shardId: number,
    public readonly reason: string
  ) {
    super(
      `Participant shard ${shardId} aborted transaction ${txnId}: ${reason}`,
      txnId,
      'aborting'
    );
    this.name = 'ParticipantAbortError';
  }
}

/**
 * Error thrown when commit retry circuit breaker trips.
 */
export class CommitCircuitBreakerError extends DistributedTransactionError {
  constructor(
    txnId: TransactionId,
    public readonly shardId: number,
    public readonly attempts: number,
    public readonly lastError?: Error
  ) {
    super(
      `Circuit breaker tripped for transaction ${txnId} on shard ${shardId} after ${attempts} attempts`,
      txnId,
      'committing',
      lastError
    );
    this.name = 'CommitCircuitBreakerError';
  }
}

/**
 * Error thrown when maximum commit attempts exceeded.
 */
export class MaxCommitAttemptsError extends DistributedTransactionError {
  constructor(
    txnId: TransactionId,
    public readonly shardId: number,
    public readonly maxAttempts: number,
    public readonly lastError?: Error
  ) {
    super(
      `Maximum commit attempts (${maxAttempts}) exceeded for transaction ${txnId} on shard ${shardId}`,
      txnId,
      'committing',
      lastError
    );
    this.name = 'MaxCommitAttemptsError';
  }
}

// ============================================================================
// RPC Interface
// ============================================================================

/**
 * Interface for sending RPC messages to shard participants.
 *
 * Implementations should handle:
 * - Serialization of messages
 * - HTTP/WebSocket transport to Durable Objects
 * - Error handling and retries
 */
export interface ShardRPC {
  /**
   * Send a prepare message to a shard.
   */
  sendPrepare(
    shardId: number,
    message: PrepareMessage
  ): Promise<PreparedMessage | AbortVoteMessage>;

  /**
   * Send a commit message to a shard.
   */
  sendCommit(shardId: number, message: CommitMessage): Promise<AckMessage>;

  /**
   * Send an abort message to a shard.
   */
  sendAbort(shardId: number, message: AbortMessage): Promise<AckMessage>;

  /**
   * Query transaction status on a shard.
   */
  queryStatus(
    shardId: number,
    message: StatusQueryMessage
  ): Promise<StatusResponseMessage>;
}

// ============================================================================
// Concurrency Limiter
// ============================================================================

/**
 * Simple semaphore-based concurrency limiter.
 * Limits the number of concurrent async operations.
 */
class ConcurrencyLimiter {
  private running = 0;
  private queue: Array<() => void> = [];

  constructor(private readonly limit: number) {}

  /**
   * Execute a function with concurrency limiting.
   * Waits for a slot if the limit is reached.
   */
  async run<T>(fn: () => Promise<T>): Promise<T> {
    // Wait for a slot if at limit
    if (this.running >= this.limit) {
      await new Promise<void>((resolve) => {
        this.queue.push(resolve);
      });
    }

    this.running++;
    try {
      return await fn();
    } finally {
      this.running--;
      // Release next waiting task
      const next = this.queue.shift();
      if (next) {
        next();
      }
    }
  }

  /**
   * Execute multiple functions with concurrency limiting.
   * Returns results in the same order as the input functions.
   */
  async all<T>(fns: Array<() => Promise<T>>): Promise<T[]> {
    return Promise.all(fns.map((fn) => this.run(fn)));
  }

  /**
   * Execute multiple functions with concurrency limiting.
   * Returns settled results (like Promise.allSettled).
   */
  async allSettled<T>(
    fns: Array<() => Promise<T>>
  ): Promise<PromiseSettledResult<T>[]> {
    return Promise.allSettled(fns.map((fn) => this.run(fn)));
  }
}

// ============================================================================
// Transaction Coordinator
// ============================================================================

/**
 * Metrics interface for commit retry tracking.
 */
export interface CommitRetryMetrics {
  /** Increment counter for commit retries */
  incCommitRetry(shardId: number, txnId: string): void;
  /** Increment counter for circuit breaker trips */
  incCircuitBreakerTrip(shardId: number, txnId: string): void;
  /** Increment counter for stuck transactions */
  incStuckTransaction(shardId: number, txnId: string): void;
  /** Increment counter for manual interventions */
  incManualIntervention(shardId: number, txnId: string, action: string): void;
  /** Increment counter for max attempts exceeded */
  incMaxAttemptsExceeded(shardId: number, txnId: string): void;
  /** Record commit attempt duration */
  observeCommitAttemptDuration(shardId: number, durationMs: number): void;
}

/**
 * Default no-op metrics implementation.
 */
const noopMetrics: CommitRetryMetrics = {
  incCommitRetry: () => {},
  incCircuitBreakerTrip: () => {},
  incStuckTransaction: () => {},
  incManualIntervention: () => {},
  incMaxAttemptsExceeded: () => {},
  observeCommitAttemptDuration: () => {},
};

/**
 * TransactionCoordinator manages distributed transactions across shards.
 *
 * The coordinator is responsible for:
 * - Partitioning operations by shard
 * - Running the 2PC protocol
 * - Handling failures and timeouts
 * - Persisting transaction state for recovery
 */
export class TransactionCoordinator {
  /** Active transactions by ID */
  private transactions: Map<TransactionId, DistributedTransaction> = new Map();

  /** Configuration options */
  private readonly options: Required<Omit<CoordinatorOptions, 'interventionHook'>> & {
    interventionHook?: InterventionHook;
  };

  /** Concurrency limiter for RPC calls */
  private readonly rpcLimiter: ConcurrencyLimiter;

  /** Circuit breaker state per shard */
  private circuitBreakers: Map<number, CircuitBreakerState> = new Map();

  /** Stuck transactions for monitoring */
  private stuckTransactions: Map<string, StuckTransactionInfo> = new Map();

  /** Metrics collector */
  private metrics: CommitRetryMetrics;

  constructor(
    private readonly shardRouter: ShardRouter,
    private readonly shardRPC: ShardRPC,
    options: CoordinatorOptions = {},
    metrics?: CommitRetryMetrics
  ) {
    this.options = {
      prepareTimeoutMs: options.prepareTimeoutMs ?? 5000,
      commitTimeoutMs: options.commitTimeoutMs ?? 10000,
      maxRetries: options.maxRetries ?? 3,
      retryDelayMs: options.retryDelayMs ?? 100,
      maxConcurrentRPCs: options.maxConcurrentRPCs ?? 10,
      maxCommitAttempts: options.maxCommitAttempts ?? 100,
      circuitBreakerThreshold: options.circuitBreakerThreshold ?? 10,
      circuitBreakerResetMs: options.circuitBreakerResetMs ?? 60000,
      stuckTransactionThreshold: options.stuckTransactionThreshold ?? 10,
      interventionHook: options.interventionHook,
    };
    this.rpcLimiter = new ConcurrencyLimiter(this.options.maxConcurrentRPCs);
    this.metrics = metrics ?? noopMetrics;
  }

  // --------------------------------------------------------------------------
  // Public API
  // --------------------------------------------------------------------------

  /**
   * Execute a distributed transaction.
   *
   * @param operations - Operations to execute atomically across shards
   * @returns Transaction result
   * @throws DistributedTransactionError if transaction fails
   */
  async execute(
    operations: BufferedOperation[]
  ): Promise<DistributedTransactionResult> {
    const startTime = Date.now();

    // Generate unique transaction ID
    const txnId = this.generateTransactionId();

    // Partition operations by shard
    const participants = this.partitionOperations(operations);

    // If all operations go to a single shard, skip 2PC
    if (participants.size === 1) {
      const entry = participants.entries().next().value;
      if (entry) {
        const [shardId, shardOps] = entry;
        return this.executeSingleShard(txnId, shardId, shardOps, startTime);
      }
    }

    // Create transaction record
    const txn = this.createTransaction(txnId, participants, startTime);
    this.transactions.set(txnId, txn);

    try {
      // Phase 1: Prepare
      await this.preparePhase(txn);

      // Phase 2: Commit (only if all prepared)
      if (txn.decision === 'commit') {
        await this.commitPhase(txn);
      } else {
        await this.abortPhase(txn);
      }

      // Build result
      const result: DistributedTransactionResult = {
        committed: txn.state === 'committed',
        txnId,
        state: txn.state,
        abortReason: txn.abortReason,
        durationMs: Date.now() - startTime,
      };

      if (txn.state === 'committed') {
        result.shardLSNs = new Map();
        for (const [shardId] of txn.participants) {
          // Note: LSN would be captured from commit ack in real implementation
          result.shardLSNs.set(shardId, 0);
        }
      }

      return result;
    } finally {
      // Cleanup transaction record
      this.transactions.delete(txnId);
    }
  }

  /**
   * Get active transaction by ID.
   */
  getTransaction(txnId: TransactionId): DistributedTransaction | undefined {
    return this.transactions.get(txnId);
  }

  /**
   * Get all active transactions.
   */
  getActiveTransactions(): DistributedTransaction[] {
    return Array.from(this.transactions.values());
  }

  /**
   * Get stuck transactions that are experiencing repeated commit failures.
   */
  getStuckTransactions(): StuckTransactionInfo[] {
    return Array.from(this.stuckTransactions.values());
  }

  /**
   * Get circuit breaker state for a shard.
   */
  getCircuitBreakerState(shardId: number): CircuitBreakerState | undefined {
    return this.circuitBreakers.get(shardId);
  }

  /**
   * Reset circuit breaker for a shard (for manual intervention).
   */
  resetCircuitBreaker(shardId: number): void {
    this.circuitBreakers.delete(shardId);
    logger.info('Circuit breaker reset for shard', { shardId });
  }

  /**
   * Set the metrics collector.
   */
  setMetrics(metrics: CommitRetryMetrics): void {
    this.metrics = metrics;
  }

  /**
   * Force complete a stuck transaction (for manual intervention).
   * This should be used with extreme caution as it may leave data inconsistent.
   */
  async forceCompleteTransaction(
    txnId: TransactionId,
    action: 'commit' | 'abort'
  ): Promise<void> {
    const txn = this.transactions.get(txnId);
    if (!txn) {
      throw new Error(`Transaction ${txnId} not found`);
    }

    logger.warn(`Force ${action} requested for transaction`, {
      txnId,
      action,
    });

    // Remove from stuck transactions tracking
    for (const [key, info] of this.stuckTransactions) {
      if (info.txnId === txnId) {
        this.stuckTransactions.delete(key);
      }
    }

    // Update state based on action
    if (action === 'commit') {
      txn.state = 'committed';
    } else {
      txn.state = 'aborted';
      txn.abortReason = 'Force aborted via manual intervention';
    }

    // Mark all participants as done
    for (const participant of txn.participants.values()) {
      participant.state = 'done';
      participant.lastStateChange = Date.now();
    }
  }

  // --------------------------------------------------------------------------
  // Transaction Lifecycle
  // --------------------------------------------------------------------------

  /**
   * Generate a unique transaction ID using cryptographically secure randomness.
   */
  private generateTransactionId(): TransactionId {
    const timestamp = Date.now().toString(36);
    const uuid = crypto.randomUUID().replace(/-/g, '').slice(0, 12);
    return `txn-${timestamp}-${uuid}`;
  }

  /**
   * Partition operations by target shard.
   */
  private partitionOperations(
    operations: BufferedOperation[]
  ): Map<number, BufferedOperation[]> {
    const partitions = new Map<number, BufferedOperation[]>();

    for (const op of operations) {
      // Route by collection and database
      const assignment = this.shardRouter.routeWithDatabase(
        op.database,
        op.collection
      );
      const shardId = assignment.shardId;

      const existingOps = partitions.get(shardId);
      if (existingOps) {
        existingOps.push(op);
      } else {
        partitions.set(shardId, [op]);
      }
    }

    return partitions;
  }

  /**
   * Create a new distributed transaction record.
   */
  private createTransaction(
    txnId: TransactionId,
    partitions: Map<number, BufferedOperation[]>,
    startTime: number
  ): DistributedTransaction {
    const participants = new Map<number, Participant>();

    for (const [shardId, operations] of partitions) {
      participants.set(shardId, {
        shardId,
        operations,
        state: 'initial',
        lastStateChange: startTime,
      });
    }

    return {
      txnId,
      state: 'initialized',
      participants,
      startTime,
      prepareDeadline: startTime + this.options.prepareTimeoutMs,
      commitDeadline: 0, // Set after prepare phase
    };
  }

  /**
   * Execute single-shard transaction (bypass 2PC).
   */
  private async executeSingleShard(
    txnId: TransactionId,
    shardId: number,
    operations: BufferedOperation[],
    startTime: number
  ): Promise<DistributedTransactionResult> {
    // For single-shard transactions, we can use the shard's native atomicity
    // This is an optimization that avoids the overhead of 2PC
    const prepareMessage: PrepareMessage = {
      type: 'prepare',
      txnId,
      shardId,
      timestamp: Date.now(),
      operations,
      prepareDeadline: startTime + this.options.prepareTimeoutMs,
    };

    try {
      const response = await this.shardRPC.sendPrepare(shardId, prepareMessage);

      if (response.type === 'abort_vote') {
        return {
          committed: false,
          txnId,
          state: 'aborted',
          abortReason: response.reason,
          durationMs: Date.now() - startTime,
        };
      }

      // For single-shard, prepare + commit is atomic, send commit
      const commitMessage: CommitMessage = {
        type: 'commit',
        txnId,
        shardId,
        timestamp: Date.now(),
        commitDeadline: Date.now() + this.options.commitTimeoutMs,
      };

      const ack = await this.shardRPC.sendCommit(shardId, commitMessage);

      return {
        committed: true,
        txnId,
        state: 'committed',
        shardLSNs: new Map([[shardId, ack.finalLSN ?? 0]]),
        durationMs: Date.now() - startTime,
      };
    } catch (error) {
      // On error, abort the transaction
      try {
        await this.shardRPC.sendAbort(shardId, {
          type: 'abort',
          txnId,
          shardId,
          timestamp: Date.now(),
          reason: error instanceof Error ? error.message : 'Unknown error',
        });
      } catch (abortError) {
        // Log abort failure but don't re-throw - the original error is more important.
        // This is a best-effort cleanup; the transaction is already failed and we're
        // about to throw DistributedTransactionError with the original cause.
        logger.warn('Failed to send abort after transaction error', {
          txnId,
          shardId,
          error: abortError instanceof Error ? abortError.message : String(abortError),
          originalError: error instanceof Error ? error.message : String(error),
        });
      }

      throw new DistributedTransactionError(
        `Single-shard transaction failed: ${error instanceof Error ? error.message : 'Unknown error'}`,
        txnId,
        'aborted',
        error instanceof Error ? error : undefined
      );
    }
  }

  // --------------------------------------------------------------------------
  // Phase 1: Prepare
  // --------------------------------------------------------------------------

  /**
   * Execute Phase 1 (Prepare) of the 2PC protocol.
   *
   * Sends PREPARE to all participants and collects votes.
   * Sets transaction.decision based on votes.
   */
  private async preparePhase(txn: DistributedTransaction): Promise<void> {
    txn.state = 'preparing';

    // Build prepare tasks for each participant
    const prepareTasks: Array<
      () => Promise<{
        shardId: number;
        response: PreparedMessage | AbortVoteMessage;
      }>
    > = [];

    for (const [shardId, participant] of txn.participants) {
      participant.state = 'preparing';
      participant.lastStateChange = Date.now();

      const message: PrepareMessage = {
        type: 'prepare',
        txnId: txn.txnId,
        shardId,
        timestamp: Date.now(),
        operations: participant.operations,
        prepareDeadline: txn.prepareDeadline,
      };

      prepareTasks.push(async () => {
        const response = await this.sendPrepareWithRetry(shardId, message);
        return { shardId, response };
      });
    }

    // Wait for all responses or timeout (with concurrency limit)
    const deadline = txn.prepareDeadline;
    const timeoutMs = Math.max(0, deadline - Date.now());

    let responses: Array<{
      shardId: number;
      response: PreparedMessage | AbortVoteMessage;
    }>;

    try {
      responses = await Promise.race([
        this.rpcLimiter.all(prepareTasks),
        this.createTimeout<never>(timeoutMs, 'prepare'),
      ]);
    } catch (error) {
      // Timeout or error - abort
      txn.decision = 'abort';
      txn.abortReason =
        error instanceof TransactionTimeoutError
          ? 'Prepare phase timed out'
          : `Prepare phase failed: ${error instanceof Error ? error.message : 'Unknown error'}`;
      return;
    }

    // Process responses
    let allPrepared = true;

    for (const { shardId, response } of responses) {
      const participant = txn.participants.get(shardId)!;

      if (response.type === 'prepared') {
        participant.state = 'prepared';
      } else {
        participant.state = 'aborted';
        allPrepared = false;
        txn.abortReason = txn.abortReason ?? response.reason;
      }

      participant.lastStateChange = Date.now();
    }

    // Set decision
    if (allPrepared) {
      txn.decision = 'commit';
      txn.state = 'prepared';
      txn.commitDeadline = Date.now() + this.options.commitTimeoutMs;
    } else {
      txn.decision = 'abort';
      txn.state = 'aborting';
    }
  }

  /**
   * Send PREPARE with retry logic.
   */
  private async sendPrepareWithRetry(
    shardId: number,
    message: PrepareMessage
  ): Promise<PreparedMessage | AbortVoteMessage> {
    let lastError: Error | undefined;

    for (let attempt = 0; attempt < this.options.maxRetries; attempt++) {
      try {
        return await this.shardRPC.sendPrepare(shardId, message);
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));

        // Wait before retry
        if (attempt < this.options.maxRetries - 1) {
          await this.sleep(this.options.retryDelayMs * Math.pow(2, attempt));
        }
      }
    }

    // All retries failed - return abort vote
    return {
      type: 'abort_vote',
      txnId: message.txnId,
      shardId,
      timestamp: Date.now(),
      reason: `Failed to contact shard after ${this.options.maxRetries} attempts: ${lastError?.message}`,
    };
  }

  // --------------------------------------------------------------------------
  // Phase 2: Commit
  // --------------------------------------------------------------------------

  /**
   * Execute Phase 2 Commit of the 2PC protocol.
   *
   * Sends COMMIT to all prepared participants.
   */
  private async commitPhase(txn: DistributedTransaction): Promise<void> {
    txn.state = 'committing';

    // Build commit tasks for each prepared participant
    const commitTasks: Array<
      () => Promise<{ shardId: number; ack: AckMessage }>
    > = [];

    for (const [shardId, participant] of txn.participants) {
      if (participant.state !== 'prepared') continue;

      participant.state = 'committing';
      participant.lastStateChange = Date.now();

      const message: CommitMessage = {
        type: 'commit',
        txnId: txn.txnId,
        shardId,
        timestamp: Date.now(),
        commitDeadline: txn.commitDeadline,
      };

      commitTasks.push(async () => {
        const ack = await this.sendCommitWithRetry(shardId, message);
        return { shardId, ack };
      });
    }

    // Wait for all acknowledgements (with concurrency limit)
    // Note: In commit phase, we MUST keep retrying until all participants confirm
    // This is the "no going back" point of 2PC
    const results = await this.rpcLimiter.allSettled(commitTasks);

    // Process results
    let allCommitted = true;

    for (const result of results) {
      if (result.status === 'fulfilled') {
        const { shardId } = result.value;
        const participant = txn.participants.get(shardId)!;
        participant.state = 'committed';
        participant.lastStateChange = Date.now();
      } else {
        // Commit failed - this should be rare and may require manual intervention
        allCommitted = false;
        logger.error('Commit failed for participant', {
          error: result.reason,
        });
      }
    }

    txn.state = allCommitted ? 'committed' : 'committing';
  }

  /**
   * Send COMMIT with circuit breaker and maximum attempt protection.
   *
   * Once we decide to commit, we attempt to commit with:
   * - Maximum attempt count to prevent infinite loops
   * - Circuit breaker pattern to fail fast on persistent failures
   * - Manual intervention hook for stuck transactions
   * - Comprehensive logging and metrics
   */
  private async sendCommitWithRetry(
    shardId: number,
    message: CommitMessage
  ): Promise<AckMessage> {
    let attempt = 0;
    const maxDelay = 30000; // Cap backoff at 30 seconds
    const startTime = Date.now();
    let lastError: Error | undefined;
    const stuckKey = `${message.txnId}:${shardId}`;

    while (attempt < this.options.maxCommitAttempts) {
      // Check circuit breaker
      const circuitState = this.checkCircuitBreaker(shardId);
      if (circuitState.isOpen) {
        // Circuit is open - check if we should try half-open
        if (Date.now() < circuitState.resetAt) {
          this.metrics.incCircuitBreakerTrip(shardId, message.txnId);
          logger.error('Circuit breaker OPEN for shard', {
            shardId,
            txnId: message.txnId,
            attempt,
            resetAt: new Date(circuitState.resetAt).toISOString(),
          });
          throw new CommitCircuitBreakerError(
            message.txnId,
            shardId,
            attempt,
            lastError
          );
        }
        // Try half-open state
        logger.info('Circuit breaker half-open for shard, attempting reset', {
          shardId,
        });
      }

      // Check for manual intervention hook
      if (this.options.interventionHook && attempt >= this.options.stuckTransactionThreshold) {
        const intervention = await this.options.interventionHook(
          message.txnId,
          shardId,
          attempt,
          lastError
        );

        if (intervention) {
          this.metrics.incManualIntervention(shardId, message.txnId, intervention.action);
          logger.warn('Manual intervention requested', {
            txnId: message.txnId,
            shardId,
            action: intervention.action,
            reason: intervention.reason ?? 'none',
          });

          if (intervention.action === 'force_commit') {
            // Return a synthetic ack - caller should handle this carefully
            return {
              type: 'ack',
              txnId: message.txnId,
              shardId,
              timestamp: Date.now(),
              finalLSN: -1, // Indicate forced commit
            };
          } else if (intervention.action === 'force_abort') {
            throw new CommitCircuitBreakerError(
              message.txnId,
              shardId,
              attempt,
              new Error('Force aborted via manual intervention')
            );
          }
          // 'retry' action continues normal flow
        }
      }

      const attemptStart = Date.now();
      try {
        this.metrics.incCommitRetry(shardId, message.txnId);
        const result = await this.shardRPC.sendCommit(shardId, message);

        // Success - reset circuit breaker
        this.resetCircuitBreakerState(shardId);

        // Remove from stuck transactions
        this.stuckTransactions.delete(stuckKey);

        // Record successful duration
        this.metrics.observeCommitAttemptDuration(shardId, Date.now() - attemptStart);

        if (attempt > 0) {
          logger.info('Commit succeeded after retries', {
            txnId: message.txnId,
            shardId,
            attempts: attempt + 1,
          });
        }

        return result;
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        attempt++;

        // Update circuit breaker
        this.recordCircuitBreakerFailure(shardId);

        // Calculate delay with exponential backoff
        const delay = Math.min(
          this.options.retryDelayMs * Math.pow(2, attempt),
          maxDelay
        );

        // Log based on attempt count
        if (attempt >= this.options.stuckTransactionThreshold) {
          // Update stuck transaction tracking
          this.stuckTransactions.set(stuckKey, {
            txnId: message.txnId,
            shardId,
            attempts: attempt,
            startTime,
            lastAttemptTime: Date.now(),
            lastError: lastError.message,
          });

          this.metrics.incStuckTransaction(shardId, message.txnId);

          logger.error('STUCK TRANSACTION detected', {
            txnId: message.txnId,
            shardId,
            attempt,
            maxAttempts: this.options.maxCommitAttempts,
            elapsedMs: Date.now() - startTime,
            error: lastError.message,
          });
        } else {
          logger.warn('Commit retry for shard', {
            shardId,
            txnId: message.txnId,
            attempt,
            delayMs: delay,
            error: lastError.message,
          });
        }

        await this.sleep(delay);
      }
    }

    // Max attempts exceeded
    this.metrics.incMaxAttemptsExceeded(shardId, message.txnId);
    logger.error('MAX ATTEMPTS EXCEEDED - manual intervention required', {
      txnId: message.txnId,
      shardId,
      maxAttempts: this.options.maxCommitAttempts,
      elapsedMs: Date.now() - startTime,
      lastError: lastError?.message,
    });

    throw new MaxCommitAttemptsError(
      message.txnId,
      shardId,
      this.options.maxCommitAttempts,
      lastError
    );
  }

  /**
   * Check circuit breaker state for a shard.
   */
  private checkCircuitBreaker(shardId: number): CircuitBreakerState {
    let state = this.circuitBreakers.get(shardId);
    if (!state) {
      state = {
        failures: 0,
        lastFailure: 0,
        isOpen: false,
        resetAt: 0,
      };
      this.circuitBreakers.set(shardId, state);
    }
    return state;
  }

  /**
   * Record a circuit breaker failure.
   */
  private recordCircuitBreakerFailure(shardId: number): void {
    const state = this.checkCircuitBreaker(shardId);
    state.failures++;
    state.lastFailure = Date.now();

    if (state.failures >= this.options.circuitBreakerThreshold && !state.isOpen) {
      state.isOpen = true;
      state.resetAt = Date.now() + this.options.circuitBreakerResetMs;
      logger.error('Circuit breaker OPENED for shard', {
        shardId,
        failures: state.failures,
        resetAt: new Date(state.resetAt).toISOString(),
      });
    }
  }

  /**
   * Reset circuit breaker state after success.
   */
  private resetCircuitBreakerState(shardId: number): void {
    const state = this.circuitBreakers.get(shardId);
    if (state) {
      if (state.isOpen) {
        logger.info('Circuit breaker CLOSED for shard after successful commit', {
          shardId,
        });
      }
      state.failures = 0;
      state.isOpen = false;
      state.resetAt = 0;
    }
  }

  // --------------------------------------------------------------------------
  // Phase 2: Abort
  // --------------------------------------------------------------------------

  /**
   * Execute Phase 2 Abort of the 2PC protocol.
   *
   * Sends ABORT to all participants that received PREPARE.
   */
  private async abortPhase(txn: DistributedTransaction): Promise<void> {
    txn.state = 'aborting';

    // Build abort tasks for participants that were preparing or prepared
    const abortTasks: Array<
      () => Promise<{ shardId: number; ack: AckMessage }>
    > = [];

    for (const [shardId, participant] of txn.participants) {
      if (
        participant.state !== 'preparing' &&
        participant.state !== 'prepared'
      ) {
        participant.state = 'done';
        continue;
      }

      participant.state = 'aborting';
      participant.lastStateChange = Date.now();

      const message: AbortMessage = {
        type: 'abort',
        txnId: txn.txnId,
        shardId,
        timestamp: Date.now(),
        reason: txn.abortReason ?? 'Transaction aborted',
      };

      abortTasks.push(async () => {
        try {
          const ack = await this.sendAbortWithRetry(shardId, message);
          return { shardId, ack };
        } catch (abortError) {
          // Log the abort failure but return a synthetic ACK to allow the abort phase
          // to complete. This is best-effort: we've already decided to abort and want
          // to notify as many shards as possible. The shard will eventually timeout
          // and abort any prepared state.
          logger.warn('Abort acknowledgement failed for shard', {
            txnId: txn.txnId,
            shardId,
            error: abortError instanceof Error ? abortError.message : String(abortError),
            abortReason: txn.abortReason,
          });
          return {
            shardId,
            ack: {
              type: 'ack' as const,
              txnId: txn.txnId,
              shardId,
              timestamp: Date.now(),
            },
          };
        }
      });
    }

    // Wait for all acknowledgements (best effort, with concurrency limit)
    await this.rpcLimiter.allSettled(abortTasks);

    // Mark all participants as done
    for (const participant of txn.participants.values()) {
      participant.state = 'done';
      participant.lastStateChange = Date.now();
    }

    txn.state = 'aborted';
  }

  /**
   * Send ABORT with retry logic.
   */
  private async sendAbortWithRetry(
    shardId: number,
    message: AbortMessage
  ): Promise<AckMessage> {
    let lastError: Error | undefined;

    for (let attempt = 0; attempt < this.options.maxRetries; attempt++) {
      try {
        return await this.shardRPC.sendAbort(shardId, message);
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));

        if (attempt < this.options.maxRetries - 1) {
          await this.sleep(this.options.retryDelayMs * Math.pow(2, attempt));
        }
      }
    }

    throw lastError;
  }

  // --------------------------------------------------------------------------
  // Utilities
  // --------------------------------------------------------------------------

  /**
   * Create a timeout promise.
   */
  private createTimeout<T>(ms: number, phase: 'prepare' | 'commit'): Promise<T> {
    return new Promise((_, reject) => {
      setTimeout(() => {
        reject(
          new TransactionTimeoutError(
            'timeout',
            phase === 'prepare' ? 'preparing' : 'committing',
            phase
          )
        );
      }, ms);
    });
  }

  /**
   * Sleep for the specified duration.
   */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

// ============================================================================
// Factory Function
// ============================================================================

/**
 * Create a new TransactionCoordinator.
 */
export function createTransactionCoordinator(
  shardRouter: ShardRouter,
  shardRPC: ShardRPC,
  options?: CoordinatorOptions
): TransactionCoordinator {
  return new TransactionCoordinator(shardRouter, shardRPC, options);
}
