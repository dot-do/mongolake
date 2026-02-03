/**
 * Transaction Recovery Module
 *
 * Provides recovery capabilities for distributed transactions after coordinator
 * or participant failures. This module ensures that in-doubt transactions
 * (those that have passed the prepare phase but haven't completed commit/abort)
 * can be properly resolved.
 *
 * ## Recovery Scenarios
 *
 * 1. **Coordinator Crash After Prepare Decision**
 *    - Decision was logged but commit/abort messages not fully sent
 *    - Recovery: Re-read decision from log and complete the protocol
 *
 * 2. **Coordinator Crash Before Prepare Decision**
 *    - No decision was made yet
 *    - Recovery: Abort the transaction (presumed abort protocol)
 *
 * 3. **Participant Crash While Prepared**
 *    - Participant has prepared state but coordinator may have moved on
 *    - Recovery: Query coordinator for decision, or timeout and abort
 *
 * 4. **Network Partition Resolution**
 *    - Participants couldn't reach coordinator during partition
 *    - Recovery: Re-establish communication and resolve pending transactions
 *
 * ## Recovery Protocol
 *
 * 1. On startup, scan for pending transactions:
 *    - Coordinator: Check transaction log for uncommitted decisions
 *    - Participant: Check prepared transaction store
 *
 * 2. For each pending transaction:
 *    - Determine the appropriate action (commit/abort)
 *    - Execute the action with retries
 *    - Clean up transaction state
 *
 * 3. Handle orphaned transactions:
 *    - Transactions that have exceeded timeout without resolution
 *    - Apply heuristic decisions based on configuration
 */

import type { ShardRPC, TransactionId, CoordinatorState, DistributedTransaction } from './coordinator.js';
import type { PreparedTransaction, ParticipantStorage } from './participant.js';
import { logger } from '../utils/logger.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Persisted transaction record for coordinator recovery.
 */
export interface TransactionLogEntry {
  /** Transaction ID */
  txnId: TransactionId;
  /** Coordinator state at time of logging */
  state: CoordinatorState;
  /** Decision (if made) */
  decision?: 'commit' | 'abort';
  /** Participating shard IDs */
  participantShardIds: number[];
  /** Timestamp when logged */
  loggedAt: number;
  /** Timestamp when decision was made (if any) */
  decisionAt?: number;
  /** Abort reason (if aborted) */
  abortReason?: string;
}

/**
 * Recovery action to take for a pending transaction.
 */
export type RecoveryAction =
  | { type: 'commit'; txnId: TransactionId; shardIds: number[] }
  | { type: 'abort'; txnId: TransactionId; shardIds: number[]; reason: string }
  | { type: 'query'; txnId: TransactionId; shardIds: number[] }
  | { type: 'heuristic_commit'; txnId: TransactionId; shardIds: number[] }
  | { type: 'heuristic_abort'; txnId: TransactionId; shardIds: number[]; reason: string };

/**
 * Result of a recovery operation.
 */
export interface RecoveryResult {
  /** Transaction ID */
  txnId: TransactionId;
  /** Whether recovery succeeded */
  success: boolean;
  /** Action taken */
  action: RecoveryAction['type'];
  /** Error if recovery failed */
  error?: string;
  /** Shards that completed successfully */
  completedShards: number[];
  /** Shards that failed */
  failedShards: number[];
}

/**
 * Options for transaction recovery.
 */
export interface RecoveryOptions {
  /** Maximum age of transactions to recover (milliseconds) */
  maxTransactionAgeMs?: number;
  /** Timeout for individual recovery operations (milliseconds) */
  operationTimeoutMs?: number;
  /** Maximum retries for recovery operations */
  maxRetries?: number;
  /** Whether to apply heuristic decisions for orphaned transactions */
  enableHeuristicDecisions?: boolean;
  /** Default heuristic action when decision cannot be determined */
  defaultHeuristicAction?: 'commit' | 'abort';
  /** Threshold age (ms) before applying heuristic decision */
  heuristicThresholdMs?: number;
}

/**
 * Storage interface for coordinator transaction log.
 */
export interface TransactionLogStorage {
  /**
   * Write a transaction log entry.
   */
  writeLogEntry(entry: TransactionLogEntry): Promise<void>;

  /**
   * Read a transaction log entry by ID.
   */
  readLogEntry(txnId: TransactionId): Promise<TransactionLogEntry | null>;

  /**
   * Delete a transaction log entry.
   */
  deleteLogEntry(txnId: TransactionId): Promise<void>;

  /**
   * List all pending (non-completed) transaction log entries.
   */
  listPendingEntries(): Promise<TransactionLogEntry[]>;

  /**
   * Update a transaction log entry.
   */
  updateLogEntry(txnId: TransactionId, update: Partial<TransactionLogEntry>): Promise<void>;
}

/**
 * Coordinator discovery interface for participant recovery.
 */
export interface CoordinatorDiscovery {
  /**
   * Query the coordinator for transaction decision.
   * Returns the decision or null if coordinator doesn't know about the transaction.
   */
  queryDecision(txnId: TransactionId): Promise<'commit' | 'abort' | null>;

  /**
   * Check if coordinator is available.
   */
  isCoordinatorAvailable(): Promise<boolean>;
}

// ============================================================================
// Transaction Recovery Manager
// ============================================================================

/**
 * TransactionRecoveryManager handles recovery of in-doubt transactions
 * for the coordinator side.
 */
export class TransactionRecoveryManager {
  /** Default options */
  private readonly options: Required<RecoveryOptions>;

  constructor(
    private readonly logStorage: TransactionLogStorage,
    private readonly shardRPC: ShardRPC,
    options: RecoveryOptions = {}
  ) {
    this.options = {
      maxTransactionAgeMs: options.maxTransactionAgeMs ?? 24 * 60 * 60 * 1000, // 24 hours
      operationTimeoutMs: options.operationTimeoutMs ?? 30000, // 30 seconds
      maxRetries: options.maxRetries ?? 10,
      enableHeuristicDecisions: options.enableHeuristicDecisions ?? false,
      defaultHeuristicAction: options.defaultHeuristicAction ?? 'abort',
      heuristicThresholdMs: options.heuristicThresholdMs ?? 60 * 60 * 1000, // 1 hour
    };
  }

  /**
   * Run recovery for all pending transactions.
   * Should be called on coordinator startup.
   */
  async recoverPendingTransactions(): Promise<RecoveryResult[]> {
    const results: RecoveryResult[] = [];

    try {
      const pendingEntries = await this.logStorage.listPendingEntries();

      logger.info('Starting transaction recovery', {
        pendingCount: pendingEntries.length,
      });

      for (const entry of pendingEntries) {
        const result = await this.recoverTransaction(entry);
        results.push(result);
      }

      const successful = results.filter(r => r.success).length;
      const failed = results.length - successful;

      logger.info('Transaction recovery completed', {
        total: results.length,
        successful,
        failed,
      });

      return results;
    } catch (error) {
      logger.error('Transaction recovery failed', {
        error: error instanceof Error ? error.message : String(error),
      });
      throw error;
    }
  }

  /**
   * Recover a single transaction.
   */
  async recoverTransaction(entry: TransactionLogEntry): Promise<RecoveryResult> {
    const { txnId, state, decision, participantShardIds } = entry;

    logger.info('Recovering transaction', {
      txnId,
      state,
      decision,
      participantCount: participantShardIds.length,
    });

    // Determine recovery action based on state
    const action = this.determineRecoveryAction(entry);

    // Execute recovery action
    return this.executeRecoveryAction(action);
  }

  /**
   * Determine the appropriate recovery action for a transaction.
   */
  private determineRecoveryAction(entry: TransactionLogEntry): RecoveryAction {
    const { txnId, state, decision, participantShardIds, loggedAt, abortReason } = entry;
    const age = Date.now() - loggedAt;

    // If decision was already made, execute it
    if (decision) {
      if (decision === 'commit') {
        return { type: 'commit', txnId, shardIds: participantShardIds };
      } else {
        return {
          type: 'abort',
          txnId,
          shardIds: participantShardIds,
          reason: abortReason ?? 'Recovery: previous abort decision',
        };
      }
    }

    // Check if transaction is too old
    if (age > this.options.maxTransactionAgeMs) {
      logger.warn('Transaction exceeded max age, will be aborted', {
        txnId,
        ageMs: age,
        maxAgeMs: this.options.maxTransactionAgeMs,
      });
      return {
        type: 'abort',
        txnId,
        shardIds: participantShardIds,
        reason: `Recovery: transaction exceeded max age (${age}ms)`,
      };
    }

    // Determine action based on state
    switch (state) {
      case 'initialized':
      case 'preparing':
        // No decision made yet, safe to abort (presumed abort)
        return {
          type: 'abort',
          txnId,
          shardIds: participantShardIds,
          reason: 'Recovery: coordinator crashed before decision',
        };

      case 'prepared':
        // All participants prepared, but decision not logged
        // This is an edge case - should not happen if decision logging is atomic
        // Apply heuristic if enabled and threshold met
        if (this.options.enableHeuristicDecisions && age > this.options.heuristicThresholdMs) {
          if (this.options.defaultHeuristicAction === 'commit') {
            return { type: 'heuristic_commit', txnId, shardIds: participantShardIds };
          } else {
            return {
              type: 'heuristic_abort',
              txnId,
              shardIds: participantShardIds,
              reason: 'Recovery: heuristic abort after prepared state timeout',
            };
          }
        }
        // Default to abort for safety
        return {
          type: 'abort',
          txnId,
          shardIds: participantShardIds,
          reason: 'Recovery: prepared but no decision logged',
        };

      case 'committing':
        // Decision was commit, but not all shards acknowledged
        return { type: 'commit', txnId, shardIds: participantShardIds };

      case 'aborting':
        // Decision was abort, but not all shards acknowledged
        return {
          type: 'abort',
          txnId,
          shardIds: participantShardIds,
          reason: abortReason ?? 'Recovery: completing abort',
        };

      case 'committed':
      case 'aborted':
        // Transaction already completed, just clean up log entry
        // This shouldn't happen if log entries are properly cleaned
        logger.warn('Found completed transaction in pending log', {
          txnId,
          state,
        });
        return {
          type: state === 'committed' ? 'commit' : 'abort',
          txnId,
          shardIds: [],
          reason: abortReason ?? 'Already completed',
        };

      default:
        // Unknown state - abort for safety
        return {
          type: 'abort',
          txnId,
          shardIds: participantShardIds,
          reason: `Recovery: unknown state ${state}`,
        };
    }
  }

  /**
   * Execute a recovery action.
   */
  private async executeRecoveryAction(action: RecoveryAction): Promise<RecoveryResult> {
    const completedShards: number[] = [];
    const failedShards: number[] = [];
    let success = true;
    let error: string | undefined;

    try {
      switch (action.type) {
        case 'commit':
        case 'heuristic_commit':
          for (const shardId of action.shardIds) {
            try {
              await this.sendCommitWithRetry(action.txnId, shardId);
              completedShards.push(shardId);
            } catch (err) {
              failedShards.push(shardId);
              success = false;
              error = err instanceof Error ? err.message : String(err);
              logger.error('Recovery commit failed for shard', {
                txnId: action.txnId,
                shardId,
                error: err,
              });
            }
          }

          // Update log entry if all succeeded
          if (success) {
            await this.logStorage.updateLogEntry(action.txnId, {
              state: 'committed',
              decision: 'commit',
            });
            await this.logStorage.deleteLogEntry(action.txnId);
          }
          break;

        case 'abort':
        case 'heuristic_abort':
          for (const shardId of action.shardIds) {
            try {
              await this.sendAbortWithRetry(action.txnId, shardId, action.reason);
              completedShards.push(shardId);
            } catch (err) {
              failedShards.push(shardId);
              // Continue trying other shards even if one fails
              logger.error('Recovery abort failed for shard', {
                txnId: action.txnId,
                shardId,
                error: err,
              });
            }
          }

          // Update log entry
          await this.logStorage.updateLogEntry(action.txnId, {
            state: 'aborted',
            decision: 'abort',
            abortReason: action.reason,
          });
          await this.logStorage.deleteLogEntry(action.txnId);
          success = true; // Abort is best-effort
          break;

        case 'query':
          // Query each shard for their state
          // This would be used for determining global state
          logger.info('Query action not implemented in recovery', {
            txnId: action.txnId,
          });
          break;
      }
    } catch (err) {
      success = false;
      error = err instanceof Error ? err.message : String(err);
      logger.error('Recovery action failed', {
        action: action.type,
        txnId: action.txnId,
        error,
      });
    }

    return {
      txnId: action.txnId,
      success,
      action: action.type,
      error,
      completedShards,
      failedShards,
    };
  }

  /**
   * Send commit with retry.
   */
  private async sendCommitWithRetry(txnId: TransactionId, shardId: number): Promise<void> {
    let lastError: Error | undefined;

    for (let attempt = 0; attempt < this.options.maxRetries; attempt++) {
      try {
        await this.shardRPC.sendCommit(shardId, {
          type: 'commit',
          txnId,
          shardId,
          timestamp: Date.now(),
          commitDeadline: Date.now() + this.options.operationTimeoutMs,
        });
        return;
      } catch (err) {
        lastError = err instanceof Error ? err : new Error(String(err));
        await this.sleep(Math.min(1000 * Math.pow(2, attempt), 30000));
      }
    }

    throw lastError ?? new Error('Max retries exceeded');
  }

  /**
   * Send abort with retry.
   */
  private async sendAbortWithRetry(
    txnId: TransactionId,
    shardId: number,
    reason: string
  ): Promise<void> {
    let lastError: Error | undefined;

    for (let attempt = 0; attempt < this.options.maxRetries; attempt++) {
      try {
        await this.shardRPC.sendAbort(shardId, {
          type: 'abort',
          txnId,
          shardId,
          timestamp: Date.now(),
          reason,
        });
        return;
      } catch (err) {
        lastError = err instanceof Error ? err : new Error(String(err));
        await this.sleep(Math.min(1000 * Math.pow(2, attempt), 30000));
      }
    }

    throw lastError ?? new Error('Max retries exceeded');
  }

  /**
   * Log a transaction for recovery.
   * Should be called before starting prepare phase.
   */
  async logTransaction(txn: DistributedTransaction): Promise<void> {
    const entry: TransactionLogEntry = {
      txnId: txn.txnId,
      state: txn.state,
      decision: txn.decision,
      participantShardIds: Array.from(txn.participants.keys()),
      loggedAt: Date.now(),
      decisionAt: txn.decision ? Date.now() : undefined,
      abortReason: txn.abortReason,
    };

    await this.logStorage.writeLogEntry(entry);
  }

  /**
   * Update transaction decision in log.
   * Should be called after prepare phase completes.
   */
  async logDecision(
    txnId: TransactionId,
    decision: 'commit' | 'abort',
    abortReason?: string
  ): Promise<void> {
    await this.logStorage.updateLogEntry(txnId, {
      decision,
      decisionAt: Date.now(),
      state: decision === 'commit' ? 'committing' : 'aborting',
      abortReason,
    });
  }

  /**
   * Complete transaction in log.
   * Should be called after all participants acknowledge.
   */
  async completeTransaction(txnId: TransactionId): Promise<void> {
    await this.logStorage.deleteLogEntry(txnId);
  }

  /**
   * Sleep for specified milliseconds.
   */
  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

// ============================================================================
// Participant Recovery Manager
// ============================================================================

/**
 * ParticipantRecoveryManager handles recovery of prepared transactions
 * on the participant side.
 */
export class ParticipantRecoveryManager {
  /** Default options */
  private readonly options: Required<RecoveryOptions>;

  constructor(
    private readonly shardId: number,
    private readonly storage: ParticipantStorage,
    private readonly coordinatorDiscovery: CoordinatorDiscovery,
    private readonly onCommit: (txnId: TransactionId) => Promise<void>,
    private readonly onAbort: (txnId: TransactionId) => Promise<void>,
    options: RecoveryOptions = {}
  ) {
    this.options = {
      maxTransactionAgeMs: options.maxTransactionAgeMs ?? 24 * 60 * 60 * 1000,
      operationTimeoutMs: options.operationTimeoutMs ?? 30000,
      maxRetries: options.maxRetries ?? 10,
      enableHeuristicDecisions: options.enableHeuristicDecisions ?? false,
      defaultHeuristicAction: options.defaultHeuristicAction ?? 'abort',
      heuristicThresholdMs: options.heuristicThresholdMs ?? 60 * 60 * 1000,
    };
  }

  /**
   * Recover all prepared transactions.
   * Should be called on participant startup.
   */
  async recoverPreparedTransactions(): Promise<RecoveryResult[]> {
    const results: RecoveryResult[] = [];

    try {
      const preparedTxns = await this.storage.loadAllPreparedTransactions();

      logger.info('Starting participant recovery', {
        shardId: this.shardId,
        preparedCount: preparedTxns.length,
      });

      for (const txn of preparedTxns) {
        const result = await this.recoverPreparedTransaction(txn);
        results.push(result);
      }

      const successful = results.filter(r => r.success).length;
      const failed = results.length - successful;

      logger.info('Participant recovery completed', {
        shardId: this.shardId,
        total: results.length,
        successful,
        failed,
      });

      return results;
    } catch (error) {
      logger.error('Participant recovery failed', {
        shardId: this.shardId,
        error: error instanceof Error ? error.message : String(error),
      });
      throw error;
    }
  }

  /**
   * Recover a single prepared transaction.
   */
  async recoverPreparedTransaction(txn: PreparedTransaction): Promise<RecoveryResult> {
    const { txnId, preparedAt } = txn;
    const age = Date.now() - preparedAt;

    logger.info('Recovering prepared transaction', {
      shardId: this.shardId,
      txnId,
      ageMs: age,
    });

    // Check if transaction is too old
    if (age > this.options.maxTransactionAgeMs) {
      logger.warn('Prepared transaction exceeded max age', {
        shardId: this.shardId,
        txnId,
        ageMs: age,
      });

      try {
        await this.onAbort(txnId);
        return {
          txnId,
          success: true,
          action: 'abort',
          completedShards: [this.shardId],
          failedShards: [],
        };
      } catch (err) {
        return {
          txnId,
          success: false,
          action: 'abort',
          error: err instanceof Error ? err.message : String(err),
          completedShards: [],
          failedShards: [this.shardId],
        };
      }
    }

    // Try to query coordinator for decision
    try {
      const isAvailable = await this.coordinatorDiscovery.isCoordinatorAvailable();
      if (isAvailable) {
        const decision = await this.coordinatorDiscovery.queryDecision(txnId);

        if (decision === 'commit') {
          await this.onCommit(txnId);
          return {
            txnId,
            success: true,
            action: 'commit',
            completedShards: [this.shardId],
            failedShards: [],
          };
        } else if (decision === 'abort') {
          await this.onAbort(txnId);
          return {
            txnId,
            success: true,
            action: 'abort',
            completedShards: [this.shardId],
            failedShards: [],
          };
        }

        // Coordinator doesn't know about this transaction
        // This could mean coordinator crashed before logging, so we abort
        if (decision === null) {
          logger.info('Coordinator has no record of transaction, aborting', {
            shardId: this.shardId,
            txnId,
          });
          await this.onAbort(txnId);
          return {
            txnId,
            success: true,
            action: 'abort',
            completedShards: [this.shardId],
            failedShards: [],
          };
        }
      }
    } catch (err) {
      logger.warn('Failed to query coordinator for decision', {
        shardId: this.shardId,
        txnId,
        error: err instanceof Error ? err.message : String(err),
      });
    }

    // Check if we should apply heuristic decision
    if (this.options.enableHeuristicDecisions && age > this.options.heuristicThresholdMs) {
      const action = this.options.defaultHeuristicAction;
      logger.warn('Applying heuristic decision for prepared transaction', {
        shardId: this.shardId,
        txnId,
        action,
        ageMs: age,
      });

      try {
        if (action === 'commit') {
          await this.onCommit(txnId);
        } else {
          await this.onAbort(txnId);
        }

        return {
          txnId,
          success: true,
          action: action === 'commit' ? 'heuristic_commit' : 'heuristic_abort',
          completedShards: [this.shardId],
          failedShards: [],
        };
      } catch (err) {
        return {
          txnId,
          success: false,
          action: action === 'commit' ? 'heuristic_commit' : 'heuristic_abort',
          error: err instanceof Error ? err.message : String(err),
          completedShards: [],
          failedShards: [this.shardId],
        };
      }
    }

    // Transaction is still within timeout, leave it prepared
    // It will be resolved when coordinator comes back or times out
    logger.info('Leaving transaction in prepared state', {
      shardId: this.shardId,
      txnId,
      ageMs: age,
      thresholdMs: this.options.heuristicThresholdMs,
    });

    return {
      txnId,
      success: true,
      action: 'query',
      completedShards: [],
      failedShards: [],
    };
  }
}

// ============================================================================
// In-Memory Transaction Log Storage (for testing)
// ============================================================================

/**
 * In-memory implementation of TransactionLogStorage for testing.
 */
export class InMemoryTransactionLogStorage implements TransactionLogStorage {
  private entries: Map<TransactionId, TransactionLogEntry> = new Map();

  async writeLogEntry(entry: TransactionLogEntry): Promise<void> {
    this.entries.set(entry.txnId, { ...entry });
  }

  async readLogEntry(txnId: TransactionId): Promise<TransactionLogEntry | null> {
    const entry = this.entries.get(txnId);
    return entry ? { ...entry } : null;
  }

  async deleteLogEntry(txnId: TransactionId): Promise<void> {
    this.entries.delete(txnId);
  }

  async listPendingEntries(): Promise<TransactionLogEntry[]> {
    const pending: TransactionLogEntry[] = [];
    for (const entry of this.entries.values()) {
      if (entry.state !== 'committed' && entry.state !== 'aborted') {
        pending.push({ ...entry });
      }
    }
    return pending;
  }

  async updateLogEntry(
    txnId: TransactionId,
    update: Partial<TransactionLogEntry>
  ): Promise<void> {
    const entry = this.entries.get(txnId);
    if (entry) {
      this.entries.set(txnId, { ...entry, ...update });
    }
  }

  // Test helper methods
  getEntryCount(): number {
    return this.entries.size;
  }

  clear(): void {
    this.entries.clear();
  }
}

// ============================================================================
// Factory Functions
// ============================================================================

/**
 * Create a TransactionRecoveryManager for coordinator recovery.
 */
export function createTransactionRecoveryManager(
  logStorage: TransactionLogStorage,
  shardRPC: ShardRPC,
  options?: RecoveryOptions
): TransactionRecoveryManager {
  return new TransactionRecoveryManager(logStorage, shardRPC, options);
}

/**
 * Create a ParticipantRecoveryManager for participant recovery.
 */
export function createParticipantRecoveryManager(
  shardId: number,
  storage: ParticipantStorage,
  coordinatorDiscovery: CoordinatorDiscovery,
  onCommit: (txnId: TransactionId) => Promise<void>,
  onAbort: (txnId: TransactionId) => Promise<void>,
  options?: RecoveryOptions
): ParticipantRecoveryManager {
  return new ParticipantRecoveryManager(
    shardId,
    storage,
    coordinatorDiscovery,
    onCommit,
    onAbort,
    options
  );
}
