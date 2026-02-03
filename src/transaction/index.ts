/**
 * Transaction Module
 *
 * Provides MongoDB-style multi-document ACID transaction support.
 *
 * ## Architecture
 *
 * Transactions in MongoLake follow a snapshot isolation model:
 * - Reads see a consistent snapshot of data at transaction start time
 * - Writes are buffered until commit
 * - On commit, all writes are applied atomically
 * - On abort, all buffered writes are discarded
 *
 * ## Integration with Durable Objects
 *
 * When used with ShardDO:
 * - The TransactionManager tracks transaction state
 * - Writes are buffered in the session until commit
 * - On commit, writes are applied through the DO's atomic write path
 * - The DO's WAL ensures durability of committed transactions
 *
 * @example
 * ```typescript
 * const session = client.startSession();
 * const txnManager = new TransactionManager(session);
 *
 * try {
 *   txnManager.begin({ readConcern: { level: 'snapshot' } });
 *
 *   // Buffer writes
 *   txnManager.write({
 *     type: 'insert',
 *     collection: 'accounts',
 *     database: 'bank',
 *     document: { _id: 'acc1', balance: 1000 },
 *   });
 *
 *   await txnManager.commit();
 * } catch (error) {
 *   await txnManager.abort();
 *   throw error;
 * }
 * ```
 */

import {
  ClientSession,
  type TransactionOptions,
  type BufferedOperation,
  TransactionError,
} from '../session/index.js';
import type { Document } from '../types.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Write operation for transaction buffering.
 */
export interface TransactionWrite {
  type: 'insert' | 'update' | 'delete' | 'replace';
  collection: string;
  database: string;
  document?: Document;
  filter?: Document;
  update?: Document;
  replacement?: Document;
  options?: Record<string, unknown>;
}

/**
 * Snapshot information for read consistency.
 */
export interface TransactionSnapshot {
  /** Timestamp when transaction started (milliseconds since epoch) */
  startTime: number;
  /** LSN at transaction start for each shard */
  shardLSNs: Map<string, number>;
}

/**
 * Transaction commit result.
 */
export interface TransactionCommitResult {
  /** Whether commit succeeded */
  success: boolean;
  /** Number of operations committed */
  operationCount: number;
  /** Commit timestamp */
  commitTime: number;
}

/**
 * Options for running a transaction with retry logic.
 */
export interface RunTransactionOptions {
  /** Transaction options */
  transactionOptions?: TransactionOptions;
  /** Maximum number of retry attempts */
  maxRetries?: number;
  /** Delay between retries in milliseconds */
  retryDelayMs?: number;
}

// ============================================================================
// TransactionManager
// ============================================================================

/**
 * TransactionManager coordinates multi-document ACID transactions.
 *
 * Responsibilities:
 * - Track transaction state and snapshot
 * - Buffer write operations during transaction
 * - Coordinate atomic commit across shards
 * - Handle abort and cleanup
 */
export class TransactionManager {
  /** Associated session */
  private session: ClientSession;

  /** Snapshot for read consistency */
  private snapshot: TransactionSnapshot | null = null;

  /** Commit handlers by shard */
  private commitHandlers: Map<
    string,
    (operations: BufferedOperation[]) => Promise<void>
  > = new Map();

  constructor(session: ClientSession) {
    this.session = session;
  }

  // --------------------------------------------------------------------------
  // Transaction Lifecycle
  // --------------------------------------------------------------------------

  /**
   * Begin a new transaction.
   *
   * @param options - Transaction options
   * @throws TransactionError if transaction already in progress
   */
  begin(options?: TransactionOptions): void {
    this.session.startTransaction(options);

    // Capture snapshot for read consistency
    this.snapshot = {
      startTime: Date.now(),
      shardLSNs: new Map(),
    };
  }

  /**
   * Commit the current transaction.
   *
   * Applies all buffered operations atomically.
   *
   * @returns Commit result
   * @throws TransactionError if no transaction in progress or commit fails
   */
  async commit(): Promise<TransactionCommitResult> {
    if (!this.session.inTransaction) {
      throw new TransactionError('No transaction in progress');
    }

    const operations = this.session.getBufferedOperations();
    const operationCount = operations.length;
    const commitTime = Date.now();

    try {
      // Commit through session (which uses the client's commit handler)
      await this.session.commitTransaction();

      this.snapshot = null;

      return {
        success: true,
        operationCount,
        commitTime,
      };
    } catch (error) {
      // Keep transaction in progress for potential retry
      throw error;
    }
  }

  /**
   * Abort the current transaction.
   *
   * Discards all buffered operations.
   *
   * @throws TransactionError if no transaction in progress
   */
  async abort(): Promise<void> {
    await this.session.abortTransaction();
    this.snapshot = null;
  }

  // --------------------------------------------------------------------------
  // Write Operations
  // --------------------------------------------------------------------------

  /**
   * Buffer a write operation for the current transaction.
   *
   * @param write - Write operation to buffer
   * @throws TransactionError if no transaction in progress
   */
  write(write: TransactionWrite): void {
    this.session.bufferOperation({
      type: write.type,
      collection: write.collection,
      database: write.database,
      document: write.document,
      filter: write.filter,
      update: write.update,
      replacement: write.replacement,
      options: write.options,
    });
  }

  /**
   * Insert a document within the transaction.
   */
  insert(
    database: string,
    collection: string,
    document: Document
  ): void {
    this.write({
      type: 'insert',
      database,
      collection,
      document,
    });
  }

  /**
   * Update a document within the transaction.
   */
  update(
    database: string,
    collection: string,
    filter: Document,
    update: Document,
    options?: Record<string, unknown>
  ): void {
    this.write({
      type: 'update',
      database,
      collection,
      filter,
      update,
      options,
    });
  }

  /**
   * Replace a document within the transaction.
   */
  replace(
    database: string,
    collection: string,
    filter: Document,
    replacement: Document,
    options?: Record<string, unknown>
  ): void {
    this.write({
      type: 'replace',
      database,
      collection,
      filter,
      replacement,
      options,
    });
  }

  /**
   * Delete a document within the transaction.
   */
  delete(
    database: string,
    collection: string,
    filter: Document,
    options?: Record<string, unknown>
  ): void {
    this.write({
      type: 'delete',
      database,
      collection,
      filter,
      options,
    });
  }

  // --------------------------------------------------------------------------
  // Properties
  // --------------------------------------------------------------------------

  /**
   * Whether a transaction is currently in progress.
   */
  get inTransaction(): boolean {
    return this.session.inTransaction;
  }

  /**
   * Get the current snapshot information.
   */
  getSnapshot(): TransactionSnapshot | null {
    return this.snapshot;
  }

  /**
   * Get the number of buffered operations.
   */
  get operationCount(): number {
    return this.session.operationCount;
  }

  /**
   * Get all buffered operations.
   */
  getOperations(): BufferedOperation[] {
    return this.session.getBufferedOperations();
  }

  // --------------------------------------------------------------------------
  // Commit Handler Registration
  // --------------------------------------------------------------------------

  /**
   * Register a commit handler for a shard.
   *
   * @param shardId - Shard identifier
   * @param handler - Handler to apply operations atomically
   * @internal
   */
  registerCommitHandler(
    shardId: string,
    handler: (operations: BufferedOperation[]) => Promise<void>
  ): void {
    this.commitHandlers.set(shardId, handler);
  }

  /**
   * Record a shard's LSN in the snapshot.
   *
   * @param shardId - Shard identifier
   * @param lsn - Log Sequence Number
   * @internal
   */
  recordShardLSN(shardId: string, lsn: number): void {
    if (this.snapshot) {
      this.snapshot.shardLSNs.set(shardId, lsn);
    }
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Run a callback within a transaction with automatic retry logic.
 *
 * @param session - Client session
 * @param callback - Async callback to run within transaction
 * @param options - Run options
 * @returns Result of the callback
 *
 * @example
 * ```typescript
 * const result = await runTransaction(session, async (txn) => {
 *   txn.insert('bank', 'accounts', { _id: 'acc1', balance: 1000 });
 *   txn.update('bank', 'accounts', { _id: 'acc2' }, { $inc: { balance: -100 } });
 *   return 'success';
 * });
 * ```
 */
export async function runTransaction<T>(
  session: ClientSession,
  callback: (txn: TransactionManager) => Promise<T>,
  options: RunTransactionOptions = {}
): Promise<T> {
  const { transactionOptions, maxRetries = 3, retryDelayMs = 100 } = options;

  const txn = new TransactionManager(session);
  let lastError: Error | undefined;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      txn.begin(transactionOptions);

      const result = await callback(txn);

      await txn.commit();
      return result;
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));

      // Abort the transaction if still in progress
      if (txn.inTransaction) {
        try {
          await txn.abort();
        } catch {
          // Ignore abort errors during retry
        }
      }

      // Check if error is retryable
      const isRetryable = isTransientError(lastError);
      if (!isRetryable || attempt >= maxRetries) {
        throw lastError;
      }

      // Wait before retrying
      if (retryDelayMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, retryDelayMs));
      }
    }
  }

  throw lastError || new Error('Transaction failed');
}

/**
 * Check if an error is transient and can be retried.
 */
function isTransientError(error: Error): boolean {
  const message = error.message.toLowerCase();

  // Transient errors that can be retried
  const transientPatterns = [
    'write conflict',
    'lock timeout',
    'transaction too old',
    'stale',
    'retry',
    'temporary',
  ];

  return transientPatterns.some((pattern) => message.includes(pattern));
}

// ============================================================================
// Exports
// ============================================================================

export {
  ClientSession,
  TransactionError,
  type TransactionOptions,
  type BufferedOperation,
} from '../session/index.js';

// Cross-shard transaction coordinator
export {
  TransactionCoordinator,
  createTransactionCoordinator,
  DistributedTransactionError,
  TransactionTimeoutError,
  ParticipantAbortError,
  type TransactionId,
  type Participant,
  type ParticipantState,
  type CoordinatorState,
  type DistributedTransaction,
  type TwoPhaseCommitMessage,
  type TwoPhaseCommitMessageType,
  type PrepareMessage,
  type PreparedMessage,
  type AbortVoteMessage,
  type CommitMessage,
  type AbortMessage,
  type AckMessage,
  type StatusQueryMessage,
  type StatusResponseMessage,
  type AnyTwoPhaseCommitMessage,
  type CoordinatorOptions,
  type DistributedTransactionResult,
  type ShardRPC,
} from './coordinator.js';

// Cross-shard transaction participant
export {
  TransactionParticipant,
  createTransactionParticipant,
  LockManager,
  type PreparedTransaction,
  type DocumentLock,
  type ValidationResult,
  type ParticipantOptions,
  type ParticipantStorage,
  type OperationExecutor,
} from './participant.js';

// Transaction recovery
export {
  TransactionRecoveryManager,
  ParticipantRecoveryManager,
  InMemoryTransactionLogStorage,
  createTransactionRecoveryManager,
  createParticipantRecoveryManager,
  type TransactionLogEntry,
  type RecoveryAction,
  type RecoveryResult,
  type RecoveryOptions,
  type TransactionLogStorage,
  type CoordinatorDiscovery,
} from './recovery.js';
