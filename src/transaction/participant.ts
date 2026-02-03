/**
 * Two-Phase Commit Participant
 *
 * Implements the participant (shard) side of the 2PC protocol.
 * Each ShardDO acts as a participant in distributed transactions.
 *
 * ## Responsibilities
 *
 * - Receive PREPARE messages from coordinator
 * - Validate operations and acquire locks
 * - Persist prepared state for durability
 * - Execute COMMIT or ABORT based on coordinator decision
 * - Handle timeout-based abort for coordinator failures
 *
 * ## State Machine
 *
 * ```
 *              PREPARE
 * [idle] ----------------> [preparing]
 *                              |
 *          +------------------+------------------+
 *          | (success)                           | (failure)
 *          v                                     v
 *    [prepared] <-- PREPARED              [aborted] <-- ABORT_VOTE
 *          |                                     |
 *    +-----+-----+                               |
 *    | COMMIT    | ABORT                         |
 *    v           v                               |
 * [committed] [aborted] <------------------------+
 * ```
 *
 * ## Durability
 *
 * Before responding PREPARED, the participant must:
 * 1. Validate all operations
 * 2. Acquire locks on affected documents
 * 3. Persist prepared state to SQLite
 *
 * This ensures that after a crash, the participant can:
 * - Resume waiting for coordinator decision
 * - Apply commit if coordinator sends COMMIT
 * - Abort if coordinator sends ABORT or times out
 */

import type { BufferedOperation } from '../session/index.js';
import { logger } from '../utils/logger.js';
import type {
  TransactionId,
  PrepareMessage,
  PreparedMessage,
  AbortVoteMessage,
  CommitMessage,
  AbortMessage,
  AckMessage,
  StatusQueryMessage,
  StatusResponseMessage,
  ParticipantState,
} from './coordinator.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Prepared transaction record persisted to SQLite.
 */
export interface PreparedTransaction {
  /** Transaction ID */
  txnId: TransactionId;
  /** Participant state */
  state: ParticipantState;
  /** Operations to apply on commit */
  operations: BufferedOperation[];
  /** LSN at which operations are prepared */
  preparedLSN: number;
  /** Prepare deadline */
  prepareDeadline: number;
  /** Timestamp when prepared */
  preparedAt: number;
}

/**
 * Document lock record.
 */
export interface DocumentLock {
  /** Document ID */
  documentId: string;
  /** Collection name */
  collection: string;
  /** Database name */
  database: string;
  /** Transaction holding the lock */
  txnId: TransactionId;
  /** Lock type */
  lockType: 'read' | 'write';
  /** When the lock was acquired */
  acquiredAt: number;
  /** Lock expiration time */
  expiresAt: number;
}

/**
 * Validation result for prepare phase.
 */
export interface ValidationResult {
  /** Whether validation passed */
  valid: boolean;
  /** Validation errors if failed */
  errors?: string[];
}

/**
 * Participant options.
 */
export interface ParticipantOptions {
  /** Lock timeout in milliseconds (default: 5000) */
  lockTimeoutMs?: number;
  /** Prepared transaction timeout in milliseconds (default: 30000) */
  preparedTimeoutMs?: number;
}

/**
 * Storage interface for participant state persistence.
 */
export interface ParticipantStorage {
  /**
   * Persist a prepared transaction.
   */
  savePreparedTransaction(txn: PreparedTransaction): Promise<void>;

  /**
   * Load a prepared transaction by ID.
   */
  loadPreparedTransaction(txnId: TransactionId): Promise<PreparedTransaction | null>;

  /**
   * Delete a prepared transaction.
   */
  deletePreparedTransaction(txnId: TransactionId): Promise<void>;

  /**
   * Load all prepared transactions (for recovery).
   */
  loadAllPreparedTransactions(): Promise<PreparedTransaction[]>;

  /**
   * Allocate the next LSN.
   */
  allocateLSN(): number;

  /**
   * Get current LSN.
   */
  getCurrentLSN(): number;
}

/**
 * Operation executor interface.
 */
export interface OperationExecutor {
  /**
   * Validate operations without applying them.
   */
  validateOperations(operations: BufferedOperation[]): Promise<ValidationResult>;

  /**
   * Apply operations atomically.
   */
  applyOperations(operations: BufferedOperation[]): Promise<number>;
}

// ============================================================================
// Lock Manager
// ============================================================================

/**
 * LockManager handles document-level locking for transaction isolation.
 *
 * Locks are held during the prepared state to prevent conflicting
 * modifications from other transactions.
 */
export class LockManager {
  /** Active locks by document key */
  private locks: Map<string, DocumentLock> = new Map();

  /** Locks by transaction ID */
  private locksByTxn: Map<TransactionId, Set<string>> = new Map();

  constructor(
    private readonly lockTimeoutMs: number = 5000
  ) {}

  /**
   * Generate a unique key for a document.
   */
  private getDocumentKey(database: string, collection: string, documentId: string): string {
    return `${database}:${collection}:${documentId}`;
  }

  /**
   * Acquire locks for all documents affected by operations.
   *
   * @param txnId - Transaction ID
   * @param operations - Operations requiring locks
   * @returns List of errors if locks could not be acquired
   */
  acquireLocks(
    txnId: TransactionId,
    operations: BufferedOperation[]
  ): { success: boolean; errors: string[] } {
    const errors: string[] = [];
    const keysToLock: string[] = [];
    const now = Date.now();

    // Clean up expired locks first
    this.cleanupExpiredLocks();

    // Collect all document keys that need to be locked
    for (const op of operations) {
      const documentId = this.getDocumentIdFromOperation(op);
      if (!documentId) continue;

      const key = this.getDocumentKey(op.database, op.collection, documentId);
      keysToLock.push(key);

      // Check if lock is already held by another transaction
      const existingLock = this.locks.get(key);
      if (existingLock && existingLock.txnId !== txnId) {
        errors.push(
          `Document ${documentId} in ${op.database}.${op.collection} is locked by transaction ${existingLock.txnId}`
        );
      }
    }

    if (errors.length > 0) {
      return { success: false, errors };
    }

    // Acquire all locks
    if (!this.locksByTxn.has(txnId)) {
      this.locksByTxn.set(txnId, new Set());
    }
    const txnLocks = this.locksByTxn.get(txnId)!;

    for (const key of keysToLock) {
      // Skip if already locked by this transaction
      if (txnLocks.has(key)) continue;

      const [database, collection, documentId] = key.split(':') as [string, string, string];
      const lock: DocumentLock = {
        documentId,
        collection,
        database,
        txnId,
        lockType: 'write',
        acquiredAt: now,
        expiresAt: now + this.lockTimeoutMs,
      };

      this.locks.set(key, lock);
      txnLocks.add(key);
    }

    return { success: true, errors: [] };
  }

  /**
   * Release all locks held by a transaction.
   */
  releaseLocks(txnId: TransactionId): void {
    const txnLocks = this.locksByTxn.get(txnId);
    if (!txnLocks) return;

    for (const key of txnLocks) {
      const lock = this.locks.get(key);
      if (lock && lock.txnId === txnId) {
        this.locks.delete(key);
      }
    }

    this.locksByTxn.delete(txnId);
  }

  /**
   * Extend lock timeout for a transaction.
   */
  extendLocks(txnId: TransactionId, additionalMs: number): void {
    const txnLocks = this.locksByTxn.get(txnId);
    if (!txnLocks) return;

    const now = Date.now();
    for (const key of txnLocks) {
      const lock = this.locks.get(key);
      if (lock && lock.txnId === txnId) {
        lock.expiresAt = now + additionalMs;
      }
    }
  }

  /**
   * Clean up expired locks.
   */
  private cleanupExpiredLocks(): void {
    const now = Date.now();
    const expiredTxns = new Set<TransactionId>();

    for (const [key, lock] of this.locks) {
      if (lock.expiresAt < now) {
        this.locks.delete(key);
        expiredTxns.add(lock.txnId);
      }
    }

    // Clean up transaction lock sets
    for (const txnId of expiredTxns) {
      const txnLocks = this.locksByTxn.get(txnId);
      if (txnLocks) {
        for (const key of txnLocks) {
          if (!this.locks.has(key)) {
            txnLocks.delete(key);
          }
        }
        if (txnLocks.size === 0) {
          this.locksByTxn.delete(txnId);
        }
      }
    }
  }

  /**
   * Extract document ID from an operation.
   */
  private getDocumentIdFromOperation(op: BufferedOperation): string | null {
    if (op.document?._id) {
      return String(op.document._id);
    }
    if (op.filter?._id) {
      return String(op.filter._id);
    }
    return null;
  }

  /**
   * Get lock statistics.
   */
  getStats(): { totalLocks: number; transactionCount: number } {
    return {
      totalLocks: this.locks.size,
      transactionCount: this.locksByTxn.size,
    };
  }
}

// ============================================================================
// Transaction Participant
// ============================================================================

/**
 * TransactionParticipant handles the shard-side 2PC protocol.
 */
export class TransactionParticipant {
  /** Lock manager for document-level locking */
  private readonly lockManager: LockManager;

  /** Prepared transactions waiting for commit/abort */
  private readonly preparedTransactions: Map<TransactionId, PreparedTransaction> = new Map();

  /**
   * Aborted transaction IDs for idempotency.
   * Tracks transactions that were aborted so we can reject re-prepare attempts.
   */
  private readonly abortedTransactions: Set<TransactionId> = new Set();

  /** Configuration options */
  private readonly options: Required<ParticipantOptions>;

  /** Shard ID */
  private readonly shardId: number;

  constructor(
    shardId: number,
    private readonly storage: ParticipantStorage,
    private readonly executor: OperationExecutor,
    options: ParticipantOptions = {}
  ) {
    this.shardId = shardId;
    this.options = {
      lockTimeoutMs: options.lockTimeoutMs ?? 5000,
      preparedTimeoutMs: options.preparedTimeoutMs ?? 30000,
    };
    this.lockManager = new LockManager(this.options.lockTimeoutMs);
  }

  // --------------------------------------------------------------------------
  // Recovery
  // --------------------------------------------------------------------------

  /**
   * Recover prepared transactions after restart.
   *
   * Should be called during shard initialization.
   */
  async recover(): Promise<void> {
    const preparedTxns = await this.storage.loadAllPreparedTransactions();

    for (const txn of preparedTxns) {
      // Check if transaction has expired
      if (Date.now() > txn.preparedAt + this.options.preparedTimeoutMs) {
        // Transaction timed out - abort it
        await this.abortPreparedTransaction(txn.txnId);
      } else {
        // Transaction still valid - restore it
        this.preparedTransactions.set(txn.txnId, txn);

        // Re-acquire locks
        const lockResult = this.lockManager.acquireLocks(txn.txnId, txn.operations);
        if (!lockResult.success) {
          // Cannot re-acquire locks - abort
          logger.error('Failed to re-acquire locks during recovery', {
            shardId: this.shardId,
            txnId: txn.txnId,
            operation: 'recover',
            errors: lockResult.errors,
          });
          await this.abortPreparedTransaction(txn.txnId);
        }
      }
    }
  }

  // --------------------------------------------------------------------------
  // Message Handlers
  // --------------------------------------------------------------------------

  /**
   * Handle PREPARE message from coordinator.
   */
  async handlePrepare(
    message: PrepareMessage
  ): Promise<PreparedMessage | AbortVoteMessage> {
    const { txnId, operations, prepareDeadline } = message;

    // Check if transaction was already aborted (idempotency)
    if (this.abortedTransactions.has(txnId)) {
      return this.createAbortVoteMessage(txnId, 'Transaction already aborted');
    }

    // Check if already prepared (idempotency)
    const existing = this.preparedTransactions.get(txnId);
    if (existing) {
      if (existing.state === 'prepared') {
        return this.createPreparedMessage(txnId, existing.preparedLSN);
      }
      return this.createAbortVoteMessage(txnId, 'Transaction already aborted');
    }

    // Check deadline
    if (Date.now() > prepareDeadline) {
      return this.createAbortVoteMessage(txnId, 'Prepare deadline exceeded');
    }

    // Validate operations
    const validationResult = await this.executor.validateOperations(operations);
    if (!validationResult.valid) {
      return this.createAbortVoteMessage(
        txnId,
        `Validation failed: ${validationResult.errors?.join(', ')}`
      );
    }

    // Acquire locks
    const lockResult = this.lockManager.acquireLocks(txnId, operations);
    if (!lockResult.success) {
      return this.createAbortVoteMessage(
        txnId,
        `Lock acquisition failed: ${lockResult.errors.join(', ')}`
      );
    }

    // Allocate LSN for prepared transaction
    const preparedLSN = this.storage.allocateLSN();

    // Create prepared transaction record
    const preparedTxn: PreparedTransaction = {
      txnId,
      state: 'prepared',
      operations,
      preparedLSN,
      prepareDeadline,
      preparedAt: Date.now(),
    };

    // Persist prepared state (before responding)
    await this.storage.savePreparedTransaction(preparedTxn);

    // Store in memory
    this.preparedTransactions.set(txnId, preparedTxn);

    return this.createPreparedMessage(txnId, preparedLSN);
  }

  /**
   * Handle COMMIT message from coordinator.
   */
  async handleCommit(message: CommitMessage): Promise<AckMessage> {
    const { txnId } = message;

    // Get prepared transaction
    const preparedTxn = this.preparedTransactions.get(txnId);
    if (!preparedTxn) {
      // Transaction not found - might have been committed already
      // Return ACK anyway for idempotency
      return this.createAckMessage(txnId, this.storage.getCurrentLSN());
    }

    try {
      // Apply operations
      const finalLSN = await this.executor.applyOperations(preparedTxn.operations);

      // Clean up
      this.lockManager.releaseLocks(txnId);
      this.preparedTransactions.delete(txnId);
      await this.storage.deletePreparedTransaction(txnId);

      return this.createAckMessage(txnId, finalLSN);
    } catch (error) {
      // Commit failed - this is a serious error
      logger.error('Commit failed for prepared transaction', {
        shardId: this.shardId,
        txnId,
        operation: 'commit',
        error,
      });
      throw error;
    }
  }

  /**
   * Handle ABORT message from coordinator.
   */
  async handleAbort(message: AbortMessage): Promise<AckMessage> {
    const { txnId } = message;

    await this.abortPreparedTransaction(txnId);

    return this.createAckMessage(txnId);
  }

  /**
   * Handle STATUS_QUERY message from coordinator.
   */
  async handleStatusQuery(
    message: StatusQueryMessage
  ): Promise<StatusResponseMessage> {
    const { txnId } = message;

    const preparedTxn = this.preparedTransactions.get(txnId);

    return {
      type: 'status_response',
      txnId,
      shardId: this.shardId,
      timestamp: Date.now(),
      participantState: preparedTxn?.state ?? 'done',
      preparedLSN: preparedTxn?.preparedLSN,
    };
  }

  // --------------------------------------------------------------------------
  // Internal Methods
  // --------------------------------------------------------------------------

  /**
   * Abort a prepared transaction.
   */
  private async abortPreparedTransaction(txnId: TransactionId): Promise<void> {
    // Track that this transaction was aborted for idempotency
    this.abortedTransactions.add(txnId);

    const preparedTxn = this.preparedTransactions.get(txnId);
    if (!preparedTxn) return;

    // Release locks
    this.lockManager.releaseLocks(txnId);

    // Remove from memory
    this.preparedTransactions.delete(txnId);

    // Remove from storage
    await this.storage.deletePreparedTransaction(txnId);
  }

  /**
   * Create a PREPARED message.
   */
  private createPreparedMessage(
    txnId: TransactionId,
    preparedLSN: number
  ): PreparedMessage {
    return {
      type: 'prepared',
      txnId,
      shardId: this.shardId,
      timestamp: Date.now(),
      preparedLSN,
    };
  }

  /**
   * Create an ABORT_VOTE message.
   */
  private createAbortVoteMessage(
    txnId: TransactionId,
    reason: string
  ): AbortVoteMessage {
    return {
      type: 'abort_vote',
      txnId,
      shardId: this.shardId,
      timestamp: Date.now(),
      reason,
    };
  }

  /**
   * Create an ACK message.
   */
  private createAckMessage(txnId: TransactionId, finalLSN?: number): AckMessage {
    return {
      type: 'ack',
      txnId,
      shardId: this.shardId,
      timestamp: Date.now(),
      finalLSN,
    };
  }

  // --------------------------------------------------------------------------
  // Statistics
  // --------------------------------------------------------------------------

  /**
   * Get participant statistics.
   */
  getStats(): {
    preparedCount: number;
    lockStats: { totalLocks: number; transactionCount: number };
  } {
    return {
      preparedCount: this.preparedTransactions.size,
      lockStats: this.lockManager.getStats(),
    };
  }

  /**
   * Get prepared transaction IDs.
   */
  getPreparedTransactionIds(): TransactionId[] {
    return Array.from(this.preparedTransactions.keys());
  }
}

// ============================================================================
// Factory Function
// ============================================================================

/**
 * Create a new TransactionParticipant.
 */
export function createTransactionParticipant(
  shardId: number,
  storage: ParticipantStorage,
  executor: OperationExecutor,
  options?: ParticipantOptions
): TransactionParticipant {
  return new TransactionParticipant(shardId, storage, executor, options);
}
