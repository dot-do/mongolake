/**
 * MongoDB-compatible Session and Transaction Support
 *
 * Provides ClientSession class for transaction management with ACID guarantees.
 *
 * @example
 * ```typescript
 * const session = client.startSession();
 * session.startTransaction();
 *
 * try {
 *   await collection.insertOne(doc, { session });
 *   await collection.updateOne(filter, update, { session });
 *   await session.commitTransaction();
 * } catch (error) {
 *   await session.abortTransaction();
 * } finally {
 *   await session.endSession();
 * }
 * ```
 */

import type { Document } from '../types.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Transaction state enum following MongoDB semantics.
 */
export type TransactionState =
  | 'none'           // No transaction active
  | 'starting'       // startTransaction() called but no operations yet
  | 'in_progress'    // Transaction has operations
  | 'committed'      // Transaction committed successfully
  | 'aborted';       // Transaction aborted

/**
 * Read concern levels for transactions.
 */
export type ReadConcernLevel = 'local' | 'majority' | 'linearizable' | 'snapshot';

/**
 * Write concern levels for transactions.
 */
export interface WriteConcern {
  w?: number | 'majority';
  j?: boolean;
  wtimeout?: number;
}

/**
 * Transaction options.
 */
export interface TransactionOptions {
  readConcern?: { level: ReadConcernLevel };
  writeConcern?: WriteConcern;
  maxCommitTimeMS?: number;
}

/**
 * Session options for starting a new session.
 */
export interface SessionOptions {
  /** Default transaction options for all transactions in this session */
  defaultTransactionOptions?: TransactionOptions;
  /** Whether to enable causal consistency */
  causalConsistency?: boolean;
}

/**
 * Options that can include a session for transaction support.
 */
export interface SessionOperationOptions {
  session?: ClientSession;
}

/**
 * Buffered operation during a transaction.
 */
export interface BufferedOperation {
  type: 'insert' | 'update' | 'delete' | 'replace';
  collection: string;
  database: string;
  document?: Document;
  filter?: Document;
  update?: Document;
  replacement?: Document;
  options?: Record<string, unknown>;
  timestamp: number;
}

/**
 * Session ID type - UUID string.
 */
export type SessionId = string;

// ============================================================================
// Error Classes
// ============================================================================

/**
 * Error thrown when transaction operations are invalid.
 */
export class TransactionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'TransactionError';
  }
}

/**
 * Error thrown when session operations are invalid.
 */
export class SessionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SessionError';
  }
}

// ============================================================================
// ClientSession
// ============================================================================

/**
 * ClientSession provides transaction management for MongoLake operations.
 *
 * Sessions track transaction state and buffer operations until commit.
 * When committed, all buffered operations are applied atomically.
 * When aborted, all buffered operations are discarded.
 */
export class ClientSession {
  /** Unique session identifier */
  public readonly id: SessionId;

  /** Session options */
  private readonly options: SessionOptions;

  /** Current transaction state */
  private _transactionState: TransactionState = 'none';

  /** Current transaction options */
  private _transactionOptions: TransactionOptions | null = null;

  /** Buffered operations for the current transaction */
  private _bufferedOperations: BufferedOperation[] = [];

  /** Whether the session has ended */
  private _ended: boolean = false;

  /** Transaction number (incremented for each new transaction) */
  private _txnNumber: number = 0;

  /** Commit handler provided by the client */
  private _commitHandler:
    | ((session: ClientSession, operations: BufferedOperation[]) => Promise<void>)
    | null = null;

  /** Timestamp when session was created */
  public readonly createdAt: Date;

  /** Last activity timestamp */
  private _lastUsed: Date;

  constructor(options: SessionOptions = {}) {
    this.id = generateSessionId();
    this.options = options;
    this.createdAt = new Date();
    this._lastUsed = new Date();
  }

  // --------------------------------------------------------------------------
  // Properties
  // --------------------------------------------------------------------------

  /**
   * Get the current transaction state.
   */
  get transactionState(): TransactionState {
    return this._transactionState;
  }

  /**
   * Check if session has ended.
   */
  get hasEnded(): boolean {
    return this._ended;
  }

  /**
   * Check if a transaction is in progress.
   */
  get inTransaction(): boolean {
    return (
      this._transactionState === 'starting' ||
      this._transactionState === 'in_progress'
    );
  }

  /**
   * Get the current transaction number.
   */
  get txnNumber(): number {
    return this._txnNumber;
  }

  /**
   * Get the current transaction options.
   */
  get transactionOptions(): TransactionOptions | null {
    return this._transactionOptions;
  }

  /**
   * Get the number of buffered operations.
   */
  get operationCount(): number {
    return this._bufferedOperations.length;
  }

  /**
   * Get the last used timestamp.
   */
  get lastUsed(): Date {
    return this._lastUsed;
  }

  // --------------------------------------------------------------------------
  // Transaction Methods
  // --------------------------------------------------------------------------

  /**
   * Start a new transaction.
   *
   * @param options - Transaction options (readConcern, writeConcern)
   * @throws TransactionError if a transaction is already active or session has ended
   */
  startTransaction(options?: TransactionOptions): void {
    this.ensureSessionActive();

    if (this.inTransaction) {
      throw new TransactionError(
        'Transaction already in progress. Commit or abort the current transaction before starting a new one.'
      );
    }

    this._transactionState = 'starting';
    this._transactionOptions = {
      ...this.options.defaultTransactionOptions,
      ...options,
    };
    this._bufferedOperations = [];
    this._txnNumber++;
    this._lastUsed = new Date();
  }

  /**
   * Commit the current transaction.
   *
   * Applies all buffered operations atomically.
   *
   * @throws TransactionError if no transaction is active
   */
  async commitTransaction(): Promise<void> {
    this.ensureSessionActive();

    if (!this.inTransaction) {
      throw new TransactionError(
        'No transaction in progress. Call startTransaction() first.'
      );
    }

    try {
      // Call the commit handler to apply operations atomically
      if (this._commitHandler && this._bufferedOperations.length > 0) {
        await this._commitHandler(this, [...this._bufferedOperations]);
      }

      this._transactionState = 'committed';
      this._bufferedOperations = [];
      this._lastUsed = new Date();
    } catch (error) {
      // On commit failure, keep transaction in progress for retry
      throw error;
    }
  }

  /**
   * Abort the current transaction.
   *
   * Discards all buffered operations.
   *
   * @throws TransactionError if no transaction is active
   */
  async abortTransaction(): Promise<void> {
    this.ensureSessionActive();

    if (!this.inTransaction) {
      throw new TransactionError(
        'No transaction in progress. Call startTransaction() first.'
      );
    }

    this._transactionState = 'aborted';
    this._bufferedOperations = [];
    this._lastUsed = new Date();
  }

  /**
   * End the session.
   *
   * If a transaction is in progress, it will be aborted.
   * The transaction state is preserved (remains 'aborted' if aborted).
   */
  async endSession(): Promise<void> {
    if (this._ended) {
      return;
    }

    // Abort any active transaction
    if (this.inTransaction) {
      await this.abortTransaction();
    }

    this._ended = true;
    // Note: Don't reset transaction state - keep 'aborted' if we aborted
    this._bufferedOperations = [];
  }

  // --------------------------------------------------------------------------
  // Operation Buffering
  // --------------------------------------------------------------------------

  /**
   * Buffer an operation for the current transaction.
   *
   * @param operation - The operation to buffer
   * @throws TransactionError if no transaction is active
   * @internal
   */
  bufferOperation(operation: Omit<BufferedOperation, 'timestamp'>): void {
    this.ensureSessionActive();

    if (!this.inTransaction) {
      throw new TransactionError(
        'Cannot buffer operation: no transaction in progress.'
      );
    }

    // Move from 'starting' to 'in_progress' on first operation
    if (this._transactionState === 'starting') {
      this._transactionState = 'in_progress';
    }

    this._bufferedOperations.push({
      ...operation,
      timestamp: Date.now(),
    });
    this._lastUsed = new Date();
  }

  /**
   * Get all buffered operations (for commit processing).
   *
   * @returns Copy of buffered operations
   * @internal
   */
  getBufferedOperations(): BufferedOperation[] {
    return [...this._bufferedOperations];
  }

  // --------------------------------------------------------------------------
  // Internal Methods
  // --------------------------------------------------------------------------

  /**
   * Set the commit handler for this session.
   *
   * @param handler - Function to call on commit
   * @internal
   */
  setCommitHandler(
    handler: (session: ClientSession, operations: BufferedOperation[]) => Promise<void>
  ): void {
    this._commitHandler = handler;
  }

  /**
   * Ensure the session is still active.
   *
   * @throws SessionError if session has ended
   */
  private ensureSessionActive(): void {
    if (this._ended) {
      throw new SessionError('Cannot use a session that has ended.');
    }
  }

  /**
   * Update last used timestamp.
   * @internal
   */
  touch(): void {
    this._lastUsed = new Date();
  }

  /**
   * Serialize session for wire protocol.
   */
  toJSON(): { id: string; txnNumber: number } {
    return {
      id: this.id,
      txnNumber: this._txnNumber,
    };
  }
}

// ============================================================================
// Session Store
// ============================================================================

/**
 * SessionStore manages active sessions.
 *
 * Tracks sessions by ID and provides cleanup for expired sessions.
 */
export class SessionStore {
  private sessions: Map<SessionId, ClientSession> = new Map();
  private cleanupInterval: ReturnType<typeof setInterval> | null = null;

  /** Default session timeout in milliseconds (30 minutes) */
  public readonly timeoutMs: number;

  constructor(options: { timeoutMs?: number; cleanupIntervalMs?: number } = {}) {
    this.timeoutMs = options.timeoutMs ?? 30 * 60 * 1000;

    // Start cleanup interval if specified
    const cleanupIntervalMs = options.cleanupIntervalMs ?? 60000;
    if (cleanupIntervalMs > 0) {
      this.cleanupInterval = setInterval(() => {
        this.cleanupExpired();
      }, cleanupIntervalMs);
    }
  }

  /**
   * Add a session to the store.
   */
  add(session: ClientSession): void {
    this.sessions.set(session.id, session);
  }

  /**
   * Get a session by ID.
   */
  get(sessionId: SessionId): ClientSession | undefined {
    return this.sessions.get(sessionId);
  }

  /**
   * Remove a session from the store.
   */
  remove(sessionId: SessionId): boolean {
    return this.sessions.delete(sessionId);
  }

  /**
   * Check if a session exists.
   */
  has(sessionId: SessionId): boolean {
    return this.sessions.has(sessionId);
  }

  /**
   * Get the number of active sessions.
   */
  get size(): number {
    return this.sessions.size;
  }

  /**
   * Clean up expired sessions.
   */
  cleanupExpired(): number {
    const now = Date.now();
    let cleaned = 0;

    for (const [id, session] of this.sessions) {
      const age = now - session.lastUsed.getTime();
      if (age > this.timeoutMs || session.hasEnded) {
        this.sessions.delete(id);
        cleaned++;
      }
    }

    return cleaned;
  }

  /**
   * End all sessions and stop cleanup.
   */
  async closeAll(): Promise<void> {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
    }

    for (const session of this.sessions.values()) {
      await session.endSession();
    }
    this.sessions.clear();
  }

  /**
   * Get all session IDs.
   */
  getSessionIds(): SessionId[] {
    return Array.from(this.sessions.keys());
  }
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Generate a unique session ID using UUID v4.
 */
export function generateSessionId(): SessionId {
  return crypto.randomUUID();
}

/**
 * Check if an options object contains a session.
 */
export function hasSession(options?: SessionOperationOptions): options is { session: ClientSession } {
  return options?.session instanceof ClientSession;
}

/**
 * Extract session from options if present.
 */
export function extractSession(options?: SessionOperationOptions): ClientSession | undefined {
  if (hasSession(options)) {
    return options.session;
  }
  return undefined;
}

// ============================================================================
// Exports
// ============================================================================

export default ClientSession;
