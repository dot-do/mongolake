/**
 * Iceberg Snapshot Manager
 *
 * Manages Iceberg-compatible snapshots for table versioning.
 *
 * Iceberg Specification Reference:
 * https://iceberg.apache.org/spec/#snapshots
 *
 * A snapshot represents the state of a table at some point in time.
 * Each snapshot contains:
 * - snapshot-id: unique long ID
 * - parent-snapshot-id: ID of parent snapshot (null for first)
 * - sequence-number: monotonically increasing sequence number
 * - timestamp-ms: creation timestamp
 * - manifest-list: path to the manifest list
 * - summary: map of summary stats and operation type
 * - schema-id: ID of the schema used for the snapshot
 */

import type { StorageBackend } from '../storage/index.js';

// ============================================================================
// Constants
// ============================================================================

/** Valid operation types for Iceberg snapshots */
const VALID_OPERATIONS = new Set<OperationType>(['append', 'overwrite', 'delete', 'replace']);

/** Default values for snapshot configuration */
const DEFAULTS = {
  SCHEMA_ID: 0,
  MIN_SNAPSHOTS_TO_RETAIN: 1,
  SEQUENCE_NUMBER_START: 1n,
  ID_START: 1n,
} as const;

// ============================================================================
// Types
// ============================================================================

/**
 * Valid operation types for Iceberg snapshots.
 *
 * - append: Data files are added without affecting existing data
 * - overwrite: Data files are added and removed in a single operation
 * - delete: Data files or records are removed
 * - replace: Schema or partition spec is changed
 */
export type OperationType = 'append' | 'overwrite' | 'delete' | 'replace';

/**
 * Snapshot summary - a map of string key-value pairs.
 *
 * Required:
 * - operation: The operation type that produced the snapshot
 *
 * Optional (commonly used):
 * - added-data-files: Number of data files added
 * - deleted-data-files: Number of data files removed
 * - total-data-files: Total data files in snapshot
 * - added-records: Number of records added
 * - deleted-records: Number of records removed
 * - total-records: Total records in snapshot
 * - added-files-size: Total size of added files
 * - removed-files-size: Total size of removed files
 * - total-files-size: Total size of all files
 * - added-equality-delete-files: Equality delete files added
 * - added-position-delete-files: Position delete files added
 * - total-equality-deletes: Total equality deletes
 * - total-position-deletes: Total position deletes
 * - changed-partition-count: Number of partitions with changes
 */
export interface SnapshotSummary {
  operation: OperationType;
  [key: string]: string;
}

/**
 * Iceberg snapshot representing table state at a point in time.
 */
export interface Snapshot {
  /** Unique 64-bit snapshot ID */
  snapshotId: bigint;

  /** Parent snapshot ID (null for first snapshot) */
  parentSnapshotId: bigint | null;

  /** Monotonically increasing sequence number */
  sequenceNumber: bigint;

  /** Snapshot creation timestamp in milliseconds */
  timestampMs: number;

  /** Path to the manifest list file */
  manifestList: string;

  /** Summary of changes and statistics */
  summary: SnapshotSummary;

  /** Schema ID used for this snapshot */
  schemaId: number;
}

/**
 * Options for creating a new snapshot.
 */
export interface CreateSnapshotOptions {
  /** Operation type */
  operation: OperationType;

  /** Path to manifest list file */
  manifestListPath: string;

  /** Optional custom timestamp (defaults to current time) */
  timestampMs?: number;

  /** Optional schema ID (defaults to 0) */
  schemaId?: number;

  /** Optional summary statistics */
  summary?: Partial<Omit<SnapshotSummary, 'operation'>>;

  /** Optional explicit parent snapshot ID (for branching) */
  parentSnapshotId?: bigint;

  /** Optional expected parent for optimistic concurrency */
  expectedParentSnapshotId?: bigint;
}

/**
 * Options for listing snapshots.
 */
export interface ListSnapshotsOptions {
  /** Maximum number of snapshots to return */
  limit?: number;

  /** Offset for pagination */
  offset?: number;
}

/**
 * Options for getting snapshot ancestry.
 */
export interface AncestryOptions {
  /** Maximum depth to traverse */
  maxDepth?: number;
}

/**
 * Options for expiring snapshots.
 */
export interface ExpireSnapshotsOptions {
  /** Expire snapshots older than this timestamp */
  olderThanMs: number;

  /** Minimum number of snapshots to retain */
  minSnapshotsToRetain?: number;
}

/**
 * Result of snapshot expiration.
 */
export interface ExpireSnapshotsResult {
  /** Number of snapshots expired */
  expiredCount: number;

  /** IDs of expired snapshots */
  expiredSnapshots: bigint[];
}

/**
 * Result of rollback operation.
 */
export interface RollbackResult {
  /** ID of the new snapshot created for rollback */
  newSnapshotId: bigint;

  /** ID of the target snapshot rolled back to */
  targetSnapshotId: bigint;
}

/**
 * Result of cherry-pick operation.
 */
export interface CherryPickResult {
  /** ID of the new snapshot created */
  newSnapshotId: bigint;

  /** ID of the source snapshot cherry-picked */
  sourceSnapshotId: bigint;
}

/**
 * Snapshot retention policy configuration.
 */
export interface RetentionPolicy {
  /** Maximum age of snapshots to retain in milliseconds */
  maxAgeMs?: number;

  /** Maximum number of snapshots to retain */
  maxSnapshots?: number;

  /** Minimum number of snapshots to always retain */
  minSnapshots?: number;
}

/**
 * Snapshot manager configuration options.
 */
export interface SnapshotManagerConfig {
  /** Custom ID generator function */
  idGenerator?: () => bigint;

  /** Table location (e.g., s3://bucket/path) */
  tableLocation?: string;

  /** Enable metadata caching */
  cacheEnabled?: boolean;

  /** Cache TTL in milliseconds */
  cacheTtlMs?: number;

  /** Default retention policy for automatic cleanup */
  retentionPolicy?: RetentionPolicy;
}

/**
 * Internal snapshot state persisted to storage.
 */
interface SnapshotManagerState {
  /** All snapshots indexed by ID */
  snapshots: Map<bigint, Snapshot>;

  /** Current snapshot ID */
  currentSnapshotId: bigint | null;

  /** Next sequence number */
  nextSequenceNumber: bigint;

  /** Next ID (used by default ID generator) */
  nextId: bigint;
}

/**
 * Serializable state for persistence.
 */
interface SerializedState {
  snapshots: Array<{
    snapshotId: string;
    parentSnapshotId: string | null;
    sequenceNumber: string;
    timestampMs: number;
    manifestList: string;
    summary: SnapshotSummary;
    schemaId: number;
  }>;
  currentSnapshotId: string | null;
  nextSequenceNumber: string;
  nextId: string;
}

// ============================================================================
// Type Guards and Validation Helpers
// ============================================================================

/**
 * Check if a string is a valid operation type.
 */
function isValidOperation(operation: string): operation is OperationType {
  return VALID_OPERATIONS.has(operation as OperationType);
}

/**
 * Validation error messages for consistent error reporting.
 */
const ValidationErrors = {
  notInitialized: 'SnapshotManager is not initialized',
  manifestListRequired: 'Manifest list path is required',
  invalidOperation: (op: string) => `Invalid operation type: "${op}"`,
  invalidTimestamp: 'Invalid timestamp: timestamp cannot be negative',
  invalidSchemaId: 'Invalid schema ID: schema ID cannot be negative',
  parentNotFound: (id: bigint) => `Parent snapshot not found: ${id}`,
  snapshotNotFound: (id: bigint) => `Snapshot not found: ${id}`,
  concurrentModification: (expected: bigint | null, current: bigint | null) =>
    `Concurrent modification detected: expected parent ${expected}, but current is ${current}`,
} as const;

// ============================================================================
// Snapshot Manager Implementation
// ============================================================================

/**
 * SnapshotManager handles Iceberg-compatible snapshot operations.
 *
 * This class provides:
 * - Snapshot creation with automatic ID and sequence number generation
 * - Parent tracking for snapshot lineage
 * - Time-travel queries (as-of timestamp)
 * - Snapshot expiration and retention management
 * - Rollback and cherry-pick operations
 * - Optimistic concurrency control
 */
export class SnapshotManager {
  private _initialized = false;
  private readonly _storage: StorageBackend;
  private readonly _tableName: string;
  private readonly _config: SnapshotManagerConfig;

  private _state: SnapshotManagerState = {
    snapshots: new Map(),
    currentSnapshotId: null,
    nextSequenceNumber: DEFAULTS.SEQUENCE_NUMBER_START,
    nextId: DEFAULTS.ID_START,
  };

  /** Mutex for concurrent operations */
  private _operationLock: Promise<void> = Promise.resolve();

  constructor(
    storage: StorageBackend,
    tableName: string,
    config: SnapshotManagerConfig = {}
  ) {
    this._storage = storage;
    this._tableName = tableName;
    this._config = config;
  }

  // ==========================================================================
  // Initialization
  // ==========================================================================

  /**
   * Initialize the snapshot manager.
   * Idempotent - safe to call multiple times.
   */
  async initialize(): Promise<void> {
    if (this._initialized) {
      return;
    }

    await this.loadState();
    this._initialized = true;
  }

  /**
   * Check if the manager is initialized.
   */
  isInitialized(): boolean {
    return this._initialized;
  }

  /**
   * Ensure the manager is initialized, throwing if not.
   */
  private ensureInitialized(): void {
    if (!this._initialized) {
      throw new Error(ValidationErrors.notInitialized);
    }
  }

  // ==========================================================================
  // Storage Operations
  // ==========================================================================

  /**
   * Get the storage key for snapshot state.
   */
  private getStateKey(): string {
    return `${this._tableName}/metadata/snapshot-state.json`;
  }

  /**
   * Load state from storage.
   */
  private async loadState(): Promise<void> {
    const data = await this._storage.get(this.getStateKey());
    if (!data) {
      return; // No existing state, start fresh
    }

    const json = new TextDecoder().decode(data);
    const serialized: SerializedState = JSON.parse(json);
    this.deserializeState(serialized);
  }

  /**
   * Deserialize state from storage format.
   */
  private deserializeState(serialized: SerializedState): void {
    this._state.snapshots = new Map();

    for (const s of serialized.snapshots) {
      const snapshot = this.deserializeSnapshot(s);
      this._state.snapshots.set(snapshot.snapshotId, snapshot);
    }

    this._state.currentSnapshotId = serialized.currentSnapshotId !== null
      ? BigInt(serialized.currentSnapshotId)
      : null;
    this._state.nextSequenceNumber = BigInt(serialized.nextSequenceNumber);
    this._state.nextId = BigInt(serialized.nextId);
  }

  /**
   * Deserialize a single snapshot from storage format.
   */
  private deserializeSnapshot(s: SerializedState['snapshots'][0]): Snapshot {
    return {
      snapshotId: BigInt(s.snapshotId),
      parentSnapshotId: s.parentSnapshotId !== null ? BigInt(s.parentSnapshotId) : null,
      sequenceNumber: BigInt(s.sequenceNumber),
      timestampMs: s.timestampMs,
      manifestList: s.manifestList,
      summary: s.summary,
      schemaId: s.schemaId,
    };
  }

  /**
   * Save state to storage.
   */
  private async saveState(): Promise<void> {
    const serialized = this.serializeState();
    const json = JSON.stringify(serialized);
    await this._storage.put(this.getStateKey(), new TextEncoder().encode(json));
  }

  /**
   * Serialize state for storage.
   */
  private serializeState(): SerializedState {
    return {
      snapshots: Array.from(this._state.snapshots.values()).map(s => this.serializeSnapshot(s)),
      currentSnapshotId: this._state.currentSnapshotId?.toString() ?? null,
      nextSequenceNumber: this._state.nextSequenceNumber.toString(),
      nextId: this._state.nextId.toString(),
    };
  }

  /**
   * Serialize a single snapshot for storage.
   */
  private serializeSnapshot(s: Snapshot): SerializedState['snapshots'][0] {
    return {
      snapshotId: s.snapshotId.toString(),
      parentSnapshotId: s.parentSnapshotId?.toString() ?? null,
      sequenceNumber: s.sequenceNumber.toString(),
      timestampMs: s.timestampMs,
      manifestList: s.manifestList,
      summary: s.summary,
      schemaId: s.schemaId,
    };
  }

  // ==========================================================================
  // Concurrency Control
  // ==========================================================================

  /**
   * Acquire a lock for operations to ensure sequential execution.
   */
  private async withLock<T>(fn: () => Promise<T>): Promise<T> {
    const prevLock = this._operationLock;
    let resolve: () => void;
    this._operationLock = new Promise<void>(r => { resolve = r; });

    await prevLock;
    try {
      return await fn();
    } finally {
      resolve!();
    }
  }

  // ==========================================================================
  // ID Generation
  // ==========================================================================

  /**
   * Generate a new unique snapshot ID.
   */
  private generateSnapshotId(): bigint {
    if (this._config.idGenerator) {
      return this._config.idGenerator();
    }
    return this._state.nextId++;
  }

  /**
   * Get the next sequence number and increment.
   */
  private getNextSequenceNumber(): bigint {
    return this._state.nextSequenceNumber++;
  }

  // ==========================================================================
  // Validation
  // ==========================================================================

  /**
   * Validate create snapshot options.
   */
  private validateCreateOptions(options: CreateSnapshotOptions): void {
    if (!isValidOperation(options.operation)) {
      throw new Error(ValidationErrors.invalidOperation(options.operation));
    }

    if (!options.manifestListPath?.trim()) {
      throw new Error(ValidationErrors.manifestListRequired);
    }

    if (options.timestampMs !== undefined && options.timestampMs < 0) {
      throw new Error(ValidationErrors.invalidTimestamp);
    }

    if (options.schemaId !== undefined && options.schemaId < 0) {
      throw new Error(ValidationErrors.invalidSchemaId);
    }
  }

  /**
   * Validate and resolve the parent snapshot ID for a new snapshot.
   */
  private resolveParentSnapshotId(options: CreateSnapshotOptions): bigint | null {
    if (options.parentSnapshotId !== undefined) {
      if (!this._state.snapshots.has(options.parentSnapshotId)) {
        throw new Error(ValidationErrors.parentNotFound(options.parentSnapshotId));
      }
      return options.parentSnapshotId;
    }
    return this._state.currentSnapshotId;
  }

  /**
   * Check optimistic concurrency control constraint.
   */
  private checkConcurrencyControl(expectedParentSnapshotId: bigint | undefined): void {
    if (expectedParentSnapshotId !== undefined) {
      if (this._state.currentSnapshotId !== expectedParentSnapshotId) {
        throw new Error(ValidationErrors.concurrentModification(
          expectedParentSnapshotId,
          this._state.currentSnapshotId
        ));
      }
    }
  }

  // ==========================================================================
  // Snapshot Creation
  // ==========================================================================

  /**
   * Create a new snapshot.
   */
  async createSnapshot(options: CreateSnapshotOptions): Promise<Snapshot> {
    this.ensureInitialized();

    return this.withLock(async () => {
      this.validateCreateOptions(options);
      this.checkConcurrencyControl(options.expectedParentSnapshotId);

      const parentSnapshotId = this.resolveParentSnapshotId(options);
      const snapshot = this.buildSnapshot(options, parentSnapshotId);

      this._state.snapshots.set(snapshot.snapshotId, snapshot);
      this._state.currentSnapshotId = snapshot.snapshotId;

      await this.saveState();
      return snapshot;
    });
  }

  /**
   * Build a snapshot object from options.
   */
  private buildSnapshot(options: CreateSnapshotOptions, parentSnapshotId: bigint | null): Snapshot {
    const summary: SnapshotSummary = {
      operation: options.operation,
      ...options.summary,
    };

    return {
      snapshotId: this.generateSnapshotId(),
      parentSnapshotId,
      sequenceNumber: this.getNextSequenceNumber(),
      timestampMs: options.timestampMs ?? Date.now(),
      manifestList: options.manifestListPath,
      summary,
      schemaId: options.schemaId ?? DEFAULTS.SCHEMA_ID,
    };
  }

  // ==========================================================================
  // Snapshot Retrieval
  // ==========================================================================

  /**
   * Get a snapshot by ID.
   */
  async getSnapshot(snapshotId: bigint): Promise<Snapshot | null> {
    this.ensureInitialized();
    return this._state.snapshots.get(snapshotId) ?? null;
  }

  /**
   * Get the current snapshot.
   */
  async getCurrentSnapshot(): Promise<Snapshot | null> {
    this.ensureInitialized();

    if (this._state.currentSnapshotId === null) {
      return null;
    }
    return this._state.snapshots.get(this._state.currentSnapshotId) ?? null;
  }

  /**
   * Get the current snapshot ID.
   */
  getCurrentSnapshotId(): bigint | null {
    this.ensureInitialized();
    return this._state.currentSnapshotId;
  }

  /**
   * List all snapshots with optional pagination.
   */
  async listSnapshots(options?: ListSnapshotsOptions): Promise<Snapshot[]> {
    this.ensureInitialized();

    const snapshots = this.getSnapshotsSortedBySequence();
    return this.applyPagination(snapshots, options);
  }

  /**
   * Get all snapshots sorted by sequence number.
   */
  private getSnapshotsSortedBySequence(): Snapshot[] {
    return Array.from(this._state.snapshots.values())
      .sort((a, b) => Number(a.sequenceNumber - b.sequenceNumber));
  }

  /**
   * Get all snapshots sorted by timestamp (newest first).
   */
  private getSnapshotsSortedByTimestamp(): Snapshot[] {
    return Array.from(this._state.snapshots.values())
      .sort((a, b) => b.timestampMs - a.timestampMs);
  }

  /**
   * Apply pagination to a list of snapshots.
   */
  private applyPagination(snapshots: Snapshot[], options?: ListSnapshotsOptions): Snapshot[] {
    const offset = options?.offset ?? 0;
    const limit = options?.limit ?? snapshots.length;
    return snapshots.slice(offset, offset + limit);
  }

  // ==========================================================================
  // Ancestry and Lineage
  // ==========================================================================

  /**
   * Get snapshot ancestry (parent chain).
   */
  async getSnapshotAncestry(
    snapshotId: bigint,
    options?: AncestryOptions
  ): Promise<Snapshot[]> {
    this.ensureInitialized();

    const snapshot = this._state.snapshots.get(snapshotId);
    if (!snapshot) {
      throw new Error(ValidationErrors.snapshotNotFound(snapshotId));
    }

    return this.walkAncestry(snapshot.parentSnapshotId, options?.maxDepth ?? Infinity);
  }

  /**
   * Walk the ancestry chain from a given parent ID.
   */
  private walkAncestry(startParentId: bigint | null, maxDepth: number): Snapshot[] {
    const ancestry: Snapshot[] = [];
    let currentParentId = startParentId;

    while (currentParentId !== null && ancestry.length < maxDepth) {
      const parent = this._state.snapshots.get(currentParentId);
      if (!parent) break;

      ancestry.push(parent);
      currentParentId = parent.parentSnapshotId;
    }

    return ancestry;
  }

  /**
   * Collect all ancestor IDs for a snapshot (including itself).
   */
  private collectAncestorIds(snapshotId: bigint): Set<bigint> {
    const ancestors = new Set<bigint>();
    ancestors.add(snapshotId);

    let currentId: bigint | null = snapshotId;
    while (currentId !== null) {
      const snapshot = this._state.snapshots.get(currentId);
      if (!snapshot) break;

      if (snapshot.parentSnapshotId !== null) {
        ancestors.add(snapshot.parentSnapshotId);
      }
      currentId = snapshot.parentSnapshotId;
    }

    return ancestors;
  }

  /**
   * Find common ancestor between two snapshots.
   */
  async findCommonAncestor(
    snapshotId1: bigint,
    snapshotId2: bigint
  ): Promise<Snapshot | null> {
    this.ensureInitialized();

    const ancestors1 = this.collectAncestorIds(snapshotId1);

    // Walk ancestry of second snapshot and find first common ancestor
    let currentId: bigint | null = snapshotId2;
    while (currentId !== null) {
      const snapshot = this._state.snapshots.get(currentId);
      if (!snapshot) break;

      if (snapshot.parentSnapshotId !== null && ancestors1.has(snapshot.parentSnapshotId)) {
        return this._state.snapshots.get(snapshot.parentSnapshotId) ?? null;
      }
      currentId = snapshot.parentSnapshotId;
    }

    return null;
  }

  // ==========================================================================
  // History and Time Travel
  // ==========================================================================

  /**
   * Get snapshot log (history in reverse chronological order).
   */
  async getSnapshotLog(): Promise<Snapshot[]> {
    this.ensureInitialized();
    return this.getSnapshotsSortedByTimestamp();
  }

  /**
   * Get snapshot as of a specific timestamp.
   * Returns the latest snapshot at or before the timestamp.
   */
  async getSnapshotAsOf(timestampMs: number): Promise<Snapshot | null> {
    this.ensureInitialized();

    let result: Snapshot | null = null;

    for (const snapshot of this._state.snapshots.values()) {
      if (snapshot.timestampMs <= timestampMs) {
        if (result === null || snapshot.timestampMs > result.timestampMs) {
          result = snapshot;
        }
      }
    }

    return result;
  }

  /**
   * List all snapshots that existed as of a specific timestamp.
   */
  async listSnapshotsAsOf(timestampMs: number): Promise<Snapshot[]> {
    this.ensureInitialized();

    return Array.from(this._state.snapshots.values())
      .filter(s => s.timestampMs <= timestampMs)
      .sort((a, b) => Number(a.sequenceNumber - b.sequenceNumber));
  }

  // ==========================================================================
  // Expiration and Retention
  // ==========================================================================

  /**
   * Expire old snapshots based on age criteria.
   */
  async expireSnapshots(options: ExpireSnapshotsOptions): Promise<ExpireSnapshotsResult> {
    this.ensureInitialized();

    return this.withLock(async () => {
      const expiredSnapshots = this.collectExpirableSnapshots(options);

      for (const snapshotId of expiredSnapshots) {
        this._state.snapshots.delete(snapshotId);
      }

      if (expiredSnapshots.length > 0) {
        await this.saveState();
      }

      return {
        expiredCount: expiredSnapshots.length,
        expiredSnapshots,
      };
    });
  }

  /**
   * Collect snapshot IDs that can be expired based on options.
   */
  private collectExpirableSnapshots(options: ExpireSnapshotsOptions): bigint[] {
    const { olderThanMs, minSnapshotsToRetain = 0 } = options;

    // Get snapshots sorted by timestamp (oldest first)
    const sortedSnapshots = Array.from(this._state.snapshots.values())
      .sort((a, b) => a.timestampMs - b.timestampMs);

    const totalSnapshots = sortedSnapshots.length;
    const minToKeep = Math.max(minSnapshotsToRetain, DEFAULTS.MIN_SNAPSHOTS_TO_RETAIN);
    const maxExpirable = Math.max(0, totalSnapshots - minToKeep);

    const expiredSnapshots: bigint[] = [];

    for (const snapshot of sortedSnapshots) {
      if (expiredSnapshots.length >= maxExpirable) break;
      if (snapshot.snapshotId === this._state.currentSnapshotId) continue;

      if (snapshot.timestampMs < olderThanMs) {
        expiredSnapshots.push(snapshot.snapshotId);
      }
    }

    return expiredSnapshots;
  }

  /**
   * Apply the configured retention policy.
   * Returns information about expired snapshots.
   */
  async applyRetentionPolicy(): Promise<ExpireSnapshotsResult> {
    this.ensureInitialized();

    const policy = this._config.retentionPolicy;
    if (!policy) {
      return { expiredCount: 0, expiredSnapshots: [] };
    }

    return this.withLock(async () => {
      const expiredSnapshots: bigint[] = [];

      // Expire by age
      if (policy.maxAgeMs !== undefined) {
        const cutoffMs = Date.now() - policy.maxAgeMs;
        const byAge = this.collectExpirableSnapshots({
          olderThanMs: cutoffMs,
          minSnapshotsToRetain: policy.minSnapshots,
        });
        expiredSnapshots.push(...byAge);
      }

      // Expire by count
      if (policy.maxSnapshots !== undefined) {
        const byCount = this.collectExpirableByCount(
          policy.maxSnapshots,
          policy.minSnapshots ?? DEFAULTS.MIN_SNAPSHOTS_TO_RETAIN,
          new Set(expiredSnapshots)
        );
        expiredSnapshots.push(...byCount);
      }

      // Remove duplicates
      const uniqueExpired = [...new Set(expiredSnapshots)];

      for (const snapshotId of uniqueExpired) {
        this._state.snapshots.delete(snapshotId);
      }

      if (uniqueExpired.length > 0) {
        await this.saveState();
      }

      return {
        expiredCount: uniqueExpired.length,
        expiredSnapshots: uniqueExpired,
      };
    });
  }

  /**
   * Collect snapshots to expire based on count limit.
   */
  private collectExpirableByCount(
    maxSnapshots: number,
    minSnapshots: number,
    alreadyExpired: Set<bigint>
  ): bigint[] {
    const sortedSnapshots = Array.from(this._state.snapshots.values())
      .filter(s => !alreadyExpired.has(s.snapshotId))
      .sort((a, b) => a.timestampMs - b.timestampMs);

    const toExpire = Math.max(0, sortedSnapshots.length - maxSnapshots);
    const canExpire = Math.max(0, sortedSnapshots.length - minSnapshots);
    const expireCount = Math.min(toExpire, canExpire);

    const result: bigint[] = [];
    for (const snapshot of sortedSnapshots) {
      if (result.length >= expireCount) break;
      if (snapshot.snapshotId === this._state.currentSnapshotId) continue;
      result.push(snapshot.snapshotId);
    }

    return result;
  }

  /**
   * Get the configured retention policy.
   */
  getRetentionPolicy(): RetentionPolicy | undefined {
    return this._config.retentionPolicy;
  }

  // ==========================================================================
  // Rollback and Cherry-Pick
  // ==========================================================================

  /**
   * Rollback to a previous snapshot.
   * Creates a new snapshot representing the rollback and sets current to the target.
   */
  async rollbackToSnapshot(snapshotId: bigint): Promise<RollbackResult> {
    this.ensureInitialized();

    return this.withLock(async () => {
      const targetSnapshot = this._state.snapshots.get(snapshotId);
      if (!targetSnapshot) {
        throw new Error(ValidationErrors.snapshotNotFound(snapshotId));
      }

      const rollbackSnapshot = this.buildRollbackSnapshot(targetSnapshot);

      this._state.snapshots.set(rollbackSnapshot.snapshotId, rollbackSnapshot);
      this._state.currentSnapshotId = snapshotId;

      await this.saveState();

      return {
        newSnapshotId: rollbackSnapshot.snapshotId,
        targetSnapshotId: snapshotId,
      };
    });
  }

  /**
   * Build a snapshot representing a rollback operation.
   */
  private buildRollbackSnapshot(targetSnapshot: Snapshot): Snapshot {
    return {
      snapshotId: this.generateSnapshotId(),
      parentSnapshotId: this._state.currentSnapshotId,
      sequenceNumber: this.getNextSequenceNumber(),
      timestampMs: Date.now(),
      manifestList: targetSnapshot.manifestList,
      summary: {
        operation: 'replace',
        'rollback-to-snapshot-id': targetSnapshot.snapshotId.toString(),
      },
      schemaId: targetSnapshot.schemaId,
    };
  }

  /**
   * Cherry-pick changes from another snapshot.
   * Creates a new snapshot that applies the source changes to the current state.
   */
  async cherryPick(sourceSnapshotId: bigint): Promise<CherryPickResult> {
    this.ensureInitialized();

    return this.withLock(async () => {
      const sourceSnapshot = this._state.snapshots.get(sourceSnapshotId);
      if (!sourceSnapshot) {
        throw new Error(ValidationErrors.snapshotNotFound(sourceSnapshotId));
      }

      const cherryPickSnapshot = this.buildCherryPickSnapshot(sourceSnapshot);

      this._state.snapshots.set(cherryPickSnapshot.snapshotId, cherryPickSnapshot);
      this._state.currentSnapshotId = cherryPickSnapshot.snapshotId;

      await this.saveState();

      return {
        newSnapshotId: cherryPickSnapshot.snapshotId,
        sourceSnapshotId,
      };
    });
  }

  /**
   * Build a snapshot representing a cherry-pick operation.
   */
  private buildCherryPickSnapshot(sourceSnapshot: Snapshot): Snapshot {
    return {
      snapshotId: this.generateSnapshotId(),
      parentSnapshotId: this._state.currentSnapshotId,
      sequenceNumber: this.getNextSequenceNumber(),
      timestampMs: Date.now(),
      manifestList: sourceSnapshot.manifestList,
      summary: {
        ...sourceSnapshot.summary,
        'cherry-pick-source-snapshot-id': sourceSnapshot.snapshotId.toString(),
      },
      schemaId: sourceSnapshot.schemaId,
    };
  }

  // ==========================================================================
  // Configuration Accessors
  // ==========================================================================

  /**
   * Get table location.
   */
  getTableLocation(): string {
    return this._config.tableLocation ?? this._tableName;
  }

  /**
   * Check if caching is enabled.
   */
  isCacheEnabled(): boolean {
    return this._config.cacheEnabled ?? false;
  }
}
