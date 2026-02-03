/**
 * Branch Manager
 *
 * High-level API for Git-like branch operations in MongoLake.
 * Provides copy-on-write branching semantics for database isolation.
 *
 * ## Key Concepts
 *
 * - **Branch**: An isolated view of the database at a specific snapshot
 * - **Snapshot**: A point-in-time state of the database (like a Git commit)
 * - **Copy-on-Write**: Branches share data until modified, then create isolated copies
 *
 * ## Usage
 *
 * ```typescript
 * const manager = new BranchManager(storage, 'mydb');
 * await manager.initialize('initial-snapshot');
 *
 * // Create a branch
 * const branch = await manager.createBranch('feature-branch');
 *
 * // Switch to branch
 * await manager.checkout('feature-branch');
 *
 * // Work on branch, then list all branches
 * const branches = await manager.listBranches();
 * ```
 */

import type { StorageBackend } from '../storage/index.js';
import {
  BranchStore,
  isValidBranchName,
  normalizeBranchName,
  DEFAULT_BRANCH,
  type BranchMetadata,
  type CreateBranchOptions as StoreBranchOptions,
  type ListBranchesOptions,
} from './metadata.js';

// ============================================================================
// Error Types
// ============================================================================

/**
 * Base error for branch-related operations.
 */
export class BranchError extends Error {
  constructor(message: string, public readonly code: string) {
    super(message);
    this.name = 'BranchError';
  }
}

/**
 * Error thrown when branch validation fails.
 */
export class BranchValidationError extends BranchError {
  constructor(message: string, public readonly branchName: string) {
    super(message, 'BRANCH_VALIDATION_ERROR');
    this.name = 'BranchValidationError';
  }
}

/**
 * Error thrown when a branch already exists.
 */
export class BranchExistsError extends BranchError {
  constructor(branchName: string) {
    super(`Branch "${branchName}" already exists`, 'BRANCH_EXISTS');
    this.name = 'BranchExistsError';
  }
}

/**
 * Error thrown when a branch is not found.
 */
export class BranchNotFoundError extends BranchError {
  constructor(branchName: string) {
    super(`Branch "${branchName}" not found`, 'BRANCH_NOT_FOUND');
    this.name = 'BranchNotFoundError';
  }
}

/**
 * Error thrown when a snapshot is not found.
 */
export class SnapshotNotFoundError extends BranchError {
  constructor(snapshotId: string) {
    super(`Snapshot "${snapshotId}" not found`, 'SNAPSHOT_NOT_FOUND');
    this.name = 'SnapshotNotFoundError';
  }
}

/**
 * Error thrown when the manager is not initialized.
 */
export class NotInitializedError extends BranchError {
  constructor() {
    super('BranchManager not initialized. Call initialize() first.', 'NOT_INITIALIZED');
    this.name = 'NotInitializedError';
  }
}

// ============================================================================
// Type Definitions
// ============================================================================

/**
 * Options for creating a new branch.
 */
export interface BranchCreateOptions {
  /** Optional description of the branch */
  description?: string;

  /** User or system that created the branch */
  createdBy?: string;

  /** Custom metadata for the branch */
  metadata?: Record<string, unknown>;

  /** Create branch from a specific snapshot ID */
  fromSnapshotId?: string;

  /** Base commit to branch from (alternative to fromSnapshotId) */
  baseCommit?: string;

  /** Parent branch to create from (defaults to current branch) */
  parentBranch?: string;

  /** Whether to checkout the branch immediately after creation */
  checkout?: boolean;
}

/**
 * Information about a branch.
 */
export interface BranchInfo {
  /** Branch name */
  name: string;

  /** Base commit hash/ID this branch was created from */
  baseCommit: string;

  /** Current head commit of this branch */
  headCommit: string;

  /** Timestamp when the branch was created */
  createdAt: string;

  /** Last time the branch was updated */
  updatedAt: string;

  /** Optional description of the branch */
  description?: string;

  /** User or system that created the branch */
  createdBy?: string;

  /** Current state of the branch */
  state: 'active' | 'merged' | 'deleted';

  /** Parent branch name (null for main branch) */
  parentBranch: string | null;

  /** Snapshot ID this branch was created from */
  fromSnapshotId?: number | string;

  /** Whether this branch is protected from deletion */
  protected?: boolean;

  /** Custom metadata for the branch */
  metadata?: Record<string, unknown>;

  /** Files that have been modified on this branch */
  modifiedFiles?: string[];

  /** Merge commit if this branch has been merged */
  mergeCommit?: string;

  /** Branch this was merged into */
  mergedInto?: string;
}

/**
 * Snapshot information.
 */
export interface SnapshotInfo {
  /** Snapshot ID */
  id: string;

  /** Timestamp when snapshot was created */
  createdAt: string;

  /** Branch the snapshot belongs to */
  branch: string;
}

/**
 * Hook called before a branch is created.
 * Return false to cancel branch creation.
 */
export type BeforeCreateHook = (name: string, options: BranchCreateOptions) => boolean | Promise<boolean>;

/**
 * Hook called after a branch is created.
 */
export type AfterCreateHook = (branch: BranchInfo) => void | Promise<void>;

/**
 * Configuration options for BranchManager.
 */
export interface BranchManagerConfig {
  /** Hook called before branch creation */
  beforeCreate?: BeforeCreateHook;

  /** Hook called after branch creation */
  afterCreate?: AfterCreateHook;

  /** Whether to cache branch lookups (default: true) */
  enableCache?: boolean;

  /** Cache TTL in milliseconds (default: 60000) */
  cacheTtlMs?: number;
}

// ============================================================================
// Branch Factory
// ============================================================================

/**
 * Factory for creating branch store options from manager options.
 *
 * This extracts the branch creation logic to make it testable and reusable.
 */
export class BranchFactory {
  /**
   * Create store options from manager options.
   *
   * @param normalizedName - Normalized branch name
   * @param baseCommit - Base commit ID
   * @param parentBranch - Parent branch name
   * @param options - User-provided options
   * @returns Options for BranchStore.createBranch
   */
  static createStoreOptions(
    normalizedName: string,
    baseCommit: string,
    parentBranch: string,
    options: BranchCreateOptions
  ): StoreBranchOptions {
    return {
      name: normalizedName,
      baseCommit,
      description: options.description,
      createdBy: options.createdBy,
      metadata: options.metadata,
      fromSnapshotId: BranchFactory.parseSnapshotId(options.fromSnapshotId),
      parentBranch,
    };
  }

  /**
   * Parse a snapshot ID to number if possible.
   */
  private static parseSnapshotId(snapshotId: string | undefined): number | undefined {
    if (!snapshotId) return undefined;
    const parsed = parseInt(snapshotId, 10);
    return isNaN(parsed) ? undefined : parsed;
  }
}

// ============================================================================
// Branch Cache
// ============================================================================

/**
 * Simple cache for branch lookups.
 */
class BranchCache {
  private cache: Map<string, { info: BranchInfo; expiry: number }> = new Map();
  private readonly ttlMs: number;

  constructor(ttlMs: number = 60000) {
    this.ttlMs = ttlMs;
  }

  get(name: string): BranchInfo | null {
    const entry = this.cache.get(name);
    if (!entry) return null;
    if (Date.now() > entry.expiry) {
      this.cache.delete(name);
      return null;
    }
    return entry.info;
  }

  set(name: string, info: BranchInfo): void {
    this.cache.set(name, {
      info,
      expiry: Date.now() + this.ttlMs,
    });
  }

  invalidate(name: string): void {
    this.cache.delete(name);
  }

  clear(): void {
    this.cache.clear();
  }
}

// ============================================================================
// Branch Manager Implementation
// ============================================================================

/**
 * BranchManager provides a high-level API for Git-like branch operations.
 *
 * It wraps BranchStore to provide additional functionality:
 * - Current branch tracking
 * - Snapshot management
 * - Copy-on-write file tracking
 * - Branch ancestry tracking
 * - Caching for performance
 * - Hooks for extensibility
 *
 * @example
 * ```typescript
 * const manager = new BranchManager(storage, 'mydb', {
 *   beforeCreate: (name) => {
 *     console.log(`Creating branch: ${name}`);
 *     return true; // Allow creation
 *   },
 *   afterCreate: (branch) => {
 *     console.log(`Created branch: ${branch.name}`);
 *   },
 * });
 *
 * await manager.initialize('initial-snapshot');
 * const branch = await manager.createBranch('feature');
 * ```
 */
export class BranchManager {
  private readonly store: BranchStore;
  private readonly storage: StorageBackend;
  private readonly database: string;
  private readonly config: BranchManagerConfig;
  private readonly cache: BranchCache;

  /** Whether the manager has been initialized */
  private initialized: boolean = false;

  /** Current active branch */
  private currentBranch: string = DEFAULT_BRANCH;

  /** Known snapshots for validation */
  private snapshots: Map<string, SnapshotInfo> = new Map();

  /**
   * Create a new BranchManager.
   *
   * @param storage - Storage backend for persisting branch data
   * @param database - Database name
   * @param config - Optional configuration
   */
  constructor(storage: StorageBackend, database: string, config: BranchManagerConfig = {}) {
    this.storage = storage;
    this.database = database;
    this.config = {
      enableCache: true,
      cacheTtlMs: 60000,
      ...config,
    };
    this.store = new BranchStore(storage, database);
    this.cache = new BranchCache(this.config.cacheTtlMs);
  }

  // ==========================================================================
  // Initialization
  // ==========================================================================

  /**
   * Initialize the branch manager with an initial snapshot.
   *
   * This creates the default branch (main) if it doesn't exist.
   * The method is idempotent - calling it multiple times with
   * different snapshots will not reinitialize if already initialized.
   *
   * @param initialSnapshot - The initial snapshot ID
   */
  async initialize(initialSnapshot: string): Promise<void> {
    if (this.initialized) {
      return;
    }

    // Initialize the default branch
    await this.store.initializeDefaultBranch(initialSnapshot);

    // Register the initial snapshot
    this.registerSnapshot(initialSnapshot, DEFAULT_BRANCH);

    // Load existing snapshots from storage
    await this.loadSnapshots();

    this.initialized = true;
  }

  /**
   * Check if the manager is initialized.
   *
   * @returns True if initialized
   */
  isInitialized(): boolean {
    return this.initialized;
  }

  /**
   * Ensure the manager is initialized, throwing if not.
   */
  private ensureInitialized(): void {
    if (!this.initialized) {
      throw new NotInitializedError();
    }
  }

  // ==========================================================================
  // Branch Creation
  // ==========================================================================

  /**
   * Create a new branch.
   *
   * @param name - Branch name
   * @param options - Branch creation options
   * @returns Created branch info
   * @throws BranchValidationError if branch name is invalid
   * @throws BranchExistsError if branch already exists
   * @throws BranchNotFoundError if parent branch not found
   * @throws SnapshotNotFoundError if specified snapshot not found
   */
  async createBranch(name: string, options: BranchCreateOptions = {}): Promise<BranchInfo> {
    this.ensureInitialized();

    // Normalize and validate name
    const normalizedName = normalizeBranchName(name);

    if (!isValidBranchName(normalizedName)) {
      throw new BranchValidationError(
        `Invalid branch name: "${name}". Branch names must be alphanumeric with hyphens, underscores, or slashes, and cannot be reserved names.`,
        name
      );
    }

    // Run before create hook
    if (this.config.beforeCreate) {
      const shouldProceed = await this.config.beforeCreate(normalizedName, options);
      if (!shouldProceed) {
        throw new BranchError('Branch creation cancelled by beforeCreate hook', 'CREATION_CANCELLED');
      }
    }

    // Resolve base commit and parent branch
    const { baseCommit, parentBranch } = await this.resolveBaseCommit(options);

    // Create branch using store
    const storeOptions = BranchFactory.createStoreOptions(
      normalizedName,
      baseCommit,
      parentBranch,
      options
    );

    try {
      const metadata = await this.store.createBranch(storeOptions);
      const branchInfo = this.metadataToInfo(metadata);

      // Cache the new branch
      if (this.config.enableCache) {
        this.cache.set(normalizedName, branchInfo);
      }

      // Checkout if requested
      if (options.checkout) {
        this.currentBranch = normalizedName;
      }

      // Run after create hook
      if (this.config.afterCreate) {
        await this.config.afterCreate(branchInfo);
      }

      return branchInfo;
    } catch (error) {
      // Wrap known errors with better types
      if (error instanceof Error && error.message.includes('already exists')) {
        throw new BranchExistsError(normalizedName);
      }
      throw error;
    }
  }

  /**
   * Resolve the base commit from options.
   *
   * @param options - Branch creation options
   * @returns Base commit and parent branch
   */
  private async resolveBaseCommit(
    options: BranchCreateOptions
  ): Promise<{ baseCommit: string; parentBranch: string }> {
    let baseCommit = options.baseCommit || options.fromSnapshotId;
    const parentBranch = options.parentBranch || this.currentBranch;

    // Validate snapshot if specified
    if (options.fromSnapshotId && !this.snapshots.has(options.fromSnapshotId)) {
      throw new SnapshotNotFoundError(options.fromSnapshotId);
    }

    // Get base commit from parent branch if not specified
    if (!baseCommit) {
      const parent = await this.store.getBranch(parentBranch);
      if (!parent) {
        throw new BranchNotFoundError(parentBranch);
      }
      baseCommit = parent.headCommit;
    }

    return { baseCommit, parentBranch };
  }

  // ==========================================================================
  // Branch Retrieval
  // ==========================================================================

  /**
   * Get branch information by name.
   *
   * @param name - Branch name
   * @returns Branch info or null if not found
   */
  async getBranch(name: string): Promise<BranchInfo | null> {
    this.ensureInitialized();

    // Check cache first
    if (this.config.enableCache) {
      const cached = this.cache.get(name);
      if (cached) return cached;
    }

    const metadata = await this.store.getBranch(name);
    if (!metadata) {
      return null;
    }

    const info = this.metadataToInfo(metadata);

    // Cache the result
    if (this.config.enableCache) {
      this.cache.set(name, info);
    }

    return info;
  }

  /**
   * List all branches.
   *
   * @param options - List options for filtering and sorting
   * @returns Array of branch info
   */
  async listBranches(options: ListBranchesOptions = {}): Promise<BranchInfo[]> {
    this.ensureInitialized();

    const branches = await this.store.listBranches(options);
    return branches.map(b => this.metadataToInfo(b));
  }

  // ==========================================================================
  // Branch Operations
  // ==========================================================================

  /**
   * Checkout (switch to) a branch.
   *
   * @param name - Branch name to checkout
   * @returns Branch info
   * @throws BranchNotFoundError if branch not found
   */
  async checkout(name: string): Promise<BranchInfo> {
    this.ensureInitialized();

    const branch = await this.store.getBranch(name);
    if (!branch) {
      throw new BranchNotFoundError(name);
    }

    this.currentBranch = name;
    return this.metadataToInfo(branch);
  }

  /**
   * Delete a branch.
   *
   * @param name - Branch name to delete
   * @param force - Force delete even if protected
   * @returns Whether the branch was deleted
   */
  async deleteBranch(name: string, force: boolean = false): Promise<boolean> {
    this.ensureInitialized();

    const result = await this.store.deleteBranch(name, force);

    // Invalidate cache
    if (this.config.enableCache && result.deleted) {
      this.cache.invalidate(name);
    }

    return result.deleted;
  }

  /**
   * Get the current branch name.
   */
  getCurrentBranch(): string {
    return this.currentBranch;
  }

  // ==========================================================================
  // Copy-on-Write Operations
  // ==========================================================================

  /**
   * Record a modified file on a branch.
   *
   * This is used to track copy-on-write semantics - only files that
   * have been modified are stored separately per branch.
   *
   * @param branchName - Branch name
   * @param filePath - Path to the modified file
   */
  async recordModifiedFile(branchName: string, filePath: string): Promise<void> {
    this.ensureInitialized();

    await this.store.updateBranch(branchName, {
      addModifiedFiles: [filePath],
    });

    // Invalidate cache for this branch
    if (this.config.enableCache) {
      this.cache.invalidate(branchName);
    }
  }

  /**
   * Record multiple modified files on a branch.
   *
   * @param branchName - Branch name
   * @param filePaths - Paths to the modified files
   */
  async recordModifiedFiles(branchName: string, filePaths: string[]): Promise<void> {
    this.ensureInitialized();

    await this.store.updateBranch(branchName, {
      addModifiedFiles: filePaths,
    });

    // Invalidate cache for this branch
    if (this.config.enableCache) {
      this.cache.invalidate(branchName);
    }
  }

  /**
   * Get the ancestry of a branch (path to root).
   *
   * @param branchName - Branch name
   * @returns Array of ancestor branch names (parent to root)
   */
  async getBranchAncestry(branchName: string): Promise<string[]> {
    this.ensureInitialized();

    const ancestry: string[] = [];
    let currentName: string | null = branchName;
    const visited = new Set<string>(); // Prevent infinite loops

    while (currentName && !visited.has(currentName)) {
      visited.add(currentName);

      const branch = await this.store.getBranch(currentName);
      if (!branch) {
        break;
      }

      if (branch.parentBranch) {
        ancestry.push(branch.parentBranch);
      }
      currentName = branch.parentBranch;
    }

    return ancestry;
  }

  // ==========================================================================
  // Snapshot Management
  // ==========================================================================

  /**
   * Advance the snapshot on a branch.
   *
   * @param snapshotId - New snapshot ID
   * @param branchName - Branch to advance (defaults to current)
   */
  async advanceSnapshot(snapshotId: string, branchName?: string): Promise<void> {
    this.ensureInitialized();

    const targetBranch = branchName || this.currentBranch;

    // Update the branch head commit
    await this.store.updateBranch(targetBranch, {
      headCommit: snapshotId,
    });

    // Register the snapshot
    this.registerSnapshot(snapshotId, targetBranch);

    // Persist snapshot to storage
    await this.persistSnapshot(snapshotId, targetBranch);

    // Invalidate cache for this branch
    if (this.config.enableCache) {
      this.cache.invalidate(targetBranch);
    }
  }

  /**
   * Check if a snapshot exists.
   *
   * @param snapshotId - Snapshot ID to check
   */
  async snapshotExists(snapshotId: string): Promise<boolean> {
    return this.snapshots.has(snapshotId);
  }

  /**
   * List all known snapshots.
   */
  async listSnapshots(): Promise<SnapshotInfo[]> {
    return Array.from(this.snapshots.values());
  }

  // ==========================================================================
  // Cache Management
  // ==========================================================================

  /**
   * Clear the branch cache.
   */
  clearCache(): void {
    this.cache.clear();
  }

  // ==========================================================================
  // Private Helpers
  // ==========================================================================

  /**
   * Convert BranchMetadata to BranchInfo.
   */
  private metadataToInfo(metadata: BranchMetadata): BranchInfo {
    return {
      name: metadata.name,
      baseCommit: metadata.baseCommit,
      headCommit: metadata.headCommit,
      createdAt: metadata.createdAt,
      updatedAt: metadata.updatedAt,
      description: metadata.description,
      createdBy: metadata.createdBy,
      state: metadata.state,
      parentBranch: metadata.parentBranch,
      fromSnapshotId: metadata.fromSnapshotId,
      protected: metadata.protected,
      metadata: metadata.metadata,
      modifiedFiles: metadata.modifiedFiles,
      mergeCommit: metadata.mergeCommit,
      mergedInto: metadata.mergedInto,
    };
  }

  /**
   * Register a snapshot in the internal map.
   */
  private registerSnapshot(snapshotId: string, branch: string): void {
    this.snapshots.set(snapshotId, {
      id: snapshotId,
      createdAt: new Date().toISOString(),
      branch,
    });
  }

  /**
   * Load existing snapshots from storage.
   */
  private async loadSnapshots(): Promise<void> {
    const snapshotsPrefix = `${this.database}/snapshots/`;

    try {
      const files = await this.storage.list(snapshotsPrefix);

      for (const file of files) {
        if (file.endsWith('.json')) {
          try {
            const data = await this.storage.get(file);
            if (data) {
              const snapshot = JSON.parse(new TextDecoder().decode(data)) as SnapshotInfo;
              this.snapshots.set(snapshot.id, snapshot);
            }
          } catch {
            // Skip invalid snapshot files
          }
        }
      }
    } catch {
      // Storage list may fail if prefix doesn't exist - that's OK
    }

    // Also load snapshots from branch head commits
    const branches = await this.store.listBranches();
    for (const branch of branches) {
      if (branch.headCommit && !this.snapshots.has(branch.headCommit)) {
        this.registerSnapshot(branch.headCommit, branch.name);
      }
      if (branch.baseCommit && !this.snapshots.has(branch.baseCommit)) {
        this.registerSnapshot(branch.baseCommit, branch.name);
      }
    }
  }

  /**
   * Persist a snapshot to storage.
   */
  private async persistSnapshot(snapshotId: string, branch: string): Promise<void> {
    const snapshot: SnapshotInfo = {
      id: snapshotId,
      createdAt: new Date().toISOString(),
      branch,
    };

    const path = `${this.database}/snapshots/${snapshotId}.json`;
    const data = new TextEncoder().encode(JSON.stringify(snapshot, null, 2));
    await this.storage.put(path, data);
  }
}
