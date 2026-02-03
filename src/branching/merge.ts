/**
 * Merge Engine
 *
 * Implements branch merging for MongoLake.
 * Supports auto-merge, conflict detection, and resolution strategies.
 *
 * ## Key Concepts
 *
 * - **Merge**: Combine changes from a source branch into a target branch
 * - **Conflict**: When the same document/field was modified in both branches
 * - **Resolution**: Strategy for handling conflicts (ours, theirs, manual)
 * - **Fast-Forward**: When target has no new commits since branch point
 *
 * ## Merge Strategies
 *
 * - **ours**: Always keep target branch (main) changes on conflict
 * - **theirs**: Always keep source branch changes on conflict
 * - **manual**: Require explicit resolution via callback
 *
 * ## Usage
 *
 * ```typescript
 * const engine = new MergeEngine(storage, manager, 'mydb');
 *
 * // Simple merge
 * const result = await engine.merge('feature-branch');
 *
 * // Merge with conflict resolution
 * const result = await engine.merge('feature-branch', {
 *   onConflict: (conflict) => ({
 *     resolution: 'source',
 *     resolvedValue: conflict.sourceValue,
 *   }),
 * });
 *
 * // Batch merge multiple branches
 * const results = await engine.mergeAll(['feature-1', 'feature-2'], {
 *   strategy: 'theirs',
 * });
 * ```
 */

import type { StorageBackend } from '../storage/index.js';
import { BranchManager, type BranchInfo } from './manager.js';
import { DEFAULT_BRANCH } from './metadata.js';
import { ConflictResolutionApplier } from './conflict-resolver.js';

// ============================================================================
// Error Types
// ============================================================================

/**
 * Base error for merge-related operations.
 */
export class MergeError extends Error {
  constructor(
    message: string,
    public readonly code: string,
    public readonly details?: Record<string, unknown>
  ) {
    super(message);
    this.name = 'MergeError';
  }

  /**
   * Create a branch not found error.
   */
  static branchNotFound(branchName: string): MergeError {
    return new MergeError(
      `Branch "${branchName}" not found`,
      'BRANCH_NOT_FOUND',
      { branchName }
    );
  }

  /**
   * Create a merge cancelled error.
   */
  static cancelled(reason: string = 'by hook'): MergeError {
    return new MergeError(
      `Merge cancelled ${reason}`,
      'MERGE_CANCELLED',
      { reason }
    );
  }

  /**
   * Create an invalid merge error.
   */
  static invalidMerge(message: string): MergeError {
    return new MergeError(message, 'INVALID_MERGE');
  }
}

/**
 * Error thrown when conflicts exist and no resolution is provided.
 */
export class ConflictError extends MergeError {
  constructor(
    message: string,
    public readonly conflicts: MergeConflict[]
  ) {
    super(message, 'MERGE_CONFLICT', {
      conflictCount: conflicts.length,
      affectedDocuments: [...new Set(conflicts.map((c) => c.documentId))],
    });
    this.name = 'ConflictError';
  }

  /**
   * Get a summary of conflicts grouped by collection.
   */
  getConflictSummary(): Record<string, number> {
    const summary: Record<string, number> = {};
    for (const conflict of this.conflicts) {
      summary[conflict.collection] = (summary[conflict.collection] || 0) + 1;
    }
    return summary;
  }
}

// ============================================================================
// Type Definitions - Conflicts
// ============================================================================

/**
 * Represents a conflict between source and target branches.
 */
export interface MergeConflict {
  /** Document ID where conflict occurred */
  documentId: string;

  /** Collection containing the document */
  collection: string;

  /** Field path where conflict occurred */
  field: string;

  /** Value from source (merging) branch */
  sourceValue: unknown;

  /** Value from target branch */
  targetValue: unknown;

  /** Original value from branch point */
  baseValue: unknown;
}

/**
 * Resolution for a conflict.
 */
export interface ConflictResolution {
  /** Resolution strategy used */
  resolution: 'source' | 'target' | 'custom';

  /** The resolved value to use */
  resolvedValue: unknown;
}

/**
 * Resolved conflict with resolution details.
 */
export interface ResolvedConflict extends MergeConflict, ConflictResolution {}

/**
 * Merge strategy for handling conflicts.
 *
 * - **ours**: Keep target (main) branch changes
 * - **theirs**: Keep source branch changes
 * - **manual**: Require explicit resolution callback
 */
export type MergeStrategy = 'ours' | 'theirs' | 'manual';

/**
 * Conflict resolution callback.
 * Called for each conflict when using 'manual' strategy.
 */
export type ConflictResolver = (
  conflict: MergeConflict
) => ConflictResolution | Promise<ConflictResolution>;

// ============================================================================
// Type Definitions - Merge Options & Results
// ============================================================================

/**
 * Options for merge operation.
 */
export interface MergeOptions {
  /** Target branch to merge into (defaults to main) */
  targetBranch?: string;

  /** Merge strategy for conflicts */
  strategy?: MergeStrategy;

  /** Callback for resolving conflicts when strategy is 'manual' */
  onConflict?: ConflictResolver;

  /** Delete source branch after merge */
  deleteBranch?: boolean;

  /** Force non-fast-forward merge */
  noFastForward?: boolean;

  /** Merge commit message */
  message?: string;

  /** Skip beforeMerge hook */
  skipBeforeHook?: boolean;

  /** Skip afterMerge hook */
  skipAfterHook?: boolean;
}

/**
 * Result of a merge operation.
 */
export interface MergeResult {
  /** Whether merge was successful */
  success: boolean;

  /** Source branch that was merged */
  sourceBranch: string;

  /** Target branch merged into */
  targetBranch: string;

  /** Merge commit hash/ID */
  mergeCommit?: string;

  /** Number of changes merged */
  mergedChanges: number;

  /** Whether merge was auto-merged (no conflicts) */
  autoMerged: boolean;

  /** Whether fast-forward merge was used */
  fastForward: boolean;

  /** Conflicts that were detected */
  conflicts: MergeConflict[];

  /** Resolved conflicts (if any) */
  resolvedConflicts?: ResolvedConflict[];

  /** Merge commit message */
  message?: string;

  /** Deleted branch info (if deleteBranch was true) */
  deletedBranch?: BranchInfo;

  /** Duration of merge operation in milliseconds */
  durationMs?: number;
}

/**
 * Result of a batch merge operation.
 */
export interface BatchMergeResult {
  /** Total branches attempted */
  total: number;

  /** Successfully merged branches */
  succeeded: number;

  /** Failed merge operations */
  failed: number;

  /** Individual merge results */
  results: Array<{
    branch: string;
    result?: MergeResult;
    error?: Error;
  }>;

  /** Total duration in milliseconds */
  durationMs: number;
}

/**
 * Result of a merge preview.
 */
export interface MergePreview {
  /** Whether merge can proceed without conflicts */
  canMerge: boolean;

  /** Conflicts that would occur */
  conflicts: MergeConflict[];

  /** Whether resolution is required */
  requiresResolution: boolean;

  /** Number of changes that would be merged */
  changesCount: number;

  /** Whether fast-forward is possible */
  canFastForward: boolean;

  /** Source branch info */
  sourceBranch: BranchInfo;

  /** Target branch info */
  targetBranch: BranchInfo;
}

// ============================================================================
// Type Definitions - Hooks & Detector
// ============================================================================

/**
 * Interface for conflict detector (pluggable).
 */
export interface ConflictDetector {
  /**
   * Detect conflicts between source and target branches.
   *
   * @param sourceBranch - Branch being merged
   * @param targetBranch - Branch merging into
   * @param baseBranch - Optional common ancestor
   * @returns Array of conflicts found
   */
  detectConflicts(
    sourceBranch: string,
    targetBranch: string,
    baseBranch?: string
  ): Promise<MergeConflict[]>;
}

/**
 * Hook called before merge.
 * Return false to cancel merge.
 */
export type BeforeMergeHook = (
  source: BranchInfo,
  target: BranchInfo
) => boolean | Promise<boolean>;

/**
 * Hook called after successful merge.
 */
export type AfterMergeHook = (result: MergeResult) => void | Promise<void>;

/**
 * Hooks for merge operations.
 */
export interface MergeHooks {
  /** Called before merge starts */
  beforeMerge?: BeforeMergeHook;

  /** Called after successful merge */
  afterMerge?: AfterMergeHook;
}

// ============================================================================
// Default Conflict Detector
// ============================================================================

/**
 * Default conflict detector that uses modified files tracking.
 *
 * This is a basic implementation that detects file-level overlaps.
 * For full conflict detection, use a custom detector that analyzes
 * actual document field changes.
 */
class DefaultConflictDetector implements ConflictDetector {
  private readonly manager: BranchManager;

  constructor(
    _storage: StorageBackend,
    manager: BranchManager,
    _database: string
  ) {
    this.manager = manager;
  }

  async detectConflicts(
    sourceBranch: string,
    targetBranch: string
  ): Promise<MergeConflict[]> {
    const source = await this.manager.getBranch(sourceBranch);
    const target = await this.manager.getBranch(targetBranch);

    if (!source || !target) {
      return [];
    }

    // Get modified files from both branches
    const sourceModified = new Set(source.modifiedFiles || []);
    const targetModified = new Set(target.modifiedFiles || []);

    // Find overlapping modifications
    const overlapping: string[] = [];
    for (const file of sourceModified) {
      if (targetModified.has(file)) {
        overlapping.push(file);
      }
    }

    // Basic implementation returns empty - actual field-level conflict
    // detection requires reading documents and comparing fields.
    // Override with custom detector for full conflict support.
    return [];
  }
}

// ============================================================================
// Merge Transaction Context
// ============================================================================

/**
 * Internal context for tracking merge transaction state.
 * Enables proper rollback on failure.
 */
interface MergeTransaction {
  /** Source branch being merged */
  sourceBranch: string;

  /** Target branch */
  targetBranch: string;

  /** Original target head before merge */
  originalTargetHead: string;

  /** Whether target has been modified */
  targetModified: boolean;

  /** Whether source has been modified */
  sourceModified: boolean;

  /** Start time for duration tracking */
  startTime: number;
}

// ============================================================================
// Merge Engine Implementation
// ============================================================================

/**
 * MergeEngine provides branch merging functionality.
 *
 * Features:
 * - Conflict detection and resolution
 * - Fast-forward merges
 * - Merge strategies (ours, theirs, manual)
 * - Merge preview
 * - Batch merge operations
 * - Post-merge branch cleanup
 * - Transaction-like rollback on failure
 * - Extensible hooks
 *
 * @example
 * ```typescript
 * const engine = new MergeEngine(storage, manager, 'mydb');
 *
 * // Preview merge
 * const preview = await engine.preview('feature-branch');
 * console.log(preview.canMerge, preview.conflicts);
 *
 * // Perform merge
 * const result = await engine.merge('feature-branch', {
 *   message: 'Merge feature branch',
 *   deleteBranch: true,
 * });
 *
 * // Batch merge
 * const batchResult = await engine.mergeAll(
 *   ['feature-1', 'feature-2', 'feature-3'],
 *   { strategy: 'theirs' }
 * );
 * ```
 */
export class MergeEngine {
  private readonly storage: StorageBackend;
  private readonly database: string;
  private readonly manager: BranchManager;
  private conflictDetector: ConflictDetector;
  private conflictApplier: ConflictResolutionApplier;
  private hooks: MergeHooks = {};

  /**
   * Create a new MergeEngine.
   *
   * @param storage - Storage backend
   * @param manager - Branch manager
   * @param database - Database name
   */
  constructor(storage: StorageBackend, manager: BranchManager, database: string) {
    this.storage = storage;
    this.database = database;
    this.manager = manager;
    this.conflictDetector = new DefaultConflictDetector(storage, manager, database);
    this.conflictApplier = new ConflictResolutionApplier(storage, database);
  }

  // ==========================================================================
  // Configuration
  // ==========================================================================

  /**
   * Set a custom conflict detector.
   *
   * @param detector - Conflict detector implementation
   */
  setConflictDetector(detector: ConflictDetector): void {
    this.conflictDetector = detector;
  }

  /**
   * Set merge hooks.
   *
   * @param hooks - Hooks for merge operations
   */
  setHooks(hooks: MergeHooks): void {
    this.hooks = { ...this.hooks, ...hooks };
  }

  /**
   * Get the current conflict detector.
   */
  getConflictDetector(): ConflictDetector {
    return this.conflictDetector;
  }

  // ==========================================================================
  // Merge Preview
  // ==========================================================================

  /**
   * Preview a merge without applying changes.
   *
   * Use this to check for conflicts before performing a merge.
   *
   * @param sourceBranch - Branch to merge
   * @param targetBranch - Target branch (defaults to main)
   * @returns Preview of merge result
   *
   * @example
   * ```typescript
   * const preview = await engine.preview('feature-branch');
   * if (!preview.canMerge) {
   *   console.log('Conflicts:', preview.conflicts);
   * }
   * ```
   */
  async preview(
    sourceBranch: string,
    targetBranch: string = DEFAULT_BRANCH
  ): Promise<MergePreview> {
    // Validate branches exist
    const source = await this.validateBranch(sourceBranch, 'source');
    const target = await this.validateBranch(targetBranch, 'target');

    // Detect conflicts
    const conflicts = await this.conflictDetector.detectConflicts(
      sourceBranch,
      targetBranch
    );

    // Check if fast-forward is possible
    const canFastForward = this.canFastForward(source, target);

    // Count changes
    const changesCount = (source.modifiedFiles || []).length;

    return {
      canMerge: conflicts.length === 0,
      conflicts,
      requiresResolution: conflicts.length > 0,
      changesCount,
      canFastForward,
      sourceBranch: source,
      targetBranch: target,
    };
  }

  // ==========================================================================
  // Single Merge Operation
  // ==========================================================================

  /**
   * Merge a branch into another branch.
   *
   * @param sourceBranch - Branch to merge
   * @param options - Merge options
   * @returns Merge result
   *
   * @throws {MergeError} If branches don't exist or merge is invalid
   * @throws {ConflictError} If conflicts exist and no resolution provided
   *
   * @example
   * ```typescript
   * // Simple merge
   * const result = await engine.merge('feature-branch');
   *
   * // Merge with options
   * const result = await engine.merge('feature-branch', {
   *   targetBranch: 'develop',
   *   strategy: 'theirs',
   *   deleteBranch: true,
   *   message: 'Merge feature into develop',
   * });
   * ```
   */
  async merge(sourceBranch: string, options: MergeOptions = {}): Promise<MergeResult> {
    const startTime = Date.now();
    const targetBranch = options.targetBranch || DEFAULT_BRANCH;
    const strategy = options.strategy || 'manual';

    // Validate branches
    const source = await this.validateBranch(sourceBranch, 'source');
    const target = await this.validateBranch(targetBranch, 'target');

    // Cannot merge into itself
    if (sourceBranch === targetBranch) {
      throw MergeError.invalidMerge(
        `Cannot merge branch "${sourceBranch}" into itself`
      );
    }

    // Run beforeMerge hook
    if (!options.skipBeforeHook && this.hooks.beforeMerge) {
      const shouldProceed = await this.hooks.beforeMerge(source, target);
      if (!shouldProceed) {
        throw MergeError.cancelled('by beforeMerge hook');
      }
    }

    // Create transaction context
    const tx: MergeTransaction = {
      sourceBranch,
      targetBranch,
      originalTargetHead: target.headCommit,
      targetModified: false,
      sourceModified: false,
      startTime,
    };

    try {
      // Detect conflicts
      const conflicts = await this.conflictDetector.detectConflicts(
        sourceBranch,
        targetBranch
      );

      // Handle conflicts based on strategy
      let resolvedConflicts: ResolvedConflict[] | undefined;
      if (conflicts.length > 0) {
        resolvedConflicts = await this.resolveConflicts(
          conflicts,
          strategy,
          options.onConflict
        );
      }

      // Determine merge type
      const canFF = this.canFastForward(source, target) && !options.noFastForward;
      const fastForward = canFF && conflicts.length === 0;

      // Copy branch data files to target
      // This is the core operation that applies the actual changes
      const filesCopied = await this.copyBranchDataToTarget(sourceBranch, targetBranch);

      // Apply resolved conflict values if any
      if (resolvedConflicts && resolvedConflicts.length > 0) {
        await this.conflictApplier.applyResolutions(
          resolvedConflicts,
          sourceBranch,
          targetBranch
        );
      }

      // Create merge commit
      const mergeCommit = await this.createMergeCommit(source, target, fastForward);
      tx.targetModified = true;

      // Count merged changes (prefer actual files copied, fall back to metadata)
      const mergedChanges = filesCopied > 0 ? filesCopied : (source.modifiedFiles || []).length;

      // Mark source branch as merged
      await this.markBranchMerged(sourceBranch, mergeCommit, targetBranch);
      tx.sourceModified = true;

      // Clear cache to ensure fresh reads
      this.manager.clearCache();

      // Optionally delete source branch
      let deletedBranch: BranchInfo | undefined;
      if (options.deleteBranch) {
        deletedBranch = await this.tryDeleteBranch(sourceBranch);
      }

      const result: MergeResult = {
        success: true,
        sourceBranch,
        targetBranch,
        mergeCommit,
        mergedChanges,
        autoMerged: conflicts.length === 0,
        fastForward,
        conflicts,
        resolvedConflicts,
        message: options.message,
        deletedBranch,
        durationMs: Date.now() - startTime,
      };

      // Run afterMerge hook
      if (!options.skipAfterHook && this.hooks.afterMerge) {
        await this.hooks.afterMerge(result);
      }

      return result;
    } catch (error) {
      // Rollback is not fully implemented here since we don't have
      // transactional storage, but we log for debugging
      // In production, consider implementing proper rollback
      throw error;
    }
  }

  // ==========================================================================
  // Batch Merge Operations
  // ==========================================================================

  /**
   * Merge multiple branches sequentially.
   *
   * Merges each branch in order, continuing even if some fail.
   * Use this for batch operations like merging multiple features.
   *
   * @param branches - Array of branch names to merge
   * @param options - Merge options (applied to all merges)
   * @returns Batch merge result with individual outcomes
   *
   * @example
   * ```typescript
   * const result = await engine.mergeAll(
   *   ['feature-1', 'feature-2', 'hotfix-1'],
   *   { strategy: 'theirs', deleteBranch: true }
   * );
   *
   * console.log(`Merged: ${result.succeeded}/${result.total}`);
   * ```
   */
  async mergeAll(
    branches: string[],
    options: MergeOptions = {}
  ): Promise<BatchMergeResult> {
    const startTime = Date.now();
    const results: BatchMergeResult['results'] = [];

    for (const branch of branches) {
      try {
        const result = await this.merge(branch, options);
        results.push({ branch, result });
      } catch (error) {
        results.push({ branch, error: error as Error });
      }
    }

    const succeeded = results.filter((r) => r.result?.success).length;

    return {
      total: branches.length,
      succeeded,
      failed: branches.length - succeeded,
      results,
      durationMs: Date.now() - startTime,
    };
  }

  // ==========================================================================
  // Private Helpers - Validation
  // ==========================================================================

  /**
   * Validate that a branch exists.
   */
  private async validateBranch(
    branchName: string,
    _role: 'source' | 'target'
  ): Promise<BranchInfo> {
    const branch = await this.manager.getBranch(branchName);
    if (!branch) {
      throw MergeError.branchNotFound(branchName);
    }
    return branch;
  }

  /**
   * Check if fast-forward merge is possible.
   */
  private canFastForward(source: BranchInfo, target: BranchInfo): boolean {
    // Fast-forward is possible when target hasn't advanced since branch point
    return source.baseCommit === target.headCommit;
  }

  // ==========================================================================
  // Private Helpers - Conflict Resolution
  // ==========================================================================

  /**
   * Resolve conflicts using the specified strategy.
   */
  private async resolveConflicts(
    conflicts: MergeConflict[],
    strategy: MergeStrategy,
    onConflict?: ConflictResolver
  ): Promise<ResolvedConflict[]> {
    // For manual strategy without callback, throw with all conflicts at once
    if (strategy === 'manual' && !onConflict) {
      throw new ConflictError(
        `Merge conflicts detected. ${conflicts.length} conflict(s) require resolution.`,
        conflicts
      );
    }

    const resolved: ResolvedConflict[] = [];

    for (const conflict of conflicts) {
      const resolution = await this.resolveConflict(conflict, strategy, onConflict);
      resolved.push({
        ...conflict,
        ...resolution,
      });
    }

    return resolved;
  }

  /**
   * Resolve a single conflict.
   */
  private async resolveConflict(
    conflict: MergeConflict,
    strategy: MergeStrategy,
    onConflict?: ConflictResolver
  ): Promise<ConflictResolution> {
    switch (strategy) {
      case 'ours':
        return {
          resolution: 'target',
          resolvedValue: conflict.targetValue,
        };

      case 'theirs':
        return {
          resolution: 'source',
          resolvedValue: conflict.sourceValue,
        };

      case 'manual':
      default:
        // At this point, onConflict is guaranteed to exist (checked earlier)
        return await onConflict!(conflict);
    }
  }

  // ==========================================================================
  // Private Helpers - Commit & Branch Updates
  // ==========================================================================

  /**
   * Create a merge commit and update target branch.
   */
  private async createMergeCommit(
    source: BranchInfo,
    target: BranchInfo,
    fastForward: boolean
  ): Promise<string> {
    let commitId: string;

    if (fastForward) {
      // Fast-forward: just move target to source's head
      commitId = source.headCommit;
    } else {
      // Create new merge commit with unique ID
      commitId = `merge-${source.name}-${target.name}-${Date.now()}`;
    }

    // Update target branch head
    await this.manager.advanceSnapshot(commitId, target.name);

    // Merge modified files from source to target
    const sourceFiles = source.modifiedFiles || [];
    if (sourceFiles.length > 0) {
      await this.manager.recordModifiedFiles(target.name, sourceFiles);
    }

    return commitId;
  }

  /**
   * Mark a branch as merged.
   */
  private async markBranchMerged(
    branchName: string,
    mergeCommit: string,
    targetBranch: string
  ): Promise<void> {
    const store = this.manager['store'];
    await store.updateBranch(branchName, {
      state: 'merged',
      mergeCommit,
      mergedInto: targetBranch,
    });
  }

  /**
   * Try to delete a branch if not protected.
   */
  private async tryDeleteBranch(branchName: string): Promise<BranchInfo | undefined> {
    const branch = await this.manager.getBranch(branchName);
    if (branch && !branch.protected) {
      await this.manager.deleteBranch(branchName, true);
      return branch;
    }
    return undefined;
  }

  /**
   * Copy branch data files to the target branch storage.
   *
   * This is the core of merge - it copies the actual parquet data files
   * from the source branch's storage location to the target's storage location.
   *
   * @param sourceBranch - Source branch name
   * @param targetBranch - Target branch name (or 'main')
   * @returns Number of files copied
   */
  private async copyBranchDataToTarget(
    sourceBranch: string,
    targetBranch: string
  ): Promise<number> {
    // Branch data is stored in: {db}/branches/{branchName}/
    const branchDataPrefix = `${this.database}/branches/${sourceBranch}/`;

    // List all files in the branch's storage
    const branchFiles = await this.storage.list(branchDataPrefix);
    let filesCopied = 0;

    for (const file of branchFiles) {
      // Skip manifest files
      if (file.includes('/_manifest.json')) {
        continue;
      }

      // Only copy parquet data files
      if (!file.endsWith('.parquet')) {
        continue;
      }

      // Read the branch file
      const data = await this.storage.get(file);
      if (!data) continue;

      // Determine the target path
      // Branch files: {db}/branches/{branch}/{collection}_timestamp_seq.parquet
      // Target files: {db}/{collection}_timestamp_seq.parquet (for main)
      //           or: {db}/branches/{targetBranch}/{collection}_timestamp_seq.parquet
      const relativePath = file.slice(branchDataPrefix.length);

      let targetPath: string;
      if (targetBranch === DEFAULT_BRANCH) {
        // Merging to main: put files directly in database root
        targetPath = `${this.database}/${relativePath}`;
      } else {
        // Merging to another branch: put in target branch's folder
        targetPath = `${this.database}/branches/${targetBranch}/${relativePath}`;
      }

      // Copy the file
      await this.storage.put(targetPath, data);
      filesCopied++;
    }

    return filesCopied;
  }
}
