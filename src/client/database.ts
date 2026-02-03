/**
 * MongoLake Database
 *
 * MongoDB-compatible database class for managing collections.
 */

import type {
  Document,
  MongoLakeConfig,
  CollectionSchema,
} from '@types';
import type { StorageBackend } from '@storage/index.js';
import { validateCollectionName } from '@utils/validation.js';
import {
  BranchStore,
  BranchManager,
  MergeEngine,
  type BranchMetadata,
  type CreateBranchOptions,
  type ListBranchesOptions,
  type MergeOptions,
  type MergeResult,
} from '@mongolake/branching/index.js';
import {
  DiffGenerator,
  type DiffResult,
  type DiffOptions,
} from '@mongolake/branching/diff.js';
import { Collection } from './collection.js';
import { BranchCollection } from './branch-collection.js';

// ============================================================================
// Collection Options
// ============================================================================

/**
 * Options for getting a collection
 */
export interface CollectionOptions {
  /** Branch to access the collection on */
  branch?: string;
}

// ============================================================================
// Database
// ============================================================================

export class Database {
  private collections: Map<string, Collection<Document>> = new Map();
  private branchCollections: Map<string, BranchCollection<Document>> = new Map();
  private branchStore: BranchStore;
  private branchManager: BranchManager | null = null;
  private branchManagerInitialized: boolean = false;

  constructor(
    public readonly name: string,
    private storage: StorageBackend,
    private config: MongoLakeConfig
  ) {
    this.branchStore = new BranchStore(storage, name);
  }

  /**
   * Get or create the branch manager (lazy initialization)
   * @internal
   */
  private async getBranchManager(): Promise<BranchManager> {
    if (!this.branchManager) {
      this.branchManager = new BranchManager(this.storage, this.name);
    }

    if (!this.branchManagerInitialized) {
      // Initialize with a default snapshot based on current time
      const initialSnapshot = `snapshot-${Date.now()}`;
      await this.branchManager.initialize(initialSnapshot);
      this.branchManagerInitialized = true;
    }

    return this.branchManager;
  }

  /**
   * Get the branch store for this database
   * @internal
   */
  getBranchStore(): BranchStore {
    return this.branchStore;
  }

  /**
   * Get a collection
   */
  collection<T extends Document = Document>(name: string, options?: CollectionOptions): Collection<T> | BranchCollection<T> {
    // Validate collection name to prevent path traversal attacks
    validateCollectionName(name);

    // If branch option is provided, return a branch-aware collection
    if (options?.branch) {
      const branchKey = `${name}:${options.branch}`;
      if (!this.branchCollections.has(branchKey)) {
        const schema = this.config.schema?.[name];
        this.branchCollections.set(
          branchKey,
          new BranchCollection<Document>(name, this, this.storage, options.branch, schema)
        );
      }
      // Collection<Document> can be safely cast to Collection<T> when T extends Document.
      // Double cast is required due to TypeScript's strict variance checking.
      return this.branchCollections.get(branchKey) as unknown as BranchCollection<T>;
    }

    // Return regular collection for main branch
    if (!this.collections.has(name)) {
      const schema = this.config.schema?.[name];
      this.collections.set(name, new Collection<Document>(name, this, this.storage, schema));
    }
    // Collection<Document> can be safely cast to Collection<T> when T extends Document.
    // Double cast is required due to TypeScript's strict variance checking.
    return this.collections.get(name) as unknown as Collection<T>;
  }

  /**
   * List all collections
   */
  async listCollections(): Promise<string[]> {
    const files = await this.storage.list(`${this.name}/`);
    const collections = new Set<string>();

    for (const file of files) {
      // Extract collection name from path
      const match = file.match(new RegExp(`^${this.name}/([^/_][^/]*?)(?:_\\d+)?\\.parquet$`));
      if (match) {
        collections.add(match[1]!);
      }
    }

    return Array.from(collections);
  }

  /**
   * Create a collection
   */
  async createCollection<T extends Document = Document>(
    name: string,
    _options?: { schema?: CollectionSchema }
  ): Promise<Collection<T>> {
    const collection = this.collection<T>(name);
    // Initialize manifest if needed
    await collection.ensureManifest();
    return collection;
  }

  /**
   * Drop a collection
   */
  async dropCollection(name: string): Promise<boolean> {
    // Validate collection name to prevent path traversal attacks
    validateCollectionName(name);

    const files = await this.storage.list(`${this.name}/`);
    let dropped = false;

    for (const file of files) {
      if (file.startsWith(`${this.name}/${name}.`) || file.startsWith(`${this.name}/${name}_`)) {
        await this.storage.delete(file);
        dropped = true;
      }
    }

    this.collections.delete(name);
    return dropped;
  }

  /**
   * Create a branch from the current database state.
   *
   * Creates an isolated copy-on-write branch of the database.
   * The branch shares data with the parent until modifications are made.
   *
   * @param branchName - Name for the new branch
   * @param options - Branch creation options
   * @returns The created branch metadata
   *
   * @example
   * ```typescript
   * // Create a simple branch
   * const branch = await db.branch('feature-branch');
   *
   * // Create with options
   * const branch = await db.branch('feature-branch', {
   *   description: 'Working on new feature',
   *   createdBy: 'developer@example.com',
   * });
   * ```
   */
  async branch(branchName: string, options?: Omit<CreateBranchOptions, 'name'>): Promise<BranchMetadata> {
    const manager = await this.getBranchManager();
    // Convert CreateBranchOptions to BranchCreateOptions
    // Note: fromSnapshotId type differs between interfaces (number vs string)
    const managerOptions = options ? {
      description: options.description,
      createdBy: options.createdBy,
      metadata: options.metadata,
      parentBranch: options.parentBranch,
      baseCommit: options.baseCommit,
      fromSnapshotId: options.fromSnapshotId !== undefined ? String(options.fromSnapshotId) : undefined,
    } : undefined;
    const branchInfo = await manager.createBranch(branchName, managerOptions);

    // Convert BranchInfo to BranchMetadata format
    return {
      name: branchInfo.name,
      baseCommit: branchInfo.baseCommit,
      headCommit: branchInfo.headCommit,
      createdAt: branchInfo.createdAt,
      updatedAt: branchInfo.updatedAt,
      state: branchInfo.state,
      parentBranch: branchInfo.parentBranch,
      branchSequence: 0,
      description: branchInfo.description,
      createdBy: branchInfo.createdBy,
      protected: branchInfo.protected,
      metadata: branchInfo.metadata,
      modifiedFiles: branchInfo.modifiedFiles,
      mergeCommit: branchInfo.mergeCommit,
      mergedInto: branchInfo.mergedInto,
    };
  }

  /**
   * Merge a branch into a target branch.
   *
   * Combines changes from the source branch into the target branch (defaults to main).
   * Supports conflict detection and various merge strategies.
   *
   * @param sourceBranch - Branch to merge from
   * @param targetBranch - Branch to merge into (defaults to 'main')
   * @param options - Merge options including strategy and conflict resolution
   * @returns The merge result
   *
   * @example
   * ```typescript
   * // Simple merge to main
   * const result = await db.merge('feature-branch');
   *
   * // Merge with options
   * const result = await db.merge('feature-branch', 'develop', {
   *   strategy: 'theirs',
   *   deleteBranch: true,
   *   message: 'Merge feature into develop',
   * });
   * ```
   */
  async merge(sourceBranch: string, targetBranch?: string, options?: MergeOptions): Promise<MergeResult> {
    const manager = await this.getBranchManager();
    const mergeEngine = new MergeEngine(this.storage, manager, this.name);

    return mergeEngine.merge(sourceBranch, {
      ...options,
      targetBranch: targetBranch ?? options?.targetBranch,
    });
  }

  /**
   * Get branch metadata by name.
   *
   * @param branchName - Branch name to look up
   * @returns Branch metadata or null if not found
   *
   * @example
   * ```typescript
   * const branch = await db.getBranch('feature-branch');
   * if (branch) {
   *   console.log(`Head commit: ${branch.headCommit}`);
   * }
   * ```
   */
  async getBranch(branchName: string): Promise<BranchMetadata | null> {
    return this.branchStore.getBranch(branchName);
  }

  /**
   * Delete a branch.
   *
   * Protected branches require force=true to delete.
   * The default 'main' branch cannot be deleted.
   *
   * @param branchName - Branch name to delete
   * @param force - Force delete even if protected (default: false)
   * @returns True if deleted, false otherwise
   *
   * @example
   * ```typescript
   * const deleted = await db.deleteBranch('feature-branch');
   * if (!deleted) {
   *   console.error('Branch could not be deleted');
   * }
   * ```
   */
  async deleteBranch(branchName: string, force: boolean = false): Promise<boolean> {
    const result = await this.branchStore.deleteBranch(branchName, force);
    return result.deleted;
  }

  /**
   * List all branches in the database.
   *
   * @param options - Options for filtering and sorting branches
   * @returns Array of branch metadata
   *
   * @example
   * ```typescript
   * // List all branches
   * const branches = await db.listBranches();
   *
   * // List feature branches, newest first
   * const branches = await db.listBranches({
   *   prefix: 'feature/',
   *   sortBy: 'createdAt',
   *   sortOrder: 'desc',
   * });
   * ```
   */
  async listBranches(options?: ListBranchesOptions): Promise<BranchMetadata[]> {
    return this.branchStore.listBranches(options);
  }

  /**
   * Get storage path for this database
   */
  getPath(): string {
    return this.name;
  }

  /**
   * Generate a diff between a branch and its base.
   *
   * Shows all documents that were inserted, updated, or deleted on the branch
   * compared to the state at the time the branch was created.
   *
   * @param branchName - The branch to diff
   * @param options - Options for filtering the diff
   * @returns The diff result containing all changes
   *
   * @example
   * ```typescript
   * const diff = await db.diff('feature-branch');
   *
   * console.log(`Inserted: ${diff.summary.insertedCount}`);
   * console.log(`Updated: ${diff.summary.updatedCount}`);
   * console.log(`Deleted: ${diff.summary.deletedCount}`);
   *
   * for (const change of diff.updated) {
   *   console.log(`${change.documentId}: ${change.changedFields.join(', ')}`);
   * }
   * ```
   */
  async diff<T extends Document = Document>(
    branchName: string,
    options?: DiffOptions
  ): Promise<DiffResult<T>> {
    const diffGenerator = new DiffGenerator(this.storage, this.name, this.branchStore);
    return diffGenerator.diff<T>(branchName, options);
  }
}
