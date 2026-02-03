/**
 * Branch Metadata Storage
 *
 * Manages branch metadata for database branching feature.
 * Branches allow creating isolated copies of database state
 * for testing, development, or feature work.
 *
 * Storage format: .mongolake/branches/<branch-name>.json
 *
 * ## Copy-on-Write Semantics
 *
 * Branches use copy-on-write for efficient storage:
 * - Reading from a branch falls through to the parent if data hasn't been modified
 * - Writing to a branch creates isolated copies of only the modified data
 * - Shared Parquet files are referenced, not duplicated
 * - Each branch maintains its own WAL entries and sequence numbers
 *
 * ## Storage Format
 *
 * Branch metadata is stored as JSON with the following structure:
 * ```json
 * {
 *   "version": 1,
 *   "name": "feature-branch",
 *   "baseCommit": "abc123",
 *   "headCommit": "def456",
 *   "createdAt": "2024-01-01T00:00:00.000Z",
 *   "updatedAt": "2024-01-02T00:00:00.000Z",
 *   "state": "active",
 *   "parentBranch": "main",
 *   "branchSequence": 1
 * }
 * ```
 *
 * @module branching/metadata
 */

import type { StorageBackend } from '../storage/index.js';

// ============================================================================
// Branded Types for Type Safety
// ============================================================================

/**
 * Branded type for branch names.
 * Use {@link toBranchName} to create a validated BranchName.
 *
 * @example
 * ```typescript
 * const name = toBranchName('feature-branch'); // BranchName
 * const invalid = 'HEAD' as BranchName; // Compiles but unsafe
 * ```
 */
export type BranchName = string & { readonly __brand: 'BranchName' };

/**
 * Branded type for commit identifiers.
 * Use {@link toCommitId} to create a validated CommitId.
 *
 * @example
 * ```typescript
 * const commit = toCommitId('abc123'); // CommitId
 * ```
 */
export type CommitId = string & { readonly __brand: 'CommitId' };

/**
 * Convert a string to a validated BranchName.
 *
 * @param name - The branch name to validate
 * @returns A branded BranchName if valid
 * @throws Error if the branch name is invalid
 *
 * @example
 * ```typescript
 * const name = toBranchName('feature-branch');
 * // name is now BranchName type
 * ```
 */
export function toBranchName(name: string): BranchName {
  const normalized = normalizeBranchName(name);
  if (!isValidBranchName(normalized)) {
    throw new Error(`Invalid branch name: "${name}"`);
  }
  return normalized as BranchName;
}

/**
 * Convert a string to a CommitId.
 * CommitIds are not validated beyond being non-empty strings.
 *
 * @param id - The commit identifier
 * @returns A branded CommitId
 * @throws Error if the commit ID is empty
 *
 * @example
 * ```typescript
 * const commit = toCommitId('snapshot-001');
 * ```
 */
export function toCommitId(id: string): CommitId {
  if (!id || id.trim().length === 0) {
    throw new Error('Commit ID cannot be empty');
  }
  return id as CommitId;
}

/**
 * Type guard to check if a value is a valid BranchName.
 *
 * @param value - Value to check
 * @returns True if the value is a valid branch name
 *
 * @example
 * ```typescript
 * if (isBranchName(input)) {
 *   // input is now BranchName type
 * }
 * ```
 */
export function isBranchName(value: unknown): value is BranchName {
  return typeof value === 'string' && isValidBranchName(value);
}

/**
 * Type guard to check if a value is a valid CommitId.
 *
 * @param value - Value to check
 * @returns True if the value is a valid commit ID
 */
export function isCommitId(value: unknown): value is CommitId {
  return typeof value === 'string' && value.trim().length > 0;
}

// ============================================================================
// Type Definitions
// ============================================================================

/** Current version of the branch metadata storage format */
export const BRANCH_METADATA_VERSION = 1;

/**
 * Branch state indicating the lifecycle stage of a branch.
 *
 * - `active`: Branch is in use and can be modified
 * - `merged`: Branch has been merged into another branch
 * - `deleted`: Branch has been marked for deletion (soft delete)
 */
export type BranchState = 'active' | 'merged' | 'deleted';

/**
 * Branch metadata stored in JSON format.
 * Each branch points to a specific commit (snapshot) in the database history.
 *
 * @example
 * ```typescript
 * const metadata: BranchMetadata = {
 *   version: 1,
 *   name: 'feature-branch',
 *   baseCommit: 'snapshot-001',
 *   headCommit: 'snapshot-002',
 *   createdAt: '2024-01-01T00:00:00.000Z',
 *   updatedAt: '2024-01-02T00:00:00.000Z',
 *   state: 'active',
 *   parentBranch: 'main',
 *   branchSequence: 1,
 * };
 * ```
 */
export interface BranchMetadata {
  /**
   * Storage format version for forward compatibility.
   * Current version is 1.
   */
  version?: number;

  /**
   * Branch name (unique identifier).
   * Must be alphanumeric with hyphens, underscores, or slashes.
   * Cannot be a reserved name like 'HEAD'.
   */
  name: string;

  /**
   * Base commit hash/ID this branch was created from.
   * This value is immutable after branch creation.
   */
  baseCommit: string;

  /**
   * Timestamp when the branch was created (ISO 8601 format).
   * This value is immutable after branch creation.
   */
  createdAt: string;

  /**
   * Optional description of the branch purpose.
   * Can be updated after creation.
   */
  description?: string;

  /**
   * User or system that created the branch.
   * Useful for auditing and tracking ownership.
   */
  createdBy?: string;

  /**
   * Current head commit of this branch.
   * May diverge from baseCommit as the branch evolves.
   */
  headCommit: string;

  /**
   * Last time the branch was updated (ISO 8601 format).
   * Updated automatically on any metadata change.
   */
  updatedAt: string;

  /**
   * Whether this branch is protected from deletion.
   * Protected branches require force=true to delete.
   */
  protected?: boolean;

  /**
   * Custom metadata for the branch.
   * Can store arbitrary key-value pairs for application use.
   *
   * @example
   * ```typescript
   * metadata: {
   *   ticket: 'JIRA-123',
   *   environment: 'staging',
   *   reviewers: ['alice', 'bob']
   * }
   * ```
   */
  metadata?: Record<string, unknown>;

  /**
   * Current state of the branch.
   * @see BranchState
   */
  state: BranchState;

  /**
   * Parent branch name (null for main branch).
   * Used to track branch hierarchy for ancestry queries.
   */
  parentBranch: string | null;

  /**
   * Snapshot ID this branch was created from.
   * Alternative to baseCommit for numeric snapshot IDs.
   */
  fromSnapshotId?: number;

  /**
   * Sequence number when branch was created.
   * Used for ordering branches in the hierarchy.
   */
  branchSequence: number;

  /**
   * Files that have been modified on this branch.
   * Used for copy-on-write tracking to identify branch-specific data.
   */
  modifiedFiles?: string[];

  /**
   * Merge commit if this branch has been merged.
   * Set when the branch is merged into another branch.
   */
  mergeCommit?: string;

  /**
   * Branch this was merged into.
   * Set when the branch is merged into another branch.
   */
  mergedInto?: string;
}

/**
 * Options for creating a new branch.
 *
 * @example
 * ```typescript
 * // Minimal options
 * const opts1: CreateBranchOptions = { name: 'feature-branch' };
 *
 * // Full options
 * const opts2: CreateBranchOptions = {
 *   name: 'feature/auth',
 *   baseCommit: 'snapshot-001',
 *   description: 'Authentication feature',
 *   createdBy: 'developer@example.com',
 *   protected: false,
 *   metadata: { ticket: 'JIRA-123' },
 *   parentBranch: 'develop',
 * };
 * ```
 */
export interface CreateBranchOptions {
  /**
   * Branch name (required, must be unique).
   * Will be normalized (trimmed) before use.
   */
  name: string;

  /**
   * Commit to branch from (defaults to parent's HEAD).
   * If not specified, uses the head commit of the parent branch.
   */
  baseCommit?: string;

  /**
   * Optional description of the branch purpose.
   */
  description?: string;

  /**
   * Creator identification (email, username, or system ID).
   */
  createdBy?: string;

  /**
   * Whether to protect the branch from deletion.
   * Default: false
   */
  protected?: boolean;

  /**
   * Custom metadata to attach to the branch.
   */
  metadata?: Record<string, unknown>;

  /**
   * Create branch from a specific snapshot ID.
   * Alternative to baseCommit for numeric snapshot IDs.
   */
  fromSnapshotId?: number;

  /**
   * Parent branch to create from.
   * Defaults to current branch or 'main' if not specified.
   */
  parentBranch?: string;
}

/**
 * Options for listing branches.
 *
 * @example
 * ```typescript
 * // List all feature branches, newest first
 * const opts: ListBranchesOptions = {
 *   prefix: 'feature/',
 *   sortBy: 'createdAt',
 *   sortOrder: 'desc',
 * };
 * ```
 */
export interface ListBranchesOptions {
  /**
   * Filter by branch name prefix.
   * Useful for listing branches in a namespace (e.g., 'feature/').
   */
  prefix?: string;

  /**
   * Include only protected branches.
   * Default: false (include all branches)
   */
  protectedOnly?: boolean;

  /**
   * Field to sort by.
   * Default: 'name'
   */
  sortBy?: 'name' | 'createdAt' | 'updatedAt';

  /**
   * Sort order.
   * Default: 'asc'
   */
  sortOrder?: 'asc' | 'desc';

  /**
   * Filter by branch state.
   * If not specified, returns all states.
   */
  state?: BranchState;

  /**
   * Maximum number of branches to return.
   * Useful for pagination.
   */
  limit?: number;
}

/**
 * Options for updating branch metadata.
 *
 * All fields are optional. Only specified fields are updated.
 * Metadata is merged (not replaced) with existing metadata.
 *
 * @example
 * ```typescript
 * // Update description and add modified files
 * const opts: UpdateBranchOptions = {
 *   description: 'Updated description',
 *   addModifiedFiles: ['data/users.parquet'],
 * };
 * ```
 */
export interface UpdateBranchOptions {
  /**
   * New description (replaces existing).
   */
  description?: string;

  /**
   * New head commit (advances the branch).
   */
  headCommit?: string;

  /**
   * Update protected status.
   */
  protected?: boolean;

  /**
   * Merge additional metadata with existing.
   * New keys are added, existing keys are overwritten.
   */
  metadata?: Record<string, unknown>;

  /**
   * Update branch state.
   */
  state?: BranchState;

  /**
   * Add files to the modified files list.
   * Duplicates are automatically filtered.
   */
  addModifiedFiles?: string[];

  /**
   * Remove files from the modified files list.
   */
  removeModifiedFiles?: string[];

  /**
   * Record merge commit when branch is merged.
   */
  mergeCommit?: string;

  /**
   * Record target branch when branch is merged.
   */
  mergedInto?: string;
}

/**
 * Result of branch deletion.
 *
 * @example
 * ```typescript
 * const result = await store.deleteBranch('feature');
 * if (result.deleted) {
 *   console.log(`Deleted ${result.name}`);
 * } else {
 *   console.error(`Failed: ${result.reason}`);
 * }
 * ```
 */
export interface DeleteBranchResult {
  /**
   * Whether the branch was successfully deleted.
   */
  deleted: boolean;

  /**
   * Branch name that was deleted (or attempted).
   */
  name: string;

  /**
   * Reason if deletion failed.
   * Only present when deleted is false.
   */
  reason?: string;

  /**
   * Error code for programmatic handling.
   */
  code?: 'NOT_FOUND' | 'PROTECTED' | 'DEFAULT_BRANCH' | 'HAS_CHILDREN';
}

// ============================================================================
// Validation Result Types
// ============================================================================

/**
 * Result of branch name validation.
 * Provides detailed information about validation failures.
 */
export interface BranchNameValidationResult {
  /**
   * Whether the branch name is valid.
   */
  valid: boolean;

  /**
   * The normalized branch name (if valid).
   */
  normalizedName?: string;

  /**
   * Error message if validation failed.
   */
  error?: string;

  /**
   * Error code for programmatic handling.
   */
  code?: 'EMPTY' | 'TOO_LONG' | 'RESERVED' | 'INVALID_CHARS' | 'INVALID_PATTERN';
}

/**
 * Result of branch metadata validation.
 */
export interface BranchMetadataValidationResult {
  /**
   * Whether the metadata is valid.
   */
  valid: boolean;

  /**
   * List of validation errors.
   */
  errors: string[];

  /**
   * List of validation warnings (non-fatal).
   */
  warnings: string[];
}

// ============================================================================
// Constants
// ============================================================================

/** Default branch name */
export const DEFAULT_BRANCH = 'main';

/** Branch storage directory relative to storage root */
export const BRANCHES_DIR = 'branches';

/** File extension for branch metadata files */
export const BRANCH_FILE_EXTENSION = '.json';

/** Maximum branch name length */
export const MAX_BRANCH_NAME_LENGTH = 255;

/** Minimum branch name length */
export const MIN_BRANCH_NAME_LENGTH = 1;

// ============================================================================
// Branch Name Validation
// ============================================================================

/**
 * Valid branch name pattern: alphanumeric, hyphens, underscores, slashes.
 * Must start with alphanumeric, cannot end with slash.
 */
const BRANCH_NAME_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9_\-/]*[a-zA-Z0-9]$|^[a-zA-Z0-9]$/;

/**
 * Reserved branch names that cannot be used.
 * These names conflict with Git internals or MongoLake internals.
 */
const RESERVED_NAMES = new Set(['HEAD', 'head', 'refs', 'objects', 'hooks', 'config']);

/**
 * Characters that are not allowed in branch names.
 */
const INVALID_CHARS_PATTERN = /[~^:?*\[\]\\@{}\s]/;

/**
 * Validate a branch name with detailed results.
 *
 * Validation rules:
 * - Must be 1-255 characters long
 * - Must start and end with alphanumeric character
 * - Can contain alphanumeric, hyphens, underscores, slashes
 * - Cannot be a reserved name (HEAD, refs, etc.)
 * - Cannot contain consecutive slashes (//)
 * - Cannot contain consecutive dots (..)
 * - Cannot contain special characters (~, ^, :, ?, *, [, ], \, @, {, })
 *
 * @param name - Branch name to validate
 * @returns Detailed validation result
 *
 * @example
 * ```typescript
 * const result = validateBranchName('feature/auth');
 * if (result.valid) {
 *   console.log(`Valid: ${result.normalizedName}`);
 * } else {
 *   console.error(`Invalid: ${result.error} (${result.code})`);
 * }
 * ```
 */
export function validateBranchName(name: string): BranchNameValidationResult {
  // Normalize first
  const normalized = name?.trim() ?? '';

  // Check empty
  if (!normalized || normalized.length === 0) {
    return {
      valid: false,
      error: 'Branch name cannot be empty',
      code: 'EMPTY',
    };
  }

  // Check length
  if (normalized.length > MAX_BRANCH_NAME_LENGTH) {
    return {
      valid: false,
      error: `Branch name cannot exceed ${MAX_BRANCH_NAME_LENGTH} characters`,
      code: 'TOO_LONG',
    };
  }

  // Check reserved names
  if (RESERVED_NAMES.has(normalized)) {
    return {
      valid: false,
      error: `"${normalized}" is a reserved name and cannot be used as a branch name`,
      code: 'RESERVED',
    };
  }

  // Check for invalid characters
  if (INVALID_CHARS_PATTERN.test(normalized)) {
    return {
      valid: false,
      error: 'Branch name contains invalid characters (spaces, ~, ^, :, ?, *, [, ], \\, @, {, })',
      code: 'INVALID_CHARS',
    };
  }

  // Check for consecutive slashes
  if (normalized.includes('//')) {
    return {
      valid: false,
      error: 'Branch name cannot contain consecutive slashes (//)',
      code: 'INVALID_PATTERN',
    };
  }

  // Check for consecutive dots
  if (normalized.includes('..')) {
    return {
      valid: false,
      error: 'Branch name cannot contain consecutive dots (..)',
      code: 'INVALID_PATTERN',
    };
  }

  // Check pattern
  if (!BRANCH_NAME_PATTERN.test(normalized)) {
    return {
      valid: false,
      error: 'Branch name must start and end with alphanumeric characters',
      code: 'INVALID_PATTERN',
    };
  }

  return {
    valid: true,
    normalizedName: normalized,
  };
}

/**
 * Validate a branch name (simple boolean check).
 *
 * @param name - Branch name to validate
 * @returns True if valid, false otherwise
 *
 * @example
 * ```typescript
 * if (isValidBranchName('feature-branch')) {
 *   // proceed
 * }
 * ```
 */
export function isValidBranchName(name: string): boolean {
  return validateBranchName(name).valid;
}

/**
 * Normalize a branch name (trim whitespace).
 *
 * @param name - Branch name to normalize
 * @returns Normalized branch name
 *
 * @example
 * ```typescript
 * normalizeBranchName('  feature-branch  ') // 'feature-branch'
 * ```
 */
export function normalizeBranchName(name: string): string {
  return name.trim();
}

/**
 * Validate branch metadata for completeness and correctness.
 *
 * @param metadata - Branch metadata to validate
 * @returns Validation result with errors and warnings
 *
 * @example
 * ```typescript
 * const result = validateBranchMetadata(metadata);
 * if (!result.valid) {
 *   console.error('Validation errors:', result.errors);
 * }
 * if (result.warnings.length > 0) {
 *   console.warn('Warnings:', result.warnings);
 * }
 * ```
 */
export function validateBranchMetadata(metadata: Partial<BranchMetadata>): BranchMetadataValidationResult {
  const errors: string[] = [];
  const warnings: string[] = [];

  // Required fields
  if (!metadata.name) {
    errors.push('Missing required field: name');
  } else if (!isValidBranchName(metadata.name)) {
    errors.push(`Invalid branch name: "${metadata.name}"`);
  }

  if (!metadata.baseCommit) {
    errors.push('Missing required field: baseCommit');
  }

  if (!metadata.headCommit) {
    errors.push('Missing required field: headCommit');
  }

  if (!metadata.createdAt) {
    errors.push('Missing required field: createdAt');
  } else if (isNaN(Date.parse(metadata.createdAt))) {
    errors.push('Invalid createdAt timestamp');
  }

  if (!metadata.updatedAt) {
    errors.push('Missing required field: updatedAt');
  } else if (isNaN(Date.parse(metadata.updatedAt))) {
    errors.push('Invalid updatedAt timestamp');
  }

  if (!metadata.state) {
    errors.push('Missing required field: state');
  } else if (!['active', 'merged', 'deleted'].includes(metadata.state)) {
    errors.push(`Invalid state: "${metadata.state}"`);
  }

  if (metadata.branchSequence === undefined || metadata.branchSequence === null) {
    errors.push('Missing required field: branchSequence');
  } else if (typeof metadata.branchSequence !== 'number' || metadata.branchSequence < 0) {
    errors.push('branchSequence must be a non-negative number');
  }

  // Warnings for optional but recommended fields
  if (!metadata.description) {
    warnings.push('Branch has no description');
  }

  if (!metadata.createdBy) {
    warnings.push('Branch has no createdBy field');
  }

  // Version check
  if (metadata.version !== undefined && metadata.version !== BRANCH_METADATA_VERSION) {
    warnings.push(`Metadata version ${metadata.version} differs from current version ${BRANCH_METADATA_VERSION}`);
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
  };
}

/**
 * Type guard to check if a value is valid BranchMetadata.
 *
 * @param value - Value to check
 * @returns True if the value is valid BranchMetadata
 *
 * @example
 * ```typescript
 * const data = JSON.parse(jsonString);
 * if (isBranchMetadata(data)) {
 *   // data is now BranchMetadata type
 * }
 * ```
 */
export function isBranchMetadata(value: unknown): value is BranchMetadata {
  if (!value || typeof value !== 'object') {
    return false;
  }

  const obj = value as Record<string, unknown>;

  return (
    typeof obj.name === 'string' &&
    typeof obj.baseCommit === 'string' &&
    typeof obj.headCommit === 'string' &&
    typeof obj.createdAt === 'string' &&
    typeof obj.updatedAt === 'string' &&
    typeof obj.state === 'string' &&
    ['active', 'merged', 'deleted'].includes(obj.state) &&
    (obj.parentBranch === null || typeof obj.parentBranch === 'string') &&
    typeof obj.branchSequence === 'number'
  );
}

// ============================================================================
// BranchStore Class
// ============================================================================

/**
 * Configuration options for BranchStore.
 */
export interface BranchStoreConfig {
  /**
   * Whether to include the version field in stored metadata.
   * Default: true
   */
  includeVersion?: boolean;

  /**
   * Whether to use compact JSON (no indentation).
   * Default: false
   */
  compactJson?: boolean;

  /**
   * Whether to validate metadata before writing.
   * Default: true
   */
  validateOnWrite?: boolean;
}

/**
 * Manages branch metadata storage and retrieval.
 *
 * Branches are stored as JSON files in the .mongolake/branches/ directory.
 * Each branch file contains the branch metadata including the base commit,
 * creation time, and current head commit.
 *
 * @example
 * ```typescript
 * const storage = new MemoryStorage();
 * const store = new BranchStore(storage, 'mydb');
 *
 * // Initialize default branch
 * await store.initializeDefaultBranch('snapshot-001');
 *
 * // Create a feature branch
 * const branch = await store.createBranch({
 *   name: 'feature-branch',
 *   baseCommit: 'snapshot-001',
 * });
 *
 * // List all branches
 * const branches = await store.listBranches();
 * ```
 */
export class BranchStore {
  private readonly storage: StorageBackend;
  private readonly basePath: string;
  private readonly config: Required<BranchStoreConfig>;

  /**
   * Create a new BranchStore.
   *
   * @param storage - Storage backend to use
   * @param basePath - Base path for branch storage (e.g., 'testdb')
   * @param config - Optional configuration
   *
   * @example
   * ```typescript
   * const store = new BranchStore(storage, 'mydb', {
   *   compactJson: true,
   *   validateOnWrite: true,
   * });
   * ```
   */
  constructor(storage: StorageBackend, basePath: string = '', config: BranchStoreConfig = {}) {
    this.storage = storage;
    this.basePath = basePath;
    this.config = {
      includeVersion: config.includeVersion ?? true,
      compactJson: config.compactJson ?? false,
      validateOnWrite: config.validateOnWrite ?? true,
    };
  }

  /**
   * Get the full storage path for a branch.
   * @param branchName - Branch name
   * @returns Full storage path
   */
  private getBranchPath(branchName: string): string {
    const normalizedName = normalizeBranchName(branchName);
    // Replace slashes in branch names with a safe separator for filesystem
    const safeName = normalizedName.replace(/\//g, '__');
    const pathParts = [this.basePath, BRANCHES_DIR, `${safeName}${BRANCH_FILE_EXTENSION}`]
      .filter(Boolean);
    return pathParts.join('/');
  }

  /**
   * Get the prefix for listing all branches.
   * @returns Storage prefix for branch files
   */
  private getBranchesPrefix(): string {
    const pathParts = [this.basePath, BRANCHES_DIR].filter(Boolean);
    return pathParts.join('/') + '/';
  }

  /**
   * Create a new branch.
   *
   * @param options - Branch creation options
   * @returns Created branch metadata
   * @throws Error if branch name is invalid or branch already exists
   *
   * @example
   * ```typescript
   * const branch = await store.createBranch({
   *   name: 'feature-branch',
   *   baseCommit: 'snapshot-001',
   *   description: 'New feature work',
   * });
   * ```
   */
  async createBranch(options: CreateBranchOptions): Promise<BranchMetadata> {
    const {
      name,
      baseCommit,
      description,
      createdBy,
      protected: isProtected,
      metadata,
      fromSnapshotId,
      parentBranch,
    } = options;

    // Validate branch name with detailed error
    const validation = validateBranchName(name);
    if (!validation.valid) {
      throw new Error(`Invalid branch name: "${name}". ${validation.error}`);
    }
    const normalizedName = validation.normalizedName!;

    // Check if branch already exists
    const existing = await this.getBranch(normalizedName);
    if (existing) {
      throw new Error(`Branch "${normalizedName}" already exists`);
    }

    // Determine parent branch
    const effectiveParentBranch = parentBranch ?? DEFAULT_BRANCH;

    // Determine base commit and sequence number
    let effectiveBaseCommit = baseCommit;
    let branchSequence = 0;

    if (!effectiveBaseCommit) {
      // Try to get from parent branch
      const parent = await this.getBranch(effectiveParentBranch);
      if (parent) {
        effectiveBaseCommit = parent.headCommit;
        branchSequence = parent.branchSequence + 1;
      } else {
        effectiveBaseCommit = await this.getCurrentCommit();
      }
    }

    if (!effectiveBaseCommit) {
      throw new Error('No base commit specified and no current commit found');
    }

    // Create branch metadata
    const now = new Date().toISOString();
    const branch: BranchMetadata = {
      // Include version for forward compatibility
      ...(this.config.includeVersion && { version: BRANCH_METADATA_VERSION }),
      name: normalizedName,
      baseCommit: effectiveBaseCommit,
      headCommit: effectiveBaseCommit,
      createdAt: now,
      updatedAt: now,
      state: 'active',
      parentBranch: effectiveParentBranch === DEFAULT_BRANCH && normalizedName === DEFAULT_BRANCH ? null : effectiveParentBranch,
      branchSequence,
      modifiedFiles: [],
      ...(description && { description }),
      ...(createdBy && { createdBy }),
      ...(isProtected !== undefined && { protected: isProtected }),
      ...(metadata && { metadata }),
      ...(fromSnapshotId !== undefined && { fromSnapshotId }),
    };

    // Validate before writing if configured
    if (this.config.validateOnWrite) {
      const metadataValidation = validateBranchMetadata(branch);
      if (!metadataValidation.valid) {
        throw new Error(`Invalid branch metadata: ${metadataValidation.errors.join(', ')}`);
      }
    }

    // Write branch metadata to storage
    await this.writeBranch(branch);

    return branch;
  }

  /**
   * Get branch metadata by name.
   *
   * @param name - Branch name
   * @returns Branch metadata or null if not found
   *
   * @example
   * ```typescript
   * const branch = await store.getBranch('feature-branch');
   * if (branch) {
   *   console.log(`Head commit: ${branch.headCommit}`);
   * }
   * ```
   */
  async getBranch(name: string): Promise<BranchMetadata | null> {
    const path = this.getBranchPath(name);
    const data = await this.storage.get(path);

    if (!data) {
      return null;
    }

    try {
      const json = new TextDecoder().decode(data);
      const parsed = JSON.parse(json);

      // Migrate old format if needed
      const metadata = this.migrateMetadata(parsed);

      // Validate structure
      if (!isBranchMetadata(metadata)) {
        return null;
      }

      return metadata;
    } catch {
      return null;
    }
  }

  /**
   * Migrate metadata from older versions if needed.
   *
   * @param data - Raw parsed data
   * @returns Migrated metadata
   * @throws Error if the data cannot be migrated to valid BranchMetadata
   */
  private migrateMetadata(data: Record<string, unknown>): BranchMetadata {
    // Handle missing version (pre-version format)
    if (data.version === undefined) {
      data.version = BRANCH_METADATA_VERSION;
    }

    // Future migrations would go here based on version number

    // Validate the migrated data is valid BranchMetadata
    if (!isBranchMetadata(data)) {
      throw new Error(`Invalid branch metadata: missing required fields`);
    }

    return data;
  }

  /**
   * List all branches.
   *
   * @param options - List options for filtering and sorting
   * @returns Array of branch metadata
   *
   * @example
   * ```typescript
   * // List all feature branches
   * const branches = await store.listBranches({
   *   prefix: 'feature/',
   *   sortBy: 'createdAt',
   *   sortOrder: 'desc',
   * });
   * ```
   */
  async listBranches(options: ListBranchesOptions = {}): Promise<BranchMetadata[]> {
    const { prefix, protectedOnly, sortBy = 'name', sortOrder = 'asc', state, limit } = options;

    // List all branch files
    const branchPrefix = this.getBranchesPrefix();
    const files = await this.storage.list(branchPrefix);

    // Filter to only .json files
    const branchFiles = files.filter((f) => f.endsWith(BRANCH_FILE_EXTENSION));

    // Load all branch metadata
    const branches: BranchMetadata[] = [];
    for (const file of branchFiles) {
      const data = await this.storage.get(file);
      if (data) {
        try {
          const json = new TextDecoder().decode(data);
          const parsed = JSON.parse(json);
          const branch = this.migrateMetadata(parsed);
          if (isBranchMetadata(branch)) {
            branches.push(branch);
          }
        } catch {
          // Skip invalid branch files
        }
      }
    }

    // Apply filters
    let filtered = branches;

    if (prefix) {
      filtered = filtered.filter((b) => b.name.startsWith(prefix));
    }

    if (protectedOnly) {
      filtered = filtered.filter((b) => b.protected === true);
    }

    if (state) {
      filtered = filtered.filter((b) => b.state === state);
    }

    // Sort
    filtered.sort((a, b) => {
      let comparison = 0;
      switch (sortBy) {
        case 'name':
          comparison = a.name.localeCompare(b.name);
          break;
        case 'createdAt':
          comparison = new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime();
          break;
        case 'updatedAt':
          comparison = new Date(a.updatedAt).getTime() - new Date(b.updatedAt).getTime();
          break;
      }
      return sortOrder === 'desc' ? -comparison : comparison;
    });

    // Apply limit
    if (limit !== undefined && limit > 0) {
      filtered = filtered.slice(0, limit);
    }

    return filtered;
  }

  /**
   * Update branch metadata.
   *
   * @param name - Branch name
   * @param options - Update options
   * @returns Updated branch metadata
   * @throws Error if branch not found
   *
   * @example
   * ```typescript
   * const updated = await store.updateBranch('feature', {
   *   description: 'Updated description',
   *   headCommit: 'snapshot-002',
   * });
   * ```
   */
  async updateBranch(name: string, options: UpdateBranchOptions): Promise<BranchMetadata> {
    const branch = await this.getBranch(name);
    if (!branch) {
      throw new Error(`Branch "${name}" not found`);
    }

    // Apply updates
    const updated: BranchMetadata = {
      ...branch,
      updatedAt: new Date().toISOString(),
    };

    // Ensure version is set
    if (this.config.includeVersion && updated.version === undefined) {
      updated.version = BRANCH_METADATA_VERSION;
    }

    if (options.description !== undefined) {
      updated.description = options.description;
    }

    if (options.headCommit !== undefined) {
      updated.headCommit = options.headCommit;
    }

    if (options.protected !== undefined) {
      updated.protected = options.protected;
    }

    if (options.metadata !== undefined) {
      updated.metadata = {
        ...(branch.metadata ?? {}),
        ...options.metadata,
      };
    }

    if (options.state !== undefined) {
      updated.state = options.state;
    }

    // Handle modifiedFiles updates
    let modifiedFiles = updated.modifiedFiles ?? [];

    if (options.addModifiedFiles !== undefined) {
      const newFiles = options.addModifiedFiles.filter(f => !modifiedFiles.includes(f));
      modifiedFiles = [...modifiedFiles, ...newFiles];
    }

    if (options.removeModifiedFiles !== undefined) {
      const removeSet = new Set(options.removeModifiedFiles);
      modifiedFiles = modifiedFiles.filter(f => !removeSet.has(f));
    }

    updated.modifiedFiles = modifiedFiles;

    if (options.mergeCommit !== undefined) {
      updated.mergeCommit = options.mergeCommit;
    }

    if (options.mergedInto !== undefined) {
      updated.mergedInto = options.mergedInto;
    }

    // Validate before writing if configured
    if (this.config.validateOnWrite) {
      const metadataValidation = validateBranchMetadata(updated);
      if (!metadataValidation.valid) {
        throw new Error(`Invalid branch metadata: ${metadataValidation.errors.join(', ')}`);
      }
    }

    // Write updated metadata
    await this.writeBranch(updated);

    return updated;
  }

  /**
   * Delete a branch.
   *
   * @param name - Branch name to delete
   * @param force - Force delete even if protected
   * @returns Deletion result
   *
   * @example
   * ```typescript
   * const result = await store.deleteBranch('feature');
   * if (!result.deleted) {
   *   if (result.code === 'PROTECTED') {
   *     // Ask user to confirm force delete
   *   }
   * }
   * ```
   */
  async deleteBranch(name: string, force: boolean = false): Promise<DeleteBranchResult> {
    // Cannot delete default branch
    if (name === DEFAULT_BRANCH) {
      return {
        deleted: false,
        name,
        reason: `Cannot delete the default branch "${DEFAULT_BRANCH}"`,
        code: 'DEFAULT_BRANCH',
      };
    }

    const branch = await this.getBranch(name);
    if (!branch) {
      return {
        deleted: false,
        name,
        reason: `Branch "${name}" not found`,
        code: 'NOT_FOUND',
      };
    }

    // Check if protected
    if (branch.protected && !force) {
      return {
        deleted: false,
        name,
        reason: `Branch "${name}" is protected. Use force=true to delete.`,
        code: 'PROTECTED',
      };
    }

    // Delete the branch file
    const path = this.getBranchPath(name);
    await this.storage.delete(path);

    return {
      deleted: true,
      name,
    };
  }

  /**
   * Check if a branch exists.
   *
   * @param name - Branch name
   * @returns True if branch exists
   */
  async branchExists(name: string): Promise<boolean> {
    const path = this.getBranchPath(name);
    return this.storage.exists(path);
  }

  /**
   * Rename a branch.
   *
   * @param oldName - Current branch name
   * @param newName - New branch name
   * @returns Renamed branch metadata
   * @throws Error if old branch not found, new name invalid, or new name exists
   */
  async renameBranch(oldName: string, newName: string): Promise<BranchMetadata> {
    // Cannot rename default branch
    if (oldName === DEFAULT_BRANCH) {
      throw new Error(`Cannot rename the default branch "${DEFAULT_BRANCH}"`);
    }

    // Validate new name
    if (!isValidBranchName(newName)) {
      throw new Error(`Invalid branch name: "${newName}"`);
    }

    // Check old branch exists
    const branch = await this.getBranch(oldName);
    if (!branch) {
      throw new Error(`Branch "${oldName}" not found`);
    }

    // Check new name doesn't exist
    const existing = await this.getBranch(newName);
    if (existing) {
      throw new Error(`Branch "${newName}" already exists`);
    }

    // Create new branch with new name
    const renamed: BranchMetadata = {
      ...branch,
      name: normalizeBranchName(newName),
      updatedAt: new Date().toISOString(),
    };

    // Write new branch file
    await this.writeBranch(renamed);

    // Delete old branch file
    const oldPath = this.getBranchPath(oldName);
    await this.storage.delete(oldPath);

    return renamed;
  }

  /**
   * Get the current commit (HEAD) for the default branch.
   * This is a placeholder - in a real implementation, this would
   * read from the current database state or Iceberg metadata.
   *
   * @returns Current commit hash or undefined
   */
  private async getCurrentCommit(): Promise<string | undefined> {
    // Try to get the default branch
    const mainBranch = await this.getBranch(DEFAULT_BRANCH);
    if (mainBranch) {
      return mainBranch.headCommit;
    }

    // No default branch, return undefined
    return undefined;
  }

  /**
   * Write branch metadata to storage.
   *
   * @param branch - Branch metadata to write
   */
  private async writeBranch(branch: BranchMetadata): Promise<void> {
    const path = this.getBranchPath(branch.name);
    const json = this.config.compactJson
      ? JSON.stringify(branch)
      : JSON.stringify(branch, null, 2);
    const data = new TextEncoder().encode(json);
    await this.storage.put(path, data);
  }

  /**
   * Initialize the default branch if it doesn't exist.
   *
   * @param initialCommit - Initial commit for the default branch
   * @returns Default branch metadata
   *
   * @example
   * ```typescript
   * const mainBranch = await store.initializeDefaultBranch('snapshot-001');
   * console.log(mainBranch.name); // 'main'
   * console.log(mainBranch.protected); // true
   * ```
   */
  async initializeDefaultBranch(initialCommit: string): Promise<BranchMetadata> {
    const existing = await this.getBranch(DEFAULT_BRANCH);
    if (existing) {
      return existing;
    }

    // For the default branch, we need to create it directly to avoid circular parent reference
    const now = new Date().toISOString();
    const branch: BranchMetadata = {
      // Include version for forward compatibility
      ...(this.config.includeVersion && { version: BRANCH_METADATA_VERSION }),
      name: DEFAULT_BRANCH,
      baseCommit: initialCommit,
      headCommit: initialCommit,
      createdAt: now,
      updatedAt: now,
      description: 'Default branch',
      protected: true,
      state: 'active',
      parentBranch: null,
      branchSequence: 0,
      modifiedFiles: [],
    };

    await this.writeBranch(branch);
    return branch;
  }

  /**
   * Get branches that are children of a given branch.
   *
   * @param parentName - Parent branch name
   * @returns Array of child branch metadata
   */
  async getChildBranches(parentName: string): Promise<BranchMetadata[]> {
    const allBranches = await this.listBranches();
    return allBranches.filter(b => b.parentBranch === parentName);
  }

  /**
   * Check if a branch can be safely deleted.
   *
   * A branch cannot be deleted if:
   * - It's the default branch
   * - It's protected (without force)
   * - It has unmerged child branches
   *
   * @param name - Branch name
   * @returns Object with canDelete boolean, reason, and code if not deletable
   *
   * @example
   * ```typescript
   * const check = await store.canDeleteBranch('feature');
   * if (!check.canDelete) {
   *   if (check.code === 'HAS_CHILDREN') {
   *     console.log('Delete child branches first');
   *   }
   * }
   * ```
   */
  async canDeleteBranch(name: string): Promise<{
    canDelete: boolean;
    reason?: string;
    code?: 'NOT_FOUND' | 'PROTECTED' | 'DEFAULT_BRANCH' | 'HAS_CHILDREN';
  }> {
    if (name === DEFAULT_BRANCH) {
      return {
        canDelete: false,
        reason: `Cannot delete the default branch "${DEFAULT_BRANCH}"`,
        code: 'DEFAULT_BRANCH',
      };
    }

    const branch = await this.getBranch(name);
    if (!branch) {
      return {
        canDelete: false,
        reason: `Branch "${name}" not found`,
        code: 'NOT_FOUND',
      };
    }

    if (branch.protected) {
      return {
        canDelete: false,
        reason: `Branch "${name}" is protected. Use force=true to delete.`,
        code: 'PROTECTED',
      };
    }

    const children = await this.getChildBranches(name);
    const activeChildren = children.filter(c => c.state === 'active');
    if (activeChildren.length > 0) {
      return {
        canDelete: false,
        reason: `Branch "${name}" has ${activeChildren.length} active child branch(es): ${activeChildren.map(c => c.name).join(', ')}`,
        code: 'HAS_CHILDREN',
      };
    }

    return { canDelete: true };
  }

  /**
   * Get branch statistics.
   *
   * @returns Statistics about branches
   */
  async getStats(): Promise<{
    totalBranches: number;
    protectedBranches: number;
    oldestBranch: string | null;
    newestBranch: string | null;
  }> {
    const branches = await this.listBranches();

    if (branches.length === 0) {
      return {
        totalBranches: 0,
        protectedBranches: 0,
        oldestBranch: null,
        newestBranch: null,
      };
    }

    const protectedCount = branches.filter((b) => b.protected).length;

    // Sort by createdAt
    const sorted = [...branches].sort(
      (a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime()
    );

    return {
      totalBranches: branches.length,
      protectedBranches: protectedCount,
      oldestBranch: sorted[0]?.name ?? null,
      newestBranch: sorted[sorted.length - 1]?.name ?? null,
    };
  }
}

// ============================================================================
// Convenience Functions
// ============================================================================

/**
 * Create a branch store for a database.
 *
 * @param storage - Storage backend
 * @param database - Database name
 * @returns BranchStore instance
 */
export function createBranchStore(storage: StorageBackend, database: string): BranchStore {
  return new BranchStore(storage, database);
}

/**
 * Quick helper to create a branch.
 *
 * @param storage - Storage backend
 * @param database - Database name
 * @param options - Branch creation options
 * @returns Created branch metadata
 */
export async function createBranch(
  storage: StorageBackend,
  database: string,
  options: CreateBranchOptions
): Promise<BranchMetadata> {
  const store = createBranchStore(storage, database);
  return store.createBranch(options);
}

/**
 * Quick helper to get a branch.
 *
 * @param storage - Storage backend
 * @param database - Database name
 * @param name - Branch name
 * @returns Branch metadata or null
 */
export async function getBranch(
  storage: StorageBackend,
  database: string,
  name: string
): Promise<BranchMetadata | null> {
  const store = createBranchStore(storage, database);
  return store.getBranch(name);
}

/**
 * Quick helper to list branches.
 *
 * @param storage - Storage backend
 * @param database - Database name
 * @param options - List options
 * @returns Array of branch metadata
 */
export async function listBranches(
  storage: StorageBackend,
  database: string,
  options?: ListBranchesOptions
): Promise<BranchMetadata[]> {
  const store = createBranchStore(storage, database);
  return store.listBranches(options);
}

/**
 * Quick helper to delete a branch.
 *
 * @param storage - Storage backend
 * @param database - Database name
 * @param name - Branch name
 * @param force - Force delete protected branches
 * @returns Deletion result
 */
export async function deleteBranch(
  storage: StorageBackend,
  database: string,
  name: string,
  force?: boolean
): Promise<DeleteBranchResult> {
  const store = createBranchStore(storage, database);
  return store.deleteBranch(name, force);
}
