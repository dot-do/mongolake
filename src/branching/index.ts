/**
 * Branching Module
 *
 * Database branching support for MongoLake.
 * Allows creating isolated copies of database state for development,
 * testing, or feature work.
 */

export {
  // Branded Types
  type BranchName,
  type CommitId,
  // Branded Type Factories
  toBranchName,
  toCommitId,
  // Type Guards
  isBranchName,
  isCommitId,
  isBranchMetadata,
  // Types
  type BranchMetadata,
  type BranchState,
  type CreateBranchOptions,
  type ListBranchesOptions,
  type UpdateBranchOptions,
  type DeleteBranchResult,
  type BranchNameValidationResult,
  type BranchMetadataValidationResult,
  type BranchStoreConfig,
  // Constants
  DEFAULT_BRANCH,
  BRANCHES_DIR,
  BRANCH_FILE_EXTENSION,
  BRANCH_METADATA_VERSION,
  MAX_BRANCH_NAME_LENGTH,
  MIN_BRANCH_NAME_LENGTH,
  // Validation
  isValidBranchName,
  normalizeBranchName,
  validateBranchName,
  validateBranchMetadata,
  // Class
  BranchStore,
  // Convenience functions
  createBranchStore,
  createBranch,
  getBranch,
  listBranches,
  deleteBranch,
} from './metadata.js';

// Branch Manager (high-level API)
export {
  // Class
  BranchManager,
  BranchFactory,
  // Error types
  BranchError,
  BranchValidationError,
  BranchExistsError,
  BranchNotFoundError,
  SnapshotNotFoundError,
  NotInitializedError,
  // Types
  type BranchCreateOptions,
  type BranchInfo,
  type SnapshotInfo,
  type BranchManagerConfig,
  type BeforeCreateHook,
  type AfterCreateHook,
} from './manager.js';

// Conflict Detection (for merging)
export {
  // Class
  ConflictDetector,
  // Enums
  ConflictType,
  ConflictSeverity,
  // Types
  type DocumentChange,
  type DocumentConflict,
  type ConflictReport,
  type ConflictSummary,
  type AutoMergeableChange,
} from './conflict-detector.js';

// Diff Generator
export {
  DiffGenerator,
  type DiffOptions,
  type DiffResult,
  type DiffSummary,
  type InsertedChange,
  type UpdatedChange,
  type DeletedChange,
  type CollectionChangeSummary,
  type DiffChange,
  type DiffFormat,
  type ChangeType,
} from './diff.js';

// Merge Engine
export {
  // Class
  MergeEngine,
  // Error types
  MergeError,
  ConflictError,
  // Types
  type MergeConflict,
  type ConflictResolution,
  type ResolvedConflict,
  type MergeStrategy,
  type ConflictResolver,
  type MergeOptions,
  type MergeResult,
  type MergePreview,
  type BatchMergeResult,
  type ConflictDetector as MergeConflictDetector,
  type BeforeMergeHook,
  type AfterMergeHook,
  type MergeHooks,
} from './merge.js';

// Conflict Resolution
export {
  // Classes
  ConflictResolutionApplier,
  MergeResultApplier,
  // Types
  type ApplyResolutionsResult,
} from './conflict-resolver.js';
