/**
 * MongoLake Client
 *
 * MongoDB-compatible client API - Main entry point
 */

// ============================================================================
// Core Client Exports
// ============================================================================

// MongoLake client and factory functions
export { MongoLake, createClient, createDatabase } from './mongo-lake.js';

// Database and Collection classes
export { Database, type CollectionOptions } from './database.js';
export { Collection } from './collection.js';
export { BranchCollection } from './branch-collection.js';

// Cursors
export { FindCursor, StreamingFindCursor, TimeTravelFindCursor } from './cursors.js';
export { AggregationCursor, TimeTravelAggregationCursor } from './aggregation.js';

// Time Travel
export { TimeTravelCollection, type TimeTravelOptions } from './time-travel.js';

// Helpers
export { extractDocumentId } from './helpers.js';

// Corruption Audit
export {
  CorruptionAudit,
  createCorruptionAudit,
} from './corruption-audit.js';
export type {
  CorruptionAuditEntry,
  CorruptionSummary,
  CorruptionAuditOptions,
} from './corruption-audit.js';

// Distributed aggregation
export {
  DistributedAggregationPlanner,
  DistributedAggregationExecutor,
  DistributedAggregator,
  createDistributedAggregationPlanner,
  createDistributedAggregationExecutor,
  createDistributedAggregator,
} from './distributed-aggregation.js';
export type {
  ExecutionPhase,
  PartialAggregate,
  PartialAccumulatorValue,
  PipelineAnalysis,
  ClassifiedStage,
  ShardResult,
  DistributedAggregationOptions,
  ShardExecutor,
  DistributedAggregatorOptions,
} from './distributed-aggregation.js';

// ============================================================================
// External Re-exports
// ============================================================================

export { ObjectId } from '@types';
export {
  ValidationError,
  validateDatabaseName,
  validateCollectionName,
  validateFilter,
  validateProjection,
  validateUpdate,
  validateDocument,
  validateAggregationPipeline,
  validateFieldName,
  validateInputs,
  VALID_QUERY_OPERATORS,
  VALID_UPDATE_OPERATORS,
  VALID_AGGREGATION_STAGES,
} from '@utils/validation.js';

// Connection string utilities
export {
  parseConnectionString,
  buildConnectionString,
  isConnectionString,
  ConnectionStringParseError,
} from '@utils/connection-string.js';
export type {
  ParsedConnectionString,
  ConnectionOptions,
  HostInfo,
} from '@utils/connection-string.js';
export { ChangeStream, computeUpdateDescription, createChangeStream } from '@mongolake/change-stream/index.js';
export type {
  OperationType,
  ResumeToken,
  UpdateDescription,
  ChangeStreamNamespace,
  ChangeStreamDocument,
  ChangeStreamOptions,
  ChangeEventHandler,
} from '@mongolake/change-stream/index.js';
export type {
  Document,
  WithId,
  Filter,
  Update,
  AggregationStage,
  FindOptions,
  UpdateOptions,
  DeleteOptions,
  AggregateOptions,
  InsertOptions,
  InsertOneResult,
  InsertManyResult,
  UpdateResult,
  DeleteResult,
  MongoLakeConfig,
  CorruptionReport,
  CorruptionCallback,
  QueryMetadata,
  SessionOption,
  DocumentId,
  ShardId,
  CollectionName,
  DatabaseName,
  // Semantic type wrappers
  DocumentFields,
  OperatorCondition,
  FilterQuery,
  UpdateQuery,
  ProjectionQuery,
  SortQuery,
  ConnectionOptions as TypesConnectionOptions,
} from '@types';

// Branded type runtime validation helpers
export {
  // Type guards (narrowing)
  isDocumentId,
  isShardId,
  isCollectionName,
  isDatabaseName,
  // Assertion functions (throwing)
  assertDocumentId,
  assertShardId,
  assertCollectionName,
  assertDatabaseName,
  // Factory functions
  toDocumentId,
  toShardId,
  toCollectionName,
  toDatabaseName,
  // Semantic type validation helpers
  isDocumentFields,
  isOperatorCondition,
  toDocumentFields,
  isFilterQuery,
  isUpdateQuery,
} from '@types';

// Cursor exports
export { Cursor, StreamingCursor, CursorStore, generateCursorId } from '@mongolake/cursor/index.js';
export type { CursorOptions, CursorState, DocumentSource } from '@mongolake/cursor/index.js';

// Session and Transaction exports
export {
  ClientSession,
  SessionStore,
  TransactionError,
  SessionError,
  generateSessionId,
  hasSession,
  extractSession,
} from '@mongolake/session/index.js';
export type {
  TransactionState,
  ReadConcernLevel,
  WriteConcern,
  TransactionOptions,
  SessionOptions,
  SessionOperationOptions,
  BufferedOperation,
  SessionId,
} from '@mongolake/session/index.js';

// Transaction Manager exports
export {
  TransactionManager,
  runTransaction,
} from '@mongolake/transaction/index.js';
export type {
  TransactionWrite,
  TransactionSnapshot,
  TransactionCommitResult,
  RunTransactionOptions,
} from '@mongolake/transaction/index.js';

// Branching exports
export {
  BranchStore,
  BranchManager,
  MergeEngine,
  MergeError,
  ConflictError,
  BranchError,
  BranchExistsError,
  BranchNotFoundError,
  DEFAULT_BRANCH,
} from '@mongolake/branching/index.js';
export type {
  BranchMetadata,
  BranchState,
  CreateBranchOptions,
  ListBranchesOptions,
  UpdateBranchOptions,
  DeleteBranchResult,
  MergeOptions,
  MergeResult,
  MergeConflict,
  MergePreview,
  MergeStrategy,
  BranchInfo,
} from '@mongolake/branching/index.js';
