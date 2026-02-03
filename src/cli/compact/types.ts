/**
 * Type definitions for the Compact command
 *
 * @module cli/compact/types
 */

import type { BlockMetadata } from '../../compaction/scheduler.js';

// ============================================================================
// Core Compact Options and Results
// ============================================================================

export interface CompactOptions {
  /** Database name */
  database: string;

  /** Collection name */
  collection: string;

  /** Path to data directory (default: .mongolake) */
  path: string;

  /** Dry run mode - show what would be compacted without making changes */
  dryRun: boolean;

  /** Enable verbose logging */
  verbose: boolean;

  /** Event emitter for progress and lifecycle events */
  eventEmitter?: CompactEventEmitter | CompactionEventEmitter;

  /** Progress reporter for detailed progress updates */
  progressReporter?: ProgressReporter;

  /** Callback for progress updates */
  onProgress?: (update: ProgressUpdate) => void;

  /** AbortSignal for cancellation support */
  signal?: AbortSignal;

  /** Retry configuration */
  retryConfig?: RetryConfig;

  /** Remove tombstone documents during compaction */
  removeTombstones?: boolean;

  /** Optimize for read performance */
  optimize?: boolean;

  /** Column to cluster/sort data by */
  clusterBy?: string;

  /** Maximum bytes to process in this run */
  maxSize?: number;

  /** Only process files older than this (ms) */
  minAge?: number;

  /** Resume from a previous checkpoint */
  resume?: boolean;

  /** Force restart, ignoring any checkpoint */
  forceRestart?: boolean;
}

export interface CompactResult {
  /** Whether compaction succeeded */
  success: boolean;

  /** Whether compaction was skipped */
  skipped: boolean;

  /** Reason for skipping */
  reason?: string;

  /** Number of blocks processed */
  processedBlocks: number;

  /** Merged blocks created */
  mergedBlocks: MergedBlockInfo[];

  /** Number of tombstones removed */
  tombstonesRemoved?: number;

  /** Error information if failed */
  error?: CompactionErrorInfo;

  /** Compaction statistics */
  stats: CompactStats;

  /** Whether operation was aborted */
  aborted?: boolean;

  /** Whether cleanup was performed after abort */
  cleanedUp?: boolean;

  /** Checkpoint state for resume */
  checkpointState?: CheckpointState;

  /** If resumed, the block we resumed from */
  resumedFrom?: string;

  /** Column data was sorted by */
  sortedBy?: string;

  /** Sort order used */
  sortOrder?: 'ascending' | 'descending';

  /** Whether retries were exhausted */
  retriesExhausted?: boolean;

  /** Files that were processed */
  processedFiles?: ProcessedFile[];
}

export interface MergedBlockInfo {
  path: string;
  size: number;
  rowCount: number;
  minSeq: number;
  maxSeq: number;
}

export interface CompactStats {
  bytesProcessed: number;
  rowsProcessed: number;
  compressionRatio: number;
  tombstonesRemoved?: number;
  spaceReclaimed?: number;
  spaceSaved?: number;
  spaceSavedPercent?: number;
  filesBefore?: number;
  filesAfter?: number;
  filesRemoved?: number;
  compressionBefore?: number;
  compressionAfter?: number;
  compressionImprovement?: number;
}

export interface CompactionErrorInfo {
  code: string;
  message: string;
}

export interface ProcessedFile {
  path: string;
  createdAt: Date;
}

// ============================================================================
// Collection Block Type
// ============================================================================

export interface CollectionBlock extends BlockMetadata {
  id: string;
  path: string;
  size: number;
  rowCount: number;
  minSeq: number;
  maxSeq: number;
  createdAt: Date;
  tombstoneCount?: number;
}

// ============================================================================
// Trigger Types
// ============================================================================

export interface TriggerOptions {
  database: string;
  collection: string;
  path: string;
  immediate?: boolean;
  delay?: number;
}

export interface TriggerResult {
  triggered: boolean;
  scheduled: boolean;
  alreadyRunning: boolean;
  startedAt?: Date;
  scheduledFor?: Date;
  compactionId: string;
}

export interface QueueOptions {
  database: string;
  collection: string;
  path: string;
  priority?: 'normal' | 'high' | 'low';
}

export interface QueueResult {
  queued: boolean;
  position: number;
}

export interface CancelResult {
  cancelled: boolean;
  compactionId: string;
}

export interface PendingCompaction {
  database: string;
  collection: string;
  queuedAt: Date;
  priority: string;
}

// ============================================================================
// Collection Targeting Types
// ============================================================================

export interface CollectionCompactOptions {
  database: string;
  collection: string;
  path: string;
}

export interface CollectionCompactResult {
  database: string;
  collection: string;
  processedBlocks: number;
  skipped: boolean;
  reason?: string;
}

export interface ValidationResult {
  exists: boolean;
  error?: string;
}

export interface CollectionsCompactOptions {
  database: string;
  pattern: string;
  path: string;
  dryRun: boolean;
  exclude?: string[];
  continueOnError?: boolean;
}

export interface CollectionsCompactResult {
  collectionsCompacted: string[];
  collectionsProcessed?: number;
  perCollection: Record<string, PerCollectionResult>;
  errors: CollectionError[];
  totalStats?: CompactStats;
}

export interface PerCollectionResult {
  processedBlocks: number;
  stats: CompactStats;
}

export interface CollectionError {
  collection: string;
  error: string;
}

export interface CollectionStats {
  blockCount: number;
  totalSize: number;
  smallBlockCount: number;
  needsCompaction: boolean;
}

export interface CompactAllOptions {
  database: string;
  path: string;
  dryRun: boolean;
}

export interface CompactAllResult {
  collectionsProcessed: number;
  collectionsCompacted: string[];
  perCollection: Record<string, PerCollectionResult>;
  errors: CollectionError[];
  totalStats: CompactStats;
}

export interface CompactDatabaseOptions {
  database: string;
  path: string;
  dryRun: boolean;
  verbose?: boolean;
}

export interface CompactDatabaseResult {
  collectionsProcessed: number;
  collectionsCompacted: string[];
  perCollection: Record<string, PerCollectionResult>;
  errors: CollectionError[];
  totalStats: CompactStats;
}

// ============================================================================
// Progress Reporting Types
// ============================================================================

export interface ProgressReporterOptions {
  onProgress: (event: ProgressEvent) => void;
  format?: 'text' | 'json';
  throttleMs?: number;
}

export interface ProgressReporter {
  report(event: ProgressEvent): void;
}

export interface ProgressEvent {
  phase: string;
  currentBlock: number;
  totalBlocks: number;
  bytesProcessed: number;
  timestamp?: string;
  // Aliases for convenience
  progress?: number;
  total?: number;
}

export interface ProgressUpdate {
  phase: string;
  currentBlock: number;
  totalBlocks: number;
  bytesProcessed: number;
  totalBytes?: number;
  elapsedMs?: number;
  estimatedRemainingMs?: number;
}

export interface ETAInput {
  bytesProcessed: number;
  totalBytes: number;
  elapsedMs: number;
}

export interface ETAResult {
  remainingMs: number;
  estimatedCompletion: Date;
}

export interface ThroughputInput {
  bytesProcessed: number;
  rowsProcessed?: number;
  durationMs: number;
}

export interface ThroughputResult {
  bytesPerSecond: number;
  mbPerSecond: number;
  rowsPerSecond?: number;
}

export interface ProgressBarInput {
  current: number;
  total: number;
  width: number;
}

export interface ProgressSummaryInput {
  phase: string;
  currentBlock: number;
  totalBlocks: number;
  bytesProcessed: number;
  totalBytes: number;
  elapsedMs: number;
  estimatedRemainingMs: number;
}

// ============================================================================
// Error Handling Types
// ============================================================================

export interface RetryConfig {
  maxRetries: number;
  backoffMs: number;
}

export interface DiskSpaceResult {
  sufficient: boolean;
  available: number;
  required: number;
}

export interface RollbackState {
  sourceBlocks: string[];
  mergedBlock: string;
  manifestBackup: unknown;
}

export interface RollbackResult {
  success: boolean;
  restoredBlocks: string[];
  removedMerged: boolean;
}

export interface LockResult {
  acquired: boolean;
  holder?: string;
  heldSince?: Date;
}

export interface MemoryEstimate {
  required: number;
  available: number;
  sufficient: boolean;
}

export interface ParquetValidation {
  valid: boolean;
  error?: string;
}

// ============================================================================
// Tombstone Types
// ============================================================================

export interface TombstoneInfo {
  totalCount: number;
  blocksWithTombstones: BlockMetadata[];
}

export interface TombstoneAgeAnalysis {
  distribution: Record<string, number>;
}

// ============================================================================
// Optimization Types
// ============================================================================

export interface SchemaField {
  name: string;
  type: string;
}

export interface Schema {
  fields: SchemaField[];
}

export interface ZoneMapBlock {
  id: string;
  path: string;
  minValues: Record<string, number>;
  maxValues: Record<string, number>;
}

export interface ZoneMaps {
  [column: string]: {
    globalMin: number;
    globalMax: number;
    blocks: ZoneMapBlock[];
  };
}

export interface BloomFilterOptions {
  columns: string[];
  falsePositiveRate: number;
}

export interface BloomFilter {
  mightContain(value: string): boolean;
}

export interface BloomFilters {
  [column: string]: BloomFilter;
}

export interface RowGroupSizeInput {
  avgRowSize: number;
  totalRows: number;
  memoryBudget: number;
}

export interface ColumnStatsInput {
  [column: string]: {
    distinctCount: number;
    totalCount: number;
  };
}

export interface EncodingPlan {
  [column: string]: 'dictionary' | 'plain';
}

// ============================================================================
// Checkpoint Types
// ============================================================================

export interface CheckpointState {
  database: string;
  collection: string;
  lastProcessedBlock: string;
  processedBlocks: number;
  remainingBlocks: number;
  startedAt: Date;
}

// ============================================================================
// Statistics Types
// ============================================================================

export interface BlockStatsInput {
  id: string;
  size: number;
  rowCount: number;
}

export interface BlockStats {
  totalSize: number;
  totalRows: number;
  avgBlockSize: number;
  minBlockSize: number;
  maxBlockSize: number;
}

export interface CompactionHistoryEntry {
  database: string;
  collection: string;
  timestamp: Date;
  stats: {
    filesBefore: number;
    filesAfter: number;
    duration: number;
  };
}

export interface ReportStats {
  filesBefore: number;
  filesAfter: number;
  sizeBefore: number;
  sizeAfter: number;
  rowsProcessed: number;
  tombstonesRemoved: number;
  durationMs: number;
}

// ============================================================================
// Event Emitter Interfaces (for type checking)
// ============================================================================

import type { EventEmitter } from 'node:events';

export interface CompactEventEmitter extends EventEmitter {
  emitProgress(event: ProgressEvent): void;
  emitPhase(phase: string): void;
}

export interface CompactionEventEmitter extends EventEmitter {
  emitStarted(event: { database: string; collection: string; startedAt: Date }): void;
  emitCompleted(event: { database: string; collection: string; completedAt: Date; result: CompactResult }): void;
}
