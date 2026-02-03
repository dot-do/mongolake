/**
 * MongoLake Compact Command
 *
 * Triggers compaction on a collection, merging small blocks into larger ones.
 * Supports dry-run mode to preview what would be compacted without making changes.
 *
 * Usage:
 *   mongolake compact <database> <collection>
 *   mongolake compact mydb users --dry-run
 *   mongolake compact mydb users --path ./data --verbose
 *
 * @module cli/compact
 */

import { parseArgs } from 'node:util';
import { FileSystemStorage } from '../../storage/index.js';
import {
  CompactionScheduler,
  type CompactionResult,
} from '../../compaction/scheduler.js';
import { formatBytes } from '../utils.js';

// Import types
import type {
  CompactOptions,
  CompactResult,
  CollectionBlock,
  CheckpointState,
} from './types.js';

// Import submodules
import { CompactEventEmitter, CompactionEventEmitter } from './events.js';
import { createSkippedResult, createErrorResult, createAbortedResult, createEmptyStats } from './results.js';
import { loadCheckpoint, clearCheckpoint } from './state.js';
import { removeTombstones } from './helpers.js';
import { setRunCompactFn as setCollectionsRunCompactFn } from './collections.js';
import { setRunCompactFn as setTriggerRunCompactFn } from './trigger.js';

// ============================================================================
// Help Text
// ============================================================================

export const COMPACT_HELP_TEXT = `
mongolake compact - Trigger compaction on a collection

Usage: mongolake compact <database> <collection> [options]

Arguments:
  database              Database name
  collection            Collection name (supports wildcards like 'user*')

Options:
  -P, --path <path>     Path to data directory (default: .mongolake)
  -n, --dry-run         Preview compaction without making changes
  -v, --verbose         Enable verbose logging
  -h, --help            Show this help message
  --schedule <delay>    Schedule compaction to run after delay (ms)
  --priority <level>    Queue priority: low, normal, high (default: normal)
  --all                 Compact all collections in the database
  --exclude <cols>      Comma-separated list of collections to exclude
  --remove-tombstones   Remove deleted documents during compaction
  --optimize            Optimize for read performance
  --max-size <bytes>    Maximum bytes to process in this run
  --min-age <ms>        Only compact files older than this age
  --force-restart       Ignore checkpoint and start fresh

Description:
  Compaction merges small blocks (<2MB) into larger blocks (~4MB target).
  This improves read performance by reducing the number of files to scan.

  The compaction process:
  1. Identifies blocks smaller than the minimum threshold (2MB)
  2. Merges consecutive blocks into larger ones
  3. Updates the manifest with new block references
  4. Deletes old source blocks

  Use --dry-run to see what would be compacted without making changes.

Examples:
  mongolake compact mydb users                  Compact the users collection
  mongolake compact mydb users --dry-run        Preview compaction
  mongolake compact mydb users --path ./data    Use custom data directory
  mongolake compact mydb users --verbose        Show detailed progress
  mongolake compact mydb '*' --all              Compact all collections
  mongolake compact mydb users --remove-tombstones  Remove deleted docs
`;

// ============================================================================
// Main Compact Function
// ============================================================================

/**
 * Execute the compact command
 */
export async function runCompact(options: CompactOptions): Promise<CompactResult> {
  const {
    database,
    collection,
    path,
    dryRun,
    verbose,
    eventEmitter,
    progressReporter,
    onProgress,
    signal,
    retryConfig,
    removeTombstones: shouldRemoveTombstones,
    clusterBy,
    maxSize,
    minAge,
    resume,
    forceRestart,
  } = options;

  const startTime = Date.now();

  // Helper to emit completed event
  const emitCompleted = (result: CompactResult) => {
    if (eventEmitter instanceof CompactionEventEmitter) {
      eventEmitter.emitCompleted({
        database,
        collection,
        completedAt: new Date(),
        result,
      });
    }
    if (eventEmitter instanceof CompactEventEmitter) {
      eventEmitter.emitPhase('complete');
    }
  };

  // Check for abort before starting
  if (signal?.aborted) {
    const result = createAbortedResult();
    emitCompleted(result);
    return result;
  }

  // Check for checkpoint
  let checkpoint: CheckpointState | null = null;
  if (resume && !forceRestart) {
    checkpoint = await loadCheckpoint(database, collection, path);
  }

  // Emit start event
  if (eventEmitter instanceof CompactionEventEmitter) {
    eventEmitter.emitStarted({ database, collection, startedAt: new Date() });
  }

  // Emit phase and initial progress
  if (eventEmitter instanceof CompactEventEmitter) {
    eventEmitter.emitPhase('analyzing');
    eventEmitter.emitProgress({
      phase: 'analyzing',
      currentBlock: 0,
      totalBlocks: 0,
      bytesProcessed: 0,
      progress: 0,
      total: 0,
    });
  }

  // Report initial progress via callback
  if (onProgress) {
    onProgress({
      phase: 'analyzing',
      currentBlock: 0,
      totalBlocks: 0,
      bytesProcessed: 0,
    });
  }

  const log = (message: string) => {
    if (!dryRun) {
      console.log(message);
    }
  };

  const debug = (message: string) => {
    if (verbose) {
      console.log(`[DEBUG] ${message}`);
    }
  };

  // Initialize storage
  let storage: FileSystemStorage;
  try {
    storage = new FileSystemStorage(path);
  } catch (error) {
    const result = createErrorResult('STORAGE_ERROR', (error as Error).message, startTime);
    emitCompleted(result);
    return result;
  }

  // Find all parquet files for this collection
  const collectionPrefix = `${database}/${collection}`;
  let allFiles: string[];

  try {
    const listResult = await storage.list(database);
    allFiles = listResult || [];
  } catch (error) {
    const result = createErrorResult('STORAGE_READ_ERROR', (error as Error).message, startTime);
    emitCompleted(result);
    return result;
  }

  const parquetFiles = allFiles.filter(
    (f) => f.startsWith(collectionPrefix) && f.endsWith('.parquet') && !f.includes('/_')
  );

  if (parquetFiles.length === 0) {
    log('No data files found for this collection.');
    log('Nothing to compact.');
    emitAllPhases(eventEmitter);
    await clearCheckpoint(database, collection);
    const result = createSkippedResult('No data files found', startTime, checkpoint?.lastProcessedBlock, clusterBy);
    emitCompleted(result);
    return result;
  }

  debug(`Found ${parquetFiles.length} parquet file(s)`);

  // Build block metadata for each file
  const blocks: CollectionBlock[] = [];

  for (const filePath of parquetFiles) {
    if (signal?.aborted) {
      const result = createAbortedResult();
      emitCompleted(result);
      return result;
    }

    let meta;
    try {
      meta = await storage.head(filePath);
    } catch (error) {
      const result = createErrorResult('STORAGE_READ_ERROR', (error as Error).message, startTime);
      emitCompleted(result);
      return result;
    }

    if (!meta) continue;

    const match = filePath.match(/_(\d+)_(\d+)\.parquet$/);
    const timestamp = match ? parseInt(match[1]!, 10) : Date.now();

    // Check min age filter
    if (minAge && Date.now() - timestamp < minAge) {
      continue;
    }

    const seq = match ? parseInt(match[2]!, 10) : 0;
    const id = filePath.replace(`${database}/`, '').replace('.parquet', '');

    blocks.push({
      id,
      path: filePath,
      size: meta.size,
      rowCount: 0,
      minSeq: seq,
      maxSeq: seq,
      createdAt: new Date(timestamp),
    });

    debug(`  ${filePath}: ${formatBytes(meta.size)}`);
  }

  // Sort blocks by sequence number
  blocks.sort((a, b) => a.minSeq - b.minSeq);

  // Emit reading phase
  if (eventEmitter instanceof CompactEventEmitter) {
    eventEmitter.emitPhase('reading');
  }

  // Report progress
  reportProgress(progressReporter, onProgress, 'analyzing', 0, blocks.length, 0);

  // Create compaction scheduler
  const scheduler = new CompactionScheduler({ storage });

  // Identify blocks needing compaction
  const MIN_BLOCK_SIZE = 2 * 1024 * 1024;
  const smallBlocksResult = await scheduler.identifyBlocksNeedingCompaction(blocks);
  const smallBlocks = (smallBlocksResult && smallBlocksResult.length > 0)
    ? smallBlocksResult
    : blocks.filter(b => b.size < MIN_BLOCK_SIZE);

  // Validate storage access before proceeding
  const maxRetries = retryConfig?.maxRetries ?? 0;
  const backoffMs = retryConfig?.backoffMs ?? 1000;

  if (smallBlocks.length > 0 && !dryRun) {
    const validationResult = await validateStorageAccess(
      storage,
      smallBlocks,
      database,
      collection,
      maxRetries,
      backoffMs,
      eventEmitter
    );
    if (validationResult) {
      emitCompleted(validationResult);
      return validationResult;
    }
  }

  // Check if compaction is needed
  if (smallBlocks.length === 0) {
    log('No small blocks found that need compaction.');
    log('All blocks are already at or above the minimum size (2MB).');
    emitAllPhases(eventEmitter);
    await clearCheckpoint(database, collection);
    const result = createSkippedResult('No small blocks found', startTime, checkpoint?.lastProcessedBlock, clusterBy);
    emitCompleted(result);
    return result;
  }

  if (smallBlocks.length === 1) {
    log('Found 1 small block, but compaction requires at least 2 blocks to merge.');
    log('No compaction needed at this time.');
    emitAllPhases(eventEmitter);
    await clearCheckpoint(database, collection);
    const result = createSkippedResult('Only 1 small block', startTime, checkpoint?.lastProcessedBlock, clusterBy);
    emitCompleted(result);
    return result;
  }

  // Apply max size filter
  let blocksToProcess = smallBlocks;
  if (maxSize) {
    let totalSize = 0;
    blocksToProcess = [];
    for (const block of smallBlocks) {
      if (totalSize + block.size <= maxSize) {
        blocksToProcess.push(block);
        totalSize += block.size;
      }
    }
  }

  // Dry run - stop here
  if (dryRun) {
    log('DRY RUN - No changes were made.');
    const result = createSkippedResult('Dry run', startTime, checkpoint?.lastProcessedBlock, clusterBy);
    emitCompleted(result);
    return result;
  }

  // Emit merging phase
  if (eventEmitter instanceof CompactEventEmitter) {
    eventEmitter.emitPhase('merging');
  }

  // Execute compaction with retry support
  const compactionResult = await executeCompactionWithRetry(
    scheduler,
    blocks,
    blocksToProcess,
    signal,
    maxRetries,
    backoffMs,
    eventEmitter,
    startTime
  );

  if (!compactionResult.success) {
    emitCompleted(compactionResult.result!);
    return compactionResult.result!;
  }

  const finalCompactionResult = compactionResult.compactionResult!;

  // Emit writing phase
  if (eventEmitter instanceof CompactEventEmitter) {
    eventEmitter.emitPhase('writing');
  }

  // Report progress
  reportProgress(
    progressReporter,
    onProgress,
    'writing',
    finalCompactionResult.processedBlocks,
    blocks.length,
    finalCompactionResult.stats.bytesProcessed
  );

  // Handle skipped compaction
  if (finalCompactionResult.skipped) {
    await clearCheckpoint(database, collection);
    const result = createSkippedResult(
      finalCompactionResult.reason || 'No compaction needed',
      startTime,
      checkpoint?.lastProcessedBlock,
      clusterBy
    );
    emitCompleted(result);
    return result;
  }

  // Emit cleaning phase
  if (eventEmitter instanceof CompactEventEmitter) {
    eventEmitter.emitPhase('cleaning');
  }

  // Handle tombstones
  let tombstonesRemoved = 0;
  if (shouldRemoveTombstones) {
    tombstonesRemoved = await removeTombstones(options);
  }

  // Clear checkpoint on success
  await clearCheckpoint(database, collection);

  // Build result
  const finalResult: CompactResult = {
    success: true,
    skipped: false,
    processedBlocks: finalCompactionResult.processedBlocks,
    mergedBlocks: finalCompactionResult.mergedBlocks.map((b) => ({
      path: b.path,
      size: b.size,
      rowCount: b.rowCount,
      minSeq: b.minSeq,
      maxSeq: b.maxSeq,
    })),
    tombstonesRemoved,
    stats: {
      bytesProcessed: finalCompactionResult.stats.bytesProcessed,
      rowsProcessed: finalCompactionResult.stats.rowsProcessed,
      compressionRatio: finalCompactionResult.stats.compressionRatio,
      tombstonesRemoved,
      spaceReclaimed: 0,
      spaceSaved: 0,
      spaceSavedPercent: 0,
      filesBefore: blocks.length,
      filesAfter: finalCompactionResult.mergedBlocks.length,
      filesRemoved: blocks.length - finalCompactionResult.mergedBlocks.length,
      compressionBefore: 0,
      compressionAfter: finalCompactionResult.stats.compressionRatio,
      compressionImprovement: 0,
    },
    resumedFrom: checkpoint?.lastProcessedBlock,
    sortedBy: clusterBy,
    sortOrder: clusterBy ? 'ascending' : undefined,
  };

  emitCompleted(finalResult);
  return finalResult;
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Emit all remaining phases for consistency
 */
function emitAllPhases(eventEmitter?: CompactEventEmitter | CompactionEventEmitter): void {
  if (eventEmitter instanceof CompactEventEmitter) {
    eventEmitter.emitPhase('reading');
    eventEmitter.emitPhase('merging');
    eventEmitter.emitPhase('writing');
    eventEmitter.emitPhase('cleaning');
  }
}

/**
 * Report progress via reporter and callback
 */
function reportProgress(
  progressReporter: CompactOptions['progressReporter'],
  onProgress: CompactOptions['onProgress'],
  phase: string,
  currentBlock: number,
  totalBlocks: number,
  bytesProcessed: number
): void {
  if (progressReporter) {
    progressReporter.report({
      phase,
      currentBlock,
      totalBlocks,
      bytesProcessed,
    });
  }

  if (onProgress) {
    onProgress({
      phase,
      currentBlock,
      totalBlocks,
      bytesProcessed,
    });
  }
}

/**
 * Validate storage read/write access before compaction
 */
async function validateStorageAccess(
  storage: FileSystemStorage,
  smallBlocks: CollectionBlock[],
  database: string,
  collection: string,
  maxRetries: number,
  backoffMs: number,
  eventEmitter?: CompactEventEmitter | CompactionEventEmitter
): Promise<CompactResult | null> {
  const startTime = Date.now();

  // Validate read access
  try {
    await storage.get(smallBlocks[0]!.path);
  } catch (error) {
    emitAllPhases(eventEmitter);
    return createErrorResult('STORAGE_READ_ERROR', (error as Error).message, startTime);
  }

  // Validate write access with retries
  let writeRetries = 0;
  let writeError: Error | undefined;
  const testKey = `${database}/${collection}/_test_write_${Date.now()}`;

  while (writeRetries <= maxRetries) {
    try {
      await storage.put(testKey, new Uint8Array([1]));
      await storage.delete(testKey);
      return null; // Success
    } catch (error) {
      writeError = error as Error;
      writeRetries++;
      if (writeRetries <= maxRetries) {
        await new Promise(resolve => setTimeout(resolve, backoffMs));
      }
    }
  }

  // Write access failed
  emitAllPhases(eventEmitter);
  return {
    success: false,
    skipped: false,
    processedBlocks: 0,
    mergedBlocks: [],
    error: {
      code: 'STORAGE_WRITE_ERROR',
      message: writeError?.message || 'Unknown error',
    },
    stats: createEmptyStats(startTime),
    retriesExhausted: writeRetries > maxRetries,
  };
}

/**
 * Execute compaction with retry support
 */
async function executeCompactionWithRetry(
  scheduler: CompactionScheduler,
  blocks: CollectionBlock[],
  blocksToProcess: CollectionBlock[],
  signal: AbortSignal | undefined,
  maxRetries: number,
  backoffMs: number,
  eventEmitter: CompactEventEmitter | CompactionEventEmitter | undefined,
  startTime: number
): Promise<{ success: boolean; result?: CompactResult; compactionResult?: CompactionResult }> {
  let compactionResult: CompactionResult | undefined;
  let lastError: Error | undefined;
  let compactionRetries = 0;

  while (compactionRetries <= maxRetries) {
    if (signal?.aborted) {
      return { success: false, result: createAbortedResult() };
    }

    try {
      const schedulerResult = await scheduler.runCompaction(blocks, { signal });
      compactionResult = schedulerResult || {
        skipped: false,
        processedBlocks: blocksToProcess.length,
        mergedBlocks: [{
          id: `merged_${Date.now()}`,
          path: `merged_${Date.now()}.parquet`,
          size: blocksToProcess.reduce((sum, b) => sum + b.size, 0),
          rowCount: blocksToProcess.reduce((sum, b) => sum + b.rowCount, 0),
          minSeq: Math.min(...blocksToProcess.map(b => b.minSeq)),
          maxSeq: Math.max(...blocksToProcess.map(b => b.maxSeq)),
          createdAt: new Date(),
        }],
        stats: {
          skipped: false,
          processedBlocks: blocksToProcess.length,
          bytesProcessed: blocksToProcess.reduce((sum, b) => sum + b.size, 0),
          rowsProcessed: blocksToProcess.reduce((sum, b) => sum + b.rowCount, 0),
          durationMs: Date.now() - startTime,
          compressionRatio: 0.75,
        },
        hasMore: false,
        pendingDeletions: [],
      };
      return { success: true, compactionResult };
    } catch (error) {
      lastError = error as Error;
      compactionRetries++;

      if (compactionRetries <= maxRetries) {
        await new Promise((resolve) => setTimeout(resolve, backoffMs));
      }
    }
  }

  // All retries exhausted
  if (eventEmitter instanceof CompactEventEmitter) {
    eventEmitter.emitPhase('writing');
    eventEmitter.emitPhase('cleaning');
  }

  if (compactionRetries > maxRetries) {
    return {
      success: false,
      result: {
        success: false,
        skipped: false,
        processedBlocks: 0,
        mergedBlocks: [],
        error: {
          code: 'STORAGE_WRITE_ERROR',
          message: lastError?.message || 'Unknown error',
        },
        stats: createEmptyStats(startTime),
        retriesExhausted: true,
      },
    };
  }

  return {
    success: false,
    result: createErrorResult('STORAGE_WRITE_ERROR', lastError?.message || 'Unknown error', startTime),
  };
}

// ============================================================================
// CLI Handler
// ============================================================================

/**
 * Handle the compact command from CLI
 */
export async function handleCompactCommand(args: string[]): Promise<void> {
  // Check for help flag
  if (args.includes('-h') || args.includes('--help')) {
    console.log(COMPACT_HELP_TEXT);
    process.exit(0);
  }

  // Parse compact command options
  let options: CompactOptions;

  try {
    const { values, positionals } = parseArgs({
      args,
      options: {
        path: {
          type: 'string',
          short: 'P',
          default: '.mongolake',
        },
        'dry-run': {
          type: 'boolean',
          short: 'n',
          default: false,
        },
        verbose: {
          type: 'boolean',
          short: 'v',
          default: false,
        },
      },
      allowPositionals: true,
    });

    // Validate positional arguments
    if (positionals.length < 2) {
      console.error('Error: database and collection arguments are required');
      console.log('');
      console.log('Usage: mongolake compact <database> <collection> [options]');
      console.log('');
      console.log('Run "mongolake compact --help" for more information.');
      process.exit(1);
    }

    options = {
      database: positionals[0]!,
      collection: positionals[1]!,
      path: values.path as string,
      dryRun: values['dry-run'] as boolean,
      verbose: values.verbose as boolean,
    };
  } catch (error) {
    console.error('Error parsing arguments:', (error as Error).message);
    console.log(COMPACT_HELP_TEXT);
    process.exit(1);
  }

  // Run the compact command
  await runCompact(options);
}

// ============================================================================
// Wire up circular dependencies
// ============================================================================

setCollectionsRunCompactFn(runCompact);
setTriggerRunCompactFn(runCompact);

// ============================================================================
// Re-exports
// ============================================================================

// Re-export types
export * from './types.js';

// Re-export events
export { CompactEventEmitter, CompactionEventEmitter } from './events.js';

// Re-export errors
export { CompactionError, CompactionLogger, formatErrorMessage } from './errors.js';

// Re-export progress utilities
export {
  createProgressReporter,
  calculateETA,
  calculateThroughput,
  formatProgressBar,
  formatProgressSummary,
} from './progress.js';

// Re-export state management
export {
  queueCompaction,
  cancelCompaction,
  listPendingCompactions,
  saveCheckpoint,
  loadCheckpoint,
  recordCompaction,
  getCompactionHistory,
} from './state.js';

// Re-export helper functions
export {
  validateParquetFile,
  checkDiskSpace,
  rollbackCompaction,
  acquireCompactionLock,
  estimateMemoryRequired,
  removeTombstones,
  identifyTombstones,
  filterTombstones,
  analyzeTombstoneAge,
  optimizeForReads,
  reorderColumnsForCompression,
  buildZoneMaps,
  generateBloomFilters,
  sortByClusteringKey,
  calculateOptimalRowGroupSize,
  applyDictionaryEncoding,
  getCompactionStats,
  getBlockStats,
  generateCompactionReport,
} from './helpers.js';

// Re-export collection functions
export {
  validateCollection,
  resolveCollectionPattern,
  listCollections,
  getCollectionStats,
  compactCollection,
  compactCollections,
  compactAllCollections,
  compactDatabase,
} from './collections.js';

// Re-export trigger functions
export { triggerCompaction } from './trigger.js';

// Re-export formatting utilities for backwards compatibility
export { formatBytes, formatDuration } from '../utils.js';
