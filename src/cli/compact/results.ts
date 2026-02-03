/**
 * Result factory functions for the Compact command
 *
 * @module cli/compact/results
 */

import type { CompactResult, CompactStats } from './types.js';

/**
 * Create empty stats with default values
 */
export function createEmptyStats(_startTime: number): CompactStats {
  return {
    bytesProcessed: 0,
    rowsProcessed: 0,
    compressionRatio: 0,
    tombstonesRemoved: 0,
    spaceReclaimed: 0,
    spaceSaved: 0,
    spaceSavedPercent: 0,
    filesBefore: 0,
    filesAfter: 0,
    filesRemoved: 0,
    compressionBefore: 0,
    compressionAfter: 0,
    compressionImprovement: 0,
  };
}

/**
 * Create a result for skipped compaction operations
 */
export function createSkippedResult(
  reason: string,
  startTime: number,
  resumedFrom?: string,
  sortedBy?: string
): CompactResult {
  return {
    success: true,
    skipped: true,
    reason,
    processedBlocks: 0,
    mergedBlocks: [],
    tombstonesRemoved: 0,
    stats: createEmptyStats(startTime),
    resumedFrom,
    sortedBy,
    sortOrder: sortedBy ? 'ascending' : undefined,
  };
}

/**
 * Create a result for errored compaction operations
 */
export function createErrorResult(code: string, message: string, startTime: number): CompactResult {
  return {
    success: false,
    skipped: false,
    processedBlocks: 0,
    mergedBlocks: [],
    error: { code, message },
    stats: createEmptyStats(startTime),
  };
}

/**
 * Create a result for aborted compaction operations
 */
export function createAbortedResult(): CompactResult {
  return {
    success: false,
    skipped: false,
    processedBlocks: 0,
    mergedBlocks: [],
    aborted: true,
    cleanedUp: true,
    stats: {
      bytesProcessed: 0,
      rowsProcessed: 0,
      compressionRatio: 0,
    },
  };
}
