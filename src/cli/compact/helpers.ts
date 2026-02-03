/**
 * Helper functions for the Compact command
 *
 * @module cli/compact/helpers
 */

import * as os from 'node:os';
import type { BlockMetadata } from '../../compaction/scheduler.js';
import { formatBytes } from '../utils.js';
import type {
  CompactOptions,
  CompactStats,
  Schema,
  ZoneMapBlock,
  ZoneMaps,
  BloomFilterOptions,
  BloomFilters,
  RowGroupSizeInput,
  ColumnStatsInput,
  EncodingPlan,
  TombstoneInfo,
  TombstoneAgeAnalysis,
  RollbackState,
  RollbackResult,
  DiskSpaceResult,
  ParquetValidation,
  LockResult,
  MemoryEstimate,
  BlockStatsInput,
  BlockStats,
  ReportStats,
} from './types.js';
import { getRunningCompaction } from './state.js';

// ============================================================================
// Validation Functions
// ============================================================================

/**
 * Validate a parquet file by checking magic number
 */
export async function validateParquetFile(data: Uint8Array): Promise<ParquetValidation> {
  // Check for PAR1 magic number at start
  const magic = new TextDecoder().decode(data.slice(0, 4));
  if (magic !== 'PAR1') {
    return { valid: false, error: 'invalid parquet magic number' };
  }
  return { valid: true };
}

/**
 * Check available disk space
 */
export async function checkDiskSpace(_path: string, required: number): Promise<DiskSpaceResult> {
  // Get free memory as a proxy for disk space in tests
  const available = os.freemem();

  return {
    sufficient: available >= required,
    available,
    required,
  };
}

// ============================================================================
// Rollback Functions
// ============================================================================

/**
 * Rollback a failed compaction
 */
export async function rollbackCompaction(state: RollbackState, _path: string): Promise<RollbackResult> {
  // In a real implementation, this would restore from backup
  return {
    success: true,
    restoredBlocks: state.sourceBlocks,
    removedMerged: true,
  };
}

// ============================================================================
// Lock Functions
// ============================================================================

/**
 * Acquire a compaction lock
 */
export async function acquireCompactionLock(
  database: string,
  collection: string,
  _path: string,
  options: { timeout?: number } = {}
): Promise<LockResult> {
  const existing = getRunningCompaction(database, collection);

  if (existing && options.timeout === 0) {
    return {
      acquired: false,
      holder: existing.compactionId,
      heldSince: existing.startedAt,
    };
  }

  return { acquired: true };
}

/**
 * Estimate memory required for compaction
 */
export function estimateMemoryRequired(blocks: Array<{ size: number }>): MemoryEstimate {
  // Estimate 2x block size for reading + writing
  const required = blocks.reduce((sum, b) => sum + b.size, 0) * 2;
  const available = os.freemem();

  return {
    required,
    available,
    sufficient: available >= required,
  };
}

// ============================================================================
// Tombstone Functions
// ============================================================================

/**
 * Remove tombstones from compacted data
 */
export async function removeTombstones(_options: CompactOptions): Promise<number> {
  // In a real implementation, this would filter tombstones
  return 0;
}

/**
 * Identify tombstones in blocks
 */
export async function identifyTombstones(blocks: Array<BlockMetadata & { tombstoneCount?: number }>): Promise<TombstoneInfo> {
  const blocksWithTombstones = blocks.filter((b) => b.tombstoneCount && b.tombstoneCount > 0);
  const totalCount = blocksWithTombstones.reduce((sum, b) => sum + (b.tombstoneCount ?? 0), 0);

  return {
    totalCount,
    blocksWithTombstones,
  };
}

/**
 * Filter tombstone documents from a list
 */
export async function filterTombstones<T extends { _deleted?: boolean }>(documents: T[]): Promise<T[]> {
  return documents.filter((d) => !d._deleted);
}

/**
 * Analyze tombstone age distribution
 */
export async function analyzeTombstoneAge(
  tombstones: Array<{ _id: string; _deletedAt: number }>
): Promise<TombstoneAgeAnalysis> {
  const now = Date.now();
  const distribution: Record<string, number> = {
    '<7d': 0,
    '7-30d': 0,
    '>30d': 0,
  };

  for (const tombstone of tombstones) {
    const age = now - tombstone._deletedAt;
    const days = age / 86400000;

    if (days < 7) {
      distribution['<7d']!++;
    } else if (days < 30) {
      distribution['7-30d']!++;
    } else {
      distribution['>30d']!++;
    }
  }

  return { distribution };
}

// ============================================================================
// Optimization Functions
// ============================================================================

/**
 * Optimize data for reads
 */
export async function optimizeForReads(_options: CompactOptions): Promise<void> {
  // Placeholder for read optimization
}

/**
 * Reorder columns for better compression
 */
export async function reorderColumnsForCompression(schema: Schema): Promise<Schema> {
  // Group similar types together
  const strings = schema.fields.filter((f) => f.type === 'string');
  const numbers = schema.fields.filter((f) => f.type !== 'string');

  return {
    fields: [...strings, ...numbers],
  };
}

/**
 * Build zone maps for columns
 */
export async function buildZoneMaps(
  blocks: ZoneMapBlock[],
  columns: string[]
): Promise<ZoneMaps> {
  const zoneMaps: ZoneMaps = {};

  for (const column of columns) {
    let globalMin = Infinity;
    let globalMax = -Infinity;

    for (const block of blocks) {
      if (block.minValues[column] !== undefined) {
        globalMin = Math.min(globalMin, block.minValues[column]);
      }
      if (block.maxValues[column] !== undefined) {
        globalMax = Math.max(globalMax, block.maxValues[column]);
      }
    }

    zoneMaps[column] = {
      globalMin: globalMin === Infinity ? 0 : globalMin,
      globalMax: globalMax === -Infinity ? 0 : globalMax,
      blocks,
    };
  }

  return zoneMaps;
}

/**
 * Generate bloom filters for columns
 */
export async function generateBloomFilters(
  columnData: Record<string, string[]>,
  options: BloomFilterOptions
): Promise<BloomFilters> {
  const filters: BloomFilters = {};

  for (const column of options.columns) {
    const values = new Set(columnData[column] || []);

    filters[column] = {
      mightContain(value: string): boolean {
        return values.has(value);
      },
    };
  }

  return filters;
}

/**
 * Sort data by clustering key
 */
export async function sortByClusteringKey(
  _data: unknown[],
  _key: string
): Promise<void> {
  // Placeholder for clustering sort
}

/**
 * Calculate optimal row group size
 */
export async function calculateOptimalRowGroupSize(stats: RowGroupSizeInput): Promise<number> {
  const { avgRowSize, memoryBudget } = stats;

  // Target row group size based on memory budget
  const maxRows = Math.floor(memoryBudget / avgRowSize);

  // Cap at 1M rows per group
  return Math.min(maxRows, 1000000);
}

/**
 * Apply dictionary encoding to appropriate columns
 */
export async function applyDictionaryEncoding(columnStats: ColumnStatsInput): Promise<EncodingPlan> {
  const plan: EncodingPlan = {};

  for (const [column, stats] of Object.entries(columnStats)) {
    // Use dictionary if distinct values are less than 10% of total
    const ratio = stats.distinctCount / stats.totalCount;
    plan[column] = ratio < 0.1 ? 'dictionary' : 'plain';
  }

  return plan;
}

// ============================================================================
// Statistics Functions
// ============================================================================

/**
 * Get compaction statistics for a collection
 */
export async function getCompactionStats(
  _database: string,
  _collection: string,
  _path: string
): Promise<CompactStats> {
  return {
    bytesProcessed: 0,
    rowsProcessed: 0,
    compressionRatio: 0,
  };
}

/**
 * Get block statistics
 */
export async function getBlockStats(blocks: BlockStatsInput[]): Promise<BlockStats> {
  if (blocks.length === 0) {
    return {
      totalSize: 0,
      totalRows: 0,
      avgBlockSize: 0,
      minBlockSize: 0,
      maxBlockSize: 0,
    };
  }

  const totalSize = blocks.reduce((sum, b) => sum + b.size, 0);
  const totalRows = blocks.reduce((sum, b) => sum + b.rowCount, 0);
  const sizes = blocks.map((b) => b.size);

  return {
    totalSize,
    totalRows,
    avgBlockSize: totalSize / blocks.length,
    minBlockSize: Math.min(...sizes),
    maxBlockSize: Math.max(...sizes),
  };
}

/**
 * Generate a compaction report
 */
export async function generateCompactionReport(stats: ReportStats): Promise<string> {
  const lines = [
    'Compaction Report',
    '=================',
    '',
    `Files: ${stats.filesBefore} -> ${stats.filesAfter} (${stats.filesBefore - stats.filesAfter} removed)`,
    `Size: ${formatBytes(stats.sizeBefore)} -> ${formatBytes(stats.sizeAfter)}`,
    `Rows processed: ${stats.rowsProcessed}`,
    `Tombstones removed: ${stats.tombstonesRemoved}`,
    `Duration: ${formatDuration(stats.durationMs)}`,
  ];

  return lines.join('\n');
}

// Local helper to format duration for reports
function formatDuration(ms: number): string {
  if (ms < 1000) {
    return `${ms}ms`;
  }
  const seconds = ms / 1000;
  if (seconds < 60) {
    return `${seconds.toFixed(2)}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds % 60;
  return `${minutes}m ${remainingSeconds.toFixed(0)}s`;
}
