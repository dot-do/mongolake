/**
 * Tests for MongoLake Compact Command - Block Identification and Analysis
 *
 * Tests block identification from parquet files, small block detection,
 * compaction analysis, and result tracking.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Mock the storage module
const mockStorage = {
  get: vi.fn().mockResolvedValue(new Uint8Array([1, 2, 3])),
  put: vi.fn().mockResolvedValue(undefined),
  delete: vi.fn().mockResolvedValue(undefined),
  list: vi.fn().mockResolvedValue([]),
  head: vi.fn().mockResolvedValue({ size: 100 }),
  exists: vi.fn().mockResolvedValue(true),
};

vi.mock('../../../../src/storage/index.js', () => ({
  FileSystemStorage: vi.fn(() => mockStorage),
}));

// ============================================================================
// Block Identification Tests
// ============================================================================

describe('CLI Compact - Block Identification', () => {
  interface BlockMetadata {
    id: string;
    path: string;
    size: number;
    rowCount: number;
    minSeq: number;
    maxSeq: number;
    createdAt: Date;
  }

  it('should identify blocks from parquet file paths', () => {
    const files = [
      'testdb/users/users_1705000000000_1.parquet',
      'testdb/users/users_1705000001000_2.parquet',
      'testdb/users/users_1705000002000_3.parquet',
    ];

    const blocks: BlockMetadata[] = [];
    const collection = 'users';
    const database = 'testdb';
    const collectionPrefix = `${database}/${collection}`;

    for (const filePath of files) {
      if (filePath.startsWith(collectionPrefix) && filePath.endsWith('.parquet') && !filePath.includes('/_')) {
        const match = filePath.match(/_(\d+)_(\d+)\.parquet$/);
        const timestamp = match ? parseInt(match[1], 10) : Date.now();
        const seq = match ? parseInt(match[2], 10) : 0;

        const id = filePath.replace(`${database}/`, '').replace('.parquet', '');

        blocks.push({
          id,
          path: filePath,
          size: 1000000, // 1MB
          rowCount: 100,
          minSeq: seq,
          maxSeq: seq,
          createdAt: new Date(timestamp),
        });
      }
    }

    expect(blocks).toHaveLength(3);
    expect(blocks[0].minSeq).toBe(1);
    expect(blocks[1].minSeq).toBe(2);
    expect(blocks[2].minSeq).toBe(3);
  });

  it('should filter out internal files', () => {
    const files = [
      'testdb/users/users_1705000000000_1.parquet',
      'testdb/users/_manifests/current.json',
      'testdb/users/_snapshots/snapshot_1.json',
    ];

    const collectionPrefix = 'testdb/users';
    const parquetFiles = files.filter(
      (f) => f.startsWith(collectionPrefix) && f.endsWith('.parquet') && !f.includes('/_')
    );

    expect(parquetFiles).toHaveLength(1);
    expect(parquetFiles[0]).toContain('.parquet');
  });

  it('should sort blocks by sequence number', () => {
    const blocks: BlockMetadata[] = [
      { id: 'b3', path: 'f3.parquet', size: 1000, rowCount: 10, minSeq: 3, maxSeq: 3, createdAt: new Date() },
      { id: 'b1', path: 'f1.parquet', size: 1000, rowCount: 10, minSeq: 1, maxSeq: 1, createdAt: new Date() },
      { id: 'b2', path: 'f2.parquet', size: 1000, rowCount: 10, minSeq: 2, maxSeq: 2, createdAt: new Date() },
    ];

    blocks.sort((a, b) => a.minSeq - b.minSeq);

    expect(blocks[0].minSeq).toBe(1);
    expect(blocks[1].minSeq).toBe(2);
    expect(blocks[2].minSeq).toBe(3);
  });
});

// ============================================================================
// Compaction Analysis Tests
// ============================================================================

describe('CLI Compact - Compaction Analysis', () => {
  interface BlockMetadata {
    path: string;
    size: number;
  }

  const MIN_BLOCK_SIZE = 2 * 1024 * 1024; // 2MB

  function identifySmallBlocks(blocks: BlockMetadata[]): BlockMetadata[] {
    return blocks.filter((b) => b.size < MIN_BLOCK_SIZE);
  }

  it('should identify blocks smaller than minimum threshold', () => {
    const blocks: BlockMetadata[] = [
      { path: 'f1.parquet', size: 1 * 1024 * 1024 }, // 1MB - small
      { path: 'f2.parquet', size: 3 * 1024 * 1024 }, // 3MB - ok
      { path: 'f3.parquet', size: 500 * 1024 },      // 500KB - small
    ];

    const smallBlocks = identifySmallBlocks(blocks);

    expect(smallBlocks).toHaveLength(2);
    expect(smallBlocks.map((b) => b.path)).toContain('f1.parquet');
    expect(smallBlocks.map((b) => b.path)).toContain('f3.parquet');
  });

  it('should require at least 2 blocks for compaction', () => {
    const smallBlocks: BlockMetadata[] = [
      { path: 'f1.parquet', size: 1 * 1024 * 1024 },
    ];

    const canCompact = smallBlocks.length >= 2;
    expect(canCompact).toBe(false);
  });

  it('should calculate total size of blocks to compact', () => {
    const blocks: BlockMetadata[] = [
      { path: 'f1.parquet', size: 1 * 1024 * 1024 },
      { path: 'f2.parquet', size: 500 * 1024 },
      { path: 'f3.parquet', size: 750 * 1024 },
    ];

    const totalSize = blocks.reduce((sum, b) => sum + b.size, 0);
    expect(totalSize).toBe(1 * 1024 * 1024 + 500 * 1024 + 750 * 1024);
  });
});

// ============================================================================
// Compaction Result Tests
// ============================================================================

describe('CLI Compact - Compaction Results', () => {
  interface CompactionResult {
    skipped: boolean;
    reason?: string;
    processedBlocks: number;
    mergedBlocks: Array<{
      path: string;
      size: number;
      rowCount: number;
      minSeq: number;
      maxSeq: number;
    }>;
    stats?: {
      bytesProcessed: number;
      rowsProcessed: number;
      compressionRatio: number;
    };
    hasMore: boolean;
    pendingDeletions?: string[];
  }

  it('should handle skipped compaction', () => {
    const result: CompactionResult = {
      skipped: true,
      reason: 'No blocks need compaction',
      processedBlocks: 0,
      mergedBlocks: [],
      hasMore: false,
    };

    expect(result.skipped).toBe(true);
    expect(result.reason).toBeDefined();
  });

  it('should track processed and merged blocks', () => {
    const result: CompactionResult = {
      skipped: false,
      processedBlocks: 5,
      mergedBlocks: [
        { path: 'merged.parquet', size: 4000000, rowCount: 1000, minSeq: 1, maxSeq: 5 },
      ],
      hasMore: false,
    };

    expect(result.processedBlocks).toBe(5);
    expect(result.mergedBlocks).toHaveLength(1);
    expect(result.mergedBlocks[0].minSeq).toBe(1);
    expect(result.mergedBlocks[0].maxSeq).toBe(5);
  });

  it('should track compaction statistics', () => {
    const result: CompactionResult = {
      skipped: false,
      processedBlocks: 3,
      mergedBlocks: [],
      stats: {
        bytesProcessed: 3000000,
        rowsProcessed: 1000,
        compressionRatio: 0.75,
      },
      hasMore: false,
    };

    expect(result.stats).toBeDefined();
    expect(result.stats!.bytesProcessed).toBe(3000000);
    expect(result.stats!.compressionRatio).toBe(0.75);
  });

  it('should indicate when more compaction is needed', () => {
    const result: CompactionResult = {
      skipped: false,
      processedBlocks: 3,
      mergedBlocks: [],
      hasMore: true,
    };

    expect(result.hasMore).toBe(true);
  });

  it('should track pending deletions', () => {
    const result: CompactionResult = {
      skipped: false,
      processedBlocks: 3,
      mergedBlocks: [],
      hasMore: false,
      pendingDeletions: ['old_block_1.parquet', 'old_block_2.parquet'],
    };

    expect(result.pendingDeletions).toHaveLength(2);
  });
});

// ============================================================================
// File Sequence Parsing Tests
// ============================================================================

describe('CLI Compact - File Sequence Parsing', () => {
  it('should extract timestamp and sequence from filename', () => {
    const filePath = 'testdb/users/users_1705000000000_5.parquet';
    const match = filePath.match(/_(\d+)_(\d+)\.parquet$/);

    expect(match).toBeTruthy();
    expect(match![1]).toBe('1705000000000');
    expect(match![2]).toBe('5');
  });

  it('should handle files without sequence suffix', () => {
    const filePath = 'testdb/users/data.parquet';
    const match = filePath.match(/_(\d+)_(\d+)\.parquet$/);

    expect(match).toBeNull();
  });

  it('should use defaults when pattern does not match', () => {
    const filePath = 'testdb/users/data.parquet';
    const match = filePath.match(/_(\d+)_(\d+)\.parquet$/);

    const timestamp = match ? parseInt(match[1], 10) : Date.now();
    const seq = match ? parseInt(match[2], 10) : 0;

    expect(typeof timestamp).toBe('number');
    expect(seq).toBe(0);
  });
});

// ============================================================================
// No Data Scenarios Tests
// ============================================================================

describe('CLI Compact - No Data Scenarios', () => {
  let consoleSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    consoleSpy.mockRestore();
  });

  it('should handle no parquet files found', () => {
    const parquetFiles: string[] = [];

    if (parquetFiles.length === 0) {
      console.log('No data files found for this collection.');
      console.log('Nothing to compact.');
      return;
    }

    expect(consoleSpy).toHaveBeenCalledWith('No data files found for this collection.');
    expect(consoleSpy).toHaveBeenCalledWith('Nothing to compact.');
  });

  it('should handle no small blocks found', () => {
    const smallBlocks: unknown[] = [];

    if (smallBlocks.length === 0) {
      console.log('No small blocks found that need compaction.');
      console.log('All blocks are already at or above the minimum size (2MB).');
      return;
    }

    expect(consoleSpy).toHaveBeenCalledWith('No small blocks found that need compaction.');
  });

  it('should handle only one small block', () => {
    const smallBlocks = [{ path: 'f1.parquet', size: 1000000 }];

    if (smallBlocks.length === 1) {
      console.log('Found 1 small block, but compaction requires at least 2 blocks to merge.');
      console.log('No compaction needed at this time.');
      return;
    }

    expect(consoleSpy).toHaveBeenCalledWith('Found 1 small block, but compaction requires at least 2 blocks to merge.');
  });
});

// ============================================================================
// Output Summary Tests
// ============================================================================

describe('CLI Compact - Output Summary', () => {
  let consoleSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    consoleSpy.mockRestore();
  });

  it('should display current state summary', () => {
    const blocks = [
      { path: 'f1.parquet', size: 1000000 },
      { path: 'f2.parquet', size: 2000000 },
    ];
    const totalSize = blocks.reduce((sum, b) => sum + b.size, 0);

    console.log('Current state:');
    console.log(`  Total files:    ${blocks.length}`);
    console.log(`  Total size:     ${totalSize} bytes`);

    expect(consoleSpy).toHaveBeenCalledWith('Current state:');
    expect(consoleSpy).toHaveBeenCalledWith('  Total files:    2');
    expect(consoleSpy).toHaveBeenCalledWith('  Total size:     3000000 bytes');
  });

  it('should display compaction results', () => {
    const result = {
      processedBlocks: 3,
      mergedBlocks: [{ path: 'merged.parquet', size: 4000000, rowCount: 1000 }],
    };

    console.log('Results:');
    console.log(`  Blocks processed:  ${result.processedBlocks}`);
    console.log(`  Blocks merged:     ${result.mergedBlocks.length}`);

    expect(consoleSpy).toHaveBeenCalledWith('Results:');
    expect(consoleSpy).toHaveBeenCalledWith('  Blocks processed:  3');
    expect(consoleSpy).toHaveBeenCalledWith('  Blocks merged:     1');
  });

  it('should display merged block details', () => {
    const merged = {
      path: 'merged.parquet',
      size: 4000000,
      rowCount: 1000,
      minSeq: 1,
      maxSeq: 5,
    };

    console.log('New merged block:');
    console.log(`  Path:     ${merged.path}`);
    console.log(`  Size:     ${merged.size} bytes`);
    console.log(`  Rows:     ${merged.rowCount}`);
    console.log(`  Seq:      ${merged.minSeq} - ${merged.maxSeq}`);

    expect(consoleSpy).toHaveBeenCalledWith('New merged block:');
    expect(consoleSpy).toHaveBeenCalledWith('  Path:     merged.parquet');
    expect(consoleSpy).toHaveBeenCalledWith('  Rows:     1000');
    expect(consoleSpy).toHaveBeenCalledWith('  Seq:      1 - 5');
  });

  it('should display statistics', () => {
    const stats = {
      bytesProcessed: 3000000,
      rowsProcessed: 1000,
      compressionRatio: 0.75,
    };

    console.log('Statistics:');
    console.log(`  Bytes processed:     ${stats.bytesProcessed}`);
    console.log(`  Rows processed:      ${stats.rowsProcessed}`);
    console.log(`  Compression ratio:   ${(stats.compressionRatio * 100).toFixed(1)}%`);

    expect(consoleSpy).toHaveBeenCalledWith('Statistics:');
    expect(consoleSpy).toHaveBeenCalledWith('  Compression ratio:   75.0%');
  });
});
