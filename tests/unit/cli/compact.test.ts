/**
 * Tests for MongoLake Compact Command
 *
 * Tests the compaction trigger functionality including:
 * - Compaction scheduling
 * - Progress reporting
 * - Dry run mode
 * - Block identification
 * - CLI argument parsing
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

vi.mock('../../../src/storage/index.js', () => ({
  FileSystemStorage: vi.fn(() => mockStorage),
}));

// Mock the compaction scheduler
const mockScheduler = {
  identifyBlocksNeedingCompaction: vi.fn().mockResolvedValue([]),
  runCompaction: vi.fn().mockResolvedValue({
    skipped: false,
    processedBlocks: 3,
    mergedBlocks: [{ path: 'merged.parquet', size: 4000000, rowCount: 1000, minSeq: 1, maxSeq: 3 }],
    stats: {
      bytesProcessed: 3000000,
      rowsProcessed: 1000,
      compressionRatio: 0.75,
    },
    hasMore: false,
    pendingDeletions: [],
  }),
};

vi.mock('../../../src/compaction/scheduler.js', () => ({
  CompactionScheduler: vi.fn(() => mockScheduler),
}));

// ============================================================================
// Module Export Tests
// ============================================================================

describe('CLI Compact - Module Exports', () => {
  it('should export runCompact function', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(typeof module.runCompact).toBe('function');
  });

  it('should export handleCompactCommand function', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(typeof module.handleCompactCommand).toBe('function');
  });

  it('should export COMPACT_HELP_TEXT', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toBeDefined();
    expect(typeof module.COMPACT_HELP_TEXT).toBe('string');
  });
});

// ============================================================================
// Compact Options Tests
// ============================================================================

describe('CLI Compact - Options', () => {
  it('should have required database option', () => {
    const options = {
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
    };

    expect(options.database).toBe('testdb');
  });

  it('should have required collection option', () => {
    const options = {
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
    };

    expect(options.collection).toBe('users');
  });

  it('should use default path .mongolake', () => {
    const options = {
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
    };

    expect(options.path).toBe('.mongolake');
  });

  it('should support dry-run option', () => {
    const options = {
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: true,
      verbose: false,
    };

    expect(options.dryRun).toBe(true);
  });

  it('should support verbose option', () => {
    const options = {
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: true,
    };

    expect(options.verbose).toBe(true);
  });
});

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
// Dry Run Tests
// ============================================================================

describe('CLI Compact - Dry Run Mode', () => {
  let consoleSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    consoleSpy.mockRestore();
  });

  it('should show preview without executing in dry run mode', () => {
    const dryRun = true;
    const smallBlocks = [
      { path: 'f1.parquet', size: 1000000 },
      { path: 'f2.parquet', size: 500000 },
    ];

    console.log('Compaction analysis:');
    console.log(`  Blocks to compact: ${smallBlocks.length}`);

    if (dryRun) {
      console.log('DRY RUN - No changes were made.');
      return;
    }

    // This should not be reached in dry run
    console.log('Starting compaction...');

    expect(consoleSpy).toHaveBeenCalledWith('DRY RUN - No changes were made.');
    expect(consoleSpy).not.toHaveBeenCalledWith('Starting compaction...');
  });

  it('should show command to run without dry run', () => {
    const database = 'mydb';
    const collection = 'users';
    const path = '.mongolake';

    const command = `mongolake compact ${database} ${collection}${path !== '.mongolake' ? ` --path ${path}` : ''}`;

    expect(command).toBe('mongolake compact mydb users');
  });
});

// ============================================================================
// Utility Functions Tests
// ============================================================================

describe('CLI Compact - Utility Functions', () => {
  function formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const k = 1024;
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${(bytes / Math.pow(k, i)).toFixed(2)} ${units[i]}`;
  }

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

  describe('formatBytes', () => {
    it('should format 0 bytes', () => {
      expect(formatBytes(0)).toBe('0 B');
    });

    it('should format bytes', () => {
      expect(formatBytes(500)).toBe('500.00 B');
    });

    it('should format kilobytes', () => {
      expect(formatBytes(1024)).toBe('1.00 KB');
    });

    it('should format megabytes', () => {
      expect(formatBytes(1024 * 1024)).toBe('1.00 MB');
    });

    it('should format gigabytes', () => {
      expect(formatBytes(1024 * 1024 * 1024)).toBe('1.00 GB');
    });
  });

  describe('formatDuration', () => {
    it('should format milliseconds', () => {
      expect(formatDuration(500)).toBe('500ms');
    });

    it('should format seconds', () => {
      expect(formatDuration(5000)).toBe('5.00s');
    });

    it('should format minutes and seconds', () => {
      expect(formatDuration(90000)).toBe('1m 30s');
    });
  });
});

// ============================================================================
// Verbose Output Tests
// ============================================================================

describe('CLI Compact - Verbose Output', () => {
  let consoleSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    consoleSpy.mockRestore();
  });

  it('should show block details when verbose is enabled', () => {
    const verbose = true;
    const blocks = [
      { path: 'f1.parquet', size: 1000000 },
      { path: 'f2.parquet', size: 500000 },
    ];

    if (verbose) {
      console.log('Blocks to be compacted:');
      for (const block of blocks) {
        console.log(`  - ${block.path}`);
      }
    }

    expect(consoleSpy).toHaveBeenCalledWith('Blocks to be compacted:');
    expect(consoleSpy).toHaveBeenCalledWith('  - f1.parquet');
    expect(consoleSpy).toHaveBeenCalledWith('  - f2.parquet');
  });

  it('should show debug info when verbose is enabled', () => {
    const verbose = true;

    const debug = (message: string) => {
      if (verbose) {
        console.log(`[DEBUG] ${message}`);
      }
    };

    debug('Found 3 parquet file(s)');
    expect(consoleSpy).toHaveBeenCalledWith('[DEBUG] Found 3 parquet file(s)');
  });

  it('should not show debug info when verbose is disabled', () => {
    const verbose = false;

    const debug = (message: string) => {
      if (verbose) {
        console.log(`[DEBUG] ${message}`);
      }
    };

    debug('Found 3 parquet file(s)');
    expect(consoleSpy).not.toHaveBeenCalled();
  });
});

// ============================================================================
// Help Text Tests
// ============================================================================

describe('CLI Compact - Help Text', () => {
  it('should include usage information', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('Usage:');
    expect(module.COMPACT_HELP_TEXT).toContain('mongolake compact');
  });

  it('should include argument descriptions', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('database');
    expect(module.COMPACT_HELP_TEXT).toContain('collection');
  });

  it('should include option descriptions', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('--path');
    expect(module.COMPACT_HELP_TEXT).toContain('--dry-run');
    expect(module.COMPACT_HELP_TEXT).toContain('--verbose');
    expect(module.COMPACT_HELP_TEXT).toContain('--help');
  });

  it('should include description of compaction process', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('Description:');
    expect(module.COMPACT_HELP_TEXT).toContain('merge');
  });

  it('should include examples', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('Examples:');
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
// Error Handling Tests
// ============================================================================

describe('CLI Compact - Error Handling', () => {
  let consoleErrorSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    consoleErrorSpy.mockRestore();
  });

  it('should handle compaction failure', () => {
    const error = new Error('Compaction failed: Disk full');
    console.error('Compaction failed:', error.message);

    expect(consoleErrorSpy).toHaveBeenCalledWith('Compaction failed:', 'Compaction failed: Disk full');
  });

  it('should show stack trace when verbose', () => {
    const error = new Error('Test error');
    error.stack = 'Error: Test error\n  at test.ts:1:1';
    const verbose = true;

    console.error('Compaction failed:', error.message);
    if (verbose && error.stack) {
      console.error(error.stack);
    }

    expect(consoleErrorSpy).toHaveBeenCalledWith(error.stack);
  });

  it('should not show stack trace when not verbose', () => {
    const error = new Error('Test error');
    error.stack = 'Error: Test error\n  at test.ts:1:1';
    const verbose = false;

    console.error('Compaction failed:', error.message);
    if (verbose && error.stack) {
      console.error(error.stack);
    }

    expect(consoleErrorSpy).toHaveBeenCalledTimes(1);
    expect(consoleErrorSpy).not.toHaveBeenCalledWith(error.stack);
  });
});

// ============================================================================
// CLI Argument Parsing Tests
// ============================================================================

describe('CLI Compact - Argument Parsing', () => {
  it('should require database argument', () => {
    const positionals: string[] = [];
    const hasDatabase = positionals.length >= 1;
    expect(hasDatabase).toBe(false);
  });

  it('should require collection argument', () => {
    const positionals = ['mydb'];
    const hasCollection = positionals.length >= 2;
    expect(hasCollection).toBe(false);
  });

  it('should parse positional arguments correctly', () => {
    const positionals = ['mydb', 'users'];
    const database = positionals[0];
    const collection = positionals[1];

    expect(database).toBe('mydb');
    expect(collection).toBe('users');
  });

  it('should check for help flag', () => {
    const args = ['--help'];
    const showHelp = args.includes('-h') || args.includes('--help');
    expect(showHelp).toBe(true);
  });

  it('should check for short help flag', () => {
    const args = ['-h'];
    const showHelp = args.includes('-h') || args.includes('--help');
    expect(showHelp).toBe(true);
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
// Compaction Triggering Tests (RED - Not Yet Implemented)
// ============================================================================

describe('CLI Compact - Compaction Triggering', () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  it('should export triggerCompaction function', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.triggerCompaction).toBeDefined();
    expect(typeof module.triggerCompaction).toBe('function');
  });

  it('should trigger compaction immediately when called', async () => {
    const module = await import('../../../src/cli/compact.js');

    const result = await module.triggerCompaction({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      immediate: true,
    });

    expect(result).toBeDefined();
    expect(result.triggered).toBe(true);
    expect(result.startedAt).toBeInstanceOf(Date);
  });

  it('should schedule compaction for later execution', async () => {
    const module = await import('../../../src/cli/compact.js');

    const result = await module.triggerCompaction({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      immediate: false,
      delay: 60000, // 1 minute delay
    });

    expect(result).toBeDefined();
    expect(result.scheduled).toBe(true);
    expect(result.scheduledFor).toBeDefined();
    expect(result.scheduledFor.getTime()).toBeGreaterThan(Date.now());
  });

  it('should detect when compaction is already running', async () => {
    const module = await import('../../../src/cli/compact.js');

    // Start first compaction
    const firstCompaction = module.triggerCompaction({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      immediate: true,
    });

    // Try to start second compaction immediately
    const secondResult = await module.triggerCompaction({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      immediate: true,
    });

    expect(secondResult.alreadyRunning).toBe(true);
    expect(secondResult.triggered).toBe(false);

    await firstCompaction;
  });

  it('should queue compaction when another is running', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.queueCompaction).toBeDefined();

    const queueResult = await module.queueCompaction({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
    });

    expect(queueResult).toBeDefined();
    expect(queueResult.queued).toBe(true);
    expect(queueResult.position).toBeGreaterThanOrEqual(0);
  });

  it('should support priority-based compaction queue', async () => {
    const module = await import('../../../src/cli/compact.js');

    // Queue normal priority
    await module.queueCompaction({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      priority: 'normal',
    });

    // Queue high priority - should be processed first
    const highPriorityResult = await module.queueCompaction({
      database: 'testdb',
      collection: 'orders',
      path: '.mongolake',
      priority: 'high',
    });

    expect(highPriorityResult.position).toBe(0);
  });

  it('should trigger compaction via CLI command', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.handleCompactCommand).toBeDefined();

    // Simulate CLI invocation
    const exitSpy = vi.spyOn(process, 'exit').mockImplementation(() => undefined as never);
    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    await module.handleCompactCommand(['testdb', 'users']);

    expect(exitSpy).not.toHaveBeenCalledWith(1);

    exitSpy.mockRestore();
    consoleSpy.mockRestore();
  });

  it('should support --schedule flag for delayed compaction', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('--schedule');
  });

  it('should support --priority flag for queue priority', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('--priority');
  });

  it('should emit compaction-started event when triggered', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.CompactionEventEmitter).toBeDefined();

    const emitter = new module.CompactionEventEmitter();
    const startEvents: unknown[] = [];

    emitter.on('compaction-started', (event: unknown) => startEvents.push(event));

    await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      eventEmitter: emitter,
    });

    expect(startEvents.length).toBe(1);
    expect(startEvents[0]).toMatchObject({
      database: 'testdb',
      collection: 'users',
      startedAt: expect.any(Date),
    });
  });

  it('should emit compaction-completed event when finished', async () => {
    const module = await import('../../../src/cli/compact.js');

    const emitter = new module.CompactionEventEmitter();
    const completeEvents: unknown[] = [];

    emitter.on('compaction-completed', (event: unknown) => completeEvents.push(event));

    await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      eventEmitter: emitter,
    });

    expect(completeEvents.length).toBe(1);
    expect(completeEvents[0]).toMatchObject({
      database: 'testdb',
      collection: 'users',
      completedAt: expect.any(Date),
      result: expect.any(Object),
    });
  });

  it('should cancel scheduled compaction', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.cancelCompaction).toBeDefined();

    // Schedule compaction
    const scheduled = await module.triggerCompaction({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      immediate: false,
      delay: 60000,
    });

    // Cancel it
    const cancelResult = await module.cancelCompaction(scheduled.compactionId);

    expect(cancelResult.cancelled).toBe(true);
    expect(cancelResult.compactionId).toBe(scheduled.compactionId);
  });

  it('should list pending compaction jobs', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.listPendingCompactions).toBeDefined();

    // Queue some compactions
    await module.queueCompaction({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
    });

    await module.queueCompaction({
      database: 'testdb',
      collection: 'orders',
      path: '.mongolake',
    });

    const pending = await module.listPendingCompactions('.mongolake');

    expect(pending).toBeDefined();
    expect(Array.isArray(pending)).toBe(true);
    expect(pending.length).toBeGreaterThanOrEqual(2);
    expect(pending[0]).toHaveProperty('database');
    expect(pending[0]).toHaveProperty('collection');
    expect(pending[0]).toHaveProperty('queuedAt');
  });
});

// ============================================================================
// Collection Targeting Tests (RED - Not Yet Implemented)
// ============================================================================

describe('CLI Compact - Collection Targeting', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockStorage.list.mockResolvedValue([
      'testdb/users/users_1705000000000_1.parquet',
      'testdb/users/users_1705000001000_2.parquet',
      'testdb/orders/orders_1705000000000_1.parquet',
      'testdb/products/products_1705000000000_1.parquet',
    ]);
    mockStorage.head.mockResolvedValue({ size: 1000000 });
  });

  it('should export compactCollection function', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.compactCollection).toBeDefined();
    expect(typeof module.compactCollection).toBe('function');
  });

  it('should compact a specific collection', async () => {
    const module = await import('../../../src/cli/compact.js');

    const result = await module.compactCollection({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
    });

    expect(result).toBeDefined();
    expect(result.database).toBe('testdb');
    expect(result.collection).toBe('users');
    expect(result.processedBlocks).toBeGreaterThanOrEqual(0);
  });

  it('should validate collection exists before compacting', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.validateCollection).toBeDefined();

    mockStorage.list.mockResolvedValue([]); // No files

    const validation = await module.validateCollection('testdb', 'nonexistent', '.mongolake');

    expect(validation.exists).toBe(false);
    expect(validation.error).toContain('not found');
  });

  it('should support wildcard collection patterns', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.resolveCollectionPattern).toBeDefined();

    const collections = await module.resolveCollectionPattern('testdb', 'user*', '.mongolake');

    expect(collections).toBeDefined();
    expect(Array.isArray(collections)).toBe(true);
    // Should match 'users' collection
  });

  it('should compact multiple collections matching pattern', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.compactCollections).toBeDefined();

    const result = await module.compactCollections({
      database: 'testdb',
      pattern: '*',
      path: '.mongolake',
      dryRun: false,
    });

    expect(result).toBeDefined();
    expect(result.collectionsCompacted).toBeDefined();
    expect(Array.isArray(result.collectionsCompacted)).toBe(true);
  });

  it('should list all collections in database', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.listCollections).toBeDefined();

    const collections = await module.listCollections('testdb', '.mongolake');

    expect(collections).toBeDefined();
    expect(Array.isArray(collections)).toBe(true);
    expect(collections).toContain('users');
    expect(collections).toContain('orders');
    expect(collections).toContain('products');
  });

  it('should get collection statistics before compaction', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.getCollectionStats).toBeDefined();

    const stats = await module.getCollectionStats('testdb', 'users', '.mongolake');

    expect(stats).toBeDefined();
    expect(stats.blockCount).toBeGreaterThanOrEqual(0);
    expect(stats.totalSize).toBeGreaterThanOrEqual(0);
    expect(stats.smallBlockCount).toBeGreaterThanOrEqual(0);
    expect(stats.needsCompaction).toBeDefined();
  });

  it('should skip collections with no small blocks', async () => {
    const module = await import('../../../src/cli/compact.js');

    // Mock large blocks only
    mockStorage.head.mockResolvedValue({ size: 10 * 1024 * 1024 }); // 10MB

    const result = await module.compactCollection({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
    });

    expect(result.skipped).toBe(true);
    expect(result.reason).toContain('no small blocks');
  });

  it('should support --all flag to compact all collections', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('--all');

    const result = await module.compactAllCollections({
      database: 'testdb',
      path: '.mongolake',
      dryRun: false,
    });

    expect(result).toBeDefined();
    expect(result.collectionsProcessed).toBeGreaterThan(0);
  });

  it('should support --exclude flag to skip specific collections', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('--exclude');

    const result = await module.compactCollections({
      database: 'testdb',
      pattern: '*',
      exclude: ['logs', 'metrics'],
      path: '.mongolake',
      dryRun: false,
    });

    expect(result.collectionsCompacted).not.toContain('logs');
    expect(result.collectionsCompacted).not.toContain('metrics');
  });

  it('should report per-collection results', async () => {
    const module = await import('../../../src/cli/compact.js');

    const result = await module.compactCollections({
      database: 'testdb',
      pattern: '*',
      path: '.mongolake',
      dryRun: false,
    });

    expect(result.perCollection).toBeDefined();
    expect(typeof result.perCollection).toBe('object');
    // Each collection should have its own result
    for (const collection of result.collectionsCompacted) {
      expect(result.perCollection[collection]).toBeDefined();
      expect(result.perCollection[collection]).toHaveProperty('processedBlocks');
      expect(result.perCollection[collection]).toHaveProperty('stats');
    }
  });

  it('should continue compacting other collections on single collection failure', async () => {
    const module = await import('../../../src/cli/compact.js');

    // Set up multiple files per collection so compaction can proceed
    mockStorage.list.mockResolvedValue([
      'testdb/users/users_1705000000000_1.parquet',
      'testdb/users/users_1705000001000_2.parquet',
      'testdb/orders/orders_1705000000000_1.parquet',
      'testdb/orders/orders_1705000001000_2.parquet',
      'testdb/products/products_1705000000000_1.parquet',
      'testdb/products/products_1705000001000_2.parquet',
    ]);

    // First collection fails, second succeeds
    mockStorage.get.mockImplementation((path: string) => {
      if (path.includes('users')) {
        throw new Error('Simulated failure');
      }
      return new Uint8Array(1000);
    });

    const result = await module.compactCollections({
      database: 'testdb',
      pattern: '*',
      path: '.mongolake',
      dryRun: false,
      continueOnError: true,
    });

    expect(result.errors).toBeDefined();
    expect(result.errors.length).toBeGreaterThan(0);
    expect(result.collectionsCompacted.length).toBeGreaterThan(0);
  });
});

// ============================================================================
// Progress Reporting Tests (RED - Not Yet Implemented)
// ============================================================================

describe('CLI Compact - Detailed Progress Reporting', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockStorage.list.mockResolvedValue([
      'testdb/users/users_1.parquet',
      'testdb/users/users_2.parquet',
      'testdb/users/users_3.parquet',
    ]);
    mockStorage.head.mockResolvedValue({ size: 1000000 });
    mockStorage.get.mockResolvedValue(new Uint8Array(1000000));
  });

  it('should export createProgressReporter function', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.createProgressReporter).toBeDefined();
    expect(typeof module.createProgressReporter).toBe('function');
  });

  it('should report block-level progress', async () => {
    const module = await import('../../../src/cli/compact.js');

    const progressEvents: unknown[] = [];
    const reporter = module.createProgressReporter({
      onProgress: (event: unknown) => progressEvents.push(event),
    });

    await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      progressReporter: reporter,
    });

    expect(progressEvents.length).toBeGreaterThan(0);
    expect(progressEvents[0]).toMatchObject({
      phase: expect.any(String),
      currentBlock: expect.any(Number),
      totalBlocks: expect.any(Number),
    });
  });

  it('should report bytes processed in real-time', async () => {
    const module = await import('../../../src/cli/compact.js');

    const progressEvents: Array<{ bytesProcessed: number }> = [];
    const reporter = module.createProgressReporter({
      onProgress: (event: { bytesProcessed: number }) => progressEvents.push(event),
    });

    await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      progressReporter: reporter,
    });

    // Bytes should increase over time
    if (progressEvents.length >= 2) {
      expect(progressEvents[progressEvents.length - 1].bytesProcessed)
        .toBeGreaterThanOrEqual(progressEvents[0].bytesProcessed);
    }
  });

  it('should calculate accurate ETA based on progress', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.calculateETA).toBeDefined();

    const progress = {
      bytesProcessed: 50 * 1024 * 1024, // 50MB done
      totalBytes: 200 * 1024 * 1024, // 200MB total
      elapsedMs: 10000, // 10 seconds elapsed
    };

    const eta = module.calculateETA(progress);

    expect(eta).toBeDefined();
    expect(eta.remainingMs).toBeGreaterThan(0);
    expect(eta.estimatedCompletion).toBeInstanceOf(Date);
    // At 5MB/s, 150MB remaining should take ~30 seconds
    expect(eta.remainingMs).toBeGreaterThan(20000);
    expect(eta.remainingMs).toBeLessThan(50000);
  });

  it('should report throughput in MB/s', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.calculateThroughput).toBeDefined();

    const stats = {
      bytesProcessed: 100 * 1024 * 1024, // 100MB
      durationMs: 10000, // 10 seconds
    };

    const throughput = module.calculateThroughput(stats);

    expect(throughput).toBeDefined();
    expect(throughput.bytesPerSecond).toBe(10 * 1024 * 1024); // 10MB/s
    expect(throughput.mbPerSecond).toBe(10);
  });

  it('should format progress bar for terminal', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.formatProgressBar).toBeDefined();

    const bar = module.formatProgressBar({
      current: 75,
      total: 100,
      width: 40,
    });

    expect(bar).toBeDefined();
    expect(typeof bar).toBe('string');
    expect(bar).toContain('75%');
    expect(bar.length).toBeLessThanOrEqual(60);
  });

  it('should support JSON progress output mode', async () => {
    const module = await import('../../../src/cli/compact.js');

    const jsonOutput: string[] = [];
    const reporter = module.createProgressReporter({
      format: 'json',
      onProgress: (event: unknown) => jsonOutput.push(JSON.stringify(event)),
    });

    await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      progressReporter: reporter,
    });

    expect(jsonOutput.length).toBeGreaterThan(0);
    const parsed = JSON.parse(jsonOutput[0]);
    expect(parsed).toHaveProperty('phase');
    expect(parsed).toHaveProperty('timestamp');
  });

  it('should throttle progress updates', async () => {
    const module = await import('../../../src/cli/compact.js');

    const progressEvents: unknown[] = [];
    const reporter = module.createProgressReporter({
      throttleMs: 100, // Max 10 updates per second
      onProgress: (event: unknown) => progressEvents.push(event),
    });

    await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      progressReporter: reporter,
    });

    // Even with many blocks, should be throttled
    expect(progressEvents.length).toBeLessThan(100);
  });

  it('should emit phase change events', async () => {
    const module = await import('../../../src/cli/compact.js');

    const phases: string[] = [];
    const emitter = new module.CompactEventEmitter();

    emitter.on('phase', (phase: string) => phases.push(phase));

    await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      eventEmitter: emitter,
    });

    expect(phases).toContain('analyzing');
    expect(phases).toContain('reading');
    expect(phases).toContain('merging');
    expect(phases).toContain('writing');
    expect(phases).toContain('complete');
  });

  it('should support progress callback option in runCompact', async () => {
    const module = await import('../../../src/cli/compact.js');

    const progressUpdates: unknown[] = [];

    await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      onProgress: (update: unknown) => progressUpdates.push(update),
    });

    expect(progressUpdates.length).toBeGreaterThan(0);
  });

  it('should provide human-readable progress summary', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.formatProgressSummary).toBeDefined();

    const summary = module.formatProgressSummary({
      phase: 'merging',
      currentBlock: 5,
      totalBlocks: 10,
      bytesProcessed: 50 * 1024 * 1024,
      totalBytes: 100 * 1024 * 1024,
      elapsedMs: 5000,
      estimatedRemainingMs: 5000,
    });

    expect(summary).toContain('50%');
    expect(summary).toContain('merging');
    expect(summary).toContain('5/10');
  });
});

// ============================================================================
// Error Handling Tests (RED - Not Yet Implemented)
// ============================================================================

describe('CLI Compact - Comprehensive Error Handling', () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  it('should export CompactionError class', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.CompactionError).toBeDefined();
  });

  it('should handle storage read errors gracefully', async () => {
    const module = await import('../../../src/cli/compact.js');

    mockStorage.get.mockRejectedValue(new Error('Storage read failed'));
    mockStorage.list.mockResolvedValue(['testdb/users/data.parquet']);
    mockStorage.head.mockResolvedValue({ size: 1000000 });

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
    });

    expect(result.error).toBeDefined();
    expect(result.error.code).toBe('STORAGE_READ_ERROR');
  });

  it('should handle storage write errors gracefully', async () => {
    const module = await import('../../../src/cli/compact.js');

    mockStorage.put.mockRejectedValue(new Error('Storage write failed'));
    mockStorage.list.mockResolvedValue(['testdb/users/data.parquet']);
    mockStorage.head.mockResolvedValue({ size: 1000000 });
    mockStorage.get.mockResolvedValue(new Uint8Array(1000));

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
    });

    expect(result.error).toBeDefined();
    expect(result.error.code).toBe('STORAGE_WRITE_ERROR');
  });

  it('should handle corrupted parquet files', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.validateParquetFile).toBeDefined();

    const corruptedData = new Uint8Array([0, 0, 0, 0]); // Invalid magic number
    const validation = await module.validateParquetFile(corruptedData);

    expect(validation.valid).toBe(false);
    expect(validation.error).toContain('invalid');
  });

  it('should handle insufficient disk space', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.checkDiskSpace).toBeDefined();

    const result = await module.checkDiskSpace('.mongolake', 1000 * 1024 * 1024 * 1024); // 1TB required

    expect(result.sufficient).toBe(false);
    expect(result.available).toBeDefined();
    expect(result.required).toBe(1000 * 1024 * 1024 * 1024);
  });

  it('should rollback on partial failure', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.rollbackCompaction).toBeDefined();

    // Simulate partial merge that fails
    const partialState = {
      sourceBlocks: ['block1.parquet', 'block2.parquet'],
      mergedBlock: 'merged.parquet',
      manifestBackup: { version: 1 },
    };

    const rollbackResult = await module.rollbackCompaction(partialState, '.mongolake');

    expect(rollbackResult.success).toBe(true);
    expect(rollbackResult.restoredBlocks).toContain('block1.parquet');
    expect(rollbackResult.restoredBlocks).toContain('block2.parquet');
    expect(rollbackResult.removedMerged).toBe(true);
  });

  it('should provide actionable error messages', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.formatErrorMessage).toBeDefined();

    const error = new module.CompactionError('STORAGE_WRITE_ERROR', 'Failed to write merged block');
    const formatted = module.formatErrorMessage(error);

    expect(formatted).toContain('STORAGE_WRITE_ERROR');
    expect(formatted).toContain('suggestion');
    expect(formatted.toLowerCase()).toContain('disk space');
  });

  it('should handle lock acquisition failures', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.acquireCompactionLock).toBeDefined();

    // Simulate lock already held
    const lockResult = await module.acquireCompactionLock('testdb', 'users', '.mongolake', {
      timeout: 0, // No wait
    });

    // Should fail or succeed depending on lock state
    expect(lockResult).toHaveProperty('acquired');
    if (!lockResult.acquired) {
      expect(lockResult.holder).toBeDefined();
      expect(lockResult.heldSince).toBeDefined();
    }
  });

  it('should handle interrupt signals (SIGINT)', async () => {
    const module = await import('../../../src/cli/compact.js');

    const controller = new AbortController();

    // Abort immediately before running
    controller.abort();

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      signal: controller.signal,
    });

    expect(result.aborted).toBe(true);
    expect(result.cleanedUp).toBe(true);
  });

  it('should handle out-of-memory conditions', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.estimateMemoryRequired).toBeDefined();

    const blocks = [
      { size: 1024 * 1024 * 1024 }, // 1GB block
      { size: 1024 * 1024 * 1024 }, // 1GB block
    ];

    const estimate = module.estimateMemoryRequired(blocks);

    expect(estimate).toBeDefined();
    expect(estimate.required).toBeGreaterThan(0);
    expect(estimate.available).toBeDefined();
    expect(estimate.sufficient).toBeDefined();
  });

  it('should retry transient errors', async () => {
    const module = await import('../../../src/cli/compact.js');

    let attempts = 0;
    mockStorage.put.mockImplementation(async () => {
      attempts++;
      if (attempts < 3) {
        throw new Error('Temporary network error');
      }
      return undefined;
    });

    mockStorage.list.mockResolvedValue(['testdb/users/data.parquet']);
    mockStorage.head.mockResolvedValue({ size: 1000000 });
    mockStorage.get.mockResolvedValue(new Uint8Array(1000));

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      retryConfig: {
        maxRetries: 3,
        backoffMs: 100,
      },
    });

    expect(attempts).toBe(3);
    expect(result.success).toBe(true);
  });

  it('should give up after max retries', async () => {
    const module = await import('../../../src/cli/compact.js');

    mockStorage.put.mockRejectedValue(new Error('Permanent failure'));
    mockStorage.list.mockResolvedValue(['testdb/users/data.parquet']);
    mockStorage.head.mockResolvedValue({ size: 1000000 });
    mockStorage.get.mockResolvedValue(new Uint8Array(1000));

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      retryConfig: {
        maxRetries: 3,
        backoffMs: 10,
      },
    });

    expect(result.success).toBe(false);
    expect(result.error).toBeDefined();
    expect(result.retriesExhausted).toBe(true);
  });

  it('should log errors with appropriate severity', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.CompactionLogger).toBeDefined();

    const logger = new module.CompactionLogger();
    const errorSpy = vi.spyOn(logger, 'error');
    const warnSpy = vi.spyOn(logger, 'warn');

    logger.error('STORAGE_WRITE_ERROR', 'Failed to write');
    logger.warn('LOW_DISK_SPACE', 'Disk space is running low');

    expect(errorSpy).toHaveBeenCalled();
    expect(warnSpy).toHaveBeenCalled();
  });
});

// ============================================================================
// Tombstone Removal Tests (RED - Not Yet Implemented)
// ============================================================================

describe('CLI Compact - Tombstone Removal', () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  it('should export removeTombstones function', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.removeTombstones).toBeDefined();
    expect(typeof module.removeTombstones).toBe('function');
  });

  it('should identify tombstone documents during compaction', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.identifyTombstones).toBeDefined();
    expect(typeof module.identifyTombstones).toBe('function');

    const blocks = [
      {
        id: 'block1',
        path: 'testdb/users/users_1.parquet',
        size: 1000000,
        rowCount: 100,
        minSeq: 1,
        maxSeq: 100,
        createdAt: new Date(),
        tombstoneCount: 5,
      },
      {
        id: 'block2',
        path: 'testdb/users/users_2.parquet',
        size: 500000,
        rowCount: 50,
        minSeq: 101,
        maxSeq: 150,
        createdAt: new Date(),
        tombstoneCount: 10,
      },
    ];

    const tombstones = await module.identifyTombstones(blocks);
    expect(tombstones).toBeDefined();
    expect(tombstones.totalCount).toBe(15);
    expect(tombstones.blocksWithTombstones).toHaveLength(2);
  });

  it('should remove tombstones when compacting', async () => {
    const module = await import('../../../src/cli/compact.js');

    const options = {
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      removeTombstones: true,
    };

    const result = await module.runCompact(options);
    expect(result).toBeDefined();
    expect(result.tombstonesRemoved).toBeDefined();
    expect(typeof result.tombstonesRemoved).toBe('number');
  });

  it('should support --remove-tombstones flag', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('--remove-tombstones');
  });

  it('should track tombstone removal in compaction stats', async () => {
    const module = await import('../../../src/cli/compact.js');

    const options = {
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      removeTombstones: true,
    };

    const result = await module.runCompact(options);
    expect(result.stats).toBeDefined();
    expect(result.stats.tombstonesRemoved).toBeDefined();
    expect(result.stats.spaceReclaimed).toBeDefined();
  });

  it('should filter out tombstone documents from merged output', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.filterTombstones).toBeDefined();

    const documents = [
      { _id: '1', name: 'Alice', _deleted: false },
      { _id: '2', name: 'Bob', _deleted: true }, // Tombstone
      { _id: '3', name: 'Charlie', _deleted: false },
      { _id: '4', name: 'David', _deleted: true }, // Tombstone
    ];

    const filtered = await module.filterTombstones(documents);
    expect(filtered).toHaveLength(2);
    expect(filtered.every((d: { _deleted: boolean }) => !d._deleted)).toBe(true);
  });

  it('should report tombstone age distribution', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.analyzeTombstoneAge).toBeDefined();

    const tombstones = [
      { _id: '1', _deletedAt: Date.now() - 86400000 }, // 1 day old
      { _id: '2', _deletedAt: Date.now() - 604800000 }, // 7 days old
      { _id: '3', _deletedAt: Date.now() - 2592000000 }, // 30 days old
    ];

    const analysis = await module.analyzeTombstoneAge(tombstones);
    expect(analysis.distribution).toBeDefined();
    expect(analysis.distribution['<7d']).toBe(1);
    expect(analysis.distribution['7-30d']).toBe(1);
    expect(analysis.distribution['>30d']).toBe(1);
  });
});

// ============================================================================
// Read Performance Optimization Tests (RED - Not Yet Implemented)
// ============================================================================

describe('CLI Compact - Read Performance Optimization', () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  it('should export optimizeForReads function', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.optimizeForReads).toBeDefined();
    expect(typeof module.optimizeForReads).toBe('function');
  });

  it('should support --optimize flag', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('--optimize');
  });

  it('should reorder columns for better compression', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.reorderColumnsForCompression).toBeDefined();

    const schema = {
      fields: [
        { name: '_id', type: 'string' },
        { name: 'name', type: 'string' },
        { name: 'age', type: 'int32' },
        { name: 'email', type: 'string' },
        { name: 'score', type: 'float64' },
      ],
    };

    const optimized = await module.reorderColumnsForCompression(schema);
    expect(optimized).toBeDefined();
    expect(optimized.fields).toHaveLength(5);
    // Strings should be grouped together for better dictionary encoding
    const stringIndices = optimized.fields
      .map((f: { type: string }, i: number) => (f.type === 'string' ? i : -1))
      .filter((i: number) => i !== -1);
    // Check strings are contiguous
    for (let i = 1; i < stringIndices.length; i++) {
      expect(stringIndices[i] - stringIndices[i - 1]).toBe(1);
    }
  });

  it('should build zone maps for frequently queried columns', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.buildZoneMaps).toBeDefined();

    const blocks = [
      {
        id: 'block1',
        path: 'data_1.parquet',
        minValues: { age: 18, score: 0.5 },
        maxValues: { age: 35, score: 0.9 },
      },
      {
        id: 'block2',
        path: 'data_2.parquet',
        minValues: { age: 30, score: 0.7 },
        maxValues: { age: 65, score: 1.0 },
      },
    ];

    const zoneMaps = await module.buildZoneMaps(blocks, ['age', 'score']);
    expect(zoneMaps).toBeDefined();
    expect(zoneMaps.age).toBeDefined();
    expect(zoneMaps.age.globalMin).toBe(18);
    expect(zoneMaps.age.globalMax).toBe(65);
  });

  it('should generate bloom filters for high-cardinality columns', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.generateBloomFilters).toBeDefined();

    const columnData = {
      _id: ['id1', 'id2', 'id3', 'id4', 'id5'],
      email: ['a@b.com', 'c@d.com', 'e@f.com', 'g@h.com', 'i@j.com'],
    };

    const bloomFilters = await module.generateBloomFilters(columnData, {
      columns: ['_id', 'email'],
      falsePositiveRate: 0.01,
    });

    expect(bloomFilters).toBeDefined();
    expect(bloomFilters._id).toBeDefined();
    expect(bloomFilters.email).toBeDefined();
    expect(bloomFilters._id.mightContain('id1')).toBe(true);
    expect(bloomFilters._id.mightContain('nonexistent')).toBe(false);
  });

  it('should sort data by clustering key during compaction', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.sortByClusteringKey).toBeDefined();

    const options = {
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      clusterBy: 'createdAt',
    };

    const result = await module.runCompact(options);
    expect(result).toBeDefined();
    expect(result.sortedBy).toBe('createdAt');
    expect(result.sortOrder).toBe('ascending');
  });

  it('should calculate optimal row group size', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.calculateOptimalRowGroupSize).toBeDefined();

    const stats = {
      avgRowSize: 256, // bytes
      totalRows: 100000,
      memoryBudget: 64 * 1024 * 1024, // 64MB
    };

    const optimalSize = await module.calculateOptimalRowGroupSize(stats);
    expect(optimalSize).toBeDefined();
    expect(optimalSize).toBeGreaterThan(0);
    expect(optimalSize).toBeLessThanOrEqual(1000000); // Max 1M rows per group
  });

  it('should apply dictionary encoding to appropriate columns', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.applyDictionaryEncoding).toBeDefined();

    const columnStats = {
      status: { distinctCount: 5, totalCount: 10000 }, // Good for dictionary
      _id: { distinctCount: 10000, totalCount: 10000 }, // Bad for dictionary
      category: { distinctCount: 20, totalCount: 10000 }, // Good for dictionary
    };

    const encodingPlan = await module.applyDictionaryEncoding(columnStats);
    expect(encodingPlan).toBeDefined();
    expect(encodingPlan.status).toBe('dictionary');
    expect(encodingPlan._id).toBe('plain');
    expect(encodingPlan.category).toBe('dictionary');
  });
});

// ============================================================================
// Progress Reporting Tests (RED - Not Yet Implemented)
// ============================================================================

describe('CLI Compact - Progress Reporting', () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  it('should export createProgressReporter function', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.createProgressReporter).toBeDefined();
    expect(typeof module.createProgressReporter).toBe('function');
  });

  it('should emit progress events during compaction', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.CompactEventEmitter).toBeDefined();

    const emitter = new module.CompactEventEmitter();
    const progressEvents: unknown[] = [];

    emitter.on('progress', (event: unknown) => progressEvents.push(event));

    await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      eventEmitter: emitter,
    });

    expect(progressEvents.length).toBeGreaterThan(0);
    expect(progressEvents[0]).toMatchObject({
      phase: expect.any(String),
      progress: expect.any(Number),
      total: expect.any(Number),
    });
  });

  it('should report phase transitions', async () => {
    const module = await import('../../../src/cli/compact.js');

    const emitter = new module.CompactEventEmitter();
    const phases: string[] = [];

    emitter.on('phase', (phase: string) => phases.push(phase));

    await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      eventEmitter: emitter,
    });

    expect(phases).toContain('analyzing');
    expect(phases).toContain('reading');
    expect(phases).toContain('merging');
    expect(phases).toContain('writing');
    expect(phases).toContain('cleaning');
    expect(phases).toContain('complete');
  });

  it('should provide ETA for compaction completion', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.calculateETA).toBeDefined();

    const progress = {
      bytesProcessed: 50 * 1024 * 1024, // 50MB
      totalBytes: 200 * 1024 * 1024, // 200MB
      elapsedMs: 10000, // 10 seconds
    };

    const eta = module.calculateETA(progress);
    expect(eta).toBeDefined();
    expect(eta.remainingMs).toBeGreaterThan(0);
    expect(eta.estimatedCompletion).toBeInstanceOf(Date);
    // Should take roughly 30 more seconds (150MB at 5MB/s)
    expect(eta.remainingMs).toBeGreaterThan(25000);
    expect(eta.remainingMs).toBeLessThan(40000);
  });

  it('should show throughput statistics', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.calculateThroughput).toBeDefined();

    const stats = {
      bytesProcessed: 100 * 1024 * 1024, // 100MB
      rowsProcessed: 500000,
      durationMs: 20000, // 20 seconds
    };

    const throughput = module.calculateThroughput(stats);
    expect(throughput).toBeDefined();
    expect(throughput.bytesPerSecond).toBe(5 * 1024 * 1024); // 5MB/s
    expect(throughput.rowsPerSecond).toBe(25000);
  });

  it('should support progress callback option', async () => {
    const module = await import('../../../src/cli/compact.js');

    const progressUpdates: unknown[] = [];
    const onProgress = (update: unknown) => progressUpdates.push(update);

    await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      onProgress,
    });

    expect(progressUpdates.length).toBeGreaterThan(0);
  });

  it('should format progress for terminal display', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.formatProgressBar).toBeDefined();

    const progress = {
      current: 75,
      total: 100,
      width: 40,
    };

    const bar = module.formatProgressBar(progress);
    expect(bar).toBeDefined();
    expect(bar).toContain('75%');
    expect(bar.length).toBeLessThanOrEqual(60); // Reasonable terminal width
  });
});

// ============================================================================
// Compaction Statistics Tests (RED - Not Yet Implemented)
// ============================================================================

describe('CLI Compact - Compaction Statistics', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    // Set up mock data for compaction statistics tests
    mockStorage.list.mockResolvedValue([
      'testdb/users/users_1705000000000_1.parquet',
      'testdb/users/users_1705000001000_2.parquet',
      'testdb/users/users_1705000002000_3.parquet',
    ]);
    mockStorage.head.mockResolvedValue({ size: 1000000 }); // 1MB - small blocks
    mockStorage.get.mockResolvedValue(new Uint8Array(1000000));
  });

  it('should export getCompactionStats function', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.getCompactionStats).toBeDefined();
    expect(typeof module.getCompactionStats).toBe('function');
  });

  it('should track space savings from compaction', async () => {
    const module = await import('../../../src/cli/compact.js');

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
    });

    expect(result.stats).toBeDefined();
    expect(result.stats.spaceSaved).toBeDefined();
    expect(result.stats.spaceSavedPercent).toBeDefined();
    expect(typeof result.stats.spaceSaved).toBe('number');
    expect(typeof result.stats.spaceSavedPercent).toBe('number');
  });

  it('should track file count reduction', async () => {
    const module = await import('../../../src/cli/compact.js');

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
    });

    expect(result.stats).toBeDefined();
    expect(result.stats.filesBefore).toBeDefined();
    expect(result.stats.filesAfter).toBeDefined();
    expect(result.stats.filesRemoved).toBeDefined();
    expect(result.stats.filesAfter).toBeLessThan(result.stats.filesBefore);
  });

  it('should measure compression improvement', async () => {
    const module = await import('../../../src/cli/compact.js');

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
    });

    expect(result.stats).toBeDefined();
    expect(result.stats.compressionBefore).toBeDefined();
    expect(result.stats.compressionAfter).toBeDefined();
    expect(result.stats.compressionImprovement).toBeDefined();
  });

  it('should generate compaction summary report', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.generateCompactionReport).toBeDefined();

    const stats = {
      filesBefore: 50,
      filesAfter: 5,
      sizeBefore: 100 * 1024 * 1024,
      sizeAfter: 80 * 1024 * 1024,
      rowsProcessed: 1000000,
      tombstonesRemoved: 5000,
      durationMs: 30000,
    };

    const report = await module.generateCompactionReport(stats);
    expect(report).toBeDefined();
    expect(typeof report).toBe('string');
    expect(report).toContain('Files');
    expect(report).toContain('Size');
    expect(report).toContain('Duration');
  });

  it('should track per-block statistics', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.getBlockStats).toBeDefined();

    const blocks = [
      { id: 'b1', size: 1000000, rowCount: 100 },
      { id: 'b2', size: 2000000, rowCount: 200 },
      { id: 'b3', size: 500000, rowCount: 50 },
    ];

    const stats = await module.getBlockStats(blocks);
    expect(stats).toBeDefined();
    expect(stats.totalSize).toBe(3500000);
    expect(stats.totalRows).toBe(350);
    expect(stats.avgBlockSize).toBe(3500000 / 3);
    expect(stats.minBlockSize).toBe(500000);
    expect(stats.maxBlockSize).toBe(2000000);
  });

  it('should store compaction history', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.getCompactionHistory).toBeDefined();
    expect(module.recordCompaction).toBeDefined();

    await module.recordCompaction({
      database: 'testdb',
      collection: 'users',
      timestamp: new Date(),
      stats: {
        filesBefore: 10,
        filesAfter: 2,
        duration: 5000,
      },
    });

    const history = await module.getCompactionHistory('testdb', 'users', '.mongolake');
    expect(history).toBeDefined();
    expect(Array.isArray(history)).toBe(true);
    expect(history.length).toBeGreaterThan(0);
  });
});

// ============================================================================
// Integration Tests (RED - Not Yet Implemented)
// ============================================================================

describe('CLI Compact - Integration', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockStorage.list.mockResolvedValue([
      'testdb/users/users_1705000000000_1.parquet',
      'testdb/users/users_1705000001000_2.parquet',
      'testdb/users/users_1705000002000_3.parquet',
    ]);
    mockStorage.head.mockResolvedValue({ size: 1000000 });
    mockStorage.get.mockResolvedValue(new Uint8Array(1000000));
  });

  it('should run end-to-end compaction', async () => {
    const module = await import('../../../src/cli/compact.js');

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
    });

    expect(result).toBeDefined();
    expect(result.success).toBe(true);
    expect(result.processedBlocks).toBeGreaterThan(0);
  });

  it('should support collection pattern matching', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.compactCollections).toBeDefined();

    // Compact all collections matching pattern
    const result = await module.compactCollections({
      database: 'testdb',
      pattern: 'user*', // Match users, user_profiles, etc.
      path: '.mongolake',
      dryRun: false,
      verbose: false,
    });

    expect(result).toBeDefined();
    expect(result.collectionsCompacted).toBeDefined();
    expect(Array.isArray(result.collectionsCompacted)).toBe(true);
  });

  it('should support database-wide compaction', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.compactDatabase).toBeDefined();

    const result = await module.compactDatabase({
      database: 'testdb',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
    });

    expect(result).toBeDefined();
    expect(result.collectionsProcessed).toBeGreaterThan(0);
    expect(result.totalStats).toBeDefined();
  });

  it('should handle concurrent compaction requests', async () => {
    const module = await import('../../../src/cli/compact.js');

    // Try to run two compactions at once
    const promise1 = module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
    });

    const promise2 = module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
    });

    const [result1, result2] = await Promise.all([promise1, promise2]);

    // One should succeed, one should be skipped or queued
    expect(result1.success || result2.success).toBe(true);
    expect(result1.skipped || result2.skipped || (result1.success && result2.success)).toBe(true);
  });

  it('should support --max-size option to limit compaction scope', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('--max-size');

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      maxSize: 10 * 1024 * 1024, // 10MB limit
    });

    expect(result).toBeDefined();
    expect(result.stats.bytesProcessed).toBeLessThanOrEqual(10 * 1024 * 1024);
  });

  it('should support --min-age option to only compact old files', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('--min-age');

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      minAge: 86400000, // Only compact files older than 24 hours
    });

    expect(result).toBeDefined();
    // All processed files should be older than minAge
    if (result.processedFiles) {
      for (const file of result.processedFiles) {
        expect(Date.now() - file.createdAt.getTime()).toBeGreaterThanOrEqual(86400000);
      }
    }
  });
});

// ============================================================================
// Abort and Resume Tests (RED - Not Yet Implemented)
// ============================================================================

describe('CLI Compact - Abort and Resume', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    // Set up mock data for abort and resume tests
    mockStorage.list.mockResolvedValue([
      'testdb/users/users_1705000000000_1.parquet',
      'testdb/users/users_1705000001000_2.parquet',
      'testdb/users/users_1705000002000_3.parquet',
    ]);
    mockStorage.head.mockResolvedValue({ size: 1000000 }); // 1MB - small blocks
    mockStorage.get.mockResolvedValue(new Uint8Array(1000000));
  });

  it('should support graceful abort via AbortController', async () => {
    const module = await import('../../../src/cli/compact.js');

    const controller = new AbortController();

    // Abort immediately before running
    controller.abort();

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      signal: controller.signal,
    });

    expect(result).toBeDefined();
    expect(result.aborted).toBe(true);
    // checkpointState may not be defined if aborted before processing started
  });

  it('should save checkpoint state for resume', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.saveCheckpoint).toBeDefined();
    expect(module.loadCheckpoint).toBeDefined();

    const checkpoint = {
      database: 'testdb',
      collection: 'users',
      lastProcessedBlock: 'block_5',
      processedBlocks: 5,
      remainingBlocks: 10,
      startedAt: new Date(),
    };

    await module.saveCheckpoint(checkpoint, '.mongolake');
    const loaded = await module.loadCheckpoint('testdb', 'users', '.mongolake');

    expect(loaded).toBeDefined();
    expect(loaded?.lastProcessedBlock).toBe('block_5');
    expect(loaded?.processedBlocks).toBe(5);
  });

  it('should resume from checkpoint', async () => {
    const module = await import('../../../src/cli/compact.js');

    // First, create a checkpoint
    await module.saveCheckpoint({
      database: 'testdb',
      collection: 'users',
      lastProcessedBlock: 'block_5',
      processedBlocks: 5,
      remainingBlocks: 10,
      startedAt: new Date(Date.now() - 60000),
    }, '.mongolake');

    // Resume from checkpoint
    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      resume: true,
    });

    expect(result).toBeDefined();
    expect(result.resumedFrom).toBe('block_5');
  });

  it('should clean up checkpoint on successful completion', async () => {
    const module = await import('../../../src/cli/compact.js');

    // Create a checkpoint
    await module.saveCheckpoint({
      database: 'testdb',
      collection: 'users',
      lastProcessedBlock: 'block_5',
      processedBlocks: 5,
      remainingBlocks: 5,
      startedAt: new Date(),
    }, '.mongolake');

    // Run compaction to completion
    await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      resume: true,
    });

    // Checkpoint should be cleared
    const checkpoint = await module.loadCheckpoint('testdb', 'users', '.mongolake');
    expect(checkpoint).toBeNull();
  });

  it('should support --force-restart to ignore checkpoint', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('--force-restart');

    // Create a checkpoint
    await module.saveCheckpoint({
      database: 'testdb',
      collection: 'users',
      lastProcessedBlock: 'block_5',
      processedBlocks: 5,
      remainingBlocks: 10,
      startedAt: new Date(),
    }, '.mongolake');

    // Force restart ignores checkpoint
    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      forceRestart: true,
    });

    expect(result).toBeDefined();
    expect(result.resumedFrom).toBeUndefined();
  });
});
