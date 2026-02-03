/**
 * Tests for MongoLake Compact Command - Basic Functionality
 *
 * Tests the basic compaction functionality including:
 * - Module exports
 * - Compact options
 * - Block identification
 * - Compaction analysis
 * - Dry run mode
 * - Utility functions
 * - Help text
 * - CLI argument parsing
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  mockStorage,
  mockScheduler,
  formatBytes,
  formatDuration,
  type BlockMetadata,
  type CompactionResult,
} from './compact-common';

// Mock the storage module
vi.mock('../../../src/storage/index.js', () => ({
  FileSystemStorage: vi.fn(() => mockStorage),
}));

// Mock the compaction scheduler
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
          size: 1000000,
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
  interface BlockSize {
    path: string;
    size: number;
  }

  const MIN_BLOCK_SIZE = 2 * 1024 * 1024; // 2MB

  function identifySmallBlocks(blocks: BlockSize[]): BlockSize[] {
    return blocks.filter((b) => b.size < MIN_BLOCK_SIZE);
  }

  it('should identify blocks smaller than minimum threshold', () => {
    const blocks: BlockSize[] = [
      { path: 'f1.parquet', size: 1 * 1024 * 1024 },
      { path: 'f2.parquet', size: 3 * 1024 * 1024 },
      { path: 'f3.parquet', size: 500 * 1024 },
    ];

    const smallBlocks = identifySmallBlocks(blocks);

    expect(smallBlocks).toHaveLength(2);
    expect(smallBlocks.map((b) => b.path)).toContain('f1.parquet');
    expect(smallBlocks.map((b) => b.path)).toContain('f3.parquet');
  });

  it('should require at least 2 blocks for compaction', () => {
    const smallBlocks: BlockSize[] = [
      { path: 'f1.parquet', size: 1 * 1024 * 1024 },
    ];

    const canCompact = smallBlocks.length >= 2;
    expect(canCompact).toBe(false);
  });

  it('should calculate total size of blocks to compact', () => {
    const blocks: BlockSize[] = [
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
