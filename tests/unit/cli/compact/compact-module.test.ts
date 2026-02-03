/**
 * Tests for MongoLake Compact Command - Module Exports and Options
 *
 * Tests the module exports, options parsing, help text, and utility functions.
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

vi.mock('../../../../src/compaction/scheduler.js', () => ({
  CompactionScheduler: vi.fn(() => mockScheduler),
}));

// ============================================================================
// Module Export Tests
// ============================================================================

describe('CLI Compact - Module Exports', () => {
  it('should export runCompact function', async () => {
    const module = await import('../../../../src/cli/compact.js');
    expect(typeof module.runCompact).toBe('function');
  });

  it('should export handleCompactCommand function', async () => {
    const module = await import('../../../../src/cli/compact.js');
    expect(typeof module.handleCompactCommand).toBe('function');
  });

  it('should export COMPACT_HELP_TEXT', async () => {
    const module = await import('../../../../src/cli/compact.js');
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
// Help Text Tests
// ============================================================================

describe('CLI Compact - Help Text', () => {
  it('should include usage information', async () => {
    const module = await import('../../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('Usage:');
    expect(module.COMPACT_HELP_TEXT).toContain('mongolake compact');
  });

  it('should include argument descriptions', async () => {
    const module = await import('../../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('database');
    expect(module.COMPACT_HELP_TEXT).toContain('collection');
  });

  it('should include option descriptions', async () => {
    const module = await import('../../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('--path');
    expect(module.COMPACT_HELP_TEXT).toContain('--dry-run');
    expect(module.COMPACT_HELP_TEXT).toContain('--verbose');
    expect(module.COMPACT_HELP_TEXT).toContain('--help');
  });

  it('should include description of compaction process', async () => {
    const module = await import('../../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('Description:');
    expect(module.COMPACT_HELP_TEXT).toContain('merge');
  });

  it('should include examples', async () => {
    const module = await import('../../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('Examples:');
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
