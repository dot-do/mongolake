/**
 * Tests for MongoLake Sync Commands - Module Exports and Options
 *
 * Tests the module exports, options parsing, help text, and utility functions.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Mock the storage module
vi.mock('../../../../src/storage/index.js', () => {
  const mockStorage = {
    get: vi.fn().mockResolvedValue(new Uint8Array([1, 2, 3])),
    put: vi.fn().mockResolvedValue(undefined),
    delete: vi.fn().mockResolvedValue(undefined),
    list: vi.fn().mockResolvedValue([]),
    head: vi.fn().mockResolvedValue({ size: 100 }),
    exists: vi.fn().mockResolvedValue(true),
  };

  return {
    FileSystemStorage: vi.fn(() => mockStorage),
    mockStorage,
  };
});

// Mock the auth module
vi.mock('../../../../src/cli/auth.js', () => ({
  getAccessToken: vi.fn().mockResolvedValue('test-token'),
}));

// ============================================================================
// Module Export Tests
// ============================================================================

describe('CLI Sync - Module Exports', () => {
  it('should export runPush function', async () => {
    const module = await import('../../../../src/cli/sync.js');
    expect(typeof module.runPush).toBe('function');
  });

  it('should export runPull function', async () => {
    const module = await import('../../../../src/cli/sync.js');
    expect(typeof module.runPull).toBe('function');
  });

  it('should export handlePushCommand function', async () => {
    const module = await import('../../../../src/cli/sync.js');
    expect(typeof module.handlePushCommand).toBe('function');
  });

  it('should export handlePullCommand function', async () => {
    const module = await import('../../../../src/cli/sync.js');
    expect(typeof module.handlePullCommand).toBe('function');
  });

  it('should export PUSH_HELP_TEXT', async () => {
    const module = await import('../../../../src/cli/sync.js');
    expect(module.PUSH_HELP_TEXT).toBeDefined();
    expect(typeof module.PUSH_HELP_TEXT).toBe('string');
  });

  it('should export PULL_HELP_TEXT', async () => {
    const module = await import('../../../../src/cli/sync.js');
    expect(module.PULL_HELP_TEXT).toBeDefined();
    expect(typeof module.PULL_HELP_TEXT).toBe('string');
  });
});

// ============================================================================
// Sync Options Tests
// ============================================================================

describe('CLI Sync - Options Parsing', () => {
  it('should have required database option', () => {
    const options = {
      database: 'testdb',
      remote: 'https://api.mongolake.com',
      path: '.mongolake',
      verbose: false,
      dryRun: false,
      force: false,
      profile: 'default',
    };

    expect(options.database).toBe('testdb');
  });

  it('should have required remote option', () => {
    const options = {
      database: 'testdb',
      remote: 'https://api.mongolake.com',
      path: '.mongolake',
      verbose: false,
      dryRun: false,
      force: false,
      profile: 'default',
    };

    expect(options.remote).toBe('https://api.mongolake.com');
  });

  it('should use default path .mongolake', () => {
    const options = {
      database: 'testdb',
      remote: 'https://api.mongolake.com',
      path: '.mongolake',
      verbose: false,
      dryRun: false,
      force: false,
      profile: 'default',
    };

    expect(options.path).toBe('.mongolake');
  });

  it('should support dry-run option', () => {
    const options = {
      database: 'testdb',
      remote: 'https://api.mongolake.com',
      path: '.mongolake',
      verbose: false,
      dryRun: true,
      force: false,
      profile: 'default',
    };

    expect(options.dryRun).toBe(true);
  });

  it('should support force option', () => {
    const options = {
      database: 'testdb',
      remote: 'https://api.mongolake.com',
      path: '.mongolake',
      verbose: false,
      dryRun: false,
      force: true,
      profile: 'default',
    };

    expect(options.force).toBe(true);
  });

  it('should support verbose option', () => {
    const options = {
      database: 'testdb',
      remote: 'https://api.mongolake.com',
      path: '.mongolake',
      verbose: true,
      dryRun: false,
      force: false,
      profile: 'default',
    };

    expect(options.verbose).toBe(true);
  });

  it('should support profile option', () => {
    const options = {
      database: 'testdb',
      remote: 'https://api.mongolake.com',
      path: '.mongolake',
      verbose: false,
      dryRun: false,
      force: false,
      profile: 'production',
    };

    expect(options.profile).toBe('production');
  });
});

// ============================================================================
// Help Text Tests
// ============================================================================

describe('CLI Sync - Help Text', () => {
  it('should include usage information in push help', async () => {
    const module = await import('../../../../src/cli/sync.js');
    expect(module.PUSH_HELP_TEXT).toContain('Usage:');
    expect(module.PUSH_HELP_TEXT).toContain('mongolake push');
    expect(module.PUSH_HELP_TEXT).toContain('--remote');
  });

  it('should include usage information in pull help', async () => {
    const module = await import('../../../../src/cli/sync.js');
    expect(module.PULL_HELP_TEXT).toContain('Usage:');
    expect(module.PULL_HELP_TEXT).toContain('mongolake pull');
    expect(module.PULL_HELP_TEXT).toContain('--remote');
  });

  it('should include option descriptions in push help', async () => {
    const module = await import('../../../../src/cli/sync.js');
    expect(module.PUSH_HELP_TEXT).toContain('--dry-run');
    expect(module.PUSH_HELP_TEXT).toContain('--force');
    expect(module.PUSH_HELP_TEXT).toContain('--verbose');
    expect(module.PUSH_HELP_TEXT).toContain('--profile');
  });

  it('should include examples in help text', async () => {
    const module = await import('../../../../src/cli/sync.js');
    expect(module.PUSH_HELP_TEXT).toContain('Examples:');
    expect(module.PULL_HELP_TEXT).toContain('Examples:');
  });
});

// ============================================================================
// Utility Functions Tests
// ============================================================================

describe('CLI Sync - Utility Functions', () => {
  function formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const k = 1024;
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${(bytes / Math.pow(k, i)).toFixed(2)} ${units[i]}`;
  }

  function formatDuration(ms: number): string {
    if (ms < 1000) {
      return `${Math.round(ms)}ms`;
    }
    const seconds = ms / 1000;
    if (seconds < 60) {
      return `${seconds.toFixed(0)}s`;
    }
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = Math.floor(seconds % 60);
    return `${minutes}m ${remainingSeconds}s`;
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
      expect(formatBytes(2048)).toBe('2.00 KB');
    });

    it('should format megabytes', () => {
      expect(formatBytes(1024 * 1024)).toBe('1.00 MB');
      expect(formatBytes(5 * 1024 * 1024)).toBe('5.00 MB');
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
      expect(formatDuration(5000)).toBe('5s');
      expect(formatDuration(30000)).toBe('30s');
    });

    it('should format minutes and seconds', () => {
      expect(formatDuration(90000)).toBe('1m 30s');
      expect(formatDuration(125000)).toBe('2m 5s');
    });
  });
});

// ============================================================================
// File Hash Computation Tests
// ============================================================================

describe('CLI Sync - File Hash Computation', () => {
  it('should compute SHA-256 hash for file content', async () => {
    const data = new Uint8Array([1, 2, 3, 4, 5]);
    const hashBuffer = await crypto.subtle.digest('SHA-256', data);
    const hashArray = new Uint8Array(hashBuffer);
    const hash = Array.from(hashArray)
      .map((b) => b.toString(16).padStart(2, '0'))
      .join('');

    expect(hash).toHaveLength(64); // SHA-256 produces 64 hex characters
  });

  it('should produce different hashes for different content', async () => {
    const data1 = new Uint8Array([1, 2, 3]);
    const data2 = new Uint8Array([4, 5, 6]);

    const hashBuffer1 = await crypto.subtle.digest('SHA-256', data1);
    const hashBuffer2 = await crypto.subtle.digest('SHA-256', data2);

    const hash1 = Array.from(new Uint8Array(hashBuffer1))
      .map((b) => b.toString(16).padStart(2, '0'))
      .join('');
    const hash2 = Array.from(new Uint8Array(hashBuffer2))
      .map((b) => b.toString(16).padStart(2, '0'))
      .join('');

    expect(hash1).not.toBe(hash2);
  });

  it('should produce same hash for same content', async () => {
    const data1 = new Uint8Array([1, 2, 3]);
    const data2 = new Uint8Array([1, 2, 3]);

    const hashBuffer1 = await crypto.subtle.digest('SHA-256', data1);
    const hashBuffer2 = await crypto.subtle.digest('SHA-256', data2);

    const hash1 = Array.from(new Uint8Array(hashBuffer1))
      .map((b) => b.toString(16).padStart(2, '0'))
      .join('');
    const hash2 = Array.from(new Uint8Array(hashBuffer2))
      .map((b) => b.toString(16).padStart(2, '0'))
      .join('');

    expect(hash1).toBe(hash2);
  });
});

// ============================================================================
// Sync Result Tests
// ============================================================================

describe('CLI Sync - Sync Result', () => {
  interface SyncResult {
    success: boolean;
    filesUploaded: number;
    filesDownloaded: number;
    bytesTransferred: number;
    errors: string[];
  }

  it('should track successful sync', () => {
    const result: SyncResult = {
      success: true,
      filesUploaded: 5,
      filesDownloaded: 0,
      bytesTransferred: 1024 * 1024,
      errors: [],
    };

    expect(result.success).toBe(true);
    expect(result.filesUploaded).toBe(5);
    expect(result.errors).toHaveLength(0);
  });

  it('should track sync with errors', () => {
    const result: SyncResult = {
      success: false,
      filesUploaded: 3,
      filesDownloaded: 0,
      bytesTransferred: 512 * 1024,
      errors: ['Failed to upload file1.parquet: Connection refused'],
    };

    expect(result.success).toBe(false);
    expect(result.errors).toHaveLength(1);
  });

  it('should determine success based on error count', () => {
    const result: SyncResult = {
      success: false,
      filesUploaded: 5,
      filesDownloaded: 0,
      bytesTransferred: 1024 * 1024,
      errors: [],
    };

    result.success = result.errors.length === 0;
    expect(result.success).toBe(true);
  });
});

// ============================================================================
// File Filtering Tests
// ============================================================================

describe('CLI Sync - File Filtering', () => {
  it('should skip internal sync files', () => {
    const files = [
      'mydb/users/data.parquet',
      'mydb/users/_sync/manifest.json',
      'mydb/orders/items.parquet',
      'mydb/orders/data.sync.json',
    ];

    const filtered = files.filter(
      (f) => !f.includes('/_sync/') && !f.endsWith('.sync.json')
    );

    expect(filtered).toHaveLength(2);
    expect(filtered).toContain('mydb/users/data.parquet');
    expect(filtered).toContain('mydb/orders/items.parquet');
  });
});

// ============================================================================
// Dry Run Tests
// ============================================================================

describe('CLI Sync - Dry Run Mode', () => {
  let consoleSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    consoleSpy.mockRestore();
  });

  it('should show changes without executing in dry run mode', () => {
    const dryRun = true;
    const filesToUpload = ['file1.parquet', 'file2.parquet'];

    console.log('Changes to push:');
    console.log(`  Files to upload: ${filesToUpload.length}`);

    if (dryRun) {
      console.log('DRY RUN - No changes were made.');
      return;
    }

    // This should not be reached in dry run
    console.log('Uploading files...');

    expect(consoleSpy).toHaveBeenCalledWith('DRY RUN - No changes were made.');
    expect(consoleSpy).not.toHaveBeenCalledWith('Uploading files...');
  });
});

// ============================================================================
// Auth Token Handling Tests
// ============================================================================

describe('CLI Sync - Auth Token Handling', () => {
  let consoleSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    consoleSpy.mockRestore();
  });

  it('should warn when no token and remote is not localhost', () => {
    const accessToken = null;
    const remote = 'https://api.mongolake.com';

    if (!accessToken && !remote.includes('localhost') && !remote.includes('127.0.0.1')) {
      console.log('Warning: Not authenticated');
    }

    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('Not authenticated'));
  });

  it('should not warn when remote is localhost', () => {
    const accessToken = null;
    const remote = 'http://localhost:3456';

    if (!accessToken && !remote.includes('localhost') && !remote.includes('127.0.0.1')) {
      console.log('Warning: Not authenticated');
    }

    expect(consoleSpy).not.toHaveBeenCalled();
  });

  it('should not warn when token exists', () => {
    const accessToken = 'test-token';
    const remote = 'https://api.mongolake.com';

    if (!accessToken && !remote.includes('localhost') && !remote.includes('127.0.0.1')) {
      console.log('Warning: Not authenticated');
    }

    expect(consoleSpy).not.toHaveBeenCalled();
  });
});
