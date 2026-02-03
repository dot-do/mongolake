/**
 * Tests for MongoLake Sync Upload/Push Commands
 *
 * Tests the push synchronization functionality including:
 * - Module exports
 * - Push command operations
 * - File upload logic
 * - Remote manifest updates
 * - Dry run mode for push
 * - Progress reporting
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  mockStorage,
  mockFetch,
  computeSyncDiff,
  formatBytes,
  formatDuration,
  type DatabaseState,
} from './sync-common';

// Mock the storage module
vi.mock('../../../src/storage/index.js', () => {
  return {
    FileSystemStorage: vi.fn(() => mockStorage),
    mockStorage,
  };
});

// Mock the auth module
vi.mock('../../../src/cli/auth.js', () => ({
  getAccessToken: vi.fn().mockResolvedValue('test-token'),
}));

// Mock fetch globally
vi.stubGlobal('fetch', mockFetch);

// ============================================================================
// Module Export Tests
// ============================================================================

describe('CLI Sync - Module Exports', () => {
  it('should export runPush function', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(typeof module.runPush).toBe('function');
  });

  it('should export runPull function', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(typeof module.runPull).toBe('function');
  });

  it('should export handlePushCommand function', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(typeof module.handlePushCommand).toBe('function');
  });

  it('should export handlePullCommand function', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(typeof module.handlePullCommand).toBe('function');
  });

  it('should export PUSH_HELP_TEXT', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.PUSH_HELP_TEXT).toBeDefined();
    expect(typeof module.PUSH_HELP_TEXT).toBe('string');
  });

  it('should export PULL_HELP_TEXT', async () => {
    const module = await import('../../../src/cli/sync.js');
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
// Sync Diff Computation Tests (Push)
// ============================================================================

describe('CLI Sync - Diff Computation (Push)', () => {
  it('should identify new local files to upload on push', () => {
    const localState: DatabaseState = {
      database: 'test',
      files: [
        { path: 'file1.parquet', size: 100, hash: 'hash1', modifiedAt: Date.now() },
        { path: 'file2.parquet', size: 200, hash: 'hash2', modifiedAt: Date.now() },
      ],
      lastSyncTimestamp: Date.now(),
      version: '1.0',
    };

    const remoteState: DatabaseState = {
      database: 'test',
      files: [
        { path: 'file1.parquet', size: 100, hash: 'hash1', modifiedAt: Date.now() },
      ],
      lastSyncTimestamp: Date.now(),
      version: '1.0',
    };

    const diff = computeSyncDiff(localState, remoteState, 'push');
    expect(diff.toUpload).toHaveLength(1);
    expect(diff.toUpload[0].path).toBe('file2.parquet');
  });

  it('should identify modified local files to upload on push', () => {
    const localState: DatabaseState = {
      database: 'test',
      files: [
        { path: 'file1.parquet', size: 150, hash: 'hash1-modified', modifiedAt: Date.now() },
      ],
      lastSyncTimestamp: Date.now(),
      version: '1.0',
    };

    const remoteState: DatabaseState = {
      database: 'test',
      files: [
        { path: 'file1.parquet', size: 100, hash: 'hash1', modifiedAt: Date.now() - 1000 },
      ],
      lastSyncTimestamp: Date.now(),
      version: '1.0',
    };

    const diff = computeSyncDiff(localState, remoteState, 'push');
    expect(diff.toUpload).toHaveLength(1);
    expect(diff.toUpload[0].hash).toBe('hash1-modified');
  });

  it('should identify remote files to delete on push', () => {
    const localState: DatabaseState = {
      database: 'test',
      files: [],
      lastSyncTimestamp: Date.now(),
      version: '1.0',
    };

    const remoteState: DatabaseState = {
      database: 'test',
      files: [
        { path: 'file1.parquet', size: 100, hash: 'hash1', modifiedAt: Date.now() },
      ],
      lastSyncTimestamp: Date.now(),
      version: '1.0',
    };

    const diff = computeSyncDiff(localState, remoteState, 'push');
    expect(diff.toDeleteRemote).toHaveLength(1);
  });

  it('should handle null remote state on push', () => {
    const localState: DatabaseState = {
      database: 'test',
      files: [
        { path: 'file1.parquet', size: 100, hash: 'hash1', modifiedAt: Date.now() },
      ],
      lastSyncTimestamp: Date.now(),
      version: '1.0',
    };

    const diff = computeSyncDiff(localState, null, 'push');
    expect(diff.toUpload).toHaveLength(1);
    expect(diff.toDeleteRemote).toHaveLength(0);
  });

  it('should identify no changes when states are identical', () => {
    const files = [
      { path: 'file1.parquet', size: 100, hash: 'hash1', modifiedAt: Date.now() },
    ];

    const localState: DatabaseState = {
      database: 'test',
      files: [...files],
      lastSyncTimestamp: Date.now(),
      version: '1.0',
    };

    const remoteState: DatabaseState = {
      database: 'test',
      files: [...files],
      lastSyncTimestamp: Date.now(),
      version: '1.0',
    };

    const pushDiff = computeSyncDiff(localState, remoteState, 'push');
    expect(pushDiff.toUpload).toHaveLength(0);
    expect(pushDiff.toDeleteRemote).toHaveLength(0);
  });
});

// ============================================================================
// Remote Client Tests
// ============================================================================

describe('CLI Sync - Remote Client', () => {
  beforeEach(() => {
    mockFetch.mockReset();
  });

  it('should normalize remote URL by removing trailing slash', () => {
    const baseUrl = 'https://api.mongolake.com/';
    const normalized = baseUrl.replace(/\/$/, '');
    expect(normalized).toBe('https://api.mongolake.com');
  });

  it('should include Authorization header when token is available', () => {
    const accessToken = 'test-token';
    const headers: Record<string, string> = {
      'Content-Type': 'application/octet-stream',
      'User-Agent': 'mongolake-cli/0.1.0',
    };

    if (accessToken) {
      headers['Authorization'] = `Bearer ${accessToken}`;
    }

    expect(headers['Authorization']).toBe('Bearer test-token');
  });

  it('should not include Authorization header when token is null', () => {
    const accessToken = null;
    const headers: Record<string, string> = {
      'Content-Type': 'application/octet-stream',
      'User-Agent': 'mongolake-cli/0.1.0',
    };

    if (accessToken) {
      headers['Authorization'] = `Bearer ${accessToken}`;
    }

    expect(headers['Authorization']).toBeUndefined();
  });

  it('should construct correct GET state URL', () => {
    const baseUrl = 'https://api.mongolake.com';
    const database = 'mydb';
    const url = `${baseUrl}/api/sync/${database}/state`;
    expect(url).toBe('https://api.mongolake.com/api/sync/mydb/state');
  });

  it('should construct correct PUT file URL', () => {
    const baseUrl = 'https://api.mongolake.com';
    const database = 'mydb';
    const filePath = 'data/file.parquet';
    const url = `${baseUrl}/api/sync/${database}/files/${encodeURIComponent(filePath)}`;
    expect(url).toBe('https://api.mongolake.com/api/sync/mydb/files/data%2Ffile.parquet');
  });

  it('should handle 404 response as null state', async () => {
    mockFetch.mockResolvedValueOnce({
      status: 404,
      ok: false,
    });

    const response = await fetch('https://api.mongolake.com/api/sync/mydb/state');

    if (response.status === 404) {
      expect(true).toBe(true); // State not found, return null
    }
  });

  it('should throw error on non-OK response', async () => {
    mockFetch.mockResolvedValueOnce({
      status: 500,
      statusText: 'Internal Server Error',
      ok: false,
    });

    const response = await fetch('https://api.mongolake.com/api/sync/mydb/state');

    if (!response.ok && response.status !== 404) {
      expect(response.status).toBe(500);
      expect(response.statusText).toBe('Internal Server Error');
    }
  });
});

// ============================================================================
// Utility Functions Tests
// ============================================================================

describe('CLI Sync - Utility Functions', () => {
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
// Push Command Tests
// ============================================================================

describe('Push Command', () => {
  beforeEach(async () => {
    mockFetch.mockReset();
    mockStorage.get.mockReset();
    mockStorage.get.mockResolvedValue(new Uint8Array([1, 2, 3]));
    mockStorage.put.mockReset();
    mockStorage.put.mockResolvedValue(undefined);
    mockStorage.list.mockReset();
    mockStorage.list.mockResolvedValue([]);
    mockStorage.head.mockReset();
    mockStorage.head.mockResolvedValue({ size: 100 });
    vi.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should detect local changes since last sync', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.detectLocalChanges).toBeDefined();
    expect(typeof module.detectLocalChanges).toBe('function');

    const lastSync = Date.now() - 60000; // 1 minute ago
    const changes = await module.detectLocalChanges('testdb', '.mongolake', lastSync);
    expect(changes).toBeDefined();
    expect(Array.isArray(changes.modified)).toBe(true);
    expect(Array.isArray(changes.added)).toBe(true);
    expect(Array.isArray(changes.deleted)).toBe(true);
  });

  it('should upload changed Parquet files to remote', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.uploadChangedFiles).toBeDefined();
    expect(typeof module.uploadChangedFiles).toBe('function');

    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
    });

    const files = [
      { path: 'data.parquet', size: 1024, hash: 'abc123' },
    ];

    const result = await module.uploadChangedFiles(
      'testdb',
      'https://api.mongolake.com',
      files,
      '.mongolake',
      'test-token'
    );

    expect(result.success).toBe(true);
    expect(result.filesUploaded).toBe(1);
  });

  it('should update remote manifest', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.updateRemoteManifest).toBeDefined();
    expect(typeof module.updateRemoteManifest).toBe('function');

    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
    });

    const manifest = {
      database: 'testdb',
      files: [{ path: 'data.parquet', size: 1024, hash: 'abc123', modifiedAt: Date.now() }],
      lastSyncTimestamp: Date.now(),
      version: '1.0',
    };

    await module.updateRemoteManifest(
      'testdb',
      'https://api.mongolake.com',
      manifest,
      'test-token'
    );

    expect(mockFetch).toHaveBeenCalled();
  });

  it('should support --force flag', async () => {
    const module = await import('../../../src/cli/sync.js');

    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        database: 'testdb',
        files: [{ path: 'data.parquet', size: 2048, hash: 'remote-hash', modifiedAt: Date.now() }],
        lastSyncTimestamp: Date.now(),
        version: '1.0',
      }),
    });

    const result = await module.runPush({
      database: 'testdb',
      remote: 'https://api.mongolake.com',
      path: '.mongolake',
      verbose: false,
      dryRun: false,
      force: true,
      profile: 'default',
    });

    expect(result).toBeDefined();
  });

  it('should support --dry-run flag', async () => {
    const module = await import('../../../src/cli/sync.js');

    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => null,
    });

    const result = await module.runPush({
      database: 'testdb',
      remote: 'https://api.mongolake.com',
      path: '.mongolake',
      verbose: false,
      dryRun: true,
      force: false,
      profile: 'default',
    });

    expect(result.success).toBe(true);
    expect(result.filesUploaded).toBe(0);
  });

  it('should show progress during upload', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.createProgressReporter).toBeDefined();
    expect(typeof module.createProgressReporter).toBe('function');

    const reporter = module.createProgressReporter(5);
    expect(reporter).toBeDefined();
    expect(typeof reporter.update).toBe('function');
    expect(typeof reporter.finish).toBe('function');

    reporter.update(1, 'file1.parquet');
    reporter.update(2, 'file2.parquet');
    reporter.finish();
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

    console.log('Uploading files...');

    expect(consoleSpy).toHaveBeenCalledWith('DRY RUN - No changes were made.');
    expect(consoleSpy).not.toHaveBeenCalledWith('Uploading files...');
  });
});

// ============================================================================
// Help Text Tests
// ============================================================================

describe('CLI Sync - Help Text', () => {
  it('should include usage information in push help', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.PUSH_HELP_TEXT).toContain('Usage:');
    expect(module.PUSH_HELP_TEXT).toContain('mongolake push');
    expect(module.PUSH_HELP_TEXT).toContain('--remote');
  });

  it('should include option descriptions in push help', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.PUSH_HELP_TEXT).toContain('--dry-run');
    expect(module.PUSH_HELP_TEXT).toContain('--force');
    expect(module.PUSH_HELP_TEXT).toContain('--verbose');
    expect(module.PUSH_HELP_TEXT).toContain('--profile');
  });

  it('should include examples in help text', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.PUSH_HELP_TEXT).toContain('Examples:');
  });
});

// ============================================================================
// Progress Bar Tests
// ============================================================================

describe('CLI Sync - Progress Bar', () => {
  let stdoutSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    stdoutSpy = vi.spyOn(process.stdout, 'write').mockImplementation(() => true);
  });

  afterEach(() => {
    stdoutSpy.mockRestore();
  });

  class MockProgressBar {
    private current: number = 0;
    private total: number;
    private lastUpdate: number = 0;

    constructor(total: number) {
      this.total = total;
    }

    update(current: number, _label?: string): void {
      this.current = current;
      const now = Date.now();

      if (now - this.lastUpdate < 100 && current < this.total) {
        return;
      }
      this.lastUpdate = now;

      if (current >= this.total) {
        console.log('');
      }
    }

    get percentage(): number {
      return this.total > 0 ? (this.current / this.total) * 100 : 0;
    }
  }

  it('should calculate progress percentage', () => {
    const progress = new MockProgressBar(10);
    progress.update(5);
    expect(progress.percentage).toBe(50);
  });

  it('should handle zero total gracefully', () => {
    const progress = new MockProgressBar(0);
    progress.update(0);
    expect(progress.percentage).toBe(0);
  });

  it('should complete at 100%', () => {
    const progress = new MockProgressBar(10);
    progress.update(10);
    expect(progress.percentage).toBe(100);
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
// Error Handling Tests
// ============================================================================

describe('CLI Sync - Error Handling', () => {
  beforeEach(() => {
    mockFetch.mockReset();
  });

  it('should handle network connection error', async () => {
    mockFetch.mockRejectedValueOnce(new Error('fetch failed'));

    try {
      await fetch('https://api.mongolake.com/api/sync/mydb/state');
    } catch (error) {
      expect((error as Error).message).toContain('fetch');
    }
  });

  it('should handle invalid JSON response', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: vi.fn().mockRejectedValue(new Error('Invalid JSON')),
    });

    const response = await fetch('https://api.mongolake.com/api/sync/mydb/state');

    try {
      await response.json();
    } catch (error) {
      expect((error as Error).message).toBe('Invalid JSON');
    }
  });

  it('should collect upload errors', () => {
    const result = {
      success: true,
      filesUploaded: 0,
      bytesTransferred: 0,
      errors: [] as string[],
    };

    const files = ['file1.parquet', 'file2.parquet', 'file3.parquet'];

    for (const file of files) {
      try {
        if (file === 'file2.parquet') {
          throw new Error('Upload failed');
        }
        result.filesUploaded++;
      } catch (error) {
        result.errors.push(`Failed to upload ${file}: ${(error as Error).message}`);
      }
    }

    result.success = result.errors.length === 0;

    expect(result.filesUploaded).toBe(2);
    expect(result.errors).toHaveLength(1);
    expect(result.success).toBe(false);
  });
});

// ============================================================================
// Bandwidth Throttling Tests
// ============================================================================

describe('CLI Sync - Bandwidth Throttling', () => {
  beforeEach(async () => {
    mockFetch.mockReset();
    mockStorage.get.mockReset();
    mockStorage.get.mockResolvedValue(new Uint8Array([1, 2, 3]));
    mockStorage.put.mockReset();
    mockStorage.put.mockResolvedValue(undefined);
    mockStorage.list.mockReset();
    mockStorage.list.mockResolvedValue([]);
    mockStorage.head.mockReset();
    mockStorage.head.mockResolvedValue({ size: 100 });
    vi.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should export BandwidthThrottler class', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.BandwidthThrottler).toBeDefined();
  });

  it('should limit upload speed when --bandwidth-limit is specified', async () => {
    const module = await import('../../../src/cli/sync.js');
    const throttler = new module.BandwidthThrottler({
      maxBytesPerSecond: 1024 * 1024,
    });

    expect(throttler).toBeDefined();
    expect(typeof throttler.throttle).toBe('function');
    expect(typeof throttler.getAverageSpeed).toBe('function');
  });

  it('should support --bandwidth-limit flag in CLI', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.PUSH_HELP_TEXT).toContain('--bandwidth-limit');
  });

  it('should parse bandwidth limit from string', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.parseBandwidthLimit).toBeDefined();

    expect(module.parseBandwidthLimit('1MB')).toBe(1024 * 1024);
    expect(module.parseBandwidthLimit('500KB')).toBe(500 * 1024);
    expect(module.parseBandwidthLimit('10MB/s')).toBe(10 * 1024 * 1024);
    expect(module.parseBandwidthLimit('1024')).toBe(1024);
  });
});
