/**
 * Tests for MongoLake Sync Download/Pull Commands
 *
 * Tests the pull synchronization functionality including:
 * - Pull command operations
 * - File download logic
 * - Local manifest updates
 * - Dry run mode for pull
 * - Selective sync
 * - Progress display
 * - Resume capability
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  mockStorage,
  mockFetch,
  computeSyncDiff,
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
// Sync Diff Computation Tests (Pull)
// ============================================================================

describe('CLI Sync - Diff Computation (Pull)', () => {
  it('should identify new remote files to download on pull', () => {
    const localState: DatabaseState = {
      database: 'test',
      files: [
        { path: 'file1.parquet', size: 100, hash: 'hash1', modifiedAt: Date.now() },
      ],
      lastSyncTimestamp: Date.now(),
      version: '1.0',
    };

    const remoteState: DatabaseState = {
      database: 'test',
      files: [
        { path: 'file1.parquet', size: 100, hash: 'hash1', modifiedAt: Date.now() },
        { path: 'file2.parquet', size: 200, hash: 'hash2', modifiedAt: Date.now() },
      ],
      lastSyncTimestamp: Date.now(),
      version: '1.0',
    };

    const diff = computeSyncDiff(localState, remoteState, 'pull');
    expect(diff.toDownload).toHaveLength(1);
    expect(diff.toDownload[0].path).toBe('file2.parquet');
  });

  it('should identify local files to delete on pull', () => {
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

    const diff = computeSyncDiff(localState, remoteState, 'pull');
    expect(diff.toDeleteLocal).toHaveLength(1);
    expect(diff.toDeleteLocal[0].path).toBe('file2.parquet');
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

    const pullDiff = computeSyncDiff(localState, remoteState, 'pull');
    expect(pullDiff.toDownload).toHaveLength(0);
    expect(pullDiff.toDeleteLocal).toHaveLength(0);
  });
});

// ============================================================================
// Pull Command Tests
// ============================================================================

describe('Pull Command', () => {
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

  it('should detect remote changes since last sync', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.detectRemoteChanges).toBeDefined();
    expect(typeof module.detectRemoteChanges).toBe('function');

    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        database: 'testdb',
        files: [
          { path: 'data.parquet', size: 1024, hash: 'abc123', modifiedAt: Date.now() },
          { path: 'new-file.parquet', size: 2048, hash: 'def456', modifiedAt: Date.now() },
        ],
        lastSyncTimestamp: Date.now(),
        version: '1.0',
      }),
    });

    const lastSync = Date.now() - 60000;
    const changes = await module.detectRemoteChanges(
      'testdb',
      'https://api.mongolake.com',
      lastSync,
      'test-token'
    );

    expect(changes).toBeDefined();
    expect(Array.isArray(changes.modified)).toBe(true);
    expect(Array.isArray(changes.added)).toBe(true);
    expect(Array.isArray(changes.deleted)).toBe(true);
  });

  it('should download changed Parquet files', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.downloadChangedFiles).toBeDefined();
    expect(typeof module.downloadChangedFiles).toBe('function');

    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
      arrayBuffer: async () => new ArrayBuffer(1024),
    });

    const files = [
      { path: 'data.parquet', size: 1024, hash: 'abc123', modifiedAt: Date.now() },
    ];

    const result = await module.downloadChangedFiles(
      'testdb',
      'https://api.mongolake.com',
      files,
      '.mongolake',
      'test-token'
    );

    expect(result.success).toBe(true);
    expect(result.filesDownloaded).toBe(1);
  });

  it('should update local manifest', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.updateLocalManifest).toBeDefined();
    expect(typeof module.updateLocalManifest).toBe('function');

    const manifest = {
      database: 'testdb',
      files: [{ path: 'data.parquet', size: 1024, hash: 'abc123', modifiedAt: Date.now() }],
      lastSyncTimestamp: Date.now(),
      version: '1.0',
    };

    await module.updateLocalManifest('testdb', '.mongolake', manifest);
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
      arrayBuffer: async () => new ArrayBuffer(2048),
    });

    const result = await module.runPull({
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
      json: async () => ({
        database: 'testdb',
        files: [{ path: 'data.parquet', size: 1024, hash: 'abc123', modifiedAt: Date.now() }],
        lastSyncTimestamp: Date.now(),
        version: '1.0',
      }),
    });

    const result = await module.runPull({
      database: 'testdb',
      remote: 'https://api.mongolake.com',
      path: '.mongolake',
      verbose: false,
      dryRun: true,
      force: false,
      profile: 'default',
    });

    expect(result.success).toBe(true);
    expect(result.filesDownloaded).toBe(0);
  });
});

// ============================================================================
// Help Text Tests (Pull)
// ============================================================================

describe('CLI Sync - Help Text (Pull)', () => {
  it('should include usage information in pull help', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.PULL_HELP_TEXT).toContain('Usage:');
    expect(module.PULL_HELP_TEXT).toContain('mongolake pull');
    expect(module.PULL_HELP_TEXT).toContain('--remote');
  });

  it('should include examples in pull help text', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.PULL_HELP_TEXT).toContain('Examples:');
  });

  it('should support --bandwidth-limit flag in CLI', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.PULL_HELP_TEXT).toContain('--bandwidth-limit');
  });
});

// ============================================================================
// Sync Manifest Tests
// ============================================================================

describe('CLI Sync - Manifest Management', () => {
  it('should export SyncManifest type', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.createSyncManifest).toBeDefined();
    expect(typeof module.createSyncManifest).toBe('function');
  });

  it('should create manifest with proper structure', async () => {
    const module = await import('../../../src/cli/sync.js');
    const manifest = module.createSyncManifest('testdb', [
      { path: 'data.parquet', size: 1024, hash: 'abc123', modifiedAt: Date.now() },
    ]);

    expect(manifest.version).toBe('1.0');
    expect(manifest.database).toBe('testdb');
    expect(manifest.files).toHaveLength(1);
    expect(manifest.lastSyncTimestamp).toBeDefined();
    expect(typeof manifest.lastSyncTimestamp).toBe('number');
  });

  it('should load manifest from local storage', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.loadLocalManifest).toBeDefined();
    expect(typeof module.loadLocalManifest).toBe('function');

    const manifest = await module.loadLocalManifest('testdb', '.mongolake');
    expect(manifest === null || typeof manifest === 'object').toBe(true);
  });

  it('should save manifest to local storage', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.saveLocalManifest).toBeDefined();
    expect(typeof module.saveLocalManifest).toBe('function');

    const manifest = {
      version: '1.0' as const,
      database: 'testdb',
      files: [{ path: 'data.parquet', size: 1024, hash: 'abc123', modifiedAt: Date.now() }],
      lastSyncTimestamp: Date.now(),
    };

    await module.saveLocalManifest('testdb', '.mongolake', manifest);
  });
});

// ============================================================================
// Resume Capability Tests
// ============================================================================

describe('CLI Sync - Resume Capability', () => {
  beforeEach(() => {
    mockFetch.mockReset();
    vi.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should track sync progress for resume', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.SyncProgressTracker).toBeDefined();

    const tracker = new module.SyncProgressTracker('testdb', '.mongolake');
    expect(tracker).toBeDefined();
    expect(typeof tracker.save).toBe('function');
    expect(typeof tracker.load).toBe('function');
    expect(typeof tracker.clear).toBe('function');
  });

  it('should save incomplete sync state', async () => {
    const module = await import('../../../src/cli/sync.js');
    const tracker = new module.SyncProgressTracker('testdb', '.mongolake');

    await tracker.save({
      operation: 'push',
      completedFiles: ['file1.parquet', 'file2.parquet'],
      pendingFiles: ['file3.parquet', 'file4.parquet'],
      startedAt: Date.now(),
    });

    const state = await tracker.load();
    expect(state).toBeDefined();
    expect(state?.operation).toBe('push');
    expect(state?.completedFiles).toHaveLength(2);
    expect(state?.pendingFiles).toHaveLength(2);
  });

  it('should resume from last checkpoint', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.resumeSync).toBeDefined();
    expect(typeof module.resumeSync).toBe('function');

    const savedState = {
      operation: 'push' as const,
      completedFiles: ['file1.parquet'],
      pendingFiles: ['file2.parquet', 'file3.parquet'],
      startedAt: Date.now() - 60000,
    };

    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
    });

    const result = await module.resumeSync('testdb', '.mongolake', 'https://api.mongolake.com', savedState, 'test-token');
    expect(result).toBeDefined();
    expect(result.filesProcessed).toBeLessThanOrEqual(2);
  });

  it('should clear progress on successful completion', async () => {
    const module = await import('../../../src/cli/sync.js');
    const tracker = new module.SyncProgressTracker('testdb', '.mongolake');

    await tracker.save({
      operation: 'push',
      completedFiles: ['file1.parquet'],
      pendingFiles: [],
      startedAt: Date.now(),
    });

    await tracker.clear();
    const state = await tracker.load();
    expect(state).toBeNull();
  });
});

// ============================================================================
// Selective Sync (Specific Collections) Tests
// ============================================================================

describe('CLI Sync - Selective Sync', () => {
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

  it('should export SelectiveSyncOptions type and function', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.runSelectiveSync).toBeDefined();
    expect(typeof module.runSelectiveSync).toBe('function');
  });

  it('should support --collections flag to specify which collections to sync', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.parseCollectionsFilter).toBeDefined();
    expect(typeof module.parseCollectionsFilter).toBe('function');

    const filter = module.parseCollectionsFilter('users,orders,products');
    expect(filter).toEqual(['users', 'orders', 'products']);
  });

  it('should push only specified collections', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.runPushWithCollections).toBeDefined();

    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => null,
    });

    const result = await module.runPushWithCollections({
      database: 'testdb',
      remote: 'https://api.mongolake.com',
      path: '.mongolake',
      verbose: false,
      dryRun: false,
      force: false,
      profile: 'default',
      collections: ['users', 'orders'],
    });

    expect(result).toBeDefined();
    expect(result.collectionsProcessed).toEqual(['users', 'orders']);
  });

  it('should pull only specified collections', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.runPullWithCollections).toBeDefined();

    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        database: 'testdb',
        files: [
          { path: 'testdb/users/data.parquet', size: 1024, hash: 'abc', modifiedAt: Date.now() },
          { path: 'testdb/orders/data.parquet', size: 2048, hash: 'def', modifiedAt: Date.now() },
          { path: 'testdb/products/data.parquet', size: 4096, hash: 'ghi', modifiedAt: Date.now() },
        ],
        lastSyncTimestamp: Date.now(),
        version: '1.0',
      }),
      arrayBuffer: async () => new ArrayBuffer(1024),
    });

    const result = await module.runPullWithCollections({
      database: 'testdb',
      remote: 'https://api.mongolake.com',
      path: '.mongolake',
      verbose: false,
      dryRun: false,
      force: false,
      profile: 'default',
      collections: ['users', 'orders'],
    });

    expect(result).toBeDefined();
    expect(result.collectionsProcessed).toEqual(['users', 'orders']);
    expect(result.collectionsProcessed).not.toContain('products');
  });

  it('should filter files by collection prefix', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.filterFilesByCollections).toBeDefined();
    expect(typeof module.filterFilesByCollections).toBe('function');

    const files = [
      { path: 'testdb/users/data.parquet', size: 1024, hash: 'abc', modifiedAt: Date.now() },
      { path: 'testdb/orders/data.parquet', size: 2048, hash: 'def', modifiedAt: Date.now() },
      { path: 'testdb/products/data.parquet', size: 4096, hash: 'ghi', modifiedAt: Date.now() },
      { path: 'testdb/inventory/data.parquet', size: 8192, hash: 'jkl', modifiedAt: Date.now() },
    ];

    const filtered = module.filterFilesByCollections(files, 'testdb', ['users', 'orders']);
    expect(filtered).toHaveLength(2);
    expect(filtered.map((f: { path: string }) => f.path)).toContain('testdb/users/data.parquet');
    expect(filtered.map((f: { path: string }) => f.path)).toContain('testdb/orders/data.parquet');
    expect(filtered.map((f: { path: string }) => f.path)).not.toContain('testdb/products/data.parquet');
  });

  it('should support glob patterns for collection filtering', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.matchCollectionPattern).toBeDefined();
    expect(typeof module.matchCollectionPattern).toBe('function');

    expect(module.matchCollectionPattern('users', 'user*')).toBe(true);
    expect(module.matchCollectionPattern('user_events', 'user*')).toBe(true);
    expect(module.matchCollectionPattern('orders', 'user*')).toBe(false);

    expect(module.matchCollectionPattern('audit_logs', '*_logs')).toBe(true);
    expect(module.matchCollectionPattern('system_logs', '*_logs')).toBe(true);
    expect(module.matchCollectionPattern('users', '*_logs')).toBe(false);
  });

  it('should validate collection names before sync', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.validateCollectionNames).toBeDefined();
    expect(typeof module.validateCollectionNames).toBe('function');

    expect(module.validateCollectionNames(['users', 'orders', 'products'])).toEqual({
      valid: true,
      errors: [],
    });

    const result = module.validateCollectionNames(['', 'users', '$system', 'my collection']);
    expect(result.valid).toBe(false);
    expect(result.errors.length).toBeGreaterThan(0);
  });

  it('should list available collections from remote', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.listRemoteCollections).toBeDefined();
    expect(typeof module.listRemoteCollections).toBe('function');

    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        collections: ['users', 'orders', 'products', 'inventory'],
      }),
    });

    const collections = await module.listRemoteCollections(
      'testdb',
      'https://api.mongolake.com',
      'test-token'
    );

    expect(collections).toEqual(['users', 'orders', 'products', 'inventory']);
  });

  it('should list available collections from local', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.listLocalCollections).toBeDefined();
    expect(typeof module.listLocalCollections).toBe('function');

    const collections = await module.listLocalCollections('testdb', '.mongolake');
    expect(Array.isArray(collections)).toBe(true);
  });

  it('should show summary of collections to be synced', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.generateSyncSummary).toBeDefined();
    expect(typeof module.generateSyncSummary).toBe('function');

    const summary = module.generateSyncSummary({
      database: 'testdb',
      collections: ['users', 'orders'],
      direction: 'push',
      filesCount: 5,
      totalSize: 1024 * 1024,
    });

    expect(summary).toContain('testdb');
    expect(summary).toContain('users');
    expect(summary).toContain('orders');
    expect(summary).toContain('push');
  });

  it('should support --exclude-collections flag', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.parseExcludeCollections).toBeDefined();
    expect(typeof module.parseExcludeCollections).toBe('function');

    const excluded = module.parseExcludeCollections('logs,metrics,_internal');
    expect(excluded).toEqual(['logs', 'metrics', '_internal']);
  });

  it('should exclude specified collections from sync', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.applyCollectionFilters).toBeDefined();
    expect(typeof module.applyCollectionFilters).toBe('function');

    const allCollections = ['users', 'orders', 'logs', 'metrics', 'products'];
    const filtered = module.applyCollectionFilters(allCollections, {
      include: null,
      exclude: ['logs', 'metrics'],
    });

    expect(filtered).toEqual(['users', 'orders', 'products']);
  });

  it('should combine include and exclude filters correctly', async () => {
    const module = await import('../../../src/cli/sync.js');

    const allCollections = ['users', 'user_events', 'user_logs', 'orders', 'products'];
    const filtered = module.applyCollectionFilters(allCollections, {
      include: ['user*', 'orders'],
      exclude: ['*_logs'],
    });

    expect(filtered).toContain('users');
    expect(filtered).toContain('user_events');
    expect(filtered).toContain('orders');
    expect(filtered).not.toContain('user_logs');
    expect(filtered).not.toContain('products');
  });
});

// ============================================================================
// Sync Progress Display Tests
// ============================================================================

describe('CLI Sync - Progress Display', () => {
  let stdoutSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    stdoutSpy = vi.spyOn(process.stdout, 'write').mockImplementation(() => true);
    vi.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should export progress reporter with detailed stats', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.SyncProgressReporter).toBeDefined();

    const reporter = new module.SyncProgressReporter({
      totalFiles: 10,
      totalBytes: 1024 * 1024 * 100,
      operation: 'push',
    });

    expect(reporter).toBeDefined();
    expect(typeof reporter.onFileStart).toBe('function');
    expect(typeof reporter.onFileComplete).toBe('function');
    expect(typeof reporter.onFileError).toBe('function');
    expect(typeof reporter.finish).toBe('function');
  });

  it('should report per-file progress', async () => {
    const module = await import('../../../src/cli/sync.js');
    const reporter = new module.SyncProgressReporter({
      totalFiles: 3,
      totalBytes: 1024 * 3,
      operation: 'push',
    });

    reporter.onFileStart('users/data.parquet', 1024);
    reporter.onFileComplete('users/data.parquet', 1024);

    expect(reporter.filesCompleted).toBe(1);
    expect(reporter.bytesTransferred).toBe(1024);
  });

  it('should calculate and display transfer rate', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.calculateTransferRate).toBeDefined();
    expect(typeof module.calculateTransferRate).toBe('function');

    const rate = module.calculateTransferRate(10 * 1024 * 1024, 2000);
    expect(rate).toBe(5 * 1024 * 1024);
  });

  it('should format transfer rate for display', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.formatTransferRate).toBeDefined();
    expect(typeof module.formatTransferRate).toBe('function');

    expect(module.formatTransferRate(1024)).toBe('1.00 KB/s');
    expect(module.formatTransferRate(1024 * 1024)).toBe('1.00 MB/s');
    expect(module.formatTransferRate(1024 * 1024 * 10)).toBe('10.00 MB/s');
  });

  it('should estimate time remaining', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.estimateTimeRemaining).toBeDefined();
    expect(typeof module.estimateTimeRemaining).toBe('function');

    const eta = module.estimateTimeRemaining(50 * 1024 * 1024, 10 * 1024 * 1024);
    expect(eta).toBe(5000);
  });

  it('should display collection-level progress', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.CollectionProgressTracker).toBeDefined();

    const tracker = new module.CollectionProgressTracker(['users', 'orders', 'products']);
    expect(tracker).toBeDefined();

    tracker.startCollection('users');
    tracker.updateCollection('users', { filesCompleted: 3, totalFiles: 5 });
    tracker.completeCollection('users');

    expect(tracker.getCollectionStatus('users')).toBe('completed');
    expect(tracker.getCollectionStatus('orders')).toBe('pending');
  });

  it('should show overall sync progress percentage', async () => {
    const module = await import('../../../src/cli/sync.js');
    const reporter = new module.SyncProgressReporter({
      totalFiles: 10,
      totalBytes: 1024 * 1024,
      operation: 'pull',
    });

    reporter.onFileComplete('file1.parquet', 102400);
    reporter.onFileComplete('file2.parquet', 102400);

    expect(reporter.getProgressPercentage()).toBe(20);
  });

  it('should support verbose progress with file names', async () => {
    const module = await import('../../../src/cli/sync.js');
    const reporter = new module.SyncProgressReporter({
      totalFiles: 3,
      totalBytes: 1024 * 3,
      operation: 'push',
      verbose: true,
    });

    reporter.onFileStart('users/data.parquet', 1024);
    expect(reporter.currentFile).toBe('users/data.parquet');
  });

  it('should emit progress events', async () => {
    const module = await import('../../../src/cli/sync.js');
    const reporter = new module.SyncProgressReporter({
      totalFiles: 5,
      totalBytes: 5120,
      operation: 'push',
    });

    const events: Array<{ type: string; data: unknown }> = [];
    reporter.on('progress', (data: unknown) => events.push({ type: 'progress', data }));
    reporter.on('complete', (data: unknown) => events.push({ type: 'complete', data }));

    reporter.onFileComplete('file1.parquet', 1024);
    reporter.onFileComplete('file2.parquet', 1024);

    expect(events.filter(e => e.type === 'progress')).toHaveLength(2);
  });
});

// ============================================================================
// Checksum Verification Tests
// ============================================================================

describe('CLI Sync - Checksum Verification', () => {
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

  it('should export verifyChecksum function', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.verifyChecksum).toBeDefined();
    expect(typeof module.verifyChecksum).toBe('function');
  });

  it('should verify file integrity after download', async () => {
    const module = await import('../../../src/cli/sync.js');

    const data = new Uint8Array([1, 2, 3, 4, 5]);
    const expectedHash = 'abc123';

    const isValid = await module.verifyChecksum(data, expectedHash);
    expect(typeof isValid).toBe('boolean');
  });
});
