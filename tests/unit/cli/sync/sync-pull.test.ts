/**
 * Tests for MongoLake Sync Commands - Pull Command
 *
 * Tests the pull synchronization functionality including:
 * - Remote change detection
 * - File download
 * - Local manifest updates
 * - Merge conflict handling
 * - Force and dry-run modes
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

// Mock fetch globally
const mockFetch = vi.fn();
vi.stubGlobal('fetch', mockFetch);

// ============================================================================
// Pull Command Tests
// ============================================================================

describe('Pull Command', () => {
  beforeEach(async () => {
    mockFetch.mockReset();
    // Reset the storage mock to ensure clean state
    const { mockStorage } = await import('../../../../src/storage/index.js');
    (mockStorage as { get: ReturnType<typeof vi.fn> }).get.mockReset();
    (mockStorage as { get: ReturnType<typeof vi.fn> }).get.mockResolvedValue(new Uint8Array([1, 2, 3]));
    (mockStorage as { put: ReturnType<typeof vi.fn> }).put.mockReset();
    (mockStorage as { put: ReturnType<typeof vi.fn> }).put.mockResolvedValue(undefined);
    (mockStorage as { list: ReturnType<typeof vi.fn> }).list.mockReset();
    (mockStorage as { list: ReturnType<typeof vi.fn> }).list.mockResolvedValue([]);
    (mockStorage as { head: ReturnType<typeof vi.fn> }).head.mockReset();
    (mockStorage as { head: ReturnType<typeof vi.fn> }).head.mockResolvedValue({ size: 100 });
    vi.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should detect remote changes since last sync', async () => {
    const module = await import('../../../../src/cli/sync.js');
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

    const lastSync = Date.now() - 60000; // 1 minute ago
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
    const module = await import('../../../../src/cli/sync.js');
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
    const module = await import('../../../../src/cli/sync.js');
    expect(module.updateLocalManifest).toBeDefined();
    expect(typeof module.updateLocalManifest).toBe('function');

    const manifest = {
      database: 'testdb',
      files: [{ path: 'data.parquet', size: 1024, hash: 'abc123', modifiedAt: Date.now() }],
      lastSyncTimestamp: Date.now(),
      version: '1.0',
    };

    await module.updateLocalManifest('testdb', '.mongolake', manifest);
    // Should write to local storage
  });

  it('should handle merge conflicts', async () => {
    const module = await import('../../../../src/cli/sync.js');
    expect(module.handleMergeConflicts).toBeDefined();
    expect(typeof module.handleMergeConflicts).toBe('function');

    const conflicts = [
      {
        path: 'data.parquet',
        localHash: 'local-hash',
        remoteHash: 'remote-hash',
        localModifiedAt: Date.now() - 30000,
        remoteModifiedAt: Date.now(),
      },
    ];

    // Default strategy: remote wins on pull
    const resolution = await module.handleMergeConflicts(conflicts, 'remote-wins');
    expect(resolution).toBeDefined();
    expect(resolution.filesToDownload).toContain('data.parquet');
  });

  it('should support --force flag', async () => {
    const module = await import('../../../../src/cli/sync.js');

    // With force flag, should overwrite local even with conflicts
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
      force: true, // Force flag enabled
      profile: 'default',
    });

    // With force, should succeed even if there are conflicts
    expect(result).toBeDefined();
  });

  it('should support --dry-run flag', async () => {
    const module = await import('../../../../src/cli/sync.js');

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
      dryRun: true, // Dry run enabled
      force: false,
      profile: 'default',
    });

    expect(result.success).toBe(true);
    // In dry run, no files should be actually downloaded
    expect(result.filesDownloaded).toBe(0);
  });
});
