/**
 * Tests for MongoLake Sync Commands - Push Command
 *
 * Tests the push synchronization functionality including:
 * - Local change detection
 * - File upload
 * - Remote manifest updates
 * - Conflict detection
 * - Force and dry-run modes
 * - Progress reporting
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
// Push Command Tests
// ============================================================================

describe('Push Command', () => {
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

  it('should detect local changes since last sync', async () => {
    const module = await import('../../../../src/cli/sync.js');
    expect(module.detectLocalChanges).toBeDefined();
    expect(typeof module.detectLocalChanges).toBe('function');

    // Should track last sync timestamp
    const lastSync = Date.now() - 60000; // 1 minute ago
    const changes = await module.detectLocalChanges('testdb', '.mongolake', lastSync);
    expect(changes).toBeDefined();
    expect(Array.isArray(changes.modified)).toBe(true);
    expect(Array.isArray(changes.added)).toBe(true);
    expect(Array.isArray(changes.deleted)).toBe(true);
  });

  it('should upload changed Parquet files to remote', async () => {
    const module = await import('../../../../src/cli/sync.js');
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
    const module = await import('../../../../src/cli/sync.js');
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

  it('should handle conflicts (remote changed)', async () => {
    const module = await import('../../../../src/cli/sync.js');
    expect(module.detectConflicts).toBeDefined();
    expect(typeof module.detectConflicts).toBe('function');

    const localState = {
      database: 'testdb',
      files: [{ path: 'data.parquet', size: 1024, hash: 'local-hash', modifiedAt: Date.now() }],
      lastSyncTimestamp: Date.now() - 60000,
      version: '1.0',
    };

    const remoteState = {
      database: 'testdb',
      files: [{ path: 'data.parquet', size: 2048, hash: 'remote-hash', modifiedAt: Date.now() }],
      lastSyncTimestamp: Date.now(),
      version: '1.0',
    };

    const conflicts = module.detectConflicts(localState, remoteState);
    expect(conflicts).toHaveLength(1);
    expect(conflicts[0].path).toBe('data.parquet');
    expect(conflicts[0].localHash).toBe('local-hash');
    expect(conflicts[0].remoteHash).toBe('remote-hash');
  });

  it('should support --force flag', async () => {
    const module = await import('../../../../src/cli/sync.js');

    // With force flag, should overwrite remote even with conflicts
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
      json: async () => null,
    });

    const result = await module.runPush({
      database: 'testdb',
      remote: 'https://api.mongolake.com',
      path: '.mongolake',
      verbose: false,
      dryRun: true, // Dry run enabled
      force: false,
      profile: 'default',
    });

    expect(result.success).toBe(true);
    // In dry run, no files should be actually uploaded
    expect(result.filesUploaded).toBe(0);
  });

  it('should show progress during upload', async () => {
    const module = await import('../../../../src/cli/sync.js');
    expect(module.createProgressReporter).toBeDefined();
    expect(typeof module.createProgressReporter).toBe('function');

    const reporter = module.createProgressReporter(5);
    expect(reporter).toBeDefined();
    expect(typeof reporter.update).toBe('function');
    expect(typeof reporter.finish).toBe('function');

    // Report progress
    reporter.update(1, 'file1.parquet');
    reporter.update(2, 'file2.parquet');
    reporter.finish();
  });
});
