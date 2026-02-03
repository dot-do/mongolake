/**
 * Tests for MongoLake Sync Conflict Resolution
 *
 * Tests conflict resolution functionality including:
 * - Conflict detection
 * - Conflict resolution strategies
 * - Bidirectional sync
 * - Error scenarios
 * - Merge conflict handling
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  mockStorage,
  mockFetch,
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
// Conflict Resolution Strategy Tests
// ============================================================================

describe('CLI Sync - Conflict Resolution', () => {
  it('should support local-wins strategy', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.resolveConflict).toBeDefined();

    const conflict = {
      path: 'data.parquet',
      localHash: 'local-hash',
      remoteHash: 'remote-hash',
      localModifiedAt: Date.now(),
      remoteModifiedAt: Date.now() - 30000,
    };

    const resolution = module.resolveConflict(conflict, 'local-wins');
    expect(resolution.action).toBe('upload');
    expect(resolution.path).toBe('data.parquet');
  });

  it('should support remote-wins strategy', async () => {
    const module = await import('../../../src/cli/sync.js');

    const conflict = {
      path: 'data.parquet',
      localHash: 'local-hash',
      remoteHash: 'remote-hash',
      localModifiedAt: Date.now() - 30000,
      remoteModifiedAt: Date.now(),
    };

    const resolution = module.resolveConflict(conflict, 'remote-wins');
    expect(resolution.action).toBe('download');
    expect(resolution.path).toBe('data.parquet');
  });

  it('should support newest-wins strategy', async () => {
    const module = await import('../../../src/cli/sync.js');

    const conflictLocalNewer = {
      path: 'data.parquet',
      localHash: 'local-hash',
      remoteHash: 'remote-hash',
      localModifiedAt: Date.now(),
      remoteModifiedAt: Date.now() - 30000,
    };

    const conflictRemoteNewer = {
      path: 'data.parquet',
      localHash: 'local-hash',
      remoteHash: 'remote-hash',
      localModifiedAt: Date.now() - 30000,
      remoteModifiedAt: Date.now(),
    };

    const resolutionLocalNewer = module.resolveConflict(conflictLocalNewer, 'newest-wins');
    expect(resolutionLocalNewer.action).toBe('upload');

    const resolutionRemoteNewer = module.resolveConflict(conflictRemoteNewer, 'newest-wins');
    expect(resolutionRemoteNewer.action).toBe('download');
  });

  it('should support abort-on-conflict strategy', async () => {
    const module = await import('../../../src/cli/sync.js');

    const conflict = {
      path: 'data.parquet',
      localHash: 'local-hash',
      remoteHash: 'remote-hash',
      localModifiedAt: Date.now(),
      remoteModifiedAt: Date.now(),
    };

    expect(() => {
      module.resolveConflict(conflict, 'abort');
    }).toThrow(/conflict/i);
  });
});

// ============================================================================
// Conflict Detection Tests
// ============================================================================

describe('CLI Sync - Conflict Detection', () => {
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

  it('should handle conflicts (remote changed)', async () => {
    const module = await import('../../../src/cli/sync.js');
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

  it('should handle merge conflicts', async () => {
    const module = await import('../../../src/cli/sync.js');
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

    const resolution = await module.handleMergeConflicts(conflicts, 'remote-wins');
    expect(resolution).toBeDefined();
    expect(resolution.filesToDownload).toContain('data.parquet');
  });
});

// ============================================================================
// Bidirectional Sync Tests
// ============================================================================

describe('CLI Sync - Bidirectional Sync', () => {
  beforeEach(() => {
    mockFetch.mockReset();
    vi.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should export sync command for bidirectional sync', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.runSync).toBeDefined();
    expect(typeof module.runSync).toBe('function');
  });

  it('should detect changes in both directions', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.detectBidirectionalChanges).toBeDefined();

    const localState = {
      database: 'testdb',
      files: [
        { path: 'testdb/users/data.parquet', size: 1024, hash: 'local-users', modifiedAt: Date.now() },
        { path: 'testdb/orders/data.parquet', size: 2048, hash: 'local-orders', modifiedAt: Date.now() - 60000 },
      ],
      lastSyncTimestamp: Date.now() - 120000,
      version: '1.0',
    };

    const remoteState = {
      database: 'testdb',
      files: [
        { path: 'testdb/users/data.parquet', size: 1024, hash: 'remote-users', modifiedAt: Date.now() - 30000 },
        { path: 'testdb/orders/data.parquet', size: 4096, hash: 'remote-orders', modifiedAt: Date.now() },
        { path: 'testdb/products/data.parquet', size: 8192, hash: 'remote-products', modifiedAt: Date.now() },
      ],
      lastSyncTimestamp: Date.now(),
      version: '1.0',
    };

    const changes = module.detectBidirectionalChanges(localState, remoteState);

    expect(changes.toUpload).toBeDefined();
    expect(changes.toDownload).toBeDefined();
    expect(changes.conflicts).toBeDefined();
  });

  it('should merge non-conflicting changes', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.mergeNonConflictingChanges).toBeDefined();

    const localChanges = [
      { path: 'testdb/users/data.parquet', action: 'modified' as const },
    ];

    const remoteChanges = [
      { path: 'testdb/orders/data.parquet', action: 'modified' as const },
      { path: 'testdb/products/data.parquet', action: 'added' as const },
    ];

    const merged = module.mergeNonConflictingChanges(localChanges, remoteChanges);

    expect(merged.toUpload).toHaveLength(1);
    expect(merged.toDownload).toHaveLength(2);
    expect(merged.conflicts).toHaveLength(0);
  });

  it('should identify three-way conflicts', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.identifyThreeWayConflicts).toBeDefined();

    const baseState = {
      files: [{ path: 'data.parquet', hash: 'base-hash' }],
    };

    const localState = {
      files: [{ path: 'data.parquet', hash: 'local-hash' }],
    };

    const remoteState = {
      files: [{ path: 'data.parquet', hash: 'remote-hash' }],
    };

    const conflicts = module.identifyThreeWayConflicts(baseState, localState, remoteState);

    expect(conflicts).toHaveLength(1);
    expect(conflicts[0].path).toBe('data.parquet');
    expect(conflicts[0].baseHash).toBe('base-hash');
    expect(conflicts[0].localHash).toBe('local-hash');
    expect(conflicts[0].remoteHash).toBe('remote-hash');
  });
});

// ============================================================================
// Sync Error Scenarios Tests
// ============================================================================

describe('CLI Sync - Error Scenarios', () => {
  beforeEach(() => {
    mockFetch.mockReset();
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should handle partial upload failures gracefully', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.handlePartialFailure).toBeDefined();

    const result = module.handlePartialFailure({
      successful: ['file1.parquet', 'file2.parquet', 'file3.parquet'],
      failed: [
        { path: 'file4.parquet', error: 'Network timeout' },
        { path: 'file5.parquet', error: 'Server error 500' },
      ],
    });

    expect(result.partialSuccess).toBe(true);
    expect(result.successCount).toBe(3);
    expect(result.failedCount).toBe(2);
    expect(result.retryable).toHaveLength(2);
  });

  it('should support retry for failed files', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.retryFailedFiles).toBeDefined();
    expect(typeof module.retryFailedFiles).toBe('function');

    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
    });

    const failedFiles = [
      { path: 'file1.parquet', error: 'Timeout', retryCount: 0 },
      { path: 'file2.parquet', error: 'Server error', retryCount: 1 },
    ];

    const result = await module.retryFailedFiles(
      'testdb',
      'https://api.mongolake.com',
      failedFiles,
      '.mongolake',
      'test-token',
      { maxRetries: 3 }
    );

    expect(result).toBeDefined();
  });

  it('should handle authentication expiry during sync', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.handleAuthExpiry).toBeDefined();

    mockFetch
      .mockResolvedValueOnce({ ok: true, status: 200 })
      .mockResolvedValueOnce({ ok: false, status: 401, statusText: 'Unauthorized' });

    const result = await module.handleAuthExpiry({
      database: 'testdb',
      remote: 'https://api.mongolake.com',
      profile: 'default',
    });

    expect(result.needsReauth).toBe(true);
    expect(result.message).toContain('expired');
  });

  it('should handle remote server unavailable', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.checkRemoteAvailability).toBeDefined();
    expect(typeof module.checkRemoteAvailability).toBe('function');

    mockFetch.mockRejectedValue(new Error('ECONNREFUSED'));

    const available = await module.checkRemoteAvailability('https://api.mongolake.com');
    expect(available).toBe(false);
  });

  it('should handle disk space errors during pull', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.checkDiskSpace).toBeDefined();
    expect(typeof module.checkDiskSpace).toBe('function');

    const result = await module.checkDiskSpace('.mongolake', 100 * 1024 * 1024);
    expect(result).toHaveProperty('available');
    expect(result).toHaveProperty('sufficient');
  });

  it('should rollback partial sync on critical error', async () => {
    const module = await import('../../../src/cli/sync.js');
    expect(module.rollbackSync).toBeDefined();
    expect(typeof module.rollbackSync).toBe('function');

    const rollbackResult = await module.rollbackSync({
      database: 'testdb',
      path: '.mongolake',
      filesWritten: ['file1.parquet', 'file2.parquet'],
      originalState: {
        files: [{ path: 'original.parquet', hash: 'abc123' }],
      },
    });

    expect(rollbackResult.success).toBe(true);
    expect(rollbackResult.restoredFiles).toHaveLength(1);
    expect(rollbackResult.removedFiles).toHaveLength(2);
  });
});
