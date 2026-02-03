/**
 * Tests for MongoLake Sync Commands - Diff Computation and Conflict Resolution
 *
 * Tests the sync diff computation, conflict detection, conflict resolution strategies,
 * and manifest management.
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
// Sync Diff Computation Tests
// ============================================================================

describe('CLI Sync - Diff Computation', () => {
  interface FileState {
    path: string;
    size: number;
    hash: string;
    modifiedAt: number;
  }

  interface DatabaseState {
    database: string;
    files: FileState[];
    lastSyncTimestamp: number;
    version: string;
  }

  interface SyncDiff {
    toUpload: FileState[];
    toDownload: FileState[];
    toDeleteLocal: FileState[];
    toDeleteRemote: FileState[];
  }

  function computeSyncDiff(
    localState: DatabaseState,
    remoteState: DatabaseState | null,
    direction: 'push' | 'pull'
  ): SyncDiff {
    const diff: SyncDiff = {
      toUpload: [],
      toDownload: [],
      toDeleteLocal: [],
      toDeleteRemote: [],
    };

    const localFiles = new Map(localState.files.map((f) => [f.path, f]));
    const remoteFiles = new Map(remoteState?.files.map((f) => [f.path, f]) ?? []);

    if (direction === 'push') {
      // Upload new or modified local files
      for (const [path, localFile] of localFiles) {
        const remoteFile = remoteFiles.get(path);
        if (!remoteFile || localFile.hash !== remoteFile.hash) {
          diff.toUpload.push(localFile);
        }
      }
      // Delete files that exist remotely but not locally
      for (const [path, remoteFile] of remoteFiles) {
        if (!localFiles.has(path)) {
          diff.toDeleteRemote.push(remoteFile);
        }
      }
    } else {
      // Download new or modified remote files
      for (const [path, remoteFile] of remoteFiles) {
        const localFile = localFiles.get(path);
        if (!localFile || localFile.hash !== remoteFile.hash) {
          diff.toDownload.push(remoteFile);
        }
      }
      // Delete files that exist locally but not remotely
      for (const [path, localFile] of localFiles) {
        if (!remoteFiles.has(path)) {
          diff.toDeleteLocal.push(localFile);
        }
      }
    }

    return diff;
  }

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

    const pullDiff = computeSyncDiff(localState, remoteState, 'pull');
    expect(pullDiff.toDownload).toHaveLength(0);
    expect(pullDiff.toDeleteLocal).toHaveLength(0);
  });
});

// ============================================================================
// Conflict Resolution Strategy Tests
// ============================================================================

describe('CLI Sync - Conflict Resolution', () => {
  it('should support local-wins strategy', async () => {
    const module = await import('../../../../src/cli/sync.js');
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
    const module = await import('../../../../src/cli/sync.js');

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
    const module = await import('../../../../src/cli/sync.js');

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
    const module = await import('../../../../src/cli/sync.js');

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
// Sync Manifest Tests
// ============================================================================

describe('CLI Sync - Manifest Management', () => {
  it('should export SyncManifest type', async () => {
    const module = await import('../../../../src/cli/sync.js');
    // The type should be exported for use by other modules
    expect(module.createSyncManifest).toBeDefined();
    expect(typeof module.createSyncManifest).toBe('function');
  });

  it('should create manifest with proper structure', async () => {
    const module = await import('../../../../src/cli/sync.js');
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
    const module = await import('../../../../src/cli/sync.js');
    expect(module.loadLocalManifest).toBeDefined();
    expect(typeof module.loadLocalManifest).toBe('function');

    const manifest = await module.loadLocalManifest('testdb', '.mongolake');
    // Should return null if no manifest exists
    expect(manifest === null || typeof manifest === 'object').toBe(true);
  });

  it('should save manifest to local storage', async () => {
    const module = await import('../../../../src/cli/sync.js');
    expect(module.saveLocalManifest).toBeDefined();
    expect(typeof module.saveLocalManifest).toBe('function');

    const manifest = {
      version: '1.0' as const,
      database: 'testdb',
      files: [{ path: 'data.parquet', size: 1024, hash: 'abc123', modifiedAt: Date.now() }],
      lastSyncTimestamp: Date.now(),
    };

    await module.saveLocalManifest('testdb', '.mongolake', manifest);
    // Should not throw
  });
});

// ============================================================================
// Bidirectional Sync Tests
// ============================================================================

describe('CLI Sync - Bidirectional Sync', () => {
  const mockFetch = vi.fn();
  vi.stubGlobal('fetch', mockFetch);

  beforeEach(() => {
    mockFetch.mockReset();
    vi.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should export sync command for bidirectional sync', async () => {
    const module = await import('../../../../src/cli/sync.js');
    expect(module.runSync).toBeDefined();
    expect(typeof module.runSync).toBe('function');
  });

  it('should detect changes in both directions', async () => {
    const module = await import('../../../../src/cli/sync.js');
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
    const module = await import('../../../../src/cli/sync.js');
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
    const module = await import('../../../../src/cli/sync.js');
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
