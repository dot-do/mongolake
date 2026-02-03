/**
 * Shared test utilities and mocks for sync tests
 */

import { vi } from 'vitest';

// Mock storage module
export const mockStorage = {
  get: vi.fn().mockResolvedValue(new Uint8Array([1, 2, 3])),
  put: vi.fn().mockResolvedValue(undefined),
  delete: vi.fn().mockResolvedValue(undefined),
  list: vi.fn().mockResolvedValue([]),
  head: vi.fn().mockResolvedValue({ size: 100 }),
  exists: vi.fn().mockResolvedValue(true),
};

// Mock fetch globally
export const mockFetch = vi.fn();

// Shared interfaces
export interface FileState {
  path: string;
  size: number;
  hash: string;
  modifiedAt: number;
}

export interface DatabaseState {
  database: string;
  files: FileState[];
  lastSyncTimestamp: number;
  version: string;
}

export interface SyncDiff {
  toUpload: FileState[];
  toDownload: FileState[];
  toDeleteLocal: FileState[];
  toDeleteRemote: FileState[];
}

export interface SyncResult {
  success: boolean;
  filesUploaded: number;
  filesDownloaded: number;
  bytesTransferred: number;
  errors: string[];
}

// Utility functions for tests
export function computeSyncDiff(
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

export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const k = 1024;
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(2)} ${units[i]}`;
}

export function formatDuration(ms: number): string {
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

// Setup function for sync tests
export function setupSyncMocks() {
  vi.mock('../../../src/storage/index.js', () => {
    return {
      FileSystemStorage: vi.fn(() => mockStorage),
      mockStorage,
    };
  });

  vi.mock('../../../src/cli/auth.js', () => ({
    getAccessToken: vi.fn().mockResolvedValue('test-token'),
  }));

  vi.stubGlobal('fetch', mockFetch);
}

export function resetSyncMocks() {
  mockFetch.mockReset();
  mockStorage.get.mockReset();
  mockStorage.get.mockResolvedValue(new Uint8Array([1, 2, 3]));
  mockStorage.put.mockReset();
  mockStorage.put.mockResolvedValue(undefined);
  mockStorage.list.mockReset();
  mockStorage.list.mockResolvedValue([]);
  mockStorage.head.mockReset();
  mockStorage.head.mockResolvedValue({ size: 100 });
}
