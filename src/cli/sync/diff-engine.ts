/**
 * Diff Engine for MongoLake Sync
 *
 * Computes differences between local and remote database states.
 */

import type {
  DatabaseState,
  FileState,
  SyncDiff,
  Conflict,
} from './types.js';

/**
 * Compute the sync diff between local and remote states
 */
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
    conflicts: [],
  };

  const localFiles = new Map(localState.files.map((f) => [f.path, f]));
  const remoteFiles = new Map(remoteState?.files.map((f) => [f.path, f]) ?? []);

  if (direction === 'push') {
    computePushDiff(diff, localFiles, remoteFiles);
  } else {
    computePullDiff(diff, localFiles, remoteFiles);
  }

  return diff;
}

function computePushDiff(
  diff: SyncDiff,
  localFiles: Map<string, FileState>,
  remoteFiles: Map<string, FileState>
): void {
  // Upload new or modified local files
  for (const [path, localFile] of localFiles) {
    const remoteFile = remoteFiles.get(path);
    if (!remoteFile) {
      diff.toUpload.push(localFile);
    } else if (localFile.hash !== remoteFile.hash) {
      diff.toUpload.push(localFile);
    }
  }

  // Delete files that exist remotely but not locally
  for (const [path, remoteFile] of remoteFiles) {
    if (!localFiles.has(path)) {
      diff.toDeleteRemote.push(remoteFile);
    }
  }
}

function computePullDiff(
  diff: SyncDiff,
  localFiles: Map<string, FileState>,
  remoteFiles: Map<string, FileState>
): void {
  // Download new or modified remote files
  for (const [path, remoteFile] of remoteFiles) {
    const localFile = localFiles.get(path);
    if (!localFile) {
      diff.toDownload.push(remoteFile);
    } else if (localFile.hash !== remoteFile.hash) {
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

/**
 * Detect bidirectional changes for two-way sync
 */
export function detectBidirectionalChanges(
  localState: DatabaseState,
  remoteState: DatabaseState
): {
  toUpload: FileState[];
  toDownload: FileState[];
  conflicts: Conflict[];
} {
  const toUpload: FileState[] = [];
  const toDownload: FileState[] = [];
  const conflicts: Conflict[] = [];

  const localFiles = new Map(localState.files.map((f) => [f.path, f]));
  const remoteFiles = new Map(remoteState.files.map((f) => [f.path, f]));

  for (const [path, localFile] of localFiles) {
    const remoteFile = remoteFiles.get(path);
    if (!remoteFile) {
      toUpload.push(localFile);
    } else if (localFile.hash !== remoteFile.hash) {
      conflicts.push({
        path,
        localHash: localFile.hash,
        remoteHash: remoteFile.hash,
        localModifiedAt: localFile.modifiedAt,
        remoteModifiedAt: remoteFile.modifiedAt,
      });
    }
  }

  for (const [path, remoteFile] of remoteFiles) {
    if (!localFiles.has(path)) {
      toDownload.push(remoteFile);
    }
  }

  return { toUpload, toDownload, conflicts };
}

/**
 * Merge non-conflicting changes for bidirectional sync
 */
export function mergeNonConflictingChanges(
  localChanges: Array<{ path: string; action: 'modified' | 'added' | 'deleted' }>,
  remoteChanges: Array<{ path: string; action: 'modified' | 'added' | 'deleted' }>
): {
  toUpload: Array<{ path: string; action: string }>;
  toDownload: Array<{ path: string; action: string }>;
  conflicts: Array<{ path: string }>;
} {
  const localPaths = new Set(localChanges.map((c) => c.path));
  const remotePaths = new Set(remoteChanges.map((c) => c.path));

  const conflicts = localChanges
    .filter((c) => remotePaths.has(c.path))
    .map((c) => ({ path: c.path }));

  return {
    toUpload: localChanges.filter((c) => !remotePaths.has(c.path)),
    toDownload: remoteChanges.filter((c) => !localPaths.has(c.path)),
    conflicts,
  };
}

/**
 * Identify three-way conflicts using a common base state
 */
export function identifyThreeWayConflicts(
  baseState: { files: Array<{ path: string; hash: string }> },
  localState: { files: Array<{ path: string; hash: string }> },
  remoteState: { files: Array<{ path: string; hash: string }> }
): Array<{ path: string; baseHash: string; localHash: string; remoteHash: string }> {
  const baseFiles = new Map(baseState.files.map((f) => [f.path, f.hash]));
  const localFiles = new Map(localState.files.map((f) => [f.path, f.hash]));
  const remoteFiles = new Map(remoteState.files.map((f) => [f.path, f.hash]));

  const conflicts: Array<{
    path: string;
    baseHash: string;
    localHash: string;
    remoteHash: string;
  }> = [];

  for (const [path, baseHash] of baseFiles) {
    const localHash = localFiles.get(path);
    const remoteHash = remoteFiles.get(path);

    if (
      localHash &&
      remoteHash &&
      localHash !== baseHash &&
      remoteHash !== baseHash &&
      localHash !== remoteHash
    ) {
      conflicts.push({
        path,
        baseHash,
        localHash,
        remoteHash,
      });
    }
  }

  return conflicts;
}

/**
 * Get files changed since a given timestamp
 */
export function getFilesChangedSince(files: FileState[], timestamp: number): FileState[] {
  return files.filter((f) => f.modifiedAt > timestamp);
}

/**
 * Check if a file should be skipped (hashes match)
 */
export function shouldSkipFile(localFile: FileState, remoteFile: FileState): boolean {
  return localFile.hash === remoteFile.hash;
}

/**
 * Filter files by collection paths
 */
export function filterFilesByCollections(
  files: FileState[],
  database: string,
  collections: string[]
): FileState[] {
  return files.filter((file) => {
    for (const collection of collections) {
      if (file.path.startsWith(`${database}/${collection}/`)) {
        return true;
      }
    }
    return false;
  });
}
