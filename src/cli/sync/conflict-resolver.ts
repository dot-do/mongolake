/**
 * Conflict Resolution for MongoLake Sync
 *
 * Detects and resolves conflicts between local and remote states.
 */

import type {
  DatabaseState,
  Conflict,
  ConflictResolution,
  ConflictStrategy,
} from './types.js';

/**
 * Detect conflicts between local and remote states
 */
export function detectConflicts(
  localState: DatabaseState,
  remoteState: DatabaseState
): Conflict[] {
  const conflicts: Conflict[] = [];
  const localFiles = new Map(localState.files.map((f) => [f.path, f]));
  const remoteFiles = new Map(remoteState.files.map((f) => [f.path, f]));

  for (const [path, localFile] of localFiles) {
    const remoteFile = remoteFiles.get(path);
    if (remoteFile && localFile.hash !== remoteFile.hash) {
      conflicts.push({
        path,
        localHash: localFile.hash,
        remoteHash: remoteFile.hash,
        localModifiedAt: localFile.modifiedAt,
        remoteModifiedAt: remoteFile.modifiedAt,
      });
    }
  }

  return conflicts;
}

/**
 * Resolve a single conflict using the specified strategy
 */
export function resolveConflict(
  conflict: Conflict,
  strategy: ConflictStrategy
): ConflictResolution {
  if (strategy === 'abort') {
    throw new Error(`Conflict detected for file: ${conflict.path}`);
  }

  if (strategy === 'local-wins') {
    return { action: 'upload', path: conflict.path };
  }

  if (strategy === 'remote-wins') {
    return { action: 'download', path: conflict.path };
  }

  // newest-wins
  if (conflict.localModifiedAt > conflict.remoteModifiedAt) {
    return { action: 'upload', path: conflict.path };
  }
  return { action: 'download', path: conflict.path };
}

/**
 * Handle multiple merge conflicts with a given strategy
 */
export async function handleMergeConflicts(
  conflicts: Conflict[],
  strategy: 'local-wins' | 'remote-wins' | 'newest-wins'
): Promise<{ filesToUpload: string[]; filesToDownload: string[] }> {
  const result = {
    filesToUpload: [] as string[],
    filesToDownload: [] as string[],
  };

  for (const conflict of conflicts) {
    const resolution = resolveConflict(conflict, strategy);
    if (resolution.action === 'upload') {
      result.filesToUpload.push(resolution.path);
    } else if (resolution.action === 'download') {
      result.filesToDownload.push(resolution.path);
    }
  }

  return result;
}
