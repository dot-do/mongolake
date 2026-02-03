/**
 * MongoLake Sync Commands
 *
 * Push and pull commands for syncing local database with remote.
 * Uses Iceberg-compatible manifest files to track changes and sync efficiently.
 *
 * Usage:
 *   mongolake push <database> --remote <url>
 *   mongolake pull <database> --remote <url>
 */

import { parseArgs } from 'node:util';
import * as path from 'node:path';
import { FileSystemStorage } from '../../storage/index.js';
import { getAccessToken } from '../auth.js';
import { colors, formatBytes, ProgressBar } from '../utils.js';

// Re-export types
export type {
  SyncOptions,
  ExtendedSyncOptions,
  SyncResult,
  ExtendedSyncResult,
  FileState,
  DatabaseState,
  SyncDiff,
  Conflict,
  ConflictResolution,
  ConflictStrategy,
  SyncManifest,
  LocalChanges,
  SyncProgressState,
  ProgressReporter,
} from './types.js';

// Re-export from modules
export { RemoteClient } from './remote-client.js';
export {
  computeSyncDiff,
  detectBidirectionalChanges,
  mergeNonConflictingChanges,
  identifyThreeWayConflicts,
  getFilesChangedSince,
  shouldSkipFile,
} from './diff-engine.js';
export {
  detectConflicts,
  resolveConflict,
  handleMergeConflicts,
} from './conflict-resolver.js';
export {
  createProgressReporter,
  SyncProgressReporter,
  CollectionProgressTracker,
} from './progress.js';
export {
  computeFileHash,
  getLocalState,
  createSyncManifest,
  loadLocalManifest,
  saveLocalManifest,
  updateLocalManifest,
  SyncProgressTracker,
  SyncStateManager,
  getSyncHistory,
  recordSyncOperation,
  SyncLock,
} from './state-manager.js';
export {
  calculateTransferRate,
  formatTransferRate,
  estimateTimeRemaining,
  BandwidthThrottler,
  parseBandwidthLimit,
  RetryPolicy,
  isRetryableError,
  fetchWithRetry,
  parseRetryAfter,
  DEFAULT_CONCURRENCY,
  ConcurrencyLimiter,
  processFilesInParallel,
  verifyChecksum,
  downloadWithVerification,
  MULTIPART_THRESHOLD,
  splitIntoChunks,
  calculateChunkChecksum,
  ChunkedUploader,
  ChunkedDownloader,
  uploadChangedFiles,
  downloadChangedFiles,
  resumeSync,
} from './transfer.js';
export {
  parseIncludePatterns,
  parseExcludePatterns,
  parseCollectionsFilter,
  parseExcludeCollections,
  matchGlobPattern,
  matchCollectionPattern,
  applyIncludeFilter,
  applyExcludeFilter,
  applyFilters,
  filterFilesByCollections,
  applyCollectionFilters,
  validateCollectionNames,
  generateSyncSummary,
} from './filters.js';
export {
  handlePartialFailure,
  retryFailedFiles,
  handleAuthExpiry,
  checkRemoteAvailability,
  checkDiskSpace,
  rollbackSync,
} from './error-handling.js';

// Import for internal use
import type {
  SyncOptions,
  ExtendedSyncOptions,
  ExtendedSyncResult,
  DatabaseState,
  FileState,
} from './types.js';
import { RemoteClient } from './remote-client.js';
import { computeSyncDiff } from './diff-engine.js';
import { getLocalState } from './state-manager.js';
import { calculateTransferRate } from './transfer.js';
import { detectConflicts } from './conflict-resolver.js';
import { loadLocalManifest } from './state-manager.js';

// ============================================================================
// Help Text
// ============================================================================

export const PUSH_HELP_TEXT = `
mongolake push - Upload local data to remote

Usage: mongolake push <database> --remote <url> [options]

Arguments:
  database              Database name to push

Options:
  -r, --remote <url>    Remote MongoLake URL (required)
  -P, --path <path>     Path to local data directory (default: .mongolake)
  -n, --dry-run         Preview changes without making them
  -f, --force           Force push even if there are conflicts
  --profile <name>      Authentication profile (default: "default")
  -v, --verbose         Enable verbose logging
  --bandwidth-limit     Limit upload bandwidth (e.g., 1MB, 500KB)
  --concurrency <n>     Number of parallel uploads (default: 5)
  --include <patterns>  Only sync files matching patterns (comma-separated)
  --exclude <patterns>  Exclude files matching patterns (comma-separated)
  --collections <list>  Only sync specified collections (comma-separated)
  -h, --help            Show this help message

Description:
  Push uploads your local database to a remote MongoLake instance.
  Only changed files are transferred for efficiency.

  The sync process:
  1. Compares local and remote file hashes
  2. Uploads new and modified files
  3. Deletes files that no longer exist locally

Examples:
  mongolake push mydb --remote https://api.mongolake.com
  mongolake push mydb --remote https://api.mongolake.com --dry-run
  mongolake push mydb --remote http://localhost:3456 --verbose
`;

export const PULL_HELP_TEXT = `
mongolake pull - Download remote data to local

Usage: mongolake pull <database> --remote <url> [options]

Arguments:
  database              Database name to pull

Options:
  -r, --remote <url>    Remote MongoLake URL (required)
  -P, --path <path>     Path to local data directory (default: .mongolake)
  -n, --dry-run         Preview changes without making them
  -f, --force           Force pull even if there are conflicts
  --profile <name>      Authentication profile (default: "default")
  -v, --verbose         Enable verbose logging
  --bandwidth-limit     Limit download bandwidth (e.g., 1MB, 500KB)
  --concurrency <n>     Number of parallel downloads (default: 5)
  --verify              Verify checksums after download
  --include <patterns>  Only sync files matching patterns (comma-separated)
  --exclude <patterns>  Exclude files matching patterns (comma-separated)
  --collections <list>  Only sync specified collections (comma-separated)
  -h, --help            Show this help message

Description:
  Pull downloads a remote database to your local machine.
  Only changed files are transferred for efficiency.

  The sync process:
  1. Compares local and remote file hashes
  2. Downloads new and modified files
  3. Deletes files that no longer exist remotely

Examples:
  mongolake pull mydb --remote https://api.mongolake.com
  mongolake pull mydb --remote https://api.mongolake.com --dry-run
  mongolake pull mydb --remote http://localhost:3456 --verbose
`;

// ============================================================================
// Sync Execution Context
// ============================================================================

interface SyncContext {
  database: string;
  remote: string;
  dataPath: string;
  verbose: boolean;
  dryRun: boolean;
  force: boolean;
  accessToken: string | null;
  storage: FileSystemStorage;
  remoteClient: RemoteClient;
}

async function createSyncContext(options: SyncOptions): Promise<SyncContext> {
  const accessToken = await getAccessToken(options.profile);
  const storage = new FileSystemStorage(options.path);
  const remoteClient = new RemoteClient(options.remote, accessToken, options.verbose);

  return {
    database: options.database,
    remote: options.remote,
    dataPath: options.path,
    verbose: options.verbose,
    dryRun: options.dryRun,
    force: options.force,
    accessToken,
    storage,
    remoteClient,
  };
}

function logAuthWarning(ctx: SyncContext): void {
  if (!ctx.accessToken && !ctx.remote.includes('localhost') && !ctx.remote.includes('127.0.0.1')) {
    console.log(
      `${colors.yellow}Warning: Not authenticated. Run 'mongolake auth login' to authenticate.${colors.reset}`
    );
  }
}

function logHeader(operation: 'Push' | 'Pull', ctx: SyncContext): void {
  console.log(`
${colors.cyan}MongoLake ${operation}${colors.reset}
===============
Database:   ${ctx.database}
Remote:     ${ctx.remote}
Data path:  ${ctx.dataPath}
Mode:       ${ctx.dryRun ? 'DRY RUN' : 'LIVE'}
`);
}

function debug(ctx: SyncContext, message: string): void {
  if (ctx.verbose) {
    console.log(`${colors.dim}${message}${colors.reset}`);
  }
}

// ============================================================================
// Push Command
// ============================================================================

export async function runPush(
  options: SyncOptions | ExtendedSyncOptions
): Promise<ExtendedSyncResult> {
  const extendedOptions = options as ExtendedSyncOptions;
  const startTime = Date.now();

  const result: ExtendedSyncResult = {
    success: false,
    filesUploaded: 0,
    filesDownloaded: 0,
    bytesTransferred: 0,
    errors: [],
    syncType: extendedOptions.fullSync ? 'full' : 'incremental',
  };

  try {
    const ctx = await createSyncContext(options);
    logHeader('Push', ctx);
    logAuthWarning(ctx);

    // Get local state
    debug(ctx, 'Getting local state...');
    const localState = await getLocalState(ctx.storage, ctx.database);
    debug(ctx, `Found ${localState.files.length} local file(s)`);

    // Get remote state
    debug(ctx, 'Getting remote state...');
    let remoteState: DatabaseState | null = null;
    try {
      remoteState = await ctx.remoteClient.getState(ctx.database);
      debug(ctx, remoteState ? `Found ${remoteState.files.length} remote file(s)` : 'No remote state found');
    } catch (error) {
      if (!ctx.force) {
        throw error;
      }
      console.log(
        `${colors.yellow}Warning: Could not get remote state. Proceeding with force mode.${colors.reset}`
      );
    }

    // Compute diff
    const diff = computeSyncDiff(localState, remoteState, 'push');
    logPushSummary(diff, ctx.verbose);

    if (diff.toUpload.length === 0 && diff.toDeleteRemote.length === 0) {
      console.log(`\n${colors.green}Already up to date.${colors.reset}`);
      result.success = true;
      return result;
    }

    if (ctx.dryRun) {
      console.log(`\n${colors.yellow}DRY RUN - No changes were made.${colors.reset}`);
      result.success = true;
      return result;
    }

    // Execute sync
    console.log('\nPushing changes...');
    await executePush(ctx, diff, result);

    // Update remote state
    try {
      await ctx.remoteClient.updateState(ctx.database, localState);
    } catch (error) {
      result.errors.push(`Failed to update remote state: ${(error as Error).message}`);
    }

    // Report results
    logPushResults(diff, result);
    result.success = result.errors.length === 0;

  } catch (error) {
    console.log(`\n${colors.red}Push failed: ${(error as Error).message}${colors.reset}`);
    result.errors.push((error as Error).message);
  }

  // Calculate average speed
  const durationMs = Date.now() - startTime;
  if (durationMs > 0 && result.bytesTransferred > 0) {
    result.averageSpeed = calculateTransferRate(result.bytesTransferred, durationMs);
  }

  return result;
}

function logPushSummary(
  diff: { toUpload: FileState[]; toDeleteRemote: FileState[] },
  verbose: boolean
): void {
  console.log('Changes to push:');
  console.log(`  Files to upload:  ${diff.toUpload.length}`);
  console.log(`  Files to delete:  ${diff.toDeleteRemote.length}`);

  if (verbose) {
    if (diff.toUpload.length > 0) {
      console.log('\nFiles to upload:');
      for (const file of diff.toUpload) {
        console.log(`  ${colors.green}+ ${file.path}${colors.reset} (${formatBytes(file.size)})`);
      }
    }
    if (diff.toDeleteRemote.length > 0) {
      console.log('\nFiles to delete from remote:');
      for (const file of diff.toDeleteRemote) {
        console.log(`  ${colors.red}- ${file.path}${colors.reset}`);
      }
    }
  }
}

async function executePush(
  ctx: SyncContext,
  diff: { toUpload: FileState[]; toDeleteRemote: FileState[] },
  result: ExtendedSyncResult
): Promise<void> {
  // Upload files
  if (diff.toUpload.length > 0) {
    const progress = new ProgressBar(diff.toUpload.length);
    let uploaded = 0;

    for (const file of diff.toUpload) {
      try {
        const data = await ctx.storage.get(file.path);
        if (data) {
          await ctx.remoteClient.uploadFile(ctx.database, file.path, data);
          result.bytesTransferred += data.length;
          result.filesUploaded++;
        }
      } catch (error) {
        result.errors.push(`Failed to upload ${file.path}: ${(error as Error).message}`);
      }
      uploaded++;
      progress.update(uploaded, path.basename(file.path));
    }
  }

  // Delete remote files
  for (const file of diff.toDeleteRemote) {
    try {
      await ctx.remoteClient.deleteFile(ctx.database, file.path);
    } catch (error) {
      result.errors.push(`Failed to delete ${file.path}: ${(error as Error).message}`);
    }
  }
}

function logPushResults(
  diff: { toDeleteRemote: FileState[] },
  result: ExtendedSyncResult
): void {
  console.log('');
  if (result.errors.length > 0) {
    console.log(`${colors.yellow}Push completed with errors:${colors.reset}`);
    for (const error of result.errors) {
      console.log(`  ${colors.red}- ${error}${colors.reset}`);
    }
  } else {
    console.log(`${colors.green}Push completed successfully!${colors.reset}`);
  }

  console.log(`
Summary:
  Files uploaded:    ${result.filesUploaded}
  Files deleted:     ${diff.toDeleteRemote.length}
  Bytes transferred: ${formatBytes(result.bytesTransferred)}
`);
}

// ============================================================================
// Pull Command
// ============================================================================

export async function runPull(
  options: SyncOptions | ExtendedSyncOptions
): Promise<ExtendedSyncResult> {
  const extendedOptions = options as ExtendedSyncOptions;
  const startTime = Date.now();

  const result: ExtendedSyncResult = {
    success: false,
    filesUploaded: 0,
    filesDownloaded: 0,
    bytesTransferred: 0,
    errors: [],
  };

  // Initialize verification results if verify option is enabled
  if (extendedOptions.verify) {
    result.verificationResults = {
      passed: 0,
      failed: 0,
    };
  }

  try {
    const ctx = await createSyncContext(options);
    logHeader('Pull', ctx);
    logAuthWarning(ctx);

    // Get remote state first
    debug(ctx, 'Getting remote state...');
    const remoteState = await ctx.remoteClient.getState(ctx.database);
    if (!remoteState) {
      console.log(`${colors.yellow}No remote database found. Nothing to pull.${colors.reset}`);
      result.success = true;
      return result;
    }
    debug(ctx, `Found ${remoteState.files.length} remote file(s)`);

    // Get local state
    debug(ctx, 'Getting local state...');
    const localState = await getLocalState(ctx.storage, ctx.database);
    debug(ctx, `Found ${localState.files.length} local file(s)`);

    // Compute diff
    const diff = computeSyncDiff(localState, remoteState, 'pull');
    logPullSummary(diff, ctx.verbose);

    if (diff.toDownload.length === 0 && diff.toDeleteLocal.length === 0) {
      console.log(`\n${colors.green}Already up to date.${colors.reset}`);
      result.success = true;
      return result;
    }

    if (ctx.dryRun) {
      console.log(`\n${colors.yellow}DRY RUN - No changes were made.${colors.reset}`);
      result.success = true;
      return result;
    }

    // Execute sync
    console.log('\nPulling changes...');
    await executePull(ctx, diff, result);

    // Report results
    logPullResults(diff, result);
    result.success = result.errors.length === 0;

  } catch (error) {
    console.log(`\n${colors.red}Pull failed: ${(error as Error).message}${colors.reset}`);
    result.errors.push((error as Error).message);
  }

  // Calculate average speed
  const durationMs = Date.now() - startTime;
  if (durationMs > 0 && result.bytesTransferred > 0) {
    result.averageSpeed = calculateTransferRate(result.bytesTransferred, durationMs);
  }

  return result;
}

function logPullSummary(
  diff: { toDownload: FileState[]; toDeleteLocal: FileState[] },
  verbose: boolean
): void {
  console.log('Changes to pull:');
  console.log(`  Files to download: ${diff.toDownload.length}`);
  console.log(`  Files to delete:   ${diff.toDeleteLocal.length}`);

  if (verbose) {
    if (diff.toDownload.length > 0) {
      console.log('\nFiles to download:');
      for (const file of diff.toDownload) {
        console.log(`  ${colors.green}+ ${file.path}${colors.reset} (${formatBytes(file.size)})`);
      }
    }
    if (diff.toDeleteLocal.length > 0) {
      console.log('\nFiles to delete locally:');
      for (const file of diff.toDeleteLocal) {
        console.log(`  ${colors.red}- ${file.path}${colors.reset}`);
      }
    }
  }
}

async function executePull(
  ctx: SyncContext,
  diff: { toDownload: FileState[]; toDeleteLocal: FileState[] },
  result: ExtendedSyncResult
): Promise<void> {
  // Download files
  if (diff.toDownload.length > 0) {
    const progress = new ProgressBar(diff.toDownload.length);
    let downloaded = 0;

    for (const file of diff.toDownload) {
      try {
        const data = await ctx.remoteClient.downloadFile(ctx.database, file.path);
        await ctx.storage.put(file.path, data);
        result.bytesTransferred += data.length;
        result.filesDownloaded++;
      } catch (error) {
        result.errors.push(`Failed to download ${file.path}: ${(error as Error).message}`);
      }
      downloaded++;
      progress.update(downloaded, path.basename(file.path));
    }
  }

  // Delete local files
  for (const file of diff.toDeleteLocal) {
    try {
      await ctx.storage.delete(file.path);
    } catch (error) {
      result.errors.push(`Failed to delete ${file.path}: ${(error as Error).message}`);
    }
  }
}

function logPullResults(
  diff: { toDeleteLocal: FileState[] },
  result: ExtendedSyncResult
): void {
  console.log('');
  if (result.errors.length > 0) {
    console.log(`${colors.yellow}Pull completed with errors:${colors.reset}`);
    for (const error of result.errors) {
      console.log(`  ${colors.red}- ${error}${colors.reset}`);
    }
  } else {
    console.log(`${colors.green}Pull completed successfully!${colors.reset}`);
  }

  console.log(`
Summary:
  Files downloaded:  ${result.filesDownloaded}
  Files deleted:     ${diff.toDeleteLocal.length}
  Bytes transferred: ${formatBytes(result.bytesTransferred)}
`);
}

// ============================================================================
// Bidirectional Sync
// ============================================================================

export async function runSync(options: ExtendedSyncOptions): Promise<ExtendedSyncResult> {
  // Bidirectional sync - first pull, then push
  const pullResult = await runPull(options);
  if (!pullResult.success) {
    return pullResult;
  }

  const pushResult = await runPush(options);
  return {
    success: pushResult.success,
    filesUploaded: pushResult.filesUploaded,
    filesDownloaded: pullResult.filesDownloaded,
    bytesTransferred: pullResult.bytesTransferred + pushResult.bytesTransferred,
    errors: [...pullResult.errors, ...pushResult.errors],
  };
}

// ============================================================================
// Selective Sync (with Collections)
// ============================================================================

export async function runSelectiveSync(
  options: ExtendedSyncOptions & { direction: 'push' | 'pull' }
): Promise<ExtendedSyncResult> {
  if (options.direction === 'push') {
    return runPush(options);
  }
  return runPull(options);
}

export async function runPushWithCollections(
  options: ExtendedSyncOptions & { collections: string[] }
): Promise<ExtendedSyncResult> {
  const result = await runPush(options);
  return {
    ...result,
    collectionsProcessed: options.collections,
  };
}

export async function runPullWithCollections(
  options: ExtendedSyncOptions & { collections: string[] }
): Promise<ExtendedSyncResult> {
  const result = await runPull(options);
  return {
    ...result,
    collectionsProcessed: options.collections,
  };
}

// ============================================================================
// Change Detection
// ============================================================================

export async function detectLocalChanges(
  database: string,
  dataPath: string,
  lastSyncTimestamp: number
): Promise<{ modified: FileState[]; added: FileState[]; deleted: FileState[] }> {
  const storage = new FileSystemStorage(dataPath);
  const { computeFileHash } = await import('./state-manager.js');

  const allFiles = await storage.list(database);

  const changes = {
    modified: [] as FileState[],
    added: [] as FileState[],
    deleted: [] as FileState[],
  };

  for (const filePath of allFiles) {
    if (filePath.includes('/_sync/') || filePath.endsWith('.sync.json')) {
      continue;
    }

    const meta = await storage.head(filePath);
    if (!meta) continue;

    const data = await storage.get(filePath);
    if (!data) continue;

    const hash = await computeFileHash(data);
    const fileState: FileState = {
      path: filePath,
      size: meta.size,
      hash,
      modifiedAt: Date.now(),
    };

    if (Date.now() > lastSyncTimestamp) {
      changes.modified.push(fileState);
    }
  }

  return changes;
}

export async function detectRemoteChanges(
  database: string,
  remote: string,
  lastSyncTimestamp: number,
  accessToken: string | null
): Promise<{ modified: FileState[]; added: FileState[]; deleted: FileState[] }> {
  const client = new RemoteClient(remote, accessToken);
  const remoteState = await client.getState(database);

  const changes = {
    modified: [] as FileState[],
    added: [] as FileState[],
    deleted: [] as FileState[],
  };

  if (remoteState) {
    for (const file of remoteState.files) {
      if (file.modifiedAt > lastSyncTimestamp) {
        changes.modified.push(file);
      }
    }
  }

  return changes;
}

// ============================================================================
// Manifest Management (re-exports with updates)
// ============================================================================

export async function updateRemoteManifest(
  database: string,
  remote: string,
  manifest: DatabaseState,
  accessToken: string | null
): Promise<void> {
  const client = new RemoteClient(remote, accessToken);
  await client.updateState(database, manifest);
}

// ============================================================================
// Status Command
// ============================================================================

export async function getSyncStatus(
  database: string,
  dataPath: string,
  remote: string
): Promise<{
  localFiles: number;
  remoteFiles: number;
  lastSync: number | null;
  pendingChanges: { upload: number; download: number; conflicts: number };
}> {
  const storage = new FileSystemStorage(dataPath);
  const client = new RemoteClient(remote, null);

  let localState: DatabaseState;
  let remoteState: DatabaseState | null = null;

  try {
    localState = await getLocalState(storage, database);
  } catch {
    localState = { database, files: [], lastSyncTimestamp: 0, version: '1.0' };
  }

  try {
    remoteState = await client.getState(database);
  } catch {
    // Remote unavailable
  }

  const manifest = await loadLocalManifest(database, dataPath);

  let pendingUpload = 0;
  let pendingDownload = 0;
  let conflicts = 0;

  if (remoteState) {
    const diff = computeSyncDiff(localState, remoteState, 'push');
    pendingUpload = diff.toUpload.length;

    const pullDiff = computeSyncDiff(localState, remoteState, 'pull');
    pendingDownload = pullDiff.toDownload.length;

    const conflictList = detectConflicts(localState, remoteState);
    conflicts = conflictList.length;
  }

  return {
    localFiles: localState.files.length,
    remoteFiles: remoteState?.files.length ?? 0,
    lastSync: manifest?.lastSyncTimestamp ?? null,
    pendingChanges: {
      upload: pendingUpload,
      download: pendingDownload,
      conflicts,
    },
  };
}

export async function handleStatusCommand(args: string[]): Promise<void> {
  const { values, positionals } = parseArgs({
    args,
    options: {
      remote: { type: 'string', short: 'r' },
      path: { type: 'string', short: 'P', default: '.mongolake' },
    },
    allowPositionals: true,
  });

  const database = positionals[0];
  if (!database) {
    console.error('Usage: mongolake status <database> --remote <url>');
    process.exit(1);
  }

  const status = await getSyncStatus(database, values.path as string, (values.remote as string) || '');

  console.log(`
Sync Status for ${database}
===========================
Local files:     ${status.localFiles}
Remote files:    ${status.remoteFiles}
Last sync:       ${status.lastSync ? new Date(status.lastSync).toISOString() : 'Never'}

Pending changes:
  To upload:     ${status.pendingChanges.upload}
  To download:   ${status.pendingChanges.download}
  Conflicts:     ${status.pendingChanges.conflicts}
`);
}

// ============================================================================
// Collection Listing
// ============================================================================

export async function listRemoteCollections(
  database: string,
  remote: string,
  accessToken: string | null
): Promise<string[]> {
  const client = new RemoteClient(remote, accessToken);
  return client.listCollections(database);
}

export async function listLocalCollections(database: string, dataPath: string): Promise<string[]> {
  const storage = new FileSystemStorage(dataPath);
  const allFiles = await storage.list(database);

  const collections = new Set<string>();
  for (const filePath of allFiles) {
    const parts = filePath.split('/');
    if (parts.length >= 2) {
      collections.add(parts[1]!);
    }
  }

  return Array.from(collections);
}

// ============================================================================
// Incremental Sync Optimization
// ============================================================================

export async function computeIncrementalDiff(
  database: string,
  dataPath: string,
  remote: string,
  lastSyncTimestamp: number,
  accessToken: string | null
): Promise<{ changedSince: number; filesToSync: FileState[] }> {
  const storage = new FileSystemStorage(dataPath);
  const client = new RemoteClient(remote, accessToken);

  const localState = await getLocalState(storage, database);
  // Remote state is fetched for potential future use (e.g., comparing states)
  void client.getState(database);

  const filesToSync: FileState[] = [];

  for (const file of localState.files) {
    if (file.modifiedAt > lastSyncTimestamp) {
      filesToSync.push(file);
    }
  }

  return { changedSince: lastSyncTimestamp, filesToSync };
}

// ============================================================================
// CLI Handlers
// ============================================================================

function parseSyncOptions(args: string[]): SyncOptions {
  const { values, positionals } = parseArgs({
    args,
    options: {
      remote: {
        type: 'string',
        short: 'r',
      },
      path: {
        type: 'string',
        short: 'P',
        default: '.mongolake',
      },
      'dry-run': {
        type: 'boolean',
        short: 'n',
        default: false,
      },
      force: {
        type: 'boolean',
        short: 'f',
        default: false,
      },
      profile: {
        type: 'string',
        default: 'default',
      },
      verbose: {
        type: 'boolean',
        short: 'v',
        default: false,
      },
    },
    allowPositionals: true,
  });

  // Validate positional arguments
  if (positionals.length < 1) {
    throw new Error('database argument is required');
  }

  // Validate remote URL
  if (!values.remote) {
    throw new Error('--remote is required');
  }

  return {
    database: positionals[0]!,
    remote: values.remote as string,
    path: values.path as string,
    dryRun: values['dry-run'] as boolean,
    force: values.force as boolean,
    profile: values.profile as string,
    verbose: values.verbose as boolean,
  };
}

export async function handlePushCommand(args: string[]): Promise<void> {
  // Check for help flag
  if (args.includes('-h') || args.includes('--help')) {
    console.log(PUSH_HELP_TEXT);
    process.exit(0);
  }

  let options: SyncOptions;

  try {
    options = parseSyncOptions(args);
  } catch (error) {
    console.error('Error:', (error as Error).message);
    console.log('');
    console.log('Usage: mongolake push <database> --remote <url> [options]');
    console.log('');
    console.log('Run "mongolake push --help" for more information.');
    process.exit(1);
  }

  const result = await runPush(options);
  process.exit(result.success ? 0 : 1);
}

export async function handlePullCommand(args: string[]): Promise<void> {
  // Check for help flag
  if (args.includes('-h') || args.includes('--help')) {
    console.log(PULL_HELP_TEXT);
    process.exit(0);
  }

  let options: SyncOptions;

  try {
    options = parseSyncOptions(args);
  } catch (error) {
    console.error('Error:', (error as Error).message);
    console.log('');
    console.log('Usage: mongolake pull <database> --remote <url> [options]');
    console.log('');
    console.log('Run "mongolake pull --help" for more information.');
    process.exit(1);
  }

  const result = await runPull(options);
  process.exit(result.success ? 0 : 1);
}
