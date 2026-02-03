/**
 * Type definitions for MongoLake Sync Commands
 */

// ============================================================================
// Core Types
// ============================================================================

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

// ============================================================================
// Sync Options
// ============================================================================

export interface SyncOptions {
  /** Database name */
  database: string;

  /** Remote URL to sync with */
  remote: string;

  /** Path to local data directory (default: .mongolake) */
  path: string;

  /** Enable verbose logging */
  verbose: boolean;

  /** Dry run mode - show what would be synced without making changes */
  dryRun: boolean;

  /** Force sync even if there are conflicts */
  force: boolean;

  /** Authentication profile */
  profile: string;
}

export interface ExtendedSyncOptions extends SyncOptions {
  bandwidthLimit?: number;
  verify?: boolean;
  fullSync?: boolean;
  collections?: string[];
  concurrency?: number;
  include?: string[];
  exclude?: string[];
}

// ============================================================================
// Sync Results
// ============================================================================

export interface SyncResult {
  success: boolean;
  filesUploaded: number;
  filesDownloaded: number;
  bytesTransferred: number;
  errors: string[];
}

export interface ExtendedSyncResult extends SyncResult {
  averageSpeed?: number;
  verificationResults?: {
    passed: number;
    failed: number;
  };
  syncType?: 'full' | 'incremental';
  collectionsProcessed?: string[];
}

// ============================================================================
// Diff Types
// ============================================================================

export interface SyncDiff {
  toUpload: FileState[];
  toDownload: FileState[];
  toDeleteLocal: FileState[];
  toDeleteRemote: FileState[];
  conflicts: Array<{ local: FileState; remote: FileState }>;
}

// ============================================================================
// Conflict Types
// ============================================================================

export interface Conflict {
  path: string;
  localHash: string;
  remoteHash: string;
  localModifiedAt: number;
  remoteModifiedAt: number;
}

export interface ConflictResolution {
  action: 'upload' | 'download' | 'skip';
  path: string;
}

export type ConflictStrategy = 'local-wins' | 'remote-wins' | 'newest-wins' | 'abort';

// ============================================================================
// Progress Types
// ============================================================================

export interface ProgressReporter {
  update: (current: number, label?: string) => void;
  finish: () => void;
}

export type EventCallback = (data: unknown) => void;

// ============================================================================
// Manifest Types
// ============================================================================

export interface SyncManifest {
  version: '1.0';
  database: string;
  files: FileState[];
  lastSyncTimestamp: number;
}

// ============================================================================
// Change Detection Types
// ============================================================================

export interface LocalChanges {
  modified: FileState[];
  added: FileState[];
  deleted: FileState[];
}

// ============================================================================
// Resume Types
// ============================================================================

export interface SyncProgressState {
  operation: 'push' | 'pull';
  completedFiles: string[];
  pendingFiles: string[];
  startedAt: number;
}

// ============================================================================
// Chunked Transfer Types
// ============================================================================

export interface ChunkMetadata {
  totalChunks: number;
  chunkSize: number;
}

export interface UploadProgress {
  uploadId: string;
  filePath: string;
  totalChunks: number;
  completedChunks: number;
  completedBytes: number;
}
