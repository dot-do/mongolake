/**
 * State Management for MongoLake Sync
 *
 * Handles local state, manifests, and sync history.
 */

import * as crypto from 'node:crypto';
import { FileSystemStorage, type StorageBackend } from '../../storage/index.js';
import type {
  DatabaseState,
  FileState,
  SyncManifest,
  SyncProgressState,
} from './types.js';

// ============================================================================
// Hash Computation
// ============================================================================

export async function computeFileHash(data: Uint8Array): Promise<string> {
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = new Uint8Array(hashBuffer);
  return Array.from(hashArray)
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
}

// ============================================================================
// Local State
// ============================================================================

export async function getLocalState(
  storage: StorageBackend,
  database: string
): Promise<DatabaseState> {
  const files: FileState[] = [];
  const allFiles = await storage.list(database);

  for (const filePath of allFiles) {
    // Skip internal files
    if (filePath.includes('/_sync/') || filePath.endsWith('.sync.json')) {
      continue;
    }

    const meta = await storage.head(filePath);
    if (!meta) continue;

    const data = await storage.get(filePath);
    if (!data) continue;

    const hash = await computeFileHash(data);

    files.push({
      path: filePath,
      size: meta.size,
      hash,
      modifiedAt: Date.now(),
    });
  }

  return {
    database,
    files,
    lastSyncTimestamp: Date.now(),
    version: '1.0',
  };
}

// ============================================================================
// Manifest Management
// ============================================================================

export function createSyncManifest(database: string, files: FileState[]): SyncManifest {
  return {
    version: '1.0',
    database,
    files,
    lastSyncTimestamp: Date.now(),
  };
}

export async function loadLocalManifest(
  database: string,
  dataPath: string
): Promise<SyncManifest | null> {
  const storage = new FileSystemStorage(dataPath);
  const manifestPath = `${database}/_sync/manifest.json`;

  try {
    const data = await storage.get(manifestPath);
    if (!data) return null;
    return JSON.parse(new TextDecoder().decode(data)) as SyncManifest;
  } catch {
    return null;
  }
}

export async function saveLocalManifest(
  database: string,
  dataPath: string,
  manifest: SyncManifest
): Promise<void> {
  const storage = new FileSystemStorage(dataPath);
  const manifestPath = `${database}/_sync/manifest.json`;
  const data = new TextEncoder().encode(JSON.stringify(manifest, null, 2));
  await storage.put(manifestPath, data);
}

export async function updateLocalManifest(
  database: string,
  dataPath: string,
  manifest: DatabaseState
): Promise<void> {
  const storage = new FileSystemStorage(dataPath);
  const manifestPath = `${database}/_sync/manifest.json`;
  const data = new TextEncoder().encode(JSON.stringify(manifest, null, 2));
  await storage.put(manifestPath, data);
}

// ============================================================================
// Sync Progress Tracking (for Resume)
// ============================================================================

export class SyncProgressTracker {
  private database: string;
  private dataPath: string;
  private state: SyncProgressState | null = null;

  constructor(database: string, dataPath: string) {
    this.database = database;
    this.dataPath = dataPath;
  }

  async save(state: SyncProgressState): Promise<void> {
    this.state = state;
    const storage = new FileSystemStorage(this.dataPath);
    const progressPath = `${this.database}/_sync/progress.json`;
    const data = new TextEncoder().encode(JSON.stringify(state, null, 2));
    await storage.put(progressPath, data);
  }

  async load(): Promise<SyncProgressState | null> {
    if (this.state) return this.state;

    const storage = new FileSystemStorage(this.dataPath);
    const progressPath = `${this.database}/_sync/progress.json`;

    try {
      const data = await storage.get(progressPath);
      if (!data) return null;
      this.state = JSON.parse(new TextDecoder().decode(data)) as SyncProgressState;
      return this.state;
    } catch {
      return null;
    }
  }

  async clear(): Promise<void> {
    this.state = null;
    const storage = new FileSystemStorage(this.dataPath);
    const progressPath = `${this.database}/_sync/progress.json`;
    try {
      await storage.delete(progressPath);
    } catch {
      // Ignore if file doesn't exist
    }
  }
}

// ============================================================================
// Sync History
// ============================================================================

export class SyncStateManager {
  private database: string;
  private dataPath: string;

  constructor(database: string, dataPath: string) {
    this.database = database;
    this.dataPath = dataPath;
  }

  async recordSync(record: {
    timestamp: number;
    filesProcessed: number;
    direction: 'push' | 'pull';
  }): Promise<void> {
    const storage = new FileSystemStorage(this.dataPath);
    const historyPath = `${this.database}/_sync/history.json`;

    let history: Array<typeof record> = [];
    try {
      const data = await storage.get(historyPath);
      if (data) {
        history = JSON.parse(new TextDecoder().decode(data));
      }
    } catch {
      // Start fresh
    }

    history.unshift(record);
    history = history.slice(0, 100); // Keep last 100 records

    await storage.put(historyPath, new TextEncoder().encode(JSON.stringify(history, null, 2)));
  }

  async getLastSync(): Promise<{
    timestamp: number;
    filesProcessed: number;
    direction: 'push' | 'pull';
  } | null> {
    const storage = new FileSystemStorage(this.dataPath);
    const historyPath = `${this.database}/_sync/history.json`;

    try {
      const data = await storage.get(historyPath);
      if (!data) return null;
      const history = JSON.parse(new TextDecoder().decode(data));
      return history[0] || null;
    } catch {
      return null;
    }
  }
}

export async function getSyncHistory(
  database: string,
  dataPath: string,
  _options?: { limit?: number }
): Promise<
  Array<{
    timestamp: number;
    direction: 'push' | 'pull';
    filesTransferred: number;
    bytesTransferred: number;
    duration: number;
    success: boolean;
  }>
> {
  const storage = new FileSystemStorage(dataPath);
  const historyPath = `${database}/_sync/history.json`;

  try {
    const data = await storage.get(historyPath);
    if (!data) return [];
    return JSON.parse(new TextDecoder().decode(data));
  } catch {
    return [];
  }
}

export async function recordSyncOperation(
  database: string,
  dataPath: string,
  record: {
    timestamp: number;
    direction: 'push' | 'pull';
    filesTransferred: number;
    bytesTransferred: number;
    duration: number;
    success: boolean;
  }
): Promise<void> {
  const storage = new FileSystemStorage(dataPath);
  const historyPath = `${database}/_sync/history.json`;

  let history: typeof record[] = [];
  try {
    const data = await storage.get(historyPath);
    if (data) {
      history = JSON.parse(new TextDecoder().decode(data));
    }
  } catch {
    // Start fresh
  }

  history.unshift(record);
  history = history.slice(0, 100);

  await storage.put(historyPath, new TextEncoder().encode(JSON.stringify(history, null, 2)));
}

// ============================================================================
// Sync Lock
// ============================================================================

export class SyncLock {
  private database: string;
  private dataPath: string;
  private _isLocked: boolean = false;
  private _hasCleanupHandler: boolean = false;
  private lockInfo: { pid: number; timestamp: number; hostname: string } | null = null;

  constructor(database: string, dataPath: string) {
    this.database = database;
    this.dataPath = dataPath;
  }

  get isLocked(): boolean {
    return this._isLocked;
  }

  get hasCleanupHandler(): boolean {
    return this._hasCleanupHandler;
  }

  async acquire(options?: { timeout?: number; force?: boolean }): Promise<boolean> {
    const storage = new FileSystemStorage(this.dataPath);
    const lockPath = `${this.database}/_sync/lock.json`;

    try {
      const existingLock = await storage.get(lockPath);
      if (existingLock && !options?.force) {
        const lockData = JSON.parse(new TextDecoder().decode(existingLock));
        const lockAge = Date.now() - lockData.timestamp;

        // If lock is stale (older than 1 hour), allow override
        if (lockAge < 3600000) {
          if (options?.timeout) {
            await new Promise((resolve) => setTimeout(resolve, options.timeout));
            return false;
          }
          return false;
        }
      }

      this.lockInfo = {
        pid: process.pid,
        timestamp: Date.now(),
        hostname: require('os').hostname(),
      };

      await storage.put(lockPath, new TextEncoder().encode(JSON.stringify(this.lockInfo)));
      this._isLocked = true;
      this._hasCleanupHandler = true;

      return true;
    } catch {
      return false;
    }
  }

  async release(): Promise<void> {
    if (!this._isLocked) return;

    const storage = new FileSystemStorage(this.dataPath);
    const lockPath = `${this.database}/_sync/lock.json`;

    try {
      await storage.delete(lockPath);
    } catch {
      // Ignore
    }

    this._isLocked = false;
    this.lockInfo = null;
  }

  async getLockInfo(): Promise<{ pid: number; timestamp: number; hostname: string } | null> {
    return this.lockInfo;
  }

  async simulateStaleLock(): Promise<void> {
    const storage = new FileSystemStorage(this.dataPath);
    const lockPath = `${this.database}/_sync/lock.json`;

    const staleLock = {
      pid: 99999,
      timestamp: Date.now() - 7200000, // 2 hours ago
      hostname: 'stale-host',
    };

    await storage.put(lockPath, new TextEncoder().encode(JSON.stringify(staleLock)));
  }
}
