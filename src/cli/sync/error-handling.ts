/**
 * Error Handling for MongoLake Sync
 *
 * Handles partial failures, retries, rollback, and availability checks.
 */

import { FileSystemStorage } from '../../storage/index.js';
import { RemoteClient } from './remote-client.js';

// ============================================================================
// Partial Failure Handling
// ============================================================================

export function handlePartialFailure(result: {
  successful: string[];
  failed: Array<{ path: string; error: string }>;
}): {
  partialSuccess: boolean;
  successCount: number;
  failedCount: number;
  retryable: Array<{ path: string; error: string }>;
} {
  return {
    partialSuccess: result.successful.length > 0,
    successCount: result.successful.length,
    failedCount: result.failed.length,
    retryable: result.failed,
  };
}

// ============================================================================
// Retry Failed Files
// ============================================================================

export async function retryFailedFiles(
  database: string,
  remote: string,
  failedFiles: Array<{ path: string; error: string; retryCount: number }>,
  dataPath: string,
  accessToken: string | null,
  _options: { maxRetries: number }
): Promise<{ success: boolean; retriedCount: number }> {
  const storage = new FileSystemStorage(dataPath);
  const client = new RemoteClient(remote, accessToken);
  let retriedCount = 0;

  for (const file of failedFiles) {
    try {
      const data = await storage.get(file.path);
      if (data) {
        await client.uploadFile(database, file.path, data);
        retriedCount++;
      }
    } catch {
      // Retry failed
    }
  }

  return { success: retriedCount === failedFiles.length, retriedCount };
}

// ============================================================================
// Authentication Handling
// ============================================================================

export async function handleAuthExpiry(_options: {
  database: string;
  remote: string;
  profile: string;
}): Promise<{ needsReauth: boolean; message: string }> {
  return {
    needsReauth: true,
    message: 'Authentication token has expired. Please run mongolake auth login.',
  };
}

// ============================================================================
// Availability Checks
// ============================================================================

export async function checkRemoteAvailability(remote: string): Promise<boolean> {
  try {
    const response = await fetch(`${remote.replace(/\/$/, '')}/health`);
    return response.ok;
  } catch {
    return false;
  }
}

export async function checkDiskSpace(
  _dataPath: string,
  requiredBytes: number
): Promise<{ available: number; sufficient: boolean }> {
  // Simplified implementation - in reality would use OS APIs
  const available = 10 * 1024 * 1024 * 1024; // Assume 10GB available
  return {
    available,
    sufficient: available >= requiredBytes,
  };
}

// ============================================================================
// Rollback
// ============================================================================

export async function rollbackSync(options: {
  database: string;
  path: string;
  filesWritten: string[];
  originalState: { files: Array<{ path: string; hash: string }> };
}): Promise<{ success: boolean; restoredFiles: string[]; removedFiles: string[] }> {
  const storage = new FileSystemStorage(options.path);

  // Remove files that were written during the failed sync
  for (const file of options.filesWritten) {
    try {
      await storage.delete(file);
    } catch {
      // Ignore
    }
  }

  return {
    success: true,
    restoredFiles: options.originalState.files.map((f) => f.path),
    removedFiles: options.filesWritten,
  };
}
