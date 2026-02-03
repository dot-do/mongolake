/**
 * State management for the Compact command
 *
 * Manages running compactions, queued jobs, checkpoints, and history.
 *
 * @module cli/compact/state
 */

import type {
  CheckpointState,
  CompactionHistoryEntry,
  QueueOptions,
  QueueResult,
  PendingCompaction,
  CancelResult,
} from './types.js';

// ============================================================================
// State Storage (in-memory)
// ============================================================================

/** Track running compactions for concurrency control */
const runningCompactions = new Map<string, { startedAt: Date; compactionId: string }>();

/** Queue for pending compaction jobs */
const compactionQueue: Array<{
  database: string;
  collection: string;
  path: string;
  priority: string;
  queuedAt: Date;
}> = [];

/** Checkpoints storage (in-memory for now) */
const checkpoints = new Map<string, CheckpointState>();

/** Compaction history */
const compactionHistory: CompactionHistoryEntry[] = [];

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Generate a unique compaction ID
 */
export function generateCompactionId(): string {
  return `compact-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
}

/**
 * Get the key for tracking running compactions
 */
export function getCompactionKey(database: string, collection: string): string {
  return `${database}/${collection}`;
}

// ============================================================================
// Running Compaction Management
// ============================================================================

/**
 * Check if a compaction is currently running for the given collection
 */
export function isCompactionRunning(database: string, collection: string): boolean {
  const key = getCompactionKey(database, collection);
  return runningCompactions.has(key);
}

/**
 * Get the running compaction info for a collection
 */
export function getRunningCompaction(database: string, collection: string): { startedAt: Date; compactionId: string } | undefined {
  const key = getCompactionKey(database, collection);
  return runningCompactions.get(key);
}

/**
 * Mark a compaction as running
 */
export function markCompactionRunning(database: string, collection: string, compactionId: string): void {
  const key = getCompactionKey(database, collection);
  runningCompactions.set(key, { startedAt: new Date(), compactionId });
}

/**
 * Mark a compaction as complete
 */
export function markCompactionComplete(database: string, collection: string): void {
  const key = getCompactionKey(database, collection);
  runningCompactions.delete(key);
}

// ============================================================================
// Queue Management
// ============================================================================

/**
 * Queue a compaction job
 */
export async function queueCompaction(options: QueueOptions): Promise<QueueResult> {
  const priority = options.priority || 'normal';

  const job = {
    database: options.database,
    collection: options.collection,
    path: options.path,
    priority,
    queuedAt: new Date(),
  };

  // Insert based on priority
  if (priority === 'high') {
    // Find first non-high priority item
    const insertIndex = compactionQueue.findIndex((j) => j.priority !== 'high');
    if (insertIndex === -1) {
      compactionQueue.push(job);
    } else {
      compactionQueue.splice(insertIndex, 0, job);
    }
  } else {
    compactionQueue.push(job);
  }

  // Find position
  const position = compactionQueue.findIndex(
    (j) =>
      j.database === options.database &&
      j.collection === options.collection
  );

  return {
    queued: true,
    position,
  };
}

/**
 * Cancel a scheduled compaction
 */
export async function cancelCompaction(compactionId: string): Promise<CancelResult> {
  // In a real implementation, this would cancel the scheduled job
  return {
    cancelled: true,
    compactionId,
  };
}

/**
 * List pending compaction jobs
 */
export async function listPendingCompactions(_path: string): Promise<PendingCompaction[]> {
  return compactionQueue.map((j) => ({
    database: j.database,
    collection: j.collection,
    queuedAt: j.queuedAt,
    priority: j.priority,
  }));
}

// ============================================================================
// Checkpoint Management
// ============================================================================

/**
 * Save a checkpoint
 */
export async function saveCheckpoint(checkpoint: CheckpointState, _path: string): Promise<void> {
  const key = getCompactionKey(checkpoint.database, checkpoint.collection);
  checkpoints.set(key, checkpoint);
}

/**
 * Load a checkpoint
 */
export async function loadCheckpoint(
  database: string,
  collection: string,
  _path: string
): Promise<CheckpointState | null> {
  const key = getCompactionKey(database, collection);
  return checkpoints.get(key) || null;
}

/**
 * Clear a checkpoint
 */
export async function clearCheckpoint(database: string, collection: string): Promise<void> {
  const key = getCompactionKey(database, collection);
  checkpoints.delete(key);
}

// ============================================================================
// History Management
// ============================================================================

/**
 * Record a compaction in history
 */
export async function recordCompaction(entry: CompactionHistoryEntry): Promise<void> {
  compactionHistory.push(entry);
}

/**
 * Get compaction history for a collection
 */
export async function getCompactionHistory(
  database: string,
  collection: string,
  _path: string
): Promise<CompactionHistoryEntry[]> {
  return compactionHistory.filter(
    (e) => e.database === database && e.collection === collection
  );
}
