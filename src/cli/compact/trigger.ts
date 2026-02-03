/**
 * Trigger functions for the Compact command
 *
 * @module cli/compact/trigger
 */

import type { TriggerOptions, TriggerResult } from './types.js';
import {
  generateCompactionId,
  isCompactionRunning,
  markCompactionRunning,
  markCompactionComplete,
} from './state.js';

// Forward declaration - will be imported from index to avoid circular dependency
let runCompactFn: typeof import('./index.js').runCompact;

/**
 * Set the runCompact function to avoid circular dependency
 * @internal
 */
export function setRunCompactFn(fn: typeof import('./index.js').runCompact): void {
  runCompactFn = fn;
}

/**
 * Trigger compaction for a collection
 */
export async function triggerCompaction(options: TriggerOptions): Promise<TriggerResult> {
  const compactionId = generateCompactionId();

  // Check if already running
  if (isCompactionRunning(options.database, options.collection)) {
    return {
      triggered: false,
      scheduled: false,
      alreadyRunning: true,
      compactionId,
    };
  }

  if (options.immediate) {
    // Mark as running
    markCompactionRunning(options.database, options.collection, compactionId);

    // Run compaction in background
    runCompactFn({
      database: options.database,
      collection: options.collection,
      path: options.path,
      dryRun: false,
      verbose: false,
    }).finally(() => {
      markCompactionComplete(options.database, options.collection);
    });

    return {
      triggered: true,
      scheduled: false,
      alreadyRunning: false,
      startedAt: new Date(),
      compactionId,
    };
  }

  // Schedule for later
  const scheduledFor = new Date(Date.now() + (options.delay || 0));

  return {
    triggered: false,
    scheduled: true,
    alreadyRunning: false,
    scheduledFor,
    compactionId,
  };
}
