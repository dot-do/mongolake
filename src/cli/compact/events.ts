/**
 * Event emitters for the Compact command
 *
 * @module cli/compact/events
 */

import { EventEmitter } from 'node:events';
import type { ProgressEvent, CompactResult } from './types.js';

/**
 * Event emitter for compact command progress updates
 */
export class CompactEventEmitter extends EventEmitter {
  emitProgress(event: ProgressEvent): void {
    this.emit('progress', event);
  }

  emitPhase(phase: string): void {
    this.emit('phase', phase);
  }
}

/**
 * Event emitter for compaction lifecycle events
 */
export class CompactionEventEmitter extends EventEmitter {
  emitStarted(event: { database: string; collection: string; startedAt: Date }): void {
    this.emit('compaction-started', event);
  }

  emitCompleted(event: { database: string; collection: string; completedAt: Date; result: CompactResult }): void {
    this.emit('compaction-completed', event);
  }
}
