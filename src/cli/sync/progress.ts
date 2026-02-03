/**
 * Progress Reporting for MongoLake Sync
 *
 * Classes and utilities for tracking and reporting sync progress.
 */

import { ProgressBar } from '../utils.js';
import type { ProgressReporter, EventCallback } from './types.js';

/**
 * Create a simple progress reporter
 */
export function createProgressReporter(total: number): ProgressReporter {
  const bar = new ProgressBar(total);
  return {
    update: (current: number, label?: string) => bar.update(current, label),
    finish: () => bar.finish(),
  };
}

/**
 * Detailed sync progress reporter with event support
 */
export class SyncProgressReporter {
  private _totalFiles: number;
  private _verbose: boolean;
  private _filesCompleted: number = 0;
  private _bytesTransferred: number = 0;
  private _currentFile: string | null = null;
  private _events: Map<string, EventCallback[]> = new Map();

  constructor(options: {
    totalFiles: number;
    totalBytes: number;
    operation: 'push' | 'pull';
    verbose?: boolean;
  }) {
    this._totalFiles = options.totalFiles;
    // totalBytes and operation are available for future use
    void options.totalBytes;
    void options.operation;
    this._verbose = options.verbose ?? false;
  }

  get filesCompleted(): number {
    return this._filesCompleted;
  }

  get bytesTransferred(): number {
    return this._bytesTransferred;
  }

  get currentFile(): string | null {
    return this._currentFile;
  }

  onFileStart(filePath: string, _size: number): void {
    this._currentFile = filePath;
    if (this._verbose) {
      console.log(`Starting: ${filePath}`);
    }
  }

  onFileComplete(filePath: string, size: number): void {
    this._filesCompleted++;
    this._bytesTransferred += size;
    this._currentFile = null;
    this._emit('progress', {
      file: filePath,
      size,
      filesCompleted: this._filesCompleted,
      bytesTransferred: this._bytesTransferred,
    });
  }

  onFileError(filePath: string, error: Error): void {
    console.error(`Error processing ${filePath}: ${error.message}`);
  }

  getProgressPercentage(): number {
    if (this._totalFiles === 0) return 0;
    return Math.round((this._filesCompleted / this._totalFiles) * 100);
  }

  finish(): void {
    this._emit('complete', {
      filesCompleted: this._filesCompleted,
      bytesTransferred: this._bytesTransferred,
    });
  }

  on(event: string, callback: EventCallback): void {
    const callbacks = this._events.get(event) || [];
    callbacks.push(callback);
    this._events.set(event, callbacks);
  }

  private _emit(event: string, data: unknown): void {
    const callbacks = this._events.get(event) || [];
    for (const callback of callbacks) {
      callback(data);
    }
  }
}

/**
 * Track progress per collection
 */
export class CollectionProgressTracker {
  private status: Map<string, 'pending' | 'in-progress' | 'completed'> = new Map();

  constructor(collections: string[]) {
    for (const collection of collections) {
      this.status.set(collection, 'pending');
    }
  }

  startCollection(name: string): void {
    this.status.set(name, 'in-progress');
  }

  updateCollection(
    _name: string,
    _progress: { filesCompleted: number; totalFiles: number }
  ): void {
    // Update progress for collection
  }

  completeCollection(name: string): void {
    this.status.set(name, 'completed');
  }

  getCollectionStatus(name: string): string {
    return this.status.get(name) || 'unknown';
  }
}
