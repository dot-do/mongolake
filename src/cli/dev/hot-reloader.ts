/**
 * Hot Reloader for MongoLake Dev Server
 *
 * File watcher that monitors source files and triggers reloads on changes.
 * Uses chokidar for efficient cross-platform file watching with debouncing
 * to prevent excessive reloads during rapid edits.
 *
 * @module cli/dev/hot-reloader
 */

import { EventEmitter } from 'node:events';
// @ts-expect-error chokidar types are provided by the test mocks
import * as chokidar from 'chokidar';
import type { HotReloaderOptions, FileChangeEvent, ReloadEvent } from './types.js';
import { DEFAULT_DEBOUNCE_MS, IGNORE_PATTERNS } from './constants.js';

// ============================================================================
// HotReloader Class
// ============================================================================

/**
 * Hot reloader that watches files for changes and triggers reloads.
 *
 * Events:
 * - 'change': Emitted immediately when a file changes (FileChangeEvent)
 * - 'reload': Emitted after debounce when ready to reload (ReloadEvent)
 *
 * @example
 * ```typescript
 * const reloader = new HotReloader({
 *   paths: ['src/**\/*.ts'],
 *   debounceMs: 300,
 * });
 *
 * reloader.on('change', (event) => console.log(`Changed: ${event.path}`));
 * reloader.on('reload', (event) => console.log(`Reloading: ${event.files.length} files`));
 *
 * await reloader.start();
 * // ... later
 * await reloader.stop();
 * ```
 */
export class HotReloader extends EventEmitter {
  /** Glob patterns for files to watch */
  public readonly paths: string[];
  /** Debounce time in milliseconds */
  public readonly debounceMs: number;

  private _watcher: ReturnType<typeof chokidar.watch> | null = null;
  private _changedFiles: Set<string> = new Set();
  private _debounceTimer: NodeJS.Timeout | null = null;

  constructor(options: HotReloaderOptions) {
    super();
    this.paths = options.paths;
    this.debounceMs = options.debounceMs ?? DEFAULT_DEBOUNCE_MS;
  }

  // --------------------------------------------------------------------------
  // Lifecycle
  // --------------------------------------------------------------------------

  /**
   * Start watching files for changes.
   */
  async start(): Promise<void> {
    this._watcher = chokidar.watch(this.paths, {
      ignored: IGNORE_PATTERNS,
      persistent: true,
      ignoreInitial: true,
    });

    this._watcher.on('change', (filePath: string) => {
      this._handleFileChange(filePath);
    });

    this._watcher.on('add', (filePath: string) => {
      this._handleFileChange(filePath);
    });

    this._watcher.on('unlink', (filePath: string) => {
      this._handleFileChange(filePath);
    });
  }

  /**
   * Stop watching files.
   */
  async stop(): Promise<void> {
    if (this._watcher) {
      await this._watcher.close();
      this._watcher = null;
    }

    if (this._debounceTimer) {
      clearTimeout(this._debounceTimer);
      this._debounceTimer = null;
    }
  }

  // --------------------------------------------------------------------------
  // Dynamic Path Management
  // --------------------------------------------------------------------------

  /**
   * Add a path to watch dynamically.
   *
   * @param watchPath - Glob pattern or path to add
   */
  addPath(watchPath: string): void {
    if (this._watcher) {
      this._watcher.add(watchPath);
    }
    this.paths.push(watchPath);
  }

  /**
   * Remove a path from watching.
   *
   * @param watchPath - Glob pattern or path to remove
   */
  removePath(watchPath: string): void {
    if (this._watcher) {
      this._watcher.unwatch(watchPath);
    }
    const index = this.paths.indexOf(watchPath);
    if (index !== -1) {
      this.paths.splice(index, 1);
    }
  }

  // --------------------------------------------------------------------------
  // Private Methods
  // --------------------------------------------------------------------------

  /**
   * Handle a file change event with debouncing.
   */
  private _handleFileChange(filePath: string): void {
    // Emit change event immediately for logging/UI purposes
    const changeEvent: FileChangeEvent = { path: filePath };
    this.emit('change', changeEvent);

    // Add to changed files set
    this._changedFiles.add(filePath);

    // Reset debounce timer
    if (this._debounceTimer) {
      clearTimeout(this._debounceTimer);
    }

    // Schedule reload after debounce period
    this._debounceTimer = setTimeout(() => {
      const files = Array.from(this._changedFiles);
      this._changedFiles.clear();

      const reloadEvent: ReloadEvent = { files };
      this.emit('reload', reloadEvent);
    }, this.debounceMs);
  }
}
