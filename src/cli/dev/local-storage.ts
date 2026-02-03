/**
 * Local R2 Bucket Simulation
 *
 * Provides a local filesystem-based implementation of R2 bucket operations
 * for development and testing. This is distinct from the main storage
 * abstraction layer and is specifically designed for CLI dev command usage.
 *
 * @module cli/dev/local-storage
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import type { R2LocalBucketOptions, R2ListResult, R2ListOptions } from './types.js';
import { DEFAULT_LIST_LIMIT } from './constants.js';

// ============================================================================
// R2LocalBucket Class
// ============================================================================

/**
 * Local R2 bucket simulation for development.
 *
 * Provides a file-system backed implementation of R2 operations that
 * matches the interface expected by Miniflare and Wrangler local development.
 *
 * @example
 * ```typescript
 * const bucket = new R2LocalBucket({
 *   path: '.mongolake/r2',
 *   bucketName: 'mongolake-local',
 * });
 *
 * await bucket.initialize();
 * await bucket.put('data/doc.json', Buffer.from('{"id": 1}'));
 * const data = await bucket.get('data/doc.json');
 * ```
 */
export class R2LocalBucket {
  /** Path to the bucket storage directory */
  public readonly path: string;
  /** Name of the bucket */
  public readonly bucketName: string;

  constructor(options: R2LocalBucketOptions) {
    this.path = options.path;
    this.bucketName = options.bucketName;
  }

  // --------------------------------------------------------------------------
  // Lifecycle
  // --------------------------------------------------------------------------

  /**
   * Initialize the bucket directory.
   * Creates the storage directory if it doesn't exist.
   */
  async initialize(): Promise<void> {
    if (!fs.existsSync(this.path)) {
      fs.mkdirSync(this.path, { recursive: true });
    }
  }

  // --------------------------------------------------------------------------
  // Object Operations
  // --------------------------------------------------------------------------

  /**
   * Put an object into the bucket.
   *
   * @param key - Object key (path-like string)
   * @param data - Object data as Buffer
   */
  async put(key: string, data: Buffer): Promise<void> {
    const filePath = path.join(this.path, key);
    const dir = path.dirname(filePath);

    // Create subdirectories if needed
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    fs.writeFileSync(filePath, data);
  }

  /**
   * Get an object from the bucket.
   *
   * @param key - Object key
   * @returns Object data or null if not found
   */
  async get(key: string): Promise<Buffer | null> {
    const filePath = path.join(this.path, key);

    if (!fs.existsSync(filePath)) {
      return null;
    }

    return fs.readFileSync(filePath);
  }

  /**
   * Delete an object from the bucket.
   *
   * @param key - Object key
   */
  async delete(key: string): Promise<void> {
    const filePath = path.join(this.path, key);

    if (fs.existsSync(filePath)) {
      fs.rmSync(filePath, { force: true });
    }
  }

  /**
   * List objects in the bucket.
   *
   * @param options - List options (prefix, cursor, limit)
   * @returns List of objects with pagination info
   */
  async list(options?: R2ListOptions): Promise<R2ListResult> {
    const prefix = options?.prefix ?? '';
    const limit = options?.limit ?? DEFAULT_LIST_LIMIT;

    const objects: R2ListResult['objects'] = [];

    // Recursive directory walker
    const walkDir = (dir: string, baseKey: string = ''): void => {
      if (!fs.existsSync(dir)) {
        return;
      }

      const entries = fs.readdirSync(dir, { withFileTypes: true });

      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        const key = baseKey ? `${baseKey}/${entry.name}` : entry.name;

        if (entry.isDirectory()) {
          walkDir(fullPath, key);
        } else if (key.startsWith(prefix)) {
          const stats = fs.statSync(fullPath);
          objects.push({
            key,
            size: stats.size,
            uploaded: stats.mtime,
          });
        }
      }
    };

    walkDir(this.path);

    // Sort by key for consistent ordering
    objects.sort((a, b) => a.key.localeCompare(b.key));

    // Apply pagination
    const truncated = objects.length > limit;
    const result = objects.slice(0, limit);

    return {
      objects: result,
      truncated,
      cursor: truncated ? result[result.length - 1]!.key : undefined,
    };
  }

  /**
   * Clear all objects in the bucket.
   * Removes the entire storage directory.
   */
  async clear(): Promise<void> {
    fs.rmSync(this.path, { recursive: true, force: true });
  }
}
