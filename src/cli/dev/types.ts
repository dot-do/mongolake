/**
 * Type definitions for MongoLake Dev Server
 *
 * Centralized type definitions used across the dev command modules.
 *
 * @module cli/dev/types
 */

// ============================================================================
// Server Options
// ============================================================================

/**
 * Options for configuring the development server.
 */
export interface DevServerOptions {
  /** Port to listen on (default: 3456) */
  port?: number;
  /** Host to bind to (default: 'localhost') */
  host?: string;
  /** Path to data directory (default: '.mongolake') */
  path?: string;
  /** Enable verbose logging */
  verbose?: boolean;
  /** Enable CORS headers (default: true) */
  cors?: boolean;
  /** Enable file watching and hot reload (default: true) */
  watch?: boolean;
  /** Use Miniflare instead of Wrangler */
  miniflare?: boolean;
  /** Number of retry attempts for transient errors */
  retries?: number;
  /** Environment variables to pass to the server */
  env?: Record<string, string>;
  /** Custom Wrangler configuration overrides */
  wranglerConfig?: Partial<WranglerConfig>;
  /** Custom R2 bucket name */
  r2BucketName?: string;
}

// ============================================================================
// Wrangler Configuration
// ============================================================================

/**
 * Options for creating a Wrangler configuration.
 */
export interface WranglerConfigOptions {
  /** Name of the worker */
  name: string;
  /** Port for the dev server */
  port: number;
  /** Path to data directory */
  path: string;
  /** Environment variables to include in config */
  vars?: Record<string, string>;
}

/**
 * Wrangler configuration object with TOML serialization.
 */
export interface WranglerConfig {
  /** Worker name */
  name: string;
  /** Main entry point */
  main: string;
  /** Compatibility date for Workers runtime */
  compatibility_date: string;
  /** Compatibility flags for Workers runtime */
  compatibility_flags?: string[];
  /** R2 bucket bindings */
  r2_buckets: Array<{
    binding: string;
    bucket_name: string;
    preview_bucket_name?: string;
  }>;
  /** Durable Objects bindings */
  durable_objects: {
    bindings: Array<{
      name: string;
      class_name: string;
    }>;
  };
  /** Miniflare persistence configuration */
  miniflare?: {
    d1_persist: string;
    kv_persist: string;
    r2_persist: string;
  };
  /** Dev server configuration */
  dev: {
    port: number;
    local: boolean;
  };
  /** Environment variables */
  vars?: Record<string, string>;
  /** Serialize configuration to TOML format */
  toTOML(): string;
}

/**
 * Miniflare-specific configuration.
 */
export interface MiniflareConfig {
  /** R2 bucket names */
  r2Buckets: string[];
  /** Durable Object bindings */
  durableObjects: Array<{
    name: string;
    className: string;
  }>;
  /** Path for persistent storage */
  persistTo: string;
  /** Enable live reload */
  liveReload: boolean;
}

// ============================================================================
// Hot Reloader
// ============================================================================

/**
 * Options for the hot reloader.
 */
export interface HotReloaderOptions {
  /** Glob patterns for files to watch */
  paths: string[];
  /** Debounce time in milliseconds (default: 300) */
  debounceMs?: number;
}

/**
 * Event emitted when a file changes.
 */
export interface FileChangeEvent {
  /** Path to the changed file */
  path: string;
}

/**
 * Event emitted when a reload is triggered.
 */
export interface ReloadEvent {
  /** List of files that changed */
  files: string[];
}

// ============================================================================
// Local R2 Storage
// ============================================================================

/**
 * Options for local R2 bucket simulation.
 */
export interface R2LocalBucketOptions {
  /** Path to storage directory */
  path: string;
  /** Name of the bucket */
  bucketName: string;
}

/**
 * Result of listing objects in the bucket.
 */
export interface R2ListResult {
  /** List of objects matching the query */
  objects: Array<{
    key: string;
    size: number;
    uploaded: Date;
  }>;
  /** Whether there are more results */
  truncated: boolean;
  /** Cursor for pagination */
  cursor?: string;
}

/**
 * Options for listing objects.
 */
export interface R2ListOptions {
  /** Filter by key prefix */
  prefix?: string;
  /** Pagination cursor */
  cursor?: string;
  /** Maximum number of results */
  limit?: number;
}

// ============================================================================
// Server Events
// ============================================================================

/**
 * Event emitted when the server is ready.
 */
export interface ServerReadyEvent {
  /** URL where the server is listening */
  url: string;
  /** Runtime being used (wrangler or miniflare) */
  runtime: 'wrangler' | 'miniflare';
}

/**
 * Event emitted when the server encounters an error.
 */
export interface ServerErrorEvent {
  /** Exit code from the process */
  code: number;
}
