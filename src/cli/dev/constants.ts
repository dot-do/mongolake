/**
 * Constants for MongoLake Dev Server
 *
 * Centralized constants for configuration defaults and magic values.
 *
 * @module cli/dev/constants
 */

// ============================================================================
// Server Defaults
// ============================================================================

/** Default port for the development server */
export const DEFAULT_PORT = 3456;

/** Default host to bind to */
export const DEFAULT_HOST = 'localhost';

/** Default path for local data storage */
export const DEFAULT_PATH = '.mongolake';

// ============================================================================
// Wrangler Defaults
// ============================================================================

/** Default worker name for development */
export const DEFAULT_WORKER_NAME = 'mongolake-dev';

/** Default main entry point for the worker */
export const DEFAULT_WORKER_MAIN = 'src/worker/index.ts';

/** Default R2 bucket name for local development */
export const DEFAULT_R2_BUCKET_NAME = 'mongolake-local';

/** Default R2 binding name */
export const DEFAULT_R2_BINDING = 'BUCKET';

/** Default Durable Object binding name */
export const DEFAULT_DO_BINDING = 'SHARD';

/** Default Durable Object class name */
export const DEFAULT_DO_CLASS = 'ShardDO';

// ============================================================================
// Hot Reload Defaults
// ============================================================================

/** Default debounce time for file changes in milliseconds */
export const DEFAULT_DEBOUNCE_MS = 300;

/** Default watch patterns for hot reload */
export const DEFAULT_WATCH_PATTERNS = ['src/**/*.ts', 'wrangler.toml'];

/** Patterns to ignore when watching files */
export const IGNORE_PATTERNS = ['**/node_modules/**', '**/dist/**', '**/.git/**'];

// ============================================================================
// Process Management
// ============================================================================

/** Minimum valid port number */
export const MIN_PORT = 1;

/** Maximum valid port number */
export const MAX_PORT = 65535;

/** Timeout for auto-resolving process start (ms) */
export const PROCESS_START_TIMEOUT_MS = 10;

/** Delay between retry attempts (ms) */
export const RETRY_DELAY_MS = 100;

// ============================================================================
// Local R2 Defaults
// ============================================================================

/** Default limit for listing objects */
export const DEFAULT_LIST_LIMIT = 1000;
