/**
 * MongoLake Dev Server
 *
 * Local development server that uses Wrangler/Miniflare for Workers simulation.
 * Supports hot reloading, local R2 bucket configuration, and development server
 * lifecycle management.
 *
 * Features:
 * - Wrangler/Miniflare integration for Workers simulation
 * - Hot reloading support via chokidar
 * - Local R2 bucket configuration
 * - Development server lifecycle management
 *
 * @module cli/dev
 *
 * @example
 * ```typescript
 * import { startDevServer, DevServer, createWranglerConfig } from 'mongolake/cli/dev';
 *
 * // Simple usage with CLI entry point
 * await startDevServer({ port: 8080, verbose: true });
 *
 * // Programmatic usage
 * const server = new DevServer({ port: 8080 });
 * server.on('ready', ({ url }) => console.log(`Running at ${url}`));
 * await server.start();
 * ```
 */

// Re-export types
export type {
  DevServerOptions,
  WranglerConfig,
  WranglerConfigOptions,
  MiniflareConfig,
  HotReloaderOptions,
  R2LocalBucketOptions,
  R2ListResult,
  R2ListOptions,
  FileChangeEvent,
  ReloadEvent,
  ServerReadyEvent,
  ServerErrorEvent,
} from './types.js';

// Re-export constants
export {
  DEFAULT_PORT,
  DEFAULT_HOST,
  DEFAULT_PATH,
  DEFAULT_DEBOUNCE_MS,
  DEFAULT_WATCH_PATTERNS,
  DEFAULT_WORKER_NAME,
  DEFAULT_R2_BUCKET_NAME,
  MIN_PORT,
  MAX_PORT,
} from './constants.js';

// Re-export configuration utilities
export { createWranglerConfig, parseEnvFile } from './config.js';

// Re-export classes
export { HotReloader } from './hot-reloader.js';
export { R2LocalBucket } from './local-storage.js';
export {
  DevServer,
  WranglerNotFoundError,
  PortInUseError,
  ServerAlreadyRunningError,
  InvalidPortError,
} from './server.js';

// Import for CLI entry point
import { DevServer } from './server.js';
import type { DevServerOptions } from './types.js';

// ============================================================================
// CLI Entry Point
// ============================================================================

/**
 * Start the development server (CLI entry point).
 *
 * This function provides a convenient entry point for the CLI that handles:
 * - Startup banner output
 * - Signal handler registration (SIGINT, SIGTERM)
 * - Server status output
 *
 * @param options - Server configuration options
 * @returns The running DevServer instance
 *
 * @example
 * ```typescript
 * // Used by CLI to start the server
 * await startDevServer({ port: 8080, verbose: true });
 * ```
 */
export async function startDevServer(options: DevServerOptions): Promise<DevServer> {
  const server = new DevServer(options);

  // Print startup banner
  console.log(`
  MongoLake Development Server

  Starting server...
`);

  // Register signal handlers
  process.on('SIGINT', async () => {
    console.log('\nShutting down...');
    await server.stop();
    process.exit(0);
  });

  process.on('SIGTERM', async () => {
    console.log('\nShutting down...');
    await server.stop();
    process.exit(0);
  });

  await server.start();

  // Print server info
  console.log(`  Server running at http://${server.host}:${server.port}`);
  console.log(`  R2 bucket: ${server.path}/r2`);

  if (server.watch) {
    console.log('  Hot reload enabled');
  }

  console.log(`
  Press Ctrl+C to stop
`);

  return server;
}
