/**
 * MongoLake Dev Server (Wrangler/Miniflare-based)
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

// Re-export everything from the dev module
export * from './dev/index.js';
