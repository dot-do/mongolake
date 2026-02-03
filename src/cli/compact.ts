/**
 * MongoLake Compact Command
 *
 * This module re-exports from the modularized compact directory structure.
 * The implementation has been refactored into separate files for better
 * maintainability:
 *
 * - compact/types.ts    - Type definitions
 * - compact/events.ts   - Event emitters
 * - compact/errors.ts   - Error handling
 * - compact/progress.ts - Progress reporting utilities
 * - compact/state.ts    - State management (queues, checkpoints, history)
 * - compact/helpers.ts  - Helper functions (validation, tombstones, optimization)
 * - compact/collections.ts - Collection operations
 * - compact/trigger.ts  - Trigger functions
 * - compact/results.ts  - Result factory functions
 * - compact/index.ts    - Main runCompact function and CLI handler
 *
 * @module cli/compact
 */

// Re-export everything from the compact module
export * from './compact/index.js';
