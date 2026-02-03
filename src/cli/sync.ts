/**
 * MongoLake Sync Commands
 *
 * This module re-exports the refactored sync functionality from ./sync/
 * for backwards compatibility.
 *
 * The sync implementation has been refactored into modular components:
 * - ./sync/types.ts - Type definitions
 * - ./sync/remote-client.ts - HTTP client for remote communication
 * - ./sync/diff-engine.ts - Sync diff computation
 * - ./sync/conflict-resolver.ts - Conflict detection and resolution
 * - ./sync/progress.ts - Progress tracking and reporting
 * - ./sync/state-manager.ts - Local state, manifests, and history
 * - ./sync/transfer.ts - Bandwidth, retry, chunked transfers
 * - ./sync/filters.ts - Include/exclude filtering
 * - ./sync/error-handling.ts - Error handling and rollback
 * - ./sync/index.ts - Main push/pull implementations
 */

// Re-export everything from the modular sync implementation
export * from './sync/index.js';
