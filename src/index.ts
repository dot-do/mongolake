/**
 * MongoLake Worker Entry Point
 *
 * Exports the Worker fetch handler and Durable Object classes
 * for Cloudflare Workers runtime.
 *
 * ## Portability
 *
 * MongoLake can be deployed on platforms other than Cloudflare Workers
 * by implementing the abstraction interfaces. See the abstractions module:
 *
 * ```typescript
 * import {
 *   type ObjectStorageBackend,
 *   type CoordinationBackend,
 *   type CacheBackend,
 * } from 'mongolake/abstractions';
 * ```
 */

// Re-export Worker handler
export { MongoLakeWorker, type MongoLakeEnv, type RequestContext } from './worker/index.js';

// Re-export Durable Object classes
export { ShardDO } from './do/shard.js';

// Re-export abstraction interfaces and implementations
export * from './abstractions/index.js';

// Re-export error classes and utilities
export {
  // Base error
  MongoLakeError,
  // Error subclasses
  ValidationError,
  StorageError,
  InvalidStorageKeyError,
  AuthenticationError,
  QueryError,
  RPCError,
  TransientError,
  ShardUnavailableError,
  SchemaError,
  TransactionError,
  ParquetError,
  BranchError,
  // Error codes
  ErrorCodes,
  // Helper functions
  hasErrorCode,
  isRetryableError,
  wrapError,
  // Types
  type ErrorCode,
} from './errors/index.js';

// Re-export plugin system
export {
  // Core plugin types and classes
  PluginRegistry,
  definePlugin,
  composePlugins,
  createPluginLogger,
  getGlobalRegistry,
  setGlobalRegistry,
  resetGlobalRegistry,
  // Types
  type Plugin,
  type PluginMetadata,
  type PluginContext,
  type PluginLogger,
  type PluginRegistryReadOnly,
  type PluginHooks,
  type PluginDefinition,
  type HookContext,
  type CollectionHookContext,
  type HookResult,
  type HookName,
  type CollectionHooks,
  type ClientHooks,
  type StorageHooks,
  type AuthHooks,
} from './plugin/index.js';

// Default export for Cloudflare Workers
import workerHandler from './worker/index.js';
import { ShardDO } from './do/shard.js';

export default {
  ...workerHandler,
  // Durable Objects must be exported from the main entry point
  ShardDO,
};
