/**
 * Utility modules for MongoLake
 *
 * Re-exports all shared utilities for external consumption:
 * - Filter matching for MongoDB-style queries
 * - Document sorting with MongoDB sort specification
 * - Update operator application
 * - Nested value access with dot notation
 * - MongoDB-style projection
 * - Database/collection name validation
 * - LRU cache for bounded caching with optional TTL
 * - Error sanitization to prevent credential leakage in logs
 *
 * @module utils
 */

export { matchesFilter } from './filter.js';
export { sortDocuments, type Sort } from './sort.js';
export { applyUpdate, extractFilterFields } from './update.js';
export { getNestedValue } from './nested.js';
export { applyProjection } from './projection.js';
export {
  validateCollectionName,
  validateDatabaseName,
  validateFieldName,
  validateFilter,
  validateProjection,
  validateUpdate,
  validateDocument,
  validateAggregationPipeline,
  validateInputs,
  ValidationError,
  VALID_QUERY_OPERATORS,
  VALID_UPDATE_OPERATORS,
  VALID_AGGREGATION_STAGES,
  MAX_NAME_LENGTH,
  RESERVED_DATABASE_NAMES,
  SYSTEM_COLLECTION_PREFIX,
  type FilterValidationOptions,
  type UpdateValidationOptions,
} from './validation.js';
export {
  LRUCache,
  createLRUCache,
  type LRUCacheOptions,
  type LRUCacheStats,
} from './lru-cache.js';
export {
  CacheConfigManager,
  getCacheConfigManager,
  setCacheConfigManager,
  resetCacheConfigManager,
  validateCacheConfig,
  estimateMemorySize,
  reportCacheMetrics,
  createCacheMetricsSummary,
  CacheConfigError,
  DEFAULT_CACHE_CONFIGS,
  MIN_CACHE_SIZE,
  MAX_CACHE_SIZE,
  MIN_TTL_MS,
  MAX_TTL_MS,
  MIN_MEMORY_BYTES,
  MAX_MEMORY_BYTES,
  type CacheType,
  type CacheConfig,
  type CacheConfigOptions,
  type CollectionCacheConfig,
  type GlobalCacheConfig,
  type ExtendedCacheStats,
} from './cache-config.js';
export {
  sanitizeError,
  sanitizeMessage,
  sanitizeConfig,
  createSafeErrorMessage,
  looksLikeCredential,
  type SanitizeOptions,
  type SanitizedError,
} from './sanitize-error.js';
export {
  parseConnectionString,
  buildConnectionString,
  isConnectionString,
  ConnectionStringParseError,
  type ParsedConnectionString,
  type ConnectionOptions,
  type HostInfo,
} from './connection-string.js';
export {
  Logger,
  logger,
  createLogger,
  debug,
  info,
  warn,
  error,
  setRequestId,
  getRequestId,
  clearRequestId,
  generateRequestId,
  withRequestId,
  type LogLevel,
  type LogContext,
  type LogEntry,
  type LoggerConfig,
  type LoggerOutput,
} from './logger.js';
