/**
 * Cache Configuration Module for MongoLake
 *
 * Provides comprehensive cache configuration support including:
 * - CacheConfig interface for cache settings
 * - Global cache configuration management
 * - Per-collection cache overrides
 * - Configuration validation
 * - Cache metrics reporting
 *
 * @module utils/cache-config
 */

import { MetricsCollector, METRICS } from '../metrics/index.js';
import {
  DEFAULT_CACHE_MAX_SIZE,
  DEFAULT_CACHE_TTL_SECONDS,
  DEFAULT_ROUTER_CACHE_SIZE,
  DEFAULT_RPC_CACHE_SIZE,
  DEFAULT_RPC_CACHE_TTL_MS,
  DEFAULT_RPC_WRITE_CACHE_SIZE,
  DEFAULT_RPC_WRITE_CACHE_TTL_MS,
  DEFAULT_INDEX_CACHE_SIZE,
  DEFAULT_ZONE_MAP_CACHE_SIZE,
  DEFAULT_QUERY_CACHE_SIZE,
  DEFAULT_QUERY_CACHE_TTL_MS,
  DEFAULT_QUERY_CACHE_MAX_MEMORY_BYTES,
} from '../constants.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Cache types available in MongoLake
 */
export type CacheType =
  | 'token'
  | 'router'
  | 'rpc-read'
  | 'rpc-write'
  | 'index'
  | 'zone-map'
  | 'replica'
  | 'parquet'
  | 'query'
  | 'custom';

/**
 * Configuration for a single cache instance
 */
export interface CacheConfig {
  /** Maximum number of entries in the cache */
  maxEntries: number;
  /** Maximum memory in bytes (optional, for memory-based limits) */
  maxMemoryBytes?: number;
  /** TTL in milliseconds (optional) */
  ttlMs?: number;
  /** Whether to enable metrics collection */
  metricsEnabled?: boolean;
  /** Custom name for metrics labels */
  name?: string;
}

/**
 * Options for creating a cache configuration
 */
export interface CacheConfigOptions {
  /** Maximum number of entries (default varies by cache type) */
  maxEntries?: number;
  /** Maximum memory in bytes */
  maxMemoryBytes?: number;
  /** TTL in milliseconds */
  ttlMs?: number;
  /** Enable metrics collection */
  metricsEnabled?: boolean;
  /** Custom name for metrics labels */
  name?: string;
}

/**
 * Per-collection cache override configuration
 */
export interface CollectionCacheConfig {
  /** Collection name pattern (supports wildcards like "users*") */
  pattern: string;
  /** Cache configuration overrides */
  config: Partial<CacheConfig>;
}

/**
 * Global cache configuration
 */
export interface GlobalCacheConfig {
  /** Default configuration for each cache type */
  defaults: Record<CacheType, CacheConfig>;
  /** Per-collection overrides */
  collectionOverrides: CollectionCacheConfig[];
  /** Global metrics collector */
  metrics?: MetricsCollector;
}

/**
 * Extended cache statistics including memory usage
 */
export interface ExtendedCacheStats {
  /** Current number of entries */
  size: number;
  /** Maximum capacity (entries) */
  maxSize: number;
  /** Estimated memory usage in bytes */
  memoryBytes: number;
  /** Maximum memory limit in bytes (if configured) */
  maxMemoryBytes?: number;
  /** Number of cache hits */
  hits: number;
  /** Number of cache misses */
  misses: number;
  /** Number of entries evicted due to capacity */
  capacityEvictions: number;
  /** Number of entries evicted due to TTL expiration */
  ttlEvictions: number;
  /** Number of entries evicted due to memory pressure */
  memoryEvictions: number;
  /** Hit rate as a percentage (0-100) */
  hitRate: number;
  /** Memory usage as a percentage (0-100) */
  memoryUsagePercent: number;
  /** TTL configured (in ms) */
  ttlMs?: number;
  /** Cache name */
  name: string;
}

/**
 * Validation error for cache configuration
 */
export class CacheConfigError extends Error {
  constructor(message: string, public readonly field?: string) {
    super(message);
    this.name = 'CacheConfigError';
  }
}

// ============================================================================
// Default Configurations
// ============================================================================

/**
 * Default cache configurations by type
 */
export const DEFAULT_CACHE_CONFIGS: Record<CacheType, CacheConfig> = {
  token: {
    maxEntries: DEFAULT_CACHE_MAX_SIZE,
    ttlMs: DEFAULT_CACHE_TTL_SECONDS * 1000,
    metricsEnabled: true,
    name: 'token',
  },
  router: {
    maxEntries: DEFAULT_ROUTER_CACHE_SIZE,
    metricsEnabled: true,
    name: 'router',
  },
  'rpc-read': {
    maxEntries: DEFAULT_RPC_CACHE_SIZE,
    ttlMs: DEFAULT_RPC_CACHE_TTL_MS,
    metricsEnabled: true,
    name: 'rpc-read',
  },
  'rpc-write': {
    maxEntries: DEFAULT_RPC_WRITE_CACHE_SIZE,
    ttlMs: DEFAULT_RPC_WRITE_CACHE_TTL_MS,
    metricsEnabled: true,
    name: 'rpc-write',
  },
  index: {
    maxEntries: DEFAULT_INDEX_CACHE_SIZE,
    metricsEnabled: true,
    name: 'index',
  },
  'zone-map': {
    maxEntries: DEFAULT_ZONE_MAP_CACHE_SIZE,
    metricsEnabled: true,
    name: 'zone-map',
  },
  replica: {
    maxEntries: 100,
    metricsEnabled: true,
    name: 'replica',
  },
  parquet: {
    maxEntries: 100,
    metricsEnabled: true,
    name: 'parquet',
  },
  query: {
    maxEntries: DEFAULT_QUERY_CACHE_SIZE,
    maxMemoryBytes: DEFAULT_QUERY_CACHE_MAX_MEMORY_BYTES,
    ttlMs: DEFAULT_QUERY_CACHE_TTL_MS,
    metricsEnabled: true,
    name: 'query',
  },
  custom: {
    maxEntries: 1000,
    metricsEnabled: false,
    name: 'custom',
  },
};

// ============================================================================
// Validation
// ============================================================================

/** Minimum allowed cache size */
export const MIN_CACHE_SIZE = 1;

/** Maximum allowed cache size */
export const MAX_CACHE_SIZE = 10_000_000;

/** Minimum TTL in milliseconds */
export const MIN_TTL_MS = 100;

/** Maximum TTL in milliseconds (24 hours) */
export const MAX_TTL_MS = 24 * 60 * 60 * 1000;

/** Minimum memory limit in bytes (1KB) */
export const MIN_MEMORY_BYTES = 1024;

/** Maximum memory limit in bytes (10GB) */
export const MAX_MEMORY_BYTES = 10 * 1024 * 1024 * 1024;

/**
 * Validate a cache configuration
 *
 * @param config - Configuration to validate
 * @param strict - Whether to throw on invalid values (default: true)
 * @returns Validated configuration (with defaults applied)
 * @throws CacheConfigError if validation fails in strict mode
 */
export function validateCacheConfig(
  config: CacheConfigOptions,
  strict: boolean = true
): CacheConfig {
  const errors: string[] = [];

  // Validate maxEntries
  if (config.maxEntries !== undefined) {
    if (!Number.isInteger(config.maxEntries)) {
      errors.push('maxEntries must be an integer');
    } else if (config.maxEntries < MIN_CACHE_SIZE) {
      errors.push(`maxEntries must be at least ${MIN_CACHE_SIZE}`);
    } else if (config.maxEntries > MAX_CACHE_SIZE) {
      errors.push(`maxEntries must be at most ${MAX_CACHE_SIZE}`);
    }
  }

  // Validate maxMemoryBytes
  if (config.maxMemoryBytes !== undefined) {
    if (!Number.isInteger(config.maxMemoryBytes)) {
      errors.push('maxMemoryBytes must be an integer');
    } else if (config.maxMemoryBytes < MIN_MEMORY_BYTES) {
      errors.push(`maxMemoryBytes must be at least ${MIN_MEMORY_BYTES}`);
    } else if (config.maxMemoryBytes > MAX_MEMORY_BYTES) {
      errors.push(`maxMemoryBytes must be at most ${MAX_MEMORY_BYTES}`);
    }
  }

  // Validate ttlMs
  if (config.ttlMs !== undefined) {
    if (!Number.isInteger(config.ttlMs)) {
      errors.push('ttlMs must be an integer');
    } else if (config.ttlMs < MIN_TTL_MS) {
      errors.push(`ttlMs must be at least ${MIN_TTL_MS}`);
    } else if (config.ttlMs > MAX_TTL_MS) {
      errors.push(`ttlMs must be at most ${MAX_TTL_MS}`);
    }
  }

  // Validate name
  if (config.name !== undefined) {
    if (typeof config.name !== 'string') {
      errors.push('name must be a string');
    } else if (config.name.length === 0) {
      errors.push('name cannot be empty');
    } else if (config.name.length > 64) {
      errors.push('name must be at most 64 characters');
    } else if (!/^[a-zA-Z0-9_-]+$/.test(config.name)) {
      errors.push('name must contain only alphanumeric characters, underscores, and hyphens');
    }
  }

  if (errors.length > 0 && strict) {
    throw new CacheConfigError(`Invalid cache configuration: ${errors.join('; ')}`);
  }

  // Return validated config with defaults
  return {
    maxEntries: config.maxEntries ?? DEFAULT_CACHE_MAX_SIZE,
    maxMemoryBytes: config.maxMemoryBytes,
    ttlMs: config.ttlMs,
    metricsEnabled: config.metricsEnabled ?? true,
    name: config.name ?? 'default',
  };
}

// ============================================================================
// Cache Configuration Manager
// ============================================================================

/**
 * Global cache configuration manager
 *
 * Manages cache configurations across the application with support for:
 * - Global defaults per cache type
 * - Per-collection overrides
 * - Dynamic configuration updates
 * - Configuration validation
 *
 * @example
 * ```typescript
 * // Get the global manager
 * const manager = getCacheConfigManager();
 *
 * // Configure global defaults
 * manager.setGlobalDefaults('token', {
 *   maxEntries: 5000,
 *   ttlMs: 10 * 60 * 1000, // 10 minutes
 * });
 *
 * // Add per-collection override
 * manager.addCollectionOverride('users*', {
 *   maxEntries: 10000,
 * });
 *
 * // Get configuration for a cache
 * const config = manager.getConfig('token', 'users');
 * ```
 */
export class CacheConfigManager {
  private config: GlobalCacheConfig;
  private listeners: Set<(type: CacheType, config: CacheConfig) => void> = new Set();

  constructor(initialConfig?: Partial<GlobalCacheConfig>) {
    this.config = {
      defaults: { ...DEFAULT_CACHE_CONFIGS },
      collectionOverrides: [],
      metrics: initialConfig?.metrics,
    };

    // Apply any initial configuration
    if (initialConfig?.defaults) {
      for (const [type, config] of Object.entries(initialConfig.defaults) as [CacheType, CacheConfig][]) {
        this.setGlobalDefaults(type, config);
      }
    }

    if (initialConfig?.collectionOverrides) {
      for (const override of initialConfig.collectionOverrides) {
        this.addCollectionOverride(override.pattern, override.config);
      }
    }
  }

  /**
   * Get configuration for a specific cache type and optional collection
   *
   * @param type - Cache type
   * @param collection - Optional collection name for per-collection overrides
   * @returns Merged cache configuration
   */
  getConfig(type: CacheType, collection?: string): CacheConfig {
    // Start with global defaults for this type
    const baseConfig = { ...this.config.defaults[type] };

    // If no collection specified, return base config
    if (!collection) {
      return baseConfig;
    }

    // Check for collection overrides
    for (const override of this.config.collectionOverrides) {
      if (this.matchesPattern(collection, override.pattern)) {
        return {
          ...baseConfig,
          ...override.config,
        };
      }
    }

    return baseConfig;
  }

  /**
   * Set global defaults for a cache type
   *
   * @param type - Cache type
   * @param config - Configuration options
   */
  setGlobalDefaults(type: CacheType, config: Partial<CacheConfig>): void {
    const currentConfig = this.config.defaults[type];
    const newConfig = {
      ...currentConfig,
      ...config,
    };

    // Validate the merged configuration
    validateCacheConfig(newConfig);

    this.config.defaults[type] = newConfig;
    this.notifyListeners(type, newConfig);
  }

  /**
   * Add a per-collection cache override
   *
   * @param pattern - Collection name pattern (supports * wildcard)
   * @param config - Configuration overrides
   */
  addCollectionOverride(pattern: string, config: Partial<CacheConfig>): void {
    // Validate the partial config
    if (config.maxEntries !== undefined) {
      validateCacheConfig({ maxEntries: config.maxEntries });
    }
    if (config.maxMemoryBytes !== undefined) {
      validateCacheConfig({ maxMemoryBytes: config.maxMemoryBytes });
    }
    if (config.ttlMs !== undefined) {
      validateCacheConfig({ ttlMs: config.ttlMs });
    }

    // Remove existing override for this pattern
    this.config.collectionOverrides = this.config.collectionOverrides.filter(
      (o) => o.pattern !== pattern
    );

    // Add new override
    this.config.collectionOverrides.push({ pattern, config });
  }

  /**
   * Remove a per-collection cache override
   *
   * @param pattern - Collection name pattern to remove
   */
  removeCollectionOverride(pattern: string): void {
    this.config.collectionOverrides = this.config.collectionOverrides.filter(
      (o) => o.pattern !== pattern
    );
  }

  /**
   * Get all collection overrides
   */
  getCollectionOverrides(): CollectionCacheConfig[] {
    return [...this.config.collectionOverrides];
  }

  /**
   * Set the metrics collector
   *
   * @param metrics - Metrics collector instance
   */
  setMetrics(metrics: MetricsCollector): void {
    this.config.metrics = metrics;
  }

  /**
   * Get the current metrics collector
   */
  getMetrics(): MetricsCollector | undefined {
    return this.config.metrics;
  }

  /**
   * Register a listener for configuration changes
   *
   * @param listener - Callback invoked when configuration changes
   * @returns Unsubscribe function
   */
  onConfigChange(listener: (type: CacheType, config: CacheConfig) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  /**
   * Get all current configurations
   */
  getAllConfigs(): Record<CacheType, CacheConfig> {
    return { ...this.config.defaults };
  }

  /**
   * Reset all configurations to defaults
   */
  reset(): void {
    this.config = {
      defaults: { ...DEFAULT_CACHE_CONFIGS },
      collectionOverrides: [],
      metrics: this.config.metrics,
    };
  }

  /**
   * Export current configuration as JSON
   */
  toJSON(): object {
    return {
      defaults: this.config.defaults,
      collectionOverrides: this.config.collectionOverrides,
    };
  }

  /**
   * Import configuration from JSON
   *
   * @param json - Configuration object to import
   */
  fromJSON(json: { defaults?: Partial<Record<CacheType, Partial<CacheConfig>>>; collectionOverrides?: CollectionCacheConfig[] }): void {
    if (json.defaults) {
      for (const [type, config] of Object.entries(json.defaults) as [CacheType, Partial<CacheConfig>][]) {
        if (config) {
          this.setGlobalDefaults(type, config);
        }
      }
    }

    if (json.collectionOverrides) {
      this.config.collectionOverrides = [];
      for (const override of json.collectionOverrides) {
        this.addCollectionOverride(override.pattern, override.config);
      }
    }
  }

  /**
   * Check if a collection name matches a pattern
   */
  private matchesPattern(collection: string, pattern: string): boolean {
    // Convert pattern to regex (support * as wildcard)
    const regexPattern = pattern
      .replace(/[.+?^${}()|[\]\\]/g, '\\$&') // Escape special chars except *
      .replace(/\*/g, '.*'); // Convert * to .*
    const regex = new RegExp(`^${regexPattern}$`);
    return regex.test(collection);
  }

  /**
   * Notify listeners of configuration changes
   */
  private notifyListeners(type: CacheType, config: CacheConfig): void {
    for (const listener of this.listeners) {
      try {
        listener(type, config);
      } catch {
        // Ignore listener errors
      }
    }
  }
}

// ============================================================================
// Global Instance
// ============================================================================

let globalCacheConfigManager: CacheConfigManager | null = null;

/**
 * Get the global cache configuration manager
 */
export function getCacheConfigManager(): CacheConfigManager {
  if (!globalCacheConfigManager) {
    globalCacheConfigManager = new CacheConfigManager();
  }
  return globalCacheConfigManager;
}

/**
 * Set the global cache configuration manager
 *
 * @param manager - Manager instance to use globally
 */
export function setCacheConfigManager(manager: CacheConfigManager): void {
  globalCacheConfigManager = manager;
}

/**
 * Reset the global cache configuration manager
 */
export function resetCacheConfigManager(): void {
  globalCacheConfigManager = null;
}

// ============================================================================
// Memory Size Estimation
// ============================================================================

/**
 * Estimate the memory size of a value in bytes
 *
 * This provides a rough estimate for memory-based cache limits.
 * It handles common types like strings, numbers, arrays, and objects.
 *
 * @param value - Value to estimate size of
 * @returns Estimated size in bytes
 */
export function estimateMemorySize(value: unknown): number {
  if (value === null || value === undefined) {
    return 8; // Pointer size
  }

  const type = typeof value;

  if (type === 'boolean') {
    return 4;
  }

  if (type === 'number') {
    return 8;
  }

  if (type === 'string') {
    // 2 bytes per character (UTF-16) + object overhead
    return (value as string).length * 2 + 48;
  }

  if (type === 'bigint') {
    return 16;
  }

  if (type === 'symbol') {
    return 16;
  }

  if (type === 'function') {
    return 64;
  }

  if (value instanceof Date) {
    return 48;
  }

  if (value instanceof RegExp) {
    return 64 + (value as RegExp).source.length * 2;
  }

  if (ArrayBuffer.isView(value)) {
    return (value as ArrayBufferView).byteLength + 16;
  }

  if (value instanceof ArrayBuffer) {
    return value.byteLength + 16;
  }

  if (value instanceof Map) {
    let size = 48;
    for (const [k, v] of value) {
      size += estimateMemorySize(k) + estimateMemorySize(v);
    }
    return size;
  }

  if (value instanceof Set) {
    let size = 48;
    for (const v of value) {
      size += estimateMemorySize(v);
    }
    return size;
  }

  if (Array.isArray(value)) {
    let size = 48 + value.length * 8; // Array overhead + pointers
    for (const item of value) {
      size += estimateMemorySize(item);
    }
    return size;
  }

  if (type === 'object') {
    let size = 48; // Object overhead
    for (const key of Object.keys(value as object)) {
      size += estimateMemorySize(key);
      size += estimateMemorySize((value as Record<string, unknown>)[key]);
    }
    return size;
  }

  return 16; // Default estimate
}

// ============================================================================
// Cache Metrics Reporter
// ============================================================================

/**
 * Report cache metrics to a metrics collector
 *
 * @param stats - Extended cache statistics
 * @param metrics - Metrics collector instance
 */
export function reportCacheMetrics(stats: ExtendedCacheStats, metrics: MetricsCollector): void {
  const labels = { cache_type: stats.name };

  // Report entries count
  metrics.set(METRICS.CACHE_ENTRIES.name, stats.size, labels);

  // Report memory usage
  if (stats.memoryBytes > 0) {
    metrics.set(METRICS.CACHE_SIZE.name, stats.memoryBytes, labels);
  }
}

/**
 * Create a summary of all cache metrics
 *
 * @param caches - Map of cache name to stats
 * @returns Summary object
 */
export function createCacheMetricsSummary(
  caches: Map<string, ExtendedCacheStats>
): {
  totalEntries: number;
  totalMemoryBytes: number;
  totalHits: number;
  totalMisses: number;
  averageHitRate: number;
  caches: ExtendedCacheStats[];
} {
  let totalEntries = 0;
  let totalMemoryBytes = 0;
  let totalHits = 0;
  let totalMisses = 0;
  const allStats: ExtendedCacheStats[] = [];

  for (const stats of caches.values()) {
    totalEntries += stats.size;
    totalMemoryBytes += stats.memoryBytes;
    totalHits += stats.hits;
    totalMisses += stats.misses;
    allStats.push(stats);
  }

  const total = totalHits + totalMisses;
  const averageHitRate = total > 0 ? (totalHits / total) * 100 : 0;

  return {
    totalEntries,
    totalMemoryBytes,
    totalHits,
    totalMisses,
    averageHitRate: Math.round(averageHitRate * 100) / 100,
    caches: allStats,
  };
}
