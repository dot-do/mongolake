/**
 * Cache Configuration Tests
 *
 * Tests for the cache configuration system including:
 * - CacheConfig interface and validation
 * - Global cache configuration manager
 * - Per-collection cache overrides
 * - Dynamic cache resizing at runtime
 * - Memory-based limits
 * - Cache metrics reporting
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
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
  type CacheConfig,
  type ExtendedCacheStats,
} from '../../../src/utils/cache-config.js';
import { LRUCache, createLRUCache } from '../../../src/utils/lru-cache.js';
import { MetricsCollector } from '../../../src/metrics/index.js';

// =============================================================================
// CacheConfig Validation Tests
// =============================================================================

describe('CacheConfig - Validation', () => {
  it('should validate valid configuration', () => {
    const config = validateCacheConfig({
      maxEntries: 1000,
      maxMemoryBytes: 1024 * 1024,
      ttlMs: 60000,
      name: 'test-cache',
    });

    expect(config.maxEntries).toBe(1000);
    expect(config.maxMemoryBytes).toBe(1024 * 1024);
    expect(config.ttlMs).toBe(60000);
    expect(config.name).toBe('test-cache');
  });

  it('should apply defaults for missing fields', () => {
    const config = validateCacheConfig({});

    expect(config.maxEntries).toBe(1000); // DEFAULT_CACHE_MAX_SIZE
    expect(config.metricsEnabled).toBe(true);
    expect(config.name).toBe('default');
  });

  it('should reject maxEntries below minimum', () => {
    expect(() => validateCacheConfig({ maxEntries: 0 })).toThrow(CacheConfigError);
    expect(() => validateCacheConfig({ maxEntries: -1 })).toThrow(CacheConfigError);
  });

  it('should reject maxEntries above maximum', () => {
    expect(() => validateCacheConfig({ maxEntries: MAX_CACHE_SIZE + 1 })).toThrow(CacheConfigError);
  });

  it('should reject non-integer maxEntries', () => {
    expect(() => validateCacheConfig({ maxEntries: 100.5 })).toThrow(CacheConfigError);
  });

  it('should reject maxMemoryBytes below minimum', () => {
    expect(() => validateCacheConfig({ maxMemoryBytes: 100 })).toThrow(CacheConfigError);
  });

  it('should reject maxMemoryBytes above maximum', () => {
    expect(() => validateCacheConfig({ maxMemoryBytes: MAX_MEMORY_BYTES + 1 })).toThrow(CacheConfigError);
  });

  it('should reject ttlMs below minimum', () => {
    expect(() => validateCacheConfig({ ttlMs: 50 })).toThrow(CacheConfigError);
  });

  it('should reject ttlMs above maximum', () => {
    expect(() => validateCacheConfig({ ttlMs: MAX_TTL_MS + 1 })).toThrow(CacheConfigError);
  });

  it('should reject empty name', () => {
    expect(() => validateCacheConfig({ name: '' })).toThrow(CacheConfigError);
  });

  it('should reject name with invalid characters', () => {
    expect(() => validateCacheConfig({ name: 'test cache' })).toThrow(CacheConfigError);
    expect(() => validateCacheConfig({ name: 'test@cache' })).toThrow(CacheConfigError);
  });

  it('should accept valid name with hyphens and underscores', () => {
    const config = validateCacheConfig({ name: 'test-cache_v2' });
    expect(config.name).toBe('test-cache_v2');
  });

  it('should not throw in non-strict mode', () => {
    const config = validateCacheConfig({ maxEntries: -1 }, false);
    expect(config.maxEntries).toBe(-1);
  });
});

// =============================================================================
// CacheConfigManager Tests
// =============================================================================

describe('CacheConfigManager', () => {
  let manager: CacheConfigManager;

  beforeEach(() => {
    manager = new CacheConfigManager();
  });

  describe('Global Defaults', () => {
    it('should provide default configurations for all cache types', () => {
      expect(manager.getConfig('token')).toEqual(DEFAULT_CACHE_CONFIGS.token);
      expect(manager.getConfig('router')).toEqual(DEFAULT_CACHE_CONFIGS.router);
      expect(manager.getConfig('rpc-read')).toEqual(DEFAULT_CACHE_CONFIGS['rpc-read']);
    });

    it('should allow setting global defaults', () => {
      manager.setGlobalDefaults('token', {
        maxEntries: 5000,
        ttlMs: 600000,
      });

      const config = manager.getConfig('token');
      expect(config.maxEntries).toBe(5000);
      expect(config.ttlMs).toBe(600000);
    });

    it('should validate when setting global defaults', () => {
      expect(() => {
        manager.setGlobalDefaults('token', { maxEntries: -1 });
      }).toThrow();
    });
  });

  describe('Per-Collection Overrides', () => {
    it('should apply exact collection override', () => {
      manager.addCollectionOverride('users', {
        maxEntries: 10000,
      });

      const config = manager.getConfig('token', 'users');
      expect(config.maxEntries).toBe(10000);
    });

    it('should apply wildcard collection override', () => {
      manager.addCollectionOverride('logs_*', {
        maxEntries: 500,
        ttlMs: 30000,
      });

      const config = manager.getConfig('token', 'logs_2024');
      expect(config.maxEntries).toBe(500);
      expect(config.ttlMs).toBe(30000);
    });

    it('should not apply non-matching override', () => {
      manager.addCollectionOverride('users', {
        maxEntries: 10000,
      });

      const config = manager.getConfig('token', 'products');
      expect(config.maxEntries).toBe(DEFAULT_CACHE_CONFIGS.token.maxEntries);
    });

    it('should merge override with base config', () => {
      manager.setGlobalDefaults('token', {
        maxEntries: 2000,
        ttlMs: 300000,
      });

      manager.addCollectionOverride('users', {
        maxEntries: 5000,
      });

      const config = manager.getConfig('token', 'users');
      expect(config.maxEntries).toBe(5000);
      expect(config.ttlMs).toBe(300000); // From base config
    });

    it('should remove collection override', () => {
      manager.addCollectionOverride('users', {
        maxEntries: 10000,
      });

      manager.removeCollectionOverride('users');

      const config = manager.getConfig('token', 'users');
      expect(config.maxEntries).toBe(DEFAULT_CACHE_CONFIGS.token.maxEntries);
    });

    it('should list all collection overrides', () => {
      manager.addCollectionOverride('users', { maxEntries: 1000 });
      manager.addCollectionOverride('products', { maxEntries: 2000 });

      const overrides = manager.getCollectionOverrides();
      expect(overrides).toHaveLength(2);
    });
  });

  describe('Configuration Change Listeners', () => {
    it('should notify listeners on config change', () => {
      const listener = vi.fn();
      manager.onConfigChange(listener);

      manager.setGlobalDefaults('token', { maxEntries: 5000 });

      expect(listener).toHaveBeenCalledTimes(1);
      expect(listener).toHaveBeenCalledWith('token', expect.objectContaining({ maxEntries: 5000 }));
    });

    it('should allow unsubscribing from changes', () => {
      const listener = vi.fn();
      const unsubscribe = manager.onConfigChange(listener);

      unsubscribe();
      manager.setGlobalDefaults('token', { maxEntries: 5000 });

      expect(listener).not.toHaveBeenCalled();
    });
  });

  describe('Import/Export', () => {
    it('should export configuration as JSON', () => {
      manager.setGlobalDefaults('token', { maxEntries: 5000 });
      manager.addCollectionOverride('users', { maxEntries: 10000 });

      const json = manager.toJSON() as { defaults: Record<string, CacheConfig>; collectionOverrides: Array<{ pattern: string; config: Partial<CacheConfig> }> };

      expect(json.defaults.token.maxEntries).toBe(5000);
      expect(json.collectionOverrides).toHaveLength(1);
    });

    it('should import configuration from JSON', () => {
      const newManager = new CacheConfigManager();
      newManager.fromJSON({
        defaults: {
          token: { maxEntries: 3000 },
        },
        collectionOverrides: [
          { pattern: 'users', config: { maxEntries: 8000 } },
        ],
      });

      expect(newManager.getConfig('token').maxEntries).toBe(3000);
      expect(newManager.getConfig('token', 'users').maxEntries).toBe(8000);
    });
  });

  describe('Reset', () => {
    it('should reset to default configuration', () => {
      manager.setGlobalDefaults('token', { maxEntries: 5000 });
      manager.addCollectionOverride('users', { maxEntries: 10000 });

      manager.reset();

      expect(manager.getConfig('token')).toEqual(DEFAULT_CACHE_CONFIGS.token);
      expect(manager.getCollectionOverrides()).toHaveLength(0);
    });
  });
});

// =============================================================================
// Global Manager Tests
// =============================================================================

describe('Global CacheConfigManager', () => {
  beforeEach(() => {
    resetCacheConfigManager();
  });

  it('should return singleton instance', () => {
    const manager1 = getCacheConfigManager();
    const manager2 = getCacheConfigManager();

    expect(manager1).toBe(manager2);
  });

  it('should allow setting custom manager', () => {
    const customManager = new CacheConfigManager();
    setCacheConfigManager(customManager);

    expect(getCacheConfigManager()).toBe(customManager);
  });

  it('should reset to new instance', () => {
    const manager1 = getCacheConfigManager();
    resetCacheConfigManager();
    const manager2 = getCacheConfigManager();

    expect(manager1).not.toBe(manager2);
  });
});

// =============================================================================
// LRU Cache - Dynamic Resizing Tests
// =============================================================================

describe('LRUCache - Dynamic Resizing', () => {
  it('should resize to larger size', () => {
    const cache = createLRUCache<string, number>({ maxSize: 3 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    cache.resize(5);

    expect(cache.getMaxSize()).toBe(5);
    expect(cache.size).toBe(3);

    // Should be able to add more entries now
    cache.set('d', 4);
    cache.set('e', 5);
    expect(cache.size).toBe(5);
  });

  it('should resize to smaller size and evict', () => {
    const onEvict = vi.fn();
    const cache = createLRUCache<string, number>({ maxSize: 5, onEvict });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);
    cache.set('d', 4);
    cache.set('e', 5);

    cache.resize(3);

    expect(cache.getMaxSize()).toBe(3);
    expect(cache.size).toBe(3);

    // Oldest entries should be evicted
    expect(cache.has('a')).toBe(false);
    expect(cache.has('b')).toBe(false);
    expect(cache.has('c')).toBe(true);
    expect(cache.has('d')).toBe(true);
    expect(cache.has('e')).toBe(true);

    // Should have been called with 'resize' reason
    expect(onEvict).toHaveBeenCalledWith('a', 1, 'resize');
    expect(onEvict).toHaveBeenCalledWith('b', 2, 'resize');
  });

  it('should track resize evictions in stats', () => {
    const cache = createLRUCache<string, number>({ maxSize: 5 });

    for (let i = 0; i < 5; i++) {
      cache.set(`key${i}`, i);
    }

    cache.resize(2);

    const stats = cache.getStats();
    expect(stats.resizeEvictions).toBe(3);
  });

  it('should throw on invalid resize size', () => {
    const cache = createLRUCache<string, number>({ maxSize: 5 });

    expect(() => cache.resize(0)).toThrow('maxSize must be at least 1');
    expect(() => cache.resize(-1)).toThrow('maxSize must be at least 1');
  });
});

// =============================================================================
// LRU Cache - Memory-Based Limits Tests
// =============================================================================

describe('LRUCache - Memory-Based Limits', () => {
  it('should track memory usage', () => {
    const cache = createLRUCache<string, string>({ maxSize: 100 });

    cache.set('key1', 'value1');
    cache.set('key2', 'value2');

    const stats = cache.getStats();
    expect(stats.memoryBytes).toBeGreaterThan(0);
  });

  it('should evict based on memory limit', () => {
    // Create cache with small memory limit (1KB)
    const cache = createLRUCache<string, string>({
      maxSize: 1000,
      maxMemoryBytes: 1024,
    });

    // Add entries until we exceed memory limit
    for (let i = 0; i < 20; i++) {
      cache.set(`key${i}`, 'x'.repeat(100));
    }

    // Should have evicted some entries due to memory pressure
    expect(cache.size).toBeLessThan(20);
    const stats = cache.getStats();
    expect(stats.memoryEvictions).toBeGreaterThan(0);
    expect(stats.memoryBytes).toBeLessThanOrEqual(1024);
  });

  it('should use custom size estimator', () => {
    const sizeEstimator = vi.fn().mockReturnValue(100);

    const cache = createLRUCache<string, { data: string }>({
      maxSize: 100,
      maxMemoryBytes: 500,
      sizeEstimator,
    });

    cache.set('key1', { data: 'test' });
    cache.set('key2', { data: 'test' });

    expect(sizeEstimator).toHaveBeenCalledTimes(2);
    expect(cache.getMemoryBytes()).toBe(200);
  });

  it('should resize memory limit', () => {
    const cache = createLRUCache<string, string>({
      maxSize: 100,
      maxMemoryBytes: 10000,
    });

    for (let i = 0; i < 10; i++) {
      cache.set(`key${i}`, 'x'.repeat(500));
    }

    // Shrink memory limit
    cache.resizeMemory(2000);

    expect(cache.getMaxMemoryBytes()).toBe(2000);
    expect(cache.getMemoryBytes()).toBeLessThanOrEqual(2000);
  });

  it('should disable memory limit', () => {
    const cache = createLRUCache<string, string>({
      maxSize: 100,
      maxMemoryBytes: 1000,
    });

    cache.set('key1', 'x'.repeat(500));

    cache.resizeMemory(undefined);

    expect(cache.getMaxMemoryBytes()).toBeUndefined();

    // Should be able to add more without memory-based eviction
    for (let i = 0; i < 50; i++) {
      cache.set(`key${i}`, 'x'.repeat(100));
    }

    expect(cache.size).toBe(50);
  });

  it('should report memory usage percentage', () => {
    const cache = createLRUCache<string, string>({
      maxSize: 100,
      maxMemoryBytes: 10000,
    });

    for (let i = 0; i < 5; i++) {
      cache.set(`key${i}`, 'x'.repeat(100));
    }

    const stats = cache.getStats();
    expect(stats.memoryUsagePercent).toBeDefined();
    expect(stats.memoryUsagePercent).toBeGreaterThan(0);
    expect(stats.memoryUsagePercent).toBeLessThanOrEqual(100);
  });

  it('should throw on invalid memory resize', () => {
    const cache = createLRUCache<string, string>({ maxSize: 100 });

    expect(() => cache.resizeMemory(0)).toThrow('maxMemoryBytes must be at least 1');
    expect(() => cache.resizeMemory(-1)).toThrow('maxMemoryBytes must be at least 1');
  });

  it('should track memory on delete', () => {
    const cache = createLRUCache<string, string>({ maxSize: 100 });

    cache.set('key1', 'x'.repeat(100));
    const memoryBefore = cache.getMemoryBytes();

    cache.delete('key1');

    expect(cache.getMemoryBytes()).toBeLessThan(memoryBefore);
  });

  it('should reset memory on clear', () => {
    const cache = createLRUCache<string, string>({ maxSize: 100 });

    for (let i = 0; i < 10; i++) {
      cache.set(`key${i}`, 'x'.repeat(100));
    }

    cache.clear();

    expect(cache.getMemoryBytes()).toBe(0);
  });
});

// =============================================================================
// Memory Size Estimation Tests
// =============================================================================

describe('Memory Size Estimation', () => {
  it('should estimate null/undefined size', () => {
    expect(estimateMemorySize(null)).toBe(8);
    expect(estimateMemorySize(undefined)).toBe(8);
  });

  it('should estimate primitive sizes', () => {
    expect(estimateMemorySize(true)).toBe(4);
    expect(estimateMemorySize(42)).toBe(8);
    expect(estimateMemorySize(BigInt(1000))).toBe(16);
  });

  it('should estimate string size based on length', () => {
    const shortString = 'hello';
    const longString = 'x'.repeat(1000);

    const shortSize = estimateMemorySize(shortString);
    const longSize = estimateMemorySize(longString);

    expect(longSize).toBeGreaterThan(shortSize);
  });

  it('should estimate array size', () => {
    const emptyArray: unknown[] = [];
    const filledArray = [1, 2, 3, 4, 5];

    const emptySize = estimateMemorySize(emptyArray);
    const filledSize = estimateMemorySize(filledArray);

    expect(filledSize).toBeGreaterThan(emptySize);
  });

  it('should estimate object size', () => {
    const smallObj = { a: 1 };
    const largeObj = { a: 1, b: 2, c: 3, d: 4, e: 5, f: 'hello world' };

    const smallSize = estimateMemorySize(smallObj);
    const largeSize = estimateMemorySize(largeObj);

    expect(largeSize).toBeGreaterThan(smallSize);
  });

  it('should estimate Map size', () => {
    const map = new Map<string, number>();
    map.set('a', 1);
    map.set('b', 2);

    const size = estimateMemorySize(map);
    expect(size).toBeGreaterThan(48); // Base overhead
  });

  it('should estimate Set size', () => {
    const set = new Set<number>();
    set.add(1);
    set.add(2);

    const size = estimateMemorySize(set);
    expect(size).toBeGreaterThan(48); // Base overhead
  });

  it('should estimate ArrayBuffer size', () => {
    const buffer = new ArrayBuffer(1024);
    const size = estimateMemorySize(buffer);

    expect(size).toBeGreaterThanOrEqual(1024);
  });

  it('should estimate typed array size', () => {
    const uint8Array = new Uint8Array(100);
    const size = estimateMemorySize(uint8Array);

    expect(size).toBeGreaterThanOrEqual(100);
  });
});

// =============================================================================
// Cache Metrics Reporting Tests
// =============================================================================

describe('Cache Metrics Reporting', () => {
  it('should report cache metrics to collector', () => {
    const metrics = new MetricsCollector();
    const stats: ExtendedCacheStats = {
      size: 100,
      maxSize: 1000,
      memoryBytes: 50000,
      hits: 800,
      misses: 200,
      capacityEvictions: 50,
      ttlEvictions: 10,
      memoryEvictions: 5,
      resizeEvictions: 0,
      hitRate: 80,
      memoryUsagePercent: 50,
      name: 'test-cache',
    };

    reportCacheMetrics(stats, metrics);

    // Verify metrics were recorded
    expect(metrics.getValue('mongolake_cache_entries', { cache_type: 'test-cache' })).toBe(100);
    expect(metrics.getValue('mongolake_cache_size_bytes', { cache_type: 'test-cache' })).toBe(50000);
  });

  it('should create cache metrics summary', () => {
    const caches = new Map<string, ExtendedCacheStats>();

    caches.set('cache1', {
      size: 100,
      maxSize: 1000,
      memoryBytes: 50000,
      hits: 800,
      misses: 200,
      capacityEvictions: 50,
      ttlEvictions: 10,
      memoryEvictions: 0,
      resizeEvictions: 0,
      hitRate: 80,
      memoryUsagePercent: 50,
      name: 'cache1',
    });

    caches.set('cache2', {
      size: 50,
      maxSize: 500,
      memoryBytes: 25000,
      hits: 400,
      misses: 100,
      capacityEvictions: 20,
      ttlEvictions: 5,
      memoryEvictions: 0,
      resizeEvictions: 0,
      hitRate: 80,
      memoryUsagePercent: 50,
      name: 'cache2',
    });

    const summary = createCacheMetricsSummary(caches);

    expect(summary.totalEntries).toBe(150);
    expect(summary.totalMemoryBytes).toBe(75000);
    expect(summary.totalHits).toBe(1200);
    expect(summary.totalMisses).toBe(300);
    expect(summary.averageHitRate).toBe(80);
    expect(summary.caches).toHaveLength(2);
  });

  it('should handle empty cache metrics', () => {
    const summary = createCacheMetricsSummary(new Map());

    expect(summary.totalEntries).toBe(0);
    expect(summary.totalMemoryBytes).toBe(0);
    expect(summary.averageHitRate).toBe(0);
    expect(summary.caches).toHaveLength(0);
  });
});

// =============================================================================
// Extended LRU Cache Stats Tests
// =============================================================================

describe('LRUCache - Extended Stats', () => {
  it('should include all stats fields', () => {
    const cache = createLRUCache<string, number>({
      maxSize: 100,
      maxMemoryBytes: 10000,
      ttlMs: 60000,
      name: 'test-cache',
    });

    cache.set('key1', 1);
    cache.get('key1'); // Hit
    cache.get('nonexistent'); // Miss

    const stats = cache.getStats();

    expect(stats.size).toBe(1);
    expect(stats.maxSize).toBe(100);
    expect(stats.maxMemoryBytes).toBe(10000);
    expect(stats.ttlMs).toBe(60000);
    expect(stats.name).toBe('test-cache');
    expect(stats.hits).toBe(1);
    expect(stats.misses).toBe(1);
    expect(stats.hitRate).toBe(50);
    expect(stats.memoryBytes).toBeGreaterThan(0);
    expect(stats.memoryUsagePercent).toBeDefined();
    expect(stats.capacityEvictions).toBe(0);
    expect(stats.ttlEvictions).toBe(0);
    expect(stats.memoryEvictions).toBe(0);
    expect(stats.resizeEvictions).toBe(0);
  });

  it('should reset all stats', () => {
    const cache = createLRUCache<string, number>({ maxSize: 3 });

    // Generate some stats
    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);
    cache.set('d', 4); // Causes eviction
    cache.get('a'); // Miss
    cache.get('b'); // Hit

    cache.resize(2); // Causes resize eviction

    cache.resetStats();

    const stats = cache.getStats();
    expect(stats.hits).toBe(0);
    expect(stats.misses).toBe(0);
    expect(stats.capacityEvictions).toBe(0);
    expect(stats.memoryEvictions).toBe(0);
    expect(stats.resizeEvictions).toBe(0);
  });
});

// =============================================================================
// Integration with Existing Cache Usage
// =============================================================================

describe('Integration - CacheConfigManager with LRUCache', () => {
  it('should create cache from configuration', () => {
    const manager = new CacheConfigManager();
    manager.setGlobalDefaults('token', {
      maxEntries: 500,
      ttlMs: 120000,
    });

    const config = manager.getConfig('token');
    const cache = createLRUCache<string, string>({
      maxSize: config.maxEntries,
      ttlMs: config.ttlMs,
      name: config.name,
    });

    expect(cache.getMaxSize()).toBe(500);
    expect(cache.getStats().name).toBe('token');
  });

  it('should apply collection-specific configuration', () => {
    const manager = new CacheConfigManager();
    manager.addCollectionOverride('high-traffic-*', {
      maxEntries: 10000,
      maxMemoryBytes: 100 * 1024 * 1024, // 100MB
    });

    const config = manager.getConfig('rpc-read', 'high-traffic-users');

    expect(config.maxEntries).toBe(10000);
    expect(config.maxMemoryBytes).toBe(100 * 1024 * 1024);
  });

  it('should dynamically reconfigure cache on config change', () => {
    const manager = new CacheConfigManager();
    const cache = createLRUCache<string, number>({
      maxSize: manager.getConfig('token').maxEntries,
    });

    // Listen for config changes
    manager.onConfigChange((type, config) => {
      if (type === 'token') {
        cache.resize(config.maxEntries);
      }
    });

    // Populate cache
    for (let i = 0; i < 100; i++) {
      cache.set(`key${i}`, i);
    }

    // Change configuration
    manager.setGlobalDefaults('token', { maxEntries: 50 });

    // Cache should have been resized
    expect(cache.getMaxSize()).toBe(50);
    expect(cache.size).toBe(50);
  });
});
