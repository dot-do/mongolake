/**
 * Query Result Cache Tests
 *
 * Tests for the query result cache implementation covering:
 * - Cache key generation from query parameters
 * - TTL-based expiration
 * - Size limits (entry count and memory)
 * - Cache invalidation on writes
 * - Cache statistics
 * - QueryCacheManager for multi-collection caching
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  QueryResultCache,
  QueryCacheManager,
  generateCacheKey,
  createQueryCache,
  createQueryCacheManager,
  type QueryCacheOptions,
} from '../../../src/query/result-cache.js';
import type { Document, WithId, Filter, FindOptions } from '../../../src/types.js';

// =============================================================================
// Cache Key Generation
// =============================================================================

describe('generateCacheKey', () => {
  it('should generate consistent keys for identical queries', () => {
    const filter = { name: 'John', age: 30 };
    const options: FindOptions = { sort: { name: 1 }, limit: 10 };

    const key1 = generateCacheKey(filter, options);
    const key2 = generateCacheKey(filter, options);

    expect(key1).toBe(key2);
  });

  it('should generate consistent keys regardless of object key order', () => {
    const filter1 = { name: 'John', age: 30 };
    const filter2 = { age: 30, name: 'John' };

    const key1 = generateCacheKey(filter1);
    const key2 = generateCacheKey(filter2);

    expect(key1).toBe(key2);
  });

  it('should generate different keys for different filters', () => {
    const filter1 = { name: 'John' };
    const filter2 = { name: 'Jane' };

    const key1 = generateCacheKey(filter1);
    const key2 = generateCacheKey(filter2);

    expect(key1).not.toBe(key2);
  });

  it('should generate different keys for different options', () => {
    const filter = { name: 'John' };
    const options1: FindOptions = { limit: 10 };
    const options2: FindOptions = { limit: 20 };

    const key1 = generateCacheKey(filter, options1);
    const key2 = generateCacheKey(filter, options2);

    expect(key1).not.toBe(key2);
  });

  it('should handle empty filter and options', () => {
    const key1 = generateCacheKey();
    const key2 = generateCacheKey({});
    const key3 = generateCacheKey({}, {});

    expect(key1).toBe(key2);
    expect(key2).toBe(key3);
  });

  it('should handle nested objects in filters', () => {
    const filter1 = { address: { city: 'NYC', zip: '10001' } };
    const filter2 = { address: { zip: '10001', city: 'NYC' } };

    const key1 = generateCacheKey(filter1);
    const key2 = generateCacheKey(filter2);

    expect(key1).toBe(key2);
  });

  it('should handle arrays in filters', () => {
    const filter = { tags: { $in: ['a', 'b', 'c'] } };

    const key1 = generateCacheKey(filter);
    const key2 = generateCacheKey(filter);

    expect(key1).toBe(key2);
  });

  it('should include projection in cache key', () => {
    const filter = { name: 'John' };
    const options1: FindOptions = { projection: { name: 1 } };
    const options2: FindOptions = { projection: { name: 1, age: 1 } };

    const key1 = generateCacheKey(filter, options1);
    const key2 = generateCacheKey(filter, options2);

    expect(key1).not.toBe(key2);
  });

  it('should include skip in cache key', () => {
    const filter = { name: 'John' };
    const options1: FindOptions = { skip: 0 };
    const options2: FindOptions = { skip: 10 };

    const key1 = generateCacheKey(filter, options1);
    const key2 = generateCacheKey(filter, options2);

    expect(key1).not.toBe(key2);
  });
});

// =============================================================================
// QueryResultCache - Basic Operations
// =============================================================================

describe('QueryResultCache - Basic Operations', () => {
  let cache: QueryResultCache<Document>;

  beforeEach(() => {
    cache = new QueryResultCache('test_collection');
  });

  it('should store and retrieve query results', () => {
    const filter = { name: 'John' };
    const results: WithId<Document>[] = [
      { _id: '1', name: 'John', age: 30 },
      { _id: '2', name: 'John', age: 25 },
    ];

    cache.set(filter, undefined, results);
    const cached = cache.get(filter);

    expect(cached).toEqual(results);
  });

  it('should return undefined for cache miss', () => {
    const filter = { name: 'Jane' };
    const cached = cache.get(filter);

    expect(cached).toBeUndefined();
  });

  it('should store results with options', () => {
    const filter = { status: 'active' };
    const options: FindOptions = { sort: { name: 1 }, limit: 10 };
    const results: WithId<Document>[] = [{ _id: '1', status: 'active' }];

    cache.set(filter, options, results);
    const cached = cache.get(filter, options);

    expect(cached).toEqual(results);
  });

  it('should miss cache with different options', () => {
    const filter = { status: 'active' };
    const options1: FindOptions = { limit: 10 };
    const options2: FindOptions = { limit: 20 };
    const results: WithId<Document>[] = [{ _id: '1', status: 'active' }];

    cache.set(filter, options1, results);
    const cached = cache.get(filter, options2);

    expect(cached).toBeUndefined();
  });

  it('should check if query is cached with has()', () => {
    const filter = { name: 'John' };
    const results: WithId<Document>[] = [{ _id: '1', name: 'John' }];

    expect(cache.has(filter)).toBe(false);

    cache.set(filter, undefined, results);

    expect(cache.has(filter)).toBe(true);
  });

  it('should invalidate a specific query', () => {
    const filter = { name: 'John' };
    const results: WithId<Document>[] = [{ _id: '1', name: 'John' }];

    cache.set(filter, undefined, results);
    expect(cache.has(filter)).toBe(true);

    cache.invalidateQuery(filter);
    expect(cache.has(filter)).toBe(false);
  });

  it('should invalidate all queries', () => {
    const filter1 = { name: 'John' };
    const filter2 = { name: 'Jane' };
    const results: WithId<Document>[] = [{ _id: '1', name: 'John' }];

    cache.set(filter1, undefined, results);
    cache.set(filter2, undefined, results);

    expect(cache.size).toBe(2);

    cache.invalidate();

    expect(cache.size).toBe(0);
    expect(cache.has(filter1)).toBe(false);
    expect(cache.has(filter2)).toBe(false);
  });

  it('should return a copy of results to prevent mutation', () => {
    const filter = { name: 'John' };
    const results: WithId<Document>[] = [{ _id: '1', name: 'John' }];

    cache.set(filter, undefined, results);
    const cached1 = cache.get(filter);
    const cached2 = cache.get(filter);

    // Should be different array instances
    expect(cached1).not.toBe(cached2);
    expect(cached1).toEqual(cached2);
  });
});

// =============================================================================
// QueryResultCache - TTL Expiration
// =============================================================================

describe('QueryResultCache - TTL Expiration', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should return cached results within TTL', () => {
    const cache = new QueryResultCache('test', { ttlMs: 5000 });
    const filter = { name: 'John' };
    const results: WithId<Document>[] = [{ _id: '1', name: 'John' }];

    cache.set(filter, undefined, results);

    // Advance time but stay within TTL
    vi.advanceTimersByTime(3000);

    const cached = cache.get(filter);
    expect(cached).toEqual(results);
  });

  it('should return undefined after TTL expires', () => {
    const cache = new QueryResultCache('test', { ttlMs: 5000 });
    const filter = { name: 'John' };
    const results: WithId<Document>[] = [{ _id: '1', name: 'John' }];

    cache.set(filter, undefined, results);

    // Advance time past TTL
    vi.advanceTimersByTime(6000);

    const cached = cache.get(filter);
    expect(cached).toBeUndefined();
  });

  it('should prune expired entries', () => {
    const cache = new QueryResultCache('test', { ttlMs: 5000 });
    const results: WithId<Document>[] = [{ _id: '1', name: 'John' }];

    cache.set({ a: 1 }, undefined, results);
    cache.set({ a: 2 }, undefined, results);

    // Advance time past TTL
    vi.advanceTimersByTime(6000);

    const pruned = cache.prune();
    expect(pruned).toBe(2);
    expect(cache.size).toBe(0);
  });
});

// =============================================================================
// QueryResultCache - Size Limits
// =============================================================================

describe('QueryResultCache - Size Limits', () => {
  it('should evict LRU entries when maxEntries is exceeded', () => {
    const cache = new QueryResultCache('test', { maxEntries: 3 });
    const results: WithId<Document>[] = [{ _id: '1' }];

    cache.set({ a: 1 }, undefined, results);
    cache.set({ a: 2 }, undefined, results);
    cache.set({ a: 3 }, undefined, results);

    // All should be cached
    expect(cache.size).toBe(3);

    // Add one more - should evict { a: 1 }
    cache.set({ a: 4 }, undefined, results);

    expect(cache.size).toBe(3);
    expect(cache.has({ a: 1 })).toBe(false);
    expect(cache.has({ a: 2 })).toBe(true);
    expect(cache.has({ a: 3 })).toBe(true);
    expect(cache.has({ a: 4 })).toBe(true);
  });

  it('should not cache very large result sets', () => {
    const cache = new QueryResultCache('test');
    const filter = { name: 'John' };

    // Create a result set with 11000 documents (over the 10000 limit)
    const results: WithId<Document>[] = [];
    for (let i = 0; i < 11000; i++) {
      results.push({ _id: String(i), name: 'John' });
    }

    cache.set(filter, undefined, results);

    // Should not be cached due to size
    expect(cache.has(filter)).toBe(false);
  });

  it('should respect memory limits', () => {
    // Create cache with very small memory limit
    const cache = new QueryResultCache('test', {
      maxMemoryBytes: 1000,
      maxEntries: 1000, // High entry limit to test memory eviction
    });

    const results: WithId<Document>[] = [
      { _id: '1', data: 'x'.repeat(200) },
    ];

    // Add entries until memory limit is reached
    for (let i = 0; i < 10; i++) {
      cache.set({ a: i }, undefined, results);
    }

    // Should have evicted some entries due to memory pressure
    expect(cache.size).toBeLessThan(10);
  });
});

// =============================================================================
// QueryResultCache - Enable/Disable
// =============================================================================

describe('QueryResultCache - Enable/Disable', () => {
  it('should not cache when disabled', () => {
    const cache = new QueryResultCache('test', { enabled: false });
    const filter = { name: 'John' };
    const results: WithId<Document>[] = [{ _id: '1', name: 'John' }];

    cache.set(filter, undefined, results);
    const cached = cache.get(filter);

    expect(cached).toBeUndefined();
    expect(cache.size).toBe(0);
  });

  it('should allow enabling/disabling at runtime', () => {
    const cache = new QueryResultCache('test');
    const filter = { name: 'John' };
    const results: WithId<Document>[] = [{ _id: '1', name: 'John' }];

    // Initially enabled
    expect(cache.isEnabled()).toBe(true);

    cache.set(filter, undefined, results);
    expect(cache.get(filter)).toEqual(results);

    // Disable - should clear cache
    cache.setEnabled(false);
    expect(cache.isEnabled()).toBe(false);
    expect(cache.get(filter)).toBeUndefined();

    // Re-enable
    cache.setEnabled(true);
    expect(cache.isEnabled()).toBe(true);

    // Can cache again
    cache.set(filter, undefined, results);
    expect(cache.get(filter)).toEqual(results);
  });
});

// =============================================================================
// QueryResultCache - Statistics
// =============================================================================

describe('QueryResultCache - Statistics', () => {
  it('should track cache hits and misses', () => {
    const cache = new QueryResultCache('test');
    const filter = { name: 'John' };
    const results: WithId<Document>[] = [{ _id: '1', name: 'John' }];

    // Miss
    cache.get(filter);

    // Set and hit
    cache.set(filter, undefined, results);
    cache.get(filter);
    cache.get(filter);

    // Another miss
    cache.get({ name: 'Jane' });

    const stats = cache.getStats();
    expect(stats.hits).toBe(2);
    expect(stats.misses).toBe(2);
    expect(stats.hitRate).toBe(50);
  });

  it('should track invalidations', () => {
    const cache = new QueryResultCache('test');
    const results: WithId<Document>[] = [{ _id: '1' }];

    cache.set({ a: 1 }, undefined, results);
    cache.set({ a: 2 }, undefined, results);

    cache.invalidate();
    cache.invalidate();

    const stats = cache.getStats();
    expect(stats.invalidations).toBe(2);
  });

  it('should reset statistics', () => {
    const cache = new QueryResultCache('test');
    const results: WithId<Document>[] = [{ _id: '1' }];

    cache.set({ a: 1 }, undefined, results);
    cache.get({ a: 1 });
    cache.get({ a: 2 });
    cache.invalidate();

    cache.resetStats();

    const stats = cache.getStats();
    expect(stats.hits).toBe(0);
    expect(stats.misses).toBe(0);
    expect(stats.invalidations).toBe(0);
  });

  it('should report memory usage', () => {
    const cache = new QueryResultCache('test');
    const results: WithId<Document>[] = [
      { _id: '1', data: 'x'.repeat(1000) },
    ];

    cache.set({ a: 1 }, undefined, results);

    const stats = cache.getStats();
    expect(stats.memoryBytes).toBeGreaterThan(0);
  });
});

// =============================================================================
// QueryCacheManager
// =============================================================================

describe('QueryCacheManager', () => {
  it('should create and manage caches for multiple collections', () => {
    const manager = new QueryCacheManager();

    const usersCache = manager.getCache('users');
    const ordersCache = manager.getCache('orders');

    expect(usersCache).toBeInstanceOf(QueryResultCache);
    expect(ordersCache).toBeInstanceOf(QueryResultCache);
    expect(usersCache).not.toBe(ordersCache);
  });

  it('should return same cache instance for same collection', () => {
    const manager = new QueryCacheManager();

    const cache1 = manager.getCache('users');
    const cache2 = manager.getCache('users');

    expect(cache1).toBe(cache2);
  });

  it('should invalidate all caches', () => {
    const manager = new QueryCacheManager();
    const results: WithId<Document>[] = [{ _id: '1' }];

    const usersCache = manager.getCache('users');
    const ordersCache = manager.getCache('orders');

    usersCache.set({ a: 1 }, undefined, results);
    ordersCache.set({ b: 1 }, undefined, results);

    manager.invalidateAll();

    expect(usersCache.size).toBe(0);
    expect(ordersCache.size).toBe(0);
  });

  it('should get combined statistics', () => {
    const manager = new QueryCacheManager();
    const results: WithId<Document>[] = [{ _id: '1' }];

    const usersCache = manager.getCache('users');
    const ordersCache = manager.getCache('orders');

    // Users: 2 hits, 1 miss
    usersCache.set({ a: 1 }, undefined, results);
    usersCache.get({ a: 1 });
    usersCache.get({ a: 1 });
    usersCache.get({ a: 2 });

    // Orders: 1 hit, 2 misses
    ordersCache.set({ b: 1 }, undefined, results);
    ordersCache.get({ b: 1 });
    ordersCache.get({ b: 2 });
    ordersCache.get({ b: 3 });

    const stats = manager.getCombinedStats();

    expect(stats.totalEntries).toBe(2);
    expect(stats.totalHits).toBe(3);
    expect(stats.totalMisses).toBe(3);
    expect(stats.overallHitRate).toBe(50);
    expect(stats.cacheCount).toBe(2);
  });

  it('should remove a cache', () => {
    const manager = new QueryCacheManager();

    manager.getCache('users');
    expect(manager.hasCache('users')).toBe(true);

    manager.removeCache('users');
    expect(manager.hasCache('users')).toBe(false);
  });

  it('should enable/disable all caches', () => {
    const manager = new QueryCacheManager();
    const results: WithId<Document>[] = [{ _id: '1' }];

    const usersCache = manager.getCache('users');
    const ordersCache = manager.getCache('orders');

    manager.setEnabledAll(false);

    usersCache.set({ a: 1 }, undefined, results);
    ordersCache.set({ b: 1 }, undefined, results);

    expect(usersCache.get({ a: 1 })).toBeUndefined();
    expect(ordersCache.get({ b: 1 })).toBeUndefined();
  });

  it('should apply global options to new caches', () => {
    const manager = new QueryCacheManager({ maxEntries: 5 });

    const cache = manager.getCache('users');
    const results: WithId<Document>[] = [{ _id: '1' }];

    // Fill cache beyond limit
    for (let i = 0; i < 10; i++) {
      cache.set({ a: i }, undefined, results);
    }

    // Should be limited to 5 entries
    expect(cache.size).toBe(5);
  });
});

// =============================================================================
// Factory Functions
// =============================================================================

describe('Factory Functions', () => {
  it('should create query cache with createQueryCache', () => {
    const cache = createQueryCache('test', { maxEntries: 100 });

    expect(cache).toBeInstanceOf(QueryResultCache);
    expect(cache.getCollectionName()).toBe('test');
  });

  it('should create cache manager with createQueryCacheManager', () => {
    const manager = createQueryCacheManager({ maxEntries: 100 });

    expect(manager).toBeInstanceOf(QueryCacheManager);
  });
});

// =============================================================================
// Edge Cases
// =============================================================================

describe('QueryResultCache - Edge Cases', () => {
  it('should handle null values in filter', () => {
    const cache = new QueryResultCache('test');
    const filter = { name: null };
    const results: WithId<Document>[] = [{ _id: '1', name: null }];

    cache.set(filter as Filter<Document>, undefined, results);
    const cached = cache.get(filter as Filter<Document>);

    expect(cached).toEqual(results);
  });

  it('should handle empty results', () => {
    const cache = new QueryResultCache('test');
    const filter = { name: 'NotFound' };
    const results: WithId<Document>[] = [];

    cache.set(filter, undefined, results);
    const cached = cache.get(filter);

    expect(cached).toEqual([]);
  });

  it('should handle complex nested filters', () => {
    const cache = new QueryResultCache('test');
    const filter = {
      $and: [
        { status: 'active' },
        { $or: [{ age: { $gt: 25 } }, { role: 'admin' }] },
      ],
    };
    const results: WithId<Document>[] = [{ _id: '1', status: 'active' }];

    cache.set(filter as Filter<Document>, undefined, results);
    const cached = cache.get(filter as Filter<Document>);

    expect(cached).toEqual(results);
  });

  it('should handle filters with Date objects', () => {
    const cache = new QueryResultCache('test');
    const date = new Date('2024-01-01');
    const filter = { createdAt: { $gt: date } };
    const results: WithId<Document>[] = [{ _id: '1', createdAt: new Date() }];

    cache.set(filter as Filter<Document>, undefined, results);

    // Same date value should hit cache
    const cached = cache.get({ createdAt: { $gt: new Date('2024-01-01') } } as Filter<Document>);
    expect(cached).toEqual(results);
  });

  it('should handle documents with ObjectId-like _id', () => {
    const cache = new QueryResultCache('test');
    const filter = { status: 'active' };
    const results: WithId<Document>[] = [
      { _id: '507f1f77bcf86cd799439011', status: 'active' },
    ];

    cache.set(filter, undefined, results);
    const cached = cache.get(filter);

    expect(cached).toEqual(results);
  });
});
