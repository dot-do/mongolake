/**
 * LRU Cache Tests
 *
 * Tests for the LRU cache implementation covering:
 * - Basic get/set/has/delete operations
 * - LRU eviction behavior
 * - Configurable max size
 * - Optional TTL support
 * - Cache statistics
 * - Edge cases
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { LRUCache, createLRUCache, type LRUCacheOptions } from '../../../src/utils/lru-cache.js';

// =============================================================================
// Basic Operations
// =============================================================================

describe('LRUCache - Basic Operations', () => {
  it('should store and retrieve values', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    expect(cache.get('a')).toBe(1);
    expect(cache.get('b')).toBe(2);
    expect(cache.get('c')).toBe(3);
  });

  it('should return undefined for non-existent keys', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    expect(cache.get('nonexistent')).toBeUndefined();
  });

  it('should update existing values', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('key', 1);
    expect(cache.get('key')).toBe(1);

    cache.set('key', 2);
    expect(cache.get('key')).toBe(2);
  });

  it('should check if key exists with has()', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);

    expect(cache.has('a')).toBe(true);
    expect(cache.has('b')).toBe(false);
  });

  it('should delete specific keys', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);

    expect(cache.delete('a')).toBe(true);
    expect(cache.get('a')).toBeUndefined();
    expect(cache.has('a')).toBe(false);
    expect(cache.get('b')).toBe(2);
  });

  it('should return false when deleting non-existent keys', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    expect(cache.delete('nonexistent')).toBe(false);
  });

  it('should clear all entries', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    cache.clear();

    expect(cache.size).toBe(0);
    expect(cache.get('a')).toBeUndefined();
    expect(cache.get('b')).toBeUndefined();
    expect(cache.get('c')).toBeUndefined();
  });

  it('should report correct size', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    expect(cache.size).toBe(0);

    cache.set('a', 1);
    expect(cache.size).toBe(1);

    cache.set('b', 2);
    expect(cache.size).toBe(2);

    cache.delete('a');
    expect(cache.size).toBe(1);
  });

  it('should support method chaining for set()', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    const result = cache.set('a', 1).set('b', 2).set('c', 3);

    expect(result).toBe(cache);
    expect(cache.size).toBe(3);
  });
});

// =============================================================================
// LRU Eviction Behavior
// =============================================================================

describe('LRUCache - LRU Eviction', () => {
  it('should evict least recently used entry when at capacity', () => {
    const cache = createLRUCache<string, number>({ maxSize: 3 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);
    // Cache is now at capacity [a, b, c]

    cache.set('d', 4);
    // 'a' should be evicted as it's the oldest

    expect(cache.get('a')).toBeUndefined();
    expect(cache.get('b')).toBe(2);
    expect(cache.get('c')).toBe(3);
    expect(cache.get('d')).toBe(4);
    expect(cache.size).toBe(3);
  });

  it('should move accessed entry to most recently used position', () => {
    const cache = createLRUCache<string, number>({ maxSize: 3 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);
    // Order: [a, b, c]

    // Access 'a' to move it to the end
    cache.get('a');
    // Order: [b, c, a]

    cache.set('d', 4);
    // 'b' should be evicted (now oldest)

    expect(cache.get('a')).toBe(1); // Still exists (was recently accessed)
    expect(cache.get('b')).toBeUndefined(); // Evicted
    expect(cache.get('c')).toBe(3);
    expect(cache.get('d')).toBe(4);
  });

  it('should move updated entry to most recently used position', () => {
    const cache = createLRUCache<string, number>({ maxSize: 3 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);
    // Order: [a, b, c]

    // Update 'a' to move it to the end
    cache.set('a', 10);
    // Order: [b, c, a]

    cache.set('d', 4);
    // 'b' should be evicted (now oldest)

    expect(cache.get('a')).toBe(10);
    expect(cache.get('b')).toBeUndefined();
    expect(cache.get('c')).toBe(3);
    expect(cache.get('d')).toBe(4);
  });

  it('should handle single-entry cache correctly', () => {
    const cache = createLRUCache<string, number>({ maxSize: 1 });

    cache.set('a', 1);
    expect(cache.get('a')).toBe(1);

    cache.set('b', 2);
    expect(cache.get('a')).toBeUndefined();
    expect(cache.get('b')).toBe(2);
    expect(cache.size).toBe(1);
  });

  it('should not evict when updating existing key at capacity', () => {
    const cache = createLRUCache<string, number>({ maxSize: 3 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    // Update existing key should not cause eviction
    cache.set('a', 10);

    expect(cache.size).toBe(3);
    expect(cache.get('a')).toBe(10);
    expect(cache.get('b')).toBe(2);
    expect(cache.get('c')).toBe(3);
  });
});

// =============================================================================
// TTL Support
// =============================================================================

describe('LRUCache - TTL Support', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should return value before TTL expires', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10, ttlMs: 1000 });

    cache.set('key', 42);

    vi.advanceTimersByTime(500);

    expect(cache.get('key')).toBe(42);
  });

  it('should return undefined after TTL expires', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10, ttlMs: 1000 });

    cache.set('key', 42);

    vi.advanceTimersByTime(1001);

    expect(cache.get('key')).toBeUndefined();
  });

  it('should report has() as false after TTL expires', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10, ttlMs: 1000 });

    cache.set('key', 42);

    vi.advanceTimersByTime(1001);

    expect(cache.has('key')).toBe(false);
  });

  it('should not extend TTL on access', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10, ttlMs: 1000 });

    cache.set('key', 42);

    vi.advanceTimersByTime(500);
    expect(cache.get('key')).toBe(42); // Access at 500ms

    vi.advanceTimersByTime(600); // Total: 1100ms from creation

    expect(cache.get('key')).toBeUndefined(); // Expired
  });

  it('should update TTL on set() with same key', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10, ttlMs: 1000 });

    cache.set('key', 42);

    vi.advanceTimersByTime(800);
    cache.set('key', 100); // Reset TTL

    vi.advanceTimersByTime(800); // Total: 1600ms, but only 800ms since last set

    expect(cache.get('key')).toBe(100); // Still valid
  });

  it('should prune expired entries', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10, ttlMs: 1000 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    vi.advanceTimersByTime(1001);

    const pruned = cache.prune();

    expect(pruned).toBe(3);
    expect(cache.size).toBe(0);
  });

  it('should only prune expired entries during prune()', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10, ttlMs: 1000 });

    cache.set('a', 1);

    vi.advanceTimersByTime(500);
    cache.set('b', 2);

    vi.advanceTimersByTime(600); // 'a' expired, 'b' still valid

    const pruned = cache.prune();

    expect(pruned).toBe(1);
    expect(cache.has('a')).toBe(false);
    expect(cache.has('b')).toBe(true);
  });

  it('should track TTL evictions in stats', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10, ttlMs: 1000 });

    cache.set('key', 42);

    vi.advanceTimersByTime(1001);
    cache.get('key'); // Triggers expiration check

    const stats = cache.getStats();
    expect(stats.ttlEvictions).toBe(1);
  });
});

// =============================================================================
// Eviction Callback
// =============================================================================

describe('LRUCache - Eviction Callback', () => {
  it('should call onEvict for capacity evictions', () => {
    const onEvict = vi.fn();
    const cache = createLRUCache<string, number>({
      maxSize: 2,
      onEvict,
    });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3); // Evicts 'a'

    expect(onEvict).toHaveBeenCalledTimes(1);
    expect(onEvict).toHaveBeenCalledWith('a', 1, 'capacity');
  });

  it('should call onEvict for manual deletes', () => {
    const onEvict = vi.fn();
    const cache = createLRUCache<string, number>({
      maxSize: 10,
      onEvict,
    });

    cache.set('a', 1);
    cache.delete('a');

    expect(onEvict).toHaveBeenCalledTimes(1);
    expect(onEvict).toHaveBeenCalledWith('a', 1, 'manual');
  });

  it('should call onEvict for TTL expirations', () => {
    vi.useFakeTimers();

    const onEvict = vi.fn();
    const cache = createLRUCache<string, number>({
      maxSize: 10,
      ttlMs: 1000,
      onEvict,
    });

    cache.set('key', 42);

    vi.advanceTimersByTime(1001);
    cache.get('key'); // Triggers expiration

    expect(onEvict).toHaveBeenCalledTimes(1);
    expect(onEvict).toHaveBeenCalledWith('key', 42, 'expired');

    vi.useRealTimers();
  });

  it('should call onEvict for all entries during clear() when notifyEvictions is true', () => {
    const onEvict = vi.fn();
    const cache = createLRUCache<string, number>({
      maxSize: 10,
      onEvict,
    });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.clear(true);

    expect(onEvict).toHaveBeenCalledTimes(2);
    expect(onEvict).toHaveBeenCalledWith('a', 1, 'manual');
    expect(onEvict).toHaveBeenCalledWith('b', 2, 'manual');
  });

  it('should not call onEvict during clear() when notifyEvictions is false', () => {
    const onEvict = vi.fn();
    const cache = createLRUCache<string, number>({
      maxSize: 10,
      onEvict,
    });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.clear(false);

    expect(onEvict).not.toHaveBeenCalled();
  });
});

// =============================================================================
// Statistics
// =============================================================================

describe('LRUCache - Statistics', () => {
  it('should track cache hits', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.get('a');
    cache.get('a');
    cache.get('a');

    const stats = cache.getStats();
    expect(stats.hits).toBe(3);
  });

  it('should track cache misses', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.get('nonexistent');
    cache.get('also-nonexistent');

    const stats = cache.getStats();
    expect(stats.misses).toBe(2);
  });

  it('should calculate hit rate correctly', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);

    // 3 hits, 1 miss = 75% hit rate
    cache.get('a'); // hit
    cache.get('a'); // hit
    cache.get('a'); // hit
    cache.get('b'); // miss

    const stats = cache.getStats();
    expect(stats.hitRate).toBe(75);
  });

  it('should report 0 hit rate when no accesses', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    const stats = cache.getStats();
    expect(stats.hitRate).toBe(0);
  });

  it('should track capacity evictions', () => {
    const cache = createLRUCache<string, number>({ maxSize: 2 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3); // Evicts 'a'
    cache.set('d', 4); // Evicts 'b'

    const stats = cache.getStats();
    expect(stats.capacityEvictions).toBe(2);
  });

  it('should reset statistics', () => {
    const cache = createLRUCache<string, number>({ maxSize: 2 });

    cache.set('a', 1);
    cache.get('a');
    cache.get('b'); // miss
    cache.set('c', 2);
    cache.set('d', 3); // eviction

    cache.resetStats();

    const stats = cache.getStats();
    expect(stats.hits).toBe(0);
    expect(stats.misses).toBe(0);
    expect(stats.capacityEvictions).toBe(0);
    expect(stats.ttlEvictions).toBe(0);
  });

  it('should report correct size and maxSize', () => {
    const cache = createLRUCache<string, number>({ maxSize: 100 });

    cache.set('a', 1);
    cache.set('b', 2);

    const stats = cache.getStats();
    expect(stats.size).toBe(2);
    expect(stats.maxSize).toBe(100);
  });
});

// =============================================================================
// Iteration Methods
// =============================================================================

describe('LRUCache - Iteration', () => {
  it('should return keys in LRU order', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    const keys = Array.from(cache.keys());
    expect(keys).toEqual(['a', 'b', 'c']);
  });

  it('should return values in LRU order', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    expect(cache.values()).toEqual([1, 2, 3]);
  });

  it('should return entries in LRU order', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    expect(cache.entries()).toEqual([
      ['a', 1],
      ['b', 2],
      ['c', 3],
    ]);
  });

  it('should iterate with forEach', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);

    const collected: Array<[string, number]> = [];
    cache.forEach((value, key) => {
      collected.push([key, value]);
    });

    expect(collected).toEqual([
      ['a', 1],
      ['b', 2],
    ]);
  });

  it('should handle deletion during forEach iteration', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    const collected: Array<[string, number]> = [];
    // Delete entries during iteration - should not crash
    cache.forEach((value, key) => {
      collected.push([key, value]);
      if (key === 'a') {
        cache.delete('b'); // Delete next entry
      }
    });

    // 'a' and 'c' should be collected, 'b' was deleted before being visited
    expect(collected).toEqual([
      ['a', 1],
      ['c', 3],
    ]);
    expect(cache.size).toBe(2);
    expect(cache.has('b')).toBe(false);
  });

  it('should handle clear during forEach iteration', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    const collected: Array<[string, number]> = [];
    // Clear cache during iteration - should not crash
    cache.forEach((value, key) => {
      collected.push([key, value]);
      if (key === 'a') {
        cache.clear();
      }
    });

    // Only 'a' should be collected since cache was cleared after first entry
    expect(collected).toEqual([['a', 1]]);
    expect(cache.size).toBe(0);
  });

  it('should handle set during forEach iteration', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);

    const collected: Array<[string, number]> = [];
    // Add new entries during iteration - should not affect current iteration
    cache.forEach((value, key) => {
      collected.push([key, value]);
      if (key === 'a') {
        cache.set('d', 4); // Add new entry during iteration
      }
    });

    // Original entries should be collected (new entry not in snapshot)
    expect(collected).toEqual([
      ['a', 1],
      ['b', 2],
    ]);
    // New entry should exist after iteration
    expect(cache.has('d')).toBe(true);
    expect(cache.get('d')).toBe(4);
  });
});

// =============================================================================
// Peek Methods
// =============================================================================

describe('LRUCache - Peek Methods', () => {
  it('should peek at value without updating access time', () => {
    const cache = createLRUCache<string, number>({ maxSize: 3 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    // Peek at 'a' - should not move it to the end
    expect(cache.peek('a')).toBe(1);

    // Adding new entry should evict 'a' (still oldest)
    cache.set('d', 4);

    expect(cache.peek('a')).toBeUndefined();
  });

  it('should return undefined for non-existent key on peek', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    expect(cache.peek('nonexistent')).toBeUndefined();
  });

  it('should return oldest entry with peekOldest()', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    const oldest = cache.peekOldest();
    expect(oldest).toEqual({ key: 'a', value: 1 });
  });

  it('should return newest entry with peekNewest()', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    const newest = cache.peekNewest();
    expect(newest).toEqual({ key: 'c', value: 3 });
  });

  it('should return undefined from peekOldest/peekNewest on empty cache', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    expect(cache.peekOldest()).toBeUndefined();
    expect(cache.peekNewest()).toBeUndefined();
  });
});

// =============================================================================
// Edge Cases
// =============================================================================

describe('LRUCache - Edge Cases', () => {
  it('should throw error for maxSize < 1', () => {
    expect(() => createLRUCache<string, number>({ maxSize: 0 })).toThrow(
      'LRU cache maxSize must be at least 1'
    );

    expect(() => createLRUCache<string, number>({ maxSize: -5 })).toThrow(
      'LRU cache maxSize must be at least 1'
    );
  });

  it('should handle storing undefined values', () => {
    const cache = createLRUCache<string, number | undefined>({ maxSize: 10 });

    cache.set('key', undefined);

    // has() should return true (key exists)
    expect(cache.has('key')).toBe(true);

    // get() returns undefined but that's the stored value
    expect(cache.get('key')).toBeUndefined();
  });

  it('should handle storing null values', () => {
    const cache = createLRUCache<string, number | null>({ maxSize: 10 });

    cache.set('key', null);

    expect(cache.has('key')).toBe(true);
    expect(cache.get('key')).toBeNull();
  });

  it('should handle various key types', () => {
    // String keys
    const stringCache = createLRUCache<string, number>({ maxSize: 10 });
    stringCache.set('key', 1);
    expect(stringCache.get('key')).toBe(1);

    // Number keys
    const numberCache = createLRUCache<number, string>({ maxSize: 10 });
    numberCache.set(42, 'value');
    expect(numberCache.get(42)).toBe('value');

    // Object keys (by reference)
    const objCache = createLRUCache<object, string>({ maxSize: 10 });
    const keyObj = { id: 1 };
    objCache.set(keyObj, 'value');
    expect(objCache.get(keyObj)).toBe('value');
    expect(objCache.get({ id: 1 })).toBeUndefined(); // Different reference
  });

  it('should handle complex value types', () => {
    interface ComplexValue {
      name: string;
      data: number[];
      nested: { a: number };
    }

    const cache = createLRUCache<string, ComplexValue>({ maxSize: 10 });

    const value: ComplexValue = {
      name: 'test',
      data: [1, 2, 3],
      nested: { a: 42 },
    };

    cache.set('key', value);

    const retrieved = cache.get('key');
    expect(retrieved).toEqual(value);
    expect(retrieved).toBe(value); // Same reference
  });

  it('should work without TTL configured', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('key', 42);

    // prune() should return 0 when no TTL is set
    expect(cache.prune()).toBe(0);

    // Values should persist indefinitely (until evicted by capacity)
    expect(cache.get('key')).toBe(42);
  });

  it('should handle rapid successive operations', () => {
    const cache = createLRUCache<string, number>({ maxSize: 5 });

    // Rapidly add and access entries
    for (let i = 0; i < 100; i++) {
      cache.set(`key${i}`, i);
      cache.get(`key${i}`);
    }

    // Should only have last 5 entries
    expect(cache.size).toBe(5);
    expect(cache.has('key95')).toBe(true);
    expect(cache.has('key99')).toBe(true);
    expect(cache.has('key0')).toBe(false);
  });
});

// =============================================================================
// Constructor Validation
// =============================================================================

describe('LRUCache - Constructor', () => {
  it('should create cache with class constructor directly', () => {
    const cache = new LRUCache<string, number>({ maxSize: 10 });

    cache.set('key', 42);
    expect(cache.get('key')).toBe(42);
  });

  it('should create cache with factory function', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('key', 42);
    expect(cache.get('key')).toBe(42);
  });

  it('should accept all optional configuration', () => {
    const onEvict = vi.fn();
    const cache = createLRUCache<string, number>({
      maxSize: 5,
      ttlMs: 60000,
      onEvict,
    });

    expect(cache.getStats().maxSize).toBe(5);
  });
});
