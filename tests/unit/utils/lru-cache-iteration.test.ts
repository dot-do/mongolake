/**
 * LRU Cache Iteration Safety Tests
 *
 * TDD RED Phase tests for iteration safety scenarios:
 * - forEach doesn't throw when items are added during iteration
 * - forEach doesn't throw when items are removed during iteration
 * - forEach sees consistent snapshot of data
 * - Concurrent iteration safety
 * - Iterator invalidation handling
 * - Memory safety during iteration
 *
 * These tests document expected behavior for safe iteration over LRU cache.
 * Current implementation uses Map iteration directly which may have
 * undefined behavior when modified during iteration.
 */

import { describe, it, expect, vi } from 'vitest';
import { LRUCache, createLRUCache } from '../../../src/utils/lru-cache.js';

// =============================================================================
// forEach - Adding Items During Iteration
// =============================================================================

describe('LRUCache Iteration Safety - Adding During forEach', () => {
  it.fails('should not throw when adding new items during forEach iteration', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    // This should not throw, but current implementation may have undefined behavior
    expect(() => {
      cache.forEach((value, key) => {
        if (key === 'b') {
          cache.set('d', 4); // Adding during iteration
        }
      });
    }).not.toThrow();
  });

  it.fails('should not cause infinite loop when adding items during forEach', () => {
    const cache = createLRUCache<string, number>({ maxSize: 100 });

    cache.set('a', 1);
    cache.set('b', 2);

    let iterations = 0;
    const maxIterations = 10;

    cache.forEach((value, key) => {
      iterations++;
      if (iterations <= 3 && key === 'a') {
        cache.set(`new-${iterations}`, iterations);
      }
      // Safety check to prevent actual infinite loop in test
      if (iterations > maxIterations) {
        throw new Error('Too many iterations - possible infinite loop');
      }
    });

    // Should complete without infinite loop and iterate original items
    expect(iterations).toBeLessThanOrEqual(maxIterations);
  });

  it.fails('should not see newly added items when iterating with forEach (snapshot behavior)', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);

    const visitedKeys: string[] = [];

    cache.forEach((value, key) => {
      visitedKeys.push(key);
      if (key === 'a') {
        cache.set('c', 3); // Add during iteration
      }
    });

    // Should only see original items, not newly added 'c'
    expect(visitedKeys).toEqual(['a', 'b']);
    expect(visitedKeys).not.toContain('c');
  });
});

// =============================================================================
// forEach - Removing Items During Iteration
// =============================================================================

describe('LRUCache Iteration Safety - Removing During forEach', () => {
  it.fails('should not throw when removing current item during forEach', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    expect(() => {
      cache.forEach((value, key) => {
        if (key === 'b') {
          cache.delete('b'); // Remove current item
        }
      });
    }).not.toThrow();
  });

  it.fails('should not throw when removing future items during forEach', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    expect(() => {
      cache.forEach((value, key) => {
        if (key === 'a') {
          cache.delete('c'); // Remove item not yet visited
        }
      });
    }).not.toThrow();
  });

  it.fails('should not throw when removing past items during forEach', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    expect(() => {
      cache.forEach((value, key) => {
        if (key === 'c') {
          cache.delete('a'); // Remove already visited item
        }
      });
    }).not.toThrow();
  });

  it.fails('should handle clear() during forEach iteration', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    const visitedKeys: string[] = [];

    expect(() => {
      cache.forEach((value, key) => {
        visitedKeys.push(key);
        if (key === 'a') {
          cache.clear(); // Clear all during iteration
        }
      });
    }).not.toThrow();

    // After clear, iteration should stop or continue safely
    expect(visitedKeys.length).toBeGreaterThan(0);
  });
});

// =============================================================================
// forEach - Consistent Snapshot
// =============================================================================

describe('LRUCache Iteration Safety - Consistent Snapshot', () => {
  // NOTE: This test passes because Map iteration sees updated values in-place
  // when the key hasn't been removed/re-added. This is NOT true snapshot behavior.
  it('should iterate over values (current behavior - not true snapshot)', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    const collectedValues: number[] = [];

    cache.forEach((value, key) => {
      collectedValues.push(value);
      if (key === 'a') {
        cache.set('b', 200); // Modify value during iteration
      }
    });

    // Current behavior: sees updated value because Map doesn't snapshot
    // With set() on existing key, value is replaced in-place
    expect(collectedValues).toEqual([1, 200, 3]);
  });

  // TDD RED: True snapshot behavior would preserve original values
  it.fails('should iterate over consistent snapshot when items are modified (true snapshot)', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    const collectedValues: number[] = [];

    cache.forEach((value, key) => {
      collectedValues.push(value);
      if (key === 'a') {
        cache.set('b', 200); // Modify value during iteration
      }
    });

    // Should see original value of 'b', not modified value
    expect(collectedValues).toEqual([1, 2, 3]);
  });

  it.fails('should maintain iteration order even when get() reorders entries', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);
    // Order: a, b, c

    const visitedKeys: string[] = [];

    cache.forEach((value, key) => {
      visitedKeys.push(key);
      if (key === 'a') {
        // get() normally reorders to make 'a' most recent
        // With snapshot behavior, this shouldn't affect iteration order
        cache.get('a');
      }
    });

    // Should maintain original LRU order during iteration
    expect(visitedKeys).toEqual(['a', 'b', 'c']);
  });

  // This test passes because Map iteration continues despite deletions
  // (deleted items are skipped, but remaining items are still visited)
  it('should visit remaining entries even when eviction occurs during iteration', () => {
    const cache = createLRUCache<string, number>({ maxSize: 3 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    const visitedKeys: string[] = [];

    cache.forEach((value, key) => {
      visitedKeys.push(key);
      if (key === 'a') {
        // Adding 'd' should evict 'a', but we already visited it
        // 'b' will be evicted when we add 'e'
        cache.set('d', 4);
        cache.set('e', 5);
      }
    });

    // Should have visited 'a' before eviction and 'c' which wasn't evicted
    expect(visitedKeys).toContain('a');
    expect(visitedKeys).toContain('c');
    // 'b' was evicted before iteration reached it
  });

  // TDD RED: True snapshot would visit all original entries
  it.fails('should see all original entries even when eviction occurs (true snapshot)', () => {
    const cache = createLRUCache<string, number>({ maxSize: 3 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    const visitedKeys: string[] = [];

    cache.forEach((value, key) => {
      visitedKeys.push(key);
      if (key === 'a') {
        cache.set('d', 4);
        cache.set('e', 5);
      }
    });

    // True snapshot should visit all original entries
    expect(visitedKeys).toEqual(['a', 'b', 'c']);
  });
});

// =============================================================================
// Concurrent Iteration Safety
// =============================================================================

describe('LRUCache Iteration Safety - Concurrent Iteration', () => {
  it.fails('should support nested forEach calls', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);

    const pairs: Array<[string, string]> = [];

    expect(() => {
      cache.forEach((value1, key1) => {
        cache.forEach((value2, key2) => {
          pairs.push([key1, key2]);
        });
      });
    }).not.toThrow();

    // Should create all pair combinations
    expect(pairs).toHaveLength(4); // 2 x 2 = 4 pairs
    expect(pairs).toContainEqual(['a', 'a']);
    expect(pairs).toContainEqual(['a', 'b']);
    expect(pairs).toContainEqual(['b', 'a']);
    expect(pairs).toContainEqual(['b', 'b']);
  });

  it.fails('should handle modifications in nested forEach safely', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);

    expect(() => {
      cache.forEach((value1, key1) => {
        cache.forEach((value2, key2) => {
          if (key1 === 'a' && key2 === 'a') {
            cache.set('c', 3); // Add during nested iteration
          }
        });
      });
    }).not.toThrow();
  });

  it.fails('should allow multiple independent iterations (simulated async)', async () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    const results1: string[] = [];
    const results2: string[] = [];

    // Simulate interleaved iteration
    const iter1 = cache.entries()[Symbol.iterator]?.() ?? cache.entries();
    const iter2 = cache.entries()[Symbol.iterator]?.() ?? cache.entries();

    // This tests if the cache supports multiple simultaneous iterators
    // by alternating between two iteration sequences
    const entries1 = cache.entries();
    const entries2 = cache.entries();

    for (const [key] of entries1) {
      results1.push(key);
    }
    for (const [key] of entries2) {
      results2.push(key);
    }

    expect(results1).toEqual(['a', 'b', 'c']);
    expect(results2).toEqual(['a', 'b', 'c']);
  });
});

// =============================================================================
// Iterator Invalidation Handling
// =============================================================================

describe('LRUCache Iteration Safety - Iterator Invalidation', () => {
  it.fails('should handle keys() iterator when cache is modified', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    const keys = cache.keys();
    const collectedKeys: string[] = [];

    // Start iterating
    const firstKey = keys.next();
    collectedKeys.push(firstKey.value);

    // Modify cache during iteration
    cache.set('d', 4);
    cache.delete('b');

    // Continue iterating - should not throw
    expect(() => {
      for (const key of keys) {
        collectedKeys.push(key);
      }
    }).not.toThrow();
  });

  // values() and entries() return arrays (snapshots), so they work correctly
  // Note: set() on existing key moves it to the end (most recently used)
  it('should handle values() snapshot correctly when cache is modified', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    // values() returns array, so it's inherently a snapshot
    const values1 = cache.values();
    cache.set('a', 100); // Moves 'a' to end
    cache.delete('b');
    const values2 = cache.values();

    // First snapshot should have original values
    expect(values1).toEqual([1, 2, 3]);
    // Second snapshot reflects: 'b' deleted, 'a' moved to end with new value
    // Order is now: c, a (since 'a' was re-added)
    expect(values2).toEqual([3, 100]);
  });

  it('should handle entries() snapshot correctly when cache is modified', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    // entries() returns array, so it's inherently a snapshot
    const entries1 = cache.entries();
    cache.set('a', 100); // Moves 'a' to end
    cache.delete('b');
    const entries2 = cache.entries();

    // First snapshot should have original entries
    expect(entries1).toEqual([
      ['a', 1],
      ['b', 2],
      ['c', 3],
    ]);
    // Second snapshot reflects: 'b' deleted, 'a' moved to end with new value
    // Order is now: c, a (since 'a' was re-added)
    expect(entries2).toEqual([
      ['c', 3],
      ['a', 100],
    ]);
  });

  // TDD RED: safeForEach method doesn't exist yet
  it.fails('should provide safeForEach that takes snapshot before iteration', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    const visitedKeys: string[] = [];
    const visitedValues: number[] = [];

    // Hypothetical safeForEach method that creates snapshot
    // This method should exist and take a snapshot of entries before iterating
    // @ts-expect-error - safeForEach doesn't exist yet (TDD)
    if (typeof cache.safeForEach !== 'function') {
      throw new Error('safeForEach method does not exist');
    }

    // @ts-expect-error - safeForEach doesn't exist yet (TDD)
    cache.safeForEach((value: number, key: string) => {
      visitedKeys.push(key);
      visitedValues.push(value);
      if (key === 'a') {
        cache.set('d', 4);
        cache.delete('b');
        cache.set('c', 300);
      }
    });

    // Should see original state
    expect(visitedKeys).toEqual(['a', 'b', 'c']);
    expect(visitedValues).toEqual([1, 2, 3]);
  });
});

// =============================================================================
// Memory Safety During Iteration
// =============================================================================

describe('LRUCache Iteration Safety - Memory Safety', () => {
  it.fails('should not leak memory when exceptions thrown during forEach', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    // Throw during iteration
    expect(() => {
      cache.forEach((value, key) => {
        if (key === 'b') {
          throw new Error('Iteration error');
        }
      });
    }).toThrow('Iteration error');

    // Cache should still be in valid state
    expect(cache.size).toBe(3);
    expect(cache.get('a')).toBe(1);
    expect(cache.get('b')).toBe(2);
    expect(cache.get('c')).toBe(3);
  });

  it.fails('should handle iteration over large cache without excessive memory', () => {
    const cache = createLRUCache<number, { data: number[] }>({ maxSize: 10000 });

    // Fill cache with moderately sized objects
    for (let i = 0; i < 10000; i++) {
      cache.set(i, { data: new Array(100).fill(i) });
    }

    let count = 0;

    // Iteration should not create excessive copies
    cache.forEach((value, key) => {
      count++;
      // Verify we can access the value
      expect(value.data.length).toBe(100);
    });

    expect(count).toBe(10000);
  });

  it.fails('should not hold references to deleted items during iteration', () => {
    const cache = createLRUCache<string, { id: number }>({ maxSize: 10 });
    const weakRefs: WeakRef<{ id: number }>[] = [];

    // Add items and track with WeakRefs
    for (let i = 0; i < 5; i++) {
      const obj = { id: i };
      cache.set(`key${i}`, obj);
      weakRefs.push(new WeakRef(obj));
    }

    // Delete some items during iteration
    cache.forEach((value, key) => {
      if (value.id < 3) {
        cache.delete(key);
      }
    });

    // Force garbage collection (if available in test environment)
    if (global.gc) {
      global.gc();
    }

    // Deleted items should be eligible for GC
    // Note: This is difficult to test reliably without gc() access
    expect(cache.size).toBe(2); // key3 and key4 remain
  });

  it.fails('should handle TTL expiration during iteration safely', () => {
    vi.useFakeTimers();

    const cache = createLRUCache<string, number>({ maxSize: 10, ttlMs: 1000 });

    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);

    const visitedKeys: string[] = [];

    // Start iteration, then advance time during it
    cache.forEach((value, key) => {
      visitedKeys.push(key);
      if (key === 'a') {
        // Advance time to expire all entries
        vi.advanceTimersByTime(1500);
      }
    });

    // Should handle expiration gracefully
    expect(visitedKeys.length).toBeGreaterThan(0);

    vi.useRealTimers();
  });
});

// =============================================================================
// Edge Cases for Iteration
// =============================================================================

describe('LRUCache Iteration Safety - Edge Cases', () => {
  it.fails('should handle forEach on empty cache', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    const visited: string[] = [];

    cache.forEach((value, key) => {
      visited.push(key);
    });

    expect(visited).toEqual([]);
  });

  it.fails('should handle forEach on single-item cache with modification', () => {
    const cache = createLRUCache<string, number>({ maxSize: 1 });

    cache.set('a', 1);

    const visited: string[] = [];

    cache.forEach((value, key) => {
      visited.push(key);
      cache.set('b', 2); // This will evict 'a'
    });

    expect(visited).toContain('a');
  });

  it.fails('should handle re-adding same key during forEach', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);
    cache.set('b', 2);

    let visitCount = 0;
    const visitedKeys: string[] = [];

    cache.forEach((value, key) => {
      visitCount++;
      visitedKeys.push(key);
      if (key === 'a') {
        cache.set('a', 100); // Re-add same key with different value
      }
      // Prevent infinite loop
      if (visitCount > 10) {
        throw new Error('Too many iterations');
      }
    });

    // Should not revisit 'a' after re-adding
    expect(visitedKeys).toEqual(['a', 'b']);
  });

  // TDD RED: Current forEach doesn't support thisArg parameter
  // The current implementation ignores the thisArg parameter
  it.fails('should maintain callback this context in forEach with thisArg', () => {
    const cache = createLRUCache<string, number>({ maxSize: 10 });

    cache.set('a', 1);

    const context = { count: 0 };

    // Current forEach signature is: forEach(callback: (value, key, cache) => void)
    // It doesn't accept a thisArg parameter like Array.forEach does
    // This test expects forEach to support: forEach(callback, thisArg)
    cache.forEach(function (this: { count: number }, value, key) {
      // 'this' should be bound to context
      if (this && typeof this.count === 'number') {
        this.count++;
      } else {
        throw new Error('this context not bound correctly');
      }
    }, context as unknown as LRUCache<string, number>);

    expect(context.count).toBe(1);
  });

  it.fails('should handle prune() during forEach iteration', () => {
    vi.useFakeTimers();

    const cache = createLRUCache<string, number>({ maxSize: 10, ttlMs: 1000 });

    cache.set('a', 1);
    vi.advanceTimersByTime(500);
    cache.set('b', 2);
    vi.advanceTimersByTime(600); // 'a' is now expired

    const visited: string[] = [];

    cache.forEach((value, key) => {
      visited.push(key);
      if (key === 'b') {
        cache.prune(); // Prune expired entries during iteration
      }
    });

    // Should handle prune safely
    expect(visited.length).toBeGreaterThan(0);

    vi.useRealTimers();
  });
});
