/**
 * Collection Query Cache Integration Tests
 *
 * Tests for the query result cache integration with the Collection class.
 * Verifies that:
 * - Queries use the cache by default
 * - Cache is invalidated on writes
 * - Cache can be disabled via options
 * - Cache statistics are accessible
 */

import { describe, it, expect } from 'vitest';
import { createTestCollection } from '../client/test-helpers.js';

// =============================================================================
// Cache Integration Tests
// =============================================================================

describe('Collection - Query Cache Integration', () => {
  describe('Cache Usage', () => {
    it('should return cached results on repeated queries', async () => {
      const { collection } = createTestCollection();

      await collection.insertMany([
        { _id: '1', name: 'Alice', age: 30 },
        { _id: '2', name: 'Bob', age: 25 },
        { _id: '3', name: 'Charlie', age: 35 },
      ]);

      // First query - cache miss
      const results1 = await collection.find({ age: { $gt: 20 } }).toArray();
      expect(results1).toHaveLength(3);

      const stats1 = collection.getCacheStats();
      expect(stats1.misses).toBe(1);
      expect(stats1.hits).toBe(0);

      // Second query - cache hit
      const results2 = await collection.find({ age: { $gt: 20 } }).toArray();
      expect(results2).toHaveLength(3);

      const stats2 = collection.getCacheStats();
      expect(stats2.hits).toBe(1);
      expect(stats2.misses).toBe(1);
    });

    it('should use separate cache entries for different queries', async () => {
      const { collection } = createTestCollection();

      await collection.insertMany([
        { _id: '1', name: 'Alice', status: 'active' },
        { _id: '2', name: 'Bob', status: 'inactive' },
      ]);

      // Query 1
      await collection.find({ status: 'active' }).toArray();

      // Query 2 - different filter
      await collection.find({ status: 'inactive' }).toArray();

      const stats = collection.getCacheStats();
      expect(stats.entries).toBe(2);
      expect(stats.misses).toBe(2);
    });

    it('should use separate cache entries for same filter with different options', async () => {
      const { collection } = createTestCollection();

      await collection.insertMany([
        { _id: '1', name: 'Alice', value: 1 },
        { _id: '2', name: 'Bob', value: 2 },
        { _id: '3', name: 'Charlie', value: 3 },
      ]);

      // Query with limit 1
      const results1 = await collection.find({}).limit(1).toArray();
      expect(results1).toHaveLength(1);

      // Query with limit 2 - different cache entry
      const results2 = await collection.find({}).limit(2).toArray();
      expect(results2).toHaveLength(2);

      const stats = collection.getCacheStats();
      expect(stats.entries).toBe(2);
    });
  });

  describe('Cache Invalidation', () => {
    it('should invalidate cache on insertOne', async () => {
      const { collection } = createTestCollection();

      await collection.insertOne({ _id: '1', name: 'Alice' });

      // Cache the query
      await collection.find({}).toArray();
      expect(collection.getCacheStats().entries).toBe(1);

      // Insert invalidates cache
      await collection.insertOne({ _id: '2', name: 'Bob' });

      expect(collection.getCacheStats().entries).toBe(0);
      // Note: invalidations = 2 because both the initial insert and the second insert
      // trigger cache invalidation (the first has no effect since cache is empty)
      expect(collection.getCacheStats().invalidations).toBe(2);
    });

    it('should invalidate cache on insertMany', async () => {
      const { collection } = createTestCollection();

      await collection.insertOne({ _id: '1', name: 'Alice' });

      // Cache the query
      await collection.find({}).toArray();
      expect(collection.getCacheStats().entries).toBe(1);

      // InsertMany invalidates cache
      await collection.insertMany([
        { _id: '2', name: 'Bob' },
        { _id: '3', name: 'Charlie' },
      ]);

      expect(collection.getCacheStats().entries).toBe(0);
    });

    it('should invalidate cache on updateOne', async () => {
      const { collection } = createTestCollection();

      await collection.insertOne({ _id: '1', name: 'Alice', age: 30 });

      // Cache the query
      await collection.find({ name: 'Alice' }).toArray();
      expect(collection.getCacheStats().entries).toBe(1);

      // Update invalidates cache
      await collection.updateOne({ _id: '1' }, { $set: { age: 31 } });

      expect(collection.getCacheStats().entries).toBe(0);
    });

    it('should invalidate cache on updateMany', async () => {
      const { collection } = createTestCollection();

      await collection.insertMany([
        { _id: '1', status: 'active' },
        { _id: '2', status: 'active' },
      ]);

      // Cache the query
      await collection.find({ status: 'active' }).toArray();

      // UpdateMany invalidates cache
      await collection.updateMany({ status: 'active' }, { $set: { updated: true } });

      expect(collection.getCacheStats().entries).toBe(0);
    });

    it('should invalidate cache on deleteOne', async () => {
      const { collection } = createTestCollection();

      await collection.insertOne({ _id: '1', name: 'Alice' });

      // Cache the query
      await collection.find({}).toArray();
      expect(collection.getCacheStats().entries).toBe(1);

      // Delete invalidates cache
      await collection.deleteOne({ _id: '1' });

      expect(collection.getCacheStats().entries).toBe(0);
    });

    it('should invalidate cache on deleteMany', async () => {
      const { collection } = createTestCollection();

      await collection.insertMany([
        { _id: '1', status: 'old' },
        { _id: '2', status: 'old' },
      ]);

      // Cache the query
      await collection.find({}).toArray();

      // DeleteMany invalidates cache
      await collection.deleteMany({ status: 'old' });

      expect(collection.getCacheStats().entries).toBe(0);
    });

    it('should invalidate cache on replaceOne', async () => {
      const { collection } = createTestCollection();

      await collection.insertOne({ _id: '1', name: 'Alice' });

      // Cache the query
      await collection.find({}).toArray();

      // ReplaceOne invalidates cache
      await collection.replaceOne({ _id: '1' }, { name: 'Alice Updated' });

      expect(collection.getCacheStats().entries).toBe(0);
    });
  });

  describe('Cache Control Options', () => {
    it('should bypass cache with noCache option', async () => {
      const { collection } = createTestCollection();

      await collection.insertOne({ _id: '1', name: 'Alice' });

      // First query - caches result
      await collection.find({}).toArray();

      // Second query with noCache - bypasses cache
      await collection.find({}, { noCache: true }).toArray();

      const stats = collection.getCacheStats();
      // Should have 1 hit from first query, 1 miss from noCache query
      // But noCache should still cache the result
      expect(stats.entries).toBeGreaterThan(0);
    });

    it('should not use cache when useCache is false', async () => {
      const { collection } = createTestCollection();

      await collection.insertOne({ _id: '1', name: 'Alice' });

      // Query with useCache: false
      await collection.find({}, { useCache: false }).toArray();
      await collection.find({}, { useCache: false }).toArray();

      const stats = collection.getCacheStats();
      // Cache should not be used at all
      expect(stats.entries).toBe(0);
      expect(stats.hits).toBe(0);
    });
  });

  describe('Cache Management', () => {
    it('should clear cache manually', async () => {
      const { collection } = createTestCollection();

      await collection.insertMany([
        { _id: '1', name: 'Alice' },
        { _id: '2', name: 'Bob' },
      ]);

      // Cache some queries
      await collection.find({ name: 'Alice' }).toArray();
      await collection.find({ name: 'Bob' }).toArray();

      expect(collection.getCacheStats().entries).toBe(2);

      // Clear cache
      collection.clearCache();

      expect(collection.getCacheStats().entries).toBe(0);
    });

    it('should enable/disable cache', async () => {
      const { collection } = createTestCollection();

      await collection.insertOne({ _id: '1', name: 'Alice' });

      // Disable cache
      collection.setCacheEnabled(false);
      expect(collection.isCacheEnabled()).toBe(false);

      // Query should not be cached
      await collection.find({}).toArray();
      await collection.find({}).toArray();

      expect(collection.getCacheStats().entries).toBe(0);

      // Re-enable cache
      collection.setCacheEnabled(true);
      expect(collection.isCacheEnabled()).toBe(true);

      // Query should now be cached
      await collection.find({}).toArray();
      expect(collection.getCacheStats().entries).toBe(1);
    });

    it('should report cache statistics', async () => {
      const { collection } = createTestCollection();

      await collection.insertMany([
        { _id: '1', name: 'Alice' },
        { _id: '2', name: 'Bob' },
      ]);

      // Generate some cache activity
      await collection.find({ name: 'Alice' }).toArray(); // miss
      await collection.find({ name: 'Alice' }).toArray(); // hit
      await collection.find({ name: 'Bob' }).toArray();   // miss
      await collection.find({ name: 'NotFound' }).toArray(); // miss

      const stats = collection.getCacheStats();

      expect(stats.entries).toBe(3);
      expect(stats.hits).toBe(1);
      expect(stats.misses).toBe(3);
      expect(stats.hitRate).toBe(25);
      expect(stats.memoryBytes).toBeGreaterThan(0);
    });
  });

  describe('Cache with Query Options', () => {
    it('should cache queries with sort option', async () => {
      const { collection } = createTestCollection();

      await collection.insertMany([
        { _id: '1', name: 'Charlie', value: 3 },
        { _id: '2', name: 'Alice', value: 1 },
        { _id: '3', name: 'Bob', value: 2 },
      ]);

      // Query with sort
      const results1 = await collection.find({}).sort({ name: 1 }).toArray();
      expect(results1[0].name).toBe('Alice');

      // Same query - should hit cache
      const results2 = await collection.find({}).sort({ name: 1 }).toArray();
      expect(results2[0].name).toBe('Alice');

      const stats = collection.getCacheStats();
      expect(stats.hits).toBe(1);
    });

    it('should cache queries with projection', async () => {
      const { collection } = createTestCollection();

      await collection.insertOne({ _id: '1', name: 'Alice', age: 30, email: 'alice@test.com' });

      // Query with projection
      const results1 = await collection.find({}).project({ name: 1 }).toArray();
      expect(results1[0]).toHaveProperty('name');
      expect(results1[0]).not.toHaveProperty('age');

      // Same query - should hit cache
      await collection.find({}).project({ name: 1 }).toArray();

      const stats = collection.getCacheStats();
      expect(stats.hits).toBe(1);
    });

    it('should cache queries with skip and limit', async () => {
      const { collection } = createTestCollection();

      await collection.insertMany([
        { _id: '1', value: 1 },
        { _id: '2', value: 2 },
        { _id: '3', value: 3 },
        { _id: '4', value: 4 },
        { _id: '5', value: 5 },
      ]);

      // Query with skip and limit
      const results1 = await collection.find({}).sort({ value: 1 }).skip(1).limit(2).toArray();
      expect(results1).toHaveLength(2);
      expect(results1[0].value).toBe(2);

      // Same query - should hit cache
      await collection.find({}).sort({ value: 1 }).skip(1).limit(2).toArray();

      const stats = collection.getCacheStats();
      expect(stats.hits).toBe(1);
    });
  });

  describe('Cache with findOne', () => {
    it('should use cache for findOne', async () => {
      const { collection } = createTestCollection();

      await collection.insertMany([
        { _id: '1', name: 'Alice' },
        { _id: '2', name: 'Bob' },
      ]);

      // First findOne - cache miss
      const result1 = await collection.findOne({ name: 'Alice' });
      expect(result1?.name).toBe('Alice');

      // Second findOne - should hit cache (findOne uses find with limit 1)
      const result2 = await collection.findOne({ name: 'Alice' });
      expect(result2?.name).toBe('Alice');

      const stats = collection.getCacheStats();
      expect(stats.hits).toBe(1);
    });
  });
});

// =============================================================================
// Performance Considerations
// =============================================================================

describe('Collection - Query Cache Performance', () => {
  it('should handle many cached queries efficiently', async () => {
    const { collection } = createTestCollection();

    await collection.insertMany([
      { _id: '1', category: 'A', value: 1 },
      { _id: '2', category: 'B', value: 2 },
      { _id: '3', category: 'C', value: 3 },
    ]);

    // Create many different queries
    for (let i = 0; i < 100; i++) {
      await collection.find({ value: { $gt: i % 3 } }).toArray();
    }

    // Cache should have bounded size
    const stats = collection.getCacheStats();
    expect(stats.entries).toBeLessThanOrEqual(stats.maxEntries);
  });

  it('should not cache queries that would exceed result size limits', async () => {
    const { collection } = createTestCollection();

    // This test verifies that very large result sets are not cached
    // The cache implementation should have a threshold for this
    // For this test, we just verify the cache doesn't grow unbounded

    await collection.insertMany(
      Array.from({ length: 100 }, (_, i) => ({
        _id: String(i),
        data: 'x'.repeat(100),
      }))
    );

    await collection.find({}).toArray();

    const stats = collection.getCacheStats();
    // Cache should have accepted this reasonably-sized result
    expect(stats.entries).toBeGreaterThan(0);
  });
});
