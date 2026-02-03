/**
 * MongoLake Shard Selection Randomness Tests
 *
 * RED tests verifying that shard selection does NOT use Math.random.
 * Shard selection should be deterministic based on input (using hash functions)
 * or use cryptographically secure randomness when needed.
 *
 * Key test scenarios:
 * - Shard selection is deterministic for same input
 * - Math.random mock doesn't affect selection
 * - Distribution is even across shards (good hash distribution)
 * - Crypto randomness used if any randomness is needed
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  ShardRouter,
  hashCollectionToShard,
  hashDocumentToShard,
  createShardRouter,
} from '../../../src/shard/router';
import { DEFAULT_SHARD_COUNT } from '../../../src/constants';

describe('Shard Selection Randomness Tests', () => {
  let originalMathRandom: typeof Math.random;
  let mathRandomCallCount: number;

  beforeEach(() => {
    // Store original Math.random
    originalMathRandom = Math.random;
    mathRandomCallCount = 0;
  });

  afterEach(() => {
    // Restore original Math.random
    Math.random = originalMathRandom;
    vi.restoreAllMocks();
  });

  describe('Determinism: same input produces same output', () => {
    it('should return identical shard for identical collection name across multiple calls', () => {
      const collectionName = 'users';
      const results: number[] = [];

      for (let i = 0; i < 100; i++) {
        results.push(hashCollectionToShard(collectionName));
      }

      const uniqueResults = new Set(results);
      expect(uniqueResults.size).toBe(1);
      expect(results[0]).toBe(results[99]);
    });

    it('should return identical shard for identical document ID across multiple calls', () => {
      const documentId = '507f1f77bcf86cd799439011';
      const results: number[] = [];

      for (let i = 0; i < 100; i++) {
        results.push(hashDocumentToShard(documentId));
      }

      const uniqueResults = new Set(results);
      expect(uniqueResults.size).toBe(1);
      expect(results[0]).toBe(results[99]);
    });

    it('should return identical shard assignment from router for same collection', () => {
      const router = createShardRouter();
      const results: number[] = [];

      for (let i = 0; i < 100; i++) {
        results.push(router.route('orders').shardId);
      }

      const uniqueResults = new Set(results);
      expect(uniqueResults.size).toBe(1);
    });

    it('should return identical shard for document routing with same collection and ID', () => {
      const router = createShardRouter();
      const collection = 'events';
      const docId = 'event-12345-abcdef';
      const results: number[] = [];

      for (let i = 0; i < 100; i++) {
        results.push(router.routeDocument(collection, docId).shardId);
      }

      const uniqueResults = new Set(results);
      expect(uniqueResults.size).toBe(1);
    });

    it('should produce same results across different router instances', () => {
      const router1 = createShardRouter();
      const router2 = createShardRouter();
      const router3 = createShardRouter();

      const collection = 'products';
      const shard1 = router1.route(collection).shardId;
      const shard2 = router2.route(collection).shardId;
      const shard3 = router3.route(collection).shardId;

      expect(shard1).toBe(shard2);
      expect(shard2).toBe(shard3);
    });

    it('should produce same document routing results across different router instances', () => {
      const router1 = createShardRouter();
      const router2 = createShardRouter();

      const collection = 'logs';
      const docId = 'log-2024-01-15-001';

      const shard1 = router1.routeDocument(collection, docId).shardId;
      const shard2 = router2.routeDocument(collection, docId).shardId;

      expect(shard1).toBe(shard2);
    });
  });

  describe('Math.random independence: mocking Math.random should not affect results', () => {
    it('should not call Math.random during hashCollectionToShard', () => {
      Math.random = vi.fn(() => {
        mathRandomCallCount++;
        return 0.5;
      });

      hashCollectionToShard('test_collection');

      expect(mathRandomCallCount).toBe(0);
      expect(Math.random).not.toHaveBeenCalled();
    });

    it('should not call Math.random during hashDocumentToShard', () => {
      Math.random = vi.fn(() => {
        mathRandomCallCount++;
        return 0.5;
      });

      hashDocumentToShard('507f1f77bcf86cd799439011');

      expect(mathRandomCallCount).toBe(0);
      expect(Math.random).not.toHaveBeenCalled();
    });

    it('should not call Math.random during router.route()', () => {
      Math.random = vi.fn(() => {
        mathRandomCallCount++;
        return 0.5;
      });

      const router = createShardRouter();
      router.route('users');
      router.route('orders');
      router.route('products');

      expect(mathRandomCallCount).toBe(0);
      expect(Math.random).not.toHaveBeenCalled();
    });

    it('should not call Math.random during router.routeDocument()', () => {
      Math.random = vi.fn(() => {
        mathRandomCallCount++;
        return 0.5;
      });

      const router = createShardRouter();
      router.routeDocument('users', 'user-123');
      router.routeDocument('orders', 'order-456');

      expect(mathRandomCallCount).toBe(0);
      expect(Math.random).not.toHaveBeenCalled();
    });

    it('should not call Math.random during router.routeWithDatabase()', () => {
      Math.random = vi.fn(() => {
        mathRandomCallCount++;
        return 0.5;
      });

      const router = createShardRouter();
      router.routeWithDatabase('production', 'users');
      router.routeWithDatabase('staging', 'users');

      expect(mathRandomCallCount).toBe(0);
      expect(Math.random).not.toHaveBeenCalled();
    });

    it('should produce same results regardless of Math.random mock value', () => {
      const collection = 'deterministic_test';

      // Get baseline result with original Math.random
      const baselineResult = hashCollectionToShard(collection);

      // Mock Math.random to return 0
      Math.random = () => 0;
      const resultWith0 = hashCollectionToShard(collection);

      // Mock Math.random to return 1
      Math.random = () => 0.999999;
      const resultWith1 = hashCollectionToShard(collection);

      // Mock Math.random to return 0.5
      Math.random = () => 0.5;
      const resultWith05 = hashCollectionToShard(collection);

      // All results should be identical
      expect(resultWith0).toBe(baselineResult);
      expect(resultWith1).toBe(baselineResult);
      expect(resultWith05).toBe(baselineResult);
    });

    it('should produce same document routing regardless of Math.random mock value', () => {
      const docId = 'test-doc-id-12345';

      // Get baseline result with original Math.random
      const baselineResult = hashDocumentToShard(docId);

      // Mock Math.random to various values
      Math.random = () => 0;
      const result1 = hashDocumentToShard(docId);

      Math.random = () => 0.999999;
      const result2 = hashDocumentToShard(docId);

      Math.random = () => 0.123456789;
      const result3 = hashDocumentToShard(docId);

      expect(result1).toBe(baselineResult);
      expect(result2).toBe(baselineResult);
      expect(result3).toBe(baselineResult);
    });
  });

  describe('Even distribution: hash function produces good distribution', () => {
    it('should distribute collections evenly across all shards', () => {
      const shardCounts = new Map<number, number>();

      // Use a large sample with deterministic names
      for (let i = 0; i < 10000; i++) {
        const collectionName = `collection_${i}`;
        const shardId = hashCollectionToShard(collectionName);
        shardCounts.set(shardId, (shardCounts.get(shardId) || 0) + 1);
      }

      // All 16 shards should be used
      expect(shardCounts.size).toBe(DEFAULT_SHARD_COUNT);

      // Each shard should have roughly 625 collections (10000/16)
      const expectedPerShard = 10000 / DEFAULT_SHARD_COUNT;
      for (const [shardId, count] of shardCounts) {
        expect(shardId).toBeGreaterThanOrEqual(0);
        expect(shardId).toBeLessThan(DEFAULT_SHARD_COUNT);
        // Allow 40% variance
        expect(count).toBeGreaterThan(expectedPerShard * 0.6);
        expect(count).toBeLessThan(expectedPerShard * 1.4);
      }
    });

    it('should distribute document IDs evenly across all shards', () => {
      const shardCounts = new Map<number, number>();

      // Use deterministic document IDs
      for (let i = 0; i < 10000; i++) {
        const docId = `doc_${i.toString().padStart(8, '0')}`;
        const shardId = hashDocumentToShard(docId);
        shardCounts.set(shardId, (shardCounts.get(shardId) || 0) + 1);
      }

      // All 16 shards should be used
      expect(shardCounts.size).toBe(DEFAULT_SHARD_COUNT);

      // Each shard should have roughly 625 documents (10000/16)
      const expectedPerShard = 10000 / DEFAULT_SHARD_COUNT;
      for (const [_, count] of shardCounts) {
        expect(count).toBeGreaterThan(expectedPerShard * 0.6);
        expect(count).toBeLessThan(expectedPerShard * 1.4);
      }
    });

    it('should distribute sequential inputs without clustering', () => {
      const shardCounts = new Map<number, number>();
      const consecutiveShards: number[] = [];

      // Sequential collection names
      for (let i = 0; i < 1000; i++) {
        const collectionName = `seq_${i}`;
        const shardId = hashCollectionToShard(collectionName);
        shardCounts.set(shardId, (shardCounts.get(shardId) || 0) + 1);
        consecutiveShards.push(shardId);
      }

      // Check that consecutive inputs don't produce consecutive shards
      let consecutiveCount = 0;
      for (let i = 1; i < consecutiveShards.length; i++) {
        if (consecutiveShards[i] === consecutiveShards[i - 1]) {
          consecutiveCount++;
        }
      }

      // With random distribution, we expect ~6.25% consecutive matches (1/16)
      // Allow up to 15% to account for variance
      expect(consecutiveCount / consecutiveShards.length).toBeLessThan(0.15);
    });

    it('should produce uniform chi-squared distribution for collections', () => {
      const shardCounts = new Map<number, number>();
      const numSamples = 16000; // 1000 per shard expected

      for (let i = 0; i < numSamples; i++) {
        const collectionName = `chi_test_${i}`;
        const shardId = hashCollectionToShard(collectionName);
        shardCounts.set(shardId, (shardCounts.get(shardId) || 0) + 1);
      }

      // Calculate chi-squared statistic
      const expected = numSamples / DEFAULT_SHARD_COUNT;
      let chiSquared = 0;
      for (let i = 0; i < DEFAULT_SHARD_COUNT; i++) {
        const observed = shardCounts.get(i) || 0;
        chiSquared += Math.pow(observed - expected, 2) / expected;
      }

      // For 15 degrees of freedom (16 shards - 1), chi-squared critical value
      // at p=0.01 is approximately 30.58
      // A good hash should have chi-squared well below this
      expect(chiSquared).toBeLessThan(35);
    });
  });

  describe('Crypto randomness: verify no weak randomness in selection', () => {
    it('should not use predictable seed-based random number generation', () => {
      // If the implementation used a seeded PRNG, results might vary after "reseeding"
      // Our deterministic hash should never change
      const collection = 'seed_test_collection';
      const results = new Set<number>();

      for (let i = 0; i < 1000; i++) {
        results.add(hashCollectionToShard(collection));
      }

      // Should always return the exact same value
      expect(results.size).toBe(1);
    });

    it('should use pure function hash (no internal state)', () => {
      const router1 = createShardRouter();
      const router2 = createShardRouter();

      // Route many collections through router1 first
      for (let i = 0; i < 100; i++) {
        router1.route(`collection_${i}`);
      }

      // Router2 should produce identical results for same inputs
      // even though it hasn't "warmed up"
      for (let i = 0; i < 100; i++) {
        const name = `collection_${i}`;
        expect(router1.route(name).shardId).toBe(router2.route(name).shardId);
      }
    });

    it('should produce deterministic results even with cache cleared', () => {
      const router = createShardRouter();
      const collection = 'cache_clear_test';

      const result1 = router.route(collection).shardId;
      router.clearCache();
      const result2 = router.route(collection).shardId;
      router.clearCache();
      const result3 = router.route(collection).shardId;

      expect(result1).toBe(result2);
      expect(result2).toBe(result3);
    });
  });

  describe('Hash function quality: avalanche effect and bit distribution', () => {
    it('should produce different hashes for single character differences', () => {
      const base = 'test_collection';
      const variants = [
        'test_collectiom', // n -> m
        'test_collectio', // remove n
        'test_collectionn', // add n
        'Test_collection', // t -> T
        'test_collection1', // add 1
      ];

      const baseHash = hashCollectionToShard(base);
      let differentCount = 0;

      for (const variant of variants) {
        if (hashCollectionToShard(variant) !== baseHash) {
          differentCount++;
        }
      }

      // At least 80% should produce different shards
      // (with 16 shards, ~93.75% chance of difference for random distribution)
      expect(differentCount).toBeGreaterThanOrEqual(variants.length * 0.8);
    });

    it('should utilize all bits in shard range', () => {
      const shardsUsed = new Set<number>();

      // Generate enough inputs to statistically hit all shards
      for (let i = 0; i < 1000; i++) {
        shardsUsed.add(hashCollectionToShard(`bit_coverage_${i}`));
      }

      // All 16 shards should be covered
      expect(shardsUsed.size).toBe(DEFAULT_SHARD_COUNT);
    });

    it('should not cluster similar names to same shard', () => {
      const prefixes = ['user_', 'order_', 'product_', 'session_'];
      const results = new Map<string, Set<number>>();

      for (const prefix of prefixes) {
        results.set(prefix, new Set());
        for (let i = 0; i < 100; i++) {
          const name = `${prefix}${i}`;
          results.get(prefix)!.add(hashCollectionToShard(name));
        }
      }

      // Each prefix group should use multiple shards
      for (const [prefix, shards] of results) {
        expect(shards.size).toBeGreaterThan(10); // Should use most of the 16 shards
      }
    });
  });

  describe('Split collection routing: deterministic document distribution', () => {
    it('should route split collection documents deterministically', () => {
      const router = createShardRouter();
      router.splitCollection('hot_data', [0, 1, 2, 3]);

      const docId = 'doc-12345';
      const results = new Set<number>();

      for (let i = 0; i < 100; i++) {
        results.add(router.routeDocument('hot_data', docId).shardId);
      }

      // Should always route to the same shard
      expect(results.size).toBe(1);
    });

    it('should distribute split collection documents evenly across split shards', () => {
      const router = createShardRouter();
      const splitShards = [4, 5, 6, 7];
      router.splitCollection('events', splitShards);

      const shardCounts = new Map<number, number>();

      for (let i = 0; i < 4000; i++) {
        const docId = `event_${i}`;
        const shardId = router.routeDocument('events', docId).shardId;
        shardCounts.set(shardId, (shardCounts.get(shardId) || 0) + 1);
      }

      // Only split shards should be used
      expect(shardCounts.size).toBe(4);
      for (const shardId of shardCounts.keys()) {
        expect(splitShards).toContain(shardId);
      }

      // Each shard should have roughly 1000 documents
      for (const count of shardCounts.values()) {
        expect(count).toBeGreaterThan(700);
        expect(count).toBeLessThan(1300);
      }
    });

    it('should not use Math.random for split collection routing', () => {
      Math.random = vi.fn(() => {
        mathRandomCallCount++;
        return 0.5;
      });

      const router = createShardRouter();
      router.splitCollection('partitioned', [8, 9, 10, 11]);

      for (let i = 0; i < 100; i++) {
        router.routeDocument('partitioned', `doc_${i}`);
      }

      expect(mathRandomCallCount).toBe(0);
      expect(Math.random).not.toHaveBeenCalled();
    });
  });
});
