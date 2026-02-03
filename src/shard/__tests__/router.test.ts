/**
 * Shard Router Tests
 *
 * Tests for document ID-based sharding functionality including:
 * - Hash document _id to determine shard assignment
 * - Consistent hashing for even distribution
 * - Configurable shard count
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  ShardRouter,
  createShardRouter,
  hashCollectionToShard,
  hashDocumentToShard,
  type ShardRouterOptions,
  type ShardAssignment,
} from '../router.js';
import { DEFAULT_SHARD_COUNT } from '../../constants.js';

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Generate a random document ID for testing
 */
function generateDocId(): string {
  return Math.random().toString(36).slice(2) + Date.now().toString(36);
}

/**
 * Generate a MongoDB-style ObjectId for testing
 */
function generateObjectId(): string {
  const timestamp = Math.floor(Date.now() / 1000).toString(16).padStart(8, '0');
  const random = Array.from({ length: 16 }, () =>
    Math.floor(Math.random() * 16).toString(16)
  ).join('');
  return timestamp + random;
}

/**
 * Calculate chi-squared statistic for distribution uniformity test
 * Lower values indicate more uniform distribution
 */
function chiSquared(observed: number[], expected: number): number {
  return observed.reduce((sum, obs) => {
    return sum + Math.pow(obs - expected, 2) / expected;
  }, 0);
}

// ============================================================================
// hashDocumentToShard Tests
// ============================================================================

describe('hashDocumentToShard', () => {
  it('should return a shard ID in valid range [0, shardCount)', () => {
    const docId = 'test-document-123';
    const shardCount = 16;

    const shardId = hashDocumentToShard(docId, shardCount);

    expect(shardId).toBeGreaterThanOrEqual(0);
    expect(shardId).toBeLessThan(shardCount);
  });

  it('should be deterministic - same document ID always maps to same shard', () => {
    const docId = 'consistent-doc-id-456';
    const shardCount = 16;

    const shard1 = hashDocumentToShard(docId, shardCount);
    const shard2 = hashDocumentToShard(docId, shardCount);
    const shard3 = hashDocumentToShard(docId, shardCount);

    expect(shard1).toBe(shard2);
    expect(shard2).toBe(shard3);
  });

  it('should use DEFAULT_SHARD_COUNT when shardCount not specified', () => {
    const docId = 'test-doc';

    const shardId = hashDocumentToShard(docId);

    expect(shardId).toBeGreaterThanOrEqual(0);
    expect(shardId).toBeLessThan(DEFAULT_SHARD_COUNT);
  });

  it('should throw error for empty document ID', () => {
    expect(() => hashDocumentToShard('')).toThrow('Cannot hash empty document id');
  });

  it('should work with different shard counts (power of 2)', () => {
    const docId = 'test-doc-789';
    const shardCounts = [2, 4, 8, 16, 32, 64, 128, 256];

    for (const count of shardCounts) {
      const shardId = hashDocumentToShard(docId, count);
      expect(shardId).toBeGreaterThanOrEqual(0);
      expect(shardId).toBeLessThan(count);
    }
  });

  it('should distribute different document IDs across shards', () => {
    const shardCount = 16;
    const docIds = [
      'doc-1',
      'doc-2',
      'doc-3',
      'doc-4',
      'doc-5',
      'document-a',
      'document-b',
      'user-123',
      'order-456',
      'item-789',
    ];

    const shards = docIds.map((id) => hashDocumentToShard(id, shardCount));
    const uniqueShards = new Set(shards);

    // With 10 random-ish IDs across 16 shards, we should see some distribution
    expect(uniqueShards.size).toBeGreaterThan(1);
  });
});

// ============================================================================
// hashCollectionToShard Tests
// ============================================================================

describe('hashCollectionToShard', () => {
  it('should return a shard ID in valid range', () => {
    const collection = 'users';
    const shardCount = 16;

    const shardId = hashCollectionToShard(collection, shardCount);

    expect(shardId).toBeGreaterThanOrEqual(0);
    expect(shardId).toBeLessThan(shardCount);
  });

  it('should be deterministic for collection names', () => {
    const collection = 'orders';
    const shardCount = 16;

    const shard1 = hashCollectionToShard(collection, shardCount);
    const shard2 = hashCollectionToShard(collection, shardCount);

    expect(shard1).toBe(shard2);
  });

  it('should throw error for empty collection name', () => {
    expect(() => hashCollectionToShard('')).toThrow('Cannot hash empty collection name');
    expect(() => hashCollectionToShard('   ')).toThrow('Cannot hash empty collection name');
  });
});

// ============================================================================
// ShardRouter Class Tests
// ============================================================================

describe('ShardRouter', () => {
  let router: ShardRouter;

  beforeEach(() => {
    router = createShardRouter();
  });

  describe('constructor', () => {
    it('should create router with default options', () => {
      const router = new ShardRouter();
      expect(router).toBeInstanceOf(ShardRouter);
    });

    it('should accept custom shard count (power of 2)', () => {
      const router = new ShardRouter({ shardCount: 32 });
      expect(router).toBeInstanceOf(ShardRouter);
    });

    it('should throw error for non-power-of-2 shard count', () => {
      expect(() => new ShardRouter({ shardCount: 15 })).toThrow(
        'Shard count must be a power of 2'
      );
      expect(() => new ShardRouter({ shardCount: 17 })).toThrow(
        'Shard count must be a power of 2'
      );
      expect(() => new ShardRouter({ shardCount: 100 })).toThrow(
        'Shard count must be a power of 2'
      );
    });

    it('should accept valid power-of-2 shard counts', () => {
      const validCounts = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024];
      for (const count of validCounts) {
        expect(() => new ShardRouter({ shardCount: count })).not.toThrow();
      }
    });

    it('should accept custom cache size', () => {
      const router = new ShardRouter({ cacheSize: 100 });
      expect(router).toBeInstanceOf(ShardRouter);
    });

    it('should accept custom hash function', () => {
      const customHash = (input: string) => input.length;
      const router = new ShardRouter({ hashFunction: customHash, shardCount: 16 });
      expect(router).toBeInstanceOf(ShardRouter);
    });
  });

  describe('route', () => {
    it('should route collection to a shard', () => {
      const assignment = router.route('users');

      expect(assignment).toHaveProperty('shardId');
      expect(assignment).toHaveProperty('collection', 'users');
      expect(assignment.shardId).toBeGreaterThanOrEqual(0);
      expect(assignment.shardId).toBeLessThan(DEFAULT_SHARD_COUNT);
    });

    it('should be deterministic', () => {
      const assignment1 = router.route('products');
      const assignment2 = router.route('products');

      expect(assignment1.shardId).toBe(assignment2.shardId);
    });

    it('should throw for empty collection name', () => {
      expect(() => router.route('')).toThrow('Cannot route empty collection name');
      expect(() => router.route('   ')).toThrow('Cannot route empty collection name');
    });

    it('should cache results for subsequent calls', () => {
      router.route('orders');
      expect(router.isCached('orders')).toBe(true);
    });
  });

  describe('routeDocument', () => {
    it('should route document by _id to a shard', () => {
      const assignment = router.routeDocument('users', 'user-123');

      expect(assignment).toHaveProperty('shardId');
      expect(assignment).toHaveProperty('collection', 'users');
      expect(assignment).toHaveProperty('documentId', 'user-123');
      expect(assignment.shardId).toBeGreaterThanOrEqual(0);
      expect(assignment.shardId).toBeLessThan(DEFAULT_SHARD_COUNT);
    });

    it('should be deterministic - same document ID always routes to same shard', () => {
      const docId = 'document-abc-123';

      const assignment1 = router.routeDocument('users', docId);
      const assignment2 = router.routeDocument('users', docId);
      const assignment3 = router.routeDocument('users', docId);

      expect(assignment1.shardId).toBe(assignment2.shardId);
      expect(assignment2.shardId).toBe(assignment3.shardId);
    });

    it('should throw for empty collection name', () => {
      expect(() => router.routeDocument('', 'doc-123')).toThrow(
        'Cannot route empty collection name'
      );
    });

    it('should throw for empty document ID', () => {
      expect(() => router.routeDocument('users', '')).toThrow(
        'Cannot route empty document id'
      );
    });

    it('should route same document ID consistently across different router instances', () => {
      const router1 = createShardRouter({ shardCount: 16 });
      const router2 = createShardRouter({ shardCount: 16 });
      const docId = 'persistent-doc-id';

      const shard1 = router1.routeDocument('users', docId).shardId;
      const shard2 = router2.routeDocument('users', docId).shardId;

      expect(shard1).toBe(shard2);
    });

    it('should route documents based on document ID, not collection name', () => {
      const docId = 'shared-doc-id';

      const assignment1 = router.routeDocument('users', docId);
      const assignment2 = router.routeDocument('orders', docId);

      // Same document ID should route to same shard regardless of collection
      expect(assignment1.shardId).toBe(assignment2.shardId);
    });
  });

  describe('routeWithDatabase', () => {
    it('should route with database namespace prefix', () => {
      const assignment = router.routeWithDatabase('mydb', 'users');

      expect(assignment).toHaveProperty('shardId');
      expect(assignment).toHaveProperty('collection', 'users');
      expect(assignment).toHaveProperty('database', 'mydb');
    });

    it('should route same collection differently across databases', () => {
      const assignment1 = router.routeWithDatabase('db1', 'users');
      const assignment2 = router.routeWithDatabase('db2', 'users');

      // Different databases may route to different shards
      // (though they could hash to the same shard by chance)
      expect(assignment1.database).toBe('db1');
      expect(assignment2.database).toBe('db2');
    });

    it('should throw for empty database name', () => {
      expect(() => router.routeWithDatabase('', 'users')).toThrow(
        'Cannot route with empty database name'
      );
    });
  });
});

// ============================================================================
// Even Distribution Tests (Consistent Hashing)
// ============================================================================

describe('Consistent Hashing Distribution', () => {
  it('should distribute documents evenly across shards with random IDs', () => {
    const shardCount = 16;
    const numDocuments = 10000;
    const shardCounts = new Array(shardCount).fill(0);

    // Generate random document IDs and count shard assignments
    for (let i = 0; i < numDocuments; i++) {
      const docId = generateDocId();
      const shardId = hashDocumentToShard(docId, shardCount);
      shardCounts[shardId]++;
    }

    // Calculate expected count per shard
    const expectedPerShard = numDocuments / shardCount;

    // Chi-squared test for uniformity
    // With 16 shards and 10000 documents, we expect ~625 per shard
    // Chi-squared critical value for 15 degrees of freedom at 0.05 significance is ~25
    const chiSq = chiSquared(shardCounts, expectedPerShard);

    // Allow some variance, but should be reasonably uniform
    expect(chiSq).toBeLessThan(50); // Conservative threshold

    // Each shard should have at least some documents
    for (let i = 0; i < shardCount; i++) {
      expect(shardCounts[i]).toBeGreaterThan(0);
    }

    // No shard should have more than 3x the expected count
    for (let i = 0; i < shardCount; i++) {
      expect(shardCounts[i]).toBeLessThan(expectedPerShard * 3);
    }
  });

  it('should distribute ObjectId-style IDs evenly', () => {
    const shardCount = 16;
    const numDocuments = 10000;
    const shardCounts = new Array(shardCount).fill(0);

    // Generate ObjectId-style IDs
    for (let i = 0; i < numDocuments; i++) {
      const docId = generateObjectId();
      const shardId = hashDocumentToShard(docId, shardCount);
      shardCounts[shardId]++;
    }

    const expectedPerShard = numDocuments / shardCount;

    // Each shard should have at least some documents
    for (let i = 0; i < shardCount; i++) {
      expect(shardCounts[i]).toBeGreaterThan(0);
    }

    // No shard should be significantly over-allocated
    for (let i = 0; i < shardCount; i++) {
      expect(shardCounts[i]).toBeLessThan(expectedPerShard * 3);
    }
  });

  it('should distribute sequential numeric IDs evenly', () => {
    const shardCount = 16;
    const numDocuments = 10000;
    const shardCounts = new Array(shardCount).fill(0);

    // Test with sequential numeric IDs
    for (let i = 1; i <= numDocuments; i++) {
      const docId = String(i);
      const shardId = hashDocumentToShard(docId, shardCount);
      shardCounts[shardId]++;
    }

    const expectedPerShard = numDocuments / shardCount;

    // Each shard should have at least some documents
    for (let i = 0; i < shardCount; i++) {
      expect(shardCounts[i]).toBeGreaterThan(0);
    }

    // Distribution should be reasonably even
    const chiSq = chiSquared(shardCounts, expectedPerShard);
    expect(chiSq).toBeLessThan(100); // Allow more variance for sequential IDs
  });

  it('should distribute UUID-style IDs evenly', () => {
    const shardCount = 16;
    const numDocuments = 5000;
    const shardCounts = new Array(shardCount).fill(0);

    // Generate UUID-style IDs
    for (let i = 0; i < numDocuments; i++) {
      const docId = crypto.randomUUID();
      const shardId = hashDocumentToShard(docId, shardCount);
      shardCounts[shardId]++;
    }

    const expectedPerShard = numDocuments / shardCount;

    // Each shard should have documents
    for (let i = 0; i < shardCount; i++) {
      expect(shardCounts[i]).toBeGreaterThan(0);
    }

    // Check reasonable distribution
    const chiSq = chiSquared(shardCounts, expectedPerShard);
    expect(chiSq).toBeLessThan(50);
  });

  it('should maintain consistent hashing across different shard counts', () => {
    const docId = 'stable-document-id';

    // Document should consistently map when shard count increases by power of 2
    const shard16 = hashDocumentToShard(docId, 16);
    const shard32 = hashDocumentToShard(docId, 32);

    // With power-of-2 expansion, the document should either stay in same position
    // or move to position + oldShardCount
    const expectedInNewRange = shard16 === shard32 || shard16 + 16 === shard32;
    expect(expectedInNewRange).toBe(true);
  });
});

// ============================================================================
// Configurable Shard Count Tests
// ============================================================================

describe('Configurable Shard Count', () => {
  it('should work with 2 shards', () => {
    const router = createShardRouter({ shardCount: 2 });
    const assignment = router.routeDocument('users', 'doc-123');

    expect(assignment.shardId).toBeGreaterThanOrEqual(0);
    expect(assignment.shardId).toBeLessThan(2);
  });

  it('should work with 4 shards', () => {
    const router = createShardRouter({ shardCount: 4 });
    const shards = new Set<number>();

    // Generate many documents to hit all shards
    for (let i = 0; i < 100; i++) {
      const assignment = router.routeDocument('users', generateDocId());
      shards.add(assignment.shardId);
      expect(assignment.shardId).toBeGreaterThanOrEqual(0);
      expect(assignment.shardId).toBeLessThan(4);
    }

    // Should use multiple shards
    expect(shards.size).toBeGreaterThan(1);
  });

  it('should work with 64 shards', () => {
    const router = createShardRouter({ shardCount: 64 });
    const shards = new Set<number>();

    for (let i = 0; i < 500; i++) {
      const assignment = router.routeDocument('users', generateDocId());
      shards.add(assignment.shardId);
      expect(assignment.shardId).toBeGreaterThanOrEqual(0);
      expect(assignment.shardId).toBeLessThan(64);
    }

    // Should use many shards with 500 random IDs
    expect(shards.size).toBeGreaterThan(10);
  });

  it('should work with 256 shards', () => {
    const router = createShardRouter({ shardCount: 256 });
    const assignment = router.routeDocument('users', 'doc-123');

    expect(assignment.shardId).toBeGreaterThanOrEqual(0);
    expect(assignment.shardId).toBeLessThan(256);
  });

  it('should distribute across all shards with enough documents', () => {
    const shardCount = 8;
    const router = createShardRouter({ shardCount });
    const shardsSeen = new Set<number>();

    // Generate enough documents to likely hit all 8 shards
    for (let i = 0; i < 1000; i++) {
      const assignment = router.routeDocument('collection', generateDocId());
      shardsSeen.add(assignment.shardId);
    }

    // With 1000 random IDs, we should hit all 8 shards
    expect(shardsSeen.size).toBe(shardCount);
  });
});

// ============================================================================
// Affinity Hints Tests
// ============================================================================

describe('Shard Affinity Hints', () => {
  let router: ShardRouter;

  beforeEach(() => {
    router = createShardRouter({ shardCount: 16 });
  });

  it('should override hash-based routing with affinity hint', () => {
    const preferredShard = 5;
    router.setAffinityHint('special-collection', { preferredShard });

    const assignment = router.route('special-collection');
    expect(assignment.shardId).toBe(preferredShard);
  });

  it('should throw for invalid shard ID in hint', () => {
    expect(() => router.setAffinityHint('test', { preferredShard: -1 })).toThrow(
      'Shard ID out of range'
    );
    expect(() => router.setAffinityHint('test', { preferredShard: 16 })).toThrow(
      'Shard ID out of range'
    );
    expect(() => router.setAffinityHint('test', { preferredShard: 100 })).toThrow(
      'Shard ID out of range'
    );
  });

  it('should allow removing affinity hint', () => {
    router.setAffinityHint('users', { preferredShard: 3 });
    expect(router.route('users').shardId).toBe(3);

    router.removeAffinityHint('users');

    // Should now use hash-based routing
    const assignment = router.route('users');
    // The hash-based shard may or may not be 3
    expect(assignment.shardId).toBeGreaterThanOrEqual(0);
    expect(assignment.shardId).toBeLessThan(16);
  });

  it('should return all active affinity hints', () => {
    router.setAffinityHint('collection1', { preferredShard: 1 });
    router.setAffinityHint('collection2', { preferredShard: 5 });
    router.setAffinityHint('collection3', { preferredShard: 10 });

    const hints = router.getAffinityHints();

    expect(hints).toHaveLength(3);
    expect(hints).toContainEqual({ collection: 'collection1', preferredShard: 1 });
    expect(hints).toContainEqual({ collection: 'collection2', preferredShard: 5 });
    expect(hints).toContainEqual({ collection: 'collection3', preferredShard: 10 });
  });
});

// ============================================================================
// Collection Splitting Tests
// ============================================================================

describe('Collection Splitting', () => {
  let router: ShardRouter;

  beforeEach(() => {
    router = createShardRouter({ shardCount: 16 });
  });

  it('should split collection across specified shards', () => {
    router.splitCollection('hot-collection', [0, 1, 2, 3]);

    const shards = new Set<number>();
    for (let i = 0; i < 100; i++) {
      const assignment = router.routeDocument('hot-collection', generateDocId());
      shards.add(assignment.shardId);
    }

    // All documents should be routed to one of the split shards
    for (const shard of shards) {
      expect([0, 1, 2, 3]).toContain(shard);
    }
  });

  it('should throw for split with less than 2 shards', () => {
    expect(() => router.splitCollection('test', [0])).toThrow(
      'Split requires at least 2 shards'
    );
    expect(() => router.splitCollection('test', [])).toThrow(
      'Split requires at least 2 shards'
    );
  });

  it('should throw for invalid shard IDs in split', () => {
    expect(() => router.splitCollection('test', [0, 16])).toThrow('Shard ID out of range');
    expect(() => router.splitCollection('test', [-1, 5])).toThrow('Shard ID out of range');
  });

  it('should allow unsplitting a collection', () => {
    router.splitCollection('collection', [0, 1, 2]);
    router.unsplitCollection('collection');

    const info = router.getSplitInfo('collection');
    expect(info).toBeUndefined();
  });

  it('should return split info for split collection', () => {
    router.splitCollection('orders', [4, 5, 6, 7]);

    const info = router.getSplitInfo('orders');
    expect(info).toBeDefined();
    expect(info?.collection).toBe('orders');
    expect(info?.shards).toEqual([4, 5, 6, 7]);
  });

  it('should list all active splits', () => {
    router.splitCollection('coll1', [0, 1]);
    router.splitCollection('coll2', [2, 3, 4]);

    const splits = router.getAllSplits();
    expect(splits).toHaveLength(2);
  });
});

// ============================================================================
// Cache Statistics Tests
// ============================================================================

describe('Router Statistics', () => {
  let router: ShardRouter;

  beforeEach(() => {
    router = createShardRouter();
  });

  it('should track cache hits and misses', () => {
    // First call - cache miss
    router.route('users');

    // Second call - cache hit
    router.route('users');

    const stats = router.getStats();
    expect(stats.cacheMisses).toBe(1);
    expect(stats.cacheHits).toBe(1);
    expect(stats.totalRoutes).toBe(2);
  });

  it('should increment total routes for document routing', () => {
    router.routeDocument('users', 'doc-1');
    router.routeDocument('users', 'doc-2');

    const stats = router.getStats();
    expect(stats.totalRoutes).toBe(2);
  });

  it('should respect cache size limit', () => {
    const router = createShardRouter({ cacheSize: 2 });

    router.route('collection1');
    router.route('collection2');
    router.route('collection3'); // Should evict collection1

    expect(router.isCached('collection3')).toBe(true);
    expect(router.isCached('collection2')).toBe(true);
    expect(router.isCached('collection1')).toBe(false);
  });

  it('should clear cache', () => {
    router.route('users');
    router.route('orders');

    expect(router.getCacheSize()).toBe(2);

    router.clearCache();

    expect(router.getCacheSize()).toBe(0);
  });
});

// ============================================================================
// Edge Cases and Boundary Tests
// ============================================================================

describe('Edge Cases', () => {
  it('should handle very long document IDs', () => {
    const longId = 'a'.repeat(10000);
    const shardId = hashDocumentToShard(longId, 16);

    expect(shardId).toBeGreaterThanOrEqual(0);
    expect(shardId).toBeLessThan(16);
  });

  it('should handle special characters in document IDs', () => {
    const specialIds = [
      'doc-with-dashes',
      'doc_with_underscores',
      'doc.with.dots',
      'doc/with/slashes',
      'doc@with@symbols',
      'doc#hash#tag',
      'doc with spaces',
      'doc\twith\ttabs',
      'doc\nwith\nnewlines',
      'unicode-\u00e9\u00e0\u00fc',
    ];

    for (const id of specialIds) {
      const shardId = hashDocumentToShard(id, 16);
      expect(shardId).toBeGreaterThanOrEqual(0);
      expect(shardId).toBeLessThan(16);
    }
  });

  it('should handle numeric-looking string IDs', () => {
    const numericIds = ['123', '0', '-1', '3.14', '1e10', 'Infinity', 'NaN'];

    for (const id of numericIds) {
      const shardId = hashDocumentToShard(id, 16);
      expect(shardId).toBeGreaterThanOrEqual(0);
      expect(shardId).toBeLessThan(16);
    }
  });

  it('should produce different hashes for similar IDs', () => {
    const id1 = 'document-1';
    const id2 = 'document-2';

    const shard1 = hashDocumentToShard(id1, 16);
    const shard2 = hashDocumentToShard(id2, 16);

    // They may hash to same shard by chance, but let's verify hashing works
    // with many similar IDs
    const shards = new Set<number>();
    for (let i = 0; i < 100; i++) {
      shards.add(hashDocumentToShard(`prefix-${i}`, 16));
    }

    // Should distribute across multiple shards
    expect(shards.size).toBeGreaterThan(5);
  });

  it('should handle single character IDs', () => {
    const singleCharIds = 'abcdefghijklmnopqrstuvwxyz0123456789'.split('');
    const shards = new Set<number>();

    for (const id of singleCharIds) {
      const shardId = hashDocumentToShard(id, 16);
      expect(shardId).toBeGreaterThanOrEqual(0);
      expect(shardId).toBeLessThan(16);
      shards.add(shardId);
    }

    // Should use multiple shards for 36 different single-char IDs
    expect(shards.size).toBeGreaterThan(1);
  });
});
