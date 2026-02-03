/**
 * Shard Routing Error Scenario Tests
 *
 * Comprehensive tests for shard routing error handling:
 * - Invalid shard configuration errors
 * - Routing failures with empty/invalid inputs
 * - Shard affinity hint validation
 * - Split collection validation errors
 * - Cache overflow scenarios
 * - Hash function failures
 *
 * These tests verify that shard routing errors are properly
 * handled with informative error messages.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  ShardRouter,
  createShardRouter,
  hashCollectionToShard,
  hashDocumentToShard,
} from '../../../src/shard/router.js';
import { MongoLakeError } from '../../../src/errors/index.js';

// ============================================================================
// Invalid Shard Configuration Tests
// ============================================================================

describe('Shard Routing - Invalid Configuration Errors', () => {
  it('should throw error for non-power-of-2 shard count', () => {
    expect(() => createShardRouter({ shardCount: 3 })).toThrow(/power of 2/i);
    expect(() => createShardRouter({ shardCount: 5 })).toThrow(/power of 2/i);
    expect(() => createShardRouter({ shardCount: 10 })).toThrow(/power of 2/i);
    expect(() => createShardRouter({ shardCount: 17 })).toThrow(/power of 2/i);
  });

  it('should throw error for zero shard count', () => {
    expect(() => createShardRouter({ shardCount: 0 })).toThrow();
  });

  it('should throw error for negative shard count', () => {
    expect(() => createShardRouter({ shardCount: -1 })).toThrow();
    expect(() => createShardRouter({ shardCount: -16 })).toThrow();
  });

  it('should include invalid value in error message', () => {
    try {
      createShardRouter({ shardCount: 7 });
    } catch (error) {
      expect(error).toBeInstanceOf(MongoLakeError);
      expect((error as MongoLakeError).message).toContain('7');
    }
  });

  it('should accept valid power-of-2 shard counts', () => {
    expect(() => createShardRouter({ shardCount: 1 })).not.toThrow();
    expect(() => createShardRouter({ shardCount: 2 })).not.toThrow();
    expect(() => createShardRouter({ shardCount: 4 })).not.toThrow();
    expect(() => createShardRouter({ shardCount: 8 })).not.toThrow();
    expect(() => createShardRouter({ shardCount: 16 })).not.toThrow();
    expect(() => createShardRouter({ shardCount: 32 })).not.toThrow();
    expect(() => createShardRouter({ shardCount: 64 })).not.toThrow();
  });
});

// ============================================================================
// Empty and Invalid Input Tests
// ============================================================================

describe('Shard Routing - Empty/Invalid Input Errors', () => {
  let router: ShardRouter;

  beforeEach(() => {
    router = createShardRouter();
  });

  describe('collection routing errors', () => {
    it('should throw error for empty collection name', () => {
      expect(() => router.route('')).toThrow(/cannot.*empty/i);
    });

    it('should throw error for whitespace-only collection name', () => {
      expect(() => router.route('   ')).toThrow(/cannot.*empty/i);
      expect(() => router.route('\t\n')).toThrow(/cannot.*empty/i);
      expect(() => router.route('  \t  ')).toThrow(/cannot.*empty/i);
    });

    it('should provide MongoLakeError for invalid collection', () => {
      try {
        router.route('');
      } catch (error) {
        expect(error).toBeInstanceOf(MongoLakeError);
        expect((error as MongoLakeError).code).toBeDefined();
      }
    });
  });

  describe('database-prefixed routing errors', () => {
    it('should throw error for empty database name', () => {
      expect(() => router.routeWithDatabase('', 'collection')).toThrow(/cannot.*empty/i);
    });

    it('should throw error for empty collection name with database', () => {
      expect(() => router.routeWithDatabase('db', '')).toThrow(/cannot.*empty/i);
    });

    it('should throw error for whitespace-only database name', () => {
      expect(() => router.routeWithDatabase('   ', 'collection')).toThrow(/cannot.*empty/i);
    });

    it('should throw error for both empty', () => {
      expect(() => router.routeWithDatabase('', '')).toThrow(/cannot.*empty/i);
    });
  });

  describe('document routing errors', () => {
    it('should throw error for empty collection in document routing', () => {
      expect(() => router.routeDocument('', 'doc123')).toThrow(/cannot.*empty/i);
    });

    it('should throw error for empty document ID', () => {
      expect(() => router.routeDocument('collection', '')).toThrow(/cannot.*empty/i);
    });

    it('should throw error for whitespace-only collection', () => {
      expect(() => router.routeDocument('   ', 'doc123')).toThrow(/cannot.*empty/i);
    });
  });

  describe('hash function input errors', () => {
    it('should throw error for empty collection hash', () => {
      expect(() => hashCollectionToShard('')).toThrow(/cannot.*empty/i);
    });

    it('should throw error for empty document ID hash', () => {
      expect(() => hashDocumentToShard('')).toThrow(/cannot.*empty/i);
    });
  });
});

// ============================================================================
// Affinity Hint Validation Errors
// ============================================================================

describe('Shard Routing - Affinity Hint Validation Errors', () => {
  let router: ShardRouter;

  beforeEach(() => {
    router = createShardRouter({ shardCount: 16 });
  });

  it('should throw error for shard ID below range', () => {
    expect(() => {
      router.setAffinityHint('collection', { preferredShard: -1 });
    }).toThrow(/shard.*range/i);
  });

  it('should throw error for shard ID above range', () => {
    expect(() => {
      router.setAffinityHint('collection', { preferredShard: 16 });
    }).toThrow(/shard.*range/i);
  });

  it('should throw error for way-out-of-range shard ID', () => {
    expect(() => {
      router.setAffinityHint('collection', { preferredShard: 100 });
    }).toThrow(/shard.*range/i);

    expect(() => {
      router.setAffinityHint('collection', { preferredShard: 1000 });
    }).toThrow(/shard.*range/i);
  });

  it('should include shard ID in error message', () => {
    try {
      router.setAffinityHint('collection', { preferredShard: 20 });
    } catch (error) {
      expect(error).toBeInstanceOf(MongoLakeError);
      expect((error as MongoLakeError).message).toContain('20');
    }
  });

  it('should include valid range in error message', () => {
    try {
      router.setAffinityHint('collection', { preferredShard: 20 });
    } catch (error) {
      expect(error).toBeInstanceOf(MongoLakeError);
      const msg = (error as MongoLakeError).message;
      expect(msg).toContain('0');
      expect(msg).toContain('15');
    }
  });

  it('should accept valid shard IDs at boundaries', () => {
    expect(() => router.setAffinityHint('c1', { preferredShard: 0 })).not.toThrow();
    expect(() => router.setAffinityHint('c2', { preferredShard: 15 })).not.toThrow();
  });

  it('should respect custom shard count in range validation', () => {
    const router8 = createShardRouter({ shardCount: 8 });

    expect(() => router8.setAffinityHint('c', { preferredShard: 7 })).not.toThrow();
    expect(() => router8.setAffinityHint('c', { preferredShard: 8 })).toThrow(/shard.*range/i);
  });
});

// ============================================================================
// Split Collection Validation Errors
// ============================================================================

describe('Shard Routing - Split Collection Validation Errors', () => {
  let router: ShardRouter;

  beforeEach(() => {
    router = createShardRouter({ shardCount: 16 });
  });

  it('should throw error for single shard split', () => {
    expect(() => {
      router.splitCollection('collection', [5]);
    }).toThrow(/at least 2/i);
  });

  it('should throw error for empty shard array', () => {
    expect(() => {
      router.splitCollection('collection', []);
    }).toThrow(/at least 2/i);
  });

  it('should throw error for shard ID out of range in split', () => {
    expect(() => {
      router.splitCollection('collection', [0, 1, 20]);
    }).toThrow(/shard.*range/i);
  });

  it('should throw error for negative shard ID in split', () => {
    expect(() => {
      router.splitCollection('collection', [-1, 0, 1]);
    }).toThrow(/shard.*range/i);
  });

  it('should throw error for all out-of-range shards', () => {
    expect(() => {
      router.splitCollection('collection', [16, 17, 18]);
    }).toThrow(/shard.*range/i);
  });

  it('should include invalid shard ID in error message', () => {
    try {
      router.splitCollection('collection', [0, 1, 25]);
    } catch (error) {
      expect(error).toBeInstanceOf(MongoLakeError);
      expect((error as MongoLakeError).message).toContain('25');
    }
  });

  it('should accept valid split configurations', () => {
    expect(() => router.splitCollection('c1', [0, 1])).not.toThrow();
    expect(() => router.splitCollection('c2', [0, 1, 2, 3])).not.toThrow();
    expect(() => router.splitCollection('c3', [0, 5, 10, 15])).not.toThrow();
  });

  it('should respect custom shard count in split validation', () => {
    const router8 = createShardRouter({ shardCount: 8 });

    expect(() => router8.splitCollection('c', [0, 7])).not.toThrow();
    expect(() => router8.splitCollection('c', [0, 8])).toThrow(/shard.*range/i);
  });
});

// ============================================================================
// Custom Hash Function Error Tests
// ============================================================================

describe('Shard Routing - Custom Hash Function Errors', () => {
  it('should handle hash function that throws', () => {
    const throwingHash = () => {
      throw new Error('Hash computation failed');
    };

    const router = createShardRouter({ hashFunction: throwingHash });

    expect(() => router.route('collection')).toThrow('Hash computation failed');
  });

  it('should handle hash function returning NaN', () => {
    const nanHash = () => NaN;
    const router = createShardRouter({ hashFunction: nanHash });

    // NaN % n is NaN, so the shard ID would be NaN
    const assignment = router.route('collection');
    expect(Number.isNaN(assignment.shardId)).toBe(true);
  });

  it('should handle hash function returning negative', () => {
    const negativeHash = () => -5;
    const router = createShardRouter({ hashFunction: negativeHash });

    // Negative modulo behavior in JS
    const assignment = router.route('collection');
    // -5 % 16 = -5 in JS
    expect(assignment.shardId).toBe(-5);
  });

  it('should handle hash function returning very large number', () => {
    const largeHash = () => Number.MAX_SAFE_INTEGER;
    const router = createShardRouter({ hashFunction: largeHash });

    const assignment = router.route('collection');
    expect(assignment.shardId).toBeGreaterThanOrEqual(0);
    expect(assignment.shardId).toBeLessThan(16);
  });
});

// ============================================================================
// Concurrent Access Error Scenarios
// ============================================================================

describe('Shard Routing - Concurrent Access Scenarios', () => {
  it('should handle rapid consecutive routes without errors', () => {
    const router = createShardRouter();

    for (let i = 0; i < 10000; i++) {
      expect(() => router.route(`collection_${i % 100}`)).not.toThrow();
    }
  });

  it('should handle parallel route calls', async () => {
    const router = createShardRouter();
    const promises: Promise<void>[] = [];

    for (let i = 0; i < 1000; i++) {
      promises.push(
        Promise.resolve().then(() => {
          router.route(`collection_${i}`);
        })
      );
    }

    await expect(Promise.all(promises)).resolves.not.toThrow();
  });

  it('should handle mixed operations under load', async () => {
    const router = createShardRouter({ cacheSize: 100 });
    const operations: Promise<void>[] = [];

    for (let i = 0; i < 500; i++) {
      operations.push(
        Promise.resolve().then(() => {
          // Mix of different operations
          switch (i % 5) {
            case 0:
              router.route(`collection_${i}`);
              break;
            case 1:
              router.routeWithDatabase(`db_${i % 10}`, `collection_${i}`);
              break;
            case 2:
              router.routeDocument(`collection_${i % 20}`, `doc_${i}`);
              break;
            case 3:
              if (i % 10 === 3) router.clearCache();
              break;
            case 4:
              router.getStats();
              break;
          }
        })
      );
    }

    await expect(Promise.all(operations)).resolves.not.toThrow();
  });
});

// ============================================================================
// Cache Behavior Error Scenarios
// ============================================================================

describe('Shard Routing - Cache Behavior Edge Cases', () => {
  it('should handle cache size of 1', () => {
    const router = createShardRouter({ cacheSize: 1 });

    router.route('collection1');
    router.route('collection2');
    router.route('collection3');

    expect(router.getCacheSize()).toBe(1);
  });

  it('should handle cache clear during routing', () => {
    const router = createShardRouter();

    router.route('collection1');
    expect(router.isCached('collection1')).toBe(true);

    router.clearCache();
    expect(router.isCached('collection1')).toBe(false);

    // Should work fine after clear
    const assignment = router.route('collection1');
    expect(assignment.shardId).toBeGreaterThanOrEqual(0);
  });

  it('should handle affinity hint invalidating cache', () => {
    const router = createShardRouter();

    const assignment1 = router.route('collection');
    expect(router.isCached('collection')).toBe(true);

    // Set affinity hint should invalidate cache
    router.setAffinityHint('collection', { preferredShard: 5 });
    // Cache should still exist but with new value after next route
    const assignment2 = router.route('collection');

    expect(assignment2.shardId).toBe(5);
  });
});

// ============================================================================
// Error Recovery Scenarios
// ============================================================================

describe('Shard Routing - Error Recovery', () => {
  it('should recover after invalid input error', () => {
    const router = createShardRouter();

    // Trigger error
    expect(() => router.route('')).toThrow();

    // Should still work for valid input
    const assignment = router.route('valid_collection');
    expect(assignment.shardId).toBeGreaterThanOrEqual(0);
  });

  it('should recover after invalid affinity hint', () => {
    const router = createShardRouter();

    // Trigger error
    expect(() => router.setAffinityHint('c', { preferredShard: 100 })).toThrow();

    // Should still work
    router.setAffinityHint('c', { preferredShard: 5 });
    expect(router.route('c').shardId).toBe(5);
  });

  it('should recover after invalid split', () => {
    const router = createShardRouter();

    // Trigger error
    expect(() => router.splitCollection('c', [100, 101])).toThrow();

    // Should still work
    router.splitCollection('c', [0, 1, 2, 3]);
    expect(router.getSplitInfo('c')).toBeDefined();
  });
});

// ============================================================================
// Special Character Handling in Errors
// ============================================================================

describe('Shard Routing - Special Character Edge Cases', () => {
  let router: ShardRouter;

  beforeEach(() => {
    router = createShardRouter();
  });

  it('should handle null bytes in collection name', () => {
    const nameWithNull = 'collection\x00name';
    // Should not throw - null bytes are valid characters
    const assignment = router.route(nameWithNull);
    expect(assignment.shardId).toBeGreaterThanOrEqual(0);
  });

  it('should handle very long collection names', () => {
    const longName = 'a'.repeat(100000);
    // Should not throw even for very long names
    const assignment = router.route(longName);
    expect(assignment.shardId).toBeGreaterThanOrEqual(0);
    expect(assignment.shardId).toBeLessThan(16);
  });

  it('should handle unicode in collection names', () => {
    const unicodeNames = [
      'коллекция',
      'コレクション',
      '集合',
      'مجموعة',
      'אוסף',
    ];

    for (const name of unicodeNames) {
      expect(() => router.route(name)).not.toThrow();
    }
  });
});
