/**
 * Shard Coordination Integration Tests
 *
 * Tests cross-shard query routing and coordination across multiple shards.
 * Validates that the shard router correctly distributes operations and
 * aggregates results from multiple shards.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  ShardRouter,
  createShardRouter,
  hashCollectionToShard,
} from '../../src/shard/router.js';
import { MemoryStorage } from '../../src/storage/index.js';
import { MongoLake, Collection, Database } from '../../src/client/index.js';
import { createUser, createUsers, createProduct, resetDocumentCounter } from '../utils/factories.js';

// ============================================================================
// Test Types
// ============================================================================

interface TestDocument {
  _id: string;
  name: string;
  shardKey?: string;
  category?: string;
  value?: number;
  [key: string]: unknown;
}

interface ShardState {
  shardId: number;
  storage: MemoryStorage;
  documents: TestDocument[];
}

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Create a multi-shard test environment
 */
function createShardedEnvironment(shardCount: number = 4): {
  router: ShardRouter;
  shards: Map<number, ShardState>;
  cleanup: () => void;
} {
  const router = createShardRouter({ shardCount });
  const shards = new Map<number, ShardState>();

  // Initialize shards
  for (let i = 0; i < shardCount; i++) {
    shards.set(i, {
      shardId: i,
      storage: new MemoryStorage(),
      documents: [],
    });
  }

  return {
    router,
    shards,
    cleanup: () => {
      shards.forEach((shard) => shard.storage.clear());
      shards.clear();
    },
  };
}

/**
 * Distribute a document to the appropriate shard based on its _id
 */
function distributeDocument(
  router: ShardRouter,
  shards: Map<number, ShardState>,
  collection: string,
  document: TestDocument
): number {
  const assignment = router.routeDocument(collection, document._id);
  const shard = shards.get(assignment.shardId);
  if (shard) {
    shard.documents.push(document);
  }
  return assignment.shardId;
}

/**
 * Query all shards and aggregate results
 */
function queryAllShards<T extends TestDocument>(
  shards: Map<number, ShardState>,
  predicate: (doc: T) => boolean
): T[] {
  const results: T[] = [];
  shards.forEach((shard) => {
    results.push(...(shard.documents.filter(predicate) as T[]));
  });
  return results;
}

// ============================================================================
// Basic Shard Routing Tests
// ============================================================================

describe('Shard Coordination - Basic Routing', () => {
  let router: ShardRouter;
  let shards: Map<number, ShardState>;
  let cleanup: () => void;

  beforeEach(() => {
    resetDocumentCounter();
    const env = createShardedEnvironment(4);
    router = env.router;
    shards = env.shards;
    cleanup = env.cleanup;
  });

  afterEach(() => {
    cleanup();
  });

  it('should route documents to appropriate shards based on _id', () => {
    const documents: TestDocument[] = [
      { _id: 'doc-1', name: 'Document 1' },
      { _id: 'doc-2', name: 'Document 2' },
      { _id: 'doc-3', name: 'Document 3' },
      { _id: 'doc-4', name: 'Document 4' },
    ];

    const assignments: number[] = [];
    for (const doc of documents) {
      const shardId = distributeDocument(router, shards, 'test', doc);
      assignments.push(shardId);
    }

    // All assignments should be valid shard IDs
    for (const shardId of assignments) {
      expect(shardId).toBeGreaterThanOrEqual(0);
      expect(shardId).toBeLessThan(4);
    }

    // Documents should be distributed to shards
    const totalDocs = Array.from(shards.values()).reduce(
      (sum, shard) => sum + shard.documents.length,
      0
    );
    expect(totalDocs).toBe(4);
  });

  it('should consistently route same document to same shard', () => {
    const docId = 'consistent-doc-id';

    const assignment1 = router.routeDocument('test', docId);
    const assignment2 = router.routeDocument('test', docId);
    const assignment3 = router.routeDocument('test', docId);

    expect(assignment1.shardId).toBe(assignment2.shardId);
    expect(assignment2.shardId).toBe(assignment3.shardId);
  });

  it('should distribute many documents across all shards', () => {
    const documents: TestDocument[] = [];
    for (let i = 0; i < 100; i++) {
      documents.push({ _id: `doc-${i}-${Math.random().toString(36)}`, name: `Document ${i}` });
    }

    for (const doc of documents) {
      distributeDocument(router, shards, 'test', doc);
    }

    // All shards should have some documents
    const shardCounts = Array.from(shards.values()).map((s) => s.documents.length);

    // With 100 docs across 4 shards, each should have at least some
    for (const count of shardCounts) {
      expect(count).toBeGreaterThan(0);
    }

    // Total should be 100
    const total = shardCounts.reduce((a, b) => a + b, 0);
    expect(total).toBe(100);
  });

  it('should route collection-level queries to consistent shard', () => {
    const collectionAssignment1 = router.route('users');
    const collectionAssignment2 = router.route('users');

    expect(collectionAssignment1.shardId).toBe(collectionAssignment2.shardId);

    const ordersAssignment = router.route('orders');
    // Different collections may (or may not) route to different shards
    expect(ordersAssignment.shardId).toBeGreaterThanOrEqual(0);
    expect(ordersAssignment.shardId).toBeLessThan(4);
  });
});

// ============================================================================
// Cross-Shard Query Tests
// ============================================================================

describe('Shard Coordination - Cross-Shard Queries', () => {
  let router: ShardRouter;
  let shards: Map<number, ShardState>;
  let cleanup: () => void;

  beforeEach(() => {
    resetDocumentCounter();
    const env = createShardedEnvironment(4);
    router = env.router;
    shards = env.shards;
    cleanup = env.cleanup;

    // Populate shards with test data
    for (let i = 0; i < 40; i++) {
      const doc: TestDocument = {
        _id: `user-${i}`,
        name: `User ${i}`,
        category: i % 2 === 0 ? 'even' : 'odd',
        value: i * 10,
      };
      distributeDocument(router, shards, 'users', doc);
    }
  });

  afterEach(() => {
    cleanup();
  });

  it('should aggregate results from all shards', () => {
    const allDocs = queryAllShards(shards, () => true);
    expect(allDocs).toHaveLength(40);
  });

  it('should filter results across all shards', () => {
    const evenDocs = queryAllShards<TestDocument>(shards, (doc) => doc.category === 'even');
    expect(evenDocs).toHaveLength(20);

    for (const doc of evenDocs) {
      expect(doc.category).toBe('even');
    }
  });

  it('should support range queries across shards', () => {
    const highValueDocs = queryAllShards<TestDocument>(
      shards,
      (doc) => (doc.value ?? 0) >= 200
    );

    // Documents with value >= 200 (i.e., i >= 20)
    expect(highValueDocs).toHaveLength(20);

    for (const doc of highValueDocs) {
      expect(doc.value).toBeGreaterThanOrEqual(200);
    }
  });

  it('should combine filters across shards', () => {
    const filteredDocs = queryAllShards<TestDocument>(
      shards,
      (doc) => doc.category === 'even' && (doc.value ?? 0) >= 200
    );

    // Even numbers >= 20: 20, 22, 24, ..., 38 = 10 documents
    expect(filteredDocs).toHaveLength(10);

    for (const doc of filteredDocs) {
      expect(doc.category).toBe('even');
      expect(doc.value).toBeGreaterThanOrEqual(200);
    }
  });

  it('should handle empty results from some shards', () => {
    // Query for a very specific value that only exists on one shard
    const specificDocs = queryAllShards<TestDocument>(
      shards,
      (doc) => doc._id === 'user-0'
    );

    expect(specificDocs).toHaveLength(1);
    expect(specificDocs[0]._id).toBe('user-0');
  });
});

// ============================================================================
// Shard Split and Affinity Tests
// ============================================================================

describe('Shard Coordination - Split Collections', () => {
  let router: ShardRouter;
  let shards: Map<number, ShardState>;
  let cleanup: () => void;

  beforeEach(() => {
    resetDocumentCounter();
    const env = createShardedEnvironment(8);
    router = env.router;
    shards = env.shards;
    cleanup = env.cleanup;
  });

  afterEach(() => {
    cleanup();
  });

  it('should route split collection to designated shards', () => {
    // Split 'events' collection across shards 0, 1, 2, 3
    router.splitCollection('events', [0, 1, 2, 3]);

    const usedShards = new Set<number>();
    for (let i = 0; i < 100; i++) {
      const docId = `event-${i}-${Math.random().toString(36)}`;
      const assignment = router.routeDocument('events', docId);
      usedShards.add(assignment.shardId);
    }

    // Only shards 0-3 should be used
    for (const shardId of usedShards) {
      expect([0, 1, 2, 3]).toContain(shardId);
    }

    // Should use multiple of the designated shards
    expect(usedShards.size).toBeGreaterThan(1);
  });

  it('should coordinate writes across split collection shards', () => {
    router.splitCollection('logs', [4, 5, 6, 7]);

    const documents: TestDocument[] = [];
    for (let i = 0; i < 80; i++) {
      documents.push({
        _id: `log-${i}-${Math.random().toString(36)}`,
        name: `Log entry ${i}`,
        value: i,
      });
    }

    const shardCounts = new Map<number, number>();
    for (const doc of documents) {
      const assignment = router.routeDocument('logs', doc._id);
      shardCounts.set(assignment.shardId, (shardCounts.get(assignment.shardId) ?? 0) + 1);

      const shard = shards.get(assignment.shardId);
      if (shard) {
        shard.documents.push(doc);
      }
    }

    // Documents should be distributed across shards 4-7
    for (const [shardId, count] of shardCounts) {
      expect([4, 5, 6, 7]).toContain(shardId);
      expect(count).toBeGreaterThan(0);
    }

    // Verify total across designated shards
    let total = 0;
    for (const shardId of [4, 5, 6, 7]) {
      total += shards.get(shardId)?.documents.length ?? 0;
    }
    expect(total).toBe(80);
  });

  it('should handle affinity hints for hot collections', () => {
    // Set affinity to force 'hot_data' to shard 2
    router.setAffinityHint('hot_data', { preferredShard: 2 });

    const assignment = router.route('hot_data');
    expect(assignment.shardId).toBe(2);

    // Multiple calls should return same shard
    for (let i = 0; i < 10; i++) {
      expect(router.route('hot_data').shardId).toBe(2);
    }
  });

  it('should coordinate queries across split shards only', () => {
    router.splitCollection('metrics', [0, 2, 4, 6]);

    // Populate only the split shards
    for (let i = 0; i < 40; i++) {
      const doc: TestDocument = {
        _id: `metric-${i}`,
        name: `Metric ${i}`,
        value: i * 100,
      };
      const assignment = router.routeDocument('metrics', doc._id);
      const shard = shards.get(assignment.shardId);
      if (shard) {
        shard.documents.push(doc);
      }
    }

    // Query only the split shards
    const splitShardIds = [0, 2, 4, 6];
    const results: TestDocument[] = [];
    for (const shardId of splitShardIds) {
      const shard = shards.get(shardId);
      if (shard) {
        results.push(...shard.documents);
      }
    }

    expect(results).toHaveLength(40);

    // Non-split shards should be empty
    for (const shardId of [1, 3, 5, 7]) {
      expect(shards.get(shardId)?.documents.length).toBe(0);
    }
  });
});

// ============================================================================
// Database-Prefixed Routing Tests
// ============================================================================

describe('Shard Coordination - Database Namespace Isolation', () => {
  let router: ShardRouter;
  let shards: Map<number, ShardState>;
  let cleanup: () => void;

  beforeEach(() => {
    resetDocumentCounter();
    const env = createShardedEnvironment(4);
    router = env.router;
    shards = env.shards;
    cleanup = env.cleanup;
  });

  afterEach(() => {
    cleanup();
  });

  it('should isolate same collection name across databases', () => {
    const prodAssignment = router.routeWithDatabase('production', 'users');
    const stagingAssignment = router.routeWithDatabase('staging', 'users');

    expect(prodAssignment.database).toBe('production');
    expect(stagingAssignment.database).toBe('staging');

    // Both should have valid shard assignments
    expect(prodAssignment.shardId).toBeGreaterThanOrEqual(0);
    expect(stagingAssignment.shardId).toBeGreaterThanOrEqual(0);
  });

  it('should consistently route same database.collection', () => {
    const assignment1 = router.routeWithDatabase('mydb', 'orders');
    const assignment2 = router.routeWithDatabase('mydb', 'orders');

    expect(assignment1.shardId).toBe(assignment2.shardId);
    expect(assignment1.database).toBe(assignment2.database);
    expect(assignment1.collection).toBe(assignment2.collection);
  });

  it('should support affinity hints with database prefix', () => {
    router.setAffinityHint('production.users', { preferredShard: 1 });

    const prodAssignment = router.routeWithDatabase('production', 'users');
    expect(prodAssignment.shardId).toBe(1);

    // Staging should use hash-based routing
    const stagingAssignment = router.routeWithDatabase('staging', 'users');
    // May or may not be 1 (depends on hash)
    expect(stagingAssignment.shardId).toBeGreaterThanOrEqual(0);
  });

  it('should coordinate multi-database queries', () => {
    const databases = ['db1', 'db2', 'db3'];

    for (const db of databases) {
      for (let i = 0; i < 10; i++) {
        const doc: TestDocument = {
          _id: `${db}-doc-${i}`,
          name: `Document ${i} in ${db}`,
          shardKey: db,
        };
        const assignment = router.routeWithDatabase(db, 'items');
        const shard = shards.get(assignment.shardId);
        if (shard) {
          shard.documents.push(doc);
        }
      }
    }

    // Query all documents and group by database
    const allDocs = queryAllShards(shards, () => true);
    expect(allDocs).toHaveLength(30);

    const byDatabase = new Map<string, number>();
    for (const doc of allDocs) {
      const db = doc.shardKey ?? 'unknown';
      byDatabase.set(db, (byDatabase.get(db) ?? 0) + 1);
    }

    expect(byDatabase.get('db1')).toBe(10);
    expect(byDatabase.get('db2')).toBe(10);
    expect(byDatabase.get('db3')).toBe(10);
  });
});

// ============================================================================
// Concurrent Operation Tests
// ============================================================================

describe('Shard Coordination - Concurrent Operations', () => {
  let router: ShardRouter;
  let shards: Map<number, ShardState>;
  let cleanup: () => void;

  beforeEach(() => {
    resetDocumentCounter();
    const env = createShardedEnvironment(4);
    router = env.router;
    shards = env.shards;
    cleanup = env.cleanup;
  });

  afterEach(() => {
    cleanup();
  });

  it('should handle concurrent routing requests', async () => {
    const promises: Promise<number>[] = [];

    for (let i = 0; i < 100; i++) {
      promises.push(
        Promise.resolve(router.routeDocument('concurrent', `doc-${i}`).shardId)
      );
    }

    const results = await Promise.all(promises);

    // All results should be valid shard IDs
    for (const shardId of results) {
      expect(shardId).toBeGreaterThanOrEqual(0);
      expect(shardId).toBeLessThan(4);
    }
  });

  it('should maintain consistency under concurrent writes', async () => {
    const writePromises: Promise<void>[] = [];

    for (let i = 0; i < 50; i++) {
      writePromises.push(
        (async () => {
          const doc: TestDocument = {
            _id: `concurrent-${i}`,
            name: `Concurrent Doc ${i}`,
            value: i,
          };
          const shardId = distributeDocument(router, shards, 'concurrent', doc);
          expect(shardId).toBeGreaterThanOrEqual(0);
        })()
      );
    }

    await Promise.all(writePromises);

    const allDocs = queryAllShards(shards, () => true);
    expect(allDocs).toHaveLength(50);
  });

  it('should handle concurrent queries across shards', async () => {
    // Populate data first
    for (let i = 0; i < 40; i++) {
      const doc: TestDocument = {
        _id: `query-${i}`,
        name: `Query Doc ${i}`,
        category: i % 4 === 0 ? 'target' : 'other',
      };
      distributeDocument(router, shards, 'querytest', doc);
    }

    // Run concurrent queries
    const queryPromises: Promise<TestDocument[]>[] = [];
    for (let i = 0; i < 10; i++) {
      queryPromises.push(
        Promise.resolve(
          queryAllShards<TestDocument>(shards, (doc) => doc.category === 'target')
        )
      );
    }

    const results = await Promise.all(queryPromises);

    // All queries should return same results
    for (const result of results) {
      expect(result).toHaveLength(10); // 40 / 4 = 10 target docs
    }
  });
});

// ============================================================================
// Error Handling and Edge Cases
// ============================================================================

describe('Shard Coordination - Error Handling', () => {
  let router: ShardRouter;
  let shards: Map<number, ShardState>;
  let cleanup: () => void;

  beforeEach(() => {
    resetDocumentCounter();
    const env = createShardedEnvironment(4);
    router = env.router;
    shards = env.shards;
    cleanup = env.cleanup;
  });

  afterEach(() => {
    cleanup();
  });

  it('should handle empty query results gracefully', () => {
    const results = queryAllShards(shards, () => false);
    expect(results).toHaveLength(0);
  });

  it('should handle non-existent document lookups', () => {
    const results = queryAllShards<TestDocument>(
      shards,
      (doc) => doc._id === 'non-existent-id'
    );
    expect(results).toHaveLength(0);
  });

  it('should validate shard IDs in split configuration', () => {
    expect(() => router.splitCollection('invalid', [-1, 0, 1])).toThrow(/shard.*range/i);
    expect(() => router.splitCollection('invalid', [0, 1, 100])).toThrow(/shard.*range/i);
  });

  it('should require minimum shards for split', () => {
    expect(() => router.splitCollection('single', [0])).toThrow(/at least 2/i);
  });

  it('should handle router cache clearing during operations', () => {
    const assignment1 = router.route('cached-collection');
    router.clearCache();
    const assignment2 = router.route('cached-collection');

    // Same shard ID even after cache clear
    expect(assignment1.shardId).toBe(assignment2.shardId);
  });

  it('should handle affinity hint removal', () => {
    router.setAffinityHint('removable', { preferredShard: 3 });
    expect(router.route('removable').shardId).toBe(3);

    router.removeAffinityHint('removable');

    // Should now use hash-based routing
    const naturalShard = hashCollectionToShard('removable', 4);
    expect(router.route('removable').shardId).toBe(naturalShard);
  });
});
