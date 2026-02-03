/**
 * Query Routing Integration Tests
 *
 * Tests the full query pipeline from client query through:
 * - Query validation
 * - Query planning (cost estimation)
 * - Shard routing
 * - Result aggregation
 *
 * This tests multiple components working together to execute queries.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  ShardRouter,
  createShardRouter,
  hashCollectionToShard,
} from '../../../src/shard/router.js';
import {
  QueryPlanner,
  QueryCostEstimator,
  type CollectionStats,
  type CostEstimate,
  type ExecutionPlan,
} from '../../../src/query/planner.js';
import { MemoryStorage } from '../../../src/storage/index.js';
import { MongoLake, Collection, Database } from '../../../src/client/index.js';
import {
  validateFilter,
  validateAggregationPipeline,
  ValidationError,
} from '../../../src/validation/index.js';
import { resetDocumentCounter, createUser, createUsers } from '../../utils/factories.js';

// ============================================================================
// Test Types
// ============================================================================

interface TestDocument {
  _id: string;
  name: string;
  age?: number;
  status?: string;
  category?: string;
  score?: number;
  tags?: string[];
}

interface ShardState {
  shardId: number;
  storage: MemoryStorage;
  documents: TestDocument[];
}

// ============================================================================
// Test Helpers
// ============================================================================

function createQueryTestEnvironment(shardCount: number = 4) {
  const router = createShardRouter({ shardCount });
  const shards = new Map<number, ShardState>();
  const planner = new QueryPlanner();
  const costEstimator = new QueryCostEstimator();

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
    planner,
    costEstimator,
    cleanup: () => {
      shards.forEach((shard) => shard.storage.clear());
      shards.clear();
    },
  };
}

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
// Query Validation -> Planning Integration Tests
// ============================================================================

describe('Query Validation and Planning Integration', () => {
  let router: ShardRouter;
  let shards: Map<number, ShardState>;
  let planner: QueryPlanner;
  let costEstimator: QueryCostEstimator;
  let cleanup: () => void;

  beforeEach(() => {
    resetDocumentCounter();
    const env = createQueryTestEnvironment(4);
    router = env.router;
    shards = env.shards;
    planner = env.planner;
    costEstimator = env.costEstimator;
    cleanup = env.cleanup;
  });

  afterEach(() => {
    cleanup();
  });

  it('should validate filter before creating execution plan', async () => {
    const validFilter = { name: 'Alice', age: { $gt: 25 } };

    // Validation should pass
    expect(() => validateFilter(validFilter)).not.toThrow();

    // Planning should work with valid filter
    const plan = await planner.createExecutionPlan(validFilter);
    expect(plan).toBeDefined();
    expect(plan.cost).toBeDefined();
    expect(plan.cost.totalCost).toBeGreaterThan(0);
  });

  it('should reject invalid filter before planning', async () => {
    const invalidFilter = { $invalidOp: 'value' };

    // Validation should fail
    expect(() => validateFilter(invalidFilter)).toThrow(ValidationError);
  });

  it('should estimate costs correctly for equality queries', async () => {
    const stats: CollectionStats = {
      documentCount: 10000,
      fieldCardinality: new Map([
        ['status', 5], // 5 unique values
        ['category', 100], // 100 unique values
      ]),
    };

    // High-cardinality field should have lower selectivity
    const highCardFilter = { category: 'electronics' };
    const highCardCost = costEstimator.estimateFullScanCost(highCardFilter, stats);

    // Low-cardinality field should have higher selectivity
    const lowCardFilter = { status: 'active' };
    const lowCardCost = costEstimator.estimateFullScanCost(lowCardFilter, stats);

    // Both should examine all documents in full scan
    expect(highCardCost.documentsExamined).toBe(10000);
    expect(lowCardCost.documentsExamined).toBe(10000);

    // High cardinality should return fewer documents
    expect(highCardCost.documentsReturned).toBeLessThan(lowCardCost.documentsReturned);
  });

  it('should estimate costs correctly for range queries', async () => {
    const stats: CollectionStats = {
      documentCount: 10000,
      fieldCardinality: new Map([['age', 100]]),
    };

    const equalityFilter = { age: 25 };
    const rangeFilter = { age: { $gt: 25 } };

    const equalityCost = costEstimator.estimateFullScanCost(equalityFilter, stats);
    const rangeCost = costEstimator.estimateFullScanCost(rangeFilter, stats);

    // Range queries typically have higher selectivity (return more docs)
    expect(rangeCost.selectivity).toBeGreaterThan(equalityCost.selectivity);
  });

  it('should create execution plan with cost breakdown', async () => {
    const filter = { status: 'active', score: { $gte: 80 } };

    const plan = await planner.createExecutionPlan(filter, {
      stats: { documentCount: 5000 },
    });

    expect(plan.planId).toBeDefined();
    expect(plan.strategy).toBe('full_scan');
    expect(plan.cost.breakdown).toBeDefined();
    expect(plan.cost.breakdown.documentFetch).toBeGreaterThan(0);
    expect(plan.cost.breakdown.filter).toBeGreaterThan(0);
    expect(plan.explanation).toBeDefined();
  });
});

// ============================================================================
// Query Planning -> Shard Routing Integration Tests
// ============================================================================

describe('Query Planning and Shard Routing Integration', () => {
  let router: ShardRouter;
  let shards: Map<number, ShardState>;
  let planner: QueryPlanner;
  let cleanup: () => void;

  beforeEach(() => {
    resetDocumentCounter();
    const env = createQueryTestEnvironment(4);
    router = env.router;
    shards = env.shards;
    planner = env.planner;
    cleanup = env.cleanup;

    // Populate shards with test data
    for (let i = 0; i < 100; i++) {
      const doc: TestDocument = {
        _id: `user-${i}`,
        name: `User ${i}`,
        age: 20 + (i % 50),
        status: i % 3 === 0 ? 'active' : i % 3 === 1 ? 'inactive' : 'pending',
        category: i % 5 === 0 ? 'premium' : 'standard',
        score: i * 10,
      };
      distributeDocument(router, shards, 'users', doc);
    }
  });

  afterEach(() => {
    cleanup();
  });

  it('should route collection-level query to consistent shard', async () => {
    const filter = { status: 'active' };

    // Create plan
    const plan = await planner.createExecutionPlan(filter);
    expect(plan).toBeDefined();

    // Route collection
    const assignment1 = router.route('users');
    const assignment2 = router.route('users');

    // Routing should be consistent
    expect(assignment1.shardId).toBe(assignment2.shardId);
  });

  it('should route document queries to appropriate shards', async () => {
    const docIds = ['user-0', 'user-25', 'user-50', 'user-75'];
    const shardAssignments = new Map<number, string[]>();

    for (const docId of docIds) {
      const assignment = router.routeDocument('users', docId);
      if (!shardAssignments.has(assignment.shardId)) {
        shardAssignments.set(assignment.shardId, []);
      }
      shardAssignments.get(assignment.shardId)!.push(docId);
    }

    // Documents should be distributed across shards
    expect(shardAssignments.size).toBeGreaterThan(0);

    // Total documents should match
    let total = 0;
    shardAssignments.forEach((docs) => (total += docs.length));
    expect(total).toBe(docIds.length);
  });

  it('should execute query across shards and aggregate results', async () => {
    const filter = { status: 'active' };

    // Validate filter
    validateFilter(filter);

    // Create plan
    const plan = await planner.createExecutionPlan(filter);
    expect(plan).toBeDefined();

    // Execute query across all shards
    const results = queryAllShards<TestDocument>(shards, (doc) => doc.status === 'active');

    // Verify results match expected count (every 3rd document is active)
    expect(results.length).toBe(34); // ceil(100/3) = 34

    // All results should match filter
    for (const doc of results) {
      expect(doc.status).toBe('active');
    }
  });

  it('should handle compound filters across shards', async () => {
    const filter = { status: 'active', category: 'premium' };

    // Validate
    validateFilter(filter);

    // Execute across shards
    const results = queryAllShards<TestDocument>(
      shards,
      (doc) => doc.status === 'active' && doc.category === 'premium'
    );

    // premium = every 5th, active = every 3rd, both = LCM(3,5) = every 15th
    // 0, 15, 30, 45, 60, 75, 90 = 7 documents
    expect(results.length).toBe(7);

    for (const doc of results) {
      expect(doc.status).toBe('active');
      expect(doc.category).toBe('premium');
    }
  });

  it('should handle range queries across shards', async () => {
    const filter = { age: { $gte: 50, $lt: 60 } };

    // Validate
    validateFilter(filter);

    // Execute across shards
    const results = queryAllShards<TestDocument>(
      shards,
      (doc) => doc.age !== undefined && doc.age >= 50 && doc.age < 60
    );

    // ages 50-59 = 10 documents * 2 cycles (0-49 maps to ages, 50-99 maps again)
    expect(results.length).toBe(20);

    for (const doc of results) {
      expect(doc.age).toBeGreaterThanOrEqual(50);
      expect(doc.age).toBeLessThan(60);
    }
  });
});

// ============================================================================
// Split Collection Query Routing Tests
// ============================================================================

describe('Split Collection Query Routing', () => {
  let router: ShardRouter;
  let shards: Map<number, ShardState>;
  let cleanup: () => void;

  beforeEach(() => {
    resetDocumentCounter();
    const env = createQueryTestEnvironment(8);
    router = env.router;
    shards = env.shards;
    cleanup = env.cleanup;
  });

  afterEach(() => {
    cleanup();
  });

  it('should route queries to split collection shards only', () => {
    // Split 'events' across shards 0, 2, 4, 6
    router.splitCollection('events', [0, 2, 4, 6]);

    const usedShards = new Set<number>();

    // Route many documents
    for (let i = 0; i < 100; i++) {
      const docId = `event-${i}-${Math.random().toString(36)}`;
      const assignment = router.routeDocument('events', docId);
      usedShards.add(assignment.shardId);

      const shard = shards.get(assignment.shardId);
      if (shard) {
        shard.documents.push({ _id: docId, name: `Event ${i}` });
      }
    }

    // Only split shards should be used
    for (const shardId of usedShards) {
      expect([0, 2, 4, 6]).toContain(shardId);
    }

    // Query should only hit split shards
    const splitShardDocs: TestDocument[] = [];
    for (const shardId of [0, 2, 4, 6]) {
      const shard = shards.get(shardId);
      if (shard) {
        splitShardDocs.push(...shard.documents);
      }
    }

    expect(splitShardDocs.length).toBe(100);
  });

  it('should combine split shard results correctly', () => {
    router.splitCollection('logs', [1, 3, 5, 7]);

    // Insert documents with different severities
    for (let i = 0; i < 80; i++) {
      const doc: TestDocument = {
        _id: `log-${i}`,
        name: `Log ${i}`,
        status: i % 4 === 0 ? 'error' : i % 4 === 1 ? 'warn' : 'info',
      };

      const assignment = router.routeDocument('logs', doc._id);
      const shard = shards.get(assignment.shardId);
      if (shard) {
        shard.documents.push(doc);
      }
    }

    // Query only split shards for errors
    const errorLogs: TestDocument[] = [];
    for (const shardId of [1, 3, 5, 7]) {
      const shard = shards.get(shardId);
      if (shard) {
        errorLogs.push(...shard.documents.filter((doc) => doc.status === 'error'));
      }
    }

    // Every 4th document is error = 20 errors
    expect(errorLogs.length).toBe(20);
  });
});

// ============================================================================
// Aggregation Pipeline Validation and Routing Tests
// ============================================================================

describe('Aggregation Pipeline Validation and Routing', () => {
  let router: ShardRouter;
  let shards: Map<number, ShardState>;
  let cleanup: () => void;

  beforeEach(() => {
    resetDocumentCounter();
    const env = createQueryTestEnvironment(4);
    router = env.router;
    shards = env.shards;
    cleanup = env.cleanup;

    // Populate with test data
    for (let i = 0; i < 50; i++) {
      const doc: TestDocument = {
        _id: `product-${i}`,
        name: `Product ${i}`,
        category: ['electronics', 'clothing', 'books', 'home', 'sports'][i % 5]!,
        score: (i % 10) * 10 + 10,
      };
      distributeDocument(router, shards, 'products', doc);
    }
  });

  afterEach(() => {
    cleanup();
  });

  it('should validate aggregation pipeline stages', () => {
    const validPipeline = [
      { $match: { category: 'electronics' } },
      { $project: { name: 1, score: 1 } },
      { $sort: { score: -1 } },
      { $limit: 10 },
    ];

    expect(() => validateAggregationPipeline(validPipeline)).not.toThrow();
  });

  it('should reject invalid aggregation pipeline', () => {
    const invalidPipeline = [{ $invalidStage: {} }];

    expect(() => validateAggregationPipeline(invalidPipeline)).toThrow(ValidationError);
  });

  it('should validate $match stage filter', () => {
    const pipelineWithValidMatch = [{ $match: { score: { $gt: 50 } } }];

    expect(() => validateAggregationPipeline(pipelineWithValidMatch)).not.toThrow();
  });

  it('should reject $match with invalid operators', () => {
    const pipelineWithInvalidMatch = [{ $match: { score: { $badOp: 50 } } }];

    expect(() => validateAggregationPipeline(pipelineWithInvalidMatch)).toThrow(ValidationError);
  });

  it('should simulate aggregation across shards', () => {
    // Match stage filter
    const matchFilter = { category: 'electronics' };
    validateFilter(matchFilter);

    // Execute match across shards
    const matchedDocs = queryAllShards<TestDocument>(
      shards,
      (doc) => doc.category === 'electronics'
    );

    // electronics = every 5th = 10 documents
    expect(matchedDocs.length).toBe(10);

    // Simulate project stage
    const projected = matchedDocs.map((doc) => ({
      _id: doc._id,
      name: doc.name,
      score: doc.score,
    }));

    expect(projected[0]).toHaveProperty('name');
    expect(projected[0]).toHaveProperty('score');
    expect(projected[0]).not.toHaveProperty('category');

    // Simulate sort stage
    projected.sort((a, b) => (b.score ?? 0) - (a.score ?? 0));

    // Verify sorted order
    for (let i = 1; i < projected.length; i++) {
      expect(projected[i]!.score).toBeLessThanOrEqual(projected[i - 1]!.score ?? 0);
    }

    // Simulate limit stage
    const limited = projected.slice(0, 5);
    expect(limited.length).toBe(5);
  });
});

// ============================================================================
// Query Plan Explain Tests
// ============================================================================

describe('Query Plan Explain', () => {
  let planner: QueryPlanner;

  beforeEach(() => {
    planner = new QueryPlanner();
  });

  it('should generate human-readable explanation', async () => {
    const filter = { status: 'active', age: { $gt: 25 } };

    const explanation = await planner.explain(filter, {
      stats: { documentCount: 10000 },
    });

    expect(explanation).toContain('Query Plan');
    expect(explanation).toContain('Strategy');
    expect(explanation).toContain('Total Cost');
    expect(explanation).toContain('Documents Examined');
    expect(explanation).toContain('Selectivity');
  });

  it('should show full scan for queries without indexes', async () => {
    const filter = { name: 'Alice' };

    const plan = await planner.createExecutionPlan(filter);

    expect(plan.strategy).toBe('full_scan');
    expect(plan.cost.usesIndex).toBe(false);
  });

  it('should include residual filter in plan', async () => {
    const filter = { status: 'active', category: 'premium', age: { $gt: 30 } };

    const plan = await planner.createExecutionPlan(filter);

    expect(plan.residualFilter).toBeDefined();
    expect(Object.keys(plan.residualFilter!).length).toBeGreaterThan(0);
  });
});

// ============================================================================
// Error Handling in Query Pipeline Tests
// ============================================================================

describe('Error Handling in Query Pipeline', () => {
  let router: ShardRouter;
  let planner: QueryPlanner;

  beforeEach(() => {
    router = createShardRouter({ shardCount: 4 });
    planner = new QueryPlanner();
  });

  it('should handle empty filter gracefully', async () => {
    const emptyFilter = {};

    // Validation should pass for empty filter
    expect(() => validateFilter(emptyFilter)).not.toThrow();

    // Plan should indicate full scan
    const plan = await planner.createExecutionPlan(emptyFilter);
    expect(plan.strategy).toBe('full_scan');
    expect(plan.explanation).toContain('Empty filter');
  });

  it('should handle deeply nested filters', () => {
    const deeplyNestedFilter = {
      $and: [
        { status: 'active' },
        {
          $or: [
            { category: 'premium' },
            {
              $and: [{ age: { $gt: 30 } }, { score: { $gte: 80 } }],
            },
          ],
        },
      ],
    };

    // Should validate without throwing
    expect(() => validateFilter(deeplyNestedFilter)).not.toThrow();
  });

  it('should reject filter exceeding max depth', () => {
    // Create a deeply nested filter that exceeds max depth
    let deepFilter: Record<string, unknown> = { a: 1 };
    for (let i = 0; i < 15; i++) {
      deepFilter = { $and: [deepFilter, { b: i }] };
    }

    expect(() => validateFilter(deepFilter, { maxDepth: 10 })).toThrow(ValidationError);
  });

  it('should handle empty collection name in routing', () => {
    expect(() => router.route('')).toThrow();
  });

  it('should handle empty document ID in routing', () => {
    expect(() => router.routeDocument('collection', '')).toThrow();
  });
});
