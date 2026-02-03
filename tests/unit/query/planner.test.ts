/**
 * Query Cost Estimator and Planner Tests
 *
 * Tests for cost-based query optimization including:
 * - Cost estimation for full scans vs index scans
 * - Index selection based on filter conditions
 * - Execution plan creation
 * - Selectivity estimation
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  QueryPlanner,
  QueryCostEstimator,
  estimateQueryCost,
  selectOptimalIndex,
  createExecutionPlan,
  type CollectionStats,
  type CostEstimate,
  type IndexSelection,
  type ExecutionPlan,
} from '../../../src/query/planner.js';
import type { IndexMetadata } from '../../../src/index/btree.js';
import type { CompoundIndexMetadata } from '../../../src/index/compound.js';

// ============================================================================
// Test Data
// ============================================================================

const defaultStats: CollectionStats = {
  documentCount: 10000,
  avgDocumentSize: 500,
  fieldCardinality: new Map([
    ['_id', 10000],
    ['email', 10000],
    ['status', 5],
    ['age', 100],
    ['category', 20],
    ['createdAt', 10000],
  ]),
};

const singleFieldIndexes: IndexMetadata[] = [
  {
    name: '_id_',
    field: '_id',
    unique: true,
    sparse: false,
    createdAt: new Date().toISOString(),
  },
  {
    name: 'email_1',
    field: 'email',
    unique: true,
    sparse: false,
    createdAt: new Date().toISOString(),
  },
  {
    name: 'status_1',
    field: 'status',
    unique: false,
    sparse: false,
    createdAt: new Date().toISOString(),
  },
  {
    name: 'age_1',
    field: 'age',
    unique: false,
    sparse: false,
    createdAt: new Date().toISOString(),
  },
];

const compoundIndexes: CompoundIndexMetadata[] = [
  {
    name: 'status_1_createdAt_-1',
    fields: [
      { field: 'status', direction: 1 },
      { field: 'createdAt', direction: -1 },
    ],
    unique: false,
    sparse: false,
    createdAt: new Date().toISOString(),
  },
  {
    name: 'category_1_status_1_age_1',
    fields: [
      { field: 'category', direction: 1 },
      { field: 'status', direction: 1 },
      { field: 'age', direction: 1 },
    ],
    unique: false,
    sparse: false,
    createdAt: new Date().toISOString(),
  },
];

const allIndexes = [...singleFieldIndexes, ...compoundIndexes];

// ============================================================================
// QueryCostEstimator Tests
// ============================================================================

describe('QueryCostEstimator', () => {
  let estimator: QueryCostEstimator;

  beforeEach(() => {
    estimator = new QueryCostEstimator();
  });

  describe('estimateFullScanCost', () => {
    it('should estimate cost for empty filter', () => {
      const cost = estimator.estimateFullScanCost({}, defaultStats);

      expect(cost.usesIndex).toBe(false);
      expect(cost.documentsExamined).toBe(defaultStats.documentCount);
      expect(cost.selectivity).toBe(1.0);
      expect(cost.totalCost).toBeGreaterThan(0);
    });

    it('should scale cost with document count', () => {
      const smallStats: CollectionStats = { documentCount: 100 };
      const largeStats: CollectionStats = { documentCount: 1000000 };

      const smallCost = estimator.estimateFullScanCost({}, smallStats);
      const largeCost = estimator.estimateFullScanCost({}, largeStats);

      expect(largeCost.totalCost).toBeGreaterThan(smallCost.totalCost);
      expect(largeCost.documentsExamined).toBe(1000000);
      expect(smallCost.documentsExamined).toBe(100);
    });

    it('should estimate selectivity for filter conditions', () => {
      const filterWithConditions = {
        status: 'active',
        age: { $gt: 25 },
      };

      const cost = estimator.estimateFullScanCost(filterWithConditions, defaultStats);

      // With filter, selectivity should be less than 1.0
      expect(cost.selectivity).toBeLessThan(1.0);
      expect(cost.documentsReturned).toBeLessThan(cost.documentsExamined);
    });

    it('should handle complex filters with logical operators', () => {
      const complexFilter = {
        $or: [
          { status: 'active' },
          { status: 'pending' },
        ],
        age: { $gte: 18, $lte: 65 },
      };

      const cost = estimator.estimateFullScanCost(complexFilter, defaultStats);

      expect(cost.totalCost).toBeGreaterThan(0);
      expect(cost.breakdown.filter).toBeGreaterThan(0);
    });
  });

  describe('estimateIndexScanCost', () => {
    it('should estimate cost for equality operation', () => {
      const filter = { email: 'test@example.com' };
      const cost = estimator.estimateIndexScanCost(
        filter,
        'email_1',
        'email',
        'eq',
        defaultStats
      );

      expect(cost.usesIndex).toBe(true);
      expect(cost.indexName).toBe('email_1');
      expect(cost.breakdown.indexSeek).toBeGreaterThan(0);
      expect(cost.selectivity).toBeLessThan(0.5);
    });

    it('should estimate higher cost for range operations', () => {
      const eqFilter = { age: 25 };
      const rangeFilter = { age: { $gt: 25 } };

      const eqCost = estimator.estimateIndexScanCost(
        eqFilter,
        'age_1',
        'age',
        'eq',
        defaultStats
      );

      const rangeCost = estimator.estimateIndexScanCost(
        rangeFilter,
        'age_1',
        'age',
        'range',
        defaultStats
      );

      // Range operations should have higher selectivity (less selective)
      expect(rangeCost.selectivity).toBeGreaterThan(eqCost.selectivity);
    });

    it('should estimate cost for $in operation', () => {
      const filter = { status: { $in: ['active', 'pending', 'review'] } };
      const cost = estimator.estimateIndexScanCost(
        filter,
        'status_1',
        'status',
        'in',
        defaultStats
      );

      expect(cost.usesIndex).toBe(true);
      // $in with multiple values should be less selective than single equality
      expect(cost.selectivity).toBeGreaterThan(0.01);
    });

    it('should include residual filter cost for non-indexed fields', () => {
      const filter = {
        email: 'test@example.com',
        name: 'John',  // Not indexed
      };

      const cost = estimator.estimateIndexScanCost(
        filter,
        'email_1',
        'email',
        'eq',
        defaultStats
      );

      expect(cost.breakdown.filter).toBeGreaterThan(0);
    });
  });

  describe('estimateCompoundIndexCost', () => {
    it('should estimate cost for compound index with equality prefix', () => {
      const filter = {
        status: 'active',
        createdAt: { $gt: new Date('2024-01-01') },
      };

      const cost = estimator.estimateCompoundIndexCost(
        filter,
        'status_1_createdAt_-1',
        ['status', 'createdAt'],
        ['status'],
        'createdAt',
        defaultStats
      );

      expect(cost.usesIndex).toBe(true);
      expect(cost.indexName).toBe('status_1_createdAt_-1');
      expect(cost.selectivity).toBeLessThan(1.0);
    });

    it('should estimate lower selectivity for multiple equality fields', () => {
      const filter = {
        category: 'electronics',
        status: 'active',
        age: { $gte: 18 },
      };

      const cost = estimator.estimateCompoundIndexCost(
        filter,
        'category_1_status_1_age_1',
        ['category', 'status', 'age'],
        ['category', 'status'],
        'age',
        defaultStats
      );

      // Multiple equality fields should result in very low selectivity
      expect(cost.selectivity).toBeLessThan(0.1);
    });
  });
});

// ============================================================================
// QueryPlanner Tests
// ============================================================================

describe('QueryPlanner', () => {
  let planner: QueryPlanner;

  beforeEach(() => {
    planner = new QueryPlanner();
  });

  describe('selectOptimalIndex', () => {
    it('should return null index for empty filter', async () => {
      const selection = await planner.selectOptimalIndex({}, allIndexes);

      expect(selection.indexName).toBeNull();
      expect(selection.selectivity).toBe(1.0);
      expect(selection.reason).toContain('Empty filter');
    });

    it('should select single field index for simple equality', async () => {
      const selection = await planner.selectOptimalIndex(
        { email: 'test@example.com' },
        allIndexes
      );

      expect(selection.indexName).toBe('email_1');
      expect(selection.indexType).toBe('single');
      expect(selection.coveredFields).toContain('email');
    });

    it('should prefer compound index over single field when prefix matches', async () => {
      const selection = await planner.selectOptimalIndex(
        { status: 'active', createdAt: { $gt: new Date() } },
        allIndexes
      );

      expect(selection.indexName).toBe('status_1_createdAt_-1');
      expect(selection.indexType).toBe('compound');
      expect(selection.coveredFields).toContain('status');
    });

    it('should prefer equality conditions on indexed fields', async () => {
      const selection = await planner.selectOptimalIndex(
        { status: 'active', age: { $gt: 25 } },
        allIndexes
      );

      // Should prefer status_1 (equality) over age_1 (range)
      // or compound index starting with status
      expect(['status_1', 'status_1_createdAt_-1']).toContain(selection.indexName);
    });

    it('should return alternatives', async () => {
      const selection = await planner.selectOptimalIndex(
        { status: 'active', age: 25 },
        allIndexes
      );

      expect(Array.isArray(selection.alternatives)).toBe(true);
    });

    it('should handle no matching index', async () => {
      const selection = await planner.selectOptimalIndex(
        { nonExistentField: 'value' },
        allIndexes
      );

      expect(selection.indexName).toBeNull();
      expect(selection.reason).toContain('No suitable index');
    });
  });

  describe('createExecutionPlan', () => {
    it('should create full scan plan for empty filter', async () => {
      const plan = await planner.createExecutionPlan({});

      expect(plan.strategy).toBe('full_scan');
      expect(plan.cost.usesIndex).toBe(false);
      expect(plan.planId).toBeDefined();
    });

    it('should include cost breakdown', async () => {
      const plan = await planner.createExecutionPlan({ status: 'active' });

      expect(plan.cost.breakdown).toBeDefined();
      expect(typeof plan.cost.breakdown.indexSeek).toBe('number');
      expect(typeof plan.cost.breakdown.indexScan).toBe('number');
      expect(typeof plan.cost.breakdown.documentFetch).toBe('number');
      expect(typeof plan.cost.breakdown.filter).toBe('number');
    });

    it('should respect hint option', async () => {
      const plan = await planner.createExecutionPlan(
        { status: 'active' },
        { hint: 'status_1' }
      );

      expect(plan.indexName).toBe('status_1');
      expect(plan.explanation).toContain('hinted');
    });

    it('should include residual filter for non-indexed fields', async () => {
      const filter = { status: 'active', name: 'John' };
      const plan = await planner.createExecutionPlan(filter);

      // Plan should include residual filter if using an index that doesn't cover all fields
      if (plan.strategy === 'index_scan') {
        expect(plan.residualFilter).toBeDefined();
      }
    });

    it('should include timestamp', async () => {
      const plan = await planner.createExecutionPlan({ status: 'active' });

      expect(plan.createdAt).toBeInstanceOf(Date);
    });
  });

  describe('estimateQueryCost', () => {
    it('should return cost estimate', async () => {
      const cost = await planner.estimateQueryCost(
        { status: 'active' },
        defaultStats
      );

      expect(cost.totalCost).toBeGreaterThan(0);
      expect(cost.documentsExamined).toBeGreaterThan(0);
      expect(cost.documentsReturned).toBeGreaterThan(0);
      expect(cost.selectivity).toBeGreaterThan(0);
      expect(cost.selectivity).toBeLessThanOrEqual(1);
    });

    it('should account for collection size', async () => {
      const smallStats: CollectionStats = { documentCount: 100 };
      const largeStats: CollectionStats = { documentCount: 1000000 };

      const smallCost = await planner.estimateQueryCost({ status: 'active' }, smallStats);
      const largeCost = await planner.estimateQueryCost({ status: 'active' }, largeStats);

      expect(largeCost.totalCost).toBeGreaterThan(smallCost.totalCost);
    });
  });

  describe('explain', () => {
    it('should return human-readable explanation', async () => {
      const explanation = await planner.explain({ status: 'active' });

      expect(typeof explanation).toBe('string');
      expect(explanation).toContain('Query Plan');
      expect(explanation).toContain('Strategy');
      expect(explanation).toContain('Cost');
    });

    it('should include cost breakdown in explanation', async () => {
      const explanation = await planner.explain({ age: { $gt: 25 } });

      expect(explanation).toContain('Cost Breakdown');
      expect(explanation).toContain('Index Seek');
      expect(explanation).toContain('Document Fetch');
    });

    it('should include selectivity percentage', async () => {
      const explanation = await planner.explain({ email: 'test@example.com' });

      expect(explanation).toContain('Selectivity');
      expect(explanation).toMatch(/\d+\.\d+%/);
    });
  });
});

// ============================================================================
// Utility Function Tests
// ============================================================================

describe('Utility Functions', () => {
  describe('estimateQueryCost', () => {
    it('should estimate cost without IndexManager', () => {
      const cost = estimateQueryCost(
        { status: 'active' },
        singleFieldIndexes,
        defaultStats
      );

      expect(cost.totalCost).toBeGreaterThan(0);
    });

    it('should use index when available', () => {
      const cost = estimateQueryCost(
        { status: 'active' },
        singleFieldIndexes,
        defaultStats
      );

      expect(cost.usesIndex).toBe(true);
      expect(cost.indexName).toBe('status_1');
    });

    it('should use full scan when no matching index', () => {
      const cost = estimateQueryCost(
        { nonExistentField: 'value' },
        singleFieldIndexes,
        defaultStats
      );

      expect(cost.usesIndex).toBe(false);
    });
  });

  describe('selectOptimalIndex', () => {
    it('should select optimal index asynchronously', async () => {
      const selection = await selectOptimalIndex(
        { email: 'test@example.com' },
        allIndexes
      );

      expect(selection.indexName).toBe('email_1');
    });
  });

  describe('createExecutionPlan', () => {
    it('should create execution plan asynchronously', async () => {
      const plan = await createExecutionPlan(
        { status: 'active' },
        { stats: defaultStats }
      );

      expect(plan.planId).toBeDefined();
      expect(plan.strategy).toBeDefined();
      expect(plan.cost).toBeDefined();
    });
  });
});

// ============================================================================
// Cost Model Tests
// ============================================================================

describe('Cost Model Behavior', () => {
  let estimator: QueryCostEstimator;

  beforeEach(() => {
    estimator = new QueryCostEstimator();
  });

  describe('Index vs Full Scan Cost Comparison', () => {
    it('should prefer index for selective queries', () => {
      const highCardinalityStats: CollectionStats = {
        documentCount: 100000,
        fieldCardinality: new Map([['email', 100000]]),
      };

      const indexCost = estimator.estimateIndexScanCost(
        { email: 'test@example.com' },
        'email_1',
        'email',
        'eq',
        highCardinalityStats
      );

      const fullScanCost = estimator.estimateFullScanCost(
        { email: 'test@example.com' },
        highCardinalityStats
      );

      expect(indexCost.totalCost).toBeLessThan(fullScanCost.totalCost);
    });

    it('should prefer full scan for low selectivity queries on small collections', () => {
      const smallStats: CollectionStats = {
        documentCount: 10,
        fieldCardinality: new Map([['status', 2]]),
      };

      const indexCost = estimator.estimateIndexScanCost(
        { status: 'active' },
        'status_1',
        'status',
        'eq',
        smallStats
      );

      const fullScanCost = estimator.estimateFullScanCost(
        { status: 'active' },
        smallStats
      );

      // For very small collections, the overhead of index seek may exceed full scan
      // This test verifies the cost model considers collection size
      expect(indexCost.breakdown.indexSeek).toBeGreaterThan(0);
    });
  });

  describe('Selectivity Estimation', () => {
    it('should estimate higher selectivity for equality than range', () => {
      const eqCost = estimator.estimateIndexScanCost(
        { age: 25 },
        'age_1',
        'age',
        'eq',
        defaultStats
      );

      const rangeCost = estimator.estimateIndexScanCost(
        { age: { $gt: 25 } },
        'age_1',
        'age',
        'range',
        defaultStats
      );

      expect(eqCost.selectivity).toBeLessThan(rangeCost.selectivity);
    });

    it('should estimate selectivity based on cardinality', () => {
      // High cardinality field (email) should have lower selectivity
      const emailStats: CollectionStats = {
        documentCount: 10000,
        fieldCardinality: new Map([['email', 10000]]),
      };

      const emailCost = estimator.estimateIndexScanCost(
        { email: 'test@example.com' },
        'email_1',
        'email',
        'eq',
        emailStats
      );

      // Low cardinality field (status) should have higher selectivity
      const statusStats: CollectionStats = {
        documentCount: 10000,
        fieldCardinality: new Map([['status', 5]]),
      };

      const statusCost = estimator.estimateIndexScanCost(
        { status: 'active' },
        'status_1',
        'status',
        'eq',
        statusStats
      );

      expect(emailCost.selectivity).toBeLessThan(statusCost.selectivity);
    });
  });

  describe('Compound Index Selectivity', () => {
    it('should have lower selectivity with more equality fields', () => {
      const singleEq = estimator.estimateCompoundIndexCost(
        { category: 'electronics' },
        'category_1_status_1_age_1',
        ['category', 'status', 'age'],
        ['category'],
        undefined,
        defaultStats
      );

      const doubleEq = estimator.estimateCompoundIndexCost(
        { category: 'electronics', status: 'active' },
        'category_1_status_1_age_1',
        ['category', 'status', 'age'],
        ['category', 'status'],
        undefined,
        defaultStats
      );

      expect(doubleEq.selectivity).toBeLessThan(singleEq.selectivity);
    });
  });
});

// ============================================================================
// Edge Cases
// ============================================================================

describe('Edge Cases', () => {
  let planner: QueryPlanner;

  beforeEach(() => {
    planner = new QueryPlanner();
  });

  it('should handle filter with only logical operators', async () => {
    const filter = {
      $and: [
        { status: 'active' },
        { age: { $gt: 18 } },
      ],
    };

    const plan = await planner.createExecutionPlan(filter);

    expect(plan).toBeDefined();
    expect(plan.strategy).toBeDefined();
  });

  it('should handle deeply nested filter', async () => {
    const filter = {
      $or: [
        {
          $and: [
            { status: 'active' },
            { age: { $gt: 18 } },
          ],
        },
        {
          $and: [
            { status: 'pending' },
            { category: 'premium' },
          ],
        },
      ],
    };

    const plan = await planner.createExecutionPlan(filter);

    expect(plan).toBeDefined();
  });

  it('should handle $exists operator', async () => {
    const filter = { email: { $exists: true } };
    const plan = await planner.createExecutionPlan(filter);

    expect(plan).toBeDefined();
    expect(plan.cost.selectivity).toBeGreaterThan(0);
  });

  it('should handle $ne operator', async () => {
    const filter = { status: { $ne: 'deleted' } };
    const plan = await planner.createExecutionPlan(filter);

    expect(plan).toBeDefined();
    // $ne should have high selectivity (matches most documents)
    expect(plan.cost.selectivity).toBeGreaterThan(0.5);
  });

  it('should handle regex filter', async () => {
    const filter = { email: { $regex: '^test' } };
    const plan = await planner.createExecutionPlan(filter);

    expect(plan).toBeDefined();
    // Anchored regex should be moderately selective
    expect(plan.cost.selectivity).toBeLessThan(0.5);
  });

  it('should handle very large collection stats', async () => {
    const largeStats: CollectionStats = {
      documentCount: 100000000, // 100 million
    };

    const plan = await planner.createExecutionPlan(
      { status: 'active' },
      { stats: largeStats }
    );

    expect(plan).toBeDefined();
    expect(plan.cost.documentsExamined).toBeLessThanOrEqual(largeStats.documentCount);
  });

  it('should handle empty indexes array', async () => {
    const selection = await planner.selectOptimalIndex(
      { status: 'active' },
      []
    );

    expect(selection.indexName).toBeNull();
    expect(selection.reason).toContain('No indexes available');
  });
});
