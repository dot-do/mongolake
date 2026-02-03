/**
 * Query Planner Tests - Index-Aware Query Planning
 *
 * RED tests for query planning that uses available indexes.
 * These tests verify that the query planner correctly:
 * - Uses _id index for _id filters
 * - Uses available indexes for indexed fields
 * - Falls back to collection scan for non-indexed fields
 * - Selects the best index for compound queries
 * - Generates proper query plans
 * - Considers index selectivity
 * - Uses indexes for range queries
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { QueryPlanner, type ExecutionPlan } from '../../../src/index/query-planner.js';
import { IndexManager } from '../../../src/index/index-manager.js';
import type { StorageBackend } from '../../../src/storage/index.js';

// ============================================================================
// Mock Storage Backend
// ============================================================================

function createMockStorage(): StorageBackend {
  const data = new Map<string, Uint8Array>();

  return {
    get: vi.fn(async (key: string) => data.get(key) || null),
    put: vi.fn(async (key: string, value: Uint8Array) => {
      data.set(key, value);
    }),
    delete: vi.fn(async (key: string) => {
      data.delete(key);
    }),
    list: vi.fn(async () => ({ objects: [], truncated: false })),
    head: vi.fn(async () => null),
  } as unknown as StorageBackend;
}

// ============================================================================
// Test 1-3: _id Index Usage
// ============================================================================

describe('Query with _id filter uses _id index', () => {
  let indexManager: IndexManager;
  let planner: QueryPlanner;
  let storage: StorageBackend;

  beforeEach(async () => {
    storage = createMockStorage();
    indexManager = new IndexManager('testdb', 'users', storage);
    planner = new QueryPlanner(indexManager);

    // Ensure _id index exists
    await indexManager.ensureIdIndex();

    // Index some documents
    await indexManager.indexDocument({ _id: 'user1', name: 'Alice' });
    await indexManager.indexDocument({ _id: 'user2', name: 'Bob' });
    await indexManager.indexDocument({ _id: 'user3', name: 'Charlie' });
  });

  it('should use _id index for simple _id equality filter', async () => {
    const plan = await planner.createPlan('users', { _id: 'user1' });

    expect(plan.strategy).toBe('index_scan');
    expect(plan.indexName).toBe('_id_');
    expect(plan.field).toBe('_id');
    expect(plan.operation).toBe('eq');
  });

  it('should return correct document for _id lookup', async () => {
    const filter = { _id: 'user2' };
    const plan = await planner.createPlan('users', filter);
    const result = await planner.executePlan('users', filter, plan);

    expect(result.docIds).toEqual(['user2']);
    expect(result.exact).toBe(true);
  });

  it('should use _id index for $eq operator', async () => {
    const plan = await planner.createPlan('users', { _id: { $eq: 'user1' } });

    expect(plan.strategy).toBe('index_scan');
    expect(plan.indexName).toBe('_id_');
    expect(plan.operation).toBe('eq');
  });
});

// ============================================================================
// Test 4-6: Indexed Field Usage
// ============================================================================

describe('Query with indexed field uses that index', () => {
  let indexManager: IndexManager;
  let planner: QueryPlanner;
  let storage: StorageBackend;

  beforeEach(async () => {
    storage = createMockStorage();
    indexManager = new IndexManager('testdb', 'users', storage);
    planner = new QueryPlanner(indexManager);

    // Create indexes
    await indexManager.createIndex({ age: 1 });
    await indexManager.createIndex({ email: 1 }, { unique: true });
    await indexManager.createIndex({ status: 1 });

    // Index documents
    await indexManager.indexDocument({ _id: 'doc1', age: 25, email: 'alice@test.com', status: 'active' });
    await indexManager.indexDocument({ _id: 'doc2', age: 30, email: 'bob@test.com', status: 'active' });
    await indexManager.indexDocument({ _id: 'doc3', age: 25, email: 'charlie@test.com', status: 'inactive' });
  });

  it('should use age index for age filter', async () => {
    const plan = await planner.createPlan('users', { age: 25 });

    expect(plan.strategy).toBe('index_scan');
    expect(plan.indexName).toBe('age_1');
    expect(plan.field).toBe('age');
  });

  it('should use unique email index for email lookup', async () => {
    const plan = await planner.createPlan('users', { email: 'alice@test.com' });

    expect(plan.strategy).toBe('index_scan');
    expect(plan.indexName).toBe('email_1');
    expect(plan.field).toBe('email');
  });

  it('should return matching documents using index', async () => {
    const filter = { age: 25 };
    const plan = await planner.createPlan('users', filter);
    const result = await planner.executePlan('users', filter, plan);

    expect(result.docIds).toHaveLength(2);
    expect(result.docIds).toContain('doc1');
    expect(result.docIds).toContain('doc3');
    expect(result.exact).toBe(true);
  });
});

// ============================================================================
// Test 7-9: Non-indexed Field Falls Back to Collection Scan
// ============================================================================

describe('Query with non-indexed field does collection scan', () => {
  let indexManager: IndexManager;
  let planner: QueryPlanner;
  let storage: StorageBackend;

  beforeEach(async () => {
    storage = createMockStorage();
    indexManager = new IndexManager('testdb', 'users', storage);
    planner = new QueryPlanner(indexManager);

    // Only create age index, leave name unindexed
    await indexManager.createIndex({ age: 1 });
  });

  it('should use full_scan for non-indexed field', async () => {
    const plan = await planner.createPlan('users', { name: 'Alice' });

    expect(plan.strategy).toBe('full_scan');
    expect(plan.indexName).toBeUndefined();
    expect(plan.estimatedSelectivity).toBe(1.0);
  });

  it('should explain why full scan is needed', async () => {
    const plan = await planner.createPlan('users', { name: 'Alice' });

    expect(plan.explanation).toContain('No index available');
    expect(plan.explanation).toContain('name');
  });

  it('should return no docIds for full scan plan execution', async () => {
    const filter = { name: 'Alice' };
    const plan = await planner.createPlan('users', filter);
    const result = await planner.executePlan('users', filter, plan);

    expect(result.docIds).toBeUndefined();
    expect(result.exact).toBe(false);
  });
});

// ============================================================================
// Test 10-12: Compound Query Index Selection
// ============================================================================

describe('Compound query uses best available index', () => {
  let indexManager: IndexManager;
  let planner: QueryPlanner;
  let storage: StorageBackend;

  beforeEach(async () => {
    storage = createMockStorage();
    indexManager = new IndexManager('testdb', 'users', storage);
    planner = new QueryPlanner(indexManager);

    // Create multiple indexes
    await indexManager.createIndex({ age: 1 });
    await indexManager.createIndex({ status: 1 });
    await indexManager.createIndex({ city: 1 });

    // Index documents
    await indexManager.indexDocument({ _id: 'doc1', age: 25, status: 'active', city: 'NYC', name: 'Alice' });
    await indexManager.indexDocument({ _id: 'doc2', age: 30, status: 'active', city: 'LA', name: 'Bob' });
    await indexManager.indexDocument({ _id: 'doc3', age: 25, status: 'inactive', city: 'NYC', name: 'Charlie' });
  });

  it('should use an index for compound query with indexed and non-indexed fields', async () => {
    const filter = { age: 25, name: 'Alice' };
    const plan = await planner.createPlan('users', filter);

    expect(plan.strategy).toBe('index_scan');
    expect(plan.field).toBe('age');
    expect(plan.indexName).toBe('age_1');
  });

  it('should create residual filter for non-indexed fields in compound query', async () => {
    const filter = { age: 25, name: 'Alice' };
    const plan = await planner.createPlan('users', filter);

    expect(plan.residualFilter).toEqual({ name: 'Alice' });
  });

  it('should select one of multiple available indexes for multi-indexed compound query', async () => {
    const filter = { age: 25, status: 'active', city: 'NYC' };
    const plan = await planner.createPlan('users', filter);

    expect(plan.strategy).toBe('index_scan');
    // Should use one of the indexed fields
    expect(['age_1', 'status_1', 'city_1']).toContain(plan.indexName);
    // Other indexed fields should be in residual filter
    expect(plan.residualFilter).toBeDefined();
  });
});

// ============================================================================
// Test 13-15: Query Plan Generation
// ============================================================================

describe('Query plan generation', () => {
  let indexManager: IndexManager;
  let planner: QueryPlanner;
  let storage: StorageBackend;

  beforeEach(async () => {
    storage = createMockStorage();
    indexManager = new IndexManager('testdb', 'users', storage);
    planner = new QueryPlanner(indexManager);

    await indexManager.createIndex({ age: 1 });
  });

  it('should generate complete execution plan for index scan', async () => {
    const plan = await planner.createPlan('users', { age: 25 });

    expect(plan).toMatchObject({
      strategy: 'index_scan',
      indexName: 'age_1',
      field: 'age',
      operation: 'eq',
      condition: 25,
      estimatedSelectivity: 0.01,
    });
    expect(plan.explanation).toContain("Using index 'age_1'");
  });

  it('should generate human-readable explanation via explain()', async () => {
    const explanation = await planner.explain('users', { age: 25 });

    expect(explanation).toContain("Query Plan for collection 'users'");
    expect(explanation).toContain('Strategy: index_scan');
    expect(explanation).toContain('Index: age_1');
    expect(explanation).toContain('Field: age');
    expect(explanation).toContain('Operation: eq');
    expect(explanation).toContain('Estimated Selectivity');
  });

  it('should generate full scan plan for empty filter', async () => {
    const plan = await planner.createPlan('users', {});

    expect(plan.strategy).toBe('full_scan');
    expect(plan.estimatedSelectivity).toBe(1.0);
    expect(plan.explanation).toContain('Empty filter');
  });
});

// ============================================================================
// Test 16-18: Index Selection Based on Selectivity
// ============================================================================

describe('Index selection based on selectivity', () => {
  let indexManager: IndexManager;
  let planner: QueryPlanner;
  let storage: StorageBackend;

  beforeEach(async () => {
    storage = createMockStorage();
    indexManager = new IndexManager('testdb', 'data', storage);
    planner = new QueryPlanner(indexManager);

    await indexManager.createIndex({ field: 1 });
  });

  it('should estimate equality operation as highly selective (1%)', async () => {
    const plan = await planner.createPlan('data', { field: 'value' });

    expect(plan.estimatedSelectivity).toBe(0.01);
    expect(plan.operation).toBe('eq');
  });

  it('should estimate $in operation as moderately selective (10%)', async () => {
    const plan = await planner.createPlan('data', { field: { $in: ['a', 'b', 'c'] } });

    expect(plan.estimatedSelectivity).toBe(0.1);
    expect(plan.operation).toBe('in');
  });

  it('should estimate range operation as less selective (30%)', async () => {
    const plan = await planner.createPlan('data', { field: { $gt: 100 } });

    expect(plan.estimatedSelectivity).toBe(0.3);
    expect(plan.operation).toBe('range');
  });
});

// ============================================================================
// Test 19-23: Range Queries Use Index When Available
// ============================================================================

describe('Range queries use index when available', () => {
  let indexManager: IndexManager;
  let planner: QueryPlanner;
  let storage: StorageBackend;

  beforeEach(async () => {
    storage = createMockStorage();
    indexManager = new IndexManager('testdb', 'users', storage);
    planner = new QueryPlanner(indexManager);

    await indexManager.createIndex({ age: 1 });

    // Index documents with various ages
    await indexManager.indexDocument({ _id: 'doc1', age: 20 });
    await indexManager.indexDocument({ _id: 'doc2', age: 25 });
    await indexManager.indexDocument({ _id: 'doc3', age: 30 });
    await indexManager.indexDocument({ _id: 'doc4', age: 35 });
    await indexManager.indexDocument({ _id: 'doc5', age: 40 });
  });

  it('should use index for $gt range query', async () => {
    const filter = { age: { $gt: 25 } };
    const plan = await planner.createPlan('users', filter);

    expect(plan.strategy).toBe('index_scan');
    expect(plan.operation).toBe('range');
    expect(plan.indexName).toBe('age_1');
  });

  it('should use index for $gte range query', async () => {
    const filter = { age: { $gte: 30 } };
    const plan = await planner.createPlan('users', filter);

    expect(plan.strategy).toBe('index_scan');
    expect(plan.operation).toBe('range');
  });

  it('should use index for $lt range query', async () => {
    const filter = { age: { $lt: 30 } };
    const plan = await planner.createPlan('users', filter);

    expect(plan.strategy).toBe('index_scan');
    expect(plan.operation).toBe('range');
  });

  it('should use index for $lte range query', async () => {
    const filter = { age: { $lte: 35 } };
    const plan = await planner.createPlan('users', filter);

    expect(plan.strategy).toBe('index_scan');
    expect(plan.operation).toBe('range');
  });

  it('should return correct documents for bounded range query', async () => {
    const filter = { age: { $gte: 25, $lte: 35 } };
    const plan = await planner.createPlan('users', filter);
    const result = await planner.executePlan('users', filter, plan);

    expect(result.docIds).toHaveLength(3);
    expect(result.docIds).toContain('doc2'); // age 25
    expect(result.docIds).toContain('doc3'); // age 30
    expect(result.docIds).toContain('doc4'); // age 35
  });
});

// ============================================================================
// Test 24-26: Utility Methods
// ============================================================================

describe('QueryPlanner utility methods', () => {
  let indexManager: IndexManager;
  let planner: QueryPlanner;
  let storage: StorageBackend;

  beforeEach(async () => {
    storage = createMockStorage();
    indexManager = new IndexManager('testdb', 'users', storage);
    planner = new QueryPlanner(indexManager);

    await indexManager.createIndex({ age: 1 });
    await indexManager.createIndex({ email: 1 }, { unique: true });
  });

  it('canUseIndex should return true for indexed field', async () => {
    const result = await planner.canUseIndex({ age: 25 });
    expect(result).toBe(true);
  });

  it('canUseIndex should return false for non-indexed field', async () => {
    const result = await planner.canUseIndex({ name: 'Alice' });
    expect(result).toBe(false);
  });

  it('getBestIndex should return index name for indexed field', async () => {
    const result = await planner.getBestIndex({ email: 'test@example.com' });
    expect(result).toBe('email_1');
  });
});

// ============================================================================
// Test 27-30: Edge Cases
// ============================================================================

describe('QueryPlanner edge cases', () => {
  let indexManager: IndexManager;
  let planner: QueryPlanner;
  let storage: StorageBackend;

  beforeEach(async () => {
    storage = createMockStorage();
    indexManager = new IndexManager('testdb', 'users', storage);
    planner = new QueryPlanner(indexManager);

    await indexManager.createIndex({ age: 1 });
  });

  it('should handle null filter gracefully', async () => {
    const plan = await planner.createPlan('users', null as unknown as Record<string, unknown>);
    expect(plan.strategy).toBe('full_scan');
  });

  it('should handle $ne operator with full scan fallback', async () => {
    const plan = await planner.createPlan('users', { age: { $ne: 25 } });
    // $ne is not efficiently indexable
    expect(plan.strategy).toBe('full_scan');
  });

  it('should handle logical operators with full scan', async () => {
    const plan = await planner.createPlan('users', {
      $or: [{ age: 25 }, { age: 30 }],
    });
    expect(plan.strategy).toBe('full_scan');
  });

  it('should handle null value in indexed field filter', async () => {
    await indexManager.indexDocument({ _id: 'docNull', age: null });

    const plan = await planner.createPlan('users', { age: null });
    expect(plan.strategy).toBe('index_scan');

    const result = await planner.executePlan('users', { age: null }, plan);
    expect(result.docIds).toContain('docNull');
  });
});
