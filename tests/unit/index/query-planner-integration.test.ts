/**
 * Query Planner Integration Tests
 *
 * Tests demonstrating index usage in query planning scenarios.
 * These tests show the full workflow of:
 * 1. Creating indexes
 * 2. Indexing documents
 * 3. Creating query plans
 * 4. Executing plans to retrieve documents
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { QueryPlanner, type ExecutionPlan } from '../../../src/index/query-planner.js';
import { IndexManager } from '../../../src/index/index-manager.js';
import type { StorageBackend } from '../../../src/storage/index.js';
import type { Document } from '../../../src/types.js';

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
// Integration Test: User Collection
// ============================================================================

describe('QueryPlanner Integration - User Collection', () => {
  let indexManager: IndexManager;
  let planner: QueryPlanner;
  let storage: StorageBackend;

  // Sample user documents
  const users: Document[] = [
    { _id: 'user1', name: 'Alice', age: 28, email: 'alice@example.com', status: 'active', city: 'NYC' },
    { _id: 'user2', name: 'Bob', age: 35, email: 'bob@example.com', status: 'active', city: 'LA' },
    { _id: 'user3', name: 'Charlie', age: 28, email: 'charlie@example.com', status: 'inactive', city: 'NYC' },
    { _id: 'user4', name: 'Diana', age: 42, email: 'diana@example.com', status: 'active', city: 'Chicago' },
    { _id: 'user5', name: 'Eve', age: 31, email: 'eve@example.com', status: 'pending', city: 'NYC' },
    { _id: 'user6', name: 'Frank', age: 28, email: 'frank@example.com', status: 'active', city: 'LA' },
    { _id: 'user7', name: 'Grace', age: 55, email: 'grace@example.com', status: 'active', city: 'NYC' },
    { _id: 'user8', name: 'Henry', age: 22, email: 'henry@example.com', status: 'inactive', city: 'Boston' },
  ];

  beforeEach(async () => {
    storage = createMockStorage();
    indexManager = new IndexManager('testdb', 'users', storage);
    planner = new QueryPlanner(indexManager);

    // Create indexes
    await indexManager.ensureIdIndex(); // _id index
    await indexManager.createIndex({ age: 1 });
    await indexManager.createIndex({ status: 1 });
    await indexManager.createIndex({ email: 1 }, { unique: true });
    await indexManager.createIndex({ city: 1 });

    // Index all documents
    for (const user of users) {
      await indexManager.indexDocument(user);
    }
  });

  describe('Single field queries', () => {
    it('should use index for _id lookup', async () => {
      const plan = await planner.createPlan('users', { _id: 'user1' });

      expect(plan.strategy).toBe('index_scan');
      expect(plan.indexName).toBe('_id_');
      expect(plan.field).toBe('_id');

      const result = await planner.executePlan('users', { _id: 'user1' }, plan);
      expect(result.docIds).toEqual(['user1']);
      expect(result.exact).toBe(true);
    });

    it('should use index for age equality', async () => {
      const plan = await planner.createPlan('users', { age: 28 });

      expect(plan.strategy).toBe('index_scan');
      expect(plan.field).toBe('age');

      const result = await planner.executePlan('users', { age: 28 }, plan);
      expect(result.docIds).toHaveLength(3);
      expect(result.docIds).toContain('user1');
      expect(result.docIds).toContain('user3');
      expect(result.docIds).toContain('user6');
    });

    it('should use index for status filter', async () => {
      const plan = await planner.createPlan('users', { status: 'active' });

      expect(plan.strategy).toBe('index_scan');

      const result = await planner.executePlan('users', { status: 'active' }, plan);
      expect(result.docIds).toHaveLength(5);
      expect(result.docIds).toContain('user1');
      expect(result.docIds).toContain('user2');
      expect(result.docIds).toContain('user4');
      expect(result.docIds).toContain('user6');
      expect(result.docIds).toContain('user7');
    });

    it('should use unique index for email lookup', async () => {
      const plan = await planner.createPlan('users', { email: 'alice@example.com' });

      expect(plan.strategy).toBe('index_scan');
      expect(plan.indexName).toBe('email_1');

      const result = await planner.executePlan('users', { email: 'alice@example.com' }, plan);
      expect(result.docIds).toEqual(['user1']);
    });
  });

  describe('Range queries', () => {
    it('should find users older than 30', async () => {
      const filter = { age: { $gt: 30 } };
      const plan = await planner.createPlan('users', filter);

      expect(plan.strategy).toBe('index_scan');
      expect(plan.operation).toBe('range');

      const result = await planner.executePlan('users', filter, plan);
      expect(result.docIds).toHaveLength(4);
      expect(result.docIds).toContain('user2'); // 35
      expect(result.docIds).toContain('user4'); // 42
      expect(result.docIds).toContain('user5'); // 31
      expect(result.docIds).toContain('user7'); // 55
    });

    it('should find users between ages 25 and 35', async () => {
      const filter = { age: { $gte: 25, $lte: 35 } };
      const plan = await planner.createPlan('users', filter);

      expect(plan.strategy).toBe('index_scan');

      const result = await planner.executePlan('users', filter, plan);
      expect(result.docIds).toHaveLength(5);
      expect(result.docIds).toContain('user1'); // 28
      expect(result.docIds).toContain('user2'); // 35
      expect(result.docIds).toContain('user3'); // 28
      expect(result.docIds).toContain('user5'); // 31
      expect(result.docIds).toContain('user6'); // 28
    });

    it('should find users younger than 25', async () => {
      const filter = { age: { $lt: 25 } };
      const plan = await planner.createPlan('users', filter);

      const result = await planner.executePlan('users', filter, plan);
      expect(result.docIds).toHaveLength(1);
      expect(result.docIds).toContain('user8'); // 22
    });
  });

  describe('$in queries', () => {
    it('should find users in specific cities', async () => {
      const filter = { city: { $in: ['NYC', 'LA'] } };
      const plan = await planner.createPlan('users', filter);

      expect(plan.strategy).toBe('index_scan');
      expect(plan.operation).toBe('in');

      const result = await planner.executePlan('users', filter, plan);
      expect(result.docIds).toHaveLength(6);
      expect(result.docIds).toContain('user1'); // NYC
      expect(result.docIds).toContain('user2'); // LA
      expect(result.docIds).toContain('user3'); // NYC
      expect(result.docIds).toContain('user5'); // NYC
      expect(result.docIds).toContain('user6'); // LA
      expect(result.docIds).toContain('user7'); // NYC
    });

    it('should find users with specific statuses', async () => {
      const filter = { status: { $in: ['inactive', 'pending'] } };
      const plan = await planner.createPlan('users', filter);

      const result = await planner.executePlan('users', filter, plan);
      expect(result.docIds).toHaveLength(3);
      expect(result.docIds).toContain('user3'); // inactive
      expect(result.docIds).toContain('user5'); // pending
      expect(result.docIds).toContain('user8'); // inactive
    });
  });

  describe('Compound queries with residual filter', () => {
    it('should use index and create residual filter', async () => {
      const filter = { age: 28, status: 'active' };
      const plan = await planner.createPlan('users', filter);

      // Should use one of the indexed fields
      expect(plan.strategy).toBe('index_scan');
      expect(plan.residualFilter).toBeDefined();

      const result = await planner.executePlan('users', filter, plan);

      // Index scan returns candidates, residual filter narrows down
      // If using age index: returns user1, user3, user6 (all age 28)
      // If using status index: returns user1, user2, user4, user6, user7 (all active)
      expect(result.docIds).toBeDefined();
      expect(result.docIds!.length).toBeGreaterThan(0);
      expect(result.exact).toBe(false); // Has residual filter
    });

    it('should handle mixed indexed and non-indexed fields', async () => {
      const filter = { age: { $gte: 30 }, name: 'Bob' };
      const plan = await planner.createPlan('users', filter);

      expect(plan.strategy).toBe('index_scan');
      expect(plan.field).toBe('age');
      expect(plan.residualFilter).toEqual({ name: 'Bob' });

      const result = await planner.executePlan('users', filter, plan);
      // Returns all users age >= 30, then residual filter for name='Bob'
      expect(result.docIds).toContain('user2'); // Bob, age 35
    });
  });

  describe('Full scan fallback', () => {
    it('should fall back to full scan for non-indexed field', async () => {
      const filter = { name: 'Alice' };
      const plan = await planner.createPlan('users', filter);

      expect(plan.strategy).toBe('full_scan');
      expect(plan.explanation).toContain('No index available');
    });

    it('should fall back to full scan for complex operators', async () => {
      const filter = { $or: [{ age: 28 }, { status: 'pending' }] };
      const plan = await planner.createPlan('users', filter);

      expect(plan.strategy).toBe('full_scan');
    });
  });

  describe('Query explanation', () => {
    it('should provide useful explanation for index scan', async () => {
      const explanation = await planner.explain('users', { age: 28 });

      expect(explanation).toContain('Strategy: index_scan');
      expect(explanation).toContain('Index: age_1');
      expect(explanation).toContain('Operation: eq');
      expect(explanation).toContain('Selectivity');
    });

    it('should provide useful explanation for range query', async () => {
      const explanation = await planner.explain('users', { age: { $gte: 18, $lte: 65 } });

      expect(explanation).toContain('Strategy: index_scan');
      expect(explanation).toContain('Operation: range');
    });

    it('should provide useful explanation for full scan', async () => {
      const explanation = await planner.explain('users', { name: 'Alice' });

      expect(explanation).toContain('Strategy: full_scan');
      expect(explanation).toContain('No index available');
    });
  });
});

// ============================================================================
// Integration Test: Index Updates
// ============================================================================

describe('QueryPlanner Integration - Index Updates', () => {
  let indexManager: IndexManager;
  let planner: QueryPlanner;
  let storage: StorageBackend;

  beforeEach(async () => {
    storage = createMockStorage();
    indexManager = new IndexManager('testdb', 'products', storage);
    planner = new QueryPlanner(indexManager);

    await indexManager.createIndex({ price: 1 });
    await indexManager.createIndex({ category: 1 });
  });

  it('should reflect document insertions in query results', async () => {
    // Initially no documents
    let result = await planner.executePlan(
      'products',
      { price: 99 },
      await planner.createPlan('products', { price: 99 })
    );
    expect(result.docIds).toEqual([]);

    // Add a document
    await indexManager.indexDocument({ _id: 'prod1', price: 99, category: 'electronics' });

    // Now should find it
    result = await planner.executePlan(
      'products',
      { price: 99 },
      await planner.createPlan('products', { price: 99 })
    );
    expect(result.docIds).toEqual(['prod1']);
  });

  it('should reflect document deletions in query results', async () => {
    // Add documents
    await indexManager.indexDocument({ _id: 'prod1', price: 99, category: 'electronics' });
    await indexManager.indexDocument({ _id: 'prod2', price: 99, category: 'clothing' });

    // Both should be found
    let result = await planner.executePlan(
      'products',
      { price: 99 },
      await planner.createPlan('products', { price: 99 })
    );
    expect(result.docIds).toHaveLength(2);

    // Remove one document
    await indexManager.unindexDocument({ _id: 'prod1', price: 99, category: 'electronics' });

    // Only one should remain
    result = await planner.executePlan(
      'products',
      { price: 99 },
      await planner.createPlan('products', { price: 99 })
    );
    expect(result.docIds).toEqual(['prod2']);
  });
});

// ============================================================================
// Performance Characteristics
// ============================================================================

describe('QueryPlanner - Selectivity Estimates', () => {
  let indexManager: IndexManager;
  let planner: QueryPlanner;
  let storage: StorageBackend;

  beforeEach(async () => {
    storage = createMockStorage();
    indexManager = new IndexManager('testdb', 'data', storage);
    planner = new QueryPlanner(indexManager);

    await indexManager.createIndex({ field: 1 });
  });

  it('should estimate equality as highly selective (1%)', async () => {
    const plan = await planner.createPlan('data', { field: 'value' });
    expect(plan.estimatedSelectivity).toBe(0.01);
  });

  it('should estimate $in as moderately selective (10%)', async () => {
    const plan = await planner.createPlan('data', { field: { $in: ['a', 'b', 'c'] } });
    expect(plan.estimatedSelectivity).toBe(0.1);
  });

  it('should estimate range as less selective (30%)', async () => {
    const plan = await planner.createPlan('data', { field: { $gt: 100 } });
    expect(plan.estimatedSelectivity).toBe(0.3);
  });

  it('should estimate full scan as not selective (100%)', async () => {
    const plan = await planner.createPlan('data', { unindexedField: 'value' });
    expect(plan.estimatedSelectivity).toBe(1.0);
  });
});
