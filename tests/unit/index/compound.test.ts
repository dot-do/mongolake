/**
 * Compound Index Tests
 *
 * Comprehensive tests for multi-field compound index functionality:
 * - Compound index metadata storage
 * - Index key generation for multiple fields
 * - B-tree structure for compound lookups
 * - Query planner integration
 * - Field ordering support (ascending/descending)
 * - Unique constraint enforcement
 * - Sparse index behavior
 * - Index intersection logic
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  CompoundIndex,
  intersectIndexResults,
  unionIndexResults,
  parseIndexSpec,
  generateCompoundIndexName,
  type CompoundIndexField,
} from '../../../src/index/compound.js';
import { IndexManager } from '../../../src/index/index-manager.js';
import { QueryPlanner } from '../../../src/index/query-planner.js';
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
// Test 1: Compound Index Basic Operations
// ============================================================================

describe('CompoundIndex - Basic Operations', () => {
  let index: CompoundIndex;

  beforeEach(() => {
    index = new CompoundIndex(
      'name_1_age_-1',
      [
        { field: 'name', direction: 1 },
        { field: 'age', direction: -1 },
      ]
    );
  });

  it('should create a compound index with correct metadata', () => {
    expect(index.name).toBe('name_1_age_-1');
    expect(index.fields).toHaveLength(2);
    expect(index.fields[0]).toEqual({ field: 'name', direction: 1 });
    expect(index.fields[1]).toEqual({ field: 'age', direction: -1 });
    expect(index.unique).toBe(false);
    expect(index.sparse).toBe(false);
  });

  it('should throw error for empty fields array', () => {
    expect(() => new CompoundIndex('empty', [])).toThrow(
      'Compound index must have at least one field'
    );
  });

  it('should index and search documents', () => {
    index.indexDocument('doc1', { _id: 'doc1', name: 'Alice', age: 30 });
    index.indexDocument('doc2', { _id: 'doc2', name: 'Alice', age: 25 });
    index.indexDocument('doc3', { _id: 'doc3', name: 'Bob', age: 35 });

    // Exact match search
    const result = index.search(['Alice', 30]);
    expect(result).toEqual(['doc1']);

    // Another exact match
    const result2 = index.search(['Bob', 35]);
    expect(result2).toEqual(['doc3']);
  });

  it('should return empty for non-existent key', () => {
    index.indexDocument('doc1', { _id: 'doc1', name: 'Alice', age: 30 });

    const result = index.search(['NonExistent', 99]);
    expect(result).toEqual([]);
  });

  it('should handle unindexing documents', () => {
    const doc = { _id: 'doc1', name: 'Alice', age: 30 };
    index.indexDocument('doc1', doc);

    expect(index.search(['Alice', 30])).toEqual(['doc1']);

    index.unindexDocument('doc1', doc);
    expect(index.search(['Alice', 30])).toEqual([]);
  });

  it('should track index size correctly', () => {
    expect(index.isEmpty).toBe(true);
    expect(index.size).toBe(0);

    index.indexDocument('doc1', { _id: 'doc1', name: 'Alice', age: 30 });
    expect(index.isEmpty).toBe(false);
    expect(index.size).toBe(1);

    index.indexDocument('doc2', { _id: 'doc2', name: 'Bob', age: 25 });
    expect(index.size).toBe(2);
  });
});

// ============================================================================
// Test 2: Field Ordering Support (Ascending/Descending)
// ============================================================================

describe('CompoundIndex - Field Ordering', () => {
  it('should order by ascending field correctly', () => {
    const index = new CompoundIndex(
      'name_1',
      [{ field: 'name', direction: 1 }]
    );

    index.indexDocument('doc1', { _id: 'doc1', name: 'Charlie' });
    index.indexDocument('doc2', { _id: 'doc2', name: 'Alice' });
    index.indexDocument('doc3', { _id: 'doc3', name: 'Bob' });

    const entries = index.entries();
    const names = entries.map(([key]) => key[0]);

    expect(names).toEqual(['Alice', 'Bob', 'Charlie']);
  });

  it('should order by descending field correctly', () => {
    const index = new CompoundIndex(
      'age_-1',
      [{ field: 'age', direction: -1 }]
    );

    index.indexDocument('doc1', { _id: 'doc1', age: 25 });
    index.indexDocument('doc2', { _id: 'doc2', age: 35 });
    index.indexDocument('doc3', { _id: 'doc3', age: 30 });

    const entries = index.entries();
    const ages = entries.map(([key]) => key[0]);

    expect(ages).toEqual([35, 30, 25]);
  });

  it('should handle compound ascending/descending ordering', () => {
    const index = new CompoundIndex(
      'name_1_age_-1',
      [
        { field: 'name', direction: 1 },
        { field: 'age', direction: -1 },
      ]
    );

    index.indexDocument('doc1', { _id: 'doc1', name: 'Alice', age: 30 });
    index.indexDocument('doc2', { _id: 'doc2', name: 'Alice', age: 25 });
    index.indexDocument('doc3', { _id: 'doc3', name: 'Alice', age: 35 });
    index.indexDocument('doc4', { _id: 'doc4', name: 'Bob', age: 28 });

    const entries = index.entries();

    // Alice entries should be ordered by age descending
    expect(entries[0]![0]).toEqual(['Alice', 35]);
    expect(entries[1]![0]).toEqual(['Alice', 30]);
    expect(entries[2]![0]).toEqual(['Alice', 25]);
    // Bob comes after Alice
    expect(entries[3]![0]).toEqual(['Bob', 28]);
  });
});

// ============================================================================
// Test 3: Prefix Search
// ============================================================================

describe('CompoundIndex - Prefix Search', () => {
  let index: CompoundIndex;

  beforeEach(() => {
    index = new CompoundIndex(
      'name_1_age_1_city_1',
      [
        { field: 'name', direction: 1 },
        { field: 'age', direction: 1 },
        { field: 'city', direction: 1 },
      ]
    );

    index.indexDocument('doc1', { _id: 'doc1', name: 'Alice', age: 30, city: 'NYC' });
    index.indexDocument('doc2', { _id: 'doc2', name: 'Alice', age: 30, city: 'LA' });
    index.indexDocument('doc3', { _id: 'doc3', name: 'Alice', age: 25, city: 'NYC' });
    index.indexDocument('doc4', { _id: 'doc4', name: 'Bob', age: 30, city: 'NYC' });
    index.indexDocument('doc5', { _id: 'doc5', name: 'Bob', age: 35, city: 'Chicago' });
  });

  it('should search by first field prefix', () => {
    const result = index.searchByPrefix([
      { field: 'name', value: 'Alice', op: 'eq' },
    ]);

    expect(result).toHaveLength(3);
    expect(result).toContain('doc1');
    expect(result).toContain('doc2');
    expect(result).toContain('doc3');
  });

  it('should search by two-field prefix', () => {
    const result = index.searchByPrefix([
      { field: 'name', value: 'Alice', op: 'eq' },
      { field: 'age', value: 30, op: 'eq' },
    ]);

    expect(result).toHaveLength(2);
    expect(result).toContain('doc1');
    expect(result).toContain('doc2');
  });

  it('should search by full compound key', () => {
    const result = index.searchByPrefix([
      { field: 'name', value: 'Alice', op: 'eq' },
      { field: 'age', value: 30, op: 'eq' },
      { field: 'city', value: 'NYC', op: 'eq' },
    ]);

    expect(result).toEqual(['doc1']);
  });

  it('should return all documents when no conditions', () => {
    const result = index.searchByPrefix([]);
    expect(result).toHaveLength(5);
  });
});

// ============================================================================
// Test 4: Range Queries on Compound Index
// ============================================================================

describe('CompoundIndex - Range Queries', () => {
  let index: CompoundIndex;

  beforeEach(() => {
    index = new CompoundIndex(
      'name_1_age_1',
      [
        { field: 'name', direction: 1 },
        { field: 'age', direction: 1 },
      ]
    );

    index.indexDocument('doc1', { _id: 'doc1', name: 'Alice', age: 20 });
    index.indexDocument('doc2', { _id: 'doc2', name: 'Alice', age: 25 });
    index.indexDocument('doc3', { _id: 'doc3', name: 'Alice', age: 30 });
    index.indexDocument('doc4', { _id: 'doc4', name: 'Alice', age: 35 });
    index.indexDocument('doc5', { _id: 'doc5', name: 'Bob', age: 28 });
  });

  it('should handle $gt range query on second field', () => {
    const result = index.searchByPrefix([
      { field: 'name', value: 'Alice', op: 'eq' },
      { field: 'age', value: 25, op: 'gt' },
    ]);

    expect(result).toHaveLength(2);
    expect(result).toContain('doc3'); // age 30
    expect(result).toContain('doc4'); // age 35
  });

  it('should handle $gte range query on second field', () => {
    const result = index.searchByPrefix([
      { field: 'name', value: 'Alice', op: 'eq' },
      { field: 'age', value: 25, op: 'gte' },
    ]);

    expect(result).toHaveLength(3);
    expect(result).toContain('doc2'); // age 25
    expect(result).toContain('doc3'); // age 30
    expect(result).toContain('doc4'); // age 35
  });

  it('should handle $lt range query on second field', () => {
    const result = index.searchByPrefix([
      { field: 'name', value: 'Alice', op: 'eq' },
      { field: 'age', value: 30, op: 'lt' },
    ]);

    expect(result).toHaveLength(2);
    expect(result).toContain('doc1'); // age 20
    expect(result).toContain('doc2'); // age 25
  });

  it('should handle $lte range query on second field', () => {
    const result = index.searchByPrefix([
      { field: 'name', value: 'Alice', op: 'eq' },
      { field: 'age', value: 30, op: 'lte' },
    ]);

    expect(result).toHaveLength(3);
    expect(result).toContain('doc1'); // age 20
    expect(result).toContain('doc2'); // age 25
    expect(result).toContain('doc3'); // age 30
  });
});

// ============================================================================
// Test 5: $in Operator Support
// ============================================================================

describe('CompoundIndex - $in Operator', () => {
  let index: CompoundIndex;

  beforeEach(() => {
    index = new CompoundIndex(
      'name_1_status_1',
      [
        { field: 'name', direction: 1 },
        { field: 'status', direction: 1 },
      ]
    );

    index.indexDocument('doc1', { _id: 'doc1', name: 'Alice', status: 'active' });
    index.indexDocument('doc2', { _id: 'doc2', name: 'Alice', status: 'inactive' });
    index.indexDocument('doc3', { _id: 'doc3', name: 'Alice', status: 'pending' });
    index.indexDocument('doc4', { _id: 'doc4', name: 'Bob', status: 'active' });
  });

  it('should handle $in operator on first field', () => {
    const result = index.searchByPrefix([
      { field: 'name', values: ['Alice', 'Bob'], op: 'in' },
    ]);

    expect(result).toHaveLength(4);
  });

  it('should handle $in operator with equality prefix', () => {
    const result = index.searchByPrefix([
      { field: 'name', value: 'Alice', op: 'eq' },
      { field: 'status', values: ['active', 'pending'], op: 'in' },
    ]);

    expect(result).toHaveLength(2);
    expect(result).toContain('doc1');
    expect(result).toContain('doc3');
  });
});

// ============================================================================
// Test 6: Unique Constraint Enforcement
// ============================================================================

describe('CompoundIndex - Unique Constraint', () => {
  let index: CompoundIndex;

  beforeEach(() => {
    index = new CompoundIndex(
      'name_1_email_1',
      [
        { field: 'name', direction: 1 },
        { field: 'email', direction: 1 },
      ],
      true // unique
    );
  });

  it('should allow different compound keys', () => {
    index.indexDocument('doc1', { _id: 'doc1', name: 'Alice', email: 'alice@example.com' });
    index.indexDocument('doc2', { _id: 'doc2', name: 'Alice', email: 'alice2@example.com' });
    index.indexDocument('doc3', { _id: 'doc3', name: 'Bob', email: 'alice@example.com' });

    expect(index.size).toBe(3);
  });

  it('should throw on duplicate compound key', () => {
    index.indexDocument('doc1', { _id: 'doc1', name: 'Alice', email: 'alice@example.com' });

    expect(() => {
      index.indexDocument('doc2', { _id: 'doc2', name: 'Alice', email: 'alice@example.com' });
    }).toThrow(/Duplicate key/);
  });

  it('should allow same key after deletion', () => {
    const doc = { _id: 'doc1', name: 'Alice', email: 'alice@example.com' };
    index.indexDocument('doc1', doc);
    index.unindexDocument('doc1', doc);

    // Should not throw
    index.indexDocument('doc2', { _id: 'doc2', name: 'Alice', email: 'alice@example.com' });
    expect(index.size).toBe(1);
  });
});

// ============================================================================
// Test 7: Sparse Index Behavior
// ============================================================================

describe('CompoundIndex - Sparse Index', () => {
  let index: CompoundIndex;

  beforeEach(() => {
    index = new CompoundIndex(
      'name_1_optional_1',
      [
        { field: 'name', direction: 1 },
        { field: 'optional', direction: 1 },
      ],
      false, // not unique
      true   // sparse
    );
  });

  it('should skip documents with undefined field', () => {
    index.indexDocument('doc1', { _id: 'doc1', name: 'Alice', optional: 'value' });
    index.indexDocument('doc2', { _id: 'doc2', name: 'Bob' }); // optional is undefined

    expect(index.size).toBe(1);
    expect(index.search(['Alice', 'value'])).toEqual(['doc1']);
  });

  it('should skip documents with null field', () => {
    index.indexDocument('doc1', { _id: 'doc1', name: 'Alice', optional: 'value' });
    index.indexDocument('doc2', { _id: 'doc2', name: 'Bob', optional: null });

    expect(index.size).toBe(1);
  });

  it('should index documents with all fields present', () => {
    index.indexDocument('doc1', { _id: 'doc1', name: 'Alice', optional: 'a' });
    index.indexDocument('doc2', { _id: 'doc2', name: 'Alice', optional: 'b' });

    expect(index.size).toBe(2);
  });
});

// ============================================================================
// Test 8: Index Filter Support Analysis
// ============================================================================

describe('CompoundIndex - canSupportFilter', () => {
  let index: CompoundIndex;

  beforeEach(() => {
    index = new CompoundIndex(
      'a_1_b_1_c_1',
      [
        { field: 'a', direction: 1 },
        { field: 'b', direction: 1 },
        { field: 'c', direction: 1 },
      ]
    );
  });

  it('should support filter on first field', () => {
    const result = index.canSupportFilter({ a: 1 });

    expect(result.canUse).toBe(true);
    expect(result.equalityFields).toEqual(['a']);
    expect(result.coveredFields).toEqual(['a']);
  });

  it('should support filter on first two fields', () => {
    const result = index.canSupportFilter({ a: 1, b: 2 });

    expect(result.canUse).toBe(true);
    expect(result.equalityFields).toEqual(['a', 'b']);
    expect(result.coveredFields).toEqual(['a', 'b']);
  });

  it('should support filter on all fields', () => {
    const result = index.canSupportFilter({ a: 1, b: 2, c: 3 });

    expect(result.canUse).toBe(true);
    expect(result.equalityFields).toEqual(['a', 'b', 'c']);
  });

  it('should support range query after equality prefix', () => {
    const result = index.canSupportFilter({ a: 1, b: { $gt: 5 } });

    expect(result.canUse).toBe(true);
    expect(result.equalityFields).toEqual(['a']);
    expect(result.rangeField).toBe('b');
  });

  it('should not support filter starting with non-first field', () => {
    const result = index.canSupportFilter({ b: 2 });

    expect(result.canUse).toBe(false);
  });

  it('should not support filter with gap in fields', () => {
    const result = index.canSupportFilter({ a: 1, c: 3 }); // missing b

    expect(result.canUse).toBe(true);
    expect(result.equalityFields).toEqual(['a']); // Only a is covered
    expect(result.coveredFields).toEqual(['a']);
  });

  it('should support $in as equality', () => {
    const result = index.canSupportFilter({ a: { $in: [1, 2, 3] } });

    expect(result.canUse).toBe(true);
    expect(result.equalityFields).toEqual(['a']);
  });
});

// ============================================================================
// Test 9: Serialization and Deserialization
// ============================================================================

describe('CompoundIndex - Serialization', () => {
  it('should serialize and deserialize correctly', () => {
    const original = new CompoundIndex(
      'name_1_age_-1',
      [
        { field: 'name', direction: 1 },
        { field: 'age', direction: -1 },
      ],
      true, // unique
      true  // sparse
    );

    original.indexDocument('doc1', { _id: 'doc1', name: 'Alice', age: 30 });
    original.indexDocument('doc2', { _id: 'doc2', name: 'Bob', age: 25 });

    const serialized = original.serialize();
    const restored = CompoundIndex.deserialize(serialized);

    expect(restored.name).toBe('name_1_age_-1');
    expect(restored.fields).toEqual(original.fields);
    expect(restored.unique).toBe(true);
    expect(restored.sparse).toBe(true);
    expect(restored.size).toBe(2);
    expect(restored.search(['Alice', 30])).toEqual(['doc1']);
    expect(restored.search(['Bob', 25])).toEqual(['doc2']);
  });

  it('should handle empty index serialization', () => {
    const original = new CompoundIndex(
      'empty',
      [{ field: 'a', direction: 1 }]
    );

    const serialized = original.serialize();
    const restored = CompoundIndex.deserialize(serialized);

    expect(restored.isEmpty).toBe(true);
  });

  it('should serialize to JSON and back', () => {
    const original = new CompoundIndex(
      'test',
      [{ field: 'x', direction: 1 }, { field: 'y', direction: -1 }]
    );

    original.indexDocument('doc1', { _id: 'doc1', x: 'a', y: 10 });

    const json = original.toJSON();
    const restored = CompoundIndex.fromJSON(json);

    expect(restored.search(['a', 10])).toEqual(['doc1']);
  });
});

// ============================================================================
// Test 10: Index Intersection and Union
// ============================================================================

describe('Index Intersection and Union', () => {
  describe('intersectIndexResults', () => {
    it('should return empty for empty input', () => {
      expect(intersectIndexResults([])).toEqual([]);
    });

    it('should return same result for single array', () => {
      expect(intersectIndexResults([['a', 'b', 'c']])).toEqual(['a', 'b', 'c']);
    });

    it('should intersect two arrays', () => {
      const result = intersectIndexResults([
        ['a', 'b', 'c', 'd'],
        ['b', 'c', 'e'],
      ]);

      expect(result.sort()).toEqual(['b', 'c']);
    });

    it('should intersect multiple arrays', () => {
      const result = intersectIndexResults([
        ['a', 'b', 'c', 'd'],
        ['b', 'c', 'd', 'e'],
        ['c', 'd', 'f'],
      ]);

      expect(result.sort()).toEqual(['c', 'd']);
    });

    it('should return empty for no common elements', () => {
      const result = intersectIndexResults([
        ['a', 'b'],
        ['c', 'd'],
      ]);

      expect(result).toEqual([]);
    });
  });

  describe('unionIndexResults', () => {
    it('should return empty for empty input', () => {
      expect(unionIndexResults([])).toEqual([]);
    });

    it('should return same result for single array', () => {
      expect(unionIndexResults([['a', 'b', 'c']])).toEqual(['a', 'b', 'c']);
    });

    it('should union two arrays without duplicates', () => {
      const result = unionIndexResults([
        ['a', 'b'],
        ['b', 'c'],
      ]);

      expect(result.sort()).toEqual(['a', 'b', 'c']);
    });

    it('should union multiple arrays', () => {
      const result = unionIndexResults([
        ['a'],
        ['b', 'c'],
        ['c', 'd'],
      ]);

      expect(result.sort()).toEqual(['a', 'b', 'c', 'd']);
    });
  });
});

// ============================================================================
// Test 11: Utility Functions
// ============================================================================

describe('Compound Index Utility Functions', () => {
  describe('parseIndexSpec', () => {
    it('should parse simple ascending spec', () => {
      const fields = parseIndexSpec({ name: 1 });

      expect(fields).toEqual([{ field: 'name', direction: 1 }]);
    });

    it('should parse compound spec', () => {
      const fields = parseIndexSpec({ name: 1, age: -1 });

      expect(fields).toEqual([
        { field: 'name', direction: 1 },
        { field: 'age', direction: -1 },
      ]);
    });

    it('should skip text index types', () => {
      const fields = parseIndexSpec({ name: 1, content: 'text' });

      expect(fields).toEqual([{ field: 'name', direction: 1 }]);
    });
  });

  describe('generateCompoundIndexName', () => {
    it('should generate name for single field', () => {
      expect(generateCompoundIndexName({ name: 1 })).toBe('name_1');
    });

    it('should generate name for compound fields', () => {
      expect(generateCompoundIndexName({ name: 1, age: -1 })).toBe('name_1_age_-1');
    });
  });
});

// ============================================================================
// Test 12: IndexManager Integration
// ============================================================================

describe('IndexManager - Compound Index Integration', () => {
  let indexManager: IndexManager;
  let storage: StorageBackend;

  beforeEach(async () => {
    storage = createMockStorage();
    indexManager = new IndexManager('testdb', 'users', storage);
  });

  it('should create a compound index', async () => {
    const name = await indexManager.createCompoundIndex({ name: 1, age: -1 });

    expect(name).toBe('name_1_age_-1');

    const indexes = await indexManager.listCompoundIndexes();
    expect(indexes).toHaveLength(1);
    expect(indexes[0]!.name).toBe('name_1_age_-1');
  });

  it('should index documents in compound index', async () => {
    await indexManager.createCompoundIndex({ name: 1, age: 1 });

    await indexManager.indexDocument({ _id: 'doc1', name: 'Alice', age: 30 });
    await indexManager.indexDocument({ _id: 'doc2', name: 'Alice', age: 25 });
    await indexManager.indexDocument({ _id: 'doc3', name: 'Bob', age: 30 });

    const index = await indexManager.getCompoundIndex('name_1_age_1');
    expect(index).toBeDefined();
    expect(index!.size).toBe(3);
  });

  it('should unindex documents from compound index', async () => {
    await indexManager.createCompoundIndex({ name: 1, age: 1 });

    const doc = { _id: 'doc1', name: 'Alice', age: 30 };
    await indexManager.indexDocument(doc);
    await indexManager.unindexDocument(doc);

    const index = await indexManager.getCompoundIndex('name_1_age_1');
    expect(index!.size).toBe(0);
  });

  it('should drop compound index', async () => {
    await indexManager.createCompoundIndex({ name: 1, age: 1 });

    const dropped = await indexManager.dropCompoundIndex('name_1_age_1');
    expect(dropped).toBe(true);

    const indexes = await indexManager.listCompoundIndexes();
    expect(indexes).toHaveLength(0);
  });

  it('should find best compound index for filter', async () => {
    await indexManager.createCompoundIndex({ name: 1, age: 1 });
    await indexManager.createCompoundIndex({ city: 1, status: 1 });

    const match = await indexManager.findBestCompoundIndex({ name: 'Alice', age: 30 });

    expect(match).toBeDefined();
    expect(match!.index.name).toBe('name_1_age_1');
    expect(match!.equalityFields).toEqual(['name', 'age']);
  });

  it('should scan compound index', async () => {
    await indexManager.createCompoundIndex({ name: 1, age: 1 });

    await indexManager.indexDocument({ _id: 'doc1', name: 'Alice', age: 30 });
    await indexManager.indexDocument({ _id: 'doc2', name: 'Alice', age: 25 });
    await indexManager.indexDocument({ _id: 'doc3', name: 'Bob', age: 30 });

    const result = await indexManager.scanCompoundIndex('name_1_age_1', {
      name: 'Alice',
    });

    expect(result.docIds).toHaveLength(2);
    expect(result.docIds).toContain('doc1');
    expect(result.docIds).toContain('doc2');
  });
});

// ============================================================================
// Test 13: Query Planner Integration with Compound Indexes
// ============================================================================

describe('QueryPlanner - Compound Index Integration', () => {
  let indexManager: IndexManager;
  let planner: QueryPlanner;
  let storage: StorageBackend;

  const users: Document[] = [
    { _id: 'user1', name: 'Alice', age: 28, status: 'active' },
    { _id: 'user2', name: 'Alice', age: 35, status: 'inactive' },
    { _id: 'user3', name: 'Bob', age: 28, status: 'active' },
    { _id: 'user4', name: 'Bob', age: 42, status: 'pending' },
  ];

  beforeEach(async () => {
    storage = createMockStorage();
    indexManager = new IndexManager('testdb', 'users', storage);
    planner = new QueryPlanner(indexManager);

    // Create compound index
    await indexManager.createCompoundIndex({ name: 1, age: 1 });

    // Index documents
    for (const user of users) {
      await indexManager.indexDocument(user);
    }
  });

  it('should use compound index for multi-field filter', async () => {
    const plan = await planner.createPlan('users', { name: 'Alice', age: 28 });

    expect(plan.strategy).toBe('index_scan');
    expect(plan.indexName).toBe('name_1_age_1');
  });

  it('should execute compound index scan correctly', async () => {
    const filter = { name: 'Alice' };
    const plan = await planner.createPlan('users', filter);
    const result = await planner.executePlan('users', filter, plan);

    expect(result.docIds).toHaveLength(2);
    expect(result.docIds).toContain('user1');
    expect(result.docIds).toContain('user2');
  });

  it('should use compound index for prefix match', async () => {
    const filter = { name: 'Bob' };
    const plan = await planner.createPlan('users', filter);

    expect(plan.strategy).toBe('index_scan');
    expect(plan.indexName).toBe('name_1_age_1');

    const result = await planner.executePlan('users', filter, plan);
    expect(result.docIds).toHaveLength(2);
  });

  it('should prefer compound index over single-field index when more selective', async () => {
    // Create single-field index
    await indexManager.createIndex({ name: 1 });

    // Compound index should be preferred for multi-field query
    const plan = await planner.createPlan('users', { name: 'Alice', age: 28 });

    expect(plan.strategy).toBe('index_scan');
    // Should use compound index because it covers more fields
    expect(plan.indexName).toBe('name_1_age_1');
  });
});

// ============================================================================
// Test 14: Edge Cases
// ============================================================================

describe('CompoundIndex - Edge Cases', () => {
  it('should handle null values in compound key', () => {
    const index = new CompoundIndex(
      'a_1_b_1',
      [
        { field: 'a', direction: 1 },
        { field: 'b', direction: 1 },
      ]
    );

    index.indexDocument('doc1', { _id: 'doc1', a: 'value', b: null });

    expect(index.search(['value', null])).toEqual(['doc1']);
  });

  it('should handle nested field paths', () => {
    const index = new CompoundIndex(
      'user.name_1_user.age_1',
      [
        { field: 'user.name', direction: 1 },
        { field: 'user.age', direction: 1 },
      ]
    );

    index.indexDocument('doc1', {
      _id: 'doc1',
      user: { name: 'Alice', age: 30 },
    });

    expect(index.search(['Alice', 30])).toEqual(['doc1']);
  });

  it('should handle Date values', () => {
    const index = new CompoundIndex(
      'name_1_created_1',
      [
        { field: 'name', direction: 1 },
        { field: 'created', direction: 1 },
      ]
    );

    const date1 = new Date('2024-01-01');
    const date2 = new Date('2024-06-01');

    index.indexDocument('doc1', { _id: 'doc1', name: 'Alice', created: date1 });
    index.indexDocument('doc2', { _id: 'doc2', name: 'Alice', created: date2 });

    expect(index.search(['Alice', date1])).toEqual(['doc1']);
  });

  it('should handle numeric types correctly', () => {
    const index = new CompoundIndex(
      'type_1_value_1',
      [
        { field: 'type', direction: 1 },
        { field: 'value', direction: 1 },
      ]
    );

    index.indexDocument('doc1', { _id: 'doc1', type: 'int', value: 42 });
    index.indexDocument('doc2', { _id: 'doc2', type: 'int', value: -10 });
    index.indexDocument('doc3', { _id: 'doc3', type: 'int', value: 0 });
    index.indexDocument('doc4', { _id: 'doc4', type: 'float', value: 3.14 });

    const entries = index.entries();

    // Float should come before int (alphabetically)
    expect(entries[0]![0][0]).toBe('float');

    // Int entries should be ordered by value
    const intEntries = entries.filter(([key]) => key[0] === 'int');
    const values = intEntries.map(([key]) => key[1]);
    expect(values).toEqual([-10, 0, 42]);
  });

  it('should handle multiple documents with same key (non-unique)', () => {
    const index = new CompoundIndex(
      'category_1_status_1',
      [
        { field: 'category', direction: 1 },
        { field: 'status', direction: 1 },
      ]
    );

    index.indexDocument('doc1', { _id: 'doc1', category: 'A', status: 'active' });
    index.indexDocument('doc2', { _id: 'doc2', category: 'A', status: 'active' });
    index.indexDocument('doc3', { _id: 'doc3', category: 'A', status: 'active' });

    const result = index.search(['A', 'active']);
    expect(result).toHaveLength(3);
    expect(result).toContain('doc1');
    expect(result).toContain('doc2');
    expect(result).toContain('doc3');
  });

  it('should clear index correctly', () => {
    const index = new CompoundIndex(
      'a_1_b_1',
      [
        { field: 'a', direction: 1 },
        { field: 'b', direction: 1 },
      ]
    );

    index.indexDocument('doc1', { _id: 'doc1', a: 1, b: 2 });
    index.indexDocument('doc2', { _id: 'doc2', a: 3, b: 4 });

    expect(index.size).toBe(2);

    index.clear();

    expect(index.size).toBe(0);
    expect(index.isEmpty).toBe(true);
    expect(index.search([1, 2])).toEqual([]);
  });
});

// ============================================================================
// Test 15: coversFields and getFieldNames
// ============================================================================

describe('CompoundIndex - Field Coverage', () => {
  let index: CompoundIndex;

  beforeEach(() => {
    index = new CompoundIndex(
      'a_1_b_1_c_1',
      [
        { field: 'a', direction: 1 },
        { field: 'b', direction: 1 },
        { field: 'c', direction: 1 },
      ]
    );
  });

  it('should return field names in order', () => {
    expect(index.getFieldNames()).toEqual(['a', 'b', 'c']);
  });

  it('should report coverage for subset of fields', () => {
    expect(index.coversFields(['a'])).toBe(true);
    expect(index.coversFields(['a', 'b'])).toBe(true);
    expect(index.coversFields(['a', 'b', 'c'])).toBe(true);
    expect(index.coversFields(['b', 'c'])).toBe(true);
  });

  it('should report non-coverage for fields not in index', () => {
    expect(index.coversFields(['d'])).toBe(false);
    expect(index.coversFields(['a', 'd'])).toBe(false);
  });
});
