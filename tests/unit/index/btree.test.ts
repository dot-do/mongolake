/**
 * B-tree Index Tests
 *
 * Comprehensive tests for the B-tree data structure covering:
 * - Basic operations (insert, search, delete)
 * - Range queries
 * - Serialization and deserialization
 * - Edge cases
 * - Unique index constraints
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { BTree, BTreeNode } from '../../../src/index/btree.js';

// ============================================================================
// BTreeNode Tests
// ============================================================================

describe('BTreeNode', () => {
  it('should create a leaf node by default', () => {
    const node = new BTreeNode<number>();
    expect(node.isLeaf).toBe(true);
    expect(node.keys).toEqual([]);
    expect(node.docIds).toEqual([]);
    expect(node.children).toEqual([]);
    expect(node.id).toBeDefined();
  });

  it('should create an internal node when specified', () => {
    const node = new BTreeNode<number>(false);
    expect(node.isLeaf).toBe(false);
  });

  it('should serialize correctly', () => {
    const node = new BTreeNode<number>(true);
    node.keys = [1, 2, 3];
    node.docIds = [['a'], ['b'], ['c']];

    const serialized = node.serialize();

    expect(serialized.id).toBe(node.id);
    expect(serialized.isLeaf).toBe(true);
    expect(serialized.keys).toEqual([1, 2, 3]);
    expect(serialized.docIds).toEqual([['a'], ['b'], ['c']]);
    expect(serialized.childIds).toEqual([]);
  });

  it('should deserialize correctly', () => {
    const data = {
      id: 'test-id',
      isLeaf: false,
      keys: [10, 20],
      docIds: [['x'], ['y']],
      childIds: ['child1', 'child2', 'child3'],
    };

    const node = BTreeNode.deserialize<number>(data);

    expect(node.id).toBe('test-id');
    expect(node.isLeaf).toBe(false);
    expect(node.keys).toEqual([10, 20]);
    expect(node.docIds).toEqual([['x'], ['y']]);
    expect(node.childIds).toEqual(['child1', 'child2', 'child3']);
  });
});

// ============================================================================
// BTree Basic Operations
// ============================================================================

describe('BTree - Basic Operations', () => {
  let tree: BTree<number>;

  beforeEach(() => {
    tree = new BTree<number>('test_index', 'age', 3);
  });

  describe('constructor', () => {
    it('should create an empty tree', () => {
      expect(tree.isEmpty).toBe(true);
      expect(tree.size).toBe(0);
      expect(tree.root).toBeNull();
    });

    it('should set index properties', () => {
      expect(tree.name).toBe('test_index');
      expect(tree.field).toBe('age');
      expect(tree.minDegree).toBe(3);
      expect(tree.unique).toBe(false);
    });

    it('should throw for invalid minDegree', () => {
      expect(() => new BTree('test', 'field', 1)).toThrow('Minimum degree must be at least 2');
    });
  });

  describe('insert and search', () => {
    it('should insert and find a single key', () => {
      tree.insert(25, 'doc1');

      expect(tree.isEmpty).toBe(false);
      expect(tree.size).toBe(1);
      expect(tree.search(25)).toEqual(['doc1']);
    });

    it('should handle multiple documents for same key', () => {
      tree.insert(25, 'doc1');
      tree.insert(25, 'doc2');
      tree.insert(25, 'doc3');

      expect(tree.search(25)).toEqual(['doc1', 'doc2', 'doc3']);
      expect(tree.size).toBe(1);
    });

    it('should not duplicate docIds for same key', () => {
      tree.insert(25, 'doc1');
      tree.insert(25, 'doc1');

      expect(tree.search(25)).toEqual(['doc1']);
    });

    it('should insert multiple different keys', () => {
      tree.insert(10, 'doc1');
      tree.insert(20, 'doc2');
      tree.insert(5, 'doc3');
      tree.insert(15, 'doc4');
      tree.insert(25, 'doc5');

      expect(tree.search(10)).toEqual(['doc1']);
      expect(tree.search(20)).toEqual(['doc2']);
      expect(tree.search(5)).toEqual(['doc3']);
      expect(tree.search(15)).toEqual(['doc4']);
      expect(tree.search(25)).toEqual(['doc5']);
      expect(tree.size).toBe(5);
    });

    it('should return empty array for non-existent key', () => {
      tree.insert(10, 'doc1');
      expect(tree.search(999)).toEqual([]);
    });

    it('should handle has() correctly', () => {
      tree.insert(10, 'doc1');
      expect(tree.has(10)).toBe(true);
      expect(tree.has(999)).toBe(false);
    });
  });

  describe('insert - tree growth', () => {
    it('should handle many insertions (causing splits)', () => {
      // With minDegree=3, max keys per node is 5
      // Insert enough to cause multiple splits
      for (let i = 0; i < 50; i++) {
        tree.insert(i, `doc${i}`);
      }

      expect(tree.size).toBe(50);

      // Verify all keys are findable
      for (let i = 0; i < 50; i++) {
        expect(tree.search(i)).toEqual([`doc${i}`]);
      }
    });

    it('should handle random insertion order', () => {
      const keys = [30, 10, 45, 5, 25, 15, 35, 40, 20, 50];
      for (const key of keys) {
        tree.insert(key, `doc${key}`);
      }

      expect(tree.size).toBe(10);
      for (const key of keys) {
        expect(tree.search(key)).toEqual([`doc${key}`]);
      }
    });
  });
});

// ============================================================================
// BTree Delete Operations
// ============================================================================

describe('BTree - Delete Operations', () => {
  let tree: BTree<number>;

  beforeEach(() => {
    tree = new BTree<number>('test_index', 'age', 3);
  });

  describe('delete from leaf', () => {
    it('should delete a single key', () => {
      tree.insert(10, 'doc1');
      expect(tree.delete(10)).toBe(true);
      expect(tree.search(10)).toEqual([]);
      expect(tree.isEmpty).toBe(true);
    });

    it('should delete specific docId from key', () => {
      tree.insert(10, 'doc1');
      tree.insert(10, 'doc2');
      tree.insert(10, 'doc3');

      expect(tree.delete(10, 'doc2')).toBe(true);
      expect(tree.search(10)).toEqual(['doc1', 'doc3']);
    });

    it('should return false for non-existent key', () => {
      tree.insert(10, 'doc1');
      expect(tree.delete(999)).toBe(false);
    });

    it('should return false for non-existent docId', () => {
      tree.insert(10, 'doc1');
      expect(tree.delete(10, 'nonexistent')).toBe(false);
    });
  });

  describe('delete causing tree reorganization', () => {
    it('should handle deletion with borrowing from sibling', () => {
      // Insert enough keys to have multiple nodes
      for (let i = 1; i <= 20; i++) {
        tree.insert(i, `doc${i}`);
      }

      // Delete keys to trigger borrowing
      expect(tree.delete(1)).toBe(true);
      expect(tree.delete(2)).toBe(true);

      // Verify remaining keys
      expect(tree.search(1)).toEqual([]);
      expect(tree.search(2)).toEqual([]);
      expect(tree.search(3)).toEqual(['doc3']);
    });

    it('should handle deletion with merging', () => {
      // Insert keys
      for (let i = 1; i <= 15; i++) {
        tree.insert(i, `doc${i}`);
      }

      // Delete many keys to trigger merges
      for (let i = 1; i <= 10; i++) {
        expect(tree.delete(i)).toBe(true);
      }

      // Verify remaining keys
      for (let i = 11; i <= 15; i++) {
        expect(tree.search(i)).toEqual([`doc${i}`]);
      }
    });

    it('should handle emptying the tree', () => {
      for (let i = 1; i <= 10; i++) {
        tree.insert(i, `doc${i}`);
      }

      for (let i = 1; i <= 10; i++) {
        expect(tree.delete(i)).toBe(true);
      }

      expect(tree.isEmpty).toBe(true);
      expect(tree.root).toBeNull();
    });
  });
});

// ============================================================================
// BTree Range Queries
// ============================================================================

describe('BTree - Range Queries', () => {
  let tree: BTree<number>;

  beforeEach(() => {
    tree = new BTree<number>('test_index', 'age', 3);
    // Insert values 10, 20, 30, 40, 50
    tree.insert(30, 'doc30');
    tree.insert(10, 'doc10');
    tree.insert(50, 'doc50');
    tree.insert(20, 'doc20');
    tree.insert(40, 'doc40');
  });

  describe('range()', () => {
    it('should return all entries for unbounded range', () => {
      const entries = tree.range(null, null);

      expect(entries).toEqual([
        [10, ['doc10']],
        [20, ['doc20']],
        [30, ['doc30']],
        [40, ['doc40']],
        [50, ['doc50']],
      ]);
    });

    it('should return entries greater than or equal to min', () => {
      const entries = tree.range(25, null);

      expect(entries).toEqual([
        [30, ['doc30']],
        [40, ['doc40']],
        [50, ['doc50']],
      ]);
    });

    it('should return entries less than or equal to max', () => {
      const entries = tree.range(null, 35);

      expect(entries).toEqual([
        [10, ['doc10']],
        [20, ['doc20']],
        [30, ['doc30']],
      ]);
    });

    it('should return entries in bounded range', () => {
      const entries = tree.range(20, 40);

      expect(entries).toEqual([
        [20, ['doc20']],
        [30, ['doc30']],
        [40, ['doc40']],
      ]);
    });

    it('should return empty array for non-overlapping range', () => {
      const entries = tree.range(100, 200);
      expect(entries).toEqual([]);
    });

    it('should handle single-value range', () => {
      const entries = tree.range(30, 30);
      expect(entries).toEqual([[30, ['doc30']]]);
    });
  });

  describe('entries()', () => {
    it('should return all entries in sorted order', () => {
      const entries = tree.entries();

      expect(entries.map(([k]) => k)).toEqual([10, 20, 30, 40, 50]);
    });
  });

  describe('min() and max()', () => {
    it('should return minimum key', () => {
      expect(tree.min()).toBe(10);
    });

    it('should return maximum key', () => {
      expect(tree.max()).toBe(50);
    });

    it('should return undefined for empty tree', () => {
      const emptyTree = new BTree<number>('test', 'field', 3);
      expect(emptyTree.min()).toBeUndefined();
      expect(emptyTree.max()).toBeUndefined();
    });
  });
});

// ============================================================================
// BTree Unique Index
// ============================================================================

describe('BTree - Unique Index', () => {
  let tree: BTree<number>;

  beforeEach(() => {
    tree = new BTree<number>('test_unique', 'email', 3, undefined, true);
  });

  it('should allow inserting unique keys', () => {
    tree.insert(1, 'doc1');
    tree.insert(2, 'doc2');
    tree.insert(3, 'doc3');

    expect(tree.size).toBe(3);
  });

  it('should throw on duplicate key insertion', () => {
    tree.insert(1, 'doc1');

    expect(() => tree.insert(1, 'doc2')).toThrow('Duplicate key value: 1');
  });

  it('should allow same key after deletion', () => {
    tree.insert(1, 'doc1');
    tree.delete(1);
    tree.insert(1, 'doc2');

    expect(tree.search(1)).toEqual(['doc2']);
  });
});

// ============================================================================
// BTree Serialization
// ============================================================================

describe('BTree - Serialization', () => {
  let tree: BTree<number>;

  beforeEach(() => {
    tree = new BTree<number>('age_index', 'age', 3);
    for (let i = 1; i <= 20; i++) {
      tree.insert(i, `doc${i}`);
    }
  });

  describe('serialize and deserialize', () => {
    it('should serialize tree to plain object', () => {
      const serialized = tree.serialize();

      expect(serialized.name).toBe('age_index');
      expect(serialized.field).toBe('age');
      expect(serialized.minDegree).toBe(3);
      expect(serialized.unique).toBe(false);
      expect(serialized.rootId).toBeDefined();
      expect(serialized.nodes.length).toBeGreaterThan(0);
    });

    it('should deserialize back to working tree', () => {
      const serialized = tree.serialize();
      const restored = BTree.deserialize<number>(serialized);

      expect(restored.name).toBe('age_index');
      expect(restored.field).toBe('age');
      expect(restored.size).toBe(20);

      // Verify all values are restored
      for (let i = 1; i <= 20; i++) {
        expect(restored.search(i)).toEqual([`doc${i}`]);
      }
    });

    it('should handle empty tree serialization', () => {
      const emptyTree = new BTree<number>('empty', 'field', 3);
      const serialized = emptyTree.serialize();

      expect(serialized.rootId).toBeNull();
      expect(serialized.nodes).toEqual([]);

      const restored = BTree.deserialize<number>(serialized);
      expect(restored.isEmpty).toBe(true);
    });
  });

  describe('toJSON and fromJSON', () => {
    it('should convert to JSON string and back', () => {
      const json = tree.toJSON();
      const restored = BTree.fromJSON<number>(json);

      expect(restored.size).toBe(20);
      expect(restored.search(10)).toEqual(['doc10']);
    });

    it('should preserve all properties in JSON', () => {
      const uniqueTree = new BTree<string>('unique_idx', 'email', 4, undefined, true);
      uniqueTree.insert('a@test.com', 'doc1');
      uniqueTree.insert('b@test.com', 'doc2');

      const json = uniqueTree.toJSON();
      const restored = BTree.fromJSON<string>(json);

      expect(restored.name).toBe('unique_idx');
      expect(restored.field).toBe('email');
      expect(restored.minDegree).toBe(4);
      expect(restored.unique).toBe(true);
    });
  });
});

// ============================================================================
// BTree String Keys
// ============================================================================

describe('BTree - String Keys', () => {
  let tree: BTree<string>;

  beforeEach(() => {
    tree = new BTree<string>('name_index', 'name', 3);
  });

  it('should handle string keys with natural ordering', () => {
    tree.insert('Charlie', 'doc1');
    tree.insert('Alice', 'doc2');
    tree.insert('Bob', 'doc3');

    const entries = tree.entries();
    expect(entries.map(([k]) => k)).toEqual(['Alice', 'Bob', 'Charlie']);
  });

  it('should search for exact string match', () => {
    tree.insert('Alice', 'doc1');
    tree.insert('Bob', 'doc2');

    expect(tree.search('Alice')).toEqual(['doc1']);
    expect(tree.search('alice')).toEqual([]); // case-sensitive
  });

  it('should handle range queries on strings', () => {
    tree.insert('Apple', 'doc1');
    tree.insert('Banana', 'doc2');
    tree.insert('Cherry', 'doc3');
    tree.insert('Date', 'doc4');

    const entries = tree.range('B', 'D');
    expect(entries.map(([k]) => k)).toEqual(['Banana', 'Cherry']);
  });
});

// ============================================================================
// BTree Custom Comparison
// ============================================================================

describe('BTree - Custom Comparison', () => {
  it('should support custom comparison function', () => {
    // Case-insensitive string comparison
    const compare = (a: string, b: string) =>
      a.toLowerCase().localeCompare(b.toLowerCase());

    const tree = new BTree<string>('ci_index', 'name', 3, compare);

    tree.insert('Alice', 'doc1');
    tree.insert('BOB', 'doc2');
    tree.insert('charlie', 'doc3');

    // Search should be case-insensitive
    expect(tree.search('alice')).toEqual(['doc1']);
    expect(tree.search('ALICE')).toEqual(['doc1']);
    expect(tree.search('bob')).toEqual(['doc2']);
  });

  it('should support reverse ordering', () => {
    const compare = (a: number, b: number) => b - a; // Reverse order

    const tree = new BTree<number>('reverse', 'value', 3, compare);

    tree.insert(10, 'doc1');
    tree.insert(30, 'doc2');
    tree.insert(20, 'doc3');

    const entries = tree.entries();
    expect(entries.map(([k]) => k)).toEqual([30, 20, 10]);
  });
});

// ============================================================================
// BTree Edge Cases
// ============================================================================

describe('BTree - Edge Cases', () => {
  let tree: BTree<unknown>;

  beforeEach(() => {
    tree = new BTree('test', 'field', 3);
  });

  it('should handle null keys', () => {
    tree.insert(null, 'doc1');
    expect(tree.search(null)).toEqual(['doc1']);
  });

  it('should handle undefined keys', () => {
    tree.insert(undefined, 'doc1');
    expect(tree.search(undefined)).toEqual(['doc1']);
  });

  it('should handle mixed types (natural ordering)', () => {
    tree.insert(10, 'doc1');
    tree.insert('abc', 'doc2');
    tree.insert(null, 'doc3');

    expect(tree.search(10)).toEqual(['doc1']);
    expect(tree.search('abc')).toEqual(['doc2']);
    expect(tree.search(null)).toEqual(['doc3']);
  });

  it('should clear tree correctly', () => {
    tree.insert(1, 'doc1');
    tree.insert(2, 'doc2');

    tree.clear();

    expect(tree.isEmpty).toBe(true);
    expect(tree.size).toBe(0);
    expect(tree.search(1)).toEqual([]);
  });

  it('should handle very deep tree', () => {
    const deepTree = new BTree<number>('deep', 'field', 2); // Minimum degree

    // Insert many values to create deep tree
    for (let i = 0; i < 100; i++) {
      deepTree.insert(i, `doc${i}`);
    }

    expect(deepTree.size).toBe(100);

    // Verify all values are accessible
    for (let i = 0; i < 100; i++) {
      expect(deepTree.search(i)).toEqual([`doc${i}`]);
    }
  });

  it('should handle negative numbers', () => {
    const numTree = new BTree<number>('num', 'value', 3);

    numTree.insert(-10, 'doc1');
    numTree.insert(0, 'doc2');
    numTree.insert(10, 'doc3');

    const entries = numTree.entries();
    expect(entries.map(([k]) => k)).toEqual([-10, 0, 10]);
  });

  it('should handle floating point numbers', () => {
    const numTree = new BTree<number>('float', 'value', 3);

    numTree.insert(1.5, 'doc1');
    numTree.insert(1.1, 'doc2');
    numTree.insert(1.9, 'doc3');

    const entries = numTree.entries();
    expect(entries.map(([k]) => k)).toEqual([1.1, 1.5, 1.9]);
  });
});

// ============================================================================
// BTree Large Dataset
// ============================================================================

describe('BTree - Large Dataset', () => {
  it('should handle 1000 insertions efficiently', () => {
    const tree = new BTree<number>('large', 'id', 64);

    for (let i = 0; i < 1000; i++) {
      tree.insert(i, `doc${i}`);
    }

    expect(tree.size).toBe(1000);

    // Verify random samples
    expect(tree.search(0)).toEqual(['doc0']);
    expect(tree.search(500)).toEqual(['doc500']);
    expect(tree.search(999)).toEqual(['doc999']);
  });

  it('should handle 1000 deletions', () => {
    const tree = new BTree<number>('large', 'id', 64);

    for (let i = 0; i < 1000; i++) {
      tree.insert(i, `doc${i}`);
    }

    // Delete half
    for (let i = 0; i < 500; i++) {
      tree.delete(i);
    }

    expect(tree.size).toBe(500);
    expect(tree.search(0)).toEqual([]);
    expect(tree.search(500)).toEqual(['doc500']);
  });

  it('should handle interleaved inserts and deletes', () => {
    const tree = new BTree<number>('mixed', 'id', 32);

    // Insert 500
    for (let i = 0; i < 500; i++) {
      tree.insert(i, `doc${i}`);
    }

    // Delete odd, insert 500-999
    for (let i = 0; i < 500; i++) {
      if (i % 2 === 1) {
        tree.delete(i);
      }
      tree.insert(500 + i, `doc${500 + i}`);
    }

    expect(tree.size).toBe(750); // 250 even + 500 new

    // Verify
    expect(tree.search(0)).toEqual(['doc0']); // even, kept
    expect(tree.search(1)).toEqual([]); // odd, deleted
    expect(tree.search(500)).toEqual(['doc500']); // new
  });
});

// ============================================================================
// BTree - Multiple DocIds Per Key
// ============================================================================

describe('BTree - Multiple DocIds Per Key', () => {
  let tree: BTree<string>;

  beforeEach(() => {
    tree = new BTree<string>('status_index', 'status', 3);
  });

  it('should store multiple docIds for same key', () => {
    tree.insert('active', 'doc1');
    tree.insert('active', 'doc2');
    tree.insert('active', 'doc3');
    tree.insert('inactive', 'doc4');

    expect(tree.search('active')).toEqual(['doc1', 'doc2', 'doc3']);
    expect(tree.search('inactive')).toEqual(['doc4']);
  });

  it('should delete specific docId', () => {
    tree.insert('pending', 'doc1');
    tree.insert('pending', 'doc2');
    tree.insert('pending', 'doc3');

    tree.delete('pending', 'doc2');

    expect(tree.search('pending')).toEqual(['doc1', 'doc3']);
  });

  it('should remove key when last docId is deleted', () => {
    tree.insert('test', 'doc1');
    tree.delete('test', 'doc1');

    expect(tree.search('test')).toEqual([]);
    expect(tree.has('test')).toBe(false);
  });

  it('should handle range query with multiple docIds', () => {
    tree.insert('a', 'doc1');
    tree.insert('a', 'doc2');
    tree.insert('b', 'doc3');
    tree.insert('c', 'doc4');
    tree.insert('c', 'doc5');

    const entries = tree.range('a', 'b');

    expect(entries).toEqual([
      ['a', ['doc1', 'doc2']],
      ['b', ['doc3']],
    ]);
  });
});
