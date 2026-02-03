/**
 * Update Operator Tests
 *
 * Tests for the applyUpdate function that handles:
 * - $set operator
 * - $unset operator
 * - $inc operator
 * - $push and $addToSet with $each
 * - $pull and $pop
 * - $rename
 * - Nested field updates
 * - Edge cases
 */

import { describe, it, expect } from 'vitest';
import { applyUpdate, extractFilterFields, createUpsertDocument } from '../../../src/utils/update.js';
import { ValidationError } from '../../../src/validation/index.js';

// =============================================================================
// Test Document Type
// =============================================================================

interface TestDocument {
  _id: string;
  name?: string;
  age?: number;
  score?: number;
  status?: string;
  active?: boolean;
  tags?: string[];
  scores?: number[];
  items?: Array<{ id: number; name: string }>;
  profile?: {
    firstName?: string;
    lastName?: string;
    settings?: {
      theme?: string;
      notifications?: boolean;
    };
  };
  metadata?: Record<string, unknown>;
  count?: number;
  oldField?: string;
  newField?: string;
}

// =============================================================================
// Helper Functions
// =============================================================================

function createDoc(id: string, data: Partial<Omit<TestDocument, '_id'>> = {}): TestDocument {
  return { _id: id, ...data };
}

// =============================================================================
// $set Operator
// =============================================================================

describe('applyUpdate - $set Operator', () => {
  it('should set a single field', () => {
    const doc = createDoc('1', { name: 'Alice' });
    const result = applyUpdate(doc, { $set: { name: 'Bob' } });

    expect(result.name).toBe('Bob');
  });

  it('should set multiple fields', () => {
    const doc = createDoc('1', { name: 'Alice' });
    const result = applyUpdate(doc, { $set: { name: 'Bob', age: 30 } });

    expect(result.name).toBe('Bob');
    expect(result.age).toBe(30);
  });

  it('should add a new field', () => {
    const doc = createDoc('1', { name: 'Alice' });
    const result = applyUpdate(doc, { $set: { age: 25 } });

    expect(result.name).toBe('Alice');
    expect(result.age).toBe(25);
  });

  it('should set field to null', () => {
    const doc = createDoc('1', { name: 'Alice' });
    const result = applyUpdate(doc, { $set: { name: null } });

    expect(result.name).toBeNull();
  });

  it('should set field to an array', () => {
    const doc = createDoc('1', {});
    const result = applyUpdate(doc, { $set: { tags: ['a', 'b', 'c'] } });

    expect(result.tags).toEqual(['a', 'b', 'c']);
  });

  it('should set field to an object', () => {
    const doc = createDoc('1', {});
    const result = applyUpdate(doc, { $set: { profile: { firstName: 'Alice' } } });

    expect(result.profile).toEqual({ firstName: 'Alice' });
  });

  it('should set boolean values', () => {
    const doc = createDoc('1', { active: true });
    const result = applyUpdate(doc, { $set: { active: false } });

    expect(result.active).toBe(false);
  });

  it('should set numeric values including zero', () => {
    const doc = createDoc('1', { score: 100 });
    const result = applyUpdate(doc, { $set: { score: 0 } });

    expect(result.score).toBe(0);
  });

  it('should not affect other fields', () => {
    const doc = createDoc('1', { name: 'Alice', age: 30 });
    const result = applyUpdate(doc, { $set: { name: 'Bob' } });

    expect(result.name).toBe('Bob');
    expect(result.age).toBe(30);
    expect(result._id).toBe('1');
  });
});

// =============================================================================
// $unset Operator
// =============================================================================

describe('applyUpdate - $unset Operator', () => {
  it('should remove a single field', () => {
    const doc = createDoc('1', { name: 'Alice', age: 30 });
    const result = applyUpdate(doc, { $unset: { age: '' } });

    expect(result.name).toBe('Alice');
    expect(result.age).toBeUndefined();
    expect('age' in result).toBe(false);
  });

  it('should remove multiple fields', () => {
    const doc = createDoc('1', { name: 'Alice', age: 30, status: 'active' });
    const result = applyUpdate(doc, { $unset: { age: '', status: '' } });

    expect(result.name).toBe('Alice');
    expect('age' in result).toBe(false);
    expect('status' in result).toBe(false);
  });

  it('should handle removing non-existent field', () => {
    const doc = createDoc('1', { name: 'Alice' });
    const result = applyUpdate(doc, { $unset: { age: '' } });

    expect(result.name).toBe('Alice');
    expect('age' in result).toBe(false);
  });

  it('should not affect other fields', () => {
    const doc = createDoc('1', { name: 'Alice', age: 30, status: 'active' });
    const result = applyUpdate(doc, { $unset: { age: '' } });

    expect(result.name).toBe('Alice');
    expect(result.status).toBe('active');
    expect(result._id).toBe('1');
  });

  it('should work with $unset value of 1 (MongoDB style)', () => {
    const doc = createDoc('1', { name: 'Alice', age: 30 });
    const result = applyUpdate(doc, { $unset: { age: 1 } });

    expect('age' in result).toBe(false);
  });

  it('should work with $unset value of true', () => {
    const doc = createDoc('1', { name: 'Alice', age: 30 });
    const result = applyUpdate(doc, { $unset: { age: true } });

    expect('age' in result).toBe(false);
  });
});

// =============================================================================
// $inc Operator
// =============================================================================

describe('applyUpdate - $inc Operator', () => {
  it('should increment an existing field', () => {
    const doc = createDoc('1', { count: 5 });
    const result = applyUpdate(doc, { $inc: { count: 1 } });

    expect(result.count).toBe(6);
  });

  it('should decrement with negative value', () => {
    const doc = createDoc('1', { count: 5 });
    const result = applyUpdate(doc, { $inc: { count: -2 } });

    expect(result.count).toBe(3);
  });

  it('should increment multiple fields', () => {
    const doc = createDoc('1', { count: 5, score: 100 });
    const result = applyUpdate(doc, { $inc: { count: 1, score: 10 } });

    expect(result.count).toBe(6);
    expect(result.score).toBe(110);
  });

  it('should initialize non-existent field to increment value', () => {
    const doc = createDoc('1', {});
    const result = applyUpdate(doc, { $inc: { count: 5 } });

    expect(result.count).toBe(5);
  });

  it('should handle increment by zero', () => {
    const doc = createDoc('1', { count: 5 });
    const result = applyUpdate(doc, { $inc: { count: 0 } });

    expect(result.count).toBe(5);
  });

  it('should handle floating point increments', () => {
    const doc = createDoc('1', { score: 10.5 });
    const result = applyUpdate(doc, { $inc: { score: 0.5 } });

    expect(result.score).toBe(11);
  });

  it('should handle negative numbers', () => {
    const doc = createDoc('1', { count: -5 });
    const result = applyUpdate(doc, { $inc: { count: -3 } });

    expect(result.count).toBe(-8);
  });

  it('should handle large increments', () => {
    const doc = createDoc('1', { count: 1000000 });
    const result = applyUpdate(doc, { $inc: { count: 1000000 } });

    expect(result.count).toBe(2000000);
  });

  describe('type validation', () => {
    it('should throw error when incrementing a string value', () => {
      const doc = { _id: '1', count: '5' } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $inc: { count: 1 } })).toThrow(
        "Cannot apply $inc to a value of non-numeric type. Field 'count' has type string"
      );
    });

    it('should throw error when incrementing a boolean value', () => {
      const doc = { _id: '1', count: true } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $inc: { count: 1 } })).toThrow(
        "Cannot apply $inc to a value of non-numeric type. Field 'count' has type boolean"
      );
    });

    it('should throw error when incrementing an object value', () => {
      const doc = { _id: '1', count: { value: 5 } } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $inc: { count: 1 } })).toThrow(
        "Cannot apply $inc to a value of non-numeric type. Field 'count' has type object"
      );
    });

    it('should throw error when incrementing an array value', () => {
      const doc = { _id: '1', count: [1, 2, 3] } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $inc: { count: 1 } })).toThrow(
        "Cannot apply $inc to a value of non-numeric type. Field 'count' has type object"
      );
    });

    it('should handle null field by initializing to increment amount', () => {
      const doc = { _id: '1', count: null } as unknown as TestDocument;
      const result = applyUpdate(doc, { $inc: { count: 5 } });

      expect(result.count).toBe(5);
    });

    it('should handle undefined field by initializing to increment amount', () => {
      const doc = { _id: '1', count: undefined } as unknown as TestDocument;
      const result = applyUpdate(doc, { $inc: { count: 5 } });

      expect(result.count).toBe(5);
    });

    it('should allow incrementing zero', () => {
      const doc = createDoc('1', { count: 0 });
      const result = applyUpdate(doc, { $inc: { count: 5 } });

      expect(result.count).toBe(5);
    });

    it('should handle numeric string coercion attempt correctly', () => {
      // Even if a string looks numeric, it should still throw
      const doc = { _id: '1', count: '123' } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $inc: { count: 1 } })).toThrow(
        "Cannot apply $inc to a value of non-numeric type. Field 'count' has type string"
      );
    });

    it('should throw error when amount is not a number', () => {
      const doc = createDoc('1', { count: 5 });

      // Cast to bypass TypeScript type checking - simulates runtime type error
      expect(() => applyUpdate(doc, { $inc: { count: '5' as unknown as number } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $inc: { count: '5' as unknown as number } })).toThrow(
        "Cannot apply $inc with non-numeric amount. Field 'count' received type string"
      );
    });

    it('should throw error when amount is an object', () => {
      const doc = createDoc('1', { count: 5 });

      // Cast to bypass TypeScript type checking - simulates runtime type error
      expect(() => applyUpdate(doc, { $inc: { count: { value: 5 } as unknown as number } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $inc: { count: { value: 5 } as unknown as number } })).toThrow(
        "Cannot apply $inc with non-numeric amount. Field 'count' received type object"
      );
    });
  });
});

// =============================================================================
// $push Operator
// =============================================================================

describe('applyUpdate - $push Operator', () => {
  it('should push a single element to existing array', () => {
    const doc = createDoc('1', { tags: ['a', 'b'] });
    const result = applyUpdate(doc, { $push: { tags: 'c' } });

    expect(result.tags).toEqual(['a', 'b', 'c']);
  });

  it('should create array if field does not exist', () => {
    const doc = createDoc('1', {});
    const result = applyUpdate(doc, { $push: { tags: 'a' } });

    expect(result.tags).toEqual(['a']);
  });

  it('should push to multiple arrays', () => {
    const doc = createDoc('1', { tags: ['a'], scores: [1] });
    const result = applyUpdate(doc, { $push: { tags: 'b', scores: 2 } });

    expect(result.tags).toEqual(['a', 'b']);
    expect(result.scores).toEqual([1, 2]);
  });

  it('should push with $each modifier', () => {
    const doc = createDoc('1', { tags: ['a'] });
    const result = applyUpdate(doc, { $push: { tags: { $each: ['b', 'c', 'd'] } } });

    expect(result.tags).toEqual(['a', 'b', 'c', 'd']);
  });

  it('should create array when using $each on non-existent field', () => {
    const doc = createDoc('1', {});
    const result = applyUpdate(doc, { $push: { tags: { $each: ['a', 'b'] } } });

    expect(result.tags).toEqual(['a', 'b']);
  });

  it('should push object to array', () => {
    const doc = createDoc('1', { items: [{ id: 1, name: 'Item 1' }] });
    const result = applyUpdate(doc, { $push: { items: { id: 2, name: 'Item 2' } } });

    expect(result.items).toEqual([
      { id: 1, name: 'Item 1' },
      { id: 2, name: 'Item 2' },
    ]);
  });

  it('should allow pushing duplicate values', () => {
    const doc = createDoc('1', { tags: ['a', 'b'] });
    const result = applyUpdate(doc, { $push: { tags: 'a' } });

    expect(result.tags).toEqual(['a', 'b', 'a']);
  });

  it('should handle empty $each array', () => {
    const doc = createDoc('1', { tags: ['a'] });
    const result = applyUpdate(doc, { $push: { tags: { $each: [] } } });

    expect(result.tags).toEqual(['a']);
  });
});

// =============================================================================
// $addToSet Operator
// =============================================================================

describe('applyUpdate - $addToSet Operator', () => {
  it('should add element if not present', () => {
    const doc = createDoc('1', { tags: ['a', 'b'] });
    const result = applyUpdate(doc, { $addToSet: { tags: 'c' } });

    expect(result.tags).toEqual(['a', 'b', 'c']);
  });

  it('should not add element if already present', () => {
    const doc = createDoc('1', { tags: ['a', 'b', 'c'] });
    const result = applyUpdate(doc, { $addToSet: { tags: 'b' } });

    expect(result.tags).toEqual(['a', 'b', 'c']);
  });

  it('should create array if field does not exist', () => {
    const doc = createDoc('1', {});
    const result = applyUpdate(doc, { $addToSet: { tags: 'a' } });

    expect(result.tags).toEqual(['a']);
  });

  it('should use $each to add multiple unique elements', () => {
    const doc = createDoc('1', { tags: ['a'] });
    const result = applyUpdate(doc, { $addToSet: { tags: { $each: ['b', 'c', 'a'] } } });

    expect(result.tags).toEqual(['a', 'b', 'c']);
  });

  it('should handle $each with all existing elements', () => {
    const doc = createDoc('1', { tags: ['a', 'b', 'c'] });
    const result = applyUpdate(doc, { $addToSet: { tags: { $each: ['a', 'b'] } } });

    expect(result.tags).toEqual(['a', 'b', 'c']);
  });

  it('should handle empty $each array', () => {
    const doc = createDoc('1', { tags: ['a'] });
    const result = applyUpdate(doc, { $addToSet: { tags: { $each: [] } } });

    expect(result.tags).toEqual(['a']);
  });

  it('should add to multiple arrays', () => {
    const doc = createDoc('1', { tags: ['a'], scores: [1] });
    const result = applyUpdate(doc, { $addToSet: { tags: 'b', scores: 2 } });

    expect(result.tags).toEqual(['a', 'b']);
    expect(result.scores).toEqual([1, 2]);
  });

  it('should compare values by identity (not deep equality for objects)', () => {
    const doc = createDoc('1', { items: [{ id: 1 }] });
    const result = applyUpdate(doc, { $addToSet: { items: { id: 1 } } });

    // Objects are compared by reference, so this will add a duplicate
    expect(result.items).toHaveLength(2);
  });
});

// =============================================================================
// $pull Operator
// =============================================================================

describe('applyUpdate - $pull Operator', () => {
  it('should remove matching element from array', () => {
    const doc = createDoc('1', { tags: ['a', 'b', 'c'] });
    const result = applyUpdate(doc, { $pull: { tags: 'b' } });

    expect(result.tags).toEqual(['a', 'c']);
  });

  it('should remove all matching elements', () => {
    const doc = createDoc('1', { tags: ['a', 'b', 'a', 'c', 'a'] });
    const result = applyUpdate(doc, { $pull: { tags: 'a' } });

    expect(result.tags).toEqual(['b', 'c']);
  });

  it('should handle non-matching element', () => {
    const doc = createDoc('1', { tags: ['a', 'b', 'c'] });
    const result = applyUpdate(doc, { $pull: { tags: 'd' } });

    expect(result.tags).toEqual(['a', 'b', 'c']);
  });

  it('should handle empty array', () => {
    const doc = createDoc('1', { tags: [] });
    const result = applyUpdate(doc, { $pull: { tags: 'a' } });

    expect(result.tags).toEqual([]);
  });

  it('should do nothing if field does not exist (MongoDB behavior)', () => {
    const doc = createDoc('1', {});
    const result = applyUpdate(doc, { $pull: { tags: 'a' } });

    // MongoDB $pull does nothing if the field doesn't exist
    expect(result.tags).toBeUndefined();
  });

  it('should pull from multiple arrays', () => {
    const doc = createDoc('1', { tags: ['a', 'b'], scores: [1, 2, 3] });
    const result = applyUpdate(doc, { $pull: { tags: 'a', scores: 2 } });

    expect(result.tags).toEqual(['b']);
    expect(result.scores).toEqual([1, 3]);
  });

  it('should remove numeric values', () => {
    const doc = createDoc('1', { scores: [1, 2, 3, 2, 4] });
    const result = applyUpdate(doc, { $pull: { scores: 2 } });

    expect(result.scores).toEqual([1, 3, 4]);
  });
});

// =============================================================================
// $pop Operator
// =============================================================================

describe('applyUpdate - $pop Operator', () => {
  it('should remove last element with 1', () => {
    const doc = createDoc('1', { tags: ['a', 'b', 'c'] });
    const result = applyUpdate(doc, { $pop: { tags: 1 } });

    expect(result.tags).toEqual(['a', 'b']);
  });

  it('should remove first element with -1', () => {
    const doc = createDoc('1', { tags: ['a', 'b', 'c'] });
    const result = applyUpdate(doc, { $pop: { tags: -1 } });

    expect(result.tags).toEqual(['b', 'c']);
  });

  it('should handle single element array (pop last)', () => {
    const doc = createDoc('1', { tags: ['a'] });
    const result = applyUpdate(doc, { $pop: { tags: 1 } });

    expect(result.tags).toEqual([]);
  });

  it('should handle single element array (pop first)', () => {
    const doc = createDoc('1', { tags: ['a'] });
    const result = applyUpdate(doc, { $pop: { tags: -1 } });

    expect(result.tags).toEqual([]);
  });

  it('should handle empty array', () => {
    const doc = createDoc('1', { tags: [] });
    const result = applyUpdate(doc, { $pop: { tags: 1 } });

    expect(result.tags).toEqual([]);
  });

  it('should not affect non-existent field', () => {
    const doc = createDoc('1', {});
    const result = applyUpdate(doc, { $pop: { tags: 1 } });

    // $pop on non-existent field should not create the field
    expect(result.tags).toBeUndefined();
  });

  it('should pop from multiple arrays', () => {
    const doc = createDoc('1', { tags: ['a', 'b'], scores: [1, 2, 3] });
    const result = applyUpdate(doc, { $pop: { tags: 1, scores: -1 } });

    expect(result.tags).toEqual(['a']);
    expect(result.scores).toEqual([2, 3]);
  });
});

// =============================================================================
// $rename Operator
// =============================================================================

describe('applyUpdate - $rename Operator', () => {
  it('should rename a field', () => {
    const doc = createDoc('1', { oldField: 'value' });
    const result = applyUpdate(doc, { $rename: { oldField: 'newField' } });

    expect(result.newField).toBe('value');
    expect('oldField' in result).toBe(false);
  });

  it('should rename multiple fields', () => {
    const doc = createDoc('1', { oldField: 'value1', name: 'value2' });
    const result = applyUpdate(doc, { $rename: { oldField: 'newField', name: 'fullName' } });

    expect(result.newField).toBe('value1');
    expect((result as Record<string, unknown>).fullName).toBe('value2');
    expect('oldField' in result).toBe(false);
    expect('name' in result).toBe(false);
  });

  it('should handle renaming non-existent field', () => {
    const doc = createDoc('1', { name: 'Alice' });
    const result = applyUpdate(doc, { $rename: { oldField: 'newField' } });

    expect(result.name).toBe('Alice');
    expect('oldField' in result).toBe(false);
    expect('newField' in result).toBe(false);
  });

  it('should overwrite existing field with same new name', () => {
    const doc = createDoc('1', { oldField: 'old value', newField: 'existing' });
    const result = applyUpdate(doc, { $rename: { oldField: 'newField' } });

    expect(result.newField).toBe('old value');
    expect('oldField' in result).toBe(false);
  });

  it('should preserve other fields', () => {
    const doc = createDoc('1', { oldField: 'value', name: 'Alice', age: 30 });
    const result = applyUpdate(doc, { $rename: { oldField: 'newField' } });

    expect(result.newField).toBe('value');
    expect(result.name).toBe('Alice');
    expect(result.age).toBe(30);
  });
});

// =============================================================================
// Nested Field Updates
// =============================================================================

describe('applyUpdate - Nested Field Updates', () => {
  describe('$set with nested fields', () => {
    it('should set a nested field directly using object', () => {
      const doc = createDoc('1', { profile: { firstName: 'Alice' } });
      const result = applyUpdate(doc, {
        $set: { profile: { firstName: 'Bob', lastName: 'Smith' } },
      });

      expect(result.profile).toEqual({ firstName: 'Bob', lastName: 'Smith' });
    });

    it('should replace entire nested object', () => {
      const doc = createDoc('1', {
        profile: { firstName: 'Alice', settings: { theme: 'dark' } },
      });
      const result = applyUpdate(doc, { $set: { profile: { firstName: 'Bob' } } });

      expect(result.profile).toEqual({ firstName: 'Bob' });
      expect(result.profile?.settings).toBeUndefined();
    });
  });

  describe('$inc with nested fields (top-level only)', () => {
    it('should increment a top-level numeric field', () => {
      const doc = createDoc('1', { count: 5 });
      const result = applyUpdate(doc, { $inc: { count: 3 } });

      expect(result.count).toBe(8);
    });
  });
});

// =============================================================================
// Combined Operators
// =============================================================================

describe('applyUpdate - Combined Operators', () => {
  it('should apply $set and $unset together', () => {
    const doc = createDoc('1', { name: 'Alice', age: 30, status: 'active' });
    const result = applyUpdate(doc, {
      $set: { name: 'Bob' },
      $unset: { status: '' },
    });

    expect(result.name).toBe('Bob');
    expect(result.age).toBe(30);
    expect('status' in result).toBe(false);
  });

  it('should apply $set and $inc together', () => {
    const doc = createDoc('1', { name: 'Alice', count: 5 });
    const result = applyUpdate(doc, {
      $set: { name: 'Bob' },
      $inc: { count: 3 },
    });

    expect(result.name).toBe('Bob');
    expect(result.count).toBe(8);
  });

  it('should apply $push and $addToSet together', () => {
    const doc = createDoc('1', { tags: ['a'], scores: [1] });
    const result = applyUpdate(doc, {
      $push: { tags: 'b' },
      $addToSet: { scores: 2 },
    });

    expect(result.tags).toEqual(['a', 'b']);
    expect(result.scores).toEqual([1, 2]);
  });

  it('should apply all operators in complex update', () => {
    const doc = createDoc('1', {
      name: 'Alice',
      age: 30,
      count: 5,
      tags: ['a'],
      oldField: 'value',
    });

    const result = applyUpdate(doc, {
      $set: { name: 'Bob', status: 'active' },
      $unset: { age: '' },
      $inc: { count: 10 },
      $push: { tags: 'b' },
      $rename: { oldField: 'newField' },
    });

    expect(result.name).toBe('Bob');
    expect(result.status).toBe('active');
    expect('age' in result).toBe(false);
    expect(result.count).toBe(15);
    expect(result.tags).toEqual(['a', 'b']);
    expect(result.newField).toBe('value');
    expect('oldField' in result).toBe(false);
  });
});

// =============================================================================
// Edge Cases
// =============================================================================

describe('applyUpdate - Edge Cases', () => {
  describe('immutability', () => {
    it('should not mutate the original document top-level properties with $set', () => {
      const doc = createDoc('1', { name: 'Alice' });
      const originalName = doc.name;

      applyUpdate(doc, { $set: { name: 'Bob' } });

      expect(doc.name).toBe(originalName);
    });

    it('should return a new object', () => {
      const doc = createDoc('1', { name: 'Alice' });
      const result = applyUpdate(doc, { $set: { name: 'Bob' } });

      expect(result).not.toBe(doc);
    });

    it('should create shallow copy (note: nested arrays may be mutated)', () => {
      // Note: The current implementation creates a shallow copy of the document
      // Array operations like $push may mutate the original array reference
      // This is a known limitation - for deep immutability, consider using a library
      const doc = createDoc('1', { name: 'Alice', tags: ['a', 'b'] });
      const result = applyUpdate(doc, { $push: { tags: 'c' } });

      expect(result).not.toBe(doc);
      expect(result.tags).toEqual(['a', 'b', 'c']);
    });
  });

  describe('empty update', () => {
    it('should return copy of document with empty update object', () => {
      const doc = createDoc('1', { name: 'Alice', age: 30 });
      const result = applyUpdate(doc, {});

      expect(result).toEqual(doc);
      expect(result).not.toBe(doc);
    });
  });

  describe('special values', () => {
    it('should handle setting undefined value', () => {
      const doc = createDoc('1', { name: 'Alice' });
      const result = applyUpdate(doc, { $set: { name: undefined } });

      expect(result.name).toBeUndefined();
    });

    it('should handle $inc with floating point precision', () => {
      const doc = createDoc('1', { score: 0.1 });
      const result = applyUpdate(doc, { $inc: { score: 0.2 } });

      expect(result.score).toBeCloseTo(0.3, 10);
    });
  });

  describe('operator order', () => {
    it('should apply operators in correct order', () => {
      // $set happens before $unset, so setting then unsetting should remove the field
      const doc = createDoc('1', { name: 'Alice' });
      const result = applyUpdate(doc, {
        $set: { age: 30 },
        $unset: { age: '' },
      });

      // $unset runs after $set
      expect('age' in result).toBe(false);
    });
  });

  describe('array edge cases', () => {
    it('should handle $push to non-array field (creates new array)', () => {
      const doc = createDoc('1', {});
      const result = applyUpdate(doc, { $push: { tags: 'a' } });

      expect(result.tags).toEqual(['a']);
    });

    it('should handle $pull from non-existent field (does nothing - MongoDB behavior)', () => {
      const doc = createDoc('1', {});
      const result = applyUpdate(doc, { $pull: { tags: 'a' } });

      // MongoDB $pull does nothing if the field doesn't exist
      expect(result.tags).toBeUndefined();
    });

    it('should handle $addToSet on non-array field (creates new array)', () => {
      const doc = createDoc('1', {});
      const result = applyUpdate(doc, { $addToSet: { tags: 'a' } });

      expect(result.tags).toEqual(['a']);
    });
  });
});

// =============================================================================
// Type Safety
// =============================================================================

describe('applyUpdate - Type Safety', () => {
  it('should preserve document type', () => {
    const doc: TestDocument = createDoc('1', { name: 'Alice', age: 30 });
    const result = applyUpdate(doc, { $set: { name: 'Bob' } });

    // TypeScript should recognize result as TestDocument
    expect(result._id).toBe('1');
    expect(result.name).toBe('Bob');
    expect(result.age).toBe(30);
  });

  it('should work with generic record type', () => {
    const doc: Record<string, unknown> = { _id: '1', custom: 'value' };
    const result = applyUpdate(doc, { $set: { custom: 'updated' } });

    expect(result._id).toBe('1');
    expect(result.custom).toBe('updated');
  });

  describe('$push type validation', () => {
    it('should throw error when pushing to a non-array field', () => {
      const doc = createDoc('1', { tags: 'not-an-array' });

      expect(() => applyUpdate(doc, { $push: { tags: 'new' } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $push: { tags: 'new' } })).toThrow(
        /Cannot apply \$push to field 'tags': expected array, got string/
      );
    });

    it('should throw error when pushing to a number field', () => {
      const doc = createDoc('1', { count: 42 });

      expect(() => applyUpdate(doc, { $push: { count: 1 } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $push: { count: 1 } })).toThrow(
        /Cannot apply \$push to field 'count': expected array, got number/
      );
    });

    it('should throw error when $each is not an array', () => {
      const doc = createDoc('1', { tags: ['a', 'b'] });

      expect(() => applyUpdate(doc, { $push: { tags: { $each: 'not-array' } } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $push: { tags: { $each: 'not-array' } } })).toThrow(
        /\$push \$each requires an array/
      );
    });
  });

  describe('$pull type validation', () => {
    it('should throw error when pulling from a non-array field', () => {
      const doc = createDoc('1', { tags: 'not-an-array' });

      expect(() => applyUpdate(doc, { $pull: { tags: 'a' } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $pull: { tags: 'a' } })).toThrow(
        /Cannot apply \$pull to field 'tags': expected array, got string/
      );
    });

    it('should throw error when pulling from a number field', () => {
      const doc = createDoc('1', { count: 42 });

      expect(() => applyUpdate(doc, { $pull: { count: 1 } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $pull: { count: 1 } })).toThrow(
        /Cannot apply \$pull to field 'count': expected array, got number/
      );
    });
  });

  describe('$addToSet type validation', () => {
    it('should throw error when adding to a non-array field', () => {
      const doc = createDoc('1', { tags: 'not-an-array' });

      expect(() => applyUpdate(doc, { $addToSet: { tags: 'new' } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $addToSet: { tags: 'new' } })).toThrow(
        /Cannot apply \$addToSet to field 'tags': expected array, got string/
      );
    });

    it('should throw error when adding to a number field', () => {
      const doc = createDoc('1', { count: 42 });

      expect(() => applyUpdate(doc, { $addToSet: { count: 1 } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $addToSet: { count: 1 } })).toThrow(
        /Cannot apply \$addToSet to field 'count': expected array, got number/
      );
    });

    it('should throw error when $each is not an array', () => {
      const doc = createDoc('1', { tags: ['a', 'b'] });

      expect(() => applyUpdate(doc, { $addToSet: { tags: { $each: 'not-array' } } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $addToSet: { tags: { $each: 'not-array' } } })).toThrow(
        /\$addToSet \$each requires an array/
      );
    });
  });
});

// =============================================================================
// extractFilterFields - For Upsert Operations
// =============================================================================

describe('extractFilterFields', () => {
  describe('equality matches', () => {
    it('should extract direct equality values', () => {
      const filter = { name: 'Alice', status: 'active' };
      const result = extractFilterFields(filter);

      expect(result).toEqual({ name: 'Alice', status: 'active' });
    });

    it('should extract $eq operator values', () => {
      const filter = { name: { $eq: 'Alice' }, status: { $eq: 'active' } };
      const result = extractFilterFields(filter);

      expect(result).toEqual({ name: 'Alice', status: 'active' });
    });

    it('should extract mixed direct and $eq values', () => {
      const filter = { name: 'Alice', status: { $eq: 'active' } };
      const result = extractFilterFields(filter);

      expect(result).toEqual({ name: 'Alice', status: 'active' });
    });

    it('should extract null values', () => {
      const filter = { name: 'Alice', deleted: null };
      const result = extractFilterFields(filter);

      expect(result).toEqual({ name: 'Alice', deleted: null });
    });

    it('should extract numeric values', () => {
      const filter = { count: 0, score: 100 };
      const result = extractFilterFields(filter);

      expect(result).toEqual({ count: 0, score: 100 });
    });

    it('should extract boolean values', () => {
      const filter = { active: true, deleted: false };
      const result = extractFilterFields(filter);

      expect(result).toEqual({ active: true, deleted: false });
    });

    it('should extract array equality values', () => {
      const filter = { tags: ['a', 'b', 'c'] };
      const result = extractFilterFields(filter);

      expect(result).toEqual({ tags: ['a', 'b', 'c'] });
    });

    it('should extract embedded document equality values', () => {
      const filter = { profile: { firstName: 'Alice', lastName: 'Smith' } };
      const result = extractFilterFields(filter);

      expect(result).toEqual({ profile: { firstName: 'Alice', lastName: 'Smith' } });
    });
  });

  describe('exclusions - comparison operators', () => {
    it('should not extract $gt operator fields', () => {
      const filter = { age: { $gt: 18 }, name: 'Alice' };
      const result = extractFilterFields(filter);

      expect(result).toEqual({ name: 'Alice' });
      expect(result.age).toBeUndefined();
    });

    it('should not extract $gte operator fields', () => {
      const filter = { age: { $gte: 18 } };
      const result = extractFilterFields(filter);

      expect(result).toEqual({});
    });

    it('should not extract $lt operator fields', () => {
      const filter = { age: { $lt: 65 } };
      const result = extractFilterFields(filter);

      expect(result).toEqual({});
    });

    it('should not extract $lte operator fields', () => {
      const filter = { age: { $lte: 65 } };
      const result = extractFilterFields(filter);

      expect(result).toEqual({});
    });

    it('should not extract $ne operator fields', () => {
      const filter = { status: { $ne: 'deleted' } };
      const result = extractFilterFields(filter);

      expect(result).toEqual({});
    });

    it('should not extract $in operator fields', () => {
      const filter = { status: { $in: ['active', 'pending'] } };
      const result = extractFilterFields(filter);

      expect(result).toEqual({});
    });

    it('should not extract $nin operator fields', () => {
      const filter = { status: { $nin: ['deleted', 'archived'] } };
      const result = extractFilterFields(filter);

      expect(result).toEqual({});
    });
  });

  describe('exclusions - logical operators', () => {
    it('should skip $and logical operator', () => {
      const filter = { $and: [{ status: 'active' }, { age: { $gt: 18 } }], name: 'Alice' };
      const result = extractFilterFields(filter);

      expect(result).toEqual({ name: 'Alice' });
      expect(result.$and).toBeUndefined();
    });

    it('should skip $or logical operator', () => {
      const filter = { $or: [{ status: 'active' }, { status: 'pending' }], name: 'Alice' };
      const result = extractFilterFields(filter);

      expect(result).toEqual({ name: 'Alice' });
      expect(result.$or).toBeUndefined();
    });

    it('should skip $nor logical operator', () => {
      const filter = { $nor: [{ deleted: true }], name: 'Alice' };
      const result = extractFilterFields(filter);

      expect(result).toEqual({ name: 'Alice' });
    });

    it('should skip $not logical operator', () => {
      const filter = { $not: { status: 'deleted' }, name: 'Alice' };
      const result = extractFilterFields(filter);

      expect(result).toEqual({ name: 'Alice' });
    });
  });

  describe('exclusions - dot notation', () => {
    it('should skip dot notation nested fields', () => {
      const filter = { 'profile.firstName': 'Alice', status: 'active' };
      const result = extractFilterFields(filter);

      expect(result).toEqual({ status: 'active' });
      expect(result['profile.firstName']).toBeUndefined();
    });

    it('should skip deeply nested dot notation fields', () => {
      const filter = { 'profile.settings.theme': 'dark', name: 'Alice' };
      const result = extractFilterFields(filter);

      expect(result).toEqual({ name: 'Alice' });
    });
  });

  describe('complex filters', () => {
    it('should handle filter with mixed conditions', () => {
      const filter = {
        name: 'Alice',
        age: { $gte: 18 },
        status: { $eq: 'active' },
        'profile.verified': true,
        $or: [{ role: 'admin' }, { role: 'moderator' }],
      };
      const result = extractFilterFields(filter);

      expect(result).toEqual({
        name: 'Alice',
        status: 'active',
      });
    });

    it('should return empty object for filter with only operators', () => {
      const filter = {
        age: { $gt: 18, $lt: 65 },
        $and: [{ status: 'active' }],
      };
      const result = extractFilterFields(filter);

      expect(result).toEqual({});
    });

    it('should handle empty filter', () => {
      const filter = {};
      const result = extractFilterFields(filter);

      expect(result).toEqual({});
    });
  });

  describe('special cases', () => {
    it('should handle _id field', () => {
      const filter = { _id: 'abc123', name: 'Alice' };
      const result = extractFilterFields(filter);

      expect(result).toEqual({ _id: 'abc123', name: 'Alice' });
    });

    it('should handle filter with $exists operator', () => {
      const filter = { email: { $exists: true }, name: 'Alice' };
      const result = extractFilterFields(filter);

      expect(result).toEqual({ name: 'Alice' });
    });

    it('should handle filter with $type operator', () => {
      const filter = { count: { $type: 'number' }, name: 'Alice' };
      const result = extractFilterFields(filter);

      expect(result).toEqual({ name: 'Alice' });
    });

    it('should handle filter with $regex operator', () => {
      const filter = { name: { $regex: '^Alice' }, status: 'active' };
      const result = extractFilterFields(filter);

      expect(result).toEqual({ status: 'active' });
    });
  });
});

// =============================================================================
// Prototype Pollution Protection
// =============================================================================

describe('applyUpdate - Prototype Pollution Protection', () => {
  // NOTE: JavaScript's special handling of __proto__ means Object.entries() won't
  // enumerate it even when set via bracket notation. The real attack vector is
  // via dot-notation nested paths like 'a.__proto__.polluted', which our validation
  // catches. Direct __proto__ keys are inherently safe because they're not enumerable.

  describe('$set operator - nested path protection', () => {
    it('should reject __proto__ in nested path', () => {
      const doc = createDoc('1', {});
      expect(() => applyUpdate(doc, { $set: { 'a.__proto__.polluted': true } })).toThrow(
        "Invalid property name: '__proto__' is not allowed"
      );
    });

    it('should reject __proto__ as first segment in path', () => {
      const doc = createDoc('1', {});
      expect(() => applyUpdate(doc, { $set: { '__proto__.polluted': true } })).toThrow(
        "Invalid property name: '__proto__' is not allowed"
      );
    });

    it('should reject constructor as direct key', () => {
      const doc = createDoc('1', {});
      expect(() => applyUpdate(doc, { $set: { constructor: { polluted: true } } })).toThrow(
        "Invalid property name: 'constructor' is not allowed"
      );
    });

    it('should reject constructor in nested path', () => {
      const doc = createDoc('1', {});
      expect(() => applyUpdate(doc, { $set: { 'a.constructor.prototype': {} } })).toThrow(
        "Invalid property name: 'constructor' is not allowed"
      );
    });

    it('should reject prototype as direct key', () => {
      const doc = createDoc('1', {});
      expect(() => applyUpdate(doc, { $set: { prototype: { polluted: true } } })).toThrow(
        "Invalid property name: 'prototype' is not allowed"
      );
    });

    it('should reject prototype in nested path', () => {
      const doc = createDoc('1', {});
      expect(() => applyUpdate(doc, { $set: { 'a.prototype.polluted': true } })).toThrow(
        "Invalid property name: 'prototype' is not allowed"
      );
    });

    it('should reject any key starting with double underscore in nested path', () => {
      const doc = createDoc('1', {});
      expect(() => applyUpdate(doc, { $set: { 'a.__internal__.b': 'value' } })).toThrow(
        "Invalid property name: keys starting with '__' are not allowed"
      );
    });

    it('should reject __defineGetter__ in nested path', () => {
      const doc = createDoc('1', {});
      expect(() => applyUpdate(doc, { $set: { 'a.__defineGetter__.b': 'value' } })).toThrow(
        "Invalid property name: keys starting with '__' are not allowed"
      );
    });

    it('should allow keys with single underscore prefix', () => {
      const doc = createDoc('1', {});
      const result = applyUpdate(doc, { $set: { _privateField: 'value' } });
      expect((result as Record<string, unknown>)._privateField).toBe('value');
    });

    it('should allow keys with underscore in the middle', () => {
      const doc = createDoc('1', {});
      const result = applyUpdate(doc, { $set: { my_field: 'value' } });
      expect((result as Record<string, unknown>).my_field).toBe('value');
    });

    it('should allow nested paths with underscores', () => {
      const doc = createDoc('1', {});
      const result = applyUpdate(doc, { $set: { 'a._b.c_d': 'value' } });
      expect((result as Record<string, unknown>).a).toEqual({ _b: { c_d: 'value' } });
    });
  });

  describe('$unset operator - nested path protection', () => {
    it('should reject __proto__ in nested $unset path', () => {
      const doc = createDoc('1', {});
      expect(() => applyUpdate(doc, { $unset: { 'a.__proto__': '' } })).toThrow(
        "Invalid property name: '__proto__' is not allowed"
      );
    });

    it('should reject constructor in nested $unset path', () => {
      const doc = createDoc('1', {});
      expect(() => applyUpdate(doc, { $unset: { 'a.constructor': '' } })).toThrow(
        "Invalid property name: 'constructor' is not allowed"
      );
    });

    it('should reject prototype in nested $unset path', () => {
      const doc = createDoc('1', {});
      expect(() => applyUpdate(doc, { $unset: { 'obj.prototype': '' } })).toThrow(
        "Invalid property name: 'prototype' is not allowed"
      );
    });
  });

  describe('$inc operator - nested path protection', () => {
    it('should reject __proto__ in nested $inc path', () => {
      const doc = createDoc('1', {});
      expect(() => applyUpdate(doc, { $inc: { 'a.__proto__.count': 1 } })).toThrow(
        "Invalid property name: '__proto__' is not allowed"
      );
    });

    it('should reject constructor in nested $inc path', () => {
      const doc = createDoc('1', {});
      expect(() => applyUpdate(doc, { $inc: { 'a.constructor.count': 1 } })).toThrow(
        "Invalid property name: 'constructor' is not allowed"
      );
    });
  });

  describe('prototype pollution attack scenarios', () => {
    it('should not pollute Object.prototype via __proto__ in path', () => {
      const doc = createDoc('1', {});

      // Attempt prototype pollution via nested path
      expect(() =>
        applyUpdate(doc, { $set: { '__proto__.polluted': true } })
      ).toThrow("Invalid property name: '__proto__' is not allowed");

      // Verify prototype was not polluted
      expect(({} as Record<string, unknown>).polluted).toBeUndefined();
    });

    it('should not pollute via constructor.prototype path', () => {
      const doc = createDoc('1', {});

      // Attempt prototype pollution via constructor
      expect(() =>
        applyUpdate(doc, { $set: { 'constructor.prototype.polluted': true } })
      ).toThrow("Invalid property name: 'constructor' is not allowed");

      // Verify prototype was not polluted
      expect(({} as Record<string, unknown>).polluted).toBeUndefined();
    });

    it('should not pollute via deeply nested __proto__ path', () => {
      const doc = createDoc('1', {});

      // Attempt prototype pollution via deeply nested path
      expect(() =>
        applyUpdate(doc, { $set: { 'a.b.c.__proto__.polluted': true } })
      ).toThrow("Invalid property name: '__proto__' is not allowed");

      // Verify prototype was not polluted
      expect(({} as Record<string, unknown>).polluted).toBeUndefined();
    });

    it('should not pollute via __proto__ as intermediate path segment', () => {
      const doc = createDoc('1', {});

      // This is the main attack vector - using __proto__ in the middle of a path
      expect(() =>
        applyUpdate(doc, { $set: { 'x.__proto__.isAdmin': true } })
      ).toThrow("Invalid property name: '__proto__' is not allowed");

      // Verify prototype was not polluted
      expect(({} as Record<string, unknown>).isAdmin).toBeUndefined();
    });

    it('should not pollute via constructor at any path depth', () => {
      const doc = createDoc('1', {});

      // Attempt via constructor at different depths
      expect(() =>
        applyUpdate(doc, { $set: { 'constructor.polluted': true } })
      ).toThrow("Invalid property name: 'constructor' is not allowed");

      expect(() =>
        applyUpdate(doc, { $set: { 'a.b.constructor.prototype.polluted': true } })
      ).toThrow("Invalid property name: 'constructor' is not allowed");

      // Verify prototype was not polluted
      expect(({} as Record<string, unknown>).polluted).toBeUndefined();
    });
  });
});

// =============================================================================
// Path Depth Validation
// =============================================================================

describe('applyUpdate - Path Depth Validation', () => {
  const MAX_DEPTH = 32; // MAX_NESTED_PATH_DEPTH

  describe('$set operator depth limit', () => {
    it('should allow paths up to the maximum depth', () => {
      const parts = Array.from({ length: MAX_DEPTH }, (_, i) => `key${i}`);
      const path = parts.join('.');

      const doc = createDoc('1', {});
      // Should not throw for exactly MAX_DEPTH levels
      expect(() => applyUpdate(doc, { $set: { [path]: 'value' } })).not.toThrow();
    });

    it('should throw error for paths exceeding maximum depth', () => {
      const parts = Array.from({ length: MAX_DEPTH + 1 }, (_, i) => `key${i}`);
      const path = parts.join('.');

      const doc = createDoc('1', {});
      expect(() => applyUpdate(doc, { $set: { [path]: 'value' } })).toThrow(
        `Path exceeds maximum nesting depth of ${MAX_DEPTH} levels`
      );
    });

    it('should throw error for extremely deep paths', () => {
      const parts = Array.from({ length: 1000 }, (_, i) => `k${i}`);
      const path = parts.join('.');

      const doc = createDoc('1', {});
      expect(() => applyUpdate(doc, { $set: { [path]: 'value' } })).toThrow(
        `Path exceeds maximum nesting depth of ${MAX_DEPTH} levels`
      );
    });
  });

  describe('$unset operator depth limit', () => {
    it('should allow paths up to the maximum depth', () => {
      const parts = Array.from({ length: MAX_DEPTH }, (_, i) => `key${i}`);
      const path = parts.join('.');

      const doc = createDoc('1', {});
      // Should not throw for exactly MAX_DEPTH levels
      expect(() => applyUpdate(doc, { $unset: { [path]: '' } })).not.toThrow();
    });

    it('should throw error for paths exceeding maximum depth', () => {
      const parts = Array.from({ length: MAX_DEPTH + 1 }, (_, i) => `key${i}`);
      const path = parts.join('.');

      const doc = createDoc('1', {});
      expect(() => applyUpdate(doc, { $unset: { [path]: '' } })).toThrow(
        `Path exceeds maximum nesting depth of ${MAX_DEPTH} levels`
      );
    });
  });

  describe('$inc operator depth limit', () => {
    it('should allow paths up to the maximum depth', () => {
      const parts = Array.from({ length: MAX_DEPTH }, (_, i) => `key${i}`);
      const path = parts.join('.');

      const doc = createDoc('1', {});
      // Should not throw for exactly MAX_DEPTH levels
      expect(() => applyUpdate(doc, { $inc: { [path]: 1 } })).not.toThrow();
    });

    it('should throw error for paths exceeding maximum depth', () => {
      const parts = Array.from({ length: MAX_DEPTH + 1 }, (_, i) => `key${i}`);
      const path = parts.join('.');

      const doc = createDoc('1', {});
      expect(() => applyUpdate(doc, { $inc: { [path]: 1 } })).toThrow(
        `Path exceeds maximum nesting depth of ${MAX_DEPTH} levels`
      );
    });
  });

  describe('DoS prevention scenarios', () => {
    it('should prevent DoS via deeply nested $set paths', () => {
      const parts = Array.from({ length: 10000 }, () => 'x');
      const path = parts.join('.');

      const doc = createDoc('1', {});
      expect(() => applyUpdate(doc, { $set: { [path]: 'value' } })).toThrow(
        `Path exceeds maximum nesting depth of ${MAX_DEPTH} levels`
      );
    });

    it('should prevent DoS via deeply nested $unset paths', () => {
      const parts = Array.from({ length: 10000 }, () => 'x');
      const path = parts.join('.');

      const doc = createDoc('1', {});
      expect(() => applyUpdate(doc, { $unset: { [path]: '' } })).toThrow(
        `Path exceeds maximum nesting depth of ${MAX_DEPTH} levels`
      );
    });

    it('should prevent DoS via deeply nested $inc paths', () => {
      const parts = Array.from({ length: 10000 }, () => 'x');
      const path = parts.join('.');

      const doc = createDoc('1', {});
      expect(() => applyUpdate(doc, { $inc: { [path]: 1 } })).toThrow(
        `Path exceeds maximum nesting depth of ${MAX_DEPTH} levels`
      );
    });
  });
});

// =============================================================================
// Reserved Field Validation
// =============================================================================

describe('applyUpdate - Reserved Field Validation', () => {
  describe('$set operator', () => {
    it('should reject modification of _id field', () => {
      const doc = createDoc('1', { name: 'Alice' });

      expect(() => applyUpdate(doc, { $set: { _id: 'new-id' } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $set: { _id: 'new-id' } })).toThrow(
        "Cannot modify reserved field '_id'"
      );
    });

    it('should reject modification of _seq field', () => {
      const doc = createDoc('1', { name: 'Alice' });

      expect(() => applyUpdate(doc, { $set: { _seq: 123 } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $set: { _seq: 123 } })).toThrow(
        "Cannot modify reserved field '_seq'"
      );
    });

    it('should reject modification of _op field', () => {
      const doc = createDoc('1', { name: 'Alice' });

      expect(() => applyUpdate(doc, { $set: { _op: 'insert' } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $set: { _op: 'insert' } })).toThrow(
        "Cannot modify reserved field '_op'"
      );
    });

    it('should reject modification of nested reserved field paths', () => {
      const doc = createDoc('1', { name: 'Alice' });

      expect(() => applyUpdate(doc, { $set: { '_id.nested': 'value' } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $set: { '_id.nested': 'value' } })).toThrow(
        "Cannot modify reserved field '_id'"
      );
    });

    it('should allow modification of fields starting with underscore but not reserved', () => {
      const doc = createDoc('1', {});
      const result = applyUpdate(doc, { $set: { _customField: 'value' } });

      expect((result as Record<string, unknown>)._customField).toBe('value');
    });
  });

  describe('$unset operator', () => {
    it('should reject unsetting _id field', () => {
      const doc = createDoc('1', { name: 'Alice' });

      expect(() => applyUpdate(doc, { $unset: { _id: '' } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $unset: { _id: '' } })).toThrow(
        "Cannot modify reserved field '_id'"
      );
    });

    it('should reject unsetting _seq field', () => {
      const doc = createDoc('1', { name: 'Alice' });

      expect(() => applyUpdate(doc, { $unset: { _seq: '' } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $unset: { _seq: '' } })).toThrow(
        "Cannot modify reserved field '_seq'"
      );
    });

    it('should reject unsetting _op field', () => {
      const doc = createDoc('1', { name: 'Alice' });

      expect(() => applyUpdate(doc, { $unset: { _op: '' } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $unset: { _op: '' } })).toThrow(
        "Cannot modify reserved field '_op'"
      );
    });
  });

  describe('$inc operator', () => {
    it('should reject incrementing _id field', () => {
      const doc = createDoc('1', { count: 5 });

      expect(() => applyUpdate(doc, { $inc: { _id: 1 } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $inc: { _id: 1 } })).toThrow(
        "Cannot modify reserved field '_id'"
      );
    });

    it('should reject incrementing _seq field', () => {
      const doc = createDoc('1', { count: 5 });

      expect(() => applyUpdate(doc, { $inc: { _seq: 1 } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $inc: { _seq: 1 } })).toThrow(
        "Cannot modify reserved field '_seq'"
      );
    });

    it('should reject incrementing _op field', () => {
      const doc = createDoc('1', { count: 5 });

      expect(() => applyUpdate(doc, { $inc: { _op: 1 } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $inc: { _op: 1 } })).toThrow(
        "Cannot modify reserved field '_op'"
      );
    });
  });

  describe('$push operator', () => {
    it('should reject pushing to _id field', () => {
      const doc = createDoc('1', {});

      expect(() => applyUpdate(doc, { $push: { _id: 'value' } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $push: { _id: 'value' } })).toThrow(
        "Cannot modify reserved field '_id'"
      );
    });

    it('should reject pushing to _seq field', () => {
      const doc = createDoc('1', {});

      expect(() => applyUpdate(doc, { $push: { _seq: 1 } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $push: { _seq: 1 } })).toThrow(
        "Cannot modify reserved field '_seq'"
      );
    });

    it('should reject pushing to _op field', () => {
      const doc = createDoc('1', {});

      expect(() => applyUpdate(doc, { $push: { _op: 'insert' } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $push: { _op: 'insert' } })).toThrow(
        "Cannot modify reserved field '_op'"
      );
    });
  });

  describe('$pull operator', () => {
    it('should reject pulling from _id field', () => {
      const doc = createDoc('1', {});

      expect(() => applyUpdate(doc, { $pull: { _id: 'value' } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $pull: { _id: 'value' } })).toThrow(
        "Cannot modify reserved field '_id'"
      );
    });

    it('should reject pulling from _seq field', () => {
      const doc = createDoc('1', {});

      expect(() => applyUpdate(doc, { $pull: { _seq: 1 } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $pull: { _seq: 1 } })).toThrow(
        "Cannot modify reserved field '_seq'"
      );
    });

    it('should reject pulling from _op field', () => {
      const doc = createDoc('1', {});

      expect(() => applyUpdate(doc, { $pull: { _op: 'insert' } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $pull: { _op: 'insert' } })).toThrow(
        "Cannot modify reserved field '_op'"
      );
    });
  });

  describe('$addToSet operator', () => {
    it('should reject adding to _id field', () => {
      const doc = createDoc('1', {});

      expect(() => applyUpdate(doc, { $addToSet: { _id: 'value' } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $addToSet: { _id: 'value' } })).toThrow(
        "Cannot modify reserved field '_id'"
      );
    });

    it('should reject adding to _seq field', () => {
      const doc = createDoc('1', {});

      expect(() => applyUpdate(doc, { $addToSet: { _seq: 1 } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $addToSet: { _seq: 1 } })).toThrow(
        "Cannot modify reserved field '_seq'"
      );
    });

    it('should reject adding to _op field', () => {
      const doc = createDoc('1', {});

      expect(() => applyUpdate(doc, { $addToSet: { _op: 'insert' } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $addToSet: { _op: 'insert' } })).toThrow(
        "Cannot modify reserved field '_op'"
      );
    });
  });

  describe('$pop operator', () => {
    it('should reject popping from _id field', () => {
      const doc = createDoc('1', {});

      expect(() => applyUpdate(doc, { $pop: { _id: 1 } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $pop: { _id: 1 } })).toThrow(
        "Cannot modify reserved field '_id'"
      );
    });

    it('should reject popping from _seq field', () => {
      const doc = createDoc('1', {});

      expect(() => applyUpdate(doc, { $pop: { _seq: 1 } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $pop: { _seq: 1 } })).toThrow(
        "Cannot modify reserved field '_seq'"
      );
    });

    it('should reject popping from _op field', () => {
      const doc = createDoc('1', {});

      expect(() => applyUpdate(doc, { $pop: { _op: 1 } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $pop: { _op: 1 } })).toThrow(
        "Cannot modify reserved field '_op'"
      );
    });
  });

  describe('$rename operator', () => {
    it('should reject renaming _id field', () => {
      const doc = createDoc('1', { name: 'Alice' });

      expect(() => applyUpdate(doc, { $rename: { _id: 'newId' } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $rename: { _id: 'newId' } })).toThrow(
        "Cannot modify reserved field '_id'"
      );
    });

    it('should reject renaming _seq field', () => {
      const doc = createDoc('1', { name: 'Alice' });

      expect(() => applyUpdate(doc, { $rename: { _seq: 'sequence' } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $rename: { _seq: 'sequence' } })).toThrow(
        "Cannot modify reserved field '_seq'"
      );
    });

    it('should reject renaming _op field', () => {
      const doc = createDoc('1', { name: 'Alice' });

      expect(() => applyUpdate(doc, { $rename: { _op: 'operation' } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $rename: { _op: 'operation' } })).toThrow(
        "Cannot modify reserved field '_op'"
      );
    });

    it('should reject renaming a field to _id', () => {
      const doc = createDoc('1', { name: 'Alice' });

      expect(() => applyUpdate(doc, { $rename: { name: '_id' } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $rename: { name: '_id' } })).toThrow(
        "Cannot modify reserved field '_id'"
      );
    });

    it('should reject renaming a field to _seq', () => {
      const doc = createDoc('1', { name: 'Alice' });

      expect(() => applyUpdate(doc, { $rename: { name: '_seq' } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $rename: { name: '_seq' } })).toThrow(
        "Cannot modify reserved field '_seq'"
      );
    });

    it('should reject renaming a field to _op', () => {
      const doc = createDoc('1', { name: 'Alice' });

      expect(() => applyUpdate(doc, { $rename: { name: '_op' } })).toThrow(ValidationError);
      expect(() => applyUpdate(doc, { $rename: { name: '_op' } })).toThrow(
        "Cannot modify reserved field '_op'"
      );
    });
  });

  describe('$setOnInsert operator (via createUpsertDocument)', () => {
    it('should reject setting _id field via $setOnInsert', () => {
      const filter = { name: 'Alice' };
      const update = { $setOnInsert: { _id: 'custom-id' } };

      expect(() => createUpsertDocument(filter, update)).toThrow(ValidationError);
      expect(() => createUpsertDocument(filter, update)).toThrow(
        "Cannot modify reserved field '_id'"
      );
    });

    it('should reject setting _seq field via $setOnInsert', () => {
      const filter = { name: 'Alice' };
      const update = { $setOnInsert: { _seq: 123 } };

      expect(() => createUpsertDocument(filter, update)).toThrow(ValidationError);
      expect(() => createUpsertDocument(filter, update)).toThrow(
        "Cannot modify reserved field '_seq'"
      );
    });

    it('should reject setting _op field via $setOnInsert', () => {
      const filter = { name: 'Alice' };
      const update = { $setOnInsert: { _op: 'insert' } };

      expect(() => createUpsertDocument(filter, update)).toThrow(ValidationError);
      expect(() => createUpsertDocument(filter, update)).toThrow(
        "Cannot modify reserved field '_op'"
      );
    });

    it('should allow setting non-reserved fields via $setOnInsert', () => {
      const filter = { name: 'Alice' };
      const update = { $setOnInsert: { createdAt: '2024-01-01', status: 'active' } };

      const result = createUpsertDocument(filter, update);

      expect((result as Record<string, unknown>).createdAt).toBe('2024-01-01');
      expect((result as Record<string, unknown>).status).toBe('active');
    });
  });
});
