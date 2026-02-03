/**
 * Update Type Safety Tests (RED Phase)
 *
 * These tests detect unsafe type coercion in MongoDB update operations.
 * They are intentionally written as FAILING tests to drive implementation
 * of proper type checking in the update utilities.
 *
 * Issue: mongolake-rew9
 */

import { describe, it, expect } from 'vitest';
import { applyUpdate } from '../../../src/utils/update.js';

// =============================================================================
// Test Document Types
// =============================================================================

interface TestDocument {
  _id: string;
  count?: number;
  name?: string;
  tags?: string[];
  scores?: number[];
  active?: boolean;
  metadata?: Record<string, unknown>;
  nested?: { value: number };
}

function createDoc(id: string, data: Partial<Omit<TestDocument, '_id'>> = {}): TestDocument {
  return { _id: id, ...data };
}

// =============================================================================
// $inc Type Safety Tests
// =============================================================================

describe('$inc Type Safety', () => {
  describe('should throw TypeError for non-numeric field types', () => {
    it('should throw TypeError when $inc is applied to a string field', () => {
      const doc = { _id: '1', count: 'five' } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $inc: { count: 1 } })).toThrow(TypeError);
    });

    it('should throw TypeError when $inc is applied to a null field', () => {
      // MongoDB behavior: $inc on null throws "Cannot apply $inc to a value of non-numeric type"
      const doc = { _id: '1', count: null } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $inc: { count: 1 } })).toThrow(TypeError);
    });

    it('should work when $inc is applied to an undefined field (treat as 0)', () => {
      const doc = { _id: '1' } as TestDocument;
      const result = applyUpdate(doc, { $inc: { count: 5 } });

      // Undefined fields should initialize to the increment amount (this is valid MongoDB behavior)
      expect(result.count).toBe(5);
    });

    it('should throw TypeError when $inc is applied to an object field', () => {
      const doc = { _id: '1', count: { value: 5 } } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $inc: { count: 1 } })).toThrow(TypeError);
    });

    it('should throw TypeError when $inc is applied to an array field', () => {
      const doc = { _id: '1', count: [1, 2, 3] } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $inc: { count: 1 } })).toThrow(TypeError);
    });

    it('should throw TypeError when $inc is applied to a boolean field', () => {
      const doc = { _id: '1', count: true } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $inc: { count: 1 } })).toThrow(TypeError);
    });

    it('should throw TypeError when $inc is applied to a Date field', () => {
      const doc = { _id: '1', count: new Date() } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $inc: { count: 1 } })).toThrow(TypeError);
    });

    it('should throw TypeError when $inc is applied to a function field', () => {
      const doc = { _id: '1', count: () => 5 } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $inc: { count: 1 } })).toThrow(TypeError);
    });

    it('should throw TypeError for numeric string that looks like a number', () => {
      // Even "123" should not be coerced to a number
      const doc = { _id: '1', count: '123' } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $inc: { count: 1 } })).toThrow(TypeError);
    });

    it('should throw TypeError when $inc amount is non-numeric', () => {
      const doc = createDoc('1', { count: 5 });

      expect(() =>
        applyUpdate(doc, { $inc: { count: '2' as unknown as number } })
      ).toThrow(TypeError);
    });
  });

  describe('$inc with valid numeric types', () => {
    it('should work with integer values', () => {
      const doc = createDoc('1', { count: 10 });
      const result = applyUpdate(doc, { $inc: { count: 5 } });

      expect(result.count).toBe(15);
    });

    it('should work with floating point values', () => {
      const doc = createDoc('1', { count: 10.5 });
      const result = applyUpdate(doc, { $inc: { count: 2.5 } });

      expect(result.count).toBe(13);
    });

    it('should work with negative values', () => {
      const doc = createDoc('1', { count: 10 });
      const result = applyUpdate(doc, { $inc: { count: -3 } });

      expect(result.count).toBe(7);
    });

    it('should work with zero', () => {
      const doc = createDoc('1', { count: 0 });
      const result = applyUpdate(doc, { $inc: { count: 5 } });

      expect(result.count).toBe(5);
    });

    it('should work with Infinity', () => {
      const doc = createDoc('1', { count: 0 });
      const result = applyUpdate(doc, { $inc: { count: Infinity } });

      expect(result.count).toBe(Infinity);
    });

    it('should return NaN when incrementing by NaN', () => {
      const doc = createDoc('1', { count: 5 });
      const result = applyUpdate(doc, { $inc: { count: NaN } });

      expect(result.count).toBeNaN();
    });
  });
});

// =============================================================================
// $push Type Safety Tests
// =============================================================================

describe('$push Type Safety', () => {
  describe('should throw TypeError when pushing to non-array fields', () => {
    it('should throw TypeError when $push is applied to a string field', () => {
      const doc = { _id: '1', tags: 'hello' } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $push: { tags: 'world' } })).toThrow(TypeError);
    });

    it('should throw TypeError when $push is applied to a number field', () => {
      const doc = { _id: '1', tags: 123 } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $push: { tags: 'a' } })).toThrow(TypeError);
    });

    it('should throw TypeError when $push is applied to a boolean field', () => {
      const doc = { _id: '1', tags: true } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $push: { tags: 'a' } })).toThrow(TypeError);
    });

    it('should throw TypeError when $push is applied to an object field', () => {
      const doc = { _id: '1', tags: { value: 'hello' } } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $push: { tags: 'a' } })).toThrow(TypeError);
    });

    it('should throw TypeError when $push is applied to a null field', () => {
      const doc = { _id: '1', tags: null } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $push: { tags: 'a' } })).toThrow(TypeError);
    });

    it('should throw TypeError when $push with $each is applied to non-array field', () => {
      const doc = { _id: '1', tags: 'hello' } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $push: { tags: { $each: ['a', 'b'] } } })).toThrow(
        TypeError
      );
    });
  });

  describe('$push with valid array fields', () => {
    it('should work with existing array', () => {
      const doc = createDoc('1', { tags: ['a', 'b'] });
      const result = applyUpdate(doc, { $push: { tags: 'c' } });

      expect(result.tags).toEqual(['a', 'b', 'c']);
    });

    it('should create array for undefined field', () => {
      const doc = createDoc('1', {});
      const result = applyUpdate(doc, { $push: { tags: 'a' } });

      expect(result.tags).toEqual(['a']);
    });

    it('should work with empty array', () => {
      const doc = createDoc('1', { tags: [] });
      const result = applyUpdate(doc, { $push: { tags: 'a' } });

      expect(result.tags).toEqual(['a']);
    });
  });
});

// =============================================================================
// $pull Type Safety Tests
// =============================================================================

describe('$pull Type Safety', () => {
  describe('should throw TypeError when pulling from non-array fields', () => {
    it('should throw TypeError when $pull is applied to a string field', () => {
      const doc = { _id: '1', tags: 'hello' } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pull: { tags: 'h' } })).toThrow(TypeError);
    });

    it('should throw TypeError when $pull is applied to a number field', () => {
      const doc = { _id: '1', tags: 123 } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pull: { tags: 1 } })).toThrow(TypeError);
    });

    it('should throw TypeError when $pull is applied to a boolean field', () => {
      const doc = { _id: '1', tags: true } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pull: { tags: true } })).toThrow(TypeError);
    });

    it('should throw TypeError when $pull is applied to an object field', () => {
      const doc = { _id: '1', tags: { a: 1 } } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pull: { tags: 'a' } })).toThrow(TypeError);
    });

    it('should throw TypeError when $pull is applied to a null field', () => {
      const doc = { _id: '1', tags: null } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pull: { tags: 'a' } })).toThrow(TypeError);
    });
  });

  describe('$pull with valid array fields', () => {
    it('should work with existing array', () => {
      const doc = createDoc('1', { tags: ['a', 'b', 'c'] });
      const result = applyUpdate(doc, { $pull: { tags: 'b' } });

      expect(result.tags).toEqual(['a', 'c']);
    });

    it('should work with empty array', () => {
      const doc = createDoc('1', { tags: [] });
      const result = applyUpdate(doc, { $pull: { tags: 'a' } });

      expect(result.tags).toEqual([]);
    });

    it('should do nothing for undefined field (MongoDB behavior)', () => {
      const doc = createDoc('1', {});
      const result = applyUpdate(doc, { $pull: { tags: 'a' } });

      // MongoDB $pull does nothing if the field doesn't exist
      expect(result.tags).toBeUndefined();
    });
  });
});

// =============================================================================
// $addToSet Type Safety Tests
// =============================================================================

describe('$addToSet Type Safety', () => {
  describe('should throw TypeError when adding to non-array fields', () => {
    it('should throw TypeError when $addToSet is applied to a string field', () => {
      const doc = { _id: '1', tags: 'hello' } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $addToSet: { tags: 'world' } })).toThrow(TypeError);
    });

    it('should throw TypeError when $addToSet is applied to a number field', () => {
      const doc = { _id: '1', tags: 123 } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $addToSet: { tags: 'a' } })).toThrow(TypeError);
    });

    it('should throw TypeError when $addToSet is applied to a boolean field', () => {
      const doc = { _id: '1', tags: true } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $addToSet: { tags: 'a' } })).toThrow(TypeError);
    });

    it('should throw TypeError when $addToSet is applied to an object field', () => {
      const doc = { _id: '1', tags: { value: 1 } } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $addToSet: { tags: 'a' } })).toThrow(TypeError);
    });

    it('should throw TypeError when $addToSet is applied to a null field', () => {
      const doc = { _id: '1', tags: null } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $addToSet: { tags: 'a' } })).toThrow(TypeError);
    });

    it('should throw TypeError when $addToSet with $each is applied to non-array field', () => {
      const doc = { _id: '1', tags: 'hello' } as unknown as TestDocument;

      expect(() =>
        applyUpdate(doc, { $addToSet: { tags: { $each: ['a', 'b'] } } })
      ).toThrow(TypeError);
    });
  });

  describe('$addToSet with valid array fields', () => {
    it('should work with existing array', () => {
      const doc = createDoc('1', { tags: ['a', 'b'] });
      const result = applyUpdate(doc, { $addToSet: { tags: 'c' } });

      expect(result.tags).toEqual(['a', 'b', 'c']);
    });

    it('should not add duplicate values', () => {
      const doc = createDoc('1', { tags: ['a', 'b'] });
      const result = applyUpdate(doc, { $addToSet: { tags: 'a' } });

      expect(result.tags).toEqual(['a', 'b']);
    });

    it('should create array for undefined field', () => {
      const doc = createDoc('1', {});
      const result = applyUpdate(doc, { $addToSet: { tags: 'a' } });

      expect(result.tags).toEqual(['a']);
    });
  });
});

// =============================================================================
// $pop Type Safety Tests
// =============================================================================

describe('$pop Type Safety', () => {
  describe('should throw TypeError when popping from non-array fields', () => {
    it('should throw TypeError when $pop is applied to a string field', () => {
      const doc = { _id: '1', tags: 'hello' } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pop: { tags: 1 } })).toThrow(TypeError);
    });

    it('should throw TypeError when $pop is applied to a number field', () => {
      const doc = { _id: '1', tags: 123 } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pop: { tags: 1 } })).toThrow(TypeError);
    });

    it('should throw TypeError when $pop is applied to an object field', () => {
      const doc = { _id: '1', tags: { a: 1 } } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pop: { tags: 1 } })).toThrow(TypeError);
    });

    it('should throw TypeError when $pop is applied to a null field', () => {
      const doc = { _id: '1', tags: null } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pop: { tags: 1 } })).toThrow(TypeError);
    });
  });

  describe('$pop with valid array fields', () => {
    it('should work with existing array (pop last)', () => {
      const doc = createDoc('1', { tags: ['a', 'b', 'c'] });
      const result = applyUpdate(doc, { $pop: { tags: 1 } });

      expect(result.tags).toEqual(['a', 'b']);
    });

    it('should work with existing array (pop first)', () => {
      const doc = createDoc('1', { tags: ['a', 'b', 'c'] });
      const result = applyUpdate(doc, { $pop: { tags: -1 } });

      expect(result.tags).toEqual(['b', 'c']);
    });
  });
});

// =============================================================================
// $set Type Preservation Tests
// =============================================================================

describe('$set Type Preservation', () => {
  describe('should preserve types correctly', () => {
    it('should preserve number type', () => {
      const doc = createDoc('1', { count: 10 });
      const result = applyUpdate(doc, { $set: { count: 20 } });

      expect(result.count).toBe(20);
      expect(typeof result.count).toBe('number');
    });

    it('should preserve string type', () => {
      const doc = createDoc('1', { name: 'Alice' });
      const result = applyUpdate(doc, { $set: { name: 'Bob' } });

      expect(result.name).toBe('Bob');
      expect(typeof result.name).toBe('string');
    });

    it('should preserve boolean type', () => {
      const doc = createDoc('1', { active: true });
      const result = applyUpdate(doc, { $set: { active: false } });

      expect(result.active).toBe(false);
      expect(typeof result.active).toBe('boolean');
    });

    it('should preserve array type', () => {
      const doc = createDoc('1', { tags: ['a'] });
      const result = applyUpdate(doc, { $set: { tags: ['b', 'c'] } });

      expect(result.tags).toEqual(['b', 'c']);
      expect(Array.isArray(result.tags)).toBe(true);
    });

    it('should preserve object type', () => {
      const doc = createDoc('1', { metadata: { key: 'value' } });
      const result = applyUpdate(doc, { $set: { metadata: { newKey: 'newValue' } } });

      expect(result.metadata).toEqual({ newKey: 'newValue' });
      expect(typeof result.metadata).toBe('object');
    });

    it('should allow type change with $set (number to string)', () => {
      const doc = { _id: '1', value: 123 } as unknown as TestDocument;
      const result = applyUpdate(doc, { $set: { value: 'hello' } });

      expect((result as unknown as { value: string }).value).toBe('hello');
    });
  });

  describe('$set with null', () => {
    it('should set field to null', () => {
      const doc = createDoc('1', { name: 'Alice' });
      const result = applyUpdate(doc, { $set: { name: null } });

      expect(result.name).toBeNull();
    });

    it('should set nested field to null', () => {
      const doc = createDoc('1', { nested: { value: 5 } });
      const result = applyUpdate(doc, { $set: { 'nested.value': null } });

      expect(result.nested?.value).toBeNull();
    });
  });

  describe('$set with undefined should remove field', () => {
    it('should remove field when set to undefined', () => {
      const doc = createDoc('1', { name: 'Alice', count: 5 });
      const result = applyUpdate(doc, { $set: { name: undefined } });

      // MongoDB behavior: setting a field to undefined removes it
      expect('name' in result).toBe(false);
      expect(result.count).toBe(5);
    });

    it('should remove nested field when set to undefined', () => {
      const doc = createDoc('1', { nested: { value: 5 } });
      const result = applyUpdate(doc, { $set: { 'nested.value': undefined } });

      expect('value' in (result.nested || {})).toBe(false);
    });
  });
});

// =============================================================================
// Combined Type Safety Tests
// =============================================================================

describe('Combined Update Type Safety', () => {
  it('should validate types across multiple operators in single update', () => {
    const doc = {
      _id: '1',
      count: 'not a number', // Should fail $inc
      tags: 'not an array', // Should fail $push
    } as unknown as TestDocument;

    // Should throw on first invalid operation
    expect(() =>
      applyUpdate(doc, {
        $inc: { count: 1 },
        $push: { tags: 'a' },
      })
    ).toThrow(TypeError);
  });

  it('should not partially apply updates when type validation fails', () => {
    const doc = {
      _id: '1',
      name: 'Alice',
      count: 'invalid',
    } as unknown as TestDocument;

    const originalDoc = { ...doc };

    try {
      applyUpdate(doc, {
        $set: { name: 'Bob' },
        $inc: { count: 1 },
      });
    } catch {
      // Document should not be mutated even for $set before the failing $inc
      expect(doc.name).toBe(originalDoc.name);
    }
  });
});

// =============================================================================
// Edge Cases
// =============================================================================

describe('Type Safety Edge Cases', () => {
  it('should handle nested path $inc with non-numeric intermediate', () => {
    const doc = {
      _id: '1',
      nested: 'not an object',
    } as unknown as TestDocument;

    expect(() => applyUpdate(doc, { $inc: { 'nested.value': 1 } })).toThrow(TypeError);
  });

  it('should handle array index in $inc path', () => {
    const doc = {
      _id: '1',
      scores: [1, 2, 3],
    };

    const result = applyUpdate(doc, { $inc: { 'scores.1': 10 } });
    expect(result.scores[1]).toBe(12);
  });

  it('should throw TypeError for $inc on array element that is not a number', () => {
    const doc = {
      _id: '1',
      items: ['a', 'b', 'c'],
    };

    expect(() => applyUpdate(doc, { $inc: { 'items.0': 1 } })).toThrow(TypeError);
  });
});
