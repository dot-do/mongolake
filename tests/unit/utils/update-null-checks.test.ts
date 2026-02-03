/**
 * Update Array Operations - Null/Type Check Tests (RED Phase)
 *
 * These tests verify proper null and type checking behavior in MongoDB-style
 * update array operations: $pull, $push, $addToSet, $pop
 *
 * Follows MongoDB behavior:
 * - Array operations on non-array fields throw TypeError
 * - Array operations on null fields throw TypeError
 * - Behavior on undefined fields varies by operator
 *
 * Issue: mongolake-rl21
 */

import { describe, it, expect } from 'vitest';
import { applyUpdate } from '../../../src/utils/update.js';

// =============================================================================
// Test Document Type
// =============================================================================

interface TestDocument {
  _id: string;
  tags?: string[];
  scores?: number[];
  items?: Array<{ id: number; name: string }>;
  count?: number;
  name?: string;
  active?: boolean;
  metadata?: Record<string, unknown>;
}

function createDoc(id: string, data: Partial<Omit<TestDocument, '_id'>> = {}): TestDocument {
  return { _id: id, ...data };
}

// =============================================================================
// $pull Null/Type Check Tests
// =============================================================================

describe('$pull null/type checks', () => {
  describe('$pull on non-array throws TypeError', () => {
    it('should throw TypeError when $pull is applied to a string field', () => {
      const doc = { _id: '1', tags: 'not-an-array' } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pull: { tags: 'a' } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $pull: { tags: 'a' } })).toThrow(
        /Cannot apply \$pull to field 'tags'/
      );
    });

    it('should throw TypeError when $pull is applied to a number field', () => {
      const doc = { _id: '1', tags: 123 } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pull: { tags: 123 } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $pull: { tags: 123 } })).toThrow(
        /Cannot apply \$pull to field 'tags'/
      );
    });

    it('should throw TypeError when $pull is applied to a boolean field', () => {
      const doc = { _id: '1', tags: true } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pull: { tags: true } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $pull: { tags: true } })).toThrow(
        /Cannot apply \$pull to field 'tags'/
      );
    });

    it('should throw TypeError when $pull is applied to an object field', () => {
      const doc = { _id: '1', tags: { key: 'value' } } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pull: { tags: 'key' } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $pull: { tags: 'key' } })).toThrow(
        /Cannot apply \$pull to field 'tags'/
      );
    });

    it('should throw TypeError when $pull is applied to a Date field', () => {
      const doc = { _id: '1', tags: new Date() } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pull: { tags: 'value' } })).toThrow(TypeError);
    });

    it('should throw TypeError when $pull is applied to a function field', () => {
      const doc = { _id: '1', tags: () => [] } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pull: { tags: 'value' } })).toThrow(TypeError);
    });
  });

  describe('$pull on null throws TypeError', () => {
    it('should throw TypeError when $pull is applied to a null field', () => {
      const doc = { _id: '1', tags: null } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pull: { tags: 'a' } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $pull: { tags: 'a' } })).toThrow(
        /Cannot apply \$pull to field 'tags'/
      );
    });

    it('should throw TypeError when $pull is applied to an explicitly null field (MongoDB compatibility)', () => {
      // MongoDB behavior: Cannot apply $pull to a value of non-array type
      const doc = createDoc('1', {});
      (doc as Record<string, unknown>).tags = null;

      expect(() => applyUpdate(doc, { $pull: { tags: 'a' } })).toThrow(TypeError);
    });
  });

  describe('$pull on undefined', () => {
    it('should do nothing when $pull is applied to an undefined field (MongoDB behavior)', () => {
      // MongoDB behavior: $pull on non-existent field does nothing
      const doc = createDoc('1', {});
      const result = applyUpdate(doc, { $pull: { tags: 'a' } });

      expect(result.tags).toBeUndefined();
    });

    it('should not create array when $pull is applied to missing field', () => {
      const doc = createDoc('1', { name: 'Alice' });
      const result = applyUpdate(doc, { $pull: { tags: 'x' } });

      expect('tags' in result).toBe(false);
      expect(result.name).toBe('Alice');
    });
  });
});

// =============================================================================
// $push Null/Type Check Tests
// =============================================================================

describe('$push null/type checks', () => {
  describe('$push on non-array throws TypeError', () => {
    it('should throw TypeError when $push is applied to a string field', () => {
      const doc = { _id: '1', tags: 'not-an-array' } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $push: { tags: 'new-item' } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $push: { tags: 'new-item' } })).toThrow(
        /Cannot apply \$push to field 'tags'/
      );
    });

    it('should throw TypeError when $push is applied to a number field', () => {
      const doc = { _id: '1', tags: 42 } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $push: { tags: 'new-item' } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $push: { tags: 'new-item' } })).toThrow(
        /Cannot apply \$push to field 'tags'/
      );
    });

    it('should throw TypeError when $push is applied to a boolean field', () => {
      const doc = { _id: '1', tags: false } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $push: { tags: 'new-item' } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $push: { tags: 'new-item' } })).toThrow(
        /Cannot apply \$push to field 'tags'/
      );
    });

    it('should throw TypeError when $push is applied to an object field', () => {
      const doc = { _id: '1', tags: { items: [] } } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $push: { tags: 'new-item' } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $push: { tags: 'new-item' } })).toThrow(
        /Cannot apply \$push to field 'tags'/
      );
    });

    it('should throw TypeError when $push with $each is applied to non-array field', () => {
      const doc = { _id: '1', tags: 'string-value' } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $push: { tags: { $each: ['a', 'b'] } } })).toThrow(TypeError);
    });

    it('should throw TypeError when $push is applied to a Symbol field', () => {
      const doc = { _id: '1', tags: Symbol('test') } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $push: { tags: 'new-item' } })).toThrow(TypeError);
    });
  });

  describe('$push on null throws TypeError', () => {
    it('should throw TypeError when $push is applied to a null field', () => {
      const doc = { _id: '1', tags: null } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $push: { tags: 'new-item' } })).toThrow(TypeError);
    });

    it('should throw TypeError when $push with $each is applied to null field', () => {
      const doc = { _id: '1', tags: null } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $push: { tags: { $each: ['a', 'b'] } } })).toThrow(TypeError);
    });
  });

  describe('$push on undefined creates array', () => {
    it('should create array when $push is applied to undefined field', () => {
      const doc = createDoc('1', {});
      const result = applyUpdate(doc, { $push: { tags: 'first-item' } });

      expect(result.tags).toEqual(['first-item']);
    });

    it('should create array when $push with $each is applied to undefined field', () => {
      const doc = createDoc('1', {});
      const result = applyUpdate(doc, { $push: { tags: { $each: ['a', 'b', 'c'] } } });

      expect(result.tags).toEqual(['a', 'b', 'c']);
    });
  });
});

// =============================================================================
// $addToSet Null/Type Check Tests
// =============================================================================

describe('$addToSet null/type checks', () => {
  describe('$addToSet on non-array throws TypeError', () => {
    it('should throw TypeError when $addToSet is applied to a string field', () => {
      const doc = { _id: '1', tags: 'hello-world' } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $addToSet: { tags: 'new-tag' } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $addToSet: { tags: 'new-tag' } })).toThrow(
        /Cannot apply \$addToSet to field 'tags'/
      );
    });

    it('should throw TypeError when $addToSet is applied to a number field', () => {
      const doc = { _id: '1', tags: 999 } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $addToSet: { tags: 'new-tag' } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $addToSet: { tags: 'new-tag' } })).toThrow(
        /Cannot apply \$addToSet to field 'tags'/
      );
    });

    it('should throw TypeError when $addToSet is applied to a boolean field', () => {
      const doc = { _id: '1', tags: true } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $addToSet: { tags: 'new-tag' } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $addToSet: { tags: 'new-tag' } })).toThrow(
        /Cannot apply \$addToSet to field 'tags'/
      );
    });

    it('should throw TypeError when $addToSet is applied to an object field', () => {
      const doc = { _id: '1', tags: { set: ['a', 'b'] } } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $addToSet: { tags: 'new-tag' } })).toThrow(TypeError);
      expect(() => applyUpdate(doc, { $addToSet: { tags: 'new-tag' } })).toThrow(
        /Cannot apply \$addToSet to field 'tags'/
      );
    });

    it('should throw TypeError when $addToSet with $each is applied to non-array field', () => {
      const doc = { _id: '1', tags: 'string-tags' } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $addToSet: { tags: { $each: ['x', 'y'] } } })).toThrow(
        TypeError
      );
    });

    it('should throw TypeError when $addToSet is applied to a nested non-array field', () => {
      const doc = { _id: '1', metadata: { tags: 'not-array' } } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $addToSet: { 'metadata.tags': 'value' } })).toThrow(
        TypeError
      );
    });
  });

  describe('$addToSet on null throws TypeError', () => {
    it('should throw TypeError when $addToSet is applied to a null field', () => {
      const doc = { _id: '1', tags: null } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $addToSet: { tags: 'new-tag' } })).toThrow(TypeError);
    });

    it('should throw TypeError when $addToSet with $each is applied to null field', () => {
      const doc = { _id: '1', tags: null } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $addToSet: { tags: { $each: ['a'] } } })).toThrow(TypeError);
    });
  });

  describe('$addToSet on undefined creates array', () => {
    it('should create array when $addToSet is applied to undefined field', () => {
      const doc = createDoc('1', {});
      const result = applyUpdate(doc, { $addToSet: { tags: 'first-tag' } });

      expect(result.tags).toEqual(['first-tag']);
    });

    it('should create array with unique items when $addToSet with $each is applied to undefined field', () => {
      const doc = createDoc('1', {});
      const result = applyUpdate(doc, { $addToSet: { tags: { $each: ['a', 'b', 'a', 'c'] } } });

      // $each adds items in order, skipping duplicates
      expect(result.tags).toEqual(['a', 'b', 'c']);
    });
  });
});

// =============================================================================
// $pop Null/Type Check Tests
// =============================================================================

describe('$pop null/type checks', () => {
  describe('$pop on non-array throws TypeError', () => {
    it('should throw TypeError when $pop is applied to a string field', () => {
      const doc = { _id: '1', tags: 'string-value' } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pop: { tags: 1 } })).toThrow(TypeError);
    });

    it('should throw TypeError when $pop is applied to a number field', () => {
      const doc = { _id: '1', tags: 100 } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pop: { tags: 1 } })).toThrow(TypeError);
    });

    it('should throw TypeError when $pop is applied to a boolean field', () => {
      const doc = { _id: '1', tags: false } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pop: { tags: -1 } })).toThrow(TypeError);
    });

    it('should throw TypeError when $pop is applied to an object field', () => {
      const doc = { _id: '1', tags: { first: 'a', last: 'z' } } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pop: { tags: 1 } })).toThrow(TypeError);
    });

    it('should throw TypeError when $pop (first) is applied to non-array', () => {
      const doc = { _id: '1', tags: 'abc' } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pop: { tags: -1 } })).toThrow(TypeError);
    });
  });

  describe('$pop on null throws TypeError', () => {
    it('should throw TypeError when $pop is applied to a null field', () => {
      const doc = { _id: '1', tags: null } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pop: { tags: 1 } })).toThrow(TypeError);
    });

    it('should throw TypeError when $pop (first) is applied to null field', () => {
      const doc = { _id: '1', tags: null } as unknown as TestDocument;

      expect(() => applyUpdate(doc, { $pop: { tags: -1 } })).toThrow(TypeError);
    });
  });

  describe('$pop on undefined', () => {
    it('should do nothing when $pop is applied to undefined field (MongoDB behavior)', () => {
      // MongoDB behavior: $pop on non-existent field does nothing
      const doc = createDoc('1', {});
      const result = applyUpdate(doc, { $pop: { tags: 1 } });

      expect(result.tags).toBeUndefined();
    });

    it('should not create array when $pop (first) is applied to missing field', () => {
      const doc = createDoc('1', { name: 'Test' });
      const result = applyUpdate(doc, { $pop: { tags: -1 } });

      expect('tags' in result).toBe(false);
      expect(result.name).toBe('Test');
    });
  });
});

// =============================================================================
// Multiple Field Null Check Tests
// =============================================================================

describe('Multiple field null/type checks', () => {
  it('should throw on first invalid field when multiple fields have type errors', () => {
    const doc = {
      _id: '1',
      tags: 'not-array',
      scores: 'also-not-array',
    } as unknown as TestDocument;

    // Should throw on the first encountered error
    expect(() =>
      applyUpdate(doc, {
        $push: { tags: 'a', scores: 1 },
      })
    ).toThrow(TypeError);
  });

  it('should validate all array operators independently', () => {
    const doc = { _id: '1', tags: null, scores: ['valid'] } as unknown as TestDocument;

    // $push on null should throw even if $addToSet target is valid
    expect(() =>
      applyUpdate(doc, {
        $push: { tags: 'a' },
        $addToSet: { scores: 99 },
      })
    ).toThrow(TypeError);
  });
});

// =============================================================================
// Edge Cases for Null/Type Checks
// =============================================================================

describe('Edge cases for null/type checks', () => {
  it('should handle empty string as non-array for $push', () => {
    const doc = { _id: '1', tags: '' } as unknown as TestDocument;

    expect(() => applyUpdate(doc, { $push: { tags: 'item' } })).toThrow(TypeError);
  });

  it('should handle zero as non-array for $pull', () => {
    const doc = { _id: '1', tags: 0 } as unknown as TestDocument;

    expect(() => applyUpdate(doc, { $pull: { tags: 0 } })).toThrow(TypeError);
  });

  it('should handle NaN as non-array for $addToSet', () => {
    const doc = { _id: '1', tags: NaN } as unknown as TestDocument;

    expect(() => applyUpdate(doc, { $addToSet: { tags: 'item' } })).toThrow(TypeError);
  });

  it('should handle Infinity as non-array for $pop', () => {
    const doc = { _id: '1', tags: Infinity } as unknown as TestDocument;

    expect(() => applyUpdate(doc, { $pop: { tags: 1 } })).toThrow(TypeError);
  });

  it('should correctly identify array-like objects as non-arrays', () => {
    // Array-like objects (with length property) are not actual arrays
    const doc = { _id: '1', tags: { 0: 'a', 1: 'b', length: 2 } } as unknown as TestDocument;

    expect(() => applyUpdate(doc, { $push: { tags: 'c' } })).toThrow(TypeError);
  });

  it('should work with valid empty array', () => {
    const doc = createDoc('1', { tags: [] });
    const result = applyUpdate(doc, { $push: { tags: 'first' } });

    expect(result.tags).toEqual(['first']);
  });

  it('should preserve immutability when type check fails', () => {
    const doc = { _id: '1', tags: 'not-array', name: 'original' } as unknown as TestDocument;
    const originalTags = doc.tags;

    try {
      applyUpdate(doc, { $push: { tags: 'item' } });
    } catch {
      // Document should not be mutated
      expect(doc.tags).toBe(originalTags);
    }
  });
});
