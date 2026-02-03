/**
 * Document Types Tests
 *
 * Comprehensive tests for Document types and helper functions:
 * - BSONValue type validation
 * - Document interface
 * - isDocument type guard
 * - assertDocument assertion
 * - asDocument conversion
 * - AnyDocument type
 */

import { describe, it, expect } from 'vitest';
import {
  ObjectId,
  isDocument,
  assertDocument,
  asDocument,
  type Document,
  type BSONValue,
  type AnyDocument,
} from '../../../src/types.js';

// =============================================================================
// BSONValue Type Tests
// =============================================================================

describe('BSONValue Type', () => {
  describe('primitive values', () => {
    it('should accept string values', () => {
      const value: BSONValue = 'hello world';
      expect(typeof value).toBe('string');
    });

    it('should accept number values', () => {
      const intValue: BSONValue = 42;
      const floatValue: BSONValue = 3.14159;
      const negativeValue: BSONValue = -100;
      expect(typeof intValue).toBe('number');
      expect(typeof floatValue).toBe('number');
      expect(typeof negativeValue).toBe('number');
    });

    it('should accept boolean values', () => {
      const trueValue: BSONValue = true;
      const falseValue: BSONValue = false;
      expect(typeof trueValue).toBe('boolean');
      expect(typeof falseValue).toBe('boolean');
    });

    it('should accept null', () => {
      const value: BSONValue = null;
      expect(value).toBeNull();
    });
  });

  describe('complex values', () => {
    it('should accept Date objects', () => {
      const value: BSONValue = new Date('2024-01-01');
      expect(value).toBeInstanceOf(Date);
    });

    it('should accept Uint8Array (binary)', () => {
      const value: BSONValue = new Uint8Array([1, 2, 3, 4]);
      expect(value).toBeInstanceOf(Uint8Array);
    });

    it('should accept ObjectId', () => {
      const value: BSONValue = new ObjectId();
      expect(value).toBeInstanceOf(ObjectId);
    });

    it('should accept arrays of BSONValues', () => {
      const value: BSONValue = [1, 'two', true, null, new Date()];
      expect(Array.isArray(value)).toBe(true);
    });

    it('should accept nested objects', () => {
      const value: BSONValue = {
        name: 'test',
        count: 42,
        active: true,
        nested: {
          level: 1,
          data: [1, 2, 3],
        },
      };
      expect(typeof value).toBe('object');
    });
  });

  describe('deeply nested values', () => {
    it('should accept deeply nested structures', () => {
      const value: BSONValue = {
        level1: {
          level2: {
            level3: {
              level4: {
                value: 'deep',
              },
            },
          },
        },
      };
      expect((value as Record<string, unknown>).level1).toBeDefined();
    });

    it('should accept arrays containing objects', () => {
      const value: BSONValue = [
        { name: 'item1', price: 10 },
        { name: 'item2', price: 20 },
      ];
      expect(Array.isArray(value)).toBe(true);
      expect((value as Array<unknown>).length).toBe(2);
    });

    it('should accept objects containing arrays', () => {
      const value: BSONValue = {
        tags: ['tag1', 'tag2'],
        scores: [100, 95, 87],
        nested: {
          items: [{ id: 1 }, { id: 2 }],
        },
      };
      expect((value as Record<string, unknown>).tags).toBeDefined();
    });
  });
});

// =============================================================================
// isDocument Type Guard Tests
// =============================================================================

describe('isDocument Type Guard', () => {
  describe('valid documents', () => {
    it('should return true for empty object', () => {
      expect(isDocument({})).toBe(true);
    });

    it('should return true for object with string _id', () => {
      expect(isDocument({ _id: 'doc-123' })).toBe(true);
    });

    it('should return true for object with ObjectId _id', () => {
      expect(isDocument({ _id: new ObjectId() })).toBe(true);
    });

    it('should return true for object without _id', () => {
      expect(isDocument({ name: 'test', value: 42 })).toBe(true);
    });

    it('should return true for object with various fields', () => {
      const doc = {
        _id: 'test-123',
        name: 'Test Document',
        count: 100,
        active: true,
        tags: ['tag1', 'tag2'],
        metadata: { created: new Date() },
      };
      expect(isDocument(doc)).toBe(true);
    });

    it('should return true for object with nested structures', () => {
      const doc = {
        _id: new ObjectId(),
        user: {
          profile: {
            name: 'John',
            settings: {
              theme: 'dark',
            },
          },
        },
      };
      expect(isDocument(doc)).toBe(true);
    });
  });

  describe('invalid documents', () => {
    it('should return false for null', () => {
      expect(isDocument(null)).toBe(false);
    });

    it('should return false for undefined', () => {
      expect(isDocument(undefined)).toBe(false);
    });

    it('should return false for string', () => {
      expect(isDocument('not a document')).toBe(false);
    });

    it('should return false for number', () => {
      expect(isDocument(42)).toBe(false);
    });

    it('should return false for boolean', () => {
      expect(isDocument(true)).toBe(false);
      expect(isDocument(false)).toBe(false);
    });

    it('should return true for array (arrays are objects in JS)', () => {
      // Note: Arrays pass the isDocument check because they are objects in JavaScript
      // and don't have an invalid _id. This is a known behavior.
      expect(isDocument([1, 2, 3])).toBe(true);
      expect(isDocument([])).toBe(true);
    });

    it('should return false for object with numeric _id', () => {
      expect(isDocument({ _id: 123 })).toBe(false);
    });

    it('should return false for object with boolean _id', () => {
      expect(isDocument({ _id: true })).toBe(false);
    });

    it('should return false for object with null _id', () => {
      expect(isDocument({ _id: null })).toBe(false);
    });

    it('should return false for object with array _id', () => {
      expect(isDocument({ _id: ['id1', 'id2'] })).toBe(false);
    });

    it('should return false for object with object _id (not ObjectId)', () => {
      expect(isDocument({ _id: { value: 'test' } })).toBe(false);
    });

    it('should return true for Date object (dates are objects in JS)', () => {
      // Note: Date objects pass the isDocument check because they are objects in JavaScript
      // This is a known behavior - callers should use more specific checks if needed.
      expect(isDocument(new Date())).toBe(true);
    });

    it('should return false for function', () => {
      expect(isDocument(() => {})).toBe(false);
    });

    it('should return false for symbol', () => {
      expect(isDocument(Symbol('test'))).toBe(false);
    });
  });

  describe('edge cases', () => {
    it('should handle object with empty string _id', () => {
      expect(isDocument({ _id: '' })).toBe(true);
    });

    it('should handle object with ObjectId-like string _id', () => {
      expect(isDocument({ _id: '507f1f77bcf86cd799439011' })).toBe(true);
    });

    it('should handle object created with Object.create(null)', () => {
      const doc = Object.create(null);
      doc.name = 'test';
      expect(isDocument(doc)).toBe(true);
    });

    it('should handle frozen object', () => {
      const doc = Object.freeze({ _id: 'frozen', data: 'value' });
      expect(isDocument(doc)).toBe(true);
    });

    it('should handle sealed object', () => {
      const doc = Object.seal({ _id: 'sealed', data: 'value' });
      expect(isDocument(doc)).toBe(true);
    });
  });
});

// =============================================================================
// assertDocument Tests
// =============================================================================

describe('assertDocument Function', () => {
  describe('valid documents - should not throw', () => {
    it('should not throw for valid document with string _id', () => {
      expect(() => assertDocument({ _id: 'test-123' })).not.toThrow();
    });

    it('should not throw for valid document with ObjectId _id', () => {
      expect(() => assertDocument({ _id: new ObjectId() })).not.toThrow();
    });

    it('should not throw for valid document without _id', () => {
      expect(() => assertDocument({ name: 'test', value: 42 })).not.toThrow();
    });

    it('should not throw for empty object', () => {
      expect(() => assertDocument({})).not.toThrow();
    });
  });

  describe('invalid documents - should throw TypeError', () => {
    it('should throw for null', () => {
      expect(() => assertDocument(null)).toThrow(TypeError);
      expect(() => assertDocument(null)).toThrow('Value is not a valid Document');
    });

    it('should throw for undefined', () => {
      expect(() => assertDocument(undefined)).toThrow(TypeError);
    });

    it('should throw for string', () => {
      expect(() => assertDocument('not a document')).toThrow(TypeError);
    });

    it('should throw for number', () => {
      expect(() => assertDocument(42)).toThrow(TypeError);
    });

    it('should not throw for array (arrays are objects in JS)', () => {
      // Note: Arrays don't throw because they pass isDocument check
      expect(() => assertDocument([1, 2, 3])).not.toThrow();
    });

    it('should throw for object with numeric _id', () => {
      expect(() => assertDocument({ _id: 123 })).toThrow(TypeError);
    });

    it('should throw for object with object _id', () => {
      expect(() => assertDocument({ _id: { invalid: true } })).toThrow(TypeError);
    });
  });

  describe('custom error messages', () => {
    it('should use custom error message when provided', () => {
      expect(() => assertDocument(null, 'Custom error message')).toThrow(
        'Custom error message'
      );
    });

    it('should use custom message for invalid _id type', () => {
      expect(() => assertDocument({ _id: 123 }, 'Invalid document format')).toThrow(
        'Invalid document format'
      );
    });

    it('should use default message when not provided', () => {
      expect(() => assertDocument('invalid')).toThrow('Value is not a valid Document');
    });
  });

  describe('type narrowing', () => {
    it('should narrow type after assertion', () => {
      const maybeDoc: unknown = { _id: 'test', name: 'Test' };
      assertDocument(maybeDoc);
      // After assertion, maybeDoc is typed as Document
      // This test verifies the assertion doesn't throw
      expect(maybeDoc._id).toBe('test');
    });
  });
});

// =============================================================================
// asDocument Tests
// =============================================================================

describe('asDocument Function', () => {
  it('should return the same object with Document type', () => {
    const input = { _id: 'test-123', name: 'Test' };
    const result = asDocument(input);
    expect(result).toBe(input);
  });

  it('should work with AnyDocument type', () => {
    const input: AnyDocument = { _id: 'test', arbitrary: { nested: 'value' } };
    const result = asDocument(input);
    expect(result._id).toBe('test');
  });

  it('should preserve original object reference', () => {
    const original = { _id: 'ref-test', data: [1, 2, 3] };
    const converted = asDocument(original);
    converted.data = [4, 5, 6];
    expect(original.data).toEqual([4, 5, 6]); // Same reference
  });

  it('should work with complex nested structures', () => {
    const input = {
      _id: new ObjectId(),
      user: {
        profile: {
          settings: {
            theme: 'dark',
          },
        },
      },
      tags: ['a', 'b', 'c'],
    };
    const result = asDocument(input);
    expect(result._id).toBe(input._id);
    expect(result.user).toBe(input.user);
  });

  it('should work with objects without _id', () => {
    const input = { name: 'test', value: 42 };
    const result = asDocument(input);
    expect(result.name).toBe('test');
    expect(result._id).toBeUndefined();
  });
});

// =============================================================================
// Document Interface Tests
// =============================================================================

describe('Document Interface', () => {
  describe('basic structure', () => {
    it('should allow documents with string _id', () => {
      const doc: Document = { _id: 'string-id' };
      expect(doc._id).toBe('string-id');
    });

    it('should allow documents with ObjectId _id', () => {
      const oid = new ObjectId();
      const doc: Document = { _id: oid };
      expect(doc._id).toBe(oid);
    });

    it('should allow documents without _id', () => {
      const doc: Document = { name: 'no-id-doc' };
      expect(doc._id).toBeUndefined();
    });

    it('should allow arbitrary string keys', () => {
      const doc: Document = {
        _id: 'test',
        customField1: 'value1',
        customField2: 42,
        customField3: true,
      };
      expect(doc.customField1).toBe('value1');
    });
  });

  describe('nested documents', () => {
    it('should allow nested objects', () => {
      const doc: Document = {
        _id: 'nested-doc',
        user: {
          name: 'John',
          address: {
            city: 'NYC',
            zip: '10001',
          },
        },
      };
      expect((doc.user as Record<string, unknown>).name).toBe('John');
    });

    it('should allow arrays of documents', () => {
      const doc: Document = {
        _id: 'parent',
        children: [
          { _id: 'child1', name: 'First' },
          { _id: 'child2', name: 'Second' },
        ],
      };
      expect(Array.isArray(doc.children)).toBe(true);
    });
  });

  describe('BSON value types in documents', () => {
    it('should accept all BSON value types', () => {
      const doc: Document = {
        _id: 'bson-types',
        string: 'hello',
        number: 42,
        float: 3.14,
        boolean: true,
        null: null,
        date: new Date(),
        binary: new Uint8Array([1, 2, 3]),
        objectId: new ObjectId(),
        array: [1, 2, 3],
        object: { nested: 'value' },
      };
      expect(doc.string).toBe('hello');
      expect(doc.date).toBeInstanceOf(Date);
    });
  });
});

// =============================================================================
// AnyDocument Type Tests
// =============================================================================

describe('AnyDocument Type', () => {
  it('should accept documents with unknown _id type', () => {
    const doc: AnyDocument = { _id: 12345 as unknown }; // Numeric _id
    expect(doc._id).toBe(12345);
  });

  it('should accept documents with arbitrary field types', () => {
    const doc: AnyDocument = {
      _id: 'test',
      customFunction: () => 'hello',
      symbol: Symbol('test'),
      bigInt: BigInt(9007199254740991),
    };
    expect(typeof doc.customFunction).toBe('function');
  });

  it('should be usable with asDocument for type casting', () => {
    const anyDoc: AnyDocument = {
      _id: 'any-doc',
      specialField: new Map([['key', 'value']]),
    };
    const doc = asDocument(anyDoc);
    expect(doc._id).toBe('any-doc');
  });

  it('should work with external data sources', () => {
    // Simulating data from an external API
    const externalData: AnyDocument = JSON.parse('{"_id": "ext-123", "data": {"nested": true}}');
    expect(externalData._id).toBe('ext-123');
  });
});

// =============================================================================
// Type Guard Integration Tests
// =============================================================================

describe('Type Guard Integration', () => {
  it('should work in conditional narrowing', () => {
    const unknownValues: unknown[] = [
      { _id: 'valid-1' },
      null,
      { _id: new ObjectId() },
      'not a document',
      { _id: 123 },
      { name: 'no-id' },
      42,
    ];

    const validDocs = unknownValues.filter(isDocument);
    expect(validDocs.length).toBe(3);
  });

  it('should work with array.every', () => {
    const allValid = [{ _id: 'a' }, { _id: 'b' }, { name: 'c' }];
    expect(allValid.every(isDocument)).toBe(true);
  });

  it('should work with array.some', () => {
    const mixedValues = ['string', 42, { _id: 'doc' }, null];
    expect(mixedValues.some(isDocument)).toBe(true);
  });

  it('should chain with assertDocument for strict validation', () => {
    const validateAndProcess = (input: unknown): Document => {
      assertDocument(input);
      return input;
    };

    const validInput = { _id: 'test', data: 'value' };
    expect(validateAndProcess(validInput)).toBe(validInput);

    expect(() => validateAndProcess('invalid')).toThrow(TypeError);
  });
});
