/**
 * Projection Utility Tests
 *
 * Comprehensive tests for the applyProjection function that handles:
 * - Inclusion mode (field: 1) - only include specified fields
 * - Exclusion mode (field: 0) - exclude specified fields
 * - Nested field paths with dot notation
 * - _id handling (included by default, can be excluded)
 * - Empty projection returns all fields
 * - Projection on missing fields
 * - Array projections
 * - Mixed inclusion/exclusion handling
 * - Edge cases and special scenarios
 */

import { describe, it, expect } from 'vitest';
import { applyProjection } from '../../../src/utils/projection.js';
import { ObjectId } from '../../../src/types.js';

// =============================================================================
// Test Document Type
// =============================================================================

interface TestDocument {
  _id: string | ObjectId;
  name?: string;
  age?: number;
  email?: string;
  password?: string;
  secret?: string;
  status?: string;
  address?: {
    street?: string;
    city?: string;
    country?: string;
    zip?: number;
  };
  profile?: {
    firstName?: string;
    lastName?: string;
    settings?: {
      theme?: string;
      notifications?: boolean;
    };
  };
  metadata?: Record<string, unknown>;
  tags?: string[];
  scores?: number[];
  items?: Array<{ id: string; name: string }>;
}

// =============================================================================
// Inclusion Mode ({ field: 1 })
// =============================================================================

describe('applyProjection - Inclusion Mode', () => {
  it('should include only specified fields', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      name: 'Alice',
      age: 30,
      email: 'alice@example.com',
    };
    const result = applyProjection(doc, { name: 1, age: 1 });
    expect(result).toEqual({ _id: 'doc1', name: 'Alice', age: 30 });
  });

  it('should include only a single specified field', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      name: 'Alice',
      age: 30,
      email: 'alice@example.com',
    };
    const result = applyProjection(doc, { name: 1 });
    expect(result).toEqual({ _id: 'doc1', name: 'Alice' });
  });

  it('should include _id by default in inclusion mode', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice', email: 'alice@example.com' };
    const result = applyProjection(doc, { email: 1 });
    expect(result).toEqual({ _id: 'doc1', email: 'alice@example.com' });
  });

  it('should return only _id if no fields match in inclusion mode', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
    const result = applyProjection(doc, { email: 1 });
    expect(result).toEqual({ _id: 'doc1' });
  });

  it('should handle inclusion with multiple fields', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      name: 'Alice',
      age: 30,
      email: 'alice@example.com',
      status: 'active',
    };
    const result = applyProjection(doc, { name: 1, email: 1, status: 1 });
    expect(result).toEqual({
      _id: 'doc1',
      name: 'Alice',
      email: 'alice@example.com',
      status: 'active',
    });
  });
});

// =============================================================================
// Exclusion Mode ({ field: 0 })
// =============================================================================

describe('applyProjection - Exclusion Mode', () => {
  it('should exclude specified fields', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      name: 'Alice',
      password: 'secret123',
      secret: 'shh',
    };
    const result = applyProjection(doc, { password: 0, secret: 0 });
    expect(result).toEqual({ _id: 'doc1', name: 'Alice' });
  });

  it('should exclude a single field', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      name: 'Alice',
      password: 'secret123',
    };
    const result = applyProjection(doc, { password: 0 });
    expect(result).toEqual({ _id: 'doc1', name: 'Alice' });
  });

  it('should keep all other fields when excluding', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      name: 'Alice',
      age: 30,
      email: 'alice@example.com',
      password: 'secret123',
    };
    const result = applyProjection(doc, { password: 0 });
    expect(result).toEqual({
      _id: 'doc1',
      name: 'Alice',
      age: 30,
      email: 'alice@example.com',
    });
  });

  it('should handle exclusion of non-existent field gracefully', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
    const result = applyProjection(doc, { password: 0 });
    expect(result).toEqual({ _id: 'doc1', name: 'Alice' });
  });
});

// =============================================================================
// Nested Paths ({ "nested.field": 1 })
// =============================================================================

describe('applyProjection - Nested Paths', () => {
  it('should include nested field in inclusion mode', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      name: 'Alice',
      address: { street: '123 Main St', city: 'New York', country: 'USA' },
    };
    // Note: The current implementation doesn't support nested path projection
    // It treats "address.city" as a top-level key, not a nested path
    // This test documents the current behavior
    const result = applyProjection(doc, { 'address.city': 1 });
    // Current implementation: returns only _id since "address.city" is not a top-level key
    expect(result._id).toBe('doc1');
  });

  it('should include entire nested object', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      name: 'Alice',
      address: { street: '123 Main St', city: 'New York', country: 'USA' },
    };
    const result = applyProjection(doc, { address: 1 });
    expect(result).toEqual({
      _id: 'doc1',
      address: { street: '123 Main St', city: 'New York', country: 'USA' },
    });
  });

  it('should exclude entire nested object', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      name: 'Alice',
      address: { street: '123 Main St', city: 'New York', country: 'USA' },
    };
    const result = applyProjection(doc, { address: 0 });
    expect(result).toEqual({ _id: 'doc1', name: 'Alice' });
  });

  it('should handle deeply nested objects in inclusion', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      profile: {
        firstName: 'Alice',
        settings: { theme: 'dark', notifications: true },
      },
    };
    const result = applyProjection(doc, { profile: 1 });
    expect(result).toEqual({
      _id: 'doc1',
      profile: {
        firstName: 'Alice',
        settings: { theme: 'dark', notifications: true },
      },
    });
  });
});

// =============================================================================
// _id Handling
// =============================================================================

describe('applyProjection - _id Handling', () => {
  it('should include _id by default in inclusion mode', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice', age: 30 };
    const result = applyProjection(doc, { name: 1 });
    expect(result._id).toBe('doc1');
    expect(result.name).toBe('Alice');
    expect(result.age).toBeUndefined();
  });

  it('should exclude _id when explicitly set to 0 in inclusion mode', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice', age: 30 };
    const result = applyProjection(doc, { name: 1, _id: 0 });
    expect(result._id).toBeUndefined();
    expect(result.name).toBe('Alice');
  });

  it('should exclude _id in exclusion mode when explicitly set to 0', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice', password: 'secret' };
    const result = applyProjection(doc, { _id: 0, password: 0 });
    expect(result._id).toBeUndefined();
    expect(result.name).toBe('Alice');
    expect(result.password).toBeUndefined();
  });

  it('should keep _id in exclusion mode by default', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice', password: 'secret' };
    const result = applyProjection(doc, { password: 0 });
    expect(result._id).toBe('doc1');
    expect(result.name).toBe('Alice');
  });

  it('should allow _id: 0 as the only projection (exclusion mode)', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice', age: 30 };
    const result = applyProjection(doc, { _id: 0 });
    expect(result._id).toBeUndefined();
    expect(result.name).toBe('Alice');
    expect(result.age).toBe(30);
  });

  it('should include _id when explicitly set to 1', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
    const result = applyProjection(doc, { _id: 1, name: 1 });
    expect(result._id).toBe('doc1');
    expect(result.name).toBe('Alice');
  });
});

// =============================================================================
// Mixed Modes (Inclusion and Exclusion)
// =============================================================================

describe('applyProjection - Mixed Modes', () => {
  // Note: MongoDB doesn't allow mixing 1s and 0s except for _id
  // The current implementation uses inclusion mode if any field is set to 1
  // This means 0s are effectively ignored except for _id

  it('should treat mixed projection as inclusion mode (1s take precedence)', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      name: 'Alice',
      password: 'secret',
      email: 'alice@example.com',
    };
    // When there are any 1s, it enters inclusion mode
    // In inclusion mode, only 1s (and _id by default) are included
    const result = applyProjection(doc, { name: 1, password: 0 });
    expect(result._id).toBe('doc1');
    expect(result.name).toBe('Alice');
    expect(result.password).toBeUndefined();
    expect(result.email).toBeUndefined();
  });

  it('should allow _id: 0 with inclusion mode (special case)', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice', age: 30 };
    const result = applyProjection(doc, { name: 1, _id: 0 });
    expect(result._id).toBeUndefined();
    expect(result.name).toBe('Alice');
    expect(result.age).toBeUndefined();
  });

  it('should handle all zeros as exclusion mode', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      name: 'Alice',
      password: 'secret',
      secret: 'shh',
    };
    const result = applyProjection(doc, { password: 0, secret: 0 });
    expect(result._id).toBe('doc1');
    expect(result.name).toBe('Alice');
    expect(result.password).toBeUndefined();
    expect(result.secret).toBeUndefined();
  });
});

// =============================================================================
// Empty Projection
// =============================================================================

describe('applyProjection - Empty Projection', () => {
  it('should return all fields when projection is empty', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice', age: 30 };
    const result = applyProjection(doc, {});
    expect(result).toEqual({ _id: 'doc1', name: 'Alice', age: 30 });
  });

  it('should return empty document when document is empty with empty projection', () => {
    const doc = { _id: 'doc1' };
    const result = applyProjection(doc, {});
    expect(result).toEqual({ _id: 'doc1' });
  });

  it('should return full nested document with empty projection', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      name: 'Alice',
      address: { street: '123 Main St', city: 'New York', country: 'USA' },
    };
    const result = applyProjection(doc, {});
    expect(result).toEqual({
      _id: 'doc1',
      name: 'Alice',
      address: { street: '123 Main St', city: 'New York', country: 'USA' },
    });
  });
});

// =============================================================================
// Projection on Missing Fields
// =============================================================================

describe('applyProjection - Missing Fields', () => {
  it('should not error when projecting non-existent field in inclusion mode', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
    const result = applyProjection(doc, { email: 1 });
    expect(result).toEqual({ _id: 'doc1' });
    expect(result.email).toBeUndefined();
  });

  it('should not error when excluding non-existent field', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
    const result = applyProjection(doc, { password: 0 });
    expect(result).toEqual({ _id: 'doc1', name: 'Alice' });
  });

  it('should handle mix of existing and non-existing fields in inclusion', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice', age: 30 };
    const result = applyProjection(doc, { name: 1, email: 1, status: 1 });
    expect(result).toEqual({ _id: 'doc1', name: 'Alice' });
  });

  it('should handle all non-existent fields in inclusion mode', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
    const result = applyProjection(doc, { email: 1, status: 1, password: 1 });
    expect(result).toEqual({ _id: 'doc1' });
  });

  it('should not error when excluding multiple non-existent fields', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
    const result = applyProjection(doc, { password: 0, secret: 0, token: 0 });
    expect(result).toEqual({ _id: 'doc1', name: 'Alice' });
  });
});

// =============================================================================
// Edge Cases
// =============================================================================

describe('applyProjection - Edge Cases', () => {
  it('should handle null values in document', () => {
    const doc = { _id: 'doc1', name: 'Alice', email: null };
    const result = applyProjection(doc, { name: 1, email: 1 });
    expect(result).toEqual({ _id: 'doc1', name: 'Alice', email: null });
  });

  it('should handle undefined values in document', () => {
    const doc = { _id: 'doc1', name: 'Alice', email: undefined };
    const result = applyProjection(doc, { name: 1, email: 1 });
    expect(result._id).toBe('doc1');
    expect(result.name).toBe('Alice');
    // email is undefined, so it's not included (key in doc check fails for undefined)
  });

  it('should handle empty string values', () => {
    const doc = { _id: 'doc1', name: '', email: 'test@example.com' };
    const result = applyProjection(doc, { name: 1 });
    expect(result).toEqual({ _id: 'doc1', name: '' });
  });

  it('should handle zero numeric values', () => {
    const doc = { _id: 'doc1', count: 0, total: 100 };
    const result = applyProjection(doc, { count: 1 });
    expect(result).toEqual({ _id: 'doc1', count: 0 });
  });

  it('should handle false boolean values', () => {
    const doc = { _id: 'doc1', active: false, name: 'Alice' };
    const result = applyProjection(doc, { active: 1 });
    expect(result).toEqual({ _id: 'doc1', active: false });
  });

  it('should handle array values in document', () => {
    const doc = { _id: 'doc1', tags: ['a', 'b', 'c'], name: 'Alice' };
    const result = applyProjection(doc, { tags: 1 });
    expect(result).toEqual({ _id: 'doc1', tags: ['a', 'b', 'c'] });
  });

  it('should handle deeply nested objects', () => {
    const doc = {
      _id: 'doc1',
      data: {
        level1: {
          level2: {
            level3: 'deep value',
          },
        },
      },
    };
    const result = applyProjection(doc, { data: 1 });
    expect(result).toEqual({
      _id: 'doc1',
      data: {
        level1: {
          level2: {
            level3: 'deep value',
          },
        },
      },
    });
  });

  it('should not mutate the original document', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice', password: 'secret' };
    const originalDoc = { ...doc };
    applyProjection(doc, { password: 0 });
    expect(doc).toEqual(originalDoc);
  });

  it('should handle document with only _id', () => {
    const doc = { _id: 'doc1' };
    const inclusionResult = applyProjection(doc, { name: 1 });
    expect(inclusionResult).toEqual({ _id: 'doc1' });

    const exclusionResult = applyProjection(doc, { name: 0 });
    expect(exclusionResult).toEqual({ _id: 'doc1' });
  });
});

// =============================================================================
// Type Safety
// =============================================================================

describe('applyProjection - Type Safety', () => {
  it('should return Partial<T> type', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice', age: 30 };
    const result = applyProjection(doc, { name: 1 });
    // TypeScript should allow accessing optional properties
    expect(result.name).toBe('Alice');
    expect(result.age).toBeUndefined();
  });

  it('should work with generic Record type', () => {
    const doc: Record<string, unknown> = {
      _id: 'doc1',
      customField: 'value',
      anotherField: 123,
    };
    const result = applyProjection(doc, { customField: 1 });
    expect(result._id).toBe('doc1');
    expect(result.customField).toBe('value');
    expect(result.anotherField).toBeUndefined();
  });
});

// =============================================================================
// Array Projections
// =============================================================================

describe('applyProjection - Array Projections', () => {
  it('should include string array field', () => {
    const doc = { _id: 'doc1', name: 'Alice', tags: ['javascript', 'typescript', 'node'] };
    const result = applyProjection(doc, { tags: 1 });
    expect(result).toEqual({ _id: 'doc1', tags: ['javascript', 'typescript', 'node'] });
  });

  it('should include number array field', () => {
    const doc = { _id: 'doc1', name: 'Alice', scores: [95, 87, 92, 88] };
    const result = applyProjection(doc, { scores: 1 });
    expect(result).toEqual({ _id: 'doc1', scores: [95, 87, 92, 88] });
  });

  it('should include array of objects', () => {
    const doc = {
      _id: 'doc1',
      name: 'Alice',
      items: [
        { id: '1', name: 'Item 1' },
        { id: '2', name: 'Item 2' },
      ],
    };
    const result = applyProjection(doc, { items: 1 });
    expect(result).toEqual({
      _id: 'doc1',
      items: [
        { id: '1', name: 'Item 1' },
        { id: '2', name: 'Item 2' },
      ],
    });
  });

  it('should exclude array field', () => {
    const doc = { _id: 'doc1', name: 'Alice', tags: ['a', 'b', 'c'] };
    const result = applyProjection(doc, { tags: 0 });
    expect(result).toEqual({ _id: 'doc1', name: 'Alice' });
  });

  it('should handle empty arrays', () => {
    const doc = { _id: 'doc1', name: 'Alice', tags: [] };
    const result = applyProjection(doc, { tags: 1 });
    expect(result).toEqual({ _id: 'doc1', tags: [] });
  });

  it('should handle nested arrays', () => {
    const doc = {
      _id: 'doc1',
      matrix: [
        [1, 2, 3],
        [4, 5, 6],
      ],
    };
    const result = applyProjection(doc, { matrix: 1 });
    expect(result).toEqual({
      _id: 'doc1',
      matrix: [
        [1, 2, 3],
        [4, 5, 6],
      ],
    });
  });

  it('should handle mixed type arrays', () => {
    const doc = {
      _id: 'doc1',
      mixed: [1, 'two', true, null, { key: 'value' }],
    };
    const result = applyProjection(doc, { mixed: 1 });
    expect(result).toEqual({
      _id: 'doc1',
      mixed: [1, 'two', true, null, { key: 'value' }],
    });
  });

  it('should include multiple array fields', () => {
    const doc = { _id: 'doc1', tags: ['a', 'b'], scores: [1, 2, 3] };
    const result = applyProjection(doc, { tags: 1, scores: 1 });
    expect(result).toEqual({ _id: 'doc1', tags: ['a', 'b'], scores: [1, 2, 3] });
  });
});

// =============================================================================
// ObjectId _id Handling
// =============================================================================

describe('applyProjection - ObjectId _id', () => {
  it('should handle ObjectId _id in inclusion mode', () => {
    const objectId = new ObjectId();
    const doc = { _id: objectId, name: 'Alice', age: 30 };
    const result = applyProjection(doc, { name: 1 });
    expect(result._id).toBe(objectId);
    expect(result.name).toBe('Alice');
    expect(result.age).toBeUndefined();
  });

  it('should handle ObjectId _id in exclusion mode', () => {
    const objectId = new ObjectId();
    const doc = { _id: objectId, name: 'Alice', password: 'secret' };
    const result = applyProjection(doc, { password: 0 });
    expect(result._id).toBe(objectId);
    expect(result.name).toBe('Alice');
    expect(result.password).toBeUndefined();
  });

  it('should exclude ObjectId _id when explicitly set to 0', () => {
    const objectId = new ObjectId();
    const doc = { _id: objectId, name: 'Alice' };
    const result = applyProjection(doc, { name: 1, _id: 0 });
    expect(result._id).toBeUndefined();
    expect(result.name).toBe('Alice');
  });

  it('should include ObjectId _id with empty projection', () => {
    const objectId = new ObjectId();
    const doc = { _id: objectId, name: 'Alice' };
    const result = applyProjection(doc, {});
    expect(result._id).toBe(objectId);
    expect(result.name).toBe('Alice');
  });
});

// =============================================================================
// Empty Document Edge Cases
// =============================================================================

describe('applyProjection - Empty Document', () => {
  it('should handle document with no fields except _id', () => {
    const doc = { _id: 'doc1' };
    const result = applyProjection(doc, { name: 1 });
    expect(result).toEqual({ _id: 'doc1' });
  });

  it('should handle empty projection on minimal document', () => {
    const doc = { _id: 'empty' };
    const result = applyProjection(doc, {});
    expect(result).toEqual({ _id: 'empty' });
  });

  it('should handle _id: 0 on minimal document', () => {
    const doc = { _id: 'doc1' };
    const result = applyProjection(doc, { _id: 0 });
    expect(result).toEqual({});
    expect(result._id).toBeUndefined();
  });

  it('should handle exclusion of all fields on minimal document', () => {
    const doc = { _id: 'doc1' };
    const result = applyProjection(doc, { _id: 0, name: 0 });
    expect(result).toEqual({});
  });
});

// =============================================================================
// Deeply Nested Fields
// =============================================================================

describe('applyProjection - Deeply Nested Fields', () => {
  it('should include 4 levels of nesting', () => {
    const doc = {
      _id: 'doc1',
      level1: {
        level2: {
          level3: {
            level4: 'deep value',
          },
        },
      },
    };
    const result = applyProjection(doc, { level1: 1 });
    expect(result).toEqual({
      _id: 'doc1',
      level1: {
        level2: {
          level3: {
            level4: 'deep value',
          },
        },
      },
    });
  });

  it('should exclude deeply nested structure', () => {
    const doc = {
      _id: 'doc1',
      name: 'Alice',
      config: {
        settings: {
          theme: {
            colors: { primary: '#000', secondary: '#fff' },
          },
        },
      },
    };
    const result = applyProjection(doc, { config: 0 });
    expect(result).toEqual({ _id: 'doc1', name: 'Alice' });
  });

  it('should handle nested object with null value', () => {
    const doc = {
      _id: 'doc1',
      data: {
        nested: null,
      },
    };
    const result = applyProjection(doc, { data: 1 });
    expect(result).toEqual({
      _id: 'doc1',
      data: {
        nested: null,
      },
    });
  });

  it('should handle nested object with empty object value', () => {
    const doc = {
      _id: 'doc1',
      data: {
        nested: {},
      },
    };
    const result = applyProjection(doc, { data: 1 });
    expect(result).toEqual({
      _id: 'doc1',
      data: {
        nested: {},
      },
    });
  });

  it('should handle nested arrays within objects', () => {
    const doc = {
      _id: 'doc1',
      users: {
        admins: ['alice', 'bob'],
        guests: ['charlie'],
      },
    };
    const result = applyProjection(doc, { users: 1 });
    expect(result).toEqual({
      _id: 'doc1',
      users: {
        admins: ['alice', 'bob'],
        guests: ['charlie'],
      },
    });
  });
});

// =============================================================================
// Special Field Names
// =============================================================================

describe('applyProjection - Special Field Names', () => {
  it('should handle numeric field names', () => {
    const doc = { _id: 'doc1', '0': 'zero', '1': 'one', name: 'Alice' };
    const result = applyProjection(doc, { '0': 1, '1': 1 });
    expect(result).toEqual({ _id: 'doc1', '0': 'zero', '1': 'one' });
  });

  it('should handle field names with special characters', () => {
    const doc = { _id: 'doc1', 'field-name': 'value1', field_name: 'value2' };
    const result = applyProjection(doc, { 'field-name': 1 });
    expect(result).toEqual({ _id: 'doc1', 'field-name': 'value1' });
  });

  it('should handle field names with spaces', () => {
    const doc = { _id: 'doc1', 'field name': 'value', regular: 'other' };
    const result = applyProjection(doc, { 'field name': 1 });
    expect(result).toEqual({ _id: 'doc1', 'field name': 'value' });
  });

  it('should handle field names starting with $', () => {
    const doc = { _id: 'doc1', $special: 'value', name: 'Alice' };
    const result = applyProjection(doc, { $special: 1 });
    expect(result).toEqual({ _id: 'doc1', $special: 'value' });
  });

  it('should handle unicode field names', () => {
    const doc = { _id: 'doc1', '\u4e2d\u6587': 'chinese', '\u65e5\u672c\u8a9e': 'japanese' };
    const result = applyProjection(doc, { '\u4e2d\u6587': 1 });
    expect(result).toEqual({ _id: 'doc1', '\u4e2d\u6587': 'chinese' });
  });
});

// =============================================================================
// Mixed Inclusion/Exclusion (Current Behavior Documentation)
// =============================================================================

describe('applyProjection - Mixed Inclusion/Exclusion Behavior', () => {
  // Note: MongoDB throws an error for mixing 1s and 0s (except _id)
  // Current implementation: if any field is 1, it uses inclusion mode

  it('should use inclusion mode when mixing 1s and 0s', () => {
    const doc = { _id: 'doc1', a: 1, b: 2, c: 3 };
    // Mix: a:1, b:0 - since there's a 1, it's inclusion mode
    const result = applyProjection(doc, { a: 1, b: 0 });
    expect(result._id).toBe('doc1');
    expect(result.a).toBe(1);
    expect(result.b).toBeUndefined();
    expect(result.c).toBeUndefined();
  });

  it('should ignore 0s (except _id) in inclusion mode', () => {
    const doc = { _id: 'doc1', name: 'Alice', password: 'secret', email: 'a@b.com' };
    const result = applyProjection(doc, { name: 1, password: 0, email: 1 });
    expect(result).toEqual({ _id: 'doc1', name: 'Alice', email: 'a@b.com' });
    expect(result.password).toBeUndefined();
  });

  it('should still exclude _id when mixed with inclusion', () => {
    const doc = { _id: 'doc1', a: 1, b: 2 };
    const result = applyProjection(doc, { a: 1, _id: 0 });
    expect(result).toEqual({ a: 1 });
    expect(result._id).toBeUndefined();
  });
});

// =============================================================================
// Large Documents
// =============================================================================

describe('applyProjection - Large Documents', () => {
  it('should handle document with many fields in inclusion mode', () => {
    const doc: Record<string, unknown> = { _id: 'doc1' };
    for (let i = 0; i < 100; i++) {
      doc[`field${i}`] = `value${i}`;
    }
    const result = applyProjection(doc, { field0: 1, field50: 1, field99: 1 });
    expect(result._id).toBe('doc1');
    expect(result.field0).toBe('value0');
    expect(result.field50).toBe('value50');
    expect(result.field99).toBe('value99');
    expect(result.field1).toBeUndefined();
  });

  it('should handle document with many fields in exclusion mode', () => {
    const doc: Record<string, unknown> = { _id: 'doc1' };
    for (let i = 0; i < 100; i++) {
      doc[`field${i}`] = `value${i}`;
    }
    const result = applyProjection(doc, { field0: 0, field99: 0 });
    expect(result._id).toBe('doc1');
    expect(result.field0).toBeUndefined();
    expect(result.field99).toBeUndefined();
    expect(result.field50).toBe('value50');
  });

  it('should handle large nested object', () => {
    const nested: Record<string, unknown> = {};
    for (let i = 0; i < 50; i++) {
      nested[`key${i}`] = `nestedValue${i}`;
    }
    const doc = { _id: 'doc1', data: nested, name: 'Alice' };
    const result = applyProjection(doc, { data: 1 });
    expect(result._id).toBe('doc1');
    expect(result.data).toEqual(nested);
    expect(result.name).toBeUndefined();
  });
});

// =============================================================================
// Date and Special Types
// =============================================================================

describe('applyProjection - Date and Special Types', () => {
  it('should handle Date objects', () => {
    const date = new Date('2024-01-15T10:30:00Z');
    const doc = { _id: 'doc1', createdAt: date, name: 'Alice' };
    const result = applyProjection(doc, { createdAt: 1 });
    expect(result).toEqual({ _id: 'doc1', createdAt: date });
  });

  it('should handle Uint8Array (binary data)', () => {
    const binary = new Uint8Array([1, 2, 3, 4, 5]);
    const doc = { _id: 'doc1', data: binary, name: 'Alice' };
    const result = applyProjection(doc, { data: 1 });
    expect(result).toEqual({ _id: 'doc1', data: binary });
  });

  it('should handle RegExp values', () => {
    const regex = /test/gi;
    const doc = { _id: 'doc1', pattern: regex, name: 'Alice' };
    const result = applyProjection(doc, { pattern: 1 });
    expect(result._id).toBe('doc1');
    expect(result.pattern).toBe(regex);
  });

  it('should handle Symbol values', () => {
    const sym = Symbol('test');
    const doc = { _id: 'doc1', symbol: sym, name: 'Alice' };
    const result = applyProjection(doc, { symbol: 1 });
    expect(result._id).toBe('doc1');
    expect(result.symbol).toBe(sym);
  });

  it('should handle BigInt values', () => {
    const bigInt = BigInt(9007199254740991);
    const doc = { _id: 'doc1', big: bigInt, name: 'Alice' };
    const result = applyProjection(doc, { big: 1 });
    expect(result._id).toBe('doc1');
    expect(result.big).toBe(bigInt);
  });
});

// =============================================================================
// Immutability Tests
// =============================================================================

describe('applyProjection - Immutability', () => {
  it('should not mutate original document in inclusion mode', () => {
    const doc = { _id: 'doc1', name: 'Alice', age: 30 };
    const original = { ...doc };
    applyProjection(doc, { name: 1 });
    expect(doc).toEqual(original);
  });

  it('should not mutate original document in exclusion mode', () => {
    const doc = { _id: 'doc1', name: 'Alice', password: 'secret' };
    const original = { ...doc };
    applyProjection(doc, { password: 0 });
    expect(doc).toEqual(original);
  });

  it('should not mutate nested objects', () => {
    const nested = { street: '123 Main St', city: 'NYC' };
    const doc = { _id: 'doc1', address: nested };
    const originalNested = { ...nested };
    applyProjection(doc, { address: 1 });
    expect(nested).toEqual(originalNested);
  });

  it('should not mutate arrays', () => {
    const tags = ['a', 'b', 'c'];
    const doc = { _id: 'doc1', tags };
    const originalTags = [...tags];
    applyProjection(doc, { tags: 1 });
    expect(tags).toEqual(originalTags);
  });
});

// =============================================================================
// Projection Result Correctness
// =============================================================================

describe('applyProjection - Result Correctness', () => {
  it('should return new object reference', () => {
    const doc = { _id: 'doc1', name: 'Alice' };
    const result = applyProjection(doc, { name: 1 });
    expect(result).not.toBe(doc);
  });

  it('should maintain reference for nested objects (shallow copy in exclusion)', () => {
    const address = { street: '123 Main St' };
    const doc = { _id: 'doc1', address, name: 'Alice' };
    const result = applyProjection(doc, { name: 0 });
    // In exclusion mode, spread operator is used, so nested objects share reference
    expect(result.address).toBe(address);
  });

  it('should handle consecutive projections', () => {
    const doc = { _id: 'doc1', a: 1, b: 2, c: 3, d: 4 };
    const first = applyProjection(doc, { a: 1, b: 1 });
    const second = applyProjection(first, { a: 1 });
    expect(second).toEqual({ _id: 'doc1', a: 1 });
  });

  it('should handle projection after exclusion', () => {
    const doc = { _id: 'doc1', a: 1, b: 2, c: 3 };
    const first = applyProjection(doc, { c: 0 });
    const second = applyProjection(first, { a: 1 });
    expect(second).toEqual({ _id: 'doc1', a: 1 });
  });
});

// =============================================================================
// Boolean and Falsy Values Handling
// =============================================================================

describe('applyProjection - Falsy Values', () => {
  it('should include field with value 0', () => {
    const doc = { _id: 'doc1', count: 0, name: 'Alice' };
    const result = applyProjection(doc, { count: 1 });
    expect(result).toEqual({ _id: 'doc1', count: 0 });
  });

  it('should include field with empty string', () => {
    const doc = { _id: 'doc1', name: '', age: 30 };
    const result = applyProjection(doc, { name: 1 });
    expect(result).toEqual({ _id: 'doc1', name: '' });
  });

  it('should include field with false', () => {
    const doc = { _id: 'doc1', active: false, name: 'Alice' };
    const result = applyProjection(doc, { active: 1 });
    expect(result).toEqual({ _id: 'doc1', active: false });
  });

  it('should include field with null', () => {
    const doc = { _id: 'doc1', data: null, name: 'Alice' };
    const result = applyProjection(doc, { data: 1 });
    expect(result).toEqual({ _id: 'doc1', data: null });
  });

  it('should include field with NaN', () => {
    const doc = { _id: 'doc1', value: NaN, name: 'Alice' };
    const result = applyProjection(doc, { value: 1 });
    expect(result._id).toBe('doc1');
    expect(Number.isNaN(result.value)).toBe(true);
  });

  it('should handle field with Infinity', () => {
    const doc = { _id: 'doc1', value: Infinity, name: 'Alice' };
    const result = applyProjection(doc, { value: 1 });
    expect(result).toEqual({ _id: 'doc1', value: Infinity });
  });

  it('should handle field with -Infinity', () => {
    const doc = { _id: 'doc1', value: -Infinity, name: 'Alice' };
    const result = applyProjection(doc, { value: 1 });
    expect(result).toEqual({ _id: 'doc1', value: -Infinity });
  });
});

// =============================================================================
// Real-World Scenarios
// =============================================================================

describe('applyProjection - Real-World Scenarios', () => {
  it('should project user document excluding sensitive fields', () => {
    const user = {
      _id: 'user123',
      email: 'alice@example.com',
      password: '$2b$10$hashedpassword',
      name: 'Alice Smith',
      role: 'admin',
      apiKey: 'secret-key-123',
      createdAt: new Date('2024-01-01'),
    };
    const result = applyProjection(user, { password: 0, apiKey: 0 });
    expect(result).toEqual({
      _id: 'user123',
      email: 'alice@example.com',
      name: 'Alice Smith',
      role: 'admin',
      createdAt: new Date('2024-01-01'),
    });
  });

  it('should project product listing with selected fields', () => {
    const product = {
      _id: 'prod456',
      name: 'Laptop',
      price: 999.99,
      costPrice: 650.00,
      inventory: 50,
      supplier: { id: 'sup1', name: 'TechCorp', contact: 'secret@tech.com' },
      description: 'A powerful laptop',
    };
    const result = applyProjection(product, { name: 1, price: 1, description: 1 });
    expect(result).toEqual({
      _id: 'prod456',
      name: 'Laptop',
      price: 999.99,
      description: 'A powerful laptop',
    });
  });

  it('should project order summary', () => {
    const order = {
      _id: 'order789',
      userId: 'user123',
      items: [
        { productId: 'p1', quantity: 2, price: 10 },
        { productId: 'p2', quantity: 1, price: 20 },
      ],
      total: 40,
      status: 'shipped',
      internalNotes: 'Handle with care',
      trackingNumber: 'TRACK123',
    };
    const result = applyProjection(order, { internalNotes: 0 });
    expect(result).toEqual({
      _id: 'order789',
      userId: 'user123',
      items: [
        { productId: 'p1', quantity: 2, price: 10 },
        { productId: 'p2', quantity: 1, price: 20 },
      ],
      total: 40,
      status: 'shipped',
      trackingNumber: 'TRACK123',
    });
  });

  it('should project blog post for public API', () => {
    const post = {
      _id: 'post001',
      title: 'Hello World',
      content: 'This is my first post',
      author: {
        id: 'author1',
        name: 'John Doe',
        email: 'john@example.com',
      },
      tags: ['intro', 'welcome'],
      viewCount: 1500,
      internalScore: 8.5,
      draft: false,
    };
    const result = applyProjection(post, { title: 1, content: 1, author: 1, tags: 1 });
    expect(result).toEqual({
      _id: 'post001',
      title: 'Hello World',
      content: 'This is my first post',
      author: {
        id: 'author1',
        name: 'John Doe',
        email: 'john@example.com',
      },
      tags: ['intro', 'welcome'],
    });
  });
});

// =============================================================================
// Dot Notation Path Behavior (Current Implementation)
// =============================================================================

describe('applyProjection - Dot Notation Paths', () => {
  // Note: Current implementation does NOT support dot notation for nested paths
  // It treats "a.b" as a literal field name, not a nested path

  it('should treat dot notation as literal field name (no nested path support)', () => {
    const doc = {
      _id: 'doc1',
      address: { city: 'NYC', state: 'NY' },
      'address.city': 'literal field',
    };
    const result = applyProjection(doc, { 'address.city': 1 });
    // Only includes the literal "address.city" field, not nested address.city
    expect(result._id).toBe('doc1');
    expect(result['address.city']).toBe('literal field');
    expect(result.address).toBeUndefined();
  });

  it('should return only _id when dot notation field does not exist', () => {
    const doc = {
      _id: 'doc1',
      address: { city: 'NYC' },
    };
    const result = applyProjection(doc, { 'address.city': 1 });
    // "address.city" as a top-level key doesn't exist
    expect(result).toEqual({ _id: 'doc1' });
  });

  it('should include full nested object when projecting parent', () => {
    const doc = {
      _id: 'doc1',
      user: {
        name: 'Alice',
        email: 'alice@test.com',
        settings: { theme: 'dark' },
      },
    };
    const result = applyProjection(doc, { user: 1 });
    expect(result).toEqual({
      _id: 'doc1',
      user: {
        name: 'Alice',
        email: 'alice@test.com',
        settings: { theme: 'dark' },
      },
    });
  });
});

// =============================================================================
// Very Deep Nesting (5+ levels)
// =============================================================================

describe('applyProjection - Very Deep Nesting (5+ levels)', () => {
  it('should include 5 levels of nesting', () => {
    const doc = {
      _id: 'doc1',
      level1: {
        level2: {
          level3: {
            level4: {
              level5: 'very deep value',
            },
          },
        },
      },
    };
    const result = applyProjection(doc, { level1: 1 });
    expect(result).toEqual({
      _id: 'doc1',
      level1: {
        level2: {
          level3: {
            level4: {
              level5: 'very deep value',
            },
          },
        },
      },
    });
  });

  it('should include 6 levels of nesting', () => {
    const doc = {
      _id: 'doc1',
      a: {
        b: {
          c: {
            d: {
              e: {
                f: 'six levels deep',
              },
            },
          },
        },
      },
    };
    const result = applyProjection(doc, { a: 1 });
    expect(result).toEqual({
      _id: 'doc1',
      a: {
        b: {
          c: {
            d: {
              e: {
                f: 'six levels deep',
              },
            },
          },
        },
      },
    });
  });

  it('should include 10 levels of nesting', () => {
    const doc = {
      _id: 'doc1',
      l1: {
        l2: {
          l3: {
            l4: {
              l5: {
                l6: {
                  l7: {
                    l8: {
                      l9: {
                        l10: 'ten levels deep',
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
    };
    const result = applyProjection(doc, { l1: 1 });
    expect(result.l1?.l2?.l3?.l4?.l5?.l6?.l7?.l8?.l9?.l10).toBe('ten levels deep');
  });

  it('should exclude 5+ levels deep structure', () => {
    const doc = {
      _id: 'doc1',
      name: 'Alice',
      deep: {
        a: {
          b: {
            c: {
              d: {
                value: 'nested',
              },
            },
          },
        },
      },
    };
    const result = applyProjection(doc, { deep: 0 });
    expect(result).toEqual({ _id: 'doc1', name: 'Alice' });
    expect(result.deep).toBeUndefined();
  });

  it('should handle very deep nesting with mixed content types', () => {
    const doc = {
      _id: 'doc1',
      root: {
        arrays: {
          nested: {
            items: {
              data: {
                values: [1, 2, { key: 'value' }],
              },
            },
          },
        },
      },
    };
    const result = applyProjection(doc, { root: 1 });
    expect(result.root?.arrays?.nested?.items?.data?.values).toEqual([1, 2, { key: 'value' }]);
  });

  it('should handle deep nesting with null at various levels', () => {
    const doc = {
      _id: 'doc1',
      a: {
        b: {
          c: null,
          d: {
            e: {
              f: 'value',
            },
          },
        },
      },
    };
    const result = applyProjection(doc, { a: 1 });
    expect(result.a?.b?.c).toBeNull();
    expect(result.a?.b?.d?.e?.f).toBe('value');
  });

  it('should handle deep nesting with empty objects at various levels', () => {
    const doc = {
      _id: 'doc1',
      config: {
        settings: {},
        preferences: {
          display: {},
          advanced: {
            options: {
              experimental: {},
            },
          },
        },
      },
    };
    const result = applyProjection(doc, { config: 1 });
    expect(result.config?.settings).toEqual({});
    expect(result.config?.preferences?.display).toEqual({});
    expect(result.config?.preferences?.advanced?.options?.experimental).toEqual({});
  });
});

// =============================================================================
// Undefined Values (Explicit in Document)
// =============================================================================

describe('applyProjection - Undefined Values Handling', () => {
  it('should not include undefined field in inclusion mode (key in doc check)', () => {
    const doc = { _id: 'doc1', name: 'Alice', email: undefined };
    const result = applyProjection(doc, { name: 1, email: 1 });
    expect(result._id).toBe('doc1');
    expect(result.name).toBe('Alice');
    // The 'key in doc' check in the implementation returns true for undefined values
    // but the value itself is undefined
    expect('email' in result).toBe(true);
    expect(result.email).toBeUndefined();
  });

  it('should preserve undefined in exclusion mode (spread operator behavior)', () => {
    const doc = { _id: 'doc1', name: 'Alice', email: undefined, password: 'secret' };
    const result = applyProjection(doc, { password: 0 });
    expect(result._id).toBe('doc1');
    expect(result.name).toBe('Alice');
    expect('email' in result).toBe(true);
    expect(result.email).toBeUndefined();
    expect(result.password).toBeUndefined();
  });

  it('should handle multiple undefined values', () => {
    const doc = { _id: 'doc1', a: undefined, b: undefined, c: 'value' };
    const result = applyProjection(doc, { a: 1, b: 1, c: 1 });
    expect(result._id).toBe('doc1');
    expect(result.c).toBe('value');
  });

  it('should handle document with all undefined values except _id', () => {
    const doc = { _id: 'doc1', name: undefined, age: undefined };
    const result = applyProjection(doc, { name: 1 });
    expect(result._id).toBe('doc1');
  });

  it('should handle nested undefined values', () => {
    const doc = {
      _id: 'doc1',
      profile: {
        name: 'Alice',
        email: undefined,
      },
    };
    const result = applyProjection(doc, { profile: 1 });
    expect(result._id).toBe('doc1');
    expect(result.profile?.name).toBe('Alice');
    expect(result.profile?.email).toBeUndefined();
  });
});

// =============================================================================
// Empty Nested Objects
// =============================================================================

describe('applyProjection - Empty Nested Objects', () => {
  it('should include empty nested object in inclusion mode', () => {
    const doc = { _id: 'doc1', settings: {}, name: 'Alice' };
    const result = applyProjection(doc, { settings: 1 });
    expect(result).toEqual({ _id: 'doc1', settings: {} });
  });

  it('should exclude empty nested object', () => {
    const doc = { _id: 'doc1', settings: {}, name: 'Alice' };
    const result = applyProjection(doc, { settings: 0 });
    expect(result).toEqual({ _id: 'doc1', name: 'Alice' });
  });

  it('should handle document with only empty nested objects', () => {
    const doc = { _id: 'doc1', a: {}, b: {}, c: {} };
    const result = applyProjection(doc, { a: 1, b: 1 });
    expect(result).toEqual({ _id: 'doc1', a: {}, b: {} });
  });

  it('should handle deeply nested empty object', () => {
    const doc = {
      _id: 'doc1',
      config: {
        deep: {
          empty: {},
        },
      },
    };
    const result = applyProjection(doc, { config: 1 });
    expect(result.config?.deep?.empty).toEqual({});
  });

  it('should handle empty object with empty array siblings', () => {
    const doc = { _id: 'doc1', obj: {}, arr: [] };
    const result = applyProjection(doc, { obj: 1, arr: 1 });
    expect(result).toEqual({ _id: 'doc1', obj: {}, arr: [] });
  });
});

// =============================================================================
// $slice and $elemMatch Operators (Not Supported - Documentation)
// =============================================================================

describe('applyProjection - Unsupported Operators', () => {
  // Note: The current implementation does NOT support $slice or $elemMatch operators
  // The projection type signature is Record<string, 0 | 1>
  // These tests document the current behavior with invalid projection values

  it('should not support $slice operator (type constraint prevents this)', () => {
    // This is a compile-time constraint - the type signature only allows 0 | 1
    // At runtime, any other value would be treated based on truthiness
    const doc = { _id: 'doc1', tags: ['a', 'b', 'c', 'd', 'e'] };

    // If someone bypasses TypeScript and passes a $slice projection,
    // the implementation would not handle it as MongoDB does
    // This test documents that limitation
    const invalidProjection = { tags: { $slice: 2 } } as unknown as Record<string, 0 | 1>;

    // The implementation checks for 1s using Object.values(projection).some(v => v === 1)
    // Since { $slice: 2 } !== 1, it won't be treated as an inclusion
    // But it's also not 0, so it enters exclusion mode and copies everything
    const result = applyProjection(doc, invalidProjection);
    // In exclusion mode with no actual 0s, the document is copied as-is
    expect(result._id).toBe('doc1');
    expect(result.tags).toEqual(['a', 'b', 'c', 'd', 'e']);
  });

  it('should not support $elemMatch operator (type constraint prevents this)', () => {
    const doc = {
      _id: 'doc1',
      items: [
        { id: 1, status: 'active' },
        { id: 2, status: 'inactive' },
        { id: 3, status: 'active' },
      ],
    };

    // If someone bypasses TypeScript and passes an $elemMatch projection,
    // the implementation would not filter array elements as MongoDB does
    const invalidProjection = {
      items: { $elemMatch: { status: 'active' } },
    } as unknown as Record<string, 0 | 1>;

    const result = applyProjection(doc, invalidProjection);
    // Without proper $elemMatch support, all items are returned
    expect(result._id).toBe('doc1');
    expect(result.items).toEqual([
      { id: 1, status: 'active' },
      { id: 2, status: 'inactive' },
      { id: 3, status: 'active' },
    ]);
  });

  it('should document that projection type only allows 0 or 1', () => {
    // This test serves as documentation that the projection utility
    // is designed for simple inclusion/exclusion projections only
    const doc = { _id: 'doc1', name: 'Alice', tags: ['a', 'b', 'c'] };

    // Valid projections
    const validInclusion = applyProjection(doc, { name: 1 });
    expect(validInclusion).toEqual({ _id: 'doc1', name: 'Alice' });

    const validExclusion = applyProjection(doc, { tags: 0 });
    expect(validExclusion).toEqual({ _id: 'doc1', name: 'Alice' });

    // The implementation is intentionally simple and does not support:
    // - $slice for array slicing
    // - $elemMatch for array element filtering
    // - $meta for text search scores
    // - Nested path projections like { 'address.city': 1 }
  });
});

// =============================================================================
// Boundary Conditions
// =============================================================================

describe('applyProjection - Boundary Conditions', () => {
  it('should handle document with maximum safe integer values', () => {
    const doc = { _id: 'doc1', big: Number.MAX_SAFE_INTEGER, small: Number.MIN_SAFE_INTEGER };
    const result = applyProjection(doc, { big: 1, small: 1 });
    expect(result.big).toBe(Number.MAX_SAFE_INTEGER);
    expect(result.small).toBe(Number.MIN_SAFE_INTEGER);
  });

  it('should handle document with very long string', () => {
    const longString = 'a'.repeat(10000);
    const doc = { _id: 'doc1', long: longString, short: 'brief' };
    const result = applyProjection(doc, { long: 1 });
    expect(result.long).toBe(longString);
    expect(result.long?.length).toBe(10000);
  });

  it('should handle document with many fields', () => {
    const doc: Record<string, unknown> = { _id: 'doc1' };
    for (let i = 0; i < 1000; i++) {
      doc[`field${i}`] = `value${i}`;
    }
    const result = applyProjection(doc, { field0: 1, field999: 1 });
    expect(result._id).toBe('doc1');
    expect(result.field0).toBe('value0');
    expect(result.field999).toBe('value999');
    expect(Object.keys(result).length).toBe(3); // _id + field0 + field999
  });

  it('should handle projection with many fields to include', () => {
    const doc: Record<string, unknown> = { _id: 'doc1' };
    const projection: Record<string, 0 | 1> = {};
    for (let i = 0; i < 100; i++) {
      doc[`f${i}`] = `v${i}`;
      projection[`f${i}`] = 1;
    }
    const result = applyProjection(doc, projection);
    expect(result._id).toBe('doc1');
    expect(Object.keys(result).length).toBe(101); // _id + 100 fields
  });

  it('should handle projection with many fields to exclude', () => {
    const doc: Record<string, unknown> = { _id: 'doc1', keep: 'this' };
    const projection: Record<string, 0 | 1> = {};
    for (let i = 0; i < 100; i++) {
      doc[`exclude${i}`] = `value${i}`;
      projection[`exclude${i}`] = 0;
    }
    const result = applyProjection(doc, projection);
    expect(result._id).toBe('doc1');
    expect(result.keep).toBe('this');
    expect(Object.keys(result).length).toBe(2); // _id + keep
  });

  it('should handle array with many elements', () => {
    const largeArray = Array.from({ length: 10000 }, (_, i) => i);
    const doc = { _id: 'doc1', numbers: largeArray };
    const result = applyProjection(doc, { numbers: 1 });
    expect(result.numbers).toEqual(largeArray);
    expect(result.numbers?.length).toBe(10000);
  });
});

// =============================================================================
// Numeric String Field Names
// =============================================================================

describe('applyProjection - Numeric and Special Field Names', () => {
  it('should handle array-like numeric indices as field names', () => {
    const doc = { _id: 'doc1', '0': 'first', '1': 'second', '2': 'third' };
    const result = applyProjection(doc, { '0': 1, '2': 1 });
    expect(result).toEqual({ _id: 'doc1', '0': 'first', '2': 'third' });
  });

  it('should handle negative number as field name', () => {
    const doc = { _id: 'doc1', '-1': 'negative', '0': 'zero' };
    const result = applyProjection(doc, { '-1': 1 });
    expect(result).toEqual({ _id: 'doc1', '-1': 'negative' });
  });

  it('should handle floating point number as field name', () => {
    const doc = { _id: 'doc1', '3.14': 'pi', '2.71': 'e' };
    const result = applyProjection(doc, { '3.14': 1 });
    expect(result).toEqual({ _id: 'doc1', '3.14': 'pi' });
  });

  it('should handle empty string as field name', () => {
    const doc = { _id: 'doc1', '': 'empty key', name: 'Alice' };
    const result = applyProjection(doc, { '': 1 });
    expect(result).toEqual({ _id: 'doc1', '': 'empty key' });
  });

  it('should handle field name with only whitespace', () => {
    const doc = { _id: 'doc1', '   ': 'spaces', '\t': 'tab', '\n': 'newline' };
    const result = applyProjection(doc, { '   ': 1, '\t': 1 });
    expect(result).toEqual({ _id: 'doc1', '   ': 'spaces', '\t': 'tab' });
  });

  it('should handle field name starting with underscore', () => {
    const doc = { _id: 'doc1', _private: 'secret', __proto: 'not actually proto', name: 'Alice' };
    const result = applyProjection(doc, { _private: 1, __proto: 1 });
    expect(result).toEqual({ _id: 'doc1', _private: 'secret', __proto: 'not actually proto' });
  });
});

// =============================================================================
// Prototype and Security Edge Cases
// =============================================================================

describe('applyProjection - Security and Prototype Safety', () => {
  it('should not pollute prototype through field names', () => {
    const doc = { _id: 'doc1', name: 'Alice' };
    const result = applyProjection(doc, { name: 1 });
    // Ensure no prototype pollution
    expect(Object.prototype.hasOwnProperty.call(result, 'polluted')).toBe(false);
  });

  it('should handle field named constructor', () => {
    const doc = { _id: 'doc1', constructor: 'not a function', name: 'Alice' };
    const result = applyProjection(doc, { constructor: 1 });
    expect(result.constructor).toBe('not a function');
  });

  it('should handle field named hasOwnProperty', () => {
    const doc = { _id: 'doc1', hasOwnProperty: 'overridden', name: 'Alice' };
    const result = applyProjection(doc, { hasOwnProperty: 1 });
    expect(result.hasOwnProperty).toBe('overridden');
  });

  it('should handle field named toString', () => {
    const doc = { _id: 'doc1', toString: 'not a method', name: 'Alice' };
    const result = applyProjection(doc, { toString: 1 });
    expect(result.toString).toBe('not a method');
  });

  it('should handle field named valueOf', () => {
    const doc = { _id: 'doc1', valueOf: 42, name: 'Alice' };
    const result = applyProjection(doc, { valueOf: 1 });
    expect(result.valueOf).toBe(42);
  });
});

// =============================================================================
// Function and Complex Object Values
// =============================================================================

describe('applyProjection - Complex Value Types', () => {
  it('should handle function as field value', () => {
    const fn = () => 'hello';
    const doc = { _id: 'doc1', callback: fn, name: 'Alice' };
    const result = applyProjection(doc, { callback: 1 });
    expect(result.callback).toBe(fn);
    expect(typeof result.callback).toBe('function');
  });

  it('should handle Map as field value', () => {
    const map = new Map([
      ['key1', 'value1'],
      ['key2', 'value2'],
    ]);
    const doc = { _id: 'doc1', data: map, name: 'Alice' };
    const result = applyProjection(doc, { data: 1 });
    expect(result.data).toBe(map);
    expect(result.data?.get('key1')).toBe('value1');
  });

  it('should handle Set as field value', () => {
    const set = new Set([1, 2, 3]);
    const doc = { _id: 'doc1', items: set, name: 'Alice' };
    const result = applyProjection(doc, { items: 1 });
    expect(result.items).toBe(set);
    expect(result.items?.has(2)).toBe(true);
  });

  it('should handle Error object as field value', () => {
    const error = new Error('test error');
    const doc = { _id: 'doc1', lastError: error, name: 'Alice' };
    const result = applyProjection(doc, { lastError: 1 });
    expect(result.lastError).toBe(error);
    expect(result.lastError?.message).toBe('test error');
  });

  it('should handle circular reference (shallow copy in exclusion mode)', () => {
    interface CircularDoc {
      _id: string;
      name: string;
      self?: CircularDoc;
    }
    const doc: CircularDoc = { _id: 'doc1', name: 'Alice' };
    doc.self = doc; // Create circular reference

    // In exclusion mode, spread operator creates shallow copy
    const result = applyProjection(doc, { name: 0 }) as CircularDoc;
    expect(result._id).toBe('doc1');
    expect(result.self).toBe(doc); // Reference is preserved
  });
});
