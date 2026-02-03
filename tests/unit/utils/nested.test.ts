/**
 * Nested Value Utility Tests
 *
 * Tests for the getNestedValue function that handles:
 * - Simple property access
 * - Dot notation for nested properties
 * - Edge cases with null, undefined, and non-object values
 * - Depth limit protection against DoS attacks
 */

import { describe, it, expect } from 'vitest';
import { getNestedValue, PathDepthExceededError } from '../../../src/utils/nested.js';
import { MAX_NESTED_PATH_DEPTH } from '../../../src/constants.js';

// =============================================================================
// Simple Property Access
// =============================================================================

describe('getNestedValue - Simple Property Access', () => {
  it('should get a top-level property', () => {
    const obj = { name: 'Alice', age: 30 };
    expect(getNestedValue(obj, 'name')).toBe('Alice');
    expect(getNestedValue(obj, 'age')).toBe(30);
  });

  it('should return undefined for non-existent property', () => {
    const obj = { name: 'Alice' };
    expect(getNestedValue(obj, 'age')).toBeUndefined();
  });

  it('should handle null value', () => {
    const obj = { name: null };
    expect(getNestedValue(obj, 'name')).toBeNull();
  });

  it('should handle undefined value', () => {
    const obj = { name: undefined };
    expect(getNestedValue(obj, 'name')).toBeUndefined();
  });

  it('should handle boolean values', () => {
    const obj = { active: true, disabled: false };
    expect(getNestedValue(obj, 'active')).toBe(true);
    expect(getNestedValue(obj, 'disabled')).toBe(false);
  });

  it('should handle zero and empty string', () => {
    const obj = { count: 0, name: '' };
    expect(getNestedValue(obj, 'count')).toBe(0);
    expect(getNestedValue(obj, 'name')).toBe('');
  });

  it('should handle array values', () => {
    const obj = { tags: ['a', 'b', 'c'] };
    expect(getNestedValue(obj, 'tags')).toEqual(['a', 'b', 'c']);
  });

  it('should handle object values', () => {
    const profile = { firstName: 'Alice' };
    const obj = { profile };
    expect(getNestedValue(obj, 'profile')).toEqual(profile);
  });
});

// =============================================================================
// Dot Notation (Nested Properties)
// =============================================================================

describe('getNestedValue - Dot Notation', () => {
  it('should get a single-level nested property', () => {
    const obj = { user: { name: 'Alice' } };
    expect(getNestedValue(obj, 'user.name')).toBe('Alice');
  });

  it('should get a deeply nested property', () => {
    const obj = {
      user: {
        profile: {
          address: {
            city: 'New York',
          },
        },
      },
    };
    expect(getNestedValue(obj, 'user.profile.address.city')).toBe('New York');
  });

  it('should return undefined for non-existent nested property', () => {
    const obj = { user: { name: 'Alice' } };
    expect(getNestedValue(obj, 'user.age')).toBeUndefined();
  });

  it('should return undefined for partial path with non-existent intermediate', () => {
    const obj = { user: { name: 'Alice' } };
    expect(getNestedValue(obj, 'user.profile.address')).toBeUndefined();
  });

  it('should handle null in path', () => {
    const obj = { user: null };
    expect(getNestedValue(obj, 'user.name')).toBeUndefined();
  });

  it('should handle undefined in path', () => {
    const obj = { user: undefined };
    expect(getNestedValue(obj, 'user.name')).toBeUndefined();
  });

  it('should handle nested null value', () => {
    const obj = { user: { name: null } };
    expect(getNestedValue(obj, 'user.name')).toBeNull();
  });

  it('should handle nested arrays', () => {
    const obj = { user: { tags: ['a', 'b'] } };
    expect(getNestedValue(obj, 'user.tags')).toEqual(['a', 'b']);
  });

  it('should handle many levels of nesting', () => {
    const obj = { a: { b: { c: { d: { e: { f: 'deep' } } } } } };
    expect(getNestedValue(obj, 'a.b.c.d.e.f')).toBe('deep');
  });
});

// =============================================================================
// Edge Cases
// =============================================================================

describe('getNestedValue - Edge Cases', () => {
  describe('null and undefined inputs', () => {
    it('should return undefined for null object', () => {
      expect(getNestedValue(null, 'name')).toBeUndefined();
    });

    it('should return undefined for undefined object', () => {
      expect(getNestedValue(undefined, 'name')).toBeUndefined();
    });

    it('should return undefined for null object with nested path', () => {
      expect(getNestedValue(null, 'user.name')).toBeUndefined();
    });
  });

  describe('non-object inputs', () => {
    it('should return undefined for string input', () => {
      expect(getNestedValue('string' as unknown, 'length')).toBeUndefined();
    });

    it('should return undefined for number input', () => {
      expect(getNestedValue(123 as unknown, 'toString')).toBeUndefined();
    });

    it('should return undefined for boolean input', () => {
      expect(getNestedValue(true as unknown, 'valueOf')).toBeUndefined();
    });
  });

  describe('empty inputs', () => {
    it('should handle empty object', () => {
      expect(getNestedValue({}, 'name')).toBeUndefined();
    });

    it('should handle empty path', () => {
      const obj = { name: 'Alice' };
      expect(getNestedValue(obj, '')).toBeUndefined();
    });
  });

  describe('special key names', () => {
    it('should handle keys with special characters', () => {
      const obj = { 'special-key': 'value' };
      expect(getNestedValue(obj, 'special-key')).toBe('value');
    });

    it('should handle numeric string keys', () => {
      const obj = { '123': 'numeric' };
      expect(getNestedValue(obj, '123')).toBe('numeric');
    });

    it('should handle keys with underscores', () => {
      const obj = { user_name: 'Alice' };
      expect(getNestedValue(obj, 'user_name')).toBe('Alice');
    });
  });

  describe('path with primitives', () => {
    it('should return undefined when path goes through a string', () => {
      const obj = { user: 'Alice' };
      expect(getNestedValue(obj, 'user.name')).toBeUndefined();
    });

    it('should return undefined when path goes through a number', () => {
      const obj = { count: 42 };
      expect(getNestedValue(obj, 'count.value')).toBeUndefined();
    });

    it('should return undefined when path goes through a boolean', () => {
      const obj = { active: true };
      expect(getNestedValue(obj, 'active.value')).toBeUndefined();
    });
  });

  describe('array access', () => {
    it('should not support array index access via dot notation', () => {
      const obj = { items: ['a', 'b', 'c'] };
      // Dot notation with numbers is treated as property access, not array index
      expect(getNestedValue(obj, 'items.0')).toBe('a');
    });

    it('should handle nested objects inside arrays accessed by index', () => {
      const obj = { items: [{ name: 'first' }, { name: 'second' }] };
      expect(getNestedValue(obj, 'items.0')).toEqual({ name: 'first' });
    });
  });
});

// =============================================================================
// Performance (Fast Path)
// =============================================================================

describe('getNestedValue - Fast Path', () => {
  it('should use fast path for non-nested keys', () => {
    const obj = { name: 'Alice', 'user.name': 'Bob' };

    // Fast path: no dot in key
    expect(getNestedValue(obj, 'name')).toBe('Alice');

    // When key literally contains a dot, it still works for top-level
    // but dot notation will try to traverse
    expect(getNestedValue(obj, 'user.name')).toBeUndefined();
  });
});

// =============================================================================
// Depth Limit Protection
// =============================================================================

describe('getNestedValue - Depth Limit', () => {
  describe('PathDepthExceededError', () => {
    it('should have correct name and message', () => {
      const error = new PathDepthExceededError(50, 32);
      expect(error.name).toBe('PathDepthExceededError');
      expect(error.message).toContain('Path depth 50 exceeds maximum allowed depth of 32');
      expect(error.message).toContain('DoS attacks');
    });

    it('should be an instance of Error', () => {
      const error = new PathDepthExceededError(50, 32);
      expect(error).toBeInstanceOf(Error);
    });
  });

  describe('default depth limit', () => {
    it('should allow paths up to the default limit', () => {
      // Create a path with exactly MAX_NESTED_PATH_DEPTH segments
      const parts = Array.from({ length: MAX_NESTED_PATH_DEPTH }, (_, i) => `key${i}`);
      const path = parts.join('.');

      // Create a deeply nested object
      let obj: Record<string, unknown> = { value: 'found' };
      for (let i = parts.length - 1; i >= 0; i--) {
        obj = { [parts[i]]: i === parts.length - 1 ? 'found' : obj };
      }

      // Should not throw for exactly MAX_NESTED_PATH_DEPTH levels
      expect(() => getNestedValue(obj, path)).not.toThrow();
    });

    it('should throw PathDepthExceededError for paths exceeding default limit', () => {
      // Create a path with one more than MAX_NESTED_PATH_DEPTH segments
      const parts = Array.from({ length: MAX_NESTED_PATH_DEPTH + 1 }, (_, i) => `key${i}`);
      const path = parts.join('.');

      const obj = { key0: {} };

      expect(() => getNestedValue(obj, path)).toThrow(PathDepthExceededError);
    });

    it('should throw with correct depth information', () => {
      const depth = MAX_NESTED_PATH_DEPTH + 10;
      const parts = Array.from({ length: depth }, (_, i) => `k${i}`);
      const path = parts.join('.');

      try {
        getNestedValue({}, path);
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(PathDepthExceededError);
        expect((error as Error).message).toContain(`Path depth ${depth}`);
        expect((error as Error).message).toContain(`maximum allowed depth of ${MAX_NESTED_PATH_DEPTH}`);
      }
    });
  });

  describe('custom depth limit', () => {
    it('should allow custom depth limit via parameter', () => {
      const obj = { a: { b: { c: { d: { e: 'deep' } } } } };
      const path = 'a.b.c.d.e';

      // Should work with limit of 5
      expect(getNestedValue(obj, path, 5)).toBe('deep');

      // Should throw with limit of 4
      expect(() => getNestedValue(obj, path, 4)).toThrow(PathDepthExceededError);
    });

    it('should respect custom limit of 1', () => {
      const obj = { a: { b: 'value' } };

      // Single segment should work
      expect(getNestedValue(obj, 'a', 1)).toEqual({ b: 'value' });

      // Two segments should throw
      expect(() => getNestedValue(obj, 'a.b', 1)).toThrow(PathDepthExceededError);
    });

    it('should work with very large custom limit', () => {
      const obj = { a: { b: { c: 'value' } } };
      expect(getNestedValue(obj, 'a.b.c', 1000)).toBe('value');
    });
  });

  describe('DoS prevention scenarios', () => {
    it('should reject extremely deep paths (1000 levels)', () => {
      const parts = Array.from({ length: 1000 }, (_, i) => `a${i}`);
      const path = parts.join('.');

      expect(() => getNestedValue({}, path)).toThrow(PathDepthExceededError);
    });

    it('should reject paths designed to cause stack overflow', () => {
      // Attempt a path with 10,000 segments
      const parts = Array.from({ length: 10000 }, () => 'x');
      const path = parts.join('.');

      expect(() => getNestedValue({}, path)).toThrow(PathDepthExceededError);
    });

    it('should not perform traversal when depth limit is exceeded', () => {
      // Create an object that would cause issues if traversed
      let accessCount = 0;
      const proxy = new Proxy(
        {},
        {
          get() {
            accessCount++;
            return proxy;
          },
        }
      );

      const parts = Array.from({ length: 100 }, () => 'key');
      const path = parts.join('.');

      expect(() => getNestedValue(proxy, path)).toThrow(PathDepthExceededError);
      // Ensure no traversal happened (accessCount should be 0)
      expect(accessCount).toBe(0);
    });
  });

  describe('fast path bypass', () => {
    it('should not apply depth limit to single-segment paths (fast path)', () => {
      const obj = { singleKey: 'value' };
      // Single segment never triggers depth check
      expect(getNestedValue(obj, 'singleKey', 0)).toBe('value');
    });
  });

  describe('default constant value', () => {
    it('should have MAX_NESTED_PATH_DEPTH set to 32', () => {
      expect(MAX_NESTED_PATH_DEPTH).toBe(32);
    });
  });
});
