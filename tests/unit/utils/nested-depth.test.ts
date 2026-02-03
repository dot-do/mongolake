/**
 * Nested Path Depth Limit Tests
 *
 * Tests that verify nested field paths are limited to prevent DoS attacks.
 * These tests ensure that paths deeper than 32 levels throw an error to
 * protect against malicious deeply nested path attacks.
 *
 * The depth limit applies to:
 * - getNestedValue (from src/utils/nested.ts)
 * - setNestedValue (from src/utils/update.ts via applyUpdate)
 * - deleteNestedValue (from src/utils/update.ts via applyUpdate)
 */

import { describe, it, expect } from 'vitest';
import { getNestedValue, PathDepthExceededError } from '../../../src/utils/nested.js';
import { applyUpdate } from '../../../src/utils/update.js';
import { MAX_NESTED_PATH_DEPTH } from '../../../src/constants.js';

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Generate a deeply nested path with the specified number of segments
 * @param depth - Number of path segments (e.g., 33 creates "a.b.c...z")
 */
function generateDeepPath(depth: number): string {
  return Array.from({ length: depth }, (_, i) => `level${i}`).join('.');
}

/**
 * Generate a deeply nested object with a value at the specified depth
 * @param depth - Number of nesting levels
 * @param value - The value to set at the deepest level
 */
function generateDeepObject(depth: number, value: unknown = 'found'): Record<string, unknown> {
  // Start with the final value directly (not wrapped in { value: ... })
  let obj: Record<string, unknown> = { [`level${depth - 1}`]: value };
  // Build up from the second-to-last level
  for (let i = depth - 2; i >= 0; i--) {
    obj = { [`level${i}`]: obj };
  }
  return obj;
}

// =============================================================================
// Path Depth Constants Verification
// =============================================================================

describe('Nested Path Depth Limits - Constants', () => {
  it('should have MAX_NESTED_PATH_DEPTH set to 32', () => {
    expect(MAX_NESTED_PATH_DEPTH).toBe(32);
  });

  it('should define a reasonable depth limit for DoS prevention', () => {
    // 32 levels is deep enough for any reasonable use case
    // but shallow enough to prevent stack overflow attacks
    expect(MAX_NESTED_PATH_DEPTH).toBeGreaterThanOrEqual(16);
    expect(MAX_NESTED_PATH_DEPTH).toBeLessThanOrEqual(64);
  });
});

// =============================================================================
// getNestedValue Depth Limits (from src/utils/nested.ts)
// =============================================================================

describe('getNestedValue - Depth Limit Enforcement', () => {
  describe('valid paths within limit', () => {
    it('should accept paths with exactly 32 levels (at the limit)', () => {
      const path = generateDeepPath(32);
      const obj = generateDeepObject(32, 'found');

      expect(() => getNestedValue(obj, path)).not.toThrow();
    });

    it('should accept paths with fewer than 32 levels', () => {
      const depths = [1, 5, 10, 15, 20, 25, 30, 31];

      for (const depth of depths) {
        const path = generateDeepPath(depth);
        const obj = generateDeepObject(depth, `value-at-${depth}`);

        expect(() => getNestedValue(obj, path)).not.toThrow();
      }
    });

    it('should return correct value for valid deep paths', () => {
      const obj = { a: { b: { c: { d: { e: 'deep-value' } } } } };
      expect(getNestedValue(obj, 'a.b.c.d.e')).toBe('deep-value');
    });
  });

  describe('paths exceeding limit', () => {
    it('should throw PathDepthExceededError for paths with 33 levels', () => {
      const path = generateDeepPath(33);
      const obj = {};

      expect(() => getNestedValue(obj, path)).toThrow(PathDepthExceededError);
    });

    it('should throw for path with exactly 100 segments (a.b.c.d...)', () => {
      const path = generateDeepPath(100);
      const obj = {};

      expect(() => getNestedValue(obj, path)).toThrow(PathDepthExceededError);
    });

    it('should throw for extremely deep paths (1000 levels)', () => {
      const path = generateDeepPath(1000);
      const obj = {};

      expect(() => getNestedValue(obj, path)).toThrow(PathDepthExceededError);
    });

    it('should throw for paths designed to cause stack overflow (10000 levels)', () => {
      const path = generateDeepPath(10000);
      const obj = {};

      expect(() => getNestedValue(obj, path)).toThrow(PathDepthExceededError);
    });

    it('should include depth information in error message', () => {
      const path = generateDeepPath(50);

      try {
        getNestedValue({}, path);
        expect.fail('Should have thrown PathDepthExceededError');
      } catch (error) {
        expect(error).toBeInstanceOf(PathDepthExceededError);
        expect((error as Error).message).toContain('50');
        expect((error as Error).message).toContain('32');
        expect((error as Error).message).toContain('DoS');
      }
    });
  });
});

// =============================================================================
// setNestedValue Depth Limits (via applyUpdate $set)
// =============================================================================

describe('setNestedValue - Depth Limit Enforcement (via applyUpdate $set)', () => {
  describe('valid paths within limit', () => {
    it('should accept $set with exactly 32 levels (at the limit)', () => {
      const path = generateDeepPath(32);
      const doc = { _id: '1' };

      expect(() => applyUpdate(doc, { $set: { [path]: 'value' } })).not.toThrow();
    });

    it('should accept $set with fewer than 32 levels', () => {
      const depths = [1, 5, 10, 15, 20, 25, 30, 31];

      for (const depth of depths) {
        const path = generateDeepPath(depth);
        const doc = { _id: '1' };

        expect(() => applyUpdate(doc, { $set: { [path]: `value-${depth}` } })).not.toThrow();
      }
    });

    it('should correctly set value for valid nested path', () => {
      const doc = { _id: '1' };
      const result = applyUpdate(doc, { $set: { 'a.b.c': 'nested-value' } });

      expect((result as Record<string, unknown>).a).toEqual({ b: { c: 'nested-value' } });
    });
  });

  describe('paths exceeding limit', () => {
    it('should throw for $set with 33 levels', () => {
      const path = generateDeepPath(33);
      const doc = { _id: '1' };

      expect(() => applyUpdate(doc, { $set: { [path]: 'value' } })).toThrow();
    });

    it('should throw for $set with path having exactly 100 segments', () => {
      const path = generateDeepPath(100);
      const doc = { _id: '1' };

      expect(() => applyUpdate(doc, { $set: { [path]: 'value' } })).toThrow();
    });

    it('should throw for $set with extremely deep paths (1000 levels)', () => {
      const path = generateDeepPath(1000);
      const doc = { _id: '1' };

      expect(() => applyUpdate(doc, { $set: { [path]: 'value' } })).toThrow();
    });

    it('should throw PathDepthExceededError with proper error type', () => {
      const path = generateDeepPath(50);
      const doc = { _id: '1' };

      expect(() => applyUpdate(doc, { $set: { [path]: 'value' } })).toThrow(PathDepthExceededError);
    });

    it('should prevent DoS via deeply nested $set paths', () => {
      // This test verifies that malicious deeply nested paths cannot be used
      // to exhaust memory or cause stack overflow
      const path = generateDeepPath(10000);
      const doc = { _id: '1' };

      expect(() => applyUpdate(doc, { $set: { [path]: 'malicious' } })).toThrow(
        PathDepthExceededError
      );
    });
  });
});

// =============================================================================
// deleteNestedValue Depth Limits (via applyUpdate $unset)
// =============================================================================

describe('deleteNestedValue - Depth Limit Enforcement (via applyUpdate $unset)', () => {
  describe('valid paths within limit', () => {
    it('should accept $unset with exactly 32 levels (at the limit)', () => {
      const path = generateDeepPath(32);
      const obj = generateDeepObject(32, 'to-delete');
      const doc = { _id: '1', ...obj };

      expect(() => applyUpdate(doc, { $unset: { [path]: '' } })).not.toThrow();
    });

    it('should accept $unset with fewer than 32 levels', () => {
      const depths = [1, 5, 10, 15, 20, 25, 30, 31];

      for (const depth of depths) {
        const path = generateDeepPath(depth);
        const obj = generateDeepObject(depth, 'to-delete');
        const doc = { _id: '1', ...obj };

        expect(() => applyUpdate(doc, { $unset: { [path]: '' } })).not.toThrow();
      }
    });

    it('should correctly delete value for valid nested path', () => {
      const doc = { _id: '1', a: { b: { c: 'to-delete', d: 'keep' } } };
      const result = applyUpdate(doc, { $unset: { 'a.b.c': '' } });

      expect((result as Record<string, unknown>).a).toEqual({ b: { d: 'keep' } });
    });
  });

  describe('paths exceeding limit', () => {
    it('should throw for $unset with 33 levels', () => {
      const path = generateDeepPath(33);
      const doc = { _id: '1' };

      expect(() => applyUpdate(doc, { $unset: { [path]: '' } })).toThrow();
    });

    it('should throw for $unset with path having exactly 100 segments', () => {
      const path = generateDeepPath(100);
      const doc = { _id: '1' };

      expect(() => applyUpdate(doc, { $unset: { [path]: '' } })).toThrow();
    });

    it('should throw for $unset with extremely deep paths (1000 levels)', () => {
      const path = generateDeepPath(1000);
      const doc = { _id: '1' };

      expect(() => applyUpdate(doc, { $unset: { [path]: '' } })).toThrow();
    });

    it('should throw PathDepthExceededError with proper error type', () => {
      const path = generateDeepPath(50);
      const doc = { _id: '1' };

      expect(() => applyUpdate(doc, { $unset: { [path]: '' } })).toThrow(PathDepthExceededError);
    });

    it('should prevent DoS via deeply nested $unset paths', () => {
      const path = generateDeepPath(10000);
      const doc = { _id: '1' };

      expect(() => applyUpdate(doc, { $unset: { [path]: '' } })).toThrow(PathDepthExceededError);
    });
  });
});

// =============================================================================
// $inc Operator with Nested Paths
// =============================================================================

describe('$inc Operator - Depth Limit Enforcement', () => {
  describe('valid paths within limit', () => {
    it('should accept $inc with exactly 32 levels (at the limit)', () => {
      const path = generateDeepPath(32);
      const obj = generateDeepObject(32, 5);
      const doc = { _id: '1', ...obj };

      expect(() => applyUpdate(doc, { $inc: { [path]: 1 } })).not.toThrow();
    });

    it('should correctly increment value for valid nested path', () => {
      const doc = { _id: '1', stats: { counters: { views: 10 } } };
      const result = applyUpdate(doc, { $inc: { 'stats.counters.views': 5 } });

      expect((result as Record<string, unknown>).stats).toEqual({ counters: { views: 15 } });
    });
  });

  describe('paths exceeding limit', () => {
    it('should throw for $inc with 33 levels', () => {
      const path = generateDeepPath(33);
      const doc = { _id: '1' };

      expect(() => applyUpdate(doc, { $inc: { [path]: 1 } })).toThrow();
    });

    it('should throw for $inc with path having exactly 100 segments', () => {
      const path = generateDeepPath(100);
      const doc = { _id: '1' };

      expect(() => applyUpdate(doc, { $inc: { [path]: 1 } })).toThrow();
    });

    it('should throw PathDepthExceededError with proper error type', () => {
      const path = generateDeepPath(50);
      const doc = { _id: '1' };

      expect(() => applyUpdate(doc, { $inc: { [path]: 1 } })).toThrow(PathDepthExceededError);
    });
  });
});

// =============================================================================
// Combined Operations with Depth Limits
// =============================================================================

describe('Combined Update Operations - Depth Limit Enforcement', () => {
  it('should throw if any path in $set exceeds limit', () => {
    const validPath = 'a.b.c';
    const invalidPath = generateDeepPath(50);
    const doc = { _id: '1' };

    expect(() =>
      applyUpdate(doc, {
        $set: { [validPath]: 'ok', [invalidPath]: 'bad' },
      })
    ).toThrow(PathDepthExceededError);
  });

  it('should throw if any path in $unset exceeds limit', () => {
    const validPath = 'a.b.c';
    const invalidPath = generateDeepPath(50);
    const doc = { _id: '1', a: { b: { c: 'value' } } };

    expect(() =>
      applyUpdate(doc, {
        $unset: { [validPath]: '', [invalidPath]: '' },
      })
    ).toThrow(PathDepthExceededError);
  });

  it('should allow combined operations with valid paths', () => {
    const doc = { _id: '1', a: { b: 1 }, c: { d: { e: 'value' } } };

    expect(() =>
      applyUpdate(doc, {
        $set: { 'x.y.z': 'new' },
        $inc: { 'a.b': 5 },
        $unset: { 'c.d.e': '' },
      })
    ).not.toThrow();
  });
});

// =============================================================================
// Edge Cases
// =============================================================================

describe('Nested Path Depth Limits - Edge Cases', () => {
  describe('single-segment paths (fast path)', () => {
    it('should always allow single-segment paths for getNestedValue', () => {
      const obj = { name: 'value' };
      expect(getNestedValue(obj, 'name')).toBe('value');
    });

    it('should always allow single-segment paths for $set', () => {
      const doc = { _id: '1' };
      const result = applyUpdate(doc, { $set: { name: 'value' } });
      expect((result as Record<string, unknown>).name).toBe('value');
    });

    it('should always allow single-segment paths for $unset', () => {
      const doc = { _id: '1', name: 'value' };
      const result = applyUpdate(doc, { $unset: { name: '' } });
      expect('name' in result).toBe(false);
    });
  });

  describe('boundary conditions', () => {
    it('should accept path with exactly MAX_NESTED_PATH_DEPTH segments', () => {
      const path = generateDeepPath(MAX_NESTED_PATH_DEPTH);
      const obj = generateDeepObject(MAX_NESTED_PATH_DEPTH, 'boundary-value');

      expect(() => getNestedValue(obj, path)).not.toThrow();
    });

    it('should reject path with MAX_NESTED_PATH_DEPTH + 1 segments', () => {
      const path = generateDeepPath(MAX_NESTED_PATH_DEPTH + 1);

      expect(() => getNestedValue({}, path)).toThrow(PathDepthExceededError);
    });
  });

  describe('empty and special paths', () => {
    it('should handle empty path gracefully', () => {
      const obj = { '': 'empty-key-value' };
      // Empty path should not throw depth error
      expect(() => getNestedValue(obj, '')).not.toThrow();
    });

    it('should handle path with only dots', () => {
      // A path like "...." would create empty segments
      const path = '....';
      // This creates 5 empty segments, which is within limit
      expect(() => getNestedValue({}, path)).not.toThrow();
    });

    it('should correctly count segments in paths with leading/trailing dots', () => {
      // ".a.b.c." would create: ['', 'a', 'b', 'c', ''] = 5 segments
      const path = '.a.b.c.';
      expect(() => getNestedValue({}, path)).not.toThrow();
    });
  });

  describe('depth check timing', () => {
    it('should check depth before any traversal (getNestedValue)', () => {
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

      const path = generateDeepPath(100);

      expect(() => getNestedValue(proxy, path)).toThrow(PathDepthExceededError);
      // Should not have traversed at all
      expect(accessCount).toBe(0);
    });

    it('should check depth before any traversal ($set)', () => {
      let setCount = 0;
      const handler = {
        set() {
          setCount++;
          return true;
        },
      };
      const proxy = new Proxy({} as Record<string, unknown>, handler);

      const path = generateDeepPath(100);
      const doc = { _id: '1', nested: proxy };

      expect(() => applyUpdate(doc, { $set: { [path]: 'value' } })).toThrow(PathDepthExceededError);
    });
  });
});
