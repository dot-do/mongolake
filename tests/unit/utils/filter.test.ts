/**
 * Filter Matching Tests
 *
 * Tests for the matchesFilter function that handles:
 * - Basic equality matching
 * - Comparison operators ($eq, $ne, $gt, $gte, $lt, $lte)
 * - Array operators ($in, $nin)
 * - Logical operators ($and, $or, $nor, $not)
 * - Element operators ($exists)
 * - Evaluation operators ($regex)
 * - Nested field paths with dot notation
 * - Edge cases (null, undefined, empty objects)
 */

import { describe, it, expect } from 'vitest';
import {
  matchesFilter,
  validateRegexPattern,
  RegexSecurityError,
  MAX_REGEX_PATTERN_LENGTH,
} from '../../../src/utils/filter.js';

// =============================================================================
// Test Document Type
// =============================================================================

interface TestDocument {
  _id: string;
  name?: string;
  age?: number;
  status?: string;
  score?: number;
  tags?: string[];
  active?: boolean;
  email?: string | null;
  profile?: {
    firstName?: string;
    lastName?: string;
    address?: {
      city?: string;
      country?: string;
      zip?: number;
    };
    settings?: {
      theme?: string;
      notifications?: boolean;
    };
  };
  metadata?: Record<string, unknown>;
  createdAt?: Date;
  items?: Array<{ id: number; name: string }>;
}

// =============================================================================
// Basic Equality Matching
// =============================================================================

describe('matchesFilter - Basic Equality', () => {
  it('should match document with exact string equality', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
    expect(matchesFilter(doc, { name: 'Alice' })).toBe(true);
  });

  it('should not match document with different string value', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
    expect(matchesFilter(doc, { name: 'Bob' })).toBe(false);
  });

  it('should match document with exact number equality', () => {
    const doc: TestDocument = { _id: 'doc1', age: 30 };
    expect(matchesFilter(doc, { age: 30 })).toBe(true);
  });

  it('should not match document with different number value', () => {
    const doc: TestDocument = { _id: 'doc1', age: 30 };
    expect(matchesFilter(doc, { age: 25 })).toBe(false);
  });

  it('should match document with exact boolean equality', () => {
    const doc: TestDocument = { _id: 'doc1', active: true };
    expect(matchesFilter(doc, { active: true })).toBe(true);
    expect(matchesFilter(doc, { active: false })).toBe(false);
  });

  it('should match document with multiple field equality', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice', age: 30, status: 'active' };
    expect(matchesFilter(doc, { name: 'Alice', age: 30 })).toBe(true);
  });

  it('should not match document if any field does not match', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice', age: 30 };
    expect(matchesFilter(doc, { name: 'Alice', age: 25 })).toBe(false);
  });

  it('should match empty filter to any document', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice', age: 30 };
    expect(matchesFilter(doc, {})).toBe(true);
  });

  it('should match document with $eq operator', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
    expect(matchesFilter(doc, { name: { $eq: 'Alice' } })).toBe(true);
    expect(matchesFilter(doc, { name: { $eq: 'Bob' } })).toBe(false);
  });
});

// =============================================================================
// Comparison Operators
// =============================================================================

describe('matchesFilter - Comparison Operators', () => {
  describe('$gt (greater than)', () => {
    it('should match when value is greater than condition', () => {
      const doc: TestDocument = { _id: 'doc1', age: 30 };
      expect(matchesFilter(doc, { age: { $gt: 25 } })).toBe(true);
    });

    it('should not match when value equals condition', () => {
      const doc: TestDocument = { _id: 'doc1', age: 30 };
      expect(matchesFilter(doc, { age: { $gt: 30 } })).toBe(false);
    });

    it('should not match when value is less than condition', () => {
      const doc: TestDocument = { _id: 'doc1', age: 30 };
      expect(matchesFilter(doc, { age: { $gt: 35 } })).toBe(false);
    });

    it('should work with string comparison', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Bob' };
      expect(matchesFilter(doc, { name: { $gt: 'Alice' } })).toBe(true);
      expect(matchesFilter(doc, { name: { $gt: 'Charlie' } })).toBe(false);
    });
  });

  describe('$gte (greater than or equal)', () => {
    it('should match when value is greater than condition', () => {
      const doc: TestDocument = { _id: 'doc1', age: 30 };
      expect(matchesFilter(doc, { age: { $gte: 25 } })).toBe(true);
    });

    it('should match when value equals condition', () => {
      const doc: TestDocument = { _id: 'doc1', age: 30 };
      expect(matchesFilter(doc, { age: { $gte: 30 } })).toBe(true);
    });

    it('should not match when value is less than condition', () => {
      const doc: TestDocument = { _id: 'doc1', age: 30 };
      expect(matchesFilter(doc, { age: { $gte: 35 } })).toBe(false);
    });
  });

  describe('$lt (less than)', () => {
    it('should match when value is less than condition', () => {
      const doc: TestDocument = { _id: 'doc1', age: 30 };
      expect(matchesFilter(doc, { age: { $lt: 35 } })).toBe(true);
    });

    it('should not match when value equals condition', () => {
      const doc: TestDocument = { _id: 'doc1', age: 30 };
      expect(matchesFilter(doc, { age: { $lt: 30 } })).toBe(false);
    });

    it('should not match when value is greater than condition', () => {
      const doc: TestDocument = { _id: 'doc1', age: 30 };
      expect(matchesFilter(doc, { age: { $lt: 25 } })).toBe(false);
    });
  });

  describe('$lte (less than or equal)', () => {
    it('should match when value is less than condition', () => {
      const doc: TestDocument = { _id: 'doc1', age: 30 };
      expect(matchesFilter(doc, { age: { $lte: 35 } })).toBe(true);
    });

    it('should match when value equals condition', () => {
      const doc: TestDocument = { _id: 'doc1', age: 30 };
      expect(matchesFilter(doc, { age: { $lte: 30 } })).toBe(true);
    });

    it('should not match when value is greater than condition', () => {
      const doc: TestDocument = { _id: 'doc1', age: 30 };
      expect(matchesFilter(doc, { age: { $lte: 25 } })).toBe(false);
    });
  });

  describe('$ne (not equal)', () => {
    it('should match when value is not equal to condition', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
      expect(matchesFilter(doc, { name: { $ne: 'Bob' } })).toBe(true);
    });

    it('should not match when value equals condition', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
      expect(matchesFilter(doc, { name: { $ne: 'Alice' } })).toBe(false);
    });

    it('should match when field does not exist and condition is a value', () => {
      const doc: TestDocument = { _id: 'doc1' };
      expect(matchesFilter(doc, { name: { $ne: 'Alice' } })).toBe(true);
    });
  });

  describe('combined comparison operators', () => {
    it('should support range queries with $gt and $lt', () => {
      const doc: TestDocument = { _id: 'doc1', age: 30 };
      expect(matchesFilter(doc, { age: { $gt: 25, $lt: 35 } })).toBe(true);
      expect(matchesFilter(doc, { age: { $gt: 30, $lt: 35 } })).toBe(false);
      expect(matchesFilter(doc, { age: { $gt: 25, $lt: 30 } })).toBe(false);
    });

    it('should support inclusive range queries with $gte and $lte', () => {
      const doc: TestDocument = { _id: 'doc1', age: 30 };
      expect(matchesFilter(doc, { age: { $gte: 30, $lte: 30 } })).toBe(true);
      expect(matchesFilter(doc, { age: { $gte: 25, $lte: 35 } })).toBe(true);
    });
  });
});

// =============================================================================
// Array Operators
// =============================================================================

describe('matchesFilter - Array Operators', () => {
  describe('$in', () => {
    it('should match when value is in array', () => {
      const doc: TestDocument = { _id: 'doc1', status: 'active' };
      expect(matchesFilter(doc, { status: { $in: ['active', 'pending'] } })).toBe(true);
    });

    it('should not match when value is not in array', () => {
      const doc: TestDocument = { _id: 'doc1', status: 'inactive' };
      expect(matchesFilter(doc, { status: { $in: ['active', 'pending'] } })).toBe(false);
    });

    it('should match with numeric values', () => {
      const doc: TestDocument = { _id: 'doc1', age: 30 };
      expect(matchesFilter(doc, { age: { $in: [25, 30, 35] } })).toBe(true);
      expect(matchesFilter(doc, { age: { $in: [25, 35, 40] } })).toBe(false);
    });

    it('should work with single-element array', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
      expect(matchesFilter(doc, { name: { $in: ['Alice'] } })).toBe(true);
    });

    it('should not match with empty array', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
      expect(matchesFilter(doc, { name: { $in: [] } })).toBe(false);
    });
  });

  describe('$nin (not in)', () => {
    it('should match when value is not in array', () => {
      const doc: TestDocument = { _id: 'doc1', status: 'completed' };
      expect(matchesFilter(doc, { status: { $nin: ['active', 'pending'] } })).toBe(true);
    });

    it('should not match when value is in array', () => {
      const doc: TestDocument = { _id: 'doc1', status: 'active' };
      expect(matchesFilter(doc, { status: { $nin: ['active', 'pending'] } })).toBe(false);
    });

    it('should match when field does not exist', () => {
      const doc: TestDocument = { _id: 'doc1' };
      expect(matchesFilter(doc, { status: { $nin: ['active', 'pending'] } })).toBe(true);
    });

    it('should match with empty array', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
      expect(matchesFilter(doc, { name: { $nin: [] } })).toBe(true);
    });
  });
});

// =============================================================================
// Logical Operators
// =============================================================================

describe('matchesFilter - Logical Operators', () => {
  describe('$and', () => {
    it('should match when all conditions are true', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice', age: 30 };
      expect(
        matchesFilter(doc, {
          $and: [{ name: 'Alice' }, { age: { $gte: 25 } }],
        })
      ).toBe(true);
    });

    it('should not match when any condition is false', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice', age: 30 };
      expect(
        matchesFilter(doc, {
          $and: [{ name: 'Alice' }, { age: { $lt: 25 } }],
        })
      ).toBe(false);
    });

    it('should match with single condition', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
      expect(matchesFilter(doc, { $and: [{ name: 'Alice' }] })).toBe(true);
    });

    it('should match with empty conditions array', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
      expect(matchesFilter(doc, { $and: [] })).toBe(true);
    });

    it('should support multiple conditions on same field', () => {
      const doc: TestDocument = { _id: 'doc1', age: 30 };
      expect(
        matchesFilter(doc, {
          $and: [{ age: { $gte: 25 } }, { age: { $lte: 35 } }],
        })
      ).toBe(true);
    });
  });

  describe('$or', () => {
    it('should match when any condition is true', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice', age: 30 };
      expect(
        matchesFilter(doc, {
          $or: [{ name: 'Bob' }, { age: 30 }],
        })
      ).toBe(true);
    });

    it('should not match when all conditions are false', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice', age: 30 };
      expect(
        matchesFilter(doc, {
          $or: [{ name: 'Bob' }, { age: 25 }],
        })
      ).toBe(false);
    });

    it('should match when first condition is true', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
      expect(
        matchesFilter(doc, {
          $or: [{ name: 'Alice' }, { name: 'Bob' }],
        })
      ).toBe(true);
    });

    it('should match with single condition', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
      expect(matchesFilter(doc, { $or: [{ name: 'Alice' }] })).toBe(true);
    });

    it('should not match with empty conditions array', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
      expect(matchesFilter(doc, { $or: [] })).toBe(false);
    });
  });

  describe('$nor', () => {
    it('should match when no conditions are true', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice', age: 30 };
      expect(
        matchesFilter(doc, {
          $nor: [{ name: 'Bob' }, { age: 25 }],
        })
      ).toBe(true);
    });

    it('should not match when any condition is true', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice', age: 30 };
      expect(
        matchesFilter(doc, {
          $nor: [{ name: 'Alice' }, { age: 25 }],
        })
      ).toBe(false);
    });

    it('should not match when all conditions are true', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice', age: 30 };
      expect(
        matchesFilter(doc, {
          $nor: [{ name: 'Alice' }, { age: 30 }],
        })
      ).toBe(false);
    });

    it('should match with empty conditions array', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
      expect(matchesFilter(doc, { $nor: [] })).toBe(true);
    });
  });

  describe('$not', () => {
    it('should match when nested condition is false', () => {
      const doc: TestDocument = { _id: 'doc1', age: 30 };
      expect(matchesFilter(doc, { age: { $not: { $lt: 25 } } })).toBe(true);
    });

    it('should not match when nested condition is true', () => {
      const doc: TestDocument = { _id: 'doc1', age: 30 };
      expect(matchesFilter(doc, { age: { $not: { $gte: 25 } } })).toBe(false);
    });

    it('should work with regex negation', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
      expect(matchesFilter(doc, { name: { $not: { $regex: '^B' } } })).toBe(true);
      expect(matchesFilter(doc, { name: { $not: { $regex: '^A' } } })).toBe(false);
    });

    it('should work with $in negation', () => {
      const doc: TestDocument = { _id: 'doc1', status: 'completed' };
      expect(matchesFilter(doc, { status: { $not: { $in: ['active', 'pending'] } } })).toBe(true);
    });
  });

  describe('nested logical operators', () => {
    it('should support $and inside $or', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice', age: 30, status: 'active' };
      expect(
        matchesFilter(doc, {
          $or: [
            { $and: [{ name: 'Bob' }, { age: 25 }] },
            { $and: [{ name: 'Alice' }, { age: 30 }] },
          ],
        })
      ).toBe(true);
    });

    it('should support $or inside $and', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice', age: 30, status: 'active' };
      expect(
        matchesFilter(doc, {
          $and: [{ $or: [{ name: 'Alice' }, { name: 'Bob' }] }, { age: { $gte: 25 } }],
        })
      ).toBe(true);
    });

    it('should support deeply nested logical operators', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice', age: 30, status: 'active' };
      expect(
        matchesFilter(doc, {
          $and: [
            {
              $or: [{ name: 'Alice' }, { $and: [{ name: 'Bob' }, { status: 'pending' }] }],
            },
            { age: { $gte: 25 } },
          ],
        })
      ).toBe(true);
    });
  });
});

// =============================================================================
// $exists Operator
// =============================================================================

describe('matchesFilter - $exists Operator', () => {
  it('should match when field exists and $exists is true', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
    expect(matchesFilter(doc, { name: { $exists: true } })).toBe(true);
  });

  it('should not match when field does not exist and $exists is true', () => {
    const doc: TestDocument = { _id: 'doc1' };
    expect(matchesFilter(doc, { name: { $exists: true } })).toBe(false);
  });

  it('should match when field does not exist and $exists is false', () => {
    const doc: TestDocument = { _id: 'doc1' };
    expect(matchesFilter(doc, { name: { $exists: false } })).toBe(true);
  });

  it('should not match when field exists and $exists is false', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
    expect(matchesFilter(doc, { name: { $exists: false } })).toBe(false);
  });

  it('should consider null values as existing', () => {
    const doc: TestDocument = { _id: 'doc1', email: null };
    expect(matchesFilter(doc, { email: { $exists: true } })).toBe(true);
    expect(matchesFilter(doc, { email: { $exists: false } })).toBe(false);
  });

  it('should work with nested field paths', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      profile: { firstName: 'Alice' },
    };
    expect(matchesFilter(doc, { 'profile.firstName': { $exists: true } })).toBe(true);
    expect(matchesFilter(doc, { 'profile.lastName': { $exists: false } })).toBe(true);
  });
});

// =============================================================================
// $regex Operator
// =============================================================================

describe('matchesFilter - $regex Operator', () => {
  it('should match string with regex pattern', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
    expect(matchesFilter(doc, { name: { $regex: '^A' } })).toBe(true);
  });

  it('should not match string that does not match pattern', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
    expect(matchesFilter(doc, { name: { $regex: '^B' } })).toBe(false);
  });

  it('should match with regex object', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
    expect(matchesFilter(doc, { name: { $regex: /alice/i } })).toBe(true);
  });

  it('should not match non-string values', () => {
    const doc: TestDocument = { _id: 'doc1', age: 30 };
    expect(matchesFilter(doc, { age: { $regex: '30' } })).toBe(false);
  });

  it('should match with complex patterns', () => {
    const doc: TestDocument = { _id: 'doc1', email: 'alice@example.com' };
    expect(matchesFilter(doc, { email: { $regex: '^[a-z]+@[a-z]+\\.[a-z]+$' } })).toBe(true);
  });

  it('should match partial patterns', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice Smith' };
    expect(matchesFilter(doc, { name: { $regex: 'Smith' } })).toBe(true);
  });

  it('should handle special regex characters', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Hello (World)' };
    expect(matchesFilter(doc, { name: { $regex: '\\(World\\)' } })).toBe(true);
  });
});

// =============================================================================
// Nested Field Paths (Dot Notation)
// =============================================================================

describe('matchesFilter - Nested Field Paths', () => {
  it('should match single level nested field', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      profile: { firstName: 'Alice', lastName: 'Smith' },
    };
    expect(matchesFilter(doc, { 'profile.firstName': 'Alice' })).toBe(true);
  });

  it('should match deeply nested field', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      profile: {
        address: { city: 'New York', country: 'USA' },
      },
    };
    expect(matchesFilter(doc, { 'profile.address.city': 'New York' })).toBe(true);
  });

  it('should not match when nested field has different value', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      profile: { firstName: 'Alice' },
    };
    expect(matchesFilter(doc, { 'profile.firstName': 'Bob' })).toBe(false);
  });

  it('should return undefined for non-existent nested path', () => {
    const doc: TestDocument = { _id: 'doc1' };
    expect(matchesFilter(doc, { 'profile.firstName': { $exists: false } })).toBe(true);
  });

  it('should handle partial nested path', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      profile: { firstName: 'Alice' },
    };
    expect(matchesFilter(doc, { 'profile.address.city': { $exists: false } })).toBe(true);
  });

  it('should work with comparison operators on nested fields', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      profile: { address: { zip: 10001 } },
    };
    expect(matchesFilter(doc, { 'profile.address.zip': { $gt: 10000 } })).toBe(true);
    expect(matchesFilter(doc, { 'profile.address.zip': { $lt: 10000 } })).toBe(false);
  });

  it('should work with $in on nested fields', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      profile: { settings: { theme: 'dark' } },
    };
    expect(matchesFilter(doc, { 'profile.settings.theme': { $in: ['dark', 'light'] } })).toBe(true);
  });

  it('should work with $regex on nested fields', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      profile: { firstName: 'Alice' },
    };
    expect(matchesFilter(doc, { 'profile.firstName': { $regex: '^A' } })).toBe(true);
  });
});

// =============================================================================
// Edge Cases
// =============================================================================

describe('matchesFilter - Edge Cases', () => {
  describe('null values', () => {
    it('should match null with null filter', () => {
      const doc: TestDocument = { _id: 'doc1', email: null };
      expect(matchesFilter(doc, { email: null })).toBe(true);
    });

    it('should not match non-null with null filter', () => {
      const doc: TestDocument = { _id: 'doc1', email: 'alice@example.com' };
      expect(matchesFilter(doc, { email: null })).toBe(false);
    });

    it('should not match undefined with null filter', () => {
      const doc: TestDocument = { _id: 'doc1' };
      expect(matchesFilter(doc, { email: null })).toBe(false);
    });
  });

  describe('undefined values', () => {
    it('should handle undefined field with $exists', () => {
      const doc: TestDocument = { _id: 'doc1' };
      expect(matchesFilter(doc, { name: { $exists: false } })).toBe(true);
    });

    it('should not match undefined with equality filter', () => {
      const doc: TestDocument = { _id: 'doc1' };
      expect(matchesFilter(doc, { name: 'Alice' })).toBe(false);
    });
  });

  describe('empty objects', () => {
    it('should match any document with empty filter', () => {
      const doc: TestDocument = { _id: 'doc1', name: 'Alice', age: 30 };
      expect(matchesFilter(doc, {})).toBe(true);
    });

    it('should match empty document with empty filter', () => {
      const doc: TestDocument = { _id: 'doc1' };
      expect(matchesFilter(doc, {})).toBe(true);
    });

    it('should handle document with empty nested object', () => {
      const doc: TestDocument = { _id: 'doc1', profile: {} };
      expect(matchesFilter(doc, { 'profile.firstName': { $exists: false } })).toBe(true);
    });
  });

  describe('array values', () => {
    it('should not match array field with equality (uses reference comparison)', () => {
      // Note: matchesFilter uses strict equality (===) for arrays, not deep comparison
      const doc: TestDocument = { _id: 'doc1', tags: ['a', 'b', 'c'] };
      expect(matchesFilter(doc, { tags: ['a', 'b', 'c'] })).toBe(false);
    });

    it('should match array field when same reference', () => {
      const tags = ['a', 'b', 'c'];
      const doc: TestDocument = { _id: 'doc1', tags };
      expect(matchesFilter(doc, { tags })).toBe(true);
    });
  });

  describe('special number values', () => {
    it('should handle zero values', () => {
      const doc: TestDocument = { _id: 'doc1', score: 0 };
      expect(matchesFilter(doc, { score: 0 })).toBe(true);
      expect(matchesFilter(doc, { score: { $gte: 0 } })).toBe(true);
      expect(matchesFilter(doc, { score: { $gt: 0 } })).toBe(false);
    });

    it('should handle negative values', () => {
      const doc: TestDocument = { _id: 'doc1', score: -10 };
      expect(matchesFilter(doc, { score: { $lt: 0 } })).toBe(true);
      expect(matchesFilter(doc, { score: { $gte: -10 } })).toBe(true);
    });
  });

  describe('type coercion', () => {
    it('should not match string "30" with number 30', () => {
      const doc = { _id: 'doc1', value: '30' };
      expect(matchesFilter(doc, { value: 30 })).toBe(false);
    });

    it('should not match number 1 with boolean true', () => {
      const doc = { _id: 'doc1', value: 1 };
      expect(matchesFilter(doc, { value: true })).toBe(false);
    });
  });
});

// =============================================================================
// Complex Queries
// =============================================================================

describe('matchesFilter - Complex Queries', () => {
  it('should handle complex real-world filter', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      name: 'Alice Smith',
      age: 30,
      status: 'active',
      profile: {
        address: { city: 'New York', country: 'USA' },
        settings: { theme: 'dark', notifications: true },
      },
      tags: ['premium', 'verified'],
    };

    const filter = {
      $and: [
        { status: { $in: ['active', 'pending'] } },
        { age: { $gte: 18, $lte: 65 } },
        { name: { $regex: 'Smith' } },
        { 'profile.address.country': 'USA' },
        { 'profile.settings.notifications': true },
      ],
    };

    expect(matchesFilter(doc, filter)).toBe(true);
  });

  it('should handle filter with mixed top-level and logical operators', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      name: 'Alice',
      age: 30,
      status: 'active',
    };

    const filter = {
      status: 'active',
      $or: [{ age: { $lt: 25 } }, { name: 'Alice' }],
    };

    expect(matchesFilter(doc, filter)).toBe(true);
  });

  it('should handle filter with all operator types', () => {
    const doc: TestDocument = {
      _id: 'doc1',
      name: 'Alice',
      age: 30,
      status: 'active',
      email: 'alice@example.com',
      profile: { firstName: 'Alice' },
    };

    const filter = {
      name: 'Alice', // equality
      age: { $gte: 25, $lte: 35 }, // comparison
      status: { $in: ['active', 'pending'] }, // array
      email: { $regex: '@example\\.com$' }, // regex
      'profile.firstName': { $exists: true }, // exists + nested
      $and: [{ name: { $ne: 'Bob' } }], // logical
    };

    expect(matchesFilter(doc, filter)).toBe(true);
  });
});

// =============================================================================
// Regex Security Validation (ReDoS Prevention)
// =============================================================================

describe('validateRegexPattern - ReDoS Prevention', () => {
  describe('pattern length validation', () => {
    it('should accept patterns within length limit', () => {
      const pattern = 'a'.repeat(100);
      expect(() => validateRegexPattern(pattern)).not.toThrow();
    });

    it('should accept patterns at exactly the length limit', () => {
      const pattern = 'a'.repeat(MAX_REGEX_PATTERN_LENGTH);
      expect(() => validateRegexPattern(pattern)).not.toThrow();
    });

    it('should reject patterns exceeding length limit', () => {
      const pattern = 'a'.repeat(MAX_REGEX_PATTERN_LENGTH + 1);
      expect(() => validateRegexPattern(pattern)).toThrow(RegexSecurityError);
      expect(() => validateRegexPattern(pattern)).toThrow(/exceeds maximum allowed length/);
    });

    it('should validate RegExp object source length', () => {
      const pattern = new RegExp('a'.repeat(MAX_REGEX_PATTERN_LENGTH + 1));
      expect(() => validateRegexPattern(pattern)).toThrow(RegexSecurityError);
    });
  });

  describe('dangerous pattern detection', () => {
    it('should reject nested quantifiers (a+)+', () => {
      expect(() => validateRegexPattern('(a+)+')).toThrow(RegexSecurityError);
      expect(() => validateRegexPattern('(a+)+')).toThrow(/dangerous constructs/);
    });

    it('should reject nested quantifiers (a*)+', () => {
      expect(() => validateRegexPattern('(a*)+')).toThrow(RegexSecurityError);
    });

    it('should reject nested quantifiers (a+)*', () => {
      expect(() => validateRegexPattern('(a+)*')).toThrow(RegexSecurityError);
    });

    it('should reject nested quantifiers (a*)*', () => {
      expect(() => validateRegexPattern('(a*)*')).toThrow(RegexSecurityError);
    });

    it('should reject (.+)+ pattern', () => {
      expect(() => validateRegexPattern('(.+)+')).toThrow(RegexSecurityError);
    });

    it('should reject (.*)+  pattern', () => {
      expect(() => validateRegexPattern('(.*)+')).toThrow(RegexSecurityError);
    });

    it('should reject (.+)* pattern', () => {
      expect(() => validateRegexPattern('(.+)*')).toThrow(RegexSecurityError);
    });

    it('should reject (.*)*  pattern', () => {
      expect(() => validateRegexPattern('(.*)*')).toThrow(RegexSecurityError);
    });

    it('should reject nested quantifiers with character classes ([a-z]+)+', () => {
      expect(() => validateRegexPattern('([a-z]+)+')).toThrow(RegexSecurityError);
    });

    it('should reject ([a-zA-Z0-9]+)+ pattern', () => {
      expect(() => validateRegexPattern('([a-zA-Z0-9]+)+')).toThrow(RegexSecurityError);
    });

    it('should reject quantifier on quantified group (a{1,})+', () => {
      expect(() => validateRegexPattern('(a{1,})+')).toThrow(RegexSecurityError);
    });

    it('should reject quantifier on quantified group (a{2,5})*', () => {
      expect(() => validateRegexPattern('(a{2,5})*')).toThrow(RegexSecurityError);
    });

    it('should reject dangerous RegExp objects', () => {
      expect(() => validateRegexPattern(/(a+)+/)).toThrow(RegexSecurityError);
      expect(() => validateRegexPattern(/(.+)+/)).toThrow(RegexSecurityError);
    });
  });

  describe('safe pattern acceptance', () => {
    it('should accept simple patterns', () => {
      expect(() => validateRegexPattern('abc')).not.toThrow();
      expect(() => validateRegexPattern('^hello')).not.toThrow();
      expect(() => validateRegexPattern('world$')).not.toThrow();
    });

    it('should accept patterns with single quantifiers', () => {
      expect(() => validateRegexPattern('a+')).not.toThrow();
      expect(() => validateRegexPattern('a*')).not.toThrow();
      expect(() => validateRegexPattern('a?')).not.toThrow();
      expect(() => validateRegexPattern('a{1,3}')).not.toThrow();
    });

    it('should accept patterns with non-nested groups', () => {
      expect(() => validateRegexPattern('(abc)+')).not.toThrow();
      expect(() => validateRegexPattern('(hello|world)')).not.toThrow();
      expect(() => validateRegexPattern('(foo)(bar)')).not.toThrow();
    });

    it('should accept character class patterns', () => {
      expect(() => validateRegexPattern('[a-z]+')).not.toThrow();
      expect(() => validateRegexPattern('[0-9]{3}')).not.toThrow();
      expect(() => validateRegexPattern('[^abc]')).not.toThrow();
    });

    it('should accept email-like patterns', () => {
      expect(() => validateRegexPattern('^[a-z]+@[a-z]+\\.[a-z]+$')).not.toThrow();
    });

    it('should accept common safe patterns', () => {
      expect(() => validateRegexPattern('^\\d{3}-\\d{4}$')).not.toThrow(); // phone
      expect(() => validateRegexPattern('^[A-Z]{2}\\d{6}$')).not.toThrow(); // ID format
      expect(() => validateRegexPattern('\\b\\w+\\b')).not.toThrow(); // word boundary
    });

    it('should accept non-string/non-RegExp values (validation passes, matching fails)', () => {
      expect(() => validateRegexPattern(123)).not.toThrow();
      expect(() => validateRegexPattern(null)).not.toThrow();
      expect(() => validateRegexPattern(undefined)).not.toThrow();
    });
  });
});

describe('matchesFilter - $regex Security', () => {
  it('should throw RegexSecurityError for dangerous patterns in filter', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
    expect(() => matchesFilter(doc, { name: { $regex: '(a+)+' } })).toThrow(RegexSecurityError);
  });

  it('should throw RegexSecurityError for patterns exceeding length limit', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
    const longPattern = 'a'.repeat(MAX_REGEX_PATTERN_LENGTH + 1);
    expect(() => matchesFilter(doc, { name: { $regex: longPattern } })).toThrow(RegexSecurityError);
  });

  it('should throw RegexSecurityError for dangerous RegExp objects in filter', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
    expect(() => matchesFilter(doc, { name: { $regex: /(a+)+/ } })).toThrow(RegexSecurityError);
  });

  it('should still match safe patterns correctly', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
    expect(matchesFilter(doc, { name: { $regex: '^A' } })).toBe(true);
    expect(matchesFilter(doc, { name: { $regex: '^B' } })).toBe(false);
  });

  it('should still support case-insensitive regex matching', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
    expect(matchesFilter(doc, { name: { $regex: /alice/i } })).toBe(true);
  });

  it('should throw for nested quantifiers with $not operator', () => {
    const doc: TestDocument = { _id: 'doc1', name: 'Alice' };
    expect(() => matchesFilter(doc, { name: { $not: { $regex: '(a+)+' } } })).toThrow(
      RegexSecurityError
    );
  });

  it('should handle non-string field values without throwing', () => {
    const doc: TestDocument = { _id: 'doc1', age: 30 };
    // Should not throw - pattern validation happens but matching returns false for non-strings
    expect(matchesFilter(doc, { age: { $regex: '^3' } })).toBe(false);
  });
});
