/**
 * Validation Error Scenario Tests
 *
 * Comprehensive tests for validation error handling:
 * - Invalid filter operators
 * - Invalid update operators
 * - Schema validation failures
 *
 * These tests verify that validation errors are properly thrown with
 * informative error messages that help users understand and fix their inputs.
 */

import { describe, it, expect } from 'vitest';
import {
  ValidationError,
  validateFilter,
  validateUpdate,
  validateDocument,
  validateProjection,
  validateAggregationPipeline,
  validateDatabaseName,
  validateCollectionName,
  validateFieldName,
} from '../../../src/validation/index.js';

// ============================================================================
// Invalid Filter Operator Tests
// ============================================================================

describe('Invalid Filter Operators', () => {
  describe('unknown operators', () => {
    it('should throw ValidationError for unknown comparison operators', () => {
      expect(() => validateFilter({ age: { $greaterThan: 18 } })).toThrow(ValidationError);
      expect(() => validateFilter({ age: { $greaterThan: 18 } })).toThrow(/invalid query operator/);
      expect(() => validateFilter({ age: { $greaterThan: 18 } })).toThrow(/\$greaterThan/);
    });

    it('should throw ValidationError for unknown logical operators', () => {
      expect(() => validateFilter({ $both: [{ a: 1 }, { b: 2 }] })).toThrow(ValidationError);
      expect(() => validateFilter({ $both: [{ a: 1 }, { b: 2 }] })).toThrow(/invalid query operator/);
    });

    it('should throw ValidationError for typos in common operators', () => {
      expect(() => validateFilter({ field: { $equals: 'value' } })).toThrow(ValidationError);
      expect(() => validateFilter({ field: { $equals: 'value' } })).toThrow(/invalid query operator/);

      expect(() => validateFilter({ field: { $inArray: [1, 2] } })).toThrow(ValidationError);
      expect(() => validateFilter({ field: { $notIn: [1, 2] } })).toThrow(ValidationError);
    });

    it('should throw ValidationError with helpful message for common mistakes', () => {
      const error = getValidationError(() => validateFilter({ field: { $gte1: 10 } }));
      expect(error).toBeInstanceOf(ValidationError);
      expect(error?.message).toContain('invalid query operator');
      expect(error?.message).toContain('$gte1');
    });
  });

  describe('incorrect operator usage', () => {
    it('should throw ValidationError when $in is not given an array', () => {
      expect(() => validateFilter({ status: { $in: 'active' } })).toThrow(ValidationError);
      expect(() => validateFilter({ status: { $in: 'active' } })).toThrow(/requires an array/);
    });

    it('should throw ValidationError when $nin is not given an array', () => {
      expect(() => validateFilter({ status: { $nin: 123 } })).toThrow(ValidationError);
      expect(() => validateFilter({ status: { $nin: 123 } })).toThrow(/requires an array/);
    });

    it('should throw ValidationError when $and is not given an array', () => {
      expect(() => validateFilter({ $and: { a: 1 } })).toThrow(ValidationError);
      expect(() => validateFilter({ $and: { a: 1 } })).toThrow(/requires an array/);
    });

    it('should throw ValidationError when $or is not given an array', () => {
      expect(() => validateFilter({ $or: 'condition' })).toThrow(ValidationError);
    });

    it('should throw ValidationError when $nor is not given an array', () => {
      expect(() => validateFilter({ $nor: { a: 1 } })).toThrow(ValidationError);
    });
  });

  describe('security-related operator errors', () => {
    it('should throw ValidationError for $where operator by default', () => {
      expect(() => validateFilter({ $where: 'this.a > 1' })).toThrow(ValidationError);
      expect(() => validateFilter({ $where: 'this.a > 1' })).toThrow(/not allowed/);
      expect(() => validateFilter({ $where: 'this.a > 1' })).toThrow(/security/);
    });

    it('should throw ValidationError for nested $where', () => {
      expect(() =>
        validateFilter({ $and: [{ $where: 'this.x > 0' }] })
      ).toThrow(ValidationError);
    });

    it('should allow $where when explicitly enabled', () => {
      expect(() =>
        validateFilter({ $where: 'this.a > 1' }, { allowWhere: true })
      ).not.toThrow();
    });
  });

  describe('nesting and complexity errors', () => {
    it('should throw ValidationError for excessive nesting depth', () => {
      let filter: Record<string, unknown> = { a: 1 };
      for (let i = 0; i < 15; i++) {
        filter = { $and: [filter] };
      }

      expect(() => validateFilter(filter, { maxDepth: 10 })).toThrow(ValidationError);
      expect(() => validateFilter(filter, { maxDepth: 10 })).toThrow(/maximum nesting depth/);
    });

    it('should throw ValidationError for too many operators', () => {
      const filter: Record<string, unknown> = {};
      for (let i = 0; i < 110; i++) {
        filter[`field${i}`] = { $eq: i };
      }

      expect(() => validateFilter(filter, { maxOperators: 100 })).toThrow(ValidationError);
      expect(() => validateFilter(filter, { maxOperators: 100 })).toThrow(/maximum operator count/);
    });
  });
});

// ============================================================================
// Invalid Update Operator Tests
// ============================================================================

describe('Invalid Update Operators', () => {
  describe('unknown operators', () => {
    it('should throw ValidationError for unknown update operators', () => {
      expect(() => validateUpdate({ $assign: { name: 'value' } })).toThrow(ValidationError);
      expect(() => validateUpdate({ $assign: { name: 'value' } })).toThrow(/invalid update operator/);
    });

    it('should throw ValidationError for typos in common operators', () => {
      expect(() => validateUpdate({ $setval: { name: 'value' } })).toThrow(ValidationError);
      expect(() => validateUpdate({ $increment: { count: 1 } })).toThrow(ValidationError);
      expect(() => validateUpdate({ $update: { name: 'value' } })).toThrow(ValidationError);
    });

    it('should throw ValidationError with helpful message including operator name', () => {
      const error = getValidationError(() => validateUpdate({ $badOp: { a: 1 } }));
      expect(error).toBeInstanceOf(ValidationError);
      expect(error?.message).toContain('$badOp');
      expect(error?.message).toContain('invalid update operator');
    });
  });

  describe('missing operator errors', () => {
    it('should throw ValidationError when update has no operators', () => {
      expect(() => validateUpdate({ name: 'Alice' })).toThrow(ValidationError);
      expect(() => validateUpdate({ name: 'Alice' })).toThrow(/must use operators/);
    });

    it('should provide hint about using $set', () => {
      const error = getValidationError(() => validateUpdate({ name: 'Alice' }));
      expect(error?.context?.hint).toContain('$set');
    });

    it('should throw ValidationError for empty update object', () => {
      expect(() => validateUpdate({})).toThrow(ValidationError);
      expect(() => validateUpdate({})).toThrow(/cannot be an empty object/);
    });

    it('should throw ValidationError for null update', () => {
      expect(() => validateUpdate(null)).toThrow(ValidationError);
      expect(() => validateUpdate(null)).toThrow(/cannot be null/);
    });
  });

  describe('incorrect operator value types', () => {
    it('should throw ValidationError when $inc has non-numeric value', () => {
      expect(() => validateUpdate({ $inc: { count: 'one' } })).toThrow(ValidationError);
      expect(() => validateUpdate({ $inc: { count: 'one' } })).toThrow(/numeric values/);
    });

    it('should throw ValidationError when $mul has non-numeric value', () => {
      expect(() => validateUpdate({ $mul: { price: '1.5' } })).toThrow(ValidationError);
      expect(() => validateUpdate({ $mul: { price: '1.5' } })).toThrow(/numeric values/);
    });

    it('should throw ValidationError when $pop has invalid value', () => {
      expect(() => validateUpdate({ $pop: { array: 0 } })).toThrow(ValidationError);
      expect(() => validateUpdate({ $pop: { array: 0 } })).toThrow(/1 or -1/);

      expect(() => validateUpdate({ $pop: { array: 2 } })).toThrow(ValidationError);
      expect(() => validateUpdate({ $pop: { array: 'first' } })).toThrow(ValidationError);
    });

    it('should throw ValidationError when $rename has non-string value', () => {
      expect(() => validateUpdate({ $rename: { oldName: 123 } })).toThrow(ValidationError);
      expect(() => validateUpdate({ $rename: { oldName: 123 } })).toThrow(/string value/);
    });

    it('should throw ValidationError when operator value is not an object', () => {
      expect(() => validateUpdate({ $set: 'value' })).toThrow(ValidationError);
      expect(() => validateUpdate({ $set: ['a', 'b'] })).toThrow(ValidationError);
      expect(() => validateUpdate({ $inc: 5 })).toThrow(ValidationError);
    });
  });

  describe('invalid field names in updates', () => {
    it('should throw ValidationError for empty field name segments', () => {
      expect(() => validateUpdate({ $set: { 'a..b': 1 } })).toThrow(ValidationError);
      expect(() => validateUpdate({ $set: { 'a..b': 1 } })).toThrow(/empty segment/);
    });

    it('should throw ValidationError for field names with invalid $ prefix', () => {
      expect(() => validateUpdate({ $set: { '$field': 1 } })).toThrow(ValidationError);
    });

    it('should allow positional operator $ in field names', () => {
      expect(() => validateUpdate({ $set: { 'items.$.price': 100 } })).not.toThrow();
    });
  });

  describe('invalid array modifiers', () => {
    it('should throw ValidationError for unknown array modifiers in $push', () => {
      expect(() =>
        validateUpdate({ $push: { tags: { $badModifier: ['a', 'b'] } } })
      ).toThrow(ValidationError);
      expect(() =>
        validateUpdate({ $push: { tags: { $badModifier: ['a', 'b'] } } })
      ).toThrow(/invalid array modifier/);
    });

    it('should allow valid array modifiers in $push', () => {
      expect(() =>
        validateUpdate({ $push: { tags: { $each: ['a', 'b'] } } })
      ).not.toThrow();

      expect(() =>
        validateUpdate({ $push: { tags: { $each: ['a'], $position: 0 } } })
      ).not.toThrow();
    });
  });
});

// ============================================================================
// Schema Validation Failures Tests
// ============================================================================

describe('Schema Validation Failures', () => {
  describe('document validation errors', () => {
    it('should throw ValidationError for null document', () => {
      expect(() => validateDocument(null)).toThrow(ValidationError);
      expect(() => validateDocument(null)).toThrow(/cannot be null/);
    });

    it('should throw ValidationError for undefined document', () => {
      expect(() => validateDocument(undefined)).toThrow(ValidationError);
    });

    it('should throw ValidationError for non-object documents', () => {
      expect(() => validateDocument('string')).toThrow(ValidationError);
      expect(() => validateDocument('string')).toThrow(/must be an object/);

      expect(() => validateDocument(123)).toThrow(ValidationError);
      expect(() => validateDocument(['array'])).toThrow(ValidationError);
    });

    it('should throw ValidationError for field names starting with $', () => {
      expect(() => validateDocument({ $field: 'value' })).toThrow(ValidationError);
      expect(() => validateDocument({ $field: 'value' })).toThrow(/cannot start with \$/);
    });

    it('should throw ValidationError for field names with null bytes', () => {
      expect(() => validateDocument({ 'field\0name': 'value' })).toThrow(ValidationError);
      expect(() => validateDocument({ 'field\0name': 'value' })).toThrow(/null bytes/);
    });

    it('should throw ValidationError for excessive nesting', () => {
      let doc: Record<string, unknown> = { value: 'leaf' };
      for (let i = 0; i < 150; i++) {
        doc = { nested: doc };
      }

      expect(() => validateDocument(doc, { maxDepth: 100 })).toThrow(ValidationError);
      expect(() => validateDocument(doc, { maxDepth: 100 })).toThrow(/maximum nesting depth/);
    });

    it('should throw ValidationError when _id is required but missing', () => {
      expect(() => validateDocument({ name: 'Alice' }, { requireId: true })).toThrow(ValidationError);
      expect(() => validateDocument({ name: 'Alice' }, { requireId: true })).toThrow(/_id/);
    });
  });

  describe('database name validation errors', () => {
    it('should throw ValidationError for empty database name', () => {
      expect(() => validateDatabaseName('')).toThrow(ValidationError);
      expect(() => validateDatabaseName('')).toThrow(/cannot be empty/);
    });

    it('should throw ValidationError for database name with dots', () => {
      expect(() => validateDatabaseName('my.db')).toThrow(ValidationError);
      expect(() => validateDatabaseName('my.db')).toThrow(/cannot contain dots/);
    });

    it('should throw ValidationError for database name with slashes', () => {
      expect(() => validateDatabaseName('path/to/db')).toThrow(ValidationError);
      expect(() => validateDatabaseName('path/to/db')).toThrow(/cannot contain slashes/);
    });

    it('should throw ValidationError for database name exceeding max length', () => {
      expect(() => validateDatabaseName('a'.repeat(121))).toThrow(ValidationError);
      expect(() => validateDatabaseName('a'.repeat(121))).toThrow(/maximum length/);
    });

    it('should throw ValidationError for database name starting with underscore or hyphen', () => {
      expect(() => validateDatabaseName('_hidden')).toThrow(ValidationError);
      expect(() => validateDatabaseName('-invalid')).toThrow(ValidationError);
    });
  });

  describe('collection name validation errors', () => {
    it('should throw ValidationError for empty collection name', () => {
      expect(() => validateCollectionName('')).toThrow(ValidationError);
      expect(() => validateCollectionName('')).toThrow(/cannot be empty/);
    });

    it('should throw ValidationError for collection name with $', () => {
      expect(() => validateCollectionName('my$collection')).toThrow(ValidationError);
      expect(() => validateCollectionName('my$collection')).toThrow(/\$ character/);
    });

    it('should throw ValidationError for system. prefix', () => {
      expect(() => validateCollectionName('system.users')).toThrow(ValidationError);
      expect(() => validateCollectionName('system.users')).toThrow(/system\./);
    });

    it('should throw ValidationError for path traversal attempts', () => {
      expect(() => validateCollectionName('../etc/passwd')).toThrow(ValidationError);
      expect(() => validateCollectionName('..\\windows')).toThrow(ValidationError);
    });
  });

  describe('field name validation errors', () => {
    it('should throw ValidationError for empty field name', () => {
      expect(() => validateFieldName('')).toThrow(ValidationError);
      expect(() => validateFieldName('')).toThrow(/cannot be empty/);
    });

    it('should throw ValidationError for field name with null bytes', () => {
      expect(() => validateFieldName('field\0')).toThrow(ValidationError);
      expect(() => validateFieldName('field\0')).toThrow(/null bytes/);
    });

    it('should throw ValidationError for field name starting with $', () => {
      expect(() => validateFieldName('$field')).toThrow(ValidationError);
      expect(() => validateFieldName('$field')).toThrow(/cannot start with \$/);
    });

    it('should allow $ prefix when operators are permitted', () => {
      expect(() => validateFieldName('$set', true)).not.toThrow();
      expect(() => validateFieldName('$gt', true)).not.toThrow();
    });
  });

  describe('projection validation errors', () => {
    it('should throw ValidationError for non-object projection', () => {
      expect(() => validateProjection('name')).toThrow(ValidationError);
      expect(() => validateProjection('name')).toThrow(/must be an object/);
    });

    it('should throw ValidationError for invalid projection values', () => {
      expect(() => validateProjection({ name: 2 })).toThrow(ValidationError);
      expect(() => validateProjection({ name: 2 })).toThrow(/must be 0 or 1/);

      expect(() => validateProjection({ name: -1 })).toThrow(ValidationError);
    });

    it('should throw ValidationError for mixed inclusion and exclusion', () => {
      expect(() => validateProjection({ name: 1, age: 0 })).toThrow(ValidationError);
      expect(() => validateProjection({ name: 1, age: 0 })).toThrow(/cannot mix/);
    });

    it('should throw ValidationError for invalid projection operators', () => {
      expect(() => validateProjection({ field: { $badOp: 1 } })).toThrow(ValidationError);
    });
  });

  describe('aggregation pipeline validation errors', () => {
    it('should throw ValidationError for non-array pipeline', () => {
      expect(() => validateAggregationPipeline({ $match: {} })).toThrow(ValidationError);
      expect(() => validateAggregationPipeline({ $match: {} })).toThrow(/must be an array/);
    });

    it('should throw ValidationError for empty pipeline', () => {
      expect(() => validateAggregationPipeline([])).toThrow(ValidationError);
      expect(() => validateAggregationPipeline([])).toThrow(/cannot be empty/);
    });

    it('should throw ValidationError for invalid stage operators', () => {
      expect(() => validateAggregationPipeline([{ $badStage: {} }])).toThrow(ValidationError);
      expect(() => validateAggregationPipeline([{ $badStage: {} }])).toThrow(/invalid aggregation stage/);
    });

    it('should throw ValidationError for stages with multiple keys', () => {
      expect(() =>
        validateAggregationPipeline([{ $match: {}, $sort: {} }])
      ).toThrow(ValidationError);
      expect(() =>
        validateAggregationPipeline([{ $match: {}, $sort: {} }])
      ).toThrow(/exactly one key/);
    });

    it('should throw ValidationError for invalid $limit/$skip values', () => {
      expect(() => validateAggregationPipeline([{ $limit: -1 }])).toThrow(ValidationError);
      expect(() => validateAggregationPipeline([{ $limit: -1 }])).toThrow(/non-negative integer/);

      expect(() => validateAggregationPipeline([{ $skip: 1.5 }])).toThrow(ValidationError);
      expect(() => validateAggregationPipeline([{ $limit: 'ten' }])).toThrow(ValidationError);
    });

    it('should throw ValidationError for invalid $sort values', () => {
      expect(() => validateAggregationPipeline([{ $sort: 'name' }])).toThrow(ValidationError);
      expect(() => validateAggregationPipeline([{ $sort: { name: 2 } }])).toThrow(ValidationError);
      expect(() => validateAggregationPipeline([{ $sort: { name: 2 } }])).toThrow(/1 or -1/);
    });

    it('should throw ValidationError for too many stages', () => {
      const pipeline = Array(110).fill({ $match: {} });
      expect(() => validateAggregationPipeline(pipeline, { maxStages: 100 })).toThrow(ValidationError);
      expect(() => validateAggregationPipeline(pipeline, { maxStages: 100 })).toThrow(/maximum.*stages/);
    });
  });
});

// ============================================================================
// Error Message Quality Tests
// ============================================================================

describe('Validation Error Message Quality', () => {
  it('should include validation type in error', () => {
    const error = getValidationError(() => validateDatabaseName(''));
    expect(error?.validationType).toBe('database_name');
  });

  it('should include invalid value in error when appropriate', () => {
    const error = getValidationError(() => validateFilter({ field: { $badOp: 1 } }));
    expect(error?.invalidValue).toBe('$badOp');
  });

  it('should include context information in error', () => {
    const error = getValidationError(() => validateUpdate({ $inc: { count: 'one' } }));
    expect(error?.context?.operator).toBe('$inc');
  });

  it('should serialize to JSON properly', () => {
    const error = getValidationError(() => validateDatabaseName(''));
    const json = error?.toJSON();
    expect(json).toHaveProperty('name', 'ValidationError');
    expect(json).toHaveProperty('message');
    expect(json).toHaveProperty('validationType');
  });

  it('should truncate very long invalid values', () => {
    const longValue = 'a'.repeat(200);
    const error = new ValidationError('test', 'test', { invalidValue: longValue });
    expect(error.invalidValue!.length).toBeLessThanOrEqual(103); // 100 + '...'
  });

  it('should handle object invalid values safely', () => {
    const error = new ValidationError('test', 'test', { invalidValue: { complex: 'object' } });
    expect(error.invalidValue).toBe('[object]');
  });
});

// ============================================================================
// Helper Functions
// ============================================================================

function getValidationError(fn: () => void): ValidationError | undefined {
  try {
    fn();
    return undefined;
  } catch (e) {
    if (e instanceof ValidationError) {
      return e;
    }
    throw e;
  }
}
