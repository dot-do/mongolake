/**
 * Comprehensive tests for the validation module
 *
 * Tests all validation functions for MongoDB-compatible input validation.
 */

import { describe, it, expect } from 'vitest';
import {
  ValidationError,
  validateDatabaseName,
  validateCollectionName,
  validateFieldName,
  validateFilter,
  validateProjection,
  validateUpdate,
  validateDocument,
  validateAggregationPipeline,
  validateInputs,
  VALID_QUERY_OPERATORS,
  VALID_UPDATE_OPERATORS,
  VALID_AGGREGATION_STAGES,
} from '../../../src/validation/index.js';

// ============================================================================
// ValidationError Tests
// ============================================================================

describe('ValidationError', () => {
  it('should be an instance of Error', () => {
    const error = new ValidationError('test message', 'test_type');
    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(ValidationError);
  });

  it('should have correct name property', () => {
    const error = new ValidationError('test message', 'test_type');
    expect(error.name).toBe('ValidationError');
  });

  it('should include validation type', () => {
    const error = new ValidationError('test message', 'database_name');
    expect(error.validationType).toBe('database_name');
  });

  it('should sanitize invalid value for logging', () => {
    const longValue = 'a'.repeat(200);
    const error = new ValidationError('test', 'test', { invalidValue: longValue });
    expect(error.invalidValue?.length).toBeLessThanOrEqual(103); // 100 + '...'
  });

  it('should serialize to JSON', () => {
    const error = new ValidationError('test message', 'test_type', {
      invalidValue: 'bad',
      context: { field: 'name' },
    });
    const json = error.toJSON();
    expect(json.name).toBe('ValidationError');
    expect(json.message).toBe('test message');
    expect(json.validationType).toBe('test_type');
    expect(json.invalidValue).toBe('bad');
    expect(json.context).toEqual({ field: 'name' });
  });
});

// ============================================================================
// Database Name Validation Tests
// ============================================================================

describe('validateDatabaseName', () => {
  describe('valid names', () => {
    it('should accept alphanumeric names', () => {
      expect(() => validateDatabaseName('mydb')).not.toThrow();
      expect(() => validateDatabaseName('database1')).not.toThrow();
      expect(() => validateDatabaseName('DB123')).not.toThrow();
    });

    it('should accept names with underscores', () => {
      expect(() => validateDatabaseName('my_database')).not.toThrow();
      expect(() => validateDatabaseName('user_data_v2')).not.toThrow();
    });

    it('should accept names with hyphens', () => {
      expect(() => validateDatabaseName('my-database')).not.toThrow();
      expect(() => validateDatabaseName('prod-db-v1')).not.toThrow();
    });

    it('should accept single character names', () => {
      expect(() => validateDatabaseName('a')).not.toThrow();
      expect(() => validateDatabaseName('Z')).not.toThrow();
      expect(() => validateDatabaseName('1')).not.toThrow();
    });

    it('should accept names at max length (120)', () => {
      expect(() => validateDatabaseName('a'.repeat(120))).not.toThrow();
    });
  });

  describe('invalid names', () => {
    it('should reject empty string', () => {
      expect(() => validateDatabaseName('')).toThrow(ValidationError);
      expect(() => validateDatabaseName('')).toThrow(/cannot be empty/);
    });

    it('should reject names exceeding max length', () => {
      expect(() => validateDatabaseName('a'.repeat(121))).toThrow(ValidationError);
      expect(() => validateDatabaseName('a'.repeat(121))).toThrow(/exceeds maximum length/);
    });

    it('should reject names with null bytes', () => {
      expect(() => validateDatabaseName('db\0name')).toThrow(ValidationError);
      expect(() => validateDatabaseName('db\0name')).toThrow(/null bytes/);
    });

    it('should reject names with dots', () => {
      expect(() => validateDatabaseName('my.db')).toThrow(ValidationError);
      expect(() => validateDatabaseName('..')).toThrow(ValidationError);
    });

    it('should reject names with slashes', () => {
      expect(() => validateDatabaseName('path/to/db')).toThrow(ValidationError);
      expect(() => validateDatabaseName('..\\parent')).toThrow(ValidationError);
    });

    it('should reject names starting with underscore or hyphen', () => {
      expect(() => validateDatabaseName('_hidden')).toThrow(ValidationError);
      expect(() => validateDatabaseName('-invalid')).toThrow(ValidationError);
    });

    it('should reject non-string inputs', () => {
      expect(() => validateDatabaseName(null as unknown as string)).toThrow(ValidationError);
      expect(() => validateDatabaseName(undefined as unknown as string)).toThrow(ValidationError);
      expect(() => validateDatabaseName(123 as unknown as string)).toThrow(ValidationError);
    });
  });
});

// ============================================================================
// Collection Name Validation Tests
// ============================================================================

describe('validateCollectionName', () => {
  describe('valid names', () => {
    it('should accept alphanumeric names', () => {
      expect(() => validateCollectionName('users')).not.toThrow();
      expect(() => validateCollectionName('Products123')).not.toThrow();
    });

    it('should accept names with underscores and hyphens', () => {
      expect(() => validateCollectionName('user_profiles')).not.toThrow();
      expect(() => validateCollectionName('user-data')).not.toThrow();
    });
  });

  describe('invalid names', () => {
    it('should reject empty string', () => {
      expect(() => validateCollectionName('')).toThrow(ValidationError);
    });

    it('should reject names with $ character', () => {
      expect(() => validateCollectionName('my$collection')).toThrow(ValidationError);
      expect(() => validateCollectionName('my$collection')).toThrow(/\$ character/);
    });

    it('should reject names with system. prefix', () => {
      expect(() => validateCollectionName('system.users')).toThrow(ValidationError);
      expect(() => validateCollectionName('system.users')).toThrow(/system\./);
    });

    it('should reject path traversal attempts', () => {
      expect(() => validateCollectionName('../etc/passwd')).toThrow(ValidationError);
      expect(() => validateCollectionName('../../secret')).toThrow(ValidationError);
    });
  });
});

// ============================================================================
// Field Name Validation Tests
// ============================================================================

describe('validateFieldName', () => {
  describe('valid names', () => {
    it('should accept normal field names', () => {
      expect(() => validateFieldName('name')).not.toThrow();
      expect(() => validateFieldName('user_id')).not.toThrow();
      expect(() => validateFieldName('field123')).not.toThrow();
    });

    it('should accept _id field', () => {
      expect(() => validateFieldName('_id')).not.toThrow();
    });
  });

  describe('invalid names', () => {
    it('should reject empty field names', () => {
      expect(() => validateFieldName('')).toThrow(ValidationError);
    });

    it('should reject field names with null bytes', () => {
      expect(() => validateFieldName('field\0name')).toThrow(ValidationError);
    });

    it('should reject field names starting with $', () => {
      expect(() => validateFieldName('$field')).toThrow(ValidationError);
    });

    it('should allow $ prefix when operators are permitted', () => {
      expect(() => validateFieldName('$set', true)).not.toThrow();
      expect(() => validateFieldName('$gt', true)).not.toThrow();
    });
  });
});

// ============================================================================
// Filter Validation Tests
// ============================================================================

describe('validateFilter', () => {
  describe('valid filters', () => {
    it('should accept empty filter', () => {
      expect(() => validateFilter({})).not.toThrow();
    });

    it('should accept simple equality filters', () => {
      expect(() => validateFilter({ name: 'Alice' })).not.toThrow();
      expect(() => validateFilter({ age: 30 })).not.toThrow();
      expect(() => validateFilter({ active: true })).not.toThrow();
    });

    it('should accept comparison operators', () => {
      expect(() => validateFilter({ age: { $gt: 18 } })).not.toThrow();
      expect(() => validateFilter({ age: { $gte: 18 } })).not.toThrow();
      expect(() => validateFilter({ age: { $lt: 100 } })).not.toThrow();
      expect(() => validateFilter({ age: { $lte: 100 } })).not.toThrow();
      expect(() => validateFilter({ name: { $eq: 'Alice' } })).not.toThrow();
      expect(() => validateFilter({ name: { $ne: 'Bob' } })).not.toThrow();
    });

    it('should accept array operators', () => {
      expect(() => validateFilter({ status: { $in: ['active', 'pending'] } })).not.toThrow();
      expect(() => validateFilter({ status: { $nin: ['deleted'] } })).not.toThrow();
    });

    it('should accept logical operators', () => {
      expect(() => validateFilter({ $and: [{ a: 1 }, { b: 2 }] })).not.toThrow();
      expect(() => validateFilter({ $or: [{ a: 1 }, { b: 2 }] })).not.toThrow();
      expect(() => validateFilter({ $nor: [{ a: 1 }] })).not.toThrow();
    });

    it('should accept $not operator', () => {
      expect(() => validateFilter({ age: { $not: { $lt: 18 } } })).not.toThrow();
    });

    it('should accept element operators', () => {
      expect(() => validateFilter({ field: { $exists: true } })).not.toThrow();
      expect(() => validateFilter({ field: { $type: 'string' } })).not.toThrow();
    });

    it('should accept $regex operator', () => {
      expect(() => validateFilter({ name: { $regex: '^A' } })).not.toThrow();
    });

    it('should accept nested filters', () => {
      expect(() =>
        validateFilter({
          $and: [
            { age: { $gte: 18 } },
            { $or: [{ status: 'active' }, { role: 'admin' }] },
          ],
        })
      ).not.toThrow();
    });

    it('should accept null values', () => {
      expect(() => validateFilter({ field: null })).not.toThrow();
      expect(() => validateFilter(null)).not.toThrow();
    });
  });

  describe('invalid filters', () => {
    it('should reject invalid operators', () => {
      expect(() => validateFilter({ field: { $badOp: 1 } })).toThrow(ValidationError);
      expect(() => validateFilter({ field: { $badOp: 1 } })).toThrow(/invalid query operator/);
    });

    it('should reject $where operator by default', () => {
      expect(() => validateFilter({ $where: 'this.a > 1' })).toThrow(ValidationError);
      expect(() => validateFilter({ $where: 'this.a > 1' })).toThrow(/\$where.*not allowed/);
    });

    it('should allow $where when explicitly permitted', () => {
      expect(() =>
        validateFilter({ $where: 'this.a > 1' }, { allowWhere: true })
      ).not.toThrow();
    });

    it('should reject excessive nesting depth', () => {
      let filter: Record<string, unknown> = { a: 1 };
      for (let i = 0; i < 15; i++) {
        filter = { $and: [filter] };
      }
      expect(() => validateFilter(filter, { maxDepth: 10 })).toThrow(ValidationError);
      expect(() => validateFilter(filter, { maxDepth: 10 })).toThrow(/maximum nesting depth/);
    });

    it('should reject too many operators', () => {
      const filter: Record<string, unknown> = {};
      for (let i = 0; i < 110; i++) {
        filter[`field${i}`] = { $eq: i };
      }
      expect(() => validateFilter(filter, { maxOperators: 100 })).toThrow(ValidationError);
    });

    it('should reject logical operators without arrays', () => {
      expect(() => validateFilter({ $and: { a: 1 } })).toThrow(ValidationError);
      expect(() => validateFilter({ $or: 'invalid' })).toThrow(ValidationError);
    });

    it('should reject $in/$nin with non-array values', () => {
      expect(() => validateFilter({ status: { $in: 'active' } })).toThrow(ValidationError);
      expect(() => validateFilter({ status: { $nin: 123 } })).toThrow(ValidationError);
    });
  });
});

// ============================================================================
// Projection Validation Tests
// ============================================================================

describe('validateProjection', () => {
  describe('valid projections', () => {
    it('should accept null/undefined', () => {
      expect(() => validateProjection(null)).not.toThrow();
      expect(() => validateProjection(undefined)).not.toThrow();
    });

    it('should accept inclusion projection', () => {
      expect(() => validateProjection({ name: 1, age: 1 })).not.toThrow();
    });

    it('should accept exclusion projection', () => {
      expect(() => validateProjection({ password: 0, secret: 0 })).not.toThrow();
    });

    it('should allow _id exclusion with inclusion', () => {
      expect(() => validateProjection({ _id: 0, name: 1 })).not.toThrow();
    });

    it('should accept boolean values', () => {
      expect(() => validateProjection({ name: true, age: true })).not.toThrow();
      expect(() => validateProjection({ password: false })).not.toThrow();
    });

    it('should accept projection operators', () => {
      expect(() => validateProjection({ items: { $slice: 5 } })).not.toThrow();
      expect(() => validateProjection({ tags: { $elemMatch: { active: true } } })).not.toThrow();
    });
  });

  describe('invalid projections', () => {
    it('should reject non-object projections', () => {
      expect(() => validateProjection('name')).toThrow(ValidationError);
      expect(() => validateProjection(['name'])).toThrow(ValidationError);
    });

    it('should reject invalid values', () => {
      expect(() => validateProjection({ name: 2 })).toThrow(ValidationError);
      expect(() => validateProjection({ name: -1 })).toThrow(ValidationError);
    });

    it('should reject mixed inclusion and exclusion', () => {
      expect(() => validateProjection({ name: 1, age: 0 })).toThrow(ValidationError);
      expect(() => validateProjection({ name: 1, age: 0 })).toThrow(/cannot mix/);
    });

    it('should reject invalid projection operators', () => {
      expect(() => validateProjection({ name: { $badOp: 1 } })).toThrow(ValidationError);
    });
  });
});

// ============================================================================
// Update Validation Tests
// ============================================================================

describe('validateUpdate', () => {
  describe('valid updates', () => {
    it('should accept $set operator', () => {
      expect(() => validateUpdate({ $set: { name: 'Alice' } })).not.toThrow();
    });

    it('should accept $unset operator', () => {
      expect(() => validateUpdate({ $unset: { temp: '' } })).not.toThrow();
    });

    it('should accept $inc operator', () => {
      expect(() => validateUpdate({ $inc: { count: 1 } })).not.toThrow();
      expect(() => validateUpdate({ $inc: { balance: -5.5 } })).not.toThrow();
    });

    it('should accept $mul operator', () => {
      expect(() => validateUpdate({ $mul: { price: 1.1 } })).not.toThrow();
    });

    it('should accept array operators', () => {
      expect(() => validateUpdate({ $push: { tags: 'new' } })).not.toThrow();
      expect(() => validateUpdate({ $pull: { tags: 'old' } })).not.toThrow();
      expect(() => validateUpdate({ $addToSet: { tags: 'unique' } })).not.toThrow();
      expect(() => validateUpdate({ $pop: { array: 1 } })).not.toThrow();
      expect(() => validateUpdate({ $pop: { array: -1 } })).not.toThrow();
    });

    it('should accept $push with $each modifier', () => {
      expect(() =>
        validateUpdate({ $push: { tags: { $each: ['a', 'b'] } } })
      ).not.toThrow();
    });

    it('should accept $rename operator', () => {
      expect(() => validateUpdate({ $rename: { oldName: 'newName' } })).not.toThrow();
    });

    it('should accept multiple operators', () => {
      expect(() =>
        validateUpdate({
          $set: { name: 'Bob' },
          $inc: { version: 1 },
          $push: { history: 'updated' },
        })
      ).not.toThrow();
    });

    it('should accept nested field updates', () => {
      expect(() => validateUpdate({ $set: { 'address.city': 'NYC' } })).not.toThrow();
    });

    it('should accept positional operator', () => {
      expect(() => validateUpdate({ $set: { 'items.$.price': 100 } })).not.toThrow();
    });
  });

  describe('invalid updates', () => {
    it('should reject null or undefined', () => {
      expect(() => validateUpdate(null)).toThrow(ValidationError);
      expect(() => validateUpdate(undefined)).toThrow(ValidationError);
    });

    it('should reject empty object', () => {
      expect(() => validateUpdate({})).toThrow(ValidationError);
    });

    it('should reject updates without operators', () => {
      expect(() => validateUpdate({ name: 'Alice' })).toThrow(ValidationError);
      expect(() => validateUpdate({ name: 'Alice' })).toThrow(/must use operators/);
    });

    it('should reject invalid operators', () => {
      expect(() => validateUpdate({ $badOp: { a: 1 } })).toThrow(ValidationError);
      expect(() => validateUpdate({ $badOp: { a: 1 } })).toThrow(/invalid update operator/);
    });

    it('should reject non-numeric values for $inc', () => {
      expect(() => validateUpdate({ $inc: { count: 'one' } })).toThrow(ValidationError);
      expect(() => validateUpdate({ $inc: { count: 'one' } })).toThrow(/numeric values/);
    });

    it('should reject invalid $pop values', () => {
      expect(() => validateUpdate({ $pop: { array: 0 } })).toThrow(ValidationError);
      expect(() => validateUpdate({ $pop: { array: 2 } })).toThrow(ValidationError);
    });

    it('should reject non-string values for $rename', () => {
      expect(() => validateUpdate({ $rename: { old: 123 } })).toThrow(ValidationError);
    });

    it('should reject invalid array modifiers', () => {
      expect(() =>
        validateUpdate({ $push: { tags: { $badModifier: ['a'] } } })
      ).toThrow(ValidationError);
    });

    it('should reject empty field name segments', () => {
      expect(() => validateUpdate({ $set: { 'a..b': 1 } })).toThrow(ValidationError);
    });
  });
});

// ============================================================================
// Document Validation Tests
// ============================================================================

describe('validateDocument', () => {
  describe('valid documents', () => {
    it('should accept simple documents', () => {
      expect(() => validateDocument({ name: 'Alice', age: 30 })).not.toThrow();
    });

    it('should accept documents with _id', () => {
      expect(() => validateDocument({ _id: '123', name: 'Alice' })).not.toThrow();
    });

    it('should accept nested documents', () => {
      expect(() =>
        validateDocument({
          name: 'Alice',
          address: { city: 'NYC', zip: '10001' },
        })
      ).not.toThrow();
    });

    it('should accept arrays', () => {
      expect(() =>
        validateDocument({ tags: ['a', 'b'], items: [{ id: 1 }, { id: 2 }] })
      ).not.toThrow();
    });
  });

  describe('invalid documents', () => {
    it('should reject null or undefined', () => {
      expect(() => validateDocument(null)).toThrow(ValidationError);
      expect(() => validateDocument(undefined)).toThrow(ValidationError);
    });

    it('should reject non-objects', () => {
      expect(() => validateDocument('string')).toThrow(ValidationError);
      expect(() => validateDocument(123)).toThrow(ValidationError);
      expect(() => validateDocument(['array'])).toThrow(ValidationError);
    });

    it('should reject field names starting with $', () => {
      expect(() => validateDocument({ $field: 'value' })).toThrow(ValidationError);
    });

    it('should reject field names with null bytes', () => {
      expect(() => validateDocument({ 'field\0name': 'value' })).toThrow(ValidationError);
    });

    it('should reject excessive nesting', () => {
      let doc: Record<string, unknown> = { value: 'leaf' };
      for (let i = 0; i < 150; i++) {
        doc = { nested: doc };
      }
      expect(() => validateDocument(doc, { maxDepth: 100 })).toThrow(ValidationError);
    });

    it('should require _id when specified', () => {
      expect(() => validateDocument({ name: 'Alice' }, { requireId: true })).toThrow(
        ValidationError
      );
    });
  });
});

// ============================================================================
// Aggregation Pipeline Validation Tests
// ============================================================================

describe('validateAggregationPipeline', () => {
  describe('valid pipelines', () => {
    it('should accept $match stage', () => {
      expect(() => validateAggregationPipeline([{ $match: { active: true } }])).not.toThrow();
    });

    it('should accept $project stage', () => {
      expect(() => validateAggregationPipeline([{ $project: { name: 1 } }])).not.toThrow();
    });

    it('should accept $group stage', () => {
      expect(() =>
        validateAggregationPipeline([{ $group: { _id: '$category', count: { $sum: 1 } } }])
      ).not.toThrow();
    });

    it('should accept $sort stage', () => {
      expect(() => validateAggregationPipeline([{ $sort: { name: 1 } }])).not.toThrow();
      expect(() => validateAggregationPipeline([{ $sort: { age: -1 } }])).not.toThrow();
    });

    it('should accept $limit and $skip stages', () => {
      expect(() => validateAggregationPipeline([{ $limit: 10 }])).not.toThrow();
      expect(() => validateAggregationPipeline([{ $skip: 5 }])).not.toThrow();
    });

    it('should accept $unwind stage', () => {
      expect(() => validateAggregationPipeline([{ $unwind: '$items' }])).not.toThrow();
    });

    it('should accept $lookup stage', () => {
      expect(() =>
        validateAggregationPipeline([
          {
            $lookup: {
              from: 'orders',
              localField: 'userId',
              foreignField: 'customerId',
              as: 'orders',
            },
          },
        ])
      ).not.toThrow();
    });

    it('should accept $addFields and $set stages', () => {
      expect(() =>
        validateAggregationPipeline([{ $addFields: { total: '$price' } }])
      ).not.toThrow();
      expect(() => validateAggregationPipeline([{ $set: { status: 'active' } }])).not.toThrow();
    });

    it('should accept $count stage', () => {
      expect(() => validateAggregationPipeline([{ $count: 'totalDocs' }])).not.toThrow();
    });

    it('should accept multi-stage pipelines', () => {
      expect(() =>
        validateAggregationPipeline([
          { $match: { active: true } },
          { $group: { _id: '$category', count: { $sum: 1 } } },
          { $sort: { count: -1 } },
          { $limit: 10 },
        ])
      ).not.toThrow();
    });
  });

  describe('invalid pipelines', () => {
    it('should reject non-array pipelines', () => {
      expect(() => validateAggregationPipeline({ $match: {} })).toThrow(ValidationError);
      expect(() => validateAggregationPipeline('pipeline')).toThrow(ValidationError);
    });

    it('should reject empty pipelines', () => {
      expect(() => validateAggregationPipeline([])).toThrow(ValidationError);
    });

    it('should reject invalid stage operators', () => {
      expect(() => validateAggregationPipeline([{ $badStage: {} }])).toThrow(ValidationError);
    });

    it('should reject stages with multiple keys', () => {
      expect(() =>
        validateAggregationPipeline([{ $match: {}, $sort: {} }])
      ).toThrow(ValidationError);
    });

    it('should reject non-object stages', () => {
      expect(() => validateAggregationPipeline(['$match'])).toThrow(ValidationError);
      expect(() => validateAggregationPipeline([null])).toThrow(ValidationError);
    });

    it('should reject invalid $limit/$skip values', () => {
      expect(() => validateAggregationPipeline([{ $limit: -1 }])).toThrow(ValidationError);
      expect(() => validateAggregationPipeline([{ $skip: 1.5 }])).toThrow(ValidationError);
      expect(() => validateAggregationPipeline([{ $limit: 'ten' }])).toThrow(ValidationError);
    });

    it('should reject invalid $sort values', () => {
      expect(() => validateAggregationPipeline([{ $sort: 'name' }])).toThrow(ValidationError);
      expect(() => validateAggregationPipeline([{ $sort: { name: 2 } }])).toThrow(ValidationError);
    });

    it('should reject too many stages', () => {
      const pipeline = Array(110).fill({ $match: {} });
      expect(() => validateAggregationPipeline(pipeline, { maxStages: 100 })).toThrow(
        ValidationError
      );
    });
  });
});

// ============================================================================
// validateInputs Tests
// ============================================================================

describe('validateInputs', () => {
  it('should validate all provided inputs', () => {
    expect(() =>
      validateInputs({
        database: 'mydb',
        collection: 'users',
        filter: { active: true },
        projection: { name: 1 },
        update: { $set: { status: 'active' } },
        document: { name: 'Alice' },
        pipeline: [{ $match: {} }],
      })
    ).not.toThrow();
  });

  it('should throw on invalid database', () => {
    expect(() => validateInputs({ database: '' })).toThrow(ValidationError);
  });

  it('should throw on invalid collection', () => {
    expect(() => validateInputs({ collection: 'system.users' })).toThrow(ValidationError);
  });

  it('should throw on invalid filter', () => {
    expect(() => validateInputs({ filter: { $badOp: 1 } })).toThrow(ValidationError);
  });

  it('should skip undefined inputs', () => {
    expect(() => validateInputs({})).not.toThrow();
    expect(() => validateInputs({ database: 'mydb' })).not.toThrow();
  });
});

// ============================================================================
// Constants Tests
// ============================================================================

describe('Constants', () => {
  it('should export valid query operators', () => {
    expect(VALID_QUERY_OPERATORS).toContain('$eq');
    expect(VALID_QUERY_OPERATORS).toContain('$gt');
    expect(VALID_QUERY_OPERATORS).toContain('$in');
    expect(VALID_QUERY_OPERATORS).toContain('$and');
    expect(VALID_QUERY_OPERATORS).toContain('$or');
    expect(VALID_QUERY_OPERATORS).toContain('$exists');
    expect(VALID_QUERY_OPERATORS).toContain('$regex');
  });

  it('should export valid update operators', () => {
    expect(VALID_UPDATE_OPERATORS).toContain('$set');
    expect(VALID_UPDATE_OPERATORS).toContain('$unset');
    expect(VALID_UPDATE_OPERATORS).toContain('$inc');
    expect(VALID_UPDATE_OPERATORS).toContain('$push');
    expect(VALID_UPDATE_OPERATORS).toContain('$pull');
    expect(VALID_UPDATE_OPERATORS).toContain('$addToSet');
  });

  it('should export valid aggregation stages', () => {
    expect(VALID_AGGREGATION_STAGES).toContain('$match');
    expect(VALID_AGGREGATION_STAGES).toContain('$project');
    expect(VALID_AGGREGATION_STAGES).toContain('$group');
    expect(VALID_AGGREGATION_STAGES).toContain('$sort');
    expect(VALID_AGGREGATION_STAGES).toContain('$lookup');
  });
});

// ============================================================================
// Performance Tests
// ============================================================================

describe('Validation Performance', () => {
  it('should validate filters quickly', () => {
    const filter = {
      $and: [
        { age: { $gte: 18, $lte: 65 } },
        { $or: [{ status: 'active' }, { role: { $in: ['admin', 'moderator'] } }] },
      ],
    };

    const start = performance.now();
    for (let i = 0; i < 1000; i++) {
      validateFilter(filter);
    }
    const elapsed = performance.now() - start;

    // Should complete 1000 validations in under 100ms
    expect(elapsed).toBeLessThan(100);
  });

  it('should validate updates quickly', () => {
    const update = {
      $set: { name: 'Alice', 'address.city': 'NYC' },
      $inc: { version: 1 },
      $push: { history: { $each: ['a', 'b', 'c'] } },
    };

    const start = performance.now();
    for (let i = 0; i < 1000; i++) {
      validateUpdate(update);
    }
    const elapsed = performance.now() - start;

    expect(elapsed).toBeLessThan(100);
  });

  it('should validate documents quickly', () => {
    const doc = {
      name: 'Alice',
      age: 30,
      address: { city: 'NYC', zip: '10001' },
      tags: ['a', 'b', 'c'],
      metadata: { created: new Date(), updated: new Date() },
    };

    const start = performance.now();
    for (let i = 0; i < 1000; i++) {
      validateDocument(doc);
    }
    const elapsed = performance.now() - start;

    expect(elapsed).toBeLessThan(100);
  });
});
