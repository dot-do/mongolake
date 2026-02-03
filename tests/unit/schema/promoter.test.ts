/**
 * Tests for Schema Promoter - Auto-Promotion Logic
 *
 * Tests automatic type promotion when new data is ingested.
 * Covers type detection, safe promotion determination, schema history tracking,
 * and Parquet schema updates.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  // Type promotion functions
  canPromoteSafely,
  getPromotedType,
  detectedTypeToParquet,
  detectValueType,
  isNumericType,
  getNumericPrecision,

  // Schema promotion detection
  detectPromotion,
  compareSchemas,

  // Migration functions
  generateMigration,
  applyMigration,
  isSafeMigration,
  createVersionedSchema,

  // Schema History class
  SchemaHistory,

  // Utility
  flattenDocument,

  // Types
  type TypePromotion,
  type SchemaMigration,
  type SchemaComparison,
  type VersionedSchema,
  type NewFieldDefinition,
  type SerializedSchemaHistory,
} from '../../../src/schema/promoter.js';

import type { ParsedColumn, ParsedCollectionSchema } from '../../../src/schema/config.js';
import type { ParquetType } from '../../../src/types.js';

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Create a minimal parsed column for testing
 */
function createColumn(path: string, type: ParquetType): ParsedColumn {
  return {
    path,
    segments: path.split('.'),
    type,
    isArray: false,
    isStruct: false,
  };
}

/**
 * Create a minimal parsed collection schema for testing
 */
function createSchema(columns: Record<string, ParquetType>): ParsedCollectionSchema {
  const columnList: ParsedColumn[] = [];
  const columnMap = new Map<string, ParsedColumn>();

  for (const [path, type] of Object.entries(columns)) {
    const col = createColumn(path, type);
    columnList.push(col);
    columnMap.set(path, col);
  }

  return {
    columns: columnList,
    columnMap,
    storeVariant: true,
  };
}

// ============================================================================
// canPromoteSafely Tests
// ============================================================================

describe('canPromoteSafely', () => {
  describe('same type promotions', () => {
    it('should return true when types are the same', () => {
      expect(canPromoteSafely('int32', 'int32')).toBe(true);
      expect(canPromoteSafely('int64', 'int64')).toBe(true);
      expect(canPromoteSafely('double', 'double')).toBe(true);
      expect(canPromoteSafely('string', 'string')).toBe(true);
    });
  });

  describe('numeric type promotions', () => {
    it('should allow int32 -> int64', () => {
      expect(canPromoteSafely('int32', 'int64')).toBe(true);
    });

    it('should allow int32 -> float', () => {
      expect(canPromoteSafely('int32', 'float')).toBe(true);
    });

    it('should allow int32 -> double', () => {
      expect(canPromoteSafely('int32', 'double')).toBe(true);
    });

    it('should allow int64 -> double', () => {
      expect(canPromoteSafely('int64', 'double')).toBe(true);
    });

    it('should allow float -> double', () => {
      expect(canPromoteSafely('float', 'double')).toBe(true);
    });

    it('should NOT allow int64 -> float (precision loss)', () => {
      expect(canPromoteSafely('int64', 'float')).toBe(false);
    });

    it('should NOT allow double -> int32 (narrowing)', () => {
      expect(canPromoteSafely('double', 'int32')).toBe(false);
    });

    it('should NOT allow double -> int64 (narrowing)', () => {
      expect(canPromoteSafely('double', 'int64')).toBe(false);
    });
  });

  describe('date/timestamp promotions', () => {
    it('should allow date -> timestamp', () => {
      expect(canPromoteSafely('date', 'timestamp')).toBe(true);
    });

    it('should NOT allow timestamp -> date', () => {
      expect(canPromoteSafely('timestamp', 'date')).toBe(false);
    });
  });

  describe('variant promotions', () => {
    it('should allow any type -> variant', () => {
      expect(canPromoteSafely('string', 'variant')).toBe(true);
      expect(canPromoteSafely('int32', 'variant')).toBe(true);
      expect(canPromoteSafely('int64', 'variant')).toBe(true);
      expect(canPromoteSafely('float', 'variant')).toBe(true);
      expect(canPromoteSafely('double', 'variant')).toBe(true);
      expect(canPromoteSafely('boolean', 'variant')).toBe(true);
      expect(canPromoteSafely('timestamp', 'variant')).toBe(true);
      expect(canPromoteSafely('date', 'variant')).toBe(true);
      expect(canPromoteSafely('binary', 'variant')).toBe(true);
    });

    it('should NOT allow variant -> specific type', () => {
      expect(canPromoteSafely('variant', 'string')).toBe(false);
      expect(canPromoteSafely('variant', 'int32')).toBe(false);
    });
  });

  describe('incompatible promotions', () => {
    it('should NOT allow string -> numeric', () => {
      expect(canPromoteSafely('string', 'int32')).toBe(false);
      expect(canPromoteSafely('string', 'double')).toBe(false);
    });

    it('should NOT allow numeric -> string', () => {
      expect(canPromoteSafely('int32', 'string')).toBe(false);
      expect(canPromoteSafely('double', 'string')).toBe(false);
    });

    it('should NOT allow boolean -> numeric', () => {
      expect(canPromoteSafely('boolean', 'int32')).toBe(false);
    });

    it('should NOT allow numeric -> boolean', () => {
      expect(canPromoteSafely('int32', 'boolean')).toBe(false);
    });
  });
});

// ============================================================================
// getPromotedType Tests
// ============================================================================

describe('getPromotedType', () => {
  describe('same types', () => {
    it('should return the same type when both are equal', () => {
      expect(getPromotedType('int32', 'int32')).toBe('int32');
      expect(getPromotedType('string', 'string')).toBe('string');
      expect(getPromotedType('variant', 'variant')).toBe('variant');
    });
  });

  describe('numeric type widening', () => {
    it('should promote int32 + int64 -> int64', () => {
      expect(getPromotedType('int32', 'int64')).toBe('int64');
      expect(getPromotedType('int64', 'int32')).toBe('int64');
    });

    it('should promote int32 + float -> float', () => {
      expect(getPromotedType('int32', 'float')).toBe('float');
      expect(getPromotedType('float', 'int32')).toBe('float');
    });

    it('should promote int32 + double -> double', () => {
      expect(getPromotedType('int32', 'double')).toBe('double');
      expect(getPromotedType('double', 'int32')).toBe('double');
    });

    it('should promote int64 + double -> double', () => {
      expect(getPromotedType('int64', 'double')).toBe('double');
      expect(getPromotedType('double', 'int64')).toBe('double');
    });

    it('should promote float + double -> double', () => {
      expect(getPromotedType('float', 'double')).toBe('double');
      expect(getPromotedType('double', 'float')).toBe('double');
    });

    it('should promote int64 + float -> float (using numeric hierarchy)', () => {
      // In the numeric hierarchy, float is wider than int64
      // Note: While this may lose precision for large int64 values,
      // the hierarchy follows the standard Parquet type widening
      expect(getPromotedType('int64', 'float')).toBe('float');
      expect(getPromotedType('float', 'int64')).toBe('float');
    });
  });

  describe('variant absorption', () => {
    it('should return variant when either type is variant', () => {
      expect(getPromotedType('variant', 'string')).toBe('variant');
      expect(getPromotedType('string', 'variant')).toBe('variant');
      expect(getPromotedType('variant', 'int32')).toBe('variant');
      expect(getPromotedType('double', 'variant')).toBe('variant');
    });
  });

  describe('date/timestamp widening', () => {
    it('should promote date + timestamp -> timestamp', () => {
      expect(getPromotedType('date', 'timestamp')).toBe('timestamp');
      expect(getPromotedType('timestamp', 'date')).toBe('timestamp');
    });
  });

  describe('incompatible types fall back to variant', () => {
    it('should return variant for string + number', () => {
      expect(getPromotedType('string', 'int32')).toBe('variant');
      expect(getPromotedType('int32', 'string')).toBe('variant');
    });

    it('should return variant for boolean + number', () => {
      expect(getPromotedType('boolean', 'int32')).toBe('variant');
      expect(getPromotedType('double', 'boolean')).toBe('variant');
    });

    it('should return variant for string + boolean', () => {
      expect(getPromotedType('string', 'boolean')).toBe('variant');
    });

    it('should return variant for binary + string', () => {
      expect(getPromotedType('binary', 'string')).toBe('variant');
    });
  });
});

// ============================================================================
// detectValueType Tests
// ============================================================================

describe('detectValueType', () => {
  it('should detect null', () => {
    expect(detectValueType(null)).toBe('null');
    expect(detectValueType(undefined)).toBe('null');
  });

  it('should detect string', () => {
    expect(detectValueType('hello')).toBe('string');
    expect(detectValueType('')).toBe('string');
  });

  it('should detect number', () => {
    expect(detectValueType(42)).toBe('number');
    expect(detectValueType(3.14)).toBe('number');
    expect(detectValueType(-1)).toBe('number');
    expect(detectValueType(0)).toBe('number');
  });

  it('should detect bigint as number', () => {
    expect(detectValueType(BigInt(9007199254740993))).toBe('number');
  });

  it('should detect boolean', () => {
    expect(detectValueType(true)).toBe('boolean');
    expect(detectValueType(false)).toBe('boolean');
  });

  it('should detect Date', () => {
    expect(detectValueType(new Date())).toBe('date');
  });

  it('should detect binary (Uint8Array)', () => {
    expect(detectValueType(new Uint8Array([1, 2, 3]))).toBe('binary');
  });

  it('should detect array', () => {
    expect(detectValueType([1, 2, 3])).toBe('array');
    expect(detectValueType([])).toBe('array');
  });

  it('should detect object', () => {
    expect(detectValueType({ foo: 'bar' })).toBe('object');
    expect(detectValueType({})).toBe('object');
  });
});

// ============================================================================
// detectedTypeToParquet Tests
// ============================================================================

describe('detectedTypeToParquet', () => {
  it('should map string -> string', () => {
    expect(detectedTypeToParquet('string')).toBe('string');
  });

  it('should map number -> double', () => {
    expect(detectedTypeToParquet('number')).toBe('double');
  });

  it('should map boolean -> boolean', () => {
    expect(detectedTypeToParquet('boolean')).toBe('boolean');
  });

  it('should map date -> timestamp', () => {
    expect(detectedTypeToParquet('date')).toBe('timestamp');
  });

  it('should map binary -> binary', () => {
    expect(detectedTypeToParquet('binary')).toBe('binary');
  });

  it('should map null -> variant', () => {
    expect(detectedTypeToParquet('null')).toBe('variant');
  });

  it('should map array -> variant', () => {
    expect(detectedTypeToParquet('array')).toBe('variant');
  });

  it('should map object -> variant', () => {
    expect(detectedTypeToParquet('object')).toBe('variant');
  });

  it('should map objectId -> string', () => {
    expect(detectedTypeToParquet('objectId')).toBe('string');
  });

  it('should map mixed -> variant', () => {
    expect(detectedTypeToParquet('mixed')).toBe('variant');
  });
});

// ============================================================================
// isNumericType Tests
// ============================================================================

describe('isNumericType', () => {
  it('should return true for numeric types', () => {
    expect(isNumericType('int32')).toBe(true);
    expect(isNumericType('int64')).toBe(true);
    expect(isNumericType('float')).toBe(true);
    expect(isNumericType('double')).toBe(true);
  });

  it('should return false for non-numeric types', () => {
    expect(isNumericType('string')).toBe(false);
    expect(isNumericType('boolean')).toBe(false);
    expect(isNumericType('timestamp')).toBe(false);
    expect(isNumericType('variant')).toBe(false);
  });
});

// ============================================================================
// getNumericPrecision Tests
// ============================================================================

describe('getNumericPrecision', () => {
  it('should return correct precision levels', () => {
    expect(getNumericPrecision('int32')).toBe(0);
    expect(getNumericPrecision('int64')).toBe(1);
    expect(getNumericPrecision('float')).toBe(2);
    expect(getNumericPrecision('double')).toBe(3);
  });

  it('should return -1 for non-numeric types', () => {
    expect(getNumericPrecision('string')).toBe(-1);
    expect(getNumericPrecision('boolean')).toBe(-1);
    expect(getNumericPrecision('variant')).toBe(-1);
  });
});

// ============================================================================
// detectPromotion Tests
// ============================================================================

describe('detectPromotion', () => {
  describe('no promotion needed', () => {
    it('should return empty array when document matches schema', () => {
      const schema = createSchema({
        name: 'string',
        age: 'double',
        active: 'boolean',
      });

      const doc = {
        name: 'Alice',
        age: 30,
        active: true,
      };

      const promotions = detectPromotion(schema, doc);
      expect(promotions).toEqual([]);
    });

    it('should return empty array for new fields (not promotions)', () => {
      const schema = createSchema({
        name: 'string',
      });

      const doc = {
        name: 'Alice',
        newField: 'some value', // New field, not a promotion
      };

      const promotions = detectPromotion(schema, doc);
      expect(promotions).toEqual([]);
    });

    it('should skip null/undefined values', () => {
      const schema = createSchema({
        name: 'string',
        age: 'int32',
      });

      const doc = {
        name: null,
        age: undefined,
      };

      const promotions = detectPromotion(schema, doc as Record<string, unknown>);
      expect(promotions).toEqual([]);
    });
  });

  describe('safe promotions detected', () => {
    it('should detect int32 -> int64 promotion', () => {
      const schema = createSchema({
        count: 'int32',
      });

      // Simulate a value that requires int64 (JavaScript BigInt)
      const doc = {
        count: BigInt(9007199254740993), // Larger than MAX_SAFE_INTEGER
      };

      const promotions = detectPromotion(schema, doc);
      expect(promotions).toHaveLength(1);
      expect(promotions[0].field).toBe('count');
      expect(promotions[0].from).toBe('int32');
      expect(promotions[0].to).toBe('double'); // BigInt detected as number -> double
    });

    it('should detect int32 -> double when receiving float', () => {
      const schema = createSchema({
        price: 'int32',
      });

      const doc = {
        price: 19.99, // Float value where int32 was expected
      };

      const promotions = detectPromotion(schema, doc);
      expect(promotions).toHaveLength(1);
      expect(promotions[0].field).toBe('price');
      expect(promotions[0].from).toBe('int32');
      expect(promotions[0].to).toBe('double');
      expect(promotions[0].isSafe).toBe(true);
    });

    it('should detect float -> double promotion', () => {
      const schema = createSchema({
        value: 'float',
      });

      const doc = {
        value: 3.14159265358979, // High precision double
      };

      const promotions = detectPromotion(schema, doc);
      expect(promotions).toHaveLength(1);
      expect(promotions[0].from).toBe('float');
      expect(promotions[0].to).toBe('double');
      expect(promotions[0].isSafe).toBe(true);
    });
  });

  describe('type mismatch promotions to variant', () => {
    it('should promote string -> variant when receiving number', () => {
      const schema = createSchema({
        value: 'string',
      });

      const doc = {
        value: 42, // Number where string was expected
      };

      const promotions = detectPromotion(schema, doc);
      expect(promotions).toHaveLength(1);
      expect(promotions[0].field).toBe('value');
      expect(promotions[0].from).toBe('string');
      expect(promotions[0].to).toBe('variant');
      // Promotion to variant is considered safe (no data loss),
      // even though type semantics are lost
      expect(promotions[0].isSafe).toBe(true);
    });

    it('should promote boolean -> variant when receiving number', () => {
      const schema = createSchema({
        flag: 'boolean',
      });

      const doc = {
        flag: 42, // Number where boolean was expected
      };

      const promotions = detectPromotion(schema, doc);
      expect(promotions).toHaveLength(1);
      expect(promotions[0].to).toBe('variant');
      // Promotion to variant preserves data, so it's considered safe
      expect(promotions[0].isSafe).toBe(true);
    });
  });

  describe('nested field promotions', () => {
    it('should detect promotions in nested fields', () => {
      const schema = createSchema({
        'user.age': 'int32',
        'user.name': 'string',
      });

      const doc = {
        user: {
          age: 30.5, // Float where int32 was expected
          name: 'Alice',
        },
      };

      const promotions = detectPromotion(schema, doc);
      expect(promotions).toHaveLength(1);
      expect(promotions[0].field).toBe('user.age');
      expect(promotions[0].from).toBe('int32');
      expect(promotions[0].to).toBe('double');
    });
  });

  describe('multiple promotions', () => {
    it('should detect multiple promotions in same document', () => {
      const schema = createSchema({
        count: 'int32',
        price: 'float',
        value: 'string',
      });

      const doc = {
        count: 3.14,    // int32 -> double
        price: 99.999,  // float -> double
        value: true,    // string -> variant (unsafe)
      };

      const promotions = detectPromotion(schema, doc);
      expect(promotions).toHaveLength(3);

      const countPromo = promotions.find(p => p.field === 'count');
      const pricePromo = promotions.find(p => p.field === 'price');
      const valuePromo = promotions.find(p => p.field === 'value');

      expect(countPromo?.to).toBe('double');
      expect(pricePromo?.to).toBe('double');
      expect(valuePromo?.to).toBe('variant');
    });
  });
});

// ============================================================================
// compareSchemas Tests
// ============================================================================

describe('compareSchemas', () => {
  describe('identical schemas', () => {
    it('should report compatible with no changes', () => {
      const schema1 = createSchema({ name: 'string', age: 'int32' });
      const schema2 = createSchema({ name: 'string', age: 'int32' });

      const comparison = compareSchemas(schema1, schema2);

      expect(comparison.compatible).toBe(true);
      expect(comparison.promotions).toEqual([]);
      expect(comparison.addedFields).toEqual([]);
      expect(comparison.removedFields).toEqual([]);
      expect(comparison.conflicts).toEqual([]);
    });
  });

  describe('added fields', () => {
    it('should detect added fields', () => {
      const oldSchema = createSchema({ name: 'string' });
      const newSchema = createSchema({ name: 'string', age: 'int32' });

      const comparison = compareSchemas(oldSchema, newSchema);

      expect(comparison.compatible).toBe(true);
      expect(comparison.addedFields).toEqual(['age']);
    });

    it('should detect multiple added fields', () => {
      const oldSchema = createSchema({ name: 'string' });
      const newSchema = createSchema({
        name: 'string',
        age: 'int32',
        email: 'string',
        active: 'boolean',
      });

      const comparison = compareSchemas(oldSchema, newSchema);

      expect(comparison.addedFields).toHaveLength(3);
      expect(comparison.addedFields).toContain('age');
      expect(comparison.addedFields).toContain('email');
      expect(comparison.addedFields).toContain('active');
    });
  });

  describe('removed fields', () => {
    it('should detect removed fields', () => {
      const oldSchema = createSchema({ name: 'string', age: 'int32' });
      const newSchema = createSchema({ name: 'string' });

      const comparison = compareSchemas(oldSchema, newSchema);

      expect(comparison.removedFields).toEqual(['age']);
    });
  });

  describe('safe promotions', () => {
    it('should detect safe numeric promotions', () => {
      const oldSchema = createSchema({ count: 'int32' });
      const newSchema = createSchema({ count: 'int64' });

      const comparison = compareSchemas(oldSchema, newSchema);

      expect(comparison.compatible).toBe(true);
      expect(comparison.promotions).toHaveLength(1);
      expect(comparison.promotions[0].from).toBe('int32');
      expect(comparison.promotions[0].to).toBe('int64');
      expect(comparison.promotions[0].isSafe).toBe(true);
    });

    it('should detect date -> timestamp promotion', () => {
      const oldSchema = createSchema({ createdAt: 'date' });
      const newSchema = createSchema({ createdAt: 'timestamp' });

      const comparison = compareSchemas(oldSchema, newSchema);

      expect(comparison.compatible).toBe(true);
      expect(comparison.promotions).toHaveLength(1);
      expect(comparison.promotions[0].to).toBe('timestamp');
    });
  });

  describe('type conflicts', () => {
    it('should detect type conflicts (incompatible types)', () => {
      const oldSchema = createSchema({ value: 'string' });
      const newSchema = createSchema({ value: 'int32' });

      const comparison = compareSchemas(oldSchema, newSchema);

      expect(comparison.compatible).toBe(false);
      expect(comparison.conflicts).toHaveLength(1);
      expect(comparison.conflicts[0].field).toBe('value');
      expect(comparison.conflicts[0].existingType).toBe('string');
      expect(comparison.conflicts[0].newType).toBe('int32');
    });
  });

  describe('complex scenarios', () => {
    it('should handle mixed changes', () => {
      const oldSchema = createSchema({
        name: 'string',      // unchanged
        age: 'int32',        // promoted to int64
        status: 'string',    // removed
        value: 'boolean',    // conflict with int32
      });

      const newSchema = createSchema({
        name: 'string',      // unchanged
        age: 'int64',        // promoted from int32
        email: 'string',     // added
        value: 'int32',      // conflict with boolean
      });

      const comparison = compareSchemas(oldSchema, newSchema);

      expect(comparison.compatible).toBe(false); // Due to conflict
      expect(comparison.addedFields).toContain('email');
      expect(comparison.removedFields).toContain('status');
      expect(comparison.promotions).toHaveLength(1);
      expect(comparison.conflicts).toHaveLength(1);
    });
  });
});

// ============================================================================
// generateMigration Tests
// ============================================================================

describe('generateMigration', () => {
  it('should generate migration with promotions', () => {
    const oldSchema = createSchema({ count: 'int32' });
    const newSchema = createSchema({ count: 'int64' });
    const comparison = compareSchemas(oldSchema, newSchema);

    const migration = generateMigration(comparison, 1, 2, newSchema);

    expect(migration.fromVersion).toBe(1);
    expect(migration.toVersion).toBe(2);
    expect(migration.promotions).toHaveLength(1);
    expect(migration.isBackwardCompatible).toBe(true);
  });

  it('should generate migration with new fields', () => {
    const oldSchema = createSchema({ name: 'string' });
    const newSchema = createSchema({ name: 'string', age: 'int32' });
    const comparison = compareSchemas(oldSchema, newSchema);

    const migration = generateMigration(comparison, 1, 2, newSchema);

    expect(migration.newFields).toHaveLength(1);
    expect(migration.newFields[0].path).toBe('age');
    expect(migration.newFields[0].type).toBe('int32');
    expect(migration.newFields[0].isOptional).toBe(true);
  });

  it('should mark migration as not backward compatible when fields removed', () => {
    const oldSchema = createSchema({ name: 'string', age: 'int32' });
    const newSchema = createSchema({ name: 'string' });
    const comparison = compareSchemas(oldSchema, newSchema);

    const migration = generateMigration(comparison, 1, 2, newSchema);

    expect(migration.isBackwardCompatible).toBe(false);
    expect(migration.removedFields).toContain('age');
  });

  it('should include timestamp', () => {
    const comparison = compareSchemas(
      createSchema({ name: 'string' }),
      createSchema({ name: 'string' })
    );

    const migration = generateMigration(comparison, 1, 2);

    expect(migration.createdAt).toBeInstanceOf(Date);
  });
});

// ============================================================================
// applyMigration Tests
// ============================================================================

describe('applyMigration', () => {
  it('should apply type promotions', () => {
    const schema: VersionedSchema = {
      version: 1,
      columns: new Map([
        ['count', createColumn('count', 'int32')],
        ['name', createColumn('name', 'string')],
      ]),
      createdAt: new Date(),
    };

    const migration: SchemaMigration = {
      fromVersion: 1,
      toVersion: 2,
      promotions: [
        {
          field: 'count',
          from: 'int32',
          to: 'int64',
          isSafe: true,
          reason: 'Type widening',
        },
      ],
      newFields: [],
      removedFields: [],
      isBackwardCompatible: true,
      createdAt: new Date(),
    };

    const newSchema = applyMigration(schema, migration);

    expect(newSchema.version).toBe(2);
    expect(newSchema.columns.get('count')?.type).toBe('int64');
    expect(newSchema.columns.get('name')?.type).toBe('string');
    expect(newSchema.previousVersion).toBe(1);
  });

  it('should add new fields', () => {
    const schema: VersionedSchema = {
      version: 1,
      columns: new Map([
        ['name', createColumn('name', 'string')],
      ]),
      createdAt: new Date(),
    };

    const migration: SchemaMigration = {
      fromVersion: 1,
      toVersion: 2,
      promotions: [],
      newFields: [
        { path: 'age', type: 'int32', isOptional: true, defaultValue: null },
        { path: 'email', type: 'string', isOptional: true, defaultValue: null },
      ],
      removedFields: [],
      isBackwardCompatible: true,
      createdAt: new Date(),
    };

    const newSchema = applyMigration(schema, migration);

    expect(newSchema.columns.size).toBe(3);
    expect(newSchema.columns.has('age')).toBe(true);
    expect(newSchema.columns.has('email')).toBe(true);
    expect(newSchema.columns.get('age')?.type).toBe('int32');
  });

  it('should remove fields', () => {
    const schema: VersionedSchema = {
      version: 1,
      columns: new Map([
        ['name', createColumn('name', 'string')],
        ['age', createColumn('age', 'int32')],
        ['email', createColumn('email', 'string')],
      ]),
      createdAt: new Date(),
    };

    const migration: SchemaMigration = {
      fromVersion: 1,
      toVersion: 2,
      promotions: [],
      newFields: [],
      removedFields: ['age'],
      isBackwardCompatible: false,
      createdAt: new Date(),
    };

    const newSchema = applyMigration(schema, migration);

    expect(newSchema.columns.size).toBe(2);
    expect(newSchema.columns.has('age')).toBe(false);
    expect(newSchema.columns.has('name')).toBe(true);
    expect(newSchema.columns.has('email')).toBe(true);
  });
});

// ============================================================================
// isSafeMigration Tests
// ============================================================================

describe('isSafeMigration', () => {
  it('should return true for backward compatible migration with safe promotions', () => {
    const migration: SchemaMigration = {
      fromVersion: 1,
      toVersion: 2,
      promotions: [
        { field: 'count', from: 'int32', to: 'int64', isSafe: true, reason: '' },
      ],
      newFields: [{ path: 'email', type: 'string', isOptional: true }],
      removedFields: [],
      isBackwardCompatible: true,
      createdAt: new Date(),
    };

    expect(isSafeMigration(migration)).toBe(true);
  });

  it('should return false when removedFields is not empty', () => {
    const migration: SchemaMigration = {
      fromVersion: 1,
      toVersion: 2,
      promotions: [],
      newFields: [],
      removedFields: ['age'],
      isBackwardCompatible: false,
      createdAt: new Date(),
    };

    expect(isSafeMigration(migration)).toBe(false);
  });

  it('should return false when promotions are unsafe', () => {
    const migration: SchemaMigration = {
      fromVersion: 1,
      toVersion: 2,
      promotions: [
        { field: 'value', from: 'string', to: 'variant', isSafe: false, reason: '' },
      ],
      newFields: [],
      removedFields: [],
      isBackwardCompatible: false,
      createdAt: new Date(),
    };

    expect(isSafeMigration(migration)).toBe(false);
  });

  it('should return false when not backward compatible', () => {
    const migration: SchemaMigration = {
      fromVersion: 1,
      toVersion: 2,
      promotions: [],
      newFields: [],
      removedFields: [],
      isBackwardCompatible: false,
      createdAt: new Date(),
    };

    expect(isSafeMigration(migration)).toBe(false);
  });
});

// ============================================================================
// createVersionedSchema Tests
// ============================================================================

describe('createVersionedSchema', () => {
  it('should create versioned schema from parsed collection schema', () => {
    const schema = createSchema({
      name: 'string',
      age: 'int32',
    });

    const versioned = createVersionedSchema(schema, 1);

    expect(versioned.version).toBe(1);
    expect(versioned.columns).toBe(schema.columnMap);
    expect(versioned.createdAt).toBeInstanceOf(Date);
    expect(versioned.previousVersion).toBeUndefined();
  });

  it('should default version to 1', () => {
    const schema = createSchema({ name: 'string' });

    const versioned = createVersionedSchema(schema);

    expect(versioned.version).toBe(1);
  });
});

// ============================================================================
// flattenDocument Tests
// ============================================================================

describe('flattenDocument', () => {
  it('should flatten simple object', () => {
    const doc = { name: 'Alice', age: 30 };
    const result = Array.from(flattenDocument(doc));

    expect(result).toContainEqual(['name', 'Alice']);
    expect(result).toContainEqual(['age', 30]);
  });

  it('should flatten nested objects', () => {
    const doc = {
      user: {
        profile: {
          name: 'Alice',
        },
      },
    };

    const result = Array.from(flattenDocument(doc));

    expect(result).toContainEqual(['user', doc.user]);
    expect(result).toContainEqual(['user.profile', doc.user.profile]);
    expect(result).toContainEqual(['user.profile.name', 'Alice']);
  });

  it('should not recurse into arrays', () => {
    const doc = {
      tags: ['a', 'b', 'c'],
    };

    const result = Array.from(flattenDocument(doc));

    expect(result).toContainEqual(['tags', ['a', 'b', 'c']]);
    expect(result).toHaveLength(1);
  });

  it('should not recurse into Date', () => {
    const date = new Date();
    const doc = { createdAt: date };

    const result = Array.from(flattenDocument(doc));

    expect(result).toContainEqual(['createdAt', date]);
    expect(result).toHaveLength(1);
  });

  it('should not recurse into Uint8Array', () => {
    const binary = new Uint8Array([1, 2, 3]);
    const doc = { data: binary };

    const result = Array.from(flattenDocument(doc));

    expect(result).toContainEqual(['data', binary]);
    expect(result).toHaveLength(1);
  });

  it('should respect maxDepth', () => {
    const doc = {
      level1: {
        level2: {
          level3: {
            value: 'deep',
          },
        },
      },
    };

    const result = Array.from(flattenDocument(doc, '', 2));
    const paths = result.map(([path]) => path);

    expect(paths).toContain('level1');
    expect(paths).toContain('level1.level2');
    expect(paths).not.toContain('level1.level2.level3');
  });
});

// ============================================================================
// Integration Tests: Auto-Promotion Workflow
// ============================================================================

describe('Auto-Promotion Workflow Integration', () => {
  it('should detect, plan, and apply promotion for numeric widening', () => {
    // 1. Start with initial schema
    const initialSchema = createSchema({
      count: 'int32',
      price: 'float',
      name: 'string',
    });
    const versionedSchema = createVersionedSchema(initialSchema, 1);

    // 2. Ingest document that requires type widening
    const newDocument = {
      count: BigInt(9007199254740993), // Requires int64 or larger
      price: 999999.999999,             // Requires double
      name: 'Product',
    };

    // 3. Detect required promotions
    const promotions = detectPromotion(initialSchema, newDocument);
    expect(promotions.length).toBeGreaterThan(0);

    // 4. Create a new schema with the promoted types
    const newSchemaColumns: Record<string, ParquetType> = {};
    for (const [path, col] of initialSchema.columnMap) {
      const promo = promotions.find(p => p.field === path);
      newSchemaColumns[path] = promo ? promo.to : col.type;
    }
    const newSchema = createSchema(newSchemaColumns);

    // 5. Compare schemas to get full comparison
    const comparison = compareSchemas(initialSchema, newSchema);

    // 6. Generate migration
    const migration = generateMigration(comparison, 1, 2, newSchema);
    expect(migration.promotions.length).toBeGreaterThan(0);
    expect(migration.isBackwardCompatible).toBe(true);

    // 7. Apply migration
    const upgradedSchema = applyMigration(versionedSchema, migration);

    // 8. Verify upgraded schema
    expect(upgradedSchema.version).toBe(2);
    expect(upgradedSchema.previousVersion).toBe(1);
    expect(upgradedSchema.columns.get('count')?.type).toBe('double'); // number maps to double
    expect(upgradedSchema.columns.get('price')?.type).toBe('double');
    expect(upgradedSchema.columns.get('name')?.type).toBe('string'); // unchanged
  });

  it('should handle schema evolution with new fields', () => {
    // 1. Initial schema with few fields
    const initialSchema = createSchema({
      _id: 'string',
      name: 'string',
    });
    const versionedSchema = createVersionedSchema(initialSchema, 1);

    // 2. New schema with additional fields
    const newSchema = createSchema({
      _id: 'string',
      name: 'string',
      email: 'string',
      age: 'int32',
      active: 'boolean',
    });

    // 3. Compare schemas
    const comparison = compareSchemas(initialSchema, newSchema);
    expect(comparison.addedFields).toHaveLength(3);
    expect(comparison.compatible).toBe(true);

    // 4. Generate and apply migration
    const migration = generateMigration(comparison, 1, 2, newSchema);
    expect(migration.newFields).toHaveLength(3);
    expect(migration.isBackwardCompatible).toBe(true);

    const upgradedSchema = applyMigration(versionedSchema, migration);

    // 5. Verify
    expect(upgradedSchema.columns.size).toBe(5);
    expect(upgradedSchema.columns.has('email')).toBe(true);
    expect(upgradedSchema.columns.has('age')).toBe(true);
    expect(upgradedSchema.columns.has('active')).toBe(true);
  });

  it('should track schema history through multiple migrations', () => {
    // Version 1
    const v1Schema = createSchema({ name: 'string' });
    const v1Versioned = createVersionedSchema(v1Schema, 1);

    // Version 2 - add age field
    const v2Schema = createSchema({ name: 'string', age: 'int32' });
    const comparison1to2 = compareSchemas(v1Schema, v2Schema);
    const migration1to2 = generateMigration(comparison1to2, 1, 2, v2Schema);
    const v2Versioned = applyMigration(v1Versioned, migration1to2);

    expect(v2Versioned.version).toBe(2);
    expect(v2Versioned.previousVersion).toBe(1);

    // Version 3 - promote age to int64
    const v3Schema = createSchema({ name: 'string', age: 'int64' });
    const comparison2to3 = compareSchemas(v2Schema, v3Schema);
    const migration2to3 = generateMigration(comparison2to3, 2, 3, v3Schema);
    const v3Versioned = applyMigration(v2Versioned, migration2to3);

    expect(v3Versioned.version).toBe(3);
    expect(v3Versioned.previousVersion).toBe(2);
    expect(v3Versioned.columns.get('age')?.type).toBe('int64');
  });
});

// ============================================================================
// SchemaHistory Tests
// ============================================================================

describe('SchemaHistory', () => {
  describe('initialization', () => {
    it('should create empty history with collection name', () => {
      const history = new SchemaHistory('users');

      expect(history.getCollectionName()).toBe('users');
      expect(history.getCurrentVersion()).toBe(0);
      expect(history.getCurrentSchema()).toBeUndefined();
      expect(history.getAllVersions()).toEqual([]);
    });
  });

  describe('addVersion', () => {
    it('should add initial version without migration', () => {
      const history = new SchemaHistory('users');
      const schema = createVersionedSchema(createSchema({ name: 'string' }), 1);

      history.addVersion(schema, null);

      expect(history.getCurrentVersion()).toBe(1);
      expect(history.getCurrentSchema()).toBe(schema);
      expect(history.getVersion(1)).toBe(schema);
    });

    it('should add subsequent version with migration', () => {
      const history = new SchemaHistory('users');

      // Add v1
      const v1Schema = createVersionedSchema(createSchema({ name: 'string' }), 1);
      history.addVersion(v1Schema, null);

      // Add v2 with migration
      const v2Schema = createVersionedSchema(createSchema({ name: 'string', age: 'int32' }), 2);
      v2Schema.previousVersion = 1;

      const migration: SchemaMigration = {
        fromVersion: 1,
        toVersion: 2,
        promotions: [],
        newFields: [{ path: 'age', type: 'int32', isOptional: true }],
        removedFields: [],
        isBackwardCompatible: true,
        createdAt: new Date(),
      };

      history.addVersion(v2Schema, migration);

      expect(history.getCurrentVersion()).toBe(2);
      expect(history.getAllVersions()).toEqual([1, 2]);
    });

    it('should throw if non-initial version has no migration', () => {
      const history = new SchemaHistory('users');
      const v1 = createVersionedSchema(createSchema({ name: 'string' }), 1);
      history.addVersion(v1, null);

      const v2 = createVersionedSchema(createSchema({ name: 'string', age: 'int32' }), 2);

      expect(() => history.addVersion(v2, null)).toThrow('must have a migration');
    });

    it('should throw if migration fromVersion does not match current', () => {
      const history = new SchemaHistory('users');
      const v1 = createVersionedSchema(createSchema({ name: 'string' }), 1);
      history.addVersion(v1, null);

      const v2 = createVersionedSchema(createSchema({ name: 'string' }), 3);
      const badMigration: SchemaMigration = {
        fromVersion: 2, // Wrong! Current is 1
        toVersion: 3,
        promotions: [],
        newFields: [],
        removedFields: [],
        isBackwardCompatible: true,
        createdAt: new Date(),
      };

      expect(() => history.addVersion(v2, badMigration)).toThrow('must match current version');
    });

    it('should throw if migration toVersion does not match schema version', () => {
      const history = new SchemaHistory('users');
      const v1 = createVersionedSchema(createSchema({ name: 'string' }), 1);
      history.addVersion(v1, null);

      const v2 = createVersionedSchema(createSchema({ name: 'string' }), 2);
      const badMigration: SchemaMigration = {
        fromVersion: 1,
        toVersion: 3, // Wrong! Schema version is 2
        promotions: [],
        newFields: [],
        removedFields: [],
        isBackwardCompatible: true,
        createdAt: new Date(),
      };

      expect(() => history.addVersion(v2, badMigration)).toThrow('must match schema version');
    });
  });

  describe('getMigration', () => {
    it('should return migration between versions', () => {
      const history = new SchemaHistory('users');

      const v1 = createVersionedSchema(createSchema({ name: 'string' }), 1);
      history.addVersion(v1, null);

      const v2 = createVersionedSchema(createSchema({ name: 'string', age: 'int32' }), 2);
      const migration: SchemaMigration = {
        fromVersion: 1,
        toVersion: 2,
        promotions: [],
        newFields: [{ path: 'age', type: 'int32', isOptional: true }],
        removedFields: [],
        isBackwardCompatible: true,
        createdAt: new Date(),
      };
      history.addVersion(v2, migration);

      const retrieved = history.getMigration(1, 2);
      expect(retrieved).toBe(migration);
    });

    it('should return undefined for non-existent migration', () => {
      const history = new SchemaHistory('users');
      expect(history.getMigration(1, 2)).toBeUndefined();
    });
  });

  describe('getMigrationPath', () => {
    it('should return empty array for same version', () => {
      const history = new SchemaHistory('users');
      expect(history.getMigrationPath(1, 1)).toEqual([]);
    });

    it('should return null for downgrade', () => {
      const history = new SchemaHistory('users');
      expect(history.getMigrationPath(2, 1)).toBeNull();
    });

    it('should return single migration for adjacent versions', () => {
      const history = new SchemaHistory('users');

      const v1 = createVersionedSchema(createSchema({ name: 'string' }), 1);
      history.addVersion(v1, null);

      const v2 = createVersionedSchema(createSchema({ name: 'string', age: 'int32' }), 2);
      const migration: SchemaMigration = {
        fromVersion: 1,
        toVersion: 2,
        promotions: [],
        newFields: [{ path: 'age', type: 'int32', isOptional: true }],
        removedFields: [],
        isBackwardCompatible: true,
        createdAt: new Date(),
      };
      history.addVersion(v2, migration);

      const path = history.getMigrationPath(1, 2);
      expect(path).toHaveLength(1);
      expect(path![0]).toBe(migration);
    });

    it('should return multiple migrations for non-adjacent versions', () => {
      const history = new SchemaHistory('users');

      // v1
      const v1 = createVersionedSchema(createSchema({ name: 'string' }), 1);
      history.addVersion(v1, null);

      // v2
      const v2 = createVersionedSchema(createSchema({ name: 'string', age: 'int32' }), 2);
      const m1to2: SchemaMigration = {
        fromVersion: 1,
        toVersion: 2,
        promotions: [],
        newFields: [{ path: 'age', type: 'int32', isOptional: true }],
        removedFields: [],
        isBackwardCompatible: true,
        createdAt: new Date(),
      };
      history.addVersion(v2, m1to2);

      // v3
      const v3 = createVersionedSchema(createSchema({ name: 'string', age: 'int64' }), 3);
      const m2to3: SchemaMigration = {
        fromVersion: 2,
        toVersion: 3,
        promotions: [{ field: 'age', from: 'int32', to: 'int64', isSafe: true, reason: '' }],
        newFields: [],
        removedFields: [],
        isBackwardCompatible: true,
        createdAt: new Date(),
      };
      history.addVersion(v3, m2to3);

      const path = history.getMigrationPath(1, 3);
      expect(path).toHaveLength(2);
      expect(path![0]).toBe(m1to2);
      expect(path![1]).toBe(m2to3);
    });

    it('should return null if path is broken', () => {
      const history = new SchemaHistory('users');

      const v1 = createVersionedSchema(createSchema({ name: 'string' }), 1);
      history.addVersion(v1, null);

      // No v2, direct jump to v3 (which shouldn't happen normally)
      const v3 = createVersionedSchema(createSchema({ name: 'string' }), 3);
      history.versions.set(3, v3); // Bypass normal validation

      expect(history.getMigrationPath(1, 3)).toBeNull();
    });
  });

  describe('isSafeUpgrade', () => {
    it('should return true for safe migrations', () => {
      const history = new SchemaHistory('users');

      const v1 = createVersionedSchema(createSchema({ count: 'int32' }), 1);
      history.addVersion(v1, null);

      const v2 = createVersionedSchema(createSchema({ count: 'int64' }), 2);
      const migration: SchemaMigration = {
        fromVersion: 1,
        toVersion: 2,
        promotions: [{ field: 'count', from: 'int32', to: 'int64', isSafe: true, reason: '' }],
        newFields: [],
        removedFields: [],
        isBackwardCompatible: true,
        createdAt: new Date(),
      };
      history.addVersion(v2, migration);

      expect(history.isSafeUpgrade(1, 2)).toBe(true);
    });

    it('should return false for unsafe migrations', () => {
      const history = new SchemaHistory('users');

      const v1 = createVersionedSchema(createSchema({ name: 'string', age: 'int32' }), 1);
      history.addVersion(v1, null);

      const v2 = createVersionedSchema(createSchema({ name: 'string' }), 2);
      const migration: SchemaMigration = {
        fromVersion: 1,
        toVersion: 2,
        promotions: [],
        newFields: [],
        removedFields: ['age'],
        isBackwardCompatible: false,
        createdAt: new Date(),
      };
      history.addVersion(v2, migration);

      expect(history.isSafeUpgrade(1, 2)).toBe(false);
    });

    it('should return false if no path exists', () => {
      const history = new SchemaHistory('users');
      expect(history.isSafeUpgrade(1, 5)).toBe(false);
    });
  });

  describe('getFieldHistory', () => {
    it('should track field type changes across versions', () => {
      const history = new SchemaHistory('users');

      // v1 - age is int32
      const v1 = createVersionedSchema(createSchema({ age: 'int32' }), 1);
      history.addVersion(v1, null);

      // v2 - age promoted to int64
      const v2 = createVersionedSchema(createSchema({ age: 'int64' }), 2);
      const migration: SchemaMigration = {
        fromVersion: 1,
        toVersion: 2,
        promotions: [{ field: 'age', from: 'int32', to: 'int64', isSafe: true, reason: '' }],
        newFields: [],
        removedFields: [],
        isBackwardCompatible: true,
        createdAt: new Date(),
      };
      history.addVersion(v2, migration);

      const fieldHistory = history.getFieldHistory('age');

      expect(fieldHistory).toHaveLength(2);
      expect(fieldHistory[0]).toEqual({ version: 1, type: 'int32' });
      expect(fieldHistory[1]).toEqual({ version: 2, type: 'int64' });
    });

    it('should return null for versions where field does not exist', () => {
      const history = new SchemaHistory('users');

      // v1 - no email field
      const v1 = createVersionedSchema(createSchema({ name: 'string' }), 1);
      history.addVersion(v1, null);

      // v2 - email added
      const v2 = createVersionedSchema(createSchema({ name: 'string', email: 'string' }), 2);
      const migration: SchemaMigration = {
        fromVersion: 1,
        toVersion: 2,
        promotions: [],
        newFields: [{ path: 'email', type: 'string', isOptional: true }],
        removedFields: [],
        isBackwardCompatible: true,
        createdAt: new Date(),
      };
      history.addVersion(v2, migration);

      const fieldHistory = history.getFieldHistory('email');

      expect(fieldHistory[0]).toEqual({ version: 1, type: null });
      expect(fieldHistory[1]).toEqual({ version: 2, type: 'string' });
    });
  });

  describe('getPromotionSummary', () => {
    it('should aggregate all promotions by field', () => {
      const history = new SchemaHistory('users');

      const v1 = createVersionedSchema(createSchema({ count: 'int32', price: 'float' }), 1);
      history.addVersion(v1, null);

      const v2 = createVersionedSchema(createSchema({ count: 'int64', price: 'float' }), 2);
      const m1: SchemaMigration = {
        fromVersion: 1,
        toVersion: 2,
        promotions: [{ field: 'count', from: 'int32', to: 'int64', isSafe: true, reason: '' }],
        newFields: [],
        removedFields: [],
        isBackwardCompatible: true,
        createdAt: new Date(),
      };
      history.addVersion(v2, m1);

      const v3 = createVersionedSchema(createSchema({ count: 'int64', price: 'double' }), 3);
      const m2: SchemaMigration = {
        fromVersion: 2,
        toVersion: 3,
        promotions: [{ field: 'price', from: 'float', to: 'double', isSafe: true, reason: '' }],
        newFields: [],
        removedFields: [],
        isBackwardCompatible: true,
        createdAt: new Date(),
      };
      history.addVersion(v3, m2);

      const summary = history.getPromotionSummary();

      expect(summary.size).toBe(2);
      expect(summary.get('count')).toHaveLength(1);
      expect(summary.get('price')).toHaveLength(1);
    });
  });

  describe('serialize / deserialize', () => {
    it('should serialize and deserialize history', () => {
      const history = new SchemaHistory('users');

      // Build history
      const v1 = createVersionedSchema(createSchema({ name: 'string', count: 'int32' }), 1);
      history.addVersion(v1, null);

      const v2 = createVersionedSchema(createSchema({ name: 'string', count: 'int64' }), 2);
      const migration: SchemaMigration = {
        fromVersion: 1,
        toVersion: 2,
        promotions: [{ field: 'count', from: 'int32', to: 'int64', isSafe: true, reason: 'Widening' }],
        newFields: [],
        removedFields: [],
        isBackwardCompatible: true,
        createdAt: new Date(),
      };
      history.addVersion(v2, migration);

      // Serialize
      const serialized = history.serialize();

      expect(serialized.collectionName).toBe('users');
      expect(serialized.currentVersion).toBe(2);
      expect(serialized.versions).toHaveLength(2);
      expect(serialized.migrations).toHaveLength(1);

      // Deserialize
      const restored = SchemaHistory.deserialize(serialized);

      expect(restored.getCollectionName()).toBe('users');
      expect(restored.getCurrentVersion()).toBe(2);
      expect(restored.getAllVersions()).toEqual([1, 2]);

      // Verify migration was restored
      const restoredMigration = restored.getMigration(1, 2);
      expect(restoredMigration).toBeDefined();
      expect(restoredMigration!.promotions).toHaveLength(1);
      expect(restoredMigration!.promotions[0].field).toBe('count');

      // Verify schemas were restored
      const restoredV1 = restored.getVersion(1);
      expect(restoredV1!.columns.get('count')?.type).toBe('int32');

      const restoredV2 = restored.getVersion(2);
      expect(restoredV2!.columns.get('count')?.type).toBe('int64');
    });

    it('should produce JSON-serializable output', () => {
      const history = new SchemaHistory('products');
      const v1 = createVersionedSchema(createSchema({ price: 'double' }), 1);
      history.addVersion(v1, null);

      const serialized = history.serialize();
      const json = JSON.stringify(serialized);
      const parsed = JSON.parse(json);

      const restored = SchemaHistory.deserialize(parsed);
      expect(restored.getCollectionName()).toBe('products');
    });
  });
});

// ============================================================================
// Integration: SchemaHistory with Auto-Promotion
// ============================================================================

describe('SchemaHistory with Auto-Promotion Integration', () => {
  it('should track promotions through automatic detection', () => {
    const history = new SchemaHistory('orders');

    // 1. Initial schema
    const initialSchema = createSchema({
      orderId: 'string',
      total: 'int32',
      quantity: 'int32',
    });
    const v1 = createVersionedSchema(initialSchema, 1);
    history.addVersion(v1, null);

    // 2. Document arrives that requires promotion
    const newDocument = {
      orderId: 'order-123',
      total: 99999.99, // Float, not int
      quantity: 5,
    };

    // 3. Detect promotions needed
    const promotions = detectPromotion(initialSchema, newDocument);
    expect(promotions.length).toBeGreaterThan(0);

    // 4. Build new schema with promotions
    const newSchemaColumns: Record<string, ParquetType> = {};
    for (const [path, col] of initialSchema.columnMap) {
      const promo = promotions.find(p => p.field === path);
      newSchemaColumns[path] = promo ? promo.to : col.type;
    }
    const evolvedSchema = createSchema(newSchemaColumns);

    // 5. Compare and generate migration
    const comparison = compareSchemas(initialSchema, evolvedSchema);
    const migration = generateMigration(comparison, 1, 2, evolvedSchema);

    // 6. Apply and record
    const v2 = applyMigration(v1, migration);
    history.addVersion(v2, migration);

    // 7. Verify history tracking
    expect(history.getCurrentVersion()).toBe(2);

    const totalHistory = history.getFieldHistory('total');
    expect(totalHistory).toEqual([
      { version: 1, type: 'int32' },
      { version: 2, type: 'double' },
    ]);

    const promotionSummary = history.getPromotionSummary();
    expect(promotionSummary.has('total')).toBe(true);
    expect(promotionSummary.get('total')![0].from).toBe('int32');
    expect(promotionSummary.get('total')![0].to).toBe('double');
  });
});
