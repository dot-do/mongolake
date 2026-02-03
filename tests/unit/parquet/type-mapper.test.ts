/**
 * Parquet Type Mapper Tests
 *
 * Tests for mapping MongoDB/BSON types to Parquet types with support for:
 * - All BSON type mappings
 * - Nested documents (struct) and arrays (list)
 * - Type promotion rules
 * - Null/optional field handling
 */

import { describe, it, expect } from 'vitest';
import {
  BSONType,
  ParquetPhysicalType,
  ParquetLogicalType,
  TypeMapping,
  bsonToParquet,
  inferBSONType,
  getPromotedMapping,
  canPromoteType,
  mapSchemaToParquet,
  mapValueToParquet,
  getDefaultValue,
  isNumericPhysicalType,
  getPhysicalTypeByteSize,
  type SchemaField,
  type ParquetSchema,
} from '../../../src/parquet/type-mapper.js';

// Import type-mapper module for checking exports
import * as TypeMapper from '../../../src/parquet/type-mapper.js';

// ============================================================================
// BSON Type Constants
// ============================================================================

describe('BSONType', () => {
  it('should define all standard BSON types', () => {
    expect(BSONType.DOUBLE).toBe('double');
    expect(BSONType.STRING).toBe('string');
    expect(BSONType.OBJECT).toBe('object');
    expect(BSONType.ARRAY).toBe('array');
    expect(BSONType.BINARY).toBe('binData');
    expect(BSONType.OBJECT_ID).toBe('objectId');
    expect(BSONType.BOOLEAN).toBe('bool');
    expect(BSONType.DATE).toBe('date');
    expect(BSONType.NULL).toBe('null');
    expect(BSONType.REGEX).toBe('regex');
    expect(BSONType.INT32).toBe('int');
    expect(BSONType.TIMESTAMP).toBe('timestamp');
    expect(BSONType.INT64).toBe('long');
    expect(BSONType.DECIMAL128).toBe('decimal');
    expect(BSONType.MIN_KEY).toBe('minKey');
    expect(BSONType.MAX_KEY).toBe('maxKey');
  });
});

// ============================================================================
// Parquet Type Constants
// ============================================================================

describe('ParquetPhysicalType', () => {
  it('should define all Parquet physical types', () => {
    expect(ParquetPhysicalType.BOOLEAN).toBe('BOOLEAN');
    expect(ParquetPhysicalType.INT32).toBe('INT32');
    expect(ParquetPhysicalType.INT64).toBe('INT64');
    expect(ParquetPhysicalType.INT96).toBe('INT96');
    expect(ParquetPhysicalType.FLOAT).toBe('FLOAT');
    expect(ParquetPhysicalType.DOUBLE).toBe('DOUBLE');
    expect(ParquetPhysicalType.BYTE_ARRAY).toBe('BYTE_ARRAY');
    expect(ParquetPhysicalType.FIXED_LEN_BYTE_ARRAY).toBe('FIXED_LEN_BYTE_ARRAY');
  });
});

describe('ParquetLogicalType', () => {
  it('should define all Parquet logical types', () => {
    expect(ParquetLogicalType.STRING).toBe('STRING');
    expect(ParquetLogicalType.MAP).toBe('MAP');
    expect(ParquetLogicalType.LIST).toBe('LIST');
    expect(ParquetLogicalType.ENUM).toBe('ENUM');
    expect(ParquetLogicalType.DECIMAL).toBe('DECIMAL');
    expect(ParquetLogicalType.DATE).toBe('DATE');
    expect(ParquetLogicalType.TIME_MILLIS).toBe('TIME_MILLIS');
    expect(ParquetLogicalType.TIME_MICROS).toBe('TIME_MICROS');
    expect(ParquetLogicalType.TIMESTAMP_MILLIS).toBe('TIMESTAMP_MILLIS');
    expect(ParquetLogicalType.TIMESTAMP_MICROS).toBe('TIMESTAMP_MICROS');
    expect(ParquetLogicalType.UINT_8).toBe('UINT_8');
    expect(ParquetLogicalType.UINT_16).toBe('UINT_16');
    expect(ParquetLogicalType.UINT_32).toBe('UINT_32');
    expect(ParquetLogicalType.UINT_64).toBe('UINT_64');
    expect(ParquetLogicalType.INT_8).toBe('INT_8');
    expect(ParquetLogicalType.INT_16).toBe('INT_16');
    expect(ParquetLogicalType.INT_32).toBe('INT_32');
    expect(ParquetLogicalType.INT_64).toBe('INT_64');
    expect(ParquetLogicalType.JSON).toBe('JSON');
    expect(ParquetLogicalType.BSON).toBe('BSON');
    expect(ParquetLogicalType.UUID).toBe('UUID');
  });
});

// ============================================================================
// BSON to Parquet Type Mapping
// ============================================================================

describe('bsonToParquet', () => {
  describe('Primitive type mappings', () => {
    it('should map double to DOUBLE', () => {
      const mapping = bsonToParquet(BSONType.DOUBLE);
      expect(mapping.physicalType).toBe(ParquetPhysicalType.DOUBLE);
      expect(mapping.logicalType).toBeUndefined();
    });

    it('should map string to BYTE_ARRAY with STRING logical type', () => {
      const mapping = bsonToParquet(BSONType.STRING);
      expect(mapping.physicalType).toBe(ParquetPhysicalType.BYTE_ARRAY);
      expect(mapping.logicalType).toBe(ParquetLogicalType.STRING);
    });

    it('should map int32 to INT32', () => {
      const mapping = bsonToParquet(BSONType.INT32);
      expect(mapping.physicalType).toBe(ParquetPhysicalType.INT32);
      expect(mapping.logicalType).toBe(ParquetLogicalType.INT_32);
    });

    it('should map int64 to INT64', () => {
      const mapping = bsonToParquet(BSONType.INT64);
      expect(mapping.physicalType).toBe(ParquetPhysicalType.INT64);
      expect(mapping.logicalType).toBe(ParquetLogicalType.INT_64);
    });

    it('should map boolean to BOOLEAN', () => {
      const mapping = bsonToParquet(BSONType.BOOLEAN);
      expect(mapping.physicalType).toBe(ParquetPhysicalType.BOOLEAN);
      expect(mapping.logicalType).toBeUndefined();
    });

    it('should map null to BYTE_ARRAY (variant)', () => {
      const mapping = bsonToParquet(BSONType.NULL);
      expect(mapping.physicalType).toBe(ParquetPhysicalType.BYTE_ARRAY);
      expect(mapping.isVariant).toBe(true);
    });
  });

  describe('Date/time type mappings', () => {
    it('should map date to INT64 with TIMESTAMP_MILLIS', () => {
      const mapping = bsonToParquet(BSONType.DATE);
      expect(mapping.physicalType).toBe(ParquetPhysicalType.INT64);
      expect(mapping.logicalType).toBe(ParquetLogicalType.TIMESTAMP_MILLIS);
    });

    it('should map timestamp to INT64 with TIMESTAMP_MILLIS', () => {
      const mapping = bsonToParquet(BSONType.TIMESTAMP);
      expect(mapping.physicalType).toBe(ParquetPhysicalType.INT64);
      expect(mapping.logicalType).toBe(ParquetLogicalType.TIMESTAMP_MILLIS);
    });
  });

  describe('Binary/ObjectId type mappings', () => {
    it('should map binary to BYTE_ARRAY', () => {
      const mapping = bsonToParquet(BSONType.BINARY);
      expect(mapping.physicalType).toBe(ParquetPhysicalType.BYTE_ARRAY);
      expect(mapping.logicalType).toBeUndefined();
    });

    it('should map objectId to FIXED_LEN_BYTE_ARRAY with length 12', () => {
      const mapping = bsonToParquet(BSONType.OBJECT_ID);
      expect(mapping.physicalType).toBe(ParquetPhysicalType.FIXED_LEN_BYTE_ARRAY);
      expect(mapping.typeLength).toBe(12);
    });
  });

  describe('Complex type mappings', () => {
    it('should map object to BYTE_ARRAY (variant)', () => {
      const mapping = bsonToParquet(BSONType.OBJECT);
      expect(mapping.physicalType).toBe(ParquetPhysicalType.BYTE_ARRAY);
      expect(mapping.isVariant).toBe(true);
    });

    it('should map array to BYTE_ARRAY (variant)', () => {
      const mapping = bsonToParquet(BSONType.ARRAY);
      expect(mapping.physicalType).toBe(ParquetPhysicalType.BYTE_ARRAY);
      expect(mapping.isVariant).toBe(true);
    });

    it('should map decimal128 to FIXED_LEN_BYTE_ARRAY with DECIMAL', () => {
      const mapping = bsonToParquet(BSONType.DECIMAL128);
      expect(mapping.physicalType).toBe(ParquetPhysicalType.FIXED_LEN_BYTE_ARRAY);
      expect(mapping.logicalType).toBe(ParquetLogicalType.DECIMAL);
      expect(mapping.typeLength).toBe(16);
      expect(mapping.precision).toBe(34);
      expect(mapping.scale).toBe(0);
    });

    it('should map regex to BYTE_ARRAY (variant)', () => {
      const mapping = bsonToParquet(BSONType.REGEX);
      expect(mapping.physicalType).toBe(ParquetPhysicalType.BYTE_ARRAY);
      expect(mapping.isVariant).toBe(true);
    });
  });

  describe('Special type mappings', () => {
    it('should map minKey to BYTE_ARRAY (variant)', () => {
      const mapping = bsonToParquet(BSONType.MIN_KEY);
      expect(mapping.physicalType).toBe(ParquetPhysicalType.BYTE_ARRAY);
      expect(mapping.isVariant).toBe(true);
    });

    it('should map maxKey to BYTE_ARRAY (variant)', () => {
      const mapping = bsonToParquet(BSONType.MAX_KEY);
      expect(mapping.physicalType).toBe(ParquetPhysicalType.BYTE_ARRAY);
      expect(mapping.isVariant).toBe(true);
    });

    it('should handle unknown types as variant', () => {
      const mapping = bsonToParquet('unknownType' as any);
      expect(mapping.physicalType).toBe(ParquetPhysicalType.BYTE_ARRAY);
      expect(mapping.isVariant).toBe(true);
    });
  });
});

// ============================================================================
// BSON Type Inference
// ============================================================================

describe('inferBSONType', () => {
  describe('Primitive values', () => {
    it('should infer null type', () => {
      expect(inferBSONType(null)).toBe(BSONType.NULL);
    });

    it('should infer undefined as null type', () => {
      expect(inferBSONType(undefined)).toBe(BSONType.NULL);
    });

    it('should infer string type', () => {
      expect(inferBSONType('hello')).toBe(BSONType.STRING);
      expect(inferBSONType('')).toBe(BSONType.STRING);
    });

    it('should infer boolean type', () => {
      expect(inferBSONType(true)).toBe(BSONType.BOOLEAN);
      expect(inferBSONType(false)).toBe(BSONType.BOOLEAN);
    });

    it('should infer integer values as int32 when in range', () => {
      expect(inferBSONType(0)).toBe(BSONType.INT32);
      expect(inferBSONType(42)).toBe(BSONType.INT32);
      expect(inferBSONType(-100)).toBe(BSONType.INT32);
      expect(inferBSONType(2147483647)).toBe(BSONType.INT32);
      expect(inferBSONType(-2147483648)).toBe(BSONType.INT32);
    });

    it('should infer integer values as int64 when outside int32 range', () => {
      expect(inferBSONType(2147483648)).toBe(BSONType.INT64);
      expect(inferBSONType(-2147483649)).toBe(BSONType.INT64);
      expect(inferBSONType(Number.MAX_SAFE_INTEGER)).toBe(BSONType.INT64);
    });

    it('should infer floating point values as double', () => {
      expect(inferBSONType(3.14)).toBe(BSONType.DOUBLE);
      expect(inferBSONType(0.1)).toBe(BSONType.DOUBLE);
      expect(inferBSONType(-0.5)).toBe(BSONType.DOUBLE);
      expect(inferBSONType(Infinity)).toBe(BSONType.DOUBLE);
      expect(inferBSONType(NaN)).toBe(BSONType.DOUBLE);
    });

    it('should infer BigInt as int64', () => {
      expect(inferBSONType(BigInt(42))).toBe(BSONType.INT64);
      expect(inferBSONType(BigInt('9223372036854775807'))).toBe(BSONType.INT64);
    });
  });

  describe('Date values', () => {
    it('should infer Date type', () => {
      expect(inferBSONType(new Date())).toBe(BSONType.DATE);
      expect(inferBSONType(new Date('2024-01-15'))).toBe(BSONType.DATE);
    });
  });

  describe('Binary values', () => {
    it('should infer Uint8Array as binary', () => {
      expect(inferBSONType(new Uint8Array([1, 2, 3]))).toBe(BSONType.BINARY);
    });

    it('should infer ArrayBuffer as binary', () => {
      expect(inferBSONType(new ArrayBuffer(8))).toBe(BSONType.BINARY);
    });

    it('should infer Buffer as binary', () => {
      // Node.js Buffer extends Uint8Array
      if (typeof Buffer !== 'undefined') {
        expect(inferBSONType(Buffer.from([1, 2, 3]))).toBe(BSONType.BINARY);
      }
    });
  });

  describe('ObjectId values', () => {
    it('should infer ObjectId-like objects', () => {
      // Mock ObjectId-like object
      const objectId = {
        toString: () => '507f1f77bcf86cd799439011',
        toHexString: () => '507f1f77bcf86cd799439011',
      };
      expect(inferBSONType(objectId)).toBe(BSONType.OBJECT_ID);
    });

    it('should not infer regular objects as ObjectId', () => {
      expect(inferBSONType({ toString: () => 'not an objectId' })).toBe(BSONType.OBJECT);
    });
  });

  describe('Array values', () => {
    it('should infer array type', () => {
      expect(inferBSONType([])).toBe(BSONType.ARRAY);
      expect(inferBSONType([1, 2, 3])).toBe(BSONType.ARRAY);
      expect(inferBSONType(['a', 'b'])).toBe(BSONType.ARRAY);
    });
  });

  describe('Object values', () => {
    it('should infer plain objects as object type', () => {
      expect(inferBSONType({})).toBe(BSONType.OBJECT);
      expect(inferBSONType({ foo: 'bar' })).toBe(BSONType.OBJECT);
    });

    it('should infer nested objects', () => {
      expect(inferBSONType({ nested: { deep: { value: 1 } } })).toBe(BSONType.OBJECT);
    });
  });

  describe('RegExp values', () => {
    it('should infer RegExp as regex type', () => {
      expect(inferBSONType(/test/i)).toBe(BSONType.REGEX);
      expect(inferBSONType(new RegExp('pattern'))).toBe(BSONType.REGEX);
    });
  });
});

// ============================================================================
// Type Promotion Rules
// ============================================================================

describe('canPromoteType', () => {
  describe('Same type promotion', () => {
    it('should allow promoting same types', () => {
      expect(canPromoteType(BSONType.INT32, BSONType.INT32)).toBe(true);
      expect(canPromoteType(BSONType.STRING, BSONType.STRING)).toBe(true);
      expect(canPromoteType(BSONType.DOUBLE, BSONType.DOUBLE)).toBe(true);
    });
  });

  describe('Numeric type widening', () => {
    it('should allow promoting int32 to int64', () => {
      expect(canPromoteType(BSONType.INT32, BSONType.INT64)).toBe(true);
    });

    it('should allow promoting int32 to double', () => {
      expect(canPromoteType(BSONType.INT32, BSONType.DOUBLE)).toBe(true);
    });

    it('should allow promoting int64 to double', () => {
      expect(canPromoteType(BSONType.INT64, BSONType.DOUBLE)).toBe(true);
    });

    it('should not allow promoting double to int32', () => {
      expect(canPromoteType(BSONType.DOUBLE, BSONType.INT32)).toBe(false);
    });

    it('should not allow promoting int64 to int32', () => {
      expect(canPromoteType(BSONType.INT64, BSONType.INT32)).toBe(false);
    });
  });

  describe('Date/timestamp promotion', () => {
    it('should allow promoting date to timestamp', () => {
      expect(canPromoteType(BSONType.DATE, BSONType.TIMESTAMP)).toBe(true);
    });

    it('should allow promoting timestamp to date', () => {
      expect(canPromoteType(BSONType.TIMESTAMP, BSONType.DATE)).toBe(true);
    });
  });

  describe('String/objectId promotion', () => {
    it('should allow promoting objectId to string', () => {
      expect(canPromoteType(BSONType.OBJECT_ID, BSONType.STRING)).toBe(true);
    });

    it('should not allow promoting string to objectId', () => {
      expect(canPromoteType(BSONType.STRING, BSONType.OBJECT_ID)).toBe(false);
    });
  });

  describe('Incompatible types', () => {
    it('should not allow promoting string to number', () => {
      expect(canPromoteType(BSONType.STRING, BSONType.INT32)).toBe(false);
      expect(canPromoteType(BSONType.STRING, BSONType.DOUBLE)).toBe(false);
    });

    it('should not allow promoting boolean to number', () => {
      expect(canPromoteType(BSONType.BOOLEAN, BSONType.INT32)).toBe(false);
    });

    it('should not allow promoting array to object', () => {
      expect(canPromoteType(BSONType.ARRAY, BSONType.OBJECT)).toBe(false);
    });
  });
});

describe('getPromotedMapping', () => {
  it('should return the wider numeric type mapping', () => {
    const int32Mapping = bsonToParquet(BSONType.INT32);
    const int64Mapping = bsonToParquet(BSONType.INT64);

    const promoted = getPromotedMapping(int32Mapping, int64Mapping);
    expect(promoted.physicalType).toBe(ParquetPhysicalType.INT64);
  });

  it('should promote int32 and double to double', () => {
    const int32Mapping = bsonToParquet(BSONType.INT32);
    const doubleMapping = bsonToParquet(BSONType.DOUBLE);

    const promoted = getPromotedMapping(int32Mapping, doubleMapping);
    expect(promoted.physicalType).toBe(ParquetPhysicalType.DOUBLE);
  });

  it('should fall back to variant for incompatible types', () => {
    const stringMapping = bsonToParquet(BSONType.STRING);
    const int32Mapping = bsonToParquet(BSONType.INT32);

    const promoted = getPromotedMapping(stringMapping, int32Mapping);
    expect(promoted.isVariant).toBe(true);
  });

  it('should return same mapping when types match', () => {
    const mapping = bsonToParquet(BSONType.STRING);
    const promoted = getPromotedMapping(mapping, mapping);
    expect(promoted).toEqual(mapping);
  });

  it('should handle variant types', () => {
    const variantMapping = bsonToParquet(BSONType.OBJECT);
    const stringMapping = bsonToParquet(BSONType.STRING);

    // Variant absorbs everything
    const promoted = getPromotedMapping(variantMapping, stringMapping);
    expect(promoted.isVariant).toBe(true);
  });
});

// ============================================================================
// Nested Type Handling (Struct and List)
// ============================================================================

describe('mapSchemaToParquet', () => {
  describe('Simple schemas', () => {
    it('should map a flat schema', () => {
      const schema: SchemaField[] = [
        { name: 'name', type: BSONType.STRING },
        { name: 'age', type: BSONType.INT32 },
        { name: 'active', type: BSONType.BOOLEAN },
      ];

      const parquetSchema = mapSchemaToParquet(schema);

      expect(parquetSchema.fields).toHaveLength(3);
      expect(parquetSchema.fields[0].name).toBe('name');
      expect(parquetSchema.fields[0].physicalType).toBe(ParquetPhysicalType.BYTE_ARRAY);
      expect(parquetSchema.fields[1].name).toBe('age');
      expect(parquetSchema.fields[1].physicalType).toBe(ParquetPhysicalType.INT32);
      expect(parquetSchema.fields[2].name).toBe('active');
      expect(parquetSchema.fields[2].physicalType).toBe(ParquetPhysicalType.BOOLEAN);
    });

    it('should handle optional fields', () => {
      const schema: SchemaField[] = [
        { name: 'required', type: BSONType.STRING, optional: false },
        { name: 'optional', type: BSONType.STRING, optional: true },
      ];

      const parquetSchema = mapSchemaToParquet(schema);

      expect(parquetSchema.fields[0].optional).toBe(false);
      expect(parquetSchema.fields[1].optional).toBe(true);
    });
  });

  describe('Nested structs', () => {
    it('should map nested object fields as struct', () => {
      const schema: SchemaField[] = [
        {
          name: 'address',
          type: BSONType.OBJECT,
          children: [
            { name: 'street', type: BSONType.STRING },
            { name: 'city', type: BSONType.STRING },
            { name: 'zip', type: BSONType.INT32 },
          ],
        },
      ];

      const parquetSchema = mapSchemaToParquet(schema);

      expect(parquetSchema.fields).toHaveLength(1);
      expect(parquetSchema.fields[0].name).toBe('address');
      expect(parquetSchema.fields[0].isStruct).toBe(true);
      expect(parquetSchema.fields[0].children).toHaveLength(3);
      expect(parquetSchema.fields[0].children![0].name).toBe('street');
      expect(parquetSchema.fields[0].children![1].name).toBe('city');
      expect(parquetSchema.fields[0].children![2].name).toBe('zip');
    });

    it('should handle deeply nested structs', () => {
      const schema: SchemaField[] = [
        {
          name: 'person',
          type: BSONType.OBJECT,
          children: [
            { name: 'name', type: BSONType.STRING },
            {
              name: 'address',
              type: BSONType.OBJECT,
              children: [
                { name: 'city', type: BSONType.STRING },
                {
                  name: 'geo',
                  type: BSONType.OBJECT,
                  children: [
                    { name: 'lat', type: BSONType.DOUBLE },
                    { name: 'lng', type: BSONType.DOUBLE },
                  ],
                },
              ],
            },
          ],
        },
      ];

      const parquetSchema = mapSchemaToParquet(schema);

      expect(parquetSchema.fields[0].children![1].children![1].children).toHaveLength(2);
    });
  });

  describe('Array types (list)', () => {
    it('should map array fields as list', () => {
      const schema: SchemaField[] = [
        {
          name: 'tags',
          type: BSONType.ARRAY,
          elementType: BSONType.STRING,
        },
      ];

      const parquetSchema = mapSchemaToParquet(schema);

      expect(parquetSchema.fields[0].name).toBe('tags');
      expect(parquetSchema.fields[0].isList).toBe(true);
      expect(parquetSchema.fields[0].elementType).toBeDefined();
      expect(parquetSchema.fields[0].elementType!.physicalType).toBe(
        ParquetPhysicalType.BYTE_ARRAY
      );
    });

    it('should map array of objects', () => {
      const schema: SchemaField[] = [
        {
          name: 'items',
          type: BSONType.ARRAY,
          elementType: BSONType.OBJECT,
          elementChildren: [
            { name: 'name', type: BSONType.STRING },
            { name: 'qty', type: BSONType.INT32 },
          ],
        },
      ];

      const parquetSchema = mapSchemaToParquet(schema);

      expect(parquetSchema.fields[0].isList).toBe(true);
      expect(parquetSchema.fields[0].elementType!.isStruct).toBe(true);
      expect(parquetSchema.fields[0].elementType!.children).toHaveLength(2);
    });

    it('should map nested arrays', () => {
      const schema: SchemaField[] = [
        {
          name: 'matrix',
          type: BSONType.ARRAY,
          elementType: BSONType.ARRAY,
          nestedElementType: BSONType.INT32,
        },
      ];

      const parquetSchema = mapSchemaToParquet(schema);

      expect(parquetSchema.fields[0].isList).toBe(true);
      expect(parquetSchema.fields[0].elementType!.isList).toBe(true);
      expect(parquetSchema.fields[0].elementType!.elementType!.physicalType).toBe(
        ParquetPhysicalType.INT32
      );
    });
  });
});

// ============================================================================
// Value Mapping
// ============================================================================

describe('mapValueToParquet', () => {
  describe('Primitive value mapping', () => {
    it('should map null values', () => {
      const result = mapValueToParquet(null);
      expect(result.value).toBe(null);
      expect(result.isNull).toBe(true);
    });

    it('should map string values', () => {
      const result = mapValueToParquet('hello');
      expect(result.value).toBe('hello');
      expect(result.bsonType).toBe(BSONType.STRING);
    });

    it('should map integer values', () => {
      const result = mapValueToParquet(42);
      expect(result.value).toBe(42);
      expect(result.bsonType).toBe(BSONType.INT32);
    });

    it('should map double values', () => {
      const result = mapValueToParquet(3.14);
      expect(result.value).toBe(3.14);
      expect(result.bsonType).toBe(BSONType.DOUBLE);
    });

    it('should map boolean values', () => {
      const result = mapValueToParquet(true);
      expect(result.value).toBe(true);
      expect(result.bsonType).toBe(BSONType.BOOLEAN);
    });
  });

  describe('Date value mapping', () => {
    it('should map Date to timestamp', () => {
      const date = new Date('2024-01-15T12:30:00Z');
      const result = mapValueToParquet(date);

      expect(result.value).toBe(date.getTime());
      expect(result.bsonType).toBe(BSONType.DATE);
    });
  });

  describe('Binary value mapping', () => {
    it('should map Uint8Array', () => {
      const binary = new Uint8Array([1, 2, 3, 4]);
      const result = mapValueToParquet(binary);

      expect(result.value).toBe(binary);
      expect(result.bsonType).toBe(BSONType.BINARY);
    });
  });

  describe('ObjectId value mapping', () => {
    it('should map ObjectId to bytes', () => {
      const objectId = {
        toString: () => '507f1f77bcf86cd799439011',
        toHexString: () => '507f1f77bcf86cd799439011',
      };
      const result = mapValueToParquet(objectId);

      expect(result.bsonType).toBe(BSONType.OBJECT_ID);
      expect(result.value).toBeInstanceOf(Uint8Array);
      expect((result.value as Uint8Array).length).toBe(12);
    });
  });

  describe('Complex value mapping', () => {
    it('should map arrays to variant encoding', () => {
      const result = mapValueToParquet([1, 2, 3]);

      expect(result.bsonType).toBe(BSONType.ARRAY);
      expect(result.useVariant).toBe(true);
    });

    it('should map objects to variant encoding', () => {
      const result = mapValueToParquet({ foo: 'bar' });

      expect(result.bsonType).toBe(BSONType.OBJECT);
      expect(result.useVariant).toBe(true);
    });
  });
});

// ============================================================================
// Null/Optional Field Handling
// ============================================================================

describe('Null and optional field handling', () => {
  it('should handle null values in optional fields', () => {
    const schema: SchemaField[] = [
      { name: 'optional_string', type: BSONType.STRING, optional: true },
    ];

    const parquetSchema = mapSchemaToParquet(schema);
    expect(parquetSchema.fields[0].optional).toBe(true);

    const result = mapValueToParquet(null);
    expect(result.isNull).toBe(true);
  });

  it('should preserve nullable flag in nested structs', () => {
    const schema: SchemaField[] = [
      {
        name: 'nested',
        type: BSONType.OBJECT,
        optional: true,
        children: [
          { name: 'value', type: BSONType.INT32, optional: true },
        ],
      },
    ];

    const parquetSchema = mapSchemaToParquet(schema);
    expect(parquetSchema.fields[0].optional).toBe(true);
    expect(parquetSchema.fields[0].children![0].optional).toBe(true);
  });

  it('should handle nullable array elements', () => {
    const schema: SchemaField[] = [
      {
        name: 'items',
        type: BSONType.ARRAY,
        elementType: BSONType.STRING,
        elementOptional: true,
      },
    ];

    const parquetSchema = mapSchemaToParquet(schema);
    expect(parquetSchema.fields[0].elementType!.optional).toBe(true);
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('Type Mapper Integration', () => {
  it('should handle a complete MongoDB document schema', () => {
    const schema: SchemaField[] = [
      { name: '_id', type: BSONType.OBJECT_ID },
      { name: 'name', type: BSONType.STRING },
      { name: 'age', type: BSONType.INT32, optional: true },
      { name: 'email', type: BSONType.STRING },
      { name: 'active', type: BSONType.BOOLEAN },
      { name: 'createdAt', type: BSONType.DATE },
      {
        name: 'profile',
        type: BSONType.OBJECT,
        children: [
          { name: 'bio', type: BSONType.STRING, optional: true },
          { name: 'avatar', type: BSONType.BINARY, optional: true },
        ],
      },
      {
        name: 'tags',
        type: BSONType.ARRAY,
        elementType: BSONType.STRING,
      },
      {
        name: 'orders',
        type: BSONType.ARRAY,
        elementType: BSONType.OBJECT,
        elementChildren: [
          { name: 'id', type: BSONType.STRING },
          { name: 'amount', type: BSONType.DOUBLE },
          { name: 'date', type: BSONType.DATE },
        ],
      },
    ];

    const parquetSchema = mapSchemaToParquet(schema);

    expect(parquetSchema.fields).toHaveLength(9);

    // Check _id mapping
    const idField = parquetSchema.fields.find((f) => f.name === '_id')!;
    expect(idField.physicalType).toBe(ParquetPhysicalType.FIXED_LEN_BYTE_ARRAY);
    expect(idField.typeLength).toBe(12);

    // Check profile struct
    const profileField = parquetSchema.fields.find((f) => f.name === 'profile')!;
    expect(profileField.isStruct).toBe(true);
    expect(profileField.children).toHaveLength(2);

    // Check tags list
    const tagsField = parquetSchema.fields.find((f) => f.name === 'tags')!;
    expect(tagsField.isList).toBe(true);
    expect(tagsField.elementType!.physicalType).toBe(ParquetPhysicalType.BYTE_ARRAY);

    // Check orders list of structs
    const ordersField = parquetSchema.fields.find((f) => f.name === 'orders')!;
    expect(ordersField.isList).toBe(true);
    expect(ordersField.elementType!.isStruct).toBe(true);
    expect(ordersField.elementType!.children).toHaveLength(3);
  });

  it('should handle type promotion across schema versions', () => {
    const v1Schema: SchemaField[] = [
      { name: 'count', type: BSONType.INT32 },
    ];

    const v2Schema: SchemaField[] = [
      { name: 'count', type: BSONType.INT64 },
    ];

    const v1Parquet = mapSchemaToParquet(v1Schema);
    const v2Parquet = mapSchemaToParquet(v2Schema);

    // Should be able to promote int32 to int64
    expect(canPromoteType(BSONType.INT32, BSONType.INT64)).toBe(true);

    const promotedMapping = getPromotedMapping(
      { physicalType: v1Parquet.fields[0].physicalType },
      { physicalType: v2Parquet.fields[0].physicalType }
    );

    expect(promotedMapping.physicalType).toBe(ParquetPhysicalType.INT64);
  });
});

// ============================================================================
// Column Type Mapper - Additional Tests for Schema Column Type Mapping
// ============================================================================

/**
 * These tests verify comprehensive column type mapping functionality including:
 * - Type coercion between compatible types
 * - Decimal128 handling with precision and scale
 * - Binary data with subtypes (UUID, MD5, etc.)
 * - Schema inference from documents
 * - Mixed type handling in arrays
 * - Edge cases and error handling
 */

describe('Column Type Mapper - Type Coercion', () => {
  describe('coerceValue export', () => {
    it('should export coerceValue function', () => {
      // RED: This function needs to be implemented
      expect(TypeMapper.coerceValue).toBeDefined();
      expect(typeof TypeMapper.coerceValue).toBe('function');
    });
  });

  describe('Numeric type coercion', () => {
    it('should coerce int32 value to int64', () => {
      // RED: coerceValue function needs to be implemented
      const coerceValue = TypeMapper.coerceValue as (
        value: unknown,
        from: string,
        to: string,
        options?: { allowNarrowing?: boolean }
      ) => unknown;

      const result = coerceValue(42, BSONType.INT32, BSONType.INT64);
      expect(result).toBe(BigInt(42));
    });

    it('should coerce int32 value to double', () => {
      const coerceValue = TypeMapper.coerceValue as (
        value: unknown,
        from: string,
        to: string
      ) => unknown;

      const result = coerceValue(42, BSONType.INT32, BSONType.DOUBLE);
      expect(result).toBe(42.0);
      expect(typeof result).toBe('number');
    });

    it('should coerce int64 value to double', () => {
      const coerceValue = TypeMapper.coerceValue as (
        value: unknown,
        from: string,
        to: string
      ) => unknown;

      const result = coerceValue(BigInt(42), BSONType.INT64, BSONType.DOUBLE);
      expect(result).toBe(42.0);
    });

    it('should throw when coercing incompatible numeric types (double to int32)', () => {
      const coerceValue = TypeMapper.coerceValue as (
        value: unknown,
        from: string,
        to: string
      ) => unknown;

      expect(() => coerceValue(3.14, BSONType.DOUBLE, BSONType.INT32)).toThrow();
    });

    it('should throw when coercing int64 to int32 with value overflow', () => {
      const coerceValue = TypeMapper.coerceValue as (
        value: unknown,
        from: string,
        to: string
      ) => unknown;

      const largeValue = BigInt('9223372036854775807');
      expect(() => coerceValue(largeValue, BSONType.INT64, BSONType.INT32)).toThrow();
    });

    it('should allow int64 to int32 coercion when value fits', () => {
      const coerceValue = TypeMapper.coerceValue as (
        value: unknown,
        from: string,
        to: string,
        options?: { allowNarrowing?: boolean }
      ) => unknown;

      const result = coerceValue(BigInt(100), BSONType.INT64, BSONType.INT32, { allowNarrowing: true });
      expect(result).toBe(100);
    });
  });

  describe('String type coercion', () => {
    it('should coerce ObjectId to string', () => {
      const coerceValue = TypeMapper.coerceValue as (
        value: unknown,
        from: string,
        to: string
      ) => unknown;

      const objectId = {
        toString: () => '507f1f77bcf86cd799439011',
        toHexString: () => '507f1f77bcf86cd799439011',
      };

      const result = coerceValue(objectId, BSONType.OBJECT_ID, BSONType.STRING);
      expect(result).toBe('507f1f77bcf86cd799439011');
    });

    it('should coerce Date to string (ISO format)', () => {
      const coerceValue = TypeMapper.coerceValue as (
        value: unknown,
        from: string,
        to: string
      ) => unknown;

      const date = new Date('2024-01-15T12:30:00.000Z');
      const result = coerceValue(date, BSONType.DATE, BSONType.STRING);
      expect(result).toBe('2024-01-15T12:30:00.000Z');
    });

    it('should coerce number to string', () => {
      const coerceValue = TypeMapper.coerceValue as (
        value: unknown,
        from: string,
        to: string
      ) => unknown;

      const result = coerceValue(42.5, BSONType.DOUBLE, BSONType.STRING);
      expect(result).toBe('42.5');
    });

    it('should coerce boolean to string', () => {
      const coerceValue = TypeMapper.coerceValue as (
        value: unknown,
        from: string,
        to: string
      ) => unknown;

      expect(coerceValue(true, BSONType.BOOLEAN, BSONType.STRING)).toBe('true');
      expect(coerceValue(false, BSONType.BOOLEAN, BSONType.STRING)).toBe('false');
    });
  });

  describe('Date/timestamp coercion', () => {
    it('should coerce Date to timestamp', () => {
      const coerceValue = TypeMapper.coerceValue as (
        value: unknown,
        from: string,
        to: string
      ) => unknown;

      const date = new Date('2024-01-15T12:30:00.000Z');
      const result = coerceValue(date, BSONType.DATE, BSONType.TIMESTAMP);
      expect(result).toBe(date.getTime());
    });

    it('should coerce timestamp to Date', () => {
      const coerceValue = TypeMapper.coerceValue as (
        value: unknown,
        from: string,
        to: string
      ) => unknown;

      const timestamp = 1705321800000;
      const result = coerceValue(timestamp, BSONType.TIMESTAMP, BSONType.DATE) as Date;
      expect(result).toBeInstanceOf(Date);
      expect(result.getTime()).toBe(timestamp);
    });

    it('should coerce string to Date (ISO format)', () => {
      const coerceValue = TypeMapper.coerceValue as (
        value: unknown,
        from: string,
        to: string
      ) => unknown;

      const result = coerceValue('2024-01-15T12:30:00.000Z', BSONType.STRING, BSONType.DATE) as Date;
      expect(result).toBeInstanceOf(Date);
      expect(result.toISOString()).toBe('2024-01-15T12:30:00.000Z');
    });
  });
});

describe('Column Type Mapper - Decimal128 Handling', () => {
  describe('Decimal128 function exports', () => {
    it('should export mapDecimal128ToParquet function', () => {
      // RED: This function needs to be implemented
      expect(TypeMapper.mapDecimal128ToParquet).toBeDefined();
      expect(typeof TypeMapper.mapDecimal128ToParquet).toBe('function');
    });

    it('should export inferDecimal128Properties function', () => {
      // RED: This function needs to be implemented
      expect(TypeMapper.inferDecimal128Properties).toBeDefined();
      expect(typeof TypeMapper.inferDecimal128Properties).toBe('function');
    });

    it('should export mapDecimal128Value function', () => {
      // RED: This function needs to be implemented
      expect(TypeMapper.mapDecimal128Value).toBeDefined();
      expect(typeof TypeMapper.mapDecimal128Value).toBe('function');
    });

    it('should export decimal128ToBytes function', () => {
      // RED: This function needs to be implemented
      expect(TypeMapper.decimal128ToBytes).toBeDefined();
      expect(typeof TypeMapper.decimal128ToBytes).toBe('function');
    });
  });

  describe('Decimal128 type mapping', () => {
    it('should map Decimal128 with custom precision and scale', () => {
      const mapDecimal128ToParquet = TypeMapper.mapDecimal128ToParquet as (
        options: { precision: number; scale: number }
      ) => TypeMapping;

      const mapping = mapDecimal128ToParquet({ precision: 18, scale: 4 });
      expect(mapping.physicalType).toBe(ParquetPhysicalType.FIXED_LEN_BYTE_ARRAY);
      expect(mapping.logicalType).toBe(ParquetLogicalType.DECIMAL);
      expect(mapping.precision).toBe(18);
      expect(mapping.scale).toBe(4);
      expect(mapping.typeLength).toBe(16);
    });

    it('should infer precision and scale from Decimal128 value', () => {
      const inferDecimal128Properties = TypeMapper.inferDecimal128Properties as (
        decimal: { toString: () => string }
      ) => { precision: number; scale: number };

      // Mock Decimal128-like value
      const decimal = {
        toString: () => '12345.6789',
        toJSON: () => ({ $numberDecimal: '12345.6789' }),
      };

      const props = inferDecimal128Properties(decimal);
      expect(props.precision).toBe(9); // 5 digits + 4 decimal places
      expect(props.scale).toBe(4);
    });

    it('should handle Decimal128 with scientific notation', () => {
      const inferDecimal128Properties = TypeMapper.inferDecimal128Properties as (
        decimal: { toString: () => string }
      ) => { precision: number; scale: number };

      const decimal = {
        toString: () => '1.23E+10',
        toJSON: () => ({ $numberDecimal: '1.23E+10' }),
      };

      const props = inferDecimal128Properties(decimal);
      expect(props.precision).toBeGreaterThan(0);
    });

    it('should handle Decimal128 special values (Infinity, NaN)', () => {
      const mapDecimal128Value = TypeMapper.mapDecimal128Value as (
        decimal: { toString: () => string }
      ) => { isSpecial: boolean; specialType?: string };

      const infinity = { toString: () => 'Infinity' };
      const nan = { toString: () => 'NaN' };

      const infResult = mapDecimal128Value(infinity);
      const nanResult = mapDecimal128Value(nan);

      expect(infResult.isSpecial).toBe(true);
      expect(infResult.specialType).toBe('infinity');
      expect(nanResult.isSpecial).toBe(true);
      expect(nanResult.specialType).toBe('nan');
    });

    it('should convert Decimal128 to bytes correctly', () => {
      const decimal128ToBytes = TypeMapper.decimal128ToBytes as (
        decimal: { toString: () => string; bytes?: Uint8Array }
      ) => Uint8Array;

      const decimal = {
        toString: () => '123.456',
        bytes: new Uint8Array(16),
      };

      const bytes = decimal128ToBytes(decimal);
      expect(bytes).toBeInstanceOf(Uint8Array);
      expect(bytes.length).toBe(16);
    });
  });

  describe('Decimal128 value mapping', () => {
    it('should detect Decimal128-like values in inferBSONType', () => {
      // RED: inferBSONType needs to detect Decimal128-like objects
      const decimal = {
        toString: () => '99.99',
        toJSON: () => ({ $numberDecimal: '99.99' }),
      };

      const bsonType = inferBSONType(decimal);
      expect(bsonType).toBe(BSONType.DECIMAL128);
    });

    it('should map Decimal128 value to Parquet format', () => {
      const decimal = {
        toString: () => '99.99',
        toJSON: () => ({ $numberDecimal: '99.99' }),
      };

      const result = mapValueToParquet(decimal);
      expect(result.bsonType).toBe(BSONType.DECIMAL128);
      // Should include precision/scale info for proper storage
    });
  });
});

describe('Column Type Mapper - Binary Data Handling', () => {
  describe('Binary function exports', () => {
    it('should export mapBinaryToParquet function', () => {
      // RED: This function needs to be implemented
      expect(TypeMapper.mapBinaryToParquet).toBeDefined();
      expect(typeof TypeMapper.mapBinaryToParquet).toBe('function');
    });

    it('should export mapBinaryValue function', () => {
      // RED: This function needs to be implemented
      expect(TypeMapper.mapBinaryValue).toBeDefined();
      expect(typeof TypeMapper.mapBinaryValue).toBe('function');
    });
  });

  describe('Binary subtype mapping', () => {
    it('should map UUID binary subtype to correct Parquet type', () => {
      const mapBinaryToParquet = TypeMapper.mapBinaryToParquet as (
        options: { subtype: number }
      ) => TypeMapping;

      const mapping = mapBinaryToParquet({ subtype: 0x04 }); // UUID subtype
      expect(mapping.physicalType).toBe(ParquetPhysicalType.FIXED_LEN_BYTE_ARRAY);
      expect(mapping.logicalType).toBe(ParquetLogicalType.UUID);
      expect(mapping.typeLength).toBe(16);
    });

    it('should map MD5 binary subtype', () => {
      const mapBinaryToParquet = TypeMapper.mapBinaryToParquet as (
        options: { subtype: number }
      ) => TypeMapping;

      const mapping = mapBinaryToParquet({ subtype: 0x05 }); // MD5 subtype
      expect(mapping.physicalType).toBe(ParquetPhysicalType.FIXED_LEN_BYTE_ARRAY);
      expect(mapping.typeLength).toBe(16);
    });

    it('should map generic binary to BYTE_ARRAY', () => {
      const mapBinaryToParquet = TypeMapper.mapBinaryToParquet as (
        options: { subtype: number }
      ) => TypeMapping;

      const mapping = mapBinaryToParquet({ subtype: 0x00 }); // Generic binary
      expect(mapping.physicalType).toBe(ParquetPhysicalType.BYTE_ARRAY);
    });

    it('should map user-defined binary subtype', () => {
      const mapBinaryToParquet = TypeMapper.mapBinaryToParquet as (
        options: { subtype: number }
      ) => TypeMapping;

      const mapping = mapBinaryToParquet({ subtype: 0x80 }); // User-defined
      expect(mapping.physicalType).toBe(ParquetPhysicalType.BYTE_ARRAY);
    });
  });

  describe('Binary value mapping', () => {
    it('should detect and map MongoDB Binary objects', () => {
      // Mock MongoDB Binary object
      const binary = {
        sub_type: 0x04,
        buffer: new Uint8Array(16),
        length: () => 16,
        toString: () => '[Binary UUID]',
      };

      // RED: inferBSONType should detect Binary objects with sub_type property
      const bsonType = inferBSONType(binary);
      expect(bsonType).toBe(BSONType.BINARY);

      const result = mapValueToParquet(binary);
      expect(result.bsonType).toBe(BSONType.BINARY);
    });

    it('should preserve binary subtype information in mapped value', () => {
      const mapBinaryValue = TypeMapper.mapBinaryValue as (
        binary: { sub_type: number; buffer: Uint8Array }
      ) => { subtype: number; value: Uint8Array };

      const uuid = {
        sub_type: 0x04,
        buffer: new Uint8Array(16).fill(0xab),
      };

      const result = mapBinaryValue(uuid);
      expect(result.subtype).toBe(0x04);
      expect(result.value).toBeInstanceOf(Uint8Array);
      expect(result.value.length).toBe(16);
    });
  });
});

describe('Column Type Mapper - Schema Inference', () => {
  describe('Schema inference function exports', () => {
    it('should export inferSchemaFromDocument function', () => {
      // RED: This function needs to be implemented
      expect(TypeMapper.inferSchemaFromDocument).toBeDefined();
      expect(typeof TypeMapper.inferSchemaFromDocument).toBe('function');
    });

    it('should export mergeSchemas function', () => {
      // RED: This function needs to be implemented
      expect(TypeMapper.mergeSchemas).toBeDefined();
      expect(typeof TypeMapper.mergeSchemas).toBe('function');
    });

    it('should export inferSchemaFromDocuments function', () => {
      // RED: This function needs to be implemented (convenience function)
      expect(TypeMapper.inferSchemaFromDocuments).toBeDefined();
      expect(typeof TypeMapper.inferSchemaFromDocuments).toBe('function');
    });
  });

  describe('Schema inference from documents', () => {
    it('should infer schema from a single document', () => {
      const inferSchemaFromDocument = TypeMapper.inferSchemaFromDocument as (
        doc: Record<string, unknown>
      ) => SchemaField[];

      const doc = {
        _id: { toString: () => '507f1f77bcf86cd799439011', toHexString: () => '507f1f77bcf86cd799439011' },
        name: 'Alice',
        age: 30,
        score: 95.5,
        active: true,
        createdAt: new Date('2024-01-15'),
        tags: ['developer', 'designer'],
      };

      const schema = inferSchemaFromDocument(doc);

      expect(schema).toHaveLength(7);
      expect(schema.find(f => f.name === '_id')?.type).toBe(BSONType.OBJECT_ID);
      expect(schema.find(f => f.name === 'name')?.type).toBe(BSONType.STRING);
      expect(schema.find(f => f.name === 'age')?.type).toBe(BSONType.INT32);
      expect(schema.find(f => f.name === 'score')?.type).toBe(BSONType.DOUBLE);
      expect(schema.find(f => f.name === 'active')?.type).toBe(BSONType.BOOLEAN);
      expect(schema.find(f => f.name === 'createdAt')?.type).toBe(BSONType.DATE);
      expect(schema.find(f => f.name === 'tags')?.type).toBe(BSONType.ARRAY);
      expect(schema.find(f => f.name === 'tags')?.elementType).toBe(BSONType.STRING);
    });

    it('should infer nested object schema', () => {
      const inferSchemaFromDocument = TypeMapper.inferSchemaFromDocument as (
        doc: Record<string, unknown>
      ) => SchemaField[];

      const doc = {
        name: 'Company',
        address: {
          street: '123 Main St',
          city: 'NYC',
          zip: 10001,
          location: {
            lat: 40.7128,
            lng: -74.0060,
          },
        },
      };

      const schema = inferSchemaFromDocument(doc);

      const addressField = schema.find(f => f.name === 'address');
      expect(addressField?.type).toBe(BSONType.OBJECT);
      expect(addressField?.children).toHaveLength(4);

      const locationField = addressField?.children?.find(f => f.name === 'location');
      expect(locationField?.type).toBe(BSONType.OBJECT);
      expect(locationField?.children).toHaveLength(2);
    });

    it('should infer array element type from first non-null element', () => {
      const inferSchemaFromDocument = TypeMapper.inferSchemaFromDocument as (
        doc: Record<string, unknown>
      ) => SchemaField[];

      const doc = {
        scores: [null, 85.5, 92.0, null, 78.5],
      };

      const schema = inferSchemaFromDocument(doc);
      const scoresField = schema.find(f => f.name === 'scores');

      expect(scoresField?.type).toBe(BSONType.ARRAY);
      expect(scoresField?.elementType).toBe(BSONType.DOUBLE);
      expect(scoresField?.elementOptional).toBe(true);
    });

    it('should handle empty arrays with unknown element type', () => {
      const inferSchemaFromDocument = TypeMapper.inferSchemaFromDocument as (
        doc: Record<string, unknown>
      ) => SchemaField[];

      const doc = {
        items: [],
      };

      const schema = inferSchemaFromDocument(doc);
      const itemsField = schema.find(f => f.name === 'items');

      expect(itemsField?.type).toBe(BSONType.ARRAY);
      // Should mark as variant or unknown when empty
      expect(itemsField?.elementType).toBeUndefined();
    });
  });

  describe('Schema merging from multiple documents', () => {
    it('should merge schemas from multiple documents', () => {
      const mergeSchemas = TypeMapper.mergeSchemas as (
        schemas: SchemaField[][]
      ) => SchemaField[];

      const schema1: SchemaField[] = [
        { name: 'id', type: BSONType.INT32 },
        { name: 'name', type: BSONType.STRING },
      ];

      const schema2: SchemaField[] = [
        { name: 'id', type: BSONType.INT32 },
        { name: 'email', type: BSONType.STRING },
      ];

      const merged = mergeSchemas([schema1, schema2]);

      expect(merged).toHaveLength(3);
      expect(merged.find(f => f.name === 'id')?.type).toBe(BSONType.INT32);
      expect(merged.find(f => f.name === 'name')?.type).toBe(BSONType.STRING);
      expect(merged.find(f => f.name === 'name')?.optional).toBe(true); // Not in all docs
      expect(merged.find(f => f.name === 'email')?.type).toBe(BSONType.STRING);
      expect(merged.find(f => f.name === 'email')?.optional).toBe(true); // Not in all docs
    });

    it('should promote types when merging conflicting schemas', () => {
      const mergeSchemas = TypeMapper.mergeSchemas as (
        schemas: SchemaField[][]
      ) => SchemaField[];

      const schema1: SchemaField[] = [
        { name: 'count', type: BSONType.INT32 },
      ];

      const schema2: SchemaField[] = [
        { name: 'count', type: BSONType.INT64 },
      ];

      const merged = mergeSchemas([schema1, schema2]);

      expect(merged.find(f => f.name === 'count')?.type).toBe(BSONType.INT64);
    });

    it('should fall back to variant for incompatible types', () => {
      const mergeSchemas = TypeMapper.mergeSchemas as (
        schemas: SchemaField[][]
      ) => SchemaField[];

      const schema1: SchemaField[] = [
        { name: 'value', type: BSONType.STRING },
      ];

      const schema2: SchemaField[] = [
        { name: 'value', type: BSONType.INT32 },
      ];

      const merged = mergeSchemas([schema1, schema2]);

      // Incompatible types should result in variant/any type
      const valueField = merged.find(f => f.name === 'value');
      expect(valueField?.type).toBe(BSONType.OBJECT); // Variant fallback
    });
  });
});

describe('Column Type Mapper - Null Handling', () => {
  describe('Null value detection and mapping', () => {
    it('should distinguish between null and undefined', () => {
      const nullResult = mapValueToParquet(null);
      const undefinedResult = mapValueToParquet(undefined);

      expect(nullResult.isNull).toBe(true);
      expect(undefinedResult.isNull).toBe(true);
      // Both should map to null type
      expect(nullResult.bsonType).toBe(BSONType.NULL);
      expect(undefinedResult.bsonType).toBe(BSONType.NULL);
    });

    it('should export mapSpecialValue function', () => {
      // RED: This function needs to be implemented
      expect(TypeMapper.mapSpecialValue).toBeDefined();
      expect(typeof TypeMapper.mapSpecialValue).toBe('function');
    });

    it('should handle BSON MinKey as null-like special value', () => {
      const mapSpecialValue = TypeMapper.mapSpecialValue as (
        value: { _bsontype: string }
      ) => { bsonType: string; isSpecialBoundary: boolean };

      const minKey = { _bsontype: 'MinKey' };
      const result = mapSpecialValue(minKey);

      expect(result.bsonType).toBe(BSONType.MIN_KEY);
      expect(result.isSpecialBoundary).toBe(true);
    });

    it('should handle BSON MaxKey as null-like special value', () => {
      const mapSpecialValue = TypeMapper.mapSpecialValue as (
        value: { _bsontype: string }
      ) => { bsonType: string; isSpecialBoundary: boolean };

      const maxKey = { _bsontype: 'MaxKey' };
      const result = mapSpecialValue(maxKey);

      expect(result.bsonType).toBe(BSONType.MAX_KEY);
      expect(result.isSpecialBoundary).toBe(true);
    });
  });

  describe('Optional field handling', () => {
    it('should mark fields as optional when null values are present', () => {
      const inferSchemaFromDocuments = TypeMapper.inferSchemaFromDocuments as (
        docs: Record<string, unknown>[]
      ) => SchemaField[];

      const docs = [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: null },
        { name: 'Charlie' }, // age missing
      ];

      const schema = inferSchemaFromDocuments(docs);

      expect(schema.find(f => f.name === 'name')?.optional).toBe(false);
      expect(schema.find(f => f.name === 'age')?.optional).toBe(true);
    });

    it('should propagate nullable through nested structures', () => {
      const inferSchemaFromDocuments = TypeMapper.inferSchemaFromDocuments as (
        docs: Record<string, unknown>[]
      ) => SchemaField[];

      const docs = [
        { profile: { bio: 'Hello', avatar: 'url' } },
        { profile: { bio: 'Hi' } }, // avatar missing
        { profile: null }, // entire profile null
      ];

      const schema = inferSchemaFromDocuments(docs);

      const profileField = schema.find(f => f.name === 'profile');
      expect(profileField?.optional).toBe(true);

      const avatarField = profileField?.children?.find(f => f.name === 'avatar');
      expect(avatarField?.optional).toBe(true);
    });
  });
});

describe('Column Type Mapper - Array Type Handling', () => {
  describe('Array inference function exports', () => {
    it('should export inferArrayElementType function', () => {
      // RED: This function needs to be implemented
      expect(TypeMapper.inferArrayElementType).toBeDefined();
      expect(typeof TypeMapper.inferArrayElementType).toBe('function');
    });
  });

  describe('Mixed type arrays', () => {
    it('should detect mixed type arrays and use variant encoding', () => {
      const inferArrayElementType = TypeMapper.inferArrayElementType as (
        array: unknown[]
      ) => { isMixed: boolean; elementType: string; nestedElementType?: string; elementChildren?: SchemaField[]; hasHoles?: boolean };

      const mixedArray = [1, 'two', 3, 'four'];
      const result = inferArrayElementType(mixedArray);

      expect(result.isMixed).toBe(true);
      expect(result.elementType).toBe(BSONType.OBJECT); // Variant fallback
    });

    it('should promote compatible numeric types in arrays', () => {
      const inferArrayElementType = TypeMapper.inferArrayElementType as (
        array: unknown[]
      ) => { isMixed: boolean; elementType: string };

      const numericArray = [1, 2, 3.14, 4, 5.5];
      const result = inferArrayElementType(numericArray);

      expect(result.isMixed).toBe(false);
      expect(result.elementType).toBe(BSONType.DOUBLE); // Promoted from int to double
    });

    it('should detect array of arrays and infer nested element type', () => {
      const inferArrayElementType = TypeMapper.inferArrayElementType as (
        array: unknown[]
      ) => { elementType: string; nestedElementType?: string };

      const nestedArray = [[1, 2], [3, 4], [5, 6]];
      const result = inferArrayElementType(nestedArray);

      expect(result.elementType).toBe(BSONType.ARRAY);
      expect(result.nestedElementType).toBe(BSONType.INT32);
    });

    it('should detect array of objects and infer object schema', () => {
      const inferArrayElementType = TypeMapper.inferArrayElementType as (
        array: unknown[]
      ) => { elementType: string; elementChildren?: SchemaField[] };

      const objectArray = [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 },
      ];
      const result = inferArrayElementType(objectArray);

      expect(result.elementType).toBe(BSONType.OBJECT);
      expect(result.elementChildren).toBeDefined();
      expect(result.elementChildren).toHaveLength(2);
    });
  });

  describe('Sparse arrays', () => {
    it('should handle sparse arrays with undefined elements', () => {
      const inferArrayElementType = TypeMapper.inferArrayElementType as (
        array: unknown[]
      ) => { elementType: string; hasHoles?: boolean };

      // eslint-disable-next-line no-sparse-arrays
      const sparseArray = [1, , 3, , 5]; // Sparse array with holes
      const result = inferArrayElementType(sparseArray);

      expect(result.elementType).toBe(BSONType.INT32);
      expect(result.hasHoles).toBe(true);
    });
  });
});

describe('Column Type Mapper - Edge Cases', () => {
  describe('Special numeric values', () => {
    it('should handle -0 (negative zero)', () => {
      const result = mapValueToParquet(-0);
      expect(result.bsonType).toBe(BSONType.DOUBLE); // -0 is a double
      expect(Object.is(result.value, -0)).toBe(true);
    });

    it('should handle Number.EPSILON', () => {
      const result = mapValueToParquet(Number.EPSILON);
      expect(result.bsonType).toBe(BSONType.DOUBLE);
    });

    it('should handle subnormal numbers', () => {
      // Number.MIN_VALUE is the smallest positive subnormal number in IEEE 754
      // Note: Number.MIN_VALUE / 2 underflows to 0 in JavaScript, so we use MIN_VALUE directly
      const subnormal = Number.MIN_VALUE;
      const result = mapValueToParquet(subnormal);
      expect(result.bsonType).toBe(BSONType.DOUBLE);
    });
  });

  describe('String edge cases', () => {
    it('should handle strings with null characters', () => {
      const stringWithNull = 'hello\0world';
      const result = mapValueToParquet(stringWithNull);
      expect(result.bsonType).toBe(BSONType.STRING);
      expect(result.value).toBe('hello\0world');
    });

    it('should handle surrogate pairs', () => {
      const emoji = '\uD83D\uDE00'; // Grinning face emoji
      const result = mapValueToParquet(emoji);
      expect(result.bsonType).toBe(BSONType.STRING);
      expect(result.value).toBe(emoji);
    });

    it('should handle very long strings', () => {
      const longString = 'a'.repeat(10_000_000); // 10MB string
      const result = mapValueToParquet(longString);
      expect(result.bsonType).toBe(BSONType.STRING);
    });
  });

  describe('Date edge cases', () => {
    it('should handle dates before Unix epoch', () => {
      const preEpoch = new Date('1900-01-01T00:00:00Z');
      const result = mapValueToParquet(preEpoch);
      expect(result.bsonType).toBe(BSONType.DATE);
      expect(result.value).toBe(preEpoch.getTime());
      expect(result.value).toBeLessThan(0);
    });

    it('should handle far future dates', () => {
      const farFuture = new Date('9999-12-31T23:59:59Z');
      const result = mapValueToParquet(farFuture);
      expect(result.bsonType).toBe(BSONType.DATE);
    });

    it('should handle Invalid Date', () => {
      const invalidDate = new Date('invalid');
      const result = mapValueToParquet(invalidDate);
      expect(result.bsonType).toBe(BSONType.DATE);
      expect(Number.isNaN(result.value)).toBe(true);
    });
  });

  describe('Object edge cases', () => {
    it('should handle objects with symbol keys', () => {
      const sym = Symbol('key');
      const obj = { [sym]: 'value', name: 'test' };
      const result = mapValueToParquet(obj);
      expect(result.bsonType).toBe(BSONType.OBJECT);
      // Symbol keys should be ignored
    });

    it('should handle objects with circular references', () => {
      const obj: Record<string, unknown> = { name: 'circular' };
      obj.self = obj;

      // RED: Should detect circular references and throw meaningful error
      expect(() => mapValueToParquet(obj)).toThrow(/circular/i);
    });

    it('should handle objects with prototype pollution attempts', () => {
      const obj = JSON.parse('{"__proto__": {"polluted": true}}');
      const result = mapValueToParquet(obj);
      expect(result.bsonType).toBe(BSONType.OBJECT);
      // Should not pollute prototype
      expect(({} as any).polluted).toBeUndefined();
    });
  });
});

describe('Column Type Mapper - Utility Functions', () => {
  describe('getDefaultValue', () => {
    it('should return false for BOOLEAN', () => {
      expect(getDefaultValue(ParquetPhysicalType.BOOLEAN)).toBe(false);
    });

    it('should return 0 for numeric types', () => {
      expect(getDefaultValue(ParquetPhysicalType.INT32)).toBe(0);
      expect(getDefaultValue(ParquetPhysicalType.INT64)).toBe(0);
      expect(getDefaultValue(ParquetPhysicalType.FLOAT)).toBe(0);
      expect(getDefaultValue(ParquetPhysicalType.DOUBLE)).toBe(0);
    });

    it('should return empty Uint8Array for byte arrays', () => {
      const result = getDefaultValue(ParquetPhysicalType.BYTE_ARRAY) as Uint8Array;
      expect(result).toBeInstanceOf(Uint8Array);
      expect(result.length).toBe(0);
    });

    it('should return null for INT96', () => {
      // RED: INT96 should return null as default value
      expect(getDefaultValue(ParquetPhysicalType.INT96)).toBe(null);
    });
  });

  describe('isNumericPhysicalType', () => {
    it('should return true for all numeric types', () => {
      expect(isNumericPhysicalType(ParquetPhysicalType.INT32)).toBe(true);
      expect(isNumericPhysicalType(ParquetPhysicalType.INT64)).toBe(true);
      expect(isNumericPhysicalType(ParquetPhysicalType.INT96)).toBe(true);
      expect(isNumericPhysicalType(ParquetPhysicalType.FLOAT)).toBe(true);
      expect(isNumericPhysicalType(ParquetPhysicalType.DOUBLE)).toBe(true);
    });

    it('should return false for non-numeric types', () => {
      expect(isNumericPhysicalType(ParquetPhysicalType.BOOLEAN)).toBe(false);
      expect(isNumericPhysicalType(ParquetPhysicalType.BYTE_ARRAY)).toBe(false);
      expect(isNumericPhysicalType(ParquetPhysicalType.FIXED_LEN_BYTE_ARRAY)).toBe(false);
    });
  });

  describe('getPhysicalTypeByteSize', () => {
    it('should return correct sizes for fixed-size types', () => {
      expect(getPhysicalTypeByteSize(ParquetPhysicalType.BOOLEAN)).toBe(1);
      expect(getPhysicalTypeByteSize(ParquetPhysicalType.INT32)).toBe(4);
      expect(getPhysicalTypeByteSize(ParquetPhysicalType.FLOAT)).toBe(4);
      expect(getPhysicalTypeByteSize(ParquetPhysicalType.INT64)).toBe(8);
      expect(getPhysicalTypeByteSize(ParquetPhysicalType.DOUBLE)).toBe(8);
      expect(getPhysicalTypeByteSize(ParquetPhysicalType.INT96)).toBe(12);
    });

    it('should return -1 for variable length types', () => {
      expect(getPhysicalTypeByteSize(ParquetPhysicalType.BYTE_ARRAY)).toBe(-1);
    });

    it('should use typeLength for FIXED_LEN_BYTE_ARRAY', () => {
      expect(getPhysicalTypeByteSize(ParquetPhysicalType.FIXED_LEN_BYTE_ARRAY, 16)).toBe(16);
      expect(getPhysicalTypeByteSize(ParquetPhysicalType.FIXED_LEN_BYTE_ARRAY)).toBe(-1);
    });
  });
});

// ============================================================================
// Extensible Type Registry Tests
// ============================================================================

describe('Column Type Mapper - Type Registry', () => {
  // Clear the registry before each test to avoid interference
  beforeEach(() => {
    TypeMapper.typeRegistry.clear();
  });

  describe('typeRegistry exports', () => {
    it('should export typeRegistry', () => {
      expect(TypeMapper.typeRegistry).toBeDefined();
    });

    it('should have register method', () => {
      expect(typeof TypeMapper.typeRegistry.register).toBe('function');
    });

    it('should have unregister method', () => {
      expect(typeof TypeMapper.typeRegistry.unregister).toBe('function');
    });

    it('should have findHandler method', () => {
      expect(typeof TypeMapper.typeRegistry.findHandler).toBe('function');
    });

    it('should have getHandlers method', () => {
      expect(typeof TypeMapper.typeRegistry.getHandlers).toBe('function');
    });

    it('should have has method', () => {
      expect(typeof TypeMapper.typeRegistry.has).toBe('function');
    });

    it('should have clear method', () => {
      expect(typeof TypeMapper.typeRegistry.clear).toBe('function');
    });
  });

  describe('register and unregister', () => {
    it('should register a custom type handler', () => {
      const handler = {
        name: 'TestType',
        detect: () => false,
        getMapping: () => ({ physicalType: ParquetPhysicalType.BYTE_ARRAY }),
        transformValue: (v: unknown) => v,
        getBSONType: () => BSONType.OBJECT,
      };

      TypeMapper.typeRegistry.register(handler);
      expect(TypeMapper.typeRegistry.has('TestType')).toBe(true);
    });

    it('should throw error when registering duplicate handler', () => {
      const handler = {
        name: 'DuplicateType',
        detect: () => false,
        getMapping: () => ({ physicalType: ParquetPhysicalType.BYTE_ARRAY }),
        transformValue: (v: unknown) => v,
        getBSONType: () => BSONType.OBJECT,
      };

      TypeMapper.typeRegistry.register(handler);
      expect(() => TypeMapper.typeRegistry.register(handler)).toThrow(/already registered/);
    });

    it('should unregister a handler', () => {
      const handler = {
        name: 'RemovableType',
        detect: () => false,
        getMapping: () => ({ physicalType: ParquetPhysicalType.BYTE_ARRAY }),
        transformValue: (v: unknown) => v,
        getBSONType: () => BSONType.OBJECT,
      };

      TypeMapper.typeRegistry.register(handler);
      expect(TypeMapper.typeRegistry.has('RemovableType')).toBe(true);

      const removed = TypeMapper.typeRegistry.unregister('RemovableType');
      expect(removed).toBe(true);
      expect(TypeMapper.typeRegistry.has('RemovableType')).toBe(false);
    });

    it('should return false when unregistering non-existent handler', () => {
      const removed = TypeMapper.typeRegistry.unregister('NonExistentType');
      expect(removed).toBe(false);
    });
  });

  describe('findHandler', () => {
    it('should find matching handler for value', () => {
      const handler = {
        name: 'GeoPoint',
        detect: (v: unknown) => {
          return typeof v === 'object' && v !== null && 'lat' in v && 'lng' in v;
        },
        getMapping: () => ({
          physicalType: ParquetPhysicalType.BYTE_ARRAY,
          logicalType: ParquetLogicalType.JSON,
        }),
        transformValue: (v: unknown) => JSON.stringify(v),
        getBSONType: () => BSONType.OBJECT,
      };

      TypeMapper.typeRegistry.register(handler);

      const geoPoint = { lat: 40.7128, lng: -74.0060 };
      const foundHandler = TypeMapper.typeRegistry.findHandler(geoPoint);
      expect(foundHandler).toBeDefined();
      expect(foundHandler?.name).toBe('GeoPoint');
    });

    it('should return undefined when no handler matches', () => {
      const handler = {
        name: 'SpecificType',
        detect: () => false,
        getMapping: () => ({ physicalType: ParquetPhysicalType.BYTE_ARRAY }),
        transformValue: (v: unknown) => v,
        getBSONType: () => BSONType.OBJECT,
      };

      TypeMapper.typeRegistry.register(handler);

      const foundHandler = TypeMapper.typeRegistry.findHandler('some value');
      expect(foundHandler).toBeUndefined();
    });

    it('should respect priority order', () => {
      const lowPriorityHandler = {
        name: 'LowPriority',
        priority: 1,
        detect: () => true,
        getMapping: () => ({ physicalType: ParquetPhysicalType.BYTE_ARRAY }),
        transformValue: (v: unknown) => v,
        getBSONType: () => BSONType.OBJECT,
      };

      const highPriorityHandler = {
        name: 'HighPriority',
        priority: 10,
        detect: () => true,
        getMapping: () => ({ physicalType: ParquetPhysicalType.INT32 }),
        transformValue: (v: unknown) => v,
        getBSONType: () => BSONType.INT32,
      };

      TypeMapper.typeRegistry.register(lowPriorityHandler);
      TypeMapper.typeRegistry.register(highPriorityHandler);

      const foundHandler = TypeMapper.typeRegistry.findHandler({});
      expect(foundHandler?.name).toBe('HighPriority');
    });
  });

  describe('getHandlers', () => {
    it('should return all registered handlers', () => {
      const handler1 = {
        name: 'Handler1',
        detect: () => false,
        getMapping: () => ({ physicalType: ParquetPhysicalType.BYTE_ARRAY }),
        transformValue: (v: unknown) => v,
        getBSONType: () => BSONType.OBJECT,
      };

      const handler2 = {
        name: 'Handler2',
        detect: () => false,
        getMapping: () => ({ physicalType: ParquetPhysicalType.INT32 }),
        transformValue: (v: unknown) => v,
        getBSONType: () => BSONType.INT32,
      };

      TypeMapper.typeRegistry.register(handler1);
      TypeMapper.typeRegistry.register(handler2);

      const handlers = TypeMapper.typeRegistry.getHandlers();
      expect(handlers.length).toBe(2);
    });

    it('should return handlers sorted by priority', () => {
      const handler1 = {
        name: 'Low',
        priority: 1,
        detect: () => false,
        getMapping: () => ({ physicalType: ParquetPhysicalType.BYTE_ARRAY }),
        transformValue: (v: unknown) => v,
        getBSONType: () => BSONType.OBJECT,
      };

      const handler2 = {
        name: 'High',
        priority: 10,
        detect: () => false,
        getMapping: () => ({ physicalType: ParquetPhysicalType.INT32 }),
        transformValue: (v: unknown) => v,
        getBSONType: () => BSONType.INT32,
      };

      const handler3 = {
        name: 'Medium',
        priority: 5,
        detect: () => false,
        getMapping: () => ({ physicalType: ParquetPhysicalType.INT64 }),
        transformValue: (v: unknown) => v,
        getBSONType: () => BSONType.INT64,
      };

      TypeMapper.typeRegistry.register(handler1);
      TypeMapper.typeRegistry.register(handler2);
      TypeMapper.typeRegistry.register(handler3);

      const handlers = TypeMapper.typeRegistry.getHandlers();
      expect(handlers[0].name).toBe('High');
      expect(handlers[1].name).toBe('Medium');
      expect(handlers[2].name).toBe('Low');
    });
  });

  describe('clear', () => {
    it('should remove all registered handlers', () => {
      const handler = {
        name: 'ToClear',
        detect: () => false,
        getMapping: () => ({ physicalType: ParquetPhysicalType.BYTE_ARRAY }),
        transformValue: (v: unknown) => v,
        getBSONType: () => BSONType.OBJECT,
      };

      TypeMapper.typeRegistry.register(handler);
      expect(TypeMapper.typeRegistry.getHandlers().length).toBe(1);

      TypeMapper.typeRegistry.clear();
      expect(TypeMapper.typeRegistry.getHandlers().length).toBe(0);
    });
  });

  describe('integration with inferBSONType', () => {
    it('should use custom handler in inferBSONType', () => {
      const customHandler = {
        name: 'CustomGeoPoint',
        priority: 100, // High priority to be checked first
        detect: (v: unknown) => {
          return typeof v === 'object' && v !== null && 'latitude' in v && 'longitude' in v;
        },
        getMapping: () => ({
          physicalType: ParquetPhysicalType.BYTE_ARRAY,
          logicalType: ParquetLogicalType.JSON,
        }),
        transformValue: (v: unknown) => JSON.stringify(v),
        getBSONType: () => BSONType.STRING, // Custom BSON type representation
      };

      TypeMapper.typeRegistry.register(customHandler);

      const geoPoint = { latitude: 40.7128, longitude: -74.0060 };
      const bsonType = inferBSONType(geoPoint);
      expect(bsonType).toBe(BSONType.STRING);
    });
  });
});

// ============================================================================
// Documentation Generation Tests
// ============================================================================

describe('Column Type Mapper - Documentation Generation', () => {
  // Clear the registry before tests
  beforeEach(() => {
    TypeMapper.typeRegistry.clear();
  });

  describe('generateTypeMappingDocs export', () => {
    it('should export generateTypeMappingDocs function', () => {
      expect(TypeMapper.generateTypeMappingDocs).toBeDefined();
      expect(typeof TypeMapper.generateTypeMappingDocs).toBe('function');
    });
  });

  describe('generateTypeMappingMarkdown export', () => {
    it('should export generateTypeMappingMarkdown function', () => {
      expect(TypeMapper.generateTypeMappingMarkdown).toBeDefined();
      expect(typeof TypeMapper.generateTypeMappingMarkdown).toBe('function');
    });
  });

  describe('generateTypeMappingDocs', () => {
    it('should return array of type mapping docs', () => {
      const docs = TypeMapper.generateTypeMappingDocs();
      expect(Array.isArray(docs)).toBe(true);
      expect(docs.length).toBeGreaterThan(0);
    });

    it('should include all standard BSON types', () => {
      const docs = TypeMapper.generateTypeMappingDocs();
      const bsonTypes = docs.map(d => d.bsonType);

      expect(bsonTypes).toContain(BSONType.DOUBLE);
      expect(bsonTypes).toContain(BSONType.STRING);
      expect(bsonTypes).toContain(BSONType.OBJECT);
      expect(bsonTypes).toContain(BSONType.ARRAY);
      expect(bsonTypes).toContain(BSONType.BINARY);
      expect(bsonTypes).toContain(BSONType.OBJECT_ID);
      expect(bsonTypes).toContain(BSONType.BOOLEAN);
      expect(bsonTypes).toContain(BSONType.DATE);
      expect(bsonTypes).toContain(BSONType.NULL);
      expect(bsonTypes).toContain(BSONType.REGEX);
      expect(bsonTypes).toContain(BSONType.INT32);
      expect(bsonTypes).toContain(BSONType.INT64);
      expect(bsonTypes).toContain(BSONType.DECIMAL128);
    });

    it('should include description for each type', () => {
      const docs = TypeMapper.generateTypeMappingDocs();
      for (const doc of docs) {
        expect(doc.description).toBeDefined();
        expect(doc.description.length).toBeGreaterThan(0);
      }
    });

    it('should include physicalType for each type', () => {
      const docs = TypeMapper.generateTypeMappingDocs();
      for (const doc of docs) {
        expect(doc.physicalType).toBeDefined();
      }
    });

    it('should include custom handlers in documentation', () => {
      const customHandler = {
        name: 'DocumentedType',
        detect: () => false,
        getMapping: () => ({
          physicalType: ParquetPhysicalType.BYTE_ARRAY,
          logicalType: ParquetLogicalType.JSON,
        }),
        transformValue: (v: unknown) => v,
        getBSONType: () => BSONType.OBJECT,
      };

      TypeMapper.typeRegistry.register(customHandler);

      const docs = TypeMapper.generateTypeMappingDocs();
      const customDoc = docs.find(d => d.description.includes('DocumentedType'));
      expect(customDoc).toBeDefined();
    });
  });

  describe('generateTypeMappingMarkdown', () => {
    it('should return markdown string', () => {
      const markdown = TypeMapper.generateTypeMappingMarkdown();
      expect(typeof markdown).toBe('string');
      expect(markdown.length).toBeGreaterThan(0);
    });

    it('should include markdown table headers', () => {
      const markdown = TypeMapper.generateTypeMappingMarkdown();
      expect(markdown).toContain('| BSON Type |');
      expect(markdown).toContain('| Description |');
      expect(markdown).toContain('| Physical Type |');
      expect(markdown).toContain('| Logical Type |');
    });

    it('should include type mappings in table', () => {
      const markdown = TypeMapper.generateTypeMappingMarkdown();
      expect(markdown).toContain(BSONType.STRING);
      expect(markdown).toContain(ParquetPhysicalType.BYTE_ARRAY);
      expect(markdown).toContain(ParquetLogicalType.STRING);
    });

    it('should include header', () => {
      const markdown = TypeMapper.generateTypeMappingMarkdown();
      expect(markdown).toContain('# BSON to Parquet Type Mappings');
    });
  });
});

// ============================================================================
// valueToParquetMapping Tests
// ============================================================================

describe('Column Type Mapper - valueToParquetMapping', () => {
  beforeEach(() => {
    TypeMapper.typeRegistry.clear();
  });

  describe('valueToParquetMapping export', () => {
    it('should export valueToParquetMapping function', () => {
      expect(TypeMapper.valueToParquetMapping).toBeDefined();
      expect(typeof TypeMapper.valueToParquetMapping).toBe('function');
    });
  });

  describe('basic value mapping', () => {
    it('should map string values', () => {
      const mapping = TypeMapper.valueToParquetMapping('hello');
      expect(mapping.physicalType).toBe(ParquetPhysicalType.BYTE_ARRAY);
      expect(mapping.logicalType).toBe(ParquetLogicalType.STRING);
    });

    it('should map integer values', () => {
      const mapping = TypeMapper.valueToParquetMapping(42);
      expect(mapping.physicalType).toBe(ParquetPhysicalType.INT32);
    });

    it('should map double values', () => {
      const mapping = TypeMapper.valueToParquetMapping(3.14);
      expect(mapping.physicalType).toBe(ParquetPhysicalType.DOUBLE);
    });

    it('should map Date values', () => {
      const mapping = TypeMapper.valueToParquetMapping(new Date());
      expect(mapping.physicalType).toBe(ParquetPhysicalType.INT64);
      expect(mapping.logicalType).toBe(ParquetLogicalType.TIMESTAMP_MILLIS);
    });
  });

  describe('custom handler integration', () => {
    it('should use custom handler for matching values', () => {
      const customHandler = {
        name: 'CustomMapping',
        priority: 100,
        detect: (v: unknown) => typeof v === 'object' && v !== null && 'custom' in v,
        getMapping: () => ({
          physicalType: ParquetPhysicalType.FIXED_LEN_BYTE_ARRAY,
          typeLength: 32,
        }),
        transformValue: (v: unknown) => v,
        getBSONType: () => BSONType.BINARY,
      };

      TypeMapper.typeRegistry.register(customHandler);

      const customValue = { custom: true };
      const mapping = TypeMapper.valueToParquetMapping(customValue);
      expect(mapping.physicalType).toBe(ParquetPhysicalType.FIXED_LEN_BYTE_ARRAY);
      expect(mapping.typeLength).toBe(32);
    });
  });
});
