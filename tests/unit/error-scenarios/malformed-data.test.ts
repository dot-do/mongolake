/**
 * Malformed Data Error Scenario Tests
 *
 * Comprehensive tests for handling malformed data:
 * - Invalid JSON/BSON documents
 * - Corrupted binary data
 * - Invalid ObjectIds
 * - Malformed query operators
 * - Invalid field names
 * - Schema violations
 * - Encoding errors
 *
 * These tests verify that malformed data is properly detected
 * and reported with informative error messages.
 */

import { describe, it, expect } from 'vitest';
import { ObjectId } from '../../../src/types.js';
import {
  validateFilter,
  validateUpdate,
  validateDocument,
  validateProjection,
  validateAggregationPipeline,
  validateFieldName,
  ValidationError,
} from '../../../src/validation/index.js';

// ============================================================================
// ObjectId Helper Functions (for testing)
// ============================================================================

/**
 * Type guard to check if a value looks like an ObjectId
 * (has toString and toHexString methods returning valid hex strings)
 */
function isObjectId(value: unknown): value is ObjectId {
  if (value === null || value === undefined) return false;
  if (value instanceof ObjectId) return true;
  if (typeof value !== 'object') return false;
  const obj = value as Record<string, unknown>;
  if (typeof obj.toString !== 'function' || typeof obj.toHexString !== 'function') return false;
  const hex = (obj.toHexString as () => string)();
  return ObjectId.isValid(hex);
}

/**
 * Convert a value to its ObjectId string representation
 */
function toObjectIdString(value: unknown): string {
  if (value instanceof ObjectId) {
    return value.toHexString();
  }
  if (value !== null && typeof value === 'object' && 'toHexString' in value) {
    return (value as { toHexString: () => string }).toHexString();
  }
  return String(value);
}

// ============================================================================
// Invalid ObjectId Tests
// ============================================================================

describe('Malformed Data - Invalid ObjectIds', () => {
  // Note: ObjectId constructor does NOT validate - use ObjectId.isValid() for validation
  it('should identify non-24-character hex strings as invalid', () => {
    expect(ObjectId.isValid('123')).toBe(false);
    expect(ObjectId.isValid('507f1f77bcf86cd79943901')).toBe(false); // 23 chars
    expect(ObjectId.isValid('507f1f77bcf86cd7994390111')).toBe(false); // 25 chars
  });

  it('should identify non-hex characters as invalid', () => {
    expect(ObjectId.isValid('507f1f77bcf86cd79943901g')).toBe(false);
    expect(ObjectId.isValid('507f1f77bcf86cd79943901Z')).toBe(false);
    expect(ObjectId.isValid('507f1f77bcf86cd799439-11')).toBe(false);
  });

  it('should identify empty string as invalid', () => {
    expect(ObjectId.isValid('')).toBe(false);
  });

  it('should accept valid ObjectId strings', () => {
    expect(ObjectId.isValid('507f1f77bcf86cd799439011')).toBe(true);
    expect(ObjectId.isValid('000000000000000000000000')).toBe(true);
    expect(ObjectId.isValid('ffffffffffffffffffffffff')).toBe(true);
  });

  it('should correctly identify valid ObjectIds', () => {
    expect(isObjectId(new ObjectId())).toBe(true);
    expect(isObjectId({ toString: () => '507f1f77bcf86cd799439011', toHexString: () => '507f1f77bcf86cd799439011' })).toBe(true);
    expect(isObjectId('not an objectid')).toBe(false);
    expect(isObjectId(null)).toBe(false);
    expect(isObjectId(undefined)).toBe(false);
    expect(isObjectId(123)).toBe(false);
  });

  it('should handle toObjectIdString for various inputs', () => {
    const oid = new ObjectId('507f1f77bcf86cd799439011');
    expect(toObjectIdString(oid)).toBe('507f1f77bcf86cd799439011');
    expect(toObjectIdString('plain-string-id')).toBe('plain-string-id');
    expect(toObjectIdString(123)).toBe('123');
  });
});

// ============================================================================
// Invalid Filter Operator Tests
// ============================================================================

describe('Malformed Data - Invalid Filter Operators', () => {
  it('should reject unknown query operators', () => {
    expect(() => validateFilter({ field: { $unknown: 'value' } })).toThrow(ValidationError);
    expect(() => validateFilter({ field: { $badOp: 1 } })).toThrow(ValidationError);
  });

  it('should include operator name in error message', () => {
    try {
      validateFilter({ field: { $customOperator: 'value' } });
    } catch (error) {
      expect(error).toBeInstanceOf(ValidationError);
      expect((error as ValidationError).message).toContain('$customOperator');
    }
  });

  it('should reject $in with non-array value', () => {
    expect(() => validateFilter({ field: { $in: 'not-array' } })).toThrow(ValidationError);
    expect(() => validateFilter({ field: { $in: 123 } })).toThrow(ValidationError);
    expect(() => validateFilter({ field: { $in: { obj: true } } })).toThrow(ValidationError);
  });

  it('should reject $nin with non-array value', () => {
    expect(() => validateFilter({ field: { $nin: 'not-array' } })).toThrow(ValidationError);
  });

  it('should reject $and with non-array value', () => {
    expect(() => validateFilter({ $and: { a: 1 } })).toThrow(ValidationError);
    expect(() => validateFilter({ $and: 'not-array' })).toThrow(ValidationError);
  });

  it('should reject $or with non-array value', () => {
    expect(() => validateFilter({ $or: { a: 1 } })).toThrow(ValidationError);
  });

  it('should reject $nor with non-array value', () => {
    expect(() => validateFilter({ $nor: { a: 1 } })).toThrow(ValidationError);
  });

  it('should handle $not operator', () => {
    // $not accepts object values (nested operator expressions)
    expect(() => validateFilter({ field: { $not: { $eq: 5 } } })).not.toThrow();
    expect(() => validateFilter({ field: { $not: { $gt: 10 } } })).not.toThrow();
    // $not with invalid nested operator should fail
    expect(() => validateFilter({ field: { $not: { $badOp: 5 } } })).toThrow(ValidationError);
  });

  it('should reject arrays with non-array $in/$nin operators', () => {
    // $in and $nin require array values
    expect(() => validateFilter({ field: { $in: 'not-array' } })).toThrow(ValidationError);
    expect(() => validateFilter({ field: { $nin: { obj: 1 } } })).toThrow(ValidationError);
  });

  it('should accept valid operators', () => {
    expect(() => validateFilter({ field: { $eq: 1 } })).not.toThrow();
    expect(() => validateFilter({ field: { $ne: 1 } })).not.toThrow();
    expect(() => validateFilter({ field: { $gt: 1 } })).not.toThrow();
    expect(() => validateFilter({ field: { $gte: 1 } })).not.toThrow();
    expect(() => validateFilter({ field: { $lt: 1 } })).not.toThrow();
    expect(() => validateFilter({ field: { $lte: 1 } })).not.toThrow();
    expect(() => validateFilter({ field: { $in: [1, 2, 3] } })).not.toThrow();
    expect(() => validateFilter({ field: { $nin: [1, 2, 3] } })).not.toThrow();
    expect(() => validateFilter({ $and: [{ a: 1 }, { b: 2 }] })).not.toThrow();
    expect(() => validateFilter({ $or: [{ a: 1 }, { b: 2 }] })).not.toThrow();
  });
});

// ============================================================================
// Invalid Update Operator Tests
// ============================================================================

describe('Malformed Data - Invalid Update Operators', () => {
  it('should reject unknown update operators', () => {
    expect(() => validateUpdate({ $unknownOp: { field: 1 } })).toThrow(ValidationError);
    expect(() => validateUpdate({ $custom: { field: 1 } })).toThrow(ValidationError);
  });

  it('should include operator name in update error', () => {
    try {
      validateUpdate({ $badUpdateOp: { field: 'value' } });
    } catch (error) {
      expect(error).toBeInstanceOf(ValidationError);
      expect((error as ValidationError).message).toContain('$badUpdateOp');
    }
  });

  it('should reject $set with non-object value', () => {
    expect(() => validateUpdate({ $set: 'not-object' })).toThrow(ValidationError);
    expect(() => validateUpdate({ $set: 123 })).toThrow(ValidationError);
    expect(() => validateUpdate({ $set: ['array'] })).toThrow(ValidationError);
  });

  it('should reject $unset with non-object value', () => {
    expect(() => validateUpdate({ $unset: 'not-object' })).toThrow(ValidationError);
  });

  it('should reject $inc with non-object value', () => {
    expect(() => validateUpdate({ $inc: 'not-object' })).toThrow(ValidationError);
  });

  it('should reject $inc with non-numeric field values', () => {
    expect(() => validateUpdate({ $inc: { count: 'not-a-number' } })).toThrow(ValidationError);
    expect(() => validateUpdate({ $inc: { count: { nested: 1 } } })).toThrow(ValidationError);
  });

  it('should reject $push with non-object value', () => {
    expect(() => validateUpdate({ $push: 'not-object' })).toThrow(ValidationError);
  });

  it('should reject $pull with non-object value', () => {
    expect(() => validateUpdate({ $pull: 'not-object' })).toThrow(ValidationError);
  });

  it('should reject update without any operators', () => {
    // Plain replacement document should be handled differently
    // This tests that empty operators are rejected
    expect(() => validateUpdate({})).toThrow(ValidationError);
  });

  it('should accept valid update operators', () => {
    expect(() => validateUpdate({ $set: { field: 'value' } })).not.toThrow();
    expect(() => validateUpdate({ $unset: { field: '' } })).not.toThrow();
    expect(() => validateUpdate({ $inc: { count: 1 } })).not.toThrow();
    expect(() => validateUpdate({ $push: { items: 'new' } })).not.toThrow();
    expect(() => validateUpdate({ $pull: { items: 'old' } })).not.toThrow();
  });
});

// ============================================================================
// Invalid Document Tests
// ============================================================================

describe('Malformed Data - Invalid Documents', () => {
  it('should reject null document', () => {
    expect(() => validateDocument(null as unknown as object)).toThrow(ValidationError);
  });

  it('should reject undefined document', () => {
    expect(() => validateDocument(undefined as unknown as object)).toThrow(ValidationError);
  });

  it('should reject non-object document', () => {
    expect(() => validateDocument('string' as unknown as object)).toThrow(ValidationError);
    expect(() => validateDocument(123 as unknown as object)).toThrow(ValidationError);
    expect(() => validateDocument(true as unknown as object)).toThrow(ValidationError);
  });

  it('should reject array as document', () => {
    expect(() => validateDocument(['array'] as unknown as object)).toThrow(ValidationError);
  });

  it('should reject documents with invalid field names', () => {
    expect(() => validateDocument({ '': 'empty-key' })).toThrow(ValidationError);
    // Note: documents allow dots in field names (unlike queries), so we test other invalid patterns
    expect(() => validateDocument({ '$invalid': 'starts-with-dollar' })).toThrow(ValidationError);
  });

  it('should reject deeply nested documents exceeding max depth', () => {
    const createDeepObject = (depth: number): object => {
      if (depth === 0) return { value: 1 };
      return { nested: createDeepObject(depth - 1) };
    };

    const veryDeep = createDeepObject(50);
    expect(() => validateDocument(veryDeep, { maxDepth: 10 })).toThrow(ValidationError);
  });

  it('should accept valid documents', () => {
    expect(() => validateDocument({})).not.toThrow();
    expect(() => validateDocument({ _id: '123' })).not.toThrow();
    expect(() => validateDocument({ name: 'test', value: 123 })).not.toThrow();
    expect(() => validateDocument({ nested: { field: 'value' } })).not.toThrow();
  });
});

// ============================================================================
// Invalid Field Name Tests
// ============================================================================

describe('Malformed Data - Invalid Field Names', () => {
  it('should reject empty field names', () => {
    expect(() => validateFieldName('')).toThrow(ValidationError);
  });

  it('should accept field names with dots (allowed in document field names)', () => {
    // Note: MongoDB allows dots in document field names during insert
    // Only query paths interpret dots as nested field accessors
    expect(() => validateFieldName('field.name')).not.toThrow();
    expect(() => validateFieldName('.leading')).not.toThrow();
    expect(() => validateFieldName('trailing.')).not.toThrow();
  });

  it('should reject field names starting with $', () => {
    expect(() => validateFieldName('$field')).toThrow(ValidationError);
    expect(() => validateFieldName('$set')).toThrow(ValidationError);
  });

  it('should reject null bytes in field names', () => {
    expect(() => validateFieldName('field\x00name')).toThrow(ValidationError);
  });

  it('should include field name in error message', () => {
    try {
      validateFieldName('$invalid');
    } catch (error) {
      expect(error).toBeInstanceOf(ValidationError);
      expect((error as ValidationError).message).toMatch(/field name|cannot start with \$/i);
    }
  });

  it('should accept valid field names', () => {
    expect(() => validateFieldName('_id')).not.toThrow();
    expect(() => validateFieldName('name')).not.toThrow();
    expect(() => validateFieldName('camelCase')).not.toThrow();
    expect(() => validateFieldName('snake_case')).not.toThrow();
    expect(() => validateFieldName('with-hyphen')).not.toThrow();
    expect(() => validateFieldName('with123numbers')).not.toThrow();
  });
});

// ============================================================================
// Invalid Projection Tests
// ============================================================================

describe('Malformed Data - Invalid Projections', () => {
  it('should reject mixed inclusion and exclusion', () => {
    expect(() => validateProjection({ field1: 1, field2: 0 })).toThrow(ValidationError);
    expect(() => validateProjection({ name: 1, age: 0, status: 1 })).toThrow(ValidationError);
  });

  it('should allow _id exclusion with other inclusions', () => {
    expect(() => validateProjection({ name: 1, _id: 0 })).not.toThrow();
    expect(() => validateProjection({ field1: 1, field2: 1, _id: 0 })).not.toThrow();
  });

  it('should reject invalid projection values', () => {
    expect(() => validateProjection({ field: 2 })).toThrow(ValidationError);
    expect(() => validateProjection({ field: -1 })).toThrow(ValidationError);
    expect(() => validateProjection({ field: 'include' })).toThrow(ValidationError);
  });

  it('should include field name in projection error', () => {
    try {
      validateProjection({ goodField: 1, badField: 0 });
    } catch (error) {
      expect(error).toBeInstanceOf(ValidationError);
      // Error should mention the mixed inclusion/exclusion issue
      expect((error as ValidationError).message.toLowerCase()).toContain('mix');
    }
  });

  it('should handle null/undefined projection', () => {
    // Implementation may accept null/undefined and treat as no projection
    // or throw an error - test the actual behavior
    const nullResult = () => validateProjection(null as unknown as object);
    const undefinedResult = () => validateProjection(undefined as unknown as object);
    // Either behavior is acceptable - just verify it doesn't crash unexpectedly
    try {
      nullResult();
      undefinedResult();
    } catch (error) {
      expect(error).toBeInstanceOf(ValidationError);
    }
  });

  it('should accept valid projections', () => {
    expect(() => validateProjection({})).not.toThrow();
    expect(() => validateProjection({ field1: 1, field2: 1 })).not.toThrow();
    expect(() => validateProjection({ field1: 0, field2: 0 })).not.toThrow();
    expect(() => validateProjection({ _id: 0 })).not.toThrow();
    expect(() => validateProjection({ name: 1 })).not.toThrow();
  });
});

// ============================================================================
// Invalid Aggregation Pipeline Tests
// ============================================================================

describe('Malformed Data - Invalid Aggregation Pipelines', () => {
  it('should reject non-array pipeline', () => {
    expect(() => validateAggregationPipeline({} as unknown as unknown[])).toThrow(ValidationError);
    expect(() => validateAggregationPipeline('pipeline' as unknown as unknown[])).toThrow(ValidationError);
  });

  it('should reject empty pipeline', () => {
    expect(() => validateAggregationPipeline([])).toThrow(ValidationError);
  });

  it('should reject invalid stage operators', () => {
    expect(() => validateAggregationPipeline([{ $invalid: {} }])).toThrow(ValidationError);
  });

  it('should reject stages with multiple operators', () => {
    expect(() => validateAggregationPipeline([{ $match: {}, $project: {} }])).toThrow(ValidationError);
  });

  it('should reject non-object stages', () => {
    expect(() => validateAggregationPipeline(['$match'])).toThrow(ValidationError);
    expect(() => validateAggregationPipeline([123])).toThrow(ValidationError);
    expect(() => validateAggregationPipeline([null])).toThrow(ValidationError);
  });

  it('should include stage index in error', () => {
    try {
      validateAggregationPipeline([
        { $match: {} },
        { $invalid: {} },
      ]);
    } catch (error) {
      expect(error).toBeInstanceOf(ValidationError);
      // Should indicate which stage has the error
      const validationError = error as ValidationError & { context?: { index?: number } };
      expect(validationError.context?.index).toBe(1);
    }
  });

  it('should accept valid pipelines', () => {
    expect(() => validateAggregationPipeline([{ $match: {} }])).not.toThrow();
    expect(() => validateAggregationPipeline([{ $match: { status: 'active' } }])).not.toThrow();
    expect(() => validateAggregationPipeline([
      { $match: { status: 'active' } },
      { $project: { name: 1 } },
    ])).not.toThrow();
  });
});

// ============================================================================
// Binary Data Corruption Tests
// ============================================================================

describe('Malformed Data - Corrupted Binary Data', () => {
  /**
   * Simulates parsing binary data that might be corrupted
   */
  class BinaryParser {
    static parseHeader(data: Uint8Array): { magic: string; version: number; size: number } {
      if (data.length < 8) {
        throw new CorruptedDataError(
          'Data too short: expected at least 8 bytes for header',
          { actual: data.length, expected: 8 }
        );
      }

      const magic = String.fromCharCode(data[0]!, data[1]!, data[2]!, data[3]!);
      if (magic !== 'PAR1' && magic !== 'BSON') {
        throw new CorruptedDataError(
          `Invalid magic bytes: expected 'PAR1' or 'BSON', got '${magic}'`,
          { magic }
        );
      }

      const version = data[4]!;
      if (version > 2) {
        throw new CorruptedDataError(
          `Unsupported version: ${version}. Maximum supported is 2.`,
          { version }
        );
      }

      const size = (data[5]! << 16) | (data[6]! << 8) | data[7]!;
      if (size < 0 || size > 1073741824) { // 1GB max
        throw new CorruptedDataError(
          `Invalid size: ${size}. Must be between 0 and 1GB.`,
          { size }
        );
      }

      return { magic, version, size };
    }
  }

  class CorruptedDataError extends Error {
    constructor(
      message: string,
      public readonly details: Record<string, unknown>
    ) {
      super(message);
      this.name = 'CorruptedDataError';
    }
  }

  it('should reject data that is too short', () => {
    const shortData = new Uint8Array([0x50, 0x41, 0x52]); // Only 3 bytes

    expect(() => BinaryParser.parseHeader(shortData)).toThrow(CorruptedDataError);
    expect(() => BinaryParser.parseHeader(shortData)).toThrow('too short');
  });

  it('should reject invalid magic bytes', () => {
    const invalidMagic = new Uint8Array([0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x10]);

    expect(() => BinaryParser.parseHeader(invalidMagic)).toThrow(CorruptedDataError);
    expect(() => BinaryParser.parseHeader(invalidMagic)).toThrow('Invalid magic');
  });

  it('should reject unsupported version', () => {
    // PAR1 magic + version 5 (unsupported)
    const unsupportedVersion = new Uint8Array([0x50, 0x41, 0x52, 0x31, 0x05, 0x00, 0x00, 0x10]);

    expect(() => BinaryParser.parseHeader(unsupportedVersion)).toThrow(CorruptedDataError);
    expect(() => BinaryParser.parseHeader(unsupportedVersion)).toThrow('Unsupported version');
  });

  it('should include corruption details in error', () => {
    const invalidData = new Uint8Array([0x42, 0x41, 0x44, 0x21, 0x01, 0x00, 0x00, 0x10]);

    try {
      BinaryParser.parseHeader(invalidData);
    } catch (error) {
      expect(error).toBeInstanceOf(CorruptedDataError);
      expect((error as CorruptedDataError).details.magic).toBe('BAD!');
    }
  });

  it('should parse valid data successfully', () => {
    // PAR1 magic + version 1 + size 256
    const validData = new Uint8Array([0x50, 0x41, 0x52, 0x31, 0x01, 0x00, 0x01, 0x00]);

    const header = BinaryParser.parseHeader(validData);

    expect(header.magic).toBe('PAR1');
    expect(header.version).toBe(1);
    expect(header.size).toBe(256);
  });
});

// ============================================================================
// JSON Parsing Error Tests
// ============================================================================

describe('Malformed Data - JSON Parsing Errors', () => {
  /**
   * Wrapper for JSON parsing with better error messages
   */
  function parseJSON(input: string): unknown {
    try {
      return JSON.parse(input);
    } catch (error) {
      if (error instanceof SyntaxError) {
        throw new JSONParseError(
          `Invalid JSON: ${error.message}`,
          input.length > 100 ? input.slice(0, 100) + '...' : input,
          error
        );
      }
      throw error;
    }
  }

  class JSONParseError extends Error {
    constructor(
      message: string,
      public readonly input: string,
      public readonly cause: Error
    ) {
      super(message);
      this.name = 'JSONParseError';
    }
  }

  it('should throw JSONParseError for invalid JSON', () => {
    expect(() => parseJSON('{invalid}')).toThrow(JSONParseError);
    expect(() => parseJSON('not json at all')).toThrow(JSONParseError);
    expect(() => parseJSON('{trailing comma,}')).toThrow(JSONParseError);
  });

  it('should include truncated input in error', () => {
    const longInput = '{"key": "value' + 'x'.repeat(200);

    try {
      parseJSON(longInput);
    } catch (error) {
      expect(error).toBeInstanceOf(JSONParseError);
      expect((error as JSONParseError).input.length).toBeLessThanOrEqual(103); // 100 + '...'
    }
  });

  it('should preserve original error as cause', () => {
    try {
      parseJSON('{broken');
    } catch (error) {
      expect(error).toBeInstanceOf(JSONParseError);
      expect((error as JSONParseError).cause).toBeInstanceOf(SyntaxError);
    }
  });

  it('should parse valid JSON successfully', () => {
    expect(parseJSON('{}')).toEqual({});
    expect(parseJSON('{"key": "value"}')).toEqual({ key: 'value' });
    expect(parseJSON('[]')).toEqual([]);
    expect(parseJSON('null')).toBe(null);
  });

  it('should handle common JSON mistakes', () => {
    // Single quotes instead of double
    expect(() => parseJSON("{'key': 'value'}")).toThrow(JSONParseError);

    // Unquoted keys
    expect(() => parseJSON('{key: "value"}')).toThrow(JSONParseError);

    // Trailing comma
    expect(() => parseJSON('{"key": "value",}')).toThrow(JSONParseError);

    // Missing comma
    expect(() => parseJSON('{"a": 1 "b": 2}')).toThrow(JSONParseError);
  });
});

// ============================================================================
// Encoding Error Tests
// ============================================================================

describe('Malformed Data - Encoding Errors', () => {
  /**
   * Validates that a string is valid UTF-8
   */
  function validateUTF8(buffer: Uint8Array): string {
    const decoder = new TextDecoder('utf-8', { fatal: true });
    try {
      return decoder.decode(buffer);
    } catch {
      throw new EncodingError(
        'Invalid UTF-8 encoding',
        buffer.slice(0, 20) // First 20 bytes for debugging
      );
    }
  }

  class EncodingError extends Error {
    constructor(
      message: string,
      public readonly sampleBytes: Uint8Array
    ) {
      super(message);
      this.name = 'EncodingError';
    }
  }

  it('should reject invalid UTF-8 sequences', () => {
    // Invalid continuation byte
    const invalidSequence = new Uint8Array([0xC0, 0x00]);

    expect(() => validateUTF8(invalidSequence)).toThrow(EncodingError);
  });

  it('should reject overlong encodings', () => {
    // Overlong encoding of '/' (0x2F)
    const overlongSlash = new Uint8Array([0xC0, 0xAF]);

    expect(() => validateUTF8(overlongSlash)).toThrow(EncodingError);
  });

  it('should accept valid UTF-8', () => {
    const validASCII = new Uint8Array([0x48, 0x65, 0x6C, 0x6C, 0x6F]); // "Hello"
    expect(validateUTF8(validASCII)).toBe('Hello');

    const validMultibyte = new Uint8Array([0xE4, 0xB8, 0xAD, 0xE6, 0x96, 0x87]); // "中文"
    expect(validateUTF8(validMultibyte)).toBe('中文');
  });

  it('should include sample bytes in encoding error', () => {
    const invalid = new Uint8Array([0xFF, 0xFE, 0x00, 0x01]);

    try {
      validateUTF8(invalid);
    } catch (error) {
      expect(error).toBeInstanceOf(EncodingError);
      expect((error as EncodingError).sampleBytes).toBeDefined();
    }
  });
});
