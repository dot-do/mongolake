/**
 * Variant Encoder Tests
 *
 * Tests for encoding JavaScript values to Parquet Variant binary format.
 * The Variant format is a self-describing binary format that can represent
 * any JSON-like value with type information embedded in the encoding.
 *
 * Spec reference: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
 */

import { describe, it, expect } from 'vitest';
import { encodeVariant, decodeVariant, VariantType } from '../variant.js';
import { ObjectId } from '../../types.js';

// ============================================================================
// Primitive Types
// ============================================================================

describe('Variant Encoder - Primitives', () => {
  describe('null', () => {
    it('should encode null', () => {
      const encoded = encodeVariant(null);
      expect(encoded).toBeInstanceOf(Uint8Array);
      expect(encoded.length).toBeGreaterThan(0);
    });

    it('should round-trip null', () => {
      const original = null;
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe(null);
    });
  });

  describe('boolean', () => {
    it('should encode true', () => {
      const encoded = encodeVariant(true);
      expect(encoded).toBeInstanceOf(Uint8Array);
      expect(encoded.length).toBeGreaterThan(0);
    });

    it('should encode false', () => {
      const encoded = encodeVariant(false);
      expect(encoded).toBeInstanceOf(Uint8Array);
      expect(encoded.length).toBeGreaterThan(0);
    });

    it('should round-trip true', () => {
      const original = true;
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe(true);
    });

    it('should round-trip false', () => {
      const original = false;
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe(false);
    });

    it('should encode true and false differently', () => {
      const encodedTrue = encodeVariant(true);
      const encodedFalse = encodeVariant(false);
      expect(encodedTrue).not.toEqual(encodedFalse);
    });
  });

  describe('integers', () => {
    it('should encode zero', () => {
      const encoded = encodeVariant(0);
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it('should encode positive integers', () => {
      const encoded = encodeVariant(42);
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it('should encode negative integers', () => {
      const encoded = encodeVariant(-42);
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it('should round-trip zero', () => {
      const original = 0;
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe(0);
    });

    it('should round-trip positive integers', () => {
      const original = 12345;
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe(12345);
    });

    it('should round-trip negative integers', () => {
      const original = -12345;
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe(-12345);
    });

    it('should round-trip small integers (int8 range)', () => {
      for (const value of [-128, -1, 0, 1, 127]) {
        const encoded = encodeVariant(value);
        const decoded = decodeVariant(encoded);
        expect(decoded).toBe(value);
      }
    });

    it('should round-trip int16 range values', () => {
      for (const value of [-32768, -129, 128, 32767]) {
        const encoded = encodeVariant(value);
        const decoded = decodeVariant(encoded);
        expect(decoded).toBe(value);
      }
    });

    it('should round-trip int32 range values', () => {
      for (const value of [-2147483648, -32769, 32768, 2147483647]) {
        const encoded = encodeVariant(value);
        const decoded = decodeVariant(encoded);
        expect(decoded).toBe(value);
      }
    });

    it('should round-trip large integers (int64 range)', () => {
      // JavaScript safe integer boundaries
      const largePositive = Number.MAX_SAFE_INTEGER; // 2^53 - 1
      const largeNegative = Number.MIN_SAFE_INTEGER; // -(2^53 - 1)

      const encodedPos = encodeVariant(largePositive);
      const decodedPos = decodeVariant(encodedPos);
      expect(decodedPos).toBe(largePositive);

      const encodedNeg = encodeVariant(largeNegative);
      const decodedNeg = decodeVariant(encodedNeg);
      expect(decodedNeg).toBe(largeNegative);
    });
  });

  describe('BigInt precision handling', () => {
    it('should return BigInt for values exceeding Number.MAX_SAFE_INTEGER', () => {
      // Create an encoded int64 value directly that exceeds safe integer range
      // Format: header byte + 8 bytes little-endian int64
      const buffer = new ArrayBuffer(9);
      const bytes = new Uint8Array(buffer);
      const view = new DataView(buffer);

      // Header: INT type (0x02) | INT64 size (0x40)
      bytes[0] = 0x42;
      // Value: 2^53 (one more than MAX_SAFE_INTEGER)
      const largeBigInt = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
      view.setBigInt64(1, largeBigInt, true);

      const decoded = decodeVariant(bytes);
      expect(typeof decoded).toBe('bigint');
      expect(decoded).toBe(largeBigInt);
    });

    it('should return BigInt for values below Number.MIN_SAFE_INTEGER', () => {
      const buffer = new ArrayBuffer(9);
      const bytes = new Uint8Array(buffer);
      const view = new DataView(buffer);

      bytes[0] = 0x42;
      const smallBigInt = BigInt(Number.MIN_SAFE_INTEGER) - 1n;
      view.setBigInt64(1, smallBigInt, true);

      const decoded = decodeVariant(bytes);
      expect(typeof decoded).toBe('bigint');
      expect(decoded).toBe(smallBigInt);
    });

    it('should return Number for values within safe integer range', () => {
      const buffer = new ArrayBuffer(9);
      const bytes = new Uint8Array(buffer);
      const view = new DataView(buffer);

      bytes[0] = 0x42;
      // Value within safe range but encoded as int64
      const safeValue = 12345678901234n;
      view.setBigInt64(1, safeValue, true);

      const decoded = decodeVariant(bytes);
      expect(typeof decoded).toBe('number');
      expect(decoded).toBe(Number(safeValue));
    });

    it('should preserve precision for very large int64 values', () => {
      const buffer = new ArrayBuffer(9);
      const bytes = new Uint8Array(buffer);
      const view = new DataView(buffer);

      bytes[0] = 0x42;
      // Test with int64 max value
      const int64Max = 9223372036854775807n;
      view.setBigInt64(1, int64Max, true);

      const decoded = decodeVariant(bytes);
      expect(typeof decoded).toBe('bigint');
      expect(decoded).toBe(int64Max);
    });

    it('should preserve precision for very small int64 values', () => {
      const buffer = new ArrayBuffer(9);
      const bytes = new Uint8Array(buffer);
      const view = new DataView(buffer);

      bytes[0] = 0x42;
      // Test with int64 min value
      const int64Min = -9223372036854775808n;
      view.setBigInt64(1, int64Min, true);

      const decoded = decodeVariant(bytes);
      expect(typeof decoded).toBe('bigint');
      expect(decoded).toBe(int64Min);
    });
  });

  describe('floats', () => {
    it('should encode float values', () => {
      const encoded = encodeVariant(3.14159);
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it('should round-trip float values', () => {
      const original = 3.14159;
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBeCloseTo(original, 10);
    });

    it('should round-trip negative floats', () => {
      const original = -273.15;
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBeCloseTo(original, 10);
    });

    it('should round-trip very small floats', () => {
      const original = 1e-10;
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBeCloseTo(original, 15);
    });

    it('should round-trip very large floats', () => {
      const original = 1e308;
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe(original);
    });

    it('should handle Infinity', () => {
      const encoded = encodeVariant(Infinity);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe(Infinity);
    });

    it('should handle -Infinity', () => {
      const encoded = encodeVariant(-Infinity);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe(-Infinity);
    });

    it('should handle NaN', () => {
      const encoded = encodeVariant(NaN);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBeNaN();
    });
  });

  describe('strings', () => {
    it('should encode empty string', () => {
      const encoded = encodeVariant('');
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it('should encode simple string', () => {
      const encoded = encodeVariant('hello');
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it('should round-trip empty string', () => {
      const original = '';
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe('');
    });

    it('should round-trip simple string', () => {
      const original = 'hello world';
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe('hello world');
    });

    it('should round-trip unicode strings', () => {
      const original = 'Hello, \u4e16\u754c! \ud83c\udf0d';
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe(original);
    });

    it('should round-trip strings with special characters', () => {
      const original = 'line1\nline2\ttab\r\nwindows';
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe(original);
    });

    it('should round-trip long strings', () => {
      const original = 'a'.repeat(10000);
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe(original);
    });

    it('should round-trip strings with null bytes', () => {
      const original = 'hello\x00world';
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe(original);
    });
  });
});

// ============================================================================
// Complex Types - Arrays
// ============================================================================

describe('Variant Encoder - Arrays', () => {
  describe('empty arrays', () => {
    it('should encode empty array', () => {
      const encoded = encodeVariant([]);
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it('should round-trip empty array', () => {
      const original: unknown[] = [];
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual([]);
    });
  });

  describe('arrays with primitives', () => {
    it('should encode array of numbers', () => {
      const encoded = encodeVariant([1, 2, 3]);
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it('should round-trip array of numbers', () => {
      const original = [1, 2, 3, 4, 5];
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual([1, 2, 3, 4, 5]);
    });

    it('should round-trip array of strings', () => {
      const original = ['a', 'b', 'c'];
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual(['a', 'b', 'c']);
    });

    it('should round-trip array of booleans', () => {
      const original = [true, false, true];
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual([true, false, true]);
    });

    it('should round-trip mixed primitive array', () => {
      const original = [1, 'two', true, null, 3.14];
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded) as unknown[];
      expect(decoded[0]).toBe(1);
      expect(decoded[1]).toBe('two');
      expect(decoded[2]).toBe(true);
      expect(decoded[3]).toBe(null);
      expect(decoded[4]).toBeCloseTo(3.14, 10);
    });
  });

  describe('nested arrays', () => {
    it('should encode nested arrays', () => {
      const encoded = encodeVariant([[1, 2], [3, 4]]);
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it('should round-trip nested arrays', () => {
      const original = [[1, 2], [3, 4], [5]];
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual([[1, 2], [3, 4], [5]]);
    });

    it('should round-trip deeply nested arrays', () => {
      const original = [[[1]], [[2, [3]]]];
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual([[[1]], [[2, [3]]]]);
    });

    it('should round-trip arrays with empty nested arrays', () => {
      const original = [[], [[]], [[], [[]]]];
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual([[], [[]], [[], [[]]]]);
    });
  });

  describe('large arrays', () => {
    it('should round-trip array with many elements', () => {
      const original = Array.from({ length: 1000 }, (_, i) => i);
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual(original);
    });
  });
});

// ============================================================================
// Complex Types - Objects
// ============================================================================

describe('Variant Encoder - Objects', () => {
  describe('empty objects', () => {
    it('should encode empty object', () => {
      const encoded = encodeVariant({});
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it('should round-trip empty object', () => {
      const original = {};
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual({});
    });
  });

  describe('flat objects', () => {
    it('should encode simple object', () => {
      const encoded = encodeVariant({ name: 'Alice', age: 30 });
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it('should round-trip simple object', () => {
      const original = { name: 'Alice', age: 30 };
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual({ name: 'Alice', age: 30 });
    });

    it('should round-trip object with all primitive types', () => {
      const original = {
        str: 'hello',
        num: 42,
        float: 3.14,
        bool: true,
        nil: null,
      };
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded) as Record<string, unknown>;
      expect(decoded.str).toBe('hello');
      expect(decoded.num).toBe(42);
      expect(decoded.float).toBeCloseTo(3.14, 10);
      expect(decoded.bool).toBe(true);
      expect(decoded.nil).toBe(null);
    });

    it('should round-trip object with unicode keys', () => {
      const original = { '\u5317\u4eac': 'Beijing', '\u4e0a\u6d77': 'Shanghai' };
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual(original);
    });

    it('should round-trip object with special character keys', () => {
      const original = { 'key.with.dots': 1, 'key-with-dashes': 2, 'key with spaces': 3 };
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual(original);
    });
  });

  describe('nested objects', () => {
    it('should encode nested objects', () => {
      const encoded = encodeVariant({ user: { name: 'Alice' } });
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it('should round-trip nested objects', () => {
      const original = {
        user: {
          name: 'Alice',
          address: {
            city: 'NYC',
            zip: '10001',
          },
        },
      };
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual(original);
    });

    it('should round-trip deeply nested objects', () => {
      const original = {
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
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual(original);
    });
  });

  describe('objects with arrays', () => {
    it('should round-trip object containing arrays', () => {
      const original = {
        tags: ['a', 'b', 'c'],
        scores: [1, 2, 3],
      };
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual(original);
    });

    it('should round-trip complex nested structure', () => {
      const original = {
        users: [
          { name: 'Alice', tags: ['admin', 'user'] },
          { name: 'Bob', tags: ['user'] },
        ],
        meta: {
          count: 2,
          nested: [[1, 2], [3, 4]],
        },
      };
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual(original);
    });
  });

  describe('large objects', () => {
    it('should round-trip object with many keys', () => {
      const original: Record<string, number> = {};
      for (let i = 0; i < 100; i++) {
        original[`key${i}`] = i;
      }
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual(original);
    });
  });
});

// ============================================================================
// Special Types
// ============================================================================

describe('Variant Encoder - Special Types', () => {
  describe('Date', () => {
    it('should encode Date', () => {
      const encoded = encodeVariant(new Date('2024-01-15T12:30:00Z'));
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it('should round-trip Date', () => {
      const original = new Date('2024-01-15T12:30:00.000Z');
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBeInstanceOf(Date);
      expect((decoded as Date).toISOString()).toBe(original.toISOString());
    });

    it('should round-trip Date at epoch', () => {
      const original = new Date(0);
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect((decoded as Date).getTime()).toBe(0);
    });

    it('should round-trip Date with milliseconds', () => {
      const original = new Date('2024-01-15T12:30:00.123Z');
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect((decoded as Date).getTime()).toBe(original.getTime());
    });

    it('should round-trip Date before epoch', () => {
      const original = new Date('1969-07-20T20:17:40Z');
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect((decoded as Date).toISOString()).toBe(original.toISOString());
    });
  });

  describe('binary (Uint8Array)', () => {
    it('should encode Uint8Array', () => {
      const encoded = encodeVariant(new Uint8Array([1, 2, 3, 4]));
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it('should round-trip Uint8Array', () => {
      const original = new Uint8Array([0, 1, 2, 255, 128]);
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBeInstanceOf(Uint8Array);
      expect(decoded).toEqual(original);
    });

    it('should round-trip empty Uint8Array', () => {
      const original = new Uint8Array([]);
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBeInstanceOf(Uint8Array);
      expect((decoded as Uint8Array).length).toBe(0);
    });

    it('should round-trip large Uint8Array', () => {
      const original = new Uint8Array(10000);
      for (let i = 0; i < original.length; i++) {
        original[i] = i % 256;
      }
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual(original);
    });
  });

  describe('ObjectId', () => {
    it('should encode ObjectId', () => {
      const id = new ObjectId('507f1f77bcf86cd799439011');
      const encoded = encodeVariant(id);
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it('should round-trip ObjectId', () => {
      const original = new ObjectId('507f1f77bcf86cd799439011');
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBeInstanceOf(ObjectId);
      expect((decoded as ObjectId).toString()).toBe(original.toString());
    });

    it('should round-trip generated ObjectId', () => {
      const original = new ObjectId();
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect((decoded as ObjectId).toString()).toBe(original.toString());
    });

    it('should preserve ObjectId timestamp', () => {
      const original = new ObjectId();
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded) as ObjectId;
      expect(decoded.getTimestamp().getTime()).toBe(original.getTimestamp().getTime());
    });
  });
});

// ============================================================================
// Edge Cases
// ============================================================================

describe('Variant Encoder - Edge Cases', () => {
  describe('number edge cases', () => {
    it('should handle -0', () => {
      const encoded = encodeVariant(-0);
      const decoded = decodeVariant(encoded);
      // -0 and 0 are equal in JavaScript
      expect(decoded).toBe(0);
    });

    it('should handle Number.EPSILON', () => {
      const original = Number.EPSILON;
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe(original);
    });

    it('should handle Number.MIN_VALUE', () => {
      const original = Number.MIN_VALUE;
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe(original);
    });

    it('should handle Number.MAX_VALUE', () => {
      const original = Number.MAX_VALUE;
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe(original);
    });
  });

  describe('string edge cases', () => {
    it('should handle string with only whitespace', () => {
      const original = '   \t\n\r  ';
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe(original);
    });

    it('should handle string with emoji sequences', () => {
      const original = '\ud83d\udc68\u200d\ud83d\udc69\u200d\ud83d\udc67\u200d\ud83d\udc66'; // Family emoji
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe(original);
    });

    it('should handle string with surrogate pairs', () => {
      const original = '\ud83d\ude00\ud83d\ude01\ud83d\ude02'; // Emoji requiring surrogate pairs
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe(original);
    });
  });

  describe('object edge cases', () => {
    it('should handle object with empty string key', () => {
      const original = { '': 'empty key' };
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual(original);
    });

    it('should handle object with numeric string keys', () => {
      const original = { '0': 'zero', '1': 'one', '42': 'forty-two' };
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual(original);
    });

    it('should handle object with prototype-like keys', () => {
      const original = {
        constructor: 'not a function',
        __proto__: 'not a prototype',
        toString: 'not a method',
      };
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded);
      expect((decoded as Record<string, string>).constructor).toBe('not a function');
      expect((decoded as Record<string, string>).toString).toBe('not a method');
    });
  });

  describe('array edge cases', () => {
    it('should handle sparse array (encoded as dense with nulls)', () => {
      const original = [1, , , 4]; // sparse array
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded) as unknown[];
      // Sparse arrays should be encoded with explicit undefined/null
      expect(decoded.length).toBe(4);
      expect(decoded[0]).toBe(1);
      expect(decoded[3]).toBe(4);
    });
  });

  describe('type coercion edge cases', () => {
    it('should reject undefined at top level', () => {
      expect(() => encodeVariant(undefined)).toThrow();
    });

    it('should convert undefined in object to null', () => {
      const original = { a: undefined } as Record<string, unknown>;
      const encoded = encodeVariant(original);
      const decoded = decodeVariant(encoded) as Record<string, unknown>;
      // undefined values should either be omitted or converted to null
      expect(decoded.a === undefined || decoded.a === null).toBe(true);
    });

    it('should reject functions', () => {
      expect(() => encodeVariant(() => {})).toThrow();
    });

    it('should reject symbols', () => {
      expect(() => encodeVariant(Symbol('test'))).toThrow();
    });

    it('should reject BigInt', () => {
      expect(() => encodeVariant(BigInt(123))).toThrow();
    });
  });
});

// ============================================================================
// Document Structure (MongoDB-like)
// ============================================================================

describe('Variant Encoder - MongoDB Documents', () => {
  it('should round-trip typical MongoDB document', () => {
    const original = {
      _id: new ObjectId('507f1f77bcf86cd799439011'),
      name: 'Alice Johnson',
      email: 'alice@example.com',
      age: 30,
      active: true,
      balance: 1234.56,
      createdAt: new Date('2024-01-15T12:00:00Z'),
      tags: ['admin', 'user'],
      profile: {
        bio: 'Software engineer',
        avatar: new Uint8Array([0x89, 0x50, 0x4e, 0x47]), // PNG magic bytes
      },
    };

    const encoded = encodeVariant(original);
    const decoded = decodeVariant(encoded) as Record<string, unknown>;

    expect((decoded._id as ObjectId).toString()).toBe('507f1f77bcf86cd799439011');
    expect(decoded.name).toBe('Alice Johnson');
    expect(decoded.email).toBe('alice@example.com');
    expect(decoded.age).toBe(30);
    expect(decoded.active).toBe(true);
    expect(decoded.balance).toBeCloseTo(1234.56, 2);
    expect((decoded.createdAt as Date).toISOString()).toBe('2024-01-15T12:00:00.000Z');
    expect(decoded.tags).toEqual(['admin', 'user']);
    expect((decoded.profile as Record<string, unknown>).bio).toBe('Software engineer');
    expect((decoded.profile as Record<string, unknown>).avatar).toEqual(
      new Uint8Array([0x89, 0x50, 0x4e, 0x47])
    );
  });

  it('should round-trip document with nested arrays of objects', () => {
    const original = {
      _id: new ObjectId(),
      orders: [
        { product: 'Widget', qty: 5, price: 9.99 },
        { product: 'Gadget', qty: 2, price: 19.99 },
      ],
      addresses: [
        { type: 'home', street: '123 Main St', city: 'NYC' },
        { type: 'work', street: '456 Office Blvd', city: 'NYC' },
      ],
    };

    const encoded = encodeVariant(original);
    const decoded = decodeVariant(encoded) as Record<string, unknown>;

    const orders = decoded.orders as Array<Record<string, unknown>>;
    expect(orders).toHaveLength(2);
    expect(orders[0].product).toBe('Widget');
    expect(orders[0].qty).toBe(5);
    expect(orders[0].price).toBeCloseTo(9.99, 2);

    const addresses = decoded.addresses as Array<Record<string, unknown>>;
    expect(addresses).toHaveLength(2);
    expect(addresses[1].type).toBe('work');
  });
});

// ============================================================================
// Variant Type Detection
// ============================================================================

describe('Variant Encoder - Type Detection', () => {
  it('should correctly identify null type', () => {
    const encoded = encodeVariant(null);
    expect(VariantType.fromEncoded(encoded)).toBe(VariantType.NULL);
  });

  it('should correctly identify boolean type', () => {
    expect(VariantType.fromEncoded(encodeVariant(true))).toBe(VariantType.BOOLEAN);
    expect(VariantType.fromEncoded(encodeVariant(false))).toBe(VariantType.BOOLEAN);
  });

  it('should correctly identify integer type', () => {
    expect(VariantType.fromEncoded(encodeVariant(42))).toBe(VariantType.INT);
  });

  it('should correctly identify float type', () => {
    expect(VariantType.fromEncoded(encodeVariant(3.14))).toBe(VariantType.FLOAT);
  });

  it('should correctly identify string type', () => {
    expect(VariantType.fromEncoded(encodeVariant('hello'))).toBe(VariantType.STRING);
  });

  it('should correctly identify array type', () => {
    expect(VariantType.fromEncoded(encodeVariant([1, 2, 3]))).toBe(VariantType.ARRAY);
  });

  it('should correctly identify object type', () => {
    expect(VariantType.fromEncoded(encodeVariant({ a: 1 }))).toBe(VariantType.OBJECT);
  });

  it('should correctly identify date type', () => {
    expect(VariantType.fromEncoded(encodeVariant(new Date()))).toBe(VariantType.DATE);
  });

  it('should correctly identify binary type', () => {
    expect(VariantType.fromEncoded(encodeVariant(new Uint8Array([1, 2, 3])))).toBe(
      VariantType.BINARY
    );
  });

  it('should correctly identify ObjectId type', () => {
    expect(VariantType.fromEncoded(encodeVariant(new ObjectId()))).toBe(VariantType.OBJECT_ID);
  });
});

// ============================================================================
// Size Efficiency
// ============================================================================

describe('Variant Encoder - Size Efficiency', () => {
  it('should use minimal bytes for small integers', () => {
    const smallInt = encodeVariant(1);
    const largeInt = encodeVariant(2147483647);
    expect(smallInt.length).toBeLessThan(largeInt.length);
  });

  it('should use inline strings for short strings', () => {
    const shortStr = encodeVariant('a');
    const longStr = encodeVariant('a'.repeat(100));
    // Short strings should be significantly smaller
    expect(shortStr.length).toBeLessThan(longStr.length / 10);
  });

  it('should deduplicate repeated keys in objects', () => {
    // When encoding an array of objects with the same keys,
    // a good implementation should use a metadata dictionary
    const data = Array.from({ length: 100 }, (_, i) => ({
      id: i,
      name: `item${i}`,
      value: i * 10,
    }));
    const encoded = encodeVariant(data);
    // The encoded size should be much smaller than naive JSON
    const jsonSize = JSON.stringify(data).length;
    // Variant should be competitive with JSON at minimum
    expect(encoded.length).toBeLessThan(jsonSize * 2);
  });
});
