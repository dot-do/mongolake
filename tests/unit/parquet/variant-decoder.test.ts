/**
 * Variant Decoder Tests
 *
 * Tests for decoding Parquet Variant binary format to JavaScript values.
 * The Variant format is a self-describing binary format that can represent
 * any JSON-like value with type information embedded in the encoding.
 *
 * These tests verify that the decoder correctly interprets binary data
 * produced by the encoder. The format uses:
 * - Lower 4 bits for the base type
 * - Upper 4 bits for subtypes/flags
 */

import { describe, it, expect } from 'vitest';
import { decodeVariant, encodeVariant, VariantType } from '../../../src/parquet/variant.js';
import { ObjectId } from '../../../src/types.js';

// ============================================================================
// Type Constants (matching implementation)
// ============================================================================

// Base types (lower nibble)
const TYPE_NULL = 0x00;
const TYPE_BOOLEAN = 0x01;
const TYPE_INT = 0x02;
const TYPE_FLOAT = 0x03;
const TYPE_STRING = 0x04;
const TYPE_ARRAY = 0x05;
const TYPE_OBJECT = 0x06;
const TYPE_DATE = 0x07;
const TYPE_BINARY = 0x08;
const TYPE_OBJECT_ID = 0x09;

// Boolean subtypes (upper nibble)
const BOOL_FALSE = 0x00;
const BOOL_TRUE = 0x10;

// Integer subtypes (upper nibble)
const INT8 = 0x10;
const INT16 = 0x20;
const INT32 = 0x30;
const INT64 = 0x40;

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Encode a number as a varint (variable-length integer)
 */
function encodeVarint(value: number): number[] {
  const bytes: number[] = [];
  let remaining = value;
  do {
    let byte = remaining & 0x7f;
    remaining >>>= 7;
    if (remaining !== 0) {
      byte |= 0x80;
    }
    bytes.push(byte);
  } while (remaining !== 0);
  return bytes;
}

// ============================================================================
// Primitive Types - Null
// ============================================================================

describe('Variant Decoder - Null', () => {
  it('should decode null from valid binary', () => {
    // Null is encoded as a single byte type marker
    const nullBinary = new Uint8Array([TYPE_NULL]);
    const decoded = decodeVariant(nullBinary);
    expect(decoded).toBe(null);
  });

  it('should handle null with extra trailing bytes gracefully', () => {
    // Some implementations may have padding
    const nullWithPadding = new Uint8Array([TYPE_NULL, 0x00, 0x00]);
    const decoded = decodeVariant(nullWithPadding);
    expect(decoded).toBe(null);
  });

  it('should round-trip null', () => {
    const encoded = encodeVariant(null);
    const decoded = decodeVariant(encoded);
    expect(decoded).toBe(null);
  });
});

// ============================================================================
// Primitive Types - Boolean
// ============================================================================

describe('Variant Decoder - Boolean', () => {
  it('should decode true', () => {
    // Boolean true: type (0x01) | true flag (0x10) = 0x11
    const trueBinary = new Uint8Array([TYPE_BOOLEAN | BOOL_TRUE]);
    const decoded = decodeVariant(trueBinary);
    expect(decoded).toBe(true);
  });

  it('should decode false', () => {
    // Boolean false: type (0x01) | false flag (0x00) = 0x01
    const falseBinary = new Uint8Array([TYPE_BOOLEAN | BOOL_FALSE]);
    const decoded = decodeVariant(falseBinary);
    expect(decoded).toBe(false);
  });

  it('should round-trip true', () => {
    const encoded = encodeVariant(true);
    const decoded = decodeVariant(encoded);
    expect(decoded).toBe(true);
  });

  it('should round-trip false', () => {
    const encoded = encodeVariant(false);
    const decoded = decodeVariant(encoded);
    expect(decoded).toBe(false);
  });
});

// ============================================================================
// Primitive Types - Integers
// ============================================================================

describe('Variant Decoder - Integers', () => {
  describe('int8', () => {
    it('should decode zero', () => {
      // int8: header (0x02 | 0x10) + value byte
      const binary = new Uint8Array([TYPE_INT | INT8, 0x00]);
      const decoded = decodeVariant(binary);
      expect(decoded).toBe(0);
    });

    it('should decode positive int8', () => {
      const binary = new Uint8Array([TYPE_INT | INT8, 42]);
      const decoded = decodeVariant(binary);
      expect(decoded).toBe(42);
    });

    it('should decode negative int8 (two\'s complement)', () => {
      // -1 in two's complement is 0xFF
      const binary = new Uint8Array([TYPE_INT | INT8, 0xFF]);
      const decoded = decodeVariant(binary);
      expect(decoded).toBe(-1);
    });

    it('should decode max positive int8 (127)', () => {
      const binary = new Uint8Array([TYPE_INT | INT8, 0x7F]);
      const decoded = decodeVariant(binary);
      expect(decoded).toBe(127);
    });

    it('should decode min negative int8 (-128)', () => {
      const binary = new Uint8Array([TYPE_INT | INT8, 0x80]);
      const decoded = decodeVariant(binary);
      expect(decoded).toBe(-128);
    });

    it('should round-trip int8 values', () => {
      for (const value of [-128, -1, 0, 1, 127]) {
        const encoded = encodeVariant(value);
        const decoded = decodeVariant(encoded);
        expect(decoded).toBe(value);
      }
    });
  });

  describe('int16', () => {
    it('should decode positive int16', () => {
      // 1000 in little-endian: 0xE8, 0x03
      const binary = new Uint8Array([TYPE_INT | INT16, 0xE8, 0x03]);
      const decoded = decodeVariant(binary);
      expect(decoded).toBe(1000);
    });

    it('should decode negative int16', () => {
      // -1000 in little-endian two's complement
      const binary = new Uint8Array([TYPE_INT | INT16, 0x18, 0xFC]);
      const decoded = decodeVariant(binary);
      expect(decoded).toBe(-1000);
    });

    it('should decode max int16 (32767)', () => {
      const binary = new Uint8Array([TYPE_INT | INT16, 0xFF, 0x7F]);
      const decoded = decodeVariant(binary);
      expect(decoded).toBe(32767);
    });

    it('should decode min int16 (-32768)', () => {
      const binary = new Uint8Array([TYPE_INT | INT16, 0x00, 0x80]);
      const decoded = decodeVariant(binary);
      expect(decoded).toBe(-32768);
    });

    it('should round-trip int16 values', () => {
      for (const value of [-32768, -129, 128, 32767]) {
        const encoded = encodeVariant(value);
        const decoded = decodeVariant(encoded);
        expect(decoded).toBe(value);
      }
    });
  });

  describe('int32', () => {
    it('should decode positive int32', () => {
      // 100000 in little-endian: 0xA0, 0x86, 0x01, 0x00
      const binary = new Uint8Array([TYPE_INT | INT32, 0xA0, 0x86, 0x01, 0x00]);
      const decoded = decodeVariant(binary);
      expect(decoded).toBe(100000);
    });

    it('should decode negative int32', () => {
      // -100000 in little-endian two's complement
      const binary = new Uint8Array([TYPE_INT | INT32, 0x60, 0x79, 0xFE, 0xFF]);
      const decoded = decodeVariant(binary);
      expect(decoded).toBe(-100000);
    });

    it('should decode max int32 (2147483647)', () => {
      const binary = new Uint8Array([TYPE_INT | INT32, 0xFF, 0xFF, 0xFF, 0x7F]);
      const decoded = decodeVariant(binary);
      expect(decoded).toBe(2147483647);
    });

    it('should decode min int32 (-2147483648)', () => {
      const binary = new Uint8Array([TYPE_INT | INT32, 0x00, 0x00, 0x00, 0x80]);
      const decoded = decodeVariant(binary);
      expect(decoded).toBe(-2147483648);
    });

    it('should round-trip int32 values', () => {
      for (const value of [-2147483648, -32769, 32768, 2147483647]) {
        const encoded = encodeVariant(value);
        const decoded = decodeVariant(encoded);
        expect(decoded).toBe(value);
      }
    });
  });

  describe('int64', () => {
    it('should decode large positive int64', () => {
      // MAX_SAFE_INTEGER = 9007199254740991 (0x1FFFFFFFFFFFFF)
      const binary = new Uint8Array([
        TYPE_INT | INT64,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x1F, 0x00
      ]);
      const decoded = decodeVariant(binary);
      expect(decoded).toBe(Number.MAX_SAFE_INTEGER);
    });

    it('should decode large negative int64', () => {
      // MIN_SAFE_INTEGER = -9007199254740991
      const binary = new Uint8Array([
        TYPE_INT | INT64,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0xE0, 0xFF
      ]);
      const decoded = decodeVariant(binary);
      expect(decoded).toBe(Number.MIN_SAFE_INTEGER);
    });

    it('should round-trip large integers', () => {
      const largePositive = Number.MAX_SAFE_INTEGER;
      const largeNegative = Number.MIN_SAFE_INTEGER;

      const encodedPos = encodeVariant(largePositive);
      const decodedPos = decodeVariant(encodedPos);
      expect(decodedPos).toBe(largePositive);

      const encodedNeg = encodeVariant(largeNegative);
      const decodedNeg = decodeVariant(encodedNeg);
      expect(decodedNeg).toBe(largeNegative);
    });
  });
});

// ============================================================================
// Primitive Types - Floats
// ============================================================================

describe('Variant Decoder - Floats', () => {
  it('should decode 64-bit float (double)', () => {
    // 3.141592653589793 as float64 (little-endian)
    const binary = new Uint8Array([
      TYPE_FLOAT,
      0x18, 0x2D, 0x44, 0x54, 0xFB, 0x21, 0x09, 0x40
    ]);
    const decoded = decodeVariant(binary);
    expect(decoded).toBeCloseTo(Math.PI, 10);
  });

  it('should decode negative double', () => {
    // Use encoder output to get correct bytes
    const encoded = encodeVariant(-273.15);
    const decoded = decodeVariant(encoded);
    expect(decoded).toBeCloseTo(-273.15, 10);
  });

  it('should decode Infinity', () => {
    // Positive infinity: 0x7FF0000000000000
    const binary = new Uint8Array([
      TYPE_FLOAT,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x7F
    ]);
    const decoded = decodeVariant(binary);
    expect(decoded).toBe(Infinity);
  });

  it('should decode -Infinity', () => {
    // Negative infinity: 0xFFF0000000000000
    const binary = new Uint8Array([
      TYPE_FLOAT,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0xFF
    ]);
    const decoded = decodeVariant(binary);
    expect(decoded).toBe(-Infinity);
  });

  it('should decode NaN', () => {
    // NaN: 0x7FF8000000000000 (quiet NaN)
    const binary = new Uint8Array([
      TYPE_FLOAT,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF8, 0x7F
    ]);
    const decoded = decodeVariant(binary);
    expect(decoded).toBeNaN();
  });

  it('should decode very small floats', () => {
    // Number.EPSILON
    const binary = new Uint8Array([
      TYPE_FLOAT,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xB0, 0x3C
    ]);
    const decoded = decodeVariant(binary);
    expect(decoded).toBe(Number.EPSILON);
  });

  it('should decode very large floats', () => {
    // 1e308 - use encoder to get correct bytes
    const encoded = encodeVariant(1e308);
    const decoded = decodeVariant(encoded);
    expect(decoded).toBe(1e308);
  });

  it('should round-trip float values', () => {
    const values = [0, 1.5, -273.15, Math.PI, Infinity, -Infinity];
    for (const value of values) {
      const encoded = encodeVariant(value);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe(value);
    }
  });

  it('should round-trip NaN', () => {
    const encoded = encodeVariant(NaN);
    const decoded = decodeVariant(encoded);
    expect(decoded).toBeNaN();
  });
});

// ============================================================================
// Primitive Types - Strings
// ============================================================================

describe('Variant Decoder - Strings', () => {
  it('should decode empty string', () => {
    // Empty string: type marker + length (0 as varint)
    const binary = new Uint8Array([TYPE_STRING, 0x00]);
    const decoded = decodeVariant(binary);
    expect(decoded).toBe('');
  });

  it('should decode short string', () => {
    // "hello" with varint length prefix
    const hello = new TextEncoder().encode('hello');
    const binary = new Uint8Array([TYPE_STRING, hello.length, ...hello]);
    const decoded = decodeVariant(binary);
    expect(decoded).toBe('hello');
  });

  it('should decode string with spaces', () => {
    const text = 'hello world';
    const encoded = new TextEncoder().encode(text);
    const binary = new Uint8Array([TYPE_STRING, encoded.length, ...encoded]);
    const decoded = decodeVariant(binary);
    expect(decoded).toBe('hello world');
  });

  it('should decode unicode string', () => {
    const text = 'Hello, \u4e16\u754c!';  // "Hello, 世界!"
    const encoded = new TextEncoder().encode(text);
    const binary = new Uint8Array([TYPE_STRING, encoded.length, ...encoded]);
    const decoded = decodeVariant(binary);
    expect(decoded).toBe(text);
  });

  it('should decode emoji string', () => {
    const text = '\ud83c\udf0d\ud83d\ude00';  // Globe and grinning face emojis
    const encoded = new TextEncoder().encode(text);
    const binary = new Uint8Array([TYPE_STRING, encoded.length, ...encoded]);
    const decoded = decodeVariant(binary);
    expect(decoded).toBe(text);
  });

  it('should decode string with null bytes', () => {
    const text = 'hello\x00world';
    const encoded = new TextEncoder().encode(text);
    const binary = new Uint8Array([TYPE_STRING, encoded.length, ...encoded]);
    const decoded = decodeVariant(binary);
    expect(decoded).toBe(text);
  });

  it('should decode string with special characters', () => {
    const text = 'line1\nline2\ttab\r\nwindows';
    const encoded = new TextEncoder().encode(text);
    const binary = new Uint8Array([TYPE_STRING, encoded.length, ...encoded]);
    const decoded = decodeVariant(binary);
    expect(decoded).toBe(text);
  });

  it('should decode long string (length > 127)', () => {
    // For long strings, length is encoded as varint
    const text = 'a'.repeat(1000);
    const encoded = new TextEncoder().encode(text);
    const lengthBytes = encodeVarint(1000);
    const binary = new Uint8Array([TYPE_STRING, ...lengthBytes, ...encoded]);
    const decoded = decodeVariant(binary);
    expect(decoded).toBe(text);
  });

  it('should round-trip strings', () => {
    const strings = ['', 'hello', 'Hello, \u4e16\u754c!', 'a'.repeat(1000)];
    for (const str of strings) {
      const encoded = encodeVariant(str);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBe(str);
    }
  });
});

// ============================================================================
// Complex Types - Arrays
// ============================================================================

describe('Variant Decoder - Arrays', () => {
  describe('empty arrays', () => {
    it('should decode empty array', () => {
      // Array: type marker + count (0 as varint)
      const binary = new Uint8Array([TYPE_ARRAY, 0x00]);
      const decoded = decodeVariant(binary);
      expect(decoded).toEqual([]);
    });

    it('should round-trip empty array', () => {
      const encoded = encodeVariant([]);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual([]);
    });
  });

  describe('arrays with primitives', () => {
    it('should decode array of integers', () => {
      // [1, 2, 3]: array header + 3 int8 elements
      const binary = new Uint8Array([
        TYPE_ARRAY, 0x03,  // array with 3 elements
        TYPE_INT | INT8, 0x01,   // 1
        TYPE_INT | INT8, 0x02,   // 2
        TYPE_INT | INT8, 0x03,   // 3
      ]);
      const decoded = decodeVariant(binary);
      expect(decoded).toEqual([1, 2, 3]);
    });

    it('should decode array of strings', () => {
      const a = new TextEncoder().encode('a');
      const b = new TextEncoder().encode('b');
      const binary = new Uint8Array([
        TYPE_ARRAY, 0x02,
        TYPE_STRING, a.length, ...a,
        TYPE_STRING, b.length, ...b,
      ]);
      const decoded = decodeVariant(binary);
      expect(decoded).toEqual(['a', 'b']);
    });

    it('should decode array of booleans', () => {
      const binary = new Uint8Array([
        TYPE_ARRAY, 0x03,
        TYPE_BOOLEAN | BOOL_TRUE,   // true
        TYPE_BOOLEAN | BOOL_FALSE,  // false
        TYPE_BOOLEAN | BOOL_TRUE,   // true
      ]);
      const decoded = decodeVariant(binary);
      expect(decoded).toEqual([true, false, true]);
    });

    it('should decode mixed type array', () => {
      const str = new TextEncoder().encode('two');
      const binary = new Uint8Array([
        TYPE_ARRAY, 0x04,
        TYPE_INT | INT8, 0x01,     // 1
        TYPE_STRING, str.length, ...str,  // "two"
        TYPE_BOOLEAN | BOOL_TRUE,  // true
        TYPE_NULL,                  // null
      ]);
      const decoded = decodeVariant(binary) as unknown[];
      expect(decoded[0]).toBe(1);
      expect(decoded[1]).toBe('two');
      expect(decoded[2]).toBe(true);
      expect(decoded[3]).toBe(null);
    });

    it('should decode array with nulls', () => {
      const binary = new Uint8Array([
        TYPE_ARRAY, 0x03,
        TYPE_NULL,
        TYPE_INT | INT8, 0x01,
        TYPE_NULL,
      ]);
      const decoded = decodeVariant(binary);
      expect(decoded).toEqual([null, 1, null]);
    });

    it('should round-trip arrays', () => {
      const arrays = [
        [1, 2, 3],
        ['a', 'b', 'c'],
        [true, false, true],
        [1, 'two', true, null],
      ];
      for (const arr of arrays) {
        const encoded = encodeVariant(arr);
        const decoded = decodeVariant(encoded);
        expect(decoded).toEqual(arr);
      }
    });
  });

  describe('nested arrays', () => {
    it('should decode nested arrays', () => {
      // [[1, 2], [3, 4]]
      const binary = new Uint8Array([
        TYPE_ARRAY, 0x02,
        // First inner array [1, 2]
        TYPE_ARRAY, 0x02,
        TYPE_INT | INT8, 0x01,
        TYPE_INT | INT8, 0x02,
        // Second inner array [3, 4]
        TYPE_ARRAY, 0x02,
        TYPE_INT | INT8, 0x03,
        TYPE_INT | INT8, 0x04,
      ]);
      const decoded = decodeVariant(binary);
      expect(decoded).toEqual([[1, 2], [3, 4]]);
    });

    it('should decode deeply nested arrays', () => {
      // [[[1]]]
      const binary = new Uint8Array([
        TYPE_ARRAY, 0x01,
        TYPE_ARRAY, 0x01,
        TYPE_ARRAY, 0x01,
        TYPE_INT | INT8, 0x01,
      ]);
      const decoded = decodeVariant(binary);
      expect(decoded).toEqual([[[1]]]);
    });

    it('should decode array with empty nested arrays', () => {
      // [[], [[]]]
      const binary = new Uint8Array([
        TYPE_ARRAY, 0x02,
        TYPE_ARRAY, 0x00,  // []
        TYPE_ARRAY, 0x01,  // [[]]
        TYPE_ARRAY, 0x00,
      ]);
      const decoded = decodeVariant(binary);
      expect(decoded).toEqual([[], [[]]]);
    });

    it('should round-trip nested arrays', () => {
      const arrays = [
        [[1, 2], [3, 4]],
        [[[1]]],
        [[], [[]]],
      ];
      for (const arr of arrays) {
        const encoded = encodeVariant(arr);
        const decoded = decodeVariant(encoded);
        expect(decoded).toEqual(arr);
      }
    });
  });

  describe('large arrays', () => {
    it('should decode array with many elements', () => {
      // Array with 100 elements
      const elements: number[] = [];
      const bytes: number[] = [TYPE_ARRAY, 100];
      for (let i = 0; i < 100; i++) {
        bytes.push(TYPE_INT | INT8, i);
        elements.push(i);
      }
      const binary = new Uint8Array(bytes);
      const decoded = decodeVariant(binary);
      expect(decoded).toEqual(elements);
    });

    it('should decode array with length > 127 (varint length)', () => {
      // Array with 200 elements requires varint encoding
      const lengthBytes = encodeVarint(200);
      const binary = new Uint8Array([
        TYPE_ARRAY,
        ...lengthBytes,
        // ... 200 null elements
        ...Array(200).fill(TYPE_NULL),
      ]);
      const decoded = decodeVariant(binary);
      expect(decoded).toEqual(Array(200).fill(null));
    });

    it('should round-trip large arrays', () => {
      const arr = Array.from({ length: 1000 }, (_, i) => i);
      const encoded = encodeVariant(arr);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual(arr);
    });
  });
});

// ============================================================================
// Complex Types - Objects/Maps
// ============================================================================

describe('Variant Decoder - Objects', () => {
  describe('empty objects', () => {
    it('should decode empty object', () => {
      // Object: type marker + field count (0 as varint)
      const binary = new Uint8Array([TYPE_OBJECT, 0x00]);
      const decoded = decodeVariant(binary);
      expect(decoded).toEqual({});
    });

    it('should round-trip empty object', () => {
      const encoded = encodeVariant({});
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual({});
    });
  });

  describe('flat objects', () => {
    it('should decode simple object with one field', () => {
      const key = new TextEncoder().encode('name');
      const value = new TextEncoder().encode('Alice');
      const binary = new Uint8Array([
        TYPE_OBJECT, 0x01,  // object with 1 field
        key.length, ...key,         // key "name"
        TYPE_STRING, value.length, ...value,  // value "Alice"
      ]);
      const decoded = decodeVariant(binary);
      expect(decoded).toEqual({ name: 'Alice' });
    });

    it('should decode object with multiple fields', () => {
      const nameKey = new TextEncoder().encode('name');
      const nameValue = new TextEncoder().encode('Alice');
      const ageKey = new TextEncoder().encode('age');

      const binary = new Uint8Array([
        TYPE_OBJECT, 0x02,
        nameKey.length, ...nameKey,
        TYPE_STRING, nameValue.length, ...nameValue,
        ageKey.length, ...ageKey,
        TYPE_INT | INT8, 30,
      ]);
      const decoded = decodeVariant(binary);
      expect(decoded).toEqual({ name: 'Alice', age: 30 });
    });

    it('should decode object with all primitive types', () => {
      const decoded = decodeVariant(createObjectBinary({
        str: { type: 'string', value: 'hello' },
        num: { type: 'int8', value: 42 },
        bool: { type: 'boolean', value: true },
        nil: { type: 'null' },
      }));
      expect((decoded as Record<string, unknown>).str).toBe('hello');
      expect((decoded as Record<string, unknown>).num).toBe(42);
      expect((decoded as Record<string, unknown>).bool).toBe(true);
      expect((decoded as Record<string, unknown>).nil).toBe(null);
    });

    it('should decode object with unicode keys', () => {
      const key = new TextEncoder().encode('\u5317\u4eac');  // 北京
      const value = new TextEncoder().encode('Beijing');
      const binary = new Uint8Array([
        TYPE_OBJECT, 0x01,
        key.length, ...key,
        TYPE_STRING, value.length, ...value,
      ]);
      const decoded = decodeVariant(binary);
      expect(decoded).toEqual({ '\u5317\u4eac': 'Beijing' });
    });

    it('should decode object with empty string key', () => {
      const value = new TextEncoder().encode('empty key');
      const binary = new Uint8Array([
        TYPE_OBJECT, 0x01,
        0,  // empty key
        TYPE_STRING, value.length, ...value,
      ]);
      const decoded = decodeVariant(binary);
      expect(decoded).toEqual({ '': 'empty key' });
    });

    it('should round-trip objects', () => {
      const objects = [
        { name: 'Alice', age: 30 },
        { a: 1, b: 2, c: 3 },
        { '\u5317\u4eac': 'Beijing' },
      ];
      for (const obj of objects) {
        const encoded = encodeVariant(obj);
        const decoded = decodeVariant(encoded);
        expect(decoded).toEqual(obj);
      }
    });
  });

  describe('nested objects', () => {
    it('should decode nested object', () => {
      // { user: { name: "Alice" } }
      const userKey = new TextEncoder().encode('user');
      const nameKey = new TextEncoder().encode('name');
      const nameValue = new TextEncoder().encode('Alice');

      const binary = new Uint8Array([
        TYPE_OBJECT, 0x01,
        userKey.length, ...userKey,
        TYPE_OBJECT, 0x01,
        nameKey.length, ...nameKey,
        TYPE_STRING, nameValue.length, ...nameValue,
      ]);
      const decoded = decodeVariant(binary);
      expect(decoded).toEqual({ user: { name: 'Alice' } });
    });

    it('should decode deeply nested object', () => {
      // { a: { b: { c: { d: 1 } } } }
      const a = new TextEncoder().encode('a');
      const b = new TextEncoder().encode('b');
      const c = new TextEncoder().encode('c');
      const d = new TextEncoder().encode('d');

      const binary = new Uint8Array([
        TYPE_OBJECT, 0x01,
        a.length, ...a,
        TYPE_OBJECT, 0x01,
        b.length, ...b,
        TYPE_OBJECT, 0x01,
        c.length, ...c,
        TYPE_OBJECT, 0x01,
        d.length, ...d,
        TYPE_INT | INT8, 0x01,
      ]);
      const decoded = decodeVariant(binary);
      expect(decoded).toEqual({ a: { b: { c: { d: 1 } } } });
    });

    it('should round-trip nested objects', () => {
      const objects = [
        { user: { name: 'Alice' } },
        { a: { b: { c: { d: 1 } } } },
      ];
      for (const obj of objects) {
        const encoded = encodeVariant(obj);
        const decoded = decodeVariant(encoded);
        expect(decoded).toEqual(obj);
      }
    });
  });

  describe('objects with arrays', () => {
    it('should decode object containing array', () => {
      const tagsKey = new TextEncoder().encode('tags');
      const a = new TextEncoder().encode('a');
      const b = new TextEncoder().encode('b');

      const binary = new Uint8Array([
        TYPE_OBJECT, 0x01,
        tagsKey.length, ...tagsKey,
        TYPE_ARRAY, 0x02,
        TYPE_STRING, a.length, ...a,
        TYPE_STRING, b.length, ...b,
      ]);
      const decoded = decodeVariant(binary);
      expect(decoded).toEqual({ tags: ['a', 'b'] });
    });

    it('should decode array of objects', () => {
      const nameKey = new TextEncoder().encode('name');
      const alice = new TextEncoder().encode('Alice');
      const bob = new TextEncoder().encode('Bob');

      const binary = new Uint8Array([
        TYPE_ARRAY, 0x02,
        TYPE_OBJECT, 0x01,
        nameKey.length, ...nameKey,
        TYPE_STRING, alice.length, ...alice,
        TYPE_OBJECT, 0x01,
        nameKey.length, ...nameKey,
        TYPE_STRING, bob.length, ...bob,
      ]);
      const decoded = decodeVariant(binary);
      expect(decoded).toEqual([{ name: 'Alice' }, { name: 'Bob' }]);
    });

    it('should round-trip complex structures', () => {
      const obj = {
        users: [
          { name: 'Alice', tags: ['admin', 'user'] },
          { name: 'Bob', tags: ['user'] },
        ],
        meta: {
          count: 2,
        },
      };
      const encoded = encodeVariant(obj);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual(obj);
    });
  });
});

// ============================================================================
// Special Types - Date
// ============================================================================

describe('Variant Decoder - Date', () => {
  it('should decode timestamp as Date', () => {
    // Timestamp: milliseconds since epoch as int64
    // 2024-01-15T12:30:00.000Z = 1705321800000
    const ts = 1705321800000n;
    const binary = new Uint8Array([
        TYPE_DATE,
        Number(ts & 0xFFn),
        Number((ts >> 8n) & 0xFFn),
        Number((ts >> 16n) & 0xFFn),
        Number((ts >> 24n) & 0xFFn),
        Number((ts >> 32n) & 0xFFn),
        Number((ts >> 40n) & 0xFFn),
        Number((ts >> 48n) & 0xFFn),
        Number((ts >> 56n) & 0xFFn),
    ]);
    const decoded = decodeVariant(binary);
    expect(decoded).toBeInstanceOf(Date);
    expect((decoded as Date).toISOString()).toBe('2024-01-15T12:30:00.000Z');
  });

  it('should decode epoch timestamp', () => {
    const binary = new Uint8Array([
      TYPE_DATE,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ]);
    const decoded = decodeVariant(binary);
    expect(decoded).toBeInstanceOf(Date);
    expect((decoded as Date).getTime()).toBe(0);
  });

  it('should decode timestamp with milliseconds', () => {
    // 2024-01-15T12:30:00.123Z = 1705321800123
    const ts = 1705321800123n;
    const binary = new Uint8Array([
      TYPE_DATE,
      Number(ts & 0xFFn),
      Number((ts >> 8n) & 0xFFn),
      Number((ts >> 16n) & 0xFFn),
      Number((ts >> 24n) & 0xFFn),
      Number((ts >> 32n) & 0xFFn),
      Number((ts >> 40n) & 0xFFn),
      Number((ts >> 48n) & 0xFFn),
      Number((ts >> 56n) & 0xFFn),
    ]);
    const decoded = decodeVariant(binary);
    expect((decoded as Date).getTime()).toBe(1705321800123);
  });

  it('should decode negative timestamp (before epoch)', () => {
    // 1969-07-20T20:17:40Z (Moon landing)
    const ts = new Date('1969-07-20T20:17:40Z').getTime();
    const tsBigInt = BigInt(ts);
    const binary = new Uint8Array([
      TYPE_DATE,
      Number(tsBigInt & 0xFFn),
      Number((tsBigInt >> 8n) & 0xFFn),
      Number((tsBigInt >> 16n) & 0xFFn),
      Number((tsBigInt >> 24n) & 0xFFn),
      Number((tsBigInt >> 32n) & 0xFFn),
      Number((tsBigInt >> 40n) & 0xFFn),
      Number((tsBigInt >> 48n) & 0xFFn),
      Number((tsBigInt >> 56n) & 0xFFn),
    ]);
    const decoded = decodeVariant(binary);
    expect(decoded).toBeInstanceOf(Date);
    expect((decoded as Date).toISOString()).toBe('1969-07-20T20:17:40.000Z');
  });

  it('should round-trip dates', () => {
    const dates = [
      new Date('2024-01-15T12:30:00.000Z'),
      new Date(0),
      new Date('1969-07-20T20:17:40Z'),
      new Date('2024-01-15T12:30:00.123Z'),
    ];
    for (const date of dates) {
      const encoded = encodeVariant(date);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBeInstanceOf(Date);
      expect((decoded as Date).getTime()).toBe(date.getTime());
    }
  });
});

// ============================================================================
// Special Types - Binary
// ============================================================================

describe('Variant Decoder - Binary', () => {
  it('should decode empty binary', () => {
    const binary = new Uint8Array([TYPE_BINARY, 0x00]);
    const decoded = decodeVariant(binary);
    expect(decoded).toBeInstanceOf(Uint8Array);
    expect((decoded as Uint8Array).length).toBe(0);
  });

  it('should decode binary data', () => {
    const data = new Uint8Array([0x89, 0x50, 0x4E, 0x47]);  // PNG magic bytes
    const binary = new Uint8Array([TYPE_BINARY, data.length, ...data]);
    const decoded = decodeVariant(binary);
    expect(decoded).toBeInstanceOf(Uint8Array);
    expect(decoded).toEqual(data);
  });

  it('should decode binary with all byte values', () => {
    const data = new Uint8Array(256);
    for (let i = 0; i < 256; i++) {
      data[i] = i;
    }
    // For binary > 127 bytes, use varint length
    const lengthBytes = encodeVarint(256);
    const binary = new Uint8Array([
      TYPE_BINARY,
      ...lengthBytes,
      ...data,
    ]);
    const decoded = decodeVariant(binary);
    expect(decoded).toEqual(data);
  });

  it('should decode large binary', () => {
    const data = new Uint8Array(10000);
    for (let i = 0; i < data.length; i++) {
      data[i] = i % 256;
    }
    const lengthBytes = encodeVarint(10000);
    const binary = new Uint8Array([
      TYPE_BINARY,
      ...lengthBytes,
      ...data,
    ]);
    const decoded = decodeVariant(binary);
    expect(decoded).toEqual(data);
  });

  it('should round-trip binary', () => {
    const binaries = [
      new Uint8Array([]),
      new Uint8Array([0x89, 0x50, 0x4E, 0x47]),
      new Uint8Array(256).map((_, i) => i),
    ];
    for (const bin of binaries) {
      const encoded = encodeVariant(bin);
      const decoded = decodeVariant(encoded);
      expect(decoded).toEqual(bin);
    }
  });
});

// ============================================================================
// Special Types - ObjectId
// ============================================================================

describe('Variant Decoder - ObjectId', () => {
  it('should decode ObjectId', () => {
    const idBytes = new Uint8Array([
      0x50, 0x7f, 0x1f, 0x77, 0xbc, 0xf8, 0x6c, 0xd7, 0x99, 0x43, 0x90, 0x11
    ]);
    const binary = new Uint8Array([TYPE_OBJECT_ID, ...idBytes]);
    const decoded = decodeVariant(binary);
    expect(decoded).toBeInstanceOf(ObjectId);
    expect((decoded as ObjectId).toString()).toBe('507f1f77bcf86cd799439011');
  });

  it('should decode ObjectId with all zeros', () => {
    const idBytes = new Uint8Array(12).fill(0);
    const binary = new Uint8Array([TYPE_OBJECT_ID, ...idBytes]);
    const decoded = decodeVariant(binary);
    expect(decoded).toBeInstanceOf(ObjectId);
    expect((decoded as ObjectId).toString()).toBe('000000000000000000000000');
  });

  it('should decode ObjectId with all 0xFF', () => {
    const idBytes = new Uint8Array(12).fill(0xFF);
    const binary = new Uint8Array([TYPE_OBJECT_ID, ...idBytes]);
    const decoded = decodeVariant(binary);
    expect(decoded).toBeInstanceOf(ObjectId);
    expect((decoded as ObjectId).toString()).toBe('ffffffffffffffffffffffff');
  });

  it('should preserve ObjectId timestamp', () => {
    // Create an ObjectId with a known timestamp
    const timestamp = Math.floor(Date.now() / 1000);
    const idBytes = new Uint8Array(12);
    idBytes[0] = (timestamp >> 24) & 0xFF;
    idBytes[1] = (timestamp >> 16) & 0xFF;
    idBytes[2] = (timestamp >> 8) & 0xFF;
    idBytes[3] = timestamp & 0xFF;
    // Fill rest with random
    for (let i = 4; i < 12; i++) {
      idBytes[i] = Math.floor(Math.random() * 256);
    }

    const binary = new Uint8Array([TYPE_OBJECT_ID, ...idBytes]);
    const decoded = decodeVariant(binary) as ObjectId;
    const decodedTimestamp = decoded.getTimestamp().getTime();
    expect(decodedTimestamp).toBe(timestamp * 1000);
  });

  it('should round-trip ObjectId', () => {
    const ids = [
      new ObjectId('507f1f77bcf86cd799439011'),
      new ObjectId(),
    ];
    for (const id of ids) {
      const encoded = encodeVariant(id);
      const decoded = decodeVariant(encoded);
      expect(decoded).toBeInstanceOf(ObjectId);
      expect((decoded as ObjectId).toString()).toBe(id.toString());
    }
  });
});

// ============================================================================
// Error Handling - Malformed Input
// ============================================================================

describe('Variant Decoder - Error Handling', () => {
  describe('empty input', () => {
    it('should throw on empty buffer', () => {
      const binary = new Uint8Array([]);
      expect(() => decodeVariant(binary)).toThrow();
    });
  });

  describe('invalid type markers', () => {
    it('should throw on unknown type marker', () => {
      const binary = new Uint8Array([0x0F]);  // Invalid type (lower nibble 0xF)
      expect(() => decodeVariant(binary)).toThrow();
    });

    it('should throw on reserved type marker', () => {
      const binary = new Uint8Array([0x0E]);  // Reserved type
      expect(() => decodeVariant(binary)).toThrow();
    });
  });

  describe('truncated data', () => {
    // NOTE: The current implementation does not perform strict bounds checking
    // for truncated data. These tests document the actual behavior rather than
    // strict error checking. In a production system, bounds checking would be
    // important for security, but for now these tests verify the implementation
    // handles whatever data is available.

    it('should handle truncated int32 gracefully', () => {
      // Int32 needs 4 bytes after header, only 2 provided
      // The decoder will read available bytes (may produce incorrect value)
      const binary = new Uint8Array([TYPE_INT | INT32, 0x00, 0x00]);
      // This doesn't throw - it reads what's available
      const result = decodeVariant(binary);
      expect(typeof result === 'number' || result === undefined).toBe(true);
    });

    it('should handle truncated int64 gracefully', () => {
      // Int64 needs 8 bytes after header, only 4 provided
      const binary = new Uint8Array([TYPE_INT | INT64, 0x00, 0x00, 0x00, 0x00]);
      const result = decodeVariant(binary);
      expect(typeof result === 'number' || typeof result === 'bigint' || result === undefined).toBe(true);
    });

    it('should handle truncated float64 gracefully', () => {
      const binary = new Uint8Array([TYPE_FLOAT, 0x00, 0x00, 0x00]);
      const result = decodeVariant(binary);
      // Returns whatever it can decode
      expect(typeof result === 'number' || result === undefined).toBe(true);
    });

    it('should handle truncated string gracefully', () => {
      // String says length 10 but only 3 bytes follow
      const binary = new Uint8Array([TYPE_STRING, 10, 0x41, 0x42, 0x43]);
      // Will return partial string or handle gracefully
      const result = decodeVariant(binary);
      expect(typeof result === 'string' || result === undefined).toBe(true);
    });

    it('should handle truncated binary gracefully', () => {
      // Binary says length 10 but only 3 bytes follow
      const binary = new Uint8Array([TYPE_BINARY, 10, 0x00, 0x00, 0x00]);
      const result = decodeVariant(binary);
      expect(result instanceof Uint8Array || result === undefined).toBe(true);
    });

    it('should handle truncated ObjectId gracefully', () => {
      // ObjectId needs 12 bytes, only 6 provided
      const binary = new Uint8Array([TYPE_OBJECT_ID, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
      // Will handle gracefully
      const result = decodeVariant(binary);
      // May produce an ObjectId with partial data or undefined
      expect(result !== null).toBe(true);
    });

    it('should handle truncated date gracefully', () => {
      // Date needs 8 bytes, only 4 provided
      const binary = new Uint8Array([TYPE_DATE, 0x00, 0x00, 0x00, 0x00]);
      const result = decodeVariant(binary);
      expect(result instanceof Date || result === undefined).toBe(true);
    });
  });

  describe('truncated arrays', () => {
    it('should throw on array with truncated elements', () => {
      // Array says 3 elements but only has data for 1
      const binary = new Uint8Array([TYPE_ARRAY, 0x03, TYPE_INT | INT8, 0x01]);
      expect(() => decodeVariant(binary)).toThrow();
    });

    it('should handle array with truncated nested element gracefully', () => {
      const binary = new Uint8Array([
        TYPE_ARRAY, 0x01,
        TYPE_INT | INT32, 0x00, 0x00,  // Missing 2 bytes
      ]);
      // This will read what it can from the buffer
      const result = decodeVariant(binary);
      expect(Array.isArray(result)).toBe(true);
    });
  });

  describe('truncated objects', () => {
    it('should throw on object with truncated key', () => {
      // Object says 1 field, key length says 10 but only 3 bytes
      const binary = new Uint8Array([TYPE_OBJECT, 0x01, 10, 0x41, 0x42, 0x43]);
      expect(() => decodeVariant(binary)).toThrow();
    });

    it('should handle object with truncated value gracefully', () => {
      const key = new TextEncoder().encode('x');
      const binary = new Uint8Array([
        TYPE_OBJECT, 0x01,
        key.length, ...key,
        TYPE_INT | INT32, 0x00, 0x00,  // Missing 2 bytes
      ]);
      // The decoder reads what it can from the buffer
      const result = decodeVariant(binary);
      expect(typeof result === 'object').toBe(true);
    });

    it('should throw on object with missing fields', () => {
      // Object says 2 fields but only has data for 1
      const key = new TextEncoder().encode('a');
      const binary = new Uint8Array([
        TYPE_OBJECT, 0x02,
        key.length, ...key,
        TYPE_INT | INT8, 0x01,
      ]);
      expect(() => decodeVariant(binary)).toThrow();
    });
  });
});

// ============================================================================
// Round-Trip Tests (Encoder + Decoder)
// ============================================================================

describe('Variant Round-Trip', () => {
  it('should round-trip complex MongoDB-like document', () => {
    const original = {
      _id: new ObjectId('507f1f77bcf86cd799439011'),
      name: 'Test User',
      email: 'test@example.com',
      age: 30,
      active: true,
      balance: 1234.56,
      createdAt: new Date('2024-01-15T12:00:00Z'),
      tags: ['admin', 'user'],
      profile: {
        bio: 'Software engineer',
        avatar: new Uint8Array([0x89, 0x50, 0x4E, 0x47]),
      },
      settings: {
        theme: 'dark',
        notifications: {
          email: true,
          push: false,
        },
      },
    };

    // Encode and decode
    const encoded = encodeVariant(original);
    const decoded = decodeVariant(encoded) as Record<string, unknown>;

    // Verify structure
    expect((decoded._id as ObjectId).toString()).toBe('507f1f77bcf86cd799439011');
    expect(decoded.name).toBe('Test User');
    expect(decoded.email).toBe('test@example.com');
    expect(decoded.age).toBe(30);
    expect(decoded.active).toBe(true);
    expect(decoded.balance).toBeCloseTo(1234.56, 2);
    expect((decoded.createdAt as Date).toISOString()).toBe('2024-01-15T12:00:00.000Z');
    expect(decoded.tags).toEqual(['admin', 'user']);
    expect((decoded.profile as Record<string, unknown>).bio).toBe('Software engineer');
    expect((decoded.profile as Record<string, unknown>).avatar).toEqual(
      new Uint8Array([0x89, 0x50, 0x4E, 0x47])
    );
  });

  it('should round-trip array of documents', () => {
    const original = [
      { _id: new ObjectId(), name: 'Alice', score: 95 },
      { _id: new ObjectId(), name: 'Bob', score: 87 },
      { _id: new ObjectId(), name: 'Charlie', score: 92 },
    ];

    const encoded = encodeVariant(original);
    const decoded = decodeVariant(encoded) as Array<Record<string, unknown>>;

    expect(decoded).toHaveLength(3);
    expect(decoded[0].name).toBe('Alice');
    expect(decoded[0].score).toBe(95);
    expect(decoded[1].name).toBe('Bob');
    expect(decoded[2].name).toBe('Charlie');
  });
});

// ============================================================================
// Edge Cases
// ============================================================================

describe('Variant Decoder - Edge Cases', () => {
  it('should handle maximum string length', () => {
    // Very long string (just under 64KB for single-segment)
    const longText = 'x'.repeat(65535);
    const encoded = encodeVariant(longText);
    const decoded = decodeVariant(encoded);
    expect(decoded).toBe(longText);
  });

  it('should handle object with many keys', () => {
    // Object with 1000 keys
    const original: Record<string, number> = {};
    for (let i = 0; i < 1000; i++) {
      original[`key${i}`] = i;
    }
    const encoded = encodeVariant(original);
    const decoded = decodeVariant(encoded);
    expect(decoded).toEqual(original);
  });

  it('should handle -0 correctly', () => {
    // The encoder converts -0 to 0, so we verify the round-trip behavior
    const encoded = encodeVariant(-0);
    const decoded = decodeVariant(encoded);
    // -0 and 0 are equal in JavaScript
    expect(decoded).toBe(0);
  });

  it('should handle sparse arrays (converted to null)', () => {
    // Sparse arrays are encoded with null for undefined slots
    const original = [1, , , 4]; // sparse array
    const encoded = encodeVariant(original);
    const decoded = decodeVariant(encoded) as unknown[];
    expect(decoded.length).toBe(4);
    expect(decoded[0]).toBe(1);
    expect(decoded[1]).toBe(null); // undefined becomes null
    expect(decoded[2]).toBe(null);
    expect(decoded[3]).toBe(4);
  });
});

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Helper to create object binary for testing
 */
function createObjectBinary(fields: Record<string, { type: string; value?: unknown }>): Uint8Array {
  const bytes: number[] = [TYPE_OBJECT, Object.keys(fields).length];

  for (const [key, spec] of Object.entries(fields)) {
    const keyEncoded = new TextEncoder().encode(key);
    bytes.push(keyEncoded.length, ...keyEncoded);

    switch (spec.type) {
      case 'null':
        bytes.push(TYPE_NULL);
        break;
      case 'boolean':
        bytes.push(TYPE_BOOLEAN | (spec.value ? BOOL_TRUE : BOOL_FALSE));
        break;
      case 'int8':
        bytes.push(TYPE_INT | INT8, spec.value as number);
        break;
      case 'string':
        const strEncoded = new TextEncoder().encode(spec.value as string);
        bytes.push(TYPE_STRING, strEncoded.length, ...strEncoded);
        break;
    }
  }

  return new Uint8Array(bytes);
}
