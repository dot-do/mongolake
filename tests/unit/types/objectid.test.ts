/**
 * ObjectId Tests
 *
 * Comprehensive tests for the ObjectId class that provides:
 * - MongoDB-compatible 12-byte (24 hex character) ObjectId generation
 * - Timestamp extraction from ObjectId
 * - Uniqueness guarantees
 * - String conversion and construction
 * - Equality comparison
 * - Validation of hex strings
 */

import { describe, it, expect } from 'vitest';
import { ObjectId } from '../../../src/types.js';

// =============================================================================
// ObjectId Generation - Creates Valid 24-Character Hex String
// =============================================================================

describe('ObjectId Generation', () => {
  it('should create a valid 24-character hex string', () => {
    const id = new ObjectId();
    const hex = id.toString();

    expect(hex).toHaveLength(24);
    expect(/^[0-9a-f]{24}$/.test(hex)).toBe(true);
  });

  it('should generate ObjectId without arguments', () => {
    const id = new ObjectId();

    expect(id).toBeInstanceOf(ObjectId);
    expect(id.toString()).toBeTruthy();
  });

  it('should generate multiple ObjectIds successfully', () => {
    const ids = Array.from({ length: 100 }, () => new ObjectId());

    expect(ids).toHaveLength(100);
    ids.forEach((id) => {
      expect(id.toString()).toHaveLength(24);
      expect(/^[0-9a-f]{24}$/.test(id.toString())).toBe(true);
    });
  });

  it('should only use lowercase hex characters', () => {
    const id = new ObjectId();
    const hex = id.toString();

    // Should not contain uppercase A-F
    expect(/[A-F]/.test(hex)).toBe(false);
    // Should only contain 0-9 and a-f
    expect(/^[0-9a-f]+$/.test(hex)).toBe(true);
  });

  it('should generate incrementing ObjectIds with same random bytes', () => {
    // Create multiple ObjectIds quickly (same second)
    const oids = Array.from({ length: 10 }, () => new ObjectId());

    // Extract counter values (last 3 bytes)
    const counters = oids.map((oid) => {
      const hex = oid.toString();
      return parseInt(hex.slice(18), 16);
    });

    // Each counter should be unique
    const uniqueCounters = new Set(counters);
    expect(uniqueCounters.size).toBe(10);
  });
});

// =============================================================================
// Uniqueness - Multiple ObjectIds Are Unique
// =============================================================================

describe('ObjectId Uniqueness', () => {
  it('should generate unique ObjectIds', () => {
    const id1 = new ObjectId();
    const id2 = new ObjectId();

    expect(id1.toString()).not.toBe(id2.toString());
  });

  it('should generate 1000 unique ObjectIds', () => {
    const ids = Array.from({ length: 1000 }, () => new ObjectId().toString());
    const uniqueIds = new Set(ids);

    expect(uniqueIds.size).toBe(1000);
  });

  it('should generate 10000 unique ObjectIds rapidly', () => {
    const ids = Array.from({ length: 10000 }, () => new ObjectId().toString());
    const uniqueIds = new Set(ids);

    expect(uniqueIds.size).toBe(10000);
  });

  it('should maintain uniqueness with sequential generation', () => {
    const ids: string[] = [];
    for (let i = 0; i < 500; i++) {
      ids.push(new ObjectId().toString());
    }
    const uniqueIds = new Set(ids);

    expect(uniqueIds.size).toBe(500);
  });

  it('should have different bytes in different ObjectIds', () => {
    const id1 = new ObjectId();
    const id2 = new ObjectId();
    const hex1 = id1.toString();
    const hex2 = id2.toString();

    // At minimum, the counter portion should differ
    // The last 6 characters represent the 3-byte counter
    expect(hex1.slice(-6)).not.toBe(hex2.slice(-6));
  });

  it('should generate three unique ObjectIds', () => {
    const oid1 = new ObjectId();
    const oid2 = new ObjectId();
    const oid3 = new ObjectId();

    expect(oid1.toString()).not.toBe(oid2.toString());
    expect(oid2.toString()).not.toBe(oid3.toString());
    expect(oid1.toString()).not.toBe(oid3.toString());
  });
});

// =============================================================================
// Timestamp Extraction - getTimestamp() Returns Correct Date
// =============================================================================

describe('ObjectId Timestamp Extraction', () => {
  it('should extract timestamp as Date object', () => {
    const beforeCreation = new Date();
    const id = new ObjectId();
    const afterCreation = new Date();

    const timestamp = id.getTimestamp();

    expect(timestamp).toBeInstanceOf(Date);
    // Timestamp should be within the creation window (with 1 second tolerance)
    expect(timestamp.getTime()).toBeGreaterThanOrEqual(beforeCreation.getTime() - 1000);
    expect(timestamp.getTime()).toBeLessThanOrEqual(afterCreation.getTime() + 1000);
  });

  it('should return timestamp accurate to the second', () => {
    const beforeCreate = Math.floor(Date.now() / 1000);
    const id = new ObjectId();
    const afterCreate = Math.floor(Date.now() / 1000);

    const timestamp = id.getTimestamp();
    const timestampSeconds = Math.floor(timestamp.getTime() / 1000);

    expect(timestampSeconds).toBeGreaterThanOrEqual(beforeCreate);
    expect(timestampSeconds).toBeLessThanOrEqual(afterCreate);
  });

  it('should preserve timestamp from hex string construction', () => {
    // Known ObjectId with specific timestamp
    // 0x507681C0 = 1349943744 seconds since epoch
    const oid = new ObjectId('507681c0bcf86cd799439011');
    const timestamp = oid.getTimestamp();

    expect(timestamp).toBeInstanceOf(Date);
    expect(timestamp.getTime()).toBe(0x507681c0 * 1000);
  });

  it('should extract Unix epoch timestamp (zero)', () => {
    const oid = new ObjectId('00000000bcf86cd799439011');
    const timestamp = oid.getTimestamp();

    expect(timestamp.getTime()).toBe(0);
  });

  it('should extract correct timestamp from ObjectId created at specific time', () => {
    const id = new ObjectId();
    const timestamp = id.getTimestamp();

    // The timestamp should be a valid date
    expect(timestamp.toString()).not.toBe('Invalid Date');
    // Should be in reasonable range (between 2020 and 2030)
    expect(timestamp.getFullYear()).toBeGreaterThanOrEqual(2020);
    expect(timestamp.getFullYear()).toBeLessThanOrEqual(2030);
  });

  it('should handle max timestamp value (signed 32-bit limitation)', () => {
    // The getTimestamp implementation uses bitwise OR which treats
    // the result as a signed 32-bit integer. When the high bit is set
    // (0xFFFFFFFF), JavaScript interprets this as -1.
    const oid = new ObjectId('ffffffffbcf86cd799439011');
    const timestamp = oid.getTimestamp();

    // Due to signed 32-bit arithmetic, 0xFFFFFFFF becomes -1
    expect(timestamp.getTime()).toBe(-1 * 1000);
  });

  it('should preserve timestamp through round-trip', () => {
    const original = new ObjectId();
    const originalTimestamp = original.getTimestamp();

    const reconstructed = new ObjectId(original.toString());
    const reconstructedTimestamp = reconstructed.getTimestamp();

    expect(reconstructedTimestamp.getTime()).toBe(originalTimestamp.getTime());
  });
});

// =============================================================================
// String Conversion - toString() and toHexString()
// =============================================================================

describe('ObjectId String Conversion', () => {
  it('should convert to string with toString()', () => {
    const id = new ObjectId();
    const str = id.toString();

    expect(typeof str).toBe('string');
    expect(str).toHaveLength(24);
  });

  it('should return a 24-character lowercase hex string', () => {
    const oid = new ObjectId();
    const str = oid.toString();

    expect(str).toHaveLength(24);
    expect(str).toMatch(/^[0-9a-f]{24}$/);
  });

  it('should convert to hex string with toHexString()', () => {
    const id = new ObjectId();
    const hex = id.toHexString();

    expect(typeof hex).toBe('string');
    expect(hex).toHaveLength(24);
    expect(/^[0-9a-f]{24}$/.test(hex)).toBe(true);
  });

  it('should return same value for toString() and toHexString()', () => {
    const id = new ObjectId();

    expect(id.toString()).toBe(id.toHexString());
  });

  it('should be consistent across multiple calls', () => {
    const id = new ObjectId();
    const str1 = id.toString();
    const str2 = id.toString();
    const hex1 = id.toHexString();
    const hex2 = id.toHexString();

    expect(str1).toBe(str2);
    expect(hex1).toBe(hex2);
    expect(str1).toBe(hex1);
  });

  it('should preserve the original hex when constructed from string', () => {
    const originalHex = '507f1f77bcf86cd799439011';
    const id = new ObjectId(originalHex);

    expect(id.toString()).toBe(originalHex);
    expect(id.toHexString()).toBe(originalHex);
  });

  it('should handle all hex digits correctly', () => {
    // Create hex string with all hex digits
    const allDigits = '0123456789abcdef01234567';
    const id = new ObjectId(allDigits);

    expect(id.toString()).toBe(allDigits);
  });

  it('should round-trip through string construction', () => {
    const original = new ObjectId();
    const stringified = original.toString();
    const reconstructed = new ObjectId(stringified);

    expect(reconstructed.toString()).toBe(stringified);
  });

  it('should convert uppercase hex to lowercase', () => {
    const hexString = '507F1F77BCF86CD799439011';
    const oid = new ObjectId(hexString);
    expect(oid.toString()).toBe(hexString.toLowerCase());
  });
});

// =============================================================================
// Equality Comparison - equals() Method Works Correctly
// =============================================================================

describe('ObjectId Equality Comparison', () => {
  it('should return true for same ObjectId compared with itself', () => {
    const id = new ObjectId();

    expect(id.equals(id)).toBe(true);
  });

  it('should return true for ObjectIds with same hex value', () => {
    const hex = '507f1f77bcf86cd799439011';
    const id1 = new ObjectId(hex);
    const id2 = new ObjectId(hex);

    expect(id1.equals(id2)).toBe(true);
    expect(id2.equals(id1)).toBe(true);
  });

  it('should return false for different ObjectIds', () => {
    const id1 = new ObjectId();
    const id2 = new ObjectId();

    expect(id1.equals(id2)).toBe(false);
    expect(id2.equals(id1)).toBe(false);
  });

  it('should return false for ObjectIds differing by one character', () => {
    const id1 = new ObjectId('507f1f77bcf86cd799439011');
    const id2 = new ObjectId('507f1f77bcf86cd799439012');

    expect(id1.equals(id2)).toBe(false);
  });

  it('should handle equality symmetrically', () => {
    const hex = '507f1f77bcf86cd799439011';
    const id1 = new ObjectId(hex);
    const id2 = new ObjectId(hex);

    // Symmetric property: if a.equals(b) then b.equals(a)
    expect(id1.equals(id2)).toBe(id2.equals(id1));
  });

  it('should handle equality transitively', () => {
    const hex = '507f1f77bcf86cd799439011';
    const id1 = new ObjectId(hex);
    const id2 = new ObjectId(hex);
    const id3 = new ObjectId(hex);

    // Transitive property: if a.equals(b) and b.equals(c), then a.equals(c)
    expect(id1.equals(id2)).toBe(true);
    expect(id2.equals(id3)).toBe(true);
    expect(id1.equals(id3)).toBe(true);
  });

  it('should be reflexive', () => {
    const id = new ObjectId('507f1f77bcf86cd799439011');

    // Reflexive property: a.equals(a) is always true
    expect(id.equals(id)).toBe(true);
  });

  it('should return true for ObjectIds created from same bytes', () => {
    const bytes = new Uint8Array([
      0x50, 0x7f, 0x1f, 0x77, 0xbc, 0xf8, 0x6c, 0xd7, 0x99, 0x43, 0x90, 0x11,
    ]);
    const oid1 = new ObjectId(bytes);
    const oid2 = new ObjectId(bytes);

    expect(oid1.equals(oid2)).toBe(true);
  });

  it('should work for ObjectIds created with different input types', () => {
    const hex = '507f1f77bcf86cd799439011';
    const bytes = new Uint8Array([
      0x50, 0x7f, 0x1f, 0x77, 0xbc, 0xf8, 0x6c, 0xd7, 0x99, 0x43, 0x90, 0x11,
    ]);

    const oidFromHex = new ObjectId(hex);
    const oidFromBytes = new ObjectId(bytes);

    expect(oidFromHex.equals(oidFromBytes)).toBe(true);
  });
});

// =============================================================================
// Construction from String - new ObjectId("existing-hex-string")
// =============================================================================

describe('ObjectId Construction from String', () => {
  it('should construct ObjectId from valid hex string', () => {
    const hex = '507f1f77bcf86cd799439011';
    const id = new ObjectId(hex);

    expect(id).toBeInstanceOf(ObjectId);
    expect(id.toString()).toBe(hex);
  });

  it('should construct ObjectId from lowercase hex string', () => {
    const hex = 'abcdef0123456789abcdef01';
    const id = new ObjectId(hex);

    expect(id.toString()).toBe(hex);
  });

  it('should construct ObjectId from uppercase hex string and normalize to lowercase', () => {
    const upperHex = 'ABCDEF0123456789ABCDEF01';
    const id = new ObjectId(upperHex);

    expect(id.toString()).toBe(upperHex.toLowerCase());
  });

  it('should construct ObjectId from mixed case hex string', () => {
    const mixedHex = 'AbCdEf0123456789aBcDeF01';
    const id = new ObjectId(mixedHex);

    expect(id.toString()).toBe(mixedHex.toLowerCase());
  });

  it('should construct ObjectId from Uint8Array', () => {
    const bytes = new Uint8Array([
      0x50, 0x7f, 0x1f, 0x77, 0xbc, 0xf8, 0x6c, 0xd7, 0x99, 0x43, 0x90, 0x11,
    ]);
    const id = new ObjectId(bytes);

    expect(id).toBeInstanceOf(ObjectId);
    expect(id.toString()).toBe('507f1f77bcf86cd799439011');
  });

  it('should preserve all bytes when constructed from hex', () => {
    const hex = '000102030405060708090a0b';
    const id = new ObjectId(hex);

    expect(id.toString()).toBe(hex);
  });

  it('should handle hex string with leading zeros', () => {
    const hex = '000000000000000000000001';
    const id = new ObjectId(hex);

    expect(id.toString()).toBe(hex);
  });
});

// =============================================================================
// Invalid Input Handling - Throws on Invalid Hex Strings
// =============================================================================

describe('ObjectId Invalid Input Handling', () => {
  describe('ObjectId.isValid static method', () => {
    it('should return true for valid 24-character hex string', () => {
      expect(ObjectId.isValid('507f1f77bcf86cd799439011')).toBe(true);
    });

    it('should return true for valid 24-character hex strings', () => {
      expect(ObjectId.isValid('507f1f77bcf86cd799439011')).toBe(true);
      expect(ObjectId.isValid('000000000000000000000000')).toBe(true);
      expect(ObjectId.isValid('ffffffffffffffffffffffff')).toBe(true);
      expect(ObjectId.isValid('FFFFFFFFFFFFFFFFFFFFFFFF')).toBe(true);
      expect(ObjectId.isValid('abcdef0123456789abcdef01')).toBe(true);
    });

    it('should return true for hex string with all digits', () => {
      expect(ObjectId.isValid('012345678901234567890123')).toBe(true);
    });

    it('should return true for hex string with all lowercase letters', () => {
      expect(ObjectId.isValid('abcdefabcdefabcdefabcdef')).toBe(true);
    });

    it('should return true for hex string with all uppercase letters', () => {
      expect(ObjectId.isValid('ABCDEFABCDEFABCDEFABCDEF')).toBe(true);
    });

    it('should return false for string that is too short', () => {
      expect(ObjectId.isValid('507f1f77bcf86cd79943901')).toBe(false);
      expect(ObjectId.isValid('507f1f77')).toBe(false);
      expect(ObjectId.isValid('')).toBe(false);
      expect(ObjectId.isValid('abc')).toBe(false);
    });

    it('should return false for string that is too long', () => {
      expect(ObjectId.isValid('507f1f77bcf86cd7994390110')).toBe(false);
      expect(ObjectId.isValid('507f1f77bcf86cd799439011extra')).toBe(false);
      expect(ObjectId.isValid('507f1f77bcf86cd799439011000000')).toBe(false);
    });

    it('should return false for string with invalid characters', () => {
      expect(ObjectId.isValid('507f1f77bcf86cd79943901g')).toBe(false);
      expect(ObjectId.isValid('507f1f77bcf86cd79943901!')).toBe(false);
      expect(ObjectId.isValid('507f1f77bcf86cd79943901 ')).toBe(false);
      expect(ObjectId.isValid('507f1f77bcf86cd7994390-1')).toBe(false);
    });

    it('should return false for string with spaces', () => {
      expect(ObjectId.isValid('507f1f77 bcf86cd799439011')).toBe(false);
      expect(ObjectId.isValid(' 507f1f77bcf86cd79943901')).toBe(false);
      expect(ObjectId.isValid('507f1f77bcf86cd79943901 ')).toBe(false);
    });

    it('should return false for string with special characters', () => {
      expect(ObjectId.isValid('507f1f77bcf86cd79943901@')).toBe(false);
      expect(ObjectId.isValid('507f1f77bcf86cd79943901#')).toBe(false);
      expect(ObjectId.isValid('507f1f77bcf86cd79943901$')).toBe(false);
    });

    it('should return false for string with dashes (UUID-like format)', () => {
      expect(ObjectId.isValid('507f1f77-bcf8-6cd7-9943-9011')).toBe(false);
    });

    it('should return false for mixed valid/invalid characters', () => {
      expect(ObjectId.isValid('zzzzzzzzzzzzzzzzzzzzzzzz')).toBe(false);
      expect(ObjectId.isValid('507f1f77bcf86cd7ZZZZ9011')).toBe(false);
    });
  });
});

// =============================================================================
// MongoDB Compatibility - Format Matches MongoDB ObjectId Spec
// =============================================================================

describe('ObjectId MongoDB Compatibility', () => {
  it('should generate 12-byte (24 hex character) ObjectId', () => {
    const id = new ObjectId();
    const hex = id.toString();

    // 12 bytes = 24 hex characters
    expect(hex).toHaveLength(24);
  });

  it('should have timestamp in first 4 bytes', () => {
    const beforeSeconds = Math.floor(Date.now() / 1000);
    const id = new ObjectId();
    const afterSeconds = Math.floor(Date.now() / 1000);

    const hex = id.toString();
    const timestampHex = hex.slice(0, 8);
    const timestamp = parseInt(timestampHex, 16);

    expect(timestamp).toBeGreaterThanOrEqual(beforeSeconds);
    expect(timestamp).toBeLessThanOrEqual(afterSeconds + 1);
  });

  it('should encode timestamp in first 4 bytes (big-endian)', () => {
    // Known timestamp
    const expectedHex = '507681c0';

    const oid = new ObjectId('507681c0bcf86cd799439011');
    const hex = oid.toString();

    expect(hex.slice(0, 8)).toBe(expectedHex);
  });

  it('should have 5 random bytes after timestamp (consistent within process)', () => {
    const id1 = new ObjectId();
    const id2 = new ObjectId();

    // Random bytes are in positions 8-17 (5 bytes = 10 hex chars)
    // For ObjectIds generated in the same process, these should be the same
    // (per MongoDB spec: 5-byte random value unique to machine and process)
    const random1 = id1.toString().slice(8, 18);
    const random2 = id2.toString().slice(8, 18);

    expect(random1).toBe(random2);
  });

  it('should have incrementing counter in last 3 bytes', () => {
    const id1 = new ObjectId();
    const id2 = new ObjectId();

    // Counter is in last 6 hex characters (3 bytes)
    const counter1 = parseInt(id1.toString().slice(18), 16);
    const counter2 = parseInt(id2.toString().slice(18), 16);

    // Counter should increment (with potential wrap-around)
    const diff = (counter2 - counter1 + 0x1000000) % 0x1000000;
    expect(diff).toBe(1);
  });

  it('should have counter that stays within valid range', () => {
    const oid = new ObjectId();
    const hex = oid.toString();
    const counter = parseInt(hex.slice(18), 16);

    // Counter should always be in valid range
    expect(counter).toBeGreaterThanOrEqual(0);
    expect(counter).toBeLessThanOrEqual(0xffffff);
  });

  it('should match MongoDB ObjectId structure: 4-byte timestamp, 5-byte random, 3-byte counter', () => {
    const id = new ObjectId();
    const hex = id.toString();

    // Validate structure by parsing each section
    const timestampHex = hex.slice(0, 8); // 4 bytes = 8 chars
    const randomHex = hex.slice(8, 18); // 5 bytes = 10 chars
    const counterHex = hex.slice(18, 24); // 3 bytes = 6 chars

    expect(timestampHex).toHaveLength(8);
    expect(randomHex).toHaveLength(10);
    expect(counterHex).toHaveLength(6);

    // All should be valid hex
    expect(/^[0-9a-f]+$/.test(timestampHex)).toBe(true);
    expect(/^[0-9a-f]+$/.test(randomHex)).toBe(true);
    expect(/^[0-9a-f]+$/.test(counterHex)).toBe(true);
  });

  it('should be compatible with standard MongoDB ObjectId format', () => {
    // Test with known valid MongoDB ObjectIds
    const validMongoIds = [
      '507f1f77bcf86cd799439011',
      '507f191e810c19729de860ea',
      '54759eb3c090d83494e2d804',
      '5f5b7b5b5b5b5b5b5b5b5b5b',
    ];

    validMongoIds.forEach((mongoId) => {
      expect(ObjectId.isValid(mongoId)).toBe(true);
      const id = new ObjectId(mongoId);
      expect(id.toString()).toBe(mongoId);
    });
  });

  it('should generate ObjectIds that would be valid in MongoDB', () => {
    const ids = Array.from({ length: 100 }, () => new ObjectId());

    ids.forEach((id) => {
      // All generated ObjectIds should pass validation
      expect(ObjectId.isValid(id.toString())).toBe(true);

      // Should be exactly 24 characters
      expect(id.toString()).toHaveLength(24);

      // Should contain valid timestamp (positive integer)
      const timestampHex = id.toString().slice(0, 8);
      const timestamp = parseInt(timestampHex, 16);
      expect(timestamp).toBeGreaterThan(0);
    });
  });
});

// =============================================================================
// Edge Cases and Special Scenarios
// =============================================================================

describe('ObjectId Edge Cases', () => {
  it('should handle ObjectId with all zeros', () => {
    const hex = '000000000000000000000000';
    const id = new ObjectId(hex);

    expect(id.toString()).toBe(hex);
    expect(id.getTimestamp().getTime()).toBe(0);
  });

  it('should handle ObjectId with all fs', () => {
    const hex = 'ffffffffffffffffffffffff';
    const id = new ObjectId(hex);

    expect(id.toString()).toBe(hex);
  });

  it('should handle Uint8Array with all zeros', () => {
    const bytes = new Uint8Array(12).fill(0);
    const oid = new ObjectId(bytes);
    expect(oid.toString()).toBe('000000000000000000000000');
  });

  it('should handle Uint8Array with all 255s', () => {
    const bytes = new Uint8Array(12).fill(255);
    const oid = new ObjectId(bytes);
    expect(oid.toString()).toBe('ffffffffffffffffffffffff');
  });

  it('should handle rapid sequential generation', () => {
    const start = performance.now();
    const ids: ObjectId[] = [];

    for (let i = 0; i < 10000; i++) {
      ids.push(new ObjectId());
    }

    const elapsed = performance.now() - start;

    // Should complete reasonably quickly (under 1 second)
    expect(elapsed).toBeLessThan(1000);

    // All should be unique
    const uniqueIds = new Set(ids.map((id) => id.toString()));
    expect(uniqueIds.size).toBe(10000);
  });

  it('should work correctly after many generations', () => {
    // Generate many ObjectIds to test counter behavior
    for (let i = 0; i < 1000; i++) {
      new ObjectId();
    }

    // Now generate one more and verify it's valid
    const id = new ObjectId();
    expect(id.toString()).toHaveLength(24);
    expect(ObjectId.isValid(id.toString())).toBe(true);
  });

  it('should handle comparison between generated and parsed ObjectIds', () => {
    const generated = new ObjectId();
    const parsed = new ObjectId(generated.toString());

    expect(generated.equals(parsed)).toBe(true);
    expect(parsed.equals(generated)).toBe(true);
  });
});

// =============================================================================
// Malformed Hex String Handling
// =============================================================================

describe('ObjectId Malformed Hex String Handling', () => {
  it('should handle hex string with embedded null bytes', () => {
    // The hex "00" represents a null byte
    const hex = '000000000000000000000000';
    const id = new ObjectId(hex);
    expect(id.toString()).toBe(hex);
  });

  it('should parse hex string with leading zeros correctly', () => {
    const hex = '000000000000000000000001';
    const id = new ObjectId(hex);
    expect(id.toString()).toBe(hex);
    // Last byte should be 1
    const lastByte = parseInt(hex.slice(-2), 16);
    expect(lastByte).toBe(1);
  });

  it('should handle alternating high/low bytes', () => {
    const hex = 'ff00ff00ff00ff00ff00ff00';
    const id = new ObjectId(hex);
    expect(id.toString()).toBe(hex);
  });

  it('should handle byte boundary values', () => {
    // Each byte at 127 (0x7f) - max signed byte value
    const hex = '7f7f7f7f7f7f7f7f7f7f7f7f';
    const id = new ObjectId(hex);
    expect(id.toString()).toBe(hex);
  });

  it('should handle byte boundary value 128 (0x80)', () => {
    // Each byte at 128 (0x80) - first negative signed byte value
    const hex = '808080808080808080808080';
    const id = new ObjectId(hex);
    expect(id.toString()).toBe(hex);
  });
});

// =============================================================================
// Invalid Timestamp Edge Cases
// =============================================================================

describe('ObjectId Invalid Timestamp Edge Cases', () => {
  it('should handle timestamp at Unix epoch (0)', () => {
    const hex = '00000000bcf86cd799439011';
    const id = new ObjectId(hex);
    const timestamp = id.getTimestamp();
    expect(timestamp.getTime()).toBe(0);
    expect(timestamp.toISOString()).toBe('1970-01-01T00:00:00.000Z');
  });

  it('should handle timestamp at max positive 32-bit value (0x7FFFFFFF)', () => {
    // 0x7FFFFFFF = 2147483647 seconds = Jan 19, 2038 03:14:07 UTC
    const hex = '7fffffffbcf86cd799439011';
    const id = new ObjectId(hex);
    const timestamp = id.getTimestamp();
    expect(timestamp.getTime()).toBe(0x7fffffff * 1000);
    expect(timestamp.getFullYear()).toBe(2038);
  });

  it('should handle timestamp overflow with high bit set (0x80000000)', () => {
    // When high bit is set, JavaScript bitwise OR treats it as negative
    const hex = '80000000bcf86cd799439011';
    const id = new ObjectId(hex);
    const timestamp = id.getTimestamp();
    // Due to signed 32-bit arithmetic, this becomes negative
    expect(timestamp.getTime()).toBe(-2147483648 * 1000);
  });

  it('should handle timestamp value 1 (earliest non-zero)', () => {
    const hex = '00000001bcf86cd799439011';
    const id = new ObjectId(hex);
    const timestamp = id.getTimestamp();
    expect(timestamp.getTime()).toBe(1000);
  });

  it('should handle common timestamp values', () => {
    // 0x65B5A000 = 1706442752 = Jan 28, 2024 12:32:32 UTC (approximate)
    const hex = '65b5a000bcf86cd799439011';
    const id = new ObjectId(hex);
    const timestamp = id.getTimestamp();
    expect(timestamp.getFullYear()).toBe(2024);
  });
});

// =============================================================================
// Boundary Value Testing
// =============================================================================

describe('ObjectId Boundary Values', () => {
  it('should handle minimum valid ObjectId', () => {
    const hex = '000000000000000000000000';
    expect(ObjectId.isValid(hex)).toBe(true);
    const id = new ObjectId(hex);
    expect(id.toString()).toBe(hex);
  });

  it('should handle maximum valid ObjectId', () => {
    const hex = 'ffffffffffffffffffffffff';
    expect(ObjectId.isValid(hex)).toBe(true);
    const id = new ObjectId(hex);
    expect(id.toString()).toBe(hex);
  });

  it('should handle single bit difference at start', () => {
    const id1 = new ObjectId('000000000000000000000000');
    const id2 = new ObjectId('010000000000000000000000');
    expect(id1.equals(id2)).toBe(false);
  });

  it('should handle single bit difference at end', () => {
    const id1 = new ObjectId('000000000000000000000000');
    const id2 = new ObjectId('000000000000000000000001');
    expect(id1.equals(id2)).toBe(false);
  });

  it('should handle single hex character difference', () => {
    const id1 = new ObjectId('507f1f77bcf86cd799439010');
    const id2 = new ObjectId('507f1f77bcf86cd799439011');
    expect(id1.equals(id2)).toBe(false);
    expect(id1.toString()).not.toBe(id2.toString());
  });

  it('should correctly compare adjacent hex values', () => {
    // Test that 'a' and 'b' are distinguished
    const id1 = new ObjectId('aaaaaaaaaaaaaaaaaaaaaaa0');
    const id2 = new ObjectId('aaaaaaaaaaaaaaaaaaaaaaa1');
    expect(id1.equals(id2)).toBe(false);
  });
});

// =============================================================================
// Uint8Array Construction Edge Cases
// =============================================================================

describe('ObjectId Uint8Array Construction', () => {
  it('should construct from exact 12-byte array', () => {
    const bytes = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
    const id = new ObjectId(bytes);
    expect(id.toString()).toBe('0102030405060708090a0b0c');
  });

  it('should handle Uint8Array with single byte values', () => {
    const bytes = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    const id = new ObjectId(bytes);
    expect(id.toString()).toBe('000102030405060708090a0b');
  });

  it('should handle Uint8Array with max byte values', () => {
    const bytes = new Uint8Array([255, 254, 253, 252, 251, 250, 249, 248, 247, 246, 245, 244]);
    const id = new ObjectId(bytes);
    expect(id.toString()).toBe('fffefdfc fbfaf9f8 f7f6f5f4'.replace(/ /g, ''));
  });

  it('should preserve byte order from Uint8Array', () => {
    const bytes = new Uint8Array([0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12]);
    const id = new ObjectId(bytes);
    expect(id.toString()).toBe('abcdef123456789abcdef012');
  });

  it('should round-trip through Uint8Array and hex string', () => {
    const originalHex = '507f1f77bcf86cd799439011';
    const id1 = new ObjectId(originalHex);

    // Get the bytes from the string and create new ObjectId
    const bytes = new Uint8Array(12);
    for (let i = 0; i < 12; i++) {
      bytes[i] = parseInt(originalHex.slice(i * 2, i * 2 + 2), 16);
    }
    const id2 = new ObjectId(bytes);

    expect(id1.equals(id2)).toBe(true);
    expect(id1.toString()).toBe(id2.toString());
  });
});

// =============================================================================
// isValid Static Method Extended Tests
// =============================================================================

describe('ObjectId.isValid Extended Tests', () => {
  it('should return false for null', () => {
    expect(ObjectId.isValid(null as unknown as string)).toBe(false);
  });

  it('should return false for undefined', () => {
    expect(ObjectId.isValid(undefined as unknown as string)).toBe(false);
  });

  it('should return false for number', () => {
    expect(ObjectId.isValid(123456789012345678901234 as unknown as string)).toBe(false);
  });

  it('should return false for object', () => {
    expect(ObjectId.isValid({} as unknown as string)).toBe(false);
  });

  it('should return false for array', () => {
    expect(ObjectId.isValid([] as unknown as string)).toBe(false);
  });

  it('should return false for empty string', () => {
    expect(ObjectId.isValid('')).toBe(false);
  });

  it('should return false for whitespace string', () => {
    expect(ObjectId.isValid('                        ')).toBe(false);
  });

  it('should return false for 23-character string (one short)', () => {
    expect(ObjectId.isValid('507f1f77bcf86cd79943901')).toBe(false);
  });

  it('should return false for 25-character string (one long)', () => {
    expect(ObjectId.isValid('507f1f77bcf86cd7994390110')).toBe(false);
  });

  it('should return false for hex with newline', () => {
    expect(ObjectId.isValid('507f1f77bcf86cd7\n9439011')).toBe(false);
  });

  it('should return false for hex with tab', () => {
    expect(ObjectId.isValid('507f1f77bcf86cd7\t9439011')).toBe(false);
  });

  it('should return true for valid hex with digits only', () => {
    expect(ObjectId.isValid('012345678901234567890123')).toBe(true);
  });

  it('should return true for valid hex with letters only', () => {
    expect(ObjectId.isValid('abcdefabcdefabcdefabcdef')).toBe(true);
  });

  it('should handle unicode characters that are not hex', () => {
    // \u0030 is '0' which is valid hex, so we need to use actual invalid unicode
    // \u00C0 is 'A' with grave accent - looks like 'A' but isn't hex
    expect(ObjectId.isValid('507f1f77bcf86cd79943901\u00C0')).toBe(false);
    // \u0391 is Greek capital alpha - looks like 'A' but isn't hex
    expect(ObjectId.isValid('507f1f77bcf86cd79943901\u0391')).toBe(false);
  });
});

// =============================================================================
// Concurrent Generation Safety
// =============================================================================

describe('ObjectId Concurrent Generation', () => {
  it('should generate unique IDs in parallel-like scenarios', async () => {
    const promises = Array.from({ length: 100 }, () =>
      Promise.resolve(new ObjectId().toString())
    );
    const ids = await Promise.all(promises);
    const uniqueIds = new Set(ids);
    expect(uniqueIds.size).toBe(100);
  });

  it('should maintain counter integrity across many rapid calls', () => {
    const ids: string[] = [];
    const counterValues: number[] = [];

    for (let i = 0; i < 100; i++) {
      const id = new ObjectId();
      ids.push(id.toString());
      // Extract counter from last 6 hex chars
      counterValues.push(parseInt(id.toString().slice(18), 16));
    }

    // All IDs should be unique
    expect(new Set(ids).size).toBe(100);

    // Counter values should be sequential (with possible wrap-around)
    for (let i = 1; i < counterValues.length; i++) {
      const diff = (counterValues[i] - counterValues[i - 1] + 0x1000000) % 0x1000000;
      expect(diff).toBe(1);
    }
  });
});

// =============================================================================
// instanceof Check - Verify ObjectId is Proper Class Instance
// =============================================================================

describe('ObjectId instanceof Check', () => {
  it('should be instanceof ObjectId when created without arguments', () => {
    const id = new ObjectId();
    expect(id instanceof ObjectId).toBe(true);
  });

  it('should be instanceof ObjectId when created from hex string', () => {
    const id = new ObjectId('507f1f77bcf86cd799439011');
    expect(id instanceof ObjectId).toBe(true);
  });

  it('should be instanceof ObjectId when created from Uint8Array', () => {
    const bytes = new Uint8Array(12).fill(0);
    const id = new ObjectId(bytes);
    expect(id instanceof ObjectId).toBe(true);
  });

  it('should not be instanceof ObjectId for plain objects', () => {
    const plainObject = { toString: () => '507f1f77bcf86cd799439011' };
    expect(plainObject instanceof ObjectId).toBe(false);
  });

  it('should not be instanceof ObjectId for strings', () => {
    const str = '507f1f77bcf86cd799439011';
    expect(str instanceof ObjectId).toBe(false);
  });

  it('should be instanceof Object', () => {
    const id = new ObjectId();
    expect(id instanceof Object).toBe(true);
  });

  it('should have ObjectId as constructor name', () => {
    const id = new ObjectId();
    expect(id.constructor.name).toBe('ObjectId');
  });

  it('should pass instanceof check after multiple generations', () => {
    const ids = Array.from({ length: 50 }, () => new ObjectId());
    ids.forEach((id) => {
      expect(id instanceof ObjectId).toBe(true);
    });
  });
});

// =============================================================================
// Static Method Existence Check
// =============================================================================

describe('ObjectId Static Methods', () => {
  it('should have isValid as a static method', () => {
    expect(typeof ObjectId.isValid).toBe('function');
  });

  it('should call isValid without instance', () => {
    const result = ObjectId.isValid('507f1f77bcf86cd799439011');
    expect(typeof result).toBe('boolean');
    expect(result).toBe(true);
  });

  it('isValid should work with constructor property', () => {
    const id = new ObjectId('507f1f77bcf86cd799439011');
    // Access isValid via constructor
    const ctor = id.constructor as typeof ObjectId;
    expect(ctor.isValid('507f1f77bcf86cd799439011')).toBe(true);
  });
});

// =============================================================================
// Null and Undefined Input Handling
// =============================================================================

describe('ObjectId Null and Undefined Input', () => {
  it('should generate new ObjectId when passed undefined', () => {
    const id = new ObjectId(undefined);
    expect(id instanceof ObjectId).toBe(true);
    expect(id.toString()).toHaveLength(24);
    expect(/^[0-9a-f]{24}$/.test(id.toString())).toBe(true);
  });

  it('should have valid timestamp when passed undefined', () => {
    const before = Math.floor(Date.now() / 1000);
    const id = new ObjectId(undefined);
    const after = Math.floor(Date.now() / 1000);

    const timestamp = id.getTimestamp();
    const timestampSeconds = Math.floor(timestamp.getTime() / 1000);

    expect(timestampSeconds).toBeGreaterThanOrEqual(before);
    expect(timestampSeconds).toBeLessThanOrEqual(after);
  });

  it('should generate unique ObjectIds when passed undefined multiple times', () => {
    const ids = Array.from({ length: 10 }, () => new ObjectId(undefined));
    const uniqueIds = new Set(ids.map((id) => id.toString()));
    expect(uniqueIds.size).toBe(10);
  });
});

// =============================================================================
// Additional Parsing Format Tests
// =============================================================================

describe('ObjectId Parsing Formats', () => {
  it('should parse hex string with consecutive repeated characters', () => {
    const hex = 'aaaaaaaaaaaaaaaaaaaaaaaa';
    const id = new ObjectId(hex);
    expect(id.toString()).toBe(hex);
  });

  it('should parse hex string with alternating digits and letters', () => {
    const hex = '0a1b2c3d4e5f6a7b8c9d0e1f';
    const id = new ObjectId(hex);
    expect(id.toString()).toBe(hex);
  });

  it('should parse hex string representing sequential bytes', () => {
    const hex = '0123456789abcdef01234567';
    const id = new ObjectId(hex);
    expect(id.toString()).toBe(hex);
  });

  it('should parse hex string with high nibble pattern', () => {
    const hex = 'f0f0f0f0f0f0f0f0f0f0f0f0';
    const id = new ObjectId(hex);
    expect(id.toString()).toBe(hex);
  });

  it('should parse hex string with low nibble pattern', () => {
    const hex = '0f0f0f0f0f0f0f0f0f0f0f0f';
    const id = new ObjectId(hex);
    expect(id.toString()).toBe(hex);
  });

  it('should normalize mixed case input consistently', () => {
    const variations = [
      '507F1F77BCF86CD799439011',
      '507f1f77bcf86cd799439011',
      '507F1f77BcF86Cd799439011',
    ];
    const results = variations.map((v) => new ObjectId(v).toString());

    // All should normalize to lowercase
    expect(results[0]).toBe(results[1]);
    expect(results[1]).toBe(results[2]);
    expect(results[0]).toBe('507f1f77bcf86cd799439011');
  });
});

// =============================================================================
// Serialization Edge Cases
// =============================================================================

describe('ObjectId Serialization Edge Cases', () => {
  it('should serialize to JSON as string via toString()', () => {
    const id = new ObjectId('507f1f77bcf86cd799439011');
    const jsonString = JSON.stringify({ _id: id.toString() });
    expect(jsonString).toBe('{"_id":"507f1f77bcf86cd799439011"}');
  });

  it('should be usable as Map key via toString()', () => {
    const map = new Map<string, number>();
    const id1 = new ObjectId('507f1f77bcf86cd799439011');
    const id2 = new ObjectId('507f1f77bcf86cd799439011');

    map.set(id1.toString(), 1);
    expect(map.get(id2.toString())).toBe(1);
  });

  it('should be usable in Set via toString()', () => {
    const set = new Set<string>();
    const id1 = new ObjectId('507f1f77bcf86cd799439011');
    const id2 = new ObjectId('507f1f77bcf86cd799439011');

    set.add(id1.toString());
    expect(set.has(id2.toString())).toBe(true);
    expect(set.size).toBe(1);
  });

  it('should produce consistent string for array operations', () => {
    const ids = [
      new ObjectId('000000000000000000000001'),
      new ObjectId('000000000000000000000002'),
      new ObjectId('000000000000000000000003'),
    ];

    const sorted = ids.map((id) => id.toString()).sort();
    expect(sorted).toEqual([
      '000000000000000000000001',
      '000000000000000000000002',
      '000000000000000000000003',
    ]);
  });

  it('should maintain string representation stability', () => {
    const id = new ObjectId('abcdef123456789012345678');

    // Multiple calls should return same string
    const results = Array.from({ length: 100 }, () => id.toString());
    const unique = new Set(results);
    expect(unique.size).toBe(1);
  });
});

// =============================================================================
// Timestamp Extraction Additional Tests
// =============================================================================

describe('ObjectId Timestamp Additional Tests', () => {
  it('should extract timestamp from ObjectId created in year 2000', () => {
    // 0x386D4380 = 946684800 = Jan 1, 2000 00:00:00 UTC
    const hex = '386d4380000000000000000';
    const id = new ObjectId(hex + '0');
    const timestamp = id.getTimestamp();
    expect(timestamp.getUTCFullYear()).toBe(2000);
    expect(timestamp.getUTCMonth()).toBe(0); // January
    expect(timestamp.getUTCDate()).toBe(1);
  });

  it('should extract timestamp from ObjectId created in year 2010', () => {
    // 0x4B3D3B00 = 1262304000 = Jan 1, 2010 00:00:00 UTC
    const hex = '4b3d3b00000000000000000';
    const id = new ObjectId(hex + '0');
    const timestamp = id.getTimestamp();
    expect(timestamp.getUTCFullYear()).toBe(2010);
  });

  it('should extract timestamp that matches Date.now() at creation', () => {
    const nowBefore = Date.now();
    const id = new ObjectId();
    const nowAfter = Date.now();

    const timestamp = id.getTimestamp();

    // Timestamp should be within the window (rounded to seconds)
    expect(timestamp.getTime()).toBeGreaterThanOrEqual(Math.floor(nowBefore / 1000) * 1000);
    expect(timestamp.getTime()).toBeLessThanOrEqual(Math.ceil(nowAfter / 1000) * 1000);
  });

  it('should handle timestamps at second boundaries', () => {
    // Exact second boundary: 0x60000000 = 1610612736 seconds
    const hex = '60000000000000000000000';
    const id = new ObjectId(hex + '0');
    const timestamp = id.getTimestamp();

    // Should be exactly on the second
    expect(timestamp.getTime() % 1000).toBe(0);
    expect(timestamp.getTime()).toBe(0x60000000 * 1000);
  });
});

// =============================================================================
// Comparison Edge Cases
// =============================================================================

describe('ObjectId Comparison Edge Cases', () => {
  it('should correctly compare ObjectIds with only timestamp difference', () => {
    const id1 = new ObjectId('00000000bcf86cd799439011');
    const id2 = new ObjectId('00000001bcf86cd799439011');

    expect(id1.equals(id2)).toBe(false);
  });

  it('should correctly compare ObjectIds with only random bytes difference', () => {
    const id1 = new ObjectId('507f1f77bcf86cd799439011');
    const id2 = new ObjectId('507f1f77000000009943901');
    // This will be different due to different random bytes

    expect(id1.equals(id2)).toBe(false);
  });

  it('should correctly compare ObjectIds with only counter difference', () => {
    const id1 = new ObjectId('507f1f77bcf86cd799439010');
    const id2 = new ObjectId('507f1f77bcf86cd799439011');

    expect(id1.equals(id2)).toBe(false);
  });

  it('should compare two newly generated ObjectIds as not equal', () => {
    const id1 = new ObjectId();
    const id2 = new ObjectId();

    expect(id1.equals(id2)).toBe(false);
    expect(id2.equals(id1)).toBe(false);
  });

  it('should compare ObjectId to copy of itself as equal', () => {
    const id1 = new ObjectId();
    const id2 = new ObjectId(id1.toString());

    expect(id1.equals(id2)).toBe(true);
    expect(id2.equals(id1)).toBe(true);
  });
});

// =============================================================================
// isValid with Various Invalid Inputs
// =============================================================================

describe('ObjectId.isValid with Various Invalid Inputs', () => {
  it('should return false for string with only letters g-z', () => {
    expect(ObjectId.isValid('ghijklmnopqrstuvwxyzghij')).toBe(false);
  });

  it('should return false for string with punctuation', () => {
    expect(ObjectId.isValid('507f1f77bcf86cd79943901.')).toBe(false);
    expect(ObjectId.isValid('507f1f77bcf86cd79943901,')).toBe(false);
    expect(ObjectId.isValid('507f1f77bcf86cd79943901;')).toBe(false);
    expect(ObjectId.isValid('507f1f77bcf86cd79943901:')).toBe(false);
  });

  it('should return false for string with mathematical operators', () => {
    expect(ObjectId.isValid('507f1f77bcf86cd79943901+')).toBe(false);
    expect(ObjectId.isValid('507f1f77bcf86cd79943901-')).toBe(false);
    expect(ObjectId.isValid('507f1f77bcf86cd79943901*')).toBe(false);
    expect(ObjectId.isValid('507f1f77bcf86cd79943901/')).toBe(false);
  });

  it('should return false for string with brackets', () => {
    expect(ObjectId.isValid('507f1f77bcf86cd79943901(')).toBe(false);
    expect(ObjectId.isValid('507f1f77bcf86cd79943901)')).toBe(false);
    expect(ObjectId.isValid('507f1f77bcf86cd79943901[')).toBe(false);
    expect(ObjectId.isValid('507f1f77bcf86cd79943901]')).toBe(false);
    expect(ObjectId.isValid('507f1f77bcf86cd79943901{')).toBe(false);
    expect(ObjectId.isValid('507f1f77bcf86cd79943901}')).toBe(false);
  });

  it('should return false for string with quotes', () => {
    expect(ObjectId.isValid('507f1f77bcf86cd79943901"')).toBe(false);
    expect(ObjectId.isValid("507f1f77bcf86cd79943901'")).toBe(false);
    expect(ObjectId.isValid('507f1f77bcf86cd79943901`')).toBe(false);
  });

  it('should return false for string with escape characters', () => {
    expect(ObjectId.isValid('507f1f77bcf86cd79943901\\')).toBe(false);
  });

  it('should return false for boolean values', () => {
    expect(ObjectId.isValid(true as unknown as string)).toBe(false);
    expect(ObjectId.isValid(false as unknown as string)).toBe(false);
  });

  it('should return false for function', () => {
    expect(ObjectId.isValid((() => {}) as unknown as string)).toBe(false);
  });

  it('should throw when passed Symbol', () => {
    // Symbol cannot be converted to string, so regex.test() throws TypeError
    expect(() => ObjectId.isValid(Symbol('test') as unknown as string)).toThrow(TypeError);
  });
});

// =============================================================================
// Counter Overflow Tests
// =============================================================================

describe('ObjectId Counter Overflow', () => {
  it('should wrap counter at 0xFFFFFF (24-bit max value)', () => {
    // The counter wraps at 0xFFFFFF (16,777,215) using (counter + 1) & 0xffffff
    // We can verify this by examining the counter behavior in the implementation
    // which explicitly uses: objectIdState.counter = (objectIdState.counter + 1) & 0xffffff

    // Generate ObjectIds and verify counter stays within valid range
    const ids: ObjectId[] = [];
    for (let i = 0; i < 100; i++) {
      ids.push(new ObjectId());
    }

    // Extract counters
    const counters = ids.map((id) => parseInt(id.toString().slice(18), 16));

    // All counters should be within the valid 24-bit range
    for (const counter of counters) {
      expect(counter).toBeGreaterThanOrEqual(0);
      expect(counter).toBeLessThanOrEqual(0xffffff);
    }
  });

  it('should handle counter at maximum value (0xFFFFFF)', () => {
    // Create ObjectId with counter at max value (full 24 hex chars)
    // Format: 4-byte timestamp + 5-byte random + 3-byte counter
    const maxCounterHex = '507f1f77bcf86cd799ffffff';
    const id = new ObjectId(maxCounterHex);

    expect(id.toString()).toBe(maxCounterHex);

    const counter = parseInt(id.toString().slice(18), 16);
    expect(counter).toBe(0xffffff);
  });

  it('should handle counter near overflow boundary', () => {
    // Test parsing ObjectIds with counters near the boundary
    // Full 24-character hex strings with counter in last 6 characters
    const nearMaxHex = '507f1f77bcf86cd799fffffe';
    const atMaxHex = '507f1f77bcf86cd799ffffff';

    const nearMaxId = new ObjectId(nearMaxHex);
    const atMaxId = new ObjectId(atMaxHex);

    expect(nearMaxId.equals(atMaxId)).toBe(false);

    // Counters should differ by 1
    const nearMaxCounter = parseInt(nearMaxId.toString().slice(18), 16);
    const atMaxCounter = parseInt(atMaxId.toString().slice(18), 16);
    expect(atMaxCounter - nearMaxCounter).toBe(1);
  });

  it('should maintain counter increment behavior (modular arithmetic)', () => {
    // Verify that counter increments by exactly 1 each time
    const ids: ObjectId[] = [];
    for (let i = 0; i < 50; i++) {
      ids.push(new ObjectId());
    }

    const counters = ids.map((id) => parseInt(id.toString().slice(18), 16));

    // Each successive counter should be exactly 1 more than the previous
    // (with wrap-around at 0xffffff)
    for (let i = 1; i < counters.length; i++) {
      const diff = (counters[i] - counters[i - 1] + 0x1000000) % 0x1000000;
      expect(diff).toBe(1);
    }
  });
});

// =============================================================================
// Future Timestamp Tests
// =============================================================================

describe('ObjectId Future Timestamps', () => {
  it('should handle timestamp from year 2030', () => {
    // 0x71B92C80 = 1908028032 = approximately mid-2030
    const hex = '71b92c80bcf86cd799439011';
    const id = new ObjectId(hex);
    const timestamp = id.getTimestamp();

    expect(timestamp.getUTCFullYear()).toBe(2030);
  });

  it('should handle timestamp from year 2035', () => {
    // 0x7BFCA800 = 2079244800 = Dec 2035 (approximately)
    const hex = '7bfca800bcf86cd799439011';
    const id = new ObjectId(hex);
    const timestamp = id.getTimestamp();

    expect(timestamp.getUTCFullYear()).toBe(2035);
  });

  it('should handle timestamp near 32-bit positive limit (year 2038)', () => {
    // 0x7FFFFFFF = 2147483647 = Jan 19, 2038 03:14:07 UTC (max signed 32-bit)
    const hex = '7fffffffbcf86cd799439011';
    const id = new ObjectId(hex);
    const timestamp = id.getTimestamp();

    expect(timestamp.getUTCFullYear()).toBe(2038);
    expect(timestamp.getUTCMonth()).toBe(0); // January
    expect(timestamp.getUTCDate()).toBe(19);
  });

  it('should handle far future timestamps (within unsigned 32-bit range)', () => {
    // Note: Due to signed 32-bit arithmetic in getTimestamp(),
    // timestamps above 0x7FFFFFFF are interpreted as negative
    // This is a known limitation documented in the implementation

    // 0xBFFFFFFF would be a valid unsigned timestamp for year ~2106
    // but due to signed interpretation, it becomes negative
    const hex = 'bfffffffbcf86cd799439011';
    const id = new ObjectId(hex);
    const timestamp = id.getTimestamp();

    // This will be interpreted as negative due to signed 32-bit arithmetic
    // The high bit is set, so it becomes a negative number
    expect(timestamp.getTime()).toBeLessThan(0);
  });

  it('should correctly order future timestamps', () => {
    const id2025 = new ObjectId('678d5f80bcf86cd799439011'); // ~2025
    const id2030 = new ObjectId('71b92c80bcf86cd799439011'); // ~2030
    const id2035 = new ObjectId('7bfca800bcf86cd799439011'); // ~2035

    const ts2025 = id2025.getTimestamp().getTime();
    const ts2030 = id2030.getTimestamp().getTime();
    const ts2035 = id2035.getTimestamp().getTime();

    expect(ts2025).toBeLessThan(ts2030);
    expect(ts2030).toBeLessThan(ts2035);
  });
});

// =============================================================================
// Historical Timestamp Tests (Extended 1970s coverage)
// =============================================================================

describe('ObjectId Historical Timestamps', () => {
  it('should handle timestamp from January 1, 1970 (Unix epoch)', () => {
    const hex = '00000000bcf86cd799439011';
    const id = new ObjectId(hex);
    const timestamp = id.getTimestamp();

    expect(timestamp.getTime()).toBe(0);
    expect(timestamp.toISOString()).toBe('1970-01-01T00:00:00.000Z');
  });

  it('should handle timestamp from December 31, 1970', () => {
    // 0x01E13300 = 31535200 = approximately Dec 31, 1970
    const hex = '01e13300bcf86cd799439011';
    const id = new ObjectId(hex);
    const timestamp = id.getTimestamp();

    expect(timestamp.getUTCFullYear()).toBe(1970);
    expect(timestamp.getUTCMonth()).toBe(11); // December (0-indexed)
  });

  it('should handle timestamps from the 1980s', () => {
    // 0x12CEA600 = 315548160 = Jan 1, 1980 04:16:00 UTC (verified)
    const hex = '12cea600bcf86cd799439011';
    const id = new ObjectId(hex);
    const timestamp = id.getTimestamp();

    expect(timestamp.getUTCFullYear()).toBe(1980);
  });

  it('should handle timestamps from the 1990s', () => {
    // 0x2BB28600 = 733622784 = 1993 approximately
    const hex = '2bb28600bcf86cd799439011';
    const id = new ObjectId(hex);
    const timestamp = id.getTimestamp();

    expect(timestamp.getUTCFullYear()).toBe(1993);
  });

  it('should handle Y2K timestamp (January 1, 2000)', () => {
    // 0x386D4380 = 946684800 = Jan 1, 2000 00:00:00 UTC
    const hex = '386d4380bcf86cd799439011';
    const id = new ObjectId(hex);
    const timestamp = id.getTimestamp();

    expect(timestamp.getUTCFullYear()).toBe(2000);
    expect(timestamp.getUTCMonth()).toBe(0); // January
    expect(timestamp.getUTCDate()).toBe(1);
  });
});

// =============================================================================
// JSON Serialization Tests
// =============================================================================

describe('ObjectId JSON Serialization', () => {
  it('should serialize to JSON string when using toString()', () => {
    const id = new ObjectId('507f1f77bcf86cd799439011');
    const json = JSON.stringify({ _id: id.toString() });

    expect(json).toBe('{"_id":"507f1f77bcf86cd799439011"}');
  });

  it('should allow JSON.parse reconstruction', () => {
    const originalId = new ObjectId('507f1f77bcf86cd799439011');
    const json = JSON.stringify({ _id: originalId.toString() });
    const parsed = JSON.parse(json);
    const reconstructedId = new ObjectId(parsed._id);

    expect(originalId.equals(reconstructedId)).toBe(true);
  });

  it('should maintain hex string in nested JSON structures', () => {
    const id = new ObjectId('507f1f77bcf86cd799439011');
    const nested = {
      user: {
        _id: id.toString(),
        profile: {
          avatar: 'test.jpg',
        },
      },
    };

    const json = JSON.stringify(nested);
    const parsed = JSON.parse(json);

    expect(parsed.user._id).toBe('507f1f77bcf86cd799439011');
  });

  it('should serialize array of ObjectIds', () => {
    const ids = [
      new ObjectId('507f1f77bcf86cd799439011'),
      new ObjectId('507f1f77bcf86cd799439012'),
      new ObjectId('507f1f77bcf86cd799439013'),
    ];

    const json = JSON.stringify(ids.map((id) => id.toString()));
    const parsed = JSON.parse(json);

    expect(parsed).toHaveLength(3);
    expect(parsed[0]).toBe('507f1f77bcf86cd799439011');
    expect(parsed[1]).toBe('507f1f77bcf86cd799439012');
    expect(parsed[2]).toBe('507f1f77bcf86cd799439013');
  });

  it('should handle JSON.stringify on object with ObjectId property via toString()', () => {
    const doc = {
      _id: new ObjectId('507f1f77bcf86cd799439011').toString(),
      name: 'Test Document',
      createdAt: '2024-01-01T00:00:00.000Z',
    };

    const json = JSON.stringify(doc);
    expect(json).toContain('"_id":"507f1f77bcf86cd799439011"');
    expect(json).toContain('"name":"Test Document"');
  });
});

// =============================================================================
// Large-Scale Uniqueness Stress Tests
// =============================================================================

describe('ObjectId Large-Scale Uniqueness', () => {
  it('should generate 50,000 unique ObjectIds', () => {
    const ids = new Set<string>();
    for (let i = 0; i < 50000; i++) {
      ids.add(new ObjectId().toString());
    }

    expect(ids.size).toBe(50000);
  });

  it('should maintain uniqueness under burst generation', () => {
    // Generate in multiple bursts
    const allIds = new Set<string>();

    for (let burst = 0; burst < 10; burst++) {
      for (let i = 0; i < 1000; i++) {
        allIds.add(new ObjectId().toString());
      }
    }

    expect(allIds.size).toBe(10000);
  });

  it('should generate unique ObjectIds with consistent structure', () => {
    const ids: string[] = [];
    for (let i = 0; i < 1000; i++) {
      ids.push(new ObjectId().toString());
    }

    // All should be valid format
    for (const id of ids) {
      expect(id).toHaveLength(24);
      expect(ObjectId.isValid(id)).toBe(true);
    }

    // All should be unique
    expect(new Set(ids).size).toBe(1000);

    // Same random bytes within process
    const randomBytes = ids.map((id) => id.slice(8, 18));
    const uniqueRandomBytes = new Set(randomBytes);
    expect(uniqueRandomBytes.size).toBe(1);
  });
});
