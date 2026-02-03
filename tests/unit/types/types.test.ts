/**
 * Comprehensive Types Tests
 *
 * This file provides additional tests for src/types.ts covering:
 * - ObjectId creation from Date
 * - Branded types helper functions
 * - ObjectId JSON/toJSON behavior
 * - ObjectId comparison and sorting
 * - ObjectId edge cases not covered elsewhere
 * - Type coercion behaviors
 */

import { describe, it, expect } from 'vitest';
import {
  ObjectId,
  toDocumentId,
  toShardId,
  toCollectionName,
  toDatabaseName,
  isDocument,
  assertDocument,
  asDocument,
  isDocumentId,
  assertDocumentId,
  isShardId,
  assertShardId,
  isCollectionName,
  assertCollectionName,
  isDatabaseName,
  assertDatabaseName,
  type DocumentId,
  type ShardId,
  type CollectionName,
  type DatabaseName,
  type Document,
  type BSONValue,
  type AnyDocument,
  type WithId,
} from '../../../src/types.js';

// =============================================================================
// ObjectId Creation from Date
// =============================================================================

describe('ObjectId Creation from Date', () => {
  describe('creating ObjectId with specific timestamp', () => {
    it('should create ObjectId that extracts correct timestamp from known date', () => {
      // Create an ObjectId from a hex string with known timestamp
      // Timestamp: 0x65B5A000 = 1706442752 = Jan 28, 2024 (approximately)
      const knownTimestamp = 0x65b5a000;
      const knownDate = new Date(knownTimestamp * 1000);

      // Create ObjectId with this timestamp in first 4 bytes
      const hexWithTimestamp = '65b5a000' + '1234567890' + '123456';
      const oid = new ObjectId(hexWithTimestamp);

      const extractedDate = oid.getTimestamp();
      expect(extractedDate.getTime()).toBe(knownDate.getTime());
    });

    it('should allow creating ObjectId-like value for specific date via hex construction', () => {
      // To create an ObjectId for a specific date, construct hex with timestamp
      const targetDate = new Date('2025-01-01T00:00:00.000Z');
      const timestampSeconds = Math.floor(targetDate.getTime() / 1000);

      // Convert timestamp to 8-char hex (big-endian)
      const timestampHex = timestampSeconds.toString(16).padStart(8, '0');

      // Append random bytes and counter
      const fullHex = timestampHex + 'aabbccddee' + 'ffffff';
      const oid = new ObjectId(fullHex);

      const extractedDate = oid.getTimestamp();
      expect(extractedDate.getTime()).toBe(targetDate.getTime());
    });

    it('should preserve date precision to the second', () => {
      // ObjectId timestamp has second-level precision
      const date = new Date('2024-06-15T12:30:45.123Z');
      const timestampSeconds = Math.floor(date.getTime() / 1000);
      const timestampHex = timestampSeconds.toString(16).padStart(8, '0');

      const oid = new ObjectId(timestampHex + '0000000000' + '000000');
      const extracted = oid.getTimestamp();

      // Should be accurate to the second, not millisecond
      expect(extracted.getTime()).toBe(timestampSeconds * 1000);
      // Milliseconds are lost
      expect(extracted.getMilliseconds()).toBe(0);
    });

    it('should handle Unix epoch (January 1, 1970)', () => {
      const epoch = new Date(0);
      const oid = new ObjectId('000000000000000000000000');

      const extracted = oid.getTimestamp();
      expect(extracted.getTime()).toBe(epoch.getTime());
    });

    it('should handle dates in the distant past (1970s)', () => {
      // 0x0001E240 = 123456 seconds = about 1.4 days after epoch
      const hex = '0001e2400000000000000000';
      const oid = new ObjectId(hex);

      const extracted = oid.getTimestamp();
      expect(extracted.getFullYear()).toBe(1970);
    });

    it('should handle dates in year 2038 (32-bit limit)', () => {
      // Max positive signed 32-bit: 0x7FFFFFFF = 2147483647 seconds
      const maxSignedTimestamp = 0x7fffffff;
      const hex = '7fffffff0000000000000000';
      const oid = new ObjectId(hex);

      const extracted = oid.getTimestamp();
      expect(extracted.getTime()).toBe(maxSignedTimestamp * 1000);
      expect(extracted.getFullYear()).toBe(2038);
    });

    it('should correctly order ObjectIds by timestamp', () => {
      // Create ObjectIds with sequential timestamps
      const timestamps = [
        '60000000', // 2021
        '65000000', // 2023
        '6a000000', // 2024
      ];

      const oids = timestamps.map(ts => new ObjectId(ts + '0000000000' + '000000'));

      // Sort by string comparison (should work since hex is big-endian)
      const sorted = [...oids].sort((a, b) => a.toString().localeCompare(b.toString()));

      // Verify sorted order matches timestamp order
      for (let i = 1; i < sorted.length; i++) {
        const prevTs = sorted[i - 1].getTimestamp().getTime();
        const currTs = sorted[i].getTimestamp().getTime();
        expect(currTs).toBeGreaterThan(prevTs);
      }
    });
  });
});

// =============================================================================
// Branded Types Helper Functions
// =============================================================================

describe('Branded Types Helper Functions', () => {
  describe('toDocumentId', () => {
    it('should create DocumentId from string', () => {
      const id: DocumentId = toDocumentId('doc-123');
      expect(id).toBe('doc-123');
    });

    it('should create DocumentId from ObjectId hex string', () => {
      const hexString = '507f1f77bcf86cd799439011';
      const id: DocumentId = toDocumentId(hexString);
      expect(id).toBe(hexString);
    });

    it('should create DocumentId from empty string', () => {
      const id: DocumentId = toDocumentId('');
      expect(id).toBe('');
    });

    it('should create DocumentId from UUID-format string', () => {
      const uuid = '550e8400-e29b-41d4-a716-446655440000';
      const id: DocumentId = toDocumentId(uuid);
      expect(id).toBe(uuid);
    });

    it('should preserve string value', () => {
      const original = 'my-unique-document-id-123';
      const id: DocumentId = toDocumentId(original);
      expect(String(id)).toBe(original);
      expect(id.length).toBe(original.length);
    });

    it('should work with string operations', () => {
      const id: DocumentId = toDocumentId('prefix_suffix');
      expect(id.startsWith('prefix')).toBe(true);
      expect(id.endsWith('suffix')).toBe(true);
      expect(id.includes('_')).toBe(true);
    });
  });

  describe('toShardId', () => {
    it('should create ShardId from positive integer', () => {
      const id: ShardId = toShardId(5);
      expect(id).toBe(5);
    });

    it('should create ShardId from zero', () => {
      const id: ShardId = toShardId(0);
      expect(id).toBe(0);
    });

    it('should create ShardId from negative integer', () => {
      const id: ShardId = toShardId(-1);
      expect(id).toBe(-1);
    });

    it('should create ShardId from large integer', () => {
      const id: ShardId = toShardId(1000000);
      expect(id).toBe(1000000);
    });

    it('should preserve numeric operations', () => {
      const id: ShardId = toShardId(10);
      expect(id + 5).toBe(15);
      expect(id * 2).toBe(20);
      expect(id / 2).toBe(5);
    });

    it('should work with numeric comparisons', () => {
      const id1: ShardId = toShardId(5);
      const id2: ShardId = toShardId(10);
      expect(id1 < id2).toBe(true);
      expect(id2 > id1).toBe(true);
    });

    it('should work in arrays', () => {
      const shards: ShardId[] = [toShardId(1), toShardId(2), toShardId(3)];
      expect(shards.length).toBe(3);
      expect(shards.includes(toShardId(2))).toBe(true);
    });
  });

  describe('toCollectionName', () => {
    it('should create CollectionName from string', () => {
      const name: CollectionName = toCollectionName('users');
      expect(name).toBe('users');
    });

    it('should create CollectionName from dot-notation string', () => {
      const name: CollectionName = toCollectionName('system.users');
      expect(name).toBe('system.users');
    });

    it('should create CollectionName from empty string', () => {
      const name: CollectionName = toCollectionName('');
      expect(name).toBe('');
    });

    it('should preserve string value', () => {
      const original = 'my_collection_name';
      const name: CollectionName = toCollectionName(original);
      expect(String(name)).toBe(original);
    });

    it('should work with template literals', () => {
      const name: CollectionName = toCollectionName('test');
      const fullPath = `db.${name}`;
      expect(fullPath).toBe('db.test');
    });
  });

  describe('toDatabaseName', () => {
    it('should create DatabaseName from string', () => {
      const name: DatabaseName = toDatabaseName('mydb');
      expect(name).toBe('mydb');
    });

    it('should create DatabaseName from empty string', () => {
      const name: DatabaseName = toDatabaseName('');
      expect(name).toBe('');
    });

    it('should handle reserved database names', () => {
      const adminDb: DatabaseName = toDatabaseName('admin');
      const localDb: DatabaseName = toDatabaseName('local');
      const configDb: DatabaseName = toDatabaseName('config');

      expect(adminDb).toBe('admin');
      expect(localDb).toBe('local');
      expect(configDb).toBe('config');
    });

    it('should preserve string operations', () => {
      const name: DatabaseName = toDatabaseName('production_db');
      expect(name.toUpperCase()).toBe('PRODUCTION_DB');
      expect(name.toLowerCase()).toBe('production_db');
    });
  });

  describe('branded type type safety', () => {
    it('should preserve underlying value type', () => {
      const docId: DocumentId = toDocumentId('test');
      const shardId: ShardId = toShardId(5);
      const collName: CollectionName = toCollectionName('coll');
      const dbName: DatabaseName = toDatabaseName('db');

      // Type assertions - these should compile
      expect(typeof docId).toBe('string');
      expect(typeof shardId).toBe('number');
      expect(typeof collName).toBe('string');
      expect(typeof dbName).toBe('string');
    });

    it('should work in generic contexts', () => {
      const ids: DocumentId[] = [
        toDocumentId('a'),
        toDocumentId('b'),
        toDocumentId('c'),
      ];

      expect(ids.map(id => id.toUpperCase())).toEqual(['A', 'B', 'C']);
    });

    it('should work with Set for deduplication', () => {
      const shardIds: Set<ShardId> = new Set([
        toShardId(1),
        toShardId(2),
        toShardId(1), // duplicate
        toShardId(3),
      ]);

      expect(shardIds.size).toBe(3);
    });

    it('should work with Map as keys', () => {
      const map = new Map<CollectionName, number>();
      const coll1 = toCollectionName('users');
      const coll2 = toCollectionName('orders');

      map.set(coll1, 100);
      map.set(coll2, 200);

      expect(map.get(coll1)).toBe(100);
      expect(map.get(toCollectionName('users'))).toBe(100);
    });
  });
});

// =============================================================================
// ObjectId JSON Serialization Behavior
// =============================================================================

describe('ObjectId JSON Serialization', () => {
  describe('JSON.stringify behavior', () => {
    it('should serialize ObjectId via toString() in JSON', () => {
      const oid = new ObjectId('507f1f77bcf86cd799439011');
      const json = JSON.stringify({ _id: oid.toString() });

      expect(json).toBe('{"_id":"507f1f77bcf86cd799439011"}');
    });

    it('should not have toJSON method (use toString explicitly)', () => {
      const oid = new ObjectId();
      // ObjectId doesn't implement toJSON, so direct serialization may produce [object Object]
      // Users should always use oid.toString() explicitly
      expect(typeof (oid as { toJSON?: unknown }).toJSON).toBe('undefined');
    });

    it('should round-trip through JSON with explicit toString', () => {
      const original = new ObjectId();
      const originalHex = original.toString();

      const json = JSON.stringify({ _id: originalHex });
      const parsed = JSON.parse(json);
      const reconstructed = new ObjectId(parsed._id);

      expect(original.equals(reconstructed)).toBe(true);
    });

    it('should serialize array of ObjectIds', () => {
      const oids = [
        new ObjectId('000000000000000000000001'),
        new ObjectId('000000000000000000000002'),
        new ObjectId('000000000000000000000003'),
      ];

      const json = JSON.stringify(oids.map(o => o.toString()));
      const parsed = JSON.parse(json);

      expect(parsed).toEqual([
        '000000000000000000000001',
        '000000000000000000000002',
        '000000000000000000000003',
      ]);
    });

    it('should serialize nested document with ObjectId', () => {
      const doc = {
        _id: new ObjectId('507f1f77bcf86cd799439011').toString(),
        user: {
          _id: new ObjectId('507f1f77bcf86cd799439012').toString(),
          profile: {
            avatar_id: new ObjectId('507f1f77bcf86cd799439013').toString(),
          },
        },
      };

      const json = JSON.stringify(doc);
      const parsed = JSON.parse(json);

      expect(parsed._id).toBe('507f1f77bcf86cd799439011');
      expect(parsed.user._id).toBe('507f1f77bcf86cd799439012');
      expect(parsed.user.profile.avatar_id).toBe('507f1f77bcf86cd799439013');
    });
  });

  describe('conversion to string contexts', () => {
    it('should work with template literals', () => {
      const oid = new ObjectId('507f1f77bcf86cd799439011');
      const str = `ObjectId: ${oid}`;

      // Template literal calls toString() implicitly
      expect(str).toBe('ObjectId: 507f1f77bcf86cd799439011');
    });

    it('should work with String() constructor', () => {
      const oid = new ObjectId('507f1f77bcf86cd799439011');
      const str = String(oid);

      expect(str).toBe('507f1f77bcf86cd799439011');
    });

    it('should work with concatenation', () => {
      const oid = new ObjectId('507f1f77bcf86cd799439011');
      const str = 'id:' + oid;

      expect(str).toBe('id:507f1f77bcf86cd799439011');
    });
  });
});

// =============================================================================
// ObjectId Comparison and Sorting
// =============================================================================

describe('ObjectId Comparison and Sorting', () => {
  describe('equals method', () => {
    it('should return true for identical hex strings', () => {
      const hex = '507f1f77bcf86cd799439011';
      const oid1 = new ObjectId(hex);
      const oid2 = new ObjectId(hex);

      expect(oid1.equals(oid2)).toBe(true);
      expect(oid2.equals(oid1)).toBe(true);
    });

    it('should return false for different ObjectIds', () => {
      const oid1 = new ObjectId('507f1f77bcf86cd799439011');
      const oid2 = new ObjectId('507f1f77bcf86cd799439012');

      expect(oid1.equals(oid2)).toBe(false);
    });

    it('should compare by value, not reference', () => {
      const bytes1 = new Uint8Array([0x50, 0x7f, 0x1f, 0x77, 0xbc, 0xf8, 0x6c, 0xd7, 0x99, 0x43, 0x90, 0x11]);
      const bytes2 = new Uint8Array([0x50, 0x7f, 0x1f, 0x77, 0xbc, 0xf8, 0x6c, 0xd7, 0x99, 0x43, 0x90, 0x11]);

      const oid1 = new ObjectId(bytes1);
      const oid2 = new ObjectId(bytes2);

      expect(oid1.equals(oid2)).toBe(true);
    });

    it('should handle self-comparison', () => {
      const oid = new ObjectId();
      expect(oid.equals(oid)).toBe(true);
    });
  });

  describe('lexicographic sorting via toString', () => {
    it('should sort ObjectIds chronologically by string comparison', () => {
      // ObjectIds with increasing timestamps
      const oid1 = new ObjectId('60000000bcf86cd799439011'); // earlier
      const oid2 = new ObjectId('65000000bcf86cd799439011'); // later
      const oid3 = new ObjectId('70000000bcf86cd799439011'); // latest

      const oids = [oid3, oid1, oid2];
      const sorted = oids.sort((a, b) => a.toString().localeCompare(b.toString()));

      expect(sorted[0].toString()).toBe(oid1.toString());
      expect(sorted[1].toString()).toBe(oid2.toString());
      expect(sorted[2].toString()).toBe(oid3.toString());
    });

    it('should sort ObjectIds with same timestamp by random bytes', () => {
      const oid1 = new ObjectId('65000000aabbccddee000001');
      const oid2 = new ObjectId('65000000aabbccddee000002');
      const oid3 = new ObjectId('65000000ffffffffffffff');

      const oids = [oid3, oid1, oid2];
      const sorted = oids.sort((a, b) => a.toString().localeCompare(b.toString()));

      expect(sorted[0].toString()).toBe(oid1.toString());
      expect(sorted[1].toString()).toBe(oid2.toString());
      expect(sorted[2].toString()).toBe(oid3.toString());
    });

    it('should sort array of generated ObjectIds', () => {
      const oids = Array.from({ length: 100 }, () => new ObjectId());
      const strings = oids.map(o => o.toString());
      const sorted = [...strings].sort();

      // Generated ObjectIds should already be in sorted order (same timestamp + incrementing counter)
      // unless they span a second boundary
      // At minimum, verify sorting doesn't throw and produces valid result
      expect(sorted.length).toBe(100);
      expect(sorted.every(s => ObjectId.isValid(s))).toBe(true);
    });
  });

  describe('comparison edge cases', () => {
    it('should differentiate ObjectIds differing only in last byte', () => {
      const oid1 = new ObjectId('507f1f77bcf86cd799439010');
      const oid2 = new ObjectId('507f1f77bcf86cd799439011');

      expect(oid1.equals(oid2)).toBe(false);
      expect(oid1.toString() < oid2.toString()).toBe(true);
    });

    it('should differentiate ObjectIds differing only in first byte', () => {
      const oid1 = new ObjectId('00000000bcf86cd799439011');
      const oid2 = new ObjectId('01000000bcf86cd799439011');

      expect(oid1.equals(oid2)).toBe(false);
      expect(oid1.toString() < oid2.toString()).toBe(true);
    });

    it('should handle all-zeros vs all-ones comparison', () => {
      const zeros = new ObjectId('000000000000000000000000');
      const ones = new ObjectId('ffffffffffffffffffffffff');

      expect(zeros.equals(ones)).toBe(false);
      expect(zeros.toString() < ones.toString()).toBe(true);
    });
  });
});

// =============================================================================
// ObjectId isValid Edge Cases
// =============================================================================

describe('ObjectId.isValid Edge Cases', () => {
  describe('valid inputs', () => {
    it('should accept lowercase hex', () => {
      expect(ObjectId.isValid('abcdef0123456789abcdef01')).toBe(true);
    });

    it('should accept uppercase hex', () => {
      expect(ObjectId.isValid('ABCDEF0123456789ABCDEF01')).toBe(true);
    });

    it('should accept mixed case hex', () => {
      expect(ObjectId.isValid('AbCdEf0123456789aBcDeF01')).toBe(true);
    });

    it('should accept all zeros', () => {
      expect(ObjectId.isValid('000000000000000000000000')).toBe(true);
    });

    it('should accept all f/F', () => {
      expect(ObjectId.isValid('ffffffffffffffffffffffff')).toBe(true);
      expect(ObjectId.isValid('FFFFFFFFFFFFFFFFFFFFFFFF')).toBe(true);
    });
  });

  describe('invalid inputs', () => {
    it('should reject 23 characters (too short)', () => {
      expect(ObjectId.isValid('507f1f77bcf86cd79943901')).toBe(false);
    });

    it('should reject 25 characters (too long)', () => {
      expect(ObjectId.isValid('507f1f77bcf86cd7994390111')).toBe(false);
    });

    it('should reject empty string', () => {
      expect(ObjectId.isValid('')).toBe(false);
    });

    it('should reject string with spaces', () => {
      expect(ObjectId.isValid('507f1f77 bcf86cd799439011')).toBe(false);
      expect(ObjectId.isValid(' 507f1f77bcf86cd79943901')).toBe(false);
      expect(ObjectId.isValid('507f1f77bcf86cd79943901 ')).toBe(false);
    });

    it('should reject string with non-hex letters', () => {
      expect(ObjectId.isValid('507f1f77bcf86cd79943901g')).toBe(false);
      expect(ObjectId.isValid('507f1f77bcf86cd79943901z')).toBe(false);
      expect(ObjectId.isValid('507f1f77bcf86cd79943901G')).toBe(false);
    });

    it('should reject string with special characters', () => {
      expect(ObjectId.isValid('507f1f77bcf86cd79943901!')).toBe(false);
      expect(ObjectId.isValid('507f1f77bcf86cd79943901@')).toBe(false);
      expect(ObjectId.isValid('507f1f77-bcf8-6cd7-9943-')).toBe(false);
    });

    it('should reject null', () => {
      expect(ObjectId.isValid(null as unknown as string)).toBe(false);
    });

    it('should reject undefined', () => {
      expect(ObjectId.isValid(undefined as unknown as string)).toBe(false);
    });

    it('should reject numbers', () => {
      expect(ObjectId.isValid(123456789012345678901234 as unknown as string)).toBe(false);
    });

    it('should reject objects', () => {
      expect(ObjectId.isValid({} as unknown as string)).toBe(false);
    });

    it('should reject arrays', () => {
      expect(ObjectId.isValid([] as unknown as string)).toBe(false);
    });

    it('should reject booleans', () => {
      expect(ObjectId.isValid(true as unknown as string)).toBe(false);
      expect(ObjectId.isValid(false as unknown as string)).toBe(false);
    });
  });

  describe('unicode and special characters', () => {
    it('should reject unicode lookalikes', () => {
      // Cyrillic 'a' looks like Latin 'a' but is different
      expect(ObjectId.isValid('507f1f77bcf86cd79943901\u0430')).toBe(false);
    });

    it('should reject fullwidth digits', () => {
      // Fullwidth digit 1: U+FF11
      expect(ObjectId.isValid('507f1f77bcf86cd79943901\uff11')).toBe(false);
    });

    it('should reject strings with newlines', () => {
      expect(ObjectId.isValid('507f1f77bcf86cd7\n9943901')).toBe(false);
    });

    it('should reject strings with tabs', () => {
      expect(ObjectId.isValid('507f1f77bcf86cd7\t9943901')).toBe(false);
    });

    it('should reject strings with null bytes', () => {
      expect(ObjectId.isValid('507f1f77bcf86cd7\x009943')).toBe(false);
    });
  });
});

// =============================================================================
// WithId Type Tests
// =============================================================================

describe('WithId Type', () => {
  it('should require _id field', () => {
    interface User {
      name: string;
      email: string;
    }

    const userWithId: WithId<User> = {
      _id: 'user-123',
      name: 'John',
      email: 'john@example.com',
    };

    expect(userWithId._id).toBe('user-123');
    expect(userWithId.name).toBe('John');
  });

  it('should work with ObjectId _id', () => {
    interface Product {
      name: string;
      price: number;
    }

    const oid = new ObjectId();
    const product: WithId<Product> = {
      _id: oid,
      name: 'Widget',
      price: 9.99,
    };

    expect(product._id).toBe(oid);
  });

  it('should preserve original type properties', () => {
    interface ComplexDoc {
      tags: string[];
      metadata: {
        created: Date;
        version: number;
      };
    }

    const doc: WithId<ComplexDoc> = {
      _id: 'doc-1',
      tags: ['a', 'b'],
      metadata: {
        created: new Date(),
        version: 1,
      },
    };

    expect(doc.tags).toEqual(['a', 'b']);
    expect(doc.metadata.version).toBe(1);
  });
});

// =============================================================================
// Document Type Guard Additional Tests
// =============================================================================

describe('Document Type Guards Additional Tests', () => {
  describe('isDocument with various _id types', () => {
    it('should accept undefined _id', () => {
      expect(isDocument({ name: 'test' })).toBe(true);
    });

    it('should accept string _id', () => {
      expect(isDocument({ _id: 'string-id' })).toBe(true);
    });

    it('should accept ObjectId _id', () => {
      expect(isDocument({ _id: new ObjectId() })).toBe(true);
    });

    it('should reject number _id', () => {
      expect(isDocument({ _id: 123 })).toBe(false);
    });

    it('should reject object _id (not ObjectId)', () => {
      expect(isDocument({ _id: { invalid: true } })).toBe(false);
    });

    it('should reject null _id', () => {
      expect(isDocument({ _id: null })).toBe(false);
    });

    it('should reject array _id', () => {
      expect(isDocument({ _id: ['a', 'b'] })).toBe(false);
    });

    it('should reject Date _id', () => {
      expect(isDocument({ _id: new Date() })).toBe(false);
    });
  });

  describe('assertDocument with edge cases', () => {
    it('should not throw for valid document', () => {
      expect(() => assertDocument({ _id: 'valid' })).not.toThrow();
    });

    it('should throw TypeError for invalid document', () => {
      expect(() => assertDocument(null)).toThrow(TypeError);
    });

    it('should use custom message when provided', () => {
      expect(() => assertDocument(null, 'Custom error')).toThrow('Custom error');
    });

    it('should use default message when not provided', () => {
      expect(() => assertDocument(null)).toThrow('Value is not a valid Document');
    });
  });

  describe('asDocument type casting', () => {
    it('should return same object reference', () => {
      const obj = { _id: 'test', data: 'value' };
      const doc = asDocument(obj);
      expect(doc).toBe(obj);
    });

    it('should work with AnyDocument', () => {
      const anyDoc: AnyDocument = { _id: 'any', custom: Symbol('test') };
      const doc = asDocument(anyDoc);
      expect(doc._id).toBe('any');
    });
  });
});

// =============================================================================
// BSONValue Type Additional Tests
// =============================================================================

describe('BSONValue Type Additional Tests', () => {
  describe('all valid BSON types', () => {
    it('should accept string', () => {
      const value: BSONValue = 'test';
      expect(typeof value).toBe('string');
    });

    it('should accept integer', () => {
      const value: BSONValue = 42;
      expect(typeof value).toBe('number');
    });

    it('should accept float', () => {
      const value: BSONValue = 3.14159;
      expect(typeof value).toBe('number');
    });

    it('should accept boolean true', () => {
      const value: BSONValue = true;
      expect(value).toBe(true);
    });

    it('should accept boolean false', () => {
      const value: BSONValue = false;
      expect(value).toBe(false);
    });

    it('should accept null', () => {
      const value: BSONValue = null;
      expect(value).toBeNull();
    });

    it('should accept Date', () => {
      const value: BSONValue = new Date();
      expect(value).toBeInstanceOf(Date);
    });

    it('should accept Uint8Array', () => {
      const value: BSONValue = new Uint8Array([1, 2, 3]);
      expect(value).toBeInstanceOf(Uint8Array);
    });

    it('should accept ObjectId', () => {
      const value: BSONValue = new ObjectId();
      expect(value).toBeInstanceOf(ObjectId);
    });

    it('should accept array of BSONValues', () => {
      const value: BSONValue = [1, 'two', true, null, new Date()];
      expect(Array.isArray(value)).toBe(true);
    });

    it('should accept nested object', () => {
      const value: BSONValue = {
        level1: {
          level2: {
            value: 'deep',
          },
        },
      };
      expect(typeof value).toBe('object');
    });
  });

  describe('complex nested structures', () => {
    it('should accept deeply nested arrays and objects', () => {
      const value: BSONValue = {
        users: [
          {
            _id: new ObjectId(),
            name: 'John',
            tags: ['admin', 'user'],
            metadata: {
              created: new Date(),
              binary: new Uint8Array([1, 2]),
            },
          },
        ],
      };
      expect((value as Record<string, unknown>).users).toBeDefined();
    });
  });
});

// =============================================================================
// ObjectId Generation Counter Behavior
// =============================================================================

describe('ObjectId Generation Counter Behavior', () => {
  it('should increment counter for sequential ObjectIds', () => {
    const oid1 = new ObjectId();
    const oid2 = new ObjectId();
    const oid3 = new ObjectId();

    // Extract counters (last 6 hex chars = 3 bytes)
    const counter1 = parseInt(oid1.toString().slice(18), 16);
    const counter2 = parseInt(oid2.toString().slice(18), 16);
    const counter3 = parseInt(oid3.toString().slice(18), 16);

    // Counters should increment by 1 (with potential wrap)
    const diff1 = (counter2 - counter1 + 0x1000000) % 0x1000000;
    const diff2 = (counter3 - counter2 + 0x1000000) % 0x1000000;

    expect(diff1).toBe(1);
    expect(diff2).toBe(1);
  });

  it('should maintain same random bytes within process', () => {
    const oids = Array.from({ length: 10 }, () => new ObjectId());

    // Extract random bytes (bytes 4-8 = hex chars 8-17)
    const randomBytesSet = new Set(oids.map(o => o.toString().slice(8, 18)));

    // All should have same random bytes (generated once per process)
    expect(randomBytesSet.size).toBe(1);
  });

  it('should have counter in valid 24-bit range', () => {
    const oids = Array.from({ length: 100 }, () => new ObjectId());

    for (const oid of oids) {
      const counter = parseInt(oid.toString().slice(18), 16);
      expect(counter).toBeGreaterThanOrEqual(0);
      expect(counter).toBeLessThanOrEqual(0xffffff);
    }
  });
});

// =============================================================================
// ObjectId Uint8Array Construction
// =============================================================================

describe('ObjectId Uint8Array Construction', () => {
  it('should construct from 12-byte Uint8Array', () => {
    const bytes = new Uint8Array([0x50, 0x7f, 0x1f, 0x77, 0xbc, 0xf8, 0x6c, 0xd7, 0x99, 0x43, 0x90, 0x11]);
    const oid = new ObjectId(bytes);

    expect(oid.toString()).toBe('507f1f77bcf86cd799439011');
  });

  it('should handle all zeros', () => {
    const bytes = new Uint8Array(12).fill(0);
    const oid = new ObjectId(bytes);

    expect(oid.toString()).toBe('000000000000000000000000');
  });

  it('should handle all 255s', () => {
    const bytes = new Uint8Array(12).fill(255);
    const oid = new ObjectId(bytes);

    expect(oid.toString()).toBe('ffffffffffffffffffffffff');
  });

  it('should handle sequential bytes', () => {
    const bytes = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    const oid = new ObjectId(bytes);

    expect(oid.toString()).toBe('000102030405060708090a0b');
  });

  it('should preserve exact byte values', () => {
    const bytes = new Uint8Array([0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12]);
    const oid = new ObjectId(bytes);

    expect(oid.toString()).toBe('abcdef123456789abcdef012');
  });

  it('should round-trip Uint8Array to hex and back', () => {
    const originalBytes = new Uint8Array([0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe, 0xba, 0xbe, 0x12, 0x34, 0x56, 0x78]);
    const oid = new ObjectId(originalBytes);
    const hex = oid.toString();

    // Reconstruct from hex
    const reconstructed = new ObjectId(hex);

    expect(oid.equals(reconstructed)).toBe(true);
  });
});

// =============================================================================
// ObjectId Instance Methods
// =============================================================================

describe('ObjectId Instance Methods', () => {
  describe('toString', () => {
    it('should return 24-character lowercase hex string', () => {
      const oid = new ObjectId();
      const str = oid.toString();

      expect(str).toHaveLength(24);
      expect(/^[0-9a-f]{24}$/.test(str)).toBe(true);
    });

    it('should be stable across multiple calls', () => {
      const oid = new ObjectId();

      const str1 = oid.toString();
      const str2 = oid.toString();
      const str3 = oid.toString();

      expect(str1).toBe(str2);
      expect(str2).toBe(str3);
    });
  });

  describe('toHexString', () => {
    it('should return same value as toString', () => {
      const oid = new ObjectId();

      expect(oid.toHexString()).toBe(oid.toString());
    });

    it('should return 24-character hex string', () => {
      const oid = new ObjectId('507f1f77bcf86cd799439011');

      expect(oid.toHexString()).toBe('507f1f77bcf86cd799439011');
    });
  });

  describe('getTimestamp', () => {
    it('should return Date object', () => {
      const oid = new ObjectId();
      const timestamp = oid.getTimestamp();

      expect(timestamp).toBeInstanceOf(Date);
    });

    it('should return current time for newly generated ObjectId', () => {
      const before = Date.now();
      const oid = new ObjectId();
      const after = Date.now();

      const timestamp = oid.getTimestamp();

      // Should be within the creation window (rounded to seconds)
      expect(timestamp.getTime()).toBeGreaterThanOrEqual(Math.floor(before / 1000) * 1000);
      expect(timestamp.getTime()).toBeLessThanOrEqual(Math.ceil(after / 1000) * 1000);
    });

    it('should extract known timestamp', () => {
      // 0x507681C0 = 1349943744 seconds
      const oid = new ObjectId('507681c0bcf86cd799439011');
      const timestamp = oid.getTimestamp();

      expect(timestamp.getTime()).toBe(0x507681c0 * 1000);
    });
  });

  describe('equals', () => {
    it('should return true for equal ObjectIds', () => {
      const hex = '507f1f77bcf86cd799439011';
      const oid1 = new ObjectId(hex);
      const oid2 = new ObjectId(hex);

      expect(oid1.equals(oid2)).toBe(true);
    });

    it('should return false for different ObjectIds', () => {
      const oid1 = new ObjectId('507f1f77bcf86cd799439011');
      const oid2 = new ObjectId('507f1f77bcf86cd799439012');

      expect(oid1.equals(oid2)).toBe(false);
    });

    it('should be symmetric', () => {
      const oid1 = new ObjectId();
      const oid2 = new ObjectId(oid1.toString());

      expect(oid1.equals(oid2)).toBe(oid2.equals(oid1));
    });

    it('should be reflexive', () => {
      const oid = new ObjectId();
      expect(oid.equals(oid)).toBe(true);
    });

    it('should be transitive', () => {
      const hex = '507f1f77bcf86cd799439011';
      const oid1 = new ObjectId(hex);
      const oid2 = new ObjectId(hex);
      const oid3 = new ObjectId(hex);

      expect(oid1.equals(oid2)).toBe(true);
      expect(oid2.equals(oid3)).toBe(true);
      expect(oid1.equals(oid3)).toBe(true);
    });
  });
});

// =============================================================================
// ObjectId Static Methods
// =============================================================================

describe('ObjectId Static Methods', () => {
  describe('isValid', () => {
    it('should be a static method', () => {
      expect(typeof ObjectId.isValid).toBe('function');
    });

    it('should validate correct hex strings', () => {
      expect(ObjectId.isValid('507f1f77bcf86cd799439011')).toBe(true);
    });

    it('should reject invalid strings', () => {
      expect(ObjectId.isValid('invalid')).toBe(false);
    });

    it('should be callable without instance', () => {
      const result = ObjectId.isValid('507f1f77bcf86cd799439011');
      expect(typeof result).toBe('boolean');
    });
  });
});

// =============================================================================
// Branded Type Runtime Validation Tests
// =============================================================================

describe('Branded Type Runtime Validation', () => {
  // ---------------------------------------------------------------------------
  // isDocumentId Tests
  // ---------------------------------------------------------------------------
  describe('isDocumentId', () => {
    describe('valid DocumentIds', () => {
      it('should accept non-empty string', () => {
        expect(isDocumentId('doc-123')).toBe(true);
      });

      it('should accept ObjectId hex string', () => {
        expect(isDocumentId('507f1f77bcf86cd799439011')).toBe(true);
      });

      it('should accept UUID format string', () => {
        expect(isDocumentId('550e8400-e29b-41d4-a716-446655440000')).toBe(true);
      });

      it('should accept single character string', () => {
        expect(isDocumentId('a')).toBe(true);
      });

      it('should accept string with special characters', () => {
        expect(isDocumentId('doc/path:123#fragment')).toBe(true);
      });

      it('should accept very long string', () => {
        expect(isDocumentId('a'.repeat(1000))).toBe(true);
      });
    });

    describe('invalid DocumentIds', () => {
      it('should reject empty string', () => {
        expect(isDocumentId('')).toBe(false);
      });

      it('should reject null', () => {
        expect(isDocumentId(null)).toBe(false);
      });

      it('should reject undefined', () => {
        expect(isDocumentId(undefined)).toBe(false);
      });

      it('should reject number', () => {
        expect(isDocumentId(123)).toBe(false);
      });

      it('should reject object', () => {
        expect(isDocumentId({ id: '123' })).toBe(false);
      });

      it('should reject array', () => {
        expect(isDocumentId(['123'])).toBe(false);
      });

      it('should reject boolean', () => {
        expect(isDocumentId(true)).toBe(false);
      });

      it('should reject ObjectId instance', () => {
        expect(isDocumentId(new ObjectId())).toBe(false);
      });
    });

    describe('type narrowing', () => {
      it('should narrow type correctly', () => {
        const value: unknown = 'doc-123';
        if (isDocumentId(value)) {
          // TypeScript should recognize value as DocumentId here
          const docId: DocumentId = value;
          expect(docId).toBe('doc-123');
        }
      });
    });
  });

  // ---------------------------------------------------------------------------
  // assertDocumentId Tests
  // ---------------------------------------------------------------------------
  describe('assertDocumentId', () => {
    it('should not throw for valid DocumentId', () => {
      expect(() => assertDocumentId('doc-123')).not.toThrow();
    });

    it('should throw TypeError for empty string', () => {
      expect(() => assertDocumentId('')).toThrow(TypeError);
    });

    it('should throw TypeError for null', () => {
      expect(() => assertDocumentId(null)).toThrow(TypeError);
    });

    it('should throw TypeError for undefined', () => {
      expect(() => assertDocumentId(undefined)).toThrow(TypeError);
    });

    it('should throw TypeError for number', () => {
      expect(() => assertDocumentId(123)).toThrow(TypeError);
    });

    it('should use default message when not provided', () => {
      expect(() => assertDocumentId(null)).toThrow('Value is not a valid DocumentId');
    });

    it('should use custom message when provided', () => {
      expect(() => assertDocumentId(null, 'Custom error')).toThrow('Custom error');
    });

    it('should provide type assertion', () => {
      const value: unknown = 'doc-123';
      assertDocumentId(value);
      // TypeScript should recognize value as DocumentId after assertion
      const docId: DocumentId = value;
      expect(docId).toBe('doc-123');
    });
  });

  // ---------------------------------------------------------------------------
  // isShardId Tests
  // ---------------------------------------------------------------------------
  describe('isShardId', () => {
    describe('valid ShardIds', () => {
      it('should accept zero', () => {
        expect(isShardId(0)).toBe(true);
      });

      it('should accept positive integer', () => {
        expect(isShardId(5)).toBe(true);
      });

      it('should accept large positive integer', () => {
        expect(isShardId(1000000)).toBe(true);
      });

      it('should accept MAX_SAFE_INTEGER', () => {
        expect(isShardId(Number.MAX_SAFE_INTEGER)).toBe(true);
      });
    });

    describe('invalid ShardIds', () => {
      it('should reject negative integer', () => {
        expect(isShardId(-1)).toBe(false);
      });

      it('should reject floating point number', () => {
        expect(isShardId(1.5)).toBe(false);
      });

      it('should reject NaN', () => {
        expect(isShardId(NaN)).toBe(false);
      });

      it('should reject Infinity', () => {
        expect(isShardId(Infinity)).toBe(false);
      });

      it('should reject negative Infinity', () => {
        expect(isShardId(-Infinity)).toBe(false);
      });

      it('should reject string', () => {
        expect(isShardId('5')).toBe(false);
      });

      it('should reject null', () => {
        expect(isShardId(null)).toBe(false);
      });

      it('should reject undefined', () => {
        expect(isShardId(undefined)).toBe(false);
      });

      it('should reject object', () => {
        expect(isShardId({ id: 5 })).toBe(false);
      });

      it('should reject array', () => {
        expect(isShardId([5])).toBe(false);
      });

      it('should reject boolean', () => {
        expect(isShardId(true)).toBe(false);
      });
    });

    describe('type narrowing', () => {
      it('should narrow type correctly', () => {
        const value: unknown = 5;
        if (isShardId(value)) {
          // TypeScript should recognize value as ShardId here
          const shardId: ShardId = value;
          expect(shardId).toBe(5);
        }
      });
    });
  });

  // ---------------------------------------------------------------------------
  // assertShardId Tests
  // ---------------------------------------------------------------------------
  describe('assertShardId', () => {
    it('should not throw for valid ShardId', () => {
      expect(() => assertShardId(0)).not.toThrow();
      expect(() => assertShardId(5)).not.toThrow();
    });

    it('should throw TypeError for negative number', () => {
      expect(() => assertShardId(-1)).toThrow(TypeError);
    });

    it('should throw TypeError for floating point', () => {
      expect(() => assertShardId(1.5)).toThrow(TypeError);
    });

    it('should throw TypeError for string', () => {
      expect(() => assertShardId('5')).toThrow(TypeError);
    });

    it('should throw TypeError for null', () => {
      expect(() => assertShardId(null)).toThrow(TypeError);
    });

    it('should use default message when not provided', () => {
      expect(() => assertShardId(-1)).toThrow('Value is not a valid ShardId');
    });

    it('should use custom message when provided', () => {
      expect(() => assertShardId(-1, 'Custom error')).toThrow('Custom error');
    });

    it('should provide type assertion', () => {
      const value: unknown = 5;
      assertShardId(value);
      // TypeScript should recognize value as ShardId after assertion
      const shardId: ShardId = value;
      expect(shardId).toBe(5);
    });
  });

  // ---------------------------------------------------------------------------
  // isCollectionName Tests
  // ---------------------------------------------------------------------------
  describe('isCollectionName', () => {
    describe('valid CollectionNames', () => {
      it('should accept simple name', () => {
        expect(isCollectionName('users')).toBe(true);
      });

      it('should accept name with underscore', () => {
        expect(isCollectionName('user_profiles')).toBe(true);
      });

      it('should accept name with hyphen', () => {
        expect(isCollectionName('user-profiles')).toBe(true);
      });

      it('should accept name with numbers', () => {
        expect(isCollectionName('users123')).toBe(true);
      });

      it('should accept single character name', () => {
        expect(isCollectionName('a')).toBe(true);
      });

      it('should accept name with dots (not at start)', () => {
        expect(isCollectionName('users.archive')).toBe(true);
      });

      it('should accept 120 character name', () => {
        expect(isCollectionName('a'.repeat(120))).toBe(true);
      });
    });

    describe('invalid CollectionNames', () => {
      it('should reject empty string', () => {
        expect(isCollectionName('')).toBe(false);
      });

      it('should reject name starting with system.', () => {
        expect(isCollectionName('system.users')).toBe(false);
      });

      it('should reject name containing $', () => {
        expect(isCollectionName('users$special')).toBe(false);
      });

      it('should reject name containing null character', () => {
        expect(isCollectionName('users\x00test')).toBe(false);
      });

      it('should reject name longer than 120 characters', () => {
        expect(isCollectionName('a'.repeat(121))).toBe(false);
      });

      it('should reject null', () => {
        expect(isCollectionName(null)).toBe(false);
      });

      it('should reject undefined', () => {
        expect(isCollectionName(undefined)).toBe(false);
      });

      it('should reject number', () => {
        expect(isCollectionName(123)).toBe(false);
      });

      it('should reject object', () => {
        expect(isCollectionName({ name: 'users' })).toBe(false);
      });

      it('should reject array', () => {
        expect(isCollectionName(['users'])).toBe(false);
      });
    });

    describe('type narrowing', () => {
      it('should narrow type correctly', () => {
        const value: unknown = 'users';
        if (isCollectionName(value)) {
          // TypeScript should recognize value as CollectionName here
          const collName: CollectionName = value;
          expect(collName).toBe('users');
        }
      });
    });
  });

  // ---------------------------------------------------------------------------
  // assertCollectionName Tests
  // ---------------------------------------------------------------------------
  describe('assertCollectionName', () => {
    it('should not throw for valid CollectionName', () => {
      expect(() => assertCollectionName('users')).not.toThrow();
    });

    it('should throw TypeError for empty string', () => {
      expect(() => assertCollectionName('')).toThrow(TypeError);
    });

    it('should throw TypeError for system. prefix', () => {
      expect(() => assertCollectionName('system.users')).toThrow(TypeError);
    });

    it('should throw TypeError for $ character', () => {
      expect(() => assertCollectionName('users$test')).toThrow(TypeError);
    });

    it('should throw TypeError for null', () => {
      expect(() => assertCollectionName(null)).toThrow(TypeError);
    });

    it('should use default message when not provided', () => {
      expect(() => assertCollectionName('')).toThrow('Value is not a valid CollectionName');
    });

    it('should use custom message when provided', () => {
      expect(() => assertCollectionName('', 'Custom error')).toThrow('Custom error');
    });

    it('should provide type assertion', () => {
      const value: unknown = 'users';
      assertCollectionName(value);
      // TypeScript should recognize value as CollectionName after assertion
      const collName: CollectionName = value;
      expect(collName).toBe('users');
    });
  });

  // ---------------------------------------------------------------------------
  // isDatabaseName Tests
  // ---------------------------------------------------------------------------
  describe('isDatabaseName', () => {
    describe('valid DatabaseNames', () => {
      it('should accept simple name', () => {
        expect(isDatabaseName('mydb')).toBe(true);
      });

      it('should accept name with underscore', () => {
        expect(isDatabaseName('my_database')).toBe(true);
      });

      it('should accept name with hyphen', () => {
        expect(isDatabaseName('my-database')).toBe(true);
      });

      it('should accept name with numbers', () => {
        expect(isDatabaseName('db123')).toBe(true);
      });

      it('should accept single character name', () => {
        expect(isDatabaseName('a')).toBe(true);
      });

      it('should accept reserved names (admin, local, config)', () => {
        expect(isDatabaseName('admin')).toBe(true);
        expect(isDatabaseName('local')).toBe(true);
        expect(isDatabaseName('config')).toBe(true);
      });

      it('should accept 64 character name', () => {
        expect(isDatabaseName('a'.repeat(64))).toBe(true);
      });
    });

    describe('invalid DatabaseNames', () => {
      it('should reject empty string', () => {
        expect(isDatabaseName('')).toBe(false);
      });

      it('should reject name with forward slash', () => {
        expect(isDatabaseName('my/db')).toBe(false);
      });

      it('should reject name with backslash', () => {
        expect(isDatabaseName('my\\db')).toBe(false);
      });

      it('should reject name with dot', () => {
        expect(isDatabaseName('my.db')).toBe(false);
      });

      it('should reject name with space', () => {
        expect(isDatabaseName('my db')).toBe(false);
      });

      it('should reject name with double quote', () => {
        expect(isDatabaseName('my"db')).toBe(false);
      });

      it('should reject name with dollar sign', () => {
        expect(isDatabaseName('my$db')).toBe(false);
      });

      it('should reject name with asterisk', () => {
        expect(isDatabaseName('my*db')).toBe(false);
      });

      it('should reject name with less than', () => {
        expect(isDatabaseName('my<db')).toBe(false);
      });

      it('should reject name with greater than', () => {
        expect(isDatabaseName('my>db')).toBe(false);
      });

      it('should reject name with colon', () => {
        expect(isDatabaseName('my:db')).toBe(false);
      });

      it('should reject name with pipe', () => {
        expect(isDatabaseName('my|db')).toBe(false);
      });

      it('should reject name with question mark', () => {
        expect(isDatabaseName('my?db')).toBe(false);
      });

      it('should reject name with null character', () => {
        expect(isDatabaseName('my\x00db')).toBe(false);
      });

      it('should reject name longer than 64 characters', () => {
        expect(isDatabaseName('a'.repeat(65))).toBe(false);
      });

      it('should reject null', () => {
        expect(isDatabaseName(null)).toBe(false);
      });

      it('should reject undefined', () => {
        expect(isDatabaseName(undefined)).toBe(false);
      });

      it('should reject number', () => {
        expect(isDatabaseName(123)).toBe(false);
      });

      it('should reject object', () => {
        expect(isDatabaseName({ name: 'mydb' })).toBe(false);
      });

      it('should reject array', () => {
        expect(isDatabaseName(['mydb'])).toBe(false);
      });
    });

    describe('type narrowing', () => {
      it('should narrow type correctly', () => {
        const value: unknown = 'mydb';
        if (isDatabaseName(value)) {
          // TypeScript should recognize value as DatabaseName here
          const dbName: DatabaseName = value;
          expect(dbName).toBe('mydb');
        }
      });
    });
  });

  // ---------------------------------------------------------------------------
  // assertDatabaseName Tests
  // ---------------------------------------------------------------------------
  describe('assertDatabaseName', () => {
    it('should not throw for valid DatabaseName', () => {
      expect(() => assertDatabaseName('mydb')).not.toThrow();
    });

    it('should throw TypeError for empty string', () => {
      expect(() => assertDatabaseName('')).toThrow(TypeError);
    });

    it('should throw TypeError for invalid characters', () => {
      expect(() => assertDatabaseName('my/db')).toThrow(TypeError);
      expect(() => assertDatabaseName('my.db')).toThrow(TypeError);
      expect(() => assertDatabaseName('my db')).toThrow(TypeError);
    });

    it('should throw TypeError for null', () => {
      expect(() => assertDatabaseName(null)).toThrow(TypeError);
    });

    it('should use default message when not provided', () => {
      expect(() => assertDatabaseName('')).toThrow('Value is not a valid DatabaseName');
    });

    it('should use custom message when provided', () => {
      expect(() => assertDatabaseName('', 'Custom error')).toThrow('Custom error');
    });

    it('should provide type assertion', () => {
      const value: unknown = 'mydb';
      assertDatabaseName(value);
      // TypeScript should recognize value as DatabaseName after assertion
      const dbName: DatabaseName = value;
      expect(dbName).toBe('mydb');
    });
  });

  // ---------------------------------------------------------------------------
  // Integration with to* functions
  // ---------------------------------------------------------------------------
  describe('integration with to* functions', () => {
    it('should validate values created with toDocumentId', () => {
      const docId = toDocumentId('doc-123');
      expect(isDocumentId(docId)).toBe(true);
    });

    it('should validate values created with toShardId', () => {
      const shardId = toShardId(5);
      expect(isShardId(shardId)).toBe(true);
    });

    it('should validate values created with toCollectionName', () => {
      const collName = toCollectionName('users');
      expect(isCollectionName(collName)).toBe(true);
    });

    it('should validate values created with toDatabaseName', () => {
      const dbName = toDatabaseName('mydb');
      expect(isDatabaseName(dbName)).toBe(true);
    });
  });

  // ---------------------------------------------------------------------------
  // Edge cases
  // ---------------------------------------------------------------------------
  describe('edge cases', () => {
    it('should handle whitespace-only strings for DocumentId', () => {
      // Whitespace-only is technically a valid string, though unusual
      expect(isDocumentId('   ')).toBe(true);
      expect(isDocumentId('\t')).toBe(true);
      expect(isDocumentId('\n')).toBe(true);
    });

    it('should handle unicode in DocumentId', () => {
      expect(isDocumentId('\u4e2d\u6587')).toBe(true); // Chinese characters
      expect(isDocumentId('\ud83d\ude00')).toBe(true); // Emoji
    });

    it('should handle exact boundary for ShardId', () => {
      expect(isShardId(0)).toBe(true);
      expect(isShardId(-0)).toBe(true); // -0 is equal to 0 in JS
    });

    it('should handle exact boundary for collection name length', () => {
      expect(isCollectionName('a'.repeat(120))).toBe(true);
      expect(isCollectionName('a'.repeat(121))).toBe(false);
    });

    it('should handle exact boundary for database name length', () => {
      expect(isDatabaseName('a'.repeat(64))).toBe(true);
      expect(isDatabaseName('a'.repeat(65))).toBe(false);
    });

    it('should handle systemX prefix (allowed)', () => {
      // "systemX" doesn't start with "system." so it's valid
      expect(isCollectionName('systemX')).toBe(true);
      expect(isCollectionName('systems')).toBe(true);
    });
  });
});
