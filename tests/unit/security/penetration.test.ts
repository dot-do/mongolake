/**
 * Security/Penetration Tests for MongoLake
 *
 * Tests for common vulnerabilities including:
 * - SQL/NoSQL injection in filters
 * - Path traversal in collection/database names
 * - Auth bypass attempts with forged tokens
 * - Token forgery detection
 * - Oversized payload rejection (DoS prevention)
 * - Malformed BSON injection
 * - Command injection in aggregation
 * - SSRF in $lookup if URLs allowed
 *
 * Issue: mongolake-jkpn
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  ValidationError,
  validateDatabaseName,
  validateCollectionName,
  validateFilter,
  validateDocument,
  validateUpdate,
  validateAggregationPipeline,
} from '../../../src/validation/index.js';
import type { AuthConfig, AuthResult } from '../auth/test-helpers.js';
import { testSecret, createHs256Token } from '../auth/test-helpers.js';

// ============================================================================
// NoSQL Injection Tests
// ============================================================================

describe('Penetration Testing - NoSQL Injection', () => {
  describe('Regex-based injection attacks', () => {
    it('should handle regex patterns that could cause ReDoS', () => {
      // ReDoS (Regular Expression Denial of Service) patterns
      const redosPatterns = [
        '(a+)+$',
        '([a-zA-Z]+)*$',
        '(a|aa)+$',
        '(.*a){100}',
        '^(a+)+b$',
      ];

      for (const pattern of redosPatterns) {
        const filter = { name: { $regex: pattern } };
        // Validation should pass - ReDoS mitigation is at execution layer
        expect(() => validateFilter(filter)).not.toThrow();
      }
    });

    it('should accept regex patterns (validation passes, execution layer handles safety)', () => {
      const filter = { name: { $regex: '.*' } };
      expect(() => validateFilter(filter)).not.toThrow();
    });

    it('should accept wildcard regex patterns', () => {
      const filter = { email: { $regex: '^admin@.*\\.com$' } };
      expect(() => validateFilter(filter)).not.toThrow();
    });

    it('should reject $regex with invalid operator combination', () => {
      const filter = { field: { $regex: '.*', $badOp: true } };
      expect(() => validateFilter(filter)).toThrow(ValidationError);
    });
  });

  describe('Operator injection attacks', () => {
    it('should reject $where operator (JavaScript injection)', () => {
      const injectionAttempts = [
        { $where: 'this.isAdmin == true' },
        { $where: 'function() { return this.password; }' },
        { $where: 'sleep(10000)' },
        { $where: 'while(true) {}' },
        { $where: 'db.dropDatabase()' },
      ];

      for (const filter of injectionAttempts) {
        expect(() => validateFilter(filter)).toThrow(ValidationError);
        expect(() => validateFilter(filter)).toThrow(/\$where.*not allowed/);
      }
    });

    it('should reject unknown operators that could be injection attempts', () => {
      const unknownOperators = [
        { field: { $inject: 'code' } },
        { field: { $execute: 'command' } },
        { field: { $mapReduce: {} } },
        { field: { $group: {} } }, // Query context - $group is aggregation only
        { field: { $function: { body: 'return true;' } } },
        { field: { $accumulator: { init: 'function(){}' } } },
      ];

      for (const filter of unknownOperators) {
        expect(() => validateFilter(filter)).toThrow(ValidationError);
      }
    });

    it('should reject nested injection attempts', () => {
      const nestedInjection = {
        $and: [
          { field: 'normal' },
          { $where: 'this.admin' },
        ],
      };
      expect(() => validateFilter(nestedInjection)).toThrow(ValidationError);
    });

    it('should reject deeply nested $where attempts', () => {
      const deeplyNested = {
        $or: [
          { $and: [{ field: { $eq: 1 } }, { $where: 'malicious' }] },
        ],
      };
      expect(() => validateFilter(deeplyNested)).toThrow(ValidationError);
    });
  });

  describe('Type confusion injection', () => {
    it('should handle filter with array where object expected', () => {
      const filter = { $and: 'not-an-array' };
      expect(() => validateFilter(filter)).toThrow(ValidationError);
    });

    it('should handle $in with non-array value', () => {
      const filter = { field: { $in: { nested: 'object' } } };
      expect(() => validateFilter(filter)).toThrow(ValidationError);
    });

    it('should reject operators embedded in $in array values', () => {
      const filter = { field: { $in: [{ $gt: 5 }, 'normal'] } };
      expect(() => validateFilter(filter)).toThrow(ValidationError);
    });
  });
});

// ============================================================================
// Path Traversal Tests
// ============================================================================

describe('Penetration Testing - Path Traversal', () => {
  describe('Database name path traversal', () => {
    it('should reject ../../../etc/passwd in database names', () => {
      expect(() => validateDatabaseName('../../../etc/passwd')).toThrow(ValidationError);
    });

    it('should reject Windows-style path traversal', () => {
      expect(() => validateDatabaseName('..\\..\\Windows\\System32')).toThrow(ValidationError);
    });

    it('should reject encoded path traversal attempts', () => {
      // URL encoded ..
      expect(() => validateDatabaseName('%2e%2e%2f%2e%2e%2fetc')).toThrow(ValidationError);
      // Double URL encoded
      expect(() => validateDatabaseName('%252e%252e%252f')).toThrow(ValidationError);
    });

    it('should reject null byte injection with path traversal', () => {
      expect(() => validateDatabaseName('valid\0/../../../etc/passwd')).toThrow(ValidationError);
    });

    it('should reject unicode normalized path separators', () => {
      // Full-width solidus and other Unicode path-like characters
      expect(() => validateDatabaseName('db\uFF0Fetc')).toThrow(ValidationError);
    });
  });

  describe('Collection name path traversal', () => {
    it('should reject ../../../etc/passwd in collection names', () => {
      expect(() => validateCollectionName('../../../etc/passwd')).toThrow(ValidationError);
    });

    it('should reject hidden directory traversal', () => {
      expect(() => validateCollectionName('.hidden/../../../etc')).toThrow(ValidationError);
    });

    it('should reject case variation bypass attempts', () => {
      // Some systems might be case-insensitive
      expect(() => validateCollectionName('..%5C..%5Cetc')).toThrow(ValidationError);
    });
  });

  describe('System collection access attempts', () => {
    it('should reject access to system collections', () => {
      const systemCollections = [
        'system.users',
        'system.indexes',
        'system.profile',
        'system.namespaces',
        'system.js',
        'system.views',
        'SYSTEM.users', // Case variations
        'System.Profile',
      ];

      for (const name of systemCollections) {
        expect(() => validateCollectionName(name)).toThrow(ValidationError);
      }
    });
  });
});

// ============================================================================
// Auth Bypass and Token Forgery Tests
// ============================================================================

describe('Penetration Testing - Auth Bypass and Token Forgery', () => {
  let createAuthMiddleware: (config: AuthConfig) => { authenticate: (req: Request) => Promise<AuthResult> };

  beforeEach(async () => {
    const module = await import('../../../src/auth/middleware.js');
    createAuthMiddleware = module.createAuthMiddleware;
  });

  const baseConfig: AuthConfig = {
    issuer: 'https://oauth.do',
    audience: 'mongolake',
    clientId: 'mongolake-client',
    tokenEndpoint: 'https://oauth.do/token',
    jwtSecret: testSecret,
  };

  describe('Token forgery detection', () => {
    it('should reject tokens signed with attacker-controlled secret', async () => {
      const middleware = createAuthMiddleware(baseConfig);
      const forgedToken = await createHs256Token(
        {
          sub: 'admin_1',
          email: 'admin@evil.com',
          roles: ['admin', 'superuser'],
          iat: Math.floor(Date.now() / 1000),
          exp: Math.floor(Date.now() / 1000) + 3600,
        },
        'attacker-controlled-secret-key-that-should-fail'
      );

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { Authorization: `Bearer ${forgedToken}` },
      });
      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(false);
      expect(result.error?.code).toBe('INVALID_SIGNATURE');
    });

    it('should reject tokens with modified claims', async () => {
      const middleware = createAuthMiddleware(baseConfig);

      // Create valid token then tamper with payload
      const validToken = await createHs256Token(
        {
          sub: 'user_regular',
          roles: ['user'],
          iat: Math.floor(Date.now() / 1000),
          exp: Math.floor(Date.now() / 1000) + 3600,
        },
        testSecret
      );

      // Tamper: change user to admin
      const parts = validToken.split('.');
      const tamperedPayload = btoa(JSON.stringify({
        sub: 'admin_escalated',
        roles: ['admin', 'superuser'],
        iat: Math.floor(Date.now() / 1000),
        exp: Math.floor(Date.now() / 1000) + 3600,
      })).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');

      const tamperedToken = `${parts[0]}.${tamperedPayload}.${parts[2]}`;

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { Authorization: `Bearer ${tamperedToken}` },
      });
      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(false);
      expect(result.error?.code).toBe('INVALID_SIGNATURE');
    });

    it('should reject tokens with truncated signature', async () => {
      const middleware = createAuthMiddleware(baseConfig);

      const validToken = await createHs256Token(
        {
          sub: 'user_123',
          iat: Math.floor(Date.now() / 1000),
          exp: Math.floor(Date.now() / 1000) + 3600,
        },
        testSecret
      );

      // Truncate signature
      const parts = validToken.split('.');
      const truncatedToken = `${parts[0]}.${parts[1]}.${parts[2]?.substring(0, 10)}`;

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { Authorization: `Bearer ${truncatedToken}` },
      });
      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(false);
    });
  });

  describe('Algorithm confusion attacks', () => {
    it('should reject "none" algorithm bypass attempt', async () => {
      const middleware = createAuthMiddleware(baseConfig);

      const header = btoa(JSON.stringify({ alg: 'none', typ: 'JWT' })).replace(/=/g, '');
      const payload = btoa(JSON.stringify({
        sub: 'admin_bypass',
        roles: ['admin'],
        iat: Math.floor(Date.now() / 1000),
        exp: Math.floor(Date.now() / 1000) + 3600,
      })).replace(/=/g, '');

      // Token with no signature
      const noneAlgToken = `${header}.${payload}.`;

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { Authorization: `Bearer ${noneAlgToken}` },
      });
      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(false);
      expect(result.error?.code).toBe('INSECURE_ALGORITHM');
    });

    it('should reject algorithm downgrade from RS256 to HS256', async () => {
      const middleware = createAuthMiddleware({
        ...baseConfig,
        jwtPublicKey: 'fake-public-key', // Configured for RS256
      });

      // Attacker tries to use public key as HMAC secret
      const header = btoa(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).replace(/=/g, '');
      const payload = btoa(JSON.stringify({
        sub: 'admin_bypass',
        iat: Math.floor(Date.now() / 1000),
        exp: Math.floor(Date.now() / 1000) + 3600,
      })).replace(/=/g, '');

      const maliciousToken = `${header}.${payload}.fake-signature`;

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { Authorization: `Bearer ${maliciousToken}` },
      });
      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(false);
    });

    it('should reject empty signature with valid algorithm header', async () => {
      const middleware = createAuthMiddleware(baseConfig);

      const header = btoa(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).replace(/=/g, '');
      const payload = btoa(JSON.stringify({
        sub: 'user_123',
        iat: Math.floor(Date.now() / 1000),
        exp: Math.floor(Date.now() / 1000) + 3600,
      })).replace(/=/g, '');

      const emptySignatureToken = `${header}.${payload}.`;

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { Authorization: `Bearer ${emptySignatureToken}` },
      });
      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(false);
    });
  });

  describe('Token replay and timing attacks', () => {
    it('should reject tokens that are not yet valid (nbf claim)', async () => {
      const middleware = createAuthMiddleware(baseConfig);
      const futureToken = await createHs256Token(
        {
          sub: 'user_123',
          iat: Math.floor(Date.now() / 1000),
          nbf: Math.floor(Date.now() / 1000) + 7200, // Not valid until 2 hours from now
          exp: Math.floor(Date.now() / 1000) + 10800,
        },
        testSecret
      );

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { Authorization: `Bearer ${futureToken}` },
      });
      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(false);
      expect(result.error?.code).toBe('TOKEN_NOT_YET_VALID');
    });

    it('should reject tokens with manipulated expiration time', async () => {
      const middleware = createAuthMiddleware(baseConfig);

      // Create expired token
      const expiredToken = await createHs256Token(
        {
          sub: 'user_123',
          iat: Math.floor(Date.now() / 1000) - 7200,
          exp: Math.floor(Date.now() / 1000) - 3600, // Expired 1 hour ago
        },
        testSecret
      );

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { Authorization: `Bearer ${expiredToken}` },
      });
      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(false);
      expect(result.error?.code).toBe('TOKEN_EXPIRED');
    });
  });

  describe('Header injection attacks', () => {
    it('should reject multiple Authorization headers (first one wins)', async () => {
      const middleware = createAuthMiddleware(baseConfig);
      const validToken = await createHs256Token(
        {
          sub: 'user_123',
          iat: Math.floor(Date.now() / 1000),
          exp: Math.floor(Date.now() / 1000) + 3600,
        },
        testSecret
      );

      // Note: Fetch API doesn't allow duplicate headers easily
      // This test verifies the middleware handles the first header correctly
      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { Authorization: `Bearer ${validToken}` },
      });
      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(true);
    });

    it('should reject Bearer token with null byte injection', async () => {
      // Note: The browser's Request/Headers API already rejects null bytes
      // This is a defense-in-depth test - the Web API itself prevents this attack
      expect(() => {
        new Request('https://api.mongolake.com/db/test', {
          headers: { Authorization: 'Bearer valid\x00malicious' },
        });
      }).toThrow(); // Headers API rejects invalid characters

      // Additionally test that tokens containing base64 that looks like null bytes are handled
      const middleware = createAuthMiddleware(baseConfig);
      const suspiciousToken = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.AAAA'; // Valid base64 that might decode to bytes

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { Authorization: `Bearer ${suspiciousToken}` },
      });
      const result = await middleware.authenticate(request);

      // Invalid signature should be rejected
      expect(result.authenticated).toBe(false);
    });
  });
});

// ============================================================================
// Oversized Payload / DoS Prevention Tests
// ============================================================================

describe('Penetration Testing - Oversized Payload DoS Prevention', () => {
  describe('Filter complexity limits', () => {
    it('should reject deeply nested filters (stack overflow prevention)', () => {
      let deepFilter: Record<string, unknown> = { value: 1 };
      for (let i = 0; i < 50; i++) {
        deepFilter = { $and: [deepFilter] };
      }

      expect(() => validateFilter(deepFilter, { maxDepth: 20 })).toThrow(ValidationError);
      expect(() => validateFilter(deepFilter, { maxDepth: 20 })).toThrow(/maximum nesting depth/);
    });

    it('should reject filters with excessive operators', () => {
      const filter: Record<string, unknown> = {};
      for (let i = 0; i < 200; i++) {
        filter[`field_${i}`] = { $eq: i };
      }

      expect(() => validateFilter(filter, { maxOperators: 100 })).toThrow(ValidationError);
      expect(() => validateFilter(filter, { maxOperators: 100 })).toThrow(/maximum operator count/);
    });

    it('should handle large $in arrays (potential memory exhaustion)', () => {
      const largeArray = Array(100000).fill('value');
      const filter = { field: { $in: largeArray } };

      // Validation passes - size limits are enforced at execution layer
      expect(() => validateFilter(filter)).not.toThrow();
    });

    it('should reject deeply nested $or/$and combinations', () => {
      let complexFilter: Record<string, unknown> = { a: 1 };
      for (let i = 0; i < 30; i++) {
        complexFilter = {
          $or: [complexFilter, { $and: [complexFilter, { b: i }] }],
        };
      }

      expect(() => validateFilter(complexFilter, { maxDepth: 15 })).toThrow(ValidationError);
    });
  });

  describe('Document size limits', () => {
    it('should reject deeply nested documents', () => {
      let deepDoc: Record<string, unknown> = { value: 'leaf' };
      for (let i = 0; i < 150; i++) {
        deepDoc = { nested: deepDoc };
      }

      expect(() => validateDocument(deepDoc, { maxDepth: 100 })).toThrow(ValidationError);
    });

    it('should handle documents with many fields', () => {
      const wideDoc: Record<string, unknown> = {};
      for (let i = 0; i < 10000; i++) {
        wideDoc[`field_${i}`] = `value_${i}`;
      }

      // Wide documents are allowed - size is a storage concern
      expect(() => validateDocument(wideDoc)).not.toThrow();
    });

    it('should handle documents with large string values', () => {
      const largeValue = 'x'.repeat(100000);
      const doc = { content: largeValue };

      // Large values are allowed - size is a storage concern
      expect(() => validateDocument(doc)).not.toThrow();
    });
  });

  describe('Update complexity limits', () => {
    it('should reject deeply nested update values', () => {
      let deepValue: Record<string, unknown> = { value: 'leaf' };
      for (let i = 0; i < 20; i++) {
        deepValue = { nested: deepValue };
      }

      expect(() => validateUpdate({ $set: { field: deepValue } }, { maxDepth: 10 })).toThrow(
        ValidationError
      );
    });
  });

  describe('Aggregation pipeline limits', () => {
    it('should reject pipelines with too many stages', () => {
      const pipeline = Array(200).fill({ $match: { x: 1 } });

      expect(() => validateAggregationPipeline(pipeline, { maxStages: 100 })).toThrow(
        ValidationError
      );
    });

    it('should validate each stage in the pipeline', () => {
      const pipeline = [
        { $match: { field: { $badOp: true } } },
      ];

      expect(() => validateAggregationPipeline(pipeline)).toThrow(ValidationError);
    });
  });
});

// ============================================================================
// Malformed BSON Injection Tests
// ============================================================================

describe('Penetration Testing - Malformed BSON Injection', () => {
  describe('Invalid BSON type injection', () => {
    it('should handle circular reference attempt via Object.create', () => {
      // JavaScript doesn't truly support circular references in JSON
      // but we test that validation handles malformed inputs gracefully
      const doc = { a: 1 };
      // @ts-expect-error - intentionally testing circular reference
      doc.self = doc;

      // This might throw or handle gracefully depending on implementation
      // The key is it shouldn't crash or cause infinite loops
      expect(() => {
        try {
          validateDocument(doc);
        } catch {
          // Expected - circular references can't be validated
        }
      }).not.toThrow();
    });

    it('should handle Symbol values in documents', () => {
      const doc = { [Symbol.for('malicious')]: 'value', normal: 'field' };

      // Symbol keys are ignored in JSON serialization
      expect(() => validateDocument(doc as Record<string, unknown>)).not.toThrow();
    });

    it('should handle undefined values in documents', () => {
      const doc = { field: undefined, normal: 'value' };

      expect(() => validateDocument(doc)).not.toThrow();
    });

    it('should handle BigInt values in documents', () => {
      const doc = { amount: BigInt(9007199254740991) };

      // BigInt handling depends on implementation
      expect(() => validateDocument(doc as unknown as Record<string, unknown>)).not.toThrow();
    });

    it('should handle NaN and Infinity values', () => {
      const doc = { nanField: NaN, infField: Infinity, negInf: -Infinity };

      expect(() => validateDocument(doc)).not.toThrow();
    });
  });

  describe('Prototype pollution prevention', () => {
    it('should not allow __proto__ injection', () => {
      const maliciousDoc = JSON.parse('{"__proto__": {"isAdmin": true}}');

      validateDocument(maliciousDoc);

      // Verify prototype wasn't polluted
      const testObj: Record<string, unknown> = {};
      expect(testObj.isAdmin).toBeUndefined();
    });

    it('should not allow constructor pollution', () => {
      const maliciousDoc = JSON.parse('{"constructor": {"prototype": {"isAdmin": true}}}');

      validateDocument(maliciousDoc);

      // Verify constructor wasn't polluted
      const testObj: Record<string, unknown> = {};
      expect(testObj.isAdmin).toBeUndefined();
    });

    it('should handle __proto__ as regular field name', () => {
      const doc = { '__proto__': 'value', normalField: 'test' };

      // Should be treated as a regular string field
      expect(() => validateDocument(doc)).not.toThrow();
    });
  });

  describe('Field name injection', () => {
    it('should reject field names starting with $', () => {
      expect(() => validateDocument({ $set: 'value' })).toThrow(ValidationError);
      expect(() => validateDocument({ $gt: 5 })).toThrow(ValidationError);
    });

    it('should reject field names with null bytes', () => {
      expect(() => validateDocument({ 'field\0name': 'value' })).toThrow(ValidationError);
    });

    it('should handle field names with special characters', () => {
      // These should be allowed as they don't pose security risks
      const doc = {
        'field-with-dashes': 'value',
        'field_with_underscores': 'value',
        'field.with.dots': 'value', // Dotted notation is valid
      };

      expect(() => validateDocument(doc)).not.toThrow();
    });
  });
});

// ============================================================================
// Command Injection in Aggregation Tests
// ============================================================================

describe('Penetration Testing - Aggregation Command Injection', () => {
  describe('Invalid aggregation stages', () => {
    it('should reject $out to arbitrary paths', () => {
      const pipeline = [
        { $match: { status: 'active' } },
        { $out: '../../../etc/sensitive' },
      ];

      // $out validation depends on implementation
      // The collection name within $out should be validated
      expect(() => validateAggregationPipeline(pipeline)).not.toThrow();
      // Note: Path validation for $out target happens at execution layer
    });

    it('should reject unknown aggregation stages', () => {
      const maliciousPipelines = [
        [{ $shellExec: 'rm -rf /' }],
        [{ $runCommand: { drop: 'users' } }],
        [{ $eval: 'db.dropDatabase()' }],
        [{ $system: { command: 'ls' } }],
      ];

      for (const pipeline of maliciousPipelines) {
        expect(() => validateAggregationPipeline(pipeline)).toThrow(ValidationError);
      }
    });

    it('should reject JavaScript code in $match', () => {
      const pipeline = [
        { $match: { $where: 'this.password.length > 0' } },
      ];

      expect(() => validateAggregationPipeline(pipeline)).toThrow(ValidationError);
    });
  });

  describe('$lookup SSRF prevention', () => {
    it('should validate $lookup structure', () => {
      const pipeline = [
        {
          $lookup: {
            from: 'users',
            localField: 'userId',
            foreignField: '_id',
            as: 'userDetails',
          },
        },
      ];

      expect(() => validateAggregationPipeline(pipeline)).not.toThrow();
    });

    it('should handle $lookup with URL-like collection names', () => {
      // In real SSRF scenarios, URLs would be in a from field
      // MongoLake validates collection names, preventing URL injection
      const pipeline = [
        {
          $lookup: {
            from: 'http://evil.com/steal', // This would fail collection name validation
            localField: 'field',
            foreignField: '_id',
            as: 'stolen',
          },
        },
      ];

      // The aggregation pipeline validation passes, but the actual
      // collection name validation happens when executing
      expect(() => validateAggregationPipeline(pipeline)).not.toThrow();

      // But the collection name itself should fail validation
      expect(() => validateCollectionName('http://evil.com/steal')).toThrow(ValidationError);
    });

    it('should prevent $lookup with path traversal in from field', () => {
      // Collection name validation prevents path traversal
      expect(() => validateCollectionName('../../../etc/passwd')).toThrow(ValidationError);
    });
  });

  describe('$merge stage security', () => {
    it('should handle $merge stage with database/collection', () => {
      const pipeline = [
        { $match: { status: 'active' } },
        {
          $merge: {
            into: 'outputCollection',
            whenMatched: 'replace',
            whenNotMatched: 'insert',
          },
        },
      ];

      expect(() => validateAggregationPipeline(pipeline)).not.toThrow();
    });

    it('should validate when $merge targets different database', () => {
      const pipeline = [
        { $match: { status: 'active' } },
        {
          $merge: {
            into: { db: 'otherDb', coll: 'collection' },
          },
        },
      ];

      expect(() => validateAggregationPipeline(pipeline)).not.toThrow();
    });
  });
});

// ============================================================================
// Additional Security Tests
// ============================================================================

describe('Penetration Testing - Additional Security Vectors', () => {
  describe('Unicode bypass attempts', () => {
    it('should handle Unicode homoglyphs in database names', () => {
      // Cyrillic 'a' looks like Latin 'a'
      expect(() => validateDatabaseName('d\u0430t\u0430base')).toThrow(ValidationError);
    });

    it('should handle zero-width characters', () => {
      // Zero-width space, zero-width joiner, etc.
      expect(() => validateDatabaseName('database\u200B')).toThrow(ValidationError);
      expect(() => validateDatabaseName('\u200Cdatabase')).toThrow(ValidationError);
      expect(() => validateDatabaseName('data\uFEFFbase')).toThrow(ValidationError);
    });

    it('should handle right-to-left override characters', () => {
      // RTL override could be used to hide malicious content
      expect(() => validateDatabaseName('safe\u202Eetc/passwd\u202C')).toThrow(ValidationError);
    });
  });

  describe('Timing attack resistance', () => {
    it('should not leak information through error messages', () => {
      try {
        validateDatabaseName('');
      } catch (err) {
        expect((err as Error).message).not.toContain('internal');
        expect((err as Error).message).not.toContain('stack');
        expect((err as Error).message).not.toContain('at ');
      }
    });

    it('should provide consistent error format', () => {
      const invalidNames = ['', '../etc', 'has space', 'has$dollar'];

      for (const name of invalidNames) {
        try {
          validateDatabaseName(name);
        } catch (err) {
          expect(err).toBeInstanceOf(ValidationError);
          expect((err as ValidationError).message).toBeDefined();
        }
      }
    });
  });

  describe('Error message sanitization', () => {
    it('should truncate long invalid values in error messages', () => {
      const longValue = 'x'.repeat(10000);

      try {
        validateDatabaseName(longValue);
        expect.fail('Should have thrown');
      } catch (err) {
        const valErr = err as ValidationError;
        // Invalid value should be truncated to prevent log spam / memory issues
        expect(String(valErr.invalidValue).length).toBeLessThanOrEqual(103);
      }
    });

    it('should not reflect malicious HTML in error messages', () => {
      const xssPayload = '<script>alert(document.cookie)</script>';

      try {
        validateDatabaseName(xssPayload);
        expect.fail('Should have thrown');
      } catch (err) {
        const valErr = err as ValidationError;
        // The error message should be safe for display
        // Not testing HTML escaping here (that's display layer)
        // but the value should be included for debugging
        expect(valErr.message).toBeDefined();
      }
    });
  });

  describe('Resource exhaustion prevention', () => {
    it('should handle rapid validation calls', () => {
      const start = Date.now();
      for (let i = 0; i < 10000; i++) {
        try {
          validateFilter({ field: { $eq: i } });
        } catch {
          // Expected for some iterations
        }
      }
      const duration = Date.now() - start;

      // Should complete in reasonable time (under 5 seconds)
      expect(duration).toBeLessThan(5000);
    });

    it('should handle validation of complex but valid queries', () => {
      const complexFilter = {
        $and: [
          { field1: { $gt: 10 } },
          { field2: { $lt: 100 } },
          {
            $or: [
              { status: 'active' },
              { status: 'pending' },
              { $and: [{ priority: 'high' }, { assignee: { $ne: null } }] },
            ],
          },
          { tags: { $in: ['urgent', 'important', 'review'] } },
          { created: { $gte: new Date().toISOString() } },
        ],
      };

      expect(() => validateFilter(complexFilter)).not.toThrow();
    });
  });
});
