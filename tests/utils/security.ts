/**
 * Security Testing Utilities
 *
 * Utilities for testing security aspects of MongoLake including:
 * - NoSQL injection patterns
 * - Path traversal attacks
 * - Token forgery helpers
 * - Oversized payload generation
 * - Malformed BSON generation
 * - Auth bypass patterns
 */

// ============================================================================
// NoSQL Injection Payloads
// ============================================================================

/**
 * NoSQL injection payloads for testing query validation.
 * These patterns attempt to exploit MongoDB query operators.
 */
export const nosqlInjectionPayloads = {
  /**
   * Operator injection - attempts to use operators as values
   */
  operators: [
    { $gt: '' },
    { $gte: '' },
    { $lt: '' },
    { $lte: '' },
    { $ne: null },
    { $ne: '' },
    { $regex: '.*' },
    { $regex: '^' },
    { $exists: true },
    { $exists: false },
    { $type: 'string' },
    { $in: ['', null] },
    { $nin: [] },
    { $or: [{ x: 1 }] },
    { $and: [{ x: 1 }] },
    { $not: { $eq: '' } },
  ],

  /**
   * Where clause injection - attempts to execute JavaScript
   */
  whereClauses: [
    { $where: 'this.password' },
    { $where: 'this.secret' },
    { $where: '1 == 1' },
    { $where: 'true' },
    { $where: 'function() { return true; }' },
    { $where: 'sleep(5000)' },
    { $where: 'this.constructor.constructor("return process")().exit()' },
  ],

  /**
   * Prototype pollution attempts
   */
  prototypePollution: [
    { __proto__: { admin: true } },
    { constructor: { prototype: { admin: true } } },
    { 'constructor.prototype.admin': true },
    { '__proto__.admin': true },
  ],

  /**
   * Special field name attacks
   */
  specialFields: [
    { $comment: 'injection' },
    { $explain: true },
    { $hint: 'index' },
    { $maxScan: 1 },
    { $maxTimeMS: 1 },
    { $orderby: { _id: 1 } },
    { $query: {} },
    { $returnKey: true },
    { $showDiskLoc: true },
    { $natural: 1 },
  ],

  /**
   * Aggregation pipeline injection
   */
  aggregation: [
    { $lookup: { from: 'users', localField: 'x', foreignField: 'y', as: 'z' } },
    { $out: 'hackedCollection' },
    { $merge: { into: 'hackedCollection' } },
    { $project: { password: 1, secret: 1 } },
    { $unwind: '$sensitiveData' },
  ],
} as const;

/**
 * All NoSQL injection payloads as a flat array
 */
export const allNosqlPayloads = [
  ...nosqlInjectionPayloads.operators,
  ...nosqlInjectionPayloads.whereClauses,
  ...nosqlInjectionPayloads.prototypePollution,
  ...nosqlInjectionPayloads.specialFields,
  ...nosqlInjectionPayloads.aggregation,
];

// ============================================================================
// Path Traversal Payloads
// ============================================================================

/**
 * Path traversal attack payloads for testing file/path validation.
 */
export const pathTraversalPayloads = {
  /**
   * Unix-style path traversal
   */
  unix: [
    '../../../etc/passwd',
    '../../../etc/shadow',
    '../../../../etc/passwd',
    '../../../../../etc/passwd',
    '../../../../../../etc/passwd',
    '../../../root/.ssh/id_rsa',
    '../../../home/user/.bashrc',
    '../../../var/log/auth.log',
  ],

  /**
   * Windows-style path traversal
   */
  windows: [
    '..\\..\\..\\windows\\system32\\config\\sam',
    '..\\..\\..\\windows\\system32\\config\\system',
    '..\\..\\..\\boot.ini',
    '..\\..\\..\\windows\\win.ini',
    '..\\..\\..\\Users\\Administrator\\Desktop',
  ],

  /**
   * URL-encoded variants
   */
  urlEncoded: [
    '%2e%2e%2f%2e%2e%2f%2e%2e%2fetc%2fpasswd',
    '%2e%2e/%2e%2e/%2e%2e/etc/passwd',
    '..%2f..%2f..%2fetc%2fpasswd',
    '%2e%2e%5c%2e%2e%5c%2e%2e%5cwindows%5csystem32',
    '..%5c..%5c..%5cwindows%5csystem32',
  ],

  /**
   * Double-encoded variants
   */
  doubleEncoded: [
    '%252e%252e%252f%252e%252e%252f%252e%252e%252fetc%252fpasswd',
    '%252e%252e/%252e%252e/%252e%252e/etc/passwd',
    '..%252f..%252f..%252fetc%252fpasswd',
  ],

  /**
   * Unicode/UTF-8 variants
   */
  unicode: [
    '\u002e\u002e\u002f\u002e\u002e\u002f\u002e\u002e\u002fetc\u002fpasswd',
    '\u002e\u002e/\u002e\u002e/\u002e\u002e/etc/passwd',
    '%c0%ae%c0%ae/%c0%ae%c0%ae/%c0%ae%c0%ae/etc/passwd',
    '%c0%ae%c0%ae%c0%af%c0%ae%c0%ae%c0%af%c0%ae%c0%ae%c0%afetc%c0%afpasswd',
  ],

  /**
   * Null byte injection
   */
  nullByte: [
    '../../../etc/passwd%00.png',
    '../../../etc/passwd\x00.txt',
    '../../../etc/passwd%00.jpg',
  ],

  /**
   * Absolute paths (bypass relative checks)
   */
  absolute: [
    '/etc/passwd',
    '/etc/shadow',
    '/root/.ssh/id_rsa',
    'C:\\Windows\\System32\\config\\sam',
    'C:/Windows/System32/config/sam',
  ],
} as const;

/**
 * All path traversal payloads as a flat array
 */
export const allPathTraversalPayloads = [
  ...pathTraversalPayloads.unix,
  ...pathTraversalPayloads.windows,
  ...pathTraversalPayloads.urlEncoded,
  ...pathTraversalPayloads.doubleEncoded,
  ...pathTraversalPayloads.unicode,
  ...pathTraversalPayloads.nullByte,
  ...pathTraversalPayloads.absolute,
];

// ============================================================================
// Token Forgery Helpers
// ============================================================================

/**
 * Generate a malformed JWT token for testing
 */
export function generateMalformedJwt(type: 'missing-signature' | 'invalid-header' | 'invalid-payload' | 'expired' | 'future-iat' | 'wrong-algorithm' | 'none-algorithm'): string {
  const base64url = (str: string): string => {
    return Buffer.from(str).toString('base64url');
  };

  const now = Math.floor(Date.now() / 1000);

  switch (type) {
    case 'missing-signature': {
      const header = base64url(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
      const payload = base64url(JSON.stringify({ sub: 'user', iat: now, exp: now + 3600 }));
      return `${header}.${payload}.`;
    }

    case 'invalid-header': {
      const header = base64url('not-json');
      const payload = base64url(JSON.stringify({ sub: 'user', iat: now, exp: now + 3600 }));
      return `${header}.${payload}.fake-signature`;
    }

    case 'invalid-payload': {
      const header = base64url(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
      const payload = base64url('not-json');
      return `${header}.${payload}.fake-signature`;
    }

    case 'expired': {
      const header = base64url(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
      const payload = base64url(JSON.stringify({
        sub: 'user',
        iat: now - 7200,
        exp: now - 3600, // Expired 1 hour ago
      }));
      return `${header}.${payload}.fake-signature`;
    }

    case 'future-iat': {
      const header = base64url(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
      const payload = base64url(JSON.stringify({
        sub: 'user',
        iat: now + 3600, // Issued in the future
        exp: now + 7200,
      }));
      return `${header}.${payload}.fake-signature`;
    }

    case 'wrong-algorithm': {
      const header = base64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' })); // Claims RS256
      const payload = base64url(JSON.stringify({ sub: 'user', iat: now, exp: now + 3600 }));
      return `${header}.${payload}.hs256-signature-pretending-to-be-rs256`;
    }

    case 'none-algorithm': {
      const header = base64url(JSON.stringify({ alg: 'none', typ: 'JWT' }));
      const payload = base64url(JSON.stringify({ sub: 'admin', iat: now, exp: now + 3600, role: 'admin' }));
      return `${header}.${payload}.`;
    }
  }
}

/**
 * Forged token patterns for testing authentication bypass
 */
export const forgedTokens = {
  /**
   * Common weak/test tokens
   */
  weak: [
    'test',
    'token',
    'bearer',
    'admin',
    'root',
    'password',
    '12345',
    'secret',
  ],

  /**
   * SQL injection in token
   */
  sqlInjection: [
    "' OR '1'='1",
    "admin'--",
    "' OR 1=1--",
    "'; DROP TABLE users;--",
  ],

  /**
   * NoSQL injection in token
   */
  nosqlInjection: [
    '{"$gt": ""}',
    '{"$ne": null}',
    '{"$regex": ".*"}',
  ],

  /**
   * Encoding attacks
   */
  encoding: [
    Buffer.from('admin:admin').toString('base64'),
    Buffer.from('admin').toString('base64'),
    '%61%64%6d%69%6e', // URL-encoded 'admin'
  ],

  /**
   * Null/undefined attacks
   */
  nullish: [
    '',
    'null',
    'undefined',
    'NaN',
    'Infinity',
  ],
} as const;

/**
 * Generate a JWT-like token with custom claims
 */
export function generateForgedJwt(claims: Record<string, unknown>): string {
  const base64url = (str: string): string => {
    return Buffer.from(str).toString('base64url');
  };

  const header = base64url(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
  const payload = base64url(JSON.stringify(claims));
  return `${header}.${payload}.forged-signature`;
}

// ============================================================================
// Oversized Payload Generation
// ============================================================================

/**
 * Generate an oversized payload for testing size limits.
 *
 * @param sizeMB - Size of the payload in megabytes
 * @returns Object with a data field containing the oversized string
 */
export function generateOversizedPayload(sizeMB: number): { data: string } {
  const sizeBytes = sizeMB * 1024 * 1024;
  return { data: 'x'.repeat(sizeBytes) };
}

/**
 * Generate a deeply nested object for testing recursion limits.
 *
 * @param depth - Number of nesting levels
 * @returns Deeply nested object
 */
export function generateDeeplyNestedObject(depth: number): Record<string, unknown> {
  let result: Record<string, unknown> = { value: 'leaf' };
  for (let i = 0; i < depth; i++) {
    result = { nested: result };
  }
  return result;
}

/**
 * Generate an object with many keys for testing field count limits.
 *
 * @param keyCount - Number of keys to generate
 * @returns Object with many keys
 */
export function generateWideObject(keyCount: number): Record<string, string> {
  const result: Record<string, string> = {};
  for (let i = 0; i < keyCount; i++) {
    result[`field_${i}`] = `value_${i}`;
  }
  return result;
}

/**
 * Generate an array with many elements for testing array size limits.
 *
 * @param elementCount - Number of elements
 * @returns Array with many elements
 */
export function generateLargeArray(elementCount: number): unknown[] {
  return Array.from({ length: elementCount }, (_, i) => ({
    index: i,
    value: `item_${i}`,
  }));
}

/**
 * Generate a string with a specific byte size.
 *
 * @param sizeBytes - Size in bytes
 * @param char - Character to repeat (default 'x')
 * @returns String of the specified size
 */
export function generateOversizedString(sizeBytes: number, char: string = 'x'): string {
  return char.repeat(sizeBytes);
}

// ============================================================================
// Malformed BSON Generation
// ============================================================================

/**
 * Malformed BSON payloads for testing BSON parsing robustness.
 */
export const malformedBsonPayloads = {
  /**
   * Invalid document structures
   */
  invalidStructures: [
    null,
    undefined,
    '',
    'not-an-object',
    123,
    true,
    false,
    [],
    [1, 2, 3],
    Symbol('test'),
    () => {},
    new Date('invalid'),
  ],

  /**
   * Circular reference creators (function to avoid issues)
   */
  createCircularReference: (): Record<string, unknown> => {
    const obj: Record<string, unknown> = { name: 'circular' };
    obj.self = obj;
    return obj;
  },

  /**
   * Invalid ObjectId formats
   */
  invalidObjectIds: [
    { _id: 'not-24-chars' },
    { _id: '12345' },
    { _id: 'zzzzzzzzzzzzzzzzzzzzzzzz' }, // Invalid hex
    { _id: '123456789012345678901234567890' }, // Too long
    { _id: 123456 }, // Number instead of string
    { _id: true }, // Boolean instead of string
    { _id: null }, // Null
    { _id: { $oid: 'invalid' } }, // Invalid extended JSON
  ],

  /**
   * Invalid field names
   */
  invalidFieldNames: [
    { '': 'empty key' },
    { 'key.with.dots': 'dots' },
    { 'key\x00null': 'null byte' },
    { '$dollarStart': 'dollar prefix' },
    { ['a'.repeat(1000)]: 'very long key' },
  ],

  /**
   * Invalid values
   */
  invalidValues: [
    { field: BigInt(9007199254740991) },
    { field: Symbol('test') },
    { field: () => {} },
    { field: new WeakMap() },
    { field: new WeakSet() },
    { field: new Proxy({}, {}) },
  ],
} as const;

/**
 * Generate a document with invalid BSON types
 */
export function generateMalformedBsonDocument(type: 'circular' | 'invalid-id' | 'invalid-field' | 'invalid-value' | 'truncated'): unknown {
  switch (type) {
    case 'circular':
      return malformedBsonPayloads.createCircularReference();

    case 'invalid-id':
      return malformedBsonPayloads.invalidObjectIds[0];

    case 'invalid-field':
      return malformedBsonPayloads.invalidFieldNames[0];

    case 'invalid-value':
      return { field: Symbol('test') };

    case 'truncated':
      // Return a buffer that looks like truncated BSON
      return new Uint8Array([0x10, 0x00, 0x00, 0x00]); // Size says 16 bytes but no data
  }
}

// ============================================================================
// Auth Bypass Patterns
// ============================================================================

/**
 * Authentication bypass patterns for testing auth implementations.
 */
export const authBypassPatterns = {
  /**
   * Header manipulation
   */
  headers: {
    /**
     * Headers that might bypass authentication
     */
    bypass: [
      { 'X-Forwarded-For': '127.0.0.1' },
      { 'X-Forwarded-For': 'localhost' },
      { 'X-Real-IP': '127.0.0.1' },
      { 'X-Original-URL': '/admin' },
      { 'X-Rewrite-URL': '/admin' },
      { 'X-Custom-IP-Authorization': '127.0.0.1' },
      { 'X-Forwarded-Host': 'localhost' },
      { 'X-Host': 'localhost' },
      { 'X-ProxyUser-Ip': '127.0.0.1' },
    ],

    /**
     * Headers that should be rejected
     */
    malformed: [
      { 'Authorization': '' },
      { 'Authorization': 'Bearer' },
      { 'Authorization': 'Bearer ' },
      { 'Authorization': 'Basic' },
      { 'Authorization': 'Basic ' },
      { 'Authorization': 'InvalidScheme token' },
      { 'Cookie': 'session=' },
      { 'Cookie': 'session=;' },
    ],
  },

  /**
   * URL manipulation for bypassing path-based auth
   */
  urlManipulation: [
    '/admin/../public/../../admin',
    '/admin/./././',
    '/admin%00',
    '/admin%20',
    '/admin%09',
    '/admin;',
    '/admin/',
    '/admin//',
    '//admin',
    '/Admin', // Case variation
    '/ADMIN',
    '/aDmIn',
  ],

  /**
   * Method override attempts
   */
  methodOverride: [
    { 'X-HTTP-Method': 'GET' },
    { 'X-HTTP-Method-Override': 'GET' },
    { 'X-Method-Override': 'GET' },
    { '_method': 'GET' },
  ],

  /**
   * Content-type manipulation
   */
  contentType: [
    'application/x-www-form-urlencoded',
    'text/plain',
    'application/xml',
    'text/xml',
    'text/html',
    '', // Empty content type
    'application/json; charset=utf-7', // Different encoding
  ],
} as const;

/**
 * Generate authentication test cases
 */
export function generateAuthTestCases(): Array<{
  name: string;
  headers: Record<string, string>;
  shouldReject: boolean;
}> {
  return [
    // Missing auth
    { name: 'No Authorization header', headers: {}, shouldReject: true },
    { name: 'Empty Authorization header', headers: { Authorization: '' }, shouldReject: true },

    // Invalid formats
    { name: 'Bearer without token', headers: { Authorization: 'Bearer' }, shouldReject: true },
    { name: 'Bearer with space only', headers: { Authorization: 'Bearer ' }, shouldReject: true },
    { name: 'Unknown scheme', headers: { Authorization: 'Custom token123' }, shouldReject: true },

    // Malformed tokens
    { name: 'Null token', headers: { Authorization: 'Bearer null' }, shouldReject: true },
    { name: 'Undefined token', headers: { Authorization: 'Bearer undefined' }, shouldReject: true },

    // JWT-specific
    { name: 'Incomplete JWT', headers: { Authorization: 'Bearer header.payload' }, shouldReject: true },
    { name: 'None algorithm JWT', headers: { Authorization: `Bearer ${generateMalformedJwt('none-algorithm')}` }, shouldReject: true },
    { name: 'Expired JWT', headers: { Authorization: `Bearer ${generateMalformedJwt('expired')}` }, shouldReject: true },

    // Injection attempts
    { name: 'SQL injection in auth', headers: { Authorization: "Bearer ' OR '1'='1" }, shouldReject: true },
  ];
}

// ============================================================================
// Injection Payloads Summary (for quick access)
// ============================================================================

/**
 * Consolidated injection payloads for easy iteration in tests.
 */
export const injectionPayloads = {
  nosql: [
    { $gt: '' },
    { $regex: '.*' },
    { $where: 'this.password' },
    { $ne: null },
    { __proto__: { admin: true } },
  ],
  pathTraversal: [
    '../../../etc/passwd',
    '..\\..\\..\\windows\\system32',
    '%2e%2e%2f',
    '%252e%252e%252f',
  ],
} as const;

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Test if a string contains path traversal sequences.
 *
 * @param path - The path to test
 * @returns True if path contains traversal sequences
 */
export function containsPathTraversal(path: string): boolean {
  const traversalPatterns = [
    /\.\.\//,
    /\.\.\\/,
    /%2e%2e/i,
    /%252e%252e/i,
    /\.\.%2f/i,
    /\.\.%5c/i,
  ];
  return traversalPatterns.some(pattern => pattern.test(path));
}

/**
 * Test if an object contains potential NoSQL injection operators.
 *
 * @param obj - The object to test
 * @returns True if object contains potential injection
 */
export function containsNoSqlInjection(obj: unknown): boolean {
  if (obj === null || typeof obj !== 'object') {
    return false;
  }

  const dangerousOperators = [
    '$gt', '$gte', '$lt', '$lte', '$ne', '$in', '$nin',
    '$regex', '$where', '$or', '$and', '$nor', '$not',
    '$exists', '$type', '$expr', '$jsonSchema',
    '__proto__', 'constructor', 'prototype',
  ];

  const keys = Object.keys(obj as object);
  for (const key of keys) {
    if (dangerousOperators.includes(key)) {
      return true;
    }
    const value = (obj as Record<string, unknown>)[key];
    if (typeof value === 'object' && value !== null) {
      if (containsNoSqlInjection(value)) {
        return true;
      }
    }
  }

  return false;
}

/**
 * Sanitize a path by removing traversal sequences.
 * (Example helper - actual implementation should be more robust)
 *
 * @param path - The path to sanitize
 * @returns Sanitized path
 */
export function sanitizePath(path: string): string {
  // Decode URL encoding first
  let decoded = path;
  try {
    decoded = decodeURIComponent(decoded);
    // Handle double encoding
    decoded = decodeURIComponent(decoded);
  } catch {
    // Ignore decoding errors
  }

  // Remove traversal sequences
  return decoded
    .replace(/\.\./g, '')
    .replace(/\/+/g, '/')
    .replace(/\\+/g, '/')
    .replace(/^\//, '');
}

// ============================================================================
// Types
// ============================================================================

/**
 * Type for security test case
 */
export interface SecurityTestCase {
  name: string;
  payload: unknown;
  expectedBehavior: 'reject' | 'sanitize' | 'allow';
  category: 'injection' | 'traversal' | 'auth' | 'size' | 'format';
}

/**
 * Create a comprehensive security test suite
 */
export function createSecurityTestSuite(): SecurityTestCase[] {
  return [
    // NoSQL injection tests
    ...nosqlInjectionPayloads.operators.map((payload, i) => ({
      name: `NoSQL operator injection #${i + 1}`,
      payload,
      expectedBehavior: 'reject' as const,
      category: 'injection' as const,
    })),

    // Path traversal tests
    ...allPathTraversalPayloads.slice(0, 10).map((payload, i) => ({
      name: `Path traversal #${i + 1}`,
      payload,
      expectedBehavior: 'reject' as const,
      category: 'traversal' as const,
    })),

    // Size limit tests
    {
      name: 'Oversized payload (10MB)',
      payload: { size: 10 * 1024 * 1024 }, // Placeholder, actual generation is expensive
      expectedBehavior: 'reject' as const,
      category: 'size' as const,
    },
    {
      name: 'Deeply nested object (100 levels)',
      payload: generateDeeplyNestedObject(100),
      expectedBehavior: 'reject' as const,
      category: 'size' as const,
    },
    {
      name: 'Wide object (10000 keys)',
      payload: { keyCount: 10000 }, // Placeholder
      expectedBehavior: 'reject' as const,
      category: 'size' as const,
    },

    // Format tests
    ...malformedBsonPayloads.invalidObjectIds.map((payload, i) => ({
      name: `Invalid ObjectId #${i + 1}`,
      payload,
      expectedBehavior: 'reject' as const,
      category: 'format' as const,
    })),
  ];
}
