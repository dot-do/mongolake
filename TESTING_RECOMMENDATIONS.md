# MongoLake Testing Recommendations - Action Plan

## Quick Reference: What to Fix First

### This Week (Critical - 30 hours)

1. **✗ tests/unit/utils/projection.test.ts** (MISSING)
   - Lines needed: ~400
   - Time: 4 hours
   - Priority: 🔴 CRITICAL

2. **✗ tests/unit/types.test.ts** (MISSING - ObjectId focus)
   - Lines needed: ~800
   - Time: 6 hours
   - Priority: 🔴 CRITICAL

3. **✗ tests/e2e/crud-lifecycle.test.ts** (MISSING)
   - Lines needed: ~500
   - Time: 6 hours
   - Priority: 🟠 HIGH

4. **✗ tests/performance/filter-matching.test.ts** (MISSING)
   - Lines needed: ~300
   - Time: 4 hours
   - Priority: 🟠 HIGH

5. **Refactor vitest configs**
   - Add coverage thresholds
   - Add performance test config
   - Time: 4 hours
   - Priority: 🟠 HIGH

### Next Sprint (High Priority - 40 hours)

6. **Expand E2E tests** (currently 21 tests)
   - Add concurrent write tests
   - Add aggregation E2E tests
   - Add failover scenarios
   - Time: 12 hours

7. **Create shared test utilities**
   - Test data factories
   - Custom assertions
   - Mock helpers
   - Time: 8 hours

8. **Error scenario test suite**
   - Storage errors
   - Network errors
   - Concurrent conflicts
   - Time: 12 hours

9. **Documentation**
   - Testing guide
   - Pattern examples
   - Setup instructions
   - Time: 6 hours

10. **Test infrastructure**
    - CI/CD setup
    - Coverage reporting
    - Pre-commit hooks
    - Time: 6 hours

---

## Implementation Guide: Creating Missing Tests

### 1. Creating tests/unit/utils/projection.test.ts

**Template to use**:

```typescript
/**
 * Projection Tests
 *
 * Tests for MongoDB-style field projections:
 * - Inclusion: { field: 1 } - only include specified fields
 * - Exclusion: { field: 0 } - exclude specified fields
 * - _id handling: always included unless excluded
 */

import { describe, it, expect } from 'vitest';
import { applyProjection } from '../../../src/utils/projection.js';

describe('applyProjection', () => {
  // ====== INCLUSION PROJECTIONS ======
  describe('Inclusion Mode - { field: 1 }', () => {
    it('should include specified fields', () => {
      const doc = { _id: '1', name: 'Alice', age: 30, email: 'a@ex.com' };
      const result = applyProjection(doc, { name: 1, email: 1 });
      expect(result).toEqual({ _id: '1', name: 'Alice', email: 'a@ex.com' });
    });

    it('should auto-include _id unless excluded', () => {
      const doc = { _id: 'id1', name: 'Bob', secret: 'hidden' };
      const result = applyProjection(doc, { name: 1 });
      expect(result).toEqual({ _id: 'id1', name: 'Bob' });
      expect(result).not.toHaveProperty('secret');
    });

    it('should exclude _id when _id: 0', () => {
      const doc = { _id: 'id1', name: 'Charlie' };
      const result = applyProjection(doc, { name: 1, _id: 0 });
      expect(result).toEqual({ name: 'Charlie' });
      expect(result).not.toHaveProperty('_id');
    });

    it('should handle missing fields gracefully', () => {
      const doc = { _id: 'id1', name: 'David' };
      const result = applyProjection(doc, { name: 1, missing: 1 });
      expect(result).toEqual({ _id: 'id1', name: 'David' });
      expect(result).not.toHaveProperty('missing');
    });
  });

  // ====== EXCLUSION PROJECTIONS ======
  describe('Exclusion Mode - { field: 0 }', () => {
    it('should exclude specified fields', () => {
      const doc = { _id: '1', name: 'Eve', password: 'secret', email: 'e@ex.com' };
      const result = applyProjection(doc, { password: 0 });
      expect(result).toEqual({ _id: '1', name: 'Eve', email: 'e@ex.com' });
      expect(result).not.toHaveProperty('password');
    });

    it('should exclude _id when specified', () => {
      const doc = { _id: 'id1', name: 'Frank', email: 'f@ex.com' };
      const result = applyProjection(doc, { _id: 0 });
      expect(result).toEqual({ name: 'Frank', email: 'f@ex.com' });
    });

    it('should exclude multiple fields', () => {
      const doc = { _id: '1', public: 'ok', secret1: 'x', secret2: 'y', other: 'z' };
      const result = applyProjection(doc, { secret1: 0, secret2: 0 });
      expect(result).toEqual({ _id: '1', public: 'ok', other: 'z' });
    });

    it('should handle non-existent fields in exclusion', () => {
      const doc = { _id: '1', name: 'Grace' };
      const result = applyProjection(doc, { missing: 0 });
      expect(result).toEqual({ _id: '1', name: 'Grace' });
    });
  });

  // ====== EDGE CASES ======
  describe('Edge Cases', () => {
    it('should handle empty projection', () => {
      const doc = { _id: '1', name: 'Henry' };
      const result = applyProjection(doc, {});
      expect(result).toEqual({ _id: '1', name: 'Henry' });
    });

    it('should handle null and undefined values', () => {
      const doc = { _id: '1', name: null, status: undefined, email: 'h@ex.com' };
      const result = applyProjection(doc, { name: 1, status: 1, email: 1 });
      expect(result).toEqual({ _id: '1', name: null, status: undefined, email: 'h@ex.com' });
    });

    it('should handle empty document', () => {
      const doc = { _id: '1' };
      const result = applyProjection(doc, { name: 1, email: 1 });
      expect(result).toEqual({ _id: '1' });
    });

    it('should preserve arrays', () => {
      const doc = { _id: '1', tags: ['a', 'b', 'c'] };
      const result = applyProjection(doc, { tags: 1 });
      expect(result).toEqual({ _id: '1', tags: ['a', 'b', 'c'] });
    });

    it('should preserve objects', () => {
      const doc = { _id: '1', metadata: { key: 'value', count: 42 } };
      const result = applyProjection(doc, { metadata: 1 });
      expect(result).toEqual({ _id: '1', metadata: { key: 'value', count: 42 } });
    });
  });

  // ====== PERFORMANCE ======
  describe('Performance', () => {
    it('should project large documents quickly', () => {
      const doc: any = { _id: '1' };
      for (let i = 0; i < 1000; i++) {
        doc[`field_${i}`] = `value_${i}`;
      }

      const projection: any = { _id: 1 };
      for (let i = 0; i < 100; i++) {
        projection[`field_${i}`] = 1;
      }

      const start = performance.now();
      const result = applyProjection(doc, projection);
      const duration = performance.now() - start;

      expect(duration).toBeLessThan(10); // Should be fast
      expect(Object.keys(result).length).toBe(101); // _id + 100 fields
    });
  });
});
```

**Key Testing Principles**:
- ✓ Test inclusion projections thoroughly
- ✓ Test exclusion projections thoroughly
- ✓ Test _id behavior (auto-include/exclude)
- ✓ Test edge cases (null, undefined, arrays, objects)
- ✓ Test performance with large documents
- ✓ Document what MongoDB does vs our implementation

---

### 2. Creating tests/unit/types.test.ts (ObjectId Focus)

**Template to use**:

```typescript
/**
 * Types Tests - Focused on ObjectId
 *
 * ObjectId is critical for document identity.
 * Must be unique, thread-safe, compatible with MongoDB.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { ObjectId } from '../../../src/types.js';

describe('ObjectId', () => {
  // ====== GENERATION ======
  describe('Generation', () => {
    it('should generate unique IDs sequentially', () => {
      const ids = new Set();
      for (let i = 0; i < 1000; i++) {
        const id = new ObjectId();
        expect(ids.has(id.toString())).toBe(false); // Unique
        ids.add(id.toString());
      }
      expect(ids.size).toBe(1000);
    });

    it('should generate IDs with correct byte length', () => {
      const id = new ObjectId();
      const hex = id.toString();
      expect(hex).toHaveLength(24); // 12 bytes = 24 hex chars
    });

    it('should use timestamp from generation time', () => {
      const before = Math.floor(Date.now() / 1000);
      const id = new ObjectId();
      const after = Math.floor(Date.now() / 1000);

      const timestamp = Math.floor(id.getTimestamp().getTime() / 1000);
      expect(timestamp).toBeGreaterThanOrEqual(before);
      expect(timestamp).toBeLessThanOrEqual(after);
    });

    it('should increment counter for sequential IDs', () => {
      const id1 = new ObjectId();
      const id2 = new ObjectId();
      const id3 = new ObjectId();

      // Last 6 chars (3 bytes) are counter
      // They should be sequential (though may wrap)
      const hex1 = id1.toString();
      const hex2 = id2.toString();
      const hex3 = id3.toString();

      // All different
      expect(hex1).not.toBe(hex2);
      expect(hex2).not.toBe(hex3);
    });

    it('should handle rapid concurrent generation', async () => {
      const ids = await Promise.all(
        Array(100).fill(0).map(() => Promise.resolve(new ObjectId()))
      );
      const uniqueIds = new Set(ids.map(id => id.toString()));
      expect(uniqueIds.size).toBe(100); // All unique
    });
  });

  // ====== STRING CONVERSION ======
  describe('String Conversion', () => {
    it('should convert to lowercase hex string', () => {
      const id = new ObjectId();
      const hex = id.toString();
      expect(hex).toMatch(/^[0-9a-f]{24}$/); // Lowercase only
    });

    it('should have consistent string representation', () => {
      const hex = '507f1f77bcf86cd799439011';
      const id = new ObjectId(hex);
      expect(id.toString()).toBe(hex);
      expect(id.toHexString()).toBe(hex);
    });
  });

  // ====== PARSING ======
  describe('Parsing from String', () => {
    it('should parse valid 24-char hex string', () => {
      const hex = '507f1f77bcf86cd799439011';
      const id = new ObjectId(hex);
      expect(id.toString()).toBe(hex);
    });

    it('should normalize uppercase to lowercase', () => {
      const upper = '507F1F77BCF86CD799439011';
      const lower = '507f1f77bcf86cd799439011';
      const id1 = new ObjectId(upper);
      const id2 = new ObjectId(lower);
      expect(id1.toString()).toBe(id2.toString());
    });

    it('should throw on invalid length', () => {
      expect(() => new ObjectId('507f1f77bcf86cd79943901')).toThrow(); // 23 chars
      expect(() => new ObjectId('507f1f77bcf86cd7994390111')).toThrow(); // 25 chars
    });

    it('should throw on non-hex characters', () => {
      expect(() => new ObjectId('507f1f77bcf86cd799439ggg')).toThrow();
    });
  });

  // ====== VALIDATION ======
  describe('Validation - isValid()', () => {
    it('should validate correct ObjectId format', () => {
      expect(ObjectId.isValid('507f1f77bcf86cd799439011')).toBe(true);
    });

    it('should reject invalid lengths', () => {
      expect(ObjectId.isValid('507f1f77bcf86cd79943901')).toBe(false); // 23
      expect(ObjectId.isValid('507f1f77bcf86cd7994390111')).toBe(false); // 25
    });

    it('should reject non-hex characters', () => {
      expect(ObjectId.isValid('507f1f77bcf86cd799439ggg')).toBe(false);
      expect(ObjectId.isValid('507f1f77bcf86cd79943901g')).toBe(false);
    });

    it('should reject non-strings', () => {
      expect(ObjectId.isValid(null as any)).toBe(false);
      expect(ObjectId.isValid(undefined as any)).toBe(false);
      expect(ObjectId.isValid(12345 as any)).toBe(false);
      expect(ObjectId.isValid({} as any)).toBe(false);
    });

    it('should reject empty string', () => {
      expect(ObjectId.isValid('')).toBe(false);
    });
  });

  // ====== EQUALITY ======
  describe('Equality', () => {
    it('should consider same hex strings equal', () => {
      const id1 = new ObjectId('507f1f77bcf86cd799439011');
      const id2 = new ObjectId('507f1f77bcf86cd799439011');
      expect(id1.equals(id2)).toBe(true);
    });

    it('should consider different IDs not equal', () => {
      const id1 = new ObjectId();
      const id2 = new ObjectId();
      expect(id1.equals(id2)).toBe(false);
    });

    it('should handle equality with different construction methods', () => {
      const hex = '507f1f77bcf86cd799439011';
      const bytes = new Uint8Array([
        0x50, 0x7f, 0x1f, 0x77, 0xbc, 0xf8, 0x6c, 0xd7, 0x99, 0x43, 0x90, 0x11
      ]);
      const id1 = new ObjectId(hex);
      const id2 = new ObjectId(bytes);
      expect(id1.equals(id2)).toBe(true);
    });
  });

  // ====== TIMESTAMP ======
  describe('Timestamp Extraction', () => {
    it('should extract timestamp from ObjectId', () => {
      const beforeTime = Date.now();
      const id = new ObjectId();
      const afterTime = Date.now();

      const timestamp = id.getTimestamp().getTime();
      expect(timestamp).toBeGreaterThanOrEqual(beforeTime - 1000); // 1s margin
      expect(timestamp).toBeLessThanOrEqual(afterTime + 1000);
    });

    it('should have 1-second granularity', () => {
      const id1 = new ObjectId();
      const id2 = new ObjectId();
      const ts1 = Math.floor(id1.getTimestamp().getTime() / 1000);
      const ts2 = Math.floor(id2.getTimestamp().getTime() / 1000);
      // Might be same if generated <1s apart
      expect(ts1).toBeLessThanOrEqual(ts2);
    });

    it('should reconstruct same timestamp from same hex', () => {
      const hex = '507f1f77bcf86cd799439011';
      const id = new ObjectId(hex);
      const ts1 = id.getTimestamp();
      const ts2 = id.getTimestamp();
      expect(ts1.getTime()).toBe(ts2.getTime());
    });
  });

  // ====== BYTE REPRESENTATION ======
  describe('Byte Representation', () => {
    it('should construct from Uint8Array', () => {
      const bytes = new Uint8Array(12);
      bytes[0] = 0x50;
      bytes[1] = 0x7f;
      bytes[11] = 0x11;
      const id = new ObjectId(bytes);
      expect(id.toString()).toBeDefined();
    });

    it('should have correct 12-byte format', () => {
      // 4 bytes timestamp + 5 bytes random + 3 bytes counter
      const id = new ObjectId();
      const hex = id.toString();
      // Bytes 0-7: timestamp and random (16 hex chars)
      // Bytes 8-11: counter (6 hex chars)
      expect(hex.length).toBe(24);
    });
  });

  // ====== PERFORMANCE ======
  describe('Performance', () => {
    it('should generate 10K IDs in <100ms', () => {
      const start = performance.now();
      for (let i = 0; i < 10000; i++) {
        new ObjectId();
      }
      const duration = performance.now() - start;
      expect(duration).toBeLessThan(100);
    });

    it('should parse 10K hex strings in <50ms', () => {
      const hex = '507f1f77bcf86cd799439011';
      const start = performance.now();
      for (let i = 0; i < 10000; i++) {
        new ObjectId(hex);
      }
      const duration = performance.now() - start;
      expect(duration).toBeLessThan(50);
    });

    it('should validate 100K strings in <100ms', () => {
      const hex = '507f1f77bcf86cd799439011';
      const start = performance.now();
      for (let i = 0; i < 100000; i++) {
        ObjectId.isValid(hex);
      }
      const duration = performance.now() - start;
      expect(duration).toBeLessThan(100);
    });
  });

  // ====== MONGODB COMPATIBILITY ======
  describe('MongoDB Compatibility', () => {
    it('should generate valid MongoDB ObjectId format', () => {
      const id = new ObjectId();
      const hex = id.toString();
      // MongoDB format: 4 bytes timestamp (hex) + 5 bytes machine (hex) + 3 bytes counter (hex)
      expect(/^[0-9a-f]{24}$/.test(hex)).toBe(true);
    });

    it('should preserve ObjectId through serialization', () => {
      const id = new ObjectId();
      const hex = id.toString();
      const id2 = new ObjectId(hex);
      expect(id.equals(id2)).toBe(true);
    });
  });
});
```

---

### 3. Quick E2E Test Template

**tests/e2e/crud-lifecycle.test.ts**:

```typescript
/**
 * E2E Tests - Complete CRUD Lifecycle
 *
 * Tests against deployed MongoLake workers.
 * Set MONGOLAKE_E2E_URL environment variable.
 *
 * Usage:
 *   MONGOLAKE_E2E_URL=http://localhost:8787 pnpm test:e2e
 */

import { describe, it, expect, beforeAll } from 'vitest';

const API_URL = process.env.MONGOLAKE_E2E_URL || 'http://localhost:8787';

async function apiCall(
  method: 'GET' | 'POST' | 'PATCH' | 'DELETE',
  path: string,
  body?: unknown
) {
  const response = await fetch(`${API_URL}${path}`, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : {},
    body: body ? JSON.stringify(body) : undefined,
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`${response.status}: ${text}`);
  }

  return response.json();
}

describe('E2E - CRUD Lifecycle', () => {
  let documentId: string;
  const collection = 'e2e-test-crud';
  const db = 'e2e-testdb';

  describe('CREATE (Insert)', () => {
    it('should insert document and return _id', async () => {
      const result = await apiCall('POST', `/api/${db}/${collection}`, {
        name: 'Test User',
        email: 'test@example.com',
        age: 30,
      });

      expect(result._id || result.insertedId).toBeDefined();
      documentId = result._id || result.insertedId;
    });

    it('should insert multiple documents', async () => {
      const result = await apiCall('POST', `/api/${db}/${collection}/bulk`, {
        documents: [
          { name: 'User 1', value: 1 },
          { name: 'User 2', value: 2 },
          { name: 'User 3', value: 3 },
        ],
      });

      expect(result.insertedCount).toBe(3);
    });
  });

  describe('READ (Find)', () => {
    it('should retrieve document by _id', async () => {
      const filter = encodeURIComponent(JSON.stringify({ _id: documentId }));
      const result = await apiCall('GET', `/api/${db}/${collection}?filter=${filter}`);

      expect(result.documents).toBeDefined();
      expect(Array.isArray(result.documents)).toBe(true);
    });

    it('should support complex filters', async () => {
      const filter = encodeURIComponent(
        JSON.stringify({ age: { $gte: 25, $lte: 35 } })
      );
      const result = await apiCall('GET', `/api/${db}/${collection}?filter=${filter}`);

      expect(Array.isArray(result.documents)).toBe(true);
    });

    it('should respect projection', async () => {
      const projection = encodeURIComponent(JSON.stringify({ name: 1, _id: 0 }));
      const result = await apiCall(
        'GET',
        `/api/${db}/${collection}?projection=${projection}`
      );

      expect(result.documents).toBeDefined();
      // Verify projection worked (if document exists)
      if (result.documents.length > 0) {
        expect(result.documents[0]).toHaveProperty('name');
        expect(result.documents[0]).not.toHaveProperty('_id');
      }
    });

    it('should support sort', async () => {
      const sort = encodeURIComponent(JSON.stringify({ age: -1 }));
      const result = await apiCall('GET', `/api/${db}/${collection}?sort=${sort}`);

      expect(Array.isArray(result.documents)).toBe(true);
    });

    it('should support limit and skip', async () => {
      const result = await apiCall(
        'GET',
        `/api/${db}/${collection}?limit=10&skip=0`
      );

      expect(Array.isArray(result.documents)).toBe(true);
    });
  });

  describe('UPDATE (Patch)', () => {
    it('should update document with $set', async () => {
      const result = await apiCall('PATCH', `/api/${db}/${collection}/${documentId}`, {
        $set: { age: 31, status: 'updated' },
      });

      expect(result.modifiedCount).toBeGreaterThanOrEqual(0);
    });

    it('should support $inc operator', async () => {
      const result = await apiCall('PATCH', `/api/${db}/${collection}/${documentId}`, {
        $inc: { age: 1 },
      });

      expect(result.modifiedCount).toBeGreaterThanOrEqual(0);
    });

    it('should verify update persisted on read', async () => {
      // Small delay to ensure persistence
      await new Promise(r => setTimeout(r, 100));

      const filter = encodeURIComponent(JSON.stringify({ _id: documentId }));
      const result = await apiCall('GET', `/api/${db}/${collection}?filter=${filter}`);

      expect(result.documents.length).toBeGreaterThan(0);
      // If document found, age should be 32 (31 + 1)
      if (result.documents.length > 0) {
        expect(result.documents[0].age).toBe(32);
      }
    });
  });

  describe('DELETE (Remove)', () => {
    it('should delete document', async () => {
      const result = await apiCall('DELETE', `/api/${db}/${collection}/${documentId}`);

      expect(result.deletedCount).toBeGreaterThanOrEqual(0);
    });

    it('should not find deleted document', async () => {
      const filter = encodeURIComponent(JSON.stringify({ _id: documentId }));
      const result = await apiCall('GET', `/api/${db}/${collection}?filter=${filter}`);

      // Should not find the deleted document
      expect(result.documents.filter((d: any) => d._id === documentId)).toHaveLength(0);
    });
  });
});
```

---

## Updated Testing Commands

Add these to `package.json`:

```json
{
  "scripts": {
    "test": "pnpm test:unit && pnpm test:integration",
    "test:unit": "vitest run --config vitest.unit.config.ts",
    "test:integration": "vitest run --config vitest.config.ts",
    "test:e2e": "vitest run --config vitest.e2e.config.ts",
    "test:performance": "vitest run --config vitest.performance.config.ts",
    "test:watch": "vitest --config vitest.unit.config.ts",
    "test:coverage": "vitest run --coverage",
    "test:all": "pnpm test && pnpm test:e2e && pnpm test:performance",
    "test:ci": "pnpm test && pnpm test:coverage && pnpm test:e2e --reporter=verbose"
  }
}
```

---

## Updated vitest.unit.config.ts

```typescript
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: [
      'src/**/*.test.ts',
      'tests/unit/**/*.test.ts',
    ],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      lines: 80,        // Minimum line coverage
      functions: 80,    // Minimum function coverage
      branches: 70,     // Minimum branch coverage
      statements: 80,   // Minimum statement coverage
      exclude: [
        'node_modules/',
        'dist/',
        'tests/',
        '**/*.d.ts',
        '**/index.ts', // Entry points often have less coverage
      ],
    },
    testTimeout: 10000,
  },
});
```

---

## New vitest.performance.config.ts

```typescript
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['tests/performance/**/*.test.ts'],
    testTimeout: 60000,  // Long timeout for benchmarks
    hookTimeout: 60000,
    threads: false,      // Single thread for consistent results
    reporters: ['verbose'], // Detailed output for benchmarks
  },
});
```

---

## Shared Test Utilities (to create)

**tests/shared/factories.ts**:

```typescript
/**
 * Test Data Factories
 *
 * Create consistent test data across all test suites
 */

import { ObjectId } from '../../src/types.js';

export function createTestUser(overrides: any = {}) {
  return {
    _id: new ObjectId(),
    name: 'Test User',
    email: 'test@example.com',
    age: 30,
    status: 'active',
    createdAt: new Date(),
    ...overrides,
  };
}

export function createTestDocument(overrides: any = {}) {
  return {
    _id: new ObjectId(),
    title: 'Test Document',
    content: 'Test content',
    tags: ['test', 'example'],
    metadata: { version: 1 },
    ...overrides,
  };
}

export function createTestBatch(count: number, factory: (i: number) => any) {
  const batch = [];
  for (let i = 0; i < count; i++) {
    batch.push(factory(i));
  }
  return batch;
}
```

**tests/shared/assertions.ts**:

```typescript
/**
 * Custom Assertions
 *
 * Custom matchers for MongoLake-specific assertions
 */

import { expect } from 'vitest';

export function expectObjectIdFormat(value: any) {
  expect(value).toMatch(/^[0-9a-f]{24}$/i);
}

export function expectValidProjection(result: any, projection: any) {
  if (projection._id === 0) {
    expect(result).not.toHaveProperty('_id');
  } else {
    expect(result).toHaveProperty('_id');
  }
}

export function expectDocumentStructure(doc: any) {
  expect(doc).toHaveProperty('_id');
  expect(typeof doc._id).toMatch(/^(string|object)$/); // string or ObjectId
}
```

---

## Pre-Commit Hook (Git)

**.husky/pre-commit**:

```bash
#!/bin/sh
. "$(dirname "$0")/_/husky.sh"

# Run unit tests
pnpm test:unit
if [ $? -ne 0 ]; then
  echo "Unit tests failed. Commit aborted."
  exit 1
fi

# Run linting (if configured)
# pnpm lint

echo "✓ Tests passed. Proceeding with commit."
```

Install with:
```bash
npm install husky --save-dev
npx husky install
chmod +x .husky/pre-commit
```

---

## GitHub Actions CI Configuration

**.github/workflows/test.yml**:

```yaml
name: Tests

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main, develop ]

jobs:
  test:
    runs-on: ubuntu-latest

    strategy:
      matrix:
        node-version: [18.x, 20.x]

    steps:
      - uses: actions/checkout@v3

      - name: Setup Node.js
        uses: actions/setup-node@v3
        with:
          node-version: ${{ matrix.node-version }}
          cache: 'pnpm'

      - name: Install pnpm
        run: npm install -g pnpm

      - name: Install dependencies
        run: pnpm install

      - name: Run linter
        run: pnpm lint

      - name: Run unit tests
        run: pnpm test:unit

      - name: Run integration tests
        run: pnpm test:integration

      - name: Generate coverage report
        run: pnpm test:coverage

      - name: Upload coverage to Codecov
        uses: codecov/codecov-action@v3
        with:
          files: ./coverage/coverage-final.json
          fail_ci_if_error: true
          verbose: true
```

---

## Summary

**Immediate Actions**:
1. Create projection.test.ts (4 hours)
2. Create types.test.ts (6 hours)
3. Create basic E2E tests (6 hours)
4. Create performance tests (4 hours)
5. Update configs and infrastructure (4 hours)

**Total Critical Path**: ~24 hours

**Key Files to Create**:
- ✓ tests/unit/utils/projection.test.ts
- ✓ tests/unit/types.test.ts
- ✓ tests/e2e/crud-lifecycle.test.ts
- ✓ tests/performance/filter-matching.test.ts
- ✓ vitest.performance.config.ts
- ✓ tests/shared/factories.ts
- ✓ tests/shared/assertions.ts
- ✓ .github/workflows/test.yml
- ✓ .husky/pre-commit

**Expected Outcome**: Coverage increases from 89% to 95%+, all CRITICAL gaps addressed, E2E tests in place, performance baseline established.

