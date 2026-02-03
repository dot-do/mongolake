# Testing Standards and Patterns

This document defines the testing standards, conventions, and best practices for the MongoLake codebase.

## Table of Contents

- [Test File Naming Conventions](#test-file-naming-conventions)
- [Test Organization](#test-organization)
- [Fixture Usage Patterns](#fixture-usage-patterns)
- [Mock Usage Guidelines](#mock-usage-guidelines)
- [Test Types: Unit vs Integration vs E2E](#test-types-unit-vs-integration-vs-e2e)
- [Assertion Best Practices](#assertion-best-practices)
- [Coverage Expectations](#coverage-expectations)
- [Running Tests](#running-tests)

---

## Test File Naming Conventions

### File Naming

All test files use the `.test.ts` extension:

```
<feature-name>.test.ts
```

### Location Patterns

Tests can be organized in two ways:

**1. Centralized tests directory (preferred for larger modules):**
```
tests/
  unit/
    client/mongolake.test.ts
    parquet/footer-parser.test.ts
    storage/range-handler.test.ts
  integration/
    worker.test.ts
  e2e/
    crud.test.ts
```

**2. Co-located with source code (for tightly coupled unit tests):**
```
src/
  parquet/
    __tests__/
      variant.test.ts
      zone-map.test.ts
    footer-parser.ts
    zone-map.ts
```

### Naming Guidelines

- Name test files after the module or feature being tested
- Use descriptive names that match the source file: `footer-parser.ts` -> `footer-parser.test.ts`
- Group related tests in the same directory structure as source code
- For cross-cutting concerns, use descriptive names: `read-your-writes.test.ts`, `path-traversal.test.ts`

---

## Test Organization

### Describe/It Structure

Tests use Vitest's `describe` and `it` blocks with a consistent hierarchy:

```typescript
import { describe, it, expect, beforeEach } from 'vitest';

describe('ModuleName', () => {
  // Setup shared across all tests in this describe block
  beforeEach(() => {
    // Reset state, create fixtures, etc.
  });

  // Group related functionality
  describe('methodName()', () => {
    it('should handle the happy path', () => {
      // Test implementation
    });

    it('should handle edge case X', () => {
      // Test implementation
    });

    it('should throw on invalid input', () => {
      // Test implementation
    });
  });

  describe('anotherMethod()', () => {
    // More tests
  });
});
```

### Test Structure Guidelines

**1. Use descriptive `describe` blocks:**
```typescript
// Good: Describes the component and method
describe('FooterParser', () => {
  describe('parse()', () => { ... });
  describe('parseFromTail()', () => { ... });
});

// Good: Groups by functionality
describe('MongoLake', () => {
  describe('constructor', () => { ... });
  describe('db()', () => { ... });
  describe('listDatabases()', () => { ... });
});
```

**2. Use descriptive `it` statements starting with "should":**
```typescript
// Good
it('should parse a minimal valid Parquet footer', () => { ... });
it('should throw on missing end magic bytes', () => { ... });
it('should cache and reuse Database instances', () => { ... });

// Avoid
it('parse works', () => { ... });
it('error test', () => { ... });
```

**3. Group tests by behavior category:**
```typescript
describe('Extract schema', () => {
  it('should extract schema with column names', () => { ... });
  it('should extract schema with correct column types', () => { ... });
  it('should extract schema with repetition types', () => { ... });
});

describe('Handle truncated/invalid footer', () => {
  it('should throw on missing end magic bytes', () => { ... });
  it('should throw on truncated footer data', () => { ... });
});
```

### File Header Comments

Include a descriptive header comment explaining what the test file covers:

```typescript
/**
 * Parquet Footer Parser Tests
 *
 * Comprehensive tests for parsing Parquet file footers.
 * The footer contains the file metadata including schema,
 * row group metadata, and column chunk locations/statistics.
 */
```

---

## Fixture Usage Patterns

### Using Test Utilities

Import fixtures and factories from `tests/utils/index.ts`:

```typescript
import {
  // Factories
  createUser,
  createObjectId,
  createDeduplicationDoc,

  // Mocks
  createMockR2Bucket,
  createMockEnv,
  createMockFetch,

  // Assertions
  assertDocumentId,
  assertInsertSuccess,
  assertSortedBy,

  // Fixtures
  USERS,
  PRODUCTS,
  FILTERS,
} from '../utils/index.js';
```

### Fixtures (`tests/utils/fixtures.ts`)

Static test data for consistent testing:

```typescript
// Pre-defined ObjectIds
import { OBJECT_IDS, getObjectIdInstance } from '../utils/index.js';
const testId = OBJECT_IDS.TEST_1;  // '507f1f77bcf86cd799439011'

// Pre-defined user documents
import { USERS } from '../utils/index.js';
const alice = USERS.alice;  // Complete user document

// Filter patterns
import { FILTERS } from '../utils/index.js';
const filter = FILTERS.comparison.greaterThan('age', 25);  // { age: { $gt: 25 } }

// Update patterns
import { UPDATES } from '../utils/index.js';
const update = UPDATES.set({ name: 'New Name' });  // { $set: { name: 'New Name' } }
```

### Factories (`tests/utils/factories.ts`)

Generate test documents dynamically:

```typescript
import { createUser, createProduct, createOrder } from '../utils/index.js';

// Create with defaults
const user = createUser();

// Create with overrides
const customUser = createUser({
  name: 'Custom Name',
  age: 35,
  status: 'active',
});

// Create multiple documents
const users = createUsers(10);

// Create nested documents
const nested = createNestedDocument(3);  // 3 levels deep

// Reset counter between tests for predictable IDs
import { resetDocumentCounter } from '../utils/index.js';
beforeEach(() => {
  resetDocumentCounter();
});
```

### Test-Specific Fixtures

For complex test scenarios, define fixtures locally:

```typescript
/**
 * Creates a minimal valid Parquet file buffer with footer
 */
function createMinimalParquetBuffer(): Uint8Array {
  const startMagic = new TextEncoder().encode('PAR1');
  // ... build buffer
  return buffer;
}

/**
 * Creates a Parquet buffer with specific schema
 */
function createParquetBufferWithSchema(columns: SchemaColumn[]): Uint8Array {
  // ... implementation
}
```

---

## Mock Usage Guidelines

### Storage Mocks

```typescript
import {
  createMockStorage,
  createMockR2Bucket,
  createSpiedR2Bucket,
  createMockDurableObjectStorage,
} from '../utils/index.js';

// Simple in-memory storage
const storage = createMockStorage();
await storage.put('key', new Uint8Array([1, 2, 3]));
const data = await storage.get('key');

// Mock R2 bucket with spies
const bucket = createSpiedR2Bucket();
// ... use bucket
expect(bucket.get).toHaveBeenCalledWith('some-key');

// Mock Durable Object storage
const doStorage = createMockDurableObjectStorage();
await doStorage.put('key', { value: 42 });
await doStorage.setAlarm(Date.now() + 5000);
```

### Network Mocks

```typescript
import { createMockFetch, installFetchMock, restoreFetch } from '../utils/index.js';

// Create mock fetch
const mockFetch = createMockFetch();

// Mock specific responses
mockFetch.mockJsonResponse({ data: 'test' }, 200);
mockFetch.mockError(new Error('Network error'));
mockFetch.mockTimeout(5000);

// Install globally
const mock = installFetchMock();
// ... run tests
restoreFetch();  // Clean up
```

### Timer Mocks

```typescript
import { createMockTimers } from '../utils/index.js';

const timers = createMockTimers();

timers.useFakeTimers();
// ... schedule timers
await timers.advanceTime(5000);
await timers.runAllTimers();
timers.useRealTimers();
```

### Environment Mocks

```typescript
import { createMockEnv, createMockRequest } from '../utils/index.js';

// Mock MongoLake worker environment
const env = createMockEnv({
  requireAuth: true,
  environment: 'development',
});

// Create mock HTTP requests
const request = createMockRequest('POST', '/api/db/collection', { name: 'test' });
```

### When to Use Mocks

- **Do mock:** External services (R2, Durable Objects), network calls, timers
- **Do mock:** Dependencies that have side effects or are slow
- **Don't mock:** The unit under test itself
- **Don't mock:** Simple data transformations or pure functions

---

## Test Types: Unit vs Integration vs E2E

### Unit Tests

**Location:** `tests/unit/` or `src/**/__tests__/`

**Purpose:** Test individual functions, classes, or modules in isolation.

**Characteristics:**
- Run in Node.js environment
- Use mocks for all external dependencies
- Fast execution (milliseconds)
- High coverage of edge cases

**Example:**
```typescript
// tests/unit/parquet/footer-parser.test.ts
import { describe, it, expect } from 'vitest';
import { FooterParser, InvalidMagicBytesError } from '../../../src/parquet/footer-parser.js';

describe('FooterParser', () => {
  it('should parse a minimal valid Parquet footer', () => {
    const parser = new FooterParser();
    const buffer = createMinimalParquetBuffer();
    const footer = parser.parse(buffer);

    expect(footer.version).toBeDefined();
    expect(footer.schema).toBeDefined();
  });

  it('should throw on invalid magic bytes', () => {
    const parser = new FooterParser();
    const invalidBuffer = new Uint8Array([0, 0, 0, 0]);

    expect(() => parser.parse(invalidBuffer)).toThrow(InvalidMagicBytesError);
  });
});
```

**Run:** `npm run test:unit` or `npx vitest --config vitest.unit.config.ts`

### Integration Tests

**Location:** `tests/integration/`

**Purpose:** Test components working together in the Cloudflare Workers runtime.

**Characteristics:**
- Run in `@cloudflare/vitest-pool-workers`
- Use real Cloudflare bindings (R2, Durable Objects) via Miniflare
- Test Worker request handling end-to-end
- Moderate execution time

**Example:**
```typescript
// tests/integration/worker.test.ts
import { describe, it, expect } from 'vitest';
import { env, SELF } from 'cloudflare:test';

describe('MongoLake Worker Integration', () => {
  it('should insert a document', async () => {
    const doc = { name: 'Test User', email: 'test@example.com' };

    const response = await SELF.fetch('https://mongolake.test/api/testdb/users', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(doc),
    });

    expect(response.status).toBe(201);
    const result = await response.json();
    expect(result.insertedId).toBeDefined();
  });
});
```

**Run:** `npm run test:integration` or `npx vitest --config vitest.config.ts`

### E2E Tests

**Location:** `tests/e2e/`

**Purpose:** Test the deployed system against real infrastructure.

**Characteristics:**
- Run against a deployed MongoLake worker
- Test full stack: Worker -> Durable Objects -> R2
- Verify production-like behavior
- Slowest execution (network latency)

**Example:**
```typescript
// tests/e2e/crud.test.ts
import { describe, it, expect, beforeAll } from 'vitest';

const BASE_URL = process.env.MONGOLAKE_E2E_URL || 'http://localhost:8787';

describe('MongoLake E2E Tests', () => {
  beforeAll(async () => {
    // Verify worker is accessible
    const health = await fetch(`${BASE_URL}/health`);
    if (!health.ok) throw new Error(`Worker not accessible at ${BASE_URL}`);
  });

  it('should insert and query a document', async () => {
    const doc = { _id: `test-${Date.now()}`, name: 'E2E Test' };

    // Insert
    const insertRes = await fetch(`${BASE_URL}/api/testdb/collection`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(doc),
    });
    expect(insertRes.status).toBe(201);

    // Query back
    const queryRes = await fetch(
      `${BASE_URL}/api/testdb/collection?filter=${encodeURIComponent(JSON.stringify({ _id: doc._id }))}`
    );
    expect(queryRes.status).toBe(200);
  });
});
```

**Run:** `MONGOLAKE_E2E_URL=https://your-worker.workers.dev npm run test:e2e`

### Choosing Test Types

| Scenario | Test Type |
|----------|-----------|
| Pure function logic | Unit |
| Class method behavior | Unit |
| Error handling | Unit |
| Database operations with mocks | Unit |
| Worker request routing | Integration |
| Durable Object behavior | Integration |
| R2 storage operations | Integration |
| Full API workflows | E2E |
| Cross-service communication | E2E |
| Production deployment verification | E2E |

---

## Assertion Best Practices

### Custom Assertions

Use domain-specific assertions from `tests/utils/assertions.ts`:

```typescript
import {
  assertDocumentId,
  assertInsertSuccess,
  assertUpdateSuccess,
  assertSortedBy,
  assertThrowsAsync,
  assertCompletesWithin,
} from '../utils/index.js';

// Document assertions
assertDocumentId(result, expectedId);
assertDocumentFields(doc, { name: 'Alice', age: 30 });

// Operation result assertions
assertInsertSuccess(result, expectedId);
assertUpdateSuccess(result, 1, 1);  // matchedCount, modifiedCount
assertDeleteSuccess(result, 1);     // deletedCount

// Collection assertions
assertSortedBy(documents, 'createdAt', 'desc');
assertAllMatch(documents, doc => doc.status === 'active');
assertUniqueIds(documents);

// Async assertions
await assertThrowsAsync(() => parser.parse(invalid), 'Invalid');
await assertCompletesWithin(() => query.execute(), 1000);  // 1 second
```

### Standard Vitest Assertions

```typescript
// Equality
expect(value).toBe(expected);           // Strict equality
expect(object).toEqual(expected);       // Deep equality
expect(value).toBeCloseTo(3.14, 2);     // Floating point

// Truthiness
expect(value).toBeDefined();
expect(value).not.toBeNull();
expect(array).toBeTruthy();

// Numbers
expect(count).toBeGreaterThan(0);
expect(count).toBeLessThanOrEqual(100);

// Strings
expect(str).toContain('substring');
expect(str).toMatch(/pattern/);

// Arrays
expect(array).toHaveLength(3);
expect(array).toContain(item);

// Errors
expect(() => fn()).toThrow(ErrorClass);
expect(() => fn()).toThrow('message');
await expect(asyncFn()).rejects.toThrow();
```

### Assertion Guidelines

1. **Be specific:** Test exact values when possible
2. **Add context:** Use custom error messages for complex assertions
3. **Test one thing:** Each `it` block should verify one behavior
4. **Check edge cases:** null, undefined, empty arrays, boundary values
5. **Verify both success and failure paths**

---

## Coverage Expectations

### Coverage Thresholds

The project enforces minimum coverage thresholds (configured in `vitest.unit.config.ts`):

| Metric | Threshold |
|--------|-----------|
| Lines | 80% |
| Branches | 80% |
| Functions | 80% |
| Statements | 80% |

### Running Coverage

```bash
# Unit test coverage
npm run test:unit -- --coverage

# View HTML report
open coverage/index.html
```

### Coverage Guidelines

1. **Focus on meaningful coverage:** Don't write tests just to hit numbers
2. **Cover critical paths:** Error handling, edge cases, security-sensitive code
3. **Skip generated code:** Type definitions, auto-generated files
4. **Test boundary conditions:** Off-by-one, empty inputs, maximum values

### Files Excluded from Coverage

```typescript
exclude: [
  'node_modules/**',
  'dist/**',
  'tests/**',
  '**/*.d.ts',
  '**/*.test.ts',
]
```

---

## Running Tests

### npm Scripts

```bash
# Run unit tests
npm run test:unit

# Run integration tests (Cloudflare Workers)
npm run test:integration

# Run E2E tests
MONGOLAKE_E2E_URL=https://your-worker.workers.dev npm run test:e2e

# Run all tests
npm test

# Watch mode
npm run test:unit -- --watch

# Coverage report
npm run test:unit -- --coverage

# Run specific test file
npm run test:unit -- tests/unit/parquet/footer-parser.test.ts

# Run tests matching pattern
npm run test:unit -- -t "should parse"
```

### Direct Vitest Commands

```bash
# Unit tests with config
npx vitest --config vitest.unit.config.ts

# Integration tests with config
npx vitest --config vitest.config.ts

# E2E tests with config
npx vitest --config vitest.e2e.config.ts

# UI mode
npx vitest --ui
```

### CI/CD Integration

Tests run automatically on:
- Pull request creation
- Push to main branch
- Manual workflow trigger

Coverage reports are uploaded to the CI artifacts.

---

## Summary

| Aspect | Standard |
|--------|----------|
| File naming | `*.test.ts` |
| Test structure | `describe` / `it` with "should" statements |
| Fixtures | Import from `tests/utils/index.js` |
| Mocks | Use provided mock factories |
| Unit tests | `tests/unit/` or `src/**/__tests__/` |
| Integration tests | `tests/integration/` |
| E2E tests | `tests/e2e/` |
| Coverage | 80% minimum for lines, branches, functions |
