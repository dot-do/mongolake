# MongoLake Testing Guide

This document defines the testing standards, conventions, patterns, and best practices for the MongoLake codebase.

## Table of Contents

- [Quick Start](#quick-start)
- [Test Types Overview](#test-types-overview)
- [Running Tests](#running-tests)
- [File Organization](#file-organization)
- [Naming Conventions](#naming-conventions)
- [Test Structure](#test-structure)
- [Mock and Stub Patterns](#mock-and-stub-patterns)
- [Fixture Patterns](#fixture-patterns)
- [Coverage Requirements](#coverage-requirements)
- [Writing Tests](#writing-tests)
- [Configuration Files](#configuration-files)

---

## Quick Start

```bash
# Run all tests (unit + integration)
pnpm test

# Run unit tests only
pnpm test:unit

# Run integration tests only
pnpm test:integration

# Run E2E tests (requires deployed worker)
MONGOLAKE_E2E_URL=https://mongolake.workers.dev pnpm test:e2e

# Run tests in watch mode
pnpm test:watch

# Run tests with coverage report
pnpm test:coverage
```

---

## Test Types Overview

MongoLake uses three types of tests, each serving a different purpose:

### Unit Tests

**Purpose:** Test individual functions, classes, or modules in isolation.

| Aspect | Details |
|--------|---------|
| Location | `tests/unit/` or `src/**/__tests__/` |
| Environment | Node.js |
| Runtime | Fast (milliseconds) |
| Dependencies | All external dependencies mocked |
| Config | `vitest.unit.config.ts` |

**When to use:**
- Testing pure functions and business logic
- Testing error handling and edge cases
- Testing class methods in isolation
- Testing data transformations

### Integration Tests

**Purpose:** Test components working together in the Cloudflare Workers runtime.

| Aspect | Details |
|--------|---------|
| Location | `tests/integration/` |
| Environment | `@cloudflare/vitest-pool-workers` |
| Runtime | Moderate (seconds) |
| Dependencies | Real Cloudflare bindings via Miniflare |
| Config | `vitest.config.ts` |

**When to use:**
- Testing Worker request handling
- Testing Durable Object behavior
- Testing R2 storage operations
- Testing multiple components together

### E2E Tests

**Purpose:** Test the full system against deployed infrastructure.

| Aspect | Details |
|--------|---------|
| Location | `tests/e2e/` |
| Environment | Node.js with network calls |
| Runtime | Slow (network latency) |
| Dependencies | Real deployed worker |
| Config | `vitest.e2e.config.ts` |

**When to use:**
- Verifying production-like behavior
- Testing full API workflows
- Cross-service communication
- Deployment verification

---

## Running Tests

### npm/pnpm Scripts

| Script | Description |
|--------|-------------|
| `pnpm test` | Run unit + integration tests |
| `pnpm test:unit` | Run unit tests only |
| `pnpm test:integration` | Run integration tests in Workers runtime |
| `pnpm test:e2e` | Run E2E tests (requires `MONGOLAKE_E2E_URL`) |
| `pnpm test:watch` | Run unit tests in watch mode |
| `pnpm test:coverage` | Run unit tests with coverage report |
| `pnpm test:load` | Run load tests |
| `pnpm test:compat` | Run compatibility tests |
| `pnpm test:perf` | Run performance tests |

### Direct Vitest Commands

```bash
# Run with specific config
npx vitest --config vitest.unit.config.ts

# Run specific test file
npx vitest tests/unit/parquet/footer-parser.test.ts

# Run tests matching pattern
npx vitest -t "should parse"

# Run in UI mode
npx vitest --ui

# Run with verbose output
npx vitest --reporter=verbose
```

### E2E Test Environment

E2E tests require a running MongoLake worker:

```bash
# Against deployed worker
MONGOLAKE_E2E_URL=https://mongolake.workers.dev pnpm test:e2e

# Against local development
MONGOLAKE_E2E_URL=http://localhost:8787 pnpm test:e2e
```

---

## File Organization

### Directory Structure

```
tests/
├── unit/                    # Unit tests organized by module
│   ├── auth/               # Auth module tests
│   │   ├── bearer-token.test.ts
│   │   ├── api-key.test.ts
│   │   └── test-helpers.ts # Module-specific test helpers
│   ├── client/             # Client tests
│   ├── do/                 # Durable Object tests
│   ├── parquet/            # Parquet parsing tests
│   ├── storage/            # Storage layer tests
│   ├── wire-protocol/      # Wire protocol tests
│   └── ...
├── integration/            # Integration tests
│   ├── worker.test.ts
│   ├── wire-protocol.test.ts
│   └── client-storage.test.ts
├── e2e/                    # End-to-end tests
│   ├── crud.test.ts
│   ├── crud-lifecycle.test.ts
│   └── aggregation.test.ts
├── utils/                  # Shared test utilities
│   ├── index.ts           # Re-exports all utilities
│   ├── factories.ts       # Document/object factories
│   ├── fixtures.ts        # Static test data
│   ├── mocks.ts           # Mock implementations
│   ├── assertions.ts      # Custom assertions
│   └── mock-socket.ts     # Socket mock for wire protocol
├── benchmark/              # Performance benchmarks
└── load/                   # Load tests
```

### Co-located Tests

For tightly coupled unit tests, use `__tests__` directories next to source:

```
src/
├── parquet/
│   ├── __tests__/
│   │   ├── variant.test.ts
│   │   ├── zone-map.test.ts
│   │   └── row-group.test.ts
│   ├── footer-parser.ts
│   ├── variant.ts
│   └── zone-map.ts
└── branching/
    ├── __tests__/
    │   ├── metadata.test.ts
    │   └── switching.test.ts
    └── branch-manager.ts
```

---

## Naming Conventions

### Test File Naming

All test files use the `.test.ts` extension:

| Source File | Test File |
|-------------|-----------|
| `footer-parser.ts` | `footer-parser.test.ts` |
| `shard.ts` | `shard-buffer.test.ts`, `shard-query.test.ts` |
| `variant.ts` | `variant.test.ts` |

### Test Naming Guidelines

- Name test files after the module or feature being tested
- Use descriptive names that indicate what's being tested
- For cross-cutting concerns: `read-your-writes.test.ts`, `path-traversal.test.ts`
- For feature areas: `shard-buffer.test.ts`, `shard-compaction.test.ts`

### Test Helper Naming

Module-specific test helpers are named `test-helpers.ts`:

```
tests/unit/auth/test-helpers.ts
tests/unit/do/test-helpers.ts
```

---

## Test Structure

### Basic Structure

```typescript
/**
 * ModuleName Tests
 *
 * Brief description of what this test file covers.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

describe('ModuleName', () => {
  // Setup shared across all tests
  let instance: ModuleName;

  beforeEach(() => {
    instance = new ModuleName();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('methodName()', () => {
    it('should handle the happy path', () => {
      const result = instance.methodName('input');
      expect(result).toBe('expected');
    });

    it('should handle edge case X', () => {
      const result = instance.methodName('');
      expect(result).toBeNull();
    });

    it('should throw on invalid input', () => {
      expect(() => instance.methodName(null)).toThrow('Invalid input');
    });
  });
});
```

### Describe Block Guidelines

Group tests logically by functionality:

```typescript
// Good: Descriptive grouping
describe('FooterParser', () => {
  describe('parse()', () => { /* ... */ });
  describe('parseFromTail()', () => { /* ... */ });
});

describe('ShardDO - Buffer Documents in Memory', () => {
  it('should accept a single document write', async () => { /* ... */ });
  it('should buffer documents before flushing to R2', async () => { /* ... */ });
});

describe('ShardDO - Flush Buffer to R2', () => {
  it('should flush buffer when size threshold is reached', async () => { /* ... */ });
  it('should write Parquet format to R2', async () => { /* ... */ });
});
```

### It Block Guidelines

Test descriptions should start with "should":

```typescript
// Good
it('should parse a minimal valid Parquet footer', () => { /* ... */ });
it('should throw on missing end magic bytes', () => { /* ... */ });
it('should accept valid Bearer token in Authorization header', async () => { /* ... */ });
it('should reject expired tokens with 401', async () => { /* ... */ });

// Avoid
it('parse works', () => { /* ... */ });
it('error test', () => { /* ... */ });
it('test1', () => { /* ... */ });
```

### File Header Comments

Include descriptive header comments:

```typescript
/**
 * Auth Middleware Bearer Token Validation Tests
 *
 * Tests for Bearer token validation and expiration handling.
 */

/**
 * S3 AWS Signature Version 4 Signing Tests
 *
 * Tests for the AWS SigV4 signing implementation:
 * - toHex conversion
 * - sha256Hex hashing
 * - hmacSha256 computation
 * - getSigningKey derivation
 *
 * Uses AWS test vectors where available to verify correctness.
 */
```

---

## Mock and Stub Patterns

### Available Mocks

Import mocks from `tests/utils/mocks.ts`:

```typescript
import {
  // Storage mocks
  createMockStorage,
  createMockR2Bucket,
  createSpiedR2Bucket,
  createMockDurableObjectStorage,

  // Network mocks
  createMockFetch,
  installFetchMock,
  restoreFetch,

  // Timer mocks
  createMockTimers,

  // Environment mocks
  createMockEnv,
  createMockRequest,
  createWebSocketRequest,

  // Event emitter mocks
  createMockEventEmitter,
} from '../utils/mocks.js';
```

### Storage Mocks

```typescript
// In-memory storage (simple key-value)
const storage = createMockStorage();
await storage.put('key', new Uint8Array([1, 2, 3]));
const data = await storage.get('key');

// Mock R2 bucket with full API
const bucket = createMockR2Bucket();
await bucket.put('test.parquet', parquetData);
const object = await bucket.get('test.parquet');

// Spied R2 bucket (tracks calls)
const spiedBucket = createSpiedR2Bucket();
await spiedBucket.put('key', data);
expect(spiedBucket.put).toHaveBeenCalledWith('key', data);
```

### Durable Object Storage Mock

```typescript
const doStorage = createMockDurableObjectStorage();

// Key-value operations
await doStorage.put('key', { value: 42 });
const value = await doStorage.get('key');

// SQLite operations
doStorage.sql.exec('CREATE TABLE test (id INTEGER PRIMARY KEY)');

// Alarm operations
await doStorage.setAlarm(Date.now() + 5000);
const alarm = await doStorage.getAlarm();
```

### Module-Specific Test Helpers

Create dedicated test helpers for complex modules:

```typescript
// tests/unit/do/test-helpers.ts
import { vi } from 'vitest';

export function createMockState(): DurableObjectState {
  return {
    id: {
      toString: () => 'test-shard-id',
      equals: (other) => other.toString() === 'test-shard-id',
      name: 'test-shard',
    },
    storage: createMockStorage(),
    waitUntil: vi.fn(),
    blockConcurrencyWhile: vi.fn(async (closure) => closure()),
  };
}

export function createMockEnv(bucket?: R2Bucket): ShardDOEnv {
  return {
    DATA_BUCKET: bucket || createMockR2Bucket(),
    SHARD_DO: {} as DurableObjectNamespace,
  };
}

export function createTestDocument(overrides = {}) {
  return {
    _id: `doc-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    name: 'Test User',
    age: 25,
    tags: ['test'],
    ...overrides,
  };
}
```

### Network Mocks

```typescript
const mockFetch = createMockFetch();

// Mock specific responses
mockFetch.mockJsonResponse({ data: 'test' }, 200);
mockFetch.mockResponse(404, { error: 'Not found' });
mockFetch.mockError(new Error('Network error'));
mockFetch.mockTimeout(5000);

// Check calls
expect(mockFetch.calls).toHaveLength(1);
expect(mockFetch.calls[0].url).toBe('https://api.example.com/data');
```

### Timer Mocks

```typescript
const timers = createMockTimers();

timers.useFakeTimers();

// Advance time
await timers.advanceTime(5000);
await timers.advanceToNextTimer();
await timers.runAllTimers();

timers.useRealTimers();
```

### When to Mock

| Mock | Don't Mock |
|------|------------|
| External services (R2, DO, APIs) | The unit under test |
| Network calls | Simple pure functions |
| Timers and dates | Data transformations |
| Dependencies with side effects | Value objects |

---

## Fixture Patterns

### Using Factories

Import factories from `tests/utils/factories.ts`:

```typescript
import {
  // ObjectId factories
  createObjectId,
  createObjectIdString,
  createObjectIdFromDate,

  // Date factories
  createDate,
  createPastDate,
  createFutureDate,

  // Document factories
  createUser,
  createUsers,
  createOrder,
  createProduct,
  createDeduplicationDoc,

  // Utility
  resetDocumentCounter,
} from '../utils/factories.js';
```

### Document Factories

```typescript
// Create with defaults
const user = createUser();
// { _id: 'user-1', name: 'Test User 1', email: 'user1@example.com', ... }

// Create with overrides
const customUser = createUser({
  name: 'Alice',
  age: 35,
  status: 'active',
});

// Create multiple
const users = createUsers(10);

// Reset counter for predictable IDs
beforeEach(() => {
  resetDocumentCounter();
});
```

### Static Fixtures

Import static fixtures from `tests/utils/fixtures.ts`:

```typescript
import {
  // Pre-defined ObjectIds
  OBJECT_IDS,

  // Pre-defined documents
  USERS,
  PRODUCTS,

  // Filter patterns
  FILTERS,

  // Update patterns
  UPDATES,
} from '../utils/fixtures.js';

// Usage
const testId = OBJECT_IDS.TEST_1;
const filter = FILTERS.comparison.greaterThan('age', 25);
const update = UPDATES.set({ name: 'New Name' });
```

### Test-Specific Fixtures

For complex scenarios, define fixtures locally:

```typescript
/**
 * Creates a minimal valid Parquet file buffer
 */
function createMinimalParquetBuffer(): Uint8Array {
  const startMagic = new TextEncoder().encode('PAR1');
  const endMagic = new TextEncoder().encode('PAR1');
  // ... build buffer
  return buffer;
}

describe('FooterParser', () => {
  const buffer = createMinimalParquetBuffer();

  it('should parse the buffer', () => {
    const footer = parser.parse(buffer);
    expect(footer).toBeDefined();
  });
});
```

---

## Coverage Requirements

### Thresholds

The project enforces minimum coverage thresholds (configured in `vitest.unit.config.ts`):

| Metric | Threshold |
|--------|-----------|
| Lines | 80% |
| Branches | 80% |
| Functions | 80% |
| Statements | 80% |

### Running Coverage

```bash
# Generate coverage report
pnpm test:coverage

# View HTML report
open coverage/index.html
```

### Excluded from Coverage

```typescript
// vitest.unit.config.ts
exclude: [
  'node_modules/**',
  'dist/**',
  'tests/**',
  '**/*.d.ts',
  '**/*.test.ts',
]
```

### Coverage Guidelines

1. **Focus on meaningful coverage** - Don't write tests just to hit numbers
2. **Cover critical paths** - Error handling, edge cases, security code
3. **Test boundary conditions** - Off-by-one, empty inputs, max values
4. **Skip generated code** - Type definitions, auto-generated files

---

## Mutation Testing

MongoLake uses [Stryker Mutator](https://stryker-mutator.io/) for mutation testing to identify weak test spots where code can be changed without any test failing.

### Running Mutation Tests

```bash
# Run mutation tests on src/utils/
pnpm test:mutation

# Run on a specific file
npx stryker run --mutate "src/utils/sanitize-error.ts"
```

### Mutation Score Thresholds

| Level | Score | Description |
|-------|-------|-------------|
| High | 85%+ | Target score for well-tested modules |
| Low | 70% | Minimum acceptable score |
| Break | 60% | CI will fail below this |

### Configuration

The Stryker configuration is in `stryker.config.mjs`:

```javascript
{
  testRunner: 'vitest',
  mutate: [
    'src/utils/**/*.ts',
    '!src/utils/**/*.test.ts',
    '!src/utils/__tests__/**',
    '!src/utils/index.ts'
  ],
  thresholds: {
    high: 85,
    low: 70,
    break: 60
  }
}
```

### Interpreting Results

- **Killed**: Mutant was detected by tests (good)
- **Survived**: Mutant was NOT detected (tests need improvement)
- **No Coverage**: Code not covered by any test
- **Timeout**: Mutant caused an infinite loop

### Improving Mutation Score

When mutants survive:

1. **Check the mutation** - Understand what code change was made
2. **Add targeted tests** - Write tests that would fail with the mutation
3. **Consider edge cases** - Often mutations survive in boundary conditions
4. **Review assertions** - Ensure tests check the right values

Example: If a `>` to `>=` mutation survives, add a boundary test case.

---

## Writing Tests

### Assertion Best Practices

```typescript
// Use specific assertions
expect(value).toBe(42);                    // Strict equality
expect(object).toEqual({ a: 1, b: 2 });    // Deep equality
expect(float).toBeCloseTo(3.14159, 5);     // Float comparison

// Check types
expect(value).toBeInstanceOf(ObjectId);
expect(typeof result).toBe('string');

// Check arrays
expect(array).toHaveLength(3);
expect(array).toContain('expected');
expect(array).toEqual(expect.arrayContaining([1, 2]));

// Check strings
expect(str).toContain('substring');
expect(str).toMatch(/^prefix/);

// Check errors
expect(() => fn()).toThrow(InvalidInputError);
expect(() => fn()).toThrow('message');
await expect(asyncFn()).rejects.toThrow();
```

### Custom Assertions

Import from `tests/utils/assertions.ts`:

```typescript
import {
  assertDocumentId,
  assertInsertSuccess,
  assertUpdateSuccess,
  assertDeleteSuccess,
  assertSortedBy,
  assertThrowsAsync,
  assertCompletesWithin,
} from '../utils/assertions.js';

// Usage
assertInsertSuccess(result, expectedId);
assertUpdateSuccess(result, 1, 1);  // matchedCount, modifiedCount
assertSortedBy(documents, 'createdAt', 'desc');
await assertCompletesWithin(() => query.execute(), 1000);
```

### Test Isolation

```typescript
describe('SomeModule', () => {
  let instance: SomeModule;

  beforeEach(() => {
    // Fresh instance for each test
    instance = new SomeModule();
    vi.clearAllMocks();
  });

  afterEach(() => {
    // Cleanup
    vi.restoreAllMocks();
    vi.useRealTimers();
  });
});
```

### Async Tests

```typescript
// Async/await
it('should fetch data', async () => {
  const result = await fetchData();
  expect(result).toBeDefined();
});

// Promise rejection
it('should reject on error', async () => {
  await expect(failingOperation()).rejects.toThrow('Expected error');
});

// Timeout handling
it('should complete within timeout', async () => {
  const result = await Promise.race([
    operation(),
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error('Timeout')), 5000)
    ),
  ]);
  expect(result).toBeDefined();
});
```

---

## Configuration Files

### vitest.unit.config.ts

```typescript
export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: [
      'src/**/*.test.ts',
      'tests/unit/**/*.test.ts',
    ],
    pool: 'forks',
    poolOptions: {
      forks: { maxForks: 2, minForks: 1 },
    },
    testTimeout: 30000,
    hookTimeout: 30000,
    bail: 5,
    coverage: {
      provider: 'v8',
      reporter: ['text', 'html', 'lcov'],
      thresholds: {
        lines: 80,
        branches: 80,
        functions: 80,
        statements: 80,
      },
    },
  },
});
```

### vitest.config.ts (Integration)

```typescript
export default defineWorkersConfig({
  test: {
    globals: true,
    pool: '@cloudflare/vitest-pool-workers',
    poolOptions: {
      workers: {
        wrangler: { configPath: './wrangler.toml' },
        miniflare: {
          r2Buckets: ['BUCKET'],
          durableObjects: {
            RPC_NAMESPACE: { className: 'ShardDO', useSQLite: true },
          },
        },
      },
    },
    include: ['tests/integration/**/*.test.ts'],
  },
});
```

### vitest.e2e.config.ts

```typescript
export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['tests/e2e/**/*.test.ts'],
    testTimeout: 30000,
    hookTimeout: 30000,
  },
});
```

---

## Summary

| Aspect | Standard |
|--------|----------|
| File extension | `*.test.ts` |
| Test structure | `describe` / `it` with "should" statements |
| File header | JSDoc comment describing test scope |
| Unit test location | `tests/unit/` or `src/**/__tests__/` |
| Integration test location | `tests/integration/` |
| E2E test location | `tests/e2e/` |
| Test helpers | `test-helpers.ts` in module directory |
| Shared utilities | `tests/utils/` |
| Coverage threshold | 80% (lines, branches, functions, statements) |
| Test runner | Vitest |
| Integration runtime | `@cloudflare/vitest-pool-workers` |

For more details on specific patterns, see:
- `tests/utils/` - Shared test utilities and mocks
- `tests/README.md` - Quick reference for running tests
- `docs/testing/STANDARDS.md` - Extended standards documentation
