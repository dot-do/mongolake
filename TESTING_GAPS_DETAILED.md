# MongoLake Testing Gaps - Detailed Analysis with Examples

## Critical Gap #1: Missing Projection Tests

**File**: `/Users/nathanclevenger/projects/mongolake/src/utils/projection.ts`
**Status**: NO TESTS FOUND
**Code Size**: 68 lines
**Recommended Tests**: 400+ lines

### Current Implementation

```typescript
export function applyProjection<T extends Record<string, unknown>>(
  doc: T,
  projection: Record<string, 0 | 1>
): Partial<T> {
  // Logic for inclusion/exclusion projections
  // But NO TESTS verify this works correctly
}
```

### Test Scenarios NOT Covered

```typescript
// MISSING TESTS - these should be in tests/unit/utils/projection.test.ts

describe('applyProjection', () => {
  // Inclusion Mode Tests (NOT TESTED)
  it('should only include specified fields with inclusion projection', () => {
    const doc = { _id: '1', name: 'Alice', age: 30, email: 'a@example.com' };
    const result = applyProjection(doc, { name: 1, email: 1 });
    // Expected: { _id: '1', name: 'Alice', email: 'a@example.com' }
    // CURRENTLY UNTESTED ❌
  });

  it('should auto-include _id in inclusion mode unless excluded', () => {
    const doc = { _id: '1', name: 'Alice', age: 30 };
    const result = applyProjection(doc, { name: 1 });
    // Expected: { _id: '1', name: 'Alice' }
    // CURRENTLY UNTESTED ❌
  });

  it('should exclude _id when _id: 0 in inclusion projection', () => {
    const doc = { _id: '1', name: 'Alice', age: 30 };
    const result = applyProjection(doc, { name: 1, _id: 0 });
    // Expected: { name: 'Alice' }
    // CURRENTLY UNTESTED ❌
  });

  // Exclusion Mode Tests (NOT TESTED)
  it('should exclude specified fields with exclusion projection', () => {
    const doc = { _id: '1', name: 'Alice', age: 30, password: 'secret' };
    const result = applyProjection(doc, { password: 0 });
    // Expected: { _id: '1', name: 'Alice', age: 30 }
    // CURRENTLY UNTESTED ❌
  });

  it('should exclude _id when explicitly specified', () => {
    const doc = { _id: '1', name: 'Alice', age: 30 };
    const result = applyProjection(doc, { _id: 0 });
    // Expected: { name: 'Alice', age: 30 }
    // CURRENTLY UNTESTED ❌
  });

  // Nested Field Tests (NOT TESTED)
  it('should handle nested field projections', () => {
    const doc = {
      _id: '1',
      user: { name: 'Alice', email: 'a@example.com' },
      metadata: { created: '2024-01-01', updated: '2024-02-01' }
    };
    // MongoDB allows: { 'user.name': 1 }
    // Current implementation doesn't support this
    // CURRENTLY UNTESTED ❌
  });

  // Edge Cases (NOT TESTED)
  it('should handle empty projection', () => {
    const doc = { _id: '1', name: 'Alice' };
    const result = applyProjection(doc, {});
    // Expected: entire doc with _id
    // CURRENTLY UNTESTED ❌
  });

  it('should handle missing fields in projection', () => {
    const doc = { _id: '1', name: 'Alice' };
    const result = applyProjection(doc, { missing_field: 1 });
    // Expected: { _id: '1' } (only _id since field doesn't exist)
    // CURRENTLY UNTESTED ❌
  });

  it('should handle null and undefined values', () => {
    const doc = { _id: '1', name: null, status: undefined, email: 'a@example.com' };
    const result = applyProjection(doc, { name: 1, status: 1, email: 1 });
    // CURRENTLY UNTESTED ❌
  });

  // Array Slicing (MongoDB Feature - PROBABLY NOT SUPPORTED)
  it('should handle array slicing with $slice', () => {
    // MongoDB allows: { tags: { $slice: [1, 2] } }
    // Not sure if implemented
    // CURRENTLY UNTESTED ❌
  });
});
```

### Impact

**Severity**: 🔴 CRITICAL
**Risk**: Queries return incorrect fields if projection logic is broken
**Discovery Method**: Integration tests would catch, but unit tests would catch faster

---

## Critical Gap #2: Missing ObjectId/Types Tests

**File**: `/Users/nathanclevenger/projects/mongolake/src/types.ts`
**Status**: NO DEDICATED TESTS
**Code Size**: 494 lines (ObjectId = 102 lines)
**Recommended Tests**: 1000+ lines

### Current ObjectId Implementation

```typescript
export class ObjectId {
  private readonly bytes: Uint8Array;

  constructor(id?: string | Uint8Array) {
    if (id instanceof Uint8Array) {
      this.bytes = id;
    } else if (typeof id === 'string') {
      this.bytes = ObjectId.fromHex(id);
    } else {
      this.bytes = ObjectId.generate();
    }
  }

  private static generate(): Uint8Array {
    const bytes = new Uint8Array(12);
    // Timestamp (4 bytes)
    // Random value (5 bytes)
    // Counter (3 bytes)
    return bytes;
  }

  // ... other methods
}
```

### Test Scenarios NOT Covered

```typescript
// MISSING TESTS - these should be in tests/unit/types.test.ts

describe('ObjectId', () => {
  describe('Generation', () => {
    // Uniqueness Tests (NOT TESTED)
    it('should generate unique IDs on successive calls', () => {
      const id1 = new ObjectId();
      const id2 = new ObjectId();
      expect(id1.toString()).not.toBe(id2.toString());
      // CURRENTLY UNTESTED ❌
    });

    it('should generate unique IDs under concurrent access', async () => {
      const ids = await Promise.all(
        Array(1000).fill(0).map(() => Promise.resolve(new ObjectId()))
      );
      const uniqueIds = new Set(ids.map(id => id.toString()));
      expect(uniqueIds.size).toBe(1000); // All unique
      // CURRENTLY UNTESTED ❌
    });

    // Counter Overflow Tests (NOT TESTED)
    it('should handle counter overflow at 0xffffff', () => {
      // Internal counter should wrap around
      // Generate many IDs to test counter overflow
      // CURRENTLY UNTESTED ❌
    });

    // Timestamp Tests (NOT TESTED)
    it('should encode current timestamp in ObjectId', () => {
      const before = Math.floor(Date.now() / 1000);
      const id = new ObjectId();
      const timestamp = id.getTimestamp();
      const after = Math.floor(Date.now() / 1000);

      const seconds = Math.floor(timestamp.getTime() / 1000);
      expect(seconds).toBeGreaterThanOrEqual(before);
      expect(seconds).toBeLessThanOrEqual(after);
      // CURRENTLY UNTESTED ❌
    });

    it('should have 1-second granularity (not milliseconds)', () => {
      const id1 = new ObjectId();
      const id2 = new ObjectId();
      // They might have same timestamp if called <1s apart
      const ts1 = id1.getTimestamp();
      const ts2 = id2.getTimestamp();
      // CURRENTLY UNTESTED ❌
    });
  });

  describe('String Conversion', () => {
    // Hex Conversion Tests (NOT TESTED)
    it('should convert to 24-character hex string', () => {
      const id = new ObjectId();
      const hex = id.toString();
      expect(hex).toHaveLength(24);
      expect(/^[0-9a-f]{24}$/i.test(hex)).toBe(true);
      // CURRENTLY UNTESTED ❌
    });

    it('should parse hex string correctly', () => {
      const original = 'ObjectId("507f1f77bcf86cd799439011")';
      const id = new ObjectId('507f1f77bcf86cd799439011');
      expect(id.toString()).toBe('507f1f77bcf86cd799439011');
      // CURRENTLY UNTESTED ❌
    });

    // Case Sensitivity Tests (NOT TESTED)
    it('should normalize hex strings to lowercase', () => {
      const id1 = new ObjectId('507F1F77BCF86CD799439011');
      const id2 = new ObjectId('507f1f77bcf86cd799439011');
      expect(id1.toString()).toBe(id2.toString());
      // CURRENTLY UNTESTED ❌
    });
  });

  describe('Validation', () => {
    // isValid Tests (NOT TESTED)
    it('should validate correct ObjectId format', () => {
      expect(ObjectId.isValid('507f1f77bcf86cd799439011')).toBe(true);
      expect(ObjectId.isValid('507f1f77bcf86cd799439011')).toBe(true);
      // CURRENTLY UNTESTED ❌
    });

    it('should reject invalid ObjectId formats', () => {
      expect(ObjectId.isValid('invalid')).toBe(false);
      expect(ObjectId.isValid('507f1f77bcf86cd79943901')).toBe(false); // 23 chars
      expect(ObjectId.isValid('507f1f77bcf86cd7994390gg')).toBe(false); // non-hex
      // CURRENTLY UNTESTED ❌
    });

    it('should reject non-string values', () => {
      expect(ObjectId.isValid(null as any)).toBe(false);
      expect(ObjectId.isValid(undefined as any)).toBe(false);
      expect(ObjectId.isValid(123 as any)).toBe(false);
      // CURRENTLY UNTESTED ❌
    });
  });

  describe('Equality', () => {
    // Equality Tests (NOT TESTED)
    it('should consider ObjectIds with same bytes equal', () => {
      const id1 = new ObjectId('507f1f77bcf86cd799439011');
      const id2 = new ObjectId('507f1f77bcf86cd799439011');
      expect(id1.equals(id2)).toBe(true);
      // CURRENTLY UNTESTED ❌
    });

    it('should consider different ObjectIds not equal', () => {
      const id1 = new ObjectId();
      const id2 = new ObjectId();
      expect(id1.equals(id2)).toBe(false);
      // CURRENTLY UNTESTED ❌
    });
  });

  describe('Byte Representation', () => {
    // Byte Format Tests (NOT TESTED)
    it('should maintain 12-byte representation', () => {
      const id = new ObjectId();
      // Verify internal structure: 4 bytes timestamp + 5 bytes random + 3 bytes counter
      // CURRENTLY UNTESTED ❌
    });

    it('should preserve bytes when constructed from Uint8Array', () => {
      const bytes = new Uint8Array([
        0x50, 0x7f, 0x1f, 0x77, // timestamp
        0xbc, 0xf8, 0x6c, 0xd7, 0x99, 0x43, // random
        0x90, 0x11 // counter
      ]);
      const id = new ObjectId(bytes);
      expect(id.toString()).toBe('507f1f77bcf86cd799439011');
      // CURRENTLY UNTESTED ❌
    });
  });

  describe('MongoDB Compatibility', () => {
    // Compatibility Tests (NOT TESTED)
    it('should be compatible with MongoDB driver ObjectId', () => {
      // Can we use MongoLake ObjectId where MongoDB expects ObjectId?
      // CURRENTLY UNTESTED ❌
    });

    it('should convert to/from MongoDB ObjectId format', () => {
      // Integration with mongo client library
      // CURRENTLY UNTESTED ❌
    });
  });

  describe('Performance', () => {
    // Performance Tests (NOT TESTED)
    it('should generate 100K IDs in <1 second', () => {
      const start = performance.now();
      for (let i = 0; i < 100000; i++) {
        new ObjectId();
      }
      const duration = performance.now() - start;
      expect(duration).toBeLessThan(1000);
      // CURRENTLY UNTESTED ❌
    });

    it('should parse 10K hex strings in <100ms', () => {
      const hexString = '507f1f77bcf86cd799439011';
      const start = performance.now();
      for (let i = 0; i < 10000; i++) {
        new ObjectId(hexString);
      }
      const duration = performance.now() - start;
      expect(duration).toBeLessThan(100);
      // CURRENTLY UNTESTED ❌
    });
  });
});
```

### Impact

**Severity**: 🔴 CRITICAL
**Risk**:
- ObjectId collisions could corrupt data
- String parsing bugs could cause query failures
- Performance degradation with large IDs
- MongoDB incompatibility

---

## High Priority Gap: E2E Testing

**Status**: Directory exists but EMPTY
**Location**: `/Users/nathanclevenger/projects/mongolake/tests/e2e/`
**Config File**: `/Users/nathanclevenger/projects/mongolake/vitest.e2e.config.ts` (ready)
**Missing Tests**: ~50+ scenarios

### What E2E Tests Would Look Like

```typescript
// tests/e2e/crud-complete-lifecycle.test.ts
// Should test AGAINST DEPLOYED WORKERS, not local

import { describe, it, expect, beforeAll, afterAll } from 'vitest';

const API_URL = process.env.MONGOLAKE_E2E_URL || 'http://localhost:8787';

describe('E2E - Complete CRUD Lifecycle', () => {
  let documentId: string;

  describe('Create (INSERT)', () => {
    it('should insert document and return _id', async () => {
      const response = await fetch(`${API_URL}/api/testdb/users`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: 'Alice',
          email: 'alice@example.com',
          age: 30
        })
      });

      expect(response.status).toBe(201);
      const data = await response.json() as { _id: string; insertedId: string };
      documentId = data._id || data.insertedId;
      expect(documentId).toBeDefined();
      // CURRENTLY NOT IN E2E SUITE
    });
  });

  describe('Read (FIND)', () => {
    it('should retrieve inserted document by filter', async () => {
      const query = encodeURIComponent(JSON.stringify({ _id: documentId }));
      const response = await fetch(
        `${API_URL}/api/testdb/users?filter=${query}`
      );

      expect(response.status).toBe(200);
      const data = await response.json() as { documents: unknown[] };
      expect(data.documents).toContainEqual(
        expect.objectContaining({ _id: documentId, name: 'Alice' })
      );
      // CURRENTLY NOT IN E2E SUITE
    });

    it('should support complex filters', async () => {
      const filter = { age: { $gt: 25, $lt: 40 } };
      const query = encodeURIComponent(JSON.stringify(filter));
      const response = await fetch(
        `${API_URL}/api/testdb/users?filter=${query}`
      );

      expect(response.status).toBe(200);
      const data = await response.json() as { documents: unknown[] };
      expect(Array.isArray(data.documents)).toBe(true);
      // CURRENTLY NOT IN E2E SUITE
    });

    it('should respect projection', async () => {
      const projection = encodeURIComponent(JSON.stringify({ name: 1, _id: 0 }));
      const response = await fetch(
        `${API_URL}/api/testdb/users?projection=${projection}`
      );

      expect(response.status).toBe(200);
      const data = await response.json() as { documents: unknown[] };
      // Should only have name field, no _id
      // CURRENTLY NOT IN E2E SUITE
    });
  });

  describe('Update (PATCH)', () => {
    it('should update document with $set', async () => {
      const response = await fetch(
        `${API_URL}/api/testdb/users/${documentId}`,
        {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ $set: { age: 31 } })
        }
      );

      expect(response.status).toBe(200);
      const data = await response.json() as { modifiedCount: number };
      expect(data.modifiedCount).toBe(1);
      // CURRENTLY NOT IN E2E SUITE
    });

    it('should verify update persisted on next read', async () => {
      // Wait for consistency (important in distributed system)
      await new Promise(r => setTimeout(r, 100));

      const query = encodeURIComponent(JSON.stringify({ _id: documentId }));
      const response = await fetch(
        `${API_URL}/api/testdb/users?filter=${query}`
      );

      const data = await response.json() as { documents: Array<{ age: number }> };
      expect(data.documents[0].age).toBe(31);
      // CURRENTLY NOT IN E2E SUITE - CRITICAL FOR CONSISTENCY
    });
  });

  describe('Delete (DELETE)', () => {
    it('should delete document', async () => {
      const response = await fetch(
        `${API_URL}/api/testdb/users/${documentId}`,
        { method: 'DELETE' }
      );

      expect(response.status).toBe(200);
      const data = await response.json() as { deletedCount: number };
      expect(data.deletedCount).toBeGreaterThanOrEqual(1);
      // CURRENTLY NOT IN E2E SUITE
    });

    it('should not find deleted document', async () => {
      const query = encodeURIComponent(JSON.stringify({ _id: documentId }));
      const response = await fetch(
        `${API_URL}/api/testdb/users?filter=${query}`
      );

      const data = await response.json() as { documents: unknown[] };
      expect(data.documents.length).toBe(0);
      // CURRENTLY NOT IN E2E SUITE
    });
  });
});
```

### More Missing E2E Scenarios

```typescript
// tests/e2e/concurrent-writes.test.ts (MISSING ENTIRELY)
describe('E2E - Concurrent Write Handling', () => {
  it('should handle 100 concurrent inserts to same collection', async () => {
    // MISSING - Tests shard batching and consistency
  });

  it('should handle concurrent updates to same document', async () => {
    // MISSING - Tests conflict resolution
  });

  it('should maintain data consistency under concurrent writes', async () => {
    // MISSING - Tests WAL durability
  });
});

// tests/e2e/distributed-consistency.test.ts (MISSING ENTIRELY)
describe('E2E - Distributed Consistency', () => {
  it('should provide read-your-writes consistency', async () => {
    // Insert → immediately read should see it
    // MISSING
  });

  it('should sync across shards', async () => {
    // Write to one shard, read from another
    // MISSING
  });

  it('should handle shard unavailability gracefully', async () => {
    // MISSING - Tests failover
  });
});

// tests/e2e/aggregation-e2e.test.ts (MISSING ENTIRELY)
describe('E2E - Aggregation Pipeline', () => {
  beforeAll(async () => {
    // Insert test data
  });

  it('should execute multi-stage aggregation', async () => {
    // MISSING
  });

  it('should handle $lookup across collections', async () => {
    // MISSING
  });

  it('should respect memory limits in aggregation', async () => {
    // MISSING - Tests allowDiskUse option
  });
});
```

### Setup Instructions Missing

```bash
# From README or docs:
# Run against deployed worker:
MONGOLAKE_E2E_URL=https://your-worker.workers.dev pnpm test:e2e

# But currently: pnpm test:e2e finds nothing
```

---

## High Priority Gap: Performance Testing

**Status**: NO PERFORMANCE TESTS EXIST
**Missing**: ~20+ benchmark scenarios

### Example Performance Tests NOT Being Run

```typescript
// tests/performance/filter-matching.test.ts (MISSING ENTIRELY)

import { describe, it, expect } from 'vitest';
import { matchesFilter } from '../../../src/utils/filter';

describe('Performance - Filter Matching', () => {
  function generateDocuments(count: number) {
    const docs = [];
    for (let i = 0; i < count; i++) {
      docs.push({
        _id: `doc_${i}`,
        userId: Math.floor(i / 10),
        age: Math.floor(Math.random() * 100),
        status: ['active', 'inactive', 'pending'][Math.floor(Math.random() * 3)],
        tags: ['a', 'b', 'c'],
        nested: { value: Math.random() * 1000 }
      });
    }
    return docs;
  }

  describe('Simple Filters', () => {
    it('should filter 100K documents in <50ms', () => {
      const docs = generateDocuments(100000);
      const filter = { status: 'active' };

      const start = performance.now();
      let count = 0;
      for (const doc of docs) {
        if (matchesFilter(doc, filter)) count++;
      }
      const duration = performance.now() - start;

      console.log(`Filtered 100K docs in ${duration.toFixed(2)}ms, matched: ${count}`);
      expect(duration).toBeLessThan(50);
      // CURRENTLY NOT BEING MEASURED
    });
  });

  describe('Complex Filters', () => {
    it('should execute complex $or/$and in <200ms on 100K docs', () => {
      const docs = generateDocuments(100000);
      const filter = {
        $or: [
          { status: 'active', age: { $gt: 25 } },
          { tags: { $in: ['premium', 'vip'] } }
        ]
      };

      const start = performance.now();
      let count = 0;
      for (const doc of docs) {
        if (matchesFilter(doc, filter)) count++;
      }
      const duration = performance.now() - start;

      console.log(`Complex filter on 100K docs: ${duration.toFixed(2)}ms, matched: ${count}`);
      expect(duration).toBeLessThan(200);
      // CURRENTLY NOT BEING MEASURED
    });
  });

  describe('Nested Path Filters', () => {
    it('should handle nested dot notation efficiently', () => {
      const docs = generateDocuments(100000);
      const filter = { 'nested.value': { $gt: 500 } };

      const start = performance.now();
      let count = 0;
      for (const doc of docs) {
        if (matchesFilter(doc, filter)) count++;
      }
      const duration = performance.now() - start;

      expect(duration).toBeLessThan(100);
      // CURRENTLY NOT BEING MEASURED
    });
  });
});

// tests/performance/parquet-throughput.test.ts (MISSING ENTIRELY)
describe('Performance - Parquet Serialization', () => {
  it('should serialize 100K rows in <1s', async () => {
    // MISSING - Tests RowGroupSerializer throughput
  });

  it('should maintain streaming performance for 1M rows', async () => {
    // MISSING - Tests StreamingWriter doesn't degrade
  });

  it('should use <500MB memory for 1M row batch', async () => {
    // MISSING - Tests memory efficiency
  });
});

// tests/performance/concurrent-writes.test.ts (MISSING ENTIRELY)
describe('Performance - Concurrent Operations', () => {
  it('should handle 1000 concurrent inserts in <10s', async () => {
    // MISSING - Tests DO batching efficiency
  });

  it('should maintain <100ms p99 latency at 1000 ops/sec', async () => {
    // MISSING - Tests consistency under load
  });
});
```

---

## Medium Priority Gap: Error Scenarios

**Status**: Some tested, many untested
**Missing**: ~25+ error handling scenarios

### Untested Error Scenarios

```typescript
// tests/unit/error-scenarios/ (SHOULD BE CREATED)

// tests/unit/error-scenarios/storage-errors.test.ts
describe('Error Handling - Storage Failures', () => {
  it('should handle R2 quota exceeded gracefully', async () => {
    // MISSING - Tests degradation when R2 full
  });

  it('should retry on transient storage errors', async () => {
    // MISSING - Tests exponential backoff
  });

  it('should detect corrupted Parquet files', async () => {
    // MISSING - Tests file format validation
  });

  it('should recover from incomplete multipart uploads', async () => {
    // MISSING - Tests cleanup of abandoned uploads
  });
});

// tests/unit/error-scenarios/concurrent-conflicts.test.ts
describe('Error Handling - Concurrent Conflicts', () => {
  it('should handle concurrent updates to same document', async () => {
    // MISSING - Tests last-write-wins or conflict detection
  });

  it('should prevent duplicate inserts with same _id', async () => {
    // MISSING - Tests _id uniqueness enforcement
  });

  it('should handle shard split during write', async () => {
    // MISSING - Tests consistency during rebalancing
  });
});

// tests/unit/error-scenarios/network-errors.test.ts
describe('Error Handling - Network Issues', () => {
  it('should timeout long-running queries (>30s)', async () => {
    // MISSING - Tests timeout enforcement
  });

  it('should circuit break after 5 failures to same shard', async () => {
    // MISSING - Tests circuit breaker pattern
  });

  it('should retry with exponential backoff', async () => {
    // MISSING - Tests backoff strategy (1ms, 2ms, 4ms, etc)
  });
});

// tests/unit/error-scenarios/wal-corruption.test.ts
describe('Error Handling - WAL Corruption', () => {
  it('should detect corrupted WAL entries', async () => {
    // MISSING - Tests checksum validation
  });

  it('should skip corrupted entries and continue', async () => {
    // MISSING - Tests graceful degradation
  });

  it('should report corruption for monitoring', async () => {
    // MISSING - Tests error metrics
  });
});
```

---

## Summary Table: Test Gap Analysis

| Category | Current | Needed | Gap Size | Impact |
|----------|---------|--------|----------|--------|
| **Unit Tests** | 1,747 | 1,800+ | ~50 tests | Low - mostly covered |
| **Integration** | 21 | 100+ | ~80 tests | Medium - important flows missing |
| **E2E Tests** | 0 | 50+ | **CRITICAL** | High - deployment risk |
| **Performance** | 0 | 20+ | **CRITICAL** | High - ops/sec unknown |
| **Error Scenarios** | Limited | 25+ | Medium | Medium - production readiness |
| **Security Tests** | Limited | 15+ | Medium | Medium - attack surface unknown |
| **Load Testing** | 0 | 10+ | High | Medium - scaling limits unknown |

---

## Recommended Testing Task Schedule

### Week 1: Critical Gaps
- [ ] Day 1-2: Create projection.test.ts (4 hours)
- [ ] Day 2-3: Create types.test.ts with ObjectId focus (6 hours)
- [ ] Day 3-4: Create basic E2E tests (8 hours)
- [ ] Day 4-5: Create performance baseline tests (6 hours)

### Week 2: High Priority
- [ ] Day 1-2: Expand E2E test coverage (8 hours)
- [ ] Day 2-3: Create error scenario tests (8 hours)
- [ ] Day 3-4: Create shared test utilities (6 hours)
- [ ] Day 4-5: Document testing patterns (4 hours)

### Week 3: Medium Priority
- [ ] Refactor large test files (5 hours)
- [ ] Add security tests (6 hours)
- [ ] Add load testing scenarios (4 hours)
- [ ] Setup CI/CD integration (6 hours)
- [ ] Document and review (3 hours)

**Total Estimated Effort**: 94-100 hours (~2 weeks full-time)

