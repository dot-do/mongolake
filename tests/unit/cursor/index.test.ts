/**
 * Tests for MongoLake Cursor
 *
 * Comprehensive unit tests for:
 * - Cursor class (iteration, batching, modifiers)
 * - CursorStore (management, cleanup, timeout)
 * - FindCursor (collection integration)
 * - Cursor ID generation
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  Cursor,
  CursorStore,
  generateCursorId,
  type DocumentSource,
} from '../../../src/cursor/index.js';
import type { Document, WithId, Filter, FindOptions } from '../../../src/types.js';

// =============================================================================
// Test Helpers
// =============================================================================

interface TestDoc extends Document {
  _id?: string;
  name: string;
  age?: number;
  status?: string;
}

/**
 * Create a mock document source for testing
 */
function createMockSource(docs: WithId<TestDoc>[]): DocumentSource<TestDoc> {
  return {
    async readDocuments(filter?: Filter<TestDoc>, options?: FindOptions): Promise<WithId<TestDoc>[]> {
      let result = [...docs];

      // Apply simple filter (for testing)
      if (filter) {
        result = result.filter((doc) => {
          for (const [key, value] of Object.entries(filter)) {
            if (key.startsWith('$')) continue; // Skip operators for simple test
            if (doc[key as keyof typeof doc] !== value) {
              return false;
            }
          }
          return true;
        });
      }

      // Apply sort
      if (options?.sort) {
        const sortKey = Object.keys(options.sort)[0];
        const sortDir = options.sort[sortKey];
        result.sort((a, b) => {
          const aVal = a[sortKey as keyof typeof a] as string | number;
          const bVal = b[sortKey as keyof typeof b] as string | number;
          if (aVal < bVal) return -sortDir;
          if (aVal > bVal) return sortDir;
          return 0;
        });
      }

      // Apply skip
      if (options?.skip) {
        result = result.slice(options.skip);
      }

      // Apply limit
      if (options?.limit) {
        result = result.slice(0, options.limit);
      }

      // Apply projection (simple implementation)
      if (options?.projection) {
        result = result.map((doc) => {
          const projected: Record<string, unknown> = { _id: doc._id };
          for (const [key, include] of Object.entries(options.projection!)) {
            if (include === 1 && key in doc) {
              projected[key] = doc[key as keyof typeof doc];
            }
          }
          return projected as WithId<TestDoc>;
        });
      }

      return result;
    },
  };
}

/**
 * Generate test documents
 */
function generateTestDocs(count: number): WithId<TestDoc>[] {
  return Array.from({ length: count }, (_, i) => ({
    _id: `doc-${i}`,
    name: `User ${i}`,
    age: 20 + (i % 50),
    status: i % 2 === 0 ? 'active' : 'inactive',
  }));
}

// =============================================================================
// Cursor ID Generation Tests
// =============================================================================

describe('generateCursorId', () => {
  it('should generate unique cursor IDs', () => {
    const ids = new Set<bigint>();
    for (let i = 0; i < 1000; i++) {
      ids.add(generateCursorId());
    }
    expect(ids.size).toBe(1000);
  });

  it('should generate non-zero cursor IDs', () => {
    for (let i = 0; i < 100; i++) {
      const id = generateCursorId();
      expect(id).not.toBe(0n);
    }
  });

  it('should generate IDs with timestamp component', () => {
    const beforeTime = BigInt(Math.floor(Date.now() / 1000));
    const id = generateCursorId();
    const afterTime = BigInt(Math.floor(Date.now() / 1000));

    // Extract timestamp (upper 32 bits, in seconds)
    const timestamp = id >> 32n;
    expect(timestamp).toBeGreaterThanOrEqual(beforeTime);
    expect(timestamp).toBeLessThanOrEqual(afterTime);
  });
});

// =============================================================================
// Cursor Class Tests
// =============================================================================

describe('Cursor', () => {
  let testDocs: WithId<TestDoc>[];
  let source: DocumentSource<TestDoc>;

  beforeEach(() => {
    testDocs = generateTestDocs(100);
    source = createMockSource(testDocs);
  });

  describe('constructor', () => {
    it('should create cursor with unique ID', () => {
      const cursor1 = new Cursor(source, 'test.collection');
      const cursor2 = new Cursor(source, 'test.collection');
      expect(cursor1.cursorId).not.toBe(cursor2.cursorId);
    });

    it('should set namespace correctly', () => {
      const cursor = new Cursor(source, 'mydb.mycollection');
      expect(cursor.namespace).toBe('mydb.mycollection');
    });

    it('should start as not closed and not exhausted', () => {
      const cursor = new Cursor(source, 'test.collection');
      expect(cursor.isClosed).toBe(false);
      expect(cursor.isExhausted).toBe(false);
    });
  });

  describe('toArray()', () => {
    it('should return all documents', async () => {
      const cursor = new Cursor(source, 'test.collection');
      const results = await cursor.toArray();
      expect(results).toHaveLength(100);
    });

    it('should apply filter', async () => {
      const cursor = new Cursor(source, 'test.collection', { status: 'active' });
      const results = await cursor.toArray();
      expect(results.every((d) => d.status === 'active')).toBe(true);
    });

    it('should exhaust cursor on first call', async () => {
      const cursor = new Cursor(source, 'test.collection');
      const results1 = await cursor.toArray();
      const results2 = await cursor.toArray();
      expect(results1).toHaveLength(100);  // First call gets all
      expect(results2).toEqual([]);  // Second call gets none (cursor exhausted)
    });
  });

  describe('next()', () => {
    it('should return documents one at a time', async () => {
      const cursor = new Cursor(source, 'test.collection');
      const first = await cursor.next();
      const second = await cursor.next();
      expect(first?._id).toBe('doc-0');
      expect(second?._id).toBe('doc-1');
    });

    it('should return null when exhausted', async () => {
      const smallDocs = generateTestDocs(3);
      const smallSource = createMockSource(smallDocs);
      const cursor = new Cursor(smallSource, 'test.collection');

      await cursor.next();
      await cursor.next();
      await cursor.next();
      const result = await cursor.next();
      expect(result).toBeNull();
    });

    it('should return null when cursor is closed', async () => {
      const cursor = new Cursor(source, 'test.collection');
      await cursor.close();
      const result = await cursor.next();
      expect(result).toBeNull();
    });
  });

  describe('hasNext()', () => {
    it('should return true when documents remain', async () => {
      const cursor = new Cursor(source, 'test.collection');
      expect(await cursor.hasNext()).toBe(true);
    });

    it('should return false when exhausted', async () => {
      const smallDocs = generateTestDocs(2);
      const smallSource = createMockSource(smallDocs);
      const cursor = new Cursor(smallSource, 'test.collection');

      await cursor.next();
      await cursor.next();
      expect(await cursor.hasNext()).toBe(false);
    });

    it('should return false when cursor is closed', async () => {
      const cursor = new Cursor(source, 'test.collection');
      await cursor.close();
      expect(await cursor.hasNext()).toBe(false);
    });
  });

  describe('forEach()', () => {
    it('should iterate over all documents', async () => {
      const cursor = new Cursor(source, 'test.collection');
      const collected: WithId<TestDoc>[] = [];
      await cursor.forEach((doc) => collected.push(doc));
      expect(collected).toHaveLength(100);
    });

    it('should support async callbacks', async () => {
      const smallDocs = generateTestDocs(5);
      const smallSource = createMockSource(smallDocs);
      const cursor = new Cursor(smallSource, 'test.collection');

      const results: string[] = [];
      await cursor.forEach(async (doc) => {
        await new Promise((r) => setTimeout(r, 1));
        results.push(doc._id as string);
      });
      expect(results).toHaveLength(5);
    });
  });

  describe('map()', () => {
    it('should transform documents', async () => {
      const cursor = new Cursor(source, 'test.collection');
      const names = await cursor.map((doc) => doc.name);
      expect(names).toHaveLength(100);
      expect(names[0]).toBe('User 0');
    });
  });

  describe('count()', () => {
    it('should return document count', async () => {
      const cursor = new Cursor(source, 'test.collection');
      const count = await cursor.count();
      expect(count).toBe(100);
    });
  });

  describe('async iteration', () => {
    it('should support for-await-of', async () => {
      const cursor = new Cursor(source, 'test.collection');
      const collected: WithId<TestDoc>[] = [];
      for await (const doc of cursor) {
        collected.push(doc);
      }
      expect(collected).toHaveLength(100);
    });
  });

  describe('modifiers', () => {
    it('should apply limit()', async () => {
      const cursor = new Cursor(source, 'test.collection');
      cursor.limit(10);
      const results = await cursor.toArray();
      expect(results).toHaveLength(10);
    });

    it('should apply skip()', async () => {
      const cursor = new Cursor(source, 'test.collection');
      cursor.skip(95);
      const results = await cursor.toArray();
      expect(results).toHaveLength(5);
    });

    it('should apply sort()', async () => {
      const cursor = new Cursor(source, 'test.collection');
      cursor.sort({ age: -1 });
      const results = await cursor.toArray();
      // Results should be sorted by age descending
      for (let i = 1; i < results.length; i++) {
        expect(results[i].age).toBeLessThanOrEqual(results[i - 1].age!);
      }
    });

    it('should apply batchSize()', () => {
      const cursor = new Cursor(source, 'test.collection');
      cursor.batchSize(50);
      // batchSize is validated but doesn't affect in-memory iteration
      expect(cursor).toBeDefined();
    });

    it('should throw on batchSize less than 1', () => {
      const cursor = new Cursor(source, 'test.collection');
      expect(() => cursor.batchSize(0)).toThrow('Batch size must be at least 1');
    });

    it('should chain modifiers', async () => {
      const cursor = new Cursor(source, 'test.collection');
      cursor.sort({ name: 1 }).skip(10).limit(5);
      const results = await cursor.toArray();
      expect(results).toHaveLength(5);
    });

    it('should throw when modifying after execution', async () => {
      const cursor = new Cursor(source, 'test.collection');
      await cursor.toArray();
      expect(() => cursor.limit(10)).toThrow('Cannot call limit() after cursor has been executed');
    });
  });

  describe('close()', () => {
    it('should mark cursor as closed', async () => {
      const cursor = new Cursor(source, 'test.collection');
      expect(cursor.isClosed).toBe(false);
      await cursor.close();
      expect(cursor.isClosed).toBe(true);
    });

    it('should return empty results after close', async () => {
      const cursor = new Cursor(source, 'test.collection');
      await cursor.close();
      const results = await cursor.toArray();
      expect(results).toEqual([]);
    });
  });

  describe('rewind()', () => {
    it('should reset cursor position', async () => {
      const cursor = new Cursor(source, 'test.collection');
      await cursor.next();
      await cursor.next();
      await cursor.rewind();
      const doc = await cursor.next();
      expect(doc?._id).toBe('doc-0');
    });

    it('should allow re-execution after rewind', async () => {
      const cursor = new Cursor(source, 'test.collection');
      const first = await cursor.toArray();
      await cursor.rewind();
      const second = await cursor.toArray();
      expect(first).toHaveLength(100);
      expect(second).toHaveLength(100);
    });
  });

  describe('batching', () => {
    it('should get first batch', async () => {
      const cursor = new Cursor(source, 'test.collection');
      const batch = await cursor.getFirstBatch(10);
      expect(batch).toHaveLength(10);
    });

    it('should get next batch', async () => {
      const cursor = new Cursor(source, 'test.collection');
      await cursor.getFirstBatch(10);
      const batch = await cursor.getNextBatch(10);
      expect(batch).toHaveLength(10);
      expect(batch[0]._id).toBe('doc-10');
    });

    it('should mark exhausted when no more batches', async () => {
      const smallDocs = generateTestDocs(15);
      const smallSource = createMockSource(smallDocs);
      const cursor = new Cursor(smallSource, 'test.collection');

      await cursor.getFirstBatch(10);
      await cursor.getNextBatch(10);
      expect(cursor.isExhausted).toBe(true);
    });

    it('should return empty batch after exhaustion', async () => {
      const smallDocs = generateTestDocs(5);
      const smallSource = createMockSource(smallDocs);
      const cursor = new Cursor(smallSource, 'test.collection');

      await cursor.getFirstBatch(10);
      const batch = await cursor.getNextBatch(10);
      expect(batch).toEqual([]);
    });

    it('should respect limit when batching', async () => {
      const cursor = new Cursor(source, 'test.collection');
      cursor.limit(25);
      await cursor.getFirstBatch(10);
      await cursor.getNextBatch(10);
      const batch = await cursor.getNextBatch(10);
      expect(batch).toHaveLength(5);
    });
  });

  describe('timeout', () => {
    it('should not be timed out initially', () => {
      const cursor = new Cursor(source, 'test.collection');
      expect(cursor.isTimedOut(60000)).toBe(false);
    });

    it('should be timed out after timeout period', () => {
      const cursor = new Cursor(source, 'test.collection');
      // Manually set state for testing
      // @ts-expect-error - accessing private state for testing
      cursor.state.lastActivityAt = Date.now() - 120000;
      expect(cursor.isTimedOut(60000)).toBe(true);
    });
  });
});

// =============================================================================
// CursorStore Tests
// =============================================================================

describe('CursorStore', () => {
  let store: CursorStore;
  let testDocs: WithId<TestDoc>[];
  let source: DocumentSource<TestDoc>;

  beforeEach(() => {
    testDocs = generateTestDocs(100);
    source = createMockSource(testDocs);
    store = new CursorStore({
      timeoutMs: 60000,
      cleanupIntervalMs: 100000, // Long interval to prevent auto-cleanup during tests
    });
  });

  afterEach(() => {
    store.close();
  });

  describe('add()', () => {
    it('should add cursor to store', () => {
      const cursor = new Cursor(source, 'test.collection');
      store.add(cursor as Cursor<Document>);
      expect(store.size).toBe(1);
    });

    it('should track multiple cursors', () => {
      for (let i = 0; i < 5; i++) {
        const cursor = new Cursor(source, 'test.collection');
        store.add(cursor as Cursor<Document>);
      }
      expect(store.size).toBe(5);
    });
  });

  describe('get()', () => {
    it('should retrieve cursor by ID', () => {
      const cursor = new Cursor(source, 'test.collection');
      store.add(cursor as Cursor<Document>);
      const retrieved = store.get(cursor.cursorId);
      expect(retrieved).toBe(cursor);
    });

    it('should return undefined for unknown ID', () => {
      const cursor = store.get(999999n);
      expect(cursor).toBeUndefined();
    });
  });

  describe('remove()', () => {
    it('should remove cursor from store', () => {
      const cursor = new Cursor(source, 'test.collection');
      store.add(cursor as Cursor<Document>);
      const removed = store.remove(cursor.cursorId);
      expect(removed).toBe(true);
      expect(store.size).toBe(0);
    });

    it('should return false for unknown cursor', () => {
      const removed = store.remove(999999n);
      expect(removed).toBe(false);
    });

    it('should close cursor when removing', async () => {
      const cursor = new Cursor(source, 'test.collection');
      store.add(cursor as Cursor<Document>);
      store.remove(cursor.cursorId);
      expect(cursor.isClosed).toBe(true);
    });
  });

  describe('cleanupExpiredCursors()', () => {
    it('should remove timed out cursors', () => {
      const cursor = new Cursor(source, 'test.collection');
      // Set cursor as timed out
      // @ts-expect-error - accessing private state for testing
      cursor.state.lastActivityAt = Date.now() - 120000;
      store.add(cursor as Cursor<Document>);

      const cleaned = store.cleanupExpiredCursors();
      expect(cleaned).toBe(1);
      expect(store.size).toBe(0);
    });

    it('should remove closed cursors', async () => {
      const cursor = new Cursor(source, 'test.collection');
      store.add(cursor as Cursor<Document>);
      await cursor.close();

      const cleaned = store.cleanupExpiredCursors();
      expect(cleaned).toBe(1);
    });

    it('should remove exhausted cursors', async () => {
      const smallDocs = generateTestDocs(5);
      const smallSource = createMockSource(smallDocs);
      const cursor = new Cursor(smallSource, 'test.collection');
      store.add(cursor as Cursor<Document>);
      await cursor.toArray(); // Exhaust cursor

      const cleaned = store.cleanupExpiredCursors();
      expect(cleaned).toBe(1);
    });

    it('should keep active cursors', () => {
      const cursor = new Cursor(source, 'test.collection');
      store.add(cursor as Cursor<Document>);

      const cleaned = store.cleanupExpiredCursors();
      expect(cleaned).toBe(0);
      expect(store.size).toBe(1);
    });
  });

  describe('close()', () => {
    it('should close all cursors', () => {
      const cursors: Cursor<TestDoc>[] = [];
      for (let i = 0; i < 5; i++) {
        const cursor = new Cursor(source, 'test.collection');
        cursors.push(cursor);
        store.add(cursor as Cursor<Document>);
      }

      store.close();

      expect(store.size).toBe(0);
      for (const cursor of cursors) {
        expect(cursor.isClosed).toBe(true);
      }
    });
  });
});

// =============================================================================
// Integration Tests (with FindCursor and Collection)
// =============================================================================

describe('FindCursor Integration', () => {
  // These tests require the actual collection implementation
  // They are covered in the client/index.test.ts file
  // Here we just verify the class structure

  it('should export FindCursor', async () => {
    const { FindCursor } = await import('../../../src/client/index.js');
    expect(FindCursor).toBeDefined();
  });
});
