/**
 * Tests for StreamingCursor
 *
 * Tests for memory-efficient streaming cursor implementation that processes
 * documents in batches rather than loading entire collections into memory.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  StreamingCursor,
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

/**
 * Create a mock document source with streaming support
 */
function createStreamingMockSource(docs: WithId<TestDoc>[]): DocumentSource<TestDoc> {
  return {
    async readDocuments(filter?: Filter<TestDoc>, options?: FindOptions): Promise<WithId<TestDoc>[]> {
      let result = [...docs];
      if (options?.skip) result = result.slice(options.skip);
      if (options?.limit) result = result.slice(0, options.limit);
      return result;
    },

    async *readDocumentsStream(
      filter?: Filter<TestDoc>,
      options?: FindOptions & { batchSize?: number }
    ): AsyncGenerator<WithId<TestDoc>[], void, undefined> {
      const batchSize = options?.batchSize ?? 100;
      let result = [...docs];

      // Apply skip at stream level
      if (options?.skip) result = result.slice(options.skip);

      // Yield in batches
      for (let i = 0; i < result.length; i += batchSize) {
        yield result.slice(i, i + batchSize);
      }
    },
  };
}

/**
 * Create a mock source without streaming (fallback test)
 */
function createNonStreamingMockSource(docs: WithId<TestDoc>[]): DocumentSource<TestDoc> {
  return {
    async readDocuments(filter?: Filter<TestDoc>, options?: FindOptions): Promise<WithId<TestDoc>[]> {
      let result = [...docs];
      if (options?.skip) result = result.slice(options.skip);
      if (options?.limit) result = result.slice(0, options.limit);
      return result;
    },
    // No readDocumentsStream - forces fallback
  };
}

// =============================================================================
// StreamingCursor Tests
// =============================================================================

describe('StreamingCursor', () => {
  let testDocs: WithId<TestDoc>[];
  let streamingSource: DocumentSource<TestDoc>;
  let nonStreamingSource: DocumentSource<TestDoc>;

  beforeEach(() => {
    testDocs = generateTestDocs(100);
    streamingSource = createStreamingMockSource(testDocs);
    nonStreamingSource = createNonStreamingMockSource(testDocs);
  });

  describe('basic iteration', () => {
    it('should iterate all documents with for-await-of', async () => {
      const cursor = new StreamingCursor(streamingSource, 'test.collection');
      const collected: WithId<TestDoc>[] = [];

      for await (const doc of cursor) {
        collected.push(doc);
      }

      expect(collected).toHaveLength(100);
      expect(collected[0]._id).toBe('doc-0');
      expect(collected[99]._id).toBe('doc-99');
    });

    it('should support hasNext() and next()', async () => {
      const cursor = new StreamingCursor(streamingSource, 'test.collection');

      expect(await cursor.hasNext()).toBe(true);
      const first = await cursor.next();
      expect(first?._id).toBe('doc-0');

      const second = await cursor.next();
      expect(second?._id).toBe('doc-1');
    });

    it('should return null when exhausted', async () => {
      const smallDocs = generateTestDocs(3);
      const smallSource = createStreamingMockSource(smallDocs);
      const cursor = new StreamingCursor(smallSource, 'test.collection');

      await cursor.next();
      await cursor.next();
      await cursor.next();

      expect(await cursor.hasNext()).toBe(false);
      expect(await cursor.next()).toBeNull();
    });
  });

  describe('batch iteration', () => {
    it('should iterate in batches with batches()', async () => {
      const cursor = new StreamingCursor(streamingSource, 'test.collection');
      cursor.batchSize(25);

      const batches: WithId<TestDoc>[][] = [];
      for await (const batch of cursor.batches()) {
        batches.push(batch);
      }

      expect(batches).toHaveLength(4);
      expect(batches[0]).toHaveLength(25);
      expect(batches[3]).toHaveLength(25);
    });

    it('should respect batchSize modifier', async () => {
      const cursor = new StreamingCursor(streamingSource, 'test.collection');
      cursor.batchSize(10);

      let batchCount = 0;
      for await (const batch of cursor.batches()) {
        expect(batch.length).toBeLessThanOrEqual(10);
        batchCount++;
      }

      expect(batchCount).toBe(10);
    });

    it('should throw for invalid batchSize', () => {
      const cursor = new StreamingCursor(streamingSource, 'test.collection');
      expect(() => cursor.batchSize(0)).toThrow('Batch size must be at least 1');
    });
  });

  describe('modifiers', () => {
    it('should apply limit', async () => {
      const cursor = new StreamingCursor(streamingSource, 'test.collection');
      cursor.limit(10);

      const results = await cursor.toArray();
      expect(results).toHaveLength(10);
    });

    it('should apply skip', async () => {
      const cursor = new StreamingCursor(streamingSource, 'test.collection');
      cursor.skip(90);

      const results = await cursor.toArray();
      expect(results).toHaveLength(10);
      expect(results[0]._id).toBe('doc-90');
    });

    it('should chain modifiers', async () => {
      const cursor = new StreamingCursor(streamingSource, 'test.collection');
      cursor.skip(10).limit(5).batchSize(2);

      const results = await cursor.toArray();
      expect(results).toHaveLength(5);
      expect(results[0]._id).toBe('doc-10');
    });

    it('should throw when modifying after streaming starts', async () => {
      const cursor = new StreamingCursor(streamingSource, 'test.collection');
      await cursor.next(); // Start streaming

      expect(() => cursor.limit(10)).toThrow('after cursor has started streaming');
      expect(() => cursor.skip(10)).toThrow('after cursor has started streaming');
      expect(() => cursor.sort({ name: 1 })).toThrow('after cursor has started streaming');
    });
  });

  describe('lifecycle', () => {
    it('should close cursor', async () => {
      const cursor = new StreamingCursor(streamingSource, 'test.collection');
      expect(cursor.isClosed).toBe(false);

      await cursor.close();
      expect(cursor.isClosed).toBe(true);
    });

    it('should return empty after close', async () => {
      const cursor = new StreamingCursor(streamingSource, 'test.collection');
      await cursor.close();

      expect(await cursor.hasNext()).toBe(false);
      expect(await cursor.next()).toBeNull();
      expect(await cursor.toArray()).toEqual([]);
    });

    it('should support rewind', async () => {
      const cursor = new StreamingCursor(streamingSource, 'test.collection');

      await cursor.next();
      await cursor.next();

      await cursor.rewind();

      const doc = await cursor.next();
      expect(doc?._id).toBe('doc-0');
    });
  });

  describe('fallback behavior', () => {
    it('should work with non-streaming source', async () => {
      const cursor = new StreamingCursor(nonStreamingSource, 'test.collection');

      const results = await cursor.toArray();
      expect(results).toHaveLength(100);
    });

    it('should batch results from non-streaming source', async () => {
      const cursor = new StreamingCursor(nonStreamingSource, 'test.collection');
      cursor.batchSize(25);

      const batches: WithId<TestDoc>[][] = [];
      for await (const batch of cursor.batches()) {
        batches.push(batch);
      }

      expect(batches).toHaveLength(4);
    });
  });

  describe('helper methods', () => {
    it('should support forEach', async () => {
      const cursor = new StreamingCursor(streamingSource, 'test.collection');
      cursor.limit(5);

      const collected: string[] = [];
      await cursor.forEach((doc) => {
        collected.push(doc._id as string);
      });

      expect(collected).toHaveLength(5);
    });

    it('should support map', async () => {
      const cursor = new StreamingCursor(streamingSource, 'test.collection');
      cursor.limit(5);

      const names = await cursor.map((doc) => doc.name);
      expect(names).toEqual(['User 0', 'User 1', 'User 2', 'User 3', 'User 4']);
    });

    it('should support count', async () => {
      const cursor = new StreamingCursor(streamingSource, 'test.collection');
      cursor.limit(25);

      const count = await cursor.count();
      expect(count).toBe(25);
    });
  });

  describe('cursor properties', () => {
    it('should generate unique cursor IDs', () => {
      const cursor1 = new StreamingCursor(streamingSource, 'test.collection');
      const cursor2 = new StreamingCursor(streamingSource, 'test.collection');
      expect(cursor1.cursorId).not.toBe(cursor2.cursorId);
    });

    it('should track namespace', () => {
      const cursor = new StreamingCursor(streamingSource, 'mydb.mycollection');
      expect(cursor.namespace).toBe('mydb.mycollection');
    });

    it('should track exhausted state', async () => {
      const smallDocs = generateTestDocs(2);
      const smallSource = createStreamingMockSource(smallDocs);
      const cursor = new StreamingCursor(smallSource, 'test.collection');

      expect(cursor.isExhausted).toBe(false);
      await cursor.toArray();
      expect(cursor.isExhausted).toBe(true);
    });
  });

  describe('limit with batches', () => {
    it('should stop yielding batches when limit reached', async () => {
      const cursor = new StreamingCursor(streamingSource, 'test.collection');
      cursor.limit(35).batchSize(10);

      let totalDocs = 0;
      for await (const batch of cursor.batches()) {
        totalDocs += batch.length;
      }

      expect(totalDocs).toBe(35);
    });

    it('should trim last batch to respect limit', async () => {
      const cursor = new StreamingCursor(streamingSource, 'test.collection');
      cursor.limit(25).batchSize(10);

      const batches: WithId<TestDoc>[][] = [];
      for await (const batch of cursor.batches()) {
        batches.push(batch);
      }

      expect(batches).toHaveLength(3);
      expect(batches[0]).toHaveLength(10);
      expect(batches[1]).toHaveLength(10);
      expect(batches[2]).toHaveLength(5);
    });
  });

  describe('memory efficiency', () => {
    it('should not load all documents at once with streaming source', async () => {
      // Track how many times readDocuments vs readDocumentsStream is called
      const readDocumentsCalls: number[] = [];
      const streamBatchesFetched: number[] = [];

      const trackingSource: DocumentSource<TestDoc> = {
        async readDocuments(): Promise<WithId<TestDoc>[]> {
          readDocumentsCalls.push(1);
          return testDocs;
        },
        async *readDocumentsStream(
          _filter?: Filter<TestDoc>,
          options?: FindOptions & { batchSize?: number }
        ): AsyncGenerator<WithId<TestDoc>[], void, undefined> {
          const batchSize = options?.batchSize ?? 100;
          for (let i = 0; i < testDocs.length; i += batchSize) {
            streamBatchesFetched.push(i);
            yield testDocs.slice(i, i + batchSize);
          }
        },
      };

      const cursor = new StreamingCursor(trackingSource, 'test.collection');
      cursor.batchSize(10);

      // Iterate through all
      for await (const _doc of cursor) {
        // Just consume
      }

      // Should use streaming, not readDocuments
      expect(readDocumentsCalls).toHaveLength(0);
      expect(streamBatchesFetched.length).toBeGreaterThan(0);
    });
  });
});
