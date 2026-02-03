/**
 * FindCursor Tests
 *
 * Tests for FindCursor class methods:
 * - toArray, next, hasNext
 * - limit, skip, sort, project
 * - forEach, map
 * - async iteration
 * - method chaining
 */

import { describe, it, expect } from 'vitest';
import { createTestCollection } from './test-helpers.js';

describe('FindCursor', () => {
  describe('toArray()', () => {
    it('should return all matching documents', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', name: 'A' },
        { _id: '2', name: 'B' },
      ]);

      const docs = await collection.find({}).toArray();

      expect(docs).toHaveLength(2);
    });

    it('should exhaust cursor after toArray()', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([{ _id: '1' }, { _id: '2' }]);

      const cursor = collection.find({});
      const result1 = await cursor.toArray();
      const result2 = await cursor.toArray();

      expect(result1).toHaveLength(2);
      expect(result2).toEqual([]);
    });
  });

  describe('limit()', () => {
    it('should limit number of returned documents', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', name: 'A' },
        { _id: '2', name: 'B' },
        { _id: '3', name: 'C' },
      ]);

      const docs = await collection.find({}).limit(2).toArray();

      expect(docs).toHaveLength(2);
    });

    it('should return all if limit exceeds count', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([{ _id: '1' }, { _id: '2' }]);

      const docs = await collection.find({}).limit(10).toArray();

      expect(docs).toHaveLength(2);
    });
  });

  describe('skip()', () => {
    it('should skip documents', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', value: 1 },
        { _id: '2', value: 2 },
        { _id: '3', value: 3 },
      ]);

      const docs = await collection.find({}).sort({ value: 1 }).skip(1).toArray();

      expect(docs).toHaveLength(2);
      expect(docs[0].value).toBe(2);
    });

    it('should work with limit', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', value: 1 },
        { _id: '2', value: 2 },
        { _id: '3', value: 3 },
        { _id: '4', value: 4 },
      ]);

      const docs = await collection.find({}).sort({ value: 1 }).skip(1).limit(2).toArray();

      expect(docs).toHaveLength(2);
      expect(docs[0].value).toBe(2);
      expect(docs[1].value).toBe(3);
    });
  });

  describe('sort()', () => {
    it('should sort ascending', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '3', value: 30 },
        { _id: '1', value: 10 },
        { _id: '2', value: 20 },
      ]);

      const docs = await collection.find({}).sort({ value: 1 }).toArray();

      expect(docs[0].value).toBe(10);
      expect(docs[1].value).toBe(20);
      expect(docs[2].value).toBe(30);
    });

    it('should sort descending', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', value: 10 },
        { _id: '3', value: 30 },
        { _id: '2', value: 20 },
      ]);

      const docs = await collection.find({}).sort({ value: -1 }).toArray();

      expect(docs[0].value).toBe(30);
      expect(docs[1].value).toBe(20);
      expect(docs[2].value).toBe(10);
    });

    it('should sort by string field', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', name: 'Charlie' },
        { _id: '2', name: 'Alice' },
        { _id: '3', name: 'Bob' },
      ]);

      const docs = await collection.find({}).sort({ name: 1 }).toArray();

      expect(docs[0].name).toBe('Alice');
      expect(docs[1].name).toBe('Bob');
      expect(docs[2].name).toBe('Charlie');
    });

    it('should support multiple sort fields', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', category: 'A', priority: 2 },
        { _id: '2', category: 'B', priority: 1 },
        { _id: '3', category: 'A', priority: 1 },
      ]);

      const docs = await collection.find({}).sort({ category: 1, priority: 1 }).toArray();

      expect(docs[0]._id).toBe('3'); // A, 1
      expect(docs[1]._id).toBe('1'); // A, 2
      expect(docs[2]._id).toBe('2'); // B, 1
    });
  });

  describe('project()', () => {
    it('should include only specified fields (inclusion)', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({
        _id: '1',
        name: 'Alice',
        email: 'alice@test.com',
        password: 'secret',
      });

      const docs = await collection.find({}).project({ name: 1, email: 1 }).toArray();

      expect(docs[0]).toHaveProperty('_id');
      expect(docs[0]).toHaveProperty('name');
      expect(docs[0]).toHaveProperty('email');
      expect(docs[0]).not.toHaveProperty('password');
    });

    it('should exclude specified fields (exclusion)', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({
        _id: '1',
        name: 'Alice',
        email: 'alice@test.com',
        password: 'secret',
      });

      const docs = await collection.find({}).project({ password: 0 }).toArray();

      expect(docs[0]).toHaveProperty('name');
      expect(docs[0]).toHaveProperty('email');
      expect(docs[0]).not.toHaveProperty('password');
    });

    it('should always include _id by default', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', name: 'Alice' });

      const docs = await collection.find({}).project({ name: 1 }).toArray();

      expect(docs[0]._id).toBe('1');
    });

    it('should allow excluding _id', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', name: 'Alice' });

      const docs = await collection.find({}).project({ _id: 0 }).toArray();

      expect(docs[0]).not.toHaveProperty('_id');
    });
  });

  describe('next()', () => {
    it('should return documents one at a time', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', name: 'A' },
        { _id: '2', name: 'B' },
      ]);

      const cursor = collection.find({});
      const doc1 = await cursor.next();
      const doc2 = await cursor.next();
      const doc3 = await cursor.next();

      expect(doc1).not.toBeNull();
      expect(doc2).not.toBeNull();
      expect(doc3).toBeNull();
    });
  });

  describe('forEach()', () => {
    it('should iterate over all documents', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', value: 1 },
        { _id: '2', value: 2 },
        { _id: '3', value: 3 },
      ]);

      const values: number[] = [];
      await collection.find({}).forEach((doc) => {
        values.push(doc.value as number);
      });

      expect(values).toHaveLength(3);
      expect(values).toContain(1);
      expect(values).toContain(2);
      expect(values).toContain(3);
    });
  });

  describe('map()', () => {
    it('should transform documents', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', name: 'Alice' },
        { _id: '2', name: 'Bob' },
      ]);

      const names = await collection.find({}).map((doc) => doc.name as string);

      expect(names).toHaveLength(2);
      expect(names).toContain('Alice');
      expect(names).toContain('Bob');
    });

    it('should transform to different type', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', value: 10 },
        { _id: '2', value: 20 },
      ]);

      const doubled = await collection.find({}).map((doc) => (doc.value as number) * 2);

      expect(doubled).toContain(20);
      expect(doubled).toContain(40);
    });
  });

  describe('hasNext()', () => {
    it('should return true when documents exist', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1' });

      const hasNext = await collection.find({}).hasNext();

      expect(hasNext).toBe(true);
    });

    it('should return false for empty result', async () => {
      const { collection } = createTestCollection();

      const hasNext = await collection.find({}).hasNext();

      expect(hasNext).toBe(false);
    });
  });

  describe('async iteration', () => {
    it('should support for await...of', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', value: 1 },
        { _id: '2', value: 2 },
        { _id: '3', value: 3 },
      ]);

      const values: number[] = [];
      for await (const doc of collection.find({})) {
        values.push(doc.value as number);
      }

      expect(values).toHaveLength(3);
    });
  });

  describe('chaining', () => {
    it('should support method chaining', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', category: 'A', priority: 3 },
        { _id: '2', category: 'B', priority: 1 },
        { _id: '3', category: 'A', priority: 2 },
        { _id: '4', category: 'A', priority: 1 },
      ]);

      const docs = await collection
        .find({ category: 'A' })
        .sort({ priority: 1 })
        .skip(1)
        .limit(1)
        .project({ priority: 1 })
        .toArray();

      expect(docs).toHaveLength(1);
      expect(docs[0].priority).toBe(2);
    });
  });
});
