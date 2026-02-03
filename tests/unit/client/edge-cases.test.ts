/**
 * Edge Cases Tests
 *
 * Tests for edge cases and complex scenarios:
 * - null and undefined values
 * - nested documents
 * - arrays
 * - special characters
 * - document lifecycle
 */

import { describe, it, expect } from 'vitest';
import { createTestCollection } from './test-helpers.js';

describe('Edge Cases', () => {
  describe('null and undefined values', () => {
    it('should handle null values in documents', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', value: null });

      const doc = await collection.findOne({ value: null });
      expect(doc?.value).toBeNull();
    });

    it('should distinguish between null and missing field', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', value: null },
        { _id: '2' },
      ]);

      const nullDocs = await collection.find({ value: null }).toArray();
      expect(nullDocs).toHaveLength(1);

      const existsDocs = await collection.find({ value: { $exists: true } }).toArray();
      expect(existsDocs).toHaveLength(1);
    });
  });

  describe('nested documents', () => {
    it('should filter on nested field with dot notation', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', user: { name: 'Alice', role: 'admin' } },
        { _id: '2', user: { name: 'Bob', role: 'user' } },
      ]);

      const docs = await collection.find({ 'user.role': 'admin' }).toArray();

      expect(docs).toHaveLength(1);
      expect((docs[0].user as { name: string }).name).toBe('Alice');
    });

    it('should update nested object by replacing it', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', user: { name: 'Alice', age: 30 } });

      await collection.updateOne({ _id: '1' }, { $set: { user: { name: 'Alice', age: 31 } } });

      const doc = await collection.findOne({ _id: '1' });
      expect((doc?.user as { age: number }).age).toBe(31);
    });
  });

  describe('arrays', () => {
    it('should handle array fields', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', tags: ['a', 'b', 'c'] });

      const doc = await collection.findOne({ _id: '1' });
      expect(doc?.tags).toEqual(['a', 'b', 'c']);
    });

    it('should push to array', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', items: [] });

      await collection.updateOne({ _id: '1' }, { $push: { items: 'new-item' } });

      const doc = await collection.findOne({ _id: '1' });
      expect(doc?.items).toEqual(['new-item']);
    });

    it('should push multiple with $each', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', items: ['a'] });

      await collection.updateOne({ _id: '1' }, { $push: { items: { $each: ['b', 'c'] } } });

      const doc = await collection.findOne({ _id: '1' });
      expect(doc?.items).toEqual(['a', 'b', 'c']);
    });

    it('should addToSet (no duplicates)', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', items: ['a', 'b'] });

      await collection.updateOne({ _id: '1' }, { $addToSet: { items: 'b' } });
      await collection.updateOne({ _id: '1' }, { $addToSet: { items: 'c' } });

      const doc = await collection.findOne({ _id: '1' });
      expect(doc?.items).toEqual(['a', 'b', 'c']);
    });

    it('should pop from array', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', items: ['a', 'b', 'c'] });

      // Pop last element
      await collection.updateOne({ _id: '1' }, { $pop: { items: 1 } });

      const doc1 = await collection.findOne({ _id: '1' });
      expect(doc1?.items).toEqual(['a', 'b']);

      // Pop first element
      await collection.updateOne({ _id: '1' }, { $pop: { items: -1 } });

      const doc2 = await collection.findOne({ _id: '1' });
      expect(doc2?.items).toEqual(['b']);
    });
  });

  describe('special characters', () => {
    it('should handle special characters in field names', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', 'field-with-dash': 'value' });

      const doc = await collection.findOne({ _id: '1' });
      expect(doc?.['field-with-dash']).toBe('value');
    });

    it('should handle special characters in values', async () => {
      const { collection } = createTestCollection();
      const specialValue = "Test with 'quotes' and \"double quotes\" and \n newline";
      await collection.insertOne({ _id: '1', text: specialValue });

      const doc = await collection.findOne({ _id: '1' });
      expect(doc?.text).toBe(specialValue);
    });

    it('should handle unicode in values', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', name: 'Cafe', emoji: 'Hello World!' });

      const doc = await collection.findOne({ _id: '1' });
      expect(doc?.name).toBe('Cafe');
      expect(doc?.emoji).toBe('Hello World!');
    });
  });

  describe('document lifecycle', () => {
    it('should handle insert -> update -> delete cycle', async () => {
      const { collection } = createTestCollection();

      // Insert
      await collection.insertOne({ _id: '1', version: 1 });
      let doc = await collection.findOne({ _id: '1' });
      expect(doc?.version).toBe(1);

      // Update
      await collection.updateOne({ _id: '1' }, { $set: { version: 2 } });
      doc = await collection.findOne({ _id: '1' });
      expect(doc?.version).toBe(2);

      // Delete
      await collection.deleteOne({ _id: '1' });
      doc = await collection.findOne({ _id: '1' });
      expect(doc).toBeNull();
    });

    it('should handle multiple updates to same document', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: '1', count: 0 });

      for (let i = 0; i < 10; i++) {
        await collection.updateOne({ _id: '1' }, { $inc: { count: 1 } });
      }

      const doc = await collection.findOne({ _id: '1' });
      expect(doc?.count).toBe(10);
    });
  });
});
