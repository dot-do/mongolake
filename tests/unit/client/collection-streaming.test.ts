/**
 * Tests for Collection streaming functionality
 *
 * Tests the readDocumentsStream and findStream methods for memory-efficient
 * processing of large collections.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createDatabase, Database, Collection } from '../../../src/client/index.js';
import { MemoryStorage } from '../../../src/storage/index.js';
import type { Document, WithId } from '../../../src/types.js';

// =============================================================================
// Test Helpers
// =============================================================================

interface TestDoc extends Document {
  _id?: string;
  name: string;
  age: number;
  status: string;
}

// =============================================================================
// Collection Streaming Tests
// =============================================================================

describe('Collection Streaming', () => {
  let storage: MemoryStorage;
  let db: Database;
  let collection: Collection<TestDoc>;
  let testId = 0;

  beforeEach(async () => {
    storage = new MemoryStorage();
    testId++;
    db = await createDatabase(`testdb_${testId}_${Date.now()}`, storage);
    collection = db.collection<TestDoc>(`users_${testId}`);
  });

  afterEach(async () => {
    storage.clear();
  });

  describe('readDocumentsStream', () => {
    it('should stream documents in batches', async () => {
      // Insert test documents
      const docs = Array.from({ length: 50 }, (_, i) => ({
        name: `User ${i}`,
        age: 20 + (i % 30),
        status: i % 2 === 0 ? 'active' : 'inactive',
      }));
      await collection.insertMany(docs);

      // Stream with small batch size
      const batches: WithId<TestDoc>[][] = [];
      for await (const batch of collection.readDocumentsStream({}, { batchSize: 10 })) {
        batches.push(batch);
      }

      // Verify all documents received
      const totalDocs = batches.reduce((sum, b) => sum + b.length, 0);
      expect(totalDocs).toBe(50);

      // Verify batch sizes
      for (let i = 0; i < batches.length - 1; i++) {
        expect(batches[i].length).toBe(10);
      }
    });

    it('should apply filter to streamed documents', async () => {
      // Insert test documents
      const docs = Array.from({ length: 20 }, (_, i) => ({
        name: `User ${i}`,
        age: 20 + i,
        status: i % 2 === 0 ? 'active' : 'inactive',
      }));
      await collection.insertMany(docs);

      // Stream with filter
      const collected: WithId<TestDoc>[] = [];
      for await (const batch of collection.readDocumentsStream({ status: 'active' }, { batchSize: 5 })) {
        collected.push(...batch);
      }

      expect(collected).toHaveLength(10);
      expect(collected.every((d) => d.status === 'active')).toBe(true);
    });

    it('should handle empty collection', async () => {
      const batches: WithId<TestDoc>[][] = [];
      for await (const batch of collection.readDocumentsStream({}, { batchSize: 10 })) {
        batches.push(batch);
      }

      expect(batches).toHaveLength(0);
    });

    it('should respect skip option', async () => {
      const docs = Array.from({ length: 20 }, (_, i) => ({
        name: `User ${i}`,
        age: 20 + i,
        status: 'active',
      }));
      await collection.insertMany(docs);

      const collected: WithId<TestDoc>[] = [];
      for await (const batch of collection.readDocumentsStream({}, { skip: 15, batchSize: 5 })) {
        collected.push(...batch);
      }

      expect(collected).toHaveLength(5);
    });

    it('should respect limit option', async () => {
      const docs = Array.from({ length: 50 }, (_, i) => ({
        name: `User ${i}`,
        age: 20 + i,
        status: 'active',
      }));
      await collection.insertMany(docs);

      const collected: WithId<TestDoc>[] = [];
      for await (const batch of collection.readDocumentsStream({}, { limit: 15, batchSize: 10 })) {
        collected.push(...batch);
      }

      expect(collected).toHaveLength(15);
    });

    it('should handle sort option (collects all docs first)', async () => {
      const docs = [
        { name: 'Charlie', age: 30, status: 'active' },
        { name: 'Alice', age: 25, status: 'active' },
        { name: 'Bob', age: 35, status: 'active' },
      ];
      await collection.insertMany(docs);

      const collected: WithId<TestDoc>[] = [];
      for await (const batch of collection.readDocumentsStream({}, { sort: { name: 1 }, batchSize: 2 })) {
        collected.push(...batch);
      }

      expect(collected).toHaveLength(3);
      expect(collected[0].name).toBe('Alice');
      expect(collected[1].name).toBe('Bob');
      expect(collected[2].name).toBe('Charlie');
    });
  });

  describe('findStream', () => {
    it('should return a StreamingFindCursor', async () => {
      const cursor = collection.findStream({});
      expect(cursor.constructor.name).toBe('StreamingFindCursor');
    });

    it('should stream documents with for-await-of', async () => {
      const docs = Array.from({ length: 25 }, (_, i) => ({
        name: `User ${i}`,
        age: 20 + i,
        status: 'active',
      }));
      await collection.insertMany(docs);

      const cursor = collection.findStream({}, { batchSize: 10 });
      const collected: WithId<TestDoc>[] = [];

      for await (const doc of cursor) {
        collected.push(doc);
      }

      expect(collected).toHaveLength(25);
    });

    it('should support batches() method', async () => {
      const docs = Array.from({ length: 25 }, (_, i) => ({
        name: `User ${i}`,
        age: 20 + i,
        status: 'active',
      }));
      await collection.insertMany(docs);

      const cursor = collection.findStream({}, { batchSize: 10 });
      const batches: WithId<TestDoc>[][] = [];

      for await (const batch of cursor.batches()) {
        batches.push(batch);
      }

      expect(batches.length).toBeGreaterThan(0);
      const totalDocs = batches.reduce((sum, b) => sum + b.length, 0);
      expect(totalDocs).toBe(25);
    });

    it('should support chained modifiers', async () => {
      const docs = Array.from({ length: 50 }, (_, i) => ({
        name: `User ${i}`,
        age: 20 + i,
        status: i % 2 === 0 ? 'active' : 'inactive',
      }));
      await collection.insertMany(docs);

      const cursor = collection
        .findStream({ status: 'active' })
        .limit(10)
        .batchSize(5);

      const collected: WithId<TestDoc>[] = [];
      for await (const doc of cursor) {
        collected.push(doc);
      }

      expect(collected).toHaveLength(10);
      expect(collected.every((d) => d.status === 'active')).toBe(true);
    });
  });

  describe('deduplication', () => {
    it('should properly deduplicate documents across multiple writes', async () => {
      // Insert initial document
      await collection.insertOne({
        _id: 'doc-1',
        name: 'Original',
        age: 25,
        status: 'active',
      } as TestDoc);

      // Update the document (creates new parquet file)
      await collection.updateOne({ _id: 'doc-1' } as any, { $set: { name: 'Updated' } });

      // Stream should return deduplicated result
      const collected: WithId<TestDoc>[] = [];
      for await (const batch of collection.readDocumentsStream({}, { batchSize: 10 })) {
        collected.push(...batch);
      }

      expect(collected).toHaveLength(1);
      expect(collected[0].name).toBe('Updated');
    });

    it('should exclude deleted documents', async () => {
      // Insert documents
      await collection.insertMany([
        { _id: 'doc-1', name: 'User 1', age: 25, status: 'active' } as TestDoc,
        { _id: 'doc-2', name: 'User 2', age: 30, status: 'active' } as TestDoc,
      ]);

      // Delete one
      await collection.deleteOne({ _id: 'doc-1' } as any);

      // Stream should not include deleted document
      const collected: WithId<TestDoc>[] = [];
      for await (const batch of collection.readDocumentsStream({}, { batchSize: 10 })) {
        collected.push(...batch);
      }

      expect(collected).toHaveLength(1);
      expect(collected[0]._id).toBe('doc-2');
    });
  });

  describe('backward compatibility', () => {
    it('should produce same results as readDocuments', async () => {
      const docs = Array.from({ length: 30 }, (_, i) => ({
        name: `User ${i}`,
        age: 20 + (i % 10),
        status: i % 3 === 0 ? 'active' : 'inactive',
      }));
      await collection.insertMany(docs);

      // Get results both ways
      const standardResults = await collection.readDocuments({ status: 'active' });

      const streamResults: WithId<TestDoc>[] = [];
      for await (const batch of collection.readDocumentsStream({ status: 'active' }, { batchSize: 5 })) {
        streamResults.push(...batch);
      }

      // Should have same documents (order may differ without sort)
      expect(streamResults).toHaveLength(standardResults.length);

      const standardIds = new Set(standardResults.map((d) => d._id));
      const streamIds = new Set(streamResults.map((d) => d._id));
      expect(standardIds).toEqual(streamIds);
    });

    it('should work with index-assisted queries', async () => {
      // Create index
      await collection.createIndex({ age: 1 });

      const docs = Array.from({ length: 20 }, (_, i) => ({
        name: `User ${i}`,
        age: 20 + i,
        status: 'active',
      }));
      await collection.insertMany(docs);

      // Stream with filter that can use index
      const collected: WithId<TestDoc>[] = [];
      for await (const batch of collection.readDocumentsStream({ age: { $gte: 35 } }, { batchSize: 5 })) {
        collected.push(...batch);
      }

      expect(collected.length).toBeGreaterThan(0);
      expect(collected.every((d) => d.age >= 35)).toBe(true);
    });
  });
});
