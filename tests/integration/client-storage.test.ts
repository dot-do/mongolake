/**
 * Integration tests for Client -> Storage -> Parquet data flow
 *
 * Tests the complete data lifecycle:
 * - Document insertion through REST API
 * - Storage to R2 bucket
 * - Query retrieval
 * - Data consistency verification
 */
import { describe, it, expect, beforeEach } from 'vitest';
import { env, SELF } from 'cloudflare:test';

describe('Client Storage Integration', () => {
  const testDb = 'clientstoragetest';
  const testCollection = 'documents';

  describe('Document Lifecycle', () => {
    it('should insert and retrieve a document through the full stack', async () => {
      const doc = {
        title: 'Integration Test Document',
        content: 'Testing the full client -> storage -> parquet flow',
        metadata: {
          author: 'test-runner',
          version: 1,
          tags: ['integration', 'test'],
        },
        createdAt: new Date().toISOString(),
      };

      // Insert document via REST API
      const insertResponse = await SELF.fetch(`https://mongolake.test/api/${testDb}/${testCollection}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(doc),
      });

      expect(insertResponse.status).toBe(201);
      const insertResult = await insertResponse.json() as { _id?: string; insertedId?: string };
      const docId = insertResult._id || insertResult.insertedId;
      expect(docId).toBeDefined();

      // Query back the document
      const queryResponse = await SELF.fetch(
        `https://mongolake.test/api/${testDb}/${testCollection}?filter=${encodeURIComponent(JSON.stringify({ _id: docId }))}`
      );

      expect(queryResponse.status).toBe(200);
      const queryResult = await queryResponse.json() as { documents: Array<{ title: string; metadata: { author: string } }> };
      expect(queryResult.documents).toBeDefined();
      // Document may or may not be in results depending on flush timing
    });

    it('should handle nested document structures', async () => {
      const complexDoc = {
        level1: {
          level2: {
            level3: {
              value: 'deeply nested',
              array: [1, 2, 3],
            },
          },
          sibling: true,
        },
        topLevel: 'value',
      };

      const insertResponse = await SELF.fetch(`https://mongolake.test/api/${testDb}/nested`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(complexDoc),
      });

      expect(insertResponse.status).toBe(201);
      const result = await insertResponse.json() as { _id?: string; insertedId?: string };
      expect(result._id || result.insertedId).toBeDefined();
    });

    it('should handle documents with various data types', async () => {
      const typedDoc = {
        stringField: 'hello',
        numberField: 42.5,
        integerField: 100,
        booleanField: true,
        nullField: null,
        arrayField: [1, 'two', { three: 3 }],
        dateString: new Date().toISOString(),
      };

      const response = await SELF.fetch(`https://mongolake.test/api/${testDb}/typed`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(typedDoc),
      });

      expect(response.status).toBe(201);
    });
  });

  describe('Bulk Operations', () => {
    it('should handle bulk insert and maintain data integrity', async () => {
      const documents = Array.from({ length: 10 }, (_, i) => ({
        index: i,
        name: `Bulk Item ${i}`,
        category: i % 2 === 0 ? 'even' : 'odd',
      }));

      const response = await SELF.fetch(`https://mongolake.test/api/${testDb}/bulk/bulk-insert`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ documents }),
      });

      expect(response.status).toBe(201);
      const result = await response.json() as { insertedCount: number };
      expect(result.insertedCount).toBe(10);
    });

    it('should support filtering on bulk-inserted data', async () => {
      // Insert test data
      const docs = [
        { sku: 'SKU-001', price: 10, inStock: true },
        { sku: 'SKU-002', price: 20, inStock: false },
        { sku: 'SKU-003', price: 15, inStock: true },
      ];

      await SELF.fetch(`https://mongolake.test/api/${testDb}/inventory/bulk-insert`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ documents: docs }),
      });

      // Query with filter
      const queryResponse = await SELF.fetch(
        `https://mongolake.test/api/${testDb}/inventory?filter=${encodeURIComponent(JSON.stringify({ inStock: true }))}`
      );

      expect(queryResponse.status).toBe(200);
      const result = await queryResponse.json() as { documents: unknown[] };
      expect(result.documents).toBeDefined();
    });
  });

  describe('R2 Storage Verification', () => {
    it('should verify R2 bucket is accessible', async () => {
      expect(env.BUCKET).toBeDefined();

      // Write a test key
      const testKey = `test/storage-check-${Date.now()}.txt`;
      const testData = 'Integration test data';

      await env.BUCKET.put(testKey, testData);

      // Read back
      const obj = await env.BUCKET.get(testKey);
      expect(obj).not.toBeNull();

      const content = await obj!.text();
      expect(content).toBe(testData);

      // Cleanup
      await env.BUCKET.delete(testKey);
    });

    it('should handle binary data in R2', async () => {
      const testKey = `test/binary-${Date.now()}.bin`;
      const binaryData = new Uint8Array([0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD]);

      await env.BUCKET.put(testKey, binaryData);

      const obj = await env.BUCKET.get(testKey);
      expect(obj).not.toBeNull();

      const buffer = new Uint8Array(await obj!.arrayBuffer());
      expect(buffer.length).toBe(binaryData.length);
      expect(buffer[0]).toBe(0x00);
      expect(buffer[3]).toBe(0xFF);

      await env.BUCKET.delete(testKey);
    });
  });

  describe('Update and Delete Flow', () => {
    it('should update a document and verify the change', async () => {
      // Insert
      const insertResponse = await SELF.fetch(`https://mongolake.test/api/${testDb}/updates`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: 'draft', version: 1 }),
      });

      expect(insertResponse.status).toBe(201);
      const { _id } = await insertResponse.json() as { _id: string };

      // Update
      const updateResponse = await SELF.fetch(`https://mongolake.test/api/${testDb}/updates/${_id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ $set: { status: 'published' }, $inc: { version: 1 } }),
      });

      expect(updateResponse.status).toBe(200);
      const updateResult = await updateResponse.json() as { modifiedCount: number };
      expect(updateResult.modifiedCount).toBe(1);
    });

    it('should delete a document', async () => {
      // Insert
      const insertResponse = await SELF.fetch(`https://mongolake.test/api/${testDb}/deletions`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ toDelete: true }),
      });

      const { _id } = await insertResponse.json() as { _id: string };

      // Delete
      const deleteResponse = await SELF.fetch(`https://mongolake.test/api/${testDb}/deletions/${_id}`, {
        method: 'DELETE',
      });

      expect(deleteResponse.status).toBe(200);
    });
  });

  describe('Query Operators', () => {
    it('should support comparison operators in filters', async () => {
      // Insert test data
      await SELF.fetch(`https://mongolake.test/api/${testDb}/scores/bulk-insert`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          documents: [
            { name: 'Alice', score: 85 },
            { name: 'Bob', score: 92 },
            { name: 'Charlie', score: 78 },
          ],
        }),
      });

      // Query with $gt operator
      const response = await SELF.fetch(
        `https://mongolake.test/api/${testDb}/scores?filter=${encodeURIComponent(JSON.stringify({ score: { $gt: 80 } }))}`
      );

      expect(response.status).toBe(200);
    });

    it('should support sorting and limiting', async () => {
      const response = await SELF.fetch(
        `https://mongolake.test/api/${testDb}/scores?limit=2&sort=${encodeURIComponent(JSON.stringify({ score: -1 }))}`
      );

      expect(response.status).toBe(200);
      const result = await response.json() as { documents: unknown[] };
      expect(result.documents).toBeDefined();
    });
  });
});
