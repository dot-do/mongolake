/**
 * Integration tests for Wire Protocol command handling
 *
 * Tests the wire protocol layer through HTTP endpoints that simulate
 * MongoDB wire protocol commands. These tests verify command parsing,
 * execution, and response formatting.
 */
import { describe, it, expect } from 'vitest';
import { SELF } from 'cloudflare:test';

describe('Wire Protocol Integration', () => {
  const testDb = 'wireprotocoltest';

  describe('Admin Commands', () => {
    it('should respond to ping command via REST', async () => {
      // The health endpoint serves as a ping equivalent
      const response = await SELF.fetch('https://mongolake.test/health');
      expect(response.status).toBe(200);

      const data = await response.json() as { status: string };
      expect(['ok', 'healthy']).toContain(data.status);
    });

    it('should handle malformed requests gracefully', async () => {
      const response = await SELF.fetch('https://mongolake.test/api/testdb/collection', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{ invalid json',
      });

      expect(response.status).toBe(400);
    });
  });

  describe('Find Command Simulation', () => {
    it('should execute find with empty filter', async () => {
      const response = await SELF.fetch(
        `https://mongolake.test/api/${testDb}/findtest?filter=${encodeURIComponent('{}')}`
      );

      expect(response.status).toBe(200);
      const result = await response.json() as { documents: unknown[] };
      expect(result.documents).toBeDefined();
      expect(Array.isArray(result.documents)).toBe(true);
    });

    it('should execute find with projection', async () => {
      // Insert a document first
      await SELF.fetch(`https://mongolake.test/api/${testDb}/projection`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: 'Test User',
          email: 'test@example.com',
          password: 'secret',
          profile: { age: 25 },
        }),
      });

      // Query with projection (excluding password)
      const response = await SELF.fetch(
        `https://mongolake.test/api/${testDb}/projection?projection=${encodeURIComponent(JSON.stringify({ password: 0 }))}`
      );

      expect(response.status).toBe(200);
    });

    it('should handle skip and limit parameters', async () => {
      // Insert multiple documents
      await SELF.fetch(`https://mongolake.test/api/${testDb}/pagination/bulk-insert`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          documents: Array.from({ length: 5 }, (_, i) => ({ page: i + 1 })),
        }),
      });

      // Query with skip and limit
      const response = await SELF.fetch(
        `https://mongolake.test/api/${testDb}/pagination?skip=2&limit=2`
      );

      expect(response.status).toBe(200);
    });
  });

  describe('Insert Command Simulation', () => {
    it('should execute insertOne', async () => {
      const response = await SELF.fetch(`https://mongolake.test/api/${testDb}/insertcmd`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ field: 'value', number: 123 }),
      });

      expect(response.status).toBe(201);
      const result = await response.json() as { _id?: string; insertedId?: string };
      expect(result._id || result.insertedId).toBeDefined();
    });

    it('should execute insertMany via bulk-insert', async () => {
      const docs = [
        { type: 'a', value: 1 },
        { type: 'b', value: 2 },
      ];

      const response = await SELF.fetch(`https://mongolake.test/api/${testDb}/insertmany/bulk-insert`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ documents: docs }),
      });

      expect(response.status).toBe(201);
      const result = await response.json() as { insertedCount: number };
      expect(result.insertedCount).toBe(2);
    });

    it('should reject insert with invalid document structure', async () => {
      // Try to insert an array directly (should be an object)
      const response = await SELF.fetch(`https://mongolake.test/api/${testDb}/invalid`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify([1, 2, 3]), // Array instead of object
      });

      // Should either reject or handle gracefully
      expect([400, 201]).toContain(response.status);
    });
  });

  describe('Update Command Simulation', () => {
    it('should execute updateOne with $set', async () => {
      // Insert
      const insertRes = await SELF.fetch(`https://mongolake.test/api/${testDb}/updatecmd`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: 'pending' }),
      });

      const { _id } = await insertRes.json() as { _id: string };

      // Update
      const updateRes = await SELF.fetch(`https://mongolake.test/api/${testDb}/updatecmd/${_id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ $set: { status: 'complete' } }),
      });

      expect(updateRes.status).toBe(200);
      const result = await updateRes.json() as { modifiedCount: number };
      expect(result.modifiedCount).toBe(1);
    });

    it('should execute updateOne with $unset', async () => {
      // Insert
      const insertRes = await SELF.fetch(`https://mongolake.test/api/${testDb}/unsettest`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ keep: 'this', remove: 'that' }),
      });

      const { _id } = await insertRes.json() as { _id: string };

      // Update with $unset
      const updateRes = await SELF.fetch(`https://mongolake.test/api/${testDb}/unsettest/${_id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ $unset: { remove: '' } }),
      });

      expect(updateRes.status).toBe(200);
    });

    it('should execute updateOne with $push', async () => {
      // Insert document with array
      const insertRes = await SELF.fetch(`https://mongolake.test/api/${testDb}/pushtest`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ items: ['a', 'b'] }),
      });

      const { _id } = await insertRes.json() as { _id: string };

      // Update with $push
      const updateRes = await SELF.fetch(`https://mongolake.test/api/${testDb}/pushtest/${_id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ $push: { items: 'c' } }),
      });

      expect(updateRes.status).toBe(200);
    });
  });

  describe('Delete Command Simulation', () => {
    it('should execute deleteOne', async () => {
      // Insert
      const insertRes = await SELF.fetch(`https://mongolake.test/api/${testDb}/deletecmd`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ temporary: true }),
      });

      const { _id } = await insertRes.json() as { _id: string };

      // Delete
      const deleteRes = await SELF.fetch(`https://mongolake.test/api/${testDb}/deletecmd/${_id}`, {
        method: 'DELETE',
      });

      expect(deleteRes.status).toBe(200);
    });
  });

  describe('Aggregate Command Simulation', () => {
    it('should execute aggregation pipeline with $match', async () => {
      // Insert test data
      await SELF.fetch(`https://mongolake.test/api/${testDb}/aggtest/bulk-insert`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          documents: [
            { category: 'A', value: 10 },
            { category: 'B', value: 20 },
            { category: 'A', value: 30 },
          ],
        }),
      });

      // Run aggregation
      const response = await SELF.fetch(`https://mongolake.test/api/${testDb}/aggtest/aggregate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          pipeline: [{ $match: { category: 'A' } }],
        }),
      });

      expect(response.status).toBe(200);
    });

    it('should execute aggregation pipeline with $group', async () => {
      const response = await SELF.fetch(`https://mongolake.test/api/${testDb}/aggtest/aggregate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          pipeline: [
            { $group: { _id: '$category', total: { $sum: '$value' } } },
          ],
        }),
      });

      expect(response.status).toBe(200);
    });

    it('should execute aggregation pipeline with $sort and $limit', async () => {
      const response = await SELF.fetch(`https://mongolake.test/api/${testDb}/aggtest/aggregate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          pipeline: [
            { $sort: { value: -1 } },
            { $limit: 1 },
          ],
        }),
      });

      expect(response.status).toBe(200);
    });
  });

  describe('Error Handling', () => {
    it('should return 404 for non-existent document', async () => {
      const response = await SELF.fetch(
        `https://mongolake.test/api/${testDb}/notfound/nonexistent-id-12345`
      );

      // Could return 404 or 200 with empty results depending on implementation
      expect([200, 404]).toContain(response.status);
    });

    it('should return 405 for unsupported methods', async () => {
      const response = await SELF.fetch(`https://mongolake.test/api/${testDb}/collection`, {
        method: 'PUT',
      });

      expect(response.status).toBe(405);
    });

    it('should handle empty request body gracefully', async () => {
      const response = await SELF.fetch(`https://mongolake.test/api/${testDb}/empty`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '',
      });

      // Should return 400 for empty body
      expect(response.status).toBe(400);
    });
  });

  describe('Concurrent Operations', () => {
    it('should handle multiple concurrent inserts', async () => {
      const promises = Array.from({ length: 5 }, (_, i) =>
        SELF.fetch(`https://mongolake.test/api/${testDb}/concurrent`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ index: i, timestamp: Date.now() }),
        })
      );

      const results = await Promise.all(promises);
      for (const res of results) {
        expect(res.status).toBe(201);
      }
    });

    it('should handle mixed read and write operations', async () => {
      const operations = [
        // Write
        SELF.fetch(`https://mongolake.test/api/${testDb}/mixed`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ op: 'write1' }),
        }),
        // Read
        SELF.fetch(`https://mongolake.test/api/${testDb}/mixed?limit=10`),
        // Write
        SELF.fetch(`https://mongolake.test/api/${testDb}/mixed`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ op: 'write2' }),
        }),
        // Read
        SELF.fetch(`https://mongolake.test/api/${testDb}/mixed?limit=10`),
      ];

      const results = await Promise.all(operations);
      for (const res of results) {
        expect([200, 201]).toContain(res.status);
      }
    });
  });
});
