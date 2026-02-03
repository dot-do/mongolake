/**
 * MongoDB Session Creation Tests
 *
 * Based on MongoDB Driver Specifications:
 * - driver-sessions-server-support.json
 *
 * Tests session creation, ID generation, and basic session management.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  ClientSession,
  generateSessionId,
  SessionStore,
} from '../../../src/session/index.js';
import { MongoLake } from '../../../src/client/index.js';
import { MemoryStorage } from '../../../src/storage/index.js';

function createTestClient(): MongoLake {
  const storage = new MemoryStorage();
  const client = new MongoLake({ local: '.test-mongolake' });
  // @ts-expect-error - accessing private field for testing
  client.storage = storage;
  return client;
}

describe('Session Creation - driver-sessions-server-support', () => {
  let client: MongoLake;

  beforeEach(() => {
    client = createTestClient();
  });

  afterEach(async () => {
    await client.close();
  });

  /**
   * MongoDB Spec: "Explicit session" test
   *
   * Drivers should be able to create explicit sessions and use them
   * with operations. The session should have a logical session ID (lsid).
   */
  describe('Explicit Sessions', () => {
    it('should create session with startSession()', () => {
      const session = client.startSession();

      expect(session).toBeInstanceOf(ClientSession);
      expect(session.id).toBeDefined();

      session.endSession();
    });

    it('should have a valid lsid (logical session ID)', () => {
      const session = client.startSession();

      // lsid should be a valid UUID string
      expect(typeof session.id).toBe('string');
      expect(session.id.length).toBeGreaterThan(0);

      // UUID format validation
      const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
      expect(session.id).toMatch(uuidRegex);

      session.endSession();
    });

    it('should be able to use session with operations', async () => {
      const session = client.startSession();
      const db = client.db('testdb');
      const collection = db.collection('test');

      // Insert with session
      await collection.insertOne({ _id: 'doc1', value: 1 }, { session });

      // Find with session
      const doc = await collection.findOne({ _id: 'doc1' }, { session });
      expect(doc).toBeDefined();
      expect(doc?.value).toBe(1);

      await session.endSession();
    });
  });

  /**
   * MongoDB Spec: "Implicit session" test
   *
   * Operations without explicit sessions should work.
   * Note: MongoDB creates implicit sessions server-side; MongoLake doesn't.
   */
  describe('Implicit Sessions', () => {
    it('should execute operations without explicit session', async () => {
      const db = client.db('testdb');
      const collection = db.collection('implicit_test');

      // Operations without session
      await collection.insertOne({ _id: 'doc1', value: 1 });
      const doc = await collection.findOne({ _id: 'doc1' });

      expect(doc).toBeDefined();
      expect(doc?.value).toBe(1);
    });
  });

  /**
   * MongoDB Spec: Session uniqueness
   *
   * Each session should have a unique lsid.
   */
  describe('Session ID Uniqueness', () => {
    it('should generate unique IDs for each session', () => {
      const sessions = Array.from({ length: 100 }, () => client.startSession());
      const ids = sessions.map((s) => s.id);
      const uniqueIds = new Set(ids);

      expect(uniqueIds.size).toBe(100);

      sessions.forEach((s) => s.endSession());
    });

    it('should generate unique IDs via generateSessionId()', () => {
      const ids = Array.from({ length: 100 }, () => generateSessionId());
      const uniqueIds = new Set(ids);

      expect(uniqueIds.size).toBe(100);
    });
  });
});

describe('Session Creation - Session Store Management', () => {
  /**
   * Test session store tracking
   */
  describe('SessionStore', () => {
    it('should add and retrieve sessions', () => {
      const store = new SessionStore({ cleanupIntervalMs: 0 });
      const session = new ClientSession();

      store.add(session);

      expect(store.get(session.id)).toBe(session);
      expect(store.has(session.id)).toBe(true);
    });

    it('should remove sessions', () => {
      const store = new SessionStore({ cleanupIntervalMs: 0 });
      const session = new ClientSession();

      store.add(session);
      const removed = store.remove(session.id);

      expect(removed).toBe(true);
      expect(store.has(session.id)).toBe(false);
    });

    it('should track session count', () => {
      const store = new SessionStore({ cleanupIntervalMs: 0 });

      expect(store.size).toBe(0);

      store.add(new ClientSession());
      expect(store.size).toBe(1);

      store.add(new ClientSession());
      expect(store.size).toBe(2);
    });

    it('should get all session IDs', () => {
      const store = new SessionStore({ cleanupIntervalMs: 0 });
      const session1 = new ClientSession();
      const session2 = new ClientSession();

      store.add(session1);
      store.add(session2);

      const ids = store.getSessionIds();
      expect(ids).toContain(session1.id);
      expect(ids).toContain(session2.id);
    });
  });
});
