/**
 * Session Management Unit Tests
 *
 * Tests for session lifecycle, expiration, concurrency, and metadata tracking.
 *
 * Issue: mongolake-4301
 *
 * Requirements:
 * 1. Session expiration after timeout
 * 2. Concurrent request handling in same session
 * 3. Session state persistence
 * 4. Expired session cleanup
 * 5. Unique session ID generation
 * 6. Session metadata tracking (created, lastUsed, etc.)
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  ClientSession,
  SessionStore,
  SessionError,
  generateSessionId,
} from '../../../src/session/index.js';

// ============================================================================
// Session Expiration Tests
// ============================================================================

describe('Session Management - Session Expiration', () => {
  let store: SessionStore;

  beforeEach(() => {
    // Use a short timeout for testing (100ms)
    store = new SessionStore({ timeoutMs: 100, cleanupIntervalMs: 0 });
  });

  afterEach(async () => {
    await store.closeAll();
  });

  it('should expire sessions after timeout', async () => {
    const session = new ClientSession();
    store.add(session);

    expect(store.has(session.id)).toBe(true);

    // Wait for timeout to expire
    await new Promise(resolve => setTimeout(resolve, 150));

    // Cleanup expired sessions
    const cleaned = store.cleanupExpired();

    expect(cleaned).toBe(1);
    expect(store.has(session.id)).toBe(false);
  });

  it('should not expire sessions before timeout', async () => {
    const session = new ClientSession();
    store.add(session);

    // Wait less than timeout
    await new Promise(resolve => setTimeout(resolve, 50));

    const cleaned = store.cleanupExpired();

    expect(cleaned).toBe(0);
    expect(store.has(session.id)).toBe(true);
  });

  it('should reset expiration timer on activity', async () => {
    const session = new ClientSession();
    store.add(session);

    // Wait 60ms
    await new Promise(resolve => setTimeout(resolve, 60));

    // Touch the session to reset timer
    session.touch();

    // Wait another 60ms (total 120ms, but only 60ms since touch)
    await new Promise(resolve => setTimeout(resolve, 60));

    const cleaned = store.cleanupExpired();

    // Session should not be expired because it was touched
    expect(cleaned).toBe(0);
    expect(store.has(session.id)).toBe(true);
  });

  it('should expire multiple sessions correctly', async () => {
    const session1 = new ClientSession();
    const session2 = new ClientSession();
    const session3 = new ClientSession();

    store.add(session1);
    store.add(session2);
    store.add(session3);

    expect(store.size).toBe(3);

    // Wait for timeout
    await new Promise(resolve => setTimeout(resolve, 150));

    // Touch session2 before cleanup (simulate activity)
    // Note: Touch was before timeout, so it's still expired
    // Let's create a fresh scenario

    const cleaned = store.cleanupExpired();

    expect(cleaned).toBe(3);
    expect(store.size).toBe(0);
  });

  it('should clean up ended sessions regardless of timeout', async () => {
    const session = new ClientSession();
    store.add(session);

    // End session immediately
    await session.endSession();

    // Run cleanup (no timeout wait needed)
    const cleaned = store.cleanupExpired();

    expect(cleaned).toBe(1);
    expect(store.has(session.id)).toBe(false);
  });

  it('should configure custom timeout value', () => {
    const customStore = new SessionStore({ timeoutMs: 5000, cleanupIntervalMs: 0 });

    expect(customStore.timeoutMs).toBe(5000);

    customStore.closeAll();
  });

  it('should use default 30 minute timeout when not specified', () => {
    const defaultStore = new SessionStore({ cleanupIntervalMs: 0 });

    expect(defaultStore.timeoutMs).toBe(30 * 60 * 1000);

    defaultStore.closeAll();
  });
});

// ============================================================================
// Concurrent Request Handling Tests
// ============================================================================

describe('Session Management - Concurrent Requests', () => {
  let store: SessionStore;

  beforeEach(() => {
    store = new SessionStore({ timeoutMs: 60000, cleanupIntervalMs: 0 });
  });

  afterEach(async () => {
    await store.closeAll();
  });

  it('should handle concurrent requests in same session', async () => {
    const session = new ClientSession();
    store.add(session);

    session.startTransaction();

    // Simulate concurrent operations buffering
    const operations = [
      { type: 'insert' as const, collection: 'users', database: 'test', document: { _id: '1', name: 'User 1' } },
      { type: 'insert' as const, collection: 'users', database: 'test', document: { _id: '2', name: 'User 2' } },
      { type: 'update' as const, collection: 'users', database: 'test', filter: { _id: '1' }, update: { $set: { active: true } } },
    ];

    // Buffer all operations concurrently
    await Promise.all(
      operations.map(op => Promise.resolve(session.bufferOperation(op)))
    );

    expect(session.operationCount).toBe(3);
    expect(session.transactionState).toBe('in_progress');

    await session.abortTransaction();
  });

  it('should maintain operation order in concurrent buffering', async () => {
    const session = new ClientSession();
    store.add(session);

    session.startTransaction();

    // Buffer operations with known timestamps
    session.bufferOperation({ type: 'insert', collection: 'test', database: 'db', document: { _id: '1', order: 1 } });
    session.bufferOperation({ type: 'insert', collection: 'test', database: 'db', document: { _id: '2', order: 2 } });
    session.bufferOperation({ type: 'insert', collection: 'test', database: 'db', document: { _id: '3', order: 3 } });

    const ops = session.getBufferedOperations();

    expect(ops.length).toBe(3);
    expect(ops[0].document?._id).toBe('1');
    expect(ops[1].document?._id).toBe('2');
    expect(ops[2].document?._id).toBe('3');

    await session.endSession();
  });

  it('should isolate concurrent sessions from each other', async () => {
    const session1 = new ClientSession();
    const session2 = new ClientSession();

    store.add(session1);
    store.add(session2);

    session1.startTransaction();
    session2.startTransaction();

    // Concurrent operations on different sessions
    await Promise.all([
      Promise.resolve(session1.bufferOperation({ type: 'insert', collection: 'test', database: 'db', document: { _id: '1' } })),
      Promise.resolve(session2.bufferOperation({ type: 'insert', collection: 'test', database: 'db', document: { _id: '2' } })),
      Promise.resolve(session1.bufferOperation({ type: 'insert', collection: 'test', database: 'db', document: { _id: '3' } })),
      Promise.resolve(session2.bufferOperation({ type: 'insert', collection: 'test', database: 'db', document: { _id: '4' } })),
    ]);

    expect(session1.operationCount).toBe(2);
    expect(session2.operationCount).toBe(2);

    const ops1 = session1.getBufferedOperations();
    const ops2 = session2.getBufferedOperations();

    expect(ops1.map(o => o.document?._id)).toContain('1');
    expect(ops1.map(o => o.document?._id)).toContain('3');
    expect(ops2.map(o => o.document?._id)).toContain('2');
    expect(ops2.map(o => o.document?._id)).toContain('4');

    await session1.endSession();
    await session2.endSession();
  });

  it('should handle concurrent commit and abort on different sessions', async () => {
    const session1 = new ClientSession();
    const session2 = new ClientSession();

    store.add(session1);
    store.add(session2);

    session1.startTransaction();
    session2.startTransaction();

    session1.bufferOperation({ type: 'insert', collection: 'test', database: 'db', document: { _id: '1' } });
    session2.bufferOperation({ type: 'insert', collection: 'test', database: 'db', document: { _id: '2' } });

    // Concurrent commit and abort
    await Promise.all([
      session1.commitTransaction(),
      session2.abortTransaction(),
    ]);

    expect(session1.transactionState).toBe('committed');
    expect(session2.transactionState).toBe('aborted');

    await session1.endSession();
    await session2.endSession();
  });

  it('should update lastUsed on concurrent operations', async () => {
    const session = new ClientSession();
    store.add(session);

    const initialLastUsed = session.lastUsed.getTime();

    await new Promise(resolve => setTimeout(resolve, 10));

    session.startTransaction();

    // Concurrent buffer operations
    await Promise.all([
      Promise.resolve(session.bufferOperation({ type: 'insert', collection: 'test', database: 'db', document: { _id: '1' } })),
      Promise.resolve(session.bufferOperation({ type: 'insert', collection: 'test', database: 'db', document: { _id: '2' } })),
    ]);

    expect(session.lastUsed.getTime()).toBeGreaterThan(initialLastUsed);

    await session.endSession();
  });
});

// ============================================================================
// Session State Persistence Tests
// ============================================================================

describe('Session Management - State Persistence', () => {
  let store: SessionStore;

  beforeEach(() => {
    store = new SessionStore({ timeoutMs: 60000, cleanupIntervalMs: 0 });
  });

  afterEach(async () => {
    await store.closeAll();
  });

  it('should persist session state correctly', () => {
    const session = new ClientSession({
      causalConsistency: true,
      defaultTransactionOptions: {
        readConcern: { level: 'snapshot' },
        writeConcern: { w: 'majority' },
      },
    });

    store.add(session);

    // Retrieve session from store
    const retrieved = store.get(session.id);

    expect(retrieved).toBe(session);
    expect(retrieved?.id).toBe(session.id);
  });

  it('should preserve transaction state across operations', async () => {
    const session = new ClientSession();
    store.add(session);

    expect(session.transactionState).toBe('none');

    session.startTransaction();
    expect(session.transactionState).toBe('starting');

    session.bufferOperation({ type: 'insert', collection: 'test', database: 'db', document: { _id: '1' } });
    expect(session.transactionState).toBe('in_progress');

    await session.commitTransaction();
    expect(session.transactionState).toBe('committed');

    // Start new transaction
    session.startTransaction();
    expect(session.transactionState).toBe('starting');

    await session.abortTransaction();
    expect(session.transactionState).toBe('aborted');

    await session.endSession();
    expect(session.hasEnded).toBe(true);
  });

  it('should persist buffered operations until commit/abort', async () => {
    const session = new ClientSession();
    store.add(session);

    session.startTransaction();

    session.bufferOperation({ type: 'insert', collection: 'test', database: 'db', document: { _id: '1' } });
    session.bufferOperation({ type: 'update', collection: 'test', database: 'db', filter: { _id: '1' }, update: { $set: { x: 1 } } });

    // Operations should persist
    expect(session.operationCount).toBe(2);
    expect(session.getBufferedOperations().length).toBe(2);

    // After abort, operations should be cleared
    await session.abortTransaction();
    expect(session.operationCount).toBe(0);
    expect(session.getBufferedOperations().length).toBe(0);
  });

  it('should persist transaction number across transactions', async () => {
    const session = new ClientSession();
    store.add(session);

    expect(session.txnNumber).toBe(0);

    session.startTransaction();
    expect(session.txnNumber).toBe(1);
    await session.commitTransaction();

    session.startTransaction();
    expect(session.txnNumber).toBe(2);
    await session.abortTransaction();

    session.startTransaction();
    expect(session.txnNumber).toBe(3);
    await session.commitTransaction();

    await session.endSession();
  });

  it('should serialize session state to JSON', () => {
    const session = new ClientSession();
    store.add(session);

    session.startTransaction();

    const json = session.toJSON();

    expect(json.id).toBe(session.id);
    expect(json.txnNumber).toBe(session.txnNumber);
    expect(typeof json.id).toBe('string');
    expect(typeof json.txnNumber).toBe('number');
  });
});

// ============================================================================
// Expired Session Cleanup Tests
// ============================================================================

describe('Session Management - Expired Session Cleanup', () => {
  it('should clean up expired sessions', async () => {
    const store = new SessionStore({ timeoutMs: 50, cleanupIntervalMs: 0 });

    const session1 = new ClientSession();
    const session2 = new ClientSession();

    store.add(session1);
    store.add(session2);

    expect(store.size).toBe(2);

    // Wait for expiration
    await new Promise(resolve => setTimeout(resolve, 100));

    const cleaned = store.cleanupExpired();

    expect(cleaned).toBe(2);
    expect(store.size).toBe(0);

    await store.closeAll();
  });

  it('should only clean up expired sessions not active ones', async () => {
    const store = new SessionStore({ timeoutMs: 100, cleanupIntervalMs: 0 });

    const expiredSession = new ClientSession();
    store.add(expiredSession);

    // Wait some time (more than timeout)
    await new Promise(resolve => setTimeout(resolve, 120));

    // Create and add active session now (after wait)
    const activeSession = new ClientSession();
    store.add(activeSession);

    const cleaned = store.cleanupExpired();

    expect(cleaned).toBe(1);
    expect(store.has(expiredSession.id)).toBe(false);
    expect(store.has(activeSession.id)).toBe(true);

    await store.closeAll();
  });

  it('should run automatic cleanup on interval', async () => {
    // Use a very short cleanup interval for testing
    const store = new SessionStore({ timeoutMs: 30, cleanupIntervalMs: 50 });

    const session = new ClientSession();
    store.add(session);

    expect(store.size).toBe(1);

    // Wait for automatic cleanup to run
    await new Promise(resolve => setTimeout(resolve, 150));

    // Session should be cleaned up automatically
    expect(store.size).toBe(0);

    await store.closeAll();
  });

  it('should stop cleanup interval on closeAll', async () => {
    const store = new SessionStore({ timeoutMs: 1000, cleanupIntervalMs: 50 });

    const session = new ClientSession();
    store.add(session);

    await store.closeAll();

    // Add new session after closeAll
    const newSession = new ClientSession();
    store.add(newSession);

    // Wait for what would have been cleanup time
    await new Promise(resolve => setTimeout(resolve, 100));

    // Session should still exist because interval was stopped
    // (though manual cleanup would still work)
    expect(store.has(newSession.id)).toBe(true);
  });

  it('should end sessions before removing them on closeAll', async () => {
    const store = new SessionStore({ timeoutMs: 60000, cleanupIntervalMs: 0 });

    const session = new ClientSession();
    store.add(session);

    session.startTransaction();
    session.bufferOperation({ type: 'insert', collection: 'test', database: 'db', document: { _id: '1' } });

    await store.closeAll();

    expect(session.hasEnded).toBe(true);
    expect(session.transactionState).toBe('aborted');
    expect(store.size).toBe(0);
  });

  it('should return count of cleaned sessions', async () => {
    const store = new SessionStore({ timeoutMs: 20, cleanupIntervalMs: 0 });

    // Add 5 sessions
    for (let i = 0; i < 5; i++) {
      store.add(new ClientSession());
    }

    expect(store.size).toBe(5);

    await new Promise(resolve => setTimeout(resolve, 50));

    const cleaned = store.cleanupExpired();

    expect(cleaned).toBe(5);

    await store.closeAll();
  });
});

// ============================================================================
// Unique Session ID Generation Tests
// ============================================================================

describe('Session Management - Unique Session IDs', () => {
  it('should generate unique session IDs', () => {
    const ids = new Set<string>();

    for (let i = 0; i < 1000; i++) {
      const id = generateSessionId();
      expect(ids.has(id)).toBe(false);
      ids.add(id);
    }

    expect(ids.size).toBe(1000);
  });

  it('should generate UUID v4 format session IDs', () => {
    const id = generateSessionId();

    // UUID v4 format: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
    const uuidV4Regex = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

    expect(id).toMatch(uuidV4Regex);
  });

  it('should create sessions with unique IDs', () => {
    const session1 = new ClientSession();
    const session2 = new ClientSession();
    const session3 = new ClientSession();

    expect(session1.id).not.toBe(session2.id);
    expect(session2.id).not.toBe(session3.id);
    expect(session1.id).not.toBe(session3.id);
  });

  it('should generate IDs that are string type', () => {
    const id = generateSessionId();

    expect(typeof id).toBe('string');
    expect(id.length).toBe(36); // UUID length with hyphens
  });

  it('should generate different IDs in rapid succession', () => {
    const ids: string[] = [];

    for (let i = 0; i < 100; i++) {
      ids.push(generateSessionId());
    }

    const uniqueIds = new Set(ids);
    expect(uniqueIds.size).toBe(100);
  });

  it('should assign unique ID to session on construction', () => {
    const session = new ClientSession();

    expect(session.id).toBeDefined();
    expect(typeof session.id).toBe('string');
    expect(session.id.length).toBe(36);
  });
});

// ============================================================================
// Session Metadata Tracking Tests
// ============================================================================

describe('Session Management - Metadata Tracking', () => {
  let store: SessionStore;

  beforeEach(() => {
    store = new SessionStore({ timeoutMs: 60000, cleanupIntervalMs: 0 });
  });

  afterEach(async () => {
    await store.closeAll();
  });

  it('should track session metadata (created, lastUsed, etc.)', () => {
    const beforeCreate = new Date();
    const session = new ClientSession();
    const afterCreate = new Date();

    expect(session.createdAt).toBeInstanceOf(Date);
    expect(session.lastUsed).toBeInstanceOf(Date);
    expect(session.createdAt.getTime()).toBeGreaterThanOrEqual(beforeCreate.getTime());
    expect(session.createdAt.getTime()).toBeLessThanOrEqual(afterCreate.getTime());
  });

  it('should update lastUsed on startTransaction', async () => {
    const session = new ClientSession();
    const initialLastUsed = session.lastUsed.getTime();

    await new Promise(resolve => setTimeout(resolve, 10));

    session.startTransaction();

    expect(session.lastUsed.getTime()).toBeGreaterThan(initialLastUsed);

    await session.endSession();
  });

  it('should update lastUsed on commitTransaction', async () => {
    const session = new ClientSession();

    session.startTransaction();
    const afterStart = session.lastUsed.getTime();

    await new Promise(resolve => setTimeout(resolve, 10));

    await session.commitTransaction();

    expect(session.lastUsed.getTime()).toBeGreaterThan(afterStart);
  });

  it('should update lastUsed on abortTransaction', async () => {
    const session = new ClientSession();

    session.startTransaction();
    const afterStart = session.lastUsed.getTime();

    await new Promise(resolve => setTimeout(resolve, 10));

    await session.abortTransaction();

    expect(session.lastUsed.getTime()).toBeGreaterThan(afterStart);
  });

  it('should update lastUsed on bufferOperation', async () => {
    const session = new ClientSession();

    session.startTransaction();
    const afterStart = session.lastUsed.getTime();

    await new Promise(resolve => setTimeout(resolve, 10));

    session.bufferOperation({ type: 'insert', collection: 'test', database: 'db', document: { _id: '1' } });

    expect(session.lastUsed.getTime()).toBeGreaterThan(afterStart);

    await session.endSession();
  });

  it('should update lastUsed on touch', async () => {
    const session = new ClientSession();
    const initialLastUsed = session.lastUsed.getTime();

    await new Promise(resolve => setTimeout(resolve, 10));

    session.touch();

    expect(session.lastUsed.getTime()).toBeGreaterThan(initialLastUsed);
  });

  it('should track hasEnded state', async () => {
    const session = new ClientSession();

    expect(session.hasEnded).toBe(false);

    await session.endSession();

    expect(session.hasEnded).toBe(true);
  });

  it('should track inTransaction state', async () => {
    const session = new ClientSession();

    expect(session.inTransaction).toBe(false);

    session.startTransaction();
    expect(session.inTransaction).toBe(true);

    await session.commitTransaction();
    expect(session.inTransaction).toBe(false);

    session.startTransaction();
    expect(session.inTransaction).toBe(true);

    await session.abortTransaction();
    expect(session.inTransaction).toBe(false);

    await session.endSession();
  });

  it('should track transactionState correctly', async () => {
    const session = new ClientSession();

    expect(session.transactionState).toBe('none');

    session.startTransaction();
    expect(session.transactionState).toBe('starting');

    session.bufferOperation({ type: 'insert', collection: 'test', database: 'db', document: { _id: '1' } });
    expect(session.transactionState).toBe('in_progress');

    await session.commitTransaction();
    expect(session.transactionState).toBe('committed');

    session.startTransaction();
    await session.abortTransaction();
    expect(session.transactionState).toBe('aborted');

    await session.endSession();
  });

  it('should preserve createdAt timestamp throughout session lifecycle', async () => {
    const session = new ClientSession();
    const createdAt = session.createdAt.getTime();

    session.startTransaction();
    expect(session.createdAt.getTime()).toBe(createdAt);

    session.bufferOperation({ type: 'insert', collection: 'test', database: 'db', document: { _id: '1' } });
    expect(session.createdAt.getTime()).toBe(createdAt);

    await session.commitTransaction();
    expect(session.createdAt.getTime()).toBe(createdAt);

    await session.endSession();
    expect(session.createdAt.getTime()).toBe(createdAt);
  });

  it('should track txnNumber incrementing', async () => {
    const session = new ClientSession();

    expect(session.txnNumber).toBe(0);

    session.startTransaction();
    expect(session.txnNumber).toBe(1);

    await session.commitTransaction();
    expect(session.txnNumber).toBe(1);

    session.startTransaction();
    expect(session.txnNumber).toBe(2);

    await session.abortTransaction();
    expect(session.txnNumber).toBe(2);

    session.startTransaction();
    expect(session.txnNumber).toBe(3);

    await session.endSession();
  });

  it('should track operationCount correctly', async () => {
    const session = new ClientSession();

    expect(session.operationCount).toBe(0);

    session.startTransaction();
    expect(session.operationCount).toBe(0);

    session.bufferOperation({ type: 'insert', collection: 'test', database: 'db', document: { _id: '1' } });
    expect(session.operationCount).toBe(1);

    session.bufferOperation({ type: 'insert', collection: 'test', database: 'db', document: { _id: '2' } });
    expect(session.operationCount).toBe(2);

    session.bufferOperation({ type: 'update', collection: 'test', database: 'db', filter: { _id: '1' }, update: { $set: { x: 1 } } });
    expect(session.operationCount).toBe(3);

    await session.abortTransaction();
    expect(session.operationCount).toBe(0);

    await session.endSession();
  });
});

// ============================================================================
// Session Store Management Tests
// ============================================================================

describe('Session Management - Store Operations', () => {
  let store: SessionStore;

  beforeEach(() => {
    store = new SessionStore({ timeoutMs: 60000, cleanupIntervalMs: 0 });
  });

  afterEach(async () => {
    await store.closeAll();
  });

  it('should add and retrieve sessions', () => {
    const session = new ClientSession();
    store.add(session);

    const retrieved = store.get(session.id);
    expect(retrieved).toBe(session);
  });

  it('should remove sessions by ID', () => {
    const session = new ClientSession();
    store.add(session);

    expect(store.has(session.id)).toBe(true);

    const removed = store.remove(session.id);
    expect(removed).toBe(true);
    expect(store.has(session.id)).toBe(false);
  });

  it('should return false when removing non-existent session', () => {
    const removed = store.remove('non-existent-id');
    expect(removed).toBe(false);
  });

  it('should track store size', () => {
    expect(store.size).toBe(0);

    const session1 = new ClientSession();
    store.add(session1);
    expect(store.size).toBe(1);

    const session2 = new ClientSession();
    store.add(session2);
    expect(store.size).toBe(2);

    store.remove(session1.id);
    expect(store.size).toBe(1);
  });

  it('should return all session IDs', () => {
    const session1 = new ClientSession();
    const session2 = new ClientSession();
    const session3 = new ClientSession();

    store.add(session1);
    store.add(session2);
    store.add(session3);

    const ids = store.getSessionIds();

    expect(ids.length).toBe(3);
    expect(ids).toContain(session1.id);
    expect(ids).toContain(session2.id);
    expect(ids).toContain(session3.id);
  });

  it('should check if session exists', () => {
    const session = new ClientSession();

    expect(store.has(session.id)).toBe(false);

    store.add(session);

    expect(store.has(session.id)).toBe(true);
  });

  it('should return undefined for non-existent session', () => {
    const retrieved = store.get('non-existent-id');
    expect(retrieved).toBeUndefined();
  });
});

// ============================================================================
// Session Error Handling Tests
// ============================================================================

describe('Session Management - Error Handling', () => {
  it('should throw SessionError when using ended session', async () => {
    const session = new ClientSession();
    await session.endSession();

    expect(() => session.startTransaction()).toThrow(SessionError);
    expect(() => session.startTransaction()).toThrow('Cannot use a session that has ended.');
  });

  it('should throw SessionError on commit after session ended', async () => {
    const session = new ClientSession();
    session.startTransaction();
    await session.endSession();

    await expect(session.commitTransaction()).rejects.toThrow(SessionError);
  });

  it('should throw SessionError on abort after session ended', async () => {
    const session = new ClientSession();
    session.startTransaction();
    await session.endSession();

    await expect(session.abortTransaction()).rejects.toThrow(SessionError);
  });

  it('should throw SessionError on bufferOperation after session ended', async () => {
    const session = new ClientSession();
    session.startTransaction();
    await session.endSession();

    expect(() => session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'db',
      document: { _id: '1' },
    })).toThrow(SessionError);
  });

  it('should allow calling endSession multiple times', async () => {
    const session = new ClientSession();

    await session.endSession();
    await session.endSession();
    await session.endSession();

    expect(session.hasEnded).toBe(true);
  });
});
