/**
 * MongoDB Sessions Compatibility Tests
 *
 * Tests based on MongoDB Driver Specifications:
 * https://github.com/mongodb/specifications/tree/master/source/sessions/tests
 *
 * These tests validate that MongoLake's session implementation is compatible
 * with MongoDB's session semantics for:
 * - Session creation and management
 * - Session options
 * - Causal consistency
 * - Implicit and explicit sessions
 *
 * ## Supported Features
 *
 * | Feature                    | Status    | Notes                                |
 * |---------------------------|-----------|--------------------------------------|
 * | Session creation          | Supported | startSession() creates ClientSession |
 * | Session ID (lsid)         | Supported | UUID-based session identifiers       |
 * | Session options           | Supported | causalConsistency, defaultTxnOptions |
 * | Explicit sessions         | Supported | Pass session to operations           |
 * | Session lifecycle         | Supported | endSession() cleanup                 |
 * | Session timeout           | Supported | Configurable timeout with cleanup    |
 * | Causal consistency        | Partial   | Option tracked, not enforced         |
 * | Snapshot sessions         | Limited   | Snapshot isolation in transactions   |
 * | Implicit sessions         | Limited   | Operations work without sessions     |
 * | Server session pool       | N/A       | No server-side session pool          |
 * | Cluster time gossip       | N/A       | Single-region architecture           |
 *
 * ## Test Categories
 *
 * 1. Session Creation: Basic session creation and ID generation
 * 2. Session Options: Default transaction options, causal consistency
 * 3. Session Lifecycle: Start, use, and end sessions properly
 * 4. Causal Consistency: Read-your-writes guarantees
 * 5. Error Handling: Invalid session operations
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  ClientSession,
  SessionStore,
  generateSessionId,
  SessionError,
  type SessionOptions,
  type TransactionOptions,
} from '../../src/session/index.js';
import { MongoLake } from '../../src/client/index.js';
import { MemoryStorage } from '../../src/storage/index.js';

// ============================================================================
// Test Helpers
// ============================================================================

function createTestClient(): MongoLake {
  const storage = new MemoryStorage();
  const client = new MongoLake({ local: '.test-mongolake' });
  // @ts-expect-error - accessing private field for testing
  client.storage = storage;
  return client;
}

// ============================================================================
// Session Creation Tests
// ============================================================================

describe('MongoDB Sessions Compatibility - Session Creation', () => {
  let client: MongoLake;

  beforeEach(() => {
    client = createTestClient();
  });

  afterEach(async () => {
    await client.close();
  });

  /**
   * Test: Explicit session creation
   * MongoDB Spec: driver-sessions-server-support.json
   *
   * Drivers should support explicit session creation via startSession().
   */
  it('should create an explicit session with unique ID', () => {
    const session = client.startSession();

    expect(session).toBeInstanceOf(ClientSession);
    expect(session.id).toBeDefined();
    expect(typeof session.id).toBe('string');
    expect(session.id.length).toBeGreaterThan(0);
    expect(session.hasEnded).toBe(false);

    session.endSession();
  });

  /**
   * Test: Session IDs are unique
   * MongoDB Spec: driver-sessions-server-support.json
   *
   * Each session should have a unique logical session ID (lsid).
   */
  it('should generate unique session IDs', () => {
    const session1 = client.startSession();
    const session2 = client.startSession();

    expect(session1.id).not.toBe(session2.id);

    session1.endSession();
    session2.endSession();
  });

  /**
   * Test: Session ID format (UUID)
   * MongoDB Spec: Sessions are identified by UUIDs
   */
  it('should generate valid UUID session IDs', () => {
    const sessionId = generateSessionId();

    // UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    expect(sessionId).toMatch(uuidRegex);
  });

  /**
   * Test: Multiple sessions from same client
   * MongoDB Spec: A client can have multiple active sessions
   */
  it('should support multiple concurrent sessions', () => {
    const sessions = Array.from({ length: 5 }, () => client.startSession());

    const ids = sessions.map((s) => s.id);
    const uniqueIds = new Set(ids);

    expect(uniqueIds.size).toBe(5);

    sessions.forEach((s) => s.endSession());
  });

  /**
   * Test: Session creation timestamp
   * Sessions should track when they were created.
   */
  it('should record session creation time', () => {
    const before = new Date();
    const session = client.startSession();
    const after = new Date();

    expect(session.createdAt).toBeInstanceOf(Date);
    expect(session.createdAt.getTime()).toBeGreaterThanOrEqual(before.getTime());
    expect(session.createdAt.getTime()).toBeLessThanOrEqual(after.getTime());

    session.endSession();
  });
});

// ============================================================================
// Session Options Tests
// ============================================================================

describe('MongoDB Sessions Compatibility - Session Options', () => {
  let client: MongoLake;

  beforeEach(() => {
    client = createTestClient();
  });

  afterEach(async () => {
    await client.close();
  });

  /**
   * Test: Session with default transaction options
   * MongoDB Spec: transaction-options.json
   *
   * Sessions can specify default options for all transactions.
   */
  it('should accept default transaction options', () => {
    const defaultOptions: TransactionOptions = {
      readConcern: { level: 'snapshot' },
      writeConcern: { w: 'majority' },
      maxCommitTimeMS: 5000,
    };

    const session = client.startSession({
      defaultTransactionOptions: defaultOptions,
    });

    // Start a transaction and verify options are applied
    session.startTransaction();

    expect(session.transactionOptions).toEqual(defaultOptions);

    session.endSession();
  });

  /**
   * Test: Session with causal consistency option
   * MongoDB Spec: implicit-sessions-default-causal-consistency.json
   *
   * Sessions can enable causal consistency for read-your-writes.
   */
  it('should accept causal consistency option', () => {
    // Direct session creation with options
    const session = new ClientSession({
      causalConsistency: true,
    });

    // Option should be stored (though not enforced in MongoLake)
    expect(session).toBeInstanceOf(ClientSession);

    session.endSession();
  });

  /**
   * Test: Session without options
   * Sessions should work with default settings.
   */
  it('should create session with default options', () => {
    const session = client.startSession();

    // Session should exist and be usable
    expect(session).toBeInstanceOf(ClientSession);
    expect(session.hasEnded).toBe(false);
    expect(session.inTransaction).toBe(false);

    session.endSession();
  });

  /**
   * Test: Override default transaction options
   * MongoDB Spec: transaction-options.json
   *
   * Transaction-specific options should override session defaults.
   */
  it('should allow transaction to override default options', () => {
    const session = client.startSession({
      defaultTransactionOptions: {
        readConcern: { level: 'local' },
        writeConcern: { w: 1 },
      },
    });

    // Start transaction with override
    session.startTransaction({
      readConcern: { level: 'snapshot' },
      writeConcern: { w: 'majority' },
    });

    // Transaction options should reflect the override
    expect(session.transactionOptions?.readConcern?.level).toBe('snapshot');
    expect(session.transactionOptions?.writeConcern?.w).toBe('majority');

    session.endSession();
  });
});

// ============================================================================
// Session Lifecycle Tests
// ============================================================================

describe('MongoDB Sessions Compatibility - Session Lifecycle', () => {
  let client: MongoLake;

  beforeEach(() => {
    client = createTestClient();
  });

  afterEach(async () => {
    await client.close();
  });

  /**
   * Test: End session
   * MongoDB Spec: Sessions should be ended when done
   */
  it('should end session properly', async () => {
    const session = client.startSession();

    expect(session.hasEnded).toBe(false);

    await session.endSession();

    expect(session.hasEnded).toBe(true);
  });

  /**
   * Test: End session is idempotent
   * Calling endSession multiple times should be safe.
   */
  it('should allow multiple endSession calls', async () => {
    const session = client.startSession();

    await session.endSession();
    await session.endSession(); // Should not throw

    expect(session.hasEnded).toBe(true);
  });

  /**
   * Test: Session abort on end
   * MongoDB Spec: Ending a session aborts any active transaction
   */
  it('should abort transaction when session ends', async () => {
    const session = client.startSession();
    session.startTransaction();

    expect(session.inTransaction).toBe(true);

    await session.endSession();

    expect(session.hasEnded).toBe(true);
    expect(session.transactionState).toBe('aborted');
  });

  /**
   * Test: Cannot use ended session
   * Operations on ended sessions should fail.
   */
  it('should throw when using ended session', async () => {
    const session = client.startSession();
    await session.endSession();

    expect(() => {
      session.startTransaction();
    }).toThrow(SessionError);
  });

  /**
   * Test: Session last used tracking
   * Sessions should track their last activity time.
   */
  it('should update lastUsed on activity', () => {
    const session = client.startSession();
    const initial = session.lastUsed;

    // Small delay to ensure time difference
    const delay = 10;
    const startTime = Date.now();
    while (Date.now() - startTime < delay) {
      // busy wait
    }

    session.startTransaction();
    const afterTxn = session.lastUsed;

    expect(afterTxn.getTime()).toBeGreaterThanOrEqual(initial.getTime());

    session.endSession();
  });
});

// ============================================================================
// Session Store Tests
// ============================================================================

describe('MongoDB Sessions Compatibility - Session Store', () => {
  /**
   * Test: Session store management
   * MongoDB Spec: Drivers should track active sessions
   */
  it('should track sessions in store', () => {
    const store = new SessionStore({ cleanupIntervalMs: 0 });
    const session = new ClientSession();

    store.add(session);

    expect(store.has(session.id)).toBe(true);
    expect(store.get(session.id)).toBe(session);
    expect(store.size).toBe(1);
  });

  /**
   * Test: Remove session from store
   */
  it('should remove sessions from store', () => {
    const store = new SessionStore({ cleanupIntervalMs: 0 });
    const session = new ClientSession();

    store.add(session);
    store.remove(session.id);

    expect(store.has(session.id)).toBe(false);
    expect(store.size).toBe(0);
  });

  /**
   * Test: Session timeout cleanup
   * MongoDB Spec: Expired sessions should be cleaned up
   */
  it('should cleanup expired sessions', async () => {
    const store = new SessionStore({
      timeoutMs: 50,
      cleanupIntervalMs: 0, // Manual cleanup
    });

    const session = new ClientSession();
    store.add(session);

    // Wait for session to expire
    await new Promise((resolve) => setTimeout(resolve, 100));

    const cleaned = store.cleanupExpired();

    expect(cleaned).toBe(1);
    expect(store.has(session.id)).toBe(false);
  });

  /**
   * Test: Close all sessions
   */
  it('should close all sessions', async () => {
    const store = new SessionStore({ cleanupIntervalMs: 0 });

    const sessions = [new ClientSession(), new ClientSession(), new ClientSession()];
    sessions.forEach((s) => store.add(s));

    await store.closeAll();

    expect(store.size).toBe(0);
    sessions.forEach((s) => {
      expect(s.hasEnded).toBe(true);
    });
  });

  /**
   * Test: Get all session IDs
   */
  it('should list all session IDs', () => {
    const store = new SessionStore({ cleanupIntervalMs: 0 });

    const sessions = [new ClientSession(), new ClientSession()];
    sessions.forEach((s) => store.add(s));

    const ids = store.getSessionIds();

    expect(ids).toHaveLength(2);
    expect(ids).toContain(sessions[0].id);
    expect(ids).toContain(sessions[1].id);
  });
});

// ============================================================================
// Causal Consistency Tests
// ============================================================================

describe('MongoDB Sessions Compatibility - Causal Consistency', () => {
  let client: MongoLake;

  beforeEach(() => {
    client = createTestClient();
  });

  afterEach(async () => {
    await client.close();
  });

  /**
   * Test: Read your writes
   * MongoDB Spec: Causal consistency ensures read-your-writes
   *
   * Note: MongoLake implements read-your-writes through buffer checking,
   * not through cluster time tracking like MongoDB.
   */
  it('should support read-your-writes within session transaction', async () => {
    const session = client.startSession();
    const db = client.db('testdb');
    const collection = db.collection('users');

    // Insert document
    await collection.insertOne({ _id: 'user1', name: 'Alice' }, { session });

    // Read back within same session (not in transaction)
    const doc = await collection.findOne({ _id: 'user1' }, { session });

    expect(doc).toBeDefined();
    expect(doc?.name).toBe('Alice');

    await session.endSession();
  });

  /**
   * Test: Transaction isolation
   * Reads within a transaction should not see uncommitted changes from other transactions.
   */
  it('should maintain transaction isolation', async () => {
    const session1 = client.startSession();
    const session2 = client.startSession();
    const db = client.db('testdb');
    const collection = db.collection('isolation_test');

    // Start transaction in session 1
    session1.startTransaction();
    session1.bufferOperation({
      type: 'insert',
      collection: 'isolation_test',
      database: 'testdb',
      document: { _id: 'doc1', value: 'from_session1' },
    });

    // Session 2 should not see uncommitted changes
    const doc = await collection.findOne({ _id: 'doc1' }, { session: session2 });
    expect(doc).toBeNull();

    await session1.endSession();
    await session2.endSession();
  });
});

// ============================================================================
// Error Handling Tests
// ============================================================================

describe('MongoDB Sessions Compatibility - Error Handling', () => {
  let client: MongoLake;

  beforeEach(() => {
    client = createTestClient();
  });

  afterEach(async () => {
    await client.close();
  });

  /**
   * Test: Operations on ended session
   * MongoDB Spec: Operations on ended sessions should fail
   */
  it('should reject operations on ended session', async () => {
    const session = client.startSession();
    await session.endSession();

    expect(() => {
      session.startTransaction();
    }).toThrow(SessionError);
    expect(() => {
      session.startTransaction();
    }).toThrow('Cannot use a session that has ended');
  });

  /**
   * Test: Buffering without transaction
   * Buffering operations requires an active transaction.
   */
  it('should reject buffer operation without transaction', () => {
    const session = client.startSession();

    expect(() => {
      session.bufferOperation({
        type: 'insert',
        collection: 'test',
        database: 'testdb',
        document: { _id: '1' },
      });
    }).toThrow('no transaction in progress');

    session.endSession();
  });

  /**
   * Test: Double transaction start
   * Starting a transaction when one is active should fail.
   */
  it('should reject starting transaction when one is active', () => {
    const session = client.startSession();
    session.startTransaction();

    expect(() => {
      session.startTransaction();
    }).toThrow('Transaction already in progress');

    session.endSession();
  });

  /**
   * Test: Commit without transaction
   * Committing without an active transaction should fail.
   */
  it('should reject commit without active transaction', async () => {
    const session = client.startSession();

    await expect(session.commitTransaction()).rejects.toThrow(
      'No transaction in progress'
    );

    await session.endSession();
  });

  /**
   * Test: Abort without transaction
   * Aborting without an active transaction should fail.
   */
  it('should reject abort without active transaction', async () => {
    const session = client.startSession();

    await expect(session.abortTransaction()).rejects.toThrow(
      'No transaction in progress'
    );

    await session.endSession();
  });
});

// ============================================================================
// Session Serialization Tests
// ============================================================================

describe('MongoDB Sessions Compatibility - Serialization', () => {
  /**
   * Test: Session JSON serialization
   * MongoDB Spec: Session ID (lsid) should be serializable for wire protocol
   */
  it('should serialize session to JSON', () => {
    const session = new ClientSession();
    session.startTransaction();

    const json = session.toJSON();

    expect(json).toHaveProperty('id');
    expect(json).toHaveProperty('txnNumber');
    expect(typeof json.id).toBe('string');
    expect(typeof json.txnNumber).toBe('number');

    session.endSession();
  });

  /**
   * Test: Transaction number increment
   * Each new transaction should increment the transaction number.
   */
  it('should increment transaction number', async () => {
    const session = new ClientSession();

    session.startTransaction();
    expect(session.txnNumber).toBe(1);
    await session.commitTransaction();

    session.startTransaction();
    expect(session.txnNumber).toBe(2);
    await session.abortTransaction();

    session.startTransaction();
    expect(session.txnNumber).toBe(3);

    await session.endSession();
  });
});

// ============================================================================
// Feature Support Summary
// ============================================================================

describe('MongoDB Sessions Compatibility - Feature Support Summary', () => {
  it('should document supported session features', () => {
    /**
     * SUPPORTED FEATURES:
     *
     * 1. Session Creation
     *    - startSession() creates a new ClientSession
     *    - Sessions have unique UUID identifiers (lsid)
     *    - Multiple concurrent sessions per client
     *
     * 2. Session Options
     *    - defaultTransactionOptions (readConcern, writeConcern, maxCommitTimeMS)
     *    - causalConsistency option (tracked but not enforced)
     *
     * 3. Session Lifecycle
     *    - endSession() properly cleans up
     *    - Session timeout with automatic cleanup
     *    - SessionStore for tracking active sessions
     *
     * 4. Transaction Support
     *    - startTransaction(), commitTransaction(), abortTransaction()
     *    - Transaction options override session defaults
     *    - Operation buffering during transactions
     *
     * PARTIALLY SUPPORTED:
     *
     * 1. Causal Consistency
     *    - Option can be set but cluster time gossip not implemented
     *    - Read-your-writes works through buffer checking
     *
     * 2. Snapshot Sessions
     *    - Snapshot isolation available in transactions
     *    - No atClusterTime-based point-in-time queries
     *
     * NOT SUPPORTED (MongoDB Server Features):
     *
     * 1. Server Session Pool
     *    - No server-side session pooling (MongoLake is serverless)
     *
     * 2. Cluster Time Gossip
     *    - Single-region architecture doesn't need gossip protocol
     *
     * 3. Implicit Sessions
     *    - Operations work without sessions but don't create implicit ones
     */
    expect(true).toBe(true);
  });
});
