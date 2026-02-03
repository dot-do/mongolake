/**
 * Connection Benchmark - Vitest bench suite
 *
 * Measures connection-related performance for MongoLake operations including:
 * - Connection establishment time (session creation)
 * - Connection pool exhaustion recovery
 * - Concurrent connection handling (10, 50, 100 connections)
 *
 * Run with: pnpm run benchmark:vitest
 */

import { bench, describe, beforeEach, afterEach } from 'vitest';
import {
  ClientSession,
  SessionStore,
  generateSessionId,
} from '../../src/session/index.js';

// ============================================================================
// Connection Establishment Benchmarks
// ============================================================================

describe('connection establishment time', () => {
  bench('create single session', () => {
    const session = new ClientSession();
    return session.id;
  }, {
    iterations: 1000,
    warmupIterations: 100,
  });

  bench('create session with options', () => {
    const session = new ClientSession({
      causalConsistency: true,
      defaultTransactionOptions: {
        readConcern: { level: 'snapshot' },
        writeConcern: { w: 'majority', j: true },
        maxCommitTimeMS: 5000,
      },
    });
    return session.id;
  }, {
    iterations: 1000,
    warmupIterations: 100,
  });

  bench('generate session ID only', () => {
    return generateSessionId();
  }, {
    iterations: 2000,
    warmupIterations: 200,
  });

  bench('create and end session', async () => {
    const session = new ClientSession();
    await session.endSession();
  }, {
    iterations: 500,
    warmupIterations: 50,
  });

  bench('create session and start transaction', () => {
    const session = new ClientSession();
    session.startTransaction();
    return session.txnNumber;
  }, {
    iterations: 500,
    warmupIterations: 50,
  });
});

// ============================================================================
// Session Store Benchmarks (Connection Pool Simulation)
// ============================================================================

describe('session store operations', () => {
  let store: SessionStore;

  beforeEach(() => {
    store = new SessionStore({ cleanupIntervalMs: 0 }); // Disable auto-cleanup for benchmarks
  });

  afterEach(async () => {
    await store.closeAll();
  });

  bench('add session to store', () => {
    const session = new ClientSession();
    store.add(session);
    return store.size;
  }, {
    iterations: 1000,
    warmupIterations: 100,
  });

  bench('get session from store', () => {
    // Pre-populate store
    const sessions: ClientSession[] = [];
    for (let i = 0; i < 100; i++) {
      const session = new ClientSession();
      store.add(session);
      sessions.push(session);
    }

    // Benchmark lookup
    const targetSession = sessions[50];
    return store.get(targetSession.id);
  }, {
    iterations: 500,
    warmupIterations: 50,
  });

  bench('check session exists', () => {
    // Pre-populate store
    const sessions: ClientSession[] = [];
    for (let i = 0; i < 100; i++) {
      const session = new ClientSession();
      store.add(session);
      sessions.push(session);
    }

    // Benchmark has() check
    const targetSession = sessions[50];
    return store.has(targetSession.id);
  }, {
    iterations: 500,
    warmupIterations: 50,
  });

  bench('remove session from store', () => {
    const session = new ClientSession();
    store.add(session);
    return store.remove(session.id);
  }, {
    iterations: 500,
    warmupIterations: 50,
  });
});

// ============================================================================
// Connection Pool Exhaustion Recovery Benchmarks
// ============================================================================

describe('connection pool exhaustion recovery', () => {
  bench('create and cleanup 100 sessions', async () => {
    const store = new SessionStore({ cleanupIntervalMs: 0 });

    // Create 100 sessions
    const sessions: ClientSession[] = [];
    for (let i = 0; i < 100; i++) {
      const session = new ClientSession();
      store.add(session);
      sessions.push(session);
    }

    // End all sessions
    for (const session of sessions) {
      await session.endSession();
    }

    // Cleanup expired
    const cleaned = store.cleanupExpired();
    return cleaned;
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('rapid session creation/destruction', async () => {
    const store = new SessionStore({ cleanupIntervalMs: 0 });

    // Rapid create/destroy cycle
    for (let i = 0; i < 10; i++) {
      const session = new ClientSession();
      store.add(session);
      await session.endSession();
      store.remove(session.id);
    }
  }, {
    iterations: 100,
    warmupIterations: 10,
  });

  bench('cleanup expired sessions (100 ended)', async () => {
    const store = new SessionStore({ cleanupIntervalMs: 0 });

    // Create and end sessions
    for (let i = 0; i < 100; i++) {
      const session = new ClientSession();
      store.add(session);
      await session.endSession();
    }

    // Benchmark cleanup
    return store.cleanupExpired();
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('mixed active/ended session cleanup', async () => {
    const store = new SessionStore({ cleanupIntervalMs: 0 });

    // Create mix of active and ended sessions
    for (let i = 0; i < 100; i++) {
      const session = new ClientSession();
      store.add(session);
      if (i % 2 === 0) {
        await session.endSession();
      }
    }

    // Benchmark cleanup (should only clean ended ones)
    return store.cleanupExpired();
  }, {
    iterations: 50,
    warmupIterations: 5,
  });
});

// ============================================================================
// Concurrent Connection Handling - 10 Connections
// ============================================================================

describe('concurrent connection handling (10 connections)', () => {
  bench('create 10 sessions concurrently', async () => {
    const promises = Array.from({ length: 10 }, () =>
      Promise.resolve(new ClientSession())
    );
    const sessions = await Promise.all(promises);
    return sessions.length;
  }, {
    iterations: 200,
    warmupIterations: 20,
  });

  bench('10 concurrent transactions', async () => {
    const sessions = Array.from({ length: 10 }, () => {
      const session = new ClientSession();
      session.setCommitHandler(async () => {});
      return session;
    });

    // Start and commit transactions concurrently
    await Promise.all(
      sessions.map(async (session) => {
        session.startTransaction();
        session.bufferOperation({
          type: 'insert',
          database: 'testdb',
          collection: 'testcoll',
          document: { _id: session.id },
        });
        await session.commitTransaction();
      })
    );

    return sessions.length;
  }, {
    iterations: 100,
    warmupIterations: 10,
  });

  bench('10 concurrent session lifecycle', async () => {
    const sessions = Array.from({ length: 10 }, () => {
      const session = new ClientSession();
      session.setCommitHandler(async () => {});
      return session;
    });

    await Promise.all(
      sessions.map(async (session) => {
        session.startTransaction();
        session.bufferOperation({
          type: 'insert',
          database: 'testdb',
          collection: 'testcoll',
          document: { _id: session.id },
        });
        await session.commitTransaction();
        await session.endSession();
      })
    );

    return sessions.length;
  }, {
    iterations: 100,
    warmupIterations: 10,
  });
});

// ============================================================================
// Concurrent Connection Handling - 50 Connections
// ============================================================================

describe('concurrent connection handling (50 connections)', () => {
  bench('create 50 sessions concurrently', async () => {
    const promises = Array.from({ length: 50 }, () =>
      Promise.resolve(new ClientSession())
    );
    const sessions = await Promise.all(promises);
    return sessions.length;
  }, {
    iterations: 100,
    warmupIterations: 10,
  });

  bench('50 concurrent transactions', async () => {
    const sessions = Array.from({ length: 50 }, () => {
      const session = new ClientSession();
      session.setCommitHandler(async () => {});
      return session;
    });

    await Promise.all(
      sessions.map(async (session) => {
        session.startTransaction();
        session.bufferOperation({
          type: 'insert',
          database: 'testdb',
          collection: 'testcoll',
          document: { _id: session.id },
        });
        await session.commitTransaction();
      })
    );

    return sessions.length;
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('50 concurrent session lifecycle', async () => {
    const sessions = Array.from({ length: 50 }, () => {
      const session = new ClientSession();
      session.setCommitHandler(async () => {});
      return session;
    });

    await Promise.all(
      sessions.map(async (session) => {
        session.startTransaction();
        session.bufferOperation({
          type: 'insert',
          database: 'testdb',
          collection: 'testcoll',
          document: { _id: session.id },
        });
        await session.commitTransaction();
        await session.endSession();
      })
    );

    return sessions.length;
  }, {
    iterations: 50,
    warmupIterations: 5,
  });
});

// ============================================================================
// Concurrent Connection Handling - 100 Connections
// ============================================================================

describe('concurrent connection handling (100 connections)', () => {
  bench('create 100 sessions concurrently', async () => {
    const promises = Array.from({ length: 100 }, () =>
      Promise.resolve(new ClientSession())
    );
    const sessions = await Promise.all(promises);
    return sessions.length;
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('100 concurrent transactions', async () => {
    const sessions = Array.from({ length: 100 }, () => {
      const session = new ClientSession();
      session.setCommitHandler(async () => {});
      return session;
    });

    await Promise.all(
      sessions.map(async (session) => {
        session.startTransaction();
        session.bufferOperation({
          type: 'insert',
          database: 'testdb',
          collection: 'testcoll',
          document: { _id: session.id },
        });
        await session.commitTransaction();
      })
    );

    return sessions.length;
  }, {
    iterations: 30,
    warmupIterations: 3,
  });

  bench('100 concurrent session lifecycle', async () => {
    const sessions = Array.from({ length: 100 }, () => {
      const session = new ClientSession();
      session.setCommitHandler(async () => {});
      return session;
    });

    await Promise.all(
      sessions.map(async (session) => {
        session.startTransaction();
        session.bufferOperation({
          type: 'insert',
          database: 'testdb',
          collection: 'testcoll',
          document: { _id: session.id },
        });
        await session.commitTransaction();
        await session.endSession();
      })
    );

    return sessions.length;
  }, {
    iterations: 30,
    warmupIterations: 3,
  });

  bench('100 sessions with store management', async () => {
    const store = new SessionStore({ cleanupIntervalMs: 0 });
    const sessions: ClientSession[] = [];

    // Create and add sessions
    for (let i = 0; i < 100; i++) {
      const session = new ClientSession();
      session.setCommitHandler(async () => {});
      store.add(session);
      sessions.push(session);
    }

    // Run transactions concurrently
    await Promise.all(
      sessions.map(async (session) => {
        session.startTransaction();
        session.bufferOperation({
          type: 'insert',
          database: 'testdb',
          collection: 'testcoll',
          document: { _id: session.id },
        });
        await session.commitTransaction();
      })
    );

    // Cleanup
    await store.closeAll();
    return sessions.length;
  }, {
    iterations: 20,
    warmupIterations: 2,
  });
});

// ============================================================================
// Session ID Generation Performance
// ============================================================================

describe('session ID generation', () => {
  bench('generate 100 session IDs sequentially', () => {
    const ids: string[] = [];
    for (let i = 0; i < 100; i++) {
      ids.push(generateSessionId());
    }
    return ids.length;
  }, {
    iterations: 200,
    warmupIterations: 20,
  });

  bench('generate 1000 session IDs sequentially', () => {
    const ids: string[] = [];
    for (let i = 0; i < 1000; i++) {
      ids.push(generateSessionId());
    }
    return ids.length;
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('uniqueness check for 1000 IDs', () => {
    const ids = new Set<string>();
    for (let i = 0; i < 1000; i++) {
      ids.add(generateSessionId());
    }
    return ids.size === 1000;
  }, {
    iterations: 50,
    warmupIterations: 5,
  });
});

// ============================================================================
// Session Store Scalability
// ============================================================================

describe('session store scalability', () => {
  bench('store with 1000 sessions - lookup', async () => {
    const store = new SessionStore({ cleanupIntervalMs: 0 });
    const sessions: ClientSession[] = [];

    // Pre-populate
    for (let i = 0; i < 1000; i++) {
      const session = new ClientSession();
      store.add(session);
      sessions.push(session);
    }

    // Benchmark random lookups
    for (let i = 0; i < 100; i++) {
      const idx = Math.floor(Math.random() * 1000);
      store.get(sessions[idx].id);
    }

    await store.closeAll();
  }, {
    iterations: 20,
    warmupIterations: 2,
  });

  bench('store with 1000 sessions - getSessionIds', async () => {
    const store = new SessionStore({ cleanupIntervalMs: 0 });

    // Pre-populate
    for (let i = 0; i < 1000; i++) {
      const session = new ClientSession();
      store.add(session);
    }

    // Benchmark getting all IDs
    const ids = store.getSessionIds();

    await store.closeAll();
    return ids.length;
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('store closeAll with 100 active sessions', async () => {
    const store = new SessionStore({ cleanupIntervalMs: 0 });

    // Pre-populate
    for (let i = 0; i < 100; i++) {
      const session = new ClientSession();
      store.add(session);
    }

    // Benchmark closeAll
    await store.closeAll();
  }, {
    iterations: 30,
    warmupIterations: 3,
  });
});
