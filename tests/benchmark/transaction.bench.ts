/**
 * Transaction Benchmark - Vitest bench suite
 *
 * Measures transaction performance for MongoLake operations including:
 * - Transaction commit latency (single shard)
 * - Transaction abort latency
 * - Transaction with varying operation counts (10, 100, 1000)
 *
 * Run with: pnpm run benchmark:vitest
 */

import { bench, describe, beforeAll } from 'vitest';
import { ClientSession } from '../../src/session/index.js';
import { TransactionManager } from '../../src/transaction/index.js';
import { generateSimpleDoc, generateMediumDoc } from './utils.js';

// ============================================================================
// Test Data Setup
// ============================================================================

interface MockShardRouter {
  routeWithDatabase(database: string, collection: string): { shardId: number };
}

interface MockShardRPC {
  sendPrepare: () => Promise<{ type: 'prepared'; txnId: string; shardId: number; timestamp: number; preparedLSN: number }>;
  sendCommit: () => Promise<{ type: 'ack'; txnId: string; shardId: number; timestamp: number; finalLSN: number }>;
  sendAbort: () => Promise<{ type: 'ack'; txnId: string; shardId: number; timestamp: number }>;
}

/**
 * Creates a mock session with a commit handler that simulates commit latency.
 */
function createMockSession(commitDelayMs: number = 0): ClientSession {
  const session = new ClientSession();
  session.setCommitHandler(async () => {
    if (commitDelayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, commitDelayMs));
    }
  });
  return session;
}

/**
 * Creates a mock TransactionManager for benchmarking.
 */
function createMockTransactionManager(commitDelayMs: number = 0): TransactionManager {
  const session = createMockSession(commitDelayMs);
  return new TransactionManager(session);
}

// ============================================================================
// Transaction Commit Latency Benchmarks
// ============================================================================

describe('transaction commit latency (single shard)', () => {
  bench('commit empty transaction', async () => {
    const txn = createMockTransactionManager();
    txn.begin();
    await txn.commit();
  }, {
    iterations: 500,
    warmupIterations: 50,
  });

  bench('commit with 1 insert operation', async () => {
    const txn = createMockTransactionManager();
    txn.begin();
    txn.insert('testdb', 'testcoll', generateSimpleDoc(0));
    await txn.commit();
  }, {
    iterations: 500,
    warmupIterations: 50,
  });

  bench('commit with 5 mixed operations', async () => {
    const txn = createMockTransactionManager();
    txn.begin();
    txn.insert('testdb', 'testcoll', generateSimpleDoc(0));
    txn.insert('testdb', 'testcoll', generateSimpleDoc(1));
    txn.update('testdb', 'testcoll', { _id: 'doc-0' }, { $set: { updated: true } });
    txn.delete('testdb', 'testcoll', { _id: 'doc-1' });
    txn.replace('testdb', 'testcoll', { _id: 'doc-2' }, generateSimpleDoc(2));
    await txn.commit();
  }, {
    iterations: 300,
    warmupIterations: 30,
  });
});

// ============================================================================
// Transaction Abort Latency Benchmarks
// ============================================================================

describe('transaction abort latency', () => {
  bench('abort empty transaction', async () => {
    const txn = createMockTransactionManager();
    txn.begin();
    await txn.abort();
  }, {
    iterations: 500,
    warmupIterations: 50,
  });

  bench('abort with 1 insert operation', async () => {
    const txn = createMockTransactionManager();
    txn.begin();
    txn.insert('testdb', 'testcoll', generateSimpleDoc(0));
    await txn.abort();
  }, {
    iterations: 500,
    warmupIterations: 50,
  });

  bench('abort with 10 operations', async () => {
    const txn = createMockTransactionManager();
    txn.begin();
    for (let i = 0; i < 10; i++) {
      txn.insert('testdb', 'testcoll', generateSimpleDoc(i));
    }
    await txn.abort();
  }, {
    iterations: 300,
    warmupIterations: 30,
  });

  bench('abort with 100 operations', async () => {
    const txn = createMockTransactionManager();
    txn.begin();
    for (let i = 0; i < 100; i++) {
      txn.insert('testdb', 'testcoll', generateSimpleDoc(i));
    }
    await txn.abort();
  }, {
    iterations: 100,
    warmupIterations: 10,
  });
});

// ============================================================================
// Transaction Operation Count Benchmarks
// ============================================================================

describe('transaction with 10 operations', () => {
  bench('10 inserts - simple docs', async () => {
    const txn = createMockTransactionManager();
    txn.begin();
    for (let i = 0; i < 10; i++) {
      txn.insert('testdb', 'testcoll', generateSimpleDoc(i));
    }
    await txn.commit();
  }, {
    iterations: 200,
    warmupIterations: 20,
  });

  bench('10 inserts - medium docs', async () => {
    const txn = createMockTransactionManager();
    txn.begin();
    for (let i = 0; i < 10; i++) {
      txn.insert('testdb', 'testcoll', generateMediumDoc(i));
    }
    await txn.commit();
  }, {
    iterations: 200,
    warmupIterations: 20,
  });

  bench('10 mixed operations', async () => {
    const txn = createMockTransactionManager();
    txn.begin();
    for (let i = 0; i < 4; i++) {
      txn.insert('testdb', 'testcoll', generateSimpleDoc(i));
    }
    for (let i = 0; i < 3; i++) {
      txn.update('testdb', 'testcoll', { _id: `doc-${i}` }, { $set: { updated: true } });
    }
    for (let i = 0; i < 3; i++) {
      txn.delete('testdb', 'testcoll', { _id: `doc-${i}` });
    }
    await txn.commit();
  }, {
    iterations: 200,
    warmupIterations: 20,
  });
});

describe('transaction with 100 operations', () => {
  bench('100 inserts - simple docs', async () => {
    const txn = createMockTransactionManager();
    txn.begin();
    for (let i = 0; i < 100; i++) {
      txn.insert('testdb', 'testcoll', generateSimpleDoc(i));
    }
    await txn.commit();
  }, {
    iterations: 100,
    warmupIterations: 10,
  });

  bench('100 inserts - medium docs', async () => {
    const txn = createMockTransactionManager();
    txn.begin();
    for (let i = 0; i < 100; i++) {
      txn.insert('testdb', 'testcoll', generateMediumDoc(i));
    }
    await txn.commit();
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('100 mixed operations', async () => {
    const txn = createMockTransactionManager();
    txn.begin();
    for (let i = 0; i < 40; i++) {
      txn.insert('testdb', 'testcoll', generateSimpleDoc(i));
    }
    for (let i = 0; i < 30; i++) {
      txn.update('testdb', 'testcoll', { _id: `doc-${i}` }, { $set: { updated: true } });
    }
    for (let i = 0; i < 30; i++) {
      txn.delete('testdb', 'testcoll', { _id: `doc-${i}` });
    }
    await txn.commit();
  }, {
    iterations: 100,
    warmupIterations: 10,
  });
});

describe('transaction with 1000 operations', () => {
  bench('1000 inserts - simple docs', async () => {
    const txn = createMockTransactionManager();
    txn.begin();
    for (let i = 0; i < 1000; i++) {
      txn.insert('testdb', 'testcoll', generateSimpleDoc(i));
    }
    await txn.commit();
  }, {
    iterations: 20,
    warmupIterations: 3,
  });

  bench('1000 inserts - medium docs', async () => {
    const txn = createMockTransactionManager();
    txn.begin();
    for (let i = 0; i < 1000; i++) {
      txn.insert('testdb', 'testcoll', generateMediumDoc(i));
    }
    await txn.commit();
  }, {
    iterations: 10,
    warmupIterations: 2,
  });

  bench('1000 mixed operations', async () => {
    const txn = createMockTransactionManager();
    txn.begin();
    for (let i = 0; i < 400; i++) {
      txn.insert('testdb', 'testcoll', generateSimpleDoc(i));
    }
    for (let i = 0; i < 300; i++) {
      txn.update('testdb', 'testcoll', { _id: `doc-${i}` }, { $set: { updated: true } });
    }
    for (let i = 0; i < 300; i++) {
      txn.delete('testdb', 'testcoll', { _id: `doc-${i}` });
    }
    await txn.commit();
  }, {
    iterations: 20,
    warmupIterations: 3,
  });
});

// ============================================================================
// Transaction State Transition Benchmarks
// ============================================================================

describe('transaction lifecycle overhead', () => {
  bench('begin-commit cycle (empty)', async () => {
    const session = createMockSession();
    session.startTransaction();
    await session.commitTransaction();
  }, {
    iterations: 1000,
    warmupIterations: 100,
  });

  bench('begin-abort cycle (empty)', async () => {
    const session = createMockSession();
    session.startTransaction();
    await session.abortTransaction();
  }, {
    iterations: 1000,
    warmupIterations: 100,
  });

  bench('full session lifecycle (start session, begin, commit, end)', async () => {
    const session = createMockSession();
    session.startTransaction();
    session.bufferOperation({
      type: 'insert',
      database: 'testdb',
      collection: 'testcoll',
      document: generateSimpleDoc(0),
    });
    await session.commitTransaction();
    await session.endSession();
  }, {
    iterations: 500,
    warmupIterations: 50,
  });

  bench('multiple transactions on same session', async () => {
    const session = createMockSession();

    // First transaction
    session.startTransaction();
    session.bufferOperation({
      type: 'insert',
      database: 'testdb',
      collection: 'testcoll',
      document: generateSimpleDoc(0),
    });
    await session.commitTransaction();

    // Second transaction
    session.startTransaction();
    session.bufferOperation({
      type: 'insert',
      database: 'testdb',
      collection: 'testcoll',
      document: generateSimpleDoc(1),
    });
    await session.commitTransaction();

    // Third transaction
    session.startTransaction();
    session.bufferOperation({
      type: 'insert',
      database: 'testdb',
      collection: 'testcoll',
      document: generateSimpleDoc(2),
    });
    await session.commitTransaction();

    await session.endSession();
  }, {
    iterations: 200,
    warmupIterations: 20,
  });
});

// ============================================================================
// Operation Buffering Benchmarks
// ============================================================================

describe('operation buffering performance', () => {
  bench('buffer 10 operations sequentially', () => {
    const session = new ClientSession();
    session.startTransaction();
    for (let i = 0; i < 10; i++) {
      session.bufferOperation({
        type: 'insert',
        database: 'testdb',
        collection: 'testcoll',
        document: generateSimpleDoc(i),
      });
    }
  }, {
    iterations: 500,
    warmupIterations: 50,
  });

  bench('buffer 100 operations sequentially', () => {
    const session = new ClientSession();
    session.startTransaction();
    for (let i = 0; i < 100; i++) {
      session.bufferOperation({
        type: 'insert',
        database: 'testdb',
        collection: 'testcoll',
        document: generateSimpleDoc(i),
      });
    }
  }, {
    iterations: 200,
    warmupIterations: 20,
  });

  bench('buffer 1000 operations sequentially', () => {
    const session = new ClientSession();
    session.startTransaction();
    for (let i = 0; i < 1000; i++) {
      session.bufferOperation({
        type: 'insert',
        database: 'testdb',
        collection: 'testcoll',
        document: generateSimpleDoc(i),
      });
    }
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('get buffered operations (1000 ops)', () => {
    const session = new ClientSession();
    session.startTransaction();
    for (let i = 0; i < 1000; i++) {
      session.bufferOperation({
        type: 'insert',
        database: 'testdb',
        collection: 'testcoll',
        document: generateSimpleDoc(i),
      });
    }
    // Benchmark the retrieval
    const ops = session.getBufferedOperations();
    return ops.length;
  }, {
    iterations: 50,
    warmupIterations: 5,
  });
});
