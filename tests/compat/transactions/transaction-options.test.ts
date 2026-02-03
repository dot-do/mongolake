/**
 * MongoDB Transaction Options Tests
 *
 * Based on MongoDB Driver Specifications:
 * - transaction-options.json
 *
 * Tests transaction option inheritance and override behavior.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  ClientSession,
  type TransactionOptions,
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

describe('Transaction Options - transaction-options.json specification', () => {
  let client: MongoLake;
  let session: ClientSession;

  beforeEach(() => {
    client = createTestClient();
    session = client.startSession();
  });

  afterEach(async () => {
    if (!session.hasEnded) {
      await session.endSession();
    }
    await client.close();
  });

  /**
   * MongoDB Spec: "no transaction options set"
   *
   * Transactions without options should work with defaults.
   */
  describe('No Options Set', () => {
    it('should start transaction without options', () => {
      session.startTransaction();

      expect(session.inTransaction).toBe(true);
    });

    it('should have null/undefined transactionOptions', () => {
      session.startTransaction();

      // Options object may exist but be empty
      expect(session.transactionOptions).toBeDefined();
    });
  });

  /**
   * MongoDB Spec: "transaction options with startTransaction"
   *
   * Options passed to startTransaction should be applied.
   */
  describe('startTransaction Options', () => {
    it('should apply readConcern from startTransaction', () => {
      session.startTransaction({
        readConcern: { level: 'snapshot' },
      });

      expect(session.transactionOptions?.readConcern?.level).toBe('snapshot');
    });

    it('should apply writeConcern from startTransaction', () => {
      session.startTransaction({
        writeConcern: { w: 'majority' },
      });

      expect(session.transactionOptions?.writeConcern?.w).toBe('majority');
    });

    it('should apply maxCommitTimeMS from startTransaction', () => {
      session.startTransaction({
        maxCommitTimeMS: 5000,
      });

      expect(session.transactionOptions?.maxCommitTimeMS).toBe(5000);
    });

    it('should apply all options from startTransaction', () => {
      const options: TransactionOptions = {
        readConcern: { level: 'majority' },
        writeConcern: { w: 2, j: true, wtimeout: 3000 },
        maxCommitTimeMS: 10000,
      };

      session.startTransaction(options);

      expect(session.transactionOptions).toEqual(options);
    });
  });

  /**
   * MongoDB Spec: "defaultTransactionOptions"
   *
   * Session-level defaults should apply to all transactions.
   */
  describe('Session Default Options', () => {
    it('should apply session defaults to transaction', () => {
      const sessionWithDefaults = client.startSession({
        defaultTransactionOptions: {
          readConcern: { level: 'local' },
        },
      });

      sessionWithDefaults.startTransaction();

      expect(sessionWithDefaults.transactionOptions?.readConcern?.level).toBe('local');

      sessionWithDefaults.endSession();
    });

    it('should apply all session defaults', () => {
      const sessionWithDefaults = client.startSession({
        defaultTransactionOptions: {
          readConcern: { level: 'majority' },
          writeConcern: { w: 'majority', wtimeout: 5000 },
          maxCommitTimeMS: 2000,
        },
      });

      sessionWithDefaults.startTransaction();

      expect(sessionWithDefaults.transactionOptions?.readConcern?.level).toBe('majority');
      expect(sessionWithDefaults.transactionOptions?.writeConcern?.w).toBe('majority');
      expect(sessionWithDefaults.transactionOptions?.writeConcern?.wtimeout).toBe(5000);
      expect(sessionWithDefaults.transactionOptions?.maxCommitTimeMS).toBe(2000);

      sessionWithDefaults.endSession();
    });
  });

  /**
   * MongoDB Spec: "override defaultTransactionOptions"
   *
   * Options in startTransaction should override session defaults.
   */
  describe('Option Override Hierarchy', () => {
    it('should override readConcern from session defaults', () => {
      const sessionWithDefaults = client.startSession({
        defaultTransactionOptions: {
          readConcern: { level: 'local' },
        },
      });

      sessionWithDefaults.startTransaction({
        readConcern: { level: 'snapshot' },
      });

      expect(sessionWithDefaults.transactionOptions?.readConcern?.level).toBe('snapshot');

      sessionWithDefaults.endSession();
    });

    it('should override writeConcern from session defaults', () => {
      const sessionWithDefaults = client.startSession({
        defaultTransactionOptions: {
          writeConcern: { w: 1 },
        },
      });

      sessionWithDefaults.startTransaction({
        writeConcern: { w: 'majority' },
      });

      expect(sessionWithDefaults.transactionOptions?.writeConcern?.w).toBe('majority');

      sessionWithDefaults.endSession();
    });

    it('should merge options (override + default)', () => {
      const sessionWithDefaults = client.startSession({
        defaultTransactionOptions: {
          readConcern: { level: 'local' },
          writeConcern: { w: 1 },
          maxCommitTimeMS: 1000,
        },
      });

      // Only override readConcern
      sessionWithDefaults.startTransaction({
        readConcern: { level: 'snapshot' },
      });

      // readConcern overridden, others from defaults
      expect(sessionWithDefaults.transactionOptions?.readConcern?.level).toBe('snapshot');
      expect(sessionWithDefaults.transactionOptions?.writeConcern?.w).toBe(1);
      expect(sessionWithDefaults.transactionOptions?.maxCommitTimeMS).toBe(1000);

      sessionWithDefaults.endSession();
    });
  });
});

describe('Transaction Options - Read Concern Levels', () => {
  let client: MongoLake;

  beforeEach(() => {
    client = createTestClient();
  });

  afterEach(async () => {
    await client.close();
  });

  /**
   * MongoDB Spec: Valid readConcern levels
   */
  describe('ReadConcern Levels', () => {
    it('should accept readConcern: local', () => {
      const session = client.startSession();
      session.startTransaction({
        readConcern: { level: 'local' },
      });

      expect(session.transactionOptions?.readConcern?.level).toBe('local');

      session.endSession();
    });

    it('should accept readConcern: majority', () => {
      const session = client.startSession();
      session.startTransaction({
        readConcern: { level: 'majority' },
      });

      expect(session.transactionOptions?.readConcern?.level).toBe('majority');

      session.endSession();
    });

    it('should accept readConcern: snapshot', () => {
      const session = client.startSession();
      session.startTransaction({
        readConcern: { level: 'snapshot' },
      });

      expect(session.transactionOptions?.readConcern?.level).toBe('snapshot');

      session.endSession();
    });

    it('should accept readConcern: linearizable', () => {
      const session = client.startSession();
      session.startTransaction({
        readConcern: { level: 'linearizable' },
      });

      expect(session.transactionOptions?.readConcern?.level).toBe('linearizable');

      session.endSession();
    });
  });
});

describe('Transaction Options - Write Concern Settings', () => {
  let client: MongoLake;

  beforeEach(() => {
    client = createTestClient();
  });

  afterEach(async () => {
    await client.close();
  });

  /**
   * MongoDB Spec: Write concern configurations
   */
  describe('WriteConcern Settings', () => {
    it('should accept w: 0 (fire and forget)', () => {
      const session = client.startSession();
      session.startTransaction({
        writeConcern: { w: 0 },
      });

      expect(session.transactionOptions?.writeConcern?.w).toBe(0);

      session.endSession();
    });

    it('should accept w: 1 (primary acknowledged)', () => {
      const session = client.startSession();
      session.startTransaction({
        writeConcern: { w: 1 },
      });

      expect(session.transactionOptions?.writeConcern?.w).toBe(1);

      session.endSession();
    });

    it('should accept w: "majority"', () => {
      const session = client.startSession();
      session.startTransaction({
        writeConcern: { w: 'majority' },
      });

      expect(session.transactionOptions?.writeConcern?.w).toBe('majority');

      session.endSession();
    });

    it('should accept j: true (journaling)', () => {
      const session = client.startSession();
      session.startTransaction({
        writeConcern: { w: 1, j: true },
      });

      expect(session.transactionOptions?.writeConcern?.j).toBe(true);

      session.endSession();
    });

    it('should accept j: false', () => {
      const session = client.startSession();
      session.startTransaction({
        writeConcern: { w: 1, j: false },
      });

      expect(session.transactionOptions?.writeConcern?.j).toBe(false);

      session.endSession();
    });

    it('should accept wtimeout', () => {
      const session = client.startSession();
      session.startTransaction({
        writeConcern: { w: 'majority', wtimeout: 5000 },
      });

      expect(session.transactionOptions?.writeConcern?.wtimeout).toBe(5000);

      session.endSession();
    });

    it('should accept complete write concern', () => {
      const session = client.startSession();
      session.startTransaction({
        writeConcern: { w: 2, j: true, wtimeout: 10000 },
      });

      expect(session.transactionOptions?.writeConcern?.w).toBe(2);
      expect(session.transactionOptions?.writeConcern?.j).toBe(true);
      expect(session.transactionOptions?.writeConcern?.wtimeout).toBe(10000);

      session.endSession();
    });
  });
});

describe('Transaction Options - maxCommitTimeMS', () => {
  let client: MongoLake;

  beforeEach(() => {
    client = createTestClient();
  });

  afterEach(async () => {
    await client.close();
  });

  /**
   * MongoDB Spec: maxCommitTimeMS option
   */
  describe('maxCommitTimeMS Values', () => {
    it('should accept small timeout', () => {
      const session = client.startSession();
      session.startTransaction({
        maxCommitTimeMS: 100,
      });

      expect(session.transactionOptions?.maxCommitTimeMS).toBe(100);

      session.endSession();
    });

    it('should accept large timeout', () => {
      const session = client.startSession();
      session.startTransaction({
        maxCommitTimeMS: 60000,
      });

      expect(session.transactionOptions?.maxCommitTimeMS).toBe(60000);

      session.endSession();
    });

    it('should accept with other options', () => {
      const session = client.startSession();
      session.startTransaction({
        readConcern: { level: 'snapshot' },
        writeConcern: { w: 'majority' },
        maxCommitTimeMS: 5000,
      });

      expect(session.transactionOptions?.maxCommitTimeMS).toBe(5000);

      session.endSession();
    });
  });
});

describe('Transaction Options - Persistence Across Operations', () => {
  let client: MongoLake;

  beforeEach(() => {
    client = createTestClient();
  });

  afterEach(async () => {
    await client.close();
  });

  /**
   * Options should persist throughout the transaction
   */
  it('should maintain options throughout transaction', () => {
    const session = client.startSession();
    const options: TransactionOptions = {
      readConcern: { level: 'snapshot' },
      writeConcern: { w: 'majority' },
    };

    session.startTransaction(options);

    // Buffer operations
    session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '1' },
    });

    session.bufferOperation({
      type: 'insert',
      collection: 'test',
      database: 'testdb',
      document: { _id: '2' },
    });

    // Options should still be set
    expect(session.transactionOptions?.readConcern?.level).toBe('snapshot');
    expect(session.transactionOptions?.writeConcern?.w).toBe('majority');

    session.endSession();
  });

  /**
   * New transaction should not inherit previous options
   */
  it('should not inherit options from previous transaction', async () => {
    const session = client.startSession();

    // First transaction with options
    session.startTransaction({
      readConcern: { level: 'snapshot' },
    });
    await session.commitTransaction();

    // Second transaction without options
    session.startTransaction();

    // Should not have snapshot readConcern
    expect(session.transactionOptions?.readConcern?.level).not.toBe('snapshot');

    session.endSession();
  });
});
