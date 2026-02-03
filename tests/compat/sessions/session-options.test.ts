/**
 * MongoDB Session Options Tests
 *
 * Based on MongoDB Driver Specifications:
 * - transaction-options.json (session defaults)
 * - implicit-sessions-default-causal-consistency.json
 *
 * Tests session configuration options and their effects.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  ClientSession,
  type SessionOptions,
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

describe('Session Options - Default Transaction Options', () => {
  let client: MongoLake;

  beforeEach(() => {
    client = createTestClient();
  });

  afterEach(async () => {
    await client.close();
  });

  /**
   * MongoDB Spec: transaction-options.json
   *
   * Sessions can specify default transaction options that apply
   * to all transactions started within that session.
   */
  describe('defaultTransactionOptions', () => {
    it('should apply default readConcern to transactions', () => {
      const session = client.startSession({
        defaultTransactionOptions: {
          readConcern: { level: 'majority' },
        },
      });

      session.startTransaction();

      expect(session.transactionOptions?.readConcern?.level).toBe('majority');

      session.endSession();
    });

    it('should apply default writeConcern to transactions', () => {
      const session = client.startSession({
        defaultTransactionOptions: {
          writeConcern: { w: 'majority', j: true },
        },
      });

      session.startTransaction();

      expect(session.transactionOptions?.writeConcern?.w).toBe('majority');
      expect(session.transactionOptions?.writeConcern?.j).toBe(true);

      session.endSession();
    });

    it('should apply default maxCommitTimeMS to transactions', () => {
      const session = client.startSession({
        defaultTransactionOptions: {
          maxCommitTimeMS: 10000,
        },
      });

      session.startTransaction();

      expect(session.transactionOptions?.maxCommitTimeMS).toBe(10000);

      session.endSession();
    });

    it('should apply all default options together', () => {
      const defaults: TransactionOptions = {
        readConcern: { level: 'snapshot' },
        writeConcern: { w: 2, wtimeout: 5000 },
        maxCommitTimeMS: 3000,
      };

      const session = client.startSession({
        defaultTransactionOptions: defaults,
      });

      session.startTransaction();

      expect(session.transactionOptions?.readConcern?.level).toBe('snapshot');
      expect(session.transactionOptions?.writeConcern?.w).toBe(2);
      expect(session.transactionOptions?.writeConcern?.wtimeout).toBe(5000);
      expect(session.transactionOptions?.maxCommitTimeMS).toBe(3000);

      session.endSession();
    });

    it('should allow transaction-level options to override defaults', () => {
      const session = client.startSession({
        defaultTransactionOptions: {
          readConcern: { level: 'local' },
          writeConcern: { w: 1 },
        },
      });

      // Override with transaction-specific options
      session.startTransaction({
        readConcern: { level: 'snapshot' },
        // writeConcern not specified - should use default
      });

      expect(session.transactionOptions?.readConcern?.level).toBe('snapshot');
      expect(session.transactionOptions?.writeConcern?.w).toBe(1);

      session.endSession();
    });

    it('should work with no default options', () => {
      const session = client.startSession();

      session.startTransaction({
        readConcern: { level: 'majority' },
      });

      expect(session.transactionOptions?.readConcern?.level).toBe('majority');

      session.endSession();
    });
  });
});

describe('Session Options - Causal Consistency', () => {
  let client: MongoLake;

  beforeEach(() => {
    client = createTestClient();
  });

  afterEach(async () => {
    await client.close();
  });

  /**
   * MongoDB Spec: implicit-sessions-default-causal-consistency.json
   *
   * Sessions can enable causal consistency for read-your-writes guarantees.
   * Note: MongoLake tracks this option but doesn't enforce it via cluster time.
   */
  describe('causalConsistency Option', () => {
    it('should accept causalConsistency: true', () => {
      const session = new ClientSession({
        causalConsistency: true,
      });

      // Option should be stored (implementation-specific)
      expect(session).toBeInstanceOf(ClientSession);

      session.endSession();
    });

    it('should accept causalConsistency: false', () => {
      const session = new ClientSession({
        causalConsistency: false,
      });

      expect(session).toBeInstanceOf(ClientSession);

      session.endSession();
    });

    it('should work with both causalConsistency and defaultTransactionOptions', () => {
      const session = new ClientSession({
        causalConsistency: true,
        defaultTransactionOptions: {
          readConcern: { level: 'majority' },
        },
      });

      session.startTransaction();

      expect(session.transactionOptions?.readConcern?.level).toBe('majority');

      session.endSession();
    });
  });
});

describe('Session Options - Read Concern Levels', () => {
  /**
   * MongoDB Spec: Various readConcern levels
   *
   * Tests different read concern levels that can be set.
   */
  describe('readConcern Levels', () => {
    it('should support local readConcern', () => {
      const session = new ClientSession({
        defaultTransactionOptions: {
          readConcern: { level: 'local' },
        },
      });

      session.startTransaction();
      expect(session.transactionOptions?.readConcern?.level).toBe('local');

      session.endSession();
    });

    it('should support majority readConcern', () => {
      const session = new ClientSession({
        defaultTransactionOptions: {
          readConcern: { level: 'majority' },
        },
      });

      session.startTransaction();
      expect(session.transactionOptions?.readConcern?.level).toBe('majority');

      session.endSession();
    });

    it('should support snapshot readConcern', () => {
      const session = new ClientSession({
        defaultTransactionOptions: {
          readConcern: { level: 'snapshot' },
        },
      });

      session.startTransaction();
      expect(session.transactionOptions?.readConcern?.level).toBe('snapshot');

      session.endSession();
    });

    it('should support linearizable readConcern', () => {
      const session = new ClientSession({
        defaultTransactionOptions: {
          readConcern: { level: 'linearizable' },
        },
      });

      session.startTransaction();
      expect(session.transactionOptions?.readConcern?.level).toBe('linearizable');

      session.endSession();
    });
  });
});

describe('Session Options - Write Concern Settings', () => {
  /**
   * MongoDB Spec: Various writeConcern settings
   *
   * Tests different write concern configurations.
   */
  describe('writeConcern Settings', () => {
    it('should support numeric w value', () => {
      const session = new ClientSession({
        defaultTransactionOptions: {
          writeConcern: { w: 1 },
        },
      });

      session.startTransaction();
      expect(session.transactionOptions?.writeConcern?.w).toBe(1);

      session.endSession();
    });

    it('should support w: "majority"', () => {
      const session = new ClientSession({
        defaultTransactionOptions: {
          writeConcern: { w: 'majority' },
        },
      });

      session.startTransaction();
      expect(session.transactionOptions?.writeConcern?.w).toBe('majority');

      session.endSession();
    });

    it('should support j: true (journaling)', () => {
      const session = new ClientSession({
        defaultTransactionOptions: {
          writeConcern: { w: 1, j: true },
        },
      });

      session.startTransaction();
      expect(session.transactionOptions?.writeConcern?.j).toBe(true);

      session.endSession();
    });

    it('should support wtimeout', () => {
      const session = new ClientSession({
        defaultTransactionOptions: {
          writeConcern: { w: 'majority', wtimeout: 5000 },
        },
      });

      session.startTransaction();
      expect(session.transactionOptions?.writeConcern?.wtimeout).toBe(5000);

      session.endSession();
    });

    it('should support complete writeConcern configuration', () => {
      const session = new ClientSession({
        defaultTransactionOptions: {
          writeConcern: { w: 2, j: true, wtimeout: 10000 },
        },
      });

      session.startTransaction();
      const wc = session.transactionOptions?.writeConcern;
      expect(wc?.w).toBe(2);
      expect(wc?.j).toBe(true);
      expect(wc?.wtimeout).toBe(10000);

      session.endSession();
    });
  });
});
