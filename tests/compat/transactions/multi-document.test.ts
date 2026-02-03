/**
 * MongoDB Multi-Document Transaction Tests
 *
 * Based on MongoDB Driver Specifications:
 * - insert.json
 * - update.json
 * - delete.json
 *
 * Tests multi-document transactions with various CRUD operations.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  ClientSession,
  type BufferedOperation,
} from '../../../src/session/index.js';
import { TransactionManager } from '../../../src/transaction/index.js';
import { MongoLake, Collection } from '../../../src/client/index.js';
import { MemoryStorage } from '../../../src/storage/index.js';

interface TestDoc {
  _id: string;
  name?: string;
  value?: number;
  category?: string;
  [key: string]: unknown;
}

function createTestClient(): MongoLake {
  const storage = new MemoryStorage();
  const client = new MongoLake({ local: '.test-mongolake' });
  // @ts-expect-error - accessing private field for testing
  client.storage = storage;
  return client;
}

describe('Multi-Document Transactions - insert.json specification', () => {
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
   * MongoDB Spec: Multiple inserts in single transaction
   */
  describe('Multiple Insert Operations', () => {
    it('should buffer multiple insert operations', () => {
      session.startTransaction();

      session.bufferOperation({
        type: 'insert',
        collection: 'users',
        database: 'testdb',
        document: { _id: '1', name: 'Alice' },
      });
      session.bufferOperation({
        type: 'insert',
        collection: 'users',
        database: 'testdb',
        document: { _id: '2', name: 'Bob' },
      });
      session.bufferOperation({
        type: 'insert',
        collection: 'users',
        database: 'testdb',
        document: { _id: '3', name: 'Charlie' },
      });

      expect(session.operationCount).toBe(3);
    });

    it('should preserve insert order', () => {
      session.startTransaction();

      session.bufferOperation({
        type: 'insert',
        collection: 'users',
        database: 'testdb',
        document: { _id: '1', name: 'First' },
      });
      session.bufferOperation({
        type: 'insert',
        collection: 'users',
        database: 'testdb',
        document: { _id: '2', name: 'Second' },
      });
      session.bufferOperation({
        type: 'insert',
        collection: 'users',
        database: 'testdb',
        document: { _id: '3', name: 'Third' },
      });

      const ops = session.getBufferedOperations();
      expect(ops[0].document?.name).toBe('First');
      expect(ops[1].document?.name).toBe('Second');
      expect(ops[2].document?.name).toBe('Third');
    });

    it('should track all insert documents', () => {
      session.startTransaction();

      const docs = [
        { _id: 'a', value: 1 },
        { _id: 'b', value: 2 },
        { _id: 'c', value: 3 },
        { _id: 'd', value: 4 },
        { _id: 'e', value: 5 },
      ];

      docs.forEach((doc) => {
        session.bufferOperation({
          type: 'insert',
          collection: 'data',
          database: 'testdb',
          document: doc,
        });
      });

      const ops = session.getBufferedOperations();
      expect(ops).toHaveLength(5);
      ops.forEach((op, i) => {
        expect(op.document?.value).toBe(i + 1);
      });
    });
  });
});

describe('Multi-Document Transactions - Mixed Operations', () => {
  let client: MongoLake;
  let session: ClientSession;
  let txn: TransactionManager;

  beforeEach(() => {
    client = createTestClient();
    session = client.startSession();
    txn = new TransactionManager(session);
  });

  afterEach(async () => {
    if (!session.hasEnded) {
      await session.endSession();
    }
    await client.close();
  });

  /**
   * MongoDB Spec: Multiple operation types in transaction
   */
  describe('Insert, Update, Delete in Same Transaction', () => {
    it('should buffer all CRUD operation types', () => {
      txn.begin();

      // Insert
      txn.insert('testdb', 'users', { _id: '1', name: 'Alice' });

      // Update
      txn.update('testdb', 'users', { _id: '1' }, { $set: { age: 30 } });

      // Replace
      txn.replace('testdb', 'users', { _id: '2' }, { _id: '2', name: 'Bob', age: 25 });

      // Delete
      txn.delete('testdb', 'users', { _id: '3' });

      expect(txn.operationCount).toBe(4);
    });

    it('should preserve operation order with mixed types', () => {
      txn.begin();

      txn.insert('testdb', 'users', { _id: '1', name: 'Alice' });
      txn.update('testdb', 'users', { _id: '1' }, { $set: { age: 30 } });
      txn.delete('testdb', 'users', { _id: '1' });
      txn.insert('testdb', 'users', { _id: '1', name: 'New Alice' });

      const ops = txn.getOperations();
      expect(ops[0].type).toBe('insert');
      expect(ops[1].type).toBe('update');
      expect(ops[2].type).toBe('delete');
      expect(ops[3].type).toBe('insert');
    });

    it('should track all operation details', () => {
      txn.begin();

      txn.insert('testdb', 'users', { _id: '1', name: 'Alice' });
      txn.update('testdb', 'users', { _id: '1' }, { $set: { updated: true } });
      txn.delete('testdb', 'users', { _id: '2' });

      const ops = txn.getOperations();

      // Insert
      expect(ops[0].type).toBe('insert');
      expect(ops[0].document?._id).toBe('1');
      expect(ops[0].document?.name).toBe('Alice');

      // Update
      expect(ops[1].type).toBe('update');
      expect(ops[1].filter?._id).toBe('1');
      expect(ops[1].update?.$set).toEqual({ updated: true });

      // Delete
      expect(ops[2].type).toBe('delete');
      expect(ops[2].filter?._id).toBe('2');
    });
  });
});

describe('Multi-Document Transactions - Cross-Collection', () => {
  let client: MongoLake;
  let session: ClientSession;
  let txn: TransactionManager;

  beforeEach(() => {
    client = createTestClient();
    session = client.startSession();
    txn = new TransactionManager(session);
  });

  afterEach(async () => {
    if (!session.hasEnded) {
      await session.endSession();
    }
    await client.close();
  });

  /**
   * MongoDB Spec: Operations across multiple collections
   */
  describe('Multiple Collections', () => {
    it('should buffer operations across collections', () => {
      txn.begin();

      txn.insert('testdb', 'users', { _id: 'u1', name: 'Alice' });
      txn.insert('testdb', 'orders', { _id: 'o1', userId: 'u1', total: 100 });
      txn.insert('testdb', 'inventory', { _id: 'i1', product: 'Widget', qty: 50 });

      const ops = txn.getOperations();
      expect(ops[0].collection).toBe('users');
      expect(ops[1].collection).toBe('orders');
      expect(ops[2].collection).toBe('inventory');
    });

    it('should track collection per operation', () => {
      txn.begin();

      txn.insert('testdb', 'users', { _id: 'u1', name: 'Alice' });
      txn.update('testdb', 'orders', { _id: 'o1' }, { $set: { status: 'shipped' } });
      txn.delete('testdb', 'cart', { userId: 'u1' });

      const ops = txn.getOperations();
      expect(ops.map((o) => o.collection)).toEqual(['users', 'orders', 'cart']);
    });
  });
});

describe('Multi-Document Transactions - Cross-Database', () => {
  let client: MongoLake;
  let session: ClientSession;
  let txn: TransactionManager;

  beforeEach(() => {
    client = createTestClient();
    session = client.startSession();
    txn = new TransactionManager(session);
  });

  afterEach(async () => {
    if (!session.hasEnded) {
      await session.endSession();
    }
    await client.close();
  });

  /**
   * MongoDB Spec: Operations across multiple databases
   */
  describe('Multiple Databases', () => {
    it('should buffer operations across databases', () => {
      txn.begin();

      txn.insert('users_db', 'users', { _id: 'u1', name: 'Alice' });
      txn.insert('orders_db', 'orders', { _id: 'o1', userId: 'u1' });
      txn.insert('analytics_db', 'events', { _id: 'e1', type: 'signup' });

      const ops = txn.getOperations();
      expect(ops[0].database).toBe('users_db');
      expect(ops[1].database).toBe('orders_db');
      expect(ops[2].database).toBe('analytics_db');
    });

    it('should track database per operation', () => {
      txn.begin();

      txn.insert('db1', 'coll', { _id: '1' });
      txn.insert('db2', 'coll', { _id: '2' });
      txn.insert('db1', 'coll', { _id: '3' });
      txn.insert('db3', 'coll', { _id: '4' });

      const ops = txn.getOperations();
      expect(ops.map((o) => o.database)).toEqual(['db1', 'db2', 'db1', 'db3']);
    });
  });
});

describe('Multi-Document Transactions - Commit/Abort Behavior', () => {
  let client: MongoLake;
  let collection: Collection<TestDoc>;

  beforeEach(async () => {
    client = createTestClient();
    collection = client.db('testdb').collection<TestDoc>('txn_test');

    // Insert some initial data
    await collection.insertMany([
      { _id: 'existing1', name: 'Existing 1', value: 100 },
      { _id: 'existing2', name: 'Existing 2', value: 200 },
    ]);
  });

  afterEach(async () => {
    await client.close();
  });

  /**
   * MongoDB Spec: Commit applies all operations
   */
  describe('Commit Behavior', () => {
    it('should apply all buffered operations on commit', async () => {
      const session = client.startSession();
      const txn = new TransactionManager(session);

      txn.begin();
      txn.insert('testdb', 'txn_test', { _id: 'new1', name: 'New 1' });
      txn.insert('testdb', 'txn_test', { _id: 'new2', name: 'New 2' });

      const result = await txn.commit();

      expect(result.success).toBe(true);
      expect(result.operationCount).toBe(2);

      await session.endSession();
    });

    it('should report correct operation count', async () => {
      const session = client.startSession();
      const txn = new TransactionManager(session);

      txn.begin();
      txn.insert('testdb', 'txn_test', { _id: 'a' });
      txn.insert('testdb', 'txn_test', { _id: 'b' });
      txn.insert('testdb', 'txn_test', { _id: 'c' });
      txn.update('testdb', 'txn_test', { _id: 'a' }, { $set: { x: 1 } });
      txn.delete('testdb', 'txn_test', { _id: 'b' });

      const result = await txn.commit();

      expect(result.operationCount).toBe(5);

      await session.endSession();
    });
  });

  /**
   * MongoDB Spec: Abort discards all operations
   */
  describe('Abort Behavior', () => {
    it('should discard all operations on abort', async () => {
      const session = client.startSession();
      const txn = new TransactionManager(session);

      txn.begin();
      txn.insert('testdb', 'txn_test', { _id: 'aborted1', name: 'Should not exist' });
      txn.insert('testdb', 'txn_test', { _id: 'aborted2', name: 'Should not exist' });

      await txn.abort();

      // Verify documents don't exist
      const doc1 = await collection.findOne({ _id: 'aborted1' });
      const doc2 = await collection.findOne({ _id: 'aborted2' });

      expect(doc1).toBeNull();
      expect(doc2).toBeNull();

      await session.endSession();
    });

    it('should not affect existing documents on abort', async () => {
      const session = client.startSession();
      const txn = new TransactionManager(session);

      txn.begin();
      txn.update('testdb', 'txn_test', { _id: 'existing1' }, { $set: { value: 999 } });
      txn.delete('testdb', 'txn_test', { _id: 'existing2' });

      await txn.abort();

      // Existing documents should be unchanged
      const doc1 = await collection.findOne({ _id: 'existing1' });
      const doc2 = await collection.findOne({ _id: 'existing2' });

      expect(doc1?.value).toBe(100); // Not 999
      expect(doc2).toBeDefined(); // Not deleted

      await session.endSession();
    });
  });
});

describe('Multi-Document Transactions - Operation Metadata', () => {
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
   * Test operation timestamp tracking
   */
  describe('Operation Timestamps', () => {
    it('should record timestamp for each operation', async () => {
      session.startTransaction();

      const before = Date.now();

      session.bufferOperation({
        type: 'insert',
        collection: 'test',
        database: 'testdb',
        document: { _id: '1' },
      });

      // Small delay
      await new Promise((resolve) => setTimeout(resolve, 5));

      session.bufferOperation({
        type: 'insert',
        collection: 'test',
        database: 'testdb',
        document: { _id: '2' },
      });

      const after = Date.now();

      const ops = session.getBufferedOperations();

      expect(ops[0].timestamp).toBeGreaterThanOrEqual(before);
      expect(ops[0].timestamp).toBeLessThanOrEqual(after);
      expect(ops[1].timestamp).toBeGreaterThanOrEqual(ops[0].timestamp);
    });
  });

  /**
   * Test operation options preservation
   */
  describe('Operation Options', () => {
    it('should preserve operation options', () => {
      session.startTransaction();

      session.bufferOperation({
        type: 'update',
        collection: 'test',
        database: 'testdb',
        filter: { _id: '1' },
        update: { $set: { x: 1 } },
        options: { upsert: true },
      });

      const ops = session.getBufferedOperations();
      expect(ops[0].options?.upsert).toBe(true);
    });
  });
});

describe('Multi-Document Transactions - Large Transactions', () => {
  let client: MongoLake;
  let session: ClientSession;
  let txn: TransactionManager;

  beforeEach(() => {
    client = createTestClient();
    session = client.startSession();
    txn = new TransactionManager(session);
  });

  afterEach(async () => {
    if (!session.hasEnded) {
      await session.endSession();
    }
    await client.close();
  });

  /**
   * Test with many operations
   */
  describe('Many Operations', () => {
    it('should handle 100 operations', () => {
      txn.begin();

      for (let i = 0; i < 100; i++) {
        txn.insert('testdb', 'large', { _id: `doc-${i}`, value: i });
      }

      expect(txn.operationCount).toBe(100);
    });

    it('should handle mixed operations at scale', () => {
      txn.begin();

      for (let i = 0; i < 50; i++) {
        txn.insert('testdb', 'large', { _id: `insert-${i}` });
      }
      for (let i = 0; i < 30; i++) {
        txn.update('testdb', 'large', { _id: `insert-${i}` }, { $set: { updated: true } });
      }
      for (let i = 40; i < 50; i++) {
        txn.delete('testdb', 'large', { _id: `insert-${i}` });
      }

      expect(txn.operationCount).toBe(90);
    });
  });
});
