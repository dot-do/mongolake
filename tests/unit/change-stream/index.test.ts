/**
 * Tests for MongoLake Change Streams
 *
 * Comprehensive unit tests for:
 * - ChangeStream class
 * - watch() method on collection
 * - Event types (insert, update, delete, replace)
 * - fullDocument option
 * - resumeAfter and startAtOperationTime options
 * - Filter with match pipeline stage
 * - Async iteration
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { Database, Collection, ChangeStream } from '../../../src/client/index.js';
import { MemoryStorage } from '../../../src/storage/index.js';
import type { ChangeStreamDocument, ChangeStreamOptions } from '../../../src/client/index.js';

// =============================================================================
// Test Helpers
// =============================================================================

interface TestDocument {
  _id?: string;
  name?: string;
  status?: string;
  count?: number;
  tags?: string[];
  [key: string]: unknown;
}

/**
 * Create a Collection with mocked storage for testing
 */
function createTestCollection(
  collectionName = 'users',
  dbName = 'testdb'
): { collection: Collection<TestDocument>; storage: MemoryStorage; db: Database } {
  const storage = new MemoryStorage();
  const config = { database: dbName };
  const database = new Database(dbName, storage, config);
  const collection = database.collection<TestDocument>(collectionName);
  return { collection, storage, db: database };
}

/**
 * Wait for a short time to allow async operations to complete
 */
function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// =============================================================================
// ChangeStream Basic Tests
// =============================================================================

describe('ChangeStream', () => {
  describe('watch()', () => {
    it('should create a ChangeStream instance', () => {
      const { collection } = createTestCollection();
      const changeStream = collection.watch();

      expect(changeStream).toBeInstanceOf(ChangeStream);
      expect(changeStream.isClosed).toBe(false);
    });

    it('should create a ChangeStream with pipeline', () => {
      const { collection } = createTestCollection();
      const changeStream = collection.watch([{ $match: { operationType: 'insert' } }]);

      expect(changeStream).toBeInstanceOf(ChangeStream);
    });

    it('should create a ChangeStream with options', () => {
      const { collection } = createTestCollection();
      const changeStream = collection.watch([], {
        fullDocument: 'updateLookup',
      });

      expect(changeStream).toBeInstanceOf(ChangeStream);
    });
  });

  describe('close()', () => {
    it('should close the change stream', () => {
      const { collection } = createTestCollection();
      const changeStream = collection.watch();

      expect(changeStream.isClosed).toBe(false);
      changeStream.close();
      expect(changeStream.isClosed).toBe(true);
    });

    it('should return null from next() after close', async () => {
      const { collection } = createTestCollection();
      const changeStream = collection.watch();

      changeStream.close();
      const event = await changeStream.next();

      expect(event).toBeNull();
    });
  });

  describe('resumeToken', () => {
    it('should be null initially', () => {
      const { collection } = createTestCollection();
      const changeStream = collection.watch();

      expect(changeStream.resumeToken).toBeNull();
    });

    it('should be updated after receiving an event', async () => {
      const { collection } = createTestCollection();
      const changeStream = collection.watch();

      // Insert a document to trigger an event
      await collection.insertOne({ _id: 'test-1', name: 'Test' });

      // Get the event
      const event = changeStream.tryNext();

      expect(event).not.toBeUndefined();
      expect(changeStream.resumeToken).not.toBeNull();
      expect(changeStream.resumeToken?._data).toBeDefined();
      expect(changeStream.resumeToken?.clusterTime).toBeInstanceOf(Date);
    });
  });
});

// =============================================================================
// Event Type Tests
// =============================================================================

describe('Change Events', () => {
  describe('insert events', () => {
    it('should emit insert event on insertOne', async () => {
      const { collection } = createTestCollection();
      const changeStream = collection.watch();

      await collection.insertOne({ _id: 'insert-1', name: 'Alice' });

      const event = changeStream.tryNext();

      expect(event).toBeDefined();
      expect(event?.operationType).toBe('insert');
      expect(event?.documentKey._id).toBe('insert-1');
      expect(event?.fullDocument?.name).toBe('Alice');
      expect(event?.ns.coll).toBe('users');
    });

    it('should emit insert events on insertMany', async () => {
      const { collection } = createTestCollection();
      const changeStream = collection.watch();

      await collection.insertMany([
        { _id: 'insert-a', name: 'A' },
        { _id: 'insert-b', name: 'B' },
      ]);

      const event1 = changeStream.tryNext();
      const event2 = changeStream.tryNext();

      expect(event1?.operationType).toBe('insert');
      expect(event1?.documentKey._id).toBe('insert-a');

      expect(event2?.operationType).toBe('insert');
      expect(event2?.documentKey._id).toBe('insert-b');
    });
  });

  describe('update events', () => {
    it('should emit update event on updateOne', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: 'update-1', name: 'Alice', count: 0 });

      const changeStream = collection.watch();
      await collection.updateOne({ _id: 'update-1' }, { $set: { count: 5 } });

      const event = changeStream.tryNext();

      expect(event).toBeDefined();
      expect(event?.operationType).toBe('update');
      expect(event?.documentKey._id).toBe('update-1');
      expect(event?.updateDescription?.updatedFields).toHaveProperty('count', 5);
    });

    it('should emit update events on updateMany', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: 'um-1', status: 'pending' },
        { _id: 'um-2', status: 'pending' },
      ]);

      const changeStream = collection.watch();
      await collection.updateMany({ status: 'pending' }, { $set: { status: 'active' } });

      const event1 = changeStream.tryNext();
      const event2 = changeStream.tryNext();

      expect(event1?.operationType).toBe('update');
      expect(event2?.operationType).toBe('update');
    });

    it('should include updateDescription with updatedFields', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: 'ud-1', name: 'Alice', count: 0 });

      const changeStream = collection.watch();
      await collection.updateOne({ _id: 'ud-1' }, { $set: { name: 'Alicia', count: 10 } });

      const event = changeStream.tryNext();

      expect(event?.updateDescription?.updatedFields).toEqual({
        name: 'Alicia',
        count: 10,
      });
    });

    it('should include updateDescription with removedFields', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: 'rf-1', name: 'Alice', temp: 'value' });

      const changeStream = collection.watch();
      await collection.updateOne({ _id: 'rf-1' }, { $unset: { temp: '' } });

      const event = changeStream.tryNext();

      expect(event?.updateDescription?.removedFields).toContain('temp');
    });
  });

  describe('replace events', () => {
    it('should emit replace event on replaceOne', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: 'replace-1', name: 'Alice', age: 30 });

      const changeStream = collection.watch();
      await collection.replaceOne({ _id: 'replace-1' }, { name: 'Bob', age: 25 });

      const event = changeStream.tryNext();

      expect(event).toBeDefined();
      expect(event?.operationType).toBe('replace');
      expect(event?.documentKey._id).toBe('replace-1');
      expect(event?.fullDocument?.name).toBe('Bob');
      expect(event?.fullDocument?.age).toBe(25);
    });
  });

  describe('delete events', () => {
    it('should emit delete event on deleteOne', async () => {
      const { collection } = createTestCollection();
      await collection.insertOne({ _id: 'delete-1', name: 'ToDelete' });

      const changeStream = collection.watch();
      await collection.deleteOne({ _id: 'delete-1' });

      const event = changeStream.tryNext();

      expect(event).toBeDefined();
      expect(event?.operationType).toBe('delete');
      expect(event?.documentKey._id).toBe('delete-1');
      expect(event?.fullDocument).toBeUndefined();
    });

    it('should emit delete events on deleteMany', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: 'dm-1', category: 'toDelete' },
        { _id: 'dm-2', category: 'toDelete' },
      ]);

      const changeStream = collection.watch();
      await collection.deleteMany({ category: 'toDelete' });

      const event1 = changeStream.tryNext();
      const event2 = changeStream.tryNext();

      expect(event1?.operationType).toBe('delete');
      expect(event2?.operationType).toBe('delete');
    });
  });
});

// =============================================================================
// fullDocument Option Tests
// =============================================================================

describe('fullDocument option', () => {
  it('should include fullDocument for insert events by default', async () => {
    const { collection } = createTestCollection();
    const changeStream = collection.watch();

    await collection.insertOne({ _id: 'fd-insert', name: 'Alice' });

    const event = changeStream.tryNext();

    expect(event?.fullDocument).toBeDefined();
    expect(event?.fullDocument?.name).toBe('Alice');
  });

  it('should include fullDocument for replace events by default', async () => {
    const { collection } = createTestCollection();
    await collection.insertOne({ _id: 'fd-replace', name: 'Alice' });

    const changeStream = collection.watch();
    await collection.replaceOne({ _id: 'fd-replace' }, { name: 'Bob' });

    const event = changeStream.tryNext();

    expect(event?.fullDocument).toBeDefined();
    expect(event?.fullDocument?.name).toBe('Bob');
  });

  it('should NOT include fullDocument for update events by default', async () => {
    const { collection } = createTestCollection();
    await collection.insertOne({ _id: 'fd-update-def', name: 'Alice' });

    const changeStream = collection.watch([], { fullDocument: 'default' });
    await collection.updateOne({ _id: 'fd-update-def' }, { $set: { name: 'Alicia' } });

    const event = changeStream.tryNext();

    expect(event?.fullDocument).toBeUndefined();
  });

  it('should include fullDocument for update events with updateLookup', async () => {
    const { collection } = createTestCollection();
    await collection.insertOne({ _id: 'fd-update-lookup', name: 'Alice' });

    const changeStream = collection.watch([], { fullDocument: 'updateLookup' });
    await collection.updateOne({ _id: 'fd-update-lookup' }, { $set: { name: 'Alicia' } });

    const event = changeStream.tryNext();

    expect(event?.fullDocument).toBeDefined();
    expect(event?.fullDocument?.name).toBe('Alicia');
  });

  it('should include fullDocument for update events with whenAvailable', async () => {
    const { collection } = createTestCollection();
    await collection.insertOne({ _id: 'fd-update-when', name: 'Alice' });

    const changeStream = collection.watch([], { fullDocument: 'whenAvailable' });
    await collection.updateOne({ _id: 'fd-update-when' }, { $set: { name: 'Alicia' } });

    const event = changeStream.tryNext();

    expect(event?.fullDocument).toBeDefined();
  });
});

// =============================================================================
// Pipeline Filter Tests
// =============================================================================

describe('Pipeline filtering', () => {
  it('should filter by operationType', async () => {
    const { collection } = createTestCollection();
    const changeStream = collection.watch([{ $match: { operationType: 'insert' } }]);

    // Insert should be captured
    await collection.insertOne({ _id: 'filter-1', name: 'Alice' });

    // Update should be filtered out
    await collection.updateOne({ _id: 'filter-1' }, { $set: { name: 'Alicia' } });

    const event1 = changeStream.tryNext();
    const event2 = changeStream.tryNext();

    expect(event1?.operationType).toBe('insert');
    expect(event2).toBeUndefined();
  });

  it('should filter by fullDocument fields', async () => {
    const { collection } = createTestCollection();
    const changeStream = collection.watch([{ $match: { 'fullDocument.status': 'active' } }]);

    // This should be captured (status: active)
    await collection.insertOne({ _id: 'status-1', status: 'active' });

    // This should be filtered out (status: inactive)
    await collection.insertOne({ _id: 'status-2', status: 'inactive' });

    const event1 = changeStream.tryNext();
    const event2 = changeStream.tryNext();

    expect(event1?.documentKey._id).toBe('status-1');
    expect(event2).toBeUndefined();
  });

  it('should filter with $or operator', async () => {
    const { collection } = createTestCollection();
    const changeStream = collection.watch([
      {
        $match: {
          $or: [{ operationType: 'insert' }, { operationType: 'delete' }],
        },
      },
    ]);

    await collection.insertOne({ _id: 'or-1', name: 'Test' });
    await collection.deleteOne({ _id: 'or-1' });
    // Updates should be filtered out
    await collection.insertOne({ _id: 'or-2', name: 'Another' });
    await collection.updateOne({ _id: 'or-2' }, { $set: { name: 'Updated' } });

    const event1 = changeStream.tryNext();
    const event2 = changeStream.tryNext();
    const event3 = changeStream.tryNext();
    const event4 = changeStream.tryNext();

    expect(event1?.operationType).toBe('insert');
    expect(event2?.operationType).toBe('delete');
    expect(event3?.operationType).toBe('insert');
    expect(event4).toBeUndefined(); // Update was filtered
  });
});

// =============================================================================
// Resume Tests
// =============================================================================

describe('Resume options', () => {
  it('should create change stream with resumeAfter', () => {
    const { collection } = createTestCollection();
    const resumeToken = {
      _data: '5',
      clusterTime: new Date(),
    };

    const changeStream = collection.watch([], { resumeAfter: resumeToken });

    expect(changeStream).toBeInstanceOf(ChangeStream);
    expect(changeStream.isClosed).toBe(false);
  });

  it('should create change stream with startAtOperationTime', () => {
    const { collection } = createTestCollection();
    const startTime = new Date(Date.now() - 60000); // 1 minute ago

    const changeStream = collection.watch([], { startAtOperationTime: startTime });

    expect(changeStream).toBeInstanceOf(ChangeStream);
    expect(changeStream.isClosed).toBe(false);
  });
});

// =============================================================================
// Async Iteration Tests
// =============================================================================

describe('Async iteration', () => {
  it('should support for-await-of loop', async () => {
    const { collection } = createTestCollection();
    const changeStream = collection.watch();

    // Insert documents
    await collection.insertMany([
      { _id: 'async-1', name: 'A' },
      { _id: 'async-2', name: 'B' },
    ]);

    const events: ChangeStreamDocument<TestDocument>[] = [];

    // Collect events with a timeout
    const iterateWithTimeout = async () => {
      const timeout = setTimeout(() => changeStream.close(), 100);

      for await (const event of changeStream) {
        events.push(event);
        if (events.length >= 2) {
          clearTimeout(timeout);
          changeStream.close();
          break;
        }
      }
    };

    await iterateWithTimeout();

    expect(events).toHaveLength(2);
    expect(events[0].operationType).toBe('insert');
    expect(events[1].operationType).toBe('insert');
  });

  it('should exit loop when close() is called', async () => {
    const { collection } = createTestCollection();
    const changeStream = collection.watch();

    let eventCount = 0;

    // Start iteration in background
    const iterationPromise = (async () => {
      for await (const _event of changeStream) {
        eventCount++;
        if (eventCount >= 1) {
          changeStream.close();
        }
      }
    })();

    // Insert a document
    await collection.insertOne({ _id: 'close-test', name: 'Test' });

    // Wait for iteration to complete
    await iterationPromise;

    expect(eventCount).toBe(1);
    expect(changeStream.isClosed).toBe(true);
  });

  it('should support toArray() with limit', async () => {
    const { collection } = createTestCollection();
    const changeStream = collection.watch();

    // Insert documents
    await collection.insertMany([
      { _id: 'array-1', name: 'A' },
      { _id: 'array-2', name: 'B' },
      { _id: 'array-3', name: 'C' },
    ]);

    // Get events as array with limit
    const events = await changeStream.toArray(2);

    expect(events).toHaveLength(2);

    changeStream.close();
  });
});

// =============================================================================
// hasNext and tryNext Tests
// =============================================================================

describe('hasNext and tryNext', () => {
  it('should return false for hasNext when buffer is empty', () => {
    const { collection } = createTestCollection();
    const changeStream = collection.watch();

    expect(changeStream.hasNext()).toBe(false);
  });

  it('should return true for hasNext when events are buffered', async () => {
    const { collection } = createTestCollection();
    const changeStream = collection.watch();

    await collection.insertOne({ _id: 'has-next', name: 'Test' });

    expect(changeStream.hasNext()).toBe(true);
  });

  it('should return undefined for tryNext when buffer is empty', () => {
    const { collection } = createTestCollection();
    const changeStream = collection.watch();

    const event = changeStream.tryNext();

    expect(event).toBeUndefined();
  });

  it('should return event for tryNext when events are buffered', async () => {
    const { collection } = createTestCollection();
    const changeStream = collection.watch();

    await collection.insertOne({ _id: 'try-next', name: 'Test' });

    const event = changeStream.tryNext();

    expect(event).toBeDefined();
    expect(event?.operationType).toBe('insert');
  });
});

// =============================================================================
// Event Structure Tests
// =============================================================================

describe('Event structure', () => {
  it('should include all required fields', async () => {
    const { collection, db } = createTestCollection();
    const changeStream = collection.watch();

    await collection.insertOne({ _id: 'struct-1', name: 'Test' });

    const event = changeStream.tryNext();

    expect(event).toBeDefined();
    expect(event?._id).toBeDefined();
    expect(event?._id._data).toBeDefined();
    expect(event?._id.clusterTime).toBeInstanceOf(Date);
    expect(event?.operationType).toBe('insert');
    expect(event?.ns).toEqual({ db: db.name, coll: 'users' });
    expect(event?.documentKey).toEqual({ _id: 'struct-1' });
    expect(event?.clusterTime).toBeInstanceOf(Date);
    expect(event?.wallTime).toBeInstanceOf(Date);
  });

  it('should have incrementing resume tokens', async () => {
    const { collection } = createTestCollection();
    const changeStream = collection.watch();

    await collection.insertOne({ _id: 'seq-1', name: 'A' });
    await collection.insertOne({ _id: 'seq-2', name: 'B' });

    const event1 = changeStream.tryNext();
    const event2 = changeStream.tryNext();

    const token1 = parseInt(event1?._id._data || '0', 10);
    const token2 = parseInt(event2?._id._data || '0', 10);

    expect(token2).toBeGreaterThan(token1);
  });
});

// =============================================================================
// Multiple Change Streams Tests
// =============================================================================

describe('Multiple change streams', () => {
  it('should support multiple change streams on same collection', async () => {
    const { collection } = createTestCollection();
    const stream1 = collection.watch();
    const stream2 = collection.watch();

    await collection.insertOne({ _id: 'multi-1', name: 'Test' });

    const event1 = stream1.tryNext();
    const event2 = stream2.tryNext();

    expect(event1?.documentKey._id).toBe('multi-1');
    expect(event2?.documentKey._id).toBe('multi-1');

    stream1.close();
    stream2.close();
  });

  it('should only deliver events to streams that match filter', async () => {
    const { collection } = createTestCollection();
    const insertStream = collection.watch([{ $match: { operationType: 'insert' } }]);
    const deleteStream = collection.watch([{ $match: { operationType: 'delete' } }]);

    await collection.insertOne({ _id: 'filter-multi-1', name: 'Test' });
    await collection.deleteOne({ _id: 'filter-multi-1' });

    // insertStream should only have the insert event (delete was filtered out)
    const insertEvent = insertStream.tryNext();
    const secondEventFromInsertStream = insertStream.tryNext();

    // deleteStream should only have the delete event (insert was filtered out)
    const deleteEvent = deleteStream.tryNext();
    const secondEventFromDeleteStream = deleteStream.tryNext();

    expect(insertEvent?.operationType).toBe('insert');
    expect(secondEventFromInsertStream).toBeUndefined(); // delete was filtered out
    expect(deleteEvent?.operationType).toBe('delete');
    expect(secondEventFromDeleteStream).toBeUndefined(); // no more events

    insertStream.close();
    deleteStream.close();
  });

  it('should clean up closed streams automatically', async () => {
    const { collection } = createTestCollection();
    const stream1 = collection.watch();
    const stream2 = collection.watch();

    // Close stream1
    stream1.close();

    // Insert should still work and not fail
    await collection.insertOne({ _id: 'cleanup-1', name: 'Test' });

    // stream2 should still receive events
    const event = stream2.tryNext();
    expect(event?.documentKey._id).toBe('cleanup-1');

    stream2.close();
  });
});

// =============================================================================
// Edge Cases
// =============================================================================

describe('Edge cases', () => {
  it('should handle watch() before any documents exist', async () => {
    const { collection } = createTestCollection();
    const changeStream = collection.watch();

    // Watch on empty collection
    expect(changeStream.hasNext()).toBe(false);

    // Insert first document
    await collection.insertOne({ _id: 'first', name: 'First' });

    expect(changeStream.hasNext()).toBe(true);
    const event = changeStream.tryNext();
    expect(event?.operationType).toBe('insert');

    changeStream.close();
  });

  it('should not emit events after close()', async () => {
    const { collection } = createTestCollection();
    const changeStream = collection.watch();

    changeStream.close();

    // Insert after close
    await collection.insertOne({ _id: 'after-close', name: 'Test' });

    expect(changeStream.hasNext()).toBe(false);
    expect(changeStream.tryNext()).toBeUndefined();
  });

  it('should handle rapid successive operations', async () => {
    const { collection } = createTestCollection();
    const changeStream = collection.watch();

    // Rapid inserts
    for (let i = 0; i < 10; i++) {
      await collection.insertOne({ _id: `rapid-${i}`, name: `Doc ${i}` });
    }

    // Should have all 10 events
    let count = 0;
    while (changeStream.hasNext()) {
      changeStream.tryNext();
      count++;
    }

    expect(count).toBe(10);

    changeStream.close();
  });

  it('should handle special characters in document fields', async () => {
    const { collection } = createTestCollection();
    const changeStream = collection.watch();

    await collection.insertOne({
      _id: 'special-chars',
      name: "Test with 'quotes' and \"double\"",
      data: 'Unicode: Hello',
    });

    const event = changeStream.tryNext();
    expect(event?.fullDocument?.name).toBe("Test with 'quotes' and \"double\"");
    expect(event?.fullDocument?.data).toBe('Unicode: Hello');

    changeStream.close();
  });
});
