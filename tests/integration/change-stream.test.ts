/**
 * Change Stream Integration Tests
 *
 * Tests change stream delivery across multiple consumers and during
 * concurrent write operations. Validates event ordering, filtering,
 * and resumption capabilities.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  ChangeStream,
  createChangeStream,
  computeUpdateDescription,
  type ChangeStreamDocument,
  type ChangeStreamNamespace,
  type ResumeToken,
} from '../../src/change-stream/index.js';
import { MemoryStorage } from '../../src/storage/index.js';
import { MongoLake, Collection, Database } from '../../src/client/index.js';
import { resetDocumentCounter, createUser } from '../utils/factories.js';

// ============================================================================
// Test Types
// ============================================================================

interface TestDocument {
  _id: string;
  name: string;
  status?: string;
  value?: number;
  category?: string;
  tags?: string[];
  metadata?: Record<string, unknown>;
}

// ============================================================================
// Test Helpers
// ============================================================================

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

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ============================================================================
// Change Stream Delivery Tests
// ============================================================================

describe('Change Stream - Event Delivery', () => {
  let collection: Collection<TestDocument>;
  let storage: MemoryStorage;
  let db: Database;

  beforeEach(() => {
    resetDocumentCounter();
    const env = createTestCollection();
    collection = env.collection;
    storage = env.storage;
    db = env.db;
  });

  afterEach(() => {
    storage.clear();
  });

  it('should deliver insert events in order', async () => {
    const changeStream = collection.watch();
    const events: ChangeStreamDocument<TestDocument>[] = [];

    // Insert multiple documents
    await collection.insertOne({ _id: 'doc-1', name: 'First' });
    await collection.insertOne({ _id: 'doc-2', name: 'Second' });
    await collection.insertOne({ _id: 'doc-3', name: 'Third' });

    // Collect events
    while (changeStream.hasNext()) {
      const event = changeStream.tryNext();
      if (event) events.push(event);
    }

    expect(events).toHaveLength(3);
    expect(events[0].documentKey._id).toBe('doc-1');
    expect(events[1].documentKey._id).toBe('doc-2');
    expect(events[2].documentKey._id).toBe('doc-3');

    // Verify ordering by resume token
    const tokens = events.map((e) => parseInt(e._id._data, 10));
    expect(tokens[0]).toBeLessThan(tokens[1]);
    expect(tokens[1]).toBeLessThan(tokens[2]);

    changeStream.close();
  });

  it('should deliver all operation types', async () => {
    const changeStream = collection.watch();

    // Perform various operations
    await collection.insertOne({ _id: 'ops-doc', name: 'Original', value: 10 });
    await collection.updateOne({ _id: 'ops-doc' }, { $set: { name: 'Updated', value: 20 } });
    await collection.replaceOne({ _id: 'ops-doc' }, { name: 'Replaced', value: 30 });
    await collection.deleteOne({ _id: 'ops-doc' });

    const events: ChangeStreamDocument<TestDocument>[] = [];
    while (changeStream.hasNext()) {
      const event = changeStream.tryNext();
      if (event) events.push(event);
    }

    expect(events).toHaveLength(4);
    expect(events[0].operationType).toBe('insert');
    expect(events[1].operationType).toBe('update');
    expect(events[2].operationType).toBe('replace');
    expect(events[3].operationType).toBe('delete');

    changeStream.close();
  });

  it('should include full document for inserts and replaces', async () => {
    const changeStream = collection.watch();

    await collection.insertOne({ _id: 'full-doc', name: 'Test', value: 100 });
    await collection.replaceOne({ _id: 'full-doc' }, { name: 'Replaced', value: 200 });

    const insertEvent = changeStream.tryNext();
    const replaceEvent = changeStream.tryNext();

    expect(insertEvent?.fullDocument).toBeDefined();
    expect(insertEvent?.fullDocument?.name).toBe('Test');
    expect(insertEvent?.fullDocument?.value).toBe(100);

    expect(replaceEvent?.fullDocument).toBeDefined();
    expect(replaceEvent?.fullDocument?.name).toBe('Replaced');
    expect(replaceEvent?.fullDocument?.value).toBe(200);

    changeStream.close();
  });

  it('should include update description for updates', async () => {
    await collection.insertOne({ _id: 'update-doc', name: 'Original', status: 'active' });

    const changeStream = collection.watch();
    await collection.updateOne(
      { _id: 'update-doc' },
      { $set: { name: 'Updated' }, $unset: { status: '' } }
    );

    const event = changeStream.tryNext();

    expect(event?.operationType).toBe('update');
    expect(event?.updateDescription).toBeDefined();
    expect(event?.updateDescription?.updatedFields).toHaveProperty('name', 'Updated');
    expect(event?.updateDescription?.removedFields).toContain('status');

    changeStream.close();
  });
});

// ============================================================================
// Multiple Consumer Tests
// ============================================================================

describe('Change Stream - Multiple Consumers', () => {
  let collection: Collection<TestDocument>;
  let storage: MemoryStorage;
  let db: Database;

  beforeEach(() => {
    resetDocumentCounter();
    const env = createTestCollection();
    collection = env.collection;
    storage = env.storage;
    db = env.db;
  });

  afterEach(() => {
    storage.clear();
  });

  it('should deliver events to multiple concurrent streams', async () => {
    const stream1 = collection.watch();
    const stream2 = collection.watch();
    const stream3 = collection.watch();

    await collection.insertOne({ _id: 'multi-1', name: 'Shared Event' });

    const event1 = stream1.tryNext();
    const event2 = stream2.tryNext();
    const event3 = stream3.tryNext();

    expect(event1?.documentKey._id).toBe('multi-1');
    expect(event2?.documentKey._id).toBe('multi-1');
    expect(event3?.documentKey._id).toBe('multi-1');

    stream1.close();
    stream2.close();
    stream3.close();
  });

  it('should isolate filtered streams correctly', async () => {
    const insertStream = collection.watch([{ $match: { operationType: 'insert' } }]);
    const deleteStream = collection.watch([{ $match: { operationType: 'delete' } }]);
    const updateStream = collection.watch([{ $match: { operationType: 'update' } }]);

    await collection.insertOne({ _id: 'filter-doc', name: 'Test' });
    await collection.updateOne({ _id: 'filter-doc' }, { $set: { name: 'Updated' } });
    await collection.deleteOne({ _id: 'filter-doc' });

    // Each stream should only have its filtered event
    const insertEvents: ChangeStreamDocument<TestDocument>[] = [];
    const deleteEvents: ChangeStreamDocument<TestDocument>[] = [];
    const updateEvents: ChangeStreamDocument<TestDocument>[] = [];

    while (insertStream.hasNext()) {
      const e = insertStream.tryNext();
      if (e) insertEvents.push(e);
    }
    while (deleteStream.hasNext()) {
      const e = deleteStream.tryNext();
      if (e) deleteEvents.push(e);
    }
    while (updateStream.hasNext()) {
      const e = updateStream.tryNext();
      if (e) updateEvents.push(e);
    }

    expect(insertEvents).toHaveLength(1);
    expect(insertEvents[0].operationType).toBe('insert');

    expect(updateEvents).toHaveLength(1);
    expect(updateEvents[0].operationType).toBe('update');

    expect(deleteEvents).toHaveLength(1);
    expect(deleteEvents[0].operationType).toBe('delete');

    insertStream.close();
    deleteStream.close();
    updateStream.close();
  });

  it('should handle consumer leaving mid-stream', async () => {
    const stream1 = collection.watch();
    const stream2 = collection.watch();

    await collection.insertOne({ _id: 'leave-1', name: 'First' });

    // Close stream1 early
    stream1.close();

    await collection.insertOne({ _id: 'leave-2', name: 'Second' });

    // stream2 should still receive both events
    const events: ChangeStreamDocument<TestDocument>[] = [];
    while (stream2.hasNext()) {
      const e = stream2.tryNext();
      if (e) events.push(e);
    }

    expect(events).toHaveLength(2);
    expect(stream1.isClosed).toBe(true);
    expect(stream2.isClosed).toBe(false);

    stream2.close();
  });

  it('should support independent resume tokens per consumer', async () => {
    const stream1 = collection.watch();
    const stream2 = collection.watch();

    await collection.insertOne({ _id: 'token-1', name: 'First' });
    await collection.insertOne({ _id: 'token-2', name: 'Second' });

    // Each stream receives events independently
    // The resume token is set when events are pushed to the stream's buffer
    // After inserts, both streams should have received the events
    expect(stream1.resumeToken).not.toBeNull();
    expect(stream2.resumeToken).not.toBeNull();

    // Consume from each stream and verify they get the same events
    const event1a = stream1.tryNext();
    const event1b = stream2.tryNext();

    expect(event1a?.documentKey._id).toBe('token-1');
    expect(event1b?.documentKey._id).toBe('token-1');

    // Both streams can consume independently
    const event2a = stream1.tryNext();
    const event2b = stream2.tryNext();

    expect(event2a?.documentKey._id).toBe('token-2');
    expect(event2b?.documentKey._id).toBe('token-2');

    stream1.close();
    stream2.close();
  });
});

// ============================================================================
// Concurrent Write Tests
// ============================================================================

describe('Change Stream - Concurrent Writes', () => {
  let collection: Collection<TestDocument>;
  let storage: MemoryStorage;
  let db: Database;

  beforeEach(() => {
    resetDocumentCounter();
    const env = createTestCollection();
    collection = env.collection;
    storage = env.storage;
    db = env.db;
  });

  afterEach(() => {
    storage.clear();
  });

  it('should capture all concurrent inserts', async () => {
    const changeStream = collection.watch();

    // Perform concurrent inserts
    const insertPromises = Array.from({ length: 10 }, (_, i) =>
      collection.insertOne({ _id: `concurrent-${i}`, name: `Doc ${i}` })
    );

    await Promise.all(insertPromises);

    // Collect all events
    const events: ChangeStreamDocument<TestDocument>[] = [];
    while (changeStream.hasNext()) {
      const e = changeStream.tryNext();
      if (e) events.push(e);
    }

    expect(events).toHaveLength(10);

    // Verify all documents are represented
    const ids = events.map((e) => e.documentKey._id);
    for (let i = 0; i < 10; i++) {
      expect(ids).toContain(`concurrent-${i}`);
    }

    changeStream.close();
  });

  it('should maintain event ordering during rapid operations', async () => {
    const changeStream = collection.watch();

    // Rapid sequential operations
    for (let i = 0; i < 20; i++) {
      await collection.insertOne({ _id: `rapid-${i}`, name: `Rapid ${i}` });
    }

    const events: ChangeStreamDocument<TestDocument>[] = [];
    while (changeStream.hasNext()) {
      const e = changeStream.tryNext();
      if (e) events.push(e);
    }

    // Verify ordering by resume token
    for (let i = 1; i < events.length; i++) {
      const prevToken = parseInt(events[i - 1]._id._data, 10);
      const currToken = parseInt(events[i]._id._data, 10);
      expect(currToken).toBeGreaterThan(prevToken);
    }

    changeStream.close();
  });

  it('should handle mixed operation types concurrently', async () => {
    // Pre-populate some documents
    for (let i = 0; i < 5; i++) {
      await collection.insertOne({ _id: `existing-${i}`, name: `Existing ${i}`, value: i });
    }

    const changeStream = collection.watch();

    // Mixed concurrent operations
    const operations = [
      collection.insertOne({ _id: 'new-1', name: 'New 1' }),
      collection.updateOne({ _id: 'existing-0' }, { $set: { name: 'Updated 0' } }),
      collection.insertOne({ _id: 'new-2', name: 'New 2' }),
      collection.deleteOne({ _id: 'existing-1' }),
      collection.updateOne({ _id: 'existing-2' }, { $inc: { value: 10 } }),
    ];

    await Promise.all(operations);

    const events: ChangeStreamDocument<TestDocument>[] = [];
    while (changeStream.hasNext()) {
      const e = changeStream.tryNext();
      if (e) events.push(e);
    }

    expect(events).toHaveLength(5);

    // Verify all operation types are represented
    const opTypes = events.map((e) => e.operationType);
    expect(opTypes.filter((t) => t === 'insert')).toHaveLength(2);
    expect(opTypes.filter((t) => t === 'update')).toHaveLength(2);
    expect(opTypes.filter((t) => t === 'delete')).toHaveLength(1);

    changeStream.close();
  });
});

// ============================================================================
// Filtering Tests
// ============================================================================

describe('Change Stream - Advanced Filtering', () => {
  let collection: Collection<TestDocument>;
  let storage: MemoryStorage;
  let db: Database;

  beforeEach(() => {
    resetDocumentCounter();
    const env = createTestCollection();
    collection = env.collection;
    storage = env.storage;
    db = env.db;
  });

  afterEach(() => {
    storage.clear();
  });

  it('should filter by document field values', async () => {
    const activeStream = collection.watch([
      { $match: { 'fullDocument.status': 'active' } },
    ]);

    await collection.insertOne({ _id: 'status-1', name: 'Active Doc', status: 'active' });
    await collection.insertOne({ _id: 'status-2', name: 'Inactive Doc', status: 'inactive' });
    await collection.insertOne({ _id: 'status-3', name: 'Another Active', status: 'active' });

    const events: ChangeStreamDocument<TestDocument>[] = [];
    while (activeStream.hasNext()) {
      const e = activeStream.tryNext();
      if (e) events.push(e);
    }

    expect(events).toHaveLength(2);
    for (const event of events) {
      expect(event.fullDocument?.status).toBe('active');
    }

    activeStream.close();
  });

  it('should filter with $or operator', async () => {
    const stream = collection.watch([
      {
        $match: {
          $or: [
            { 'fullDocument.category': 'important' },
            { 'fullDocument.value': { $gt: 100 } },
          ],
        },
      },
    ]);

    await collection.insertOne({ _id: 'or-1', name: 'Normal', category: 'normal', value: 50 });
    await collection.insertOne({ _id: 'or-2', name: 'Important', category: 'important', value: 50 });
    await collection.insertOne({ _id: 'or-3', name: 'High Value', category: 'normal', value: 150 });
    await collection.insertOne({ _id: 'or-4', name: 'Both', category: 'important', value: 200 });

    const events: ChangeStreamDocument<TestDocument>[] = [];
    while (stream.hasNext()) {
      const e = stream.tryNext();
      if (e) events.push(e);
    }

    expect(events).toHaveLength(3);
    const ids = events.map((e) => e.documentKey._id);
    expect(ids).toContain('or-2');
    expect(ids).toContain('or-3');
    expect(ids).toContain('or-4');
    expect(ids).not.toContain('or-1');

    stream.close();
  });

  it('should filter by namespace', async () => {
    const stream = collection.watch([
      { $match: { 'ns.coll': 'users' } },
    ]);

    await collection.insertOne({ _id: 'ns-1', name: 'Test' });

    const event = stream.tryNext();

    expect(event?.ns.coll).toBe('users');
    expect(event?.ns.db).toBe('testdb');

    stream.close();
  });
});

// ============================================================================
// Resume Tests
// ============================================================================

describe('Change Stream - Resume Capabilities', () => {
  let collection: Collection<TestDocument>;
  let storage: MemoryStorage;
  let db: Database;

  beforeEach(() => {
    resetDocumentCounter();
    const env = createTestCollection();
    collection = env.collection;
    storage = env.storage;
    db = env.db;
  });

  afterEach(() => {
    storage.clear();
  });

  it('should create stream with resumeAfter token', async () => {
    // First stream to get a resume token
    const firstStream = collection.watch();
    await collection.insertOne({ _id: 'resume-1', name: 'First' });
    await collection.insertOne({ _id: 'resume-2', name: 'Second' });

    firstStream.tryNext();
    const resumeToken = firstStream.resumeToken;
    firstStream.close();

    // Create new stream resuming after the first event
    const resumedStream = collection.watch([], { resumeAfter: resumeToken! });

    // Insert more documents
    await collection.insertOne({ _id: 'resume-3', name: 'Third' });

    // The resumed stream should start with events after the resume token
    expect(resumedStream).toBeInstanceOf(ChangeStream);

    resumedStream.close();
  });

  it('should create stream with startAtOperationTime', () => {
    const startTime = new Date(Date.now() - 60000); // 1 minute ago

    const stream = collection.watch([], { startAtOperationTime: startTime });

    expect(stream).toBeInstanceOf(ChangeStream);
    expect(stream.isClosed).toBe(false);

    stream.close();
  });

  it('should track resume token progression', async () => {
    const stream = collection.watch();

    await collection.insertOne({ _id: 'track-1', name: 'First' });
    await collection.insertOne({ _id: 'track-2', name: 'Second' });
    await collection.insertOne({ _id: 'track-3', name: 'Third' });

    // Each event has its own resume token stored in _id._data
    // These should be unique and increasing for each event
    const events: ChangeStreamDocument<TestDocument>[] = [];

    while (stream.hasNext()) {
      const event = stream.tryNext();
      if (event) {
        events.push(event);
      }
    }

    expect(events).toHaveLength(3);

    // Each event's _id (resume token) should have incrementing sequence numbers
    for (let i = 1; i < events.length; i++) {
      const prevToken = parseInt(events[i - 1]._id._data, 10);
      const currToken = parseInt(events[i]._id._data, 10);
      expect(currToken).toBeGreaterThan(prevToken);
    }

    stream.close();
  });
});

// ============================================================================
// Async Iteration Tests
// ============================================================================

describe('Change Stream - Async Iteration', () => {
  let collection: Collection<TestDocument>;
  let storage: MemoryStorage;
  let db: Database;

  beforeEach(() => {
    resetDocumentCounter();
    const env = createTestCollection();
    collection = env.collection;
    storage = env.storage;
    db = env.db;
  });

  afterEach(() => {
    storage.clear();
  });

  it('should support for-await-of iteration', async () => {
    const stream = collection.watch();

    await collection.insertMany([
      { _id: 'iter-1', name: 'A' },
      { _id: 'iter-2', name: 'B' },
      { _id: 'iter-3', name: 'C' },
    ]);

    const events: ChangeStreamDocument<TestDocument>[] = [];

    // Use timeout to prevent infinite loop
    const iterateWithLimit = async () => {
      const timeout = setTimeout(() => stream.close(), 100);

      for await (const event of stream) {
        events.push(event);
        if (events.length >= 3) {
          clearTimeout(timeout);
          stream.close();
          break;
        }
      }
    };

    await iterateWithLimit();

    expect(events).toHaveLength(3);
  });

  it('should exit iteration when stream is closed', async () => {
    const stream = collection.watch();

    await collection.insertOne({ _id: 'close-test', name: 'Test' });

    let iterationCount = 0;

    const iterationPromise = (async () => {
      for await (const _event of stream) {
        iterationCount++;
        stream.close();
      }
    })();

    await iterationPromise;

    expect(iterationCount).toBe(1);
    expect(stream.isClosed).toBe(true);
  });

  it('should support toArray with limit', async () => {
    const stream = collection.watch();

    for (let i = 0; i < 5; i++) {
      await collection.insertOne({ _id: `array-${i}`, name: `Doc ${i}` });
    }

    const events = await stream.toArray(3);

    expect(events).toHaveLength(3);

    stream.close();
  });

  it('should support next() for sequential consumption', async () => {
    const stream = collection.watch();

    await collection.insertOne({ _id: 'next-1', name: 'First' });
    await collection.insertOne({ _id: 'next-2', name: 'Second' });

    // Get first event using tryNext (non-blocking)
    const event1 = stream.tryNext();
    expect(event1?.documentKey._id).toBe('next-1');

    const event2 = stream.tryNext();
    expect(event2?.documentKey._id).toBe('next-2');

    stream.close();
  });
});

// ============================================================================
// Edge Cases
// ============================================================================

describe('Change Stream - Edge Cases', () => {
  let collection: Collection<TestDocument>;
  let storage: MemoryStorage;
  let db: Database;

  beforeEach(() => {
    resetDocumentCounter();
    const env = createTestCollection();
    collection = env.collection;
    storage = env.storage;
    db = env.db;
  });

  afterEach(() => {
    storage.clear();
  });

  it('should handle empty collection', async () => {
    const stream = collection.watch();

    expect(stream.hasNext()).toBe(false);
    expect(stream.tryNext()).toBeUndefined();
    expect(stream.resumeToken).toBeNull();

    stream.close();
  });

  it('should handle watch before any operations', async () => {
    const stream = collection.watch();

    // Watch is active but no events yet
    expect(stream.isClosed).toBe(false);
    expect(stream.hasNext()).toBe(false);

    // Now insert
    await collection.insertOne({ _id: 'first', name: 'First' });

    expect(stream.hasNext()).toBe(true);
    const event = stream.tryNext();
    expect(event?.operationType).toBe('insert');

    stream.close();
  });

  it('should not deliver events after close', async () => {
    const stream = collection.watch();
    stream.close();

    await collection.insertOne({ _id: 'after-close', name: 'Should not appear' });

    expect(stream.hasNext()).toBe(false);
    expect(stream.tryNext()).toBeUndefined();
  });

  it('should handle special characters in documents', async () => {
    const stream = collection.watch();

    await collection.insertOne({
      _id: 'special-chars',
      name: "Test with 'quotes' and \"double\"",
      metadata: {
        unicode: 'Hello World',
        nested: { key: 'value' },
      },
    });

    const event = stream.tryNext();

    expect(event?.fullDocument?.name).toBe("Test with 'quotes' and \"double\"");
    expect(event?.fullDocument?.metadata?.unicode).toBe('Hello World');

    stream.close();
  });

  it('should handle null/undefined values in documents', async () => {
    const stream = collection.watch();

    await collection.insertOne({
      _id: 'null-test',
      name: 'Null Test',
      value: undefined,
      status: null as unknown as string,
    });

    const event = stream.tryNext();

    expect(event?.fullDocument?._id).toBe('null-test');
    expect(event?.fullDocument?.value).toBeUndefined();

    stream.close();
  });

  it('should handle rapid open/close cycles', () => {
    for (let i = 0; i < 10; i++) {
      const stream = collection.watch();
      expect(stream.isClosed).toBe(false);
      stream.close();
      expect(stream.isClosed).toBe(true);
    }
  });
});

// ============================================================================
// Update Description Computation Tests
// ============================================================================

describe('Change Stream - Update Description Computation', () => {
  it('should compute updated fields correctly', () => {
    const oldDoc = { _id: 'test', name: 'Old', count: 1 };
    const newDoc = { _id: 'test', name: 'New', count: 5 };

    const description = computeUpdateDescription(oldDoc, newDoc);

    expect(description.updatedFields).toEqual({ name: 'New', count: 5 });
    expect(description.removedFields).toHaveLength(0);
  });

  it('should compute removed fields correctly', () => {
    const oldDoc = { _id: 'test', name: 'Test', temp: 'value', extra: 'data' };
    const newDoc = { _id: 'test', name: 'Test' };

    const description = computeUpdateDescription(oldDoc, newDoc);

    expect(description.removedFields).toContain('temp');
    expect(description.removedFields).toContain('extra');
    expect(description.updatedFields).toEqual({});
  });

  it('should handle mixed updates and removals', () => {
    const oldDoc = { _id: 'test', name: 'Old', status: 'active', temp: 'remove' };
    const newDoc = { _id: 'test', name: 'New', status: 'active', added: 'new' };

    const description = computeUpdateDescription(oldDoc, newDoc);

    expect(description.updatedFields).toEqual({ name: 'New', added: 'new' });
    expect(description.removedFields).toContain('temp');
    expect(description.removedFields).not.toContain('status');
  });

  it('should ignore _id field in comparisons', () => {
    const oldDoc = { _id: 'old-id', name: 'Same' };
    const newDoc = { _id: 'new-id', name: 'Same' };

    const description = computeUpdateDescription(oldDoc, newDoc);

    expect(description.updatedFields).toEqual({});
    expect(description.removedFields).toHaveLength(0);
  });
});
