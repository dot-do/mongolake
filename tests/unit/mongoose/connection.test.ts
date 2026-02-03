/**
 * Mongoose Connection Unit Tests
 *
 * Tests for the MongoLake Mongoose Connection implementation
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  Connection,
  ConnectionPool,
  ConnectionStates,
  Schema,
  Model,
  deleteModel,
  modelNames,
} from '../../../src/mongoose/index.js';

// ============================================================================
// Connection State Tests
// ============================================================================

describe('Connection States', () => {
  it('starts in disconnected state', () => {
    const conn = new Connection();

    expect(conn.readyState).toBe(ConnectionStates.disconnected);
    expect(conn.state).toBe('disconnected');
  });

  it('moves to connecting state during openUri', async () => {
    const conn = new Connection();
    let capturedState: ConnectionStates | undefined;

    conn.on('connecting', () => {
      capturedState = conn.readyState;
    });

    await conn.openUri('mongolake://localhost/test?local=.test');

    expect(capturedState).toBe(ConnectionStates.connecting);
    await conn.close();
  });

  it('moves to connected state after openUri', async () => {
    const conn = new Connection();

    await conn.openUri('mongolake://localhost/test?local=.test');

    expect(conn.readyState).toBe(ConnectionStates.connected);
    expect(conn.state).toBe('connected');
    await conn.close();
  });

  it('moves to disconnected state after close', async () => {
    const conn = new Connection();

    await conn.openUri('mongolake://localhost/test?local=.test');
    await conn.close();

    expect(conn.readyState).toBe(ConnectionStates.disconnected);
    expect(conn.state).toBe('disconnected');
  });
});

// ============================================================================
// Connection Events Tests
// ============================================================================

describe('Connection Events', () => {
  let conn: Connection;

  beforeEach(() => {
    conn = new Connection();
  });

  afterEach(async () => {
    await conn.close();
  });

  it('emits connecting event', async () => {
    const handler = vi.fn();
    conn.on('connecting', handler);

    await conn.openUri('mongolake://localhost/test?local=.test');

    expect(handler).toHaveBeenCalled();
  });

  it('emits connected event', async () => {
    const handler = vi.fn();
    conn.on('connected', handler);

    await conn.openUri('mongolake://localhost/test?local=.test');

    expect(handler).toHaveBeenCalled();
  });

  it('emits open event', async () => {
    const handler = vi.fn();
    conn.on('open', handler);

    await conn.openUri('mongolake://localhost/test?local=.test');

    expect(handler).toHaveBeenCalled();
  });

  it('emits disconnecting event on close', async () => {
    await conn.openUri('mongolake://localhost/test?local=.test');

    const handler = vi.fn();
    conn.on('disconnecting', handler);

    await conn.close();

    expect(handler).toHaveBeenCalled();
  });

  it('emits disconnected event on close', async () => {
    await conn.openUri('mongolake://localhost/test?local=.test');

    const handler = vi.fn();
    conn.on('disconnected', handler);

    await conn.close();

    expect(handler).toHaveBeenCalled();
  });

  it('emits close event on close', async () => {
    await conn.openUri('mongolake://localhost/test?local=.test');

    const handler = vi.fn();
    conn.on('close', handler);

    await conn.close();

    expect(handler).toHaveBeenCalled();
  });

  it('emits all events to "all" listener', async () => {
    const handler = vi.fn();
    conn.on('all', handler);

    await conn.openUri('mongolake://localhost/test?local=.test');
    await conn.close();

    // Should have received: connecting, connected, open, disconnecting, disconnected, close
    expect(handler.mock.calls.length).toBeGreaterThanOrEqual(6);
  });

  it('once() adds one-time listener', async () => {
    const handler = vi.fn();
    conn.once('connected', handler);

    await conn.openUri('mongolake://localhost/test?local=.test');
    await conn.close();
    await conn.openUri('mongolake://localhost/test?local=.test');

    expect(handler).toHaveBeenCalledTimes(1);
  });

  it('off() removes listener', async () => {
    const handler = vi.fn();
    conn.on('connected', handler);
    conn.off('connected', handler);

    await conn.openUri('mongolake://localhost/test?local=.test');

    expect(handler).not.toHaveBeenCalled();
  });

  it('removeListener() is alias for off()', async () => {
    const handler = vi.fn();
    conn.on('connected', handler);
    conn.removeListener('connected', handler);

    await conn.openUri('mongolake://localhost/test?local=.test');

    expect(handler).not.toHaveBeenCalled();
  });

  it('removeAllListeners() removes all listeners for event', async () => {
    const handler1 = vi.fn();
    const handler2 = vi.fn();
    conn.on('connected', handler1);
    conn.on('connected', handler2);
    conn.removeAllListeners('connected');

    await conn.openUri('mongolake://localhost/test?local=.test');

    expect(handler1).not.toHaveBeenCalled();
    expect(handler2).not.toHaveBeenCalled();
  });

  it('removeAllListeners() without argument removes all', async () => {
    const handler1 = vi.fn();
    const handler2 = vi.fn();
    conn.on('connected', handler1);
    conn.on('open', handler2);
    conn.removeAllListeners();

    await conn.openUri('mongolake://localhost/test?local=.test');

    expect(handler1).not.toHaveBeenCalled();
    expect(handler2).not.toHaveBeenCalled();
  });
});

// ============================================================================
// Connection URI Parsing Tests
// ============================================================================

describe('Connection URI Parsing', () => {
  afterEach(async () => {
    for (const name of modelNames()) {
      deleteModel(name);
    }
  });

  it('parses database name from URI', async () => {
    const conn = new Connection();

    await conn.openUri('mongolake://localhost/mydb?local=.test');

    expect(conn.name).toBe('mydb');
    await conn.close();
  });

  it('parses local option from URI', async () => {
    const conn = new Connection();

    await conn.openUri('mongolake://localhost/test?local=/custom/path');

    expect(conn.config.local).toBe('/custom/path');
    await conn.close();
  });

  it('parses branch option from URI', async () => {
    const conn = new Connection();

    await conn.openUri('mongolake://localhost/test?local=.test&branch=feature-1');

    expect(conn.config.branch).toBe('feature-1');
    await conn.close();
  });

  it('parses autoIndex option from URI', async () => {
    const conn = new Connection();

    await conn.openUri('mongolake://localhost/test?local=.test&autoIndex=false');

    expect(conn.config.autoIndex).toBe(false);
    await conn.close();
  });
});

// ============================================================================
// Connection Model Management Tests
// ============================================================================

describe('Connection Model Management', () => {
  let conn: Connection;

  beforeEach(async () => {
    for (const name of modelNames()) {
      deleteModel(name);
    }
    conn = new Connection();
    await conn.openUri('mongolake://localhost/test?local=.test');
  });

  afterEach(async () => {
    await conn.close();
    for (const name of modelNames()) {
      deleteModel(name);
    }
  });

  it('model() creates a new model', () => {
    const schema = new Schema({ name: String });
    const User = conn.model('ConnUser', schema);

    expect(User).toBeDefined();
    expect(User.modelName).toBe('ConnUser');
  });

  it('model() returns existing model when called without schema', () => {
    const schema = new Schema({ name: String });
    const User1 = conn.model('ConnUser2', schema);
    const User2 = conn.model('ConnUser2');

    expect(User2).toBe(User1);
  });

  it('modelNames() returns registered model names', () => {
    const schema = new Schema({ name: String });
    conn.model('Model1', schema);
    conn.model('Model2', schema);

    const names = conn.modelNames();

    expect(names).toContain('Model1');
    expect(names).toContain('Model2');
  });

  it('deleteModel() removes a model', () => {
    const schema = new Schema({ name: String });
    conn.model('ToDelete', schema);
    conn.deleteModel('ToDelete');

    expect(() => conn.model('ToDelete')).toThrow();
  });

  it('models property returns all models', () => {
    const schema = new Schema({ name: String });
    conn.model('ModelA', schema);
    conn.model('ModelB', schema);

    const models = conn.models;

    expect(models.ModelA).toBeDefined();
    expect(models.ModelB).toBeDefined();
  });
});

// ============================================================================
// Connection Collection Access Tests
// ============================================================================

describe('Connection Collection Access', () => {
  let conn: Connection;

  beforeEach(async () => {
    conn = new Connection();
    await conn.openUri('mongolake://localhost/test?local=.test');
  });

  afterEach(async () => {
    await conn.close();
  });

  it('collection() returns a collection', () => {
    const coll = conn.collection('users');

    expect(coll).toBeDefined();
    expect(coll.name).toBe('users');
  });

  it('collection() throws when not connected', async () => {
    await conn.close();

    expect(() => conn.collection('users')).toThrow();
  });
});

// ============================================================================
// Connection Promise Interface Tests
// ============================================================================

describe('Connection Promise Interface', () => {
  it('asPromise() resolves when connected', async () => {
    const conn = new Connection();
    await conn.openUri('mongolake://localhost/test?local=.test');

    const result = await conn.asPromise();

    expect(result).toBe(conn);
    await conn.close();
  });

  it('asPromise() waits for connection', async () => {
    const conn = new Connection();

    // Start connection
    const openPromise = conn.openUri('mongolake://localhost/test?local=.test');

    // asPromise should wait
    const promise = conn.asPromise();
    await openPromise;

    const result = await promise;
    expect(result).toBe(conn);
    await conn.close();
  });
});

// ============================================================================
// Connection Session Tests
// ============================================================================

describe('Connection Session', () => {
  let conn: Connection;

  beforeEach(async () => {
    conn = new Connection();
    await conn.openUri('mongolake://localhost/test?local=.test');
  });

  afterEach(async () => {
    await conn.close();
  });

  it('startSession() returns a session', async () => {
    const session = await conn.startSession();

    expect(session).toBeDefined();
    expect(session).toHaveProperty('id');
    expect(session).toHaveProperty('startTransaction');
  });

  it('transaction() helper runs transaction', async () => {
    const result = await conn.transaction(async (session) => {
      expect(session).toBeDefined();
      return 'success';
    });

    expect(result).toBe('success');
  });
});

// ============================================================================
// ConnectionPool Tests
// ============================================================================

describe('ConnectionPool', () => {
  it('has a default connection', () => {
    const pool = new ConnectionPool();

    expect(pool.connection).toBeDefined();
    expect(pool.connection).toBeInstanceOf(Connection);
  });

  it('createConnection() creates new connection', () => {
    const pool = new ConnectionPool();

    const conn = pool.createConnection();

    expect(conn).toBeInstanceOf(Connection);
    expect(conn).not.toBe(pool.connection);
  });

  it('createConnection() with URI connects', async () => {
    const pool = new ConnectionPool();

    const conn = pool.createConnection('mongolake://localhost/test?local=.test');

    // Wait for connection
    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(conn.readyState).toBe(ConnectionStates.connected);
    await conn.close();
  });

  it('getConnection() creates named connection', () => {
    const pool = new ConnectionPool();

    const conn1 = pool.getConnection('db1');
    const conn2 = pool.getConnection('db1');

    expect(conn1).toBe(conn2);
    expect(conn1).toBeInstanceOf(Connection);
  });

  it('connections property returns all connections', async () => {
    const pool = new ConnectionPool();
    pool.getConnection('db1');
    pool.getConnection('db2');

    const connections = pool.connections;

    // Default + 2 named = 3
    expect(connections.length).toBe(3);
  });

  it('connect() connects default connection', async () => {
    const pool = new ConnectionPool();

    await pool.connect('mongolake://localhost/test?local=.test');

    expect(pool.connection.readyState).toBe(ConnectionStates.connected);
    await pool.disconnect();
  });

  it('disconnect() closes default and named connections', async () => {
    const pool = new ConnectionPool();

    await pool.connect('mongolake://localhost/test?local=.test');
    const conn2 = pool.getConnection('db2');
    await conn2.openUri('mongolake://localhost/test2?local=.test');

    await pool.disconnect();

    expect(pool.connection.readyState).toBe(ConnectionStates.disconnected);
    expect(conn2.readyState).toBe(ConnectionStates.disconnected);
  });

  it('createConnection() creates independent connection not tracked by pool', async () => {
    const pool = new ConnectionPool();

    await pool.connect('mongolake://localhost/test?local=.test');
    const conn2 = pool.createConnection();
    await conn2.openUri('mongolake://localhost/test2?local=.test');

    // createConnection creates independent connections not tracked by pool
    await pool.disconnect();

    expect(pool.connection.readyState).toBe(ConnectionStates.disconnected);
    // conn2 should still be connected since it's not tracked
    expect(conn2.readyState).toBe(ConnectionStates.connected);

    // Clean up manually
    await conn2.close();
  });
});
