/**
 * Connection Pool Tests
 *
 * Tests for connection pooling functionality including:
 * - Pool initialization
 * - Connection acquisition and release
 * - Pool exhaustion handling
 * - Idle connection cleanup
 * - Connection error recovery
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  ConnectionPool,
  type ConnectionPoolConfig,
  type PooledConnection,
} from '../connection-pool.js';
import { createMockSocket } from '../../../tests/utils/mocks.js';

// ============================================================================
// Pool Initialization Tests
// ============================================================================

describe('ConnectionPool - Initialization', () => {
  let pool: ConnectionPool;

  afterEach(async () => {
    if (pool) {
      await pool.shutdown();
    }
  });

  it('should initialize with default configuration', () => {
    pool = new ConnectionPool();
    const config = pool.getConfig();

    expect(config.maxConnections).toBe(100);
    expect(config.minConnections).toBe(10);
    expect(config.idleTimeout).toBe(30000);
    expect(config.acquireTimeout).toBe(5000);
    expect(config.healthCheckInterval).toBe(10000);
    expect(config.debug).toBe(false);
  });

  it('should initialize with custom configuration', () => {
    const customConfig: ConnectionPoolConfig = {
      maxConnections: 50,
      minConnections: 5,
      idleTimeout: 15000,
      acquireTimeout: 3000,
      healthCheckInterval: 5000,
      debug: true,
    };

    pool = new ConnectionPool(customConfig);
    const config = pool.getConfig();

    expect(config.maxConnections).toBe(50);
    expect(config.minConnections).toBe(5);
    expect(config.idleTimeout).toBe(15000);
    expect(config.acquireTimeout).toBe(3000);
    expect(config.healthCheckInterval).toBe(5000);
    expect(config.debug).toBe(true);
  });

  it('should start with zero connections', () => {
    pool = new ConnectionPool();
    const metrics = pool.getMetrics();

    expect(metrics.totalConnections).toBe(0);
    expect(metrics.activeConnections).toBe(0);
    expect(metrics.idleConnections).toBe(0);
  });

  it('should start with zero metrics counters', () => {
    pool = new ConnectionPool();
    const metrics = pool.getMetrics();

    expect(metrics.totalCreated).toBe(0);
    expect(metrics.totalDestroyed).toBe(0);
    expect(metrics.acquireCount).toBe(0);
    expect(metrics.releaseCount).toBe(0);
    expect(metrics.acquireTimeouts).toBe(0);
    expect(metrics.errorCount).toBe(0);
    expect(metrics.idleTimeoutCount).toBe(0);
  });
});

// ============================================================================
// Connection Addition Tests
// ============================================================================

describe('ConnectionPool - Connection Addition', () => {
  let pool: ConnectionPool;

  beforeEach(() => {
    pool = new ConnectionPool({ maxConnections: 10 });
  });

  afterEach(async () => {
    await pool.shutdown();
  });

  it('should add a connection and return PooledConnection', () => {
    const socket = createMockSocket();
    const connection = pool.addConnection(socket);

    expect(connection).toBeDefined();
    expect(connection.id).toBe(1);
    expect(connection.socket).toBe(socket);
    expect(connection.inUse).toBe(true);
    expect(connection.reuseCount).toBe(0);
    expect(connection.healthy).toBe(true);
    expect(connection.buffer).toBeInstanceOf(Uint8Array);
  });

  it('should assign sequential connection IDs', () => {
    const conn1 = pool.addConnection(createMockSocket());
    const conn2 = pool.addConnection(createMockSocket());
    const conn3 = pool.addConnection(createMockSocket());

    expect(conn1.id).toBe(1);
    expect(conn2.id).toBe(2);
    expect(conn3.id).toBe(3);
  });

  it('should update metrics when adding connections', () => {
    pool.addConnection(createMockSocket());
    pool.addConnection(createMockSocket());

    const metrics = pool.getMetrics();

    expect(metrics.totalConnections).toBe(2);
    expect(metrics.activeConnections).toBe(2);
    expect(metrics.idleConnections).toBe(0);
    expect(metrics.totalCreated).toBe(2);
  });

  it('should emit connection:created event', () => {
    const createdHandler = vi.fn();
    pool.on('connection:created', createdHandler);

    const socket = createMockSocket();
    const connection = pool.addConnection(socket);

    expect(createdHandler).toHaveBeenCalledTimes(1);
    expect(createdHandler).toHaveBeenCalledWith(connection);
  });

  it('should report capacity correctly', () => {
    expect(pool.hasCapacity()).toBe(true);

    // Add 10 connections (max)
    for (let i = 0; i < 10; i++) {
      pool.addConnection(createMockSocket());
    }

    expect(pool.hasCapacity()).toBe(false);
  });
});

// ============================================================================
// Connection Acquisition and Release Tests
// ============================================================================

describe('ConnectionPool - Acquisition and Release', () => {
  let pool: ConnectionPool;

  beforeEach(() => {
    pool = new ConnectionPool({
      maxConnections: 5,
      minConnections: 2,
      acquireTimeout: 100,
    });
  });

  afterEach(async () => {
    await pool.shutdown();
  });

  it('should release a connection and mark it as idle', () => {
    const connection = pool.addConnection(createMockSocket());

    expect(connection.inUse).toBe(true);

    pool.release(connection.id);

    expect(connection.inUse).toBe(false);

    const metrics = pool.getMetrics();
    expect(metrics.activeConnections).toBe(0);
    expect(metrics.idleConnections).toBe(1);
    expect(metrics.releaseCount).toBe(1);
  });

  it('should emit connection:released event', () => {
    const releasedHandler = vi.fn();
    pool.on('connection:released', releasedHandler);

    const connection = pool.addConnection(createMockSocket());
    pool.release(connection.id);

    expect(releasedHandler).toHaveBeenCalledTimes(1);
    expect(releasedHandler).toHaveBeenCalledWith(connection);
  });

  it('should acquire an idle connection', async () => {
    const connection = pool.addConnection(createMockSocket());
    pool.release(connection.id);

    const acquired = await pool.acquire();

    expect(acquired.id).toBe(connection.id);
    expect(acquired.inUse).toBe(true);
    expect(acquired.reuseCount).toBe(1);
  });

  it('should emit connection:acquired event', async () => {
    const acquiredHandler = vi.fn();
    pool.on('connection:acquired', acquiredHandler);

    const connection = pool.addConnection(createMockSocket());
    pool.release(connection.id);

    await pool.acquire();

    expect(acquiredHandler).toHaveBeenCalledTimes(1);
  });

  it('should update lastUsedAt on release', async () => {
    const connection = pool.addConnection(createMockSocket());
    const originalLastUsed = connection.lastUsedAt;

    // Small delay
    await new Promise((r) => setTimeout(r, 10));

    pool.release(connection.id);

    expect(connection.lastUsedAt).toBeGreaterThan(originalLastUsed);
  });

  it('should increment reuseCount on acquire', async () => {
    const connection = pool.addConnection(createMockSocket());

    expect(connection.reuseCount).toBe(0);

    pool.release(connection.id);
    await pool.acquire();
    expect(connection.reuseCount).toBe(1);

    pool.release(connection.id);
    await pool.acquire();
    expect(connection.reuseCount).toBe(2);
  });

  it('should throw when no idle connections and caller should create new', async () => {
    // All connections are in use
    pool.addConnection(createMockSocket());

    await expect(pool.acquire()).rejects.toThrow(
      'No idle connections available; create a new connection'
    );
  });

  it('should handle release of unknown connection gracefully', () => {
    // Should not throw
    pool.release(999);

    const metrics = pool.getMetrics();
    expect(metrics.releaseCount).toBe(0);
  });
});

// ============================================================================
// Pool Exhaustion Tests
// ============================================================================

describe('ConnectionPool - Pool Exhaustion', () => {
  let pool: ConnectionPool;

  beforeEach(() => {
    pool = new ConnectionPool({
      maxConnections: 3,
      acquireTimeout: 50,
    });
  });

  afterEach(async () => {
    await pool.shutdown();
  });

  it('should timeout when waiting for connection in exhausted pool', async () => {
    // Fill the pool with active connections
    pool.addConnection(createMockSocket());
    pool.addConnection(createMockSocket());
    pool.addConnection(createMockSocket());

    // Try to acquire - should timeout
    await expect(pool.acquire()).rejects.toThrow('Acquire timeout after 50ms - pool exhausted');

    const metrics = pool.getMetrics();
    expect(metrics.acquireTimeouts).toBe(1);
  });

  it('should emit pool:exhausted event on timeout', async () => {
    const exhaustedHandler = vi.fn();
    pool.on('pool:exhausted', exhaustedHandler);

    // Fill the pool
    pool.addConnection(createMockSocket());
    pool.addConnection(createMockSocket());
    pool.addConnection(createMockSocket());

    // Try to acquire - should timeout
    await expect(pool.acquire()).rejects.toThrow();

    expect(exhaustedHandler).toHaveBeenCalledTimes(1);
  });

  it('should acquire when connection becomes available during wait', async () => {
    // Fill the pool
    const conn1 = pool.addConnection(createMockSocket());
    pool.addConnection(createMockSocket());
    pool.addConnection(createMockSocket());

    // Start acquire (will wait)
    const acquirePromise = pool.acquire();

    // Release a connection after a short delay
    setTimeout(() => {
      pool.release(conn1.id);
    }, 20);

    // Should resolve with the released connection
    const acquired = await acquirePromise;
    expect(acquired.id).toBe(conn1.id);
  });

  it('should emit pool:available when connection becomes available', async () => {
    const availableHandler = vi.fn();
    pool.on('pool:available', availableHandler);

    // Fill the pool
    const conn1 = pool.addConnection(createMockSocket());
    pool.addConnection(createMockSocket());
    pool.addConnection(createMockSocket());

    // Start acquire (will wait)
    const acquirePromise = pool.acquire();

    // Release a connection after a short delay
    setTimeout(() => {
      pool.release(conn1.id);
    }, 10);

    await acquirePromise;

    expect(availableHandler).toHaveBeenCalledTimes(1);
  });
});

// ============================================================================
// Idle Connection Cleanup Tests
// ============================================================================

describe('ConnectionPool - Idle Connection Cleanup', () => {
  let pool: ConnectionPool;

  afterEach(async () => {
    if (pool) {
      await pool.shutdown();
    }
  });

  it('should remove idle connections after timeout (respecting minConnections)', async () => {
    pool = new ConnectionPool({
      maxConnections: 10,
      minConnections: 1,
      idleTimeout: 30, // 30ms for testing
      idleCheckInterval: 20, // Check every 20ms for faster tests
    });

    // Add 3 connections and release them
    const conn1 = pool.addConnection(createMockSocket());
    const conn2 = pool.addConnection(createMockSocket());
    const conn3 = pool.addConnection(createMockSocket());

    pool.release(conn1.id);
    pool.release(conn2.id);
    pool.release(conn3.id);

    expect(pool.getMetrics().idleConnections).toBe(3);

    // Wait for idle timeout + idle check interval
    await new Promise((r) => setTimeout(r, 100));

    // Should have removed excess idle connections but kept minConnections
    const metrics = pool.getMetrics();
    expect(metrics.idleConnections).toBeLessThanOrEqual(1);
    expect(metrics.idleTimeoutCount).toBeGreaterThan(0);
  });

  it('should not remove active connections', async () => {
    pool = new ConnectionPool({
      maxConnections: 5,
      minConnections: 0,
      idleTimeout: 30,
      idleCheckInterval: 20,
    });

    // Add connections but keep them active (in use)
    pool.addConnection(createMockSocket());
    pool.addConnection(createMockSocket());

    expect(pool.getMetrics().activeConnections).toBe(2);

    // Wait for idle timeout check
    await new Promise((r) => setTimeout(r, 100));

    // Active connections should not be removed
    expect(pool.getMetrics().activeConnections).toBe(2);
    expect(pool.getMetrics().idleTimeoutCount).toBe(0);
  });

  it('should emit connection:destroyed event on idle cleanup', async () => {
    pool = new ConnectionPool({
      maxConnections: 5,
      minConnections: 0,
      idleTimeout: 30,
      idleCheckInterval: 20,
    });

    const destroyedHandler = vi.fn();
    pool.on('connection:destroyed', destroyedHandler);

    const conn = pool.addConnection(createMockSocket());
    pool.release(conn.id);

    // Wait for idle timeout + idle check interval
    await new Promise((r) => setTimeout(r, 100));

    expect(destroyedHandler).toHaveBeenCalled();
    expect(destroyedHandler).toHaveBeenCalledWith(conn.id, 'idle timeout');
  });
});

// ============================================================================
// Connection Error Recovery Tests
// ============================================================================

describe('ConnectionPool - Error Recovery', () => {
  let pool: ConnectionPool;

  beforeEach(() => {
    pool = new ConnectionPool({ maxConnections: 5 });
  });

  afterEach(async () => {
    await pool.shutdown();
  });

  it('should handle connection error and remove connection', () => {
    const connection = pool.addConnection(createMockSocket());
    const error = new Error('Connection lost');

    pool.handleError(connection.id, error);

    expect(pool.get(connection.id)).toBeUndefined();

    const metrics = pool.getMetrics();
    expect(metrics.errorCount).toBe(1);
    expect(metrics.totalDestroyed).toBe(1);
    expect(metrics.totalConnections).toBe(0);
  });

  it('should emit connection:error event', () => {
    const errorHandler = vi.fn();
    pool.on('connection:error', errorHandler);

    const connection = pool.addConnection(createMockSocket());
    const error = new Error('Connection lost');

    pool.handleError(connection.id, error);

    expect(errorHandler).toHaveBeenCalledTimes(1);
    expect(errorHandler).toHaveBeenCalledWith(connection.id, error);
  });

  it('should emit connection:destroyed event on error', () => {
    const destroyedHandler = vi.fn();
    pool.on('connection:destroyed', destroyedHandler);

    const connection = pool.addConnection(createMockSocket());
    pool.handleError(connection.id, new Error('test'));

    expect(destroyedHandler).toHaveBeenCalledWith(connection.id, expect.stringContaining('error'));
  });

  it('should mark connection as unhealthy before removal', () => {
    const connection = pool.addConnection(createMockSocket());

    expect(connection.healthy).toBe(true);

    // Capture the connection state before it's removed
    let capturedHealthy: boolean | undefined;
    pool.on('connection:error', () => {
      capturedHealthy = connection.healthy;
    });

    pool.handleError(connection.id, new Error('test'));

    expect(capturedHealthy).toBe(false);
  });

  it('should handle error for unknown connection gracefully', () => {
    // Should not throw
    pool.handleError(999, new Error('test'));

    const metrics = pool.getMetrics();
    expect(metrics.errorCount).toBe(0);
  });

  it('should destroy socket on removal', () => {
    const socket = createMockSocket();
    const connection = pool.addConnection(socket);

    expect(socket.destroyed).toBe(false);

    pool.remove(connection.id, 'test removal');

    expect(socket.destroyed).toBe(true);
  });
});

// ============================================================================
// Connection Retrieval Tests
// ============================================================================

describe('ConnectionPool - Connection Retrieval', () => {
  let pool: ConnectionPool;

  beforeEach(() => {
    pool = new ConnectionPool();
  });

  afterEach(async () => {
    await pool.shutdown();
  });

  it('should get connection by ID', () => {
    const connection = pool.addConnection(createMockSocket());

    const retrieved = pool.get(connection.id);

    expect(retrieved).toBeDefined();
    expect(retrieved?.id).toBe(connection.id);
  });

  it('should return undefined for unknown connection ID', () => {
    const retrieved = pool.get(999);

    expect(retrieved).toBeUndefined();
  });
});

// ============================================================================
// Shutdown Tests
// ============================================================================

describe('ConnectionPool - Shutdown', () => {
  let pool: ConnectionPool;

  afterEach(async () => {
    if (pool) {
      await pool.shutdown();
    }
  });

  it('should close all connections on shutdown', async () => {
    pool = new ConnectionPool();

    const socket1 = createMockSocket();
    const socket2 = createMockSocket();

    pool.addConnection(socket1);
    pool.addConnection(socket2);

    await pool.shutdown();

    expect(socket1.destroyed).toBe(true);
    expect(socket2.destroyed).toBe(true);
  });

  it('should reject pending acquires on shutdown', async () => {
    pool = new ConnectionPool({
      maxConnections: 1,
      acquireTimeout: 5000,
    });

    // Fill the pool
    pool.addConnection(createMockSocket());

    // Start waiting for a connection
    const acquirePromise = pool.acquire();

    // Shutdown while waiting
    const shutdownPromise = pool.shutdown();

    // Both should resolve/reject appropriately
    await expect(acquirePromise).rejects.toThrow('Connection pool is shutting down');
    await shutdownPromise;
  });

  it('should reject new connections after shutdown starts', async () => {
    pool = new ConnectionPool();

    // Start shutdown
    const shutdownPromise = pool.shutdown();

    // Try to add a connection
    const socket = createMockSocket();

    expect(() => pool.addConnection(socket)).toThrow(
      'Connection pool is shutting down'
    );

    await shutdownPromise;
  });
});

// ============================================================================
// Health Check Tests
// ============================================================================

describe('ConnectionPool - Health Check', () => {
  let pool: ConnectionPool;

  afterEach(async () => {
    if (pool) {
      await pool.shutdown();
    }
  });

  it('should remove connections with destroyed sockets', async () => {
    pool = new ConnectionPool({
      healthCheckInterval: 50,
    });

    const socket = createMockSocket();
    const connection = pool.addConnection(socket);
    pool.release(connection.id);

    // Simulate socket destruction
    socket.destroy();

    // Wait for health check
    await new Promise((r) => setTimeout(r, 100));

    expect(pool.get(connection.id)).toBeUndefined();
  });

  it('should not check active connections during health check', async () => {
    pool = new ConnectionPool({
      healthCheckInterval: 50,
    });

    const socket = createMockSocket();
    const connection = pool.addConnection(socket);

    // Keep connection active (in use)
    // Simulate socket becoming unwritable
    socket.writable = false;

    // Wait for health check
    await new Promise((r) => setTimeout(r, 100));

    // Active connections should not be checked/removed
    expect(pool.get(connection.id)).toBeDefined();
  });
});

// ============================================================================
// Metrics Tests
// ============================================================================

describe('ConnectionPool - Metrics', () => {
  let pool: ConnectionPool;

  beforeEach(() => {
    pool = new ConnectionPool({ maxConnections: 10 });
  });

  afterEach(async () => {
    await pool.shutdown();
  });

  it('should track totalCreated correctly', () => {
    pool.addConnection(createMockSocket());
    pool.addConnection(createMockSocket());
    pool.addConnection(createMockSocket());

    expect(pool.getMetrics().totalCreated).toBe(3);
  });

  it('should track totalDestroyed correctly', () => {
    const conn1 = pool.addConnection(createMockSocket());
    const conn2 = pool.addConnection(createMockSocket());

    pool.remove(conn1.id, 'test');
    pool.remove(conn2.id, 'test');

    expect(pool.getMetrics().totalDestroyed).toBe(2);
  });

  it('should track acquireCount correctly', async () => {
    const conn = pool.addConnection(createMockSocket());

    pool.release(conn.id);
    await pool.acquire();

    pool.release(conn.id);
    await pool.acquire();

    expect(pool.getMetrics().acquireCount).toBe(2);
  });

  it('should track releaseCount correctly', () => {
    const conn = pool.addConnection(createMockSocket());

    pool.release(conn.id);
    pool.release(conn.id); // Releasing again should still count

    expect(pool.getMetrics().releaseCount).toBe(2);
  });

  it('should return a copy of metrics', () => {
    pool.addConnection(createMockSocket());

    const metrics1 = pool.getMetrics();
    const metrics2 = pool.getMetrics();

    expect(metrics1).not.toBe(metrics2);
    expect(metrics1).toEqual(metrics2);
  });
});
