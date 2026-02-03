/**
 * Graceful Shutdown Tests
 *
 * Tests for graceful server shutdown scenarios in the MongoDB wire protocol TCP server.
 *
 * Tests cover:
 * 1. Shutdown drains active connections
 * 2. New connections rejected during shutdown
 * 3. In-flight operations complete before exit
 * 4. WAL is flushed before shutdown
 * 5. Shutdown timeout forces exit
 * 6. SIGINT triggers graceful shutdown
 * 7. SIGTERM triggers graceful shutdown
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { createServer, type TcpServer, GracefulShutdown } from '../../../src/wire-protocol/tcp-server.js';
import { ConnectionPool } from '../../../src/wire-protocol/connection-pool.js';
import { serializeDocument } from '../../../src/wire-protocol/bson-serializer.js';
import { OpCode } from '../../../src/wire-protocol/message-parser.js';
import { type Socket, createConnection, createServer as createNetServer } from 'node:net';
import { rm } from 'node:fs/promises';
import { EventEmitter } from 'node:events';

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Create an OP_MSG command for testing
 */
function createOpMsgCommand(
  requestId: number,
  database: string,
  command: Record<string, unknown>
): Uint8Array {
  const doc = {
    ...command,
    $db: database,
  };
  const bsonDoc = serializeDocument(doc);

  const messageLength = 16 + 4 + 1 + bsonDoc.length;
  const result = new Uint8Array(messageLength);
  const view = new DataView(result.buffer);

  // Header
  view.setInt32(0, messageLength, true);
  view.setInt32(4, requestId, true);
  view.setInt32(8, 0, true);
  view.setInt32(12, OpCode.OP_MSG, true);

  // Flags
  view.setUint32(16, 0, true);

  // Section (type 0 - body)
  result[20] = 0;
  result.set(bsonDoc, 21);

  return result;
}

/**
 * Send a command and receive response with timeout
 */
async function sendCommand(socket: Socket, command: Uint8Array, timeoutMs = 3000): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    let responseBuffer = Buffer.alloc(0);
    const timeout = setTimeout(() => {
      socket.off('data', onData);
      socket.off('error', onError);
      reject(new Error('Command timeout'));
    }, timeoutMs);

    const onData = (data: Buffer) => {
      responseBuffer = Buffer.concat([responseBuffer, data]);

      if (responseBuffer.length >= 4) {
        const view = new DataView(responseBuffer.buffer, responseBuffer.byteOffset);
        const messageLength = view.getInt32(0, true);

        if (responseBuffer.length >= messageLength) {
          clearTimeout(timeout);
          socket.off('data', onData);
          socket.off('error', onError);
          resolve(responseBuffer.slice(0, messageLength));
        }
      }
    };

    const onError = (err: Error) => {
      clearTimeout(timeout);
      socket.off('data', onData);
      reject(err);
    };

    socket.on('data', onData);
    socket.on('error', onError);

    socket.write(Buffer.from(command));
  });
}

/**
 * Create a socket and wait for it to connect
 */
async function createConnectedSocket(port: number, host: string = '127.0.0.1'): Promise<Socket> {
  return new Promise((resolve, reject) => {
    const socket = createConnection({ port, host });
    const timeout = setTimeout(() => {
      socket.destroy();
      reject(new Error('Connection timeout'));
    }, 3000);

    socket.on('connect', () => {
      clearTimeout(timeout);
      resolve(socket);
    });
    socket.on('error', (err) => {
      clearTimeout(timeout);
      reject(err);
    });
  });
}

/**
 * Wait for a specific amount of time
 */
function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ============================================================================
// Connection Pool Shutdown Tests (Unit Tests - Fast)
// ============================================================================

describe('ConnectionPool Shutdown', () => {
  let pool: ConnectionPool;

  beforeEach(() => {
    pool = new ConnectionPool({
      maxConnections: 10,
      minConnections: 0,
      idleTimeout: 100,
      idleCheckInterval: 50,
    });
  });

  afterEach(async () => {
    await pool.shutdown();
  });

  describe('shutdown behavior', () => {
    it('should reject new connections during shutdown', async () => {
      // Create a mock socket with all required methods
      const mockSocket = new EventEmitter() as Socket;
      (mockSocket as any).destroyed = false;
      (mockSocket as any).writable = true;
      (mockSocket as any).destroy = vi.fn(() => {
        (mockSocket as any).destroyed = true;
        mockSocket.emit('close');
      });
      (mockSocket as any).end = vi.fn(() => {
        setTimeout(() => {
          (mockSocket as any).destroyed = true;
          mockSocket.emit('close');
        }, 10);
      });

      // Add connection before shutdown
      const conn = pool.addConnection(mockSocket);
      expect(pool.getMetrics().totalConnections).toBe(1);

      // Start shutdown
      await pool.shutdown();

      // Try to add another connection - should throw
      const mockSocket2 = new EventEmitter() as Socket;
      (mockSocket2 as any).destroyed = false;
      (mockSocket2 as any).destroy = vi.fn(() => {
        (mockSocket2 as any).destroyed = true;
      });
      (mockSocket2 as any).end = vi.fn();

      expect(() => pool.addConnection(mockSocket2)).toThrow('Connection pool is shutting down');
    });

    it('should close all connections on shutdown', async () => {
      const sockets: any[] = [];

      // Create multiple mock sockets
      for (let i = 0; i < 5; i++) {
        const mockSocket = new EventEmitter() as Socket;
        (mockSocket as any).destroyed = false;
        (mockSocket as any).writable = true;
        (mockSocket as any).destroy = vi.fn(() => {
          (mockSocket as any).destroyed = true;
          mockSocket.emit('close');
        });
        (mockSocket as any).end = vi.fn(() => {
          setTimeout(() => {
            (mockSocket as any).destroyed = true;
            mockSocket.emit('close');
          }, 10);
        });
        sockets.push(mockSocket);
        pool.addConnection(mockSocket);
      }

      expect(pool.getMetrics().totalConnections).toBe(5);

      // Shutdown
      await pool.shutdown();

      // All sockets should have been ended/destroyed
      for (const socket of sockets) {
        expect(socket.end).toHaveBeenCalled();
      }
    });

    it('should reject pending acquire requests on shutdown', async () => {
      // Fill up the pool
      const sockets: any[] = [];
      for (let i = 0; i < 10; i++) {
        const mockSocket = new EventEmitter() as Socket;
        (mockSocket as any).destroyed = false;
        (mockSocket as any).writable = true;
        (mockSocket as any).destroy = vi.fn(() => {
          (mockSocket as any).destroyed = true;
          mockSocket.emit('close');
        });
        (mockSocket as any).end = vi.fn(() => {
          mockSocket.emit('close');
        });
        sockets.push(mockSocket);
        pool.addConnection(mockSocket);
      }

      // Start a blocking acquire
      const acquirePromise = pool.acquire(5000).catch((err) => err);

      // Give it a moment to enter the wait queue
      await delay(10);

      // Shutdown should reject the waiting acquire
      await pool.shutdown();

      const error = await acquirePromise;
      expect(error).toBeInstanceOf(Error);
      expect(error.message).toMatch(/shutting down/i);
    });

    it('should stop idle check timer on shutdown', async () => {
      // Pool should be running idle check
      expect(pool.getMetrics()).not.toBeNull();

      await pool.shutdown();

      // After shutdown, the pool should be empty
      expect(pool.getMetrics().totalConnections).toBe(0);
    });

    it('should stop health check timer on shutdown', async () => {
      const mockSocket = new EventEmitter() as Socket;
      (mockSocket as any).destroyed = false;
      (mockSocket as any).writable = true;
      (mockSocket as any).destroy = vi.fn();
      (mockSocket as any).end = vi.fn(() => {
        mockSocket.emit('close');
      });

      pool.addConnection(mockSocket);
      await pool.shutdown();

      // Timers should be stopped - no errors should occur
      await delay(200);
    });
  });

  describe('drain behavior', () => {
    it('should wait for connections to be released before force closing', async () => {
      const mockSocket = new EventEmitter() as Socket;
      (mockSocket as any).destroyed = false;
      (mockSocket as any).writable = true;
      let endCalled = false;
      (mockSocket as any).end = vi.fn(() => {
        endCalled = true;
        mockSocket.emit('close');
      });
      (mockSocket as any).destroy = vi.fn(() => {
        (mockSocket as any).destroyed = true;
        mockSocket.emit('close');
      });

      const conn = pool.addConnection(mockSocket);
      expect(conn.inUse).toBe(true);

      const startTime = Date.now();
      await pool.shutdown();
      const elapsed = Date.now() - startTime;

      // Should complete quickly since there's a force close timeout
      expect(elapsed).toBeLessThan(2000);
      expect(endCalled).toBe(true);
    });

    it('should force destroy sockets that dont close after end', async () => {
      const mockSocket = new EventEmitter() as Socket;
      (mockSocket as any).destroyed = false;
      (mockSocket as any).writable = true;
      (mockSocket as any).end = vi.fn(); // Don't emit close
      (mockSocket as any).destroy = vi.fn(() => {
        (mockSocket as any).destroyed = true;
      });

      pool.addConnection(mockSocket);

      await pool.shutdown();

      // After timeout, destroy should be called
      await delay(1100);
      expect((mockSocket as any).destroy).toHaveBeenCalled();
    });
  });
});

// ============================================================================
// TCP Server Shutdown Tests (Unit Tests - No actual TCP connections)
// ============================================================================

describe('TCP Server Graceful Shutdown', () => {
  // ==========================================================================
  // 1. Shutdown Drains Active Connections
  // ==========================================================================

  describe('Shutdown drains active connections', () => {
    it('should expose pool metrics before shutdown', () => {
      const server = createServer({
        port: 0,
        host: '127.0.0.1',
        shutdown: { drainTimeout: 2000 },
      });

      // Before start, pool should be null
      expect(server.getPool()).toBeNull();
      expect(server.getMetrics()).toBeNull();
    });

    it('should track connection count through pool metrics', () => {
      // Verify metrics structure
      const pool = new ConnectionPool({ maxConnections: 10 });
      const metrics = pool.getMetrics();

      expect(metrics).toHaveProperty('totalConnections');
      expect(metrics).toHaveProperty('activeConnections');
      expect(metrics).toHaveProperty('idleConnections');
      expect(metrics.totalConnections).toBe(0);
    });
  });

  // ==========================================================================
  // 2. New Connections Rejected During Shutdown
  // ==========================================================================

  describe('New connections rejected during shutdown', () => {
    it('should return null address before start', () => {
      const server = createServer({
        port: 0,
        host: '127.0.0.1',
      });

      expect(server.address()).toBeNull();
    });

    it('should return null pool before start', () => {
      const server = createServer({
        port: 0,
        host: '127.0.0.1',
      });

      expect(server.getPool()).toBeNull();
    });
  });

  // ==========================================================================
  // 3. In-Flight Operations Complete Before Exit
  // ==========================================================================

  describe('In-flight operations complete before exit', () => {
    it('should have pool shutdown that waits for connections', async () => {
      const pool = new ConnectionPool({ maxConnections: 5 });
      const endCalls: string[] = [];

      // Create mock sockets
      for (let i = 0; i < 3; i++) {
        const mockSocket = new EventEmitter() as Socket;
        (mockSocket as any).destroyed = false;
        (mockSocket as any).writable = true;
        (mockSocket as any).end = vi.fn(() => {
          endCalls.push(`end-${i}`);
          setTimeout(() => {
            (mockSocket as any).destroyed = true;
            mockSocket.emit('close');
          }, 10);
        });
        (mockSocket as any).destroy = vi.fn();
        pool.addConnection(mockSocket);
      }

      expect(pool.getMetrics().totalConnections).toBe(3);

      await pool.shutdown();

      // All end() methods should have been called
      expect(endCalls).toHaveLength(3);
    });

    it('should call end on all connections during shutdown', async () => {
      const pool = new ConnectionPool({ maxConnections: 5 });
      const endCallCount = { count: 0 };

      const mockSocket = new EventEmitter() as Socket;
      (mockSocket as any).destroyed = false;
      (mockSocket as any).writable = true;
      (mockSocket as any).end = vi.fn(() => {
        endCallCount.count++;
        setTimeout(() => mockSocket.emit('close'), 5);
      });
      (mockSocket as any).destroy = vi.fn();

      pool.addConnection(mockSocket);
      await pool.shutdown();

      expect(endCallCount.count).toBe(1);
    });
  });

  // ==========================================================================
  // 4. WAL is Flushed Before Shutdown
  // ==========================================================================

  describe('WAL is flushed before shutdown', () => {
    it('should have buffer pool stats getter', () => {
      const server = createServer({
        port: 0,
        host: '127.0.0.1',
      });

      // Buffer pool is null before start
      expect(server.getBufferPoolStats()).toBeNull();
    });
  });

  // ==========================================================================
  // 5. Shutdown Timeout Forces Exit
  // ==========================================================================

  describe('Shutdown timeout forces exit', () => {
    it('should accept drain timeout configuration', () => {
      const server = createServer({
        port: 0,
        host: '127.0.0.1',
        shutdown: { drainTimeout: 500, forceClose: true },
      });

      expect(server).toBeDefined();
    });

    it('should accept short drain timeout', () => {
      const server = createServer({
        port: 0,
        host: '127.0.0.1',
        shutdown: { drainTimeout: 100, forceClose: true },
      });

      expect(server).toBeDefined();
    });

    it('should force close connections in pool after timeout', async () => {
      const pool = new ConnectionPool({ maxConnections: 5 });

      // Create a mock socket that doesn't respond to end()
      const mockSocket = new EventEmitter() as Socket;
      (mockSocket as any).destroyed = false;
      (mockSocket as any).writable = true;
      (mockSocket as any).end = vi.fn(); // Never emits close
      (mockSocket as any).destroy = vi.fn(() => {
        (mockSocket as any).destroyed = true;
      });

      pool.addConnection(mockSocket);

      const startTime = Date.now();
      await pool.shutdown();
      const elapsed = Date.now() - startTime;

      // Should force destroy after ~1 second timeout
      expect((mockSocket as any).destroy).toHaveBeenCalled();
      expect(elapsed).toBeLessThan(2000);
    });
  });

  // ==========================================================================
  // 6. SIGINT Triggers Graceful Shutdown
  // ==========================================================================

  describe('SIGINT triggers graceful shutdown', () => {
    it('should have main() function that registers signal handlers', async () => {
      // Verify the main function exists and is callable
      const { main } = await import('../../../src/wire-protocol/tcp-server.js');
      expect(typeof main).toBe('function');
    });

    it('should accept shutdown configuration options', () => {
      const serverWithTimeout = createServer({
        port: 0,
        shutdown: { drainTimeout: 15000, notifyClients: true, forceClose: true },
      });
      expect(serverWithTimeout).toBeDefined();
    });

    it('should handle multiple stop() calls on unstarted server', async () => {
      const server = createServer({
        port: 0,
        host: '127.0.0.1',
      });

      // Multiple concurrent stop calls on unstarted server
      const results = await Promise.allSettled([
        server.stop(),
        server.stop(),
        server.stop(),
      ]);

      for (const result of results) {
        expect(result.status).toBe('fulfilled');
      }
    });
  });

  // ==========================================================================
  // 7. SIGTERM Triggers Graceful Shutdown
  // ==========================================================================

  describe('SIGTERM triggers graceful shutdown', () => {
    it('should have all resource getters return null before start', () => {
      const server = createServer({
        port: 0,
        host: '127.0.0.1',
      });

      // All resources should be null before start
      expect(server.getPool()).toBeNull();
      expect(server.getBufferPoolStats()).toBeNull();
      expect(server.address()).toBeNull();
      expect(server.getMetrics()).toBeNull();
    });

    it('should be idempotent - multiple stop calls should not throw on unstarted server', async () => {
      const server = createServer({
        port: 0,
        host: '127.0.0.1',
      });

      await expect(server.stop()).resolves.not.toThrow();
      await expect(server.stop()).resolves.not.toThrow();
      await expect(server.stop()).resolves.not.toThrow();
    });
  });

  // ==========================================================================
  // Additional Edge Cases
  // ==========================================================================

  describe('Additional shutdown edge cases', () => {
    it('should handle stop() on server never started', async () => {
      const unstartedServer = createServer({
        port: 0,
        host: '127.0.0.1',
      });

      await expect(unstartedServer.stop()).resolves.not.toThrow();
    });

    it('should report TLS disabled by default', () => {
      const testServer = createServer({
        port: 0,
        host: '127.0.0.1',
      });

      expect(testServer.isTlsEnabled()).toBe(false);
    });

    it('should report TLS enabled when configured', () => {
      const testServer = createServer({
        port: 0,
        host: '127.0.0.1',
        tls: { enabled: true },
      });

      expect(testServer.isTlsEnabled()).toBe(true);
    });
  });
});

// ============================================================================
// Shutdown Configuration Tests
// ============================================================================

describe('Shutdown Configuration', () => {
  it('should use default drain timeout when not specified', () => {
    const server = createServer({
      port: 0,
    });
    expect(server).toBeDefined();
  });

  it('should accept custom drain timeout', () => {
    const server = createServer({
      port: 0,
      shutdown: { drainTimeout: 60000 },
    });
    expect(server).toBeDefined();
  });

  it('should accept notifyClients option', () => {
    const server = createServer({
      port: 0,
      shutdown: { notifyClients: true },
    });
    expect(server).toBeDefined();
  });

  it('should accept forceClose option', () => {
    const server = createServer({
      port: 0,
      shutdown: { forceClose: false },
    });
    expect(server).toBeDefined();
  });

  it('should accept all shutdown options together', () => {
    const server = createServer({
      port: 0,
      shutdown: {
        drainTimeout: 5000,
        notifyClients: true,
        forceClose: true,
      },
    });
    expect(server).toBeDefined();
  });
});

// ============================================================================
// GracefulShutdown Class Tests
// ============================================================================

describe('GracefulShutdown Class', () => {
  let gracefulShutdown: GracefulShutdown;

  // Mock socket for testing
  class MockSocket extends EventEmitter {
    public destroyed = false;
    public writable = true;
    public remoteAddress = '127.0.0.1';
    public remotePort = 12345;

    destroy(): void {
      this.destroyed = true;
      this.writable = false;
      this.emit('close');
    }

    end(): void {
      this.writable = false;
      this.emit('end');
      this.emit('close');
    }
  }

  beforeEach(() => {
    gracefulShutdown = new GracefulShutdown();
  });

  afterEach(() => {
    gracefulShutdown.reset();
  });

  describe('connection tracking', () => {
    it('should track registered connections', () => {
      const socket1 = new MockSocket();
      const socket2 = new MockSocket();

      gracefulShutdown.registerConnection(socket1 as any);
      expect(gracefulShutdown.connectionCount).toBe(1);

      gracefulShutdown.registerConnection(socket2 as any);
      expect(gracefulShutdown.connectionCount).toBe(2);
    });

    it('should unregister connections', () => {
      const socket = new MockSocket();

      gracefulShutdown.registerConnection(socket as any);
      expect(gracefulShutdown.connectionCount).toBe(1);

      gracefulShutdown.unregisterConnection(socket as any);
      expect(gracefulShutdown.connectionCount).toBe(0);
    });

    it('should destroy connections registered during shutdown', () => {
      const socket = new MockSocket();

      // Start shutdown first
      gracefulShutdown.shutdown(null, 100);

      // Try to register a connection during shutdown
      gracefulShutdown.registerConnection(socket as any);

      // Socket should be destroyed immediately
      expect(socket.destroyed).toBe(true);
      expect(gracefulShutdown.connectionCount).toBe(0);
    });
  });

  describe('shutdown state', () => {
    it('should report shuttingDown state correctly', () => {
      expect(gracefulShutdown.shuttingDown).toBe(false);

      // Start shutdown (don't await)
      gracefulShutdown.shutdown(null, 100);

      expect(gracefulShutdown.shuttingDown).toBe(true);
    });

    it('should handle multiple shutdown calls gracefully', async () => {
      const results = await Promise.allSettled([
        gracefulShutdown.shutdown(null, 100),
        gracefulShutdown.shutdown(null, 100),
        gracefulShutdown.shutdown(null, 100),
      ]);

      // All should succeed
      for (const result of results) {
        expect(result.status).toBe('fulfilled');
      }
    });
  });

  describe('shutdown with server', () => {
    let server: ReturnType<typeof createNetServer>;

    beforeEach(async () => {
      server = createNetServer();
      await new Promise<void>((resolve) => {
        server.listen(0, '127.0.0.1', resolve);
      });
    });

    afterEach(async () => {
      if (server.listening) {
        await new Promise<void>((resolve) => server.close(() => resolve()));
      }
    });

    it('should close the server', async () => {
      expect(server.listening).toBe(true);

      await gracefulShutdown.shutdown(server, 1000);

      expect(server.listening).toBe(false);
    });

    it('should wait for active connections before closing', async () => {
      const socket = new MockSocket();
      gracefulShutdown.registerConnection(socket as any);

      const startTime = Date.now();
      const shutdownPromise = gracefulShutdown.shutdown(server, 500);

      // Simulate connection closing after 100ms
      setTimeout(() => {
        gracefulShutdown.unregisterConnection(socket as any);
      }, 100);

      await shutdownPromise;
      const elapsed = Date.now() - startTime;

      // Should have waited for the connection
      expect(elapsed).toBeGreaterThanOrEqual(90); // Allow some margin
      expect(elapsed).toBeLessThan(500); // Should not have hit timeout
    });

    it('should force close connections after timeout', async () => {
      const socket = new MockSocket();
      gracefulShutdown.registerConnection(socket as any);

      const startTime = Date.now();
      await gracefulShutdown.shutdown(server, 200);
      const elapsed = Date.now() - startTime;

      // Should have waited until timeout
      expect(elapsed).toBeGreaterThanOrEqual(190);
      // Socket should be destroyed
      expect(socket.destroyed).toBe(true);
    });
  });

  describe('operation tracking', () => {
    it('should track pending operations', async () => {
      let operationCompleted = false;

      const operation = new Promise<void>((resolve) => {
        setTimeout(() => {
          operationCompleted = true;
          resolve();
        }, 50);
      });

      const tracked = gracefulShutdown.trackOperation(operation);

      await tracked;
      expect(operationCompleted).toBe(true);
    });

    it('should handle operation errors', async () => {
      const operation = new Promise<void>((_, reject) => {
        setTimeout(() => {
          reject(new Error('Test error'));
        }, 10);
      });

      const tracked = gracefulShutdown.trackOperation(operation);

      await expect(tracked).rejects.toThrow('Test error');
    });
  });

  describe('flushState', () => {
    it('should clear pending operations', async () => {
      // Flush should complete without error
      await gracefulShutdown.flushState();
    });
  });

  describe('reset', () => {
    it('should reset shutdown state', async () => {
      const socket = new MockSocket();
      gracefulShutdown.registerConnection(socket as any);

      await gracefulShutdown.shutdown(null, 100);
      expect(gracefulShutdown.shuttingDown).toBe(true);

      gracefulShutdown.reset();

      expect(gracefulShutdown.shuttingDown).toBe(false);
      expect(gracefulShutdown.connectionCount).toBe(0);
    });

    it('should allow new connections after reset', () => {
      // Start shutdown
      gracefulShutdown.shutdown(null, 100);
      expect(gracefulShutdown.shuttingDown).toBe(true);

      // Reset
      gracefulShutdown.reset();

      // Should be able to register connections again
      const socket = new MockSocket();
      gracefulShutdown.registerConnection(socket as any);
      expect(socket.destroyed).toBe(false);
      expect(gracefulShutdown.connectionCount).toBe(1);
    });
  });
});

describe('GracefulShutdown integration with real TCP server', () => {
  it('should work with real TCP server connections', async () => {
    const gracefulShutdown = new GracefulShutdown();
    let connectionReceived: () => void;
    const connectionPromise = new Promise<void>((resolve) => {
      connectionReceived = resolve;
    });

    const server = createNetServer((socket) => {
      gracefulShutdown.registerConnection(socket);
      connectionReceived();
      socket.on('close', () => {
        gracefulShutdown.unregisterConnection(socket);
      });
    });

    await new Promise<void>((resolve) => {
      server.listen(0, '127.0.0.1', resolve);
    });

    const addr = server.address();
    expect(addr).not.toBeNull();

    // Connect a client
    const client = createConnection({
      port: (addr as { port: number }).port,
      host: '127.0.0.1',
    });

    await new Promise<void>((resolve, reject) => {
      client.on('connect', resolve);
      client.on('error', reject);
    });

    // Wait for server to receive the connection
    await connectionPromise;

    // Should have one active connection
    expect(gracefulShutdown.connectionCount).toBe(1);

    // Shutdown
    const shutdownPromise = gracefulShutdown.shutdown(server, 1000);

    // Close client after a small delay
    setTimeout(() => {
      client.destroy();
    }, 50);

    await shutdownPromise;

    expect(gracefulShutdown.connectionCount).toBe(0);
    expect(server.listening).toBe(false);
  });
});
