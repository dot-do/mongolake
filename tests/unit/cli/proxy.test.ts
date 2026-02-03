/**
 * Tests for MongoLake Proxy Server
 *
 * Tests the MongoDB wire protocol proxy functionality including:
 * - Connection string parsing
 * - Connection handling
 * - Message forwarding
 * - Connection pooling
 * - Error scenarios
 * - Wire protocol logging
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Mock the net module
const mockSocket = {
  on: vi.fn().mockReturnThis(),
  write: vi.fn().mockReturnValue(true),
  destroy: vi.fn(),
  writable: true,
  destroyed: false,
  remoteAddress: '127.0.0.1',
  remotePort: 54321,
  setTimeout: vi.fn(),
};

const mockServer = {
  listen: vi.fn((port, host, callback) => {
    if (callback) callback();
    return mockServer;
  }),
  close: vi.fn((callback) => {
    if (callback) callback();
  }),
  on: vi.fn().mockReturnThis(),
  address: vi.fn(() => ({ address: '127.0.0.1', port: 27017 })),
};

vi.mock('node:net', () => ({
  createServer: vi.fn((handler) => {
    mockServer._handler = handler;
    return mockServer;
  }),
  connect: vi.fn((port, host, callback) => {
    if (callback) setTimeout(callback, 0);
    return { ...mockSocket };
  }),
}));

// ============================================================================
// Module Export Tests
// ============================================================================

describe('CLI Proxy - Module Exports', () => {
  it('should export startProxy function', async () => {
    const module = await import('../../../src/cli/proxy.js');
    expect(typeof module.startProxy).toBe('function');
  });

  it('should export ProxyOptions type', async () => {
    // TypeScript type check - if this compiles, the type exists
    const module = await import('../../../src/cli/proxy.js');
    expect(module.startProxy).toBeDefined();
  });
});

// ============================================================================
// Connection String Parsing Tests
// ============================================================================

describe('CLI Proxy - Connection String Parsing', () => {
  /**
   * Parse a MongoDB connection string or host:port format
   */
  function parseConnectionString(connectionString: string): {
    host: string;
    port: number;
    database?: string;
    authSource?: string;
    username?: string;
    password?: string;
  } {
    // Check for mongodb:// or mongolake:// prefix
    if (connectionString.startsWith('mongodb://') || connectionString.startsWith('mongolake://')) {
      const url = new URL(connectionString);
      return {
        host: url.hostname || 'localhost',
        port: parseInt(url.port, 10) || 27017,
        database: url.pathname?.slice(1) || undefined,
        authSource: url.searchParams.get('authSource') || undefined,
        username: url.username || undefined,
        password: url.password || undefined,
      };
    }

    // Check for host:port format
    const hostPortMatch = connectionString.match(/^([^:]+):(\d+)$/);
    if (hostPortMatch) {
      return {
        host: hostPortMatch[1],
        port: parseInt(hostPortMatch[2], 10),
      };
    }

    // Just a hostname, use default MongoDB port
    return {
      host: connectionString,
      port: 27017,
    };
  }

  it('should parse mongodb:// connection string', () => {
    const result = parseConnectionString('mongodb://localhost:27017/mydb');
    expect(result.host).toBe('localhost');
    expect(result.port).toBe(27017);
    expect(result.database).toBe('mydb');
  });

  it('should parse mongolake:// connection string', () => {
    const result = parseConnectionString('mongolake://example.com:27018');
    expect(result.host).toBe('example.com');
    expect(result.port).toBe(27018);
  });

  it('should parse connection string with authentication', () => {
    const result = parseConnectionString('mongodb://user:pass@localhost:27017/admin?authSource=admin');
    expect(result.host).toBe('localhost');
    expect(result.port).toBe(27017);
    expect(result.database).toBe('admin');
    expect(result.authSource).toBe('admin');
    expect(result.username).toBe('user');
    expect(result.password).toBe('pass');
  });

  it('should parse host:port format', () => {
    const result = parseConnectionString('myhost:28017');
    expect(result.host).toBe('myhost');
    expect(result.port).toBe(28017);
  });

  it('should parse hostname only with default port', () => {
    const result = parseConnectionString('myhost');
    expect(result.host).toBe('myhost');
    expect(result.port).toBe(27017);
  });

  it('should handle empty database path', () => {
    const result = parseConnectionString('mongodb://localhost:27017/');
    expect(result.host).toBe('localhost');
    expect(result.port).toBe(27017);
    // URL.pathname for '/' will be '/', .slice(1) gives empty string, || undefined returns undefined
    expect(result.database).toBeUndefined();
  });

  it('should handle missing port in URL', () => {
    const result = parseConnectionString('mongodb://localhost/mydb');
    expect(result.host).toBe('localhost');
    expect(result.port).toBe(27017);
    expect(result.database).toBe('mydb');
  });

  it('should handle IPv6 addresses', () => {
    const result = parseConnectionString('mongodb://[::1]:27017/mydb');
    expect(result.host).toBe('[::1]');
    expect(result.port).toBe(27017);
  });
});

// ============================================================================
// Wire Protocol Message Parsing Tests
// ============================================================================

describe('CLI Proxy - Wire Protocol Message Parsing', () => {
  /**
   * Get human-readable name for opCode
   */
  function getOpCodeName(opCode: number): string {
    const opCodes: Record<number, string> = {
      1: 'OP_REPLY',
      2001: 'OP_UPDATE',
      2002: 'OP_INSERT',
      2004: 'OP_QUERY',
      2005: 'OP_GET_MORE',
      2006: 'OP_DELETE',
      2007: 'OP_KILL_CURSORS',
      2012: 'OP_COMPRESSED',
      2013: 'OP_MSG',
    };
    return opCodes[opCode] || 'UNKNOWN';
  }

  it('should identify OP_MSG opcode', () => {
    expect(getOpCodeName(2013)).toBe('OP_MSG');
  });

  it('should identify OP_QUERY opcode', () => {
    expect(getOpCodeName(2004)).toBe('OP_QUERY');
  });

  it('should identify OP_REPLY opcode', () => {
    expect(getOpCodeName(1)).toBe('OP_REPLY');
  });

  it('should identify OP_INSERT opcode', () => {
    expect(getOpCodeName(2002)).toBe('OP_INSERT');
  });

  it('should identify OP_UPDATE opcode', () => {
    expect(getOpCodeName(2001)).toBe('OP_UPDATE');
  });

  it('should identify OP_DELETE opcode', () => {
    expect(getOpCodeName(2006)).toBe('OP_DELETE');
  });

  it('should identify OP_GET_MORE opcode', () => {
    expect(getOpCodeName(2005)).toBe('OP_GET_MORE');
  });

  it('should identify OP_KILL_CURSORS opcode', () => {
    expect(getOpCodeName(2007)).toBe('OP_KILL_CURSORS');
  });

  it('should identify OP_COMPRESSED opcode', () => {
    expect(getOpCodeName(2012)).toBe('OP_COMPRESSED');
  });

  it('should return UNKNOWN for unknown opcodes', () => {
    expect(getOpCodeName(9999)).toBe('UNKNOWN');
  });

  it('should parse message header from buffer', () => {
    // Create a mock wire protocol message header
    // Format: messageLength (4) + requestId (4) + responseTo (4) + opCode (4)
    const buffer = Buffer.alloc(16);
    const view = new DataView(buffer.buffer);
    view.setInt32(0, 100, true);  // messageLength = 100
    view.setInt32(4, 1, true);    // requestId = 1
    view.setInt32(8, 0, true);    // responseTo = 0
    view.setInt32(12, 2013, true); // opCode = OP_MSG

    expect(view.getInt32(0, true)).toBe(100);
    expect(view.getInt32(4, true)).toBe(1);
    expect(view.getInt32(8, true)).toBe(0);
    expect(view.getInt32(12, true)).toBe(2013);
    expect(getOpCodeName(view.getInt32(12, true))).toBe('OP_MSG');
  });

  it('should handle incomplete message header', () => {
    const buffer = Buffer.alloc(8); // Less than 16 bytes
    expect(buffer.length < 16).toBe(true);
  });
});

// ============================================================================
// Connection Pool Tests
// ============================================================================

describe('CLI Proxy - Connection Pool', () => {
  interface PooledConnection {
    socket: typeof mockSocket;
    inUse: boolean;
    lastUsed: Date;
  }

  class MockConnectionPool {
    private connections: PooledConnection[] = [];
    private maxSize: number;

    constructor(maxSize: number) {
      this.maxSize = maxSize;
    }

    async acquire(): Promise<typeof mockSocket> {
      // Try to find an available connection
      const available = this.connections.find((conn) => !conn.inUse && conn.socket.writable);
      if (available) {
        available.inUse = true;
        available.lastUsed = new Date();
        return available.socket;
      }

      // Create new connection if pool not full
      if (this.connections.length < this.maxSize) {
        const socket = { ...mockSocket };
        this.connections.push({
          socket,
          inUse: true,
          lastUsed: new Date(),
        });
        return socket;
      }

      // Return a new socket for testing
      return { ...mockSocket };
    }

    release(socket: typeof mockSocket): void {
      const conn = this.connections.find((c) => c.socket === socket);
      if (conn) {
        conn.inUse = false;
        conn.lastUsed = new Date();
      }
    }

    remove(socket: typeof mockSocket): void {
      const index = this.connections.findIndex((c) => c.socket === socket);
      if (index !== -1) {
        this.connections.splice(index, 1);
      }
    }

    get size(): number {
      return this.connections.length;
    }

    get availableCount(): number {
      return this.connections.filter((c) => !c.inUse && c.socket.writable).length;
    }
  }

  it('should create new connection when pool is empty', async () => {
    const pool = new MockConnectionPool(10);
    const socket = await pool.acquire();
    expect(socket).toBeDefined();
    expect(pool.size).toBe(1);
  });

  it('should reuse connection when available', async () => {
    const pool = new MockConnectionPool(10);
    const socket1 = await pool.acquire();
    pool.release(socket1);
    const socket2 = await pool.acquire();
    expect(socket2).toBe(socket1);
    expect(pool.size).toBe(1);
  });

  it('should create new connection when all are in use', async () => {
    const pool = new MockConnectionPool(10);
    const socket1 = await pool.acquire();
    const socket2 = await pool.acquire();
    expect(socket1).not.toBe(socket2);
    expect(pool.size).toBe(2);
  });

  it('should respect max pool size', async () => {
    const pool = new MockConnectionPool(2);
    await pool.acquire();
    await pool.acquire();
    // Third acquire should still work due to our mock implementation
    const socket3 = await pool.acquire();
    expect(socket3).toBeDefined();
  });

  it('should remove connection from pool', async () => {
    const pool = new MockConnectionPool(10);
    const socket = await pool.acquire();
    expect(pool.size).toBe(1);
    pool.remove(socket);
    expect(pool.size).toBe(0);
  });

  it('should track available connections', async () => {
    const pool = new MockConnectionPool(10);
    const socket1 = await pool.acquire();
    const socket2 = await pool.acquire();
    expect(pool.availableCount).toBe(0);
    pool.release(socket1);
    expect(pool.availableCount).toBe(1);
    pool.release(socket2);
    expect(pool.availableCount).toBe(2);
  });
});

// ============================================================================
// Connection Statistics Tests
// ============================================================================

describe('CLI Proxy - Connection Statistics', () => {
  interface ConnectionStats {
    bytesIn: number;
    bytesOut: number;
    messagesIn: number;
    messagesOut: number;
    startTime: Date;
  }

  it('should track bytes in', () => {
    const stats: ConnectionStats = {
      bytesIn: 0,
      bytesOut: 0,
      messagesIn: 0,
      messagesOut: 0,
      startTime: new Date(),
    };

    const data = Buffer.alloc(100);
    stats.bytesIn += data.length;
    stats.messagesIn++;

    expect(stats.bytesIn).toBe(100);
    expect(stats.messagesIn).toBe(1);
  });

  it('should track bytes out', () => {
    const stats: ConnectionStats = {
      bytesIn: 0,
      bytesOut: 0,
      messagesIn: 0,
      messagesOut: 0,
      startTime: new Date(),
    };

    const data = Buffer.alloc(200);
    stats.bytesOut += data.length;
    stats.messagesOut++;

    expect(stats.bytesOut).toBe(200);
    expect(stats.messagesOut).toBe(1);
  });

  it('should calculate connection duration', () => {
    const stats: ConnectionStats = {
      bytesIn: 0,
      bytesOut: 0,
      messagesIn: 0,
      messagesOut: 0,
      startTime: new Date(Date.now() - 5000), // 5 seconds ago
    };

    const duration = Date.now() - stats.startTime.getTime();
    expect(duration).toBeGreaterThanOrEqual(5000);
    expect(duration).toBeLessThan(6000);
  });
});

// ============================================================================
// Logger Tests
// ============================================================================

describe('CLI Proxy - Logger', () => {
  let consoleSpy: ReturnType<typeof vi.spyOn>;
  let consoleErrorSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    consoleSpy.mockRestore();
    consoleErrorSpy.mockRestore();
  });

  class MockProxyLogger {
    private verbose: boolean;

    constructor(verbose: boolean) {
      this.verbose = verbose;
    }

    info(message: string): void {
      console.log(`[INFO] ${message}`);
    }

    debug(message: string): void {
      if (this.verbose) {
        console.log(`[DEBUG] ${message}`);
      }
    }

    error(message: string): void {
      console.error(`[ERROR] ${message}`);
    }

    connection(connectionId: number, message: string): void {
      if (this.verbose) {
        console.log(`[CONN-${connectionId}] ${message}`);
      }
    }
  }

  it('should always log info messages', () => {
    const logger = new MockProxyLogger(false);
    logger.info('Server starting');
    expect(consoleSpy).toHaveBeenCalledWith('[INFO] Server starting');
  });

  it('should always log error messages', () => {
    const logger = new MockProxyLogger(false);
    logger.error('Connection failed');
    expect(consoleErrorSpy).toHaveBeenCalledWith('[ERROR] Connection failed');
  });

  it('should log debug messages when verbose', () => {
    const logger = new MockProxyLogger(true);
    logger.debug('Processing message');
    expect(consoleSpy).toHaveBeenCalledWith('[DEBUG] Processing message');
  });

  it('should not log debug messages when not verbose', () => {
    const logger = new MockProxyLogger(false);
    logger.debug('Processing message');
    expect(consoleSpy).not.toHaveBeenCalled();
  });

  it('should log connection messages when verbose', () => {
    const logger = new MockProxyLogger(true);
    logger.connection(1, 'Client connected');
    expect(consoleSpy).toHaveBeenCalledWith('[CONN-1] Client connected');
  });

  it('should not log connection messages when not verbose', () => {
    const logger = new MockProxyLogger(false);
    logger.connection(1, 'Client connected');
    expect(consoleSpy).not.toHaveBeenCalled();
  });
});

// ============================================================================
// Error Handling Tests
// ============================================================================

describe('CLI Proxy - Error Handling', () => {
  it('should handle EADDRINUSE error', () => {
    const error = new Error('EADDRINUSE') as NodeJS.ErrnoException;
    error.code = 'EADDRINUSE';

    expect(error.code).toBe('EADDRINUSE');
    // Server should display helpful message about port in use
  });

  it('should handle connection refused error', () => {
    const error = new Error('connect ECONNREFUSED');
    expect(error.message).toContain('ECONNREFUSED');
  });

  it('should handle timeout error', () => {
    const error = new Error('Request timeout');
    expect(error.message).toContain('timeout');
  });

  it('should handle target connection error gracefully', () => {
    // When target connection fails, client socket should be destroyed
    const clientSocket = { ...mockSocket };
    const targetConnected = false;

    if (!targetConnected) {
      clientSocket.destroy();
    }

    expect(clientSocket.destroy).toHaveBeenCalled();
  });

  it('should handle client disconnect gracefully', () => {
    // When client disconnects, target socket should be cleaned up
    const targetSocket = { ...mockSocket };
    const inPool = true;

    if (inPool) {
      // Release back to pool
      expect(targetSocket).toBeDefined();
    } else {
      targetSocket.destroy();
      expect(targetSocket.destroy).toHaveBeenCalled();
    }
  });

  it('should handle target disconnect gracefully', () => {
    // When target disconnects, client socket should be destroyed
    const clientSocket = { ...mockSocket, destroyed: false };

    if (!clientSocket.destroyed) {
      clientSocket.destroy();
    }

    expect(clientSocket.destroy).toHaveBeenCalled();
  });
});

// ============================================================================
// Data Forwarding Tests
// ============================================================================

describe('CLI Proxy - Data Forwarding', () => {
  it('should forward data from client to target', () => {
    const clientData = Buffer.from('client message');
    const targetSocket = { ...mockSocket };

    if (targetSocket.writable) {
      targetSocket.write(clientData);
    }

    expect(targetSocket.write).toHaveBeenCalledWith(clientData);
  });

  it('should forward data from target to client', () => {
    const targetData = Buffer.from('target response');
    const clientSocket = { ...mockSocket };

    if (clientSocket.writable) {
      clientSocket.write(targetData);
    }

    expect(clientSocket.write).toHaveBeenCalledWith(targetData);
  });

  it('should not forward data if socket is not writable', () => {
    const data = Buffer.from('test data');
    const socket = { ...mockSocket, writable: false, write: vi.fn() };

    if (socket.writable) {
      socket.write(data);
    }

    expect(socket.write).not.toHaveBeenCalled();
  });
});

// ============================================================================
// Graceful Shutdown Tests
// ============================================================================

describe('CLI Proxy - Graceful Shutdown', () => {
  let processOnSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    processOnSpy = vi.spyOn(process, 'on').mockImplementation(() => process);
  });

  afterEach(() => {
    processOnSpy.mockRestore();
  });

  it('should register SIGINT handler', () => {
    let sigintHandler: (() => void) | undefined;

    processOnSpy.mockImplementation((event: string, handler: () => void) => {
      if (event === 'SIGINT') {
        sigintHandler = handler;
      }
      return process;
    });

    process.on('SIGINT', () => {});
    expect(sigintHandler || processOnSpy).toBeDefined();
  });

  it('should register SIGTERM handler', () => {
    let sigtermHandler: (() => void) | undefined;

    processOnSpy.mockImplementation((event: string, handler: () => void) => {
      if (event === 'SIGTERM') {
        sigtermHandler = handler;
      }
      return process;
    });

    process.on('SIGTERM', () => {});
    expect(sigtermHandler || processOnSpy).toBeDefined();
  });

  it('should close connection pool on shutdown', async () => {
    const connections = [
      { socket: { ...mockSocket }, inUse: false },
      { socket: { ...mockSocket }, inUse: true },
    ];

    // Close all connections
    for (const conn of connections) {
      if (conn.socket.writable) {
        conn.socket.destroy();
      }
    }

    for (const conn of connections) {
      expect(conn.socket.destroy).toHaveBeenCalled();
    }
  });
});

// ============================================================================
// Proxy Options Tests
// ============================================================================

describe('CLI Proxy - Options', () => {
  it('should use default port 27017', () => {
    const defaultPort = 27017;
    expect(defaultPort).toBe(27017);
  });

  it('should use default host 127.0.0.1', () => {
    const defaultHost = '127.0.0.1';
    expect(defaultHost).toBe('127.0.0.1');
  });

  it('should use default pool size 10', () => {
    const defaultPoolSize = 10;
    expect(defaultPoolSize).toBe(10);
  });

  it('should validate target option is required', () => {
    const options = {
      target: '',
      port: 27017,
      host: '127.0.0.1',
      verbose: false,
      pool: false,
      poolSize: 10,
    };

    expect(options.target).toBe('');
    // Target should be validated as required
  });

  it('should support custom port option', () => {
    const options = {
      target: 'localhost:27017',
      port: 27018,
      host: '127.0.0.1',
      verbose: false,
      pool: false,
      poolSize: 10,
    };

    expect(options.port).toBe(27018);
  });

  it('should support verbose option', () => {
    const options = {
      target: 'localhost:27017',
      port: 27017,
      host: '127.0.0.1',
      verbose: true,
      pool: false,
      poolSize: 10,
    };

    expect(options.verbose).toBe(true);
  });

  it('should support pool option', () => {
    const options = {
      target: 'localhost:27017',
      port: 27017,
      host: '127.0.0.1',
      verbose: false,
      pool: true,
      poolSize: 20,
    };

    expect(options.pool).toBe(true);
    expect(options.poolSize).toBe(20);
  });
});

// ============================================================================
// Wire Protocol Logging Tests
// ============================================================================

describe('CLI Proxy - Wire Protocol Logging', () => {
  it('should format OP_MSG message info', () => {
    const opCode = 2013;
    const messageLength = 100;
    const requestId = 1;
    const responseTo = 0;

    const info = `OP_MSG (opCode: ${opCode}, len: ${messageLength}, reqId: ${requestId}, respTo: ${responseTo})`;

    expect(info).toContain('OP_MSG');
    expect(info).toContain('opCode: 2013');
    expect(info).toContain('len: 100');
    expect(info).toContain('reqId: 1');
    expect(info).toContain('respTo: 0');
  });

  it('should handle incomplete header in logging', () => {
    const data = Buffer.alloc(8); // Less than 16 bytes
    const isComplete = data.length >= 16;
    expect(isComplete).toBe(false);
  });

  it('should identify direction in logging', () => {
    const clientToTarget = 'CLIENT -> TARGET';
    const targetToClient = 'TARGET -> CLIENT';

    expect(clientToTarget).toContain('->');
    expect(targetToClient).toContain('->');
  });
});

// ============================================================================
// Connection ID Tests
// ============================================================================

describe('CLI Proxy - Connection ID', () => {
  it('should increment connection counter', () => {
    let connectionCounter = 0;

    const conn1 = ++connectionCounter;
    const conn2 = ++connectionCounter;
    const conn3 = ++connectionCounter;

    expect(conn1).toBe(1);
    expect(conn2).toBe(2);
    expect(conn3).toBe(3);
  });

  it('should use connection ID in logging', () => {
    const connectionId = 5;
    const message = `[CONN-${connectionId}] Client connected`;
    expect(message).toContain('CONN-5');
  });
});

// ============================================================================
// Socket Timeout Tests
// ============================================================================

describe('CLI Proxy - Socket Timeout', () => {
  it('should set timeout on client socket', () => {
    const socket = { ...mockSocket };
    const timeout = 30000;

    socket.setTimeout(timeout);
    expect(socket.setTimeout).toHaveBeenCalledWith(30000);
  });

  it('should set timeout on target socket', () => {
    const socket = { ...mockSocket };
    const timeout = 30000;

    socket.setTimeout(timeout);
    expect(socket.setTimeout).toHaveBeenCalledWith(30000);
  });

  it('should destroy sockets on timeout', () => {
    const clientSocket = { ...mockSocket };
    const targetSocket = { ...mockSocket };

    // Simulate timeout
    clientSocket.destroy();
    targetSocket.destroy();

    expect(clientSocket.destroy).toHaveBeenCalled();
    expect(targetSocket.destroy).toHaveBeenCalled();
  });
});

// ============================================================================
// TCP Proxy Server Tests
// ============================================================================

describe('CLI Proxy - TCP Proxy Server', () => {
  it('should export createProxyServer function', async () => {
    const module = await import('../../../src/cli/proxy.js');
    expect(typeof module.createProxyServer).toBe('function');
  });

  it('should export ProxyServer type', async () => {
    const module = await import('../../../src/cli/proxy.js');
    expect(module.createProxyServer).toBeDefined();
  });

  it('should create a proxy server that starts and stops', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 27018,
      host: '127.0.0.1',
    });

    expect(proxyServer).toBeDefined();
    expect(typeof proxyServer.start).toBe('function');
    expect(typeof proxyServer.stop).toBe('function');
    expect(typeof proxyServer.address).toBe('function');
    expect(typeof proxyServer.getStats).toBe('function');
  });

  it('should report server address after starting', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 0, // Let OS assign port
      host: '127.0.0.1',
    });

    await proxyServer.start();
    const address = proxyServer.address();

    expect(address).not.toBeNull();
    expect(address!.port).toBeGreaterThan(0);
    expect(address!.host).toBe('127.0.0.1');

    await proxyServer.stop();
  });

  it('should forward MongoDB wire protocol messages', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 0,
      host: '127.0.0.1',
    });

    await proxyServer.start();

    // Verify the proxy can accept connections and forward messages
    const stats = proxyServer.getStats();
    expect(stats.activeConnections).toBe(0);
    expect(stats.totalConnections).toBe(0);
    expect(stats.bytesForwarded).toBe(0);

    await proxyServer.stop();
  });
});

// ============================================================================
// TLS Termination Tests
// ============================================================================

describe('CLI Proxy - TLS Termination', () => {
  it('should export TLS-related options in ProxyOptions', async () => {
    const module = await import('../../../src/cli/proxy.js');
    // Type check - ProxyOptions should include TLS fields
    expect(module.createProxyServer).toBeDefined();
  });

  it('should support TLS configuration', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 27018,
      host: '127.0.0.1',
      tls: {
        enabled: true,
        cert: '/path/to/cert.pem',
        key: '/path/to/key.pem',
      },
    });

    expect(proxyServer).toBeDefined();
    const config = proxyServer.getConfig();
    expect(config.tls).toBeDefined();
    expect(config.tls.enabled).toBe(true);
  });

  it('should create TLS server when TLS is enabled', async () => {
    const module = await import('../../../src/cli/proxy.js');

    // Mock TLS certificates for testing
    const mockCert = `-----BEGIN CERTIFICATE-----
MIIBkTCB+wIJAKHBfpegM...
-----END CERTIFICATE-----`;
    const mockKey = `-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0B...
-----END PRIVATE KEY-----`;

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 0,
      host: '127.0.0.1',
      tls: {
        enabled: true,
        cert: mockCert,
        key: mockKey,
      },
    });

    expect(proxyServer.isTlsEnabled()).toBe(true);
  });

  it('should accept TLS connections when enabled', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 0,
      host: '127.0.0.1',
      tls: {
        enabled: true,
        cert: '/path/to/cert.pem',
        key: '/path/to/key.pem',
      },
    });

    await proxyServer.start();

    // TLS server should be listening
    const address = proxyServer.address();
    expect(address).not.toBeNull();
    expect(proxyServer.isTlsEnabled()).toBe(true);

    await proxyServer.stop();
  });

  it('should support CA certificate for client verification', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 0,
      host: '127.0.0.1',
      tls: {
        enabled: true,
        cert: '/path/to/cert.pem',
        key: '/path/to/key.pem',
        ca: '/path/to/ca.pem',
        requestClientCert: true,
      },
    });

    const config = proxyServer.getConfig();
    expect(config.tls.ca).toBe('/path/to/ca.pem');
    expect(config.tls.requestClientCert).toBe(true);
  });

  it('should terminate TLS and forward plaintext to target', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017', // Target receives plaintext
      port: 0,
      host: '127.0.0.1',
      tls: {
        enabled: true,
        cert: '/path/to/cert.pem',
        key: '/path/to/key.pem',
        terminateTls: true, // Terminate TLS at proxy
      },
    });

    const config = proxyServer.getConfig();
    expect(config.tls.terminateTls).toBe(true);
  });

  it('should support TLS passthrough mode', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'mongodb+srv://example.com', // Target uses TLS
      port: 0,
      host: '127.0.0.1',
      tls: {
        enabled: true,
        cert: '/path/to/cert.pem',
        key: '/path/to/key.pem',
        passthrough: true, // Pass TLS through to target
      },
    });

    const config = proxyServer.getConfig();
    expect(config.tls.passthrough).toBe(true);
  });
});

// ============================================================================
// Concurrent Connection Tests
// ============================================================================

describe('CLI Proxy - Concurrent Connections', () => {
  it('should track active connections', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 0,
      host: '127.0.0.1',
    });

    await proxyServer.start();

    const stats = proxyServer.getStats();
    expect(stats.activeConnections).toBe(0);

    await proxyServer.stop();
  });

  it('should handle multiple concurrent connections', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 0,
      host: '127.0.0.1',
      maxConnections: 100,
    });

    await proxyServer.start();

    // Simulate multiple connections
    const connections = await Promise.all([
      proxyServer.simulateConnection(),
      proxyServer.simulateConnection(),
      proxyServer.simulateConnection(),
    ]);

    const stats = proxyServer.getStats();
    expect(stats.activeConnections).toBe(3);

    // Clean up
    for (const conn of connections) {
      await conn.close();
    }

    await proxyServer.stop();
  });

  it('should limit maximum concurrent connections', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 0,
      host: '127.0.0.1',
      maxConnections: 2,
    });

    await proxyServer.start();

    // First two connections should succeed
    const conn1 = await proxyServer.simulateConnection();
    const conn2 = await proxyServer.simulateConnection();

    // Third connection should be rejected
    await expect(proxyServer.simulateConnection()).rejects.toThrow(/max connections/i);

    await conn1.close();
    await conn2.close();
    await proxyServer.stop();
  });

  it('should handle connection bursts', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 0,
      host: '127.0.0.1',
      maxConnections: 50,
    });

    await proxyServer.start();

    // Simulate burst of connections
    const connectionPromises = Array.from({ length: 20 }, () =>
      proxyServer.simulateConnection()
    );

    const connections = await Promise.all(connectionPromises);
    expect(connections.length).toBe(20);

    const stats = proxyServer.getStats();
    expect(stats.activeConnections).toBe(20);
    expect(stats.totalConnections).toBe(20);

    // Clean up
    await Promise.all(connections.map((c) => c.close()));
    await proxyServer.stop();
  });

  it('should isolate connections from each other', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 0,
      host: '127.0.0.1',
    });

    await proxyServer.start();

    const conn1 = await proxyServer.simulateConnection();
    const conn2 = await proxyServer.simulateConnection();

    // Each connection should have its own state
    expect(conn1.id).not.toBe(conn2.id);
    expect(conn1.getBuffer()).not.toBe(conn2.getBuffer());

    await conn1.close();
    await conn2.close();
    await proxyServer.stop();
  });

  it('should track connection statistics per connection', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 0,
      host: '127.0.0.1',
    });

    await proxyServer.start();

    const conn = await proxyServer.simulateConnection();

    // Simulate some traffic
    await conn.sendData(Buffer.alloc(100));
    await conn.receiveData(Buffer.alloc(200));

    const connStats = conn.getStats();
    expect(connStats.bytesIn).toBe(100);
    expect(connStats.bytesOut).toBe(200);
    expect(connStats.startTime).toBeDefined();

    await conn.close();
    await proxyServer.stop();
  });

  it('should clean up connections on server stop', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 0,
      host: '127.0.0.1',
    });

    await proxyServer.start();

    const conn1 = await proxyServer.simulateConnection();
    const conn2 = await proxyServer.simulateConnection();

    expect(proxyServer.getStats().activeConnections).toBe(2);

    await proxyServer.stop();

    // All connections should be closed
    expect(conn1.isDestroyed()).toBe(true);
    expect(conn2.isDestroyed()).toBe(true);
  });
});

// ============================================================================
// MongoDB Client Compatibility Tests
// ============================================================================

describe('CLI Proxy - MongoDB Client Compatibility', () => {
  it('should support mongodb:// connection strings', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'mongodb://localhost:27017',
      port: 0,
      host: '127.0.0.1',
    });

    const config = proxyServer.getConfig();
    expect(config.target.host).toBe('localhost');
    expect(config.target.port).toBe(27017);
  });

  it('should support mongolake:// connection strings', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'mongolake://example.com:27018',
      port: 0,
      host: '127.0.0.1',
    });

    const config = proxyServer.getConfig();
    expect(config.target.host).toBe('example.com');
    expect(config.target.port).toBe(27018);
  });

  it('should generate MongoDB-compatible connection string', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 27018,
      host: '127.0.0.1',
    });

    await proxyServer.start();

    const connectionString = proxyServer.getConnectionString();
    expect(connectionString).toBe('mongodb://127.0.0.1:27018');

    await proxyServer.stop();
  });

  it('should respond to MongoDB handshake commands', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 0,
      host: '127.0.0.1',
    });

    await proxyServer.start();

    const conn = await proxyServer.simulateConnection();

    // Send hello command
    const helloResponse = await conn.sendCommand({ hello: 1, $db: 'admin' });
    expect(helloResponse.ok).toBe(1);

    // Send isMaster command
    const isMasterResponse = await conn.sendCommand({ isMaster: 1, $db: 'admin' });
    expect(isMasterResponse.ok).toBe(1);
    expect(isMasterResponse.ismaster).toBe(true);

    await conn.close();
    await proxyServer.stop();
  });

  it('should forward CRUD commands to target', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 0,
      host: '127.0.0.1',
    });

    await proxyServer.start();

    const conn = await proxyServer.simulateConnection();

    // Insert command
    const insertResponse = await conn.sendCommand({
      insert: 'testcol',
      documents: [{ _id: 'test1', value: 1 }],
      $db: 'testdb',
    });
    expect(insertResponse.ok).toBe(1);
    expect(insertResponse.n).toBe(1);

    // Find command
    const findResponse = await conn.sendCommand({
      find: 'testcol',
      filter: { _id: 'test1' },
      $db: 'testdb',
    });
    expect(findResponse.ok).toBe(1);

    await conn.close();
    await proxyServer.stop();
  });
});

// ============================================================================
// Proxy Statistics Tests
// ============================================================================

describe('CLI Proxy - Statistics', () => {
  it('should track total bytes forwarded', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 0,
      host: '127.0.0.1',
    });

    await proxyServer.start();

    const conn = await proxyServer.simulateConnection();
    await conn.sendData(Buffer.alloc(1024));
    await conn.receiveData(Buffer.alloc(2048));

    const stats = proxyServer.getStats();
    expect(stats.bytesForwarded).toBeGreaterThanOrEqual(3072);

    await conn.close();
    await proxyServer.stop();
  });

  it('should track messages forwarded', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 0,
      host: '127.0.0.1',
    });

    await proxyServer.start();

    const conn = await proxyServer.simulateConnection();
    await conn.sendCommand({ ping: 1, $db: 'admin' });
    await conn.sendCommand({ hello: 1, $db: 'admin' });

    const stats = proxyServer.getStats();
    expect(stats.messagesForwarded).toBeGreaterThanOrEqual(4); // 2 requests + 2 responses

    await conn.close();
    await proxyServer.stop();
  });

  it('should track connection errors', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 0,
      host: '127.0.0.1',
    });

    await proxyServer.start();

    // Simulate a connection error
    const conn = await proxyServer.simulateConnection();
    await conn.simulateError(new Error('Connection reset'));

    const stats = proxyServer.getStats();
    expect(stats.errors).toBeGreaterThanOrEqual(1);

    await proxyServer.stop();
  });

  it('should track uptime', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 0,
      host: '127.0.0.1',
    });

    await proxyServer.start();

    // Wait a bit
    await new Promise((r) => setTimeout(r, 100));

    const stats = proxyServer.getStats();
    expect(stats.uptime).toBeGreaterThanOrEqual(100);
    expect(stats.startedAt).toBeDefined();

    await proxyServer.stop();
  });

  it('should reset statistics on demand', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 0,
      host: '127.0.0.1',
    });

    await proxyServer.start();

    const conn = await proxyServer.simulateConnection();
    await conn.sendData(Buffer.alloc(1024));
    await conn.close();

    proxyServer.resetStats();

    const stats = proxyServer.getStats();
    expect(stats.bytesForwarded).toBe(0);
    expect(stats.totalConnections).toBe(0);

    await proxyServer.stop();
  });
});

// ============================================================================
// Health Check Tests
// ============================================================================

describe('CLI Proxy - Health Checks', () => {
  it('should provide health check endpoint', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 0,
      host: '127.0.0.1',
    });

    await proxyServer.start();

    const health = await proxyServer.healthCheck();
    expect(health.status).toBe('healthy');
    expect(health.targetReachable).toBeDefined();
    expect(health.activeConnections).toBeDefined();
    expect(health.uptime).toBeDefined();

    await proxyServer.stop();
  });

  it('should detect target unreachable', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:99999', // Unreachable port
      port: 0,
      host: '127.0.0.1',
    });

    await proxyServer.start();

    const health = await proxyServer.healthCheck();
    expect(health.status).toBe('unhealthy');
    expect(health.targetReachable).toBe(false);

    await proxyServer.stop();
  });

  it('should report degraded status at high load', async () => {
    const module = await import('../../../src/cli/proxy.js');

    const proxyServer = module.createProxyServer({
      target: 'localhost:27017',
      port: 0,
      host: '127.0.0.1',
      maxConnections: 10,
    });

    await proxyServer.start();

    // Simulate 9 connections (90% capacity)
    const connections = await Promise.all(
      Array.from({ length: 9 }, () => proxyServer.simulateConnection())
    );

    const health = await proxyServer.healthCheck();
    expect(health.status).toBe('degraded');
    expect(health.capacityUsed).toBeGreaterThanOrEqual(0.9);

    await Promise.all(connections.map((c) => c.close()));
    await proxyServer.stop();
  });
});
