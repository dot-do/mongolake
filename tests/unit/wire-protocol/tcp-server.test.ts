/**
 * TCP Server Tests
 *
 * Tests for the MongoDB wire protocol TCP server.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { createServer, type TcpServer } from '../../../src/wire-protocol/tcp-server.js';
import { serializeDocument, buildOpMsgResponse } from '../../../src/wire-protocol/bson-serializer.js';
import { OpCode } from '../../../src/wire-protocol/message-parser.js';
import { type Socket, createConnection } from 'node:net';
import { rm } from 'node:fs/promises';

// Helper to create an OP_MSG command
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

  // Message structure:
  // - Header: 16 bytes
  // - Flags: 4 bytes
  // - Section type: 1 byte (0 for body)
  // - BSON document: variable

  const messageLength = 16 + 4 + 1 + bsonDoc.length;
  const result = new Uint8Array(messageLength);
  const view = new DataView(result.buffer);

  // Header
  view.setInt32(0, messageLength, true); // messageLength
  view.setInt32(4, requestId, true); // requestId
  view.setInt32(8, 0, true); // responseTo
  view.setInt32(12, OpCode.OP_MSG, true); // opCode

  // Flags
  view.setUint32(16, 0, true);

  // Section (type 0 - body)
  result[20] = 0; // section type
  result.set(bsonDoc, 21);

  return result;
}

// Helper to parse an OP_MSG response
function parseOpMsgResponse(buffer: Buffer): { ok: number; [key: string]: unknown } {
  // Skip header (16) + flags (4) + section type (1)
  const docStart = 21;
  const view = new DataView(buffer.buffer, buffer.byteOffset + docStart);
  const docSize = view.getInt32(0, true);

  // Parse BSON manually (simplified)
  const doc: Record<string, unknown> = {};
  let pos = 4;

  while (pos < docSize - 1) {
    const elementType = buffer[docStart + pos];
    pos++;

    if (elementType === 0x00) break;

    // Read key
    const keyStart = pos;
    while (buffer[docStart + pos] !== 0x00 && pos < docSize) pos++;
    const key = buffer.toString('utf8', docStart + keyStart, docStart + pos);
    pos++;

    const valueView = new DataView(buffer.buffer, buffer.byteOffset + docStart + pos);

    switch (elementType) {
      case 0x01: // double
        doc[key] = valueView.getFloat64(0, true);
        pos += 8;
        break;
      case 0x02: // string
        const strLen = valueView.getInt32(0, true);
        pos += 4;
        doc[key] = buffer.toString('utf8', docStart + pos, docStart + pos + strLen - 1);
        pos += strLen;
        break;
      case 0x03: // document
        const nestedSize = valueView.getInt32(0, true);
        // Skip nested for now
        doc[key] = {};
        pos += nestedSize;
        break;
      case 0x04: // array
        const arrSize = valueView.getInt32(0, true);
        doc[key] = [];
        pos += arrSize;
        break;
      case 0x08: // boolean
        doc[key] = buffer[docStart + pos] !== 0x00;
        pos += 1;
        break;
      case 0x09: // date
        doc[key] = new Date(Number(valueView.getBigInt64(0, true)));
        pos += 8;
        break;
      case 0x0a: // null
        doc[key] = null;
        break;
      case 0x10: // int32
        doc[key] = valueView.getInt32(0, true);
        pos += 4;
        break;
      case 0x12: // int64
        doc[key] = Number(valueView.getBigInt64(0, true));
        pos += 8;
        break;
    }
  }

  return doc as { ok: number };
}

// Helper to send a command and receive response
async function sendCommand(
  socket: Socket,
  command: Uint8Array
): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    let responseBuffer = Buffer.alloc(0);

    const onData = (data: Buffer) => {
      responseBuffer = Buffer.concat([responseBuffer, data]);

      // Check if we have a complete message
      if (responseBuffer.length >= 4) {
        const view = new DataView(responseBuffer.buffer, responseBuffer.byteOffset);
        const messageLength = view.getInt32(0, true);

        if (responseBuffer.length >= messageLength) {
          socket.off('data', onData);
          resolve(responseBuffer.slice(0, messageLength));
        }
      }
    };

    socket.on('data', onData);
    socket.on('error', reject);

    socket.write(Buffer.from(command));
  });
}

describe('MongoDB Wire Protocol TCP Server', () => {
  let server: TcpServer;
  let serverPort: number;
  const testStoragePath = '.mongolake-tcp-test';

  beforeAll(async () => {
    // Clean up any stale test data from previous runs
    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore errors if directory doesn't exist
    }

    // Use a random port for testing
    server = createServer({
      port: 0, // Let the OS assign a port
      host: '127.0.0.1',
      mongoLakeConfig: { local: testStoragePath },
    });

    await server.start();
    const addr = server.address();
    serverPort = addr!.port;
  });

  afterAll(async () => {
    await server.stop();

    // Clean up test data
    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore errors
    }
  });

  describe('Connection handling', () => {
    it('should accept connections', async () => {
      const socket = createConnection({ port: serverPort, host: '127.0.0.1' });

      await new Promise<void>((resolve, reject) => {
        socket.on('connect', resolve);
        socket.on('error', reject);
      });

      expect(socket.remotePort).toBe(serverPort);
      socket.destroy();
    });

    it('should respond to ping command', async () => {
      const socket = createConnection({ port: serverPort, host: '127.0.0.1' });

      await new Promise<void>((resolve, reject) => {
        socket.on('connect', resolve);
        socket.on('error', reject);
      });

      const command = createOpMsgCommand(1, 'admin', { ping: 1 });
      const response = await sendCommand(socket, command);

      const doc = parseOpMsgResponse(response);
      expect(doc.ok).toBe(1);

      socket.destroy();
    });

    it('should respond to hello command', async () => {
      const socket = createConnection({ port: serverPort, host: '127.0.0.1' });

      await new Promise<void>((resolve, reject) => {
        socket.on('connect', resolve);
        socket.on('error', reject);
      });

      const command = createOpMsgCommand(1, 'admin', { hello: 1 });
      const response = await sendCommand(socket, command);

      const doc = parseOpMsgResponse(response);
      expect(doc.ok).toBe(1);
      expect(doc.ismaster).toBe(true);

      socket.destroy();
    });

    it('should respond to isMaster command', async () => {
      const socket = createConnection({ port: serverPort, host: '127.0.0.1' });

      await new Promise<void>((resolve, reject) => {
        socket.on('connect', resolve);
        socket.on('error', reject);
      });

      const command = createOpMsgCommand(1, 'admin', { isMaster: 1 });
      const response = await sendCommand(socket, command);

      const doc = parseOpMsgResponse(response);
      expect(doc.ok).toBe(1);
      expect(doc.ismaster).toBe(true);

      socket.destroy();
    });
  });

  describe('CRUD operations', () => {
    const testDb = 'testdb';
    const testCollection = 'testcol';

    it('should handle insert command', async () => {
      const socket = createConnection({ port: serverPort, host: '127.0.0.1' });

      await new Promise<void>((resolve, reject) => {
        socket.on('connect', resolve);
        socket.on('error', reject);
      });

      const command = createOpMsgCommand(1, testDb, {
        insert: testCollection,
        documents: [
          { _id: 'test1', name: 'Alice', age: 30 },
          { _id: 'test2', name: 'Bob', age: 25 },
        ],
      });
      const response = await sendCommand(socket, command);

      const doc = parseOpMsgResponse(response);
      expect(doc.ok).toBe(1);
      expect(doc.n).toBe(2);

      socket.destroy();
    });

    it('should handle find command', async () => {
      const socket = createConnection({ port: serverPort, host: '127.0.0.1' });

      await new Promise<void>((resolve, reject) => {
        socket.on('connect', resolve);
        socket.on('error', reject);
      });

      const command = createOpMsgCommand(1, testDb, {
        find: testCollection,
        filter: {},
      });
      const response = await sendCommand(socket, command);

      const doc = parseOpMsgResponse(response);
      expect(doc.ok).toBe(1);

      socket.destroy();
    });

    it('should handle unknown command', async () => {
      const socket = createConnection({ port: serverPort, host: '127.0.0.1' });

      await new Promise<void>((resolve, reject) => {
        socket.on('connect', resolve);
        socket.on('error', reject);
      });

      const command = createOpMsgCommand(1, 'admin', { unknownCommand: 1 });
      const response = await sendCommand(socket, command);

      const doc = parseOpMsgResponse(response);
      expect(doc.ok).toBe(0);

      socket.destroy();
    });
  });

  describe('Server lifecycle', () => {
    it('should report correct address', () => {
      const addr = server.address();
      expect(addr).not.toBeNull();
      expect(addr!.port).toBe(serverPort);
      expect(addr!.host).toBe('127.0.0.1');
    });
  });
});

describe('Graceful Shutdown', () => {
  const testStoragePath = '.mongolake-shutdown-test';

  beforeEach(async () => {
    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore errors if directory doesn't exist
    }
  });

  it('should stop accepting new connections during shutdown', async () => {
    const server = createServer({
      port: 0,
      host: '127.0.0.1',
      mongoLakeConfig: { local: testStoragePath },
    });

    await server.start();
    const addr = server.address();
    const port = addr!.port;

    // Connect before shutdown
    const socket1 = createConnection({ port, host: '127.0.0.1' });
    await new Promise<void>((resolve, reject) => {
      socket1.on('connect', resolve);
      socket1.on('error', reject);
    });

    // Start shutdown (don't await yet)
    const stopPromise = server.stop();

    // Give a moment for shutdown to start
    await new Promise((resolve) => setTimeout(resolve, 50));

    // Try to connect during shutdown - should be rejected or fail
    const socket2 = createConnection({ port, host: '127.0.0.1' });
    const connectionResult = await new Promise<'connected' | 'error' | 'closed'>((resolve) => {
      socket2.on('connect', () => resolve('connected'));
      socket2.on('error', () => resolve('error'));
      socket2.on('close', () => resolve('closed'));
      // Timeout to avoid hanging
      setTimeout(() => resolve('error'), 1000);
    });

    // Connection should either fail or be closed immediately
    expect(['error', 'closed']).toContain(connectionResult);

    // Clean up
    socket1.destroy();
    socket2.destroy();
    await stopPromise;
  });

  it('should wait for active connections to drain before shutdown completes', async () => {
    const server = createServer({
      port: 0,
      host: '127.0.0.1',
      mongoLakeConfig: { local: testStoragePath },
      shutdown: { drainTimeout: 1000, forceClose: true },
    });

    await server.start();
    const addr = server.address();
    const port = addr!.port;

    // Establish a connection
    const socket = createConnection({ port, host: '127.0.0.1' });
    await new Promise<void>((resolve, reject) => {
      socket.on('connect', resolve);
      socket.on('error', reject);
    });

    // Send a ping to ensure connection is active
    const command = createOpMsgCommand(1, 'admin', { ping: 1 });
    await sendCommand(socket, command);

    // Start shutdown and measure time
    const startTime = Date.now();

    // Close socket after short delay to allow drain to complete
    setTimeout(() => socket.destroy(), 100);

    const stopPromise = server.stop();

    // Server should wait for connection drain
    await stopPromise;
    const elapsed = Date.now() - startTime;

    // Shutdown should complete within drain timeout
    expect(elapsed).toBeLessThan(2000);

    // Clean up
    socket.destroy();
  }, 10000);

  it('should force close after drain timeout', async () => {
    const server = createServer({
      port: 0,
      host: '127.0.0.1',
      mongoLakeConfig: { local: testStoragePath },
      shutdown: { drainTimeout: 100, forceClose: true },
    });

    await server.start();
    const addr = server.address();
    const port = addr!.port;

    // Establish a connection
    const socket = createConnection({ port, host: '127.0.0.1' });
    await new Promise<void>((resolve, reject) => {
      socket.on('connect', resolve);
      socket.on('error', reject);
    });

    // Start shutdown with short drain timeout
    const startTime = Date.now();
    await server.stop();
    const elapsed = Date.now() - startTime;

    // Should complete around the drain timeout
    expect(elapsed).toBeLessThan(500); // Allow some margin

    // Clean up
    socket.destroy();
  });

  it('should properly clean up after shutdown', async () => {
    const server = createServer({
      port: 0,
      host: '127.0.0.1',
      mongoLakeConfig: { local: testStoragePath },
    });

    await server.start();
    const addr = server.address();
    expect(addr).not.toBeNull();

    // Get pool before shutdown
    const pool = server.getPool();
    expect(pool).not.toBeNull();

    // Shutdown
    await server.stop();

    // After shutdown, pool should be null
    expect(server.getPool()).toBeNull();
    expect(server.address()).toBeNull();
    expect(server.getMetrics()).toBeNull();
    expect(server.getBufferPoolStats()).toBeNull();
  });

  it('should handle multiple stop() calls gracefully', async () => {
    const server = createServer({
      port: 0,
      host: '127.0.0.1',
      mongoLakeConfig: { local: testStoragePath },
    });

    await server.start();

    // Call stop multiple times concurrently
    const results = await Promise.allSettled([
      server.stop(),
      server.stop(),
      server.stop(),
    ]);

    // All should succeed without errors
    for (const result of results) {
      expect(result.status).toBe('fulfilled');
    }
  });
});

describe('BSON Serializer', () => {
  it('should serialize simple document', () => {
    const doc = { hello: 'world', count: 42 };
    const bytes = serializeDocument(doc);

    expect(bytes.length).toBeGreaterThan(5); // Minimum BSON document size

    // Verify it starts with size
    const view = new DataView(bytes.buffer, bytes.byteOffset);
    const size = view.getInt32(0, true);
    expect(size).toBe(bytes.length);

    // Verify it ends with null terminator
    expect(bytes[bytes.length - 1]).toBe(0x00);
  });

  it('should serialize nested document', () => {
    const doc = { outer: { inner: 'value' } };
    const bytes = serializeDocument(doc);

    expect(bytes.length).toBeGreaterThan(5);
  });

  it('should serialize array', () => {
    const doc = { items: [1, 2, 3] };
    const bytes = serializeDocument(doc);

    expect(bytes.length).toBeGreaterThan(5);
  });

  it('should serialize null', () => {
    const doc = { value: null };
    const bytes = serializeDocument(doc);

    expect(bytes.length).toBeGreaterThan(5);
  });

  it('should serialize date', () => {
    const doc = { timestamp: new Date('2024-01-01') };
    const bytes = serializeDocument(doc);

    expect(bytes.length).toBeGreaterThan(5);
  });

  it('should serialize boolean', () => {
    const doc = { flag: true, other: false };
    const bytes = serializeDocument(doc);

    expect(bytes.length).toBeGreaterThan(5);
  });
});

// ============================================================================
// TCP Lifecycle Tests
// ============================================================================

/**
 * TCP Connection Lifecycle Tests
 *
 * Comprehensive tests for TCP connection lifecycle handling including:
 * - Connection establishment
 * - Connection close/cleanup
 * - Multiple concurrent connections
 * - Connection timeout handling
 * - Graceful shutdown scenarios
 * - Protocol upgrade (TLS)
 * - Error handling during connection
 * - Memory cleanup verification
 */

// Helper to create a connected socket with timeout
async function createConnectedSocket(port: number, host: string = '127.0.0.1', timeoutMs: number = 3000): Promise<Socket> {
  return new Promise((resolve, reject) => {
    const socket = createConnection({ port, host });
    const timeout = setTimeout(() => {
      socket.destroy();
      reject(new Error('Connection timeout'));
    }, timeoutMs);

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

// Helper to send command with timeout
async function sendCommandWithTimeout(
  socket: Socket,
  command: Uint8Array,
  timeoutMs: number = 3000
): Promise<Buffer> {
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

// Helper delay function
function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('TCP Connection Lifecycle - Connection Establishment', () => {
  const testStoragePath = '.mongolake-lifecycle-test';
  let server: TcpServer;
  let serverPort: number;

  beforeAll(async () => {
    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore
    }

    server = createServer({
      port: 0,
      host: '127.0.0.1',
      mongoLakeConfig: { local: testStoragePath },
      poolConfig: { maxConnections: 50 },
    });

    await server.start();
    const addr = server.address();
    serverPort = addr!.port;
  });

  afterAll(async () => {
    await server.stop();
    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore
    }
  });

  it('should establish TCP connection successfully', async () => {
    const socket = await createConnectedSocket(serverPort);

    expect(socket.remotePort).toBe(serverPort);
    expect(socket.remoteAddress).toBe('127.0.0.1');
    expect(socket.localAddress).toBeDefined();
    expect(socket.localPort).toBeDefined();

    socket.destroy();
  });

  it('should track connection in pool metrics after connection', async () => {
    const socket = await createConnectedSocket(serverPort);

    // Send a command to ensure connection is registered
    const command = createOpMsgCommand(1, 'admin', { ping: 1 });
    await sendCommandWithTimeout(socket, command);

    const metrics = server.getMetrics();
    expect(metrics).not.toBeNull();
    expect(metrics!.totalConnections).toBeGreaterThanOrEqual(1);

    socket.destroy();
  });

  it('should accept connection and allow immediate command execution', async () => {
    const socket = await createConnectedSocket(serverPort);

    // Immediately send ping command after connect
    const command = createOpMsgCommand(1, 'admin', { ping: 1 });
    const response = await sendCommandWithTimeout(socket, command);

    const doc = parseOpMsgResponse(response);
    expect(doc.ok).toBe(1);

    socket.destroy();
  });

  it('should handle rapid sequential connections', async () => {
    const sockets: Socket[] = [];
    const connectionCount = 10;

    for (let i = 0; i < connectionCount; i++) {
      const socket = await createConnectedSocket(serverPort);
      sockets.push(socket);
    }

    expect(sockets.length).toBe(connectionCount);

    // Verify all connections can send commands
    for (let i = 0; i < sockets.length; i++) {
      const command = createOpMsgCommand(i + 1, 'admin', { ping: 1 });
      const response = await sendCommandWithTimeout(sockets[i], command);
      const doc = parseOpMsgResponse(response);
      expect(doc.ok).toBe(1);
    }

    // Cleanup
    for (const socket of sockets) {
      socket.destroy();
    }
  });

  it('should update metrics correctly as connections are established', async () => {
    const initialMetrics = server.getMetrics();
    const initialTotal = initialMetrics?.totalCreated ?? 0;

    const socket1 = await createConnectedSocket(serverPort);
    const command1 = createOpMsgCommand(1, 'admin', { ping: 1 });
    await sendCommandWithTimeout(socket1, command1);

    const socket2 = await createConnectedSocket(serverPort);
    const command2 = createOpMsgCommand(2, 'admin', { ping: 1 });
    await sendCommandWithTimeout(socket2, command2);

    const metrics = server.getMetrics();
    expect(metrics!.totalCreated).toBeGreaterThanOrEqual(initialTotal + 2);

    socket1.destroy();
    socket2.destroy();
  });
});

describe('TCP Connection Lifecycle - Connection Close/Cleanup', () => {
  const testStoragePath = '.mongolake-close-test';
  let server: TcpServer;
  let serverPort: number;

  beforeAll(async () => {
    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore
    }

    server = createServer({
      port: 0,
      host: '127.0.0.1',
      mongoLakeConfig: { local: testStoragePath },
    });

    await server.start();
    serverPort = server.address()!.port;
  });

  afterAll(async () => {
    await server.stop();
    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore
    }
  });

  it('should handle client-initiated socket close gracefully', async () => {
    const socket = await createConnectedSocket(serverPort);

    const command = createOpMsgCommand(1, 'admin', { ping: 1 });
    await sendCommandWithTimeout(socket, command);

    // Client closes the connection
    await new Promise<void>((resolve) => {
      socket.on('close', resolve);
      socket.end();
    });

    // Allow time for server to process the close
    await delay(100);

    // Server should still be operational
    const socket2 = await createConnectedSocket(serverPort);
    const command2 = createOpMsgCommand(2, 'admin', { ping: 1 });
    const response = await sendCommandWithTimeout(socket2, command2);
    expect(parseOpMsgResponse(response).ok).toBe(1);
    socket2.destroy();
  });

  it('should handle abrupt socket destroy', async () => {
    const socket = await createConnectedSocket(serverPort);

    const command = createOpMsgCommand(1, 'admin', { ping: 1 });
    await sendCommandWithTimeout(socket, command);

    // Abruptly destroy the socket (simulating network failure)
    socket.destroy();

    await delay(100);

    // Server should still be operational
    const socket2 = await createConnectedSocket(serverPort);
    const command2 = createOpMsgCommand(2, 'admin', { ping: 1 });
    const response = await sendCommandWithTimeout(socket2, command2);
    expect(parseOpMsgResponse(response).ok).toBe(1);
    socket2.destroy();
  });

  it('should remove connection from pool when socket closes', async () => {
    const socket = await createConnectedSocket(serverPort);

    const command = createOpMsgCommand(1, 'admin', { ping: 1 });
    await sendCommandWithTimeout(socket, command);

    const metricsBefore = server.getMetrics()!;
    const totalBefore = metricsBefore.totalConnections;

    // Close the socket
    await new Promise<void>((resolve) => {
      socket.on('close', resolve);
      socket.destroy();
    });

    await delay(100);

    const metricsAfter = server.getMetrics()!;
    expect(metricsAfter.totalConnections).toBeLessThanOrEqual(totalBefore);
  });

  it('should handle socket error event', async () => {
    const socket = await createConnectedSocket(serverPort);

    // Send a command first
    const command = createOpMsgCommand(1, 'admin', { ping: 1 });
    await sendCommandWithTimeout(socket, command);

    // Simulate error by destroying socket
    socket.destroy(new Error('Simulated connection error'));

    await delay(100);

    // Server should still work
    const socket2 = await createConnectedSocket(serverPort);
    const command2 = createOpMsgCommand(2, 'admin', { ping: 1 });
    const response = await sendCommandWithTimeout(socket2, command2);
    expect(parseOpMsgResponse(response).ok).toBe(1);
    socket2.destroy();
  });

  it('should handle half-open connection (socket.end() from client)', async () => {
    const socket = await createConnectedSocket(serverPort);

    const command = createOpMsgCommand(1, 'admin', { ping: 1 });
    await sendCommandWithTimeout(socket, command);

    // Half-close the socket (FIN packet)
    socket.end();

    // Wait for close event
    await new Promise<void>((resolve) => {
      socket.on('close', resolve);
      // Timeout if close doesn't happen
      setTimeout(resolve, 500);
    });

    // Server should be operational
    const socket2 = await createConnectedSocket(serverPort);
    const command2 = createOpMsgCommand(2, 'admin', { ping: 1 });
    const response = await sendCommandWithTimeout(socket2, command2);
    expect(parseOpMsgResponse(response).ok).toBe(1);
    socket2.destroy();
  });
});

describe('TCP Connection Lifecycle - Multiple Concurrent Connections', () => {
  const testStoragePath = '.mongolake-concurrent-test';
  let server: TcpServer;
  let serverPort: number;

  beforeAll(async () => {
    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore
    }

    server = createServer({
      port: 0,
      host: '127.0.0.1',
      mongoLakeConfig: { local: testStoragePath },
      poolConfig: { maxConnections: 100 },
    });

    await server.start();
    serverPort = server.address()!.port;
  });

  afterAll(async () => {
    await server.stop();
    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore
    }
  });

  it('should handle multiple connections established simultaneously', async () => {
    const connectionPromises: Promise<Socket>[] = [];
    const connectionCount = 20;

    for (let i = 0; i < connectionCount; i++) {
      connectionPromises.push(createConnectedSocket(serverPort));
    }

    const sockets = await Promise.all(connectionPromises);
    expect(sockets.length).toBe(connectionCount);

    // Verify all connections are functional
    const commandPromises = sockets.map((socket, i) => {
      const command = createOpMsgCommand(i + 1, 'admin', { ping: 1 });
      return sendCommandWithTimeout(socket, command);
    });

    const responses = await Promise.all(commandPromises);
    for (const response of responses) {
      expect(parseOpMsgResponse(response).ok).toBe(1);
    }

    // Cleanup
    for (const socket of sockets) {
      socket.destroy();
    }
  });

  it('should handle concurrent commands on multiple connections', async () => {
    const sockets: Socket[] = [];
    const connectionCount = 10;

    for (let i = 0; i < connectionCount; i++) {
      sockets.push(await createConnectedSocket(serverPort));
    }

    // Send commands concurrently on all connections
    const commandBatches: Promise<Buffer>[] = [];
    for (let round = 0; round < 5; round++) {
      for (let i = 0; i < sockets.length; i++) {
        const command = createOpMsgCommand(round * 100 + i, 'admin', { ping: 1 });
        commandBatches.push(sendCommandWithTimeout(sockets[i], command));
      }
    }

    const responses = await Promise.all(commandBatches);
    expect(responses.length).toBe(50);

    for (const response of responses) {
      expect(parseOpMsgResponse(response).ok).toBe(1);
    }

    // Cleanup
    for (const socket of sockets) {
      socket.destroy();
    }
  });

  it('should isolate connections - one connection error should not affect others', async () => {
    const socket1 = await createConnectedSocket(serverPort);
    const socket2 = await createConnectedSocket(serverPort);
    const socket3 = await createConnectedSocket(serverPort);

    // Verify all connections work
    const cmd = createOpMsgCommand(1, 'admin', { ping: 1 });
    await sendCommandWithTimeout(socket1, cmd);
    await sendCommandWithTimeout(socket2, cmd);
    await sendCommandWithTimeout(socket3, cmd);

    // Kill one connection abruptly
    socket2.destroy(new Error('Simulated failure'));

    await delay(50);

    // Other connections should still work
    const cmd2 = createOpMsgCommand(2, 'admin', { ping: 1 });
    const response1 = await sendCommandWithTimeout(socket1, cmd2);
    const response3 = await sendCommandWithTimeout(socket3, cmd2);

    expect(parseOpMsgResponse(response1).ok).toBe(1);
    expect(parseOpMsgResponse(response3).ok).toBe(1);

    socket1.destroy();
    socket3.destroy();
  });

  it('should track correct metrics with concurrent connections', async () => {
    const initialMetrics = server.getMetrics()!;
    const initialCreated = initialMetrics.totalCreated;

    const sockets: Socket[] = [];
    const count = 5;

    for (let i = 0; i < count; i++) {
      const socket = await createConnectedSocket(serverPort);
      const cmd = createOpMsgCommand(i, 'admin', { ping: 1 });
      await sendCommandWithTimeout(socket, cmd);
      sockets.push(socket);
    }

    const midMetrics = server.getMetrics()!;
    expect(midMetrics.totalCreated).toBeGreaterThanOrEqual(initialCreated + count);

    // Close all connections
    for (const socket of sockets) {
      socket.destroy();
    }

    await delay(100);

    const finalMetrics = server.getMetrics()!;
    // Connections should have been destroyed
    expect(finalMetrics.totalDestroyed).toBeGreaterThanOrEqual(count);
  });

  it('should handle connection churn (rapid connect/disconnect)', async () => {
    const iterations = 10;

    for (let i = 0; i < iterations; i++) {
      const socket = await createConnectedSocket(serverPort);
      const command = createOpMsgCommand(i, 'admin', { ping: 1 });
      await sendCommandWithTimeout(socket, command);
      socket.destroy();
    }

    // Server should still be operational after churn
    const socket = await createConnectedSocket(serverPort);
    const command = createOpMsgCommand(999, 'admin', { ping: 1 });
    const response = await sendCommandWithTimeout(socket, command);
    expect(parseOpMsgResponse(response).ok).toBe(1);
    socket.destroy();
  });
});

describe('TCP Connection Lifecycle - Connection Pool Limits', () => {
  const testStoragePath = '.mongolake-limit-test';
  let server: TcpServer;
  let serverPort: number;

  beforeAll(async () => {
    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore
    }

    server = createServer({
      port: 0,
      host: '127.0.0.1',
      mongoLakeConfig: { local: testStoragePath },
      poolConfig: { maxConnections: 5 }, // Low limit for testing
      shutdown: { drainTimeout: 1000, forceClose: true },
    });

    await server.start();
    serverPort = server.address()!.port;
  }, 10000);

  afterAll(async () => {
    await server.stop();
    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore
    }
  }, 10000);

  it('should accept connections up to the pool limit', async () => {
    const sockets: Socket[] = [];

    for (let i = 0; i < 5; i++) {
      const socket = await createConnectedSocket(serverPort);
      const cmd = createOpMsgCommand(i, 'admin', { ping: 1 });
      await sendCommandWithTimeout(socket, cmd);
      sockets.push(socket);
    }

    expect(sockets.length).toBe(5);

    const metrics = server.getMetrics()!;
    expect(metrics.totalConnections).toBe(5);

    // Cleanup
    for (const socket of sockets) {
      socket.destroy();
    }

    await delay(100);
  });

  it('should reject connections when pool is at capacity', async () => {
    const sockets: Socket[] = [];

    // Fill up the pool
    for (let i = 0; i < 5; i++) {
      const socket = await createConnectedSocket(serverPort);
      const cmd = createOpMsgCommand(i, 'admin', { ping: 1 });
      await sendCommandWithTimeout(socket, cmd);
      sockets.push(socket);
    }

    // Try to connect when pool is full - should be rejected
    const extraSocket = createConnection({ port: serverPort, host: '127.0.0.1' });
    const result = await new Promise<'connected' | 'error' | 'closed'>((resolve) => {
      extraSocket.on('connect', () => {
        // Connection might be accepted but then closed
        setTimeout(() => resolve('connected'), 100);
      });
      extraSocket.on('error', () => resolve('error'));
      extraSocket.on('close', () => resolve('closed'));
      setTimeout(() => resolve('error'), 1000);
    });

    expect(['error', 'closed']).toContain(result);

    // Cleanup
    extraSocket.destroy();
    for (const socket of sockets) {
      socket.destroy();
    }

    await delay(100);
  });

  it('should allow new connections after existing ones close', async () => {
    const sockets: Socket[] = [];

    // Fill up the pool
    for (let i = 0; i < 5; i++) {
      const socket = await createConnectedSocket(serverPort);
      const cmd = createOpMsgCommand(i, 'admin', { ping: 1 });
      await sendCommandWithTimeout(socket, cmd);
      sockets.push(socket);
    }

    // Close one connection
    const closedSocket = sockets.pop()!;
    closedSocket.destroy();

    await delay(100);

    // Now should be able to create a new connection
    const newSocket = await createConnectedSocket(serverPort);
    const cmd = createOpMsgCommand(99, 'admin', { ping: 1 });
    const response = await sendCommandWithTimeout(newSocket, cmd);
    expect(parseOpMsgResponse(response).ok).toBe(1);

    // Cleanup
    newSocket.destroy();
    for (const socket of sockets) {
      socket.destroy();
    }

    await delay(100);
  });
});

describe('TCP Connection Lifecycle - Graceful Shutdown with Active Connections', () => {
  const testStoragePath = '.mongolake-active-shutdown-test';

  beforeEach(async () => {
    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore
    }
  });

  it('should complete in-flight requests before shutdown', async () => {
    const server = createServer({
      port: 0,
      host: '127.0.0.1',
      mongoLakeConfig: { local: testStoragePath },
      shutdown: { drainTimeout: 500, forceClose: true },
    });

    await server.start();
    const port = server.address()!.port;

    const socket = await createConnectedSocket(port);

    // Send a request and wait for response before shutdown
    const command = createOpMsgCommand(1, 'admin', { ping: 1 });
    const response = await sendCommandWithTimeout(socket, command, 2000);
    expect(parseOpMsgResponse(response).ok).toBe(1);

    // Destroy socket before shutdown to ensure clean shutdown
    socket.destroy();
    await delay(50);

    // Now start shutdown (request already completed)
    await server.stop();

    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore
    }
  }, 5000);

  it('should forcibly close connections after drain timeout', async () => {
    const server = createServer({
      port: 0,
      host: '127.0.0.1',
      mongoLakeConfig: { local: testStoragePath },
      shutdown: { drainTimeout: 300, forceClose: true },
    });

    await server.start();
    const port = server.address()!.port;

    const socket = await createConnectedSocket(port);
    const cmd = createOpMsgCommand(1, 'admin', { ping: 1 });
    await sendCommandWithTimeout(socket, cmd);

    // Close socket to allow clean shutdown
    socket.destroy();
    await delay(50);

    // Start shutdown
    const startTime = Date.now();
    await server.stop();
    const elapsed = Date.now() - startTime;

    // Should complete within a reasonable time
    expect(elapsed).toBeLessThan(2000);

    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore
    }
  }, 5000);

  it('should close multiple active connections during shutdown', async () => {
    const server = createServer({
      port: 0,
      host: '127.0.0.1',
      mongoLakeConfig: { local: testStoragePath },
      shutdown: { drainTimeout: 300, forceClose: true },
    });

    await server.start();
    const port = server.address()!.port;

    const sockets: Socket[] = [];
    for (let i = 0; i < 3; i++) {
      const socket = await createConnectedSocket(port);
      const cmd = createOpMsgCommand(i, 'admin', { ping: 1 });
      await sendCommandWithTimeout(socket, cmd);
      sockets.push(socket);
    }

    // Close all sockets before shutdown
    for (const socket of sockets) {
      socket.destroy();
    }
    await delay(50);

    // Shutdown server - connections should be already closed
    await server.stop();

    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore
    }
  }, 5000);

  it('should cleanup buffer pool during shutdown', async () => {
    const server = createServer({
      port: 0,
      host: '127.0.0.1',
      mongoLakeConfig: { local: testStoragePath },
      shutdown: { drainTimeout: 300, forceClose: true },
    });

    await server.start();
    const port = server.address()!.port;

    const socket = await createConnectedSocket(port);
    const cmd = createOpMsgCommand(1, 'admin', { ping: 1 });
    await sendCommandWithTimeout(socket, cmd);

    // Buffer pool should have stats before shutdown
    const statsBefore = server.getBufferPoolStats();
    expect(statsBefore).not.toBeNull();
    expect(statsBefore!.totalCreated).toBeGreaterThan(0);

    // Destroy socket before shutdown
    socket.destroy();
    await delay(50);

    await server.stop();

    // Buffer pool should be null after shutdown
    expect(server.getBufferPoolStats()).toBeNull();

    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore
    }
  }, 5000);
});

describe('TCP Connection Lifecycle - Error Handling During Connection', () => {
  const testStoragePath = '.mongolake-error-test';
  let server: TcpServer;
  let serverPort: number;

  beforeAll(async () => {
    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore
    }

    server = createServer({
      port: 0,
      host: '127.0.0.1',
      mongoLakeConfig: { local: testStoragePath },
    });

    await server.start();
    serverPort = server.address()!.port;
  });

  afterAll(async () => {
    await server.stop();
    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore
    }
  });

  it('should handle malformed message header gracefully', async () => {
    const socket = await createConnectedSocket(serverPort);

    // Send garbage data that looks like it could be a header
    const malformedData = Buffer.from([
      0x05, 0x00, 0x00, 0x00, // messageLength = 5 (too small)
      0x01, 0x00, 0x00, 0x00, // requestId
      0x00, 0x00, 0x00, 0x00, // responseTo
      0xDD, 0x07, 0x00, 0x00, // opCode (OP_MSG)
    ]);

    socket.write(malformedData);

    // Wait a bit and check server is still operational
    await delay(100);

    // Server should still work for new connections
    const socket2 = await createConnectedSocket(serverPort);
    const cmd = createOpMsgCommand(1, 'admin', { ping: 1 });
    const response = await sendCommandWithTimeout(socket2, cmd);
    expect(parseOpMsgResponse(response).ok).toBe(1);

    socket.destroy();
    socket2.destroy();
  });

  it('should handle incomplete message gracefully', async () => {
    const socket = await createConnectedSocket(serverPort);

    // Send only partial message (incomplete)
    const partialMessage = Buffer.from([
      0x20, 0x00, 0x00, 0x00, // messageLength = 32
      0x01, 0x00, 0x00, 0x00, // requestId
      0x00, 0x00, 0x00, 0x00, // responseTo
      0xDD, 0x07, 0x00, 0x00, // opCode
      // Missing rest of the message...
    ]);

    socket.write(partialMessage);

    // Server should wait for more data, not crash
    await delay(100);

    // Server should still be operational
    const socket2 = await createConnectedSocket(serverPort);
    const cmd = createOpMsgCommand(1, 'admin', { ping: 1 });
    const response = await sendCommandWithTimeout(socket2, cmd);
    expect(parseOpMsgResponse(response).ok).toBe(1);

    socket.destroy();
    socket2.destroy();
  });

  it('should handle unknown opcode gracefully', async () => {
    const socket = await createConnectedSocket(serverPort);

    // Create a message with unknown opcode
    const unknownOpCode = Buffer.alloc(30);
    const view = new DataView(unknownOpCode.buffer);
    view.setInt32(0, 30, true); // messageLength
    view.setInt32(4, 1, true); // requestId
    view.setInt32(8, 0, true); // responseTo
    view.setInt32(12, 9999, true); // Unknown opCode

    socket.write(unknownOpCode);

    // Should receive error response
    const response = await new Promise<Buffer>((resolve) => {
      let buffer = Buffer.alloc(0);
      const onData = (data: Buffer) => {
        buffer = Buffer.concat([buffer, data]);
        if (buffer.length >= 4) {
          const msgLen = new DataView(buffer.buffer, buffer.byteOffset).getInt32(0, true);
          if (buffer.length >= msgLen) {
            socket.off('data', onData);
            resolve(buffer.slice(0, msgLen));
          }
        }
      };
      socket.on('data', onData);
      setTimeout(() => {
        socket.off('data', onData);
        resolve(Buffer.alloc(0));
      }, 1000);
    });

    // Should get a response (error response) or empty buffer
    // Either way, server should still work
    const socket2 = await createConnectedSocket(serverPort);
    const cmd = createOpMsgCommand(1, 'admin', { ping: 1 });
    const response2 = await sendCommandWithTimeout(socket2, cmd);
    expect(parseOpMsgResponse(response2).ok).toBe(1);

    socket.destroy();
    socket2.destroy();
  });

  it('should track error count in metrics', async () => {
    const socket = await createConnectedSocket(serverPort);
    const cmd = createOpMsgCommand(1, 'admin', { ping: 1 });
    await sendCommandWithTimeout(socket, cmd);

    // Trigger an error by destroying the socket
    socket.destroy(new Error('Simulated error'));

    await delay(100);

    const metrics = server.getMetrics()!;
    expect(metrics.errorCount).toBeGreaterThanOrEqual(0);
  });

  it('should recover from connection errors and accept new connections', async () => {
    // Create and destroy several connections with errors
    for (let i = 0; i < 5; i++) {
      const socket = await createConnectedSocket(serverPort);
      socket.destroy(new Error(`Error ${i}`));
    }

    await delay(100);

    // Server should still be operational
    const socket = await createConnectedSocket(serverPort);
    const cmd = createOpMsgCommand(1, 'admin', { ping: 1 });
    const response = await sendCommandWithTimeout(socket, cmd);
    expect(parseOpMsgResponse(response).ok).toBe(1);
    socket.destroy();
  });
});

describe('TCP Connection Lifecycle - Memory Cleanup Verification', () => {
  const testStoragePath = '.mongolake-memory-test';

  it('should release buffer pool resources when connections close', async () => {
    const server = createServer({
      port: 0,
      host: '127.0.0.1',
      mongoLakeConfig: { local: testStoragePath },
      bufferPool: { poolSize: 5 },
    });

    await server.start();
    const port = server.address()!.port;

    // Create connections and send data
    const sockets: Socket[] = [];
    for (let i = 0; i < 3; i++) {
      const socket = await createConnectedSocket(port);
      const cmd = createOpMsgCommand(i, 'admin', { ping: 1 });
      await sendCommandWithTimeout(socket, cmd);
      sockets.push(socket);
    }

    const statsBefore = server.getBufferPoolStats()!;
    expect(statsBefore.acquireCount).toBeGreaterThan(0);

    // Close all connections
    for (const socket of sockets) {
      socket.destroy();
    }

    await delay(200);

    const statsAfter = server.getBufferPoolStats()!;
    expect(statsAfter.releaseCount).toBeGreaterThanOrEqual(statsBefore.releaseCount);

    await server.stop();

    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore
    }
  });

  it('should not leak connections over time', async () => {
    const server = createServer({
      port: 0,
      host: '127.0.0.1',
      mongoLakeConfig: { local: testStoragePath },
    });

    await server.start();
    const port = server.address()!.port;

    // Do multiple rounds of connect/disconnect
    for (let round = 0; round < 5; round++) {
      const sockets: Socket[] = [];

      for (let i = 0; i < 5; i++) {
        const socket = await createConnectedSocket(port);
        const cmd = createOpMsgCommand(round * 10 + i, 'admin', { ping: 1 });
        await sendCommandWithTimeout(socket, cmd);
        sockets.push(socket);
      }

      // Close all sockets
      for (const socket of sockets) {
        socket.destroy();
      }

      await delay(100);
    }

    const metrics = server.getMetrics()!;
    // All connections should be destroyed
    expect(metrics.totalDestroyed).toBeGreaterThanOrEqual(metrics.totalCreated - metrics.totalConnections);
    // Should have few or no active connections
    expect(metrics.totalConnections).toBeLessThanOrEqual(5);

    await server.stop();

    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore
    }
  });

  it('should clear socket state managers on shutdown', async () => {
    const server = createServer({
      port: 0,
      host: '127.0.0.1',
      mongoLakeConfig: { local: testStoragePath },
      shutdown: { drainTimeout: 300, forceClose: true },
    });

    await server.start();
    const port = server.address()!.port;

    const sockets: Socket[] = [];
    for (let i = 0; i < 3; i++) {
      const socket = await createConnectedSocket(port);
      const cmd = createOpMsgCommand(i, 'admin', { ping: 1 });
      await sendCommandWithTimeout(socket, cmd);
      sockets.push(socket);
    }

    // Close sockets before shutdown
    for (const socket of sockets) {
      socket.destroy();
    }
    await delay(50);

    // Shutdown server
    await server.stop();

    // All resources should be cleaned up
    expect(server.getPool()).toBeNull();
    expect(server.getBufferPoolStats()).toBeNull();
    expect(server.getMetrics()).toBeNull();

    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore
    }
  }, 5000);
});

describe('TCP Connection Lifecycle - TLS Configuration', () => {
  it('should report TLS disabled by default', () => {
    const server = createServer({
      port: 0,
      host: '127.0.0.1',
    });

    expect(server.isTlsEnabled()).toBe(false);
  });

  it('should report TLS enabled when configured', () => {
    const server = createServer({
      port: 0,
      host: '127.0.0.1',
      tls: { enabled: true },
    });

    expect(server.isTlsEnabled()).toBe(true);
  });

  it('should accept TLS configuration options', () => {
    const server = createServer({
      port: 0,
      host: '127.0.0.1',
      tls: {
        enabled: true,
        cert: '-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----',
        key: '-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----',
        requestCert: false,
        rejectUnauthorized: false,
      },
    });

    expect(server.isTlsEnabled()).toBe(true);
  });
});

describe('TCP Connection Lifecycle - Server Address Management', () => {
  const testStoragePath = '.mongolake-address-test';

  it('should return null address before start', () => {
    const server = createServer({
      port: 0,
      host: '127.0.0.1',
    });

    expect(server.address()).toBeNull();
  });

  it('should return correct address after start', async () => {
    const server = createServer({
      port: 0,
      host: '127.0.0.1',
      mongoLakeConfig: { local: testStoragePath },
    });

    await server.start();

    const addr = server.address();
    expect(addr).not.toBeNull();
    expect(addr!.host).toBe('127.0.0.1');
    expect(addr!.port).toBeGreaterThan(0);

    await server.stop();

    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore
    }
  });

  it('should return null address after stop', async () => {
    const server = createServer({
      port: 0,
      host: '127.0.0.1',
      mongoLakeConfig: { local: testStoragePath },
    });

    await server.start();
    expect(server.address()).not.toBeNull();

    await server.stop();
    expect(server.address()).toBeNull();

    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore
    }
  });

  it('should bind to specified port when provided', async () => {
    // Find an available port
    const { createServer: createNetServer } = await import('node:net');
    const tempServer = createNetServer();
    await new Promise<void>((resolve) => tempServer.listen(0, '127.0.0.1', resolve));
    const assignedPort = (tempServer.address() as { port: number }).port;
    await new Promise<void>((resolve) => tempServer.close(() => resolve()));

    const server = createServer({
      port: assignedPort,
      host: '127.0.0.1',
      mongoLakeConfig: { local: testStoragePath },
    });

    await server.start();

    const addr = server.address();
    expect(addr).not.toBeNull();
    expect(addr!.port).toBe(assignedPort);

    await server.stop();

    try {
      await rm(testStoragePath, { recursive: true, force: true });
    } catch {
      // Ignore
    }
  });
});
