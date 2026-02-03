/**
 * MongoDB Wire Protocol TCP Server
 *
 * Implements a TCP server that accepts MongoDB wire protocol connections
 * and routes them to the MongoLake storage layer.
 *
 * Features:
 * - Connection pooling with configurable limits
 * - Idle connection timeout
 * - Connection reuse tracking
 * - Health check pings
 * - Graceful error handling
 * - TLS/SSL support
 * - Configurable backpressure handling
 * - Buffer pooling for memory efficiency
 * - Graceful shutdown with connection draining
 *
 * Usage:
 *   import { createServer } from 'mongolake/wire-protocol';
 *   const server = createServer({ port: 27017 });
 *   await server.start();
 */

import type { Socket, Server } from 'node:net';
import type { TLSSocket, Server as TLSServer, SecureContextOptions } from 'node:tls';
import {
  parseMessage,
  parseMessageHeader,
  extractCommand,
  type OpQueryMessage,
  type ExtractedCommand,
  type Document,
} from './message-parser.js';
import {
  buildOpReply,
  buildErrorResponse,
} from './bson-serializer.js';
import { executeCommand, type CommandContext } from './command-handlers.js';
import { MongoLake, type MongoLakeConfig } from '../client/index.js';
import {
  ConnectionPool,
  type ConnectionPoolConfig,
  type PooledConnection,
  type PoolMetrics,
} from './connection-pool.js';
import { logger } from '../utils/logger.js';
import {
  TCP_BACKPRESSURE_HIGH_WATER_MARK,
  TCP_BACKPRESSURE_LOW_WATER_MARK,
  TCP_BACKPRESSURE_TIMEOUT_MS,
  MAX_WIRE_MESSAGE_SIZE,
} from '../constants.js';
import {
  SizeLimitValidator,
  SizeLimitError,
  type SizeLimitConfig,
} from './size-limits.js';

// ============================================================================
// Types
// ============================================================================

/**
 * TLS configuration options for secure connections
 */
export interface TlsOptions {
  /** Enable TLS (default: false) */
  enabled: boolean;

  /** Path to certificate file or certificate content */
  cert?: string;

  /** Path to private key file or key content */
  key?: string;

  /** Path to CA certificate file or CA content */
  ca?: string;

  /** Passphrase for the private key */
  passphrase?: string;

  /** Require client certificates (mTLS) */
  requestCert?: boolean;

  /** Reject unauthorized certificates */
  rejectUnauthorized?: boolean;
}

/**
 * Backpressure handling configuration
 */
export interface BackpressureConfig {
  /** High water mark - pause reading when buffer exceeds this (default: 16KB) */
  highWaterMark?: number;

  /** Low water mark - resume reading when buffer drops below this (default: 8KB) */
  lowWaterMark?: number;

  /** Maximum time to wait for drain in ms (default: 30000) */
  drainTimeout?: number;
}

/**
 * Buffer pool configuration for memory efficiency
 */
export interface BufferPoolConfig {
  /** Initial buffer size (default: 4KB) */
  initialSize?: number;

  /** Maximum buffer size before allocation (default: 1MB) */
  maxSize?: number;

  /** Number of buffers to pre-allocate (default: 10) */
  poolSize?: number;
}

/**
 * Graceful shutdown configuration
 */
export interface ShutdownConfig {
  /** Maximum time to wait for connections to drain in ms (default: 30000) */
  drainTimeout?: number;

  /** Send goaway/closing notice to clients */
  notifyClients?: boolean;

  /** Force close after drain timeout */
  forceClose?: boolean;
}

/**
 * Message size validation configuration
 */
export interface MessageSizeConfig {
  /**
   * Maximum message size in bytes before rejecting
   * Default: 48MB (MongoDB's wire protocol limit)
   */
  maxMessageSize?: number;

  /**
   * Size limit configuration for the validator
   */
  sizeLimits?: SizeLimitConfig;
}

export interface TcpServerOptions {
  /** Port to listen on (default: 27017) */
  port?: number;

  /** Host to bind to (default: '127.0.0.1') */
  host?: string;

  /** MongoLake configuration */
  mongoLakeConfig?: MongoLakeConfig;

  /** Enable debug logging */
  debug?: boolean;

  /** Connection pool configuration */
  poolConfig?: ConnectionPoolConfig;

  /** TLS/SSL configuration */
  tls?: TlsOptions;

  /** Backpressure handling configuration */
  backpressure?: BackpressureConfig;

  /** Buffer pool configuration */
  bufferPool?: BufferPoolConfig;

  /** Graceful shutdown configuration */
  shutdown?: ShutdownConfig;

  /** Message size validation configuration */
  messageSize?: MessageSizeConfig;
}

export interface TcpServer {
  /** Start the server */
  start(): Promise<void>;

  /** Stop the server gracefully */
  stop(): Promise<void>;

  /** Get the server address */
  address(): { port: number; host: string } | null;

  /** Get the connection pool (available after start) */
  getPool(): ConnectionPool | null;

  /** Get pool metrics */
  getMetrics(): PoolMetrics | null;

  /** Check if TLS is enabled */
  isTlsEnabled(): boolean;

  /** Get buffer pool stats */
  getBufferPoolStats(): BufferPoolStats | null;
}

/**
 * Buffer pool statistics
 */
export interface BufferPoolStats {
  /** Number of buffers currently in pool */
  available: number;

  /** Total buffers created */
  totalCreated: number;

  /** Number of times a buffer was acquired */
  acquireCount: number;

  /** Number of times a buffer was released back */
  releaseCount: number;

  /** Current total memory allocated */
  totalMemory: number;
}

// ============================================================================
// Graceful Shutdown Implementation
// ============================================================================

/**
 * Manages graceful shutdown of the TCP server
 * Tracks active connections and ensures clean termination
 */
export class GracefulShutdown {
  private isShuttingDown = false;
  private activeConnections: Set<Socket | TLSSocket> = new Set();
  // Use Promise<unknown> to avoid double casts when tracking operations of various types
  private pendingOperations: Set<Promise<unknown>> = new Set();

  /**
   * Check if shutdown is in progress
   */
  get shuttingDown(): boolean {
    return this.isShuttingDown;
  }

  /**
   * Get the number of active connections
   */
  get connectionCount(): number {
    return this.activeConnections.size;
  }

  /**
   * Register a connection for tracking
   */
  registerConnection(socket: Socket | TLSSocket): void {
    if (this.isShuttingDown) {
      socket.destroy();
      return;
    }
    this.activeConnections.add(socket);
  }

  /**
   * Unregister a connection when it closes
   */
  unregisterConnection(socket: Socket | TLSSocket): void {
    this.activeConnections.delete(socket);
  }

  /**
   * Track a pending operation that should complete before shutdown
   */
  trackOperation<T>(operation: Promise<T>): Promise<T> {
    const wrappedOperation = operation.then(
      (result) => {
        this.pendingOperations.delete(wrappedOperation);
        return result;
      },
      (error) => {
        this.pendingOperations.delete(wrappedOperation);
        throw error;
      }
    );
    this.pendingOperations.add(wrappedOperation);
    return wrappedOperation;
  }

  /**
   * Perform graceful shutdown
   * @param server - The server to shut down
   * @param timeoutMs - Maximum time to wait for connections to drain (default: 30000ms)
   * @returns Promise that resolves when shutdown is complete
   */
  async shutdown(server: Server | TLSServer | null, timeoutMs = 30000): Promise<void> {
    if (this.isShuttingDown) {
      // Already shutting down, wait for completion
      const deadline = Date.now() + timeoutMs;
      while (this.activeConnections.size > 0 && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 100));
      }
      return;
    }

    this.isShuttingDown = true;

    // Stop accepting new connections
    if (server) {
      await new Promise<void>((resolve) => {
        server.close(() => resolve());
      });
    }

    // Wait for active connections to complete with timeout
    const deadline = Date.now() + timeoutMs;
    while (this.activeConnections.size > 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 100));
    }

    // Force close remaining connections
    for (const conn of this.activeConnections) {
      conn.destroy();
    }
    this.activeConnections.clear();

    // Wait for pending operations with remaining time
    const remainingTime = Math.max(0, deadline - Date.now());
    if (this.pendingOperations.size > 0 && remainingTime > 0) {
      await Promise.race([
        Promise.all(this.pendingOperations),
        new Promise((r) => setTimeout(r, remainingTime)),
      ]);
    }

    // Flush any pending state
    await this.flushState();
  }

  /**
   * Flush any pending state before final shutdown
   * Override this method in subclasses to add custom flush logic
   */
  async flushState(): Promise<void> {
    // Default implementation: clear pending operations
    this.pendingOperations.clear();
  }

  /**
   * Reset the shutdown state (primarily for testing)
   */
  reset(): void {
    this.isShuttingDown = false;
    this.activeConnections.clear();
    this.pendingOperations.clear();
  }
}

// ============================================================================
// Buffer Pool Implementation
// ============================================================================

/**
 * Pooled buffer for efficient memory reuse
 */
interface PooledBuffer {
  buffer: Uint8Array;
  size: number;
  inUse: boolean;
}

/**
 * Buffer pool for efficient memory allocation
 * Reduces GC pressure by reusing buffers across connections
 */
class BufferPool {
  private readonly pool: PooledBuffer[] = [];
  private readonly initialSize: number;
  private readonly maxSize: number;
  private readonly poolSize: number;
  private stats: BufferPoolStats = {
    available: 0,
    totalCreated: 0,
    acquireCount: 0,
    releaseCount: 0,
    totalMemory: 0,
  };

  constructor(config: BufferPoolConfig = {}) {
    this.initialSize = config.initialSize ?? 4096; // 4KB
    this.maxSize = config.maxSize ?? 1048576; // 1MB
    this.poolSize = config.poolSize ?? 10;

    // Pre-allocate buffers
    this.preallocate();
  }

  private preallocate(): void {
    for (let i = 0; i < this.poolSize; i++) {
      const buffer = new Uint8Array(this.initialSize);
      this.pool.push({
        buffer,
        size: this.initialSize,
        inUse: false,
      });
      this.stats.totalCreated++;
      this.stats.totalMemory += this.initialSize;
    }
    this.stats.available = this.pool.length;
  }

  /**
   * Acquire a buffer from the pool
   */
  acquire(minSize: number = 0): Uint8Array {
    this.stats.acquireCount++;

    // Find a suitable buffer in the pool
    for (const pooled of this.pool) {
      if (!pooled.inUse && pooled.size >= minSize) {
        pooled.inUse = true;
        this.stats.available--;
        return pooled.buffer;
      }
    }

    // No suitable buffer available, create a new one
    const size = Math.max(this.initialSize, minSize);
    if (size <= this.maxSize) {
      const buffer = new Uint8Array(size);
      const pooled: PooledBuffer = {
        buffer,
        size,
        inUse: true,
      };
      this.pool.push(pooled);
      this.stats.totalCreated++;
      this.stats.totalMemory += size;
      return buffer;
    }

    // Requested size exceeds max, allocate without pooling
    return new Uint8Array(size);
  }

  /**
   * Release a buffer back to the pool
   */
  release(buffer: Uint8Array): void {
    this.stats.releaseCount++;

    for (const pooled of this.pool) {
      if (pooled.buffer === buffer) {
        pooled.inUse = false;
        this.stats.available++;
        // Clear buffer for security
        buffer.fill(0);
        return;
      }
    }
    // Buffer not from pool, let GC handle it
  }

  /**
   * Grow a buffer while keeping track in the pool
   */
  grow(oldBuffer: Uint8Array, newSize: number): Uint8Array {
    const newBuffer = this.acquire(newSize);
    newBuffer.set(oldBuffer);
    this.release(oldBuffer);
    return newBuffer;
  }

  /**
   * Get pool statistics
   */
  getStats(): BufferPoolStats {
    return { ...this.stats };
  }

  /**
   * Clear the pool
   */
  clear(): void {
    this.pool.length = 0;
    this.stats = {
      available: 0,
      totalCreated: 0,
      acquireCount: 0,
      releaseCount: 0,
      totalMemory: 0,
    };
  }
}

// ============================================================================
// Socket State Manager
// ============================================================================

/**
 * Result of size validation during message extraction
 */
export interface MessageSizeValidationResult {
  /** Whether the message size is valid */
  valid: boolean;
  /** The declared message size from the header */
  declaredSize?: number;
  /** Error if validation failed */
  error?: SizeLimitError;
}

/**
 * Manages state for a single socket connection
 * Handles buffering, backpressure, message framing, and size validation
 */
class SocketStateManager {
  readonly connectionId: number;
  private buffer: Uint8Array;
  private bufferOffset: number = 0;
  requestCounter: number = 1;
  private isPaused: boolean = false;
  private pendingWrites: number = 0;
  private lastSizeValidationError: SizeLimitError | null = null;

  private readonly socket: Socket | TLSSocket;
  private readonly bufferPool: BufferPool;
  private readonly backpressureConfig: Required<BackpressureConfig>;
  private readonly debug: boolean;
  private readonly sizeLimitValidator: SizeLimitValidator;
  private readonly maxMessageSize: number;

  constructor(
    connectionId: number,
    socket: Socket | TLSSocket,
    bufferPool: BufferPool,
    backpressureConfig: BackpressureConfig = {},
    debug: boolean = false,
    sizeLimitValidator?: SizeLimitValidator,
    maxMessageSize?: number
  ) {
    this.connectionId = connectionId;
    this.socket = socket;
    this.bufferPool = bufferPool;
    this.debug = debug;
    this.backpressureConfig = {
      highWaterMark: backpressureConfig.highWaterMark ?? TCP_BACKPRESSURE_HIGH_WATER_MARK,
      lowWaterMark: backpressureConfig.lowWaterMark ?? TCP_BACKPRESSURE_LOW_WATER_MARK,
      drainTimeout: backpressureConfig.drainTimeout ?? TCP_BACKPRESSURE_TIMEOUT_MS,
    };
    this.sizeLimitValidator = sizeLimitValidator ?? new SizeLimitValidator();
    this.maxMessageSize = maxMessageSize ?? MAX_WIRE_MESSAGE_SIZE;

    // Initialize buffer from pool
    this.buffer = bufferPool.acquire(4096);
  }

  /**
   * Append incoming data to the buffer with backpressure handling
   */
  appendData(data: Buffer): void {
    const requiredSize = this.bufferOffset + data.length;

    // Grow buffer if needed
    if (requiredSize > this.buffer.length) {
      const newSize = Math.max(this.buffer.length * 2, requiredSize);
      const newBuffer = this.bufferPool.acquire(newSize);
      newBuffer.set(this.buffer.subarray(0, this.bufferOffset));
      this.bufferPool.release(this.buffer);
      this.buffer = newBuffer;
    }

    // Copy data to buffer
    this.buffer.set(new Uint8Array(data), this.bufferOffset);
    this.bufferOffset += data.length;

    // Check backpressure
    this.checkBackpressure();
  }

  /**
   * Check and apply backpressure if needed
   */
  private checkBackpressure(): void {
    if (this.bufferOffset >= this.backpressureConfig.highWaterMark && !this.isPaused) {
      this.isPaused = true;
      this.socket.pause();
      if (this.debug) {
        logger.debug('Backpressure: paused reading', { connectionId: this.connectionId, bufferSize: this.bufferOffset });
      }
    }
  }

  /**
   * Resume reading if buffer is below low water mark
   */
  private checkResume(): void {
    if (this.bufferOffset <= this.backpressureConfig.lowWaterMark && this.isPaused) {
      this.isPaused = false;
      this.socket.resume();
      if (this.debug) {
        logger.debug('Backpressure: resumed reading', { connectionId: this.connectionId, bufferSize: this.bufferOffset });
      }
    }
  }

  /**
   * Validate the message size from the header before processing
   * This is called early to prevent memory exhaustion from oversized messages
   *
   * @returns Validation result with error details if invalid
   */
  validateMessageSize(): MessageSizeValidationResult {
    // Need at least 16 bytes (header) to read message length
    if (this.bufferOffset < 16) {
      return { valid: true };
    }

    const view = new DataView(this.buffer.buffer, this.buffer.byteOffset);
    const messageLength = view.getInt32(0, true);

    // Check for invalid message length (negative or zero)
    if (messageLength <= 0) {
      return {
        valid: false,
        declaredSize: messageLength,
        error: new SizeLimitError(
          `Invalid message length: ${messageLength}`,
          10334,
          'InvalidLength',
          messageLength,
          this.maxMessageSize,
          'wireMessage'
        ),
      };
    }

    // Validate against size limits
    const validationResult = this.sizeLimitValidator.validateRequest(messageLength);
    if (!validationResult.valid && validationResult.error) {
      this.lastSizeValidationError = validationResult.error;
      return {
        valid: false,
        declaredSize: messageLength,
        error: validationResult.error,
      };
    }

    // Also check against our configured max message size
    if (messageLength > this.maxMessageSize) {
      const error = new SizeLimitError(
        `Message size ${messageLength} bytes exceeds maximum of ${this.maxMessageSize} bytes`,
        10338,
        'MessageTooLarge',
        messageLength,
        this.maxMessageSize,
        'wireMessage'
      );
      this.lastSizeValidationError = error;
      return {
        valid: false,
        declaredSize: messageLength,
        error,
      };
    }

    return { valid: true, declaredSize: messageLength };
  }

  /**
   * Get the last size validation error (if any)
   */
  getLastSizeValidationError(): SizeLimitError | null {
    return this.lastSizeValidationError;
  }

  /**
   * Clear the last size validation error
   */
  clearSizeValidationError(): void {
    this.lastSizeValidationError = null;
  }

  /**
   * Check if a complete message is available
   */
  hasCompleteMessage(): boolean {
    if (this.bufferOffset < 16) {
      return false;
    }
    const view = new DataView(this.buffer.buffer, this.buffer.byteOffset);
    const messageLength = view.getInt32(0, true);
    return this.bufferOffset >= messageLength;
  }

  /**
   * Check if there is an oversized message that should be rejected
   * This allows the caller to detect and handle oversized messages early
   */
  hasOversizedMessage(): boolean {
    if (this.bufferOffset < 16) {
      return false;
    }
    const view = new DataView(this.buffer.buffer, this.buffer.byteOffset);
    const messageLength = view.getInt32(0, true);
    return messageLength > this.maxMessageSize || messageLength <= 0;
  }

  /**
   * Get the declared message length from the header (if available)
   */
  getDeclaredMessageLength(): number | null {
    if (this.bufferOffset < 16) {
      return null;
    }
    const view = new DataView(this.buffer.buffer, this.buffer.byteOffset);
    return view.getInt32(0, true);
  }

  /**
   * Discard the current message from the buffer
   * Used when rejecting an oversized message
   */
  discardCurrentMessage(): void {
    if (this.bufferOffset < 16) {
      // Not enough data to read message length, clear everything
      this.bufferOffset = 0;
      return;
    }

    const view = new DataView(this.buffer.buffer, this.buffer.byteOffset);
    const messageLength = view.getInt32(0, true);

    if (messageLength <= 0 || messageLength > this.bufferOffset) {
      // Invalid or incomplete message, clear everything
      this.bufferOffset = 0;
      return;
    }

    // Shift remaining data to the front
    if (this.bufferOffset > messageLength) {
      this.buffer.copyWithin(0, messageLength, this.bufferOffset);
    }
    this.bufferOffset -= messageLength;
    this.lastSizeValidationError = null;
    this.checkResume();
  }

  /**
   * Extract a complete message from the buffer
   */
  extractMessage(): Uint8Array | null {
    if (this.bufferOffset < 16) {
      return null;
    }

    const view = new DataView(this.buffer.buffer, this.buffer.byteOffset);
    const messageLength = view.getInt32(0, true);

    if (this.bufferOffset < messageLength) {
      return null;
    }

    // Extract the message
    const message = this.buffer.slice(0, messageLength);

    // Shift remaining data to the front
    if (this.bufferOffset > messageLength) {
      this.buffer.copyWithin(0, messageLength, this.bufferOffset);
    }
    this.bufferOffset -= messageLength;

    // Check if we can resume reading
    this.checkResume();

    return message;
  }

  /**
   * Get current buffer content (for compatibility with existing code)
   */
  getBuffer(): Uint8Array {
    return this.buffer.subarray(0, this.bufferOffset);
  }

  /**
   * Set buffer content (for compatibility)
   */
  setBuffer(newBuffer: Uint8Array): void {
    if (newBuffer.length > this.buffer.length) {
      const grownBuffer = this.bufferPool.acquire(newBuffer.length);
      this.bufferPool.release(this.buffer);
      this.buffer = grownBuffer;
    }
    this.buffer.set(newBuffer);
    this.bufferOffset = newBuffer.length;
  }

  /**
   * Write data with backpressure awareness
   */
  async write(data: Uint8Array): Promise<boolean> {
    if (!this.socket.writable) {
      return false;
    }

    this.pendingWrites++;

    return new Promise((resolve) => {
      const canContinue = this.socket.write(Buffer.from(data), (err) => {
        this.pendingWrites--;
        if (err) {
          if (this.debug) {
            logger.error('Write error', { connectionId: this.connectionId, error: err });
          }
          resolve(false);
        } else {
          resolve(true);
        }
      });

      // If socket buffer is full, wait for drain
      if (!canContinue) {
        this.socket.once('drain', () => {
          if (this.debug) {
            logger.debug('Socket drained', { connectionId: this.connectionId });
          }
        });
      }
    });
  }

  /**
   * Wait for all pending writes to complete
   */
  async drain(): Promise<void> {
    const startTime = Date.now();
    while (this.pendingWrites > 0) {
      if (Date.now() - startTime > this.backpressureConfig.drainTimeout) {
        throw new Error(`Drain timeout after ${this.backpressureConfig.drainTimeout}ms`);
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }

  /**
   * Clean up resources
   */
  cleanup(): void {
    this.bufferPool.release(this.buffer);
    this.buffer = new Uint8Array(0);
    this.bufferOffset = 0;
  }
}

// ============================================================================
// Message Processing
// ============================================================================

/**
 * Connection state for message processing (internal use)
 */
interface ConnectionState {
  buffer: Uint8Array;
  connectionId: number;
  requestCounter: number;
}

/**
 * Process a complete wire protocol message
 */
async function processMessage(
  buffer: Uint8Array,
  state: ConnectionState,
  client: MongoLake,
  debug: boolean
): Promise<Uint8Array | null> {
  const parsed = parseMessage(buffer);

  if (debug) {
    logger.debug('Received message', { connectionId: state.connectionId, messageType: parsed.type, opCode: parsed.header.opCode });
  }

  if (parsed.type === 'OP_MSG') {
    const cmd = extractCommand(parsed.message);

    if (debug) {
      logger.debug('Executing command', { connectionId: state.connectionId, command: cmd.name, database: cmd.database, collection: cmd.collection });
    }

    const ctx: CommandContext = {
      client,
      requestId: parsed.header.requestId,
      connectionId: state.connectionId,
    };

    const result = await executeCommand(cmd, ctx);
    return result.response;
  }

  if (parsed.type === 'OP_QUERY') {
    return handleOpQuery(parsed.opQuery, state, client, debug);
  }

  // Unknown message type
  if (debug) {
    logger.warn('Unknown opCode received', { connectionId: state.connectionId, opCode: parsed.header.opCode });
  }

  return buildErrorResponse(
    state.requestCounter++,
    parsed.header.requestId,
    59,
    `Unsupported opCode: ${parsed.header.opCode}`,
    'CommandNotFound'
  );
}

/**
 * Handle legacy OP_QUERY messages
 */
async function handleOpQuery(
  opQuery: OpQueryMessage,
  state: ConnectionState,
  client: MongoLake,
  debug: boolean
): Promise<Uint8Array> {
  const { fullCollectionName, query, header } = opQuery;

  // Parse the collection name (format: "database.$cmd" or "database.collection")
  const [database, ...collectionParts] = fullCollectionName.split('.');
  const collectionName = collectionParts.join('.');

  if (debug) {
    logger.debug('Processing OP_QUERY', { connectionId: state.connectionId, fullCollectionName });
  }

  // Special handling for admin.$cmd queries (isMaster, etc.)
  if (collectionName === '$cmd' || collectionName === '$cmd.sys.inprog') {
    // Extract command from query document
    const commandName = Object.keys(query).find((k) => !k.startsWith('$'));

    if (commandName) {
      const cmd: ExtractedCommand = {
        name: commandName,
        database: database || 'admin',
        body: {
          ...query,
          $db: database || 'admin',
        },
      };

      const ctx: CommandContext = {
        client,
        requestId: header.requestId,
        connectionId: state.connectionId,
      };

      // For OP_QUERY, we need to return OP_REPLY
      const result = await executeCommand(cmd, ctx);

      // Extract the document from the OP_MSG response and wrap in OP_REPLY
      // This is a simplified approach - parse the response and re-wrap it
      const responseDoc = extractDocumentFromOpMsg(result.response);

      return buildOpReply(
        state.requestCounter++,
        header.requestId,
        [responseDoc]
      );
    }
  }

  // Handle regular collection queries
  if (collectionName && collectionName !== '$cmd') {
    const cmd: ExtractedCommand = {
      name: 'find',
      collection: collectionName,
      database: database || 'test',
      body: {
        find: collectionName,
        filter: query,
        $db: database || 'test',
      },
    };

    const ctx: CommandContext = {
      client,
      requestId: header.requestId,
      connectionId: state.connectionId,
    };

    const result = await executeCommand(cmd, ctx);
    const responseDoc = extractDocumentFromOpMsg(result.response);

    // For find queries via OP_QUERY, return documents directly
    const cursor = responseDoc.cursor as { firstBatch?: Document[] } | undefined;
    const docs = cursor?.firstBatch || [];

    return buildOpReply(state.requestCounter++, header.requestId, docs);
  }

  // Fallback: return empty result
  return buildOpReply(state.requestCounter++, header.requestId, [{ ok: 1 }]);
}

/**
 * Extract the document from an OP_MSG response
 */
function extractDocumentFromOpMsg(response: Uint8Array): Document {
  try {
    // Skip header (16 bytes) + flags (4 bytes) + section type (1 byte)
    const docStart = 21;
    const view = new DataView(response.buffer, response.byteOffset + docStart);
    const docSize = view.getInt32(0, true);

    // Parse the BSON document manually (simplified)
    return parseBsonFromBytes(response.slice(docStart, docStart + docSize));
  } catch {
    return { ok: 1 };
  }
}

/**
 * Parse BSON bytes back to a document (simplified for response extraction)
 */
function parseBsonFromBytes(bytes: Uint8Array): Document {
  const view = new DataView(bytes.buffer, bytes.byteOffset);
  const docSize = view.getInt32(0, true);
  const doc: Document = {};

  let pos = 4; // Skip size

  while (pos < docSize - 1) {
    const elementType = bytes[pos];
    pos++;

    if (elementType === 0x00) break;

    // Read key
    const keyStart = pos;
    while (bytes[pos] !== 0x00 && pos < docSize) pos++;
    const key = new TextDecoder().decode(bytes.slice(keyStart, pos));
    pos++; // Skip null

    // Read value based on type
    const valueView = new DataView(bytes.buffer, bytes.byteOffset + pos);

    switch (elementType) {
      case 0x01: // double
        doc[key] = valueView.getFloat64(0, true);
        pos += 8;
        break;
      case 0x02: // string
        const strLen = valueView.getInt32(0, true);
        pos += 4;
        doc[key] = new TextDecoder().decode(bytes.slice(pos, pos + strLen - 1));
        pos += strLen;
        break;
      case 0x03: // document
        const nestedSize = valueView.getInt32(0, true);
        doc[key] = parseBsonFromBytes(bytes.slice(pos, pos + nestedSize));
        pos += nestedSize;
        break;
      case 0x04: // array
        const arrSize = valueView.getInt32(0, true);
        const arrDoc = parseBsonFromBytes(bytes.slice(pos, pos + arrSize));
        doc[key] = Object.values(arrDoc);
        pos += arrSize;
        break;
      case 0x08: // boolean
        doc[key] = bytes[pos] !== 0x00;
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
      default:
        // Skip unknown types
        break;
    }
  }

  return doc;
}

// ============================================================================
// Server Factory - Helper Types
// ============================================================================

/**
 * Internal server configuration after normalization
 */
interface ServerConfig {
  port: number;
  host: string;
  debug: boolean;
  tlsConfig: TlsOptions | undefined;
  backpressureConfig: BackpressureConfig;
  bufferPoolConfig: BufferPoolConfig;
  shutdownConfig: Required<ShutdownConfig>;
  poolConfig: ConnectionPoolConfig;
  mongoLakeConfig: { local: string } | MongoLakeConfig;
  maxMessageSize: number;
  sizeLimitValidator: SizeLimitValidator;
}

/**
 * Internal server state for connection management
 */
interface ServerState {
  server: Server | TLSServer | null;
  pool: ConnectionPool | null;
  bufferPool: BufferPool | null;
  gracefulShutdownManager: GracefulShutdown;
  socketStateManagers: Map<number, SocketStateManager>;
  client: MongoLake;
}

// ============================================================================
// Server Factory - Helper Functions
// ============================================================================

/**
 * Initialize and normalize server configuration from options
 */
function initializeServerConfig(options: TcpServerOptions): ServerConfig {
  const debug = options.debug ?? false;
  const maxMessageSize = options.messageSize?.maxMessageSize ?? MAX_WIRE_MESSAGE_SIZE;
  const sizeLimitValidator = new SizeLimitValidator(options.messageSize?.sizeLimits);

  return {
    port: options.port ?? 27017,
    host: options.host ?? '127.0.0.1',
    debug,
    tlsConfig: options.tls,
    backpressureConfig: options.backpressure ?? {},
    bufferPoolConfig: options.bufferPool ?? {},
    shutdownConfig: {
      drainTimeout: options.shutdown?.drainTimeout ?? 30000,
      notifyClients: options.shutdown?.notifyClients ?? true,
      forceClose: options.shutdown?.forceClose ?? true,
    },
    poolConfig: {
      ...options.poolConfig,
      debug: options.poolConfig?.debug ?? debug,
    },
    mongoLakeConfig: options.mongoLakeConfig || { local: '.mongolake' },
    maxMessageSize,
    sizeLimitValidator,
  };
}

/**
 * Set up pool event handlers for logging and cleanup
 */
function setupPoolEventHandlers(
  pool: ConnectionPool,
  socketStateManagers: Map<number, SocketStateManager>,
  debug: boolean
): void {
  if (debug) {
    pool.on('connection:created', (conn) => {
      logger.debug('Pool connection created', { connectionId: conn.id });
    });

    pool.on('connection:acquired', (conn) => {
      logger.debug('Pool connection acquired', { connectionId: conn.id, reuseCount: conn.reuseCount });
    });

    pool.on('connection:released', (conn) => {
      logger.debug('Pool connection released', { connectionId: conn.id });
    });

    pool.on('connection:error', (id, error) => {
      logger.error('Pool connection error', { connectionId: id, error: error.message });
    });

    pool.on('pool:exhausted', () => {
      logger.warn('Connection pool exhausted');
    });

    pool.on('pool:available', () => {
      logger.debug('Connection pool has availability');
    });
  }

  // Clean up socket state manager when connection is destroyed
  pool.on('connection:destroyed', (id, reason) => {
    const stateManager = socketStateManagers.get(id);
    if (stateManager) {
      stateManager.cleanup();
      socketStateManagers.delete(id);
    }
    if (debug) {
      logger.debug('Pool connection destroyed', { connectionId: id, reason });
    }
  });
}

/**
 * Set up socket event handlers for a connection
 */
function setupSocketEventHandlers(
  socket: Socket | TLSSocket,
  connection: PooledConnection,
  stateManager: SocketStateManager,
  state: ServerState,
  config: ServerConfig
): void {
  socket.on('data', (data) => {
    // Update last used timestamp
    connection.lastUsedAt = Date.now();

    // Append data using state manager (handles backpressure)
    stateManager.appendData(data);

    // Process complete messages
    processMessagesLoop(socket, stateManager, connection, state.client, config.debug).catch((error) => {
      if (config.debug) {
        logger.error('Message processing error', { connectionId: connection.id, error });
      }
      state.pool?.handleError(connection.id, error instanceof Error ? error : new Error(String(error)));
    });
  });

  socket.on('error', (error) => {
    if (config.debug) {
      logger.error('Socket error', { connectionId: connection.id, error });
    }
    state.pool?.handleError(connection.id, error);
  });

  socket.on('close', () => {
    if (config.debug) {
      logger.info('Client disconnected', { connectionId: connection.id });
    }
    // Unregister from graceful shutdown manager
    state.gracefulShutdownManager.unregisterConnection(socket);
    // Remove connection from pool on close (triggers cleanup via event)
    state.pool?.remove(connection.id, 'client disconnected');
  });

  socket.on('end', () => {
    // Client initiated graceful close - release connection back to pool
    state.pool?.release(connection.id);
  });
}

/**
 * Create a connection handler function
 */
function createConnectionHandler(
  state: ServerState,
  config: ServerConfig
): (socket: Socket | TLSSocket) => void {
  return (socket: Socket | TLSSocket): void => {
    // Reject connections during shutdown or if pool is not initialized
    if (state.gracefulShutdownManager.shuttingDown || !state.pool) {
      socket.end();
      return;
    }

    // Register connection with graceful shutdown manager
    state.gracefulShutdownManager.registerConnection(socket);

    // Check if pool has capacity
    if (!state.pool.hasCapacity()) {
      if (config.debug) {
        logger.warn('Rejecting connection - pool at capacity');
      }
      socket.end();
      return;
    }

    // Add connection to the pool
    const connection = state.pool.addConnection(socket as Socket);

    // Create socket state manager for this connection
    const stateManager = new SocketStateManager(
      connection.id,
      socket,
      state.bufferPool!,
      config.backpressureConfig,
      config.debug,
      config.sizeLimitValidator,
      config.maxMessageSize
    );
    state.socketStateManagers.set(connection.id, stateManager);

    if (config.debug) {
      const protocol = config.tlsConfig?.enabled ? 'TLS' : 'TCP';
      logger.info('Client connected', { connectionId: connection.id, protocol, remoteAddress: socket.remoteAddress, remotePort: socket.remotePort });
    }

    // Set up socket event handlers
    setupSocketEventHandlers(socket, connection, stateManager, state, config);
  };
}

/**
 * Process all complete messages in the state manager's buffer
 */
async function processMessagesLoop(
  socket: Socket | TLSSocket,
  stateManager: SocketStateManager,
  _connection: PooledConnection, // Reserved for future metrics/tracking
  mongoClient: MongoLake,
  enableDebug: boolean
): Promise<void> {
  // First, check for oversized messages before attempting to process
  const sizeValidation = stateManager.validateMessageSize();
  if (!sizeValidation.valid && sizeValidation.error) {
    if (enableDebug) {
      logger.warn('Rejecting oversized message', {
        connectionId: stateManager.connectionId,
        declaredSize: sizeValidation.declaredSize,
        maxSize: sizeValidation.error.maxSize,
      });
    }

    // Build and send error response for oversized message
    // Use requestCounter as requestId since we can't parse the actual requestId from the full message
    const errorResponse = sizeValidation.error.toErrorResponse(
      stateManager.requestCounter++,
      0 // responseTo: 0 since we don't have the original requestId
    );

    if (socket.writable) {
      await stateManager.write(errorResponse);
    }

    // Discard the oversized message data to prevent memory exhaustion
    // We need to discard data up to the declared message length (or clear buffer if we can't)
    stateManager.discardCurrentMessage();
    stateManager.clearSizeValidationError();
    return;
  }

  while (stateManager.hasCompleteMessage()) {
    const messageBuffer = stateManager.extractMessage();
    if (!messageBuffer) break;

    try {
      const response = await processMessage(
        messageBuffer,
        {
          buffer: stateManager.getBuffer(),
          connectionId: stateManager.connectionId,
          requestCounter: stateManager.requestCounter,
        },
        mongoClient,
        enableDebug
      );

      if (response && socket.writable) {
        await stateManager.write(response);
      }
    } catch (error) {
      if (enableDebug) {
        logger.error('Error processing message', { connectionId: stateManager.connectionId, error });
      }

      const header = parseMessageHeader(messageBuffer);
      const errorResponse = buildErrorResponse(
        stateManager.requestCounter++,
        header.requestId,
        1,
        error instanceof Error ? error.message : String(error),
        'InternalError'
      );

      if (socket.writable) {
        await stateManager.write(errorResponse);
      }
    }
  }
}

/**
 * Drain all socket state managers with timeout
 */
async function drainSocketStateManagers(
  socketStateManagers: Map<number, SocketStateManager>,
  drainTimeout: number,
  debug: boolean
): Promise<void> {
  const drainPromises: Promise<void>[] = [];
  for (const [id, stateManager] of socketStateManagers) {
    drainPromises.push(
      (async () => {
        try {
          await stateManager.drain();
        } catch (error) {
          if (debug) {
            logger.warn('Connection drain failed', { connectionId: id, error });
          }
        }
      })()
    );
  }

  // Wait for drain with timeout
  await Promise.race([
    Promise.all(drainPromises),
    new Promise<void>((resolve) => setTimeout(resolve, drainTimeout)),
  ]);
}

/**
 * Perform graceful shutdown with connection draining
 */
async function performGracefulShutdown(
  state: ServerState,
  config: ServerConfig
): Promise<void> {
  if (config.debug) {
    logger.info('Starting graceful shutdown', { activeConnections: state.gracefulShutdownManager.connectionCount });
  }

  const startTime = Date.now();
  const totalTimeout = config.shutdownConfig.drainTimeout;

  // Drain socket state managers before shutdown (use 1/3 of timeout)
  const drainTimeout = Math.floor(totalTimeout / 3);
  await drainSocketStateManagers(state.socketStateManagers, drainTimeout, config.debug);

  // Calculate remaining time for graceful shutdown
  const elapsed = Date.now() - startTime;
  const remainingTimeout = Math.max(100, totalTimeout - elapsed);

  // Use GracefulShutdown class for main shutdown logic with remaining time
  await state.gracefulShutdownManager.shutdown(state.server, remainingTimeout);

  if (config.debug) {
    const totalElapsed = Date.now() - startTime;
    logger.debug('Drain completed', { durationMs: totalElapsed });
  }

  // Shutdown the connection pool
  if (state.pool) {
    await state.pool.shutdown();
    state.pool = null;
  }

  // Clean up buffer pool
  if (state.bufferPool) {
    state.bufferPool.clear();
    state.bufferPool = null;
  }

  // Clear socket state managers
  state.socketStateManagers.clear();

  // Server is already closed by gracefulShutdownManager.shutdown()
  state.server = null;

  if (config.debug) {
    logger.info('Server shutdown complete');
  }
}

/**
 * Build TLS options from configuration
 */
async function buildTlsOptions(
  tlsConfig: TlsOptions
): Promise<SecureContextOptions & { requestCert?: boolean; rejectUnauthorized?: boolean }> {
  const tlsOptions: SecureContextOptions & { requestCert?: boolean; rejectUnauthorized?: boolean } = {};

  if (tlsConfig.cert) {
    // Check if it's a path or content
    if (tlsConfig.cert.includes('-----BEGIN')) {
      tlsOptions.cert = tlsConfig.cert;
    } else {
      const fs = await import('node:fs');
      tlsOptions.cert = fs.readFileSync(tlsConfig.cert);
    }
  }

  if (tlsConfig.key) {
    if (tlsConfig.key.includes('-----BEGIN')) {
      tlsOptions.key = tlsConfig.key;
    } else {
      const fs = await import('node:fs');
      tlsOptions.key = fs.readFileSync(tlsConfig.key);
    }
  }

  if (tlsConfig.ca) {
    if (tlsConfig.ca.includes('-----BEGIN')) {
      tlsOptions.ca = tlsConfig.ca;
    } else {
      const fs = await import('node:fs');
      tlsOptions.ca = fs.readFileSync(tlsConfig.ca);
    }
  }

  if (tlsConfig.passphrase) {
    tlsOptions.passphrase = tlsConfig.passphrase;
  }

  if (tlsConfig.requestCert !== undefined) {
    tlsOptions.requestCert = tlsConfig.requestCert;
  }

  if (tlsConfig.rejectUnauthorized !== undefined) {
    tlsOptions.rejectUnauthorized = tlsConfig.rejectUnauthorized;
  }

  return tlsOptions;
}

/**
 * Create and start a TLS server
 */
async function createTlsServer(
  state: ServerState,
  config: ServerConfig,
  handleConnection: (socket: TLSSocket) => void
): Promise<void> {
  const tls = await import('node:tls');
  const tlsOptions = await buildTlsOptions(config.tlsConfig!);

  return new Promise((resolve, reject) => {
    state.server = tls.createServer(tlsOptions, handleConnection);

    state.server.on('error', reject);
    state.server.on('tlsClientError', (error) => {
      if (config.debug) {
        logger.error('TLS client error', { error });
      }
    });

    state.server.listen(config.port, config.host, () => {
      if (config.debug && state.pool) {
        const poolCfg = state.pool.getConfig();
        logger.info('MongoLake TLS server listening', { host: config.host, port: config.port, maxConnections: poolCfg.maxConnections, minConnections: poolCfg.minConnections, idleTimeoutMs: poolCfg.idleTimeout });
      }
      resolve();
    });
  });
}

/**
 * Create and start a plain TCP server
 */
async function createTcpServer(
  state: ServerState,
  config: ServerConfig,
  handleConnection: (socket: Socket) => void
): Promise<void> {
  const net = await import('node:net');

  return new Promise((resolve, reject) => {
    state.server = net.createServer(handleConnection);

    state.server.on('error', reject);

    state.server.listen(config.port, config.host, () => {
      if (config.debug && state.pool) {
        const poolCfg = state.pool.getConfig();
        logger.info('MongoLake server listening', { host: config.host, port: config.port, maxConnections: poolCfg.maxConnections, minConnections: poolCfg.minConnections, idleTimeoutMs: poolCfg.idleTimeout });
      }
      resolve();
    });
  });
}

// ============================================================================
// Server Factory
// ============================================================================

/**
 * Create a MongoDB wire protocol TCP server with connection pooling
 * Supports TLS, backpressure handling, buffer pooling, and graceful shutdown
 */
export function createServer(options: TcpServerOptions = {}): TcpServer {
  // Initialize configuration
  const config = initializeServerConfig(options);

  // Initialize server state
  const state: ServerState = {
    server: null,
    pool: null,
    bufferPool: null,
    gracefulShutdownManager: new GracefulShutdown(),
    socketStateManagers: new Map<number, SocketStateManager>(),
    client: new MongoLake(config.mongoLakeConfig),
  };

  // Create connection handler
  const handleConnection = createConnectionHandler(state, config);

  return {
    async start(): Promise<void> {
      // Initialize buffer pool
      state.bufferPool = new BufferPool(config.bufferPoolConfig);

      // Initialize connection pool
      state.pool = new ConnectionPool(config.poolConfig);
      setupPoolEventHandlers(state.pool, state.socketStateManagers, config.debug);

      // Create server (TLS or plain TCP)
      if (config.tlsConfig?.enabled) {
        await createTlsServer(state, config, handleConnection as (socket: TLSSocket) => void);
      } else {
        await createTcpServer(state, config, handleConnection as (socket: Socket) => void);
      }
    },

    async stop(): Promise<void> {
      await performGracefulShutdown(state, config);
    },

    address(): { port: number; host: string } | null {
      if (state.server) {
        const addr = state.server.address();
        if (addr && typeof addr === 'object') {
          return { port: addr.port, host: addr.address };
        }
      }
      return null;
    },

    getPool(): ConnectionPool | null {
      return state.pool;
    },

    getMetrics(): PoolMetrics | null {
      return state.pool?.getMetrics() ?? null;
    },

    isTlsEnabled(): boolean {
      return config.tlsConfig?.enabled ?? false;
    },

    getBufferPoolStats(): BufferPoolStats | null {
      return state.bufferPool?.getStats() ?? null;
    },
  };
}

// ============================================================================
// CLI Entry Point
// ============================================================================

/** Default timeout for graceful shutdown in milliseconds */
const DEFAULT_SHUTDOWN_TIMEOUT = 30000;

/**
 * Start the server from command line
 */
export async function main(): Promise<void> {
  const port = parseInt(process.env.MONGOLAKE_PORT || '27017', 10);
  const host = process.env.MONGOLAKE_HOST || '127.0.0.1';
  const dataDir = process.env.MONGOLAKE_DATA || '.mongolake';
  const debug = process.env.MONGOLAKE_DEBUG === 'true';
  const shutdownTimeout = parseInt(process.env.MONGOLAKE_SHUTDOWN_TIMEOUT || String(DEFAULT_SHUTDOWN_TIMEOUT), 10);

  const server = createServer({
    port,
    host,
    debug,
    mongoLakeConfig: { local: dataDir },
    shutdown: {
      drainTimeout: shutdownTimeout,
      notifyClients: true,
      forceClose: true,
    },
  });

  logger.info('Starting MongoLake server', { port, host, dataDir });

  await server.start();

  logger.info('MongoLake is ready', { connectionString: `mongodb://${host}:${port}` });

  // Create a graceful shutdown handler for the CLI
  const cliShutdownHandler = new GracefulShutdown();

  /**
   * Perform graceful shutdown with timeout
   * @param signal - The signal that triggered the shutdown
   */
  async function handleShutdown(signal: string): Promise<void> {
    // Prevent multiple shutdown attempts by checking shutdown state
    if (cliShutdownHandler.shuttingDown) {
      logger.debug('Shutdown already in progress, ignoring signal', { signal });
      return;
    }

    logger.info('Received signal, initiating graceful shutdown', { signal });

    // Set up force exit timeout - only used as last resort
    const forceExitTimer = setTimeout(() => {
      logger.error('Shutdown timeout exceeded, forcing exit', { timeoutMs: shutdownTimeout });
      // Use process.exitCode instead of process.exit() where possible
      process.exitCode = 1;
      // Force exit only after timeout as last resort
      setImmediate(() => process.exit(1));
    }, shutdownTimeout);

    // Make sure the timer doesn't prevent the process from exiting naturally
    forceExitTimer.unref();

    try {
      // Use the server's graceful shutdown
      await server.stop();
      clearTimeout(forceExitTimer);
      logger.info('Graceful shutdown completed');
      // Set exit code and let the process exit naturally
      process.exitCode = 0;
    } catch (error) {
      clearTimeout(forceExitTimer);
      logger.error('Error during shutdown', { error });
      process.exitCode = 1;
    }
  }

  // Handle graceful shutdown signals
  process.on('SIGINT', () => {
    handleShutdown('SIGINT').catch((error) => {
      logger.error('Unhandled error during SIGINT shutdown', { error });
      process.exitCode = 1;
    });
  });

  process.on('SIGTERM', () => {
    handleShutdown('SIGTERM').catch((error) => {
      logger.error('Unhandled error during SIGTERM shutdown', { error });
      process.exitCode = 1;
    });
  });
}

// Run if executed directly
if (process.argv[1]?.endsWith('tcp-server.ts') || process.argv[1]?.endsWith('tcp-server.js')) {
  main().catch((error) => {
    logger.error('Failed to start server', { error });
    process.exit(1);
  });
}
