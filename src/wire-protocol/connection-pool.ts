/**
 * Connection Pool for MongoDB Wire Protocol TCP Server
 *
 * Manages a pool of client connections with:
 * - Maximum connection limits
 * - Idle connection timeout
 * - Connection reuse tracking
 * - Health check pings
 * - Metrics for monitoring
 */

import type { Socket } from 'node:net';
import { EventEmitter } from 'node:events';
import { logger } from '../utils/logger.js';

// ============================================================================
// Types
// ============================================================================

export interface ConnectionPoolConfig {
  /** Maximum number of connections in the pool (default: 100) */
  maxConnections?: number;

  /** Minimum number of idle connections to maintain (default: 10) */
  minConnections?: number;

  /** Close idle connections after this duration in ms (default: 30000) */
  idleTimeout?: number;

  /** Maximum time to wait for an available connection in ms (default: 5000) */
  acquireTimeout?: number;

  /** Interval for health check pings in ms (default: 10000) */
  healthCheckInterval?: number;

  /** Interval for idle connection cleanup check in ms (default: 1000) */
  idleCheckInterval?: number;

  /** Enable debug logging */
  debug?: boolean;
}

export interface PooledConnection {
  /** Unique connection identifier */
  id: number;

  /** The underlying socket */
  socket: Socket;

  /** Whether the connection is currently in use */
  inUse: boolean;

  /** Timestamp when the connection was created */
  createdAt: number;

  /** Timestamp when the connection was last used */
  lastUsedAt: number;

  /** Number of times this connection has been reused */
  reuseCount: number;

  /** Connection state buffer for partial message handling */
  buffer: Uint8Array;

  /** Request counter for this connection */
  requestCounter: number;

  /** Whether the connection is healthy */
  healthy: boolean;
}

export interface PoolMetrics {
  /** Total number of connections created */
  totalCreated: number;

  /** Total number of connections destroyed */
  totalDestroyed: number;

  /** Current number of active (in-use) connections */
  activeConnections: number;

  /** Current number of idle connections */
  idleConnections: number;

  /** Total number of connections in the pool */
  totalConnections: number;

  /** Number of times a connection was acquired */
  acquireCount: number;

  /** Number of times a connection was released */
  releaseCount: number;

  /** Number of acquire timeouts */
  acquireTimeouts: number;

  /** Number of connections closed due to idle timeout */
  idleTimeoutCount: number;

  /** Number of connections closed due to errors */
  errorCount: number;
}

export interface ConnectionPoolEvents {
  'connection:created': (connection: PooledConnection) => void;
  'connection:destroyed': (connectionId: number, reason: string) => void;
  'connection:acquired': (connection: PooledConnection) => void;
  'connection:released': (connection: PooledConnection) => void;
  'connection:error': (connectionId: number, error: Error) => void;
  'pool:exhausted': () => void;
  'pool:available': () => void;
}

// ============================================================================
// Connection Pool Implementation
// ============================================================================

export class ConnectionPool extends EventEmitter {
  private readonly config: Required<ConnectionPoolConfig>;
  private readonly connections: Map<number, PooledConnection> = new Map();
  private readonly waitQueue: Array<{
    resolve: (conn: PooledConnection) => void;
    reject: (err: Error) => void;
    timeoutId: ReturnType<typeof setTimeout>;
  }> = [];

  private connectionIdCounter = 0;
  private idleCheckTimer: ReturnType<typeof setInterval> | null = null;
  private healthCheckTimer: ReturnType<typeof setInterval> | null = null;
  private isShuttingDown = false;

  private metrics: PoolMetrics = {
    totalCreated: 0,
    totalDestroyed: 0,
    activeConnections: 0,
    idleConnections: 0,
    totalConnections: 0,
    acquireCount: 0,
    releaseCount: 0,
    acquireTimeouts: 0,
    idleTimeoutCount: 0,
    errorCount: 0,
  };

  constructor(config: ConnectionPoolConfig = {}) {
    super();

    this.config = {
      maxConnections: config.maxConnections ?? 100,
      minConnections: config.minConnections ?? 10,
      idleTimeout: config.idleTimeout ?? 30000,
      acquireTimeout: config.acquireTimeout ?? 5000,
      healthCheckInterval: config.healthCheckInterval ?? 10000,
      idleCheckInterval: config.idleCheckInterval ?? 1000,
      debug: config.debug ?? false,
    };

    // Start background timers
    this.startIdleCheck();
    this.startHealthCheck();
  }

  // --------------------------------------------------------------------------
  // Public API
  // --------------------------------------------------------------------------

  /**
   * Register a new socket connection with the pool
   */
  addConnection(socket: Socket): PooledConnection {
    if (this.isShuttingDown) {
      socket.destroy();
      throw new Error('Connection pool is shutting down');
    }

    const connectionId = ++this.connectionIdCounter;
    const now = Date.now();

    const connection: PooledConnection = {
      id: connectionId,
      socket,
      inUse: true, // New connections are immediately in use
      createdAt: now,
      lastUsedAt: now,
      reuseCount: 0,
      buffer: new Uint8Array(0),
      requestCounter: 1,
      healthy: true,
    };

    this.connections.set(connectionId, connection);
    this.updateMetrics();

    this.metrics.totalCreated++;

    if (this.config.debug) {
      logger.debug('Pool connection created', { connectionId, totalConnections: this.connections.size });
    }

    this.emit('connection:created', connection);

    return connection;
  }

  /**
   * Acquire an idle connection from the pool or wait for one
   */
  async acquire(timeoutMs?: number): Promise<PooledConnection> {
    const timeout = timeoutMs ?? this.config.acquireTimeout;

    // First, try to get an idle connection
    const idle = this.getIdleConnection();
    if (idle) {
      idle.inUse = true;
      idle.lastUsedAt = Date.now();
      idle.reuseCount++;
      this.metrics.acquireCount++;
      this.updateMetrics();

      if (this.config.debug) {
        logger.debug('Pool connection acquired', { connectionId: idle.id, reuseCount: idle.reuseCount });
      }

      this.emit('connection:acquired', idle);
      return idle;
    }

    // Check if we're at capacity
    if (this.connections.size >= this.config.maxConnections) {
      // Wait for an available connection
      return this.waitForConnection(timeout);
    }

    // We have capacity but no idle connections - caller should create new connection
    throw new Error('No idle connections available; create a new connection');
  }

  /**
   * Release a connection back to the pool
   */
  release(connectionId: number): void {
    const connection = this.connections.get(connectionId);
    if (!connection) {
      if (this.config.debug) {
        logger.warn('Attempted to release unknown connection', { connectionId });
      }
      return;
    }

    connection.inUse = false;
    connection.lastUsedAt = Date.now();
    this.metrics.releaseCount++;
    this.updateMetrics();

    if (this.config.debug) {
      logger.debug('Pool connection released', { connectionId });
    }

    this.emit('connection:released', connection);

    // Check if anyone is waiting for a connection
    this.processWaitQueue();
  }

  /**
   * Remove a connection from the pool
   */
  remove(connectionId: number, reason: string = 'removed'): void {
    const connection = this.connections.get(connectionId);
    if (!connection) {
      return;
    }

    this.connections.delete(connectionId);
    this.metrics.totalDestroyed++;
    this.updateMetrics();

    // Destroy the socket if it's still connected
    if (!connection.socket.destroyed) {
      connection.socket.destroy();
    }

    if (this.config.debug) {
      logger.debug('Pool connection destroyed', { connectionId, reason });
    }

    this.emit('connection:destroyed', connectionId, reason);
  }

  /**
   * Handle a connection error
   */
  handleError(connectionId: number, error: Error): void {
    const connection = this.connections.get(connectionId);
    if (!connection) {
      return;
    }

    connection.healthy = false;
    this.metrics.errorCount++;

    if (this.config.debug) {
      logger.error('Pool connection error', { connectionId, error: error.message });
    }

    this.emit('connection:error', connectionId, error);

    // Remove unhealthy connection
    this.remove(connectionId, `error: ${error.message}`);
  }

  /**
   * Get a connection by ID
   */
  get(connectionId: number): PooledConnection | undefined {
    return this.connections.get(connectionId);
  }

  /**
   * Check if the pool has capacity for more connections
   */
  hasCapacity(): boolean {
    return this.connections.size < this.config.maxConnections;
  }

  /**
   * Get current pool metrics
   */
  getMetrics(): Readonly<PoolMetrics> {
    return { ...this.metrics };
  }

  /**
   * Get pool configuration
   */
  getConfig(): Readonly<Required<ConnectionPoolConfig>> {
    return { ...this.config };
  }

  /**
   * Shutdown the connection pool
   */
  async shutdown(): Promise<void> {
    this.isShuttingDown = true;

    if (this.config.debug) {
      logger.info('Connection pool shutting down');
    }

    // Stop background timers
    if (this.idleCheckTimer) {
      clearInterval(this.idleCheckTimer);
      this.idleCheckTimer = null;
    }

    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
      this.healthCheckTimer = null;
    }

    // Reject all waiting acquires
    for (const waiter of this.waitQueue) {
      clearTimeout(waiter.timeoutId);
      waiter.reject(new Error('Connection pool is shutting down'));
    }
    this.waitQueue.length = 0;

    // Close all connections
    const closePromises: Promise<void>[] = [];
    for (const [id, connection] of this.connections) {
      closePromises.push(
        new Promise<void>((resolve) => {
          if (connection.socket.destroyed) {
            resolve();
            return;
          }

          connection.socket.once('close', () => resolve());
          connection.socket.end();

          // Force destroy after a short timeout
          setTimeout(() => {
            if (!connection.socket.destroyed) {
              connection.socket.destroy();
            }
            resolve();
          }, 1000);
        })
      );
      this.connections.delete(id);
    }

    await Promise.all(closePromises);

    if (this.config.debug) {
      logger.info('Connection pool shutdown complete');
    }
  }

  // --------------------------------------------------------------------------
  // Private Methods
  // --------------------------------------------------------------------------

  private getIdleConnection(): PooledConnection | null {
    for (const connection of this.connections.values()) {
      if (!connection.inUse && connection.healthy) {
        return connection;
      }
    }
    return null;
  }

  private waitForConnection(timeoutMs: number): Promise<PooledConnection> {
    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        // Remove from wait queue
        const index = this.waitQueue.findIndex((w) => w.timeoutId === timeoutId);
        if (index !== -1) {
          this.waitQueue.splice(index, 1);
        }

        this.metrics.acquireTimeouts++;

        if (this.config.debug) {
          logger.warn('Pool acquire timeout', { timeoutMs });
        }

        this.emit('pool:exhausted');
        reject(new Error(`Acquire timeout after ${timeoutMs}ms - pool exhausted`));
      }, timeoutMs);

      this.waitQueue.push({ resolve, reject, timeoutId });

      if (this.config.debug) {
        logger.debug('Waiting for connection', { queueLength: this.waitQueue.length });
      }
    });
  }

  private processWaitQueue(): void {
    if (this.waitQueue.length === 0) {
      return;
    }

    const idle = this.getIdleConnection();
    if (!idle) {
      return;
    }

    const waiter = this.waitQueue.shift();
    if (!waiter) {
      return;
    }

    clearTimeout(waiter.timeoutId);

    idle.inUse = true;
    idle.lastUsedAt = Date.now();
    idle.reuseCount++;
    this.metrics.acquireCount++;
    this.updateMetrics();

    if (this.config.debug) {
      logger.debug('Pool connection acquired from wait queue', { connectionId: idle.id, reuseCount: idle.reuseCount });
    }

    this.emit('connection:acquired', idle);
    this.emit('pool:available');
    waiter.resolve(idle);
  }

  private startIdleCheck(): void {
    // Check for idle connections periodically
    this.idleCheckTimer = setInterval(() => {
      this.checkIdleConnections();
    }, this.config.idleCheckInterval);

    // Ensure the timer doesn't prevent process exit
    if (this.idleCheckTimer.unref) {
      this.idleCheckTimer.unref();
    }
  }

  private checkIdleConnections(): void {
    const now = Date.now();
    const idleThreshold = now - this.config.idleTimeout;

    // Collect all idle connections that have exceeded the idle timeout
    const expiredIdle: Array<{ id: number; lastUsedAt: number }> = [];
    let totalIdleCount = 0;

    for (const [id, connection] of this.connections) {
      if (!connection.inUse) {
        totalIdleCount++;

        // Check if idle too long
        if (connection.lastUsedAt < idleThreshold) {
          expiredIdle.push({ id, lastUsedAt: connection.lastUsedAt });
        }
      }
    }

    // Sort by lastUsedAt (oldest first) to remove oldest idle connections first
    expiredIdle.sort((a, b) => a.lastUsedAt - b.lastUsedAt);

    // Calculate how many we can remove while staying above minConnections
    const canRemove = Math.max(0, totalIdleCount - this.config.minConnections);
    const toRemove = expiredIdle.slice(0, canRemove);

    // Remove excess idle connections
    for (const { id } of toRemove) {
      this.metrics.idleTimeoutCount++;
      this.remove(id, 'idle timeout');
    }
  }

  private startHealthCheck(): void {
    this.healthCheckTimer = setInterval(() => {
      this.performHealthCheck();
    }, this.config.healthCheckInterval);

    // Ensure the timer doesn't prevent process exit
    if (this.healthCheckTimer.unref) {
      this.healthCheckTimer.unref();
    }
  }

  private performHealthCheck(): void {
    for (const [id, connection] of this.connections) {
      // Skip connections that are in use or already unhealthy
      if (connection.inUse || !connection.healthy) {
        continue;
      }

      // Check if socket is still connected
      if (connection.socket.destroyed || !connection.socket.writable) {
        connection.healthy = false;
        this.remove(id, 'health check failed - socket not writable');
      }
    }
  }

  private updateMetrics(): void {
    let active = 0;
    let idle = 0;

    for (const connection of this.connections.values()) {
      if (connection.inUse) {
        active++;
      } else {
        idle++;
      }
    }

    this.metrics.activeConnections = active;
    this.metrics.idleConnections = idle;
    this.metrics.totalConnections = this.connections.size;
  }
}

// ============================================================================
// Typed Event Emitter Helpers
// ============================================================================

// Re-export for type-safe event handling
export type ConnectionPoolEventMap = {
  [K in keyof ConnectionPoolEvents]: ConnectionPoolEvents[K];
};
