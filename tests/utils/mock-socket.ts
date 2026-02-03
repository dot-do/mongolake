/**
 * Mock Socket Implementation
 *
 * Type-safe mock implementation of Node.js Socket for testing connection pools
 * and networking code. Implements the subset of Socket interface used by
 * ConnectionPool and related components.
 *
 * @example
 * ```ts
 * import { createMockSocket, MockSocket } from '../utils/mock-socket.js';
 *
 * // Create a basic mock socket
 * const socket = createMockSocket();
 *
 * // Create with custom properties
 * const customSocket = createMockSocket({
 *   remoteAddress: '192.168.1.1',
 *   remotePort: 54321,
 * });
 *
 * // Use in tests
 * expect(socket.write(Buffer.from('test'))).toBe(true);
 * socket.destroy();
 * expect(socket.destroyed).toBe(true);
 * ```
 */

import { EventEmitter } from 'node:events';
import type { Socket } from 'node:net';
import { vi, type MockedFunction } from 'vitest';

// ============================================================================
// Types
// ============================================================================

/**
 * Interface representing the subset of Socket properties used by ConnectionPool.
 * This allows for proper typing without requiring full Socket implementation.
 */
export interface MockSocket {
  /** Whether the socket has been destroyed */
  destroyed: boolean;

  /** Whether the socket is writable */
  writable: boolean;

  /** Remote address of the connection */
  remoteAddress?: string;

  /** Remote port of the connection */
  remotePort?: number;

  /** Destroy the socket */
  destroy(): void;

  /** End the socket connection gracefully */
  end(): void;

  /** Write data to the socket */
  write(data: Buffer | Uint8Array): boolean;

  /** Register a one-time event listener */
  once(event: string, listener: (...args: unknown[]) => void): this;

  /** Emit an event */
  emit(event: string, ...args: unknown[]): boolean;

  /** Register an event listener */
  on(event: string, listener: (...args: unknown[]) => void): this;

  /** Remove all listeners for an event */
  removeAllListeners(event?: string): this;
}

/**
 * Options for creating a mock socket.
 */
export interface MockSocketOptions {
  /** Initial remote address (default: '127.0.0.1') */
  remoteAddress?: string;

  /** Initial remote port (default: 12345) */
  remotePort?: number;

  /** Initial destroyed state (default: false) */
  destroyed?: boolean;

  /** Initial writable state (default: true) */
  writable?: boolean;

  /** Custom write behavior - return false to simulate write failure */
  writeReturnValue?: boolean;

  /** Delay in ms before emitting 'close' on end() (default: 10) */
  endDelay?: number;
}

/**
 * Mock socket with spy functions for assertion.
 */
export interface SpiedMockSocket extends MockSocket {
  /** Spy for destroy() calls */
  destroy: MockedFunction<() => void>;

  /** Spy for end() calls */
  end: MockedFunction<() => void>;

  /** Spy for write() calls */
  write: MockedFunction<(data: Buffer | Uint8Array) => boolean>;

  /** Spy for once() calls */
  once: MockedFunction<(event: string, listener: (...args: unknown[]) => void) => SpiedMockSocket>;

  /** Spy for on() calls */
  on: MockedFunction<(event: string, listener: (...args: unknown[]) => void) => SpiedMockSocket>;

  /** Spy for removeAllListeners() calls */
  removeAllListeners: MockedFunction<(event?: string) => SpiedMockSocket>;
}

// ============================================================================
// Implementation
// ============================================================================

/**
 * Mock implementation of Node.js Socket for testing connection pools and networking code.
 * Extends EventEmitter to properly handle event registration and emission.
 */
export class MockSocketImpl extends EventEmitter implements MockSocket {
  destroyed = false;
  writable = true;
  remoteAddress = '127.0.0.1';
  remotePort = 12345;

  private writeReturnValue = true;
  private endDelay = 10;

  constructor(options?: MockSocketOptions) {
    super();

    if (options?.remoteAddress !== undefined) {
      this.remoteAddress = options.remoteAddress;
    }
    if (options?.remotePort !== undefined) {
      this.remotePort = options.remotePort;
    }
    if (options?.destroyed !== undefined) {
      this.destroyed = options.destroyed;
    }
    if (options?.writable !== undefined) {
      this.writable = options.writable;
    }
    if (options?.writeReturnValue !== undefined) {
      this.writeReturnValue = options.writeReturnValue;
    }
    if (options?.endDelay !== undefined) {
      this.endDelay = options.endDelay;
    }
  }

  destroy(): void {
    this.destroyed = true;
    this.writable = false;
    this.emit('close');
  }

  end(): void {
    this.writable = false;
    setTimeout(() => {
      this.destroyed = true;
      this.emit('close');
    }, this.endDelay);
  }

  write(_data: Buffer | Uint8Array): boolean {
    if (!this.writable || this.destroyed) {
      return false;
    }
    return this.writeReturnValue;
  }
}

// ============================================================================
// Factory Functions
// ============================================================================

/**
 * Create a mock socket that can be used in place of a real Socket.
 * The mock implements all Socket properties/methods used by ConnectionPool.
 *
 * @param options - Optional configuration for the mock socket
 * @returns A MockSocketImpl instance typed as both MockSocket and Socket for flexibility
 *
 * @example
 * ```ts
 * // Basic usage
 * const socket = createMockSocket();
 * pool.addConnection(socket);
 *
 * // With custom properties
 * const socket = createMockSocket({
 *   remoteAddress: '10.0.0.1',
 *   remotePort: 27017,
 * });
 * ```
 */
export function createMockSocket(options?: MockSocketOptions): MockSocketImpl & Socket {
  return new MockSocketImpl(options) as MockSocketImpl & Socket;
}

/**
 * Create a mock socket with spied methods for assertion testing.
 * All methods are wrapped with vi.fn() for call tracking.
 *
 * @param options - Optional configuration for the mock socket
 * @returns A spied mock socket instance
 *
 * @example
 * ```ts
 * const socket = createSpiedMockSocket();
 * pool.addConnection(socket);
 *
 * // Later in test assertions
 * expect(socket.destroy).toHaveBeenCalled();
 * expect(socket.write).toHaveBeenCalledWith(expect.any(Buffer));
 * ```
 */
export function createSpiedMockSocket(options?: MockSocketOptions): SpiedMockSocket & Socket {
  const socket = new MockSocketImpl(options);

  // Wrap methods with spies while preserving original behavior
  const originalDestroy = socket.destroy.bind(socket);
  const originalEnd = socket.end.bind(socket);
  const originalWrite = socket.write.bind(socket);
  const originalOnce = socket.once.bind(socket);
  const originalOn = socket.on.bind(socket);
  const originalRemoveAllListeners = socket.removeAllListeners.bind(socket);

  const spiedSocket = socket as unknown as SpiedMockSocket;

  spiedSocket.destroy = vi.fn(() => {
    originalDestroy();
  });

  spiedSocket.end = vi.fn(() => {
    originalEnd();
  });

  spiedSocket.write = vi.fn((data: Buffer | Uint8Array) => {
    return originalWrite(data);
  });

  spiedSocket.once = vi.fn((event: string, listener: (...args: unknown[]) => void) => {
    originalOnce(event, listener);
    return spiedSocket;
  });

  spiedSocket.on = vi.fn((event: string, listener: (...args: unknown[]) => void) => {
    originalOn(event, listener);
    return spiedSocket;
  });

  spiedSocket.removeAllListeners = vi.fn((event?: string) => {
    originalRemoveAllListeners(event);
    return spiedSocket;
  });

  return spiedSocket as SpiedMockSocket & Socket;
}

/**
 * Create multiple mock sockets at once.
 *
 * @param count - Number of sockets to create
 * @param options - Optional configuration applied to all sockets
 * @returns Array of mock sockets
 *
 * @example
 * ```ts
 * const sockets = createMockSockets(5);
 * sockets.forEach(socket => pool.addConnection(socket));
 * ```
 */
export function createMockSockets(
  count: number,
  options?: MockSocketOptions
): Array<MockSocketImpl & Socket> {
  return Array.from({ length: count }, (_, i) =>
    createMockSocket({
      ...options,
      remotePort: (options?.remotePort ?? 12345) + i,
    })
  );
}

/**
 * Create a mock socket that simulates connection errors.
 *
 * @param errorAfterMs - Time in ms after which to emit an error
 * @param error - The error to emit (default: 'Connection reset')
 * @returns A mock socket that will error after the specified time
 *
 * @example
 * ```ts
 * const socket = createErroringMockSocket(100);
 * pool.addConnection(socket);
 * // After 100ms, socket will emit 'error' event
 * ```
 */
export function createErroringMockSocket(
  errorAfterMs: number,
  error: Error = new Error('Connection reset')
): MockSocketImpl & Socket {
  const socket = createMockSocket();

  setTimeout(() => {
    if (!socket.destroyed) {
      socket.emit('error', error);
    }
  }, errorAfterMs);

  return socket;
}

/**
 * Create a mock socket that simulates slow writes.
 *
 * @param writeDelayMs - Delay in ms before write completes
 * @returns A mock socket with delayed writes
 *
 * @example
 * ```ts
 * const socket = createSlowMockSocket(500);
 * // socket.write() will still return immediately but can track timing
 * ```
 */
export function createSlowMockSocket(writeDelayMs: number): MockSocketImpl & Socket {
  const socket = new MockSocketImpl();

  const originalWrite = socket.write.bind(socket);
  socket.write = (data: Buffer | Uint8Array): boolean => {
    const result = originalWrite(data);
    if (result) {
      // Emit drain after delay to simulate slow write
      setTimeout(() => {
        socket.emit('drain');
      }, writeDelayMs);
    }
    return result;
  };

  return socket as MockSocketImpl & Socket;
}
