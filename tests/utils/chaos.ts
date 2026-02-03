/**
 * Chaos Testing Framework
 *
 * Provides utilities for chaos testing including fault injection,
 * network partition simulation, random failure generation, and timeout simulation.
 * Designed to integrate with MongoLake's existing test infrastructure.
 *
 * @example
 * ```ts
 * import {
 *   FaultInjector,
 *   NetworkPartition,
 *   RandomFailure,
 *   TimeoutSimulator,
 *   createChaosStorage,
 * } from '../utils/chaos.js';
 *
 * // Inject faults into storage operations
 * const injector = new FaultInjector();
 * injector.injectOnce('storage.get', new Error('Disk read error'));
 *
 * // Simulate network partitions
 * const partition = new NetworkPartition();
 * partition.isolate('node-1');
 *
 * // Random failure generation
 * const failure = new RandomFailure(0.1); // 10% failure rate
 * failure.maybeThrow('operation');
 *
 * // Simulate timeouts
 * const timeout = new TimeoutSimulator(1000);
 * await timeout.wrapOperation(() => fetch('/api'));
 * ```
 */

import { vi, type MockedFunction } from 'vitest';
import type { R2Bucket, R2Object, R2ObjectBody, R2ListOptions, R2Objects, R2MultipartUpload } from '../../src/types.js';
import type { MockStorage } from './mocks.js';

// ============================================================================
// Types
// ============================================================================

/** Fault types that can be injected */
export type FaultType =
  | 'error' // Throws an error
  | 'timeout' // Simulates a timeout
  | 'delay' // Adds artificial latency
  | 'corruption' // Returns corrupted data
  | 'partial' // Returns partial data
  | 'disconnect' // Simulates connection drop
  | 'throttle'; // Rate limits operations

/** Configuration for a fault */
export interface FaultConfig {
  /** Type of fault to inject */
  type: FaultType;
  /** Error to throw (for 'error' type) */
  error?: Error;
  /** Delay in milliseconds (for 'delay' and 'timeout' types) */
  delayMs?: number;
  /** Probability of fault occurring (0-1, default 1) */
  probability?: number;
  /** Maximum number of times to inject this fault */
  maxOccurrences?: number;
  /** Only inject after this many calls */
  afterCalls?: number;
  /** Custom handler for the fault */
  handler?: () => void | Promise<void>;
}

/** Statistics for a fault injection point */
export interface FaultStats {
  /** Total number of calls to this injection point */
  totalCalls: number;
  /** Number of times a fault was injected */
  faultsInjected: number;
  /** Timestamp of last fault */
  lastFaultTime?: number;
  /** Errors thrown */
  errors: Error[];
}

/** Network node state */
export type NodeState = 'connected' | 'isolated' | 'slow' | 'dropping';

/** Network partition configuration */
export interface PartitionConfig {
  /** Nodes that are isolated from others */
  isolatedNodes: Set<string>;
  /** Nodes with degraded connectivity */
  slowNodes: Map<string, number>; // node -> latency in ms
  /** Nodes that randomly drop packets */
  droppingNodes: Map<string, number>; // node -> drop rate (0-1)
  /** Allowed connections between nodes (if empty, all non-isolated allowed) */
  allowedConnections: Map<string, Set<string>>;
}

/** Chaos test scenario */
export interface ChaosScenario {
  /** Name of the scenario */
  name: string;
  /** Description of what this scenario tests */
  description: string;
  /** Setup function called before scenario runs */
  setup?: () => void | Promise<void>;
  /** Faults to inject during the scenario */
  faults: Array<{ target: string; config: FaultConfig }>;
  /** Network partitions to apply */
  partitions?: PartitionConfig;
  /** Duration of the scenario in ms (for time-based scenarios) */
  durationMs?: number;
  /** Cleanup function called after scenario completes */
  cleanup?: () => void | Promise<void>;
}

// ============================================================================
// FaultInjector
// ============================================================================

/**
 * FaultInjector provides controlled failure injection for testing.
 * Allows precise control over when and how faults occur during test execution.
 *
 * @example
 * ```ts
 * const injector = new FaultInjector();
 *
 * // Inject a single error
 * injector.injectOnce('storage.get', new Error('Read failed'));
 *
 * // Inject with probability
 * injector.inject('storage.put', {
 *   type: 'error',
 *   error: new Error('Write failed'),
 *   probability: 0.3, // 30% chance
 * });
 *
 * // Add delay to operations
 * injector.inject('network.fetch', {
 *   type: 'delay',
 *   delayMs: 500,
 * });
 *
 * // Check and trigger fault
 * if (injector.shouldFault('storage.get')) {
 *   await injector.triggerFault('storage.get');
 * }
 * ```
 */
export class FaultInjector {
  private faults = new Map<string, FaultConfig[]>();
  private stats = new Map<string, FaultStats>();
  private enabled = true;
  private globalProbability = 1;

  /**
   * Inject a fault at a specific injection point.
   *
   * @param target - The injection point identifier (e.g., 'storage.get')
   * @param config - Fault configuration
   */
  inject(target: string, config: FaultConfig): void {
    if (!this.faults.has(target)) {
      this.faults.set(target, []);
    }
    this.faults.get(target)!.push({ ...config });

    if (!this.stats.has(target)) {
      this.stats.set(target, {
        totalCalls: 0,
        faultsInjected: 0,
        errors: [],
      });
    }
  }

  /**
   * Inject a one-time error at a specific injection point.
   * Convenience method for the common case of injecting a single error.
   *
   * @param target - The injection point identifier
   * @param error - The error to throw
   */
  injectOnce(target: string, error: Error): void {
    this.inject(target, {
      type: 'error',
      error,
      maxOccurrences: 1,
    });
  }

  /**
   * Inject a delay at a specific injection point.
   *
   * @param target - The injection point identifier
   * @param delayMs - Delay in milliseconds
   * @param options - Additional options
   */
  injectDelay(
    target: string,
    delayMs: number,
    options?: { probability?: number; maxOccurrences?: number }
  ): void {
    this.inject(target, {
      type: 'delay',
      delayMs,
      ...options,
    });
  }

  /**
   * Inject a timeout (delay that exceeds expected duration).
   *
   * @param target - The injection point identifier
   * @param delayMs - Delay in milliseconds (should exceed caller's timeout)
   */
  injectTimeout(target: string, delayMs: number = 30000): void {
    this.inject(target, {
      type: 'timeout',
      delayMs,
    });
  }

  /**
   * Check if a fault should be triggered for the given target.
   *
   * @param target - The injection point identifier
   * @returns True if a fault should be triggered
   */
  shouldFault(target: string): boolean {
    if (!this.enabled) return false;

    const faultConfigs = this.faults.get(target);
    if (!faultConfigs || faultConfigs.length === 0) return false;

    const stats = this.stats.get(target)!;
    stats.totalCalls++;

    for (const config of faultConfigs) {
      // Check afterCalls condition
      if (config.afterCalls !== undefined && stats.totalCalls <= config.afterCalls) {
        continue;
      }

      // Check maxOccurrences condition
      if (config.maxOccurrences !== undefined && stats.faultsInjected >= config.maxOccurrences) {
        continue;
      }

      // Check probability
      const probability = (config.probability ?? 1) * this.globalProbability;
      if (Math.random() < probability) {
        return true;
      }
    }

    return false;
  }

  /**
   * Trigger a fault for the given target.
   * Should be called after shouldFault() returns true.
   *
   * @param target - The injection point identifier
   * @throws If a fault is configured to throw
   */
  async triggerFault(target: string): Promise<void> {
    const faultConfigs = this.faults.get(target);
    if (!faultConfigs || faultConfigs.length === 0) return;

    const stats = this.stats.get(target)!;

    for (const config of faultConfigs) {
      // Check conditions again
      if (config.afterCalls !== undefined && stats.totalCalls <= config.afterCalls) {
        continue;
      }
      if (config.maxOccurrences !== undefined && stats.faultsInjected >= config.maxOccurrences) {
        continue;
      }

      const probability = (config.probability ?? 1) * this.globalProbability;
      if (Math.random() >= probability) {
        continue;
      }

      stats.faultsInjected++;
      stats.lastFaultTime = Date.now();

      // Execute custom handler if provided
      if (config.handler) {
        await config.handler();
      }

      switch (config.type) {
        case 'error':
          if (config.error) {
            stats.errors.push(config.error);
            throw config.error;
          }
          throw new Error(`Injected fault at ${target}`);

        case 'timeout':
        case 'delay':
          if (config.delayMs) {
            await new Promise((resolve) => setTimeout(resolve, config.delayMs));
          }
          if (config.type === 'timeout') {
            throw new Error(`Timeout at ${target}`);
          }
          break;

        case 'disconnect':
          throw new Error(`Connection lost at ${target}`);

        case 'corruption':
        case 'partial':
        case 'throttle':
          // These are handled by the wrapping code
          break;
      }

      // Only trigger one fault per call
      return;
    }
  }

  /**
   * Wrap an async operation with fault injection.
   *
   * @param target - The injection point identifier
   * @param operation - The operation to wrap
   * @returns The result of the operation
   */
  async wrap<T>(target: string, operation: () => Promise<T>): Promise<T> {
    if (this.shouldFault(target)) {
      await this.triggerFault(target);
    }
    return operation();
  }

  /**
   * Clear all faults for a specific target or all targets.
   *
   * @param target - Optional target to clear; if omitted, clears all
   */
  clear(target?: string): void {
    if (target) {
      this.faults.delete(target);
    } else {
      this.faults.clear();
    }
  }

  /**
   * Reset statistics for a specific target or all targets.
   *
   * @param target - Optional target to reset; if omitted, resets all
   */
  resetStats(target?: string): void {
    if (target) {
      this.stats.set(target, {
        totalCalls: 0,
        faultsInjected: 0,
        errors: [],
      });
    } else {
      this.stats.clear();
    }
  }

  /**
   * Get statistics for a specific target or all targets.
   *
   * @param target - Optional target; if omitted, returns all stats
   */
  getStats(target?: string): FaultStats | Map<string, FaultStats> {
    if (target) {
      return (
        this.stats.get(target) ?? {
          totalCalls: 0,
          faultsInjected: 0,
          errors: [],
        }
      );
    }
    return new Map(this.stats);
  }

  /**
   * Enable or disable fault injection globally.
   *
   * @param enabled - Whether fault injection should be enabled
   */
  setEnabled(enabled: boolean): void {
    this.enabled = enabled;
  }

  /**
   * Set global probability multiplier for all faults.
   *
   * @param probability - Probability multiplier (0-1)
   */
  setGlobalProbability(probability: number): void {
    this.globalProbability = Math.max(0, Math.min(1, probability));
  }

  /**
   * Check if any faults are configured.
   */
  hasFaults(): boolean {
    return this.faults.size > 0;
  }

  /**
   * Get all configured fault targets.
   */
  getTargets(): string[] {
    return Array.from(this.faults.keys());
  }
}

// ============================================================================
// NetworkPartition
// ============================================================================

/**
 * NetworkPartition simulates network partitions and degraded connectivity.
 * Useful for testing distributed system behavior under network failures.
 *
 * @example
 * ```ts
 * const partition = new NetworkPartition();
 *
 * // Isolate a node completely
 * partition.isolate('node-1');
 *
 * // Add latency to a node
 * partition.addLatency('node-2', 500); // 500ms latency
 *
 * // Enable packet dropping
 * partition.setDropRate('node-3', 0.1); // 10% drop rate
 *
 * // Check if nodes can communicate
 * if (partition.canCommunicate('node-1', 'node-2')) {
 *   await partition.simulateLatency('node-1', 'node-2');
 * }
 * ```
 */
export class NetworkPartition {
  private config: PartitionConfig = {
    isolatedNodes: new Set(),
    slowNodes: new Map(),
    droppingNodes: new Map(),
    allowedConnections: new Map(),
  };

  private messageQueue: Array<{
    from: string;
    to: string;
    message: unknown;
    deliveryTime: number;
  }> = [];

  private eventListeners = new Map<string, Array<(event: NetworkEvent) => void>>();

  /**
   * Isolate a node from the network.
   * Isolated nodes cannot send or receive messages.
   *
   * @param nodeId - The node to isolate
   */
  isolate(nodeId: string): void {
    this.config.isolatedNodes.add(nodeId);
    this.emitEvent({ type: 'isolated', nodeId, timestamp: Date.now() });
  }

  /**
   * Reconnect an isolated node to the network.
   *
   * @param nodeId - The node to reconnect
   */
  reconnect(nodeId: string): void {
    this.config.isolatedNodes.delete(nodeId);
    this.emitEvent({ type: 'reconnected', nodeId, timestamp: Date.now() });
  }

  /**
   * Check if a node is isolated.
   *
   * @param nodeId - The node to check
   */
  isIsolated(nodeId: string): boolean {
    return this.config.isolatedNodes.has(nodeId);
  }

  /**
   * Add latency to a node's communications.
   *
   * @param nodeId - The node to add latency to
   * @param latencyMs - Latency in milliseconds
   */
  addLatency(nodeId: string, latencyMs: number): void {
    this.config.slowNodes.set(nodeId, latencyMs);
    this.emitEvent({ type: 'latencyAdded', nodeId, latencyMs, timestamp: Date.now() });
  }

  /**
   * Remove latency from a node.
   *
   * @param nodeId - The node to restore normal latency to
   */
  removeLatency(nodeId: string): void {
    this.config.slowNodes.delete(nodeId);
    this.emitEvent({ type: 'latencyRemoved', nodeId, timestamp: Date.now() });
  }

  /**
   * Get the latency for a node (0 if not set).
   *
   * @param nodeId - The node to check
   */
  getLatency(nodeId: string): number {
    return this.config.slowNodes.get(nodeId) ?? 0;
  }

  /**
   * Set packet drop rate for a node.
   *
   * @param nodeId - The node to set drop rate for
   * @param dropRate - Drop rate (0-1)
   */
  setDropRate(nodeId: string, dropRate: number): void {
    this.config.droppingNodes.set(nodeId, Math.max(0, Math.min(1, dropRate)));
    this.emitEvent({ type: 'dropRateSet', nodeId, dropRate, timestamp: Date.now() });
  }

  /**
   * Remove packet dropping from a node.
   *
   * @param nodeId - The node to restore
   */
  removeDropRate(nodeId: string): void {
    this.config.droppingNodes.delete(nodeId);
    this.emitEvent({ type: 'dropRateRemoved', nodeId, timestamp: Date.now() });
  }

  /**
   * Get the drop rate for a node (0 if not set).
   *
   * @param nodeId - The node to check
   */
  getDropRate(nodeId: string): number {
    return this.config.droppingNodes.get(nodeId) ?? 0;
  }

  /**
   * Create a bidirectional partition between two groups of nodes.
   *
   * @param groupA - First group of nodes
   * @param groupB - Second group of nodes
   */
  partition(groupA: string[], groupB: string[]): void {
    // Nodes in groupA can only talk to other nodes in groupA
    // Nodes in groupB can only talk to other nodes in groupB
    for (const nodeA of groupA) {
      this.config.allowedConnections.set(nodeA, new Set(groupA));
    }
    for (const nodeB of groupB) {
      this.config.allowedConnections.set(nodeB, new Set(groupB));
    }
    this.emitEvent({
      type: 'partitioned',
      groupA,
      groupB,
      timestamp: Date.now(),
    });
  }

  /**
   * Allow specific connections between nodes.
   *
   * @param from - Source node
   * @param to - Destination node(s)
   */
  allowConnection(from: string, to: string | string[]): void {
    if (!this.config.allowedConnections.has(from)) {
      this.config.allowedConnections.set(from, new Set());
    }
    const allowed = this.config.allowedConnections.get(from)!;
    const targets = Array.isArray(to) ? to : [to];
    for (const target of targets) {
      allowed.add(target);
    }
  }

  /**
   * Check if two nodes can communicate.
   *
   * @param from - Source node
   * @param to - Destination node
   */
  canCommunicate(from: string, to: string): boolean {
    // Check isolation
    if (this.config.isolatedNodes.has(from) || this.config.isolatedNodes.has(to)) {
      return false;
    }

    // Check allowed connections (if any are configured)
    if (this.config.allowedConnections.size > 0) {
      const allowed = this.config.allowedConnections.get(from);
      if (allowed && !allowed.has(to)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Simulate sending a message between nodes.
   * Returns false if the message was dropped.
   *
   * @param from - Source node
   * @param to - Destination node
   * @param message - The message to send
   */
  async sendMessage<T>(from: string, to: string, message: T): Promise<boolean> {
    if (!this.canCommunicate(from, to)) {
      this.emitEvent({
        type: 'messageBlocked',
        from,
        to,
        timestamp: Date.now(),
      });
      return false;
    }

    // Check for packet drop
    const dropRate = Math.max(this.getDropRate(from), this.getDropRate(to));
    if (dropRate > 0 && Math.random() < dropRate) {
      this.emitEvent({
        type: 'messageDropped',
        from,
        to,
        timestamp: Date.now(),
      });
      return false;
    }

    // Simulate latency
    const latency = this.getLatency(from) + this.getLatency(to);
    if (latency > 0) {
      await new Promise((resolve) => setTimeout(resolve, latency));
    }

    this.emitEvent({
      type: 'messageDelivered',
      from,
      to,
      latency,
      timestamp: Date.now(),
    });

    return true;
  }

  /**
   * Get the state of a node.
   *
   * @param nodeId - The node to check
   */
  getNodeState(nodeId: string): NodeState {
    if (this.config.isolatedNodes.has(nodeId)) return 'isolated';
    if (this.config.droppingNodes.has(nodeId)) return 'dropping';
    if (this.config.slowNodes.has(nodeId)) return 'slow';
    return 'connected';
  }

  /**
   * Reset all partitions and restore normal connectivity.
   */
  reset(): void {
    this.config = {
      isolatedNodes: new Set(),
      slowNodes: new Map(),
      droppingNodes: new Map(),
      allowedConnections: new Map(),
    };
    this.messageQueue = [];
    this.emitEvent({ type: 'reset', timestamp: Date.now() });
  }

  /**
   * Subscribe to network events.
   *
   * @param eventType - Event type to listen for
   * @param listener - Event listener
   */
  on(eventType: string, listener: (event: NetworkEvent) => void): void {
    if (!this.eventListeners.has(eventType)) {
      this.eventListeners.set(eventType, []);
    }
    this.eventListeners.get(eventType)!.push(listener);
  }

  /**
   * Remove event listener.
   *
   * @param eventType - Event type
   * @param listener - Event listener to remove
   */
  off(eventType: string, listener: (event: NetworkEvent) => void): void {
    const listeners = this.eventListeners.get(eventType);
    if (listeners) {
      const index = listeners.indexOf(listener);
      if (index >= 0) {
        listeners.splice(index, 1);
      }
    }
  }

  private emitEvent(event: NetworkEvent): void {
    const listeners = this.eventListeners.get(event.type) ?? [];
    const allListeners = this.eventListeners.get('*') ?? [];
    for (const listener of [...listeners, ...allListeners]) {
      listener(event);
    }
  }

  /**
   * Get current partition configuration.
   */
  getConfig(): Readonly<PartitionConfig> {
    return {
      isolatedNodes: new Set(this.config.isolatedNodes),
      slowNodes: new Map(this.config.slowNodes),
      droppingNodes: new Map(this.config.droppingNodes),
      allowedConnections: new Map(this.config.allowedConnections),
    };
  }
}

/** Network event types */
export interface NetworkEvent {
  type: string;
  timestamp: number;
  nodeId?: string;
  from?: string;
  to?: string;
  latencyMs?: number;
  dropRate?: number;
  latency?: number;
  groupA?: string[];
  groupB?: string[];
}

// ============================================================================
// RandomFailure
// ============================================================================

/**
 * RandomFailure generates random failures with configurable probability.
 * Useful for probabilistic chaos testing.
 *
 * @example
 * ```ts
 * const failure = new RandomFailure(0.1); // 10% failure rate
 *
 * // Check and maybe throw
 * failure.maybeThrow('database.query');
 *
 * // Wrap an operation
 * const result = await failure.wrap('api.call', async () => {
 *   return fetch('/api');
 * });
 *
 * // Custom error generators
 * failure.setErrorGenerator('storage', () => new Error('Disk full'));
 * ```
 */
export class RandomFailure {
  private probability: number;
  private errorGenerators = new Map<string, () => Error>();
  private stats = {
    calls: 0,
    failures: 0,
    byTarget: new Map<string, { calls: number; failures: number }>(),
  };

  /**
   * Create a RandomFailure instance.
   *
   * @param probability - Base probability of failure (0-1)
   */
  constructor(probability: number = 0.1) {
    this.probability = Math.max(0, Math.min(1, probability));
  }

  /**
   * Set the base failure probability.
   *
   * @param probability - Probability (0-1)
   */
  setProbability(probability: number): void {
    this.probability = Math.max(0, Math.min(1, probability));
  }

  /**
   * Get the current failure probability.
   */
  getProbability(): number {
    return this.probability;
  }

  /**
   * Set a custom error generator for a specific target.
   *
   * @param target - Target identifier
   * @param generator - Function that creates an error
   */
  setErrorGenerator(target: string, generator: () => Error): void {
    this.errorGenerators.set(target, generator);
  }

  /**
   * Check if a failure should occur (without throwing).
   *
   * @param probability - Optional override probability
   */
  shouldFail(probability?: number): boolean {
    return Math.random() < (probability ?? this.probability);
  }

  /**
   * Maybe throw an error based on probability.
   *
   * @param target - Target identifier for error message and stats
   * @param probability - Optional override probability
   * @throws If randomly determined to fail
   */
  maybeThrow(target: string, probability?: number): void {
    this.recordCall(target);

    if (this.shouldFail(probability)) {
      this.recordFailure(target);
      const generator = this.errorGenerators.get(target);
      if (generator) {
        throw generator();
      }
      throw new Error(`Random failure at ${target}`);
    }
  }

  /**
   * Wrap an async operation with random failure injection.
   *
   * @param target - Target identifier
   * @param operation - The operation to wrap
   * @param probability - Optional override probability
   * @returns The result of the operation
   */
  async wrap<T>(target: string, operation: () => Promise<T>, probability?: number): Promise<T> {
    this.maybeThrow(target, probability);
    return operation();
  }

  /**
   * Wrap a sync operation with random failure injection.
   *
   * @param target - Target identifier
   * @param operation - The operation to wrap
   * @param probability - Optional override probability
   * @returns The result of the operation
   */
  wrapSync<T>(target: string, operation: () => T, probability?: number): T {
    this.maybeThrow(target, probability);
    return operation();
  }

  /**
   * Create a wrapped function that may randomly fail.
   *
   * @param target - Target identifier
   * @param fn - Function to wrap
   * @param probability - Optional override probability
   */
  wrapFunction<TArgs extends unknown[], TResult>(
    target: string,
    fn: (...args: TArgs) => Promise<TResult>,
    probability?: number
  ): (...args: TArgs) => Promise<TResult> {
    return async (...args: TArgs): Promise<TResult> => {
      this.maybeThrow(target, probability);
      return fn(...args);
    };
  }

  private recordCall(target: string): void {
    this.stats.calls++;
    if (!this.stats.byTarget.has(target)) {
      this.stats.byTarget.set(target, { calls: 0, failures: 0 });
    }
    this.stats.byTarget.get(target)!.calls++;
  }

  private recordFailure(target: string): void {
    this.stats.failures++;
    this.stats.byTarget.get(target)!.failures++;
  }

  /**
   * Get failure statistics.
   */
  getStats(): {
    calls: number;
    failures: number;
    failureRate: number;
    byTarget: Map<string, { calls: number; failures: number; failureRate: number }>;
  } {
    const byTarget = new Map<string, { calls: number; failures: number; failureRate: number }>();
    for (const [target, stats] of this.stats.byTarget) {
      byTarget.set(target, {
        ...stats,
        failureRate: stats.calls > 0 ? stats.failures / stats.calls : 0,
      });
    }
    return {
      calls: this.stats.calls,
      failures: this.stats.failures,
      failureRate: this.stats.calls > 0 ? this.stats.failures / this.stats.calls : 0,
      byTarget,
    };
  }

  /**
   * Reset all statistics.
   */
  resetStats(): void {
    this.stats = {
      calls: 0,
      failures: 0,
      byTarget: new Map(),
    };
  }
}

// ============================================================================
// TimeoutSimulator
// ============================================================================

/**
 * TimeoutSimulator adds timeout behavior to operations.
 * Useful for testing timeout handling and retry logic.
 *
 * @example
 * ```ts
 * const timeout = new TimeoutSimulator(5000);
 *
 * // Wrap an operation with timeout
 * try {
 *   await timeout.wrapOperation(async () => {
 *     return await slowOperation();
 *   });
 * } catch (e) {
 *   // Handle timeout
 * }
 *
 * // Simulate a delayed response
 * await timeout.simulateDelay(1000);
 *
 * // Create a racing operation
 * const result = await timeout.race(
 *   async () => fetch('/api'),
 *   3000
 * );
 * ```
 */
export class TimeoutSimulator {
  private defaultTimeoutMs: number;
  private pendingOperations = new Set<AbortController>();

  /**
   * Create a TimeoutSimulator.
   *
   * @param defaultTimeoutMs - Default timeout in milliseconds
   */
  constructor(defaultTimeoutMs: number = 5000) {
    this.defaultTimeoutMs = defaultTimeoutMs;
  }

  /**
   * Set the default timeout.
   *
   * @param timeoutMs - Timeout in milliseconds
   */
  setDefaultTimeout(timeoutMs: number): void {
    this.defaultTimeoutMs = timeoutMs;
  }

  /**
   * Get the default timeout.
   */
  getDefaultTimeout(): number {
    return this.defaultTimeoutMs;
  }

  /**
   * Wrap an operation with a timeout.
   *
   * @param operation - The operation to wrap
   * @param timeoutMs - Optional override timeout
   * @throws TimeoutError if operation exceeds timeout
   */
  async wrapOperation<T>(operation: () => Promise<T>, timeoutMs?: number): Promise<T> {
    const timeout = timeoutMs ?? this.defaultTimeoutMs;
    const controller = new AbortController();
    this.pendingOperations.add(controller);

    // Create abort rejection promise
    const abortPromise = new Promise<never>((_, reject) => {
      controller.signal.addEventListener('abort', () => {
        reject(new Error('Operation aborted'));
      });
    });

    try {
      return await Promise.race([
        operation(),
        this.createTimeoutPromise(timeout, controller.signal),
        abortPromise,
      ]);
    } finally {
      this.pendingOperations.delete(controller);
    }
  }

  /**
   * Race an operation against a timeout.
   * Returns the result or throws on timeout.
   *
   * @param operation - The operation to race
   * @param timeoutMs - Optional override timeout
   */
  async race<T>(operation: () => Promise<T>, timeoutMs?: number): Promise<T> {
    return this.wrapOperation(operation, timeoutMs);
  }

  /**
   * Simulate a delay (useful for testing timeout behavior).
   *
   * @param delayMs - Delay in milliseconds
   * @param signal - Optional abort signal
   */
  async simulateDelay(delayMs: number, signal?: AbortSignal): Promise<void> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(resolve, delayMs);

      if (signal) {
        signal.addEventListener('abort', () => {
          clearTimeout(timer);
          reject(new Error('Delay aborted'));
        });
      }
    });
  }

  /**
   * Create a delay that will exceed the default timeout.
   * Useful for testing timeout handling.
   *
   * @param multiplier - How much to exceed timeout by (default 2x)
   */
  async simulateTimeoutExceeded(multiplier: number = 2): Promise<never> {
    await this.simulateDelay(this.defaultTimeoutMs * multiplier);
    throw new Error('This should not be reached - operation should have timed out');
  }

  /**
   * Create a promise that rejects after a timeout.
   *
   * @param timeoutMs - Timeout in milliseconds
   * @param signal - Optional abort signal
   */
  createTimeoutPromise(timeoutMs: number, signal?: AbortSignal): Promise<never> {
    return new Promise((_, reject) => {
      const timer = setTimeout(() => {
        reject(new TimeoutError(`Operation timed out after ${timeoutMs}ms`));
      }, timeoutMs);

      if (signal) {
        signal.addEventListener('abort', () => {
          clearTimeout(timer);
        });
      }
    });
  }

  /**
   * Abort all pending operations.
   */
  abortAll(): void {
    const controllers = Array.from(this.pendingOperations);
    this.pendingOperations.clear();
    for (const controller of controllers) {
      controller.abort();
    }
  }

  /**
   * Get count of pending operations.
   */
  getPendingCount(): number {
    return this.pendingOperations.size;
  }

  /**
   * Create a function that times out after repeated slow responses.
   * Useful for circuit breaker testing.
   *
   * @param operation - The operation to wrap
   * @param options - Circuit breaker options
   */
  createCircuitBreaker<TArgs extends unknown[], TResult>(
    operation: (...args: TArgs) => Promise<TResult>,
    options: {
      timeout?: number;
      failureThreshold?: number;
      resetTimeout?: number;
    } = {}
  ): (...args: TArgs) => Promise<TResult> {
    const timeout = options.timeout ?? this.defaultTimeoutMs;
    const failureThreshold = options.failureThreshold ?? 5;
    const resetTimeout = options.resetTimeout ?? 30000;

    let failures = 0;
    let circuitOpen = false;
    let lastFailureTime = 0;

    return async (...args: TArgs): Promise<TResult> => {
      // Check if circuit should be reset
      if (circuitOpen && Date.now() - lastFailureTime > resetTimeout) {
        circuitOpen = false;
        failures = 0;
      }

      if (circuitOpen) {
        throw new CircuitOpenError('Circuit breaker is open');
      }

      try {
        const result = await this.wrapOperation(() => operation(...args), timeout);
        failures = 0; // Reset on success
        return result;
      } catch (error) {
        failures++;
        lastFailureTime = Date.now();

        if (failures >= failureThreshold) {
          circuitOpen = true;
        }

        throw error;
      }
    };
  }
}

/**
 * Error thrown when an operation times out.
 */
export class TimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'TimeoutError';
  }
}

/**
 * Error thrown when a circuit breaker is open.
 */
export class CircuitOpenError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CircuitOpenError';
  }
}

// ============================================================================
// Integration Helpers
// ============================================================================

/**
 * Options for creating a chaos-enabled storage mock.
 */
export interface ChaosStorageOptions {
  /** Fault injector to use */
  faultInjector?: FaultInjector;
  /** Random failure generator to use */
  randomFailure?: RandomFailure;
  /** Timeout simulator to use */
  timeoutSimulator?: TimeoutSimulator;
  /** Base storage mock to wrap */
  baseStorage?: MockStorage;
}

/**
 * Create a chaos-enabled storage mock that integrates with the existing
 * MockStorage interface while adding fault injection capabilities.
 *
 * @param options - Configuration options
 * @returns A MockStorage instance with chaos capabilities
 *
 * @example
 * ```ts
 * const injector = new FaultInjector();
 * const storage = createChaosStorage({ faultInjector: injector });
 *
 * injector.injectOnce('storage.get', new Error('Read failed'));
 *
 * await storage.get('key'); // Throws 'Read failed'
 * await storage.get('key'); // Works normally
 * ```
 */
export function createChaosStorage(options: ChaosStorageOptions = {}): MockStorage & {
  chaos: {
    injector: FaultInjector;
    randomFailure: RandomFailure;
    timeout: TimeoutSimulator;
  };
} {
  const data = new Map<string, Uint8Array>();
  const injector = options.faultInjector ?? new FaultInjector();
  const randomFailure = options.randomFailure ?? new RandomFailure(0);
  const timeout = options.timeoutSimulator ?? new TimeoutSimulator();

  const wrapWithChaos = async <T>(target: string, operation: () => Promise<T>): Promise<T> => {
    // Check fault injector first
    if (injector.shouldFault(target)) {
      await injector.triggerFault(target);
    }

    // Check random failure
    randomFailure.maybeThrow(target);

    // Wrap with timeout
    return timeout.wrapOperation(operation);
  };

  return {
    data,

    async get(key: string): Promise<Uint8Array | null> {
      return wrapWithChaos('storage.get', async () => {
        return data.get(key) ?? null;
      });
    },

    async put(key: string, value: Uint8Array | ArrayBuffer | string): Promise<void> {
      return wrapWithChaos('storage.put', async () => {
        const bytes =
          value instanceof Uint8Array
            ? value
            : typeof value === 'string'
              ? new TextEncoder().encode(value)
              : new Uint8Array(value);
        data.set(key, bytes);
      });
    },

    async delete(key: string): Promise<void> {
      return wrapWithChaos('storage.delete', async () => {
        data.delete(key);
      });
    },

    async list(prefix?: string): Promise<string[]> {
      return wrapWithChaos('storage.list', async () => {
        const keys: string[] = [];
        for (const key of data.keys()) {
          if (!prefix || key.startsWith(prefix)) {
            keys.push(key);
          }
        }
        return keys;
      });
    },

    async head(key: string): Promise<{ key: string; size: number } | null> {
      return wrapWithChaos('storage.head', async () => {
        const value = data.get(key);
        if (!value) return null;
        return { key, size: value.length };
      });
    },

    async exists(key: string): Promise<boolean> {
      return wrapWithChaos('storage.exists', async () => {
        return data.has(key);
      });
    },

    clear(): void {
      data.clear();
    },

    chaos: {
      injector,
      randomFailure,
      timeout,
    },
  };
}

/**
 * Create a chaos-enabled R2 bucket mock.
 *
 * @param options - Configuration options
 * @returns An R2Bucket mock with chaos capabilities
 */
export function createChaosR2Bucket(options: ChaosStorageOptions = {}): R2Bucket & {
  _objects: Map<string, Uint8Array>;
  chaos: {
    injector: FaultInjector;
    randomFailure: RandomFailure;
    timeout: TimeoutSimulator;
  };
} {
  const objects = new Map<string, Uint8Array>();
  const injector = options.faultInjector ?? new FaultInjector();
  const randomFailure = options.randomFailure ?? new RandomFailure(0);
  const timeout = options.timeoutSimulator ?? new TimeoutSimulator();

  const wrapWithChaos = async <T>(target: string, operation: () => Promise<T>): Promise<T> => {
    if (injector.shouldFault(target)) {
      await injector.triggerFault(target);
    }
    randomFailure.maybeThrow(target);
    return timeout.wrapOperation(operation);
  };

  return {
    _objects: objects,

    async get(key: string): Promise<R2ObjectBody | null> {
      return wrapWithChaos('r2.get', async () => {
        const data = objects.get(key);
        if (!data) return null;
        return {
          arrayBuffer: async () => data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) as ArrayBuffer,
          text: async () => new TextDecoder().decode(data),
          json: async <T>() => JSON.parse(new TextDecoder().decode(data)) as T,
          body: new ReadableStream({
            start(controller) {
              controller.enqueue(data);
              controller.close();
            },
          }),
          etag: `etag-${key}`,
        };
      });
    },

    async head(key: string): Promise<R2Object | null> {
      return wrapWithChaos('r2.head', async () => {
        const data = objects.get(key);
        if (!data) return null;
        return {
          key,
          size: data.length,
          etag: `etag-${key}`,
        };
      });
    },

    async put(key: string, value: ArrayBuffer | Uint8Array | string): Promise<R2Object> {
      return wrapWithChaos('r2.put', async () => {
        const data =
          value instanceof Uint8Array
            ? value
            : typeof value === 'string'
              ? new TextEncoder().encode(value)
              : new Uint8Array(value);
        objects.set(key, data);
        return {
          key,
          size: data.length,
          etag: `etag-${key}`,
        };
      });
    },

    async delete(key: string): Promise<void> {
      return wrapWithChaos('r2.delete', async () => {
        objects.delete(key);
      });
    },

    async list(listOptions?: R2ListOptions): Promise<R2Objects> {
      return wrapWithChaos('r2.list', async () => {
        const result: R2Object[] = [];
        for (const [key, data] of objects) {
          if (!listOptions?.prefix || key.startsWith(listOptions.prefix)) {
            result.push({
              key,
              size: data.length,
              etag: `etag-${key}`,
            });
            if (listOptions?.limit && result.length >= listOptions.limit) {
              break;
            }
          }
        }
        return {
          objects: result,
          truncated: false,
        };
      });
    },

    async createMultipartUpload(_key: string): Promise<R2MultipartUpload> {
      throw new Error('createMultipartUpload not implemented in chaos mock');
    },

    chaos: {
      injector,
      randomFailure,
      timeout,
    },
  };
}

// ============================================================================
// Chaos Scenario Runner
// ============================================================================

/**
 * Run a chaos test scenario.
 *
 * @param scenario - The scenario to run
 * @param testFn - The test function to execute during the scenario
 *
 * @example
 * ```ts
 * await runChaosScenario(
 *   {
 *     name: 'Storage failure',
 *     description: 'Test behavior when storage fails intermittently',
 *     faults: [
 *       { target: 'storage.get', config: { type: 'error', probability: 0.3 } },
 *     ],
 *   },
 *   async () => {
 *     // Test code that should handle storage failures
 *   }
 * );
 * ```
 */
export async function runChaosScenario(
  scenario: ChaosScenario,
  testFn: (context: ChaosContext) => Promise<void>
): Promise<ChaosScenarioResult> {
  const injector = new FaultInjector();
  const partition = new NetworkPartition();
  const startTime = Date.now();
  const errors: Error[] = [];

  // Setup
  if (scenario.setup) {
    await scenario.setup();
  }

  // Configure faults
  for (const { target, config } of scenario.faults) {
    injector.inject(target, config);
  }

  // Configure partitions
  if (scenario.partitions) {
    for (const nodeId of scenario.partitions.isolatedNodes) {
      partition.isolate(nodeId);
    }
    for (const [nodeId, latency] of scenario.partitions.slowNodes) {
      partition.addLatency(nodeId, latency);
    }
    for (const [nodeId, dropRate] of scenario.partitions.droppingNodes) {
      partition.setDropRate(nodeId, dropRate);
    }
  }

  const context: ChaosContext = {
    injector,
    partition,
    scenario,
  };

  // Run test
  try {
    if (scenario.durationMs) {
      // Time-based scenario
      const endTime = startTime + scenario.durationMs;
      while (Date.now() < endTime) {
        try {
          await testFn(context);
        } catch (error) {
          errors.push(error instanceof Error ? error : new Error(String(error)));
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    } else {
      // Single-run scenario
      await testFn(context);
    }
  } catch (error) {
    errors.push(error instanceof Error ? error : new Error(String(error)));
  }

  // Cleanup
  if (scenario.cleanup) {
    await scenario.cleanup();
  }

  return {
    scenario: scenario.name,
    duration: Date.now() - startTime,
    faultStats: injector.getStats() as Map<string, FaultStats>,
    errors,
    success: errors.length === 0,
  };
}

/**
 * Context provided to chaos scenario test functions.
 */
export interface ChaosContext {
  injector: FaultInjector;
  partition: NetworkPartition;
  scenario: ChaosScenario;
}

/**
 * Result of running a chaos scenario.
 */
export interface ChaosScenarioResult {
  scenario: string;
  duration: number;
  faultStats: Map<string, FaultStats>;
  errors: Error[];
  success: boolean;
}

// ============================================================================
// Predefined Chaos Scenarios
// ============================================================================

/**
 * Predefined chaos scenarios for common failure modes.
 */
export const CHAOS_SCENARIOS = {
  /**
   * Storage unavailable - all storage operations fail.
   */
  storageUnavailable: (): ChaosScenario => ({
    name: 'Storage Unavailable',
    description: 'All storage operations fail immediately',
    faults: [
      { target: 'storage.get', config: { type: 'error', error: new Error('Storage unavailable') } },
      { target: 'storage.put', config: { type: 'error', error: new Error('Storage unavailable') } },
      { target: 'storage.delete', config: { type: 'error', error: new Error('Storage unavailable') } },
      { target: 'storage.list', config: { type: 'error', error: new Error('Storage unavailable') } },
    ],
  }),

  /**
   * Intermittent storage failures - storage fails 30% of the time.
   */
  intermittentStorageFailures: (): ChaosScenario => ({
    name: 'Intermittent Storage Failures',
    description: 'Storage operations fail 30% of the time',
    faults: [
      { target: 'storage.get', config: { type: 'error', error: new Error('Storage error'), probability: 0.3 } },
      { target: 'storage.put', config: { type: 'error', error: new Error('Storage error'), probability: 0.3 } },
    ],
  }),

  /**
   * High latency - all operations are delayed by 500ms.
   */
  highLatency: (): ChaosScenario => ({
    name: 'High Latency',
    description: 'All operations are delayed by 500ms',
    faults: [
      { target: 'storage.get', config: { type: 'delay', delayMs: 500 } },
      { target: 'storage.put', config: { type: 'delay', delayMs: 500 } },
      { target: 'network.fetch', config: { type: 'delay', delayMs: 500 } },
    ],
  }),

  /**
   * Network partition - isolates specified nodes.
   */
  networkPartition: (isolatedNodes: string[]): ChaosScenario => ({
    name: 'Network Partition',
    description: `Nodes ${isolatedNodes.join(', ')} are isolated from the network`,
    faults: [],
    partitions: {
      isolatedNodes: new Set(isolatedNodes),
      slowNodes: new Map(),
      droppingNodes: new Map(),
      allowedConnections: new Map(),
    },
  }),

  /**
   * Cascading failure - initial failure triggers additional failures.
   */
  cascadingFailure: (): ChaosScenario => ({
    name: 'Cascading Failure',
    description: 'Initial storage failure triggers network and database failures',
    faults: [
      { target: 'storage.get', config: { type: 'error', error: new Error('Storage failed'), maxOccurrences: 1 } },
      { target: 'network.fetch', config: { type: 'error', error: new Error('Network failed'), afterCalls: 1 } },
      { target: 'database.query', config: { type: 'error', error: new Error('Database failed'), afterCalls: 2 } },
    ],
  }),

  /**
   * Timeout cascade - operations time out, causing backpressure.
   */
  timeoutCascade: (): ChaosScenario => ({
    name: 'Timeout Cascade',
    description: 'Operations time out, causing system-wide slowdown',
    faults: [
      { target: 'storage.get', config: { type: 'timeout', delayMs: 10000 } },
      { target: 'storage.put', config: { type: 'timeout', delayMs: 10000 } },
    ],
  }),
};

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Create a chaos test suite that runs multiple scenarios.
 *
 * @param scenarios - Scenarios to run
 * @param testFn - Test function to run for each scenario
 *
 * @example
 * ```ts
 * describe('Chaos Tests', () => {
 *   createChaosTestSuite(
 *     [
 *       CHAOS_SCENARIOS.storageUnavailable(),
 *       CHAOS_SCENARIOS.intermittentStorageFailures(),
 *     ],
 *     async (context) => {
 *       // Test that system handles failures gracefully
 *       const result = await myService.doSomething();
 *       expect(result).toBeDefined();
 *     }
 *   );
 * });
 * ```
 */
export function createChaosTestSuite(
  scenarios: ChaosScenario[],
  testFn: (context: ChaosContext) => Promise<void>
): Array<{ name: string; run: () => Promise<ChaosScenarioResult> }> {
  return scenarios.map((scenario) => ({
    name: scenario.name,
    run: () => runChaosScenario(scenario, testFn),
  }));
}

/**
 * Assert that a chaos scenario completed successfully.
 *
 * @param result - The scenario result to check
 */
export function assertChaosSuccess(result: ChaosScenarioResult): void {
  if (!result.success) {
    const errorMessages = result.errors.map((e) => e.message).join('\n');
    throw new Error(`Chaos scenario "${result.scenario}" failed:\n${errorMessages}`);
  }
}

/**
 * Assert that a chaos scenario triggered expected failures.
 *
 * @param result - The scenario result to check
 * @param expectedFaults - Minimum number of expected faults per target
 */
export function assertChaosTriggered(
  result: ChaosScenarioResult,
  expectedFaults: Record<string, number>
): void {
  for (const [target, minFaults] of Object.entries(expectedFaults)) {
    const stats = result.faultStats.get(target);
    if (!stats || stats.faultsInjected < minFaults) {
      throw new Error(
        `Expected at least ${minFaults} faults at "${target}", got ${stats?.faultsInjected ?? 0}`
      );
    }
  }
}
