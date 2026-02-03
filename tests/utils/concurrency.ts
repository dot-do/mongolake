/**
 * Concurrency Test Utilities
 *
 * Test utilities for concurrent execution patterns:
 * - ParallelRunner for executing tasks concurrently with controlled parallelism
 * - RaceConditionDetector for detecting data races
 * - LockContentionSimulator for testing lock behavior
 * - ThreadPoolMock for simulating parallel worker pools
 * - Assertion helpers for concurrent behavior verification
 */

import { expect } from 'vitest';

// ============================================================================
// Types
// ============================================================================

/**
 * Result of a parallel task execution
 */
export interface TaskResult<T> {
  /** Task identifier */
  taskId: string;
  /** Task result value if successful */
  value?: T;
  /** Error if task failed */
  error?: Error;
  /** Whether the task completed successfully */
  success: boolean;
  /** Execution duration in milliseconds */
  durationMs: number;
  /** Start timestamp */
  startedAt: number;
  /** End timestamp */
  completedAt: number;
}

/**
 * Statistics from parallel execution
 */
export interface ParallelExecutionStats {
  /** Total number of tasks */
  totalTasks: number;
  /** Number of successful tasks */
  successCount: number;
  /** Number of failed tasks */
  failureCount: number;
  /** Total execution time in milliseconds */
  totalDurationMs: number;
  /** Average task duration in milliseconds */
  avgTaskDurationMs: number;
  /** Maximum concurrent tasks observed */
  maxConcurrency: number;
  /** Minimum task duration */
  minTaskDurationMs: number;
  /** Maximum task duration */
  maxTaskDurationMs: number;
}

/**
 * Options for the ParallelRunner
 */
export interface ParallelRunnerOptions {
  /** Maximum concurrent tasks (default: unlimited) */
  maxConcurrency?: number;
  /** Timeout per task in milliseconds (default: 30000) */
  taskTimeout?: number;
  /** Whether to stop on first error (default: false) */
  stopOnError?: boolean;
  /** Delay between task starts in milliseconds (default: 0) */
  taskStartDelay?: number;
}

/**
 * Race condition event for detection
 */
export interface RaceEvent {
  /** Event type */
  type: 'read' | 'write';
  /** Resource identifier */
  resource: string;
  /** Value before the operation */
  valueBefore: unknown;
  /** Value after the operation */
  valueAfter: unknown;
  /** Timestamp of the event */
  timestamp: number;
  /** Identifier of the accessor */
  accessorId: string;
}

/**
 * Detected race condition
 */
export interface RaceCondition {
  /** The resource where the race was detected */
  resource: string;
  /** The conflicting events */
  events: RaceEvent[];
  /** Description of the race condition */
  description: string;
  /** Time window in which the race occurred */
  timeWindowMs: number;
}

/**
 * Lock request for contention simulation
 */
export interface LockRequest {
  /** Lock name/resource */
  lockName: string;
  /** Requester identifier */
  requesterId: string;
  /** Request timestamp */
  requestedAt: number;
  /** Acquired timestamp (if acquired) */
  acquiredAt?: number;
  /** Released timestamp (if released) */
  releasedAt?: number;
  /** Whether the request timed out */
  timedOut: boolean;
}

/**
 * Lock contention statistics
 */
export interface LockContentionStats {
  /** Lock name */
  lockName: string;
  /** Total acquisition requests */
  totalRequests: number;
  /** Successful acquisitions */
  successfulAcquisitions: number;
  /** Timed out requests */
  timedOutRequests: number;
  /** Average wait time in milliseconds */
  avgWaitTimeMs: number;
  /** Maximum wait time in milliseconds */
  maxWaitTimeMs: number;
  /** Average hold time in milliseconds */
  avgHoldTimeMs: number;
  /** Maximum hold time in milliseconds */
  maxHoldTimeMs: number;
}

/**
 * Worker in the thread pool mock
 */
export interface MockWorker {
  /** Worker identifier */
  id: string;
  /** Whether the worker is currently busy */
  busy: boolean;
  /** Number of tasks processed */
  tasksProcessed: number;
  /** Current task being processed (if any) */
  currentTask?: string;
}

/**
 * Thread pool configuration
 */
export interface ThreadPoolConfig {
  /** Number of workers */
  workerCount: number;
  /** Task queue size limit (default: unlimited) */
  queueLimit?: number;
  /** Default task timeout in milliseconds */
  taskTimeout?: number;
}

// ============================================================================
// ParallelRunner
// ============================================================================

/**
 * Utility for running tasks in parallel with controlled concurrency.
 *
 * @example
 * ```ts
 * const runner = new ParallelRunner({ maxConcurrency: 5 });
 *
 * const results = await runner.run([
 *   async () => { await fetch('/api/1'); return 1; },
 *   async () => { await fetch('/api/2'); return 2; },
 *   async () => { await fetch('/api/3'); return 3; },
 * ]);
 *
 * expect(results.stats.successCount).toBe(3);
 * ```
 */
export class ParallelRunner {
  private options: Required<ParallelRunnerOptions>;
  private currentConcurrency = 0;
  private maxObservedConcurrency = 0;

  constructor(options: ParallelRunnerOptions = {}) {
    this.options = {
      maxConcurrency: options.maxConcurrency ?? Infinity,
      taskTimeout: options.taskTimeout ?? 30000,
      stopOnError: options.stopOnError ?? false,
      taskStartDelay: options.taskStartDelay ?? 0,
    };
  }

  /**
   * Run tasks in parallel with controlled concurrency.
   *
   * @param tasks - Array of async functions to execute
   * @returns Results and statistics
   */
  async run<T>(
    tasks: Array<() => Promise<T>>
  ): Promise<{ results: TaskResult<T>[]; stats: ParallelExecutionStats }> {
    const results: TaskResult<T>[] = [];
    const startTime = performance.now();
    let aborted = false;

    this.currentConcurrency = 0;
    this.maxObservedConcurrency = 0;

    // Create task entries with IDs
    const taskEntries = tasks.map((task, index) => ({
      id: `task-${index}`,
      task,
    }));

    // Process tasks with concurrency control
    const semaphore = new Semaphore(this.options.maxConcurrency);

    const runTask = async (entry: { id: string; task: () => Promise<T> }): Promise<TaskResult<T>> => {
      if (aborted) {
        return {
          taskId: entry.id,
          success: false,
          error: new Error('Execution aborted'),
          durationMs: 0,
          startedAt: performance.now(),
          completedAt: performance.now(),
        };
      }

      await semaphore.acquire();
      this.currentConcurrency++;
      this.maxObservedConcurrency = Math.max(this.maxObservedConcurrency, this.currentConcurrency);

      if (this.options.taskStartDelay > 0) {
        await delay(this.options.taskStartDelay);
      }

      const startedAt = performance.now();
      let result: TaskResult<T>;

      try {
        const value = await withTimeout(entry.task(), this.options.taskTimeout);
        const completedAt = performance.now();
        result = {
          taskId: entry.id,
          value,
          success: true,
          durationMs: completedAt - startedAt,
          startedAt,
          completedAt,
        };
      } catch (error) {
        const completedAt = performance.now();
        result = {
          taskId: entry.id,
          error: error instanceof Error ? error : new Error(String(error)),
          success: false,
          durationMs: completedAt - startedAt,
          startedAt,
          completedAt,
        };

        if (this.options.stopOnError) {
          aborted = true;
        }
      } finally {
        this.currentConcurrency--;
        semaphore.release();
      }

      return result;
    };

    const taskPromises = taskEntries.map(runTask);
    const taskResults = await Promise.all(taskPromises);
    results.push(...taskResults);

    const endTime = performance.now();
    const successfulResults = results.filter((r) => r.success);
    const durations = results.map((r) => r.durationMs);

    const stats: ParallelExecutionStats = {
      totalTasks: tasks.length,
      successCount: successfulResults.length,
      failureCount: results.length - successfulResults.length,
      totalDurationMs: endTime - startTime,
      avgTaskDurationMs: durations.reduce((a, b) => a + b, 0) / durations.length || 0,
      maxConcurrency: this.maxObservedConcurrency,
      minTaskDurationMs: Math.min(...durations) || 0,
      maxTaskDurationMs: Math.max(...durations) || 0,
    };

    return { results, stats };
  }

  /**
   * Run the same task multiple times in parallel.
   *
   * @param task - Async function to execute
   * @param count - Number of times to run
   * @returns Results and statistics
   */
  async runN<T>(
    task: (index: number) => Promise<T>,
    count: number
  ): Promise<{ results: TaskResult<T>[]; stats: ParallelExecutionStats }> {
    const tasks = Array.from({ length: count }, (_, i) => () => task(i));
    return this.run(tasks);
  }

  /**
   * Race multiple tasks and return the first to complete.
   *
   * @param tasks - Array of async functions to race
   * @returns First result and all results
   */
  async race<T>(
    tasks: Array<() => Promise<T>>
  ): Promise<{ winner: TaskResult<T>; all: TaskResult<T>[] }> {
    const results: TaskResult<T>[] = [];
    let winner: TaskResult<T> | null = null;
    let winnerResolve: ((result: TaskResult<T>) => void) | null = null;

    const winnerPromise = new Promise<TaskResult<T>>((resolve) => {
      winnerResolve = resolve;
    });

    const runTask = async (task: () => Promise<T>, index: number): Promise<TaskResult<T>> => {
      const taskId = `task-${index}`;
      const startedAt = performance.now();

      try {
        const value = await withTimeout(task(), this.options.taskTimeout);
        const completedAt = performance.now();
        const result: TaskResult<T> = {
          taskId,
          value,
          success: true,
          durationMs: completedAt - startedAt,
          startedAt,
          completedAt,
        };

        if (!winner) {
          winner = result;
          winnerResolve?.(result);
        }

        return result;
      } catch (error) {
        const completedAt = performance.now();
        const result: TaskResult<T> = {
          taskId,
          error: error instanceof Error ? error : new Error(String(error)),
          success: false,
          durationMs: completedAt - startedAt,
          startedAt,
          completedAt,
        };

        if (!winner) {
          winner = result;
          winnerResolve?.(result);
        }

        return result;
      }
    };

    const allPromises = tasks.map((task, index) => runTask(task, index));
    const [winnerResult, allResults] = await Promise.all([
      winnerPromise,
      Promise.all(allPromises),
    ]);

    results.push(...allResults);

    return { winner: winnerResult, all: results };
  }
}

// ============================================================================
// RaceConditionDetector
// ============================================================================

/**
 * Utility for detecting potential race conditions during concurrent operations.
 *
 * @example
 * ```ts
 * const detector = new RaceConditionDetector();
 *
 * // Wrap operations to track them
 * async function updateCounter(id: string) {
 *   const value = await detector.trackRead('counter', counter, id);
 *   await delay(10); // Simulate async work
 *   counter = value + 1;
 *   await detector.trackWrite('counter', counter, id);
 * }
 *
 * // Run concurrent operations
 * await Promise.all([
 *   updateCounter('worker-1'),
 *   updateCounter('worker-2'),
 * ]);
 *
 * // Check for races
 * const races = detector.detectRaces();
 * expect(races).toHaveLength(1); // Race detected!
 * ```
 */
export class RaceConditionDetector {
  private events: RaceEvent[] = [];
  private readonly timeWindowMs: number;

  constructor(options: { timeWindowMs?: number } = {}) {
    this.timeWindowMs = options.timeWindowMs ?? 50;
  }

  /**
   * Track a read operation.
   *
   * @param resource - Resource identifier
   * @param value - Current value
   * @param accessorId - Identifier of the reader
   * @returns The value (pass-through)
   */
  trackRead<T>(resource: string, value: T, accessorId: string): T {
    this.events.push({
      type: 'read',
      resource,
      valueBefore: value,
      valueAfter: value,
      timestamp: performance.now(),
      accessorId,
    });
    return value;
  }

  /**
   * Track a write operation.
   *
   * @param resource - Resource identifier
   * @param valueBefore - Value before write
   * @param valueAfter - Value after write
   * @param accessorId - Identifier of the writer
   */
  trackWrite(resource: string, valueBefore: unknown, valueAfter: unknown, accessorId: string): void {
    this.events.push({
      type: 'write',
      resource,
      valueBefore,
      valueAfter,
      timestamp: performance.now(),
      accessorId,
    });
  }

  /**
   * Track a read-modify-write operation.
   *
   * @param resource - Resource identifier
   * @param readValue - Value that was read
   * @param writeValue - Value that was written
   * @param accessorId - Identifier of the accessor
   */
  trackReadModifyWrite(resource: string, readValue: unknown, writeValue: unknown, accessorId: string): void {
    const timestamp = performance.now();
    this.events.push({
      type: 'read',
      resource,
      valueBefore: readValue,
      valueAfter: readValue,
      timestamp,
      accessorId,
    });
    this.events.push({
      type: 'write',
      resource,
      valueBefore: readValue,
      valueAfter: writeValue,
      timestamp: timestamp + 0.001, // Slightly later
      accessorId,
    });
  }

  /**
   * Detect race conditions in recorded events.
   *
   * @returns Array of detected race conditions
   */
  detectRaces(): RaceCondition[] {
    const races: RaceCondition[] = [];
    const byResource = this.groupEventsByResource();

    for (const [resource, events] of byResource.entries()) {
      const sortedEvents = events.sort((a, b) => a.timestamp - b.timestamp);

      for (let i = 0; i < sortedEvents.length; i++) {
        const event = sortedEvents[i];
        if (event.type !== 'write') continue;

        // Look for concurrent reads that might have stale data
        const concurrentReads = sortedEvents.filter(
          (e) =>
            e.type === 'read' &&
            e.accessorId !== event.accessorId &&
            Math.abs(e.timestamp - event.timestamp) < this.timeWindowMs &&
            e.timestamp < event.timestamp
        );

        // Look for concurrent writes
        const concurrentWrites = sortedEvents.filter(
          (e) =>
            e.type === 'write' &&
            e !== event &&
            Math.abs(e.timestamp - event.timestamp) < this.timeWindowMs
        );

        if (concurrentReads.length > 0) {
          races.push({
            resource,
            events: [event, ...concurrentReads],
            description: `Write by ${event.accessorId} may have overwritten changes read by ${concurrentReads.map((r) => r.accessorId).join(', ')}`,
            timeWindowMs: this.timeWindowMs,
          });
        }

        if (concurrentWrites.length > 0) {
          races.push({
            resource,
            events: [event, ...concurrentWrites],
            description: `Concurrent writes by ${[event, ...concurrentWrites].map((e) => e.accessorId).join(', ')}`,
            timeWindowMs: this.timeWindowMs,
          });
        }
      }
    }

    // Deduplicate races
    return this.deduplicateRaces(races);
  }

  /**
   * Check if any races were detected.
   */
  hasRaces(): boolean {
    return this.detectRaces().length > 0;
  }

  /**
   * Clear all recorded events.
   */
  reset(): void {
    this.events = [];
  }

  /**
   * Get all recorded events.
   */
  getEvents(): RaceEvent[] {
    return [...this.events];
  }

  private groupEventsByResource(): Map<string, RaceEvent[]> {
    const map = new Map<string, RaceEvent[]>();
    for (const event of this.events) {
      const existing = map.get(event.resource) ?? [];
      existing.push(event);
      map.set(event.resource, existing);
    }
    return map;
  }

  private deduplicateRaces(races: RaceCondition[]): RaceCondition[] {
    const seen = new Set<string>();
    return races.filter((race) => {
      const key = `${race.resource}:${race.events.map((e) => e.accessorId).sort().join(',')}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }
}

// ============================================================================
// LockContentionSimulator
// ============================================================================

/**
 * Utility for simulating and testing lock contention scenarios.
 *
 * @example
 * ```ts
 * const simulator = new LockContentionSimulator();
 *
 * // Simulate concurrent lock acquisition
 * const results = await Promise.all([
 *   simulator.acquireAndHold('resource-1', 'worker-1', 100),
 *   simulator.acquireAndHold('resource-1', 'worker-2', 100),
 *   simulator.acquireAndHold('resource-1', 'worker-3', 100),
 * ]);
 *
 * const stats = simulator.getStats('resource-1');
 * expect(stats.maxWaitTimeMs).toBeGreaterThan(0);
 * ```
 */
export class LockContentionSimulator {
  private locks = new Map<string, { holder: string; acquiredAt: number }>();
  private waitQueues = new Map<string, Array<{ requesterId: string; resolve: () => void; reject: (err: Error) => void }>>();
  private requests: LockRequest[] = [];
  private readonly defaultTimeout: number;

  constructor(options: { defaultTimeout?: number } = {}) {
    this.defaultTimeout = options.defaultTimeout ?? 5000;
  }

  /**
   * Acquire a lock, waiting if necessary.
   *
   * @param lockName - Name of the lock
   * @param requesterId - Identifier of the requester
   * @param timeout - Timeout in milliseconds
   * @returns Whether the lock was acquired
   */
  async acquire(lockName: string, requesterId: string, timeout?: number): Promise<boolean> {
    const request: LockRequest = {
      lockName,
      requesterId,
      requestedAt: performance.now(),
      timedOut: false,
    };
    this.requests.push(request);

    const actualTimeout = timeout ?? this.defaultTimeout;

    if (!this.locks.has(lockName)) {
      // Lock is free, acquire immediately
      this.locks.set(lockName, { holder: requesterId, acquiredAt: performance.now() });
      request.acquiredAt = performance.now();
      return true;
    }

    // Lock is held, wait in queue
    const waitQueue = this.waitQueues.get(lockName) ?? [];
    this.waitQueues.set(lockName, waitQueue);

    return new Promise<boolean>((resolve) => {
      const timeoutId = setTimeout(() => {
        // Remove from queue
        const index = waitQueue.findIndex((w) => w.requesterId === requesterId);
        if (index !== -1) {
          waitQueue.splice(index, 1);
        }
        request.timedOut = true;
        resolve(false);
      }, actualTimeout);

      waitQueue.push({
        requesterId,
        resolve: () => {
          clearTimeout(timeoutId);
          this.locks.set(lockName, { holder: requesterId, acquiredAt: performance.now() });
          request.acquiredAt = performance.now();
          resolve(true);
        },
        reject: () => {
          clearTimeout(timeoutId);
          resolve(false);
        },
      });
    });
  }

  /**
   * Release a held lock.
   *
   * @param lockName - Name of the lock
   * @param requesterId - Identifier of the holder
   */
  release(lockName: string, requesterId: string): void {
    const lock = this.locks.get(lockName);
    if (!lock || lock.holder !== requesterId) {
      throw new Error(`Lock ${lockName} is not held by ${requesterId}`);
    }

    // Update the request record
    const request = this.requests.find(
      (r) => r.lockName === lockName && r.requesterId === requesterId && r.acquiredAt && !r.releasedAt
    );
    if (request) {
      request.releasedAt = performance.now();
    }

    this.locks.delete(lockName);

    // Wake up next waiter
    const waitQueue = this.waitQueues.get(lockName);
    if (waitQueue && waitQueue.length > 0) {
      const next = waitQueue.shift()!;
      next.resolve();
    }
  }

  /**
   * Acquire a lock, hold it for a duration, then release.
   *
   * @param lockName - Name of the lock
   * @param requesterId - Identifier of the requester
   * @param holdDurationMs - How long to hold the lock
   * @param timeout - Acquisition timeout
   * @returns Whether the operation completed successfully
   */
  async acquireAndHold(
    lockName: string,
    requesterId: string,
    holdDurationMs: number,
    timeout?: number
  ): Promise<boolean> {
    const acquired = await this.acquire(lockName, requesterId, timeout);
    if (!acquired) {
      return false;
    }

    await delay(holdDurationMs);
    this.release(lockName, requesterId);
    return true;
  }

  /**
   * Execute a function while holding a lock.
   *
   * @param lockName - Name of the lock
   * @param requesterId - Identifier of the requester
   * @param fn - Function to execute
   * @param timeout - Acquisition timeout
   * @returns Result of the function
   */
  async withLock<T>(
    lockName: string,
    requesterId: string,
    fn: () => Promise<T>,
    timeout?: number
  ): Promise<T> {
    const acquired = await this.acquire(lockName, requesterId, timeout);
    if (!acquired) {
      throw new Error(`Failed to acquire lock ${lockName} within timeout`);
    }

    try {
      return await fn();
    } finally {
      this.release(lockName, requesterId);
    }
  }

  /**
   * Get contention statistics for a lock.
   *
   * @param lockName - Name of the lock
   * @returns Lock contention statistics
   */
  getStats(lockName: string): LockContentionStats {
    const lockRequests = this.requests.filter((r) => r.lockName === lockName);
    const successful = lockRequests.filter((r) => r.acquiredAt !== undefined);
    const timedOut = lockRequests.filter((r) => r.timedOut);
    const completed = successful.filter((r) => r.releasedAt !== undefined);

    const waitTimes = successful.map((r) => (r.acquiredAt ?? 0) - r.requestedAt);
    const holdTimes = completed.map((r) => (r.releasedAt ?? 0) - (r.acquiredAt ?? 0));

    return {
      lockName,
      totalRequests: lockRequests.length,
      successfulAcquisitions: successful.length,
      timedOutRequests: timedOut.length,
      avgWaitTimeMs: waitTimes.length > 0 ? waitTimes.reduce((a, b) => a + b, 0) / waitTimes.length : 0,
      maxWaitTimeMs: waitTimes.length > 0 ? Math.max(...waitTimes) : 0,
      avgHoldTimeMs: holdTimes.length > 0 ? holdTimes.reduce((a, b) => a + b, 0) / holdTimes.length : 0,
      maxHoldTimeMs: holdTimes.length > 0 ? Math.max(...holdTimes) : 0,
    };
  }

  /**
   * Get all lock requests.
   */
  getRequests(): LockRequest[] {
    return [...this.requests];
  }

  /**
   * Check if a lock is currently held.
   */
  isLocked(lockName: string): boolean {
    return this.locks.has(lockName);
  }

  /**
   * Get the current holder of a lock.
   */
  getHolder(lockName: string): string | null {
    return this.locks.get(lockName)?.holder ?? null;
  }

  /**
   * Reset all state.
   */
  reset(): void {
    this.locks.clear();
    this.waitQueues.clear();
    this.requests = [];
  }
}

// ============================================================================
// ThreadPoolMock
// ============================================================================

/**
 * Mock thread pool for testing parallel worker scenarios.
 *
 * @example
 * ```ts
 * const pool = new ThreadPoolMock({ workerCount: 4 });
 *
 * // Submit tasks
 * const results = await Promise.all([
 *   pool.submit(() => expensiveOperation(1)),
 *   pool.submit(() => expensiveOperation(2)),
 *   pool.submit(() => expensiveOperation(3)),
 * ]);
 *
 * // Check utilization
 * const stats = pool.getStats();
 * expect(stats.tasksCompleted).toBe(3);
 * ```
 */
export class ThreadPoolMock {
  private workers: MockWorker[];
  private taskQueue: Array<{
    id: string;
    task: () => Promise<unknown>;
    resolve: (value: unknown) => void;
    reject: (error: Error) => void;
  }> = [];
  private taskCounter = 0;
  private readonly config: Required<ThreadPoolConfig>;
  private tasksCompleted = 0;
  private tasksFailed = 0;

  constructor(config: ThreadPoolConfig) {
    this.config = {
      workerCount: config.workerCount,
      queueLimit: config.queueLimit ?? Infinity,
      taskTimeout: config.taskTimeout ?? 30000,
    };

    this.workers = Array.from({ length: config.workerCount }, (_, i) => ({
      id: `worker-${i}`,
      busy: false,
      tasksProcessed: 0,
    }));
  }

  /**
   * Submit a task to the pool.
   *
   * @param task - Async function to execute
   * @returns Task result
   */
  async submit<T>(task: () => Promise<T>): Promise<T> {
    const taskId = `task-${++this.taskCounter}`;

    // Check queue limit
    if (this.taskQueue.length >= this.config.queueLimit) {
      throw new Error('Thread pool queue is full');
    }

    // Find available worker
    const availableWorker = this.workers.find((w) => !w.busy);

    if (availableWorker) {
      // Execute immediately
      return this.executeOnWorker(availableWorker, taskId, task);
    }

    // Queue the task
    return new Promise<T>((resolve, reject) => {
      this.taskQueue.push({
        id: taskId,
        task: task as () => Promise<unknown>,
        resolve: resolve as (value: unknown) => void,
        reject,
      });
    });
  }

  /**
   * Submit multiple tasks and wait for all to complete.
   *
   * @param tasks - Array of async functions
   * @returns Array of results
   */
  async submitAll<T>(tasks: Array<() => Promise<T>>): Promise<T[]> {
    return Promise.all(tasks.map((task) => this.submit(task)));
  }

  /**
   * Get current pool statistics.
   */
  getStats(): {
    workerCount: number;
    busyWorkers: number;
    idleWorkers: number;
    queuedTasks: number;
    tasksCompleted: number;
    tasksFailed: number;
    workerStats: Array<{ id: string; tasksProcessed: number }>;
  } {
    const busyWorkers = this.workers.filter((w) => w.busy).length;
    return {
      workerCount: this.config.workerCount,
      busyWorkers,
      idleWorkers: this.config.workerCount - busyWorkers,
      queuedTasks: this.taskQueue.length,
      tasksCompleted: this.tasksCompleted,
      tasksFailed: this.tasksFailed,
      workerStats: this.workers.map((w) => ({ id: w.id, tasksProcessed: w.tasksProcessed })),
    };
  }

  /**
   * Get worker states.
   */
  getWorkers(): MockWorker[] {
    return this.workers.map((w) => ({ ...w }));
  }

  /**
   * Wait for all queued tasks to complete.
   */
  async drain(): Promise<void> {
    while (this.taskQueue.length > 0 || this.workers.some((w) => w.busy)) {
      await delay(10);
    }
  }

  /**
   * Shutdown the pool (rejects queued tasks).
   */
  shutdown(): void {
    for (const queued of this.taskQueue) {
      queued.reject(new Error('Pool shutdown'));
    }
    this.taskQueue = [];
  }

  /**
   * Reset pool state.
   */
  reset(): void {
    this.shutdown();
    this.tasksCompleted = 0;
    this.tasksFailed = 0;
    this.taskCounter = 0;
    for (const worker of this.workers) {
      worker.busy = false;
      worker.tasksProcessed = 0;
      worker.currentTask = undefined;
    }
  }

  private async executeOnWorker<T>(worker: MockWorker, taskId: string, task: () => Promise<T>): Promise<T> {
    worker.busy = true;
    worker.currentTask = taskId;

    try {
      const result = await withTimeout(task(), this.config.taskTimeout);
      worker.tasksProcessed++;
      this.tasksCompleted++;
      return result;
    } catch (error) {
      this.tasksFailed++;
      throw error;
    } finally {
      worker.busy = false;
      worker.currentTask = undefined;

      // Process next queued task
      this.processNextTask(worker);
    }
  }

  private processNextTask(worker: MockWorker): void {
    const next = this.taskQueue.shift();
    if (next) {
      this.executeOnWorker(worker, next.id, next.task)
        .then(next.resolve)
        .catch(next.reject);
    }
  }
}

// ============================================================================
// Assertion Helpers for Concurrency
// ============================================================================

/**
 * Assert that operations complete in a consistent order (no interleaving).
 *
 * @param operationResults - Array of operation results with sequence numbers
 * @param message - Custom error message
 */
export function assertNoInterleaving(
  operationResults: Array<{ sequence: number; workerId: string }>,
  message?: string
): void {
  const byWorker = new Map<string, number[]>();
  for (const result of operationResults) {
    const existing = byWorker.get(result.workerId) ?? [];
    existing.push(result.sequence);
    byWorker.set(result.workerId, existing);
  }

  for (const [workerId, sequences] of byWorker) {
    for (let i = 1; i < sequences.length; i++) {
      expect(
        sequences[i] > sequences[i - 1],
        message ?? `Operations for ${workerId} are interleaved: ${sequences.join(', ')}`
      ).toBe(true);
    }
  }
}

/**
 * Assert that values are eventually consistent after concurrent updates.
 *
 * @param getValue - Function to get current value
 * @param expectedValue - Expected final value
 * @param timeoutMs - Timeout in milliseconds
 * @param message - Custom error message
 */
export async function assertEventuallyConsistent<T>(
  getValue: () => T | Promise<T>,
  expectedValue: T,
  timeoutMs: number = 1000,
  message?: string
): Promise<void> {
  const startTime = performance.now();

  while (performance.now() - startTime < timeoutMs) {
    const value = await getValue();
    if (deepEqual(value, expectedValue)) {
      return;
    }
    await delay(10);
  }

  const finalValue = await getValue();
  expect(
    finalValue,
    message ?? `Value did not converge to expected value within ${timeoutMs}ms`
  ).toEqual(expectedValue);
}

/**
 * Assert that a value monotonically increases (for counters, sequences, etc.).
 *
 * @param values - Array of observed values
 * @param message - Custom error message
 */
export function assertMonotonicallyIncreasing(values: number[], message?: string): void {
  for (let i = 1; i < values.length; i++) {
    expect(
      values[i] >= values[i - 1],
      message ?? `Values are not monotonically increasing at index ${i}: ${values[i - 1]} -> ${values[i]}`
    ).toBe(true);
  }
}

/**
 * Assert that no duplicate values exist (for unique ID generation).
 *
 * @param values - Array of values to check
 * @param message - Custom error message
 */
export function assertNoDuplicates<T>(values: T[], message?: string): void {
  const seen = new Set<string>();
  const duplicates: T[] = [];

  for (const value of values) {
    const key = JSON.stringify(value);
    if (seen.has(key)) {
      duplicates.push(value);
    }
    seen.add(key);
  }

  expect(
    duplicates.length,
    message ?? `Found ${duplicates.length} duplicate values: ${duplicates.slice(0, 5).map((d) => JSON.stringify(d)).join(', ')}`
  ).toBe(0);
}

/**
 * Assert that concurrent operations respect mutual exclusion.
 *
 * @param operationLog - Log of operations with timestamps
 * @param criticalSections - Array of [start, end] timestamps for critical sections
 * @param message - Custom error message
 */
export function assertMutualExclusion(
  criticalSections: Array<{ workerId: string; startTime: number; endTime: number }>,
  message?: string
): void {
  for (let i = 0; i < criticalSections.length; i++) {
    for (let j = i + 1; j < criticalSections.length; j++) {
      const a = criticalSections[i];
      const b = criticalSections[j];

      if (a.workerId === b.workerId) continue;

      const overlaps = a.startTime < b.endTime && b.startTime < a.endTime;
      expect(
        overlaps,
        message ?? `Critical sections overlap: ${a.workerId} (${a.startTime}-${a.endTime}) and ${b.workerId} (${b.startTime}-${b.endTime})`
      ).toBe(false);
    }
  }
}

/**
 * Assert that all concurrent operations completed within a time window.
 *
 * @param results - Task results with timing information
 * @param maxDurationMs - Maximum allowed total duration
 * @param message - Custom error message
 */
export function assertParallelCompletion(
  results: Array<{ startedAt: number; completedAt: number }>,
  maxDurationMs: number,
  message?: string
): void {
  if (results.length === 0) return;

  const overallStart = Math.min(...results.map((r) => r.startedAt));
  const overallEnd = Math.max(...results.map((r) => r.completedAt));
  const totalDuration = overallEnd - overallStart;

  expect(
    totalDuration <= maxDurationMs,
    message ?? `Operations took ${totalDuration.toFixed(2)}ms, expected at most ${maxDurationMs}ms`
  ).toBe(true);
}

/**
 * Assert that concurrent read/write operations maintain consistency.
 *
 * @param readResults - Values read during concurrent operations
 * @param validValues - Set of valid values that could have been read
 * @param message - Custom error message
 */
export function assertReadConsistency<T>(
  readResults: T[],
  validValues: T[],
  message?: string
): void {
  const validSet = new Set(validValues.map((v) => JSON.stringify(v)));

  for (const result of readResults) {
    const key = JSON.stringify(result);
    expect(
      validSet.has(key),
      message ?? `Read invalid value: ${key}. Valid values: ${[...validSet].join(', ')}`
    ).toBe(true);
  }
}

// ============================================================================
// Helper Utilities
// ============================================================================

/**
 * Simple semaphore for concurrency control.
 */
class Semaphore {
  private permits: number;
  private waitQueue: Array<() => void> = [];

  constructor(permits: number) {
    this.permits = permits;
  }

  async acquire(): Promise<void> {
    if (this.permits > 0) {
      this.permits--;
      return;
    }

    return new Promise<void>((resolve) => {
      this.waitQueue.push(resolve);
    });
  }

  release(): void {
    const next = this.waitQueue.shift();
    if (next) {
      next();
    } else {
      this.permits++;
    }
  }
}

/**
 * Delay for a specified duration.
 *
 * @param ms - Milliseconds to delay
 */
export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Wrap a promise with a timeout.
 *
 * @param promise - Promise to wrap
 * @param timeoutMs - Timeout in milliseconds
 * @returns Promise that rejects on timeout
 */
export function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      reject(new Error(`Operation timed out after ${timeoutMs}ms`));
    }, timeoutMs);

    promise
      .then((result) => {
        clearTimeout(timeoutId);
        resolve(result);
      })
      .catch((error) => {
        clearTimeout(timeoutId);
        reject(error);
      });
  });
}

/**
 * Run a function with retry on failure.
 *
 * @param fn - Function to run
 * @param maxRetries - Maximum retry attempts
 * @param retryDelayMs - Delay between retries
 * @returns Result of the function
 */
export async function withRetry<T>(
  fn: () => Promise<T>,
  maxRetries: number = 3,
  retryDelayMs: number = 100
): Promise<T> {
  let lastError: Error | undefined;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      if (attempt < maxRetries) {
        await delay(retryDelayMs);
      }
    }
  }

  throw lastError;
}

/**
 * Deep equality check for objects.
 */
function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a !== typeof b) return false;
  if (a === null || b === null) return a === b;
  if (typeof a !== 'object') return false;

  const aObj = a as Record<string, unknown>;
  const bObj = b as Record<string, unknown>;

  const aKeys = Object.keys(aObj);
  const bKeys = Object.keys(bObj);

  if (aKeys.length !== bKeys.length) return false;

  for (const key of aKeys) {
    if (!deepEqual(aObj[key], bObj[key])) return false;
  }

  return true;
}

/**
 * Create a barrier for synchronizing concurrent tasks.
 *
 * @param count - Number of tasks to wait for
 * @returns Barrier object
 */
export function createBarrier(count: number): {
  wait: () => Promise<void>;
  reset: () => void;
} {
  let remaining = count;
  let resolvers: Array<() => void> = [];

  return {
    async wait(): Promise<void> {
      return new Promise<void>((resolve) => {
        remaining--;
        if (remaining <= 0) {
          // Release all waiters
          for (const resolver of resolvers) {
            resolver();
          }
          resolve();
        } else {
          resolvers.push(resolve);
        }
      });
    },

    reset(): void {
      remaining = count;
      resolvers = [];
    },
  };
}

/**
 * Create a countdown latch for synchronization.
 *
 * @param count - Initial count
 * @returns Latch object
 */
export function createLatch(count: number): {
  countDown: () => void;
  wait: () => Promise<void>;
  getCount: () => number;
} {
  let remaining = count;
  let resolver: (() => void) | null = null;
  let promise: Promise<void> | null = null;

  return {
    countDown(): void {
      remaining--;
      if (remaining <= 0 && resolver) {
        resolver();
      }
    },

    async wait(): Promise<void> {
      if (remaining <= 0) return;

      if (!promise) {
        promise = new Promise<void>((resolve) => {
          resolver = resolve;
        });
      }

      return promise;
    },

    getCount(): number {
      return remaining;
    },
  };
}
