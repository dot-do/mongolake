/**
 * Concurrency Test Utilities - Unit Tests
 *
 * Tests for the concurrency testing utilities themselves.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  ParallelRunner,
  RaceConditionDetector,
  LockContentionSimulator,
  ThreadPoolMock,
  assertNoInterleaving,
  assertMonotonicallyIncreasing,
  assertNoDuplicates,
  assertMutualExclusion,
  assertParallelCompletion,
  delay,
  withTimeout,
  withRetry,
  createBarrier,
  createLatch,
} from '../../utils/concurrency.js';

describe('Concurrency Test Utilities', () => {
  // ==========================================================================
  // ParallelRunner Tests
  // ==========================================================================

  describe('ParallelRunner', () => {
    it('should run tasks in parallel', async () => {
      const runner = new ParallelRunner();
      const startTime = performance.now();

      const results = await runner.run([
        async () => { await delay(50); return 1; },
        async () => { await delay(50); return 2; },
        async () => { await delay(50); return 3; },
      ]);

      const duration = performance.now() - startTime;

      expect(results.stats.successCount).toBe(3);
      expect(results.results.map((r) => r.value).sort()).toEqual([1, 2, 3]);
      // Should complete in ~50ms, not 150ms (sequential)
      expect(duration).toBeLessThan(150);
    });

    it('should respect maxConcurrency', async () => {
      const runner = new ParallelRunner({ maxConcurrency: 2 });

      const results = await runner.run([
        async () => { await delay(50); return 1; },
        async () => { await delay(50); return 2; },
        async () => { await delay(50); return 3; },
        async () => { await delay(50); return 4; },
      ]);

      expect(results.stats.successCount).toBe(4);
      expect(results.stats.maxConcurrency).toBeLessThanOrEqual(2);
    });

    it('should handle task failures', async () => {
      const runner = new ParallelRunner();

      const results = await runner.run([
        async () => 1,
        async () => { throw new Error('Task failed'); },
        async () => 3,
      ]);

      expect(results.stats.successCount).toBe(2);
      expect(results.stats.failureCount).toBe(1);
      expect(results.results.find((r) => !r.success)?.error?.message).toBe('Task failed');
    });

    it('should stop on error when configured', async () => {
      const runner = new ParallelRunner({ stopOnError: true });

      // With stopOnError, once a task fails, other tasks should be marked as aborted
      // This tests the abort mechanism
      const results = await runner.run([
        async () => { throw new Error('First failure'); },
        async () => { await delay(100); return 'should be aborted'; },
      ]);

      expect(results.stats.failureCount).toBeGreaterThanOrEqual(1);
      // At least one failure should be the original error
      const firstFailure = results.results.find((r) => r.error?.message === 'First failure');
      expect(firstFailure).toBeDefined();
      expect(firstFailure?.success).toBe(false);
    });

    it('should timeout slow tasks', async () => {
      const runner = new ParallelRunner({ taskTimeout: 50 });

      const results = await runner.run([
        async () => { await delay(10); return 1; },
        async () => { await delay(200); return 2; }, // Will timeout
      ]);

      expect(results.stats.successCount).toBe(1);
      expect(results.stats.failureCount).toBe(1);
      expect(results.results.find((r) => !r.success)?.error?.message).toContain('timed out');
    });

    it('should run N copies of a task', async () => {
      const runner = new ParallelRunner();
      const indices: number[] = [];

      const results = await runner.runN(async (i) => {
        indices.push(i);
        return i * 2;
      }, 5);

      expect(results.stats.successCount).toBe(5);
      expect(indices.sort((a, b) => a - b)).toEqual([0, 1, 2, 3, 4]);
      expect(results.results.map((r) => r.value).sort((a, b) => (a ?? 0) - (b ?? 0))).toEqual([0, 2, 4, 6, 8]);
    });

    it('should race tasks and return winner', async () => {
      const runner = new ParallelRunner();

      const { winner, all } = await runner.race([
        async () => { await delay(100); return 'slow'; },
        async () => { await delay(10); return 'fast'; },
        async () => { await delay(50); return 'medium'; },
      ]);

      expect(winner.value).toBe('fast');
      expect(all.length).toBe(3);
    });
  });

  // ==========================================================================
  // RaceConditionDetector Tests
  // ==========================================================================

  describe('RaceConditionDetector', () => {
    let detector: RaceConditionDetector;

    beforeEach(() => {
      detector = new RaceConditionDetector({ timeWindowMs: 100 });
    });

    it('should track read and write operations', () => {
      detector.trackRead('counter', 0, 'worker-1');
      detector.trackWrite('counter', 0, 1, 'worker-1');

      const events = detector.getEvents();
      expect(events).toHaveLength(2);
      expect(events[0].type).toBe('read');
      expect(events[1].type).toBe('write');
    });

    it('should detect concurrent write races', () => {
      // Simulate two concurrent writes
      detector.trackWrite('counter', 0, 1, 'worker-1');
      detector.trackWrite('counter', 0, 2, 'worker-2');

      const races = detector.detectRaces();
      expect(races.length).toBeGreaterThan(0);
      expect(races[0].description).toContain('Concurrent writes');
    });

    it('should detect read-write races', () => {
      // Worker 1 reads, then Worker 2 writes (race condition)
      detector.trackRead('counter', 0, 'worker-1');
      detector.trackWrite('counter', 0, 1, 'worker-2');

      const races = detector.detectRaces();
      expect(races.length).toBeGreaterThan(0);
    });

    it('should not detect races for sequential operations', async () => {
      detector.trackRead('counter', 0, 'worker-1');
      detector.trackWrite('counter', 0, 1, 'worker-1');

      await delay(200); // Outside time window

      detector.trackRead('counter', 1, 'worker-2');
      detector.trackWrite('counter', 1, 2, 'worker-2');

      const races = detector.detectRaces();
      // Should not detect cross-worker races due to time gap
      const crossWorkerRaces = races.filter(
        (r) => r.events.some((e) => e.accessorId === 'worker-1') && r.events.some((e) => e.accessorId === 'worker-2')
      );
      expect(crossWorkerRaces).toHaveLength(0);
    });

    it('should support read-modify-write tracking', () => {
      detector.trackReadModifyWrite('counter', 0, 1, 'worker-1');

      const events = detector.getEvents();
      expect(events).toHaveLength(2);
      expect(events[0].type).toBe('read');
      expect(events[1].type).toBe('write');
    });

    it('should reset state', () => {
      detector.trackRead('counter', 0, 'worker-1');
      detector.reset();

      expect(detector.getEvents()).toHaveLength(0);
    });
  });

  // ==========================================================================
  // LockContentionSimulator Tests
  // ==========================================================================

  describe('LockContentionSimulator', () => {
    let simulator: LockContentionSimulator;

    beforeEach(() => {
      simulator = new LockContentionSimulator({ defaultTimeout: 1000 });
    });

    it('should acquire an available lock', async () => {
      const acquired = await simulator.acquire('resource', 'worker-1');
      expect(acquired).toBe(true);
      expect(simulator.isLocked('resource')).toBe(true);
      expect(simulator.getHolder('resource')).toBe('worker-1');
    });

    it('should queue requests for held locks', async () => {
      await simulator.acquire('resource', 'worker-1');

      const acquirePromise = simulator.acquire('resource', 'worker-2', 100);

      // Worker-2 should be waiting
      expect(simulator.getHolder('resource')).toBe('worker-1');

      // Release and worker-2 should get the lock
      simulator.release('resource', 'worker-1');

      const acquired = await acquirePromise;
      expect(acquired).toBe(true);
      expect(simulator.getHolder('resource')).toBe('worker-2');
    });

    it('should timeout on lock acquisition', async () => {
      await simulator.acquire('resource', 'worker-1');

      const acquired = await simulator.acquire('resource', 'worker-2', 50);

      expect(acquired).toBe(false);
      expect(simulator.getHolder('resource')).toBe('worker-1');
    });

    it('should handle acquire and hold', async () => {
      const result = await simulator.acquireAndHold('resource', 'worker-1', 50);

      expect(result).toBe(true);
      expect(simulator.isLocked('resource')).toBe(false);
    });

    it('should execute with lock', async () => {
      const result = await simulator.withLock('resource', 'worker-1', async () => {
        expect(simulator.isLocked('resource')).toBe(true);
        return 'success';
      });

      expect(result).toBe('success');
      expect(simulator.isLocked('resource')).toBe(false);
    });

    it('should calculate contention statistics', async () => {
      await simulator.acquireAndHold('resource', 'worker-1', 20);
      await simulator.acquireAndHold('resource', 'worker-2', 20);

      const stats = simulator.getStats('resource');
      expect(stats.totalRequests).toBe(2);
      expect(stats.successfulAcquisitions).toBe(2);
      expect(stats.timedOutRequests).toBe(0);
    });

    it('should throw when releasing unheld lock', async () => {
      expect(() => simulator.release('resource', 'worker-1')).toThrow();
    });

    it('should throw when wrong holder releases', async () => {
      await simulator.acquire('resource', 'worker-1');
      expect(() => simulator.release('resource', 'worker-2')).toThrow();
    });
  });

  // ==========================================================================
  // ThreadPoolMock Tests
  // ==========================================================================

  describe('ThreadPoolMock', () => {
    it('should process tasks with limited workers', async () => {
      const pool = new ThreadPoolMock({ workerCount: 2 });

      const results = await pool.submitAll([
        async () => { await delay(20); return 1; },
        async () => { await delay(20); return 2; },
        async () => { await delay(20); return 3; },
      ]);

      expect(results).toEqual([1, 2, 3]);

      const stats = pool.getStats();
      expect(stats.tasksCompleted).toBe(3);
    });

    it('should queue tasks when all workers busy', async () => {
      const pool = new ThreadPoolMock({ workerCount: 1 });

      const promise1 = pool.submit(async () => { await delay(50); return 1; });
      const promise2 = pool.submit(async () => { return 2; });

      const stats = pool.getStats();
      expect(stats.busyWorkers).toBe(1);
      expect(stats.queuedTasks).toBe(1);

      const results = await Promise.all([promise1, promise2]);
      expect(results).toEqual([1, 2]);
    });

    it('should reject when queue is full', async () => {
      const pool = new ThreadPoolMock({ workerCount: 1, queueLimit: 1 });

      pool.submit(async () => { await delay(100); return 1; });
      pool.submit(async () => { return 2; }); // Queued

      await expect(pool.submit(async () => 3)).rejects.toThrow('queue is full');
    });

    it('should track worker statistics', async () => {
      const pool = new ThreadPoolMock({ workerCount: 2 });

      await pool.submitAll([
        async () => 1,
        async () => 2,
        async () => 3,
        async () => 4,
      ]);

      const stats = pool.getStats();
      expect(stats.tasksCompleted).toBe(4);

      const totalProcessed = stats.workerStats.reduce((sum, w) => sum + w.tasksProcessed, 0);
      expect(totalProcessed).toBe(4);
    });

    it('should drain the queue', async () => {
      const pool = new ThreadPoolMock({ workerCount: 1 });

      pool.submit(async () => { await delay(30); return 1; });
      pool.submit(async () => { await delay(30); return 2; });

      await pool.drain();

      const stats = pool.getStats();
      expect(stats.queuedTasks).toBe(0);
      expect(stats.busyWorkers).toBe(0);
    });

    it('should shutdown and reject queued tasks', async () => {
      const pool = new ThreadPoolMock({ workerCount: 1 });

      pool.submit(async () => { await delay(100); return 1; });
      const promise2 = pool.submit(async () => 2);

      pool.shutdown();

      await expect(promise2).rejects.toThrow('shutdown');
    });
  });

  // ==========================================================================
  // Assertion Helpers Tests
  // ==========================================================================

  describe('Assertion Helpers', () => {
    describe('assertNoInterleaving', () => {
      it('should pass for non-interleaved operations', () => {
        const results = [
          { sequence: 1, workerId: 'a' },
          { sequence: 2, workerId: 'a' },
          { sequence: 3, workerId: 'b' },
          { sequence: 4, workerId: 'b' },
        ];

        expect(() => assertNoInterleaving(results)).not.toThrow();
      });
    });

    describe('assertMonotonicallyIncreasing', () => {
      it('should pass for increasing values', () => {
        expect(() => assertMonotonicallyIncreasing([1, 2, 3, 4, 5])).not.toThrow();
        expect(() => assertMonotonicallyIncreasing([1, 1, 2, 3, 3])).not.toThrow(); // Non-strict
      });

      it('should fail for decreasing values', () => {
        expect(() => assertMonotonicallyIncreasing([1, 2, 1])).toThrow();
      });
    });

    describe('assertNoDuplicates', () => {
      it('should pass for unique values', () => {
        expect(() => assertNoDuplicates([1, 2, 3, 4, 5])).not.toThrow();
      });

      it('should fail for duplicate values', () => {
        expect(() => assertNoDuplicates([1, 2, 2, 3])).toThrow();
      });
    });

    describe('assertMutualExclusion', () => {
      it('should pass for non-overlapping sections', () => {
        const sections = [
          { workerId: 'a', startTime: 0, endTime: 10 },
          { workerId: 'b', startTime: 15, endTime: 25 },
        ];

        expect(() => assertMutualExclusion(sections)).not.toThrow();
      });

      it('should fail for overlapping sections', () => {
        const sections = [
          { workerId: 'a', startTime: 0, endTime: 20 },
          { workerId: 'b', startTime: 10, endTime: 30 },
        ];

        expect(() => assertMutualExclusion(sections)).toThrow();
      });
    });

    describe('assertParallelCompletion', () => {
      it('should pass when operations complete within limit', () => {
        const results = [
          { startedAt: 0, completedAt: 50 },
          { startedAt: 10, completedAt: 60 },
        ];

        expect(() => assertParallelCompletion(results, 100)).not.toThrow();
      });

      it('should fail when operations exceed limit', () => {
        const results = [
          { startedAt: 0, completedAt: 50 },
          { startedAt: 10, completedAt: 200 },
        ];

        expect(() => assertParallelCompletion(results, 100)).toThrow();
      });
    });
  });

  // ==========================================================================
  // Utility Function Tests
  // ==========================================================================

  describe('Utility Functions', () => {
    describe('delay', () => {
      it('should delay for specified time', async () => {
        const start = performance.now();
        await delay(50);
        const elapsed = performance.now() - start;

        expect(elapsed).toBeGreaterThanOrEqual(45);
      });
    });

    describe('withTimeout', () => {
      it('should resolve fast promises', async () => {
        const result = await withTimeout(Promise.resolve('success'), 100);
        expect(result).toBe('success');
      });

      it('should reject slow promises', async () => {
        await expect(withTimeout(delay(200), 50)).rejects.toThrow('timed out');
      });
    });

    describe('withRetry', () => {
      it('should succeed on first try', async () => {
        const result = await withRetry(async () => 'success');
        expect(result).toBe('success');
      });

      it('should retry on failure', async () => {
        let attempts = 0;
        const result = await withRetry(async () => {
          attempts++;
          if (attempts < 3) throw new Error('Fail');
          return 'success';
        }, 3, 10);

        expect(result).toBe('success');
        expect(attempts).toBe(3);
      });

      it('should throw after max retries', async () => {
        let attempts = 0;
        await expect(
          withRetry(async () => {
            attempts++;
            throw new Error('Always fail');
          }, 2, 10)
        ).rejects.toThrow('Always fail');

        expect(attempts).toBe(3); // Initial + 2 retries
      });
    });

    describe('createBarrier', () => {
      it('should synchronize multiple tasks', async () => {
        const barrier = createBarrier(3);
        const arrivals: number[] = [];

        const tasks = [0, 1, 2].map(async (i) => {
          await delay(i * 20);
          arrivals.push(i);
          await barrier.wait();
          return i;
        });

        const results = await Promise.all(tasks);
        expect(results).toEqual([0, 1, 2]);
        expect(arrivals).toHaveLength(3);
      });
    });

    describe('createLatch', () => {
      it('should wait for countdown', async () => {
        const latch = createLatch(2);
        let resolved = false;

        const waiter = latch.wait().then(() => { resolved = true; });

        expect(resolved).toBe(false);
        latch.countDown();
        expect(resolved).toBe(false);
        latch.countDown();

        await waiter;
        expect(resolved).toBe(true);
      });

      it('should return count', () => {
        const latch = createLatch(3);
        expect(latch.getCount()).toBe(3);

        latch.countDown();
        expect(latch.getCount()).toBe(2);
      });
    });
  });
});
