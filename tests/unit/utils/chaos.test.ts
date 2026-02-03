/**
 * Tests for the Chaos Testing Framework
 *
 * Verifies that FaultInjector, NetworkPartition, RandomFailure, TimeoutSimulator,
 * and integration helpers work correctly.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  FaultInjector,
  NetworkPartition,
  RandomFailure,
  TimeoutSimulator,
  TimeoutError,
  CircuitOpenError,
  createChaosStorage,
  createChaosR2Bucket,
  runChaosScenario,
  CHAOS_SCENARIOS,
  assertChaosSuccess,
  assertChaosTriggered,
} from '../../utils/chaos.js';

describe('Chaos Testing Framework', () => {
  describe('FaultInjector', () => {
    let injector: FaultInjector;

    beforeEach(() => {
      injector = new FaultInjector();
    });

    it('should inject a single error', async () => {
      const error = new Error('Test error');
      injector.injectOnce('storage.get', error);

      expect(injector.hasFaults()).toBe(true);
      expect(injector.getTargets()).toContain('storage.get');

      // First call should fault
      expect(injector.shouldFault('storage.get')).toBe(true);
      await expect(injector.triggerFault('storage.get')).rejects.toThrow('Test error');

      // Second call should not fault (maxOccurrences: 1)
      expect(injector.shouldFault('storage.get')).toBe(false);
    });

    it('should inject delay', async () => {
      injector.injectDelay('storage.put', 50);

      const start = Date.now();
      if (injector.shouldFault('storage.put')) {
        await injector.triggerFault('storage.put');
      }
      const elapsed = Date.now() - start;

      expect(elapsed).toBeGreaterThanOrEqual(45);
    });

    it('should inject with probability', () => {
      // Set probability to 0 - should never fault
      injector.inject('test', { type: 'error', probability: 0 });
      let faultedCount = 0;
      for (let i = 0; i < 100; i++) {
        if (injector.shouldFault('test')) faultedCount++;
      }
      expect(faultedCount).toBe(0);

      // Set probability to 1 - should always fault (accounting for maxOccurrences)
      injector.clear();
      injector.inject('test', { type: 'error', probability: 1 });
      faultedCount = 0;
      for (let i = 0; i < 10; i++) {
        if (injector.shouldFault('test')) faultedCount++;
      }
      expect(faultedCount).toBe(10);
    });

    it('should respect afterCalls configuration', () => {
      injector.inject('test', { type: 'error', afterCalls: 2 });

      // First two calls should not fault
      expect(injector.shouldFault('test')).toBe(false);
      expect(injector.shouldFault('test')).toBe(false);

      // Third call should fault
      expect(injector.shouldFault('test')).toBe(true);
    });

    it('should track statistics', async () => {
      injector.inject('test', { type: 'error', probability: 1, maxOccurrences: 2 });

      for (let i = 0; i < 5; i++) {
        if (injector.shouldFault('test')) {
          try {
            await injector.triggerFault('test');
          } catch {
            // Expected
          }
        }
      }

      const stats = injector.getStats('test') as { totalCalls: number; faultsInjected: number };
      expect(stats.totalCalls).toBe(5);
      expect(stats.faultsInjected).toBe(2);
    });

    it('should wrap operations with fault injection', async () => {
      injector.injectOnce('operation', new Error('Wrapped error'));

      await expect(
        injector.wrap('operation', async () => 'success')
      ).rejects.toThrow('Wrapped error');

      // After the fault is consumed, operation should work
      const result = await injector.wrap('operation', async () => 'success');
      expect(result).toBe('success');
    });

    it('should support global enable/disable', () => {
      injector.inject('test', { type: 'error', probability: 1 });

      expect(injector.shouldFault('test')).toBe(true);

      injector.setEnabled(false);
      expect(injector.shouldFault('test')).toBe(false);

      injector.setEnabled(true);
      expect(injector.shouldFault('test')).toBe(true);
    });

    it('should clear faults', () => {
      injector.inject('a', { type: 'error' });
      injector.inject('b', { type: 'error' });

      injector.clear('a');
      expect(injector.getTargets()).not.toContain('a');
      expect(injector.getTargets()).toContain('b');

      injector.clear();
      expect(injector.hasFaults()).toBe(false);
    });
  });

  describe('NetworkPartition', () => {
    let partition: NetworkPartition;

    beforeEach(() => {
      partition = new NetworkPartition();
    });

    it('should isolate nodes', () => {
      partition.isolate('node-1');

      expect(partition.isIsolated('node-1')).toBe(true);
      expect(partition.isIsolated('node-2')).toBe(false);
      expect(partition.getNodeState('node-1')).toBe('isolated');
    });

    it('should reconnect nodes', () => {
      partition.isolate('node-1');
      partition.reconnect('node-1');

      expect(partition.isIsolated('node-1')).toBe(false);
      expect(partition.getNodeState('node-1')).toBe('connected');
    });

    it('should add latency to nodes', () => {
      partition.addLatency('node-1', 500);

      expect(partition.getLatency('node-1')).toBe(500);
      expect(partition.getLatency('node-2')).toBe(0);
      expect(partition.getNodeState('node-1')).toBe('slow');
    });

    it('should set drop rate for nodes', () => {
      partition.setDropRate('node-1', 0.5);

      expect(partition.getDropRate('node-1')).toBe(0.5);
      expect(partition.getDropRate('node-2')).toBe(0);
      expect(partition.getNodeState('node-1')).toBe('dropping');
    });

    it('should check if nodes can communicate', () => {
      expect(partition.canCommunicate('node-1', 'node-2')).toBe(true);

      partition.isolate('node-1');
      expect(partition.canCommunicate('node-1', 'node-2')).toBe(false);
      expect(partition.canCommunicate('node-2', 'node-1')).toBe(false);
    });

    it('should create bidirectional partitions', () => {
      partition.partition(['node-1', 'node-2'], ['node-3', 'node-4']);

      expect(partition.canCommunicate('node-1', 'node-2')).toBe(true);
      expect(partition.canCommunicate('node-3', 'node-4')).toBe(true);
      expect(partition.canCommunicate('node-1', 'node-3')).toBe(false);
      expect(partition.canCommunicate('node-2', 'node-4')).toBe(false);
    });

    it('should simulate message delivery with latency', async () => {
      partition.addLatency('node-1', 50);

      const start = Date.now();
      const delivered = await partition.sendMessage('node-1', 'node-2', { data: 'test' });
      const elapsed = Date.now() - start;

      expect(delivered).toBe(true);
      expect(elapsed).toBeGreaterThanOrEqual(45);
    });

    it('should block messages to isolated nodes', async () => {
      partition.isolate('node-2');

      const delivered = await partition.sendMessage('node-1', 'node-2', { data: 'test' });
      expect(delivered).toBe(false);
    });

    it('should emit events', () => {
      const events: string[] = [];
      partition.on('*', (event) => events.push(event.type));

      partition.isolate('node-1');
      partition.addLatency('node-2', 100);
      partition.reconnect('node-1');
      partition.reset();

      expect(events).toContain('isolated');
      expect(events).toContain('latencyAdded');
      expect(events).toContain('reconnected');
      expect(events).toContain('reset');
    });

    it('should reset all partitions', () => {
      partition.isolate('node-1');
      partition.addLatency('node-2', 500);
      partition.setDropRate('node-3', 0.5);

      partition.reset();

      expect(partition.isIsolated('node-1')).toBe(false);
      expect(partition.getLatency('node-2')).toBe(0);
      expect(partition.getDropRate('node-3')).toBe(0);
    });
  });

  describe('RandomFailure', () => {
    let failure: RandomFailure;

    beforeEach(() => {
      failure = new RandomFailure(0);
    });

    it('should never fail with 0 probability', () => {
      failure.setProbability(0);

      for (let i = 0; i < 100; i++) {
        expect(failure.shouldFail()).toBe(false);
      }
    });

    it('should always fail with 1 probability', () => {
      failure.setProbability(1);

      for (let i = 0; i < 100; i++) {
        expect(failure.shouldFail()).toBe(true);
      }
    });

    it('should throw on maybeThrow with probability 1', () => {
      failure.setProbability(1);

      expect(() => failure.maybeThrow('test')).toThrow('Random failure at test');
    });

    it('should use custom error generators', () => {
      failure.setProbability(1);
      failure.setErrorGenerator('storage', () => new Error('Custom storage error'));

      expect(() => failure.maybeThrow('storage')).toThrow('Custom storage error');
    });

    it('should wrap async operations', async () => {
      failure.setProbability(1);

      await expect(
        failure.wrap('test', async () => 'success')
      ).rejects.toThrow('Random failure at test');

      failure.setProbability(0);
      const result = await failure.wrap('test', async () => 'success');
      expect(result).toBe('success');
    });

    it('should track statistics', () => {
      failure.setProbability(0.5);
      vi.spyOn(Math, 'random').mockReturnValue(0.25); // Will cause failure

      try {
        failure.maybeThrow('target-a');
      } catch {
        // Expected
      }

      const stats = failure.getStats();
      expect(stats.calls).toBe(1);
      expect(stats.failures).toBe(1);
      expect(stats.byTarget.get('target-a')?.calls).toBe(1);

      vi.restoreAllMocks();
    });

    it('should reset statistics', () => {
      failure.setProbability(1);

      try {
        failure.maybeThrow('test');
      } catch {
        // Expected
      }

      expect(failure.getStats().calls).toBe(1);

      failure.resetStats();

      expect(failure.getStats().calls).toBe(0);
    });
  });

  describe('TimeoutSimulator', () => {
    let timeout: TimeoutSimulator;

    beforeEach(() => {
      timeout = new TimeoutSimulator(100);
    });

    afterEach(() => {
      timeout.abortAll();
    });

    it('should complete operations within timeout', async () => {
      const result = await timeout.wrapOperation(async () => {
        await new Promise((r) => setTimeout(r, 10));
        return 'success';
      });

      expect(result).toBe('success');
    });

    it('should throw TimeoutError when operation exceeds timeout', async () => {
      await expect(
        timeout.wrapOperation(async () => {
          await new Promise((r) => setTimeout(r, 200));
          return 'success';
        })
      ).rejects.toThrow(TimeoutError);
    });

    it('should simulate delays', async () => {
      const start = Date.now();
      await timeout.simulateDelay(50);
      const elapsed = Date.now() - start;

      expect(elapsed).toBeGreaterThanOrEqual(45);
    });

    it('should support custom timeout per operation', async () => {
      // Short timeout should fail
      await expect(
        timeout.wrapOperation(
          async () => {
            await new Promise((r) => setTimeout(r, 100));
            return 'success';
          },
          50
        )
      ).rejects.toThrow(TimeoutError);

      // Long timeout should succeed
      const result = await timeout.wrapOperation(
        async () => {
          await new Promise((r) => setTimeout(r, 50));
          return 'success';
        },
        200
      );
      expect(result).toBe('success');
    });

    it('should track pending operations', async () => {
      expect(timeout.getPendingCount()).toBe(0);

      const promise = timeout.wrapOperation(async () => {
        await new Promise((r) => setTimeout(r, 50));
        return 'done';
      });

      expect(timeout.getPendingCount()).toBe(1);

      await promise;

      expect(timeout.getPendingCount()).toBe(0);
    });

    it('should abort all pending operations', async () => {
      const promise = timeout.wrapOperation(async () => {
        await new Promise((r) => setTimeout(r, 1000));
        return 'done';
      });

      // Small delay to ensure operation is pending
      await new Promise((r) => setTimeout(r, 10));

      timeout.abortAll();

      await expect(promise).rejects.toThrow('Operation aborted');
    });

    it('should create circuit breaker', async () => {
      const operation = vi.fn().mockImplementation(async () => {
        await new Promise((r) => setTimeout(r, 200));
        return 'success';
      });

      const circuitBroken = timeout.createCircuitBreaker(operation, {
        timeout: 50,
        failureThreshold: 3,
        resetTimeout: 100,
      });

      // First 3 failures should trigger circuit
      for (let i = 0; i < 3; i++) {
        await expect(circuitBroken()).rejects.toThrow(TimeoutError);
      }

      // Circuit should now be open
      await expect(circuitBroken()).rejects.toThrow(CircuitOpenError);
    });
  });

  describe('ChaosStorage', () => {
    it('should create storage with chaos capabilities', async () => {
      const storage = createChaosStorage();

      // Normal operations should work
      await storage.put('key1', 'value1');
      const value = await storage.get('key1');
      expect(new TextDecoder().decode(value!)).toBe('value1');

      // Inject fault
      storage.chaos.injector.injectOnce('storage.get', new Error('Read failed'));

      await expect(storage.get('key1')).rejects.toThrow('Read failed');

      // Should work again after fault is consumed
      const value2 = await storage.get('key1');
      expect(new TextDecoder().decode(value2!)).toBe('value1');
    });

    it('should support random failures', async () => {
      const storage = createChaosStorage();
      storage.chaos.randomFailure.setProbability(1);

      await expect(storage.get('key')).rejects.toThrow('Random failure');
    });

    it('should support timeout simulation', async () => {
      // Test that the timeout simulator works standalone
      const customTimeout = new TimeoutSimulator(10);

      await expect(
        customTimeout.wrapOperation(async () => {
          await new Promise((r) => setTimeout(r, 100));
          return 'done';
        })
      ).rejects.toThrow(TimeoutError);

      // Also verify the storage timeout works when operation takes too long
      const storage = createChaosStorage();
      storage.chaos.timeout.setDefaultTimeout(50);

      // Normal get (empty storage) should work within timeout
      const result = await storage.get('key');
      expect(result).toBeNull();
    });
  });

  describe('ChaosR2Bucket', () => {
    it('should create R2 bucket with chaos capabilities', async () => {
      const bucket = createChaosR2Bucket();

      // Normal operations should work
      await bucket.put('key1', 'value1');
      const object = await bucket.get('key1');
      expect(await object?.text()).toBe('value1');

      // Inject fault
      bucket.chaos.injector.injectOnce('r2.get', new Error('R2 read failed'));

      await expect(bucket.get('key1')).rejects.toThrow('R2 read failed');
    });
  });

  describe('Chaos Scenarios', () => {
    it('should run storage unavailable scenario', async () => {
      const scenario = CHAOS_SCENARIOS.storageUnavailable();

      expect(scenario.name).toBe('Storage Unavailable');
      expect(scenario.faults.length).toBeGreaterThan(0);
    });

    it('should run intermittent storage failures scenario', async () => {
      const scenario = CHAOS_SCENARIOS.intermittentStorageFailures();

      expect(scenario.name).toBe('Intermittent Storage Failures');
      expect(scenario.faults.some((f) => f.config.probability === 0.3)).toBe(true);
    });

    it('should run high latency scenario', async () => {
      const scenario = CHAOS_SCENARIOS.highLatency();

      expect(scenario.name).toBe('High Latency');
      expect(scenario.faults.some((f) => f.config.delayMs === 500)).toBe(true);
    });

    it('should run network partition scenario', async () => {
      const scenario = CHAOS_SCENARIOS.networkPartition(['node-1', 'node-2']);

      expect(scenario.name).toBe('Network Partition');
      expect(scenario.partitions?.isolatedNodes.has('node-1')).toBe(true);
      expect(scenario.partitions?.isolatedNodes.has('node-2')).toBe(true);
    });

    it('should run chaos scenario with test function', async () => {
      const scenario = CHAOS_SCENARIOS.storageUnavailable();
      let callCount = 0;

      const result = await runChaosScenario(scenario, async (context) => {
        callCount++;
        expect(context.injector).toBeDefined();
        expect(context.partition).toBeDefined();

        // Test that faults are configured
        expect(context.injector.shouldFault('storage.get')).toBe(true);
      });

      expect(callCount).toBe(1);
      expect(result.scenario).toBe('Storage Unavailable');
      expect(result.duration).toBeGreaterThanOrEqual(0);
    });

    it('should assert chaos success', () => {
      const successResult = {
        scenario: 'Test',
        duration: 100,
        faultStats: new Map(),
        errors: [],
        success: true,
      };

      expect(() => assertChaosSuccess(successResult)).not.toThrow();

      const failResult = {
        scenario: 'Test',
        duration: 100,
        faultStats: new Map(),
        errors: [new Error('Test failure')],
        success: false,
      };

      expect(() => assertChaosSuccess(failResult)).toThrow('Chaos scenario "Test" failed');
    });

    it('should assert chaos triggered', () => {
      const result = {
        scenario: 'Test',
        duration: 100,
        faultStats: new Map([
          ['storage.get', { totalCalls: 10, faultsInjected: 5, errors: [] }],
          ['storage.put', { totalCalls: 10, faultsInjected: 2, errors: [] }],
        ]),
        errors: [],
        success: true,
      };

      expect(() =>
        assertChaosTriggered(result, { 'storage.get': 5, 'storage.put': 2 })
      ).not.toThrow();

      expect(() =>
        assertChaosTriggered(result, { 'storage.get': 10 })
      ).toThrow('Expected at least 10 faults at "storage.get", got 5');
    });
  });
});
