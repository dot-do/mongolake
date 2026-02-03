/**
 * ReplicaManager Tests
 *
 * Tests for the primary ShardDO replica tracking and management.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  ReplicaManager,
  type ReplicaInfo,
  type ReplicaSelectionOptions,
  type ReplicaManagerConfig,
} from '../../../src/do/shard/replica-manager.js';

describe('ReplicaManager', () => {
  let manager: ReplicaManager;

  beforeEach(() => {
    manager = new ReplicaManager();
  });

  describe('registerReplica', () => {
    it('should register a new replica', () => {
      manager.registerReplica('replica-1', 'https://replica-1.example.com');

      const replica = manager.getReplica('replica-1');
      expect(replica).toBeDefined();
      expect(replica?.id).toBe('replica-1');
      expect(replica?.endpoint).toBe('https://replica-1.example.com');
      expect(replica?.status).toBe('unknown');
    });

    it('should update existing replica on re-registration', () => {
      manager.registerReplica('replica-1', 'https://old.example.com');
      manager.registerReplica('replica-1', 'https://new.example.com');

      const replica = manager.getReplica('replica-1');
      expect(replica?.endpoint).toBe('https://new.example.com');
    });

    it('should store metadata with replica', () => {
      manager.registerReplica('replica-1', 'https://replica-1.example.com', {
        region: 'us-west-2',
        priority: 1,
      });

      const replica = manager.getReplica('replica-1');
      expect(replica?.metadata?.region).toBe('us-west-2');
      expect(replica?.metadata?.priority).toBe(1);
    });

    it('should initialize replica with correct defaults', () => {
      const beforeTime = Date.now();
      manager.registerReplica('replica-1', 'https://replica-1.example.com');
      const afterTime = Date.now();

      const replica = manager.getReplica('replica-1');
      expect(replica?.lastKnownLSN).toBe(0);
      expect(replica?.lagMs).toBe(0);
      expect(replica?.consecutiveFailures).toBe(0);
      expect(replica?.registeredAt).toBeGreaterThanOrEqual(beforeTime);
      expect(replica?.registeredAt).toBeLessThanOrEqual(afterTime);
    });
  });

  describe('deregisterReplica', () => {
    it('should remove a registered replica', () => {
      manager.registerReplica('replica-1', 'https://replica-1.example.com');
      expect(manager.getReplicaCount()).toBe(1);

      const removed = manager.deregisterReplica('replica-1');
      expect(removed).toBe(true);
      expect(manager.getReplicaCount()).toBe(0);
      expect(manager.getReplica('replica-1')).toBeNull();
    });

    it('should return false for non-existent replica', () => {
      const removed = manager.deregisterReplica('non-existent');
      expect(removed).toBe(false);
    });
  });

  describe('getAllReplicas', () => {
    it('should return all registered replicas', () => {
      manager.registerReplica('replica-1', 'https://r1.example.com');
      manager.registerReplica('replica-2', 'https://r2.example.com');
      manager.registerReplica('replica-3', 'https://r3.example.com');

      const replicas = manager.getAllReplicas();
      expect(replicas.length).toBe(3);
      expect(replicas.map(r => r.id).sort()).toEqual(['replica-1', 'replica-2', 'replica-3']);
    });

    it('should return copies (not references)', () => {
      manager.registerReplica('replica-1', 'https://r1.example.com');

      const replicas = manager.getAllReplicas();
      replicas[0].endpoint = 'https://modified.example.com';

      const original = manager.getReplica('replica-1');
      expect(original?.endpoint).toBe('https://r1.example.com');
    });
  });

  describe('getReplicaForRead', () => {
    beforeEach(() => {
      // Register some replicas with different states
      manager.registerReplica('healthy-1', 'https://h1.example.com');
      manager.registerReplica('healthy-2', 'https://h2.example.com');
      manager.registerReplica('lagging-1', 'https://l1.example.com');

      // Set up replica states
      manager.recordHeartbeat('healthy-1', 100, 50);
      manager.recordHeartbeat('healthy-2', 95, 100);
      manager.recordHeartbeat('lagging-1', 50, 5000);
    });

    it('should return null for primary preference', () => {
      const options: ReplicaSelectionOptions = { preference: 'primary' };
      const result = manager.getReplicaForRead(options);
      expect(result).toBeNull();
    });

    it('should return null for primaryPreferred (use primary)', () => {
      const options: ReplicaSelectionOptions = { preference: 'primaryPreferred' };
      const result = manager.getReplicaForRead(options);
      expect(result).toBeNull();
    });

    it('should return a secondary for secondary preference', () => {
      const options: ReplicaSelectionOptions = { preference: 'secondary' };
      const result = manager.getReplicaForRead(options);
      expect(result).toBeDefined();
      expect(['healthy-1', 'healthy-2']).toContain(result);
    });

    it('should return healthy replica for secondaryPreferred', () => {
      const options: ReplicaSelectionOptions = { preference: 'secondaryPreferred' };
      const result = manager.getReplicaForRead(options);
      expect(result).toBeDefined();
      expect(['healthy-1', 'healthy-2']).toContain(result);
    });

    it('should return null when no secondaries available', () => {
      manager.clear();
      const options: ReplicaSelectionOptions = { preference: 'secondary' };
      const result = manager.getReplicaForRead(options);
      expect(result).toBeNull();
    });

    it('should filter by maxStalenessMs', () => {
      const options: ReplicaSelectionOptions = {
        preference: 'secondary',
        maxStalenessMs: 200,
      };
      const result = manager.getReplicaForRead(options);
      expect(result).toBeDefined();
      expect(['healthy-1', 'healthy-2']).toContain(result);
    });

    it('should filter by tags', () => {
      manager.registerReplica('tagged-1', 'https://t1.example.com', { region: 'us-west-2' });
      manager.recordHeartbeat('tagged-1', 100, 50);

      const options: ReplicaSelectionOptions = {
        preference: 'secondary',
        tags: { region: 'us-west-2' },
      };
      const result = manager.getReplicaForRead(options);
      expect(result).toBe('tagged-1');
    });

    it('should return null when no replicas match tags', () => {
      const options: ReplicaSelectionOptions = {
        preference: 'secondary',
        tags: { region: 'non-existent' },
      };
      const result = manager.getReplicaForRead(options);
      expect(result).toBeNull();
    });

    it('should prefer lower lag replicas', () => {
      // Register two healthy replicas with different lags
      manager.clear();
      manager.registerReplica('low-lag', 'https://low.example.com');
      manager.registerReplica('high-lag', 'https://high.example.com');

      manager.recordHeartbeat('low-lag', 100, 10);
      manager.recordHeartbeat('high-lag', 100, 500);

      const options: ReplicaSelectionOptions = { preference: 'secondary' };
      const result = manager.getReplicaForRead(options);
      expect(result).toBe('low-lag');
    });

    it('should exclude unhealthy replicas by default', () => {
      manager.clear();
      manager.registerReplica('unhealthy-1', 'https://u1.example.com');
      const replica = manager.getReplica('unhealthy-1');
      if (replica) {
        // Simulate unhealthy state
        for (let i = 0; i < 6; i++) {
          manager.recordHeartbeat('unhealthy-1', 0, 0);
        }
      }

      // Make it unhealthy by recording failures
      const internal = (manager as any).replicas.get('unhealthy-1');
      if (internal) {
        internal.consecutiveFailures = 10;
        internal.status = 'unhealthy';
      }

      const options: ReplicaSelectionOptions = { preference: 'secondary' };
      const result = manager.getReplicaForRead(options);
      expect(result).toBeNull();
    });

    it('should include unhealthy replicas when allowUnhealthy is true', () => {
      manager.clear();
      manager.registerReplica('unhealthy-1', 'https://u1.example.com');
      manager.recordHeartbeat('unhealthy-1', 100, 50);

      // Make it unhealthy
      const internal = (manager as any).replicas.get('unhealthy-1');
      if (internal) {
        internal.consecutiveFailures = 10;
        internal.status = 'unhealthy';
      }

      const options: ReplicaSelectionOptions = {
        preference: 'secondary',
        allowUnhealthy: true,
      };
      const result = manager.getReplicaForRead(options);
      expect(result).toBe('unhealthy-1');
    });
  });

  describe('updateReplicaLSN', () => {
    it('should update replica LSN', () => {
      manager.registerReplica('replica-1', 'https://r1.example.com');
      manager.updateReplicaLSN('replica-1', 100);

      const replica = manager.getReplica('replica-1');
      expect(replica?.lastKnownLSN).toBe(100);
    });

    it('should update last heartbeat time', () => {
      manager.registerReplica('replica-1', 'https://r1.example.com');
      const beforeTime = Date.now();
      manager.updateReplicaLSN('replica-1', 100);
      const afterTime = Date.now();

      const replica = manager.getReplica('replica-1');
      expect(replica?.lastHeartbeat).toBeGreaterThanOrEqual(beforeTime);
      expect(replica?.lastHeartbeat).toBeLessThanOrEqual(afterTime);
    });

    it('should ignore non-existent replica', () => {
      // Should not throw
      manager.updateReplicaLSN('non-existent', 100);
    });
  });

  describe('recordHeartbeat', () => {
    it('should update replica state', () => {
      manager.registerReplica('replica-1', 'https://r1.example.com');
      manager.recordHeartbeat('replica-1', 100, 50);

      const replica = manager.getReplica('replica-1');
      expect(replica?.lastKnownLSN).toBe(100);
      expect(replica?.lagMs).toBe(50);
    });

    it('should reset consecutive failures', () => {
      manager.registerReplica('replica-1', 'https://r1.example.com');

      // Simulate some failures
      const internal = (manager as any).replicas.get('replica-1');
      internal.consecutiveFailures = 3;

      // Record successful heartbeat
      manager.recordHeartbeat('replica-1', 100, 50);

      const replica = manager.getReplica('replica-1');
      expect(replica?.consecutiveFailures).toBe(0);
    });

    it('should update status to healthy for low lag', () => {
      manager.registerReplica('replica-1', 'https://r1.example.com');
      manager.recordHeartbeat('replica-1', 100, 50);

      const replica = manager.getReplica('replica-1');
      expect(replica?.status).toBe('healthy');
    });

    it('should update status to lagging for moderate lag', () => {
      manager.registerReplica('replica-1', 'https://r1.example.com');
      manager.recordHeartbeat('replica-1', 100, 3000); // > 1000ms lag threshold

      const replica = manager.getReplica('replica-1');
      expect(replica?.status).toBe('lagging');
    });

    it('should update status to stale for high lag', () => {
      manager.registerReplica('replica-1', 'https://r1.example.com');
      manager.recordHeartbeat('replica-1', 100, 15000); // > 10000ms stale threshold

      const replica = manager.getReplica('replica-1');
      expect(replica?.status).toBe('stale');
    });
  });

  describe('updatePrimaryLSN', () => {
    it('should update primary LSN', () => {
      manager.updatePrimaryLSN(1000);
      // Internal state check via exported state
      const state = manager.exportState();
      expect(state.currentPrimaryLSN).toBe(1000);
    });

    it('should recalculate lag for all replicas', () => {
      manager.registerReplica('replica-1', 'https://r1.example.com');
      manager.updateReplicaLSN('replica-1', 500);
      manager.updatePrimaryLSN(1000);

      const replica = manager.getReplica('replica-1');
      // Lag should be calculated based on LSN difference
      expect(replica?.lagMs).toBeGreaterThan(0);
    });
  });

  describe('runHealthCheck', () => {
    it('should mark replicas unhealthy after heartbeat timeout', () => {
      manager.registerReplica('replica-1', 'https://r1.example.com');

      // Set last heartbeat to past timeout
      const internal = (manager as any).replicas.get('replica-1');
      internal.lastHeartbeat = Date.now() - 60000; // 60 seconds ago

      manager.runHealthCheck();

      const replica = manager.getReplica('replica-1');
      expect(replica?.status).toBe('unhealthy');
    });

    it('should increment consecutive failures on health check failure', () => {
      manager.registerReplica('replica-1', 'https://r1.example.com');

      const internal = (manager as any).replicas.get('replica-1');
      internal.lastHeartbeat = Date.now() - 60000;

      manager.runHealthCheck();

      const replica = manager.getReplica('replica-1');
      expect(replica?.consecutiveFailures).toBe(1);
    });

    it('should auto-remove unhealthy replicas when configured', () => {
      manager.configure({ autoRemoveUnhealthy: true, maxConsecutiveFailures: 3 });
      manager.registerReplica('replica-1', 'https://r1.example.com');

      // Set up for removal
      const internal = (manager as any).replicas.get('replica-1');
      internal.consecutiveFailures = 5;
      internal.lastHeartbeat = Date.now() - 60000;

      manager.runHealthCheck();

      expect(manager.getReplica('replica-1')).toBeNull();
    });
  });

  describe('getHealthSummary', () => {
    it('should return correct counts', () => {
      manager.registerReplica('healthy-1', 'https://h1.example.com');
      manager.registerReplica('lagging-1', 'https://l1.example.com');
      manager.registerReplica('stale-1', 'https://s1.example.com');
      manager.registerReplica('unknown-1', 'https://u1.example.com');

      manager.recordHeartbeat('healthy-1', 100, 50);
      manager.recordHeartbeat('lagging-1', 100, 3000);
      manager.recordHeartbeat('stale-1', 100, 15000);

      const summary = manager.getHealthSummary();
      expect(summary.total).toBe(4);
      expect(summary.healthy).toBe(1);
      expect(summary.lagging).toBe(1);
      expect(summary.stale).toBe(1);
      expect(summary.unknown).toBe(1);
    });

    it('should return zeros when no replicas', () => {
      const summary = manager.getHealthSummary();
      expect(summary.total).toBe(0);
      expect(summary.healthy).toBe(0);
      expect(summary.lagging).toBe(0);
      expect(summary.stale).toBe(0);
      expect(summary.unhealthy).toBe(0);
      expect(summary.unknown).toBe(0);
    });
  });

  describe('syncReplica', () => {
    it('should return error for non-existent replica', async () => {
      const result = await manager.syncReplica('non-existent', 0);
      expect(result.success).toBe(false);
      expect(result.error).toContain('not found');
    });

    it('should return error when no WAL provider', async () => {
      manager.registerReplica('replica-1', 'https://r1.example.com');
      const result = await manager.syncReplica('replica-1', 0);
      expect(result.success).toBe(false);
      expect(result.error).toContain('No WAL entries provider');
    });

    it('should sync successfully with WAL entries', async () => {
      manager.registerReplica('replica-1', 'https://r1.example.com');

      const mockGetWalEntries = vi.fn().mockResolvedValue({
        entries: [
          { lsn: 1, collection: 'users', op: 'i', docId: 'd1', document: {}, timestamp: Date.now() },
          { lsn: 2, collection: 'users', op: 'i', docId: 'd2', document: {}, timestamp: Date.now() },
        ],
        currentLSN: 2,
        timestamp: Date.now(),
      });

      const result = await manager.syncReplica('replica-1', 0, mockGetWalEntries);
      expect(result.success).toBe(true);
      expect(result.entriesSent).toBe(2);
      expect(result.toLSN).toBe(2);

      // Check replica LSN was updated
      const replica = manager.getReplica('replica-1');
      expect(replica?.lastKnownLSN).toBe(2);
    });

    it('should handle empty WAL entries (up to date)', async () => {
      manager.registerReplica('replica-1', 'https://r1.example.com');

      const mockGetWalEntries = vi.fn().mockResolvedValue({
        entries: [],
        currentLSN: 0,
        timestamp: Date.now(),
      });

      const result = await manager.syncReplica('replica-1', 0, mockGetWalEntries);
      expect(result.success).toBe(true);
      expect(result.entriesSent).toBe(0);
    });
  });

  describe('configure', () => {
    it('should update configuration', () => {
      manager.configure({
        healthCheckIntervalMs: 10000,
        lagThresholdMs: 2000,
      });

      const config = manager.getConfig();
      expect(config.healthCheckIntervalMs).toBe(10000);
      expect(config.lagThresholdMs).toBe(2000);
    });

    it('should preserve unspecified config values', () => {
      const originalConfig = manager.getConfig();
      manager.configure({ lagThresholdMs: 5000 });

      const newConfig = manager.getConfig();
      expect(newConfig.healthCheckIntervalMs).toBe(originalConfig.healthCheckIntervalMs);
      expect(newConfig.lagThresholdMs).toBe(5000);
    });
  });

  describe('exportState/importState', () => {
    it('should export current state', () => {
      manager.registerReplica('replica-1', 'https://r1.example.com');
      manager.updatePrimaryLSN(100);

      const state = manager.exportState();
      expect(state.replicas.length).toBe(1);
      expect(state.currentPrimaryLSN).toBe(100);
    });

    it('should import state correctly', () => {
      const state = {
        replicas: [
          {
            id: 'imported-1',
            endpoint: 'https://imported.example.com',
            registeredAt: Date.now(),
            lastKnownLSN: 50,
            lastHeartbeat: Date.now(),
            lagMs: 100,
            status: 'healthy' as const,
            consecutiveFailures: 0,
          },
        ],
        currentPrimaryLSN: 200,
        lastHealthCheck: Date.now(),
      };

      manager.importState(state);

      expect(manager.getReplicaCount()).toBe(1);
      const replica = manager.getReplica('imported-1');
      expect(replica?.endpoint).toBe('https://imported.example.com');
      expect(replica?.lastKnownLSN).toBe(50);

      const exported = manager.exportState();
      expect(exported.currentPrimaryLSN).toBe(200);
    });

    it('should clear existing state on import', () => {
      manager.registerReplica('existing-1', 'https://existing.example.com');

      const state = {
        replicas: [],
        currentPrimaryLSN: 0,
        lastHealthCheck: 0,
      };

      manager.importState(state);

      expect(manager.getReplicaCount()).toBe(0);
      expect(manager.getReplica('existing-1')).toBeNull();
    });
  });

  describe('clear', () => {
    it('should remove all replicas', () => {
      manager.registerReplica('replica-1', 'https://r1.example.com');
      manager.registerReplica('replica-2', 'https://r2.example.com');
      manager.updatePrimaryLSN(100);

      manager.clear();

      expect(manager.getReplicaCount()).toBe(0);
      const state = manager.exportState();
      expect(state.currentPrimaryLSN).toBe(0);
    });
  });

  describe('read preference scenarios', () => {
    it('should handle nearest preference', () => {
      manager.registerReplica('near', 'https://near.example.com');
      manager.registerReplica('far', 'https://far.example.com');

      manager.recordHeartbeat('near', 100, 10);
      manager.recordHeartbeat('far', 100, 200);

      const result = manager.getReplicaForRead({ preference: 'nearest' });
      expect(result).toBe('near');
    });

    it('should return null for secondaryPreferred when all stale', () => {
      manager.registerReplica('stale-1', 'https://s1.example.com');
      manager.recordHeartbeat('stale-1', 100, 50000); // Very stale

      const result = manager.getReplicaForRead({
        preference: 'secondaryPreferred',
        maxStalenessMs: 1000,
      });
      expect(result).toBeNull();
    });
  });
});
