/**
 * ReplicaSyncManager Tests
 *
 * Tests for WAL synchronization between primary and replica.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { ReplicaSyncManager } from '../../../src/do/shard/replica-sync-manager.js';
import type { ReplicationWalEntry } from '../../../src/do/shard/replica-types.js';

// Mock DurableObjectNamespace and stub
function createMockNamespace() {
  const responses: Map<string, { entries: ReplicationWalEntry[]; currentLSN: number; timestamp: number }> = new Map();

  const mockStub = {
    fetch: vi.fn(async (request: Request) => {
      const body = await request.json() as { afterLSN: number };
      const response = responses.get('default') || {
        entries: [],
        currentLSN: 0,
        timestamp: Date.now(),
      };

      // Filter entries after the requested LSN
      const filteredEntries = response.entries.filter(e => e.lsn > body.afterLSN);

      return new Response(JSON.stringify({
        entries: filteredEntries,
        currentLSN: response.currentLSN,
        timestamp: response.timestamp,
      }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      });
    }),
  };

  const mockNamespace = {
    idFromName: vi.fn((name: string) => ({ name })),
    get: vi.fn(() => mockStub),
    newUniqueId: vi.fn(),
  };

  return {
    namespace: mockNamespace as unknown as DurableObjectNamespace,
    stub: mockStub,
    setResponse: (response: { entries: ReplicationWalEntry[]; currentLSN: number; timestamp: number }) => {
      responses.set('default', response);
    },
    setError: (status: number, message: string) => {
      mockStub.fetch.mockImplementationOnce(async () => {
        return new Response(message, { status });
      });
    },
  };
}

describe('ReplicaSyncManager', () => {
  let syncManager: ReplicaSyncManager;
  let mockNamespace: ReturnType<typeof createMockNamespace>;

  beforeEach(() => {
    mockNamespace = createMockNamespace();
    syncManager = new ReplicaSyncManager(
      'shard-0',
      mockNamespace.namespace,
      { maxStalenessMs: 5000 }
    );
  });

  describe('initial state', () => {
    it('should start with LSN 0', () => {
      expect(syncManager.getAppliedLSN()).toBe(0);
    });

    it('should start disconnected', () => {
      expect(syncManager.getStatus()).toBe('disconnected');
    });

    it('should report not healthy initially', () => {
      expect(syncManager.isHealthy()).toBe(false);
    });
  });

  describe('sync', () => {
    it('should apply WAL entries', async () => {
      const entries: ReplicationWalEntry[] = [
        { lsn: 1, collection: 'users', op: 'i', docId: 'doc1', document: { name: 'Alice' }, timestamp: Date.now() },
        { lsn: 2, collection: 'users', op: 'i', docId: 'doc2', document: { name: 'Bob' }, timestamp: Date.now() },
      ];

      mockNamespace.setResponse({
        entries,
        currentLSN: 2,
        timestamp: Date.now(),
      });

      const appliedEntries: ReplicationWalEntry[] = [];
      const result = await syncManager.sync((entry) => {
        appliedEntries.push(entry);
      });

      expect(result.success).toBe(true);
      expect(result.entriesApplied).toBe(2);
      expect(result.newLSN).toBe(2);
      expect(appliedEntries.length).toBe(2);
    });

    it('should update status to healthy after successful sync', async () => {
      mockNamespace.setResponse({
        entries: [],
        currentLSN: 0,
        timestamp: Date.now(),
      });

      await syncManager.sync(() => {});

      expect(syncManager.getStatus()).toBe('healthy');
      expect(syncManager.isHealthy()).toBe(true);
    });

    it('should track applied LSN', async () => {
      mockNamespace.setResponse({
        entries: [
          { lsn: 5, collection: 'users', op: 'i', docId: 'doc1', document: {}, timestamp: Date.now() },
        ],
        currentLSN: 5,
        timestamp: Date.now(),
      });

      await syncManager.sync(() => {});
      expect(syncManager.getAppliedLSN()).toBe(5);
    });

    it('should handle fetch errors', async () => {
      mockNamespace.setError(500, 'Internal Server Error');

      const result = await syncManager.sync(() => {});

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
    });

    it('should increment consecutive failures on error', async () => {
      mockNamespace.setError(500, 'Error');
      await syncManager.sync(() => {});

      const state = syncManager.getState();
      expect(state.consecutiveFailures).toBe(1);
    });

    it('should reset consecutive failures on success', async () => {
      // First, fail a few times
      mockNamespace.setError(500, 'Error');
      await syncManager.sync(() => {});
      await syncManager.sync(() => {});

      // Then succeed
      mockNamespace.setResponse({
        entries: [],
        currentLSN: 0,
        timestamp: Date.now(),
      });
      await syncManager.sync(() => {});

      const state = syncManager.getState();
      expect(state.consecutiveFailures).toBe(0);
    });

    it('should prevent concurrent syncs', async () => {
      mockNamespace.setResponse({
        entries: [
          { lsn: 1, collection: 'users', op: 'i', docId: 'doc1', document: {}, timestamp: Date.now() },
        ],
        currentLSN: 1,
        timestamp: Date.now(),
      });

      // Start two syncs simultaneously
      const [result1, result2] = await Promise.all([
        syncManager.sync(() => {}),
        syncManager.sync(() => {}),
      ]);

      // One should succeed, one should fail with "in progress"
      const results = [result1, result2];
      const inProgressResult = results.find(r => r.error?.includes('in progress'));
      expect(inProgressResult).toBeDefined();
    });
  });

  describe('resyncFrom', () => {
    it('should reset LSN and sync', async () => {
      // First sync to LSN 5
      mockNamespace.setResponse({
        entries: [
          { lsn: 5, collection: 'users', op: 'i', docId: 'doc1', document: {}, timestamp: Date.now() },
        ],
        currentLSN: 5,
        timestamp: Date.now(),
      });
      await syncManager.sync(() => {});
      expect(syncManager.getAppliedLSN()).toBe(5);

      // Resync from LSN 0
      mockNamespace.setResponse({
        entries: [
          { lsn: 1, collection: 'users', op: 'i', docId: 'doc1', document: {}, timestamp: Date.now() },
          { lsn: 2, collection: 'users', op: 'i', docId: 'doc2', document: {}, timestamp: Date.now() },
        ],
        currentLSN: 2,
        timestamp: Date.now(),
      });

      const result = await syncManager.resyncFrom(0, () => {});

      expect(result.success).toBe(true);
      expect(result.newLSN).toBe(2);
    });
  });

  describe('status computation', () => {
    it('should report lagging when lag exceeds threshold', async () => {
      mockNamespace.setResponse({
        entries: [],
        currentLSN: 0,
        timestamp: Date.now() - 10000, // 10 seconds ago
      });

      await syncManager.sync(() => {});

      expect(syncManager.getStatus()).toBe('lagging');
    });

    it('should report disconnected after many failures', async () => {
      // Fail 5 times
      for (let i = 0; i < 5; i++) {
        mockNamespace.setError(500, 'Error');
        await syncManager.sync(() => {});
      }

      expect(syncManager.getStatus()).toBe('disconnected');
    });
  });

  describe('configuration', () => {
    it('should allow configuration updates', () => {
      syncManager.configure({ maxStalenessMs: 10000 });
      const config = syncManager.getConfig();
      expect(config.maxStalenessMs).toBe(10000);
    });

    it('should reset connection on request', () => {
      syncManager.resetConnection();
      // Should not throw - just resets internal state
      expect(syncManager.getState().consecutiveFailures).toBe(0);
    });
  });
});
