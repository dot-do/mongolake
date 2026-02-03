/**
 * Read Replica Tests
 *
 * Tests for ShardDO read replica functionality including:
 * - Replica registration with primary
 * - Read routing to replicas
 * - Replication lag detection
 * - Failover when primary fails
 * - Replica sync from WAL
 * - Read preference (primary, secondary, nearest)
 * - Replica health checks
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { ReplicaShardDO } from '../../../src/do/shard/replica.js';
import { ReplicaSyncManager } from '../../../src/do/shard/replica-sync-manager.js';
import { ReplicaBuffer } from '../../../src/do/shard/replica-buffer.js';
import type {
  ReplicaShardDOEnv,
  ReplicationWalEntry,
  ReplicaStatus,
} from '../../../src/do/shard/replica-types.js';
import type { DurableObjectState, DurableObjectStorage, R2Bucket } from '@cloudflare/workers-types';

// ============================================================================
// Test Helpers
// ============================================================================

function createMockStorage(): DurableObjectStorage & {
  _data: Map<string, unknown>;
  _alarms: number[];
} {
  const data = new Map<string, unknown>();
  const alarms: number[] = [];

  return {
    get: vi.fn(async (key: string) => data.get(key)),
    put: vi.fn(async (key: string, value: unknown) => {
      data.set(key, value);
    }),
    delete: vi.fn(async (key: string) => {
      const existed = data.has(key);
      data.delete(key);
      return existed;
    }),
    list: vi.fn(async (options?: { prefix?: string; limit?: number }) => {
      const result = new Map<string, unknown>();
      for (const [key, value] of data) {
        if (!options?.prefix || key.startsWith(options.prefix)) {
          result.set(key, value);
          if (options?.limit && result.size >= options.limit) break;
        }
      }
      return result;
    }),
    deleteAll: vi.fn(async () => {
      data.clear();
    }),
    getAlarm: vi.fn(async () => alarms[0] ?? null),
    setAlarm: vi.fn(async (time: number) => {
      alarms.push(time);
    }),
    deleteAlarm: vi.fn(async () => {
      alarms.length = 0;
    }),
    sync: vi.fn(async () => {}),
    transaction: vi.fn(async <T>(closure: () => Promise<T>) => closure()),
    transactionSync: vi.fn(<T>(closure: () => T) => closure()),
    sql: {
      exec: vi.fn(() => ({
        toArray: () => [],
        one: () => null,
        raw: () => [],
        columnNames: [],
        rowsRead: 0,
        rowsWritten: 0,
      })),
    },
    _data: data,
    _alarms: alarms,
  } as unknown as DurableObjectStorage & {
    _data: Map<string, unknown>;
    _alarms: number[];
  };
}

function createMockState(
  storage?: DurableObjectStorage,
  id?: string
): DurableObjectState {
  return {
    id: {
      toString: () => id ?? 'replica-1',
      equals: (other: { toString: () => string }) =>
        other.toString() === (id ?? 'replica-1'),
      name: id ?? 'replica-1',
    },
    storage: storage || createMockStorage(),
    waitUntil: vi.fn(),
    blockConcurrencyWhile: vi.fn(async <T>(closure: () => Promise<T>) =>
      closure()
    ),
  } as unknown as DurableObjectState;
}

function createMockR2Bucket(): R2Bucket & { _objects: Map<string, Uint8Array> } {
  const objects = new Map<string, Uint8Array>();

  return {
    get: vi.fn(async (key: string) => {
      const data = objects.get(key);
      if (!data) return null;
      return {
        arrayBuffer: async () => data.buffer,
        text: async () => new TextDecoder().decode(data),
        json: async () => JSON.parse(new TextDecoder().decode(data)),
        body: new ReadableStream(),
        etag: `etag-${key}`,
        key,
        size: data.length,
      };
    }),
    put: vi.fn(async (key: string, value: ArrayBuffer | Uint8Array | string) => {
      const data =
        value instanceof Uint8Array
          ? value
          : typeof value === 'string'
            ? new TextEncoder().encode(value)
            : new Uint8Array(value);
      objects.set(key, data);
      return { key, size: data.length, etag: `etag-${key}` };
    }),
    delete: vi.fn(async (key: string) => {
      objects.delete(key);
    }),
    list: vi.fn(
      async (options?: { prefix?: string; limit?: number; cursor?: string }) => {
        const result: Array<{ key: string; size: number; etag: string }> = [];
        for (const [key, data] of objects) {
          if (!options?.prefix || key.startsWith(options.prefix)) {
            result.push({ key, size: data.length, etag: `etag-${key}` });
          }
        }
        return { objects: result, truncated: false };
      }
    ),
    head: vi.fn(async (key: string) => {
      const data = objects.get(key);
      if (!data) return null;
      return { key, size: data.length, etag: `etag-${key}` };
    }),
    createMultipartUpload: vi.fn(),
    resumeMultipartUpload: vi.fn(),
    _objects: objects,
  } as unknown as R2Bucket & { _objects: Map<string, Uint8Array> };
}

interface MockNamespaceResult {
  namespace: DurableObjectNamespace;
  stub: {
    fetch: ReturnType<typeof vi.fn>;
  };
  setResponse: (response: {
    entries: ReplicationWalEntry[];
    currentLSN: number;
    timestamp: number;
  }) => void;
  setError: (status: number, message: string) => void;
}

function createMockNamespace(): MockNamespaceResult {
  const responses: Map<
    string,
    { entries: ReplicationWalEntry[]; currentLSN: number; timestamp: number }
  > = new Map();

  const mockStub = {
    fetch: vi.fn(async (request: Request) => {
      const body = (await request.json()) as { afterLSN: number };
      const response = responses.get('default') || {
        entries: [],
        currentLSN: 0,
        timestamp: Date.now(),
      };

      const filteredEntries = response.entries.filter(
        (e) => e.lsn > body.afterLSN
      );

      return new Response(
        JSON.stringify({
          entries: filteredEntries,
          currentLSN: response.currentLSN,
          timestamp: response.timestamp,
        }),
        {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        }
      );
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
    setResponse: (response: {
      entries: ReplicationWalEntry[];
      currentLSN: number;
      timestamp: number;
    }) => {
      responses.set('default', response);
    },
    setError: (status: number, message: string) => {
      mockStub.fetch.mockImplementationOnce(async () => {
        return new Response(message, { status });
      });
    },
  };
}

function createMockEnv(
  bucket?: R2Bucket,
  namespace?: DurableObjectNamespace
): ReplicaShardDOEnv {
  return {
    DATA_BUCKET: bucket || createMockR2Bucket(),
    SHARD_DO: namespace || (createMockNamespace().namespace as DurableObjectNamespace),
    ANALYTICS: undefined,
  };
}

function createWalEntry(
  lsn: number,
  collection: string,
  op: 'i' | 'u' | 'd',
  docId: string,
  document: Record<string, unknown>
): ReplicationWalEntry {
  return {
    lsn,
    collection,
    op,
    docId,
    document,
    timestamp: Date.now(),
  };
}

// ============================================================================
// Read Preference Router (for testing routing logic)
// ============================================================================

type ReadPreference = 'primary' | 'secondary' | 'primaryPreferred' | 'secondaryPreferred' | 'nearest';

interface ReplicaInfo {
  id: string;
  status: ReplicaStatus;
  lagMs: number;
  latencyMs: number;
  isPrimary: boolean;
}

class ReadPreferenceRouter {
  private replicas: Map<string, ReplicaInfo> = new Map();
  private primaryId: string | null = null;

  registerPrimary(id: string): void {
    this.primaryId = id;
    this.replicas.set(id, {
      id,
      status: 'healthy',
      lagMs: 0,
      latencyMs: 10,
      isPrimary: true,
    });
  }

  registerReplica(
    id: string,
    options: { lagMs?: number; latencyMs?: number; status?: ReplicaStatus } = {}
  ): void {
    this.replicas.set(id, {
      id,
      status: options.status ?? 'healthy',
      lagMs: options.lagMs ?? 0,
      latencyMs: options.latencyMs ?? 50,
      isPrimary: false,
    });
  }

  updateReplicaStatus(id: string, status: ReplicaStatus, lagMs?: number): void {
    const replica = this.replicas.get(id);
    if (replica) {
      replica.status = status;
      if (lagMs !== undefined) replica.lagMs = lagMs;
    }
  }

  updateReplicaLatency(id: string, latencyMs: number): void {
    const replica = this.replicas.get(id);
    if (replica) {
      replica.latencyMs = latencyMs;
    }
  }

  removeReplica(id: string): void {
    this.replicas.delete(id);
    if (this.primaryId === id) {
      this.primaryId = null;
    }
  }

  routeRead(preference: ReadPreference, maxStalenessMs: number = 5000): string | null {
    const healthyReplicas = Array.from(this.replicas.values()).filter(
      (r) => r.status === 'healthy' || r.status === 'lagging'
    );

    const acceptableReplicas = healthyReplicas.filter(
      (r) => r.lagMs <= maxStalenessMs
    );

    switch (preference) {
      case 'primary':
        return this.primaryId && this.replicas.get(this.primaryId)?.status === 'healthy'
          ? this.primaryId
          : null;

      case 'secondary': {
        const secondaries = acceptableReplicas.filter((r) => !r.isPrimary);
        if (secondaries.length === 0) return null;
        // Round-robin or random selection
        return secondaries[Math.floor(Math.random() * secondaries.length)].id;
      }

      case 'primaryPreferred': {
        const primary = this.primaryId ? this.replicas.get(this.primaryId) : null;
        if (primary && primary.status === 'healthy') {
          return this.primaryId;
        }
        // Fall back to secondary
        const secondaries = acceptableReplicas.filter((r) => !r.isPrimary);
        if (secondaries.length === 0) return null;
        return secondaries[0].id;
      }

      case 'secondaryPreferred': {
        const secondaries = acceptableReplicas.filter((r) => !r.isPrimary);
        if (secondaries.length > 0) {
          return secondaries[0].id;
        }
        // Fall back to primary
        return this.primaryId && this.replicas.get(this.primaryId)?.status === 'healthy'
          ? this.primaryId
          : null;
      }

      case 'nearest': {
        if (acceptableReplicas.length === 0) return null;
        // Sort by latency
        const sorted = [...acceptableReplicas].sort(
          (a, b) => a.latencyMs - b.latencyMs
        );
        return sorted[0].id;
      }

      default:
        return null;
    }
  }

  getHealthyReplicaCount(): number {
    return Array.from(this.replicas.values()).filter(
      (r) => r.status === 'healthy'
    ).length;
  }

  getAllReplicas(): ReplicaInfo[] {
    return Array.from(this.replicas.values());
  }
}

// ============================================================================
// Tests: Replica Registration
// ============================================================================

describe('Read Replica - Registration', () => {
  let mockNamespace: MockNamespaceResult;

  beforeEach(() => {
    mockNamespace = createMockNamespace();
  });

  it('should register a replica with the primary shard', async () => {
    const storage = createMockStorage();
    const state = createMockState(storage, 'replica-1');
    const env = createMockEnv(undefined, mockNamespace.namespace);

    const replica = new ReplicaShardDO(state, env);
    await replica.initialize('primary-shard');

    const status = replica.getStatus();
    expect(status.primaryShardId).toBe('primary-shard');
    expect(status.initialized).toBe(true);
    expect(status.replicaId).toBe('replica-1');
  });

  it('should persist primary shard ID to storage', async () => {
    const storage = createMockStorage();
    const state = createMockState(storage, 'replica-2');
    const env = createMockEnv(undefined, mockNamespace.namespace);

    const replica = new ReplicaShardDO(state, env);
    await replica.initialize('primary-shard-123');

    expect(storage._data.get('primaryShardId')).toBe('primary-shard-123');
  });

  it('should recover primary shard ID from storage on restart', async () => {
    const storage = createMockStorage();
    storage._data.set('primaryShardId', 'recovered-primary');
    storage._data.set('appliedLSN', 42);

    const state = createMockState(storage, 'replica-3');
    const env = createMockEnv(undefined, mockNamespace.namespace);

    const replica = new ReplicaShardDO(state, env);

    // Initialize explicitly to trigger recovery (blockConcurrencyWhile calls the closure synchronously in our mock)
    await replica.initialize('recovered-primary');

    const status = replica.getStatus();
    expect(status.primaryShardId).toBe('recovered-primary');
    expect(replica.getAppliedLSN()).toBe(42);
  });

  it('should not re-initialize if already initialized with same primary', async () => {
    const storage = createMockStorage();
    const state = createMockState(storage, 'replica-4');
    const env = createMockEnv(undefined, mockNamespace.namespace);

    const replica = new ReplicaShardDO(state, env);
    await replica.initialize('primary-shard');

    // Reset the mock to track subsequent calls
    (storage.put as ReturnType<typeof vi.fn>).mockClear();

    // Initialize again with same primary
    await replica.initialize('primary-shard');

    // Should not have called put again
    expect(storage.put).not.toHaveBeenCalled();
  });
});

// ============================================================================
// Tests: Read Routing
// ============================================================================

describe('Read Replica - Read Routing', () => {
  let router: ReadPreferenceRouter;

  beforeEach(() => {
    router = new ReadPreferenceRouter();
    router.registerPrimary('primary');
    router.registerReplica('replica-1', { lagMs: 100, latencyMs: 30 });
    router.registerReplica('replica-2', { lagMs: 200, latencyMs: 50 });
    router.registerReplica('replica-3', { lagMs: 500, latencyMs: 20 });
  });

  it('should route to primary with primary preference', () => {
    const target = router.routeRead('primary');
    expect(target).toBe('primary');
  });

  it('should route to a secondary with secondary preference', () => {
    const target = router.routeRead('secondary');
    expect(target).not.toBe('primary');
    expect(['replica-1', 'replica-2', 'replica-3']).toContain(target);
  });

  it('should prefer primary but fall back to secondary with primaryPreferred', () => {
    // Normal case - should return primary
    expect(router.routeRead('primaryPreferred')).toBe('primary');

    // Primary down - should fall back to secondary
    router.updateReplicaStatus('primary', 'disconnected');
    const target = router.routeRead('primaryPreferred');
    expect(target).not.toBe('primary');
    expect(target).not.toBeNull();
  });

  it('should prefer secondary but fall back to primary with secondaryPreferred', () => {
    // Normal case - should return a secondary
    const target1 = router.routeRead('secondaryPreferred');
    expect(target1).not.toBe('primary');

    // All secondaries down - should fall back to primary
    router.updateReplicaStatus('replica-1', 'disconnected');
    router.updateReplicaStatus('replica-2', 'disconnected');
    router.updateReplicaStatus('replica-3', 'disconnected');
    const target2 = router.routeRead('secondaryPreferred');
    expect(target2).toBe('primary');
  });

  it('should route to nearest replica with nearest preference', () => {
    // Primary has latency 10ms, which is lowest by default
    // Let's update it to be higher to test that replicas can be nearest
    router.updateReplicaLatency('primary', 100);
    // Now replica-3 has lowest latency (20ms)
    const target = router.routeRead('nearest');
    expect(target).toBe('replica-3');
  });

  it('should respect maxStaleness when routing', () => {
    // With 150ms max staleness, only replica-1 (100ms lag) should be acceptable
    router.updateReplicaStatus('primary', 'disconnected');
    const target = router.routeRead('secondary', 150);
    expect(target).toBe('replica-1');
  });
});

// ============================================================================
// Tests: Replication Lag Detection
// ============================================================================

describe('Read Replica - Replication Lag Detection', () => {
  let syncManager: ReplicaSyncManager;
  let mockNamespace: MockNamespaceResult;

  beforeEach(() => {
    mockNamespace = createMockNamespace();
    syncManager = new ReplicaSyncManager('shard-0', mockNamespace.namespace, {
      maxStalenessMs: 5000,
    });
  });

  it('should calculate lag based on primary timestamp', async () => {
    const primaryTimestamp = Date.now() - 2000; // 2 seconds ago
    mockNamespace.setResponse({
      entries: [],
      currentLSN: 10,
      timestamp: primaryTimestamp,
    });

    await syncManager.sync(() => {});

    const state = syncManager.getState();
    // Lag should be approximately 2000ms (with some tolerance for test execution)
    expect(state.lagMs).toBeGreaterThanOrEqual(1900);
    expect(state.lagMs).toBeLessThan(3000);
  });

  it('should report healthy status when lag is within threshold', async () => {
    mockNamespace.setResponse({
      entries: [],
      currentLSN: 10,
      timestamp: Date.now() - 1000, // 1 second lag
    });

    await syncManager.sync(() => {});

    expect(syncManager.getStatus()).toBe('healthy');
    expect(syncManager.isHealthy()).toBe(true);
  });

  it('should report lagging status when lag exceeds threshold', async () => {
    mockNamespace.setResponse({
      entries: [],
      currentLSN: 10,
      timestamp: Date.now() - 10000, // 10 second lag
    });

    await syncManager.sync(() => {});

    expect(syncManager.getStatus()).toBe('lagging');
  });

  it('should track lag improvement after successful sync', async () => {
    // First sync with high lag
    mockNamespace.setResponse({
      entries: [],
      currentLSN: 10,
      timestamp: Date.now() - 10000,
    });
    await syncManager.sync(() => {});
    const initialLag = syncManager.getState().lagMs;

    // Second sync with low lag
    mockNamespace.setResponse({
      entries: [],
      currentLSN: 10,
      timestamp: Date.now() - 500,
    });
    await syncManager.sync(() => {});
    const newLag = syncManager.getState().lagMs;

    expect(newLag).toBeLessThan(initialLag);
  });
});

// ============================================================================
// Tests: Failover
// ============================================================================

describe('Read Replica - Failover', () => {
  let router: ReadPreferenceRouter;

  beforeEach(() => {
    router = new ReadPreferenceRouter();
    router.registerPrimary('primary');
    router.registerReplica('replica-1', { lagMs: 100 });
    router.registerReplica('replica-2', { lagMs: 200 });
  });

  it('should detect when primary fails', () => {
    router.updateReplicaStatus('primary', 'disconnected');

    const target = router.routeRead('primary');
    expect(target).toBeNull();
  });

  it('should fail over to secondary when primary is unavailable', () => {
    router.updateReplicaStatus('primary', 'disconnected');

    const target = router.routeRead('primaryPreferred');
    expect(target).not.toBe('primary');
    expect(target).not.toBeNull();
  });

  it('should return null when all replicas fail', () => {
    router.updateReplicaStatus('primary', 'disconnected');
    router.updateReplicaStatus('replica-1', 'disconnected');
    router.updateReplicaStatus('replica-2', 'disconnected');

    const target = router.routeRead('nearest');
    expect(target).toBeNull();
  });

  it('should recover when primary comes back online', () => {
    // Primary fails
    router.updateReplicaStatus('primary', 'disconnected');
    expect(router.routeRead('primary')).toBeNull();

    // Primary recovers
    router.updateReplicaStatus('primary', 'healthy');
    expect(router.routeRead('primary')).toBe('primary');
  });

  it('should handle removal of failed replicas', () => {
    router.removeReplica('replica-1');

    const allReplicas = router.getAllReplicas();
    expect(allReplicas.find((r) => r.id === 'replica-1')).toBeUndefined();
    expect(allReplicas.length).toBe(2); // primary + replica-2
  });
});

// ============================================================================
// Tests: Replica Sync from WAL
// ============================================================================

describe('Read Replica - WAL Sync', () => {
  let buffer: ReplicaBuffer;
  let syncManager: ReplicaSyncManager;
  let mockNamespace: MockNamespaceResult;

  beforeEach(() => {
    buffer = new ReplicaBuffer({ cacheTtlMs: 60000 });
    mockNamespace = createMockNamespace();
    syncManager = new ReplicaSyncManager('shard-0', mockNamespace.namespace);
  });

  it('should apply WAL entries to buffer on sync', async () => {
    const entries: ReplicationWalEntry[] = [
      createWalEntry(1, 'users', 'i', 'u1', { name: 'Alice' }),
      createWalEntry(2, 'users', 'i', 'u2', { name: 'Bob' }),
    ];

    mockNamespace.setResponse({
      entries,
      currentLSN: 2,
      timestamp: Date.now(),
    });

    await syncManager.sync((entry) => {
      buffer.applyEntry(entry);
    });

    expect(buffer.get('users', 'u1')?.name).toBe('Alice');
    expect(buffer.get('users', 'u2')?.name).toBe('Bob');
    expect(buffer.getCurrentLSN()).toBe(2);
  });

  it('should handle delete operations from WAL', async () => {
    // First insert
    buffer.applyEntry(createWalEntry(1, 'users', 'i', 'u1', { name: 'Alice' }));
    expect(buffer.get('users', 'u1')).not.toBeNull();

    // Then delete via sync
    mockNamespace.setResponse({
      entries: [createWalEntry(2, 'users', 'd', 'u1', {})],
      currentLSN: 2,
      timestamp: Date.now(),
    });

    await syncManager.sync((entry) => {
      buffer.applyEntry(entry);
    });

    expect(buffer.get('users', 'u1')).toBeNull();
  });

  it('should handle update operations from WAL', async () => {
    // First insert
    buffer.applyEntry(createWalEntry(1, 'users', 'i', 'u1', { name: 'Alice', age: 25 }));

    // Then update via sync
    mockNamespace.setResponse({
      entries: [createWalEntry(2, 'users', 'u', 'u1', { name: 'Alice', age: 26 })],
      currentLSN: 2,
      timestamp: Date.now(),
    });

    await syncManager.sync((entry) => {
      buffer.applyEntry(entry);
    });

    const doc = buffer.get('users', 'u1');
    expect(doc?.age).toBe(26);
  });

  it('should sync incrementally from last applied LSN', async () => {
    // First sync
    mockNamespace.setResponse({
      entries: [
        createWalEntry(1, 'users', 'i', 'u1', { name: 'Alice' }),
        createWalEntry(2, 'users', 'i', 'u2', { name: 'Bob' }),
      ],
      currentLSN: 2,
      timestamp: Date.now(),
    });

    await syncManager.sync((entry) => {
      buffer.applyEntry(entry);
    });

    expect(syncManager.getAppliedLSN()).toBe(2);

    // Second sync - only new entries should be fetched
    mockNamespace.setResponse({
      entries: [
        createWalEntry(1, 'users', 'i', 'u1', { name: 'Alice' }),
        createWalEntry(2, 'users', 'i', 'u2', { name: 'Bob' }),
        createWalEntry(3, 'users', 'i', 'u3', { name: 'Charlie' }),
      ],
      currentLSN: 3,
      timestamp: Date.now(),
    });

    const result = await syncManager.sync((entry) => {
      buffer.applyEntry(entry);
    });

    // Only entry 3 should be applied
    expect(result.entriesApplied).toBe(1);
    expect(buffer.get('users', 'u3')?.name).toBe('Charlie');
  });

  it('should support resync from a specific LSN', async () => {
    // Set up initial state
    mockNamespace.setResponse({
      entries: [
        createWalEntry(1, 'users', 'i', 'u1', { name: 'Alice' }),
        createWalEntry(2, 'users', 'i', 'u2', { name: 'Bob' }),
        createWalEntry(3, 'users', 'i', 'u3', { name: 'Charlie' }),
      ],
      currentLSN: 3,
      timestamp: Date.now(),
    });

    // Resync from LSN 1 (should get entries 2 and 3)
    const result = await syncManager.resyncFrom(1, (entry) => {
      buffer.applyEntry(entry);
    });

    expect(result.entriesApplied).toBe(2);
    expect(syncManager.getAppliedLSN()).toBe(3);
  });
});

// ============================================================================
// Tests: Read Preference
// ============================================================================

describe('Read Replica - Read Preference', () => {
  let router: ReadPreferenceRouter;

  beforeEach(() => {
    router = new ReadPreferenceRouter();
    router.registerPrimary('primary');
    router.registerReplica('replica-us-east', { lagMs: 100, latencyMs: 20 });
    router.registerReplica('replica-us-west', { lagMs: 150, latencyMs: 80 });
    router.registerReplica('replica-eu', { lagMs: 200, latencyMs: 150 });
  });

  it('should enforce primary read preference', () => {
    // Multiple calls should always return primary
    for (let i = 0; i < 5; i++) {
      expect(router.routeRead('primary')).toBe('primary');
    }
  });

  it('should enforce secondary read preference', () => {
    // Multiple calls should never return primary
    for (let i = 0; i < 10; i++) {
      const target = router.routeRead('secondary');
      expect(target).not.toBe('primary');
      expect(target).not.toBeNull();
    }
  });

  it('should select nearest based on latency', () => {
    // Primary has latency 10ms by default, update it to be higher
    router.updateReplicaLatency('primary', 100);
    // Now replica-us-east has lowest latency (20ms)
    expect(router.routeRead('nearest')).toBe('replica-us-east');

    // Update latencies - make primary fastest
    router.updateReplicaLatency('replica-us-east', 200);
    router.updateReplicaLatency('primary', 5);

    // Now primary is nearest
    expect(router.routeRead('nearest')).toBe('primary');
  });

  it('should exclude stale replicas from nearest selection', () => {
    // Make replica-us-east too stale
    router.updateReplicaStatus('replica-us-east', 'lagging', 10000);

    // With 5000ms max staleness, should pick next nearest
    const target = router.routeRead('nearest', 5000);
    expect(target).not.toBe('replica-us-east');
  });

  it('should distribute reads across secondaries', () => {
    const targets = new Set<string>();
    for (let i = 0; i < 50; i++) {
      const target = router.routeRead('secondary');
      if (target) targets.add(target);
    }

    // Should have selected multiple different secondaries
    // (probabilistic, but with 50 tries should hit multiple)
    expect(targets.size).toBeGreaterThan(1);
    expect(targets.has('primary')).toBe(false);
  });
});

// ============================================================================
// Tests: Health Checks
// ============================================================================

describe('Read Replica - Health Checks', () => {
  let syncManager: ReplicaSyncManager;
  let mockNamespace: MockNamespaceResult;

  beforeEach(() => {
    mockNamespace = createMockNamespace();
    syncManager = new ReplicaSyncManager('shard-0', mockNamespace.namespace, {
      maxStalenessMs: 5000,
    });
  });

  it('should report disconnected status before first sync', () => {
    expect(syncManager.getStatus()).toBe('disconnected');
    expect(syncManager.isHealthy()).toBe(false);
  });

  it('should report healthy status after successful sync', async () => {
    mockNamespace.setResponse({
      entries: [],
      currentLSN: 10,
      timestamp: Date.now(),
    });

    await syncManager.sync(() => {});

    expect(syncManager.getStatus()).toBe('healthy');
    expect(syncManager.isHealthy()).toBe(true);
  });

  it('should report disconnected after multiple consecutive failures', async () => {
    // Fail 5 times
    for (let i = 0; i < 5; i++) {
      mockNamespace.setError(500, 'Error');
      await syncManager.sync(() => {});
    }

    expect(syncManager.getStatus()).toBe('disconnected');
    expect(syncManager.isHealthy()).toBe(false);
  });

  it('should recover health after successful sync following failures', async () => {
    // Fail a few times
    mockNamespace.setError(500, 'Error');
    await syncManager.sync(() => {});
    mockNamespace.setError(500, 'Error');
    await syncManager.sync(() => {});

    expect(syncManager.getState().consecutiveFailures).toBe(2);

    // Successful sync
    mockNamespace.setResponse({
      entries: [],
      currentLSN: 10,
      timestamp: Date.now(),
    });
    await syncManager.sync(() => {});

    expect(syncManager.getState().consecutiveFailures).toBe(0);
    expect(syncManager.getStatus()).toBe('healthy');
  });

  it('should support custom staleness threshold for health check', async () => {
    mockNamespace.setResponse({
      entries: [],
      currentLSN: 10,
      timestamp: Date.now() - 3000, // 3 second lag
    });

    await syncManager.sync(() => {});

    // Default threshold (5000ms) - should be healthy
    expect(syncManager.isHealthy()).toBe(true);

    // Stricter threshold (2000ms) - should not be healthy
    expect(syncManager.isHealthy(2000)).toBe(false);

    // Looser threshold (10000ms) - should be healthy
    expect(syncManager.isHealthy(10000)).toBe(true);
  });

  it('should track replication state accurately', async () => {
    mockNamespace.setResponse({
      entries: [createWalEntry(5, 'users', 'i', 'u1', { name: 'Alice' })],
      currentLSN: 5,
      timestamp: Date.now() - 1000,
    });

    await syncManager.sync(() => {});

    const state = syncManager.getState();
    expect(state.primaryShardId).toBe('shard-0');
    expect(state.appliedLSN).toBe(5);
    expect(state.consecutiveFailures).toBe(0);
    expect(state.lastSyncTimestamp).toBeGreaterThan(0);
    expect(state.lagMs).toBeGreaterThanOrEqual(1000);
  });

  it('should reset connection state on demand', () => {
    syncManager.resetConnection();

    const state = syncManager.getState();
    expect(state.consecutiveFailures).toBe(0);
  });
});

// ============================================================================
// Tests: HTTP Interface
// ============================================================================

describe('Read Replica - HTTP Interface', () => {
  let replica: ReplicaShardDO;
  let mockNamespace: MockNamespaceResult;
  let storage: DurableObjectStorage & { _data: Map<string, unknown> };

  beforeEach(() => {
    mockNamespace = createMockNamespace();
    storage = createMockStorage();
    const state = createMockState(storage, 'http-test-replica');
    const env = createMockEnv(createMockR2Bucket(), mockNamespace.namespace);

    replica = new ReplicaShardDO(state, env);
  });

  it('should handle /initialize endpoint', async () => {
    const request = new Request('https://replica/initialize', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ primaryShardId: 'primary-shard' }),
    });

    const response = await replica.fetch(request);
    const data = await response.json();

    expect(response.status).toBe(200);
    expect(data).toEqual({ success: true });
    expect(storage._data.get('primaryShardId')).toBe('primary-shard');
  });

  it('should handle /status endpoint', async () => {
    // Initialize first
    await replica.initialize('primary-shard');

    const request = new Request('https://replica/status', {
      method: 'GET',
    });

    const response = await replica.fetch(request);
    const data = (await response.json()) as { primaryShardId: string; initialized: boolean };

    expect(response.status).toBe(200);
    expect(data.primaryShardId).toBe('primary-shard');
    expect(data.initialized).toBe(true);
  });

  it('should return 404 for unknown endpoints', async () => {
    const request = new Request('https://replica/unknown', {
      method: 'GET',
    });

    const response = await replica.fetch(request);
    expect(response.status).toBe(404);
  });

  it('should return 500 for operations on uninitialized replica', async () => {
    const request = new Request('https://replica/find', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ collection: 'users', filter: {} }),
    });

    const response = await replica.fetch(request);
    expect(response.status).toBe(500);
  });
});
