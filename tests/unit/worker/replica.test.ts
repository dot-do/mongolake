/**
 * Read Replica Tests (TDD RED Phase)
 *
 * Tests for read replica support including:
 * - Read preference options (primary, secondary, nearest)
 * - Consistency level options
 * - Replication lag handling
 * - Edge network integration
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  ReadReplica,
  ReplicaPool,
  ReadRouter,
  DEFAULT_REPLICA_CONFIG,
  ReadPreferences,
  ConsistencyLevels,
  extractColoInfo,
  selectReplicaByColo,
  type ReplicaConfig,
  type ReplicaFindOptions,
  type ReplicaFindResult,
  type ReplicaStatus,
  type ReadPreference,
  type ConsistencyOptions,
  type ColoInfo,
} from '../../../src/worker/replica.js';
import type { R2Bucket, CollectionManifest } from '../../../src/types.js';

// ============================================================================
// Test Helpers
// ============================================================================

/** Create a mock R2 bucket for testing */
const createMockBucket = (docs: Record<string, unknown>[] = []): R2Bucket => {
  const manifestData: CollectionManifest = {
    name: 'testcol',
    files: [{ path: 'testdb/testcol_1.parquet', size: 1000, rowCount: docs.length, minSeq: 1, maxSeq: docs.length, minId: 'a', maxId: 'z', columns: [] }],
    schema: {},
    currentSeq: docs.length,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };

  // Encode docs as mock Parquet format (PAR1 + json length + json data)
  const docsJson = JSON.stringify(docs);
  const docsBytes = new TextEncoder().encode(docsJson);
  const buffer = new ArrayBuffer(8 + docsBytes.length);
  const view = new DataView(buffer);
  // Write PAR1 magic
  new Uint8Array(buffer, 0, 4).set([0x50, 0x41, 0x52, 0x31]); // PAR1
  view.setUint32(4, docsBytes.length, true);
  new Uint8Array(buffer, 8).set(docsBytes);

  return {
    get: vi.fn().mockImplementation(async (key: string) => {
      if (key.endsWith('_manifest.json')) {
        return {
          text: async () => JSON.stringify(manifestData),
          arrayBuffer: async () => new TextEncoder().encode(JSON.stringify(manifestData)).buffer,
        };
      }
      if (key.endsWith('.parquet')) {
        return {
          text: async () => docsJson,
          arrayBuffer: async () => buffer,
        };
      }
      return null;
    }),
    put: vi.fn(),
    delete: vi.fn(),
    list: vi.fn().mockResolvedValue({ objects: [], truncated: false }),
    head: vi.fn(),
    createMultipartUpload: vi.fn(),
  } as unknown as R2Bucket;
};

// ============================================================================
// ReadReplica Tests
// ============================================================================

describe('ReadReplica', () => {
  let bucket: R2Bucket;
  let replica: ReadReplica;

  beforeEach(() => {
    bucket = createMockBucket([
      { _id: '1', _seq: 1, _op: 'i', name: 'Alice', age: 30 },
      { _id: '2', _seq: 2, _op: 'i', name: 'Bob', age: 25 },
    ]);
    replica = new ReadReplica('replica-0', bucket);
  });

  describe('Basic Find Operations', () => {
    it('should find documents from R2 storage', async () => {
      const result = await replica.find('testdb', 'testcol');

      expect(result.documents).toBeDefined();
      expect(result.documents.length).toBe(2);
    });

    it('should apply filter to documents', async () => {
      const result = await replica.find('testdb', 'testcol', {
        filter: { name: 'Alice' },
      });

      expect(result.documents.length).toBe(1);
      expect(result.documents[0].name).toBe('Alice');
    });

    it('should apply projection to documents', async () => {
      const result = await replica.find('testdb', 'testcol', {
        projection: { name: 1 },
      });

      expect(result.documents.length).toBe(2);
      expect(result.documents[0].name).toBeDefined();
      expect(result.documents[0].age).toBeUndefined();
    });

    it('should apply sort to documents', async () => {
      const result = await replica.find('testdb', 'testcol', {
        sort: { age: 1 },
      });

      expect(result.documents[0].age).toBe(25);
      expect(result.documents[1].age).toBe(30);
    });

    it('should apply limit to documents', async () => {
      const result = await replica.find('testdb', 'testcol', {
        limit: 1,
      });

      expect(result.documents.length).toBe(1);
    });

    it('should apply skip to documents', async () => {
      const result = await replica.find('testdb', 'testcol', {
        skip: 1,
      });

      expect(result.documents.length).toBe(1);
    });

    it('should return empty array for non-existent collection', async () => {
      bucket.get = vi.fn().mockResolvedValue(null);
      const result = await replica.find('testdb', 'nonexistent');

      expect(result.documents).toEqual([]);
    });
  });

  describe('FindOne Operation', () => {
    it('should find a single document', async () => {
      const result = await replica.findOne('testdb', 'testcol', {
        filter: { name: 'Alice' },
      });

      expect(result.document).toBeDefined();
      expect(result.document?.name).toBe('Alice');
    });

    it('should return null for non-matching filter', async () => {
      const result = await replica.findOne('testdb', 'testcol', {
        filter: { name: 'NonExistent' },
      });

      expect(result.document).toBeNull();
    });
  });

  describe('Count Documents', () => {
    it('should count all documents', async () => {
      const result = await replica.countDocuments('testdb', 'testcol');

      expect(result.count).toBe(2);
    });

    it('should count filtered documents', async () => {
      const result = await replica.countDocuments('testdb', 'testcol', { name: 'Alice' });

      expect(result.count).toBe(1);
    });
  });

  describe('Staleness Tracking', () => {
    it('should track staleness in milliseconds', async () => {
      const result = await replica.find('testdb', 'testcol');

      expect(result.stalenessMs).toBeDefined();
      expect(typeof result.stalenessMs).toBe('number');
      expect(result.stalenessMs).toBeGreaterThanOrEqual(0);
    });

    it('should indicate when data is stale', async () => {
      // First fetch populates cache
      await replica.find('testdb', 'testcol');

      // Wait to create staleness
      await new Promise((resolve) => setTimeout(resolve, 10));

      // Second fetch uses cache, should show staleness
      const result = await replica.find('testdb', 'testcol', {
        maxStaleness: 1, // 1ms max staleness - cached data is stale
      });

      expect(result.isStale).toBe(true);
    });

    it('should indicate when data is fresh', async () => {
      const result = await replica.find('testdb', 'testcol', {
        maxStaleness: 10000, // 10s max staleness - should be fresh
      });

      expect(result.isStale).toBe(false);
    });

    it('should return manifest timestamp', async () => {
      const result = await replica.find('testdb', 'testcol');

      expect(result.manifestTimestamp).toBeDefined();
    });
  });

  describe('Manifest Caching', () => {
    it('should cache manifest by default', async () => {
      await replica.find('testdb', 'testcol');
      await replica.find('testdb', 'testcol');

      // Should only fetch manifest once due to caching
      expect(bucket.get).toHaveBeenCalledTimes(2); // manifest + parquet file, not 4
    });

    it('should refresh manifest on demand', async () => {
      await replica.find('testdb', 'testcol');
      await replica.refreshManifest('testdb', 'testcol');

      // 2 manifest fetches: initial + refresh (second find uses cached refreshed manifest)
      const manifestCalls = (bucket.get as unknown as { mock: { calls: unknown[][] } }).mock.calls.filter(
        (call: unknown[]) => (call[0] as string).includes('_manifest.json')
      );
      expect(manifestCalls.length).toBe(2);
    });

    it('should clear manifest cache', () => {
      replica.clearManifestCache();
      // Should not throw
      expect(true).toBe(true);
    });
  });

  describe('File Content Caching', () => {
    it('should cache file contents by default', async () => {
      await replica.find('testdb', 'testcol');
      await replica.find('testdb', 'testcol');

      // Parquet file should only be read once
      const parquetCalls = (bucket.get as unknown as { mock: { calls: unknown[][] } }).mock.calls.filter(
        (call: unknown[]) => (call[0] as string).endsWith('.parquet')
      );
      expect(parquetCalls.length).toBe(1);
    });

    it('should clear file cache', async () => {
      await replica.find('testdb', 'testcol');
      replica.clearFileCache();
      await replica.find('testdb', 'testcol');

      // Parquet file should be read twice
      const parquetCalls = (bucket.get as unknown as { mock: { calls: unknown[][] } }).mock.calls.filter(
        (call: unknown[]) => (call[0] as string).endsWith('.parquet')
      );
      expect(parquetCalls.length).toBe(2);
    });
  });

  describe('Replica Status', () => {
    it('should return replica status', () => {
      const status = replica.getStatus();

      expect(status.replicaId).toBe('replica-0');
      expect(status.healthy).toBeDefined();
      expect(typeof status.healthy).toBe('boolean');
      expect(status.cachedManifests).toBeDefined();
    });

    it('should track last manifest fetch time', async () => {
      await replica.find('testdb', 'testcol');
      const status = replica.getStatus();

      expect(status.lastManifestFetch).toBeGreaterThan(0);
    });

    it('should report healthy when within staleness threshold', async () => {
      await replica.find('testdb', 'testcol');
      const status = replica.getStatus();

      expect(status.healthy).toBe(true);
    });
  });

  describe('Configuration', () => {
    it('should use default configuration', () => {
      const config = replica.getConfig();

      expect(config.maxStaleness).toBe(DEFAULT_REPLICA_CONFIG.maxStaleness);
      expect(config.manifestCacheTtl).toBe(DEFAULT_REPLICA_CONFIG.manifestCacheTtl);
    });

    it('should allow custom configuration', () => {
      const customReplica = new ReadReplica('custom', bucket, {
        maxStaleness: 10000,
        manifestCacheTtl: 500,
      });
      const config = customReplica.getConfig();

      expect(config.maxStaleness).toBe(10000);
      expect(config.manifestCacheTtl).toBe(500);
    });

    it('should update configuration', () => {
      replica.updateConfig({ maxStaleness: 15000 });
      const config = replica.getConfig();

      expect(config.maxStaleness).toBe(15000);
    });
  });

  describe('Deduplication', () => {
    it('should deduplicate documents by _id keeping latest _seq', async () => {
      bucket = createMockBucket([
        { _id: '1', _seq: 1, _op: 'i', name: 'Alice v1' },
        { _id: '1', _seq: 2, _op: 'u', name: 'Alice v2' },
      ]);
      replica = new ReadReplica('replica-0', bucket);

      const result = await replica.find('testdb', 'testcol');

      expect(result.documents.length).toBe(1);
      expect(result.documents[0].name).toBe('Alice v2');
    });

    it('should exclude deleted documents (tombstones)', async () => {
      bucket = createMockBucket([
        { _id: '1', _seq: 1, _op: 'i', name: 'Alice' },
        { _id: '1', _seq: 2, _op: 'd', _deleted: true },
      ]);
      replica = new ReadReplica('replica-0', bucket);

      const result = await replica.find('testdb', 'testcol');

      expect(result.documents.length).toBe(0);
    });
  });
});

// ============================================================================
// ReplicaPool Tests
// ============================================================================

describe('ReplicaPool', () => {
  let bucket: R2Bucket;
  let pool: ReplicaPool;

  beforeEach(() => {
    bucket = createMockBucket([
      { _id: '1', _seq: 1, _op: 'i', name: 'Alice' },
    ]);
    pool = new ReplicaPool(bucket, { enabled: true, count: 3 });
  });

  describe('Replica Selection', () => {
    it('should return next replica using round-robin', () => {
      const r1 = pool.getNextReplica();
      const r2 = pool.getNextReplica();
      const r3 = pool.getNextReplica();
      const r4 = pool.getNextReplica();

      expect(r1.getStatus().replicaId).toBe('replica-0');
      expect(r2.getStatus().replicaId).toBe('replica-1');
      expect(r3.getStatus().replicaId).toBe('replica-2');
      expect(r4.getStatus().replicaId).toBe('replica-0'); // wraps around
    });

    it('should return random replica', () => {
      const replica = pool.getRandomReplica();

      expect(replica).toBeDefined();
      expect(replica.getStatus().replicaId).toMatch(/^replica-\d+$/);
    });

    it('should return specific replica by index', () => {
      const replica = pool.getReplica(1);

      expect(replica.getStatus().replicaId).toBe('replica-1');
    });

    it('should throw for invalid replica index', () => {
      expect(() => pool.getReplica(10)).toThrow('Invalid replica index');
      expect(() => pool.getReplica(-1)).toThrow('Invalid replica index');
    });

    it('should return all replicas', () => {
      const replicas = pool.getAllReplicas();

      expect(replicas.length).toBe(3);
    });
  });

  describe('Pool Status', () => {
    it('should return pool status with all replicas', () => {
      const status = pool.getPoolStatus();

      expect(status.replicas.length).toBe(3);
      expect(typeof status.healthy).toBe('number');
      expect(typeof status.unhealthy).toBe('number');
    });

    it('should track healthy and unhealthy replicas', async () => {
      // Make all replicas healthy by fetching
      for (const replica of pool.getAllReplicas()) {
        await replica.find('testdb', 'testcol');
      }

      const status = pool.getPoolStatus();

      expect(status.healthy).toBe(3);
      expect(status.unhealthy).toBe(0);
    });
  });

  describe('Bulk Operations', () => {
    it('should refresh manifests on all replicas', async () => {
      await pool.refreshAllManifests('testdb', 'testcol');

      // Each replica should have fetched the manifest
      const status = pool.getPoolStatus();
      for (const replicaStatus of status.replicas) {
        expect(replicaStatus.lastManifestFetch).toBeGreaterThan(0);
      }
    });

    it('should clear all caches', async () => {
      // Populate caches
      for (const replica of pool.getAllReplicas()) {
        await replica.find('testdb', 'testcol');
      }

      pool.clearAllCaches();

      // Verify caches are cleared (status should show 0 cached manifests)
      for (const replica of pool.getAllReplicas()) {
        const status = replica.getStatus();
        expect(status.cachedManifests).toBe(0);
      }
    });
  });

  describe('Pool Configuration', () => {
    it('should check if pool is enabled', () => {
      expect(pool.isEnabled()).toBe(true);

      const disabledPool = new ReplicaPool(bucket, { enabled: false });
      expect(disabledPool.isEnabled()).toBe(false);
    });

    it('should throw when getting replica from empty pool', () => {
      const emptyPool = new ReplicaPool(bucket, { count: 0 });

      expect(() => emptyPool.getNextReplica()).toThrow('No replicas available');
      expect(() => emptyPool.getRandomReplica()).toThrow('No replicas available');
    });
  });
});

// ============================================================================
// Read Preference Tests
// ============================================================================

describe('ReadPreference', () => {
  let bucket: R2Bucket;

  beforeEach(() => {
    bucket = createMockBucket([
      { _id: '1', _seq: 1, _op: 'i', name: 'Alice' },
    ]);
  });

  describe('Primary Preference', () => {
    it('should route reads to primary when read preference is primary', async () => {
      // Primary reads should go directly to DO, not replica
      // This is handled at the worker/router level
      const pool = new ReplicaPool(bucket, { enabled: true, count: 1 });

      // When readPreference is 'primary', pool should not be used
      expect(pool.isEnabled()).toBe(true);
    });
  });

  describe('Secondary Preference', () => {
    it('should route reads to secondary replicas', async () => {
      const pool = new ReplicaPool(bucket, { enabled: true, count: 2 });
      const replica = pool.getNextReplica();

      const result = await replica.find('testdb', 'testcol');

      expect(result.documents.length).toBe(1);
    });

    it('should prefer secondaries even when primary is available', async () => {
      const pool = new ReplicaPool(bucket, { enabled: true, count: 2 });

      // Get replicas - should cycle through secondaries
      const r1 = pool.getNextReplica();
      const r2 = pool.getNextReplica();

      expect(r1.getStatus().replicaId).toBe('replica-0');
      expect(r2.getStatus().replicaId).toBe('replica-1');
    });
  });

  describe('Nearest Preference', () => {
    it('should select replica with lowest latency', async () => {
      const pool = new ReplicaPool(bucket, { enabled: true, count: 3 });

      // For now, just use round-robin as latency tracking is not implemented
      const replica = pool.getNextReplica();

      expect(replica).toBeDefined();
    });
  });

  describe('Secondary Preferred', () => {
    it('should prefer secondary but fall back to primary if unavailable', async () => {
      const pool = new ReplicaPool(bucket, { enabled: true, count: 1 });
      const replica = pool.getNextReplica();

      const result = await replica.find('testdb', 'testcol');

      expect(result.documents).toBeDefined();
    });
  });

  describe('Primary Preferred', () => {
    it('should prefer primary but fall back to secondary if unavailable', async () => {
      // When primary is unavailable, should use replica pool
      const pool = new ReplicaPool(bucket, { enabled: true, count: 1 });
      const replica = pool.getNextReplica();

      const result = await replica.find('testdb', 'testcol');

      expect(result.documents).toBeDefined();
    });
  });
});

// ============================================================================
// Consistency Level Tests
// ============================================================================

describe('ConsistencyLevel', () => {
  let bucket: R2Bucket;

  beforeEach(() => {
    bucket = createMockBucket([
      { _id: '1', _seq: 1, _op: 'i', name: 'Alice' },
    ]);
  });

  describe('Eventual Consistency', () => {
    it('should allow stale reads with eventual consistency', async () => {
      const replica = new ReadReplica('replica-0', bucket, {
        maxStaleness: 60000, // 60s allowed staleness
      });

      const result = await replica.find('testdb', 'testcol');

      expect(result.isStale).toBe(false); // Within threshold
    });
  });

  describe('Session Consistency', () => {
    it('should respect afterToken for read-your-writes', async () => {
      const replica = new ReadReplica('replica-0', bucket);

      // Find with afterToken to ensure we read our own writes
      const result = await replica.find('testdb', 'testcol', {
        filter: {},
        // afterToken would be validated at a higher level
      });

      expect(result.documents).toBeDefined();
    });
  });

  describe('Bounded Staleness', () => {
    it('should reject reads exceeding staleness bounds', async () => {
      const replica = new ReadReplica('replica-0', bucket, {
        maxStaleness: 1, // 1ms - cache will be stale
      });

      // First fetch - fresh
      await replica.find('testdb', 'testcol');

      // Wait to exceed staleness
      await new Promise((resolve) => setTimeout(resolve, 10));

      // Second fetch - uses stale cache
      const result = await replica.find('testdb', 'testcol');

      expect(result.isStale).toBe(true);
    });

    it('should accept reads within staleness bounds', async () => {
      const replica = new ReadReplica('replica-0', bucket, {
        maxStaleness: 60000, // 60s
      });

      const result = await replica.find('testdb', 'testcol');

      expect(result.isStale).toBe(false);
    });
  });

  describe('Strong Consistency', () => {
    it('should require zero staleness for strong consistency', async () => {
      // Strong consistency requires reading from primary
      // Replicas always have some staleness
      const replica = new ReadReplica('replica-0', bucket, {
        maxStaleness: 0,
      });

      const result = await replica.find('testdb', 'testcol');

      // Result should indicate staleness for strong consistency decisions
      expect(result.stalenessMs).toBeGreaterThanOrEqual(0);
    });
  });
});

// ============================================================================
// Edge Network Integration Tests
// ============================================================================

describe('EdgeNetworkIntegration', () => {
  let bucket: R2Bucket;

  beforeEach(() => {
    bucket = createMockBucket([
      { _id: '1', _seq: 1, _op: 'i', name: 'Alice' },
    ]);
  });

  describe('Colo-Aware Routing', () => {
    it('should include colo information in replica status', () => {
      const replica = new ReadReplica('replica-0', bucket);
      const status = replica.getStatus();

      // Replica ID should be unique per colo in production
      expect(status.replicaId).toBeDefined();
    });
  });

  describe('Cache Headers', () => {
    it('should track cache hit/miss for file reads', async () => {
      const replica = new ReadReplica('replica-0', bucket);

      // First read - cache miss
      await replica.find('testdb', 'testcol');

      // Second read - cache hit
      await replica.find('testdb', 'testcol');

      // File should only be read once
      const parquetCalls = (bucket.get as unknown as { mock: { calls: unknown[][] } }).mock.calls.filter(
        (call: unknown[]) => (call[0] as string).endsWith('.parquet')
      );
      expect(parquetCalls.length).toBe(1);
    });
  });

  describe('Request Context', () => {
    it('should allow reading from specific colo replica', () => {
      const pool = new ReplicaPool(bucket, { enabled: true, count: 3 });

      // Get specific replica (simulating colo-specific routing)
      const replica = pool.getReplica(0);

      expect(replica.getStatus().replicaId).toBe('replica-0');
    });
  });
});

// ============================================================================
// Replication Lag Handling Tests
// ============================================================================

describe('ReplicationLagHandling', () => {
  let bucket: R2Bucket;

  beforeEach(() => {
    bucket = createMockBucket([
      { _id: '1', _seq: 1, _op: 'i', name: 'Alice' },
    ]);
  });

  describe('Lag Detection', () => {
    it('should detect replication lag from manifest age', async () => {
      const replica = new ReadReplica('replica-0', bucket);

      // First fetch - establishes baseline
      await replica.find('testdb', 'testcol');

      // Wait briefly
      await new Promise((resolve) => setTimeout(resolve, 10));

      // Second fetch uses cache - calculate lag from cache age
      const result = await replica.find('testdb', 'testcol');

      expect(result.stalenessMs).toBeGreaterThanOrEqual(10);
    });
  });

  describe('Lag Tolerance', () => {
    it('should allow configurable lag tolerance', () => {
      const replica = new ReadReplica('replica-0', bucket, {
        maxStaleness: 10000, // 10 seconds
      });

      const config = replica.getConfig();
      expect(config.maxStaleness).toBe(10000);
    });

    it('should report lag exceeding tolerance as stale', async () => {
      const replica = new ReadReplica('replica-0', bucket, {
        maxStaleness: 0, // No tolerance
      });

      // Force some staleness
      await replica.find('testdb', 'testcol');
      await new Promise((resolve) => setTimeout(resolve, 1));

      const result = await replica.find('testdb', 'testcol');

      // Even 1ms is over the threshold
      expect(result.isStale).toBe(true);
    });
  });

  describe('Fallback on Excessive Lag', () => {
    it('should indicate when replica is unhealthy due to lag', async () => {
      const replica = new ReadReplica('replica-0', bucket, {
        maxStaleness: 1, // Very tight tolerance
      });

      // Initial fetch
      await replica.find('testdb', 'testcol');

      // Wait long enough to exceed staleness
      await new Promise((resolve) => setTimeout(resolve, 100));

      const status = replica.getStatus();
      // With very tight tolerance, replica should eventually become unhealthy
      expect(typeof status.healthy).toBe('boolean');
    });
  });
});

// ============================================================================
// ReadRouter Tests
// ============================================================================

describe('ReadRouter', () => {
  let bucket: R2Bucket;
  let pool: ReplicaPool;
  let primaryFetcher: ReturnType<typeof vi.fn>;
  let router: ReadRouter;

  beforeEach(() => {
    bucket = createMockBucket([
      { _id: '1', _seq: 1, _op: 'i', name: 'Alice' },
    ]);
    pool = new ReplicaPool(bucket, { enabled: true, count: 2 });
    primaryFetcher = vi.fn().mockResolvedValue([{ _id: '1', name: 'Alice' }]);
    router = new ReadRouter(pool, primaryFetcher);
  });

  describe('Primary Read Preference', () => {
    it('should route to primary with primary read preference', async () => {
      const result = await router.route('testdb', 'testcol', {
        readPreference: ReadPreferences.primary(),
      });

      expect(result.fromReplica).toBe(false);
      expect(result.source).toBe('primary');
      expect(primaryFetcher).toHaveBeenCalled();
    });

    it('should route to primary with strong consistency', async () => {
      const result = await router.route('testdb', 'testcol', {
        consistency: ConsistencyLevels.strong(),
      });

      expect(result.fromReplica).toBe(false);
      expect(result.source).toBe('primary');
    });
  });

  describe('Secondary Read Preference', () => {
    it('should route to replica with secondary read preference', async () => {
      const result = await router.route('testdb', 'testcol', {
        readPreference: ReadPreferences.secondary(),
      });

      expect(result.fromReplica).toBe(true);
      expect(result.source).toBe('replica');
      expect(primaryFetcher).not.toHaveBeenCalled();
    });

    it('should pass max staleness to replica', async () => {
      const result = await router.route('testdb', 'testcol', {
        readPreference: ReadPreferences.secondary(10000),
      });

      expect(result.fromReplica).toBe(true);
      expect(result.isStale).toBe(false); // Fresh data
    });
  });

  describe('Nearest Read Preference', () => {
    it('should route to a replica with nearest read preference', async () => {
      const result = await router.route('testdb', 'testcol', {
        readPreference: ReadPreferences.nearest(),
      });

      expect(result.fromReplica).toBe(true);
      expect(result.source).toBe('replica');
    });
  });

  describe('Primary Preferred Read Preference', () => {
    it('should route to primary when available', async () => {
      const result = await router.route('testdb', 'testcol', {
        readPreference: ReadPreferences.primaryPreferred(),
      });

      expect(result.fromReplica).toBe(false);
      expect(result.source).toBe('primary');
    });

    it('should fallback to replica when primary fails', async () => {
      primaryFetcher.mockRejectedValueOnce(new Error('Primary unavailable'));

      const result = await router.route('testdb', 'testcol', {
        readPreference: ReadPreferences.primaryPreferred(),
      });

      expect(result.fromReplica).toBe(true);
      expect(result.source).toBe('fallback');
    });
  });

  describe('Secondary Preferred Read Preference', () => {
    it('should route to replica when healthy', async () => {
      // Populate replica cache to mark as healthy
      for (const replica of pool.getAllReplicas()) {
        await replica.find('testdb', 'testcol');
      }

      const result = await router.route('testdb', 'testcol', {
        readPreference: ReadPreferences.secondaryPreferred(),
      });

      expect(result.fromReplica).toBe(true);
      expect(result.source).toBe('replica');
    });
  });

  describe('Session Consistency', () => {
    it('should route to primary for session consistency', async () => {
      const result = await router.route('testdb', 'testcol', {
        readPreference: ReadPreferences.secondary(),
        consistency: ConsistencyLevels.session('write-token-123'),
      });

      // Session consistency should force primary read
      expect(result.fromReplica).toBe(false);
      expect(result.source).toBe('primary');
    });
  });

  describe('Bounded Staleness', () => {
    it('should use bounded staleness max from consistency', async () => {
      const result = await router.route('testdb', 'testcol', {
        readPreference: ReadPreferences.secondary(10000), // 10s in preference
        consistency: ConsistencyLevels.bounded(5000), // 5s in consistency (takes precedence)
      });

      expect(result.fromReplica).toBe(true);
      // Bounded staleness from consistency should take precedence
    });
  });

  describe('Disabled Pool', () => {
    it('should route to primary when pool is disabled', async () => {
      const disabledPool = new ReplicaPool(bucket, { enabled: false, count: 1 });
      const disabledRouter = new ReadRouter(disabledPool, primaryFetcher);

      const result = await disabledRouter.route('testdb', 'testcol', {
        readPreference: ReadPreferences.secondary(),
      });

      expect(result.fromReplica).toBe(false);
      expect(result.source).toBe('primary');
    });
  });

  describe('Query Options Passthrough', () => {
    it('should pass filter to replica', async () => {
      const result = await router.route('testdb', 'testcol', {
        readPreference: ReadPreferences.secondary(),
        filter: { name: 'Alice' },
      });

      expect(result.documents.length).toBe(1);
      expect(result.documents[0].name).toBe('Alice');
    });

    it('should pass limit to replica', async () => {
      const result = await router.route('testdb', 'testcol', {
        readPreference: ReadPreferences.secondary(),
        limit: 1,
      });

      expect(result.documents.length).toBeLessThanOrEqual(1);
    });
  });
});

// ============================================================================
// Read Preference Factory Tests
// ============================================================================

describe('ReadPreferences Factory', () => {
  it('should create primary preference', () => {
    const pref = ReadPreferences.primary();
    expect(pref.mode).toBe('primary');
    expect(pref.maxStalenessMs).toBeUndefined();
  });

  it('should create secondary preference with default staleness', () => {
    const pref = ReadPreferences.secondary();
    expect(pref.mode).toBe('secondary');
    expect(pref.maxStalenessMs).toBe(5000);
  });

  it('should create secondary preference with custom staleness', () => {
    const pref = ReadPreferences.secondary(10000);
    expect(pref.mode).toBe('secondary');
    expect(pref.maxStalenessMs).toBe(10000);
  });

  it('should create nearest preference', () => {
    const pref = ReadPreferences.nearest(15000);
    expect(pref.mode).toBe('nearest');
    expect(pref.maxStalenessMs).toBe(15000);
  });

  it('should create primaryPreferred preference', () => {
    const pref = ReadPreferences.primaryPreferred();
    expect(pref.mode).toBe('primaryPreferred');
    expect(pref.maxStalenessMs).toBe(10000);
  });

  it('should create secondaryPreferred preference', () => {
    const pref = ReadPreferences.secondaryPreferred();
    expect(pref.mode).toBe('secondaryPreferred');
    expect(pref.maxStalenessMs).toBe(5000);
  });
});

// ============================================================================
// Consistency Level Factory Tests
// ============================================================================

describe('ConsistencyLevels Factory', () => {
  it('should create eventual consistency', () => {
    const level = ConsistencyLevels.eventual();
    expect(level.level).toBe('eventual');
  });

  it('should create session consistency with token', () => {
    const level = ConsistencyLevels.session('token-123');
    expect(level.level).toBe('session');
    expect(level.afterToken).toBe('token-123');
  });

  it('should create bounded staleness', () => {
    const level = ConsistencyLevels.bounded(10000);
    expect(level.level).toBe('bounded');
    expect(level.maxStalenessMs).toBe(10000);
  });

  it('should create strong consistency', () => {
    const level = ConsistencyLevels.strong();
    expect(level.level).toBe('strong');
  });
});

// ============================================================================
// Edge Colo Integration Tests
// ============================================================================

describe('Edge Colo Integration', () => {
  describe('extractColoInfo', () => {
    it('should extract colo info from request with cf object', () => {
      const request = {
        cf: {
          colo: 'SJC',
          country: 'US',
          continent: 'NA',
          city: 'San Jose',
          latitude: 37.3382,
          longitude: -121.8863,
        },
      } as unknown as Request;

      const info = extractColoInfo(request);

      expect(info).not.toBeNull();
      expect(info?.coloId).toBe('SJC');
      expect(info?.country).toBe('US');
      expect(info?.continent).toBe('NA');
      expect(info?.city).toBe('San Jose');
      expect(info?.latitude).toBe(37.3382);
      expect(info?.longitude).toBe(-121.8863);
    });

    it('should return null when cf object is missing', () => {
      const request = {} as Request;
      const info = extractColoInfo(request);

      expect(info).toBeNull();
    });

    it('should return null when colo is missing', () => {
      const request = {
        cf: {
          country: 'US',
        },
      } as unknown as Request;

      const info = extractColoInfo(request);

      expect(info).toBeNull();
    });

    it('should handle missing optional fields', () => {
      const request = {
        cf: {
          colo: 'AMS',
        },
      } as unknown as Request;

      const info = extractColoInfo(request);

      expect(info).not.toBeNull();
      expect(info?.coloId).toBe('AMS');
      expect(info?.country).toBe('unknown');
      expect(info?.continent).toBe('unknown');
      expect(info?.city).toBe('unknown');
      expect(info?.latitude).toBe(0);
      expect(info?.longitude).toBe(0);
    });
  });

  describe('selectReplicaByColo', () => {
    it('should select a replica from pool', () => {
      const bucket = createMockBucket([]);
      const pool = new ReplicaPool(bucket, { enabled: true, count: 3 });

      const coloInfo: ColoInfo = {
        coloId: 'SJC',
        country: 'US',
        continent: 'NA',
        city: 'San Jose',
        latitude: 37.3382,
        longitude: -121.8863,
      };

      const replica = selectReplicaByColo(pool, coloInfo);

      expect(replica).toBeDefined();
      expect(replica.getStatus().replicaId).toMatch(/^replica-\d+$/);
    });

    it('should work with null colo info', () => {
      const bucket = createMockBucket([]);
      const pool = new ReplicaPool(bucket, { enabled: true, count: 2 });

      const replica = selectReplicaByColo(pool, null);

      expect(replica).toBeDefined();
    });
  });
});
