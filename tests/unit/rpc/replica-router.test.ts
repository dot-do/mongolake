/**
 * ReplicaRouter Tests
 *
 * Tests for replica routing based on read preference.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { ReplicaRouter, type ReadPreference } from '../../../src/rpc/replica-router.js';

// Create mock namespace
function createMockNamespace(type: 'primary' | 'replica') {
  const mockStub = {
    fetch: vi.fn(async (request: Request) => {
      const body = await request.json() as { collection: string; filter: Record<string, unknown> };

      if (type === 'primary') {
        return new Response(JSON.stringify({
          documents: [
            { _id: 'doc1', name: 'Alice', source: 'primary' },
          ],
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      } else {
        return new Response(JSON.stringify({
          data: [
            { _id: 'doc1', name: 'Alice', source: 'replica' },
          ],
          stalenessMs: 500,
          isStale: false,
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
    }),
  };

  return {
    idFromName: vi.fn((name: string) => ({ name })),
    get: vi.fn(() => mockStub),
    newUniqueId: vi.fn(),
    stub: mockStub,
  } as unknown as DurableObjectNamespace & { stub: typeof mockStub };
}

describe('ReplicaRouter', () => {
  let primaryNamespace: ReturnType<typeof createMockNamespace>;
  let replicaNamespace: ReturnType<typeof createMockNamespace>;
  let router: ReplicaRouter;

  beforeEach(() => {
    primaryNamespace = createMockNamespace('primary');
    replicaNamespace = createMockNamespace('replica');
    router = new ReplicaRouter(
      primaryNamespace,
      replicaNamespace,
      { enabled: true, replicasPerShard: 2 }
    );
  });

  describe('read preference routing', () => {
    it('should route to primary with "primary" preference', async () => {
      const result = await router.find('shard-0', 'users', {}, {
        readPreference: 'primary',
      });

      expect(result.source).toBe('primary');
      expect(result.data[0].source).toBe('primary');
    });

    it('should route to replica with "secondary" preference', async () => {
      const result = await router.find('shard-0', 'users', {}, {
        readPreference: 'secondary',
      });

      expect(result.source).toBe('replica');
      expect(result.data[0].source).toBe('replica');
      expect(result.stalenessMs).toBeDefined();
    });

    it('should prefer primary with "primaryPreferred"', async () => {
      const result = await router.find('shard-0', 'users', {}, {
        readPreference: 'primaryPreferred',
      });

      expect(result.source).toBe('primary');
    });

    it('should fallback to replica when primary fails with "primaryPreferred"', async () => {
      // Make primary fail
      primaryNamespace.stub.fetch.mockImplementationOnce(async () => {
        throw new Error('Primary unavailable');
      });

      const result = await router.find('shard-0', 'users', {}, {
        readPreference: 'primaryPreferred',
      });

      expect(result.source).toBe('replica');
    });

    it('should prefer replica with "secondaryPreferred"', async () => {
      const result = await router.find('shard-0', 'users', {}, {
        readPreference: 'secondaryPreferred',
      });

      expect(result.source).toBe('replica');
    });

    it('should fallback to primary when replica is stale with "secondaryPreferred"', async () => {
      // Make replica return stale data
      replicaNamespace.stub.fetch.mockImplementationOnce(async () => {
        return new Response(JSON.stringify({
          data: [{ _id: 'doc1', source: 'replica' }],
          stalenessMs: 10000,
          isStale: true,
        }), { status: 200 });
      });

      const result = await router.find('shard-0', 'users', {}, {
        readPreference: 'secondaryPreferred',
      });

      expect(result.source).toBe('primary');
    });

    it('should allow stale reads when allowStale is true', async () => {
      replicaNamespace.stub.fetch.mockImplementationOnce(async () => {
        return new Response(JSON.stringify({
          data: [{ _id: 'doc1', source: 'replica' }],
          stalenessMs: 10000,
          isStale: true,
        }), { status: 200 });
      });

      const result = await router.find('shard-0', 'users', {}, {
        readPreference: 'secondaryPreferred',
        allowStale: true,
      });

      expect(result.source).toBe('replica');
      expect(result.isStale).toBe(true);
    });
  });

  describe('disabled routing', () => {
    it('should always use primary when replicas are disabled', async () => {
      const disabledRouter = new ReplicaRouter(
        primaryNamespace,
        replicaNamespace,
        { enabled: false }
      );

      const result = await disabledRouter.find('shard-0', 'users', {}, {
        readPreference: 'secondary',
      });

      expect(result.source).toBe('primary');
    });

    it('should always use primary when no replica namespace', async () => {
      const noReplicaRouter = new ReplicaRouter(
        primaryNamespace,
        undefined,
        { enabled: true }
      );

      const result = await noReplicaRouter.find('shard-0', 'users', {}, {
        readPreference: 'secondary',
      });

      expect(result.source).toBe('primary');
    });
  });

  describe('replica health tracking', () => {
    it('should mark replica unhealthy on failure', async () => {
      replicaNamespace.stub.fetch.mockImplementationOnce(async () => {
        return new Response('Error', { status: 500 });
      });

      try {
        await router.find('shard-0', 'users', {}, {
          readPreference: 'secondary',
        });
      } catch {
        // Expected to fail
      }

      expect(router.isReplicaHealthy('shard-0-replica-0')).toBe(false);
    });

    it('should mark replica healthy on success', async () => {
      // First fail
      replicaNamespace.stub.fetch.mockImplementationOnce(async () => {
        return new Response('Error', { status: 500 });
      });

      try {
        await router.find('shard-0', 'users', {}, {
          readPreference: 'secondary',
        });
      } catch {
        // Expected
      }

      // Then succeed
      await router.find('shard-0', 'users', {}, {
        readPreference: 'secondary',
      });

      expect(router.isReplicaHealthy('shard-0-replica-1')).toBe(true);
    });
  });

  describe('pool status', () => {
    it('should report pool status', () => {
      const status = router.getPoolStatus();

      expect(status.enabled).toBe(true);
      expect(status.replicasPerShard).toBe(2);
    });

    it('should report isEnabled correctly', () => {
      expect(router.isEnabled()).toBe(true);

      const disabledRouter = new ReplicaRouter(primaryNamespace, undefined, { enabled: false });
      expect(disabledRouter.isEnabled()).toBe(false);
    });
  });

  describe('configuration', () => {
    it('should allow runtime configuration updates', () => {
      router.configure({ defaultMaxStalenessMs: 10000 });

      const status = router.getPoolStatus();
      expect(status.enabled).toBe(true);
    });
  });
});
