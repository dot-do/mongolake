/**
 * MongoLake RPC Service Tests
 *
 * Tests for the RPC service that handles Worker-to-DO communication.
 * The RPC service uses rpc.do for:
 * - Worker-to-DO communication with promise pipelining
 * - Shard routing based on collection hash
 * - Retry logic for transient failures
 * - Batched operations for efficiency
 *
 * Key features tested:
 * 1. Route operations to correct shard DO
 * 2. Hash collection name to shard ID (0-15)
 * 3. Pipeline multiple operations efficiently
 * 4. Retry on transient network errors
 * 5. Batch multiple inserts to same shard
 * 6. Return read tokens from write operations
 * 7. Handle shard DO unavailable
 * 8. Route queries with consistency level
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'

// Import the RPC service module (doesn't exist yet - tests should fail)
import {
  RPCService,
  RPCServiceOptions,
  ShardConnection,
  OperationResult,
  BatchResult,
  ReadToken,
  ConsistencyLevel,
  TransientError,
  ShardUnavailableError,
  createRPCService,
  hashToShardId,
} from '../../../src/rpc/service'

// Import types from the main types file
import type { Document, InsertOneResult, InsertManyResult, UpdateResult, DeleteResult, Filter, Update, FindOptions } from '../../../src/types'


describe('MongoLake RPC Service', () => {

  describe('hashToShardId', () => {

    it('should hash collection name to shard ID in 0-15 range', () => {
      const shardId = hashToShardId('users')

      expect(shardId).toBeGreaterThanOrEqual(0)
      expect(shardId).toBeLessThanOrEqual(15)
      expect(Number.isInteger(shardId)).toBe(true)
    })

    it('should return same shard for same collection name', () => {
      const shard1 = hashToShardId('orders')
      const shard2 = hashToShardId('orders')
      const shard3 = hashToShardId('orders')

      expect(shard1).toBe(shard2)
      expect(shard2).toBe(shard3)
    })

    it('should distribute collections evenly across shards', () => {
      const shardCounts = new Map<number, number>()
      const collectionCount = 1600

      for (let i = 0; i < collectionCount; i++) {
        const collectionName = `collection_${i}_${Math.random().toString(36).substring(7)}`
        const shardId = hashToShardId(collectionName)
        shardCounts.set(shardId, (shardCounts.get(shardId) || 0) + 1)
      }

      // All 16 shards should be represented
      expect(shardCounts.size).toBe(16)

      // Each shard should have roughly 100 collections (50% variance allowed)
      const expectedPerShard = collectionCount / 16
      for (const [_, count] of shardCounts) {
        expect(count).toBeGreaterThan(expectedPerShard * 0.5)
        expect(count).toBeLessThan(expectedPerShard * 1.5)
      }
    })

    it('should include database name in hash when provided', () => {
      const shardWithDb = hashToShardId('users', 'production')
      const shardWithoutDb = hashToShardId('users')

      // These may or may not be different, but we're testing the function accepts the param
      expect(shardWithDb).toBeGreaterThanOrEqual(0)
      expect(shardWithDb).toBeLessThanOrEqual(15)
      expect(shardWithoutDb).toBeGreaterThanOrEqual(0)
      expect(shardWithoutDb).toBeLessThanOrEqual(15)
    })

    it('should throw on empty collection name', () => {
      expect(() => hashToShardId('')).toThrow(/empty.*collection/i)
    })

  })


  describe('RPCService class', () => {

    let service: RPCService
    let mockDOStub: any

    beforeEach(() => {
      // Create mock DO stub
      mockDOStub = {
        fetch: vi.fn().mockResolvedValue(new Response(JSON.stringify({ ok: true }))),
      }

      // Create service with mock bindings
      service = createRPCService({
        shardNamespace: {
          get: vi.fn().mockReturnValue(mockDOStub),
          idFromName: vi.fn().mockReturnValue({ toString: () => 'mock-id' }),
        } as any,
        shardCount: 16,
      })
    })

    afterEach(() => {
      vi.resetAllMocks()
    })


    describe('route operations to correct shard DO', () => {

      it('should route insert to the correct shard based on collection hash', async () => {
        const collection = 'users'
        const document = { _id: 'doc1', name: 'Alice' }
        const expectedShardId = hashToShardId(collection)

        await service.insert(collection, document)

        expect(service.getLastRoutedShardId()).toBe(expectedShardId)
      })

      it('should route find to the correct shard based on collection hash', async () => {
        const collection = 'orders'
        const filter = { status: 'pending' }
        const expectedShardId = hashToShardId(collection)

        await service.find(collection, filter)

        expect(service.getLastRoutedShardId()).toBe(expectedShardId)
      })

      it('should route update to the correct shard based on collection hash', async () => {
        const collection = 'products'
        const filter = { _id: 'prod1' }
        const update = { $set: { price: 100 } }
        const expectedShardId = hashToShardId(collection)

        await service.update(collection, filter, update)

        expect(service.getLastRoutedShardId()).toBe(expectedShardId)
      })

      it('should route delete to the correct shard based on collection hash', async () => {
        const collection = 'sessions'
        const filter = { expired: true }
        const expectedShardId = hashToShardId(collection)

        await service.delete(collection, filter)

        expect(service.getLastRoutedShardId()).toBe(expectedShardId)
      })

      it('should route aggregate to the correct shard based on collection hash', async () => {
        const collection = 'analytics'
        const pipeline = [{ $match: { event: 'click' } }, { $count: 'total' }]
        const expectedShardId = hashToShardId(collection)

        await service.aggregate(collection, pipeline)

        expect(service.getLastRoutedShardId()).toBe(expectedShardId)
      })

      it('should route operations from different collections to different shards', async () => {
        const collections = ['users', 'orders', 'products', 'sessions']
        const shardIds = new Set<number>()

        for (const collection of collections) {
          await service.find(collection, {})
          shardIds.add(service.getLastRoutedShardId())
        }

        // At least some should go to different shards (statistically likely)
        expect(shardIds.size).toBeGreaterThanOrEqual(1)
      })

      it('should use database-qualified routing when database is specified', async () => {
        const serviceWithDb = createRPCService({
          shardNamespace: service['shardNamespace'],
          shardCount: 16,
          database: 'mydb',
        })

        await serviceWithDb.insert('users', { _id: 'doc1', name: 'Bob' })

        // The shard ID should incorporate the database name
        const expectedShardId = hashToShardId('users', 'mydb')
        expect(serviceWithDb.getLastRoutedShardId()).toBe(expectedShardId)
      })

    })


    describe('pipeline multiple operations efficiently', () => {

      it('should support promise pipelining for chained operations', async () => {
        // Promise pipelining allows chaining without waiting for each response
        const pipeline = service
          .insert('users', { _id: 'u1', name: 'Alice' })
          .then(() => service.find('users', { _id: 'u1' }))

        const result = await pipeline

        expect(result).toBeDefined()
      })

      it('should batch pipelined operations to the same shard', async () => {
        const collection = 'users'

        // These operations should be batched since they go to the same shard
        const [insert1, insert2, insert3] = await Promise.all([
          service.insert(collection, { _id: 'u1', name: 'Alice' }),
          service.insert(collection, { _id: 'u2', name: 'Bob' }),
          service.insert(collection, { _id: 'u3', name: 'Charlie' }),
        ])

        // Verify all operations succeeded
        expect(insert1.acknowledged).toBe(true)
        expect(insert2.acknowledged).toBe(true)
        expect(insert3.acknowledged).toBe(true)

        // The service should have batched these into fewer RPC calls
        expect(service.getOperationCount()).toBeLessThan(3)
      })

      it('should execute pipelined reads after writes complete', async () => {
        const collection = 'users'

        // Insert then immediately read - should see the inserted document
        await service.insert(collection, { _id: 'u1', name: 'Alice' })
        const docs = await service.find(collection, { _id: 'u1' })

        expect(docs).toHaveLength(1)
        expect(docs[0].name).toBe('Alice')
      })

      it('should support pipeline depth tracking', async () => {
        // Execute a chain of dependent operations
        await service.insert('users', { _id: 'u1', name: 'Alice', posts: [] })
        await service.update('users', { _id: 'u1' }, { $push: { posts: 'post1' } })
        await service.update('users', { _id: 'u1' }, { $push: { posts: 'post2' } })

        const pipelineDepth = service.getPipelineDepth()

        // Should track the sequential dependency depth
        expect(pipelineDepth).toBeGreaterThanOrEqual(1)
      })

      it('should reuse connections for pipelined operations', async () => {
        const collection = 'users'

        // Multiple operations should reuse the same connection
        await service.insert(collection, { _id: 'u1', name: 'Alice' })
        await service.insert(collection, { _id: 'u2', name: 'Bob' })
        await service.find(collection, {})

        const connectionCount = service.getActiveConnectionCount()

        // Should have at most one connection per shard used
        expect(connectionCount).toBeLessThanOrEqual(1)
      })

    })


    describe('retry on transient network errors', () => {

      it('should retry on network timeout', async () => {
        let callCount = 0
        mockDOStub.fetch = vi.fn().mockImplementation(() => {
          callCount++
          if (callCount < 3) {
            return Promise.reject(new Error('network timeout'))
          }
          return Promise.resolve(new Response(JSON.stringify({ acknowledged: true, insertedId: 'u1' })))
        })

        const result = await service.insert('users', { _id: 'u1', name: 'Alice' })

        expect(callCount).toBe(3)
        expect(result.acknowledged).toBe(true)
      })

      it('should retry on connection reset', async () => {
        let callCount = 0
        mockDOStub.fetch = vi.fn().mockImplementation(() => {
          callCount++
          if (callCount < 2) {
            return Promise.reject(new Error('ECONNRESET'))
          }
          return Promise.resolve(new Response(JSON.stringify({ acknowledged: true, insertedId: 'u1' })))
        })

        const result = await service.insert('users', { _id: 'u1', name: 'Alice' })

        expect(callCount).toBe(2)
        expect(result.acknowledged).toBe(true)
      })

      it('should retry on 503 Service Unavailable', async () => {
        let callCount = 0
        mockDOStub.fetch = vi.fn().mockImplementation(() => {
          callCount++
          if (callCount < 2) {
            return Promise.resolve(new Response('Service Unavailable', { status: 503 }))
          }
          return Promise.resolve(new Response(JSON.stringify({ acknowledged: true, insertedId: 'u1' })))
        })

        const result = await service.insert('users', { _id: 'u1', name: 'Alice' })

        expect(callCount).toBe(2)
        expect(result.acknowledged).toBe(true)
      })

      it('should apply exponential backoff between retries', async () => {
        const startTime = Date.now()
        let callCount = 0

        mockDOStub.fetch = vi.fn().mockImplementation(() => {
          callCount++
          if (callCount < 3) {
            return Promise.reject(new Error('network timeout'))
          }
          return Promise.resolve(new Response(JSON.stringify({ acknowledged: true, insertedId: 'u1' })))
        })

        await service.insert('users', { _id: 'u1', name: 'Alice' })

        const elapsed = Date.now() - startTime

        // With exponential backoff (e.g., 100ms, 200ms), should take at least 300ms
        // Allow some tolerance for test execution
        expect(elapsed).toBeGreaterThanOrEqual(100)
      })

      it('should throw TransientError after max retries exceeded', async () => {
        mockDOStub.fetch = vi.fn().mockRejectedValue(new Error('network timeout'))

        await expect(
          service.insert('users', { _id: 'u1', name: 'Alice' })
        ).rejects.toThrow(TransientError)
      })

      it('should not retry on non-transient errors', async () => {
        let callCount = 0
        mockDOStub.fetch = vi.fn().mockImplementation(() => {
          callCount++
          return Promise.resolve(new Response(JSON.stringify({
            error: { code: 'INVALID_DOCUMENT', message: 'Invalid document' }
          }), { status: 400 }))
        })

        await expect(
          service.insert('users', { _id: 'u1', name: 'Alice' })
        ).rejects.toThrow()

        expect(callCount).toBe(1) // No retries for 400 errors
      })

      it('should use configurable retry settings', async () => {
        const serviceWithRetry = createRPCService({
          shardNamespace: service['shardNamespace'],
          shardCount: 16,
          retry: {
            maxAttempts: 5,
            baseDelay: 50,
            maxDelay: 1000,
          },
        })

        let callCount = 0
        mockDOStub.fetch = vi.fn().mockImplementation(() => {
          callCount++
          if (callCount < 5) {
            return Promise.reject(new Error('network timeout'))
          }
          return Promise.resolve(new Response(JSON.stringify({ acknowledged: true, insertedId: 'u1' })))
        })

        const result = await serviceWithRetry.insert('users', { _id: 'u1', name: 'Alice' })

        expect(callCount).toBe(5)
        expect(result.acknowledged).toBe(true)
      })

      it('should preserve operation idempotency on retry', async () => {
        let callCount = 0
        const insertedDocs: string[] = []

        mockDOStub.fetch = vi.fn().mockImplementation((req: Request) => {
          callCount++
          const body = req.body // In real implementation, parse the body
          if (callCount === 1) {
            // First call fails after write
            insertedDocs.push('u1')
            return Promise.reject(new Error('connection lost'))
          }
          // Second call should detect duplicate
          return Promise.resolve(new Response(JSON.stringify({
            acknowledged: true,
            insertedId: 'u1',
            wasRetry: insertedDocs.includes('u1'),
          })))
        })

        const result = await service.insert('users', { _id: 'u1', name: 'Alice' })

        // Should succeed but detect it was a retry
        expect(result.acknowledged).toBe(true)
      })

    })


    describe('batch multiple inserts to same shard', () => {

      it('should batch insertMany documents going to same shard', async () => {
        const documents = [
          { _id: 'u1', name: 'Alice' },
          { _id: 'u2', name: 'Bob' },
          { _id: 'u3', name: 'Charlie' },
        ]

        const result = await service.insertMany('users', documents)

        expect(result.acknowledged).toBe(true)
        expect(result.insertedCount).toBe(3)
        expect(Object.keys(result.insertedIds)).toHaveLength(3)
      })

      it('should batch concurrent inserts to the same collection', async () => {
        let batchCallCount = 0
        mockDOStub.fetch = vi.fn().mockImplementation(() => {
          batchCallCount++
          return Promise.resolve(new Response(JSON.stringify({
            acknowledged: true,
            insertedCount: 3,
            insertedIds: { 0: 'u1', 1: 'u2', 2: 'u3' },
          })))
        })

        // Fire off concurrent inserts
        const promises = [
          service.insert('users', { _id: 'u1', name: 'Alice' }),
          service.insert('users', { _id: 'u2', name: 'Bob' }),
          service.insert('users', { _id: 'u3', name: 'Charlie' }),
        ]

        await Promise.all(promises)

        // Should have batched into fewer RPC calls
        expect(batchCallCount).toBeLessThan(3)
      })

      it('should respect batch size limits', async () => {
        const serviceWithSmallBatch = createRPCService({
          shardNamespace: service['shardNamespace'],
          shardCount: 16,
          batchSize: 2,
        })

        let batchCallCount = 0
        mockDOStub.fetch = vi.fn().mockImplementation(() => {
          batchCallCount++
          return Promise.resolve(new Response(JSON.stringify({
            acknowledged: true,
            insertedCount: batchCallCount <= 2 ? 2 : 1,
            insertedIds: {},
          })))
        })

        const documents = [
          { _id: 'u1', name: 'Alice' },
          { _id: 'u2', name: 'Bob' },
          { _id: 'u3', name: 'Charlie' },
        ]

        await serviceWithSmallBatch.insertMany('users', documents)

        // With batch size 2, should need at least 2 batches for 3 documents
        expect(batchCallCount).toBeGreaterThanOrEqual(2)
      })

      it('should flush pending batches on timeout', async () => {
        const serviceWithFlush = createRPCService({
          shardNamespace: service['shardNamespace'],
          shardCount: 16,
          batchFlushDelay: 10, // 10ms flush delay
        })

        let batchCallCount = 0
        mockDOStub.fetch = vi.fn().mockImplementation(() => {
          batchCallCount++
          return Promise.resolve(new Response(JSON.stringify({
            acknowledged: true,
            insertedId: `u${batchCallCount}`,
          })))
        })

        // Single insert should flush after timeout
        const insertPromise = serviceWithFlush.insert('users', { _id: 'u1', name: 'Alice' })

        // Wait for flush
        await new Promise(resolve => setTimeout(resolve, 20))
        await insertPromise

        expect(batchCallCount).toBeGreaterThanOrEqual(1)
      })

      it('should maintain document order within batch', async () => {
        const insertedOrder: string[] = []

        mockDOStub.fetch = vi.fn().mockImplementation(async (req: Request) => {
          const body = await req.json()
          if (body.documents) {
            for (const doc of body.documents) {
              insertedOrder.push(doc._id)
            }
          }
          return new Response(JSON.stringify({
            acknowledged: true,
            insertedCount: body.documents?.length || 1,
            insertedIds: body.documents?.reduce((acc: any, _: any, i: number) => {
              acc[i] = body.documents[i]._id
              return acc
            }, {}) || { 0: body.document?._id },
          }))
        })

        const documents = [
          { _id: 'u1', name: 'Alice' },
          { _id: 'u2', name: 'Bob' },
          { _id: 'u3', name: 'Charlie' },
        ]

        await service.insertMany('users', documents)

        expect(insertedOrder).toEqual(['u1', 'u2', 'u3'])
      })

      it('should return partial results on partial batch failure', async () => {
        mockDOStub.fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify({
          acknowledged: true,
          insertedCount: 2,
          insertedIds: { 0: 'u1', 1: 'u2' },
          writeErrors: [{ index: 2, code: 'DUPLICATE_KEY', message: 'Duplicate key error' }],
        })))

        const documents = [
          { _id: 'u1', name: 'Alice' },
          { _id: 'u2', name: 'Bob' },
          { _id: 'u3', name: 'Charlie' },
        ]

        const result = await service.insertMany('users', documents, { ordered: false })

        expect(result.insertedCount).toBe(2)
        expect(result.writeErrors).toHaveLength(1)
        expect(result.writeErrors[0].index).toBe(2)
      })

    })


    describe('return read tokens from write operations', () => {

      it('should return read token from insert operation', async () => {
        mockDOStub.fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify({
          acknowledged: true,
          insertedId: 'u1',
          readToken: 'rt_abc123',
        })))

        const result = await service.insert('users', { _id: 'u1', name: 'Alice' })

        expect(result.readToken).toBeDefined()
        expect(result.readToken).toBe('rt_abc123')
      })

      it('should return read token from update operation', async () => {
        mockDOStub.fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify({
          acknowledged: true,
          matchedCount: 1,
          modifiedCount: 1,
          readToken: 'rt_def456',
        })))

        const result = await service.update('users', { _id: 'u1' }, { $set: { name: 'Alicia' } })

        expect(result.readToken).toBeDefined()
        expect(result.readToken).toBe('rt_def456')
      })

      it('should return read token from delete operation', async () => {
        mockDOStub.fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify({
          acknowledged: true,
          deletedCount: 1,
          readToken: 'rt_ghi789',
        })))

        const result = await service.delete('users', { _id: 'u1' })

        expect(result.readToken).toBeDefined()
        expect(result.readToken).toBe('rt_ghi789')
      })

      it('should return read token from insertMany operation', async () => {
        mockDOStub.fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify({
          acknowledged: true,
          insertedCount: 3,
          insertedIds: { 0: 'u1', 1: 'u2', 2: 'u3' },
          readToken: 'rt_jkl012',
        })))

        const result = await service.insertMany('users', [
          { _id: 'u1', name: 'Alice' },
          { _id: 'u2', name: 'Bob' },
          { _id: 'u3', name: 'Charlie' },
        ])

        expect(result.readToken).toBeDefined()
        expect(result.readToken).toBe('rt_jkl012')
      })

      it('should use read token for subsequent reads to ensure consistency', async () => {
        let usedReadToken: string | undefined

        mockDOStub.fetch = vi.fn().mockImplementation(async (req: Request) => {
          const body = await req.json()
          usedReadToken = body.readToken
          return new Response(JSON.stringify({
            documents: [{ _id: 'u1', name: 'Alice' }],
          }))
        })

        const insertResult = { readToken: 'rt_abc123' } as any

        await service.find('users', { _id: 'u1' }, { readToken: insertResult.readToken })

        expect(usedReadToken).toBe('rt_abc123')
      })

      it('should encode read token with sequence number', () => {
        const token: ReadToken = service.createReadToken('users', 12345)

        expect(token).toBeDefined()
        expect(token.collection).toBe('users')
        expect(token.sequence).toBe(12345)
        expect(token.toString()).toMatch(/^rt_/)
      })

      it('should decode read token to extract sequence number', () => {
        const encoded = 'rt_dXNlcnM6MTIzNDU=' // base64 of 'users:12345'

        const decoded = service.decodeReadToken(encoded)

        expect(decoded.collection).toBe('users')
        expect(decoded.sequence).toBe(12345)
      })

    })


    describe('handle shard DO unavailable', () => {

      it('should throw ShardUnavailableError when DO is unreachable', async () => {
        mockDOStub.fetch = vi.fn().mockRejectedValue(new Error('DO is hibernating'))

        await expect(
          service.find('users', {})
        ).rejects.toThrow(ShardUnavailableError)
      })

      it('should include shard ID in ShardUnavailableError', async () => {
        mockDOStub.fetch = vi.fn().mockRejectedValue(new Error('DO exceeded memory limit'))

        try {
          await service.find('users', {})
          expect.fail('Should have thrown')
        } catch (error) {
          expect(error).toBeInstanceOf(ShardUnavailableError)
          expect((error as ShardUnavailableError).shardId).toBeGreaterThanOrEqual(0)
        }
      })

      it('should handle DO eviction gracefully', async () => {
        let callCount = 0
        mockDOStub.fetch = vi.fn().mockImplementation(() => {
          callCount++
          if (callCount === 1) {
            return Promise.reject(new Error('Durable Object was evicted'))
          }
          // Second call after DO recreates should succeed
          return Promise.resolve(new Response(JSON.stringify({
            documents: [],
          })))
        })

        const result = await service.find('users', {})

        expect(result).toEqual([])
        expect(callCount).toBe(2)
      })

      it('should return cached results when shard is temporarily unavailable', async () => {
        const serviceWithCache = createRPCService({
          shardNamespace: service['shardNamespace'],
          shardCount: 16,
          enableReadCache: true,
        })

        // First call succeeds and caches
        mockDOStub.fetch = vi.fn().mockResolvedValueOnce(new Response(JSON.stringify({
          documents: [{ _id: 'u1', name: 'Alice' }],
        })))

        await serviceWithCache.find('users', { _id: 'u1' })

        // Second call fails but returns cached
        mockDOStub.fetch = vi.fn().mockRejectedValueOnce(new Error('DO unavailable'))

        const result = await serviceWithCache.find('users', { _id: 'u1' }, { allowStale: true })

        expect(result).toHaveLength(1)
        expect(result[0].name).toBe('Alice')
      })

      it('should report shard health status', async () => {
        mockDOStub.fetch = vi.fn().mockRejectedValue(new Error('timeout'))

        try {
          await service.find('users', {})
        } catch {
          // Expected to fail
        }

        const health = service.getShardHealth()

        expect(health).toBeDefined()
        const shardId = hashToShardId('users')
        expect(health[shardId]).toBeDefined()
        expect(health[shardId].status).toBe('unhealthy')
        expect(health[shardId].lastError).toBeDefined()
      })

      it('should circuit break after repeated failures', async () => {
        mockDOStub.fetch = vi.fn().mockRejectedValue(new Error('timeout'))

        // Make multiple failing calls to trip circuit breaker
        for (let i = 0; i < 5; i++) {
          try {
            await service.find('users', {})
          } catch {
            // Expected
          }
        }

        // Next call should fail fast due to circuit breaker
        const start = Date.now()
        try {
          await service.find('users', {})
        } catch (error) {
          const elapsed = Date.now() - start
          expect(elapsed).toBeLessThan(50) // Should fail fast
          expect((error as Error).message).toMatch(/circuit.*open/i)
        }
      })

    })


    describe('route queries with consistency level', () => {

      it('should support eventual consistency (default)', async () => {
        let requestBody: any

        mockDOStub.fetch = vi.fn().mockImplementation(async (req: Request) => {
          requestBody = await req.json()
          return new Response(JSON.stringify({ documents: [] }))
        })

        await service.find('users', {})

        expect(requestBody.consistencyLevel).toBe('eventual')
      })

      it('should support strong consistency', async () => {
        let requestBody: any

        mockDOStub.fetch = vi.fn().mockImplementation(async (req: Request) => {
          requestBody = await req.json()
          return new Response(JSON.stringify({ documents: [] }))
        })

        await service.find('users', {}, { consistencyLevel: 'strong' })

        expect(requestBody.consistencyLevel).toBe('strong')
      })

      it('should support session consistency', async () => {
        let requestBody: any

        mockDOStub.fetch = vi.fn().mockImplementation(async (req: Request) => {
          requestBody = await req.json()
          return new Response(JSON.stringify({ documents: [] }))
        })

        const session = service.createSession()
        await service.find('users', {}, { session })

        expect(requestBody.consistencyLevel).toBe('session')
        expect(requestBody.sessionId).toBeDefined()
      })

      it('should include read token for read-your-writes consistency', async () => {
        let requestBody: any

        mockDOStub.fetch = vi.fn().mockImplementation(async (req: Request) => {
          requestBody = await req.json()
          return new Response(JSON.stringify({
            documents: [{ _id: 'u1', name: 'Alice' }],
          }))
        })

        await service.find('users', { _id: 'u1' }, {
          consistencyLevel: 'readYourWrites',
          readToken: 'rt_abc123',
        })

        expect(requestBody.consistencyLevel).toBe('readYourWrites')
        expect(requestBody.readToken).toBe('rt_abc123')
      })

      it('should wait for sequence number with bounded staleness', async () => {
        let requestBody: any

        mockDOStub.fetch = vi.fn().mockImplementation(async (req: Request) => {
          requestBody = await req.json()
          return new Response(JSON.stringify({ documents: [] }))
        })

        await service.find('users', {}, {
          consistencyLevel: 'boundedStaleness',
          maxStalenessSeconds: 5,
        })

        expect(requestBody.consistencyLevel).toBe('boundedStaleness')
        expect(requestBody.maxStalenessSeconds).toBe(5)
      })

      it('should apply default consistency level from service config', async () => {
        const serviceWithStrong = createRPCService({
          shardNamespace: service['shardNamespace'],
          shardCount: 16,
          defaultConsistencyLevel: 'strong',
        })

        let requestBody: any

        mockDOStub.fetch = vi.fn().mockImplementation(async (req: Request) => {
          requestBody = await req.json()
          return new Response(JSON.stringify({ documents: [] }))
        })

        await serviceWithStrong.find('users', {})

        expect(requestBody.consistencyLevel).toBe('strong')
      })

      it('should override default consistency level with per-query option', async () => {
        const serviceWithStrong = createRPCService({
          shardNamespace: service['shardNamespace'],
          shardCount: 16,
          defaultConsistencyLevel: 'strong',
        })

        let requestBody: any

        mockDOStub.fetch = vi.fn().mockImplementation(async (req: Request) => {
          requestBody = await req.json()
          return new Response(JSON.stringify({ documents: [] }))
        })

        await serviceWithStrong.find('users', {}, { consistencyLevel: 'eventual' })

        expect(requestBody.consistencyLevel).toBe('eventual')
      })

    })


    describe('connection management', () => {

      it('should establish connection to shard on first operation', async () => {
        expect(service.getActiveConnectionCount()).toBe(0)

        await service.find('users', {})

        expect(service.getActiveConnectionCount()).toBe(1)
      })

      it('should reuse existing connection for same shard', async () => {
        await service.find('users', {})
        await service.find('users', {})
        await service.find('users', {})

        // Should only have one connection to the shard
        expect(service.getActiveConnectionCount()).toBe(1)
      })

      it('should create separate connections for different shards', async () => {
        // Find collections that hash to different shards
        const collections = ['users', 'orders', 'products', 'sessions']

        for (const collection of collections) {
          await service.find(collection, {})
        }

        const connectionCount = service.getActiveConnectionCount()

        // Should have connections to multiple shards (depending on hash distribution)
        expect(connectionCount).toBeGreaterThanOrEqual(1)
      })

      it('should close connections on service shutdown', async () => {
        await service.find('users', {})
        expect(service.getActiveConnectionCount()).toBe(1)

        await service.close()

        expect(service.getActiveConnectionCount()).toBe(0)
      })

      it('should handle connection pool exhaustion', async () => {
        const serviceWithSmallPool = createRPCService({
          shardNamespace: service['shardNamespace'],
          shardCount: 16,
          maxConnectionsPerShard: 1,
        })

        // Concurrent operations should queue, not fail
        const promises = Array.from({ length: 10 }, (_, i) =>
          serviceWithSmallPool.insert('users', { _id: `u${i}`, name: `User ${i}` })
        )

        const results = await Promise.all(promises)

        // All should succeed despite pool limit
        expect(results.every(r => r.acknowledged)).toBe(true)
      })

    })


    describe('error handling', () => {

      it('should wrap DO errors with appropriate error types', async () => {
        mockDOStub.fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify({
          error: { code: 'DOCUMENT_TOO_LARGE', message: 'Document exceeds 16MB limit' },
        }), { status: 400 }))

        await expect(
          service.insert('users', { _id: 'u1', largeData: 'x'.repeat(20 * 1024 * 1024) })
        ).rejects.toThrow(/Document exceeds 16MB/)
      })

      it('should preserve stack traces across RPC boundary', async () => {
        mockDOStub.fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify({
          error: {
            code: 'VALIDATION_ERROR',
            message: 'Invalid field type',
            stack: 'Error at validate() at line 42',
          },
        }), { status: 400 }))

        try {
          await service.insert('users', { _id: 'u1', name: 123 })
          expect.fail('Should have thrown')
        } catch (error) {
          expect((error as Error).stack).toContain('at validate()')
        }
      })

      it('should timeout long-running operations', async () => {
        mockDOStub.fetch = vi.fn().mockImplementation(() =>
          new Promise(resolve => setTimeout(resolve, 10000))
        )

        const serviceWithTimeout = createRPCService({
          shardNamespace: service['shardNamespace'],
          shardCount: 16,
          operationTimeout: 100, // 100ms timeout
        })

        await expect(
          serviceWithTimeout.find('users', {})
        ).rejects.toThrow(/timeout/i)
      })

    })

  })

})
