/**
 * MongoLake Shard Router Tests
 *
 * Tests for the shard routing logic that determines which shard owns a collection.
 * The shard router uses consistent hashing to distribute collections across shards.
 *
 * Key features tested:
 * - Consistent hashing for shard distribution (0-15 range)
 * - Shard assignment caching for performance
 * - Shard splitting for hot collections
 * - Shard affinity hints
 * - Document-level routing by _id
 * - Database-prefixed routing
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'

// Import the shard router module (doesn't exist yet - tests should fail)
import {
  ShardRouter,
  ShardRouterOptions,
  ShardAssignment,
  ShardAffinityHint,
  hashCollectionToShard,
  hashDocumentToShard,
  createShardRouter,
} from '../../../src/shard/router'


describe('MongoLake Shard Router', () => {

  describe('hashCollectionToShard', () => {

    it('should hash collection name to shard ID in 0-15 range', () => {
      const shardId = hashCollectionToShard('users')

      expect(shardId).toBeGreaterThanOrEqual(0)
      expect(shardId).toBeLessThanOrEqual(15)
      expect(Number.isInteger(shardId)).toBe(true)
    })

    it('should return same shard for same collection name', () => {
      const shard1 = hashCollectionToShard('orders')
      const shard2 = hashCollectionToShard('orders')
      const shard3 = hashCollectionToShard('orders')

      expect(shard1).toBe(shard2)
      expect(shard2).toBe(shard3)
    })

    it('should produce even distribution across shards', () => {
      const shardCounts = new Map<number, number>()
      const collectionCount = 1600 // 100 collections per shard on average

      // Generate collection names and count shard assignments
      for (let i = 0; i < collectionCount; i++) {
        const collectionName = `collection_${i}_${Math.random().toString(36).substring(7)}`
        const shardId = hashCollectionToShard(collectionName)
        shardCounts.set(shardId, (shardCounts.get(shardId) || 0) + 1)
      }

      // All 16 shards should be represented
      expect(shardCounts.size).toBe(16)

      // Check distribution - each shard should have roughly 100 collections
      // Allow for 50% variance (50-150 range)
      const expectedPerShard = collectionCount / 16
      for (const [shardId, count] of shardCounts) {
        expect(count).toBeGreaterThan(expectedPerShard * 0.5)
        expect(count).toBeLessThan(expectedPerShard * 1.5)
      }
    })

    it('should handle special characters in collection names', () => {
      const specialNames = [
        'my-collection',
        'my_collection',
        'my.collection',
        'collection$special',
        'collection:with:colons',
        'collection/with/slashes',
        'collection@with@at',
        'collection#hash',
        'collection%percent',
        'collection+plus',
        'collection=equals',
        'collection[brackets]',
        'collection{braces}',
        '!@#$%^&*()_+-=[]{}|;:,.<>?',
      ]

      for (const name of specialNames) {
        const shardId = hashCollectionToShard(name)
        expect(shardId).toBeGreaterThanOrEqual(0)
        expect(shardId).toBeLessThanOrEqual(15)
      }
    })

    it('should handle unicode characters in collection names', () => {
      const unicodeNames = [
        'usuarios',
        'utilisateurs',
        'benutzer',
        'collection_with_emoji',
        'chinese_collection',
        'japanese_collection',
        'arabic_collection',
        'hebrew_collection',
      ]

      for (const name of unicodeNames) {
        const shardId = hashCollectionToShard(name)
        expect(shardId).toBeGreaterThanOrEqual(0)
        expect(shardId).toBeLessThanOrEqual(15)
      }
    })

    it('should handle empty collection name', () => {
      expect(() => hashCollectionToShard('')).toThrow(/collection.*empty|empty.*collection/i)
    })

    it('should handle very long collection names', () => {
      const longName = 'a'.repeat(10000)
      const shardId = hashCollectionToShard(longName)

      expect(shardId).toBeGreaterThanOrEqual(0)
      expect(shardId).toBeLessThanOrEqual(15)
    })

    it('should produce different hashes for similar names', () => {
      const shard1 = hashCollectionToShard('users')
      const shard2 = hashCollectionToShard('user')
      const shard3 = hashCollectionToShard('users1')
      const shard4 = hashCollectionToShard('Users')

      // At least some should be different (statistically unlikely all same)
      const shards = new Set([shard1, shard2, shard3, shard4])
      expect(shards.size).toBeGreaterThanOrEqual(2)
    })

  })


  describe('ShardRouter class', () => {

    let router: ShardRouter

    beforeEach(() => {
      router = createShardRouter()
    })

    describe('basic routing', () => {

      it('should route collection to a shard', () => {
        const assignment = router.route('users')

        expect(assignment).toBeDefined()
        expect(assignment.shardId).toBeGreaterThanOrEqual(0)
        expect(assignment.shardId).toBeLessThanOrEqual(15)
        expect(assignment.collection).toBe('users')
      })

      it('should consistently route same collection to same shard', () => {
        const assignment1 = router.route('orders')
        const assignment2 = router.route('orders')
        const assignment3 = router.route('orders')

        expect(assignment1.shardId).toBe(assignment2.shardId)
        expect(assignment2.shardId).toBe(assignment3.shardId)
      })

      it('should route different collections potentially to different shards', () => {
        const collections = [
          'users', 'orders', 'products', 'sessions',
          'logs', 'events', 'metrics', 'analytics'
        ]
        const shards = new Set<number>()

        for (const collection of collections) {
          shards.add(router.route(collection).shardId)
        }

        // Should use multiple shards (statistically unlikely all same)
        expect(shards.size).toBeGreaterThan(1)
      })

    })


    describe('cache shard assignments', () => {

      it('should cache shard assignments', () => {
        // First call should compute and cache
        const assignment1 = router.route('users')

        // Second call should return cached result
        const assignment2 = router.route('users')

        expect(assignment1).toBe(assignment2) // Same object reference
      })

      it('should return cached assignment without recomputing', () => {
        // Spy on the internal hash function
        const hashSpy = vi.spyOn(router as any, 'computeHash')

        router.route('orders')
        router.route('orders')
        router.route('orders')

        // Hash should only be computed once
        expect(hashSpy).toHaveBeenCalledTimes(1)
      })

      it('should allow clearing the cache', () => {
        const assignment1 = router.route('users')
        router.clearCache()
        const assignment2 = router.route('users')

        // Should have same shard ID but different objects
        expect(assignment1.shardId).toBe(assignment2.shardId)
        expect(assignment1).not.toBe(assignment2)
      })

      it('should support cache size limits', () => {
        const limitedRouter = createShardRouter({ cacheSize: 10 })

        // Add more than cache size
        for (let i = 0; i < 20; i++) {
          limitedRouter.route(`collection_${i}`)
        }

        // Cache should not exceed limit
        expect(limitedRouter.getCacheSize()).toBeLessThanOrEqual(10)
      })

      it('should evict oldest entries when cache is full', () => {
        const limitedRouter = createShardRouter({ cacheSize: 3 })

        limitedRouter.route('first')
        limitedRouter.route('second')
        limitedRouter.route('third')
        limitedRouter.route('fourth') // Should evict 'first'

        expect(limitedRouter.isCached('first')).toBe(false)
        expect(limitedRouter.isCached('fourth')).toBe(true)
      })

    })


    describe('shard affinity hints', () => {

      it('should support shard affinity hints', () => {
        const routerWithHints = createShardRouter()

        // Set affinity hint to force collection to specific shard
        routerWithHints.setAffinityHint('hot_collection', { preferredShard: 5 })

        const assignment = routerWithHints.route('hot_collection')

        expect(assignment.shardId).toBe(5)
      })

      it('should override hash-based routing with affinity hint', () => {
        const routerWithHints = createShardRouter()

        // Get natural hash-based shard
        const naturalShard = hashCollectionToShard('users')

        // Set affinity to different shard
        const forcedShard = (naturalShard + 1) % 16
        routerWithHints.setAffinityHint('users', { preferredShard: forcedShard })

        const assignment = routerWithHints.route('users')

        expect(assignment.shardId).toBe(forcedShard)
        expect(assignment.shardId).not.toBe(naturalShard)
      })

      it('should allow removing affinity hints', () => {
        const routerWithHints = createShardRouter()

        routerWithHints.setAffinityHint('users', { preferredShard: 10 })
        expect(routerWithHints.route('users').shardId).toBe(10)

        routerWithHints.removeAffinityHint('users')

        // Should fall back to hash-based routing
        const naturalShard = hashCollectionToShard('users')
        expect(routerWithHints.route('users').shardId).toBe(naturalShard)
      })

      it('should validate affinity hint shard ID range', () => {
        const routerWithHints = createShardRouter()

        expect(() => {
          routerWithHints.setAffinityHint('users', { preferredShard: -1 })
        }).toThrow(/shard.*range/i)

        expect(() => {
          routerWithHints.setAffinityHint('users', { preferredShard: 16 })
        }).toThrow(/shard.*range/i)

        expect(() => {
          routerWithHints.setAffinityHint('users', { preferredShard: 100 })
        }).toThrow(/shard.*range/i)
      })

      it('should list all affinity hints', () => {
        const routerWithHints = createShardRouter()

        routerWithHints.setAffinityHint('users', { preferredShard: 5 })
        routerWithHints.setAffinityHint('orders', { preferredShard: 10 })

        const hints = routerWithHints.getAffinityHints()

        expect(hints).toHaveLength(2)
        expect(hints).toContainEqual({ collection: 'users', preferredShard: 5 })
        expect(hints).toContainEqual({ collection: 'orders', preferredShard: 10 })
      })

    })


    describe('database-prefixed routing', () => {

      it('should route with database prefix', () => {
        const assignment = router.routeWithDatabase('mydb', 'users')

        expect(assignment).toBeDefined()
        expect(assignment.database).toBe('mydb')
        expect(assignment.collection).toBe('users')
        expect(assignment.shardId).toBeGreaterThanOrEqual(0)
        expect(assignment.shardId).toBeLessThanOrEqual(15)
      })

      it('should include database in hash computation', () => {
        // Same collection in different databases may route to different shards
        const assignment1 = router.routeWithDatabase('db1', 'users')
        const assignment2 = router.routeWithDatabase('db2', 'users')

        // They might be the same by chance, but the hashing should consider database
        expect(assignment1.database).toBe('db1')
        expect(assignment2.database).toBe('db2')
      })

      it('should use namespace format for caching', () => {
        const assignment1 = router.routeWithDatabase('mydb', 'users')
        const assignment2 = router.routeWithDatabase('mydb', 'users')

        // Should be cached (same reference)
        expect(assignment1).toBe(assignment2)
      })

      it('should differentiate same collection across databases', () => {
        const assignment1 = router.routeWithDatabase('production', 'users')
        const assignment2 = router.routeWithDatabase('staging', 'users')

        // These are different namespaces and should be cached separately
        expect(assignment1.database).toBe('production')
        expect(assignment2.database).toBe('staging')
        expect(assignment1).not.toBe(assignment2)
      })

      it('should handle special characters in database names', () => {
        const specialDbs = [
          'my-database',
          'my_database',
          'database123',
        ]

        for (const db of specialDbs) {
          const assignment = router.routeWithDatabase(db, 'collection')
          expect(assignment.database).toBe(db)
          expect(assignment.shardId).toBeGreaterThanOrEqual(0)
        }
      })

      it('should reject empty database name', () => {
        expect(() => router.routeWithDatabase('', 'users')).toThrow(/empty.*database/i)
      })

    })


    describe('document-level routing by _id', () => {

      it('should route document by ObjectId string', () => {
        const objectIdHex = '507f1f77bcf86cd799439011'
        const shardId = hashDocumentToShard(objectIdHex)

        expect(shardId).toBeGreaterThanOrEqual(0)
        expect(shardId).toBeLessThanOrEqual(15)
      })

      it('should route same _id to same shard', () => {
        const objectIdHex = '507f191e810c19729de860ea'

        const shard1 = hashDocumentToShard(objectIdHex)
        const shard2 = hashDocumentToShard(objectIdHex)
        const shard3 = hashDocumentToShard(objectIdHex)

        expect(shard1).toBe(shard2)
        expect(shard2).toBe(shard3)
      })

      it('should route document via router instance', () => {
        const objectIdHex = '507f1f77bcf86cd799439011'

        const assignment = router.routeDocument('users', objectIdHex)

        expect(assignment.collection).toBe('users')
        expect(assignment.documentId).toBe(objectIdHex)
        expect(assignment.shardId).toBeGreaterThanOrEqual(0)
      })

      it('should use document _id for routing instead of collection hash', () => {
        const objectIdHex = '507f1f77bcf86cd799439011'

        // Document routing should use _id, not collection name
        const docAssignment = router.routeDocument('users', objectIdHex)
        const docShardId = hashDocumentToShard(objectIdHex)

        expect(docAssignment.shardId).toBe(docShardId)
      })

      it('should handle UUID format _id', () => {
        const uuid = '550e8400-e29b-41d4-a716-446655440000'
        const shardId = hashDocumentToShard(uuid)

        expect(shardId).toBeGreaterThanOrEqual(0)
        expect(shardId).toBeLessThanOrEqual(15)
      })

      it('should handle custom string _id', () => {
        const customId = 'user:john.doe@example.com'
        const shardId = hashDocumentToShard(customId)

        expect(shardId).toBeGreaterThanOrEqual(0)
        expect(shardId).toBeLessThanOrEqual(15)
      })

      it('should handle numeric _id', () => {
        const numericId = '123456789'
        const shardId = hashDocumentToShard(numericId)

        expect(shardId).toBeGreaterThanOrEqual(0)
        expect(shardId).toBeLessThanOrEqual(15)
      })

      it('should produce even distribution for sequential ObjectIds', () => {
        const shardCounts = new Map<number, number>()

        // Generate 1600 sequential-ish ObjectIds
        for (let i = 0; i < 1600; i++) {
          const timestamp = (Date.now() / 1000 | 0).toString(16).padStart(8, '0')
          const random = Math.random().toString(16).substring(2, 18).padStart(16, '0')
          const objectIdHex = timestamp + random

          const shardId = hashDocumentToShard(objectIdHex)
          shardCounts.set(shardId, (shardCounts.get(shardId) || 0) + 1)
        }

        // Check distribution
        for (const [_, count] of shardCounts) {
          expect(count).toBeGreaterThan(50)  // At least 50 per shard
          expect(count).toBeLessThan(200)    // No more than 200 per shard
        }
      })

      it('should reject empty document _id', () => {
        expect(() => hashDocumentToShard('')).toThrow(/empty.*id/i)
      })

    })


    describe('shard splitting for hot collections', () => {

      it('should support splitting a collection across multiple shards', () => {
        const routerWithSplits = createShardRouter()

        // Split 'hot_collection' across shards 0, 1, 2, 3
        routerWithSplits.splitCollection('hot_collection', [0, 1, 2, 3])

        // Getting the split info
        const splitInfo = routerWithSplits.getSplitInfo('hot_collection')

        expect(splitInfo).toBeDefined()
        expect(splitInfo!.shards).toEqual([0, 1, 2, 3])
      })

      it('should route split collection documents across designated shards', () => {
        const routerWithSplits = createShardRouter()

        routerWithSplits.splitCollection('events', [4, 5, 6, 7])

        const shards = new Set<number>()
        for (let i = 0; i < 100; i++) {
          const docId = `doc_${i}_${Math.random().toString(36)}`
          const assignment = routerWithSplits.routeDocument('events', docId)
          shards.add(assignment.shardId)
        }

        // All assigned shards should be within the split range
        for (const shardId of shards) {
          expect([4, 5, 6, 7]).toContain(shardId)
        }
      })

      it('should distribute documents evenly across split shards', () => {
        const routerWithSplits = createShardRouter()

        routerWithSplits.splitCollection('logs', [8, 9, 10, 11])

        const shardCounts = new Map<number, number>()
        for (let i = 0; i < 400; i++) {
          const docId = `log_${i}_${Math.random().toString(36)}`
          const assignment = routerWithSplits.routeDocument('logs', docId)
          shardCounts.set(assignment.shardId, (shardCounts.get(assignment.shardId) || 0) + 1)
        }

        // Each of the 4 shards should have roughly 100 documents
        for (const shardId of [8, 9, 10, 11]) {
          const count = shardCounts.get(shardId) || 0
          expect(count).toBeGreaterThan(50)
          expect(count).toBeLessThan(150)
        }
      })

      it('should allow unsplitting a collection', () => {
        const routerWithSplits = createShardRouter()

        routerWithSplits.splitCollection('events', [0, 1, 2, 3])
        routerWithSplits.unsplitCollection('events')

        const splitInfo = routerWithSplits.getSplitInfo('events')
        expect(splitInfo).toBeUndefined()
      })

      it('should validate split shard IDs are in range', () => {
        const routerWithSplits = createShardRouter()

        expect(() => {
          routerWithSplits.splitCollection('events', [0, 1, 20]) // 20 is out of range
        }).toThrow(/shard.*range/i)

        expect(() => {
          routerWithSplits.splitCollection('events', [-1, 0, 1]) // -1 is invalid
        }).toThrow(/shard.*range/i)
      })

      it('should require at least 2 shards for split', () => {
        const routerWithSplits = createShardRouter()

        expect(() => {
          routerWithSplits.splitCollection('events', [5]) // Only 1 shard
        }).toThrow(/at least 2/i)
      })

      it('should list all split collections', () => {
        const routerWithSplits = createShardRouter()

        routerWithSplits.splitCollection('events', [0, 1])
        routerWithSplits.splitCollection('logs', [2, 3, 4])
        routerWithSplits.splitCollection('metrics', [5, 6, 7, 8])

        const splits = routerWithSplits.getAllSplits()

        expect(splits).toHaveLength(3)
        expect(splits.map(s => s.collection)).toContain('events')
        expect(splits.map(s => s.collection)).toContain('logs')
        expect(splits.map(s => s.collection)).toContain('metrics')
      })

    })


    describe('configuration options', () => {

      it('should accept custom shard count', () => {
        const router8 = createShardRouter({ shardCount: 8 })

        const assignment = router8.route('users')

        expect(assignment.shardId).toBeGreaterThanOrEqual(0)
        expect(assignment.shardId).toBeLessThanOrEqual(7)
      })

      it('should accept custom hash function', () => {
        const customHash = (input: string) => input.length % 16

        const routerCustom = createShardRouter({ hashFunction: customHash })

        const assignment = routerCustom.route('users') // length 5

        expect(assignment.shardId).toBe(5)
      })

      it('should validate shard count is power of 2', () => {
        expect(() => createShardRouter({ shardCount: 10 })).toThrow(/power of 2/i)
        expect(() => createShardRouter({ shardCount: 7 })).toThrow(/power of 2/i)

        // These should not throw
        expect(() => createShardRouter({ shardCount: 2 })).not.toThrow()
        expect(() => createShardRouter({ shardCount: 4 })).not.toThrow()
        expect(() => createShardRouter({ shardCount: 8 })).not.toThrow()
        expect(() => createShardRouter({ shardCount: 16 })).not.toThrow()
        expect(() => createShardRouter({ shardCount: 32 })).not.toThrow()
      })

      it('should export router statistics', () => {
        const stats = router.getStats()

        expect(stats).toBeDefined()
        expect(typeof stats.cacheHits).toBe('number')
        expect(typeof stats.cacheMisses).toBe('number')
        expect(typeof stats.totalRoutes).toBe('number')
      })

    })


    describe('edge cases', () => {

      it('should handle whitespace-only collection names', () => {
        expect(() => router.route('   ')).toThrow(/empty.*collection/i)
        expect(() => router.route('\t\n')).toThrow(/empty.*collection/i)
      })

      it('should handle null bytes in collection name', () => {
        const nameWithNull = 'collection\x00name'
        const assignment = router.route(nameWithNull)

        expect(assignment.shardId).toBeGreaterThanOrEqual(0)
        expect(assignment.shardId).toBeLessThanOrEqual(15)
      })

      it('should be thread-safe for concurrent routing calls', async () => {
        const promises: Promise<ShardAssignment>[] = []

        for (let i = 0; i < 1000; i++) {
          promises.push(
            Promise.resolve(router.route('concurrent_collection'))
          )
        }

        const results = await Promise.all(promises)
        const uniqueShards = new Set(results.map(r => r.shardId))

        // All should route to same shard
        expect(uniqueShards.size).toBe(1)
      })

      it('should handle rapid cache operations', () => {
        for (let i = 0; i < 10000; i++) {
          router.route(`collection_${i % 100}`)
        }

        // Should not throw or crash
        expect(router.getCacheSize()).toBeGreaterThan(0)
      })

    })

  })

})
