/**
 * MongoLake Shard Router
 *
 * Implements consistent hashing for distributing collections and documents
 * across shards (0-15 range by default). Features include:
 * - Consistent hashing for shard distribution
 * - Shard assignment caching for performance
 * - Shard splitting for hot collections
 * - Shard affinity hints
 * - Document-level routing by _id
 * - Database-prefixed routing
 */

// ============================================================================
// Types
// ============================================================================

export interface ShardRouterOptions {
  /** Number of shards (must be power of 2, default: 16) */
  shardCount?: number;
  /** Maximum cache size (default: unlimited) */
  cacheSize?: number;
  /** Custom hash function */
  hashFunction?: (input: string) => number;
}

export interface ShardAssignment {
  /** The assigned shard ID (0 to shardCount-1) */
  shardId: number;
  /** The collection name */
  collection: string;
  /** The database name (if database-prefixed routing was used) */
  database?: string;
  /** The document ID (if document-level routing was used) */
  documentId?: string;
}

export interface ShardAffinityHint {
  /** The collection name */
  collection: string;
  /** The preferred shard ID */
  preferredShard: number;
}

export interface SplitInfo {
  /** The collection name */
  collection: string;
  /** The shard IDs this collection is split across */
  shards: number[];
}

export interface RouterStats {
  /** Number of cache hits */
  cacheHits: number;
  /** Number of cache misses */
  cacheMisses: number;
  /** Total number of route calls */
  totalRoutes: number;
}

// ============================================================================
// Hash Functions
// ============================================================================

/**
 * MurmurHash3-inspired hash function for consistent string hashing.
 * Produces a well-distributed 32-bit unsigned integer.
 */
function murmurHashLite(input: string): number {
  let hash = 0x811c9dc5; // FNV-1a 32-bit offset basis

  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    // FNV-1a 32-bit prime multiplier
    hash = Math.imul(hash, 0x01000193);
  }

  // Final mixing to improve avalanche effect and bit distribution
  hash ^= hash >>> 16;
  hash = Math.imul(hash, 0x85ebca6b);
  hash ^= hash >>> 13;
  hash = Math.imul(hash, 0xc2b2ae35);
  hash ^= hash >>> 16;

  return hash >>> 0; // Ensure unsigned 32-bit integer
}

/**
 * Hash a collection name to a shard ID
 * @param collectionName The collection name to hash
 * @param shardCount Number of shards (default: 16)
 * @returns Shard ID in range [0, shardCount)
 */
export function hashCollectionToShard(
  collectionName: string,
  shardCount: number = 16
): number {
  if (!collectionName || collectionName.trim().length === 0) {
    throw new Error('Cannot hash empty collection name');
  }
  return murmurHashLite(collectionName) % shardCount;
}

/**
 * Hash a document _id to a shard ID
 * @param documentId The document ID to hash
 * @param shardCount Number of shards (default: 16)
 * @returns Shard ID in range [0, shardCount)
 */
export function hashDocumentToShard(
  documentId: string,
  shardCount: number = 16
): number {
  if (!documentId || documentId.length === 0) {
    throw new Error('Cannot hash empty document id');
  }
  return murmurHashLite(documentId) % shardCount;
}

// ============================================================================
// ShardRouter Class
// ============================================================================

/**
 * ShardRouter provides consistent hashing for distributing collections
 * and documents across shards.
 */
export class ShardRouter {
  private readonly shardCount: number;
  private readonly cacheSize: number;
  private readonly hashFunction: (input: string) => number;

  // LRU cache using Map (insertion order is maintained for FIFO eviction)
  private readonly cache: Map<string, ShardAssignment>;

  // Affinity hints: collection/namespace -> preferred shard ID
  private readonly affinityHints: Map<string, number>;

  // Split configurations: collection -> array of shard IDs
  private readonly splits: Map<string, number[]>;

  // Tracks cache performance for debugging
  private stats: RouterStats;

  constructor(options: ShardRouterOptions = {}) {
    this.shardCount = options.shardCount ?? 16;
    this.cacheSize = options.cacheSize ?? Infinity;
    this.hashFunction = options.hashFunction ?? murmurHashLite;

    // Validate shard count is power of 2 for consistent bitwise operations
    if (!this.isPowerOfTwo(this.shardCount)) {
      throw new Error(
        `Shard count must be a power of 2, got: ${this.shardCount}`
      );
    }

    this.cache = new Map();
    this.affinityHints = new Map();
    this.splits = new Map();
    this.stats = { cacheHits: 0, cacheMisses: 0, totalRoutes: 0 };
  }

  /**
   * Check if a number is a power of 2 using bitwise operation
   * n & (n-1) equals 0 only for powers of 2
   */
  private isPowerOfTwo(n: number): boolean {
    return n > 0 && (n & (n - 1)) === 0;
  }

  /**
   * Compute hash for input string modulo shard count
   */
  private computeHash(input: string): number {
    return this.hashFunction(input) % this.shardCount;
  }

  /**
   * Evict oldest (first) entry from cache if at capacity using LRU
   */
  private evictOldestIfFull(): void {
    if (this.cache.size >= this.cacheSize) {
      const oldestKey = this.cache.keys().next().value;
      if (oldestKey) {
        this.cache.delete(oldestKey);
      }
    }
  }

  /**
   * Route a collection to a shard using consistent hashing
   * Returns cached result if available
   */
  route(collection: string): ShardAssignment {
    if (!collection || collection.trim().length === 0) {
      throw new Error('Cannot route empty collection name');
    }

    this.stats.totalRoutes++;

    // Return cached result if available
    const cached = this.cache.get(collection);
    if (cached) {
      this.stats.cacheHits++;
      return cached;
    }

    this.stats.cacheMisses++;

    // Use affinity hint if set, otherwise compute hash
    const shardId = this.affinityHints.has(collection)
      ? this.affinityHints.get(collection)!
      : this.computeHash(collection);

    const assignment: ShardAssignment = { shardId, collection };

    // Cache result for future lookups
    this.evictOldestIfFull();
    this.cache.set(collection, assignment);

    return assignment;
  }

  /**
   * Route a database-prefixed collection to a shard
   * Allows same collection name to be routed differently across databases
   */
  routeWithDatabase(database: string, collection: string): ShardAssignment {
    if (!database || database.trim().length === 0) {
      throw new Error('Cannot route with empty database name');
    }
    if (!collection || collection.trim().length === 0) {
      throw new Error('Cannot route empty collection name');
    }

    this.stats.totalRoutes++;

    // Use database.collection as cache key for namespace isolation
    const namespace = `${database}.${collection}`;

    // Return cached result if available
    const cached = this.cache.get(namespace);
    if (cached) {
      this.stats.cacheHits++;
      return cached;
    }

    this.stats.cacheMisses++;

    // Use affinity hint if set, otherwise compute hash on namespace
    const shardId = this.affinityHints.has(namespace)
      ? this.affinityHints.get(namespace)!
      : this.computeHash(namespace);

    const assignment: ShardAssignment = { shardId, collection, database };

    // Cache result for future lookups
    this.evictOldestIfFull();
    this.cache.set(namespace, assignment);

    return assignment;
  }

  /**
   * Route a document by its _id to a shard
   * If collection is split, routes to one of the split shards
   * Note: Results are not cached as document IDs are unbounded
   */
  routeDocument(collection: string, documentId: string): ShardAssignment {
    if (!collection || collection.trim().length === 0) {
      throw new Error('Cannot route empty collection name');
    }
    if (!documentId || documentId.length === 0) {
      throw new Error('Cannot route empty document id');
    }

    this.stats.totalRoutes++;

    // If collection is split, use one of the split shards
    if (this.splits.has(collection)) {
      const splitShards = this.splits.get(collection)!;
      const hash = this.hashFunction(documentId);
      const shardIndex = hash % splitShards.length;
      return {
        shardId: splitShards[shardIndex],
        collection,
        documentId,
      };
    }

    // Otherwise, route document ID to shard directly
    const shardId = this.hashFunction(documentId) % this.shardCount;
    return { shardId, collection, documentId };
  }

  /**
   * Clear all cached assignments
   */
  clearCache(): void {
    this.cache.clear();
  }

  /**
   * Get current number of cached entries
   */
  getCacheSize(): number {
    return this.cache.size;
  }

  /**
   * Check if collection/namespace is cached
   */
  isCached(collection: string): boolean {
    return this.cache.has(collection);
  }

  /**
   * Set an affinity hint to force a collection to a specific shard
   * Clears cached assignment to apply new hint immediately
   */
  setAffinityHint(collection: string, hint: { preferredShard: number }): void {
    const shardId = hint.preferredShard;
    if (shardId < 0 || shardId >= this.shardCount) {
      throw new Error(
        `Shard ID out of range: ${shardId}. Must be between 0 and ${this.shardCount - 1}`
      );
    }
    this.affinityHints.set(collection, shardId);
    // Invalidate cache so new hint is applied on next route
    this.cache.delete(collection);
  }

  /**
   * Remove an affinity hint, reverting to hash-based routing
   * Clears cached assignment to revert to hashing immediately
   */
  removeAffinityHint(collection: string): void {
    this.affinityHints.delete(collection);
    // Invalidate cache so hash-based routing is used on next route
    this.cache.delete(collection);
  }

  /**
   * Get all active affinity hints
   */
  getAffinityHints(): ShardAffinityHint[] {
    const hints: ShardAffinityHint[] = [];
    for (const [collection, preferredShard] of this.affinityHints) {
      hints.push({ collection, preferredShard });
    }
    return hints;
  }

  /**
   * Split a collection across multiple shards for load distribution
   * Documents will be routed to one of the split shards via hashing
   */
  splitCollection(collection: string, shards: number[]): void {
    if (shards.length < 2) {
      throw new Error('Split requires at least 2 shards');
    }

    // Validate all shard IDs are in valid range
    for (const shardId of shards) {
      if (shardId < 0 || shardId >= this.shardCount) {
        throw new Error(
          `Shard ID out of range: ${shardId}. Must be between 0 and ${this.shardCount - 1}`
        );
      }
    }

    this.splits.set(collection, shards);
  }

  /**
   * Remove split configuration, reverting to single-shard routing
   */
  unsplitCollection(collection: string): void {
    this.splits.delete(collection);
  }

  /**
   * Get split configuration for a collection
   */
  getSplitInfo(collection: string): SplitInfo | undefined {
    const shards = this.splits.get(collection);
    return shards ? { collection, shards } : undefined;
  }

  /**
   * Get all active split configurations
   */
  getAllSplits(): SplitInfo[] {
    const splits: SplitInfo[] = [];
    for (const [collection, shards] of this.splits) {
      splits.push({ collection, shards });
    }
    return splits;
  }

  /**
   * Get cache performance statistics
   * Returns a copy to prevent external mutation
   */
  getStats(): RouterStats {
    return { ...this.stats };
  }
}

// ============================================================================
// Factory Function
// ============================================================================

/**
 * Create a new ShardRouter instance with optional configuration
 */
export function createShardRouter(options?: ShardRouterOptions): ShardRouter {
  return new ShardRouter(options);
}
