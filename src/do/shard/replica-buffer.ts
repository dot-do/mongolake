/**
 * Replica Buffer
 *
 * Manages in-memory document cache for replica reads.
 * Implements a simple LRU-style cache with TTL support.
 */

import type { CachedDocument, ReplicationWalEntry } from './replica-types.js';

/**
 * Manages replicated documents in memory.
 *
 * Unlike the primary's BufferManager, this is a read-only cache
 * that receives updates from the WAL sync process.
 */
export class ReplicaBuffer {
  /**
   * Document cache organized by collection.
   * Structure: collection name -> document _id -> CachedDocument
   */
  private cache: Map<string, Map<string, CachedDocument>> = new Map();

  /** Current highest LSN in the cache */
  private currentLSN: number = 0;

  /** TTL for cached documents in milliseconds */
  private cacheTtlMs: number;

  /** Maximum number of documents to cache per collection */
  private maxDocsPerCollection: number;

  constructor(options: { cacheTtlMs?: number; maxDocsPerCollection?: number } = {}) {
    this.cacheTtlMs = options.cacheTtlMs ?? 5000;
    this.maxDocsPerCollection = options.maxDocsPerCollection ?? 10000;
  }

  // ============================================================================
  // WAL Entry Application
  // ============================================================================

  /**
   * Apply a WAL entry to the cache.
   *
   * This is called by the sync manager when new entries arrive.
   */
  applyEntry(entry: ReplicationWalEntry): void {
    // Get or create collection cache
    let collectionCache = this.cache.get(entry.collection);
    if (!collectionCache) {
      collectionCache = new Map();
      this.cache.set(entry.collection, collectionCache);
    }

    // Apply based on operation type
    if (entry.op === 'd') {
      // Delete: remove from cache
      collectionCache.delete(entry.docId);
    } else {
      // Insert or Update: add/update in cache
      const cachedDoc: CachedDocument = {
        document: { ...entry.document, _id: entry.docId },
        lsn: entry.lsn,
        cachedAt: Date.now(),
      };
      collectionCache.set(entry.docId, cachedDoc);

      // Evict oldest if over limit
      if (collectionCache.size > this.maxDocsPerCollection) {
        this.evictOldest(collectionCache);
      }
    }

    // Update current LSN
    if (entry.lsn > this.currentLSN) {
      this.currentLSN = entry.lsn;
    }
  }

  // ============================================================================
  // Read Operations
  // ============================================================================

  /**
   * Get a document from the cache.
   *
   * @param collection - Collection name
   * @param docId - Document ID
   * @returns Document or null if not cached
   */
  get(collection: string, docId: string): Record<string, unknown> | null {
    const collectionCache = this.cache.get(collection);
    if (!collectionCache) return null;

    const cached = collectionCache.get(docId);
    if (!cached) return null;

    // Check TTL
    if (Date.now() - cached.cachedAt > this.cacheTtlMs) {
      collectionCache.delete(docId);
      return null;
    }

    return { ...cached.document };
  }

  /**
   * Get all documents from a collection in the cache.
   *
   * @param collection - Collection name
   * @returns Array of documents
   */
  getAll(collection: string): Array<Record<string, unknown>> {
    const collectionCache = this.cache.get(collection);
    if (!collectionCache) return [];

    const now = Date.now();
    const results: Array<Record<string, unknown>> = [];
    const expiredIds: string[] = [];

    for (const [docId, cached] of collectionCache) {
      if (now - cached.cachedAt > this.cacheTtlMs) {
        expiredIds.push(docId);
      } else {
        results.push({ ...cached.document });
      }
    }

    // Clean up expired entries
    for (const id of expiredIds) {
      collectionCache.delete(id);
    }

    return results;
  }

  /**
   * Check if a document exists in the cache.
   */
  has(collection: string, docId: string): boolean {
    return this.get(collection, docId) !== null;
  }

  // ============================================================================
  // State Access
  // ============================================================================

  /**
   * Get the current highest LSN in the cache.
   */
  getCurrentLSN(): number {
    return this.currentLSN;
  }

  /**
   * Get statistics about the cache.
   */
  getStats(): {
    collections: number;
    totalDocuments: number;
    currentLSN: number;
    perCollectionCounts: Record<string, number>;
  } {
    const perCollectionCounts: Record<string, number> = {};
    let totalDocuments = 0;

    for (const [collection, cache] of this.cache) {
      perCollectionCounts[collection] = cache.size;
      totalDocuments += cache.size;
    }

    return {
      collections: this.cache.size,
      totalDocuments,
      currentLSN: this.currentLSN,
      perCollectionCounts,
    };
  }

  // ============================================================================
  // Cache Management
  // ============================================================================

  /**
   * Clear the entire cache.
   */
  clear(): void {
    this.cache.clear();
    this.currentLSN = 0;
  }

  /**
   * Clear cache for a specific collection.
   */
  clearCollection(collection: string): void {
    this.cache.delete(collection);
  }

  /**
   * Evict expired entries from all collections.
   */
  evictExpired(): number {
    const now = Date.now();
    let evicted = 0;

    for (const [_, collectionCache] of this.cache) {
      const expiredIds: string[] = [];

      for (const [docId, cached] of collectionCache) {
        if (now - cached.cachedAt > this.cacheTtlMs) {
          expiredIds.push(docId);
        }
      }

      for (const id of expiredIds) {
        collectionCache.delete(id);
        evicted++;
      }
    }

    return evicted;
  }

  /**
   * Set the applied LSN (for recovery).
   */
  setCurrentLSN(lsn: number): void {
    this.currentLSN = lsn;
  }

  // ============================================================================
  // Internal Helpers
  // ============================================================================

  /**
   * Evict oldest entries when collection exceeds size limit.
   */
  private evictOldest(collectionCache: Map<string, CachedDocument>): void {
    // Find oldest entry by cachedAt timestamp
    let oldestId: string | null = null;
    let oldestTime = Infinity;

    for (const [docId, cached] of collectionCache) {
      if (cached.cachedAt < oldestTime) {
        oldestTime = cached.cachedAt;
        oldestId = docId;
      }
    }

    if (oldestId) {
      collectionCache.delete(oldestId);
    }
  }

  // ============================================================================
  // Configuration
  // ============================================================================

  /**
   * Update cache TTL.
   */
  setCacheTtl(ttlMs: number): void {
    this.cacheTtlMs = ttlMs;
  }

  /**
   * Update max documents per collection limit.
   */
  setMaxDocsPerCollection(max: number): void {
    this.maxDocsPerCollection = max;
  }
}
