/**
 * LRU Cache Implementation for MongoLake
 *
 * A generic Least-Recently-Used (LRU) cache with:
 * - Configurable maximum size
 * - LRU eviction when full
 * - get/set/has/delete operations
 * - Optional TTL (time-to-live) support
 * - O(1) operations using a Map (maintains insertion order)
 * - Optional metrics integration
 *
 * The cache maintains entry ordering by moving accessed entries to the end
 * of the underlying Map. Eviction removes entries from the beginning (oldest).
 */

import { MetricsCollector, METRICS } from '../metrics/index.js';

// ============================================================================
// Types
// ============================================================================

/** Options for creating an LRU cache */
export interface LRUCacheOptions<K, V> {
  /** Maximum number of entries in the cache */
  maxSize: number;
  /** Optional maximum memory in bytes (for memory-based limits) */
  maxMemoryBytes?: number;
  /** Optional TTL in milliseconds (entries expire after this time) */
  ttlMs?: number;
  /** Optional callback when an entry is evicted */
  onEvict?: (key: K, value: V, reason: 'capacity' | 'expired' | 'manual' | 'memory' | 'resize') => void;
  /** Optional name for this cache (used for metrics labeling) */
  name?: string;
  /** Optional metrics collector for recording cache metrics */
  metrics?: MetricsCollector;
  /** Optional function to estimate memory size of a value */
  sizeEstimator?: (value: V) => number;
}

/** Internal cache entry with metadata */
interface CacheEntry<V> {
  value: V;
  /** Timestamp when the entry was created/updated */
  createdAt: number;
  /** Last access timestamp (for debugging/metrics) */
  lastAccessedAt: number;
  /** Estimated memory size in bytes */
  memorySize: number;
}

/** Cache statistics for monitoring */
export interface LRUCacheStats {
  /** Current number of entries */
  size: number;
  /** Maximum capacity */
  maxSize: number;
  /** Number of cache hits */
  hits: number;
  /** Number of cache misses */
  misses: number;
  /** Number of entries evicted due to capacity */
  capacityEvictions: number;
  /** Number of entries evicted due to TTL expiration */
  ttlEvictions: number;
  /** Number of entries evicted due to memory pressure */
  memoryEvictions: number;
  /** Number of entries evicted due to resize */
  resizeEvictions: number;
  /** Hit rate as a percentage (0-100) */
  hitRate: number;
  /** Estimated memory usage in bytes */
  memoryBytes: number;
  /** Maximum memory limit in bytes (if configured) */
  maxMemoryBytes?: number;
  /** Memory usage as a percentage (0-100), undefined if no memory limit */
  memoryUsagePercent?: number;
  /** TTL configured (in ms) */
  ttlMs?: number;
  /** Cache name */
  name: string;
}

// ============================================================================
// LRU Cache Implementation
// ============================================================================

/**
 * Generic LRU (Least Recently Used) cache implementation.
 *
 * Uses a Map to maintain insertion order. On access, entries are moved
 * to the end by deleting and re-inserting. On eviction, the first entry
 * (oldest/least recently used) is removed.
 *
 * @template K - Key type
 * @template V - Value type
 */
export class LRUCache<K, V> {
  private readonly cache: Map<K, CacheEntry<V>> = new Map();
  private maxSize: number;
  private maxMemoryBytes: number | undefined;
  private readonly ttlMs: number | undefined;
  private readonly onEvict?: (key: K, value: V, reason: 'capacity' | 'expired' | 'manual' | 'memory' | 'resize') => void;
  private readonly cacheName: string;
  private readonly metrics?: MetricsCollector;
  private readonly sizeEstimator: (value: V) => number;

  // Memory tracking
  private currentMemoryBytes: number = 0;

  // Statistics tracking
  private hits: number = 0;
  private misses: number = 0;
  private capacityEvictions: number = 0;
  private ttlEvictions: number = 0;
  private memoryEvictions: number = 0;
  private resizeEvictions: number = 0;

  constructor(options: LRUCacheOptions<K, V>) {
    if (options.maxSize < 1) {
      throw new Error('LRU cache maxSize must be at least 1');
    }

    if (options.maxMemoryBytes !== undefined && options.maxMemoryBytes < 1) {
      throw new Error('LRU cache maxMemoryBytes must be at least 1');
    }

    this.maxSize = options.maxSize;
    this.maxMemoryBytes = options.maxMemoryBytes;
    this.ttlMs = options.ttlMs;
    this.onEvict = options.onEvict;
    this.cacheName = options.name ?? 'default';
    this.metrics = options.metrics;
    this.sizeEstimator = options.sizeEstimator ?? this.defaultSizeEstimator;
  }

  /**
   * Default size estimator for memory tracking.
   * Provides rough estimates for common types.
   */
  private defaultSizeEstimator(value: V): number {
    if (value === null || value === undefined) {
      return 8;
    }

    const type = typeof value;

    if (type === 'boolean') return 4;
    if (type === 'number') return 8;
    // When typeof returns 'string', value is guaranteed to be a string at runtime
    if (type === 'string') return (value as string).length * 2 + 48;
    if (type === 'bigint') return 16;

    // ArrayBuffer.isView is a type guard that narrows value to ArrayBufferView
    if (ArrayBuffer.isView(value)) {
      return value.byteLength + 16;
    }

    // instanceof ArrayBuffer narrows value to ArrayBuffer
    if (value instanceof ArrayBuffer) {
      return value.byteLength + 16;
    }

    if (Array.isArray(value)) {
      let size = 48 + value.length * 8;
      for (const item of value) {
        size += this.defaultSizeEstimator(item as V);
      }
      return size;
    }

    if (type === 'object') {
      let size = 48;
      for (const key of Object.keys(value as object)) {
        size += key.length * 2 + 48;
        size += this.defaultSizeEstimator((value as Record<string, unknown>)[key] as V);
      }
      return size;
    }

    return 16;
  }

  /**
   * Get a value from the cache.
   * Returns undefined if the key is not found or the entry has expired.
   * Accessing an entry moves it to the "end" (most recently used).
   */
  get(key: K): V | undefined {
    const entry = this.cache.get(key);

    if (!entry) {
      this.misses++;
      this.recordCacheMiss();
      return undefined;
    }

    // Check TTL expiration
    if (this.isExpired(entry)) {
      this.evict(key, 'expired');
      this.misses++;
      this.recordCacheMiss();
      return undefined;
    }

    // Move to end (most recently used) by re-inserting
    this.cache.delete(key);
    entry.lastAccessedAt = Date.now();
    this.cache.set(key, entry);

    this.hits++;
    this.recordCacheHit();
    return entry.value;
  }

  /**
   * Set a value in the cache.
   * If the key already exists, it updates the value and moves it to the end.
   * If the cache is at capacity, evicts the least recently used entry.
   */
  set(key: K, value: V): this {
    const memorySize = this.sizeEstimator(value);

    // If key exists, update and move to end
    if (this.cache.has(key)) {
      const existingEntry = this.cache.get(key)!;
      this.currentMemoryBytes -= existingEntry.memorySize;
      this.cache.delete(key);
    } else {
      // Make room if at capacity (entries)
      this.ensureCapacity();
    }

    // Make room if at memory capacity
    this.ensureMemoryCapacity(memorySize);

    const now = Date.now();
    this.cache.set(key, {
      value,
      createdAt: now,
      lastAccessedAt: now,
      memorySize,
    });
    this.currentMemoryBytes += memorySize;

    return this;
  }

  /**
   * Check if a key exists in the cache and is not expired.
   * Does NOT update the entry's access time (non-mutating check).
   */
  has(key: K): boolean {
    const entry = this.cache.get(key);

    if (!entry) {
      return false;
    }

    // Check TTL expiration
    if (this.isExpired(entry)) {
      this.evict(key, 'expired');
      return false;
    }

    return true;
  }

  /**
   * Delete a specific key from the cache.
   * Returns true if the key was found and deleted.
   */
  delete(key: K): boolean {
    const entry = this.cache.get(key);

    if (!entry) {
      return false;
    }

    this.cache.delete(key);
    this.currentMemoryBytes -= entry.memorySize;

    // Notify via callback
    if (this.onEvict) {
      this.onEvict(key, entry.value, 'manual');
    }

    return true;
  }

  /**
   * Clear all entries from the cache.
   * Optionally notifies via onEvict callback for each entry.
   */
  clear(notifyEvictions: boolean = false): void {
    if (notifyEvictions && this.onEvict) {
      for (const [key, entry] of this.cache) {
        this.onEvict(key, entry.value, 'manual');
      }
    }
    this.cache.clear();
    this.currentMemoryBytes = 0;
  }

  /**
   * Get the current number of entries in the cache.
   */
  get size(): number {
    return this.cache.size;
  }

  /**
   * Get all keys in the cache (in LRU order: oldest first).
   */
  keys(): IterableIterator<K> {
    return this.cache.keys();
  }

  /**
   * Get all values in the cache (in LRU order: oldest first).
   */
  values(): V[] {
    const result: V[] = [];
    for (const entry of this.cache.values()) {
      result.push(entry.value);
    }
    return result;
  }

  /**
   * Get all entries in the cache (in LRU order: oldest first).
   */
  entries(): Array<[K, V]> {
    const result: Array<[K, V]> = [];
    for (const [key, entry] of this.cache) {
      result.push([key, entry.value]);
    }
    return result;
  }

  /**
   * Iterate over all entries in the cache.
   * Callback receives (value, key, cache).
   *
   * Safe for mutation during iteration: takes a snapshot of keys before
   * iterating and checks if entries still exist before calling callback.
   */
  forEach(callback: (value: V, key: K, cache: LRUCache<K, V>) => void): void {
    // Take a snapshot of keys to safely handle mutation during iteration
    const keys = Array.from(this.cache.keys());
    for (const key of keys) {
      const entry = this.cache.get(key);
      // Check if entry still exists (may have been deleted during iteration)
      if (entry !== undefined) {
        callback(entry.value, key, this);
      }
    }
  }

  /**
   * Peek at a value without updating its access time.
   * Returns undefined if the key is not found or expired.
   */
  peek(key: K): V | undefined {
    const entry = this.cache.get(key);

    if (!entry) {
      return undefined;
    }

    if (this.isExpired(entry)) {
      this.evict(key, 'expired');
      return undefined;
    }

    return entry.value;
  }

  /**
   * Get cache statistics for monitoring and debugging.
   */
  getStats(): LRUCacheStats {
    const total = this.hits + this.misses;
    const hitRate = total > 0 ? (this.hits / total) * 100 : 0;

    const stats: LRUCacheStats = {
      size: this.cache.size,
      maxSize: this.maxSize,
      hits: this.hits,
      misses: this.misses,
      capacityEvictions: this.capacityEvictions,
      ttlEvictions: this.ttlEvictions,
      memoryEvictions: this.memoryEvictions,
      resizeEvictions: this.resizeEvictions,
      hitRate: Math.round(hitRate * 100) / 100,
      memoryBytes: this.currentMemoryBytes,
      maxMemoryBytes: this.maxMemoryBytes,
      ttlMs: this.ttlMs,
      name: this.cacheName,
    };

    if (this.maxMemoryBytes) {
      stats.memoryUsagePercent = Math.round(
        (this.currentMemoryBytes / this.maxMemoryBytes) * 10000
      ) / 100;
    }

    return stats;
  }

  /**
   * Reset cache statistics.
   */
  resetStats(): void {
    this.hits = 0;
    this.misses = 0;
    this.capacityEvictions = 0;
    this.ttlEvictions = 0;
    this.memoryEvictions = 0;
    this.resizeEvictions = 0;
  }

  /**
   * Resize the cache to a new maximum size.
   * If the new size is smaller, evicts entries until the cache fits.
   *
   * @param newMaxSize - New maximum number of entries
   */
  resize(newMaxSize: number): void {
    if (newMaxSize < 1) {
      throw new Error('LRU cache maxSize must be at least 1');
    }

    const oldMaxSize = this.maxSize;
    this.maxSize = newMaxSize;

    // If shrinking, evict entries until we fit
    if (newMaxSize < oldMaxSize) {
      while (this.cache.size > newMaxSize) {
        this.evictLRU('resize');
      }
    }
  }

  /**
   * Resize the cache memory limit.
   * If the new limit is smaller, evicts entries until memory usage fits.
   *
   * @param newMaxMemoryBytes - New maximum memory in bytes (undefined to disable memory limit)
   */
  resizeMemory(newMaxMemoryBytes: number | undefined): void {
    if (newMaxMemoryBytes !== undefined && newMaxMemoryBytes < 1) {
      throw new Error('LRU cache maxMemoryBytes must be at least 1');
    }

    this.maxMemoryBytes = newMaxMemoryBytes;

    // If setting a memory limit and we're over it, evict entries
    if (newMaxMemoryBytes !== undefined) {
      while (
        this.currentMemoryBytes > newMaxMemoryBytes &&
        this.cache.size > 0
      ) {
        this.evictLRU('resize');
      }
    }
  }

  /**
   * Get the current memory usage in bytes.
   */
  getMemoryBytes(): number {
    return this.currentMemoryBytes;
  }

  /**
   * Get the maximum memory limit in bytes.
   */
  getMaxMemoryBytes(): number | undefined {
    return this.maxMemoryBytes;
  }

  /**
   * Get the maximum size (entry count) of the cache.
   */
  getMaxSize(): number {
    return this.maxSize;
  }

  /**
   * Prune expired entries from the cache.
   * Useful for periodic cleanup when TTL is enabled.
   * Returns the number of entries pruned.
   *
   * Safe for mutation: takes a snapshot of entries before iterating.
   */
  prune(): number {
    if (!this.ttlMs) {
      return 0;
    }

    let pruned = 0;
    const now = Date.now();

    // Take a snapshot to safely handle mutation during iteration
    const entries = Array.from(this.cache.entries());
    for (const [key, entry] of entries) {
      if (now - entry.createdAt > this.ttlMs) {
        this.evict(key, 'expired');
        pruned++;
      }
    }

    return pruned;
  }

  /**
   * Get the oldest entry (least recently used) without removing it.
   * Returns undefined if the cache is empty.
   */
  peekOldest(): { key: K; value: V } | undefined {
    const firstKey = this.cache.keys().next().value;
    if (firstKey === undefined) {
      return undefined;
    }

    const entry = this.cache.get(firstKey);
    if (!entry) {
      return undefined;
    }

    return { key: firstKey, value: entry.value };
  }

  /**
   * Get the newest entry (most recently used) without removing it.
   * Returns undefined if the cache is empty.
   */
  peekNewest(): { key: K; value: V } | undefined {
    let lastKey: K | undefined;
    let lastEntry: CacheEntry<V> | undefined;

    for (const [key, entry] of this.cache) {
      lastKey = key;
      lastEntry = entry;
    }

    if (lastKey === undefined || !lastEntry) {
      return undefined;
    }

    return { key: lastKey, value: lastEntry.value };
  }

  // --------------------------------------------------------------------------
  // Private Methods
  // --------------------------------------------------------------------------

  /**
   * Check if an entry has expired based on TTL.
   */
  private isExpired(entry: CacheEntry<V>): boolean {
    if (!this.ttlMs) {
      return false;
    }
    return Date.now() - entry.createdAt > this.ttlMs;
  }

  /**
   * Ensure there's room for a new entry by evicting LRU if needed.
   */
  private ensureCapacity(): void {
    while (this.cache.size >= this.maxSize) {
      this.evictLRU('capacity');
    }
  }

  /**
   * Ensure there's room for a new entry by memory by evicting LRU if needed.
   */
  private ensureMemoryCapacity(newEntrySize: number): void {
    if (!this.maxMemoryBytes) {
      return;
    }

    while (
      this.currentMemoryBytes + newEntrySize > this.maxMemoryBytes &&
      this.cache.size > 0
    ) {
      this.evictLRU('memory');
    }
  }

  /**
   * Evict the least recently used entry (first entry in the Map).
   */
  private evictLRU(reason: 'capacity' | 'memory' | 'resize' = 'capacity'): void {
    const oldestKey = this.cache.keys().next().value;
    if (oldestKey !== undefined) {
      this.evict(oldestKey, reason);
    }
  }

  /**
   * Evict a specific entry and invoke callback.
   */
  private evict(key: K, reason: 'capacity' | 'expired' | 'memory' | 'resize'): void {
    const entry = this.cache.get(key);
    if (!entry) {
      return;
    }

    this.cache.delete(key);
    this.currentMemoryBytes -= entry.memorySize;

    // Update statistics
    switch (reason) {
      case 'capacity':
        this.capacityEvictions++;
        break;
      case 'expired':
        this.ttlEvictions++;
        break;
      case 'memory':
        this.memoryEvictions++;
        break;
      case 'resize':
        this.resizeEvictions++;
        break;
    }

    // Record eviction metrics
    this.recordEviction();

    // Notify via callback
    if (this.onEvict) {
      this.onEvict(key, entry.value, reason);
    }
  }

  // --------------------------------------------------------------------------
  // Metrics Recording
  // --------------------------------------------------------------------------

  /**
   * Record a cache hit to metrics.
   */
  private recordCacheHit(): void {
    if (this.metrics) {
      this.metrics.inc(METRICS.CACHE_HITS.name, { cache_type: this.cacheName });
    }
  }

  /**
   * Record a cache miss to metrics.
   */
  private recordCacheMiss(): void {
    if (this.metrics) {
      this.metrics.inc(METRICS.CACHE_MISSES.name, { cache_type: this.cacheName });
    }
  }

  /**
   * Record a cache eviction to metrics.
   */
  private recordEviction(): void {
    if (this.metrics) {
      this.metrics.inc(METRICS.CACHE_EVICTIONS.name, { cache_type: this.cacheName });
    }
  }

  /**
   * Update cache size metrics.
   * Call this after operations that change the cache size.
   */
  updateSizeMetrics(): void {
    if (this.metrics) {
      this.metrics.set(METRICS.CACHE_ENTRIES.name, this.cache.size, { cache_type: this.cacheName });
    }
  }
}

// ============================================================================
// Factory Function
// ============================================================================

/**
 * Create a new LRU cache instance.
 *
 * @template K - Key type
 * @template V - Value type
 * @param options - Cache configuration options
 * @returns A new LRU cache instance
 *
 * @example
 * ```typescript
 * // Simple cache with max 100 entries
 * const cache = createLRUCache<string, Document>({ maxSize: 100 });
 *
 * // Cache with TTL of 5 minutes
 * const timedCache = createLRUCache<string, TokenInfo>({
 *   maxSize: 1000,
 *   ttlMs: 5 * 60 * 1000,
 * });
 *
 * // Cache with eviction callback
 * const trackedCache = createLRUCache<string, ShardConnection>({
 *   maxSize: 50,
 *   onEvict: (key, value, reason) => {
 *     console.log(`Evicted ${key} due to ${reason}`);
 *     value.close();
 *   },
 * });
 * ```
 */
export function createLRUCache<K, V>(options: LRUCacheOptions<K, V>): LRUCache<K, V> {
  return new LRUCache(options);
}
