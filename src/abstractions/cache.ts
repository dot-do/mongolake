/**
 * Cache Abstraction
 *
 * Platform-agnostic interface for distributed caching.
 * This abstraction supports:
 * - Cloudflare KV
 * - Redis
 * - Memcached
 * - In-memory (for testing)
 *
 * ## Use Cases
 *
 * Caching in MongoLake is used for:
 * - Manifest caching to reduce R2 reads
 * - Query result caching
 * - Session/token caching for authentication
 *
 * @module abstractions/cache
 */

/**
 * Options for cache get operations.
 */
export interface CacheGetOptions {
  /**
   * Type to parse the value as.
   * - 'text': Return as string
   * - 'json': Parse as JSON
   * - 'arrayBuffer': Return as ArrayBuffer
   * - 'stream': Return as ReadableStream
   */
  type?: 'text' | 'json' | 'arrayBuffer' | 'stream';

  /**
   * Cache behavior hint.
   * Not all implementations support this.
   */
  cacheTtl?: number;
}

/**
 * Options for cache put operations.
 */
export interface CachePutOptions {
  /**
   * Time-to-live in seconds.
   * After this time, the key will be automatically deleted.
   */
  expirationTtl?: number;

  /**
   * Absolute expiration timestamp (Unix seconds).
   * The key will be deleted after this time.
   */
  expiration?: number;

  /**
   * Custom metadata to store with the value.
   */
  metadata?: Record<string, unknown>;
}

/**
 * Options for listing cache keys.
 */
export interface CacheListOptions {
  /** Prefix to filter keys */
  prefix?: string;

  /** Maximum number of keys to return */
  limit?: number;

  /** Cursor for pagination */
  cursor?: string;
}

/**
 * Result from listing cache keys.
 */
export interface CacheListResult {
  /** List of key names */
  keys: Array<{
    name: string;
    expiration?: number;
    metadata?: Record<string, unknown>;
  }>;

  /** Whether there are more results */
  list_complete: boolean;

  /** Cursor for next page */
  cursor?: string;
}

/**
 * Platform-agnostic cache interface.
 *
 * This interface provides a simple key-value cache API suitable
 * for most caching needs in MongoLake.
 *
 * ## Implementation Notes
 *
 * 1. **Consistency** - Caches are eventually consistent. Don't rely on
 *    immediate visibility of writes from other workers/instances.
 *
 * 2. **Size Limits** - Most implementations have value size limits
 *    (e.g., KV: 25MB, Redis: 512MB). Check your platform's limits.
 *
 * 3. **TTL** - Expiration is best-effort. Values may persist slightly
 *    longer than the specified TTL.
 *
 * ## Example Usage
 *
 * ```typescript
 * // Cache a manifest
 * await cache.put(`manifest:${collection}`, JSON.stringify(manifest), {
 *   expirationTtl: 300 // 5 minutes
 * });
 *
 * // Retrieve cached manifest
 * const cached = await cache.get(`manifest:${collection}`, { type: 'json' });
 * if (cached) {
 *   return cached as CollectionManifest;
 * }
 * ```
 */
export interface CacheBackend {
  /**
   * Get a value from the cache.
   *
   * @param key - Cache key
   * @param options - Options for reading the value
   * @returns The value if found, null otherwise
   */
  get(key: string, options?: CacheGetOptions): Promise<string | ArrayBuffer | ReadableStream | unknown | null>;

  /**
   * Get a value as a specific type.
   *
   * Type-safe alternative to get() with options.
   *
   * @param key - Cache key
   * @returns The value parsed as JSON, or null
   */
  getJson<T>(key: string): Promise<T | null>;

  /**
   * Store a value in the cache.
   *
   * @param key - Cache key
   * @param value - Value to store
   * @param options - Expiration and metadata options
   */
  put(
    key: string,
    value: string | ArrayBuffer | ReadableStream,
    options?: CachePutOptions
  ): Promise<void>;

  /**
   * Store a JSON value in the cache.
   *
   * Convenience method that serializes to JSON.
   *
   * @param key - Cache key
   * @param value - Value to serialize and store
   * @param options - Expiration and metadata options
   */
  putJson(key: string, value: unknown, options?: CachePutOptions): Promise<void>;

  /**
   * Delete a value from the cache.
   *
   * @param key - Cache key
   */
  delete(key: string): Promise<void>;

  /**
   * List keys in the cache.
   *
   * @param options - Listing options
   * @returns List result with pagination
   */
  list(options?: CacheListOptions): Promise<CacheListResult>;

  /**
   * Get with metadata.
   *
   * Some implementations support storing metadata alongside values.
   *
   * @param key - Cache key
   * @returns Value and metadata, or null
   */
  getWithMetadata?<T>(key: string): Promise<{
    value: T | null;
    metadata: Record<string, unknown> | null;
  }>;
}
