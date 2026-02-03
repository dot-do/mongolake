/**
 * Cloudflare KV Cache Implementation
 *
 * Implements CacheBackend using Cloudflare KV.
 *
 * @module abstractions/cloudflare/cache
 */

import type {
  CacheBackend,
  CacheGetOptions,
  CachePutOptions,
  CacheListOptions,
  CacheListResult,
} from '../cache.js';

/**
 * Cloudflare KV Namespace interface.
 *
 * Matches the KVNamespace type from @cloudflare/workers-types
 * but defined here to avoid direct dependency.
 */
export interface KVNamespace {
  get(key: string, options?: { type?: 'text' | 'json' | 'arrayBuffer' | 'stream'; cacheTtl?: number }): Promise<string | ArrayBuffer | ReadableStream | unknown | null>;
  getWithMetadata<T = unknown>(key: string, options?: { type?: 'text' | 'json' | 'arrayBuffer' | 'stream' }): Promise<{
    value: T | null;
    metadata: Record<string, unknown> | null;
  }>;
  put(key: string, value: string | ArrayBuffer | ReadableStream, options?: {
    expirationTtl?: number;
    expiration?: number;
    metadata?: Record<string, unknown>;
  }): Promise<void>;
  delete(key: string): Promise<void>;
  list(options?: { prefix?: string; limit?: number; cursor?: string }): Promise<{
    keys: Array<{ name: string; expiration?: number; metadata?: Record<string, unknown> }>;
    list_complete: boolean;
    cursor?: string;
  }>;
}

/**
 * Cloudflare KV implementation of CacheBackend.
 *
 * This implementation wraps Cloudflare KV to provide a platform-agnostic
 * caching interface.
 *
 * ## KV Characteristics
 *
 * - Eventually consistent (reads may lag writes by ~60 seconds globally)
 * - Maximum value size: 25 MB
 * - Maximum key size: 512 bytes
 * - TTL minimum: 60 seconds
 *
 * ## Usage
 *
 * ```typescript
 * const cache = new CloudflareKVCache(env.CACHE_KV);
 *
 * // Store a value
 * await cache.put('key', 'value', { expirationTtl: 300 });
 *
 * // Retrieve a value
 * const value = await cache.get('key');
 * ```
 */
export class CloudflareKVCache implements CacheBackend {
  constructor(private kv: KVNamespace) {}

  async get(
    key: string,
    options?: CacheGetOptions
  ): Promise<string | ArrayBuffer | ReadableStream | unknown | null> {
    return this.kv.get(key, {
      type: options?.type,
      cacheTtl: options?.cacheTtl,
    });
  }

  async getJson<T>(key: string): Promise<T | null> {
    return await this.kv.get(key, { type: 'json' }) as T | null;
  }

  async put(
    key: string,
    value: string | ArrayBuffer | ReadableStream,
    options?: CachePutOptions
  ): Promise<void> {
    await this.kv.put(key, value, {
      expirationTtl: options?.expirationTtl,
      expiration: options?.expiration,
      metadata: options?.metadata,
    });
  }

  async putJson(key: string, value: unknown, options?: CachePutOptions): Promise<void> {
    await this.put(key, JSON.stringify(value), options);
  }

  async delete(key: string): Promise<void> {
    await this.kv.delete(key);
  }

  async list(options?: CacheListOptions): Promise<CacheListResult> {
    return await this.kv.list({
      prefix: options?.prefix,
      limit: options?.limit,
      cursor: options?.cursor,
    });
  }

  async getWithMetadata<T>(key: string): Promise<{
    value: T | null;
    metadata: Record<string, unknown> | null;
  }> {
    return await this.kv.getWithMetadata<T>(key, { type: 'json' });
  }

  /**
   * Get the underlying KVNamespace.
   *
   * Use this for KV-specific operations not covered by the abstraction.
   * Note: Using this breaks portability.
   */
  getKVNamespace(): KVNamespace {
    return this.kv;
  }
}

/**
 * Factory function to create Cloudflare KV cache.
 *
 * @param kv - KVNamespace from environment bindings
 * @returns CacheBackend implementation
 */
export function createCloudflareCache(kv: KVNamespace): CacheBackend {
  return new CloudflareKVCache(kv);
}
