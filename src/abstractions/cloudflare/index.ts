/**
 * Cloudflare Platform Implementations
 *
 * This module provides Cloudflare-specific implementations of the
 * MongoLake abstraction interfaces.
 *
 * ## Components
 *
 * - **Storage** - R2 implementation of ObjectStorageBackend
 * - **Coordination** - Durable Objects implementation of CoordinationBackend
 * - **Cache** - KV implementation of CacheBackend
 *
 * ## Usage
 *
 * ```typescript
 * import {
 *   createCloudflareStorage,
 *   createCloudflareCoordination,
 *   createCloudflareCache,
 * } from '@mongolake/abstractions/cloudflare';
 *
 * // In your Worker or Durable Object:
 * const storage = createCloudflareStorage(env.DATA_BUCKET);
 * const coordination = createCloudflareCoordination(state);
 * const cache = createCloudflareCache(env.CACHE_KV);
 * ```
 *
 * @module abstractions/cloudflare
 */

export {
  CloudflareR2Storage,
  createCloudflareStorage,
} from './storage.js';

export {
  CloudflareCoordinationBackend,
  CloudflareSqlStorage,
  CloudflareCoordinatorStub,
  CloudflareCoordinatorNamespace,
  createCloudflareCoordination,
  createCloudflareNamespace,
} from './coordination.js';

export {
  CloudflareKVCache,
  createCloudflareCache,
  type KVNamespace,
} from './cache.js';
