/**
 * MongoLake Abstraction Layer
 *
 * This module provides platform-agnostic interfaces for core infrastructure components.
 * The goal is to decouple MongoLake from Cloudflare-specific APIs, enabling:
 * - Alternative cloud deployments (AWS, GCP, Azure)
 * - Self-hosted deployments
 * - Easier testing
 *
 * ## Architecture
 *
 * The abstraction layer defines interfaces for three key concerns:
 *
 * 1. **Storage** - Object storage for Parquet files (R2, S3, GCS, local filesystem)
 * 2. **Coordination** - Distributed state and actor coordination (Durable Objects, Redis, etcd)
 * 3. **Caching** - Key-value caching (KV, Redis, Memcached)
 *
 * ## Usage
 *
 * Import the interfaces and use the factory functions:
 *
 * ```typescript
 * import {
 *   type ObjectStorageBackend,
 *   type CoordinationBackend,
 *   type CacheBackend,
 *   createCloudflareStorage,
 *   createCloudflareCoordination,
 * } from '@mongolake/abstractions';
 * ```
 *
 * ## Implementing Alternative Backends
 *
 * To add support for a new platform:
 *
 * 1. Implement the relevant interface(s)
 * 2. Register a factory function
 * 3. Update configuration to select the backend
 *
 * @module abstractions
 */

// Re-export all interfaces and types
export {
  type ObjectStorageBackend,
  type ObjectStorageMetadata,
  type ObjectStorageListOptions,
  type ObjectStorageListResult,
  type ObjectStorageObject,
  type MultipartUploadBackend,
  type UploadedPartInfo,
} from './storage.js';

export {
  type CoordinationBackend,
  type CoordinatorState,
  type CoordinatorStub,
  type CoordinatorNamespace,
  type SqlStorage,
  type SqlCursor,
  type AlarmScheduler,
  type CoordinatorOptions,
} from './coordination.js';

export {
  type CacheBackend,
  type CacheGetOptions,
  type CachePutOptions,
  type CacheListOptions,
  type CacheListResult,
} from './cache.js';

// Re-export Cloudflare implementations
export {
  CloudflareR2Storage,
  createCloudflareStorage,
} from './cloudflare/storage.js';

export {
  CloudflareCoordinationBackend,
  CloudflareSqlStorage,
  CloudflareCoordinatorStub,
  CloudflareCoordinatorNamespace,
  createCloudflareCoordination,
  createCloudflareNamespace,
} from './cloudflare/coordination.js';

export {
  CloudflareKVCache,
  createCloudflareCache,
} from './cloudflare/cache.js';
