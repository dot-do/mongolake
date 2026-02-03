/**
 * Read Replica Worker
 *
 * A read-only worker that serves queries directly from R2/Parquet files
 * without hitting the primary Durable Object. This enables horizontal
 * scaling of read operations.
 *
 * ## Architecture
 *
 * ```
 * ┌─────────────┐     ┌─────────────┐
 * │   Client    │────▶│   Worker    │
 * └─────────────┘     └──────┬──────┘
 *                            │
 *             ┌──────────────┼──────────────┐
 *             ▼              ▼              ▼
 *      ┌──────────┐   ┌──────────┐   ┌──────────┐
 *      │ Primary  │   │ Replica  │   │ Replica  │
 *      │   DO     │   │ (Worker) │   │ (Worker) │
 *      └────┬─────┘   └────┬─────┘   └────┬─────┘
 *           │              │              │
 *           └──────────────┼──────────────┘
 *                          ▼
 *                    ┌──────────┐
 *                    │    R2    │
 *                    └──────────┘
 * ```
 *
 * ## Features
 *
 * - Reads manifest from R2
 * - Queries Parquet files directly
 * - Caches file metadata for performance
 * - Returns results without touching DO
 * - Supports staleness tracking
 */

import type { Document, Filter, R2Bucket, CollectionManifest } from '../types.js';
import { matchesFilter } from '../utils/filter.js';
import { sortDocuments } from '../utils/sort.js';
import { applyProjection } from '../utils/projection.js';
import { PARQUET_MAGIC_BYTES } from '../constants.js';
import { LRUCache } from '../utils/lru-cache.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Read preference options for routing read operations.
 *
 * MongoDB-compatible read preference modes:
 * - primary: Always read from primary (DO). Guaranteed consistency.
 * - secondary: Read from replica only. May be stale.
 * - nearest: Read from lowest-latency node. Best for geo-distributed reads.
 * - primaryPreferred: Read from primary, fallback to replica if unavailable.
 * - secondaryPreferred: Read from replica, fallback to primary if unavailable.
 */
export type ReadPreferenceMode =
  | 'primary'
  | 'secondary'
  | 'nearest'
  | 'primaryPreferred'
  | 'secondaryPreferred';

/**
 * Read preference configuration.
 */
export interface ReadPreference {
  /** The read preference mode */
  mode: ReadPreferenceMode;
  /** Maximum staleness acceptable in milliseconds (for secondary reads) */
  maxStalenessMs?: number;
  /** Tag sets for replica selection (edge colo routing) */
  tagSets?: Array<Record<string, string>>;
}

/**
 * Default read preferences for common use cases.
 */
export const ReadPreferences = {
  /** Always read from primary - strong consistency */
  primary: (): ReadPreference => ({ mode: 'primary' }),
  /** Read from secondary only - eventual consistency */
  secondary: (maxStalenessMs?: number): ReadPreference => ({
    mode: 'secondary',
    maxStalenessMs: maxStalenessMs ?? 5000,
  }),
  /** Read from nearest node - best for latency */
  nearest: (maxStalenessMs?: number): ReadPreference => ({
    mode: 'nearest',
    maxStalenessMs: maxStalenessMs ?? 10000,
  }),
  /** Primary preferred - consistency with fallback */
  primaryPreferred: (maxStalenessMs?: number): ReadPreference => ({
    mode: 'primaryPreferred',
    maxStalenessMs: maxStalenessMs ?? 10000,
  }),
  /** Secondary preferred - performance with fallback */
  secondaryPreferred: (maxStalenessMs?: number): ReadPreference => ({
    mode: 'secondaryPreferred',
    maxStalenessMs: maxStalenessMs ?? 5000,
  }),
} as const;

/**
 * Consistency level for read operations.
 *
 * - eventual: Accept potentially stale data for best performance
 * - session: Read-your-writes consistency within a session
 * - bounded: Accept data within a staleness bound
 * - strong: Always read the latest committed data
 */
export type ConsistencyLevel = 'eventual' | 'session' | 'bounded' | 'strong';

/**
 * Consistency options for read operations.
 */
export interface ConsistencyOptions {
  /** The consistency level */
  level: ConsistencyLevel;
  /** For session consistency, the token from the last write */
  afterToken?: string;
  /** For bounded staleness, the maximum acceptable lag in milliseconds */
  maxStalenessMs?: number;
}

/**
 * Default consistency options for common use cases.
 */
export const ConsistencyLevels = {
  /** Eventual consistency - best performance */
  eventual: (): ConsistencyOptions => ({ level: 'eventual' }),
  /** Session consistency - read-your-writes */
  session: (afterToken: string): ConsistencyOptions => ({
    level: 'session',
    afterToken,
  }),
  /** Bounded staleness - configurable lag */
  bounded: (maxStalenessMs: number): ConsistencyOptions => ({
    level: 'bounded',
    maxStalenessMs,
  }),
  /** Strong consistency - always latest */
  strong: (): ConsistencyOptions => ({ level: 'strong' }),
} as const;

/** Replica configuration */
export interface ReplicaConfig {
  /** Enable read replicas */
  enabled: boolean;
  /** Number of replica workers (for round-robin selection) */
  count: number;
  /** Maximum staleness in milliseconds (default: 5000) */
  maxStaleness: number;
  /** Cache TTL for manifest in milliseconds (default: 1000) */
  manifestCacheTtl: number;
  /** Cache TTL for file metadata in milliseconds (default: 5000) */
  fileMetadataCacheTtl: number;
}

/** Default replica configuration */
export const DEFAULT_REPLICA_CONFIG: ReplicaConfig = {
  enabled: false,
  count: 1,
  maxStaleness: 5000,
  manifestCacheTtl: 1000,
  fileMetadataCacheTtl: 5000,
};

/** Cached manifest with timestamp */
interface CachedManifest {
  manifest: CollectionManifest;
  fetchedAt: number;
}

/** Find options for replica queries */
export interface ReplicaFindOptions {
  filter?: Record<string, unknown>;
  projection?: Record<string, 0 | 1>;
  sort?: Record<string, 1 | -1>;
  limit?: number;
  skip?: number;
  /** Maximum staleness acceptable in milliseconds */
  maxStaleness?: number;
}

/** Replica find result */
export interface ReplicaFindResult {
  documents: Record<string, unknown>[];
  /** Staleness of the data in milliseconds */
  stalenessMs: number;
  /** Whether the result may be stale */
  isStale: boolean;
  /** Manifest version/timestamp used */
  manifestTimestamp: number;
}

/** Replica status */
export interface ReplicaStatus {
  /** Replica identifier */
  replicaId: string;
  /** Whether the replica is healthy */
  healthy: boolean;
  /** Last successful manifest fetch time */
  lastManifestFetch: number;
  /** Number of cached manifests */
  cachedManifests: number;
  /** Current staleness in milliseconds */
  currentStalenessMs: number;
}


// ============================================================================
// Read Replica Implementation
// ============================================================================

/**
 * ReadReplica handles read-only queries directly from R2 storage.
 *
 * It maintains a cache of collection manifests and file metadata to
 * minimize R2 operations while providing configurable staleness guarantees.
 */
export class ReadReplica {
  private replicaId: string;
  private config: ReplicaConfig;
  private bucket: R2Bucket;

  /** Cache for collection manifests */
  private manifestCache: Map<string, CachedManifest> = new Map();

  /** LRU cache for parsed Parquet file contents */
  private fileContentCache: LRUCache<string, Record<string, unknown>[]>;

  /** Last successful manifest fetch timestamp (for staleness tracking) */
  private lastManifestFetch: number = 0;

  constructor(replicaId: string, bucket: R2Bucket, config: Partial<ReplicaConfig> = {}) {
    this.replicaId = replicaId;
    this.bucket = bucket;
    this.config = { ...DEFAULT_REPLICA_CONFIG, ...config };

    // Initialize LRU cache with 100 entries max
    this.fileContentCache = new LRUCache<string, Record<string, unknown>[]>({ maxSize: 100 });
  }

  // ============================================================================
  // Query Operations
  // ============================================================================

  /**
   * Find documents from R2 storage.
   *
   * Queries manifests and Parquet files directly without touching the DO.
   * Returns documents with staleness information.
   */
  async find(
    database: string,
    collection: string,
    options: ReplicaFindOptions = {}
  ): Promise<ReplicaFindResult> {
    const { filter, projection, sort, limit, skip, maxStaleness } = options;

    // Get manifest (from cache or R2)
    const manifestResult = await this.getManifest(database, collection);
    const { manifest, stalenessMs } = manifestResult;

    // Check staleness against configured or requested max
    const effectiveMaxStaleness = maxStaleness ?? this.config.maxStaleness;
    const isStale = stalenessMs > effectiveMaxStaleness;

    // If manifest doesn't exist or has no files, return empty result
    if (!manifest || manifest.files.length === 0) {
      return {
        documents: [],
        stalenessMs,
        isStale,
        manifestTimestamp: manifest?.updatedAt ? new Date(manifest.updatedAt).getTime() : 0,
      };
    }

    // Read documents from all Parquet files
    const allDocs: Record<string, unknown>[] = [];
    const tombstoneIds = new Set<string>();

    for (const file of manifest.files) {
      const docs = await this.readParquetFile(file.path);

      for (const doc of docs) {
        if ((doc as { _op?: string })._op === 'd' || (doc as { _deleted?: boolean })._deleted) {
          tombstoneIds.add(String(doc._id));
        } else {
          allDocs.push(doc);
        }
      }
    }

    // Deduplicate by _id, keeping latest version (highest _seq)
    const docsById = new Map<string, Record<string, unknown>>();
    for (const doc of allDocs) {
      const id = String(doc._id);
      if (tombstoneIds.has(id)) continue;

      const existing = docsById.get(id);
      const docSeq = (doc as { _seq?: number })._seq ?? 0;
      const existingSeq = existing ? ((existing as { _seq?: number })._seq ?? 0) : -1;

      if (!existing || docSeq > existingSeq) {
        docsById.set(id, doc);
      }
    }

    // Apply filter
    let results = Array.from(docsById.values());
    if (filter && Object.keys(filter).length > 0) {
      results = results.filter((doc) =>
        matchesFilter(doc as Document, filter as Filter<Document>)
      );
    }

    // Apply sort
    if (sort) {
      results = sortDocuments(results, sort);
    }

    // Apply skip
    if (skip) {
      results = results.slice(skip);
    }

    // Apply limit
    if (limit) {
      results = results.slice(0, limit);
    }

    // Apply projection
    if (projection) {
      results = results.map((doc) => applyProjection(doc, projection));
    }

    return {
      documents: results,
      stalenessMs,
      isStale,
      manifestTimestamp: manifest.updatedAt ? new Date(manifest.updatedAt).getTime() : 0,
    };
  }

  /**
   * Find a single document.
   */
  async findOne(
    database: string,
    collection: string,
    options: ReplicaFindOptions = {}
  ): Promise<{ document: Record<string, unknown> | null; stalenessMs: number; isStale: boolean }> {
    const result = await this.find(database, collection, { ...options, limit: 1 });
    return {
      document: result.documents[0] || null,
      stalenessMs: result.stalenessMs,
      isStale: result.isStale,
    };
  }

  /**
   * Count documents matching a filter.
   */
  async countDocuments(
    database: string,
    collection: string,
    filter?: Record<string, unknown>
  ): Promise<{ count: number; stalenessMs: number; isStale: boolean }> {
    const result = await this.find(database, collection, { filter });
    return {
      count: result.documents.length,
      stalenessMs: result.stalenessMs,
      isStale: result.isStale,
    };
  }

  // ============================================================================
  // Manifest Operations
  // ============================================================================

  /**
   * Get manifest from cache or R2.
   *
   * Returns the manifest along with staleness information.
   */
  private async getManifest(
    database: string,
    collection: string
  ): Promise<{ manifest: CollectionManifest | null; stalenessMs: number }> {
    const cacheKey = `${database}/${collection}`;
    const now = Date.now();

    // Check cache
    const cached = this.manifestCache.get(cacheKey);
    if (cached) {
      const age = now - cached.fetchedAt;
      if (age <= this.config.manifestCacheTtl) {
        return {
          manifest: cached.manifest,
          stalenessMs: age,
        };
      }
    }

    // Fetch from R2
    const manifestPath = `${database}/${collection}/_manifest.json`;
    try {
      const data = await this.bucket.get(manifestPath);
      if (!data) {
        return { manifest: null, stalenessMs: 0 };
      }

      const text = await data.text();
      const manifest = JSON.parse(text) as CollectionManifest;

      // Update cache
      this.manifestCache.set(cacheKey, {
        manifest,
        fetchedAt: now,
      });

      this.lastManifestFetch = now;

      return {
        manifest,
        stalenessMs: 0,
      };
    } catch (error) {
      // If fetch fails and we have stale cache, use it
      if (cached) {
        return {
          manifest: cached.manifest,
          stalenessMs: now - cached.fetchedAt,
        };
      }
      throw error;
    }
  }

  /**
   * Refresh manifest cache for a collection.
   *
   * Call this to force a refresh of the manifest.
   */
  async refreshManifest(database: string, collection: string): Promise<void> {
    const cacheKey = `${database}/${collection}`;
    this.manifestCache.delete(cacheKey);
    await this.getManifest(database, collection);
  }

  /**
   * Clear all manifest caches.
   */
  clearManifestCache(): void {
    this.manifestCache.clear();
  }

  // ============================================================================
  // Parquet File Operations
  // ============================================================================

  /**
   * Read and parse a Parquet file from R2.
   *
   * Uses caching to minimize R2 reads.
   */
  private async readParquetFile(path: string): Promise<Record<string, unknown>[]> {
    // Check cache first
    const cached = this.fileContentCache.get(path);
    if (cached !== undefined) {
      return cached;
    }

    // Read from R2
    const data = await this.bucket.get(path);
    if (!data) {
      return [];
    }

    const buffer = await data.arrayBuffer();
    const bytes = new Uint8Array(buffer);

    // Parse the Parquet data
    const docs = this.parseParquetData(bytes);

    // Cache the result
    this.fileContentCache.set(path, docs);

    return docs;
  }

  /**
   * Parse documents from a combined Parquet/JSON data block.
   *
   * Data format: [PAR1:4][json_length:4][json_data][parquet_data_without_magic]
   */
  private parseParquetData(data: Uint8Array): Record<string, unknown>[] {
    // Verify Parquet magic bytes (PAR1)
    if (data.length < 4) {
      return [];
    }

    const magic = new TextDecoder().decode(data.slice(0, 4));
    if (magic !== PARQUET_MAGIC_BYTES) {
      return [];
    }

    try {
      if (data.length < 8) {
        return [];
      }

      // Read JSON length from bytes 4-7
      const jsonLength = new DataView(data.buffer, data.byteOffset + 4, 4).getUint32(0, true);

      // Validate JSON length is reasonable
      if (jsonLength === 0 || jsonLength > data.length - 8) {
        // Fall back to legacy parsing for old format data
        return this.parseParquetDataLegacy(data);
      }

      // Extract and parse JSON documents
      const jsonBytes = data.slice(8, 8 + jsonLength);
      const jsonStr = new TextDecoder().decode(jsonBytes);
      const docs = JSON.parse(jsonStr) as Record<string, unknown>[];
      return docs;
    } catch {
      // If any error occurs, try legacy fallback
      return this.parseParquetDataLegacy(data);
    }
  }

  /**
   * Legacy parser for older Parquet format without JSON header.
   */
  private parseParquetDataLegacy(data: Uint8Array): Record<string, unknown>[] {
    const results: Record<string, unknown>[] = [];

    try {
      const text = new TextDecoder().decode(data);
      // Match simple JSON objects containing _id field
      const jsonMatches = text.match(/\{[^{}]*"_id"[^{}]*\}/g);

      if (jsonMatches) {
        for (const match of jsonMatches) {
          try {
            const doc = JSON.parse(match);
            if (doc._id) {
              results.push(doc);
            }
          } catch {
            continue;
          }
        }
      }
    } catch {
      return [];
    }

    return results;
  }

  /**
   * Clear file content cache.
   */
  clearFileCache(): void {
    this.fileContentCache.clear();
  }

  // ============================================================================
  // Status Operations
  // ============================================================================

  /**
   * Get replica status.
   */
  getStatus(): ReplicaStatus {
    const now = Date.now();
    const currentStalenessMs = this.lastManifestFetch > 0 ? now - this.lastManifestFetch : 0;

    return {
      replicaId: this.replicaId,
      healthy: currentStalenessMs <= this.config.maxStaleness * 2,
      lastManifestFetch: this.lastManifestFetch,
      cachedManifests: this.manifestCache.size,
      currentStalenessMs,
    };
  }

  /**
   * Get replica configuration.
   */
  getConfig(): ReplicaConfig {
    return { ...this.config };
  }

  /**
   * Update replica configuration.
   */
  updateConfig(config: Partial<ReplicaConfig>): void {
    this.config = { ...this.config, ...config };
  }
}

// ============================================================================
// Replica Pool
// ============================================================================

/**
 * ReplicaPool manages multiple read replicas for load balancing.
 *
 * Provides round-robin or random selection of replicas for distributing
 * read load across multiple worker instances.
 */
export class ReplicaPool {
  private replicas: ReadReplica[] = [];
  private currentIndex: number = 0;
  private config: ReplicaConfig;

  constructor(bucket: R2Bucket, config: Partial<ReplicaConfig> = {}) {
    this.config = { ...DEFAULT_REPLICA_CONFIG, ...config };

    // Create replicas
    for (let i = 0; i < this.config.count; i++) {
      this.replicas.push(new ReadReplica(`replica-${i}`, bucket, this.config));
    }
  }

  /**
   * Get the next replica using round-robin selection.
   */
  getNextReplica(): ReadReplica {
    if (this.replicas.length === 0) {
      throw new Error('No replicas available');
    }

    const replica = this.replicas[this.currentIndex]!;
    this.currentIndex = (this.currentIndex + 1) % this.replicas.length;
    return replica;
  }

  /**
   * Get a random replica using cryptographically secure randomness.
   */
  getRandomReplica(): ReadReplica {
    if (this.replicas.length === 0) {
      throw new Error('No replicas available');
    }

    // Use crypto.getRandomValues() for better distribution than Math.random()
    const array = new Uint32Array(1);
    crypto.getRandomValues(array);
    const index = array[0]! % this.replicas.length;
    return this.replicas[index]!;
  }

  /**
   * Get a specific replica by index.
   */
  getReplica(index: number): ReadReplica {
    if (index < 0 || index >= this.replicas.length) {
      throw new Error(`Invalid replica index: ${index}`);
    }
    return this.replicas[index]!;
  }

  /**
   * Get all replicas.
   */
  getAllReplicas(): ReadReplica[] {
    return [...this.replicas];
  }

  /**
   * Get status of all replicas.
   */
  getPoolStatus(): { replicas: ReplicaStatus[]; healthy: number; unhealthy: number } {
    const statuses = this.replicas.map((r) => r.getStatus());
    const healthy = statuses.filter((s) => s.healthy).length;
    const unhealthy = statuses.length - healthy;

    return {
      replicas: statuses,
      healthy,
      unhealthy,
    };
  }

  /**
   * Refresh manifests on all replicas.
   */
  async refreshAllManifests(database: string, collection: string): Promise<void> {
    await Promise.all(this.replicas.map((r) => r.refreshManifest(database, collection)));
  }

  /**
   * Clear all caches on all replicas.
   */
  clearAllCaches(): void {
    for (const replica of this.replicas) {
      replica.clearManifestCache();
      replica.clearFileCache();
    }
  }

  /**
   * Check if replica pool is enabled.
   */
  isEnabled(): boolean {
    return this.config.enabled;
  }
}

// ============================================================================
// Read Router
// ============================================================================

/**
 * Result from a routed read operation.
 */
export interface ReadRouterResult {
  /** The documents returned */
  documents: Record<string, unknown>[];
  /** Whether the read was served from a replica */
  fromReplica: boolean;
  /** Staleness information if from replica */
  stalenessMs?: number;
  /** Whether the data may be stale */
  isStale?: boolean;
  /** The source of the read (for debugging) */
  source: 'primary' | 'replica' | 'fallback';
}

/**
 * ReadRouter handles routing read operations based on read preference
 * and consistency requirements.
 *
 * It coordinates between the primary Durable Object (for strong consistency)
 * and read replicas (for horizontal scaling).
 *
 * ## Usage
 *
 * ```typescript
 * const router = new ReadRouter(pool, primaryFetcher);
 *
 * // Route with read preference
 * const result = await router.route('testdb', 'users', {
 *   readPreference: ReadPreferences.secondaryPreferred(5000),
 *   consistency: ConsistencyLevels.eventual(),
 *   filter: { active: true },
 * });
 * ```
 */
export class ReadRouter {
  constructor(
    private pool: ReplicaPool,
    private primaryFetcher: (
      database: string,
      collection: string,
      options: ReplicaFindOptions
    ) => Promise<Record<string, unknown>[]>
  ) {}

  /**
   * Route a read operation based on read preference and consistency.
   *
   * @param database - The database name
   * @param collection - The collection name
   * @param options - Read options including preference and consistency
   */
  async route(
    database: string,
    collection: string,
    options: {
      readPreference?: ReadPreference;
      consistency?: ConsistencyOptions;
      filter?: Record<string, unknown>;
      projection?: Record<string, 0 | 1>;
      sort?: Record<string, 1 | -1>;
      limit?: number;
      skip?: number;
    } = {}
  ): Promise<ReadRouterResult> {
    const {
      readPreference = ReadPreferences.primary(),
      consistency = ConsistencyLevels.eventual(),
      ...findOptions
    } = options;

    // Strong consistency or primary read preference always goes to primary
    if (consistency.level === 'strong' || readPreference.mode === 'primary') {
      return this.readFromPrimary(database, collection, findOptions);
    }

    // Session consistency requires checking against afterToken
    if (consistency.level === 'session' && consistency.afterToken) {
      // For session consistency, we need to ensure the replica has caught up
      // to the write represented by the afterToken
      // For now, fall back to primary for session consistency
      return this.readFromPrimary(database, collection, findOptions);
    }

    // Check if pool is enabled
    if (!this.pool.isEnabled()) {
      return this.readFromPrimary(database, collection, findOptions);
    }

    // Determine max staleness from preference and consistency
    const maxStaleness = this.resolveMaxStaleness(readPreference, consistency);

    // Route based on read preference mode
    switch (readPreference.mode) {
      case 'secondary':
        return this.readFromSecondary(database, collection, findOptions, maxStaleness);

      case 'nearest':
        return this.readFromNearest(database, collection, findOptions, maxStaleness);

      case 'primaryPreferred':
        return this.readFromPrimaryPreferred(database, collection, findOptions, maxStaleness);

      case 'secondaryPreferred':
        return this.readFromSecondaryPreferred(database, collection, findOptions, maxStaleness);

      default:
        return this.readFromPrimary(database, collection, findOptions);
    }
  }

  /**
   * Read from primary (Durable Object).
   */
  private async readFromPrimary(
    database: string,
    collection: string,
    options: ReplicaFindOptions
  ): Promise<ReadRouterResult> {
    const documents = await this.primaryFetcher(database, collection, options);
    return {
      documents,
      fromReplica: false,
      source: 'primary',
    };
  }

  /**
   * Read from secondary replica only.
   */
  private async readFromSecondary(
    database: string,
    collection: string,
    options: ReplicaFindOptions,
    maxStaleness: number
  ): Promise<ReadRouterResult> {
    const replica = this.pool.getNextReplica();
    const result = await replica.find(database, collection, {
      ...options,
      maxStaleness,
    });

    return {
      documents: result.documents,
      fromReplica: true,
      stalenessMs: result.stalenessMs,
      isStale: result.isStale,
      source: 'replica',
    };
  }

  /**
   * Read from nearest replica (lowest latency).
   * Currently uses round-robin; can be enhanced with latency tracking.
   */
  private async readFromNearest(
    database: string,
    collection: string,
    options: ReplicaFindOptions,
    maxStaleness: number
  ): Promise<ReadRouterResult> {
    // For now, use random selection as latency tracking is not implemented
    const replica = this.pool.getRandomReplica();
    const result = await replica.find(database, collection, {
      ...options,
      maxStaleness,
    });

    return {
      documents: result.documents,
      fromReplica: true,
      stalenessMs: result.stalenessMs,
      isStale: result.isStale,
      source: 'replica',
    };
  }

  /**
   * Read from primary, fallback to replica if unavailable.
   */
  private async readFromPrimaryPreferred(
    database: string,
    collection: string,
    options: ReplicaFindOptions,
    maxStaleness: number
  ): Promise<ReadRouterResult> {
    try {
      return await this.readFromPrimary(database, collection, options);
    } catch {
      // Fallback to replica
      const replica = this.pool.getNextReplica();
      const result = await replica.find(database, collection, {
        ...options,
        maxStaleness,
      });

      return {
        documents: result.documents,
        fromReplica: true,
        stalenessMs: result.stalenessMs,
        isStale: result.isStale,
        source: 'fallback',
      };
    }
  }

  /**
   * Read from replica, fallback to primary if unhealthy.
   */
  private async readFromSecondaryPreferred(
    database: string,
    collection: string,
    options: ReplicaFindOptions,
    maxStaleness: number
  ): Promise<ReadRouterResult> {
    // Check pool health first
    const poolStatus = this.pool.getPoolStatus();

    if (poolStatus.healthy > 0) {
      // Use a healthy replica
      const replica = this.pool.getNextReplica();
      const result = await replica.find(database, collection, {
        ...options,
        maxStaleness,
      });

      // If result is too stale, fallback to primary
      if (result.isStale) {
        return this.readFromPrimary(database, collection, options);
      }

      return {
        documents: result.documents,
        fromReplica: true,
        stalenessMs: result.stalenessMs,
        isStale: result.isStale,
        source: 'replica',
      };
    }

    // No healthy replicas, fallback to primary
    return this.readFromPrimary(database, collection, options);
  }

  /**
   * Resolve the effective max staleness from preference and consistency.
   */
  private resolveMaxStaleness(
    preference: ReadPreference,
    consistency: ConsistencyOptions
  ): number {
    // Bounded staleness takes precedence
    if (consistency.level === 'bounded' && consistency.maxStalenessMs !== undefined) {
      return consistency.maxStalenessMs;
    }

    // Use preference staleness
    if (preference.maxStalenessMs !== undefined) {
      return preference.maxStalenessMs;
    }

    // Default staleness
    return DEFAULT_REPLICA_CONFIG.maxStaleness;
  }
}

// ============================================================================
// Edge Colo Integration
// ============================================================================

/**
 * Information about the current Cloudflare edge colo.
 */
export interface ColoInfo {
  /** The colo ID (e.g., 'SJC', 'AMS') */
  coloId: string;
  /** The country code */
  country: string;
  /** The continent code */
  continent: string;
  /** The city name */
  city: string;
  /** Latitude coordinate */
  latitude: number;
  /** Longitude coordinate */
  longitude: number;
}

/**
 * Cloudflare-extended Request type with cf object
 */
interface CloudflareRequest extends Request {
  cf?: {
    colo?: string;
    country?: string;
    continent?: string;
    city?: string;
    latitude?: number;
    longitude?: number;
  };
}

/**
 * Extract colo information from Cloudflare request headers.
 *
 * @param request - The incoming request with CF headers
 * @returns Colo information or null if not available
 */
export function extractColoInfo(request: Request): ColoInfo | null {
  // Cloudflare Workers expose colo info in cf object
  const cfRequest = request as CloudflareRequest;
  const cf = cfRequest.cf;

  if (!cf || !cf.colo) {
    return null;
  }

  return {
    coloId: cf.colo,
    country: cf.country || 'unknown',
    continent: cf.continent || 'unknown',
    city: cf.city || 'unknown',
    latitude: cf.latitude ?? 0,
    longitude: cf.longitude ?? 0,
  };
}

/**
 * Select a replica based on colo affinity.
 *
 * This can be used to route reads to the nearest replica
 * based on Cloudflare's edge network topology.
 *
 * @param pool - The replica pool
 * @param coloInfo - The current colo information
 * @returns A replica (currently uses round-robin, can be enhanced)
 */
export function selectReplicaByColo(
  pool: ReplicaPool,
  _coloInfo: ColoInfo | null
): ReadReplica {
  // For now, use round-robin
  // Future enhancement: use tagSets in ReadPreference to select by colo
  return pool.getNextReplica();
}
