/**
 * ReplicaShardDO - Read Replica Durable Object
 *
 * A read-only replica that syncs from a primary ShardDO using WAL replication.
 * Provides horizontal read scaling with configurable consistency guarantees.
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
 *      │ ShardDO  │──▶│ ShardDO  │   │ ShardDO  │
 *      └────┬─────┘   └────┬─────┘   └────┬─────┘
 *           │    WAL       │              │
 *           │   Sync       │              │
 *           └──────────────┼──────────────┘
 *                          ▼
 *                    ┌──────────┐
 *                    │    R2    │
 *                    └──────────┘
 * ```
 *
 * ## Features
 *
 * @module do/shard/replica
 * - WAL-based replication from primary
 * - Configurable staleness bounds
 * - Automatic sync scheduling via alarms
 * - Read-only query support
 * - Health monitoring and status reporting
 */

import type { DurableObjectState } from '@cloudflare/workers-types';
import type {
  ReplicaShardDOEnv,
  ReplicaConfig,
  ReplicaFindOptions,
  ReplicaReadResult,
  ReplicationState,
} from './replica-types.js';
import { DEFAULT_REPLICA_CONFIG } from './replica-types.js';
import { ReplicaSyncManager } from './replica-sync-manager.js';
import { ReplicaBuffer } from './replica-buffer.js';
import { matchesFilter } from '../../utils/filter.js';
import { sortDocuments } from '../../utils/sort.js';
import { applyProjection } from '../../utils/projection.js';
import type { Document, Filter } from '../../types.js';
import { MetricsCollector, METRICS } from '../../metrics/index.js';
import { PARQUET_MAGIC_BYTES } from '../../constants.js';
import { logger } from '../../utils/logger.js';

/**
 * ReplicaShardDO is a read-only replica of a primary ShardDO.
 *
 * It continuously syncs WAL entries from the primary and serves
 * read queries with eventual consistency guarantees.
 */
export class ReplicaShardDO {
  /** Durable Object state */
  private state: DurableObjectState;

  /** Environment bindings */
  private env: ReplicaShardDOEnv;

  /** Configuration */
  private config: Required<ReplicaConfig>;

  /** Sync manager for WAL replication */
  private syncManager: ReplicaSyncManager | null = null;

  /** In-memory document buffer */
  private buffer: ReplicaBuffer;

  /** Whether initialization has completed */
  private initialized: boolean = false;

  /** Primary shard ID this replica follows */
  private primaryShardId: string = '';

  /** Metrics collector */
  private metrics: MetricsCollector;

  constructor(state: DurableObjectState, env: ReplicaShardDOEnv) {
    this.state = state;
    this.env = env;
    this.config = { ...DEFAULT_REPLICA_CONFIG };

    // Initialize metrics
    this.metrics = new MetricsCollector({
      analyticsEngine: env.ANALYTICS,
    });

    // Initialize buffer
    this.buffer = new ReplicaBuffer({
      cacheTtlMs: this.config.cacheTtlMs,
    });

    // Auto-initialize on construction
    this.state.blockConcurrencyWhile(async () => {
      await this.initializeInternal();
    });
  }

  // ============================================================================
  // Initialization
  // ============================================================================

  /**
   * Initialize the replica with a primary shard to follow.
   *
   * Must be called before any read operations.
   */
  async initialize(primaryShardId: string): Promise<void> {
    if (this.initialized && this.primaryShardId === primaryShardId) {
      return;
    }

    this.primaryShardId = primaryShardId;
    await this.initializeInternal();
  }

  /**
   * Internal initialization logic.
   */
  private async initializeInternal(): Promise<void> {
    if (!this.primaryShardId) {
      // Try to recover primary ID from storage
      const savedPrimaryId = await this.state.storage.get<string>('primaryShardId');
      if (savedPrimaryId) {
        this.primaryShardId = savedPrimaryId;
      } else {
        // Not yet configured
        return;
      }
    }

    // Save primary ID
    await this.state.storage.put('primaryShardId', this.primaryShardId);

    // Recover applied LSN
    const savedLSN = await this.state.storage.get<number>('appliedLSN');
    if (savedLSN !== undefined) {
      this.buffer.setCurrentLSN(savedLSN);
    }

    // Initialize sync manager
    this.syncManager = new ReplicaSyncManager(
      this.primaryShardId,
      this.env.SHARD_DO,
      this.config
    );

    // Schedule first sync
    await this.scheduleSyncAlarm();

    this.initialized = true;
  }

  // ============================================================================
  // Configuration
  // ============================================================================

  /**
   * Update replica configuration.
   */
  async configure(config: Partial<ReplicaConfig>): Promise<void> {
    this.config = { ...this.config, ...config };

    if (this.syncManager) {
      this.syncManager.configure(config);
    }

    if (config.cacheTtlMs !== undefined) {
      this.buffer.setCacheTtl(config.cacheTtlMs);
    }
  }

  // ============================================================================
  // Read Operations
  // ============================================================================

  /**
   * Find documents matching a filter.
   *
   * Returns documents from the replica's cache with staleness information.
   */
  async find(
    collection: string,
    filter: Record<string, unknown>,
    options: ReplicaFindOptions = {}
  ): Promise<ReplicaReadResult<Record<string, unknown>[]>> {
    this.ensureInitialized();

    const timer = this.metrics.startTimer(METRICS.QUERY_DURATION.name, {
      operation: 'replica_find',
      collection,
    });

    try {
      // Check health
      if (!this.syncManager) {
        throw new Error('Replica not initialized. Call initialize() first.');
      }
      const status = this.syncManager.getStatus();
      const replicationState = this.syncManager.getState();
      const stalenessMs = replicationState.lagMs;

      // Check if staleness is acceptable
      const maxStaleness = options.maxStalenessMs ?? this.config.maxStalenessMs;
      const isStale = stalenessMs > maxStaleness;

      if (isStale && !options.allowStale) {
        throw new Error(
          `Replica too stale: ${stalenessMs}ms lag exceeds max ${maxStaleness}ms`
        );
      }

      // Get documents from buffer
      let documents = this.buffer.getAll(collection);

      // Also check R2 for persisted data
      const r2Docs = await this.readFromR2(collection);

      // Merge buffer and R2 documents, preferring higher LSN
      const merged = this.mergeDocuments(documents, r2Docs);

      // Apply filter
      if (filter && Object.keys(filter).length > 0) {
        merged.forEach((doc, id) => {
          if (!matchesFilter(doc as Document, filter as Filter<Document>)) {
            merged.delete(id);
          }
        });
      }

      let results = Array.from(merged.values());

      // Apply sort
      if (options.sort) {
        results = sortDocuments(results, options.sort);
      }

      // Apply skip
      if (options.skip) {
        results = results.slice(options.skip);
      }

      // Apply limit
      if (options.limit) {
        results = results.slice(0, options.limit);
      }

      // Apply projection
      if (options.projection) {
        results = results.map((doc) => applyProjection(doc, options.projection!));
      }

      timer.end();
      this.metrics.inc(METRICS.FINDS_TOTAL.name, { collection, source: 'replica' });

      return {
        data: results,
        atLSN: this.buffer.getCurrentLSN(),
        stalenessMs,
        isStale,
        status,
      };
    } catch (error) {
      timer.end();
      throw error;
    }
  }

  /**
   * Find a single document matching a filter.
   */
  async findOne(
    collection: string,
    filter: Record<string, unknown>,
    options: ReplicaFindOptions = {}
  ): Promise<ReplicaReadResult<Record<string, unknown> | null>> {
    const result = await this.find(collection, filter, { ...options, limit: 1 });
    return {
      ...result,
      data: result.data[0] || null,
    };
  }

  /**
   * Count documents matching a filter.
   */
  async countDocuments(
    collection: string,
    filter: Record<string, unknown> = {}
  ): Promise<ReplicaReadResult<number>> {
    const result = await this.find(collection, filter);
    return {
      ...result,
      data: result.data.length,
    };
  }

  // ============================================================================
  // Sync Operations
  // ============================================================================

  /**
   * Trigger a manual sync with the primary.
   */
  async sync(): Promise<void> {
    this.ensureInitialized();

    if (!this.syncManager) {
      throw new Error('Replica not initialized. Call initialize() first.');
    }
    const result = await this.syncManager.sync((entry) => {
      this.buffer.applyEntry(entry);
    });

    if (result.success) {
      // Persist applied LSN
      await this.state.storage.put('appliedLSN', result.newLSN);
    }
  }

  /**
   * Durable Object alarm handler for scheduled sync.
   */
  async alarm(): Promise<void> {
    if (!this.initialized || !this.syncManager) {
      return;
    }

    try {
      await this.sync();
    } catch (error) {
      logger.error('Replica sync failed during alarm', {
        operation: 'ReplicaShardDO.alarm',
        replicaId: this.state.id.toString(),
        primaryId: this.primaryShardId,
        error,
      });
    }

    // Schedule next sync
    await this.scheduleSyncAlarm();
  }

  /**
   * Schedule the next sync alarm.
   */
  private async scheduleSyncAlarm(): Promise<void> {
    await this.state.storage.setAlarm(Date.now() + this.config.syncIntervalMs);
  }

  // ============================================================================
  // R2 Operations
  // ============================================================================

  /**
   * Read documents from R2 for a collection.
   */
  private async readFromR2(collection: string): Promise<Map<string, Record<string, unknown>>> {
    const docs = new Map<string, Record<string, unknown>>();

    try {
      // List manifest files for collection
      const manifestPath = `${collection}/_manifest.json`;
      const manifestData = await this.env.DATA_BUCKET.get(manifestPath);

      if (!manifestData) {
        return docs;
      }

      const manifestText = await manifestData.text();
      const manifest = JSON.parse(manifestText) as { files: Array<{ path: string }> };

      // Read each Parquet file
      for (const file of manifest.files) {
        const fileData = await this.env.DATA_BUCKET.get(file.path);
        if (!fileData) continue;

        const buffer = await fileData.arrayBuffer();
        const bytes = new Uint8Array(buffer);
        const parsedDocs = this.parseParquetData(bytes);

        for (const doc of parsedDocs) {
          const id = String(doc._id);
          const seq = (doc as { _seq?: number })._seq ?? 0;
          const existing = docs.get(id);
          const existingSeq = existing ? ((existing as { _seq?: number })._seq ?? 0) : -1;

          // Handle deletions
          if ((doc as { _op?: string })._op === 'd' || (doc as { _deleted?: boolean })._deleted) {
            docs.delete(id);
          } else if (seq > existingSeq) {
            docs.set(id, doc);
          }
        }
      }
    } catch (error) {
      logger.error('Failed to read R2 data for replica', {
        operation: 'ReplicaShardDO.readFromR2',
        collection,
        error,
      });
    }

    return docs;
  }

  /**
   * Parse documents from Parquet/JSON data.
   */
  private parseParquetData(data: Uint8Array): Record<string, unknown>[] {
    if (data.length < 4) return [];

    const magic = new TextDecoder().decode(data.slice(0, 4));
    if (magic !== PARQUET_MAGIC_BYTES) return [];

    try {
      if (data.length < 8) return [];

      const jsonLength = new DataView(data.buffer, data.byteOffset + 4, 4).getUint32(0, true);
      if (jsonLength === 0 || jsonLength > data.length - 8) {
        return [];
      }

      const jsonBytes = data.slice(8, 8 + jsonLength);
      const jsonStr = new TextDecoder().decode(jsonBytes);
      return JSON.parse(jsonStr) as Record<string, unknown>[];
    } catch {
      return [];
    }
  }

  /**
   * Merge buffer and R2 documents, preferring higher sequence numbers.
   */
  private mergeDocuments(
    bufferDocs: Record<string, unknown>[],
    r2Docs: Map<string, Record<string, unknown>>
  ): Map<string, Record<string, unknown>> {
    const merged = new Map(r2Docs);

    for (const doc of bufferDocs) {
      const id = String(doc._id);
      const seq = (doc as { _seq?: number })._seq ?? (doc as { lsn?: number }).lsn ?? 0;
      const existing = merged.get(id);
      const existingSeq = existing
        ? ((existing as { _seq?: number })._seq ?? (existing as { lsn?: number }).lsn ?? 0)
        : -1;

      if (seq > existingSeq) {
        merged.set(id, doc);
      }
    }

    return merged;
  }

  // ============================================================================
  // Status Operations
  // ============================================================================

  /**
   * Get replica status.
   */
  getStatus(): {
    replicaId: string;
    primaryShardId: string;
    initialized: boolean;
    replicationState: ReplicationState | null;
    bufferStats: ReturnType<ReplicaBuffer['getStats']>;
  } {
    return {
      replicaId: this.state.id.toString(),
      primaryShardId: this.primaryShardId,
      initialized: this.initialized,
      replicationState: this.syncManager?.getState() ?? null,
      bufferStats: this.buffer.getStats(),
    };
  }

  /**
   * Get current applied LSN.
   */
  getAppliedLSN(): number {
    return this.buffer.getCurrentLSN();
  }

  // ============================================================================
  // HTTP Interface
  // ============================================================================

  /**
   * Handle HTTP requests to the replica.
   */
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      // Initialize endpoint
      if (path === '/initialize' && request.method === 'POST') {
        const body = await request.json() as { primaryShardId: string };
        await this.initialize(body.primaryShardId);
        return new Response(JSON.stringify({ success: true }), {
          headers: { 'Content-Type': 'application/json' },
        });
      }

      // Query endpoints
      if (path === '/find' && request.method === 'POST') {
        const { collection, filter, ...options } = await request.json() as {
          collection: string;
          filter: Record<string, unknown>;
        } & ReplicaFindOptions;

        const result = await this.find(collection, filter || {}, options);
        return new Response(JSON.stringify(result), {
          headers: { 'Content-Type': 'application/json' },
        });
      }

      if (path === '/findOne' && request.method === 'POST') {
        const { collection, filter, ...options } = await request.json() as {
          collection: string;
          filter: Record<string, unknown>;
        } & ReplicaFindOptions;

        const result = await this.findOne(collection, filter || {}, options);
        return new Response(JSON.stringify(result), {
          headers: { 'Content-Type': 'application/json' },
        });
      }

      // Sync endpoint
      if (path === '/sync' && request.method === 'POST') {
        await this.sync();
        return new Response(JSON.stringify({ success: true }), {
          headers: { 'Content-Type': 'application/json' },
        });
      }

      // Status endpoint
      if (path === '/status' && request.method === 'GET') {
        return new Response(JSON.stringify(this.getStatus()), {
          headers: { 'Content-Type': 'application/json' },
        });
      }

      return new Response(
        JSON.stringify({ error: 'Endpoint not found', path }),
        { status: 404, headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: (error as Error).message }),
        { status: 500, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  // ============================================================================
  // Helpers
  // ============================================================================

  /**
   * Ensure the replica is initialized before operations.
   */
  private ensureInitialized(): void {
    if (!this.initialized || !this.syncManager) {
      throw new Error('Replica not initialized. Call initialize() first.');
    }
  }
}
