/**
 * ShardDO - Shard Durable Object
 *
 * The ShardDO is the write coordinator for a shard in MongoLake. It provides
 * a MongoDB-like document storage interface built on Cloudflare's Durable Objects,
 * R2 object storage, and SQLite.
 *
 * This module composes the following focused modules:
 * - WalManager: WAL operations and durability
 * - BufferManager: In-memory buffer management
 * - IndexManager: Manifest and file operations
 * - QueryExecutor: Query execution
 * - CompactionServiceWithBucket: Background compaction
 *
 * @see types.ts for shared type definitions
 */

import type { DurableObjectState } from '@cloudflare/workers-types';

// Import types and re-export for external consumers
export { DurableObjectStorageBackend } from './types.js';

// Import metrics
import {
  MetricsCollector,
  METRICS,
} from '../../metrics/index.js';
export type {
  ShardDOEnv,
  ShardConfig,
  WriteOperation,
  WriteResult,
  ReadToken,
  FindOptions,
  FileMetadata,
  PendingFlush,
  CollectionManifest,
  BufferedDoc,
  WalEntry,
  StorageBackend,
  SqlCursor,
} from './types.js';

import type {
  ShardDOEnv,
  ShardConfig,
  WriteOperation,
  WriteResult,
  FindOptions,
  CollectionManifest,
  BufferedDoc,
  ReadToken,
} from './types.js';

import { DurableObjectStorageBackend } from './types.js';
import { WalManager } from './wal-manager.js';
import { BufferManager } from './buffer-manager.js';
import { IndexManager } from './index-manager.js';
import { QueryExecutor } from './query-executor.js';
import { CompactionServiceWithBucket, type CompactionBucket } from './compaction-service.js';
import { applyUpdate as applyUpdateOperators } from '../../utils/update.js';
import {
  validateCollectionName,
  validateFilter,
  validateUpdate,
  validateDocument,
  ValidationError,
} from '../../validation/index.js';
import { logger } from '../../utils/logger.js';

// ============================================================================
// ShardDO Implementation
// ============================================================================

/**
 * ShardDO is the core Durable Object class for MongoLake shard management.
 *
 * Each ShardDO instance manages a partition of the data, handling writes,
 * queries, flushing to R2, and background compaction.
 *
 * ## Architecture
 *
 * ShardDO implements a Log-Structured Merge (LSM) tree-inspired architecture:
 * - **In-memory buffer**: Fast writes accumulate in memory (BufferManager)
 * - **Write-Ahead Log (WAL)**: SQLite-backed durability (WalManager)
 * - **R2 Storage**: Parquet files for persistent storage (IndexManager)
 * - **Compaction**: Background process merges files (CompactionService)
 * - **Queries**: Merged buffer + R2 results (QueryExecutor)
 */
export class ShardDO {
  /** Durable Object state providing storage and identity */
  private state: DurableObjectState;

  /** Environment bindings (R2 bucket, DO namespace) */
  private env: ShardDOEnv;

  // ============================================================================
  // Module Instances
  // ============================================================================

  /** Storage backend adapter */
  private storageBackend: DurableObjectStorageBackend;

  /** WAL manager for durability */
  private walManager: WalManager;

  /** Buffer manager for in-memory documents */
  private bufferManager: BufferManager;

  /** Index manager for manifests and files */
  private indexManager: IndexManager;

  /** Query executor for document queries */
  private queryExecutor: QueryExecutor;

  /** Compaction service for background merging */
  private compactionService: CompactionServiceWithBucket;

  // ============================================================================
  // State
  // ============================================================================

  /** Whether initialization has completed */
  private initialized: boolean = false;

  /** Mutex to serialize concurrent write operations */
  private writeMutex: Promise<void> = Promise.resolve();

  /** Metrics collector for observability */
  private metrics: MetricsCollector;

  // ============================================================================
  // Constructor
  // ============================================================================

  /**
   * Creates a new ShardDO instance.
   *
   * Automatically initializes SQLite tables and recovers state from
   * previous runs using `blockConcurrencyWhile` to prevent concurrent
   * access during initialization.
   */
  constructor(state: DurableObjectState, env: ShardDOEnv) {
    this.state = state;
    this.env = env;

    // Initialize metrics collector with optional Analytics Engine
    this.metrics = new MetricsCollector({
      analyticsEngine: env.ANALYTICS,
    });

    // Initialize storage backend adapter
    this.storageBackend = new DurableObjectStorageBackend(state);

    // Initialize modules
    this.walManager = new WalManager(this.storageBackend);
    this.bufferManager = new BufferManager();
    this.indexManager = new IndexManager(this.storageBackend, env.DATA_BUCKET);

    // Initialize query executor with callbacks
    this.queryExecutor = new QueryExecutor(
      this.bufferManager,
      this.indexManager,
      () => this.state.id.toString(),
      () => this.walManager.getCurrentLSN()
    );

    // Initialize compaction service
    // DATA_BUCKET is an R2Bucket which satisfies the CompactionBucket interface
    const bucket: CompactionBucket = env.DATA_BUCKET;
    this.compactionService = new CompactionServiceWithBucket(
      this.indexManager,
      bucket,
      async (timestamp: number) => {
        await this.state.storage.setAlarm(timestamp);
      }
    );

    // Auto-initialize on construction
    this.state.blockConcurrencyWhile(async () => {
      await this.initializeInternal();
    });
  }

  // ============================================================================
  // Initialization
  // ============================================================================

  /**
   * Explicitly initialize the shard.
   *
   * This method is idempotent - calling it multiple times has no effect
   * after the first successful initialization.
   */
  async initialize(): Promise<void> {
    if (this.initialized) return;
    await this.initializeInternal();
  }

  /**
   * Internal initialization logic.
   */
  private async initializeInternal(): Promise<void> {
    if (this.initialized) return;

    // Initialize WAL tables in SQLite
    this.walManager.initializeTables();

    // Recover state from SQLite
    const manifests = this.walManager.recoverState((doc) => {
      this.bufferManager.addToBuffer(doc);
    });

    // Set manifests in index manager
    this.indexManager.setManifests(manifests);

    // Recover pending flushes from R2
    await this.walManager.recoverPendingFlushes(this.env.DATA_BUCKET, manifests);

    this.initialized = true;
  }

  // ============================================================================
  // Configuration
  // ============================================================================

  /**
   * Update shard configuration options.
   */
  async configure(config: ShardConfig): Promise<void> {
    this.bufferManager.configure(config);
  }

  // ============================================================================
  // Write Operations
  // ============================================================================

  /**
   * Execute a write operation (insert, update, or delete).
   *
   * Writes are processed atomically with the following guarantees:
   * 1. LSN is assigned and persisted to SQLite WAL before acknowledgment
   * 2. Document is added to in-memory buffer for immediate read visibility
   * 3. Buffer may be auto-flushed to R2 if thresholds are exceeded
   *
   * This method orchestrates the write flow by delegating to focused helper methods:
   * - acquireWriteLock(): Mutex acquisition for serialization
   * - allocateLSN(): LSN allocation via WAL manager
   * - persistToWal(): WAL persistence for durability
   * - updateBuffer(): Buffer management for read visibility
   * - recordMetrics(): Metrics recording for observability
   * - checkAutoFlush(): Auto-flush logic for buffer management
   */
  async write(op: WriteOperation): Promise<WriteResult> {
    this.validateWriteOperation(op);

    // Start timing for metrics
    const timer = this.metrics.startTimer(METRICS.QUERY_DURATION.name, {
      operation: op.op,
      collection: op.collection,
    });

    // Acquire write lock for serialization
    const releaseLock = await this.acquireWriteLock();

    try {
      return await this.state.blockConcurrencyWhile(async () => {
        // Allocate LSN for this write
        const lsn = this.allocateLSN();

        // Extract document and ID based on operation type
        const { docId, document } = await this.extractDocumentAndId(op);

        // Create buffered document
        const bufferedDoc: BufferedDoc = {
          _id: docId,
          _seq: lsn,
          _op: this.operationTypeToCode(op.op),
          collection: op.collection,
          document,
          lsn,
        };

        // Persist to WAL for durability
        this.persistToWal(bufferedDoc);

        // Update in-memory buffer
        this.updateBuffer(bufferedDoc);

        // Record metrics
        this.recordMetrics(op, timer);

        // Check if auto-flush is needed
        await this.checkAutoFlush();

        return {
          acknowledged: true,
          insertedId: op.op === 'insert' ? docId : undefined,
          lsn,
          readToken: this.queryExecutor.generateReadToken(lsn),
        };
      });
    } catch (error) {
      // Record error metrics
      timer.end();
      this.metrics.inc(METRICS.QUERY_COUNT.name, {
        operation: op.op,
        collection: op.collection,
        status: 'error',
      });
      throw error;
    } finally {
      releaseLock();
    }
  }

  /**
   * Acquire the write mutex for serialization of concurrent writes.
   * Returns a function to release the lock when done.
   */
  private async acquireWriteLock(): Promise<() => void> {
    const previousMutex = this.writeMutex;
    let resolveWriteMutex: () => void;
    this.writeMutex = new Promise<void>((resolve) => {
      resolveWriteMutex = resolve;
    });
    await previousMutex;
    return () => resolveWriteMutex();
  }

  /**
   * Allocate a new LSN for a write operation.
   */
  private allocateLSN(): number {
    return this.walManager.allocateLSN();
  }

  /**
   * Extract document and ID from a write operation.
   */
  private async extractDocumentAndId(op: WriteOperation): Promise<{
    docId: string;
    document: Record<string, unknown>;
  }> {
    if (op.op === 'insert') {
      const document = op.document!;
      return { docId: String(document._id), document };
    }
    // For update/delete, apply operators or mark as deleted
    if (!op.filter || op.filter._id === undefined) {
      throw new Error('Update/delete operations require a filter with _id');
    }
    const docId = String(op.filter._id);
    const document = await this.applyUpdate(op.collection, docId, op);
    return { docId, document };
  }

  /**
   * Persist a buffered document to the WAL for durability.
   */
  private persistToWal(bufferedDoc: BufferedDoc): void {
    const walEntry = this.walManager.createWalEntry(bufferedDoc);
    this.walManager.persistEntry(walEntry);
  }

  /**
   * Update the in-memory buffer with a new document.
   * Returns true if back-pressure threshold was exceeded.
   */
  private updateBuffer(bufferedDoc: BufferedDoc): boolean {
    const exceedsMaxBytes = this.bufferManager.addToBuffer(bufferedDoc);

    // Update buffer metrics
    const shardId = this.state.id.toString();
    this.metrics.set(METRICS.BUFFER_SIZE.name, this.bufferManager.getBufferSize(), { shard: shardId });
    this.metrics.set(METRICS.BUFFER_DOCS.name, this.bufferManager.getBufferDocCount(), { shard: shardId });

    // Update WAL metrics
    this.metrics.set(METRICS.WAL_SIZE_BYTES.name, this.walManager.getWalSizeBytes(), { shard: shardId });
    this.metrics.set(METRICS.WAL_ENTRIES.name, this.walManager.getWalEntryCount(), { shard: shardId });

    return exceedsMaxBytes;
  }

  /**
   * Record metrics for a write operation.
   * Returns the duration of the operation.
   */
  private recordMetrics(
    op: WriteOperation,
    timer: { end: () => number }
  ): number {
    this.metrics.recordWrite(op.op, op.collection);
    const duration = timer.end();
    this.metrics.inc(METRICS.QUERY_COUNT.name, {
      operation: op.op,
      collection: op.collection,
      status: 'success',
    });

    // Check for slow query (duration > 100ms)
    if (duration * 1000 > 100) {
      this.metrics.inc(METRICS.SLOW_QUERY_COUNT.name, {
        operation: op.op,
        collection: op.collection,
      });
    }

    return duration;
  }

  /**
   * Check if auto-flush is needed and trigger it if so.
   * This includes both buffer threshold checks and WAL limit checks.
   */
  private async checkAutoFlush(): Promise<void> {
    await this.maybeFlush();
  }

  /**
   * Validate a write operation before execution.
   */
  private validateWriteOperation(op: WriteOperation): void {
    if (!op.collection) {
      throw new ValidationError(
        'WriteOperation validation failed: collection is required',
        'write_operation'
      );
    }

    // Validate collection name
    validateCollectionName(op.collection);

    if (op.op === 'insert') {
      if (!op.document) {
        throw new ValidationError(
          'Insert operation requires a document object',
          'write_operation'
        );
      }
      if (op.document._id === null || op.document._id === undefined) {
        throw new ValidationError(
          'Insert operation requires document._id to be set',
          'write_operation'
        );
      }
      // Validate the document
      validateDocument(op.document);
    } else if (op.op === 'update') {
      if (!op.filter) {
        throw new ValidationError(
          'Update operation requires a filter to identify target document',
          'write_operation'
        );
      }
      // Validate filter and update
      validateFilter(op.filter);
      if (op.update) {
        validateUpdate(op.update);
      }
    } else if (op.op === 'delete') {
      if (!op.filter) {
        throw new ValidationError(
          'Delete operation requires a filter to identify target document',
          'write_operation'
        );
      }
      // Validate filter
      validateFilter(op.filter);
    }
  }

  /**
   * Apply an update or delete operation to a document.
   */
  private async applyUpdate(
    collection: string,
    docId: string,
    op: WriteOperation
  ): Promise<Record<string, unknown>> {
    // Get current document from buffer first, then from R2
    let existing = this.bufferManager.getFromBuffer(collection, docId);
    if (!existing) {
      existing = await this.queryExecutor.findOneInR2(collection, { _id: docId });
    }

    if (op.op === 'delete') {
      return { _id: docId, _deleted: true };
    }

    // Apply update operators
    const doc = (existing ? { ...existing } : { _id: docId }) as Parameters<typeof applyUpdateOperators>[0];
    return applyUpdateOperators(doc, op.update!);
  }

  /**
   * Convert operation type to code.
   */
  private operationTypeToCode(opType: 'insert' | 'update' | 'delete'): 'i' | 'u' | 'd' {
    switch (opType) {
      case 'insert':
        return 'i';
      case 'update':
        return 'u';
      case 'delete':
        return 'd';
    }
  }

  /**
   * Check if buffer should be flushed due to buffer thresholds, back-pressure, or WAL limits.
   */
  private async maybeFlush(): Promise<void> {
    const shardId = this.state.id.toString();

    // Check back-pressure (maxBytes) first - this is critical for OOM prevention
    if (this.bufferManager.exceedsMaxBytes()) {
      try {
        logger.info('Back-pressure flush triggered', {
          shardId,
          bufferSize: this.bufferManager.getBufferSize(),
          maxBytes: this.bufferManager.getMaxBytes(),
          bufferDocCount: this.bufferManager.getBufferDocCount(),
        });

        // Record back-pressure flush metric
        this.metrics.inc(METRICS.WAL_FORCED_FLUSHES.name, {
          shard: shardId,
          reason: 'back_pressure',
        });

        await this.flushInternal();
      } catch (error) {
        logger.error('Back-pressure flush failed', {
          shardId,
          bufferSize: this.bufferManager.getBufferSize(),
          maxBytes: this.bufferManager.getMaxBytes(),
          bufferDocCount: this.bufferManager.getBufferDocCount(),
          currentLSN: this.walManager.getCurrentLSN(),
          error: error instanceof Error ? error : String(error),
          stack: error instanceof Error ? error.stack : undefined,
        });
      }
      return;
    }

    // Check buffer thresholds
    if (this.bufferManager.shouldFlush()) {
      try {
        await this.flushInternal();
      } catch (error) {
        logger.error('Auto-flush failed', {
          shardId,
          bufferSize: this.bufferManager.getBufferSize(),
          bufferDocCount: this.bufferManager.getBufferDocCount(),
          currentLSN: this.walManager.getCurrentLSN(),
          error: error instanceof Error ? error : String(error),
          stack: error instanceof Error ? error.stack : undefined,
        });
      }
      return;
    }

    // Check WAL size limits for forced flush
    const walFlushCheck = this.walManager.shouldForceFlush();
    if (walFlushCheck.needed) {
      try {
        logger.info('Forced WAL flush triggered', {
          shardId,
          reason: walFlushCheck.reason,
          walSizeBytes: this.walManager.getWalSizeBytes(),
          walEntryCount: this.walManager.getWalEntryCount(),
        });

        // Record forced flush metric
        this.metrics.inc(METRICS.WAL_FORCED_FLUSHES.name, {
          shard: shardId,
          reason: walFlushCheck.reason!,
        });

        await this.flushInternal();
      } catch (error) {
        logger.error('Forced WAL flush failed', {
          shardId,
          reason: walFlushCheck.reason,
          walSizeBytes: this.walManager.getWalSizeBytes(),
          walEntryCount: this.walManager.getWalEntryCount(),
          error: error instanceof Error ? error : String(error),
          stack: error instanceof Error ? error.stack : undefined,
        });
      }
    }
  }

  // ============================================================================
  // Query Operations
  // ============================================================================

  /**
   * Find documents matching a filter.
   */
  async find(
    collection: string,
    filter: Record<string, unknown>,
    options: FindOptions = {}
  ): Promise<Record<string, unknown>[]> {
    // Validate inputs
    validateCollectionName(collection);
    validateFilter(filter);

    const timer = this.metrics.startTimer(METRICS.QUERY_DURATION.name, {
      operation: 'find',
      collection,
    });

    try {
      const result = await this.queryExecutor.find(collection, filter, options);
      const duration = timer.end();

      // Record metrics
      this.metrics.inc(METRICS.FINDS_TOTAL.name, { collection });
      this.metrics.inc(METRICS.QUERY_COUNT.name, {
        operation: 'find',
        collection,
        status: 'success',
      });

      // Check for slow query
      if (duration * 1000 > 100) {
        this.metrics.inc(METRICS.SLOW_QUERY_COUNT.name, {
          operation: 'find',
          collection,
        });
      }

      return result;
    } catch (error) {
      timer.end();
      this.metrics.inc(METRICS.QUERY_COUNT.name, {
        operation: 'find',
        collection,
        status: 'error',
      });
      throw error;
    }
  }

  /**
   * Find a single document matching a filter.
   */
  async findOne(
    collection: string,
    filter: Record<string, unknown>,
    options: FindOptions = {}
  ): Promise<Record<string, unknown> | null> {
    // Validate inputs
    validateCollectionName(collection);
    validateFilter(filter);

    const timer = this.metrics.startTimer(METRICS.QUERY_DURATION.name, {
      operation: 'findOne',
      collection,
    });

    try {
      const result = await this.queryExecutor.findOne(collection, filter, options);
      const duration = timer.end();

      // Record metrics
      this.metrics.inc(METRICS.FINDS_TOTAL.name, { collection });
      this.metrics.inc(METRICS.QUERY_COUNT.name, {
        operation: 'findOne',
        collection,
        status: 'success',
      });

      // Check for slow query
      if (duration * 1000 > 100) {
        this.metrics.inc(METRICS.SLOW_QUERY_COUNT.name, {
          operation: 'findOne',
          collection,
        });
      }

      return result;
    } catch (error) {
      timer.end();
      this.metrics.inc(METRICS.QUERY_COUNT.name, {
        operation: 'findOne',
        collection,
        status: 'error',
      });
      throw error;
    }
  }

  // ============================================================================
  // Flush Operations
  // ============================================================================

  /**
   * Flush the in-memory buffer to R2 storage.
   */
  async flush(): Promise<void> {
    await this.flushInternal();
  }

  /**
   * Internal flush implementation.
   */
  private async flushInternal(): Promise<void> {
    if (!this.bufferManager.hasDataToFlush()) return;

    const shardId = this.state.id.toString();
    const timer = this.metrics.startTimer(METRICS.FLUSH_DURATION.name, { shard: shardId });

    try {
      // Process each collection that has data to flush
      const collectionsToProcess = this.bufferManager.getCollectionsToFlush();

    for (const collection of collectionsToProcess) {
      const collectionBuffer = this.bufferManager.getCollectionBuffer(collection);
      const deletedIds = this.bufferManager.getDeletedDocs(collection);

      // Build document list including tombstones
      const docs: Array<{
        _id: string;
        _seq: number;
        _op: 'i' | 'u' | 'd';
        [key: string]: unknown;
      }> = [];

      // Add all documents from buffer
      if (collectionBuffer) {
        for (const d of collectionBuffer.values()) {
          docs.push({
            _id: d._id,
            _seq: d._seq,
            _op: d._op,
            ...d.document,
          });
        }
      }

      // Add tombstone records for deleted documents not in buffer
      if (deletedIds) {
        for (const deletedId of deletedIds) {
          const existsInBuffer = collectionBuffer?.has(deletedId);
          if (!existsInBuffer) {
            docs.push({
              _id: deletedId,
              _seq: this.walManager.getCurrentLSN(),
              _op: 'd',
              _deleted: true,
            });
          }
        }
      }

      if (docs.length === 0) continue;

      // Write file with two-phase commit
      await this.indexManager.writeFileWithTwoPhaseCommit(collection, docs);
    }

      // Mark WAL entries as flushed
      this.walManager.markFlushed(this.walManager.getCurrentLSN());

      // Clear buffers
      this.bufferManager.clear();

      // Schedule compaction
      await this.indexManager.scheduleCompaction();

      // Record flush success
      timer.end();
      this.metrics.inc(METRICS.FLUSH_OPERATIONS.name, { shard: shardId, status: 'success' });
    } catch (error) {
      timer.end();
      this.metrics.inc(METRICS.FLUSH_OPERATIONS.name, { shard: shardId, status: 'error' });
      throw error;
    }
  }

  // ============================================================================
  // Checkpoint Operations
  // ============================================================================

  /**
   * Checkpoint the WAL by removing flushed entries.
   */
  async checkpoint(): Promise<void> {
    this.walManager.checkpoint();
  }

  // ============================================================================
  // Compaction Operations (Alarm Handler)
  // ============================================================================

  /**
   * Durable Object alarm handler for scheduled compaction.
   */
  async alarm(): Promise<void> {
    try {
      await this.compactionService.runCompaction(this.bufferManager.getConfig());
    } catch (error) {
      logger.error('Compaction failed', {
        shardId: this.state.id.toString(),
        manifestCount: this.indexManager.getAllManifests().size,
        error: error instanceof Error ? error : String(error),
        stack: error instanceof Error ? error.stack : undefined,
      });
    }
  }

  // ============================================================================
  // Read Token Operations
  // ============================================================================

  /**
   * Get a read token representing the current shard state.
   */
  async getCurrentReadToken(): Promise<string> {
    return this.queryExecutor.getCurrentReadToken();
  }

  /**
   * Parse a read token into its components.
   */
  static parseReadToken(token: string): ReadToken {
    return QueryExecutor.parseReadToken(token);
  }

  // ============================================================================
  // Status Operations
  // ============================================================================

  /**
   * Get the current buffer size in bytes.
   */
  async getBufferSize(): Promise<number> {
    return this.bufferManager.getBufferSize();
  }

  /**
   * Get the number of documents in the buffer.
   */
  async getBufferDocCount(): Promise<number> {
    return this.bufferManager.getBufferDocCount();
  }

  /**
   * Get the LSN of the last successfully flushed write.
   */
  async getFlushedLSN(): Promise<number> {
    return this.walManager.getFlushedLSN();
  }

  /**
   * Get the current WAL size in bytes.
   */
  async getWalSizeBytes(): Promise<number> {
    return this.walManager.getWalSizeBytes();
  }

  /**
   * Get the current number of WAL entries.
   */
  async getWalEntryCount(): Promise<number> {
    return this.walManager.getWalEntryCount();
  }

  /**
   * Get the configured WAL limits.
   */
  async getWalLimits(): Promise<{ maxSizeBytes: number; maxEntries: number }> {
    return this.walManager.getWalLimits();
  }

  /**
   * Get the manifest for a collection.
   */
  async getManifest(collection: string): Promise<CollectionManifest> {
    return this.indexManager.getManifest(collection);
  }

  // ============================================================================
  // Replication Support
  // ============================================================================

  /**
   * Get WAL entries for replica synchronization.
   *
   * Returns entries after the specified LSN, up to the limit.
   * This endpoint is used by ReplicaShardDO to sync with the primary.
   *
   * @param afterLSN - Return entries with LSN > afterLSN
   * @param limit - Maximum number of entries to return
   * @returns WAL entries and current state
   */
  async getWalEntriesForReplication(
    afterLSN: number,
    limit: number = 1000
  ): Promise<{
    entries: Array<{
      lsn: number;
      collection: string;
      op: 'i' | 'u' | 'd';
      docId: string;
      document: Record<string, unknown>;
      timestamp: number;
    }>;
    currentLSN: number;
    timestamp: number;
  }> {
    // Query WAL entries from SQLite
    const rows = this.storageBackend.sqlExec(
      `SELECT lsn, collection, op, doc_id, document, created_at
       FROM wal
       WHERE lsn > ?
       ORDER BY lsn ASC
       LIMIT ?`,
      afterLSN,
      limit
    ).toArray();

    const entries = rows.map((row) => ({
      lsn: row.lsn as number,
      collection: row.collection as string,
      op: row.op as 'i' | 'u' | 'd',
      docId: row.doc_id as string,
      document: JSON.parse(row.document as string) as Record<string, unknown>,
      timestamp: (row.created_at as number) * 1000, // Convert to milliseconds
    }));

    return {
      entries,
      currentLSN: this.walManager.getCurrentLSN(),
      timestamp: Date.now(),
    };
  }

  // ============================================================================
  // HTTP Interface
  // ============================================================================

  /**
   * Handle HTTP requests to the Durable Object.
   */
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      // Handle POST requests that require JSON body parsing
      if (request.method === 'POST' && (path === '/write' || path === '/find' || path === '/findOne')) {
        let body: unknown;
        try {
          body = await request.json();
        } catch {
          return new Response(
            JSON.stringify({ error: 'Request body must be valid JSON' }),
            {
              status: 400,
              headers: { 'Content-Type': 'application/json' },
            }
          );
        }

        if (path === '/write') {
          const result = await this.write(body as WriteOperation);
          return new Response(JSON.stringify(result), {
            headers: { 'Content-Type': 'application/json' },
          });
        }

        if (path === '/find') {
          const { collection, filter, ...options } = body as {
            collection: string;
            filter: Record<string, unknown>;
          } & FindOptions;
          const documents = await this.find(collection, filter || {}, options);
          return new Response(JSON.stringify({ documents }), {
            headers: { 'Content-Type': 'application/json' },
          });
        }

        if (path === '/findOne') {
          const { collection, filter, ...options } = body as {
            collection: string;
            filter: Record<string, unknown>;
          } & FindOptions;
          const document = await this.findOne(collection, filter || {}, options);
          return new Response(JSON.stringify({ document }), {
            headers: { 'Content-Type': 'application/json' },
          });
        }
      }

      if (path === '/flush' && request.method === 'POST') {
        await this.flush();
        return new Response(JSON.stringify({ success: true }), {
          headers: { 'Content-Type': 'application/json' },
        });
      }

      // WAL endpoint for replica synchronization
      if (path === '/wal' && request.method === 'POST') {
        let body: { afterLSN: number; limit?: number };
        try {
          body = await request.json() as { afterLSN: number; limit?: number };
        } catch {
          return new Response(
            JSON.stringify({ error: 'Request body must be valid JSON' }),
            { status: 400, headers: { 'Content-Type': 'application/json' } }
          );
        }

        const walData = await this.getWalEntriesForReplication(
          body.afterLSN,
          body.limit ?? 1000
        );
        return new Response(JSON.stringify(walData), {
          headers: { 'Content-Type': 'application/json' },
        });
      }

      if (path === '/status' && request.method === 'GET') {
        const walLimits = await this.getWalLimits();
        return new Response(
          JSON.stringify({
            bufferSize: await this.getBufferSize(),
            bufferDocCount: await this.getBufferDocCount(),
            flushedLSN: await this.getFlushedLSN(),
            currentLSN: this.walManager.getCurrentLSN(),
            walSizeBytes: await this.getWalSizeBytes(),
            walEntryCount: await this.getWalEntryCount(),
            walLimits,
          }),
          { headers: { 'Content-Type': 'application/json' } }
        );
      }

      // Prometheus metrics endpoint
      if (path === '/metrics' && request.method === 'GET') {
        return new Response(this.metrics.toPrometheus(), {
          headers: { 'Content-Type': 'text/plain; version=0.0.4; charset=utf-8' },
        });
      }

      // JSON metrics endpoint for structured logging
      if (path === '/metrics/json' && request.method === 'GET') {
        return new Response(JSON.stringify(this.metrics.toJSON()), {
          headers: { 'Content-Type': 'application/json' },
        });
      }

      return new Response(
        JSON.stringify({
          error: 'Endpoint not found',
          path,
          method: request.method,
        }),
        {
          status: 404,
          headers: { 'Content-Type': 'application/json' },
        }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({
          error: (error as Error).message,
        }),
        {
          status: 500,
          headers: { 'Content-Type': 'application/json' },
        }
      );
    }
  }
}
