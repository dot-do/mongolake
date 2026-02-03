/**
 * MongoLake RPC Service
 *
 * Handles Worker-to-DO communication with:
 * - Promise pipelining for efficient batched operations
 * - Shard routing based on collection hash
 * - Retry logic for transient failures
 * - Read tokens for consistency
 */

import type {
  Document,
  InsertOneResult,
  InsertManyResult,
  UpdateResult,
  DeleteResult,
  Filter,
  Update,
  FindOptions,
  AggregationStage,
} from '../types.js';
import { matchesFilter } from '../utils/filter.js';
import { LRUCache } from '../utils/lru-cache.js';
import {
  DEFAULT_SHARD_COUNT,
  CIRCUIT_BREAKER_RESET_TIMEOUT_MS,
  DEFAULT_OPERATION_TIMEOUT_MS,
  DEFAULT_RETRY_BASE_DELAY_MS,
  DEFAULT_RETRY_MAX_DELAY_MS,
  DEFAULT_RPC_BATCH_SIZE,
  DEFAULT_MAX_CONNECTIONS_PER_SHARD,
} from '../constants.js';
import {
  RPCError as BaseRPCError,
  TransientError as BaseTransientError,
  ShardUnavailableError as BaseShardUnavailableError,
  ErrorCodes,
  ValidationError,
} from '../errors/index.js';

// ============================================================================
// Types
// ============================================================================

/** Consistency levels for read operations */
export type ConsistencyLevel =
  | 'eventual'
  | 'strong'
  | 'session'
  | 'readYourWrites'
  | 'boundedStaleness';

/** Read token for read-your-writes consistency */
export interface ReadToken {
  collection: string;
  sequence: number;
  toString(): string;
}

/** Shard connection abstraction */
export interface ShardConnection {
  shardId: number;
  stub: DurableObjectStub;
  lastUsed: number;
}

/** Operation result with read token */
export interface OperationResult<T> {
  result: T;
  readToken?: string;
}

/** Batch result for multiple operations */
export interface BatchResult {
  acknowledged: boolean;
  results: OperationResult<unknown>[];
}

/** Retry configuration */
export interface RetryConfig {
  maxAttempts: number;
  baseDelay: number;
  maxDelay: number;
}

/** Shard health status */
export interface ShardHealth {
  status: 'healthy' | 'unhealthy' | 'unknown';
  lastError?: string;
  lastCheck: number;
  consecutiveFailures: number;
}

/** RPC Service options */
export interface RPCServiceOptions {
  shardNamespace: DurableObjectNamespace;
  shardCount: number;
  database?: string;
  retry?: RetryConfig;
  batchSize?: number;
  batchFlushDelay?: number;
  enableReadCache?: boolean;
  defaultConsistencyLevel?: ConsistencyLevel;
  maxConnectionsPerShard?: number;
  operationTimeout?: number;
}

/** Session for session consistency */
export interface Session {
  id: string;
  createdAt: number;
}

/** Find options extended with RPC-specific options */
export interface RPCFindOptions extends Omit<FindOptions, 'session'> {
  readToken?: string;
  consistencyLevel?: ConsistencyLevel;
  session?: Session;
  maxStalenessSeconds?: number;
  allowStale?: boolean;
}

/** Insert many options */
export interface InsertManyOptions {
  ordered?: boolean;
}

/** Extended insert result with read token */
export interface InsertOneResultWithToken extends InsertOneResult {
  readToken?: string;
}

/** Extended insert many result with read token and write errors */
export interface InsertManyResultWithToken extends InsertManyResult {
  readToken?: string;
  writeErrors?: Array<{ index: number; code: string; message: string }>;
}

/** Extended update result with read token */
export interface UpdateResultWithToken extends UpdateResult {
  readToken?: string;
}

/** Extended delete result with read token */
export interface DeleteResultWithToken extends DeleteResult {
  readToken?: string;
}

/** Batch operation for grouping concurrent inserts */
interface BatchOperation {
  collection: string;
  documents: Document[];
  resolvers: Array<{
    resolve: (value: InsertOneResultWithToken) => void;
    reject: (error: Error) => void;
  }>;
}

/** RPC response envelope for typed JSON parsing */
interface RPCResponseEnvelope<T = unknown> {
  error?: {
    message: string;
    code?: string;
    stack?: string;
  };
  data?: T;
}

// ============================================================================
// Errors (re-exported from errors module for backwards compatibility)
// ============================================================================

/** Error thrown after transient network issues exhaust all retry attempts */
export class TransientError extends BaseTransientError {
  constructor(
    message: string,
    originalError?: Error,
    retryCount?: number
  ) {
    super(message, { originalError, retryCount });
  }
}

/** Error thrown when a shard Durable Object is unreachable or unavailable */
export class ShardUnavailableError extends BaseShardUnavailableError {
  constructor(
    message: string,
    shardId: number,
    originalError?: Error
  ) {
    super(message, shardId, originalError);
  }
}

/** Error thrown for RPC-specific errors with structured error details */
export class RPCError extends BaseRPCError {
  constructor(
    message: string,
    code?: string,
    stack_trace?: string
  ) {
    super(message, code ?? ErrorCodes.RPC_ERROR, { remoteStack: stack_trace });
  }
}

// ============================================================================
// Hash Function
// ============================================================================

/**
 * Hash a collection name to a shard ID (0-15)
 */
export function hashToShardId(collection: string, database?: string): number {
  if (!collection || collection.length === 0) {
    throw new ValidationError('Cannot hash empty collection name', ErrorCodes.INVALID_NAME, {
      validationType: 'collection_name',
      invalidValue: collection,
    });
  }

  const input = database ? `${database}:${collection}` : collection;

  // Simple FNV-1a hash
  let hash = 2166136261;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }

  // Map to shard ID range
  return Math.abs(hash) % DEFAULT_SHARD_COUNT;
}

// ============================================================================
// Read Token Implementation
// ============================================================================

class ReadTokenImpl implements ReadToken {
  constructor(
    public readonly collection: string,
    public readonly sequence: number
  ) {}

  toString(): string {
    const data = `${this.collection}:${this.sequence}`;
    // Simple base64 encoding
    return 'rt_' + btoa(data);
  }

  static decode(encoded: string): ReadTokenImpl {
    if (!encoded.startsWith('rt_')) {
      throw new Error('Invalid read token format');
    }
    const data = atob(encoded.slice(3));
    const [collection, sequenceStr] = data.split(':') as [string, string];
    return new ReadTokenImpl(collection, parseInt(sequenceStr, 10));
  }
}

// ============================================================================
// Circuit Breaker
// ============================================================================

class CircuitBreaker {
  private failures: number = 0;
  private lastFailure: number = 0;
  private state: 'closed' | 'open' | 'half-open' = 'closed';

  constructor(
    private threshold: number = 5,
    private resetTimeout: number = CIRCUIT_BREAKER_RESET_TIMEOUT_MS
  ) {}

  recordSuccess(): void {
    this.failures = 0;
    this.state = 'closed';
  }

  recordFailure(): void {
    this.failures++;
    this.lastFailure = Date.now();
    if (this.failures >= this.threshold) {
      this.state = 'open';
    }
  }

  canExecute(): boolean {
    if (this.state === 'closed') {
      return true;
    }
    if (this.state === 'open') {
      if (Date.now() - this.lastFailure > this.resetTimeout) {
        this.state = 'half-open';
        return true;
      }
      return false;
    }
    // half-open
    return true;
  }

  isOpen(): boolean {
    return this.state === 'open' && Date.now() - this.lastFailure <= this.resetTimeout;
  }
}

// ============================================================================
// RPC Service Implementation
// ============================================================================

export class RPCService {
  private readonly shardNamespace: DurableObjectNamespace;
  // Reserved for future multi-shard query routing
  private readonly _shardCount: number;
  private readonly database?: string;
  private readonly retry: RetryConfig;
  private readonly batchSize: number;
  private readonly batchFlushDelay: number;
  private readonly enableReadCache: boolean;
  private readonly defaultConsistencyLevel: ConsistencyLevel;
  // Reserved for future connection pooling implementation
  private readonly _maxConnectionsPerShard: number;
  private readonly operationTimeout: number;

  private lastRoutedShardId: number = -1;
  private operationCount: number = 0;
  private pipelineDepth: number = 0;
  private connections: Map<number, ShardConnection> = new Map();
  private shardHealth: Map<number, ShardHealth> = new Map();
  private circuitBreakers: Map<number, CircuitBreaker> = new Map();
  // LRU cache for read results with 5-minute TTL
  private cache: LRUCache<string, { data: Document[]; timestamp: number }> = new LRUCache({
    maxSize: 1000,
    ttlMs: 5 * 60 * 1000, // 5 minutes
  });

  // Pending batches with timed flush (used when batchFlushDelay > 0)
  private pendingBatches: Map<number, {
    collection: string;
    documents: Document[];
    resolvers: Array<{
      resolve: (value: InsertOneResultWithToken) => void;
      reject: (error: Error) => void;
    }>;
    timeout?: ReturnType<typeof setTimeout>;
  }> = new Map();

  // Concurrent batches (used when batchFlushDelay === 0)
  private batchQueue: Map<number, {
    collection: string;
    documents: Document[];
    resolvers: Array<{
      resolve: (value: InsertOneResultWithToken) => void;
      reject: (error: Error) => void;
    }>;
  }> = new Map();

  // Write cache for read-your-writes consistency (LRU with 1-minute TTL)
  private writeCache: LRUCache<string, Document[]> = new LRUCache({
    maxSize: 500,
    ttlMs: 60 * 1000, // 1 minute
  });

  // Track all active batch timeouts for reliable cleanup on service shutdown
  private activeBatchTimeouts: Set<ReturnType<typeof setTimeout>> = new Set();

  constructor(options: RPCServiceOptions) {
    this.shardNamespace = options.shardNamespace;
    this._shardCount = options.shardCount;
    this.database = options.database;
    this.retry = options.retry || {
      maxAttempts: 3,
      baseDelay: DEFAULT_RETRY_BASE_DELAY_MS,
      maxDelay: DEFAULT_RETRY_MAX_DELAY_MS,
    };
    this.batchSize = options.batchSize || DEFAULT_RPC_BATCH_SIZE;
    this.batchFlushDelay = options.batchFlushDelay || 0;
    this.enableReadCache = options.enableReadCache || false;
    this.defaultConsistencyLevel = options.defaultConsistencyLevel || 'eventual';
    this._maxConnectionsPerShard = options.maxConnectionsPerShard || DEFAULT_MAX_CONNECTIONS_PER_SHARD;
    this.operationTimeout = options.operationTimeout || DEFAULT_OPERATION_TIMEOUT_MS;
  }

  // ============================================================================
  // Routing & Connection Management
  // ============================================================================

  private getShardId(collection: string): number {
    return hashToShardId(collection, this.database);
  }

  private getConnection(shardId: number): ShardConnection {
    let connection = this.connections.get(shardId);
    if (!connection) {
      const id = this.shardNamespace.idFromName(`shard-${shardId}`);
      const stub = this.shardNamespace.get(id);
      connection = {
        shardId,
        stub,
        lastUsed: Date.now(),
      };
      this.connections.set(shardId, connection);
    }
    connection.lastUsed = Date.now();
    return connection;
  }

  private getCircuitBreaker(shardId: number): CircuitBreaker {
    let cb = this.circuitBreakers.get(shardId);
    if (!cb) {
      cb = new CircuitBreaker();
      this.circuitBreakers.set(shardId, cb);
    }
    return cb;
  }

  private updateShardHealth(shardId: number, success: boolean, error?: Error): void {
    const existing = this.shardHealth.get(shardId) || {
      status: 'unknown' as const,
      lastCheck: 0,
      consecutiveFailures: 0,
    };

    if (success) {
      this.shardHealth.set(shardId, {
        status: 'healthy',
        lastCheck: Date.now(),
        consecutiveFailures: 0,
      });
    } else {
      this.shardHealth.set(shardId, {
        status: 'unhealthy',
        lastError: error?.message,
        lastCheck: Date.now(),
        consecutiveFailures: existing.consecutiveFailures + 1,
      });
    }
  }

  // ============================================================================
  // Retry Logic
  // ============================================================================

  private isTransientError(error: Error): boolean {
    const message = error.message.toLowerCase();

    // Network-level transient errors
    const networkErrors = [
      'timeout',
      'econnreset',
      'connection lost',
      'network',
      'evicted',
    ];

    // HTTP-level transient errors (server-side failures)
    const httpRetryableStatuses = [
      '502', // Bad Gateway
      '503', // Service Unavailable
      '504', // Gateway Timeout
    ];

    return networkErrors.some(err => message.includes(err)) ||
           httpRetryableStatuses.some(status => message.includes(status)) ||
           message.includes('service unavailable');
  }

  private async withRetry<T>(
    shardId: number,
    operation: () => Promise<T>
  ): Promise<T> {
    const cb = this.getCircuitBreaker(shardId);

    // Fail fast if circuit breaker is already open
    if (cb.isOpen()) {
      throw new Error(`Circuit breaker is open for shard ${shardId}`);
    }

    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= this.retry.maxAttempts; attempt++) {
      try {
        // Check circuit breaker state before each attempt
        if (!cb.canExecute()) {
          throw new Error(`Circuit breaker is open for shard ${shardId}`);
        }

        const result = await operation();
        cb.recordSuccess();
        this.updateShardHealth(shardId, true);
        return result;
      } catch (error) {
        lastError = error as Error;

        // Non-transient errors fail immediately (e.g., validation, authentication)
        if (!this.isTransientError(lastError)) {
          cb.recordFailure();
          this.updateShardHealth(shardId, false, lastError);
          throw lastError;
        }

        // Record transient failure and update health
        cb.recordFailure();
        this.updateShardHealth(shardId, false, lastError);

        // Apply exponential backoff before next attempt
        if (attempt < this.retry.maxAttempts) {
          const delay = Math.min(
            this.retry.baseDelay * Math.pow(2, attempt - 1),
            this.retry.maxDelay
          );
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    // All retry attempts exhausted
    throw new TransientError(
      `Operation failed after ${this.retry.maxAttempts} attempts: ${lastError?.message}`,
      lastError,
      this.retry.maxAttempts
    );
  }

  // ============================================================================
  // RPC Execution
  // ============================================================================

  private async executeRPC<T>(
    collection: string,
    operation: string,
    body: Record<string, unknown>,
    options?: RPCFindOptions
  ): Promise<T> {
    const shardId = this.getShardId(collection);
    this.lastRoutedShardId = shardId;
    this.pipelineDepth++;

    const connection = this.getConnection(shardId);

    // Build RPC request body with consistency options
    const requestBody = {
      operation,
      collection,
      ...body,
      consistencyLevel: options?.consistencyLevel || this.defaultConsistencyLevel,
      // Session consistency overrides other consistency levels
      ...(options?.session && {
        sessionId: options.session.id,
        consistencyLevel: 'session',
      }),
      // Include read token for read-your-writes consistency
      ...(options?.readToken && { readToken: options.readToken }),
      // Include staleness bound for bounded staleness consistency
      ...(options?.maxStalenessSeconds && {
        maxStalenessSeconds: options.maxStalenessSeconds,
      }),
    };

    const executeRequest = async (): Promise<T> => {
      // Create request object for RPC to shard DO
      const request = new Request('https://shard/rpc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody),
      });

      // Set up timeout promise to race against the actual RPC call
      let timeoutId: ReturnType<typeof setTimeout> | undefined;
      const timeoutPromise = new Promise<never>((_, reject) => {
        timeoutId = setTimeout(() => {
          reject(new Error('Operation timeout'));
        }, this.operationTimeout);
      });

      try {
        // Race RPC call against timeout
        const responsePromise = connection.stub.fetch(request);
        const response = await Promise.race([responsePromise, timeoutPromise]) as Response;

        if (timeoutId) clearTimeout(timeoutId);

        // Clone response to allow multiple reads
        const clonedResponse = response.clone();

        // Handle server-side transient errors
        if (response.status === 503 || response.status === 502 || response.status === 504) {
          throw new Error(`Service Unavailable: ${response.status}`);
        }

        // Parse response body with proper typing
        const data = await clonedResponse.json() as RPCResponseEnvelope<T> & T;

        // Handle error responses (4xx, 5xx)
        if (response.status >= 400) {
          if (data.error) {
            const error = new RPCError(
              data.error.message,
              data.error.code,
              data.error.stack
            );
            throw error;
          }
          throw new Error(`RPC Error: ${response.status}`);
        }

        return data;
      } catch (error) {
        // Clean up timeout if still pending
        if (timeoutId) clearTimeout(timeoutId);
        throw error;
      }
    };

    try {
      return await this.withRetry(shardId, executeRequest);
    } catch (error) {
      // Transient errors already wrapped, pass through
      if (error instanceof TransientError) {
        throw error;
      }

      // Detect shard-level unavailability (hibernation, memory issues, etc.)
      const errorMessage = (error as Error).message;
      if (
        errorMessage.includes('hibernating') ||
        errorMessage.includes('memory limit') ||
        errorMessage.includes('unavailable')
      ) {
        throw new ShardUnavailableError(errorMessage, shardId, error as Error);
      }

      // Pass through all other errors
      throw error;
    }
  }

  // ============================================================================
  // Public API: Write Operations
  // ============================================================================

  async insert(
    collection: string,
    document: Document
  ): Promise<InsertOneResultWithToken> {
    const shardId = this.getShardId(collection);
    this.lastRoutedShardId = shardId;

    // Check if batching with flush delay is enabled
    if (this.batchFlushDelay > 0) {
      return this.batchInsertWithDelay(collection, document, shardId);
    }

    // Check if we should batch concurrent operations
    return this.batchConcurrentInsert(collection, document, shardId);
  }

  private async batchConcurrentInsert(
    collection: string,
    document: Document,
    shardId: number
  ): Promise<InsertOneResultWithToken> {
    return new Promise((resolve, reject) => {
      let queue = this.batchQueue.get(shardId);

      if (!queue) {
        queue = {
          collection,
          documents: [],
          resolvers: [],
        };
        this.batchQueue.set(shardId, queue);

        // Schedule batch flush on next microtask
        queueMicrotask(() => this.flushBatchQueue(shardId));
      }

      queue.documents.push(document);
      queue.resolvers.push({ resolve, reject });
    });
  }

  private async flushBatchQueue(shardId: number): Promise<void> {
    const queue = this.batchQueue.get(shardId);
    if (!queue || queue.documents.length === 0) return;

    this.batchQueue.delete(shardId);
    await this.processBatch(queue);
  }

  private async batchInsertWithDelay(
    collection: string,
    document: Document,
    shardId: number
  ): Promise<InsertOneResultWithToken> {
    return new Promise((resolve, reject) => {
      let batch = this.pendingBatches.get(shardId);

      if (!batch) {
        batch = {
          collection,
          documents: [],
          resolvers: [],
        };
        this.pendingBatches.set(shardId, batch);
      }

      batch.documents.push(document);
      batch.resolvers.push({ resolve, reject });

      // Clear existing timeout and set new one to aggregate documents
      if (batch.timeout) {
        clearTimeout(batch.timeout);
        this.activeBatchTimeouts.delete(batch.timeout);
      }

      const timeout = setTimeout(() => {
        this.activeBatchTimeouts.delete(timeout);
        this.flushBatch(shardId);
      }, this.batchFlushDelay);
      batch.timeout = timeout;
      this.activeBatchTimeouts.add(timeout);
    });
  }

  private async flushBatch(shardId: number): Promise<void> {
    const batch = this.pendingBatches.get(shardId);
    if (!batch || batch.documents.length === 0) return;

    // Clear timeout before processing to prevent memory leaks
    if (batch.timeout) {
      clearTimeout(batch.timeout);
      this.activeBatchTimeouts.delete(batch.timeout);
      batch.timeout = undefined;
    }

    this.pendingBatches.delete(shardId);
    await this.processBatch(batch);
  }

  private async processBatch(batch: BatchOperation): Promise<void> {
    // Count as single operation for batched inserts
    this.operationCount++;

    try {
      const result = await this.executeRPC<InsertManyResultWithToken & { ok?: boolean }>(
        batch.collection,
        'insertMany',
        { documents: batch.documents }
      );

      // Normalize response - if we got { ok: true } without acknowledged, treat as success
      const acknowledged = result.acknowledged ?? (result.ok === true);
      const insertedIds = result.insertedIds || {};

      // Cache documents for read-your-writes consistency
      const existingDocs = this.writeCache.get(batch.collection) || [];
      this.writeCache.set(batch.collection, [...existingDocs, ...batch.documents]);

      // Resolve each individual insert with the batch result
      batch.resolvers.forEach((resolver, index) => {
        resolver.resolve({
          acknowledged,
          insertedId: insertedIds[index] ?? batch.documents[index]?._id ?? `generated-${index}`,
          readToken: result.readToken,
        });
      });
    } catch (error) {
      // Reject all pending inserts in the batch
      batch.resolvers.forEach((resolver) => {
        resolver.reject(error as Error);
      });
    }
  }

  async insertMany(
    collection: string,
    documents: Document[],
    options?: InsertManyOptions
  ): Promise<InsertManyResultWithToken> {
    // If documents fit in a single batch, send directly
    if (documents.length <= this.batchSize) {
      this.operationCount++;
      const result = await this.executeRPC<InsertManyResultWithToken & { ok?: boolean }>(
        collection,
        'insertMany',
        {
          documents,
          ordered: options?.ordered,
        }
      );

      // Normalize response for consistent API
      return {
        acknowledged: result.acknowledged ?? (result.ok === true),
        insertedCount: result.insertedCount ?? documents.length,
        insertedIds: result.insertedIds || documents.reduce((acc, doc, i) => {
          acc[i] = doc._id || `generated-${i}`;
          return acc;
        }, {} as Record<number, any>),
        readToken: result.readToken,
        writeErrors: result.writeErrors,
      };
    }

    // Split large insertions into multiple batches
    const results: InsertManyResultWithToken = {
      acknowledged: true,
      insertedCount: 0,
      insertedIds: {},
      writeErrors: [],
    };

    for (let i = 0; i < documents.length; i += this.batchSize) {
      const chunk = documents.slice(i, i + this.batchSize);
      this.operationCount++;

      const chunkResult = await this.executeRPC<InsertManyResultWithToken>(
        collection,
        'insertMany',
        { documents: chunk, ordered: options?.ordered }
      );

      // Accumulate results from all chunks
      results.insertedCount += chunkResult.insertedCount;
      Object.entries(chunkResult.insertedIds).forEach(([key, value]) => {
        results.insertedIds[i + parseInt(key)] = value;
      });

      // Adjust error indices to account for chunk offset
      if (chunkResult.writeErrors) {
        if (!results.writeErrors) {
          results.writeErrors = [];
        }
        chunkResult.writeErrors.forEach((err) => {
          results.writeErrors?.push({
            ...err,
            index: i + err.index,
          });
        });
      }

      // Keep last read token from batch
      results.readToken = chunkResult.readToken;
    }

    // Remove writeErrors if empty
    if (results.writeErrors && results.writeErrors.length === 0) {
      delete results.writeErrors;
    }

    return results;
  }

  async update(
    collection: string,
    filter: Filter<Document>,
    update: Update<Document>
  ): Promise<UpdateResultWithToken> {
    this.operationCount++;
    return this.executeRPC<UpdateResultWithToken>(collection, 'updateOne', {
      filter,
      update,
    });
  }

  async delete(
    collection: string,
    filter: Filter<Document>
  ): Promise<DeleteResultWithToken> {
    this.operationCount++;
    return this.executeRPC<DeleteResultWithToken>(collection, 'deleteOne', {
      filter,
    });
  }

  // ============================================================================
  // Public API: Read Operations
  // ============================================================================

  async find(
    collection: string,
    filter: Filter<Document>,
    options?: RPCFindOptions
  ): Promise<Document[]> {
    const cacheKey = JSON.stringify({ collection, filter });

    // Attempt stale read from cache if allowed and cache is enabled
    if (options?.allowStale && this.enableReadCache) {
      const cached = this.cache.get(cacheKey);
      if (cached) {
        try {
          return await this.executeRPCFind(collection, filter, options);
        } catch {
          // Fall back to cached data when shard is unavailable
          return cached.data;
        }
      }
    }

    return this.executeRPCFind(collection, filter, options);
  }

  private async executeRPCFind(
    collection: string,
    filter: Filter<Document>,
    options?: RPCFindOptions
  ): Promise<Document[]> {
    this.operationCount++;
    const result = await this.executeRPC<{ documents?: Document[]; ok?: boolean }>(
      collection,
      'find',
      { filter, ...options },
      options
    );

    // Extract documents from RPC response
    let documents = result.documents || [];

    // Merge server results with local write cache for read-your-writes consistency
    const cachedWrites = this.writeCache.get(collection);
    if (cachedWrites && cachedWrites.length > 0) {
      // Filter cache to only documents matching the query
      if (filter && Object.keys(filter).length > 0) {
        const matchingCached = cachedWrites.filter(doc => matchesFilter(doc, filter));
        // Merge cache with server results, avoiding duplicates by _id
        const existingIds = new Set(documents.map(d => String(d._id)));
        const newDocs = matchingCached.filter(d => !existingIds.has(String(d._id)));
        documents = [...documents, ...newDocs];
      }
    }

    // Cache query results for fallback on transient failures
    if (this.enableReadCache) {
      const cacheKey = JSON.stringify({ collection, filter });
      this.cache.set(cacheKey, { data: documents, timestamp: Date.now() });
    }

    return documents;
  }

  async aggregate(
    collection: string,
    pipeline: AggregationStage[]
  ): Promise<Document[]> {
    this.operationCount++;
    const result = await this.executeRPC<{ documents: Document[] }>(
      collection,
      'aggregate',
      { pipeline }
    );
    return result.documents;
  }

  // ============================================================================
  // Public API: Read Tokens
  // ============================================================================

  createReadToken(collection: string, sequence: number): ReadToken {
    return new ReadTokenImpl(collection, sequence);
  }

  decodeReadToken(encoded: string): ReadToken {
    return ReadTokenImpl.decode(encoded);
  }

  // ============================================================================
  // Public API: Sessions
  // ============================================================================

  createSession(): Session {
    return {
      id: crypto.randomUUID(),
      createdAt: Date.now(),
    };
  }

  // ============================================================================
  // Public API: Metrics & Management
  // ============================================================================

  getLastRoutedShardId(): number {
    return this.lastRoutedShardId;
  }

  getOperationCount(): number {
    return this.operationCount;
  }

  getPipelineDepth(): number {
    return this.pipelineDepth;
  }

  getActiveConnectionCount(): number {
    return this.connections.size;
  }

  getShardCount(): number {
    return this._shardCount;
  }

  getMaxConnectionsPerShard(): number {
    return this._maxConnectionsPerShard;
  }

  getShardHealth(): Record<number, ShardHealth> {
    const health: Record<number, ShardHealth> = {};
    this.shardHealth.forEach((value, key) => {
      health[key] = value;
    });
    return health;
  }

  async close(): Promise<void> {
    // Clear all active batch timeouts to prevent memory leaks
    for (const timeout of this.activeBatchTimeouts) {
      clearTimeout(timeout);
    }
    this.activeBatchTimeouts.clear();

    // Reject all pending batches (with timed flush) still in flight
    for (const [_, batch] of this.pendingBatches) {
      if (batch.timeout) {
        clearTimeout(batch.timeout);
      }
      batch.resolvers.forEach((resolver) => {
        resolver.reject(new Error('RPC service closed'));
      });
    }
    this.pendingBatches.clear();

    // Reject all concurrent batches (microtask-based) still in flight
    for (const [_, queue] of this.batchQueue) {
      queue.resolvers.forEach((resolver) => {
        resolver.reject(new Error('RPC service closed'));
      });
    }
    this.batchQueue.clear();

    // Close all shard connections
    this.connections.clear();

    // Clear read cache
    this.cache.clear();

    // Clear write cache
    this.writeCache.clear();
  }
}

// ============================================================================
// Factory Function
// ============================================================================

export function createRPCService(options: RPCServiceOptions): RPCService {
  return new RPCService(options);
}
