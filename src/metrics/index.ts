/**
 * Metrics Collection Module
 *
 * Provides comprehensive metrics collection with support for:
 * - Counters: Monotonically increasing values (requests, errors)
 * - Gauges: Point-in-time values (buffer size, connections)
 * - Histograms: Distribution of values (latency, request sizes)
 *
 * Export formats:
 * - Prometheus text format (for /metrics endpoint)
 * - Workers Analytics (for Cloudflare integration)
 * - Structured logging (for Cloudflare Logs)
 */

// ============================================================================
// Types
// ============================================================================

/**
 * Metric type enumeration
 */
export type MetricType = 'counter' | 'gauge' | 'histogram';

/**
 * Labels for metric dimensions
 */
export type Labels = Record<string, string>;

/**
 * Configuration for histogram buckets
 */
export interface HistogramConfig {
  /** Bucket boundaries (e.g., [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10]) */
  buckets: number[];
}

/**
 * Metric definition
 */
export interface MetricDefinition {
  name: string;
  type: MetricType;
  help: string;
  labelNames?: readonly string[];
  buckets?: readonly number[];
}

/**
 * Workers Analytics Engine binding interface
 */
export interface AnalyticsEngineDataset {
  writeDataPoint(data: {
    blobs?: string[];
    doubles?: number[];
    indexes?: string[];
  }): void;
}

/**
 * Histogram bucket data
 */
interface HistogramBucket {
  le: number;
  count: number;
}

/**
 * Internal metric value storage
 */
interface MetricValue {
  value: number;
  labels: Labels;
  timestamp: number;
  /** For histograms: bucket counts */
  buckets?: HistogramBucket[];
  /** For histograms: sum of all observed values */
  sum?: number;
  /** For histograms: count of observations */
  count?: number;
}

/**
 * Internal metric storage
 */
interface MetricStorage {
  definition: MetricDefinition;
  values: Map<string, MetricValue>;
}

// ============================================================================
// Default Histogram Buckets
// ============================================================================

/**
 * Default latency buckets in seconds (1ms to 10s)
 */
export const DEFAULT_LATENCY_BUCKETS = [
  0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
];

/**
 * Default size buckets in bytes (1KB to 100MB)
 */
export const DEFAULT_SIZE_BUCKETS = [
  1024, 10240, 102400, 1048576, 10485760, 104857600,
];

// ============================================================================
// Predefined Metrics
// ============================================================================

/**
 * Standard metric definitions for MongoLake
 */
export const METRICS = {
  // Query Metrics
  QUERY_DURATION: {
    name: 'mongolake_query_duration_seconds',
    type: 'histogram' as const,
    help: 'Query execution duration in seconds',
    labelNames: ['operation', 'collection'],
    buckets: DEFAULT_LATENCY_BUCKETS,
  },
  QUERY_COUNT: {
    name: 'mongolake_query_total',
    type: 'counter' as const,
    help: 'Total number of queries executed',
    labelNames: ['operation', 'collection', 'status'],
  },
  SLOW_QUERY_COUNT: {
    name: 'mongolake_slow_queries_total',
    type: 'counter' as const,
    help: 'Total number of slow queries (>100ms)',
    labelNames: ['operation', 'collection'],
  },

  // Operation Counts
  INSERTS_TOTAL: {
    name: 'mongolake_inserts_total',
    type: 'counter' as const,
    help: 'Total number of insert operations',
    labelNames: ['collection'],
  },
  UPDATES_TOTAL: {
    name: 'mongolake_updates_total',
    type: 'counter' as const,
    help: 'Total number of update operations',
    labelNames: ['collection'],
  },
  DELETES_TOTAL: {
    name: 'mongolake_deletes_total',
    type: 'counter' as const,
    help: 'Total number of delete operations',
    labelNames: ['collection'],
  },
  FINDS_TOTAL: {
    name: 'mongolake_finds_total',
    type: 'counter' as const,
    help: 'Total number of find operations',
    labelNames: ['collection'],
  },

  // Cache Metrics
  CACHE_HITS: {
    name: 'mongolake_cache_hits_total',
    type: 'counter' as const,
    help: 'Total number of cache hits',
    labelNames: ['cache_type'],
  },
  CACHE_MISSES: {
    name: 'mongolake_cache_misses_total',
    type: 'counter' as const,
    help: 'Total number of cache misses',
    labelNames: ['cache_type'],
  },
  CACHE_EVICTIONS: {
    name: 'mongolake_cache_evictions_total',
    type: 'counter' as const,
    help: 'Total number of cache evictions',
    labelNames: ['cache_type'],
  },
  CACHE_SIZE: {
    name: 'mongolake_cache_size_bytes',
    type: 'gauge' as const,
    help: 'Current cache size in bytes',
    labelNames: ['cache_type'],
  },
  CACHE_ENTRIES: {
    name: 'mongolake_cache_entries',
    type: 'gauge' as const,
    help: 'Current number of cache entries',
    labelNames: ['cache_type'],
  },

  // Storage Metrics
  R2_READS: {
    name: 'mongolake_r2_reads_total',
    type: 'counter' as const,
    help: 'Total number of R2 read operations',
    labelNames: ['operation'],
  },
  R2_WRITES: {
    name: 'mongolake_r2_writes_total',
    type: 'counter' as const,
    help: 'Total number of R2 write operations',
    labelNames: ['operation'],
  },
  R2_BYTES_READ: {
    name: 'mongolake_r2_bytes_read_total',
    type: 'counter' as const,
    help: 'Total bytes read from R2',
  },
  R2_BYTES_WRITTEN: {
    name: 'mongolake_r2_bytes_written_total',
    type: 'counter' as const,
    help: 'Total bytes written to R2',
  },
  R2_OPERATION_DURATION: {
    name: 'mongolake_r2_operation_duration_seconds',
    type: 'histogram' as const,
    help: 'R2 operation duration in seconds',
    labelNames: ['operation'],
    buckets: DEFAULT_LATENCY_BUCKETS,
  },
  R2_ERRORS: {
    name: 'mongolake_r2_errors_total',
    type: 'counter' as const,
    help: 'Total number of R2 operation errors',
    labelNames: ['operation', 'error_type'],
  },

  // Compaction Metrics
  COMPACTION_DURATION: {
    name: 'mongolake_compaction_duration_seconds',
    type: 'histogram' as const,
    help: 'Compaction cycle duration in seconds',
    labelNames: ['collection'],
    buckets: [0.1, 0.5, 1, 5, 10, 30, 60, 120, 300],
  },
  COMPACTION_FILES_PROCESSED: {
    name: 'mongolake_compaction_files_processed_total',
    type: 'counter' as const,
    help: 'Total number of files processed during compaction',
    labelNames: ['collection'],
  },
  COMPACTION_BYTES_SAVED: {
    name: 'mongolake_compaction_bytes_saved_total',
    type: 'counter' as const,
    help: 'Total bytes saved by compaction',
    labelNames: ['collection'],
  },
  COMPACTION_CYCLES: {
    name: 'mongolake_compaction_cycles_total',
    type: 'counter' as const,
    help: 'Total number of compaction cycles',
    labelNames: ['collection', 'status'],
  },

  // Connection Metrics
  ACTIVE_CONNECTIONS: {
    name: 'mongolake_active_connections',
    type: 'gauge' as const,
    help: 'Number of active connections',
    labelNames: ['protocol'],
  },
  CONNECTION_ERRORS: {
    name: 'mongolake_connection_errors_total',
    type: 'counter' as const,
    help: 'Total number of connection errors',
    labelNames: ['protocol', 'error_type'],
  },
  CONNECTIONS_TOTAL: {
    name: 'mongolake_connections_total',
    type: 'counter' as const,
    help: 'Total number of connections established',
    labelNames: ['protocol'],
  },

  // Buffer Metrics
  BUFFER_SIZE: {
    name: 'mongolake_buffer_size_bytes',
    type: 'gauge' as const,
    help: 'Current buffer size in bytes',
    labelNames: ['shard'],
  },
  BUFFER_DOCS: {
    name: 'mongolake_buffer_documents',
    type: 'gauge' as const,
    help: 'Current number of documents in buffer',
    labelNames: ['shard'],
  },
  FLUSH_OPERATIONS: {
    name: 'mongolake_flush_operations_total',
    type: 'counter' as const,
    help: 'Total number of flush operations',
    labelNames: ['shard', 'status'],
  },
  FLUSH_DURATION: {
    name: 'mongolake_flush_duration_seconds',
    type: 'histogram' as const,
    help: 'Flush operation duration in seconds',
    labelNames: ['shard'],
    buckets: DEFAULT_LATENCY_BUCKETS,
  },

  // HTTP Request Metrics
  HTTP_REQUESTS_TOTAL: {
    name: 'mongolake_http_requests_total',
    type: 'counter' as const,
    help: 'Total number of HTTP requests',
    labelNames: ['method', 'path', 'status'],
  },
  HTTP_REQUEST_DURATION: {
    name: 'mongolake_http_request_duration_seconds',
    type: 'histogram' as const,
    help: 'HTTP request duration in seconds',
    labelNames: ['method', 'path'],
    buckets: DEFAULT_LATENCY_BUCKETS,
  },
  HTTP_REQUEST_SIZE: {
    name: 'mongolake_http_request_size_bytes',
    type: 'histogram' as const,
    help: 'HTTP request body size in bytes',
    labelNames: ['method', 'path'],
    buckets: DEFAULT_SIZE_BUCKETS,
  },
  HTTP_RESPONSE_SIZE: {
    name: 'mongolake_http_response_size_bytes',
    type: 'histogram' as const,
    help: 'HTTP response body size in bytes',
    labelNames: ['method', 'path'],
    buckets: DEFAULT_SIZE_BUCKETS,
  },

  // Shard Splitting Metrics
  SHARD_COUNT: {
    name: 'mongolake_shard_count',
    type: 'gauge' as const,
    help: 'Current number of shards',
  },
  SHARD_DOCUMENT_COUNT: {
    name: 'mongolake_shard_document_count',
    type: 'gauge' as const,
    help: 'Number of documents per shard',
    labelNames: ['shard'],
  },
  SHARD_SIZE_BYTES: {
    name: 'mongolake_shard_size_bytes',
    type: 'gauge' as const,
    help: 'Size of shard in bytes',
    labelNames: ['shard'],
  },
  SHARD_WRITE_RATE: {
    name: 'mongolake_shard_write_rate',
    type: 'gauge' as const,
    help: 'Write rate per shard (operations per second)',
    labelNames: ['shard'],
  },
  SHARD_SPLITS_TOTAL: {
    name: 'mongolake_shard_splits_total',
    type: 'counter' as const,
    help: 'Total number of shard split operations',
    labelNames: ['reason', 'status'],
  },
  SHARD_SPLIT_DURATION: {
    name: 'mongolake_shard_split_duration_seconds',
    type: 'histogram' as const,
    help: 'Duration of shard split operations in seconds',
    labelNames: ['reason'],
    buckets: [1, 5, 10, 30, 60, 120, 300, 600],
  },
  SHARD_HOT_COUNT: {
    name: 'mongolake_shard_hot_count',
    type: 'gauge' as const,
    help: 'Number of shards approaching split thresholds',
  },

  // Per-Shard Query Metrics
  SHARD_QUERY_COUNT: {
    name: 'mongolake_shard_query_total',
    type: 'counter' as const,
    help: 'Total number of queries per shard',
    labelNames: ['shard', 'operation', 'status'],
  },
  SHARD_QUERY_DURATION: {
    name: 'mongolake_shard_query_duration_seconds',
    type: 'histogram' as const,
    help: 'Query duration per shard in seconds',
    labelNames: ['shard', 'operation'],
    buckets: DEFAULT_LATENCY_BUCKETS,
  },
  SHARD_ERRORS: {
    name: 'mongolake_shard_errors_total',
    type: 'counter' as const,
    help: 'Total number of errors per shard',
    labelNames: ['shard', 'error_type'],
  },
  SHARD_OPERATIONS: {
    name: 'mongolake_shard_operations_total',
    type: 'counter' as const,
    help: 'Total number of operations per shard',
    labelNames: ['shard', 'operation'],
  },
  SHARD_LATENCY_P50: {
    name: 'mongolake_shard_latency_p50_seconds',
    type: 'gauge' as const,
    help: 'P50 latency per shard in seconds',
    labelNames: ['shard'],
  },
  SHARD_LATENCY_P99: {
    name: 'mongolake_shard_latency_p99_seconds',
    type: 'gauge' as const,
    help: 'P99 latency per shard in seconds',
    labelNames: ['shard'],
  },

  // Per-Collection Document Metrics
  COLLECTION_DOCUMENT_COUNT: {
    name: 'mongolake_collection_document_count',
    type: 'gauge' as const,
    help: 'Number of documents per collection',
    labelNames: ['collection', 'database'],
  },
  COLLECTION_SIZE_BYTES: {
    name: 'mongolake_collection_size_bytes',
    type: 'gauge' as const,
    help: 'Size of collection in bytes',
    labelNames: ['collection', 'database'],
  },
  COLLECTION_OPERATIONS: {
    name: 'mongolake_collection_operations_total',
    type: 'counter' as const,
    help: 'Total number of operations per collection',
    labelNames: ['collection', 'database', 'operation'],
  },
  COLLECTION_QUERY_DURATION: {
    name: 'mongolake_collection_query_duration_seconds',
    type: 'histogram' as const,
    help: 'Query duration per collection in seconds',
    labelNames: ['collection', 'database', 'operation'],
    buckets: DEFAULT_LATENCY_BUCKETS,
  },
  COLLECTION_ERRORS: {
    name: 'mongolake_collection_errors_total',
    type: 'counter' as const,
    help: 'Total number of errors per collection',
    labelNames: ['collection', 'database', 'error_type'],
  },
  COLLECTION_READ_COUNT: {
    name: 'mongolake_collection_reads_total',
    type: 'counter' as const,
    help: 'Total read operations per collection',
    labelNames: ['collection', 'database'],
  },
  COLLECTION_WRITE_COUNT: {
    name: 'mongolake_collection_writes_total',
    type: 'counter' as const,
    help: 'Total write operations per collection',
    labelNames: ['collection', 'database'],
  },
  COLLECTION_SCAN_COUNT: {
    name: 'mongolake_collection_scans_total',
    type: 'counter' as const,
    help: 'Total collection scan operations (no index used)',
    labelNames: ['collection', 'database'],
  },
  COLLECTION_INDEX_USAGE: {
    name: 'mongolake_collection_index_usage_total',
    type: 'counter' as const,
    help: 'Index usage count per collection',
    labelNames: ['collection', 'database', 'index_name'],
  },

  // WAL Metrics
  WAL_SIZE_BYTES: {
    name: 'mongolake_wal_size_bytes',
    type: 'gauge' as const,
    help: 'Current WAL size in bytes',
    labelNames: ['shard'],
  },
  WAL_ENTRIES: {
    name: 'mongolake_wal_entries',
    type: 'gauge' as const,
    help: 'Current number of WAL entries',
    labelNames: ['shard'],
  },
  WAL_FORCED_FLUSHES: {
    name: 'mongolake_wal_forced_flushes_total',
    type: 'counter' as const,
    help: 'Total number of forced WAL flushes due to size/entry limits',
    labelNames: ['shard', 'reason'],
  },
} as const;

// ============================================================================
// MetricsCollector Class
// ============================================================================

/**
 * MetricsCollector provides comprehensive metrics collection with minimal overhead.
 *
 * Features:
 * - Counters, gauges, and histograms
 * - Label-based dimensional metrics
 * - Prometheus text format export
 * - Workers Analytics integration
 * - Structured logging support
 *
 * @example
 * ```typescript
 * // Create collector
 * const metrics = new MetricsCollector();
 *
 * // Register and use metrics
 * metrics.register(METRICS.QUERY_COUNT);
 * metrics.inc(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'users', status: 'success' });
 *
 * // Observe histogram values
 * metrics.register(METRICS.QUERY_DURATION);
 * metrics.observe(METRICS.QUERY_DURATION.name, 0.045, { operation: 'find', collection: 'users' });
 *
 * // Export to Prometheus format
 * const output = metrics.toPrometheus();
 * ```
 */
export class MetricsCollector {
  private metrics: Map<string, MetricStorage> = new Map();
  private analyticsEngine?: AnalyticsEngineDataset;
  private slowQueryThresholdMs: number = 100;

  /**
   * Create a new MetricsCollector instance.
   *
   * @param config - Optional configuration
   * @param config.analyticsEngine - Workers Analytics Engine binding for real-time analytics
   * @param config.slowQueryThresholdMs - Threshold in ms for slow query detection (default: 100)
   */
  constructor(config?: {
    analyticsEngine?: AnalyticsEngineDataset;
    slowQueryThresholdMs?: number;
  }) {
    this.analyticsEngine = config?.analyticsEngine;
    this.slowQueryThresholdMs = config?.slowQueryThresholdMs ?? 100;

    // Auto-register all standard metrics
    this.registerAll();
  }

  /**
   * Register all predefined metrics.
   */
  private registerAll(): void {
    for (const metric of Object.values(METRICS)) {
      this.register(metric);
    }
  }

  /**
   * Register a metric definition.
   *
   * @param definition - Metric definition
   */
  register(definition: MetricDefinition): void {
    if (this.metrics.has(definition.name)) {
      return; // Already registered
    }

    this.metrics.set(definition.name, {
      definition,
      values: new Map(),
    });
  }

  /**
   * Generate a key for storing labeled metric values.
   */
  private labelsToKey(labels: Labels): string {
    if (Object.keys(labels).length === 0) return '';
    return Object.entries(labels)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => `${k}="${v}"`)
      .join(',');
  }

  /**
   * Get or create a metric value entry.
   */
  private getOrCreateValue(name: string, labels: Labels): MetricValue | null {
    const storage = this.metrics.get(name);
    if (!storage) return null;

    const key = this.labelsToKey(labels);
    let value = storage.values.get(key);

    if (!value) {
      const def = storage.definition;
      value = {
        value: 0,
        labels,
        timestamp: Date.now(),
      };

      // Initialize histogram buckets if needed
      if (def.type === 'histogram' && def.buckets) {
        value.buckets = def.buckets.map((le) => ({ le, count: 0 }));
        value.buckets.push({ le: Infinity, count: 0 }); // +Inf bucket
        value.sum = 0;
        value.count = 0;
      }

      storage.values.set(key, value);
    }

    return value;
  }

  // ============================================================================
  // Counter Operations
  // ============================================================================

  /**
   * Increment a counter by 1 or a specified amount.
   *
   * @param name - Metric name
   * @param labels - Optional labels
   * @param amount - Amount to increment (default: 1)
   */
  inc(name: string, labels: Labels = {}, amount: number = 1): void {
    const value = this.getOrCreateValue(name, labels);
    if (value) {
      value.value += amount;
      value.timestamp = Date.now();
    }
  }

  // ============================================================================
  // Gauge Operations
  // ============================================================================

  /**
   * Set a gauge to a specific value.
   *
   * @param name - Metric name
   * @param value - Value to set
   * @param labels - Optional labels
   */
  set(name: string, value: number, labels: Labels = {}): void {
    const metricValue = this.getOrCreateValue(name, labels);
    if (metricValue) {
      metricValue.value = value;
      metricValue.timestamp = Date.now();
    }
  }

  /**
   * Increment a gauge.
   *
   * @param name - Metric name
   * @param labels - Optional labels
   * @param amount - Amount to increment (default: 1)
   */
  incGauge(name: string, labels: Labels = {}, amount: number = 1): void {
    this.inc(name, labels, amount);
  }

  /**
   * Decrement a gauge.
   *
   * @param name - Metric name
   * @param labels - Optional labels
   * @param amount - Amount to decrement (default: 1)
   */
  dec(name: string, labels: Labels = {}, amount: number = 1): void {
    const value = this.getOrCreateValue(name, labels);
    if (value) {
      value.value -= amount;
      value.timestamp = Date.now();
    }
  }

  // ============================================================================
  // Histogram Operations
  // ============================================================================

  /**
   * Observe a value for a histogram.
   *
   * @param name - Metric name
   * @param value - Value to observe
   * @param labels - Optional labels
   */
  observe(name: string, value: number, labels: Labels = {}): void {
    const metricValue = this.getOrCreateValue(name, labels);
    if (!metricValue || !metricValue.buckets) return;

    // Update bucket counts
    for (const bucket of metricValue.buckets) {
      if (value <= bucket.le) {
        bucket.count++;
      }
    }

    // Update sum and count
    metricValue.sum = (metricValue.sum ?? 0) + value;
    metricValue.count = (metricValue.count ?? 0) + 1;
    metricValue.timestamp = Date.now();
  }

  /**
   * Create a timer that records duration on completion.
   *
   * @param name - Histogram metric name
   * @param labels - Optional labels
   * @returns Timer object with end() method
   *
   * @example
   * ```typescript
   * const timer = metrics.startTimer(METRICS.QUERY_DURATION.name, { operation: 'find' });
   * // ... do work ...
   * timer.end(); // Records the duration
   * ```
   */
  startTimer(name: string, labels: Labels = {}): { end: () => number } {
    const start = performance.now();
    return {
      end: (): number => {
        const duration = (performance.now() - start) / 1000; // Convert to seconds
        this.observe(name, duration, labels);
        return duration;
      },
    };
  }

  // ============================================================================
  // Convenience Methods
  // ============================================================================

  /**
   * Record a query operation with duration tracking.
   *
   * @param operation - Operation type (find, findOne, aggregate, etc.)
   * @param collection - Collection name
   * @param durationMs - Duration in milliseconds
   * @param success - Whether the query succeeded
   */
  recordQuery(
    operation: string,
    collection: string,
    durationMs: number,
    success: boolean = true
  ): void {
    const labels = { operation, collection };
    const durationSec = durationMs / 1000;

    // Record duration
    this.observe(METRICS.QUERY_DURATION.name, durationSec, labels);

    // Record count
    this.inc(METRICS.QUERY_COUNT.name, { ...labels, status: success ? 'success' : 'error' });

    // Check for slow query
    if (durationMs > this.slowQueryThresholdMs) {
      this.inc(METRICS.SLOW_QUERY_COUNT.name, labels);
    }

    // Write to Analytics Engine if available
    this.writeToAnalytics('query', {
      operation,
      collection,
      duration_ms: durationMs,
      success,
    });
  }

  /**
   * Record a write operation.
   *
   * @param operation - Operation type (insert, update, delete)
   * @param collection - Collection name
   * @param count - Number of documents affected
   */
  recordWrite(operation: 'insert' | 'update' | 'delete', collection: string, count: number = 1): void {
    const metricName = {
      insert: METRICS.INSERTS_TOTAL.name,
      update: METRICS.UPDATES_TOTAL.name,
      delete: METRICS.DELETES_TOTAL.name,
    }[operation];

    this.inc(metricName, { collection }, count);

    // Write to Analytics Engine if available
    this.writeToAnalytics('write', {
      operation,
      collection,
      count,
    });
  }

  /**
   * Record a cache operation.
   *
   * @param cacheType - Type of cache (buffer, parquet, etc.)
   * @param hit - Whether it was a cache hit
   */
  recordCacheAccess(cacheType: string, hit: boolean): void {
    const labels = { cache_type: cacheType };
    if (hit) {
      this.inc(METRICS.CACHE_HITS.name, labels);
    } else {
      this.inc(METRICS.CACHE_MISSES.name, labels);
    }
  }

  /**
   * Record R2 storage operation.
   *
   * @param operation - Operation type (get, put, delete, list)
   * @param durationMs - Duration in milliseconds
   * @param bytes - Number of bytes transferred
   * @param error - Error type if operation failed
   */
  recordR2Operation(
    operation: string,
    durationMs: number,
    bytes?: number,
    error?: string
  ): void {
    const labels = { operation };

    // Record operation count
    if (operation === 'get' || operation === 'head' || operation === 'list') {
      this.inc(METRICS.R2_READS.name, labels);
      if (bytes) {
        this.inc(METRICS.R2_BYTES_READ.name, {}, bytes);
      }
    } else {
      this.inc(METRICS.R2_WRITES.name, labels);
      if (bytes) {
        this.inc(METRICS.R2_BYTES_WRITTEN.name, {}, bytes);
      }
    }

    // Record duration
    this.observe(METRICS.R2_OPERATION_DURATION.name, durationMs / 1000, labels);

    // Record error if present
    if (error) {
      this.inc(METRICS.R2_ERRORS.name, { operation, error_type: error });
    }

    // Write to Analytics Engine if available
    this.writeToAnalytics('r2', {
      operation,
      duration_ms: durationMs,
      bytes: bytes ?? 0,
      error: error ?? '',
    });
  }

  /**
   * Record compaction operation.
   *
   * @param collection - Collection name
   * @param durationMs - Duration in milliseconds
   * @param filesProcessed - Number of files processed
   * @param bytesBefore - Total bytes before compaction
   * @param bytesAfter - Total bytes after compaction
   * @param success - Whether compaction succeeded
   */
  recordCompaction(
    collection: string,
    durationMs: number,
    filesProcessed: number,
    bytesBefore: number,
    bytesAfter: number,
    success: boolean = true
  ): void {
    const labels = { collection };

    // Record duration
    this.observe(METRICS.COMPACTION_DURATION.name, durationMs / 1000, labels);

    // Record files processed
    this.inc(METRICS.COMPACTION_FILES_PROCESSED.name, labels, filesProcessed);

    // Record bytes saved
    const bytesSaved = bytesBefore - bytesAfter;
    if (bytesSaved > 0) {
      this.inc(METRICS.COMPACTION_BYTES_SAVED.name, labels, bytesSaved);
    }

    // Record cycle count
    this.inc(METRICS.COMPACTION_CYCLES.name, { ...labels, status: success ? 'success' : 'error' });

    // Write to Analytics Engine if available
    this.writeToAnalytics('compaction', {
      collection,
      duration_ms: durationMs,
      files_processed: filesProcessed,
      bytes_before: bytesBefore,
      bytes_after: bytesAfter,
      bytes_saved: bytesSaved,
      success,
    });
  }

  /**
   * Record HTTP request.
   *
   * @param method - HTTP method
   * @param path - Request path
   * @param statusCode - Response status code
   * @param durationMs - Duration in milliseconds
   * @param requestSize - Request body size in bytes
   * @param responseSize - Response body size in bytes
   */
  recordHttpRequest(
    method: string,
    path: string,
    statusCode: number,
    durationMs: number,
    requestSize?: number,
    responseSize?: number
  ): void {
    const labels = { method, path: this.normalizePath(path) };

    // Record request count
    this.inc(METRICS.HTTP_REQUESTS_TOTAL.name, { ...labels, status: String(statusCode) });

    // Record duration
    this.observe(METRICS.HTTP_REQUEST_DURATION.name, durationMs / 1000, labels);

    // Record sizes
    if (requestSize !== undefined) {
      this.observe(METRICS.HTTP_REQUEST_SIZE.name, requestSize, labels);
    }
    if (responseSize !== undefined) {
      this.observe(METRICS.HTTP_RESPONSE_SIZE.name, responseSize, labels);
    }

    // Write to Analytics Engine if available
    this.writeToAnalytics('http', {
      method,
      path: labels.path,
      status: statusCode,
      duration_ms: durationMs,
      request_size: requestSize ?? 0,
      response_size: responseSize ?? 0,
    });
  }

  /**
   * Normalize a path for metric labels.
   * Replaces dynamic segments (UUIDs, IDs) with placeholders.
   */
  private normalizePath(path: string): string {
    return path
      // Replace UUIDs
      .replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi, ':id')
      // Replace ObjectIds
      .replace(/[0-9a-f]{24}/gi, ':id')
      // Replace numeric IDs
      .replace(/\/\d+(?=\/|$)/g, '/:id');
  }

  // ============================================================================
  // Per-Shard Metrics
  // ============================================================================

  /**
   * Record a shard query operation with duration tracking.
   *
   * @param shard - Shard identifier (e.g., 'shard-0', 'shard-1')
   * @param operation - Operation type (find, insert, update, delete, aggregate)
   * @param durationMs - Duration in milliseconds
   * @param success - Whether the query succeeded
   */
  recordShardQuery(
    shard: string,
    operation: string,
    durationMs: number,
    success: boolean = true
  ): void {
    const durationSec = durationMs / 1000;
    const opLabel = { shard, operation };
    const statusLabel = { shard, operation, status: success ? 'success' : 'error' };

    // Record query count with status
    this.inc(METRICS.SHARD_QUERY_COUNT.name, statusLabel);

    // Record duration histogram
    this.observe(METRICS.SHARD_QUERY_DURATION.name, durationSec, opLabel);

    // Record operation count
    this.inc(METRICS.SHARD_OPERATIONS.name, opLabel);

    // Write to Analytics Engine if available
    this.writeToAnalytics('shard_query', {
      shard,
      operation,
      duration_ms: durationMs,
      success,
    });
  }

  /**
   * Record a shard error.
   *
   * @param shard - Shard identifier
   * @param errorType - Type of error (timeout, connection, validation, etc.)
   */
  recordShardError(shard: string, errorType: string): void {
    this.inc(METRICS.SHARD_ERRORS.name, { shard, error_type: errorType });

    // Write to Analytics Engine if available
    this.writeToAnalytics('shard_error', {
      shard,
      error_type: errorType,
    });
  }

  /**
   * Update shard statistics.
   *
   * @param shard - Shard identifier
   * @param stats - Shard statistics to update
   */
  updateShardStats(
    shard: string,
    stats: {
      documentCount?: number;
      sizeBytes?: number;
      writeRate?: number;
      p50LatencyMs?: number;
      p99LatencyMs?: number;
    }
  ): void {
    const labels = { shard };

    if (stats.documentCount !== undefined) {
      this.set(METRICS.SHARD_DOCUMENT_COUNT.name, stats.documentCount, labels);
    }
    if (stats.sizeBytes !== undefined) {
      this.set(METRICS.SHARD_SIZE_BYTES.name, stats.sizeBytes, labels);
    }
    if (stats.writeRate !== undefined) {
      this.set(METRICS.SHARD_WRITE_RATE.name, stats.writeRate, labels);
    }
    if (stats.p50LatencyMs !== undefined) {
      this.set(METRICS.SHARD_LATENCY_P50.name, stats.p50LatencyMs / 1000, labels);
    }
    if (stats.p99LatencyMs !== undefined) {
      this.set(METRICS.SHARD_LATENCY_P99.name, stats.p99LatencyMs / 1000, labels);
    }
  }

  // ============================================================================
  // Per-Collection Metrics
  // ============================================================================

  /**
   * Record a collection operation with duration tracking.
   *
   * @param collection - Collection name
   * @param operation - Operation type (find, insert, update, delete, aggregate)
   * @param durationMs - Duration in milliseconds
   * @param database - Database name (optional, defaults to 'default')
   * @param success - Whether the operation succeeded
   */
  recordCollectionOperation(
    collection: string,
    operation: string,
    durationMs: number,
    database: string = 'default',
    success: boolean = true
  ): void {
    const durationSec = durationMs / 1000;
    const baseLabels = { collection, database };
    const opLabels = { collection, database, operation };

    // Record operation count
    this.inc(METRICS.COLLECTION_OPERATIONS.name, opLabels);

    // Record duration histogram
    this.observe(METRICS.COLLECTION_QUERY_DURATION.name, durationSec, opLabels);

    // Record read/write counts
    if (['find', 'findOne', 'aggregate', 'count', 'distinct'].includes(operation)) {
      this.inc(METRICS.COLLECTION_READ_COUNT.name, baseLabels);
    } else if (['insert', 'insertOne', 'insertMany', 'update', 'updateOne', 'updateMany', 'delete', 'deleteOne', 'deleteMany', 'replaceOne'].includes(operation)) {
      this.inc(METRICS.COLLECTION_WRITE_COUNT.name, baseLabels);
    }

    // Record error if failed
    if (!success) {
      this.inc(METRICS.COLLECTION_ERRORS.name, { collection, database, error_type: 'operation_failed' });
    }

    // Write to Analytics Engine if available
    this.writeToAnalytics('collection_operation', {
      collection,
      database,
      operation,
      duration_ms: durationMs,
      success,
    });
  }

  /**
   * Record a collection error.
   *
   * @param collection - Collection name
   * @param errorType - Type of error (validation, timeout, etc.)
   * @param database - Database name (optional, defaults to 'default')
   */
  recordCollectionError(collection: string, errorType: string, database: string = 'default'): void {
    this.inc(METRICS.COLLECTION_ERRORS.name, { collection, database, error_type: errorType });

    // Write to Analytics Engine if available
    this.writeToAnalytics('collection_error', {
      collection,
      database,
      error_type: errorType,
    });
  }

  /**
   * Record a collection scan (query without index).
   *
   * @param collection - Collection name
   * @param database - Database name (optional, defaults to 'default')
   */
  recordCollectionScan(collection: string, database: string = 'default'): void {
    this.inc(METRICS.COLLECTION_SCAN_COUNT.name, { collection, database });
  }

  /**
   * Record index usage.
   *
   * @param collection - Collection name
   * @param indexName - Name of the index used
   * @param database - Database name (optional, defaults to 'default')
   */
  recordIndexUsage(collection: string, indexName: string, database: string = 'default'): void {
    this.inc(METRICS.COLLECTION_INDEX_USAGE.name, { collection, database, index_name: indexName });
  }

  /**
   * Update collection statistics.
   *
   * @param collection - Collection name
   * @param stats - Collection statistics to update
   * @param database - Database name (optional, defaults to 'default')
   */
  updateCollectionStats(
    collection: string,
    stats: {
      documentCount?: number;
      sizeBytes?: number;
    },
    database: string = 'default'
  ): void {
    const labels = { collection, database };

    if (stats.documentCount !== undefined) {
      this.set(METRICS.COLLECTION_DOCUMENT_COUNT.name, stats.documentCount, labels);
    }
    if (stats.sizeBytes !== undefined) {
      this.set(METRICS.COLLECTION_SIZE_BYTES.name, stats.sizeBytes, labels);
    }
  }

  // ============================================================================
  // Aggregated Metrics Queries
  // ============================================================================

  /**
   * Get all metrics for a specific shard.
   *
   * @param shard - Shard identifier
   * @returns Object containing all shard metrics
   */
  getShardMetrics(shard: string): {
    queryCount: { success: number; error: number };
    operationsByType: Record<string, number>;
    errors: Record<string, number>;
    documentCount: number | null;
    sizeBytes: number | null;
    writeRate: number | null;
    p50Latency: number | null;
    p99Latency: number | null;
    queryDurationStats: { sum: number; count: number; avg: number } | null;
  } {
    // Aggregate query counts by status
    const successCount = this.getValue(METRICS.SHARD_QUERY_COUNT.name, { shard, operation: 'find', status: 'success' }) ?? 0;
    const errorCount = this.getValue(METRICS.SHARD_QUERY_COUNT.name, { shard, operation: 'find', status: 'error' }) ?? 0;

    // Get operations by type
    const operationsByType: Record<string, number> = {};
    const operations = ['find', 'insert', 'update', 'delete', 'aggregate'];
    for (const op of operations) {
      const count = this.getValue(METRICS.SHARD_OPERATIONS.name, { shard, operation: op });
      if (count !== null) {
        operationsByType[op] = count;
      }
    }

    // Get errors by type
    const errors = this.getMetricsByLabelPrefix(METRICS.SHARD_ERRORS.name, { shard });

    // Get gauge values
    const labels = { shard };

    return {
      queryCount: { success: successCount, error: errorCount },
      operationsByType,
      errors,
      documentCount: this.getValue(METRICS.SHARD_DOCUMENT_COUNT.name, labels),
      sizeBytes: this.getValue(METRICS.SHARD_SIZE_BYTES.name, labels),
      writeRate: this.getValue(METRICS.SHARD_WRITE_RATE.name, labels),
      p50Latency: this.getValue(METRICS.SHARD_LATENCY_P50.name, labels),
      p99Latency: this.getValue(METRICS.SHARD_LATENCY_P99.name, labels),
      queryDurationStats: this.getHistogramStats(METRICS.SHARD_QUERY_DURATION.name, { shard, operation: 'find' }),
    };
  }

  /**
   * Get all metrics for a specific collection.
   *
   * @param collection - Collection name
   * @param database - Database name (optional, defaults to 'default')
   * @returns Object containing all collection metrics
   */
  getCollectionMetrics(collection: string, database: string = 'default'): {
    operationsByType: Record<string, number>;
    errors: Record<string, number>;
    documentCount: number | null;
    sizeBytes: number | null;
    readCount: number | null;
    writeCount: number | null;
    scanCount: number | null;
    indexUsage: Record<string, number>;
    queryDurationStats: Record<string, { sum: number; count: number; avg: number } | null>;
  } {
    const baseLabels = { collection, database };

    // Get operations by type
    const operationsByType: Record<string, number> = {};
    const operations = ['find', 'findOne', 'insert', 'insertOne', 'insertMany', 'update', 'updateOne', 'updateMany', 'delete', 'deleteOne', 'deleteMany', 'aggregate'];
    for (const op of operations) {
      const count = this.getValue(METRICS.COLLECTION_OPERATIONS.name, { collection, database, operation: op });
      if (count !== null) {
        operationsByType[op] = count;
      }
    }

    // Get errors by type
    const errors = this.getMetricsByLabelPrefix(METRICS.COLLECTION_ERRORS.name, baseLabels);

    // Get index usage
    const indexUsage = this.getMetricsByLabelPrefix(METRICS.COLLECTION_INDEX_USAGE.name, baseLabels);

    // Get query duration stats per operation
    const queryDurationStats: Record<string, { sum: number; count: number; avg: number } | null> = {};
    for (const op of operations) {
      const stats = this.getHistogramStats(METRICS.COLLECTION_QUERY_DURATION.name, { collection, database, operation: op });
      if (stats) {
        queryDurationStats[op] = stats;
      }
    }

    return {
      operationsByType,
      errors,
      documentCount: this.getValue(METRICS.COLLECTION_DOCUMENT_COUNT.name, baseLabels),
      sizeBytes: this.getValue(METRICS.COLLECTION_SIZE_BYTES.name, baseLabels),
      readCount: this.getValue(METRICS.COLLECTION_READ_COUNT.name, baseLabels),
      writeCount: this.getValue(METRICS.COLLECTION_WRITE_COUNT.name, baseLabels),
      scanCount: this.getValue(METRICS.COLLECTION_SCAN_COUNT.name, baseLabels),
      indexUsage,
      queryDurationStats,
    };
  }

  /**
   * Get metrics values matching a label prefix.
   * Used for aggregating error types, index usage, etc.
   *
   * @param metricName - Name of the metric
   * @param labelPrefix - Labels that must match
   * @returns Record of remaining label values to metric values
   */
  private getMetricsByLabelPrefix(metricName: string, labelPrefix: Labels): Record<string, number> {
    const storage = this.metrics.get(metricName);
    if (!storage) return {};

    const result: Record<string, number> = {};
    const prefixKeys = Object.keys(labelPrefix);

    for (const [key, value] of storage.values) {
      // Check if this entry matches the label prefix
      let matches = true;
      for (const prefixKey of prefixKeys) {
        if (!key.includes(`${prefixKey}="${labelPrefix[prefixKey]}"`)) {
          matches = false;
          break;
        }
      }

      if (matches) {
        // Extract the remaining label value (e.g., error_type or index_name)
        const labelParts = key.split(',');
        for (const part of labelParts) {
          const [labelKey, labelValue] = part.split('=');
          if (labelKey && labelValue && !prefixKeys.includes(labelKey)) {
            const cleanValue = labelValue.replace(/"/g, '');
            result[cleanValue] = value.value;
          }
        }
      }
    }

    return result;
  }

  /**
   * Get summary of all shards.
   *
   * @returns Array of shard summaries
   */
  getAllShardSummaries(): Array<{
    shard: string;
    totalQueries: number;
    totalErrors: number;
    documentCount: number | null;
    sizeBytes: number | null;
  }> {
    const shards = this.getUniqueLabels(METRICS.SHARD_QUERY_COUNT.name, 'shard');
    const gaugeShards = this.getUniqueLabels(METRICS.SHARD_DOCUMENT_COUNT.name, 'shard');

    // Merge shard lists
    const allShards = new Set([...shards, ...gaugeShards]);

    return Array.from(allShards).map(shard => {
      const successCount = this.getValue(METRICS.SHARD_QUERY_COUNT.name, { shard, operation: 'find', status: 'success' }) ?? 0;
      const errorCount = this.getValue(METRICS.SHARD_QUERY_COUNT.name, { shard, operation: 'find', status: 'error' }) ?? 0;
      const errors = this.getMetricsByLabelPrefix(METRICS.SHARD_ERRORS.name, { shard });
      const totalErrors = Object.values(errors).reduce((sum, count) => sum + count, 0) + errorCount;

      return {
        shard,
        totalQueries: successCount + errorCount,
        totalErrors,
        documentCount: this.getValue(METRICS.SHARD_DOCUMENT_COUNT.name, { shard }),
        sizeBytes: this.getValue(METRICS.SHARD_SIZE_BYTES.name, { shard }),
      };
    });
  }

  /**
   * Get summary of all collections.
   *
   * @returns Array of collection summaries
   */
  getAllCollectionSummaries(): Array<{
    collection: string;
    database: string;
    totalOperations: number;
    totalErrors: number;
    documentCount: number | null;
    sizeBytes: number | null;
  }> {
    // Get unique collection/database pairs
    const pairs = this.getUniqueLabelPairs(METRICS.COLLECTION_OPERATIONS.name, 'collection', 'database');
    const gaugePairs = this.getUniqueLabelPairs(METRICS.COLLECTION_DOCUMENT_COUNT.name, 'collection', 'database');

    // Merge pairs
    const allPairs = new Map<string, { collection: string; database: string }>();
    for (const pair of [...pairs, ...gaugePairs]) {
      const key = `${pair.collection}:${pair.database}`;
      allPairs.set(key, pair);
    }

    return Array.from(allPairs.values()).map(({ collection, database }) => {
      const metrics = this.getCollectionMetrics(collection, database);
      const totalOperations = Object.values(metrics.operationsByType).reduce((sum, count) => sum + count, 0);
      const totalErrors = Object.values(metrics.errors).reduce((sum, count) => sum + count, 0);

      return {
        collection,
        database,
        totalOperations,
        totalErrors,
        documentCount: metrics.documentCount,
        sizeBytes: metrics.sizeBytes,
      };
    });
  }

  /**
   * Get unique values for a label across all entries of a metric.
   */
  private getUniqueLabels(metricName: string, labelName: string): string[] {
    const storage = this.metrics.get(metricName);
    if (!storage) return [];

    const values = new Set<string>();
    const regex = new RegExp(`${labelName}="([^"]+)"`);

    for (const key of storage.values.keys()) {
      const match = key.match(regex);
      if (match && match[1]) {
        values.add(match[1]);
      }
    }

    return Array.from(values);
  }

  /**
   * Get unique pairs of label values across all entries of a metric.
   */
  private getUniqueLabelPairs(
    metricName: string,
    label1: string,
    label2: string
  ): Array<{ collection: string; database: string }> {
    const storage = this.metrics.get(metricName);
    if (!storage) return [];

    const pairs = new Map<string, { collection: string; database: string }>();
    const regex1 = new RegExp(`${label1}="([^"]+)"`);
    const regex2 = new RegExp(`${label2}="([^"]+)"`);

    for (const key of storage.values.keys()) {
      const match1 = key.match(regex1);
      const match2 = key.match(regex2);
      if (match1?.[1] && match2?.[1]) {
        const pairKey = `${match1[1]}:${match2[1]}`;
        if (!pairs.has(pairKey)) {
          pairs.set(pairKey, { collection: match1[1], database: match2[1] });
        }
      }
    }

    return Array.from(pairs.values());
  }

  // ============================================================================
  // Export Methods
  // ============================================================================

  /**
   * Export metrics in Prometheus text format.
   *
   * @returns Prometheus-formatted metrics string
   */
  toPrometheus(): string {
    const lines: string[] = [];

    for (const [name, storage] of this.metrics) {
      const def = storage.definition;

      // Skip metrics with no values
      if (storage.values.size === 0) continue;

      // HELP and TYPE lines
      lines.push(`# HELP ${name} ${def.help}`);
      lines.push(`# TYPE ${name} ${def.type}`);

      for (const [key, value] of storage.values) {
        const labelStr = key ? `{${key}}` : '';

        if (def.type === 'histogram' && value.buckets) {
          // Output histogram buckets
          for (const bucket of value.buckets) {
            const bucketLabel = bucket.le === Infinity ? '+Inf' : String(bucket.le);
            const bucketLabelStr = key
              ? `{${key},le="${bucketLabel}"}`
              : `{le="${bucketLabel}"}`;
            lines.push(`${name}_bucket${bucketLabelStr} ${bucket.count}`);
          }
          lines.push(`${name}_sum${labelStr} ${value.sum ?? 0}`);
          lines.push(`${name}_count${labelStr} ${value.count ?? 0}`);
        } else {
          lines.push(`${name}${labelStr} ${value.value}`);
        }
      }

      lines.push(''); // Empty line between metrics
    }

    return lines.join('\n');
  }

  /**
   * Export metrics as JSON for structured logging.
   *
   * @returns JSON-serializable metrics object
   */
  toJSON(): Record<string, unknown> {
    const result: Record<string, unknown> = {};

    for (const [name, storage] of this.metrics) {
      const def = storage.definition;
      const values: Record<string, unknown>[] = [];

      for (const [, value] of storage.values) {
        if (def.type === 'histogram') {
          values.push({
            labels: value.labels,
            sum: value.sum,
            count: value.count,
            buckets: value.buckets?.map((b) => ({
              le: b.le === Infinity ? '+Inf' : b.le,
              count: b.count,
            })),
          });
        } else {
          values.push({
            labels: value.labels,
            value: value.value,
          });
        }
      }

      if (values.length > 0) {
        result[name] = {
          type: def.type,
          help: def.help,
          values,
        };
      }
    }

    return result;
  }

  /**
   * Get a single metric value.
   *
   * @param name - Metric name
   * @param labels - Labels to match
   * @returns Metric value or null if not found
   */
  getValue(name: string, labels: Labels = {}): number | null {
    const storage = this.metrics.get(name);
    if (!storage) return null;

    const key = this.labelsToKey(labels);
    const value = storage.values.get(key);
    return value?.value ?? null;
  }

  /**
   * Get histogram statistics.
   *
   * @param name - Histogram metric name
   * @param labels - Labels to match
   * @returns Histogram stats or null if not found
   */
  getHistogramStats(
    name: string,
    labels: Labels = {}
  ): { sum: number; count: number; avg: number; buckets: HistogramBucket[] } | null {
    const storage = this.metrics.get(name);
    if (!storage) return null;

    const key = this.labelsToKey(labels);
    const value = storage.values.get(key);
    if (!value || !value.buckets) return null;

    return {
      sum: value.sum ?? 0,
      count: value.count ?? 0,
      avg: value.count ? (value.sum ?? 0) / value.count : 0,
      buckets: value.buckets,
    };
  }

  /**
   * Reset all metrics.
   */
  reset(): void {
    for (const storage of this.metrics.values()) {
      storage.values.clear();
    }
  }

  /**
   * Reset a specific metric.
   *
   * @param name - Metric name to reset
   */
  resetMetric(name: string): void {
    const storage = this.metrics.get(name);
    if (storage) {
      storage.values.clear();
    }
  }

  // ============================================================================
  // Workers Analytics Integration
  // ============================================================================

  /**
   * Write a data point to Workers Analytics Engine.
   */
  private writeToAnalytics(category: string, data: Record<string, unknown>): void {
    if (!this.analyticsEngine) return;

    try {
      const blobs: string[] = [category];
      const doubles: number[] = [];

      // Add string values to blobs, numbers to doubles
      for (const [key, value] of Object.entries(data)) {
        if (typeof value === 'string' || typeof value === 'boolean') {
          blobs.push(`${key}=${String(value)}`);
        } else if (typeof value === 'number') {
          doubles.push(value);
        }
      }

      this.analyticsEngine.writeDataPoint({
        blobs,
        doubles,
        indexes: [category],
      });
    } catch {
      // Silently ignore analytics errors to avoid impacting main operations
    }
  }

  /**
   * Set the Analytics Engine binding.
   *
   * @param analyticsEngine - Workers Analytics Engine dataset binding
   */
  setAnalyticsEngine(analyticsEngine: AnalyticsEngineDataset): void {
    this.analyticsEngine = analyticsEngine;
  }
}

// ============================================================================
// Global Metrics Instance
// ============================================================================

/**
 * Global metrics collector instance.
 *
 * Use this for application-wide metrics collection.
 * For testing or isolated scenarios, create a new MetricsCollector instance.
 */
let globalMetrics: MetricsCollector | null = null;

/**
 * Get the global metrics collector instance.
 *
 * Creates a new instance if one doesn't exist.
 */
export function getMetrics(): MetricsCollector {
  if (!globalMetrics) {
    globalMetrics = new MetricsCollector();
  }
  return globalMetrics;
}

/**
 * Set the global metrics collector instance.
 *
 * Useful for initializing with custom configuration.
 *
 * @param metrics - MetricsCollector instance to use as global
 */
export function setMetrics(metrics: MetricsCollector): void {
  globalMetrics = metrics;
}

/**
 * Reset the global metrics collector.
 *
 * Creates a fresh instance, useful for testing.
 */
export function resetMetrics(): void {
  globalMetrics = null;
}

// ============================================================================
// Structured Logging
// ============================================================================

/**
 * Log level enumeration
 */
export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

/**
 * Structured log entry
 */
export interface LogEntry {
  timestamp: string;
  level: LogLevel;
  message: string;
  [key: string]: unknown;
}

/**
 * Create a structured log entry.
 *
 * @param level - Log level
 * @param message - Log message
 * @param data - Additional data to include
 * @returns Structured log entry
 */
export function createLogEntry(
  level: LogLevel,
  message: string,
  data: Record<string, unknown> = {}
): LogEntry {
  return {
    timestamp: new Date().toISOString(),
    level,
    message,
    ...data,
  };
}

/**
 * Format a log entry as JSON for structured logging.
 *
 * @param entry - Log entry to format
 * @returns JSON string
 */
export function formatLogEntry(entry: LogEntry): string {
  return JSON.stringify(entry);
}

/**
 * Logger class for structured logging.
 */
export class StructuredLogger {
  private context: Record<string, unknown> = {};

  /**
   * Create a new StructuredLogger.
   *
   * @param context - Default context to include in all log entries
   */
  constructor(context: Record<string, unknown> = {}) {
    this.context = context;
  }

  /**
   * Create a child logger with additional context.
   */
  child(context: Record<string, unknown>): StructuredLogger {
    return new StructuredLogger({ ...this.context, ...context });
  }

  /**
   * Log at debug level.
   */
  debug(message: string, data: Record<string, unknown> = {}): void {
    this.log('debug', message, data);
  }

  /**
   * Log at info level.
   */
  info(message: string, data: Record<string, unknown> = {}): void {
    this.log('info', message, data);
  }

  /**
   * Log at warn level.
   */
  warn(message: string, data: Record<string, unknown> = {}): void {
    this.log('warn', message, data);
  }

  /**
   * Log at error level.
   */
  error(message: string, data: Record<string, unknown> = {}): void {
    this.log('error', message, data);
  }

  /**
   * Log at specified level.
   */
  private log(level: LogLevel, message: string, data: Record<string, unknown>): void {
    const entry = createLogEntry(level, message, { ...this.context, ...data });
    console.log(formatLogEntry(entry));
  }
}

// ============================================================================
// Timing Utilities
// ============================================================================

/**
 * Measure the execution time of an async function.
 *
 * @param fn - Async function to measure
 * @returns Result and duration in milliseconds
 *
 * @example
 * ```typescript
 * const { result, durationMs } = await timed(async () => {
 *   return await db.collection('users').find().toArray();
 * });
 * metrics.recordQuery('find', 'users', durationMs, true);
 * ```
 */
export async function timed<T>(fn: () => Promise<T>): Promise<{ result: T; durationMs: number }> {
  const start = performance.now();
  const result = await fn();
  const durationMs = performance.now() - start;
  return { result, durationMs };
}

/**
 * Measure the execution time of a sync function.
 *
 * @param fn - Function to measure
 * @returns Result and duration in milliseconds
 */
export function timedSync<T>(fn: () => T): { result: T; durationMs: number } {
  const start = performance.now();
  const result = fn();
  const durationMs = performance.now() - start;
  return { result, durationMs };
}
