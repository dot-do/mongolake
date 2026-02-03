/**
 * Metrics Module Tests
 *
 * Comprehensive tests for the MetricsCollector class and related utilities.
 * Tests cover:
 * - Counter operations (increment)
 * - Gauge operations (set, inc, dec)
 * - Histogram operations (observe, startTimer)
 * - Prometheus export format
 * - JSON export format
 * - Workers Analytics integration
 * - Structured logging
 * - Convenience methods for query/write/cache/R2/compaction/HTTP metrics
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  MetricsCollector,
  METRICS,
  DEFAULT_LATENCY_BUCKETS,
  DEFAULT_SIZE_BUCKETS,
  getMetrics,
  setMetrics,
  resetMetrics,
  StructuredLogger,
  createLogEntry,
  formatLogEntry,
  timed,
  timedSync,
  type AnalyticsEngineDataset,
  type Labels,
  type MetricDefinition,
} from '../../../src/metrics/index.js';

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Create a mock Analytics Engine for testing
 */
function createMockAnalyticsEngine(): AnalyticsEngineDataset & { calls: Array<{ blobs?: string[]; doubles?: number[]; indexes?: string[] }> } {
  const calls: Array<{ blobs?: string[]; doubles?: number[]; indexes?: string[] }> = [];
  return {
    writeDataPoint: vi.fn((data) => {
      calls.push(data);
    }),
    calls,
  };
}

// ============================================================================
// MetricsCollector Tests
// ============================================================================

describe('MetricsCollector', () => {
  let metrics: MetricsCollector;

  beforeEach(() => {
    metrics = new MetricsCollector();
  });

  describe('constructor', () => {
    it('should auto-register all predefined metrics', () => {
      // Check that some predefined metrics are registered
      expect(metrics.getValue(METRICS.QUERY_COUNT.name)).toBeNull(); // No values yet, but registered
      expect(metrics.getValue(METRICS.INSERTS_TOTAL.name)).toBeNull();
      expect(metrics.getValue(METRICS.CACHE_HITS.name)).toBeNull();
    });

    it('should accept custom slow query threshold', () => {
      const customMetrics = new MetricsCollector({ slowQueryThresholdMs: 500 });
      // The threshold is internal, so we test by recording a query
      customMetrics.recordQuery('find', 'users', 200, true);
      // 200ms < 500ms, so no slow query should be recorded
      expect(customMetrics.getValue(METRICS.SLOW_QUERY_COUNT.name, { operation: 'find', collection: 'users' })).toBeNull();
    });

    it('should accept Analytics Engine binding', () => {
      const analyticsEngine = createMockAnalyticsEngine();
      const metricsWithAnalytics = new MetricsCollector({ analyticsEngine });

      // Record something that triggers analytics
      metricsWithAnalytics.recordQuery('find', 'users', 50, true);

      expect(analyticsEngine.writeDataPoint).toHaveBeenCalled();
    });
  });

  // --------------------------------------------------------------------------
  // Counter Operations
  // --------------------------------------------------------------------------

  describe('counter operations', () => {
    it('should increment a counter by 1', () => {
      metrics.inc(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'users', status: 'success' });

      const value = metrics.getValue(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'users', status: 'success' });
      expect(value).toBe(1);
    });

    it('should increment a counter by a custom amount', () => {
      metrics.inc(METRICS.INSERTS_TOTAL.name, { collection: 'users' }, 5);

      const value = metrics.getValue(METRICS.INSERTS_TOTAL.name, { collection: 'users' });
      expect(value).toBe(5);
    });

    it('should accumulate counter increments', () => {
      metrics.inc(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'users', status: 'success' });
      metrics.inc(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'users', status: 'success' });
      metrics.inc(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'users', status: 'success' }, 3);

      const value = metrics.getValue(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'users', status: 'success' });
      expect(value).toBe(5);
    });

    it('should keep separate counters for different label combinations', () => {
      metrics.inc(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'users', status: 'success' });
      metrics.inc(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'orders', status: 'success' });
      metrics.inc(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'users', status: 'error' });

      expect(metrics.getValue(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'users', status: 'success' })).toBe(1);
      expect(metrics.getValue(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'orders', status: 'success' })).toBe(1);
      expect(metrics.getValue(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'users', status: 'error' })).toBe(1);
    });
  });

  // --------------------------------------------------------------------------
  // Gauge Operations
  // --------------------------------------------------------------------------

  describe('gauge operations', () => {
    it('should set a gauge value', () => {
      metrics.set(METRICS.BUFFER_SIZE.name, 1024, { shard: 'shard-1' });

      const value = metrics.getValue(METRICS.BUFFER_SIZE.name, { shard: 'shard-1' });
      expect(value).toBe(1024);
    });

    it('should overwrite gauge value on subsequent sets', () => {
      metrics.set(METRICS.BUFFER_SIZE.name, 1024, { shard: 'shard-1' });
      metrics.set(METRICS.BUFFER_SIZE.name, 2048, { shard: 'shard-1' });

      const value = metrics.getValue(METRICS.BUFFER_SIZE.name, { shard: 'shard-1' });
      expect(value).toBe(2048);
    });

    it('should increment a gauge', () => {
      metrics.set(METRICS.ACTIVE_CONNECTIONS.name, 5, { protocol: 'http' });
      metrics.incGauge(METRICS.ACTIVE_CONNECTIONS.name, { protocol: 'http' }, 2);

      const value = metrics.getValue(METRICS.ACTIVE_CONNECTIONS.name, { protocol: 'http' });
      expect(value).toBe(7);
    });

    it('should decrement a gauge', () => {
      metrics.set(METRICS.ACTIVE_CONNECTIONS.name, 10, { protocol: 'http' });
      metrics.dec(METRICS.ACTIVE_CONNECTIONS.name, { protocol: 'http' }, 3);

      const value = metrics.getValue(METRICS.ACTIVE_CONNECTIONS.name, { protocol: 'http' });
      expect(value).toBe(7);
    });

    it('should allow negative gauge values', () => {
      metrics.set(METRICS.BUFFER_SIZE.name, 0, { shard: 'shard-1' });
      metrics.dec(METRICS.BUFFER_SIZE.name, { shard: 'shard-1' }, 5);

      const value = metrics.getValue(METRICS.BUFFER_SIZE.name, { shard: 'shard-1' });
      expect(value).toBe(-5);
    });
  });

  // --------------------------------------------------------------------------
  // Histogram Operations
  // --------------------------------------------------------------------------

  describe('histogram operations', () => {
    it('should observe values in histogram', () => {
      metrics.observe(METRICS.QUERY_DURATION.name, 0.05, { operation: 'find', collection: 'users' });
      metrics.observe(METRICS.QUERY_DURATION.name, 0.1, { operation: 'find', collection: 'users' });
      metrics.observe(METRICS.QUERY_DURATION.name, 0.2, { operation: 'find', collection: 'users' });

      const stats = metrics.getHistogramStats(METRICS.QUERY_DURATION.name, { operation: 'find', collection: 'users' });
      expect(stats).not.toBeNull();
      expect(stats!.count).toBe(3);
      expect(stats!.sum).toBeCloseTo(0.35, 5);
      expect(stats!.avg).toBeCloseTo(0.1167, 2);
    });

    it('should populate histogram buckets correctly', () => {
      // Observe values that fall into different buckets
      metrics.observe(METRICS.QUERY_DURATION.name, 0.002, { operation: 'find', collection: 'test' }); // <= 0.005
      metrics.observe(METRICS.QUERY_DURATION.name, 0.008, { operation: 'find', collection: 'test' }); // <= 0.01
      metrics.observe(METRICS.QUERY_DURATION.name, 0.05, { operation: 'find', collection: 'test' });  // <= 0.05
      metrics.observe(METRICS.QUERY_DURATION.name, 2.0, { operation: 'find', collection: 'test' });   // <= 2.5

      const stats = metrics.getHistogramStats(METRICS.QUERY_DURATION.name, { operation: 'find', collection: 'test' });
      expect(stats).not.toBeNull();

      // Cumulative bucket counts
      const buckets = stats!.buckets;
      const bucket001 = buckets.find(b => b.le === 0.001);
      const bucket005 = buckets.find(b => b.le === 0.005);
      const bucket01 = buckets.find(b => b.le === 0.01);
      const bucket05 = buckets.find(b => b.le === 0.05);
      const bucket25 = buckets.find(b => b.le === 2.5);
      const bucketInf = buckets.find(b => b.le === Infinity);

      expect(bucket001?.count).toBe(0); // 0.002 > 0.001
      expect(bucket005?.count).toBe(1); // 0.002 <= 0.005
      expect(bucket01?.count).toBe(2);  // 0.002, 0.008 <= 0.01
      expect(bucket05?.count).toBe(3);  // 0.002, 0.008, 0.05 <= 0.05
      expect(bucket25?.count).toBe(4);  // all 4 values <= 2.5
      expect(bucketInf?.count).toBe(4); // +Inf always has all
    });

    it('should start and end a timer', async () => {
      const timer = metrics.startTimer(METRICS.QUERY_DURATION.name, { operation: 'find', collection: 'users' });

      // Simulate some work
      await new Promise(resolve => setTimeout(resolve, 10));

      const duration = timer.end();

      expect(duration).toBeGreaterThan(0.005); // At least 5ms

      const stats = metrics.getHistogramStats(METRICS.QUERY_DURATION.name, { operation: 'find', collection: 'users' });
      expect(stats).not.toBeNull();
      expect(stats!.count).toBe(1);
    });
  });

  // --------------------------------------------------------------------------
  // Convenience Methods
  // --------------------------------------------------------------------------

  describe('recordQuery', () => {
    it('should record query duration and count', () => {
      metrics.recordQuery('find', 'users', 50, true);

      // Check count
      expect(metrics.getValue(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'users', status: 'success' })).toBe(1);

      // Check histogram
      const stats = metrics.getHistogramStats(METRICS.QUERY_DURATION.name, { operation: 'find', collection: 'users' });
      expect(stats).not.toBeNull();
      expect(stats!.count).toBe(1);
    });

    it('should record slow queries when over threshold', () => {
      metrics.recordQuery('find', 'users', 150, true); // Over 100ms threshold

      expect(metrics.getValue(METRICS.SLOW_QUERY_COUNT.name, { operation: 'find', collection: 'users' })).toBe(1);
    });

    it('should not record slow query when under threshold', () => {
      metrics.recordQuery('find', 'users', 50, true); // Under 100ms threshold

      expect(metrics.getValue(METRICS.SLOW_QUERY_COUNT.name, { operation: 'find', collection: 'users' })).toBeNull();
    });

    it('should record error status', () => {
      metrics.recordQuery('find', 'users', 50, false);

      expect(metrics.getValue(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'users', status: 'error' })).toBe(1);
    });
  });

  describe('recordWrite', () => {
    it('should record insert operations', () => {
      metrics.recordWrite('insert', 'users', 3);

      expect(metrics.getValue(METRICS.INSERTS_TOTAL.name, { collection: 'users' })).toBe(3);
    });

    it('should record update operations', () => {
      metrics.recordWrite('update', 'orders', 2);

      expect(metrics.getValue(METRICS.UPDATES_TOTAL.name, { collection: 'orders' })).toBe(2);
    });

    it('should record delete operations', () => {
      metrics.recordWrite('delete', 'sessions', 5);

      expect(metrics.getValue(METRICS.DELETES_TOTAL.name, { collection: 'sessions' })).toBe(5);
    });
  });

  describe('recordCacheAccess', () => {
    it('should record cache hits', () => {
      metrics.recordCacheAccess('buffer', true);
      metrics.recordCacheAccess('buffer', true);

      expect(metrics.getValue(METRICS.CACHE_HITS.name, { cache_type: 'buffer' })).toBe(2);
    });

    it('should record cache misses', () => {
      metrics.recordCacheAccess('parquet', false);

      expect(metrics.getValue(METRICS.CACHE_MISSES.name, { cache_type: 'parquet' })).toBe(1);
    });
  });

  describe('recordR2Operation', () => {
    it('should record read operations', () => {
      metrics.recordR2Operation('get', 100, 1024);

      expect(metrics.getValue(METRICS.R2_READS.name, { operation: 'get' })).toBe(1);
      expect(metrics.getValue(METRICS.R2_BYTES_READ.name, {})).toBe(1024);
    });

    it('should record write operations', () => {
      metrics.recordR2Operation('put', 200, 2048);

      expect(metrics.getValue(METRICS.R2_WRITES.name, { operation: 'put' })).toBe(1);
      expect(metrics.getValue(METRICS.R2_BYTES_WRITTEN.name, {})).toBe(2048);
    });

    it('should record operation duration', () => {
      metrics.recordR2Operation('get', 150);

      const stats = metrics.getHistogramStats(METRICS.R2_OPERATION_DURATION.name, { operation: 'get' });
      expect(stats).not.toBeNull();
      expect(stats!.count).toBe(1);
      expect(stats!.sum).toBeCloseTo(0.15, 3); // 150ms = 0.15s
    });

    it('should record errors', () => {
      metrics.recordR2Operation('get', 500, 0, 'timeout');

      expect(metrics.getValue(METRICS.R2_ERRORS.name, { operation: 'get', error_type: 'timeout' })).toBe(1);
    });

    it('should categorize list as read operation', () => {
      metrics.recordR2Operation('list', 50);

      expect(metrics.getValue(METRICS.R2_READS.name, { operation: 'list' })).toBe(1);
    });

    it('should categorize head as read operation', () => {
      metrics.recordR2Operation('head', 30);

      expect(metrics.getValue(METRICS.R2_READS.name, { operation: 'head' })).toBe(1);
    });
  });

  describe('recordCompaction', () => {
    it('should record compaction metrics', () => {
      metrics.recordCompaction('users', 5000, 10, 10000000, 2000000, true);

      // Check duration histogram
      const stats = metrics.getHistogramStats(METRICS.COMPACTION_DURATION.name, { collection: 'users' });
      expect(stats).not.toBeNull();
      expect(stats!.count).toBe(1);

      // Check files processed
      expect(metrics.getValue(METRICS.COMPACTION_FILES_PROCESSED.name, { collection: 'users' })).toBe(10);

      // Check bytes saved (10MB - 2MB = 8MB)
      expect(metrics.getValue(METRICS.COMPACTION_BYTES_SAVED.name, { collection: 'users' })).toBe(8000000);

      // Check cycle count
      expect(metrics.getValue(METRICS.COMPACTION_CYCLES.name, { collection: 'users', status: 'success' })).toBe(1);
    });

    it('should record compaction errors', () => {
      metrics.recordCompaction('users', 1000, 5, 5000000, 5000000, false);

      expect(metrics.getValue(METRICS.COMPACTION_CYCLES.name, { collection: 'users', status: 'error' })).toBe(1);
    });

    it('should not record negative bytes saved', () => {
      // If bytesAfter > bytesBefore, no bytes saved
      metrics.recordCompaction('users', 1000, 2, 1000, 2000, true);

      // Bytes saved should not be recorded (or be 0)
      expect(metrics.getValue(METRICS.COMPACTION_BYTES_SAVED.name, { collection: 'users' })).toBeNull();
    });
  });

  describe('recordHttpRequest', () => {
    it('should record HTTP request metrics', () => {
      metrics.recordHttpRequest('GET', '/api/users', 200, 50, 100, 5000);

      // Check request count
      expect(metrics.getValue(METRICS.HTTP_REQUESTS_TOTAL.name, { method: 'GET', path: '/api/users', status: '200' })).toBe(1);

      // Check duration histogram
      const durationStats = metrics.getHistogramStats(METRICS.HTTP_REQUEST_DURATION.name, { method: 'GET', path: '/api/users' });
      expect(durationStats).not.toBeNull();
      expect(durationStats!.count).toBe(1);

      // Check request size histogram
      const reqSizeStats = metrics.getHistogramStats(METRICS.HTTP_REQUEST_SIZE.name, { method: 'GET', path: '/api/users' });
      expect(reqSizeStats).not.toBeNull();
      expect(reqSizeStats!.count).toBe(1);

      // Check response size histogram
      const respSizeStats = metrics.getHistogramStats(METRICS.HTTP_RESPONSE_SIZE.name, { method: 'GET', path: '/api/users' });
      expect(respSizeStats).not.toBeNull();
      expect(respSizeStats!.count).toBe(1);
    });

    it('should normalize paths with UUIDs', () => {
      metrics.recordHttpRequest('GET', '/api/users/123e4567-e89b-12d3-a456-426614174000', 200, 50);

      expect(metrics.getValue(METRICS.HTTP_REQUESTS_TOTAL.name, { method: 'GET', path: '/api/users/:id', status: '200' })).toBe(1);
    });

    it('should normalize paths with ObjectIds', () => {
      metrics.recordHttpRequest('GET', '/api/users/507f1f77bcf86cd799439011', 200, 50);

      expect(metrics.getValue(METRICS.HTTP_REQUESTS_TOTAL.name, { method: 'GET', path: '/api/users/:id', status: '200' })).toBe(1);
    });

    it('should normalize paths with numeric IDs', () => {
      metrics.recordHttpRequest('GET', '/api/orders/12345', 200, 50);

      expect(metrics.getValue(METRICS.HTTP_REQUESTS_TOTAL.name, { method: 'GET', path: '/api/orders/:id', status: '200' })).toBe(1);
    });
  });

  // --------------------------------------------------------------------------
  // Export Methods
  // --------------------------------------------------------------------------

  describe('toPrometheus', () => {
    it('should export metrics in Prometheus format', () => {
      metrics.inc(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'users', status: 'success' }, 5);
      metrics.set(METRICS.BUFFER_SIZE.name, 1024, { shard: 'shard-1' });

      const output = metrics.toPrometheus();

      expect(output).toContain('# HELP mongolake_query_total');
      expect(output).toContain('# TYPE mongolake_query_total counter');
      expect(output).toContain('mongolake_query_total{collection="users",operation="find",status="success"} 5');

      expect(output).toContain('# HELP mongolake_buffer_size_bytes');
      expect(output).toContain('# TYPE mongolake_buffer_size_bytes gauge');
      expect(output).toContain('mongolake_buffer_size_bytes{shard="shard-1"} 1024');
    });

    it('should export histogram with buckets', () => {
      metrics.observe(METRICS.QUERY_DURATION.name, 0.05, { operation: 'find', collection: 'users' });

      const output = metrics.toPrometheus();

      expect(output).toContain('# HELP mongolake_query_duration_seconds');
      expect(output).toContain('# TYPE mongolake_query_duration_seconds histogram');
      expect(output).toContain('mongolake_query_duration_seconds_bucket{collection="users",operation="find",le="0.05"} 1');
      expect(output).toContain('mongolake_query_duration_seconds_bucket{collection="users",operation="find",le="+Inf"} 1');
      expect(output).toContain('mongolake_query_duration_seconds_sum{collection="users",operation="find"}');
      expect(output).toContain('mongolake_query_duration_seconds_count{collection="users",operation="find"} 1');
    });

    it('should handle metrics without labels', () => {
      metrics.inc(METRICS.R2_BYTES_READ.name, {}, 5000);

      const output = metrics.toPrometheus();

      expect(output).toContain('mongolake_r2_bytes_read_total 5000');
    });
  });

  describe('toJSON', () => {
    it('should export metrics as JSON', () => {
      metrics.inc(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'users', status: 'success' }, 3);

      const output = metrics.toJSON();

      expect(output[METRICS.QUERY_COUNT.name]).toBeDefined();
      const queryMetric = output[METRICS.QUERY_COUNT.name] as { type: string; values: Array<{ labels: Labels; value: number }> };
      expect(queryMetric.type).toBe('counter');
      expect(queryMetric.values).toHaveLength(1);
      expect(queryMetric.values[0].value).toBe(3);
    });

    it('should export histograms with bucket information', () => {
      metrics.observe(METRICS.QUERY_DURATION.name, 0.1, { operation: 'find', collection: 'users' });

      const output = metrics.toJSON();

      const histMetric = output[METRICS.QUERY_DURATION.name] as {
        type: string;
        values: Array<{ labels: Labels; sum: number; count: number; buckets: Array<{ le: number | string; count: number }> }>;
      };
      expect(histMetric.type).toBe('histogram');
      expect(histMetric.values[0].count).toBe(1);
      expect(histMetric.values[0].buckets).toBeDefined();
    });
  });

  // --------------------------------------------------------------------------
  // Reset Operations
  // --------------------------------------------------------------------------

  describe('reset operations', () => {
    it('should reset all metrics', () => {
      metrics.inc(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'users', status: 'success' });
      metrics.set(METRICS.BUFFER_SIZE.name, 1024, { shard: 'shard-1' });

      metrics.reset();

      expect(metrics.getValue(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'users', status: 'success' })).toBeNull();
      expect(metrics.getValue(METRICS.BUFFER_SIZE.name, { shard: 'shard-1' })).toBeNull();
    });

    it('should reset a specific metric', () => {
      metrics.inc(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'users', status: 'success' });
      metrics.inc(METRICS.INSERTS_TOTAL.name, { collection: 'users' });

      metrics.resetMetric(METRICS.QUERY_COUNT.name);

      expect(metrics.getValue(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'users', status: 'success' })).toBeNull();
      expect(metrics.getValue(METRICS.INSERTS_TOTAL.name, { collection: 'users' })).toBe(1);
    });
  });

  // --------------------------------------------------------------------------
  // Analytics Engine Integration
  // --------------------------------------------------------------------------

  describe('Workers Analytics integration', () => {
    it('should write query events to Analytics Engine', () => {
      const analyticsEngine = createMockAnalyticsEngine();
      const metricsWithAnalytics = new MetricsCollector({ analyticsEngine });

      metricsWithAnalytics.recordQuery('find', 'users', 50, true);

      expect(analyticsEngine.writeDataPoint).toHaveBeenCalled();
      expect(analyticsEngine.calls[0].blobs).toContain('query');
      expect(analyticsEngine.calls[0].indexes).toContain('query');
    });

    it('should write R2 events to Analytics Engine', () => {
      const analyticsEngine = createMockAnalyticsEngine();
      const metricsWithAnalytics = new MetricsCollector({ analyticsEngine });

      metricsWithAnalytics.recordR2Operation('put', 100, 1024);

      expect(analyticsEngine.writeDataPoint).toHaveBeenCalled();
      expect(analyticsEngine.calls[0].blobs).toContain('r2');
    });

    it('should write compaction events to Analytics Engine', () => {
      const analyticsEngine = createMockAnalyticsEngine();
      const metricsWithAnalytics = new MetricsCollector({ analyticsEngine });

      metricsWithAnalytics.recordCompaction('users', 5000, 10, 10000000, 2000000, true);

      expect(analyticsEngine.writeDataPoint).toHaveBeenCalled();
      expect(analyticsEngine.calls[0].blobs).toContain('compaction');
    });

    it('should write HTTP events to Analytics Engine', () => {
      const analyticsEngine = createMockAnalyticsEngine();
      const metricsWithAnalytics = new MetricsCollector({ analyticsEngine });

      metricsWithAnalytics.recordHttpRequest('GET', '/api/users', 200, 50);

      expect(analyticsEngine.writeDataPoint).toHaveBeenCalled();
      expect(analyticsEngine.calls[0].blobs).toContain('http');
    });

    it('should allow setting Analytics Engine after construction', () => {
      const analyticsEngine = createMockAnalyticsEngine();

      metrics.setAnalyticsEngine(analyticsEngine);
      metrics.recordQuery('find', 'users', 50, true);

      expect(analyticsEngine.writeDataPoint).toHaveBeenCalled();
    });

    it('should silently handle Analytics Engine errors', () => {
      const failingAnalytics: AnalyticsEngineDataset = {
        writeDataPoint: () => {
          throw new Error('Analytics failed');
        },
      };
      const metricsWithFailingAnalytics = new MetricsCollector({ analyticsEngine: failingAnalytics });

      // Should not throw
      expect(() => {
        metricsWithFailingAnalytics.recordQuery('find', 'users', 50, true);
      }).not.toThrow();
    });
  });

  // --------------------------------------------------------------------------
  // Custom Metric Registration
  // --------------------------------------------------------------------------

  describe('custom metric registration', () => {
    it('should allow registering custom metrics', () => {
      const customMetric: MetricDefinition = {
        name: 'mongolake_custom_total',
        type: 'counter',
        help: 'Custom test metric',
        labelNames: ['category'],
      };

      metrics.register(customMetric);
      metrics.inc('mongolake_custom_total', { category: 'test' }, 5);

      expect(metrics.getValue('mongolake_custom_total', { category: 'test' })).toBe(5);
    });

    it('should not re-register existing metrics', () => {
      // Pre-register some value
      metrics.inc(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'users', status: 'success' }, 10);

      // Try to re-register
      metrics.register(METRICS.QUERY_COUNT);

      // Value should still be there
      expect(metrics.getValue(METRICS.QUERY_COUNT.name, { operation: 'find', collection: 'users', status: 'success' })).toBe(10);
    });
  });
});

// ============================================================================
// Global Metrics Instance Tests
// ============================================================================

describe('Global metrics functions', () => {
  beforeEach(() => {
    resetMetrics();
  });

  it('should return the same instance on multiple getMetrics calls', () => {
    const metrics1 = getMetrics();
    const metrics2 = getMetrics();

    expect(metrics1).toBe(metrics2);
  });

  it('should allow setting a custom metrics instance', () => {
    const customMetrics = new MetricsCollector();
    setMetrics(customMetrics);

    expect(getMetrics()).toBe(customMetrics);
  });

  it('should reset to fresh instance after resetMetrics', () => {
    const metrics1 = getMetrics();
    resetMetrics();
    const metrics2 = getMetrics();

    expect(metrics1).not.toBe(metrics2);
  });
});

// ============================================================================
// Structured Logger Tests
// ============================================================================

describe('StructuredLogger', () => {
  let consoleSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
  });

  it('should log at debug level', () => {
    const logger = new StructuredLogger();
    logger.debug('Debug message', { key: 'value' });

    expect(consoleSpy).toHaveBeenCalled();
    const logged = JSON.parse(consoleSpy.mock.calls[0][0]);
    expect(logged.level).toBe('debug');
    expect(logged.message).toBe('Debug message');
    expect(logged.key).toBe('value');
  });

  it('should log at info level', () => {
    const logger = new StructuredLogger();
    logger.info('Info message');

    const logged = JSON.parse(consoleSpy.mock.calls[0][0]);
    expect(logged.level).toBe('info');
  });

  it('should log at warn level', () => {
    const logger = new StructuredLogger();
    logger.warn('Warning message');

    const logged = JSON.parse(consoleSpy.mock.calls[0][0]);
    expect(logged.level).toBe('warn');
  });

  it('should log at error level', () => {
    const logger = new StructuredLogger();
    logger.error('Error message');

    const logged = JSON.parse(consoleSpy.mock.calls[0][0]);
    expect(logged.level).toBe('error');
  });

  it('should include timestamp', () => {
    const logger = new StructuredLogger();
    logger.info('Test');

    const logged = JSON.parse(consoleSpy.mock.calls[0][0]);
    expect(logged.timestamp).toBeDefined();
    expect(new Date(logged.timestamp).getTime()).not.toBeNaN();
  });

  it('should include context in all logs', () => {
    const logger = new StructuredLogger({ service: 'test-service', version: '1.0' });
    logger.info('Test message');

    const logged = JSON.parse(consoleSpy.mock.calls[0][0]);
    expect(logged.service).toBe('test-service');
    expect(logged.version).toBe('1.0');
  });

  it('should create child logger with additional context', () => {
    const logger = new StructuredLogger({ service: 'parent' });
    const childLogger = logger.child({ requestId: '123' });
    childLogger.info('Child message');

    const logged = JSON.parse(consoleSpy.mock.calls[0][0]);
    expect(logged.service).toBe('parent');
    expect(logged.requestId).toBe('123');
  });
});

describe('createLogEntry', () => {
  it('should create a structured log entry', () => {
    const entry = createLogEntry('info', 'Test message', { userId: 'user-123' });

    expect(entry.level).toBe('info');
    expect(entry.message).toBe('Test message');
    expect(entry.userId).toBe('user-123');
    expect(entry.timestamp).toBeDefined();
  });
});

describe('formatLogEntry', () => {
  it('should format log entry as JSON', () => {
    const entry = createLogEntry('info', 'Test');
    const formatted = formatLogEntry(entry);

    expect(typeof formatted).toBe('string');
    const parsed = JSON.parse(formatted);
    expect(parsed.level).toBe('info');
  });
});

// ============================================================================
// Timing Utilities Tests
// ============================================================================

describe('Timing utilities', () => {
  describe('timed', () => {
    it('should measure async function execution time', async () => {
      const { result, durationMs } = await timed(async () => {
        await new Promise(resolve => setTimeout(resolve, 10));
        return 'done';
      });

      expect(result).toBe('done');
      expect(durationMs).toBeGreaterThan(5);
    });

    it('should propagate errors from async function', async () => {
      await expect(timed(async () => {
        throw new Error('Test error');
      })).rejects.toThrow('Test error');
    });
  });

  describe('timedSync', () => {
    it('should measure sync function execution time', () => {
      const { result, durationMs } = timedSync(() => {
        let sum = 0;
        for (let i = 0; i < 10000; i++) {
          sum += i;
        }
        return sum;
      });

      expect(result).toBe(49995000);
      expect(durationMs).toBeGreaterThanOrEqual(0);
    });

    it('should propagate errors from sync function', () => {
      expect(() => timedSync(() => {
        throw new Error('Sync error');
      })).toThrow('Sync error');
    });
  });
});

// ============================================================================
// Predefined Metrics Constants Tests
// ============================================================================

describe('Predefined metrics', () => {
  it('should have correct latency bucket defaults', () => {
    expect(DEFAULT_LATENCY_BUCKETS).toEqual([
      0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
    ]);
  });

  it('should have correct size bucket defaults', () => {
    expect(DEFAULT_SIZE_BUCKETS).toEqual([
      1024, 10240, 102400, 1048576, 10485760, 104857600,
    ]);
  });

  it('should have all required query metrics', () => {
    expect(METRICS.QUERY_DURATION).toBeDefined();
    expect(METRICS.QUERY_COUNT).toBeDefined();
    expect(METRICS.SLOW_QUERY_COUNT).toBeDefined();
  });

  it('should have all required operation metrics', () => {
    expect(METRICS.INSERTS_TOTAL).toBeDefined();
    expect(METRICS.UPDATES_TOTAL).toBeDefined();
    expect(METRICS.DELETES_TOTAL).toBeDefined();
    expect(METRICS.FINDS_TOTAL).toBeDefined();
  });

  it('should have all required cache metrics', () => {
    expect(METRICS.CACHE_HITS).toBeDefined();
    expect(METRICS.CACHE_MISSES).toBeDefined();
    expect(METRICS.CACHE_EVICTIONS).toBeDefined();
    expect(METRICS.CACHE_SIZE).toBeDefined();
    expect(METRICS.CACHE_ENTRIES).toBeDefined();
  });

  it('should have all required R2 metrics', () => {
    expect(METRICS.R2_READS).toBeDefined();
    expect(METRICS.R2_WRITES).toBeDefined();
    expect(METRICS.R2_BYTES_READ).toBeDefined();
    expect(METRICS.R2_BYTES_WRITTEN).toBeDefined();
    expect(METRICS.R2_OPERATION_DURATION).toBeDefined();
    expect(METRICS.R2_ERRORS).toBeDefined();
  });

  it('should have all required compaction metrics', () => {
    expect(METRICS.COMPACTION_DURATION).toBeDefined();
    expect(METRICS.COMPACTION_FILES_PROCESSED).toBeDefined();
    expect(METRICS.COMPACTION_BYTES_SAVED).toBeDefined();
    expect(METRICS.COMPACTION_CYCLES).toBeDefined();
  });

  it('should have all required connection metrics', () => {
    expect(METRICS.ACTIVE_CONNECTIONS).toBeDefined();
    expect(METRICS.CONNECTION_ERRORS).toBeDefined();
    expect(METRICS.CONNECTIONS_TOTAL).toBeDefined();
  });

  it('should have all required buffer metrics', () => {
    expect(METRICS.BUFFER_SIZE).toBeDefined();
    expect(METRICS.BUFFER_DOCS).toBeDefined();
    expect(METRICS.FLUSH_OPERATIONS).toBeDefined();
    expect(METRICS.FLUSH_DURATION).toBeDefined();
  });

  it('should have all required HTTP metrics', () => {
    expect(METRICS.HTTP_REQUESTS_TOTAL).toBeDefined();
    expect(METRICS.HTTP_REQUEST_DURATION).toBeDefined();
    expect(METRICS.HTTP_REQUEST_SIZE).toBeDefined();
    expect(METRICS.HTTP_RESPONSE_SIZE).toBeDefined();
  });

  it('should have all required per-shard metrics', () => {
    expect(METRICS.SHARD_QUERY_COUNT).toBeDefined();
    expect(METRICS.SHARD_QUERY_DURATION).toBeDefined();
    expect(METRICS.SHARD_ERRORS).toBeDefined();
    expect(METRICS.SHARD_OPERATIONS).toBeDefined();
    expect(METRICS.SHARD_LATENCY_P50).toBeDefined();
    expect(METRICS.SHARD_LATENCY_P99).toBeDefined();
  });

  it('should have all required per-collection metrics', () => {
    expect(METRICS.COLLECTION_DOCUMENT_COUNT).toBeDefined();
    expect(METRICS.COLLECTION_SIZE_BYTES).toBeDefined();
    expect(METRICS.COLLECTION_OPERATIONS).toBeDefined();
    expect(METRICS.COLLECTION_QUERY_DURATION).toBeDefined();
    expect(METRICS.COLLECTION_ERRORS).toBeDefined();
    expect(METRICS.COLLECTION_READ_COUNT).toBeDefined();
    expect(METRICS.COLLECTION_WRITE_COUNT).toBeDefined();
    expect(METRICS.COLLECTION_SCAN_COUNT).toBeDefined();
    expect(METRICS.COLLECTION_INDEX_USAGE).toBeDefined();
  });
});

// ============================================================================
// Per-Shard Metrics Tests
// ============================================================================

describe('Per-Shard Metrics', () => {
  let metrics: MetricsCollector;

  beforeEach(() => {
    metrics = new MetricsCollector();
  });

  describe('recordShardQuery', () => {
    it('should record successful shard query', () => {
      metrics.recordShardQuery('shard-0', 'find', 50, true);

      expect(metrics.getValue(METRICS.SHARD_QUERY_COUNT.name, { shard: 'shard-0', operation: 'find', status: 'success' })).toBe(1);
      expect(metrics.getValue(METRICS.SHARD_OPERATIONS.name, { shard: 'shard-0', operation: 'find' })).toBe(1);

      const stats = metrics.getHistogramStats(METRICS.SHARD_QUERY_DURATION.name, { shard: 'shard-0', operation: 'find' });
      expect(stats).not.toBeNull();
      expect(stats!.count).toBe(1);
      expect(stats!.sum).toBeCloseTo(0.05, 3);
    });

    it('should record failed shard query', () => {
      metrics.recordShardQuery('shard-1', 'insert', 100, false);

      expect(metrics.getValue(METRICS.SHARD_QUERY_COUNT.name, { shard: 'shard-1', operation: 'insert', status: 'error' })).toBe(1);
    });

    it('should accumulate multiple queries to same shard', () => {
      metrics.recordShardQuery('shard-0', 'find', 50, true);
      metrics.recordShardQuery('shard-0', 'find', 75, true);
      metrics.recordShardQuery('shard-0', 'find', 100, false);

      expect(metrics.getValue(METRICS.SHARD_QUERY_COUNT.name, { shard: 'shard-0', operation: 'find', status: 'success' })).toBe(2);
      expect(metrics.getValue(METRICS.SHARD_QUERY_COUNT.name, { shard: 'shard-0', operation: 'find', status: 'error' })).toBe(1);
      expect(metrics.getValue(METRICS.SHARD_OPERATIONS.name, { shard: 'shard-0', operation: 'find' })).toBe(3);
    });

    it('should track different operations separately', () => {
      metrics.recordShardQuery('shard-0', 'find', 50, true);
      metrics.recordShardQuery('shard-0', 'insert', 30, true);
      metrics.recordShardQuery('shard-0', 'update', 40, true);

      expect(metrics.getValue(METRICS.SHARD_OPERATIONS.name, { shard: 'shard-0', operation: 'find' })).toBe(1);
      expect(metrics.getValue(METRICS.SHARD_OPERATIONS.name, { shard: 'shard-0', operation: 'insert' })).toBe(1);
      expect(metrics.getValue(METRICS.SHARD_OPERATIONS.name, { shard: 'shard-0', operation: 'update' })).toBe(1);
    });

    it('should track different shards separately', () => {
      metrics.recordShardQuery('shard-0', 'find', 50, true);
      metrics.recordShardQuery('shard-1', 'find', 60, true);
      metrics.recordShardQuery('shard-2', 'find', 70, true);

      expect(metrics.getValue(METRICS.SHARD_OPERATIONS.name, { shard: 'shard-0', operation: 'find' })).toBe(1);
      expect(metrics.getValue(METRICS.SHARD_OPERATIONS.name, { shard: 'shard-1', operation: 'find' })).toBe(1);
      expect(metrics.getValue(METRICS.SHARD_OPERATIONS.name, { shard: 'shard-2', operation: 'find' })).toBe(1);
    });
  });

  describe('recordShardError', () => {
    it('should record shard errors', () => {
      metrics.recordShardError('shard-0', 'timeout');

      expect(metrics.getValue(METRICS.SHARD_ERRORS.name, { shard: 'shard-0', error_type: 'timeout' })).toBe(1);
    });

    it('should track different error types separately', () => {
      metrics.recordShardError('shard-0', 'timeout');
      metrics.recordShardError('shard-0', 'timeout');
      metrics.recordShardError('shard-0', 'connection');
      metrics.recordShardError('shard-0', 'validation');

      expect(metrics.getValue(METRICS.SHARD_ERRORS.name, { shard: 'shard-0', error_type: 'timeout' })).toBe(2);
      expect(metrics.getValue(METRICS.SHARD_ERRORS.name, { shard: 'shard-0', error_type: 'connection' })).toBe(1);
      expect(metrics.getValue(METRICS.SHARD_ERRORS.name, { shard: 'shard-0', error_type: 'validation' })).toBe(1);
    });
  });

  describe('updateShardStats', () => {
    it('should update shard document count', () => {
      metrics.updateShardStats('shard-0', { documentCount: 1000 });

      expect(metrics.getValue(METRICS.SHARD_DOCUMENT_COUNT.name, { shard: 'shard-0' })).toBe(1000);
    });

    it('should update shard size', () => {
      metrics.updateShardStats('shard-0', { sizeBytes: 1024 * 1024 });

      expect(metrics.getValue(METRICS.SHARD_SIZE_BYTES.name, { shard: 'shard-0' })).toBe(1048576);
    });

    it('should update shard write rate', () => {
      metrics.updateShardStats('shard-0', { writeRate: 500 });

      expect(metrics.getValue(METRICS.SHARD_WRITE_RATE.name, { shard: 'shard-0' })).toBe(500);
    });

    it('should update latency percentiles', () => {
      metrics.updateShardStats('shard-0', { p50LatencyMs: 10, p99LatencyMs: 100 });

      expect(metrics.getValue(METRICS.SHARD_LATENCY_P50.name, { shard: 'shard-0' })).toBeCloseTo(0.01, 5);
      expect(metrics.getValue(METRICS.SHARD_LATENCY_P99.name, { shard: 'shard-0' })).toBeCloseTo(0.1, 5);
    });

    it('should update multiple stats at once', () => {
      metrics.updateShardStats('shard-0', {
        documentCount: 5000,
        sizeBytes: 10 * 1024 * 1024,
        writeRate: 200,
        p50LatencyMs: 5,
        p99LatencyMs: 50,
      });

      expect(metrics.getValue(METRICS.SHARD_DOCUMENT_COUNT.name, { shard: 'shard-0' })).toBe(5000);
      expect(metrics.getValue(METRICS.SHARD_SIZE_BYTES.name, { shard: 'shard-0' })).toBe(10485760);
      expect(metrics.getValue(METRICS.SHARD_WRITE_RATE.name, { shard: 'shard-0' })).toBe(200);
    });
  });

  describe('getShardMetrics', () => {
    it('should return all metrics for a shard', () => {
      metrics.recordShardQuery('shard-0', 'find', 50, true);
      metrics.recordShardQuery('shard-0', 'find', 75, false);
      metrics.recordShardQuery('shard-0', 'insert', 30, true);
      metrics.recordShardError('shard-0', 'timeout');
      metrics.updateShardStats('shard-0', { documentCount: 1000, sizeBytes: 1024 * 1024 });

      const shardMetrics = metrics.getShardMetrics('shard-0');

      expect(shardMetrics.queryCount.success).toBe(1);
      expect(shardMetrics.queryCount.error).toBe(1);
      expect(shardMetrics.operationsByType.find).toBe(2);
      expect(shardMetrics.operationsByType.insert).toBe(1);
      expect(shardMetrics.errors.timeout).toBe(1);
      expect(shardMetrics.documentCount).toBe(1000);
      expect(shardMetrics.sizeBytes).toBe(1048576);
    });

    it('should return empty metrics for unknown shard', () => {
      const shardMetrics = metrics.getShardMetrics('unknown-shard');

      expect(shardMetrics.queryCount.success).toBe(0);
      expect(shardMetrics.queryCount.error).toBe(0);
      expect(Object.keys(shardMetrics.operationsByType)).toHaveLength(0);
      expect(Object.keys(shardMetrics.errors)).toHaveLength(0);
      expect(shardMetrics.documentCount).toBeNull();
    });
  });

  describe('getAllShardSummaries', () => {
    it('should return summaries for all shards', () => {
      metrics.recordShardQuery('shard-0', 'find', 50, true);
      metrics.recordShardQuery('shard-0', 'find', 75, true);
      metrics.recordShardQuery('shard-1', 'find', 60, true);
      metrics.recordShardQuery('shard-1', 'find', 80, false);
      metrics.updateShardStats('shard-0', { documentCount: 1000 });
      metrics.updateShardStats('shard-1', { documentCount: 2000 });

      const summaries = metrics.getAllShardSummaries();

      expect(summaries).toHaveLength(2);

      const shard0 = summaries.find(s => s.shard === 'shard-0');
      const shard1 = summaries.find(s => s.shard === 'shard-1');

      expect(shard0).toBeDefined();
      expect(shard0!.totalQueries).toBe(2);
      expect(shard0!.documentCount).toBe(1000);

      expect(shard1).toBeDefined();
      expect(shard1!.totalQueries).toBe(2);
      expect(shard1!.totalErrors).toBe(1);
      expect(shard1!.documentCount).toBe(2000);
    });

    it('should return empty array when no shards', () => {
      const summaries = metrics.getAllShardSummaries();
      expect(summaries).toHaveLength(0);
    });
  });
});

// ============================================================================
// Per-Collection Metrics Tests
// ============================================================================

describe('Per-Collection Metrics', () => {
  let metrics: MetricsCollector;

  beforeEach(() => {
    metrics = new MetricsCollector();
  });

  describe('recordCollectionOperation', () => {
    it('should record read operations', () => {
      metrics.recordCollectionOperation('users', 'find', 50);

      expect(metrics.getValue(METRICS.COLLECTION_OPERATIONS.name, { collection: 'users', database: 'default', operation: 'find' })).toBe(1);
      expect(metrics.getValue(METRICS.COLLECTION_READ_COUNT.name, { collection: 'users', database: 'default' })).toBe(1);

      const stats = metrics.getHistogramStats(METRICS.COLLECTION_QUERY_DURATION.name, { collection: 'users', database: 'default', operation: 'find' });
      expect(stats).not.toBeNull();
      expect(stats!.count).toBe(1);
    });

    it('should record write operations', () => {
      metrics.recordCollectionOperation('users', 'insert', 30);
      metrics.recordCollectionOperation('users', 'update', 40);
      metrics.recordCollectionOperation('users', 'delete', 20);

      expect(metrics.getValue(METRICS.COLLECTION_WRITE_COUNT.name, { collection: 'users', database: 'default' })).toBe(3);
    });

    it('should track operations with custom database', () => {
      metrics.recordCollectionOperation('users', 'find', 50, 'mydb');

      expect(metrics.getValue(METRICS.COLLECTION_OPERATIONS.name, { collection: 'users', database: 'mydb', operation: 'find' })).toBe(1);
      expect(metrics.getValue(METRICS.COLLECTION_READ_COUNT.name, { collection: 'users', database: 'mydb' })).toBe(1);
    });

    it('should record errors on failed operations', () => {
      metrics.recordCollectionOperation('users', 'find', 50, 'default', false);

      expect(metrics.getValue(METRICS.COLLECTION_ERRORS.name, { collection: 'users', database: 'default', error_type: 'operation_failed' })).toBe(1);
    });

    it('should accumulate multiple operations', () => {
      metrics.recordCollectionOperation('users', 'find', 50);
      metrics.recordCollectionOperation('users', 'find', 60);
      metrics.recordCollectionOperation('users', 'findOne', 30);
      metrics.recordCollectionOperation('users', 'aggregate', 100);

      expect(metrics.getValue(METRICS.COLLECTION_OPERATIONS.name, { collection: 'users', database: 'default', operation: 'find' })).toBe(2);
      expect(metrics.getValue(METRICS.COLLECTION_OPERATIONS.name, { collection: 'users', database: 'default', operation: 'findOne' })).toBe(1);
      expect(metrics.getValue(METRICS.COLLECTION_OPERATIONS.name, { collection: 'users', database: 'default', operation: 'aggregate' })).toBe(1);
      expect(metrics.getValue(METRICS.COLLECTION_READ_COUNT.name, { collection: 'users', database: 'default' })).toBe(4);
    });

    it('should track different collections separately', () => {
      metrics.recordCollectionOperation('users', 'find', 50);
      metrics.recordCollectionOperation('orders', 'find', 60);
      metrics.recordCollectionOperation('products', 'find', 70);

      expect(metrics.getValue(METRICS.COLLECTION_READ_COUNT.name, { collection: 'users', database: 'default' })).toBe(1);
      expect(metrics.getValue(METRICS.COLLECTION_READ_COUNT.name, { collection: 'orders', database: 'default' })).toBe(1);
      expect(metrics.getValue(METRICS.COLLECTION_READ_COUNT.name, { collection: 'products', database: 'default' })).toBe(1);
    });
  });

  describe('recordCollectionError', () => {
    it('should record collection errors', () => {
      metrics.recordCollectionError('users', 'validation');

      expect(metrics.getValue(METRICS.COLLECTION_ERRORS.name, { collection: 'users', database: 'default', error_type: 'validation' })).toBe(1);
    });

    it('should track different error types separately', () => {
      metrics.recordCollectionError('users', 'validation');
      metrics.recordCollectionError('users', 'validation');
      metrics.recordCollectionError('users', 'timeout');
      metrics.recordCollectionError('users', 'duplicate_key');

      expect(metrics.getValue(METRICS.COLLECTION_ERRORS.name, { collection: 'users', database: 'default', error_type: 'validation' })).toBe(2);
      expect(metrics.getValue(METRICS.COLLECTION_ERRORS.name, { collection: 'users', database: 'default', error_type: 'timeout' })).toBe(1);
      expect(metrics.getValue(METRICS.COLLECTION_ERRORS.name, { collection: 'users', database: 'default', error_type: 'duplicate_key' })).toBe(1);
    });

    it('should record errors with custom database', () => {
      metrics.recordCollectionError('users', 'timeout', 'mydb');

      expect(metrics.getValue(METRICS.COLLECTION_ERRORS.name, { collection: 'users', database: 'mydb', error_type: 'timeout' })).toBe(1);
    });
  });

  describe('recordCollectionScan', () => {
    it('should record collection scans', () => {
      metrics.recordCollectionScan('users');
      metrics.recordCollectionScan('users');

      expect(metrics.getValue(METRICS.COLLECTION_SCAN_COUNT.name, { collection: 'users', database: 'default' })).toBe(2);
    });

    it('should record scans with custom database', () => {
      metrics.recordCollectionScan('users', 'mydb');

      expect(metrics.getValue(METRICS.COLLECTION_SCAN_COUNT.name, { collection: 'users', database: 'mydb' })).toBe(1);
    });
  });

  describe('recordIndexUsage', () => {
    it('should record index usage', () => {
      metrics.recordIndexUsage('users', 'email_1');
      metrics.recordIndexUsage('users', 'email_1');
      metrics.recordIndexUsage('users', 'name_1');

      expect(metrics.getValue(METRICS.COLLECTION_INDEX_USAGE.name, { collection: 'users', database: 'default', index_name: 'email_1' })).toBe(2);
      expect(metrics.getValue(METRICS.COLLECTION_INDEX_USAGE.name, { collection: 'users', database: 'default', index_name: 'name_1' })).toBe(1);
    });

    it('should record index usage with custom database', () => {
      metrics.recordIndexUsage('users', 'email_1', 'mydb');

      expect(metrics.getValue(METRICS.COLLECTION_INDEX_USAGE.name, { collection: 'users', database: 'mydb', index_name: 'email_1' })).toBe(1);
    });
  });

  describe('updateCollectionStats', () => {
    it('should update collection document count', () => {
      metrics.updateCollectionStats('users', { documentCount: 5000 });

      expect(metrics.getValue(METRICS.COLLECTION_DOCUMENT_COUNT.name, { collection: 'users', database: 'default' })).toBe(5000);
    });

    it('should update collection size', () => {
      metrics.updateCollectionStats('users', { sizeBytes: 10 * 1024 * 1024 });

      expect(metrics.getValue(METRICS.COLLECTION_SIZE_BYTES.name, { collection: 'users', database: 'default' })).toBe(10485760);
    });

    it('should update stats with custom database', () => {
      metrics.updateCollectionStats('users', { documentCount: 1000, sizeBytes: 1024 * 1024 }, 'mydb');

      expect(metrics.getValue(METRICS.COLLECTION_DOCUMENT_COUNT.name, { collection: 'users', database: 'mydb' })).toBe(1000);
      expect(metrics.getValue(METRICS.COLLECTION_SIZE_BYTES.name, { collection: 'users', database: 'mydb' })).toBe(1048576);
    });

    it('should update multiple stats at once', () => {
      metrics.updateCollectionStats('users', { documentCount: 10000, sizeBytes: 50 * 1024 * 1024 });

      expect(metrics.getValue(METRICS.COLLECTION_DOCUMENT_COUNT.name, { collection: 'users', database: 'default' })).toBe(10000);
      expect(metrics.getValue(METRICS.COLLECTION_SIZE_BYTES.name, { collection: 'users', database: 'default' })).toBe(52428800);
    });
  });

  describe('getCollectionMetrics', () => {
    it('should return all metrics for a collection', () => {
      metrics.recordCollectionOperation('users', 'find', 50);
      metrics.recordCollectionOperation('users', 'find', 60);
      metrics.recordCollectionOperation('users', 'insert', 30);
      metrics.recordCollectionError('users', 'validation');
      metrics.recordCollectionScan('users');
      metrics.recordIndexUsage('users', 'email_1');
      metrics.updateCollectionStats('users', { documentCount: 5000, sizeBytes: 1024 * 1024 });

      const collectionMetrics = metrics.getCollectionMetrics('users');

      expect(collectionMetrics.operationsByType.find).toBe(2);
      expect(collectionMetrics.operationsByType.insert).toBe(1);
      expect(collectionMetrics.errors.validation).toBe(1);
      expect(collectionMetrics.documentCount).toBe(5000);
      expect(collectionMetrics.sizeBytes).toBe(1048576);
      expect(collectionMetrics.readCount).toBe(2);
      expect(collectionMetrics.writeCount).toBe(1);
      expect(collectionMetrics.scanCount).toBe(1);
      expect(collectionMetrics.indexUsage.email_1).toBe(1);
    });

    it('should return metrics for custom database', () => {
      metrics.recordCollectionOperation('users', 'find', 50, 'mydb');
      metrics.updateCollectionStats('users', { documentCount: 1000 }, 'mydb');

      const collectionMetrics = metrics.getCollectionMetrics('users', 'mydb');

      expect(collectionMetrics.operationsByType.find).toBe(1);
      expect(collectionMetrics.documentCount).toBe(1000);
    });

    it('should return empty metrics for unknown collection', () => {
      const collectionMetrics = metrics.getCollectionMetrics('unknown');

      expect(Object.keys(collectionMetrics.operationsByType)).toHaveLength(0);
      expect(Object.keys(collectionMetrics.errors)).toHaveLength(0);
      expect(collectionMetrics.documentCount).toBeNull();
    });
  });

  describe('getAllCollectionSummaries', () => {
    it('should return summaries for all collections', () => {
      metrics.recordCollectionOperation('users', 'find', 50);
      metrics.recordCollectionOperation('users', 'insert', 30);
      metrics.recordCollectionOperation('orders', 'find', 60);
      metrics.recordCollectionError('orders', 'timeout');
      metrics.updateCollectionStats('users', { documentCount: 1000 });
      metrics.updateCollectionStats('orders', { documentCount: 500 });

      const summaries = metrics.getAllCollectionSummaries();

      expect(summaries).toHaveLength(2);

      const users = summaries.find(s => s.collection === 'users');
      const orders = summaries.find(s => s.collection === 'orders');

      expect(users).toBeDefined();
      expect(users!.totalOperations).toBe(2);
      expect(users!.totalErrors).toBe(0);
      expect(users!.documentCount).toBe(1000);

      expect(orders).toBeDefined();
      expect(orders!.totalOperations).toBe(1);
      expect(orders!.totalErrors).toBe(1);
      expect(orders!.documentCount).toBe(500);
    });

    it('should handle collections across multiple databases', () => {
      metrics.recordCollectionOperation('users', 'find', 50, 'db1');
      metrics.recordCollectionOperation('users', 'find', 60, 'db2');

      const summaries = metrics.getAllCollectionSummaries();

      expect(summaries).toHaveLength(2);

      const db1Users = summaries.find(s => s.collection === 'users' && s.database === 'db1');
      const db2Users = summaries.find(s => s.collection === 'users' && s.database === 'db2');

      expect(db1Users).toBeDefined();
      expect(db2Users).toBeDefined();
    });

    it('should return empty array when no collections', () => {
      const summaries = metrics.getAllCollectionSummaries();
      expect(summaries).toHaveLength(0);
    });
  });
});

// ============================================================================
// Analytics Engine Integration for Shard/Collection Metrics
// ============================================================================

describe('Analytics Engine integration for granular metrics', () => {
  it('should write shard query events to Analytics Engine', () => {
    const analyticsEngine = createMockAnalyticsEngine();
    const metrics = new MetricsCollector({ analyticsEngine });

    metrics.recordShardQuery('shard-0', 'find', 50, true);

    expect(analyticsEngine.writeDataPoint).toHaveBeenCalled();
    expect(analyticsEngine.calls[0].blobs).toContain('shard_query');
  });

  it('should write shard error events to Analytics Engine', () => {
    const analyticsEngine = createMockAnalyticsEngine();
    const metrics = new MetricsCollector({ analyticsEngine });

    metrics.recordShardError('shard-0', 'timeout');

    expect(analyticsEngine.writeDataPoint).toHaveBeenCalled();
    expect(analyticsEngine.calls[0].blobs).toContain('shard_error');
  });

  it('should write collection operation events to Analytics Engine', () => {
    const analyticsEngine = createMockAnalyticsEngine();
    const metrics = new MetricsCollector({ analyticsEngine });

    metrics.recordCollectionOperation('users', 'find', 50);

    expect(analyticsEngine.writeDataPoint).toHaveBeenCalled();
    expect(analyticsEngine.calls[0].blobs).toContain('collection_operation');
  });

  it('should write collection error events to Analytics Engine', () => {
    const analyticsEngine = createMockAnalyticsEngine();
    const metrics = new MetricsCollector({ analyticsEngine });

    metrics.recordCollectionError('users', 'validation');

    expect(analyticsEngine.writeDataPoint).toHaveBeenCalled();
    expect(analyticsEngine.calls[0].blobs).toContain('collection_error');
  });
});

// ============================================================================
// Prometheus Export for Granular Metrics
// ============================================================================

describe('Prometheus export for granular metrics', () => {
  let metrics: MetricsCollector;

  beforeEach(() => {
    metrics = new MetricsCollector();
  });

  it('should export per-shard metrics in Prometheus format', () => {
    metrics.recordShardQuery('shard-0', 'find', 50, true);
    metrics.updateShardStats('shard-0', { documentCount: 1000 });

    const output = metrics.toPrometheus();

    expect(output).toContain('# HELP mongolake_shard_query_total');
    expect(output).toContain('# TYPE mongolake_shard_query_total counter');
    expect(output).toContain('mongolake_shard_query_total{operation="find",shard="shard-0",status="success"} 1');

    expect(output).toContain('# HELP mongolake_shard_document_count');
    expect(output).toContain('mongolake_shard_document_count{shard="shard-0"} 1000');
  });

  it('should export per-collection metrics in Prometheus format', () => {
    metrics.recordCollectionOperation('users', 'find', 50);
    metrics.updateCollectionStats('users', { documentCount: 5000 });

    const output = metrics.toPrometheus();

    expect(output).toContain('# HELP mongolake_collection_operations_total');
    expect(output).toContain('# TYPE mongolake_collection_operations_total counter');
    expect(output).toContain('mongolake_collection_operations_total{collection="users",database="default",operation="find"} 1');

    expect(output).toContain('# HELP mongolake_collection_document_count');
    expect(output).toContain('mongolake_collection_document_count{collection="users",database="default"} 5000');
  });

  it('should export shard query duration histogram in Prometheus format', () => {
    metrics.recordShardQuery('shard-0', 'find', 50, true);

    const output = metrics.toPrometheus();

    expect(output).toContain('# HELP mongolake_shard_query_duration_seconds');
    expect(output).toContain('# TYPE mongolake_shard_query_duration_seconds histogram');
    expect(output).toContain('mongolake_shard_query_duration_seconds_bucket');
    expect(output).toContain('mongolake_shard_query_duration_seconds_sum');
    expect(output).toContain('mongolake_shard_query_duration_seconds_count');
  });

  it('should export collection query duration histogram in Prometheus format', () => {
    metrics.recordCollectionOperation('users', 'find', 50);

    const output = metrics.toPrometheus();

    expect(output).toContain('# HELP mongolake_collection_query_duration_seconds');
    expect(output).toContain('# TYPE mongolake_collection_query_duration_seconds histogram');
    expect(output).toContain('mongolake_collection_query_duration_seconds_bucket');
  });
});

// ============================================================================
// JSON Export for Granular Metrics
// ============================================================================

describe('JSON export for granular metrics', () => {
  let metrics: MetricsCollector;

  beforeEach(() => {
    metrics = new MetricsCollector();
  });

  it('should export per-shard metrics as JSON', () => {
    metrics.recordShardQuery('shard-0', 'find', 50, true);
    metrics.updateShardStats('shard-0', { documentCount: 1000 });

    const output = metrics.toJSON();

    expect(output[METRICS.SHARD_QUERY_COUNT.name]).toBeDefined();
    expect(output[METRICS.SHARD_DOCUMENT_COUNT.name]).toBeDefined();
  });

  it('should export per-collection metrics as JSON', () => {
    metrics.recordCollectionOperation('users', 'find', 50);
    metrics.updateCollectionStats('users', { documentCount: 5000 });

    const output = metrics.toJSON();

    expect(output[METRICS.COLLECTION_OPERATIONS.name]).toBeDefined();
    expect(output[METRICS.COLLECTION_DOCUMENT_COUNT.name]).toBeDefined();
  });
});
