# Performance Tuning Guide

This guide covers configuration options for optimizing MongoLake performance across different workloads.

## Shard Configuration

MongoLake uses consistent hashing to distribute collections and documents across shards.

### Shard Count

The default shard count is 16. This must be a power of 2.

```typescript
import { ShardRouter } from 'mongolake/shard/router';

const router = new ShardRouter({
  shardCount: 16,      // Must be power of 2 (8, 16, 32, 64, etc.)
  cacheSize: 10000,    // Maximum cached shard assignments
});
```

| Shard Count | Use Case |
|-------------|----------|
| 8 | Small deployments, single-region |
| 16 | Default, most workloads |
| 32 | High-throughput, multi-region |
| 64+ | Large-scale, high concurrency |

### Shard Routing Options

```typescript
interface ShardRouterOptions {
  /** Number of shards (must be power of 2, default: 16) */
  shardCount?: number;

  /** Maximum cache size for shard assignments (default: 10000) */
  cacheSize?: number;

  /** Custom hash function */
  hashFunction?: (input: string) => number;
}
```

### Shard Affinity Hints

Force specific collections to specific shards for locality:

```typescript
const router = new ShardRouter({ shardCount: 16 });

// Pin high-traffic collection to specific shard
router.setAffinityHint('users', { preferredShard: 0 });

// Route related collections together
router.setAffinityHint('orders', { preferredShard: 1 });
router.setAffinityHint('order_items', { preferredShard: 1 });
```

### Collection Splitting

Split hot collections across multiple shards:

```typescript
// Distribute users collection across 4 shards
router.splitCollection('users', [0, 4, 8, 12]);

// Documents are routed by _id hash to one of the split shards
const assignment = router.routeDocument('users', 'user-123');
// assignment.shardId will be one of [0, 4, 8, 12]
```

## Buffer Sizes

### Flush Thresholds

Control when in-memory buffers are flushed to storage:

| Constant | Default | Description |
|----------|---------|-------------|
| `DEFAULT_FLUSH_THRESHOLD_BYTES` | 1 MB | Flush when buffer reaches this size |
| `DEFAULT_FLUSH_THRESHOLD_DOCS` | 1000 | Flush when buffer has this many documents |

Tune based on workload:

```typescript
// High-throughput writes: larger buffers
const config = {
  flushThresholdBytes: 4 * 1024 * 1024,  // 4 MB
  flushThresholdDocs: 5000,
};

// Low-latency reads: smaller buffers
const config = {
  flushThresholdBytes: 256 * 1024,  // 256 KB
  flushThresholdDocs: 100,
};
```

### Row Group Sizes

Configure Parquet row group sizes:

| Constant | Default | Description |
|----------|---------|-------------|
| `DEFAULT_ROW_GROUP_SIZE` | 10,000 | Rows per row group |
| `DEFAULT_ROW_GROUP_SIZE_BYTES` | 64 MB | Max row group size in bytes |

Larger row groups improve:
- Compression ratios
- Sequential read performance
- Column pruning efficiency

Smaller row groups improve:
- Memory usage during writes
- Predicate pushdown granularity
- Partial file reads

## Compaction Settings

Compaction merges small Parquet files into larger ones for better read performance.

### Compaction Thresholds

| Constant | Default | Description |
|----------|---------|-------------|
| `DEFAULT_COMPACTION_MIN_BLOCK_SIZE` | 2 MB | Blocks smaller than this are candidates |
| `DEFAULT_COMPACTION_TARGET_BLOCK_SIZE` | 4 MB | Target size for merged blocks |
| `DEFAULT_COMPACTION_MAX_BLOCKS_PER_RUN` | 10 | Max blocks processed per compaction run |
| `DEFAULT_COMPACTION_ALARM_DELAY_MS` | 100 ms | Delay between compaction continuations |

### Compaction Configuration

```typescript
import { CompactionScheduler } from 'mongolake/compaction/scheduler';

const scheduler = new CompactionScheduler({
  storage: storageBackend,

  // Tune for write-heavy workloads (fewer, larger compactions)
  minBlockSize: 4_000_000,       // 4 MB
  targetBlockSize: 8_000_000,    // 8 MB
  maxBlocksPerRun: 20,

  // Tune for read-heavy workloads (more aggressive compaction)
  minBlockSize: 1_000_000,       // 1 MB
  targetBlockSize: 4_000_000,    // 4 MB
  maxBlocksPerRun: 10,
});
```

### Compaction Alarm Scheduling

Compaction runs incrementally via Durable Object alarms:

| Constant | Default | Description |
|----------|---------|-------------|
| `COMPACTION_ALARM_DELAY_MS` | 60,000 ms | Initial delay after flush |
| `COMPACTION_RESCHEDULE_DELAY_MS` | 1,000 ms | Delay for continuation |

## Cache Configuration

### Router Cache

```typescript
const router = new ShardRouter({
  cacheSize: 10000,  // DEFAULT_ROUTER_CACHE_SIZE
});

// Monitor cache performance
const stats = router.getStats();
console.log(`Cache hit rate: ${stats.cacheHits / stats.totalRoutes}`);
```

### RPC Caches

| Constant | Default | Description |
|----------|---------|-------------|
| `DEFAULT_RPC_CACHE_SIZE` | 1,000 | Read cache entries |
| `DEFAULT_RPC_CACHE_TTL_MS` | 5 minutes | Read cache TTL |
| `DEFAULT_RPC_WRITE_CACHE_SIZE` | 500 | Write cache entries |
| `DEFAULT_RPC_WRITE_CACHE_TTL_MS` | 1 minute | Write cache TTL |

### Index Cache

```typescript
// DEFAULT_INDEX_CACHE_SIZE: 100 index entries cached
// DEFAULT_ZONE_MAP_CACHE_SIZE: 1000 zone map entries cached
```

## Connection Pooling

### Max Connections Per Shard

```typescript
// DEFAULT_MAX_CONNECTIONS_PER_SHARD: 10
```

For high-concurrency workloads, increase:

```typescript
const config = {
  maxConnectionsPerShard: 50,
};
```

## Timeout Configuration

### Operation Timeouts

| Constant | Default | Description |
|----------|---------|-------------|
| `DEFAULT_OPERATION_TIMEOUT_MS` | 30,000 ms | Max operation duration |
| `CIRCUIT_BREAKER_RESET_TIMEOUT_MS` | 30,000 ms | Circuit breaker reset |

### Retry Configuration

| Constant | Default | Description |
|----------|---------|-------------|
| `DEFAULT_RETRY_BASE_DELAY_MS` | 100 ms | Initial retry delay |
| `DEFAULT_RETRY_MAX_DELAY_MS` | 5,000 ms | Maximum retry delay |

## Workload-Specific Tuning

### Write-Heavy Workloads

```typescript
const config = {
  // Larger buffers for batching
  flushThresholdBytes: 4 * 1024 * 1024,
  flushThresholdDocs: 10000,

  // Less aggressive compaction
  compactionMinBlockSize: 4_000_000,
  compactionMaxBlocksPerRun: 5,

  // Larger row groups
  rowGroupSize: 50000,
};
```

### Read-Heavy Workloads

```typescript
const config = {
  // Smaller buffers for freshness
  flushThresholdBytes: 512 * 1024,
  flushThresholdDocs: 500,

  // Aggressive compaction
  compactionMinBlockSize: 1_000_000,
  compactionMaxBlocksPerRun: 20,

  // Larger caches
  routerCacheSize: 50000,
  rpcCacheSize: 5000,
};
```

### Mixed Workloads

```typescript
const config = {
  // Balanced settings
  flushThresholdBytes: 1024 * 1024,
  flushThresholdDocs: 1000,

  // Default compaction
  compactionMinBlockSize: 2_000_000,
  compactionMaxBlocksPerRun: 10,
};
```

## Monitoring Performance

### Metrics Endpoint

```bash
# Prometheus metrics
curl http://localhost:3456/metrics

# JSON metrics
curl http://localhost:3456/metrics/json
```

### Key Metrics

- `http_requests_total` - Total HTTP requests by method/status
- `http_request_duration_seconds` - Request latency histogram
- `shard_operations_total` - Operations by shard
- `compaction_runs_total` - Compaction executions
- `buffer_size_bytes` - Current buffer sizes

### Router Statistics

```typescript
const router = new ShardRouter();

// After some operations
const stats = router.getStats();
console.log({
  cacheHits: stats.cacheHits,
  cacheMisses: stats.cacheMisses,
  totalRoutes: stats.totalRoutes,
  hitRate: stats.cacheHits / stats.totalRoutes,
});
```

## Best Practices

1. **Start with defaults** - The default values are tuned for common workloads
2. **Monitor before tuning** - Use metrics to identify bottlenecks
3. **Change one setting at a time** - Isolate the impact of changes
4. **Test under load** - Validate changes with realistic traffic
5. **Consider tradeoffs** - Most settings trade memory for latency or throughput

## Constants Reference

All tunable constants are defined in `src/constants.ts`:

```typescript
// Sharding
DEFAULT_SHARD_COUNT = 16

// Buffers
DEFAULT_FLUSH_THRESHOLD_BYTES = 1_048_576  // 1 MB
DEFAULT_FLUSH_THRESHOLD_DOCS = 1000
DEFAULT_ROW_GROUP_SIZE = 10000
DEFAULT_ROW_GROUP_SIZE_BYTES = 67_108_864  // 64 MB

// Compaction
DEFAULT_COMPACTION_MIN_BLOCK_SIZE = 2_000_000
DEFAULT_COMPACTION_TARGET_BLOCK_SIZE = 4_000_000
DEFAULT_COMPACTION_MAX_BLOCKS_PER_RUN = 10
DEFAULT_COMPACTION_ALARM_DELAY_MS = 100
COMPACTION_ALARM_DELAY_MS = 60000
COMPACTION_RESCHEDULE_DELAY_MS = 1000

// Caches
DEFAULT_ROUTER_CACHE_SIZE = 10000
DEFAULT_RPC_CACHE_SIZE = 1000
DEFAULT_RPC_CACHE_TTL_MS = 300000  // 5 minutes
DEFAULT_INDEX_CACHE_SIZE = 100
DEFAULT_ZONE_MAP_CACHE_SIZE = 1000

// Timeouts
DEFAULT_OPERATION_TIMEOUT_MS = 30000
DEFAULT_RETRY_BASE_DELAY_MS = 100
DEFAULT_RETRY_MAX_DELAY_MS = 5000

// Connections
DEFAULT_MAX_CONNECTIONS_PER_SHARD = 10
DEFAULT_RPC_BATCH_SIZE = 100
```
