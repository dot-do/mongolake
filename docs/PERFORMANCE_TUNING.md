# Performance Tuning Guide

This comprehensive guide covers performance optimization for MongoLake deployments. It includes benchmarking methodology, key metrics to monitor, and tuning recommendations for all major system components.

## Table of Contents

1. [Benchmarking Methodology](#benchmarking-methodology)
2. [Key Performance Metrics](#key-performance-metrics)
3. [Worker Configuration Tuning](#worker-configuration-tuning)
4. [Durable Object Optimization](#durable-object-optimization)
5. [R2 Storage Optimization](#r2-storage-optimization)
6. [Parquet File Settings](#parquet-file-settings)
7. [Connection Pool Tuning](#connection-pool-tuning)
8. [Memory Management](#memory-management)
9. [Query Optimization Tips](#query-optimization-tips)
10. [Compaction Strategy Tuning](#compaction-strategy-tuning)
11. [Shard Distribution for Load Balancing](#shard-distribution-for-load-balancing)

---

## Benchmarking Methodology

### Establishing Baselines

Before optimizing, establish baseline metrics under realistic conditions:

1. **Define representative workloads**
   - Read/write ratio (e.g., 80% reads, 20% writes)
   - Document sizes (small: <1KB, medium: 1-10KB, large: >10KB)
   - Query complexity (simple lookups vs. aggregations)
   - Concurrency levels (simultaneous requests)

2. **Capture baseline metrics**
   ```bash
   # Prometheus metrics endpoint
   curl http://localhost:3456/metrics

   # JSON format for programmatic analysis
   curl http://localhost:3456/metrics/json
   ```

3. **Run benchmark tests**
   ```typescript
   import { performance } from 'perf_hooks';

   async function benchmark(name: string, iterations: number, fn: () => Promise<void>) {
     const times: number[] = [];

     // Warmup
     for (let i = 0; i < 10; i++) {
       await fn();
     }

     // Measure
     for (let i = 0; i < iterations; i++) {
       const start = performance.now();
       await fn();
       times.push(performance.now() - start);
     }

     const avg = times.reduce((a, b) => a + b, 0) / times.length;
     const p50 = percentile(times, 50);
     const p95 = percentile(times, 95);
     const p99 = percentile(times, 99);

     console.log(`${name}: avg=${avg.toFixed(2)}ms p50=${p50.toFixed(2)}ms p95=${p95.toFixed(2)}ms p99=${p99.toFixed(2)}ms`);
   }
   ```

### Benchmarking Best Practices

1. **Isolate variables** - Change one setting at a time
2. **Use production-like data** - Realistic document sizes and field distributions
3. **Test at scale** - Ensure tests cover expected peak loads
4. **Measure cold and warm performance** - First request vs. cached requests
5. **Monitor resource utilization** - CPU, memory, network alongside latency

---

## Key Performance Metrics

### Metrics to Monitor

MongoLake exposes comprehensive Prometheus metrics. Key metrics to track:

#### Query Performance
| Metric | Description | Target |
|--------|-------------|--------|
| `mongolake_query_duration_seconds` | Query latency histogram | p95 < 100ms |
| `mongolake_slow_queries_total` | Queries exceeding 100ms | Minimize |
| `mongolake_query_total` | Total queries by status | Monitor error rate |

#### Storage Operations
| Metric | Description | Target |
|--------|-------------|--------|
| `mongolake_r2_operation_duration_seconds` | R2 operation latency | p95 < 500ms |
| `mongolake_r2_bytes_read_total` | Bytes read from R2 | Monitor growth |
| `mongolake_r2_errors_total` | R2 operation errors | Zero |

#### Buffer and WAL
| Metric | Description | Target |
|--------|-------------|--------|
| `mongolake_buffer_size_bytes` | Current buffer size | Below threshold |
| `mongolake_buffer_documents` | Documents in buffer | Below threshold |
| `mongolake_wal_size_bytes` | WAL size per shard | Below 10MB |
| `mongolake_flush_duration_seconds` | Flush operation time | p95 < 5s |

#### Compaction
| Metric | Description | Target |
|--------|-------------|--------|
| `mongolake_compaction_duration_seconds` | Compaction cycle time | < 60s |
| `mongolake_compaction_bytes_saved_total` | Space reclaimed | Positive growth |
| `mongolake_compaction_cycles_total` | Compaction runs | Monitor success rate |

#### Shard Health
| Metric | Description | Target |
|--------|-------------|--------|
| `mongolake_shard_document_count` | Documents per shard | Even distribution |
| `mongolake_shard_size_bytes` | Shard size | < 10GB |
| `mongolake_shard_write_rate` | Writes per second | < 10,000/s |
| `mongolake_shard_hot_count` | Shards near limits | Zero |

### Setting Up Monitoring

```typescript
// Example: Prometheus + Grafana setup
// wrangler.toml
[observability]
enabled = true

// Dashboard queries
// Average query latency
rate(mongolake_query_duration_seconds_sum[5m]) / rate(mongolake_query_duration_seconds_count[5m])

// Error rate
sum(rate(mongolake_query_total{status="error"}[5m])) / sum(rate(mongolake_query_total[5m]))

// Cache hit rate
sum(rate(mongolake_cache_hits_total[5m])) / (sum(rate(mongolake_cache_hits_total[5m])) + sum(rate(mongolake_cache_misses_total[5m])))
```

---

## Worker Configuration Tuning

### CPU and Memory Limits

Cloudflare Workers have specific resource constraints:

| Resource | Free Tier | Paid Tier |
|----------|-----------|-----------|
| CPU time | 10ms | 30s |
| Memory | 128MB | 128MB |
| Request duration | 30s | 30s |

### Optimizing Worker Performance

1. **Minimize cold starts**
   ```typescript
   // Pre-initialize expensive objects outside request handler
   let router: ShardRouter | null = null;

   export default {
     async fetch(request: Request, env: Env) {
       // Lazy initialization
       if (!router) {
         router = new ShardRouter({ shardCount: env.SHARD_COUNT });
       }
       // Use router...
     }
   };
   ```

2. **Use streaming for large responses**
   ```typescript
   // Instead of building full response in memory
   return new Response(readableStream, {
     headers: { 'Content-Type': 'application/json' }
   });
   ```

3. **Batch operations**
   ```typescript
   // Batch multiple document operations
   const results = await Promise.all(
     documents.map(doc => shard.write(doc))
   );
   ```

### Recommended Worker Settings

```toml
# wrangler.toml
[vars]
# Shard configuration
SHARD_COUNT = "16"

# Timeout settings (in milliseconds)
OPERATION_TIMEOUT = "30000"
RETRY_BASE_DELAY = "100"
RETRY_MAX_DELAY = "5000"

# Cache settings
ROUTER_CACHE_SIZE = "10000"
RPC_CACHE_SIZE = "1000"
```

---

## Durable Object Optimization

### Understanding DO Constraints

Durable Objects provide:
- Single-writer semantics (serialized access)
- 128MB memory limit
- SQLite for persistent storage
- Alarms for background tasks

### Optimizing DO Performance

1. **Minimize lock contention**
   ```typescript
   // Use fine-grained operations instead of large batches
   // Bad: Single large write blocking all reads
   await shard.writeMany(largeDocumentArray);

   // Better: Smaller batches with yielding
   for (const batch of chunks(documents, 100)) {
     await shard.writeMany(batch);
     await scheduler.wait(0); // Yield to other requests
   }
   ```

2. **Optimize buffer thresholds**
   ```typescript
   // High-throughput writes: larger buffers, less frequent flushes
   const config = {
     flushThresholdBytes: 4 * 1024 * 1024,  // 4MB
     flushThresholdDocs: 5000,
   };

   // Low-latency reads: smaller buffers, more frequent flushes
   const config = {
     flushThresholdBytes: 256 * 1024,  // 256KB
     flushThresholdDocs: 100,
   };
   ```

3. **WAL management**
   ```typescript
   // Monitor WAL size to prevent forced flushes
   // MAX_WAL_SIZE_BYTES: 10MB
   // MAX_WAL_ENTRIES: 10,000

   // If frequently hitting limits, reduce flush thresholds
   const config = {
     flushThresholdBytes: 512 * 1024,  // 512KB
     flushThresholdDocs: 500,
   };
   ```

4. **Schedule compaction during low traffic**
   ```typescript
   // Compaction runs via DO alarms
   // COMPACTION_ALARM_DELAY_MS: 60000 (1 minute after flush)
   // COMPACTION_RESCHEDULE_DELAY_MS: 1000 (continuation delay)

   // For off-peak compaction, trigger manually:
   await shard.scheduleCompaction({ delayMs: nighttimeDelayMs });
   ```

### DO Memory Best Practices

| Component | Default | Max Recommended |
|-----------|---------|-----------------|
| Buffer size | 1MB | 16MB |
| WAL entries | 10,000 | 10,000 |
| Index cache | 100 entries | 500 entries |
| Zone map cache | 1,000 entries | 5,000 entries |

---

## R2 Storage Optimization

### Batch Operations

Minimize R2 API calls through batching:

```typescript
// Bad: Individual puts
for (const doc of documents) {
  await storage.put(`docs/${doc._id}`, encode(doc));
}

// Good: Batch into Parquet files
const parquetData = await createParquetFile(documents);
await storage.put(`blocks/${blockId}.parquet`, parquetData);
```

### Caching Strategies

1. **Use multipart uploads for large files**
   ```typescript
   // DEFAULT_MULTIPART_CONCURRENCY: 5
   // Adjust for your bandwidth/latency tradeoff
   const storage = new R2Storage(bucket, {
     multipartConcurrency: 10,  // More parallel uploads
   });
   ```

2. **Implement read-through caching**
   ```typescript
   // Zone map cache reduces R2 reads
   // DEFAULT_ZONE_MAP_CACHE_SIZE: 1000

   // Increase for read-heavy workloads
   const indexManager = new IndexManager({
     zoneMapCacheSize: 5000,
   });
   ```

3. **Use streaming for large objects**
   ```typescript
   // Instead of loading entire file into memory
   const data = await storage.get(key);  // Loads all into memory

   // Use streaming
   const stream = await storage.getStream(key);
   for await (const chunk of stream) {
     processChunk(chunk);
   }
   ```

### R2 Cost Optimization

| Operation | Cost | Optimization |
|-----------|------|--------------|
| Class A (PUT, POST) | $4.50/million | Batch writes into Parquet |
| Class B (GET, HEAD) | $0.36/million | Cache frequently accessed data |
| Storage | $0.015/GB/month | Run compaction regularly |
| Egress | Free to Workers | Keep processing in Workers |

### R2 Performance Tips

1. **Use appropriate object sizes**
   - Small objects (<1KB): High overhead, consider batching
   - Medium objects (1KB-100MB): Optimal performance
   - Large objects (>100MB): Use multipart upload

2. **Optimize list operations**
   ```typescript
   // List with prefix is efficient
   const files = await storage.list('myapp/users/');

   // Pagination for large results
   let cursor: string | undefined;
   do {
     const response = await bucket.list({ prefix, cursor, limit: 1000 });
     // Process response.objects
     cursor = response.truncated ? response.cursor : undefined;
   } while (cursor);
   ```

---

## Parquet File Settings

### Row Group Configuration

Row groups are the unit of parallelism in Parquet:

```typescript
// Default: 64MB row groups
// DEFAULT_ROW_GROUP_SIZE_BYTES = 64 * 1024 * 1024

const writer = new StreamingParquetWriter(storage, key, {
  rowGroupSizeBytes: 64 * 1024 * 1024,  // 64MB
  maxRowsPerRowGroup: 100000,            // 100K rows max
});
```

| Setting | Small (16MB) | Default (64MB) | Large (128MB) |
|---------|--------------|----------------|---------------|
| Memory usage | Low | Medium | High |
| Compression ratio | Lower | Good | Better |
| Predicate pushdown | Finer-grained | Balanced | Coarser |
| Read latency | Lower | Medium | Higher |

### Compression Settings

MongoLake supports multiple compression codecs:

```typescript
// Snappy: Fast compression/decompression
// SNAPPY_WINDOW_SIZE: 1024
// SNAPPY_MIN_MATCH_LENGTH: 4

// ZSTD: Better compression ratio
// ZSTD_WINDOW_SIZE: 4096
// ZSTD_MIN_MATCH_LENGTH: 3
// ZSTD_SLIDING_WINDOW_SIZE: 65536 (64KB)
```

| Codec | Compression Ratio | Speed | Use Case |
|-------|-------------------|-------|----------|
| None | 1.0x | Fastest | Testing, already compressed data |
| Snappy | 2-4x | Fast | General purpose, low CPU |
| ZSTD | 3-6x | Medium | Cold storage, archival |

### Field Promotion

Promote frequently queried fields to native Parquet columns:

```typescript
const writer = new StreamingParquetWriter(storage, key, {
  fieldPromotions: {
    'status': 'string',
    'createdAt': 'timestamp',
    'userId': 'string',
    'amount': 'double',
  },
  variantOnly: false,  // Enable promotions
});
```

Benefits of promotion:
- Direct column access without variant decoding
- Better compression for homogeneous data
- Statistics for predicate pushdown
- Compatible with external query engines

### Parquet Tuning Recommendations

| Workload | Row Group Size | Compression | Field Promotions |
|----------|----------------|-------------|------------------|
| OLTP (transactional) | 16-32MB | Snappy | Primary keys + filters |
| OLAP (analytical) | 64-128MB | ZSTD | All frequently queried |
| Mixed | 64MB | Snappy | Common query patterns |
| Archival | 128MB+ | ZSTD | Minimal |

---

## Connection Pool Tuning

### Pool Configuration

```typescript
const pool = new ConnectionPool({
  maxConnections: 100,      // Maximum concurrent connections
  minConnections: 10,       // Keep-alive connections
  idleTimeout: 30000,       // Close idle after 30s
  acquireTimeout: 5000,     // Wait max 5s for connection
  healthCheckInterval: 10000, // Check health every 10s
  idleCheckInterval: 1000,   // Check idle every 1s
});
```

### Tuning for Workload Types

**High-concurrency, short requests:**
```typescript
const pool = new ConnectionPool({
  maxConnections: 200,
  minConnections: 50,
  idleTimeout: 60000,
  acquireTimeout: 2000,
});
```

**Low-concurrency, long requests:**
```typescript
const pool = new ConnectionPool({
  maxConnections: 50,
  minConnections: 10,
  idleTimeout: 120000,
  acquireTimeout: 10000,
});
```

### Pool Metrics

Monitor pool health via metrics:

```typescript
const metrics = pool.getMetrics();
console.log({
  activeConnections: metrics.activeConnections,
  idleConnections: metrics.idleConnections,
  acquireTimeouts: metrics.acquireTimeouts,  // Should be zero
  idleTimeoutCount: metrics.idleTimeoutCount,
  errorCount: metrics.errorCount,
});
```

### Per-Shard Connection Limits

```typescript
// DEFAULT_MAX_CONNECTIONS_PER_SHARD: 10

// Increase for high-throughput shards
const config = {
  maxConnectionsPerShard: 50,
};
```

---

## Memory Management

### Memory Budget (128MB Worker Limit)

Allocate memory carefully across components:

| Component | Recommended Allocation |
|-----------|----------------------|
| Request parsing | 10MB |
| Buffer (per shard) | 1-4MB |
| WAL cache | 10MB |
| Index/zone map cache | 20MB |
| Query execution | 30MB |
| Response serialization | 20MB |
| Headroom | 34MB |

### Preventing OOM

1. **Stream large result sets**
   ```typescript
   // Bad: Load all into memory
   const docs = await collection.find(filter).toArray();

   // Good: Stream results
   for await (const doc of collection.find(filter)) {
     yield doc;
   }
   ```

2. **Limit batch sizes**
   ```typescript
   // MAX_BATCH_SIZE: 100,000
   // MAX_BATCH_BYTES: 16MB

   // Use smaller batches for safety
   await collection.insertMany(docs.slice(0, 1000));
   ```

3. **Use projections**
   ```typescript
   // Only fetch needed fields
   const users = await collection.find(
     { status: 'active' },
     { projection: { name: 1, email: 1 } }
   ).toArray();
   ```

4. **Configure cache sizes**
   ```typescript
   // DEFAULT_ROUTER_CACHE_SIZE: 10,000
   // DEFAULT_RPC_CACHE_SIZE: 1,000
   // DEFAULT_INDEX_CACHE_SIZE: 100
   // DEFAULT_ZONE_MAP_CACHE_SIZE: 1,000

   // Reduce for memory-constrained environments
   const router = new ShardRouter({
     cacheSize: 5000,
   });
   ```

### Memory Monitoring

Track memory usage in Durable Objects:

```typescript
// Buffer manager tracks size
const bufferSize = bufferManager.getBufferSize();
const docCount = bufferManager.getBufferDocCount();

// Trigger flush before hitting limits
if (bufferSize > 0.8 * flushThreshold) {
  await flush();
}
```

---

## Query Optimization Tips

### Index Usage

Create indexes for frequent query patterns:

```typescript
// Equality queries
await collection.createIndex({ status: 1 });

// Range queries
await collection.createIndex({ createdAt: -1 });

// Compound queries
await collection.createIndex({ tenantId: 1, status: 1, createdAt: -1 });
```

### Query Patterns

**Efficient patterns:**
```typescript
// Use indexed equality
{ status: 'active' }

// Range on indexed field
{ createdAt: { $gte: lastWeek } }

// Compound index prefix match
{ tenantId: 'abc', status: 'active' }

// Limit results
collection.find(filter).limit(100)
```

**Patterns to avoid:**
```typescript
// Regex without anchor (full scan)
{ name: { $regex: 'smith' } }

// $or without indexes
{ $or: [{ email: 'a@b.com' }, { phone: '555' }] }

// Deep pagination
collection.find().skip(10000).limit(10)

// Large $in arrays
{ status: { $in: [...hundredsOfValues] } }
```

### Zone Map Optimization

Zone maps enable predicate pushdown - skipping files:

```typescript
// Sort data before insertion for better zone maps
const sortedDocs = docs.sort((a, b) => a.createdAt - b.createdAt);
await collection.insertMany(sortedDocs);

// Queries on sort key skip more files
await collection.find({ createdAt: { $gte: lastHour } });
```

### Aggregation Pipeline

Optimize pipeline stage order:

```typescript
// Good: Filter early, project early
const pipeline = [
  { $match: { status: 'active', createdAt: { $gte: lastWeek } } },
  { $project: { name: 1, amount: 1 } },
  { $group: { _id: '$name', total: { $sum: '$amount' } } },
  { $sort: { total: -1 } },
  { $limit: 10 }
];

// Bad: Process all data first
const pipeline = [
  { $group: { _id: '$name', total: { $sum: '$amount' } } },
  { $match: { ... } }  // Too late!
];
```

---

## Compaction Strategy Tuning

### Compaction Thresholds

```typescript
// Default settings
const DEFAULT_COMPACTION_MIN_BLOCK_SIZE = 2_000_000;    // 2MB
const DEFAULT_COMPACTION_TARGET_BLOCK_SIZE = 4_000_000; // 4MB
const DEFAULT_COMPACTION_MAX_BLOCKS_PER_RUN = 10;
```

### Tuning for Workload Types

**Write-heavy workloads:**
```typescript
// Less aggressive compaction to reduce write amplification
const scheduler = new CompactionScheduler({
  minBlockSize: 4_000_000,       // 4MB (larger threshold)
  targetBlockSize: 8_000_000,    // 8MB target
  maxBlocksPerRun: 5,            // Fewer blocks per run
});
```

**Read-heavy workloads:**
```typescript
// Aggressive compaction for better read performance
const scheduler = new CompactionScheduler({
  minBlockSize: 1_000_000,       // 1MB (smaller threshold)
  targetBlockSize: 4_000_000,    // 4MB target
  maxBlocksPerRun: 20,           // More blocks per run
});
```

**Mixed workloads:**
```typescript
// Balanced settings
const scheduler = new CompactionScheduler({
  minBlockSize: 2_000_000,       // 2MB
  targetBlockSize: 4_000_000,    // 4MB
  maxBlocksPerRun: 10,           // Default
});
```

### Compaction Scheduling

```typescript
// COMPACTION_ALARM_DELAY_MS: 60000 (1 minute after flush)
// COMPACTION_RESCHEDULE_DELAY_MS: 1000 (continuation)

// For time-sensitive compaction, trigger during maintenance windows
async function scheduleOffPeakCompaction(shard: ShardDO) {
  const now = new Date();
  const hour = now.getUTCHours();

  // Run aggressive compaction during off-peak hours (2-6 AM UTC)
  if (hour >= 2 && hour < 6) {
    await shard.runCompaction({ maxBlocksPerRun: 50 });
  }
}
```

### Monitoring Compaction

```typescript
// Track compaction effectiveness
const metrics = {
  filesProcessed: 'mongolake_compaction_files_processed_total',
  bytesSaved: 'mongolake_compaction_bytes_saved_total',
  duration: 'mongolake_compaction_duration_seconds',
  cycles: 'mongolake_compaction_cycles_total',
};

// Alert if compaction is falling behind
if (smallBlockCount > 100) {
  await runEmergencyCompaction();
}
```

### Compaction Best Practices

1. **Monitor block size distribution** - Ensure most blocks are near target size
2. **Track compaction lag** - Time between flush and compaction completion
3. **Balance with writes** - Don't starve compaction during write spikes
4. **Use delayed deletions** - Prevent data loss during concurrent reads
5. **Test compaction recovery** - Ensure aborted compactions don't corrupt data

---

## Shard Distribution for Load Balancing

### Shard Count Selection

```typescript
// DEFAULT_SHARD_COUNT: 16 (must be power of 2)

// Small deployment (< 1M documents)
const shardCount = 8;

// Medium deployment (1-10M documents)
const shardCount = 16;

// Large deployment (10-100M documents)
const shardCount = 32;

// Very large deployment (> 100M documents)
const shardCount = 64;
```

### Routing Strategies

1. **Collection-level routing (default)**
   ```typescript
   const router = new ShardRouter({ shardCount: 16 });
   const assignment = router.route('users');
   // All documents in 'users' go to same shard
   ```

2. **Document-level routing (for hot collections)**
   ```typescript
   // Split collection across multiple shards
   router.splitCollection('users', [0, 4, 8, 12]);

   const assignment = router.routeDocument('users', 'user-123');
   // Documents distributed across split shards
   ```

3. **Affinity hints (for related data)**
   ```typescript
   // Co-locate related collections
   router.setAffinityHint('orders', { preferredShard: 1 });
   router.setAffinityHint('order_items', { preferredShard: 1 });
   ```

### Dynamic Shard Splitting

MongoLake supports automatic shard splitting when thresholds are exceeded:

```typescript
// Splitting thresholds
const DEFAULT_SHARD_SPLIT_MAX_DOCUMENTS = 1_000_000;  // 1M docs
const DEFAULT_SHARD_SPLIT_MAX_SIZE_BYTES = 10 * 1024 * 1024 * 1024;  // 10GB
const DEFAULT_SHARD_SPLIT_MAX_WRITE_RATE = 10_000;  // 10K writes/sec

// Split timing
const DEFAULT_SHARD_SPLIT_CHECK_INTERVAL_MS = 60_000;  // Check every minute
const DEFAULT_SHARD_SPLIT_SUSTAINED_THRESHOLD_MS = 5 * 60 * 1000;  // 5 min sustained
const DEFAULT_SHARD_SPLIT_MIN_INTERVAL_MS = 60 * 60 * 1000;  // 1 hour between splits
```

### Load Balancing Best Practices

1. **Monitor shard distribution**
   ```typescript
   // Check for hot shards
   const shardStats = await Promise.all(
     shards.map(s => s.getStats())
   );

   const maxDocs = Math.max(...shardStats.map(s => s.documentCount));
   const minDocs = Math.min(...shardStats.map(s => s.documentCount));
   const skew = maxDocs / minDocs;

   if (skew > 2.0) {
     console.warn('Significant shard skew detected');
   }
   ```

2. **Use appropriate shard keys**
   - High-cardinality fields (userId, orderId)
   - Avoid monotonically increasing keys (timestamps alone)
   - Consider composite keys for better distribution

3. **Pre-split for known hot collections**
   ```typescript
   // Before bulk load
   router.splitCollection('events', [0, 2, 4, 6, 8, 10, 12, 14]);
   ```

4. **Monitor and react to hotspots**
   - Track per-shard write rates
   - Alert when approaching split thresholds
   - Consider manual rebalancing for persistent skew

---

## Quick Reference: Default Constants

| Constant | Default Value | Description |
|----------|---------------|-------------|
| `DEFAULT_SHARD_COUNT` | 16 | Number of shards |
| `DEFAULT_FLUSH_THRESHOLD_BYTES` | 1MB | Buffer size trigger |
| `DEFAULT_FLUSH_THRESHOLD_DOCS` | 1,000 | Document count trigger |
| `DEFAULT_ROW_GROUP_SIZE_BYTES` | 64MB | Parquet row group size |
| `DEFAULT_COMPACTION_MIN_BLOCK_SIZE` | 2MB | Min size for compaction |
| `DEFAULT_COMPACTION_TARGET_BLOCK_SIZE` | 4MB | Target merged size |
| `DEFAULT_COMPACTION_MAX_BLOCKS_PER_RUN` | 10 | Blocks per compaction |
| `MAX_WAL_SIZE_BYTES` | 10MB | WAL size limit |
| `MAX_WAL_ENTRIES` | 10,000 | WAL entry limit |
| `DEFAULT_ROUTER_CACHE_SIZE` | 10,000 | Router cache entries |
| `DEFAULT_RPC_CACHE_SIZE` | 1,000 | RPC cache entries |
| `DEFAULT_MAX_CONNECTIONS_PER_SHARD` | 10 | Connections per shard |
| `DEFAULT_OPERATION_TIMEOUT_MS` | 30,000 | Operation timeout |
| `MAX_BATCH_SIZE` | 100,000 | Max batch documents |
| `MAX_BATCH_BYTES` | 16MB | Max batch size |
| `MAX_WIRE_MESSAGE_SIZE` | 48MB | Wire protocol limit |

---

## Summary

Performance tuning in MongoLake requires balancing multiple tradeoffs:

| Optimization | Benefit | Cost |
|-------------|---------|------|
| Larger buffers | Better write throughput | Higher memory, stale reads |
| More shards | Better parallelism | Coordination overhead |
| Aggressive compaction | Better read performance | Write amplification |
| Larger row groups | Better compression | Higher memory, coarser filtering |
| More caching | Lower latency | Higher memory usage |

**General approach:**
1. Start with defaults
2. Establish baseline metrics
3. Identify bottlenecks through monitoring
4. Change one setting at a time
5. Measure impact
6. Iterate

For query-specific optimization, see the [Query Optimization Guide](./QUERY_OPTIMIZATION.md).
For detailed configuration options, see the [Operations Guide](./operations/performance-tuning.md).
