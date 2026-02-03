# Deployment Guide

This guide covers deploying MongoLake to production on Cloudflare Workers, including R2 storage configuration, Durable Objects setup, regional sharding, and operational best practices.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [wrangler.toml Configuration](#wranglertoml-configuration)
- [R2 Bucket Setup](#r2-bucket-setup)
- [Environment Variables Reference](#environment-variables-reference)
- [Regional Sharding Configuration](#regional-sharding-configuration)
- [Production Checklist](#production-checklist)
- [Monitoring and Alerting](#monitoring-and-alerting)
- [Scaling Considerations](#scaling-considerations)
- [Troubleshooting](#troubleshooting)

---

## Prerequisites

Before deploying MongoLake, ensure you have:

- [Cloudflare account](https://dash.cloudflare.com/sign-up) with Workers and R2 access
- [Wrangler CLI](https://developers.cloudflare.com/workers/wrangler/install-and-update/) v3.22.0 or later
- Node.js 18+ installed
- pnpm 9.0.0+ (recommended) or npm

```bash
# Install wrangler globally
npm install -g wrangler

# Authenticate with Cloudflare
wrangler login
```

---

## Quick Start

1. **Clone the repository**:
   ```bash
   git clone https://github.com/dot-do/mongolake.git
   cd mongolake
   pnpm install
   ```

2. **Create R2 bucket**:
   ```bash
   wrangler r2 bucket create mongolake-data
   ```

3. **Configure wrangler.toml** (see [Configuration](#wranglertoml-configuration) section)

4. **Deploy**:
   ```bash
   wrangler deploy --env production
   ```

5. **Verify deployment**:
   ```bash
   curl https://your-worker.workers.dev/health
   ```

---

## wrangler.toml Configuration

Create a `wrangler.toml` file in your project root with the following configuration:

### Basic Configuration

```toml
name = "mongolake"
main = "src/index.ts"
compatibility_date = "2024-09-02"
compatibility_flags = ["nodejs_compat_v2"]

# Durable Objects for WAL and buffer management
[durable_objects]
bindings = [
  { name = "RPC_NAMESPACE", class_name = "ShardDO" }
]

# SQLite migrations for Durable Objects
[[migrations]]
tag = "v1"
new_sqlite_classes = ["ShardDO"]

# R2 Storage for Parquet files
[[r2_buckets]]
binding = "BUCKET"
bucket_name = "mongolake-data"

# Environment variables
[vars]
ENVIRONMENT = "development"
```

### Environment-Specific Configuration

```toml
# Development environment
[env.development]
vars = { ENVIRONMENT = "development" }

# Test environment (used by vitest-pool-workers)
[env.test]
vars = { ENVIRONMENT = "test" }

# Production environment
[env.production]
vars = { ENVIRONMENT = "production" }
# Consider using a separate bucket for production
# [[env.production.r2_buckets]]
# binding = "BUCKET"
# bucket_name = "mongolake-prod-data"
```

### Service Bindings for Authentication (Optional)

For low-latency authentication using in-datacenter communication:

```toml
# Uncomment to enable in all environments:
# [[services]]
# binding = "AUTH"
# service = "auth-service"

# [[services]]
# binding = "OAUTH"
# service = "oauth-service"

# Production-only service bindings:
# [[env.production.services]]
# binding = "AUTH"
# service = "auth-service"

# [[env.production.services]]
# binding = "OAUTH"
# service = "oauth-service"
```

### Workers Analytics (Optional)

Enable detailed observability with Workers Analytics Engine:

```toml
[[analytics_engine_datasets]]
binding = "ANALYTICS"
dataset = "mongolake_metrics"
```

### Worker Entry Point

Create your worker entry point at `src/index.ts`:

```typescript
import { MongoLakeWorker } from 'mongolake/worker';
import { ShardDO } from 'mongolake/do';

export default MongoLakeWorker;
export { ShardDO };
```

---

## R2 Bucket Setup

### Creating R2 Buckets

```bash
# Create the main data bucket
wrangler r2 bucket create mongolake-data

# For multi-region deployments, create regional buckets
wrangler r2 bucket create mongolake-data-us
wrangler r2 bucket create mongolake-data-eu
wrangler r2 bucket create mongolake-data-apac

# Verify bucket creation
wrangler r2 bucket list
```

### Bucket Structure

MongoLake organizes data in R2 with the following structure:

```
mongolake-data/
  {database}/
    {collection}/
      *.parquet           # Data files
      _delta/             # Delta files (pending compaction)
      _manifest/          # Collection manifests
      _iceberg/           # Iceberg metadata (if enabled)
```

### Lifecycle Rules

Configure lifecycle rules for automatic cleanup of old delta files:

```bash
# Create lifecycle rule to delete delta files older than 30 days
wrangler r2 bucket lifecycle set mongolake-data \
  --prefix "_delta/" \
  --expiration-days 30
```

### Event Notifications (Optional)

Set up event notifications for monitoring:

```bash
# Enable event notifications for the bucket
wrangler r2 bucket notification create mongolake-data \
  --queue mongolake-notifications \
  --event-type object:create \
  --event-type object:delete
```

### CORS Configuration

For browser-based access, configure CORS:

```bash
wrangler r2 bucket cors set mongolake-data \
  --origins "https://your-app.com" \
  --methods "GET,HEAD,PUT,POST,DELETE" \
  --headers "Content-Type,Authorization"
```

---

## Environment Variables Reference

### Required Bindings

| Binding | Type | Description |
|---------|------|-------------|
| `BUCKET` | R2 Bucket | R2 bucket for Parquet storage (auto-bound via wrangler.toml) |
| `RPC_NAMESPACE` | Durable Object | Durable Object namespace for ShardDO (auto-bound via wrangler.toml) |

### Application Variables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `ENVIRONMENT` | string | `"development"` | Environment name (`development`, `test`, `production`) |
| `REQUIRE_AUTH` | boolean | `false` | Require authentication for all requests |
| `ALLOWED_ORIGINS` | string | `"*"` | CORS allowed origins (comma-separated) |
| `DEBUG` | boolean | `false` | Enable debug logging |

### Authentication Variables

| Variable | Type | Description |
|----------|------|-------------|
| `AUTH_ISSUER` | string | JWT token issuer URL |
| `AUTH_AUDIENCE` | string | JWT token audience |
| `AUTH_CLIENT_ID` | string | OAuth client ID |
| `AUTH_CLIENT_SECRET` | secret | OAuth client secret (use `wrangler secret`) |
| `JWKS_URI` | string | JWKs endpoint for key validation |

### Iceberg Integration

| Variable | Type | Description |
|----------|------|-------------|
| `R2_DATA_CATALOG_TOKEN` | secret | Iceberg catalog authentication token |
| `ICEBERG_CATALOG_URI` | string | Iceberg REST catalog URI |
| `ICEBERG_WAREHOUSE` | string | Iceberg warehouse location |

### Performance Tuning

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SHARD_COUNT` | number | `16` | Number of shards (must be power of 2) |
| `FLUSH_THRESHOLD_BYTES` | number | `1048576` | Buffer flush threshold in bytes (1MB) |
| `FLUSH_THRESHOLD_DOCS` | number | `1000` | Buffer flush threshold in documents |
| `ROW_GROUP_SIZE` | number | `10000` | Parquet row group size |
| `COMPACTION_MIN_BLOCK_SIZE` | number | `2000000` | Minimum block size for compaction (2MB) |
| `COMPACTION_TARGET_BLOCK_SIZE` | number | `4000000` | Target block size for compaction (4MB) |

### Setting Secrets

Store sensitive values as secrets rather than environment variables:

```bash
# OAuth credentials
wrangler secret put AUTH_CLIENT_SECRET

# Iceberg catalog token
wrangler secret put R2_DATA_CATALOG_TOKEN

# API keys
wrangler secret put API_SECRET_KEY
```

---

## Regional Sharding Configuration

MongoLake supports multi-region deployments for low-latency access worldwide.

### Shard Distribution

Configure shard count and routing:

```typescript
import { ShardRouter } from 'mongolake/shard/router';

const router = new ShardRouter({
  shardCount: 16,      // Must be power of 2 (8, 16, 32, 64)
  cacheSize: 10000,    // Maximum cached shard assignments
});
```

| Shard Count | Use Case |
|-------------|----------|
| 8 | Small deployments, single-region |
| 16 | Default, most workloads |
| 32 | High-throughput, multi-region |
| 64+ | Large-scale, high concurrency |

### Shard Affinity

Force specific collections to specific shards for locality:

```typescript
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
```

### Multi-Region wrangler.toml

```toml
# Primary region (US)
[env.us]
name = "mongolake-us"
route = { pattern = "us.mongolake.example.com/*", zone_name = "example.com" }

[[env.us.r2_buckets]]
binding = "BUCKET"
bucket_name = "mongolake-data-us"

# EU region
[env.eu]
name = "mongolake-eu"
route = { pattern = "eu.mongolake.example.com/*", zone_name = "example.com" }

[[env.eu.r2_buckets]]
binding = "BUCKET"
bucket_name = "mongolake-data-eu"

# APAC region
[env.apac]
name = "mongolake-apac"
route = { pattern = "apac.mongolake.example.com/*", zone_name = "example.com" }

[[env.apac.r2_buckets]]
binding = "BUCKET"
bucket_name = "mongolake-data-apac"
```

### Deploy to Multiple Regions

```bash
# Deploy to all regions
wrangler deploy --env us
wrangler deploy --env eu
wrangler deploy --env apac
```

---

## Production Checklist

### Pre-Deployment

- [ ] **R2 bucket created and bound** - Verify with `wrangler r2 bucket list`
- [ ] **Durable Object migrations applied** - Ensure `[[migrations]]` tag is set
- [ ] **Environment variables configured** - Review all required variables
- [ ] **Secrets set for sensitive values** - Use `wrangler secret put`
- [ ] **CORS origins configured** - Set `ALLOWED_ORIGINS` appropriately
- [ ] **Authentication enabled** - Set `REQUIRE_AUTH=true` if needed

### Security

- [ ] **API keys rotated** - Regenerate any default or test API keys
- [ ] **Secrets not in code** - Verify no hardcoded credentials
- [ ] **TLS enabled** - Cloudflare provides this by default
- [ ] **Rate limiting configured** - Consider Cloudflare Rate Limiting rules
- [ ] **WAF rules enabled** - Enable Cloudflare WAF for protection

### Performance

- [ ] **Shard count optimized** - Review based on expected load
- [ ] **Cache sizes tuned** - Adjust based on memory constraints
- [ ] **Compaction thresholds set** - Balance read/write performance
- [ ] **Analytics enabled** - Add `[[analytics_engine_datasets]]`

### Monitoring

- [ ] **Health endpoint accessible** - Test `/health` endpoint
- [ ] **Metrics endpoint configured** - Test `/metrics` endpoint
- [ ] **Alerting configured** - Set up alerts for error rates
- [ ] **Logging enabled** - Configure structured logging

### Networking

- [ ] **Custom domain configured** (optional) - Set up DNS and routes
- [ ] **SSL certificate valid** - Cloudflare manages automatically
- [ ] **Geographic routing** (if multi-region) - Configure smart routing

### Backup and Recovery

- [ ] **Backup strategy defined** - Consider R2 object versioning
- [ ] **Recovery procedures documented** - Test restore processes
- [ ] **Data retention policies** - Configure lifecycle rules

---

## Monitoring and Alerting

### Metrics Endpoint

MongoLake exposes Prometheus-compatible metrics:

```bash
# Prometheus text format
curl https://your-worker.workers.dev/metrics

# JSON format
curl https://your-worker.workers.dev/metrics/json
```

### Key Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `mongolake_http_requests_total` | Counter | Total HTTP requests by method/status |
| `mongolake_http_request_duration_seconds` | Histogram | Request latency distribution |
| `mongolake_query_duration_seconds` | Histogram | Query execution time |
| `mongolake_query_total` | Counter | Total queries by operation/status |
| `mongolake_slow_queries_total` | Counter | Queries exceeding 100ms threshold |
| `mongolake_r2_reads_total` | Counter | R2 read operations |
| `mongolake_r2_writes_total` | Counter | R2 write operations |
| `mongolake_r2_bytes_read_total` | Counter | Bytes read from R2 |
| `mongolake_r2_bytes_written_total` | Counter | Bytes written to R2 |
| `mongolake_compaction_cycles_total` | Counter | Compaction operations |
| `mongolake_buffer_size_bytes` | Gauge | Current buffer size per shard |
| `mongolake_wal_size_bytes` | Gauge | Current WAL size per shard |
| `mongolake_cache_hits_total` | Counter | Cache hit count |
| `mongolake_cache_misses_total` | Counter | Cache miss count |

### Workers Analytics Engine

For real-time analytics, enable Workers Analytics:

```toml
[[analytics_engine_datasets]]
binding = "ANALYTICS"
dataset = "mongolake_metrics"
```

Query analytics data using the Cloudflare Dashboard or GraphQL API:

```graphql
query {
  viewer {
    accounts(filter: { accountTag: "your-account-id" }) {
      mongolakeMetrics(
        filter: { datetime_gt: "2024-01-01T00:00:00Z" }
        limit: 1000
      ) {
        dimensions {
          blob1 # category
        }
        sum {
          double1 # duration_ms
        }
      }
    }
  }
}
```

### Health Checks

```bash
# Basic health check
curl https://your-worker.workers.dev/health
# Expected: {"status":"ok","version":"0.1.0"}

# Detailed health check (if implemented)
curl https://your-worker.workers.dev/health/detailed
```

### Alerting Rules (Example Prometheus)

```yaml
groups:
  - name: mongolake
    rules:
      - alert: HighErrorRate
        expr: rate(mongolake_http_requests_total{status=~"5.."}[5m]) > 0.1
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: High error rate detected

      - alert: SlowQueries
        expr: rate(mongolake_slow_queries_total[5m]) > 10
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: High rate of slow queries

      - alert: HighR2ErrorRate
        expr: rate(mongolake_r2_errors_total[5m]) > 0.01
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: R2 storage errors detected

      - alert: BufferNearFull
        expr: mongolake_buffer_size_bytes > 900000
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: Buffer approaching flush threshold
```

### Real-Time Logs

View real-time logs from your deployment:

```bash
# Stream production logs
wrangler tail --env production

# Filter by status
wrangler tail --env production --status error

# Filter by IP
wrangler tail --env production --ip-address 192.168.1.1
```

### Structured Logging

MongoLake outputs structured JSON logs for easy parsing:

```json
{
  "timestamp": "2024-01-15T10:30:45.123Z",
  "level": "info",
  "message": "Query executed",
  "operation": "find",
  "collection": "users",
  "duration_ms": 45,
  "documents_returned": 100
}
```

---

## Scaling Considerations

### Horizontal Scaling

MongoLake scales horizontally through Cloudflare's edge network:

- **Workers**: Automatically scale based on request volume
- **Durable Objects**: Scale per-collection with SQLite storage
- **R2**: Unlimited object storage with high throughput

### Vertical Scaling (Tuning)

Adjust these parameters based on workload:

#### Write-Heavy Workloads

```typescript
const config = {
  // Larger buffers for batching
  flushThresholdBytes: 4 * 1024 * 1024,  // 4 MB
  flushThresholdDocs: 10000,

  // Less aggressive compaction
  compactionMinBlockSize: 4_000_000,
  compactionMaxBlocksPerRun: 5,

  // Larger row groups
  rowGroupSize: 50000,
};
```

#### Read-Heavy Workloads

```typescript
const config = {
  // Smaller buffers for freshness
  flushThresholdBytes: 512 * 1024,  // 512 KB
  flushThresholdDocs: 500,

  // Aggressive compaction
  compactionMinBlockSize: 1_000_000,
  compactionMaxBlocksPerRun: 20,

  // Larger caches
  routerCacheSize: 50000,
  rpcCacheSize: 5000,
};
```

### Resource Limits

Be aware of Cloudflare Workers limits:

| Resource | Free | Paid |
|----------|------|------|
| CPU time per request | 10ms | 30s |
| Memory | 128 MB | 128 MB |
| Request body size | 100 MB | 100 MB |
| Subrequests | 50 | 1000 |
| Durable Object storage | 1 GB | 50 GB |

### Optimizing R2 Performance

1. **Minimize small writes**: Buffer documents and write in batches
2. **Use multipart uploads**: For files > 5MB
3. **Enable compression**: Parquet files are already compressed
4. **Optimize key names**: Avoid hot partitions with random prefixes

### Shard Splitting

Monitor shard metrics and split when:

- Document count exceeds 1 million per shard
- Write rate exceeds 10,000 ops/second
- Size exceeds 10 GB per shard

```typescript
// Check shard statistics
const stats = router.getStats();
console.log(`Cache hit rate: ${stats.cacheHits / stats.totalRoutes}`);

// Split a hot collection
if (metrics.getValue('mongolake_shard_write_rate', { shard: '0' }) > 10000) {
  router.splitCollection('hot_collection', [0, 4, 8, 12]);
}
```

### Connection Pooling

Configure connection limits per shard:

```typescript
const config = {
  maxConnectionsPerShard: 50,  // Default: 10
};
```

### Caching Strategy

Implement multi-tier caching:

1. **Buffer cache**: In-memory documents not yet flushed
2. **Router cache**: Shard assignments (default: 10,000 entries)
3. **RPC cache**: Recent read results (default: 1,000 entries, 5 min TTL)
4. **Zone map cache**: Column statistics for predicate pushdown

---

## Troubleshooting

### Common Issues

#### "R2 bucket not found"

Ensure the bucket name in `wrangler.toml` matches the created bucket:

```bash
# List buckets
wrangler r2 bucket list

# Verify wrangler.toml has correct name
[[r2_buckets]]
binding = "BUCKET"
bucket_name = "mongolake-data"  # Must match
```

#### "Durable Object not found"

Run migrations by deploying:

```bash
wrangler deploy
```

Ensure migrations are defined in `wrangler.toml`:

```toml
[[migrations]]
tag = "v1"
new_sqlite_classes = ["ShardDO"]
```

#### CORS Errors

Configure `ALLOWED_ORIGINS` environment variable:

```toml
[vars]
ALLOWED_ORIGINS = "https://your-app.com,https://staging.your-app.com"
```

For development, use `*` (not recommended for production):

```toml
[env.development]
vars = { ALLOWED_ORIGINS = "*" }
```

#### Authentication Failures

1. Check `REQUIRE_AUTH` is set correctly
2. Verify secrets are configured:
   ```bash
   wrangler secret list
   ```
3. Ensure JWT issuer and audience match configuration

#### Slow Queries

1. Check metrics for query patterns:
   ```bash
   curl https://your-worker.workers.dev/metrics | grep slow_queries
   ```

2. Enable debug logging:
   ```toml
   [vars]
   DEBUG = "true"
   ```

3. Consider adding indexes or adjusting shard configuration

#### Memory Errors

Reduce buffer sizes if hitting memory limits:

```toml
[vars]
FLUSH_THRESHOLD_BYTES = "524288"  # 512 KB
FLUSH_THRESHOLD_DOCS = "500"
```

### Debugging Commands

```bash
# View real-time logs
wrangler tail --env production

# Check worker status
wrangler deployments list

# View worker bindings
wrangler whoami

# Test locally
wrangler dev --env development

# Deploy preview version
wrangler deploy --env preview
```

### Getting Help

- [MongoLake Documentation](https://mongolake.com/docs)
- [GitHub Issues](https://github.com/dot-do/mongolake/issues)
- [Cloudflare Workers Documentation](https://developers.cloudflare.com/workers/)
- [Cloudflare R2 Documentation](https://developers.cloudflare.com/r2/)

---

## See Also

- [Local Development Guide](./deployment/local-development.md)
- [Cloudflare Workers Deployment](./deployment/cloudflare-workers.md)
- [Performance Tuning Guide](./operations/performance-tuning.md)
- [API Reference](./api/)
