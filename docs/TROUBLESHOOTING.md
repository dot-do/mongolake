# MongoLake Troubleshooting Guide

This comprehensive guide helps diagnose and resolve common issues when running MongoLake in development and production environments.

## Table of Contents

1. [Common Error Messages](#common-error-messages)
2. [Connection Issues](#connection-issues)
3. [Authentication and Authorization](#authentication-and-authorization)
4. [Query Performance Issues](#query-performance-issues)
5. [Replication and Sync Problems](#replication-and-sync-problems)
6. [Storage Errors](#storage-errors)
7. [Memory and Resource Issues](#memory-and-resource-issues)
8. [Debug Logging Configuration](#debug-logging-configuration)
9. [Health Check Endpoints](#health-check-endpoints)
10. [Gathering Diagnostic Information](#gathering-diagnostic-information)

---

## Common Error Messages

### MONGOLAKE_UNKNOWN / MONGOLAKE_INTERNAL

**Error:** `MongoLakeError: [MONGOLAKE_INTERNAL] Internal error occurred`

**Cause:** An unexpected internal error that doesn't match a specific category.

**Resolution:**
1. Check the full error details in the response for the `originalError` field
2. Enable debug logging to capture the stack trace
3. Review recent changes that may have introduced the error

### VALIDATION_FAILED / VALIDATION_INVALID_*

**Error:** `ValidationError: [VALIDATION_INVALID_FILTER] Invalid query filter`

**Cause:** The provided query, filter, update, or document doesn't match expected format.

**Resolution:**
1. Verify filter syntax matches MongoDB query syntax
2. Check for unsupported operators (see supported operators in documentation)
3. Ensure field names don't contain invalid characters

```typescript
// Common validation issues:

// BAD: Unsupported operator
await collection.find({ field: { $unsupported: 1 } });

// GOOD: Use supported operators ($eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $exists, $regex, $not)
await collection.find({ field: { $gt: 1 } });

// BAD: Invalid nested path depth (max 32 levels)
await collection.find({ 'a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z.aa.bb.cc.dd.ee.ff.gg': 1 });

// GOOD: Keep nesting reasonable
await collection.find({ 'user.profile.settings.theme': 'dark' });
```

### STORAGE_* Errors

**Error:** `StorageError: [STORAGE_WRITE_FAILED] R2 put failed for key: data/file.parquet`

**Cause:** Storage backend (R2, filesystem, S3) operation failed.

**Resolution:**
1. Check R2 bucket exists and is properly bound in `wrangler.toml`
2. Verify storage credentials and permissions
3. Check for rate limiting (429 errors)
4. Ensure storage key doesn't contain path traversal sequences (`..`)

### AUTH_* Errors

| Error Code | Meaning | Resolution |
|------------|---------|------------|
| `AUTH_MISSING_CREDENTIALS` | No token/API key provided | Include `Authorization: Bearer <token>` header |
| `AUTH_INVALID_TOKEN` | Token format or signature invalid | Verify token is correct and not corrupted |
| `AUTH_TOKEN_EXPIRED` | JWT token has expired | Refresh the token or re-authenticate |
| `AUTH_INVALID_API_KEY` | API key not recognized | Verify API key is correct |
| `AUTH_INSUFFICIENT_PERMISSIONS` | User lacks required permissions | Request additional permissions or use different credentials |

### QUERY_* Errors

| Error Code | Meaning | Resolution |
|------------|---------|------------|
| `QUERY_SYNTAX_ERROR` | Malformed query | Check query syntax |
| `QUERY_INVALID_OPERATOR` | Unsupported query operator | Use supported operators only |
| `QUERY_TIMEOUT` | Query exceeded time limit | Add indexes, optimize query, or increase timeout |
| `QUERY_CURSOR_NOT_FOUND` | Cursor expired or invalid | Re-execute the query |

### RPC_* and Network Errors

| Error Code | Meaning | Resolution |
|------------|---------|------------|
| `RPC_TRANSIENT` | Temporary network failure | Retry with exponential backoff |
| `RPC_SHARD_UNAVAILABLE` | Shard Durable Object unreachable | Check DO health, wait for recovery |
| `RPC_TIMEOUT` | Operation exceeded timeout | Optimize query or increase timeout |
| `RATE_LIMITED` | Too many requests | Implement rate limiting on client side |

### PARQUET_* Errors

| Error Code | Meaning | Resolution |
|------------|---------|------------|
| `PARQUET_INVALID_MAGIC` | File doesn't have PAR1 header | File is corrupted or not a Parquet file |
| `PARQUET_CORRUPTED` | File structure is invalid | Re-flush data, check for storage errors |
| `PARQUET_UNSUPPORTED_VERSION` | Parquet version not supported | Check hyparquet compatibility |

---

## Connection Issues

### Cloudflare Workers Connection

**Symptom:** Requests to Worker endpoint fail or timeout.

**Diagnosis:**
```bash
# Check Worker health
curl -v https://your-worker.workers.dev/health

# Expected response:
# {"status":"ok","version":"x.y.z"}
```

**Common Causes and Solutions:**

1. **Worker not deployed**
   ```bash
   # Deploy the worker
   wrangler deploy --env production

   # Verify deployment
   wrangler deployments list
   ```

2. **Route not configured**
   ```toml
   # wrangler.toml - ensure routes are configured
   routes = [
     { pattern = "api.example.com/*", zone_id = "your-zone-id" }
   ]
   ```

3. **CORS issues**
   ```typescript
   // Check ALLOWED_ORIGINS environment variable
   // In wrangler.toml:
   [vars]
   ALLOWED_ORIGINS = "https://your-app.com,https://localhost:3000"
   ```

### Durable Objects Connection

**Symptom:** `ShardUnavailableError: Shard X is unavailable`

**Diagnosis:**
```bash
# Check DO status via shard status endpoint
curl https://your-worker.workers.dev/api/mydb/_status
```

**Common Causes and Solutions:**

1. **Durable Object not migrated**
   ```toml
   # Ensure migration tag in wrangler.toml
   [[migrations]]
   tag = "v1"
   new_sqlite_classes = ["ShardDO"]
   ```
   ```bash
   # Deploy to apply migrations
   wrangler deploy
   ```

2. **DO hibernated/cold start**
   - First request after hibernation may be slow (500ms-2s)
   - Implement retry logic with exponential backoff

3. **DO overloaded**
   - Check metrics for high request rates
   - Consider increasing shard count for better distribution

### R2 Storage Connection

**Symptom:** `StorageError: R2 bucket not found` or `R2 get failed`

**Diagnosis:**
```bash
# List R2 buckets
wrangler r2 bucket list

# Check bucket contents
wrangler r2 object list mongolake-data --prefix "mydb/"
```

**Common Causes and Solutions:**

1. **Bucket not created**
   ```bash
   wrangler r2 bucket create mongolake-data
   ```

2. **Binding mismatch**
   ```toml
   # wrangler.toml - verify binding name matches code
   [[r2_buckets]]
   binding = "BUCKET"
   bucket_name = "mongolake-data"
   ```

3. **Rate limiting (429)**
   - Implement exponential backoff
   - Check R2 request quotas in Cloudflare dashboard

### Wire Protocol (mongosh/Compass)

**Symptom:** `mongosh` or MongoDB Compass cannot connect.

**Diagnosis:**
```bash
# Test local wire protocol server
mongosh mongodb://localhost:27017/test --eval "db.ping()"
```

**Common Causes and Solutions:**

1. **Server not running**
   ```bash
   # Start the dev server
   npx mongolake dev

   # Check if port is in use
   lsof -i :27017
   ```

2. **Port conflict**
   ```bash
   # Use different port
   npx mongolake dev --wire-port 27018
   mongosh mongodb://localhost:27018/test
   ```

3. **TLS mismatch**
   ```bash
   # If server uses TLS
   mongosh "mongodb://localhost:27017/test?tls=true"

   # If server doesn't use TLS (default for local dev)
   mongosh "mongodb://localhost:27017/test"
   ```

---

## Authentication and Authorization

### JWT Token Issues

**Symptom:** `AuthenticationError: Token validation failed`

**Diagnosis:**
```bash
# Decode JWT to check claims (don't share the token!)
# Using jwt.io or:
echo 'YOUR_TOKEN' | cut -d. -f2 | base64 -d 2>/dev/null | jq
```

**Common Causes:**

1. **Token expired**
   - Check `exp` claim in token
   - Implement token refresh logic

2. **Wrong audience/issuer**
   ```typescript
   // Ensure auth config matches token:
   const authConfig = {
     issuer: 'https://your-auth-provider.com',
     audience: 'your-api-identifier',
     // ...
   };
   ```

3. **Invalid signature**
   - Verify JWKS endpoint is accessible
   - Check that signing key hasn't rotated

### API Key Issues

**Symptom:** `AUTH_INVALID_API_KEY`

**Resolution:**
1. Verify API key is correctly formatted
2. Check if API key has been revoked
3. Ensure header format is correct: `Authorization: Bearer <api-key>` or `X-API-Key: <api-key>`

### Permission Issues

**Symptom:** `AUTH_INSUFFICIENT_PERMISSIONS`

**Resolution:**
1. Check user's roles in the token claims
2. Verify RBAC configuration allows the operation
3. Check collection-level permissions

```typescript
// Example: User needs 'write' role for inserts
// Token should have: { "roles": ["read", "write"] }
// Or collection-specific: { "permissions": ["mydb.users:write"] }
```

### OAuth Flow Issues

**Symptom:** OAuth login fails or redirects incorrectly.

**Common Causes:**

1. **Incorrect redirect URI**
   - Ensure redirect URI in OAuth config matches exactly
   - Check for trailing slashes

2. **CORS blocking OAuth popup**
   - Allow OAuth provider domain in CORS config

3. **Device flow timeout**
   - Device code typically expires in 15 minutes
   - Restart flow if expired

---

## Query Performance Issues

### Slow Queries

**Symptom:** Queries take longer than expected (>100ms for simple queries).

**Diagnosis:**
```bash
# Check slow query metrics
curl https://your-worker.workers.dev/metrics | grep slow_queries

# Enable debug logging to see query plans
LOG_LEVEL=debug npx mongolake dev
```

**Common Causes and Solutions:**

1. **Missing indexes**
   ```typescript
   // Create index for frequently queried fields
   await db.collection('users').createIndex({ email: 1 });
   ```

2. **Full collection scan**
   - Add filter on indexed fields
   - Use zone maps by querying on promoted columns

3. **Large result sets**
   ```typescript
   // Use projection to return only needed fields
   await collection.find({ status: 'active' }, {
     projection: { _id: 1, name: 1 },
     limit: 100
   });
   ```

4. **Buffer not flushed**
   - Recent writes may be in memory buffer
   - Force flush if needed: `POST /flush`

### Query Timeout

**Symptom:** `QUERY_TIMEOUT` error

**Resolution:**
1. Optimize query with indexes
2. Add filters to reduce scan scope
3. Increase timeout if query is inherently slow:
   ```typescript
   // Default timeout is 30 seconds
   const config = {
     operationTimeoutMs: 60000 // 60 seconds
   };
   ```

### Zone Map Inefficiency

**Symptom:** Queries scan many Parquet files unnecessarily.

**Resolution:**
1. Promote frequently filtered fields to columns:
   ```typescript
   const lake = new MongoLake({
     schema: {
       users: {
         columns: {
           status: 'string',
           createdAt: 'timestamp',
         }
       }
     }
   });
   ```

2. Use equality and range filters on promoted columns
3. Run compaction to rebuild zone maps with better statistics

### Aggregation Pipeline Performance

**Symptom:** Aggregation queries are slow.

**Resolution:**
1. Put `$match` stages early in pipeline
2. Use `$project` to reduce document size before `$group`
3. Limit `$lookup` result sizes with `pipeline` option

```typescript
// Optimized pipeline order
await collection.aggregate([
  { $match: { status: 'active' } },           // Filter early
  { $project: { name: 1, category: 1 } },     // Reduce fields
  { $group: { _id: '$category', count: { $sum: 1 } } },
  { $sort: { count: -1 } },
  { $limit: 10 }
]);
```

---

## Replication and Sync Problems

### Replica Lag

**Symptom:** Read replicas return stale data.

**Diagnosis:**
```bash
# Check replica status
curl https://your-worker.workers.dev/api/replica/status

# Response includes:
# {
#   "replicationState": {
#     "lagMs": 5000,
#     "appliedLSN": 12345,
#     "primaryLSN": 12350
#   }
# }
```

**Resolution:**
1. Check network connectivity between replica and primary
2. Force manual sync:
   ```bash
   curl -X POST https://your-worker.workers.dev/api/replica/sync
   ```
3. Adjust sync interval:
   ```typescript
   await replica.configure({
     syncIntervalMs: 1000  // Sync every second
   });
   ```

### Replica Too Stale Error

**Symptom:** `Error: Replica too stale: 60000ms lag exceeds max 30000ms`

**Resolution:**
1. Allow stale reads for non-critical queries:
   ```typescript
   const result = await replica.find('users', {}, {
     allowStale: true,
     maxStalenessMs: 120000  // Accept up to 2 minutes stale
   });
   ```

2. Route critical reads to primary
3. Investigate why replica is falling behind (network, primary overloaded)

### Sync Conflicts

**Symptom:** CLI sync reports conflicts between local and remote.

**Diagnosis:**
```bash
# Check sync status
npx mongolake sync status --database mydb

# View pending changes
npx mongolake sync diff --database mydb
```

**Resolution:**
1. Review conflicting files
2. Choose resolution strategy:
   ```bash
   # Accept remote version
   npx mongolake sync pull --database mydb --force

   # Push local version
   npx mongolake sync push --database mydb --force
   ```

### Sync Authentication Expired

**Symptom:** `Authentication token has expired. Please run mongolake auth login.`

**Resolution:**
```bash
# Re-authenticate
npx mongolake auth login --profile default

# Retry sync
npx mongolake sync push --database mydb
```

### Remote Unavailable

**Symptom:** Sync fails with connection errors.

**Diagnosis:**
```bash
# Check remote availability
curl https://your-remote-endpoint.com/health
```

**Resolution:**
1. Check internet connectivity
2. Verify remote endpoint URL is correct
3. Check if remote service is experiencing outage
4. Retry with exponential backoff

---

## Storage Errors

### R2 Errors

#### Rate Limiting (429)

**Symptom:** `TransientError: R2 rate limited during put`

**Resolution:**
1. Implement exponential backoff (automatic for transient errors)
2. Reduce batch sizes
3. Check R2 quotas in Cloudflare dashboard
4. Consider using multipart upload for large files

#### Object Not Found

**Symptom:** `StorageError: R2 get failed for key: data/missing.parquet`

**Resolution:**
1. Verify object path is correct
2. Check if object was deleted or never created
3. List objects to verify existence:
   ```bash
   wrangler r2 object list mongolake-data --prefix "data/"
   ```

#### Write Failed

**Symptom:** `STORAGE_WRITE_FAILED`

**Resolution:**
1. Check R2 bucket write permissions
2. Verify bucket hasn't reached capacity limits
3. Check for transient network issues (retry)
4. Verify object key is valid (no path traversal)

### Parquet File Errors

#### Invalid Magic Bytes

**Symptom:** `PARQUET_INVALID_MAGIC`

**Cause:** File doesn't start with "PAR1" magic bytes.

**Resolution:**
1. File may be corrupted or truncated
2. Delete and re-flush the data:
   ```bash
   # Delete corrupted file
   wrangler r2 object delete mongolake-data "path/to/corrupted.parquet"

   # Force flush to regenerate
   curl -X POST https://your-worker.workers.dev/api/mydb/_flush
   ```

#### Corrupted File

**Symptom:** `PARQUET_CORRUPTED` or parsing errors

**Resolution:**
1. Check storage for corruption (bit rot)
2. Restore from backup if available
3. If data is in WAL, force recovery:
   ```bash
   curl -X POST https://your-worker.workers.dev/api/mydb/_recover
   ```

### Filesystem Storage Errors (Local Dev)

#### Permission Denied

**Symptom:** `EACCES: permission denied`

**Resolution:**
1. Check file/directory permissions:
   ```bash
   ls -la .mongolake/
   ```
2. Fix permissions:
   ```bash
   chmod -R 755 .mongolake/
   ```

#### Disk Full

**Symptom:** `ENOSPC: no space left on device`

**Resolution:**
1. Check disk space: `df -h`
2. Clean up old files:
   ```bash
   # Remove old delta files after compaction
   find .mongolake -name "*.parquet" -mtime +7 -delete
   ```
3. Run compaction to merge small files

### Path Traversal Errors

**Symptom:** `InvalidStorageKeyError: Storage key cannot contain path traversal sequences`

**Cause:** Attempting to use `..` or absolute paths in storage keys.

**Resolution:**
```typescript
// BAD: Path traversal attempt
await storage.get('../../../etc/passwd');
await storage.get('/etc/passwd');

// GOOD: Valid relative paths
await storage.get('mydb/users/data.parquet');
```

---

## Memory and Resource Issues

### Buffer Memory

**Symptom:** High memory usage, potential OOM in Durable Objects.

**Diagnosis:**
```bash
# Check buffer sizes
curl https://your-worker.workers.dev/api/mydb/shard-0/status

# Response includes buffer stats:
# {
#   "bufferSizeBytes": 1048576,
#   "bufferDocCount": 500
# }
```

**Resolution:**
1. Reduce flush thresholds:
   ```typescript
   const config = {
     flushThresholdBytes: 512 * 1024,  // 512KB instead of 1MB
     flushThresholdDocs: 500           // 500 instead of 1000
   };
   ```
2. Force flush when memory is high:
   ```bash
   curl -X POST https://your-worker.workers.dev/api/mydb/_flush
   ```

### WAL Memory Pressure

**Symptom:** WAL grows too large, forced flushes.

**Diagnosis:**
```bash
# Check WAL metrics
curl https://your-worker.workers.dev/metrics | grep wal
```

**Resolution:**
1. WAL has automatic limits:
   - Max 10MB size
   - Max 10,000 entries
2. Increase flush frequency if WAL consistently hits limits
3. Monitor `mongolake_wal_forced_flushes_total` metric

### Cache Memory

**Symptom:** Cache evictions, reduced hit rate.

**Diagnosis:**
```bash
curl https://your-worker.workers.dev/metrics | grep cache
```

**Resolution:**
1. Increase cache sizes for hot data:
   ```typescript
   const config = {
     routerCacheSize: 20000,  // Default: 10000
     rpcCacheSize: 2000,      // Default: 1000
   };
   ```
2. Reduce cache TTL for rapidly changing data
3. Monitor hit/miss ratios and adjust

### Connection Pool Exhaustion

**Symptom:** Connection timeouts, `ECONNRESET`

**Resolution:**
1. Increase max connections per shard:
   ```typescript
   const config = {
     maxConnectionsPerShard: 20  // Default: 10
   };
   ```
2. Implement connection pooling on client side
3. Add request queuing with backpressure

### Worker CPU Limits

**Symptom:** Worker execution exceeds CPU time limit.

**Resolution:**
1. Break large operations into smaller batches
2. Use streaming for large data transfers
3. Offload heavy computation to Durable Objects
4. Check for expensive operations (deep nesting, large aggregations)

---

## Debug Logging Configuration

### Environment Variables

```bash
# Set log level (debug, info, warn, error)
export LOG_LEVEL=debug

# Set environment (affects default log level and format)
export ENVIRONMENT=development  # JSON output: false, default level: debug
export ENVIRONMENT=production   # JSON output: true, default level: info
```

### Programmatic Configuration

```typescript
import { Logger, createLogger } from 'mongolake/utils/logger';

// Create custom logger
const logger = createLogger({
  level: 'debug',
  environment: 'development',
  jsonOutput: false,  // Human-readable format
  defaultContext: {
    service: 'my-app',
    version: '1.0.0'
  }
});

// Use logger
logger.debug('Processing query', { collection: 'users', filter: query });
logger.info('Query completed', { durationMs: 45 });
logger.warn('Slow query detected', { durationMs: 150 });
logger.error('Query failed', { error: err.message, stack: err.stack });
```

### Request ID Tracking

```typescript
import { withRequestId, generateRequestId, getRequestId } from 'mongolake/utils/logger';

// Track requests across async operations
const requestId = generateRequestId();
await withRequestId(requestId, async () => {
  // All logs within this context include the request ID
  logger.info('Processing request');  // Includes requestId in context
  await doSomethingAsync();
  logger.info('Request complete');
});
```

### Cloudflare Workers Logging

```bash
# View real-time logs
wrangler tail --env production

# Filter by status
wrangler tail --env production --status error

# Filter by search term
wrangler tail --env production --search "QUERY_TIMEOUT"
```

### Log Output Format

**Development (human-readable):**
```
[2024-01-15T10:30:45.123Z] DEBUG Processing query {"collection":"users","requestId":"req-abc123"}
[2024-01-15T10:30:45.168Z] INFO  Query completed {"durationMs":45,"requestId":"req-abc123"}
```

**Production (JSON):**
```json
{"timestamp":"2024-01-15T10:30:45.123Z","level":"debug","message":"Processing query","context":{"collection":"users","requestId":"req-abc123","environment":"production"}}
```

---

## Health Check Endpoints

### Worker Health Check

```bash
# Basic health check
curl https://your-worker.workers.dev/health

# Response:
{
  "status": "ok",
  "version": "0.1.0"
}
```

### Shard Status

```bash
# Check specific shard
curl https://your-worker.workers.dev/api/mydb/shard-0/status

# Response:
{
  "shardId": 0,
  "bufferSizeBytes": 524288,
  "bufferDocCount": 250,
  "walEntries": 250,
  "walSizeBytes": 524288,
  "lastFlushLSN": 1000,
  "currentLSN": 1250,
  "collections": ["users", "orders"]
}
```

### Metrics Endpoint

```bash
# Prometheus format
curl https://your-worker.workers.dev/metrics

# JSON format
curl https://your-worker.workers.dev/metrics/json
```

**Key metrics to monitor:**

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `mongolake_query_duration_seconds` | Query latency | p99 > 1s |
| `mongolake_slow_queries_total` | Slow query count | > 100/min |
| `mongolake_r2_errors_total` | Storage errors | > 10/min |
| `mongolake_buffer_size_bytes` | Buffer memory | > 5MB |
| `mongolake_wal_entries` | WAL size | > 8000 entries |
| `mongolake_active_connections` | Connection count | > 80% of max |

### Replica Status

```bash
curl https://your-worker.workers.dev/api/replica/status

# Response:
{
  "replicaId": "replica-abc123",
  "primaryShardId": "shard-0",
  "initialized": true,
  "replicationState": {
    "status": "syncing",
    "appliedLSN": 12345,
    "primaryLSN": 12350,
    "lagMs": 500,
    "lastSyncAt": "2024-01-15T10:30:45.123Z"
  },
  "bufferStats": {
    "documentCount": 100,
    "collectionCount": 5
  }
}
```

---

## Gathering Diagnostic Information

### Diagnostic Checklist

When reporting issues, gather the following information:

1. **Environment Information**
   ```bash
   # MongoLake version
   npm list mongolake

   # Node.js version
   node --version

   # Wrangler version (for Workers)
   wrangler --version
   ```

2. **Configuration**
   - `wrangler.toml` (remove secrets)
   - Schema configuration
   - Environment variables (remove sensitive values)

3. **Error Details**
   ```bash
   # Full error response including:
   # - Error code
   # - Error message
   # - Error details/context
   # - Stack trace (if available)
   ```

4. **Metrics Snapshot**
   ```bash
   curl https://your-worker.workers.dev/metrics > metrics.txt
   ```

5. **Recent Logs**
   ```bash
   wrangler tail --env production --format json > logs.json
   ```

### Diagnostic Script

```bash
#!/bin/bash
# diagnostic.sh - Gather MongoLake diagnostic information

echo "=== MongoLake Diagnostics ==="
echo "Timestamp: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo ""

echo "=== Environment ==="
npm list mongolake 2>/dev/null || echo "mongolake not found in npm"
node --version
wrangler --version 2>/dev/null || echo "wrangler not installed"
echo ""

echo "=== Health Check ==="
curl -s https://your-worker.workers.dev/health | jq .
echo ""

echo "=== Metrics Summary ==="
curl -s https://your-worker.workers.dev/metrics | grep -E "^mongolake_(query_total|slow_queries|r2_errors|buffer_size|wal_entries)" | head -20
echo ""

echo "=== Recent Errors ==="
wrangler tail --env production --status error --once 2>/dev/null || echo "Cannot fetch logs"
```

### Remote Debugging

For production issues requiring deeper investigation:

1. **Enable verbose logging temporarily**
   ```bash
   wrangler secret put LOG_LEVEL
   # Enter: debug
   ```

2. **Capture traffic sample**
   ```bash
   # Tail logs for 5 minutes
   timeout 300 wrangler tail --env production --format json > debug-logs.json
   ```

3. **Check Cloudflare dashboard**
   - Workers Analytics for request patterns
   - R2 metrics for storage patterns
   - Durable Objects analytics for DO health

4. **Disable verbose logging**
   ```bash
   wrangler secret put LOG_LEVEL
   # Enter: info
   ```

### Common Diagnostic Queries

```bash
# Count errors by type in last hour
curl -s https://your-worker.workers.dev/metrics | \
  grep 'mongolake.*error' | \
  awk '{print $1, $NF}'

# Check query latency percentiles
curl -s https://your-worker.workers.dev/metrics | \
  grep 'mongolake_query_duration_seconds'

# Check storage operation latencies
curl -s https://your-worker.workers.dev/metrics | \
  grep 'mongolake_r2_operation_duration'

# Check buffer pressure
curl -s https://your-worker.workers.dev/metrics | \
  grep -E 'mongolake_(buffer_size|wal_entries|flush_operations)'
```

### Getting Help

If issues persist after following this guide:

1. **Search existing issues**: [GitHub Issues](https://github.com/dot-do/mongolake/issues)
2. **Community support**: [Discord](https://discord.gg/mongolake)
3. **File a bug report** with:
   - Clear description of the problem
   - Steps to reproduce
   - Expected vs actual behavior
   - Diagnostic information gathered above
   - Relevant code snippets (sanitized of secrets)
