# Deploying MongoLake to Cloudflare Workers

This guide covers deploying MongoLake as a Cloudflare Worker with R2 storage and Durable Objects for persistence.

## Prerequisites

- [Cloudflare account](https://dash.cloudflare.com/sign-up)
- [Wrangler CLI](https://developers.cloudflare.com/workers/wrangler/install-and-update/) installed
- Node.js 18+

## wrangler.toml Configuration

Create a `wrangler.toml` file in your project root:

```toml
name = "my-mongolake"
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

# Development environment
[env.development]
vars = { ENVIRONMENT = "development" }

# Production environment
[env.production]
vars = { ENVIRONMENT = "production" }
```

## R2 Bucket Setup

### Create an R2 Bucket

```bash
# Create a new R2 bucket for MongoLake data
wrangler r2 bucket create mongolake-data

# Verify bucket creation
wrangler r2 bucket list
```

### Bucket Configuration

The R2 bucket stores:
- Parquet data files (`{database}/{collection}/*.parquet`)
- Collection manifests
- Iceberg metadata (optional)

For production, consider:
- Enabling [R2 lifecycle rules](https://developers.cloudflare.com/r2/buckets/object-lifecycles/) for old delta files
- Setting up [R2 event notifications](https://developers.cloudflare.com/r2/buckets/event-notifications/) for monitoring

## Durable Object Bindings

MongoLake uses Durable Objects (ShardDO) for:
- **Write-Ahead Log (WAL)**: Durability for writes before R2 flush
- **In-memory buffer**: Fast path for recent writes
- **Compaction coordination**: Background merging of small files

### Binding Configuration

```toml
[durable_objects]
bindings = [
  { name = "RPC_NAMESPACE", class_name = "ShardDO" }
]

[[migrations]]
tag = "v1"
new_sqlite_classes = ["ShardDO"]
```

The `new_sqlite_classes` migration enables SQLite storage within the Durable Object for WAL persistence.

## Environment Variables

### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `BUCKET` | R2 bucket binding | (automatic from wrangler.toml) |
| `RPC_NAMESPACE` | Durable Object namespace | (automatic from wrangler.toml) |

### Optional Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `ENVIRONMENT` | Environment name | `"development"` |
| `REQUIRE_AUTH` | Require authentication | `false` |
| `ALLOWED_ORIGINS` | CORS allowed origins | `"*"` |
| `R2_DATA_CATALOG_TOKEN` | Enables Iceberg mode | (none) |

### Secrets

Store sensitive values as secrets:

```bash
# Set OAuth secret for authentication
wrangler secret put OAUTH_SECRET

# Set API keys or tokens
wrangler secret put R2_DATA_CATALOG_TOKEN
```

## Service Bindings (Optional)

For low-latency authentication, configure service bindings to auth services:

```toml
# Uncomment to enable auth service bindings
# [[services]]
# binding = "AUTH"
# service = "auth-service"

# [[services]]
# binding = "OAUTH"
# service = "oauth-service"
```

## Worker Entry Point

Create your worker entry point:

```typescript
// src/index.ts
import { MongoLakeWorker } from 'mongolake/worker';
import { ShardDO } from 'mongolake/do';

export default MongoLakeWorker;
export { ShardDO };
```

## Deploy Commands

### Development

```bash
# Start local development server
wrangler dev

# Or with specific environment
wrangler dev --env development
```

### Production Deployment

```bash
# Deploy to production
wrangler deploy --env production

# Deploy with specific Cloudflare account
wrangler deploy --env production --account-id <account-id>
```

### Preview Deployment

```bash
# Deploy a preview version
wrangler deploy --env preview
```

## Post-Deployment Verification

### Health Check

```bash
# Check worker health
curl https://my-mongolake.<subdomain>.workers.dev/health

# Expected response:
# {"status":"ok","version":"0.1.0"}
```

### Test API

```bash
# Insert a document
curl -X POST https://my-mongolake.<subdomain>.workers.dev/api/mydb/users \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice", "email": "alice@example.com"}'

# Query documents
curl "https://my-mongolake.<subdomain>.workers.dev/api/mydb/users?limit=10"
```

## Production Checklist

- [ ] R2 bucket created and bound
- [ ] Durable Object migrations applied
- [ ] Environment variables configured
- [ ] Secrets set for sensitive values
- [ ] CORS origins configured (ALLOWED_ORIGINS)
- [ ] Authentication enabled if needed (REQUIRE_AUTH)
- [ ] Custom domain configured (optional)
- [ ] Analytics enabled (optional)

## Monitoring

### Metrics Endpoint

MongoLake exposes Prometheus-compatible metrics:

```bash
# Prometheus format
curl https://my-mongolake.<subdomain>.workers.dev/metrics

# JSON format
curl https://my-mongolake.<subdomain>.workers.dev/metrics/json
```

### Workers Analytics

Enable Workers Analytics Engine for detailed observability:

```toml
# Add to wrangler.toml
[[analytics_engine_datasets]]
binding = "ANALYTICS"
dataset = "mongolake_metrics"
```

## Troubleshooting

### Common Issues

1. **"R2 bucket not found"**: Ensure bucket name in wrangler.toml matches created bucket
2. **"Durable Object not found"**: Run migrations with `wrangler deploy`
3. **CORS errors**: Configure `ALLOWED_ORIGINS` variable
4. **Authentication failures**: Check `REQUIRE_AUTH` and secret configuration

### Logs

View real-time logs:

```bash
wrangler tail --env production
```
