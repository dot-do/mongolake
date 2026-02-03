# MongoLake Environment Variables

This document provides comprehensive documentation of all environment variables used by MongoLake, including configuration options for different deployment scenarios.

## Table of Contents

- [Overview](#overview)
- [Worker Configuration](#worker-configuration)
- [R2 Storage Settings](#r2-storage-settings)
- [Authentication Settings](#authentication-settings)
- [Logging Configuration](#logging-configuration)
- [Performance Tuning](#performance-tuning)
- [Debug Flags](#debug-flags)
- [Iceberg Integration](#iceberg-integration)
- [TCP Server (Wire Protocol)](#tcp-server-wire-protocol)
- [Example Environment Files](#example-environment-files)
- [Security Considerations](#security-considerations)
- [Cloudflare Workers Secrets Management](#cloudflare-workers-secrets-management)

---

## Overview

MongoLake uses environment variables for configuration across different deployment contexts:

1. **Cloudflare Workers** - Variables defined in `wrangler.toml` or via the dashboard
2. **Node.js/CLI** - Standard `process.env` variables, often loaded from `.env` files
3. **Testing** - Variables set in test configuration or CI/CD pipelines

### Variable Types

| Type | Description | Example |
|------|-------------|---------|
| **Required** | Must be set for the service to function | `BUCKET` |
| **Optional** | Provides additional functionality when set | `ANALYTICS` |
| **Conditional** | Required only in certain modes | `REQUIRE_AUTH` |

---

## Worker Configuration

### ENVIRONMENT

**Type:** String
**Required:** No
**Default:** `"production"` (for safety)
**Values:** `"development"`, `"staging"`, `"test"`, `"production"`

Determines the runtime environment. Affects:
- Error message verbosity (production masks 5xx errors)
- Default log level
- JSON output formatting
- Test token acceptance

```toml
# wrangler.toml
[vars]
ENVIRONMENT = "development"

[env.production]
vars = { ENVIRONMENT = "production" }
```

### BUCKET

**Type:** R2Bucket binding
**Required:** Yes (Worker)
**Default:** None

The R2 bucket used for storing Parquet data files. This is a binding, not a string value.

```toml
# wrangler.toml
[[r2_buckets]]
binding = "BUCKET"
bucket_name = "mongolake-data"
```

### RPC_NAMESPACE

**Type:** DurableObjectNamespace binding
**Required:** Yes (Worker)
**Default:** None

Durable Object namespace for shard coordination. Required for write operations and read-your-writes consistency.

```toml
# wrangler.toml
[durable_objects]
bindings = [
  { name = "RPC_NAMESPACE", class_name = "ShardDO" }
]
```

### ALLOWED_ORIGINS

**Type:** String
**Required:** No
**Default:** `"*"` (all origins allowed)

CORS allowed origins. Set to specific domains in production for security.

```toml
# wrangler.toml
[vars]
ALLOWED_ORIGINS = "https://app.example.com,https://admin.example.com"
```

---

## R2 Storage Settings

### DATA_BUCKET

**Type:** R2Bucket binding
**Required:** Yes (ShardDO)
**Default:** None

R2 bucket for storing Parquet data files. Used by ShardDO for persistence.

```toml
# wrangler.toml
[[r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "mongolake-data"
```

### SHARD_DO

**Type:** DurableObjectNamespace binding
**Required:** Yes (ShardDO)
**Default:** None

Durable Object namespace for shard instances. Used for routing and cross-shard operations.

```toml
# wrangler.toml
[[durable_objects.bindings]]
name = "SHARD_DO"
class_name = "ShardDO"
```

---

## Authentication Settings

### REQUIRE_AUTH

**Type:** Boolean
**Required:** No
**Default:** `false`

When `true`, all API requests must include valid authentication credentials (Bearer token or API key).

```toml
# wrangler.toml
[vars]
REQUIRE_AUTH = true
```

### OAUTH_SECRET

**Type:** String (Secret)
**Required:** No
**Default:** None

Secret key for OAuth token validation. Should be stored as a Cloudflare secret, not in `wrangler.toml`.

```bash
# Set via Wrangler CLI
wrangler secret put OAUTH_SECRET
```

### AUTH (Service Binding)

**Type:** Service binding
**Required:** No
**Default:** None

Service binding to an external authentication service for low-latency token validation.

```toml
# wrangler.toml
[[services]]
binding = "AUTH"
service = "auth-service"
```

### OAUTH (Service Binding)

**Type:** Service binding
**Required:** No
**Default:** None

Service binding to an OAuth service for token refresh and exchange operations.

```toml
# wrangler.toml
[[services]]
binding = "OAUTH"
service = "oauth-service"
```

### AUTH_CONFIG

**Type:** Object
**Required:** No
**Default:** None

Configuration object for authentication middleware. Typically set programmatically rather than via environment variables.

### ALLOW_TEST_TOKENS

**Type:** String
**Required:** No
**Default:** `"false"`

When set to `"true"`, allows test tokens (with old timestamps) to bypass expiration checks. **Never enable in production.**

```bash
# Node.js environment only
export ALLOW_TEST_TOKENS=true
```

---

## Logging Configuration

### LOG_LEVEL

**Type:** String
**Required:** No
**Default:** `"debug"` (development/test), `"info"` (production/staging)
**Values:** `"debug"`, `"info"`, `"warn"`, `"error"`

Minimum log level to output. Lower levels include higher severity logs.

```bash
export LOG_LEVEL=debug
```

### NODE_ENV

**Type:** String
**Required:** No
**Default:** None

Standard Node.js environment variable. Used for test detection and production checks.

```bash
export NODE_ENV=production
```

---

## Performance Tuning

### ANALYTICS

**Type:** AnalyticsEngineDataset binding
**Required:** No
**Default:** None

Workers Analytics Engine dataset for metrics collection. Enables advanced observability features.

```toml
# wrangler.toml
[[analytics_engine_datasets]]
binding = "ANALYTICS"
dataset = "mongolake_metrics"
```

### Configuration Constants

While not environment variables, these constants in `src/constants.ts` control performance behavior:

| Constant | Default | Description |
|----------|---------|-------------|
| `DEFAULT_FLUSH_THRESHOLD_BYTES` | 1MB | Buffer size triggering auto-flush |
| `DEFAULT_FLUSH_THRESHOLD_DOCS` | 1,000 | Document count triggering auto-flush |
| `DEFAULT_ROW_GROUP_SIZE` | 10,000 | Parquet row group size |
| `DEFAULT_SHARD_COUNT` | 16 | Number of shards for data distribution |
| `MAX_WAL_SIZE_BYTES` | 10MB | Maximum WAL size before forced flush |
| `MAX_WAL_ENTRIES` | 10,000 | Maximum WAL entries before forced flush |
| `DEFAULT_CACHE_MAX_SIZE` | 1,000 | Maximum cached tokens |
| `DEFAULT_CACHE_TTL_SECONDS` | 300 | Token cache TTL |

---

## Debug Flags

### VITEST

**Type:** String
**Required:** No
**Default:** None

Set automatically by Vitest test runner. Used to detect test environment.

### MONGOLAKE_DEBUG

**Type:** String
**Required:** No
**Default:** `"false"`

Enable debug mode for the TCP wire protocol server.

```bash
export MONGOLAKE_DEBUG=true
```

---

## Iceberg Integration

### CF_ACCOUNT_ID

**Type:** String
**Required:** Conditional (R2 Data Catalog)
**Default:** None

Cloudflare Account ID for R2 Data Catalog API access.

```bash
export CF_ACCOUNT_ID=your-account-id
```

### R2_DATA_CATALOG_TOKEN

**Type:** String (Secret)
**Required:** Conditional (R2 Data Catalog)
**Default:** None

API token for R2 Data Catalog access.

```bash
export R2_DATA_CATALOG_TOKEN=your-api-token
```

### ICEBERG_REST_CATALOG_URI

**Type:** String
**Required:** Conditional (REST Catalog)
**Default:** None

Base URI for Iceberg REST Catalog API.

```bash
export ICEBERG_REST_CATALOG_URI=https://catalog.example.com/api/v1
```

### ICEBERG_WAREHOUSE

**Type:** String
**Required:** No
**Default:** None

Warehouse location for Iceberg REST Catalog.

```bash
export ICEBERG_WAREHOUSE=s3://my-bucket/warehouse
```

### ICEBERG_REST_TOKEN

**Type:** String (Secret)
**Required:** No
**Default:** None

Bearer token for Iceberg REST Catalog authentication.

```bash
export ICEBERG_REST_TOKEN=your-bearer-token
```

### ICEBERG_REST_CREDENTIAL

**Type:** String (Secret)
**Required:** No
**Default:** None

OAuth2 credential for Iceberg REST Catalog (format: `client_id:client_secret`).

```bash
export ICEBERG_REST_CREDENTIAL=client_id:client_secret
```

### ICEBERG_REST_SCOPE

**Type:** String
**Required:** No
**Default:** None

OAuth2 scope for Iceberg REST Catalog authentication.

```bash
export ICEBERG_REST_SCOPE=catalog:read catalog:write
```

---

## TCP Server (Wire Protocol)

These variables configure the standalone TCP server for MongoDB wire protocol compatibility.

### MONGOLAKE_PORT

**Type:** Number
**Required:** No
**Default:** `27017`

TCP port for the wire protocol server.

```bash
export MONGOLAKE_PORT=27017
```

### MONGOLAKE_HOST

**Type:** String
**Required:** No
**Default:** `"127.0.0.1"`

Host address to bind the wire protocol server.

```bash
export MONGOLAKE_HOST=0.0.0.0
```

### MONGOLAKE_DATA

**Type:** String
**Required:** No
**Default:** `".mongolake"`

Directory for local data storage when running in standalone mode.

```bash
export MONGOLAKE_DATA=/var/lib/mongolake
```

### MONGOLAKE_SHUTDOWN_TIMEOUT

**Type:** Number
**Required:** No
**Default:** `30000` (30 seconds)

Timeout in milliseconds for graceful shutdown.

```bash
export MONGOLAKE_SHUTDOWN_TIMEOUT=60000
```

---

## E2E Testing

### MONGOLAKE_E2E_URL

**Type:** String
**Required:** No
**Default:** `"http://localhost:8787"`

Base URL for end-to-end tests.

```bash
export MONGOLAKE_E2E_URL=http://localhost:8787
```

---

## Example Environment Files

### Development (.env.development)

```bash
# MongoLake Development Environment

# Core Settings
ENVIRONMENT=development
LOG_LEVEL=debug

# Local Server
MONGOLAKE_PORT=27017
MONGOLAKE_HOST=127.0.0.1
MONGOLAKE_DATA=.mongolake
MONGOLAKE_DEBUG=true

# Authentication (disabled for dev)
# REQUIRE_AUTH=false

# Testing
ALLOW_TEST_TOKENS=true
MONGOLAKE_E2E_URL=http://localhost:8787
```

### Staging (.env.staging)

```bash
# MongoLake Staging Environment

# Core Settings
ENVIRONMENT=staging
LOG_LEVEL=info

# Authentication
REQUIRE_AUTH=true

# Iceberg (if using)
# ICEBERG_REST_CATALOG_URI=https://staging-catalog.example.com/api/v1
# ICEBERG_WAREHOUSE=s3://staging-bucket/warehouse

# Testing
MONGOLAKE_E2E_URL=https://staging.mongolake.example.com
```

### Production (.env.production)

```bash
# MongoLake Production Environment
# IMPORTANT: Sensitive values should be stored in Cloudflare Secrets, not here

# Core Settings
ENVIRONMENT=production
LOG_LEVEL=info

# Authentication (required in production)
REQUIRE_AUTH=true

# CORS (restrict to your domains)
# Set in wrangler.toml: ALLOWED_ORIGINS=https://app.example.com

# Iceberg Integration (if using)
# CF_ACCOUNT_ID is set via Cloudflare dashboard
# R2_DATA_CATALOG_TOKEN is stored as a secret

# NEVER enable in production
# ALLOW_TEST_TOKENS=false
```

### wrangler.toml Example

```toml
name = "mongolake"
main = "src/index.ts"
compatibility_date = "2024-09-02"
compatibility_flags = ["nodejs_compat_v2"]

# Durable Objects
[durable_objects]
bindings = [
  { name = "RPC_NAMESPACE", class_name = "ShardDO" }
]

[[migrations]]
tag = "v1"
new_sqlite_classes = ["ShardDO"]

# R2 Storage
[[r2_buckets]]
binding = "BUCKET"
bucket_name = "mongolake-data"

# Analytics (optional)
# [[analytics_engine_datasets]]
# binding = "ANALYTICS"
# dataset = "mongolake_metrics"

# Default variables
[vars]
ENVIRONMENT = "development"

# Development environment
[env.development]
vars = { ENVIRONMENT = "development" }

# Staging environment
[env.staging]
vars = { ENVIRONMENT = "staging", REQUIRE_AUTH = true }

# Production environment
[env.production]
vars = { ENVIRONMENT = "production", REQUIRE_AUTH = true }

# Service Bindings (uncomment to enable)
# [[services]]
# binding = "AUTH"
# service = "auth-service"

# [[services]]
# binding = "OAUTH"
# service = "oauth-service"
```

---

## Security Considerations

### Sensitive Variables

The following variables contain sensitive information and should **never** be committed to source control:

| Variable | Sensitivity | Storage Recommendation |
|----------|-------------|----------------------|
| `OAUTH_SECRET` | High | Cloudflare Secrets |
| `R2_DATA_CATALOG_TOKEN` | High | Cloudflare Secrets or env var |
| `ICEBERG_REST_TOKEN` | High | Cloudflare Secrets or env var |
| `ICEBERG_REST_CREDENTIAL` | High | Cloudflare Secrets or env var |
| `CF_ACCOUNT_ID` | Medium | Cloudflare Secrets or env var |

### Production Checklist

1. **Enable authentication**: Set `REQUIRE_AUTH=true`
2. **Restrict CORS origins**: Set `ALLOWED_ORIGINS` to specific domains
3. **Disable test tokens**: Ensure `ALLOW_TEST_TOKENS` is not set
4. **Set ENVIRONMENT**: Explicitly set `ENVIRONMENT=production`
5. **Use secrets**: Store sensitive values in Cloudflare Secrets
6. **Review log level**: Use `info` or `warn` in production

### Environment Isolation

```bash
# Verify you're not using development values in production
if [ "$ENVIRONMENT" = "production" ]; then
  if [ "$ALLOW_TEST_TOKENS" = "true" ]; then
    echo "ERROR: ALLOW_TEST_TOKENS must not be enabled in production"
    exit 1
  fi
  if [ -z "$REQUIRE_AUTH" ] || [ "$REQUIRE_AUTH" != "true" ]; then
    echo "WARNING: REQUIRE_AUTH should be enabled in production"
  fi
fi
```

---

## Cloudflare Workers Secrets Management

### Creating Secrets

Use the Wrangler CLI to create secrets:

```bash
# Interactive prompt (recommended for sensitive values)
wrangler secret put OAUTH_SECRET

# From environment variable
wrangler secret put OAUTH_SECRET --env production

# For specific environment
wrangler secret put OAUTH_SECRET --env staging
```

### Listing Secrets

```bash
wrangler secret list
wrangler secret list --env production
```

### Deleting Secrets

```bash
wrangler secret delete OAUTH_SECRET
wrangler secret delete OAUTH_SECRET --env production
```

### Secrets vs Variables

| Aspect | Secrets | Variables |
|--------|---------|-----------|
| Storage | Encrypted | Plain text |
| Visibility | Hidden in logs/dashboard | Visible |
| Access | `env.SECRET_NAME` | `env.VAR_NAME` |
| Use case | API keys, tokens, passwords | Configuration flags |

### Best Practices

1. **Use secrets for sensitive data**: Never store tokens or credentials in `wrangler.toml`
2. **Rotate secrets regularly**: Implement a secret rotation policy
3. **Limit secret access**: Use environment-specific secrets
4. **Audit secret usage**: Monitor access patterns
5. **Document secret requirements**: Maintain a list of required secrets per environment

### CI/CD Integration

For GitHub Actions:

```yaml
# .github/workflows/deploy.yml
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Deploy to Cloudflare
        uses: cloudflare/wrangler-action@v3
        with:
          apiToken: ${{ secrets.CF_API_TOKEN }}
          secrets: |
            OAUTH_SECRET
            R2_DATA_CATALOG_TOKEN
        env:
          OAUTH_SECRET: ${{ secrets.OAUTH_SECRET }}
          R2_DATA_CATALOG_TOKEN: ${{ secrets.R2_DATA_CATALOG_TOKEN }}
```

---

## Quick Reference

### Required Variables by Component

| Component | Required Variables |
|-----------|-------------------|
| Worker (HTTP) | `BUCKET`, `RPC_NAMESPACE` |
| ShardDO | `DATA_BUCKET`, `SHARD_DO` |
| TCP Server | None (all have defaults) |
| Iceberg (R2 Catalog) | `CF_ACCOUNT_ID`, `R2_DATA_CATALOG_TOKEN` |
| Iceberg (REST) | `ICEBERG_REST_CATALOG_URI` |

### Default Values Summary

| Variable | Default |
|----------|---------|
| `ENVIRONMENT` | `"production"` |
| `LOG_LEVEL` | `"debug"` (dev) / `"info"` (prod) |
| `ALLOWED_ORIGINS` | `"*"` |
| `REQUIRE_AUTH` | `false` |
| `MONGOLAKE_PORT` | `27017` |
| `MONGOLAKE_HOST` | `"127.0.0.1"` |
| `MONGOLAKE_DATA` | `".mongolake"` |
| `MONGOLAKE_SHUTDOWN_TIMEOUT` | `30000` |
| `MONGOLAKE_E2E_URL` | `"http://localhost:8787"` |
