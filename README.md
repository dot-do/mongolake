# MongoLake

**MongoDB re-imagined for the lakehouse era.**

[![npm version](https://badge.fury.io/js/mongolake.svg)](https://www.npmjs.com/package/mongolake)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

```bash
npm install mongolake
```

## What is MongoLake?

MongoLake is a MongoDB-compatible database that stores data as Parquet files. It combines the developer experience of MongoDB with the analytical power of the lakehouse.

- **100% MongoDB API compatible** - mongosh, Compass, and all drivers just work
- **Local-first development** - SQLite-simple file-based storage
- **Git-like workflow** - Branch, push, pull, merge databases
- **Lakehouse-native** - Iceberg integration, schema evolution, time travel
- **Query anywhere** - DuckDB, Spark, Trino can read your data directly
- **Cloudflare-native** - Deploy globally on Workers + R2

## Quick Start

### Local Development

```typescript
import { db } from 'mongolake';

// Uses local .mongolake/ folder by default
const users = db('myapp').collection('users');

await users.insertOne({
  name: 'Alice',
  email: 'alice@example.com'
});

const user = await users.findOne({ email: 'alice@example.com' });
```

### With CLI

```bash
# Start local dev server with MongoDB wire protocol
mongolake dev

# Connect with mongosh
mongosh mongodb://localhost:27017/myapp

# Or use the built-in shell
mongolake shell myapp
```

### Deploy to Cloudflare

```bash
# Push local database to production
mongolake push --remote wss://your-worker.workers.dev

# Or deploy your own MongoLake worker
wrangler deploy
```

## Features

### MongoDB Compatible

Use the MongoDB API you know:

```typescript
// All the familiar operations
await users.insertMany([...]);
await users.find({ status: 'active' }).sort({ createdAt: -1 }).limit(10);
await users.updateOne({ _id }, { $set: { status: 'inactive' } });
await users.aggregate([
  { $match: { status: 'active' } },
  { $group: { _id: '$department', count: { $sum: 1 } } }
]);
```

### Lakehouse Storage

Your data is stored as Parquet files:

```
.mongolake/
  myapp/
    users.parquet           # Collection data
    users_001.parquet       # Delta (new writes)
    orders.parquet
    _iceberg/               # Optional Iceberg metadata
```

Query with DuckDB:

```sql
SELECT * FROM '.mongolake/myapp/users*.parquet'
WHERE status = 'active'
```

### Schema Configuration

Configure how documents are stored:

```typescript
const lake = new MongoLake({
  database: 'myapp',
  schema: {
    users: {
      // Promote fields to native Parquet columns
      columns: {
        _id: 'string',
        email: 'string',
        createdAt: 'timestamp',
        profile: { name: 'string', avatar: 'string' },
        tags: ['string'],
      },
      // Auto-promote fields in >90% of documents
      autoPromote: { threshold: 0.9 },
    },
  },
});
```

### Branching & Time Travel

```typescript
// Create a branch
await db('myapp').branch('feature-x');

// Work on the branch
const branch = db('myapp', { branch: 'feature-x' });
await branch.collection('users').insertOne({ name: 'Test' });

// Query at a point in time
const historical = db('myapp', { asOf: '2024-01-15T00:00:00Z' });
const oldData = await historical.collection('users').find({});

// Merge branch back
await db('myapp').merge('feature-x');
```

### Wire Protocol Proxy

Use mongosh, Compass, or any MongoDB driver:

```bash
# Start proxy with Cloudflare tunnel
mongolake proxy --tunnel

# Output:
#   Local:  mongodb://localhost:27017
#   Tunnel: mongodb://abc123.cfargotunnel.com

# Connect from anywhere
mongosh mongodb://abc123.cfargotunnel.com/myapp
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Clients                                  │
├──────────┬──────────┬──────────┬──────────┬─────────────────────┤
│ mongosh  │ Compass  │ Drivers  │   SDK    │  DuckDB / Spark     │
└────┬─────┴────┬─────┴────┬─────┴────┬─────┴──────────┬──────────┘
     │          │          │          │                 │
     └────────┬─┴──────────┘          │                 │
              │                       │                 │
     Wire Protocol (TCP)          RPC (WS)        Direct Parquet
              │                       │                 │
              ▼                       ▼                 │
    ┌─────────────────┐    ┌──────────────────┐        │
    │  mongolake CLI  │    │ Cloudflare Worker│        │
    │  (Bun binary)   │    │                  │        │
    └────────┬────────┘    └────────┬─────────┘        │
             │                      │                   │
             └──────────┬───────────┘                   │
                        │                               │
               ┌────────▼─────────┐                     │
               │  Durable Object  │                     │
               │  (WAL + Buffer)  │                     │
               └────────┬─────────┘                     │
                        │                               │
               ┌────────▼─────────┐                     │
               │       R2         │◄────────────────────┘
               │  Parquet + Iceberg                     │
               └──────────────────┘
```

## Configuration

### Environment Variables

```bash
# Cloudflare (for managed service)
R2_DATA_CATALOG_TOKEN=xxx  # Enables Iceberg mode

# Local development
MONGOLAKE_PATH=.mongolake  # Local storage path
```

### MongoLake Options

```typescript
const lake = new MongoLake({
  // Storage backend
  local: '.mongolake',              // Local filesystem
  // OR
  bucket: env.R2_BUCKET,            // Cloudflare R2
  // OR
  endpoint: 'https://s3.amazonaws.com',  // S3-compatible
  accessKeyId: '...',
  secretAccessKey: '...',

  // Database
  database: 'myapp',

  // Iceberg integration (auto-enabled if R2_DATA_CATALOG_TOKEN set)
  iceberg: {
    token: env.R2_DATA_CATALOG_TOKEN,
    catalog: 'my-catalog',
  },

  // Schema configuration
  schema: { /* ... */ },
});
```

## CLI Reference

```bash
mongolake dev [--port 27017] [--local .mongolake] [--tunnel]
  Start local development server

mongolake shell <database>
  Interactive shell

mongolake proxy [--remote <url>] [--tunnel [name]]
  Start wire protocol proxy

mongolake push [--remote <url>]
  Push local database to remote

mongolake pull [--remote <url>]
  Pull remote database to local

mongolake branch <name>
  Create a new branch

mongolake merge <branch>
  Merge branch into main

mongolake compact <database> [collection]
  Compact Parquet files

mongolake tunnel create <name>
  Create named Cloudflare tunnel

mongolake login
  Authenticate with mongolake.com
```

## Self-Hosting

Deploy your own MongoLake:

```typescript
// worker.ts
import { MongoLakeWorker } from 'mongolake/worker';
import { MongoLakeDO } from 'mongolake/do';

export default MongoLakeWorker;
export { MongoLakeDO };
```

```toml
# wrangler.toml
name = "my-mongolake"

[[r2_buckets]]
binding = "BUCKET"
bucket_name = "my-data"

[[durable_objects.bindings]]
name = "SHARD"
class_name = "MongoLakeDO"
```

## License

MIT

## Links

- [Documentation](https://mongolake.com/docs)
- [GitHub](https://github.com/dot-do/mongolake)
- [Discord](https://discord.gg/mongolake)
