# MongoLake

**MongoDB re-imagined for the lakehouse era.**

[![npm version](https://badge.fury.io/js/mongolake.svg)](https://www.npmjs.com/package/mongolake)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![codecov](https://codecov.io/gh/dot-do/mongolake/graph/badge.svg)](https://codecov.io/gh/dot-do/mongolake)

```bash
npm install mongolake
```

## What is MongoLake?

MongoLake is a MongoDB-compatible database that stores data as Parquet files. It combines the developer experience of MongoDB with the analytical power of the lakehouse.

**Core Features:**
- **MongoDB API** - Familiar CRUD operations (insertOne, find, update, delete)
- **Local-first development** - File-based storage with `.mongolake/` directory
- **Lakehouse-native** - Data stored as queryable Parquet files
- **Cloudflare Workers** - Deploy globally on Workers + R2
- **Variant encoding** - Automatic handling of flexible document schemas
- **Multiple storage backends** - Local filesystem, Cloudflare R2, S3-compatible storage

**New in this release:**
- Wire Protocol server (mongosh, Compass compatibility)
- CLI dev command for local development
- B-tree indexing with automatic _id index
- Enhanced aggregation pipeline ($unwind, $lookup)
- Change streams with real-time watch() support
- Iceberg metadata generation

## Quick Start

### Local Development

Start a local development server with the CLI:

```bash
npx mongolake dev
```

This starts a server with:
- MongoDB wire protocol on port 27017 (use with mongosh)
- REST API on port 3000

Or use the programmatic API:

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

### Deploy to Cloudflare Workers

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

### Branching & Time Travel (Coming Soon)

Branching and time travel features are planned but not yet implemented. They will enable:

```typescript
// Create a branch (not yet implemented)
await db('myapp').branch('feature-x');

// Query at a point in time (not yet implemented)
const historical = db('myapp', { asOf: '2024-01-15T00:00:00Z' });
const oldData = await historical.collection('users').find({});
```

### Wire Protocol

Connect with mongosh or MongoDB Compass:

```bash
# Start the server
npx mongolake dev

# In another terminal
mongosh mongodb://localhost:27017/myapp
```

```javascript
// In mongosh
db.users.insertOne({ name: 'Alice', email: 'alice@example.com' })
db.users.find({ name: 'Alice' })
db.users.aggregate([{ $match: { status: 'active' } }])
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

## Implementation Status

### Implemented (✅)

- **Client API** - Full CRUD operations (`insertOne`, `insertMany`, `findOne`, `find`, `updateOne`, `updateMany`, `deleteOne`, `deleteMany`)
- **Parquet I/O** - Read/write with hyparquet
- **Variant encoding** - Schema-less fields as Parquet variant
- **Storage backends** - Local, R2, S3 (optional), Memory
- **Worker & Durable Object** - Cloudflare deployment
- **Wire Protocol Server** - TCP server compatible with mongosh
- **CLI dev command** - Local development server with REST API
- **B-tree Indexing** - Automatic _id index, range queries
- **Aggregation Pipeline** - `$match`, `$group`, `$project`, `$sort`, `$limit`, `$skip`, `$unwind`, `$lookup`
- **Change Streams** - Real-time `watch()` support
- **Iceberg Metadata** - Manifest generation in Avro format

### Partial Implementation (🚧)

- **Filter operators** - Most comparison/logical operators (`$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`, `$regex`, `$not`)
- **Update operators** - `$set`, `$unset`, `$inc` (basic)

### Not Yet Implemented (❌)

- **Branching & merging** - Git-like database versioning
- **Time travel** - Point-in-time queries
- **Full Iceberg catalog integration**

## Deploy to Cloudflare Workers

Deploy MongoLake as a Cloudflare Worker with Durable Object persistence:

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

The Worker provides an RPC interface for MongoLake operations, while Durable Objects handle Write-Ahead Logging (WAL) and buffering.

## License

MIT

## Links

- [Documentation](https://mongolake.com/docs)
- [GitHub](https://github.com/dot-do/mongolake)
- [Discord](https://discord.gg/mongolake)
