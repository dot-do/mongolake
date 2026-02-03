# Local Development Guide

This guide covers setting up MongoLake for local development using the CLI dev server.

## Quick Start

Start a local development server with a single command:

```bash
npx mongolake dev
```

This starts:
- MongoDB wire protocol server on port 27017
- REST API server on port 3000

## CLI Dev Server

### Installation

```bash
# Install globally
npm install -g mongolake

# Or use npx
npx mongolake dev
```

### Command Options

```bash
mongolake dev [options]

Options:
  --port <number>    REST API port (default: 3456)
  --host <string>    Host to bind to (default: localhost)
  --path <string>    Local storage path (default: .mongolake)
  --watch            Enable hot reload on file changes
  --verbose          Enable verbose logging
```

### Examples

```bash
# Start with default settings
mongolake dev

# Start on custom port
mongolake dev --port 8080

# Start with hot reload
mongolake dev --watch

# Start with custom storage path
mongolake dev --path ./data
```

## Local .mongolake Storage

By default, MongoLake stores data locally in a `.mongolake` directory:

```
.mongolake/
  r2/                    # Simulated R2 bucket storage
    myapp/
      users.parquet      # Collection data
      users_001.parquet  # Delta files (pending compaction)
      orders.parquet
  d1/                    # SQLite databases (WAL)
  kv/                    # Key-value storage (metadata)
```

### Storage Structure

| Directory | Purpose |
|-----------|---------|
| `.mongolake/r2/` | Parquet data files (simulates R2) |
| `.mongolake/d1/` | SQLite databases for WAL |
| `.mongolake/kv/` | Metadata and indexes |

### Querying Local Parquet Files

Use DuckDB to query local data directly:

```sql
-- Query all users
SELECT * FROM '.mongolake/r2/myapp/users*.parquet';

-- Query with filtering
SELECT * FROM '.mongolake/r2/myapp/users*.parquet'
WHERE status = 'active'
ORDER BY createdAt DESC
LIMIT 10;
```

## Wire Protocol Connection

Connect to the local server using MongoDB tools:

### mongosh

```bash
# Start the dev server first
mongolake dev

# Connect with mongosh
mongosh mongodb://localhost:27017/myapp
```

```javascript
// In mongosh
db.users.insertOne({ name: 'Alice', email: 'alice@example.com' })
db.users.find({ name: 'Alice' })
db.users.aggregate([{ $match: { status: 'active' } }])
```

### MongoDB Compass

1. Start the dev server: `mongolake dev`
2. Open MongoDB Compass
3. Connect to: `mongodb://localhost:27017`
4. Select your database from the sidebar

### Node.js Driver

```typescript
import { MongoClient } from 'mongodb';

const client = new MongoClient('mongodb://localhost:27017');
await client.connect();

const db = client.db('myapp');
const users = db.collection('users');

await users.insertOne({ name: 'Alice' });
const user = await users.findOne({ name: 'Alice' });
```

## Programmatic API

Use the SDK directly without starting a server:

```typescript
import { db } from 'mongolake';

// Uses local .mongolake/ folder by default
const users = db('myapp').collection('users');

// Insert
await users.insertOne({
  name: 'Alice',
  email: 'alice@example.com'
});

// Query
const user = await users.findOne({ email: 'alice@example.com' });

// Update
await users.updateOne(
  { email: 'alice@example.com' },
  { $set: { verified: true } }
);

// Aggregate
const results = await users.aggregate([
  { $match: { verified: true } },
  { $group: { _id: '$department', count: { $sum: 1 } } }
]);
```

### Configuration Options

```typescript
import { MongoLake } from 'mongolake';

const lake = new MongoLake({
  // Storage path (default: .mongolake)
  local: './my-data',

  // Database name
  database: 'myapp',

  // Schema configuration
  schema: {
    users: {
      columns: {
        _id: 'string',
        email: 'string',
        createdAt: 'timestamp',
      },
      autoPromote: { threshold: 0.9 },
    },
  },
});

const users = lake.collection('users');
```

## Development Server Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Dev Server                            │
├─────────────────────────────────────────────────────────┤
│                                                         │
│   ┌─────────────┐        ┌─────────────┐               │
│   │ Wire Proto  │        │  REST API   │               │
│   │ :27017      │        │  :3456      │               │
│   └──────┬──────┘        └──────┬──────┘               │
│          │                      │                       │
│          └──────────┬───────────┘                       │
│                     │                                   │
│              ┌──────▼──────┐                            │
│              │  Miniflare   │ (Cloudflare Workers sim)  │
│              └──────┬──────┘                            │
│                     │                                   │
│   ┌─────────────────┼─────────────────┐                 │
│   │                 │                 │                 │
│   ▼                 ▼                 ▼                 │
│ .mongolake/      .mongolake/      .mongolake/          │
│   r2/              d1/              kv/                 │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

## Hot Reload

Enable hot reload to automatically restart the server when source files change:

```bash
mongolake dev --watch
```

### Watched Patterns

By default, these patterns are watched:
- `src/**/*.ts`
- `wrangler.toml`

### Ignored Patterns

These patterns are always ignored:
- `**/node_modules/**`
- `**/dist/**`
- `**/.git/**`

## Environment Variables

Create a `.env` file for local configuration:

```bash
# .env
ENVIRONMENT=development
DEBUG=true
MONGOLAKE_PATH=.mongolake
```

The dev server automatically loads `.env` files.

## Testing with the Dev Server

### Integration Tests

```typescript
import { startDevServer } from 'mongolake/cli/dev';

describe('Integration Tests', () => {
  let server;

  beforeAll(async () => {
    server = await startDevServer({
      port: 8888,
      path: '.mongolake-test',
    });
  });

  afterAll(async () => {
    await server.stop();
  });

  it('should insert and query documents', async () => {
    const response = await fetch('http://localhost:8888/api/testdb/items', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'Test' }),
    });
    expect(response.status).toBe(201);
  });
});
```

## Troubleshooting

### Port Already in Use

```bash
# Find process using the port
lsof -i :3456

# Kill the process
kill -9 <PID>

# Or use a different port
mongolake dev --port 8080
```

### Storage Permission Issues

Ensure write permissions for the storage directory:

```bash
chmod -R 755 .mongolake
```

### Clearing Local Data

To reset local development data:

```bash
rm -rf .mongolake
```

### Debug Logging

Enable verbose output:

```bash
mongolake dev --verbose
```
