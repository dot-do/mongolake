# Worker and Durable Object Reference

MongoLake provides a Cloudflare Workers-compatible HTTP API and Durable Object for distributed data storage. This document covers the worker handler and ShardDO exports.

## Table of Contents

- [Worker Exports](#worker-exports)
- [MongoLakeWorker Class](#mongolakeworker-class)
- [HTTP API Endpoints](#http-api-endpoints)
- [ShardDO Class](#sharddo-class)
- [Environment Configuration](#environment-configuration)
- [Deployment Configuration](#deployment-configuration)

---

## Worker Exports

The main entry point exports everything needed for Cloudflare Workers deployment.

```typescript
// Named exports
export { MongoLakeWorker, MongoLakeEnv, RequestContext } from 'mongolake';
export { ShardDO } from 'mongolake';

// Default export (for wrangler)
export default {
  fetch: (request: Request, env: MongoLakeEnv) => Response,
  ShardDO,
};
```

**Example (worker entry point):**

```typescript
import mongolake from 'mongolake';

export default mongolake;
export const ShardDO = mongolake.ShardDO;
```

---

## MongoLakeWorker Class

HTTP request handler for the MongoLake REST API.

### Constructor

```typescript
new MongoLakeWorker()
```

### Methods

#### `fetch(request: Request, env: MongoLakeEnv): Promise<Response>`

Handle an incoming HTTP request.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `request` | `Request` | Incoming HTTP request |
| `env` | `MongoLakeEnv` | Worker environment bindings |

**Returns:** Promise resolving to HTTP `Response`

**Example:**

```typescript
const worker = new MongoLakeWorker();
const response = await worker.fetch(request, env);
```

---

## HTTP API Endpoints

### Health Check

```
GET /health
```

Returns service health status.

**Response:**

```json
{
  "status": "ok",
  "version": "0.1.0"
}
```

---

### Find Documents

```
GET /api/{database}/{collection}
```

Query documents from a collection.

**Query Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `filter` | `string` | JSON-encoded MongoDB filter |
| `projection` | `string` | JSON-encoded field projection |
| `sort` | `string` | JSON-encoded sort specification |
| `limit` | `number` | Maximum documents to return |
| `skip` | `number` | Number of documents to skip |
| `afterToken` | `string` | Read token for read-your-writes consistency |

**Response:**

```json
{
  "documents": [
    { "_id": "abc123", "name": "Alice", "age": 30 }
  ],
  "filter": {},
  "limit": 10
}
```

**Example:**

```bash
curl "https://api.example.com/api/mydb/users?filter=%7B%22age%22%3A%7B%22%24gte%22%3A18%7D%7D&limit=10"
```

---

### Insert Document

```
POST /api/{database}/{collection}
```

Insert a single document.

**Request Body:**

```json
{
  "name": "Alice",
  "email": "alice@example.com"
}
```

**Response (201 Created):**

```json
{
  "acknowledged": true,
  "insertedId": "507f1f77bcf86cd799439011",
  "readToken": "shard-abc:42"
}
```

**Error Conditions:**

| Status | Error | Cause |
|--------|-------|-------|
| 400 | Missing request body | Empty POST body |
| 400 | Invalid JSON in request body | Malformed JSON |
| 409 | Duplicate key error | Document with same `_id` exists |

---

### Update Document

```
PATCH /api/{database}/{collection}/{documentId}
```

Update a document by ID.

**Query Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `upsert` | `boolean` | Create document if not found |

**Request Body (Update Operators):**

```json
{
  "$set": { "verified": true },
  "$inc": { "loginCount": 1 }
}
```

**Valid Update Operators:**

- `$set` - Set field values
- `$unset` - Remove fields
- `$inc` - Increment numeric fields
- `$mul` - Multiply numeric fields
- `$min` - Set to minimum value
- `$max` - Set to maximum value
- `$rename` - Rename fields
- `$currentDate` - Set to current date/time
- `$push` - Add to array
- `$pull` - Remove from array
- `$addToSet` - Add unique to array
- `$pop` - Remove first/last from array

**Response:**

```json
{
  "acknowledged": true,
  "matchedCount": 1,
  "modifiedCount": 1,
  "readToken": "shard-abc:43"
}
```

**Error Conditions:**

| Status | Error | Cause |
|--------|-------|-------|
| 400 | Missing document ID | No ID in path |
| 400 | Invalid update operator | Unknown operator like `$foo` |
| 404 | Document not found | No matching document (without upsert) |

---

### Delete Document

```
DELETE /api/{database}/{collection}/{documentId}
```

Delete a document by ID.

**Response:**

```json
{
  "acknowledged": true,
  "deletedCount": 1,
  "readToken": "shard-abc:44"
}
```

**Note:** Returns 200 even when document doesn't exist (with `deletedCount: 0`)

---

### Aggregation Pipeline

```
POST /api/{database}/{collection}/aggregate
```

Execute an aggregation pipeline.

**Request Body:**

```json
{
  "pipeline": [
    { "$match": { "status": "active" } },
    { "$group": { "_id": "$category", "count": { "$sum": 1 } } },
    { "$sort": { "count": -1 } },
    { "$limit": 10 }
  ]
}
```

**Valid Aggregation Stages:**

- `$match` - Filter documents
- `$group` - Group and aggregate
- `$sort` - Sort documents
- `$limit` - Limit results
- `$skip` - Skip documents
- `$project` - Reshape documents
- `$unwind` - Deconstruct arrays
- `$lookup` - Join collections
- `$count` - Count documents
- `$addFields` / `$set` - Add fields
- `$unset` - Remove fields

**Response:**

```json
{
  "documents": [
    { "_id": "electronics", "count": 150 },
    { "_id": "clothing", "count": 89 }
  ]
}
```

**Error Conditions:**

| Status | Error | Cause |
|--------|-------|-------|
| 400 | Missing pipeline in request body | No `pipeline` field |
| 400 | Empty pipeline array | Empty array |
| 400 | Invalid aggregation stage | Unknown stage like `$foo` |

---

### Bulk Insert

```
POST /api/{database}/{collection}/bulk-insert
```

Insert multiple documents in a single request.

**Request Body:**

```json
{
  "documents": [
    { "name": "Alice", "age": 30 },
    { "name": "Bob", "age": 25 }
  ],
  "ordered": true
}
```

**Response (201 Created):**

```json
{
  "acknowledged": true,
  "insertedCount": 2,
  "insertedIds": {
    "0": "507f1f77bcf86cd799439011",
    "1": "507f1f77bcf86cd799439012"
  }
}
```

**Error Conditions:**

| Status | Error | Cause |
|--------|-------|-------|
| 400 | Missing documents in request body | No `documents` field |
| 400 | Empty documents array | Empty array |
| 409 | Duplicate key error | Duplicate `_id` in batch or storage |

---

### Wire Protocol (WebSocket)

```
GET /wire
Upgrade: websocket
```

Establish a WebSocket connection for MongoDB wire protocol.

**Headers:**

| Header | Value | Description |
|--------|-------|-------------|
| `Upgrade` | `websocket` | Required for upgrade |
| `Sec-WebSocket-Protocol` | `mongodb` | Optional protocol hint |

**Response:** 101 Switching Protocols

**Error Conditions:**

| Status | Error | Cause |
|--------|-------|-------|
| 426 | Upgrade required: WebSocket | Missing Upgrade header |
| 401 | Authorization required | Auth required but no credentials |

---

## ShardDO Class

Durable Object for write coordination and data storage.

### Constructor

```typescript
new ShardDO(state: DurableObjectState, env: ShardDOEnv)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `state` | `DurableObjectState` | Durable Object state from runtime |
| `env` | `ShardDOEnv` | Environment bindings |

### ShardDOEnv

```typescript
interface ShardDOEnv {
  DATA_BUCKET: R2Bucket;           // R2 bucket for data storage
  SHARD_DO: DurableObjectNamespace; // Self-reference for routing
}
```

---

### Write Operations

#### `write(op: WriteOperation): Promise<WriteResult>`

Execute a write operation (insert, update, or delete).

**WriteOperation:**

```typescript
interface WriteOperation {
  collection: string;              // Collection name
  op: 'insert' | 'update' | 'delete';
  document?: Record<string, unknown>;  // For insert
  filter?: Record<string, unknown>;    // For update/delete
  update?: Record<string, unknown>;    // For update
}
```

**WriteResult:**

```typescript
interface WriteResult {
  acknowledged: boolean;
  insertedId?: string;      // For insert
  lsn: number;              // Log sequence number
  readToken: string;        // For read-your-writes consistency
}
```

**Example:**

```typescript
const result = await shard.write({
  collection: 'users',
  op: 'insert',
  document: { _id: 'user1', name: 'Alice' }
});
console.log(result.readToken); // 'shard-abc:1'
```

---

### Query Operations

#### `find(collection: string, filter: Record<string, unknown>, options?: FindOptions): Promise<Record<string, unknown>[]>`

Find documents matching a filter.

**FindOptions:**

```typescript
interface FindOptions {
  projection?: Record<string, 0 | 1>;
  sort?: Record<string, 1 | -1>;
  limit?: number;
  skip?: number;
  afterToken?: string;  // For read-your-writes
}
```

**Example:**

```typescript
const docs = await shard.find('users', { age: { $gte: 18 } }, {
  sort: { name: 1 },
  limit: 10
});
```

---

#### `findOne(collection: string, filter: Record<string, unknown>, options?: FindOptions): Promise<Record<string, unknown> | null>`

Find a single document.

**Example:**

```typescript
const user = await shard.findOne('users', { _id: 'user1' });
```

---

### Buffer Management

#### `flush(): Promise<void>`

Flush buffered writes to R2 storage.

**Example:**

```typescript
await shard.flush();
```

---

#### `checkpoint(): Promise<void>`

Remove flushed WAL entries from storage.

**Example:**

```typescript
await shard.flush();
await shard.checkpoint();
```

---

### Configuration

#### `configure(config: ShardConfig): Promise<void>`

Update shard configuration.

**ShardConfig:**

```typescript
interface ShardConfig {
  flushThresholdBytes?: number;   // Flush when buffer exceeds (default: 1MB)
  flushThresholdDocs?: number;    // Flush when doc count exceeds (default: 1000)
  compactionMinAge?: number;      // Min file age for compaction (ms)
  compactionBatchSize?: number;   // Files per compaction cycle
}
```

---

### Status

#### `getBufferSize(): Promise<number>`

Get current buffer size in bytes.

---

#### `getBufferDocCount(): Promise<number>`

Get current number of buffered documents.

---

#### `getFlushedLSN(): Promise<number>`

Get the last flushed log sequence number.

---

#### `getCurrentReadToken(): Promise<string>`

Get a read token for the current state.

---

#### `getManifest(collection: string): Promise<CollectionManifest>`

Get the manifest for a collection.

**CollectionManifest:**

```typescript
interface CollectionManifest {
  collection: string;
  files: FileMetadata[];
  updatedAt: number;
}
```

---

### HTTP Interface

ShardDO exposes an HTTP interface for internal routing.

#### `POST /write`

Execute a write operation.

**Request Body:** `WriteOperation`

**Response:** `WriteResult`

---

#### `POST /find`

Find documents.

**Request Body:**

```json
{
  "collection": "users",
  "filter": { "age": { "$gte": 18 } },
  "limit": 10
}
```

**Response:**

```json
{
  "documents": [...]
}
```

---

#### `POST /findOne`

Find a single document.

**Request Body:**

```json
{
  "collection": "users",
  "filter": { "_id": "user1" }
}
```

**Response:**

```json
{
  "document": { "_id": "user1", "name": "Alice" }
}
```

---

#### `POST /flush`

Trigger a manual flush.

**Response:**

```json
{
  "success": true
}
```

---

#### `GET /status`

Get shard status.

**Response:**

```json
{
  "bufferSize": 12345,
  "bufferDocCount": 42,
  "flushedLSN": 100,
  "currentLSN": 142
}
```

---

## Environment Configuration

### MongoLakeEnv

Environment bindings for the worker.

```typescript
interface MongoLakeEnv {
  BUCKET: R2Bucket;                    // Required: R2 bucket for data
  RPC_NAMESPACE: DurableObjectNamespace; // Required: ShardDO namespace
  OAUTH_SECRET?: string;               // JWT signing secret
  REQUIRE_AUTH?: boolean;              // Enable authentication
  ENVIRONMENT?: string;                // 'production' hides error details
  ALLOWED_ORIGINS?: string;            // CORS allowed origins
}
```

### RequestContext

Context passed to request handlers.

```typescript
interface RequestContext {
  database: string;           // Database name from URL
  collection: string;         // Collection name from URL
  documentId?: string;        // Document ID from URL (if present)
  user?: UserContext;         // Authenticated user (if any)
  requestId: string;          // Unique request ID (UUID)
  timestamp: Date;            // Request timestamp
}
```

### UserContext

```typescript
interface UserContext {
  userId?: string;
  claims?: Record<string, unknown>;
}
```

---

## Deployment Configuration

### wrangler.toml

```toml
name = "mongolake"
main = "src/index.ts"
compatibility_date = "2024-01-01"

[[r2_buckets]]
binding = "BUCKET"
bucket_name = "mongolake-data"

[[durable_objects.bindings]]
name = "RPC_NAMESPACE"
class_name = "ShardDO"

[[migrations]]
tag = "v1"
new_classes = ["ShardDO"]

[vars]
ENVIRONMENT = "production"
REQUIRE_AUTH = "true"
```

### Example Worker

```typescript
import mongolake, { ShardDO } from 'mongolake';

export default mongolake;
export { ShardDO };
```

---

## CORS Configuration

The worker includes CORS support with configurable origins.

**Default Headers:**

```
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, PATCH, DELETE, OPTIONS
Access-Control-Allow-Headers: Content-Type, Authorization, X-API-Key
Access-Control-Max-Age: 86400
```

**Custom Origins:**

Set `ALLOWED_ORIGINS` in your environment to restrict origins:

```toml
[vars]
ALLOWED_ORIGINS = "https://app.example.com"
```

---

## Authentication

When `REQUIRE_AUTH` is enabled:

1. All requests (except `/health`) require authentication
2. Supported authentication methods:
   - `Authorization: Bearer <token>` header
   - `X-API-Key: <key>` header

**Error Response (401):**

```json
{
  "error": "Authorization header required"
}
```

---

## Error Handling

All error responses include:

```json
{
  "error": "Error message"
}
```

**Headers:**

```
Content-Type: application/json
X-Request-Id: <uuid>
```

In production (`ENVIRONMENT=production`), 500 errors return a generic message:

```json
{
  "error": "Internal server error"
}
```

---

## See Also

- [Client Reference](./client.md) - Client API documentation
- [Types Reference](./types.md) - TypeScript types and interfaces
- [Storage Reference](./storage.md) - Storage backend interface
