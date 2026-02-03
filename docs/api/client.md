# Client API Reference

The MongoLake Client API provides a MongoDB-compatible interface for interacting with your data. This document covers the core classes and methods for database operations.

## Table of Contents

- [MongoLake Class](#mongolake-class)
- [Database Class](#database-class)
- [Collection Class](#collection-class)
- [FindCursor Class](#findcursor-class)
- [AggregationCursor Class](#aggregationcursor-class)
- [Helper Functions](#helper-functions)

---

## MongoLake Class

The main entry point for MongoLake. Creates a client instance connected to your storage backend.

### Constructor

```typescript
new MongoLake(config?: MongoLakeConfig)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `config` | `MongoLakeConfig` | Optional configuration object |

**Example:**

```typescript
import { MongoLake } from 'mongolake';

// Local development (uses .mongolake directory)
const lake = new MongoLake({ local: '.mongolake' });

// Cloudflare Workers with R2
const lake = new MongoLake({ bucket: env.R2_BUCKET });

// S3-compatible storage
const lake = new MongoLake({
  endpoint: 'https://s3.amazonaws.com',
  accessKeyId: 'YOUR_ACCESS_KEY',
  secretAccessKey: 'YOUR_SECRET_KEY',
  bucketName: 'my-bucket',
});
```

### Methods

#### `db(name?: string): Database`

Get a database instance by name.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `string` | Optional database name (defaults to 'default') |

**Returns:** `Database` instance

**Throws:** `ValidationError` if the database name contains invalid characters (path traversal prevention)

**Example:**

```typescript
const myDb = lake.db('myapp');
const defaultDb = lake.db(); // Uses 'default' database
```

---

#### `listDatabases(): Promise<string[]>`

List all databases in the storage backend.

**Returns:** Promise resolving to an array of database names

**Example:**

```typescript
const databases = await lake.listDatabases();
console.log(databases); // ['myapp', 'analytics', 'logs']
```

---

#### `dropDatabase(name: string): Promise<void>`

Drop a database and all its collections.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `string` | Database name to drop |

**Throws:** `ValidationError` if the database name is invalid

**Example:**

```typescript
await lake.dropDatabase('old-database');
```

---

#### `close(): Promise<void>`

Close the client and cleanup resources.

**Example:**

```typescript
await lake.close();
```

---

## Database Class

Represents a database containing collections.

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `name` | `string` | The database name |

### Methods

#### `collection<T>(name: string): Collection<T>`

Get a collection instance by name.

**Type Parameters:**

| Parameter | Description |
|-----------|-------------|
| `T` | Document type (extends `Document`) |

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `string` | Collection name |

**Returns:** `Collection<T>` instance

**Throws:** `ValidationError` if the collection name is invalid

**Example:**

```typescript
interface User extends Document {
  name: string;
  email: string;
  age: number;
}

const users = db.collection<User>('users');
```

---

#### `listCollections(): Promise<string[]>`

List all collections in the database.

**Returns:** Promise resolving to an array of collection names

**Example:**

```typescript
const collections = await db.listCollections();
console.log(collections); // ['users', 'posts', 'comments']
```

---

#### `createCollection<T>(name: string, options?: { schema?: CollectionSchema }): Promise<Collection<T>>`

Create a new collection with optional schema configuration.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `string` | Collection name |
| `options.schema` | `CollectionSchema` | Optional schema configuration for column promotion |

**Returns:** Promise resolving to the `Collection<T>` instance

**Example:**

```typescript
const users = await db.createCollection('users', {
  schema: {
    columns: {
      email: 'string',
      age: 'int32',
    },
  },
});
```

---

#### `dropCollection(name: string): Promise<boolean>`

Drop a collection and all its data.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `string` | Collection name to drop |

**Returns:** Promise resolving to `true` if the collection was dropped, `false` if it didn't exist

**Example:**

```typescript
const wasDropped = await db.dropCollection('old-collection');
```

---

## Collection Class

Represents a collection of documents. Provides MongoDB-compatible CRUD operations.

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `name` | `string` | The collection name |

### Write Operations

#### `insertOne(doc: T): Promise<InsertOneResult>`

Insert a single document into the collection.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `doc` | `T` | Document to insert |

**Returns:** `InsertOneResult`

| Property | Type | Description |
|----------|------|-------------|
| `acknowledged` | `boolean` | Always `true` |
| `insertedId` | `string \| ObjectId` | The `_id` of the inserted document |

**Example:**

```typescript
const result = await users.insertOne({
  name: 'Alice',
  email: 'alice@example.com',
  age: 30,
});
console.log(result.insertedId); // Auto-generated UUID or provided _id
```

---

#### `insertMany(docs: T[]): Promise<InsertManyResult>`

Insert multiple documents into the collection.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `docs` | `T[]` | Array of documents to insert |

**Returns:** `InsertManyResult`

| Property | Type | Description |
|----------|------|-------------|
| `acknowledged` | `boolean` | Always `true` |
| `insertedCount` | `number` | Number of documents inserted |
| `insertedIds` | `{ [key: number]: string \| ObjectId }` | Map of index to inserted `_id` |

**Example:**

```typescript
const result = await users.insertMany([
  { name: 'Bob', email: 'bob@example.com', age: 25 },
  { name: 'Carol', email: 'carol@example.com', age: 35 },
]);
console.log(result.insertedCount); // 2
```

---

#### `updateOne(filter: Filter<T>, update: Update<T>, options?: UpdateOptions): Promise<UpdateResult>`

Update a single document matching the filter.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `filter` | `Filter<T>` | Query filter to match documents |
| `update` | `Update<T>` | Update operations to apply |
| `options.upsert` | `boolean` | Insert if no document matches (default: `false`) |

**Returns:** `UpdateResult`

| Property | Type | Description |
|----------|------|-------------|
| `acknowledged` | `boolean` | Always `true` |
| `matchedCount` | `number` | Number of documents matched |
| `modifiedCount` | `number` | Number of documents modified |
| `upsertedCount` | `number` | Number of documents upserted |
| `upsertedId` | `string \| ObjectId` | `_id` of upserted document (if any) |

**Example:**

```typescript
const result = await users.updateOne(
  { email: 'alice@example.com' },
  { $set: { age: 31 } }
);

// With upsert
const result = await users.updateOne(
  { email: 'new@example.com' },
  { $set: { name: 'New User', age: 20 } },
  { upsert: true }
);
```

---

#### `updateMany(filter: Filter<T>, update: Update<T>, options?: UpdateOptions): Promise<UpdateResult>`

Update all documents matching the filter.

**Parameters:** Same as `updateOne`

**Returns:** Same as `updateOne`

**Example:**

```typescript
const result = await users.updateMany(
  { age: { $lt: 18 } },
  { $set: { status: 'minor' } }
);
console.log(result.modifiedCount); // Number of users updated
```

---

#### `replaceOne(filter: Filter<T>, replacement: T, options?: UpdateOptions): Promise<UpdateResult>`

Replace a single document matching the filter.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `filter` | `Filter<T>` | Query filter to match documents |
| `replacement` | `T` | New document to replace with (preserves `_id`) |
| `options.upsert` | `boolean` | Insert if no document matches |

**Returns:** Same as `updateOne`

**Example:**

```typescript
const result = await users.replaceOne(
  { email: 'alice@example.com' },
  { name: 'Alice Smith', email: 'alice@example.com', age: 31, verified: true }
);
```

---

#### `deleteOne(filter: Filter<T>, options?: DeleteOptions): Promise<DeleteResult>`

Delete a single document matching the filter.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `filter` | `Filter<T>` | Query filter to match documents |
| `options` | `DeleteOptions` | Optional delete options |

**Returns:** `DeleteResult`

| Property | Type | Description |
|----------|------|-------------|
| `acknowledged` | `boolean` | Always `true` |
| `deletedCount` | `number` | Number of documents deleted (0 or 1) |

**Example:**

```typescript
const result = await users.deleteOne({ email: 'alice@example.com' });
console.log(result.deletedCount); // 1
```

---

#### `deleteMany(filter: Filter<T>, options?: DeleteOptions): Promise<DeleteResult>`

Delete all documents matching the filter.

**Parameters:** Same as `deleteOne`

**Returns:** Same as `deleteOne`

**Example:**

```typescript
const result = await users.deleteMany({ status: 'inactive' });
console.log(result.deletedCount); // Number of users deleted
```

---

### Read Operations

#### `findOne(filter?: Filter<T>, options?: FindOptions): Promise<WithId<T> | null>`

Find a single document matching the filter.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `filter` | `Filter<T>` | Optional query filter |
| `options` | `FindOptions` | Optional find options |

**Returns:** Promise resolving to the matched document or `null`

**Example:**

```typescript
const user = await users.findOne({ email: 'alice@example.com' });
if (user) {
  console.log(user.name);
}
```

---

#### `find(filter?: Filter<T>, options?: FindOptions): FindCursor<T>`

Find documents matching the filter. Returns a cursor for lazy evaluation.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `filter` | `Filter<T>` | Optional query filter |
| `options` | `FindOptions` | Optional find options |

**Returns:** `FindCursor<T>` for chaining operations

**Example:**

```typescript
const cursor = users.find({ age: { $gte: 18 } })
  .sort({ name: 1 })
  .limit(10);

const adults = await cursor.toArray();
```

---

#### `countDocuments(filter?: Filter<T>): Promise<number>`

Count documents matching the filter.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `filter` | `Filter<T>` | Optional query filter |

**Returns:** Promise resolving to the count

**Example:**

```typescript
const count = await users.countDocuments({ status: 'active' });
```

---

#### `estimatedDocumentCount(): Promise<number>`

Get an estimated count of all documents (faster than `countDocuments`).

**Returns:** Promise resolving to the estimated count

**Example:**

```typescript
const estimate = await users.estimatedDocumentCount();
```

---

#### `distinct<K>(field: K, filter?: Filter<T>): Promise<T[K][]>`

Get distinct values for a field.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `field` | `K` | Field name to get distinct values for |
| `filter` | `Filter<T>` | Optional query filter |

**Returns:** Promise resolving to array of distinct values

**Example:**

```typescript
const cities = await users.distinct('city');
const activeCities = await users.distinct('city', { status: 'active' });
```

---

#### `aggregate<R>(pipeline: AggregationStage[], options?: AggregateOptions): AggregationCursor<R>`

Run an aggregation pipeline.

**Type Parameters:**

| Parameter | Description |
|-----------|-------------|
| `R` | Result document type |

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `pipeline` | `AggregationStage[]` | Array of aggregation stages |
| `options` | `AggregateOptions` | Optional aggregation options |

**Returns:** `AggregationCursor<R>` for lazy evaluation

**Example:**

```typescript
const results = await users.aggregate([
  { $match: { status: 'active' } },
  { $group: { _id: '$city', count: { $sum: 1 } } },
  { $sort: { count: -1 } },
]).toArray();
```

---

### Index Operations

#### `createIndex(spec: IndexSpec, options?: IndexOptions): Promise<string>`

Create an index on the collection.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `spec` | `IndexSpec` | Index specification (e.g., `{ age: 1 }`) |
| `options.name` | `string` | Custom index name |
| `options.unique` | `boolean` | Enforce unique values |
| `options.sparse` | `boolean` | Only index documents with the field |

**Returns:** Promise resolving to the index name

**Example:**

```typescript
// Simple ascending index
await users.createIndex({ age: 1 });

// Unique index with custom name
await users.createIndex({ email: 1 }, { unique: true, name: 'email_unique' });
```

---

#### `createIndexes(specs: Array<{ key: IndexSpec; options?: IndexOptions }>): Promise<string[]>`

Create multiple indexes at once.

**Returns:** Promise resolving to array of index names

**Example:**

```typescript
await users.createIndexes([
  { key: { email: 1 }, options: { unique: true } },
  { key: { age: 1 } },
  { key: { city: 1 } },
]);
```

---

#### `dropIndex(name: string): Promise<void>`

Drop an index by name.

**Example:**

```typescript
await users.dropIndex('age_1');
```

---

#### `listIndexes(): Promise<Array<{ name: string; key: IndexSpec }>>`

List all indexes on the collection.

**Returns:** Promise resolving to array of index information

**Example:**

```typescript
const indexes = await users.listIndexes();
for (const idx of indexes) {
  console.log(`${idx.name}: ${JSON.stringify(idx.key)}`);
}
```

---

## FindCursor Class

A cursor for lazy evaluation of find operations. Supports method chaining.

### Methods

#### `sort(spec: { [key: string]: 1 | -1 }): this`

Set sort order for results.

**Example:**

```typescript
cursor.sort({ name: 1, age: -1 }); // Sort by name ascending, then age descending
```

---

#### `limit(n: number): this`

Limit the number of results.

**Example:**

```typescript
cursor.limit(10); // Return at most 10 documents
```

---

#### `skip(n: number): this`

Skip a number of results (for pagination).

**Example:**

```typescript
cursor.skip(20).limit(10); // Page 3 with 10 items per page
```

---

#### `project(spec: { [key: string]: 0 | 1 }): this`

Set field projection.

**Example:**

```typescript
cursor.project({ name: 1, email: 1 }); // Only return name and email fields
```

---

#### `toArray(): Promise<WithId<T>[]>`

Execute the query and return all results as an array.

**Example:**

```typescript
const users = await cursor.toArray();
```

---

#### `forEach(fn: (doc: WithId<T>) => void): Promise<void>`

Iterate over each document.

**Example:**

```typescript
await cursor.forEach(user => {
  console.log(user.name);
});
```

---

#### `map<R>(fn: (doc: WithId<T>) => R): Promise<R[]>`

Map results to a new array.

**Example:**

```typescript
const names = await cursor.map(user => user.name);
```

---

#### `hasNext(): Promise<boolean>`

Check if there are more results.

---

#### `next(): Promise<WithId<T> | null>`

Get the next document.

---

#### `[Symbol.asyncIterator](): AsyncIterableIterator<WithId<T>>`

Async iteration support.

**Example:**

```typescript
for await (const user of cursor) {
  console.log(user.name);
}
```

---

## AggregationCursor Class

A cursor for lazy evaluation of aggregation pipelines.

### Methods

#### `toArray(): Promise<T[]>`

Execute the pipeline and return all results.

**Example:**

```typescript
const results = await pipeline.toArray();
```

---

#### `[Symbol.asyncIterator](): AsyncIterableIterator<T>`

Async iteration support.

---

## Helper Functions

### `db(name?: string): Database`

Convenience function to get a database using the default client.

**Example:**

```typescript
import { db } from 'mongolake';

const users = db('myapp').collection('users');
await users.insertOne({ name: 'Alice' });
```

---

## Error Conditions

### ValidationError

Thrown when database or collection names contain invalid characters. This prevents path traversal attacks.

**Invalid characters:** `/`, `\`, `..`, null bytes

```typescript
try {
  const db = lake.db('../etc/passwd');
} catch (e) {
  if (e instanceof ValidationError) {
    console.error('Invalid database name');
  }
}
```

### Document Errors

- Missing `_id` field after insert: A UUID is automatically generated
- Corrupted Parquet files: Throws by default, use `skipCorruptedFiles: true` option to continue

---

## See Also

- [Types Reference](./types.md) - TypeScript types and interfaces
- [Storage Reference](./storage.md) - Storage backend interface
- [Worker Reference](./worker.md) - Worker and Durable Object exports
