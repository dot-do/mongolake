# Migration Guide: MongoDB to MongoLake

This comprehensive guide covers migrating from MongoDB to MongoLake, a MongoDB-compatible database that stores data as Parquet files for lakehouse integration.

## Table of Contents

1. [Feature Compatibility Matrix](#1-feature-compatibility-matrix)
2. [Connection String Changes](#2-connection-string-changes)
3. [Driver Compatibility](#3-driver-compatibility)
4. [Schema Considerations](#4-schema-considerations)
5. [Data Export from MongoDB](#5-data-export-from-mongodb)
6. [Data Import into MongoLake](#6-data-import-into-mongolake)
7. [Index Migration](#7-index-migration)
8. [Application Code Changes](#8-application-code-changes)
9. [Testing Strategy](#9-testing-strategy)
10. [Rollback Plan](#10-rollback-plan)
11. [Common Issues and Solutions](#11-common-issues-and-solutions)
12. [Performance Comparison Tips](#12-performance-comparison-tips)

---

## 1. Feature Compatibility Matrix

### Fully Supported Features

| Feature | MongoDB | MongoLake | Notes |
|---------|---------|-----------|-------|
| `insertOne` | Yes | Yes | Full compatibility |
| `insertMany` | Yes | Yes | Full compatibility |
| `findOne` | Yes | Yes | Full compatibility |
| `find` | Yes | Yes | Full compatibility |
| `updateOne` | Yes | Yes | Full compatibility |
| `updateMany` | Yes | Yes | Full compatibility |
| `replaceOne` | Yes | Yes | Full compatibility |
| `deleteOne` | Yes | Yes | Full compatibility |
| `deleteMany` | Yes | Yes | Full compatibility |
| `countDocuments` | Yes | Yes | Full compatibility |
| `estimatedDocumentCount` | Yes | Yes | Full compatibility |
| `distinct` | Yes | Yes | Full compatibility |
| `aggregate` | Yes | Yes | See supported stages below |
| `createIndex` | Yes | Yes | B-tree indexes |
| `dropIndex` | Yes | Yes | Full compatibility |
| `listIndexes` | Yes | Yes | Full compatibility |
| `watch` | Yes | Yes | Change streams support |

### Supported Aggregation Stages

| Stage | Supported | Notes |
|-------|-----------|-------|
| `$match` | Yes | Full filter support |
| `$group` | Yes | All accumulators |
| `$project` | Yes | Full projection support |
| `$sort` | Yes | Full sort support |
| `$limit` | Yes | Full support |
| `$skip` | Yes | Full support |
| `$unwind` | Yes | Including preserveNullAndEmptyArrays |
| `$lookup` | Yes | Including pipeline lookups |
| `$count` | Yes | Full support |
| `$addFields` | Yes | Full support |
| `$set` | Yes | Alias for $addFields |
| `$unset` | Yes | Full support |
| `$facet` | Yes | Full support |
| `$bucket` | Yes | Full support |

### Supported Query Operators

| Category | Operators | Status |
|----------|-----------|--------|
| Comparison | `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin` | Full support |
| Logical | `$and`, `$or`, `$nor`, `$not` | Full support |
| Element | `$exists`, `$type` | Full support |
| Evaluation | `$regex` | Full support |
| Array | `$all`, `$elemMatch`, `$size` | Full support |

### Supported Update Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$set` | Full | Set field values |
| `$unset` | Full | Remove fields |
| `$inc` | Full | Increment numeric values |
| `$push` | Partial | Basic push support |
| `$pull` | Partial | Basic pull support |
| `$addToSet` | Partial | Basic support |

### Features Not Yet Implemented

| Feature | Status | Workaround |
|---------|--------|------------|
| Transactions | Not implemented | Use application-level consistency |
| Change streams with resume tokens | Partial | Full change streams without guaranteed resume |
| Geospatial queries | Not implemented | Use SQL engines with Parquet data |
| Text search | Not implemented | Use external search service or SQL LIKE |
| Capped collections | Not implemented | Use TTL indexes or manual cleanup |
| GridFS | Not implemented | Store files directly in R2/S3 |
| Client-side field level encryption | Not implemented | Use server-side encryption |

### MongoLake-Specific Features

| Feature | Description |
|---------|-------------|
| Parquet storage | Query data with DuckDB, Spark, Trino |
| Iceberg metadata | Time travel and snapshot queries |
| Column promotion | Optimize frequently queried fields |
| Variant encoding | Schema flexibility with Parquet |
| Cloudflare Workers | Global edge deployment |
| Branching (planned) | Git-like database versioning |

---

## 2. Connection String Changes

### MongoDB Connection Strings

```
# MongoDB Atlas
mongodb+srv://user:pass@cluster.mongodb.net/mydb

# MongoDB Replica Set
mongodb://host1:27017,host2:27017,host3:27017/mydb?replicaSet=rs0

# MongoDB Standalone
mongodb://localhost:27017/mydb
```

### MongoLake Connection Strings

```
# Wire Protocol (mongosh, Compass, drivers)
mongodb://localhost:27017/mydb

# Programmatic API - Local development
const lake = new MongoLake({ local: '.mongolake', database: 'mydb' })

# Programmatic API - Cloudflare R2
const lake = new MongoLake({ bucket: env.R2_BUCKET, database: 'mydb' })

# Programmatic API - S3-compatible
const lake = new MongoLake({
  endpoint: 'https://s3.amazonaws.com',
  accessKeyId: 'YOUR_KEY',
  secretAccessKey: 'YOUR_SECRET',
  bucketName: 'my-bucket',
  database: 'mydb'
})

# Mongoose-style connection
await mongoose.connect('mongolake://localhost/mydb')
```

### Connection String Migration Table

| MongoDB Parameter | MongoLake Equivalent | Notes |
|-------------------|---------------------|-------|
| `mongodb://` | `mongodb://` (wire) or `MongoLake({})` | Wire protocol for tools, API for apps |
| Database name in path | `database` config option | Same concept |
| `authSource` | Not applicable | Use storage-level auth |
| `replicaSet` | Not applicable | Uses Durable Objects for consistency |
| `readPreference` | Not applicable | Single-writer architecture |
| `w` (write concern) | Not applicable | Durable by default via WAL |
| `retryWrites` | Not applicable | Built-in retry logic |

---

## 3. Driver Compatibility

### Node.js MongoDB Driver

MongoLake provides a compatible API but is not a drop-in replacement for the MongoDB Node.js driver. You'll need to update imports:

**Before (MongoDB):**
```typescript
import { MongoClient, ObjectId } from 'mongodb';

const client = new MongoClient('mongodb://localhost:27017');
await client.connect();
const db = client.db('myapp');
const users = db.collection('users');
```

**After (MongoLake):**
```typescript
import { MongoLake, ObjectId } from 'mongolake';

const lake = new MongoLake({ local: '.mongolake' });
const db = lake.db('myapp');
const users = db.collection('users');
```

### Mongoose Integration

MongoLake supports Mongoose through a custom driver:

**Option 1: Using `@dotdo/mongoose` (Recommended)**
```typescript
import { createMongoose } from '@dotdo/mongoose';
import { MongoLake } from 'mongolake/mongoose';

const lake = new MongoLake({ local: '.mongolake' });
const mongoose = createMongoose(lake);

// Define schemas as usual
const userSchema = new mongoose.Schema({
  name: String,
  email: { type: String, unique: true },
  age: Number
});

const User = mongoose.model('User', userSchema);
```

**Option 2: Using Official Mongoose with Custom Driver**
```typescript
import mongoose from 'mongoose';
import { createDriver } from 'mongolake/mongoose';

// Set the MongoLake driver
mongoose.setDriver(createDriver({ local: '.mongolake' }));

// Connect using mongolake:// URI scheme
await mongoose.connect('mongolake://localhost/mydb');

// Use Mongoose as normal
const User = mongoose.model('User', userSchema);
```

### Driver Feature Comparison

| Feature | MongoDB Driver | MongoLake Client |
|---------|---------------|------------------|
| Connection pooling | Yes | Not applicable (serverless) |
| Read preferences | Yes | Not applicable |
| Write concern | Yes | Built-in durability |
| Transactions | Yes | Not yet supported |
| Change streams | Yes | Yes (basic support) |
| Bulk write | Yes | Via insertMany/updateMany |
| GridFS | Yes | Not supported |
| Client sessions | Yes | Stub implementation |

---

## 4. Schema Considerations

### Flexible Schema Support

MongoLake supports MongoDB's flexible schema model through **variant encoding**. However, you can optimize performance by promoting frequently-queried fields to native Parquet columns.

### Schema Configuration

```typescript
const lake = new MongoLake({
  database: 'myapp',
  schema: {
    users: {
      // Promote these fields to native Parquet columns
      columns: {
        _id: 'string',
        email: 'string',
        createdAt: 'timestamp',
        age: 'int32',
        status: 'string',
        profile: {
          name: 'string',
          avatar: 'string'
        },
        tags: ['string']  // Array of strings
      },
      // Auto-promote fields appearing in >90% of documents
      autoPromote: { threshold: 0.9 }
    }
  }
});
```

### Parquet Type Mapping

| MongoDB Type | Parquet Type | Notes |
|--------------|--------------|-------|
| String | `string` | UTF-8 encoded |
| Int32 | `int32` | 32-bit signed |
| Int64/Long | `int64` | 64-bit signed |
| Double | `double` | 64-bit IEEE 754 |
| Boolean | `boolean` | true/false |
| Date | `timestamp` | Millisecond precision |
| ObjectId | `string` | 24-char hex string |
| Binary | `binary` | Raw bytes |
| Array | `list` | Typed arrays |
| Object | `struct` | Nested documents |
| Mixed/Any | `variant` | Schema-less encoding |

### Schema Migration Recommendations

1. **Identify hot fields**: Use MongoDB query logs to find frequently filtered/sorted fields
2. **Promote indexed fields**: Any field with an index should be promoted
3. **Consider analytics**: Fields used in aggregations benefit from native columns
4. **Start with variants**: If unsure, let autoPromote discover optimal schema

---

## 5. Data Export from MongoDB

### Using mongodump

```bash
# Export entire database
mongodump --uri="mongodb://localhost:27017/mydb" --out=/backup

# Export specific collection
mongodump --uri="mongodb://localhost:27017/mydb" \
  --collection=users \
  --out=/backup

# Export with query filter
mongodump --uri="mongodb://localhost:27017/mydb" \
  --collection=users \
  --query='{"status":"active"}' \
  --out=/backup

# Export as JSON (for inspection)
mongoexport --uri="mongodb://localhost:27017/mydb" \
  --collection=users \
  --out=/backup/users.json
```

### Using mongoexport for Large Collections

For very large collections, export in batches:

```bash
# Export with pagination
mongoexport --uri="mongodb://localhost:27017/mydb" \
  --collection=users \
  --skip=0 \
  --limit=100000 \
  --out=/backup/users_batch1.json

mongoexport --uri="mongodb://localhost:27017/mydb" \
  --collection=users \
  --skip=100000 \
  --limit=100000 \
  --out=/backup/users_batch2.json
```

### Programmatic Export

```javascript
const { MongoClient } = require('mongodb');
const fs = require('fs');

async function exportCollection(uri, dbName, collectionName, outputPath) {
  const client = new MongoClient(uri);
  await client.connect();

  const collection = client.db(dbName).collection(collectionName);
  const cursor = collection.find({}).batchSize(10000);

  const writeStream = fs.createWriteStream(outputPath);

  let count = 0;
  for await (const doc of cursor) {
    writeStream.write(JSON.stringify(doc) + '\n');
    count++;
    if (count % 10000 === 0) {
      console.log(`Exported ${count} documents...`);
    }
  }

  writeStream.end();
  await client.close();
  console.log(`Exported ${count} total documents`);
}
```

---

## 6. Data Import into MongoLake

### Using the MongoLake CLI

```bash
# Start MongoLake dev server
npx mongolake dev

# Import via mongorestore (wire protocol)
mongorestore --host=localhost:27017 --db=mydb /backup/mydb

# Or via mongoimport
mongoimport --host=localhost:27017 --db=mydb \
  --collection=users \
  /backup/users.json
```

### Programmatic Import

```typescript
import { MongoLake } from 'mongolake';
import * as fs from 'fs';
import * as readline from 'readline';

async function importCollection(
  lake: MongoLake,
  dbName: string,
  collectionName: string,
  inputPath: string,
  batchSize = 1000
) {
  const db = lake.db(dbName);
  const collection = db.collection(collectionName);

  const fileStream = fs.createReadStream(inputPath);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });

  let batch: unknown[] = [];
  let totalImported = 0;

  for await (const line of rl) {
    if (line.trim()) {
      batch.push(JSON.parse(line));

      if (batch.length >= batchSize) {
        await collection.insertMany(batch);
        totalImported += batch.length;
        console.log(`Imported ${totalImported} documents...`);
        batch = [];
      }
    }
  }

  // Import remaining documents
  if (batch.length > 0) {
    await collection.insertMany(batch);
    totalImported += batch.length;
  }

  console.log(`Total imported: ${totalImported} documents`);
}

// Usage
const lake = new MongoLake({ local: '.mongolake' });
await importCollection(lake, 'mydb', 'users', '/backup/users.json');
```

### Streaming Import for Large Datasets

```typescript
import { MongoLake } from 'mongolake';
import { createReadStream } from 'fs';
import { createInterface } from 'readline';

async function* parseJsonLines(filePath: string) {
  const rl = createInterface({
    input: createReadStream(filePath),
    crlfDelay: Infinity
  });

  for await (const line of rl) {
    if (line.trim()) {
      yield JSON.parse(line);
    }
  }
}

async function streamImport(
  lake: MongoLake,
  dbName: string,
  collectionName: string,
  filePath: string
) {
  const collection = lake.db(dbName).collection(collectionName);

  let batch: unknown[] = [];
  const BATCH_SIZE = 1000;

  for await (const doc of parseJsonLines(filePath)) {
    batch.push(doc);

    if (batch.length >= BATCH_SIZE) {
      await collection.insertMany(batch);
      batch = [];
    }
  }

  if (batch.length > 0) {
    await collection.insertMany(batch);
  }
}
```

### Import Validation

After import, verify data integrity:

```typescript
async function validateImport(
  mongoUri: string,
  lake: MongoLake,
  dbName: string,
  collectionName: string
) {
  // Get MongoDB count
  const mongoClient = new MongoClient(mongoUri);
  await mongoClient.connect();
  const mongoCount = await mongoClient
    .db(dbName)
    .collection(collectionName)
    .countDocuments();
  await mongoClient.close();

  // Get MongoLake count
  const lakeCount = await lake
    .db(dbName)
    .collection(collectionName)
    .countDocuments();

  console.log(`MongoDB count: ${mongoCount}`);
  console.log(`MongoLake count: ${lakeCount}`);

  if (mongoCount !== lakeCount) {
    console.error('COUNT MISMATCH - Import may be incomplete');
    return false;
  }

  console.log('Import validated successfully');
  return true;
}
```

---

## 7. Index Migration

### Exporting Index Definitions from MongoDB

```javascript
// Connect to MongoDB and export index definitions
const { MongoClient } = require('mongodb');

async function exportIndexes(uri, dbName) {
  const client = new MongoClient(uri);
  await client.connect();

  const db = client.db(dbName);
  const collections = await db.listCollections().toArray();

  const indexes = {};

  for (const coll of collections) {
    const indexList = await db.collection(coll.name).indexes();
    indexes[coll.name] = indexList.filter(idx => idx.name !== '_id_');
  }

  await client.close();
  return indexes;
}

// Save to file
const indexes = await exportIndexes('mongodb://localhost:27017/mydb', 'mydb');
fs.writeFileSync('indexes.json', JSON.stringify(indexes, null, 2));
```

### Creating Indexes in MongoLake

```typescript
import { MongoLake } from 'mongolake';
import * as fs from 'fs';

async function createIndexes(lake: MongoLake, dbName: string, indexFile: string) {
  const indexes = JSON.parse(fs.readFileSync(indexFile, 'utf-8'));
  const db = lake.db(dbName);

  for (const [collectionName, collIndexes] of Object.entries(indexes)) {
    const collection = db.collection(collectionName);

    for (const idx of collIndexes) {
      try {
        await collection.createIndex(idx.key, {
          name: idx.name,
          unique: idx.unique || false,
          sparse: idx.sparse || false
        });
        console.log(`Created index ${idx.name} on ${collectionName}`);
      } catch (error) {
        console.error(`Failed to create index ${idx.name}: ${error.message}`);
      }
    }
  }
}

// Usage
const lake = new MongoLake({ local: '.mongolake' });
await createIndexes(lake, 'mydb', 'indexes.json');
```

### Index Type Mapping

| MongoDB Index Type | MongoLake Support | Notes |
|--------------------|-------------------|-------|
| Single field | Yes | Full support |
| Compound | Yes | Full support |
| Unique | Yes | Full support |
| Sparse | Yes | Full support |
| TTL | Partial | Via compaction settings |
| Text | No | Use external search |
| 2dsphere | No | Use PostGIS or similar |
| Hashed | No | Use single-field index |
| Wildcard | No | Create explicit indexes |

### Automatic _id Index

MongoLake automatically creates a B-tree index on `_id` for all collections. You don't need to migrate the `_id_` index.

---

## 8. Application Code Changes

### Import Changes

```typescript
// Before (MongoDB)
import { MongoClient, ObjectId, Db, Collection } from 'mongodb';

// After (MongoLake)
import { MongoLake, ObjectId, Database, Collection } from 'mongolake';
// Or for types only:
import type { Document, Filter, Update, FindOptions } from 'mongolake';
```

### Client Initialization

```typescript
// Before (MongoDB)
const client = new MongoClient(process.env.MONGODB_URI);
await client.connect();
const db = client.db('myapp');

// After (MongoLake - Local)
const lake = new MongoLake({ local: '.mongolake' });
const db = lake.db('myapp');

// After (MongoLake - Cloudflare Workers)
const lake = new MongoLake({ bucket: env.R2_BUCKET });
const db = lake.db('myapp');
```

### Query Code (Usually No Changes)

Most query code works without modification:

```typescript
// This code works in both MongoDB and MongoLake
const users = db.collection('users');

// Find with filter
const activeUsers = await users.find({ status: 'active' }).toArray();

// Find with options
const recentUsers = await users
  .find({ createdAt: { $gte: lastWeek } })
  .sort({ createdAt: -1 })
  .limit(10)
  .toArray();

// Aggregation
const stats = await users.aggregate([
  { $match: { status: 'active' } },
  { $group: { _id: '$city', count: { $sum: 1 } } }
]).toArray();

// Updates
await users.updateOne(
  { _id: userId },
  { $set: { lastLogin: new Date() } }
);
```

### Transaction Code Changes

MongoLake doesn't support transactions yet. Refactor transaction-dependent code:

```typescript
// Before (MongoDB with transactions)
const session = client.startSession();
try {
  session.startTransaction();
  await orders.insertOne(order, { session });
  await inventory.updateOne(
    { productId: order.productId },
    { $inc: { quantity: -order.quantity } },
    { session }
  );
  await session.commitTransaction();
} catch (error) {
  await session.abortTransaction();
  throw error;
} finally {
  session.endSession();
}

// After (MongoLake - application-level consistency)
try {
  // Option 1: Use idempotent operations
  const orderId = crypto.randomUUID();
  await orders.insertOne({ _id: orderId, ...order, status: 'pending' });

  const result = await inventory.updateOne(
    { productId: order.productId, quantity: { $gte: order.quantity } },
    { $inc: { quantity: -order.quantity } }
  );

  if (result.modifiedCount === 0) {
    // Rollback: delete the order
    await orders.deleteOne({ _id: orderId });
    throw new Error('Insufficient inventory');
  }

  await orders.updateOne({ _id: orderId }, { $set: { status: 'confirmed' } });
} catch (error) {
  // Handle compensation logic
  throw error;
}
```

### Session Code Changes

```typescript
// Before (MongoDB sessions)
const session = client.startSession();
await collection.findOne({ _id: id }, { session });

// After (MongoLake - sessions are stubs)
// Remove session references, or use the stub for compatibility
import { MongoLakeSession } from 'mongolake/mongoose';
const session = new MongoLakeSession();
await collection.findOne({ _id: id }); // Session not needed
```

### GridFS Migration

```typescript
// Before (MongoDB GridFS)
const bucket = new GridFSBucket(db);
const uploadStream = bucket.openUploadStream('file.pdf');
fileStream.pipe(uploadStream);

// After (MongoLake - use R2/S3 directly)
// For Cloudflare Workers:
await env.R2_BUCKET.put('files/file.pdf', fileBuffer);
const file = await env.R2_BUCKET.get('files/file.pdf');

// Store file reference in collection
await files.insertOne({
  name: 'file.pdf',
  path: 'files/file.pdf',
  size: fileBuffer.byteLength,
  contentType: 'application/pdf',
  uploadedAt: new Date()
});
```

---

## 9. Testing Strategy

### Phase 1: Unit Tests

Create abstraction layer for database operations:

```typescript
// db-interface.ts
export interface DatabaseClient {
  collection<T>(name: string): CollectionClient<T>;
}

export interface CollectionClient<T> {
  findOne(filter: object): Promise<T | null>;
  find(filter: object): Promise<T[]>;
  insertOne(doc: T): Promise<{ insertedId: string }>;
  updateOne(filter: object, update: object): Promise<{ modifiedCount: number }>;
  deleteOne(filter: object): Promise<{ deletedCount: number }>;
}

// mongo-client.ts
import { MongoClient } from 'mongodb';
export class MongoDBClient implements DatabaseClient { /* ... */ }

// mongolake-client.ts
import { MongoLake } from 'mongolake';
export class MongoLakeClient implements DatabaseClient { /* ... */ }
```

### Phase 2: Integration Tests

```typescript
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { MongoLake } from 'mongolake';

describe('MongoLake Integration', () => {
  let lake: MongoLake;
  let collection: Collection<TestDoc>;

  beforeAll(async () => {
    lake = new MongoLake({ local: '.mongolake-test' });
    collection = lake.db('test').collection('users');
  });

  afterAll(async () => {
    await lake.close();
  });

  it('should insert and find documents', async () => {
    const doc = { name: 'Test User', email: 'test@example.com' };
    const result = await collection.insertOne(doc);
    expect(result.insertedId).toBeDefined();

    const found = await collection.findOne({ email: 'test@example.com' });
    expect(found?.name).toBe('Test User');
  });

  it('should support aggregation', async () => {
    await collection.insertMany([
      { name: 'User 1', city: 'NYC' },
      { name: 'User 2', city: 'NYC' },
      { name: 'User 3', city: 'LA' }
    ]);

    const result = await collection.aggregate([
      { $group: { _id: '$city', count: { $sum: 1 } } }
    ]).toArray();

    expect(result).toContainEqual({ _id: 'NYC', count: 2 });
  });
});
```

### Phase 3: Shadow Testing

Run both databases in parallel and compare results:

```typescript
async function shadowTest<T>(
  mongoCollection: MongoCollection<T>,
  lakeCollection: LakeCollection<T>,
  operation: string,
  ...args: unknown[]
) {
  const [mongoResult, lakeResult] = await Promise.all([
    mongoCollection[operation](...args),
    lakeCollection[operation](...args)
  ]);

  const mongoData = await (mongoResult.toArray ? mongoResult.toArray() : mongoResult);
  const lakeData = await (lakeResult.toArray ? lakeResult.toArray() : lakeResult);

  if (JSON.stringify(mongoData) !== JSON.stringify(lakeData)) {
    console.error('MISMATCH:', {
      operation,
      args,
      mongoData,
      lakeData
    });
    return false;
  }
  return true;
}
```

### Phase 4: Load Testing

```typescript
import { bench, describe } from 'vitest';

describe('Performance Comparison', () => {
  bench('MongoLake insertMany 1000 docs', async () => {
    await lakeCollection.insertMany(generateDocs(1000));
  });

  bench('MongoLake find with filter', async () => {
    await lakeCollection.find({ status: 'active' }).limit(100).toArray();
  });

  bench('MongoLake aggregation', async () => {
    await lakeCollection.aggregate([
      { $match: { status: 'active' } },
      { $group: { _id: '$city', count: { $sum: 1 } } }
    ]).toArray();
  });
});
```

### Testing Checklist

- [ ] All CRUD operations work correctly
- [ ] Query operators return correct results
- [ ] Aggregation pipelines produce expected output
- [ ] Indexes improve query performance
- [ ] Change streams deliver events
- [ ] Error handling matches expected behavior
- [ ] Performance meets requirements
- [ ] Data validation/schemas work correctly

---

## 10. Rollback Plan

### Pre-Migration Checklist

1. **Full MongoDB backup**
   ```bash
   mongodump --uri="mongodb://..." --out=/backup/pre-migration
   ```

2. **Document current state**
   - Collection counts
   - Index definitions
   - Schema validation rules
   - Application configuration

3. **Test rollback procedure** in staging environment

### Rollback Triggers

Consider rollback if:
- Data validation failures exceed threshold (>0.1%)
- Query performance degrades significantly (>2x slower)
- Critical functionality is broken
- Data integrity issues are discovered

### Rollback Procedure

```bash
# 1. Stop MongoLake application
pm2 stop mongolake-app

# 2. Redirect traffic back to MongoDB
# Update DNS/load balancer/configuration

# 3. Verify MongoDB is receiving traffic
mongo --eval "db.serverStatus()"

# 4. If data was written to MongoLake during migration:
#    Export and merge with MongoDB
npx mongolake export --format=json --output=/migration/lake-data

# 5. Import any missing data to MongoDB
mongoimport --uri="mongodb://..." \
  --collection=users \
  --mode=merge \
  /migration/lake-data/users.json

# 6. Restart original application
pm2 start mongodb-app

# 7. Monitor for issues
```

### Rollback Script Template

```typescript
// rollback.ts
import { MongoClient } from 'mongodb';
import { MongoLake } from 'mongolake';

async function rollback(
  mongoUri: string,
  lakeConfig: MongoLakeConfig,
  dbName: string,
  collections: string[]
) {
  const lake = new MongoLake(lakeConfig);
  const mongoClient = new MongoClient(mongoUri);
  await mongoClient.connect();

  for (const collName of collections) {
    console.log(`Rolling back ${collName}...`);

    // Get documents written to MongoLake after migration start
    const lakeDocs = await lake
      .db(dbName)
      .collection(collName)
      .find({ _migrationTimestamp: { $gte: MIGRATION_START } })
      .toArray();

    if (lakeDocs.length > 0) {
      // Merge back to MongoDB using upserts
      const mongoCollection = mongoClient.db(dbName).collection(collName);
      for (const doc of lakeDocs) {
        await mongoCollection.updateOne(
          { _id: doc._id },
          { $set: doc },
          { upsert: true }
        );
      }
      console.log(`Merged ${lakeDocs.length} documents back to MongoDB`);
    }
  }

  await mongoClient.close();
  await lake.close();
}
```

---

## 11. Common Issues and Solutions

### Issue: ObjectId Format Differences

**Symptom:** ObjectIds appear as strings instead of BSON ObjectIds

**Solution:**
```typescript
import { ObjectId } from 'mongolake';

// MongoLake ObjectId is compatible but serializes as string
const id = new ObjectId();
console.log(id.toString()); // "507f1f77bcf86cd799439011"

// For queries, both formats work:
await collection.findOne({ _id: id });
await collection.findOne({ _id: id.toString() });
await collection.findOne({ _id: '507f1f77bcf86cd799439011' });
```

### Issue: Date Serialization

**Symptom:** Dates are stored as timestamps or strings

**Solution:**
```typescript
// Ensure dates are Date objects when inserting
await collection.insertOne({
  createdAt: new Date(),  // Correct
  // NOT: createdAt: Date.now()  // Wrong - stores as number
});

// When querying, MongoLake returns Date objects
const doc = await collection.findOne({});
console.log(doc.createdAt instanceof Date); // true
```

### Issue: Nested Field Updates

**Symptom:** Nested `$set` operations don't work as expected

**Solution:**
```typescript
// Use dot notation for nested updates
await collection.updateOne(
  { _id: userId },
  { $set: { 'profile.name': 'New Name' } }  // Correct
);

// For replacing entire nested object:
await collection.updateOne(
  { _id: userId },
  { $set: { profile: { name: 'New Name', avatar: 'url' } } }
);
```

### Issue: Array Operations

**Symptom:** `$push`, `$pull` behavior differs

**Solution:**
```typescript
// Use $each for pushing multiple items
await collection.updateOne(
  { _id: userId },
  { $push: { tags: { $each: ['new', 'tags'] } } }
);

// For complex pulls, consider replaceOne or application logic
const doc = await collection.findOne({ _id: userId });
doc.items = doc.items.filter(item => item.status !== 'removed');
await collection.replaceOne({ _id: userId }, doc);
```

### Issue: Query Performance

**Symptom:** Queries are slower than MongoDB

**Solution:**
1. **Promote frequently-queried fields to columns:**
   ```typescript
   const lake = new MongoLake({
     schema: {
       users: {
         columns: { email: 'string', status: 'string', createdAt: 'timestamp' }
       }
     }
   });
   ```

2. **Ensure indexes exist:**
   ```typescript
   await collection.createIndex({ email: 1 });
   await collection.createIndex({ status: 1, createdAt: -1 });
   ```

3. **Use projection to limit returned fields:**
   ```typescript
   await collection.find({ status: 'active' })
     .project({ _id: 1, name: 1, email: 1 })
     .toArray();
   ```

### Issue: Connection/Timeout Errors

**Symptom:** Operations timeout or fail to connect

**Solution:**
```typescript
// For Cloudflare Workers, ensure proper binding
export default {
  async fetch(request: Request, env: Env) {
    const lake = new MongoLake({ bucket: env.R2_BUCKET });
    // Use lake...
  }
};

// For local development, check file permissions
const lake = new MongoLake({ local: '/path/with/write/permissions' });
```

### Issue: Wire Protocol Compatibility

**Symptom:** mongosh commands fail or behave unexpectedly

**Solution:**
```bash
# Start MongoLake with wire protocol support
npx mongolake dev

# Connect without authentication (local dev)
mongosh mongodb://localhost:27017/mydb --norc

# Some commands may not be supported - check documentation
```

### Issue: Change Stream Gaps

**Symptom:** Change stream events are missed

**Solution:**
```typescript
// Use watch with error handling
const changeStream = collection.watch();

changeStream.on('change', (change) => {
  // Process change
});

changeStream.on('error', async (error) => {
  console.error('Change stream error:', error);
  // Reconnect and resume from last known position
});
```

---

## 12. Performance Comparison Tips

### Benchmarking Setup

```typescript
import { performance } from 'perf_hooks';

async function benchmark(name: string, fn: () => Promise<void>, iterations = 100) {
  // Warmup
  for (let i = 0; i < 10; i++) {
    await fn();
  }

  const times: number[] = [];
  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    await fn();
    times.push(performance.now() - start);
  }

  const avg = times.reduce((a, b) => a + b, 0) / times.length;
  const p50 = times.sort((a, b) => a - b)[Math.floor(times.length * 0.5)];
  const p99 = times.sort((a, b) => a - b)[Math.floor(times.length * 0.99)];

  console.log(`${name}: avg=${avg.toFixed(2)}ms p50=${p50.toFixed(2)}ms p99=${p99.toFixed(2)}ms`);
}
```

### Key Metrics to Compare

| Metric | How to Measure | Expected Result |
|--------|---------------|-----------------|
| Insert latency | Single document insert time | MongoLake may be slightly higher due to WAL |
| Bulk insert throughput | Documents per second for insertMany | Similar or better with batching |
| Point query latency | findOne by _id | Similar with proper indexing |
| Scan query latency | find with filter | Depends on column promotion |
| Aggregation time | Complex pipeline execution | May vary based on data layout |
| Index lookup | Query using indexed field | Similar performance |
| Storage size | Total data size on disk | MongoLake typically smaller (Parquet compression) |

### Performance Optimization Checklist

1. **Column Promotion**
   - Promote fields used in WHERE clauses
   - Promote fields used in GROUP BY
   - Promote fields used for sorting

2. **Index Strategy**
   - Create indexes for all query patterns
   - Use compound indexes for multi-field queries
   - Monitor index usage

3. **Query Optimization**
   - Use projection to limit returned data
   - Add limits to prevent full scans
   - Use zone maps for predicate pushdown

4. **Write Optimization**
   - Batch inserts with insertMany
   - Configure flush thresholds appropriately
   - Let compaction merge small files

### Analytics Performance

MongoLake excels at analytical queries:

```sql
-- Query your MongoLake data with DuckDB
SELECT
    date_trunc('day', createdAt) as day,
    count(*) as signups,
    count(DISTINCT city) as unique_cities
FROM read_parquet('.mongolake/myapp/users*.parquet')
GROUP BY 1
ORDER BY 1;
```

This is often 10-100x faster than MongoDB for large analytical queries due to:
- Columnar storage format
- Predicate pushdown with zone maps
- Efficient compression
- Parallel query execution

---

## Additional Resources

- [MongoLake Documentation](https://mongolake.com/docs)
- [Architecture Overview](./ARCHITECTURE.md)
- [Query Engines Integration](./query-engines.md)
- [Deployment Guide](./DEPLOYMENT.md)
- [API Reference](./api/client.md)

## Getting Help

- [GitHub Issues](https://github.com/dot-do/mongolake/issues)
- [Discord Community](https://discord.gg/mongolake)
- [Stack Overflow](https://stackoverflow.com/questions/tagged/mongolake)
