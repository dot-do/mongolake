# Types Reference

This document covers all TypeScript types and interfaces exported by MongoLake.

## Table of Contents

- [Document Types](#document-types)
- [Filter Types](#filter-types)
- [Update Types](#update-types)
- [Aggregation Types](#aggregation-types)
- [Result Types](#result-types)
- [Options Types](#options-types)
- [Index Types](#index-types)
- [Schema Types](#schema-types)
- [Storage Types](#storage-types)
- [Configuration Types](#configuration-types)
- [R2 Types](#r2-types)

---

## Document Types

### BSONValue

Represents any valid BSON value that can be stored in a document.

```typescript
type BSONValue =
  | string
  | number
  | boolean
  | null
  | Date
  | Uint8Array
  | ObjectId
  | BSONValue[]
  | { [key: string]: BSONValue };
```

---

### ObjectId

MongoDB-compatible ObjectId implementation.

```typescript
class ObjectId {
  constructor(id?: string | Uint8Array);
  toString(): string;
  toHexString(): string;
  getTimestamp(): Date;
  equals(other: ObjectId): boolean;
  static isValid(id: string): boolean;
}
```

**Example:**

```typescript
import { ObjectId } from 'mongolake';

// Generate new ObjectId
const id = new ObjectId();
console.log(id.toString()); // '507f1f77bcf86cd799439011'

// From hex string
const id2 = new ObjectId('507f1f77bcf86cd799439011');

// Get creation timestamp
console.log(id.getTimestamp()); // Date object

// Validate format
ObjectId.isValid('507f1f77bcf86cd799439011'); // true
ObjectId.isValid('invalid'); // false
```

---

### Document

Base document interface with optional `_id`.

```typescript
interface Document {
  _id?: string | ObjectId;
  [key: string]: BSONValue | undefined;
}
```

---

### WithId<T>

Document type with required `_id` field.

```typescript
type WithId<T> = T & { _id: string | ObjectId };
```

**Example:**

```typescript
interface User extends Document {
  name: string;
  email: string;
}

// After insertion or query, documents have _id
const user: WithId<User> = await users.findOne({ email: 'test@example.com' });
console.log(user._id); // Guaranteed to exist
```

---

### AnyDocument

Looser document type for internal operations where strict BSON typing is not required.

```typescript
type AnyDocument = {
  _id?: string | ObjectId | unknown;
  [key: string]: unknown;
};
```

---

### Type Guards

#### `isDocument(value: unknown): value is Document`

Type guard to check if a value is a valid Document.

```typescript
if (isDocument(data)) {
  // data is typed as Document
  console.log(data._id);
}
```

---

#### `assertDocument(value: unknown, message?: string): asserts value is Document`

Assert that a value is a Document, throwing if not.

```typescript
assertDocument(data, 'Expected a valid document');
// data is now typed as Document
```

---

#### `asDocument<T>(doc: T): T & Document`

Type-safe cast of loose document types to Document.

```typescript
const doc = asDocument(externalData);
```

---

## Filter Types

### Filter<T>

Root filter type for querying documents.

```typescript
type Filter<T extends Document = Document> = {
  [K in keyof T]?: FilterCondition<T[K]>;
} & LogicalOperators<T> & {
  $text?: { $search: string; $language?: string };
  $where?: string | ((this: T) => boolean);
};
```

**Example:**

```typescript
const filter: Filter<User> = {
  age: { $gte: 18 },
  status: 'active',
  $or: [
    { city: 'New York' },
    { city: 'Los Angeles' },
  ],
};
```

---

### ComparisonOperators<T>

Comparison operators for field queries.

```typescript
interface ComparisonOperators<T> {
  $eq?: T;      // Equal to
  $ne?: T;      // Not equal to
  $gt?: T;      // Greater than
  $gte?: T;     // Greater than or equal
  $lt?: T;      // Less than
  $lte?: T;     // Less than or equal
  $in?: T[];    // In array
  $nin?: T[];   // Not in array
}
```

**Example:**

```typescript
// Find users aged 18-65
{ age: { $gte: 18, $lte: 65 } }

// Find users in specific cities
{ city: { $in: ['NYC', 'LA', 'SF'] } }
```

---

### ElementOperators

Element existence and type operators.

```typescript
interface ElementOperators {
  $exists?: boolean;          // Field exists
  $type?: string | number;    // Field type
}
```

**Example:**

```typescript
// Find documents with email field
{ email: { $exists: true } }

// Find documents where age is a number
{ age: { $type: 'number' } }
```

---

### ArrayOperators<T>

Operators for array fields.

```typescript
type ArrayOperators<T> = T extends (infer U)[]
  ? {
      $all?: U[];                                    // Contains all elements
      $elemMatch?: U extends Document ? Filter<U> : Record<string, unknown>;
      $size?: number;                                // Array size
    }
  : never;
```

**Example:**

```typescript
// Find documents with specific tags
{ tags: { $all: ['javascript', 'typescript'] } }

// Find documents with at least 3 items
{ items: { $size: 3 } }

// Match array element conditions
{ orders: { $elemMatch: { status: 'pending', amount: { $gt: 100 } } } }
```

---

### LogicalOperators<T>

Logical operators for combining conditions.

```typescript
interface LogicalOperators<T extends Document> {
  $and?: Filter<T>[];
  $or?: Filter<T>[];
  $nor?: Filter<T>[];
  $not?: Filter<T>;
}
```

**Example:**

```typescript
// Complex logical query
{
  $and: [
    { status: 'active' },
    { $or: [
      { role: 'admin' },
      { permissions: { $in: ['write', 'delete'] } }
    ]}
  ]
}
```

---

## Update Types

### Update<T>

Update document combining field and array operators.

```typescript
type Update<T extends Document> = UpdateOperators<T> & ArrayUpdateOperators<T>;
```

---

### UpdateOperators<T>

Field update operators.

```typescript
interface UpdateOperators<T extends Document> {
  $set?: Partial<T>;                                    // Set field values
  $unset?: { [K in keyof T]?: '' | 1 | true };         // Remove fields
  $inc?: { [K in keyof T]?: number };                  // Increment numeric fields
  $mul?: { [K in keyof T]?: number };                  // Multiply numeric fields
  $min?: Partial<T>;                                    // Set to minimum
  $max?: Partial<T>;                                    // Set to maximum
  $rename?: { [K in keyof T]?: string };               // Rename fields
  $currentDate?: { [K in keyof T]?: true | { $type: 'date' | 'timestamp' } };
}
```

**Example:**

```typescript
// Set multiple fields
{ $set: { name: 'Alice', verified: true } }

// Increment view count
{ $inc: { views: 1 } }

// Remove a field
{ $unset: { tempData: '' } }
```

---

### ArrayUpdateOperators<T>

Array-specific update operators.

```typescript
interface ArrayUpdateOperators<T extends Document> {
  $push?: { [K in keyof T]?: T[K] extends (infer U)[] ? U | { $each: U[] } : never };
  $pull?: { [K in keyof T]?: T[K] extends (infer U)[] ? U | Filter<U & Document> : never };
  $addToSet?: { [K in keyof T]?: T[K] extends (infer U)[] ? U | { $each: U[] } : never };
  $pop?: { [K in keyof T]?: 1 | -1 };
}
```

**Example:**

```typescript
// Add to array
{ $push: { tags: 'new-tag' } }

// Add multiple items
{ $push: { tags: { $each: ['tag1', 'tag2'] } } }

// Remove from array
{ $pull: { tags: 'old-tag' } }

// Add unique items
{ $addToSet: { categories: 'tech' } }

// Remove first/last element
{ $pop: { items: 1 } }  // Remove last
{ $pop: { items: -1 } } // Remove first
```

---

## Aggregation Types

### AggregationStage

Union type of all aggregation pipeline stages.

```typescript
type AggregationStage =
  | { $match: Filter<Document> }
  | { $group: GroupStage }
  | { $sort: { [key: string]: 1 | -1 } }
  | { $limit: number }
  | { $skip: number }
  | { $project: { [key: string]: 0 | 1 | ProjectionExpression } }
  | { $unwind: string | { path: string; preserveNullAndEmptyArrays?: boolean } }
  | { $lookup: LookupStage }
  | { $count: string }
  | { $addFields: { [key: string]: unknown } }
  | { $set: { [key: string]: unknown } }
  | { $unset: string | string[] };
```

---

### GroupStage

Grouping specification for `$group` stage.

```typescript
interface GroupStage {
  _id: unknown;              // Grouping key (field reference or expression)
  [key: string]: unknown;    // Accumulator expressions
}
```

**Example:**

```typescript
{
  $group: {
    _id: '$category',
    count: { $sum: 1 },
    avgPrice: { $avg: '$price' },
    items: { $push: '$name' }
  }
}
```

---

### AccumulatorExpression

Accumulator operators for `$group` stage.

```typescript
type AccumulatorExpression =
  | { $sum: number | string }
  | { $avg: string }
  | { $min: string }
  | { $max: string }
  | { $first: string }
  | { $last: string }
  | { $push: string }
  | { $addToSet: string }
  | { $count: Record<string, never> };
```

---

### LookupStage

Join specification for `$lookup` stage.

```typescript
interface LookupStage {
  from: string;                        // Collection to join
  localField?: string;                 // Field from input documents
  foreignField?: string;               // Field from 'from' collection
  as: string;                          // Output array field
  let?: { [key: string]: unknown };    // Variables for pipeline
  pipeline?: AggregationStage[];       // Sub-pipeline
}
```

**Example:**

```typescript
// Simple lookup
{
  $lookup: {
    from: 'orders',
    localField: '_id',
    foreignField: 'userId',
    as: 'userOrders'
  }
}

// Pipeline lookup with variables
{
  $lookup: {
    from: 'inventory',
    let: { productId: '$product_id' },
    pipeline: [
      { $match: { $expr: { $eq: ['$_id', '$$productId'] } } }
    ],
    as: 'stock'
  }
}
```

---

## Result Types

### InsertOneResult

Result of `insertOne` operation.

```typescript
interface InsertOneResult {
  acknowledged: boolean;
  insertedId: string | ObjectId;
}
```

---

### InsertManyResult

Result of `insertMany` operation.

```typescript
interface InsertManyResult {
  acknowledged: boolean;
  insertedCount: number;
  insertedIds: { [key: number]: string | ObjectId };
}
```

---

### UpdateResult

Result of update operations.

```typescript
interface UpdateResult {
  acknowledged: boolean;
  matchedCount: number;
  modifiedCount: number;
  upsertedCount: number;
  upsertedId?: string | ObjectId;
}
```

---

### DeleteResult

Result of delete operations.

```typescript
interface DeleteResult {
  acknowledged: boolean;
  deletedCount: number;
}
```

---

### BulkWriteResult

Result of bulk write operations.

```typescript
interface BulkWriteResult {
  acknowledged: boolean;
  insertedCount: number;
  matchedCount: number;
  modifiedCount: number;
  deletedCount: number;
  upsertedCount: number;
  insertedIds: { [key: number]: string | ObjectId };
  upsertedIds: { [key: number]: string | ObjectId };
}
```

---

## Options Types

### FindOptions

Options for find operations.

```typescript
interface FindOptions {
  projection?: { [key: string]: 0 | 1 };    // Field inclusion/exclusion
  sort?: { [key: string]: 1 | -1 };         // Sort specification
  limit?: number;                            // Maximum documents to return
  skip?: number;                             // Documents to skip
  hint?: string | { [key: string]: 1 | -1 }; // Index hint
  maxTimeMS?: number;                        // Maximum execution time
  skipCorruptedFiles?: boolean;              // Skip corrupted Parquet files (default: false)
}
```

---

### UpdateOptions

Options for update operations.

```typescript
interface UpdateOptions {
  upsert?: boolean;                          // Insert if not found
  arrayFilters?: Filter<Document>[];         // Filters for array updates
  hint?: string | { [key: string]: 1 | -1 }; // Index hint
}
```

---

### DeleteOptions

Options for delete operations.

```typescript
interface DeleteOptions {
  hint?: string | { [key: string]: 1 | -1 }; // Index hint
}
```

---

### AggregateOptions

Options for aggregation operations.

```typescript
interface AggregateOptions {
  allowDiskUse?: boolean;                    // Allow disk for large sorts
  maxTimeMS?: number;                        // Maximum execution time
  hint?: string | { [key: string]: 1 | -1 }; // Index hint
}
```

---

## Index Types

### IndexSpec

Index key specification.

```typescript
interface IndexSpec {
  [key: string]: 1 | -1 | 'text' | '2dsphere' | 'hashed';
}
```

**Example:**

```typescript
// Ascending index
{ email: 1 }

// Descending index
{ createdAt: -1 }

// Compound index
{ userId: 1, createdAt: -1 }

// Text index
{ title: 'text', content: 'text' }
```

---

### IndexOptions

Options for index creation.

```typescript
interface IndexOptions {
  name?: string;                              // Custom index name
  unique?: boolean;                           // Enforce uniqueness
  sparse?: boolean;                           // Only index documents with field
  background?: boolean;                       // Build in background
  expireAfterSeconds?: number;                // TTL index
  partialFilterExpression?: Filter<Document>; // Partial index filter
  weights?: { [key: string]: number };        // Text index field weights
  default_language?: string;                  // Text index language
  language_override?: string;                 // Field for document language
}
```

---

## Schema Types

### ParquetType

Parquet column data types.

```typescript
type ParquetType =
  | 'string'
  | 'int32'
  | 'int64'
  | 'float'
  | 'double'
  | 'boolean'
  | 'timestamp'
  | 'date'
  | 'binary'
  | 'variant';
```

---

### ColumnDef

Column definition for schema configuration.

```typescript
type ColumnDef =
  | ParquetType                      // Simple type
  | [ParquetType]                    // Array type
  | { [key: string]: ColumnDef };    // Struct type
```

**Example:**

```typescript
const columns: { [key: string]: ColumnDef } = {
  email: 'string',
  age: 'int32',
  tags: ['string'],
  address: {
    street: 'string',
    city: 'string',
    zip: 'string',
  },
};
```

---

### CollectionSchema

Schema configuration for a collection.

```typescript
interface CollectionSchema {
  columns?: { [key: string]: ColumnDef };    // Promoted columns
  autoPromote?: { threshold: number };        // Auto-promote threshold
  storeVariant?: boolean;                     // Store full document as variant (default: true)
}
```

---

### SchemaConfig

Database-level schema configuration.

```typescript
interface SchemaConfig {
  [collection: string]: CollectionSchema;
}
```

---

## Storage Types

### ParquetRow

Internal row format in Parquet files.

```typescript
interface ParquetRow {
  _id: string;                    // Document ID
  _seq: number;                   // Sequence number
  _op: 'i' | 'u' | 'd';          // Operation type
  _data: Uint8Array;              // Variant-encoded document
  [column: string]: unknown;      // Promoted columns
}
```

---

### FileMetadata

Metadata for a Parquet file.

```typescript
interface FileMetadata {
  path: string;        // File path in storage
  size: number;        // File size in bytes
  rowCount: number;    // Number of rows
  minSeq: number;      // Minimum sequence number
  maxSeq: number;      // Maximum sequence number
  minId: string;       // Minimum document ID
  maxId: string;       // Maximum document ID
  columns: string[];   // Column names
}
```

---

### CollectionManifest

Manifest tracking collection files.

```typescript
interface CollectionManifest {
  name: string;                 // Collection name
  files: FileMetadata[];        // File metadata list
  schema: CollectionSchema;     // Collection schema
  currentSeq: number;           // Current sequence number
  createdAt: string;            // Creation timestamp
  updatedAt: string;            // Last update timestamp
}
```

---

## Configuration Types

### MongoLakeConfig

Main configuration for MongoLake client.

```typescript
interface MongoLakeConfig {
  local?: string;                    // Local filesystem path
  bucket?: R2Bucket;                 // Cloudflare R2 bucket
  endpoint?: string;                 // S3-compatible endpoint
  accessKeyId?: string;              // S3 access key
  secretAccessKey?: string;          // S3 secret key
  bucketName?: string;               // S3 bucket name
  database?: string;                 // Default database name
  iceberg?: boolean | {              // Iceberg integration
    token: string;
    catalog?: string;
  };
  schema?: SchemaConfig;             // Schema configuration
  branch?: string;                   // Branch name
  asOf?: string | Date | number;     // Time travel query
}
```

---

## R2 Types

These types match the Cloudflare Workers R2 API.

### R2Bucket

```typescript
interface R2Bucket {
  get(key: string): Promise<R2ObjectBody | null>;
  head(key: string): Promise<R2Object | null>;
  put(key: string, value: ArrayBuffer | Uint8Array | string): Promise<R2Object>;
  delete(key: string): Promise<void>;
  list(options?: R2ListOptions): Promise<R2Objects>;
  createMultipartUpload(key: string): Promise<R2MultipartUpload>;
}
```

### R2ObjectBody

```typescript
interface R2ObjectBody {
  arrayBuffer(): Promise<ArrayBuffer>;
  text(): Promise<string>;
  json<T>(): Promise<T>;
  body: ReadableStream;
  etag: string;
}
```

### R2Object

```typescript
interface R2Object {
  key: string;
  size: number;
  etag: string;
}
```

### R2ListOptions

```typescript
interface R2ListOptions {
  prefix?: string;
  limit?: number;
  cursor?: string;
}
```

### R2Objects

```typescript
interface R2Objects {
  objects: R2Object[];
  truncated: boolean;
  cursor?: string;
}
```

### R2MultipartUpload

```typescript
interface R2MultipartUpload {
  uploadPart(partNumber: number, value: ArrayBuffer | Uint8Array): Promise<R2UploadedPart>;
  complete(parts: R2UploadedPart[]): Promise<R2Object>;
  abort(): Promise<void>;
}
```

### R2UploadedPart

```typescript
interface R2UploadedPart {
  partNumber: number;
  etag: string;
}
```

---

## See Also

- [Client Reference](./client.md) - Client API documentation
- [Storage Reference](./storage.md) - Storage backend interface
- [Worker Reference](./worker.md) - Worker and Durable Object exports
