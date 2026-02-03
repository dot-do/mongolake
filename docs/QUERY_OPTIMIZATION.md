# Query Optimization Guide

This guide covers query optimization techniques for MongoLake, including index usage, query planning, zone map filtering, aggregation pipeline optimization, and common anti-patterns to avoid.

## Table of Contents

- [Index Usage Best Practices](#index-usage-best-practices)
- [Query Planning Explanation](#query-planning-explanation)
- [Zone Map Filtering](#zone-map-filtering)
- [Aggregation Pipeline Optimization](#aggregation-pipeline-optimization)
- [Performance Tips](#performance-tips)
- [Common Anti-Patterns to Avoid](#common-anti-patterns-to-avoid)

---

## Index Usage Best Practices

MongoLake supports B-tree indexes for fast lookups and range queries, as well as text indexes for full-text search.

### B-Tree Indexes

B-tree indexes provide efficient O(log n) lookups for equality and range queries on single fields.

#### Creating Indexes

```typescript
// Create an index on the 'email' field
await collection.createIndex({ email: 1 });

// Create a unique index
await collection.createIndex({ email: 1 }, { unique: true });

// Create a sparse index (excludes documents without the field)
await collection.createIndex({ optionalField: 1 }, { sparse: true });

// Create an index with a custom name
await collection.createIndex({ createdAt: -1 }, { name: 'createdAt_desc' });
```

#### When Indexes Are Used

Indexes are automatically used for the following query patterns:

| Query Pattern | Index Usage | Example |
|---------------|-------------|---------|
| Equality (`$eq`) | Full index scan | `{ status: 'active' }` |
| Range (`$gt`, `$gte`, `$lt`, `$lte`) | Range scan | `{ age: { $gte: 18, $lt: 65 } }` |
| Set membership (`$in`) | Multiple point lookups | `{ status: { $in: ['active', 'pending'] } }` |

#### Best Practices for Index Creation

1. **Index frequently filtered fields** - Create indexes on fields that appear in `find()` filters and `$match` stages.

2. **Index high-cardinality fields first** - Fields with many unique values (like `_id`, `email`, `userId`) benefit most from indexing.

3. **Consider query patterns** - If you always query by `{ tenantId, userId }`, consider the order of filtering.

4. **Monitor index usage** - Use the query planner to verify indexes are being used.

```typescript
// Check if a query uses an index
const plan = await queryPlanner.createPlan('users', { email: 'user@example.com' });
console.log(plan.strategy); // 'index_scan' or 'full_scan'
console.log(plan.explanation);
```

5. **Avoid over-indexing** - Each index adds write overhead. Only index fields that are frequently queried.

### Text Indexes

Text indexes support full-text search with TF-IDF relevance scoring.

#### Creating Text Indexes

```typescript
// Create a text index on title and body fields
await collection.createIndex(
  { title: 'text', body: 'text' },
  {
    weights: { title: 10, body: 1 }, // Title matches score 10x higher
    name: 'content_text'
  }
);
```

#### Using Text Search

```typescript
// Basic text search
const results = await collection.find({
  $text: { $search: 'mongodb tutorial' }
}).toArray();

// Search with phrase
const phraseResults = await collection.find({
  $text: { $search: '"getting started"' }
}).toArray();

// Search with negation
const excludeResults = await collection.find({
  $text: { $search: 'mongodb -deprecated' }
}).toArray();
```

#### Text Index Features

- **Tokenization**: Text is split into words, lowercased, and filtered for stop words
- **TF-IDF scoring**: Results are ranked by relevance using term frequency and inverse document frequency
- **Field weights**: Configure different weights for different fields (e.g., title matches more important than body)
- **Phrase search**: Use quotes for exact phrase matching
- **Negation**: Use minus sign to exclude terms

---

## Query Planning Explanation

MongoLake's query planner analyzes filters and determines the optimal execution strategy.

### How the Query Planner Works

1. **Filter Analysis**: The planner examines each field condition in the filter
2. **Index Matching**: It checks if any indexed fields match the filter conditions
3. **Strategy Selection**: Based on available indexes, it selects either:
   - `index_scan`: Use an index for efficient lookup
   - `full_scan`: Scan all documents (no suitable index)
4. **Selectivity Estimation**: The planner estimates how many documents will match

### Execution Plans

```typescript
import { QueryPlanner } from 'mongolake';

const planner = new QueryPlanner(indexManager);

// Get detailed execution plan
const plan = await planner.createPlan('users', {
  status: 'active',
  age: { $gte: 18 }
});

console.log(plan);
// {
//   strategy: 'index_scan',
//   indexName: 'status_1',
//   field: 'status',
//   operation: 'eq',
//   residualFilter: { age: { $gte: 18 } },
//   estimatedSelectivity: 0.01,
//   explanation: "Using index 'status_1' for eq on 'status'"
// }
```

### Understanding Execution Plans

| Field | Description |
|-------|-------------|
| `strategy` | Either `index_scan` (uses index) or `full_scan` (scans all docs) |
| `indexName` | Name of the index being used (if index scan) |
| `field` | The indexed field being queried |
| `operation` | Type of index operation: `eq`, `range`, or `in` |
| `residualFilter` | Additional filter conditions applied after index scan |
| `estimatedSelectivity` | Expected fraction of documents matching (0.0-1.0, lower is more selective) |
| `explanation` | Human-readable description of the plan |

### Selectivity Estimates

| Operation | Estimated Selectivity | Description |
|-----------|----------------------|-------------|
| `eq` | 0.01 (1%) | Equality is highly selective |
| `in` | 0.1 (10%) | Set membership is fairly selective |
| `range` | 0.3 (30%) | Range queries are less selective |

### Using explain() for Debugging

```typescript
const explanation = await planner.explain('users', {
  email: 'test@example.com',
  status: 'active'
});

console.log(explanation);
// Query Plan for collection 'users':
//   Strategy: index_scan
//   Index: email_1
//   Field: email
//   Operation: eq
//   Estimated Selectivity: 1.0%
//   Residual Filter: {"status":"active"}
//   Explanation: Using index 'email_1' for eq on 'email'
```

---

## Zone Map Filtering

Zone maps enable predicate pushdown - skipping entire Parquet files during queries based on min/max statistics.

### How Zone Maps Work

Zone maps track statistics for each Parquet file:
- **Min/Max values**: The smallest and largest values for each column
- **Null count**: How many null values exist
- **Row count**: Total number of rows

During query execution, MongoLake evaluates the filter against these statistics to determine if a file can possibly contain matching documents.

### Supported Predicates

Zone map filtering works with these operators:

| Operator | Zone Map Evaluation |
|----------|---------------------|
| `$eq` | Value must be within [min, max] range |
| `$ne` | Can only skip if min == max == value |
| `$gt` | File may match if max > value |
| `$gte` | File may match if max >= value |
| `$lt` | File may match if min < value |
| `$lte` | File may match if min <= value |
| `$in` | Any value in set must fall within [min, max] |
| `$nin` | Cannot reliably skip files |

### Example: Zone Map Filtering in Action

Consider a collection with these Parquet files:

| File | age_min | age_max | status_values |
|------|---------|---------|---------------|
| file1.parquet | 18 | 35 | ['active'] |
| file2.parquet | 30 | 65 | ['active', 'inactive'] |
| file3.parquet | 60 | 85 | ['retired'] |

For the query `{ age: { $lt: 25 }, status: 'active' }`:

- **file1.parquet**: age_min(18) < 25, so MAY match - must scan
- **file2.parquet**: age_min(30) >= 25, so CANNOT match - **SKIPPED**
- **file3.parquet**: age_min(60) >= 25, so CANNOT match - **SKIPPED**

Only 1 of 3 files needs to be scanned!

### Optimizing for Zone Map Filtering

1. **Sort data before loading**: Group related data together so min/max ranges are tighter.

```typescript
// Poor: Random insertion order spreads values across all files
for (const user of users) {
  await collection.insertOne(user);
}

// Better: Sort by commonly filtered fields before bulk insert
const sortedUsers = users.sort((a, b) => a.createdAt - b.createdAt);
await collection.insertMany(sortedUsers);
```

2. **Use range queries on sorted fields**: Queries on fields that correlate with data ordering benefit most from zone maps.

3. **Choose appropriate row group sizes**: Smaller row groups provide finer-grained filtering but more overhead.

### Zone Map Limitations

Zone maps cannot help with:
- **Regex patterns** (`$regex`)
- **Existence checks** (`$exists`)
- **Negation with large value sets** (`$nin`)
- **Logical operators at filter root** (`$or`, `$nor`)
- **Fields with high value diversity per file** (e.g., UUIDs)

---

## Aggregation Pipeline Optimization

MongoLake supports MongoDB-compatible aggregation pipelines with stages for filtering, grouping, sorting, and joining.

### Supported Aggregation Stages

| Stage | Description |
|-------|-------------|
| `$match` | Filter documents |
| `$project` | Include/exclude fields |
| `$sort` | Order documents |
| `$limit` | Limit result count |
| `$skip` | Skip documents |
| `$group` | Group and aggregate |
| `$unwind` | Deconstruct arrays |
| `$lookup` | Join with other collections |
| `$count` | Count documents |
| `$addFields` / `$set` | Add computed fields |
| `$unset` | Remove fields |
| `$facet` | Multi-facet aggregation |
| `$bucket` | Bucket by boundaries |

### Pipeline Optimization Strategies

#### 1. Place $match Early

Always place `$match` stages as early as possible to reduce the number of documents processed by subsequent stages.

```typescript
// Good: Filter first, then process
const pipeline = [
  { $match: { status: 'active', createdAt: { $gte: lastWeek } } },
  { $group: { _id: '$category', count: { $sum: 1 } } },
  { $sort: { count: -1 } }
];

// Bad: Processing all documents before filtering
const pipeline = [
  { $group: { _id: '$category', count: { $sum: 1 }, docs: { $push: '$$ROOT' } } },
  { $unwind: '$docs' },
  { $match: { 'docs.status': 'active' } } // Too late!
];
```

#### 2. Use $project to Limit Fields

Reduce memory usage by projecting only needed fields early in the pipeline.

```typescript
const pipeline = [
  { $match: { status: 'active' } },
  { $project: { name: 1, email: 1, department: 1 } }, // Only keep needed fields
  { $group: { _id: '$department', employees: { $push: '$name' } } }
];
```

#### 3. Combine $limit with $sort

When you need top N results, always use `$limit` immediately after `$sort`.

```typescript
// Efficient: Sort and immediately limit
const pipeline = [
  { $match: { type: 'sale' } },
  { $sort: { amount: -1 } },
  { $limit: 10 } // Top 10 sales
];
```

#### 4. Optimize $lookup Operations

For joins, filter the foreign collection with a pipeline when possible.

```typescript
// Better: Filter foreign collection with pipeline
const pipeline = [
  {
    $lookup: {
      from: 'orders',
      let: { customerId: '$_id' },
      pipeline: [
        { $match: { $expr: { $eq: ['$customerId', '$$customerId'] } } },
        { $match: { status: 'completed' } }, // Filter in lookup
        { $limit: 5 } // Limit in lookup
      ],
      as: 'recentOrders'
    }
  }
];

// Avoid: Fetching all orders then filtering
const pipeline = [
  {
    $lookup: {
      from: 'orders',
      localField: '_id',
      foreignField: 'customerId',
      as: 'allOrders'
    }
  },
  { $unwind: '$allOrders' },
  { $match: { 'allOrders.status': 'completed' } } // Too late, already joined everything
];
```

#### 5. Use $facet for Multiple Aggregations

When you need multiple aggregation results from the same data, use `$facet` instead of running multiple pipelines.

```typescript
// Efficient: Single pipeline with facet
const pipeline = [
  { $match: { createdAt: { $gte: lastMonth } } },
  {
    $facet: {
      byCategory: [
        { $group: { _id: '$category', count: { $sum: 1 } } }
      ],
      byStatus: [
        { $group: { _id: '$status', count: { $sum: 1 } } }
      ],
      topItems: [
        { $sort: { views: -1 } },
        { $limit: 5 }
      ]
    }
  }
];
```

### Group Accumulators

MongoLake supports these accumulators in `$group`:

| Accumulator | Description | Example |
|-------------|-------------|---------|
| `$sum` | Sum values or count | `{ $sum: '$amount' }` or `{ $sum: 1 }` |
| `$avg` | Calculate average | `{ $avg: '$score' }` |
| `$min` | Find minimum | `{ $min: '$price' }` |
| `$max` | Find maximum | `{ $max: '$price' }` |
| `$first` | First value in group | `{ $first: '$name' }` |
| `$last` | Last value in group | `{ $last: '$name' }` |
| `$push` | Array of all values | `{ $push: '$item' }` |
| `$addToSet` | Array of unique values | `{ $addToSet: '$tag' }` |
| `$count` | Count documents | `{ $count: {} }` |

---

## Performance Tips

### Query Performance

1. **Use indexes for frequent queries** - Every frequent query pattern should have a supporting index.

2. **Prefer equality over range** - Equality queries are more selective and faster.

```typescript
// Better: Equality
{ status: 'active' }

// Slower: Range on same data
{ status: { $in: ['active'] } }
```

3. **Limit result sets** - Always use `limit()` when you don't need all results.

```typescript
// Get just the first 10 matching documents
const results = await collection.find({ status: 'active' }).limit(10).toArray();
```

4. **Project only needed fields** - Reduce data transfer and memory usage.

```typescript
// Only fetch name and email
const users = await collection.find(
  { status: 'active' },
  { projection: { name: 1, email: 1 } }
).toArray();
```

5. **Use count for existence checks** - Don't fetch documents just to check if they exist.

```typescript
// Good: Just count
const exists = await collection.countDocuments({ email: 'test@example.com' }) > 0;

// Bad: Fetching document just to check existence
const doc = await collection.findOne({ email: 'test@example.com' });
const exists = doc !== null;
```

### Write Performance

1. **Use bulk operations** - `insertMany()` and `bulkWrite()` are much faster than individual operations.

```typescript
// Good: Batch insert
await collection.insertMany(documents);

// Bad: Individual inserts
for (const doc of documents) {
  await collection.insertOne(doc);
}
```

2. **Consider write ordering** - Sorting data before insertion can improve zone map effectiveness.

3. **Balance index count** - Each index adds write overhead. Only index what you query.

### Memory Management

1. **Use streaming for large result sets** - Use async iterators instead of `toArray()` for large results.

```typescript
// Good: Stream results
for await (const doc of collection.find({ type: 'log' })) {
  processDoc(doc);
}

// Bad: Load everything into memory
const allDocs = await collection.find({ type: 'log' }).toArray();
```

2. **Limit aggregation memory** - Use `$limit` and `$project` early to reduce working set size.

---

## Common Anti-Patterns to Avoid

### 1. Missing Indexes on Filtered Fields

**Problem**: Queries without indexes require full collection scans.

```typescript
// Anti-pattern: Frequent query without index
await collection.find({ status: 'active', region: 'US' }).toArray();
// This scans ALL documents if no index exists
```

**Solution**: Create indexes for frequently filtered fields.

```typescript
await collection.createIndex({ status: 1 });
// Or compound index if always queried together
await collection.createIndex({ status: 1, region: 1 });
```

### 2. Using $or Without Indexes

**Problem**: `$or` queries cannot use zone map filtering and often require full scans.

```typescript
// Anti-pattern: $or forces evaluation of all conditions
await collection.find({
  $or: [
    { email: 'user@example.com' },
    { phone: '555-1234' }
  ]
}).toArray();
```

**Solution**: If possible, restructure queries or ensure all `$or` branches have indexes.

### 3. Fetching Entire Documents When Only IDs Are Needed

**Problem**: Transferring unnecessary data wastes bandwidth and memory.

```typescript
// Anti-pattern: Fetching full documents
const docs = await collection.find({ category: 'electronics' }).toArray();
const ids = docs.map(d => d._id);
```

**Solution**: Use projection to fetch only needed fields.

```typescript
const docs = await collection.find(
  { category: 'electronics' },
  { projection: { _id: 1 } }
).toArray();
const ids = docs.map(d => d._id);
```

### 4. Using $regex Without Anchors

**Problem**: Regex patterns without anchors cannot use indexes and require full scans.

```typescript
// Anti-pattern: Unanchored regex
await collection.find({ name: { $regex: 'smith' } }).toArray();
```

**Solution**: Use anchored patterns when possible, or consider text indexes for search.

```typescript
// Better: Anchored regex (can use index)
await collection.find({ name: { $regex: '^Smith' } }).toArray();

// Best: Use text index for search
await collection.find({ $text: { $search: 'smith' } }).toArray();
```

### 5. Deep Pagination with Skip

**Problem**: Using large `$skip` values is inefficient as documents must still be scanned.

```typescript
// Anti-pattern: Deep pagination
await collection.find().skip(10000).limit(10).toArray();
// Must scan and discard 10,000 documents
```

**Solution**: Use cursor-based pagination with a sort key.

```typescript
// Better: Cursor-based pagination
const lastId = previousPage[previousPage.length - 1]._id;
await collection.find({ _id: { $gt: lastId } })
  .sort({ _id: 1 })
  .limit(10)
  .toArray();
```

### 6. Sorting Without Indexes

**Problem**: Sorting large result sets without indexes requires in-memory sorting.

```typescript
// Anti-pattern: Sort on unindexed field
await collection.find({ status: 'active' })
  .sort({ createdAt: -1 })
  .toArray();
```

**Solution**: Create indexes that support common sort patterns.

```typescript
await collection.createIndex({ status: 1, createdAt: -1 });
```

### 7. Using $where or Complex Expressions

**Problem**: `$where` and complex expressions cannot use indexes.

```typescript
// Anti-pattern: JavaScript expression
await collection.find({
  $expr: { $gt: [{ $size: '$items' }, 5] }
}).toArray();
```

**Solution**: Pre-compute values or restructure data to enable indexed queries.

```typescript
// Better: Store computed value
// When inserting: { items: [...], itemCount: items.length }
await collection.find({ itemCount: { $gt: 5 } }).toArray();
```

### 8. Unnecessary $unwind in Aggregations

**Problem**: `$unwind` multiplies documents, increasing memory and processing time.

```typescript
// Anti-pattern: Unwind just to filter
const pipeline = [
  { $unwind: '$tags' },
  { $match: { tags: 'important' } },
  { $group: { _id: '$_id', doc: { $first: '$$ROOT' } } }
];
```

**Solution**: Use array operators when possible.

```typescript
// Better: Match directly on array
const pipeline = [
  { $match: { tags: 'important' } }
];
```

### 9. Not Using $limit in Lookups

**Problem**: `$lookup` without limits fetches all matching foreign documents.

```typescript
// Anti-pattern: Unlimited lookup
{
  $lookup: {
    from: 'comments',
    localField: '_id',
    foreignField: 'postId',
    as: 'comments' // Could be thousands of comments!
  }
}
```

**Solution**: Use pipeline lookups with limits.

```typescript
// Better: Limited lookup
{
  $lookup: {
    from: 'comments',
    let: { postId: '$_id' },
    pipeline: [
      { $match: { $expr: { $eq: ['$postId', '$$postId'] } } },
      { $sort: { createdAt: -1 } },
      { $limit: 10 } // Only get recent comments
    ],
    as: 'recentComments'
  }
}
```

### 10. Ignoring Query Plan Analysis

**Problem**: Not checking if queries use indexes leads to undetected performance issues.

```typescript
// Anti-pattern: Assume query is optimized
await collection.find(complexFilter).toArray();
```

**Solution**: Regularly analyze query plans.

```typescript
const plan = await queryPlanner.createPlan('collection', complexFilter);
if (plan.strategy === 'full_scan') {
  console.warn('Query requires full scan:', plan.explanation);
}
```

---

## Summary

Optimizing MongoLake queries requires:

1. **Create appropriate indexes** for frequent query patterns
2. **Use the query planner** to verify index usage
3. **Leverage zone maps** by sorting data and using supported operators
4. **Optimize aggregation pipelines** with early `$match` and `$project`
5. **Avoid common anti-patterns** like missing indexes and deep pagination

For configuration tuning, see the [Performance Tuning Guide](./operations/performance-tuning.md).
