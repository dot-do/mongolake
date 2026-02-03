# Data Modeling Guide for MongoLake

This guide provides comprehensive MongoDB-compatible data modeling guidance optimized for MongoLake's unique architecture. MongoLake stores data as Parquet files, enabling both transactional workloads and analytical queries while maintaining full MongoDB API compatibility.

## Table of Contents

- [Introduction](#introduction)
- [Schema Design Principles](#schema-design-principles)
- [MongoLake-Specific Considerations](#mongolake-specific-considerations)
- [Relationship Patterns](#relationship-patterns)
- [Anti-Patterns to Avoid](#anti-patterns-to-avoid)
- [Index Design Guidance](#index-design-guidance)
- [Example Schemas](#example-schemas)
- [Performance Implications](#performance-implications)

---

## Introduction

MongoLake combines the flexibility of MongoDB's document model with the analytical power of columnar Parquet storage. Understanding how to model your data effectively can significantly impact both write performance and query efficiency.

### Key Differences from Traditional MongoDB

| Aspect | Traditional MongoDB | MongoLake |
|--------|---------------------|-----------|
| Storage Format | BSON documents | Parquet columnar files |
| Query Optimization | B-tree indexes | Zone maps + B-tree indexes |
| Analytical Queries | Limited | Native columnar efficiency |
| Schema Evolution | Fully flexible | Flexible with column promotion |
| External Tools | MongoDB ecosystem | DuckDB, Spark, Trino compatible |

---

## Schema Design Principles

### 1. Document Structure

MongoLake documents follow MongoDB's BSON structure but are stored in Parquet format. Design your documents with both operational and analytical use cases in mind.

#### Flat vs. Nested Documents

**Flat documents** are optimal when:
- Fields are frequently queried independently
- Analytics tools will aggregate across single fields
- Schema is stable and well-defined

```javascript
// Flat structure - optimal for analytics
{
  _id: ObjectId("..."),
  userId: "user-123",
  orderTotal: 99.99,
  orderDate: ISODate("2024-01-15"),
  customerName: "Alice Smith",
  customerEmail: "alice@example.com",
  shippingCity: "Seattle",
  shippingState: "WA",
  shippingCountry: "USA"
}
```

**Nested documents** are better when:
- Data is always accessed together
- Representing logical groupings
- Maintaining data locality for read patterns

```javascript
// Nested structure - good for document-oriented access
{
  _id: ObjectId("..."),
  userId: "user-123",
  orderTotal: 99.99,
  orderDate: ISODate("2024-01-15"),
  customer: {
    name: "Alice Smith",
    email: "alice@example.com"
  },
  shipping: {
    city: "Seattle",
    state: "WA",
    country: "USA"
  }
}
```

### 2. Embedding vs. Referencing

Choose between embedding related data within a document or referencing it via IDs.

#### When to Embed

Embed data when:
- **One-to-one relationships** with data always accessed together
- **One-to-few relationships** where the "many" side is bounded
- **Read-heavy workloads** where joins would be expensive
- **Data locality** is important for your access patterns

```javascript
// Embedded comments (one-to-few)
{
  _id: ObjectId("..."),
  title: "My Blog Post",
  content: "...",
  comments: [
    { author: "Alice", text: "Great post!", date: ISODate("2024-01-15") },
    { author: "Bob", text: "Thanks!", date: ISODate("2024-01-16") }
  ]
}
```

#### When to Reference

Use references when:
- **One-to-many relationships** where "many" is unbounded
- **Many-to-many relationships**
- **Data changes independently** and frequently
- **Avoiding document size limits** (16MB)
- **Shared data** referenced by multiple documents

```javascript
// Referenced orders (one-to-many, unbounded)
// Users collection
{
  _id: ObjectId("user-123"),
  name: "Alice Smith",
  email: "alice@example.com"
}

// Orders collection
{
  _id: ObjectId("order-456"),
  userId: ObjectId("user-123"),  // Reference
  total: 99.99,
  items: [...]
}
```

### 3. Field Naming Conventions

Choose field names carefully for both readability and performance:

- **Use short but descriptive names** - Field names are stored in Parquet metadata
- **Use camelCase consistently** - MongoDB convention
- **Avoid special characters** - Stick to alphanumeric and underscores
- **Use meaningful prefixes** for related fields

```javascript
// Good field naming
{
  _id: ObjectId("..."),
  userId: "user-123",
  userName: "Alice",
  orderTotal: 99.99,
  orderDate: ISODate("2024-01-15"),
  shippingAddr: { city: "Seattle", state: "WA" }
}

// Avoid: overly long or abbreviated names
{
  _id: ObjectId("..."),
  u: "user-123",              // Too short
  userFullNameForDisplay: "Alice",  // Too long
  tot: 99.99,                 // Unclear abbreviation
}
```

---

## MongoLake-Specific Considerations

MongoLake's Parquet storage introduces unique considerations for data modeling that differ from traditional MongoDB.

### 1. Column Promotion

MongoLake uses a hybrid storage model:
- **Promoted columns**: Frequently accessed fields stored as native Parquet columns
- **Variant encoding**: Flexible fields stored in a `_data` column using Parquet's variant format

#### Benefits of Promoted Columns

| Benefit | Description |
|---------|-------------|
| Faster queries | Direct column access without parsing |
| Better compression | Type-specific compression algorithms |
| Zone map filtering | Min/max statistics enable predicate pushdown |
| Analytics compatibility | Native Parquet column access from external tools |

#### Configuring Column Promotion

Define frequently queried fields in your schema configuration:

```yaml
# schema.yaml
collections:
  orders:
    promotedFields:
      - path: "_id"
        type: "objectId"
      - path: "userId"
        type: "string"
      - path: "orderDate"
        type: "timestamp"
      - path: "total"
        type: "double"
      - path: "status"
        type: "string"
```

#### When to Promote Fields

Promote fields that are:
- **Frequently filtered** in queries (`WHERE` clauses)
- **Used in aggregations** (GROUP BY, ORDER BY)
- **Queried by external tools** (DuckDB, Spark)
- **Have consistent types** across documents

### 2. Zone Maps and Predicate Pushdown

Zone maps track min/max values per Parquet file, enabling queries to skip files that cannot contain matching data.

#### Optimizing for Zone Maps

**Sort data before insertion** to create tighter min/max ranges:

```typescript
// Sort by date before bulk insert
const orders = await fetchOrders();
orders.sort((a, b) => a.orderDate - b.orderDate);
await collection.insertMany(orders);
```

**Choose appropriate clustering fields**:
- Timestamp fields (createdAt, updatedAt)
- Sequential IDs or counters
- Category/status fields with limited values

**Zone map effective operators**:

| Operator | Zone Map Benefit |
|----------|------------------|
| `$eq` | High - can skip files where value is outside [min, max] |
| `$lt`, `$lte` | High - can skip files where min > value |
| `$gt`, `$gte` | High - can skip files where max < value |
| `$in` | Medium - checks each value against range |
| `$ne`, `$nin` | Low - cannot reliably skip files |
| `$regex` | None - requires full scan |

### 3. Variant Encoding Overhead

Fields not promoted to columns are stored using variant encoding. While flexible, this adds overhead:

```
Variant Encoding Overhead:
- 1 byte type marker per value
- Variable-length integers for sizes
- UTF-8 string encoding with length prefix
- Recursive encoding for nested objects/arrays
```

#### Minimizing Variant Overhead

1. **Promote frequently accessed fields** to native columns
2. **Use consistent types** for the same field across documents
3. **Avoid deeply nested structures** when possible
4. **Consider denormalization** for analytics-heavy workloads

### 4. Parquet Type Mappings

Understanding how MongoDB types map to Parquet helps optimize storage:

| MongoDB/BSON Type | Parquet Physical Type | Parquet Logical Type | Notes |
|-------------------|----------------------|----------------------|-------|
| String | BYTE_ARRAY | STRING | UTF-8 encoded |
| Int32 | INT32 | INT_32 | 32-bit signed |
| Int64 / Long | INT64 | INT_64 | 64-bit signed |
| Double | DOUBLE | - | IEEE 754 |
| Boolean | BOOLEAN | - | 1-bit packed |
| Date | INT64 | TIMESTAMP_MILLIS | Milliseconds since epoch |
| ObjectId | FIXED_LEN_BYTE_ARRAY | - | 12 bytes |
| Object | BYTE_ARRAY | - | Variant encoded |
| Array | BYTE_ARRAY | - | Variant encoded |
| Decimal128 | FIXED_LEN_BYTE_ARRAY | DECIMAL | 16 bytes |
| Binary | BYTE_ARRAY | - | Raw bytes |

### 5. Columnar Format Benefits

Design schemas to maximize columnar storage benefits:

**Aggregate-friendly schemas**:
```javascript
// Analytics-optimized schema
{
  _id: ObjectId("..."),
  // Frequently aggregated fields as top-level
  category: "electronics",
  region: "west",
  revenue: 1500.00,
  quantity: 5,
  date: ISODate("2024-01-15"),

  // Less frequently accessed details
  details: {
    productName: "Widget Pro",
    sku: "WP-123",
    description: "..."
  }
}
```

**Wide tables for analytics** - When analytical queries are primary:
```javascript
// Wide, flat structure for BI tools
{
  order_id: "ORD-123",
  order_date: ISODate("2024-01-15"),
  customer_id: "CUST-456",
  customer_name: "Alice",
  customer_region: "west",
  product_id: "PROD-789",
  product_name: "Widget",
  product_category: "electronics",
  quantity: 5,
  unit_price: 99.99,
  total_price: 499.95,
  discount_percent: 10,
  shipping_cost: 15.00
}
```

---

## Relationship Patterns

### One-to-One Relationships

Embed the related document when data is always accessed together:

```javascript
// User with profile (embedded one-to-one)
{
  _id: ObjectId("user-123"),
  email: "alice@example.com",
  profile: {
    firstName: "Alice",
    lastName: "Smith",
    avatar: "https://...",
    bio: "Software engineer..."
  },
  settings: {
    theme: "dark",
    notifications: true,
    language: "en"
  }
}
```

Use references when related data changes independently:

```javascript
// User with billing (referenced one-to-one)
// Users collection
{
  _id: ObjectId("user-123"),
  email: "alice@example.com",
  billingId: ObjectId("billing-456")
}

// Billing collection
{
  _id: ObjectId("billing-456"),
  cardLast4: "1234",
  expiryMonth: 12,
  expiryYear: 2025,
  billingAddress: { ... }
}
```

### One-to-Many Relationships

#### Embedding (One-to-Few)

When the "many" side is small and bounded:

```javascript
// Blog post with tags (one-to-few, embedded)
{
  _id: ObjectId("post-123"),
  title: "MongoLake Guide",
  content: "...",
  tags: ["database", "parquet", "analytics"],  // Few items
  author: {
    id: "author-456",
    name: "Alice"
  }
}
```

#### Referencing (One-to-Many)

When the "many" side is large or unbounded:

```javascript
// Author with articles (one-to-many, referenced)
// Authors collection
{
  _id: ObjectId("author-456"),
  name: "Alice Smith",
  articleCount: 150  // Denormalized count
}

// Articles collection
{
  _id: ObjectId("article-789"),
  authorId: ObjectId("author-456"),  // Reference to parent
  title: "Understanding Parquet",
  publishedAt: ISODate("2024-01-15")
}

// Query: Find all articles by author
db.articles.find({ authorId: ObjectId("author-456") })
```

#### Parent Reference Pattern

Store reference to parent in child documents:

```javascript
// Categories with products (parent reference)
// Categories collection
{
  _id: ObjectId("cat-electronics"),
  name: "Electronics",
  path: "/electronics"
}

// Products collection
{
  _id: ObjectId("prod-123"),
  name: "Laptop",
  categoryId: ObjectId("cat-electronics"),  // Parent reference
  categoryPath: "/electronics"  // Denormalized for queries
}
```

#### Array of References Pattern

Store child references in parent (for bounded relationships):

```javascript
// Course with enrolled students (array of references)
{
  _id: ObjectId("course-123"),
  title: "Data Modeling 101",
  instructor: "Alice Smith",
  enrolledStudents: [
    ObjectId("student-1"),
    ObjectId("student-2"),
    ObjectId("student-3")
  ],
  enrollmentCount: 3  // Denormalized count
}
```

### Many-to-Many Relationships

#### Two-Way Referencing

When both sides need efficient queries:

```javascript
// Students and courses (many-to-many)
// Students collection
{
  _id: ObjectId("student-123"),
  name: "Bob",
  enrolledCourseIds: [
    ObjectId("course-1"),
    ObjectId("course-2")
  ]
}

// Courses collection
{
  _id: ObjectId("course-1"),
  title: "Data Modeling",
  enrolledStudentIds: [
    ObjectId("student-123"),
    ObjectId("student-456")
  ]
}
```

#### Junction Collection Pattern

For relationships with attributes or very large cardinalities:

```javascript
// Enrollments junction collection
{
  _id: ObjectId("enrollment-789"),
  studentId: ObjectId("student-123"),
  courseId: ObjectId("course-1"),
  enrolledAt: ISODate("2024-01-15"),
  grade: "A",
  completed: true
}

// Indexes for efficient queries
db.enrollments.createIndex({ studentId: 1 })
db.enrollments.createIndex({ courseId: 1 })
db.enrollments.createIndex({ studentId: 1, courseId: 1 }, { unique: true })
```

### Hierarchical Data

#### Materialized Path Pattern

For tree structures with subtree queries:

```javascript
// Categories with materialized path
{
  _id: ObjectId("cat-123"),
  name: "Smartphones",
  path: "/electronics/phones/smartphones",
  ancestors: [
    ObjectId("cat-electronics"),
    ObjectId("cat-phones")
  ],
  parent: ObjectId("cat-phones"),
  depth: 3
}

// Find all descendants of "phones"
db.categories.find({ path: { $regex: "^/electronics/phones" } })

// Find all ancestors
db.categories.find({ _id: { $in: doc.ancestors } })
```

#### Nested Sets Pattern

For read-heavy hierarchies:

```javascript
// Categories with nested sets
{
  _id: ObjectId("cat-123"),
  name: "Smartphones",
  left: 4,   // Left boundary
  right: 7,  // Right boundary
  depth: 2
}

// Find all descendants
db.categories.find({
  left: { $gt: parent.left },
  right: { $lt: parent.right }
})
```

---

## Anti-Patterns to Avoid

### 1. Massive Arrays

**Problem**: Unbounded arrays that grow indefinitely cause document bloat and poor performance.

```javascript
// Anti-pattern: Unbounded activity log array
{
  _id: ObjectId("user-123"),
  activities: [
    // Can grow to thousands of entries
    { action: "login", timestamp: ISODate("...") },
    { action: "view", timestamp: ISODate("...") },
    // ... potentially millions more
  ]
}
```

**Solution**: Use a separate collection for unbounded data.

```javascript
// Activities collection
{
  _id: ObjectId("activity-456"),
  userId: ObjectId("user-123"),
  action: "login",
  timestamp: ISODate("2024-01-15T10:30:00Z")
}
```

### 2. Deep Nesting

**Problem**: Deeply nested documents are harder to query and update.

```javascript
// Anti-pattern: Deep nesting
{
  company: {
    departments: {
      engineering: {
        teams: {
          backend: {
            members: {
              lead: {
                name: "Alice",
                contact: {
                  email: "alice@..."
                }
              }
            }
          }
        }
      }
    }
  }
}
```

**Solution**: Flatten or use separate collections.

```javascript
// Better: Flat structure with references
{
  _id: ObjectId("member-123"),
  name: "Alice",
  email: "alice@...",
  role: "lead",
  teamId: ObjectId("team-backend"),
  departmentId: ObjectId("dept-eng"),
  companyId: ObjectId("company-1")
}
```

### 3. Mixed Types in Same Field

**Problem**: Inconsistent types prevent column promotion and complicate queries.

```javascript
// Anti-pattern: Mixed types
{ price: 99.99 }      // Number
{ price: "99.99" }    // String
{ price: null }       // Null
{ price: { amount: 99.99, currency: "USD" } }  // Object
```

**Solution**: Enforce consistent types.

```javascript
// Better: Consistent types with separate fields
{ price: 99.99, currency: "USD" }
{ price: 0, currency: "USD" }  // Use 0 instead of null if appropriate
```

### 4. Storing Computed Values Without Refresh Strategy

**Problem**: Denormalized data becomes stale without a refresh strategy.

```javascript
// Anti-pattern: Computed values with no refresh
{
  userId: "user-123",
  orderCount: 42,     // When was this last updated?
  totalSpent: 1500.00 // Is this accurate?
}
```

**Solution**: Document refresh strategy or use aggregation.

```javascript
// Option 1: Track last updated
{
  userId: "user-123",
  orderCount: 42,
  totalSpent: 1500.00,
  statsUpdatedAt: ISODate("2024-01-15")  // Track staleness
}

// Option 2: Compute on demand
const stats = await db.orders.aggregate([
  { $match: { userId: "user-123" } },
  { $group: { _id: null, count: { $sum: 1 }, total: { $sum: "$amount" } } }
]).toArray();
```

### 5. Over-Embedding for Write-Heavy Data

**Problem**: Embedding frequently updated data causes document rewrites.

```javascript
// Anti-pattern: Embedded counters
{
  postId: "post-123",
  content: "...",
  // These update frequently
  viewCount: 10542,
  likeCount: 234,
  commentCount: 56
}
```

**Solution**: Separate volatile data.

```javascript
// Posts collection (stable)
{
  _id: ObjectId("post-123"),
  content: "..."
}

// Post stats collection (volatile)
{
  _id: ObjectId("stats-123"),
  postId: ObjectId("post-123"),
  viewCount: 10542,
  likeCount: 234,
  commentCount: 56
}
```

### 6. Ignoring Column Promotion

**Problem**: Treating all fields equally loses columnar storage benefits.

```javascript
// Anti-pattern: No thought to field promotion
{
  // All fields stored in variant encoding
  metadata: {
    userId: "user-123",
    timestamp: ISODate("..."),
    category: "sales",
    amount: 99.99,
    region: "west",
    notes: "..."
  }
}
```

**Solution**: Structure for promotion.

```javascript
// Better: Top-level promotable fields
{
  // These can be promoted to columns
  userId: "user-123",
  timestamp: ISODate("..."),
  category: "sales",
  amount: 99.99,
  region: "west",
  // Less frequently queried
  metadata: {
    notes: "..."
  }
}
```

### 7. Storing Large Blobs in Documents

**Problem**: Large binary data (images, files) in documents degrades performance.

```javascript
// Anti-pattern: Large embedded blob
{
  _id: ObjectId("doc-123"),
  name: "report.pdf",
  content: Binary("...megabytes of data...")  // Bad!
}
```

**Solution**: Store references to external storage.

```javascript
// Better: Reference to object storage
{
  _id: ObjectId("doc-123"),
  name: "report.pdf",
  storageUrl: "r2://bucket/reports/doc-123.pdf",
  sizeBytes: 2048576,
  contentType: "application/pdf"
}
```

---

## Index Design Guidance

### B-Tree Index Principles

1. **Index fields used in filters** - Create indexes on fields that appear in `find()` and `$match`

```javascript
// Common query pattern
db.orders.find({ userId: "user-123", status: "pending" })

// Supporting index
db.orders.createIndex({ userId: 1, status: 1 })
```

2. **Consider field order** - Most selective field first for compound indexes

```javascript
// If status has few values but userId has many
db.orders.createIndex({ userId: 1, status: 1 })  // Better

// Not as good for this case
db.orders.createIndex({ status: 1, userId: 1 })
```

3. **Index for sorting** - Include sort fields in indexes

```javascript
// Query with sort
db.orders.find({ userId: "user-123" }).sort({ createdAt: -1 })

// Index covers both filter and sort
db.orders.createIndex({ userId: 1, createdAt: -1 })
```

### Index Types

| Index Type | Use Case | Example |
|------------|----------|---------|
| Single field | Simple equality/range queries | `{ email: 1 }` |
| Compound | Multi-field queries | `{ userId: 1, status: 1 }` |
| Unique | Enforce uniqueness | `{ email: 1 }, { unique: true }` |
| Sparse | Index only documents with field | `{ optionalField: 1 }, { sparse: true }` |
| Text | Full-text search | `{ title: "text", body: "text" }` |

### Index and Zone Map Synergy

Combine indexes with zone map-friendly data ordering:

```javascript
// 1. Create index for query filtering
db.events.createIndex({ userId: 1, timestamp: 1 })

// 2. Insert data sorted by timestamp for better zone maps
const events = fetchEvents().sort((a, b) => a.timestamp - b.timestamp);
await db.events.insertMany(events);

// 3. Query benefits from both index and zone map
db.events.find({
  userId: "user-123",
  timestamp: { $gte: ISODate("2024-01-01") }
})
```

### Index Maintenance

- **Monitor index usage** - Remove unused indexes
- **Avoid over-indexing** - Each index adds write overhead
- **Rebuild indexes periodically** - After major data changes

---

## Example Schemas

### E-Commerce Application

```javascript
// Users collection
{
  _id: ObjectId("user-123"),
  email: "alice@example.com",
  passwordHash: "...",
  profile: {
    firstName: "Alice",
    lastName: "Smith",
    phone: "+1-555-1234"
  },
  addresses: [
    {
      type: "shipping",
      street: "123 Main St",
      city: "Seattle",
      state: "WA",
      zip: "98101",
      country: "USA",
      isDefault: true
    }
  ],
  createdAt: ISODate("2024-01-01"),
  lastLoginAt: ISODate("2024-01-15")
}

// Products collection
{
  _id: ObjectId("prod-456"),
  sku: "WDG-001",
  name: "Premium Widget",
  description: "...",
  category: "electronics",
  subcategory: "gadgets",
  price: 99.99,
  currency: "USD",
  inventory: {
    quantity: 150,
    warehouse: "SEA-1"
  },
  attributes: {
    color: "blue",
    weight: 0.5,
    dimensions: { l: 10, w: 5, h: 3 }
  },
  tags: ["bestseller", "featured"],
  createdAt: ISODate("2024-01-01"),
  updatedAt: ISODate("2024-01-15")
}

// Orders collection
{
  _id: ObjectId("order-789"),
  orderNumber: "ORD-2024-0001",
  userId: ObjectId("user-123"),
  status: "shipped",
  items: [
    {
      productId: ObjectId("prod-456"),
      sku: "WDG-001",
      name: "Premium Widget",
      quantity: 2,
      unitPrice: 99.99,
      totalPrice: 199.98
    }
  ],
  subtotal: 199.98,
  tax: 20.00,
  shipping: 9.99,
  total: 229.97,
  shippingAddress: {
    street: "123 Main St",
    city: "Seattle",
    state: "WA",
    zip: "98101"
  },
  createdAt: ISODate("2024-01-15"),
  shippedAt: ISODate("2024-01-16")
}

// Indexes for e-commerce
db.users.createIndex({ email: 1 }, { unique: true })
db.products.createIndex({ sku: 1 }, { unique: true })
db.products.createIndex({ category: 1, subcategory: 1 })
db.products.createIndex({ name: "text", description: "text" })
db.orders.createIndex({ userId: 1, createdAt: -1 })
db.orders.createIndex({ orderNumber: 1 }, { unique: true })
db.orders.createIndex({ status: 1, createdAt: -1 })
```

### Social Media Application

```javascript
// Users collection
{
  _id: ObjectId("user-123"),
  username: "alice_dev",
  email: "alice@example.com",
  displayName: "Alice Smith",
  bio: "Software engineer | Coffee enthusiast",
  avatarUrl: "https://...",
  followerCount: 1542,
  followingCount: 234,
  postCount: 89,
  createdAt: ISODate("2023-06-15"),
  lastActiveAt: ISODate("2024-01-15")
}

// Posts collection
{
  _id: ObjectId("post-456"),
  authorId: ObjectId("user-123"),
  authorUsername: "alice_dev",  // Denormalized for display
  content: "Just shipped a new feature! #coding #mongodb",
  mediaUrls: ["https://..."],
  hashtags: ["coding", "mongodb"],
  mentions: [ObjectId("user-789")],
  likeCount: 42,
  commentCount: 8,
  repostCount: 3,
  visibility: "public",
  createdAt: ISODate("2024-01-15T10:30:00Z")
}

// Follows collection (junction)
{
  _id: ObjectId("follow-789"),
  followerId: ObjectId("user-123"),
  followeeId: ObjectId("user-456"),
  createdAt: ISODate("2024-01-10")
}

// Comments collection
{
  _id: ObjectId("comment-abc"),
  postId: ObjectId("post-456"),
  authorId: ObjectId("user-789"),
  authorUsername: "bob_tech",
  content: "Congrats! What tech stack?",
  likeCount: 5,
  replyToId: null,  // null for top-level, ObjectId for replies
  createdAt: ISODate("2024-01-15T11:00:00Z")
}

// Likes collection (junction)
{
  _id: ObjectId("like-def"),
  userId: ObjectId("user-789"),
  targetType: "post",  // "post" or "comment"
  targetId: ObjectId("post-456"),
  createdAt: ISODate("2024-01-15T10:35:00Z")
}

// Indexes for social media
db.users.createIndex({ username: 1 }, { unique: true })
db.users.createIndex({ email: 1 }, { unique: true })
db.posts.createIndex({ authorId: 1, createdAt: -1 })
db.posts.createIndex({ hashtags: 1, createdAt: -1 })
db.posts.createIndex({ createdAt: -1 })  // For global feed
db.follows.createIndex({ followerId: 1, followeeId: 1 }, { unique: true })
db.follows.createIndex({ followeeId: 1 })  // For follower queries
db.comments.createIndex({ postId: 1, createdAt: 1 })
db.likes.createIndex({ userId: 1, targetType: 1, targetId: 1 }, { unique: true })
db.likes.createIndex({ targetType: 1, targetId: 1 })
```

### IoT / Time Series Application

```javascript
// Devices collection
{
  _id: ObjectId("device-123"),
  deviceId: "sensor-temp-001",
  type: "temperature",
  location: {
    building: "HQ",
    floor: 3,
    room: "server-room-1"
  },
  metadata: {
    manufacturer: "SensorCorp",
    model: "TC-2000",
    firmwareVersion: "2.1.0"
  },
  status: "active",
  lastSeenAt: ISODate("2024-01-15T12:00:00Z"),
  registeredAt: ISODate("2023-01-01")
}

// Readings collection (time series optimized)
{
  _id: ObjectId("reading-456"),
  deviceId: "sensor-temp-001",
  timestamp: ISODate("2024-01-15T12:00:00Z"),
  // Promoted fields for analytics
  temperature: 72.5,
  humidity: 45.2,
  pressure: 1013.25,
  // Metadata
  unit: "fahrenheit",
  quality: "good"
}

// Alerts collection
{
  _id: ObjectId("alert-789"),
  deviceId: "sensor-temp-001",
  type: "threshold_exceeded",
  severity: "warning",
  message: "Temperature above 80F",
  value: 82.5,
  threshold: 80.0,
  triggeredAt: ISODate("2024-01-15T14:30:00Z"),
  acknowledgedAt: null,
  acknowledgedBy: null,
  resolvedAt: null
}

// Aggregated metrics (pre-computed for dashboards)
{
  _id: ObjectId("metric-abc"),
  deviceId: "sensor-temp-001",
  period: "hourly",
  timestamp: ISODate("2024-01-15T12:00:00Z"),
  temperature: {
    min: 71.2,
    max: 74.8,
    avg: 72.5,
    count: 60
  },
  humidity: {
    min: 44.0,
    max: 46.5,
    avg: 45.2,
    count: 60
  }
}

// Indexes for IoT
db.devices.createIndex({ deviceId: 1 }, { unique: true })
db.devices.createIndex({ type: 1, status: 1 })
db.devices.createIndex({ "location.building": 1, "location.floor": 1 })

// Time series indexes - critical for performance
db.readings.createIndex({ deviceId: 1, timestamp: -1 })
db.readings.createIndex({ timestamp: -1 })  // For global queries

db.alerts.createIndex({ deviceId: 1, triggeredAt: -1 })
db.alerts.createIndex({ severity: 1, acknowledgedAt: 1 })

db.metrics.createIndex({ deviceId: 1, period: 1, timestamp: -1 })
```

---

## Performance Implications

### Schema Design Impact on Performance

| Design Choice | Write Performance | Read Performance | Analytics |
|---------------|-------------------|------------------|-----------|
| Flat documents | Slower (more columns) | Faster (direct access) | Excellent |
| Deep nesting | Faster (single variant) | Slower (parsing) | Poor |
| Embedding | Faster (single write) | Faster (no joins) | Moderate |
| References | Slower (multiple writes) | Slower (requires $lookup) | Excellent |
| Column promotion | Slower (schema management) | Faster (native columns) | Excellent |

### Optimizing for Different Workloads

#### Write-Heavy Workloads

- Use fewer indexes
- Consider append-only patterns
- Batch writes with `insertMany()`
- Avoid deeply nested updates

```javascript
// Efficient batch writes
const documents = generateDocuments(1000);
await collection.insertMany(documents, { ordered: false });
```

#### Read-Heavy Workloads

- Promote frequently queried fields
- Create covering indexes
- Denormalize for common access patterns
- Sort data for zone map effectiveness

#### Analytics Workloads

- Use flat, wide schemas
- Promote all analytical dimensions
- Pre-aggregate common metrics
- Partition by time for range queries

### Measuring Performance

Monitor these metrics to evaluate schema effectiveness:

1. **Query execution time** - Use explain() to understand query plans
2. **Zone map skip rate** - Higher is better for analytical queries
3. **Index usage** - Verify indexes are being used
4. **Document size** - Monitor average document size
5. **Write throughput** - Track inserts per second

```typescript
// Check query plan
const plan = await queryPlanner.createPlan('orders', {
  userId: 'user-123',
  status: 'pending'
});

console.log('Strategy:', plan.strategy);
console.log('Index:', plan.indexName);
console.log('Selectivity:', plan.estimatedSelectivity);
```

---

## Summary

Effective data modeling in MongoLake requires balancing:

1. **Document flexibility** - Use MongoDB's document model for operational needs
2. **Columnar efficiency** - Promote fields for analytical performance
3. **Zone map optimization** - Structure data for predicate pushdown
4. **Index design** - Create indexes that support your query patterns

Key principles:
- **Promote frequently queried fields** to native Parquet columns
- **Use consistent types** for the same field across documents
- **Embed for locality**, reference for scalability
- **Sort data by time or clustering key** for zone map effectiveness
- **Design for your access patterns**, not just data relationships

For query optimization techniques, see the [Query Optimization Guide](./QUERY_OPTIMIZATION.md).
For architecture details, see the [Architecture Guide](./ARCHITECTURE.md).
