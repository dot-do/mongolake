# Changelog

All notable changes to MongoLake will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- API versioning documentation and guidelines
- Comprehensive deprecation policy

## [0.1.0] - 2026-02-01

Initial public release of MongoLake - MongoDB reimagined for the lakehouse era.

### Added

#### Core Client API
- `MongoLake` class - Main client entry point
- `createClient()` factory function for creating isolated client instances
- `createDatabase()` factory function for direct database access
- MongoDB connection string support (`mongodb://` and `mongodb+srv://` schemes)
- `Database` class with collection management
- `Collection` class with full CRUD operations

#### Document Operations
- `insertOne()` - Insert a single document
- `insertMany()` - Insert multiple documents with batch validation
- `findOne()` - Find a single document
- `find()` - Find documents with cursor support
- `updateOne()` - Update a single document
- `updateMany()` - Update multiple documents
- `replaceOne()` - Replace a single document
- `deleteOne()` - Delete a single document
- `deleteMany()` - Delete multiple documents
- `countDocuments()` - Count documents matching a filter
- `estimatedDocumentCount()` - Fast approximate count
- `distinct()` - Get distinct field values

#### Query Support
- Full filter operators: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`
- Logical operators: `$and`, `$or`, `$nor`, `$not`
- Element operators: `$exists`, `$type`
- Array operators: `$all`, `$elemMatch`, `$size`
- Regex support via `$regex`
- Projection support for field selection
- Sort, skip, and limit options

#### Update Support
- Field operators: `$set`, `$unset`, `$setOnInsert`
- Numeric operators: `$inc`, `$mul`, `$min`, `$max`
- Array operators: `$push`, `$pull`, `$addToSet`, `$pop`
- Field rename via `$rename`
- `$currentDate` for timestamp updates
- Upsert support for updateOne/updateMany/replaceOne

#### Aggregation Pipeline
- `$match` - Filter documents
- `$group` - Group with accumulators (`$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`, `$push`, `$addToSet`, `$count`)
- `$project` - Field projection
- `$sort` - Sort documents
- `$limit` - Limit results
- `$skip` - Skip results
- `$unwind` - Unwind arrays
- `$lookup` - Join collections (including pipeline lookups)
- `$count` - Count documents
- `$addFields` / `$set` - Add computed fields
- `$unset` - Remove fields
- `$facet` - Multiple pipelines
- `$bucket` / `$bucketAuto` - Bucketing
- `$graphLookup` - Recursive graph traversal
- `$sample` - Random sampling
- `$sortByCount` - Sort by frequency
- `$merge` / `$out` - Output stages
- `$redact` - Document-level access control
- `$replaceRoot` / `$replaceWith` - Document replacement

#### Indexing
- B-tree indexes for single and compound fields
- Unique indexes
- Sparse indexes
- Text indexes for full-text search
- `createIndex()` / `createIndexes()`
- `dropIndex()`
- `listIndexes()`
- Automatic `_id` index creation

#### Cursors
- `FindCursor` with method chaining (sort, limit, skip, project)
- `AggregationCursor` for pipeline results
- `toArray()` - Get all results
- `forEach()` - Iterate documents
- `map()` - Transform documents
- `hasNext()` / `next()` - Manual iteration
- Async iterator support (`for await...of`)

#### Change Streams
- `watch()` method for real-time change notifications
- Operation types: insert, update, replace, delete
- Pipeline filtering for change events
- `fullDocument: 'updateLookup'` option
- `updateDescription` with changed/removed fields

#### Time Travel
- `asOf(timestamp)` - Query historical data by timestamp
- `atSnapshot(snapshotId)` - Query by Iceberg snapshot ID
- Read-only historical views via `TimeTravelCollection`

#### Branching (Experimental)
- `BranchStore` for branch metadata management
- `BranchManager` for branch operations
- `MergeEngine` for branch merging
- `BranchCollection` for branch-specific operations
- Create, list, update, delete branches
- Merge strategies: ours, theirs, manual

#### Session and Transaction Support
- `startSession()` for client sessions
- `startTransaction()` / `commitTransaction()` / `abortTransaction()`
- Buffered operations with atomic commit
- Rollback support for failed transactions
- `TransactionManager` for coordinated transactions
- `runTransaction()` helper function

#### Storage Backends
- `FileSystemStorage` - Local file system for development
- `R2Storage` - Cloudflare R2 for production
- `S3Storage` - S3-compatible storage
- `MemoryStorage` - In-memory for testing
- Multipart upload support for large files
- Range request support for partial reads

#### Parquet Integration
- Native Parquet file format storage
- Variant encoding for schema flexibility
- Column promotion for frequently-queried fields
- Zone maps for predicate pushdown
- Streaming writer for large datasets
- Compression support (Snappy, Gzip, Zstd)

#### Iceberg Integration
- Apache Iceberg table format metadata
- Snapshot management
- Time travel queries
- Schema evolution support
- Catalog integration (REST, R2 Data Catalog)

#### Wire Protocol
- MongoDB wire protocol server
- Support for mongosh and MongoDB Compass
- Compatible with MongoDB drivers
- OP_MSG and OP_QUERY support
- Cursor management for large result sets

#### Cloudflare Workers
- Durable Object-based sharding
- WAL (Write-Ahead Log) for durability
- Automatic buffer flushing
- Background compaction via alarms
- Distributed aggregation support

#### Utilities
- `ObjectId` class compatible with MongoDB ObjectIds
- Connection string parsing and building
- Input validation for all operations
- Corruption handling with skip option
- Comprehensive logging

#### Mongoose Compatibility
- MongoLake-compatible Mongoose driver
- Schema and model support
- Plugin system compatibility

### Security
- Database and collection name validation (path traversal prevention)
- Document validation
- Service binding authentication support
- OAuth integration ready

### Notes
- This is an alpha release focused on API compatibility
- Performance optimizations ongoing
- Some MongoDB features not yet implemented (see Migration Guide)

---

## Migration Notes

### From MongoDB

See [MIGRATION_FROM_MONGODB.md](./MIGRATION_FROM_MONGODB.md) for comprehensive migration guidance.

Key differences:
- Import from `mongolake` instead of `mongodb`
- Use `MongoLake` class instead of `MongoClient`
- No connection required (uses storage backends directly)
- Transactions are session-based with application-level rollback
- GridFS not supported (use R2/S3 directly)

### API Stability

For version 0.x releases:
- The API is considered unstable and may change
- Breaking changes will be documented in the changelog
- Deprecation warnings will be provided when possible
- Upgrade guides will be provided for significant changes

---

## Version History

| Version | Date | Status |
|---------|------|--------|
| 0.1.0 | 2026-02-01 | Current |

---

## Deprecation Schedule

No deprecations announced yet.

Upcoming deprecations will be listed here with:
- Feature being deprecated
- Deprecation version
- Removal version
- Migration path

---

## Breaking Changes by Version

### 0.1.0
- Initial release, no breaking changes from previous versions.

---

## Links

- [API Versioning Guide](./API_VERSIONING.md)
- [Migration from MongoDB](./MIGRATION_FROM_MONGODB.md)
- [GitHub Releases](https://github.com/dot-do/mongolake/releases)
- [API Reference](./api/client.md)
