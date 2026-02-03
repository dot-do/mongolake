# MongoLake Architecture

This document describes the internal architecture of MongoLake, a MongoDB-compatible database that stores data as Parquet files for lakehouse integration.

## Table of Contents

- [System Overview](#system-overview)
- [High-Level Architecture](#high-level-architecture)
- [LSM-Tree Storage Architecture](#lsm-tree-storage-architecture)
- [Sharding and Routing](#sharding-and-routing)
- [Durable Objects Architecture](#durable-objects-architecture)
- [Data Flow](#data-flow)
- [Component Interactions](#component-interactions)
- [Wire Protocol](#wire-protocol)
- [Storage Layer](#storage-layer)
- [Iceberg Integration](#iceberg-integration)

---

## System Overview

MongoLake is a MongoDB-compatible database designed for the lakehouse era. It provides:

- **MongoDB API compatibility** - Familiar CRUD operations (insertOne, find, update, delete)
- **Parquet storage** - Data stored as queryable Parquet files
- **Cloudflare Workers deployment** - Global edge deployment with Durable Objects
- **Lakehouse integration** - Compatible with DuckDB, Spark, and other analytical tools
- **Iceberg metadata** - Optional Apache Iceberg table format support

### Key Design Principles

1. **Write-optimized with read efficiency** - LSM-tree architecture for fast writes, Parquet for efficient analytical reads
2. **Durability first** - Write-Ahead Log (WAL) ensures no data loss
3. **Globally distributed** - Sharding across Durable Objects for horizontal scaling
4. **Schema flexibility** - Variant encoding for schema-less fields with optional column promotion

---

## High-Level Architecture

```
                              ┌─────────────────────────────────────────────────────────┐
                              │                      Clients                             │
                              ├──────────┬──────────┬──────────┬──────────┬─────────────┤
                              │ mongosh  │ Compass  │ Drivers  │   SDK    │ DuckDB/Spark│
                              └────┬─────┴────┬─────┴────┬─────┴────┬─────┴──────┬──────┘
                                   │          │          │          │             │
                                   └──────┬───┴──────────┘          │             │
                                          │                         │             │
                              Wire Protocol (TCP)             RPC (WebSocket)     │
                                          │                         │             │
                                          ▼                         ▼             │
                              ┌─────────────────────┐    ┌───────────────────┐    │
                              │   mongolake CLI     │    │ Cloudflare Worker │    │
                              │   (Bun runtime)     │    │  (HTTP/RPC API)   │    │
                              └─────────┬───────────┘    └─────────┬─────────┘    │
                                        │                          │              │
                                        └────────────┬─────────────┘              │
                                                     │                            │
                                    ┌────────────────▼────────────────┐           │
                                    │         Shard Router            │           │
                                    │  (Consistent Hashing, Caching)  │           │
                                    └────────────────┬────────────────┘           │
                                                     │                            │
               ┌────────────────┬────────────────────┼────────────────────┬───────┼──────┐
               │                │                    │                    │       │      │
               ▼                ▼                    ▼                    ▼       │      │
        ┌────────────┐   ┌────────────┐       ┌────────────┐       ┌────────────┐ │      │
        │  ShardDO   │   │  ShardDO   │  ...  │  ShardDO   │  ...  │  ShardDO   │ │      │
        │  (Shard 0) │   │  (Shard 1) │       │  (Shard N) │       │ (Shard 15) │ │      │
        └─────┬──────┘   └─────┬──────┘       └─────┬──────┘       └─────┬──────┘ │      │
              │                │                    │                    │        │      │
              └────────────────┴────────────────────┴────────────────────┘        │      │
                                                     │                            │      │
                                    ┌────────────────▼────────────────┐           │      │
                                    │       R2 Object Storage         │◄──────────┘      │
                                    │   (Parquet + Iceberg Metadata)  │                  │
                                    └─────────────────────────────────┘                  │
                                                     ▲                                   │
                                                     │  Direct Parquet Access            │
                                                     └───────────────────────────────────┘
```

---

## LSM-Tree Storage Architecture

MongoLake implements a Log-Structured Merge (LSM) tree-inspired architecture optimized for write-heavy workloads while maintaining efficient read access.

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            Write Request                                     │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          In-Memory Buffer                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  BufferManager                                                       │    │
│  │  - Map<collection, Map<docId, BufferedDoc>>                         │    │
│  │  - Tracks buffer size in bytes                                       │    │
│  │  - Tracks pending deletions                                          │    │
│  │  - Configurable flush thresholds (1MB bytes / 1000 docs)            │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                           ◄── Fast reads from buffer                         │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │ (threshold exceeded or WAL limit)
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SQLite WAL                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  WalManager                                                          │    │
│  │  - Write-Ahead Log in Durable Object SQLite                         │    │
│  │  - LSN (Log Sequence Number) tracking                                │    │
│  │  - Crash recovery from WAL on startup                                │    │
│  │  - Max 10MB or 10,000 entries before forced flush                   │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                           ◄── Durability guarantee                           │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │ (flush to R2)
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          R2 Parquet Files                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  IndexManager                                                        │    │
│  │  - Two-phase commit for atomic writes                                │    │
│  │  - Manifest tracking per collection                                  │    │
│  │  - Zone maps for predicate pushdown                                  │    │
│  │  - Parquet files with variant encoding                               │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                           ◄── Long-term storage                              │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │ (background compaction)
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Merged Parquet Files                                │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  CompactionScheduler                                                 │    │
│  │  - Merges small blocks (<2MB) into larger blocks (4MB target)       │    │
│  │  - Triggered by Durable Object alarms                                │    │
│  │  - Incremental processing (max 10 blocks per run)                   │    │
│  │  - Maintains field and column statistics                             │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                           ◄── Optimized for analytical reads                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

### WAL Manager

The WAL Manager (`src/do/shard/wal-manager.ts`) provides durability guarantees:

```typescript
interface WalEntry {
  lsn: number;           // Log Sequence Number
  collection: string;    // Collection name
  op: 'i' | 'u' | 'd';  // Operation type (insert/update/delete)
  docId: string;         // Document ID
  document: object;      // Document content
  flushed: boolean;      // Whether flushed to R2
}
```

**Key responsibilities:**
- Persisting WAL entries to SQLite for crash recovery
- Managing LSN allocation and tracking
- Checkpointing (removing flushed entries)
- Enforcing WAL size limits (10MB / 10,000 entries)

### Buffer Manager

The Buffer Manager (`src/do/shard/buffer-manager.ts`) handles in-memory document storage:

**Features:**
- O(1) document lookup by collection and ID
- Configurable flush thresholds
- Tracks pending deletions
- Memory-efficient size estimation

### Parquet Storage

Documents are stored in Parquet format with:

1. **Promoted columns** - Frequently accessed fields as native Parquet columns
2. **Variant encoding** - Schema-less fields encoded in `_data` column
3. **Zone maps** - Min/max statistics for predicate pushdown
4. **Row group batching** - 64MB target row groups for streaming writes

---

## Sharding and Routing

MongoLake distributes data across multiple shards (default: 16) using consistent hashing.

### Shard Router Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            Shard Router                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Consistent Hashing (MurmurHash3-inspired)                          │    │
│  │  - Collection-level routing: hash(collection) % shardCount          │    │
│  │  - Document-level routing: hash(documentId) % shardCount            │    │
│  │  - Database-prefixed routing: hash(db.collection) % shardCount      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  LRU Cache (10,000 entries default)                                 │    │
│  │  - Caches shard assignments for fast lookups                         │    │
│  │  - Tracks cache hits/misses for debugging                            │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Affinity Hints                                                      │    │
│  │  - Force specific collections to preferred shards                    │    │
│  │  - Useful for co-locating related data                               │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Collection Splitting                                                │    │
│  │  - Split hot collections across multiple shards                      │    │
│  │  - Document-level routing within split shards                        │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
        ┌──────────────────────────┼──────────────────────────┐
        │                          │                          │
        ▼                          ▼                          ▼
   ┌─────────┐               ┌─────────┐               ┌─────────┐
   │ Shard 0 │               │ Shard 1 │      ...      │Shard 15 │
   └─────────┘               └─────────┘               └─────────┘
```

### Routing Flow

```typescript
// Collection routing (default)
const assignment = router.route('users');
// Returns: { shardId: 7, collection: 'users' }

// Database-prefixed routing
const assignment = router.routeWithDatabase('myapp', 'users');
// Returns: { shardId: 12, collection: 'users', database: 'myapp' }

// Document-level routing (for split collections)
const assignment = router.routeDocument('users', 'user-123');
// Returns: { shardId: 3, collection: 'users', documentId: 'user-123' }
```

---

## Durable Objects Architecture

Each shard is managed by a ShardDO (Durable Object) that provides single-writer semantics and strong consistency.

### ShardDO Structure

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              ShardDO                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Constructor (blockConcurrencyWhile)                                │    │
│  │  - Initializes all managers                                          │    │
│  │  - Recovers state from SQLite WAL                                    │    │
│  │  - Recovers pending flushes from R2                                  │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐  ┌───────────┐    │
│  │  WalManager   │  │ BufferManager │  │ IndexManager  │  │  Query    │    │
│  │               │  │               │  │               │  │ Executor  │    │
│  │ - LSN tracking│  │ - Doc buffer  │  │ - Manifests   │  │           │    │
│  │ - Persistence │  │ - Size limits │  │ - File ops    │  │ - find()  │    │
│  │ - Recovery    │  │ - Deletions   │  │ - Two-phase   │  │ - Merge   │    │
│  │ - Checkpoint  │  │               │  │   commit      │  │   results │    │
│  └───────────────┘  └───────────────┘  └───────────────┘  └───────────┘    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  CompactionService                                                   │    │
│  │  - Triggered by DO alarm handler                                     │    │
│  │  - Runs background compaction                                        │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  HTTP Interface                                                      │    │
│  │  - /write     POST  - Execute write operations                       │    │
│  │  - /find      POST  - Query documents                                │    │
│  │  - /findOne   POST  - Query single document                          │    │
│  │  - /flush     POST  - Force buffer flush                             │    │
│  │  - /status    GET   - Shard status                                   │    │
│  │  - /metrics   GET   - Prometheus metrics                             │    │
│  │  - /wal       POST  - WAL entries for replication                    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Write Operation Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Write Operation                                    │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                        1. Acquire Write Lock (Mutex)
                                   │
                        2. Validate Operation
                                   │
                        3. Allocate LSN
                                   │
                        4. Extract Document & ID
                                   │
                        5. Create BufferedDoc
                                   │
                        6. Persist to WAL (SQLite)
                                   │
                        7. Update Buffer
                                   │
                        8. Record Metrics
                                   │
                        9. Check Auto-Flush
                                   │
                       10. Release Lock & Return Result
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  WriteResult { acknowledged, insertedId?, lsn, readToken }                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Query Execution Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            find(collection, filter, options)                 │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    │                             │
                    ▼                             ▼
         ┌─────────────────────┐       ┌─────────────────────┐
         │   Buffer Query      │       │    R2 Query         │
         │                     │       │                     │
         │ - Check buffer      │       │ - Load manifest     │
         │ - Filter in-memory  │       │ - Filter by zone    │
         │ - Apply deletions   │       │   maps              │
         │                     │       │ - Read Parquet      │
         └─────────┬───────────┘       │   files             │
                   │                   │ - Apply filter      │
                   │                   └─────────┬───────────┘
                   │                             │
                   └──────────────┬──────────────┘
                                  │
                       Merge & Deduplicate
                                  │
                       Apply Sort/Limit/Skip
                                  │
                       Apply Projection
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Result Documents                                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

### Write Path (Detailed)

```
Client                    Worker/CLI            ShardDO                    R2
  │                          │                     │                       │
  │  insertOne(doc)          │                     │                       │
  │────────────────────────▶│                     │                       │
  │                          │                     │                       │
  │                          │  route(collection)  │                       │
  │                          │──────────┐          │                       │
  │                          │          │          │                       │
  │                          │◀─────────┘          │                       │
  │                          │  shardId            │                       │
  │                          │                     │                       │
  │                          │  write(op)          │                       │
  │                          │───────────────────▶│                       │
  │                          │                     │                       │
  │                          │                     │  allocate LSN         │
  │                          │                     │──────────┐            │
  │                          │                     │          │            │
  │                          │                     │◀─────────┘            │
  │                          │                     │                       │
  │                          │                     │  persist to SQLite    │
  │                          │                     │──────────┐            │
  │                          │                     │          │            │
  │                          │                     │◀─────────┘            │
  │                          │                     │                       │
  │                          │                     │  add to buffer        │
  │                          │                     │──────────┐            │
  │                          │                     │          │            │
  │                          │                     │◀─────────┘            │
  │                          │                     │                       │
  │                          │  WriteResult        │                       │
  │                          │◀───────────────────│                       │
  │                          │                     │                       │
  │  { acknowledged, lsn }   │                     │                       │
  │◀────────────────────────│                     │                       │
  │                          │                     │                       │
  │                          │                     │  [if threshold met]   │
  │                          │                     │                       │
  │                          │                     │  flush buffer         │
  │                          │                     │─────────────────────▶│
  │                          │                     │                       │
  │                          │                     │  write Parquet file   │
  │                          │                     │◀─────────────────────│
  │                          │                     │                       │
  │                          │                     │  update manifest      │
  │                          │                     │──────────┐            │
  │                          │                     │          │            │
  │                          │                     │◀─────────┘            │
  │                          │                     │                       │
  │                          │                     │  schedule compaction  │
  │                          │                     │──────────┐            │
  │                          │                     │          │            │
  │                          │                     │◀─────────┘            │
```

### Read Path (Detailed)

```
Client                    Worker/CLI            ShardDO                    R2
  │                          │                     │                       │
  │  find(filter)            │                     │                       │
  │────────────────────────▶│                     │                       │
  │                          │                     │                       │
  │                          │  route(collection)  │                       │
  │                          │──────────┐          │                       │
  │                          │          │          │                       │
  │                          │◀─────────┘          │                       │
  │                          │  shardId            │                       │
  │                          │                     │                       │
  │                          │  find(filter)       │                       │
  │                          │───────────────────▶│                       │
  │                          │                     │                       │
  │                          │                     │  query buffer         │
  │                          │                     │──────────┐            │
  │                          │                     │          │            │
  │                          │                     │◀─────────┘            │
  │                          │                     │  buffer results       │
  │                          │                     │                       │
  │                          │                     │  get manifest         │
  │                          │                     │──────────┐            │
  │                          │                     │          │            │
  │                          │                     │◀─────────┘            │
  │                          │                     │  file list            │
  │                          │                     │                       │
  │                          │                     │  [for each file]      │
  │                          │                     │                       │
  │                          │                     │  check zone maps      │
  │                          │                     │──────────┐            │
  │                          │                     │          │            │
  │                          │                     │◀─────────┘            │
  │                          │                     │  [if might match]     │
  │                          │                     │                       │
  │                          │                     │  read Parquet         │
  │                          │                     │─────────────────────▶│
  │                          │                     │                       │
  │                          │                     │  row groups           │
  │                          │                     │◀─────────────────────│
  │                          │                     │                       │
  │                          │                     │  apply filter         │
  │                          │                     │──────────┐            │
  │                          │                     │          │            │
  │                          │                     │◀─────────┘            │
  │                          │                     │                       │
  │                          │                     │  merge results        │
  │                          │                     │──────────┐            │
  │                          │                     │          │            │
  │                          │                     │◀─────────┘            │
  │                          │                     │                       │
  │                          │                     │  deduplicate          │
  │                          │                     │──────────┐            │
  │                          │                     │          │            │
  │                          │                     │◀─────────┘            │
  │                          │                     │                       │
  │                          │  documents[]        │                       │
  │                          │◀───────────────────│                       │
  │                          │                     │                       │
  │  documents[]             │                     │                       │
  │◀────────────────────────│                     │                       │
```

---

## Component Interactions

### Module Dependencies

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                             src/index.ts                                     │
│                          (Public API Entry)                                  │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
          ┌────────────────────────┼────────────────────────┐
          │                        │                        │
          ▼                        ▼                        ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   src/do/       │     │ src/storage/    │     │ src/wire-       │
│                 │     │                 │     │  protocol/      │
│ - shard.ts      │     │ - index.ts      │     │                 │
│ - shard/        │     │ - s3.ts         │     │ - tcp-server.ts │
│   - wal-manager │     │ - range-handler │     │ - command-*     │
│   - buffer-*    │     │                 │     │ - bson-*        │
│   - index-*     │     └────────┬────────┘     └────────┬────────┘
│   - query-*     │              │                       │
│   - compaction-*│              │                       │
│   - replica.*   │              │                       │
└────────┬────────┘              │                       │
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
          ┌──────────────────────┼──────────────────────┐
          │                      │                      │
          ▼                      ▼                      ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│  src/parquet/   │   │   src/shard/    │   │  src/iceberg/   │
│                 │   │                 │   │                 │
│ - streaming-*   │   │ - router.ts     │   │ - manifest-*    │
│ - variant.ts    │   │                 │   │ - snapshot-*    │
│ - compression   │   │                 │   │ - metadata-*    │
│ - zone-map      │   │                 │   │ - catalog-*     │
│ - footer-parser │   │                 │   │                 │
└─────────────────┘   └─────────────────┘   └─────────────────┘
          │                      │                      │
          └──────────────────────┼──────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            src/utils/                                        │
│  - filter.ts, sort.ts, projection.ts, nested.ts, validation.ts              │
│  - lru-cache.ts, zone-map-filter.ts, connection-string.ts, logger.ts        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Component Responsibilities

| Component | File(s) | Responsibility |
|-----------|---------|----------------|
| **ShardDO** | `src/do/shard/index.ts` | Write coordination, query execution, flush orchestration |
| **WalManager** | `src/do/shard/wal-manager.ts` | Durability via SQLite WAL, crash recovery |
| **BufferManager** | `src/do/shard/buffer-manager.ts` | In-memory document buffer, flush threshold detection |
| **IndexManager** | `src/do/shard/index-manager.ts` | Manifest management, two-phase commit for R2 writes |
| **QueryExecutor** | `src/do/shard/query-executor.ts` | Query execution across buffer and R2 |
| **CompactionScheduler** | `src/compaction/scheduler.ts` | Background block merging |
| **ShardRouter** | `src/shard/router.ts` | Consistent hashing, shard assignment |
| **StreamingParquetWriter** | `src/parquet/streaming-writer.ts` | Large Parquet file generation |
| **Storage** | `src/storage/index.ts` | Abstraction over R2, filesystem, memory |

---

## Wire Protocol

MongoLake implements the MongoDB wire protocol for compatibility with mongosh, Compass, and drivers.

### Protocol Stack

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         TCP Connection (port 27017)                          │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      StreamingMessageParser                                  │
│  - Parses wire protocol messages from byte stream                           │
│  - Handles message fragmentation                                             │
│  - Supports OP_MSG, OP_QUERY, OP_COMPRESSED                                 │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Command Decoder                                       │
│  - Type-safe command parsing with discriminated unions                      │
│  - Validates command structure                                               │
│  - Extracts command parameters                                               │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Command Router                                        │
│  - Routes to appropriate handler (find, insert, update, etc.)               │
│  - Handles admin commands (hello, ping, listDatabases)                      │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Command Handlers                                      │
│  - Execute operations against ShardDO                                        │
│  - Build wire protocol responses                                             │
│  - Manage cursors for pagination                                             │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        BSON Serializer                                       │
│  - Serializes responses to BSON format                                       │
│  - Builds OP_MSG/OP_REPLY responses                                          │
│  - Buffer pooling for performance                                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Supported Commands

| Category | Commands |
|----------|----------|
| **CRUD** | find, findOne, insert, update, delete, aggregate |
| **Cursors** | getMore, killCursors |
| **Admin** | hello, isMaster, ping, listDatabases, listCollections |
| **Index** | createIndexes, dropIndexes, listIndexes |
| **Collection** | create, drop, dropDatabase |

---

## Storage Layer

### Storage Backends

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        StorageBackend Interface                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  get(key): Promise<Uint8Array | null>                                 │  │
│  │  put(key, data): Promise<void>                                        │  │
│  │  delete(key): Promise<void>                                           │  │
│  │  list(prefix): Promise<string[]>                                      │  │
│  │  exists(key): Promise<boolean>                                        │  │
│  │  head(key): Promise<{size} | null>                                    │  │
│  │  createMultipartUpload(key): Promise<MultipartUpload>                 │  │
│  │  getStream(key): Promise<ReadableStream | null>                       │  │
│  │  putStream(key, stream): Promise<void>                                │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
         ┌─────────────────────────┼─────────────────────────┐
         │                         │                         │
         ▼                         ▼                         ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  R2Storage      │     │FileSystemStorage│     │  MemoryStorage  │
│                 │     │                 │     │                 │
│  Production     │     │  Local Dev      │     │    Testing      │
│  (Cloudflare)   │     │  (.mongolake/)  │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                   │
                                   │
                                   ▼
                        ┌─────────────────┐
                        │   S3Storage     │
                        │   (Optional)    │
                        │                 │
                        │ AWS S3 or any   │
                        │ S3-compatible   │
                        └─────────────────┘
```

### File Layout

```
.mongolake/                          # Local development
├── myapp/                           # Database
│   ├── users/                       # Collection
│   │   ├── 001.parquet             # Data block
│   │   ├── 002.parquet             # Data block
│   │   └── manifest.json           # Collection manifest
│   └── orders/
│       ├── 001.parquet
│       └── manifest.json
└── _iceberg/                        # Optional Iceberg metadata
    └── myapp/
        └── users/
            ├── metadata/
            │   └── v1.metadata.json
            └── manifests/
                └── manifest-1.avro

R2 (Production):
bucket/
├── shard-0/
│   ├── myapp/
│   │   ├── users/
│   │   │   ├── block-1706000000-abc123.parquet
│   │   │   └── manifest.json
│   │   └── orders/...
│   └── _pending/                    # Two-phase commit markers
│       └── flush-xxx.json
├── shard-1/...
└── shard-15/...
```

---

## Iceberg Integration

MongoLake optionally generates Apache Iceberg metadata for interoperability with analytical engines.

### Iceberg Components

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Iceberg Module                                      │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  ManifestWriter                                                        │  │
│  │  - Generates Avro-encoded manifest files                               │  │
│  │  - Tracks data files with statistics                                   │  │
│  │  - Supports partition field summaries                                  │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  SnapshotManager                                                       │  │
│  │  - Creates and manages snapshots                                       │  │
│  │  - Supports cherry-pick and rollback                                   │  │
│  │  - Handles snapshot expiration                                         │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  TimeTravelReader                                                      │  │
│  │  - Queries historical snapshots                                        │  │
│  │  - Computes snapshot diffs                                             │  │
│  │  - Reads changes between versions                                      │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  MetadataWriter                                                        │  │
│  │  - Generates table metadata JSON                                       │  │
│  │  - Manages schema evolution                                            │  │
│  │  - Handles partition specs and sort orders                             │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  Catalog Clients                                                       │  │
│  │  - R2DataCatalogClient: Cloudflare R2 Data Catalog                    │  │
│  │  - RestCatalog: Iceberg REST Catalog API                               │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Configuration Constants

Key configuration constants are centralized in `src/constants.ts`:

| Constant | Default | Description |
|----------|---------|-------------|
| `DEFAULT_SHARD_COUNT` | 16 | Number of shards for data distribution |
| `DEFAULT_FLUSH_THRESHOLD_BYTES` | 1MB | Buffer size threshold for auto-flush |
| `DEFAULT_FLUSH_THRESHOLD_DOCS` | 1,000 | Document count threshold for auto-flush |
| `MAX_WAL_SIZE_BYTES` | 10MB | Maximum WAL size before forced flush |
| `MAX_WAL_ENTRIES` | 10,000 | Maximum WAL entries before forced flush |
| `DEFAULT_ROW_GROUP_SIZE_BYTES` | 64MB | Target row group size for Parquet files |
| `DEFAULT_COMPACTION_MIN_BLOCK_SIZE` | 2MB | Minimum block size for compaction |
| `DEFAULT_COMPACTION_TARGET_BLOCK_SIZE` | 4MB | Target merged block size |
| `MAX_WIRE_MESSAGE_SIZE` | 48MB | Maximum wire protocol message size |

---

## See Also

- [Deployment Guide](./DEPLOYMENT.md) - Production deployment instructions
- [API Documentation](./api/) - REST and RPC API reference
- [Query Engines Integration](./query-engines.md) - DuckDB, Spark, Trino integration
