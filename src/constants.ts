/**
 * MongoLake Constants
 *
 * Centralized constants extracted from magic numbers throughout the codebase.
 * Organized by functional area for easy reference and maintenance.
 */

// ============================================================================
// PARQUET Constants
// ============================================================================

/** Parquet file format magic bytes (ASCII: "PAR1") */
export const PARQUET_MAGIC_BYTES = 'PAR1';

/** Size of Parquet magic bytes header */
export const PARQUET_MAGIC_SIZE = 4;

// ============================================================================
// COMPRESSION Constants
// ============================================================================

/** Snappy compression sliding window size for finding repeated sequences */
export const SNAPPY_WINDOW_SIZE = 1024;

/** Minimum match length to encode as back-reference (Snappy) */
export const SNAPPY_MIN_MATCH_LENGTH = 4;

/** Maximum match length for back-reference encoding */
export const MAX_MATCH_LENGTH = 255;

/** Threshold for flushing literal blocks to prevent unbounded growth */
export const LITERAL_FLUSH_THRESHOLD = 16384;

/** ZSTD compression window size for more aggressive matching */
export const ZSTD_WINDOW_SIZE = 4096;

/** Minimum match length for ZSTD-style compression */
export const ZSTD_MIN_MATCH_LENGTH = 3;

/** Maximum number of positions to keep in hash table per pattern */
export const HASH_POSITION_LIMIT = 8;

// ============================================================================
// SHARDING Constants
// ============================================================================

/** Default number of shards for data distribution */
export const DEFAULT_SHARD_COUNT = 16;

// ============================================================================
// Dynamic Shard Splitting Constants
// ============================================================================

/** Default maximum documents per shard before triggering split (1 million) */
export const DEFAULT_SHARD_SPLIT_MAX_DOCUMENTS = 1_000_000;

/** Default maximum size per shard before triggering split (10GB) */
export const DEFAULT_SHARD_SPLIT_MAX_SIZE_BYTES = 10 * 1024 * 1024 * 1024;

/** Default maximum write rate per second before triggering split */
export const DEFAULT_SHARD_SPLIT_MAX_WRITE_RATE = 10_000;

/** Default interval between split threshold checks (in milliseconds) - 1 minute */
export const DEFAULT_SHARD_SPLIT_CHECK_INTERVAL_MS = 60_000;

/** Default duration threshold must be exceeded before split (in milliseconds) - 5 minutes */
export const DEFAULT_SHARD_SPLIT_SUSTAINED_THRESHOLD_MS = 5 * 60 * 1000;

/** Default minimum time between splits of the same shard (in milliseconds) - 1 hour */
export const DEFAULT_SHARD_SPLIT_MIN_INTERVAL_MS = 60 * 60 * 1000;

/** Default maximum concurrent split operations */
export const DEFAULT_SHARD_SPLIT_MAX_CONCURRENT = 1;

/** Default write rate sliding window size (in milliseconds) - 1 minute */
export const DEFAULT_SHARD_WRITE_RATE_WINDOW_MS = 60_000;

// ============================================================================
// TIMING Constants
// ============================================================================

/** Delay before scheduling compaction alarm after flush (in milliseconds) */
export const COMPACTION_ALARM_DELAY_MS = 60000;

/** Delay before rescheduling compaction when more work remains (in milliseconds) */
export const COMPACTION_RESCHEDULE_DELAY_MS = 1000;

// ============================================================================
// CACHE Constants
// ============================================================================

/** Default maximum number of tokens to cache */
export const DEFAULT_CACHE_MAX_SIZE = 1000;

/** Default TTL for cached tokens (in seconds) */
export const DEFAULT_CACHE_TTL_SECONDS = 300;

/** Default maximum entries for shard router cache */
export const DEFAULT_ROUTER_CACHE_SIZE = 10000;

/** Default maximum entries for RPC read cache */
export const DEFAULT_RPC_CACHE_SIZE = 1000;

/** Default TTL for RPC read cache (in milliseconds) */
export const DEFAULT_RPC_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

/** Default maximum entries for RPC write cache */
export const DEFAULT_RPC_WRITE_CACHE_SIZE = 500;

/** Default TTL for RPC write cache (in milliseconds) */
export const DEFAULT_RPC_WRITE_CACHE_TTL_MS = 60 * 1000; // 1 minute

/** Default maximum entries for index cache */
export const DEFAULT_INDEX_CACHE_SIZE = 100;

/** Default maximum entries for zone map cache */
export const DEFAULT_ZONE_MAP_CACHE_SIZE = 1000;

/** Default maximum entries for query result cache */
export const DEFAULT_QUERY_CACHE_SIZE = 1000;

/** Default TTL for query result cache (in milliseconds) - 5 minutes */
export const DEFAULT_QUERY_CACHE_TTL_MS = 5 * 60 * 1000;

/** Default maximum memory for query result cache (50MB) */
export const DEFAULT_QUERY_CACHE_MAX_MEMORY_BYTES = 50 * 1024 * 1024;

// ============================================================================
// HTTP Constants
// ============================================================================

/** Maximum age for CORS preflight cache (in seconds) - 24 hours */
export const CORS_MAX_AGE_SECONDS = 86400;

// ============================================================================
// RPC Service Constants
// ============================================================================

/** Default circuit breaker reset timeout (in milliseconds) */
export const CIRCUIT_BREAKER_RESET_TIMEOUT_MS = 30000;

/** Default operation timeout for RPC calls (in milliseconds) */
export const DEFAULT_OPERATION_TIMEOUT_MS = 30000;

/** Default retry base delay (in milliseconds) */
export const DEFAULT_RETRY_BASE_DELAY_MS = 100;

/** Default retry max delay (in milliseconds) */
export const DEFAULT_RETRY_MAX_DELAY_MS = 5000;

/** Default batch size for RPC operations */
export const DEFAULT_RPC_BATCH_SIZE = 100;

/** Default maximum connections per shard */
export const DEFAULT_MAX_CONNECTIONS_PER_SHARD = 10;

// ============================================================================
// Shard Configuration Constants
// ============================================================================

/** Default flush threshold in bytes (1MB) */
export const DEFAULT_FLUSH_THRESHOLD_BYTES = 1024 * 1024;

/** Default flush threshold in document count */
export const DEFAULT_FLUSH_THRESHOLD_DOCS = 1000;

/** Default maximum buffer size in bytes before back-pressure triggers auto-flush (100MB) */
export const DEFAULT_BUFFER_MAX_BYTES = 100 * 1024 * 1024;

// ============================================================================
// Parquet Constants
// ============================================================================

/** Default row group size for Parquet files */
export const DEFAULT_ROW_GROUP_SIZE = 10000;

/** Default row group size in bytes (64MB) for streaming writer */
export const DEFAULT_ROW_GROUP_SIZE_BYTES = 64 * 1024 * 1024;

/** ZSTD compression sliding window size (64KB) */
export const ZSTD_SLIDING_WINDOW_SIZE = 65536;

// ============================================================================
// Deduplication Constants
// ============================================================================

/** Default batch size for deduplication operations */
export const DEFAULT_DEDUPLICATION_BATCH_SIZE = 1000;

// ============================================================================
// Compaction Constants
// ============================================================================

/** Default minimum block size for compaction (2MB) */
export const DEFAULT_COMPACTION_MIN_BLOCK_SIZE = 2_000_000;

/** Default target block size for compaction (4MB) */
export const DEFAULT_COMPACTION_TARGET_BLOCK_SIZE = 4_000_000;

/** Default maximum blocks to process per compaction run */
export const DEFAULT_COMPACTION_MAX_BLOCKS_PER_RUN = 10;

/** Default alarm delay for compaction scheduler (in milliseconds) */
export const DEFAULT_COMPACTION_ALARM_DELAY_MS = 100;

// ============================================================================
// Wire Protocol Constants
// ============================================================================

/** Maximum MongoDB wire protocol message size (48MB) */
export const MAX_WIRE_MESSAGE_SIZE = 48 * 1024 * 1024;

/** Minimum MongoDB wire protocol message size (header only, 16 bytes) */
export const MIN_WIRE_MESSAGE_SIZE = 16;

/** Wire protocol cursor timeout (10 minutes) */
export const WIRE_PROTOCOL_CURSOR_TIMEOUT_MS = 600000;

/** Wire protocol cursor cleanup interval (1 minute) */
export const WIRE_PROTOCOL_CURSOR_CLEANUP_INTERVAL_MS = 60000;

/** MongoDB server version reported by wire protocol */
export const WIRE_PROTOCOL_SERVER_VERSION = '7.0.0';

/** Minimum supported wire protocol version */
export const WIRE_PROTOCOL_VERSION_MIN = 0;

/** Maximum supported wire protocol version */
export const WIRE_PROTOCOL_VERSION_MAX = 21;

// ============================================================================
// Storage Constants
// ============================================================================

/**
 * Default limit for concurrent multipart upload operations.
 * Prevents exhausting connection pool or memory under load.
 */
export const STORAGE_MULTIPART_CONCURRENCY = 5;

// ============================================================================
// Path Traversal Constants
// ============================================================================

/**
 * Maximum depth for nested field path traversal.
 * Prevents DoS attacks via deeply nested paths like "a.b.c.d..." with thousands of segments.
 */
export const MAX_NESTED_PATH_DEPTH = 32;

// ============================================================================
// Batch Operation Limits
// ============================================================================

/** Maximum number of documents allowed in a single batch operation (insertMany, updateMany, deleteMany) */
export const MAX_BATCH_SIZE = 100_000;

/** Maximum total size in bytes for a single batch operation (16MB, matching MongoDB's document size limit) */
export const MAX_BATCH_BYTES = 16 * 1024 * 1024;

// ============================================================================
// WAL Constants
// ============================================================================

/** Maximum WAL size in bytes before forcing a flush (10MB) */
export const MAX_WAL_SIZE_BYTES = 10 * 1024 * 1024;

/** Maximum number of WAL entries before forcing a flush */
export const MAX_WAL_ENTRIES = 10_000;

// ============================================================================
// TCP Backpressure Constants
// ============================================================================

/** High water mark for TCP backpressure (16KB) - pause reading when buffer exceeds this */
export const TCP_BACKPRESSURE_HIGH_WATER_MARK = 16384;

/** Low water mark for TCP backpressure (8KB) - resume reading when buffer drops below this */
export const TCP_BACKPRESSURE_LOW_WATER_MARK = 8192;

/** Timeout for TCP drain operations (in milliseconds) */
export const TCP_BACKPRESSURE_TIMEOUT_MS = 30000;
