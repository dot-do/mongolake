/**
 * BSON Serializer for MongoDB Wire Protocol
 *
 * Serializes JavaScript objects to BSON format for wire protocol responses.
 * This is the counterpart to the BSON parser in message-parser.ts.
 *
 * Features:
 * - Fluent ResponseBuilder API for constructing responses
 * - Typed MongoErrorCode enum for standard error codes
 * - Buffer pooling for optimized BSON serialization
 * - OP_COMPRESSED support with multiple compression algorithms
 * - CRC-32C checksum support for OP_MSG messages
 *
 * Usage:
 * ```typescript
 * import {
 *   buildSuccessResponse,
 *   buildErrorResponse,
 *   buildCursorResponse,
 *   MongoErrorCode,
 *   ResponseBuilder,
 * } from './bson-serializer.js';
 *
 * // Simple success response
 * const success = buildSuccessResponse(requestId, responseTo, { n: 1 });
 *
 * // Error response with typed error code
 * const error = buildErrorResponse(
 *   requestId, responseTo,
 *   MongoErrorCode.CursorNotFound,
 *   'Cursor not found'
 * );
 *
 * // Cursor response (common pattern for find/aggregate)
 * const cursor = buildCursorResponse(
 *   requestId, responseTo,
 *   0n, 'db.collection',
 *   documents
 * );
 *
 * // Fluent builder for complex responses
 * const response = ResponseBuilder.create(requestId, responseTo)
 *   .success()
 *   .withCursor(0n, 'db.collection', documents)
 *   .build();
 * ```
 *
 * @module wire-protocol/bson-serializer
 */

import type { ObjectId } from '../types.js';
import { OpCode } from './message-parser.js';

// ============================================================================
// BSON Type Codes
// ============================================================================

/**
 * BSON element type codes as defined in the BSON specification.
 * @see https://bsonspec.org/spec.html
 */
const BSON_TYPES = {
  /** 64-bit IEEE 754 floating point */
  DOUBLE: 0x01,
  /** UTF-8 string */
  STRING: 0x02,
  /** Embedded BSON document */
  DOCUMENT: 0x03,
  /** BSON array (document with numeric string keys) */
  ARRAY: 0x04,
  /** Binary data with subtype */
  BINARY: 0x05,
  /** 12-byte MongoDB ObjectId */
  OBJECT_ID: 0x07,
  /** Boolean (true/false) */
  BOOLEAN: 0x08,
  /** UTC datetime (int64 milliseconds since Unix epoch) */
  DATE: 0x09,
  /** Null value */
  NULL: 0x0a,
  /** Regular expression with options */
  REGEX: 0x0b,
  /** 32-bit signed integer */
  INT32: 0x10,
  /** MongoDB internal timestamp (uint64) */
  TIMESTAMP: 0x11,
  /** 64-bit signed integer */
  INT64: 0x12,
} as const;

/** Type for BSON type code values */
type BsonTypeCode = (typeof BSON_TYPES)[keyof typeof BSON_TYPES];

// ============================================================================
// MongoDB Error Codes Enum
// ============================================================================

/**
 * Standard MongoDB error codes for wire protocol responses.
 *
 * These codes are used in error responses to indicate the type of failure.
 * Based on MongoDB error code reference.
 *
 * @see https://www.mongodb.com/docs/manual/reference/error-codes/
 *
 * @example
 * ```typescript
 * import { MongoErrorCode, getErrorCodeName } from './bson-serializer.js';
 *
 * const code = MongoErrorCode.CursorNotFound;
 * const name = getErrorCodeName(code); // 'CursorNotFound'
 * ```
 */
export const MongoErrorCode = {
  // General errors (1-99)
  /** Generic internal error */
  InternalError: 1,
  /** Invalid namespace (database.collection format) */
  InvalidNamespace: 2,
  /** Bad value provided for a parameter */
  BadValue: 2,
  /** Target host is unreachable */
  HostUnreachable: 6,
  /** Target host could not be found */
  HostNotFound: 7,
  /** Unknown/unspecified error */
  UnknownError: 8,
  /** Failed to parse command or document */
  FailedToParse: 9,
  /** Unauthorized to perform operation */
  Unauthorized: 13,
  /** Type mismatch in operation */
  TypeMismatch: 14,
  /** Authentication credentials were invalid */
  AuthenticationFailed: 18,
  /** Operation is not legal in current state */
  IllegalOperation: 20,

  // Namespace errors (26-27)
  /** Requested namespace (collection) does not exist */
  NamespaceNotFound: 26,
  /** Requested index does not exist */
  IndexNotFound: 27,

  // Cursor errors (43)
  /** Cursor ID not found (expired or invalid) */
  CursorNotFound: 43,
  /** Invalid cursor state or parameters */
  InvalidCursor: 43,

  // Resource/timing errors (50-89)
  /** Operation exceeded time limit */
  ExceededTimeLimit: 50,
  /** Write concern requirements not satisfied */
  WriteConcernError: 64,
  /** Invalid options provided */
  InvalidOptions: 72,
  /** Network operation timed out */
  NetworkTimeout: 89,

  // Command errors (59, 115)
  /** Requested command does not exist */
  CommandNotFound: 59,
  /** Command exists but is not supported */
  CommandNotSupported: 115,

  // Resource limit errors (146+)
  /** Operation exceeded memory limit */
  ExceededMemoryLimit: 146,
  /** Invalid length for parameter or data */
  InvalidLength: 307,

  // Duplicate key error (11000)
  /** Duplicate key violation (unique index constraint) */
  DuplicateKey: 11000,

  // Operation control errors (11601)
  /** Operation was interrupted */
  Interrupted: 11601,

  // Application-specific errors (51024)
  /** Invalid parameter value */
  InvalidParameter: 51024,
} as const;

/** Type representing valid MongoDB error codes */
export type MongoErrorCode = (typeof MongoErrorCode)[keyof typeof MongoErrorCode];

/**
 * Mapping of error codes to their canonical string names.
 * Used by getErrorCodeName() to convert numeric codes to strings.
 */
const ERROR_CODE_NAMES: Readonly<Record<number, string>> = {
  [MongoErrorCode.InternalError]: 'InternalError',
  [MongoErrorCode.InvalidNamespace]: 'InvalidNamespace',
  [MongoErrorCode.HostUnreachable]: 'HostUnreachable',
  [MongoErrorCode.HostNotFound]: 'HostNotFound',
  [MongoErrorCode.UnknownError]: 'UnknownError',
  [MongoErrorCode.FailedToParse]: 'FailedToParse',
  [MongoErrorCode.IllegalOperation]: 'IllegalOperation',
  [MongoErrorCode.TypeMismatch]: 'TypeMismatch',
  [MongoErrorCode.NamespaceNotFound]: 'NamespaceNotFound',
  [MongoErrorCode.IndexNotFound]: 'IndexNotFound',
  [MongoErrorCode.CursorNotFound]: 'CursorNotFound',
  [MongoErrorCode.CommandNotSupported]: 'CommandNotSupported',
  [MongoErrorCode.CommandNotFound]: 'CommandNotFound',
  [MongoErrorCode.InvalidOptions]: 'InvalidOptions',
  [MongoErrorCode.WriteConcernError]: 'WriteConcernError',
  [MongoErrorCode.DuplicateKey]: 'DuplicateKey',
  [MongoErrorCode.NetworkTimeout]: 'NetworkTimeout',
  [MongoErrorCode.ExceededTimeLimit]: 'ExceededTimeLimit',
  [MongoErrorCode.AuthenticationFailed]: 'AuthenticationFailed',
  [MongoErrorCode.Unauthorized]: 'Unauthorized',
  [MongoErrorCode.InvalidLength]: 'InvalidLength',
  [MongoErrorCode.Interrupted]: 'Interrupted',
  [MongoErrorCode.ExceededMemoryLimit]: 'ExceededMemoryLimit',
};

/**
 * Get the standard code name for a MongoDB error code.
 *
 * @param code - The numeric error code
 * @returns The string name of the error code, or 'UnknownError' if not recognized
 *
 * @example
 * ```typescript
 * getErrorCodeName(43) // => 'CursorNotFound'
 * getErrorCodeName(999) // => 'UnknownError'
 * ```
 */
export function getErrorCodeName(code: MongoErrorCode | number): string {
  return ERROR_CODE_NAMES[code] ?? 'UnknownError';
}

// ============================================================================
// Compression Support
// ============================================================================

/**
 * Compression algorithms supported by MongoDB wire protocol.
 *
 * MongoDB supports multiple compression algorithms for OP_COMPRESSED messages.
 * The client and server negotiate which algorithms to use during the initial
 * handshake (hello/isMaster).
 *
 * @see https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/#op_compressed
 */
export const CompressionAlgorithm = {
  /** No compression - message is stored as-is */
  NONE: 0,
  /** Snappy compression - fast with moderate compression ratio */
  SNAPPY: 1,
  /** Zlib compression - better compression, more CPU intensive */
  ZLIB: 2,
  /** Zstandard compression - best balance of speed and compression */
  ZSTD: 3,
} as const;

/** Type representing valid compression algorithm identifiers */
export type CompressionAlgorithm = (typeof CompressionAlgorithm)[keyof typeof CompressionAlgorithm];

/**
 * Context for configuring message compression.
 *
 * Used with ResponseBuilder.withCompression() to enable compressed responses.
 */
export interface CompressionContext {
  /** The compression algorithm to use */
  algorithm: CompressionAlgorithm;
  /** Compression level (algorithm-specific, typically 1-9 for zlib) */
  level?: number;
}

/**
 * Type for compression function that transforms uncompressed data.
 * Used by ResponseBuilder to apply compression to message payloads.
 */
export type CompressorFunction = (data: Uint8Array) => Uint8Array;

// ============================================================================
// Buffer Pool for Optimized Serialization
// ============================================================================

/** Statistics for a single buffer pool bucket */
export interface BufferPoolBucketStats {
  /** Size of buffers in this bucket */
  bucketSize: number;
  /** Number of buffers currently pooled */
  pooled: number;
}

/**
 * Buffer pool for reusing allocations during BSON serialization.
 *
 * This pool reduces garbage collection pressure in high-throughput scenarios
 * by reusing buffers instead of allocating new ones for each serialization.
 *
 * The pool uses a bucketed approach with predefined bucket sizes. Buffers are
 * allocated from the smallest bucket that can fit the requested size.
 *
 * @example
 * ```typescript
 * const pool = getBufferPool();
 *
 * // Acquire a buffer
 * const buffer = pool.acquire(100); // Returns 256-byte buffer
 *
 * // Use the buffer...
 *
 * // Release back to pool
 * pool.release(buffer);
 * ```
 */
class BufferPool {
  /** Map of bucket size to available buffers */
  private readonly pools: Map<number, Uint8Array[]> = new Map();
  /** Maximum number of buffers to keep in each bucket */
  private readonly maxPoolSize: number;
  /** Available bucket sizes in ascending order */
  private readonly bucketSizes: readonly number[];

  /**
   * Create a new buffer pool.
   *
   * @param maxPoolSize - Maximum buffers to retain per bucket (default: 100)
   */
  constructor(maxPoolSize: number = 100) {
    this.maxPoolSize = maxPoolSize;
    // Bucket sizes: 64B, 256B, 1KB, 4KB, 16KB, 64KB
    this.bucketSizes = [64, 256, 1024, 4096, 16384, 65536] as const;
    for (const size of this.bucketSizes) {
      this.pools.set(size, []);
    }
  }

  /**
   * Find the appropriate bucket size for a given length.
   *
   * @param length - Required buffer length
   * @returns The bucket size, or null if length exceeds largest bucket
   */
  private getBucketSize(length: number): number | null {
    for (const size of this.bucketSizes) {
      if (length <= size) {
        return size;
      }
    }
    return null;
  }

  /**
   * Acquire a buffer of at least the specified length.
   *
   * Returns a pooled buffer if available, otherwise allocates a new one.
   * The returned buffer may be larger than requested.
   *
   * @param minLength - Minimum required buffer length
   * @returns A Uint8Array of at least minLength bytes
   */
  acquire(minLength: number): Uint8Array {
    const bucketSize = this.getBucketSize(minLength);

    if (bucketSize !== null) {
      const pool = this.pools.get(bucketSize)!;
      const buffer = pool.pop();
      if (buffer) {
        return buffer;
      }
      return new Uint8Array(bucketSize);
    }

    // For large buffers, allocate directly without pooling
    return new Uint8Array(minLength);
  }

  /**
   * Release a buffer back to the pool for reuse.
   *
   * The buffer is only pooled if:
   * - Its size matches a bucket size exactly
   * - The bucket hasn't reached maxPoolSize
   *
   * @param buffer - The buffer to release
   */
  release(buffer: Uint8Array): void {
    const bucketSize = this.getBucketSize(buffer.length);
    if (bucketSize === null || buffer.length !== bucketSize) {
      return; // Don't pool non-standard or oversized buffers
    }

    const pool = this.pools.get(bucketSize)!;
    if (pool.length < this.maxPoolSize) {
      // Clear sensitive data before returning to pool
      buffer.fill(0);
      pool.push(buffer);
    }
  }

  /**
   * Clear all pooled buffers, releasing memory.
   *
   * Call this when shutting down or during memory pressure.
   */
  clear(): void {
    for (const pool of this.pools.values()) {
      pool.length = 0;
    }
  }

  /**
   * Get statistics about the pool's current state.
   *
   * @returns Array of bucket statistics
   */
  getStats(): BufferPoolBucketStats[] {
    return Array.from(this.pools.entries()).map(([bucketSize, pool]) => ({
      bucketSize,
      pooled: pool.length,
    }));
  }

  /**
   * Get the total number of pooled buffers across all buckets.
   */
  get totalPooled(): number {
    let total = 0;
    for (const pool of this.pools.values()) {
      total += pool.length;
    }
    return total;
  }
}

/** Global buffer pool instance for BSON serialization */
const bufferPool = new BufferPool();

/**
 * Get the global buffer pool instance.
 *
 * Use this for monitoring pool statistics or manually managing buffers
 * in performance-critical code paths.
 *
 * @returns The global BufferPool instance
 */
export function getBufferPool(): BufferPool {
  return bufferPool;
}

// ============================================================================
// CRC-32C Checksum Support
// ============================================================================

/**
 * CRC-32C lookup table (Castagnoli polynomial: 0x1EDC6F41).
 * MongoDB uses CRC-32C for message checksums when checksumPresent flag is set.
 * Pre-computed for performance.
 */
const CRC32C_TABLE: Uint32Array = (() => {
  const table = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let crc = i;
    for (let j = 0; j < 8; j++) {
      crc = crc & 1 ? (crc >>> 1) ^ 0x82f63b78 : crc >>> 1;
    }
    table[i] = crc >>> 0;
  }
  return table;
})();

/**
 * Calculate CRC-32C checksum for a buffer.
 *
 * MongoDB uses the Castagnoli polynomial (CRC-32C) for message checksums.
 * This is different from the more common CRC-32 used in ZIP files.
 *
 * @param data - The data to checksum
 * @returns The 32-bit CRC-32C checksum
 *
 * @example
 * ```typescript
 * const message = new Uint8Array([...]);
 * const checksum = calculateCrc32c(message);
 * ```
 */
export function calculateCrc32c(data: Uint8Array): number {
  let crc = 0xffffffff;
  for (let i = 0; i < data.length; i++) {
    crc = CRC32C_TABLE[(crc ^ data[i]!) & 0xff]! ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

// ============================================================================
// Helper Types
// ============================================================================

/**
 * Interface for objects that can be serialized to BSON.
 * Any JavaScript object with string keys and unknown values.
 */
interface BsonSerializable {
  [key: string]: unknown;
}

/** Reusable TextEncoder instance for UTF-8 string encoding */
const textEncoder = new TextEncoder();

// ============================================================================
// Serialization Functions
// ============================================================================

/**
 * Calculate the size of a BSON-encoded value (without element type and key).
 *
 * This is used internally to pre-compute document sizes before serialization,
 * allowing for efficient single-pass buffer allocation.
 *
 * @param value - The JavaScript value to measure
 * @returns The number of bytes needed to encode the value
 */
function calculateValueSize(value: unknown): number {
  if (value === null || value === undefined) {
    return 0;
  }

  if (typeof value === 'boolean') {
    return 1;
  }

  if (typeof value === 'number') {
    // Use int32 for safe integers in int32 range, otherwise double
    if (Number.isInteger(value) && value >= -2147483648 && value <= 2147483647) {
      return 4;
    }
    return 8;
  }

  if (typeof value === 'bigint') {
    return 8;
  }

  if (typeof value === 'string') {
    const encoded = textEncoder.encode(value);
    return 4 + encoded.length + 1; // length prefix + bytes + null terminator
  }

  if (value instanceof Date) {
    return 8;
  }

  if (value instanceof Uint8Array) {
    return 4 + 1 + value.length; // length + subtype + data
  }

  if (value instanceof RegExp) {
    const pattern = textEncoder.encode(value.source);
    const flags = textEncoder.encode(value.flags);
    return pattern.length + 1 + flags.length + 1;
  }

  if (Array.isArray(value)) {
    return calculateDocumentSize(
      Object.fromEntries(value.map((v, i) => [i.toString(), v]))
    );
  }

  if (typeof value === 'object') {
    // Check if it's an ObjectId
    if (isObjectId(value)) {
      return 12;
    }
    return calculateDocumentSize(value as BsonSerializable);
  }

  return 0;
}

/**
 * Calculate the total size of a BSON document.
 *
 * A BSON document consists of:
 * - 4 bytes: document size (int32)
 * - N bytes: elements (type + key + value)
 * - 1 byte: null terminator
 *
 * @param doc - The document to measure
 * @returns The total size in bytes
 */
function calculateDocumentSize(doc: BsonSerializable): number {
  let size = 4 + 1; // 4 bytes for size + 1 byte for terminator

  for (const [key, value] of Object.entries(doc)) {
    if (value === undefined) continue;

    const keyBytes = textEncoder.encode(key);
    size += 1; // element type
    size += keyBytes.length + 1; // key + null terminator
    size += calculateValueSize(value);
  }

  return size;
}

/**
 * Check if a value is an ObjectId-like object
 */
function isObjectId(value: unknown): value is ObjectId {
  return (
    typeof value === 'object' &&
    value !== null &&
    'toHexString' in value &&
    typeof (value as { toHexString: unknown }).toHexString === 'function'
  );
}

/**
 * Serialize a value to BSON bytes into a target buffer at specified offset.
 * Returns the number of bytes written.
 */
function serializeValueInto(
  target: Uint8Array,
  offset: number,
  value: unknown
): number {
  if (value === null || value === undefined) {
    return 0;
  }

  const view = new DataView(target.buffer, target.byteOffset);

  if (typeof value === 'boolean') {
    target[offset] = value ? 0x01 : 0x00;
    return 1;
  }

  if (typeof value === 'number') {
    if (Number.isInteger(value) && value >= -2147483648 && value <= 2147483647) {
      view.setInt32(offset, value, true);
      return 4;
    }
    view.setFloat64(offset, value, true);
    return 8;
  }

  if (typeof value === 'bigint') {
    view.setBigInt64(offset, value, true);
    return 8;
  }

  if (typeof value === 'string') {
    const encoded = textEncoder.encode(value);
    view.setInt32(offset, encoded.length + 1, true);
    target.set(encoded, offset + 4);
    target[offset + 4 + encoded.length] = 0x00;
    return 4 + encoded.length + 1;
  }

  if (value instanceof Date) {
    view.setBigInt64(offset, BigInt(value.getTime()), true);
    return 8;
  }

  if (value instanceof Uint8Array) {
    view.setInt32(offset, value.length, true);
    target[offset + 4] = 0x00; // Binary subtype: generic
    target.set(value, offset + 5);
    return 4 + 1 + value.length;
  }

  if (value instanceof RegExp) {
    const pattern = textEncoder.encode(value.source);
    const flags = textEncoder.encode(value.flags);
    target.set(pattern, offset);
    target[offset + pattern.length] = 0x00;
    target.set(flags, offset + pattern.length + 1);
    target[offset + pattern.length + 1 + flags.length] = 0x00;
    return pattern.length + 1 + flags.length + 1;
  }

  if (Array.isArray(value)) {
    const arrayDoc = Object.fromEntries(value.map((v, i) => [i.toString(), v]));
    return serializeDocumentInto(target, offset, arrayDoc);
  }

  if (typeof value === 'object') {
    if (isObjectId(value)) {
      const hexString = value.toHexString();
      for (let i = 0; i < 12; i++) {
        target[offset + i] = parseInt(hexString.slice(i * 2, i * 2 + 2), 16);
      }
      return 12;
    }
    return serializeDocumentInto(target, offset, value as BsonSerializable);
  }

  return 0;
}

/**
 * Get the BSON type code for a JavaScript value.
 *
 * Maps JavaScript types to their BSON type codes as defined in the spec.
 *
 * @param value - The JavaScript value
 * @returns The BSON type code
 */
function getBsonType(value: unknown): BsonTypeCode {
  if (value === null || value === undefined) {
    return BSON_TYPES.NULL;
  }

  if (typeof value === 'boolean') {
    return BSON_TYPES.BOOLEAN;
  }

  if (typeof value === 'number') {
    if (Number.isInteger(value) && value >= -2147483648 && value <= 2147483647) {
      return BSON_TYPES.INT32;
    }
    return BSON_TYPES.DOUBLE;
  }

  if (typeof value === 'bigint') {
    return BSON_TYPES.INT64;
  }

  if (typeof value === 'string') {
    return BSON_TYPES.STRING;
  }

  if (value instanceof Date) {
    return BSON_TYPES.DATE;
  }

  if (value instanceof Uint8Array) {
    return BSON_TYPES.BINARY;
  }

  if (value instanceof RegExp) {
    return BSON_TYPES.REGEX;
  }

  if (Array.isArray(value)) {
    return BSON_TYPES.ARRAY;
  }

  if (typeof value === 'object') {
    if (isObjectId(value)) {
      return BSON_TYPES.OBJECT_ID;
    }
    return BSON_TYPES.DOCUMENT;
  }

  return BSON_TYPES.NULL;
}

/**
 * Serialize a BSON document into a target buffer at specified offset.
 *
 * This is the core serialization function that writes a complete BSON document
 * to the buffer. It assumes the buffer has sufficient space.
 *
 * @param target - The target buffer
 * @param offset - Byte offset to start writing at
 * @param doc - The document to serialize
 * @returns The number of bytes written
 */
function serializeDocumentInto(
  target: Uint8Array,
  offset: number,
  doc: BsonSerializable
): number {
  const docSize = calculateDocumentSize(doc);
  const view = new DataView(target.buffer, target.byteOffset);

  // Write document size
  view.setInt32(offset, docSize, true);

  let pos = offset + 4;

  for (const [key, value] of Object.entries(doc)) {
    if (value === undefined) continue;

    // Write element type
    target[pos] = getBsonType(value);
    pos += 1;

    // Write key (C-string)
    const keyBytes = textEncoder.encode(key);
    target.set(keyBytes, pos);
    pos += keyBytes.length;
    target[pos] = 0x00;
    pos += 1;

    // Write value
    pos += serializeValueInto(target, pos, value);
  }

  // Write terminator
  target[pos] = 0x00;

  return docSize;
}

/**
 * Serialize a JavaScript object to BSON format.
 *
 * This is the main entry point for BSON serialization. It allocates a new
 * buffer of the exact size needed and serializes the document into it.
 *
 * @param doc - The JavaScript object to serialize
 * @returns A new Uint8Array containing the BSON-encoded document
 *
 * @example
 * ```typescript
 * const bson = serializeDocument({ name: 'test', value: 42 });
 * ```
 */
export function serializeDocument(doc: BsonSerializable): Uint8Array {
  const docSize = calculateDocumentSize(doc);
  const result = new Uint8Array(docSize);
  serializeDocumentInto(result, 0, doc);
  return result;
}

/** Result of pooled document serialization */
export interface PooledSerializationResult {
  /** The buffer containing the serialized document */
  buffer: Uint8Array;
  /** The actual length of the serialized data (may be less than buffer.length) */
  length: number;
  /** Release the buffer back to the pool when done */
  release: () => void;
}

/**
 * Serialize a document using buffer pool for optimized allocation.
 *
 * In high-throughput scenarios, this reduces garbage collection pressure by
 * reusing buffers from a pool instead of allocating new ones.
 *
 * **Important:** Caller MUST call release() when done with the buffer.
 *
 * @param doc - The JavaScript object to serialize
 * @returns Object with buffer, actual length, and release function
 *
 * @example
 * ```typescript
 * const result = serializeDocumentPooled({ name: 'test' });
 * try {
 *   // Use result.buffer.slice(0, result.length)
 *   socket.write(result.buffer.subarray(0, result.length));
 * } finally {
 *   result.release(); // IMPORTANT: Always release!
 * }
 * ```
 */
export function serializeDocumentPooled(doc: BsonSerializable): PooledSerializationResult {
  const docSize = calculateDocumentSize(doc);
  const buffer = bufferPool.acquire(docSize);
  serializeDocumentInto(buffer, 0, doc);
  return {
    buffer,
    length: docSize,
    release: () => bufferPool.release(buffer),
  };
}

// ============================================================================
// Wire Protocol Message Building
// ============================================================================

/** Wire protocol header size in bytes */
const HEADER_SIZE = 16;

/** OP_MSG flags field size in bytes */
const FLAGS_SIZE = 4;

/** Checksum size in bytes (CRC-32C) */
const CHECKSUM_SIZE = 4;

/** OP_MSG section type for body (single BSON document) */
const SECTION_TYPE_BODY = 0;

/**
 * Build an OP_MSG response message.
 *
 * OP_MSG is the standard message format for MongoDB 3.6+. It consists of:
 * - 16-byte header (length, requestId, responseTo, opCode)
 * - 4-byte flags
 * - One or more sections (type + payload)
 * - Optional 4-byte CRC-32C checksum (if checksumPresent flag is set)
 *
 * @param requestId - The request ID for this message
 * @param responseTo - The request ID this is responding to
 * @param doc - The response document
 * @param flags - OP_MSG flags (default: 0)
 * @returns The complete wire protocol message
 *
 * @example
 * ```typescript
 * const response = buildOpMsgResponse(123, 456, { ok: 1 });
 * socket.write(response);
 * ```
 */
export function buildOpMsgResponse(
  requestId: number,
  responseTo: number,
  doc: BsonSerializable,
  flags: number = 0
): Uint8Array {
  const bsonDoc = serializeDocument(doc);

  // Check if checksum is requested
  const includeChecksum = (flags & 0x01) !== 0; // checksumPresent flag

  // Message structure:
  // - Header: 16 bytes
  // - Flags: 4 bytes
  // - Section type: 1 byte (0 for body)
  // - BSON document: variable
  // - Checksum (optional): 4 bytes

  const baseLength = HEADER_SIZE + FLAGS_SIZE + 1 + bsonDoc.length;
  const messageLength = includeChecksum ? baseLength + CHECKSUM_SIZE : baseLength;
  const result = new Uint8Array(messageLength);
  const view = new DataView(result.buffer);

  // Header
  view.setInt32(0, messageLength, true); // messageLength
  view.setInt32(4, requestId, true); // requestId
  view.setInt32(8, responseTo, true); // responseTo
  view.setInt32(12, OpCode.OP_MSG, true); // opCode

  // Flags
  view.setUint32(HEADER_SIZE, flags, true);

  // Section (type 0 - body)
  result[HEADER_SIZE + FLAGS_SIZE] = SECTION_TYPE_BODY;
  result.set(bsonDoc, HEADER_SIZE + FLAGS_SIZE + 1);

  // Calculate and append checksum if requested
  if (includeChecksum) {
    const messageWithoutChecksum = result.subarray(0, baseLength);
    const checksum = calculateCrc32c(messageWithoutChecksum);
    view.setUint32(baseLength, checksum, true);
  }

  return result;
}

/**
 * Build an OP_REPLY message (legacy format for OP_QUERY responses).
 *
 * OP_REPLY is the legacy response format used with OP_QUERY. While deprecated,
 * it's still needed for compatibility with older clients and some tools.
 *
 * Message structure:
 * - 16-byte header (length, requestId, responseTo, opCode=1)
 * - 4-byte response flags
 * - 8-byte cursor ID (int64)
 * - 4-byte starting from (int32)
 * - 4-byte number returned (int32)
 * - BSON documents (variable)
 *
 * @param requestId - The request ID for this message
 * @param responseTo - The request ID this is responding to
 * @param docs - Array of response documents
 * @param flags - Response flags (default: 0)
 * @param cursorId - Cursor ID for getMore (default: 0n = no cursor)
 * @param startingFrom - Starting position in results (default: 0)
 * @returns The complete wire protocol message
 */
export function buildOpReply(
  requestId: number,
  responseTo: number,
  docs: BsonSerializable[],
  flags: number = 0,
  cursorId: bigint = 0n,
  startingFrom: number = 0
): Uint8Array {
  // Serialize all documents upfront
  const serializedDocs = docs.map(serializeDocument);
  const docsLength = serializedDocs.reduce((sum, d) => sum + d.length, 0);

  // Calculate total message length
  const messageLength = HEADER_SIZE + 4 + 8 + 4 + 4 + docsLength;
  const result = new Uint8Array(messageLength);
  const view = new DataView(result.buffer);

  // Header
  view.setInt32(0, messageLength, true);
  view.setInt32(4, requestId, true);
  view.setInt32(8, responseTo, true);
  view.setInt32(12, OpCode.OP_REPLY, true);

  // OP_REPLY body
  view.setUint32(16, flags, true); // Response flags
  view.setBigInt64(20, cursorId, true); // Cursor ID
  view.setInt32(28, startingFrom, true); // Starting from
  view.setInt32(32, docs.length, true); // Number returned

  // Documents
  let offset = 36;
  for (const doc of serializedDocs) {
    result.set(doc, offset);
    offset += doc.length;
  }

  return result;
}

/**
 * Build an OP_COMPRESSED message wrapper.
 *
 * OP_COMPRESSED wraps another message (typically OP_MSG) with compression.
 * The inner message's header is excluded from compression.
 *
 * Message structure:
 * - 16-byte header (length, requestId, responseTo, opCode=2012)
 * - 4-byte original opcode (the wrapped message's opcode)
 * - 4-byte uncompressed size (size of wrapped message body)
 * - 1-byte compressor ID (algorithm identifier)
 * - Compressed data (variable)
 *
 * **Note:** This function builds the message structure but does not perform
 * the actual compression. The caller must compress the data beforehand.
 *
 * @param originalOpCode - The opcode of the wrapped message (e.g., OP_MSG)
 * @param uncompressedSize - Size of the uncompressed message body (without header)
 * @param compressorId - The compression algorithm used
 * @param compressedData - The already-compressed message body
 * @param requestId - The request ID for this message
 * @param responseTo - The request ID this is responding to
 * @returns The complete OP_COMPRESSED wire protocol message
 *
 * @example
 * ```typescript
 * // Build inner message
 * const innerMsg = buildOpMsgResponse(1, 2, { ok: 1 });
 * const innerBody = innerMsg.slice(16); // Exclude header
 *
 * // Compress the body (using your compression library)
 * const compressed = zlibCompress(innerBody);
 *
 * // Build OP_COMPRESSED wrapper
 * const message = buildOpCompressed(
 *   OpCode.OP_MSG,
 *   innerBody.length,
 *   CompressionAlgorithm.ZLIB,
 *   compressed,
 *   1,
 *   2
 * );
 * ```
 */
export function buildOpCompressed(
  originalOpCode: number,
  uncompressedSize: number,
  compressorId: CompressionAlgorithm,
  compressedData: Uint8Array,
  requestId: number,
  responseTo: number
): Uint8Array {
  const messageLength = HEADER_SIZE + 4 + 4 + 1 + compressedData.length;
  const result = new Uint8Array(messageLength);
  const view = new DataView(result.buffer);

  // Header
  view.setInt32(0, messageLength, true);
  view.setInt32(4, requestId, true);
  view.setInt32(8, responseTo, true);
  view.setInt32(12, OpCode.OP_COMPRESSED, true);

  // OP_COMPRESSED body
  view.setInt32(16, originalOpCode, true); // Original opcode
  view.setInt32(20, uncompressedSize, true); // Uncompressed size
  result[24] = compressorId; // Compressor ID
  result.set(compressedData, 25); // Compressed data

  return result;
}

/**
 * Build a compressed response message.
 *
 * This is a convenience function that builds an OP_MSG response and wraps it
 * in an OP_COMPRESSED message in one step.
 *
 * @param requestId - The request ID for this message
 * @param responseTo - The request ID this is responding to
 * @param doc - The response document
 * @param algorithm - The compression algorithm to use
 * @param compressor - Function that compresses the data
 * @param flags - OP_MSG flags (default: 0)
 * @returns The complete OP_COMPRESSED wire protocol message
 *
 * @example
 * ```typescript
 * const response = buildCompressedResponse(
 *   1, 2,
 *   { ok: 1 },
 *   CompressionAlgorithm.ZLIB,
 *   zlibCompress
 * );
 * ```
 */
export function buildCompressedResponse(
  requestId: number,
  responseTo: number,
  doc: BsonSerializable,
  algorithm: CompressionAlgorithm,
  compressor: CompressorFunction,
  flags: number = 0
): Uint8Array {
  // Build the inner OP_MSG response
  const innerMsg = buildOpMsgResponse(requestId, responseTo, doc, flags);

  // Extract the body (everything after the header)
  const innerBody = innerMsg.slice(HEADER_SIZE);

  // Compress the body
  const compressedData = compressor(innerBody);

  // Build the OP_COMPRESSED wrapper
  return buildOpCompressed(
    OpCode.OP_MSG,
    innerBody.length,
    algorithm,
    compressedData,
    requestId,
    responseTo
  );
}

/**
 * Build a standard success response with ok: 1.
 *
 * Convenience function for building successful MongoDB responses.
 *
 * @param requestId - The request ID for this message
 * @param responseTo - The request ID this is responding to
 * @param data - Additional fields to include in the response (default: {})
 * @returns The complete wire protocol message
 *
 * @example
 * ```typescript
 * // Simple success
 * const response = buildSuccessResponse(1, 2);
 *
 * // Success with data
 * const response = buildSuccessResponse(1, 2, {
 *   cursor: { id: 0n, ns: 'test.users', firstBatch: docs }
 * });
 * ```
 */
export function buildSuccessResponse(
  requestId: number,
  responseTo: number,
  data: BsonSerializable = {}
): Uint8Array {
  return buildOpMsgResponse(requestId, responseTo, {
    ...data,
    ok: 1, // Ensure ok:1 is always set for success responses (cannot be overridden by data)
  });
}

/**
 * Build an error response with ok: 0.
 *
 * Convenience function for building MongoDB error responses with
 * standard error document structure. The codeName is automatically
 * derived from the error code if not provided.
 *
 * @param requestId - The request ID for this message
 * @param responseTo - The request ID this is responding to
 * @param code - Numeric error code (use MongoErrorCode constants)
 * @param message - Human-readable error message
 * @param codeName - Error code name (default: looked up from code via getErrorCodeName)
 * @returns The complete wire protocol message
 *
 * @example
 * ```typescript
 * // Using MongoErrorCode constant (codeName auto-derived)
 * const response = buildErrorResponse(
 *   1, 2,
 *   MongoErrorCode.CursorNotFound,
 *   'cursor id 12345 not found'
 * );
 *
 * // With explicit codeName
 * const response = buildErrorResponse(
 *   1, 2,
 *   59,
 *   'no such command: unknown',
 *   'CommandNotFound'
 * );
 * ```
 */
export function buildErrorResponse(
  requestId: number,
  responseTo: number,
  code: MongoErrorCode | number,
  message: string,
  codeName?: string
): Uint8Array {
  return buildOpMsgResponse(requestId, responseTo, {
    ok: 0,
    errmsg: message,
    code,
    codeName: codeName ?? getErrorCodeName(code),
  });
}

/**
 * Build a cursor response with standard structure.
 *
 * This is a convenience function for the common pattern of returning
 * query results with a cursor. Combines ok: 1 with a cursor object.
 *
 * @param requestId - The request ID for this message
 * @param responseTo - The request ID this is responding to
 * @param cursorId - Cursor ID (0n for exhausted/no cursor)
 * @param namespace - Full namespace (database.collection)
 * @param batch - Array of documents
 * @param batchType - Either 'firstBatch' (for find) or 'nextBatch' (for getMore)
 * @returns The complete wire protocol message
 *
 * @example
 * ```typescript
 * // First batch from find
 * const response = buildCursorResponse(
 *   1, 2,
 *   cursorId,
 *   'testdb.users',
 *   documents,
 *   'firstBatch'
 * );
 *
 * // Next batch from getMore
 * const response = buildCursorResponse(
 *   1, 2,
 *   cursorId,
 *   'testdb.users',
 *   documents,
 *   'nextBatch'
 * );
 * ```
 */
export function buildCursorResponse(
  requestId: number,
  responseTo: number,
  cursorId: bigint,
  namespace: string,
  batch: unknown[],
  batchType: 'firstBatch' | 'nextBatch' = 'firstBatch'
): Uint8Array {
  return buildSuccessResponse(requestId, responseTo, {
    cursor: {
      id: cursorId,
      ns: namespace,
      [batchType]: batch,
    },
  });
}

/**
 * Build a write result response with standard structure.
 *
 * Convenience function for insert/update/delete operation responses.
 *
 * @param requestId - The request ID for this message
 * @param responseTo - The request ID this is responding to
 * @param n - Number of documents affected
 * @param options - Additional write result options
 * @returns The complete wire protocol message
 *
 * @example
 * ```typescript
 * // Simple insert result
 * const response = buildWriteResultResponse(1, 2, 5);
 *
 * // Update result with modifications
 * const response = buildWriteResultResponse(1, 2, 3, { nModified: 2 });
 *
 * // Upsert result
 * const response = buildWriteResultResponse(1, 2, 1, {
 *   upserted: [{ index: 0, _id: newId }]
 * });
 * ```
 */
export function buildWriteResultResponse(
  requestId: number,
  responseTo: number,
  n: number,
  options?: {
    nModified?: number;
    upserted?: Array<{ index: number; _id: unknown }>;
  }
): Uint8Array {
  const data: BsonSerializable = { n };

  if (options?.nModified !== undefined) {
    data.nModified = options.nModified;
  }

  if (options?.upserted && options.upserted.length > 0) {
    data.upserted = options.upserted;
  }

  return buildSuccessResponse(requestId, responseTo, data);
}

// ============================================================================
// Fluent Response Builder
// ============================================================================

/** OP_MSG response flag constants for use with ResponseBuilder.withFlags() */
export const OpMsgResponseFlags = {
  /** Include CRC-32C checksum at end of message */
  checksumPresent: 0x01,
  /** More messages coming, don't respond yet (exhaust streaming) */
  moreToCome: 0x02,
} as const;

/**
 * Fluent builder for constructing MongoDB wire protocol responses.
 *
 * ResponseBuilder provides a type-safe, fluent API for building OP_MSG responses
 * with support for:
 * - Success and error responses
 * - Cursor data (find/aggregate results)
 * - Write results (insert/update/delete)
 * - Optional CRC-32C checksum via withChecksum()
 * - Optional compression via withCompression()
 *
 * @example Success response with cursor
 * ```typescript
 * const response = ResponseBuilder.create(requestId, responseTo)
 *   .success()
 *   .withCursor(0n, 'db.collection', documents)
 *   .build();
 * ```
 *
 * @example Error response
 * ```typescript
 * const response = ResponseBuilder.create(requestId, responseTo)
 *   .error(MongoErrorCode.CursorNotFound, 'Cursor not found')
 *   .build();
 * ```
 *
 * @example With checksum
 * ```typescript
 * const response = ResponseBuilder.create(requestId, responseTo)
 *   .success()
 *   .withData({ n: 1 })
 *   .withChecksum()
 *   .build();
 * ```
 *
 * @example With compression
 * ```typescript
 * const response = ResponseBuilder.create(requestId, responseTo)
 *   .success()
 *   .withData({ n: 1 })
 *   .withCompression({ algorithm: CompressionAlgorithm.ZLIB }, zlibCompress)
 *   .build();
 * ```
 */
export class ResponseBuilder {
  private readonly requestId: number;
  private readonly responseTo: number;
  private isSuccess: boolean = true;
  private responseData: BsonSerializable = {};
  private errorCode: number = 0;
  private errorMessage: string = '';
  private errorCodeName: string = '';
  private flags: number = 0;
  private compressionContext: CompressionContext | null = null;
  private compressor: ((data: Uint8Array) => Uint8Array) | null = null;

  private constructor(requestId: number, responseTo: number) {
    this.requestId = requestId;
    this.responseTo = responseTo;
  }

  /**
   * Create a new ResponseBuilder
   */
  static create(requestId: number, responseTo: number): ResponseBuilder {
    return new ResponseBuilder(requestId, responseTo);
  }

  /**
   * Mark this as a success response
   */
  success(): this {
    this.isSuccess = true;
    return this;
  }

  /**
   * Mark this as an error response
   */
  error(code: MongoErrorCode | number, message: string, codeName?: string): this {
    this.isSuccess = false;
    this.errorCode = code;
    this.errorMessage = message;
    this.errorCodeName = codeName || getErrorCodeName(code);
    return this;
  }

  /**
   * Add data to the response
   */
  withData(data: BsonSerializable): this {
    this.responseData = { ...this.responseData, ...data };
    return this;
  }

  /**
   * Add cursor data to the response
   */
  withCursor(
    cursorId: bigint,
    namespace: string,
    batch: unknown[],
    batchType: 'firstBatch' | 'nextBatch' = 'firstBatch'
  ): this {
    this.responseData.cursor = {
      id: cursorId,
      ns: namespace,
      [batchType]: batch,
    };
    return this;
  }

  /**
   * Add write result data
   */
  withWriteResult(n: number, options?: {
    nModified?: number;
    upserted?: Array<{ index: number; _id: unknown }>;
  }): this {
    this.responseData.n = n;
    if (options?.nModified !== undefined) {
      this.responseData.nModified = options.nModified;
    }
    if (options?.upserted && options.upserted.length > 0) {
      this.responseData.upserted = options.upserted;
    }
    return this;
  }

  /**
   * Set OP_MSG flags.
   *
   * @param flags - Flag bits to set (use OpMsgResponseFlags constants)
   *
   * @example
   * ```typescript
   * builder.withFlags(OpMsgResponseFlags.moreToCome);
   * ```
   */
  withFlags(flags: number): this {
    this.flags = flags;
    return this;
  }

  /**
   * Enable CRC-32C checksum for the response.
   *
   * When enabled, a 4-byte CRC-32C checksum is appended to the message
   * and the checksumPresent flag is set automatically.
   *
   * The checksum is calculated over the entire message (excluding the
   * checksum bytes themselves).
   */
  withChecksum(): this {
    this.flags |= OpMsgResponseFlags.checksumPresent;
    return this;
  }

  /**
   * Enable compression for the response.
   *
   * When enabled, the response is wrapped in an OP_COMPRESSED message.
   * The inner OP_MSG body (excluding header) is compressed.
   *
   * @param context - Compression configuration
   * @param compressor - Function to compress the data
   *
   * @example
   * ```typescript
   * builder.withCompression(
   *   { algorithm: CompressionAlgorithm.ZLIB, level: 6 },
   *   (data) => zlibSync(data)
   * );
   * ```
   */
  withCompression(
    context: CompressionContext,
    compressor: (data: Uint8Array) => Uint8Array
  ): this {
    this.compressionContext = context;
    this.compressor = compressor;
    return this;
  }

  /**
   * Build the final wire protocol response
   */
  build(): Uint8Array {
    let doc: BsonSerializable;

    if (this.isSuccess) {
      doc = {
        ok: 1,
        ...this.responseData,
      };
    } else {
      doc = {
        ok: 0,
        errmsg: this.errorMessage,
        code: this.errorCode,
        codeName: this.errorCodeName,
        ...this.responseData,
      };
    }

    // Build the base OP_MSG
    const opMsg = buildOpMsgResponse(this.requestId, this.responseTo, doc, this.flags);

    // Apply compression if configured
    if (this.compressionContext && this.compressor) {
      const compressed = this.compressor(opMsg.slice(16)); // Compress everything after header
      return buildOpCompressed(
        OpCode.OP_MSG,
        opMsg.length - 16, // Uncompressed size (without header)
        this.compressionContext.algorithm,
        compressed,
        this.requestId,
        this.responseTo
      );
    }

    return opMsg;
  }

  /**
   * Build the response document only (without wire protocol framing)
   */
  buildDocument(): BsonSerializable {
    if (this.isSuccess) {
      return {
        ok: 1,
        ...this.responseData,
      };
    }
    return {
      ok: 0,
      errmsg: this.errorMessage,
      code: this.errorCode,
      codeName: this.errorCodeName,
      ...this.responseData,
    };
  }
}
