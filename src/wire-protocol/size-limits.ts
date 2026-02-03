/**
 * MongoDB Wire Protocol Size Limits
 *
 * Implements request/response size limits for the wire protocol, matching MongoDB's
 * limits for compatibility and protection against resource exhaustion.
 *
 * Features:
 * - Request size validation (configurable, default 16MB)
 * - Response size limits with streaming for large responses
 * - Document size validation (16MB default per MongoDB spec)
 * - Batch operation limits
 * - Proper error responses for oversized payloads
 * - Configuration options for all limits
 *
 * MongoDB Limits Reference:
 * - Max BSON Document Size: 16MB (16,777,216 bytes)
 * - Max Message Size: 48MB (50,331,648 bytes)
 * - Max Batch Size: 100,000 documents (for insert/update/delete)
 *
 * @module wire-protocol/size-limits
 */

import {
  MAX_WIRE_MESSAGE_SIZE,
  MAX_BATCH_SIZE,
  MAX_BATCH_BYTES,
} from '../constants.js';
import {
  buildErrorResponse,
  ResponseBuilder,
} from './bson-serializer.js';

// ============================================================================
// Configuration Types
// ============================================================================

/**
 * Configuration options for wire protocol size limits
 */
export interface SizeLimitConfig {
  /**
   * Maximum request message size in bytes
   * Default: 16MB (16,777,216 bytes) - matching MongoDB's default
   */
  maxRequestSize?: number;

  /**
   * Maximum response message size in bytes
   * Default: 16MB (16,777,216 bytes)
   * Larger responses should be streamed via cursor batches
   */
  maxResponseSize?: number;

  /**
   * Maximum single document size in bytes
   * Default: 16MB (16,777,216 bytes) - per MongoDB specification
   */
  maxDocumentSize?: number;

  /**
   * Maximum number of documents in a batch operation
   * Default: 100,000 - matching MongoDB's maxWriteBatchSize
   */
  maxBatchCount?: number;

  /**
   * Maximum total size of documents in a batch operation
   * Default: 16MB (16,777,216 bytes)
   */
  maxBatchSize?: number;

  /**
   * Target batch size for cursor responses (in bytes)
   * When a response exceeds this, remaining docs go to cursor batches
   * Default: 4MB (4,194,304 bytes)
   */
  cursorBatchTargetSize?: number;

  /**
   * Maximum wire protocol message size (header + body)
   * Default: 48MB (50,331,648 bytes) - MongoDB's OP_MSG limit
   */
  maxWireMessageSize?: number;

  /**
   * Enable strict mode that rejects all oversized payloads
   * When false, allows some graceful degradation
   * Default: true
   */
  strictMode?: boolean;
}

/**
 * Resolved size limit configuration with all defaults applied
 */
export interface ResolvedSizeLimitConfig {
  maxRequestSize: number;
  maxResponseSize: number;
  maxDocumentSize: number;
  maxBatchCount: number;
  maxBatchSize: number;
  cursorBatchTargetSize: number;
  maxWireMessageSize: number;
  strictMode: boolean;
}

// ============================================================================
// Default Values
// ============================================================================

/**
 * MongoDB standard document size limit (16MB)
 */
export const MONGODB_MAX_DOCUMENT_SIZE = 16 * 1024 * 1024; // 16MB

/**
 * MongoDB standard message size limit (48MB)
 */
export const MONGODB_MAX_MESSAGE_SIZE = MAX_WIRE_MESSAGE_SIZE;

/**
 * MongoDB standard batch size limit (100,000 documents)
 */
export const MONGODB_MAX_BATCH_COUNT = MAX_BATCH_SIZE;

/**
 * Default batch bytes limit (matches constants)
 */
export const DEFAULT_MAX_BATCH_BYTES = MAX_BATCH_BYTES;

/**
 * Default target size for cursor batches (4MB)
 */
export const DEFAULT_CURSOR_BATCH_TARGET_SIZE = 4 * 1024 * 1024;

/**
 * Default configuration with MongoDB-compatible limits
 */
export const DEFAULT_SIZE_LIMITS: ResolvedSizeLimitConfig = {
  maxRequestSize: MONGODB_MAX_DOCUMENT_SIZE,
  maxResponseSize: MONGODB_MAX_DOCUMENT_SIZE,
  maxDocumentSize: MONGODB_MAX_DOCUMENT_SIZE,
  maxBatchCount: MONGODB_MAX_BATCH_COUNT,
  maxBatchSize: DEFAULT_MAX_BATCH_BYTES,
  cursorBatchTargetSize: DEFAULT_CURSOR_BATCH_TARGET_SIZE,
  maxWireMessageSize: MONGODB_MAX_MESSAGE_SIZE,
  strictMode: true,
};

// ============================================================================
// Error Types
// ============================================================================

/**
 * Error codes for size limit violations
 */
export const SizeLimitErrorCode = {
  /** Request exceeds maximum size */
  RequestTooLarge: 10334,
  /** Response exceeds maximum size */
  ResponseTooLarge: 10335,
  /** Single document exceeds maximum size */
  DocumentTooLarge: 10334,
  /** Batch contains too many documents */
  BatchTooLarge: 10336,
  /** Batch total size exceeds limit */
  BatchSizeTooLarge: 10337,
  /** Wire message exceeds protocol limit */
  MessageTooLarge: 10338,
} as const;

export type SizeLimitErrorCode = (typeof SizeLimitErrorCode)[keyof typeof SizeLimitErrorCode];

/**
 * Error thrown when a size limit is exceeded
 */
export class SizeLimitError extends Error {
  readonly code: SizeLimitErrorCode | number;
  readonly codeName: string;
  readonly actualSize: number;
  readonly maxSize: number;
  readonly limitType: string;

  constructor(
    message: string,
    code: SizeLimitErrorCode | number,
    codeName: string,
    actualSize: number,
    maxSize: number,
    limitType: string
  ) {
    super(message);
    this.name = 'SizeLimitError';
    this.code = code;
    this.codeName = codeName;
    this.actualSize = actualSize;
    this.maxSize = maxSize;
    this.limitType = limitType;
  }

  /**
   * Build an error response for this size limit violation
   */
  toErrorResponse(requestId: number, responseTo: number): Uint8Array {
    return buildErrorResponse(
      requestId,
      responseTo,
      this.code,
      this.message,
      this.codeName
    );
  }
}

// ============================================================================
// Size Limit Validator
// ============================================================================

/**
 * Validation result for size checks
 */
export interface SizeValidationResult {
  valid: boolean;
  error?: SizeLimitError;
  actualSize: number;
  maxSize: number;
}

/**
 * Batch validation result with detailed information
 */
export interface BatchValidationResult extends SizeValidationResult {
  documentCount: number;
  totalSize: number;
  oversizedDocuments: number[];
}

/**
 * Validates wire protocol size limits.
 *
 * This class provides comprehensive size validation for MongoDB wire protocol
 * messages, documents, and batch operations.
 *
 * @example
 * ```typescript
 * const validator = new SizeLimitValidator();
 *
 * // Validate a request
 * const result = validator.validateRequest(messageBytes);
 * if (!result.valid) {
 *   return result.error!.toErrorResponse(requestId, responseTo);
 * }
 *
 * // Validate documents before insert
 * const batchResult = validator.validateBatch(documents);
 * if (!batchResult.valid) {
 *   throw batchResult.error;
 * }
 * ```
 */
export class SizeLimitValidator {
  private readonly config: ResolvedSizeLimitConfig;

  /**
   * Create a new size limit validator
   *
   * @param config - Optional configuration overrides
   */
  constructor(config: SizeLimitConfig = {}) {
    this.config = {
      ...DEFAULT_SIZE_LIMITS,
      ...config,
    };
  }

  /**
   * Get the current configuration
   */
  getConfig(): Readonly<ResolvedSizeLimitConfig> {
    return this.config;
  }

  /**
   * Validate request message size
   *
   * @param data - Raw message bytes or size
   * @returns Validation result
   */
  validateRequest(data: Uint8Array | number): SizeValidationResult {
    const size = typeof data === 'number' ? data : data.length;

    // Check wire message limit first (absolute maximum)
    if (size > this.config.maxWireMessageSize) {
      return {
        valid: false,
        error: new SizeLimitError(
          `Message size ${size} bytes exceeds maximum wire message size of ${this.config.maxWireMessageSize} bytes`,
          SizeLimitErrorCode.MessageTooLarge,
          'MessageTooLarge',
          size,
          this.config.maxWireMessageSize,
          'wireMessage'
        ),
        actualSize: size,
        maxSize: this.config.maxWireMessageSize,
      };
    }

    // Check request size limit
    if (size > this.config.maxRequestSize) {
      return {
        valid: false,
        error: new SizeLimitError(
          `Request size ${size} bytes exceeds maximum request size of ${this.config.maxRequestSize} bytes`,
          SizeLimitErrorCode.RequestTooLarge,
          'RequestTooLarge',
          size,
          this.config.maxRequestSize,
          'request'
        ),
        actualSize: size,
        maxSize: this.config.maxRequestSize,
      };
    }

    return {
      valid: true,
      actualSize: size,
      maxSize: this.config.maxRequestSize,
    };
  }

  /**
   * Validate response message size
   *
   * @param data - Raw message bytes or size
   * @returns Validation result
   */
  validateResponse(data: Uint8Array | number): SizeValidationResult {
    const size = typeof data === 'number' ? data : data.length;

    // Check wire message limit first
    if (size > this.config.maxWireMessageSize) {
      return {
        valid: false,
        error: new SizeLimitError(
          `Response size ${size} bytes exceeds maximum wire message size of ${this.config.maxWireMessageSize} bytes`,
          SizeLimitErrorCode.MessageTooLarge,
          'MessageTooLarge',
          size,
          this.config.maxWireMessageSize,
          'wireMessage'
        ),
        actualSize: size,
        maxSize: this.config.maxWireMessageSize,
      };
    }

    // Check response size limit
    if (size > this.config.maxResponseSize) {
      return {
        valid: false,
        error: new SizeLimitError(
          `Response size ${size} bytes exceeds maximum response size of ${this.config.maxResponseSize} bytes`,
          SizeLimitErrorCode.ResponseTooLarge,
          'ResponseTooLarge',
          size,
          this.config.maxResponseSize,
          'response'
        ),
        actualSize: size,
        maxSize: this.config.maxResponseSize,
      };
    }

    return {
      valid: true,
      actualSize: size,
      maxSize: this.config.maxResponseSize,
    };
  }

  /**
   * Validate a single document size
   *
   * @param data - Document bytes or size
   * @returns Validation result
   */
  validateDocument(data: Uint8Array | number): SizeValidationResult {
    const size = typeof data === 'number' ? data : data.length;

    if (size > this.config.maxDocumentSize) {
      return {
        valid: false,
        error: new SizeLimitError(
          `Document size ${size} bytes exceeds maximum document size of ${this.config.maxDocumentSize} bytes`,
          SizeLimitErrorCode.DocumentTooLarge,
          'BSONObjectTooLarge',
          size,
          this.config.maxDocumentSize,
          'document'
        ),
        actualSize: size,
        maxSize: this.config.maxDocumentSize,
      };
    }

    return {
      valid: true,
      actualSize: size,
      maxSize: this.config.maxDocumentSize,
    };
  }

  /**
   * Validate a batch of documents (for insert/update/delete operations)
   *
   * @param documents - Array of document sizes or serialized documents
   * @returns Batch validation result with detailed information
   */
  validateBatch(documents: Array<Uint8Array | number>): BatchValidationResult {
    const documentCount = documents.length;
    let totalSize = 0;
    const oversizedDocuments: number[] = [];

    // Check document count limit
    if (documentCount > this.config.maxBatchCount) {
      return {
        valid: false,
        error: new SizeLimitError(
          `Batch contains ${documentCount} documents, exceeds maximum of ${this.config.maxBatchCount} documents`,
          SizeLimitErrorCode.BatchTooLarge,
          'TooManyDocuments',
          documentCount,
          this.config.maxBatchCount,
          'batchCount'
        ),
        actualSize: documentCount,
        maxSize: this.config.maxBatchCount,
        documentCount,
        totalSize: 0,
        oversizedDocuments: [],
      };
    }

    // Validate each document and calculate total size
    for (let i = 0; i < documents.length; i++) {
      const doc = documents[i]!;
      const docSize = typeof doc === 'number' ? doc : doc.length;
      totalSize += docSize;

      // Check individual document size
      if (docSize > this.config.maxDocumentSize) {
        oversizedDocuments.push(i);
      }
    }

    // Report first oversized document if any
    if (oversizedDocuments.length > 0) {
      const firstOversized = oversizedDocuments[0]!;
      const doc = documents[firstOversized]!;
      const docSize = typeof doc === 'number' ? doc : doc.length;

      return {
        valid: false,
        error: new SizeLimitError(
          `Document at index ${firstOversized} is ${docSize} bytes, exceeds maximum of ${this.config.maxDocumentSize} bytes`,
          SizeLimitErrorCode.DocumentTooLarge,
          'BSONObjectTooLarge',
          docSize,
          this.config.maxDocumentSize,
          'document'
        ),
        actualSize: docSize,
        maxSize: this.config.maxDocumentSize,
        documentCount,
        totalSize,
        oversizedDocuments,
      };
    }

    // Check total batch size
    if (totalSize > this.config.maxBatchSize) {
      return {
        valid: false,
        error: new SizeLimitError(
          `Batch total size ${totalSize} bytes exceeds maximum of ${this.config.maxBatchSize} bytes`,
          SizeLimitErrorCode.BatchSizeTooLarge,
          'BatchTooLarge',
          totalSize,
          this.config.maxBatchSize,
          'batchSize'
        ),
        actualSize: totalSize,
        maxSize: this.config.maxBatchSize,
        documentCount,
        totalSize,
        oversizedDocuments,
      };
    }

    return {
      valid: true,
      actualSize: totalSize,
      maxSize: this.config.maxBatchSize,
      documentCount,
      totalSize,
      oversizedDocuments: [],
    };
  }

  /**
   * Calculate how many documents fit within a size limit
   *
   * Useful for determining cursor batch sizes.
   *
   * @param documentSizes - Array of document sizes
   * @param maxSize - Maximum total size (default: cursorBatchTargetSize)
   * @returns Number of documents that fit
   */
  calculateBatchFit(
    documentSizes: number[],
    maxSize: number = this.config.cursorBatchTargetSize
  ): number {
    let totalSize = 0;
    let count = 0;

    for (const size of documentSizes) {
      if (totalSize + size > maxSize && count > 0) {
        break;
      }
      totalSize += size;
      count++;
    }

    return count;
  }

  /**
   * Check if a response needs to be streamed via cursor batches
   *
   * @param totalSize - Total size of all response documents
   * @returns true if streaming is recommended
   */
  shouldStream(totalSize: number): boolean {
    return totalSize > this.config.cursorBatchTargetSize;
  }
}

// ============================================================================
// Streaming Response Builder
// ============================================================================

/**
 * Result of building a potentially streamed response
 */
export interface StreamedResponseResult {
  /** First response to send immediately */
  response: Uint8Array;
  /** Whether more batches are available via cursor */
  hasMore: boolean;
  /** Cursor ID if streaming (0n if complete) */
  cursorId: bigint;
  /** Documents included in first response */
  includedCount: number;
  /** Remaining documents for cursor batches */
  remainingCount: number;
}

/**
 * Builds responses that respect size limits, automatically using cursors
 * for large result sets.
 *
 * @example
 * ```typescript
 * const builder = new StreamingResponseBuilder(validator);
 *
 * // Build a response with automatic streaming
 * const result = builder.buildResponse(
 *   requestId,
 *   responseTo,
 *   'testdb.users',
 *   documents,
 *   documentSizes
 * );
 *
 * // Send first response
 * socket.write(result.response);
 *
 * // Store remaining docs if more batches needed
 * if (result.hasMore) {
 *   cursorStore.set(result.cursorId, remainingDocs);
 * }
 * ```
 */
export class StreamingResponseBuilder {
  private readonly validator: SizeLimitValidator;
  private cursorIdCounter: bigint = 1n;

  constructor(validator?: SizeLimitValidator) {
    this.validator = validator ?? new SizeLimitValidator();
  }

  /**
   * Build a response that respects size limits
   *
   * If the total response would exceed limits, this returns the first batch
   * and indicates that more data is available via cursor.
   *
   * @param requestId - Request ID for response
   * @param responseTo - Request ID being responded to
   * @param namespace - Full namespace (database.collection)
   * @param documents - All result documents
   * @param documentSizes - Size of each serialized document (for efficient batching)
   * @param generateCursorId - Optional function to generate cursor ID
   * @returns Streamed response result
   */
  buildResponse(
    requestId: number,
    responseTo: number,
    namespace: string,
    documents: unknown[],
    documentSizes: number[],
    generateCursorId?: () => bigint
  ): StreamedResponseResult {
    const config = this.validator.getConfig();
    const totalSize = documentSizes.reduce((sum, size) => sum + size, 0);

    // Check if we need to stream
    if (!this.validator.shouldStream(totalSize)) {
      // All documents fit in one response
      const response = ResponseBuilder.create(requestId, responseTo)
        .success()
        .withCursor(0n, namespace, documents, 'firstBatch')
        .build();

      return {
        response,
        hasMore: false,
        cursorId: 0n,
        includedCount: documents.length,
        remainingCount: 0,
      };
    }

    // Need to stream - calculate how many docs fit in first batch
    const batchCount = this.validator.calculateBatchFit(
      documentSizes,
      config.cursorBatchTargetSize
    );

    // Get first batch
    const firstBatch = documents.slice(0, batchCount);
    const remainingCount = documents.length - batchCount;

    // Generate cursor ID if more data remains
    const cursorId = remainingCount > 0
      ? (generateCursorId?.() ?? this.nextCursorId())
      : 0n;

    const response = ResponseBuilder.create(requestId, responseTo)
      .success()
      .withCursor(cursorId, namespace, firstBatch, 'firstBatch')
      .build();

    return {
      response,
      hasMore: remainingCount > 0,
      cursorId,
      includedCount: batchCount,
      remainingCount,
    };
  }

  /**
   * Build a getMore response for cursor continuation
   *
   * @param requestId - Request ID for response
   * @param responseTo - Request ID being responded to
   * @param cursorId - Current cursor ID
   * @param namespace - Full namespace
   * @param documents - Remaining documents
   * @param documentSizes - Size of each document
   * @param startIndex - Starting index in the remaining documents
   * @returns Streamed response result
   */
  buildGetMoreResponse(
    requestId: number,
    responseTo: number,
    cursorId: bigint,
    namespace: string,
    documents: unknown[],
    documentSizes: number[],
    startIndex: number = 0
  ): StreamedResponseResult {
    const config = this.validator.getConfig();
    const remainingDocs = documents.slice(startIndex);
    const remainingSizes = documentSizes.slice(startIndex);

    // Calculate how many docs fit in this batch
    const batchCount = this.validator.calculateBatchFit(
      remainingSizes,
      config.cursorBatchTargetSize
    );

    const nextBatch = remainingDocs.slice(0, batchCount);
    const stillRemaining = remainingDocs.length - batchCount;

    // Set cursor ID to 0 if this is the last batch
    const nextCursorId = stillRemaining > 0 ? cursorId : 0n;

    const response = ResponseBuilder.create(requestId, responseTo)
      .success()
      .withCursor(nextCursorId, namespace, nextBatch, 'nextBatch')
      .build();

    return {
      response,
      hasMore: stillRemaining > 0,
      cursorId: nextCursorId,
      includedCount: batchCount,
      remainingCount: stillRemaining,
    };
  }

  /**
   * Generate a unique cursor ID
   */
  private nextCursorId(): bigint {
    return this.cursorIdCounter++;
  }
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Create a size limit validator with custom configuration
 *
 * @param config - Configuration overrides
 * @returns Configured validator
 */
export function createSizeLimitValidator(
  config?: SizeLimitConfig
): SizeLimitValidator {
  return new SizeLimitValidator(config);
}

/**
 * Create a streaming response builder with custom configuration
 *
 * @param config - Configuration overrides
 * @returns Configured builder
 */
export function createStreamingResponseBuilder(
  config?: SizeLimitConfig
): StreamingResponseBuilder {
  const validator = new SizeLimitValidator(config);
  return new StreamingResponseBuilder(validator);
}

/**
 * Quick validation check for request size
 *
 * @param size - Size in bytes
 * @param maxSize - Maximum allowed size (default: 16MB)
 * @returns true if size is within limit
 */
export function isValidRequestSize(
  size: number,
  maxSize: number = MONGODB_MAX_DOCUMENT_SIZE
): boolean {
  return size > 0 && size <= maxSize;
}

/**
 * Quick validation check for document size
 *
 * @param size - Size in bytes
 * @param maxSize - Maximum allowed size (default: 16MB)
 * @returns true if size is within limit
 */
export function isValidDocumentSize(
  size: number,
  maxSize: number = MONGODB_MAX_DOCUMENT_SIZE
): boolean {
  return size > 0 && size <= maxSize;
}

/**
 * Format bytes into human-readable string
 *
 * @param bytes - Number of bytes
 * @returns Formatted string (e.g., "16.5 MB")
 */
export function formatBytes(bytes: number): string {
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let unitIndex = 0;
  let size = bytes;

  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex++;
  }

  return `${size.toFixed(unitIndex === 0 ? 0 : 2)} ${units[unitIndex]}`;
}

/**
 * Build an error response for a size limit violation
 *
 * @param requestId - Request ID
 * @param responseTo - Request ID being responded to
 * @param error - Size limit error
 * @returns Wire protocol error response
 */
export function buildSizeLimitErrorResponse(
  requestId: number,
  responseTo: number,
  error: SizeLimitError
): Uint8Array {
  return error.toErrorResponse(requestId, responseTo);
}
