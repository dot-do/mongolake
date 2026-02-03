/**
 * Standardized Error Hierarchy for MongoLake
 *
 * This module provides a consistent error handling framework across the codebase.
 * All errors extend MongoLakeError which provides:
 * - Structured error codes for programmatic handling
 * - Detailed error messages for debugging
 * - Optional context/details for additional information
 * - JSON serialization for API responses
 *
 * @module errors
 */

// ============================================================================
// Error Codes
// ============================================================================

/**
 * Standard error codes used across MongoLake
 */
export const ErrorCodes = {
  // Base errors
  UNKNOWN: 'MONGOLAKE_UNKNOWN',
  INTERNAL: 'MONGOLAKE_INTERNAL',

  // Validation errors (1xx)
  VALIDATION_FAILED: 'VALIDATION_FAILED',
  INVALID_INPUT: 'VALIDATION_INVALID_INPUT',
  INVALID_FIELD: 'VALIDATION_INVALID_FIELD',
  INVALID_FILTER: 'VALIDATION_INVALID_FILTER',
  INVALID_UPDATE: 'VALIDATION_INVALID_UPDATE',
  INVALID_DOCUMENT: 'VALIDATION_INVALID_DOCUMENT',
  INVALID_PROJECTION: 'VALIDATION_INVALID_PROJECTION',
  INVALID_PIPELINE: 'VALIDATION_INVALID_PIPELINE',
  MAX_DEPTH_EXCEEDED: 'VALIDATION_MAX_DEPTH_EXCEEDED',
  INVALID_NAME: 'VALIDATION_INVALID_NAME',

  // Storage errors (2xx)
  STORAGE_ERROR: 'STORAGE_ERROR',
  STORAGE_NOT_FOUND: 'STORAGE_NOT_FOUND',
  STORAGE_WRITE_FAILED: 'STORAGE_WRITE_FAILED',
  STORAGE_READ_FAILED: 'STORAGE_READ_FAILED',
  STORAGE_DELETE_FAILED: 'STORAGE_DELETE_FAILED',
  STORAGE_INVALID_KEY: 'STORAGE_INVALID_KEY',
  STORAGE_PATH_TRAVERSAL: 'STORAGE_PATH_TRAVERSAL',
  STORAGE_QUOTA_EXCEEDED: 'STORAGE_QUOTA_EXCEEDED',
  MULTIPART_UPLOAD_FINALIZED: 'STORAGE_MULTIPART_UPLOAD_FINALIZED',

  // Authentication errors (3xx)
  AUTH_ERROR: 'AUTH_ERROR',
  AUTH_MISSING_CREDENTIALS: 'AUTH_MISSING_CREDENTIALS',
  AUTH_INVALID_TOKEN: 'AUTH_INVALID_TOKEN',
  AUTH_TOKEN_EXPIRED: 'AUTH_TOKEN_EXPIRED',
  AUTH_INVALID_API_KEY: 'AUTH_INVALID_API_KEY',
  AUTH_INSUFFICIENT_PERMISSIONS: 'AUTH_INSUFFICIENT_PERMISSIONS',
  AUTH_UNKNOWN_PROVIDER: 'AUTH_UNKNOWN_PROVIDER',
  AUTH_SIGNATURE_FAILED: 'AUTH_SIGNATURE_FAILED',

  // Query errors (4xx)
  QUERY_ERROR: 'QUERY_ERROR',
  QUERY_SYNTAX_ERROR: 'QUERY_SYNTAX_ERROR',
  QUERY_INVALID_OPERATOR: 'QUERY_INVALID_OPERATOR',
  QUERY_EXECUTION_FAILED: 'QUERY_EXECUTION_FAILED',
  QUERY_TIMEOUT: 'QUERY_TIMEOUT',
  QUERY_TOO_COMPLEX: 'QUERY_TOO_COMPLEX',
  QUERY_CURSOR_NOT_FOUND: 'QUERY_CURSOR_NOT_FOUND',

  // RPC/Network errors (5xx)
  RPC_ERROR: 'RPC_ERROR',
  RPC_TRANSIENT: 'RPC_TRANSIENT',
  RPC_SHARD_UNAVAILABLE: 'RPC_SHARD_UNAVAILABLE',
  RPC_TIMEOUT: 'RPC_TIMEOUT',
  CONNECTION_CLOSED: 'RPC_CONNECTION_CLOSED',
  RATE_LIMITED: 'RATE_LIMITED',

  // Schema errors (6xx)
  SCHEMA_ERROR: 'SCHEMA_ERROR',
  SCHEMA_INVALID: 'SCHEMA_INVALID',
  SCHEMA_EVOLUTION_FAILED: 'SCHEMA_EVOLUTION_FAILED',

  // Transaction errors (7xx)
  TRANSACTION_ERROR: 'TRANSACTION_ERROR',
  TRANSACTION_CONFLICT: 'TRANSACTION_CONFLICT',
  TRANSACTION_ABORTED: 'TRANSACTION_ABORTED',
  DUPLICATE_KEY: 'TRANSACTION_DUPLICATE_KEY',
  LOCK_TIMEOUT: 'TRANSACTION_LOCK_TIMEOUT',
  DEADLOCK: 'TRANSACTION_DEADLOCK',

  // Parquet errors (8xx)
  PARQUET_ERROR: 'PARQUET_ERROR',
  PARQUET_INVALID_MAGIC: 'PARQUET_INVALID_MAGIC',
  PARQUET_CORRUPTED: 'PARQUET_CORRUPTED',
  PARQUET_UNSUPPORTED_VERSION: 'PARQUET_UNSUPPORTED_VERSION',

  // Branch errors (9xx)
  BRANCH_ERROR: 'BRANCH_ERROR',
  BRANCH_NOT_FOUND: 'BRANCH_NOT_FOUND',
  BRANCH_EXISTS: 'BRANCH_EXISTS',
  BRANCH_MERGE_CONFLICT: 'BRANCH_MERGE_CONFLICT',
} as const;

export type ErrorCode = (typeof ErrorCodes)[keyof typeof ErrorCodes];

// ============================================================================
// Base Error Class
// ============================================================================

/**
 * Base error class for all MongoLake errors.
 *
 * Provides a consistent structure for error handling with:
 * - Error code for programmatic error identification
 * - Human-readable message
 * - Optional details object for additional context
 * - JSON serialization for API responses
 *
 * @example
 * ```typescript
 * throw new MongoLakeError(
 *   'Operation failed',
 *   ErrorCodes.INTERNAL,
 *   { operation: 'insert', collection: 'users' }
 * );
 * ```
 */
export class MongoLakeError extends Error {
  /** Error code for programmatic handling */
  public readonly code: string;

  /** Additional error context */
  public readonly details?: Record<string, unknown>;

  /** Timestamp when error occurred */
  public readonly timestamp: number;

  constructor(
    message: string,
    code: string = ErrorCodes.UNKNOWN,
    details?: Record<string, unknown>
  ) {
    super(message);
    this.name = 'MongoLakeError';
    this.code = code;
    this.details = details;
    this.timestamp = Date.now();

    // Ensure proper prototype chain for instanceof checks
    Object.setPrototypeOf(this, new.target.prototype);
  }

  /**
   * Create a JSON-serializable representation of the error
   */
  toJSON(): Record<string, unknown> {
    return {
      name: this.name,
      code: this.code,
      message: this.message,
      details: this.details,
      timestamp: this.timestamp,
    };
  }

  /**
   * Create an error response suitable for API responses
   */
  toResponse(): { error: Record<string, unknown> } {
    return {
      error: this.toJSON(),
    };
  }

  /**
   * Check if an error is a MongoLakeError
   */
  static isMongoLakeError(error: unknown): error is MongoLakeError {
    return error instanceof MongoLakeError;
  }

  /**
   * Wrap any error as a MongoLakeError
   */
  static from(error: unknown, defaultCode: string = ErrorCodes.UNKNOWN): MongoLakeError {
    if (error instanceof MongoLakeError) {
      return error;
    }

    if (error instanceof Error) {
      return new MongoLakeError(error.message, defaultCode, {
        originalName: error.name,
        originalStack: error.stack,
      });
    }

    return new MongoLakeError(String(error), defaultCode);
  }
}

// ============================================================================
// Validation Errors
// ============================================================================

/**
 * Error thrown when input validation fails.
 *
 * Used for validating user inputs like database names, collection names,
 * field names, queries, updates, and documents.
 *
 * @example
 * ```typescript
 * throw new ValidationError(
 *   'database name cannot be empty',
 *   ErrorCodes.INVALID_NAME,
 *   { field: 'database', value: '' }
 * );
 * ```
 */
export class ValidationError extends MongoLakeError {
  /** The type of validation that failed */
  public readonly validationType?: string;

  /** The invalid value (sanitized for logging) */
  public readonly invalidValue?: string;

  constructor(
    message: string,
    code: string = ErrorCodes.VALIDATION_FAILED,
    details?: Record<string, unknown> & {
      validationType?: string;
      invalidValue?: unknown;
    }
  ) {
    super(message, code, details);
    this.name = 'ValidationError';
    this.validationType = details?.validationType;

    // Sanitize the invalid value for safe logging
    if (details?.invalidValue !== undefined) {
      const val = details.invalidValue;
      if (typeof val === 'string') {
        this.invalidValue = val.length > 100 ? val.slice(0, 100) + '...' : val;
      } else if (typeof val === 'object' && val !== null) {
        this.invalidValue = '[object]';
      } else {
        this.invalidValue = String(val);
      }
    }
  }

  toJSON(): Record<string, unknown> {
    return {
      ...super.toJSON(),
      validationType: this.validationType,
      invalidValue: this.invalidValue,
    };
  }
}

// ============================================================================
// Storage Errors
// ============================================================================

/**
 * Error thrown for storage-related failures.
 *
 * Used for R2, filesystem, and other storage backend errors including:
 * - Read/write failures
 * - Path validation errors
 * - Quota exceeded
 *
 * @example
 * ```typescript
 * throw new StorageError(
 *   'Failed to write to storage',
 *   ErrorCodes.STORAGE_WRITE_FAILED,
 *   { key: 'data/file.parquet', cause: 'Network error' }
 * );
 * ```
 */
export class StorageError extends MongoLakeError {
  /** Storage key involved in the error */
  public readonly key?: string;

  /** Original error that caused this storage error */
  public readonly cause?: Error;

  constructor(
    message: string,
    code: string = ErrorCodes.STORAGE_ERROR,
    details?: Record<string, unknown> & {
      key?: string;
      cause?: Error;
    }
  ) {
    super(message, code, details);
    this.name = 'StorageError';
    this.key = details?.key;
    this.cause = details?.cause;
  }

  toJSON(): Record<string, unknown> {
    return {
      ...super.toJSON(),
      key: this.key,
      cause: this.cause?.message,
    };
  }
}

/**
 * Error thrown when a storage key is invalid or attempts path traversal.
 */
export class InvalidStorageKeyError extends StorageError {
  constructor(message: string, key?: string) {
    super(message, ErrorCodes.STORAGE_INVALID_KEY, { key });
    this.name = 'InvalidStorageKeyError';
  }
}

// ============================================================================
// Authentication Errors
// ============================================================================

/**
 * Error thrown for authentication and authorization failures.
 *
 * Used for:
 * - Missing or invalid tokens
 * - Expired credentials
 * - Insufficient permissions
 * - API key validation failures
 *
 * @example
 * ```typescript
 * throw new AuthenticationError(
 *   'Token has expired',
 *   ErrorCodes.AUTH_TOKEN_EXPIRED,
 *   { userId: 'user123', expiredAt: 1234567890 }
 * );
 * ```
 */
export class AuthenticationError extends MongoLakeError {
  /** HTTP status code to return */
  public readonly statusCode: number;

  constructor(
    message: string,
    code: string = ErrorCodes.AUTH_ERROR,
    details?: Record<string, unknown> & {
      statusCode?: number;
    }
  ) {
    super(message, code, details);
    this.name = 'AuthenticationError';
    // Default to 401, but use 403 for permission errors
    this.statusCode = details?.statusCode ?? (code === ErrorCodes.AUTH_INSUFFICIENT_PERMISSIONS ? 403 : 401);
  }

  toJSON(): Record<string, unknown> {
    return {
      ...super.toJSON(),
      statusCode: this.statusCode,
    };
  }
}

// ============================================================================
// Query Errors
// ============================================================================

/**
 * Error thrown for query execution failures.
 *
 * Used for:
 * - Invalid query syntax
 * - Query execution errors
 * - Timeout errors
 * - Invalid operators
 *
 * @example
 * ```typescript
 * throw new QueryError(
 *   'Invalid operator: $badOp',
 *   ErrorCodes.QUERY_INVALID_OPERATOR,
 *   { operator: '$badOp', query: { field: { $badOp: 1 } } }
 * );
 * ```
 */
export class QueryError extends MongoLakeError {
  constructor(
    message: string,
    code: string = ErrorCodes.QUERY_ERROR,
    details?: Record<string, unknown>
  ) {
    super(message, code, details);
    this.name = 'QueryError';
  }
}

// ============================================================================
// RPC/Network Errors
// ============================================================================

/**
 * Error thrown for RPC communication failures.
 *
 * Used for Worker-to-DO communication errors including
 * transient failures, timeouts, and shard unavailability.
 */
export class RPCError extends MongoLakeError {
  /** Remote stack trace if available */
  public readonly remoteStack?: string;

  constructor(
    message: string,
    code: string = ErrorCodes.RPC_ERROR,
    details?: Record<string, unknown> & {
      remoteStack?: string;
    }
  ) {
    super(message, code, details);
    this.name = 'RPCError';
    this.remoteStack = details?.remoteStack;

    // Preserve remote stack trace for debugging
    if (this.remoteStack) {
      this.stack = this.remoteStack + '\n' + (this.stack || '');
    }
  }
}

/**
 * Error thrown after transient network issues exhaust all retry attempts.
 */
export class TransientError extends RPCError {
  /** The original error that triggered retries */
  public readonly originalError?: Error;

  /** Number of retry attempts made */
  public readonly retryCount?: number;

  constructor(
    message: string,
    details?: {
      originalError?: Error;
      retryCount?: number;
    }
  ) {
    super(message, ErrorCodes.RPC_TRANSIENT, {
      ...details,
      originalErrorMessage: details?.originalError?.message,
    });
    this.name = 'TransientError';
    this.originalError = details?.originalError;
    this.retryCount = details?.retryCount;
  }
}

/**
 * Error thrown when a shard Durable Object is unreachable or unavailable.
 */
export class ShardUnavailableError extends RPCError {
  /** ID of the unavailable shard */
  public readonly shardId: number;

  /** The original error that caused unavailability */
  public readonly originalError?: Error;

  constructor(
    message: string,
    shardId: number,
    originalError?: Error
  ) {
    super(message, ErrorCodes.RPC_SHARD_UNAVAILABLE, {
      shardId,
      originalErrorMessage: originalError?.message,
    });
    this.name = 'ShardUnavailableError';
    this.shardId = shardId;
    this.originalError = originalError;
  }
}

// ============================================================================
// Schema Errors
// ============================================================================

/**
 * Error thrown for schema-related failures.
 */
export class SchemaError extends MongoLakeError {
  constructor(
    message: string,
    code: string = ErrorCodes.SCHEMA_ERROR,
    details?: Record<string, unknown>
  ) {
    super(message, code, details);
    this.name = 'SchemaError';
  }
}

// ============================================================================
// Transaction Errors
// ============================================================================

/**
 * Error thrown for transaction-related failures.
 */
export class TransactionError extends MongoLakeError {
  constructor(
    message: string,
    code: string = ErrorCodes.TRANSACTION_ERROR,
    details?: Record<string, unknown>
  ) {
    super(message, code, details);
    this.name = 'TransactionError';
  }
}

// ============================================================================
// Parquet Errors
// ============================================================================

/**
 * Error thrown for Parquet file format errors.
 */
export class ParquetError extends MongoLakeError {
  constructor(
    message: string,
    code: string = ErrorCodes.PARQUET_ERROR,
    details?: Record<string, unknown>
  ) {
    super(message, code, details);
    this.name = 'ParquetError';
  }
}

// ============================================================================
// Branch Errors
// ============================================================================

/**
 * Error thrown for branch-related failures.
 */
export class BranchError extends MongoLakeError {
  constructor(
    message: string,
    code: string = ErrorCodes.BRANCH_ERROR,
    details?: Record<string, unknown>
  ) {
    super(message, code, details);
    this.name = 'BranchError';
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Check if an error has a specific error code
 */
export function hasErrorCode(error: unknown, code: string): boolean {
  return error instanceof MongoLakeError && error.code === code;
}

/**
 * Check if an error is retryable (transient network error or rate limit)
 */
export function isRetryableError(error: unknown): boolean {
  if (error instanceof TransientError || error instanceof ShardUnavailableError) {
    return true;
  }
  if (error instanceof MongoLakeError) {
    return (
      error.code === ErrorCodes.RPC_TRANSIENT ||
      error.code === ErrorCodes.RPC_TIMEOUT ||
      error.code === ErrorCodes.RATE_LIMITED
    );
  }
  return false;
}

/**
 * Create an appropriate MongoLakeError from an unknown error
 */
export function wrapError(error: unknown, context?: string): MongoLakeError {
  if (error instanceof MongoLakeError) {
    return error;
  }

  const message = context
    ? `${context}: ${error instanceof Error ? error.message : String(error)}`
    : error instanceof Error
      ? error.message
      : String(error);

  return new MongoLakeError(message, ErrorCodes.INTERNAL, {
    originalError: error instanceof Error ? error.name : typeof error,
  });
}
