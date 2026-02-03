/**
 * Tests for the standardized error hierarchy
 */

import { describe, it, expect } from 'vitest';
import {
  MongoLakeError,
  ValidationError,
  StorageError,
  InvalidStorageKeyError,
  AuthenticationError,
  QueryError,
  RPCError,
  TransientError,
  ShardUnavailableError,
  SchemaError,
  TransactionError,
  ParquetError,
  BranchError,
  ErrorCodes,
  hasErrorCode,
  isRetryableError,
  wrapError,
} from '../../../src/errors/index.js';

describe('MongoLakeError', () => {
  it('should create error with message', () => {
    const error = new MongoLakeError('Test error');
    expect(error.message).toBe('Test error');
    expect(error.name).toBe('MongoLakeError');
    expect(error.code).toBe(ErrorCodes.UNKNOWN);
  });

  it('should create error with code and details', () => {
    const error = new MongoLakeError('Test error', ErrorCodes.INTERNAL, { foo: 'bar' });
    expect(error.code).toBe(ErrorCodes.INTERNAL);
    expect(error.details).toEqual({ foo: 'bar' });
    expect(error.timestamp).toBeGreaterThan(0);
  });

  it('should serialize to JSON', () => {
    const error = new MongoLakeError('Test error', ErrorCodes.INTERNAL, { foo: 'bar' });
    const json = error.toJSON();
    expect(json.name).toBe('MongoLakeError');
    expect(json.code).toBe(ErrorCodes.INTERNAL);
    expect(json.message).toBe('Test error');
    expect(json.details).toEqual({ foo: 'bar' });
    expect(json.timestamp).toBeGreaterThan(0);
  });

  it('should create response format', () => {
    const error = new MongoLakeError('Test error');
    const response = error.toResponse();
    expect(response).toHaveProperty('error');
    expect(response.error.message).toBe('Test error');
  });

  it('should support instanceof checks', () => {
    const error = new MongoLakeError('Test error');
    expect(error instanceof MongoLakeError).toBe(true);
    expect(error instanceof Error).toBe(true);
  });

  it('should wrap other errors', () => {
    const original = new Error('Original error');
    const wrapped = MongoLakeError.from(original);
    expect(wrapped.message).toBe('Original error');
    expect(wrapped.details?.originalName).toBe('Error');
  });

  it('should pass through MongoLakeErrors unchanged', () => {
    const original = new MongoLakeError('Original', ErrorCodes.INTERNAL);
    const wrapped = MongoLakeError.from(original);
    expect(wrapped).toBe(original);
  });

  it('should wrap non-Error values', () => {
    const wrapped = MongoLakeError.from('string error');
    expect(wrapped.message).toBe('string error');
  });
});

describe('ValidationError', () => {
  it('should extend MongoLakeError', () => {
    const error = new ValidationError('Invalid input');
    expect(error instanceof ValidationError).toBe(true);
    expect(error instanceof MongoLakeError).toBe(true);
    expect(error.name).toBe('ValidationError');
  });

  it('should support validation type', () => {
    const error = new ValidationError('Invalid name', ErrorCodes.INVALID_NAME, {
      validationType: 'database_name',
      invalidValue: 'bad/name',
    });
    expect(error.validationType).toBe('database_name');
    expect(error.invalidValue).toBe('bad/name');
  });

  it('should truncate long invalid values', () => {
    const longValue = 'x'.repeat(200);
    const error = new ValidationError('Invalid input', ErrorCodes.VALIDATION_FAILED, {
      invalidValue: longValue,
    });
    expect(error.invalidValue?.length).toBeLessThanOrEqual(103); // 100 + '...'
    expect(error.invalidValue?.endsWith('...')).toBe(true);
  });

  it('should sanitize object invalid values', () => {
    const error = new ValidationError('Invalid input', ErrorCodes.VALIDATION_FAILED, {
      invalidValue: { nested: 'object' },
    });
    expect(error.invalidValue).toBe('[object]');
  });
});

describe('StorageError', () => {
  it('should extend MongoLakeError', () => {
    const error = new StorageError('Write failed');
    expect(error instanceof StorageError).toBe(true);
    expect(error instanceof MongoLakeError).toBe(true);
    expect(error.name).toBe('StorageError');
  });

  it('should include key in details', () => {
    const error = new StorageError('Not found', ErrorCodes.STORAGE_NOT_FOUND, {
      key: 'path/to/file.parquet',
    });
    expect(error.key).toBe('path/to/file.parquet');
  });

  it('should include cause', () => {
    const cause = new Error('Network error');
    const error = new StorageError('Write failed', ErrorCodes.STORAGE_WRITE_FAILED, {
      cause,
    });
    expect(error.cause).toBe(cause);
    expect(error.toJSON().cause).toBe('Network error');
  });
});

describe('InvalidStorageKeyError', () => {
  it('should extend StorageError', () => {
    const error = new InvalidStorageKeyError('Invalid key');
    expect(error instanceof InvalidStorageKeyError).toBe(true);
    expect(error instanceof StorageError).toBe(true);
    expect(error.name).toBe('InvalidStorageKeyError');
    expect(error.code).toBe(ErrorCodes.STORAGE_INVALID_KEY);
  });
});

describe('AuthenticationError', () => {
  it('should extend MongoLakeError', () => {
    const error = new AuthenticationError('Invalid token');
    expect(error instanceof AuthenticationError).toBe(true);
    expect(error instanceof MongoLakeError).toBe(true);
    expect(error.name).toBe('AuthenticationError');
  });

  it('should default to 401 status', () => {
    const error = new AuthenticationError('Invalid token');
    expect(error.statusCode).toBe(401);
  });

  it('should use 403 for permission errors', () => {
    const error = new AuthenticationError('Forbidden', ErrorCodes.AUTH_INSUFFICIENT_PERMISSIONS);
    expect(error.statusCode).toBe(403);
  });

  it('should allow custom status code', () => {
    const error = new AuthenticationError('Custom', ErrorCodes.AUTH_ERROR, {
      statusCode: 429,
    });
    expect(error.statusCode).toBe(429);
  });
});

describe('QueryError', () => {
  it('should extend MongoLakeError', () => {
    const error = new QueryError('Invalid operator');
    expect(error instanceof QueryError).toBe(true);
    expect(error instanceof MongoLakeError).toBe(true);
    expect(error.name).toBe('QueryError');
  });
});

describe('RPCError', () => {
  it('should extend MongoLakeError', () => {
    const error = new RPCError('RPC failed');
    expect(error instanceof RPCError).toBe(true);
    expect(error instanceof MongoLakeError).toBe(true);
    expect(error.name).toBe('RPCError');
  });

  it('should preserve remote stack trace', () => {
    const error = new RPCError('RPC failed', ErrorCodes.RPC_ERROR, {
      remoteStack: 'Remote:\n  at remote.js:1',
    });
    expect(error.remoteStack).toBe('Remote:\n  at remote.js:1');
    expect(error.stack).toContain('Remote:');
  });
});

describe('TransientError', () => {
  it('should extend RPCError', () => {
    const error = new TransientError('Network error');
    expect(error instanceof TransientError).toBe(true);
    expect(error instanceof RPCError).toBe(true);
    expect(error.name).toBe('TransientError');
    expect(error.code).toBe(ErrorCodes.RPC_TRANSIENT);
  });

  it('should track retry count', () => {
    const original = new Error('Connection reset');
    const error = new TransientError('Retries exhausted', { originalError: original, retryCount: 3 });
    expect(error.originalError).toBe(original);
    expect(error.retryCount).toBe(3);
  });
});

describe('ShardUnavailableError', () => {
  it('should extend RPCError', () => {
    const error = new ShardUnavailableError('Shard down', 5);
    expect(error instanceof ShardUnavailableError).toBe(true);
    expect(error instanceof RPCError).toBe(true);
    expect(error.name).toBe('ShardUnavailableError');
    expect(error.code).toBe(ErrorCodes.RPC_SHARD_UNAVAILABLE);
    expect(error.shardId).toBe(5);
  });
});

describe('Domain-specific errors', () => {
  it('SchemaError should extend MongoLakeError', () => {
    const error = new SchemaError('Invalid schema');
    expect(error instanceof SchemaError).toBe(true);
    expect(error instanceof MongoLakeError).toBe(true);
    expect(error.name).toBe('SchemaError');
  });

  it('TransactionError should extend MongoLakeError', () => {
    const error = new TransactionError('Transaction aborted');
    expect(error instanceof TransactionError).toBe(true);
    expect(error instanceof MongoLakeError).toBe(true);
    expect(error.name).toBe('TransactionError');
  });

  it('ParquetError should extend MongoLakeError', () => {
    const error = new ParquetError('Invalid magic bytes');
    expect(error instanceof ParquetError).toBe(true);
    expect(error instanceof MongoLakeError).toBe(true);
    expect(error.name).toBe('ParquetError');
  });

  it('BranchError should extend MongoLakeError', () => {
    const error = new BranchError('Branch not found');
    expect(error instanceof BranchError).toBe(true);
    expect(error instanceof MongoLakeError).toBe(true);
    expect(error.name).toBe('BranchError');
  });
});

describe('hasErrorCode', () => {
  it('should return true for matching code', () => {
    const error = new ValidationError('Test', ErrorCodes.VALIDATION_FAILED);
    expect(hasErrorCode(error, ErrorCodes.VALIDATION_FAILED)).toBe(true);
  });

  it('should return false for non-matching code', () => {
    const error = new ValidationError('Test', ErrorCodes.VALIDATION_FAILED);
    expect(hasErrorCode(error, ErrorCodes.STORAGE_ERROR)).toBe(false);
  });

  it('should return false for non-MongoLakeError', () => {
    const error = new Error('Test');
    expect(hasErrorCode(error, ErrorCodes.UNKNOWN)).toBe(false);
  });
});

describe('isRetryableError', () => {
  it('should return true for TransientError', () => {
    const error = new TransientError('Network error');
    expect(isRetryableError(error)).toBe(true);
  });

  it('should return true for ShardUnavailableError', () => {
    const error = new ShardUnavailableError('Shard down', 1);
    expect(isRetryableError(error)).toBe(true);
  });

  it('should return true for RPC_TRANSIENT code', () => {
    const error = new MongoLakeError('Transient', ErrorCodes.RPC_TRANSIENT);
    expect(isRetryableError(error)).toBe(true);
  });

  it('should return true for RPC_TIMEOUT code', () => {
    const error = new MongoLakeError('Timeout', ErrorCodes.RPC_TIMEOUT);
    expect(isRetryableError(error)).toBe(true);
  });

  it('should return false for other errors', () => {
    const error = new ValidationError('Invalid');
    expect(isRetryableError(error)).toBe(false);
  });
});

describe('wrapError', () => {
  it('should pass through MongoLakeErrors', () => {
    const original = new ValidationError('Test');
    const wrapped = wrapError(original);
    expect(wrapped).toBe(original);
  });

  it('should wrap regular errors', () => {
    const original = new Error('Test');
    const wrapped = wrapError(original);
    expect(wrapped).not.toBe(original);
    expect(wrapped.message).toBe('Test');
    expect(wrapped.code).toBe(ErrorCodes.INTERNAL);
  });

  it('should add context to message', () => {
    const original = new Error('Network error');
    const wrapped = wrapError(original, 'Failed to fetch data');
    expect(wrapped.message).toBe('Failed to fetch data: Network error');
  });

  it('should wrap string errors', () => {
    const wrapped = wrapError('Something went wrong', 'Context');
    expect(wrapped.message).toBe('Context: Something went wrong');
  });
});

describe('ErrorCodes', () => {
  it('should have all expected error codes', () => {
    // Validation
    expect(ErrorCodes.VALIDATION_FAILED).toBe('VALIDATION_FAILED');
    expect(ErrorCodes.INVALID_INPUT).toBe('VALIDATION_INVALID_INPUT');

    // Storage
    expect(ErrorCodes.STORAGE_ERROR).toBe('STORAGE_ERROR');
    expect(ErrorCodes.STORAGE_NOT_FOUND).toBe('STORAGE_NOT_FOUND');

    // Auth
    expect(ErrorCodes.AUTH_ERROR).toBe('AUTH_ERROR');
    expect(ErrorCodes.AUTH_TOKEN_EXPIRED).toBe('AUTH_TOKEN_EXPIRED');

    // Query
    expect(ErrorCodes.QUERY_ERROR).toBe('QUERY_ERROR');
    expect(ErrorCodes.QUERY_TIMEOUT).toBe('QUERY_TIMEOUT');

    // RPC
    expect(ErrorCodes.RPC_ERROR).toBe('RPC_ERROR');
    expect(ErrorCodes.RPC_TRANSIENT).toBe('RPC_TRANSIENT');

    // Schema
    expect(ErrorCodes.SCHEMA_ERROR).toBe('SCHEMA_ERROR');

    // Transaction
    expect(ErrorCodes.TRANSACTION_ERROR).toBe('TRANSACTION_ERROR');

    // Parquet
    expect(ErrorCodes.PARQUET_ERROR).toBe('PARQUET_ERROR');

    // Branch
    expect(ErrorCodes.BRANCH_ERROR).toBe('BRANCH_ERROR');
  });
});
