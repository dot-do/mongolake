/**
 * Tests for Standardized Error Format
 *
 * These tests verify that all MongoLake error types follow a consistent format:
 * - code property (string)
 * - message property (string)
 * - name property matching class name
 * - optional details object
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
} from '../../../src/errors/index.js';

/**
 * Interface defining the standardized error format
 */
interface StandardizedErrorFormat {
  code: string;
  message: string;
  name: string;
  details?: Record<string, unknown>;
  timestamp: number;
}

/**
 * Helper to verify an error conforms to the standard format
 */
function assertStandardFormat(error: MongoLakeError): void {
  // Must have code property as string
  expect(typeof error.code).toBe('string');
  expect(error.code.length).toBeGreaterThan(0);

  // Must have message property as string
  expect(typeof error.message).toBe('string');

  // Must have name property matching class name
  expect(typeof error.name).toBe('string');
  expect(error.name.length).toBeGreaterThan(0);

  // Must have timestamp as number
  expect(typeof error.timestamp).toBe('number');
  expect(error.timestamp).toBeGreaterThan(0);

  // Details must be undefined or an object
  if (error.details !== undefined) {
    expect(typeof error.details).toBe('object');
    expect(error.details).not.toBeNull();
  }
}

/**
 * Helper to verify JSON serialization format
 */
function assertJSONFormat(json: Record<string, unknown>): void {
  expect(json).toHaveProperty('code');
  expect(json).toHaveProperty('message');
  expect(json).toHaveProperty('name');
  expect(json).toHaveProperty('timestamp');
  expect(typeof json.code).toBe('string');
  expect(typeof json.message).toBe('string');
  expect(typeof json.name).toBe('string');
  expect(typeof json.timestamp).toBe('number');
}

describe('Standardized Error Format', () => {
  describe('MongoLakeError base class format', () => {
    it('should have code, message, name properties with correct types', () => {
      const error = new MongoLakeError('Base error message', ErrorCodes.INTERNAL);
      assertStandardFormat(error);
      expect(error.name).toBe('MongoLakeError');
      expect(error.code).toBe(ErrorCodes.INTERNAL);
    });

    it('should have name property matching class name', () => {
      const error = new MongoLakeError('Test');
      expect(error.name).toBe('MongoLakeError');
      expect(error.name).toBe(error.constructor.name);
    });

    it('should support optional details object', () => {
      const errorWithDetails = new MongoLakeError('With details', ErrorCodes.INTERNAL, {
        key: 'value',
        nested: { data: 123 },
      });
      expect(errorWithDetails.details).toEqual({
        key: 'value',
        nested: { data: 123 },
      });

      const errorWithoutDetails = new MongoLakeError('Without details');
      expect(errorWithoutDetails.details).toBeUndefined();
    });

    it('should serialize to JSON with all standard properties', () => {
      const error = new MongoLakeError('Test', ErrorCodes.INTERNAL, { foo: 'bar' });
      const json = error.toJSON();
      assertJSONFormat(json);
      expect(json.details).toEqual({ foo: 'bar' });
    });
  });

  describe('ValidationError format', () => {
    it('should have code, message, name properties with correct types', () => {
      const error = new ValidationError('Validation failed');
      assertStandardFormat(error);
      expect(error.name).toBe('ValidationError');
    });

    it('should have name property matching class name', () => {
      const error = new ValidationError('Invalid input');
      expect(error.name).toBe('ValidationError');
    });

    it('should have default validation code when not specified', () => {
      const error = new ValidationError('Test');
      expect(error.code).toBe(ErrorCodes.VALIDATION_FAILED);
    });

    it('should support specific validation codes', () => {
      const invalidName = new ValidationError('Invalid name', ErrorCodes.INVALID_NAME);
      expect(invalidName.code).toBe(ErrorCodes.INVALID_NAME);

      const invalidFilter = new ValidationError('Invalid filter', ErrorCodes.INVALID_FILTER);
      expect(invalidFilter.code).toBe(ErrorCodes.INVALID_FILTER);
    });

    it('should include validationType and invalidValue in JSON', () => {
      const error = new ValidationError('Test', ErrorCodes.VALIDATION_FAILED, {
        validationType: 'field_name',
        invalidValue: 'bad-value',
      });
      const json = error.toJSON();
      assertJSONFormat(json);
      expect(json.validationType).toBe('field_name');
      expect(json.invalidValue).toBe('bad-value');
    });
  });

  describe('StorageError format', () => {
    it('should have code, message, name properties with correct types', () => {
      const error = new StorageError('Storage operation failed');
      assertStandardFormat(error);
      expect(error.name).toBe('StorageError');
    });

    it('should have name property matching class name', () => {
      const error = new StorageError('Test');
      expect(error.name).toBe('StorageError');
    });

    it('should have default storage code when not specified', () => {
      const error = new StorageError('Test');
      expect(error.code).toBe(ErrorCodes.STORAGE_ERROR);
    });

    it('should include key and cause in JSON', () => {
      const cause = new Error('Underlying error');
      const error = new StorageError('Write failed', ErrorCodes.STORAGE_WRITE_FAILED, {
        key: 'path/to/file.parquet',
        cause,
      });
      const json = error.toJSON();
      assertJSONFormat(json);
      expect(json.key).toBe('path/to/file.parquet');
      expect(json.cause).toBe('Underlying error');
    });
  });

  describe('InvalidStorageKeyError format', () => {
    it('should have code, message, name properties with correct types', () => {
      const error = new InvalidStorageKeyError('Invalid key: ../path');
      assertStandardFormat(error);
      expect(error.name).toBe('InvalidStorageKeyError');
    });

    it('should have name property matching class name', () => {
      const error = new InvalidStorageKeyError('Test');
      expect(error.name).toBe('InvalidStorageKeyError');
    });

    it('should always have STORAGE_INVALID_KEY code', () => {
      const error = new InvalidStorageKeyError('Test', 'bad/key');
      expect(error.code).toBe(ErrorCodes.STORAGE_INVALID_KEY);
    });
  });

  describe('AuthenticationError format', () => {
    it('should have code, message, name properties with correct types', () => {
      const error = new AuthenticationError('Authentication failed');
      assertStandardFormat(error);
      expect(error.name).toBe('AuthenticationError');
    });

    it('should have name property matching class name', () => {
      const error = new AuthenticationError('Test');
      expect(error.name).toBe('AuthenticationError');
    });

    it('should have default auth code when not specified', () => {
      const error = new AuthenticationError('Test');
      expect(error.code).toBe(ErrorCodes.AUTH_ERROR);
    });

    it('should include statusCode in JSON', () => {
      const error = new AuthenticationError('Forbidden', ErrorCodes.AUTH_INSUFFICIENT_PERMISSIONS);
      const json = error.toJSON();
      assertJSONFormat(json);
      expect(json.statusCode).toBe(403);
    });
  });

  describe('QueryError format', () => {
    it('should have code, message, name properties with correct types', () => {
      const error = new QueryError('Query execution failed');
      assertStandardFormat(error);
      expect(error.name).toBe('QueryError');
    });

    it('should have name property matching class name', () => {
      const error = new QueryError('Test');
      expect(error.name).toBe('QueryError');
    });

    it('should have default query code when not specified', () => {
      const error = new QueryError('Test');
      expect(error.code).toBe(ErrorCodes.QUERY_ERROR);
    });

    it('should support various query error codes', () => {
      const syntaxError = new QueryError('Syntax error', ErrorCodes.QUERY_SYNTAX_ERROR);
      expect(syntaxError.code).toBe(ErrorCodes.QUERY_SYNTAX_ERROR);

      const timeoutError = new QueryError('Query timeout', ErrorCodes.QUERY_TIMEOUT);
      expect(timeoutError.code).toBe(ErrorCodes.QUERY_TIMEOUT);
    });
  });

  describe('RPCError format', () => {
    it('should have code, message, name properties with correct types', () => {
      const error = new RPCError('RPC call failed');
      assertStandardFormat(error);
      expect(error.name).toBe('RPCError');
    });

    it('should have name property matching class name', () => {
      const error = new RPCError('Test');
      expect(error.name).toBe('RPCError');
    });

    it('should have default RPC code when not specified', () => {
      const error = new RPCError('Test');
      expect(error.code).toBe(ErrorCodes.RPC_ERROR);
    });

    it('should optionally include remoteStack', () => {
      const error = new RPCError('Remote failure', ErrorCodes.RPC_ERROR, {
        remoteStack: 'Error at remote:1',
      });
      expect(error.remoteStack).toBe('Error at remote:1');
    });
  });

  describe('TransientError format', () => {
    it('should have code, message, name properties with correct types', () => {
      const error = new TransientError('Transient network issue');
      assertStandardFormat(error);
      expect(error.name).toBe('TransientError');
    });

    it('should have name property matching class name', () => {
      const error = new TransientError('Test');
      expect(error.name).toBe('TransientError');
    });

    it('should always have RPC_TRANSIENT code', () => {
      const error = new TransientError('Test');
      expect(error.code).toBe(ErrorCodes.RPC_TRANSIENT);
    });

    it('should optionally include originalError and retryCount', () => {
      const original = new Error('Connection reset');
      const error = new TransientError('Retries exhausted', {
        originalError: original,
        retryCount: 3,
      });
      expect(error.originalError).toBe(original);
      expect(error.retryCount).toBe(3);
    });
  });

  describe('ShardUnavailableError format', () => {
    it('should have code, message, name properties with correct types', () => {
      const error = new ShardUnavailableError('Shard unavailable', 5);
      assertStandardFormat(error);
      expect(error.name).toBe('ShardUnavailableError');
    });

    it('should have name property matching class name', () => {
      const error = new ShardUnavailableError('Test', 1);
      expect(error.name).toBe('ShardUnavailableError');
    });

    it('should always have RPC_SHARD_UNAVAILABLE code', () => {
      const error = new ShardUnavailableError('Test', 1);
      expect(error.code).toBe(ErrorCodes.RPC_SHARD_UNAVAILABLE);
    });

    it('should include shardId', () => {
      const error = new ShardUnavailableError('Shard down', 42);
      expect(error.shardId).toBe(42);
    });
  });

  describe('SchemaError format', () => {
    it('should have code, message, name properties with correct types', () => {
      const error = new SchemaError('Schema validation failed');
      assertStandardFormat(error);
      expect(error.name).toBe('SchemaError');
    });

    it('should have name property matching class name', () => {
      const error = new SchemaError('Test');
      expect(error.name).toBe('SchemaError');
    });

    it('should have default schema code when not specified', () => {
      const error = new SchemaError('Test');
      expect(error.code).toBe(ErrorCodes.SCHEMA_ERROR);
    });
  });

  describe('TransactionError format', () => {
    it('should have code, message, name properties with correct types', () => {
      const error = new TransactionError('Transaction aborted');
      assertStandardFormat(error);
      expect(error.name).toBe('TransactionError');
    });

    it('should have name property matching class name', () => {
      const error = new TransactionError('Test');
      expect(error.name).toBe('TransactionError');
    });

    it('should have default transaction code when not specified', () => {
      const error = new TransactionError('Test');
      expect(error.code).toBe(ErrorCodes.TRANSACTION_ERROR);
    });

    it('should support various transaction error codes', () => {
      const conflict = new TransactionError('Conflict', ErrorCodes.TRANSACTION_CONFLICT);
      expect(conflict.code).toBe(ErrorCodes.TRANSACTION_CONFLICT);

      const deadlock = new TransactionError('Deadlock', ErrorCodes.DEADLOCK);
      expect(deadlock.code).toBe(ErrorCodes.DEADLOCK);
    });
  });

  describe('ParquetError format', () => {
    it('should have code, message, name properties with correct types', () => {
      const error = new ParquetError('Invalid Parquet file');
      assertStandardFormat(error);
      expect(error.name).toBe('ParquetError');
    });

    it('should have name property matching class name', () => {
      const error = new ParquetError('Test');
      expect(error.name).toBe('ParquetError');
    });

    it('should have default parquet code when not specified', () => {
      const error = new ParquetError('Test');
      expect(error.code).toBe(ErrorCodes.PARQUET_ERROR);
    });

    it('should support various parquet error codes', () => {
      const invalidMagic = new ParquetError('Invalid magic', ErrorCodes.PARQUET_INVALID_MAGIC);
      expect(invalidMagic.code).toBe(ErrorCodes.PARQUET_INVALID_MAGIC);

      const corrupted = new ParquetError('Corrupted', ErrorCodes.PARQUET_CORRUPTED);
      expect(corrupted.code).toBe(ErrorCodes.PARQUET_CORRUPTED);
    });
  });

  describe('BranchError format', () => {
    it('should have code, message, name properties with correct types', () => {
      const error = new BranchError('Branch operation failed');
      assertStandardFormat(error);
      expect(error.name).toBe('BranchError');
    });

    it('should have name property matching class name', () => {
      const error = new BranchError('Test');
      expect(error.name).toBe('BranchError');
    });

    it('should have default branch code when not specified', () => {
      const error = new BranchError('Test');
      expect(error.code).toBe(ErrorCodes.BRANCH_ERROR);
    });

    it('should support various branch error codes', () => {
      const notFound = new BranchError('Not found', ErrorCodes.BRANCH_NOT_FOUND);
      expect(notFound.code).toBe(ErrorCodes.BRANCH_NOT_FOUND);

      const exists = new BranchError('Already exists', ErrorCodes.BRANCH_EXISTS);
      expect(exists.code).toBe(ErrorCodes.BRANCH_EXISTS);
    });
  });

  describe('All errors extend Error and MongoLakeError', () => {
    const errorClasses = [
      { name: 'ValidationError', create: () => new ValidationError('test') },
      { name: 'StorageError', create: () => new StorageError('test') },
      { name: 'InvalidStorageKeyError', create: () => new InvalidStorageKeyError('test') },
      { name: 'AuthenticationError', create: () => new AuthenticationError('test') },
      { name: 'QueryError', create: () => new QueryError('test') },
      { name: 'RPCError', create: () => new RPCError('test') },
      { name: 'TransientError', create: () => new TransientError('test') },
      { name: 'ShardUnavailableError', create: () => new ShardUnavailableError('test', 1) },
      { name: 'SchemaError', create: () => new SchemaError('test') },
      { name: 'TransactionError', create: () => new TransactionError('test') },
      { name: 'ParquetError', create: () => new ParquetError('test') },
      { name: 'BranchError', create: () => new BranchError('test') },
    ];

    it.each(errorClasses)('$name should extend both Error and MongoLakeError', ({ create }) => {
      const error = create();
      expect(error instanceof Error).toBe(true);
      expect(error instanceof MongoLakeError).toBe(true);
    });

    it.each(errorClasses)('$name should have stack trace', ({ create }) => {
      const error = create();
      expect(error.stack).toBeDefined();
      expect(typeof error.stack).toBe('string');
    });
  });

  describe('Error code consistency', () => {
    it('all error codes should be non-empty strings', () => {
      Object.values(ErrorCodes).forEach((code) => {
        expect(typeof code).toBe('string');
        expect(code.length).toBeGreaterThan(0);
      });
    });

    it('error codes should follow naming convention', () => {
      Object.values(ErrorCodes).forEach((code) => {
        // Codes should be uppercase with underscores (SCREAMING_SNAKE_CASE)
        expect(code).toMatch(/^[A-Z][A-Z0-9_]*$/);
      });
    });

    it('error codes should be unique', () => {
      const codes = Object.values(ErrorCodes);
      const uniqueCodes = new Set(codes);
      expect(uniqueCodes.size).toBe(codes.length);
    });
  });

  describe('Error toResponse() format', () => {
    const errorClasses = [
      { name: 'MongoLakeError', create: () => new MongoLakeError('test') },
      { name: 'ValidationError', create: () => new ValidationError('test') },
      { name: 'StorageError', create: () => new StorageError('test') },
      { name: 'AuthenticationError', create: () => new AuthenticationError('test') },
      { name: 'QueryError', create: () => new QueryError('test') },
      { name: 'RPCError', create: () => new RPCError('test') },
      { name: 'TransientError', create: () => new TransientError('test') },
      { name: 'ShardUnavailableError', create: () => new ShardUnavailableError('test', 1) },
      { name: 'SchemaError', create: () => new SchemaError('test') },
      { name: 'TransactionError', create: () => new TransactionError('test') },
      { name: 'ParquetError', create: () => new ParquetError('test') },
      { name: 'BranchError', create: () => new BranchError('test') },
    ];

    it.each(errorClasses)('$name.toResponse() should wrap error in error object', ({ create }) => {
      const error = create();
      const response = error.toResponse();
      expect(response).toHaveProperty('error');
      expect(response.error).toHaveProperty('code');
      expect(response.error).toHaveProperty('message');
      expect(response.error).toHaveProperty('name');
      expect(response.error).toHaveProperty('timestamp');
    });
  });

  describe('Error message format', () => {
    it('should preserve original message exactly', () => {
      const message = 'This is a detailed error message with special chars: !@#$%';
      const error = new MongoLakeError(message);
      expect(error.message).toBe(message);
    });

    it('should handle empty message', () => {
      const error = new MongoLakeError('');
      expect(error.message).toBe('');
      expect(typeof error.message).toBe('string');
    });

    it('should handle multiline messages', () => {
      const message = 'Line 1\nLine 2\nLine 3';
      const error = new MongoLakeError(message);
      expect(error.message).toBe(message);
    });

    it('should handle unicode characters in message', () => {
      const message = 'Error: Invalid character \u4e2d\u6587 detected';
      const error = new MongoLakeError(message);
      expect(error.message).toBe(message);
    });
  });

  describe('Details object format', () => {
    it('should accept any valid JSON-serializable object', () => {
      const details = {
        string: 'value',
        number: 42,
        boolean: true,
        null: null,
        array: [1, 2, 3],
        nested: { a: { b: { c: 1 } } },
      };
      const error = new MongoLakeError('Test', ErrorCodes.INTERNAL, details);
      expect(error.details).toEqual(details);
    });

    it('should be included in toJSON() output', () => {
      const details = { key: 'value' };
      const error = new MongoLakeError('Test', ErrorCodes.INTERNAL, details);
      const json = error.toJSON();
      expect(json.details).toEqual(details);
    });

    it('should be undefined when not provided', () => {
      const error = new MongoLakeError('Test');
      expect(error.details).toBeUndefined();
      const json = error.toJSON();
      expect(json.details).toBeUndefined();
    });
  });
});
