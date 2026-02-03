/**
 * Comprehensive tests for the configuration validation framework
 *
 * Tests all validation functions for MongoLakeConfig validation.
 */

import { describe, it, expect } from 'vitest';
import {
  validate,
  validateConfig,
  validateAndMerge,
  getDefaults,
  mergeWithDefaults,
  ConfigValidationError,
  isValidParquetType,
  VALID_PARQUET_TYPES,
  type ValidationResult,
} from '../../../src/config/validator.js';

// ============================================================================
// ConfigValidationError Tests
// ============================================================================

describe('ConfigValidationError', () => {
  it('should be an instance of Error', () => {
    const error = new ConfigValidationError('test message', 'database');
    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(ConfigValidationError);
  });

  it('should have correct name property', () => {
    const error = new ConfigValidationError('test message', 'database');
    expect(error.name).toBe('ConfigValidationError');
  });

  it('should include field information', () => {
    const error = new ConfigValidationError('Invalid value', 'database');
    expect(error.field).toBe('database');
  });

  it('should include additional details', () => {
    const error = new ConfigValidationError('Invalid value', 'database', { value: 'bad' });
    expect(error.details).toEqual(expect.objectContaining({ value: 'bad' }));
  });
});

// ============================================================================
// getDefaults Tests
// ============================================================================

describe('getDefaults', () => {
  it('should return default database name', () => {
    const defaults = getDefaults();
    expect(defaults.database).toBe('default');
  });

  it('should return default branch name', () => {
    const defaults = getDefaults();
    expect(defaults.branch).toBe('main');
  });

  it('should return iceberg disabled by default', () => {
    const defaults = getDefaults();
    expect(defaults.iceberg).toBe(false);
  });
});

// ============================================================================
// mergeWithDefaults Tests
// ============================================================================

describe('mergeWithDefaults', () => {
  it('should return defaults for empty config', () => {
    const result = mergeWithDefaults({});
    expect(result.database).toBe('default');
  });

  it('should return defaults for undefined config', () => {
    const result = mergeWithDefaults(undefined);
    expect(result.database).toBe('default');
  });

  it('should preserve user-provided values', () => {
    const result = mergeWithDefaults({ database: 'mydb', local: '.mongolake' });
    expect(result.database).toBe('mydb');
    expect(result.local).toBe('.mongolake');
  });

  it('should not override user-provided database with default', () => {
    const result = mergeWithDefaults({ database: 'custom' });
    expect(result.database).toBe('custom');
  });
});

// ============================================================================
// validate (throwing) Tests
// ============================================================================

describe('validate', () => {
  describe('valid configurations', () => {
    it('should accept empty config', () => {
      expect(() => validate({})).not.toThrow();
    });

    it('should accept null config', () => {
      expect(() => validate(null)).not.toThrow();
    });

    it('should accept undefined config', () => {
      expect(() => validate(undefined)).not.toThrow();
    });

    it('should accept minimal local storage config', () => {
      expect(() => validate({ local: '.mongolake' })).not.toThrow();
    });

    it('should accept config with database name', () => {
      expect(() => validate({ database: 'mydb' })).not.toThrow();
    });

    it('should accept config with branch', () => {
      expect(() => validate({ branch: 'feature-branch' })).not.toThrow();
    });

    it('should accept config with iceberg boolean', () => {
      expect(() => validate({ iceberg: true })).not.toThrow();
      expect(() => validate({ iceberg: false })).not.toThrow();
    });

    it('should accept config with iceberg object', () => {
      expect(() => validate({ iceberg: { token: 'my-token' } })).not.toThrow();
      expect(() => validate({ iceberg: { token: 'my-token', catalog: 'my-catalog' } })).not.toThrow();
    });
  });

  describe('invalid configurations', () => {
    it('should reject non-object config', () => {
      expect(() => validate('invalid')).toThrow(ConfigValidationError);
      expect(() => validate(123)).toThrow(ConfigValidationError);
      expect(() => validate(['array'])).toThrow(ConfigValidationError);
    });

    it('should throw with field information', () => {
      try {
        validate({ database: 123 });
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(ConfigValidationError);
        expect((error as ConfigValidationError).field).toBe('database');
      }
    });
  });
});

// ============================================================================
// validateConfig (non-throwing) Tests
// ============================================================================

describe('validateConfig', () => {
  describe('basic validation', () => {
    it('should return valid for empty config', () => {
      const result = validateConfig({});
      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should return valid for null/undefined', () => {
      expect(validateConfig(null).valid).toBe(true);
      expect(validateConfig(undefined).valid).toBe(true);
    });

    it('should return invalid for non-object', () => {
      const result = validateConfig('string');
      expect(result.valid).toBe(false);
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0]?.message).toContain('must be an object');
    });
  });

  describe('storage validation', () => {
    describe('local storage', () => {
      it('should accept valid local path', () => {
        const result = validateConfig({ local: '.mongolake' });
        expect(result.valid).toBe(true);
      });

      it('should reject non-string local path', () => {
        const result = validateConfig({ local: 123 });
        expect(result.valid).toBe(false);
        expect(result.errors[0]?.field).toBe('local');
      });

      it('should reject empty local path', () => {
        const result = validateConfig({ local: '' });
        expect(result.valid).toBe(false);
        expect(result.errors[0]?.field).toBe('local');
      });

      it('should reject whitespace-only local path', () => {
        const result = validateConfig({ local: '   ' });
        expect(result.valid).toBe(false);
      });
    });

    describe('R2 bucket', () => {
      it('should accept object bucket', () => {
        const result = validateConfig({ bucket: { get: () => {} } });
        expect(result.valid).toBe(true);
      });

      it('should reject non-object bucket', () => {
        const result = validateConfig({ bucket: 'bucket-name' });
        expect(result.valid).toBe(false);
        expect(result.errors[0]?.field).toBe('bucket');
      });
    });

    describe('S3 configuration', () => {
      it('should accept complete S3 config', () => {
        const result = validateConfig({
          endpoint: 'https://s3.amazonaws.com',
          accessKeyId: 'AKIAIOSFODNN7EXAMPLE',
          secretAccessKey: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
          bucketName: 'my-bucket',
        });
        expect(result.valid).toBe(true);
      });

      it('should warn on incomplete S3 config', () => {
        const result = validateConfig({
          endpoint: 'https://s3.amazonaws.com',
          // Missing other fields
        });
        expect(result.warnings).toHaveLength(1);
        expect(result.warnings[0]?.message).toContain('Incomplete S3 configuration');
      });

      it('should reject invalid endpoint URL', () => {
        const result = validateConfig({
          endpoint: 'not-a-url',
          accessKeyId: 'key',
          secretAccessKey: 'secret',
          bucketName: 'bucket',
        });
        expect(result.valid).toBe(false);
        expect(result.errors.some(e => e.field === 'endpoint')).toBe(true);
      });

      it('should reject non-string S3 fields', () => {
        const result = validateConfig({
          endpoint: 123,
          accessKeyId: 456,
          secretAccessKey: 789,
          bucketName: 0,
        });
        expect(result.valid).toBe(false);
        expect(result.errors.length).toBeGreaterThan(0);
      });

      it('should reject empty bucket name', () => {
        const result = validateConfig({
          endpoint: 'https://s3.amazonaws.com',
          accessKeyId: 'key',
          secretAccessKey: 'secret',
          bucketName: '',
        });
        expect(result.valid).toBe(false);
      });
    });

    describe('multiple storage backends', () => {
      it('should warn when multiple backends configured', () => {
        const result = validateConfig({
          local: '.mongolake',
          endpoint: 'https://s3.amazonaws.com',
          accessKeyId: 'key',
          secretAccessKey: 'secret',
          bucketName: 'bucket',
        });
        expect(result.warnings.some(w => w.message.includes('Multiple storage backends'))).toBe(true);
      });
    });
  });

  describe('database validation', () => {
    it('should accept valid database names', () => {
      expect(validateConfig({ database: 'mydb' }).valid).toBe(true);
      expect(validateConfig({ database: 'my-database' }).valid).toBe(true);
      expect(validateConfig({ database: 'my_database' }).valid).toBe(true);
      expect(validateConfig({ database: 'db123' }).valid).toBe(true);
    });

    it('should reject non-string database name', () => {
      const result = validateConfig({ database: 123 });
      expect(result.valid).toBe(false);
      expect(result.errors[0]?.field).toBe('database');
    });

    it('should reject empty database name', () => {
      const result = validateConfig({ database: '' });
      expect(result.valid).toBe(false);
    });

    it('should reject database name with null bytes', () => {
      const result = validateConfig({ database: 'db\0name' });
      expect(result.valid).toBe(false);
      expect(result.errors[0]?.message).toContain('null bytes');
    });

    it('should reject database name with path traversal', () => {
      expect(validateConfig({ database: '../etc' }).valid).toBe(false);
      expect(validateConfig({ database: 'db/name' }).valid).toBe(false);
      expect(validateConfig({ database: 'db\\name' }).valid).toBe(false);
    });

    it('should reject database name starting with underscore or hyphen', () => {
      expect(validateConfig({ database: '_hidden' }).valid).toBe(false);
      expect(validateConfig({ database: '-invalid' }).valid).toBe(false);
    });

    it('should reject database name exceeding max length', () => {
      const result = validateConfig({ database: 'a'.repeat(121) });
      expect(result.valid).toBe(false);
      expect(result.errors[0]?.message).toContain('maximum length');
    });
  });

  describe('iceberg validation', () => {
    it('should accept boolean iceberg', () => {
      expect(validateConfig({ iceberg: true }).valid).toBe(true);
      expect(validateConfig({ iceberg: false }).valid).toBe(true);
    });

    it('should accept object iceberg with token', () => {
      const result = validateConfig({ iceberg: { token: 'my-token' } });
      expect(result.valid).toBe(true);
    });

    it('should accept object iceberg with token and catalog', () => {
      const result = validateConfig({ iceberg: { token: 'my-token', catalog: 'my-catalog' } });
      expect(result.valid).toBe(true);
    });

    it('should reject iceberg object without token', () => {
      const result = validateConfig({ iceberg: { catalog: 'my-catalog' } });
      expect(result.valid).toBe(false);
      expect(result.errors[0]?.field).toBe('iceberg.token');
    });

    it('should reject non-string token', () => {
      const result = validateConfig({ iceberg: { token: 123 } });
      expect(result.valid).toBe(false);
    });

    it('should reject empty token', () => {
      const result = validateConfig({ iceberg: { token: '' } });
      expect(result.valid).toBe(false);
    });

    it('should reject non-string catalog', () => {
      const result = validateConfig({ iceberg: { token: 'token', catalog: 123 } });
      expect(result.valid).toBe(false);
    });

    it('should reject invalid iceberg type', () => {
      const result = validateConfig({ iceberg: 'invalid' });
      expect(result.valid).toBe(false);
    });
  });

  describe('schema validation', () => {
    describe('basic schema structure', () => {
      it('should accept valid schema', () => {
        const result = validateConfig({
          schema: {
            users: {
              columns: { name: 'string', age: 'int32' },
            },
          },
        });
        expect(result.valid).toBe(true);
      });

      it('should reject non-object schema', () => {
        const result = validateConfig({ schema: 'invalid' });
        expect(result.valid).toBe(false);
      });

      it('should reject non-object collection schema', () => {
        const result = validateConfig({ schema: { users: 'invalid' } });
        expect(result.valid).toBe(false);
      });
    });

    describe('column definitions', () => {
      it('should accept all valid Parquet types', () => {
        const columns: Record<string, string> = {};
        VALID_PARQUET_TYPES.forEach((type, i) => {
          columns[`col${i}`] = type;
        });
        const result = validateConfig({
          schema: { test: { columns } },
        });
        expect(result.valid).toBe(true);
      });

      it('should reject invalid Parquet type', () => {
        const result = validateConfig({
          schema: {
            test: { columns: { field: 'invalid_type' } },
          },
        });
        expect(result.valid).toBe(false);
        expect(result.errors[0]?.message).toContain('Invalid Parquet type');
      });

      it('should accept array column definitions', () => {
        const result = validateConfig({
          schema: {
            test: { columns: { tags: ['string'] } },
          },
        });
        expect(result.valid).toBe(true);
      });

      it('should reject array with wrong length', () => {
        const result = validateConfig({
          schema: {
            test: { columns: { tags: ['string', 'int32'] } },
          },
        });
        expect(result.valid).toBe(false);
        expect(result.errors[0]?.message).toContain('exactly one element');
      });

      it('should accept nested struct definitions', () => {
        const result = validateConfig({
          schema: {
            test: {
              columns: {
                address: {
                  city: 'string',
                  zip: 'string',
                  coords: {
                    lat: 'double',
                    lon: 'double',
                  },
                },
              },
            },
          },
        });
        expect(result.valid).toBe(true);
      });

      it('should reject invalid nested type', () => {
        const result = validateConfig({
          schema: {
            test: {
              columns: {
                address: {
                  city: 'not_a_type',
                },
              },
            },
          },
        });
        expect(result.valid).toBe(false);
      });

      it('should reject non-object columns', () => {
        const result = validateConfig({
          schema: { test: { columns: 'invalid' } },
        });
        expect(result.valid).toBe(false);
      });
    });

    describe('autoPromote configuration', () => {
      it('should accept valid autoPromote', () => {
        const result = validateConfig({
          schema: {
            test: { autoPromote: { threshold: 0.8 } },
          },
        });
        expect(result.valid).toBe(true);
      });

      it('should reject missing threshold', () => {
        const result = validateConfig({
          schema: {
            test: { autoPromote: {} },
          },
        });
        expect(result.valid).toBe(false);
        expect(result.errors[0]?.message).toContain('threshold is required');
      });

      it('should reject non-numeric threshold', () => {
        const result = validateConfig({
          schema: {
            test: { autoPromote: { threshold: 'high' } },
          },
        });
        expect(result.valid).toBe(false);
      });

      it('should reject threshold out of range', () => {
        expect(
          validateConfig({
            schema: { test: { autoPromote: { threshold: -0.1 } } },
          }).valid
        ).toBe(false);
        expect(
          validateConfig({
            schema: { test: { autoPromote: { threshold: 1.1 } } },
          }).valid
        ).toBe(false);
      });

      it('should accept boundary threshold values', () => {
        expect(
          validateConfig({
            schema: { test: { autoPromote: { threshold: 0 } } },
          }).valid
        ).toBe(true);
        expect(
          validateConfig({
            schema: { test: { autoPromote: { threshold: 1 } } },
          }).valid
        ).toBe(true);
      });
    });

    describe('storeVariant configuration', () => {
      it('should accept boolean storeVariant', () => {
        expect(
          validateConfig({
            schema: { test: { storeVariant: true } },
          }).valid
        ).toBe(true);
        expect(
          validateConfig({
            schema: { test: { storeVariant: false } },
          }).valid
        ).toBe(true);
      });

      it('should reject non-boolean storeVariant', () => {
        const result = validateConfig({
          schema: { test: { storeVariant: 'yes' } },
        });
        expect(result.valid).toBe(false);
      });
    });
  });

  describe('branch validation', () => {
    it('should accept valid branch names', () => {
      expect(validateConfig({ branch: 'main' }).valid).toBe(true);
      expect(validateConfig({ branch: 'feature/new-feature' }).valid).toBe(true);
      expect(validateConfig({ branch: 'release-1.0' }).valid).toBe(true);
    });

    it('should reject non-string branch', () => {
      const result = validateConfig({ branch: 123 });
      expect(result.valid).toBe(false);
    });

    it('should reject empty branch', () => {
      const result = validateConfig({ branch: '' });
      expect(result.valid).toBe(false);
    });

    it('should reject branch with double dots', () => {
      const result = validateConfig({ branch: 'main..feature' });
      expect(result.valid).toBe(false);
    });

    it('should reject branch starting or ending with slash', () => {
      expect(validateConfig({ branch: '/main' }).valid).toBe(false);
      expect(validateConfig({ branch: 'main/' }).valid).toBe(false);
    });

    it('should reject branch with double slashes', () => {
      const result = validateConfig({ branch: 'feature//branch' });
      expect(result.valid).toBe(false);
    });

    it('should reject branch ending with .lock', () => {
      const result = validateConfig({ branch: 'branch.lock' });
      expect(result.valid).toBe(false);
    });

    it('should reject branch with @{', () => {
      const result = validateConfig({ branch: 'branch@{1}' });
      expect(result.valid).toBe(false);
    });

    it('should reject branch exceeding max length', () => {
      const result = validateConfig({ branch: 'a'.repeat(256) });
      expect(result.valid).toBe(false);
    });
  });

  describe('asOf validation', () => {
    it('should accept valid ISO date string', () => {
      const result = validateConfig({ asOf: '2024-01-15T12:00:00Z' });
      expect(result.valid).toBe(true);
    });

    it('should accept Date object', () => {
      const result = validateConfig({ asOf: new Date() });
      expect(result.valid).toBe(true);
    });

    it('should accept timestamp number', () => {
      const result = validateConfig({ asOf: Date.now() });
      expect(result.valid).toBe(true);
    });

    it('should reject invalid date string', () => {
      const result = validateConfig({ asOf: 'not-a-date' });
      expect(result.valid).toBe(false);
      expect(result.errors[0]?.message).toContain('valid ISO 8601');
    });

    it('should reject invalid Date object', () => {
      const result = validateConfig({ asOf: new Date('invalid') });
      expect(result.valid).toBe(false);
    });

    it('should reject negative timestamp', () => {
      const result = validateConfig({ asOf: -1000 });
      expect(result.valid).toBe(false);
    });

    it('should reject Infinity timestamp', () => {
      const result = validateConfig({ asOf: Infinity });
      expect(result.valid).toBe(false);
    });

    it('should reject invalid asOf type', () => {
      const result = validateConfig({ asOf: { date: '2024-01-15' } });
      expect(result.valid).toBe(false);
    });
  });

  describe('connectionString validation (internal)', () => {
    it('should accept valid connectionString', () => {
      const result = validateConfig({
        connectionString: {
          hosts: [{ host: 'localhost', port: 27017 }],
          username: 'user',
          password: 'pass',
          options: {},
        },
      });
      expect(result.valid).toBe(true);
    });

    it('should reject non-object connectionString', () => {
      const result = validateConfig({ connectionString: 'mongodb://localhost' });
      expect(result.valid).toBe(false);
    });

    it('should reject non-array hosts', () => {
      const result = validateConfig({
        connectionString: { hosts: 'localhost' },
      });
      expect(result.valid).toBe(false);
    });

    it('should reject invalid host objects', () => {
      const result = validateConfig({
        connectionString: { hosts: [{ host: 123, port: 'invalid' }] },
      });
      expect(result.valid).toBe(false);
    });

    it('should reject invalid port range', () => {
      expect(
        validateConfig({
          connectionString: { hosts: [{ host: 'localhost', port: 0 }] },
        }).valid
      ).toBe(false);
      expect(
        validateConfig({
          connectionString: { hosts: [{ host: 'localhost', port: 70000 }] },
        }).valid
      ).toBe(false);
    });

    it('should reject non-string username/password', () => {
      expect(
        validateConfig({
          connectionString: { username: 123 },
        }).valid
      ).toBe(false);
      expect(
        validateConfig({
          connectionString: { password: 123 },
        }).valid
      ).toBe(false);
    });

    it('should reject non-object options', () => {
      const result = validateConfig({
        connectionString: { options: 'invalid' },
      });
      expect(result.valid).toBe(false);
    });
  });
});

// ============================================================================
// validateAndMerge Tests
// ============================================================================

describe('validateAndMerge', () => {
  it('should validate and return merged config', () => {
    const result = validateAndMerge({ local: '.mongolake' });
    expect(result.local).toBe('.mongolake');
    expect(result.database).toBe('default');
  });

  it('should throw on invalid config', () => {
    expect(() => validateAndMerge({ database: 123 })).toThrow(ConfigValidationError);
  });

  it('should preserve user values over defaults', () => {
    const result = validateAndMerge({ database: 'mydb', local: './data' });
    expect(result.database).toBe('mydb');
    expect(result.local).toBe('./data');
  });
});

// ============================================================================
// isValidParquetType Tests
// ============================================================================

describe('isValidParquetType', () => {
  it('should return true for valid types', () => {
    expect(isValidParquetType('string')).toBe(true);
    expect(isValidParquetType('int32')).toBe(true);
    expect(isValidParquetType('int64')).toBe(true);
    expect(isValidParquetType('float')).toBe(true);
    expect(isValidParquetType('double')).toBe(true);
    expect(isValidParquetType('boolean')).toBe(true);
    expect(isValidParquetType('timestamp')).toBe(true);
    expect(isValidParquetType('date')).toBe(true);
    expect(isValidParquetType('binary')).toBe(true);
    expect(isValidParquetType('variant')).toBe(true);
  });

  it('should return false for invalid types', () => {
    expect(isValidParquetType('invalid')).toBe(false);
    expect(isValidParquetType('integer')).toBe(false);
    expect(isValidParquetType('varchar')).toBe(false);
    expect(isValidParquetType(123)).toBe(false);
    expect(isValidParquetType(null)).toBe(false);
    expect(isValidParquetType(undefined)).toBe(false);
  });
});

// ============================================================================
// VALID_PARQUET_TYPES Tests
// ============================================================================

describe('VALID_PARQUET_TYPES', () => {
  it('should contain all expected types', () => {
    expect(VALID_PARQUET_TYPES).toContain('string');
    expect(VALID_PARQUET_TYPES).toContain('int32');
    expect(VALID_PARQUET_TYPES).toContain('int64');
    expect(VALID_PARQUET_TYPES).toContain('float');
    expect(VALID_PARQUET_TYPES).toContain('double');
    expect(VALID_PARQUET_TYPES).toContain('boolean');
    expect(VALID_PARQUET_TYPES).toContain('timestamp');
    expect(VALID_PARQUET_TYPES).toContain('date');
    expect(VALID_PARQUET_TYPES).toContain('binary');
    expect(VALID_PARQUET_TYPES).toContain('variant');
  });

  it('should have exactly 10 types', () => {
    expect(VALID_PARQUET_TYPES).toHaveLength(10);
  });
});

// ============================================================================
// Complex Configuration Tests
// ============================================================================

describe('Complex Configuration Scenarios', () => {
  it('should validate complete production config', () => {
    const result = validateConfig({
      endpoint: 'https://s3.us-east-1.amazonaws.com',
      accessKeyId: 'AKIAIOSFODNN7EXAMPLE',
      secretAccessKey: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
      bucketName: 'my-mongolake-bucket',
      database: 'production',
      iceberg: {
        token: 'iceberg-token-123',
        catalog: 'my-catalog',
      },
      schema: {
        users: {
          columns: {
            name: 'string',
            email: 'string',
            age: 'int32',
            createdAt: 'timestamp',
            tags: ['string'],
            address: {
              street: 'string',
              city: 'string',
              country: 'string',
              zip: 'string',
            },
          },
          autoPromote: { threshold: 0.9 },
          storeVariant: true,
        },
        orders: {
          columns: {
            userId: 'string',
            total: 'double',
            items: ['variant'],
          },
        },
      },
      branch: 'main',
      asOf: '2024-01-15T00:00:00Z',
    });

    expect(result.valid).toBe(true);
    expect(result.errors).toHaveLength(0);
  });

  it('should collect multiple errors', () => {
    const result = validateConfig({
      database: '',
      local: '',
      iceberg: { token: '' },
      branch: '',
      schema: {
        test: {
          columns: { bad: 'invalid_type' },
          autoPromote: { threshold: 2 },
          storeVariant: 'yes',
        },
      },
    });

    expect(result.valid).toBe(false);
    expect(result.errors.length).toBeGreaterThan(3);
  });

  it('should distinguish errors from warnings', () => {
    const result = validateConfig({
      local: '.mongolake',
      endpoint: 'https://s3.amazonaws.com',
      // Incomplete S3 config - should warn
    });

    expect(result.valid).toBe(true); // Only warnings, no errors
    expect(result.warnings.length).toBeGreaterThan(0);
    expect(result.errors).toHaveLength(0);
  });
});

// ============================================================================
// Edge Cases Tests
// ============================================================================

describe('Edge Cases', () => {
  it('should handle deeply nested schema', () => {
    const result = validateConfig({
      schema: {
        test: {
          columns: {
            level1: {
              level2: {
                level3: {
                  level4: {
                    value: 'string',
                  },
                },
              },
            },
          },
        },
      },
    });
    expect(result.valid).toBe(true);
  });

  it('should handle empty schema collections', () => {
    const result = validateConfig({
      schema: {
        empty: {},
      },
    });
    expect(result.valid).toBe(true);
  });

  it('should handle config with only optional fields', () => {
    const result = validateConfig({
      iceberg: false,
      branch: 'develop',
    });
    expect(result.valid).toBe(true);
  });

  it('should handle asOf with Date.now()', () => {
    const result = validateConfig({
      asOf: Date.now(),
    });
    expect(result.valid).toBe(true);
  });

  it('should handle zero as asOf timestamp', () => {
    const result = validateConfig({
      asOf: 0,
    });
    expect(result.valid).toBe(true);
  });
});
