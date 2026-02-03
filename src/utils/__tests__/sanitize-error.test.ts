/**
 * Sanitize Error Utility Tests
 *
 * These tests verify that credentials and sensitive data are never leaked
 * in error messages, stack traces, or logged objects. This is a critical
 * security requirement to prevent credential exposure.
 *
 * Issue: mongolake-7fi - Credential logging vulnerability
 */

import { describe, it, expect } from 'vitest';
import {
  sanitizeError,
  sanitizeMessage,
  sanitizeConfig,
  createSafeErrorMessage,
  looksLikeCredential,
} from '../sanitize-error.js';

// ============================================================================
// Test Data
// ============================================================================

/** Sample AWS-style credentials that should always be redacted */
const AWS_ACCESS_KEY_ID = 'AKIAIOSFODNN7EXAMPLE';
const AWS_SECRET_ACCESS_KEY = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY';
const AWS_SESSION_TOKEN = 'FwoGZXIvYXdzEBYaDNqT1yVCpxBOXSIx...truncated';

/** Sample OAuth tokens that should always be redacted */
const ACCESS_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c';
const REFRESH_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0eXBlIjoicmVmcmVzaCIsInN1YiI6IjEyMzQ1Njc4OTAifQ.xyz';

/** Sample API keys that should always be redacted */
const STRIPE_API_KEY = 'sk_test_FAKE_KEY_FOR_TESTING_1234567890';
const GENERIC_API_KEY = 'api_key_abcd1234567890efghijklmnopqr';

/** Sample passwords that should always be redacted */
const PASSWORD = 'super_secret_password_123!';

/** Sample connection strings with embedded credentials */
const CONNECTION_STRING = 'mongodb://user:password123@localhost:27017/mydb';

// ============================================================================
// sanitizeError Tests
// ============================================================================

describe('sanitizeError', () => {
  describe('AWS credentials', () => {
    it('should redact accessKeyId from error properties', () => {
      const error = new Error('S3 operation failed') as Error & Record<string, unknown>;
      error.accessKeyId = AWS_ACCESS_KEY_ID;
      error.bucket = 'my-bucket';

      const sanitized = sanitizeError(error);

      expect(sanitized.accessKeyId).toBe('[REDACTED]');
      expect(sanitized.bucket).toBe('my-bucket');
      expect(sanitized.message).toBe('S3 operation failed');
    });

    it('should redact secretAccessKey from error properties', () => {
      const error = new Error('S3 operation failed') as Error & Record<string, unknown>;
      error.secretAccessKey = AWS_SECRET_ACCESS_KEY;

      const sanitized = sanitizeError(error);

      expect(sanitized.secretAccessKey).toBe('[REDACTED]');
    });

    it('should redact AWS access key patterns from error messages', () => {
      const error = new Error(`Failed with key ${AWS_ACCESS_KEY_ID}`);

      const sanitized = sanitizeError(error);

      expect(sanitized.message).not.toContain(AWS_ACCESS_KEY_ID);
      expect(sanitized.message).toContain('[REDACTED');
    });

    it('should redact AWS credentials from nested config objects', () => {
      const error = new Error('S3 operation failed') as Error & Record<string, unknown>;
      error.config = {
        endpoint: 'https://s3.amazonaws.com',
        accessKeyId: AWS_ACCESS_KEY_ID,
        secretAccessKey: AWS_SECRET_ACCESS_KEY,
        bucket: 'my-bucket',
      };

      const sanitized = sanitizeError(error);
      const config = sanitized.config as Record<string, unknown>;

      expect(config.accessKeyId).toBe('[REDACTED]');
      expect(config.secretAccessKey).toBe('[REDACTED]');
      expect(config.endpoint).toBe('https://s3.amazonaws.com');
      expect(config.bucket).toBe('my-bucket');
    });
  });

  describe('OAuth tokens', () => {
    it('should redact accessToken from error properties', () => {
      const error = new Error('Token validation failed') as Error & Record<string, unknown>;
      error.accessToken = ACCESS_TOKEN;

      const sanitized = sanitizeError(error);

      expect(sanitized.accessToken).toBe('[REDACTED]');
    });

    it('should redact refreshToken from error properties', () => {
      const error = new Error('Token refresh failed') as Error & Record<string, unknown>;
      error.refreshToken = REFRESH_TOKEN;

      const sanitized = sanitizeError(error);

      expect(sanitized.refreshToken).toBe('[REDACTED]');
    });

    it('should redact JWT tokens from error messages', () => {
      const error = new Error(`Invalid token: ${ACCESS_TOKEN}`);

      const sanitized = sanitizeError(error);

      expect(sanitized.message).not.toContain('eyJ');
      expect(sanitized.message).toContain('[REDACTED');
    });

    it('should redact Bearer tokens from error messages', () => {
      const error = new Error(`Authorization failed: Bearer ${ACCESS_TOKEN}`);

      const sanitized = sanitizeError(error);

      expect(sanitized.message).not.toContain(ACCESS_TOKEN);
      expect(sanitized.message).toContain('Bearer [REDACTED');
    });
  });

  describe('API keys', () => {
    it('should redact apiKey from error properties', () => {
      const error = new Error('API request failed') as Error & Record<string, unknown>;
      error.apiKey = GENERIC_API_KEY;

      const sanitized = sanitizeError(error);

      expect(sanitized.apiKey).toBe('[REDACTED]');
    });

    it('should redact Stripe-style API keys from error messages', () => {
      const error = new Error(`Invalid API key: ${STRIPE_API_KEY}`);

      const sanitized = sanitizeError(error);

      expect(sanitized.message).not.toContain(STRIPE_API_KEY);
      expect(sanitized.message).toContain('[REDACTED');
    });
  });

  describe('passwords', () => {
    it('should redact password from error properties', () => {
      const error = new Error('Authentication failed') as Error & Record<string, unknown>;
      error.password = PASSWORD;

      const sanitized = sanitizeError(error);

      expect(sanitized.password).toBe('[REDACTED]');
    });

    it('should redact connection strings with passwords', () => {
      const error = new Error(`Cannot connect to ${CONNECTION_STRING}`);

      const sanitized = sanitizeError(error);

      expect(sanitized.message).not.toContain('password123');
      expect(sanitized.message).toContain('[REDACTED');
    });
  });

  describe('stack traces', () => {
    it('should redact credentials from stack traces', () => {
      const error = new Error('Failed');
      error.stack = `Error: Failed\n    at S3Storage.put (s3.ts:100)\n    with key ${AWS_ACCESS_KEY_ID}\n    at async main`;

      const sanitized = sanitizeError(error);

      expect(sanitized.stack).not.toContain(AWS_ACCESS_KEY_ID);
      expect(sanitized.stack).toContain('[REDACTED');
    });

    it('should redact JWT tokens from stack traces', () => {
      const error = new Error('Failed');
      error.stack = `Error: Failed\n    at AuthMiddleware.validate\n    token: ${ACCESS_TOKEN}\n    at async handler`;

      const sanitized = sanitizeError(error);

      expect(sanitized.stack).not.toContain('eyJ');
      expect(sanitized.stack).toContain('[REDACTED');
    });
  });

  describe('error cause chain', () => {
    it('should sanitize nested error causes', () => {
      // Use a credential that can be detected by the pattern matching
      const cause = new Error(`API key invalid: ${AWS_ACCESS_KEY_ID}`) as Error & Record<string, unknown>;
      cause.apiKey = GENERIC_API_KEY;

      const error = new Error('Request failed', { cause }) as Error & Record<string, unknown>;
      error.token = ACCESS_TOKEN;

      const sanitized = sanitizeError(error);

      expect(sanitized.token).toBe('[REDACTED]');
      expect(sanitized.cause).toBeDefined();
      expect(sanitized.cause!.apiKey).toBe('[REDACTED]');
      // AWS access key pattern should be detected and redacted
      expect(sanitized.cause!.message).not.toContain(AWS_ACCESS_KEY_ID);
    });
  });

  describe('string errors', () => {
    it('should sanitize string errors with credentials', () => {
      const error = `S3 failed with key ${AWS_ACCESS_KEY_ID}`;

      const sanitized = sanitizeError(error);

      expect(sanitized.message).not.toContain(AWS_ACCESS_KEY_ID);
      expect(sanitized.message).toContain('[REDACTED');
    });
  });

  describe('object errors', () => {
    it('should sanitize plain objects with credentials', () => {
      const error = {
        message: 'S3 operation failed',
        accessKeyId: AWS_ACCESS_KEY_ID,
        secretAccessKey: AWS_SECRET_ACCESS_KEY,
        bucket: 'my-bucket',
      };

      const sanitized = sanitizeError(error);

      expect(sanitized.accessKeyId).toBe('[REDACTED]');
      expect(sanitized.secretAccessKey).toBe('[REDACTED]');
      expect(sanitized.bucket).toBe('my-bucket');
    });
  });

  describe('circular references', () => {
    it('should handle circular references in objects without crashing', () => {
      // Create a plain object with a circular reference
      const obj: Record<string, unknown> = { message: 'Circular error' };
      obj.self = obj;

      const sanitized = sanitizeError(obj);

      expect(sanitized.message).toBe('Circular error');
      // The self property contains a circular reference marker
      expect(sanitized.self).toBeDefined();
      const self = sanitized.self as Record<string, unknown>;
      expect(self._circular).toBe('Circular reference detected');
    });

    it('should handle errors with circular references', () => {
      // Error objects are handled separately from their properties
      const error = new Error('Circular error') as Error & Record<string, unknown>;
      const data: Record<string, unknown> = { value: 'test' };
      data.circular = data; // Create circular reference in data
      error.data = data;

      const sanitized = sanitizeError(error);

      expect(sanitized.message).toBe('Circular error');
      expect(sanitized.data).toBeDefined();
      const sanitizedData = sanitized.data as Record<string, unknown>;
      expect(sanitizedData.value).toBe('test');
      expect((sanitizedData.circular as Record<string, unknown>)._circular).toBe('Circular reference detected');
    });
  });

  describe('max depth', () => {
    it('should handle deeply nested objects', () => {
      let obj: Record<string, unknown> = { secretKey: 'secret_value' };
      for (let i = 0; i < 20; i++) {
        obj = { nested: obj };
      }
      const error = new Error('Deep error') as Error & Record<string, unknown>;
      error.data = obj;

      // Should not throw
      const sanitized = sanitizeError(error);
      expect(sanitized.message).toBe('Deep error');
    });
  });
});

// ============================================================================
// sanitizeMessage Tests
// ============================================================================

describe('sanitizeMessage', () => {
  it('should redact AWS access keys from messages', () => {
    const message = `S3 operation failed with access key ${AWS_ACCESS_KEY_ID}`;

    const sanitized = sanitizeMessage(message);

    expect(sanitized).not.toContain(AWS_ACCESS_KEY_ID);
    expect(sanitized).toContain('[REDACTED');
  });

  it('should redact JWT tokens from messages', () => {
    const message = `Token validation failed: ${ACCESS_TOKEN}`;

    const sanitized = sanitizeMessage(message);

    expect(sanitized).not.toContain('eyJ');
    expect(sanitized).toContain('[REDACTED');
  });

  it('should redact connection strings with passwords', () => {
    const message = `Failed to connect: ${CONNECTION_STRING}`;

    const sanitized = sanitizeMessage(message);

    expect(sanitized).not.toContain('password123');
    expect(sanitized).toContain('[REDACTED');
  });

  it('should redact long hex strings (potential secrets)', () => {
    const hexSecret = 'a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2';
    const message = `Secret: ${hexSecret}`;

    const sanitized = sanitizeMessage(message);

    expect(sanitized).not.toContain(hexSecret);
    expect(sanitized).toContain('[REDACTED');
  });

  it('should preserve non-sensitive content', () => {
    const message = 'S3 operation failed: bucket=my-bucket, key=path/to/file.txt';

    const sanitized = sanitizeMessage(message);

    expect(sanitized).toBe(message);
  });
});

// ============================================================================
// sanitizeConfig Tests
// ============================================================================

describe('sanitizeConfig', () => {
  it('should redact S3 credentials from config', () => {
    const config = {
      endpoint: 'https://s3.amazonaws.com',
      accessKeyId: AWS_ACCESS_KEY_ID,
      secretAccessKey: AWS_SECRET_ACCESS_KEY,
      bucket: 'my-bucket',
      region: 'us-east-1',
    };

    const sanitized = sanitizeConfig(config);

    expect(sanitized.accessKeyId).toBe('[REDACTED]');
    expect(sanitized.secretAccessKey).toBe('[REDACTED]');
    expect(sanitized.endpoint).toBe('https://s3.amazonaws.com');
    expect(sanitized.bucket).toBe('my-bucket');
    expect(sanitized.region).toBe('us-east-1');
  });

  it('should redact OAuth credentials from config', () => {
    const config = {
      clientId: 'mongolake-cli',
      clientSecret: 'super_secret_client_secret',
      authUrl: 'https://auth.example.com/token',
    };

    const sanitized = sanitizeConfig(config);

    expect(sanitized.clientId).toBe('[REDACTED]');
    expect(sanitized.clientSecret).toBe('[REDACTED]');
    // authUrl is not a sensitive field, only *Endpoint containing 'token' is
    expect(sanitized.authUrl).toBe('https://auth.example.com/token');
  });

  it('should redact database passwords from config', () => {
    const config = {
      host: 'localhost',
      port: 27017,
      database: 'mydb',
      databasePassword: 'secret_db_password',
    };

    const sanitized = sanitizeConfig(config);

    expect(sanitized.databasePassword).toBe('[REDACTED]');
    expect(sanitized.host).toBe('localhost');
    expect(sanitized.port).toBe(27017);
    expect(sanitized.database).toBe('mydb');
  });

  it('should redact nested credentials', () => {
    const config = {
      storage: {
        s3: {
          endpoint: 'https://s3.amazonaws.com',
          accessKeyId: AWS_ACCESS_KEY_ID,
          secretAccessKey: AWS_SECRET_ACCESS_KEY,
        },
      },
      auth: {
        apiKey: GENERIC_API_KEY,
      },
    };

    const sanitized = sanitizeConfig(config);
    const s3 = (sanitized.storage as Record<string, unknown>).s3 as Record<string, unknown>;
    const auth = sanitized.auth as Record<string, unknown>;

    expect(s3.accessKeyId).toBe('[REDACTED]');
    expect(s3.secretAccessKey).toBe('[REDACTED]');
    expect(s3.endpoint).toBe('https://s3.amazonaws.com');
    expect(auth.apiKey).toBe('[REDACTED]');
  });

  it('should handle arrays of configs', () => {
    const config = {
      credentials: [
        { name: 'prod', apiKey: 'api_key_prod_123456' },
        { name: 'dev', apiKey: 'api_key_dev_789012' },
      ],
    };

    const sanitized = sanitizeConfig(config);
    const creds = sanitized.credentials as Array<Record<string, unknown>>;

    expect(creds[0].apiKey).toBe('[REDACTED]');
    expect(creds[0].name).toBe('prod');
    expect(creds[1].apiKey).toBe('[REDACTED]');
    expect(creds[1].name).toBe('dev');
  });
});

// ============================================================================
// createSafeErrorMessage Tests
// ============================================================================

describe('createSafeErrorMessage', () => {
  it('should create safe error message from Error with credentials', () => {
    const error = new Error(`API call failed with key ${AWS_ACCESS_KEY_ID}`);

    const message = createSafeErrorMessage('S3 PUT', error);

    expect(message).toContain('S3 PUT:');
    expect(message).not.toContain(AWS_ACCESS_KEY_ID);
    expect(message).toContain('[REDACTED');
  });

  it('should create safe error message from string with credentials', () => {
    const error = `Token expired: ${ACCESS_TOKEN}`;

    const message = createSafeErrorMessage('Auth refresh', error);

    expect(message).toContain('Auth refresh:');
    expect(message).not.toContain('eyJ');
    expect(message).toContain('[REDACTED');
  });

  it('should handle unknown error types', () => {
    const message = createSafeErrorMessage('Operation', null);

    expect(message).toBe('Operation: Unknown error');
  });

  it('should preserve context without credentials', () => {
    const error = new Error('File not found: path/to/file.txt');

    const message = createSafeErrorMessage('S3 GET', error);

    expect(message).toBe('S3 GET: File not found: path/to/file.txt');
  });
});

// ============================================================================
// looksLikeCredential Tests
// ============================================================================

describe('looksLikeCredential', () => {
  it('should detect AWS access keys', () => {
    expect(looksLikeCredential(AWS_ACCESS_KEY_ID)).toBe(true);
  });

  it('should detect JWT tokens', () => {
    expect(looksLikeCredential(ACCESS_TOKEN)).toBe(true);
  });

  it('should detect Stripe API keys', () => {
    expect(looksLikeCredential(STRIPE_API_KEY)).toBe(true);
  });

  it('should detect long hex strings', () => {
    const hexSecret = 'a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2';
    expect(looksLikeCredential(hexSecret)).toBe(true);
  });

  it('should not flag normal strings as credentials', () => {
    expect(looksLikeCredential('my-bucket')).toBe(false);
    expect(looksLikeCredential('path/to/file.txt')).toBe(false);
    expect(looksLikeCredential('user@example.com')).toBe(false);
  });

  it('should not flag short hex strings as credentials', () => {
    expect(looksLikeCredential('abc123')).toBe(false);
    expect(looksLikeCredential('deadbeef')).toBe(false);
  });
});

// ============================================================================
// Integration: S3 Error Scenarios
// ============================================================================

describe('S3 Error Scenarios', () => {
  it('should safely handle S3 config in error context', () => {
    const s3Config = {
      endpoint: 'https://s3.us-east-1.amazonaws.com',
      accessKeyId: AWS_ACCESS_KEY_ID,
      secretAccessKey: AWS_SECRET_ACCESS_KEY,
      bucket: 'my-bucket',
      region: 'us-east-1',
    };

    const error = new Error('S3 PUT failed') as Error & Record<string, unknown>;
    error.config = s3Config;
    error.key = 'path/to/object.parquet';
    error.statusCode = 403;

    const sanitized = sanitizeError(error);
    const config = sanitized.config as Record<string, unknown>;

    // Sensitive data should be redacted
    expect(config.accessKeyId).toBe('[REDACTED]');
    expect(config.secretAccessKey).toBe('[REDACTED]');

    // Non-sensitive data should be preserved
    expect(config.endpoint).toBe('https://s3.us-east-1.amazonaws.com');
    expect(config.bucket).toBe('my-bucket');
    expect(config.region).toBe('us-east-1');
    expect(sanitized.key).toBe('path/to/object.parquet');
    expect(sanitized.statusCode).toBe(403);
  });
});

// ============================================================================
// Integration: Auth Error Scenarios
// ============================================================================

describe('Auth Error Scenarios', () => {
  it('should safely handle OAuth token errors', () => {
    const error = new Error('Token validation failed') as Error & Record<string, unknown>;
    error.token = ACCESS_TOKEN;
    error.refreshToken = REFRESH_TOKEN;
    error.userId = 'user123';

    const sanitized = sanitizeError(error);

    // Tokens should be redacted
    expect(sanitized.token).toBe('[REDACTED]');
    expect(sanitized.refreshToken).toBe('[REDACTED]');

    // Non-sensitive data should be preserved
    expect(sanitized.userId).toBe('user123');
  });

  it('should safely handle API key validation errors', () => {
    const error = new Error('Invalid API key') as Error & Record<string, unknown>;
    error.apiKey = STRIPE_API_KEY;
    error.endpoint = '/api/v1/resource';

    const sanitized = sanitizeError(error);

    expect(sanitized.apiKey).toBe('[REDACTED]');
    expect(sanitized.endpoint).toBe('/api/v1/resource');
  });

  it('should safely handle keychain access errors', () => {
    const error = new Error(`Keychain read failed for token: ${ACCESS_TOKEN}`);

    const sanitized = sanitizeError(error);

    expect(sanitized.message).not.toContain('eyJ');
    expect(sanitized.message).toContain('[REDACTED');
  });
});

// ============================================================================
// Edge Cases
// ============================================================================

describe('Edge Cases', () => {
  it('should handle null error', () => {
    const sanitized = sanitizeError(null);
    expect(sanitized.message).toBe('Unknown error');
  });

  it('should handle undefined error', () => {
    const sanitized = sanitizeError(undefined);
    expect(sanitized.message).toBe('Unknown error');
  });

  it('should handle number error', () => {
    const sanitized = sanitizeError(404);
    expect(sanitized.message).toBe('404');
  });

  it('should handle empty string error', () => {
    const sanitized = sanitizeError('');
    expect(sanitized.message).toBe('');
  });

  it('should handle error with no message', () => {
    const error = {} as Record<string, unknown>;
    error.accessKeyId = AWS_ACCESS_KEY_ID;

    const sanitized = sanitizeError(error);

    expect(sanitized.accessKeyId).toBe('[REDACTED]');
    expect(sanitized.message).toBe('Unknown error');
  });

  it('should not modify original error', () => {
    const error = new Error('Original') as Error & Record<string, unknown>;
    error.accessKeyId = AWS_ACCESS_KEY_ID;

    sanitizeError(error);

    // Original should be unchanged
    expect(error.accessKeyId).toBe(AWS_ACCESS_KEY_ID);
    expect(error.message).toBe('Original');
  });
});
