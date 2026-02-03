/**
 * Error Sanitization Utility
 *
 * Provides functions to sanitize error messages and objects to prevent
 * credential leakage in logs, error responses, and stack traces.
 *
 * @module utils/sanitize-error
 */

// ============================================================================
// Constants
// ============================================================================

/**
 * List of field names that contain sensitive data and should be redacted.
 * These are matched case-insensitively against object keys and string content.
 */
const SENSITIVE_FIELD_NAMES = [
  // AWS/S3 credentials
  'accessKeyId',
  'secretAccessKey',
  'sessionToken',
  'awsAccessKeyId',
  'awsSecretAccessKey',
  'awsSessionToken',

  // Generic authentication
  'password',
  'passwd',
  'secret',
  'apiKey',
  'api_key',
  'apikey',
  'authToken',
  'auth_token',
  'accessToken',
  'access_token',
  'refreshToken',
  'refresh_token',
  'idToken',
  'id_token',
  'bearerToken',
  'bearer_token',
  'token',

  // OAuth/OIDC
  'clientSecret',
  'client_secret',
  'clientId',
  'client_id',

  // Database credentials
  'connectionString',
  'connection_string',
  'databasePassword',
  'database_password',
  'dbPassword',
  'db_password',

  // Private keys
  'privateKey',
  'private_key',
  'signingKey',
  'signing_key',
  'encryptionKey',
  'encryption_key',
  'jwtSecret',
  'jwt_secret',

  // Other sensitive data
  'authorization',
  'cookie',
  'x-api-key',
  'x-auth-token',
];

/**
 * Regex patterns for detecting credentials in string content.
 * These patterns match common credential formats that might appear in error messages.
 */
const SENSITIVE_PATTERNS: Array<{ pattern: RegExp; replacement: string }> = [
  // AWS Access Key ID (starts with AKIA, ABIA, ACCA, AIDA, AROA, AIPA, ANPA, ANVA, ASIA)
  {
    pattern: /\b(A[KS]IA|ABIA|ACCA|AIDA|AROA|AIPA|ANPA|ANVA|ASIA)[A-Z0-9]{16}\b/g,
    replacement: '[REDACTED_AWS_KEY]',
  },
  // AWS Secret Access Key (40 characters, base64-like)
  {
    pattern: /\b[A-Za-z0-9/+=]{40}\b/g,
    replacement: '[REDACTED_SECRET]',
  },
  // Generic API keys (long alphanumeric strings that look like keys)
  {
    pattern: /\b(sk_live_|sk_test_|pk_live_|pk_test_)[a-zA-Z0-9]{24,}\b/g,
    replacement: '[REDACTED_API_KEY]',
  },
  // Bearer tokens in strings
  {
    pattern: /Bearer\s+[A-Za-z0-9\-_=]+\.[A-Za-z0-9\-_=]+\.[A-Za-z0-9\-_=]*/gi,
    replacement: 'Bearer [REDACTED_TOKEN]',
  },
  // JWT tokens (header.payload.signature format)
  {
    pattern: /eyJ[A-Za-z0-9\-_=]+\.eyJ[A-Za-z0-9\-_=]+\.[A-Za-z0-9\-_=]*/g,
    replacement: '[REDACTED_JWT]',
  },
  // Generic long hex strings (possible secrets)
  {
    pattern: /\b[a-fA-F0-9]{32,}\b/g,
    replacement: '[REDACTED_HEX]',
  },
  // Connection strings with passwords
  {
    pattern: /:\/\/[^:]+:[^@]+@/g,
    replacement: '://[REDACTED_CREDENTIALS]@',
  },
];

/**
 * The replacement text for redacted sensitive values
 */
const REDACTED = '[REDACTED]';

// ============================================================================
// Types
// ============================================================================

/**
 * Options for error sanitization
 */
export interface SanitizeOptions {
  /**
   * Additional field names to treat as sensitive (in addition to defaults)
   */
  additionalSensitiveFields?: string[];

  /**
   * Whether to sanitize nested objects recursively (default: true)
   */
  recursive?: boolean;

  /**
   * Maximum depth for recursive sanitization (default: 10)
   */
  maxDepth?: number;

  /**
   * Whether to redact patterns in string values (default: true)
   */
  redactPatterns?: boolean;
}

/**
 * Result of sanitizing an error
 */
export interface SanitizedError {
  message: string;
  name?: string;
  code?: string;
  stack?: string;
  cause?: SanitizedError;
  [key: string]: unknown;
}

// ============================================================================
// Internal Helpers
// ============================================================================

/**
 * Check if a field name should be considered sensitive
 */
function isSensitiveField(fieldName: string, additionalFields: string[] = []): boolean {
  const lowerFieldName = fieldName.toLowerCase();
  const allSensitiveFields = [...SENSITIVE_FIELD_NAMES, ...additionalFields];

  return allSensitiveFields.some((sensitive) => {
    const lowerSensitive = sensitive.toLowerCase();
    // Exact match or contains the sensitive name
    return lowerFieldName === lowerSensitive || lowerFieldName.includes(lowerSensitive);
  });
}

/**
 * Redact sensitive patterns from a string
 */
function redactPatternsInString(str: string): string {
  let result = str;
  for (const { pattern, replacement } of SENSITIVE_PATTERNS) {
    // Reset lastIndex for global patterns
    pattern.lastIndex = 0;
    result = result.replace(pattern, replacement);
  }
  return result;
}

/**
 * Recursively sanitize an object
 */
function sanitizeObject(
  obj: Record<string, unknown>,
  options: Required<SanitizeOptions>,
  depth: number,
  seen: WeakSet<object>
): Record<string, unknown> {
  // Prevent infinite recursion
  if (depth > options.maxDepth) {
    return { _truncated: 'Max depth exceeded' };
  }

  // Detect circular references
  if (seen.has(obj)) {
    return { _circular: 'Circular reference detected' };
  }
  seen.add(obj);

  const result: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(obj)) {
    // Check if this field is sensitive
    if (isSensitiveField(key, options.additionalSensitiveFields)) {
      result[key] = REDACTED;
      continue;
    }

    // Handle different value types
    if (value === null || value === undefined) {
      result[key] = value;
    } else if (typeof value === 'string') {
      result[key] = options.redactPatterns ? redactPatternsInString(value) : value;
    } else if (typeof value === 'object') {
      if (Array.isArray(value)) {
        result[key] = value.map((item) => {
          if (typeof item === 'string') {
            return options.redactPatterns ? redactPatternsInString(item) : item;
          } else if (typeof item === 'object' && item !== null) {
            return sanitizeObject(item as Record<string, unknown>, options, depth + 1, seen);
          }
          return item;
        });
      } else if (options.recursive) {
        result[key] = sanitizeObject(value as Record<string, unknown>, options, depth + 1, seen);
      } else {
        result[key] = value;
      }
    } else {
      result[key] = value;
    }
  }

  return result;
}

// ============================================================================
// Public API
// ============================================================================

/**
 * Sanitize an error object to remove sensitive credentials from its properties.
 *
 * This function:
 * 1. Removes sensitive field values from the error object
 * 2. Redacts credential patterns from the error message
 * 3. Sanitizes the stack trace to remove credentials
 * 4. Recursively sanitizes nested objects and error causes
 *
 * @param error - The error to sanitize (can be Error, object, or string)
 * @param options - Sanitization options
 * @returns A sanitized copy of the error (original is not modified)
 *
 * @example
 * ```ts
 * try {
 *   await s3.put(key, data);
 * } catch (err) {
 *   const sanitized = sanitizeError(err);
 *   console.error('S3 operation failed:', sanitized);
 * }
 * ```
 */
export function sanitizeError(error: unknown, options: SanitizeOptions = {}): SanitizedError {
  const opts: Required<SanitizeOptions> = {
    additionalSensitiveFields: options.additionalSensitiveFields ?? [],
    recursive: options.recursive ?? true,
    maxDepth: options.maxDepth ?? 10,
    redactPatterns: options.redactPatterns ?? true,
  };

  const seen = new WeakSet<object>();

  // Handle string errors
  if (typeof error === 'string') {
    return {
      message: opts.redactPatterns ? redactPatternsInString(error) : error,
    };
  }

  // Handle null/undefined
  if (error === null || error === undefined) {
    return { message: 'Unknown error' };
  }

  // Handle Error objects
  if (error instanceof Error) {
    const result: SanitizedError = {
      name: error.name,
      message: opts.redactPatterns ? redactPatternsInString(error.message) : error.message,
    };

    // Sanitize stack trace
    if (error.stack) {
      result.stack = opts.redactPatterns ? redactPatternsInString(error.stack) : error.stack;
    }

    // Handle error cause (ES2022+)
    if ('cause' in error && error.cause) {
      result.cause = sanitizeError(error.cause, options);
    }

    // Handle additional properties on the error
    const errorObj = error as Error & Record<string, unknown>;
    for (const key of Object.keys(errorObj)) {
      if (key === 'name' || key === 'message' || key === 'stack' || key === 'cause') {
        continue;
      }

      const value = errorObj[key];
      if (isSensitiveField(key, opts.additionalSensitiveFields)) {
        result[key] = REDACTED;
      } else if (typeof value === 'string') {
        result[key] = opts.redactPatterns ? redactPatternsInString(value) : value;
      } else if (typeof value === 'object' && value !== null) {
        result[key] = sanitizeObject(value as Record<string, unknown>, opts, 1, seen);
      } else {
        result[key] = value;
      }
    }

    // Check for code property (common in Node.js errors)
    if ('code' in errorObj && typeof errorObj.code === 'string') {
      result.code = errorObj.code;
    }

    return result;
  }

  // Handle plain objects
  if (typeof error === 'object') {
    const obj = error as Record<string, unknown>;
    const sanitized = sanitizeObject(obj, opts, 0, seen);

    // Ensure message property exists
    return {
      message:
        typeof sanitized.message === 'string'
          ? sanitized.message
          : typeof sanitized.error === 'string'
            ? sanitized.error
            : 'Unknown error',
      ...sanitized,
    };
  }

  // Handle other types
  return {
    message: String(error),
  };
}

/**
 * Sanitize a message string to remove sensitive credentials.
 *
 * Use this function when you need to sanitize a string message without
 * creating a full error object.
 *
 * @param message - The message to sanitize
 * @returns The sanitized message with credentials redacted
 *
 * @example
 * ```ts
 * const safeMessage = sanitizeMessage(`Failed to connect with key ${accessKeyId}`);
 * console.error(safeMessage);
 * ```
 */
export function sanitizeMessage(message: string): string {
  return redactPatternsInString(message);
}

/**
 * Sanitize a configuration object to remove sensitive fields.
 *
 * Use this for logging configuration objects safely.
 *
 * @param config - The configuration object to sanitize
 * @param options - Sanitization options
 * @returns A sanitized copy of the configuration
 *
 * @example
 * ```ts
 * const safeConfig = sanitizeConfig({
 *   endpoint: 'https://s3.amazonaws.com',
 *   accessKeyId: 'AKIAIOSFODNN7EXAMPLE',
 *   secretAccessKey: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
 *   bucket: 'my-bucket',
 * });
 * console.log('S3 config:', safeConfig);
 * // Output: { endpoint: 'https://s3.amazonaws.com', accessKeyId: '[REDACTED]', secretAccessKey: '[REDACTED]', bucket: 'my-bucket' }
 * ```
 */
export function sanitizeConfig(
  config: Record<string, unknown>,
  options: SanitizeOptions = {}
): Record<string, unknown> {
  const opts: Required<SanitizeOptions> = {
    additionalSensitiveFields: options.additionalSensitiveFields ?? [],
    recursive: options.recursive ?? true,
    maxDepth: options.maxDepth ?? 10,
    redactPatterns: options.redactPatterns ?? true,
  };

  return sanitizeObject(config, opts, 0, new WeakSet());
}

/**
 * Create a safe error message that includes context without credentials.
 *
 * @param context - A description of what operation failed
 * @param error - The original error
 * @returns A safe error message string
 *
 * @example
 * ```ts
 * try {
 *   await s3.put(key, data);
 * } catch (err) {
 *   throw new Error(createSafeErrorMessage('S3 PUT', err));
 * }
 * ```
 */
export function createSafeErrorMessage(context: string, error: unknown): string {
  if (error instanceof Error) {
    const sanitized = sanitizeError(error);
    return `${context}: ${sanitized.message}`;
  }
  if (typeof error === 'string') {
    return `${context}: ${sanitizeMessage(error)}`;
  }
  return `${context}: Unknown error`;
}

/**
 * Check if a value looks like a sensitive credential.
 *
 * This can be used to validate that a value should not be logged.
 *
 * @param value - The value to check
 * @returns true if the value looks like a credential
 */
export function looksLikeCredential(value: string): boolean {
  // Check against all patterns
  for (const { pattern } of SENSITIVE_PATTERNS) {
    pattern.lastIndex = 0;
    if (pattern.test(value)) {
      return true;
    }
  }
  return false;
}
