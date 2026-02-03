/**
 * Configuration Validation Framework for MongoLake
 *
 * Provides comprehensive validation for MongoLakeConfig objects including:
 * - Required field validation
 * - Type checking for all configuration options
 * - Value range validation
 * - Default value management
 * - Configuration merging with defaults
 *
 * @module config/validator
 */

import type { MongoLakeConfig, SchemaConfig, CollectionSchema, ColumnDef, ParquetType } from '@types';
import { ValidationError, ErrorCodes } from '../errors/index.js';

// ============================================================================
// Constants
// ============================================================================

/**
 * Valid Parquet types for column definitions
 */
export const VALID_PARQUET_TYPES: readonly ParquetType[] = [
  'string',
  'int32',
  'int64',
  'float',
  'double',
  'boolean',
  'timestamp',
  'date',
  'binary',
  'variant',
] as const;

/**
 * Maximum length for database names
 */
const MAX_DATABASE_NAME_LENGTH = 120;

/**
 * Maximum length for branch names
 */
const MAX_BRANCH_NAME_LENGTH = 255;

/**
 * Minimum auto-promote threshold (0-1 range, representing percentage)
 */
const MIN_AUTO_PROMOTE_THRESHOLD = 0;

/**
 * Maximum auto-promote threshold
 */
const MAX_AUTO_PROMOTE_THRESHOLD = 1;

// ============================================================================
// Error Types
// ============================================================================

/**
 * Configuration validation error with detailed field information
 */
export class ConfigValidationError extends ValidationError {
  /** The configuration field that failed validation */
  public readonly field: string;

  constructor(
    message: string,
    field: string,
    details?: Record<string, unknown>
  ) {
    super(message, ErrorCodes.VALIDATION_FAILED, {
      ...details,
      validationType: 'config',
      field,
    });
    this.name = 'ConfigValidationError';
    this.field = field;
  }
}

// ============================================================================
// Validation Result Types
// ============================================================================

/**
 * Represents a single validation issue
 */
export interface ValidationIssue {
  /** The field path that has the issue (e.g., 'schema.users.columns.name') */
  field: string;
  /** Human-readable error message */
  message: string;
  /** Severity of the issue */
  severity: 'error' | 'warning';
}

/**
 * Result of configuration validation
 */
export interface ValidationResult {
  /** Whether the configuration is valid (no errors) */
  valid: boolean;
  /** List of validation issues (errors and warnings) */
  issues: ValidationIssue[];
  /** Just the errors (convenience accessor) */
  errors: ValidationIssue[];
  /** Just the warnings (convenience accessor) */
  warnings: ValidationIssue[];
}

// ============================================================================
// Default Configuration
// ============================================================================

/**
 * Get default configuration values for MongoLake
 *
 * @returns Default configuration object with all optional fields set to their defaults
 *
 * @example
 * ```typescript
 * const defaults = getDefaults();
 * console.log(defaults.database); // 'default'
 * ```
 */
export function getDefaults(): Required<Pick<MongoLakeConfig, 'database'>> & Partial<MongoLakeConfig> {
  return {
    database: 'default',
    iceberg: false,
    branch: 'main',
  };
}

/**
 * Merge user configuration with default values
 *
 * User-provided values take precedence over defaults.
 * Undefined values in user config are replaced with defaults.
 *
 * @param config - User-provided configuration (may be partial)
 * @returns Configuration with defaults applied
 *
 * @example
 * ```typescript
 * const config = mergeWithDefaults({ local: '.mongolake' });
 * console.log(config.database); // 'default' (from defaults)
 * console.log(config.local); // '.mongolake' (from user)
 * ```
 */
export function mergeWithDefaults(config: MongoLakeConfig = {}): MongoLakeConfig {
  const defaults = getDefaults();

  return {
    ...defaults,
    ...config,
    // Ensure database has a value
    database: config.database ?? defaults.database,
  };
}

// ============================================================================
// Validation Functions
// ============================================================================

/**
 * Validate a MongoLakeConfig object
 *
 * Throws ConfigValidationError if the configuration is invalid.
 * Use validateConfig for non-throwing validation.
 *
 * @param config - Configuration to validate
 * @throws ConfigValidationError if validation fails
 *
 * @example
 * ```typescript
 * try {
 *   validate({ local: '.mongolake' });
 * } catch (error) {
 *   if (error instanceof ConfigValidationError) {
 *     console.error(`Invalid config field: ${error.field}`);
 *   }
 * }
 * ```
 */
export function validate(config: unknown): asserts config is MongoLakeConfig {
  const result = validateConfig(config);

  if (!result.valid) {
    const firstError = result.errors[0];
    if (firstError) {
      throw new ConfigValidationError(
        firstError.message,
        firstError.field,
        { allIssues: result.issues }
      );
    }
    // This should never happen if valid is false, but handle it defensively
    throw new ConfigValidationError(
      'Configuration validation failed',
      'config',
      { issues: result.issues }
    );
  }
}

/**
 * Validate configuration and return detailed results
 *
 * Unlike validate(), this function doesn't throw. It returns a detailed
 * result object with all validation issues categorized by severity.
 *
 * @param config - Configuration to validate
 * @returns Validation result with issues array
 *
 * @example
 * ```typescript
 * const result = validateConfig({ database: '' });
 * if (!result.valid) {
 *   result.errors.forEach(e => console.error(e.message));
 * }
 * ```
 */
export function validateConfig(config: unknown): ValidationResult {
  const issues: ValidationIssue[] = [];

  // Check that config is an object
  if (config === null || config === undefined) {
    // null/undefined is valid - will use defaults
    return { valid: true, issues: [], errors: [], warnings: [] };
  }

  if (typeof config !== 'object' || Array.isArray(config)) {
    issues.push({
      field: 'config',
      message: 'Configuration must be an object',
      severity: 'error',
    });
    return {
      valid: false,
      issues,
      errors: issues.filter(i => i.severity === 'error'),
      warnings: issues.filter(i => i.severity === 'warning'),
    };
  }

  const cfg = config as Record<string, unknown>;

  // Validate storage configuration
  validateStorageConfig(cfg, issues);

  // Validate database name
  validateDatabaseConfig(cfg, issues);

  // Validate iceberg configuration
  validateIcebergConfig(cfg, issues);

  // Validate schema configuration
  validateSchemaConfig(cfg, issues);

  // Validate branch name
  validateBranchConfig(cfg, issues);

  // Validate asOf timestamp
  validateAsOfConfig(cfg, issues);

  // Validate connectionString (internal field)
  validateConnectionStringConfig(cfg, issues);

  return {
    valid: issues.filter(i => i.severity === 'error').length === 0,
    issues,
    errors: issues.filter(i => i.severity === 'error'),
    warnings: issues.filter(i => i.severity === 'warning'),
  };
}

// ============================================================================
// Storage Validation
// ============================================================================

function validateStorageConfig(config: Record<string, unknown>, issues: ValidationIssue[]): void {
  const { local, bucket, endpoint, accessKeyId, secretAccessKey, bucketName } = config;

  // Count how many storage backends are configured
  const hasLocal = local !== undefined;
  const hasBucket = bucket !== undefined;
  const hasS3 = endpoint !== undefined || accessKeyId !== undefined ||
                secretAccessKey !== undefined || bucketName !== undefined;

  // Validate local storage path
  if (hasLocal) {
    if (typeof local !== 'string') {
      issues.push({
        field: 'local',
        message: 'Local storage path must be a string',
        severity: 'error',
      });
    } else if (local.trim() === '') {
      issues.push({
        field: 'local',
        message: 'Local storage path cannot be empty',
        severity: 'error',
      });
    }
  }

  // Validate R2 bucket (type check - actual R2Bucket validation is runtime)
  if (hasBucket && bucket !== null && typeof bucket !== 'object') {
    issues.push({
      field: 'bucket',
      message: 'R2 bucket must be an object implementing R2Bucket interface',
      severity: 'error',
    });
  }

  // Validate S3 configuration - all fields required if any are specified
  if (hasS3) {
    if (endpoint !== undefined && typeof endpoint !== 'string') {
      issues.push({
        field: 'endpoint',
        message: 'S3 endpoint must be a string',
        severity: 'error',
      });
    } else if (typeof endpoint === 'string' && endpoint.trim() !== '') {
      // Validate endpoint URL format
      try {
        new URL(endpoint);
      } catch {
        issues.push({
          field: 'endpoint',
          message: 'S3 endpoint must be a valid URL',
          severity: 'error',
        });
      }
    }

    if (accessKeyId !== undefined && typeof accessKeyId !== 'string') {
      issues.push({
        field: 'accessKeyId',
        message: 'S3 access key ID must be a string',
        severity: 'error',
      });
    }

    if (secretAccessKey !== undefined && typeof secretAccessKey !== 'string') {
      issues.push({
        field: 'secretAccessKey',
        message: 'S3 secret access key must be a string',
        severity: 'error',
      });
    }

    if (bucketName !== undefined && typeof bucketName !== 'string') {
      issues.push({
        field: 'bucketName',
        message: 'S3 bucket name must be a string',
        severity: 'error',
      });
    } else if (typeof bucketName === 'string' && bucketName.trim() === '') {
      issues.push({
        field: 'bucketName',
        message: 'S3 bucket name cannot be empty',
        severity: 'error',
      });
    }

    // Warn if S3 config is incomplete
    const s3Fields = [endpoint, accessKeyId, secretAccessKey, bucketName];
    const definedS3Fields = s3Fields.filter(f => f !== undefined);
    if (definedS3Fields.length > 0 && definedS3Fields.length < 4) {
      const missingFields: string[] = [];
      if (endpoint === undefined) missingFields.push('endpoint');
      if (accessKeyId === undefined) missingFields.push('accessKeyId');
      if (secretAccessKey === undefined) missingFields.push('secretAccessKey');
      if (bucketName === undefined) missingFields.push('bucketName');

      issues.push({
        field: 'storage',
        message: `Incomplete S3 configuration. Missing: ${missingFields.join(', ')}`,
        severity: 'warning',
      });
    }
  }

  // Warn if multiple storage backends are configured
  const configuredBackends = [hasLocal, hasBucket, hasS3].filter(Boolean).length;
  if (configuredBackends > 1) {
    issues.push({
      field: 'storage',
      message: 'Multiple storage backends configured. Only one will be used (priority: bucket > S3 > local)',
      severity: 'warning',
    });
  }
}

// ============================================================================
// Database Validation
// ============================================================================

function validateDatabaseConfig(config: Record<string, unknown>, issues: ValidationIssue[]): void {
  const { database } = config;

  if (database === undefined) {
    return; // Optional, will use default
  }

  if (typeof database !== 'string') {
    issues.push({
      field: 'database',
      message: 'Database name must be a string',
      severity: 'error',
    });
    return;
  }

  if (database.trim() === '') {
    issues.push({
      field: 'database',
      message: 'Database name cannot be empty',
      severity: 'error',
    });
    return;
  }

  if (database.length > MAX_DATABASE_NAME_LENGTH) {
    issues.push({
      field: 'database',
      message: `Database name exceeds maximum length of ${MAX_DATABASE_NAME_LENGTH} characters`,
      severity: 'error',
    });
  }

  // Check for invalid characters
  if (database.includes('\0')) {
    issues.push({
      field: 'database',
      message: 'Database name cannot contain null bytes',
      severity: 'error',
    });
  }

  // Check for path traversal attempts
  if (database.includes('/') || database.includes('\\') ||
      database.includes('..') || database.includes('.')) {
    issues.push({
      field: 'database',
      message: 'Database name contains invalid characters (/, \\, .)',
      severity: 'error',
    });
  }

  // Check for names starting with underscore or hyphen
  if (database.startsWith('_') || database.startsWith('-')) {
    issues.push({
      field: 'database',
      message: 'Database name cannot start with underscore or hyphen',
      severity: 'error',
    });
  }
}

// ============================================================================
// Iceberg Validation
// ============================================================================

function validateIcebergConfig(config: Record<string, unknown>, issues: ValidationIssue[]): void {
  const { iceberg } = config;

  if (iceberg === undefined) {
    return; // Optional
  }

  if (typeof iceberg === 'boolean') {
    return; // Valid: iceberg: true/false
  }

  if (typeof iceberg !== 'object' || iceberg === null || Array.isArray(iceberg)) {
    issues.push({
      field: 'iceberg',
      message: 'Iceberg configuration must be a boolean or an object with token and optional catalog',
      severity: 'error',
    });
    return;
  }

  const icebergConfig = iceberg as Record<string, unknown>;

  // Token is required when iceberg is an object
  if (icebergConfig.token === undefined) {
    issues.push({
      field: 'iceberg.token',
      message: 'Iceberg token is required when using object configuration',
      severity: 'error',
    });
  } else if (typeof icebergConfig.token !== 'string') {
    issues.push({
      field: 'iceberg.token',
      message: 'Iceberg token must be a string',
      severity: 'error',
    });
  } else if (icebergConfig.token.trim() === '') {
    issues.push({
      field: 'iceberg.token',
      message: 'Iceberg token cannot be empty',
      severity: 'error',
    });
  }

  // Catalog is optional but must be a string if provided
  if (icebergConfig.catalog !== undefined && typeof icebergConfig.catalog !== 'string') {
    issues.push({
      field: 'iceberg.catalog',
      message: 'Iceberg catalog must be a string',
      severity: 'error',
    });
  }
}

// ============================================================================
// Schema Validation
// ============================================================================

function validateSchemaConfig(config: Record<string, unknown>, issues: ValidationIssue[]): void {
  const { schema } = config;

  if (schema === undefined) {
    return; // Optional
  }

  if (typeof schema !== 'object' || schema === null || Array.isArray(schema)) {
    issues.push({
      field: 'schema',
      message: 'Schema configuration must be an object mapping collection names to schemas',
      severity: 'error',
    });
    return;
  }

  const schemaConfig = schema as Record<string, unknown>;

  for (const [collectionName, collectionSchema] of Object.entries(schemaConfig)) {
    validateCollectionSchema(collectionName, collectionSchema, issues);
  }
}

function validateCollectionSchema(
  collectionName: string,
  schema: unknown,
  issues: ValidationIssue[]
): void {
  const fieldPrefix = `schema.${collectionName}`;

  if (typeof schema !== 'object' || schema === null || Array.isArray(schema)) {
    issues.push({
      field: fieldPrefix,
      message: `Collection schema for '${collectionName}' must be an object`,
      severity: 'error',
    });
    return;
  }

  const collSchema = schema as Record<string, unknown>;

  // Validate columns
  if (collSchema.columns !== undefined) {
    if (typeof collSchema.columns !== 'object' || collSchema.columns === null || Array.isArray(collSchema.columns)) {
      issues.push({
        field: `${fieldPrefix}.columns`,
        message: 'Columns configuration must be an object',
        severity: 'error',
      });
    } else {
      const columns = collSchema.columns as Record<string, unknown>;
      for (const [columnName, columnDef] of Object.entries(columns)) {
        validateColumnDef(columnName, columnDef, `${fieldPrefix}.columns.${columnName}`, issues);
      }
    }
  }

  // Validate autoPromote
  if (collSchema.autoPromote !== undefined) {
    if (typeof collSchema.autoPromote !== 'object' || collSchema.autoPromote === null || Array.isArray(collSchema.autoPromote)) {
      issues.push({
        field: `${fieldPrefix}.autoPromote`,
        message: 'autoPromote configuration must be an object with a threshold property',
        severity: 'error',
      });
    } else {
      const autoPromote = collSchema.autoPromote as Record<string, unknown>;
      if (autoPromote.threshold === undefined) {
        issues.push({
          field: `${fieldPrefix}.autoPromote.threshold`,
          message: 'autoPromote.threshold is required',
          severity: 'error',
        });
      } else if (typeof autoPromote.threshold !== 'number') {
        issues.push({
          field: `${fieldPrefix}.autoPromote.threshold`,
          message: 'autoPromote.threshold must be a number',
          severity: 'error',
        });
      } else if (autoPromote.threshold < MIN_AUTO_PROMOTE_THRESHOLD || autoPromote.threshold > MAX_AUTO_PROMOTE_THRESHOLD) {
        issues.push({
          field: `${fieldPrefix}.autoPromote.threshold`,
          message: `autoPromote.threshold must be between ${MIN_AUTO_PROMOTE_THRESHOLD} and ${MAX_AUTO_PROMOTE_THRESHOLD}`,
          severity: 'error',
        });
      }
    }
  }

  // Validate storeVariant
  if (collSchema.storeVariant !== undefined && typeof collSchema.storeVariant !== 'boolean') {
    issues.push({
      field: `${fieldPrefix}.storeVariant`,
      message: 'storeVariant must be a boolean',
      severity: 'error',
    });
  }
}

function validateColumnDef(
  columnName: string,
  def: unknown,
  fieldPath: string,
  issues: ValidationIssue[]
): void {
  // String type (e.g., 'string', 'int32')
  if (typeof def === 'string') {
    if (!VALID_PARQUET_TYPES.includes(def as ParquetType)) {
      issues.push({
        field: fieldPath,
        message: `Invalid Parquet type '${def}'. Valid types: ${VALID_PARQUET_TYPES.join(', ')}`,
        severity: 'error',
      });
    }
    return;
  }

  // Array type (e.g., ['string'] for array of strings)
  if (Array.isArray(def)) {
    if (def.length !== 1) {
      issues.push({
        field: fieldPath,
        message: 'Array column definition must have exactly one element specifying the element type',
        severity: 'error',
      });
      return;
    }
    validateColumnDef(`${columnName}[]`, def[0], `${fieldPath}[0]`, issues);
    return;
  }

  // Struct type (nested object)
  if (typeof def === 'object' && def !== null) {
    const structDef = def as Record<string, unknown>;
    for (const [nestedName, nestedDef] of Object.entries(structDef)) {
      validateColumnDef(nestedName, nestedDef, `${fieldPath}.${nestedName}`, issues);
    }
    return;
  }

  // Invalid type
  issues.push({
    field: fieldPath,
    message: `Invalid column definition. Expected a Parquet type string, array, or struct object`,
    severity: 'error',
  });
}

// ============================================================================
// Branch Validation
// ============================================================================

function validateBranchConfig(config: Record<string, unknown>, issues: ValidationIssue[]): void {
  const { branch } = config;

  if (branch === undefined) {
    return; // Optional, will use default
  }

  if (typeof branch !== 'string') {
    issues.push({
      field: 'branch',
      message: 'Branch name must be a string',
      severity: 'error',
    });
    return;
  }

  if (branch.trim() === '') {
    issues.push({
      field: 'branch',
      message: 'Branch name cannot be empty',
      severity: 'error',
    });
    return;
  }

  if (branch.length > MAX_BRANCH_NAME_LENGTH) {
    issues.push({
      field: 'branch',
      message: `Branch name exceeds maximum length of ${MAX_BRANCH_NAME_LENGTH} characters`,
      severity: 'error',
    });
  }

  // Git branch name validation (simplified)
  if (branch.includes('..') || branch.startsWith('/') || branch.endsWith('/') ||
      branch.includes('//') || branch.endsWith('.lock') || branch.includes('@{')) {
    issues.push({
      field: 'branch',
      message: 'Branch name contains invalid characters or patterns',
      severity: 'error',
    });
  }
}

// ============================================================================
// AsOf Validation
// ============================================================================

function validateAsOfConfig(config: Record<string, unknown>, issues: ValidationIssue[]): void {
  const { asOf } = config;

  if (asOf === undefined) {
    return; // Optional
  }

  // Valid types: string (ISO date), Date, number (timestamp)
  if (typeof asOf === 'string') {
    const parsed = Date.parse(asOf);
    if (isNaN(parsed)) {
      issues.push({
        field: 'asOf',
        message: 'asOf string must be a valid ISO 8601 date string',
        severity: 'error',
      });
    }
    return;
  }

  if (asOf instanceof Date) {
    if (isNaN(asOf.getTime())) {
      issues.push({
        field: 'asOf',
        message: 'asOf Date is invalid',
        severity: 'error',
      });
    }
    return;
  }

  if (typeof asOf === 'number') {
    if (!Number.isFinite(asOf) || asOf < 0) {
      issues.push({
        field: 'asOf',
        message: 'asOf timestamp must be a positive finite number',
        severity: 'error',
      });
    }
    return;
  }

  issues.push({
    field: 'asOf',
    message: 'asOf must be a string (ISO date), Date object, or number (timestamp)',
    severity: 'error',
  });
}

// ============================================================================
// Connection String Validation (Internal)
// ============================================================================

function validateConnectionStringConfig(config: Record<string, unknown>, issues: ValidationIssue[]): void {
  const { connectionString } = config;

  if (connectionString === undefined) {
    return; // Optional, internal field
  }

  if (typeof connectionString !== 'object' || connectionString === null || Array.isArray(connectionString)) {
    issues.push({
      field: 'connectionString',
      message: 'connectionString must be an object (internal field)',
      severity: 'error',
    });
    return;
  }

  const connStr = connectionString as Record<string, unknown>;

  // Validate hosts array
  if (connStr.hosts !== undefined) {
    if (!Array.isArray(connStr.hosts)) {
      issues.push({
        field: 'connectionString.hosts',
        message: 'connectionString.hosts must be an array',
        severity: 'error',
      });
    } else {
      for (let i = 0; i < connStr.hosts.length; i++) {
        const host = connStr.hosts[i] as Record<string, unknown>;
        if (typeof host !== 'object' || host === null) {
          issues.push({
            field: `connectionString.hosts[${i}]`,
            message: 'Each host must be an object with host and port properties',
            severity: 'error',
          });
        } else {
          if (typeof host.host !== 'string') {
            issues.push({
              field: `connectionString.hosts[${i}].host`,
              message: 'Host must be a string',
              severity: 'error',
            });
          }
          if (typeof host.port !== 'number' || host.port < 1 || host.port > 65535) {
            issues.push({
              field: `connectionString.hosts[${i}].port`,
              message: 'Port must be a number between 1 and 65535',
              severity: 'error',
            });
          }
        }
      }
    }
  }

  // Validate username/password if present
  if (connStr.username !== undefined && typeof connStr.username !== 'string') {
    issues.push({
      field: 'connectionString.username',
      message: 'Username must be a string',
      severity: 'error',
    });
  }

  if (connStr.password !== undefined && typeof connStr.password !== 'string') {
    issues.push({
      field: 'connectionString.password',
      message: 'Password must be a string',
      severity: 'error',
    });
  }

  // Validate options if present
  if (connStr.options !== undefined && (typeof connStr.options !== 'object' || connStr.options === null || Array.isArray(connStr.options))) {
    issues.push({
      field: 'connectionString.options',
      message: 'Options must be an object',
      severity: 'error',
    });
  }
}

// ============================================================================
// Convenience Exports
// ============================================================================

/**
 * Check if a value is a valid Parquet type
 */
export function isValidParquetType(type: unknown): type is ParquetType {
  return typeof type === 'string' && VALID_PARQUET_TYPES.includes(type as ParquetType);
}

/**
 * Validate and merge configuration in one step
 *
 * @param config - User configuration to validate and merge with defaults
 * @returns Merged configuration
 * @throws ConfigValidationError if validation fails
 */
export function validateAndMerge(config: unknown): MongoLakeConfig {
  validate(config);
  return mergeWithDefaults(config as MongoLakeConfig);
}
