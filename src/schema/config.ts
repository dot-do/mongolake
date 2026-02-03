/**
 * Schema Configuration Parser
 *
 * Parses and validates schema configuration for field promotion.
 * Allows specifying which fields should be promoted to native Parquet columns
 * for efficient querying.
 *
 * @module schema/config
 */

import * as yaml from 'yaml';
import type { ParquetType, SchemaConfig, CollectionSchema } from '../types.js';
import { getNestedValue } from '../utils/nested.js';

// ============================================================================
// Error Classes
// ============================================================================

/**
 * Options for constructing SchemaConfigError
 */
export interface SchemaConfigErrorOptions {
  lineNumber?: number;
  columnNumber?: number;
  sourceContext?: string;
  path?: string;
  suggestion?: string;
}

/**
 * Parse error information for detailed error reporting
 */
export interface ParseError {
  message: string;
  path: string;
  lineNumber?: number;
  columnNumber?: number;
  sourceContext?: string;
  suggestion?: string;
}

/**
 * Error class for schema configuration validation failures with line number support
 */
export class SchemaConfigError extends Error {
  /** Line number where error occurred (1-indexed) */
  readonly lineNumber?: number;
  /** Column number where error occurred (1-indexed) */
  readonly columnNumber?: number;
  /** Source context around the error */
  readonly sourceContext?: string;
  /** Path in the schema where error occurred */
  readonly path?: string;
  /** Suggestion for fixing the error */
  readonly suggestion?: string;

  constructor(message: string, options?: SchemaConfigErrorOptions) {
    super(message);
    this.name = 'SchemaConfigError';
    this.lineNumber = options?.lineNumber;
    this.columnNumber = options?.columnNumber;
    this.sourceContext = options?.sourceContext;
    this.path = options?.path;
    this.suggestion = options?.suggestion;
  }

  /**
   * Create a new error with additional location information
   */
  withLocation(location: {
    lineNumber?: number;
    columnNumber?: number;
    sourceContext?: string;
  }): SchemaConfigError {
    return new SchemaConfigError(this.message, {
      lineNumber: location.lineNumber ?? this.lineNumber,
      columnNumber: location.columnNumber ?? this.columnNumber,
      sourceContext: location.sourceContext ?? this.sourceContext,
      path: this.path,
      suggestion: this.suggestion,
    });
  }
}

// ============================================================================
// Types
// ============================================================================

/**
 * Extended column definition with additional properties
 */
export interface ExtendedColumnDef {
  type: ParquetType;
  nullable?: boolean;
  default?: unknown;
  coerce?: {
    from: ParquetType[];
    strict?: boolean;
  };
}

/**
 * Column definition that can be simple or extended
 */
export type ColumnDefinition =
  | ParquetType
  | [ParquetType]
  | [{ [key: string]: ColumnDefinition }]
  | { [key: string]: ColumnDefinition }
  | ExtendedColumnDef;

/**
 * Extended collection schema with additional validation options
 */
export interface ExtendedCollectionSchema extends Omit<CollectionSchema, 'columns'> {
  /** Columns with extended definition support */
  columns?: { [key: string]: ColumnDefinition };
  /** Whether _id column is required */
  requireIdColumn?: boolean;
  /** List of required field names */
  requiredFields?: string[];
  /** Base schema to extend */
  extends?: string;
}

/**
 * Full schema configuration with database-level collections
 */
export interface FullSchemaConfig {
  /** Reusable schema definitions for inheritance */
  schemas?: {
    [schemaName: string]: ExtendedCollectionSchema;
  };
  /** Collection schemas */
  collections: {
    [collectionName: string]: ExtendedCollectionSchema;
  };
}

/**
 * Coercion rules for type conversion
 */
export interface CoercionRules {
  from: ParquetType[];
  strict?: boolean;
}

/**
 * Parsed column definition with path and type information
 */
export interface ParsedColumn {
  /** Original field path (e.g., 'profile.name') */
  path: string;
  /** Path segments for nested access */
  segments: string[];
  /** Parquet type for this column */
  type: ParquetType;
  /** Whether this is an array type */
  isArray: boolean;
  /** Whether this is a struct type */
  isStruct: boolean;
  /** Nested structure definition (for struct types) */
  structDef?: { [key: string]: ParsedColumn };
  /** Whether the field is required */
  required?: boolean;
  /** Whether the field is nullable */
  nullable?: boolean;
  /** Default value for the column */
  defaultValue?: unknown;
  /** Type coercion rules */
  coercionRules?: CoercionRules;
}

/**
 * Parsed collection schema with resolved columns
 */
export interface ParsedCollectionSchema {
  /** Parsed column definitions */
  columns: ParsedColumn[];
  /** Column lookup by path for O(1) access */
  columnMap: Map<string, ParsedColumn>;
  /** Auto-promotion configuration */
  autoPromote?: { threshold: number };
  /** Whether to store full document as variant (default: true) */
  storeVariant: boolean;
  /** List of required field names */
  requiredFields?: string[];
}

/**
 * Parsed full schema configuration
 */
export interface ParsedSchemaConfig {
  collections: Map<string, ParsedCollectionSchema>;
  /** Errors collected when using collectAllErrors option */
  errors?: ParseError[];
}

/**
 * Options for parsing schema configuration
 */
export interface ParseOptions {
  /** Collect all errors instead of throwing on first error */
  collectAllErrors?: boolean;
}

// ============================================================================
// Validation Constants and Type Validator
// ============================================================================

/**
 * Valid Parquet types for column definitions (Set for O(1) lookup)
 */
const VALID_PARQUET_TYPES_SET = new Set<string>([
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
]);

/**
 * Valid Parquet types as array for error messages
 */
const VALID_PARQUET_TYPES_LIST: readonly ParquetType[] = [
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
 * Type suggestions for common mistakes (readonly for safety)
 */
const TYPE_SUGGESTIONS: Readonly<Record<string, string>> = {
  number: 'Did you mean int32, int64, float, or double?',
  integer: 'Did you mean int32 or int64?',
  int: 'Did you mean int32 or int64?',
  bool: 'Did you mean boolean?',
  datetime: 'Did you mean timestamp?',
  time: 'Did you mean timestamp?',
  text: 'Did you mean string?',
  str: 'Did you mean string?',
  bytes: 'Did you mean binary?',
  blob: 'Did you mean binary?',
};

/**
 * Maximum nesting depth for field paths
 */
const MAX_NESTING_DEPTH = 10;

/**
 * Maximum number of columns per collection
 */
const MAX_COLUMNS_PER_COLLECTION = 100;

/**
 * Pattern for valid field path segments (compiled once for reuse)
 */
const VALID_FIELD_SEGMENT_PATTERN = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

// ============================================================================
// Type Validation Module
// ============================================================================

/**
 * Centralized type validation utilities
 */
const TypeValidator = {
  /**
   * Check if a value is a valid Parquet type
   */
  isValidParquetType(value: unknown): value is ParquetType {
    return typeof value === 'string' && VALID_PARQUET_TYPES_SET.has(value);
  },

  /**
   * Get suggestion for an invalid type
   */
  getSuggestion(invalidType: string): string | undefined {
    return TYPE_SUGGESTIONS[invalidType.toLowerCase()];
  },

  /**
   * Get formatted list of valid types for error messages
   */
  getValidTypesList(): string {
    return VALID_PARQUET_TYPES_LIST.join(', ');
  },

  /**
   * Create error for invalid Parquet type
   */
  createInvalidTypeError(invalidType: string, fieldPath: string, schemaPath: string): SchemaConfigError {
    const suggestion = this.getSuggestion(invalidType);
    return new SchemaConfigError(
      `Invalid Parquet type '${invalidType}' for field '${fieldPath}'. Valid types are: ${this.getValidTypesList()}`,
      { path: schemaPath, suggestion }
    );
  },

  /**
   * Create error for invalid array element type
   */
  createInvalidArrayTypeError(invalidType: string, fieldPath: string, schemaPath: string): SchemaConfigError {
    const suggestion = this.getSuggestion(invalidType);
    return new SchemaConfigError(
      `Invalid Parquet array element type '${invalidType}' for field '${fieldPath}'. Valid types are: ${this.getValidTypesList()}`,
      { path: schemaPath, suggestion }
    );
  },
} as const;

// ============================================================================
// Field Path Validation Module
// ============================================================================

/**
 * Field path validation utilities
 */
const FieldPathValidator = {
  /**
   * Validate a single path segment
   */
  validateSegment(segment: string, fullPath: string): void {
    if (!segment) {
      throw new SchemaConfigError(
        `Invalid field path '${fullPath}': empty segment found`,
        { path: fullPath }
      );
    }

    if (!VALID_FIELD_SEGMENT_PATTERN.test(segment)) {
      throw new SchemaConfigError(
        `Invalid field path '${fullPath}': segment '${segment}' contains invalid characters. ` +
          'Segments must start with a letter or underscore and contain only letters, numbers, and underscores.',
        { path: fullPath }
      );
    }
  },

  /**
   * Validate and parse a field path
   * @returns Array of path segments
   */
  validate(path: string): string[] {
    if (!path || typeof path !== 'string') {
      throw new SchemaConfigError('Field path must be a non-empty string', { path });
    }

    const segments = path.split('.');

    if (segments.length > MAX_NESTING_DEPTH) {
      throw new SchemaConfigError(
        `Field path '${path}' exceeds maximum nesting depth of ${MAX_NESTING_DEPTH}`,
        { path }
      );
    }

    for (const segment of segments) {
      this.validateSegment(segment, path);
    }

    return segments;
  },
} as const;

// ============================================================================
// Auto-Promote Validation Module
// ============================================================================

/**
 * Auto-promote configuration validation utilities
 */
const AutoPromoteValidator = {
  /**
   * Validate auto-promote configuration
   */
  validate(autoPromote: unknown, path: string): { threshold: number } {
    if (typeof autoPromote !== 'object' || autoPromote === null) {
      throw new SchemaConfigError('autoPromote must be an object', { path });
    }

    const config = autoPromote as Record<string, unknown>;

    if (!('threshold' in config)) {
      throw new SchemaConfigError('autoPromote must have a threshold property', { path });
    }

    const { threshold } = config;

    if (typeof threshold !== 'number') {
      throw new SchemaConfigError('autoPromote.threshold must be a number', {
        path: `${path}.threshold`,
      });
    }

    if (threshold < 0 || threshold > 1) {
      throw new SchemaConfigError(
        `autoPromote.threshold must be between 0 and 1, got ${threshold}`,
        { path: `${path}.threshold` }
      );
    }

    return { threshold };
  },
} as const;

// ============================================================================
// Default Value Validation Module
// ============================================================================

/**
 * Default value validation utilities
 */
const DefaultValueValidator = {
  /**
   * Validate default value matches the column type
   */
  validate(defaultValue: unknown, type: ParquetType, path: string): void {
    if (defaultValue === undefined || defaultValue === null) {
      return;
    }

    const actualType = typeof defaultValue;

    switch (type) {
      case 'string':
        if (actualType !== 'string') {
          this.throwTypeMismatch(path, 'string', actualType);
        }
        break;

      case 'int32':
      case 'int64':
        if (actualType !== 'number' || !Number.isInteger(defaultValue)) {
          this.throwTypeMismatch(path, type, actualType);
        }
        break;

      case 'float':
      case 'double':
        if (actualType !== 'number') {
          this.throwTypeMismatch(path, type, actualType);
        }
        break;

      case 'boolean':
        if (actualType !== 'boolean') {
          this.throwTypeMismatch(path, 'boolean', actualType);
        }
        break;
    }
  },

  throwTypeMismatch(path: string, expected: string, actual: string): never {
    throw new SchemaConfigError(
      `Invalid default value for field '${path}': expected ${expected}, got ${actual}`,
      { path: `${path}.default` }
    );
  },
} as const;

// ============================================================================
// Line Number Tracking Utilities
// ============================================================================

/**
 * Find line and column number for a path in JSON source
 */
function findLocationInJSON(
  source: string,
  path: string,
  targetValue?: string
): { lineNumber: number; columnNumber: number; sourceContext: string } | undefined {
  const pathParts = path.split('.');
  let searchPattern = '';

  // Build a search pattern to find the location
  if (targetValue) {
    // Search for the value itself
    searchPattern = targetValue;
  } else {
    // Search for the last key in the path
    const lastKey = pathParts[pathParts.length - 1];
    searchPattern = `"${lastKey}"`;
  }

  const lines = source.split('\n');

  for (let i = 0; i < lines.length; i++) {
    const col = lines[i]!.indexOf(searchPattern);
    if (col !== -1) {
      // Verify this is likely the right location by checking context
      const contextStart = Math.max(0, i - 1);
      const contextEnd = Math.min(lines.length - 1, i + 1);
      const context = lines.slice(contextStart, contextEnd + 1).join('\n');

      // Check if higher-level keys are present in surrounding context
      let isMatch = true;
      for (const part of pathParts.slice(0, -1)) {
        const checkLines = lines.slice(Math.max(0, i - 10), i + 1).join('\n');
        if (!checkLines.includes(`"${part}"`)) {
          isMatch = false;
          break;
        }
      }

      if (isMatch) {
        return {
          lineNumber: i + 1,
          columnNumber: col + 1,
          sourceContext: context,
        };
      }
    }
  }

  return undefined;
}

/**
 * Find line number for a YAML parse error
 */
function findLocationInYAML(
  source: string,
  path: string,
  targetValue?: string
): { lineNumber: number; columnNumber: number; sourceContext: string } | undefined {
  const pathParts = path.split('.');
  const lines = source.split('\n');

  // For YAML, search for the key or value
  const lastKey = pathParts[pathParts.length - 1];
  const searchPatterns = [
    `${lastKey}:`,
    targetValue ? `: ${targetValue}` : null,
    targetValue,
  ].filter(Boolean) as string[];

  for (let i = 0; i < lines.length; i++) {
    for (const pattern of searchPatterns) {
      const col = lines[i]!.indexOf(pattern);
      if (col !== -1) {
        const contextStart = Math.max(0, i - 1);
        const contextEnd = Math.min(lines.length - 1, i + 1);
        const context = lines.slice(contextStart, contextEnd + 1).join('\n');

        return {
          lineNumber: i + 1,
          columnNumber: col + 1,
          sourceContext: context,
        };
      }
    }
  }

  // Fallback: return first non-empty line
  for (let i = 0; i < lines.length; i++) {
    if (lines[i]!.trim()) {
      return {
        lineNumber: i + 1,
        columnNumber: 1,
        sourceContext: lines[i]!,
      };
    }
  }

  return undefined;
}

// ============================================================================
// Column Definition Helpers
// ============================================================================

/**
 * Check if a column definition is an extended definition
 */
function isExtendedColumnDef(def: unknown): def is ExtendedColumnDef {
  if (typeof def !== 'object' || def === null || Array.isArray(def)) {
    return false;
  }
  const obj = def as Record<string, unknown>;
  return 'type' in obj && TypeValidator.isValidParquetType(obj.type);
}

/**
 * Parse a column definition
 *
 * @param path - Field path
 * @param def - Column definition (type string, array, struct, or extended)
 * @param currentPath - Current schema path for error reporting
 * @returns Parsed column information
 * @throws {SchemaConfigError} If definition is invalid
 */
function parseColumnDef(
  path: string,
  def: ColumnDefinition,
  currentPath: string
): ParsedColumn {
  const segments = FieldPathValidator.validate(path);
  const fullPath = `${currentPath}.${path}`;

  // Extended definition: { type: 'string', nullable: true, default: 'value' }
  if (isExtendedColumnDef(def)) {
    const { type, nullable, default: defaultValue, coerce } = def;

    // Validate default value matches type
    if (defaultValue !== undefined) {
      DefaultValueValidator.validate(defaultValue, type, fullPath);
    }

    // Build coercion rules
    let coercionRules: CoercionRules | undefined;
    if (coerce) {
      coercionRules = {
        from: coerce.from,
        strict: coerce.strict,
      };
    }

    return {
      path,
      segments,
      type,
      isArray: false,
      isStruct: false,
      nullable,
      defaultValue,
      coercionRules,
    };
  }

  // Simple type: 'string', 'int32', etc.
  if (typeof def === 'string') {
    if (!TypeValidator.isValidParquetType(def)) {
      throw TypeValidator.createInvalidTypeError(def, path, fullPath);
    }

    return {
      path,
      segments,
      type: def,
      isArray: false,
      isStruct: false,
    };
  }

  // Array type: ['string'], ['int32'], etc.
  if (Array.isArray(def)) {
    if (def.length !== 1) {
      throw new SchemaConfigError(
        `Array type definition for field '${path}' must have exactly one element type`,
        { path: fullPath }
      );
    }

    const elementType = def[0];

    if (typeof elementType === 'string') {
      if (!TypeValidator.isValidParquetType(elementType)) {
        throw TypeValidator.createInvalidArrayTypeError(elementType, path, fullPath);
      }

      return {
        path,
        segments,
        type: elementType,
        isArray: true,
        isStruct: false,
      };
    }

    // Array of structs
    if (typeof elementType === 'object' && elementType !== null && !Array.isArray(elementType)) {
      const structDef = parseStructDef(path, elementType as { [key: string]: ColumnDefinition }, fullPath);

      return {
        path,
        segments,
        type: 'variant', // Struct arrays are stored as variant for now
        isArray: true,
        isStruct: true,
        structDef,
      };
    }

    throw new SchemaConfigError(
      `Invalid array element type for field '${path}': must be a Parquet type string or struct object`,
      { path: fullPath }
    );
  }

  // Struct type: { field1: 'string', field2: 'int32' }
  if (typeof def === 'object' && def !== null) {
    const structDef = parseStructDef(path, def as { [key: string]: ColumnDefinition }, fullPath);

    return {
      path,
      segments,
      type: 'variant', // Nested structs stored as variant
      isArray: false,
      isStruct: true,
      structDef,
    };
  }

  throw new SchemaConfigError(
    `Invalid column definition for field '${path}': must be a Parquet type string, array, or struct object`,
    { path: fullPath }
  );
}

/**
 * Parse a struct definition
 *
 * @param _parentPath - Parent field path (unused but kept for API consistency)
 * @param structDef - Struct definition object
 * @param currentPath - Current schema path for error reporting
 * @returns Map of nested parsed columns
 */
function parseStructDef(
  _parentPath: string,
  structDef: { [key: string]: ColumnDefinition },
  currentPath: string
): { [key: string]: ParsedColumn } {
  const result: { [key: string]: ParsedColumn } = {};

  for (const [key, def] of Object.entries(structDef)) {
    result[key] = parseColumnDef(key, def, currentPath);
  }

  return result;
}

// ============================================================================
// Parsing Functions
// ============================================================================

/**
 * Context for parsing with error collection
 */
interface ParseContext {
  source?: string;
  sourceType?: 'json' | 'yaml';
  errors: ParseError[];
  collectAllErrors: boolean;
}

/**
 * Report an error in parse context
 */
function reportError(
  ctx: ParseContext,
  message: string,
  path: string,
  targetValue?: string
): void {
  let location:
    | { lineNumber: number; columnNumber: number; sourceContext: string }
    | undefined;

  if (ctx.source) {
    if (ctx.sourceType === 'json') {
      location = findLocationInJSON(ctx.source, path, targetValue);
    } else if (ctx.sourceType === 'yaml') {
      location = findLocationInYAML(ctx.source, path, targetValue);
    }
  }

  const suggestion = targetValue ? TypeValidator.getSuggestion(targetValue) : undefined;

  const error: ParseError = {
    message,
    path,
    lineNumber: location?.lineNumber,
    columnNumber: location?.columnNumber,
    sourceContext: location?.sourceContext,
    suggestion,
  };

  if (ctx.collectAllErrors) {
    ctx.errors.push(error);
  } else {
    throw new SchemaConfigError(message, {
      lineNumber: location?.lineNumber,
      columnNumber: location?.columnNumber,
      sourceContext: location?.sourceContext,
      path,
      suggestion,
    });
  }
}

/**
 * Parse a collection schema configuration
 *
 * @param name - Collection name (for error messages)
 * @param schema - Raw collection schema
 * @param ctx - Parse context
 * @param baseSchemas - Base schemas for inheritance
 * @returns Parsed collection schema
 * @throws {SchemaConfigError} If schema is invalid
 */
function parseCollectionSchema(
  name: string,
  schema: ExtendedCollectionSchema,
  ctx: ParseContext,
  baseSchemas?: { [name: string]: ExtendedCollectionSchema }
): ParsedCollectionSchema {
  const columns: ParsedColumn[] = [];
  const columnMap = new Map<string, ParsedColumn>();
  const currentPath = `collections.${name}`;

  // Handle schema inheritance
  let mergedColumns: { [key: string]: ColumnDefinition } = {};

  if (schema.extends) {
    if (!baseSchemas || !baseSchemas[schema.extends]) {
      reportError(
        ctx,
        `Schema '${name}' extends non-existent schema '${schema.extends}'`,
        `${currentPath}.extends`,
        schema.extends
      );
    } else {
      const baseSchema = baseSchemas[schema.extends]!;
      if (baseSchema.columns) {
        mergedColumns = { ...baseSchema.columns };
      }
    }
  }

  // Merge own columns (override inherited)
  if (schema.columns) {
    mergedColumns = { ...mergedColumns, ...schema.columns };
  }

  // Parse columns
  if (Object.keys(mergedColumns).length > 0) {
    const columnEntries = Object.entries(mergedColumns);

    if (columnEntries.length > MAX_COLUMNS_PER_COLLECTION) {
      reportError(
        ctx,
        `Collection '${name}': exceeds maximum of ${MAX_COLUMNS_PER_COLLECTION} columns`,
        currentPath
      );
    }

    for (const [path, def] of columnEntries) {
      try {
        const parsed = parseColumnDef(path, def, `${currentPath}.columns`);
        columns.push(parsed);
        columnMap.set(path, parsed);
      } catch (error) {
        if (error instanceof SchemaConfigError && ctx.collectAllErrors) {
          ctx.errors.push({
            message: error.message,
            path: error.path || `${currentPath}.columns.${path}`,
            lineNumber: error.lineNumber,
            columnNumber: error.columnNumber,
            sourceContext: error.sourceContext,
            suggestion: error.suggestion,
          });
        } else {
          throw error;
        }
      }
    }
  }

  // Validate requireIdColumn
  if (schema.requireIdColumn && !columnMap.has('_id')) {
    reportError(
      ctx,
      `Collection '${name}': _id column is required but not defined`,
      `${currentPath}.columns`
    );
  }

  // Validate requiredFields
  if (schema.requiredFields) {
    for (const fieldName of schema.requiredFields) {
      if (!columnMap.has(fieldName)) {
        reportError(
          ctx,
          `Collection '${name}': required field '${fieldName}' is not defined in columns`,
          `${currentPath}.requiredFields`
        );
      } else {
        const col = columnMap.get(fieldName)!;
        // Check if required field is nullable (not allowed)
        if (col.nullable) {
          reportError(
            ctx,
            `Collection '${name}': required field '${fieldName}' cannot be nullable`,
            `${currentPath}.columns.${fieldName}`
          );
        }
        // Mark as required
        col.required = true;
      }
    }
  }

  // Mark non-required fields
  for (const col of columns) {
    if (col.required === undefined) {
      col.required = false;
    }
  }

  // Parse auto-promote if present
  let autoPromote: { threshold: number } | undefined;
  if (schema.autoPromote !== undefined) {
    try {
      autoPromote = AutoPromoteValidator.validate(schema.autoPromote, `${currentPath}.autoPromote`);
    } catch (error) {
      if (error instanceof SchemaConfigError && ctx.collectAllErrors) {
        ctx.errors.push({
          message: error.message,
          path: error.path || `${currentPath}.autoPromote`,
        });
      } else {
        throw error;
      }
    }
  }

  // Default storeVariant to true
  const storeVariant = schema.storeVariant !== false;

  return {
    columns,
    columnMap,
    autoPromote,
    storeVariant,
    requiredFields: schema.requiredFields,
  };
}

/**
 * Parse a full schema configuration
 *
 * @param config - Raw schema configuration object
 * @param options - Parse options
 * @returns Parsed schema configuration
 * @throws {SchemaConfigError} If configuration is invalid
 */
export function parseSchemaConfig(
  config: FullSchemaConfig,
  options?: ParseOptions
): ParsedSchemaConfig {
  const ctx: ParseContext = {
    errors: [],
    collectAllErrors: options?.collectAllErrors ?? false,
  };

  if (!config || typeof config !== 'object') {
    throw new SchemaConfigError('Schema configuration must be an object');
  }

  if (!config.collections || typeof config.collections !== 'object') {
    throw new SchemaConfigError(
      'Schema configuration must have a collections object'
    );
  }

  const collections = new Map<string, ParsedCollectionSchema>();

  for (const [collectionName, collectionSchema] of Object.entries(config.collections)) {
    if (!collectionSchema || typeof collectionSchema !== 'object') {
      reportError(
        ctx,
        `Collection '${collectionName}': schema must be an object`,
        `collections.${collectionName}`
      );
      continue;
    }

    const parsed = parseCollectionSchema(
      collectionName,
      collectionSchema,
      ctx,
      config.schemas
    );
    collections.set(collectionName, parsed);
  }

  if (ctx.collectAllErrors && ctx.errors.length > 0) {
    return { collections, errors: ctx.errors };
  }

  return { collections };
}

/**
 * Parse a simplified schema configuration (SchemaConfig format from types.ts)
 *
 * @param config - Simplified schema configuration (collection -> schema)
 * @returns Parsed schema configuration
 * @throws {SchemaConfigError} If configuration is invalid
 */
export function parseSimpleSchemaConfig(config: SchemaConfig): ParsedSchemaConfig {
  if (!config || typeof config !== 'object') {
    throw new SchemaConfigError('Schema configuration must be an object');
  }

  const ctx: ParseContext = {
    errors: [],
    collectAllErrors: false,
  };

  const collections = new Map<string, ParsedCollectionSchema>();

  for (const [collectionName, collectionSchema] of Object.entries(config)) {
    if (!collectionSchema || typeof collectionSchema !== 'object') {
      throw new SchemaConfigError(
        `Collection '${collectionName}': schema must be an object`
      );
    }

    const parsed = parseCollectionSchema(collectionName, collectionSchema, ctx);
    collections.set(collectionName, parsed);
  }

  return { collections };
}

// ============================================================================
// Configuration Loading Functions
// ============================================================================

/**
 * Load and parse schema configuration from a JSON string with line number tracking
 *
 * @param json - JSON string containing schema configuration
 * @returns Parsed schema configuration
 * @throws {SchemaConfigError} If JSON is invalid or configuration is invalid
 */
export function parseSchemaConfigFromJSON(json: string): ParsedSchemaConfig {
  const ctx: ParseContext = {
    source: json,
    sourceType: 'json',
    errors: [],
    collectAllErrors: false,
  };

  let config: FullSchemaConfig;

  try {
    config = JSON.parse(json);
  } catch (error) {
    const jsonError = error as SyntaxError;
    // Try to extract line number from error message
    const match = jsonError.message.match(/position (\d+)/);
    let lineNumber: number | undefined;
    let columnNumber: number | undefined;
    let sourceContext: string | undefined;

    if (match) {
      const position = parseInt(match[1]!, 10);
      // Calculate line number from position
      const beforeError = json.substring(0, position);
      lineNumber = beforeError.split('\n').length;
      const lastNewline = beforeError.lastIndexOf('\n');
      columnNumber = position - lastNewline;

      // Get source context
      const lines = json.split('\n');
      const contextStart = Math.max(0, lineNumber - 2);
      const contextEnd = Math.min(lines.length - 1, lineNumber);
      sourceContext = lines.slice(contextStart, contextEnd + 1).join('\n');
    }

    throw new SchemaConfigError(
      `Failed to parse schema configuration JSON: ${jsonError.message}`,
      { lineNumber, columnNumber, sourceContext }
    );
  }

  // Validate structure with line number tracking
  if (!config || typeof config !== 'object') {
    const location = findLocationInJSON(json, '', undefined);
    throw new SchemaConfigError('Schema configuration must be an object', {
      lineNumber: location?.lineNumber ?? 1,
      columnNumber: location?.columnNumber,
      sourceContext: location?.sourceContext,
    });
  }

  if (!config.collections || typeof config.collections !== 'object') {
    const location = findLocationInJSON(json, 'collections', undefined);
    throw new SchemaConfigError(
      'Schema configuration must have a collections object',
      {
        lineNumber: location?.lineNumber ?? 1,
        columnNumber: location?.columnNumber,
        sourceContext: location?.sourceContext,
        path: 'collections',
      }
    );
  }

  // Parse with line number context
  const collections = new Map<string, ParsedCollectionSchema>();

  for (const [collectionName, collectionSchema] of Object.entries(config.collections)) {
    if (!collectionSchema || typeof collectionSchema !== 'object') {
      const location = findLocationInJSON(json, `collections.${collectionName}`, undefined);
      throw new SchemaConfigError(
        `Collection '${collectionName}': schema must be an object`,
        {
          lineNumber: location?.lineNumber,
          columnNumber: location?.columnNumber,
          sourceContext: location?.sourceContext,
          path: `collections.${collectionName}`,
        }
      );
    }

    try {
      const parsed = parseCollectionSchema(collectionName, collectionSchema, ctx, config.schemas);
      collections.set(collectionName, parsed);
    } catch (error) {
      if (error instanceof SchemaConfigError) {
        // Add line number information if not already present
        if (!error.lineNumber && error.path) {
          const targetValue =
            error.message.match(/Invalid Parquet type '(\w+)'/) ||
            error.message.match(/Invalid .+ '(\w+)'/);
          const location = findLocationInJSON(
            json,
            error.path,
            targetValue ? targetValue[1] : undefined
          );
          if (location) {
            throw error.withLocation(location);
          }
        }
      }
      throw error;
    }
  }

  return { collections };
}

/**
 * Load and parse schema configuration from a YAML string
 *
 * @param yamlString - YAML string containing schema configuration
 * @returns Parsed schema configuration
 * @throws {SchemaConfigError} If YAML is invalid or configuration is invalid
 */
export function parseSchemaConfigFromYAML(yamlString: string): ParsedSchemaConfig {
  const ctx: ParseContext = {
    source: yamlString,
    sourceType: 'yaml',
    errors: [],
    collectAllErrors: false,
  };

  let config: FullSchemaConfig;

  try {
    // Enable merge key support for YAML anchors/aliases (<<: *alias)
    config = yaml.parse(yamlString, { merge: true });
  } catch (error) {
    const yamlError = error as yaml.YAMLParseError;
    throw new SchemaConfigError(
      `Failed to parse YAML schema configuration: ${yamlError.message}`,
      {
        lineNumber: yamlError.linePos?.[0]?.line,
        columnNumber: yamlError.linePos?.[0]?.col,
      }
    );
  }

  // Validate structure with line number tracking
  if (!config || typeof config !== 'object') {
    const location = findLocationInYAML(yamlString, '', undefined);
    throw new SchemaConfigError('Schema configuration must be an object', {
      lineNumber: location?.lineNumber ?? 1,
      columnNumber: location?.columnNumber,
      sourceContext: location?.sourceContext,
    });
  }

  if (!config.collections || typeof config.collections !== 'object') {
    const location = findLocationInYAML(yamlString, 'collections', undefined);
    // Find the first non-empty line to report
    const lines = yamlString.split('\n');
    let firstContentLine = 1;
    for (let i = 0; i < lines.length; i++) {
      if (lines[i]!.trim() && !lines[i]!.trim().startsWith('#')) {
        firstContentLine = i + 1;
        break;
      }
    }

    throw new SchemaConfigError(
      'Schema configuration must have a collections object',
      {
        lineNumber: location?.lineNumber ?? firstContentLine,
        columnNumber: location?.columnNumber,
        sourceContext: location?.sourceContext,
        path: 'collections',
      }
    );
  }

  // Parse with line number context
  const collections = new Map<string, ParsedCollectionSchema>();

  for (const [collectionName, collectionSchema] of Object.entries(config.collections)) {
    if (!collectionSchema || typeof collectionSchema !== 'object') {
      const location = findLocationInYAML(yamlString, `collections.${collectionName}`, undefined);
      throw new SchemaConfigError(
        `Collection '${collectionName}': schema must be an object`,
        {
          lineNumber: location?.lineNumber,
          columnNumber: location?.columnNumber,
          sourceContext: location?.sourceContext,
          path: `collections.${collectionName}`,
        }
      );
    }

    try {
      const parsed = parseCollectionSchema(collectionName, collectionSchema, ctx, config.schemas);
      collections.set(collectionName, parsed);
    } catch (error) {
      if (error instanceof SchemaConfigError) {
        // Add line number information if not already present
        if (!error.lineNumber && error.path) {
          const targetValue =
            error.message.match(/Invalid Parquet type '(\w+)'/) ||
            error.message.match(/Invalid .+ '(\w+)'/);
          const location = findLocationInYAML(
            yamlString,
            error.path,
            targetValue ? targetValue[1] : undefined
          );
          if (location) {
            throw error.withLocation(location);
          }
        }
      }
      throw error;
    }
  }

  return { collections };
}

/**
 * Load schema configuration from a file path
 *
 * @param filePath - Path to the schema configuration file
 * @param options - Optional format override
 * @returns Parsed schema configuration
 */
export async function loadSchemaConfigFromFile(
  filePath: string,
  options?: { format?: 'json' | 'yaml' }
): Promise<ParsedSchemaConfig> {
  // Determine format from extension if not specified
  let format = options?.format;
  if (!format) {
    if (filePath.endsWith('.json')) {
      format = 'json';
    } else if (filePath.endsWith('.yaml') || filePath.endsWith('.yml')) {
      format = 'yaml';
    } else {
      throw new SchemaConfigError(
        `Cannot determine format for file '${filePath}'. Use .json, .yaml, or .yml extension, or specify format explicitly.`
      );
    }
  }

  // Read file content
  // Note: This uses Node.js fs for file reading
  const fs = await import('fs/promises');
  const content = await fs.readFile(filePath, 'utf-8');

  // Parse based on format
  if (format === 'json') {
    return parseSchemaConfigFromJSON(content);
  } else {
    return parseSchemaConfigFromYAML(content);
  }
}

// ============================================================================
// Schema Access Utilities
// ============================================================================

/**
 * Get the column definition for a field path in a collection
 *
 * @param schema - Parsed collection schema
 * @param path - Field path (e.g., 'profile.name')
 * @returns Parsed column or undefined if not found
 */
export function getColumnForPath(
  schema: ParsedCollectionSchema,
  path: string
): ParsedColumn | undefined {
  return schema.columnMap.get(path);
}

/**
 * Check if a field should be promoted to a Parquet column
 *
 * @param schema - Parsed collection schema
 * @param path - Field path to check
 * @returns True if field is explicitly defined as a promoted column
 */
export function isPromotedField(
  schema: ParsedCollectionSchema,
  path: string
): boolean {
  return schema.columnMap.has(path);
}

/**
 * Get all promoted column paths for a collection
 *
 * @param schema - Parsed collection schema
 * @returns Array of field paths
 */
export function getPromotedFieldPaths(schema: ParsedCollectionSchema): string[] {
  return schema.columns.map((col) => col.path);
}

/**
 * Extract promoted field values from a document
 *
 * @param schema - Parsed collection schema
 * @param document - Document to extract values from
 * @returns Map of field path to value
 */
export function extractPromotedFields(
  schema: ParsedCollectionSchema,
  document: Record<string, unknown>
): Map<string, unknown> {
  const result = new Map<string, unknown>();

  for (const column of schema.columns) {
    const value = getNestedValue(document, column.path);
    if (value !== undefined) {
      result.set(column.path, value);
    }
  }

  return result;
}

/**
 * Validate that a document's promoted field values match expected types
 *
 * @param schema - Parsed collection schema
 * @param document - Document to validate
 * @returns Array of validation errors (empty if valid)
 */
export function validatePromotedFieldTypes(
  schema: ParsedCollectionSchema,
  document: Record<string, unknown>
): string[] {
  const errors: string[] = [];

  for (const column of schema.columns) {
    const value = getNestedValue(document, column.path);

    // Skip undefined/null values (they're optional)
    if (value === undefined || value === null) {
      continue;
    }

    const typeError = validateValueType(value, column);
    if (typeError) {
      errors.push(`Field '${column.path}': ${typeError}`);
    }
  }

  return errors;
}

/**
 * Validate that a value matches the expected Parquet type
 *
 * @param value - Value to validate
 * @param column - Column definition with expected type
 * @returns Error message if invalid, undefined if valid
 */
function validateValueType(value: unknown, column: ParsedColumn): string | undefined {
  const { type, isArray } = column;

  // Handle arrays
  if (isArray) {
    if (!Array.isArray(value)) {
      return `expected array, got ${typeof value}`;
    }
    // Validate each element
    for (let i = 0; i < value.length; i++) {
      const elementError = validateScalarType(value[i], type);
      if (elementError) {
        return `element [${i}]: ${elementError}`;
      }
    }
    return undefined;
  }

  // Handle scalars
  return validateScalarType(value, type);
}

/**
 * Validate a scalar value against a Parquet type
 *
 * @param value - Value to validate
 * @param type - Expected Parquet type
 * @returns Error message if invalid, undefined if valid
 */
function validateScalarType(value: unknown, type: ParquetType): string | undefined {
  switch (type) {
    case 'string':
      if (typeof value !== 'string') {
        return `expected string, got ${typeof value}`;
      }
      break;

    case 'int32':
    case 'int64':
      if (typeof value !== 'number' || !Number.isInteger(value)) {
        return `expected integer, got ${typeof value}`;
      }
      break;

    case 'float':
    case 'double':
      if (typeof value !== 'number') {
        return `expected number, got ${typeof value}`;
      }
      break;

    case 'boolean':
      if (typeof value !== 'boolean') {
        return `expected boolean, got ${typeof value}`;
      }
      break;

    case 'timestamp':
    case 'date':
      if (!(value instanceof Date) && typeof value !== 'string' && typeof value !== 'number') {
        return `expected Date, string, or number, got ${typeof value}`;
      }
      break;

    case 'binary':
      if (!(value instanceof Uint8Array)) {
        return `expected Uint8Array, got ${typeof value}`;
      }
      break;

    case 'variant':
      // Variant accepts any value
      break;
  }

  return undefined;
}

// ============================================================================
// Schema Cache
// ============================================================================

/**
 * Cache entry with parsed schema and metadata
 */
interface CacheEntry {
  schema: ParsedSchemaConfig;
  timestamp: number;
  hash: string;
}

/**
 * Options for schema cache
 */
export interface SchemaCacheOptions {
  /** Maximum number of entries to cache */
  maxEntries?: number;
  /** Time-to-live in milliseconds (0 = no expiry) */
  ttlMs?: number;
}

/**
 * Simple string hash function for cache keys
 */
function hashString(str: string): string {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32-bit integer
  }
  return hash.toString(36);
}

/**
 * Schema cache for optimizing repeated parsing operations
 *
 * Caches parsed schemas to avoid re-parsing the same configuration multiple times.
 * Supports TTL-based expiration and LRU eviction.
 *
 * @example
 * ```typescript
 * const cache = new SchemaCache({ maxEntries: 100, ttlMs: 60000 });
 *
 * // First call parses the schema
 * const schema1 = cache.getOrParse(jsonString, parseSchemaConfigFromJSON);
 *
 * // Second call returns cached result
 * const schema2 = cache.getOrParse(jsonString, parseSchemaConfigFromJSON);
 * ```
 */
export class SchemaCache {
  private readonly cache = new Map<string, CacheEntry>();
  private readonly maxEntries: number;
  private readonly ttlMs: number;

  constructor(options: SchemaCacheOptions = {}) {
    this.maxEntries = options.maxEntries ?? 100;
    this.ttlMs = options.ttlMs ?? 0;
  }

  /**
   * Get a cached schema or parse and cache a new one
   *
   * @param source - Source string (JSON or YAML)
   * @param parser - Parser function to use if not cached
   * @returns Parsed schema configuration
   */
  getOrParse(
    source: string,
    parser: (source: string) => ParsedSchemaConfig
  ): ParsedSchemaConfig {
    const hash = hashString(source);
    const cached = this.cache.get(hash);

    if (cached) {
      // Check if entry is expired
      if (this.ttlMs > 0 && Date.now() - cached.timestamp > this.ttlMs) {
        this.cache.delete(hash);
      } else {
        // Move to end for LRU ordering
        this.cache.delete(hash);
        this.cache.set(hash, cached);
        return cached.schema;
      }
    }

    // Parse and cache
    const schema = parser(source);
    this.set(hash, schema);
    return schema;
  }

  /**
   * Store a parsed schema in the cache
   */
  private set(hash: string, schema: ParsedSchemaConfig): void {
    // Evict oldest entries if at capacity
    while (this.cache.size >= this.maxEntries) {
      const firstKey = this.cache.keys().next().value;
      if (firstKey !== undefined) {
        this.cache.delete(firstKey);
      }
    }

    this.cache.set(hash, {
      schema,
      timestamp: Date.now(),
      hash,
    });
  }

  /**
   * Clear all cached entries
   */
  clear(): void {
    this.cache.clear();
  }

  /**
   * Get the number of cached entries
   */
  get size(): number {
    return this.cache.size;
  }

  /**
   * Check if a source string is cached
   */
  has(source: string): boolean {
    const hash = hashString(source);
    const cached = this.cache.get(hash);

    if (!cached) {
      return false;
    }

    // Check expiration
    if (this.ttlMs > 0 && Date.now() - cached.timestamp > this.ttlMs) {
      this.cache.delete(hash);
      return false;
    }

    return true;
  }

  /**
   * Remove expired entries from the cache
   */
  prune(): number {
    if (this.ttlMs === 0) {
      return 0;
    }

    let removed = 0;
    const now = Date.now();

    for (const [hash, entry] of this.cache) {
      if (now - entry.timestamp > this.ttlMs) {
        this.cache.delete(hash);
        removed++;
      }
    }

    return removed;
  }
}

/**
 * Global schema cache instance for convenience
 */
let globalSchemaCache: SchemaCache | null = null;

/**
 * Get or create the global schema cache
 *
 * @param options - Cache options (only used on first call)
 * @returns Global schema cache instance
 */
export function getGlobalSchemaCache(options?: SchemaCacheOptions): SchemaCache {
  if (!globalSchemaCache) {
    globalSchemaCache = new SchemaCache(options);
  }
  return globalSchemaCache;
}

/**
 * Parse schema config with caching (uses global cache)
 *
 * @param source - JSON or YAML source string
 * @param format - Source format
 * @returns Parsed schema configuration
 */
export function parseSchemaConfigCached(
  source: string,
  format: 'json' | 'yaml'
): ParsedSchemaConfig {
  const cache = getGlobalSchemaCache();
  const parser = format === 'json' ? parseSchemaConfigFromJSON : parseSchemaConfigFromYAML;
  return cache.getOrParse(source, parser);
}
