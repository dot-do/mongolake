/**
 * Comprehensive Input Validation for MongoLake Public APIs
 *
 * This module provides validation functions for all user inputs to ensure
 * data integrity, security, and adherence to MongoDB naming conventions.
 *
 * @module validation
 */

import {
  ValidationError as BaseValidationError,
  ErrorCodes,
} from '../errors/index.js';

// ============================================================================
// ValidationError Class (re-exported from errors module for backwards compatibility)
// ============================================================================

/**
 * Custom error class for validation failures
 *
 * Provides clear, actionable error messages for invalid inputs.
 * Includes the field name and expected format when applicable.
 *
 * Note: This class extends the base ValidationError from the errors module
 * and provides a compatible API for existing code.
 */
export class ValidationError extends BaseValidationError {
  /** Additional context about the validation failure */
  public readonly context?: Record<string, unknown>;

  constructor(
    message: string,
    validationType: string = 'unknown',
    options?: {
      invalidValue?: unknown;
      context?: Record<string, unknown>;
    }
  ) {
    super(message, ErrorCodes.VALIDATION_FAILED, {
      validationType,
      invalidValue: options?.invalidValue,
      ...options?.context,
    });
    this.context = options?.context;
  }

  /**
   * Create a JSON-serializable representation of the error
   */
  override toJSON(): Record<string, unknown> {
    return {
      ...super.toJSON(),
      context: this.context,
    };
  }
}

// ============================================================================
// Constants
// ============================================================================

/** Maximum length for database and collection names */
const MAX_NAME_LENGTH = 120;

/** Regex pattern for valid name characters: letters, numbers, underscores, hyphens */
const VALID_CHARS_PATTERN = /^[a-zA-Z0-9_-]+$/;

/** MongoDB reserved database names */
const RESERVED_DATABASE_NAMES = new Set([
  'admin',
  'local',
  'config',
]);

/** MongoDB system collection prefix */
const SYSTEM_COLLECTION_PREFIX = 'system.';

// Query operators that accept values
const COMPARISON_OPERATORS = new Set([
  '$eq',
  '$ne',
  '$gt',
  '$gte',
  '$lt',
  '$lte',
]);

// Query operators that accept arrays
const ARRAY_QUERY_OPERATORS = new Set(['$in', '$nin', '$all']);

// Logical operators
const LOGICAL_OPERATORS = new Set(['$and', '$or', '$nor', '$not']);

// Element operators
const ELEMENT_OPERATORS = new Set(['$exists', '$type']);

// Evaluation operators
const EVALUATION_OPERATORS = new Set([
  '$regex',
  '$expr',
  '$mod',
  '$text',
  '$where',
  '$jsonSchema',
]);

// Array query operators
const ARRAY_OPERATORS = new Set(['$elemMatch', '$size']);

// All valid query operators
const VALID_QUERY_OPERATORS = new Set([
  ...COMPARISON_OPERATORS,
  ...ARRAY_QUERY_OPERATORS,
  ...LOGICAL_OPERATORS,
  ...ELEMENT_OPERATORS,
  ...EVALUATION_OPERATORS,
  ...ARRAY_OPERATORS,
]);

// Update operators
const FIELD_UPDATE_OPERATORS = new Set([
  '$set',
  '$unset',
  '$setOnInsert',
  '$rename',
  '$inc',
  '$mul',
  '$min',
  '$max',
  '$currentDate',
]);

const ARRAY_UPDATE_OPERATORS = new Set([
  '$push',
  '$pull',
  '$pop',
  '$addToSet',
  '$pullAll',
]);

const ARRAY_MODIFIER_OPERATORS = new Set([
  '$each',
  '$position',
  '$slice',
  '$sort',
]);

// All valid update operators
const VALID_UPDATE_OPERATORS = new Set([
  ...FIELD_UPDATE_OPERATORS,
  ...ARRAY_UPDATE_OPERATORS,
]);

// ============================================================================
// Name Validation Functions
// ============================================================================

/**
 * Validate a database name for MongoDB compatibility
 *
 * MongoDB database naming rules:
 * - Non-empty string
 * - Cannot contain: /, \, ., ", *, <, >, :, |, ?, $, space, null character
 * - Cannot start with underscore or hyphen
 * - Maximum 120 characters (MongoLake limit)
 *
 * @param name - The database name to validate
 * @throws {ValidationError} If the name is invalid
 *
 * @example
 * ```typescript
 * validateDatabaseName('myapp'); // OK
 * validateDatabaseName('my_database'); // OK
 * validateDatabaseName('../etc'); // throws ValidationError
 * ```
 */
export function validateDatabaseName(name: string): void {
  // Check for non-string or empty
  if (typeof name !== 'string' || name.length === 0) {
    throw new ValidationError(
      'database name cannot be empty',
      'database_name',
      { invalidValue: name }
    );
  }

  // Check max length
  if (name.length > MAX_NAME_LENGTH) {
    throw new ValidationError(
      `database name exceeds maximum length of ${MAX_NAME_LENGTH} characters`,
      'database_name',
      { invalidValue: name, context: { length: name.length, maxLength: MAX_NAME_LENGTH } }
    );
  }

  // Check for null bytes
  if (name.includes('\0')) {
    throw new ValidationError(
      'database name cannot contain null bytes',
      'database_name',
      { invalidValue: name }
    );
  }

  // Check for dots (prevent hidden files and path traversal like ..)
  if (name.includes('.')) {
    throw new ValidationError(
      'database name cannot contain dots',
      'database_name',
      { invalidValue: name }
    );
  }

  // Check for slashes (path traversal)
  if (name.includes('/') || name.includes('\\')) {
    throw new ValidationError(
      'database name cannot contain slashes',
      'database_name',
      { invalidValue: name }
    );
  }

  // Check that name doesn't start with underscore or hyphen
  if (name.startsWith('_') || name.startsWith('-')) {
    throw new ValidationError(
      'database name cannot start with underscore or hyphen',
      'database_name',
      { invalidValue: name }
    );
  }

  // Check for valid characters only
  if (!VALID_CHARS_PATTERN.test(name)) {
    throw new ValidationError(
      'database name contains invalid characters. Only letters, numbers, underscores, and hyphens are allowed',
      'database_name',
      { invalidValue: name }
    );
  }
}

/**
 * Validate a collection name for MongoDB compatibility
 *
 * MongoDB collection naming rules:
 * - Non-empty string
 * - Cannot contain $
 * - Cannot contain null character
 * - Cannot start with system. prefix
 * - Cannot be empty string
 * - Cannot contain dots, slashes (MongoLake restriction for path safety)
 *
 * @param name - The collection name to validate
 * @throws {ValidationError} If the name is invalid
 *
 * @example
 * ```typescript
 * validateCollectionName('users'); // OK
 * validateCollectionName('user_profiles'); // OK
 * validateCollectionName('system.users'); // throws ValidationError
 * validateCollectionName('$bad'); // throws ValidationError
 * ```
 */
export function validateCollectionName(name: string): void {
  // Check for non-string or empty
  if (typeof name !== 'string' || name.length === 0) {
    throw new ValidationError(
      'collection name cannot be empty',
      'collection_name',
      { invalidValue: name }
    );
  }

  // Check max length
  if (name.length > MAX_NAME_LENGTH) {
    throw new ValidationError(
      `collection name exceeds maximum length of ${MAX_NAME_LENGTH} characters`,
      'collection_name',
      { invalidValue: name, context: { length: name.length, maxLength: MAX_NAME_LENGTH } }
    );
  }

  // Check for null bytes
  if (name.includes('\0')) {
    throw new ValidationError(
      'collection name cannot contain null bytes',
      'collection_name',
      { invalidValue: name }
    );
  }

  // Check for $ character
  if (name.includes('$')) {
    throw new ValidationError(
      'collection name cannot contain the $ character',
      'collection_name',
      { invalidValue: name }
    );
  }

  // Check for system. prefix
  if (name.toLowerCase().startsWith(SYSTEM_COLLECTION_PREFIX)) {
    throw new ValidationError(
      'collection name cannot start with "system." prefix (reserved for internal use)',
      'collection_name',
      { invalidValue: name }
    );
  }

  // Check for dots (prevent hidden files and path traversal like ..)
  if (name.includes('.')) {
    throw new ValidationError(
      'collection name cannot contain dots',
      'collection_name',
      { invalidValue: name }
    );
  }

  // Check for slashes (path traversal)
  if (name.includes('/') || name.includes('\\')) {
    throw new ValidationError(
      'collection name cannot contain slashes',
      'collection_name',
      { invalidValue: name }
    );
  }

  // Check that name doesn't start with underscore or hyphen
  if (name.startsWith('_') || name.startsWith('-')) {
    throw new ValidationError(
      'collection name cannot start with underscore or hyphen',
      'collection_name',
      { invalidValue: name }
    );
  }

  // Check for valid characters only
  if (!VALID_CHARS_PATTERN.test(name)) {
    throw new ValidationError(
      'collection name contains invalid characters. Only letters, numbers, underscores, and hyphens are allowed',
      'collection_name',
      { invalidValue: name }
    );
  }
}

/**
 * Validate a field name for MongoDB compatibility
 *
 * MongoDB field naming rules:
 * - Cannot start with $ (except for operators)
 * - Cannot contain null characters
 * - Cannot be empty
 * - Top-level _id is allowed
 *
 * @param name - The field name to validate
 * @param allowOperators - If true, allow $ prefix for operators (default: false)
 * @throws {ValidationError} If the field name is invalid
 */
export function validateFieldName(name: string, allowOperators: boolean = false): void {
  if (typeof name !== 'string' || name.length === 0) {
    throw new ValidationError(
      'field name cannot be empty',
      'field_name',
      { invalidValue: name }
    );
  }

  // Check for null bytes
  if (name.includes('\0')) {
    throw new ValidationError(
      'field name cannot contain null bytes',
      'field_name',
      { invalidValue: name }
    );
  }

  // Check for $ prefix
  if (name.startsWith('$') && !allowOperators) {
    throw new ValidationError(
      'field name cannot start with $ character',
      'field_name',
      { invalidValue: name }
    );
  }
}

// ============================================================================
// Filter Validation Functions
// ============================================================================

/**
 * Configuration options for filter validation
 */
export interface FilterValidationOptions {
  /** Maximum depth for nested filters (default: 10) */
  maxDepth?: number;
  /** Maximum number of operators in a single filter (default: 100) */
  maxOperators?: number;
  /** Allow $where operator (security risk, default: false) */
  allowWhere?: boolean;
  /** Allow $expr operator (default: true) */
  allowExpr?: boolean;
}

/**
 * Validate a MongoDB query filter
 *
 * Validates that:
 * - All operators are valid MongoDB query operators
 * - Field names don't start with $ (except operators)
 * - Filter structure is valid (no circular refs, reasonable depth)
 * - Arrays for $in/$nin contain valid values
 *
 * @param filter - The filter object to validate
 * @param options - Validation options
 * @throws {ValidationError} If the filter is invalid
 *
 * @example
 * ```typescript
 * validateFilter({ name: 'Alice' }); // OK
 * validateFilter({ age: { $gt: 18 } }); // OK
 * validateFilter({ $and: [{ a: 1 }, { b: 2 }] }); // OK
 * validateFilter({ $badOp: 1 }); // throws ValidationError
 * ```
 */
export function validateFilter(
  filter: unknown,
  options: FilterValidationOptions = {}
): void {
  const {
    maxDepth = 10,
    maxOperators = 100,
    allowWhere = false,
    allowExpr = true,
  } = options;

  // Track operators for limit enforcement
  let operatorCount = 0;

  function validateFilterInternal(obj: unknown, depth: number, path: string): void {
    // Check depth limit
    if (depth > maxDepth) {
      throw new ValidationError(
        `filter exceeds maximum nesting depth of ${maxDepth}`,
        'filter_depth',
        { context: { path, depth, maxDepth } }
      );
    }

    // Null and primitives are valid filter values
    if (obj === null || obj === undefined) {
      return;
    }

    if (typeof obj !== 'object') {
      // Primitive values are valid
      return;
    }

    // Arrays need element validation
    if (Array.isArray(obj)) {
      for (let i = 0; i < obj.length; i++) {
        validateFilterInternal(obj[i], depth + 1, `${path}[${i}]`);
      }
      return;
    }

    // Object validation
    const record = obj as Record<string, unknown>;

    for (const [key, value] of Object.entries(record)) {
      // Check if key is an operator
      if (key.startsWith('$')) {
        operatorCount++;

        if (operatorCount > maxOperators) {
          throw new ValidationError(
            `filter exceeds maximum operator count of ${maxOperators}`,
            'filter_operators',
            { context: { operatorCount, maxOperators } }
          );
        }

        // Validate operator
        if (!VALID_QUERY_OPERATORS.has(key)) {
          throw new ValidationError(
            `invalid query operator: ${key}`,
            'filter_operator',
            { invalidValue: key }
          );
        }

        // Security: disallow $where by default
        if (key === '$where' && !allowWhere) {
          throw new ValidationError(
            '$where operator is not allowed for security reasons',
            'filter_security',
            { invalidValue: key }
          );
        }

        // Check $expr permission
        if (key === '$expr' && !allowExpr) {
          throw new ValidationError(
            '$expr operator is not allowed',
            'filter_security',
            { invalidValue: key }
          );
        }

        // Validate logical operator arrays
        if (LOGICAL_OPERATORS.has(key) && key !== '$not') {
          if (!Array.isArray(value)) {
            throw new ValidationError(
              `${key} operator requires an array`,
              'filter_structure',
              { invalidValue: key, context: { expectedType: 'array', actualType: typeof value } }
            );
          }
          for (let i = 0; i < value.length; i++) {
            validateFilterInternal(value[i], depth + 1, `${path}.${key}[${i}]`);
          }
          continue;
        }

        // Validate $in and $nin arrays
        if (ARRAY_QUERY_OPERATORS.has(key)) {
          if (!Array.isArray(value)) {
            throw new ValidationError(
              `${key} operator requires an array`,
              'filter_structure',
              { invalidValue: key, context: { expectedType: 'array', actualType: typeof value } }
            );
          }
          // Validate array elements are valid values (no nested objects with operators)
          for (let i = 0; i < value.length; i++) {
            const elem = value[i];
            if (typeof elem === 'object' && elem !== null && !Array.isArray(elem)) {
              // Objects in $in arrays shouldn't have operator keys
              for (const elemKey of Object.keys(elem as Record<string, unknown>)) {
                if (elemKey.startsWith('$')) {
                  throw new ValidationError(
                    `${key} array elements cannot contain operators`,
                    'filter_structure',
                    { invalidValue: elemKey, context: { path: `${path}.${key}[${i}]` } }
                  );
                }
              }
            }
          }
          continue;
        }

        // Validate nested conditions for $not, $elemMatch
        if (key === '$not' || key === '$elemMatch') {
          validateFilterInternal(value, depth + 1, `${path}.${key}`);
          continue;
        }

        // Other operators just need value validation
        validateFilterInternal(value, depth + 1, `${path}.${key}`);
      } else {
        // Regular field name
        validateFieldName(key);
        validateFilterInternal(value, depth + 1, `${path}.${key}`);
      }
    }
  }

  validateFilterInternal(filter, 0, 'filter');
}

// ============================================================================
// Projection Validation Functions
// ============================================================================

/**
 * Validate a MongoDB projection specification
 *
 * Validates that:
 * - Values are 0, 1, or valid projection operators
 * - No mixing of inclusion and exclusion (except _id)
 * - Field names are valid
 *
 * @param projection - The projection object to validate
 * @throws {ValidationError} If the projection is invalid
 *
 * @example
 * ```typescript
 * validateProjection({ name: 1, age: 1 }); // OK (inclusion)
 * validateProjection({ password: 0 }); // OK (exclusion)
 * validateProjection({ _id: 0, name: 1 }); // OK (_id is special)
 * validateProjection({ name: 1, age: 0 }); // throws ValidationError (mixed)
 * ```
 */
export function validateProjection(projection: unknown): void {
  if (projection === null || projection === undefined) {
    return; // No projection is valid
  }

  if (typeof projection !== 'object' || Array.isArray(projection)) {
    throw new ValidationError(
      'projection must be an object',
      'projection_type',
      { invalidValue: projection }
    );
  }

  const record = projection as Record<string, unknown>;
  let hasInclusion: boolean | null = null;

  for (const [key, value] of Object.entries(record)) {
    // Validate field name
    if (key !== '_id') {
      validateFieldName(key);
    }

    // Check value type
    if (typeof value === 'number') {
      if (value !== 0 && value !== 1) {
        throw new ValidationError(
          `projection value must be 0 or 1, got ${value}`,
          'projection_value',
          { invalidValue: value, context: { field: key } }
        );
      }

      // Track inclusion vs exclusion
      if (key !== '_id') {
        const isInclusion = value === 1;
        if (hasInclusion === null) {
          hasInclusion = isInclusion;
        } else if (hasInclusion !== isInclusion) {
          throw new ValidationError(
            'projection cannot mix inclusion and exclusion',
            'projection_mixed',
            { context: { field: key } }
          );
        }
      }
    } else if (typeof value === 'boolean') {
      // Boolean values are also valid (true = 1, false = 0)
      if (key !== '_id') {
        const isInclusion = value === true;
        if (hasInclusion === null) {
          hasInclusion = isInclusion;
        } else if (hasInclusion !== isInclusion) {
          throw new ValidationError(
            'projection cannot mix inclusion and exclusion',
            'projection_mixed',
            { context: { field: key } }
          );
        }
      }
    } else if (typeof value === 'object' && value !== null) {
      // Projection operators like $slice, $elemMatch, $meta
      const ops = value as Record<string, unknown>;
      const opKeys = Object.keys(ops);
      for (const opKey of opKeys) {
        if (!opKey.startsWith('$')) {
          throw new ValidationError(
            `invalid projection operator: ${opKey}`,
            'projection_operator',
            { invalidValue: opKey, context: { field: key } }
          );
        }
        const validProjectionOps = new Set(['$slice', '$elemMatch', '$meta']);
        if (!validProjectionOps.has(opKey)) {
          throw new ValidationError(
            `unsupported projection operator: ${opKey}`,
            'projection_operator',
            { invalidValue: opKey, context: { field: key } }
          );
        }
      }
    } else {
      throw new ValidationError(
        `invalid projection value type for field "${key}"`,
        'projection_value',
        { invalidValue: value, context: { field: key, type: typeof value } }
      );
    }
  }
}

// ============================================================================
// Update Validation Functions
// ============================================================================

/**
 * Configuration options for update validation
 */
export interface UpdateValidationOptions {
  /** Maximum depth for nested update values (default: 10) */
  maxDepth?: number;
  /** Allow $set with $ prefix fields (for array updates, default: true) */
  allowPositionalOperator?: boolean;
}

/**
 * Validate a MongoDB update specification
 *
 * Validates that:
 * - All top-level keys are valid update operators ($set, $unset, etc.)
 * - Field names within operators are valid
 * - Values are appropriate for each operator
 *
 * @param update - The update object to validate
 * @param options - Validation options
 * @throws {ValidationError} If the update is invalid
 *
 * @example
 * ```typescript
 * validateUpdate({ $set: { name: 'Bob' } }); // OK
 * validateUpdate({ $inc: { count: 1 } }); // OK
 * validateUpdate({ $push: { tags: 'new' } }); // OK
 * validateUpdate({ name: 'Bob' }); // throws ValidationError (no operator)
 * validateUpdate({ $badOp: { a: 1 } }); // throws ValidationError
 * ```
 */
export function validateUpdate(
  update: unknown,
  options: UpdateValidationOptions = {}
): void {
  const { maxDepth = 10, allowPositionalOperator = true } = options;

  if (update === null || update === undefined) {
    throw new ValidationError(
      'update cannot be null or undefined',
      'update_empty',
      { invalidValue: update }
    );
  }

  if (typeof update !== 'object' || Array.isArray(update)) {
    throw new ValidationError(
      'update must be an object',
      'update_type',
      { invalidValue: update }
    );
  }

  const record = update as Record<string, unknown>;
  const keys = Object.keys(record);

  if (keys.length === 0) {
    throw new ValidationError(
      'update cannot be an empty object',
      'update_empty',
      { invalidValue: update }
    );
  }

  // Check that all top-level keys are operators
  for (const key of keys) {
    if (!key.startsWith('$')) {
      throw new ValidationError(
        `update must use operators, found field "${key}" without operator`,
        'update_operator',
        { invalidValue: key, context: { hint: 'Use $set to set field values' } }
      );
    }

    if (!VALID_UPDATE_OPERATORS.has(key)) {
      throw new ValidationError(
        `invalid update operator: ${key}`,
        'update_operator',
        { invalidValue: key }
      );
    }

    const operatorValue = record[key];

    // Validate operator value is an object
    if (typeof operatorValue !== 'object' || operatorValue === null || Array.isArray(operatorValue)) {
      throw new ValidationError(
        `${key} operator value must be an object`,
        'update_value',
        { invalidValue: key, context: { expectedType: 'object', actualType: typeof operatorValue } }
      );
    }

    const fields = operatorValue as Record<string, unknown>;

    // Validate field names within the operator
    for (const [fieldName, fieldValue] of Object.entries(fields)) {
      // Allow positional operator $ in field names for array updates
      if (fieldName.includes('$') && !allowPositionalOperator) {
        throw new ValidationError(
          `field name cannot contain $: ${fieldName}`,
          'update_field',
          { invalidValue: fieldName }
        );
      }

      // Validate the field name (allow dots for nested updates)
      // Split by dots and validate each segment
      const segments = fieldName.split('.');
      for (const segment of segments) {
        if (segment.length === 0) {
          throw new ValidationError(
            `field name has empty segment: ${fieldName}`,
            'update_field',
            { invalidValue: fieldName }
          );
        }
        // Allow $ for positional operator
        if (segment !== '$' && segment.startsWith('$')) {
          throw new ValidationError(
            `field name segment cannot start with $: ${segment}`,
            'update_field',
            { invalidValue: fieldName }
          );
        }
      }

      // Validate value depth
      validateValueDepth(fieldValue, maxDepth, 0, `${key}.${fieldName}`);

      // Specific validations for certain operators
      if (key === '$inc' || key === '$mul') {
        if (typeof fieldValue !== 'number') {
          throw new ValidationError(
            `${key} requires numeric values`,
            'update_value',
            { invalidValue: fieldName, context: { operator: key, actualType: typeof fieldValue } }
          );
        }
      }

      if (key === '$pop') {
        if (fieldValue !== 1 && fieldValue !== -1) {
          throw new ValidationError(
            '$pop requires value of 1 or -1',
            'update_value',
            { invalidValue: fieldValue, context: { field: fieldName } }
          );
        }
      }

      if (key === '$push' || key === '$addToSet') {
        // Check for $each modifier
        if (typeof fieldValue === 'object' && fieldValue !== null && !Array.isArray(fieldValue)) {
          const pushMods = fieldValue as Record<string, unknown>;
          for (const modKey of Object.keys(pushMods)) {
            if (modKey.startsWith('$') && !ARRAY_MODIFIER_OPERATORS.has(modKey)) {
              throw new ValidationError(
                `invalid array modifier: ${modKey}`,
                'update_value',
                { invalidValue: modKey, context: { field: fieldName } }
              );
            }
          }
        }
      }

      if (key === '$rename') {
        if (typeof fieldValue !== 'string') {
          throw new ValidationError(
            '$rename requires string value (new field name)',
            'update_value',
            { invalidValue: fieldName, context: { actualType: typeof fieldValue } }
          );
        }
      }
    }
  }
}

/**
 * Validate that a value doesn't exceed maximum nesting depth
 */
function validateValueDepth(value: unknown, maxDepth: number, currentDepth: number, path: string): void {
  if (currentDepth > maxDepth) {
    throw new ValidationError(
      `value exceeds maximum nesting depth of ${maxDepth}`,
      'value_depth',
      { context: { path, depth: currentDepth, maxDepth } }
    );
  }

  if (value === null || value === undefined || typeof value !== 'object') {
    return;
  }

  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) {
      validateValueDepth(value[i], maxDepth, currentDepth + 1, `${path}[${i}]`);
    }
  } else {
    const record = value as Record<string, unknown>;
    for (const [key, val] of Object.entries(record)) {
      validateValueDepth(val, maxDepth, currentDepth + 1, `${path}.${key}`);
    }
  }
}

// ============================================================================
// Document Validation Functions
// ============================================================================

/**
 * Validate a document for insertion
 *
 * Validates that:
 * - Document is a non-null object
 * - Field names are valid (no $ prefix, no null chars)
 * - _id is present or will be auto-generated
 * - Document isn't too deeply nested
 *
 * @param document - The document to validate
 * @param options - Validation options
 * @throws {ValidationError} If the document is invalid
 */
export function validateDocument(
  document: unknown,
  options: { maxDepth?: number; requireId?: boolean } = {}
): void {
  const { maxDepth = 100, requireId = false } = options;

  if (document === null || document === undefined) {
    throw new ValidationError(
      'document cannot be null or undefined',
      'document_empty',
      { invalidValue: document }
    );
  }

  if (typeof document !== 'object' || Array.isArray(document)) {
    throw new ValidationError(
      'document must be an object',
      'document_type',
      { invalidValue: document }
    );
  }

  const record = document as Record<string, unknown>;

  if (requireId && !('_id' in record)) {
    throw new ValidationError(
      'document must have an _id field',
      'document_id',
      { invalidValue: document }
    );
  }

  // Validate all field names
  function validateDocumentFields(obj: Record<string, unknown>, depth: number, path: string): void {
    if (depth > maxDepth) {
      throw new ValidationError(
        `document exceeds maximum nesting depth of ${maxDepth}`,
        'document_depth',
        { context: { path, depth, maxDepth } }
      );
    }

    for (const [key, value] of Object.entries(obj)) {
      // _id is special
      if (key !== '_id') {
        validateFieldName(key);
      }

      // Recursively validate nested objects
      if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
        validateDocumentFields(value as Record<string, unknown>, depth + 1, `${path}.${key}`);
      } else if (Array.isArray(value)) {
        for (let i = 0; i < value.length; i++) {
          const elem = value[i];
          if (typeof elem === 'object' && elem !== null && !Array.isArray(elem)) {
            validateDocumentFields(elem as Record<string, unknown>, depth + 1, `${path}.${key}[${i}]`);
          }
        }
      }
    }
  }

  validateDocumentFields(record, 0, 'document');
}

// ============================================================================
// Aggregation Pipeline Validation
// ============================================================================

const VALID_AGGREGATION_STAGES = new Set([
  '$match',
  '$project',
  '$group',
  '$sort',
  '$limit',
  '$skip',
  '$unwind',
  '$lookup',
  '$addFields',
  '$set',
  '$unset',
  '$replaceRoot',
  '$replaceWith',
  '$count',
  '$facet',
  '$bucket',
  '$bucketAuto',
  '$sortByCount',
  '$sample',
  '$out',
  '$merge',
]);

/**
 * Validate an aggregation pipeline
 *
 * @param pipeline - The aggregation pipeline array
 * @throws {ValidationError} If the pipeline is invalid
 */
export function validateAggregationPipeline(
  pipeline: unknown,
  options: { maxStages?: number } = {}
): void {
  const { maxStages = 100 } = options;

  if (!Array.isArray(pipeline)) {
    throw new ValidationError(
      'aggregation pipeline must be an array',
      'pipeline_type',
      { invalidValue: pipeline }
    );
  }

  if (pipeline.length === 0) {
    throw new ValidationError(
      'aggregation pipeline cannot be empty',
      'pipeline_empty',
      { invalidValue: pipeline }
    );
  }

  if (pipeline.length > maxStages) {
    throw new ValidationError(
      `aggregation pipeline exceeds maximum of ${maxStages} stages`,
      'pipeline_length',
      { context: { length: pipeline.length, maxStages } }
    );
  }

  for (let i = 0; i < pipeline.length; i++) {
    const stage = pipeline[i];

    if (typeof stage !== 'object' || stage === null || Array.isArray(stage)) {
      throw new ValidationError(
        `pipeline stage at index ${i} must be an object`,
        'pipeline_stage',
        { invalidValue: stage, context: { index: i } }
      );
    }

    const stageRecord = stage as Record<string, unknown>;
    const keys = Object.keys(stageRecord);

    if (keys.length !== 1) {
      throw new ValidationError(
        `pipeline stage at index ${i} must have exactly one key`,
        'pipeline_stage',
        { context: { index: i, keys } }
      );
    }

    const stageKey = keys[0]!;

    if (!VALID_AGGREGATION_STAGES.has(stageKey)) {
      throw new ValidationError(
        `invalid aggregation stage: ${stageKey}`,
        'pipeline_stage',
        { invalidValue: stageKey, context: { index: i } }
      );
    }

    // Validate specific stages
    if (stageKey === '$match') {
      validateFilter(stageRecord[stageKey]);
    } else if (stageKey === '$project') {
      validateProjection(stageRecord[stageKey]);
    } else if (stageKey === '$limit' || stageKey === '$skip') {
      const value = stageRecord[stageKey];
      if (typeof value !== 'number' || value < 0 || !Number.isInteger(value)) {
        throw new ValidationError(
          `${stageKey} requires a non-negative integer`,
          'pipeline_stage',
          { invalidValue: value, context: { stage: stageKey, index: i } }
        );
      }
    } else if (stageKey === '$sort') {
      const sortSpec = stageRecord[stageKey];
      if (typeof sortSpec !== 'object' || sortSpec === null || Array.isArray(sortSpec)) {
        throw new ValidationError(
          '$sort requires an object',
          'pipeline_stage',
          { invalidValue: sortSpec, context: { index: i } }
        );
      }
      for (const [field, direction] of Object.entries(sortSpec as Record<string, unknown>)) {
        if (direction !== 1 && direction !== -1) {
          throw new ValidationError(
            `$sort direction must be 1 or -1, got ${direction}`,
            'pipeline_stage',
            { invalidValue: direction, context: { field, index: i } }
          );
        }
      }
    }
  }
}

// ============================================================================
// Convenience Functions
// ============================================================================

/**
 * Validate all common inputs in a single call
 *
 * @param inputs - Object containing inputs to validate
 * @throws {ValidationError} If any input is invalid
 */
export function validateInputs(inputs: {
  database?: string;
  collection?: string;
  filter?: unknown;
  projection?: unknown;
  update?: unknown;
  document?: unknown;
  pipeline?: unknown;
}): void {
  if (inputs.database !== undefined) {
    validateDatabaseName(inputs.database);
  }
  if (inputs.collection !== undefined) {
    validateCollectionName(inputs.collection);
  }
  if (inputs.filter !== undefined) {
    validateFilter(inputs.filter);
  }
  if (inputs.projection !== undefined) {
    validateProjection(inputs.projection);
  }
  if (inputs.update !== undefined) {
    validateUpdate(inputs.update);
  }
  if (inputs.document !== undefined) {
    validateDocument(inputs.document);
  }
  if (inputs.pipeline !== undefined) {
    validateAggregationPipeline(inputs.pipeline);
  }
}

// ============================================================================
// Exports
// ============================================================================

export {
  // Constants for external use
  VALID_QUERY_OPERATORS,
  VALID_UPDATE_OPERATORS,
  VALID_AGGREGATION_STAGES,
  MAX_NAME_LENGTH,
  RESERVED_DATABASE_NAMES,
  SYSTEM_COLLECTION_PREFIX,
};
