/**
 * Validation utilities for database and collection names
 *
 * Prevents path traversal attacks by validating names before
 * they are used to construct file paths.
 *
 * This module re-exports from the comprehensive validation module
 * for backward compatibility. For new code, prefer importing from
 * 'src/validation/index.ts' directly.
 *
 * @module utils/validation
 */

// Re-export from the comprehensive validation module
export {
  ValidationError,
  validateDatabaseName,
  validateCollectionName,
  validateFieldName,
  validateFilter,
  validateProjection,
  validateUpdate,
  validateDocument,
  validateAggregationPipeline,
  validateInputs,
  // Constants
  VALID_QUERY_OPERATORS,
  VALID_UPDATE_OPERATORS,
  VALID_AGGREGATION_STAGES,
  MAX_NAME_LENGTH,
  RESERVED_DATABASE_NAMES,
  SYSTEM_COLLECTION_PREFIX,
  // Types
  type FilterValidationOptions,
  type UpdateValidationOptions,
} from '@mongolake/validation/index.js';
