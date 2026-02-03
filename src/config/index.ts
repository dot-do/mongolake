/**
 * Configuration Module for MongoLake
 *
 * Provides configuration validation and management utilities.
 *
 * @module config
 */

export {
  // Validation functions
  validate,
  validateConfig,
  validateAndMerge,

  // Default management
  getDefaults,
  mergeWithDefaults,

  // Error class
  ConfigValidationError,

  // Type guards and utilities
  isValidParquetType,
  VALID_PARQUET_TYPES,

  // Types
  type ValidationIssue,
  type ValidationResult,
} from './validator.js';
