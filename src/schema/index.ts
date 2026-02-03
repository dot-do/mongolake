/**
 * Schema Module
 *
 * Provides schema configuration parsing, validation, and analysis for field promotion.
 *
 * @module schema
 */

export {
  // Error class
  SchemaConfigError,

  // Types
  type FullSchemaConfig,
  type ParsedColumn,
  type ParsedCollectionSchema,
  type ParsedSchemaConfig,
  type SchemaCacheOptions,

  // Parsing functions
  parseSchemaConfig,
  parseSimpleSchemaConfig,
  parseSchemaConfigFromJSON,
  parseSchemaConfigFromYAML,

  // Cached parsing
  SchemaCache,
  getGlobalSchemaCache,
  parseSchemaConfigCached,

  // Schema access utilities
  getColumnForPath,
  isPromotedField,
  getPromotedFieldPaths,
  extractPromotedFields,
  validatePromotedFieldTypes,
} from './config.js';

export {
  // Field Analyzer class
  FieldAnalyzer,

  // Types
  type DetectedType,
  type FieldStats,
  type PromotionSuggestion,
  type AnalyzeOptions,
  type PromotionOptions,
  type ArrayElementStats,
  type SerializedFieldAnalyzer,

  // Utility functions
  createFieldAnalyzer,
  analyzeDocuments,
  suggestFieldPromotions,
} from './analyzer.js';

export {
  // Type promotion functions
  canPromoteSafely,
  getPromotedType,
  detectedTypeToParquet,
  detectValueType,
  isNumericType,
  getNumericPrecision,

  // Schema promotion detection
  detectPromotion,
  compareSchemas,

  // Migration functions
  generateMigration,
  applyMigration,
  isSafeMigration,
  createVersionedSchema,

  // Schema History class
  SchemaHistory,

  // Utilities
  flattenDocument,

  // Types
  type TypePromotion,
  type SchemaMigration,
  type SchemaComparison,
  type VersionedSchema,
  type NewFieldDefinition,
  type TypeConflict,
  type SchemaHistoryEntry,
  type SerializedSchemaHistory,
} from './promoter.js';

export {
  // Schema Versioning class
  SchemaVersionManager,

  // Factory function
  createSchemaVersionManager,

  // Comparison functions
  compareSchemaVersions,
  generateSchemaDiff,
  calculateMigrationPath,

  // Type utilities
  isTypeWideningAllowed,
  getCommonSupertype,

  // Types
  type SchemaField,
  type SchemaVersion,
  type SchemaDiff,
  type ChangedField,
  type MigrationStep,
  type VersionMetadata,
  type CreateVersionOptions,
  type VersionHistoryOptions,
  type PruneOptions,
  type SchemaVersionManagerOptions,
} from './versioning.js';

export {
  // Migration Runner class (document-level migrations)
  MigrationRunner,

  // Migration Manager class (database-level migrations)
  MigrationManager,

  // Factory functions
  createMigrationRunner,
  defineMigration,
  createMigrationManager,
  defineDatabaseMigration,

  // Migration operations
  addField,
  removeField,
  renameField,
  changeFieldType,
  compose,
  conditional,

  // Type conversion
  convertToType,

  // Document migration types
  type MigratableDocument,
  type MigrationTransform,
  type MigrationDefinition,
  type TypeConversionOptions,
  type MigrationResult,
  type BatchMigrationResult,
  type BatchMigrationOptions,
  type SchemaVersionMetadata,
  type SerializedMigrationRegistry,

  // Database migration types
  type Migration,
  type MigrationRecord,
  type MigrationDatabase,
  type MigrationCollection,
  type MigrationCursor,
  type RunMigrationOptions,
  type MigrationRunResult,
} from './migration.js';
