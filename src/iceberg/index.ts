/**
 * Iceberg Module
 *
 * Provides Iceberg table format support for MongoLake,
 * including manifest file generation and metadata management.
 */

export {
  ManifestWriter,
  type ManifestWriterOptions,
  type ManifestEntry,
  type DataFile,
  type DeleteFile,
  type PartitionFieldSummary,
  type ManifestContent,
  type ManifestMetadata,
  type ManifestSummary,
  type ColumnStats,
  type FileFormat,
  type ManifestEntryStatus,
  type SortOrder,
  type EntryOptions,
  type PartitionValue,
  type DataFileContent,
} from './manifest-writer.js';

export {
  // Class
  SnapshotManager,
  // Types
  type Snapshot,
  type SnapshotSummary,
  type OperationType,
  type CreateSnapshotOptions,
  type ListSnapshotsOptions,
  type AncestryOptions,
  type ExpireSnapshotsOptions,
  type ExpireSnapshotsResult,
  type RollbackResult,
  type CherryPickResult,
  type SnapshotManagerConfig,
  type RetentionPolicy,
} from './snapshot-manager.js';

export {
  // Class
  TimeTravelReader,
  // Types
  type TimeTravelOptions,
  type TimeTravelResult,
  type SnapshotQueryResult,
  type ListSnapshotsOptions as TimeTravelListSnapshotsOptions,
  type AncestryOptions as TimeTravelAncestryOptions,
  type SnapshotDiff,
  type ChangesResult,
  type ReadChangesOptions,
  type ReadDocumentsOptions,
  type TimeTravelCollectionView,
} from './time-travel-reader.js';

export {
  // Classes
  MetadataWriter,
  // Error Classes
  MetadataError,
  FormatVersionError,
  InvalidSchemaError,
  InvalidPartitionSpecError,
  InvalidSortOrderError,
  InvalidSnapshotError,
  MetadataSerializationError,
  // Types
  type MetadataWriterOptions,
  type GenerateMetadataOptions,
  type IcebergPrimitiveType,
  type IcebergListType,
  type IcebergMapType,
  type IcebergStructType,
  type IcebergType,
  type IcebergSchemaField,
  type IcebergSchema,
  type PartitionField,
  type PartitionSpec,
  type SortField,
  type SortOrder as MetadataSortOrder,
  type SnapshotSummary as MetadataSnapshotSummary,
  type Snapshot as MetadataSnapshot,
  type SnapshotRef,
  type ManifestListLocation,
  type MetadataLogEntry,
  type TableMetadata,
} from './metadata-writer.js';

export {
  // Classes
  R2DataCatalogClient,
  R2DataCatalogError,
  // Factory Functions
  createR2DataCatalogClient,
  createCatalogClient,
  // Types
  type R2DataCatalogConfig,
  type CatalogNamespace,
  type RegisterTableRequest,
  type UpdateTableRequest,
  type CatalogTable,
  type ListTablesResponse as R2ListTablesResponse,
  type ListNamespacesResponse as R2ListNamespacesResponse,
  type CatalogErrorResponse,
  type CatalogApiResponse,
} from './catalog-client.js';

export {
  // Classes
  RestCatalog,
  RestCatalogError,
  NotFoundError,
  AlreadyExistsError,
  ValidationError,
  AuthenticationError,
  CommitFailedError,
  // Factory Functions
  createRestCatalog,
  createRestCatalogFromEnv,
  // Utility Functions
  generateMetadataPath,
  generateManifestListPath,
  generateManifestPath,
  generateDataFilePath,
  // Types
  type RestCatalogConfig,
  type HttpMethod,
  type AuthType,
  type NamespaceIdentifier,
  type TableIdentifier,
  type Namespace,
  type CreateNamespaceRequest,
  type CreateNamespaceResponse,
  type UpdateNamespacePropertiesRequest,
  type UpdateNamespacePropertiesResponse,
  type ListNamespacesResponse,
  type ListTablesResponse,
  type CreateTableRequest,
  type LoadTableResponse,
  type SortOrder as RestSortOrder,
  type SortField as RestSortField,
  type TableRequirement,
  type TableUpdate,
  type SnapshotUpdate,
  type CommitTableRequest,
  type CommitTableResponse,
  type RenameTableRequest,
  type ErrorResponse,
  type TokenResponse,
  type CatalogConfig,
} from './rest-catalog.js';

export {
  // Classes
  SchemaTracker,
  SchemaTrackerError,
  // Factory Functions
  createSchemaTracker,
  // Utility Functions
  validateSchemaEvolution,
  generateSchemaId,
  generateMigrationPlan,
  isMigrationSafe,
  getChangeSummary,
  ROOT_PARENT_ID,
  // Types
  type SchemaChangeType,
  type SchemaChange,
  type SchemaEvolutionMetadata,
  type AddFieldOptions,
  type SchemaEvolutionOptions,
  type SchemaComparisonResult,
  type MigrationStep,
  type MigrationPlan,
} from './schema-tracker.js';

export {
  // Classes
  ManifestListWriter,
  ManifestListError,
  ManifestEntryValidationError,
  ManifestListWriterStateError,
  ManifestListStorageError,
  // Constants
  MANIFEST_CONTENT_DATA,
  MANIFEST_CONTENT_DELETES,
  // Types
  type ManifestContent as ManifestListContent,
  type PartitionFieldSummary as ManifestListPartitionFieldSummary,
  type ManifestFileEntry,
  type ManifestListMetadata,
  type ManifestListWriterOptions,
  type WriteOptions as ManifestListWriteOptions,
  type ManifestListWriteResult,
  type ManifestListStatistics,
  type ManifestListWriterState,
} from './manifest-list-writer.js';
