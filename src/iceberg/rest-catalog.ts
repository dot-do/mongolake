/**
 * Iceberg REST Catalog
 *
 * Implementation of the Apache Iceberg REST Catalog API specification.
 * Provides a standards-compliant catalog interface for managing Iceberg tables.
 *
 * @see https://iceberg.apache.org/spec/#rest-catalog
 * @module iceberg/rest-catalog
 */

import type {
  IcebergSchema,
  PartitionSpec,
  TableMetadata,
} from './metadata-writer.js';

// ============================================================================
// Types
// ============================================================================

/** HTTP methods supported by the REST catalog */
export type HttpMethod = 'GET' | 'POST' | 'PUT' | 'DELETE' | 'HEAD';

/** Authentication type for REST catalog */
export type AuthType = 'none' | 'bearer' | 'oauth2';

/**
 * REST catalog configuration options.
 */
export interface RestCatalogConfig {
  /** Base URI for the REST catalog (e.g., 'https://catalog.example.com/api/v1') */
  uri: string;
  /** Warehouse location (e.g., 's3://bucket/warehouse') */
  warehouse?: string;
  /** Authentication type */
  authType?: AuthType;
  /** Bearer token for authentication */
  token?: string;
  /** OAuth2 credential for authentication */
  credential?: string;
  /** OAuth2 scope */
  scope?: string;
  /** Custom headers to include in requests */
  headers?: Record<string, string>;
  /** Request timeout in milliseconds */
  timeoutMs?: number;
  /** Enable SSL certificate verification */
  sslVerify?: boolean;
  /** Custom fetch implementation (for testing) */
  fetch?: typeof fetch;
}

/**
 * Namespace identifier (hierarchical array of strings).
 */
export type NamespaceIdentifier = string[];

/**
 * Table identifier consisting of namespace and name.
 */
export interface TableIdentifier {
  namespace: NamespaceIdentifier;
  name: string;
}

/**
 * Namespace metadata including properties.
 */
export interface Namespace {
  namespace: NamespaceIdentifier;
  properties?: Record<string, string>;
}

/**
 * Request to create a namespace.
 */
export interface CreateNamespaceRequest {
  namespace: NamespaceIdentifier;
  properties?: Record<string, string>;
}

/**
 * Response from creating or getting a namespace.
 */
export interface CreateNamespaceResponse {
  namespace: NamespaceIdentifier;
  properties?: Record<string, string>;
}

/**
 * Request to update namespace properties.
 */
export interface UpdateNamespacePropertiesRequest {
  removals?: string[];
  updates?: Record<string, string>;
}

/**
 * Response from updating namespace properties.
 */
export interface UpdateNamespacePropertiesResponse {
  updated: string[];
  removed: string[];
  missing?: string[];
}

/**
 * Response from listing namespaces.
 */
export interface ListNamespacesResponse {
  namespaces: NamespaceIdentifier[];
  nextPageToken?: string;
}

/**
 * Response from listing tables.
 */
export interface ListTablesResponse {
  identifiers: TableIdentifier[];
  nextPageToken?: string;
}

/**
 * Request to create a table.
 */
export interface CreateTableRequest {
  name: string;
  location?: string;
  schema: IcebergSchema;
  partitionSpec?: PartitionSpec;
  writeOrder?: SortOrder;
  stageCreate?: boolean;
  properties?: Record<string, string>;
}

/**
 * Response from loading a table.
 */
export interface LoadTableResponse {
  metadataLocation?: string;
  metadata: TableMetadata;
  config?: Record<string, string>;
}

/**
 * Sort order specification.
 */
export interface SortOrder {
  'order-id': number;
  fields: SortField[];
}

/**
 * Sort field specification.
 */
export interface SortField {
  transform: string;
  'source-id': number;
  direction: 'asc' | 'desc';
  'null-order': 'nulls-first' | 'nulls-last';
}

/**
 * Table requirement for atomic operations.
 */
export type TableRequirement =
  | { type: 'assert-create' }
  | { type: 'assert-table-uuid'; uuid: string }
  | { type: 'assert-ref-snapshot-id'; ref: string; 'snapshot-id': number | null }
  | { type: 'assert-last-assigned-field-id'; 'last-assigned-field-id': number }
  | { type: 'assert-current-schema-id'; 'current-schema-id': number }
  | { type: 'assert-last-assigned-partition-id'; 'last-assigned-partition-id': number }
  | { type: 'assert-default-spec-id'; 'default-spec-id': number }
  | { type: 'assert-default-sort-order-id'; 'default-sort-order-id': number };

/**
 * Table update operation.
 */
export type TableUpdate =
  | { action: 'assign-uuid'; uuid: string }
  | { action: 'upgrade-format-version'; 'format-version': number }
  | { action: 'add-schema'; schema: IcebergSchema; 'last-column-id'?: number }
  | { action: 'set-current-schema'; 'schema-id': number }
  | { action: 'add-spec'; spec: PartitionSpec }
  | { action: 'set-default-spec'; 'spec-id': number }
  | { action: 'add-sort-order'; 'sort-order': SortOrder }
  | { action: 'set-default-sort-order'; 'sort-order-id': number }
  | { action: 'add-snapshot'; snapshot: SnapshotUpdate }
  | { action: 'set-snapshot-ref'; 'ref-name': string; type: 'branch' | 'tag'; 'snapshot-id': number }
  | { action: 'remove-snapshots'; 'snapshot-ids': number[] }
  | { action: 'remove-snapshot-ref'; 'ref-name': string }
  | { action: 'set-location'; location: string }
  | { action: 'set-properties'; updates: Record<string, string> }
  | { action: 'remove-properties'; removals: string[] };

/**
 * Snapshot update for commit operations.
 */
export interface SnapshotUpdate {
  'snapshot-id': number;
  'parent-snapshot-id'?: number;
  'sequence-number'?: number;
  'timestamp-ms': number;
  'manifest-list': string;
  summary: {
    operation: 'append' | 'replace' | 'overwrite' | 'delete';
    [key: string]: string;
  };
  'schema-id'?: number;
}

/**
 * Request to commit table updates.
 */
export interface CommitTableRequest {
  identifier?: TableIdentifier;
  requirements: TableRequirement[];
  updates: TableUpdate[];
}

/**
 * Response from committing table updates.
 */
export interface CommitTableResponse {
  metadataLocation: string;
  metadata: TableMetadata;
}

/**
 * Request to rename a table.
 */
export interface RenameTableRequest {
  source: TableIdentifier;
  destination: TableIdentifier;
}

/**
 * Error response from the REST catalog.
 */
export interface ErrorResponse {
  error: {
    message: string;
    type: string;
    code: number;
    stack?: string[];
  };
}

/**
 * OAuth2 token response.
 */
export interface TokenResponse {
  'access_token': string;
  'token_type': string;
  'expires_in'?: number;
  'issued_token_type'?: string;
  scope?: string;
}

/**
 * Catalog configuration response.
 */
export interface CatalogConfig {
  overrides?: Record<string, string>;
  defaults?: Record<string, string>;
}

// ============================================================================
// Error Classes
// ============================================================================

/**
 * Base error for REST catalog operations.
 */
export class RestCatalogError extends Error {
  readonly statusCode: number;
  readonly errorType: string;

  constructor(message: string, statusCode: number, errorType: string = 'UnknownError') {
    super(message);
    this.name = 'RestCatalogError';
    this.statusCode = statusCode;
    this.errorType = errorType;
  }
}

/**
 * Error thrown when a resource is not found.
 */
export class NotFoundError extends RestCatalogError {
  constructor(message: string) {
    super(message, 404, 'NoSuchNamespaceException');
    this.name = 'NotFoundError';
  }
}

/**
 * Error thrown when a resource already exists.
 */
export class AlreadyExistsError extends RestCatalogError {
  constructor(message: string) {
    super(message, 409, 'AlreadyExistsException');
    this.name = 'AlreadyExistsError';
  }
}

/**
 * Error thrown for validation failures.
 */
export class ValidationError extends RestCatalogError {
  constructor(message: string) {
    super(message, 400, 'BadRequestException');
    this.name = 'ValidationError';
  }
}

/**
 * Error thrown for authentication failures.
 */
export class AuthenticationError extends RestCatalogError {
  constructor(message: string) {
    super(message, 401, 'NotAuthorizedException');
    this.name = 'AuthenticationError';
  }
}

/**
 * Error thrown for commit conflicts.
 */
export class CommitFailedError extends RestCatalogError {
  constructor(message: string) {
    super(message, 409, 'CommitFailedException');
    this.name = 'CommitFailedError';
  }
}

// ============================================================================
// Constants
// ============================================================================

/** Default request timeout in milliseconds */
const DEFAULT_TIMEOUT_MS = 30000;

/** Namespace path separator for URL encoding */
const NAMESPACE_SEPARATOR = '\x1F';

/** Content type for JSON requests/responses */
const JSON_CONTENT_TYPE = 'application/json';

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Encode a namespace for use in URL paths.
 */
function encodeNamespace(namespace: NamespaceIdentifier): string {
  return encodeURIComponent(namespace.join(NAMESPACE_SEPARATOR));
}

/**
 * Validate that a namespace is not empty.
 */
function validateNamespace(namespace: NamespaceIdentifier): void {
  if (!namespace || namespace.length === 0) {
    throw new ValidationError('Namespace cannot be empty');
  }
  for (const part of namespace) {
    if (!part || part.trim() === '') {
      throw new ValidationError('Namespace parts cannot be empty');
    }
    if (part.includes('/') || part.includes(NAMESPACE_SEPARATOR)) {
      throw new ValidationError('Namespace parts cannot contain "/" or separator characters');
    }
  }
}

/**
 * Validate that a table name is not empty.
 */
function validateTableName(name: string): void {
  if (!name || name.trim() === '') {
    throw new ValidationError('Table name cannot be empty');
  }
}

/**
 * Generate a metadata file path following Iceberg conventions.
 */
export function generateMetadataPath(
  tableLocation: string,
  version: number,
  uuid?: string
): string {
  const suffix = uuid ? `-${uuid}` : '';
  return `${tableLocation}/metadata/v${version}${suffix}.metadata.json`;
}

/**
 * Generate a manifest list path following Iceberg conventions.
 */
export function generateManifestListPath(
  tableLocation: string,
  snapshotId: bigint,
  attemptId?: number
): string {
  const attempt = attemptId !== undefined ? `-${attemptId}` : '';
  return `${tableLocation}/metadata/snap-${snapshotId}${attempt}.avro`;
}

/**
 * Generate a manifest file path following Iceberg conventions.
 */
export function generateManifestPath(
  tableLocation: string,
  manifestId: string
): string {
  return `${tableLocation}/metadata/${manifestId}.avro`;
}

/**
 * Generate a data file path following Iceberg conventions.
 */
export function generateDataFilePath(
  tableLocation: string,
  partitionPath: string | null,
  fileId: string
): string {
  if (partitionPath) {
    return `${tableLocation}/data/${partitionPath}/${fileId}.parquet`;
  }
  return `${tableLocation}/data/${fileId}.parquet`;
}

// ============================================================================
// RestCatalog Class
// ============================================================================

/**
 * REST Catalog client implementation following the Iceberg REST spec.
 *
 * Provides a complete implementation of the Iceberg REST Catalog API including:
 * - Namespace management (create, list, delete, update properties)
 * - Table management (create, load, update, delete, rename)
 * - Atomic multi-table transactions
 * - OAuth2 authentication
 *
 * @example
 * ```typescript
 * const catalog = new RestCatalog({
 *   uri: 'https://catalog.example.com/api/v1',
 *   warehouse: 's3://my-bucket/warehouse',
 *   authType: 'bearer',
 *   token: 'my-token',
 * });
 *
 * // Create a namespace
 * await catalog.createNamespace(['production', 'analytics']);
 *
 * // List tables
 * const tables = await catalog.listTables(['production', 'analytics']);
 * ```
 */
export class RestCatalog {
  private readonly uri: string;
  private readonly warehouse?: string;
  private readonly authType: AuthType;
  private readonly headers: Record<string, string>;
  private readonly timeoutMs: number;
  private readonly fetchFn: typeof fetch;

  private accessToken?: string;
  private tokenExpiresAt?: number;
  private readonly credential?: string;
  private readonly scope?: string;

  constructor(config: RestCatalogConfig) {
    if (!config.uri || config.uri.trim() === '') {
      throw new ValidationError('URI is required');
    }

    this.uri = config.uri.replace(/\/$/, ''); // Remove trailing slash
    this.warehouse = config.warehouse;
    this.authType = config.authType ?? 'none';
    this.headers = { ...config.headers };
    this.timeoutMs = config.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    this.fetchFn = config.fetch ?? fetch;

    if (config.token) {
      this.accessToken = config.token;
    }
    this.credential = config.credential;
    this.scope = config.scope;
  }

  // ==========================================================================
  // HTTP Client Methods
  // ==========================================================================

  /**
   * Make an authenticated request to the REST catalog.
   */
  private async request<T>(
    method: HttpMethod,
    path: string,
    options?: {
      body?: unknown;
      queryParams?: Record<string, string | undefined>;
    }
  ): Promise<T> {
    // Ensure we have a valid token if using OAuth2
    if (this.authType === 'oauth2') {
      await this.ensureValidToken();
    }

    const url = this.buildUrl(path, options?.queryParams);
    const headers = this.buildHeaders(options?.body !== undefined);

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const response = await this.fetchFn(url, {
        method,
        headers,
        body: options?.body ? JSON.stringify(options.body) : undefined,
        signal: controller.signal,
      });

      clearTimeout(timeoutId);
      return this.handleResponse<T>(response);
    } catch (error) {
      clearTimeout(timeoutId);
      if (error instanceof Error && error.name === 'AbortError') {
        throw new RestCatalogError('Request timeout', 408, 'TimeoutException');
      }
      throw error;
    }
  }

  /**
   * Build the full URL with query parameters.
   */
  private buildUrl(path: string, queryParams?: Record<string, string | undefined>): string {
    const url = new URL(`${this.uri}/${path}`);

    if (queryParams) {
      for (const [key, value] of Object.entries(queryParams)) {
        if (value !== undefined) {
          url.searchParams.set(key, value);
        }
      }
    }

    return url.toString();
  }

  /**
   * Build request headers including authentication.
   */
  private buildHeaders(hasBody: boolean): Record<string, string> {
    const headers: Record<string, string> = {
      ...this.headers,
      Accept: JSON_CONTENT_TYPE,
    };

    if (hasBody) {
      headers['Content-Type'] = JSON_CONTENT_TYPE;
    }

    if (this.accessToken) {
      headers['Authorization'] = `Bearer ${this.accessToken}`;
    }

    return headers;
  }

  /**
   * Handle the HTTP response and parse errors.
   */
  private async handleResponse<T>(response: Response): Promise<T> {
    const text = await response.text();

    if (!response.ok) {
      let errorResponse: ErrorResponse | undefined;
      try {
        errorResponse = JSON.parse(text) as ErrorResponse;
      } catch {
        // Response is not JSON
      }

      const message = errorResponse?.error?.message ?? text ?? 'Unknown error';
      const errorType = errorResponse?.error?.type ?? 'UnknownError';

      switch (response.status) {
        case 401:
          throw new AuthenticationError(message);
        case 404:
          throw new NotFoundError(message);
        case 409:
          if (errorType.includes('AlreadyExists')) {
            throw new AlreadyExistsError(message);
          }
          throw new CommitFailedError(message);
        case 400:
          throw new ValidationError(message);
        default:
          throw new RestCatalogError(message, response.status, errorType);
      }
    }

    if (!text) {
      return undefined as T;
    }

    return JSON.parse(text) as T;
  }

  /**
   * Ensure we have a valid OAuth2 token.
   */
  private async ensureValidToken(): Promise<void> {
    if (this.accessToken && this.tokenExpiresAt && Date.now() < this.tokenExpiresAt) {
      return; // Token is still valid
    }

    if (!this.credential) {
      throw new AuthenticationError('OAuth2 credential is required');
    }

    const tokenResponse = await this.getToken();
    this.accessToken = tokenResponse.access_token;

    if (tokenResponse.expires_in) {
      // Refresh 30 seconds before expiry
      this.tokenExpiresAt = Date.now() + (tokenResponse.expires_in - 30) * 1000;
    }
  }

  /**
   * Get a new OAuth2 token.
   */
  private async getToken(): Promise<TokenResponse> {
    const url = `${this.uri}/oauth/tokens`;
    const body = new URLSearchParams();
    body.set('grant_type', 'client_credentials');
    body.set('client_id', this.credential!.split(':')[0]!);
    body.set('client_secret', this.credential!.split(':')[1] ?? '');

    if (this.scope) {
      body.set('scope', this.scope);
    }

    const response = await this.fetchFn(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        Accept: JSON_CONTENT_TYPE,
      },
      body: body.toString(),
    });

    if (!response.ok) {
      throw new AuthenticationError('Failed to obtain OAuth2 token');
    }

    return response.json() as Promise<TokenResponse>;
  }

  // ==========================================================================
  // Configuration
  // ==========================================================================

  /**
   * Get catalog configuration.
   */
  async getConfig(): Promise<CatalogConfig> {
    const queryParams: Record<string, string | undefined> = {};
    if (this.warehouse) {
      queryParams.warehouse = this.warehouse;
    }
    return this.request<CatalogConfig>('GET', 'config', { queryParams });
  }

  // ==========================================================================
  // Namespace Operations
  // ==========================================================================

  /**
   * List all namespaces, optionally under a parent namespace.
   */
  async listNamespaces(
    parent?: NamespaceIdentifier,
    pageToken?: string,
    pageSize?: number
  ): Promise<ListNamespacesResponse> {
    const queryParams: Record<string, string | undefined> = {};

    if (parent && parent.length > 0) {
      queryParams.parent = encodeNamespace(parent);
    }
    if (pageToken) {
      queryParams.pageToken = pageToken;
    }
    if (pageSize !== undefined) {
      queryParams.pageSize = String(pageSize);
    }

    return this.request<ListNamespacesResponse>('GET', 'namespaces', { queryParams });
  }

  /**
   * Create a new namespace.
   */
  async createNamespace(
    namespace: NamespaceIdentifier,
    properties?: Record<string, string>
  ): Promise<CreateNamespaceResponse> {
    validateNamespace(namespace);

    const body: CreateNamespaceRequest = {
      namespace,
      properties,
    };

    return this.request<CreateNamespaceResponse>('POST', 'namespaces', { body });
  }

  /**
   * Get namespace metadata.
   */
  async getNamespace(namespace: NamespaceIdentifier): Promise<Namespace> {
    validateNamespace(namespace);

    const encodedNamespace = encodeNamespace(namespace);
    return this.request<Namespace>('GET', `namespaces/${encodedNamespace}`);
  }

  /**
   * Check if a namespace exists.
   */
  async namespaceExists(namespace: NamespaceIdentifier): Promise<boolean> {
    try {
      validateNamespace(namespace);
      const encodedNamespace = encodeNamespace(namespace);
      await this.request<void>('HEAD', `namespaces/${encodedNamespace}`);
      return true;
    } catch (error) {
      if (error instanceof NotFoundError) {
        return false;
      }
      throw error;
    }
  }

  /**
   * Update namespace properties.
   */
  async updateNamespaceProperties(
    namespace: NamespaceIdentifier,
    updates?: Record<string, string>,
    removals?: string[]
  ): Promise<UpdateNamespacePropertiesResponse> {
    validateNamespace(namespace);

    const encodedNamespace = encodeNamespace(namespace);
    const body: UpdateNamespacePropertiesRequest = {
      updates,
      removals,
    };

    return this.request<UpdateNamespacePropertiesResponse>(
      'POST',
      `namespaces/${encodedNamespace}/properties`,
      { body }
    );
  }

  /**
   * Drop a namespace (must be empty).
   */
  async dropNamespace(namespace: NamespaceIdentifier): Promise<void> {
    validateNamespace(namespace);

    const encodedNamespace = encodeNamespace(namespace);
    await this.request<void>('DELETE', `namespaces/${encodedNamespace}`);
  }

  // ==========================================================================
  // Table Operations
  // ==========================================================================

  /**
   * List all tables in a namespace.
   */
  async listTables(
    namespace: NamespaceIdentifier,
    pageToken?: string,
    pageSize?: number
  ): Promise<ListTablesResponse> {
    validateNamespace(namespace);

    const encodedNamespace = encodeNamespace(namespace);
    const queryParams: Record<string, string | undefined> = {};

    if (pageToken) {
      queryParams.pageToken = pageToken;
    }
    if (pageSize !== undefined) {
      queryParams.pageSize = String(pageSize);
    }

    return this.request<ListTablesResponse>(
      'GET',
      `namespaces/${encodedNamespace}/tables`,
      { queryParams }
    );
  }

  /**
   * Create a new table.
   */
  async createTable(
    namespace: NamespaceIdentifier,
    request: CreateTableRequest
  ): Promise<LoadTableResponse> {
    validateNamespace(namespace);
    validateTableName(request.name);

    const encodedNamespace = encodeNamespace(namespace);
    return this.request<LoadTableResponse>(
      'POST',
      `namespaces/${encodedNamespace}/tables`,
      { body: request }
    );
  }

  /**
   * Load table metadata.
   */
  async loadTable(
    namespace: NamespaceIdentifier,
    tableName: string,
    snapshotId?: bigint
  ): Promise<LoadTableResponse> {
    validateNamespace(namespace);
    validateTableName(tableName);

    const encodedNamespace = encodeNamespace(namespace);
    const queryParams: Record<string, string | undefined> = {};

    if (snapshotId !== undefined) {
      queryParams.snapshots = String(snapshotId);
    }

    return this.request<LoadTableResponse>(
      'GET',
      `namespaces/${encodedNamespace}/tables/${encodeURIComponent(tableName)}`,
      { queryParams }
    );
  }

  /**
   * Check if a table exists.
   */
  async tableExists(namespace: NamespaceIdentifier, tableName: string): Promise<boolean> {
    try {
      validateNamespace(namespace);
      validateTableName(tableName);

      const encodedNamespace = encodeNamespace(namespace);
      await this.request<void>(
        'HEAD',
        `namespaces/${encodedNamespace}/tables/${encodeURIComponent(tableName)}`
      );
      return true;
    } catch (error) {
      if (error instanceof NotFoundError) {
        return false;
      }
      throw error;
    }
  }

  /**
   * Update table with atomic operations.
   */
  async updateTable(
    namespace: NamespaceIdentifier,
    tableName: string,
    requirements: TableRequirement[],
    updates: TableUpdate[]
  ): Promise<CommitTableResponse> {
    validateNamespace(namespace);
    validateTableName(tableName);

    const encodedNamespace = encodeNamespace(namespace);
    const body: CommitTableRequest = {
      requirements,
      updates,
    };

    return this.request<CommitTableResponse>(
      'POST',
      `namespaces/${encodedNamespace}/tables/${encodeURIComponent(tableName)}`,
      { body }
    );
  }

  /**
   * Drop a table.
   */
  async dropTable(
    namespace: NamespaceIdentifier,
    tableName: string,
    purge: boolean = false
  ): Promise<void> {
    validateNamespace(namespace);
    validateTableName(tableName);

    const encodedNamespace = encodeNamespace(namespace);
    const queryParams: Record<string, string | undefined> = {};

    if (purge) {
      queryParams.purgeRequested = 'true';
    }

    await this.request<void>(
      'DELETE',
      `namespaces/${encodedNamespace}/tables/${encodeURIComponent(tableName)}`,
      { queryParams }
    );
  }

  /**
   * Rename a table.
   */
  async renameTable(
    source: TableIdentifier,
    destination: TableIdentifier
  ): Promise<void> {
    validateNamespace(source.namespace);
    validateTableName(source.name);
    validateNamespace(destination.namespace);
    validateTableName(destination.name);

    const body: RenameTableRequest = {
      source,
      destination,
    };

    await this.request<void>('POST', 'tables/rename', { body });
  }

  /**
   * Register an existing metadata file as a table.
   */
  async registerTable(
    namespace: NamespaceIdentifier,
    tableName: string,
    metadataLocation: string
  ): Promise<LoadTableResponse> {
    validateNamespace(namespace);
    validateTableName(tableName);

    const encodedNamespace = encodeNamespace(namespace);
    const body = {
      name: tableName,
      'metadata-location': metadataLocation,
    };

    return this.request<LoadTableResponse>(
      'POST',
      `namespaces/${encodedNamespace}/register`,
      { body }
    );
  }

  // ==========================================================================
  // Multi-Table Transaction Support
  // ==========================================================================

  /**
   * Commit updates to multiple tables atomically.
   */
  async commitTransaction(
    tableCommits: Array<{
      identifier: TableIdentifier;
      requirements: TableRequirement[];
      updates: TableUpdate[];
    }>
  ): Promise<void> {
    for (const commit of tableCommits) {
      validateNamespace(commit.identifier.namespace);
      validateTableName(commit.identifier.name);
    }

    const body = {
      'table-changes': tableCommits.map((commit) => ({
        identifier: commit.identifier,
        requirements: commit.requirements,
        updates: commit.updates,
      })),
    };

    await this.request<void>('POST', 'transactions/commit', { body });
  }

  // ==========================================================================
  // MongoLake Convenience Methods
  // ==========================================================================

  /**
   * Register a MongoLake collection as an Iceberg table.
   * Creates the necessary namespace structure if it doesn't exist.
   */
  async registerMongoLakeCollection(
    database: string,
    collection: string,
    tableLocation: string,
    schema: IcebergSchema,
    options?: {
      partitionSpec?: PartitionSpec;
      properties?: Record<string, string>;
    }
  ): Promise<LoadTableResponse> {
    const namespace = ['mongolake', database];

    // Ensure namespace exists
    const exists = await this.namespaceExists(namespace);
    if (!exists) {
      // Create parent namespace first if needed
      const parentExists = await this.namespaceExists(['mongolake']);
      if (!parentExists) {
        await this.createNamespace(['mongolake'], {
          description: 'MongoLake collections',
        });
      }

      await this.createNamespace(namespace, {
        description: `MongoLake database: ${database}`,
      });
    }

    // Create the table
    return this.createTable(namespace, {
      name: collection,
      location: tableLocation,
      schema,
      partitionSpec: options?.partitionSpec,
      properties: {
        'mongolake.database': database,
        'mongolake.collection': collection,
        ...options?.properties,
      },
    });
  }

  /**
   * List all MongoLake databases (namespaces under 'mongolake').
   */
  async listMongoLakeDatabases(): Promise<string[]> {
    try {
      const response = await this.listNamespaces(['mongolake']);
      return response.namespaces.map((ns) => ns[ns.length - 1]!);
    } catch (error) {
      if (error instanceof NotFoundError) {
        return [];
      }
      throw error;
    }
  }

  /**
   * List all collections in a MongoLake database.
   */
  async listMongoLakeCollections(database: string): Promise<string[]> {
    const namespace = ['mongolake', database];
    const response = await this.listTables(namespace);
    return response.identifiers.map((id) => id.name);
  }

  /**
   * Load a MongoLake collection's table metadata.
   */
  async loadMongoLakeCollection(
    database: string,
    collection: string
  ): Promise<LoadTableResponse> {
    const namespace = ['mongolake', database];
    return this.loadTable(namespace, collection);
  }

  /**
   * Drop a MongoLake collection (unregister from catalog).
   */
  async dropMongoLakeCollection(
    database: string,
    collection: string,
    purge: boolean = false
  ): Promise<void> {
    const namespace = ['mongolake', database];
    await this.dropTable(namespace, collection, purge);
  }
}

// ============================================================================
// Factory Functions
// ============================================================================

/**
 * Create a REST catalog client from environment variables.
 *
 * Required environment variables:
 * - ICEBERG_REST_CATALOG_URI: Base URI for the catalog
 *
 * Optional environment variables:
 * - ICEBERG_WAREHOUSE: Warehouse location
 * - ICEBERG_REST_TOKEN: Bearer token for authentication
 * - ICEBERG_REST_CREDENTIAL: OAuth2 credential (client_id:client_secret)
 * - ICEBERG_REST_SCOPE: OAuth2 scope
 */
export function createRestCatalogFromEnv(): RestCatalog {
  const uri = process.env.ICEBERG_REST_CATALOG_URI;
  if (!uri) {
    throw new Error('ICEBERG_REST_CATALOG_URI environment variable is required');
  }

  const config: RestCatalogConfig = {
    uri,
    warehouse: process.env.ICEBERG_WAREHOUSE,
  };

  const token = process.env.ICEBERG_REST_TOKEN;
  const credential = process.env.ICEBERG_REST_CREDENTIAL;

  if (token) {
    config.authType = 'bearer';
    config.token = token;
  } else if (credential) {
    config.authType = 'oauth2';
    config.credential = credential;
    config.scope = process.env.ICEBERG_REST_SCOPE;
  }

  return new RestCatalog(config);
}

/**
 * Create a REST catalog client with the provided configuration.
 */
export function createRestCatalog(config: RestCatalogConfig): RestCatalog {
  return new RestCatalog(config);
}
