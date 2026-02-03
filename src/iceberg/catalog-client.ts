/**
 * R2 Data Catalog Client
 *
 * Integration with Cloudflare R2 Data Catalog API for Iceberg tables.
 * Provides methods to register, update, and manage Iceberg tables
 * in the R2 Data Catalog.
 *
 * @see https://developers.cloudflare.com/r2/data-catalog/
 */

import type {
  IcebergSchema,
  PartitionSpec,
  TableMetadata,
} from './metadata-writer.js';

// Re-export types for consumers of this module
export type { IcebergSchema, PartitionSpec, TableMetadata };

// ============================================================================
// Types
// ============================================================================

/** R2 Data Catalog configuration */
export interface R2DataCatalogConfig {
  /** Cloudflare Account ID */
  accountId: string;
  /** R2 Data Catalog API token (R2_DATA_CATALOG_TOKEN) */
  token: string;
  /** Optional base URL for the API (defaults to Cloudflare API) */
  baseUrl?: string;
}

/** Namespace in the R2 Data Catalog */
export interface CatalogNamespace {
  /** Namespace identifier (array of names, e.g., ['db', 'schema']) */
  namespace: string[];
  /** Namespace properties */
  properties?: Record<string, string>;
}

/** Table registration request */
export interface RegisterTableRequest {
  /** Table name */
  name: string;
  /** Namespace (array of names) */
  namespace: string[];
  /** Table location in R2 (e.g., 's3://bucket/warehouse/db/table') */
  location: string;
  /** Iceberg schema */
  schema?: IcebergSchema;
  /** Partition specification */
  partitionSpec?: PartitionSpec;
  /** Table properties */
  properties?: Record<string, string>;
}

/** Table update request */
export interface UpdateTableRequest {
  /** Table name */
  name: string;
  /** Namespace */
  namespace: string[];
  /** New table location */
  location?: string;
  /** Metadata location (path to metadata.json) */
  metadataLocation?: string;
  /** Updated properties */
  properties?: Record<string, string>;
}

/** Table information from the catalog */
export interface CatalogTable {
  /** Table identifier */
  identifier: {
    namespace: string[];
    name: string;
  };
  /** Table location */
  location: string;
  /** Metadata location */
  metadataLocation: string;
  /** Table properties */
  properties: Record<string, string>;
  /** Table metadata (if loaded) */
  metadata?: TableMetadata;
}

/** List tables response */
export interface ListTablesResponse {
  /** Array of table identifiers */
  identifiers: Array<{
    namespace: string[];
    name: string;
  }>;
  /** Pagination token for next page */
  nextPageToken?: string;
}

/** List namespaces response */
export interface ListNamespacesResponse {
  /** Array of namespaces */
  namespaces: string[][];
  /** Pagination token for next page */
  nextPageToken?: string;
}

/** Error response from R2 Data Catalog API */
export interface CatalogErrorResponse {
  error: {
    code: string;
    message: string;
    type?: string;
  };
}

/** API response wrapper */
export type CatalogApiResponse<T> =
  | { success: true; result: T }
  | { success: false; error: CatalogErrorResponse['error'] };

// ============================================================================
// Constants
// ============================================================================

/** Default Cloudflare API base URL */
const DEFAULT_BASE_URL = 'https://api.cloudflare.com';

/** Namespace separator used in URL encoding */
const NAMESPACE_SEPARATOR = '\x1F'; // Unit separator character

/** MongoLake root namespace */
const MONGOLAKE_NAMESPACE = 'mongolake';

// ============================================================================
// Error Class
// ============================================================================

/**
 * Error thrown by R2 Data Catalog operations.
 */
export class R2DataCatalogError extends Error {
  readonly statusCode: number;
  readonly code?: string;

  constructor(message: string, statusCode: number, code?: string) {
    super(message);
    this.name = 'R2DataCatalogError';
    this.statusCode = statusCode;
    this.code = code;
  }
}

// ============================================================================
// Client Class
// ============================================================================

/**
 * Client for interacting with the Cloudflare R2 Data Catalog API.
 */
export class R2DataCatalogClient {
  private readonly accountId: string;
  private readonly token: string;
  private readonly baseUrl: string;

  constructor(config: R2DataCatalogConfig) {
    if (!config.accountId || config.accountId.trim() === '') {
      throw new Error('accountId is required');
    }
    if (!config.token || config.token.trim() === '') {
      throw new Error('token is required');
    }

    this.accountId = config.accountId;
    this.token = config.token;
    this.baseUrl = config.baseUrl ?? DEFAULT_BASE_URL;
  }

  // ==========================================================================
  // Private Helper Methods
  // ==========================================================================

  /**
   * Check if an error is a 404 Not Found error.
   */
  private isNotFoundError(error: unknown): boolean {
    return error instanceof R2DataCatalogError && error.statusCode === 404;
  }

  /**
   * Validate that a required string field is present and non-empty.
   */
  private validateRequiredString(value: string | undefined, fieldName: string): void {
    if (!value || value.trim() === '') {
      throw new Error(`${fieldName} is required`);
    }
  }

  /**
   * Encode a namespace array for use in URL paths.
   */
  private encodeNamespace(namespace: string[]): string {
    return encodeURIComponent(namespace.join(NAMESPACE_SEPARATOR));
  }

  /**
   * Build the API URL for a given path.
   */
  private buildUrl(path: string, queryParams?: Record<string, string | undefined>): string {
    const url = new URL(`/client/v4/accounts/${this.accountId}/r2/catalog/${path}`, this.baseUrl);

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
   * Make an authenticated API request.
   */
  private async request<T>(
    method: string,
    path: string,
    options?: {
      queryParams?: Record<string, string | undefined>;
      body?: unknown;
    }
  ): Promise<T> {
    const url = this.buildUrl(path, options?.queryParams);

    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.token}`,
    };

    if (options?.body) {
      headers['Content-Type'] = 'application/json';
    }

    let response: Response;
    try {
      response = await fetch(url, {
        method,
        headers,
        body: options?.body ? JSON.stringify(options.body) : undefined,
      });
    } catch (error) {
      // Re-throw network errors with better messages for common cases
      const errorMessage = error instanceof Error ? error.message : String(error);
      if (errorMessage.toLowerCase().includes('timed out')) {
        throw new Error(`Request timeout: ${errorMessage}`);
      }
      throw error;
    }

    const data = (await response.json()) as CatalogApiResponse<T>;

    if (data.success === false) {
      throw new R2DataCatalogError(data.error.message, response.status, data.error.code);
    }

    if (!response.ok) {
      throw new R2DataCatalogError('Unknown error', response.status);
    }

    return data.result;
  }

  /**
   * Validate namespace path.
   */
  private validateNamespace(namespace: string[]): void {
    if (!namespace || namespace.length === 0) {
      throw new Error('namespace cannot be empty');
    }

    for (const part of namespace) {
      if (part.includes('/') || part.includes(NAMESPACE_SEPARATOR)) {
        throw new Error('invalid character in namespace');
      }
    }
  }

  // ==========================================================================
  // Namespace Operations
  // ==========================================================================

  /**
   * List namespaces in the catalog.
   */
  async listNamespaces(parent?: string[], pageToken?: string): Promise<ListNamespacesResponse> {
    const queryParams: Record<string, string | undefined> = {};

    if (parent && parent.length > 0) {
      queryParams.parent = parent.join(NAMESPACE_SEPARATOR);
    }
    if (pageToken) {
      queryParams.pageToken = pageToken;
    }

    const result = await this.request<{ namespaces: string[][]; nextPageToken?: string }>(
      'GET',
      'namespaces',
      { queryParams }
    );

    return {
      namespaces: result.namespaces,
      nextPageToken: result.nextPageToken,
    };
  }

  /**
   * Create a new namespace.
   */
  async createNamespace(
    namespace: string[],
    properties?: Record<string, string>
  ): Promise<CatalogNamespace> {
    this.validateNamespace(namespace);

    const result = await this.request<{ namespace: string[]; properties?: Record<string, string> }>(
      'POST',
      'namespaces',
      {
        body: {
          namespace,
          properties,
        },
      }
    );

    return {
      namespace: result.namespace,
      properties: result.properties,
    };
  }

  /**
   * Get namespace metadata.
   */
  async getNamespace(namespace: string[]): Promise<CatalogNamespace> {
    this.validateNamespace(namespace);

    const encodedNamespace = this.encodeNamespace(namespace);
    const result = await this.request<{ namespace: string[]; properties?: Record<string, string> }>(
      'GET',
      `namespaces/${encodedNamespace}`
    );

    return {
      namespace: result.namespace,
      properties: result.properties,
    };
  }

  /**
   * Check if a namespace exists.
   */
  async namespaceExists(namespace: string[]): Promise<boolean> {
    try {
      await this.getNamespace(namespace);
      return true;
    } catch (error) {
      if (this.isNotFoundError(error)) {
        return false;
      }
      throw error;
    }
  }

  /**
   * Update namespace properties.
   */
  async updateNamespaceProperties(
    namespace: string[],
    updates: Record<string, string>,
    removals: string[]
  ): Promise<CatalogNamespace> {
    this.validateNamespace(namespace);

    const encodedNamespace = this.encodeNamespace(namespace);
    const result = await this.request<{ namespace: string[]; properties?: Record<string, string> }>(
      'POST',
      `namespaces/${encodedNamespace}`,
      {
        body: {
          updates,
          removals,
        },
      }
    );

    return {
      namespace: result.namespace,
      properties: result.properties,
    };
  }

  /**
   * Drop a namespace.
   */
  async dropNamespace(namespace: string[]): Promise<boolean> {
    this.validateNamespace(namespace);

    try {
      const encodedNamespace = this.encodeNamespace(namespace);
      await this.request<null>('DELETE', `namespaces/${encodedNamespace}`);
      return true;
    } catch (error) {
      if (this.isNotFoundError(error)) {
        return false;
      }
      throw error;
    }
  }

  // ==========================================================================
  // Table Operations
  // ==========================================================================

  /**
   * List tables in a namespace.
   */
  async listTables(namespace: string[], pageToken?: string): Promise<ListTablesResponse> {
    const encodedNamespace = this.encodeNamespace(namespace);
    const queryParams: Record<string, string | undefined> = {};

    if (pageToken) {
      queryParams.pageToken = pageToken;
    }

    const result = await this.request<{
      identifiers: Array<{ namespace: string[]; name: string }>;
      nextPageToken?: string;
    }>('GET', `tables/${encodedNamespace}`, { queryParams });

    return {
      identifiers: result.identifiers,
      nextPageToken: result.nextPageToken,
    };
  }

  /**
   * Create a new table.
   */
  async createTable(request: RegisterTableRequest): Promise<CatalogTable> {
    this.validateRequiredString(request.name, 'name');
    this.validateRequiredString(request.location, 'location');
    this.validateNamespace(request.namespace);

    const encodedNamespace = this.encodeNamespace(request.namespace);
    const body: Record<string, unknown> = {
      name: request.name,
      location: request.location,
    };

    if (request.schema) {
      body.schema = request.schema;
    }
    if (request.partitionSpec) {
      body.partitionSpec = request.partitionSpec;
    }
    if (request.properties) {
      body.properties = request.properties;
    }

    const result = await this.request<{
      identifier: { namespace: string[]; name: string };
      location: string;
      metadataLocation: string;
      properties: Record<string, string>;
      metadata?: TableMetadata;
    }>('POST', `tables/${encodedNamespace}`, { body });

    return {
      identifier: result.identifier,
      location: result.location,
      metadataLocation: result.metadataLocation,
      properties: result.properties,
      metadata: result.metadata,
    };
  }

  /**
   * Load table metadata.
   */
  async loadTable(namespace: string[], name: string): Promise<CatalogTable> {
    const encodedNamespace = this.encodeNamespace(namespace);
    const result = await this.request<{
      identifier: { namespace: string[]; name: string };
      location: string;
      metadataLocation: string;
      properties: Record<string, string>;
      metadata?: TableMetadata;
    }>('GET', `tables/${encodedNamespace}/${encodeURIComponent(name)}`);

    return {
      identifier: result.identifier,
      location: result.location,
      metadataLocation: result.metadataLocation,
      properties: result.properties,
      metadata: result.metadata,
    };
  }

  /**
   * Check if a table exists.
   */
  async tableExists(namespace: string[], name: string): Promise<boolean> {
    try {
      await this.loadTable(namespace, name);
      return true;
    } catch (error) {
      if (this.isNotFoundError(error)) {
        return false;
      }
      throw error;
    }
  }

  /**
   * Update table location and properties.
   */
  async updateTableLocation(request: UpdateTableRequest): Promise<CatalogTable> {
    const encodedNamespace = this.encodeNamespace(request.namespace);
    const body: Record<string, unknown> = {};

    if (request.location) {
      body.location = request.location;
    }
    if (request.metadataLocation) {
      body.metadataLocation = request.metadataLocation;
    }
    if (request.properties) {
      body.properties = request.properties;
    }

    const result = await this.request<{
      identifier: { namespace: string[]; name: string };
      location: string;
      metadataLocation: string;
      properties: Record<string, string>;
      metadata?: TableMetadata;
    }>('PUT', `tables/${encodedNamespace}/${encodeURIComponent(request.name)}`, { body });

    return {
      identifier: result.identifier,
      location: result.location,
      metadataLocation: result.metadataLocation,
      properties: result.properties,
      metadata: result.metadata,
    };
  }

  /**
   * Drop a table.
   */
  async dropTable(namespace: string[], name: string, purge?: boolean): Promise<boolean> {
    try {
      const encodedNamespace = this.encodeNamespace(namespace);
      const queryParams: Record<string, string | undefined> = {};

      if (purge) {
        queryParams.purge = 'true';
      }

      await this.request<null>(
        'DELETE',
        `tables/${encodedNamespace}/${encodeURIComponent(name)}`,
        { queryParams }
      );
      return true;
    } catch (error) {
      if (this.isNotFoundError(error)) {
        return false;
      }
      throw error;
    }
  }

  /**
   * Rename a table.
   */
  async renameTable(
    fromNamespace: string[],
    fromName: string,
    toNamespace: string[],
    toName: string
  ): Promise<void> {
    await this.request<null>('POST', 'tables/rename', {
      body: {
        source: {
          namespace: fromNamespace,
          name: fromName,
        },
        destination: {
          namespace: toNamespace,
          name: toName,
        },
      },
    });
  }

  // ==========================================================================
  // MongoLake Convenience Methods
  // ==========================================================================

  /**
   * Register a MongoLake collection as an Iceberg table.
   */
  async registerCollection(
    database: string,
    collection: string,
    location: string,
    options?: {
      schema?: IcebergSchema;
      partitionSpec?: PartitionSpec;
      properties?: Record<string, string>;
    }
  ): Promise<CatalogTable> {
    const namespace = [MONGOLAKE_NAMESPACE, database];

    // Ensure namespace exists
    const exists = await this.namespaceExists(namespace);
    if (!exists) {
      await this.createNamespace(namespace);
    }

    // Build properties with MongoLake metadata
    const properties: Record<string, string> = {
      'mongolake.database': database,
      'mongolake.collection': collection,
      ...options?.properties,
    };

    return this.createTable({
      name: collection,
      namespace,
      location,
      schema: options?.schema,
      partitionSpec: options?.partitionSpec,
      properties,
    });
  }

  /**
   * Unregister a MongoLake collection.
   */
  async unregisterCollection(database: string, collection: string): Promise<boolean> {
    const namespace = [MONGOLAKE_NAMESPACE, database];
    return this.dropTable(namespace, collection);
  }

  /**
   * List all collections in a MongoLake database.
   */
  async listCollections(database: string): Promise<string[]> {
    const namespace = [MONGOLAKE_NAMESPACE, database];
    const response = await this.listTables(namespace);
    return response.identifiers.map((id) => id.name);
  }

  /**
   * List all MongoLake databases.
   */
  async listDatabases(): Promise<string[]> {
    const response = await this.listNamespaces([MONGOLAKE_NAMESPACE]);
    return response.namespaces.map((ns) => ns[ns.length - 1]!);
  }

  /**
   * Refresh a table's metadata location.
   */
  async refreshTable(
    database: string,
    collection: string,
    metadataLocation: string
  ): Promise<CatalogTable> {
    const namespace = [MONGOLAKE_NAMESPACE, database];
    return this.updateTableLocation({
      name: collection,
      namespace,
      metadataLocation,
    });
  }
}

// ============================================================================
// Factory Functions
// ============================================================================

/**
 * Create an R2 Data Catalog client from environment variables.
 *
 * Required environment variables:
 * - CF_ACCOUNT_ID: Cloudflare Account ID
 * - R2_DATA_CATALOG_TOKEN: R2 Data Catalog API token
 *
 * @returns R2DataCatalogClient instance
 * @throws Error if required environment variables are missing
 */
export function createR2DataCatalogClient(): R2DataCatalogClient {
  const accountId = process.env.CF_ACCOUNT_ID;
  const token = process.env.R2_DATA_CATALOG_TOKEN;

  if (!accountId || !token) {
    throw new Error(
      'Missing required environment variables: CF_ACCOUNT_ID and R2_DATA_CATALOG_TOKEN'
    );
  }

  return new R2DataCatalogClient({
    accountId,
    token,
  });
}

/**
 * Create an R2 Data Catalog client from configuration.
 *
 * @param config - Client configuration
 * @returns R2DataCatalogClient instance
 */
export function createCatalogClient(config: R2DataCatalogConfig): R2DataCatalogClient {
  return new R2DataCatalogClient(config);
}
