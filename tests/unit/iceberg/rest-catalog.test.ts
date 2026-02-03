/**
 * REST Catalog Unit Tests
 *
 * Tests for the Iceberg REST Catalog implementation.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  RestCatalog,
  RestCatalogError,
  NotFoundError,
  AlreadyExistsError,
  ValidationError,
  AuthenticationError,
  CommitFailedError,
  createRestCatalog,
  createRestCatalogFromEnv,
  generateMetadataPath,
  generateManifestListPath,
  generateManifestPath,
  generateDataFilePath,
  type RestCatalogConfig,
  type CreateTableRequest,
  type TableRequirement,
  type TableUpdate,
} from '../../../src/iceberg/rest-catalog.js';

// ============================================================================
// Test Fixtures
// ============================================================================

const TEST_CONFIG: RestCatalogConfig = {
  uri: 'https://catalog.example.com/api/v1',
  warehouse: 's3://test-bucket/warehouse',
  token: 'test-token',
  authType: 'bearer',
};

function createMockFetch(responses: Map<string, { status: number; body: unknown }>) {
  return vi.fn(async (url: string | URL, init?: RequestInit) => {
    const urlStr = url.toString();
    const method = init?.method ?? 'GET';

    // Find matching response by URL pattern
    for (const [pattern, response] of responses) {
      if (urlStr.includes(pattern) || `${method} ${urlStr}`.includes(pattern)) {
        return new Response(JSON.stringify(response.body), {
          status: response.status,
          headers: { 'Content-Type': 'application/json' },
        });
      }
    }

    // Default 404 response
    return new Response(
      JSON.stringify({
        error: { message: 'Not found', type: 'NoSuchNamespaceException', code: 404 },
      }),
      { status: 404, headers: { 'Content-Type': 'application/json' } }
    );
  });
}

function createTestSchema() {
  return {
    type: 'struct' as const,
    'schema-id': 0,
    fields: [
      { id: 1, name: '_id', type: 'string' as const, required: true },
      { id: 2, name: 'name', type: 'string' as const, required: false },
      { id: 3, name: 'created_at', type: 'timestamp' as const, required: false },
    ],
  };
}

function createTestPartitionSpec() {
  return {
    'spec-id': 0,
    fields: [
      { 'source-id': 3, 'field-id': 1000, name: 'day', transform: 'day' },
    ],
  };
}

// ============================================================================
// Path Generation Tests
// ============================================================================

describe('Path Generation Utilities', () => {
  describe('generateMetadataPath', () => {
    it('should generate metadata path without UUID', () => {
      const path = generateMetadataPath('s3://bucket/table', 1);
      expect(path).toBe('s3://bucket/table/metadata/v1.metadata.json');
    });

    it('should generate metadata path with UUID', () => {
      const path = generateMetadataPath('s3://bucket/table', 2, 'abc-123');
      expect(path).toBe('s3://bucket/table/metadata/v2-abc-123.metadata.json');
    });

    it('should handle version numbers correctly', () => {
      expect(generateMetadataPath('s3://bucket/table', 0)).toContain('v0.metadata.json');
      expect(generateMetadataPath('s3://bucket/table', 100)).toContain('v100.metadata.json');
    });
  });

  describe('generateManifestListPath', () => {
    it('should generate manifest list path', () => {
      const path = generateManifestListPath('s3://bucket/table', 123456789n);
      expect(path).toBe('s3://bucket/table/metadata/snap-123456789.avro');
    });

    it('should include attempt ID when provided', () => {
      const path = generateManifestListPath('s3://bucket/table', 123456789n, 1);
      expect(path).toBe('s3://bucket/table/metadata/snap-123456789-1.avro');
    });
  });

  describe('generateManifestPath', () => {
    it('should generate manifest path', () => {
      const path = generateManifestPath('s3://bucket/table', 'manifest-abc-123');
      expect(path).toBe('s3://bucket/table/metadata/manifest-abc-123.avro');
    });
  });

  describe('generateDataFilePath', () => {
    it('should generate data file path without partition', () => {
      const path = generateDataFilePath('s3://bucket/table', null, 'file-123');
      expect(path).toBe('s3://bucket/table/data/file-123.parquet');
    });

    it('should generate data file path with partition', () => {
      const path = generateDataFilePath('s3://bucket/table', 'day=2024-01-01', 'file-123');
      expect(path).toBe('s3://bucket/table/data/day=2024-01-01/file-123.parquet');
    });
  });
});

// ============================================================================
// RestCatalog Initialization Tests
// ============================================================================

describe('RestCatalog - Initialization', () => {
  describe('constructor', () => {
    it('should create catalog with valid config', () => {
      const mockFetch = createMockFetch(new Map());
      const catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });
      expect(catalog).toBeInstanceOf(RestCatalog);
    });

    it('should throw when URI is missing', () => {
      expect(() => new RestCatalog({ uri: '' })).toThrow(ValidationError);
    });

    it('should throw when URI is whitespace only', () => {
      expect(() => new RestCatalog({ uri: '   ' })).toThrow(ValidationError);
    });

    it('should normalize URI by removing trailing slash', async () => {
      const mockFetch = createMockFetch(
        new Map([['namespaces', { status: 200, body: { namespaces: [] } }]])
      );

      const catalog = new RestCatalog({
        uri: 'https://catalog.example.com/api/v1/',
        fetch: mockFetch,
      });

      await catalog.listNamespaces();
      expect(mockFetch).toHaveBeenCalledWith(
        expect.stringContaining('catalog.example.com/api/v1/namespaces'),
        expect.any(Object)
      );
    });
  });

  describe('factory functions', () => {
    it('should create catalog with createRestCatalog', () => {
      const mockFetch = createMockFetch(new Map());
      const catalog = createRestCatalog({ ...TEST_CONFIG, fetch: mockFetch });
      expect(catalog).toBeInstanceOf(RestCatalog);
    });

    it('should create catalog from environment variables', () => {
      const originalEnv = process.env;
      process.env = {
        ...originalEnv,
        ICEBERG_REST_CATALOG_URI: 'https://catalog.example.com/api/v1',
        ICEBERG_WAREHOUSE: 's3://bucket/warehouse',
        ICEBERG_REST_TOKEN: 'env-token',
      };

      try {
        const catalog = createRestCatalogFromEnv();
        expect(catalog).toBeInstanceOf(RestCatalog);
      } finally {
        process.env = originalEnv;
      }
    });

    it('should throw when environment URI is missing', () => {
      const originalEnv = process.env;
      process.env = { ...originalEnv };
      delete process.env.ICEBERG_REST_CATALOG_URI;

      try {
        expect(() => createRestCatalogFromEnv()).toThrow(/ICEBERG_REST_CATALOG_URI/);
      } finally {
        process.env = originalEnv;
      }
    });
  });
});

// ============================================================================
// Namespace Operations Tests
// ============================================================================

describe('RestCatalog - Namespace Operations', () => {
  let catalog: RestCatalog;
  let mockFetch: ReturnType<typeof createMockFetch>;

  beforeEach(() => {
    mockFetch = createMockFetch(new Map());
    catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });
  });

  describe('listNamespaces', () => {
    it('should list all namespaces', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 200,
              body: { namespaces: [['db1'], ['db2'], ['db3']] },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const response = await catalog.listNamespaces();

      expect(response.namespaces).toHaveLength(3);
      expect(response.namespaces).toContainEqual(['db1']);
    });

    it('should list namespaces under parent', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 200,
              body: { namespaces: [['mongolake', 'db1'], ['mongolake', 'db2']] },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const response = await catalog.listNamespaces(['mongolake']);

      expect(response.namespaces).toHaveLength(2);
      expect(mockFetch).toHaveBeenCalledWith(
        expect.stringContaining('parent='),
        expect.any(Object)
      );
    });

    it('should handle pagination', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 200,
              body: { namespaces: [['page2']], nextPageToken: 'next-token' },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const response = await catalog.listNamespaces(undefined, 'page-token');

      expect(response.nextPageToken).toBe('next-token');
      expect(mockFetch).toHaveBeenCalledWith(
        expect.stringContaining('pageToken=page-token'),
        expect.any(Object)
      );
    });
  });

  describe('createNamespace', () => {
    it('should create a namespace', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 200,
              body: { namespace: ['production'], properties: {} },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const response = await catalog.createNamespace(['production']);

      expect(response.namespace).toEqual(['production']);
      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({ method: 'POST' })
      );
    });

    it('should create namespace with properties', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 200,
              body: {
                namespace: ['production'],
                properties: { owner: 'data-team', location: 's3://bucket/prod' },
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const response = await catalog.createNamespace(['production'], {
        owner: 'data-team',
        location: 's3://bucket/prod',
      });

      expect(response.properties).toEqual({
        owner: 'data-team',
        location: 's3://bucket/prod',
      });
    });

    it('should throw when namespace is empty', async () => {
      await expect(catalog.createNamespace([])).rejects.toThrow(ValidationError);
    });

    it('should throw when namespace part is empty', async () => {
      await expect(catalog.createNamespace(['db', ''])).rejects.toThrow(ValidationError);
    });

    it('should throw when namespace contains invalid characters', async () => {
      await expect(catalog.createNamespace(['invalid/name'])).rejects.toThrow(ValidationError);
    });

    it('should throw AlreadyExistsError when namespace exists', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 409,
              body: {
                error: {
                  message: 'Namespace already exists',
                  type: 'AlreadyExistsException',
                  code: 409,
                },
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      await expect(catalog.createNamespace(['existing'])).rejects.toThrow(AlreadyExistsError);
    });
  });

  describe('getNamespace', () => {
    it('should get namespace metadata', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'namespaces/production',
            {
              status: 200,
              body: { namespace: ['production'], properties: { owner: 'team-a' } },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const namespace = await catalog.getNamespace(['production']);

      expect(namespace.namespace).toEqual(['production']);
      expect(namespace.properties).toEqual({ owner: 'team-a' });
    });

    it('should throw NotFoundError when namespace does not exist', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 404,
              body: {
                error: { message: 'Namespace not found', type: 'NoSuchNamespaceException', code: 404 },
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      await expect(catalog.getNamespace(['nonexistent'])).rejects.toThrow(NotFoundError);
    });
  });

  describe('namespaceExists', () => {
    it('should return true when namespace exists', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'namespaces/production',
            { status: 200, body: {} },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const exists = await catalog.namespaceExists(['production']);
      expect(exists).toBe(true);
    });

    it('should return false when namespace does not exist', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 404,
              body: {
                error: { message: 'Not found', type: 'NoSuchNamespaceException', code: 404 },
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const exists = await catalog.namespaceExists(['nonexistent']);
      expect(exists).toBe(false);
    });
  });

  describe('updateNamespaceProperties', () => {
    it('should update namespace properties', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'properties',
            {
              status: 200,
              body: { updated: ['owner', 'description'], removed: [], missing: [] },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const response = await catalog.updateNamespaceProperties(
        ['production'],
        { owner: 'new-owner', description: 'Updated description' },
        []
      );

      expect(response.updated).toContain('owner');
      expect(response.updated).toContain('description');
    });

    it('should remove namespace properties', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'properties',
            {
              status: 200,
              body: { updated: [], removed: ['deprecated'], missing: [] },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const response = await catalog.updateNamespaceProperties(
        ['production'],
        {},
        ['deprecated']
      );

      expect(response.removed).toContain('deprecated');
    });
  });

  describe('dropNamespace', () => {
    it('should drop a namespace', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'DELETE',
            { status: 200, body: {} },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      await expect(catalog.dropNamespace(['production'])).resolves.not.toThrow();
      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({ method: 'DELETE' })
      );
    });

    it('should throw NotFoundError when namespace does not exist', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 404,
              body: {
                error: { message: 'Not found', type: 'NoSuchNamespaceException', code: 404 },
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      await expect(catalog.dropNamespace(['nonexistent'])).rejects.toThrow(NotFoundError);
    });
  });
});

// ============================================================================
// Table Operations Tests
// ============================================================================

describe('RestCatalog - Table Operations', () => {
  let catalog: RestCatalog;
  let mockFetch: ReturnType<typeof createMockFetch>;

  beforeEach(() => {
    mockFetch = createMockFetch(new Map());
    catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });
  });

  describe('listTables', () => {
    it('should list tables in namespace', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'tables',
            {
              status: 200,
              body: {
                identifiers: [
                  { namespace: ['db'], name: 'users' },
                  { namespace: ['db'], name: 'orders' },
                ],
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const response = await catalog.listTables(['db']);

      expect(response.identifiers).toHaveLength(2);
      expect(response.identifiers.map((id) => id.name)).toContain('users');
    });

    it('should handle pagination', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'tables',
            {
              status: 200,
              body: {
                identifiers: [{ namespace: ['db'], name: 'table1' }],
                nextPageToken: 'next-page',
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const response = await catalog.listTables(['db'], 'current-page', 10);

      expect(response.nextPageToken).toBe('next-page');
      expect(mockFetch).toHaveBeenCalledWith(
        expect.stringContaining('pageToken=current-page'),
        expect.any(Object)
      );
    });
  });

  describe('createTable', () => {
    it('should create a table', async () => {
      const tableMetadata = {
        'format-version': 2,
        'table-uuid': 'test-uuid',
        location: 's3://bucket/warehouse/db/users',
        'last-updated-ms': Date.now(),
        'last-column-id': 3,
        'current-schema-id': 0,
        schemas: [createTestSchema()],
        'default-spec-id': 0,
        'partition-specs': [{ 'spec-id': 0, fields: [] }],
        'default-sort-order-id': 0,
        'sort-orders': [{ 'order-id': 0, fields: [] }],
        properties: {},
        'current-snapshot-id': -1,
        snapshots: [],
      };

      mockFetch = createMockFetch(
        new Map([
          [
            'tables',
            {
              status: 200,
              body: {
                metadataLocation: 's3://bucket/warehouse/db/users/metadata/v1.metadata.json',
                metadata: tableMetadata,
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const request: CreateTableRequest = {
        name: 'users',
        location: 's3://bucket/warehouse/db/users',
        schema: createTestSchema(),
      };

      const response = await catalog.createTable(['db'], request);

      expect(response.metadata['table-uuid']).toBe('test-uuid');
      expect(response.metadataLocation).toContain('v1.metadata.json');
    });

    it('should create table with partition spec', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'tables',
            {
              status: 200,
              body: {
                metadataLocation: 's3://bucket/metadata/v1.metadata.json',
                metadata: {
                  'format-version': 2,
                  'table-uuid': 'test-uuid',
                  location: 's3://bucket/table',
                  schemas: [createTestSchema()],
                  'partition-specs': [createTestPartitionSpec()],
                },
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const request: CreateTableRequest = {
        name: 'events',
        schema: createTestSchema(),
        partitionSpec: createTestPartitionSpec(),
      };

      await catalog.createTable(['db'], request);

      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          body: expect.stringContaining('partitionSpec'),
        })
      );
    });

    it('should throw when table name is empty', async () => {
      const request: CreateTableRequest = {
        name: '',
        schema: createTestSchema(),
      };

      await expect(catalog.createTable(['db'], request)).rejects.toThrow(ValidationError);
    });

    it('should throw AlreadyExistsError when table exists', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'tables',
            {
              status: 409,
              body: {
                error: {
                  message: 'Table already exists',
                  type: 'AlreadyExistsException',
                  code: 409,
                },
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const request: CreateTableRequest = {
        name: 'existing',
        schema: createTestSchema(),
      };

      await expect(catalog.createTable(['db'], request)).rejects.toThrow(AlreadyExistsError);
    });
  });

  describe('loadTable', () => {
    it('should load table metadata', async () => {
      const tableMetadata = {
        'format-version': 2,
        'table-uuid': 'load-test-uuid',
        location: 's3://bucket/warehouse/db/users',
        'current-snapshot-id': 12345,
        schemas: [createTestSchema()],
      };

      mockFetch = createMockFetch(
        new Map([
          [
            '/tables/',
            {
              status: 200,
              body: {
                metadataLocation: 's3://bucket/metadata/v5.metadata.json',
                metadata: tableMetadata,
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const response = await catalog.loadTable(['db'], 'users');

      expect(response.metadata['table-uuid']).toBe('load-test-uuid');
      expect(response.metadataLocation).toContain('v5.metadata.json');
    });

    it('should load table at specific snapshot', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'tables',
            {
              status: 200,
              body: {
                metadataLocation: 's3://bucket/metadata/v1.metadata.json',
                metadata: { 'format-version': 2 },
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      await catalog.loadTable(['db'], 'users', 12345n);

      expect(mockFetch).toHaveBeenCalledWith(
        expect.stringContaining('snapshots=12345'),
        expect.any(Object)
      );
    });

    it('should throw NotFoundError when table does not exist', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'tables',
            {
              status: 404,
              body: {
                error: { message: 'Table not found', type: 'NoSuchTableException', code: 404 },
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      await expect(catalog.loadTable(['db'], 'nonexistent')).rejects.toThrow(NotFoundError);
    });
  });

  describe('tableExists', () => {
    it('should return true when table exists', async () => {
      mockFetch = createMockFetch(
        new Map([['tables/', { status: 200, body: {} }]])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const exists = await catalog.tableExists(['db'], 'users');
      expect(exists).toBe(true);
    });

    it('should return false when table does not exist', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'tables',
            {
              status: 404,
              body: {
                error: { message: 'Not found', type: 'NoSuchTableException', code: 404 },
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const exists = await catalog.tableExists(['db'], 'nonexistent');
      expect(exists).toBe(false);
    });
  });

  describe('updateTable', () => {
    it('should update table with requirements and updates', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'POST',
            {
              status: 200,
              body: {
                metadataLocation: 's3://bucket/metadata/v2.metadata.json',
                metadata: { 'format-version': 2 },
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const requirements: TableRequirement[] = [
        { type: 'assert-table-uuid', uuid: 'test-uuid' },
      ];

      const updates: TableUpdate[] = [
        { action: 'set-properties', updates: { 'write.format.default': 'parquet' } },
      ];

      const response = await catalog.updateTable(['db'], 'users', requirements, updates);

      expect(response.metadataLocation).toContain('v2.metadata.json');
    });

    it('should throw CommitFailedError on conflict', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'tables',
            {
              status: 409,
              body: {
                error: {
                  message: 'Commit conflict',
                  type: 'CommitFailedException',
                  code: 409,
                },
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      await expect(
        catalog.updateTable(['db'], 'users', [], [])
      ).rejects.toThrow(CommitFailedError);
    });
  });

  describe('dropTable', () => {
    it('should drop a table', async () => {
      mockFetch = createMockFetch(
        new Map([['DELETE', { status: 200, body: {} }]])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      await expect(catalog.dropTable(['db'], 'users')).resolves.not.toThrow();
      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({ method: 'DELETE' })
      );
    });

    it('should drop table with purge option', async () => {
      mockFetch = createMockFetch(
        new Map([['DELETE', { status: 200, body: {} }]])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      await catalog.dropTable(['db'], 'users', true);

      expect(mockFetch).toHaveBeenCalledWith(
        expect.stringContaining('purgeRequested=true'),
        expect.any(Object)
      );
    });
  });

  describe('renameTable', () => {
    it('should rename a table', async () => {
      mockFetch = createMockFetch(
        new Map([['rename', { status: 200, body: {} }]])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      await catalog.renameTable(
        { namespace: ['db'], name: 'old_name' },
        { namespace: ['db'], name: 'new_name' }
      );

      expect(mockFetch).toHaveBeenCalledWith(
        expect.stringContaining('rename'),
        expect.objectContaining({
          method: 'POST',
          body: expect.stringContaining('old_name'),
        })
      );
    });

    it('should move table to different namespace', async () => {
      mockFetch = createMockFetch(
        new Map([['rename', { status: 200, body: {} }]])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      await catalog.renameTable(
        { namespace: ['staging'], name: 'users' },
        { namespace: ['production'], name: 'users' }
      );

      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          body: expect.stringContaining('production'),
        })
      );
    });
  });

  describe('registerTable', () => {
    it('should register an existing metadata file as a table', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'register',
            {
              status: 200,
              body: {
                metadataLocation: 's3://bucket/metadata/existing.metadata.json',
                metadata: { 'format-version': 2 },
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const response = await catalog.registerTable(
        ['db'],
        'imported_table',
        's3://bucket/metadata/existing.metadata.json'
      );

      expect(response.metadataLocation).toContain('existing.metadata.json');
    });
  });
});

// ============================================================================
// MongoLake Convenience Methods Tests
// ============================================================================

describe('RestCatalog - MongoLake Methods', () => {
  let catalog: RestCatalog;
  let mockFetch: ReturnType<typeof createMockFetch>;

  beforeEach(() => {
    mockFetch = createMockFetch(new Map());
    catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });
  });

  describe('registerMongoLakeCollection', () => {
    it('should register a collection with automatic namespace creation', async () => {
      const callSequence: string[] = [];

      mockFetch = vi.fn(async (url: string, init?: RequestInit) => {
        const method = init?.method ?? 'GET';
        callSequence.push(`${method} ${url}`);

        // Namespace existence check (HEAD)
        if (method === 'HEAD' && url.includes('namespaces')) {
          if (url.includes('mongolake%1Fmydb')) {
            return new Response(null, { status: 404 });
          }
          if (url.includes('mongolake')) {
            return new Response(null, { status: 200 });
          }
        }

        // Create namespace
        if (method === 'POST' && url.includes('namespaces') && !url.includes('tables')) {
          return new Response(
            JSON.stringify({ namespace: ['mongolake', 'mydb'], properties: {} }),
            { status: 200, headers: { 'Content-Type': 'application/json' } }
          );
        }

        // Create table
        if (method === 'POST' && url.includes('tables')) {
          return new Response(
            JSON.stringify({
              metadataLocation: 's3://bucket/metadata/v1.metadata.json',
              metadata: {
                'format-version': 2,
                properties: {
                  'mongolake.database': 'mydb',
                  'mongolake.collection': 'users',
                },
              },
            }),
            { status: 200, headers: { 'Content-Type': 'application/json' } }
          );
        }

        return new Response(JSON.stringify({}), { status: 404 });
      });

      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const response = await catalog.registerMongoLakeCollection(
        'mydb',
        'users',
        's3://bucket/warehouse/mongolake/mydb/users',
        createTestSchema()
      );

      expect(response.metadata.properties?.['mongolake.database']).toBe('mydb');
      expect(response.metadata.properties?.['mongolake.collection']).toBe('users');
    });
  });

  describe('listMongoLakeDatabases', () => {
    it('should list databases under mongolake namespace', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 200,
              body: {
                namespaces: [
                  ['mongolake', 'db1'],
                  ['mongolake', 'db2'],
                  ['mongolake', 'production'],
                ],
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const databases = await catalog.listMongoLakeDatabases();

      expect(databases).toContain('db1');
      expect(databases).toContain('db2');
      expect(databases).toContain('production');
    });

    it('should return empty array when mongolake namespace does not exist', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 404,
              body: {
                error: { message: 'Not found', type: 'NoSuchNamespaceException', code: 404 },
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const databases = await catalog.listMongoLakeDatabases();

      expect(databases).toEqual([]);
    });
  });

  describe('listMongoLakeCollections', () => {
    it('should list collections in a database', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'tables',
            {
              status: 200,
              body: {
                identifiers: [
                  { namespace: ['mongolake', 'mydb'], name: 'users' },
                  { namespace: ['mongolake', 'mydb'], name: 'orders' },
                ],
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const collections = await catalog.listMongoLakeCollections('mydb');

      expect(collections).toContain('users');
      expect(collections).toContain('orders');
    });
  });

  describe('loadMongoLakeCollection', () => {
    it('should load collection metadata', async () => {
      mockFetch = createMockFetch(
        new Map([
          [
            'tables',
            {
              status: 200,
              body: {
                metadataLocation: 's3://bucket/metadata/v1.metadata.json',
                metadata: {
                  'format-version': 2,
                  properties: { 'mongolake.collection': 'users' },
                },
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const response = await catalog.loadMongoLakeCollection('mydb', 'users');

      expect(response.metadata.properties?.['mongolake.collection']).toBe('users');
    });
  });

  describe('dropMongoLakeCollection', () => {
    it('should drop a collection', async () => {
      mockFetch = createMockFetch(
        new Map([['DELETE', { status: 200, body: {} }]])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      await expect(
        catalog.dropMongoLakeCollection('mydb', 'users')
      ).resolves.not.toThrow();
    });
  });
});

// ============================================================================
// Error Handling Tests
// ============================================================================

describe('RestCatalog - Error Handling', () => {
  let catalog: RestCatalog;

  describe('HTTP errors', () => {
    it('should throw AuthenticationError for 401', async () => {
      const mockFetch = createMockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 401,
              body: {
                error: { message: 'Unauthorized', type: 'NotAuthorizedException', code: 401 },
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      await expect(catalog.listNamespaces()).rejects.toThrow(AuthenticationError);
    });

    it('should throw NotFoundError for 404', async () => {
      const mockFetch = createMockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 404,
              body: {
                error: { message: 'Not found', type: 'NoSuchNamespaceException', code: 404 },
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      await expect(catalog.getNamespace(['missing'])).rejects.toThrow(NotFoundError);
    });

    it('should throw ValidationError for 400', async () => {
      const mockFetch = createMockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 400,
              body: {
                error: { message: 'Bad request', type: 'BadRequestException', code: 400 },
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      await expect(catalog.createNamespace(['test'])).rejects.toThrow(ValidationError);
    });

    it('should throw RestCatalogError for 500', async () => {
      const mockFetch = createMockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 500,
              body: {
                error: { message: 'Internal error', type: 'InternalServerException', code: 500 },
              },
            },
          ],
        ])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      await expect(catalog.listNamespaces()).rejects.toThrow(RestCatalogError);
    });
  });

  describe('Network errors', () => {
    it('should handle timeout errors', async () => {
      const mockFetch = vi.fn(() => {
        const error = new Error('The operation was aborted');
        error.name = 'AbortError';
        return Promise.reject(error);
      });

      catalog = new RestCatalog({
        ...TEST_CONFIG,
        fetch: mockFetch,
        timeoutMs: 100,
      });

      await expect(catalog.listNamespaces()).rejects.toThrow(/timeout/i);
    });

    it('should propagate network errors', async () => {
      const mockFetch = vi.fn(() => Promise.reject(new Error('Network error')));
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      await expect(catalog.listNamespaces()).rejects.toThrow('Network error');
    });
  });
});

// ============================================================================
// Authentication Tests
// ============================================================================

describe('RestCatalog - Authentication', () => {
  describe('Bearer token', () => {
    it('should include bearer token in requests', async () => {
      const mockFetch = createMockFetch(
        new Map([['namespaces', { status: 200, body: { namespaces: [] } }]])
      );

      const catalog = new RestCatalog({
        ...TEST_CONFIG,
        authType: 'bearer',
        token: 'my-bearer-token',
        fetch: mockFetch,
      });

      await catalog.listNamespaces();

      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          headers: expect.objectContaining({
            Authorization: 'Bearer my-bearer-token',
          }),
        })
      );
    });
  });

  describe('Custom headers', () => {
    it('should include custom headers in requests', async () => {
      const mockFetch = createMockFetch(
        new Map([['namespaces', { status: 200, body: { namespaces: [] } }]])
      );

      const catalog = new RestCatalog({
        uri: TEST_CONFIG.uri,
        headers: {
          'X-Custom-Header': 'custom-value',
          'X-Request-ID': '12345',
        },
        fetch: mockFetch,
      });

      await catalog.listNamespaces();

      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          headers: expect.objectContaining({
            'X-Custom-Header': 'custom-value',
            'X-Request-ID': '12345',
          }),
        })
      );
    });
  });
});

// ============================================================================
// Transaction Tests
// ============================================================================

describe('RestCatalog - Transactions', () => {
  let catalog: RestCatalog;

  describe('commitTransaction', () => {
    it('should commit multiple table updates atomically', async () => {
      const mockFetch = createMockFetch(
        new Map([['transactions/commit', { status: 200, body: {} }]])
      );
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const tableCommits = [
        {
          identifier: { namespace: ['db'], name: 'table1' },
          requirements: [{ type: 'assert-table-uuid' as const, uuid: 'uuid-1' }],
          updates: [{ action: 'set-properties' as const, updates: { key: 'value' } }],
        },
        {
          identifier: { namespace: ['db'], name: 'table2' },
          requirements: [{ type: 'assert-table-uuid' as const, uuid: 'uuid-2' }],
          updates: [{ action: 'set-properties' as const, updates: { key: 'value' } }],
        },
      ];

      await expect(catalog.commitTransaction(tableCommits)).resolves.not.toThrow();

      expect(mockFetch).toHaveBeenCalledWith(
        expect.stringContaining('transactions/commit'),
        expect.objectContaining({
          method: 'POST',
          body: expect.stringContaining('table-changes'),
        })
      );
    });

    it('should validate all table identifiers', async () => {
      const mockFetch = createMockFetch(new Map());
      catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });

      const tableCommits = [
        {
          identifier: { namespace: [], name: 'table1' }, // Invalid empty namespace
          requirements: [],
          updates: [],
        },
      ];

      await expect(catalog.commitTransaction(tableCommits)).rejects.toThrow(ValidationError);
    });
  });
});

// ============================================================================
// Catalog Configuration Tests
// ============================================================================

describe('RestCatalog - Configuration', () => {
  it('should fetch catalog configuration', async () => {
    const mockFetch = createMockFetch(
      new Map([
        [
          'config',
          {
            status: 200,
            body: {
              defaults: { 'write.format.default': 'parquet' },
              overrides: { 'commit.retry.num-retries': '4' },
            },
          },
        ],
      ])
    );

    const catalog = new RestCatalog({ ...TEST_CONFIG, fetch: mockFetch });
    const config = await catalog.getConfig();

    expect(config.defaults?.['write.format.default']).toBe('parquet');
    expect(config.overrides?.['commit.retry.num-retries']).toBe('4');
  });

  it('should include warehouse in config request', async () => {
    const mockFetch = createMockFetch(
      new Map([['config', { status: 200, body: {} }]])
    );

    const catalog = new RestCatalog({
      ...TEST_CONFIG,
      warehouse: 's3://my-bucket/warehouse',
      fetch: mockFetch,
    });

    await catalog.getConfig();

    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('warehouse='),
      expect.any(Object)
    );
  });
});
