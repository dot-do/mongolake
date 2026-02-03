/**
 * Iceberg Catalog Client Tests (TDD RED Phase)
 *
 * Tests for the R2 Data Catalog client integration with Iceberg tables.
 * These tests should FAIL initially - they define the expected API.
 *
 * Requirements from mongolake-qkk.7.1:
 * - test table registration
 * - test table listing
 * - test namespace operations
 * - test table metadata fetching
 * - test commit operations
 *
 * The R2DataCatalogClient provides integration with Cloudflare R2 Data Catalog API
 * for managing Iceberg table metadata, enabling query engines like DuckDB, Spark,
 * and Trino to discover and query Iceberg tables stored in R2.
 *
 * @see https://developers.cloudflare.com/r2/data-catalog/
 * @see https://iceberg.apache.org/spec/#catalog-api
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
// Import catalog client types and classes - these may not exist yet (TDD RED phase)
// @ts-expect-error - CatalogClient may not exist yet
import {
  R2DataCatalogClient,
  createR2DataCatalogClient,
  createCatalogClient,
  R2DataCatalogError,
  type R2DataCatalogConfig,
  type CatalogNamespace,
  type RegisterTableRequest,
  type UpdateTableRequest,
  type CatalogTable,
  type ListTablesResponse,
  type ListNamespacesResponse,
} from '../../../src/iceberg/catalog-client.js';

// ============================================================================
// Test Fixtures and Helpers
// ============================================================================

/** Default test configuration */
const TEST_CONFIG: R2DataCatalogConfig = {
  accountId: 'test-account-id',
  token: 'test-token',
  baseUrl: 'https://api.test.cloudflare.com',
};

/** Mock fetch for API requests */
function mockFetch(responses: Map<string, { status: number; body: unknown }>) {
  return vi.fn(async (url: string | URL, init?: RequestInit) => {
    const urlStr = url.toString();
    const method = init?.method ?? 'GET';
    const key = `${method} ${urlStr}`;

    // Find matching response by URL pattern
    for (const [pattern, response] of responses) {
      if (urlStr.includes(pattern) || key.includes(pattern)) {
        return new Response(JSON.stringify(response.body), {
          status: response.status,
          headers: { 'Content-Type': 'application/json' },
        });
      }
    }

    // Default 404 response
    return new Response(
      JSON.stringify({ success: false, error: { code: 'NOT_FOUND', message: 'Not found' } }),
      { status: 404, headers: { 'Content-Type': 'application/json' } }
    );
  });
}

/** Create a test schema for tables */
function createTestSchema() {
  return {
    'schema-id': 0,
    type: 'struct' as const,
    fields: [
      { id: 1, name: '_id', type: 'string', required: true },
      { id: 2, name: 'name', type: 'string', required: false },
      { id: 3, name: 'createdAt', type: 'timestamp', required: false },
    ],
  };
}

/** Create a test partition spec */
function createTestPartitionSpec() {
  return {
    'spec-id': 0,
    fields: [
      { 'source-id': 3, 'field-id': 1000, name: 'day', transform: 'day' },
    ],
  };
}

// ============================================================================
// 1. Client Initialization
// ============================================================================

describe('R2DataCatalogClient - Initialization', () => {
  describe('constructor', () => {
    it('should create client with valid configuration', () => {
      const client = new R2DataCatalogClient(TEST_CONFIG);

      expect(client).toBeDefined();
      expect(client).toBeInstanceOf(R2DataCatalogClient);
    });

    it('should require accountId', () => {
      expect(() => {
        new R2DataCatalogClient({
          ...TEST_CONFIG,
          accountId: '',
        });
      }).toThrow(/accountId.*required/i);
    });

    it('should require token', () => {
      expect(() => {
        new R2DataCatalogClient({
          ...TEST_CONFIG,
          token: '',
        });
      }).toThrow(/token.*required/i);
    });

    it('should use default Cloudflare API URL when baseUrl not specified', () => {
      const client = new R2DataCatalogClient({
        accountId: 'test-account-id',
        token: 'test-token',
      });

      expect(client).toBeDefined();
      // Internal URL should default to Cloudflare API
    });

    it('should accept custom baseUrl', () => {
      const client = new R2DataCatalogClient({
        ...TEST_CONFIG,
        baseUrl: 'https://custom.api.example.com',
      });

      expect(client).toBeDefined();
    });
  });

  describe('factory functions', () => {
    it('should create client from configuration', () => {
      const client = createCatalogClient(TEST_CONFIG);

      expect(client).toBeDefined();
      expect(client).toBeInstanceOf(R2DataCatalogClient);
    });

    it('should create client from environment variables', () => {
      // Mock environment variables
      const originalEnv = process.env;
      process.env = {
        ...originalEnv,
        CF_ACCOUNT_ID: 'env-account-id',
        R2_DATA_CATALOG_TOKEN: 'env-token',
      };

      try {
        const client = createR2DataCatalogClient();
        expect(client).toBeDefined();
        expect(client).toBeInstanceOf(R2DataCatalogClient);
      } finally {
        process.env = originalEnv;
      }
    });

    it('should throw when environment variables are missing', () => {
      const originalEnv = process.env;
      process.env = { ...originalEnv };
      delete process.env.CF_ACCOUNT_ID;
      delete process.env.R2_DATA_CATALOG_TOKEN;

      try {
        expect(() => createR2DataCatalogClient()).toThrow(/environment variable/i);
      } finally {
        process.env = originalEnv;
      }
    });
  });
});

// ============================================================================
// 2. Namespace Operations
// ============================================================================

describe('R2DataCatalogClient - Namespace Operations', () => {
  let client: R2DataCatalogClient;
  let fetchMock: ReturnType<typeof mockFetch>;

  beforeEach(() => {
    client = new R2DataCatalogClient(TEST_CONFIG);
    fetchMock = mockFetch(new Map());
    vi.stubGlobal('fetch', fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  describe('listNamespaces', () => {
    it('should list all namespaces', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  namespaces: [['mongolake'], ['mongolake', 'production']],
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const response = await client.listNamespaces();

      expect(response.namespaces).toHaveLength(2);
      expect(response.namespaces).toContainEqual(['mongolake']);
      expect(response.namespaces).toContainEqual(['mongolake', 'production']);
    });

    it('should list namespaces under a parent', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  namespaces: [['mongolake', 'production'], ['mongolake', 'staging']],
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const response = await client.listNamespaces(['mongolake']);

      expect(response.namespaces).toHaveLength(2);
      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringContaining('parent=mongolake'),
        expect.any(Object)
      );
    });

    it('should handle pagination', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  namespaces: [['page2-ns']],
                  nextPageToken: undefined,
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const response = await client.listNamespaces(undefined, 'page-token-1');

      expect(response.namespaces).toBeDefined();
      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringContaining('pageToken=page-token-1'),
        expect.any(Object)
      );
    });

    it('should return empty array when no namespaces exist', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 200,
              body: {
                success: true,
                result: { namespaces: [] },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const response = await client.listNamespaces();

      expect(response.namespaces).toEqual([]);
    });
  });

  describe('createNamespace', () => {
    it('should create a namespace', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  namespace: ['mongolake', 'production'],
                  properties: {},
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const namespace = await client.createNamespace(['mongolake', 'production']);

      expect(namespace.namespace).toEqual(['mongolake', 'production']);
      expect(fetchMock).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          method: 'POST',
          body: expect.stringContaining('mongolake'),
        })
      );
    });

    it('should create namespace with properties', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  namespace: ['mongolake'],
                  properties: { owner: 'data-team', location: 's3://bucket/warehouse' },
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const namespace = await client.createNamespace(['mongolake'], {
        owner: 'data-team',
        location: 's3://bucket/warehouse',
      });

      expect(namespace.properties).toEqual({
        owner: 'data-team',
        location: 's3://bucket/warehouse',
      });
    });

    it('should throw when namespace already exists', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 409,
              body: {
                success: false,
                error: { code: 'ALREADY_EXISTS', message: 'Namespace already exists' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await expect(client.createNamespace(['mongolake'])).rejects.toThrow(R2DataCatalogError);
      await expect(client.createNamespace(['mongolake'])).rejects.toThrow(/already exists/i);
    });

    it('should validate namespace path', async () => {
      await expect(client.createNamespace([])).rejects.toThrow(/namespace.*empty|invalid/i);
    });

    it('should reject namespace with invalid characters', async () => {
      await expect(client.createNamespace(['invalid/namespace'])).rejects.toThrow(
        /invalid.*character/i
      );
    });
  });

  describe('getNamespace', () => {
    it('should get namespace metadata', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces/mongolake',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  namespace: ['mongolake'],
                  properties: { owner: 'data-team' },
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const namespace = await client.getNamespace(['mongolake']);

      expect(namespace.namespace).toEqual(['mongolake']);
      expect(namespace.properties).toEqual({ owner: 'data-team' });
    });

    it('should throw when namespace does not exist', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 404,
              body: {
                success: false,
                error: { code: 'NOT_FOUND', message: 'Namespace not found' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await expect(client.getNamespace(['nonexistent'])).rejects.toThrow(R2DataCatalogError);
    });
  });

  describe('namespaceExists', () => {
    it('should return true when namespace exists', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces/mongolake',
            {
              status: 200,
              body: { success: true, result: { namespace: ['mongolake'] } },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const exists = await client.namespaceExists(['mongolake']);

      expect(exists).toBe(true);
    });

    it('should return false when namespace does not exist', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 404,
              body: {
                success: false,
                error: { code: 'NOT_FOUND', message: 'Not found' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const exists = await client.namespaceExists(['nonexistent']);

      expect(exists).toBe(false);
    });
  });

  describe('updateNamespaceProperties', () => {
    it('should update namespace properties', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces/mongolake',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  namespace: ['mongolake'],
                  properties: { owner: 'new-owner', newProp: 'value' },
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const namespace = await client.updateNamespaceProperties(
        ['mongolake'],
        { owner: 'new-owner', newProp: 'value' },
        []
      );

      expect(namespace.properties?.owner).toBe('new-owner');
    });

    it('should remove namespace properties', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces/mongolake',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  namespace: ['mongolake'],
                  properties: {},
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const namespace = await client.updateNamespaceProperties(
        ['mongolake'],
        {},
        ['oldProp']
      );

      expect(namespace.properties).toEqual({});
    });

    it('should throw when namespace does not exist', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 404,
              body: {
                success: false,
                error: { code: 'NOT_FOUND', message: 'Namespace not found' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await expect(
        client.updateNamespaceProperties(['nonexistent'], { prop: 'value' }, [])
      ).rejects.toThrow(R2DataCatalogError);
    });
  });

  describe('dropNamespace', () => {
    it('should drop an empty namespace', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces/mongolake',
            {
              status: 200,
              body: { success: true, result: null },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const dropped = await client.dropNamespace(['mongolake']);

      expect(dropped).toBe(true);
      expect(fetchMock).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({ method: 'DELETE' })
      );
    });

    it('should return false when namespace does not exist', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 404,
              body: {
                success: false,
                error: { code: 'NOT_FOUND', message: 'Not found' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const dropped = await client.dropNamespace(['nonexistent']);

      expect(dropped).toBe(false);
    });

    it('should throw when namespace is not empty', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces/mongolake',
            {
              status: 409,
              body: {
                success: false,
                error: { code: 'NOT_EMPTY', message: 'Namespace is not empty' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await expect(client.dropNamespace(['mongolake'])).rejects.toThrow(R2DataCatalogError);
      await expect(client.dropNamespace(['mongolake'])).rejects.toThrow(/not empty/i);
    });
  });
});

// ============================================================================
// 3. Table Registration
// ============================================================================

describe('R2DataCatalogClient - Table Registration', () => {
  let client: R2DataCatalogClient;
  let fetchMock: ReturnType<typeof mockFetch>;

  beforeEach(() => {
    client = new R2DataCatalogClient(TEST_CONFIG);
    fetchMock = mockFetch(new Map());
    vi.stubGlobal('fetch', fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  describe('createTable', () => {
    it('should register a new table', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  identifier: { namespace: ['mongolake'], name: 'users' },
                  location: 's3://bucket/warehouse/mongolake/users',
                  metadataLocation: 's3://bucket/warehouse/mongolake/users/metadata/v1.metadata.json',
                  properties: {},
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const table = await client.createTable({
        name: 'users',
        namespace: ['mongolake'],
        location: 's3://bucket/warehouse/mongolake/users',
      });

      expect(table.identifier.name).toBe('users');
      expect(table.identifier.namespace).toEqual(['mongolake']);
      expect(table.location).toBe('s3://bucket/warehouse/mongolake/users');
    });

    it('should register table with schema', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  identifier: { namespace: ['mongolake'], name: 'users' },
                  location: 's3://bucket/warehouse/mongolake/users',
                  metadataLocation: 's3://bucket/warehouse/mongolake/users/metadata/v1.metadata.json',
                  properties: {},
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const schema = createTestSchema();
      const table = await client.createTable({
        name: 'users',
        namespace: ['mongolake'],
        location: 's3://bucket/warehouse/mongolake/users',
        schema,
      });

      expect(table.identifier.name).toBe('users');
      expect(fetchMock).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          body: expect.stringContaining('schema'),
        })
      );
    });

    it('should register table with partition spec', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  identifier: { namespace: ['mongolake'], name: 'events' },
                  location: 's3://bucket/warehouse/mongolake/events',
                  metadataLocation: 's3://bucket/warehouse/mongolake/events/metadata/v1.metadata.json',
                  properties: {},
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const partitionSpec = createTestPartitionSpec();
      const table = await client.createTable({
        name: 'events',
        namespace: ['mongolake'],
        location: 's3://bucket/warehouse/mongolake/events',
        partitionSpec,
      });

      expect(table.identifier.name).toBe('events');
      expect(fetchMock).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          body: expect.stringContaining('partition'),
        })
      );
    });

    it('should register table with properties', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  identifier: { namespace: ['mongolake'], name: 'users' },
                  location: 's3://bucket/warehouse/mongolake/users',
                  metadataLocation: 's3://bucket/warehouse/mongolake/users/metadata/v1.metadata.json',
                  properties: {
                    'write.format.default': 'parquet',
                    'mongolake.collection': 'users',
                  },
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const table = await client.createTable({
        name: 'users',
        namespace: ['mongolake'],
        location: 's3://bucket/warehouse/mongolake/users',
        properties: {
          'write.format.default': 'parquet',
          'mongolake.collection': 'users',
        },
      });

      expect(table.properties['write.format.default']).toBe('parquet');
      expect(table.properties['mongolake.collection']).toBe('users');
    });

    it('should throw when table already exists', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables',
            {
              status: 409,
              body: {
                success: false,
                error: { code: 'ALREADY_EXISTS', message: 'Table already exists' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await expect(
        client.createTable({
          name: 'users',
          namespace: ['mongolake'],
          location: 's3://bucket/warehouse/mongolake/users',
        })
      ).rejects.toThrow(R2DataCatalogError);
    });

    it('should throw when namespace does not exist', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables',
            {
              status: 404,
              body: {
                success: false,
                error: { code: 'NAMESPACE_NOT_FOUND', message: 'Namespace not found' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await expect(
        client.createTable({
          name: 'users',
          namespace: ['nonexistent'],
          location: 's3://bucket/warehouse/nonexistent/users',
        })
      ).rejects.toThrow(R2DataCatalogError);
    });

    it('should validate table name', async () => {
      await expect(
        client.createTable({
          name: '',
          namespace: ['mongolake'],
          location: 's3://bucket/warehouse/mongolake/users',
        })
      ).rejects.toThrow(/name.*required|empty/i);
    });

    it('should validate location', async () => {
      await expect(
        client.createTable({
          name: 'users',
          namespace: ['mongolake'],
          location: '',
        })
      ).rejects.toThrow(/location.*required|empty/i);
    });
  });

  describe('registerCollection', () => {
    it('should register a MongoLake collection as Iceberg table', async () => {
      // First call creates namespace if needed, second creates table
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 200,
              body: {
                success: true,
                result: { namespace: ['mongolake', 'mydb'], properties: {} },
              },
            },
          ],
          [
            'tables',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  identifier: { namespace: ['mongolake', 'mydb'], name: 'users' },
                  location: 's3://bucket/warehouse/mongolake/mydb/users',
                  metadataLocation: 's3://bucket/warehouse/mongolake/mydb/users/metadata/v1.metadata.json',
                  properties: { 'mongolake.database': 'mydb', 'mongolake.collection': 'users' },
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const table = await client.registerCollection(
        'mydb',
        'users',
        's3://bucket/warehouse/mongolake/mydb/users'
      );

      expect(table.identifier.namespace).toContain('mydb');
      expect(table.identifier.name).toBe('users');
      expect(table.properties['mongolake.database']).toBe('mydb');
      expect(table.properties['mongolake.collection']).toBe('users');
    });

    it('should create namespace if it does not exist', async () => {
      // First call returns 404, then creates namespace, then creates table
      const callCount = { namespace: 0 };
      const customFetch = vi.fn(async (url: string, init?: RequestInit) => {
        const method = init?.method ?? 'GET';

        if (url.includes('namespaces') && method === 'POST') {
          return new Response(
            JSON.stringify({
              success: true,
              result: { namespace: ['mongolake', 'newdb'], properties: {} },
            }),
            { status: 200, headers: { 'Content-Type': 'application/json' } }
          );
        }

        if (url.includes('namespaces')) {
          callCount.namespace++;
          if (callCount.namespace === 1) {
            // First check returns 404
            return new Response(
              JSON.stringify({
                success: false,
                error: { code: 'NOT_FOUND', message: 'Not found' },
              }),
              { status: 404, headers: { 'Content-Type': 'application/json' } }
            );
          }
        }

        if (url.includes('tables')) {
          return new Response(
            JSON.stringify({
              success: true,
              result: {
                identifier: { namespace: ['mongolake', 'newdb'], name: 'users' },
                location: 's3://bucket/warehouse/mongolake/newdb/users',
                metadataLocation: 's3://bucket/warehouse/mongolake/newdb/users/metadata/v1.metadata.json',
                properties: {},
              },
            }),
            { status: 200, headers: { 'Content-Type': 'application/json' } }
          );
        }

        return new Response(JSON.stringify({ success: false }), { status: 404 });
      });
      vi.stubGlobal('fetch', customFetch);

      const table = await client.registerCollection(
        'newdb',
        'users',
        's3://bucket/warehouse/mongolake/newdb/users'
      );

      expect(table.identifier.name).toBe('users');
    });

    it('should pass through schema and partition spec', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            { status: 200, body: { success: true, result: { namespace: ['mongolake', 'db'] } } },
          ],
          [
            'tables',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  identifier: { namespace: ['mongolake', 'db'], name: 'events' },
                  location: 's3://bucket/warehouse/mongolake/db/events',
                  metadataLocation: 's3://bucket/warehouse/mongolake/db/events/metadata/v1.metadata.json',
                  properties: {},
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const schema = createTestSchema();
      const partitionSpec = createTestPartitionSpec();

      await client.registerCollection('db', 'events', 's3://bucket/warehouse/mongolake/db/events', {
        schema,
        partitionSpec,
        properties: { custom: 'prop' },
      });

      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringContaining('tables'),
        expect.objectContaining({
          body: expect.stringContaining('schema'),
        })
      );
    });
  });

  describe('unregisterCollection', () => {
    it('should unregister a MongoLake collection', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables/mongolake%1Fmydb/users',
            {
              status: 200,
              body: { success: true, result: null },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const dropped = await client.unregisterCollection('mydb', 'users');

      expect(dropped).toBe(true);
    });

    it('should return false when collection does not exist', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables',
            {
              status: 404,
              body: {
                success: false,
                error: { code: 'NOT_FOUND', message: 'Table not found' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const dropped = await client.unregisterCollection('mydb', 'nonexistent');

      expect(dropped).toBe(false);
    });
  });
});

// ============================================================================
// 4. Table Listing
// ============================================================================

describe('R2DataCatalogClient - Table Listing', () => {
  let client: R2DataCatalogClient;
  let fetchMock: ReturnType<typeof mockFetch>;

  beforeEach(() => {
    client = new R2DataCatalogClient(TEST_CONFIG);
    fetchMock = mockFetch(new Map());
    vi.stubGlobal('fetch', fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  describe('listTables', () => {
    it('should list tables in a namespace', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  identifiers: [
                    { namespace: ['mongolake'], name: 'users' },
                    { namespace: ['mongolake'], name: 'orders' },
                    { namespace: ['mongolake'], name: 'products' },
                  ],
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const response = await client.listTables(['mongolake']);

      expect(response.identifiers).toHaveLength(3);
      expect(response.identifiers.map((t) => t.name)).toContain('users');
      expect(response.identifiers.map((t) => t.name)).toContain('orders');
      expect(response.identifiers.map((t) => t.name)).toContain('products');
    });

    it('should return empty array when namespace has no tables', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables',
            {
              status: 200,
              body: {
                success: true,
                result: { identifiers: [] },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const response = await client.listTables(['empty-namespace']);

      expect(response.identifiers).toEqual([]);
    });

    it('should handle pagination', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  identifiers: [{ namespace: ['mongolake'], name: 'page2-table' }],
                  nextPageToken: 'next-page-token',
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const response = await client.listTables(['mongolake'], 'page-token');

      expect(response.nextPageToken).toBe('next-page-token');
      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringContaining('pageToken=page-token'),
        expect.any(Object)
      );
    });

    it('should throw when namespace does not exist', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables',
            {
              status: 404,
              body: {
                success: false,
                error: { code: 'NAMESPACE_NOT_FOUND', message: 'Namespace not found' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await expect(client.listTables(['nonexistent'])).rejects.toThrow(R2DataCatalogError);
    });
  });

  describe('listCollections', () => {
    it('should list collections in a database', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  identifiers: [
                    { namespace: ['mongolake', 'mydb'], name: 'users' },
                    { namespace: ['mongolake', 'mydb'], name: 'orders' },
                  ],
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const collections = await client.listCollections('mydb');

      expect(collections).toContain('users');
      expect(collections).toContain('orders');
      expect(collections).toHaveLength(2);
    });

    it('should return empty array when database has no collections', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables',
            {
              status: 200,
              body: { success: true, result: { identifiers: [] } },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const collections = await client.listCollections('empty-db');

      expect(collections).toEqual([]);
    });
  });

  describe('listDatabases', () => {
    it('should list all databases', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  namespaces: [
                    ['mongolake', 'db1'],
                    ['mongolake', 'db2'],
                    ['mongolake', 'production'],
                  ],
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const databases = await client.listDatabases();

      expect(databases).toContain('db1');
      expect(databases).toContain('db2');
      expect(databases).toContain('production');
    });

    it('should return empty array when no databases exist', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 200,
              body: { success: true, result: { namespaces: [] } },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const databases = await client.listDatabases();

      expect(databases).toEqual([]);
    });
  });
});

// ============================================================================
// 5. Table Metadata Fetching
// ============================================================================

describe('R2DataCatalogClient - Table Metadata', () => {
  let client: R2DataCatalogClient;
  let fetchMock: ReturnType<typeof mockFetch>;

  beforeEach(() => {
    client = new R2DataCatalogClient(TEST_CONFIG);
    fetchMock = mockFetch(new Map());
    vi.stubGlobal('fetch', fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  describe('loadTable', () => {
    it('should load table metadata', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables/mongolake/users',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  identifier: { namespace: ['mongolake'], name: 'users' },
                  location: 's3://bucket/warehouse/mongolake/users',
                  metadataLocation: 's3://bucket/warehouse/mongolake/users/metadata/v3.metadata.json',
                  properties: {
                    'write.format.default': 'parquet',
                    'format-version': '2',
                  },
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const table = await client.loadTable(['mongolake'], 'users');

      expect(table.identifier.name).toBe('users');
      expect(table.identifier.namespace).toEqual(['mongolake']);
      expect(table.location).toBe('s3://bucket/warehouse/mongolake/users');
      expect(table.metadataLocation).toContain('v3.metadata.json');
    });

    it('should include table metadata when available', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables/mongolake/users',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  identifier: { namespace: ['mongolake'], name: 'users' },
                  location: 's3://bucket/warehouse/mongolake/users',
                  metadataLocation: 's3://bucket/warehouse/mongolake/users/metadata/v1.metadata.json',
                  properties: {},
                  metadata: {
                    'format-version': 2,
                    'table-uuid': 'abc-123-def-456',
                    location: 's3://bucket/warehouse/mongolake/users',
                    'last-updated-ms': 1704067200000,
                    'last-column-id': 3,
                    schemas: [createTestSchema()],
                    'current-schema-id': 0,
                    'partition-specs': [],
                    'default-spec-id': 0,
                    'last-partition-id': 0,
                    properties: {},
                    'current-snapshot-id': -1,
                    snapshots: [],
                    'snapshot-log': [],
                    'metadata-log': [],
                  },
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const table = await client.loadTable(['mongolake'], 'users');

      expect(table.metadata).toBeDefined();
      expect(table.metadata?.['format-version']).toBe(2);
      expect(table.metadata?.['table-uuid']).toBe('abc-123-def-456');
    });

    it('should throw when table does not exist', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables',
            {
              status: 404,
              body: {
                success: false,
                error: { code: 'NOT_FOUND', message: 'Table not found' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await expect(client.loadTable(['mongolake'], 'nonexistent')).rejects.toThrow(
        R2DataCatalogError
      );
    });

    it('should handle nested namespace', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables/mongolake%1Fproduction/users',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  identifier: { namespace: ['mongolake', 'production'], name: 'users' },
                  location: 's3://bucket/warehouse/mongolake/production/users',
                  metadataLocation: 's3://bucket/warehouse/mongolake/production/users/metadata/v1.metadata.json',
                  properties: {},
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const table = await client.loadTable(['mongolake', 'production'], 'users');

      expect(table.identifier.namespace).toEqual(['mongolake', 'production']);
    });
  });

  describe('tableExists', () => {
    it('should return true when table exists', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables/mongolake/users',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  identifier: { namespace: ['mongolake'], name: 'users' },
                  location: 's3://bucket/warehouse/mongolake/users',
                  metadataLocation: 's3://bucket/warehouse/mongolake/users/metadata/v1.metadata.json',
                  properties: {},
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const exists = await client.tableExists(['mongolake'], 'users');

      expect(exists).toBe(true);
    });

    it('should return false when table does not exist', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables',
            {
              status: 404,
              body: {
                success: false,
                error: { code: 'NOT_FOUND', message: 'Table not found' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const exists = await client.tableExists(['mongolake'], 'nonexistent');

      expect(exists).toBe(false);
    });
  });

  describe('refreshTable', () => {
    it('should refresh table metadata location', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  identifier: { namespace: ['mongolake', 'db'], name: 'users' },
                  location: 's3://bucket/warehouse/mongolake/db/users',
                  metadataLocation: 's3://bucket/warehouse/mongolake/db/users/metadata/v5.metadata.json',
                  properties: {},
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const table = await client.refreshTable(
        'db',
        'users',
        's3://bucket/warehouse/mongolake/db/users/metadata/v5.metadata.json'
      );

      expect(table.metadataLocation).toContain('v5.metadata.json');
    });

    it('should throw when table does not exist', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables',
            {
              status: 404,
              body: {
                success: false,
                error: { code: 'NOT_FOUND', message: 'Table not found' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await expect(
        client.refreshTable('db', 'nonexistent', 's3://bucket/metadata/v1.metadata.json')
      ).rejects.toThrow(R2DataCatalogError);
    });
  });
});

// ============================================================================
// 6. Table Updates and Commits
// ============================================================================

describe('R2DataCatalogClient - Table Updates', () => {
  let client: R2DataCatalogClient;
  let fetchMock: ReturnType<typeof mockFetch>;

  beforeEach(() => {
    client = new R2DataCatalogClient(TEST_CONFIG);
    fetchMock = mockFetch(new Map());
    vi.stubGlobal('fetch', fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  describe('updateTableLocation', () => {
    it('should update table location', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  identifier: { namespace: ['mongolake'], name: 'users' },
                  location: 's3://new-bucket/warehouse/mongolake/users',
                  metadataLocation: 's3://new-bucket/warehouse/mongolake/users/metadata/v1.metadata.json',
                  properties: {},
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const table = await client.updateTableLocation({
        name: 'users',
        namespace: ['mongolake'],
        location: 's3://new-bucket/warehouse/mongolake/users',
      });

      expect(table.location).toBe('s3://new-bucket/warehouse/mongolake/users');
    });

    it('should update metadata location', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  identifier: { namespace: ['mongolake'], name: 'users' },
                  location: 's3://bucket/warehouse/mongolake/users',
                  metadataLocation: 's3://bucket/warehouse/mongolake/users/metadata/v10.metadata.json',
                  properties: {},
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const table = await client.updateTableLocation({
        name: 'users',
        namespace: ['mongolake'],
        metadataLocation: 's3://bucket/warehouse/mongolake/users/metadata/v10.metadata.json',
      });

      expect(table.metadataLocation).toContain('v10.metadata.json');
    });

    it('should update table properties', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables',
            {
              status: 200,
              body: {
                success: true,
                result: {
                  identifier: { namespace: ['mongolake'], name: 'users' },
                  location: 's3://bucket/warehouse/mongolake/users',
                  metadataLocation: 's3://bucket/warehouse/mongolake/users/metadata/v1.metadata.json',
                  properties: {
                    'write.metadata.delete-after-commit.enabled': 'true',
                    'write.metadata.previous-versions-max': '10',
                  },
                },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const table = await client.updateTableLocation({
        name: 'users',
        namespace: ['mongolake'],
        properties: {
          'write.metadata.delete-after-commit.enabled': 'true',
          'write.metadata.previous-versions-max': '10',
        },
      });

      expect(table.properties['write.metadata.delete-after-commit.enabled']).toBe('true');
    });

    it('should throw when table does not exist', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables',
            {
              status: 404,
              body: {
                success: false,
                error: { code: 'NOT_FOUND', message: 'Table not found' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await expect(
        client.updateTableLocation({
          name: 'nonexistent',
          namespace: ['mongolake'],
          location: 's3://bucket/new-location',
        })
      ).rejects.toThrow(R2DataCatalogError);
    });
  });

  describe('dropTable', () => {
    it('should drop a table', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables/mongolake/users',
            {
              status: 200,
              body: { success: true, result: null },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const dropped = await client.dropTable(['mongolake'], 'users');

      expect(dropped).toBe(true);
      expect(fetchMock).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({ method: 'DELETE' })
      );
    });

    it('should return false when table does not exist', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables',
            {
              status: 404,
              body: {
                success: false,
                error: { code: 'NOT_FOUND', message: 'Table not found' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      const dropped = await client.dropTable(['mongolake'], 'nonexistent');

      expect(dropped).toBe(false);
    });

    it('should support purge option', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'tables/mongolake/users',
            {
              status: 200,
              body: { success: true, result: null },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await client.dropTable(['mongolake'], 'users', true);

      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringContaining('purge=true'),
        expect.any(Object)
      );
    });
  });

  describe('renameTable', () => {
    it('should rename a table within same namespace', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'rename',
            {
              status: 200,
              body: { success: true, result: null },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await client.renameTable(['mongolake'], 'old-name', ['mongolake'], 'new-name');

      expect(fetchMock).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          method: 'POST',
          body: expect.stringContaining('old-name'),
        })
      );
    });

    it('should move table to different namespace', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'rename',
            {
              status: 200,
              body: { success: true, result: null },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await client.renameTable(
        ['mongolake', 'staging'],
        'users',
        ['mongolake', 'production'],
        'users'
      );

      expect(fetchMock).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          body: expect.stringContaining('production'),
        })
      );
    });

    it('should throw when source table does not exist', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'rename',
            {
              status: 404,
              body: {
                success: false,
                error: { code: 'NOT_FOUND', message: 'Source table not found' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await expect(
        client.renameTable(['mongolake'], 'nonexistent', ['mongolake'], 'new-name')
      ).rejects.toThrow(R2DataCatalogError);
    });

    it('should throw when destination already exists', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'rename',
            {
              status: 409,
              body: {
                success: false,
                error: { code: 'ALREADY_EXISTS', message: 'Destination table already exists' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await expect(
        client.renameTable(['mongolake'], 'old-name', ['mongolake'], 'existing-name')
      ).rejects.toThrow(R2DataCatalogError);
    });

    it('should throw when destination namespace does not exist', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'rename',
            {
              status: 404,
              body: {
                success: false,
                error: { code: 'NAMESPACE_NOT_FOUND', message: 'Destination namespace not found' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await expect(
        client.renameTable(['mongolake'], 'users', ['nonexistent'], 'users')
      ).rejects.toThrow(R2DataCatalogError);
    });
  });
});

// ============================================================================
// 7. Error Handling
// ============================================================================

describe('R2DataCatalogClient - Error Handling', () => {
  let client: R2DataCatalogClient;
  let fetchMock: ReturnType<typeof mockFetch>;

  beforeEach(() => {
    client = new R2DataCatalogClient(TEST_CONFIG);
    fetchMock = mockFetch(new Map());
    vi.stubGlobal('fetch', fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  describe('R2DataCatalogError', () => {
    it('should include status code', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 500,
              body: {
                success: false,
                error: { code: 'INTERNAL_ERROR', message: 'Internal server error' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      try {
        await client.listNamespaces();
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(R2DataCatalogError);
        expect((error as R2DataCatalogError).statusCode).toBe(500);
      }
    });

    it('should include error message', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 400,
              body: {
                success: false,
                error: { code: 'INVALID_REQUEST', message: 'Missing required parameter' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      try {
        await client.listNamespaces();
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(Error);
        expect((error as Error).message).toContain('Missing required parameter');
      }
    });
  });

  describe('network errors', () => {
    it('should handle network timeouts', async () => {
      const timeoutFetch = vi.fn(() =>
        Promise.reject(new Error('Request timed out'))
      );
      vi.stubGlobal('fetch', timeoutFetch);

      await expect(client.listNamespaces()).rejects.toThrow(/timeout/i);
    });

    it('should handle connection refused', async () => {
      const connectionFetch = vi.fn(() =>
        Promise.reject(new Error('Connection refused'))
      );
      vi.stubGlobal('fetch', connectionFetch);

      await expect(client.listNamespaces()).rejects.toThrow(/connection/i);
    });

    it('should handle DNS resolution failure', async () => {
      const dnsFetch = vi.fn(() =>
        Promise.reject(new Error('getaddrinfo ENOTFOUND'))
      );
      vi.stubGlobal('fetch', dnsFetch);

      await expect(client.listNamespaces()).rejects.toThrow();
    });
  });

  describe('authentication errors', () => {
    it('should handle 401 unauthorized', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 401,
              body: {
                success: false,
                error: { code: 'UNAUTHORIZED', message: 'Invalid or expired token' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await expect(client.listNamespaces()).rejects.toThrow(R2DataCatalogError);
    });

    it('should handle 403 forbidden', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 403,
              body: {
                success: false,
                error: { code: 'FORBIDDEN', message: 'Access denied' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await expect(client.listNamespaces()).rejects.toThrow(R2DataCatalogError);
    });
  });

  describe('rate limiting', () => {
    it('should handle 429 rate limit', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 429,
              body: {
                success: false,
                error: { code: 'RATE_LIMITED', message: 'Too many requests' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await expect(client.listNamespaces()).rejects.toThrow(R2DataCatalogError);
    });
  });

  describe('server errors', () => {
    it('should handle 500 internal server error', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 500,
              body: {
                success: false,
                error: { code: 'INTERNAL_ERROR', message: 'Internal server error' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await expect(client.listNamespaces()).rejects.toThrow(R2DataCatalogError);
    });

    it('should handle 502 bad gateway', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 502,
              body: {
                success: false,
                error: { code: 'BAD_GATEWAY', message: 'Bad gateway' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await expect(client.listNamespaces()).rejects.toThrow(R2DataCatalogError);
    });

    it('should handle 503 service unavailable', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 503,
              body: {
                success: false,
                error: { code: 'SERVICE_UNAVAILABLE', message: 'Service unavailable' },
              },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await expect(client.listNamespaces()).rejects.toThrow(R2DataCatalogError);
    });
  });
});

// ============================================================================
// 8. API Request Formatting
// ============================================================================

describe('R2DataCatalogClient - API Requests', () => {
  let client: R2DataCatalogClient;
  let fetchMock: ReturnType<typeof mockFetch>;

  beforeEach(() => {
    client = new R2DataCatalogClient(TEST_CONFIG);
    fetchMock = mockFetch(new Map());
    vi.stubGlobal('fetch', fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  describe('authentication', () => {
    it('should include authorization header', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            { status: 200, body: { success: true, result: { namespaces: [] } } },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await client.listNamespaces();

      expect(fetchMock).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          headers: expect.objectContaining({
            Authorization: expect.stringContaining('Bearer'),
          }),
        })
      );
    });
  });

  describe('content type', () => {
    it('should set content-type for POST requests', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            { status: 200, body: { success: true, result: { namespace: ['test'] } } },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await client.createNamespace(['test']);

      expect(fetchMock).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          headers: expect.objectContaining({
            'Content-Type': 'application/json',
          }),
        })
      );
    });
  });

  describe('namespace encoding', () => {
    it('should properly encode nested namespace in URL', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            {
              status: 200,
              body: { success: true, result: { namespace: ['db', 'schema'] } },
            },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await client.getNamespace(['db', 'schema']);

      // Namespace should be encoded with separator (e.g., %1F or similar)
      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringMatching(/namespaces\/[^/]+/),
        expect.any(Object)
      );
    });
  });

  describe('URL construction', () => {
    it('should use correct API base URL', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            { status: 200, body: { success: true, result: { namespaces: [] } } },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await client.listNamespaces();

      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringContaining(TEST_CONFIG.baseUrl!),
        expect.any(Object)
      );
    });

    it('should include account ID in URL', async () => {
      fetchMock = mockFetch(
        new Map([
          [
            'namespaces',
            { status: 200, body: { success: true, result: { namespaces: [] } } },
          ],
        ])
      );
      vi.stubGlobal('fetch', fetchMock);

      await client.listNamespaces();

      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringContaining(TEST_CONFIG.accountId),
        expect.any(Object)
      );
    });
  });
});

// ============================================================================
// 9. Integration Scenarios
// ============================================================================

describe('R2DataCatalogClient - Integration Scenarios', () => {
  let client: R2DataCatalogClient;

  beforeEach(() => {
    client = new R2DataCatalogClient(TEST_CONFIG);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  describe('complete table lifecycle', () => {
    it('should support full create-update-drop lifecycle', async () => {
      const callLog: string[] = [];

      const lifecycleFetch = vi.fn(async (url: string, init?: RequestInit) => {
        const method = init?.method ?? 'GET';
        callLog.push(`${method} ${url}`);

        // Create namespace
        if (url.includes('namespaces') && method === 'POST') {
          return new Response(
            JSON.stringify({
              success: true,
              result: { namespace: ['mongolake', 'lifecycle-test'], properties: {} },
            }),
            { status: 200, headers: { 'Content-Type': 'application/json' } }
          );
        }

        // Create table
        if (url.includes('tables') && method === 'POST' && !url.includes('rename')) {
          return new Response(
            JSON.stringify({
              success: true,
              result: {
                identifier: { namespace: ['mongolake', 'lifecycle-test'], name: 'test-table' },
                location: 's3://bucket/warehouse/mongolake/lifecycle-test/test-table',
                metadataLocation: 's3://bucket/warehouse/mongolake/lifecycle-test/test-table/metadata/v1.metadata.json',
                properties: {},
              },
            }),
            { status: 200, headers: { 'Content-Type': 'application/json' } }
          );
        }

        // Update table
        if (url.includes('tables') && method === 'PUT') {
          return new Response(
            JSON.stringify({
              success: true,
              result: {
                identifier: { namespace: ['mongolake', 'lifecycle-test'], name: 'test-table' },
                location: 's3://bucket/warehouse/mongolake/lifecycle-test/test-table',
                metadataLocation: 's3://bucket/warehouse/mongolake/lifecycle-test/test-table/metadata/v2.metadata.json',
                properties: { updated: 'true' },
              },
            }),
            { status: 200, headers: { 'Content-Type': 'application/json' } }
          );
        }

        // Drop table
        if (url.includes('tables') && method === 'DELETE') {
          return new Response(
            JSON.stringify({ success: true, result: null }),
            { status: 200, headers: { 'Content-Type': 'application/json' } }
          );
        }

        return new Response(JSON.stringify({ success: false }), { status: 404 });
      });
      vi.stubGlobal('fetch', lifecycleFetch);

      // 1. Create namespace
      const namespace = await client.createNamespace(['mongolake', 'lifecycle-test']);
      expect(namespace.namespace).toEqual(['mongolake', 'lifecycle-test']);

      // 2. Create table
      const table = await client.createTable({
        name: 'test-table',
        namespace: ['mongolake', 'lifecycle-test'],
        location: 's3://bucket/warehouse/mongolake/lifecycle-test/test-table',
      });
      expect(table.identifier.name).toBe('test-table');

      // 3. Update table
      const updated = await client.updateTableLocation({
        name: 'test-table',
        namespace: ['mongolake', 'lifecycle-test'],
        metadataLocation: 's3://bucket/warehouse/mongolake/lifecycle-test/test-table/metadata/v2.metadata.json',
        properties: { updated: 'true' },
      });
      expect(updated.properties.updated).toBe('true');

      // 4. Drop table
      const dropped = await client.dropTable(['mongolake', 'lifecycle-test'], 'test-table');
      expect(dropped).toBe(true);

      // Verify call sequence
      expect(callLog.some((c) => c.includes('POST') && c.includes('namespaces'))).toBe(true);
      expect(callLog.some((c) => c.includes('POST') && c.includes('tables'))).toBe(true);
      expect(callLog.some((c) => c.includes('PUT'))).toBe(true);
      expect(callLog.some((c) => c.includes('DELETE'))).toBe(true);
    });
  });

  describe('MongoLake collection workflow', () => {
    it('should support registering and managing collections', async () => {
      const collectionFetch = vi.fn(async (url: string, init?: RequestInit) => {
        const method = init?.method ?? 'GET';

        // Namespace check/create
        if (url.includes('namespaces') && method === 'GET') {
          return new Response(
            JSON.stringify({
              success: true,
              result: { namespace: ['mongolake', 'myapp'], properties: {} },
            }),
            { status: 200, headers: { 'Content-Type': 'application/json' } }
          );
        }

        if (url.includes('namespaces') && method === 'POST') {
          return new Response(
            JSON.stringify({
              success: true,
              result: { namespace: ['mongolake', 'myapp'], properties: {} },
            }),
            { status: 200, headers: { 'Content-Type': 'application/json' } }
          );
        }

        // Table operations
        if (url.includes('tables') && method === 'POST') {
          return new Response(
            JSON.stringify({
              success: true,
              result: {
                identifier: { namespace: ['mongolake', 'myapp'], name: 'users' },
                location: 's3://bucket/warehouse/mongolake/myapp/users',
                metadataLocation: 's3://bucket/warehouse/mongolake/myapp/users/metadata/v1.metadata.json',
                properties: {
                  'mongolake.database': 'myapp',
                  'mongolake.collection': 'users',
                },
              },
            }),
            { status: 200, headers: { 'Content-Type': 'application/json' } }
          );
        }

        if (url.includes('tables') && method === 'GET') {
          return new Response(
            JSON.stringify({
              success: true,
              result: {
                identifiers: [
                  { namespace: ['mongolake', 'myapp'], name: 'users' },
                  { namespace: ['mongolake', 'myapp'], name: 'orders' },
                ],
              },
            }),
            { status: 200, headers: { 'Content-Type': 'application/json' } }
          );
        }

        return new Response(JSON.stringify({ success: false }), { status: 404 });
      });
      vi.stubGlobal('fetch', collectionFetch);

      // Register a collection
      const table = await client.registerCollection(
        'myapp',
        'users',
        's3://bucket/warehouse/mongolake/myapp/users'
      );
      expect(table.properties['mongolake.database']).toBe('myapp');
      expect(table.properties['mongolake.collection']).toBe('users');

      // List collections
      const collections = await client.listCollections('myapp');
      expect(collections).toContain('users');
    });
  });
});

// ============================================================================
// 10. Performance and Caching
// ============================================================================

describe('R2DataCatalogClient - Performance', () => {
  let client: R2DataCatalogClient;

  beforeEach(() => {
    client = new R2DataCatalogClient(TEST_CONFIG);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  describe('request efficiency', () => {
    it('should batch namespace checks when possible', async () => {
      const callCount = { namespace: 0 };
      const batchFetch = vi.fn(async () => {
        callCount.namespace++;
        return new Response(
          JSON.stringify({
            success: true,
            result: { namespaces: [['ns1'], ['ns2'], ['ns3']] },
          }),
          { status: 200, headers: { 'Content-Type': 'application/json' } }
        );
      });
      vi.stubGlobal('fetch', batchFetch);

      // List namespaces should be a single call
      await client.listNamespaces();

      expect(callCount.namespace).toBe(1);
    });
  });

  describe('pagination handling', () => {
    it('should handle large result sets efficiently', async () => {
      let pageCount = 0;
      const paginatedFetch = vi.fn(async (url: string) => {
        pageCount++;
        const hasMore = pageCount < 3;

        return new Response(
          JSON.stringify({
            success: true,
            result: {
              identifiers: Array.from({ length: 100 }, (_, i) => ({
                namespace: ['mongolake'],
                name: `table-${pageCount}-${i}`,
              })),
              nextPageToken: hasMore ? `page-${pageCount + 1}` : undefined,
            },
          }),
          { status: 200, headers: { 'Content-Type': 'application/json' } }
        );
      });
      vi.stubGlobal('fetch', paginatedFetch);

      // First page
      const response = await client.listTables(['mongolake']);

      expect(response.identifiers).toHaveLength(100);
      expect(response.nextPageToken).toBe('page-2');
    });
  });
});
