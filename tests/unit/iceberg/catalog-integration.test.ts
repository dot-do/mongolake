/**
 * Iceberg Catalog Integration Tests (TDD RED Phase)
 *
 * Comprehensive tests for full Iceberg catalog integration including:
 * 1. Table metadata management
 * 2. Schema evolution
 * 3. Partition spec handling
 * 4. Snapshot management
 * 5. Table creation and deletion
 * 6. Catalog listing operations
 * 7. Table properties
 * 8. Concurrent access patterns
 * 9. Error handling
 * 10. Integration with R2 storage
 *
 * These tests define expected behavior for a fully integrated Iceberg catalog.
 * Following TDD RED phase: tests should fail initially until features are implemented.
 *
 * @see https://iceberg.apache.org/spec/
 */

import { describe, it, expect, beforeEach, afterEach, vi, beforeAll } from 'vitest';
import {
  RestCatalog,
  RestCatalogError,
  NotFoundError,
  AlreadyExistsError,
  ValidationError,
  CommitFailedError,
  createRestCatalog,
  type RestCatalogConfig,
  type CreateTableRequest,
  type TableRequirement,
  type TableUpdate,
  type LoadTableResponse,
} from '../../../src/iceberg/rest-catalog.js';
import {
  R2DataCatalogClient,
  R2DataCatalogError,
  createCatalogClient,
  type R2DataCatalogConfig,
  type CatalogTable,
} from '../../../src/iceberg/catalog-client.js';
import {
  MetadataWriter,
  type IcebergSchema,
  type PartitionSpec,
  type TableMetadata,
  type Snapshot,
  type SortOrder,
  InvalidSchemaError,
  InvalidPartitionSpecError,
} from '../../../src/iceberg/metadata-writer.js';
import {
  SnapshotManager,
  type CreateSnapshotOptions,
  type OperationType,
} from '../../../src/iceberg/snapshot-manager.js';
import {
  SchemaTracker,
  type SchemaChange,
  type SchemaEvolutionMetadata,
  type AddFieldOptions,
} from '../../../src/iceberg/schema-tracker.js';

// ============================================================================
// Test Fixtures and Helpers
// ============================================================================

/** Default test configuration for REST catalog */
const TEST_REST_CONFIG: RestCatalogConfig = {
  uri: 'https://catalog.example.com/api/v1',
  warehouse: 's3://test-bucket/warehouse',
  token: 'test-token',
  authType: 'bearer',
};

/** Default test configuration for R2 Data Catalog */
const TEST_R2_CONFIG: R2DataCatalogConfig = {
  accountId: 'test-account-id',
  token: 'test-r2-token',
  baseUrl: 'https://api.test.cloudflare.com',
};

/** Create a mock fetch function with configurable responses */
function createMockFetch(responses: Map<string, { status: number; body: unknown }>) {
  return vi.fn(async (url: string | URL, init?: RequestInit) => {
    const urlStr = url.toString();
    const method = init?.method ?? 'GET';

    for (const [pattern, response] of responses) {
      if (urlStr.includes(pattern) || `${method} ${urlStr}`.includes(pattern)) {
        return new Response(JSON.stringify(response.body), {
          status: response.status,
          headers: { 'Content-Type': 'application/json' },
        });
      }
    }

    return new Response(
      JSON.stringify({
        error: { message: 'Not found', type: 'NoSuchNamespaceException', code: 404 },
      }),
      { status: 404, headers: { 'Content-Type': 'application/json' } }
    );
  });
}

/** Create a basic test schema */
function createTestSchema(schemaId: number = 0): IcebergSchema {
  return {
    type: 'struct',
    'schema-id': schemaId,
    fields: [
      { id: 1, name: '_id', type: 'string', required: true },
      { id: 2, name: 'name', type: 'string', required: false },
      { id: 3, name: 'created_at', type: 'timestamptz', required: false },
      { id: 4, name: 'data', type: 'binary', required: false },
    ],
  };
}

/** Create a test schema with nested types */
function createNestedTestSchema(schemaId: number = 0): IcebergSchema {
  return {
    type: 'struct',
    'schema-id': schemaId,
    fields: [
      { id: 1, name: '_id', type: 'string', required: true },
      {
        id: 2,
        name: 'metadata',
        type: {
          type: 'struct',
          fields: [
            { id: 3, name: 'version', type: 'int', required: true },
            { id: 4, name: 'tags', type: { type: 'list', 'element-id': 5, element: 'string', 'element-required': false }, required: false },
          ],
        },
        required: false,
      },
      {
        id: 6,
        name: 'attributes',
        type: {
          type: 'map',
          'key-id': 7,
          key: 'string',
          'value-id': 8,
          value: 'string',
          'value-required': false,
        },
        required: false,
      },
    ],
  };
}

/** Create a test partition spec */
function createTestPartitionSpec(specId: number = 0): PartitionSpec {
  return {
    'spec-id': specId,
    fields: [
      { 'source-id': 3, 'field-id': 1000, name: 'day', transform: 'day' },
    ],
  };
}

/** Create a multi-field partition spec */
function createMultiFieldPartitionSpec(specId: number = 0): PartitionSpec {
  return {
    'spec-id': specId,
    fields: [
      { 'source-id': 3, 'field-id': 1000, name: 'year', transform: 'year' },
      { 'source-id': 3, 'field-id': 1001, name: 'month', transform: 'month' },
      { 'source-id': 1, 'field-id': 1002, name: 'id_bucket', transform: 'bucket[16]' },
    ],
  };
}

/** Create a test sort order */
function createTestSortOrder(orderId: number = 1): SortOrder {
  return {
    'order-id': orderId,
    fields: [
      { 'source-id': 3, transform: 'identity', direction: 'desc', 'null-order': 'nulls-last' },
      { 'source-id': 1, transform: 'identity', direction: 'asc', 'null-order': 'nulls-first' },
    ],
  };
}

/** Create a test snapshot */
function createTestSnapshot(snapshotId: number, parentId: number | null = null): Snapshot {
  return {
    'snapshot-id': snapshotId,
    'parent-snapshot-id': parentId ?? undefined,
    'sequence-number': snapshotId,
    'timestamp-ms': Date.now() - (1000 - snapshotId) * 1000,
    'manifest-list': `s3://bucket/table/metadata/snap-${snapshotId}.avro`,
    summary: {
      operation: 'append',
      'added-data-files': '10',
      'added-records': '1000',
      'total-data-files': String(snapshotId * 10),
      'total-records': String(snapshotId * 1000),
    },
    'schema-id': 0,
  };
}

/** Create a complete table metadata object */
function createTestTableMetadata(options: {
  location: string;
  schema?: IcebergSchema;
  partitionSpec?: PartitionSpec;
  snapshots?: Snapshot[];
  currentSnapshotId?: number;
}): TableMetadata {
  const schema = options.schema ?? createTestSchema();
  const partitionSpec = options.partitionSpec ?? { 'spec-id': 0, fields: [] };
  const snapshots = options.snapshots ?? [];
  const currentSnapshotId = options.currentSnapshotId ?? -1;

  return {
    'format-version': 2,
    'table-uuid': 'test-uuid-' + Math.random().toString(36).substring(7),
    location: options.location,
    'last-sequence-number': snapshots.length,
    'last-updated-ms': Date.now(),
    'last-column-id': 10,
    'current-schema-id': schema['schema-id'],
    schemas: [schema],
    'default-spec-id': partitionSpec['spec-id'],
    'partition-specs': [partitionSpec],
    'last-partition-id': 1000,
    'default-sort-order-id': 0,
    'sort-orders': [{ 'order-id': 0, fields: [] }],
    properties: {},
    'current-snapshot-id': currentSnapshotId,
    snapshots,
  };
}

/** Mock storage backend for snapshot manager tests */
function createMockStorage() {
  const storage = new Map<string, Uint8Array>();
  return {
    get: vi.fn(async (key: string) => storage.get(key) ?? null),
    put: vi.fn(async (key: string, value: Uint8Array) => { storage.set(key, value); }),
    delete: vi.fn(async (key: string) => { storage.delete(key); }),
    list: vi.fn(async (prefix: string) =>
      Array.from(storage.keys()).filter(k => k.startsWith(prefix))
    ),
    exists: vi.fn(async (key: string) => storage.has(key)),
    _storage: storage,
  };
}

// ============================================================================
// 1. Table Metadata Management Tests
// ============================================================================

describe('Catalog Integration - Table Metadata Management', () => {
  let metadataWriter: MetadataWriter;

  beforeEach(() => {
    metadataWriter = new MetadataWriter({ formatVersion: 2 });
  });

  describe('metadata generation', () => {
    it('should generate valid Iceberg v2 metadata', () => {
      const metadata = metadataWriter.generate({
        location: 's3://bucket/warehouse/db/table',
        schema: createTestSchema(),
      });

      const parsed = JSON.parse(metadata);
      expect(parsed['format-version']).toBe(2);
      expect(parsed['table-uuid']).toBeDefined();
      expect(parsed.location).toBe('s3://bucket/warehouse/db/table');
      expect(parsed.schemas).toHaveLength(1);
      expect(parsed['current-schema-id']).toBe(0);
    });

    it('should include sequence numbers in v2 format', () => {
      const metadata = metadataWriter.generate({
        location: 's3://bucket/table',
        schema: createTestSchema(),
        lastSequenceNumber: 42,
      });

      const parsed = JSON.parse(metadata);
      expect(parsed['last-sequence-number']).toBe(42);
    });

    it('should track last-column-id correctly', () => {
      const metadata = metadataWriter.generate({
        location: 's3://bucket/table',
        schema: createNestedTestSchema(),
      });

      const parsed = JSON.parse(metadata);
      expect(parsed['last-column-id']).toBeGreaterThanOrEqual(8); // Nested fields have IDs up to 8
    });

    it('should preserve multiple schema versions', () => {
      const schema1 = createTestSchema(0);
      const schema2 = createTestSchema(1);
      schema2.fields.push({ id: 5, name: 'extra_field', type: 'string', required: false });

      const metadata = metadataWriter.generate({
        location: 's3://bucket/table',
        schema: schema2,
        schemas: [schema1, schema2],
      });

      const parsed = JSON.parse(metadata);
      expect(parsed.schemas).toHaveLength(2);
      expect(parsed['current-schema-id']).toBe(1);
    });

    it('should support custom table UUID', () => {
      const customUuid = 'custom-uuid-12345';
      const metadata = metadataWriter.generate({
        location: 's3://bucket/table',
        schema: createTestSchema(),
        tableUuid: customUuid,
      });

      const parsed = JSON.parse(metadata);
      expect(parsed['table-uuid']).toBe(customUuid);
    });

    it('should include snapshot references when snapshots exist', () => {
      const snapshots = [createTestSnapshot(1), createTestSnapshot(2, 1)];

      const metadata = metadataWriter.generate({
        location: 's3://bucket/table',
        schema: createTestSchema(),
        snapshots,
        currentSnapshotId: 2,
      });

      const parsed = JSON.parse(metadata);
      expect(parsed.refs).toBeDefined();
      expect(parsed.refs.main).toBeDefined();
      expect(parsed.refs.main['snapshot-id']).toBe(2);
      expect(parsed.refs.main.type).toBe('branch');
    });

    it('should generate snapshot-log from snapshots', () => {
      const snapshots = [createTestSnapshot(1), createTestSnapshot(2, 1)];

      const metadata = metadataWriter.generate({
        location: 's3://bucket/table',
        schema: createTestSchema(),
        snapshots,
        currentSnapshotId: 2,
      });

      const parsed = JSON.parse(metadata);
      expect(parsed['snapshot-log']).toHaveLength(2);
    });
  });

  describe('metadata validation', () => {
    it('should reject empty schema', () => {
      expect(() =>
        metadataWriter.generate({
          location: 's3://bucket/table',
          schema: { type: 'struct', 'schema-id': 0, fields: [] },
        })
      ).toThrow(InvalidSchemaError);
    });

    it('should reject duplicate field IDs', () => {
      expect(() =>
        metadataWriter.generate({
          location: 's3://bucket/table',
          schema: {
            type: 'struct',
            'schema-id': 0,
            fields: [
              { id: 1, name: 'a', type: 'string', required: true },
              { id: 1, name: 'b', type: 'string', required: false },
            ],
          },
        })
      ).toThrow(InvalidSchemaError);
    });

    it('should reject invalid partition transform', () => {
      expect(() =>
        metadataWriter.generate({
          location: 's3://bucket/table',
          schema: createTestSchema(),
          partitionSpec: {
            'spec-id': 0,
            fields: [{ 'source-id': 3, 'field-id': 1000, name: 'invalid', transform: 'invalid_transform' }],
          },
        })
      ).toThrow(InvalidPartitionSpecError);
    });

    it('should reject partition spec referencing non-existent field', () => {
      expect(() =>
        metadataWriter.generate({
          location: 's3://bucket/table',
          schema: createTestSchema(),
          partitionSpec: {
            'spec-id': 0,
            fields: [{ 'source-id': 999, 'field-id': 1000, name: 'missing', transform: 'identity' }],
          },
        })
      ).toThrow(InvalidPartitionSpecError);
    });

    it('should validate current-snapshot-id references existing snapshot', () => {
      expect(() =>
        metadataWriter.generate({
          location: 's3://bucket/table',
          schema: createTestSchema(),
          snapshots: [createTestSnapshot(1)],
          currentSnapshotId: 999, // Non-existent
        })
      ).toThrow();
    });
  });
});

// ============================================================================
// 2. Schema Evolution Tests
// ============================================================================

describe('Catalog Integration - Schema Evolution', () => {
  let schemaTracker: SchemaTracker;

  beforeEach(() => {
    schemaTracker = new SchemaTracker(createTestSchema());
  });

  describe('field additions', () => {
    it('should add optional field and increment schema ID', () => {
      const { schema, fieldId } = schemaTracker.addField({
        name: 'new_field',
        type: 'string',
      });

      expect(schema['schema-id']).toBe(1);
      expect(schema.fields.find(f => f.name === 'new_field')).toBeDefined();
      expect(fieldId).toBe(5); // After existing fields 1-4
    });

    it('should reject adding required field (backwards incompatible)', () => {
      expect(() =>
        schemaTracker.addField({
          name: 'required_field',
          type: 'string',
          required: true,
        })
      ).toThrow(/backwards compatibility/i);
    });

    it('should add nested field to struct', () => {
      // First add a struct field
      schemaTracker.addField({
        name: 'nested',
        type: { type: 'struct', fields: [{ id: 0, name: 'inner', type: 'string', required: false }] },
      });

      // Then add a field to it
      const { schema, fieldId } = schemaTracker.addField({
        name: 'inner_new',
        type: 'int',
        parentFieldId: 5, // The struct field we just added
      });

      expect(schema['schema-id']).toBe(2);
      expect(fieldId).toBeGreaterThan(5);
    });

    it('should support adding list type field', () => {
      const { schema } = schemaTracker.addField({
        name: 'tags',
        type: { type: 'list', 'element-id': 0, element: 'string', 'element-required': false },
      });

      const tagsField = schema.fields.find(f => f.name === 'tags');
      expect(tagsField).toBeDefined();
      expect(typeof tagsField?.type).toBe('object');
    });

    it('should support adding map type field', () => {
      const { schema } = schemaTracker.addField({
        name: 'properties',
        type: {
          type: 'map',
          'key-id': 0,
          key: 'string',
          'value-id': 0,
          value: 'string',
          'value-required': false,
        },
      });

      const propsField = schema.fields.find(f => f.name === 'properties');
      expect(propsField).toBeDefined();
    });
  });

  describe('field removals', () => {
    it('should remove optional field', () => {
      const schema = schemaTracker.removeField(2); // 'name' field (optional)

      expect(schema.fields.find(f => f.id === 2)).toBeUndefined();
      expect(schema['schema-id']).toBe(1);
    });

    it('should reject removing required field without making optional first', () => {
      expect(() =>
        schemaTracker.removeField(1) // '_id' field (required)
      ).toThrow(/required.*optional/i);
    });

    it('should allow removing field after making it optional', () => {
      schemaTracker.makeFieldOptional(1); // Make '_id' optional
      const schema = schemaTracker.removeField(1);

      expect(schema.fields.find(f => f.id === 1)).toBeUndefined();
    });
  });

  describe('field modifications', () => {
    it('should rename field preserving field ID', () => {
      const schema = schemaTracker.renameField(2, 'full_name');

      const field = schema.fields.find(f => f.id === 2);
      expect(field?.name).toBe('full_name');
    });

    it('should make required field optional', () => {
      const schema = schemaTracker.makeFieldOptional(1);

      const field = schema.fields.find(f => f.id === 1);
      expect(field?.required).toBe(false);
    });

    it('should update field documentation', () => {
      const schema = schemaTracker.updateFieldDoc(1, 'The unique document identifier');

      const field = schema.fields.find(f => f.id === 1);
      expect(field?.doc).toBe('The unique document identifier');
    });

    it('should reject making optional field required (backwards incompatible)', () => {
      const initialSchema = createTestSchema();
      // Field 2 is already optional, creating a new schema where it's required
      // should be detected as a breaking change
      const newSchema = { ...initialSchema, 'schema-id': 1 };
      newSchema.fields = initialSchema.fields.map(f =>
        f.id === 2 ? { ...f, required: true } : f
      );

      const result = schemaTracker.setSchema(newSchema);
      expect(result.compatible).toBe(false);
      expect(result.breakingChanges.length).toBeGreaterThan(0);
    });
  });

  describe('type evolution', () => {
    it('should allow widening int to long', () => {
      const initialSchema: IcebergSchema = {
        type: 'struct',
        'schema-id': 0,
        fields: [
          { id: 1, name: 'count', type: 'int', required: false },
        ],
      };
      const tracker = new SchemaTracker(initialSchema);

      const widenedSchema: IcebergSchema = {
        type: 'struct',
        'schema-id': 1,
        fields: [
          { id: 1, name: 'count', type: 'long', required: false },
        ],
      };

      const result = tracker.setSchema(widenedSchema);
      expect(result.compatible).toBe(true);
      expect(result.changes.some(c => c.type === 'widen-type')).toBe(true);
    });

    it('should allow widening float to double', () => {
      const initialSchema: IcebergSchema = {
        type: 'struct',
        'schema-id': 0,
        fields: [
          { id: 1, name: 'value', type: 'float', required: false },
        ],
      };
      const tracker = new SchemaTracker(initialSchema);

      const widenedSchema: IcebergSchema = {
        type: 'struct',
        'schema-id': 1,
        fields: [
          { id: 1, name: 'value', type: 'double', required: false },
        ],
      };

      const result = tracker.setSchema(widenedSchema);
      expect(result.compatible).toBe(true);
    });

    it('should reject narrowing long to int (data loss)', () => {
      const initialSchema: IcebergSchema = {
        type: 'struct',
        'schema-id': 0,
        fields: [
          { id: 1, name: 'count', type: 'long', required: false },
        ],
      };
      const tracker = new SchemaTracker(initialSchema);

      const narrowedSchema: IcebergSchema = {
        type: 'struct',
        'schema-id': 1,
        fields: [
          { id: 1, name: 'count', type: 'int', required: false },
        ],
      };

      const result = tracker.setSchema(narrowedSchema);
      expect(result.compatible).toBe(false);
      expect(result.breakingChanges.length).toBeGreaterThan(0);
    });
  });

  describe('evolution metadata', () => {
    it('should track all schema changes', () => {
      schemaTracker.addField({ name: 'f1', type: 'string' });
      schemaTracker.addField({ name: 'f2', type: 'int' });
      schemaTracker.renameField(2, 'renamed');
      schemaTracker.makeFieldOptional(1);

      const changes = schemaTracker.getAllChanges();
      expect(changes.length).toBe(4);
    });

    it('should provide evolution summary', () => {
      schemaTracker.addField({ name: 'f1', type: 'string' });
      schemaTracker.removeField(2); // Remove optional 'name' field

      const summary = schemaTracker.getEvolutionSummary();
      expect(summary.schemaCount).toBe(3);
      expect(summary.fieldAdditions).toBe(1);
      expect(summary.fieldRemovals).toBe(1);
    });
  });
});

// ============================================================================
// 3. Partition Spec Handling Tests
// ============================================================================

describe('Catalog Integration - Partition Spec Handling', () => {
  let metadataWriter: MetadataWriter;

  beforeEach(() => {
    metadataWriter = new MetadataWriter({ formatVersion: 2 });
  });

  describe('partition spec creation', () => {
    it('should create unpartitioned table (spec-id 0, empty fields)', () => {
      const metadata = metadataWriter.generate({
        location: 's3://bucket/table',
        schema: createTestSchema(),
      });

      const parsed = JSON.parse(metadata);
      expect(parsed['partition-specs']).toHaveLength(1);
      expect(parsed['partition-specs'][0]['spec-id']).toBe(0);
      expect(parsed['partition-specs'][0].fields).toHaveLength(0);
    });

    it('should create table with day partition', () => {
      const metadata = metadataWriter.generate({
        location: 's3://bucket/table',
        schema: createTestSchema(),
        partitionSpec: createTestPartitionSpec(),
      });

      const parsed = JSON.parse(metadata);
      expect(parsed['partition-specs'][0].fields[0].transform).toBe('day');
    });

    it('should support bucket partitioning', () => {
      const metadata = metadataWriter.generate({
        location: 's3://bucket/table',
        schema: createTestSchema(),
        partitionSpec: {
          'spec-id': 0,
          fields: [{ 'source-id': 1, 'field-id': 1000, name: 'id_bucket', transform: 'bucket[16]' }],
        },
      });

      const parsed = JSON.parse(metadata);
      expect(parsed['partition-specs'][0].fields[0].transform).toBe('bucket[16]');
    });

    it('should support truncate partitioning', () => {
      const metadata = metadataWriter.generate({
        location: 's3://bucket/table',
        schema: createTestSchema(),
        partitionSpec: {
          'spec-id': 0,
          fields: [{ 'source-id': 2, 'field-id': 1000, name: 'name_trunc', transform: 'truncate[4]' }],
        },
      });

      const parsed = JSON.parse(metadata);
      expect(parsed['partition-specs'][0].fields[0].transform).toBe('truncate[4]');
    });

    it('should support multi-field partitioning', () => {
      const metadata = metadataWriter.generate({
        location: 's3://bucket/table',
        schema: createTestSchema(),
        partitionSpec: createMultiFieldPartitionSpec(),
      });

      const parsed = JSON.parse(metadata);
      expect(parsed['partition-specs'][0].fields).toHaveLength(3);
    });

    it('should support void transform for dropped partitions', () => {
      const metadata = metadataWriter.generate({
        location: 's3://bucket/table',
        schema: createTestSchema(),
        partitionSpec: {
          'spec-id': 1,
          fields: [
            { 'source-id': 3, 'field-id': 1000, name: 'day', transform: 'void' }, // Dropped
            { 'source-id': 1, 'field-id': 1001, name: 'id_bucket', transform: 'bucket[16]' },
          ],
        },
      });

      const parsed = JSON.parse(metadata);
      expect(parsed['partition-specs'][0].fields[0].transform).toBe('void');
    });
  });

  describe('partition evolution', () => {
    it('should track partition spec history', () => {
      const spec1 = createTestPartitionSpec(0);
      const spec2 = createMultiFieldPartitionSpec(1);

      const metadata = metadataWriter.generate({
        location: 's3://bucket/table',
        schema: createTestSchema(),
        partitionSpec: spec2,
        partitionSpecs: [spec1, spec2],
      });

      const parsed = JSON.parse(metadata);
      expect(parsed['partition-specs']).toHaveLength(2);
      expect(parsed['default-spec-id']).toBe(1);
    });

    it('should track last-partition-id in v2', () => {
      const metadata = metadataWriter.generate({
        location: 's3://bucket/table',
        schema: createTestSchema(),
        partitionSpec: createMultiFieldPartitionSpec(),
      });

      const parsed = JSON.parse(metadata);
      expect(parsed['last-partition-id']).toBeGreaterThanOrEqual(1002);
    });
  });
});

// ============================================================================
// 4. Snapshot Management Tests
// ============================================================================

describe('Catalog Integration - Snapshot Management', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let snapshotManager: SnapshotManager;

  beforeEach(async () => {
    storage = createMockStorage();
    snapshotManager = new SnapshotManager(storage as any, 'test-table', {});
    await snapshotManager.initialize();
  });

  describe('snapshot creation', () => {
    it('should create first snapshot with sequence number 1', async () => {
      const snapshot = await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      });

      expect(snapshot.sequenceNumber).toBe(1n);
      expect(snapshot.parentSnapshotId).toBeNull();
    });

    it('should increment sequence number for subsequent snapshots', async () => {
      await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      });

      const snapshot2 = await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-2.avro',
      });

      expect(snapshot2.sequenceNumber).toBe(2n);
    });

    it('should track parent snapshot correctly', async () => {
      const snapshot1 = await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      });

      const snapshot2 = await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-2.avro',
      });

      expect(snapshot2.parentSnapshotId).toBe(snapshot1.snapshotId);
    });

    it('should support all operation types', async () => {
      const operations: OperationType[] = ['append', 'overwrite', 'delete', 'replace'];

      for (const operation of operations) {
        const snapshot = await snapshotManager.createSnapshot({
          operation,
          manifestListPath: `s3://bucket/table/metadata/snap-${operation}.avro`,
        });
        expect(snapshot.summary.operation).toBe(operation);
      }
    });

    it('should include snapshot summary statistics', async () => {
      const snapshot = await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
        summary: {
          'added-data-files': '5',
          'added-records': '1000',
          'total-data-files': '5',
          'total-records': '1000',
        },
      });

      expect(snapshot.summary['added-data-files']).toBe('5');
      expect(snapshot.summary['added-records']).toBe('1000');
    });
  });

  describe('snapshot retrieval', () => {
    it('should get current snapshot', async () => {
      await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      });

      const snapshot2 = await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-2.avro',
      });

      const current = await snapshotManager.getCurrentSnapshot();
      expect(current?.snapshotId).toBe(snapshot2.snapshotId);
    });

    it('should get snapshot by ID', async () => {
      const snapshot = await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      });

      const retrieved = await snapshotManager.getSnapshot(snapshot.snapshotId);
      expect(retrieved?.snapshotId).toBe(snapshot.snapshotId);
    });

    it('should list snapshots with pagination', async () => {
      for (let i = 0; i < 5; i++) {
        await snapshotManager.createSnapshot({
          operation: 'append',
          manifestListPath: `s3://bucket/table/metadata/snap-${i}.avro`,
        });
      }

      const page1 = await snapshotManager.listSnapshots({ limit: 2 });
      expect(page1).toHaveLength(2);

      const page2 = await snapshotManager.listSnapshots({ limit: 2, offset: 2 });
      expect(page2).toHaveLength(2);
    });
  });

  describe('time travel', () => {
    it('should get snapshot as-of timestamp', async () => {
      const snapshot1 = await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
        timestampMs: Date.now() - 10000,
      });

      await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-2.avro',
        timestampMs: Date.now(),
      });

      const asOf = await snapshotManager.getSnapshotAsOf(Date.now() - 5000);
      expect(asOf?.snapshotId).toBe(snapshot1.snapshotId);
    });

    it('should get snapshot ancestry (parent chain)', async () => {
      const snapshot1 = await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      });

      const snapshot2 = await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-2.avro',
      });

      const snapshot3 = await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-3.avro',
      });

      const ancestry = await snapshotManager.getSnapshotAncestry(snapshot3.snapshotId);
      expect(ancestry).toHaveLength(2);
      expect(ancestry[0]?.snapshotId).toBe(snapshot2.snapshotId);
      expect(ancestry[1]?.snapshotId).toBe(snapshot1.snapshotId);
    });
  });

  describe('snapshot rollback', () => {
    it('should rollback to previous snapshot', async () => {
      const snapshot1 = await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      });

      await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-2.avro',
      });

      const result = await snapshotManager.rollbackToSnapshot(snapshot1.snapshotId);

      expect(result.targetSnapshotId).toBe(snapshot1.snapshotId);
      expect(snapshotManager.getCurrentSnapshotId()).toBe(snapshot1.snapshotId);
    });

    it('should create rollback snapshot in history', async () => {
      const snapshot1 = await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      });

      await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-2.avro',
      });

      const result = await snapshotManager.rollbackToSnapshot(snapshot1.snapshotId);

      const rollbackSnapshot = await snapshotManager.getSnapshot(result.newSnapshotId);
      expect(rollbackSnapshot?.summary['rollback-to-snapshot-id']).toBe(snapshot1.snapshotId.toString());
    });
  });

  describe('snapshot expiration', () => {
    it('should expire snapshots older than threshold', async () => {
      const oldTimestamp = Date.now() - 100000;

      await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
        timestampMs: oldTimestamp,
      });

      await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-2.avro',
        timestampMs: Date.now(),
      });

      const result = await snapshotManager.expireSnapshots({
        olderThanMs: Date.now() - 50000,
        minSnapshotsToRetain: 1,
      });

      expect(result.expiredCount).toBe(1);
    });

    it('should retain minimum number of snapshots', async () => {
      const oldTimestamp = Date.now() - 100000;

      for (let i = 0; i < 3; i++) {
        await snapshotManager.createSnapshot({
          operation: 'append',
          manifestListPath: `s3://bucket/table/metadata/snap-${i}.avro`,
          timestampMs: oldTimestamp + i * 1000,
        });
      }

      const result = await snapshotManager.expireSnapshots({
        olderThanMs: Date.now(),
        minSnapshotsToRetain: 2,
      });

      const remaining = await snapshotManager.listSnapshots();
      expect(remaining.length).toBeGreaterThanOrEqual(2);
    });

    it('should never expire current snapshot', async () => {
      const snapshot = await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
        timestampMs: Date.now() - 100000,
      });

      const result = await snapshotManager.expireSnapshots({
        olderThanMs: Date.now(),
        minSnapshotsToRetain: 0,
      });

      expect(result.expiredSnapshots).not.toContain(snapshot.snapshotId);
    });
  });

  describe('cherry-pick', () => {
    it('should cherry-pick changes from another snapshot', async () => {
      const snapshot1 = await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-1.avro',
      });

      // Create branch scenario - snapshot from main
      await snapshotManager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/table/metadata/snap-2.avro',
      });

      const result = await snapshotManager.cherryPick(snapshot1.snapshotId);

      expect(result.sourceSnapshotId).toBe(snapshot1.snapshotId);
      const newSnapshot = await snapshotManager.getSnapshot(result.newSnapshotId);
      expect(newSnapshot?.summary['cherry-pick-source-snapshot-id']).toBe(snapshot1.snapshotId.toString());
    });
  });
});

// ============================================================================
// 5. Table Creation and Deletion Tests
// ============================================================================

describe('Catalog Integration - Table Creation and Deletion', () => {
  let catalog: RestCatalog;
  let mockFetch: ReturnType<typeof createMockFetch>;

  beforeEach(() => {
    mockFetch = createMockFetch(new Map());
    catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });
  });

  describe('table creation', () => {
    it('should create table with minimal configuration', async () => {
      mockFetch = createMockFetch(new Map([
        ['tables', {
          status: 200,
          body: {
            metadataLocation: 's3://bucket/db/users/metadata/v1.metadata.json',
            metadata: createTestTableMetadata({ location: 's3://bucket/db/users' }),
          },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const response = await catalog.createTable(['db'], {
        name: 'users',
        schema: createTestSchema(),
      });

      expect(response.metadata['format-version']).toBe(2);
      expect(response.metadataLocation).toContain('v1.metadata.json');
    });

    it('should create table with partition spec', async () => {
      mockFetch = createMockFetch(new Map([
        ['tables', {
          status: 200,
          body: {
            metadataLocation: 's3://bucket/db/events/metadata/v1.metadata.json',
            metadata: createTestTableMetadata({
              location: 's3://bucket/db/events',
              partitionSpec: createTestPartitionSpec(),
            }),
          },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const response = await catalog.createTable(['db'], {
        name: 'events',
        schema: createTestSchema(),
        partitionSpec: createTestPartitionSpec(),
      });

      expect(response.metadata['partition-specs'][0].fields).toHaveLength(1);
    });

    it('should create table with sort order', async () => {
      mockFetch = createMockFetch(new Map([
        ['tables', {
          status: 200,
          body: {
            metadataLocation: 's3://bucket/db/logs/metadata/v1.metadata.json',
            metadata: {
              ...createTestTableMetadata({ location: 's3://bucket/db/logs' }),
              'default-sort-order-id': 1,
              'sort-orders': [{ 'order-id': 0, fields: [] }, createTestSortOrder(1)],
            },
          },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const response = await catalog.createTable(['db'], {
        name: 'logs',
        schema: createTestSchema(),
        writeOrder: createTestSortOrder(1),
      });

      expect(response.metadata['default-sort-order-id']).toBe(1);
    });

    it('should create table with custom properties', async () => {
      mockFetch = createMockFetch(new Map([
        ['tables', {
          status: 200,
          body: {
            metadataLocation: 's3://bucket/db/table/metadata/v1.metadata.json',
            metadata: {
              ...createTestTableMetadata({ location: 's3://bucket/db/table' }),
              properties: {
                'write.format.default': 'parquet',
                'write.parquet.compression-codec': 'zstd',
                'mongolake.collection': 'myCollection',
              },
            },
          },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const response = await catalog.createTable(['db'], {
        name: 'table',
        schema: createTestSchema(),
        properties: {
          'write.format.default': 'parquet',
          'write.parquet.compression-codec': 'zstd',
          'mongolake.collection': 'myCollection',
        },
      });

      expect(response.metadata.properties['write.format.default']).toBe('parquet');
    });

    it('should create staged table (for atomic operations)', async () => {
      mockFetch = createMockFetch(new Map([
        ['tables', {
          status: 200,
          body: {
            metadataLocation: 's3://bucket/db/staged/metadata/v0.metadata.json',
            metadata: createTestTableMetadata({ location: 's3://bucket/db/staged' }),
          },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      await catalog.createTable(['db'], {
        name: 'staged',
        schema: createTestSchema(),
        stageCreate: true,
      });

      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          body: expect.stringContaining('stageCreate'),
        })
      );
    });

    it('should throw AlreadyExistsError when table exists', async () => {
      mockFetch = createMockFetch(new Map([
        ['tables', {
          status: 409,
          body: { error: { message: 'Table already exists', type: 'AlreadyExistsException', code: 409 } },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      await expect(
        catalog.createTable(['db'], { name: 'existing', schema: createTestSchema() })
      ).rejects.toThrow(AlreadyExistsError);
    });
  });

  describe('table deletion', () => {
    it('should drop table from catalog', async () => {
      mockFetch = createMockFetch(new Map([
        ['DELETE', { status: 200, body: {} }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      await expect(catalog.dropTable(['db'], 'users')).resolves.not.toThrow();
      expect(mockFetch).toHaveBeenCalledWith(
        expect.stringContaining('tables'),
        expect.objectContaining({ method: 'DELETE' })
      );
    });

    it('should purge table data when requested', async () => {
      mockFetch = createMockFetch(new Map([
        ['DELETE', { status: 200, body: {} }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      await catalog.dropTable(['db'], 'users', true);

      expect(mockFetch).toHaveBeenCalledWith(
        expect.stringContaining('purgeRequested=true'),
        expect.any(Object)
      );
    });

    it('should throw NotFoundError when dropping non-existent table', async () => {
      mockFetch = createMockFetch(new Map([
        ['tables', {
          status: 404,
          body: { error: { message: 'Table not found', type: 'NoSuchTableException', code: 404 } },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      await expect(catalog.dropTable(['db'], 'nonexistent')).rejects.toThrow(NotFoundError);
    });

    it('should rename table within namespace', async () => {
      mockFetch = createMockFetch(new Map([
        ['rename', { status: 200, body: {} }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      await catalog.renameTable(
        { namespace: ['db'], name: 'old_name' },
        { namespace: ['db'], name: 'new_name' }
      );

      expect(mockFetch).toHaveBeenCalledWith(
        expect.stringContaining('rename'),
        expect.objectContaining({ method: 'POST' })
      );
    });

    it('should move table to different namespace', async () => {
      mockFetch = createMockFetch(new Map([
        ['rename', { status: 200, body: {} }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

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

  describe('table registration', () => {
    it('should register existing metadata file as table', async () => {
      mockFetch = createMockFetch(new Map([
        ['register', {
          status: 200,
          body: {
            metadataLocation: 's3://bucket/external/metadata/v10.metadata.json',
            metadata: createTestTableMetadata({ location: 's3://bucket/external' }),
          },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const response = await catalog.registerTable(
        ['db'],
        'imported',
        's3://bucket/external/metadata/v10.metadata.json'
      );

      expect(response.metadataLocation).toContain('v10.metadata.json');
    });
  });
});

// ============================================================================
// 6. Catalog Listing Operations Tests
// ============================================================================

describe('Catalog Integration - Catalog Listing Operations', () => {
  let catalog: RestCatalog;
  let mockFetch: ReturnType<typeof createMockFetch>;

  beforeEach(() => {
    mockFetch = createMockFetch(new Map());
    catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });
  });

  describe('namespace listing', () => {
    it('should list all namespaces', async () => {
      mockFetch = createMockFetch(new Map([
        ['namespaces', {
          status: 200,
          body: { namespaces: [['db1'], ['db2'], ['db3']] },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const response = await catalog.listNamespaces();

      expect(response.namespaces).toHaveLength(3);
      expect(response.namespaces).toContainEqual(['db1']);
    });

    it('should list namespaces under parent', async () => {
      mockFetch = createMockFetch(new Map([
        ['namespaces', {
          status: 200,
          body: { namespaces: [['mongolake', 'prod'], ['mongolake', 'staging']] },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const response = await catalog.listNamespaces(['mongolake']);

      expect(response.namespaces).toHaveLength(2);
    });

    it('should handle namespace pagination', async () => {
      mockFetch = createMockFetch(new Map([
        ['namespaces', {
          status: 200,
          body: {
            namespaces: [['page2-ns']],
            nextPageToken: 'next-token',
          },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const response = await catalog.listNamespaces(undefined, 'page1-token', 10);

      expect(response.nextPageToken).toBe('next-token');
      expect(mockFetch).toHaveBeenCalledWith(
        expect.stringContaining('pageToken=page1-token'),
        expect.any(Object)
      );
    });
  });

  describe('table listing', () => {
    it('should list tables in namespace', async () => {
      mockFetch = createMockFetch(new Map([
        ['tables', {
          status: 200,
          body: {
            identifiers: [
              { namespace: ['db'], name: 'users' },
              { namespace: ['db'], name: 'orders' },
              { namespace: ['db'], name: 'products' },
            ],
          },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const response = await catalog.listTables(['db']);

      expect(response.identifiers).toHaveLength(3);
      expect(response.identifiers.map(t => t.name)).toContain('users');
    });

    it('should handle table pagination', async () => {
      mockFetch = createMockFetch(new Map([
        ['tables', {
          status: 200,
          body: {
            identifiers: [{ namespace: ['db'], name: 'table1' }],
            nextPageToken: 'next-table-token',
          },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const response = await catalog.listTables(['db'], 'page-token', 50);

      expect(response.nextPageToken).toBe('next-table-token');
    });

    it('should return empty array for empty namespace', async () => {
      mockFetch = createMockFetch(new Map([
        ['tables', {
          status: 200,
          body: { identifiers: [] },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const response = await catalog.listTables(['empty-ns']);

      expect(response.identifiers).toEqual([]);
    });
  });

  describe('MongoLake convenience methods', () => {
    it('should list MongoLake databases', async () => {
      mockFetch = createMockFetch(new Map([
        ['namespaces', {
          status: 200,
          body: {
            namespaces: [
              ['mongolake', 'myapp'],
              ['mongolake', 'analytics'],
              ['mongolake', 'logs'],
            ],
          },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const databases = await catalog.listMongoLakeDatabases();

      expect(databases).toContain('myapp');
      expect(databases).toContain('analytics');
      expect(databases).toContain('logs');
    });

    it('should list MongoLake collections in database', async () => {
      mockFetch = createMockFetch(new Map([
        ['tables', {
          status: 200,
          body: {
            identifiers: [
              { namespace: ['mongolake', 'myapp'], name: 'users' },
              { namespace: ['mongolake', 'myapp'], name: 'orders' },
            ],
          },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const collections = await catalog.listMongoLakeCollections('myapp');

      expect(collections).toContain('users');
      expect(collections).toContain('orders');
    });

    it('should return empty array when mongolake namespace does not exist', async () => {
      mockFetch = createMockFetch(new Map([
        ['namespaces', {
          status: 404,
          body: { error: { message: 'Not found', type: 'NoSuchNamespaceException', code: 404 } },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const databases = await catalog.listMongoLakeDatabases();

      expect(databases).toEqual([]);
    });
  });
});

// ============================================================================
// 7. Table Properties Tests
// ============================================================================

describe('Catalog Integration - Table Properties', () => {
  let catalog: RestCatalog;
  let mockFetch: ReturnType<typeof createMockFetch>;

  beforeEach(() => {
    mockFetch = createMockFetch(new Map());
    catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });
  });

  describe('property management', () => {
    it('should set table properties', async () => {
      mockFetch = createMockFetch(new Map([
        ['POST', {
          status: 200,
          body: {
            metadataLocation: 's3://bucket/db/table/metadata/v2.metadata.json',
            metadata: {
              ...createTestTableMetadata({ location: 's3://bucket/db/table' }),
              properties: {
                'write.format.default': 'parquet',
                'write.parquet.compression-codec': 'zstd',
              },
            },
          },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const updates: TableUpdate[] = [
        {
          action: 'set-properties',
          updates: {
            'write.format.default': 'parquet',
            'write.parquet.compression-codec': 'zstd',
          },
        },
      ];

      const response = await catalog.updateTable(['db'], 'table', [], updates);

      expect(response.metadata.properties['write.format.default']).toBe('parquet');
    });

    it('should remove table properties', async () => {
      mockFetch = createMockFetch(new Map([
        ['POST', {
          status: 200,
          body: {
            metadataLocation: 's3://bucket/db/table/metadata/v2.metadata.json',
            metadata: {
              ...createTestTableMetadata({ location: 's3://bucket/db/table' }),
              properties: {},
            },
          },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const updates: TableUpdate[] = [
        { action: 'remove-properties', removals: ['old.property'] },
      ];

      const response = await catalog.updateTable(['db'], 'table', [], updates);

      expect(response.metadata.properties['old.property']).toBeUndefined();
    });

    it('should update table location', async () => {
      mockFetch = createMockFetch(new Map([
        ['POST', {
          status: 200,
          body: {
            metadataLocation: 's3://new-bucket/db/table/metadata/v2.metadata.json',
            metadata: createTestTableMetadata({ location: 's3://new-bucket/db/table' }),
          },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const updates: TableUpdate[] = [
        { action: 'set-location', location: 's3://new-bucket/db/table' },
      ];

      const response = await catalog.updateTable(['db'], 'table', [], updates);

      expect(response.metadata.location).toBe('s3://new-bucket/db/table');
    });
  });

  describe('standard properties', () => {
    it.todo('should support write.format.default property');
    it.todo('should support write.parquet.compression-codec property');
    it.todo('should support write.parquet.row-group-size-bytes property');
    it.todo('should support commit.retry.num-retries property');
    it.todo('should support history.expire.max-snapshot-age-ms property');
    it.todo('should support history.expire.min-snapshots-to-keep property');
  });

  describe('MongoLake properties', () => {
    it.todo('should support mongolake.database property');
    it.todo('should support mongolake.collection property');
    it.todo('should support mongolake.sync.enabled property');
    it.todo('should support mongolake.schema.variant property');
  });
});

// ============================================================================
// 8. Concurrent Access Patterns Tests
// ============================================================================

describe('Catalog Integration - Concurrent Access Patterns', () => {
  let catalog: RestCatalog;
  let mockFetch: ReturnType<typeof createMockFetch>;

  beforeEach(() => {
    mockFetch = createMockFetch(new Map());
    catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });
  });

  describe('optimistic concurrency control', () => {
    it('should enforce table UUID requirement', async () => {
      mockFetch = createMockFetch(new Map([
        ['POST', {
          status: 200,
          body: {
            metadataLocation: 's3://bucket/db/table/metadata/v2.metadata.json',
            metadata: createTestTableMetadata({ location: 's3://bucket/db/table' }),
          },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const requirements: TableRequirement[] = [
        { type: 'assert-table-uuid', uuid: 'expected-uuid' },
      ];

      await catalog.updateTable(['db'], 'table', requirements, []);

      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          body: expect.stringContaining('assert-table-uuid'),
        })
      );
    });

    it('should enforce ref snapshot requirement', async () => {
      mockFetch = createMockFetch(new Map([
        ['POST', {
          status: 200,
          body: {
            metadataLocation: 's3://bucket/db/table/metadata/v2.metadata.json',
            metadata: createTestTableMetadata({ location: 's3://bucket/db/table' }),
          },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const requirements: TableRequirement[] = [
        { type: 'assert-ref-snapshot-id', ref: 'main', 'snapshot-id': 123 },
      ];

      await catalog.updateTable(['db'], 'table', requirements, []);

      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          body: expect.stringContaining('assert-ref-snapshot-id'),
        })
      );
    });

    it('should throw CommitFailedError on concurrent modification', async () => {
      mockFetch = createMockFetch(new Map([
        ['POST', {
          status: 409,
          body: {
            error: {
              message: 'Concurrent modification detected',
              type: 'CommitFailedException',
              code: 409,
            },
          },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      await expect(
        catalog.updateTable(
          ['db'],
          'table',
          [{ type: 'assert-table-uuid', uuid: 'stale-uuid' }],
          []
        )
      ).rejects.toThrow(CommitFailedError);
    });

    it('should enforce current schema ID requirement', async () => {
      mockFetch = createMockFetch(new Map([
        ['POST', {
          status: 200,
          body: {
            metadataLocation: 's3://bucket/db/table/metadata/v2.metadata.json',
            metadata: createTestTableMetadata({ location: 's3://bucket/db/table' }),
          },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const requirements: TableRequirement[] = [
        { type: 'assert-current-schema-id', 'current-schema-id': 0 },
      ];

      await catalog.updateTable(['db'], 'table', requirements, []);

      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          body: expect.stringContaining('assert-current-schema-id'),
        })
      );
    });
  });

  describe('multi-table transactions', () => {
    it('should commit updates to multiple tables atomically', async () => {
      mockFetch = createMockFetch(new Map([
        ['transactions/commit', { status: 200, body: {} }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const commits = [
        {
          identifier: { namespace: ['db'], name: 'table1' },
          requirements: [{ type: 'assert-table-uuid' as const, uuid: 'uuid-1' }],
          updates: [{ action: 'set-properties' as const, updates: { key: 'value1' } }],
        },
        {
          identifier: { namespace: ['db'], name: 'table2' },
          requirements: [{ type: 'assert-table-uuid' as const, uuid: 'uuid-2' }],
          updates: [{ action: 'set-properties' as const, updates: { key: 'value2' } }],
        },
      ];

      await expect(catalog.commitTransaction(commits)).resolves.not.toThrow();
    });

    it('should rollback all changes if any table commit fails', async () => {
      mockFetch = createMockFetch(new Map([
        ['transactions/commit', {
          status: 409,
          body: {
            error: { message: 'Commit failed for table2', type: 'CommitFailedException', code: 409 },
          },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const commits = [
        {
          identifier: { namespace: ['db'], name: 'table1' },
          requirements: [],
          updates: [],
        },
        {
          identifier: { namespace: ['db'], name: 'table2' },
          requirements: [],
          updates: [],
        },
      ];

      await expect(catalog.commitTransaction(commits)).rejects.toThrow(CommitFailedError);
    });
  });

  describe('snapshot manager concurrency', () => {
    it('should handle concurrent snapshot creation with OCC', async () => {
      const storage = createMockStorage();
      const manager = new SnapshotManager(storage as any, 'test-table', {});
      await manager.initialize();

      // Simulate OCC by setting expected parent
      const snapshot1 = await manager.createSnapshot({
        operation: 'append',
        manifestListPath: 's3://bucket/snap-1.avro',
      });

      // This should fail if another snapshot was created
      await expect(
        manager.createSnapshot({
          operation: 'append',
          manifestListPath: 's3://bucket/snap-2.avro',
          expectedParentSnapshotId: 0n, // Wrong expected parent
        })
      ).rejects.toThrow(/concurrent modification/i);
    });
  });
});

// ============================================================================
// 9. Error Handling Tests
// ============================================================================

describe('Catalog Integration - Error Handling', () => {
  let catalog: RestCatalog;

  describe('catalog errors', () => {
    it('should handle NotFoundError for missing namespace', async () => {
      const mockFetch = createMockFetch(new Map([
        ['namespaces', {
          status: 404,
          body: { error: { message: 'Namespace not found', type: 'NoSuchNamespaceException', code: 404 } },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      await expect(catalog.getNamespace(['nonexistent'])).rejects.toThrow(NotFoundError);
    });

    it('should handle NotFoundError for missing table', async () => {
      const mockFetch = createMockFetch(new Map([
        ['tables', {
          status: 404,
          body: { error: { message: 'Table not found', type: 'NoSuchTableException', code: 404 } },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      await expect(catalog.loadTable(['db'], 'nonexistent')).rejects.toThrow(NotFoundError);
    });

    it('should handle ValidationError for invalid requests', async () => {
      const mockFetch = createMockFetch(new Map([
        ['namespaces', {
          status: 400,
          body: { error: { message: 'Invalid namespace', type: 'BadRequestException', code: 400 } },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      await expect(catalog.createNamespace(['valid'])).rejects.toThrow(ValidationError);
    });

    it('should handle authentication errors', async () => {
      const mockFetch = createMockFetch(new Map([
        ['namespaces', {
          status: 401,
          body: { error: { message: 'Invalid token', type: 'NotAuthorizedException', code: 401 } },
        }],
      ]));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      const { AuthenticationError } = await import('../../../src/iceberg/rest-catalog.js');
      await expect(catalog.listNamespaces()).rejects.toThrow(AuthenticationError);
    });

    it('should handle timeout errors', async () => {
      const mockFetch = vi.fn(() => {
        const error = new Error('The operation was aborted');
        error.name = 'AbortError';
        return Promise.reject(error);
      });

      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch, timeoutMs: 100 });

      await expect(catalog.listNamespaces()).rejects.toThrow(/timeout/i);
    });

    it('should handle network errors', async () => {
      const mockFetch = vi.fn(() => Promise.reject(new Error('ECONNREFUSED')));
      catalog = new RestCatalog({ ...TEST_REST_CONFIG, fetch: mockFetch });

      await expect(catalog.listNamespaces()).rejects.toThrow('ECONNREFUSED');
    });
  });

  describe('metadata writer errors', () => {
    it('should throw for unsupported format version', () => {
      expect(() => new MetadataWriter({ formatVersion: 3 })).toThrow(/unsupported.*version/i);
    });

    it('should throw for missing location', () => {
      const writer = new MetadataWriter();
      expect(() =>
        writer.generate({
          location: '',
          schema: createTestSchema(),
        })
      ).toThrow(/location.*required/i);
    });
  });

  describe('snapshot manager errors', () => {
    it('should throw when not initialized', async () => {
      const storage = createMockStorage();
      const manager = new SnapshotManager(storage as any, 'test-table', {});

      // Don't call initialize()
      await expect(
        manager.createSnapshot({
          operation: 'append',
          manifestListPath: 's3://bucket/snap.avro',
        })
      ).rejects.toThrow(/not initialized/i);
    });

    it('should throw for invalid operation type', async () => {
      const storage = createMockStorage();
      const manager = new SnapshotManager(storage as any, 'test-table', {});
      await manager.initialize();

      await expect(
        manager.createSnapshot({
          operation: 'invalid' as OperationType,
          manifestListPath: 's3://bucket/snap.avro',
        })
      ).rejects.toThrow(/invalid operation/i);
    });

    it('should throw for missing manifest list path', async () => {
      const storage = createMockStorage();
      const manager = new SnapshotManager(storage as any, 'test-table', {});
      await manager.initialize();

      await expect(
        manager.createSnapshot({
          operation: 'append',
          manifestListPath: '',
        })
      ).rejects.toThrow(/manifest.*required/i);
    });

    it('should throw for non-existent parent snapshot', async () => {
      const storage = createMockStorage();
      const manager = new SnapshotManager(storage as any, 'test-table', {});
      await manager.initialize();

      await expect(
        manager.createSnapshot({
          operation: 'append',
          manifestListPath: 's3://bucket/snap.avro',
          parentSnapshotId: 999n,
        })
      ).rejects.toThrow(/parent.*not found/i);
    });

    it('should throw when rolling back to non-existent snapshot', async () => {
      const storage = createMockStorage();
      const manager = new SnapshotManager(storage as any, 'test-table', {});
      await manager.initialize();

      await expect(manager.rollbackToSnapshot(999n)).rejects.toThrow(/not found/i);
    });
  });

  describe('schema tracker errors', () => {
    it('should throw for field not found', () => {
      const tracker = new SchemaTracker(createTestSchema());

      expect(() => tracker.removeField(999)).toThrow(/not.*exist/i);
    });

    it('should throw for duplicate field name', () => {
      const tracker = new SchemaTracker(createTestSchema());

      expect(() =>
        tracker.addField({ name: '_id', type: 'string' }) // '_id' already exists
      ).toThrow(/already exists/i);
    });

    it('should throw when no current schema exists', () => {
      const tracker = new SchemaTracker();

      expect(() =>
        tracker.addField({ name: 'field', type: 'string' })
      ).toThrow(/no current schema/i);
    });
  });
});

// ============================================================================
// 10. R2 Storage Integration Tests
// ============================================================================

describe('Catalog Integration - R2 Storage Integration', () => {
  let client: R2DataCatalogClient;
  let mockFetch: ReturnType<typeof createMockFetch>;

  beforeEach(() => {
    mockFetch = createMockFetch(new Map());
    vi.stubGlobal('fetch', mockFetch);
    client = new R2DataCatalogClient(TEST_R2_CONFIG);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  describe('R2 Data Catalog API integration', () => {
    it('should register MongoLake collection in R2 Data Catalog', async () => {
      mockFetch = createMockFetch(new Map([
        ['namespaces', {
          status: 200,
          body: { success: true, result: { namespace: ['mongolake', 'mydb'], properties: {} } },
        }],
        ['tables', {
          status: 200,
          body: {
            success: true,
            result: {
              identifier: { namespace: ['mongolake', 'mydb'], name: 'users' },
              location: 's3://bucket/mongolake/mydb/users',
              metadataLocation: 's3://bucket/mongolake/mydb/users/metadata/v1.metadata.json',
              properties: { 'mongolake.database': 'mydb', 'mongolake.collection': 'users' },
            },
          },
        }],
      ]));
      vi.stubGlobal('fetch', mockFetch);

      const table = await client.registerCollection(
        'mydb',
        'users',
        's3://bucket/mongolake/mydb/users'
      );

      expect(table.properties['mongolake.database']).toBe('mydb');
      expect(table.properties['mongolake.collection']).toBe('users');
    });

    it('should create namespace if it does not exist', async () => {
      let namespaceCheckCount = 0;
      const customFetch = vi.fn(async (url: string, init?: RequestInit) => {
        const method = init?.method ?? 'GET';

        if (url.includes('namespaces') && method === 'GET') {
          namespaceCheckCount++;
          if (namespaceCheckCount === 1) {
            return new Response(
              JSON.stringify({ success: false, error: { code: 'NOT_FOUND', message: 'Not found' } }),
              { status: 404, headers: { 'Content-Type': 'application/json' } }
            );
          }
        }

        if (url.includes('namespaces') && method === 'POST') {
          return new Response(
            JSON.stringify({ success: true, result: { namespace: ['mongolake', 'newdb'], properties: {} } }),
            { status: 200, headers: { 'Content-Type': 'application/json' } }
          );
        }

        if (url.includes('tables')) {
          return new Response(
            JSON.stringify({
              success: true,
              result: {
                identifier: { namespace: ['mongolake', 'newdb'], name: 'users' },
                location: 's3://bucket/mongolake/newdb/users',
                metadataLocation: 's3://bucket/mongolake/newdb/users/metadata/v1.metadata.json',
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
        's3://bucket/mongolake/newdb/users'
      );

      expect(table.identifier.name).toBe('users');
    });

    it('should refresh table metadata location', async () => {
      mockFetch = createMockFetch(new Map([
        ['tables', {
          status: 200,
          body: {
            success: true,
            result: {
              identifier: { namespace: ['mongolake', 'db'], name: 'users' },
              location: 's3://bucket/mongolake/db/users',
              metadataLocation: 's3://bucket/mongolake/db/users/metadata/v5.metadata.json',
              properties: {},
            },
          },
        }],
      ]));
      vi.stubGlobal('fetch', mockFetch);

      const table = await client.refreshTable(
        'db',
        'users',
        's3://bucket/mongolake/db/users/metadata/v5.metadata.json'
      );

      expect(table.metadataLocation).toContain('v5.metadata.json');
    });

    it('should list all databases from R2 Data Catalog', async () => {
      mockFetch = createMockFetch(new Map([
        ['namespaces', {
          status: 200,
          body: {
            success: true,
            result: {
              namespaces: [
                ['mongolake', 'app1'],
                ['mongolake', 'app2'],
                ['mongolake', 'analytics'],
              ],
            },
          },
        }],
      ]));
      vi.stubGlobal('fetch', mockFetch);

      const databases = await client.listDatabases();

      expect(databases).toContain('app1');
      expect(databases).toContain('app2');
      expect(databases).toContain('analytics');
    });

    it('should list collections in database', async () => {
      mockFetch = createMockFetch(new Map([
        ['tables', {
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
        }],
      ]));
      vi.stubGlobal('fetch', mockFetch);

      const collections = await client.listCollections('mydb');

      expect(collections).toContain('users');
      expect(collections).toContain('orders');
    });

    it('should unregister collection', async () => {
      mockFetch = createMockFetch(new Map([
        ['tables', {
          status: 200,
          body: { success: true, result: null },
        }],
      ]));
      vi.stubGlobal('fetch', mockFetch);

      const dropped = await client.unregisterCollection('mydb', 'users');

      expect(dropped).toBe(true);
    });
  });

  describe('R2 storage path conventions', () => {
    it.todo('should use correct metadata path format: {location}/metadata/v{version}.metadata.json');
    it.todo('should use correct manifest list path format: {location}/metadata/snap-{snapshotId}.avro');
    it.todo('should use correct manifest path format: {location}/metadata/{manifestId}.avro');
    it.todo('should use correct data file path format: {location}/data/{partition}/{fileId}.parquet');
  });

  describe('R2-specific error handling', () => {
    it('should handle R2 rate limiting', async () => {
      mockFetch = createMockFetch(new Map([
        ['namespaces', {
          status: 429,
          body: { success: false, error: { code: 'RATE_LIMITED', message: 'Too many requests' } },
        }],
      ]));
      vi.stubGlobal('fetch', mockFetch);

      await expect(client.listNamespaces()).rejects.toThrow(R2DataCatalogError);
    });

    it('should handle R2 service unavailable', async () => {
      mockFetch = createMockFetch(new Map([
        ['namespaces', {
          status: 503,
          body: { success: false, error: { code: 'SERVICE_UNAVAILABLE', message: 'Service unavailable' } },
        }],
      ]));
      vi.stubGlobal('fetch', mockFetch);

      await expect(client.listNamespaces()).rejects.toThrow(R2DataCatalogError);
    });
  });
});

// ============================================================================
// End-to-End Integration Scenarios
// ============================================================================

describe('Catalog Integration - End-to-End Scenarios', () => {
  describe('complete table lifecycle', () => {
    it.todo('should create, populate, evolve schema, and drop table');
    it.todo('should support time-travel queries across schema versions');
    it.todo('should handle partition evolution with data migration');
  });

  describe('MongoLake integration', () => {
    it.todo('should sync MongoDB collection to Iceberg table');
    it.todo('should handle MongoDB schema changes via schema evolution');
    it.todo('should support BSON variant encoding in Iceberg');
    it.todo('should maintain _id as required identifier field');
  });

  describe('query engine compatibility', () => {
    it.todo('should generate metadata compatible with DuckDB');
    it.todo('should generate metadata compatible with Spark');
    it.todo('should generate metadata compatible with Trino');
    it.todo('should support predicate pushdown via partition specs');
  });
});
