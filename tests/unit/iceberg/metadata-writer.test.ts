/**
 * Iceberg Metadata Writer Tests
 *
 * Comprehensive tests for generating Iceberg v2 metadata.json files.
 * The metadata.json file is the heart of an Iceberg table, containing:
 * - Table identification and versioning
 * - Schema definitions with field IDs
 * - Partition specifications
 * - Sort orders
 * - Snapshot history
 * - Table properties
 *
 * Reference: https://iceberg.apache.org/spec/
 *
 * These are RED tests - they should fail until the implementation is complete.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  MetadataWriter,
  type TableMetadata,
  type IcebergSchema,
  type IcebergSchemaField,
  type PartitionSpec,
  type PartitionField,
  type SortOrder,
  type SortField,
  type Snapshot,
  type SnapshotRef,
  type SnapshotSummary,
  type ManifestListLocation,
  FormatVersionError,
  InvalidSchemaError,
  InvalidPartitionSpecError,
  InvalidSortOrderError,
  InvalidSnapshotError,
  MetadataSerializationError,
} from '../../../src/iceberg/metadata-writer.js';

// ============================================================================
// Constants
// ============================================================================

const ICEBERG_FORMAT_VERSION_V1 = 1;
const ICEBERG_FORMAT_VERSION_V2 = 2;

// ============================================================================
// Test Fixtures
// ============================================================================

/**
 * Creates a minimal valid Iceberg schema for testing.
 */
function createMinimalSchema(): IcebergSchema {
  return {
    type: 'struct',
    'schema-id': 0,
    fields: [
      {
        id: 1,
        name: '_id',
        required: true,
        type: 'string',
      },
    ],
  };
}

/**
 * Creates a comprehensive schema with multiple field types.
 */
function createFullSchema(): IcebergSchema {
  return {
    type: 'struct',
    'schema-id': 0,
    fields: [
      { id: 1, name: '_id', required: true, type: 'string' },
      { id: 2, name: '_seq', required: true, type: 'long' },
      { id: 3, name: '_op', required: true, type: 'string' },
      { id: 4, name: 'name', required: false, type: 'string' },
      { id: 5, name: 'age', required: false, type: 'int' },
      { id: 6, name: 'score', required: false, type: 'double' },
      { id: 7, name: 'active', required: false, type: 'boolean' },
      { id: 8, name: 'created_at', required: false, type: 'timestamptz' },
      { id: 9, name: 'metadata', required: false, type: 'binary' },
    ],
  };
}

/**
 * Creates a schema with nested struct type.
 */
function createNestedSchema(): IcebergSchema {
  return {
    type: 'struct',
    'schema-id': 0,
    fields: [
      { id: 1, name: '_id', required: true, type: 'string' },
      {
        id: 2,
        name: 'address',
        required: false,
        type: {
          type: 'struct',
          fields: [
            { id: 3, name: 'street', required: true, type: 'string' },
            { id: 4, name: 'city', required: true, type: 'string' },
            { id: 5, name: 'zip', required: false, type: 'string' },
          ],
        },
      },
    ],
  };
}

/**
 * Creates a schema with list type.
 */
function createListSchema(): IcebergSchema {
  return {
    type: 'struct',
    'schema-id': 0,
    fields: [
      { id: 1, name: '_id', required: true, type: 'string' },
      {
        id: 2,
        name: 'tags',
        required: false,
        type: {
          type: 'list',
          'element-id': 3,
          element: 'string',
          'element-required': false,
        },
      },
    ],
  };
}

/**
 * Creates a schema with map type.
 */
function createMapSchema(): IcebergSchema {
  return {
    type: 'struct',
    'schema-id': 0,
    fields: [
      { id: 1, name: '_id', required: true, type: 'string' },
      {
        id: 2,
        name: 'properties',
        required: false,
        type: {
          type: 'map',
          'key-id': 3,
          key: 'string',
          'value-id': 4,
          value: 'string',
          'value-required': false,
        },
      },
    ],
  };
}

/**
 * Creates a minimal partition spec (unpartitioned).
 */
function createUnpartitionedSpec(): PartitionSpec {
  return {
    'spec-id': 0,
    fields: [],
  };
}

/**
 * Creates a partition spec with identity transform.
 */
function createIdentityPartitionSpec(): PartitionSpec {
  return {
    'spec-id': 0,
    fields: [
      {
        'source-id': 1,
        'field-id': 1000,
        name: '_id',
        transform: 'identity',
      },
    ],
  };
}

/**
 * Creates a partition spec with bucket transform.
 */
function createBucketPartitionSpec(): PartitionSpec {
  return {
    'spec-id': 0,
    fields: [
      {
        'source-id': 1,
        'field-id': 1000,
        name: '_id_bucket',
        transform: 'bucket[16]',
      },
    ],
  };
}

/**
 * Creates an unsorted sort order.
 */
function createUnsortedOrder(): SortOrder {
  return {
    'order-id': 0,
    fields: [],
  };
}

/**
 * Creates a sort order with single field.
 */
function createSingleFieldSortOrder(): SortOrder {
  return {
    'order-id': 1,
    fields: [
      {
        transform: 'identity',
        'source-id': 2,
        direction: 'asc',
        'null-order': 'nulls-first',
      },
    ],
  };
}

/**
 * Creates a minimal snapshot.
 */
function createMinimalSnapshot(snapshotId: number, timestamp: number): Snapshot {
  return {
    'snapshot-id': snapshotId,
    'timestamp-ms': timestamp,
    summary: {
      operation: 'append',
    },
    'manifest-list': `s3://bucket/db/table/metadata/snap-${snapshotId}-uuid.avro`,
  };
}

// ============================================================================
// Table Metadata Structure Tests
// ============================================================================

describe('MetadataWriter - Table metadata structure', () => {
  let writer: MetadataWriter;

  beforeEach(() => {
    writer = new MetadataWriter();
  });

  it('should generate valid JSON output', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    expect(metadata).toBeDefined();
    expect(typeof metadata).toBe('string');

    // Should be valid JSON
    const parsed = JSON.parse(metadata);
    expect(parsed).toBeDefined();
    expect(typeof parsed).toBe('object');
  });

  it('should include format-version field', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['format-version']).toBeDefined();
    expect(typeof parsed['format-version']).toBe('number');
  });

  it('should default to format-version 2', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['format-version']).toBe(ICEBERG_FORMAT_VERSION_V2);
  });

  it('should support format-version 1 when specified', () => {
    const writer = new MetadataWriter({ formatVersion: 1 });
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['format-version']).toBe(ICEBERG_FORMAT_VERSION_V1);
  });

  it('should throw on unsupported format-version', () => {
    expect(() => new MetadataWriter({ formatVersion: 3 })).toThrow(FormatVersionError);
  });

  it('should include table-uuid field', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['table-uuid']).toBeDefined();
    expect(typeof parsed['table-uuid']).toBe('string');
    // UUID format validation (8-4-4-4-12)
    expect(parsed['table-uuid']).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
    );
  });

  it('should use provided table-uuid', () => {
    const customUuid = '550e8400-e29b-41d4-a716-446655440000';
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      tableUuid: customUuid,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['table-uuid']).toBe(customUuid);
  });

  it('should include location field', () => {
    const location = 's3://my-bucket/warehouse/db/users';
    const metadata = writer.generate({
      location,
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['location']).toBe(location);
  });

  it('should include last-updated-ms field', () => {
    const beforeTime = Date.now();
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });
    const afterTime = Date.now();

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['last-updated-ms']).toBeDefined();
    expect(typeof parsed['last-updated-ms']).toBe('number');
    expect(parsed['last-updated-ms']).toBeGreaterThanOrEqual(beforeTime);
    expect(parsed['last-updated-ms']).toBeLessThanOrEqual(afterTime);
  });

  it('should use provided last-updated-ms', () => {
    const timestamp = 1706745600000; // Fixed timestamp
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      lastUpdatedMs: timestamp,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['last-updated-ms']).toBe(timestamp);
  });

  it('should include last-column-id field', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['last-column-id']).toBeDefined();
    expect(typeof parsed['last-column-id']).toBe('number');
  });

  it('should calculate last-column-id from schema', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createFullSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    // Full schema has fields with IDs 1-9
    expect(parsed['last-column-id']).toBe(9);
  });

  it('should calculate last-column-id from nested schema', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createNestedSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    // Nested schema has fields with IDs 1-5
    expect(parsed['last-column-id']).toBe(5);
  });

  it('should include last-sequence-number in v2', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['last-sequence-number']).toBeDefined();
    expect(typeof parsed['last-sequence-number']).toBe('number');
  });

  it('should default last-sequence-number to 0 for new table', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['last-sequence-number']).toBe(0);
  });

  it('should use provided last-sequence-number', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      lastSequenceNumber: 42,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['last-sequence-number']).toBe(42);
  });

  it('should produce deterministic output for same input', () => {
    const input = {
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      tableUuid: '550e8400-e29b-41d4-a716-446655440000',
      lastUpdatedMs: 1706745600000,
    };

    const metadata1 = writer.generate(input);
    const metadata2 = writer.generate(input);

    expect(metadata1).toBe(metadata2);
  });

  it('should produce pretty-printed JSON by default', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    // Pretty-printed JSON has newlines
    expect(metadata).toContain('\n');
    expect(metadata).toContain('  '); // Indentation
  });

  it('should support compact JSON output', () => {
    const writer = new MetadataWriter({ prettyPrint: false });
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    // Compact JSON has no newlines within structure
    expect(metadata).not.toMatch(/\n\s+/);
  });
});

// ============================================================================
// Schema Serialization Tests
// ============================================================================

describe('MetadataWriter - Schema serialization', () => {
  let writer: MetadataWriter;

  beforeEach(() => {
    writer = new MetadataWriter();
  });

  it('should include schemas array', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['schemas']).toBeDefined();
    expect(Array.isArray(parsed['schemas'])).toBe(true);
  });

  it('should include at least one schema', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['schemas'].length).toBeGreaterThanOrEqual(1);
  });

  it('should include current-schema-id', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['current-schema-id']).toBeDefined();
    expect(typeof parsed['current-schema-id']).toBe('number');
  });

  it('should set current-schema-id to the schema id', () => {
    const schema = createMinimalSchema();
    schema['schema-id'] = 5;

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['current-schema-id']).toBe(5);
  });

  it('should serialize schema type as struct', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['schemas'][0].type).toBe('struct');
  });

  it('should serialize schema-id', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['schemas'][0]['schema-id']).toBeDefined();
  });

  it('should serialize schema fields array', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createFullSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const fields = parsed['schemas'][0].fields;
    expect(Array.isArray(fields)).toBe(true);
    expect(fields.length).toBe(9);
  });

  it('should serialize field id', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const field = parsed['schemas'][0].fields[0];
    expect(field.id).toBe(1);
  });

  it('should serialize field name', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const field = parsed['schemas'][0].fields[0];
    expect(field.name).toBe('_id');
  });

  it('should serialize field required status', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createFullSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const fields = parsed['schemas'][0].fields;

    // _id is required
    const idField = fields.find((f: IcebergSchemaField) => f.name === '_id');
    expect(idField?.required).toBe(true);

    // name is optional
    const nameField = fields.find((f: IcebergSchemaField) => f.name === 'name');
    expect(nameField?.required).toBe(false);
  });

  it('should serialize primitive types correctly', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createFullSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const fields = parsed['schemas'][0].fields;

    const getType = (name: string) =>
      fields.find((f: IcebergSchemaField) => f.name === name)?.type;

    expect(getType('_id')).toBe('string');
    expect(getType('_seq')).toBe('long');
    expect(getType('age')).toBe('int');
    expect(getType('score')).toBe('double');
    expect(getType('active')).toBe('boolean');
    expect(getType('created_at')).toBe('timestamptz');
    expect(getType('metadata')).toBe('binary');
  });

  it('should serialize nested struct type', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createNestedSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const addressField = parsed['schemas'][0].fields.find(
      (f: IcebergSchemaField) => f.name === 'address'
    );

    expect(addressField).toBeDefined();
    expect(typeof addressField?.type).toBe('object');
    expect((addressField?.type as { type: string }).type).toBe('struct');
    expect((addressField?.type as { fields: unknown[] }).fields).toHaveLength(3);
  });

  it('should serialize list type', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createListSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const tagsField = parsed['schemas'][0].fields.find(
      (f: IcebergSchemaField) => f.name === 'tags'
    );

    expect(tagsField).toBeDefined();
    expect(typeof tagsField?.type).toBe('object');
    const listType = tagsField?.type as {
      type: string;
      'element-id': number;
      element: string;
      'element-required': boolean;
    };
    expect(listType.type).toBe('list');
    expect(listType['element-id']).toBe(3);
    expect(listType.element).toBe('string');
    expect(listType['element-required']).toBe(false);
  });

  it('should serialize map type', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMapSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const propsField = parsed['schemas'][0].fields.find(
      (f: IcebergSchemaField) => f.name === 'properties'
    );

    expect(propsField).toBeDefined();
    expect(typeof propsField?.type).toBe('object');
    const mapType = propsField?.type as {
      type: string;
      'key-id': number;
      key: string;
      'value-id': number;
      value: string;
      'value-required': boolean;
    };
    expect(mapType.type).toBe('map');
    expect(mapType['key-id']).toBe(3);
    expect(mapType.key).toBe('string');
    expect(mapType['value-id']).toBe(4);
    expect(mapType.value).toBe('string');
    expect(mapType['value-required']).toBe(false);
  });

  it('should include field doc when provided', () => {
    const schema: IcebergSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        {
          id: 1,
          name: '_id',
          required: true,
          type: 'string',
          doc: 'Unique document identifier',
        },
      ],
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const field = parsed['schemas'][0].fields[0];
    expect(field.doc).toBe('Unique document identifier');
  });

  it('should throw on invalid schema (no fields)', () => {
    const invalidSchema: IcebergSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [],
    };

    expect(() =>
      writer.generate({
        location: 's3://bucket/db/table',
        schema: invalidSchema,
      })
    ).toThrow(InvalidSchemaError);
  });

  it('should throw on duplicate field IDs', () => {
    const invalidSchema: IcebergSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'field1', required: true, type: 'string' },
        { id: 1, name: 'field2', required: true, type: 'string' },
      ],
    };

    expect(() =>
      writer.generate({
        location: 's3://bucket/db/table',
        schema: invalidSchema,
      })
    ).toThrow(InvalidSchemaError);
  });

  it('should throw on duplicate field names', () => {
    const invalidSchema: IcebergSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'duplicate', required: true, type: 'string' },
        { id: 2, name: 'duplicate', required: true, type: 'string' },
      ],
    };

    expect(() =>
      writer.generate({
        location: 's3://bucket/db/table',
        schema: invalidSchema,
      })
    ).toThrow(InvalidSchemaError);
  });

  it('should support multiple schema versions', () => {
    const schemas: IcebergSchema[] = [
      {
        type: 'struct',
        'schema-id': 0,
        fields: [{ id: 1, name: '_id', required: true, type: 'string' }],
      },
      {
        type: 'struct',
        'schema-id': 1,
        fields: [
          { id: 1, name: '_id', required: true, type: 'string' },
          { id: 2, name: 'name', required: false, type: 'string' },
        ],
      },
    ];

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: schemas[1],
      schemas,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['schemas']).toHaveLength(2);
    expect(parsed['current-schema-id']).toBe(1);
  });
});

// ============================================================================
// Partition Spec Serialization Tests
// ============================================================================

describe('MetadataWriter - Partition spec serialization', () => {
  let writer: MetadataWriter;

  beforeEach(() => {
    writer = new MetadataWriter();
  });

  it('should include partition-specs array', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['partition-specs']).toBeDefined();
    expect(Array.isArray(parsed['partition-specs'])).toBe(true);
  });

  it('should include default-spec-id', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['default-spec-id']).toBeDefined();
    expect(typeof parsed['default-spec-id']).toBe('number');
  });

  it('should include last-partition-id in v2', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['last-partition-id']).toBeDefined();
    expect(typeof parsed['last-partition-id']).toBe('number');
  });

  it('should default to unpartitioned spec', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['partition-specs']).toHaveLength(1);
    expect(parsed['partition-specs'][0]['spec-id']).toBe(0);
    expect(parsed['partition-specs'][0].fields).toEqual([]);
  });

  it('should serialize identity partition transform', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      partitionSpec: createIdentityPartitionSpec(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const field = parsed['partition-specs'][0].fields[0];
    expect(field.transform).toBe('identity');
    expect(field['source-id']).toBe(1);
    expect(field['field-id']).toBe(1000);
    expect(field.name).toBe('_id');
  });

  it('should serialize bucket partition transform', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      partitionSpec: createBucketPartitionSpec(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const field = parsed['partition-specs'][0].fields[0];
    expect(field.transform).toBe('bucket[16]');
  });

  it('should serialize truncate partition transform', () => {
    const partitionSpec: PartitionSpec = {
      'spec-id': 0,
      fields: [
        {
          'source-id': 1,
          'field-id': 1000,
          name: '_id_trunc',
          transform: 'truncate[10]',
        },
      ],
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      partitionSpec,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const field = parsed['partition-specs'][0].fields[0];
    expect(field.transform).toBe('truncate[10]');
  });

  it('should serialize year partition transform', () => {
    const schema: IcebergSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'created_at', required: true, type: 'timestamptz' },
      ],
    };

    const partitionSpec: PartitionSpec = {
      'spec-id': 0,
      fields: [
        {
          'source-id': 1,
          'field-id': 1000,
          name: 'created_at_year',
          transform: 'year',
        },
      ],
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema,
      partitionSpec,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const field = parsed['partition-specs'][0].fields[0];
    expect(field.transform).toBe('year');
  });

  it('should serialize month partition transform', () => {
    const schema: IcebergSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'created_at', required: true, type: 'timestamptz' },
      ],
    };

    const partitionSpec: PartitionSpec = {
      'spec-id': 0,
      fields: [
        {
          'source-id': 1,
          'field-id': 1000,
          name: 'created_at_month',
          transform: 'month',
        },
      ],
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema,
      partitionSpec,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const field = parsed['partition-specs'][0].fields[0];
    expect(field.transform).toBe('month');
  });

  it('should serialize day partition transform', () => {
    const schema: IcebergSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [{ id: 1, name: 'created_at', required: true, type: 'date' }],
    };

    const partitionSpec: PartitionSpec = {
      'spec-id': 0,
      fields: [
        {
          'source-id': 1,
          'field-id': 1000,
          name: 'created_at_day',
          transform: 'day',
        },
      ],
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema,
      partitionSpec,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const field = parsed['partition-specs'][0].fields[0];
    expect(field.transform).toBe('day');
  });

  it('should serialize hour partition transform', () => {
    const schema: IcebergSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: 'created_at', required: true, type: 'timestamptz' },
      ],
    };

    const partitionSpec: PartitionSpec = {
      'spec-id': 0,
      fields: [
        {
          'source-id': 1,
          'field-id': 1000,
          name: 'created_at_hour',
          transform: 'hour',
        },
      ],
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema,
      partitionSpec,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const field = parsed['partition-specs'][0].fields[0];
    expect(field.transform).toBe('hour');
  });

  it('should serialize void partition transform', () => {
    const partitionSpec: PartitionSpec = {
      'spec-id': 0,
      fields: [
        {
          'source-id': 1,
          'field-id': 1000,
          name: '_id_void',
          transform: 'void',
        },
      ],
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      partitionSpec,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const field = parsed['partition-specs'][0].fields[0];
    expect(field.transform).toBe('void');
  });

  it('should support multiple partition fields', () => {
    const schema: IcebergSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: '_id', required: true, type: 'string' },
        { id: 2, name: 'created_at', required: true, type: 'timestamptz' },
      ],
    };

    const partitionSpec: PartitionSpec = {
      'spec-id': 0,
      fields: [
        {
          'source-id': 2,
          'field-id': 1000,
          name: 'created_at_year',
          transform: 'year',
        },
        {
          'source-id': 1,
          'field-id': 1001,
          name: '_id_bucket',
          transform: 'bucket[16]',
        },
      ],
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema,
      partitionSpec,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['partition-specs'][0].fields).toHaveLength(2);
  });

  it('should throw on invalid source-id', () => {
    const partitionSpec: PartitionSpec = {
      'spec-id': 0,
      fields: [
        {
          'source-id': 999, // Non-existent field
          'field-id': 1000,
          name: 'invalid',
          transform: 'identity',
        },
      ],
    };

    expect(() =>
      writer.generate({
        location: 's3://bucket/db/table',
        schema: createMinimalSchema(),
        partitionSpec,
      })
    ).toThrow(InvalidPartitionSpecError);
  });

  it('should update last-partition-id based on partition fields', () => {
    const partitionSpec: PartitionSpec = {
      'spec-id': 0,
      fields: [
        {
          'source-id': 1,
          'field-id': 1000,
          name: '_id',
          transform: 'identity',
        },
      ],
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      partitionSpec,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['last-partition-id']).toBe(1000);
  });

  it('should support multiple partition specs (evolution)', () => {
    const partitionSpecs: PartitionSpec[] = [
      { 'spec-id': 0, fields: [] },
      {
        'spec-id': 1,
        fields: [
          { 'source-id': 1, 'field-id': 1000, name: '_id', transform: 'identity' },
        ],
      },
    ];

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      partitionSpec: partitionSpecs[1],
      partitionSpecs,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['partition-specs']).toHaveLength(2);
    expect(parsed['default-spec-id']).toBe(1);
  });
});

// ============================================================================
// Sort Order Serialization Tests
// ============================================================================

describe('MetadataWriter - Sort order serialization', () => {
  let writer: MetadataWriter;

  beforeEach(() => {
    writer = new MetadataWriter();
  });

  it('should include sort-orders array', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['sort-orders']).toBeDefined();
    expect(Array.isArray(parsed['sort-orders'])).toBe(true);
  });

  it('should include default-sort-order-id', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['default-sort-order-id']).toBeDefined();
    expect(typeof parsed['default-sort-order-id']).toBe('number');
  });

  it('should default to unsorted order', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['sort-orders']).toHaveLength(1);
    expect(parsed['sort-orders'][0]['order-id']).toBe(0);
    expect(parsed['sort-orders'][0].fields).toEqual([]);
    expect(parsed['default-sort-order-id']).toBe(0);
  });

  it('should serialize sort order with ascending direction', () => {
    const schema: IcebergSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: '_id', required: true, type: 'string' },
        { id: 2, name: '_seq', required: true, type: 'long' },
      ],
    };

    const sortOrder: SortOrder = {
      'order-id': 1,
      fields: [
        {
          transform: 'identity',
          'source-id': 2,
          direction: 'asc',
          'null-order': 'nulls-first',
        },
      ],
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema,
      sortOrder,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const field = parsed['sort-orders'].find(
      (o: SortOrder) => o['order-id'] === 1
    )?.fields[0];
    expect(field?.direction).toBe('asc');
  });

  it('should serialize sort order with descending direction', () => {
    const schema: IcebergSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: '_id', required: true, type: 'string' },
        { id: 2, name: '_seq', required: true, type: 'long' },
      ],
    };

    const sortOrder: SortOrder = {
      'order-id': 1,
      fields: [
        {
          transform: 'identity',
          'source-id': 2,
          direction: 'desc',
          'null-order': 'nulls-last',
        },
      ],
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema,
      sortOrder,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const field = parsed['sort-orders'].find(
      (o: SortOrder) => o['order-id'] === 1
    )?.fields[0];
    expect(field?.direction).toBe('desc');
  });

  it('should serialize sort order with nulls-first', () => {
    const sortOrder = createSingleFieldSortOrder();

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createFullSchema(),
      sortOrder,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const field = parsed['sort-orders'].find(
      (o: SortOrder) => o['order-id'] === 1
    )?.fields[0];
    expect(field?.['null-order']).toBe('nulls-first');
  });

  it('should serialize sort order with nulls-last', () => {
    const sortOrder: SortOrder = {
      'order-id': 1,
      fields: [
        {
          transform: 'identity',
          'source-id': 2,
          direction: 'asc',
          'null-order': 'nulls-last',
        },
      ],
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createFullSchema(),
      sortOrder,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const field = parsed['sort-orders'].find(
      (o: SortOrder) => o['order-id'] === 1
    )?.fields[0];
    expect(field?.['null-order']).toBe('nulls-last');
  });

  it('should serialize sort order transform', () => {
    const sortOrder: SortOrder = {
      'order-id': 1,
      fields: [
        {
          transform: 'bucket[16]',
          'source-id': 1,
          direction: 'asc',
          'null-order': 'nulls-first',
        },
      ],
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      sortOrder,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const field = parsed['sort-orders'].find(
      (o: SortOrder) => o['order-id'] === 1
    )?.fields[0];
    expect(field?.transform).toBe('bucket[16]');
  });

  it('should support multiple sort fields', () => {
    const schema: IcebergSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: '_id', required: true, type: 'string' },
        { id: 2, name: '_seq', required: true, type: 'long' },
        { id: 3, name: 'created_at', required: false, type: 'timestamptz' },
      ],
    };

    const sortOrder: SortOrder = {
      'order-id': 1,
      fields: [
        {
          transform: 'identity',
          'source-id': 2,
          direction: 'asc',
          'null-order': 'nulls-first',
        },
        {
          transform: 'identity',
          'source-id': 3,
          direction: 'desc',
          'null-order': 'nulls-last',
        },
      ],
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema,
      sortOrder,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const sortFields = parsed['sort-orders'].find(
      (o: SortOrder) => o['order-id'] === 1
    )?.fields;
    expect(sortFields).toHaveLength(2);
  });

  it('should throw on invalid source-id in sort order', () => {
    const sortOrder: SortOrder = {
      'order-id': 1,
      fields: [
        {
          transform: 'identity',
          'source-id': 999, // Non-existent field
          direction: 'asc',
          'null-order': 'nulls-first',
        },
      ],
    };

    expect(() =>
      writer.generate({
        location: 's3://bucket/db/table',
        schema: createMinimalSchema(),
        sortOrder,
      })
    ).toThrow(InvalidSortOrderError);
  });

  it('should support multiple sort orders (evolution)', () => {
    const sortOrders: SortOrder[] = [
      { 'order-id': 0, fields: [] },
      {
        'order-id': 1,
        fields: [
          {
            transform: 'identity',
            'source-id': 2,
            direction: 'asc',
            'null-order': 'nulls-first',
          },
        ],
      },
    ];

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createFullSchema(),
      sortOrder: sortOrders[1],
      sortOrders,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['sort-orders']).toHaveLength(2);
    expect(parsed['default-sort-order-id']).toBe(1);
  });
});

// ============================================================================
// Properties Serialization Tests
// ============================================================================

describe('MetadataWriter - Properties serialization', () => {
  let writer: MetadataWriter;

  beforeEach(() => {
    writer = new MetadataWriter();
  });

  it('should include properties object', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['properties']).toBeDefined();
    expect(typeof parsed['properties']).toBe('object');
  });

  it('should default to empty properties', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(Object.keys(parsed['properties'])).toHaveLength(0);
  });

  it('should serialize custom properties', () => {
    const properties = {
      'write.format.default': 'parquet',
      'write.parquet.compression-codec': 'zstd',
      'commit.retry.num-retries': '4',
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      properties,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['properties']['write.format.default']).toBe('parquet');
    expect(parsed['properties']['write.parquet.compression-codec']).toBe('zstd');
    expect(parsed['properties']['commit.retry.num-retries']).toBe('4');
  });

  it('should serialize all property values as strings', () => {
    const properties = {
      'some.number': '42',
      'some.boolean': 'true',
      'some.string': 'value',
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      properties,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(typeof parsed['properties']['some.number']).toBe('string');
    expect(typeof parsed['properties']['some.boolean']).toBe('string');
    expect(typeof parsed['properties']['some.string']).toBe('string');
  });

  it('should preserve MongoLake-specific properties', () => {
    const properties = {
      'mongolake.database': 'testdb',
      'mongolake.collection': 'users',
      'mongolake.branch': 'main',
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      properties,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['properties']['mongolake.database']).toBe('testdb');
    expect(parsed['properties']['mongolake.collection']).toBe('users');
    expect(parsed['properties']['mongolake.branch']).toBe('main');
  });
});

// ============================================================================
// Current Snapshot ID Tests
// ============================================================================

describe('MetadataWriter - Current snapshot ID', () => {
  let writer: MetadataWriter;

  beforeEach(() => {
    writer = new MetadataWriter();
  });

  it('should include current-snapshot-id field', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect('current-snapshot-id' in parsed).toBe(true);
  });

  it('should default current-snapshot-id to -1 for new table', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['current-snapshot-id']).toBe(-1);
  });

  it('should use provided current-snapshot-id', () => {
    const snapshot = createMinimalSnapshot(1234567890, Date.now());

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      currentSnapshotId: 1234567890,
      snapshots: [snapshot],
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['current-snapshot-id']).toBe(1234567890);
  });

  it('should throw if current-snapshot-id references non-existent snapshot', () => {
    expect(() =>
      writer.generate({
        location: 's3://bucket/db/table',
        schema: createMinimalSchema(),
        currentSnapshotId: 1234567890,
        snapshots: [], // No snapshots
      })
    ).toThrow(InvalidSnapshotError);
  });
});

// ============================================================================
// Snapshots Serialization Tests
// ============================================================================

describe('MetadataWriter - Snapshots serialization', () => {
  let writer: MetadataWriter;

  beforeEach(() => {
    writer = new MetadataWriter();
  });

  it('should include snapshots array', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['snapshots']).toBeDefined();
    expect(Array.isArray(parsed['snapshots'])).toBe(true);
  });

  it('should default to empty snapshots for new table', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['snapshots']).toHaveLength(0);
  });

  it('should serialize snapshot-id', () => {
    const snapshot = createMinimalSnapshot(1234567890, Date.now());

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      currentSnapshotId: 1234567890,
      snapshots: [snapshot],
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['snapshots'][0]['snapshot-id']).toBe(1234567890);
  });

  it('should serialize timestamp-ms', () => {
    const timestamp = 1706745600000;
    const snapshot = createMinimalSnapshot(1, timestamp);

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      currentSnapshotId: 1,
      snapshots: [snapshot],
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['snapshots'][0]['timestamp-ms']).toBe(timestamp);
  });

  it('should serialize manifest-list', () => {
    const snapshot: Snapshot = {
      'snapshot-id': 1,
      'timestamp-ms': Date.now(),
      summary: { operation: 'append' },
      'manifest-list': 's3://bucket/db/table/metadata/snap-1-uuid.avro',
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      currentSnapshotId: 1,
      snapshots: [snapshot],
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['snapshots'][0]['manifest-list']).toBe(
      's3://bucket/db/table/metadata/snap-1-uuid.avro'
    );
  });

  it('should serialize snapshot summary', () => {
    const snapshot: Snapshot = {
      'snapshot-id': 1,
      'timestamp-ms': Date.now(),
      summary: {
        operation: 'append',
        'added-data-files': '5',
        'added-records': '1000',
        'total-data-files': '5',
        'total-records': '1000',
      },
      'manifest-list': 's3://bucket/db/table/metadata/snap-1.avro',
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      currentSnapshotId: 1,
      snapshots: [snapshot],
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const summary = parsed['snapshots'][0].summary;
    expect(summary.operation).toBe('append');
    expect(summary['added-data-files']).toBe('5');
    expect(summary['added-records']).toBe('1000');
  });

  it('should serialize parent-snapshot-id', () => {
    const snapshot1 = createMinimalSnapshot(1, Date.now() - 10000);
    const snapshot2: Snapshot = {
      'snapshot-id': 2,
      'parent-snapshot-id': 1,
      'timestamp-ms': Date.now(),
      summary: { operation: 'append' },
      'manifest-list': 's3://bucket/db/table/metadata/snap-2.avro',
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      currentSnapshotId: 2,
      snapshots: [snapshot1, snapshot2],
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    const snap2 = parsed['snapshots'].find(
      (s: Snapshot) => s['snapshot-id'] === 2
    );
    expect(snap2?.['parent-snapshot-id']).toBe(1);
  });

  it('should serialize sequence-number in v2', () => {
    const snapshot: Snapshot = {
      'snapshot-id': 1,
      'sequence-number': 42,
      'timestamp-ms': Date.now(),
      summary: { operation: 'append' },
      'manifest-list': 's3://bucket/db/table/metadata/snap-1.avro',
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      currentSnapshotId: 1,
      snapshots: [snapshot],
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['snapshots'][0]['sequence-number']).toBe(42);
  });

  it('should serialize schema-id in snapshot', () => {
    const snapshot: Snapshot = {
      'snapshot-id': 1,
      'timestamp-ms': Date.now(),
      'schema-id': 0,
      summary: { operation: 'append' },
      'manifest-list': 's3://bucket/db/table/metadata/snap-1.avro',
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      currentSnapshotId: 1,
      snapshots: [snapshot],
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['snapshots'][0]['schema-id']).toBe(0);
  });

  it('should support multiple snapshots', () => {
    const snapshots: Snapshot[] = [
      createMinimalSnapshot(1, Date.now() - 20000),
      createMinimalSnapshot(2, Date.now() - 10000),
      createMinimalSnapshot(3, Date.now()),
    ];
    snapshots[1]['parent-snapshot-id'] = 1;
    snapshots[2]['parent-snapshot-id'] = 2;

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      currentSnapshotId: 3,
      snapshots,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['snapshots']).toHaveLength(3);
  });

  it('should throw on duplicate snapshot IDs', () => {
    const snapshots: Snapshot[] = [
      createMinimalSnapshot(1, Date.now()),
      createMinimalSnapshot(1, Date.now() + 1000), // Duplicate ID
    ];

    expect(() =>
      writer.generate({
        location: 's3://bucket/db/table',
        schema: createMinimalSchema(),
        currentSnapshotId: 1,
        snapshots,
      })
    ).toThrow(InvalidSnapshotError);
  });
});

// ============================================================================
// Refs (Snapshot References) Tests
// ============================================================================

describe('MetadataWriter - Refs serialization', () => {
  let writer: MetadataWriter;

  beforeEach(() => {
    writer = new MetadataWriter();
  });

  it('should include refs object', () => {
    const snapshot = createMinimalSnapshot(1, Date.now());

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      currentSnapshotId: 1,
      snapshots: [snapshot],
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['refs']).toBeDefined();
    expect(typeof parsed['refs']).toBe('object');
  });

  it('should create main branch ref by default', () => {
    const snapshot = createMinimalSnapshot(1, Date.now());

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      currentSnapshotId: 1,
      snapshots: [snapshot],
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['refs']['main']).toBeDefined();
    expect(parsed['refs']['main']['snapshot-id']).toBe(1);
    expect(parsed['refs']['main']['type']).toBe('branch');
  });

  it('should serialize custom branch refs', () => {
    const snapshot = createMinimalSnapshot(1, Date.now());
    const refs: Record<string, SnapshotRef> = {
      main: { 'snapshot-id': 1, type: 'branch' },
      'feature-branch': { 'snapshot-id': 1, type: 'branch' },
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      currentSnapshotId: 1,
      snapshots: [snapshot],
      refs,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['refs']['feature-branch']).toBeDefined();
    expect(parsed['refs']['feature-branch']['type']).toBe('branch');
  });

  it('should serialize tag refs', () => {
    const snapshot = createMinimalSnapshot(1, Date.now());
    const refs: Record<string, SnapshotRef> = {
      main: { 'snapshot-id': 1, type: 'branch' },
      'v1.0.0': { 'snapshot-id': 1, type: 'tag' },
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      currentSnapshotId: 1,
      snapshots: [snapshot],
      refs,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['refs']['v1.0.0']).toBeDefined();
    expect(parsed['refs']['v1.0.0']['type']).toBe('tag');
  });

  it('should serialize ref with max-ref-age-ms', () => {
    const snapshot = createMinimalSnapshot(1, Date.now());
    const refs: Record<string, SnapshotRef> = {
      main: {
        'snapshot-id': 1,
        type: 'branch',
        'max-ref-age-ms': 86400000, // 1 day
      },
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      currentSnapshotId: 1,
      snapshots: [snapshot],
      refs,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['refs']['main']['max-ref-age-ms']).toBe(86400000);
  });

  it('should serialize ref with max-snapshot-age-ms', () => {
    const snapshot = createMinimalSnapshot(1, Date.now());
    const refs: Record<string, SnapshotRef> = {
      main: {
        'snapshot-id': 1,
        type: 'branch',
        'max-snapshot-age-ms': 604800000, // 7 days
      },
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      currentSnapshotId: 1,
      snapshots: [snapshot],
      refs,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['refs']['main']['max-snapshot-age-ms']).toBe(604800000);
  });

  it('should serialize ref with min-snapshots-to-keep', () => {
    const snapshot = createMinimalSnapshot(1, Date.now());
    const refs: Record<string, SnapshotRef> = {
      main: {
        'snapshot-id': 1,
        type: 'branch',
        'min-snapshots-to-keep': 5,
      },
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      currentSnapshotId: 1,
      snapshots: [snapshot],
      refs,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['refs']['main']['min-snapshots-to-keep']).toBe(5);
  });
});

// ============================================================================
// Snapshot Log and Metadata Log Tests
// ============================================================================

describe('MetadataWriter - History logs', () => {
  let writer: MetadataWriter;

  beforeEach(() => {
    writer = new MetadataWriter();
  });

  it('should include snapshot-log array when snapshots exist', () => {
    const snapshot = createMinimalSnapshot(1, Date.now());

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      currentSnapshotId: 1,
      snapshots: [snapshot],
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['snapshot-log']).toBeDefined();
    expect(Array.isArray(parsed['snapshot-log'])).toBe(true);
  });

  it('should record snapshot changes in snapshot-log', () => {
    const timestamp = Date.now();
    const snapshot = createMinimalSnapshot(1, timestamp);

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      currentSnapshotId: 1,
      snapshots: [snapshot],
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['snapshot-log']).toHaveLength(1);
    expect(parsed['snapshot-log'][0]['timestamp-ms']).toBe(timestamp);
    expect(parsed['snapshot-log'][0]['snapshot-id']).toBe(1);
  });

  it('should include metadata-log array', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['metadata-log']).toBeDefined();
    expect(Array.isArray(parsed['metadata-log'])).toBe(true);
  });

  it('should record previous metadata in metadata-log', () => {
    const previousMetadata = {
      'timestamp-ms': Date.now() - 10000,
      'metadata-file': 's3://bucket/db/table/metadata/v1.metadata.json',
    };

    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createMinimalSchema(),
      metadataLog: [previousMetadata],
    });

    const parsed = JSON.parse(metadata) as TableMetadata;
    expect(parsed['metadata-log']).toHaveLength(1);
    expect(parsed['metadata-log'][0]['metadata-file']).toBe(
      's3://bucket/db/table/metadata/v1.metadata.json'
    );
  });
});

// ============================================================================
// Error Handling Tests
// ============================================================================

describe('MetadataWriter - Error handling', () => {
  let writer: MetadataWriter;

  beforeEach(() => {
    writer = new MetadataWriter();
  });

  it('should throw on missing location', () => {
    expect(() =>
      writer.generate({
        location: '',
        schema: createMinimalSchema(),
      })
    ).toThrow(MetadataSerializationError);
  });

  it('should throw on null schema', () => {
    expect(() =>
      writer.generate({
        location: 's3://bucket/db/table',
        schema: null as unknown as IcebergSchema,
      })
    ).toThrow(InvalidSchemaError);
  });

  it('should throw on negative field ID', () => {
    const invalidSchema: IcebergSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [{ id: -1, name: 'invalid', required: true, type: 'string' }],
    };

    expect(() =>
      writer.generate({
        location: 's3://bucket/db/table',
        schema: invalidSchema,
      })
    ).toThrow(InvalidSchemaError);
  });

  it('should throw on invalid field type', () => {
    const invalidSchema: IcebergSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        {
          id: 1,
          name: 'invalid',
          required: true,
          type: 'invalid-type' as unknown as string,
        },
      ],
    };

    expect(() =>
      writer.generate({
        location: 's3://bucket/db/table',
        schema: invalidSchema,
      })
    ).toThrow(InvalidSchemaError);
  });

  it('should throw on invalid partition transform', () => {
    const partitionSpec: PartitionSpec = {
      'spec-id': 0,
      fields: [
        {
          'source-id': 1,
          'field-id': 1000,
          name: 'invalid',
          transform: 'invalid-transform',
        },
      ],
    };

    expect(() =>
      writer.generate({
        location: 's3://bucket/db/table',
        schema: createMinimalSchema(),
        partitionSpec,
      })
    ).toThrow(InvalidPartitionSpecError);
  });

  it('should throw on invalid sort direction', () => {
    const sortOrder: SortOrder = {
      'order-id': 1,
      fields: [
        {
          transform: 'identity',
          'source-id': 1,
          direction: 'invalid' as 'asc' | 'desc',
          'null-order': 'nulls-first',
        },
      ],
    };

    expect(() =>
      writer.generate({
        location: 's3://bucket/db/table',
        schema: createMinimalSchema(),
        sortOrder,
      })
    ).toThrow(InvalidSortOrderError);
  });

  it('should throw on negative snapshot ID', () => {
    const snapshot: Snapshot = {
      'snapshot-id': -1,
      'timestamp-ms': Date.now(),
      summary: { operation: 'append' },
      'manifest-list': 's3://bucket/db/table/metadata/snap.avro',
    };

    expect(() =>
      writer.generate({
        location: 's3://bucket/db/table',
        schema: createMinimalSchema(),
        currentSnapshotId: -1,
        snapshots: [snapshot],
      })
    ).toThrow(InvalidSnapshotError);
  });

  it('should throw on missing manifest-list in snapshot', () => {
    const snapshot = {
      'snapshot-id': 1,
      'timestamp-ms': Date.now(),
      summary: { operation: 'append' },
      // Missing manifest-list
    } as Snapshot;

    expect(() =>
      writer.generate({
        location: 's3://bucket/db/table',
        schema: createMinimalSchema(),
        currentSnapshotId: 1,
        snapshots: [snapshot],
      })
    ).toThrow(InvalidSnapshotError);
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('MetadataWriter - Integration', () => {
  let writer: MetadataWriter;

  beforeEach(() => {
    writer = new MetadataWriter();
  });

  it('should generate complete metadata for MongoLake table', () => {
    const schema: IcebergSchema = {
      type: 'struct',
      'schema-id': 0,
      fields: [
        { id: 1, name: '_id', required: true, type: 'string', doc: 'Document ID' },
        { id: 2, name: '_seq', required: true, type: 'long', doc: 'Sequence number' },
        { id: 3, name: '_op', required: true, type: 'string', doc: 'Operation type' },
        { id: 4, name: '_data', required: true, type: 'binary', doc: 'Variant-encoded document' },
        { id: 5, name: 'name', required: false, type: 'string' },
        { id: 6, name: 'email', required: false, type: 'string' },
      ],
    };

    const partitionSpec: PartitionSpec = {
      'spec-id': 0,
      fields: [
        {
          'source-id': 1,
          'field-id': 1000,
          name: '_id_bucket',
          transform: 'bucket[16]',
        },
      ],
    };

    const sortOrder: SortOrder = {
      'order-id': 1,
      fields: [
        {
          transform: 'identity',
          'source-id': 2,
          direction: 'asc',
          'null-order': 'nulls-first',
        },
      ],
    };

    const snapshot: Snapshot = {
      'snapshot-id': 1,
      'sequence-number': 1,
      'timestamp-ms': Date.now(),
      'schema-id': 0,
      summary: {
        operation: 'append',
        'added-data-files': '16',
        'added-records': '10000',
        'total-data-files': '16',
        'total-records': '10000',
      },
      'manifest-list': 's3://bucket/testdb/users/metadata/snap-1.avro',
    };

    const properties = {
      'mongolake.database': 'testdb',
      'mongolake.collection': 'users',
      'mongolake.branch': 'main',
      'write.format.default': 'parquet',
    };

    const metadata = writer.generate({
      location: 's3://bucket/testdb/users',
      schema,
      partitionSpec,
      sortOrder,
      currentSnapshotId: 1,
      snapshots: [snapshot],
      properties,
    });

    const parsed = JSON.parse(metadata) as TableMetadata;

    // Verify all major sections
    expect(parsed['format-version']).toBe(2);
    expect(parsed['table-uuid']).toBeDefined();
    expect(parsed['location']).toBe('s3://bucket/testdb/users');
    expect(parsed['schemas']).toHaveLength(1);
    expect(parsed['current-schema-id']).toBe(0);
    expect(parsed['partition-specs']).toHaveLength(1);
    expect(parsed['default-spec-id']).toBe(0);
    expect(parsed['sort-orders']).toHaveLength(2); // unsorted + custom
    expect(parsed['default-sort-order-id']).toBe(1);
    expect(parsed['current-snapshot-id']).toBe(1);
    expect(parsed['snapshots']).toHaveLength(1);
    expect(parsed['properties']['mongolake.database']).toBe('testdb');
  });

  it('should be parseable by standard JSON parsers', () => {
    const metadata = writer.generate({
      location: 's3://bucket/db/table',
      schema: createFullSchema(),
    });

    // Should not throw
    const parsed = JSON.parse(metadata);
    expect(parsed).toBeDefined();

    // Re-stringify and parse again should yield identical result
    const reserialized = JSON.parse(JSON.stringify(parsed));
    expect(reserialized).toEqual(parsed);
  });

  it('should generate valid metadata file name', () => {
    const fileName = MetadataWriter.generateMetadataFileName(1);
    expect(fileName).toMatch(/^v\d+\.metadata\.json$/);
    expect(fileName).toBe('v1.metadata.json');
  });

  it('should generate incrementing metadata file names', () => {
    expect(MetadataWriter.generateMetadataFileName(1)).toBe('v1.metadata.json');
    expect(MetadataWriter.generateMetadataFileName(42)).toBe('v42.metadata.json');
    expect(MetadataWriter.generateMetadataFileName(100)).toBe('v100.metadata.json');
  });
});
