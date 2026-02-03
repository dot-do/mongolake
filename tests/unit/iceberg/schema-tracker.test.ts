/**
 * Iceberg Schema Evolution Tracker Tests (TDD RED Phase)
 *
 * Tests for tracking schema evolution in Iceberg format.
 * The SchemaTracker maintains a history of schema versions and changes,
 * ensuring that schema evolution follows Iceberg's compatibility rules.
 *
 * Iceberg Schema Evolution Specification:
 * - Field IDs are never reused once assigned
 * - New fields must be optional (for backwards compatibility)
 * - Fields must be marked optional before removal
 * - Type widening is allowed (e.g., int -> long, float -> double)
 * - Field renaming preserves the field ID
 * - Nested structs support the same evolution operations
 *
 * Reference: https://iceberg.apache.org/spec/#schema-evolution
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  SchemaTracker,
  createSchemaTracker,
  validateSchemaEvolution,
  generateSchemaId,
  type SchemaChange,
  type SchemaChangeType,
  type SchemaEvolutionMetadata,
  type AddFieldOptions,
  type SchemaEvolutionOptions,
  type SchemaComparisonResult,
} from '../../../src/iceberg/schema-tracker.js';
import type {
  IcebergSchema,
  IcebergType,
  IcebergStructField,
  IcebergStructType,
} from '../../../src/iceberg/metadata.js';

// ============================================================================
// Test Fixtures
// ============================================================================

/**
 * Create a simple test schema with basic fields.
 */
function createSimpleSchema(schemaId: number = 1): IcebergSchema {
  return {
    'schema-id': schemaId,
    type: 'struct',
    fields: [
      { id: 1, name: '_id', required: true, type: 'string' },
      { id: 2, name: 'name', required: false, type: 'string' },
      { id: 3, name: 'email', required: false, type: 'string' },
    ],
  };
}

/**
 * Create a schema with nested struct fields.
 */
function createNestedSchema(schemaId: number = 1): IcebergSchema {
  return {
    'schema-id': schemaId,
    type: 'struct',
    fields: [
      { id: 1, name: '_id', required: true, type: 'string' },
      {
        id: 2,
        name: 'profile',
        required: false,
        type: {
          type: 'struct',
          fields: [
            { id: 3, name: 'firstName', required: false, type: 'string' },
            { id: 4, name: 'lastName', required: false, type: 'string' },
            { id: 5, name: 'age', required: false, type: 'int' },
          ],
        },
      },
      {
        id: 6,
        name: 'address',
        required: false,
        type: {
          type: 'struct',
          fields: [
            { id: 7, name: 'city', required: false, type: 'string' },
            { id: 8, name: 'zip', required: false, type: 'string' },
          ],
        },
      },
    ],
  };
}

/**
 * Create a schema with list and map types.
 */
function createComplexSchema(schemaId: number = 1): IcebergSchema {
  return {
    'schema-id': schemaId,
    type: 'struct',
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
      {
        id: 4,
        name: 'metadata',
        required: false,
        type: {
          type: 'map',
          'key-id': 5,
          'value-id': 6,
          key: 'string',
          value: 'string',
          'value-required': false,
        },
      },
    ],
  };
}

// ============================================================================
// Constructor Tests
// ============================================================================

describe('SchemaTracker - Constructor', () => {
  it('should create an empty schema tracker', () => {
    const tracker = new SchemaTracker();

    expect(tracker.getCurrentSchemaId()).toBe(0);
    expect(tracker.getCurrentSchema()).toBeUndefined();
    expect(tracker.getLastFieldId()).toBe(0);
  });

  it('should create a schema tracker with initial schema', () => {
    const initialSchema = createSimpleSchema(1);
    const tracker = new SchemaTracker(initialSchema);

    expect(tracker.getCurrentSchemaId()).toBe(1);
    expect(tracker.getCurrentSchema()).toEqual(initialSchema);
    expect(tracker.getLastFieldId()).toBe(3); // Highest field ID in the schema
  });

  it('should track the highest field ID from nested schemas', () => {
    const nestedSchema = createNestedSchema(1);
    const tracker = new SchemaTracker(nestedSchema);

    expect(tracker.getLastFieldId()).toBe(8); // Highest field ID in nested schema
  });

  it('should track field IDs from list and map types', () => {
    const complexSchema = createComplexSchema(1);
    const tracker = new SchemaTracker(complexSchema);

    expect(tracker.getLastFieldId()).toBe(6); // Includes element-id, key-id, value-id
  });
});

// ============================================================================
// Schema ID Assignment Tests
// ============================================================================

describe('SchemaTracker - Schema ID assignment', () => {
  let tracker: SchemaTracker;

  beforeEach(() => {
    tracker = new SchemaTracker(createSimpleSchema(1));
  });

  it('should assign incrementing schema IDs for new schemas', () => {
    const { schema } = tracker.addField({
      name: 'age',
      type: 'int',
    });

    expect(schema['schema-id']).toBe(2);

    const secondSchema = tracker.removeField(3); // Remove email
    expect(secondSchema['schema-id']).toBe(3);
  });

  it('should preserve schema ID history', () => {
    tracker.addField({ name: 'age', type: 'int' });
    tracker.addField({ name: 'createdAt', type: 'timestamp' });

    const schemas = tracker.getAllSchemas();
    expect(schemas).toHaveLength(3);
    expect(schemas.map((s) => s['schema-id'])).toEqual([1, 2, 3]);
  });

  it('should track schema IDs in evolution metadata', () => {
    tracker.addField({ name: 'age', type: 'int' });
    tracker.addField({ name: 'createdAt', type: 'timestamp' });

    const metadata = tracker.getEvolutionMetadata();
    expect(metadata.schemaIds).toEqual([1, 2, 3]);
    expect(metadata.currentSchemaId).toBe(3);
  });

  it('should get schema by ID', () => {
    tracker.addField({ name: 'age', type: 'int' });

    const schema1 = tracker.getSchema(1);
    const schema2 = tracker.getSchema(2);
    const schema3 = tracker.getSchema(3);

    expect(schema1).toBeDefined();
    expect(schema1?.fields).toHaveLength(3);

    expect(schema2).toBeDefined();
    expect(schema2?.fields).toHaveLength(4);

    expect(schema3).toBeUndefined();
  });
});

// ============================================================================
// Field ID Assignment Tests
// ============================================================================

describe('SchemaTracker - Field ID assignment', () => {
  let tracker: SchemaTracker;

  beforeEach(() => {
    tracker = new SchemaTracker(createSimpleSchema(1));
  });

  it('should assign unique field IDs that never reuse deleted IDs', () => {
    // Add a field
    const { fieldId: ageFieldId } = tracker.addField({
      name: 'age',
      type: 'int',
    });
    expect(ageFieldId).toBe(4); // Next available after 3

    // Remove a field (email = id 3)
    tracker.makeFieldOptional(3);
    tracker.removeField(3);

    // Add another field - should NOT reuse ID 3
    const { fieldId: countryFieldId } = tracker.addField({
      name: 'country',
      type: 'string',
    });
    expect(countryFieldId).toBe(5);
  });

  it('should get next field ID', () => {
    expect(tracker.getNextFieldId()).toBe(4);

    tracker.addField({ name: 'age', type: 'int' });
    expect(tracker.getNextFieldId()).toBe(5);
  });

  it('should track last field ID accurately', () => {
    expect(tracker.getLastFieldId()).toBe(3);

    tracker.addField({ name: 'age', type: 'int' });
    expect(tracker.getLastFieldId()).toBe(4);

    tracker.addField({ name: 'createdAt', type: 'timestamp' });
    expect(tracker.getLastFieldId()).toBe(5);
  });

  it('should return assigned field ID when adding field', () => {
    const { fieldId } = tracker.addField({
      name: 'createdAt',
      type: 'timestamp',
    });

    expect(fieldId).toBe(4);
  });

  it('should assign field IDs for nested struct fields', () => {
    const { fieldId: profileFieldId } = tracker.addField({
      name: 'profile',
      type: {
        type: 'struct',
        fields: [],
      },
    });

    expect(profileFieldId).toBe(4);

    // Add nested field
    const { fieldId: nestedFieldId } = tracker.addField({
      name: 'bio',
      type: 'string',
      parentFieldId: profileFieldId,
    });

    expect(nestedFieldId).toBe(5);
  });
});

// ============================================================================
// Add Column Tests
// ============================================================================

describe('SchemaTracker - Add columns', () => {
  let tracker: SchemaTracker;

  beforeEach(() => {
    tracker = new SchemaTracker(createSimpleSchema(1));
  });

  it('should add a new optional field', () => {
    const { schema, fieldId } = tracker.addField({
      name: 'age',
      type: 'int',
    });

    expect(fieldId).toBe(4);
    expect(schema.fields).toHaveLength(4);

    const newField = schema.fields.find((f) => f.name === 'age');
    expect(newField).toBeDefined();
    expect(newField?.id).toBe(4);
    expect(newField?.type).toBe('int');
    expect(newField?.required).toBe(false); // Must be optional by default
  });

  it('should reject adding required fields (breaks backwards compatibility)', () => {
    expect(() => {
      tracker.addField({
        name: 'age',
        type: 'int',
        required: true, // Should throw
      });
    }).toThrow(/optional|backwards.*compatible/i);
  });

  it('should add field with documentation', () => {
    const { schema } = tracker.addField({
      name: 'age',
      type: 'int',
      doc: 'User age in years',
    });

    const newField = schema.fields.find((f) => f.name === 'age');
    expect(newField?.doc).toBe('User age in years');
  });

  it('should add field with complex type (struct)', () => {
    const { schema, fieldId } = tracker.addField({
      name: 'address',
      type: {
        type: 'struct',
        fields: [
          { id: 0, name: 'street', required: false, type: 'string' },
          { id: 0, name: 'city', required: false, type: 'string' },
        ],
      },
    });

    expect(fieldId).toBe(4);

    const addressField = schema.fields.find((f) => f.name === 'address');
    expect(addressField).toBeDefined();
    expect((addressField?.type as IcebergStructType).type).toBe('struct');

    // Nested fields should also get unique IDs
    const nestedFields = (addressField?.type as IcebergStructType).fields;
    expect(nestedFields[0].id).toBe(5);
    expect(nestedFields[1].id).toBe(6);
  });

  it('should add field with list type', () => {
    const { schema, fieldId } = tracker.addField({
      name: 'tags',
      type: {
        type: 'list',
        'element-id': 0, // Will be assigned
        element: 'string',
        'element-required': false,
      },
    });

    expect(fieldId).toBe(4);

    const tagsField = schema.fields.find((f) => f.name === 'tags');
    expect(tagsField).toBeDefined();
    expect((tagsField?.type as any).type).toBe('list');
    expect((tagsField?.type as any)['element-id']).toBe(5); // Auto-assigned
  });

  it('should add field with map type', () => {
    const { schema, fieldId } = tracker.addField({
      name: 'metadata',
      type: {
        type: 'map',
        'key-id': 0,
        'value-id': 0,
        key: 'string',
        value: 'string',
        'value-required': false,
      },
    });

    expect(fieldId).toBe(4);

    const metadataField = schema.fields.find((f) => f.name === 'metadata');
    expect(metadataField).toBeDefined();
    expect((metadataField?.type as any).type).toBe('map');
    expect((metadataField?.type as any)['key-id']).toBe(5);
    expect((metadataField?.type as any)['value-id']).toBe(6);
  });

  it('should add nested field to existing struct', () => {
    const nestedSchema = createNestedSchema(1);
    const nestedTracker = new SchemaTracker(nestedSchema);

    // Add a field to the profile struct (id=2)
    const { schema, fieldId } = nestedTracker.addField({
      name: 'nickname',
      type: 'string',
      parentFieldId: 2, // profile field
    });

    expect(fieldId).toBe(9); // Next available after 8

    // Find the profile field and check for new nested field
    const profileField = schema.fields.find((f) => f.id === 2);
    const profileType = profileField?.type as IcebergStructType;
    expect(profileType.fields).toHaveLength(4);

    const nicknameField = profileType.fields.find((f) => f.name === 'nickname');
    expect(nicknameField).toBeDefined();
    expect(nicknameField?.id).toBe(9);
  });

  it('should track add-field change', () => {
    tracker.addField({
      name: 'age',
      type: 'int',
    }, {
      snapshotId: 123,
    });

    const changes = tracker.getChanges(2);
    expect(changes).toHaveLength(1);
    expect(changes[0].type).toBe('add-field');
    expect(changes[0].fieldId).toBe(4);
    expect(changes[0].fieldName).toBe('age');
    expect(changes[0].snapshotId).toBe(123);
  });
});

// ============================================================================
// Drop Column Tests
// ============================================================================

describe('SchemaTracker - Drop columns', () => {
  let tracker: SchemaTracker;

  beforeEach(() => {
    tracker = new SchemaTracker(createSimpleSchema(1));
  });

  it('should reject removing required fields directly', () => {
    expect(() => {
      tracker.removeField(1); // _id is required
    }).toThrow(/required.*optional/i);
  });

  it('should remove optional field', () => {
    // email (id=3) is optional
    const schema = tracker.removeField(3);

    expect(schema.fields).toHaveLength(2);
    expect(schema.fields.find((f) => f.id === 3)).toBeUndefined();
  });

  it('should make field optional before removal', () => {
    // _id (id=1) is required, must be made optional first
    const optionalSchema = tracker.makeFieldOptional(1);
    expect(optionalSchema.fields.find((f) => f.id === 1)?.required).toBe(false);

    const removedSchema = tracker.removeField(1);
    expect(removedSchema.fields.find((f) => f.id === 1)).toBeUndefined();
  });

  it('should track make-optional change', () => {
    tracker.makeFieldOptional(1, { snapshotId: 100 });

    const changes = tracker.getChanges(2);
    expect(changes).toHaveLength(1);
    expect(changes[0].type).toBe('make-optional');
    expect(changes[0].fieldId).toBe(1);
    expect(changes[0].snapshotId).toBe(100);
  });

  it('should track remove-field change', () => {
    tracker.removeField(3, { snapshotId: 200 });

    const changes = tracker.getChanges(2);
    expect(changes).toHaveLength(1);
    expect(changes[0].type).toBe('remove-field');
    expect(changes[0].fieldId).toBe(3);
    expect(changes[0].snapshotId).toBe(200);
  });

  it('should remove nested field from struct', () => {
    const nestedSchema = createNestedSchema(1);
    const nestedTracker = new SchemaTracker(nestedSchema);

    // Remove firstName (id=3) from profile struct
    const schema = nestedTracker.removeField(3);

    const profileField = schema.fields.find((f) => f.id === 2);
    const profileType = profileField?.type as IcebergStructType;
    expect(profileType.fields).toHaveLength(2); // lastName and age remain
    expect(profileType.fields.find((f) => f.id === 3)).toBeUndefined();
  });
});

// ============================================================================
// Rename Column Tests
// ============================================================================

describe('SchemaTracker - Rename columns', () => {
  let tracker: SchemaTracker;

  beforeEach(() => {
    tracker = new SchemaTracker(createSimpleSchema(1));
  });

  it('should rename a field while preserving field ID', () => {
    const schema = tracker.renameField(2, 'fullName');

    const renamedField = schema.fields.find((f) => f.id === 2);
    expect(renamedField?.name).toBe('fullName');
    expect(renamedField?.id).toBe(2); // ID preserved
  });

  it('should track rename-field change with previous name', () => {
    tracker.renameField(2, 'fullName', { snapshotId: 300 });

    const changes = tracker.getChanges(2);
    expect(changes).toHaveLength(1);
    expect(changes[0].type).toBe('rename-field');
    expect(changes[0].fieldId).toBe(2);
    expect(changes[0].fieldName).toBe('fullName');
    expect(changes[0].previousName).toBe('name');
    expect(changes[0].snapshotId).toBe(300);
  });

  it('should throw for non-existent field', () => {
    expect(() => {
      tracker.renameField(999, 'newName');
    }).toThrow(/field.*\d+.*does not exist/i);
  });

  it('should rename nested field in struct', () => {
    const nestedSchema = createNestedSchema(1);
    const nestedTracker = new SchemaTracker(nestedSchema);

    const schema = nestedTracker.renameField(3, 'givenName'); // firstName -> givenName

    const profileField = schema.fields.find((f) => f.id === 2);
    const profileType = profileField?.type as IcebergStructType;
    const renamedField = profileType.fields.find((f) => f.id === 3);

    expect(renamedField?.name).toBe('givenName');
    expect(renamedField?.id).toBe(3);
  });

  it('should reject renaming to duplicate name in same scope', () => {
    expect(() => {
      tracker.renameField(2, 'email'); // name -> email, but email already exists
    }).toThrow(/already exists/i);
  });
});

// ============================================================================
// Type Promotion Tests
// ============================================================================

describe('SchemaTracker - Type promotion', () => {
  it('should allow int to long promotion', () => {
    const schema1: IcebergSchema = {
      'schema-id': 1,
      type: 'struct',
      fields: [
        { id: 1, name: 'count', required: false, type: 'int' },
      ],
    };

    const schema2: IcebergSchema = {
      'schema-id': 2,
      type: 'struct',
      fields: [
        { id: 1, name: 'count', required: false, type: 'long' },
      ],
    };

    const result = validateSchemaEvolution(schema1, schema2);
    expect(result.compatible).toBe(true);
    expect(result.changes).toContainEqual(expect.objectContaining({
      type: 'widen-type',
      fieldId: 1,
      previousType: 'int',
      newType: 'long',
    }));
  });

  it('should allow float to double promotion', () => {
    const schema1: IcebergSchema = {
      'schema-id': 1,
      type: 'struct',
      fields: [
        { id: 1, name: 'price', required: false, type: 'float' },
      ],
    };

    const schema2: IcebergSchema = {
      'schema-id': 2,
      type: 'struct',
      fields: [
        { id: 1, name: 'price', required: false, type: 'double' },
      ],
    };

    const result = validateSchemaEvolution(schema1, schema2);
    expect(result.compatible).toBe(true);
  });

  it('should allow decimal precision increase', () => {
    const schema1: IcebergSchema = {
      'schema-id': 1,
      type: 'struct',
      fields: [
        { id: 1, name: 'amount', required: false, type: 'decimal(10,2)' as IcebergType },
      ],
    };

    const schema2: IcebergSchema = {
      'schema-id': 2,
      type: 'struct',
      fields: [
        { id: 1, name: 'amount', required: false, type: 'decimal(16,2)' as IcebergType },
      ],
    };

    const result = validateSchemaEvolution(schema1, schema2);
    expect(result.compatible).toBe(true);
  });

  it('should reject long to int narrowing', () => {
    const schema1: IcebergSchema = {
      'schema-id': 1,
      type: 'struct',
      fields: [
        { id: 1, name: 'count', required: false, type: 'long' },
      ],
    };

    const schema2: IcebergSchema = {
      'schema-id': 2,
      type: 'struct',
      fields: [
        { id: 1, name: 'count', required: false, type: 'int' },
      ],
    };

    const result = validateSchemaEvolution(schema1, schema2);
    expect(result.compatible).toBe(false);
    expect(result.breakingChanges).toHaveLength(1);
  });

  it('should reject string to int conversion', () => {
    const schema1: IcebergSchema = {
      'schema-id': 1,
      type: 'struct',
      fields: [
        { id: 1, name: 'value', required: false, type: 'string' },
      ],
    };

    const schema2: IcebergSchema = {
      'schema-id': 2,
      type: 'struct',
      fields: [
        { id: 1, name: 'value', required: false, type: 'int' },
      ],
    };

    const result = validateSchemaEvolution(schema1, schema2);
    expect(result.compatible).toBe(false);
  });

  it('should allow fixed to binary widening', () => {
    const schema1: IcebergSchema = {
      'schema-id': 1,
      type: 'struct',
      fields: [
        { id: 1, name: 'hash', required: false, type: 'fixed' },
      ],
    };

    const schema2: IcebergSchema = {
      'schema-id': 2,
      type: 'struct',
      fields: [
        { id: 1, name: 'hash', required: false, type: 'binary' },
      ],
    };

    const result = validateSchemaEvolution(schema1, schema2);
    expect(result.compatible).toBe(true);
  });

  it('should track type widening changes', () => {
    const tracker = new SchemaTracker({
      'schema-id': 1,
      type: 'struct',
      fields: [
        { id: 1, name: 'count', required: false, type: 'int' },
      ],
    });

    tracker.setSchema({
      'schema-id': 2,
      type: 'struct',
      fields: [
        { id: 1, name: 'count', required: false, type: 'long' },
      ],
    });

    const changes = tracker.getChanges(2);
    expect(changes).toContainEqual(expect.objectContaining({
      type: 'widen-type',
      fieldId: 1,
    }));
  });
});

// ============================================================================
// Nested Schema Changes Tests
// ============================================================================

describe('SchemaTracker - Nested schema changes', () => {
  let tracker: SchemaTracker;

  beforeEach(() => {
    tracker = new SchemaTracker(createNestedSchema(1));
  });

  it('should track parent field ID for nested changes', () => {
    tracker.addField({
      name: 'nickname',
      type: 'string',
      parentFieldId: 2, // profile
    });

    const changes = tracker.getChanges(2);
    expect(changes[0].parentFieldId).toBe(2);
  });

  it('should indicate root field with parentFieldId -1', () => {
    tracker.addField({
      name: 'status',
      type: 'string',
    });

    const changes = tracker.getChanges(2);
    expect(changes[0].parentFieldId).toBe(-1);
  });

  it('should add field to deeply nested struct', () => {
    const deepSchema: IcebergSchema = {
      'schema-id': 1,
      type: 'struct',
      fields: [
        {
          id: 1,
          name: 'level1',
          required: false,
          type: {
            type: 'struct',
            fields: [
              {
                id: 2,
                name: 'level2',
                required: false,
                type: {
                  type: 'struct',
                  fields: [
                    { id: 3, name: 'value', required: false, type: 'string' },
                  ],
                },
              },
            ],
          },
        },
      ],
    };

    const deepTracker = new SchemaTracker(deepSchema);

    const { fieldId } = deepTracker.addField({
      name: 'deepValue',
      type: 'int',
      parentFieldId: 2, // level2 struct
    });

    expect(fieldId).toBe(4);

    const schema = deepTracker.getCurrentSchema()!;
    const level1 = schema.fields[0].type as IcebergStructType;
    const level2 = level1.fields[0].type as IcebergStructType;

    expect(level2.fields).toHaveLength(2);
    expect(level2.fields.find((f) => f.name === 'deepValue')).toBeDefined();
  });

  it('should remove field from nested struct and track correctly', () => {
    tracker.removeField(5); // age in profile

    const changes = tracker.getChanges(2);
    expect(changes[0].type).toBe('remove-field');
    expect(changes[0].fieldId).toBe(5);
    expect(changes[0].parentFieldId).toBe(2); // profile
  });

  it('should rename field in nested struct', () => {
    tracker.renameField(7, 'town'); // city -> town in address

    const schema = tracker.getCurrentSchema()!;
    const addressField = schema.fields.find((f) => f.id === 6);
    const addressType = addressField?.type as IcebergStructType;
    const townField = addressType.fields.find((f) => f.id === 7);

    expect(townField?.name).toBe('town');
  });

  it('should allow type widening in nested struct', () => {
    const result = tracker.setSchema({
      ...createNestedSchema(2),
      fields: createNestedSchema(2).fields.map((f) => {
        if (f.id === 2) {
          // Profile struct
          const profileType = f.type as IcebergStructType;
          return {
            ...f,
            type: {
              ...profileType,
              fields: profileType.fields.map((nested) =>
                nested.id === 5 ? { ...nested, type: 'long' as IcebergType } : nested
              ),
            },
          };
        }
        return f;
      }),
    });

    expect(result.compatible).toBe(true);
    expect(result.changes).toContainEqual(expect.objectContaining({
      type: 'widen-type',
      fieldId: 5,
    }));
  });
});

// ============================================================================
// Update Documentation Tests
// ============================================================================

describe('SchemaTracker - Update documentation', () => {
  let tracker: SchemaTracker;

  beforeEach(() => {
    tracker = new SchemaTracker(createSimpleSchema(1));
  });

  it('should update field documentation', () => {
    const schema = tracker.updateFieldDoc(2, 'Full name of the user');

    const field = schema.fields.find((f) => f.id === 2);
    expect(field?.doc).toBe('Full name of the user');
  });

  it('should track update-doc change', () => {
    tracker.updateFieldDoc(2, 'Full name of the user');

    const changes = tracker.getChanges(2);
    expect(changes).toHaveLength(1);
    expect(changes[0].type).toBe('update-doc');
    expect(changes[0].fieldId).toBe(2);
    expect(changes[0].doc).toBe('Full name of the user');
  });
});

// ============================================================================
// Schema Comparison Tests
// ============================================================================

describe('SchemaTracker - Schema comparison', () => {
  it('should detect added fields', () => {
    const schema1: IcebergSchema = {
      'schema-id': 1,
      type: 'struct',
      fields: [
        { id: 1, name: '_id', required: true, type: 'string' },
      ],
    };

    const schema2: IcebergSchema = {
      'schema-id': 2,
      type: 'struct',
      fields: [
        { id: 1, name: '_id', required: true, type: 'string' },
        { id: 2, name: 'name', required: false, type: 'string' },
      ],
    };

    const result = validateSchemaEvolution(schema1, schema2);

    expect(result.compatible).toBe(true);
    expect(result.changes).toContainEqual(expect.objectContaining({
      type: 'add-field',
      fieldId: 2,
    }));
  });

  it('should detect removed fields', () => {
    const schema1: IcebergSchema = {
      'schema-id': 1,
      type: 'struct',
      fields: [
        { id: 1, name: '_id', required: true, type: 'string' },
        { id: 2, name: 'name', required: false, type: 'string' },
      ],
    };

    const schema2: IcebergSchema = {
      'schema-id': 2,
      type: 'struct',
      fields: [
        { id: 1, name: '_id', required: true, type: 'string' },
      ],
    };

    const result = validateSchemaEvolution(schema1, schema2);

    expect(result.changes).toContainEqual(expect.objectContaining({
      type: 'remove-field',
      fieldId: 2,
    }));
  });

  it('should detect renamed fields', () => {
    const schema1: IcebergSchema = {
      'schema-id': 1,
      type: 'struct',
      fields: [
        { id: 1, name: 'name', required: false, type: 'string' },
      ],
    };

    const schema2: IcebergSchema = {
      'schema-id': 2,
      type: 'struct',
      fields: [
        { id: 1, name: 'fullName', required: false, type: 'string' },
      ],
    };

    const result = validateSchemaEvolution(schema1, schema2);

    expect(result.compatible).toBe(true);
    expect(result.changes).toContainEqual(expect.objectContaining({
      type: 'rename-field',
      fieldId: 1,
      previousName: 'name',
      fieldName: 'fullName',
    }));
  });

  it('should detect required to optional change', () => {
    const schema1: IcebergSchema = {
      'schema-id': 1,
      type: 'struct',
      fields: [
        { id: 1, name: 'email', required: true, type: 'string' },
      ],
    };

    const schema2: IcebergSchema = {
      'schema-id': 2,
      type: 'struct',
      fields: [
        { id: 1, name: 'email', required: false, type: 'string' },
      ],
    };

    const result = validateSchemaEvolution(schema1, schema2);

    expect(result.compatible).toBe(true);
    expect(result.changes).toContainEqual(expect.objectContaining({
      type: 'make-optional',
      fieldId: 1,
    }));
  });

  it('should mark optional to required as incompatible', () => {
    const schema1: IcebergSchema = {
      'schema-id': 1,
      type: 'struct',
      fields: [
        { id: 1, name: 'email', required: false, type: 'string' },
      ],
    };

    const schema2: IcebergSchema = {
      'schema-id': 2,
      type: 'struct',
      fields: [
        { id: 1, name: 'email', required: true, type: 'string' },
      ],
    };

    const result = validateSchemaEvolution(schema1, schema2);

    expect(result.compatible).toBe(false);
    expect(result.breakingChanges).toHaveLength(1);
  });
});

// ============================================================================
// Set Schema Tests
// ============================================================================

describe('SchemaTracker - Set schema', () => {
  let tracker: SchemaTracker;

  beforeEach(() => {
    tracker = new SchemaTracker(createSimpleSchema(1));
  });

  it('should set new schema and detect all changes', () => {
    const newSchema: IcebergSchema = {
      'schema-id': 2,
      type: 'struct',
      fields: [
        { id: 1, name: '_id', required: true, type: 'string' },
        { id: 2, name: 'fullName', required: false, type: 'string' }, // renamed
        // email (id=3) removed
        { id: 4, name: 'age', required: false, type: 'int' }, // added
      ],
    };

    const result = tracker.setSchema(newSchema);

    expect(result.changes).toContainEqual(expect.objectContaining({ type: 'rename-field' }));
    expect(result.changes).toContainEqual(expect.objectContaining({ type: 'remove-field' }));
    expect(result.changes).toContainEqual(expect.objectContaining({ type: 'add-field' }));
  });

  it('should validate schema compatibility', () => {
    const incompatibleSchema: IcebergSchema = {
      'schema-id': 2,
      type: 'struct',
      fields: [
        { id: 1, name: '_id', required: true, type: 'int' }, // type change - breaking
      ],
    };

    const result = tracker.setSchema(incompatibleSchema);

    expect(result.compatible).toBe(false);
    expect(result.breakingChanges.length).toBeGreaterThan(0);
  });

  it('should update last field ID when setting schema with higher IDs', () => {
    expect(tracker.getLastFieldId()).toBe(3);

    tracker.setSchema({
      'schema-id': 2,
      type: 'struct',
      fields: [
        { id: 1, name: '_id', required: true, type: 'string' },
        { id: 10, name: 'newField', required: false, type: 'string' },
      ],
    });

    expect(tracker.getLastFieldId()).toBe(10);
  });
});

// ============================================================================
// Evolution Metadata Tests
// ============================================================================

describe('SchemaTracker - Evolution metadata', () => {
  let tracker: SchemaTracker;

  beforeEach(() => {
    tracker = new SchemaTracker(createSimpleSchema(1));
  });

  it('should get complete evolution metadata', () => {
    tracker.addField({ name: 'age', type: 'int' });
    tracker.renameField(2, 'fullName');

    const metadata = tracker.getEvolutionMetadata();

    expect(metadata.schemaIds).toEqual([1, 2, 3]);
    expect(metadata.currentSchemaId).toBe(3);
    expect(metadata.lastFieldId).toBe(4);
    expect(metadata.schemas.size).toBe(3);
    expect(metadata.changes.size).toBe(2); // Changes for schema 2 and 3
  });

  it('should get all changes across versions', () => {
    tracker.addField({ name: 'age', type: 'int' });
    tracker.addField({ name: 'createdAt', type: 'timestamp' });

    const allChanges = tracker.getAllChanges();

    expect(allChanges).toHaveLength(2);
    expect(allChanges.every((c) => c.type === 'add-field')).toBe(true);
  });

  it('should get evolution summary', () => {
    tracker.addField({ name: 'age', type: 'int' });
    tracker.addField({ name: 'createdAt', type: 'timestamp' });
    tracker.removeField(3); // Remove email

    const summary = tracker.getEvolutionSummary();

    expect(summary.schemaCount).toBe(4);
    expect(summary.fieldAdditions).toBe(2);
    expect(summary.fieldRemovals).toBe(1);
    expect(summary.currentFieldCount).toBe(4); // _id, name, age, createdAt
    expect(summary.history).toHaveLength(4);
  });

  it('should include timestamps in changes', () => {
    const before = Date.now();
    tracker.addField({ name: 'age', type: 'int' });
    const after = Date.now();

    const changes = tracker.getChanges(2);
    expect(changes[0].timestampMs).toBeGreaterThanOrEqual(before);
    expect(changes[0].timestampMs).toBeLessThanOrEqual(after);
  });

  it('should support custom timestamp in evolution options', () => {
    const customTimestamp = 1700000000000;
    tracker.addField({ name: 'age', type: 'int' }, { timestampMs: customTimestamp });

    const changes = tracker.getChanges(2);
    expect(changes[0].timestampMs).toBe(customTimestamp);
  });
});

// ============================================================================
// Static Factory Methods Tests
// ============================================================================

describe('SchemaTracker - Static factory methods', () => {
  it('should create from evolution metadata', () => {
    const originalTracker = new SchemaTracker(createSimpleSchema(1));
    originalTracker.addField({ name: 'age', type: 'int' });

    const metadata = originalTracker.getEvolutionMetadata();
    const restoredTracker = SchemaTracker.fromMetadata(metadata);

    expect(restoredTracker.getCurrentSchemaId()).toBe(2);
    expect(restoredTracker.getLastFieldId()).toBe(4);
    expect(restoredTracker.getAllSchemas()).toHaveLength(2);
  });

  it('should create from array of schemas', () => {
    const schemas: IcebergSchema[] = [
      createSimpleSchema(1),
      {
        'schema-id': 2,
        type: 'struct',
        fields: [
          ...createSimpleSchema(2).fields,
          { id: 4, name: 'age', required: false, type: 'int' },
        ],
      },
    ];

    const tracker = SchemaTracker.fromSchemas(schemas, 2);

    expect(tracker.getCurrentSchemaId()).toBe(2);
    expect(tracker.getLastFieldId()).toBe(4);
    expect(tracker.getAllSchemas()).toHaveLength(2);
  });
});

// ============================================================================
// Utility Function Tests
// ============================================================================

describe('createSchemaTracker', () => {
  it('should create tracker from schemas array', () => {
    const schemas: IcebergSchema[] = [createSimpleSchema(1)];
    const tracker = createSchemaTracker(schemas, 1);

    expect(tracker).toBeInstanceOf(SchemaTracker);
    expect(tracker.getCurrentSchemaId()).toBe(1);
  });
});

describe('validateSchemaEvolution', () => {
  it('should validate compatible schema evolution', () => {
    const oldSchema = createSimpleSchema(1);
    const newSchema: IcebergSchema = {
      ...createSimpleSchema(2),
      fields: [
        ...createSimpleSchema(2).fields,
        { id: 4, name: 'age', required: false, type: 'int' },
      ],
    };

    const result = validateSchemaEvolution(oldSchema, newSchema);

    expect(result.compatible).toBe(true);
    expect(result.breakingChanges).toHaveLength(0);
  });

  it('should detect incompatible schema evolution', () => {
    const oldSchema = createSimpleSchema(1);
    const newSchema: IcebergSchema = {
      'schema-id': 2,
      type: 'struct',
      fields: [
        { id: 1, name: '_id', required: true, type: 'int' }, // Breaking: string -> int
      ],
    };

    const result = validateSchemaEvolution(oldSchema, newSchema);

    expect(result.compatible).toBe(false);
    expect(result.breakingChanges.length).toBeGreaterThan(0);
  });
});

describe('generateSchemaId', () => {
  it('should generate next schema ID from existing schemas', () => {
    const schemas: IcebergSchema[] = [
      createSimpleSchema(1),
      createSimpleSchema(5),
      createSimpleSchema(3),
    ];

    const nextId = generateSchemaId(schemas);

    expect(nextId).toBe(6); // Max + 1
  });

  it('should return 1 for empty schema list', () => {
    const nextId = generateSchemaId([]);
    expect(nextId).toBe(1);
  });
});

// ============================================================================
// Edge Cases Tests
// ============================================================================

describe('SchemaTracker - Edge cases', () => {
  it('should handle empty schema', () => {
    const emptySchema: IcebergSchema = {
      'schema-id': 1,
      type: 'struct',
      fields: [],
    };

    const tracker = new SchemaTracker(emptySchema);

    expect(tracker.getCurrentSchemaId()).toBe(1);
    expect(tracker.getLastFieldId()).toBe(0);
  });

  it('should handle schema with only one field', () => {
    const singleFieldSchema: IcebergSchema = {
      'schema-id': 1,
      type: 'struct',
      fields: [
        { id: 1, name: '_id', required: true, type: 'string' },
      ],
    };

    const tracker = new SchemaTracker(singleFieldSchema);
    tracker.makeFieldOptional(1);
    tracker.removeField(1);

    const currentSchema = tracker.getCurrentSchema()!;
    expect(currentSchema.fields).toHaveLength(0);
  });

  it('should handle very large field IDs', () => {
    const largeIdSchema: IcebergSchema = {
      'schema-id': 1,
      type: 'struct',
      fields: [
        { id: 1000000, name: 'field', required: false, type: 'string' },
      ],
    };

    const tracker = new SchemaTracker(largeIdSchema);

    expect(tracker.getLastFieldId()).toBe(1000000);
    expect(tracker.getNextFieldId()).toBe(1000001);
  });

  it('should handle fields with special characters in names', () => {
    const tracker = new SchemaTracker(createSimpleSchema(1));

    const { schema, fieldId } = tracker.addField({
      name: 'field_with_underscore',
      type: 'string',
    });

    expect(fieldId).toBe(4);
    expect(schema.fields.find((f) => f.name === 'field_with_underscore')).toBeDefined();
  });

  it('should handle concurrent-like operations', () => {
    const tracker = new SchemaTracker(createSimpleSchema(1));

    // Simulate rapid changes
    tracker.addField({ name: 'field1', type: 'string' });
    tracker.addField({ name: 'field2', type: 'string' });
    tracker.addField({ name: 'field3', type: 'string' });
    tracker.renameField(4, 'renamedField1');
    tracker.removeField(5);

    expect(tracker.getLastFieldId()).toBe(6);
    expect(tracker.getCurrentSchemaId()).toBe(6); // 1 initial + 3 adds + 1 rename + 1 remove
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('SchemaTracker - Integration', () => {
  it('should track complete schema evolution lifecycle', () => {
    // Start with initial schema
    const tracker = new SchemaTracker({
      'schema-id': 1,
      type: 'struct',
      fields: [
        { id: 1, name: '_id', required: true, type: 'string' },
        { id: 2, name: 'name', required: true, type: 'string' },
      ],
    });

    // Add optional field
    tracker.addField({
      name: 'email',
      type: 'string',
      doc: 'User email address',
    }, { snapshotId: 100 });

    // Add nested struct
    tracker.addField({
      name: 'profile',
      type: {
        type: 'struct',
        fields: [
          { id: 0, name: 'bio', required: false, type: 'string' },
          { id: 0, name: 'age', required: false, type: 'int' },
        ],
      },
    }, { snapshotId: 200 });

    // Rename field
    tracker.renameField(2, 'fullName', { snapshotId: 300 });

    // Make field optional (preparation for removal)
    tracker.makeFieldOptional(2, { snapshotId: 400 });

    // Remove field
    tracker.removeField(2, { snapshotId: 500 });

    // Verify final state
    const summary = tracker.getEvolutionSummary();
    expect(summary.schemaCount).toBe(6);
    expect(summary.fieldAdditions).toBe(2); // email and profile (struct counted as 1)
    expect(summary.fieldRemovals).toBe(1); // fullName removed

    // Verify metadata
    const metadata = tracker.getEvolutionMetadata();
    expect(metadata.schemaIds).toEqual([1, 2, 3, 4, 5, 6]);
    expect(metadata.currentSchemaId).toBe(6);

    // Verify current schema
    const current = tracker.getCurrentSchema()!;
    expect(current.fields).toHaveLength(3); // _id, email, profile
    expect(current.fields.find((f) => f.name === 'fullName')).toBeUndefined();
  });

  it('should support restoring and continuing evolution', () => {
    // Create initial tracker with some evolution
    const original = new SchemaTracker(createSimpleSchema(1));
    original.addField({ name: 'age', type: 'int' });

    // Save metadata
    const metadata = original.getEvolutionMetadata();

    // Restore from metadata
    const restored = SchemaTracker.fromMetadata(metadata);

    // Continue evolution
    restored.addField({ name: 'createdAt', type: 'timestamp' });

    expect(restored.getCurrentSchemaId()).toBe(3);
    expect(restored.getLastFieldId()).toBe(5);
    expect(restored.getAllSchemas()).toHaveLength(3);
  });
});

// ============================================================================
// Migration Helper Tests
// ============================================================================

describe('generateMigrationPlan', () => {
  it('should generate migration plan for field additions', async () => {
    const { generateMigrationPlan } = await import('../../../src/iceberg/schema-tracker.js');

    const tracker = new SchemaTracker(createSimpleSchema(1));
    tracker.addField({ name: 'age', type: 'int' });

    const plan = generateMigrationPlan(tracker, 1, 2);

    expect(plan.fromSchemaId).toBe(1);
    expect(plan.toSchemaId).toBe(2);
    expect(plan.reversible).toBe(true);
    expect(plan.steps.some(s => s.operation === 'add-default' && s.targetFieldId === 4)).toBe(true);
    expect(plan.description).toContain('1 field(s) added');
  });

  it('should generate migration plan for field removals', async () => {
    const { generateMigrationPlan } = await import('../../../src/iceberg/schema-tracker.js');

    const tracker = new SchemaTracker(createSimpleSchema(1));
    tracker.removeField(3); // email is optional

    const plan = generateMigrationPlan(tracker, 1, 2);

    expect(plan.steps.some(s => s.operation === 'drop' && s.sourceFieldId === 3)).toBe(true);
    expect(plan.description).toContain('1 field(s) removed');
  });

  it('should generate migration plan for field renames', async () => {
    const { generateMigrationPlan } = await import('../../../src/iceberg/schema-tracker.js');

    const tracker = new SchemaTracker(createSimpleSchema(1));
    tracker.renameField(2, 'fullName');

    const plan = generateMigrationPlan(tracker, 1, 2);

    expect(plan.steps.some(s => s.operation === 'rename' && s.sourceFieldId === 2)).toBe(true);
    expect(plan.description).toContain('1 field(s) modified');
  });

  it('should throw for non-existent source schema', async () => {
    const { generateMigrationPlan, SchemaTrackerError } = await import('../../../src/iceberg/schema-tracker.js');

    const tracker = new SchemaTracker(createSimpleSchema(1));

    expect(() => generateMigrationPlan(tracker, 999, 1)).toThrow(SchemaTrackerError);
  });
});

describe('isMigrationSafe', () => {
  it('should return true for safe migrations', async () => {
    const { isMigrationSafe } = await import('../../../src/iceberg/schema-tracker.js');

    const tracker = new SchemaTracker(createSimpleSchema(1));
    tracker.addField({ name: 'age', type: 'int' });

    expect(isMigrationSafe(tracker, 1, 2)).toBe(true);
  });

  it('should return false for unsafe type changes', async () => {
    const { isMigrationSafe } = await import('../../../src/iceberg/schema-tracker.js');

    const tracker = new SchemaTracker({
      'schema-id': 1,
      type: 'struct',
      fields: [
        { id: 1, name: 'count', required: false, type: 'string' },
      ],
    });

    tracker.setSchema({
      'schema-id': 2,
      type: 'struct',
      fields: [
        { id: 1, name: 'count', required: false, type: 'int' },
      ],
    });

    expect(isMigrationSafe(tracker, 1, 2)).toBe(false);
  });
});

describe('getChangeSummary', () => {
  it('should return human-readable change descriptions', async () => {
    const { getChangeSummary } = await import('../../../src/iceberg/schema-tracker.js');

    const tracker = new SchemaTracker(createSimpleSchema(1));
    tracker.addField({ name: 'age', type: 'int' });
    tracker.renameField(2, 'fullName');

    const summary = getChangeSummary(tracker, 1, 3);

    expect(summary.some(s => s.includes('Added optional field'))).toBe(true);
    expect(summary.some(s => s.includes('Renamed field'))).toBe(true);
  });

  it('should indicate when schemas not found', async () => {
    const { getChangeSummary } = await import('../../../src/iceberg/schema-tracker.js');

    const tracker = new SchemaTracker(createSimpleSchema(1));

    const summary = getChangeSummary(tracker, 999, 1);

    expect(summary[0]).toContain('not found');
  });
});

describe('SchemaTrackerError', () => {
  it('should be exported and usable', async () => {
    const { SchemaTrackerError } = await import('../../../src/iceberg/schema-tracker.js');

    const error = new SchemaTrackerError('Test error', 'TEST_CODE', { extra: 'data' });

    expect(error.message).toBe('Test error');
    expect(error.code).toBe('TEST_CODE');
    expect(error.context).toEqual({ extra: 'data' });
    expect(error.name).toBe('SchemaTrackerError');
  });
});

describe('ROOT_PARENT_ID', () => {
  it('should be exported and equal to -1', async () => {
    const { ROOT_PARENT_ID } = await import('../../../src/iceberg/schema-tracker.js');

    expect(ROOT_PARENT_ID).toBe(-1);
  });
});
