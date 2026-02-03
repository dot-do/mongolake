/**
 * Backward Compatibility Reader Tests
 *
 * Tests for reading Parquet files written with older schema versions
 * using the current schema. Handles:
 * - Missing columns (new fields in current schema)
 * - Removed columns (fields in old file not in current schema)
 * - Type widening (int32 -> int64, float -> double)
 * - Default values for new required fields
 *
 * RED Phase: These tests should fail until CompatReader is implemented.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  CompatReader,
  type CompatReaderOptions,
  type SchemaMapping,
  type ColumnMapping,
  type ReadResult,
  createColumnMapping,
  reconcileSchemas,
  applyDefaults,
} from '../compat-reader.js';
import { RowGroupSerializer, type SerializedRowGroup } from '../row-group.js';
import { RowGroupReader } from '../row-group-reader.js';
import type { SchemaField } from '../../schema/versioning.js';

// ============================================================================
// Test Fixtures
// ============================================================================

interface OldUserDocument {
  _id: string;
  _seq: number;
  _op: 'i' | 'u' | 'd';
  name: string;
  age: number;
}

interface NewUserDocument {
  _id: string;
  _seq: number;
  _op: 'i' | 'u' | 'd';
  name: string;
  age: number;
  email?: string;
  createdAt?: Date;
  active?: boolean;
}

interface TypeEvolvedDocument {
  _id: string;
  _seq: number;
  _op: 'i' | 'u' | 'd';
  count: number; // was int32, now int64
  score: number; // was float, now double
}

// Create old schema (V1)
function createOldSchema(): Record<string, SchemaField> {
  return {
    _id: { type: 'string', required: true },
    _seq: { type: 'int64', required: true },
    _op: { type: 'string', required: true },
    name: { type: 'string', required: false },
    age: { type: 'int32', required: false },
  };
}

// Create new schema (V2) with added fields
function createNewSchema(): Record<string, SchemaField> {
  return {
    _id: { type: 'string', required: true },
    _seq: { type: 'int64', required: true },
    _op: { type: 'string', required: true },
    name: { type: 'string', required: false },
    age: { type: 'int64', required: false }, // Widened from int32
    email: { type: 'string', required: false },
    createdAt: { type: 'timestamp', required: false },
    active: { type: 'boolean', required: false },
  };
}

// Create schema with removed fields
function createSchemaWithRemovedFields(): Record<string, SchemaField> {
  return {
    _id: { type: 'string', required: true },
    _seq: { type: 'int64', required: true },
    _op: { type: 'string', required: true },
    name: { type: 'string', required: false },
    // age field removed
    email: { type: 'string', required: false },
  };
}

// Create old documents for serialization
function createOldDocuments(): OldUserDocument[] {
  return [
    { _id: 'user1', _seq: 1, _op: 'i', name: 'Alice', age: 25 },
    { _id: 'user2', _seq: 2, _op: 'i', name: 'Bob', age: 30 },
    { _id: 'user3', _seq: 3, _op: 'i', name: 'Charlie', age: 35 },
  ];
}

// Serialize documents to a row group
function serializeDocuments<T extends Record<string, unknown>>(docs: T[]): SerializedRowGroup {
  const serializer = new RowGroupSerializer({ compression: 'none' });
  return serializer.serialize(docs);
}

// ============================================================================
// CompatReader Constructor Tests
// ============================================================================

describe('CompatReader', () => {
  describe('constructor', () => {
    it('should create a compat reader with target schema', () => {
      const targetSchema = createNewSchema();
      const reader = new CompatReader({ targetSchema });

      expect(reader).toBeDefined();
      expect(reader.getTargetSchema()).toEqual(targetSchema);
    });

    it('should accept optional default values', () => {
      const targetSchema = createNewSchema();
      const defaults = {
        email: 'unknown@example.com',
        active: false,
      };

      const reader = new CompatReader({ targetSchema, defaults });

      expect(reader.getDefaults()).toEqual(defaults);
    });

    it('should accept strict mode option', () => {
      const targetSchema = createNewSchema();
      const reader = new CompatReader({ targetSchema, strictMode: true });

      expect(reader.isStrictMode()).toBe(true);
    });

    it('should default to non-strict mode', () => {
      const targetSchema = createNewSchema();
      const reader = new CompatReader({ targetSchema });

      expect(reader.isStrictMode()).toBe(false);
    });
  });
});

// ============================================================================
// Reading Old Version with New Schema Tests
// ============================================================================

describe('CompatReader reading old version with new schema', () => {
  let reader: CompatReader;
  let serializedOldData: SerializedRowGroup;

  beforeEach(() => {
    // Serialize documents with old schema
    serializedOldData = serializeDocuments(createOldDocuments());

    // Create reader with new schema
    reader = new CompatReader({
      targetSchema: createNewSchema(),
      sourceSchema: createOldSchema(),
    });
  });

  it('should read documents from older version file', () => {
    const result = reader.read<NewUserDocument>(serializedOldData);

    expect(result.documents).toHaveLength(3);
  });

  it('should preserve existing fields from old data', () => {
    const result = reader.read<NewUserDocument>(serializedOldData);

    expect(result.documents[0].name).toBe('Alice');
    expect(result.documents[0].age).toBe(25);
    expect(result.documents[1].name).toBe('Bob');
    expect(result.documents[1].age).toBe(30);
  });

  it('should include required system fields', () => {
    const result = reader.read<NewUserDocument>(serializedOldData);

    for (const doc of result.documents) {
      expect(doc._id).toBeDefined();
      expect(doc._seq).toBeDefined();
      expect(doc._op).toBeDefined();
    }
  });

  it('should return undefined for new fields not in old data', () => {
    const result = reader.read<NewUserDocument>(serializedOldData);

    for (const doc of result.documents) {
      expect(doc.email).toBeUndefined();
      expect(doc.createdAt).toBeUndefined();
      expect(doc.active).toBeUndefined();
    }
  });

  it('should include schema mapping info in result', () => {
    const result = reader.read<NewUserDocument>(serializedOldData);

    expect(result.schemaMapping).toBeDefined();
    expect(result.schemaMapping.missingColumns).toContain('email');
    expect(result.schemaMapping.missingColumns).toContain('createdAt');
    expect(result.schemaMapping.missingColumns).toContain('active');
  });

  it('should indicate compatibility status in result', () => {
    const result = reader.read<NewUserDocument>(serializedOldData);

    expect(result.isCompatible).toBe(true);
    expect(result.compatibilityWarnings).toHaveLength(0);
  });
});

// ============================================================================
// Handling Missing Columns Tests
// ============================================================================

describe('CompatReader handling missing columns', () => {
  it('should fill missing columns with null by default', () => {
    const reader = new CompatReader({
      targetSchema: createNewSchema(),
      sourceSchema: createOldSchema(),
    });

    const serialized = serializeDocuments(createOldDocuments());
    const result = reader.read<NewUserDocument>(serialized);

    for (const doc of result.documents) {
      // Fields not in old data should be undefined (sparse documents)
      expect(doc.email).toBeUndefined();
    }
  });

  it('should apply default values for missing columns when specified', () => {
    const reader = new CompatReader({
      targetSchema: createNewSchema(),
      sourceSchema: createOldSchema(),
      defaults: {
        email: 'default@example.com',
        active: false,
      },
    });

    const serialized = serializeDocuments(createOldDocuments());
    const result = reader.read<NewUserDocument>(serialized);

    for (const doc of result.documents) {
      expect(doc.email).toBe('default@example.com');
      expect(doc.active).toBe(false);
    }
  });

  it('should apply default values from schema definitions', () => {
    const schemaWithDefaults: Record<string, SchemaField & { default?: unknown }> = {
      ...createNewSchema(),
      email: { type: 'string', required: false, default: 'none' },
    };

    const reader = new CompatReader({
      targetSchema: schemaWithDefaults as Record<string, SchemaField>,
      sourceSchema: createOldSchema(),
      useSchemaDefaults: true,
    });

    const serialized = serializeDocuments(createOldDocuments());
    const result = reader.read<NewUserDocument>(serialized);

    // Email should have schema default
    expect(result.documents[0].email).toBe('none');
  });

  it('should track which columns were filled with defaults', () => {
    const reader = new CompatReader({
      targetSchema: createNewSchema(),
      sourceSchema: createOldSchema(),
      defaults: { email: 'default@example.com' },
    });

    const serialized = serializeDocuments(createOldDocuments());
    const result = reader.read<NewUserDocument>(serialized);

    expect(result.schemaMapping.columnsWithDefaults).toContain('email');
  });
});

// ============================================================================
// Handling Removed Columns Tests
// ============================================================================

describe('CompatReader handling removed columns', () => {
  it('should ignore columns in file but not in target schema', () => {
    const reader = new CompatReader({
      targetSchema: createSchemaWithRemovedFields(),
      sourceSchema: createOldSchema(),
    });

    const serialized = serializeDocuments(createOldDocuments());
    const result = reader.read<Record<string, unknown>>(serialized);

    // age field should not be in result since it's not in target schema
    for (const doc of result.documents) {
      expect(doc).not.toHaveProperty('age');
    }
  });

  it('should include removed columns in schema mapping', () => {
    const reader = new CompatReader({
      targetSchema: createSchemaWithRemovedFields(),
      sourceSchema: createOldSchema(),
    });

    const serialized = serializeDocuments(createOldDocuments());
    const result = reader.read<Record<string, unknown>>(serialized);

    expect(result.schemaMapping.removedColumns).toContain('age');
  });

  it('should emit warning for removed columns in non-strict mode', () => {
    const reader = new CompatReader({
      targetSchema: createSchemaWithRemovedFields(),
      sourceSchema: createOldSchema(),
      strictMode: false,
    });

    const serialized = serializeDocuments(createOldDocuments());
    const result = reader.read<Record<string, unknown>>(serialized);

    expect(result.compatibilityWarnings).toContainEqual(
      expect.objectContaining({
        type: 'removed_column',
        column: 'age',
      })
    );
  });

  it('should throw in strict mode when columns are removed', () => {
    const reader = new CompatReader({
      targetSchema: createSchemaWithRemovedFields(),
      sourceSchema: createOldSchema(),
      strictMode: true,
    });

    const serialized = serializeDocuments(createOldDocuments());

    expect(() => reader.read(serialized)).toThrow(/removed.*column/i);
  });

  it('should optionally preserve removed columns', () => {
    const reader = new CompatReader({
      targetSchema: createSchemaWithRemovedFields(),
      sourceSchema: createOldSchema(),
      preserveRemovedColumns: true,
    });

    const serialized = serializeDocuments(createOldDocuments());
    const result = reader.read<Record<string, unknown>>(serialized);

    // With preserveRemovedColumns, age should still be present
    expect(result.documents[0].age).toBe(25);
  });
});

// ============================================================================
// Type Widening Tests
// ============================================================================

describe('CompatReader type widening', () => {
  it('should widen int32 to int64', () => {
    const oldSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      _seq: { type: 'int64', required: true },
      _op: { type: 'string', required: true },
      count: { type: 'int32', required: false },
    };

    const newSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      _seq: { type: 'int64', required: true },
      _op: { type: 'string', required: true },
      count: { type: 'int64', required: false }, // Widened
    };

    const docs = [
      { _id: 'doc1', _seq: 1, _op: 'i' as const, count: 100 },
      { _id: 'doc2', _seq: 2, _op: 'i' as const, count: 200 },
    ];

    const serialized = serializeDocuments(docs);

    const reader = new CompatReader({
      targetSchema: newSchema,
      sourceSchema: oldSchema,
    });

    const result = reader.read<TypeEvolvedDocument>(serialized);

    expect(result.documents[0].count).toBe(100);
    expect(result.documents[1].count).toBe(200);
    expect(result.schemaMapping.widenedColumns).toContainEqual({
      column: 'count',
      fromType: 'int32',
      toType: 'int64',
    });
  });

  it('should widen float to double', () => {
    const oldSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      _seq: { type: 'int64', required: true },
      _op: { type: 'string', required: true },
      score: { type: 'float', required: false },
    };

    const newSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      _seq: { type: 'int64', required: true },
      _op: { type: 'string', required: true },
      score: { type: 'double', required: false }, // Widened
    };

    const docs = [
      { _id: 'doc1', _seq: 1, _op: 'i' as const, score: 95.5 },
      { _id: 'doc2', _seq: 2, _op: 'i' as const, score: 87.25 },
    ];

    const serialized = serializeDocuments(docs);

    const reader = new CompatReader({
      targetSchema: newSchema,
      sourceSchema: oldSchema,
    });

    const result = reader.read<TypeEvolvedDocument>(serialized);

    expect(result.documents[0].score).toBeCloseTo(95.5, 5);
    expect(result.documents[1].score).toBeCloseTo(87.25, 5);
  });

  it('should widen date to timestamp', () => {
    const oldSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      _seq: { type: 'int64', required: true },
      _op: { type: 'string', required: true },
      eventDate: { type: 'date', required: false },
    };

    const newSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      _seq: { type: 'int64', required: true },
      _op: { type: 'string', required: true },
      eventDate: { type: 'timestamp', required: false }, // Widened
    };

    const date = new Date('2024-06-15T10:30:00Z');
    const docs = [{ _id: 'doc1', _seq: 1, _op: 'i' as const, eventDate: date }];

    const serialized = serializeDocuments(docs);

    const reader = new CompatReader({
      targetSchema: newSchema,
      sourceSchema: oldSchema,
    });

    const result = reader.read<Record<string, unknown>>(serialized);

    expect(result.documents[0].eventDate).toBeInstanceOf(Date);
  });

  it('should widen any type to variant', () => {
    const oldSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      _seq: { type: 'int64', required: true },
      _op: { type: 'string', required: true },
      data: { type: 'string', required: false },
    };

    const newSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      _seq: { type: 'int64', required: true },
      _op: { type: 'string', required: true },
      data: { type: 'variant', required: false }, // Widened to variant
    };

    const docs = [{ _id: 'doc1', _seq: 1, _op: 'i' as const, data: 'hello' }];

    const serialized = serializeDocuments(docs);

    const reader = new CompatReader({
      targetSchema: newSchema,
      sourceSchema: oldSchema,
    });

    const result = reader.read<Record<string, unknown>>(serialized);

    expect(result.documents[0].data).toBe('hello');
  });

  it('should reject type narrowing in strict mode', () => {
    const oldSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      _seq: { type: 'int64', required: true },
      _op: { type: 'string', required: true },
      count: { type: 'int64', required: false },
    };

    const newSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      _seq: { type: 'int64', required: true },
      _op: { type: 'string', required: true },
      count: { type: 'int32', required: false }, // Narrowed - not allowed
    };

    const docs = [{ _id: 'doc1', _seq: 1, _op: 'i' as const, count: 100 }];

    const serialized = serializeDocuments(docs);

    const reader = new CompatReader({
      targetSchema: newSchema,
      sourceSchema: oldSchema,
      strictMode: true,
    });

    expect(() => reader.read(serialized)).toThrow(/type narrowing/i);
  });

  it('should warn about type narrowing in non-strict mode', () => {
    const oldSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      _seq: { type: 'int64', required: true },
      _op: { type: 'string', required: true },
      count: { type: 'int64', required: false },
    };

    const newSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      _seq: { type: 'int64', required: true },
      _op: { type: 'string', required: true },
      count: { type: 'int32', required: false }, // Narrowed
    };

    const docs = [{ _id: 'doc1', _seq: 1, _op: 'i' as const, count: 100 }];

    const serialized = serializeDocuments(docs);

    const reader = new CompatReader({
      targetSchema: newSchema,
      sourceSchema: oldSchema,
      strictMode: false,
    });

    const result = reader.read<Record<string, unknown>>(serialized);

    expect(result.compatibilityWarnings).toContainEqual(
      expect.objectContaining({
        type: 'type_narrowing',
        column: 'count',
      })
    );
  });
});

// ============================================================================
// Default Values for New Fields Tests
// ============================================================================

describe('CompatReader default values', () => {
  it('should apply function-based defaults', () => {
    const reader = new CompatReader({
      targetSchema: createNewSchema(),
      sourceSchema: createOldSchema(),
      defaults: {
        createdAt: () => new Date('2024-01-01'),
      },
    });

    const serialized = serializeDocuments(createOldDocuments());
    const result = reader.read<NewUserDocument>(serialized);

    for (const doc of result.documents) {
      expect(doc.createdAt).toEqual(new Date('2024-01-01'));
    }
  });

  it('should apply defaults based on existing field values', () => {
    const reader = new CompatReader({
      targetSchema: createNewSchema(),
      sourceSchema: createOldSchema(),
      defaults: {
        email: (doc: Record<string, unknown>) => `${String(doc.name).toLowerCase()}@example.com`,
      },
    });

    const serialized = serializeDocuments(createOldDocuments());
    const result = reader.read<NewUserDocument>(serialized);

    expect(result.documents[0].email).toBe('alice@example.com');
    expect(result.documents[1].email).toBe('bob@example.com');
  });

  it('should not override existing values with defaults', () => {
    const docs = [
      { _id: 'user1', _seq: 1, _op: 'i' as const, name: 'Alice', age: 25, email: 'alice@real.com' },
    ];

    const serialized = serializeDocuments(docs);

    const reader = new CompatReader({
      targetSchema: createNewSchema(),
      defaults: {
        email: 'default@example.com',
      },
    });

    const result = reader.read<NewUserDocument>(serialized);

    // Existing email should not be overwritten
    expect(result.documents[0].email).toBe('alice@real.com');
  });

  it('should support default value generators per row', () => {
    let counter = 0;
    const reader = new CompatReader({
      targetSchema: createNewSchema(),
      sourceSchema: createOldSchema(),
      defaults: {
        active: () => {
          counter++;
          return counter % 2 === 0;
        },
      },
    });

    const serialized = serializeDocuments(createOldDocuments());
    const result = reader.read<NewUserDocument>(serialized);

    expect(result.documents[0].active).toBe(false); // counter = 1
    expect(result.documents[1].active).toBe(true); // counter = 2
    expect(result.documents[2].active).toBe(false); // counter = 3
  });
});

// ============================================================================
// Schema Reconciliation Utility Tests
// ============================================================================

describe('reconcileSchemas utility', () => {
  it('should identify added columns', () => {
    const result = reconcileSchemas(createOldSchema(), createNewSchema());

    expect(result.addedColumns).toContain('email');
    expect(result.addedColumns).toContain('createdAt');
    expect(result.addedColumns).toContain('active');
  });

  it('should identify removed columns', () => {
    const result = reconcileSchemas(createNewSchema(), createSchemaWithRemovedFields());

    expect(result.removedColumns).toContain('age');
    expect(result.removedColumns).toContain('createdAt');
    expect(result.removedColumns).toContain('active');
  });

  it('should identify type changes', () => {
    const result = reconcileSchemas(createOldSchema(), createNewSchema());

    const ageChange = result.typeChanges.find((c) => c.column === 'age');
    expect(ageChange).toBeDefined();
    expect(ageChange?.fromType).toBe('int32');
    expect(ageChange?.toType).toBe('int64');
  });

  it('should identify compatible vs incompatible changes', () => {
    const result = reconcileSchemas(createOldSchema(), createNewSchema());

    expect(result.isCompatible).toBe(true);
  });

  it('should mark incompatible when type narrowing occurs', () => {
    const oldSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      count: { type: 'int64', required: false },
    };
    const newSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      count: { type: 'int32', required: false },
    };

    const result = reconcileSchemas(oldSchema, newSchema);

    expect(result.isCompatible).toBe(false);
  });
});

// ============================================================================
// Column Mapping Utility Tests
// ============================================================================

describe('createColumnMapping utility', () => {
  it('should create identity mapping for matching columns', () => {
    const sourceSchema = createOldSchema();
    const targetSchema = createOldSchema();

    const mapping = createColumnMapping(sourceSchema, targetSchema);

    expect(mapping.get('name')).toEqual({
      sourceColumn: 'name',
      targetColumn: 'name',
      transform: null,
    });
  });

  it('should create type conversion mapping for widened types', () => {
    const sourceSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      count: { type: 'int32', required: false },
    };
    const targetSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      count: { type: 'int64', required: false },
    };

    const mapping = createColumnMapping(sourceSchema, targetSchema);

    const countMapping = mapping.get('count');
    expect(countMapping?.transform).toBeDefined();
    expect(countMapping?.transform?.name).toBe('widen_int32_to_int64');
  });

  it('should mark missing target columns', () => {
    const sourceSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      oldField: { type: 'string', required: false },
    };
    const targetSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
    };

    const mapping = createColumnMapping(sourceSchema, targetSchema);

    expect(mapping.get('oldField')).toBeUndefined();
  });

  it('should mark missing source columns', () => {
    const sourceSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
    };
    const targetSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      newField: { type: 'string', required: false },
    };

    const mapping = createColumnMapping(sourceSchema, targetSchema);

    expect(mapping.get('newField')).toEqual({
      sourceColumn: null,
      targetColumn: 'newField',
      transform: null,
      useDefault: true,
    });
  });
});

// ============================================================================
// Apply Defaults Utility Tests
// ============================================================================

describe('applyDefaults utility', () => {
  it('should apply static defaults', () => {
    const doc = { _id: 'doc1', name: 'Alice' };
    const defaults = { email: 'default@example.com' };

    const result = applyDefaults(doc, defaults);

    expect(result.email).toBe('default@example.com');
    expect(result.name).toBe('Alice');
  });

  it('should apply function defaults', () => {
    const doc = { _id: 'doc1', name: 'Alice' };
    const defaults = {
      email: (d: Record<string, unknown>) => `${String(d.name).toLowerCase()}@example.com`,
    };

    const result = applyDefaults(doc, defaults);

    expect(result.email).toBe('alice@example.com');
  });

  it('should not override existing values', () => {
    const doc = { _id: 'doc1', name: 'Alice', email: 'alice@real.com' };
    const defaults = { email: 'default@example.com' };

    const result = applyDefaults(doc, defaults);

    expect(result.email).toBe('alice@real.com');
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('CompatReader integration', () => {
  it('should read multi-version evolution chain', () => {
    // V1: Basic document
    const v1Schema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      _seq: { type: 'int64', required: true },
      _op: { type: 'string', required: true },
      name: { type: 'string', required: false },
    };

    // V2: Added age
    const v2Schema: Record<string, SchemaField> = {
      ...v1Schema,
      age: { type: 'int32', required: false },
    };

    // V3: Added email, widened age
    const v3Schema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      _seq: { type: 'int64', required: true },
      _op: { type: 'string', required: true },
      name: { type: 'string', required: false },
      age: { type: 'int64', required: false }, // Widened
      email: { type: 'string', required: false },
    };

    // Serialize V1 documents
    const v1Docs = [{ _id: 'doc1', _seq: 1, _op: 'i' as const, name: 'Alice' }];
    const v1Serialized = serializeDocuments(v1Docs);

    // Read V1 data with V3 schema
    const reader = new CompatReader({
      targetSchema: v3Schema,
      sourceSchema: v1Schema,
      defaults: { email: 'unknown@example.com' },
    });

    const result = reader.read<Record<string, unknown>>(v1Serialized);

    expect(result.documents[0].name).toBe('Alice');
    expect(result.documents[0].age).toBeUndefined(); // Not in V1
    expect(result.documents[0].email).toBe('unknown@example.com'); // Default
  });

  it('should handle complex schema evolution with multiple changes', () => {
    const oldSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      _seq: { type: 'int64', required: true },
      _op: { type: 'string', required: true },
      firstName: { type: 'string', required: false },
      lastName: { type: 'string', required: false },
      age: { type: 'int32', required: false },
      country: { type: 'string', required: false }, // Will be removed
    };

    const newSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      _seq: { type: 'int64', required: true },
      _op: { type: 'string', required: true },
      fullName: { type: 'string', required: false }, // New field
      age: { type: 'int64', required: false }, // Widened
      email: { type: 'string', required: false }, // New field
      // country removed
    };

    const oldDocs = [
      { _id: 'user1', _seq: 1, _op: 'i' as const, firstName: 'Alice', lastName: 'Smith', age: 25, country: 'USA' },
    ];

    const serialized = serializeDocuments(oldDocs);

    const reader = new CompatReader({
      targetSchema: newSchema,
      sourceSchema: oldSchema,
      defaults: {
        fullName: (doc: Record<string, unknown>) => `${doc.firstName} ${doc.lastName}`,
        email: 'unknown@example.com',
      },
    });

    const result = reader.read<Record<string, unknown>>(serialized);

    expect(result.documents[0].fullName).toBe('Alice Smith');
    expect(result.documents[0].age).toBe(25);
    expect(result.documents[0].email).toBe('unknown@example.com');
    expect(result.documents[0]).not.toHaveProperty('country');
    expect(result.schemaMapping.removedColumns).toContain('country');
  });

  it('should work with real parquet round-trip', () => {
    // Serialize with old schema
    const oldDocs = createOldDocuments();
    const serialized = serializeDocuments(oldDocs);

    // Read with new schema
    const reader = new CompatReader({
      targetSchema: createNewSchema(),
      sourceSchema: createOldSchema(),
    });

    const result = reader.read<NewUserDocument>(serialized);

    // Verify round-trip preserves data
    expect(result.documents).toHaveLength(oldDocs.length);
    for (let i = 0; i < oldDocs.length; i++) {
      expect(result.documents[i]._id).toBe(oldDocs[i]._id);
      expect(result.documents[i].name).toBe(oldDocs[i].name);
      expect(result.documents[i].age).toBe(oldDocs[i].age);
    }
  });
});

// ============================================================================
// Edge Cases Tests
// ============================================================================

describe('CompatReader edge cases', () => {
  it('should handle empty row group', () => {
    const serialized = serializeDocuments([]);

    const reader = new CompatReader({
      targetSchema: createNewSchema(),
    });

    const result = reader.read<NewUserDocument>(serialized);

    expect(result.documents).toHaveLength(0);
    expect(result.isCompatible).toBe(true);
  });

  it('should handle null values in source data', () => {
    const docs = [
      { _id: 'user1', _seq: 1, _op: 'i' as const, name: null, age: null },
    ];

    const serialized = serializeDocuments(docs);

    const reader = new CompatReader({
      targetSchema: createNewSchema(),
    });

    const result = reader.read<NewUserDocument>(serialized);

    expect(result.documents[0].name).toBeUndefined();
    expect(result.documents[0].age).toBeUndefined();
  });

  it('should handle schema with no overlapping columns except system fields', () => {
    const sourceSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      _seq: { type: 'int64', required: true },
      _op: { type: 'string', required: true },
      oldField: { type: 'string', required: false },
    };

    const targetSchema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      _seq: { type: 'int64', required: true },
      _op: { type: 'string', required: true },
      newField: { type: 'string', required: false },
    };

    const docs = [{ _id: 'doc1', _seq: 1, _op: 'i' as const, oldField: 'value' }];
    const serialized = serializeDocuments(docs);

    const reader = new CompatReader({
      targetSchema,
      sourceSchema,
    });

    const result = reader.read<Record<string, unknown>>(serialized);

    expect(result.documents[0]._id).toBe('doc1');
    expect(result.documents[0]).not.toHaveProperty('oldField');
    expect(result.documents[0].newField).toBeUndefined();
  });

  it('should handle large batch efficiently', () => {
    const docs = Array.from({ length: 10000 }, (_, i) => ({
      _id: `user${i}`,
      _seq: i + 1,
      _op: 'i' as const,
      name: `User ${i}`,
      age: 20 + (i % 50),
    }));

    const serialized = serializeDocuments(docs);

    const reader = new CompatReader({
      targetSchema: createNewSchema(),
      sourceSchema: createOldSchema(),
    });

    const startTime = performance.now();
    const result = reader.read<NewUserDocument>(serialized);
    const elapsedTime = performance.now() - startTime;

    expect(result.documents).toHaveLength(10000);
    expect(elapsedTime).toBeLessThan(5000); // Should complete within 5 seconds
  });
});
