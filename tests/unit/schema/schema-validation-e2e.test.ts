/**
 * Schema Validation End-to-End Integration Tests
 *
 * Tests the complete schema validation and evolution workflow:
 * - Document validation against schema
 * - Schema version management
 * - Schema migration paths
 * - Type widening and compatibility
 * - Schema field analysis and promotion
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  SchemaVersionManager,
  createSchemaVersionManager,
  compareSchemaVersions,
  calculateMigrationPath,
  isTypeWideningAllowed,
  getCommonSupertype,
  type SchemaField,
  type SchemaVersion,
  type SchemaDiff,
} from '../../../src/schema/versioning.js';
import {
  FieldAnalyzer,
  createFieldAnalyzer,
  analyzeDocuments,
  suggestFieldPromotions,
  type FieldStats,
  type PromotionSuggestion,
} from '../../../src/schema/analyzer.js';
import {
  validateDocument,
  validateFieldName,
  ValidationError,
} from '../../../src/validation/index.js';
import { MemoryStorage } from '../../../src/storage/index.js';
import { resetDocumentCounter } from '../../utils/factories.js';

// ============================================================================
// Test Helpers
// ============================================================================

function createSampleSchema(): Record<string, SchemaField> {
  return {
    _id: { type: 'string', required: true },
    name: { type: 'string', required: true },
    email: { type: 'string', required: false },
    age: { type: 'int32', required: false },
    createdAt: { type: 'timestamp', required: false },
  };
}

function createEvolvingSampleSchema(): Record<string, SchemaField> {
  return {
    _id: { type: 'string', required: true },
    name: { type: 'string', required: true },
    email: { type: 'string', required: true }, // Now required
    age: { type: 'int64', required: false }, // Widened from int32
    createdAt: { type: 'timestamp', required: false },
    updatedAt: { type: 'timestamp', required: false }, // New field
    status: { type: 'string', required: false }, // New field
  };
}

// ============================================================================
// Document Validation Integration Tests
// ============================================================================

describe('Document Validation Integration', () => {
  beforeEach(() => {
    resetDocumentCounter();
  });

  it('should validate well-formed documents', () => {
    const validDoc = {
      _id: 'user-1',
      name: 'Alice',
      email: 'alice@example.com',
      age: 30,
      tags: ['premium', 'verified'],
    };

    expect(() => validateDocument(validDoc)).not.toThrow();
  });

  it('should validate nested documents', () => {
    const nestedDoc = {
      _id: 'order-1',
      customer: {
        name: 'Alice',
        address: {
          street: '123 Main St',
          city: 'New York',
          coordinates: {
            lat: 40.7128,
            lng: -74.0060,
          },
        },
      },
      items: [
        { productId: 'prod-1', quantity: 2, price: 29.99 },
        { productId: 'prod-2', quantity: 1, price: 49.99 },
      ],
      total: 109.97,
    };

    expect(() => validateDocument(nestedDoc)).not.toThrow();
  });

  it('should reject documents with invalid field names', () => {
    const docWithDollarField = {
      _id: 'bad-1',
      $invalid: 'value',
    };

    expect(() => validateDocument(docWithDollarField)).toThrow(ValidationError);
  });

  it('should reject documents exceeding max nesting depth', () => {
    // Create deeply nested document
    let deepDoc: Record<string, unknown> = { value: 'leaf' };
    for (let i = 0; i < 150; i++) {
      deepDoc = { nested: deepDoc };
    }
    deepDoc._id = 'deep-1';

    expect(() => validateDocument(deepDoc, { maxDepth: 100 })).toThrow(ValidationError);
  });

  it('should require _id when specified', () => {
    const docWithoutId = {
      name: 'Alice',
      email: 'alice@example.com',
    };

    expect(() => validateDocument(docWithoutId, { requireId: true })).toThrow(
      ValidationError
    );
  });

  it('should allow documents without _id by default', () => {
    const docWithoutId = {
      name: 'Alice',
      email: 'alice@example.com',
    };

    expect(() => validateDocument(docWithoutId)).not.toThrow();
  });

  it('should validate field names in nested objects', () => {
    const docWithNestedInvalidField = {
      _id: 'nested-bad-1',
      outer: {
        $badField: 'value',
      },
    };

    expect(() => validateDocument(docWithNestedInvalidField)).toThrow(ValidationError);
  });

  it('should validate field names in array elements', () => {
    const docWithInvalidArrayElement = {
      _id: 'array-bad-1',
      items: [
        { name: 'valid' },
        { $invalid: 'bad' },
      ],
    };

    expect(() => validateDocument(docWithInvalidArrayElement)).toThrow(ValidationError);
  });
});

// ============================================================================
// Schema Version Management Integration Tests
// ============================================================================

describe('Schema Version Management Integration', () => {
  let manager: SchemaVersionManager;

  beforeEach(() => {
    resetDocumentCounter();
    manager = createSchemaVersionManager({ collectionName: 'users' });
  });

  it('should create initial schema version', () => {
    const schema = createSampleSchema();
    const version = manager.createVersion(schema);

    expect(version.version).toBe(1);
    expect(version.schema).toEqual(schema);
    expect(version.hash).toBeDefined();
    expect(version.createdAt).toBeInstanceOf(Date);
    expect(version.parentVersion).toBeUndefined();
  });

  it('should track schema evolution with multiple versions', () => {
    // Version 1: Initial schema
    const v1Schema = createSampleSchema();
    const v1 = manager.createVersion(v1Schema);
    expect(v1.version).toBe(1);

    // Version 2: Add fields
    const v2Schema = {
      ...v1Schema,
      phone: { type: 'string', required: false } as SchemaField,
    };
    const v2 = manager.createVersion(v2Schema);
    expect(v2.version).toBe(2);
    expect(v2.parentVersion).toBe(1);

    // Version 3: Widen type
    const v3Schema = {
      ...v2Schema,
      age: { type: 'int64', required: false } as SchemaField,
    };
    const v3 = manager.createVersion(v3Schema);
    expect(v3.version).toBe(3);
    expect(v3.parentVersion).toBe(2);

    expect(manager.getVersionCount()).toBe(3);
  });

  it('should deduplicate identical schemas', () => {
    const schema = createSampleSchema();

    const v1 = manager.createVersion(schema);
    const v2 = manager.createVersion(schema);

    // Same schema should return same version
    expect(v1.version).toBe(v2.version);
    expect(v1.hash).toBe(v2.hash);
    expect(manager.getVersionCount()).toBe(1);
  });

  it('should force create version for identical schemas when requested', () => {
    const schema = createSampleSchema();

    const v1 = manager.createVersion(schema);
    const v2 = manager.createVersion(schema, { force: true });

    expect(v2.version).toBe(2);
    expect(v2.hash).toBe(v1.hash);
    expect(manager.getVersionCount()).toBe(2);
  });

  it('should compare schema versions correctly', () => {
    const v1Schema = createSampleSchema();
    manager.createVersion(v1Schema);

    const v2Schema = createEvolvingSampleSchema();
    manager.createVersion(v2Schema);

    const diff = manager.compareVersions(1, 2);

    expect(diff.addedFields).toContain('updatedAt');
    expect(diff.addedFields).toContain('status');
    expect(diff.removedFields).toHaveLength(0);
    expect(diff.changedFields.some((c) => c.path === 'email')).toBe(true);
    expect(diff.changedFields.some((c) => c.path === 'age')).toBe(true);
  });

  it('should detect backwards incompatible changes', () => {
    const v1Schema = createSampleSchema();
    manager.createVersion(v1Schema);

    // Remove a field (breaking change)
    const v2Schema = { ...v1Schema };
    delete v2Schema.email;
    manager.createVersion(v2Schema);

    const diff = manager.compareVersions(1, 2);

    expect(diff.removedFields).toContain('email');
    expect(diff.isBackwardsCompatible).toBe(false);
  });

  it('should get version history with filtering', () => {
    const schema = createSampleSchema();

    // Create multiple versions with delays
    for (let i = 0; i < 5; i++) {
      const modifiedSchema = {
        ...schema,
        [`field${i}`]: { type: 'string', required: false } as SchemaField,
      };
      manager.createVersion(modifiedSchema);
    }

    // Get all versions
    const allVersions = manager.getVersionHistory();
    expect(allVersions.length).toBe(5);

    // Get latest 3 versions
    const latestVersions = manager.getVersionHistory({ reverse: true, limit: 3 });
    expect(latestVersions.length).toBe(3);
    expect(latestVersions[0]!.version).toBe(5);

    // Get oldest 2 versions
    const oldestVersions = manager.getVersionHistory({ limit: 2 });
    expect(oldestVersions.length).toBe(2);
    expect(oldestVersions[0]!.version).toBe(1);
  });

  it('should serialize and deserialize schema manager', () => {
    const v1Schema = createSampleSchema();
    manager.createVersion(v1Schema);

    const v2Schema = createEvolvingSampleSchema();
    manager.createVersion(v2Schema);

    // Serialize
    const json = manager.toJSON();
    expect(typeof json).toBe('string');

    // Deserialize
    const restored = SchemaVersionManager.fromJSON(json);

    expect(restored.getCollectionName()).toBe('users');
    expect(restored.getVersionCount()).toBe(2);
    expect(restored.getCurrentVersion()?.version).toBe(2);
  });

  it('should export Parquet metadata', () => {
    const schema = createSampleSchema();
    manager.createVersion(schema);

    const metadata = manager.toParquetMetadata();

    expect(metadata['mongolake.collection']).toBe('users');
    expect(metadata['mongolake.schema.version']).toBe('1');
    expect(metadata['mongolake.schema.hash']).toBeDefined();
    expect(metadata['mongolake.schema.created_at']).toBeDefined();
  });
});

// ============================================================================
// Migration Path Calculation Tests
// ============================================================================

describe('Schema Migration Path Integration', () => {
  let manager: SchemaVersionManager;

  beforeEach(() => {
    resetDocumentCounter();
    manager = createSchemaVersionManager({ collectionName: 'products' });

    // Create a series of schema versions
    // V1: Basic product
    manager.createVersion({
      _id: { type: 'string', required: true },
      name: { type: 'string', required: true },
      price: { type: 'double', required: true },
    });

    // V2: Add category
    manager.createVersion({
      _id: { type: 'string', required: true },
      name: { type: 'string', required: true },
      price: { type: 'double', required: true },
      category: { type: 'string', required: false },
    });

    // V3: Add inventory
    manager.createVersion({
      _id: { type: 'string', required: true },
      name: { type: 'string', required: true },
      price: { type: 'double', required: true },
      category: { type: 'string', required: false },
      inventory: { type: 'int32', required: false },
    });

    // V4: Widen inventory to int64
    manager.createVersion({
      _id: { type: 'string', required: true },
      name: { type: 'string', required: true },
      price: { type: 'double', required: true },
      category: { type: 'string', required: false },
      inventory: { type: 'int64', required: false },
    });
  });

  it('should calculate forward migration path', () => {
    const path = manager.getMigrationPath(1, 4);

    expect(path.length).toBe(3);
    expect(path[0]!.fromVersion).toBe(1);
    expect(path[0]!.toVersion).toBe(2);
    expect(path[1]!.fromVersion).toBe(2);
    expect(path[1]!.toVersion).toBe(3);
    expect(path[2]!.fromVersion).toBe(3);
    expect(path[2]!.toVersion).toBe(4);
  });

  it('should calculate backward migration path', () => {
    const path = manager.getMigrationPath(4, 1);

    expect(path.length).toBe(3);
    expect(path[0]!.fromVersion).toBe(4);
    expect(path[0]!.toVersion).toBe(3);
    expect(path[2]!.toVersion).toBe(1);
  });

  it('should return empty path for same version', () => {
    const path = manager.getMigrationPath(2, 2);
    expect(path.length).toBe(0);
  });

  it('should include diff for each migration step', () => {
    const path = manager.getMigrationPath(1, 3);

    // Step 1: Add category
    expect(path[0]!.diff.addedFields).toContain('category');
    expect(path[0]!.diff.isBackwardsCompatible).toBe(true);

    // Step 2: Add inventory
    expect(path[1]!.diff.addedFields).toContain('inventory');
    expect(path[1]!.diff.isBackwardsCompatible).toBe(true);
  });

  it('should detect type widening in migration', () => {
    const path = manager.getMigrationPath(3, 4);

    expect(path.length).toBe(1);
    expect(path[0]!.diff.changedFields.some((c) => c.path === 'inventory')).toBe(true);

    const inventoryChange = path[0]!.diff.changedFields.find(
      (c) => c.path === 'inventory'
    );
    expect(inventoryChange?.oldType).toBe('int32');
    expect(inventoryChange?.newType).toBe('int64');
  });
});

// ============================================================================
// Type Widening Rules Tests
// ============================================================================

describe('Type Widening Rules Integration', () => {
  it('should allow valid type widenings', () => {
    // Integer widenings
    expect(isTypeWideningAllowed('int32', 'int64')).toBe(true);
    expect(isTypeWideningAllowed('int32', 'double')).toBe(true);
    expect(isTypeWideningAllowed('int64', 'double')).toBe(true);

    // Float widenings
    expect(isTypeWideningAllowed('float', 'double')).toBe(true);

    // To variant (any type can widen to variant)
    expect(isTypeWideningAllowed('string', 'variant')).toBe(true);
    expect(isTypeWideningAllowed('boolean', 'variant')).toBe(true);
    expect(isTypeWideningAllowed('int32', 'variant')).toBe(true);

    // Date/time widenings
    expect(isTypeWideningAllowed('date', 'timestamp')).toBe(true);
  });

  it('should reject invalid type narrowings', () => {
    // Narrowing is not allowed
    expect(isTypeWideningAllowed('int64', 'int32')).toBe(false);
    expect(isTypeWideningAllowed('double', 'float')).toBe(false);
    expect(isTypeWideningAllowed('double', 'int32')).toBe(false);

    // Incompatible types
    expect(isTypeWideningAllowed('string', 'int32')).toBe(false);
    expect(isTypeWideningAllowed('boolean', 'string')).toBe(false);
  });

  it('should allow same type', () => {
    expect(isTypeWideningAllowed('string', 'string')).toBe(true);
    expect(isTypeWideningAllowed('int32', 'int32')).toBe(true);
    expect(isTypeWideningAllowed('variant', 'variant')).toBe(true);
  });

  it('should find common supertype', () => {
    // Same types
    expect(getCommonSupertype('string', 'string')).toBe('string');

    // One widens to other
    expect(getCommonSupertype('int32', 'int64')).toBe('int64');
    expect(getCommonSupertype('int64', 'int32')).toBe('int64');

    // Both widen to common
    expect(getCommonSupertype('int32', 'float')).toBe('variant'); // No common numeric supertype besides variant

    // Incompatible fall back to variant
    expect(getCommonSupertype('string', 'boolean')).toBe('variant');
  });
});

// ============================================================================
// Field Analysis Integration Tests
// ============================================================================

describe('Field Analysis Integration', () => {
  beforeEach(() => {
    resetDocumentCounter();
  });

  it('should analyze documents and detect field types', () => {
    const documents = [
      { _id: '1', name: 'Alice', age: 30, active: true },
      { _id: '2', name: 'Bob', age: 25, active: false },
      { _id: '3', name: 'Charlie', age: 35, active: true },
    ];

    const analyzer = createFieldAnalyzer();
    analyzer.analyzeDocuments(documents);

    const stats = analyzer.getFieldStats();

    expect(stats.get('_id')?.types.has('string')).toBe(true);
    expect(stats.get('name')?.types.has('string')).toBe(true);
    expect(stats.get('age')?.types.has('number')).toBe(true);
    expect(stats.get('active')?.types.has('boolean')).toBe(true);
  });

  it('should detect mixed types in fields', () => {
    const documents = [
      { _id: '1', value: 100 },
      { _id: '2', value: 'hundred' },
      { _id: '3', value: 300 },
    ];

    const analyzer = createFieldAnalyzer();
    analyzer.analyzeDocuments(documents);

    const stats = analyzer.getFieldStats();
    const valueStats = stats.get('value');

    expect(valueStats?.types.has('number')).toBe(true);
    expect(valueStats?.types.has('string')).toBe(true);
  });

  it('should track null counts', () => {
    const documents = [
      { _id: '1', name: 'Alice', optional: 'value' },
      { _id: '2', name: 'Bob', optional: null },
      { _id: '3', name: 'Charlie' }, // optional missing
    ];

    const analyzer = createFieldAnalyzer();
    analyzer.analyzeDocuments(documents);

    const stats = analyzer.getFieldStats();

    // Null count is available via types.get('null')
    expect(stats.get('name')?.types.get('null') ?? 0).toBe(0);
    expect(stats.get('optional')?.types.get('null') ?? 0).toBe(1);
  });

  it('should analyze nested fields', () => {
    const documents = [
      { _id: '1', user: { name: 'Alice', profile: { score: 100 } } },
      { _id: '2', user: { name: 'Bob', profile: { score: 85 } } },
    ];

    const analyzer = createFieldAnalyzer();
    analyzer.analyzeDocuments(documents);

    const stats = analyzer.getFieldStats();

    expect(stats.has('user.name')).toBe(true);
    expect(stats.has('user.profile.score')).toBe(true);
  });

  it('should calculate field presence frequency', () => {
    const documents = [
      { _id: '1', status: 'active' },
      { _id: '2', status: 'inactive' },
      { _id: '3', status: 'active' },
      { _id: '4', status: 'pending' },
      { _id: '5', status: 'active' },
    ];

    const analyzer = createFieldAnalyzer();
    analyzer.analyzeDocuments(documents);

    const stats = analyzer.getFieldStats();
    const statusStats = stats.get('status');

    // All 5 documents have the status field
    expect(statusStats?.count).toBe(5);
    expect(statusStats?.frequency).toBe(1);
  });

  it('should suggest field promotions', () => {
    const documents = [
      { _id: '1', score: 100, category: 'A' },
      { _id: '2', score: 85, category: 'B' },
      { _id: '3', score: 92, category: 'A' },
      { _id: '4', score: 78, category: 'C' },
      { _id: '5', score: 88, category: 'A' },
    ];

    const analyzer = createFieldAnalyzer();
    analyzer.analyzeDocuments(documents);

    const suggestions = analyzer.suggestPromotions();

    // Should suggest promotion for consistent numeric field
    const scoreSuggestion = suggestions.find((s) => s.path === 'score');
    expect(scoreSuggestion).toBeDefined();
  });

  it('should serialize and deserialize analyzer state', () => {
    const documents = [
      { _id: '1', name: 'Alice', score: 100 },
      { _id: '2', name: 'Bob', score: 85 },
    ];

    const analyzer = createFieldAnalyzer();
    analyzer.analyzeDocuments(documents);

    // Serialize using toJSON
    const serialized = analyzer.toJSON();
    expect(serialized.documentCount).toBe(2);
    expect(Object.keys(serialized.fields).length).toBeGreaterThan(0);

    // Deserialize using fromJSON
    const restored = FieldAnalyzer.fromJSON(serialized);

    expect(restored.getDocumentCount()).toBe(2);
    expect(restored.getFieldStats().has('name')).toBe(true);
    expect(restored.getFieldStats().has('score')).toBe(true);
  });
});

// ============================================================================
// Version Pruning Tests
// ============================================================================

describe('Schema Version Pruning', () => {
  let manager: SchemaVersionManager;

  beforeEach(() => {
    manager = createSchemaVersionManager({ collectionName: 'test' });

    // Create 10 versions
    for (let i = 0; i < 10; i++) {
      manager.createVersion({
        _id: { type: 'string', required: true },
        [`field${i}`]: { type: 'string', required: false },
      });
    }
  });

  it('should prune versions by count', () => {
    expect(manager.getVersionCount()).toBe(10);

    manager.pruneVersions({ keepCount: 5 });

    expect(manager.getVersionCount()).toBe(5);

    // Current version should be preserved
    expect(manager.getCurrentVersion()?.version).toBe(10);
  });

  it('should always keep current version', () => {
    manager.pruneVersions({ keepCount: 1 });

    expect(manager.getVersionCount()).toBe(1);
    expect(manager.getCurrentVersion()?.version).toBe(10);
  });
});

// ============================================================================
// Complete Schema Evolution Workflow Test
// ============================================================================

describe('Complete Schema Evolution Workflow', () => {
  it('should handle full schema evolution lifecycle', () => {
    // 1. Create initial schema manager
    const manager = createSchemaVersionManager({ collectionName: 'orders' });

    // 2. Define initial schema (V1)
    const v1Schema: Record<string, SchemaField> = {
      _id: { type: 'string', required: true },
      customerId: { type: 'string', required: true },
      total: { type: 'double', required: true },
      status: { type: 'string', required: true },
    };
    const v1 = manager.createVersion(v1Schema);
    expect(v1.version).toBe(1);

    // 3. Analyze sample documents to understand actual usage
    const analyzer = createFieldAnalyzer();
    const sampleDocs = [
      { _id: 'o1', customerId: 'c1', total: 99.99, status: 'pending', items: 3 },
      { _id: 'o2', customerId: 'c2', total: 149.99, status: 'shipped', items: 5 },
      { _id: 'o3', customerId: 'c1', total: 49.99, status: 'delivered', items: 1 },
    ];

    for (const doc of sampleDocs) {
      validateDocument(doc);
    }
    analyzer.analyzeDocuments(sampleDocs);

    // 4. Get suggestions for schema improvements
    const suggestions = analyzer.suggestPromotions();
    const itemsSuggestion = suggestions.find((s) => s.path === 'items');
    expect(itemsSuggestion).toBeDefined();

    // 5. Evolve schema based on analysis (V2)
    const v2Schema: Record<string, SchemaField> = {
      ...v1Schema,
      items: { type: 'int32', required: false },
      createdAt: { type: 'timestamp', required: false },
    };
    const v2 = manager.createVersion(v2Schema);
    expect(v2.version).toBe(2);

    // 6. Compare versions to understand changes
    const diff12 = manager.compareVersions(1, 2);
    expect(diff12.addedFields).toContain('items');
    expect(diff12.addedFields).toContain('createdAt');
    expect(diff12.isBackwardsCompatible).toBe(true);

    // 7. Further evolution with type widening (V3)
    const v3Schema: Record<string, SchemaField> = {
      ...v2Schema,
      total: { type: 'variant', required: true }, // Allow flexible total (could be string for display)
      items: { type: 'int64', required: false }, // Widen for large orders
    };
    const v3 = manager.createVersion(v3Schema);
    expect(v3.version).toBe(3);

    // 8. Calculate complete migration path
    const fullPath = manager.getMigrationPath(1, 3);
    expect(fullPath.length).toBe(2);

    // 9. Verify type widening rules were respected
    const diff23 = manager.compareVersions(2, 3);
    const itemsChange = diff23.changedFields.find((c) => c.path === 'items');
    expect(itemsChange).toBeDefined();
    expect(isTypeWideningAllowed(itemsChange!.oldType!, itemsChange!.newType!)).toBe(true);

    // 10. Export for Parquet integration
    const parquetMeta = manager.toParquetMetadata();
    expect(parquetMeta['mongolake.schema.version']).toBe('3');

    // 11. Serialize for persistence
    const serialized = manager.toJSON();
    const restored = SchemaVersionManager.fromJSON(serialized);
    expect(restored.getVersionCount()).toBe(3);
    expect(restored.getCurrentVersion()?.version).toBe(3);
  });
});
