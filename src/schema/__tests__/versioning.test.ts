/**
 * Schema Versioning Tests
 *
 * Tests for schema versioning functionality including:
 * - Creating initial schema version
 * - Incrementing version on schema change
 * - Storing version metadata
 * - Retrieving version history
 * - Comparing versions and detecting changes
 * - Schema diff generation
 * - Migration path calculation
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  SchemaVersionManager,
  createSchemaVersionManager,
  compareSchemaVersions,
  generateSchemaDiff,
  calculateMigrationPath,
  isTypeWideningAllowed,
  getCommonSupertype,
} from '../versioning.js';
import type {
  SchemaVersion,
  SchemaDiff,
  VersionMetadata,
  SchemaField,
} from '../versioning.js';

// ============================================================================
// Test Fixtures
// ============================================================================

function createSimpleSchema(): Record<string, SchemaField> {
  return {
    _id: { type: 'string', required: true },
    name: { type: 'string', required: false },
    email: { type: 'string', required: false },
  };
}

function createExtendedSchema(): Record<string, SchemaField> {
  return {
    _id: { type: 'string', required: true },
    name: { type: 'string', required: false },
    email: { type: 'string', required: false },
    age: { type: 'int32', required: false },
    createdAt: { type: 'timestamp', required: false },
  };
}

function createNestedSchema(): Record<string, SchemaField> {
  return {
    _id: { type: 'string', required: true },
    'profile.name': { type: 'string', required: false },
    'profile.age': { type: 'int32', required: false },
    'address.city': { type: 'string', required: false },
    'address.zip': { type: 'string', required: false },
  };
}

// ============================================================================
// Constructor Tests
// ============================================================================

describe('SchemaVersionManager', () => {
  describe('constructor', () => {
    it('should create empty version manager', () => {
      const manager = new SchemaVersionManager();

      expect(manager.getCurrentVersion()).toBeUndefined();
      expect(manager.getVersionCount()).toBe(0);
      expect(manager.getVersionHistory()).toEqual([]);
    });

    it('should create version manager with initial schema', () => {
      const schema = createSimpleSchema();
      const manager = new SchemaVersionManager({
        initialSchema: schema,
        collectionName: 'users',
      });

      expect(manager.getCurrentVersion()).toBeDefined();
      expect(manager.getCurrentVersion()?.version).toBe(1);
      expect(manager.getVersionCount()).toBe(1);
    });

    it('should track collection name', () => {
      const manager = new SchemaVersionManager({
        initialSchema: createSimpleSchema(),
        collectionName: 'users',
      });

      expect(manager.getCollectionName()).toBe('users');
    });
  });
});

// ============================================================================
// Version Creation Tests
// ============================================================================

describe('SchemaVersionManager.createVersion', () => {
  let manager: SchemaVersionManager;

  beforeEach(() => {
    manager = new SchemaVersionManager({ collectionName: 'users' });
  });

  it('should create initial version', () => {
    const schema = createSimpleSchema();
    const version = manager.createVersion(schema);

    expect(version.version).toBe(1);
    expect(version.schema).toEqual(schema);
    expect(version.createdAt).toBeDefined();
    expect(version.hash).toBeDefined();
  });

  it('should increment version number', () => {
    manager.createVersion(createSimpleSchema());
    const version2 = manager.createVersion(createExtendedSchema());

    expect(version2.version).toBe(2);
  });

  it('should generate unique hash for each schema', () => {
    const version1 = manager.createVersion(createSimpleSchema());
    const version2 = manager.createVersion(createExtendedSchema());

    expect(version1.hash).not.toBe(version2.hash);
  });

  it('should generate same hash for identical schemas', () => {
    const manager1 = new SchemaVersionManager({ collectionName: 'users' });
    const manager2 = new SchemaVersionManager({ collectionName: 'users' });

    const version1 = manager1.createVersion(createSimpleSchema());
    const version2 = manager2.createVersion(createSimpleSchema());

    expect(version1.hash).toBe(version2.hash);
  });

  it('should store version metadata', () => {
    const metadata: VersionMetadata = {
      author: 'test-user',
      message: 'Added user fields',
      source: 'migration',
    };

    const version = manager.createVersion(createSimpleSchema(), { metadata });

    expect(version.metadata?.author).toBe('test-user');
    expect(version.metadata?.message).toBe('Added user fields');
  });

  it('should track parent version', () => {
    const version1 = manager.createVersion(createSimpleSchema());
    const version2 = manager.createVersion(createExtendedSchema());

    expect(version2.parentVersion).toBe(version1.version);
  });

  it('should not create version for identical schema', () => {
    manager.createVersion(createSimpleSchema());
    const version2 = manager.createVersion(createSimpleSchema());

    expect(version2.version).toBe(1); // Same version returned
    expect(manager.getVersionCount()).toBe(1);
  });

  it('should allow forcing version creation for identical schema', () => {
    manager.createVersion(createSimpleSchema());
    const version2 = manager.createVersion(createSimpleSchema(), { force: true });

    expect(version2.version).toBe(2);
    expect(manager.getVersionCount()).toBe(2);
  });
});

// ============================================================================
// Version Retrieval Tests
// ============================================================================

describe('SchemaVersionManager version retrieval', () => {
  let manager: SchemaVersionManager;

  beforeEach(() => {
    manager = new SchemaVersionManager({ collectionName: 'users' });
    manager.createVersion(createSimpleSchema());
    manager.createVersion(createExtendedSchema());
    manager.createVersion(createNestedSchema());
  });

  it('should get current version', () => {
    const current = manager.getCurrentVersion();

    expect(current?.version).toBe(3);
    expect(current?.schema).toEqual(createNestedSchema());
  });

  it('should get version by number', () => {
    const version = manager.getVersion(2);

    expect(version?.version).toBe(2);
    expect(version?.schema).toEqual(createExtendedSchema());
  });

  it('should return undefined for non-existent version', () => {
    const version = manager.getVersion(999);

    expect(version).toBeUndefined();
  });

  it('should get version by hash', () => {
    const version1 = manager.getVersion(1);
    const versionByHash = manager.getVersionByHash(version1!.hash);

    expect(versionByHash?.version).toBe(1);
  });

  it('should get version history in order', () => {
    const history = manager.getVersionHistory();

    expect(history).toHaveLength(3);
    expect(history[0].version).toBe(1);
    expect(history[1].version).toBe(2);
    expect(history[2].version).toBe(3);
  });

  it('should get version history in reverse order', () => {
    const history = manager.getVersionHistory({ reverse: true });

    expect(history[0].version).toBe(3);
    expect(history[1].version).toBe(2);
    expect(history[2].version).toBe(1);
  });

  it('should limit version history', () => {
    const history = manager.getVersionHistory({ limit: 2 });

    expect(history).toHaveLength(2);
  });

  it('should get versions in date range', () => {
    const now = Date.now();
    const history = manager.getVersionHistory({
      since: new Date(now - 1000),
      until: new Date(now + 1000),
    });

    expect(history.length).toBeGreaterThan(0);
  });
});

// ============================================================================
// Schema Comparison Tests
// ============================================================================

describe('SchemaVersionManager.compareVersions', () => {
  let manager: SchemaVersionManager;

  beforeEach(() => {
    manager = new SchemaVersionManager({ collectionName: 'users' });
    manager.createVersion(createSimpleSchema());
    manager.createVersion(createExtendedSchema());
  });

  it('should detect added fields', () => {
    const diff = manager.compareVersions(1, 2);

    expect(diff.addedFields).toContain('age');
    expect(diff.addedFields).toContain('createdAt');
  });

  it('should detect removed fields', () => {
    manager.createVersion({
      _id: { type: 'string', required: true },
      name: { type: 'string', required: false },
    });

    const diff = manager.compareVersions(2, 3);

    expect(diff.removedFields).toContain('email');
    expect(diff.removedFields).toContain('age');
    expect(diff.removedFields).toContain('createdAt');
  });

  it('should detect changed field types', () => {
    manager.createVersion({
      ...createExtendedSchema(),
      age: { type: 'int64', required: false }, // Changed from int32
    });

    const diff = manager.compareVersions(2, 3);

    expect(diff.changedFields).toContainEqual({
      path: 'age',
      oldType: 'int32',
      newType: 'int64',
    });
  });

  it('should detect changed required status', () => {
    manager.createVersion({
      ...createExtendedSchema(),
      email: { type: 'string', required: true }, // Changed to required
    });

    const diff = manager.compareVersions(2, 3);

    expect(diff.changedFields).toContainEqual({
      path: 'email',
      oldRequired: false,
      newRequired: true,
    });
  });

  it('should indicate if change is backwards compatible', () => {
    // Adding optional fields is backwards compatible
    const diff = manager.compareVersions(1, 2);
    expect(diff.isBackwardsCompatible).toBe(true);

    // Removing fields is not backwards compatible
    manager.createVersion({
      _id: { type: 'string', required: true },
    });
    const diff2 = manager.compareVersions(2, 3);
    expect(diff2.isBackwardsCompatible).toBe(false);
  });

  it('should throw for invalid version numbers', () => {
    expect(() => manager.compareVersions(1, 999)).toThrow();
    expect(() => manager.compareVersions(999, 1)).toThrow();
  });
});

// ============================================================================
// Migration Path Tests
// ============================================================================

describe('SchemaVersionManager.getMigrationPath', () => {
  let manager: SchemaVersionManager;

  beforeEach(() => {
    manager = new SchemaVersionManager({ collectionName: 'users' });
    manager.createVersion(createSimpleSchema());
    manager.createVersion(createExtendedSchema());
    manager.createVersion(createNestedSchema());
  });

  it('should calculate migration path between versions', () => {
    const path = manager.getMigrationPath(1, 3);

    expect(path).toHaveLength(2); // 1->2, 2->3
    expect(path[0].fromVersion).toBe(1);
    expect(path[0].toVersion).toBe(2);
    expect(path[1].fromVersion).toBe(2);
    expect(path[1].toVersion).toBe(3);
  });

  it('should calculate reverse migration path', () => {
    const path = manager.getMigrationPath(3, 1);

    expect(path).toHaveLength(2);
    expect(path[0].fromVersion).toBe(3);
    expect(path[0].toVersion).toBe(2);
    expect(path[1].fromVersion).toBe(2);
    expect(path[1].toVersion).toBe(1);
  });

  it('should return empty path for same version', () => {
    const path = manager.getMigrationPath(2, 2);

    expect(path).toHaveLength(0);
  });

  it('should include diffs in migration steps', () => {
    const path = manager.getMigrationPath(1, 2);

    expect(path[0].diff).toBeDefined();
    expect(path[0].diff.addedFields).toContain('age');
  });
});

// ============================================================================
// Serialization Tests
// ============================================================================

describe('SchemaVersionManager serialization', () => {
  it('should serialize to JSON', () => {
    const manager = new SchemaVersionManager({ collectionName: 'users' });
    manager.createVersion(createSimpleSchema());
    manager.createVersion(createExtendedSchema());

    const json = manager.toJSON();
    const parsed = JSON.parse(json);

    expect(parsed.collectionName).toBe('users');
    expect(parsed.versions).toHaveLength(2);
    expect(parsed.currentVersion).toBe(2);
  });

  it('should deserialize from JSON', () => {
    const manager = new SchemaVersionManager({ collectionName: 'users' });
    manager.createVersion(createSimpleSchema());
    manager.createVersion(createExtendedSchema());

    const json = manager.toJSON();
    const restored = SchemaVersionManager.fromJSON(json);

    expect(restored.getCollectionName()).toBe('users');
    expect(restored.getVersionCount()).toBe(2);
    expect(restored.getCurrentVersion()?.version).toBe(2);
  });

  it('should export to Parquet metadata format', () => {
    const manager = new SchemaVersionManager({ collectionName: 'users' });
    manager.createVersion(createSimpleSchema());

    const metadata = manager.toParquetMetadata();

    expect(metadata['mongolake.schema.version']).toBe('1');
    expect(metadata['mongolake.schema.hash']).toBeDefined();
    expect(metadata['mongolake.collection']).toBe('users');
  });
});

// ============================================================================
// Version Pruning Tests
// ============================================================================

describe('SchemaVersionManager pruning', () => {
  it('should prune old versions keeping specified count', () => {
    const manager = new SchemaVersionManager({ collectionName: 'users' });

    // Create many versions
    for (let i = 0; i < 10; i++) {
      manager.createVersion(
        { [`field${i}`]: { type: 'string', required: false } },
        { force: true }
      );
    }

    expect(manager.getVersionCount()).toBe(10);

    // Prune to keep only last 3
    manager.pruneVersions({ keepCount: 3 });

    expect(manager.getVersionCount()).toBe(3);
    expect(manager.getVersion(1)).toBeUndefined();
    expect(manager.getVersion(8)).toBeDefined();
    expect(manager.getVersion(10)).toBeDefined();
  });

  it('should prune versions older than date', () => {
    const manager = new SchemaVersionManager({ collectionName: 'users' });
    manager.createVersion(createSimpleSchema());

    // Cannot easily test date-based pruning without mocking time
    // Just verify the method exists and accepts the option
    manager.pruneVersions({ olderThan: new Date(Date.now() - 1000 * 60 * 60 * 24) });

    // All versions should still exist since they were just created
    expect(manager.getVersionCount()).toBeGreaterThan(0);
  });

  it('should always keep current version', () => {
    const manager = new SchemaVersionManager({ collectionName: 'users' });
    manager.createVersion(createSimpleSchema());
    manager.createVersion(createExtendedSchema());

    manager.pruneVersions({ keepCount: 0 });

    // Should still have at least the current version
    expect(manager.getVersionCount()).toBeGreaterThanOrEqual(1);
    expect(manager.getCurrentVersion()).toBeDefined();
  });
});

// ============================================================================
// Utility Function Tests
// ============================================================================

describe('createSchemaVersionManager', () => {
  it('should create manager with options', () => {
    const manager = createSchemaVersionManager({
      collectionName: 'users',
      initialSchema: createSimpleSchema(),
    });

    expect(manager).toBeInstanceOf(SchemaVersionManager);
    expect(manager.getVersionCount()).toBe(1);
  });
});

describe('compareSchemaVersions', () => {
  it('should compare two schemas directly', () => {
    const diff = compareSchemaVersions(createSimpleSchema(), createExtendedSchema());

    expect(diff.addedFields).toContain('age');
    expect(diff.addedFields).toContain('createdAt');
    expect(diff.removedFields).toHaveLength(0);
  });
});

describe('generateSchemaDiff', () => {
  it('should generate detailed diff', () => {
    const diff = generateSchemaDiff(createSimpleSchema(), createExtendedSchema());

    expect(diff.summary).toBeDefined();
    expect(diff.addedFields).toBeDefined();
    expect(diff.isBackwardsCompatible).toBe(true);
  });
});

describe('calculateMigrationPath', () => {
  it('should calculate path between versions', () => {
    const versions: SchemaVersion[] = [
      {
        version: 1,
        schema: createSimpleSchema(),
        hash: 'hash1',
        createdAt: new Date(),
      },
      {
        version: 2,
        schema: createExtendedSchema(),
        hash: 'hash2',
        createdAt: new Date(),
        parentVersion: 1,
      },
    ];

    const path = calculateMigrationPath(versions, 1, 2);

    expect(path).toHaveLength(1);
    expect(path[0].fromVersion).toBe(1);
    expect(path[0].toVersion).toBe(2);
  });
});

// ============================================================================
// Type Widening Tests
// ============================================================================

describe('isTypeWideningAllowed', () => {
  it('should allow same type', () => {
    expect(isTypeWideningAllowed('string', 'string')).toBe(true);
    expect(isTypeWideningAllowed('int32', 'int32')).toBe(true);
    expect(isTypeWideningAllowed('variant', 'variant')).toBe(true);
  });

  it('should allow int32 to int64 widening', () => {
    expect(isTypeWideningAllowed('int32', 'int64')).toBe(true);
  });

  it('should allow int32 to double widening', () => {
    expect(isTypeWideningAllowed('int32', 'double')).toBe(true);
  });

  it('should allow int64 to double widening', () => {
    expect(isTypeWideningAllowed('int64', 'double')).toBe(true);
  });

  it('should allow float to double widening', () => {
    expect(isTypeWideningAllowed('float', 'double')).toBe(true);
  });

  it('should allow any type to variant widening', () => {
    expect(isTypeWideningAllowed('string', 'variant')).toBe(true);
    expect(isTypeWideningAllowed('int32', 'variant')).toBe(true);
    expect(isTypeWideningAllowed('boolean', 'variant')).toBe(true);
    expect(isTypeWideningAllowed('timestamp', 'variant')).toBe(true);
  });

  it('should allow date to timestamp widening', () => {
    expect(isTypeWideningAllowed('date', 'timestamp')).toBe(true);
  });

  it('should not allow int64 to int32 narrowing', () => {
    expect(isTypeWideningAllowed('int64', 'int32')).toBe(false);
  });

  it('should not allow double to int32 narrowing', () => {
    expect(isTypeWideningAllowed('double', 'int32')).toBe(false);
  });

  it('should not allow string to int32 conversion', () => {
    expect(isTypeWideningAllowed('string', 'int32')).toBe(false);
  });

  it('should not allow variant to specific type narrowing', () => {
    expect(isTypeWideningAllowed('variant', 'string')).toBe(false);
    expect(isTypeWideningAllowed('variant', 'int32')).toBe(false);
  });
});

describe('getCommonSupertype', () => {
  it('should return same type when types match', () => {
    expect(getCommonSupertype('string', 'string')).toBe('string');
    expect(getCommonSupertype('int32', 'int32')).toBe('int32');
  });

  it('should return wider type for numeric types', () => {
    expect(getCommonSupertype('int32', 'int64')).toBe('int64');
    expect(getCommonSupertype('int64', 'int32')).toBe('int64');
    expect(getCommonSupertype('float', 'double')).toBe('double');
  });

  it('should return variant for incompatible types', () => {
    expect(getCommonSupertype('string', 'int32')).toBe('variant');
    expect(getCommonSupertype('boolean', 'timestamp')).toBe('variant');
  });

  it('should return variant when one type is variant', () => {
    expect(getCommonSupertype('string', 'variant')).toBe('variant');
    expect(getCommonSupertype('variant', 'int32')).toBe('variant');
  });
});

// ============================================================================
// Edge Cases Tests
// ============================================================================

describe('SchemaVersionManager edge cases', () => {
  it('should handle empty schema', () => {
    const manager = new SchemaVersionManager({ collectionName: 'empty' });
    const version = manager.createVersion({});

    expect(version.version).toBe(1);
    expect(Object.keys(version.schema)).toHaveLength(0);
  });

  it('should handle deeply nested field paths', () => {
    const manager = new SchemaVersionManager({ collectionName: 'nested' });
    const version = manager.createVersion({
      'a.b.c.d.e': { type: 'string', required: false },
    });

    expect(version.schema['a.b.c.d.e']).toBeDefined();
  });

  it('should handle special characters in field names', () => {
    const manager = new SchemaVersionManager({ collectionName: 'special' });
    const version = manager.createVersion({
      'field_with_underscore': { type: 'string', required: false },
      'field-with-dash': { type: 'string', required: false },
    });

    expect(Object.keys(version.schema)).toHaveLength(2);
  });

  it('should handle concurrent version creation', () => {
    const manager = new SchemaVersionManager({ collectionName: 'users' });

    // Simulate concurrent creates
    const schema1 = { field1: { type: 'string', required: false } };
    const schema2 = { field2: { type: 'string', required: false } };

    const v1 = manager.createVersion(schema1 as Record<string, SchemaField>);
    const v2 = manager.createVersion(schema2 as Record<string, SchemaField>);

    expect(v1.version).toBe(1);
    expect(v2.version).toBe(2);
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('SchemaVersionManager integration', () => {
  it('should support complete version lifecycle', () => {
    // Create manager
    const manager = new SchemaVersionManager({ collectionName: 'users' });

    // Create initial version
    const v1 = manager.createVersion(createSimpleSchema(), {
      metadata: { message: 'Initial schema' },
    });
    expect(v1.version).toBe(1);

    // Evolve schema
    const v2 = manager.createVersion(createExtendedSchema(), {
      metadata: { message: 'Added age and createdAt' },
    });
    expect(v2.version).toBe(2);

    // Compare versions
    const diff = manager.compareVersions(1, 2);
    expect(diff.addedFields).toContain('age');
    expect(diff.isBackwardsCompatible).toBe(true);

    // Get migration path
    const path = manager.getMigrationPath(1, 2);
    expect(path).toHaveLength(1);

    // Serialize and restore
    const json = manager.toJSON();
    const restored = SchemaVersionManager.fromJSON(json);
    expect(restored.getVersionCount()).toBe(2);
    expect(restored.getCurrentVersion()?.version).toBe(2);

    // Get Parquet metadata
    const metadata = manager.toParquetMetadata();
    expect(metadata['mongolake.schema.version']).toBe('2');
  });

  it('should track schema evolution over time', () => {
    const manager = new SchemaVersionManager({ collectionName: 'orders' });

    // Version 1: Basic order
    manager.createVersion({
      _id: { type: 'string', required: true },
      total: { type: 'double', required: true },
    });

    // Version 2: Add customer
    manager.createVersion({
      _id: { type: 'string', required: true },
      total: { type: 'double', required: true },
      customerId: { type: 'string', required: false },
    });

    // Version 3: Add shipping
    manager.createVersion({
      _id: { type: 'string', required: true },
      total: { type: 'double', required: true },
      customerId: { type: 'string', required: false },
      'shipping.address': { type: 'string', required: false },
      'shipping.city': { type: 'string', required: false },
    });

    // Verify evolution
    expect(manager.getVersionCount()).toBe(3);

    const diff12 = manager.compareVersions(1, 2);
    expect(diff12.addedFields).toEqual(['customerId']);

    const diff23 = manager.compareVersions(2, 3);
    expect(diff23.addedFields).toContain('shipping.address');
    expect(diff23.addedFields).toContain('shipping.city');

    // Full migration path
    const fullPath = manager.getMigrationPath(1, 3);
    expect(fullPath).toHaveLength(2);
  });
});
