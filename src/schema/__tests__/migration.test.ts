/**
 * Schema Migration Tests
 *
 * Tests for schema migration functionality including:
 * - MigrationRunner creation and registration (document-level)
 * - Migration operations (add, remove, rename fields)
 * - Type conversion/upcasting
 * - Batch migration processing
 * - Forward and backward migrations
 * - Migration composition and conditional transforms
 * - MigrationManager (database-level migrations)
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  MigrationRunner,
  MigrationManager,
  createMigrationRunner,
  createMigrationManager,
  defineMigration,
  defineDatabaseMigration,
  addField,
  removeField,
  renameField,
  changeFieldType,
  compose,
  conditional,
  convertToType,
} from '../migration.js';
import type {
  MigrationDefinition,
  MigratableDocument,
  MigrationResult,
  BatchMigrationResult,
  Migration,
  MigrationRecord,
  MigrationDatabase,
  MigrationCollection,
  MigrationCursor,
} from '../migration.js';

// ============================================================================
// Test Fixtures
// ============================================================================

function createTestDocument(): MigratableDocument {
  return {
    _id: '507f1f77bcf86cd799439011',
    name: 'Alice',
    age: 30,
    email: 'alice@example.com',
  };
}

function createNestedDocument(): MigratableDocument {
  return {
    _id: '507f1f77bcf86cd799439012',
    profile: {
      name: 'Bob',
      age: 25,
    },
    address: {
      city: 'New York',
      zip: '10001',
    },
  };
}

function createMigrationV1(): MigrationDefinition {
  return {
    version: 1,
    description: 'Add email field with default',
    up: addField('email', 'unknown@example.com'),
    down: removeField('email'),
  };
}

function createMigrationV2(): MigrationDefinition {
  return {
    version: 2,
    description: 'Rename name to fullName',
    up: renameField('name', 'fullName'),
    down: renameField('fullName', 'name'),
  };
}

function createMigrationV3(): MigrationDefinition {
  return {
    version: 3,
    description: 'Add createdAt timestamp',
    up: addField('createdAt', new Date('2024-01-01')),
    down: removeField('createdAt'),
  };
}

// ============================================================================
// MigrationRunner Constructor Tests
// ============================================================================

describe('MigrationRunner', () => {
  describe('constructor', () => {
    it('should create a runner with collection name', () => {
      const runner = new MigrationRunner('users');

      expect(runner.getCollectionName()).toBe('users');
      expect(runner.getCurrentVersion()).toBe(0);
      expect(runner.getMigrationVersions()).toEqual([]);
    });

    it('should create runner using factory function', () => {
      const runner = createMigrationRunner('orders');

      expect(runner.getCollectionName()).toBe('orders');
    });
  });

  describe('register', () => {
    let runner: MigrationRunner;

    beforeEach(() => {
      runner = new MigrationRunner('users');
    });

    it('should register a single migration', () => {
      runner.register(createMigrationV1());

      expect(runner.getCurrentVersion()).toBe(1);
      expect(runner.getMigrationVersions()).toEqual([1]);
    });

    it('should register multiple migrations', () => {
      runner.register(createMigrationV1());
      runner.register(createMigrationV2());
      runner.register(createMigrationV3());

      expect(runner.getCurrentVersion()).toBe(3);
      expect(runner.getMigrationVersions()).toEqual([1, 2, 3]);
    });

    it('should registerAll migrations at once', () => {
      runner.registerAll([
        createMigrationV1(),
        createMigrationV2(),
        createMigrationV3(),
      ]);

      expect(runner.getCurrentVersion()).toBe(3);
      expect(runner.getMigrationVersions()).toEqual([1, 2, 3]);
    });

    it('should throw on duplicate version', () => {
      runner.register(createMigrationV1());

      expect(() => runner.register(createMigrationV1())).toThrow(
        'Migration version 1 is already registered'
      );
    });

    it('should throw on non-positive version', () => {
      const invalid = { ...createMigrationV1(), version: 0 };

      expect(() => runner.register(invalid)).toThrow(
        'Migration version must be positive'
      );
    });

    it('should retrieve migration by version', () => {
      runner.register(createMigrationV1());

      const migration = runner.getMigration(1);
      expect(migration).toBeDefined();
      expect(migration?.description).toBe('Add email field with default');
    });

    it('should return undefined for non-existent version', () => {
      expect(runner.getMigration(99)).toBeUndefined();
    });
  });

  describe('validateMigrations', () => {
    let runner: MigrationRunner;

    beforeEach(() => {
      runner = new MigrationRunner('users');
    });

    it('should pass for sequential migrations', () => {
      runner.registerAll([
        createMigrationV1(),
        createMigrationV2(),
        createMigrationV3(),
      ]);

      const errors = runner.validateMigrations();
      expect(errors).toEqual([]);
    });

    it('should detect gaps in versions', () => {
      runner.register(createMigrationV1());
      runner.register(createMigrationV3()); // Skipping V2

      const errors = runner.validateMigrations();
      expect(errors).toContain('Gap in migration versions: 1 to 3');
    });

    it('should detect missing version 1', () => {
      runner.register(createMigrationV2());

      const errors = runner.validateMigrations();
      expect(errors).toContain('Migrations should start at version 1, but first is 2');
    });
  });
});

// ============================================================================
// Migration Operation Tests
// ============================================================================

describe('Migration Operations', () => {
  describe('addField', () => {
    it('should add field with default value', () => {
      const doc = { _id: '1', name: 'Alice' };
      const transform = addField('email', 'default@example.com');

      const result = transform(doc);

      expect(result.email).toBe('default@example.com');
      expect(result.name).toBe('Alice');
    });

    it('should add nested field', () => {
      const doc = { _id: '1' };
      const transform = addField('profile.verified', true);

      const result = transform(doc);

      expect((result.profile as MigratableDocument).verified).toBe(true);
    });

    it('should add field with null default', () => {
      const doc = { _id: '1' };
      const transform = addField('optional', null);

      const result = transform(doc);

      expect(result.optional).toBeNull();
    });
  });

  describe('removeField', () => {
    it('should remove existing field', () => {
      const doc = { _id: '1', name: 'Alice', email: 'alice@example.com' };
      const transform = removeField('email');

      const result = transform(doc);

      expect(result.email).toBeUndefined();
      expect(result.name).toBe('Alice');
    });

    it('should remove nested field', () => {
      const doc = createNestedDocument();
      const transform = removeField('profile.age');

      const result = transform(doc);

      expect((result.profile as MigratableDocument).age).toBeUndefined();
      expect((result.profile as MigratableDocument).name).toBe('Bob');
    });

    it('should do nothing for non-existent field', () => {
      const doc = { _id: '1', name: 'Alice' };
      const transform = removeField('nonexistent');

      const result = transform(doc);

      expect(result).toEqual({ _id: '1', name: 'Alice' });
    });
  });

  describe('renameField', () => {
    it('should rename field', () => {
      const doc = { _id: '1', name: 'Alice' };
      const transform = renameField('name', 'fullName');

      const result = transform(doc);

      expect(result.fullName).toBe('Alice');
      expect(result.name).toBeUndefined();
    });

    it('should rename nested field', () => {
      const doc = createNestedDocument();
      const transform = renameField('profile.name', 'profile.fullName');

      const result = transform(doc);
      const profile = result.profile as MigratableDocument;

      expect(profile.fullName).toBe('Bob');
      expect(profile.name).toBeUndefined();
    });

    it('should do nothing for non-existent field', () => {
      const doc = { _id: '1', name: 'Alice' };
      const transform = renameField('nonexistent', 'newName');

      const result = transform(doc);

      expect(result).toEqual({ _id: '1', name: 'Alice' });
    });
  });

  describe('changeFieldType', () => {
    it('should convert string to int32', () => {
      const doc = { _id: '1', count: '42' };
      const transform = changeFieldType('count', 'int32');

      const result = transform(doc);

      expect(result.count).toBe(42);
      expect(typeof result.count).toBe('number');
    });

    it('should convert number to string', () => {
      const doc = { _id: '1', age: 30 };
      const transform = changeFieldType('age', 'string');

      const result = transform(doc);

      expect(result.age).toBe('30');
      expect(typeof result.age).toBe('string');
    });

    it('should convert string to boolean', () => {
      const doc = { _id: '1', active: 'true' };
      const transform = changeFieldType('active', 'boolean');

      const result = transform(doc);

      expect(result.active).toBe(true);
    });

    it('should use custom converter', () => {
      const doc = { _id: '1', tags: 'a,b,c' };
      const transform = changeFieldType('tags', 'variant', {
        onError: 'throw',
        converter: (val) => (val as string).split(','),
      });

      const result = transform(doc);

      expect(result.tags).toEqual(['a', 'b', 'c']);
    });

    it('should throw on conversion error by default', () => {
      const doc = { _id: '1', count: 'not-a-number' };
      const transform = changeFieldType('count', 'int32');

      expect(() => transform(doc)).toThrow();
    });

    it('should use default value on error when configured', () => {
      const doc = { _id: '1', count: 'not-a-number' };
      const transform = changeFieldType('count', 'int32', {
        onError: 'default',
        defaultValue: 0,
      });

      const result = transform(doc);

      expect(result.count).toBe(0);
    });

    it('should skip conversion on error when configured', () => {
      const doc = { _id: '1', count: 'not-a-number' };
      const transform = changeFieldType('count', 'int32', {
        onError: 'skip',
      });

      const result = transform(doc);

      expect(result.count).toBe('not-a-number');
    });

    it('should skip undefined fields', () => {
      const doc = { _id: '1' };
      const transform = changeFieldType('count', 'int32');

      const result = transform(doc);

      expect(result.count).toBeUndefined();
    });
  });

  describe('compose', () => {
    it('should apply multiple transforms in order', () => {
      const doc = { _id: '1', name: 'Alice' };
      const transform = compose([
        addField('email', 'default@example.com'),
        renameField('name', 'fullName'),
      ]);

      const result = transform(doc);

      expect(result.fullName).toBe('Alice');
      expect(result.email).toBe('default@example.com');
      expect(result.name).toBeUndefined();
    });

    it('should handle empty composition', () => {
      const doc = { _id: '1', name: 'Alice' };
      const transform = compose([]);

      const result = transform(doc);

      expect(result).toEqual(doc);
    });
  });

  describe('conditional', () => {
    it('should apply transform when condition is met', () => {
      const doc = { _id: '1', totalPurchases: 1500 };
      const transform = conditional(
        (d) => (d.totalPurchases as number) > 1000,
        addField('isPremium', true)
      );

      const result = transform(doc);

      expect(result.isPremium).toBe(true);
    });

    it('should skip transform when condition is not met', () => {
      const doc = { _id: '1', totalPurchases: 500 };
      const transform = conditional(
        (d) => (d.totalPurchases as number) > 1000,
        addField('isPremium', true)
      );

      const result = transform(doc);

      expect(result.isPremium).toBeUndefined();
    });
  });
});

// ============================================================================
// Type Conversion Tests
// ============================================================================

describe('convertToType', () => {
  describe('string conversion', () => {
    it('should convert number to string', () => {
      expect(convertToType(42, 'string')).toBe('42');
    });

    it('should convert boolean to string', () => {
      expect(convertToType(true, 'string')).toBe('true');
    });

    it('should convert Date to ISO string', () => {
      const date = new Date('2024-01-15T10:30:00.000Z');
      expect(convertToType(date, 'string')).toBe('2024-01-15T10:30:00.000Z');
    });

    it('should preserve string', () => {
      expect(convertToType('hello', 'string')).toBe('hello');
    });
  });

  describe('int32 conversion', () => {
    it('should convert string to int32', () => {
      expect(convertToType('42', 'int32')).toBe(42);
    });

    it('should convert boolean to int32', () => {
      expect(convertToType(true, 'int32')).toBe(1);
      expect(convertToType(false, 'int32')).toBe(0);
    });

    it('should preserve valid int32', () => {
      expect(convertToType(100, 'int32')).toBe(100);
    });

    it('should throw for non-integer number', () => {
      expect(() => convertToType(3.14, 'int32')).toThrow();
    });

    it('should throw for out-of-range value', () => {
      expect(() => convertToType(3000000000, 'int32')).toThrow();
    });
  });

  describe('int64 conversion', () => {
    it('should convert string to int64', () => {
      expect(convertToType('42', 'int64')).toBe(42);
    });

    it('should handle large numbers as BigInt', () => {
      const largeNum = '9007199254740993'; // > Number.MAX_SAFE_INTEGER
      const result = convertToType(largeNum, 'int64');
      expect(result).toBe(BigInt('9007199254740993'));
    });
  });

  describe('double conversion', () => {
    it('should convert string to double', () => {
      expect(convertToType('3.14', 'double')).toBe(3.14);
    });

    it('should convert integer to double', () => {
      expect(convertToType(42, 'double')).toBe(42);
    });

    it('should convert bigint to double', () => {
      expect(convertToType(BigInt(100), 'double')).toBe(100);
    });
  });

  describe('boolean conversion', () => {
    it('should convert string true values', () => {
      expect(convertToType('true', 'boolean')).toBe(true);
      expect(convertToType('1', 'boolean')).toBe(true);
      expect(convertToType('yes', 'boolean')).toBe(true);
    });

    it('should convert string false values', () => {
      expect(convertToType('false', 'boolean')).toBe(false);
      expect(convertToType('0', 'boolean')).toBe(false);
      expect(convertToType('no', 'boolean')).toBe(false);
    });

    it('should convert number to boolean', () => {
      expect(convertToType(1, 'boolean')).toBe(true);
      expect(convertToType(0, 'boolean')).toBe(false);
    });
  });

  describe('timestamp conversion', () => {
    it('should convert string to Date', () => {
      const result = convertToType('2024-01-15', 'timestamp');
      expect(result).toBeInstanceOf(Date);
    });

    it('should convert number (ms) to Date', () => {
      const ms = 1705312200000;
      const result = convertToType(ms, 'timestamp');
      expect(result).toBeInstanceOf(Date);
      expect((result as Date).getTime()).toBe(ms);
    });

    it('should preserve Date', () => {
      const date = new Date();
      expect(convertToType(date, 'timestamp')).toBe(date);
    });
  });

  describe('binary conversion', () => {
    it('should convert string to Uint8Array', () => {
      const result = convertToType('hello', 'binary');
      expect(result).toBeInstanceOf(Uint8Array);
    });

    it('should convert array to Uint8Array', () => {
      const result = convertToType([1, 2, 3], 'binary');
      expect(result).toBeInstanceOf(Uint8Array);
      expect(result).toEqual(new Uint8Array([1, 2, 3]));
    });
  });

  describe('variant conversion', () => {
    it('should accept any value', () => {
      expect(convertToType('string', 'variant')).toBe('string');
      expect(convertToType(42, 'variant')).toBe(42);
      expect(convertToType({ nested: true }, 'variant')).toEqual({ nested: true });
    });
  });

  describe('null handling', () => {
    it('should pass through null', () => {
      expect(convertToType(null, 'string')).toBeNull();
      expect(convertToType(null, 'int32')).toBeNull();
    });

    it('should pass through undefined', () => {
      expect(convertToType(undefined, 'string')).toBeUndefined();
    });
  });
});

// ============================================================================
// Document Migration Tests
// ============================================================================

describe('Document Migration', () => {
  let runner: MigrationRunner;

  beforeEach(() => {
    runner = new MigrationRunner('users');
    runner.registerAll([
      createMigrationV1(),
      createMigrationV2(),
      createMigrationV3(),
    ]);
  });

  describe('migrateDocument - forward', () => {
    it('should migrate from version 0 to 1', () => {
      const doc = { _id: '1', name: 'Alice' };

      const result = runner.migrateDocument(doc, 0, 1);

      expect(result.success).toBe(true);
      expect(result.migrationsApplied).toBe(1);
      expect(result.document.email).toBe('unknown@example.com');
      expect(result.document._schemaVersion).toBe(1);
    });

    it('should migrate from version 0 to 3', () => {
      const doc = { _id: '1', name: 'Alice' };

      const result = runner.migrateDocument(doc, 0, 3);

      expect(result.success).toBe(true);
      expect(result.migrationsApplied).toBe(3);
      expect(result.document.email).toBe('unknown@example.com');
      expect(result.document.fullName).toBe('Alice');
      expect(result.document.createdAt).toBeDefined();
      expect(result.document._schemaVersion).toBe(3);
    });

    it('should use document version if fromVersion not provided', () => {
      const doc = { _id: '1', name: 'Alice', _schemaVersion: 1 };

      const result = runner.migrateDocument(doc, undefined, 2);

      expect(result.success).toBe(true);
      expect(result.migrationsApplied).toBe(1);
      expect(result.fromVersion).toBe(1);
    });

    it('should migrate to current version if toVersion not provided', () => {
      const doc = { _id: '1', name: 'Alice' };

      const result = runner.migrateDocument(doc, 0);

      expect(result.success).toBe(true);
      expect(result.toVersion).toBe(3);
    });

    it('should set _migratedAt timestamp', () => {
      const doc = { _id: '1', name: 'Alice' };

      const result = runner.migrateDocument(doc, 0, 1);

      expect(result.document._migratedAt).toBeDefined();
      expect(typeof result.document._migratedAt).toBe('string');
    });
  });

  describe('migrateDocument - backward', () => {
    it('should rollback from version 3 to 2', () => {
      const doc = {
        _id: '1',
        fullName: 'Alice',
        email: 'alice@example.com',
        createdAt: new Date(),
        _schemaVersion: 3,
      };

      const result = runner.migrateDocument(doc, 3, 2);

      expect(result.success).toBe(true);
      expect(result.migrationsApplied).toBe(1);
      expect(result.document.createdAt).toBeUndefined();
      expect(result.document._schemaVersion).toBe(2);
    });

    it('should rollback from version 3 to 0', () => {
      const doc = {
        _id: '1',
        fullName: 'Alice',
        email: 'alice@example.com',
        createdAt: new Date(),
        _schemaVersion: 3,
      };

      const result = runner.migrateDocument(doc, 3, 0);

      expect(result.success).toBe(true);
      expect(result.migrationsApplied).toBe(3);
      expect(result.document.name).toBe('Alice');
      expect(result.document.fullName).toBeUndefined();
      expect(result.document.email).toBeUndefined();
      expect(result.document.createdAt).toBeUndefined();
    });
  });

  describe('migrateDocument - edge cases', () => {
    it('should handle same version (no-op)', () => {
      const doc = { _id: '1', name: 'Alice', _schemaVersion: 2 };

      const result = runner.migrateDocument(doc, 2, 2);

      expect(result.success).toBe(true);
      expect(result.migrationsApplied).toBe(0);
    });

    it('should fail for negative versions', () => {
      const doc = { _id: '1' };

      const result = runner.migrateDocument(doc, -1, 1);

      expect(result.success).toBe(false);
      expect(result.error).toContain('non-negative');
    });

    it('should default to version 0 for documents without version', () => {
      const doc = { _id: '1', name: 'Alice' };

      expect(runner.getDocumentVersion(doc)).toBe(0);
    });
  });

  describe('needsMigration', () => {
    it('should detect when migration is needed', () => {
      const doc = { _id: '1', _schemaVersion: 1 };

      expect(runner.needsMigration(doc, 3)).toBe(true);
      expect(runner.needsMigration(doc)).toBe(true); // to current version
    });

    it('should return false when no migration needed', () => {
      const doc = { _id: '1', _schemaVersion: 3 };

      expect(runner.needsMigration(doc, 3)).toBe(false);
      expect(runner.needsMigration(doc)).toBe(false);
    });
  });
});

// ============================================================================
// Batch Migration Tests
// ============================================================================

describe('Batch Migration', () => {
  let runner: MigrationRunner;

  beforeEach(() => {
    runner = new MigrationRunner('users');
    runner.registerAll([
      createMigrationV1(),
      createMigrationV2(),
    ]);
  });

  it('should migrate batch of documents', () => {
    const docs = [
      { _id: '1', name: 'Alice' },
      { _id: '2', name: 'Bob' },
      { _id: '3', name: 'Charlie' },
    ];

    const result = runner.migrateBatch(docs, 0, 2);

    expect(result.total).toBe(3);
    expect(result.succeeded).toBe(3);
    expect(result.failed).toBe(0);
  });

  it('should report progress', () => {
    const docs = [
      { _id: '1', name: 'Alice' },
      { _id: '2', name: 'Bob' },
    ];

    const progress: Array<{ processed: number; total: number }> = [];

    runner.migrateBatch(docs, 0, 2, {
      onProgress: (processed, total) => {
        progress.push({ processed, total });
      },
    });

    expect(progress).toEqual([
      { processed: 1, total: 2 },
      { processed: 2, total: 2 },
    ]);
  });

  it('should include all results when detailed', () => {
    const docs = [
      { _id: '1', name: 'Alice' },
      { _id: '2', name: 'Bob' },
    ];

    const result = runner.migrateBatch(docs, 0, 2, { detailed: true });

    expect(result.results.length).toBe(2);
    expect(result.results[0].success).toBe(true);
    expect(result.results[1].success).toBe(true);
  });

  it('should continue on error by default', () => {
    // Create a runner with a failing migration
    const failRunner = new MigrationRunner('test');
    failRunner.register({
      version: 1,
      description: 'Fail for specific doc',
      up: (doc) => {
        if (doc._id === '2') {
          throw new Error('Intentional failure');
        }
        return { ...doc, migrated: true };
      },
      down: (doc) => doc,
    });

    const docs = [
      { _id: '1', name: 'Alice' },
      { _id: '2', name: 'Bob' },
      { _id: '3', name: 'Charlie' },
    ];

    const result = failRunner.migrateBatch(docs, 0, 1);

    expect(result.succeeded).toBe(2);
    expect(result.failed).toBe(1);
    expect(result.results.length).toBe(1); // Only failures by default
  });

  it('should stop on error when configured', () => {
    const failRunner = new MigrationRunner('test');
    failRunner.register({
      version: 1,
      description: 'Fail for specific doc',
      up: (doc) => {
        if (doc._id === '2') {
          throw new Error('Intentional failure');
        }
        return { ...doc, migrated: true };
      },
      down: (doc) => doc,
    });

    const docs = [
      { _id: '1', name: 'Alice' },
      { _id: '2', name: 'Bob' },
      { _id: '3', name: 'Charlie' },
    ];

    const result = failRunner.migrateBatch(docs, 0, 1, { stopOnError: true });

    // Should stop after first failure
    expect(result.succeeded).toBe(1);
    expect(result.failed).toBe(1);
    // Third document not processed
  });
});

// ============================================================================
// Serialization Tests
// ============================================================================

describe('Serialization', () => {
  it('should serialize migration registry', () => {
    const runner = new MigrationRunner('users');
    runner.registerAll([
      {
        ...createMigrationV1(),
        metadata: {
          author: 'Alice',
          ticket: 'JIRA-123',
        },
      },
      createMigrationV2(),
    ]);

    const serialized = runner.serialize();

    expect(serialized.collectionName).toBe('users');
    expect(serialized.currentVersion).toBe(2);
    expect(serialized.migrations).toHaveLength(2);
    expect(serialized.migrations[0].description).toBe('Add email field with default');
    expect(serialized.migrations[0].metadata?.author).toBe('Alice');
  });
});

// ============================================================================
// defineMigration Helper Tests
// ============================================================================

describe('defineMigration', () => {
  it('should return migration as-is (type helper)', () => {
    const migration = defineMigration({
      version: 1,
      description: 'Test migration',
      up: (doc) => ({ ...doc, test: true }),
      down: (doc) => {
        const { test, ...rest } = doc;
        return rest;
      },
    });

    expect(migration.version).toBe(1);
    expect(migration.description).toBe('Test migration');
  });
});

// ============================================================================
// MigrationManager Tests (Database-level migrations)
// ============================================================================

/**
 * Create a mock database for testing MigrationManager
 */
function createMockDatabase(): {
  db: MigrationDatabase;
  collections: Map<string, MigrationRecord[]>;
} {
  const collections = new Map<string, MigrationRecord[]>();

  const createMockCursor = <T>(data: T[]): MigrationCursor<T> => {
    let sortedData = [...data];
    let limitNum = Infinity;

    return {
      sort(spec: Record<string, 1 | -1>): MigrationCursor<T> {
        const key = Object.keys(spec)[0]!;
        const order = spec[key]!;
        sortedData = [...sortedData].sort((a, b) => {
          const aVal = (a as Record<string, unknown>)[key];
          const bVal = (b as Record<string, unknown>)[key];
          if (aVal instanceof Date && bVal instanceof Date) {
            return order === 1
              ? aVal.getTime() - bVal.getTime()
              : bVal.getTime() - aVal.getTime();
          }
          if (typeof aVal === 'number' && typeof bVal === 'number') {
            return order === 1 ? aVal - bVal : bVal - aVal;
          }
          return 0;
        });
        return this;
      },
      limit(n: number): MigrationCursor<T> {
        limitNum = n;
        return this;
      },
      async toArray(): Promise<T[]> {
        return sortedData.slice(0, limitNum);
      },
    };
  };

  const createMockCollection = <T extends Record<string, unknown>>(
    name: string
  ): MigrationCollection<T> => {
    if (!collections.has(name)) {
      collections.set(name, []);
    }

    return {
      async insertOne(doc: T): Promise<{ insertedId: unknown }> {
        const data = collections.get(name)!;
        data.push(doc as unknown as MigrationRecord);
        return { insertedId: (doc as Record<string, unknown>)._id ?? data.length };
      },
      async insertMany(docs: T[]): Promise<{ insertedCount: number }> {
        const data = collections.get(name)!;
        for (const doc of docs) {
          data.push(doc as unknown as MigrationRecord);
        }
        return { insertedCount: docs.length };
      },
      async updateOne(
        filter: Record<string, unknown>,
        update: Record<string, unknown>
      ): Promise<{ modifiedCount: number }> {
        const data = collections.get(name)!;
        const idx = data.findIndex((d) =>
          Object.entries(filter).every(([k, v]) => (d as Record<string, unknown>)[k] === v)
        );
        if (idx >= 0) {
          const $set = (update as { $set?: Record<string, unknown> }).$set;
          if ($set) {
            Object.assign(data[idx]!, $set);
          }
          return { modifiedCount: 1 };
        }
        return { modifiedCount: 0 };
      },
      async updateMany(
        filter: Record<string, unknown>,
        update: Record<string, unknown>
      ): Promise<{ modifiedCount: number }> {
        const data = collections.get(name)!;
        let count = 0;
        const $set = (update as { $set?: Record<string, unknown> }).$set;
        for (const doc of data) {
          const matches = Object.entries(filter).every(
            ([k, v]) => (doc as Record<string, unknown>)[k] === v
          );
          if (matches && $set) {
            Object.assign(doc, $set);
            count++;
          }
        }
        return { modifiedCount: count };
      },
      async deleteOne(filter: Record<string, unknown>): Promise<{ deletedCount: number }> {
        const data = collections.get(name)!;
        const idx = data.findIndex((d) =>
          Object.entries(filter).every(([k, v]) => (d as Record<string, unknown>)[k] === v)
        );
        if (idx >= 0) {
          data.splice(idx, 1);
          return { deletedCount: 1 };
        }
        return { deletedCount: 0 };
      },
      async deleteMany(filter: Record<string, unknown>): Promise<{ deletedCount: number }> {
        const data = collections.get(name)!;
        const initial = data.length;
        const remaining = data.filter(
          (d) =>
            !Object.entries(filter).every(([k, v]) => (d as Record<string, unknown>)[k] === v)
        );
        collections.set(name, remaining);
        return { deletedCount: initial - remaining.length };
      },
      async findOne(filter?: Record<string, unknown>): Promise<T | null> {
        const data = collections.get(name)!;
        if (!filter || Object.keys(filter).length === 0) {
          return (data[0] as unknown as T) ?? null;
        }
        const found = data.find((d) =>
          Object.entries(filter).every(([k, v]) => (d as Record<string, unknown>)[k] === v)
        );
        return (found as unknown as T) ?? null;
      },
      find(filter?: Record<string, unknown>): MigrationCursor<T> {
        const data = collections.get(name)!;
        let filtered: MigrationRecord[];
        if (!filter || Object.keys(filter).length === 0) {
          filtered = [...data];
        } else {
          filtered = data.filter((d) =>
            Object.entries(filter).every(([k, v]) => (d as Record<string, unknown>)[k] === v)
          );
        }
        return createMockCursor(filtered as unknown as T[]);
      },
      async createIndex(
        _spec: Record<string, unknown>,
        _options?: Record<string, unknown>
      ): Promise<string> {
        return 'index_created';
      },
      async dropIndex(_name: string): Promise<void> {
        // No-op for mock
      },
    };
  };

  const db: MigrationDatabase = {
    name: 'test_db',
    collection: <T extends Record<string, unknown>>(name: string) =>
      createMockCollection<T>(name),
  };

  return { db, collections };
}

describe('MigrationManager', () => {
  describe('constructor', () => {
    it('should create a manager with default migrations collection', () => {
      const { db } = createMockDatabase();
      const manager = new MigrationManager(db);

      expect(manager).toBeInstanceOf(MigrationManager);
    });

    it('should create a manager with custom migrations collection name', () => {
      const { db } = createMockDatabase();
      const manager = new MigrationManager(db, 'custom_migrations');

      expect(manager).toBeInstanceOf(MigrationManager);
    });

    it('should create manager using factory function', () => {
      const { db } = createMockDatabase();
      const manager = createMigrationManager(db);

      expect(manager).toBeInstanceOf(MigrationManager);
    });
  });

  describe('getCurrentVersion', () => {
    it('should return 0 when no migrations have been applied', async () => {
      const { db } = createMockDatabase();
      const manager = new MigrationManager(db);

      const version = await manager.getCurrentVersion();

      expect(version).toBe(0);
    });

    it('should return the current version after migrations', async () => {
      const { db, collections } = createMockDatabase();
      const manager = new MigrationManager(db);

      // Simulate applied migrations
      collections.set('_migrations', [
        {
          version: 1,
          name: 'migration-1',
          appliedAt: new Date('2024-01-01'),
          duration: 100,
          direction: 'up',
          success: true,
        },
        {
          version: 2,
          name: 'migration-2',
          appliedAt: new Date('2024-01-02'),
          duration: 50,
          direction: 'up',
          success: true,
        },
      ]);

      const version = await manager.getCurrentVersion();

      expect(version).toBe(2);
    });

    it('should account for rollbacks in version calculation', async () => {
      const { db, collections } = createMockDatabase();
      const manager = new MigrationManager(db);

      collections.set('_migrations', [
        {
          version: 1,
          name: 'migration-1',
          appliedAt: new Date('2024-01-01'),
          duration: 100,
          direction: 'up',
          success: true,
        },
        {
          version: 2,
          name: 'migration-2',
          appliedAt: new Date('2024-01-02'),
          duration: 50,
          direction: 'up',
          success: true,
        },
        {
          version: 2,
          name: 'migration-2',
          appliedAt: new Date('2024-01-03'),
          duration: 30,
          direction: 'down',
          success: true,
        },
      ]);

      const version = await manager.getCurrentVersion();

      expect(version).toBe(1);
    });
  });

  describe('runMigrations', () => {
    it('should apply pending migrations in order', async () => {
      const { db } = createMockDatabase();
      const manager = new MigrationManager(db);

      const appliedOrder: number[] = [];
      const migrations: Migration[] = [
        {
          version: 1,
          name: 'migration-1',
          up: async () => {
            appliedOrder.push(1);
          },
          down: async () => {},
        },
        {
          version: 2,
          name: 'migration-2',
          up: async () => {
            appliedOrder.push(2);
          },
          down: async () => {},
        },
      ];

      const result = await manager.runMigrations(migrations);

      expect(result.success).toBe(true);
      expect(result.migrationsApplied).toBe(2);
      expect(result.fromVersion).toBe(0);
      expect(result.toVersion).toBe(2);
      expect(appliedOrder).toEqual([1, 2]);
    });

    it('should skip already applied migrations', async () => {
      const { db, collections } = createMockDatabase();
      const manager = new MigrationManager(db);

      // Migration 1 already applied
      collections.set('_migrations', [
        {
          version: 1,
          name: 'migration-1',
          appliedAt: new Date(),
          duration: 100,
          direction: 'up',
          success: true,
        },
      ]);

      const appliedVersions: number[] = [];
      const migrations: Migration[] = [
        {
          version: 1,
          name: 'migration-1',
          up: async () => {
            appliedVersions.push(1);
          },
          down: async () => {},
        },
        {
          version: 2,
          name: 'migration-2',
          up: async () => {
            appliedVersions.push(2);
          },
          down: async () => {},
        },
      ];

      const result = await manager.runMigrations(migrations);

      expect(result.success).toBe(true);
      expect(result.migrationsApplied).toBe(1);
      expect(appliedVersions).toEqual([2]);
    });

    it('should stop at target version', async () => {
      const { db } = createMockDatabase();
      const manager = new MigrationManager(db);

      const migrations: Migration[] = [
        {
          version: 1,
          name: 'migration-1',
          up: async () => {},
          down: async () => {},
        },
        {
          version: 2,
          name: 'migration-2',
          up: async () => {},
          down: async () => {},
        },
        {
          version: 3,
          name: 'migration-3',
          up: async () => {},
          down: async () => {},
        },
      ];

      const result = await manager.runMigrations(migrations, { targetVersion: 2 });

      expect(result.success).toBe(true);
      expect(result.migrationsApplied).toBe(2);
      expect(result.toVersion).toBe(2);
    });

    it('should stop on error by default', async () => {
      const { db } = createMockDatabase();
      const manager = new MigrationManager(db);

      const migrations: Migration[] = [
        {
          version: 1,
          name: 'migration-1',
          up: async () => {},
          down: async () => {},
        },
        {
          version: 2,
          name: 'migration-2-fails',
          up: async () => {
            throw new Error('Migration failed');
          },
          down: async () => {},
        },
        {
          version: 3,
          name: 'migration-3',
          up: async () => {},
          down: async () => {},
        },
      ];

      const result = await manager.runMigrations(migrations);

      expect(result.success).toBe(false);
      expect(result.migrationsApplied).toBe(1);
      expect(result.toVersion).toBe(1);
      expect(result.error).toBe('Migration failed');
    });

    it('should call onProgress callback', async () => {
      const { db } = createMockDatabase();
      const manager = new MigrationManager(db);

      const progressCalls: Array<{ version: number; direction: string }> = [];
      const migrations: Migration[] = [
        {
          version: 1,
          name: 'migration-1',
          up: async () => {},
          down: async () => {},
        },
        {
          version: 2,
          name: 'migration-2',
          up: async () => {},
          down: async () => {},
        },
      ];

      await manager.runMigrations(migrations, {
        onProgress: (m, dir) => {
          progressCalls.push({ version: m.version, direction: dir });
        },
      });

      expect(progressCalls).toEqual([
        { version: 1, direction: 'up' },
        { version: 2, direction: 'up' },
      ]);
    });

    it('should support dry run mode', async () => {
      const { db, collections } = createMockDatabase();
      const manager = new MigrationManager(db);

      let applied = false;
      const migrations: Migration[] = [
        {
          version: 1,
          name: 'migration-1',
          up: async () => {
            applied = true;
          },
          down: async () => {},
        },
      ];

      const result = await manager.runMigrations(migrations, { dryRun: true });

      expect(result.success).toBe(true);
      expect(result.migrationsApplied).toBe(1);
      expect(applied).toBe(false); // Should not have actually run
      expect(collections.get('_migrations')?.length ?? 0).toBe(0); // No records saved
    });

    it('should return empty result when no migrations to apply', async () => {
      const { db } = createMockDatabase();
      const manager = new MigrationManager(db);

      const result = await manager.runMigrations([]);

      expect(result.success).toBe(true);
      expect(result.migrationsApplied).toBe(0);
      expect(result.fromVersion).toBe(0);
      expect(result.toVersion).toBe(0);
    });
  });

  describe('runMigrations validation', () => {
    it('should reject duplicate migration versions', async () => {
      const { db } = createMockDatabase();
      const manager = new MigrationManager(db);

      const migrations: Migration[] = [
        {
          version: 1,
          name: 'migration-1',
          up: async () => {},
          down: async () => {},
        },
        {
          version: 1,
          name: 'migration-1-duplicate',
          up: async () => {},
          down: async () => {},
        },
      ];

      await expect(manager.runMigrations(migrations)).rejects.toThrow(
        'Duplicate migration version: 1'
      );
    });

    it('should reject non-positive version numbers', async () => {
      const { db } = createMockDatabase();
      const manager = new MigrationManager(db);

      const migrations: Migration[] = [
        {
          version: 0,
          name: 'migration-0',
          up: async () => {},
          down: async () => {},
        },
      ];

      await expect(manager.runMigrations(migrations)).rejects.toThrow(
        'Migration version must be positive'
      );
    });

    it('should reject migrations without name', async () => {
      const { db } = createMockDatabase();
      const manager = new MigrationManager(db);

      const migrations = [
        {
          version: 1,
          name: '',
          up: async () => {},
          down: async () => {},
        },
      ] as Migration[];

      await expect(manager.runMigrations(migrations)).rejects.toThrow(
        'Migration version 1 must have a name'
      );
    });
  });

  describe('rollbackWithMigrations', () => {
    it('should rollback one migration by default', async () => {
      const { db, collections } = createMockDatabase();
      const manager = new MigrationManager(db);

      // Set up applied migrations
      collections.set('_migrations', [
        {
          version: 1,
          name: 'migration-1',
          appliedAt: new Date('2024-01-01'),
          duration: 100,
          direction: 'up',
          success: true,
        },
        {
          version: 2,
          name: 'migration-2',
          appliedAt: new Date('2024-01-02'),
          duration: 50,
          direction: 'up',
          success: true,
        },
      ]);

      let rolledBack = false;
      const migrations: Migration[] = [
        {
          version: 1,
          name: 'migration-1',
          up: async () => {},
          down: async () => {},
        },
        {
          version: 2,
          name: 'migration-2',
          up: async () => {},
          down: async () => {
            rolledBack = true;
          },
        },
      ];

      const result = await manager.rollbackWithMigrations(migrations);

      expect(result.success).toBe(true);
      expect(result.migrationsApplied).toBe(1);
      expect(result.fromVersion).toBe(2);
      expect(result.toVersion).toBe(1);
      expect(rolledBack).toBe(true);
    });

    it('should rollback multiple migrations', async () => {
      const { db, collections } = createMockDatabase();
      const manager = new MigrationManager(db);

      collections.set('_migrations', [
        {
          version: 1,
          name: 'migration-1',
          appliedAt: new Date('2024-01-01'),
          duration: 100,
          direction: 'up',
          success: true,
        },
        {
          version: 2,
          name: 'migration-2',
          appliedAt: new Date('2024-01-02'),
          duration: 50,
          direction: 'up',
          success: true,
        },
        {
          version: 3,
          name: 'migration-3',
          appliedAt: new Date('2024-01-03'),
          duration: 30,
          direction: 'up',
          success: true,
        },
      ]);

      const rollbackOrder: number[] = [];
      const migrations: Migration[] = [
        {
          version: 1,
          name: 'migration-1',
          up: async () => {},
          down: async () => {
            rollbackOrder.push(1);
          },
        },
        {
          version: 2,
          name: 'migration-2',
          up: async () => {},
          down: async () => {
            rollbackOrder.push(2);
          },
        },
        {
          version: 3,
          name: 'migration-3',
          up: async () => {},
          down: async () => {
            rollbackOrder.push(3);
          },
        },
      ];

      const result = await manager.rollbackWithMigrations(migrations, 2);

      expect(result.success).toBe(true);
      expect(result.migrationsApplied).toBe(2);
      expect(result.toVersion).toBe(1);
      expect(rollbackOrder).toEqual([3, 2]);
    });

    it('should handle rollback when at version 0', async () => {
      const { db } = createMockDatabase();
      const manager = new MigrationManager(db);

      const migrations: Migration[] = [
        {
          version: 1,
          name: 'migration-1',
          up: async () => {},
          down: async () => {},
        },
      ];

      const result = await manager.rollbackWithMigrations(migrations);

      expect(result.success).toBe(true);
      expect(result.migrationsApplied).toBe(0);
    });

    it('should fail if migration definition not found', async () => {
      const { db, collections } = createMockDatabase();
      const manager = new MigrationManager(db);

      collections.set('_migrations', [
        {
          version: 1,
          name: 'migration-1',
          appliedAt: new Date(),
          duration: 100,
          direction: 'up',
          success: true,
        },
      ]);

      const migrations: Migration[] = []; // No migrations provided

      const result = await manager.rollbackWithMigrations(migrations);

      expect(result.success).toBe(false);
      expect(result.error).toContain('Migration version 1 not found');
    });

    it('should reject steps less than 1', async () => {
      const { db } = createMockDatabase();
      const manager = new MigrationManager(db);

      await expect(manager.rollbackWithMigrations([], 0)).rejects.toThrow(
        'Steps must be at least 1'
      );
    });
  });

  describe('getHistory', () => {
    it('should return empty array when no history', async () => {
      const { db } = createMockDatabase();
      const manager = new MigrationManager(db);

      const history = await manager.getHistory();

      expect(history).toEqual([]);
    });

    it('should return migration history sorted by appliedAt descending', async () => {
      const { db, collections } = createMockDatabase();
      const manager = new MigrationManager(db);

      collections.set('_migrations', [
        {
          version: 1,
          name: 'migration-1',
          appliedAt: new Date('2024-01-01'),
          duration: 100,
          direction: 'up',
          success: true,
        },
        {
          version: 2,
          name: 'migration-2',
          appliedAt: new Date('2024-01-03'),
          duration: 50,
          direction: 'up',
          success: true,
        },
        {
          version: 2,
          name: 'migration-2',
          appliedAt: new Date('2024-01-02'),
          duration: 30,
          direction: 'down',
          success: true,
        },
      ]);

      const history = await manager.getHistory();

      expect(history.length).toBe(3);
      // Should be sorted by appliedAt descending
      expect(history[0].version).toBe(2);
      expect(history[0].direction).toBe('up');
    });
  });

  describe('getPendingMigrations', () => {
    it('should return all migrations when at version 0', async () => {
      const { db } = createMockDatabase();
      const manager = new MigrationManager(db);

      const migrations: Migration[] = [
        {
          version: 1,
          name: 'migration-1',
          up: async () => {},
          down: async () => {},
        },
        {
          version: 2,
          name: 'migration-2',
          up: async () => {},
          down: async () => {},
        },
      ];

      const pending = await manager.getPendingMigrations(migrations);

      expect(pending.length).toBe(2);
      expect(pending.map((m) => m.version)).toEqual([1, 2]);
    });

    it('should return only pending migrations', async () => {
      const { db, collections } = createMockDatabase();
      const manager = new MigrationManager(db);

      collections.set('_migrations', [
        {
          version: 1,
          name: 'migration-1',
          appliedAt: new Date(),
          duration: 100,
          direction: 'up',
          success: true,
        },
      ]);

      const migrations: Migration[] = [
        {
          version: 1,
          name: 'migration-1',
          up: async () => {},
          down: async () => {},
        },
        {
          version: 2,
          name: 'migration-2',
          up: async () => {},
          down: async () => {},
        },
        {
          version: 3,
          name: 'migration-3',
          up: async () => {},
          down: async () => {},
        },
      ];

      const pending = await manager.getPendingMigrations(migrations);

      expect(pending.length).toBe(2);
      expect(pending.map((m) => m.version)).toEqual([2, 3]);
    });
  });

  describe('hasPendingMigrations', () => {
    it('should return true when migrations are pending', async () => {
      const { db } = createMockDatabase();
      const manager = new MigrationManager(db);

      const migrations: Migration[] = [
        {
          version: 1,
          name: 'migration-1',
          up: async () => {},
          down: async () => {},
        },
      ];

      const hasPending = await manager.hasPendingMigrations(migrations);

      expect(hasPending).toBe(true);
    });

    it('should return false when no migrations are pending', async () => {
      const { db, collections } = createMockDatabase();
      const manager = new MigrationManager(db);

      collections.set('_migrations', [
        {
          version: 1,
          name: 'migration-1',
          appliedAt: new Date(),
          duration: 100,
          direction: 'up',
          success: true,
        },
      ]);

      const migrations: Migration[] = [
        {
          version: 1,
          name: 'migration-1',
          up: async () => {},
          down: async () => {},
        },
      ];

      const hasPending = await manager.hasPendingMigrations(migrations);

      expect(hasPending).toBe(false);
    });
  });
});

// ============================================================================
// defineDatabaseMigration Helper Tests
// ============================================================================

describe('defineDatabaseMigration', () => {
  it('should return migration as-is (type helper)', () => {
    const migration = defineDatabaseMigration({
      version: 1,
      name: 'test-migration',
      up: async (_db) => {},
      down: async (_db) => {},
    });

    expect(migration.version).toBe(1);
    expect(migration.name).toBe('test-migration');
  });
});
