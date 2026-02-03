/**
 * Schema Migration Tooling Tests
 *
 * Comprehensive tests for schema migration functionality including:
 * - Schema version tracking
 * - Running migrations up (forward)
 * - Running migrations down (rollback)
 * - Migration history tracking
 * - Rollback on failure
 * - Skip already-applied migrations
 * - Migration ordering
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  MigrationRunner,
  createMigrationRunner,
  defineMigration,
  addField,
  removeField,
  renameField,
  changeFieldType,
  compose,
  conditional,
} from '../../../src/schema/migration.js';
import type {
  MigrationDefinition,
  MigratableDocument,
} from '../../../src/schema/migration.js';

// ============================================================================
// Test Fixtures
// ============================================================================

function createBasicMigrationV1(): MigrationDefinition {
  return {
    version: 1,
    description: 'Add email field',
    up: addField('email', 'unknown@example.com'),
    down: removeField('email'),
    metadata: {
      author: 'test',
      createdAt: new Date('2024-01-01'),
      ticket: 'TICKET-001',
    },
  };
}

function createBasicMigrationV2(): MigrationDefinition {
  return {
    version: 2,
    description: 'Rename name to fullName',
    up: renameField('name', 'fullName'),
    down: renameField('fullName', 'name'),
    metadata: {
      author: 'test',
      createdAt: new Date('2024-01-02'),
      ticket: 'TICKET-002',
    },
  };
}

function createBasicMigrationV3(): MigrationDefinition {
  return {
    version: 3,
    description: 'Add createdAt timestamp',
    up: addField('createdAt', new Date('2024-01-01T00:00:00Z')),
    down: removeField('createdAt'),
    metadata: {
      author: 'test',
      createdAt: new Date('2024-01-03'),
      ticket: 'TICKET-003',
    },
  };
}

function createBasicMigrationV4(): MigrationDefinition {
  return {
    version: 4,
    description: 'Add status field',
    up: addField('status', 'active'),
    down: removeField('status'),
    metadata: {
      author: 'test',
      createdAt: new Date('2024-01-04'),
    },
  };
}

function createBasicMigrationV5(): MigrationDefinition {
  return {
    version: 5,
    description: 'Add role field',
    up: addField('role', 'user'),
    down: removeField('role'),
    metadata: {
      author: 'test',
      createdAt: new Date('2024-01-05'),
    },
  };
}

function createFailingMigration(version: number): MigrationDefinition {
  return {
    version,
    description: 'Migration that throws error',
    up: (_doc: MigratableDocument) => {
      throw new Error(`Migration V${version} failed intentionally`);
    },
    down: (doc: MigratableDocument) => doc,
  };
}

// ============================================================================
// Schema Version Tracking Tests
// ============================================================================

describe('Schema Version Tracking', () => {
  let runner: MigrationRunner;

  beforeEach(() => {
    runner = createMigrationRunner('users');
  });

  it('should start with version 0 when no migrations are registered', () => {
    expect(runner.getCurrentVersion()).toBe(0);
  });

  it('should update current version when migrations are registered', () => {
    runner.register(createBasicMigrationV1());
    expect(runner.getCurrentVersion()).toBe(1);

    runner.register(createBasicMigrationV2());
    expect(runner.getCurrentVersion()).toBe(2);

    runner.register(createBasicMigrationV3());
    expect(runner.getCurrentVersion()).toBe(3);
  });

  it('should track document schema version after migration', () => {
    runner.register(createBasicMigrationV1());

    const doc = { _id: '1', name: 'Alice' };
    const result = runner.migrateDocument(doc, 0, 1);

    expect(result.document._schemaVersion).toBe(1);
  });

  it('should update document schema version through multiple migrations', () => {
    runner.registerAll([
      createBasicMigrationV1(),
      createBasicMigrationV2(),
      createBasicMigrationV3(),
    ]);

    const doc = { _id: '1', name: 'Alice' };
    const result = runner.migrateDocument(doc, 0, 3);

    expect(result.document._schemaVersion).toBe(3);
  });

  it('should read schema version from document when not provided', () => {
    runner.registerAll([
      createBasicMigrationV1(),
      createBasicMigrationV2(),
    ]);

    const doc = { _id: '1', name: 'Alice', email: 'alice@test.com', _schemaVersion: 1 };
    const result = runner.migrateDocument(doc, undefined, 2);

    expect(result.fromVersion).toBe(1);
    expect(result.toVersion).toBe(2);
    expect(result.migrationsApplied).toBe(1);
  });

  it('should default to version 0 for documents without schema version', () => {
    runner.register(createBasicMigrationV1());

    const doc = { _id: '1', name: 'Alice' };
    expect(runner.getDocumentVersion(doc)).toBe(0);
  });

  it('should set _migratedAt timestamp after migration', () => {
    runner.register(createBasicMigrationV1());

    const doc = { _id: '1', name: 'Alice' };
    const result = runner.migrateDocument(doc, 0, 1);

    expect(result.document._migratedAt).toBeDefined();
    expect(typeof result.document._migratedAt).toBe('string');
    // Verify it's a valid ISO date string
    expect(new Date(result.document._migratedAt as string).toISOString()).toBe(result.document._migratedAt);
  });
});

// ============================================================================
// Run Migration Up Tests
// ============================================================================

describe('Run Migration Up', () => {
  let runner: MigrationRunner;

  beforeEach(() => {
    runner = createMigrationRunner('users');
    runner.registerAll([
      createBasicMigrationV1(),
      createBasicMigrationV2(),
      createBasicMigrationV3(),
      createBasicMigrationV4(),
      createBasicMigrationV5(),
    ]);
  });

  it('should apply single migration forward', () => {
    const doc = { _id: '1', name: 'Alice' };
    const result = runner.migrateDocument(doc, 0, 1);

    expect(result.success).toBe(true);
    expect(result.migrationsApplied).toBe(1);
    expect(result.document.email).toBe('unknown@example.com');
  });

  it('should apply multiple migrations forward in sequence', () => {
    const doc = { _id: '1', name: 'Alice' };
    const result = runner.migrateDocument(doc, 0, 3);

    expect(result.success).toBe(true);
    expect(result.migrationsApplied).toBe(3);
    expect(result.document.email).toBe('unknown@example.com');
    expect(result.document.fullName).toBe('Alice');
    expect(result.document.name).toBeUndefined();
    expect(result.document.createdAt).toBeDefined();
  });

  it('should apply all migrations to latest version when toVersion not specified', () => {
    const doc = { _id: '1', name: 'Alice' };
    const result = runner.migrateDocument(doc, 0);

    expect(result.success).toBe(true);
    expect(result.toVersion).toBe(5);
    expect(result.migrationsApplied).toBe(5);
    expect(result.document.role).toBe('user');
  });

  it('should apply migrations from intermediate version', () => {
    const doc = { _id: '1', fullName: 'Alice', email: 'alice@test.com', _schemaVersion: 2 };
    const result = runner.migrateDocument(doc, 2, 4);

    expect(result.success).toBe(true);
    expect(result.migrationsApplied).toBe(2);
    expect(result.document.createdAt).toBeDefined();
    expect(result.document.status).toBe('active');
  });

  it('should transform document correctly through each migration step', () => {
    const doc = { _id: '1', name: 'Bob', age: 25 };

    // Step 1: Add email
    let result = runner.migrateDocument(doc, 0, 1);
    expect(result.document.email).toBe('unknown@example.com');
    expect(result.document.name).toBe('Bob');

    // Step 2: Rename name to fullName
    result = runner.migrateDocument(result.document, 1, 2);
    expect(result.document.fullName).toBe('Bob');
    expect(result.document.name).toBeUndefined();

    // Step 3: Add createdAt
    result = runner.migrateDocument(result.document, 2, 3);
    expect(result.document.createdAt).toBeDefined();
  });

  it('should report correct from and to versions in result', () => {
    const doc = { _id: '1', name: 'Alice' };
    const result = runner.migrateDocument(doc, 0, 3);

    expect(result.fromVersion).toBe(0);
    expect(result.toVersion).toBe(3);
  });
});

// ============================================================================
// Run Migration Down (Rollback) Tests
// ============================================================================

describe('Run Migration Down (Rollback)', () => {
  let runner: MigrationRunner;

  beforeEach(() => {
    runner = createMigrationRunner('users');
    runner.registerAll([
      createBasicMigrationV1(),
      createBasicMigrationV2(),
      createBasicMigrationV3(),
      createBasicMigrationV4(),
      createBasicMigrationV5(),
    ]);
  });

  it('should rollback single migration', () => {
    const doc = {
      _id: '1',
      fullName: 'Alice',
      email: 'alice@test.com',
      createdAt: new Date(),
      _schemaVersion: 3,
    };
    const result = runner.migrateDocument(doc, 3, 2);

    expect(result.success).toBe(true);
    expect(result.migrationsApplied).toBe(1);
    expect(result.document.createdAt).toBeUndefined();
    expect(result.document._schemaVersion).toBe(2);
  });

  it('should rollback multiple migrations in reverse order', () => {
    const doc = {
      _id: '1',
      fullName: 'Alice',
      email: 'alice@test.com',
      createdAt: new Date(),
      status: 'active',
      role: 'admin',
      _schemaVersion: 5,
    };
    const result = runner.migrateDocument(doc, 5, 1);

    expect(result.success).toBe(true);
    expect(result.migrationsApplied).toBe(4);
    expect(result.document.role).toBeUndefined();
    expect(result.document.status).toBeUndefined();
    expect(result.document.createdAt).toBeUndefined();
    expect(result.document.name).toBe('Alice'); // Renamed back from fullName
    expect(result.document.fullName).toBeUndefined();
    expect(result.document._schemaVersion).toBe(1);
  });

  it('should rollback to version 0', () => {
    const doc = {
      _id: '1',
      fullName: 'Alice',
      email: 'alice@test.com',
      createdAt: new Date(),
      _schemaVersion: 3,
    };
    const result = runner.migrateDocument(doc, 3, 0);

    expect(result.success).toBe(true);
    expect(result.migrationsApplied).toBe(3);
    expect(result.document.email).toBeUndefined();
    expect(result.document.name).toBe('Alice');
    expect(result.document.createdAt).toBeUndefined();
    expect(result.document._schemaVersion).toBe(0);
  });

  it('should preserve unrelated fields during rollback', () => {
    const doc = {
      _id: '1',
      fullName: 'Alice',
      email: 'alice@test.com',
      age: 30,
      customField: 'preserved',
      _schemaVersion: 2,
    };
    const result = runner.migrateDocument(doc, 2, 1);

    expect(result.document.age).toBe(30);
    expect(result.document.customField).toBe('preserved');
    expect(result.document.name).toBe('Alice');
    expect(result.document.fullName).toBeUndefined();
  });

  it('should handle rollback from intermediate to intermediate version', () => {
    const doc = {
      _id: '1',
      fullName: 'Alice',
      email: 'alice@test.com',
      createdAt: new Date(),
      status: 'active',
      _schemaVersion: 4,
    };
    const result = runner.migrateDocument(doc, 4, 2);

    expect(result.success).toBe(true);
    expect(result.migrationsApplied).toBe(2);
    expect(result.document.status).toBeUndefined();
    expect(result.document.createdAt).toBeUndefined();
    expect(result.document.fullName).toBe('Alice');
  });
});

// ============================================================================
// Migration History Tracking Tests
// ============================================================================

describe('Migration History Tracking', () => {
  let runner: MigrationRunner;

  beforeEach(() => {
    runner = createMigrationRunner('users');
  });

  it('should track all registered migration versions', () => {
    runner.registerAll([
      createBasicMigrationV1(),
      createBasicMigrationV2(),
      createBasicMigrationV3(),
    ]);

    expect(runner.getMigrationVersions()).toEqual([1, 2, 3]);
  });

  it('should retrieve specific migration by version', () => {
    runner.registerAll([
      createBasicMigrationV1(),
      createBasicMigrationV2(),
    ]);

    const migration = runner.getMigration(1);
    expect(migration).toBeDefined();
    expect(migration?.description).toBe('Add email field');
    expect(migration?.metadata?.author).toBe('test');
    expect(migration?.metadata?.ticket).toBe('TICKET-001');
  });

  it('should serialize migration history for persistence', () => {
    runner.registerAll([
      createBasicMigrationV1(),
      createBasicMigrationV2(),
      createBasicMigrationV3(),
    ]);

    const serialized = runner.serialize();

    expect(serialized.collectionName).toBe('users');
    expect(serialized.currentVersion).toBe(3);
    expect(serialized.migrations).toHaveLength(3);
    expect(serialized.migrations[0].version).toBe(1);
    expect(serialized.migrations[0].description).toBe('Add email field');
    expect(serialized.migrations[0].metadata?.author).toBe('test');
  });

  it('should track migration count in results', () => {
    runner.registerAll([
      createBasicMigrationV1(),
      createBasicMigrationV2(),
      createBasicMigrationV3(),
    ]);

    const doc = { _id: '1', name: 'Alice' };
    const result = runner.migrateDocument(doc, 0, 3);

    expect(result.migrationsApplied).toBe(3);
  });

  it('should validate sequential migration versions', () => {
    runner.register(createBasicMigrationV1());
    runner.register(createBasicMigrationV3()); // Skip V2

    const errors = runner.validateMigrations();
    expect(errors).toContain('Gap in migration versions: 1 to 3');
  });

  it('should validate migrations start at version 1', () => {
    runner.register(createBasicMigrationV2()); // Start at V2

    const errors = runner.validateMigrations();
    expect(errors).toContain('Migrations should start at version 1, but first is 2');
  });

  it('should pass validation for properly ordered migrations', () => {
    runner.registerAll([
      createBasicMigrationV1(),
      createBasicMigrationV2(),
      createBasicMigrationV3(),
    ]);

    const errors = runner.validateMigrations();
    expect(errors).toEqual([]);
  });
});

// ============================================================================
// Rollback on Failure Tests
// ============================================================================

describe('Rollback on Failure', () => {
  let runner: MigrationRunner;

  beforeEach(() => {
    runner = createMigrationRunner('users');
  });

  it('should return failure result when migration throws', () => {
    runner.register(createFailingMigration(1));

    const doc = { _id: '1', name: 'Alice' };
    const result = runner.migrateDocument(doc, 0, 1);

    expect(result.success).toBe(false);
    expect(result.error).toContain('Migration V1 failed intentionally');
  });

  it('should preserve document state at failure point', () => {
    runner.register(createBasicMigrationV1());
    runner.register(createFailingMigration(2));
    runner.register(createBasicMigrationV3());

    const doc = { _id: '1', name: 'Alice' };
    const result = runner.migrateDocument(doc, 0, 3);

    expect(result.success).toBe(false);
    expect(result.migrationsApplied).toBe(1); // V1 succeeded before V2 failed
    expect(result.document.email).toBe('unknown@example.com'); // V1 was applied
  });

  it('should report correct migration count before failure', () => {
    runner.registerAll([
      createBasicMigrationV1(),
      createBasicMigrationV2(),
      createFailingMigration(3),
      createBasicMigrationV4(),
    ]);

    const doc = { _id: '1', name: 'Alice' };
    const result = runner.migrateDocument(doc, 0, 4);

    expect(result.success).toBe(false);
    expect(result.migrationsApplied).toBe(2); // V1 and V2 succeeded
    expect(result.error).toContain('Migration V3 failed');
  });

  it('should stop batch migration on error when configured', () => {
    runner.register(createBasicMigrationV1());

    // Create failing runner for batch test
    const failRunner = createMigrationRunner('batch-test');
    failRunner.register({
      version: 1,
      description: 'Fail for specific doc',
      up: (doc) => {
        if (doc._id === 'fail') {
          throw new Error('Intentional failure');
        }
        return { ...doc, migrated: true };
      },
      down: (doc) => doc,
    });

    const docs = [
      { _id: '1', name: 'Alice' },
      { _id: 'fail', name: 'Bob' },
      { _id: '3', name: 'Charlie' },
    ];

    const result = failRunner.migrateBatch(docs, 0, 1, { stopOnError: true });

    expect(result.succeeded).toBe(1);
    expect(result.failed).toBe(1);
    // Third doc not processed due to stopOnError
    expect(result.total).toBe(3);
  });

  it('should continue batch migration on error by default', () => {
    const failRunner = createMigrationRunner('batch-test');
    failRunner.register({
      version: 1,
      description: 'Fail for specific doc',
      up: (doc) => {
        if (doc._id === 'fail') {
          throw new Error('Intentional failure');
        }
        return { ...doc, migrated: true };
      },
      down: (doc) => doc,
    });

    const docs = [
      { _id: '1', name: 'Alice' },
      { _id: 'fail', name: 'Bob' },
      { _id: '3', name: 'Charlie' },
    ];

    const result = failRunner.migrateBatch(docs, 0, 1);

    expect(result.succeeded).toBe(2);
    expect(result.failed).toBe(1);
    expect(result.results).toHaveLength(1); // Only failures in results by default
    expect(result.results[0].error).toContain('Intentional failure');
  });

  it('should handle errors in down migrations', () => {
    const failDownRunner = createMigrationRunner('fail-down');
    failDownRunner.register({
      version: 1,
      description: 'Fail on rollback',
      up: addField('testField', 'value'),
      down: (_doc) => {
        throw new Error('Rollback failed');
      },
    });

    const doc = { _id: '1', testField: 'value', _schemaVersion: 1 };
    const result = failDownRunner.migrateDocument(doc, 1, 0);

    expect(result.success).toBe(false);
    expect(result.error).toContain('Rollback failed');
  });
});

// ============================================================================
// Skip Already-Applied Migrations Tests
// ============================================================================

describe('Skip Already-Applied Migrations', () => {
  let runner: MigrationRunner;

  beforeEach(() => {
    runner = createMigrationRunner('users');
    runner.registerAll([
      createBasicMigrationV1(),
      createBasicMigrationV2(),
      createBasicMigrationV3(),
      createBasicMigrationV4(),
      createBasicMigrationV5(),
    ]);
  });

  it('should skip migration when document is already at target version', () => {
    const doc = { _id: '1', name: 'Alice', _schemaVersion: 3 };
    const result = runner.migrateDocument(doc, 3, 3);

    expect(result.success).toBe(true);
    expect(result.migrationsApplied).toBe(0);
  });

  it('should skip already-applied migrations when migrating forward', () => {
    const doc = {
      _id: '1',
      fullName: 'Alice',
      email: 'alice@test.com',
      _schemaVersion: 2,
    };
    const result = runner.migrateDocument(doc, 2, 5);

    expect(result.success).toBe(true);
    expect(result.migrationsApplied).toBe(3); // Only V3, V4, V5
    expect(result.fromVersion).toBe(2);
  });

  it('should detect when migration is needed', () => {
    const docNeedsMigration = { _id: '1', _schemaVersion: 1 };
    const docUpToDate = { _id: '2', _schemaVersion: 5 };

    expect(runner.needsMigration(docNeedsMigration)).toBe(true);
    expect(runner.needsMigration(docUpToDate)).toBe(false);
  });

  it('should detect migration needed for specific target version', () => {
    const doc = { _id: '1', _schemaVersion: 2 };

    expect(runner.needsMigration(doc, 3)).toBe(true);
    expect(runner.needsMigration(doc, 2)).toBe(false);
    expect(runner.needsMigration(doc, 1)).toBe(true); // Rollback needed
  });

  it('should not modify document when no migration is needed', () => {
    const originalDoc = {
      _id: '1',
      fullName: 'Alice',
      email: 'alice@test.com',
      createdAt: new Date('2024-01-01'),
      _schemaVersion: 3,
    };
    const docCopy = { ...originalDoc };

    const result = runner.migrateDocument(docCopy, 3, 3);

    expect(result.migrationsApplied).toBe(0);
    // Only _schemaVersion and _migratedAt should be set
    expect(result.document._schemaVersion).toBe(3);
  });

  it('should handle batch with mixed version documents', () => {
    const docs = [
      { _id: '1', name: 'Alice', _schemaVersion: 0 },
      { _id: '2', name: 'Bob', email: 'bob@test.com', _schemaVersion: 1 },
      { _id: '3', fullName: 'Charlie', email: 'charlie@test.com', _schemaVersion: 2 },
    ];

    const results = runner.migrateBatch(docs, undefined, 2, { detailed: true });

    expect(results.succeeded).toBe(3);
    // Doc 1: 0 -> 2 = 2 migrations
    // Doc 2: 1 -> 2 = 1 migration
    // Doc 3: 2 -> 2 = 0 migrations
    expect(results.results[0].migrationsApplied).toBe(2);
    expect(results.results[1].migrationsApplied).toBe(1);
    expect(results.results[2].migrationsApplied).toBe(0);
  });
});

// ============================================================================
// Migration Ordering Tests
// ============================================================================

describe('Migration Ordering', () => {
  let runner: MigrationRunner;

  beforeEach(() => {
    runner = createMigrationRunner('users');
  });

  it('should apply migrations in ascending order when upgrading', () => {
    const executionOrder: number[] = [];

    runner.register({
      version: 1,
      description: 'V1',
      up: (doc) => { executionOrder.push(1); return { ...doc, v1: true }; },
      down: (doc) => doc,
    });
    runner.register({
      version: 2,
      description: 'V2',
      up: (doc) => { executionOrder.push(2); return { ...doc, v2: true }; },
      down: (doc) => doc,
    });
    runner.register({
      version: 3,
      description: 'V3',
      up: (doc) => { executionOrder.push(3); return { ...doc, v3: true }; },
      down: (doc) => doc,
    });

    const doc = { _id: '1' };
    runner.migrateDocument(doc, 0, 3);

    expect(executionOrder).toEqual([1, 2, 3]);
  });

  it('should apply migrations in descending order when downgrading', () => {
    const executionOrder: number[] = [];

    runner.register({
      version: 1,
      description: 'V1',
      up: (doc) => doc,
      down: (doc) => { executionOrder.push(1); return doc; },
    });
    runner.register({
      version: 2,
      description: 'V2',
      up: (doc) => doc,
      down: (doc) => { executionOrder.push(2); return doc; },
    });
    runner.register({
      version: 3,
      description: 'V3',
      up: (doc) => doc,
      down: (doc) => { executionOrder.push(3); return doc; },
    });

    const doc = { _id: '1', _schemaVersion: 3 };
    runner.migrateDocument(doc, 3, 0);

    expect(executionOrder).toEqual([3, 2, 1]);
  });

  it('should register migrations out of order and execute in correct order', () => {
    const executionOrder: number[] = [];

    // Register out of order
    runner.register({
      version: 3,
      description: 'V3',
      up: (doc) => { executionOrder.push(3); return { ...doc, v3: true }; },
      down: (doc) => doc,
    });
    runner.register({
      version: 1,
      description: 'V1',
      up: (doc) => { executionOrder.push(1); return { ...doc, v1: true }; },
      down: (doc) => doc,
    });
    runner.register({
      version: 2,
      description: 'V2',
      up: (doc) => { executionOrder.push(2); return { ...doc, v2: true }; },
      down: (doc) => doc,
    });

    const doc = { _id: '1' };
    runner.migrateDocument(doc, 0, 3);

    expect(executionOrder).toEqual([1, 2, 3]);
    expect(runner.getMigrationVersions()).toEqual([1, 2, 3]);
  });

  it('should correctly skip intermediate versions when migrating', () => {
    const executionOrder: number[] = [];

    for (let i = 1; i <= 5; i++) {
      runner.register({
        version: i,
        description: `V${i}`,
        up: (doc) => { executionOrder.push(i); return { ...doc, [`v${i}`]: true }; },
        down: (doc) => { executionOrder.push(-i); return doc; },
      });
    }

    const doc = { _id: '1', _schemaVersion: 2 };
    runner.migrateDocument(doc, 2, 4);

    // Only V3 and V4 should be applied
    expect(executionOrder).toEqual([3, 4]);
  });

  it('should handle composite migrations that depend on previous state', () => {
    // Migration that depends on previous migration's result
    runner.register({
      version: 1,
      description: 'Add firstName and lastName',
      up: compose([
        addField('firstName', ''),
        addField('lastName', ''),
      ]),
      down: compose([
        removeField('firstName'),
        removeField('lastName'),
      ]),
    });

    runner.register({
      version: 2,
      description: 'Combine into fullName',
      up: (doc) => {
        const fullName = `${doc.firstName} ${doc.lastName}`.trim();
        const result = { ...doc, fullName };
        delete result.firstName;
        delete result.lastName;
        return result;
      },
      down: (doc) => {
        const parts = ((doc.fullName as string) || '').split(' ');
        const result = {
          ...doc,
          firstName: parts[0] || '',
          lastName: parts.slice(1).join(' ') || '',
        };
        delete result.fullName;
        return result;
      },
    });

    // Forward migration
    const doc = { _id: '1' };
    let result = runner.migrateDocument(doc, 0, 2);

    expect(result.document.fullName).toBe('');
    expect(result.document.firstName).toBeUndefined();
    expect(result.document.lastName).toBeUndefined();

    // Test with actual names
    const docWithNames = { _id: '2', firstName: 'John', lastName: 'Doe', _schemaVersion: 1 };
    result = runner.migrateDocument(docWithNames, 1, 2);
    expect(result.document.fullName).toBe('John Doe');

    // Rollback
    result = runner.migrateDocument(result.document, 2, 1);
    expect(result.document.firstName).toBe('John');
    expect(result.document.lastName).toBe('Doe');
    expect(result.document.fullName).toBeUndefined();
  });

  it('should not apply migrations for versions beyond target', () => {
    const executionOrder: number[] = [];

    for (let i = 1; i <= 5; i++) {
      runner.register({
        version: i,
        description: `V${i}`,
        up: (doc) => { executionOrder.push(i); return { ...doc, [`v${i}`]: true }; },
        down: (doc) => doc,
      });
    }

    const doc = { _id: '1' };
    runner.migrateDocument(doc, 0, 3);

    // Only V1, V2, V3 should be applied, not V4 or V5
    expect(executionOrder).toEqual([1, 2, 3]);
  });
});

// ============================================================================
// Additional Edge Cases
// ============================================================================

describe('Edge Cases', () => {
  let runner: MigrationRunner;

  beforeEach(() => {
    runner = createMigrationRunner('users');
  });

  it('should reject duplicate migration versions', () => {
    runner.register(createBasicMigrationV1());

    expect(() => runner.register(createBasicMigrationV1())).toThrow(
      'Migration version 1 is already registered'
    );
  });

  it('should reject non-positive migration versions', () => {
    const invalidMigration = { ...createBasicMigrationV1(), version: 0 };

    expect(() => runner.register(invalidMigration)).toThrow(
      'Migration version must be positive'
    );

    const negativeMigration = { ...createBasicMigrationV1(), version: -1 };
    expect(() => runner.register(negativeMigration)).toThrow(
      'Migration version must be positive'
    );
  });

  it('should handle negative target versions gracefully', () => {
    runner.register(createBasicMigrationV1());

    const doc = { _id: '1', name: 'Alice' };
    const result = runner.migrateDocument(doc, 0, -1);

    expect(result.success).toBe(false);
    expect(result.error).toContain('non-negative');
  });

  it('should handle empty document', () => {
    runner.register(createBasicMigrationV1());

    const doc = {};
    const result = runner.migrateDocument(doc, 0, 1);

    expect(result.success).toBe(true);
    expect(result.document.email).toBe('unknown@example.com');
  });

  it('should handle conditional migrations', () => {
    runner.register({
      version: 1,
      description: 'Add premium flag for high-value users',
      up: conditional(
        (doc) => (doc.totalPurchases as number) > 1000,
        addField('isPremium', true)
      ),
      down: removeField('isPremium'),
    });

    const highValueDoc = { _id: '1', totalPurchases: 1500 };
    const lowValueDoc = { _id: '2', totalPurchases: 500 };

    const result1 = runner.migrateDocument(highValueDoc, 0, 1);
    const result2 = runner.migrateDocument(lowValueDoc, 0, 1);

    expect(result1.document.isPremium).toBe(true);
    expect(result2.document.isPremium).toBeUndefined();
  });

  it('should handle batch migration with progress callback', () => {
    runner.register(createBasicMigrationV1());

    const docs = [
      { _id: '1', name: 'Alice' },
      { _id: '2', name: 'Bob' },
      { _id: '3', name: 'Charlie' },
    ];

    const progressCalls: Array<{ processed: number; total: number }> = [];

    runner.migrateBatch(docs, 0, 1, {
      onProgress: (processed, total) => {
        progressCalls.push({ processed, total });
      },
    });

    expect(progressCalls).toEqual([
      { processed: 1, total: 3 },
      { processed: 2, total: 3 },
      { processed: 3, total: 3 },
    ]);
  });

  it('should use defineMigration helper for type checking', () => {
    const migration = defineMigration({
      version: 1,
      description: 'Test migration',
      up: addField('test', true),
      down: removeField('test'),
      metadata: {
        author: 'test',
        notes: 'This is a test migration',
      },
    });

    expect(migration.version).toBe(1);
    expect(migration.description).toBe('Test migration');
    expect(migration.metadata?.notes).toBe('This is a test migration');
  });
});
