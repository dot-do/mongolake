/**
 * Built-in Plugins Tests
 *
 * Tests for the built-in plugin implementations.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  PluginRegistry,
  definePlugin,
  type CollectionHookContext,
} from '../../../src/plugin/index.js';
import {
  createTimestampsPlugin,
  timestampsPlugin,
} from '../../../src/plugin/builtin/timestamps.js';
import {
  createSoftDeletePlugin,
  softDeletePlugin,
} from '../../../src/plugin/builtin/soft-delete.js';
import {
  createAuditTrailPlugin,
  auditTrailPlugin,
  type AuditEntry,
} from '../../../src/plugin/builtin/audit-trail.js';
import {
  createValidationPlugin,
  validationPlugin,
  ValidationError,
} from '../../../src/plugin/builtin/validation.js';
import type { Document, Filter, Update } from '../../../src/types.js';

describe('Timestamps Plugin', () => {
  let registry: PluginRegistry;

  beforeEach(async () => {
    registry = new PluginRegistry();
  });

  afterEach(async () => {
    await registry.destroy();
  });

  it('should add createdAt and updatedAt on insert', async () => {
    await registry.register(timestampsPlugin);
    await registry.init();

    const docs = [{ name: 'Alice' }, { name: 'Bob' }];

    const { result } = await registry.executeHook('collection:beforeInsert', docs, {
      database: 'test',
      collection: 'users',
    });

    expect(result).toHaveLength(2);
    expect((result[0] as Record<string, unknown>).createdAt).toBeInstanceOf(Date);
    expect((result[0] as Record<string, unknown>).updatedAt).toBeInstanceOf(Date);
    expect((result[1] as Record<string, unknown>).createdAt).toBeInstanceOf(Date);
    expect((result[1] as Record<string, unknown>).updatedAt).toBeInstanceOf(Date);
  });

  it('should not overwrite existing timestamps', async () => {
    await registry.register(timestampsPlugin);
    await registry.init();

    const existingDate = new Date('2020-01-01');
    const docs = [{ name: 'Alice', createdAt: existingDate }];

    const { result } = await registry.executeHook('collection:beforeInsert', docs, {
      database: 'test',
      collection: 'users',
    });

    expect((result[0] as Record<string, unknown>).createdAt).toBe(existingDate);
    expect((result[0] as Record<string, unknown>).updatedAt).toBeInstanceOf(Date);
  });

  it('should update updatedAt on update', async () => {
    await registry.register(timestampsPlugin);
    await registry.init();

    const params = {
      filter: { _id: '123' },
      update: { $set: { name: 'Updated' } } as Update<Document>,
    };

    const { result } = await registry.executeHook('collection:beforeUpdate', params, {
      database: 'test',
      collection: 'users',
    });

    const typedResult = result as { filter: unknown; update: Update<Document> };
    expect(typedResult.update.$set).toBeDefined();
    expect((typedResult.update.$set as Record<string, unknown>).updatedAt).toBeInstanceOf(Date);
  });

  it('should use custom field names', async () => {
    const customPlugin = createTimestampsPlugin({
      createdAt: '_created',
      updatedAt: '_modified',
    });

    await registry.register(customPlugin);
    await registry.init();

    const docs = [{ name: 'Alice' }];

    const { result } = await registry.executeHook('collection:beforeInsert', docs, {
      database: 'test',
      collection: 'users',
    });

    const doc = result[0] as Record<string, unknown>;
    expect(doc._created).toBeInstanceOf(Date);
    expect(doc._modified).toBeInstanceOf(Date);
    expect(doc.createdAt).toBeUndefined();
    expect(doc.updatedAt).toBeUndefined();
  });

  it('should exclude collections', async () => {
    const plugin = createTimestampsPlugin({
      excludeCollections: ['logs'],
    });

    await registry.register(plugin);
    await registry.init();

    const docs = [{ message: 'log entry' }];

    const { result } = await registry.executeHook('collection:beforeInsert', docs, {
      database: 'test',
      collection: 'logs',
    });

    expect((result[0] as Record<string, unknown>).createdAt).toBeUndefined();
  });

  it('should only include specified collections', async () => {
    const plugin = createTimestampsPlugin({
      includeCollections: ['users'],
    });

    await registry.register(plugin);
    await registry.init();

    // Should add timestamps
    const userDocs = [{ name: 'Alice' }];
    const { result: userResult } = await registry.executeHook('collection:beforeInsert', userDocs, {
      database: 'test',
      collection: 'users',
    });
    expect((userResult[0] as Record<string, unknown>).createdAt).toBeInstanceOf(Date);

    // Should not add timestamps
    const orderDocs = [{ item: 'Widget' }];
    const { result: orderResult } = await registry.executeHook('collection:beforeInsert', orderDocs, {
      database: 'test',
      collection: 'orders',
    });
    expect((orderResult[0] as Record<string, unknown>).createdAt).toBeUndefined();
  });

  it('should disable createdAt field', async () => {
    const plugin = createTimestampsPlugin({
      createdAt: false,
    });

    await registry.register(plugin);
    await registry.init();

    const docs = [{ name: 'Alice' }];

    const { result } = await registry.executeHook('collection:beforeInsert', docs, {
      database: 'test',
      collection: 'users',
    });

    const doc = result[0] as Record<string, unknown>;
    expect(doc.createdAt).toBeUndefined();
    expect(doc.updatedAt).toBeInstanceOf(Date);
  });
});

describe('Soft Delete Plugin', () => {
  let registry: PluginRegistry;

  beforeEach(async () => {
    registry = new PluginRegistry();
  });

  afterEach(async () => {
    await registry.destroy();
  });

  it('should add deletedAt filter to find queries', async () => {
    await registry.register(softDeletePlugin);
    await registry.init();

    const params = {
      filter: { status: 'active' },
      options: {},
    };

    const { result } = await registry.executeHook('collection:beforeFind', params, {
      database: 'test',
      collection: 'users',
    });

    const typedResult = result as { filter: Filter<Document>; options: unknown };
    expect((typedResult.filter as Record<string, unknown>).deletedAt).toEqual({ $exists: false });
    expect((typedResult.filter as Record<string, unknown>).status).toBe('active');
  });

  it('should include deleted documents when _includeDeleted is true', async () => {
    await registry.register(softDeletePlugin);
    await registry.init();

    const params = {
      filter: { status: 'active', _includeDeleted: true },
      options: {},
    };

    const { result } = await registry.executeHook('collection:beforeFind', params, {
      database: 'test',
      collection: 'users',
    });

    const typedResult = result as { filter: Filter<Document>; options: unknown };
    expect((typedResult.filter as Record<string, unknown>).deletedAt).toBeUndefined();
    expect((typedResult.filter as Record<string, unknown>)._includeDeleted).toBeUndefined();
    expect((typedResult.filter as Record<string, unknown>).status).toBe('active');
  });

  it('should add $match stage to aggregation pipeline', async () => {
    await registry.register(softDeletePlugin);
    await registry.init();

    const pipeline = [{ $match: { status: 'active' } }];

    const { result } = await registry.executeHook('collection:beforeAggregate', pipeline, {
      database: 'test',
      collection: 'users',
    });

    const typedResult = result as Array<Record<string, unknown>>;
    expect(typedResult).toHaveLength(2);
    expect(typedResult[0].$match).toEqual({ deletedAt: { $exists: false } });
    expect(typedResult[1].$match).toEqual({ status: 'active' });
  });

  it('should use custom field name', async () => {
    const plugin = createSoftDeletePlugin({
      deletedAtField: '_removedAt',
    });

    await registry.register(plugin);
    await registry.init();

    const params = {
      filter: { status: 'active' },
      options: {},
    };

    const { result } = await registry.executeHook('collection:beforeFind', params, {
      database: 'test',
      collection: 'users',
    });

    const typedResult = result as { filter: Filter<Document>; options: unknown };
    expect((typedResult.filter as Record<string, unknown>)._removedAt).toEqual({ $exists: false });
    expect((typedResult.filter as Record<string, unknown>).deletedAt).toBeUndefined();
  });
});

describe('Audit Trail Plugin', () => {
  let registry: PluginRegistry;

  beforeEach(async () => {
    registry = new PluginRegistry();
  });

  afterEach(async () => {
    await registry.destroy();
  });

  it('should call audit handler on insert', async () => {
    const auditEntries: AuditEntry[] = [];

    const plugin = createAuditTrailPlugin({
      auditHandler: (entry) => {
        auditEntries.push(entry);
      },
    });

    await registry.register(plugin);
    await registry.init();

    const result = {
      insertedIds: { 0: 'id1', 1: 'id2' },
      insertedCount: 2,
    };

    await registry.executeHook('collection:afterInsert', result, {
      database: 'test',
      collection: 'users',
      timestamp: new Date(),
    });

    expect(auditEntries).toHaveLength(1);
    expect(auditEntries[0].operation).toBe('insert');
    expect(auditEntries[0].collection).toBe('users');
    expect(auditEntries[0].documentIds).toEqual(['id1', 'id2']);
    expect(auditEntries[0].affectedCount).toBe(2);
  });

  it('should call audit handler on update', async () => {
    const auditEntries: AuditEntry[] = [];

    const plugin = createAuditTrailPlugin({
      auditHandler: (entry) => {
        auditEntries.push(entry);
      },
    });

    await registry.register(plugin);
    await registry.init();

    const result = {
      matchedCount: 3,
      modifiedCount: 2,
    };

    await registry.executeHook('collection:afterUpdate', result, {
      database: 'test',
      collection: 'users',
      timestamp: new Date(),
      filter: { status: 'pending' },
    });

    expect(auditEntries).toHaveLength(1);
    expect(auditEntries[0].operation).toBe('update');
    expect(auditEntries[0].affectedCount).toBe(2);
    expect(auditEntries[0].filter).toEqual({ status: 'pending' });
  });

  it('should call audit handler on delete', async () => {
    const auditEntries: AuditEntry[] = [];

    const plugin = createAuditTrailPlugin({
      auditHandler: (entry) => {
        auditEntries.push(entry);
      },
    });

    await registry.register(plugin);
    await registry.init();

    const result = {
      deletedCount: 5,
    };

    await registry.executeHook('collection:afterDelete', result, {
      database: 'test',
      collection: 'users',
      timestamp: new Date(),
      filter: { archived: true },
    });

    expect(auditEntries).toHaveLength(1);
    expect(auditEntries[0].operation).toBe('delete');
    expect(auditEntries[0].affectedCount).toBe(5);
    expect(auditEntries[0].filter).toEqual({ archived: true });
  });

  it('should not audit the audit collection', async () => {
    const auditEntries: AuditEntry[] = [];

    const plugin = createAuditTrailPlugin({
      auditCollection: '_audit',
      auditHandler: (entry) => {
        auditEntries.push(entry);
      },
    });

    await registry.register(plugin);
    await registry.init();

    const result = {
      insertedIds: { 0: 'id1' },
      insertedCount: 1,
    };

    await registry.executeHook('collection:afterInsert', result, {
      database: 'test',
      collection: '_audit',
      timestamp: new Date(),
    });

    expect(auditEntries).toHaveLength(0);
  });

  it('should include user context', async () => {
    const auditEntries: AuditEntry[] = [];

    const plugin = createAuditTrailPlugin({
      includeUser: true,
      auditHandler: (entry) => {
        auditEntries.push(entry);
      },
    });

    await registry.register(plugin);
    await registry.init();

    const result = {
      insertedIds: { 0: 'id1' },
      insertedCount: 1,
    };

    await registry.executeHook('collection:afterInsert', result, {
      database: 'test',
      collection: 'users',
      timestamp: new Date(),
      user: {
        userId: 'user123',
        email: 'test@example.com',
        roles: ['admin'],
      },
    });

    expect(auditEntries).toHaveLength(1);
    expect(auditEntries[0].user).toEqual({
      userId: 'user123',
      email: 'test@example.com',
      roles: ['admin'],
    });
  });
});

describe('Validation Plugin', () => {
  let registry: PluginRegistry;

  beforeEach(async () => {
    registry = new PluginRegistry();
  });

  afterEach(async () => {
    await registry.destroy();
  });

  it('should validate required fields', async () => {
    const plugin = createValidationPlugin({
      schemas: {
        users: {
          type: 'object',
          required: ['email', 'name'],
          properties: {
            email: { type: 'string' },
            name: { type: 'string' },
          },
        },
      },
    });

    await registry.register(plugin);
    await registry.init();

    const docs = [{ name: 'Alice' }]; // Missing email

    await expect(
      registry.executeHook('collection:beforeInsert', docs, {
        database: 'test',
        collection: 'users',
      })
    ).rejects.toThrow(ValidationError);
  });

  it('should pass valid documents', async () => {
    const plugin = createValidationPlugin({
      schemas: {
        users: {
          type: 'object',
          required: ['email', 'name'],
          properties: {
            email: { type: 'string' },
            name: { type: 'string' },
          },
        },
      },
    });

    await registry.register(plugin);
    await registry.init();

    const docs = [{ name: 'Alice', email: 'alice@example.com' }];

    const { result } = await registry.executeHook('collection:beforeInsert', docs, {
      database: 'test',
      collection: 'users',
    });

    expect(result).toEqual(docs);
  });

  it('should validate string minLength', async () => {
    const plugin = createValidationPlugin({
      schemas: {
        users: {
          type: 'object',
          properties: {
            name: { type: 'string', minLength: 2 },
          },
        },
      },
    });

    await registry.register(plugin);
    await registry.init();

    const docs = [{ name: 'A' }]; // Too short

    await expect(
      registry.executeHook('collection:beforeInsert', docs, {
        database: 'test',
        collection: 'users',
      })
    ).rejects.toThrow('must be at least 2 characters');
  });

  it('should validate number minimum', async () => {
    const plugin = createValidationPlugin({
      schemas: {
        users: {
          type: 'object',
          properties: {
            age: { type: 'number', minimum: 0 },
          },
        },
      },
    });

    await registry.register(plugin);
    await registry.init();

    const docs = [{ age: -5 }];

    await expect(
      registry.executeHook('collection:beforeInsert', docs, {
        database: 'test',
        collection: 'users',
      })
    ).rejects.toThrow('must be at least 0');
  });

  it('should validate email format', async () => {
    const plugin = createValidationPlugin({
      schemas: {
        users: {
          type: 'object',
          properties: {
            email: { type: 'string', format: 'email' },
          },
        },
      },
    });

    await registry.register(plugin);
    await registry.init();

    const docs = [{ email: 'not-an-email' }];

    await expect(
      registry.executeHook('collection:beforeInsert', docs, {
        database: 'test',
        collection: 'users',
      })
    ).rejects.toThrow('must be a valid email');
  });

  it('should validate enum values', async () => {
    const plugin = createValidationPlugin({
      schemas: {
        orders: {
          type: 'object',
          properties: {
            status: { enum: ['pending', 'shipped', 'delivered'] },
          },
        },
      },
    });

    await registry.register(plugin);
    await registry.init();

    const docs = [{ status: 'unknown' }];

    await expect(
      registry.executeHook('collection:beforeInsert', docs, {
        database: 'test',
        collection: 'orders',
      })
    ).rejects.toThrow('must be one of');
  });

  it('should use custom validator', async () => {
    const plugin = createValidationPlugin({
      validators: {
        users: (doc) => {
          if (doc.password && (doc.password as string).length < 8) {
            return { valid: false, errors: ['Password must be at least 8 characters'] };
          }
          return { valid: true };
        },
      },
    });

    await registry.register(plugin);
    await registry.init();

    const docs = [{ password: 'short' }];

    await expect(
      registry.executeHook('collection:beforeInsert', docs, {
        database: 'test',
        collection: 'users',
      })
    ).rejects.toThrow('Password must be at least 8 characters');
  });

  it('should skip validation for unconfigured collections', async () => {
    const plugin = createValidationPlugin({
      schemas: {
        users: {
          type: 'object',
          required: ['email'],
        },
      },
    });

    await registry.register(plugin);
    await registry.init();

    // Orders collection has no schema
    const docs = [{ item: 'Widget' }];

    const { result } = await registry.executeHook('collection:beforeInsert', docs, {
      database: 'test',
      collection: 'orders',
    });

    expect(result).toEqual(docs);
  });

  it('should call custom error handler', async () => {
    const errorHandler = vi.fn();

    const plugin = createValidationPlugin({
      schemas: {
        users: {
          type: 'object',
          required: ['email'],
        },
      },
      onError: errorHandler,
    });

    await registry.register(plugin);
    await registry.init();

    const docs = [{ name: 'Alice' }];

    try {
      await registry.executeHook('collection:beforeInsert', docs, {
        database: 'test',
        collection: 'users',
      });
    } catch {
      // Expected to throw
    }

    expect(errorHandler).toHaveBeenCalledWith('users', docs[0], expect.any(Array));
  });

  it('should validate $set fields on update', async () => {
    const plugin = createValidationPlugin({
      schemas: {
        users: {
          type: 'object',
          properties: {
            age: { type: 'number', minimum: 0 },
          },
        },
      },
    });

    await registry.register(plugin);
    await registry.init();

    const params = {
      filter: { _id: '123' },
      update: { $set: { age: -10 } },
    };

    await expect(
      registry.executeHook('collection:beforeUpdate', params, {
        database: 'test',
        collection: 'users',
      })
    ).rejects.toThrow('must be at least 0');
  });
});
