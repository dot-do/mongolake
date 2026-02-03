/**
 * Plugin System Integration Tests
 *
 * Tests that verify the plugin system integrates correctly with
 * the rest of the MongoLake ecosystem.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  PluginRegistry,
  definePlugin,
  composePlugins,
  getGlobalRegistry,
  resetGlobalRegistry,
  type Plugin,
  type PluginContext,
  type CollectionHookContext,
} from '../../../src/plugin/index.js';
import type { Document, Update, Filter } from '../../../src/types.js';

describe('Plugin System Integration', () => {
  describe('multiple plugins working together', () => {
    let registry: PluginRegistry;

    beforeEach(async () => {
      registry = new PluginRegistry();
    });

    afterEach(async () => {
      await registry.destroy();
    });

    it('should chain timestamps and validation plugins', async () => {
      // Timestamps plugin - adds createdAt/updatedAt
      const timestampsPlugin = definePlugin({
        name: 'timestamps',
        version: '1.0.0',
        hooks: {
          'collection:beforeInsert': async (docs: Document[]) => {
            const now = new Date();
            return docs.map((doc) => ({
              ...doc,
              createdAt: now,
              updatedAt: now,
            }));
          },
        },
      });

      // Validation plugin - ensures email is present
      const validationPlugin = definePlugin({
        name: 'validation',
        version: '1.0.0',
        dependencies: ['timestamps'], // Run after timestamps
        hooks: {
          'collection:beforeInsert': async (docs: Document[]) => {
            for (const doc of docs) {
              // Ensure timestamps were added
              if (!doc.createdAt) {
                throw new Error('Timestamps should have been added');
              }
              // Validate email
              if (doc.email && typeof doc.email === 'string') {
                if (!doc.email.includes('@')) {
                  throw new Error('Invalid email format');
                }
              }
            }
            return docs;
          },
        },
      });

      await registry.register(timestampsPlugin);
      await registry.register(validationPlugin);
      await registry.init();

      const docs = [{ name: 'Alice', email: 'alice@example.com' }];

      const { result } = await registry.executeHook('collection:beforeInsert', docs, {
        database: 'test',
        collection: 'users',
      });

      // Should have both timestamps and original data
      expect(result).toHaveLength(1);
      expect((result[0] as Document).name).toBe('Alice');
      expect((result[0] as Document).email).toBe('alice@example.com');
      expect((result[0] as Document).createdAt).toBeInstanceOf(Date);
      expect((result[0] as Document).updatedAt).toBeInstanceOf(Date);
    });

    it('should allow plugins to stop the chain', async () => {
      // Access control plugin - can deny operations
      const accessControlPlugin = definePlugin({
        name: 'access-control',
        version: '1.0.0',
        hooks: {
          'collection:beforeDelete': async (
            filter: Filter<Document>,
            context: CollectionHookContext
          ) => {
            // Prevent deletion of admin users
            const filterObj = filter as Record<string, unknown>;
            if (filterObj.role === 'admin') {
              return { stop: true, result: { denied: true, reason: 'Cannot delete admins' } };
            }
            return filter;
          },
        },
      });

      // Audit plugin - logs all operations
      let auditCalled = false;
      const auditPlugin = definePlugin({
        name: 'audit',
        version: '1.0.0',
        hooks: {
          'collection:beforeDelete': async (filter: Filter<Document>) => {
            auditCalled = true;
            return filter;
          },
        },
      });

      await registry.register(accessControlPlugin);
      await registry.register(auditPlugin);
      await registry.init();

      // Try to delete admin - should be stopped
      const adminFilter = { role: 'admin' };
      const { result, stopped } = await registry.executeHook('collection:beforeDelete', adminFilter, {
        database: 'test',
        collection: 'users',
      });

      expect(stopped).toBe(true);
      expect((result as Record<string, unknown>).denied).toBe(true);
      expect(auditCalled).toBe(false); // Audit should not have run

      // Reset audit flag
      auditCalled = false;

      // Try to delete regular user - should proceed
      const userFilter = { role: 'user' };
      const { result: result2, stopped: stopped2 } = await registry.executeHook('collection:beforeDelete', userFilter, {
        database: 'test',
        collection: 'users',
      });

      expect(stopped2).toBe(false);
      expect(result2).toEqual(userFilter);
      expect(auditCalled).toBe(true); // Audit should have run
    });

    it('should compose multiple plugins into a bundle', async () => {
      const results: string[] = [];

      const plugin1 = definePlugin({
        name: 'plugin-1',
        version: '1.0.0',
        init: async () => {
          results.push('init-1');
        },
        hooks: {
          'collection:beforeInsert': async (docs: Document[]) => {
            results.push('hook-1');
            return docs;
          },
        },
      });

      const plugin2 = definePlugin({
        name: 'plugin-2',
        version: '1.0.0',
        init: async () => {
          results.push('init-2');
        },
        hooks: {
          'collection:beforeInsert': async (docs: Document[]) => {
            results.push('hook-2');
            return docs;
          },
        },
      });

      // Create composed plugin
      const bundle = composePlugins('my-bundle', [plugin1, plugin2]);

      await registry.register(bundle);
      await registry.init();

      expect(results).toContain('init-1');
      expect(results).toContain('init-2');

      // Execute hook
      const docs = [{ name: 'test' }];
      await registry.executeHook('collection:beforeInsert', docs, {
        database: 'test',
        collection: 'test',
      });

      expect(results).toContain('hook-1');
      expect(results).toContain('hook-2');
    });
  });

  describe('plugin capabilities', () => {
    let registry: PluginRegistry;

    beforeEach(async () => {
      registry = new PluginRegistry();
    });

    afterEach(async () => {
      await registry.destroy();
    });

    it('should support storage plugins', async () => {
      const mockStorage = {
        get: async () => null,
        put: async () => {},
        delete: async () => {},
        list: async () => [],
        exists: async () => false,
        head: async () => null,
        createMultipartUpload: async () => ({
          uploadPart: async () => ({ partNumber: 1, etag: '' }),
          complete: async () => {},
          abort: async () => {},
        }),
        getStream: async () => null,
        putStream: async () => {},
      };

      const storagePlugin = definePlugin({
        name: 's3-storage',
        version: '1.0.0',
        tags: ['storage', 'aws'],
        createStorage: (config) => {
          expect(config).toHaveProperty('bucket');
          return mockStorage;
        },
      });

      await registry.register(storagePlugin);

      const storagePlugins = registry.getStoragePlugins();
      expect(storagePlugins).toHaveLength(1);
      expect(storagePlugins[0].name).toBe('s3-storage');

      // Create storage instance
      const storage = storagePlugins[0].createStorage!({ bucket: 'my-bucket' });
      expect(storage).toBe(mockStorage);
    });

    it('should support auth provider plugins', async () => {
      const mockProvider = {
        name: 'oauth',
        issuer: 'https://auth.example.com',
        validateToken: async (token: string) => ({
          valid: token === 'valid-token',
          user: { userId: '123', roles: ['user'] },
        }),
      };

      const authPlugin = definePlugin({
        name: 'oauth-provider',
        version: '1.0.0',
        tags: ['auth', 'oauth'],
        createAuthProvider: () => mockProvider,
      });

      await registry.register(authPlugin);

      const authPlugins = registry.getAuthPlugins();
      expect(authPlugins).toHaveLength(1);

      const provider = authPlugins[0].createAuthProvider!({});
      const result = await provider.validateToken('valid-token');
      expect(result.valid).toBe(true);
    });

    it('should support schema plugins (mongoose-style)', async () => {
      interface MockSchema {
        methods: Record<string, Function>;
        statics: Record<string, Function>;
        pre: (hook: string, fn: Function) => void;
      }

      const schemaPlugin = definePlugin({
        name: 'auto-populate',
        version: '1.0.0',
        tags: ['schema', 'mongoose'],
        schemaPlugin: <T extends MockSchema>(schema: T, options?: Record<string, unknown>) => {
          // Add instance method
          schema.methods.populate = function () {
            return this;
          };
          // Add static method
          schema.statics.findAndPopulate = function () {
            return this;
          };
          // Add middleware
          schema.pre('find', function (this: unknown) {
            // Auto-populate on find
          });
        },
      });

      await registry.register(schemaPlugin);

      const schemaPlugins = registry.getSchemaPlugins();
      expect(schemaPlugins).toHaveLength(1);

      // Mock schema
      const mockSchema: MockSchema = {
        methods: {},
        statics: {},
        pre: () => {},
      };

      schemaPlugins[0].schemaPlugin!(mockSchema);
      expect(mockSchema.methods.populate).toBeDefined();
      expect(mockSchema.statics.findAndPopulate).toBeDefined();
    });
  });

  describe('plugin context and inter-plugin communication', () => {
    let registry: PluginRegistry;

    beforeEach(async () => {
      registry = new PluginRegistry();
    });

    afterEach(async () => {
      await registry.destroy();
    });

    it('should provide registry access in context', async () => {
      let capturedContext: PluginContext | null = null;

      const pluginA = definePlugin({
        name: 'plugin-a',
        version: '1.0.0',
        init: async (context) => {
          capturedContext = context;
        },
      });

      const pluginB = definePlugin({
        name: 'plugin-b',
        version: '1.0.0',
        dependencies: ['plugin-a'],
      });

      await registry.register(pluginA);
      await registry.register(pluginB);
      await registry.init();

      expect(capturedContext).not.toBeNull();
      expect(capturedContext!.registry.has('plugin-a')).toBe(true);
      expect(capturedContext!.registry.has('plugin-b')).toBe(true);
      expect(capturedContext!.log).toBeDefined();
    });

    it('should allow plugins to query other plugins', async () => {
      let queriedPlugin: Plugin | undefined;

      const pluginA = definePlugin({
        name: 'plugin-a',
        version: '1.0.0',
        tags: ['core'],
      });

      const pluginB = definePlugin({
        name: 'plugin-b',
        version: '1.0.0',
        dependencies: ['plugin-a'],
        init: async (context) => {
          // Query for plugin-a
          queriedPlugin = context.registry.get('plugin-a');

          // Query by tag
          const corePlugins = context.registry.byTag('core');
          expect(corePlugins).toHaveLength(1);
        },
      });

      await registry.register(pluginA);
      await registry.register(pluginB);
      await registry.init();

      expect(queriedPlugin).toBe(pluginA);
    });
  });

  describe('real-world scenarios', () => {
    let registry: PluginRegistry;

    beforeEach(async () => {
      registry = new PluginRegistry();
    });

    afterEach(async () => {
      await registry.destroy();
    });

    it('should implement rate limiting plugin', async () => {
      const requestCounts = new Map<string, number>();
      const rateLimit = 3;

      const rateLimitPlugin = definePlugin({
        name: 'rate-limiter',
        version: '1.0.0',
        hooks: {
          'collection:beforeInsert': async (docs: Document[], context: CollectionHookContext) => {
            const key = `${context.database}:${context.collection}:${context.user?.userId ?? 'anonymous'}`;
            const count = (requestCounts.get(key) ?? 0) + 1;
            requestCounts.set(key, count);

            if (count > rateLimit) {
              return { stop: true, result: { error: 'Rate limit exceeded' } };
            }

            return docs;
          },
        },
      });

      await registry.register(rateLimitPlugin);
      await registry.init();

      const context = {
        database: 'test',
        collection: 'users',
        user: { userId: 'user1', roles: [] },
      };

      // First 3 requests should succeed
      for (let i = 0; i < 3; i++) {
        const { stopped } = await registry.executeHook('collection:beforeInsert', [{ n: i }], context);
        expect(stopped).toBe(false);
      }

      // 4th request should be rate limited
      const { stopped, result } = await registry.executeHook('collection:beforeInsert', [{ n: 4 }], context);
      expect(stopped).toBe(true);
      expect((result as Record<string, unknown>).error).toBe('Rate limit exceeded');
    });

    it('should implement encryption plugin', async () => {
      const sensitiveFields = ['password', 'ssn', 'creditCard'];

      const encryptionPlugin = definePlugin({
        name: 'field-encryption',
        version: '1.0.0',
        hooks: {
          'collection:beforeInsert': async (docs: Document[]) => {
            return docs.map((doc) => {
              const encrypted = { ...doc };
              for (const field of sensitiveFields) {
                if (field in encrypted && encrypted[field]) {
                  // Simulate encryption (in reality, use proper crypto)
                  (encrypted as Record<string, unknown>)[field] = `encrypted:${btoa(String(doc[field]))}`;
                }
              }
              return encrypted;
            });
          },
          'collection:afterFind': async (docs: Document[]) => {
            return docs.map((doc) => {
              const decrypted = { ...doc };
              for (const field of sensitiveFields) {
                const value = doc[field];
                if (typeof value === 'string' && value.startsWith('encrypted:')) {
                  // Simulate decryption
                  (decrypted as Record<string, unknown>)[field] = atob(value.slice('encrypted:'.length));
                }
              }
              return decrypted;
            });
          },
        },
      });

      await registry.register(encryptionPlugin);
      await registry.init();

      // Encrypt on insert
      const { result: insertResult } = await registry.executeHook(
        'collection:beforeInsert',
        [{ name: 'Alice', password: 'secret123' }],
        { database: 'test', collection: 'users' }
      );

      expect((insertResult[0] as Document).password).toMatch(/^encrypted:/);
      expect((insertResult[0] as Document).name).toBe('Alice');

      // Decrypt on find
      const { result: findResult } = await registry.executeHook(
        'collection:afterFind',
        insertResult,
        { database: 'test', collection: 'users' }
      );

      expect((findResult[0] as Document).password).toBe('secret123');
    });

    it('should implement multi-tenancy plugin', async () => {
      const multiTenantPlugin = definePlugin({
        name: 'multi-tenant',
        version: '1.0.0',
        hooks: {
          // Add tenant_id to all inserts
          'collection:beforeInsert': async (docs: Document[], context: CollectionHookContext) => {
            const tenantId = context.user?.metadata?.tenantId;
            if (!tenantId) {
              throw new Error('Tenant ID required');
            }
            return docs.map((doc) => ({ ...doc, tenant_id: tenantId }));
          },

          // Filter all queries by tenant_id
          'collection:beforeFind': async (
            params: { filter?: Filter<Document>; options?: Record<string, unknown> },
            context: CollectionHookContext
          ) => {
            const tenantId = context.user?.metadata?.tenantId;
            if (!tenantId) {
              throw new Error('Tenant ID required');
            }
            return {
              filter: { ...params.filter, tenant_id: tenantId } as Filter<Document>,
              options: params.options,
            };
          },
        },
      });

      await registry.register(multiTenantPlugin);
      await registry.init();

      const context = {
        database: 'test',
        collection: 'users',
        user: {
          userId: 'user1',
          roles: [],
          metadata: { tenantId: 'tenant-abc' },
        },
      };

      // Insert should add tenant_id
      const { result: insertResult } = await registry.executeHook(
        'collection:beforeInsert',
        [{ name: 'Alice' }],
        context
      );

      expect((insertResult[0] as Document).tenant_id).toBe('tenant-abc');

      // Find should filter by tenant_id
      const { result: findParams } = await registry.executeHook(
        'collection:beforeFind',
        { filter: { status: 'active' } },
        context
      );

      const typedResult = findParams as { filter: Filter<Document> };
      expect((typedResult.filter as Record<string, unknown>).tenant_id).toBe('tenant-abc');
      expect((typedResult.filter as Record<string, unknown>).status).toBe('active');
    });
  });
});
