/**
 * MongoLake Plugin System
 *
 * Formal plugin system for extending MongoLake functionality.
 * Plugins can hook into various extension points throughout the system:
 *
 * 1. **Storage Plugins**: Custom storage backends (S3, GCS, Azure Blob, etc.)
 * 2. **Auth Plugins**: Authentication providers (OAuth, SAML, API keys)
 * 3. **Schema Plugins**: Mongoose-style schema extensions (timestamps, soft delete, etc.)
 * 4. **Middleware Plugins**: Request/response interceptors
 * 5. **Lifecycle Hooks**: Pre/post operation hooks for collections
 *
 * ## Plugin Lifecycle
 *
 * Plugins go through the following lifecycle:
 * 1. **Registration**: Plugin is added to the registry
 * 2. **Initialization**: Plugin's `init()` is called with context
 * 3. **Active**: Plugin's hooks are invoked during operations
 * 4. **Destruction**: Plugin's `destroy()` is called on shutdown
 *
 * ## Creating a Plugin
 *
 * ```typescript
 * import { Plugin, PluginContext, definePlugin } from 'mongolake/plugin';
 *
 * const myPlugin = definePlugin({
 *   name: 'my-plugin',
 *   version: '1.0.0',
 *
 *   async init(context: PluginContext) {
 *     // Initialize plugin resources
 *     console.log('Plugin initialized for', context.database);
 *   },
 *
 *   hooks: {
 *     // Called before any document is inserted
 *     'collection:beforeInsert': async (docs, context) => {
 *       // Add audit timestamp to all documents
 *       return docs.map(doc => ({ ...doc, _auditedAt: new Date() }));
 *     },
 *
 *     // Called after documents are queried
 *     'collection:afterFind': async (docs, context) => {
 *       // Transform results
 *       return docs;
 *     },
 *   },
 *
 *   async destroy() {
 *     // Cleanup plugin resources
 *   },
 * });
 *
 * // Register the plugin
 * const client = new MongoLake({ plugins: [myPlugin] });
 * ```
 *
 * @module plugin
 */

import type { Document, WithId, Filter, Update, AggregationStage } from '../types.js';
import type { StorageBackend } from '../storage/index.js';
import type { AuthProvider, UserContext, AuditLogger } from '../auth/types.js';

// ============================================================================
// Plugin Types
// ============================================================================

/**
 * Plugin metadata interface.
 * Contains identifying information about the plugin.
 */
export interface PluginMetadata {
  /** Unique plugin name (should be kebab-case) */
  name: string;
  /** Semantic version string */
  version: string;
  /** Human-readable description */
  description?: string;
  /** Plugin author */
  author?: string;
  /** Plugin homepage or repository URL */
  homepage?: string;
  /** List of plugin dependencies (by name) */
  dependencies?: string[];
  /** Plugin tags for categorization */
  tags?: string[];
}

/**
 * Context provided to plugins during initialization and hook execution.
 * Contains references to core MongoLake services and configuration.
 */
export interface PluginContext {
  /** Current database name */
  database?: string;
  /** Current collection name (if applicable) */
  collection?: string;
  /** Storage backend instance */
  storage?: StorageBackend;
  /** Logger function for plugin output */
  log: PluginLogger;
  /** Access to the plugin registry for inter-plugin communication */
  registry: PluginRegistryReadOnly;
  /** Configuration passed to the plugin */
  config: Record<string, unknown>;
}

/**
 * Logger interface for plugins.
 * Provides consistent logging with plugin context.
 */
export interface PluginLogger {
  debug(message: string, ...args: unknown[]): void;
  info(message: string, ...args: unknown[]): void;
  warn(message: string, ...args: unknown[]): void;
  error(message: string, ...args: unknown[]): void;
}

/**
 * Read-only view of the plugin registry.
 * Used for inter-plugin communication and querying.
 */
export interface PluginRegistryReadOnly {
  /** Get a plugin by name */
  get(name: string): Plugin | undefined;
  /** Check if a plugin is registered */
  has(name: string): boolean;
  /** Get all registered plugins */
  all(): Plugin[];
  /** Get all plugins with a specific tag */
  byTag(tag: string): Plugin[];
}

// ============================================================================
// Hook Types
// ============================================================================

/**
 * Hook context provides additional information to hooks.
 */
export interface HookContext extends PluginContext {
  /** The operation being performed */
  operation: string;
  /** Timestamp when the operation started */
  timestamp: Date;
  /** User context if authenticated */
  user?: UserContext;
  /** Request metadata */
  request?: {
    method: string;
    path?: string;
    headers?: Record<string, string>;
  };
}

/**
 * Collection hook context for collection-level operations.
 */
export interface CollectionHookContext extends HookContext {
  /** Database name */
  database: string;
  /** Collection name */
  collection: string;
  /** Filter used in the operation (for find/update/delete) */
  filter?: Filter<Document>;
  /** Options passed to the operation */
  options?: Record<string, unknown>;
}

/**
 * Result type for hooks that can modify or stop the operation.
 */
export type HookResult<T> = T | void | { stop: true; result?: T };

/**
 * Collection lifecycle hooks.
 * These hooks are called at various points during collection operations.
 */
export interface CollectionHooks {
  /**
   * Called before documents are inserted.
   * Can modify the documents or stop the operation.
   */
  'collection:beforeInsert'?: (
    docs: Document[],
    context: CollectionHookContext
  ) => Promise<HookResult<Document[]>>;

  /**
   * Called after documents are inserted.
   * Can perform side effects but cannot modify the result.
   */
  'collection:afterInsert'?: (
    result: { insertedIds: Record<number, string | unknown>; insertedCount: number },
    context: CollectionHookContext
  ) => Promise<void>;

  /**
   * Called before documents are updated.
   * Can modify the filter, update, or stop the operation.
   */
  'collection:beforeUpdate'?: (
    params: { filter: Filter<Document>; update: Update<Document> },
    context: CollectionHookContext
  ) => Promise<HookResult<{ filter: Filter<Document>; update: Update<Document> }>>;

  /**
   * Called after documents are updated.
   */
  'collection:afterUpdate'?: (
    result: { matchedCount: number; modifiedCount: number },
    context: CollectionHookContext
  ) => Promise<void>;

  /**
   * Called before documents are deleted.
   * Can modify the filter or stop the operation.
   */
  'collection:beforeDelete'?: (
    filter: Filter<Document>,
    context: CollectionHookContext
  ) => Promise<HookResult<Filter<Document>>>;

  /**
   * Called after documents are deleted.
   */
  'collection:afterDelete'?: (
    result: { deletedCount: number },
    context: CollectionHookContext
  ) => Promise<void>;

  /**
   * Called before a find operation.
   * Can modify the filter or options.
   */
  'collection:beforeFind'?: (
    params: { filter?: Filter<Document>; options?: Record<string, unknown> },
    context: CollectionHookContext
  ) => Promise<HookResult<{ filter?: Filter<Document>; options?: Record<string, unknown> }>>;

  /**
   * Called after documents are found.
   * Can transform the results.
   */
  'collection:afterFind'?: (
    docs: WithId<Document>[],
    context: CollectionHookContext
  ) => Promise<HookResult<WithId<Document>[]>>;

  /**
   * Called before an aggregation pipeline runs.
   * Can modify the pipeline.
   */
  'collection:beforeAggregate'?: (
    pipeline: AggregationStage[],
    context: CollectionHookContext
  ) => Promise<HookResult<AggregationStage[]>>;

  /**
   * Called after aggregation completes.
   * Can transform the results.
   */
  'collection:afterAggregate'?: (
    results: Document[],
    context: CollectionHookContext
  ) => Promise<HookResult<Document[]>>;
}

/**
 * Client lifecycle hooks.
 * These hooks are called at the client/database level.
 */
export interface ClientHooks {
  /**
   * Called when a client connects.
   */
  'client:connect'?: (context: PluginContext) => Promise<void>;

  /**
   * Called when a client disconnects.
   */
  'client:disconnect'?: (context: PluginContext) => Promise<void>;

  /**
   * Called when a database is accessed.
   */
  'database:access'?: (
    database: string,
    context: PluginContext
  ) => Promise<void>;

  /**
   * Called when a collection is accessed.
   */
  'collection:access'?: (
    params: { database: string; collection: string },
    context: PluginContext
  ) => Promise<void>;
}

/**
 * Storage hooks for intercepting storage operations.
 */
export interface StorageHooks {
  /**
   * Called before data is read from storage.
   */
  'storage:beforeGet'?: (
    key: string,
    context: PluginContext
  ) => Promise<HookResult<string>>;

  /**
   * Called after data is read from storage.
   * Can transform the data.
   */
  'storage:afterGet'?: (
    data: Uint8Array | null,
    context: PluginContext & { key: string }
  ) => Promise<HookResult<Uint8Array | null>>;

  /**
   * Called before data is written to storage.
   * Can modify the data.
   */
  'storage:beforePut'?: (
    params: { key: string; data: Uint8Array },
    context: PluginContext
  ) => Promise<HookResult<{ key: string; data: Uint8Array }>>;

  /**
   * Called after data is written to storage.
   */
  'storage:afterPut'?: (
    params: { key: string; size: number },
    context: PluginContext
  ) => Promise<void>;
}

/**
 * Auth hooks for authentication events.
 */
export interface AuthHooks {
  /**
   * Called before authentication is attempted.
   */
  'auth:beforeAuthenticate'?: (
    request: Request,
    context: PluginContext
  ) => Promise<void>;

  /**
   * Called after successful authentication.
   */
  'auth:afterAuthenticate'?: (
    user: UserContext,
    context: PluginContext
  ) => Promise<void>;

  /**
   * Called when authentication fails.
   */
  'auth:onFailure'?: (
    error: { code: string; message: string },
    context: PluginContext
  ) => Promise<void>;

  /**
   * Called before permission check.
   */
  'auth:beforePermissionCheck'?: (
    params: { user: UserContext; database: string; collection?: string; operation: string },
    context: PluginContext
  ) => Promise<HookResult<boolean>>;
}

/**
 * Combined hooks interface containing all hook types.
 */
export interface PluginHooks extends CollectionHooks, ClientHooks, StorageHooks, AuthHooks {}

/**
 * All available hook names.
 */
export type HookName = keyof PluginHooks;

// ============================================================================
// Plugin Interface
// ============================================================================

/**
 * Main Plugin interface.
 * All plugins must implement this interface.
 */
export interface Plugin extends PluginMetadata {
  /**
   * Plugin initialization.
   * Called when the plugin is registered and the system starts.
   *
   * @param context - Plugin context with services and configuration
   */
  init?(context: PluginContext): Promise<void> | void;

  /**
   * Plugin destruction.
   * Called when the plugin is unregistered or the system shuts down.
   * Use this to clean up resources.
   */
  destroy?(): Promise<void> | void;

  /**
   * Hook implementations.
   * Object containing hook functions keyed by hook name.
   */
  hooks?: Partial<PluginHooks>;

  /**
   * Storage backend factory (for storage plugins).
   * If provided, this plugin can create custom storage backends.
   */
  createStorage?(config: Record<string, unknown>): StorageBackend;

  /**
   * Auth provider factory (for auth plugins).
   * If provided, this plugin can create authentication providers.
   */
  createAuthProvider?(config: Record<string, unknown>): AuthProvider;

  /**
   * Audit logger factory (for audit plugins).
   * If provided, this plugin can create audit loggers.
   */
  createAuditLogger?(config: Record<string, unknown>): AuditLogger;

  /**
   * Schema plugin function (for mongoose-style schema plugins).
   * If provided, this function will be applied to schemas.
   */
  schemaPlugin?<T>(schema: T, options?: Record<string, unknown>): void;
}

/**
 * Plugin definition options for definePlugin helper.
 */
export interface PluginDefinition extends PluginMetadata {
  init?(context: PluginContext): Promise<void> | void;
  destroy?(): Promise<void> | void;
  hooks?: Partial<PluginHooks>;
  createStorage?(config: Record<string, unknown>): StorageBackend;
  createAuthProvider?(config: Record<string, unknown>): AuthProvider;
  createAuditLogger?(config: Record<string, unknown>): AuditLogger;
  schemaPlugin?<T>(schema: T, options?: Record<string, unknown>): void;
}

// ============================================================================
// Plugin Registry
// ============================================================================

/**
 * Plugin registry manages plugin lifecycle and hook execution.
 */
export class PluginRegistry implements PluginRegistryReadOnly {
  private plugins: Map<string, Plugin> = new Map();
  private hookListeners: Map<HookName, Array<{ plugin: Plugin; fn: Function }>> = new Map();
  private initialized = false;
  private logger: PluginLogger;

  constructor(logger?: PluginLogger) {
    this.logger = logger ?? this.createDefaultLogger();
  }

  private createDefaultLogger(): PluginLogger {
    return {
      debug: (msg, ...args) => console.debug(`[plugin] ${msg}`, ...args),
      info: (msg, ...args) => console.info(`[plugin] ${msg}`, ...args),
      warn: (msg, ...args) => console.warn(`[plugin] ${msg}`, ...args),
      error: (msg, ...args) => console.error(`[plugin] ${msg}`, ...args),
    };
  }

  /**
   * Register a plugin.
   * The plugin will be initialized if the registry is already initialized.
   *
   * @param plugin - Plugin to register
   * @param config - Configuration to pass to the plugin
   * @throws Error if a plugin with the same name is already registered
   */
  async register(plugin: Plugin, config: Record<string, unknown> = {}): Promise<void> {
    if (this.plugins.has(plugin.name)) {
      throw new Error(`Plugin "${plugin.name}" is already registered`);
    }

    // Check dependencies
    if (plugin.dependencies) {
      for (const dep of plugin.dependencies) {
        if (!this.plugins.has(dep)) {
          throw new Error(
            `Plugin "${plugin.name}" depends on "${dep}" which is not registered. ` +
            `Register "${dep}" before "${plugin.name}".`
          );
        }
      }
    }

    // Register plugin
    this.plugins.set(plugin.name, plugin);

    // Register hooks
    if (plugin.hooks) {
      for (const [hookName, hookFn] of Object.entries(plugin.hooks)) {
        if (hookFn) {
          const listeners = this.hookListeners.get(hookName as HookName) ?? [];
          listeners.push({ plugin, fn: hookFn });
          this.hookListeners.set(hookName as HookName, listeners);
        }
      }
    }

    this.logger.info(`Registered plugin: ${plugin.name}@${plugin.version}`);

    // Initialize if registry is already initialized
    if (this.initialized && plugin.init) {
      const context = this.createContext(config);
      await plugin.init(context);
    }
  }

  /**
   * Unregister a plugin.
   * Calls the plugin's destroy method if defined.
   *
   * @param name - Name of the plugin to unregister
   */
  async unregister(name: string): Promise<void> {
    const plugin = this.plugins.get(name);
    if (!plugin) {
      return;
    }

    // Check if other plugins depend on this one
    for (const [otherName, otherPlugin] of this.plugins) {
      if (otherPlugin.dependencies?.includes(name)) {
        throw new Error(
          `Cannot unregister "${name}" because "${otherName}" depends on it`
        );
      }
    }

    // Call destroy
    if (plugin.destroy) {
      await plugin.destroy();
    }

    // Remove hooks
    for (const [hookName, listeners] of this.hookListeners) {
      const filtered = listeners.filter((l) => l.plugin !== plugin);
      if (filtered.length > 0) {
        this.hookListeners.set(hookName, filtered);
      } else {
        this.hookListeners.delete(hookName);
      }
    }

    this.plugins.delete(name);
    this.logger.info(`Unregistered plugin: ${name}`);
  }

  /**
   * Initialize all registered plugins.
   * Should be called after all plugins are registered.
   *
   * @param config - Base configuration to pass to all plugins
   */
  async init(config: Record<string, unknown> = {}): Promise<void> {
    if (this.initialized) {
      return;
    }

    const context = this.createContext(config);

    // Initialize plugins in order (respecting dependencies)
    const initialized = new Set<string>();
    const toInit = [...this.plugins.keys()];

    while (toInit.length > 0) {
      const ready = toInit.filter((name) => {
        const plugin = this.plugins.get(name)!;
        const deps = plugin.dependencies ?? [];
        return deps.every((d) => initialized.has(d));
      });

      if (ready.length === 0 && toInit.length > 0) {
        throw new Error(`Circular dependency detected among plugins: ${toInit.join(', ')}`);
      }

      for (const name of ready) {
        const plugin = this.plugins.get(name)!;
        if (plugin.init) {
          await plugin.init(context);
        }
        initialized.add(name);
        toInit.splice(toInit.indexOf(name), 1);
      }
    }

    this.initialized = true;
    this.logger.info(`Initialized ${this.plugins.size} plugin(s)`);
  }

  /**
   * Destroy all plugins and cleanup.
   */
  async destroy(): Promise<void> {
    // Destroy in reverse order (dependent plugins first)
    const names = [...this.plugins.keys()].reverse();

    for (const name of names) {
      await this.unregister(name);
    }

    this.hookListeners.clear();
    this.initialized = false;
  }

  /**
   * Execute hooks for a given hook name.
   * Hooks are executed in plugin registration order.
   *
   * @param hookName - Name of the hook to execute
   * @param args - Arguments to pass to the hook
   * @param context - Hook context
   * @returns Result from the last hook that returned a value, or the original args
   */
  async executeHook<T>(
    hookName: HookName,
    args: T,
    context: Partial<HookContext>
  ): Promise<{ result: T; stopped: boolean }> {
    const listeners = this.hookListeners.get(hookName);
    if (!listeners || listeners.length === 0) {
      return { result: args, stopped: false };
    }

    let result = args;
    const fullContext: HookContext = {
      operation: hookName,
      timestamp: new Date(),
      log: this.logger,
      registry: this,
      config: {},
      ...context,
    };

    for (const { plugin, fn } of listeners) {
      try {
        const hookResult = await fn(result, fullContext);

        // Check for stop signal
        if (hookResult && typeof hookResult === 'object' && 'stop' in hookResult && hookResult.stop) {
          return {
            result: hookResult.result !== undefined ? hookResult.result : result,
            stopped: true,
          };
        }

        // Update result if hook returned a value
        if (hookResult !== undefined && hookResult !== null) {
          result = hookResult;
        }
      } catch (error) {
        this.logger.error(
          `Hook "${hookName}" in plugin "${plugin.name}" threw an error:`,
          error
        );
        throw error;
      }
    }

    return { result, stopped: false };
  }

  /**
   * Check if any plugins have a specific hook.
   */
  hasHook(hookName: HookName): boolean {
    const listeners = this.hookListeners.get(hookName);
    return listeners !== undefined && listeners.length > 0;
  }

  // PluginRegistryReadOnly implementation

  get(name: string): Plugin | undefined {
    return this.plugins.get(name);
  }

  has(name: string): boolean {
    return this.plugins.has(name);
  }

  all(): Plugin[] {
    return [...this.plugins.values()];
  }

  byTag(tag: string): Plugin[] {
    return this.all().filter((p) => p.tags?.includes(tag));
  }

  /**
   * Get all plugins that provide storage backends.
   */
  getStoragePlugins(): Plugin[] {
    return this.all().filter((p) => p.createStorage !== undefined);
  }

  /**
   * Get all plugins that provide auth providers.
   */
  getAuthPlugins(): Plugin[] {
    return this.all().filter((p) => p.createAuthProvider !== undefined);
  }

  /**
   * Get all plugins that provide schema plugins.
   */
  getSchemaPlugins(): Plugin[] {
    return this.all().filter((p) => p.schemaPlugin !== undefined);
  }

  private createContext(config: Record<string, unknown>): PluginContext {
    return {
      log: this.logger,
      registry: this,
      config,
    };
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Define a plugin with type inference.
 * Helper function to create a plugin with proper typing.
 *
 * @param definition - Plugin definition object
 * @returns A typed Plugin object
 *
 * @example
 * ```typescript
 * const timestampPlugin = definePlugin({
 *   name: 'timestamps',
 *   version: '1.0.0',
 *   description: 'Adds createdAt and updatedAt timestamps to documents',
 *
 *   hooks: {
 *     'collection:beforeInsert': async (docs) => {
 *       const now = new Date();
 *       return docs.map(doc => ({
 *         ...doc,
 *         createdAt: now,
 *         updatedAt: now,
 *       }));
 *     },
 *     'collection:beforeUpdate': async ({ filter, update }) => {
 *       return {
 *         filter,
 *         update: {
 *           ...update,
 *           $set: {
 *             ...(update.$set || {}),
 *             updatedAt: new Date(),
 *           },
 *         },
 *       };
 *     },
 *   },
 * });
 * ```
 */
export function definePlugin(definition: PluginDefinition): Plugin {
  return definition;
}

/**
 * Create a logger with plugin context prefix.
 *
 * @param pluginName - Name of the plugin for log prefix
 * @param baseLogger - Optional base logger to delegate to
 * @returns A PluginLogger instance
 */
export function createPluginLogger(
  pluginName: string,
  baseLogger?: Partial<PluginLogger>
): PluginLogger {
  const prefix = `[${pluginName}]`;
  return {
    debug: (msg, ...args) => (baseLogger?.debug ?? console.debug)(`${prefix} ${msg}`, ...args),
    info: (msg, ...args) => (baseLogger?.info ?? console.info)(`${prefix} ${msg}`, ...args),
    warn: (msg, ...args) => (baseLogger?.warn ?? console.warn)(`${prefix} ${msg}`, ...args),
    error: (msg, ...args) => (baseLogger?.error ?? console.error)(`${prefix} ${msg}`, ...args),
  };
}

/**
 * Compose multiple plugins into a single plugin.
 * Useful for creating plugin bundles.
 *
 * @param name - Name for the composed plugin
 * @param plugins - Plugins to compose
 * @returns A new plugin that combines all the provided plugins
 *
 * @example
 * ```typescript
 * const corePlugins = composePlugins('core-bundle', [
 *   timestampPlugin,
 *   softDeletePlugin,
 *   auditPlugin,
 * ]);
 * ```
 */
export function composePlugins(name: string, plugins: Plugin[]): Plugin {
  // Use a simple record type to avoid complex hook type issues
  const hooks: Record<string, Function> = {};

  // Merge hooks from all plugins
  for (const plugin of plugins) {
    if (plugin.hooks) {
      for (const [hookName, hookFn] of Object.entries(plugin.hooks)) {
        if (!hookFn) continue;

        const existing = hooks[hookName];
        if (existing) {
          // Chain hooks - call existing then new
          hooks[hookName] = async (args: unknown, context: HookContext) => {
            const result1 = await existing(args, context);
            const input = result1 !== undefined ? result1 : args;
            return hookFn(input as never, context as never);
          };
        } else {
          hooks[hookName] = hookFn;
        }
      }
    }
  }

  return definePlugin({
    name,
    version: '1.0.0',
    description: `Composed plugin containing: ${plugins.map((p) => p.name).join(', ')}`,
    tags: ['composed'],

    async init(context) {
      for (const plugin of plugins) {
        if (plugin.init) {
          await plugin.init(context);
        }
      }
    },

    async destroy() {
      for (const plugin of [...plugins].reverse()) {
        if (plugin.destroy) {
          await plugin.destroy();
        }
      }
    },

    // Cast to the expected type - the runtime behavior is correct
    hooks: hooks as Partial<PluginHooks>,
  });
}

// ============================================================================
// Global Registry
// ============================================================================

/**
 * Global plugin registry instance.
 * Used as the default registry when not explicitly providing one.
 */
let globalRegistry: PluginRegistry | null = null;

/**
 * Get the global plugin registry.
 * Creates one if it doesn't exist.
 */
export function getGlobalRegistry(): PluginRegistry {
  if (!globalRegistry) {
    globalRegistry = new PluginRegistry();
  }
  return globalRegistry;
}

/**
 * Set the global plugin registry.
 * Useful for testing or custom configurations.
 */
export function setGlobalRegistry(registry: PluginRegistry): void {
  globalRegistry = registry;
}

/**
 * Reset the global plugin registry.
 * Useful for testing.
 */
export async function resetGlobalRegistry(): Promise<void> {
  if (globalRegistry) {
    await globalRegistry.destroy();
    globalRegistry = null;
  }
}

// ============================================================================
// Exports
// ============================================================================

export type {
  Document,
  WithId,
  Filter,
  Update,
  AggregationStage,
} from '../types.js';

export type {
  StorageBackend,
} from '../storage/index.js';

export type {
  AuthProvider,
  UserContext,
  AuditLogger,
} from '../auth/types.js';
