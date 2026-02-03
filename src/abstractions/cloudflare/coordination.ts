/**
 * Cloudflare Durable Objects Coordination Implementation
 *
 * Implements CoordinationBackend using Cloudflare Durable Objects.
 *
 * @module abstractions/cloudflare/coordination
 */

import type {
  CoordinationBackend,
  CoordinatorState,
  CoordinatorStub,
  CoordinatorNamespace,
  SqlStorage,
  SqlCursor,
  AlarmScheduler,
  CoordinatorOptions,
  CoordinatorId,
} from '../coordination.js';

import type {
  DurableObjectState,
  DurableObjectNamespace,
  DurableObjectStub,
  DurableObjectId,
} from '@cloudflare/workers-types';

/**
 * Wraps Cloudflare's SQL cursor.
 */
class CloudflareSqlCursor implements SqlCursor {
  constructor(private cursor: ReturnType<DurableObjectState['storage']['sql']['exec']>) {}

  toArray(): Array<Record<string, unknown>> {
    return this.cursor.toArray();
  }
}

/**
 * Wraps Cloudflare's Durable Object SQL storage.
 */
export class CloudflareSqlStorage implements SqlStorage {
  constructor(private state: DurableObjectState) {}

  exec(query: string, ...args: unknown[]): SqlCursor {
    const cursor = this.state.storage.sql.exec(query, ...args);
    return new CloudflareSqlCursor(cursor);
  }
}

/**
 * Wraps a Cloudflare DurableObjectStub.
 */
export class CloudflareCoordinatorStub implements CoordinatorStub {
  constructor(private stub: DurableObjectStub) {}

  async fetch(request: Request): Promise<Response> {
    return this.stub.fetch(request);
  }
}

/**
 * Wraps a Cloudflare DurableObjectNamespace.
 */
export class CloudflareCoordinatorNamespace implements CoordinatorNamespace {
  constructor(private namespace: DurableObjectNamespace) {}

  idFromName(name: string): CoordinatorId {
    return this.namespace.idFromName(name);
  }

  get(id: CoordinatorId): CoordinatorStub {
    const stub = this.namespace.get(id as DurableObjectId);
    return new CloudflareCoordinatorStub(stub);
  }
}

/**
 * Cloudflare-specific alarm scheduler using Durable Object alarms.
 */
class CloudflareAlarmScheduler implements AlarmScheduler {
  constructor(private state: DurableObjectState) {}

  async setAlarm(timestamp: number): Promise<void> {
    await this.state.storage.setAlarm(timestamp);
  }

  async getAlarm(): Promise<number | null> {
    return await this.state.storage.getAlarm();
  }

  async deleteAlarm(): Promise<void> {
    await this.state.storage.deleteAlarm();
  }
}

/**
 * Wraps DurableObjectState as CoordinatorState.
 */
class CloudflareCoordinatorState implements CoordinatorState {
  id: CoordinatorId;
  sql: SqlStorage;
  alarms: AlarmScheduler;
  storage?: {
    get<T>(key: string): Promise<T | undefined>;
    put(key: string, value: unknown): Promise<void>;
    delete(key: string): Promise<void>;
  };

  constructor(private state: DurableObjectState) {
    this.id = state.id;
    this.sql = new CloudflareSqlStorage(state);
    this.alarms = new CloudflareAlarmScheduler(state);

    // Wrap the KV storage if needed
    this.storage = {
      get: async <T>(key: string) => {
        return await state.storage.get<T>(key);
      },
      put: async (key: string, value: unknown) => {
        await state.storage.put(key, value);
      },
      delete: async (key: string) => {
        await state.storage.delete(key);
      },
    };
  }

  blockConcurrencyWhile<T>(fn: () => Promise<T>): Promise<T> {
    return this.state.blockConcurrencyWhile(fn);
  }

  /**
   * Get the underlying DurableObjectState.
   *
   * Use this for Cloudflare-specific operations not covered by the abstraction.
   * Note: Using this breaks portability.
   */
  getDurableObjectState(): DurableObjectState {
    return this.state;
  }
}

/**
 * Cloudflare Durable Objects implementation of CoordinationBackend.
 *
 * This implementation wraps Cloudflare Durable Objects to provide a
 * platform-agnostic coordination interface.
 *
 * ## Architecture
 *
 * Durable Objects provide:
 * - Single-threaded execution per object
 * - Built-in SQLite storage
 * - Alarm scheduling
 * - Global uniqueness via naming
 *
 * These map well to MongoLake's shard coordinator concept.
 *
 * ## Usage
 *
 * In a Cloudflare Worker, you typically access Durable Objects through
 * environment bindings rather than this backend directly:
 *
 * ```typescript
 * // In your Durable Object class:
 * export class ShardDO {
 *   private coordination: CoordinatorState;
 *
 *   constructor(state: DurableObjectState, env: Env) {
 *     this.coordination = createCloudflareCoordination(state);
 *   }
 * }
 * ```
 */
export class CloudflareCoordinationBackend implements CoordinationBackend {
  private namespaces: Map<string, CoordinatorNamespace> = new Map();

  /**
   * Register a DurableObjectNamespace.
   *
   * This is called during Worker initialization to register the DO namespaces
   * from the environment bindings.
   */
  registerNamespace(name: string, namespace: DurableObjectNamespace): void {
    this.namespaces.set(name, new CloudflareCoordinatorNamespace(namespace));
  }

  getNamespace(name: string): CoordinatorNamespace {
    const namespace = this.namespaces.get(name);
    if (!namespace) {
      throw new Error(`Coordinator namespace not registered: ${name}`);
    }
    return namespace;
  }

  createState(_options: CoordinatorOptions): CoordinatorState {
    // In Cloudflare, state is created by the runtime when a DO is instantiated.
    // This method isn't typically used directly - use createCloudflareCoordination instead.
    throw new Error(
      'Cannot create CoordinatorState directly in Cloudflare. ' +
      'Use createCloudflareCoordination(state) in your Durable Object constructor.'
    );
  }
}

/**
 * Create a CoordinatorState from a DurableObjectState.
 *
 * This is the primary way to use the coordination abstraction in Cloudflare.
 * Call this in your Durable Object constructor:
 *
 * ```typescript
 * export class ShardDO {
 *   private state: CoordinatorState;
 *
 *   constructor(durableState: DurableObjectState, env: Env) {
 *     this.state = createCloudflareCoordination(durableState);
 *   }
 * }
 * ```
 *
 * @param state - DurableObjectState from Cloudflare runtime
 * @returns Platform-agnostic CoordinatorState
 */
export function createCloudflareCoordination(state: DurableObjectState): CoordinatorState {
  return new CloudflareCoordinatorState(state);
}

/**
 * Create a CoordinatorNamespace from a DurableObjectNamespace.
 *
 * Use this to wrap DO namespaces for use with the abstraction layer:
 *
 * ```typescript
 * const shardNamespace = createCloudflareNamespace(env.SHARD_DO);
 * const shardStub = shardNamespace.get(shardNamespace.idFromName('shard-0'));
 * ```
 *
 * @param namespace - DurableObjectNamespace from Cloudflare bindings
 * @returns Platform-agnostic CoordinatorNamespace
 */
export function createCloudflareNamespace(namespace: DurableObjectNamespace): CoordinatorNamespace {
  return new CloudflareCoordinatorNamespace(namespace);
}
