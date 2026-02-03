/**
 * Coordination Abstraction
 *
 * Platform-agnostic interface for distributed coordination.
 * This abstraction supports:
 * - Cloudflare Durable Objects
 * - Redis (for self-hosted)
 * - etcd (for Kubernetes)
 * - In-memory (for single-node deployments and testing)
 *
 * ## Concepts
 *
 * The coordination layer provides:
 *
 * 1. **Coordinators** - Single-threaded actors that handle writes for a shard.
 *    In Cloudflare, these are Durable Objects. In other platforms, they could be
 *    Redis-backed actors or Kubernetes pods with leader election.
 *
 * 2. **SQL Storage** - Each coordinator has access to a SQL database for WAL
 *    and manifest storage. In Cloudflare, this is SQLite. Alternatives include
 *    embedded SQLite, PostgreSQL, or even Redis with Lua scripts.
 *
 * 3. **Alarms** - Scheduled execution for compaction and other background tasks.
 *
 * 4. **Stubs** - RPC handles to communicate with other coordinators.
 *
 * @module abstractions/coordination
 */

/**
 * SQL cursor result from query execution.
 *
 * This interface wraps SQL query results in a consistent format
 * across different SQL implementations (SQLite, PostgreSQL, etc.).
 */
export interface SqlCursor {
  /**
   * Get all results as an array of rows.
   *
   * Each row is an object with column names as keys.
   */
  toArray(): Array<Record<string, unknown>>;
}

/**
 * SQL storage interface for persistent data within a coordinator.
 *
 * Provides a subset of SQL operations needed by MongoLake:
 * - WAL entry storage
 * - Manifest persistence
 * - Recovery queries
 *
 * ## Implementation Notes
 *
 * When implementing this interface:
 *
 * 1. **Transactions** - Queries should execute atomically where possible.
 *    The exec method may be called with multiple statements separated by
 *    semicolons in some implementations.
 *
 * 2. **Type Mapping** - MongoLake uses basic types (strings, numbers, blobs).
 *    Implementations should handle type coercion appropriately.
 *
 * 3. **Concurrency** - The coordinator guarantees single-threaded access,
 *    so the SQL storage doesn't need to handle concurrent writes.
 */
export interface SqlStorage {
  /**
   * Execute a SQL query.
   *
   * @param query - SQL query string with ? placeholders
   * @param args - Query arguments
   * @returns Cursor for reading results
   */
  exec(query: string, ...args: unknown[]): SqlCursor;
}

/**
 * Alarm scheduler for background tasks.
 *
 * Alarms allow coordinators to schedule delayed execution,
 * used for compaction and other background operations.
 */
export interface AlarmScheduler {
  /**
   * Schedule an alarm at a specific timestamp.
   *
   * If an alarm is already scheduled, it may be replaced or
   * the implementation may keep the earlier one (platform-dependent).
   *
   * @param timestamp - Unix timestamp in milliseconds when to trigger
   */
  setAlarm(timestamp: number): Promise<void>;

  /**
   * Get the currently scheduled alarm timestamp, if any.
   */
  getAlarm?(): Promise<number | null>;

  /**
   * Cancel any scheduled alarm.
   */
  deleteAlarm?(): Promise<void>;
}

/**
 * Handle to communicate with a coordinator instance.
 *
 * Stubs are used for RPC-style communication between coordinators
 * and between workers and coordinators.
 */
export interface CoordinatorStub {
  /**
   * Send an HTTP-style request to the coordinator.
   *
   * This is the primary communication mechanism. The coordinator
   * handles the request and returns a response.
   *
   * @param request - HTTP request to send
   * @returns HTTP response from coordinator
   */
  fetch(request: Request): Promise<Response>;
}

/**
 * Namespace for creating and addressing coordinators.
 *
 * A namespace groups coordinators of the same type (e.g., all shard coordinators).
 * It provides methods to get coordinator stubs by name or ID.
 */
export interface CoordinatorNamespace {
  /**
   * Get a coordinator ID from a name.
   *
   * Names are human-readable identifiers that map to platform-specific IDs.
   * For example, "shard-0" might map to a Durable Object ID.
   *
   * @param name - Human-readable coordinator name
   * @returns Platform-specific coordinator ID
   */
  idFromName(name: string): CoordinatorId;

  /**
   * Get a stub to a coordinator by ID.
   *
   * @param id - Coordinator ID from idFromName
   * @returns Stub for communicating with the coordinator
   */
  get(id: CoordinatorId): CoordinatorStub;
}

/**
 * Platform-specific coordinator ID.
 *
 * This is an opaque type that varies by platform:
 * - Cloudflare: DurableObjectId
 * - Redis: string key
 * - etcd: string path
 */
export type CoordinatorId = unknown;

/**
 * Options for creating a coordinator.
 */
export interface CoordinatorOptions {
  /** Coordinator name/ID */
  name: string;

  /** Optional initial state */
  initialState?: Record<string, unknown>;
}

/**
 * State available to a coordinator during execution.
 *
 * This provides access to the coordinator's persistent storage
 * and scheduling capabilities.
 */
export interface CoordinatorState {
  /**
   * Get the coordinator's unique ID.
   */
  id: CoordinatorId;

  /**
   * SQL storage for WAL and manifests.
   */
  sql: SqlStorage;

  /**
   * Alarm scheduler for background tasks.
   */
  alarms: AlarmScheduler;

  /**
   * Block concurrent requests while executing a function.
   *
   * This ensures that only one request is processed at a time,
   * which is essential for WAL consistency.
   *
   * @param fn - Async function to execute exclusively
   * @returns Result of the function
   */
  blockConcurrencyWhile<T>(fn: () => Promise<T>): Promise<T>;

  /**
   * Key-value storage for simple state (optional).
   *
   * Some platforms provide KV storage in addition to SQL.
   * This is optional and may be undefined.
   */
  storage?: {
    get<T>(key: string): Promise<T | undefined>;
    put(key: string, value: unknown): Promise<void>;
    delete(key: string): Promise<void>;
  };
}

/**
 * Backend for coordinator lifecycle and communication.
 *
 * This interface is used to create and manage coordinators.
 * In production, you typically use this indirectly through
 * the platform-specific bindings.
 */
export interface CoordinationBackend {
  /**
   * Get or create a coordinator namespace.
   *
   * @param name - Namespace name (e.g., "shards", "replicas")
   * @returns Namespace for addressing coordinators
   */
  getNamespace(name: string): CoordinatorNamespace;

  /**
   * Create a coordinator state for a new coordinator instance.
   *
   * This is typically called by the platform when a coordinator is created.
   *
   * @param options - Coordinator creation options
   * @returns State for the new coordinator
   */
  createState(options: CoordinatorOptions): CoordinatorState;
}
