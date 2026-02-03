/**
 * Time Travel Reader
 *
 * Provides Iceberg-based time travel queries:
 * - Query by snapshot ID
 * - Query by timestamp
 * - Snapshot history traversal
 * - As-of queries
 *
 * Iceberg Specification Reference:
 * https://iceberg.apache.org/spec/#time-travel
 */

import type { StorageBackend } from '../storage/index.js';
import type { Document, Filter, FindOptions, WithId } from '../types.js';
import type { Snapshot, TableMetadata } from '@dotdo/iceberg';

// ============================================================================
// Types
// ============================================================================

/**
 * Options for time travel queries
 */
export interface TimeTravelOptions {
  /** Query at specific snapshot ID */
  snapshotId?: bigint;
  /** Query at specific timestamp (milliseconds since epoch) */
  timestamp?: number;
}

/**
 * Result of a time travel query
 */
export interface TimeTravelResult {
  /** The snapshot ID that was queried */
  snapshotId: bigint;
  /** The snapshot metadata */
  snapshot: Snapshot | null;
  /** List of data file paths in this snapshot */
  dataFiles: string[];
}

/**
 * Result of querying documents at a snapshot
 */
export interface SnapshotQueryResult<T extends Document> extends TimeTravelResult {
  /** Documents retrieved from the snapshot */
  documents: WithId<T>[];
}

/**
 * Options for listing snapshots
 */
export interface ListSnapshotsOptions {
  /** Return snapshots in reverse chronological order */
  reverse?: boolean;
  /** Maximum number of snapshots to return */
  limit?: number;
  /** Offset for pagination */
  offset?: number;
}

/**
 * Options for getting snapshot ancestry
 */
export interface AncestryOptions {
  /** Maximum depth to traverse */
  maxDepth?: number;
}

/**
 * Result of diffing two snapshots
 */
export interface SnapshotDiff {
  /** Source snapshot */
  fromSnapshot: Snapshot;
  /** Target snapshot */
  toSnapshot: Snapshot;
  /** Files added between snapshots */
  addedFiles: string[];
  /** Files removed between snapshots */
  removedFiles: string[];
}

/**
 * Result of reading changes since a snapshot
 */
export interface ChangesResult {
  /** Starting snapshot ID */
  fromSnapshotId: bigint;
  /** Ending snapshot ID */
  toSnapshotId: bigint;
  /** List of intermediate snapshots */
  intermediateSnapshots: Snapshot[];
}

/**
 * Options for reading changes
 */
export interface ReadChangesOptions {
  /** Stop at this snapshot ID (default: current) */
  toSnapshotId?: bigint;
}

/**
 * Options for reading documents at a snapshot
 */
export interface ReadDocumentsOptions {
  /** Projection to apply */
  projection?: Record<string, 0 | 1>;
}

/**
 * Read-only time travel collection interface
 */
export interface TimeTravelCollectionView<T extends Document> {
  /** Collection name */
  readonly name: string;
  /** Whether this is a read-only view */
  readonly isReadOnly: true;

  /** Get the snapshot this view is based on */
  getSnapshot(): Promise<Snapshot | null>;

  /** Find documents (read-only) */
  find(filter?: Filter<T>, options?: FindOptions): AsyncIterable<WithId<T>>;

  /** Find one document (read-only) */
  findOne(filter?: Filter<T>, options?: FindOptions): Promise<WithId<T> | null>;

  /** Count documents (read-only) */
  countDocuments(filter?: Filter<T>): Promise<number>;

  /** These methods throw errors (read-only) */
  insertOne(doc: T): Promise<never>;
  updateOne(filter: Filter<T>, update: unknown): Promise<never>;
  deleteOne(filter: Filter<T>): Promise<never>;
}

// ============================================================================
// Internal Types
// ============================================================================

/**
 * Cached entry with timestamp for TTL.
 */
interface CachedEntry<T> {
  data: T;
  cachedAt: number;
}

/**
 * Manifest entry structure (simplified for reading).
 */
interface ManifestEntry {
  status: number;
  'snapshot-id': string;
  'sequence-number': number;
  'data-file': {
    'file-path': string;
    'record-count': number;
    [key: string]: unknown;
  };
}

/**
 * Manifest file structure.
 */
interface ManifestData {
  entries: ManifestEntry[];
}

/**
 * Manifest list entry.
 */
interface ManifestListEntry {
  'manifest-path': string;
  'added-snapshot-id': string;
  [key: string]: unknown;
}

// ============================================================================
// Snapshot Resolver
// ============================================================================

/**
 * SnapshotResolver handles snapshot lookup and normalization.
 * Extracted for cleaner separation of concerns.
 */
class SnapshotResolver {
  /**
   * Find a snapshot by ID in table metadata.
   * Handles both number and BigInt snapshot IDs for precision with large values.
   */
  findById(metadata: TableMetadata, snapshotId: bigint): Snapshot | null {
    return metadata.snapshots.find((s) => {
      const storedId = s['snapshot-id'];
      // Handle both number and BigInt (cast as number) stored values
      if (typeof storedId === 'bigint') {
        return storedId === snapshotId;
      }
      // For number values, compare with BigInt
      return BigInt(storedId) === snapshotId;
    }) ?? null;
  }

  /**
   * Find snapshot at or before a timestamp.
   * Uses binary search for efficiency with sorted snapshots.
   */
  findAtTimestamp(metadata: TableMetadata, timestamp: number): Snapshot | null {
    if (metadata.snapshots.length === 0) {
      return null;
    }

    // Sort snapshots by timestamp descending
    const sortedSnapshots = [...metadata.snapshots].sort(
      (a, b) => b['timestamp-ms'] - a['timestamp-ms']
    );

    // Find first snapshot at or before timestamp
    for (const snapshot of sortedSnapshots) {
      if (snapshot['timestamp-ms'] <= timestamp) {
        return snapshot;
      }
    }

    return null;
  }

  /**
   * Normalize snapshot ID to bigint.
   */
  normalizeSnapshotId(snapshotId: bigint | number): bigint {
    return typeof snapshotId === 'number' ? BigInt(snapshotId) : snapshotId;
  }

  /**
   * Normalize timestamp to number.
   */
  normalizeTimestamp(timestamp: number | Date | string): number {
    if (timestamp instanceof Date) {
      return timestamp.getTime();
    }
    if (typeof timestamp === 'string') {
      const parsed = Date.parse(timestamp);
      if (isNaN(parsed)) {
        throw new Error(`Invalid timestamp: ${timestamp}`);
      }
      return parsed;
    }
    return timestamp;
  }

  /**
   * Validate snapshot ID.
   */
  validateSnapshotId(snapshotId: bigint): void {
    if (snapshotId < 0n) {
      throw new Error(`Invalid snapshot ID: ${snapshotId}`);
    }
  }

  /**
   * Validate timestamp.
   */
  validateTimestamp(timestamp: number): void {
    if (timestamp < 0 || !isFinite(timestamp) || isNaN(timestamp)) {
      throw new Error(`Invalid timestamp: ${timestamp}`);
    }
  }

  /**
   * Convert a snapshot to have BigInt IDs for external API compatibility.
   * Tests expect BigInt snapshot IDs even though @dotdo/iceberg types use numbers.
   *
   * Note: This returns a Snapshot-compatible object but with BigInt IDs instead of numbers.
   * The type system doesn't capture this runtime representation change.
   */
  withBigIntIds(snapshot: Snapshot): Snapshot {
    // Create a new object with BigInt IDs
    // The Snapshot type expects numbers, but we're returning BigInts for API compatibility
    // Callers accessing these fields will get BigInts at runtime
    const result = {
      ...snapshot,
      'snapshot-id': BigInt(snapshot['snapshot-id']),
      'parent-snapshot-id': snapshot['parent-snapshot-id'] !== undefined && snapshot['parent-snapshot-id'] !== null
        ? BigInt(snapshot['parent-snapshot-id'])
        : snapshot['parent-snapshot-id'],
      'sequence-number': BigInt(snapshot['sequence-number']),
    };
    // Type assertion: result shape matches Snapshot but with BigInt numeric fields
    // The double cast is necessary because BigInt is not assignable to number
    return result as unknown as Snapshot;
  }
}

// ============================================================================
// Metadata Parser
// ============================================================================

/**
 * MetadataParser handles parsing and normalization of Iceberg metadata.
 */
class MetadataParser {
  /**
   * Parse table metadata JSON, handling BigInt serialization.
   *
   * The test fixtures serialize BigInt as strings, so we need to handle this.
   * For snapshot IDs, we keep them as numbers when they fit in Number.MAX_SAFE_INTEGER,
   * otherwise we store them as BigInt (cast as number for the type).
   */
  parse(json: string): TableMetadata {
    const parsed = JSON.parse(json) as TableMetadata;

    // Convert snapshot IDs from strings/BigInt, preserving large values
    const normalizedSnapshots = parsed.snapshots.map(s => ({
      ...s,
      'snapshot-id': this.toSafeNumber(s['snapshot-id']),
      'parent-snapshot-id': s['parent-snapshot-id'] !== undefined && s['parent-snapshot-id'] !== null
        ? this.toSafeNumber(s['parent-snapshot-id'])
        : s['parent-snapshot-id'],
      'sequence-number': this.toSafeNumber(s['sequence-number']),
    })) as Snapshot[];

    // Normalize current-snapshot-id
    const currentSnapshotId = parsed['current-snapshot-id'] !== null
      ? this.toSafeNumber(parsed['current-snapshot-id'])
      : null;

    return {
      ...parsed,
      snapshots: normalizedSnapshots,
      'current-snapshot-id': currentSnapshotId,
    } as TableMetadata;
  }

  /**
   * Convert string/BigInt/number to a safe number, or keep as BigInt if too large.
   * Uses BigInt for values larger than MAX_SAFE_INTEGER to preserve precision.
   *
   * Note: The return type is `number` to match Snapshot type expectations,
   * but large values may actually be BigInt at runtime for precision.
   */
  private toSafeNumber(value: unknown): number {
    if (typeof value === 'string') {
      const numValue = Number(value);
      if (numValue > Number.MAX_SAFE_INTEGER) {
        // Return BigInt for large values - type says number but runtime is BigInt
        // This preserves precision for large snapshot IDs
        // Double cast required because BigInt is not assignable to number
        return BigInt(value) as unknown as number;
      }
      return numValue;
    }
    if (typeof value === 'bigint') {
      if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
        // Keep as BigInt for large values - type says number but runtime is BigInt
        // Double cast required because BigInt is not assignable to number
        return value as unknown as number;
      }
      return Number(value);
    }
    return value as number;
  }
}

// ============================================================================
// Manifest Loader
// ============================================================================

/**
 * ManifestLoader handles loading and caching of manifest files.
 * Optimized with parallel loading and caching.
 */
class ManifestLoader {
  private manifestListCache: Map<string, CachedEntry<ManifestListEntry[]>> = new Map();
  private manifestCache: Map<string, CachedEntry<ManifestEntry[]>> = new Map();
  private cacheTtlMs: number;

  constructor(
    private storage: StorageBackend,
    cacheTtlMs: number = 60000
  ) {
    this.cacheTtlMs = cacheTtlMs;
  }

  /**
   * Load manifest list for a snapshot with caching.
   */
  async loadManifestList(
    database: string,
    collection: string,
    manifestListPath: string
  ): Promise<ManifestListEntry[]> {
    const fullPath = this.resolveManifestListPath(database, collection, manifestListPath);

    // Check cache
    const cached = this.manifestListCache.get(fullPath);
    if (cached && Date.now() - cached.cachedAt < this.cacheTtlMs) {
      return cached.data;
    }

    // Load from storage
    const data = await this.storage.get(fullPath);
    if (!data) {
      throw new Error(`Manifest list not found: ${fullPath}`);
    }

    const json = new TextDecoder().decode(data);
    let manifestList: ManifestListEntry[];
    try {
      manifestList = JSON.parse(json) as ManifestListEntry[];
    } catch {
      throw new Error(`Invalid manifest list (parse error): ${fullPath}`);
    }

    // Cache the result
    this.manifestListCache.set(fullPath, {
      data: manifestList,
      cachedAt: Date.now(),
    });

    return manifestList;
  }

  /**
   * Load manifest entries with caching.
   */
  async loadManifest(manifestPath: string): Promise<ManifestEntry[]> {
    // Check cache
    const cached = this.manifestCache.get(manifestPath);
    if (cached && Date.now() - cached.cachedAt < this.cacheTtlMs) {
      return cached.data;
    }

    // Load from storage
    const data = await this.storage.get(manifestPath);
    if (!data) {
      throw new Error(`Manifest not found: ${manifestPath}`);
    }

    const json = new TextDecoder().decode(data);
    let entries: ManifestEntry[];
    try {
      const manifest = JSON.parse(json) as ManifestData;
      entries = manifest.entries ?? [];
    } catch {
      throw new Error(`Invalid manifest (parse error): ${manifestPath}`);
    }

    // Cache the result
    this.manifestCache.set(manifestPath, {
      data: entries,
      cachedAt: Date.now(),
    });

    return entries;
  }

  /**
   * Get data files from a snapshot's manifests with parallel loading.
   *
   * @param throwOnMissing - If true, throw error when manifest is not found. Default: true.
   */
  async getDataFilesForSnapshot(
    database: string,
    collection: string,
    snapshot: Snapshot,
    throwOnMissing: boolean = true
  ): Promise<string[]> {
    const manifestListPath = snapshot['manifest-list'];

    let manifestList: ManifestListEntry[];
    try {
      manifestList = await this.loadManifestList(database, collection, manifestListPath);
    } catch (error) {
      if (!throwOnMissing && error instanceof Error && error.message.includes('not found')) {
        return [];
      }
      throw error;
    }

    // Load all manifests in parallel for better performance
    const manifestPromises = manifestList.map(async (manifestEntry) => {
      const manifestPath = manifestEntry['manifest-path'];
      try {
        return await this.loadManifest(manifestPath);
      } catch (error) {
        if (!throwOnMissing && error instanceof Error && error.message.includes('not found')) {
          return [];
        }
        throw error;
      }
    });

    const allEntries = await Promise.all(manifestPromises);

    // Collect data files from all manifests
    const dataFiles: string[] = [];
    for (const entries of allEntries) {
      for (const entry of entries) {
        // Status 1 = ADDED, Status 0 = EXISTING
        // Status 2 = DELETED - should not be included
        if (entry.status !== 2) {
          dataFiles.push(entry['data-file']['file-path']);
        }
      }
    }

    return dataFiles;
  }

  /**
   * Clear all manifest caches.
   */
  clearCache(): void {
    this.manifestListCache.clear();
    this.manifestCache.clear();
  }

  /**
   * Resolve manifest list path to full path.
   */
  private resolveManifestListPath(
    database: string,
    collection: string,
    manifestListPath: string
  ): string {
    return manifestListPath.startsWith(`${database}/${collection}`)
      ? manifestListPath
      : `${database}/${collection}/_iceberg/${manifestListPath}`;
  }
}

// ============================================================================
// Time Travel Reader Implementation
// ============================================================================

/**
 * TimeTravelReader provides Iceberg-based time travel queries.
 */
export class TimeTravelReader {
  private storage: StorageBackend;
  private metadataCache: Map<string, CachedEntry<TableMetadata>> = new Map();
  private cacheTtlMs: number = 60000; // 1 minute default

  // Extracted components for better separation of concerns
  private snapshotResolver = new SnapshotResolver();
  private metadataParser = new MetadataParser();
  private manifestLoader: ManifestLoader;

  constructor(storage: StorageBackend) {
    this.storage = storage;
    this.manifestLoader = new ManifestLoader(storage, this.cacheTtlMs);
  }

  // --------------------------------------------------------------------------
  // Private Helpers
  // --------------------------------------------------------------------------

  /**
   * Get cache key for a database/collection pair.
   */
  private getCacheKey(database: string, collection: string): string {
    return `${database}/${collection}`;
  }

  /**
   * Load table metadata, using cache if available.
   */
  private async loadTableMetadata(
    database: string,
    collection: string
  ): Promise<TableMetadata> {
    const cacheKey = this.getCacheKey(database, collection);
    const cached = this.metadataCache.get(cacheKey);

    // Check if cache is still valid
    if (cached && Date.now() - cached.cachedAt < this.cacheTtlMs) {
      return cached.data;
    }

    // Load from storage
    const metadataPath = `${database}/${collection}/_iceberg/metadata/v1.metadata.json`;
    const data = await this.storage.get(metadataPath);

    if (!data) {
      throw new Error(`Table metadata not found: ${database}.${collection}`);
    }

    const json = new TextDecoder().decode(data);
    const metadata = this.metadataParser.parse(json);

    // Cache the metadata
    this.metadataCache.set(cacheKey, {
      data: metadata,
      cachedAt: Date.now(),
    });

    return metadata;
  }

  // --------------------------------------------------------------------------
  // Query by Snapshot ID
  // --------------------------------------------------------------------------

  /**
   * Read data at a specific snapshot ID.
   *
   * @param skipManifestLoading - If true, skip loading manifest files (useful when only snapshot metadata is needed)
   */
  async readAtSnapshot(
    database: string,
    collection: string,
    snapshotId: bigint | number,
    options?: { skipManifestLoading?: boolean }
  ): Promise<TimeTravelResult> {
    const normalizedId = this.snapshotResolver.normalizeSnapshotId(snapshotId);
    this.snapshotResolver.validateSnapshotId(normalizedId);

    const metadata = await this.loadTableMetadata(database, collection);
    const snapshot = this.snapshotResolver.findById(metadata, normalizedId);

    if (!snapshot) {
      throw new Error(`Snapshot not found: ${normalizedId}`);
    }

    // Skip manifest loading if requested (useful for metadata-only operations)
    let dataFiles: string[] = [];
    if (!options?.skipManifestLoading) {
      dataFiles = await this.manifestLoader.getDataFilesForSnapshot(
        database,
        collection,
        snapshot
      );
    }

    return {
      snapshotId: normalizedId,
      snapshot: this.snapshotResolver.withBigIntIds(snapshot),
      dataFiles,
    };
  }

  // --------------------------------------------------------------------------
  // Query by Timestamp
  // --------------------------------------------------------------------------

  /**
   * Read data at or before a specific timestamp.
   */
  async readAtTimestamp(
    database: string,
    collection: string,
    timestamp: number | Date | string
  ): Promise<TimeTravelResult> {
    const normalizedTimestamp = this.snapshotResolver.normalizeTimestamp(timestamp);
    this.snapshotResolver.validateTimestamp(normalizedTimestamp);

    const metadata = await this.loadTableMetadata(database, collection);
    const snapshot = this.snapshotResolver.findAtTimestamp(metadata, normalizedTimestamp);

    if (!snapshot) {
      // No snapshot exists at or before this timestamp
      return {
        snapshotId: 0n,
        snapshot: null,
        dataFiles: [],
      };
    }

    const dataFiles = await this.manifestLoader.getDataFilesForSnapshot(
      database,
      collection,
      snapshot
    );

    return {
      snapshotId: BigInt(snapshot['snapshot-id']),
      snapshot: this.snapshotResolver.withBigIntIds(snapshot),
      dataFiles,
    };
  }

  /**
   * Alias for readAtTimestamp.
   */
  async asOf(
    database: string,
    collection: string,
    timestamp: number | Date | string
  ): Promise<TimeTravelResult> {
    return this.readAtTimestamp(database, collection, timestamp);
  }

  // --------------------------------------------------------------------------
  // Snapshot History Traversal
  // --------------------------------------------------------------------------

  /**
   * List all snapshots for a collection.
   */
  async listSnapshots(
    database: string,
    collection: string,
    options?: ListSnapshotsOptions
  ): Promise<Snapshot[]> {
    const metadata = await this.loadTableMetadata(database, collection);

    // Sort snapshots by timestamp
    let snapshots = [...metadata.snapshots].sort(
      (a, b) => a['timestamp-ms'] - b['timestamp-ms']
    );

    // Reverse if requested
    if (options?.reverse) {
      snapshots = snapshots.reverse();
    }

    // Apply pagination
    const offset = options?.offset ?? 0;
    const limit = options?.limit ?? snapshots.length;

    // Convert to BigInt IDs for external API
    return snapshots.slice(offset, offset + limit).map(s => this.snapshotResolver.withBigIntIds(s));
  }

  /**
   * Get the parent snapshot of a given snapshot.
   */
  async getParentSnapshot(
    database: string,
    collection: string,
    snapshotId: bigint | number
  ): Promise<Snapshot | null> {
    const normalizedId = this.snapshotResolver.normalizeSnapshotId(snapshotId);
    const metadata = await this.loadTableMetadata(database, collection);
    const snapshot = this.snapshotResolver.findById(metadata, normalizedId);

    if (!snapshot) {
      throw new Error(`Snapshot not found: ${normalizedId}`);
    }

    const parentId = snapshot['parent-snapshot-id'];
    if (parentId === undefined || parentId === null) {
      return null;
    }

    const parent = this.snapshotResolver.findById(metadata, BigInt(parentId));
    return parent ? this.snapshotResolver.withBigIntIds(parent) : null;
  }

  /**
   * Get the ancestry chain of a snapshot.
   */
  async getSnapshotAncestry(
    database: string,
    collection: string,
    snapshotId: bigint | number,
    options?: AncestryOptions
  ): Promise<Snapshot[]> {
    const normalizedId = this.snapshotResolver.normalizeSnapshotId(snapshotId);
    const metadata = await this.loadTableMetadata(database, collection);
    const snapshot = this.snapshotResolver.findById(metadata, normalizedId);

    if (!snapshot) {
      throw new Error(`Snapshot not found: ${normalizedId}`);
    }

    const ancestry: Snapshot[] = [];
    let currentParentId = snapshot['parent-snapshot-id'];
    const maxDepth = options?.maxDepth ?? Infinity;

    while (
      currentParentId !== undefined &&
      currentParentId !== null &&
      ancestry.length < maxDepth
    ) {
      const parent = this.snapshotResolver.findById(metadata, BigInt(currentParentId));
      if (!parent) {
        break;
      }
      ancestry.push(this.snapshotResolver.withBigIntIds(parent));
      currentParentId = parent['parent-snapshot-id'];
    }

    return ancestry;
  }

  /**
   * Get the snapshot log (history).
   */
  async getSnapshotLog(
    database: string,
    collection: string
  ): Promise<Snapshot[]> {
    const metadata = await this.loadTableMetadata(database, collection);

    // Return snapshots sorted by timestamp (chronological order)
    return [...metadata.snapshots]
      .sort((a, b) => a['timestamp-ms'] - b['timestamp-ms'])
      .map(s => this.snapshotResolver.withBigIntIds(s));
  }

  /**
   * Find common ancestor between two snapshots.
   */
  async findCommonAncestor(
    database: string,
    collection: string,
    snapshotId1: bigint | number,
    snapshotId2: bigint | number
  ): Promise<Snapshot | null> {
    const id1 = this.snapshotResolver.normalizeSnapshotId(snapshotId1);
    const id2 = this.snapshotResolver.normalizeSnapshotId(snapshotId2);
    const metadata = await this.loadTableMetadata(database, collection);

    // Build ancestry set for first snapshot (including itself)
    const ancestors1 = new Set<number>();
    ancestors1.add(Number(id1));

    let currentId: number | undefined = Number(id1);
    while (currentId !== undefined) {
      const snapshot = this.snapshotResolver.findById(metadata, BigInt(currentId));
      if (!snapshot) break;
      const parentId = snapshot['parent-snapshot-id'];
      if (parentId !== undefined && parentId !== null) {
        ancestors1.add(parentId);
        currentId = parentId;
      } else {
        currentId = undefined;
      }
    }

    // Walk ancestry of second snapshot, finding first common ancestor
    currentId = Number(id2);
    while (currentId !== undefined) {
      // Check if current snapshot is in ancestors1
      if (ancestors1.has(currentId)) {
        const ancestor = this.snapshotResolver.findById(metadata, BigInt(currentId));
        return ancestor ? this.snapshotResolver.withBigIntIds(ancestor) : null;
      }
      const snapshot = this.snapshotResolver.findById(metadata, BigInt(currentId));
      if (!snapshot) break;
      const parentId = snapshot['parent-snapshot-id'];
      if (parentId !== undefined && parentId !== null) {
        // Check if parent is in ancestors1
        if (ancestors1.has(parentId)) {
          const ancestor = this.snapshotResolver.findById(metadata, BigInt(parentId));
          return ancestor ? this.snapshotResolver.withBigIntIds(ancestor) : null;
        }
        currentId = parentId;
      } else {
        currentId = undefined;
      }
    }

    return null;
  }

  // --------------------------------------------------------------------------
  // Document Queries
  // --------------------------------------------------------------------------

  /**
   * Read documents at a specific snapshot.
   */
  async readDocumentsAtSnapshot<T extends Document>(
    database: string,
    collection: string,
    snapshotId: bigint | number,
    _filter?: Filter<T>,
    _options?: ReadDocumentsOptions
  ): Promise<SnapshotQueryResult<T>> {
    const result = await this.readAtSnapshot(database, collection, snapshotId);

    // Note: Actual document reading would require Parquet parsing
    // For now, we return the snapshot info with empty documents
    // (full document reading would be implemented when integrating with Parquet reader)
    return {
      ...result,
      documents: [],
    };
  }

  // --------------------------------------------------------------------------
  // Diff and Changes
  // --------------------------------------------------------------------------

  /**
   * Compute diff between two snapshots.
   */
  async diffSnapshots(
    database: string,
    collection: string,
    fromSnapshotId: bigint | number,
    toSnapshotId: bigint | number
  ): Promise<SnapshotDiff> {
    const fromResult = await this.readAtSnapshot(
      database,
      collection,
      fromSnapshotId
    );
    const toResult = await this.readAtSnapshot(
      database,
      collection,
      toSnapshotId
    );

    if (!fromResult.snapshot || !toResult.snapshot) {
      throw new Error('One or both snapshots not found');
    }

    const fromFiles = new Set(fromResult.dataFiles);
    const toFiles = new Set(toResult.dataFiles);

    const addedFiles = toResult.dataFiles.filter((f) => !fromFiles.has(f));
    const removedFiles = fromResult.dataFiles.filter((f) => !toFiles.has(f));

    return {
      fromSnapshot: fromResult.snapshot,
      toSnapshot: toResult.snapshot,
      addedFiles,
      removedFiles,
    };
  }

  /**
   * Read changes since a given snapshot.
   */
  async readChangesSince(
    database: string,
    collection: string,
    snapshotId: bigint | number,
    options?: ReadChangesOptions
  ): Promise<ChangesResult> {
    const normalizedFromId = this.snapshotResolver.normalizeSnapshotId(snapshotId);
    const metadata = await this.loadTableMetadata(database, collection);

    // Determine target snapshot
    let toSnapshotId: bigint;
    if (options?.toSnapshotId) {
      toSnapshotId = this.snapshotResolver.normalizeSnapshotId(options.toSnapshotId);
    } else if (metadata['current-snapshot-id'] !== null) {
      toSnapshotId = BigInt(metadata['current-snapshot-id']);
    } else {
      throw new Error('No current snapshot');
    }

    // Find snapshots between from and to
    const fromTimestamp = this.snapshotResolver.findById(
      metadata,
      normalizedFromId
    )?.['timestamp-ms'];
    const toSnapshot = this.snapshotResolver.findById(metadata, toSnapshotId);

    if (!fromTimestamp || !toSnapshot) {
      throw new Error('Invalid snapshot range');
    }

    const intermediateSnapshots = metadata.snapshots.filter((s) => {
      const ts = s['timestamp-ms'];
      return (
        ts > fromTimestamp && ts <= toSnapshot['timestamp-ms']
      );
    }).sort((a, b) => a['timestamp-ms'] - b['timestamp-ms'])
      .map(s => this.snapshotResolver.withBigIntIds(s));

    return {
      fromSnapshotId: normalizedFromId,
      toSnapshotId,
      intermediateSnapshots,
    };
  }

  // --------------------------------------------------------------------------
  // Collection API Integration
  // --------------------------------------------------------------------------

  /**
   * Create a read-only time travel collection view.
   */
  async createTimeTravelCollection<T extends Document>(
    database: string,
    collection: string,
    options: TimeTravelOptions
  ): Promise<TimeTravelCollectionView<T>> {
    // Resolve the snapshot
    let snapshot: Snapshot | null = null;

    if (options.snapshotId !== undefined) {
      const result = await this.readAtSnapshot(
        database,
        collection,
        options.snapshotId
      );
      snapshot = result.snapshot;
    } else if (options.timestamp !== undefined) {
      const result = await this.readAtTimestamp(
        database,
        collection,
        options.timestamp
      );
      snapshot = result.snapshot;
    } else {
      throw new Error(
        'Either snapshotId or timestamp must be provided'
      );
    }

    const readOnlyError = () => {
      throw new Error('Write not allowed: time travel collection is read-only');
    };

    return {
      name: collection,
      isReadOnly: true as const,

      async getSnapshot(): Promise<Snapshot | null> {
        return snapshot;
      },

      async *find(_filter?: Filter<T>): AsyncIterable<WithId<T>> {
        // Would need Parquet integration to actually read documents
      },

      async findOne(_filter?: Filter<T>): Promise<WithId<T> | null> {
        return null;
      },

      async countDocuments(_filter?: Filter<T>): Promise<number> {
        return 0;
      },

      async insertOne(): Promise<never> {
        return readOnlyError();
      },

      async updateOne(): Promise<never> {
        return readOnlyError();
      },

      async deleteOne(): Promise<never> {
        return readOnlyError();
      },
    };
  }

  // --------------------------------------------------------------------------
  // Caching
  // --------------------------------------------------------------------------

  /**
   * Invalidate cached metadata for a collection.
   * Also clears manifest caches for the collection.
   */
  invalidateCache(database: string, collection: string): void {
    const cacheKey = this.getCacheKey(database, collection);
    this.metadataCache.delete(cacheKey);
    // Also clear manifest caches to ensure consistency
    this.manifestLoader.clearCache();
  }
}
