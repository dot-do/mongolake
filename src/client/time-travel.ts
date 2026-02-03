/**
 * MongoLake Time Travel Collection
 *
 * Read-only view of a collection at a specific point in time.
 */

import type {
  Document,
  WithId,
  Filter,
  AggregationStage,
  FindOptions,
  AggregateOptions,
  CollectionManifest,
  CollectionSchema,
} from '@types';
import {
  SnapshotManager,
  type Snapshot,
  type TableMetadata,
} from '@dotdo/iceberg';
import type { StorageBackend } from '@storage/index.js';
import { readParquet } from '@parquet/io.js';
import { sortDocuments } from '@utils/sort.js';
import { matchesFilter } from '@utils/filter.js';
import { applyProjection } from '@utils/projection.js';
import { logger } from '@utils/logger.js';
import type { Database } from './database.js';
import { TimeTravelFindCursor } from './cursors.js';
import { TimeTravelAggregationCursor } from './aggregation.js';

// ============================================================================
// Types
// ============================================================================

/** Options for time travel query */
export interface TimeTravelOptions {
  /** Query at specific timestamp (milliseconds since epoch) */
  timestamp?: number;
  /** Query at specific snapshot ID */
  snapshotId?: number;
}

// ============================================================================
// TimeTravelCollection
// ============================================================================

/**
 * Read-only view of a collection at a specific point in time.
 *
 * TimeTravelCollection provides the same read APIs as Collection but
 * queries data as it existed at a specific snapshot or timestamp.
 * Write operations are not supported on time travel views.
 *
 * @example
 * ```typescript
 * // Get collection view at a specific timestamp
 * const historicalView = collection.asOf(new Date('2024-01-01'));
 *
 * // Query historical data
 * const oldDocs = await historicalView.find({ status: 'active' }).toArray();
 *
 * // Count historical documents
 * const count = await historicalView.countDocuments();
 * ```
 */
export class TimeTravelCollection<T extends Document = Document> {
  private manifest: CollectionManifest | null = null;
  private snapshot: Snapshot | null = null;
  private snapshotDataFiles: Set<string> | null = null;

  /** This is a read-only view - write operations are not supported */
  public readonly isReadOnly: true = true;

  constructor(
    public readonly name: string,
    private db: Database,
    private storage: StorageBackend,
    private timeTravelOptions: TimeTravelOptions,
    private schema?: CollectionSchema
  ) {}

  // --------------------------------------------------------------------------
  // Read Operations (Same API as Collection)
  // --------------------------------------------------------------------------

  /**
   * Find a single document
   */
  async findOne(filter?: Filter<T>, options?: FindOptions): Promise<WithId<T> | null> {
    const cursor = this.find(filter, { ...options, limit: 1 });
    const results = await cursor.toArray();
    return results[0] || null;
  }

  /**
   * Find documents
   */
  find(filter?: Filter<T>, options?: FindOptions): TimeTravelFindCursor<T> {
    return new TimeTravelFindCursor<T>(this, filter, options);
  }

  /**
   * Count documents
   */
  async countDocuments(filter?: Filter<T>): Promise<number> {
    const docs = await this.find(filter).toArray();
    return docs.length;
  }

  /**
   * Estimated document count (fast, approximate)
   */
  async estimatedDocumentCount(): Promise<number> {
    await this.ensureSnapshot();
    // Use snapshot summary if available
    if (this.snapshot) {
      const totalRecords = this.snapshot.summary['total-records'];
      if (totalRecords) {
        return parseInt(totalRecords, 10);
      }
    }
    // Fall back to counting documents
    const docs = await this.readDocuments();
    return docs.length;
  }

  /**
   * Get distinct values for a field
   */
  async distinct<K extends keyof T & keyof WithId<T>>(field: K, filter?: Filter<T>): Promise<T[K][]> {
    const docs = await this.find(filter).toArray();
    const values = new Set<T[K]>();

    for (const doc of docs) {
      const value = doc[field] as T[K];
      if (value !== undefined) {
        values.add(value);
      }
    }

    return Array.from(values);
  }

  /**
   * Run aggregation pipeline
   */
  aggregate<R extends Document = Document>(
    pipeline: AggregationStage[],
    options?: AggregateOptions
  ): TimeTravelAggregationCursor<R> {
    // TimeTravelAggregationCursor needs TimeTravelCollection<Document> for its source operations.
    // Single cast is safe because T extends Document, making this assignment valid at runtime.
    return new TimeTravelAggregationCursor<R>(this as TimeTravelCollection<Document>, pipeline, options);
  }

  /**
   * Get the snapshot this time travel view is based on.
   * Returns null if no snapshot was found for the given time/ID.
   */
  async getSnapshot(): Promise<Snapshot | null> {
    await this.ensureSnapshot();
    return this.snapshot;
  }

  /**
   * Get the timestamp of the snapshot this view is based on.
   */
  async getSnapshotTimestamp(): Promise<Date | null> {
    await this.ensureSnapshot();
    if (!this.snapshot) {
      return null;
    }
    return new Date(this.snapshot['timestamp-ms']);
  }

  /**
   * Get a sibling time travel collection from the same database at the same point in time.
   * Used internally for $lookup operations.
   * @internal
   */
  getSiblingCollection<U extends Document = Document>(name: string): TimeTravelCollection<U> {
    return new TimeTravelCollection<U>(
      name,
      this.db,
      this.storage,
      this.timeTravelOptions
    );
  }

  // --------------------------------------------------------------------------
  // Internal Methods
  // --------------------------------------------------------------------------

  /** Flag to track if snapshot has been loaded */
  private snapshotLoaded: boolean = false;

  /**
   * Ensure snapshot is loaded
   * @internal
   */
  private async ensureSnapshot(): Promise<void> {
    if (this.snapshotLoaded) {
      return;
    }

    // Load Iceberg metadata
    const metadataPath = `${this.db.getPath()}/${this.name}/_iceberg/metadata/v1.metadata.json`;
    const metadataData = await this.storage.get(metadataPath);

    if (!metadataData) {
      // No Iceberg metadata, fall back to manifest-based filtering
      await this.ensureManifest();
      this.snapshot = null;
      this.snapshotDataFiles = null;
      this.snapshotLoaded = true;
      return;
    }

    try {
      const tableMetadata = JSON.parse(new TextDecoder().decode(metadataData)) as TableMetadata;
      const snapshotManager = new SnapshotManager(tableMetadata);

      // Find the target snapshot
      if (this.timeTravelOptions.snapshotId !== undefined) {
        this.snapshot = snapshotManager.getSnapshotById(this.timeTravelOptions.snapshotId) ?? null;
      } else if (this.timeTravelOptions.timestamp !== undefined) {
        this.snapshot = snapshotManager.getSnapshotAtTimestamp(this.timeTravelOptions.timestamp) ?? null;
      }

      if (this.snapshot) {
        // Load data files from the snapshot's manifest list
        await this.loadSnapshotDataFiles();
      } else {
        this.snapshotDataFiles = null;
      }
      this.snapshotLoaded = true;
    } catch {
      // If parsing fails, fall back to timestamp-based filtering
      this.snapshot = null;
      this.snapshotDataFiles = null;
      this.snapshotLoaded = true;
    }
  }

  /**
   * Load data files from the snapshot's manifest list
   * @internal
   */
  private async loadSnapshotDataFiles(): Promise<void> {
    if (!this.snapshot) {
      this.snapshotDataFiles = new Set();
      return;
    }

    const manifestListPath = this.snapshot['manifest-list'];
    const manifestData = await this.storage.get(manifestListPath);

    if (!manifestData) {
      // If manifest list is not found, try to parse file names from manifest path
      // or fall back to timestamp-based filtering
      this.snapshotDataFiles = null;
      return;
    }

    try {
      // Try to parse as JSON manifest list (simpler format for testing)
      const manifestList = JSON.parse(new TextDecoder().decode(manifestData));
      this.snapshotDataFiles = new Set<string>();

      // Iterate through manifest files and collect data file paths
      for (const manifest of manifestList) {
        const manifestPath = manifest['manifest-path'];
        const manifestFileData = await this.storage.get(manifestPath);

        if (manifestFileData) {
          try {
            const manifestEntries = JSON.parse(new TextDecoder().decode(manifestFileData));
            for (const entry of manifestEntries.entries || []) {
              if (entry.status !== 2) {
                // Not deleted
                this.snapshotDataFiles.add(entry['data-file']['file-path']);
              }
            }
          } catch {
            // Binary Avro format - would need Avro parser
            // For now, fall back to timestamp filtering
          }
        }
      }
    } catch {
      // Binary Avro format - fall back to timestamp filtering
      this.snapshotDataFiles = null;
    }
  }

  /**
   * Ensure manifest exists
   * @internal
   */
  private async ensureManifest(): Promise<void> {
    if (this.manifest) return;

    const manifestPath = `${this.db.getPath()}/${this.name}/_manifest.json`;
    const data = await this.storage.get(manifestPath);

    if (data) {
      this.manifest = JSON.parse(new TextDecoder().decode(data));
    } else {
      this.manifest = {
        name: this.name,
        files: [],
        schema: this.schema || {},
        currentSeq: 0,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      };
    }
  }

  /**
   * Read all documents at the specified snapshot/timestamp
   * @internal
   */
  async readDocuments(filter?: Filter<T>, options?: FindOptions): Promise<WithId<T>[]> {
    await this.ensureSnapshot();
    await this.ensureManifest();

    // Get all Parquet files for this collection
    const dbPath = this.db.getPath();
    const collectionPrefix = `${dbPath}/${this.name}`;
    const files = await this.storage.list(dbPath);

    let parquetFiles = files.filter(
      (f) => f.startsWith(collectionPrefix) && f.endsWith('.parquet') && !f.includes('/_')
    );

    // Filter files based on snapshot or timestamp
    if (this.snapshotDataFiles !== null && this.snapshotDataFiles.size > 0) {
      // Use exact file list from snapshot manifest
      const dataFiles = this.snapshotDataFiles;
      parquetFiles = parquetFiles.filter((f) => dataFiles.has(f));
    } else if (this.timeTravelOptions.timestamp !== undefined) {
      // Fall back to timestamp-based filtering using file naming convention
      // Files are named: {collection}_{timestamp}_{seq}.parquet
      const targetTimestamp = this.timeTravelOptions.timestamp;
      parquetFiles = parquetFiles.filter((f) => {
        const match = f.match(/_(\d+)_\d+\.parquet$/);
        if (match) {
          const fileTimestamp = parseInt(match[1]!, 10);
          return fileTimestamp <= targetTimestamp;
        }
        return true; // Include files without timestamp in name
      });
    } else if (this.timeTravelOptions.snapshotId !== undefined && this.snapshot) {
      // Filter by snapshot timestamp if we found the snapshot
      const snapshotTimestamp = this.snapshot['timestamp-ms'];
      parquetFiles = parquetFiles.filter((f) => {
        const match = f.match(/_(\d+)_\d+\.parquet$/);
        if (match) {
          const fileTimestamp = parseInt(match[1]!, 10);
          return fileTimestamp <= snapshotTimestamp;
        }
        return true;
      });
    }

    // Read and deduplicate
    const docsById = new Map<string, { seq: number; op: string; doc: T }>();

    for (const file of parquetFiles) {
      const data = await this.storage.get(file);
      if (!data) continue;

      try {
        const rows = await readParquet<T>(data);

        for (const row of rows) {
          // For time travel, also filter by sequence number if available
          const existing = docsById.get(row._id);
          if (!existing || row._seq > existing.seq) {
            docsById.set(row._id, {
              seq: row._seq,
              op: row._op,
              doc: row.doc,
            });
          }
        }
      } catch (error) {
        if (options?.skipCorruptedFiles) {
          logger.warn('Skipping corrupted Parquet file', {
            file,
            error: error instanceof Error ? error : String(error),
          });
        } else {
          throw new Error(`Failed to read Parquet file ${file}: ${error instanceof Error ? error.message : String(error)}`);
        }
      }
    }

    // Filter out deletes and apply filter
    const results: WithId<T>[] = [];

    for (const [id, { op, doc }] of docsById) {
      if (op === 'd') continue;

      const fullDoc = { ...doc, _id: id } as WithId<T>;

      if (!filter || matchesFilter(fullDoc, filter as Filter<WithId<T>>)) {
        results.push(fullDoc);
      }
    }

    // Apply options
    let output = results;

    if (options?.sort) {
      output = sortDocuments(output, options.sort);
    }

    if (options?.skip) {
      output = output.slice(options.skip);
    }

    if (options?.limit) {
      output = output.slice(0, options.limit);
    }

    if (options?.projection) {
      output = output.map((doc) => applyProjection(doc, options.projection!) as WithId<T>);
    }

    return output;
  }
}
