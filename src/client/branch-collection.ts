/**
 * MongoLake Branch Collection
 *
 * Branch-aware collection that reads from and writes to a specific branch.
 */

import type {
  Document,
  WithId,
  Filter,
  Update,
  FindOptions,
  UpdateOptions,
  DeleteOptions,
  InsertOneResult,
  InsertManyResult,
  UpdateResult,
  DeleteResult,
  ObjectId,
  CollectionManifest,
  CollectionSchema,
} from '@types';
import type { StorageBackend } from '@storage/index.js';
import { writeParquet, readParquet } from '@parquet/io.js';
import { sortDocuments } from '@utils/sort.js';
import { matchesFilter } from '@utils/filter.js';
import { applyUpdate, createUpsertDocument } from '@utils/update.js';
import { applyProjection } from '@utils/projection.js';
import { logger } from '@utils/logger.js';
import {
  validateFilter,
  validateUpdate,
  validateDocument,
} from '@utils/validation.js';
import {
  DEFAULT_BRANCH,
  type BranchMetadata,
} from '@mongolake/branching/index.js';
import type { Database } from './database.js';
import { Collection } from './collection.js';

// ============================================================================
// BranchCollection
// ============================================================================

/**
 * Branch-aware collection that reads from and writes to a specific branch.
 *
 * Reads layer branch changes on top of base snapshot data.
 * Writes are isolated to the branch and don't affect main.
 *
 * @example
 * ```typescript
 * // Get a collection on a feature branch
 * const users = db.collection('users', { branch: 'feature-branch' });
 *
 * // Writes go to the branch, not main
 * await users.insertOne({ name: 'Alice' });
 *
 * // Reads see branch changes layered on base
 * const docs = await users.find().toArray();
 * ```
 */
export class BranchCollection<T extends Document = Document> extends Collection<T> {
  private branchMetadata: BranchMetadata | null = null;
  private branchManifest: CollectionManifest | null = null;
  private branchCurrentSeq: number = 0;
  private baseSnapshotTimestamp: number | null = null;

  constructor(
    name: string,
    db: Database,
    storage: StorageBackend,
    private readonly branchName: string,
    schema?: CollectionSchema
  ) {
    super(name, db, storage, schema);
  }

  /**
   * Get the branch metadata, asserting it is loaded.
   * Call this after ensureBranch() to get properly typed access to branchMetadata.
   * @throws Error if branch metadata is not initialized
   */
  private getBranchMetadataInternal(): BranchMetadata {
    if (!this.branchMetadata) {
      throw new Error('BranchCollection: branch metadata not initialized. Call ensureBranch() first.');
    }
    return this.branchMetadata;
  }

  /**
   * Get the branch manifest, asserting it is loaded.
   * Call this after ensureBranchManifest() to get properly typed access to branchManifest.
   * @throws Error if branch manifest is not initialized
   */
  private getBranchManifest(): CollectionManifest {
    if (!this.branchManifest) {
      throw new Error('BranchCollection: branch manifest not initialized. Call ensureBranchManifest() first.');
    }
    return this.branchManifest;
  }

  /**
   * Get the branch name this collection is operating on.
   */
  override get branch(): string | undefined {
    return this.branchName;
  }

  /**
   * Check if this collection is on a branch (not main).
   */
  override isOnBranch(): boolean {
    return true;
  }

  /**
   * Get the branch metadata for this collection's branch.
   * Useful for inspecting branch state.
   */
  async getBranchMetadata(): Promise<BranchMetadata> {
    await this.ensureBranch();
    return this.getBranchMetadataInternal();
  }

  /**
   * Get the parent branch name (or 'main' if at root).
   */
  async getParentBranch(): Promise<string> {
    const metadata = await this.getBranchMetadata();
    return metadata.parentBranch || DEFAULT_BRANCH;
  }

  /**
   * Ensure branch metadata is loaded and validated.
   * @internal
   */
  private async ensureBranch(): Promise<void> {
    if (this.branchMetadata) return;

    const branchStore = this.db.getBranchStore();
    const branch = await branchStore.getBranch(this.branchName);

    if (!branch) {
      throw new Error(`Branch "${this.branchName}" not found`);
    }

    this.branchMetadata = branch;

    // Calculate base snapshot timestamp from branch creation
    // This is used to filter which files from main are visible
    this.baseSnapshotTimestamp = new Date(branch.createdAt).getTime();
  }

  /**
   * Ensure branch manifest exists.
   * @internal
   */
  private async ensureBranchManifest(): Promise<void> {
    if (this.branchManifest) return;

    await this.ensureBranch();

    const manifestPath = this.getBranchPath(`${this.name}/_manifest.json`);
    const data = await this.storage.get(manifestPath);

    if (data) {
      this.branchManifest = JSON.parse(new TextDecoder().decode(data));
      const manifest = this.getBranchManifest();
      this.branchCurrentSeq = manifest.currentSeq;
    } else {
      this.branchManifest = {
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
   * Get the storage path for branch-specific data.
   * @internal
   */
  private getBranchPath(relativePath: string): string {
    // Branch data is stored in: {db}/branches/{branchName}/{relativePath}
    return `${this.db.getPath()}/branches/${this.branchName}/${relativePath}`;
  }

  /**
   * Get the base (main) storage path.
   * @internal
   */
  private getBasePath(): string {
    return this.db.getPath();
  }

  // --------------------------------------------------------------------------
  // Read Operations (Layer branch on base)
  // --------------------------------------------------------------------------

  /**
   * Read all documents, layering branch changes on top of base.
   * @internal
   */
  override async readDocuments(filter?: Filter<T>, options?: FindOptions): Promise<WithId<T>[]> {
    await this.ensureBranch();
    await this.ensureBranchManifest();

    // Step 1: Read base documents (from main, filtered by branch creation time)
    const baseDocsById = await this.readBaseDocuments();

    // Step 2: Read branch-specific documents
    const branchDocsById = await this.readBranchDocuments();

    // Step 3: Layer branch changes on top of base
    // Branch data always takes precedence over base data for the same document ID
    const mergedDocsById = new Map<string, { seq: number; op: string; doc: T; isBranch: boolean }>();

    // Start with base documents
    for (const [id, entry] of baseDocsById) {
      mergedDocsById.set(id, { ...entry, isBranch: false });
    }

    // Apply branch changes (always overwrites base for same ID)
    for (const [id, entry] of branchDocsById) {
      const existing = mergedDocsById.get(id);
      // Branch entry always wins if document ID exists, or it's a new branch document
      // For branch entries with the same ID, use sequence number to determine latest
      if (!existing || !existing.isBranch || entry.seq > existing.seq) {
        mergedDocsById.set(id, { ...entry, isBranch: true });
      }
    }

    // Step 4: Filter out deletes and apply query filter
    const results: WithId<T>[] = [];

    for (const [id, { op, doc }] of mergedDocsById) {
      if (op === 'd') continue;

      const fullDoc = { ...doc, _id: id } as WithId<T>;

      if (!filter || matchesFilter(fullDoc, filter as Filter<WithId<T>>)) {
        results.push(fullDoc);
      }
    }

    // Step 5: Apply options
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

  /**
   * Read documents from base (main and parent branches) that existed at branch creation time.
   * @internal
   */
  private async readBaseDocuments(): Promise<Map<string, { seq: number; op: string; doc: T }>> {
    const docsById = new Map<string, { seq: number; op: string; doc: T }>();

    // Read from main database
    await this.readMainDocuments(docsById);

    // Read from parent branch if this branch has a non-main parent
    const parentBranch = this.branchMetadata?.parentBranch;
    if (parentBranch && parentBranch !== DEFAULT_BRANCH) {
      await this.readParentBranchDocuments(docsById, parentBranch);
    }

    return docsById;
  }

  /**
   * Read documents from main database.
   * @internal
   */
  private async readMainDocuments(
    docsById: Map<string, { seq: number; op: string; doc: T }>
  ): Promise<void> {
    const basePath = this.getBasePath();
    const collectionPrefix = `${basePath}/${this.name}`;
    const files = await this.storage.list(basePath);

    // Filter to parquet files that existed at branch creation time
    const parquetFiles = files.filter((f) => {
      if (!f.startsWith(collectionPrefix) || !f.endsWith('.parquet') || f.includes('/_')) {
        return false;
      }
      // Also exclude branch-specific files
      if (f.includes('/branches/')) {
        return false;
      }
      // Filter by timestamp if we have a base snapshot timestamp
      if (this.baseSnapshotTimestamp !== null) {
        const match = f.match(/_(\d+)_\d+\.parquet$/);
        if (match) {
          const fileTimestamp = parseInt(match[1]!, 10);
          return fileTimestamp <= this.baseSnapshotTimestamp;
        }
      }
      return true;
    });

    for (const file of parquetFiles) {
      const data = await this.storage.get(file);
      if (!data) continue;

      try {
        const rows = await readParquet<T>(data);

        for (const row of rows) {
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
        // Skip corrupted files
        logger.warn('Skipping corrupted Parquet file', {
          file,
          error: error instanceof Error ? error : String(error),
        });
      }
    }
  }

  /**
   * Read documents from a parent branch's storage.
   * @internal
   */
  private async readParentBranchDocuments(
    docsById: Map<string, { seq: number; op: string; doc: T }>,
    parentBranchName: string
  ): Promise<void> {
    const parentBranchPath = `${this.db.getPath()}/branches/${parentBranchName}/`;
    const collectionPrefix = `${parentBranchPath}${this.name}`;

    const files = await this.storage.list(parentBranchPath);

    const parquetFiles = files.filter(
      (f) => f.startsWith(collectionPrefix) && f.endsWith('.parquet') && !f.includes('/_')
    );

    for (const file of parquetFiles) {
      const data = await this.storage.get(file);
      if (!data) continue;

      try {
        const rows = await readParquet<T>(data);

        for (const row of rows) {
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
        logger.warn('Skipping corrupted Parquet file', {
          file,
          error: error instanceof Error ? error : String(error),
        });
      }
    }
  }

  /**
   * Read documents that were written on this branch.
   * @internal
   */
  private async readBranchDocuments(): Promise<Map<string, { seq: number; op: string; doc: T }>> {
    const branchBasePath = this.getBranchPath('');
    const collectionPrefix = this.getBranchPath(this.name);

    const files = await this.storage.list(branchBasePath);

    const parquetFiles = files.filter(
      (f) => f.startsWith(collectionPrefix) && f.endsWith('.parquet') && !f.includes('/_')
    );

    const docsById = new Map<string, { seq: number; op: string; doc: T }>();

    for (const file of parquetFiles) {
      const data = await this.storage.get(file);
      if (!data) continue;

      try {
        const rows = await readParquet<T>(data);

        for (const row of rows) {
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
        logger.warn('Skipping corrupted Parquet file', {
          file,
          error: error instanceof Error ? error : String(error),
        });
      }
    }

    return docsById;
  }

  // --------------------------------------------------------------------------
  // Write Operations (Write to branch only)
  // --------------------------------------------------------------------------

  /**
   * Insert a single document into the branch.
   */
  override async insertOne(doc: T): Promise<InsertOneResult> {
    const result = await this.insertMany([doc]);
    return {
      acknowledged: result.acknowledged,
      insertedId: result.insertedIds[0]!,
    };
  }

  /**
   * Insert multiple documents into the branch.
   */
  override async insertMany(docs: T[]): Promise<InsertManyResult> {
    // Validate all documents before inserting
    for (const doc of docs) {
      validateDocument(doc);
    }

    await this.ensureBranch();
    await this.ensureBranchManifest();

    const insertedIds: { [key: number]: string | ObjectId } = {};
    const rows: Array<{ _id: string; _seq: number; _op: 'i'; doc: T }> = [];

    for (let i = 0; i < docs.length; i++) {
      const doc = { ...docs[i] };

      // Generate _id if not provided
      if (!doc._id) {
        doc._id = crypto.randomUUID();
      }

      const id = this.extractDocumentId(doc);
      insertedIds[i] = doc._id;

      rows.push({
        _id: id,
        _seq: ++this.branchCurrentSeq,
        _op: 'i',
        doc: doc as T,
      });
    }

    // Write to branch-specific delta file
    await this.writeBranchDelta(rows);

    return {
      acknowledged: true,
      insertedCount: docs.length,
      insertedIds,
    };
  }

  /**
   * Update a single document on the branch.
   */
  override async updateOne(filter: Filter<T>, update: Update<T>, options?: UpdateOptions): Promise<UpdateResult> {
    validateFilter(filter);
    validateUpdate(update);

    const doc = await this.findOne(filter);

    if (!doc) {
      if (options?.upsert) {
        // Upsert: create new document combining filter fields + update operations
        // MongoDB behavior: equality fields from filter are used as initial document values
        const newDoc = createUpsertDocument<T>(filter as Record<string, unknown>, update);
        const result = await this.insertOne(newDoc);
        return {
          acknowledged: true,
          matchedCount: 0,
          modifiedCount: 0,
          upsertedCount: 1,
          upsertedId: result.insertedId,
        };
      }
      return {
        acknowledged: true,
        matchedCount: 0,
        modifiedCount: 0,
        upsertedCount: 0,
      };
    }

    const updated = applyUpdate(doc, update);
    const id = this.extractDocumentId(doc);

    await this.writeBranchDelta([
      {
        _id: id,
        _seq: ++this.branchCurrentSeq,
        _op: 'u' as const,
        doc: updated,
      },
    ]);

    return {
      acknowledged: true,
      matchedCount: 1,
      modifiedCount: 1,
      upsertedCount: 0,
    };
  }

  /**
   * Update multiple documents on the branch.
   */
  override async updateMany(filter: Filter<T>, update: Update<T>, options?: UpdateOptions): Promise<UpdateResult> {
    validateFilter(filter);
    validateUpdate(update);

    const docs = await this.find(filter).toArray();

    if (docs.length === 0) {
      if (options?.upsert) {
        // Upsert: create new document combining filter fields + update operations
        // MongoDB behavior: equality fields from filter are used as initial document values
        // Note: updateMany with upsert only creates one document (same as MongoDB)
        const newDoc = createUpsertDocument<T>(filter as Record<string, unknown>, update);
        const result = await this.insertOne(newDoc);
        return {
          acknowledged: true,
          matchedCount: 0,
          modifiedCount: 0,
          upsertedCount: 1,
          upsertedId: result.insertedId,
        };
      }
      return {
        acknowledged: true,
        matchedCount: 0,
        modifiedCount: 0,
        upsertedCount: 0,
      };
    }

    const rows: Array<{ _id: string; _seq: number; _op: 'u'; doc: T }> = [];

    for (const doc of docs) {
      const updated = applyUpdate(doc, update);
      const id = this.extractDocumentId(doc);

      rows.push({
        _id: id,
        _seq: ++this.branchCurrentSeq,
        _op: 'u',
        doc: updated,
      });
    }

    await this.writeBranchDelta(rows);

    return {
      acknowledged: true,
      matchedCount: docs.length,
      modifiedCount: docs.length,
      upsertedCount: 0,
    };
  }

  /**
   * Replace a single document on the branch.
   */
  override async replaceOne(filter: Filter<T>, replacement: T, options?: UpdateOptions): Promise<UpdateResult> {
    validateFilter(filter);
    validateDocument(replacement);

    const doc = await this.findOne(filter);

    if (!doc) {
      if (options?.upsert) {
        const result = await this.insertOne(replacement);
        return {
          acknowledged: true,
          matchedCount: 0,
          modifiedCount: 0,
          upsertedCount: 1,
          upsertedId: result.insertedId,
        };
      }
      return {
        acknowledged: true,
        matchedCount: 0,
        modifiedCount: 0,
        upsertedCount: 0,
      };
    }

    const id = this.extractDocumentId(doc);
    const newDoc = { ...replacement, _id: doc._id };

    await this.writeBranchDelta([
      {
        _id: id,
        _seq: ++this.branchCurrentSeq,
        _op: 'u' as const,
        doc: newDoc as T,
      },
    ]);

    return {
      acknowledged: true,
      matchedCount: 1,
      modifiedCount: 1,
      upsertedCount: 0,
    };
  }

  /**
   * Delete a single document on the branch.
   */
  override async deleteOne(filter: Filter<T>, _options?: DeleteOptions): Promise<DeleteResult> {
    validateFilter(filter);

    const doc = await this.findOne(filter);

    if (!doc) {
      return {
        acknowledged: true,
        deletedCount: 0,
      };
    }

    const id = this.extractDocumentId(doc);

    await this.writeBranchDelta([
      {
        _id: id,
        _seq: ++this.branchCurrentSeq,
        _op: 'd' as const,
        doc: {} as T,
      },
    ]);

    return {
      acknowledged: true,
      deletedCount: 1,
    };
  }

  /**
   * Delete multiple documents on the branch.
   */
  override async deleteMany(filter: Filter<T>, _options?: DeleteOptions): Promise<DeleteResult> {
    validateFilter(filter);

    const docs = await this.find(filter).toArray();

    if (docs.length === 0) {
      return {
        acknowledged: true,
        deletedCount: 0,
      };
    }

    const rows: Array<{ _id: string; _seq: number; _op: 'd'; doc: T }> = [];

    for (const doc of docs) {
      const id = this.extractDocumentId(doc);

      rows.push({
        _id: id,
        _seq: ++this.branchCurrentSeq,
        _op: 'd',
        doc: {} as T,
      });
    }

    await this.writeBranchDelta(rows);

    return {
      acknowledged: true,
      deletedCount: docs.length,
    };
  }

  // --------------------------------------------------------------------------
  // Internal Methods
  // --------------------------------------------------------------------------

  /**
   * Write delta file to branch-specific storage.
   * @internal
   */
  private async writeBranchDelta(
    rows: Array<{ _id: string; _seq: number; _op: 'i' | 'u' | 'd'; doc: T }>
  ): Promise<void> {
    await this.ensureBranchManifest();
    const manifest = this.getBranchManifest();

    const deltaPath = this.getBranchPath(`${this.name}_${Date.now()}_${this.branchCurrentSeq}.parquet`);
    const parquetData = writeParquet(rows);
    await this.storage.put(deltaPath, parquetData);

    // Update branch manifest
    manifest.currentSeq = this.branchCurrentSeq;
    manifest.updatedAt = new Date().toISOString();

    const manifestPath = this.getBranchPath(`${this.name}/_manifest.json`);
    await this.storage.put(manifestPath, new TextEncoder().encode(JSON.stringify(manifest)));
  }

  /**
   * Extract document ID as a string.
   * @internal
   */
  private extractDocumentId(doc: { _id?: unknown }): string {
    if (doc._id === undefined) {
      throw new Error('Document must have _id field');
    }
    return typeof doc._id === 'object' && doc._id !== null
      ? doc._id.toString()
      : String(doc._id);
  }
}
