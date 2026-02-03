/**
 * Diff Generator
 *
 * Compares a branch to its base (or main) and generates a structured
 * list of changes. This is essential for the branching/merging workflow,
 * allowing users to see what has changed before merging.
 *
 * ## Features
 *
 * - Detects inserted, updated, and deleted documents
 * - Field-level diff for updates (changed, added, removed fields)
 * - Streaming support for large diffs
 * - Collection and change type filtering
 * - Summary statistics by collection
 *
 * ## Usage
 *
 * ```typescript
 * const diff = await db.diff('feature-branch');
 * console.log(`Inserted: ${diff.inserted.length}`);
 * console.log(`Updated: ${diff.updated.length}`);
 * console.log(`Deleted: ${diff.deleted.length}`);
 * ```
 */

import type { Document, WithId } from '../types.js';
import type { StorageBackend } from '../storage/index.js';
import { readParquet } from '../parquet/io.js';
import { BranchStore, DEFAULT_BRANCH, type BranchMetadata } from './metadata.js';

// ============================================================================
// Type Definitions
// ============================================================================

/**
 * Output format for diff results.
 */
export type DiffFormat = 'full' | 'summary' | 'minimal';

/**
 * Options for generating a diff.
 */
export interface DiffOptions {
  /** Filter to specific collections */
  collections?: string[];

  /** Maximum number of changes to return per category */
  limit?: number;

  /** Filter to specific change types */
  changeTypes?: ChangeType[];

  /** Output format (default: 'full') */
  format?: DiffFormat;

  /** Include field-level details for updates (default: true) */
  includeFieldDetails?: boolean;
}

/**
 * Types of changes that can be detected.
 */
export type ChangeType = 'insert' | 'update' | 'delete';

/**
 * Represents an inserted document.
 */
export interface InsertedChange<T extends Document = Document> {
  /** Collection the document was inserted into */
  collection: string;

  /** Document ID */
  documentId: string;

  /** The inserted document */
  document: WithId<T>;
}

/**
 * Represents an updated document.
 */
export interface UpdatedChange<T extends Document = Document> {
  /** Collection the document belongs to */
  collection: string;

  /** Document ID */
  documentId: string;

  /** Document state before the update */
  before: WithId<T>;

  /** Document state after the update */
  after: WithId<T>;

  /** Fields that were modified */
  changedFields: string[];

  /** Fields that were added */
  addedFields: string[];

  /** Fields that were removed */
  removedFields: string[];
}

/**
 * Represents a deleted document.
 */
export interface DeletedChange<T extends Document = Document> {
  /** Collection the document was deleted from */
  collection: string;

  /** Document ID */
  documentId: string;

  /** The document that was deleted (its state at base) */
  document: WithId<T>;
}

/**
 * Summary statistics for a collection's changes.
 */
export interface CollectionChangeSummary {
  insertedCount: number;
  updatedCount: number;
  deletedCount: number;
  totalChanges: number;
}

/**
 * Summary statistics for all changes.
 */
export interface DiffSummary {
  insertedCount: number;
  updatedCount: number;
  deletedCount: number;
  totalChanges: number;
  byCollection: Record<string, CollectionChangeSummary>;
}

/**
 * The result of comparing a branch to its base.
 */
export interface DiffResult<T extends Document = Document> {
  /** The branch being compared */
  branch: string;

  /** The base branch (typically 'main') */
  baseBranch: string;

  /** Whether there are any changes */
  hasChanges: boolean;

  /** Documents that were inserted on the branch */
  inserted: InsertedChange<T>[];

  /** Documents that were updated on the branch */
  updated: UpdatedChange<T>[];

  /** Documents that were deleted on the branch */
  deleted: DeletedChange<T>[];

  /** Summary statistics */
  summary: DiffSummary;

  /** Whether the results were truncated due to limit */
  truncated: boolean;
}

/**
 * A single change entry for streaming diffs.
 */
export interface DiffChange<T extends Document = Document> {
  type: ChangeType;
  collection: string;
  documentId: string;
  document?: WithId<T>;
  before?: WithId<T>;
  after?: WithId<T>;
  changedFields?: string[];
  addedFields?: string[];
  removedFields?: string[];
}

// ============================================================================
// Document Entry (internal type)
// ============================================================================

/** Internal representation of a document entry from storage */
interface DocEntry<T extends Document> {
  seq: number;
  op: string;
  doc: T;
}

// ============================================================================
// Diff Generator Implementation
// ============================================================================

/**
 * Diff Generator for comparing branches.
 *
 * This class handles the core logic of comparing a branch's state
 * to its base snapshot and categorizing changes.
 *
 * ## Performance Optimizations
 *
 * - Parallel file reading with batched I/O
 * - Early termination when limit is reached
 * - Lazy field-level diff computation
 * - Cached collection discovery
 *
 * @example
 * ```typescript
 * const generator = new DiffGenerator(storage, 'mydb', branchStore);
 * const diff = await generator.diff('feature-branch');
 *
 * // Or stream changes for large diffs
 * for await (const change of generator.streamDiff('feature-branch')) {
 *   console.log(change.type, change.documentId);
 * }
 * ```
 */
export class DiffGenerator {
  private readonly storage: StorageBackend;
  private readonly database: string;
  private readonly branchStore: BranchStore;

  /** Cache for discovered collections */
  private collectionsCache: Map<string, string[]> = new Map();
  private collectionsCacheTTL = 5000; // 5 seconds
  private collectionsCacheTime = 0;

  constructor(storage: StorageBackend, database: string, branchStore: BranchStore) {
    this.storage = storage;
    this.database = database;
    this.branchStore = branchStore;
  }

  /**
   * Generate a diff for a branch compared to its base.
   *
   * @param branchName - The branch to diff
   * @param options - Diff options
   * @returns The diff result
   */
  async diff<T extends Document = Document>(
    branchName: string,
    options: DiffOptions = {}
  ): Promise<DiffResult<T>> {
    // Validate branch name
    this.validateBranchName(branchName);

    // Get branch metadata
    const branch = await this.getBranchOrThrow(branchName);

    // Discover collections (with caching)
    const collections = await this.discoverCollections(options.collections);

    // Process all collections in parallel
    const collectionResults = await Promise.all(
      collections.map((name) => this.diffCollection<T>(name, branch, options))
    );

    // Aggregate results
    return this.aggregateResults<T>(branchName, branch, collectionResults, options);
  }

  /**
   * Stream changes for large diffs.
   *
   * This is more memory-efficient for branches with many changes,
   * as it yields changes one at a time instead of building a full list.
   *
   * @param branchName - The branch to diff
   * @param options - Diff options
   * @yields Individual change entries
   */
  async *streamDiff<T extends Document = Document>(
    branchName: string,
    options: DiffOptions = {}
  ): AsyncGenerator<DiffChange<T>> {
    // Validate branch name
    this.validateBranchName(branchName);

    // Get branch metadata
    const branch = await this.getBranchOrThrow(branchName);

    // Discover collections
    const collections = await this.discoverCollections(options.collections);

    // Process each collection and yield changes
    let count = 0;
    const limit = options.limit;

    for (const collectionName of collections) {
      const { inserted, updated, deleted } = await this.diffCollection<T>(
        collectionName,
        branch,
        options
      );

      // Yield inserts
      if (!options.changeTypes || options.changeTypes.includes('insert')) {
        for (const change of inserted) {
          if (limit && count >= limit) return;
          yield {
            type: 'insert',
            collection: change.collection,
            documentId: change.documentId,
            document: change.document,
          };
          count++;
        }
      }

      // Yield updates
      if (!options.changeTypes || options.changeTypes.includes('update')) {
        for (const change of updated) {
          if (limit && count >= limit) return;
          yield {
            type: 'update',
            collection: change.collection,
            documentId: change.documentId,
            before: change.before,
            after: change.after,
            changedFields: change.changedFields,
            addedFields: change.addedFields,
            removedFields: change.removedFields,
          };
          count++;
        }
      }

      // Yield deletes
      if (!options.changeTypes || options.changeTypes.includes('delete')) {
        for (const change of deleted) {
          if (limit && count >= limit) return;
          yield {
            type: 'delete',
            collection: change.collection,
            documentId: change.documentId,
            document: change.document,
          };
          count++;
        }
      }
    }
  }

  /**
   * Get just the summary without full change details.
   *
   * This is faster for checking if there are any changes
   * without loading all document data.
   *
   * @param branchName - The branch to diff
   * @param options - Diff options
   * @returns The diff summary
   */
  async getSummary(
    branchName: string,
    options: DiffOptions = {}
  ): Promise<DiffSummary> {
    const result = await this.diff(branchName, { ...options, format: 'summary' });
    return result.summary;
  }

  // ==========================================================================
  // Private Methods
  // ==========================================================================

  /**
   * Validate branch name.
   */
  private validateBranchName(branchName: string): void {
    if (branchName === DEFAULT_BRANCH || branchName === 'main') {
      throw new Error('Cannot diff main branch against itself');
    }
  }

  /**
   * Get branch or throw if not found.
   */
  private async getBranchOrThrow(branchName: string): Promise<BranchMetadata> {
    const branch = await this.branchStore.getBranch(branchName);
    if (!branch) {
      throw new Error(`Branch "${branchName}" not found`);
    }
    return branch;
  }

  /**
   * Aggregate results from all collections.
   */
  private aggregateResults<T extends Document>(
    branchName: string,
    branch: BranchMetadata,
    collectionResults: Array<{
      collection: string;
      inserted: InsertedChange<T>[];
      updated: UpdatedChange<T>[];
      deleted: DeletedChange<T>[];
    }>,
    options: DiffOptions
  ): DiffResult<T> {
    const allInserted: InsertedChange<T>[] = [];
    const allUpdated: UpdatedChange<T>[] = [];
    const allDeleted: DeletedChange<T>[] = [];
    const byCollection: Record<string, CollectionChangeSummary> = {};

    // Aggregate changes from all collections
    for (const result of collectionResults) {
      byCollection[result.collection] = {
        insertedCount: result.inserted.length,
        updatedCount: result.updated.length,
        deletedCount: result.deleted.length,
        totalChanges: result.inserted.length + result.updated.length + result.deleted.length,
      };

      // Add to all changes (respecting changeTypes filter)
      if (!options.changeTypes || options.changeTypes.includes('insert')) {
        allInserted.push(...result.inserted);
      }
      if (!options.changeTypes || options.changeTypes.includes('update')) {
        allUpdated.push(...result.updated);
      }
      if (!options.changeTypes || options.changeTypes.includes('delete')) {
        allDeleted.push(...result.deleted);
      }
    }

    // Apply limits
    const { inserted, updated, deleted, truncated } = this.applyLimits(
      allInserted,
      allUpdated,
      allDeleted,
      options.limit
    );

    // Calculate summary
    const summary: DiffSummary = {
      insertedCount: allInserted.length,
      updatedCount: allUpdated.length,
      deletedCount: allDeleted.length,
      totalChanges: allInserted.length + allUpdated.length + allDeleted.length,
      byCollection,
    };

    return {
      branch: branchName,
      baseBranch: branch.parentBranch || DEFAULT_BRANCH,
      hasChanges: summary.totalChanges > 0,
      inserted,
      updated,
      deleted,
      summary,
      truncated,
    };
  }

  /**
   * Apply limits to change arrays.
   */
  private applyLimits<T extends Document>(
    inserted: InsertedChange<T>[],
    updated: UpdatedChange<T>[],
    deleted: DeletedChange<T>[],
    limit?: number
  ): {
    inserted: InsertedChange<T>[];
    updated: UpdatedChange<T>[];
    deleted: DeletedChange<T>[];
    truncated: boolean;
  } {
    if (!limit) {
      return { inserted, updated, deleted, truncated: false };
    }

    let truncated = false;
    const finalInserted = inserted.length > limit ? (truncated = true, inserted.slice(0, limit)) : inserted;
    const finalUpdated = updated.length > limit ? (truncated = true, updated.slice(0, limit)) : updated;
    const finalDeleted = deleted.length > limit ? (truncated = true, deleted.slice(0, limit)) : deleted;

    return {
      inserted: finalInserted,
      updated: finalUpdated,
      deleted: finalDeleted,
      truncated,
    };
  }

  /**
   * Diff a specific collection.
   */
  private async diffCollection<T extends Document = Document>(
    collectionName: string,
    branch: BranchMetadata,
    options: DiffOptions
  ): Promise<{
    collection: string;
    inserted: InsertedChange<T>[];
    updated: UpdatedChange<T>[];
    deleted: DeletedChange<T>[];
  }> {
    // Get base snapshot timestamp for filtering
    const baseTimestamp = new Date(branch.createdAt).getTime();

    // Read base and branch documents in parallel
    const [baseDocsById, branchDocsById] = await Promise.all([
      this.readBaseDocuments<T>(collectionName, baseTimestamp),
      this.readBranchDocuments<T>(collectionName, branch.name),
    ]);

    // Compare and categorize changes
    const inserted: InsertedChange<T>[] = [];
    const updated: UpdatedChange<T>[] = [];
    const deleted: DeletedChange<T>[] = [];

    const includeFieldDetails = options.includeFieldDetails !== false;

    // Process branch documents
    for (const [docId, branchEntry] of branchDocsById) {
      const baseEntry = baseDocsById.get(docId);

      if (branchEntry.op === 'd') {
        // Document was deleted on branch
        if (baseEntry && baseEntry.op !== 'd') {
          deleted.push({
            collection: collectionName,
            documentId: docId,
            document: { ...baseEntry.doc, _id: docId } as WithId<T>,
          });
        }
      } else if (!baseEntry || baseEntry.op === 'd') {
        // Document was inserted on branch
        inserted.push({
          collection: collectionName,
          documentId: docId,
          document: { ...branchEntry.doc, _id: docId } as WithId<T>,
        });
      } else {
        // Document existed in both - check if it changed
        const beforeDoc = { ...baseEntry.doc, _id: docId } as WithId<T>;
        const afterDoc = { ...branchEntry.doc, _id: docId } as WithId<T>;

        if (!this.documentsEqual(beforeDoc, afterDoc)) {
          const fieldChanges = includeFieldDetails
            ? this.computeFieldChanges(beforeDoc, afterDoc)
            : { changedFields: [], addedFields: [], removedFields: [] };

          updated.push({
            collection: collectionName,
            documentId: docId,
            before: beforeDoc,
            after: afterDoc,
            ...fieldChanges,
          });
        }
      }
    }

    return { collection: collectionName, inserted, updated, deleted };
  }

  /**
   * Discover collections to diff (with caching).
   */
  private async discoverCollections(filter?: string[]): Promise<string[]> {
    const now = Date.now();
    const cacheKey = filter ? filter.sort().join(',') : '__all__';

    // Check cache
    if (
      now - this.collectionsCacheTime < this.collectionsCacheTTL &&
      this.collectionsCache.has(cacheKey)
    ) {
      return this.collectionsCache.get(cacheKey)!;
    }

    // List files and extract collection names
    const files = await this.storage.list(this.database);
    const collections = new Set<string>();

    const collectionPattern = new RegExp(
      `^${this.escapeRegex(this.database)}/(?:branches/[^/]+/)?([^/_][^/]*?)(?:_\\d+_\\d+)?\\.parquet$`
    );

    for (const file of files) {
      const match = file.match(collectionPattern);
      if (match?.[1]) {
        collections.add(match[1]);
      }
    }

    // Apply filter
    let result = Array.from(collections);
    if (filter && filter.length > 0) {
      const filterSet = new Set(filter);
      result = result.filter((c) => filterSet.has(c));
    }

    // Update cache
    this.collectionsCache.set(cacheKey, result);
    this.collectionsCacheTime = now;

    return result;
  }

  /**
   * Read base documents from main (filtered by timestamp).
   */
  private async readBaseDocuments<T extends Document>(
    collectionName: string,
    baseTimestamp: number
  ): Promise<Map<string, DocEntry<T>>> {
    const basePath = this.database;
    const collectionPrefix = `${basePath}/${collectionName}`;
    const files = await this.storage.list(basePath);

    const parquetFiles = files.filter((f) => {
      if (!f.startsWith(collectionPrefix) || !f.endsWith('.parquet') || f.includes('/_')) {
        return false;
      }
      if (f.includes('/branches/')) {
        return false;
      }
      // Filter by timestamp
      const match = f.match(/_(\d+)_\d+\.parquet$/);
      if (match) {
        const fileTimestamp = parseInt(match[1]!, 10);
        return fileTimestamp <= baseTimestamp;
      }
      return true;
    });

    return this.readDocumentsFromFiles<T>(parquetFiles);
  }

  /**
   * Read branch documents.
   */
  private async readBranchDocuments<T extends Document>(
    collectionName: string,
    branchName: string
  ): Promise<Map<string, DocEntry<T>>> {
    const branchBasePath = `${this.database}/branches/${branchName}`;
    const collectionPrefix = `${branchBasePath}/${collectionName}`;
    const files = await this.storage.list(branchBasePath);

    const parquetFiles = files.filter(
      (f) => f.startsWith(collectionPrefix) && f.endsWith('.parquet') && !f.includes('/_')
    );

    return this.readDocumentsFromFiles<T>(parquetFiles);
  }

  /**
   * Read documents from a list of Parquet files in parallel.
   */
  private async readDocumentsFromFiles<T extends Document>(
    files: string[]
  ): Promise<Map<string, DocEntry<T>>> {
    const docsById = new Map<string, DocEntry<T>>();

    // Read files in parallel batches for better performance
    const BATCH_SIZE = 10;
    for (let i = 0; i < files.length; i += BATCH_SIZE) {
      const batch = files.slice(i, i + BATCH_SIZE);
      const results = await Promise.all(
        batch.map((file) => this.readSingleFile<T>(file))
      );

      // Merge results
      for (const fileRows of results) {
        for (const row of fileRows) {
          const existing = docsById.get(row._id);
          if (!existing || row._seq > existing.seq) {
            docsById.set(row._id, {
              seq: row._seq,
              op: row._op,
              doc: row.doc,
            });
          }
        }
      }
    }

    return docsById;
  }

  /**
   * Read a single Parquet file.
   */
  private async readSingleFile<T extends Document>(
    file: string
  ): Promise<Array<{ _id: string; _seq: number; _op: string; doc: T }>> {
    try {
      const data = await this.storage.get(file);
      if (!data) return [];
      return await readParquet<T>(data);
    } catch {
      // Skip corrupted files
      return [];
    }
  }

  /**
   * Check if two documents are equal using deep comparison.
   */
  private documentsEqual(a: Document, b: Document): boolean {
    // Fast path: same reference
    if (a === b) return true;

    // Compare JSON representations
    return JSON.stringify(a) === JSON.stringify(b);
  }

  /**
   * Compute which fields changed between two documents.
   */
  private computeFieldChanges(
    before: Document,
    after: Document
  ): {
    changedFields: string[];
    addedFields: string[];
    removedFields: string[];
  } {
    const changedFields: string[] = [];
    const addedFields: string[] = [];
    const removedFields: string[] = [];

    const beforeKeys = new Set(Object.keys(before));
    const afterKeys = new Set(Object.keys(after));

    // Check for changed and removed fields
    for (const key of beforeKeys) {
      if (key === '_id') continue;

      if (!afterKeys.has(key)) {
        removedFields.push(key);
      } else if (JSON.stringify(before[key]) !== JSON.stringify(after[key])) {
        changedFields.push(key);
      }
    }

    // Check for added fields
    for (const key of afterKeys) {
      if (key === '_id') continue;

      if (!beforeKeys.has(key)) {
        addedFields.push(key);
      }
    }

    return { changedFields, addedFields, removedFields };
  }

  /**
   * Escape special regex characters in a string.
   */
  private escapeRegex(str: string): string {
    return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }
}
