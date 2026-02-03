/**
 * Conflict Resolver
 *
 * Applies resolved conflicts to merged data by creating new Parquet files
 * with the resolved field values. This is the bridge between conflict detection
 * and the actual merge operation.
 *
 * ## How It Works
 *
 * 1. For each resolved conflict, the resolver loads the source document
 * 2. Applies the resolved field values (source, target, or custom)
 * 3. Writes a new delta file with the resolved document
 *
 * ## Usage
 *
 * ```typescript
 * const resolver = new ConflictResolutionApplier(storage, 'mydb');
 * await resolver.applyResolutions(resolvedConflicts, sourceBranch, targetBranch);
 * ```
 */

import type { StorageBackend } from '../storage/index.js';
import type { Document } from '../types.js';
import { writeParquet, readParquet } from '../parquet/io.js';
import { DEFAULT_BRANCH } from './metadata.js';
import type { ResolvedConflict } from './merge.js';

/**
 * Result of applying conflict resolutions
 */
export interface ApplyResolutionsResult {
  /** Number of conflicts successfully applied */
  appliedCount: number;

  /** Number of conflicts that failed to apply */
  failedCount: number;

  /** Details about each application */
  details: Array<{
    documentId: string;
    collection: string;
    success: boolean;
    error?: string;
  }>;

  /** Path to the resolution file created */
  resolutionFilePath?: string;
}

/**
 * Applies resolved conflicts to merged data.
 *
 * When conflicts are resolved during a merge, this class creates the necessary
 * Parquet files to persist those resolutions. It reads the source documents,
 * applies the resolved values, and writes them to the target branch storage.
 */
export class ConflictResolutionApplier {
  private readonly storage: StorageBackend;
  private readonly database: string;

  constructor(storage: StorageBackend, database: string) {
    this.storage = storage;
    this.database = database;
  }

  /**
   * Apply resolved conflicts by creating new delta files.
   *
   * @param resolvedConflicts - Array of resolved conflicts with resolution values
   * @param sourceBranch - Name of the source branch
   * @param targetBranch - Name of the target branch
   * @returns Result of applying resolutions
   */
  async applyResolutions(
    resolvedConflicts: ResolvedConflict[],
    sourceBranch: string,
    targetBranch: string
  ): Promise<ApplyResolutionsResult> {
    if (resolvedConflicts.length === 0) {
      return {
        appliedCount: 0,
        failedCount: 0,
        details: [],
      };
    }

    const details: ApplyResolutionsResult['details'] = [];

    // Group conflicts by collection
    const conflictsByCollection = this.groupByCollection(resolvedConflicts);

    // Process each collection
    for (const [collection, conflicts] of conflictsByCollection) {
      try {
        await this.applyCollectionResolutions(
          collection,
          conflicts,
          sourceBranch,
          targetBranch
        );

        // Mark all as successful
        for (const conflict of conflicts) {
          details.push({
            documentId: conflict.documentId,
            collection,
            success: true,
          });
        }
      } catch (error) {
        // Mark all as failed for this collection
        for (const conflict of conflicts) {
          details.push({
            documentId: conflict.documentId,
            collection,
            success: false,
            error: error instanceof Error ? error.message : String(error),
          });
        }
      }
    }

    const appliedCount = details.filter((d) => d.success).length;
    const failedCount = details.filter((d) => !d.success).length;

    return {
      appliedCount,
      failedCount,
      details,
    };
  }

  /**
   * Group resolved conflicts by collection name.
   */
  private groupByCollection(
    conflicts: ResolvedConflict[]
  ): Map<string, ResolvedConflict[]> {
    const groups = new Map<string, ResolvedConflict[]>();

    for (const conflict of conflicts) {
      const existing = groups.get(conflict.collection) || [];
      existing.push(conflict);
      groups.set(conflict.collection, existing);
    }

    return groups;
  }

  /**
   * Apply resolutions for a single collection.
   */
  private async applyCollectionResolutions(
    collection: string,
    conflicts: ResolvedConflict[],
    sourceBranch: string,
    targetBranch: string
  ): Promise<void> {
    // Read current documents from both branches to get full document state
    const sourceDocuments = await this.readBranchDocuments(
      collection,
      sourceBranch
    );
    const targetDocuments = await this.readBranchDocuments(
      collection,
      targetBranch === DEFAULT_BRANCH ? null : targetBranch
    );

    // Build resolution rows
    const rows: Array<{ _id: string; _seq: number; _op: 'u'; doc: Document }> = [];

    for (const conflict of conflicts) {
      const resolvedDoc = this.buildResolvedDocument(
        conflict,
        sourceDocuments.get(conflict.documentId),
        targetDocuments.get(conflict.documentId)
      );

      if (resolvedDoc) {
        rows.push({
          _id: conflict.documentId,
          _seq: Date.now(),
          _op: 'u',
          doc: resolvedDoc,
        });
      }
    }

    if (rows.length === 0) {
      return;
    }

    // Write resolution delta file
    const timestamp = Date.now();
    const targetPath =
      targetBranch === DEFAULT_BRANCH
        ? `${this.database}/${collection}_${timestamp}_resolution.parquet`
        : `${this.database}/branches/${targetBranch}/${collection}_${timestamp}_resolution.parquet`;

    const parquetData = writeParquet(rows);
    await this.storage.put(targetPath, parquetData);
  }

  /**
   * Read documents from a branch.
   */
  private async readBranchDocuments(
    collection: string,
    branchName: string | null
  ): Promise<Map<string, Document>> {
    const docsById = new Map<string, Document>();

    let basePath: string;
    let prefix: string;

    if (branchName === null) {
      // Read from main
      basePath = this.database;
      prefix = `${basePath}/${collection}`;
    } else {
      // Read from branch
      basePath = `${this.database}/branches/${branchName}`;
      prefix = `${basePath}/${collection}`;
    }

    const files = await this.storage.list(basePath);
    const parquetFiles = files.filter(
      (f) => f.startsWith(prefix) && f.endsWith('.parquet') && !f.includes('/_')
    );

    for (const file of parquetFiles) {
      const data = await this.storage.get(file);
      if (!data) continue;

      try {
        const rows = await readParquet<Document>(data);
        for (const row of rows) {
          const existing = docsById.get(row._id);
          if (!existing || row._seq > (existing as { _seq?: number })._seq!) {
            docsById.set(row._id, { ...row.doc, _id: row._id });
          }
        }
      } catch {
        // Skip corrupted files
      }
    }

    return docsById;
  }

  /**
   * Build a resolved document by applying the conflict resolution.
   */
  private buildResolvedDocument(
    conflict: ResolvedConflict,
    sourceDoc: Document | undefined,
    targetDoc: Document | undefined
  ): Document | null {
    // Determine the base document based on resolution strategy
    let baseDoc: Document;

    switch (conflict.resolution) {
      case 'source':
        // Use source document as base
        if (!sourceDoc) return null;
        baseDoc = { ...sourceDoc };
        break;

      case 'target':
        // Use target document as base
        if (!targetDoc) return null;
        baseDoc = { ...targetDoc };
        break;

      case 'custom':
        // Start with source and apply custom value to the conflicting field
        if (!sourceDoc && !targetDoc) return null;
        baseDoc = { ...(sourceDoc || targetDoc)! };
        // Apply the custom resolved value to the specific field
        this.setNestedValue(baseDoc, conflict.field, conflict.resolvedValue);
        break;

      default:
        return null;
    }

    return baseDoc;
  }

  /**
   * Set a nested value in a document using dot notation.
   */
  private setNestedValue(doc: Document, path: string, value: unknown): void {
    const parts = path.split('.');
    let current: Document = doc;

    for (let i = 0; i < parts.length - 1; i++) {
      const part = parts[i]!;
      if (current[part] === undefined || typeof current[part] !== 'object') {
        current[part] = {};
      }
      current = current[part] as Document;
    }

    const lastPart = parts[parts.length - 1]!;
    current[lastPart] = value;
  }
}

/**
 * Merge result applier that combines conflict resolution with file merging.
 *
 * This is a higher-level helper that orchestrates the full merge operation
 * including applying conflict resolutions.
 */
export class MergeResultApplier {
  private readonly conflictApplier: ConflictResolutionApplier;
  private readonly storage: StorageBackend;
  private readonly database: string;

  constructor(storage: StorageBackend, database: string) {
    this.storage = storage;
    this.database = database;
    this.conflictApplier = new ConflictResolutionApplier(storage, database);
  }

  /**
   * Apply resolved conflicts from a merge result.
   *
   * @param resolvedConflicts - Array of resolved conflicts
   * @param sourceBranch - Source branch name
   * @param targetBranch - Target branch name
   * @returns Result of applying resolutions
   */
  async applyConflictResolutions(
    resolvedConflicts: ResolvedConflict[],
    sourceBranch: string,
    targetBranch: string
  ): Promise<ApplyResolutionsResult> {
    return this.conflictApplier.applyResolutions(
      resolvedConflicts,
      sourceBranch,
      targetBranch
    );
  }
}
