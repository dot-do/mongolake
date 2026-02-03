/**
 * Query Executor - Query Execution Logic
 *
 * Handles document queries across buffer and R2 storage.
 */

import type { Document, Filter } from '../../types.js';
import type { FindOptions, ReadToken } from './types.js';
import type { BufferManager } from './buffer-manager.js';
import type { IndexManager } from './index-manager.js';
import { matchesFilter } from '../../utils/filter.js';
import { sortDocuments } from '../../utils/sort.js';
import { applyProjection } from '../../utils/projection.js';
import { canFileMatchFilter } from '../../utils/zone-map-filter.js';

/**
 * Query Executor handles document queries.
 *
 * Responsibilities:
 * - Querying documents from buffer
 * - Querying documents from R2
 * - Merging results (buffer takes precedence)
 * - Applying filters, sort, skip, limit, projection
 * - Validating read tokens
 */
export class QueryExecutor {
  constructor(
    private bufferManager: BufferManager,
    private indexManager: IndexManager,
    private getShardId: () => string,
    private getCurrentLSN: () => number
  ) {}

  // ============================================================================
  // Query Operations
  // ============================================================================

  /**
   * Find documents matching a filter.
   *
   * Queries both the in-memory buffer (for recent writes) and R2 storage
   * (for flushed data), merging results with buffer taking precedence.
   */
  async find(
    collection: string,
    filter: Record<string, unknown>,
    options: FindOptions = {}
  ): Promise<Record<string, unknown>[]> {
    // Validate read token if provided
    if (options.afterToken) {
      this.validateReadToken(options.afterToken);
    }

    // Get documents from in-memory buffer
    let results = this.findInBuffer(collection, filter);

    // Get documents from R2 storage
    const r2Docs = await this.findInR2(collection, filter);

    // Merge results: buffer takes precedence over R2
    const bufferIds = new Set(results.map((d) => d._id));
    const deletedIds = this.bufferManager.getDeletedDocs(collection);

    for (const doc of r2Docs) {
      // Skip if document already in buffer or has been deleted
      if (!bufferIds.has(doc._id) && !deletedIds.has(String(doc._id))) {
        results.push(doc);
      }
    }

    // Filter out any deleted documents from final results
    results = results.filter(
      (d) => !deletedIds.has(String(d._id)) && !(d as Record<string, unknown>)._deleted
    );

    // Apply sort if specified
    if (options.sort) {
      results = this.applySort(results, options.sort);
    }

    // Apply skip/pagination
    if (options.skip) {
      results = results.slice(options.skip);
    }

    // Apply limit to result set
    if (options.limit) {
      results = results.slice(0, options.limit);
    }

    // Apply field projection to reduce response size
    if (options.projection) {
      results = results.map((doc) => applyProjection(doc, options.projection!));
    }

    return results;
  }

  /**
   * Find a single document matching a filter.
   */
  async findOne(
    collection: string,
    filter: Record<string, unknown>,
    options: FindOptions = {}
  ): Promise<Record<string, unknown> | null> {
    const results = await this.find(collection, filter, { ...options, limit: 1 });
    return results.length > 0 ? results[0] ?? null : null;
  }

  // ============================================================================
  // Buffer Queries
  // ============================================================================

  /**
   * Query documents from the in-memory buffer.
   */
  private findInBuffer(
    collection: string,
    filter: Record<string, unknown>
  ): Record<string, unknown>[] {
    const allDocs = this.bufferManager.getAllDocsInCollection(collection);
    const results: Record<string, unknown>[] = [];

    for (const doc of allDocs) {
      if (matchesFilter(doc as Document, filter as Filter<Document>)) {
        results.push(doc);
      }
    }

    return results;
  }

  // ============================================================================
  // R2 Queries
  // ============================================================================

  /**
   * Query documents from R2 storage.
   *
   * Uses zone map filtering to skip files that cannot contain matching documents,
   * significantly reducing R2 reads for filtered queries.
   */
  private async findInR2(
    collection: string,
    filter: Record<string, unknown>
  ): Promise<Record<string, unknown>[]> {
    const manifest = this.indexManager.getManifest(collection);
    if (!manifest || manifest.files.length === 0) return [];

    // First pass: collect all documents and tombstones
    const allDocs: Record<string, unknown>[] = [];
    const tombstoneIds = new Set<string>();

    for (const file of manifest.files) {
      // Zone map filtering: skip files that can't contain matching documents
      // Note: We always read files for tombstone detection, but apply filter optimization
      // for non-delete queries. Files with only deletes have _op='d' which won't match
      // typical query filters anyway.
      if (!canFileMatchFilter(file.zoneMap, filter)) {
        // File cannot contain matching documents based on zone map statistics.
        // However, we still need to check for tombstones if the filter matches _id.
        // For simplicity, we skip zone map optimization for _id queries since tombstones
        // are critical for correctness. This is a conservative approach.
        const hasIdFilter = '_id' in filter;
        if (!hasIdFilter) {
          continue;
        }
      }

      const data = await this.indexManager.readFile(file.path);
      if (!data) continue;

      const docs = this.indexManager.parseParquetData(data);

      for (const doc of docs) {
        if (doc._op === 'd' || doc._deleted === true) {
          tombstoneIds.add(String(doc._id));
        } else {
          allDocs.push(doc);
        }
      }
    }

    // Second pass: filter out deleted documents and apply query filter
    const results: Record<string, unknown>[] = [];
    for (const doc of allDocs) {
      if (tombstoneIds.has(String(doc._id))) continue;

      if (matchesFilter(doc as Document, filter as Filter<Document>)) {
        results.push(doc);
      }
    }

    return results;
  }

  /**
   * Find a single document in R2 (used by applyUpdate).
   */
  async findOneInR2(
    collection: string,
    filter: Record<string, unknown>
  ): Promise<Record<string, unknown> | null> {
    const docs = await this.findInR2(collection, filter);
    return docs.length > 0 ? docs[0] ?? null : null;
  }

  // ============================================================================
  // Sort
  // ============================================================================

  /**
   * Apply sort to documents.
   */
  private applySort(
    docs: Record<string, unknown>[],
    sort: Record<string, 1 | -1>
  ): Record<string, unknown>[] {
    return sortDocuments(docs, sort);
  }

  // ============================================================================
  // Read Token Operations
  // ============================================================================

  /**
   * Generate a read token for the given LSN.
   */
  generateReadToken(lsn: number): string {
    return `${this.getShardId()}:${lsn}`;
  }

  /**
   * Get a read token representing the current shard state.
   */
  getCurrentReadToken(): string {
    return this.generateReadToken(this.getCurrentLSN());
  }

  /**
   * Validate a read token for this shard.
   */
  validateReadToken(token: string): void {
    const parsed = QueryExecutor.parseReadToken(token);
    const currentShardId = this.getShardId();

    // Validate shard ID matches this shard
    if (parsed.shardId !== currentShardId) {
      throw new Error(
        `Read token shard ID mismatch: token is for shard "${parsed.shardId}", but this is shard "${currentShardId}"`
      );
    }

    // Ensure read token LSN is not ahead of current state
    if (parsed.lsn > this.getCurrentLSN()) {
      throw new Error(
        `Read token references future LSN (${parsed.lsn}), current LSN is ${this.getCurrentLSN()}`
      );
    }
  }

  /**
   * Parse a read token into its components.
   */
  static parseReadToken(token: string): ReadToken {
    if (!token || token.trim() === '') {
      throw new Error('Read token cannot be empty');
    }

    const parts = token.split(':');
    if (parts.length !== 2) {
      throw new Error(
        `Read token format invalid: expected "shardId:lsn", got "${token}"`
      );
    }

    const lsn = parseInt(parts[1]!, 10);
    if (isNaN(lsn)) {
      throw new Error(
        `Read token LSN is not a valid number: "${parts[1]}"`
      );
    }

    return {
      shardId: parts[0]!,
      lsn,
    };
  }
}
