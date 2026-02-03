/**
 * Buffer Manager - In-Memory Buffer Management
 *
 * Handles document buffering before flush to R2.
 * Implements back-pressure to prevent OOM under high write volume.
 */

import type { BufferedDoc, ShardConfig } from './types.js';
import {
  DEFAULT_FLUSH_THRESHOLD_BYTES,
  DEFAULT_FLUSH_THRESHOLD_DOCS,
  DEFAULT_BUFFER_MAX_BYTES,
} from '../../constants.js';

/**
 * Buffer Manager handles in-memory document buffering.
 *
 * Responsibilities:
 * - Adding/updating documents in buffer
 * - Tracking buffer size and document count
 * - Tracking deleted document IDs
 * - Determining when to flush based on thresholds
 */
export class BufferManager {
  /**
   * In-memory buffer for uncommitted writes.
   * Structure: collection name -> document _id -> BufferedDoc
   */
  private buffer: Map<string, Map<string, BufferedDoc>> = new Map();

  /** Current buffer size in bytes (estimated via JSON serialization) */
  private bufferSize: number = 0;

  /** Current number of documents in the buffer */
  private bufferDocCount: number = 0;

  /**
   * Tracks deleted document IDs not yet flushed to R2.
   * Map: collection name -> Set of deleted _ids
   */
  private deletedDocs: Map<string, Set<string>> = new Map();

  /** Configuration for flush thresholds */
  private config: ShardConfig = {
    flushThresholdBytes: DEFAULT_FLUSH_THRESHOLD_BYTES,
    flushThresholdDocs: DEFAULT_FLUSH_THRESHOLD_DOCS,
    maxBytes: DEFAULT_BUFFER_MAX_BYTES,
    compactionMinAge: 0,
    compactionBatchSize: 10,
  };

  // ============================================================================
  // Configuration
  // ============================================================================

  /**
   * Update configuration options.
   */
  configure(config: ShardConfig): void {
    this.config = { ...this.config, ...config };
  }

  /**
   * Get the current configuration.
   */
  getConfig(): ShardConfig {
    return { ...this.config };
  }

  // ============================================================================
  // Buffer Operations
  // ============================================================================

  /**
   * Add or update a document in the in-memory buffer.
   *
   * Handles three cases:
   * 1. New document: adds to collection buffer, updates size counters
   * 2. Updated document: removes old entry, adds new, adjusts size
   * 3. Deleted document: removes from buffer, adds to deletedDocs set
   *
   * @returns true if back-pressure threshold is exceeded and auto-flush should be triggered
   */
  addToBuffer(doc: BufferedDoc): boolean {
    // Get or create collection buffer
    if (!this.buffer.has(doc.collection)) {
      this.buffer.set(doc.collection, new Map());
    }

    const collectionBuffer = this.buffer.get(doc.collection);
    if (!collectionBuffer) {
      // This should never happen since we just set it above, but handle gracefully
      return false;
    }
    const existingDoc = collectionBuffer.get(doc._id);

    // Remove old doc size if exists
    if (existingDoc) {
      this.bufferSize -= this.estimateDocSize(existingDoc);
      this.bufferDocCount--;
    }

    // Handle deletion
    if (doc._op === 'd') {
      collectionBuffer.delete(doc._id);
      if (!this.deletedDocs.has(doc.collection)) {
        this.deletedDocs.set(doc.collection, new Set());
      }
      const deletedSet = this.deletedDocs.get(doc.collection);
      if (deletedSet) {
        deletedSet.add(doc._id);
      }
    } else {
      // Add new doc
      collectionBuffer.set(doc._id, doc);
      this.bufferSize += this.estimateDocSize(doc);
      this.bufferDocCount++;
    }

    // Return true if back-pressure threshold is exceeded
    return this.exceedsMaxBytes();
  }

  /**
   * Check if the buffer exceeds the maximum bytes threshold (back-pressure).
   * When this returns true, an auto-flush should be triggered to prevent OOM.
   */
  exceedsMaxBytes(): boolean {
    return this.bufferSize >= (this.config.maxBytes ?? DEFAULT_BUFFER_MAX_BYTES);
  }

  /**
   * Get the configured maximum buffer size in bytes.
   */
  getMaxBytes(): number {
    return this.config.maxBytes ?? DEFAULT_BUFFER_MAX_BYTES;
  }

  /**
   * Get a document from the buffer synchronously.
   */
  getFromBuffer(collection: string, docId: string): Record<string, unknown> | null {
    const collectionBuffer = this.buffer.get(collection);
    if (!collectionBuffer) return null;

    const doc = collectionBuffer.get(docId);
    if (!doc || doc._op === 'd') return null;

    return { ...doc.document };
  }

  /**
   * Check if a document has been deleted (but not yet flushed).
   */
  isDeleted(collection: string, docId: string): boolean {
    const deletedSet = this.deletedDocs.get(collection);
    return deletedSet?.has(docId) ?? false;
  }

  /**
   * Get the set of deleted document IDs for a collection.
   */
  getDeletedDocs(collection: string): Set<string> {
    return this.deletedDocs.get(collection) || new Set();
  }

  // ============================================================================
  // Buffer State
  // ============================================================================

  /**
   * Get current buffer size in bytes.
   */
  getBufferSize(): number {
    return this.bufferSize;
  }

  /**
   * Get current buffer document count.
   */
  getBufferDocCount(): number {
    return this.bufferDocCount;
  }

  /**
   * Check if buffer should be flushed based on thresholds.
   */
  shouldFlush(): boolean {
    return (
      this.bufferSize >= (this.config.flushThresholdBytes ?? DEFAULT_FLUSH_THRESHOLD_BYTES) ||
      this.bufferDocCount >= (this.config.flushThresholdDocs ?? DEFAULT_FLUSH_THRESHOLD_DOCS)
    );
  }

  /**
   * Check if there is any data to flush.
   */
  hasDataToFlush(): boolean {
    const hasBufferedDocs = this.bufferDocCount > 0;
    const hasPendingDeletions = Array.from(this.deletedDocs.values()).some((s) => s.size > 0);
    return hasBufferedDocs || hasPendingDeletions;
  }

  /**
   * Get all collections that have data to flush.
   */
  getCollectionsToFlush(): Set<string> {
    return new Set([
      ...this.buffer.keys(),
      ...this.deletedDocs.keys(),
    ]);
  }

  /**
   * Get the collection buffer for a specific collection.
   */
  getCollectionBuffer(collection: string): Map<string, BufferedDoc> | undefined {
    return this.buffer.get(collection);
  }

  /**
   * Get all buffered documents for a collection.
   */
  getBufferedDocsForCollection(collection: string): BufferedDoc[] {
    const collectionBuffer = this.buffer.get(collection);
    if (!collectionBuffer) return [];
    return Array.from(collectionBuffer.values());
  }

  // ============================================================================
  // Clear Operations
  // ============================================================================

  /**
   * Clear all buffers after successful flush.
   */
  clear(): void {
    this.buffer.clear();
    this.bufferSize = 0;
    this.bufferDocCount = 0;
    this.deletedDocs.clear();
  }

  // ============================================================================
  // Query Support
  // ============================================================================

  /**
   * Get all documents from buffer matching a filter (for a collection).
   * Note: Actual filtering is done by QueryExecutor; this returns all docs.
   */
  getAllDocsInCollection(collection: string): Array<Record<string, unknown>> {
    const collectionBuffer = this.buffer.get(collection);
    if (!collectionBuffer) return [];

    const results: Record<string, unknown>[] = [];
    for (const doc of collectionBuffer.values()) {
      if (doc._op !== 'd') {
        results.push({ ...doc.document });
      }
    }
    return results;
  }

  // ============================================================================
  // Helpers
  // ============================================================================

  /**
   * Estimate document size using JSON serialization.
   */
  private estimateDocSize(doc: BufferedDoc): number {
    return JSON.stringify(doc).length;
  }
}
