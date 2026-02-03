/**
 * Compaction Service - Compaction Logic
 *
 * Handles merging small Parquet files into larger ones.
 */

import type { ShardConfig, CollectionManifest, FileMetadata } from './types.js';
import type { IndexManager } from './index-manager.js';
import { COMPACTION_RESCHEDULE_DELAY_MS } from '../../constants.js';
import { MetricsCollector, METRICS } from '../../metrics/index.js';
import { generateZoneMapEntries } from '../../utils/zone-map-filter.js';

/**
 * Minimal bucket interface for compaction operations.
 * Compatible with R2Bucket but only requires put/delete operations.
 */
export interface CompactionBucket {
  put: (key: string, value: ArrayBuffer | Uint8Array | string | null) => Promise<unknown>;
  delete: (key: string) => Promise<void>;
}

/**
 * Compaction Service handles background compaction.
 *
 * Responsibilities:
 * - Identifying files eligible for compaction
 * - Merging small files into larger ones
 * - Deduplicating documents (latest version wins)
 * - Cleaning up old files after successful compaction
 */
export class CompactionService {
  private metrics: MetricsCollector;

  /**
   * Create a CompactionService.
   *
   * @param indexManager - Index manager for manifest operations
   * @param setAlarm - Function to schedule the next compaction alarm
   * @param metrics - MetricsCollector instance for recording metrics (required for explicit dependency injection)
   */
  constructor(
    private indexManager: IndexManager,
    private setAlarm: (timestamp: number) => Promise<void>,
    metrics: MetricsCollector
  ) {
    this.metrics = metrics;
  }

  // ============================================================================
  // Compaction Operations
  // ============================================================================

  /**
   * Execute compaction for all collections.
   *
   * Compaction merges multiple small Parquet files into larger ones:
   * 1. Identifies files older than compactionMinAge
   * 2. Takes a batch of up to compactionBatchSize files
   * 3. Reads and deduplicates documents (latest version wins)
   * 4. Writes merged file to R2 with two-phase commit
   * 5. Deletes old files after successful manifest update
   */
  async runCompaction(config: ShardConfig): Promise<void> {
    let hasMoreWork = false;
    const manifests = this.indexManager.getAllManifests();

    for (const [collection, manifest] of manifests) {
      const result = await this.compactCollection(collection, manifest, config);
      if (result.hasMoreWork) {
        hasMoreWork = true;
      }
    }

    // Reschedule compaction alarm if more work remains
    if (hasMoreWork) {
      await this.setAlarm(Date.now() + COMPACTION_RESCHEDULE_DELAY_MS);
    }
  }

  /**
   * Compact a single collection.
   */
  private async compactCollection(
    collection: string,
    manifest: CollectionManifest,
    config: ShardConfig
  ): Promise<{ hasMoreWork: boolean }> {
    const timer = this.metrics.startTimer(METRICS.COMPACTION_DURATION.name, { collection });

    try {
      // Find files old enough to be compacted
      const now = Date.now();
      const eligibleFiles = manifest.files.filter(
        (f) => now - f.createdAt >= (config.compactionMinAge ?? 0)
      );

      // Skip if less than 2 files (compaction needs at least 2 files)
      if (eligibleFiles.length < 2) {
        return { hasMoreWork: false };
      }

      // Take batch of eligible files
      const batchSize = config.compactionBatchSize ?? 10;
      const batch = eligibleFiles.slice(0, batchSize);

      // Skip if batch too small to compact
      if (batch.length < 2) {
        return { hasMoreWork: false };
      }

      // Calculate bytes before compaction
      const bytesBefore = batch.reduce((sum, f) => sum + f.size, 0);

      // Read all documents from batch files
      const allDocs: Record<string, unknown>[] = [];
      for (const file of batch) {
        const data = await this.indexManager.readFile(file.path);
        if (!data) continue;

        const docs = this.indexManager.parseParquetData(data);
        allDocs.push(...docs);
      }

      // Skip if no documents found
      if (allDocs.length === 0) {
        this.metrics.inc(METRICS.COMPACTION_CYCLES.name, { collection, status: 'empty' });
        return { hasMoreWork: eligibleFiles.length > batchSize };
      }

    // Deduplicate documents by _id, keeping the latest version (highest _seq)
    const docMap = new Map<string, Record<string, unknown>>();
    for (const doc of allDocs) {
      const id = String(doc._id);
      const existing = docMap.get(id);
      // Replace with newer version if this document has higher sequence number
      if (!existing || (doc._seq as number) > (existing._seq as number)) {
        docMap.set(id, doc);
      }
    }

    // Filter out deleted documents
    const mergedDocs = Array.from(docMap.values()).filter(
      (d) => d._op !== 'd'
    );

      if (mergedDocs.length === 0) {
        // All documents were deletions, remove batch from manifest and R2
        for (const file of batch) {
          await this.indexManager.deleteFile(file.path);
        }
        this.indexManager.removeFilesFromManifest(collection, batch);

        // Record compaction metrics (all deletions)
        timer.end();
        this.metrics.recordCompaction(collection, 0, batch.length, bytesBefore, 0, true);

        return { hasMoreWork: eligibleFiles.length > batchSize };
      }

      // Prepare documents for serialization
      const docsToSerialize = mergedDocs as Array<{
        _id: string;
        _seq: number;
        _op: 'i' | 'u' | 'd';
        [key: string]: unknown;
      }>;

      // Write compacted file with two-phase commit
      const { combined } = this.indexManager.serializeToParquet(docsToSerialize);

      // Calculate bytes after compaction
      const bytesAfter = combined.length;

      // Generate compacted file path
      const fileId = this.indexManager.generateFileId(true);
      const filePath = this.indexManager.generateFilePath(collection, fileId);

      // Calculate LSN range from original batch files
      const minLSN = Math.min(...batch.map((f) => f.minLSN));
      const maxLSN = Math.max(...batch.map((f) => f.maxLSN));

      // Generate zone map for predicate pushdown filtering
      const zoneMap = generateZoneMapEntries(mergedDocs);

      // Create metadata for new compacted file
      const newFile: FileMetadata = {
        path: filePath,
        size: combined.length,
        rowCount: mergedDocs.length,
        minLSN,
        maxLSN,
        createdAt: Date.now(),
        zoneMap,
      };

      // Write pending flush marker (Phase 1)
      const flushId = this.indexManager.generateFlushId();
      const pendingPath = `_pending/${flushId}.json`;
      const pendingFlush = {
        flushId,
        collection,
        file: newFile,
        timestamp: Date.now(),
      };

      // Access bucket through indexManager for R2 operations
      await this.writePendingFlush(pendingPath, pendingFlush);

      // Write compacted file to R2
      await this.writeCompactedFile(filePath, combined);

      // Update manifest (Phase 2)
      this.indexManager.updateManifestFiles(collection, batch, newFile);

      // Cleanup: Delete pending flush marker
      await this.deletePendingFlush(pendingPath);

      // Delete old batch files from R2
      for (const file of batch) {
        await this.indexManager.deleteFile(file.path);
      }

      // Record compaction success metrics
      const durationMs = timer.end() * 1000;
      this.metrics.recordCompaction(collection, durationMs, batch.length, bytesBefore, bytesAfter, true);

      return { hasMoreWork: eligibleFiles.length > batchSize };
    } catch (error) {
      // Record compaction failure metrics
      timer.end();
      this.metrics.inc(METRICS.COMPACTION_CYCLES.name, { collection, status: 'error' });
      throw error;
    }
  }

  // ============================================================================
  // R2 Operations (delegated to IndexManager)
  // ============================================================================

  /**
   * Write pending flush marker to R2.
   * Uses IndexManager's bucket access.
   */
  private async writePendingFlush(_path: string, _data: Record<string, unknown>): Promise<void> {
    // This is a placeholder - the actual CompactionServiceWithBucket class
    // has direct bucket access and doesn't use this method.
    // This class is kept for backwards compatibility but the WithBucket variant
    // should be used in production.
  }

  /**
   * Internal storage for compacted file data.
   * Used by this backwards-compatibility class; actual production code uses CompactionServiceWithBucket.
   * @internal
   */
  private compactedFileData?: { path: string; data: Uint8Array };

  /**
   * Get the last compacted file data (for testing).
   * @internal
   */
  getCompactedFile(): { path: string; data: Uint8Array } | undefined {
    return this.compactedFileData;
  }

  /**
   * Write compacted file to R2.
   * Note: This class is kept for backwards compatibility but the WithBucket variant should be used.
   */
  private async writeCompactedFile(path: string, data: Uint8Array): Promise<void> {
    // Store the compacted file data for potential retrieval
    // This is a placeholder - the actual CompactionServiceWithBucket class has direct bucket access
    this.compactedFileData = { path, data };
  }

  /**
   * Delete pending flush marker from R2.
   */
  private async deletePendingFlush(path: string): Promise<void> {
    await this.indexManager.deleteFile(path);
  }
}

/**
 * Alternative CompactionService that receives a bucket reference directly.
 * This is the proper implementation with full R2 access.
 */
export class CompactionServiceWithBucket {
  private metrics: MetricsCollector;

  /**
   * Create a CompactionServiceWithBucket.
   *
   * @param indexManager - Index manager for manifest operations
   * @param bucket - R2 bucket for file operations
   * @param setAlarm - Function to schedule the next compaction alarm
   * @param metrics - MetricsCollector instance for recording metrics. If not provided, a no-op collector is used.
   *                  For production use, pass an explicit MetricsCollector instance.
   */
  constructor(
    private indexManager: IndexManager,
    private bucket: CompactionBucket,
    private setAlarm: (timestamp: number) => Promise<void>,
    metrics?: MetricsCollector
  ) {
    // Default to a no-op metrics collector if not provided
    // This allows backward compatibility while encouraging explicit DI
    this.metrics = metrics ?? new MetricsCollector();
  }

  /**
   * Execute compaction for all collections.
   */
  async runCompaction(config: ShardConfig): Promise<void> {
    let hasMoreWork = false;
    const manifests = this.indexManager.getAllManifests();

    for (const [collection, manifest] of manifests) {
      const result = await this.compactCollection(collection, manifest, config);
      if (result.hasMoreWork) {
        hasMoreWork = true;
      }
    }

    // Reschedule compaction alarm if more work remains
    if (hasMoreWork) {
      await this.setAlarm(Date.now() + COMPACTION_RESCHEDULE_DELAY_MS);
    }
  }

  /**
   * Compact a single collection.
   */
  private async compactCollection(
    collection: string,
    manifest: CollectionManifest,
    config: ShardConfig
  ): Promise<{ hasMoreWork: boolean }> {
    const timer = this.metrics.startTimer(METRICS.COMPACTION_DURATION.name, { collection });

    try {
      // Find files old enough to be compacted
      const now = Date.now();
      const eligibleFiles = manifest.files.filter(
        (f) => now - f.createdAt >= (config.compactionMinAge ?? 0)
      );

      // Skip if less than 2 files
      if (eligibleFiles.length < 2) {
        return { hasMoreWork: false };
      }

      // Take batch of eligible files
      const batchSize = config.compactionBatchSize ?? 10;
      const batch = eligibleFiles.slice(0, batchSize);

      // Skip if batch too small
      if (batch.length < 2) {
        return { hasMoreWork: false };
      }

      // Calculate bytes before compaction
      const bytesBefore = batch.reduce((sum, f) => sum + f.size, 0);

      // Read all documents from batch files
      const allDocs: Record<string, unknown>[] = [];
      for (const file of batch) {
        const data = await this.indexManager.readFile(file.path);
        if (!data) continue;

        const docs = this.indexManager.parseParquetData(data);
        allDocs.push(...docs);
      }

      // Skip if no documents found
      if (allDocs.length === 0) {
        this.metrics.inc(METRICS.COMPACTION_CYCLES.name, { collection, status: 'empty' });
        return { hasMoreWork: eligibleFiles.length > batchSize };
      }

      // Deduplicate documents by _id
      const docMap = new Map<string, Record<string, unknown>>();
      for (const doc of allDocs) {
        const id = String(doc._id);
        const existing = docMap.get(id);
        if (!existing || (doc._seq as number) > (existing._seq as number)) {
          docMap.set(id, doc);
        }
      }

      // Filter out deleted documents
      const mergedDocs = Array.from(docMap.values()).filter(
        (d) => d._op !== 'd'
      );

      if (mergedDocs.length === 0) {
        // All documents were deletions
        for (const file of batch) {
          await this.indexManager.deleteFile(file.path);
        }
        this.indexManager.removeFilesFromManifest(collection, batch);

        // Record compaction metrics (all deletions)
        timer.end();
        this.metrics.recordCompaction(collection, 0, batch.length, bytesBefore, 0, true);

        return { hasMoreWork: eligibleFiles.length > batchSize };
      }

      // Serialize to Parquet
      const docsToSerialize = mergedDocs as Array<{
        _id: string;
        _seq: number;
        _op: 'i' | 'u' | 'd';
        [key: string]: unknown;
      }>;
      const { combined } = this.indexManager.serializeToParquet(docsToSerialize);

      // Calculate bytes after compaction
      const bytesAfter = combined.length;

      // Generate compacted file path
      const fileId = this.indexManager.generateFileId(true);
      const filePath = this.indexManager.generateFilePath(collection, fileId);

      // Calculate LSN range
      const minLSN = Math.min(...batch.map((f) => f.minLSN));
      const maxLSN = Math.max(...batch.map((f) => f.maxLSN));

      // Generate zone map for predicate pushdown filtering
      const zoneMap = generateZoneMapEntries(mergedDocs);

      // Create file metadata
      const newFile: FileMetadata = {
        path: filePath,
        size: combined.length,
        rowCount: mergedDocs.length,
        minLSN,
        maxLSN,
        createdAt: Date.now(),
        zoneMap,
      };

      // Phase 1: Write pending flush marker
      const flushId = this.indexManager.generateFlushId();
      const pendingPath = `_pending/${flushId}.json`;
      await this.bucket.put(pendingPath, JSON.stringify({
        flushId,
        collection,
        file: newFile,
        timestamp: Date.now(),
      }));

      // Write compacted file
      await this.bucket.put(filePath, combined);

      // Phase 2: Update manifest
      this.indexManager.updateManifestFiles(collection, batch, newFile);

      // Cleanup: Delete pending marker
      await this.bucket.delete(pendingPath);

      // Delete old files
      for (const file of batch) {
        await this.indexManager.deleteFile(file.path);
      }

      // Record compaction success metrics
      const durationMs = timer.end() * 1000;
      this.metrics.recordCompaction(collection, durationMs, batch.length, bytesBefore, bytesAfter, true);

      return { hasMoreWork: eligibleFiles.length > batchSize };
    } catch (error) {
      // Record compaction failure metrics
      timer.end();
      this.metrics.inc(METRICS.COMPACTION_CYCLES.name, { collection, status: 'error' });
      throw error;
    }
  }
}
