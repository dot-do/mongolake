/**
 * Index Manager - Manifest and File Operations
 *
 * Handles collection manifests and file metadata management.
 *
 * ## Portability
 *
 * This module accepts an ObjectStorage interface that abstracts the underlying
 * storage backend. While it currently uses the R2Bucket type for compatibility,
 * any implementation of ObjectStorage will work:
 *
 * - Cloudflare R2
 * - AWS S3
 * - Google Cloud Storage
 * - Local filesystem
 * - In-memory (for testing)
 *
 * To use a portable storage backend:
 *
 * ```typescript
 * import { CloudflareR2Storage } from '@mongolake/abstractions/cloudflare';
 *
 * const objectStorage = new CloudflareR2Storage(env.DATA_BUCKET);
 * const indexManager = new IndexManager(storageBackend, objectStorage);
 * ```
 */

import type { StorageBackend, CollectionManifest, FileMetadata, PendingFlush, ObjectStorage } from './types.js';
import type { R2Bucket } from '@cloudflare/workers-types';
import { RowGroupSerializer } from '../../parquet/row-group.js';
import { PARQUET_MAGIC_BYTES, COMPACTION_ALARM_DELAY_MS } from '../../constants.js';
import { generateZoneMapEntries } from '../../utils/zone-map-filter.js';
import { logger } from '../../utils/logger.js';

/**
 * Object storage bucket interface.
 *
 * This union type allows IndexManager to accept either:
 * - R2Bucket (Cloudflare-specific, for backward compatibility)
 * - ObjectStorage (portable interface)
 *
 * The methods used are compatible with both.
 */
export type IndexManagerBucket = R2Bucket | ObjectStorage;

/**
 * Index Manager handles manifest and file operations.
 *
 * Responsibilities:
 * - Managing collection manifests
 * - Writing Parquet files to object storage
 * - Two-phase commit for atomicity
 * - File path generation
 *
 * ## Architecture
 *
 * IndexManager uses two storage backends:
 *
 * 1. **StorageBackend** (SQLite) - For WAL and manifest persistence
 *    This provides durability guarantees for writes.
 *
 * 2. **ObjectStorage** (R2/S3) - For Parquet file storage
 *    This provides scalable blob storage for the actual data.
 *
 * The two-phase commit protocol ensures atomicity:
 * 1. Write pending marker to object storage
 * 2. Write data file to object storage
 * 3. Update manifest in SQLite
 * 4. Delete pending marker
 *
 * If a crash occurs, recovery checks for pending markers and
 * either completes or rolls back the operation.
 */
export class IndexManager {
  /** Collection manifests tracking Parquet files in object storage */
  private manifests: Map<string, CollectionManifest> = new Map();

  /**
   * Create an IndexManager.
   *
   * @param storage - StorageBackend for SQLite/WAL operations
   * @param bucket - Object storage for Parquet files (R2Bucket or ObjectStorage)
   */
  constructor(
    private storage: StorageBackend,
    private bucket: IndexManagerBucket
  ) {}

  // ============================================================================
  // Initialization
  // ============================================================================

  /**
   * Set manifests from recovery.
   */
  setManifests(manifests: Map<string, CollectionManifest>): void {
    this.manifests = manifests;
  }

  // ============================================================================
  // Manifest Operations
  // ============================================================================

  /**
   * Get manifest for a collection.
   */
  getManifest(collection: string): CollectionManifest {
    return (
      this.manifests.get(collection) || {
        collection,
        files: [],
        updatedAt: 0,
      }
    );
  }

  /**
   * Get all manifests.
   */
  getAllManifests(): Map<string, CollectionManifest> {
    return this.manifests;
  }

  /**
   * Ensure a collection manifest exists.
   */
  ensureManifest(collection: string): CollectionManifest {
    if (!this.manifests.has(collection)) {
      this.manifests.set(collection, {
        collection,
        files: [],
        updatedAt: Date.now(),
      });
    }
    return this.manifests.get(collection)!;
  }

  /**
   * Add a file to a collection manifest.
   */
  addFileToManifest(collection: string, file: FileMetadata): void {
    const manifest = this.ensureManifest(collection);
    manifest.files.push(file);
    manifest.updatedAt = Date.now();
    this.persistManifest(manifest);
  }

  /**
   * Remove files from a collection manifest.
   */
  removeFilesFromManifest(collection: string, filesToRemove: FileMetadata[]): void {
    const manifest = this.manifests.get(collection);
    if (!manifest) return;

    manifest.files = manifest.files.filter((f) => !filesToRemove.includes(f));
    manifest.updatedAt = Date.now();
    this.persistManifest(manifest);
  }

  /**
   * Update manifest files (for compaction).
   */
  updateManifestFiles(collection: string, oldFiles: FileMetadata[], newFile: FileMetadata): void {
    const manifest = this.manifests.get(collection);
    if (!manifest) return;

    manifest.files = manifest.files.filter((f) => !oldFiles.includes(f));
    manifest.files.push(newFile);
    manifest.updatedAt = Date.now();
    this.persistManifest(manifest);
  }

  /**
   * Persist manifest to SQLite.
   */
  private persistManifest(manifest: CollectionManifest): void {
    this.storage.sqlExec(
      `INSERT OR REPLACE INTO manifests (collection, data) VALUES (?, ?)`,
      manifest.collection,
      JSON.stringify(manifest)
    );
  }

  // ============================================================================
  // File Operations
  // ============================================================================

  /**
   * Generate a unique file ID.
   */
  generateFileId(isCompacted: boolean = false): string {
    const prefix = isCompacted ? 'compacted-' : '';
    const random = Math.random().toString(36).slice(2, 10);
    return `${prefix}${Date.now()}-${random}`;
  }

  /**
   * Generate a unique flush ID for tracking pending flushes.
   */
  generateFlushId(): string {
    const random = Math.random().toString(36).slice(2, 10);
    return `${Date.now()}-${random}`;
  }

  /**
   * Generate file path for a collection.
   */
  generateFilePath(collection: string, fileId: string): string {
    const date = new Date().toISOString().split('T')[0];
    return `${collection}/${date}/${fileId}.parquet`;
  }

  // ============================================================================
  // Parquet Operations
  // ============================================================================

  /**
   * Combine Parquet data with JSON for fast parsing.
   *
   * Data format: [PAR1:4][json_length:4][json_data][parquet_data_without_magic]
   */
  combineParquetWithJson(
    parquetData: Uint8Array,
    documents: Record<string, unknown>[]
  ): Uint8Array {
    // Encode documents as JSON
    const jsonDocs = JSON.stringify(documents);
    const jsonBytes = new TextEncoder().encode(jsonDocs);

    // Create 4-byte length header for JSON data
    const jsonLength = new Uint8Array(4);
    new DataView(jsonLength.buffer).setUint32(0, jsonBytes.length, true);

    // Combine: [PAR1 magic:4][json_length:4][json_data][parquet_data_without_magic]
    const combined = new Uint8Array(
      4 + 4 + jsonBytes.length + parquetData.length - 4
    );
    combined.set(parquetData.slice(0, 4), 0); // Copy PAR1 magic bytes
    combined.set(jsonLength, 4);
    combined.set(jsonBytes, 8);
    combined.set(parquetData.slice(4), 8 + jsonBytes.length);

    return combined;
  }

  /**
   * Serialize documents to Parquet format.
   */
  serializeToParquet(
    docs: Array<{
      _id: string;
      _seq: number;
      _op: 'i' | 'u' | 'd';
      [key: string]: unknown;
    }>
  ): { data: Uint8Array; combined: Uint8Array } {
    const serializer = new RowGroupSerializer({ compression: 'snappy' });
    const rowGroup = serializer.serialize(docs);
    const combined = this.combineParquetWithJson(rowGroup.data, docs);
    return { data: rowGroup.data, combined };
  }

  // ============================================================================
  // Object Storage Operations with Two-Phase Commit
  // ============================================================================

  /**
   * Write a file to object storage with two-phase commit.
   *
   * Phase 1: Write pending flush marker
   * Phase 2: Write data block, update manifest
   * Cleanup: Delete pending flush marker
   */
  async writeFileWithTwoPhaseCommit(
    collection: string,
    docs: Array<{
      _id: string;
      _seq: number;
      _op: 'i' | 'u' | 'd';
      [key: string]: unknown;
    }>
  ): Promise<{ filePath: string; pendingPath: string }> {
    // Serialize to Parquet
    const { data: rowGroupData, combined } = this.serializeToParquet(docs);

    // Generate paths
    const fileId = this.generateFileId(false);
    const filePath = this.generateFilePath(collection, fileId);

    // Calculate LSN range
    const lsnValues = docs.map((d) => d._seq);
    const minLSN = Math.min(...lsnValues);
    const maxLSN = Math.max(...lsnValues);

    // Generate zone map for predicate pushdown filtering
    const zoneMap = generateZoneMapEntries(docs);

    // Create file metadata
    const fileMetadata: FileMetadata = {
      path: filePath,
      size: rowGroupData.length,
      rowCount: docs.length,
      minLSN,
      maxLSN,
      createdAt: Date.now(),
      zoneMap,
    };

    // Phase 1: Write pending flush marker
    const flushId = this.generateFlushId();
    const pendingFlush: PendingFlush = {
      flushId,
      collection,
      file: fileMetadata,
      timestamp: Date.now(),
    };

    const pendingPath = `_pending/${flushId}.json`;
    await this.bucket.put(pendingPath, JSON.stringify(pendingFlush));

    // Write data block to object storage
    await this.bucket.put(filePath, combined);

    // Phase 2: Update manifest
    this.addFileToManifest(collection, fileMetadata);

    // Cleanup: Delete pending flush marker
    await this.bucket.delete(pendingPath);

    return { filePath, pendingPath };
  }

  /**
   * Schedule compaction alarm.
   */
  async scheduleCompaction(): Promise<void> {
    await this.storage.setAlarm(Date.now() + COMPACTION_ALARM_DELAY_MS);
  }

  /**
   * Read a file from object storage.
   */
  async readFile(path: string): Promise<Uint8Array | null> {
    const data = await this.bucket.get(path);
    if (!data) return null;
    const buffer = await data.arrayBuffer();
    return new Uint8Array(buffer);
  }

  /**
   * Delete a file from object storage.
   */
  async deleteFile(path: string): Promise<void> {
    await this.bucket.delete(path);
  }

  /**
   * Get the underlying object storage bucket.
   *
   * Use this for platform-specific operations not covered by the abstraction.
   * Note: Using this may break portability.
   */
  getBucket(): IndexManagerBucket {
    return this.bucket;
  }

  /**
   * Parse documents from a combined Parquet/JSON data block.
   */
  parseParquetData(data: Uint8Array): Record<string, unknown>[] {
    // Verify Parquet magic bytes (PAR1)
    const magic = new TextDecoder().decode(data.slice(0, 4));
    if (magic !== PARQUET_MAGIC_BYTES) {
      return [];
    }

    try {
      if (data.length < 8) {
        return [];
      }

      // Read JSON length from bytes 4-7
      const jsonLength = new DataView(data.buffer, data.byteOffset + 4, 4).getUint32(0, true);

      // Validate JSON length is reasonable
      if (jsonLength === 0 || jsonLength > data.length - 8) {
        // Fall back to legacy parsing for old format data
        return this.parseParquetDataLegacy(data);
      }

      // Extract and parse JSON documents
      const jsonBytes = data.slice(8, 8 + jsonLength);
      const jsonStr = new TextDecoder().decode(jsonBytes);
      const docs = JSON.parse(jsonStr) as Record<string, unknown>[];
      return docs;
    } catch (error) {
      // If any error occurs, try legacy fallback
      logger.error('Failed to parse Parquet data, falling back to legacy', {
        operation: 'IndexManager.parseParquetData',
        dataLength: data.length,
        error: error instanceof Error ? error.message : String(error),
      });
      return this.parseParquetDataLegacy(data);
    }
  }

  /**
   * Legacy parser for older Parquet format without JSON header.
   */
  private parseParquetDataLegacy(data: Uint8Array): Record<string, unknown>[] {
    const results: Record<string, unknown>[] = [];

    try {
      const text = new TextDecoder().decode(data);
      // Match simple JSON objects containing _id field
      const jsonMatches = text.match(/\{[^{}]*"_id"[^{}]*\}/g);

      if (jsonMatches) {
        for (const match of jsonMatches) {
          try {
            const doc = JSON.parse(match);
            if (doc._id) {
              results.push(doc);
            }
          } catch {
            continue;
          }
        }
      }
    } catch (error) {
      logger.error('Failed to decode Parquet data', {
        operation: 'IndexManager.parseParquetDataLegacy',
        dataLength: data.length,
        error: error instanceof Error ? error.message : String(error),
      });
      return [];
    }

    return results;
  }
}
