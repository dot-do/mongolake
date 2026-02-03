/**
 * WAL Manager - Write-Ahead Log Operations
 *
 * Handles WAL persistence, recovery, and checkpointing for durability.
 */

import type { StorageBackend, WalEntry, BufferedDoc, CollectionManifest, PendingFlush } from './types.js';
import type { R2Bucket } from '@cloudflare/workers-types';
import { MAX_WAL_SIZE_BYTES, MAX_WAL_ENTRIES } from '../../constants.js';
import { logger } from '../../utils/logger.js';

/**
 * WAL Manager handles write-ahead log operations for durability.
 *
 * Responsibilities:
 * - Persisting WAL entries to SQLite
 * - Recovering state from WAL on startup
 * - Checkpointing (truncating flushed entries)
 * - Managing LSN counters
 */
export class WalManager {
  /** Current Log Sequence Number */
  private currentLSN: number = 0;

  /** LSN of last successfully flushed write */
  private flushedLSN: number = 0;

  /** In-memory WAL entries for fast access */
  private walEntries: WalEntry[] = [];

  /** Current WAL size in bytes (estimated) */
  private walSizeBytes: number = 0;

  /** Maximum WAL size in bytes before forcing flush */
  private maxWalSizeBytes: number = MAX_WAL_SIZE_BYTES;

  /** Maximum number of WAL entries before forcing flush */
  private maxWalEntries: number = MAX_WAL_ENTRIES;

  constructor(private storage: StorageBackend) {}

  // ============================================================================
  // Initialization
  // ============================================================================

  /**
   * Initialize WAL tables in SQLite.
   */
  initializeTables(): void {
    // Create Write-Ahead Log table for durability
    this.storage.sqlExec(`
      CREATE TABLE IF NOT EXISTS wal (
        lsn INTEGER PRIMARY KEY,
        collection TEXT NOT NULL,
        op TEXT NOT NULL,
        doc_id TEXT NOT NULL,
        document TEXT,
        flushed INTEGER DEFAULT 0,
        created_at INTEGER DEFAULT (strftime('%s', 'now'))
      )
    `);

    // Create metadata table for persistent configuration
    this.storage.sqlExec(`
      CREATE TABLE IF NOT EXISTS metadata (
        key TEXT PRIMARY KEY,
        value TEXT
      )
    `);

    // Create manifest table for collection file tracking
    this.storage.sqlExec(`
      CREATE TABLE IF NOT EXISTS manifests (
        collection TEXT PRIMARY KEY,
        data TEXT
      )
    `);
  }

  /**
   * Recover LSN counters and unflushed WAL entries from SQLite.
   *
   * @param addToBuffer - Callback to replay entries to buffer
   * @returns Recovered manifests map
   */
  recoverState(addToBuffer: (doc: BufferedDoc) => void): Map<string, CollectionManifest> {
    // Recover LSN counter from SQLite metadata
    const lsnRows = this.storage.sqlExec(
      `SELECT value FROM metadata WHERE key = 'currentLSN'`
    ).toArray();
    if (lsnRows.length > 0 && lsnRows[0]?.value) {
      this.currentLSN = parseInt(String(lsnRows[0].value), 10);
    }

    // Recover flushed LSN from SQLite metadata
    const flushedRows = this.storage.sqlExec(
      `SELECT value FROM metadata WHERE key = 'flushedLSN'`
    ).toArray();
    if (flushedRows.length > 0 && flushedRows[0]?.value) {
      this.flushedLSN = parseInt(String(flushedRows[0].value), 10);
    }

    // Recover WAL size from SQLite metadata
    const walSizeRows = this.storage.sqlExec(
      `SELECT value FROM metadata WHERE key = 'walSizeBytes'`
    ).toArray();
    if (walSizeRows.length > 0 && walSizeRows[0]?.value) {
      this.walSizeBytes = parseInt(String(walSizeRows[0].value), 10);
    }

    // Recover unflushed WAL entries from SQLite
    const walRows = this.storage.sqlExec(
      `SELECT lsn, collection, op, doc_id, document FROM wal WHERE flushed = 0 ORDER BY lsn`
    ).toArray();

    // Reset WAL size if we're recalculating from entries
    let recoveredWalSize = 0;

    for (const row of walRows) {
      const documentJson = row.document as string;
      const entry: WalEntry = {
        lsn: row.lsn as number,
        collection: row.collection as string,
        op: row.op as 'i' | 'u' | 'd',
        docId: row.doc_id as string,
        document: JSON.parse(documentJson),
        flushed: false,
      };
      this.walEntries.push(entry);

      // Track recovered WAL size
      recoveredWalSize += this.estimateEntrySize(entry, documentJson);

      // Replay to buffer
      const bufferedDoc: BufferedDoc = {
        _id: entry.docId,
        _seq: entry.lsn,
        _op: entry.op,
        collection: entry.collection,
        document: entry.document,
        lsn: entry.lsn,
      };
      addToBuffer(bufferedDoc);
    }

    // Use recovered size if no persisted size or they differ significantly
    if (this.walSizeBytes === 0 || Math.abs(this.walSizeBytes - recoveredWalSize) > 1024) {
      this.walSizeBytes = recoveredWalSize;
    }

    // Recover manifests from SQLite
    const manifests = new Map<string, CollectionManifest>();
    const manifestRows = this.storage.sqlExec(
      `SELECT collection, data FROM manifests`
    ).toArray();
    for (const row of manifestRows) {
      const manifest = JSON.parse(row.data as string) as CollectionManifest;
      manifests.set(row.collection as string, manifest);
    }

    return manifests;
  }

  /**
   * Recover pending flushes from R2 during initialization.
   * This implements the recovery phase of the two-phase commit pattern.
   */
  async recoverPendingFlushes(
    bucket: R2Bucket,
    manifests: Map<string, CollectionManifest>
  ): Promise<void> {
    try {
      // List all pending flush markers in R2
      const pendingList = await bucket.list({ prefix: '_pending/' });

      for (const obj of pendingList.objects) {
        try {
          // Read the pending flush record
          const pendingData = await bucket.get(obj.key);
          if (!pendingData) continue;

          const pendingFlush = JSON.parse(await pendingData.text()) as PendingFlush;

          // Check if the data block was successfully written
          const dataBlockExists = await bucket.head(pendingFlush.file.path);

          if (dataBlockExists) {
            // Data block exists - complete the flush by updating manifest
            this.completePendingFlush(pendingFlush, manifests);
          }
          // If data block doesn't exist, this is a partial failure - just delete the pending marker

          // Delete the pending flush marker (cleanup)
          await bucket.delete(obj.key);
        } catch (error) {
          // Log but don't fail recovery for individual pending flushes
          logger.error('Failed to process pending flush', {
            operation: 'WalManager.recoverPendingFlushes',
            shardId: this.storage.getShardId(),
            pendingKey: obj.key,
            error: error instanceof Error ? error.message : String(error),
            stack: error instanceof Error ? error.stack : undefined,
          });
        }
      }
    } catch (error) {
      // If we can't list pending flushes, continue initialization
      logger.error('Failed to list pending flushes', {
        operation: 'WalManager.recoverPendingFlushes',
        shardId: this.storage.getShardId(),
        error: error instanceof Error ? error.message : String(error),
        stack: error instanceof Error ? error.stack : undefined,
      });
    }
  }

  /**
   * Complete a pending flush by updating the SQLite manifest.
   */
  private completePendingFlush(
    pendingFlush: PendingFlush,
    manifests: Map<string, CollectionManifest>
  ): void {
    const { collection, file } = pendingFlush;

    // Ensure collection manifest exists
    if (!manifests.has(collection)) {
      manifests.set(collection, {
        collection,
        files: [],
        updatedAt: Date.now(),
      });
    }

    const manifest = manifests.get(collection)!;

    // Check if file is already in manifest (idempotency)
    const alreadyExists = manifest.files.some(f => f.path === file.path);
    if (alreadyExists) return;

    // Add file to manifest
    manifest.files.push(file);
    manifest.updatedAt = Date.now();

    // Persist manifest to SQLite
    this.storage.sqlExec(
      `INSERT OR REPLACE INTO manifests (collection, data) VALUES (?, ?)`,
      collection,
      JSON.stringify(manifest)
    );
  }

  // ============================================================================
  // Write Operations
  // ============================================================================

  /**
   * Allocate the next LSN for a write operation.
   */
  allocateLSN(): number {
    this.currentLSN++;
    return this.currentLSN;
  }

  /**
   * Get the current LSN without incrementing.
   */
  getCurrentLSN(): number {
    return this.currentLSN;
  }

  /**
   * Get the flushed LSN.
   */
  getFlushedLSN(): number {
    return this.flushedLSN;
  }

  /**
   * Persist a WAL entry to SQLite.
   */
  persistEntry(entry: WalEntry): void {
    // Serialize document for storage
    const documentJson = JSON.stringify(entry.document);

    // Persist WAL entry to SQLite for durability
    this.storage.sqlExec(
      `INSERT INTO wal (lsn, collection, op, doc_id, document) VALUES (?, ?, ?, ?, ?)`,
      entry.lsn,
      entry.collection,
      entry.op,
      entry.docId,
      documentJson
    );

    // Track in-memory for quick access
    this.walEntries.push(entry);

    // Update WAL size tracking (estimate based on serialized document + overhead)
    const entrySize = this.estimateEntrySize(entry, documentJson);
    this.walSizeBytes += entrySize;

    // Persist current LSN to SQLite metadata
    this.storage.sqlExec(
      `INSERT OR REPLACE INTO metadata (key, value) VALUES ('currentLSN', ?)`,
      String(this.currentLSN)
    );

    // Persist WAL size to metadata for recovery
    this.storage.sqlExec(
      `INSERT OR REPLACE INTO metadata (key, value) VALUES ('walSizeBytes', ?)`,
      String(this.walSizeBytes)
    );
  }

  /**
   * Estimate the size of a WAL entry in bytes.
   */
  private estimateEntrySize(entry: WalEntry, documentJson?: string): number {
    // Entry overhead: lsn (8) + collection (~50 avg) + op (1) + docId (~24 avg) + flushed (1) + created_at (8)
    const overhead = 92;
    const docSize = documentJson?.length ?? JSON.stringify(entry.document).length;
    return overhead + docSize;
  }

  /**
   * Create a WAL entry from a buffered document.
   */
  createWalEntry(doc: BufferedDoc): WalEntry {
    return {
      lsn: doc.lsn,
      collection: doc.collection,
      op: doc._op,
      docId: doc._id,
      document: doc.document,
      flushed: false,
    };
  }

  // ============================================================================
  // Flush Operations
  // ============================================================================

  /**
   * Mark WAL entries as flushed up to the given LSN.
   */
  markFlushed(lsn: number): void {
    // Mark all current WAL entries as flushed
    this.storage.sqlExec(
      `UPDATE wal SET flushed = 1 WHERE lsn <= ?`,
      lsn
    );

    // Calculate size of flushed entries and update in-memory WAL entries
    let flushedSize = 0;
    for (const entry of this.walEntries) {
      if (entry.lsn <= lsn && !entry.flushed) {
        entry.flushed = true;
        flushedSize += this.estimateEntrySize(entry);
      }
    }

    // Update WAL size tracking
    this.walSizeBytes = Math.max(0, this.walSizeBytes - flushedSize);

    // Update flushed LSN tracking
    this.flushedLSN = lsn;

    // Persist flushed LSN to SQLite
    this.storage.sqlExec(
      `INSERT OR REPLACE INTO metadata (key, value) VALUES ('flushedLSN', ?)`,
      String(this.flushedLSN)
    );

    // Persist updated WAL size
    this.storage.sqlExec(
      `INSERT OR REPLACE INTO metadata (key, value) VALUES ('walSizeBytes', ?)`,
      String(this.walSizeBytes)
    );
  }

  // ============================================================================
  // Checkpoint Operations
  // ============================================================================

  /**
   * Checkpoint the WAL by removing flushed entries.
   *
   * Deletes WAL entries that have been successfully flushed to R2,
   * freeing up SQLite storage space.
   */
  checkpoint(): void {
    // Delete flushed WAL entries from SQLite
    this.storage.sqlExec(`DELETE FROM wal WHERE flushed = 1`);

    // Remove flushed entries from in-memory WAL
    this.walEntries = this.walEntries.filter((e) => !e.flushed);

    // Recalculate WAL size from remaining entries
    this.walSizeBytes = this.walEntries.reduce(
      (size, entry) => size + this.estimateEntrySize(entry),
      0
    );

    // Persist updated WAL size
    this.storage.sqlExec(
      `INSERT OR REPLACE INTO metadata (key, value) VALUES ('walSizeBytes', ?)`,
      String(this.walSizeBytes)
    );
  }

  // ============================================================================
  // WAL Size Monitoring
  // ============================================================================

  /**
   * Get the current WAL size in bytes.
   */
  getWalSizeBytes(): number {
    return this.walSizeBytes;
  }

  /**
   * Get the current number of unflushed WAL entries.
   */
  getWalEntryCount(): number {
    return this.walEntries.filter((e) => !e.flushed).length;
  }

  /**
   * Check if a forced flush is required due to WAL size limits.
   *
   * @returns Object indicating if flush is needed and the reason
   */
  shouldForceFlush(): { needed: boolean; reason: 'size' | 'entries' | null } {
    if (this.walSizeBytes >= this.maxWalSizeBytes) {
      return { needed: true, reason: 'size' };
    }
    const unflushedCount = this.getWalEntryCount();
    if (unflushedCount >= this.maxWalEntries) {
      return { needed: true, reason: 'entries' };
    }
    return { needed: false, reason: null };
  }

  /**
   * Configure WAL size limits.
   *
   * @param maxSizeBytes - Maximum WAL size in bytes (optional)
   * @param maxEntries - Maximum number of WAL entries (optional)
   */
  configureWalLimits(maxSizeBytes?: number, maxEntries?: number): void {
    if (maxSizeBytes !== undefined) {
      this.maxWalSizeBytes = maxSizeBytes;
    }
    if (maxEntries !== undefined) {
      this.maxWalEntries = maxEntries;
    }
  }

  /**
   * Get the configured WAL limits.
   */
  getWalLimits(): { maxSizeBytes: number; maxEntries: number } {
    return {
      maxSizeBytes: this.maxWalSizeBytes,
      maxEntries: this.maxWalEntries,
    };
  }

  // ============================================================================
  // Manifest Operations
  // ============================================================================

  /**
   * Persist a collection manifest to SQLite.
   */
  persistManifest(manifest: CollectionManifest): void {
    this.storage.sqlExec(
      `INSERT OR REPLACE INTO manifests (collection, data) VALUES (?, ?)`,
      manifest.collection,
      JSON.stringify(manifest)
    );
  }
}
