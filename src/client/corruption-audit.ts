/**
 * Corruption Audit Trail
 *
 * Provides persistent logging and tracking of data corruption events
 * encountered during query operations. This audit system enables:
 * - Real-time corruption event tracking
 * - Persistent audit log across multiple queries
 * - Query-specific corruption summaries
 * - Integration with external monitoring systems
 */

import type { CorruptionReport, Filter, Document } from '@types';

// ============================================================================
// Types
// ============================================================================

/**
 * Extended audit entry with query context information.
 * Tracks not just the corruption but also the query that encountered it.
 */
export interface CorruptionAuditEntry {
  /** Path or name of the corrupted file */
  filePath: string;
  /** Error message describing the corruption */
  error: string;
  /** Stack trace if available */
  errorStack?: string;
  /** Timestamp when the corruption was detected */
  timestamp: Date;
  /** Query context that encountered the corruption */
  query: {
    /** Collection name */
    collection: string;
    /** Database name */
    database?: string;
    /** Query filter (if any) */
    filter?: object;
    /** Operation type (find, aggregate, etc.) */
    operation: string;
  };
}

/**
 * Summary of corruption encountered during a query or time period.
 */
export interface CorruptionSummary {
  /** Total number of corrupted files encountered */
  totalCorrupted: number;
  /** Number of unique files (some may be encountered multiple times) */
  uniqueFiles: number;
  /** List of affected collections */
  affectedCollections: string[];
  /** Time range of the audit entries */
  timeRange: {
    start: Date;
    end: Date;
  } | null;
  /** Whether any data loss occurred */
  hasDataLoss: boolean;
}

/**
 * Options for creating a CorruptionAudit instance.
 */
export interface CorruptionAuditOptions {
  /** Maximum number of entries to retain (oldest are evicted when exceeded) */
  maxEntries?: number;
  /** Whether to include error stack traces in entries */
  includeStackTraces?: boolean;
}

// ============================================================================
// CorruptionAudit Class
// ============================================================================

/**
 * Corruption Audit Trail for tracking data corruption events.
 *
 * This class provides a centralized audit log for corruption events
 * encountered during query operations. It can be used standalone or
 * integrated with Collection operations via the `onCorruptedFile` callback.
 *
 * @example
 * ```typescript
 * // Create an audit instance
 * const audit = new CorruptionAudit({ maxEntries: 1000 });
 *
 * // Use with collection queries
 * const docs = await collection.find({}, {
 *   skipCorruptedFiles: true,
 *   onCorruptedFile: (report) => audit.logFromReport(report, {
 *     collection: 'users',
 *     database: 'mydb',
 *     filter: {},
 *     operation: 'find'
 *   })
 * }).toArray();
 *
 * // Check for issues
 * const summary = audit.getSummary();
 * if (summary.hasDataLoss) {
 *   console.warn(`Encountered ${summary.totalCorrupted} corrupted files`);
 * }
 *
 * // Get detailed entries
 * const entries = audit.getEntries();
 * for (const entry of entries) {
 *   console.log(`File: ${entry.filePath}, Error: ${entry.error}`);
 * }
 * ```
 */
export class CorruptionAudit {
  private entries: CorruptionAuditEntry[] = [];
  private options: Required<CorruptionAuditOptions>;

  constructor(options: CorruptionAuditOptions = {}) {
    this.options = {
      maxEntries: options.maxEntries ?? 10000,
      includeStackTraces: options.includeStackTraces ?? false,
    };
  }

  // --------------------------------------------------------------------------
  // Logging Methods
  // --------------------------------------------------------------------------

  /**
   * Log a corruption event with full context.
   *
   * @param entry - Corruption audit entry (without timestamp, which is auto-added)
   */
  log(entry: Omit<CorruptionAuditEntry, 'timestamp'>): void {
    const fullEntry: CorruptionAuditEntry = {
      ...entry,
      timestamp: new Date(),
    };

    // Strip stack trace if not configured to include it
    if (!this.options.includeStackTraces) {
      delete fullEntry.errorStack;
    }

    this.entries.push(fullEntry);

    // Evict oldest entries if we exceed maxEntries
    if (this.entries.length > this.options.maxEntries) {
      this.entries = this.entries.slice(-this.options.maxEntries);
    }
  }

  /**
   * Log a corruption event from a CorruptionReport and query context.
   * This is a convenience method for use with the onCorruptedFile callback.
   *
   * @param report - The corruption report from the query engine
   * @param queryContext - Additional query context information
   */
  logFromReport(
    report: CorruptionReport,
    queryContext: {
      filter?: object;
      operation: string;
    }
  ): void {
    this.log({
      filePath: report.filename,
      error: report.error,
      query: {
        collection: report.collection,
        database: report.database,
        filter: queryContext.filter,
        operation: queryContext.operation,
      },
    });
  }

  // --------------------------------------------------------------------------
  // Query Methods
  // --------------------------------------------------------------------------

  /**
   * Get all audit entries.
   *
   * @returns Copy of all audit entries
   */
  getEntries(): CorruptionAuditEntry[] {
    return [...this.entries];
  }

  /**
   * Get entries filtered by collection name.
   *
   * @param collection - Collection name to filter by
   * @returns Filtered audit entries
   */
  getEntriesByCollection(collection: string): CorruptionAuditEntry[] {
    return this.entries.filter((e) => e.query.collection === collection);
  }

  /**
   * Get entries filtered by database name.
   *
   * @param database - Database name to filter by
   * @returns Filtered audit entries
   */
  getEntriesByDatabase(database: string): CorruptionAuditEntry[] {
    return this.entries.filter((e) => e.query.database === database);
  }

  /**
   * Get entries within a time range.
   *
   * @param start - Start of time range (inclusive)
   * @param end - End of time range (inclusive)
   * @returns Filtered audit entries
   */
  getEntriesByTimeRange(start: Date, end: Date): CorruptionAuditEntry[] {
    return this.entries.filter(
      (e) => e.timestamp >= start && e.timestamp <= end
    );
  }

  /**
   * Get entries for a specific file path.
   *
   * @param filePath - File path to search for
   * @returns Filtered audit entries
   */
  getEntriesByFile(filePath: string): CorruptionAuditEntry[] {
    return this.entries.filter((e) => e.filePath === filePath);
  }

  // --------------------------------------------------------------------------
  // Summary Methods
  // --------------------------------------------------------------------------

  /**
   * Get a summary of all corruption events.
   *
   * @returns Corruption summary
   */
  getSummary(): CorruptionSummary {
    return this.buildSummary(this.entries);
  }

  /**
   * Get a summary for a specific collection.
   *
   * @param collection - Collection name
   * @returns Corruption summary for the collection
   */
  getSummaryByCollection(collection: string): CorruptionSummary {
    return this.buildSummary(this.getEntriesByCollection(collection));
  }

  /**
   * Get a summary for a time range.
   *
   * @param start - Start of time range
   * @param end - End of time range
   * @returns Corruption summary for the time range
   */
  getSummaryByTimeRange(start: Date, end: Date): CorruptionSummary {
    return this.buildSummary(this.getEntriesByTimeRange(start, end));
  }

  private buildSummary(entries: CorruptionAuditEntry[]): CorruptionSummary {
    const uniqueFiles = new Set(entries.map((e) => e.filePath));
    const collections = new Set(entries.map((e) => e.query.collection));

    let timeRange: { start: Date; end: Date } | null = null;
    if (entries.length > 0) {
      const timestamps = entries.map((e) => e.timestamp.getTime());
      timeRange = {
        start: new Date(Math.min(...timestamps)),
        end: new Date(Math.max(...timestamps)),
      };
    }

    return {
      totalCorrupted: entries.length,
      uniqueFiles: uniqueFiles.size,
      affectedCollections: Array.from(collections),
      timeRange,
      hasDataLoss: entries.length > 0,
    };
  }

  // --------------------------------------------------------------------------
  // Management Methods
  // --------------------------------------------------------------------------

  /**
   * Clear all audit entries.
   */
  clear(): void {
    this.entries = [];
  }

  /**
   * Clear entries older than a specified date.
   *
   * @param olderThan - Date threshold; entries older than this are removed
   * @returns Number of entries removed
   */
  clearOlderThan(olderThan: Date): number {
    const originalCount = this.entries.length;
    this.entries = this.entries.filter((e) => e.timestamp >= olderThan);
    return originalCount - this.entries.length;
  }

  /**
   * Get the number of entries in the audit log.
   */
  get size(): number {
    return this.entries.length;
  }

  /**
   * Check if the audit log is empty.
   */
  get isEmpty(): boolean {
    return this.entries.length === 0;
  }

  // --------------------------------------------------------------------------
  // Integration Helpers
  // --------------------------------------------------------------------------

  /**
   * Create an onCorruptedFile callback for use with collection operations.
   * This simplifies integration with the skipCorruptedFiles option.
   *
   * @param context - Query context to include in audit entries
   * @returns Callback function for onCorruptedFile option
   *
   * @example
   * ```typescript
   * const audit = new CorruptionAudit();
   *
   * const docs = await collection.find(filter, {
   *   skipCorruptedFiles: true,
   *   onCorruptedFile: audit.createCallback({
   *     filter,
   *     operation: 'find'
   *   })
   * }).toArray();
   * ```
   */
  createCallback(context: {
    filter?: Filter<Document>;
    operation: string;
  }): (report: CorruptionReport) => void {
    return (report: CorruptionReport) => {
      this.logFromReport(report, {
        filter: context.filter as object | undefined,
        operation: context.operation,
      });
    };
  }

  /**
   * Export audit entries as JSON for external storage or monitoring.
   *
   * @returns JSON string of all audit entries
   */
  toJSON(): string {
    return JSON.stringify(this.entries, null, 2);
  }

  /**
   * Import audit entries from JSON.
   * Merges with existing entries.
   *
   * @param json - JSON string of audit entries
   * @returns Number of entries imported
   */
  fromJSON(json: string): number {
    const imported = JSON.parse(json) as CorruptionAuditEntry[];

    // Convert date strings back to Date objects
    for (const entry of imported) {
      entry.timestamp = new Date(entry.timestamp);
    }

    const originalCount = this.entries.length;
    this.entries.push(...imported);

    // Evict oldest if we exceed maxEntries
    if (this.entries.length > this.options.maxEntries) {
      this.entries = this.entries.slice(-this.options.maxEntries);
    }

    return this.entries.length - originalCount;
  }
}

// ============================================================================
// Factory Functions
// ============================================================================

/**
 * Create a new CorruptionAudit instance.
 *
 * @param options - Audit options
 * @returns New CorruptionAudit instance
 */
export function createCorruptionAudit(
  options?: CorruptionAuditOptions
): CorruptionAudit {
  return new CorruptionAudit(options);
}
