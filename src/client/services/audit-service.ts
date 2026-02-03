/**
 * Audit Service
 *
 * Encapsulates corruption auditing functionality for MongoDB collections.
 * Tracks corrupted files encountered during read operations.
 */

import type { CorruptionReport, QueryMetadata, FindOptions } from '@types';
import { logger } from '@utils/logger.js';

/**
 * Context for processing corrupted files.
 */
export interface CorruptionContext {
  /** Collection name */
  collection: string;
  /** Database name */
  database: string;
  /** Find options that may contain corruption handling settings */
  options?: FindOptions;
}

/**
 * Service for tracking and auditing data corruption.
 * Manages corruption reports and query metadata.
 */
export class AuditService {
  private corruptedFiles: CorruptionReport[] = [];
  private totalFilesProcessed: number = 0;

  /**
   * Reset the audit state for a new operation.
   */
  reset(): void {
    this.corruptedFiles = [];
    this.totalFilesProcessed = 0;
  }

  /**
   * Record a file processing attempt.
   */
  recordFileProcessed(): void {
    this.totalFilesProcessed++;
  }

  /**
   * Handle a corrupted file based on the provided context.
   *
   * @param file - Path to the corrupted file
   * @param error - Error that occurred
   * @param context - Corruption handling context
   * @returns true if the corruption was handled (skipped), false if it should throw
   */
  handleCorruptedFile(
    file: string,
    error: unknown,
    context: CorruptionContext
  ): boolean {
    if (!context.options?.skipCorruptedFiles) {
      return false;
    }

    // Create corruption report
    const report: CorruptionReport = {
      filename: file,
      error: error instanceof Error ? error.message : String(error),
      timestamp: new Date(),
      collection: context.collection,
      database: context.database,
    };

    // Track the corruption
    this.corruptedFiles.push(report);

    // Call the optional callback for real-time notification
    if (context.options.onCorruptedFile) {
      context.options.onCorruptedFile(report);
    }

    // Log warning for visibility
    logger.warn('Skipping corrupted Parquet file', {
      file,
      error: error instanceof Error ? error : String(error),
    });

    return true;
  }

  /**
   * Get query metadata including corruption information.
   *
   * @returns Query metadata object
   */
  getQueryMetadata(): QueryMetadata {
    return {
      corruptedFiles: [...this.corruptedFiles],
      totalFilesProcessed: this.totalFilesProcessed,
      skippedCount: this.corruptedFiles.length,
      hasDataLoss: this.corruptedFiles.length > 0,
    };
  }

  /**
   * Get the list of corrupted files encountered.
   */
  getCorruptedFiles(): CorruptionReport[] {
    return [...this.corruptedFiles];
  }

  /**
   * Check if any corruption was encountered.
   */
  hasCorruption(): boolean {
    return this.corruptedFiles.length > 0;
  }

  /**
   * Get a summary of the corruption state.
   */
  getSummary(): {
    filesProcessed: number;
    filesCorrupted: number;
    hasDataLoss: boolean;
  } {
    return {
      filesProcessed: this.totalFilesProcessed,
      filesCorrupted: this.corruptedFiles.length,
      hasDataLoss: this.corruptedFiles.length > 0,
    };
  }
}
