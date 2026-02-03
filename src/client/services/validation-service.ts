/**
 * Validation Service
 *
 * Encapsulates batch validation logic for MongoDB collection operations.
 * Provides validation for batch size limits to prevent resource exhaustion.
 */

import { MAX_BATCH_SIZE, MAX_BATCH_BYTES } from '@mongolake/constants.js';

/**
 * Options for batch validation.
 */
export interface BatchValidationOptions {
  /** Maximum number of documents allowed in a batch */
  maxSize?: number;
  /** Maximum total bytes allowed in a batch */
  maxBytes?: number;
}

/**
 * Service for validating batch operations.
 * Ensures batch operations don't exceed configured limits.
 */
export class ValidationService {
  private readonly maxSize: number;
  private readonly maxBytes: number;

  constructor(options: BatchValidationOptions = {}) {
    this.maxSize = options.maxSize ?? MAX_BATCH_SIZE;
    this.maxBytes = options.maxBytes ?? MAX_BATCH_BYTES;
  }

  /**
   * Calculate the approximate byte size of a document array.
   * Uses JSON serialization as a reasonable approximation of document size.
   *
   * @param docs - Array of documents to measure
   * @returns Total approximate size in bytes
   */
  calculateBatchBytes<T>(docs: T[]): number {
    return docs.reduce((total, doc) => total + JSON.stringify(doc).length, 0);
  }

  /**
   * Validate batch size limits for bulk operations.
   *
   * @param docs - Documents to validate
   * @param operation - Name of the operation for error messages
   * @throws Error if batch exceeds size limits
   *
   * @example
   * ```typescript
   * validationService.validateBatchLimits(documents, 'insertMany');
   * ```
   */
  validateBatchLimits<T>(docs: T[], operation: string): void {
    if (docs.length > this.maxSize) {
      throw new Error(
        `${operation}: batch size ${docs.length} exceeds maximum allowed ${this.maxSize} documents`
      );
    }

    const batchBytes = this.calculateBatchBytes(docs);
    if (batchBytes > this.maxBytes) {
      throw new Error(
        `${operation}: batch size ${batchBytes} bytes exceeds maximum allowed ${this.maxBytes} bytes (${Math.round(this.maxBytes / (1024 * 1024))}MB)`
      );
    }
  }

  /**
   * Check if a batch would exceed limits without throwing.
   *
   * @param docs - Documents to check
   * @returns Object with validation result and details
   */
  checkBatchLimits<T>(docs: T[]): {
    valid: boolean;
    exceedsCount: boolean;
    exceedsBytes: boolean;
    documentCount: number;
    byteCount: number;
  } {
    const documentCount = docs.length;
    const byteCount = this.calculateBatchBytes(docs);
    const exceedsCount = documentCount > this.maxSize;
    const exceedsBytes = byteCount > this.maxBytes;

    return {
      valid: !exceedsCount && !exceedsBytes,
      exceedsCount,
      exceedsBytes,
      documentCount,
      byteCount,
    };
  }

  /**
   * Get the configured maximum batch size.
   */
  getMaxSize(): number {
    return this.maxSize;
  }

  /**
   * Get the configured maximum batch bytes.
   */
  getMaxBytes(): number {
    return this.maxBytes;
  }
}
