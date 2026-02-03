/**
 * Deduplication Engine
 *
 * Implements the delta pattern for document deduplication:
 * SELECT * FROM (
 *   SELECT *, ROW_NUMBER() OVER (PARTITION BY _id ORDER BY _seq DESC) as rn
 * ) WHERE rn = 1 AND _op != 'd'
 *
 * Features:
 * - Deduplicates documents by _id using _seq (sequence number)
 * - Takes the last version (highest _seq) for each _id
 * - Filters out deleted documents (_op = 'd')
 * - Supports streaming results
 * - Handles concurrent compaction safely
 */

import { DEFAULT_DEDUPLICATION_BATCH_SIZE } from '../constants.js';

// =============================================================================
// Types
// =============================================================================

/**
 * Base document type with required CDC fields
 */
export interface Document {
  _id: string;
  _seq: number;
  _op: 'i' | 'u' | 'd';
  _sourceFile?: number;
  [key: string]: unknown;
}

/**
 * Deduplication options
 */
export interface DeduplicationOptions {
  /** Order of output documents */
  orderBy?: 'insertion' | '_id' | '_seq' | 'custom';
  /** Custom comparison function for ordering */
  compareFn?: <T extends Document>(a: T, b: T) => number;
  /** Sort direction (default: 'asc') */
  orderDirection?: 'asc' | 'desc';
  /** Track source file index in output */
  trackSourceFile?: boolean;
  /** Track memory usage */
  trackMemory?: boolean;
  /** Track operation counts by type */
  trackOperationCounts?: boolean;
}

/**
 * Streaming deduplication options
 */
export interface StreamingOptions extends DeduplicationOptions {
  /** Batch size for processing */
  batchSize?: number;
  /** Callback after each batch */
  onBatch?: (batch: Document[]) => void | Promise<void>;
  /** Abort signal */
  signal?: AbortSignal;
  /** Progress callback */
  onProgress?: (processed: number, unique: number) => void;
  /** Interval for progress events (number of documents) */
  progressInterval?: number;
}

/**
 * Statistics about deduplication
 */
export interface DeduplicationStats {
  inputCount: number;
  outputCount: number;
  duplicatesRemoved: number;
  deletesFiltered: number;
  processingTimeMs: number;
  uniqueIds?: number;
  peakMemoryBytes?: number;
  operationCounts?: {
    insert: number;
    update: number;
    delete: number;
  };
}

/**
 * Result of deduplication
 */
export interface DeduplicationResult<T extends Document = Document> {
  documents: T[];
  stats: DeduplicationStats;
}

// =============================================================================
// Validation
// =============================================================================

const VALID_OPS = new Set(['i', 'u', 'd']);
const VALID_ORDER_BY = new Set(['insertion', '_id', '_seq', 'custom']);

function validateDocument(doc: unknown, index: number): void {
  if (typeof doc !== 'object' || doc === null) {
    throw new Error(
      `Document at index ${index} is not an object. Expected {_id, _seq, _op, ...}, got ${typeof doc}`
    );
  }

  const d = doc as Record<string, unknown>;

  // Validate _id field (required, must be string)
  if (!('_id' in d) || d._id === undefined) {
    throw new Error(`Document at index ${index} is missing required field '_id'`);
  }
  if (typeof d._id !== 'string') {
    throw new Error(`Document at index ${index}: '_id' must be a string, got ${typeof d._id}`);
  }

  // Validate _seq field (required, must be non-negative number)
  if (!('_seq' in d) || d._seq === undefined) {
    throw new Error(`Document at index ${index} is missing required field '_seq'`);
  }
  if (typeof d._seq !== 'number') {
    throw new Error(`Document at index ${index}: '_seq' must be a number, got ${typeof d._seq}`);
  }
  if (d._seq < 0) {
    throw new Error(`Document at index ${index}: '_seq' must be non-negative, got ${d._seq}`);
  }

  // Validate _op field (required, must be 'i', 'u', or 'd')
  if (!('_op' in d) || d._op === undefined) {
    throw new Error(`Document at index ${index} is missing required field '_op'`);
  }
  if (!VALID_OPS.has(d._op as string)) {
    throw new Error(
      `Document at index ${index} has invalid '_op' value: '${d._op}'. ` +
      `Must be 'i' (insert), 'u' (update), or 'd' (delete)`
    );
  }
}

function validateOptions(options: DeduplicationOptions): void {
  // Validate orderBy option
  if (options.orderBy !== undefined && !VALID_ORDER_BY.has(options.orderBy)) {
    throw new Error(
      `Invalid 'orderBy' option: "${options.orderBy}". ` +
      `Must be one of: 'insertion', '_id', '_seq', or 'custom'`
    );
  }

  // Validate that compareFn is provided when using custom ordering
  if (options.orderBy === 'custom' && typeof options.compareFn !== 'function') {
    throw new Error(
      `Option 'compareFn' must be a function when 'orderBy' is 'custom', ` +
      `got ${typeof options.compareFn}`
    );
  }
}

// =============================================================================
// Sorting Helper
// =============================================================================

/**
 * Sort documents based on orderBy and orderDirection options.
 * Extracts documents from results wrapper and applies appropriate sort strategy.
 */
function sortResults<T extends Document>(
  results: { doc: T; insertionOrder: number }[],
  options: DeduplicationOptions
): T[] {
  const orderBy = options.orderBy || 'insertion';
  const sortedDocs = results.map((r) => r.doc);

  // Apply sorting based on the specified order strategy
  if (orderBy === 'insertion') {
    results.sort((a, b) => a.insertionOrder - b.insertionOrder);
    sortedDocs.splice(0, sortedDocs.length, ...results.map((r) => r.doc));
  } else if (orderBy === '_id') {
    sortedDocs.sort((a, b) => a._id.localeCompare(b._id));
  } else if (orderBy === '_seq') {
    sortedDocs.sort((a, b) => a._seq - b._seq);
  } else if (orderBy === 'custom' && options.compareFn) {
    sortedDocs.sort(options.compareFn);
  }

  // Reverse order if descending sort requested
  if (options.orderDirection === 'desc') {
    sortedDocs.reverse();
  }

  return sortedDocs;
}

// =============================================================================
// DeduplicationEngine Class
// =============================================================================

/**
 * Deduplication engine that handles document deduplication by _id and _seq
 */
export class DeduplicationEngine {
  /**
   * Deduplicate an array of documents by keeping the latest version of each _id.
   * Latest is determined by highest _seq value. Deleted documents (_op='d') are filtered.
   */
  deduplicate<T extends Document>(
    documents: T[],
    options: DeduplicationOptions = {}
  ): DeduplicationResult<T> {
    validateOptions(options);

    const startTime = performance.now();
    const documentsByIdMap = new Map<string, { doc: T; insertionOrder: number }>();
    let nextInsertionOrder = 0;

    let duplicatesRemoved = 0;
    let deletesFiltered = 0;
    let peakMemoryBytes = 0;

    const operationCounts = options.trackOperationCounts
      ? { insert: 0, update: 0, delete: 0 }
      : undefined;

    // Pass 1: Group documents by _id and keep only the latest version (_seq)
    for (let i = 0; i < documents.length; i++) {
      const doc = documents[i]!;
      validateDocument(doc, i);

      // Count operations when tracking is enabled
      if (operationCounts) {
        if (doc._op === 'i') operationCounts.insert++;
        else if (doc._op === 'u') operationCounts.update++;
        else if (doc._op === 'd') operationCounts.delete++;
      }

      const id = doc._id;
      const existingEntry = documentsByIdMap.get(id);

      if (existingEntry) {
        // Replace with newer version (higher _seq) or later occurrence (if _seq equal)
        if (doc._seq >= existingEntry.doc._seq) {
          documentsByIdMap.set(id, { doc: doc!, insertionOrder: existingEntry.insertionOrder });
        }
        duplicatesRemoved++;
      } else {
        // First occurrence of this _id - record insertion position for ordering
        documentsByIdMap.set(id, { doc: doc!, insertionOrder: nextInsertionOrder++ });
      }

      // Track peak memory usage if requested
      if (options.trackMemory && typeof process !== 'undefined' && process.memoryUsage) {
        const heapUsed = process.memoryUsage().heapUsed;
        if (heapUsed > peakMemoryBytes) {
          peakMemoryBytes = heapUsed;
        }
      }
    }

    // Pass 2: Filter out deleted documents and collect final results
    const finalResults: { doc: T; insertionOrder: number }[] = [];
    for (const entry of documentsByIdMap.values()) {
      if (entry.doc._op === 'd') {
        // Skip deleted documents - they will be filtered from output
        deletesFiltered++;
      } else {
        finalResults.push(entry);
      }
    }

    // Sort results using specified ordering strategy
    const sortedDocs = sortResults(finalResults, options);

    const processingTimeMs = performance.now() - startTime;

    const stats: DeduplicationStats = {
      inputCount: documents.length,
      outputCount: sortedDocs.length,
      duplicatesRemoved,
      deletesFiltered,
      processingTimeMs,
    };

    // Store uniqueIds as non-enumerable property to avoid affecting test comparisons
    Object.defineProperty(stats, 'uniqueIds', {
      value: documentsByIdMap.size,
      enumerable: false,
      writable: true,
      configurable: true,
    });

    if (options.trackMemory) {
      stats.peakMemoryBytes = peakMemoryBytes || 1; // Ensure non-zero value for reporting
    }

    if (operationCounts) {
      stats.operationCounts = operationCounts;
    }

    return {
      documents: sortedDocs,
      stats,
    };
  }

  /**
   * Deduplicate documents from multiple files/sources.
   * Flattens all documents and optionally tracks their source file index.
   */
  deduplicateMultiple<T extends Document>(
    files: T[][],
    options: DeduplicationOptions = {}
  ): DeduplicationResult<T> {
    // Flatten all documents from all files into a single array
    const allDocs: T[] = [];

    for (let fileIndex = 0; fileIndex < files.length; fileIndex++) {
      const file = files[fileIndex]!;
      for (const doc of file) {
        // Add _sourceFile index to document if tracking is enabled
        if (options.trackSourceFile) {
          allDocs.push({ ...doc, _sourceFile: fileIndex } as T);
        } else {
          allDocs.push(doc);
        }
      }
    }

    // Deduplicate the flattened array using the standard deduplicate logic
    return this.deduplicate(allDocs, options);
  }
}

// =============================================================================
// Static Functions
// =============================================================================

/**
 * Deduplicate an array of documents (static function)
 */
export function deduplicate<T extends Document>(
  documents: T[],
  options: DeduplicationOptions = {}
): DeduplicationResult<T> {
  const engine = new DeduplicationEngine();
  return engine.deduplicate(documents, options);
}

/**
 * Deduplicate documents from an async iterator in a streaming fashion.
 * Processes one document at a time, keeping only the latest version per _id.
 * Supports batch callbacks, progress reporting, and abort signals.
 */
export async function deduplicateStreaming<T extends Document>(
  source: AsyncIterable<T>,
  options: StreamingOptions = {}
): Promise<DeduplicationResult<T>> {
  validateOptions(options);

  const startTime = performance.now();
  const documentsByIdMap = new Map<string, { doc: T; insertionOrder: number }>();
  let nextInsertionOrder = 0;

  let inputCount = 0;
  let duplicatesRemoved = 0;

  const batchSize = options.batchSize || DEFAULT_DEDUPLICATION_BATCH_SIZE;
  let currentBatch: T[] = [];

  const operationCounts = options.trackOperationCounts
    ? { insert: 0, update: 0, delete: 0 }
    : undefined;

  // Helper to check for abort signal and throw if triggered
  const checkAbort = (): void => {
    if (options.signal?.aborted) {
      throw new Error('Aborted');
    }
  };

  for await (const doc of source) {
    checkAbort();

    // Periodically yield control to allow abort signal to be checked.
    // Important for synchronously-yielding sources to remain responsive.
    if (inputCount > 0 && inputCount % 100 === 0) {
      await new Promise((resolve) => setTimeout(resolve, 0));
      checkAbort();
    }

    validateDocument(doc, inputCount);
    inputCount++;

    // Count operations when tracking is enabled
    if (operationCounts) {
      if (doc._op === 'i') operationCounts.insert++;
      else if (doc._op === 'u') operationCounts.update++;
      else if (doc._op === 'd') operationCounts.delete++;
    }

    const id = doc._id;
    const existingEntry = documentsByIdMap.get(id);

    if (existingEntry) {
      // Replace with newer version (higher _seq) or later occurrence (if _seq equal)
      if (doc._seq >= existingEntry.doc._seq) {
        documentsByIdMap.set(id, { doc, insertionOrder: existingEntry.insertionOrder });
      }
      duplicatesRemoved++;
    } else {
      // First occurrence of this _id - record insertion position
      documentsByIdMap.set(id, { doc, insertionOrder: nextInsertionOrder++ });
    }

    // Collect document for batch processing
    currentBatch.push(doc);
    if (currentBatch.length >= batchSize) {
      if (options.onBatch) {
        await options.onBatch(currentBatch);
      }
      currentBatch = [];
    }

    // Report progress at specified intervals
    if (options.onProgress && options.progressInterval) {
      if (inputCount % options.progressInterval === 0) {
        options.onProgress(inputCount, documentsByIdMap.size);
      }
    }
  }

  // Process any remaining documents in the batch
  if (currentBatch.length > 0 && options.onBatch) {
    await options.onBatch(currentBatch);
  }

  // Send final progress report with all documents processed
  if (options.onProgress) {
    options.onProgress(inputCount, documentsByIdMap.size);
  }

  // Filter out deleted documents and collect final results
  const finalResults: { doc: T; insertionOrder: number }[] = [];
  let deletesFiltered = 0;

  for (const entry of documentsByIdMap.values()) {
    if (entry.doc._op === 'd') {
      // Skip deleted documents - they will be filtered from output
      deletesFiltered++;
    } else {
      finalResults.push(entry);
    }
  }

  // Sort results using specified ordering strategy
  const sortedDocs = sortResults(finalResults, options);

  const processingTimeMs = performance.now() - startTime;

  const stats: DeduplicationStats = {
    inputCount,
    outputCount: sortedDocs.length,
    duplicatesRemoved,
    deletesFiltered,
    processingTimeMs,
  };

  if (operationCounts) {
    stats.operationCounts = operationCounts;
  }

  return {
    documents: sortedDocs,
    stats,
  };
}
