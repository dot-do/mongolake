/**
 * Collection operations for the Compact command
 *
 * @module cli/compact/collections
 */

import { FileSystemStorage } from '../../storage/index.js';
import type {
  ValidationResult,
  CollectionStats,
  CollectionCompactOptions,
  CollectionCompactResult,
  CollectionsCompactOptions,
  CollectionsCompactResult,
  CompactAllOptions,
  CompactAllResult,
  CompactDatabaseOptions,
  CompactDatabaseResult,
} from './types.js';

// Forward declaration - will be imported from index to avoid circular dependency
let runCompactFn: typeof import('./index.js').runCompact;

/**
 * Set the runCompact function to avoid circular dependency
 * @internal
 */
export function setRunCompactFn(fn: typeof import('./index.js').runCompact): void {
  runCompactFn = fn;
}

// ============================================================================
// Collection Validation and Discovery
// ============================================================================

/**
 * Validate that a collection exists
 */
export async function validateCollection(
  database: string,
  collection: string,
  path: string
): Promise<ValidationResult> {
  const storage = new FileSystemStorage(path);
  const collectionPrefix = `${database}/${collection}`;
  const allFiles = await storage.list(database);

  const hasFiles = allFiles.some(
    (f) => f.startsWith(collectionPrefix) && f.endsWith('.parquet')
  );

  if (!hasFiles) {
    return {
      exists: false,
      error: `Collection '${collection}' not found in database '${database}'`,
    };
  }

  return { exists: true };
}

/**
 * Resolve a collection pattern to actual collection names
 */
export async function resolveCollectionPattern(
  database: string,
  pattern: string,
  path: string
): Promise<string[]> {
  const collections = await listCollections(database, path);

  if (pattern === '*') {
    return collections;
  }

  // Convert glob pattern to regex
  const regex = new RegExp(
    '^' + pattern.replace(/\*/g, '.*').replace(/\?/g, '.') + '$'
  );

  return collections.filter((c) => regex.test(c));
}

/**
 * List all collections in a database
 */
export async function listCollections(database: string, path: string): Promise<string[]> {
  const storage = new FileSystemStorage(path);
  const allFiles = await storage.list(database);

  const collections = new Set<string>();

  for (const file of allFiles) {
    if (file.endsWith('.parquet') && !file.includes('/_')) {
      // Extract collection name from path: database/collection/file.parquet
      const parts = file.split('/');
      if (parts.length >= 2) {
        collections.add(parts[1]!);
      }
    }
  }

  return Array.from(collections);
}

/**
 * Get statistics for a collection
 */
export async function getCollectionStats(
  database: string,
  collection: string,
  path: string
): Promise<CollectionStats> {
  const storage = new FileSystemStorage(path);
  const collectionPrefix = `${database}/${collection}`;
  const allFiles = await storage.list(database);

  const parquetFiles = allFiles.filter(
    (f) => f.startsWith(collectionPrefix) && f.endsWith('.parquet') && !f.includes('/_')
  );

  let totalSize = 0;
  let smallBlockCount = 0;
  const MIN_BLOCK_SIZE = 2 * 1024 * 1024;

  for (const file of parquetFiles) {
    const meta = await storage.head(file);
    if (meta) {
      totalSize += meta.size;
      if (meta.size < MIN_BLOCK_SIZE) {
        smallBlockCount++;
      }
    }
  }

  return {
    blockCount: parquetFiles.length,
    totalSize,
    smallBlockCount,
    needsCompaction: smallBlockCount >= 2,
  };
}

// ============================================================================
// Single Collection Compaction
// ============================================================================

/**
 * Compact a specific collection
 */
export async function compactCollection(
  options: CollectionCompactOptions
): Promise<CollectionCompactResult> {
  const stats = await getCollectionStats(options.database, options.collection, options.path);

  if (!stats.needsCompaction) {
    return {
      database: options.database,
      collection: options.collection,
      processedBlocks: 0,
      skipped: true,
      reason: 'no small blocks need compaction',
    };
  }

  const result = await runCompactFn({
    ...options,
    dryRun: false,
    verbose: false,
  });

  // If compaction failed, throw an error so callers can handle it
  if (!result.success && result.error) {
    throw new Error(result.error.message);
  }

  return {
    database: options.database,
    collection: options.collection,
    processedBlocks: result.processedBlocks,
    skipped: result.skipped,
    reason: result.reason,
  };
}

// ============================================================================
// Multi-Collection Compaction
// ============================================================================

/**
 * Compact multiple collections matching a pattern
 */
export async function compactCollections(
  options: CollectionsCompactOptions
): Promise<CollectionsCompactResult> {
  const collections = await resolveCollectionPattern(
    options.database,
    options.pattern,
    options.path
  );

  const excluded = options.exclude || [];
  const toCompact = collections.filter((c) => !excluded.includes(c));

  const result: CollectionsCompactResult = {
    collectionsCompacted: [],
    perCollection: {},
    errors: [],
  };

  for (const collection of toCompact) {
    try {
      const collectionResult = await compactCollection({
        database: options.database,
        collection,
        path: options.path,
      });

      if (!collectionResult.skipped) {
        result.collectionsCompacted.push(collection);
      }

      result.perCollection[collection] = {
        processedBlocks: collectionResult.processedBlocks,
        stats: {
          bytesProcessed: 0,
          rowsProcessed: 0,
          compressionRatio: 0,
        },
      };
    } catch (error) {
      if (options.continueOnError) {
        result.errors.push({
          collection,
          error: (error as Error).message,
        });
      } else {
        throw error;
      }
    }
  }

  return result;
}

/**
 * Compact all collections in a database
 */
export async function compactAllCollections(
  options: CompactAllOptions
): Promise<CompactAllResult> {
  const result = await compactCollections({
    ...options,
    pattern: '*',
  });

  return {
    collectionsProcessed: Object.keys(result.perCollection).length,
    collectionsCompacted: result.collectionsCompacted,
    perCollection: result.perCollection,
    errors: result.errors,
    totalStats: {
      bytesProcessed: 0,
      rowsProcessed: 0,
      compressionRatio: 0,
    },
  };
}

/**
 * Compact an entire database
 */
export async function compactDatabase(
  options: CompactDatabaseOptions
): Promise<CompactDatabaseResult> {
  return compactAllCollections(options);
}
