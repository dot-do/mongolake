/**
 * Filtering Utilities for MongoLake Sync
 *
 * Handles include/exclude patterns and collection filtering.
 */

import { formatBytes } from '../utils.js';
import type { FileState } from './types.js';

// ============================================================================
// Pattern Parsing
// ============================================================================

export function parseIncludePatterns(patterns: string): string[] {
  return patterns
    .split(',')
    .map((p) => p.trim())
    .filter(Boolean);
}

export function parseExcludePatterns(patterns: string): string[] {
  return patterns
    .split(',')
    .map((p) => p.trim())
    .filter(Boolean);
}

export function parseCollectionsFilter(collectionsStr: string): string[] {
  return collectionsStr
    .split(',')
    .map((c) => c.trim())
    .filter(Boolean);
}

export function parseExcludeCollections(collectionsStr: string): string[] {
  return collectionsStr
    .split(',')
    .map((c) => c.trim())
    .filter(Boolean);
}

// ============================================================================
// Glob Pattern Matching
// ============================================================================

export function matchGlobPattern(filePath: string, pattern: string): boolean {
  // Convert glob pattern to regex
  let regexPattern = pattern
    .replace(/\*\*/g, '<<<DOUBLESTAR>>>')
    .replace(/\*/g, '[^/]*')
    .replace(/<<<DOUBLESTAR>>>/g, '.*')
    .replace(/\?/g, '.');

  // If pattern doesn't start with *, match from any position
  if (!pattern.startsWith('*')) {
    regexPattern = '(^|/)' + regexPattern;
  }

  const regex = new RegExp(regexPattern + '$');
  return regex.test(filePath);
}

export function matchCollectionPattern(collection: string, pattern: string): boolean {
  const regexPattern = pattern.replace(/\*/g, '.*').replace(/\?/g, '.');
  const regex = new RegExp(`^${regexPattern}$`);
  return regex.test(collection);
}

// ============================================================================
// File Filtering
// ============================================================================

export function applyIncludeFilter<T extends { path: string }>(
  files: T[],
  patterns: string[]
): T[] {
  return files.filter((file) => {
    for (const pattern of patterns) {
      if (matchGlobPattern(file.path, pattern)) {
        return true;
      }
    }
    return false;
  });
}

export function applyExcludeFilter<T extends { path: string }>(
  files: T[],
  patterns: string[]
): T[] {
  return files.filter((file) => {
    for (const pattern of patterns) {
      // Check if the basename matches
      const basename = file.path.split('/').pop() || '';
      if (matchGlobPattern(basename, pattern) || matchGlobPattern(file.path, pattern)) {
        return false;
      }
    }
    return true;
  });
}

export function applyFilters<T extends { path: string }>(
  files: T[],
  filters: { include?: string[]; exclude?: string[] }
): T[] {
  let result = files;

  if (filters.include && filters.include.length > 0) {
    result = applyIncludeFilter(result, filters.include);
  }

  if (filters.exclude && filters.exclude.length > 0) {
    result = applyExcludeFilter(result, filters.exclude);
  }

  return result;
}

// ============================================================================
// Collection Filtering
// ============================================================================

export function filterFilesByCollections(
  files: FileState[],
  database: string,
  collections: string[]
): FileState[] {
  return files.filter((file) => {
    for (const collection of collections) {
      if (file.path.startsWith(`${database}/${collection}/`)) {
        return true;
      }
    }
    return false;
  });
}

export function applyCollectionFilters(
  collections: string[],
  filters: { include: string[] | null; exclude: string[] }
): string[] {
  let result = collections;

  if (filters.include && filters.include.length > 0) {
    result = result.filter((c) => {
      for (const pattern of filters.include!) {
        if (matchCollectionPattern(c, pattern)) {
          return true;
        }
      }
      return false;
    });
  }

  if (filters.exclude && filters.exclude.length > 0) {
    result = result.filter((c) => {
      for (const pattern of filters.exclude) {
        if (matchCollectionPattern(c, pattern)) {
          return false;
        }
      }
      return true;
    });
  }

  return result;
}

// ============================================================================
// Collection Validation
// ============================================================================

export function validateCollectionNames(
  collections: string[]
): { valid: boolean; errors: string[] } {
  const errors: string[] = [];
  const validPattern = /^[a-zA-Z][a-zA-Z0-9_]*$/;

  for (const collection of collections) {
    if (!collection) {
      errors.push('Collection name cannot be empty');
    } else if (collection.startsWith('$')) {
      errors.push(`Collection name cannot start with $: ${collection}`);
    } else if (collection.includes(' ')) {
      errors.push(`Collection name cannot contain spaces: ${collection}`);
    } else if (!validPattern.test(collection)) {
      // Still allow it but warn
    }
  }

  return { valid: errors.length === 0, errors };
}

// ============================================================================
// Summary Generation
// ============================================================================

export function generateSyncSummary(options: {
  database: string;
  collections: string[];
  direction: 'push' | 'pull';
  filesCount: number;
  totalSize: number;
}): string {
  return `
Sync Summary:
  Database:    ${options.database}
  Direction:   ${options.direction}
  Collections: ${options.collections.join(', ')}
  Files:       ${options.filesCount}
  Total Size:  ${formatBytes(options.totalSize)}
`;
}
