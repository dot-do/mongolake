/**
 * Shared sort utilities for MongoLake
 *
 * Provides MongoDB-compatible document sorting functionality
 */

import type { Document, AnyDocument } from '@types';
import { getNestedValue } from './nested.js';

/** MongoDB sort specification */
export type Sort = { [key: string]: 1 | -1 };

/**
 * Sort documents according to MongoDB sort specification
 *
 * @param docs - Array of documents to sort
 * @param sort - MongoDB sort specification (field: 1 for ascending, -1 for descending)
 * @returns New sorted array (does not mutate original)
 *
 * Supports both strongly-typed Document arrays and looser AnyDocument/Record types.
 *
 * @example
 * // Sort by name ascending
 * sortDocuments(docs, { name: 1 })
 *
 * @example
 * // Sort by age descending, then name ascending
 * sortDocuments(docs, { age: -1, name: 1 })
 *
 * @example
 * // Sort by nested field
 * sortDocuments(docs, { 'user.profile.age': -1 })
 */
export function sortDocuments<T extends Document | AnyDocument | Record<string, unknown>>(
  docs: T[],
  sort: Sort
): T[] {
  return [...docs].sort((a, b) => {
    for (const [key, direction] of Object.entries(sort)) {
      const aVal = getNestedValue(a, key);
      const bVal = getNestedValue(b, key);

      // Compare values - works for strings, numbers, dates, etc.
      if ((aVal as number) < (bVal as number)) return -direction;
      if ((aVal as number) > (bVal as number)) return direction;
    }
    return 0;
  });
}
