/**
 * Projection utility for MongoDB-style field projection
 *
 * Handles both inclusion and exclusion projections:
 * - Inclusion: { field: 1 } - only include specified fields (plus _id by default)
 * - Exclusion: { field: 0 } - exclude specified fields
 * - _id is always included unless explicitly excluded with { _id: 0 }
 */

import type { Document, AnyDocument } from '@types';

/**
 * Apply a projection to a document
 *
 * @param doc - The document to project
 * @param projection - The projection specification (field: 0 | 1)
 * @returns The projected document
 *
 * Supports both strongly-typed Document and looser AnyDocument/Record types.
 *
 * @example
 * ```typescript
 * // Inclusion - only include name and email, _id is auto-included
 * applyProjection({ _id: '1', name: 'Alice', email: 'a@b.com', age: 30 }, { name: 1, email: 1 })
 * // => { _id: '1', name: 'Alice', email: 'a@b.com' }
 *
 * // Exclusion - exclude password field
 * applyProjection({ _id: '1', name: 'Alice', password: 'secret' }, { password: 0 })
 * // => { _id: '1', name: 'Alice' }
 *
 * // Exclude _id explicitly
 * applyProjection({ _id: '1', name: 'Alice' }, { name: 1, _id: 0 })
 * // => { name: 'Alice' }
 * ```
 */
export function applyProjection<T extends Document | AnyDocument | Record<string, unknown>>(
  doc: T,
  projection: Record<string, 0 | 1>
): Partial<T> {
  const hasInclusions = Object.values(projection).some((v) => v === 1);

  if (hasInclusions) {
    // Inclusion mode: only include specified fields
    // Always include _id unless explicitly excluded
    const result: Record<string, unknown> = {};

    // Include _id by default unless explicitly excluded
    if (projection._id !== 0) {
      result._id = doc._id;
    }

    for (const [key, include] of Object.entries(projection)) {
      if (include === 1 && key !== '_id' && key in doc) {
        result[key] = doc[key];
      }
    }

    return result as Partial<T>;
  } else {
    // Exclusion mode: exclude specified fields
    const result = { ...doc };

    for (const [key, exclude] of Object.entries(projection)) {
      if (exclude === 0) {
        delete result[key];
      }
    }

    return result as Partial<T>;
  }
}
