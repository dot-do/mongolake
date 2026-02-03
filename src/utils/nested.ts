/**
 * Utility for accessing nested values in objects using dot notation
 * @module utils/nested
 */

import { MAX_NESTED_PATH_DEPTH } from '@mongolake/constants.js';

/**
 * Error thrown when path traversal depth limit is exceeded
 */
export class PathDepthExceededError extends Error {
  constructor(depth: number, maxDepth: number) {
    super(
      `Path depth ${depth} exceeds maximum allowed depth of ${maxDepth}. ` +
        `This limit prevents DoS attacks via deeply nested paths.`
    );
    this.name = 'PathDepthExceededError';
  }
}

/**
 * Get a nested value from an object using dot notation
 * @param obj - The object to get the value from
 * @param path - Dot-notation path (e.g., "user.profile.name")
 * @param maxDepth - Maximum allowed path depth (default: MAX_NESTED_PATH_DEPTH = 32)
 * @returns The value at the path, or undefined if not found
 * @throws {PathDepthExceededError} When path depth exceeds maxDepth
 *
 * @example
 * ```typescript
 * const obj = { user: { profile: { name: 'Alice' } } };
 * getNestedValue(obj, 'user.profile.name'); // 'Alice'
 * getNestedValue(obj, 'user.email'); // undefined
 * ```
 */
export function getNestedValue(
  obj: unknown,
  path: string,
  maxDepth: number = MAX_NESTED_PATH_DEPTH
): unknown {
  if (obj === null || obj === undefined) {
    return undefined;
  }

  if (typeof obj !== 'object') {
    return undefined;
  }

  // Fast path for non-nested keys
  if (path.indexOf('.') === -1) {
    return (obj as Record<string, unknown>)[path];
  }

  const parts = path.split('.');

  // Check depth limit before traversal to prevent DoS attacks
  if (parts.length > maxDepth) {
    throw new PathDepthExceededError(parts.length, maxDepth);
  }

  let current: unknown = obj;

  for (const part of parts) {
    if (current === null || current === undefined) {
      return undefined;
    }
    if (typeof current !== 'object') {
      return undefined;
    }
    current = (current as Record<string, unknown>)[part];
  }

  return current;
}
