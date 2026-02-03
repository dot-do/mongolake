/**
 * Shared update operator utilities
 *
 * Handles MongoDB update operators: $set, $unset, $inc, $push, $pull, $addToSet, $pop, $rename
 */

import type { Document, Update, DocumentFields, FilterQuery } from '@types';
import { MAX_NESTED_PATH_DEPTH } from '@mongolake/constants.js';
import { ValidationError } from '../validation/index.js';

/**
 * Dangerous property names that could lead to prototype pollution.
 * These are rejected to prevent security vulnerabilities.
 */
const DANGEROUS_KEYS = new Set(['__proto__', 'constructor', 'prototype']);

/**
 * Reserved field names that cannot be modified via update operators.
 * These fields are system-managed and their modification would break data integrity.
 */
const RESERVED_FIELDS = new Set(['_id', '_seq', '_op']);

/**
 * Validates that a property name is safe and does not allow prototype pollution.
 * Rejects dangerous keys like __proto__, constructor, prototype, and any key
 * starting with double underscores.
 *
 * @param key - The property name to validate
 * @throws {Error} When the key is a dangerous property name
 */
function validatePropertyName(key: string): void {
  if (DANGEROUS_KEYS.has(key)) {
    throw new Error(`Invalid property name: '${key}' is not allowed`);
  }
  if (key.startsWith('__')) {
    throw new Error(`Invalid property name: keys starting with '__' are not allowed`);
  }
}

/**
 * Validates that a path does not exceed the maximum nesting depth.
 * This prevents DoS attacks via deeply nested paths.
 *
 * @param path - Dot-notation path to validate
 * @throws {Error} When path depth exceeds MAX_NESTED_PATH_DEPTH
 */
function validatePathDepth(path: string): void {
  const parts = path.split('.');
  if (parts.length > MAX_NESTED_PATH_DEPTH) {
    throw new Error(`Path exceeds maximum nesting depth of ${MAX_NESTED_PATH_DEPTH} levels`);
  }
}

/**
 * Validates all segments of a path for dangerous property names.
 * This prevents prototype pollution attacks via nested paths.
 *
 * @param path - Dot-notation path to validate
 * @throws {Error} When any path segment is a dangerous property name
 */
function validatePathSegments(path: string): void {
  const parts = path.split('.');
  for (const part of parts) {
    validatePropertyName(part);
  }
}

/**
 * Validates that a field path does not target a reserved field.
 * Reserved fields (_id, _seq, _op) are system-managed and cannot be modified.
 *
 * @param path - The field path to validate (may include dot notation)
 * @throws {ValidationError} When the path targets a reserved field
 */
function validateNotReservedField(path: string): void {
  // Extract the top-level field name (first segment before any dot)
  const topLevelField = path.split('.')[0]!;
  if (RESERVED_FIELDS.has(topLevelField)) {
    throw new ValidationError(
      `Cannot modify reserved field '${topLevelField}'`,
      'reserved_field',
      { invalidValue: path, context: { field: topLevelField } }
    );
  }
}

/** Update operators in a looser form for internal use */
export type LooseUpdate = {
  $set?: DocumentFields;
  $unset?: DocumentFields;
  $setOnInsert?: DocumentFields;
  $inc?: { [key: string]: number };
  $push?: DocumentFields;
  $pull?: DocumentFields;
  $addToSet?: DocumentFields;
  $pop?: { [key: string]: 1 | -1 };
  $rename?: { [key: string]: string };
};

/**
 * Generic update type that works with any document type.
 * This is a union of the strongly-typed Update<T> and the looser LooseUpdate.
 */
export type UpdateInput<T extends Document> = Update<T> | LooseUpdate;

/**
 * Set a value at a nested path using dot notation.
 * Creates intermediate objects as needed.
 *
 * @param obj - The object to modify
 * @param path - Dot-notation path (e.g., 'profile.level' or 'a.b.c')
 * @param value - The value to set
 * @throws {Error} When path depth exceeds MAX_NESTED_PATH_DEPTH
 */
function setNestedValue(obj: DocumentFields, path: string, value: unknown): void {
  validatePathDepth(path);
  validatePathSegments(path);
  const parts = path.split('.');

  // If no dots, set directly
  if (parts.length === 1) {
    obj[path] = value;
    return;
  }

  // Traverse/create nested objects
  let current: DocumentFields = obj;
  for (let i = 0; i < parts.length - 1; i++) {
    const key = parts[i]!;
    if (current[key] === undefined || current[key] === null || typeof current[key] !== 'object') {
      current[key] = {};
    }
    current = current[key] as DocumentFields;
  }

  // Set the final value
  current[parts[parts.length - 1]!] = value;
}

/**
 * Delete a value at a nested path using dot notation.
 *
 * @param obj - The object to modify
 * @param path - Dot-notation path (e.g., 'profile.level' or 'a.b.c')
 * @throws {Error} When path depth exceeds MAX_NESTED_PATH_DEPTH
 */
function deleteNestedValue(obj: DocumentFields, path: string): void {
  validatePathDepth(path);
  validatePathSegments(path);
  const parts = path.split('.');

  // If no dots, delete directly
  if (parts.length === 1) {
    delete obj[path];
    return;
  }

  // Traverse to the parent object
  let current: DocumentFields = obj;
  for (let i = 0; i < parts.length - 1; i++) {
    const key = parts[i]!;
    if (current[key] === undefined || current[key] === null || typeof current[key] !== 'object') {
      // Path doesn't exist, nothing to delete
      return;
    }
    current = current[key] as DocumentFields;
  }

  // Delete the final key
  delete current[parts[parts.length - 1]!];
}

/**
 * Get a value at a nested path using dot notation.
 *
 * @param obj - The object to read from
 * @param path - Dot-notation path (e.g., 'profile.level' or 'a.b.c')
 * @returns The value at the path, or undefined if not found
 * @throws {Error} When path depth exceeds MAX_NESTED_PATH_DEPTH
 */
function getNestedValue(obj: DocumentFields, path: string): unknown {
  validatePathDepth(path);
  validatePathSegments(path);
  const parts = path.split('.');

  let current: unknown = obj;
  for (const key of parts) {
    if (current === undefined || current === null || typeof current !== 'object') {
      return undefined;
    }
    current = (current as DocumentFields)[key];
  }

  return current;
}

/**
 * Apply MongoDB update operators to a document
 *
 * @param doc - The document to update
 * @param update - The update operators to apply
 * @returns A new document with updates applied
 *
 * Supports both strongly-typed Document and looser AnyDocument types.
 * When using Document type, the update parameter is type-checked against the document schema.
 * When using AnyDocument or Record<string, unknown>, LooseUpdate typing is used.
 */
export function applyUpdate<T extends Document>(
  doc: T,
  update: UpdateInput<T>
): T {
  const result = { ...doc } as DocumentFields;

  // $set - Set field values (supports dot notation for nested fields)
  if (update.$set) {
    for (const [key, value] of Object.entries(update.$set)) {
      validateNotReservedField(key);
      setNestedValue(result, key, value);
    }
  }

  // $unset - Remove fields (supports dot notation for nested fields)
  if (update.$unset) {
    for (const key of Object.keys(update.$unset)) {
      validateNotReservedField(key);
      deleteNestedValue(result, key);
    }
  }

  // $inc - Increment numeric fields (supports dot notation for nested fields)
  // MongoDB behavior: $inc requires numeric values. If the field exists but is not
  // a number (e.g., string "5"), MongoDB throws an error. If the field doesn't exist,
  // it is initialized to the increment amount.
  if (update.$inc) {
    for (const [key, amount] of Object.entries(update.$inc)) {
      validateNotReservedField(key);
      // Validate amount is a number (runtime check for type safety)
      if (typeof amount !== 'number') {
        throw new TypeError(
          `Cannot apply $inc with non-numeric amount. Field '${key}' received type ${typeof amount}`
        );
      }

      const current = getNestedValue(result, key);

      // If field exists, validate it's actually a number
      if (current !== undefined && current !== null) {
        if (typeof current !== 'number') {
          throw new TypeError(
            `Cannot apply $inc to a value of non-numeric type. Field '${key}' has type ${typeof current}`
          );
        }
        setNestedValue(result, key, current + amount);
      } else {
        // Field doesn't exist or is null - initialize to the increment amount
        setNestedValue(result, key, amount);
      }
    }
  }

  // $push - Add elements to arrays
  // MongoDB behavior: If field doesn't exist, creates an empty array then pushes.
  // If field exists but is not an array (including null), throws an error.
  if (update.$push) {
    for (const [key, value] of Object.entries(update.$push)) {
      validateNotReservedField(key);
      const current = result[key];

      // Handle undefined/missing - initialize as empty array
      // Handle null - throw TypeError (null is an explicit value, not missing)
      let arr: unknown[];
      if (current === undefined) {
        arr = [];
      } else if (current === null) {
        throw new TypeError(
          `Cannot apply $push to field '${key}': expected array, got null`
        );
      } else if (!Array.isArray(current)) {
        throw new TypeError(
          `Cannot apply $push to field '${key}': expected array, got ${typeof current}`
        );
      } else {
        arr = [...current]; // Clone to avoid mutation
      }

      if (typeof value === 'object' && value !== null && '$each' in value) {
        const eachValue = (value as { $each: unknown }).$each;
        if (!Array.isArray(eachValue)) {
          throw new TypeError(
            `$push $each requires an array, got ${typeof eachValue}`
          );
        }
        arr.push(...eachValue);
      } else {
        arr.push(value);
      }
      result[key] = arr;
    }
  }

  // $pull - Remove elements from arrays
  // MongoDB behavior: If field doesn't exist, does nothing.
  // If field exists but is not an array (including null), throws an error.
  if (update.$pull) {
    for (const [key, value] of Object.entries(update.$pull)) {
      validateNotReservedField(key);
      const current = result[key];

      // Handle undefined/missing - nothing to pull from, skip
      if (current === undefined) {
        continue;
      }

      // Handle null - throw TypeError (null is an explicit value, not missing)
      if (current === null) {
        throw new TypeError(
          `Cannot apply $pull to field '${key}': expected array, got null`
        );
      }

      // Type check: must be an array
      if (!Array.isArray(current)) {
        throw new TypeError(
          `Cannot apply $pull to field '${key}': expected array, got ${typeof current}`
        );
      }

      result[key] = current.filter((item) => item !== value);
    }
  }

  // $addToSet - Add unique elements to arrays
  // MongoDB behavior: If field doesn't exist, creates an empty array then adds.
  // If field exists but is not an array (including null), throws an error.
  if (update.$addToSet) {
    for (const [key, value] of Object.entries(update.$addToSet)) {
      validateNotReservedField(key);
      const current = result[key];

      // Handle undefined/missing - initialize as empty array
      // Handle null - throw TypeError (null is an explicit value, not missing)
      let arr: unknown[];
      if (current === undefined) {
        arr = [];
      } else if (current === null) {
        throw new TypeError(
          `Cannot apply $addToSet to field '${key}': expected array, got null`
        );
      } else if (!Array.isArray(current)) {
        throw new TypeError(
          `Cannot apply $addToSet to field '${key}': expected array, got ${typeof current}`
        );
      } else {
        arr = [...current]; // Clone to avoid mutation
      }

      let values: unknown[];
      if (typeof value === 'object' && value !== null && '$each' in value) {
        const eachValue = (value as { $each: unknown }).$each;
        if (!Array.isArray(eachValue)) {
          throw new TypeError(
            `$addToSet $each requires an array, got ${typeof eachValue}`
          );
        }
        values = eachValue;
      } else {
        values = [value];
      }

      for (const v of values) {
        if (!arr.includes(v)) {
          arr.push(v);
        }
      }
      result[key] = arr;
    }
  }

  // $pop - Remove first or last element from arrays
  // MongoDB behavior: If field doesn't exist, does nothing.
  // If field exists but is not an array (including null), throws an error.
  if (update.$pop) {
    for (const [key, direction] of Object.entries(update.$pop)) {
      validateNotReservedField(key);
      const arr = result[key];

      // Handle undefined/missing - nothing to pop from, skip
      if (arr === undefined) {
        continue;
      }

      // Handle null - throw TypeError (null is an explicit value, not missing)
      if (arr === null) {
        throw new TypeError(
          `Cannot apply $pop to field '${key}': expected array, got null`
        );
      }

      // Type check: must be an array
      if (!Array.isArray(arr)) {
        throw new TypeError(
          `Cannot apply $pop to field '${key}': expected array, got ${typeof arr}`
        );
      }

      if (arr.length > 0) {
        const newArr = [...arr]; // Clone to avoid mutation
        if (direction === -1) {
          newArr.shift(); // Remove first element
        } else {
          newArr.pop(); // Remove last element
        }
        result[key] = newArr;
      }
    }
  }

  // $rename - Rename fields
  if (update.$rename) {
    for (const [oldKey, newKey] of Object.entries(update.$rename)) {
      validateNotReservedField(oldKey);
      if (typeof newKey === 'string') {
        validateNotReservedField(newKey);
      }
      if (oldKey in result && typeof newKey === 'string') {
        result[newKey] = result[oldKey];
        delete result[oldKey];
      }
    }
  }

  return result as T;
}

/**
 * Extract equality field values from a MongoDB filter.
 *
 * For upsert operations, MongoDB creates a new document using
 * equality fields from the filter plus the update operations.
 *
 * This extracts top-level equality conditions:
 * - Direct value matches: { name: 'Alice' } -> { name: 'Alice' }
 * - $eq operator: { name: { $eq: 'Alice' } } -> { name: 'Alice' }
 *
 * Ignores:
 * - Comparison operators ($gt, $lt, $in, etc.)
 * - Logical operators ($and, $or, $nor, $not)
 * - Nested dot notation paths (handled separately in MongoDB)
 *
 * @param filter - The MongoDB filter document
 * @returns Object with extracted equality fields
 */
export function extractFilterFields(filter: FilterQuery): DocumentFields {
  const result: DocumentFields = {};

  for (const [key, value] of Object.entries(filter)) {
    // Skip logical operators
    if (key.startsWith('$')) {
      continue;
    }

    // Skip dot notation paths (nested fields)
    if (key.includes('.')) {
      continue;
    }

    // Handle direct value (equality match)
    if (value === null || typeof value !== 'object') {
      result[key] = value;
      continue;
    }

    // Handle $eq operator explicitly
    if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
      const obj = value as DocumentFields;
      if ('$eq' in obj) {
        result[key] = obj.$eq;
        continue;
      }

      // Skip fields with any query operators ($gt, $lt, $in, etc.)
      const hasOperator = Object.keys(obj).some((k) => k.startsWith('$'));
      if (hasOperator) {
        continue;
      }

      // Direct object value (equality match on embedded document)
      result[key] = value;
    }

    // Handle arrays (direct equality match)
    if (Array.isArray(value)) {
      result[key] = value;
    }
  }

  return result;
}

/**
 * Create a new document for upsert operations by combining filter fields with update operators.
 *
 * This is the MongoDB behavior for upsert: equality fields from the filter are used
 * as the initial document values, then update operators are applied on top.
 *
 * $setOnInsert is handled specially here: it is only applied during upsert inserts,
 * not during regular updates. The values from $setOnInsert are applied first (before $set),
 * so $set can override them if needed.
 *
 * @param filter - The MongoDB filter (equality fields will be extracted)
 * @param update - The update operators to apply
 * @returns A new document ready for insertion
 *
 * @example
 * ```typescript
 * // filter: { name: 'Alice', status: 'active' }
 * // update: { $set: { age: 30 }, $setOnInsert: { createdAt: new Date() } }
 * // result: { name: 'Alice', status: 'active', age: 30, createdAt: <date> }
 * const doc = createUpsertDocument(filter, update);
 * ```
 */
export function createUpsertDocument<T extends Document>(
  filter: FilterQuery,
  update: UpdateInput<T>
): T {
  const filterFields = extractFilterFields(filter);

  // First, apply $setOnInsert fields (only during upsert insert, not regular updates)
  // These are applied before other update operators so $set can override if needed
  let baseDoc = filterFields as DocumentFields;
  if (update.$setOnInsert) {
    for (const [key, value] of Object.entries(update.$setOnInsert)) {
      validateNotReservedField(key);
      setNestedValue(baseDoc, key, value);
    }
  }

  // Then apply the rest of the update operators
  return applyUpdate(baseDoc as T, update);
}

// Re-export LooseUpdate for external use
export type { LooseUpdate as UpdateOperators };
