/**
 * Custom Assertion Helpers
 *
 * Custom assertion functions for MongoLake tests.
 * Provides domain-specific assertions for documents, filters, and operations.
 */

import { expect } from 'vitest';
import { ObjectId, type Document } from '../../src/types.js';

// ============================================================================
// Document Assertions
// ============================================================================

/**
 * Assert that a value is a valid ObjectId hex string.
 */
export function assertValidObjectId(value: unknown, message?: string): asserts value is string {
  expect(typeof value, message ?? 'Expected value to be a string').toBe('string');
  expect(
    /^[0-9a-fA-F]{24}$/.test(value as string),
    message ?? `Expected valid ObjectId hex string, got: ${value}`
  ).toBe(true);
}

/**
 * Assert that a value is a valid ObjectId instance or hex string.
 */
export function assertObjectId(
  value: unknown,
  message?: string
): asserts value is ObjectId | string {
  if (value instanceof ObjectId) {
    return;
  }
  assertValidObjectId(value, message);
}

/**
 * Assert that a document has the expected _id.
 */
export function assertDocumentId(
  doc: Document | null | undefined,
  expectedId: string | ObjectId,
  message?: string
): void {
  expect(doc, message ?? 'Expected document to exist').toBeDefined();
  expect(doc).not.toBeNull();
  const actualId = doc!._id instanceof ObjectId ? doc!._id.toString() : doc!._id;
  const expected = expectedId instanceof ObjectId ? expectedId.toString() : expectedId;
  expect(actualId, message ?? `Expected document _id to be ${expected}`).toBe(expected);
}

/**
 * Assert that a document has all expected fields with correct values.
 */
export function assertDocumentFields(
  doc: Document | null | undefined,
  expectedFields: Record<string, unknown>,
  message?: string
): void {
  expect(doc, message ?? 'Expected document to exist').toBeDefined();
  expect(doc).not.toBeNull();

  for (const [key, expectedValue] of Object.entries(expectedFields)) {
    const actualValue = (doc as Record<string, unknown>)[key];
    expect(
      actualValue,
      message ?? `Expected field '${key}' to be ${JSON.stringify(expectedValue)}, got ${JSON.stringify(actualValue)}`
    ).toEqual(expectedValue);
  }
}

/**
 * Assert that a document does not have specific fields.
 */
export function assertDocumentExcludesFields(
  doc: Document | null | undefined,
  excludedFields: string[],
  message?: string
): void {
  expect(doc, message ?? 'Expected document to exist').toBeDefined();
  expect(doc).not.toBeNull();

  for (const field of excludedFields) {
    expect(
      (doc as Record<string, unknown>)[field],
      message ?? `Expected field '${field}' to be undefined`
    ).toBeUndefined();
  }
}

/**
 * Assert that a document was created recently (within tolerance).
 */
export function assertRecentlyCreated(
  doc: { createdAt?: Date } | null | undefined,
  toleranceMs: number = 5000,
  message?: string
): void {
  expect(doc, message ?? 'Expected document to exist').toBeDefined();
  expect(doc!.createdAt, message ?? 'Expected document to have createdAt field').toBeDefined();

  const now = Date.now();
  const createdAt = doc!.createdAt!.getTime();
  const diff = now - createdAt;

  expect(
    diff >= 0 && diff <= toleranceMs,
    message ?? `Expected createdAt to be within ${toleranceMs}ms of now, diff was ${diff}ms`
  ).toBe(true);
}

// ============================================================================
// Array/Collection Assertions
// ============================================================================

/**
 * Assert that an array of documents contains a document with the specified _id.
 */
export function assertContainsDocumentWithId(
  docs: Document[],
  id: string | ObjectId,
  message?: string
): void {
  const idStr = id instanceof ObjectId ? id.toString() : id;
  const found = docs.some((doc) => {
    const docId = doc._id instanceof ObjectId ? doc._id.toString() : doc._id;
    return docId === idStr;
  });
  expect(found, message ?? `Expected documents to contain document with _id: ${idStr}`).toBe(true);
}

/**
 * Assert that an array of documents does not contain a document with the specified _id.
 */
export function assertNotContainsDocumentWithId(
  docs: Document[],
  id: string | ObjectId,
  message?: string
): void {
  const idStr = id instanceof ObjectId ? id.toString() : id;
  const found = docs.some((doc) => {
    const docId = doc._id instanceof ObjectId ? doc._id.toString() : doc._id;
    return docId === idStr;
  });
  expect(found, message ?? `Expected documents not to contain document with _id: ${idStr}`).toBe(
    false
  );
}

/**
 * Assert that documents are sorted by a specific field.
 */
export function assertSortedBy(
  docs: Document[],
  field: string,
  direction: 'asc' | 'desc' = 'asc',
  message?: string
): void {
  if (docs.length <= 1) return;

  for (let i = 1; i < docs.length; i++) {
    const prev = getNestedValue(docs[i - 1], field);
    const curr = getNestedValue(docs[i], field);

    if (prev === undefined || curr === undefined) continue;

    const comparison = compareValues(prev, curr);
    const valid = direction === 'asc' ? comparison <= 0 : comparison >= 0;

    expect(
      valid,
      message ??
        `Expected documents to be sorted by '${field}' ${direction}, but index ${i - 1} (${prev}) and ${i} (${curr}) are out of order`
    ).toBe(true);
  }
}

/**
 * Assert that all documents match a filter predicate.
 */
export function assertAllMatch(
  docs: Document[],
  predicate: (doc: Document) => boolean,
  message?: string
): void {
  const nonMatching = docs.filter((doc) => !predicate(doc));
  expect(
    nonMatching.length,
    message ?? `Expected all documents to match predicate, but ${nonMatching.length} did not`
  ).toBe(0);
}

/**
 * Assert that some documents match a filter predicate.
 */
export function assertSomeMatch(
  docs: Document[],
  predicate: (doc: Document) => boolean,
  message?: string
): void {
  const matching = docs.filter(predicate);
  expect(
    matching.length > 0,
    message ?? 'Expected some documents to match predicate, but none did'
  ).toBe(true);
}

/**
 * Assert that no documents match a filter predicate.
 */
export function assertNoneMatch(
  docs: Document[],
  predicate: (doc: Document) => boolean,
  message?: string
): void {
  const matching = docs.filter(predicate);
  expect(
    matching.length,
    message ?? `Expected no documents to match predicate, but ${matching.length} did`
  ).toBe(0);
}

/**
 * Assert that all document IDs are unique.
 */
export function assertUniqueIds(docs: Document[], message?: string): void {
  const ids = docs.map((doc) => (doc._id instanceof ObjectId ? doc._id.toString() : doc._id));
  const uniqueIds = new Set(ids);
  expect(
    uniqueIds.size,
    message ?? `Expected all document IDs to be unique, found ${ids.length - uniqueIds.size} duplicates`
  ).toBe(ids.length);
}

// ============================================================================
// Operation Result Assertions
// ============================================================================

/**
 * Assert that an insert result is successful.
 */
export function assertInsertSuccess(
  result: { acknowledged?: boolean; insertedId?: string | ObjectId } | null | undefined,
  expectedId?: string | ObjectId,
  message?: string
): void {
  expect(result, message ?? 'Expected insert result to exist').toBeDefined();
  expect(result!.acknowledged, message ?? 'Expected insert to be acknowledged').toBe(true);
  expect(result!.insertedId, message ?? 'Expected insertedId to be defined').toBeDefined();

  if (expectedId !== undefined) {
    const actualId =
      result!.insertedId instanceof ObjectId ? result!.insertedId.toString() : result!.insertedId;
    const expected = expectedId instanceof ObjectId ? expectedId.toString() : expectedId;
    expect(actualId, message ?? `Expected insertedId to be ${expected}`).toBe(expected);
  }
}

/**
 * Assert that an update result is successful.
 */
export function assertUpdateSuccess(
  result: {
    acknowledged?: boolean;
    matchedCount?: number;
    modifiedCount?: number;
  } | null | undefined,
  expectedMatchedCount?: number,
  expectedModifiedCount?: number,
  message?: string
): void {
  expect(result, message ?? 'Expected update result to exist').toBeDefined();
  expect(result!.acknowledged, message ?? 'Expected update to be acknowledged').toBe(true);

  if (expectedMatchedCount !== undefined) {
    expect(result!.matchedCount, message ?? `Expected matchedCount to be ${expectedMatchedCount}`).toBe(
      expectedMatchedCount
    );
  }

  if (expectedModifiedCount !== undefined) {
    expect(
      result!.modifiedCount,
      message ?? `Expected modifiedCount to be ${expectedModifiedCount}`
    ).toBe(expectedModifiedCount);
  }
}

/**
 * Assert that a delete result is successful.
 */
export function assertDeleteSuccess(
  result: { acknowledged?: boolean; deletedCount?: number } | null | undefined,
  expectedDeletedCount?: number,
  message?: string
): void {
  expect(result, message ?? 'Expected delete result to exist').toBeDefined();
  expect(result!.acknowledged, message ?? 'Expected delete to be acknowledged').toBe(true);

  if (expectedDeletedCount !== undefined) {
    expect(
      result!.deletedCount,
      message ?? `Expected deletedCount to be ${expectedDeletedCount}`
    ).toBe(expectedDeletedCount);
  }
}

// ============================================================================
// Error Assertions
// ============================================================================

/**
 * Assert that an async function throws an error matching a pattern.
 */
export async function assertThrowsAsync(
  fn: () => Promise<unknown>,
  errorPattern: string | RegExp,
  message?: string
): Promise<void> {
  let threw = false;
  let actualError: unknown;

  try {
    await fn();
  } catch (error) {
    threw = true;
    actualError = error;
  }

  expect(threw, message ?? 'Expected function to throw').toBe(true);

  if (typeof errorPattern === 'string') {
    expect(
      String(actualError).includes(errorPattern),
      message ?? `Expected error to include '${errorPattern}', got: ${actualError}`
    ).toBe(true);
  } else {
    expect(
      errorPattern.test(String(actualError)),
      message ?? `Expected error to match ${errorPattern}, got: ${actualError}`
    ).toBe(true);
  }
}

/**
 * Assert that an async function does not throw.
 */
export async function assertNoThrowAsync(
  fn: () => Promise<unknown>,
  message?: string
): Promise<void> {
  let threw = false;
  let actualError: unknown;

  try {
    await fn();
  } catch (error) {
    threw = true;
    actualError = error;
  }

  expect(threw, message ?? `Expected function not to throw, but it threw: ${actualError}`).toBe(
    false
  );
}

// ============================================================================
// Timing Assertions
// ============================================================================

/**
 * Assert that an async operation completes within a time limit.
 */
export async function assertCompletesWithin<T>(
  fn: () => Promise<T>,
  maxMs: number,
  message?: string
): Promise<T> {
  const start = performance.now();
  const result = await fn();
  const elapsed = performance.now() - start;

  expect(
    elapsed <= maxMs,
    message ?? `Expected operation to complete within ${maxMs}ms, but took ${elapsed.toFixed(2)}ms`
  ).toBe(true);

  return result;
}

/**
 * Assert that an async operation takes at least a minimum time.
 */
export async function assertTakesAtLeast<T>(
  fn: () => Promise<T>,
  minMs: number,
  message?: string
): Promise<T> {
  const start = performance.now();
  const result = await fn();
  const elapsed = performance.now() - start;

  expect(
    elapsed >= minMs,
    message ?? `Expected operation to take at least ${minMs}ms, but took ${elapsed.toFixed(2)}ms`
  ).toBe(true);

  return result;
}

// ============================================================================
// Deduplication Assertions
// ============================================================================

/**
 * Assert deduplication result statistics.
 */
export function assertDeduplicationStats(
  result: {
    stats: {
      inputCount: number;
      outputCount: number;
      duplicatesRemoved: number;
      deletesFiltered?: number;
    };
  },
  expected: {
    inputCount?: number;
    outputCount?: number;
    duplicatesRemoved?: number;
    deletesFiltered?: number;
  },
  message?: string
): void {
  if (expected.inputCount !== undefined) {
    expect(
      result.stats.inputCount,
      message ?? `Expected inputCount to be ${expected.inputCount}`
    ).toBe(expected.inputCount);
  }

  if (expected.outputCount !== undefined) {
    expect(
      result.stats.outputCount,
      message ?? `Expected outputCount to be ${expected.outputCount}`
    ).toBe(expected.outputCount);
  }

  if (expected.duplicatesRemoved !== undefined) {
    expect(
      result.stats.duplicatesRemoved,
      message ?? `Expected duplicatesRemoved to be ${expected.duplicatesRemoved}`
    ).toBe(expected.duplicatesRemoved);
  }

  if (expected.deletesFiltered !== undefined) {
    expect(
      result.stats.deletesFiltered,
      message ?? `Expected deletesFiltered to be ${expected.deletesFiltered}`
    ).toBe(expected.deletesFiltered);
  }
}

// ============================================================================
// HTTP Response Assertions
// ============================================================================

/**
 * Assert HTTP response status.
 */
export function assertResponseStatus(
  response: Response,
  expectedStatus: number,
  message?: string
): void {
  expect(
    response.status,
    message ?? `Expected response status to be ${expectedStatus}, got ${response.status}`
  ).toBe(expectedStatus);
}

/**
 * Assert HTTP response is successful (2xx).
 */
export function assertResponseSuccess(response: Response, message?: string): void {
  expect(
    response.ok,
    message ?? `Expected successful response (2xx), got ${response.status}`
  ).toBe(true);
}

/**
 * Assert HTTP response has specific header.
 */
export function assertResponseHeader(
  response: Response,
  header: string,
  expectedValue?: string,
  message?: string
): void {
  const actualValue = response.headers.get(header);
  expect(actualValue, message ?? `Expected header '${header}' to be present`).not.toBeNull();

  if (expectedValue !== undefined) {
    expect(
      actualValue,
      message ?? `Expected header '${header}' to be '${expectedValue}', got '${actualValue}'`
    ).toBe(expectedValue);
  }
}

/**
 * Assert HTTP response body contains expected JSON.
 */
export async function assertJsonResponse<T>(
  response: Response,
  expected: Partial<T>,
  message?: string
): Promise<T> {
  const body = (await response.json()) as T;

  for (const [key, value] of Object.entries(expected)) {
    expect(
      (body as Record<string, unknown>)[key],
      message ?? `Expected response body '${key}' to be ${JSON.stringify(value)}`
    ).toEqual(value);
  }

  return body;
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Get a nested value from an object using dot notation.
 */
function getNestedValue(obj: unknown, path: string): unknown {
  const parts = path.split('.');
  let current: unknown = obj;

  for (const part of parts) {
    if (current === null || current === undefined) return undefined;
    current = (current as Record<string, unknown>)[part];
  }

  return current;
}

/**
 * Compare two values for sorting purposes.
 */
function compareValues(a: unknown, b: unknown): number {
  if (typeof a === 'number' && typeof b === 'number') {
    return a - b;
  }
  if (typeof a === 'string' && typeof b === 'string') {
    return a.localeCompare(b);
  }
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() - b.getTime();
  }
  return String(a).localeCompare(String(b));
}
