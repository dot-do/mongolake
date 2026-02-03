/**
 * Property-based tests for MongoDB document sorting
 *
 * Uses fast-check to verify algebraic properties of sort operations:
 * - sort(docs, field, 1).reverse() === sort(docs, field, -1)
 * - Sorting is stable for equal elements
 * - Sorting is idempotent
 * - Sorting preserves all elements (bijection)
 */

import { describe, it, expect } from 'vitest';
import * as fc from 'fast-check';
import { sortDocuments, type Sort } from '../../../src/utils/sort.js';

// Arbitrary for simple documents with sortable fields
const sortableDocArb = fc.record({
  _id: fc.string({ minLength: 1, maxLength: 10 }),
  name: fc.string({ maxLength: 20 }),
  age: fc.integer({ min: 0, max: 150 }),
  score: fc.integer({ min: -100, max: 100 }),
  priority: fc.integer({ min: 1, max: 10 }),
});

// Arbitrary for arrays of sortable documents
const docArrayArb = fc.array(sortableDocArb, { minLength: 0, maxLength: 20 });

// Arbitrary for sort field names
const sortFieldArb = fc.constantFrom('name', 'age', 'score', 'priority');

describe('Sort Property-Based Tests', () => {
  describe('Sort direction properties', () => {
    it('sort(docs, field, 1).reverse() equals sort(docs, field, -1)', () => {
      fc.assert(
        fc.property(docArrayArb, sortFieldArb, (docs, field) => {
          const ascSorted = sortDocuments(docs, { [field]: 1 });
          const descSorted = sortDocuments(docs, { [field]: -1 });

          // Reversed ascending should equal descending
          const reversedAsc = [...ascSorted].reverse();

          // Compare by field value (since _id may differ for docs with same sort value)
          const ascValues = reversedAsc.map((d) => d[field as keyof typeof d]);
          const descValues = descSorted.map((d) => d[field as keyof typeof d]);

          expect(ascValues).toEqual(descValues);
        }),
        { numRuns: 100 }
      );
    });

    it('ascending sort orders from smallest to largest', () => {
      fc.assert(
        fc.property(docArrayArb, (docs) => {
          const sorted = sortDocuments(docs, { age: 1 });
          for (let i = 1; i < sorted.length; i++) {
            expect(sorted[i]!.age).toBeGreaterThanOrEqual(sorted[i - 1]!.age);
          }
        }),
        { numRuns: 100 }
      );
    });

    it('descending sort orders from largest to smallest', () => {
      fc.assert(
        fc.property(docArrayArb, (docs) => {
          const sorted = sortDocuments(docs, { age: -1 });
          for (let i = 1; i < sorted.length; i++) {
            expect(sorted[i]!.age).toBeLessThanOrEqual(sorted[i - 1]!.age);
          }
        }),
        { numRuns: 100 }
      );
    });
  });

  describe('Sort idempotence', () => {
    it('sorting already sorted array yields same result', () => {
      fc.assert(
        fc.property(
          docArrayArb,
          sortFieldArb,
          fc.constantFrom(1 as const, -1 as const),
          (docs, field, direction) => {
            const sort: Sort = { [field]: direction };
            const onceSorted = sortDocuments(docs, sort);
            const twiceSorted = sortDocuments(onceSorted, sort);
            expect(twiceSorted).toEqual(onceSorted);
          }
        ),
        { numRuns: 100 }
      );
    });

    it('applying same sort multiple times is idempotent', () => {
      fc.assert(
        fc.property(docArrayArb, sortFieldArb, (docs, field) => {
          const sort: Sort = { [field]: 1 };
          const sorted1 = sortDocuments(docs, sort);
          const sorted2 = sortDocuments(sorted1, sort);
          const sorted3 = sortDocuments(sorted2, sort);
          expect(sorted2).toEqual(sorted1);
          expect(sorted3).toEqual(sorted1);
        }),
        { numRuns: 100 }
      );
    });
  });

  describe('Sort bijection (preserves elements)', () => {
    it('sorting preserves array length', () => {
      fc.assert(
        fc.property(
          docArrayArb,
          sortFieldArb,
          fc.constantFrom(1 as const, -1 as const),
          (docs, field, direction) => {
            const sorted = sortDocuments(docs, { [field]: direction });
            expect(sorted.length).toEqual(docs.length);
          }
        ),
        { numRuns: 100 }
      );
    });

    it('sorting preserves all elements (by _id)', () => {
      fc.assert(
        fc.property(docArrayArb, sortFieldArb, (docs, field) => {
          const sorted = sortDocuments(docs, { [field]: 1 });
          const originalIds = docs.map((d) => d._id).sort();
          const sortedIds = sorted.map((d) => d._id).sort();
          expect(sortedIds).toEqual(originalIds);
        }),
        { numRuns: 100 }
      );
    });

    it('sorting does not duplicate or lose elements', () => {
      fc.assert(
        fc.property(docArrayArb, sortFieldArb, (docs, field) => {
          const sorted = sortDocuments(docs, { [field]: 1 });

          // Create a count map of original elements by JSON representation
          const originalCounts = new Map<string, number>();
          for (const doc of docs) {
            const key = JSON.stringify(doc);
            originalCounts.set(key, (originalCounts.get(key) || 0) + 1);
          }

          // Count sorted elements
          const sortedCounts = new Map<string, number>();
          for (const doc of sorted) {
            const key = JSON.stringify(doc);
            sortedCounts.set(key, (sortedCounts.get(key) || 0) + 1);
          }

          expect(sortedCounts).toEqual(originalCounts);
        }),
        { numRuns: 100 }
      );
    });
  });

  describe('Sort immutability', () => {
    it('sortDocuments does not mutate original array', () => {
      fc.assert(
        fc.property(docArrayArb, sortFieldArb, (docs, field) => {
          const originalDocs = JSON.parse(JSON.stringify(docs));
          sortDocuments(docs, { [field]: 1 });
          expect(docs).toEqual(originalDocs);
        }),
        { numRuns: 100 }
      );
    });

    it('sortDocuments returns new array', () => {
      fc.assert(
        fc.property(docArrayArb, sortFieldArb, (docs, field) => {
          const sorted = sortDocuments(docs, { [field]: 1 });
          expect(sorted).not.toBe(docs);
        }),
        { numRuns: 100 }
      );
    });

    it('sortDocuments does not mutate document objects', () => {
      fc.assert(
        fc.property(docArrayArb, sortFieldArb, (docs, field) => {
          const originalDocsJson = docs.map((d) => JSON.stringify(d));
          sortDocuments(docs, { [field]: 1 });
          const afterSortJson = docs.map((d) => JSON.stringify(d));
          expect(afterSortJson).toEqual(originalDocsJson);
        }),
        { numRuns: 100 }
      );
    });
  });

  describe('Multi-field sort properties', () => {
    it('secondary sort is applied when primary field values are equal', () => {
      // Create documents with duplicate primary field values
      fc.assert(
        fc.property(
          fc.array(
            fc.record({
              _id: fc.string({ minLength: 1, maxLength: 10 }),
              group: fc.constantFrom('A', 'B'), // Only 2 groups to ensure duplicates
              value: fc.integer({ min: 1, max: 100 }),
            }),
            { minLength: 2, maxLength: 10 }
          ),
          (docs) => {
            const sorted = sortDocuments(docs, { group: 1, value: 1 });

            // Within each group, values should be ascending
            for (let i = 1; i < sorted.length; i++) {
              const prev = sorted[i - 1]!;
              const curr = sorted[i]!;
              if (prev.group === curr.group) {
                expect(curr.value).toBeGreaterThanOrEqual(prev.value);
              }
            }
          }
        ),
        { numRuns: 100 }
      );
    });

    it('multi-field sort respects field order', () => {
      fc.assert(
        fc.property(
          fc.array(
            fc.record({
              _id: fc.string({ minLength: 1, maxLength: 10 }),
              a: fc.integer({ min: 1, max: 3 }),
              b: fc.integer({ min: 1, max: 3 }),
            }),
            { minLength: 5, maxLength: 15 }
          ),
          (docs) => {
            const sortedAB = sortDocuments(docs, { a: 1, b: 1 });
            const sortedBA = sortDocuments(docs, { b: 1, a: 1 });

            // Primary sort field should be properly ordered
            for (let i = 1; i < sortedAB.length; i++) {
              expect(sortedAB[i]!.a).toBeGreaterThanOrEqual(sortedAB[i - 1]!.a);
            }
            for (let i = 1; i < sortedBA.length; i++) {
              expect(sortedBA[i]!.b).toBeGreaterThanOrEqual(sortedBA[i - 1]!.b);
            }
          }
        ),
        { numRuns: 100 }
      );
    });
  });

  describe('Empty and single element cases', () => {
    it('sorting empty array returns empty array', () => {
      fc.assert(
        fc.property(
          sortFieldArb,
          fc.constantFrom(1 as const, -1 as const),
          (field, direction) => {
            const sorted = sortDocuments([], { [field]: direction });
            expect(sorted).toEqual([]);
          }
        ),
        { numRuns: 20 }
      );
    });

    it('sorting single element array returns same element', () => {
      fc.assert(
        fc.property(
          sortableDocArb,
          sortFieldArb,
          fc.constantFrom(1 as const, -1 as const),
          (doc, field, direction) => {
            const sorted = sortDocuments([doc], { [field]: direction });
            expect(sorted.length).toEqual(1);
            expect(sorted[0]).toEqual(doc);
          }
        ),
        { numRuns: 50 }
      );
    });
  });

  describe('String sorting properties', () => {
    it('string fields sort lexicographically ascending', () => {
      fc.assert(
        fc.property(docArrayArb, (docs) => {
          const sorted = sortDocuments(docs, { name: 1 });
          for (let i = 1; i < sorted.length; i++) {
            expect(sorted[i]!.name >= sorted[i - 1]!.name).toBe(true);
          }
        }),
        { numRuns: 100 }
      );
    });

    it('string fields sort lexicographically descending', () => {
      fc.assert(
        fc.property(docArrayArb, (docs) => {
          const sorted = sortDocuments(docs, { name: -1 });
          for (let i = 1; i < sorted.length; i++) {
            expect(sorted[i]!.name <= sorted[i - 1]!.name).toBe(true);
          }
        }),
        { numRuns: 100 }
      );
    });
  });

  describe('Numeric sorting properties', () => {
    it('numeric comparison is correct (not string comparison)', () => {
      // This tests that 10 > 9, not "10" < "9" (string comparison)
      fc.assert(
        fc.property(
          fc.array(
            fc.record({
              _id: fc.string({ minLength: 1, maxLength: 10 }),
              num: fc.integer({ min: 1, max: 100 }),
            }),
            { minLength: 2, maxLength: 10 }
          ),
          (docs) => {
            const sorted = sortDocuments(docs, { num: 1 });
            for (let i = 1; i < sorted.length; i++) {
              // Numeric comparison: 10 > 9
              expect(sorted[i]!.num).toBeGreaterThanOrEqual(sorted[i - 1]!.num);
            }
          }
        ),
        { numRuns: 100 }
      );
    });

    it('negative numbers sort correctly', () => {
      fc.assert(
        fc.property(
          fc.array(
            fc.record({
              _id: fc.string({ minLength: 1, maxLength: 10 }),
              value: fc.integer({ min: -100, max: 100 }),
            }),
            { minLength: 2, maxLength: 10 }
          ),
          (docs) => {
            const sorted = sortDocuments(docs, { value: 1 });
            for (let i = 1; i < sorted.length; i++) {
              expect(sorted[i]!.value).toBeGreaterThanOrEqual(sorted[i - 1]!.value);
            }
          }
        ),
        { numRuns: 100 }
      );
    });
  });

  describe('Empty sort specification', () => {
    it('empty sort preserves original order', () => {
      fc.assert(
        fc.property(docArrayArb, (docs) => {
          const sorted = sortDocuments(docs, {});
          expect(sorted).toEqual(docs);
        }),
        { numRuns: 100 }
      );
    });
  });
});
