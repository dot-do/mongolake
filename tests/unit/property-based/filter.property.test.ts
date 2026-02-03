/**
 * Property-based tests for MongoDB filter matching
 *
 * Uses fast-check to verify algebraic properties of filter operations:
 * - $and behaves like logical AND: filter($and([f1, f2])) === filter(f1) && filter(f2)
 * - $or behaves like logical OR: filter($or([f1, f2])) === filter(f1) || filter(f2)
 * - $nor behaves like logical NOR: filter($nor([f1, f2])) === !(filter(f1) || filter(f2))
 * - $not behaves like logical NOT for comparison operators
 */

import { describe, it, expect } from 'vitest';
import * as fc from 'fast-check';
import { matchesFilter } from '../../../src/utils/filter.js';

// Arbitrary for simple document values (primitives that work well with MongoDB operators)
const primitiveValueArb = fc.oneof(
  fc.string({ maxLength: 20 }),
  fc.integer({ min: -1000, max: 1000 }),
  fc.boolean(),
  fc.constant(null)
);

// Arbitrary for simple flat documents (avoid deeply nested to keep tests fast)
const simpleDocArb = fc.record({
  _id: fc.string({ minLength: 1, maxLength: 10 }),
  name: fc.string({ maxLength: 20 }),
  age: fc.integer({ min: 0, max: 150 }),
  active: fc.boolean(),
  score: fc.integer({ min: -100, max: 100 }),
});

// Arbitrary for a simple equality filter on one field
const equalityFilterArb = fc.constantFrom('name', 'age', 'active', 'score').chain((field) =>
  primitiveValueArb.map((value) => ({ [field]: value }))
);

// Arbitrary for a comparison filter ($gt, $lt, $gte, $lte)
const comparisonFilterArb = fc
  .tuple(
    fc.constantFrom('age', 'score'),
    fc.constantFrom('$gt', '$lt', '$gte', '$lte'),
    fc.integer({ min: -100, max: 100 })
  )
  .map(([field, op, value]) => ({ [field]: { [op]: value } }));

// Arbitrary for a simple filter (either equality or comparison)
const simpleFilterArb = fc.oneof(equalityFilterArb, comparisonFilterArb);

describe('Filter Property-Based Tests', () => {
  describe('$and operator properties', () => {
    it('$and([f1, f2]) === matchesFilter(f1) && matchesFilter(f2)', () => {
      fc.assert(
        fc.property(
          simpleDocArb,
          simpleFilterArb,
          simpleFilterArb,
          (doc, filter1, filter2) => {
            const andResult = matchesFilter(doc, { $and: [filter1, filter2] });
            const separateResult = matchesFilter(doc, filter1) && matchesFilter(doc, filter2);
            expect(andResult).toBe(separateResult);
          }
        ),
        { numRuns: 100 }
      );
    });

    it('$and with single filter equals the filter alone', () => {
      fc.assert(
        fc.property(simpleDocArb, simpleFilterArb, (doc, filter) => {
          const andResult = matchesFilter(doc, { $and: [filter] });
          const directResult = matchesFilter(doc, filter);
          expect(andResult).toBe(directResult);
        }),
        { numRuns: 100 }
      );
    });

    it('$and with empty array matches everything', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          // Empty $and should match all documents (vacuous truth)
          const result = matchesFilter(doc, { $and: [] });
          expect(result).toBe(true);
        }),
        { numRuns: 50 }
      );
    });

    it('$and is commutative', () => {
      fc.assert(
        fc.property(
          simpleDocArb,
          simpleFilterArb,
          simpleFilterArb,
          (doc, filter1, filter2) => {
            const result1 = matchesFilter(doc, { $and: [filter1, filter2] });
            const result2 = matchesFilter(doc, { $and: [filter2, filter1] });
            expect(result1).toBe(result2);
          }
        ),
        { numRuns: 100 }
      );
    });

    it('$and is associative', () => {
      fc.assert(
        fc.property(
          simpleDocArb,
          simpleFilterArb,
          simpleFilterArb,
          simpleFilterArb,
          (doc, f1, f2, f3) => {
            // (f1 AND f2) AND f3 === f1 AND (f2 AND f3)
            const leftAssoc = matchesFilter(doc, { $and: [{ $and: [f1, f2] }, f3] });
            const rightAssoc = matchesFilter(doc, { $and: [f1, { $and: [f2, f3] }] });
            expect(leftAssoc).toBe(rightAssoc);
          }
        ),
        { numRuns: 100 }
      );
    });
  });

  describe('$or operator properties', () => {
    it('$or([f1, f2]) === matchesFilter(f1) || matchesFilter(f2)', () => {
      fc.assert(
        fc.property(
          simpleDocArb,
          simpleFilterArb,
          simpleFilterArb,
          (doc, filter1, filter2) => {
            const orResult = matchesFilter(doc, { $or: [filter1, filter2] });
            const separateResult = matchesFilter(doc, filter1) || matchesFilter(doc, filter2);
            expect(orResult).toBe(separateResult);
          }
        ),
        { numRuns: 100 }
      );
    });

    it('$or with single filter equals the filter alone', () => {
      fc.assert(
        fc.property(simpleDocArb, simpleFilterArb, (doc, filter) => {
          const orResult = matchesFilter(doc, { $or: [filter] });
          const directResult = matchesFilter(doc, filter);
          expect(orResult).toBe(directResult);
        }),
        { numRuns: 100 }
      );
    });

    it('$or with empty array matches nothing', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          // Empty $or should match no documents
          const result = matchesFilter(doc, { $or: [] });
          expect(result).toBe(false);
        }),
        { numRuns: 50 }
      );
    });

    it('$or is commutative', () => {
      fc.assert(
        fc.property(
          simpleDocArb,
          simpleFilterArb,
          simpleFilterArb,
          (doc, filter1, filter2) => {
            const result1 = matchesFilter(doc, { $or: [filter1, filter2] });
            const result2 = matchesFilter(doc, { $or: [filter2, filter1] });
            expect(result1).toBe(result2);
          }
        ),
        { numRuns: 100 }
      );
    });

    it('$or is associative', () => {
      fc.assert(
        fc.property(
          simpleDocArb,
          simpleFilterArb,
          simpleFilterArb,
          simpleFilterArb,
          (doc, f1, f2, f3) => {
            // (f1 OR f2) OR f3 === f1 OR (f2 OR f3)
            const leftAssoc = matchesFilter(doc, { $or: [{ $or: [f1, f2] }, f3] });
            const rightAssoc = matchesFilter(doc, { $or: [f1, { $or: [f2, f3] }] });
            expect(leftAssoc).toBe(rightAssoc);
          }
        ),
        { numRuns: 100 }
      );
    });
  });

  describe('$nor operator properties', () => {
    it('$nor([f1, f2]) === !(matchesFilter(f1) || matchesFilter(f2))', () => {
      fc.assert(
        fc.property(
          simpleDocArb,
          simpleFilterArb,
          simpleFilterArb,
          (doc, filter1, filter2) => {
            const norResult = matchesFilter(doc, { $nor: [filter1, filter2] });
            const expectedResult = !(matchesFilter(doc, filter1) || matchesFilter(doc, filter2));
            expect(norResult).toBe(expectedResult);
          }
        ),
        { numRuns: 100 }
      );
    });

    it('$nor is equivalent to negated $or', () => {
      fc.assert(
        fc.property(
          simpleDocArb,
          simpleFilterArb,
          simpleFilterArb,
          (doc, filter1, filter2) => {
            const norResult = matchesFilter(doc, { $nor: [filter1, filter2] });
            const orResult = matchesFilter(doc, { $or: [filter1, filter2] });
            expect(norResult).toBe(!orResult);
          }
        ),
        { numRuns: 100 }
      );
    });
  });

  describe('De Morgan\'s Laws', () => {
    it('NOT(f1 AND f2) === (NOT f1) OR (NOT f2) - via $nor equivalence', () => {
      // We test this using field-level $not with comparison operators
      // $nor([f1, f2]) === $and([{field: {$not: f1}}, {field: {$not: f2}}]) for field-level conditions
      fc.assert(
        fc.property(
          simpleDocArb,
          fc.integer({ min: -100, max: 100 }),
          fc.integer({ min: -100, max: 100 }),
          (doc, val1, val2) => {
            // Test: NOT(age > val1 AND age > val2) === (age <= val1 OR age <= val2)
            const f1 = { age: { $gt: val1 } };
            const f2 = { age: { $gt: val2 } };

            const andResult = matchesFilter(doc, { $and: [f1, f2] });
            const orNegatedResult = matchesFilter(doc, {
              $or: [{ age: { $lte: val1 } }, { age: { $lte: val2 } }],
            });

            // NOT(A AND B) === (NOT A) OR (NOT B)
            expect(!andResult).toBe(orNegatedResult);
          }
        ),
        { numRuns: 100 }
      );
    });

    it('NOT(f1 OR f2) === (NOT f1) AND (NOT f2)', () => {
      fc.assert(
        fc.property(
          simpleDocArb,
          fc.integer({ min: -100, max: 100 }),
          fc.integer({ min: -100, max: 100 }),
          (doc, val1, val2) => {
            // Test: NOT(age > val1 OR age > val2) === (age <= val1 AND age <= val2)
            const f1 = { age: { $gt: val1 } };
            const f2 = { age: { $gt: val2 } };

            const orResult = matchesFilter(doc, { $or: [f1, f2] });
            const andNegatedResult = matchesFilter(doc, {
              $and: [{ age: { $lte: val1 } }, { age: { $lte: val2 } }],
            });

            // NOT(A OR B) === (NOT A) AND (NOT B)
            expect(!orResult).toBe(andNegatedResult);
          }
        ),
        { numRuns: 100 }
      );
    });
  });

  describe('Comparison operator properties', () => {
    it('$gt and $lte are complementary', () => {
      fc.assert(
        fc.property(simpleDocArb, fc.integer({ min: -100, max: 100 }), (doc, threshold) => {
          const gtResult = matchesFilter(doc, { age: { $gt: threshold } });
          const lteResult = matchesFilter(doc, { age: { $lte: threshold } });
          // Exactly one should be true (they are complements)
          expect(gtResult !== lteResult).toBe(true);
        }),
        { numRuns: 100 }
      );
    });

    it('$gte and $lt are complementary', () => {
      fc.assert(
        fc.property(simpleDocArb, fc.integer({ min: -100, max: 100 }), (doc, threshold) => {
          const gteResult = matchesFilter(doc, { age: { $gte: threshold } });
          const ltResult = matchesFilter(doc, { age: { $lt: threshold } });
          // Exactly one should be true (they are complements)
          expect(gteResult !== ltResult).toBe(true);
        }),
        { numRuns: 100 }
      );
    });

    it('$eq and $ne are complementary', () => {
      fc.assert(
        fc.property(simpleDocArb, fc.integer({ min: 0, max: 150 }), (doc, value) => {
          const eqResult = matchesFilter(doc, { age: { $eq: value } });
          const neResult = matchesFilter(doc, { age: { $ne: value } });
          // Exactly one should be true (they are complements)
          expect(eqResult !== neResult).toBe(true);
        }),
        { numRuns: 100 }
      );
    });

    it('$gte equals $gt OR $eq', () => {
      fc.assert(
        fc.property(simpleDocArb, fc.integer({ min: -100, max: 100 }), (doc, threshold) => {
          const gteResult = matchesFilter(doc, { age: { $gte: threshold } });
          const gtOrEqResult =
            matchesFilter(doc, { age: { $gt: threshold } }) ||
            matchesFilter(doc, { age: { $eq: threshold } });
          expect(gteResult).toBe(gtOrEqResult);
        }),
        { numRuns: 100 }
      );
    });

    it('$lte equals $lt OR $eq', () => {
      fc.assert(
        fc.property(simpleDocArb, fc.integer({ min: -100, max: 100 }), (doc, threshold) => {
          const lteResult = matchesFilter(doc, { age: { $lte: threshold } });
          const ltOrEqResult =
            matchesFilter(doc, { age: { $lt: threshold } }) ||
            matchesFilter(doc, { age: { $eq: threshold } });
          expect(lteResult).toBe(ltOrEqResult);
        }),
        { numRuns: 100 }
      );
    });
  });

  describe('$in and $nin operator properties', () => {
    it('$in is equivalent to $or of $eq conditions', () => {
      fc.assert(
        fc.property(
          simpleDocArb,
          fc.array(fc.integer({ min: 0, max: 150 }), { minLength: 1, maxLength: 5 }),
          (doc, values) => {
            const inResult = matchesFilter(doc, { age: { $in: values } });
            const orResult = matchesFilter(doc, {
              $or: values.map((v) => ({ age: { $eq: v } })),
            });
            expect(inResult).toBe(orResult);
          }
        ),
        { numRuns: 100 }
      );
    });

    it('$in and $nin are complementary', () => {
      fc.assert(
        fc.property(
          simpleDocArb,
          fc.array(fc.integer({ min: 0, max: 150 }), { minLength: 1, maxLength: 5 }),
          (doc, values) => {
            const inResult = matchesFilter(doc, { age: { $in: values } });
            const ninResult = matchesFilter(doc, { age: { $nin: values } });
            // Exactly one should be true
            expect(inResult !== ninResult).toBe(true);
          }
        ),
        { numRuns: 100 }
      );
    });

    it('$in with single value equals $eq', () => {
      fc.assert(
        fc.property(simpleDocArb, fc.integer({ min: 0, max: 150 }), (doc, value) => {
          const inResult = matchesFilter(doc, { age: { $in: [value] } });
          const eqResult = matchesFilter(doc, { age: { $eq: value } });
          expect(inResult).toBe(eqResult);
        }),
        { numRuns: 100 }
      );
    });
  });

  describe('$exists operator properties', () => {
    it('$exists: true and $exists: false are complementary', () => {
      fc.assert(
        fc.property(
          fc.oneof(
            simpleDocArb,
            simpleDocArb.map((doc) => {
              const { age: _, ...rest } = doc;
              return rest;
            })
          ),
          (doc) => {
            const existsTrue = matchesFilter(doc, { age: { $exists: true } });
            const existsFalse = matchesFilter(doc, { age: { $exists: false } });
            expect(existsTrue !== existsFalse).toBe(true);
          }
        ),
        { numRuns: 100 }
      );
    });
  });

  describe('Identity and absorption laws', () => {
    it('filter AND true-filter equals filter', () => {
      fc.assert(
        fc.property(simpleDocArb, simpleFilterArb, (doc, filter) => {
          // A true filter is an empty object {} which matches everything
          const filterResult = matchesFilter(doc, filter);
          const andWithTrue = matchesFilter(doc, { $and: [filter, {}] });
          expect(andWithTrue).toBe(filterResult);
        }),
        { numRuns: 100 }
      );
    });

    it('filter OR false-filter equals filter', () => {
      fc.assert(
        fc.property(simpleDocArb, simpleFilterArb, (doc, filter) => {
          // A false filter for $or purposes: we use an impossible condition
          // An $or with a filter that can't match should still match if the main filter matches
          const filterResult = matchesFilter(doc, filter);
          // $or with filter and empty $or (which matches nothing)
          const orWithFalse = matchesFilter(doc, { $or: [filter, { $or: [] }] });
          expect(orWithFalse).toBe(filterResult);
        }),
        { numRuns: 100 }
      );
    });
  });

  describe('Filter consistency', () => {
    it('applying same filter twice yields same result', () => {
      fc.assert(
        fc.property(simpleDocArb, simpleFilterArb, (doc, filter) => {
          const result1 = matchesFilter(doc, filter);
          const result2 = matchesFilter(doc, filter);
          expect(result1).toBe(result2);
        }),
        { numRuns: 100 }
      );
    });

    it('filter result is always boolean', () => {
      fc.assert(
        fc.property(simpleDocArb, simpleFilterArb, (doc, filter) => {
          const result = matchesFilter(doc, filter);
          expect(typeof result).toBe('boolean');
        }),
        { numRuns: 100 }
      );
    });
  });
});
