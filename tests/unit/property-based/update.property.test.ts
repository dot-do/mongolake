/**
 * Property-based tests for MongoDB update operators
 *
 * Uses fast-check to verify algebraic properties of update operations:
 * - $set then $unset on same field leaves doc without that field
 * - $inc is associative: $inc(a) then $inc(b) === $inc(a + b)
 * - $set is idempotent: setting same value twice equals setting once
 * - $push followed by $pull restores original array (for single element)
 * - $addToSet is idempotent
 */

import { describe, it, expect } from 'vitest';
import * as fc from 'fast-check';
import { applyUpdate } from '../../../src/utils/update.js';
import type { Document } from '../../../src/types.js';

// Arbitrary for simple primitive values
const primitiveValueArb = fc.oneof(
  fc.string({ maxLength: 20 }),
  fc.integer({ min: -1000, max: 1000 }),
  fc.boolean()
);

// Arbitrary for simple flat documents
const simpleDocArb = fc.record({
  _id: fc.string({ minLength: 1, maxLength: 10 }),
  name: fc.string({ maxLength: 20 }),
  count: fc.integer({ min: 0, max: 1000 }),
  active: fc.boolean(),
  score: fc.integer({ min: -100, max: 100 }),
  tags: fc.array(fc.string({ maxLength: 10 }), { maxLength: 5 }),
});

// Arbitrary for field names we can safely modify
const modifiableFieldArb = fc.constantFrom('name', 'count', 'active', 'score');

describe('Update Property-Based Tests', () => {
  describe('$set operator properties', () => {
    it('$set is idempotent: setting same value twice equals setting once', () => {
      fc.assert(
        fc.property(simpleDocArb, modifiableFieldArb, primitiveValueArb, (doc, field, value) => {
          const onceResult = applyUpdate(doc, { $set: { [field]: value } });
          const twiceResult = applyUpdate(
            applyUpdate(doc, { $set: { [field]: value } }),
            { $set: { [field]: value } }
          );
          expect(twiceResult[field]).toEqual(onceResult[field]);
        }),
        { numRuns: 100 }
      );
    });

    it('$set overwrites previous value completely', () => {
      fc.assert(
        fc.property(
          simpleDocArb,
          modifiableFieldArb,
          primitiveValueArb,
          primitiveValueArb,
          (doc, field, value1, value2) => {
            const result = applyUpdate(
              applyUpdate(doc, { $set: { [field]: value1 } }),
              { $set: { [field]: value2 } }
            );
            expect(result[field]).toEqual(value2);
          }
        ),
        { numRuns: 100 }
      );
    });

    it('$set preserves other fields', () => {
      fc.assert(
        fc.property(simpleDocArb, primitiveValueArb, (doc, value) => {
          const result = applyUpdate(doc, { $set: { name: value } });
          // All other fields should remain unchanged
          expect(result._id).toEqual(doc._id);
          expect(result.count).toEqual(doc.count);
          expect(result.active).toEqual(doc.active);
          expect(result.score).toEqual(doc.score);
          expect(result.tags).toEqual(doc.tags);
        }),
        { numRuns: 100 }
      );
    });

    it('$set with multiple fields applies all', () => {
      fc.assert(
        fc.property(
          simpleDocArb,
          fc.string({ maxLength: 20 }),
          fc.integer({ min: 0, max: 1000 }),
          (doc, nameValue, countValue) => {
            const result = applyUpdate(doc, {
              $set: { name: nameValue, count: countValue },
            });
            expect(result.name).toEqual(nameValue);
            expect(result.count).toEqual(countValue);
          }
        ),
        { numRuns: 100 }
      );
    });
  });

  describe('$unset operator properties', () => {
    it('$unset removes field from document', () => {
      fc.assert(
        fc.property(simpleDocArb, modifiableFieldArb, (doc, field) => {
          const result = applyUpdate(doc, { $unset: { [field]: '' } });
          expect(result).not.toHaveProperty(field);
        }),
        { numRuns: 100 }
      );
    });

    it('$unset is idempotent', () => {
      fc.assert(
        fc.property(simpleDocArb, modifiableFieldArb, (doc, field) => {
          const onceResult = applyUpdate(doc, { $unset: { [field]: '' } });
          const twiceResult = applyUpdate(onceResult, { $unset: { [field]: '' } });
          expect(twiceResult).toEqual(onceResult);
        }),
        { numRuns: 100 }
      );
    });

    it('$unset preserves other fields', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          const result = applyUpdate(doc, { $unset: { name: '' } });
          expect(result._id).toEqual(doc._id);
          expect(result.count).toEqual(doc.count);
          expect(result.active).toEqual(doc.active);
          expect(result.score).toEqual(doc.score);
          expect(result.tags).toEqual(doc.tags);
        }),
        { numRuns: 100 }
      );
    });
  });

  describe('$set and $unset interaction', () => {
    it('$set then $unset removes the field', () => {
      fc.assert(
        fc.property(simpleDocArb, modifiableFieldArb, primitiveValueArb, (doc, field, value) => {
          const afterSet = applyUpdate(doc, { $set: { [field]: value } });
          const afterUnset = applyUpdate(afterSet, { $unset: { [field]: '' } });
          expect(afterUnset).not.toHaveProperty(field);
        }),
        { numRuns: 100 }
      );
    });

    it('$unset then $set restores with new value', () => {
      fc.assert(
        fc.property(simpleDocArb, modifiableFieldArb, primitiveValueArb, (doc, field, value) => {
          const afterUnset = applyUpdate(doc, { $unset: { [field]: '' } });
          const afterSet = applyUpdate(afterUnset, { $set: { [field]: value } });
          expect(afterSet[field]).toEqual(value);
        }),
        { numRuns: 100 }
      );
    });
  });

  describe('$inc operator properties', () => {
    it('$inc is associative: $inc(a) then $inc(b) === $inc(a + b)', () => {
      fc.assert(
        fc.property(
          simpleDocArb,
          fc.integer({ min: -100, max: 100 }),
          fc.integer({ min: -100, max: 100 }),
          (doc, incA, incB) => {
            // Apply increments separately
            const separateResult = applyUpdate(
              applyUpdate(doc, { $inc: { count: incA } }),
              { $inc: { count: incB } }
            );

            // Apply combined increment
            const combinedResult = applyUpdate(doc, { $inc: { count: incA + incB } });

            expect(separateResult.count).toEqual(combinedResult.count);
          }
        ),
        { numRuns: 100 }
      );
    });

    it('$inc by 0 is identity', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          const result = applyUpdate(doc, { $inc: { count: 0 } });
          expect(result.count).toEqual(doc.count);
        }),
        { numRuns: 100 }
      );
    });

    it('$inc(n) then $inc(-n) restores original value', () => {
      fc.assert(
        fc.property(simpleDocArb, fc.integer({ min: -100, max: 100 }), (doc, increment) => {
          const afterInc = applyUpdate(doc, { $inc: { count: increment } });
          const afterDecrement = applyUpdate(afterInc, { $inc: { count: -increment } });
          expect(afterDecrement.count).toEqual(doc.count);
        }),
        { numRuns: 100 }
      );
    });

    it('$inc is commutative', () => {
      fc.assert(
        fc.property(
          simpleDocArb,
          fc.integer({ min: -100, max: 100 }),
          fc.integer({ min: -100, max: 100 }),
          (doc, incA, incB) => {
            const resultAB = applyUpdate(
              applyUpdate(doc, { $inc: { count: incA } }),
              { $inc: { count: incB } }
            );
            const resultBA = applyUpdate(
              applyUpdate(doc, { $inc: { count: incB } }),
              { $inc: { count: incA } }
            );
            expect(resultAB.count).toEqual(resultBA.count);
          }
        ),
        { numRuns: 100 }
      );
    });

    it('$inc on missing field initializes to increment value', () => {
      fc.assert(
        fc.property(
          fc.record({
            _id: fc.string({ minLength: 1, maxLength: 10 }),
            name: fc.string({ maxLength: 20 }),
          }),
          fc.integer({ min: -100, max: 100 }),
          (doc, increment) => {
            const result = applyUpdate(doc as Document, { $inc: { newField: increment } });
            expect((result as Record<string, unknown>).newField).toEqual(increment);
          }
        ),
        { numRuns: 100 }
      );
    });
  });

  describe('$push operator properties', () => {
    it('$push adds element to end of array', () => {
      fc.assert(
        fc.property(simpleDocArb, fc.string({ maxLength: 10 }), (doc, element) => {
          const result = applyUpdate(doc, { $push: { tags: element } });
          expect(result.tags[result.tags.length - 1]).toEqual(element);
          expect(result.tags.length).toEqual(doc.tags.length + 1);
        }),
        { numRuns: 100 }
      );
    });

    it('$push preserves existing elements', () => {
      fc.assert(
        fc.property(simpleDocArb, fc.string({ maxLength: 10 }), (doc, element) => {
          const result = applyUpdate(doc, { $push: { tags: element } });
          // All original elements should be preserved
          for (let i = 0; i < doc.tags.length; i++) {
            expect(result.tags[i]).toEqual(doc.tags[i]);
          }
        }),
        { numRuns: 100 }
      );
    });

    it('$push with $each adds multiple elements', () => {
      fc.assert(
        fc.property(
          simpleDocArb,
          fc.array(fc.string({ maxLength: 10 }), { minLength: 1, maxLength: 3 }),
          (doc, elements) => {
            const result = applyUpdate(doc, { $push: { tags: { $each: elements } } });
            expect(result.tags.length).toEqual(doc.tags.length + elements.length);
            // Last elements should be the pushed ones
            for (let i = 0; i < elements.length; i++) {
              expect(result.tags[doc.tags.length + i]).toEqual(elements[i]);
            }
          }
        ),
        { numRuns: 100 }
      );
    });
  });

  describe('$pull operator properties', () => {
    it('$pull removes matching elements', () => {
      fc.assert(
        fc.property(
          fc.record({
            _id: fc.string({ minLength: 1, maxLength: 10 }),
            tags: fc.array(fc.constantFrom('a', 'b', 'c'), { minLength: 1, maxLength: 10 }),
          }),
          fc.constantFrom('a', 'b', 'c'),
          (doc, elementToRemove) => {
            const result = applyUpdate(doc as Document, { $pull: { tags: elementToRemove } });
            expect((result as typeof doc).tags).not.toContain(elementToRemove);
          }
        ),
        { numRuns: 100 }
      );
    });

    it('$pull is idempotent', () => {
      fc.assert(
        fc.property(simpleDocArb, fc.string({ maxLength: 10 }), (doc, element) => {
          const onceResult = applyUpdate(doc, { $pull: { tags: element } });
          const twiceResult = applyUpdate(onceResult, { $pull: { tags: element } });
          expect(twiceResult.tags).toEqual(onceResult.tags);
        }),
        { numRuns: 100 }
      );
    });

    it('$pull on non-existent element is no-op', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          const uniqueElement = 'unique_element_not_in_array_' + Math.random();
          const result = applyUpdate(doc, { $pull: { tags: uniqueElement } });
          expect(result.tags).toEqual(doc.tags);
        }),
        { numRuns: 100 }
      );
    });
  });

  describe('$push and $pull interaction', () => {
    it('$push then $pull of same element restores array length (single occurrence)', () => {
      fc.assert(
        fc.property(
          fc.record({
            _id: fc.string({ minLength: 1, maxLength: 10 }),
            tags: fc.constant([] as string[]),
          }),
          fc.string({ minLength: 1, maxLength: 10 }),
          (doc, element) => {
            const afterPush = applyUpdate(doc as Document, { $push: { tags: element } });
            const afterPull = applyUpdate(afterPush, { $pull: { tags: element } });
            expect((afterPull as typeof doc).tags.length).toEqual(doc.tags.length);
          }
        ),
        { numRuns: 100 }
      );
    });
  });

  describe('$addToSet operator properties', () => {
    it('$addToSet is idempotent', () => {
      fc.assert(
        fc.property(simpleDocArb, fc.string({ maxLength: 10 }), (doc, element) => {
          const onceResult = applyUpdate(doc, { $addToSet: { tags: element } });
          const twiceResult = applyUpdate(onceResult, { $addToSet: { tags: element } });
          expect(twiceResult.tags).toEqual(onceResult.tags);
        }),
        { numRuns: 100 }
      );
    });

    it('$addToSet only adds if not present', () => {
      fc.assert(
        fc.property(
          fc.record({
            _id: fc.string({ minLength: 1, maxLength: 10 }),
            tags: fc.constant(['existing'] as string[]),
          }),
          (doc) => {
            const result = applyUpdate(doc as Document, { $addToSet: { tags: 'existing' } });
            expect((result as typeof doc).tags.length).toEqual(1);
            expect((result as typeof doc).tags).toContain('existing');
          }
        ),
        { numRuns: 100 }
      );
    });

    it('$addToSet adds new element', () => {
      fc.assert(
        fc.property(
          fc.record({
            _id: fc.string({ minLength: 1, maxLength: 10 }),
            tags: fc.constant([] as string[]),
          }),
          fc.string({ minLength: 1, maxLength: 10 }),
          (doc, element) => {
            const result = applyUpdate(doc as Document, { $addToSet: { tags: element } });
            expect((result as typeof doc).tags).toContain(element);
            expect((result as typeof doc).tags.length).toEqual(1);
          }
        ),
        { numRuns: 100 }
      );
    });

    it('$addToSet with $each is idempotent', () => {
      fc.assert(
        fc.property(
          simpleDocArb,
          fc.array(fc.string({ maxLength: 10 }), { maxLength: 3 }),
          (doc, elements) => {
            const onceResult = applyUpdate(doc, { $addToSet: { tags: { $each: elements } } });
            const twiceResult = applyUpdate(onceResult, {
              $addToSet: { tags: { $each: elements } },
            });
            expect(twiceResult.tags).toEqual(onceResult.tags);
          }
        ),
        { numRuns: 100 }
      );
    });
  });

  describe('$pop operator properties', () => {
    it('$pop removes last element when direction is 1', () => {
      fc.assert(
        fc.property(
          fc.record({
            _id: fc.string({ minLength: 1, maxLength: 10 }),
            tags: fc.array(fc.string({ maxLength: 10 }), { minLength: 1, maxLength: 5 }),
          }),
          (doc) => {
            const result = applyUpdate(doc as Document, { $pop: { tags: 1 } });
            expect((result as typeof doc).tags.length).toEqual(doc.tags.length - 1);
            // All elements except last should be preserved
            for (let i = 0; i < doc.tags.length - 1; i++) {
              expect((result as typeof doc).tags[i]).toEqual(doc.tags[i]);
            }
          }
        ),
        { numRuns: 100 }
      );
    });

    it('$pop removes first element when direction is -1', () => {
      fc.assert(
        fc.property(
          fc.record({
            _id: fc.string({ minLength: 1, maxLength: 10 }),
            tags: fc.array(fc.string({ maxLength: 10 }), { minLength: 1, maxLength: 5 }),
          }),
          (doc) => {
            const result = applyUpdate(doc as Document, { $pop: { tags: -1 } });
            expect((result as typeof doc).tags.length).toEqual(doc.tags.length - 1);
            // All elements except first should be preserved (shifted)
            for (let i = 0; i < doc.tags.length - 1; i++) {
              expect((result as typeof doc).tags[i]).toEqual(doc.tags[i + 1]);
            }
          }
        ),
        { numRuns: 100 }
      );
    });

    it('$pop on empty array is no-op', () => {
      fc.assert(
        fc.property(
          fc.record({
            _id: fc.string({ minLength: 1, maxLength: 10 }),
            tags: fc.constant([] as string[]),
          }),
          fc.constantFrom(1 as const, -1 as const),
          (doc, direction) => {
            const result = applyUpdate(doc as Document, { $pop: { tags: direction } });
            expect((result as typeof doc).tags).toEqual([]);
          }
        ),
        { numRuns: 100 }
      );
    });
  });

  describe('$rename operator properties', () => {
    it('$rename moves value to new field', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          const result = applyUpdate(doc, { $rename: { name: 'newName' } });
          expect(result).not.toHaveProperty('name');
          expect((result as Record<string, unknown>).newName).toEqual(doc.name);
        }),
        { numRuns: 100 }
      );
    });

    it('$rename preserves other fields', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          const result = applyUpdate(doc, { $rename: { name: 'newName' } });
          expect(result._id).toEqual(doc._id);
          expect(result.count).toEqual(doc.count);
          expect(result.active).toEqual(doc.active);
          expect(result.score).toEqual(doc.score);
          expect(result.tags).toEqual(doc.tags);
        }),
        { numRuns: 100 }
      );
    });
  });

  describe('Update immutability', () => {
    it('applyUpdate does not mutate original document', () => {
      fc.assert(
        fc.property(simpleDocArb, primitiveValueArb, (doc, value) => {
          const originalDoc = JSON.parse(JSON.stringify(doc));
          applyUpdate(doc, { $set: { name: value } });
          expect(doc).toEqual(originalDoc);
        }),
        { numRuns: 100 }
      );
    });

    it('applyUpdate returns new object', () => {
      fc.assert(
        fc.property(simpleDocArb, primitiveValueArb, (doc, value) => {
          const result = applyUpdate(doc, { $set: { name: value } });
          expect(result).not.toBe(doc);
        }),
        { numRuns: 100 }
      );
    });
  });
});
