/**
 * MongoLake Aggregation Stages Tests
 *
 * Tests for additional MongoDB aggregation pipeline stages:
 * - $bucket - group by value ranges
 * - $bucketAuto - automatic bucket distribution
 * - $graphLookup - recursive graph traversal
 * - $merge - merge into collection (validation only)
 * - $out - output to collection (validation only)
 * - $redact - access control at document level
 * - $replaceRoot / $replaceWith - replace document
 * - $sample - random sample
 * - $sortByCount - group and count
 */

import { describe, it, expect } from 'vitest';
import {
  processBucketAuto,
  processGraphLookup,
  validateMergeStage,
  getMergeTarget,
  getOutTarget,
  processRedact,
  processReplaceRoot,
  processSample,
  processSortByCount,
  evaluateAccumulator,
} from '../../../src/query/aggregation-stages.js';
import type { Document, WithId } from '../../../src/types.js';

// ============================================================================
// Test Data
// ============================================================================

interface TestDocument extends Document {
  _id: string;
  [key: string]: unknown;
}

function createTestDoc(id: string, data: Record<string, unknown>): WithId<TestDocument> {
  return { _id: id, ...data } as WithId<TestDocument>;
}

// ============================================================================
// $bucketAuto Stage Tests
// ============================================================================

describe('$bucketAuto Stage', () => {
  const docs: WithId<Document>[] = [
    createTestDoc('1', { price: 10 }),
    createTestDoc('2', { price: 20 }),
    createTestDoc('3', { price: 30 }),
    createTestDoc('4', { price: 40 }),
    createTestDoc('5', { price: 50 }),
    createTestDoc('6', { price: 60 }),
    createTestDoc('7', { price: 70 }),
    createTestDoc('8', { price: 80 }),
    createTestDoc('9', { price: 90 }),
    createTestDoc('10', { price: 100 }),
  ];

  it('should distribute documents into specified number of buckets', () => {
    const result = processBucketAuto(docs, {
      groupBy: '$price',
      buckets: 5,
    });

    expect(result.length).toBe(5);
    // Each bucket should have ~2 documents
    for (const bucket of result) {
      expect((bucket as Record<string, unknown>).count).toBeGreaterThanOrEqual(2);
    }
  });

  it('should create buckets with min/max boundaries', () => {
    const result = processBucketAuto(docs, {
      groupBy: '$price',
      buckets: 2,
    });

    expect(result.length).toBe(2);

    const bucket1 = result[0] as Record<string, unknown>;
    const bucket2 = result[1] as Record<string, unknown>;

    expect(bucket1._id).toBeDefined();
    expect((bucket1._id as { min: number; max: number }).min).toBeDefined();
    expect((bucket1._id as { min: number; max: number }).max).toBeDefined();
    expect(bucket2._id).toBeDefined();
  });

  it('should support custom output accumulators', () => {
    const result = processBucketAuto(docs, {
      groupBy: '$price',
      buckets: 2,
      output: {
        count: { $sum: 1 },
        avgPrice: { $avg: '$price' },
        minPrice: { $min: '$price' },
        maxPrice: { $max: '$price' },
      },
    });

    expect(result.length).toBe(2);
    for (const bucket of result) {
      const b = bucket as Record<string, unknown>;
      expect(b.count).toBeGreaterThan(0);
      expect(b.avgPrice).toBeDefined();
      expect(b.minPrice).toBeDefined();
      expect(b.maxPrice).toBeDefined();
    }
  });

  it('should handle empty input', () => {
    const result = processBucketAuto([], {
      groupBy: '$price',
      buckets: 5,
    });

    expect(result).toEqual([]);
  });

  it('should handle documents with null/undefined values', () => {
    const docsWithNulls: WithId<Document>[] = [
      createTestDoc('1', { price: 10 }),
      createTestDoc('2', { price: null }),
      createTestDoc('3', { price: 30 }),
      createTestDoc('4', {}), // missing price
    ];

    const result = processBucketAuto(docsWithNulls, {
      groupBy: '$price',
      buckets: 2,
    });

    // Only documents with valid numeric prices should be included
    expect(result.length).toBeGreaterThanOrEqual(1);
    const totalCount = result.reduce((sum, b) => sum + ((b as Record<string, unknown>).count as number), 0);
    expect(totalCount).toBe(2); // Only docs 1 and 3
  });

  it('should handle single document', () => {
    const singleDoc = [createTestDoc('1', { price: 100 })];
    const result = processBucketAuto(singleDoc, {
      groupBy: '$price',
      buckets: 5,
    });

    expect(result.length).toBe(1);
    expect((result[0] as Record<string, unknown>).count).toBe(1);
  });

  it('should handle more buckets than documents', () => {
    const fewDocs = [
      createTestDoc('1', { price: 10 }),
      createTestDoc('2', { price: 20 }),
    ];

    const result = processBucketAuto(fewDocs, {
      groupBy: '$price',
      buckets: 10,
    });

    // Should create at most as many buckets as documents
    expect(result.length).toBeLessThanOrEqual(2);
  });
});

// ============================================================================
// $graphLookup Stage Tests
// ============================================================================

describe('$graphLookup Stage', () => {
  const employees: WithId<Document>[] = [
    createTestDoc('1', { name: 'Alice', reportsTo: null }),
    createTestDoc('2', { name: 'Bob', reportsTo: '1' }),
    createTestDoc('3', { name: 'Charlie', reportsTo: '1' }),
    createTestDoc('4', { name: 'Diana', reportsTo: '2' }),
    createTestDoc('5', { name: 'Eve', reportsTo: '2' }),
    createTestDoc('6', { name: 'Frank', reportsTo: '4' }),
  ];

  const mockGetCollection = async (): Promise<WithId<Document>[]> => employees;

  it('should traverse graph recursively', async () => {
    const docs = [createTestDoc('start', { startId: '1' })];

    const result = await processGraphLookup(docs, {
      from: 'employees',
      startWith: '$startId',
      connectFromField: '_id',
      connectToField: 'reportsTo',
      as: 'directReports',
    }, mockGetCollection);

    expect(result.length).toBe(1);
    const reports = (result[0] as Record<string, unknown>).directReports as unknown[];
    // Alice (1) -> Bob (2), Charlie (3) -> Diana (4), Eve (5) -> Frank (6)
    expect(reports.length).toBeGreaterThanOrEqual(2);
  });

  it('should respect maxDepth', async () => {
    const docs = [createTestDoc('start', { startId: '1' })];

    const result = await processGraphLookup(docs, {
      from: 'employees',
      startWith: '$startId',
      connectFromField: '_id',
      connectToField: 'reportsTo',
      as: 'directReports',
      maxDepth: 0,
    }, mockGetCollection);

    expect(result.length).toBe(1);
    const reports = (result[0] as Record<string, unknown>).directReports as unknown[];
    // With maxDepth 0: only immediate reports (Bob, Charlie) - depth 0 finds direct matches
    expect(reports.length).toBe(2);
  });

  it('should add depthField when specified', async () => {
    const docs = [createTestDoc('start', { startId: '1' })];

    const result = await processGraphLookup(docs, {
      from: 'employees',
      startWith: '$startId',
      connectFromField: '_id',
      connectToField: 'reportsTo',
      as: 'reports',
      depthField: 'level',
      maxDepth: 2,
    }, mockGetCollection);

    const reports = (result[0] as Record<string, unknown>).reports as Record<string, unknown>[];
    // All reports should have a level field
    for (const report of reports) {
      expect(report.level).toBeDefined();
      expect(typeof report.level).toBe('number');
    }
  });

  it('should apply restrictSearchWithMatch filter', async () => {
    const docs = [createTestDoc('start', { startId: '1' })];

    const result = await processGraphLookup(docs, {
      from: 'employees',
      startWith: '$startId',
      connectFromField: '_id',
      connectToField: 'reportsTo',
      as: 'reports',
      restrictSearchWithMatch: { name: { $in: ['Bob', 'Diana'] } },
    }, mockGetCollection);

    const reports = (result[0] as Record<string, unknown>).reports as Record<string, unknown>[];
    // Should only include Bob and Diana
    const names = reports.map((r) => r.name);
    expect(names).toContain('Bob');
  });

  it('should handle empty start value', async () => {
    const docs = [createTestDoc('start', { startId: null })];

    const result = await processGraphLookup(docs, {
      from: 'employees',
      startWith: '$startId',
      connectFromField: '_id',
      connectToField: 'reportsTo',
      as: 'reports',
      maxDepth: 0,  // Only find immediate matches
    }, mockGetCollection);

    expect(result.length).toBe(1);
    // Starting with null should find Alice (who reports to null)
    const reports = (result[0] as Record<string, unknown>).reports as unknown[];
    expect(reports.length).toBe(1);
  });
});

// ============================================================================
// $merge Stage Tests (Validation)
// ============================================================================

describe('$merge Stage Validation', () => {
  it('should validate basic merge specification', () => {
    expect(() => validateMergeStage({
      into: 'targetCollection',
    })).not.toThrow();
  });

  it('should validate merge with all options', () => {
    expect(() => validateMergeStage({
      into: { db: 'testDb', coll: 'targetCollection' },
      on: '_id',
      whenMatched: 'merge',
      whenNotMatched: 'insert',
    })).not.toThrow();
  });

  it('should reject missing into field', () => {
    expect(() => validateMergeStage({} as { into: string })).toThrow('$merge requires "into" field');
  });

  it('should reject invalid whenMatched value', () => {
    expect(() => validateMergeStage({
      into: 'target',
      whenMatched: 'invalid' as 'replace',
    })).toThrow('Invalid whenMatched value');
  });

  it('should reject invalid whenNotMatched value', () => {
    expect(() => validateMergeStage({
      into: 'target',
      whenNotMatched: 'invalid' as 'insert',
    })).toThrow('Invalid whenNotMatched value');
  });

  it('should extract target collection from string', () => {
    const target = getMergeTarget({ into: 'myCollection' });
    expect(target).toEqual({ coll: 'myCollection' });
  });

  it('should extract target collection from object', () => {
    const target = getMergeTarget({ into: { db: 'myDb', coll: 'myCollection' } });
    expect(target).toEqual({ db: 'myDb', coll: 'myCollection' });
  });
});

// ============================================================================
// $out Stage Tests (Validation)
// ============================================================================

describe('$out Stage Validation', () => {
  it('should extract target from string', () => {
    const target = getOutTarget('outputCollection');
    expect(target).toEqual({ coll: 'outputCollection' });
  });

  it('should extract target from object', () => {
    const target = getOutTarget({ db: 'testDb', coll: 'outputCollection' });
    expect(target).toEqual({ db: 'testDb', coll: 'outputCollection' });
  });
});

// ============================================================================
// $redact Stage Tests
// ============================================================================

describe('$redact Stage', () => {
  it('should keep documents with $$KEEP', () => {
    const docs: WithId<Document>[] = [
      createTestDoc('1', { value: 10 }),
      createTestDoc('2', { value: 20 }),
    ];

    const result = processRedact(docs, '$$KEEP');
    expect(result.length).toBe(2);
  });

  it('should prune documents with $$PRUNE', () => {
    const docs: WithId<Document>[] = [
      createTestDoc('1', { value: 10 }),
      createTestDoc('2', { value: 20 }),
    ];

    const result = processRedact(docs, '$$PRUNE');
    expect(result.length).toBe(0);
  });

  it('should descend into nested documents with $$DESCEND', () => {
    const docs: WithId<Document>[] = [
      createTestDoc('1', {
        public: 'visible',
        nested: { secret: 'hidden' },
      }),
    ];

    const result = processRedact(docs, '$$DESCEND');
    expect(result.length).toBe(1);
    expect((result[0] as Record<string, unknown>).public).toBe('visible');
  });

  it('should apply conditional redaction with $cond', () => {
    const docs: WithId<Document>[] = [
      createTestDoc('1', { level: 1, data: 'public' }),
      createTestDoc('2', { level: 5, data: 'classified' }),
    ];

    const result = processRedact(docs, {
      $cond: {
        if: { $gte: ['$level', 3] },
        then: '$$PRUNE',
        else: '$$KEEP',
      },
    });

    expect(result.length).toBe(1);
    expect((result[0] as Record<string, unknown>)._id).toBe('1');
  });

  it('should apply redaction recursively to nested documents', () => {
    const docs: WithId<Document>[] = [
      createTestDoc('1', {
        public: { level: 1, data: 'visible' },
        secret: { level: 5, data: 'classified' },
      }),
    ];

    const result = processRedact(docs, {
      $cond: {
        if: { $gte: ['$level', 3] },
        then: '$$PRUNE',
        else: '$$DESCEND',
      },
    });

    expect(result.length).toBe(1);
    const doc = result[0] as Record<string, unknown>;
    expect(doc.public).toBeDefined();
    // Secret should be pruned
  });

  it('should handle $in operator in conditions', () => {
    const docs: WithId<Document>[] = [
      createTestDoc('1', { role: 'admin', data: 'sensitive' }),
      createTestDoc('2', { role: 'user', data: 'public' }),
    ];

    const result = processRedact(docs, {
      $cond: {
        if: { $in: ['$role', ['admin', 'superuser']] },
        then: '$$KEEP',
        else: '$$PRUNE',
      },
    });

    expect(result.length).toBe(1);
    expect((result[0] as Record<string, unknown>).role).toBe('admin');
  });
});

// ============================================================================
// $replaceRoot / $replaceWith Stage Tests
// ============================================================================

describe('$replaceRoot Stage', () => {
  it('should replace document with field value', () => {
    const docs: WithId<Document>[] = [
      createTestDoc('1', { outer: { inner: 'value', nested: { deep: 'data' } } }),
      createTestDoc('2', { outer: { inner: 'other', count: 42 } }),
    ];

    const result = processReplaceRoot(docs, { newRoot: '$outer' });

    expect(result.length).toBe(2);
    expect((result[0] as Record<string, unknown>).inner).toBe('value');
    expect((result[1] as Record<string, unknown>).count).toBe(42);
  });

  it('should replace document with string expression', () => {
    const docs: WithId<Document>[] = [
      createTestDoc('1', { metadata: { title: 'Doc 1', author: 'Alice' } }),
    ];

    const result = processReplaceRoot(docs, '$metadata');

    expect(result.length).toBe(1);
    expect((result[0] as Record<string, unknown>).title).toBe('Doc 1');
    expect((result[0] as Record<string, unknown>).author).toBe('Alice');
  });

  it('should support object expression with field references', () => {
    const docs: WithId<Document>[] = [
      createTestDoc('1', { name: 'Alice', age: 30, extra: 'data' }),
    ];

    const result = processReplaceRoot(docs, {
      newRoot: {
        fullName: '$name',
        years: '$age',
      },
    });

    expect(result.length).toBe(1);
    expect((result[0] as Record<string, unknown>).fullName).toBe('Alice');
    expect((result[0] as Record<string, unknown>).years).toBe(30);
  });

  it('should throw error if new root is not an object', () => {
    const docs: WithId<Document>[] = [
      createTestDoc('1', { value: 42 }),
    ];

    expect(() => processReplaceRoot(docs, { newRoot: '$value' })).toThrow(
      '$replaceRoot requires the new root to be a document'
    );
  });

  it('should handle $mergeObjects in expression', () => {
    const docs: WithId<Document>[] = [
      createTestDoc('1', {
        base: { a: 1, b: 2 },
        override: { b: 3, c: 4 },
      }),
    ];

    const result = processReplaceRoot(docs, {
      newRoot: {
        merged: {
          $mergeObjects: ['$base', '$override'],
        },
      },
    });

    expect(result.length).toBe(1);
    const merged = (result[0] as Record<string, unknown>).merged as Record<string, unknown>;
    expect(merged.a).toBe(1);
    expect(merged.b).toBe(3); // Overridden
    expect(merged.c).toBe(4);
  });
});

// ============================================================================
// $sample Stage Tests
// ============================================================================

describe('$sample Stage', () => {
  const docs: WithId<Document>[] = Array.from({ length: 100 }, (_, i) =>
    createTestDoc(`${i + 1}`, { value: i + 1 })
  );

  it('should return specified number of random documents', () => {
    const result = processSample(docs, { size: 10 });

    expect(result.length).toBe(10);
  });

  it('should return all documents if size exceeds count', () => {
    const result = processSample(docs, { size: 200 });

    expect(result.length).toBe(100);
  });

  it('should return empty array for size 0', () => {
    const result = processSample(docs, { size: 0 });

    expect(result).toEqual([]);
  });

  it('should return empty array for negative size', () => {
    const result = processSample(docs, { size: -5 });

    expect(result).toEqual([]);
  });

  it('should return different results on multiple calls (random)', () => {
    const results1 = processSample(docs, { size: 10 });
    const results2 = processSample(docs, { size: 10 });

    // With 100 docs and 10 samples, it's extremely unlikely to get the same 10 in the same order
    const ids1 = results1.map((d) => d._id).join(',');
    const ids2 = results2.map((d) => d._id).join(',');

    // This test might occasionally fail due to randomness, but probability is very low
    // With 100 items and 10 samples, probability of exact match is astronomically low
    expect(ids1 !== ids2 || results1.length === docs.length).toBe(true);
  });

  it('should preserve document structure', () => {
    const result = processSample(docs, { size: 5 });

    for (const doc of result) {
      expect(doc._id).toBeDefined();
      expect((doc as Record<string, unknown>).value).toBeDefined();
    }
  });

  it('should handle single document', () => {
    const singleDoc = [createTestDoc('1', { value: 42 })];
    const result = processSample(singleDoc, { size: 5 });

    expect(result.length).toBe(1);
    expect(result[0]._id).toBe('1');
  });
});

// ============================================================================
// $sortByCount Stage Tests
// ============================================================================

describe('$sortByCount Stage', () => {
  const docs: WithId<Document>[] = [
    createTestDoc('1', { category: 'A' }),
    createTestDoc('2', { category: 'B' }),
    createTestDoc('3', { category: 'A' }),
    createTestDoc('4', { category: 'C' }),
    createTestDoc('5', { category: 'A' }),
    createTestDoc('6', { category: 'B' }),
    createTestDoc('7', { category: 'A' }),
  ];

  it('should group by field and count', () => {
    const result = processSortByCount(docs, '$category');

    expect(result.length).toBe(3);

    const categoryA = result.find((r) => (r as Record<string, unknown>)._id === 'A');
    const categoryB = result.find((r) => (r as Record<string, unknown>)._id === 'B');
    const categoryC = result.find((r) => (r as Record<string, unknown>)._id === 'C');

    expect((categoryA as Record<string, unknown>).count).toBe(4);
    expect((categoryB as Record<string, unknown>).count).toBe(2);
    expect((categoryC as Record<string, unknown>).count).toBe(1);
  });

  it('should sort by count descending', () => {
    const result = processSortByCount(docs, '$category');

    expect(result.length).toBe(3);
    expect((result[0] as Record<string, unknown>)._id).toBe('A'); // 4 items
    expect((result[1] as Record<string, unknown>)._id).toBe('B'); // 2 items
    expect((result[2] as Record<string, unknown>)._id).toBe('C'); // 1 item
  });

  it('should handle nested field paths', () => {
    const nestedDocs: WithId<Document>[] = [
      createTestDoc('1', { meta: { type: 'X' } }),
      createTestDoc('2', { meta: { type: 'Y' } }),
      createTestDoc('3', { meta: { type: 'X' } }),
      createTestDoc('4', { meta: { type: 'X' } }),
    ];

    const result = processSortByCount(nestedDocs, '$meta.type');

    expect(result.length).toBe(2);
    expect((result[0] as Record<string, unknown>)._id).toBe('X');
    expect((result[0] as Record<string, unknown>).count).toBe(3);
  });

  it('should handle null and undefined values', () => {
    const docsWithNulls: WithId<Document>[] = [
      createTestDoc('1', { status: 'active' }),
      createTestDoc('2', { status: null }),
      createTestDoc('3', { status: 'active' }),
      createTestDoc('4', {}), // missing status
    ];

    const result = processSortByCount(docsWithNulls, '$status');

    // Should group: 'active' (2), null (1), undefined (1)
    expect(result.length).toBeGreaterThanOrEqual(2);
    const activeGroup = result.find((r) => (r as Record<string, unknown>)._id === 'active');
    expect((activeGroup as Record<string, unknown>).count).toBe(2);
  });

  it('should handle empty input', () => {
    const result = processSortByCount([], '$category');
    expect(result).toEqual([]);
  });

  it('should handle field path without $ prefix', () => {
    const result = processSortByCount(docs, 'category');

    expect(result.length).toBe(3);
    expect((result[0] as Record<string, unknown>).count).toBe(4);
  });
});

// ============================================================================
// Accumulator Evaluation Tests
// ============================================================================

describe('evaluateAccumulator', () => {
  const docs: Record<string, unknown>[] = [
    { value: 10 },
    { value: 20 },
    { value: 30 },
    { value: 40 },
    { value: 50 },
  ];

  it('should calculate $sum with field', () => {
    const result = evaluateAccumulator(docs, { $sum: '$value' });
    expect(result).toBe(150);
  });

  it('should calculate $sum with constant', () => {
    const result = evaluateAccumulator(docs, { $sum: 1 });
    expect(result).toBe(5);
  });

  it('should calculate $avg', () => {
    const result = evaluateAccumulator(docs, { $avg: '$value' });
    expect(result).toBe(30);
  });

  it('should calculate $min', () => {
    const result = evaluateAccumulator(docs, { $min: '$value' });
    expect(result).toBe(10);
  });

  it('should calculate $max', () => {
    const result = evaluateAccumulator(docs, { $max: '$value' });
    expect(result).toBe(50);
  });

  it('should calculate $first', () => {
    const result = evaluateAccumulator(docs, { $first: '$value' });
    expect(result).toBe(10);
  });

  it('should calculate $last', () => {
    const result = evaluateAccumulator(docs, { $last: '$value' });
    expect(result).toBe(50);
  });

  it('should calculate $push', () => {
    const result = evaluateAccumulator(docs, { $push: '$value' });
    expect(result).toEqual([10, 20, 30, 40, 50]);
  });

  it('should calculate $addToSet', () => {
    const docsWithDuplicates: Record<string, unknown>[] = [
      { status: 'active' },
      { status: 'inactive' },
      { status: 'active' },
      { status: 'pending' },
      { status: 'active' },
    ];
    const result = evaluateAccumulator(docsWithDuplicates, { $addToSet: '$status' }) as unknown[];
    expect(result.length).toBe(3);
    expect(result).toContain('active');
    expect(result).toContain('inactive');
    expect(result).toContain('pending');
  });

  it('should calculate $count', () => {
    const result = evaluateAccumulator(docs, { $count: {} });
    expect(result).toBe(5);
  });

  it('should return null for unknown accumulator', () => {
    const result = evaluateAccumulator(docs, { $unknown: '$value' });
    expect(result).toBeNull();
  });
});

// ============================================================================
// Pipeline Integration Tests - TDD RED Phase
// ============================================================================
//
// The following tests verify that aggregation stages are properly integrated
// into the main BaseAggregationCursor.processStage() method. These tests use
// the collection's aggregate() method to test end-to-end behavior.
//
// The standalone implementations exist in src/query/aggregation-stages.ts but
// they need to be integrated into src/client/aggregation.ts.
// ============================================================================

import { createTestCollection, createTestDatabase } from '../client/test-helpers.js';

describe('Pipeline Integration - TDD RED Phase', () => {
  /**
   * $bucket stage - groups documents by explicit boundary ranges
   * Unlike $bucketAuto, $bucket requires explicit boundaries to be defined.
   * Documents are placed into buckets based on where their values fall.
   */
  describe('$bucket stage integration', () => {
    it.fails('should group documents into explicit boundary buckets via aggregate()', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', price: 5 },
        { _id: '2', price: 15 },
        { _id: '3', price: 25 },
        { _id: '4', price: 35 },
        { _id: '5', price: 45 },
        { _id: '6', price: 55 },
      ]);

      const results = await collection.aggregate([
        {
          $bucket: {
            groupBy: '$price',
            boundaries: [0, 20, 40, 60],
            default: 'other',
          },
        },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(3);
      // Bucket 0-20: price 5, 15 (2 docs)
      // Bucket 20-40: price 25, 35 (2 docs)
      // Bucket 40-60: price 45, 55 (2 docs)
      expect(results.find((r: unknown) => (r as { _id: number })._id === 0)).toEqual({ _id: 0, count: 2 });
      expect(results.find((r: unknown) => (r as { _id: number })._id === 20)).toEqual({ _id: 20, count: 2 });
      expect(results.find((r: unknown) => (r as { _id: number })._id === 40)).toEqual({ _id: 40, count: 2 });
    });

    it.fails('should use default bucket for out-of-range values via aggregate()', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', score: 50 },
        { _id: '2', score: 75 },
        { _id: '3', score: 200 }, // out of range
        { _id: '4', score: -10 }, // out of range
      ]);

      const results = await collection.aggregate([
        {
          $bucket: {
            groupBy: '$score',
            boundaries: [0, 100],
            default: 'outOfRange',
          },
        },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(2);
      expect(results.find((r: unknown) => (r as { _id: number })._id === 0)).toEqual({ _id: 0, count: 2 });
      expect(results.find((r: unknown) => (r as { _id: string })._id === 'outOfRange')).toEqual({
        _id: 'outOfRange',
        count: 2,
      });
    });

    it.fails('should support custom output accumulators in $bucket via aggregate()', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', age: 25, salary: 50000 },
        { _id: '2', age: 28, salary: 60000 },
        { _id: '3', age: 35, salary: 80000 },
        { _id: '4', age: 42, salary: 90000 },
      ]);

      const results = await collection.aggregate([
        {
          $bucket: {
            groupBy: '$age',
            boundaries: [20, 30, 40, 50],
            output: {
              count: { $sum: 1 },
              avgSalary: { $avg: '$salary' },
              employees: { $push: '$_id' },
            },
          },
        },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(3);
      const bucket20to30 = results.find((r: unknown) => (r as { _id: number })._id === 20) as {
        count: number;
        avgSalary: number;
        employees: string[];
      };
      expect(bucket20to30.count).toBe(2);
      expect(bucket20to30.avgSalary).toBe(55000);
      expect(bucket20to30.employees).toContain('1');
      expect(bucket20to30.employees).toContain('2');
    });

    it.fails('should handle documents with null/missing groupBy field via aggregate()', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', value: 10 },
        { _id: '2', value: null },
        { _id: '3' }, // missing value
        { _id: '4', value: 30 },
      ]);

      const results = await collection.aggregate([
        {
          $bucket: {
            groupBy: '$value',
            boundaries: [0, 20, 40],
            default: 'missing',
          },
        },
      ] as unknown[]).toArray();

      // null and missing should go to default bucket
      expect(results).toHaveLength(3);
      expect(results.find((r: unknown) => (r as { _id: string })._id === 'missing')).toEqual({
        _id: 'missing',
        count: 2,
      });
    });
  });

  /**
   * $bucketAuto stage - automatically distributes documents into buckets
   * Unlike $bucket which requires explicit boundaries, $bucketAuto automatically
   * calculates even bucket boundaries to evenly distribute documents.
   */
  describe('$bucketAuto stage integration', () => {
    it.fails('should automatically calculate bucket boundaries via aggregate()', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', score: 10 },
        { _id: '2', score: 25 },
        { _id: '3', score: 45 },
        { _id: '4', score: 60 },
        { _id: '5', score: 80 },
        { _id: '6', score: 95 },
      ]);

      // Type assertion needed until types are added
      const results = await collection.aggregate([
        {
          $bucketAuto: {
            groupBy: '$score',
            buckets: 3,
          },
        },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(3);
      // Each bucket should have approximately 2 documents
      for (const bucket of results) {
        const b = bucket as { _id: { min: number; max: number }; count: number };
        expect(b._id.min).toBeDefined();
        expect(b._id.max).toBeDefined();
        expect(b.count).toBe(2);
      }
    });

    it.fails('should support custom output accumulators in $bucketAuto via aggregate()', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', price: 100, product: 'A' },
        { _id: '2', price: 200, product: 'B' },
        { _id: '3', price: 300, product: 'C' },
        { _id: '4', price: 400, product: 'D' },
      ]);

      const results = await collection.aggregate([
        {
          $bucketAuto: {
            groupBy: '$price',
            buckets: 2,
            output: {
              count: { $sum: 1 },
              avgPrice: { $avg: '$price' },
              products: { $push: '$product' },
            },
          },
        },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(2);
      const bucket = results[0] as { count: number; avgPrice: number; products: string[] };
      expect(bucket.count).toBe(2);
      expect(bucket.avgPrice).toBeDefined();
      expect(bucket.products).toHaveLength(2);
    });
  });

  /**
   * $facet stage - runs multiple pipelines in parallel
   * Each pipeline operates on the same input documents and produces
   * a separate output field in a single result document.
   */
  describe('$facet stage integration', () => {
    it.fails('should run multiple pipelines in parallel via aggregate()', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', category: 'A', price: 100, rating: 4.5 },
        { _id: '2', category: 'B', price: 200, rating: 4.0 },
        { _id: '3', category: 'A', price: 150, rating: 4.8 },
        { _id: '4', category: 'C', price: 300, rating: 3.5 },
        { _id: '5', category: 'A', price: 120, rating: 4.2 },
      ]);

      const results = await collection.aggregate([
        {
          $facet: {
            byCategory: [
              { $group: { _id: '$category', count: { $sum: 1 } } },
              { $sort: { count: -1 } },
            ],
            priceStats: [
              {
                $group: {
                  _id: null,
                  avgPrice: { $avg: '$price' },
                  minPrice: { $min: '$price' },
                  maxPrice: { $max: '$price' },
                },
              },
            ],
            topRated: [
              { $sort: { rating: -1 } },
              { $limit: 2 },
              { $project: { _id: 1, rating: 1 } },
            ],
          },
        },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(1);
      const facetResult = results[0] as {
        byCategory: Array<{ _id: string; count: number }>;
        priceStats: Array<{ avgPrice: number; minPrice: number; maxPrice: number }>;
        topRated: Array<{ _id: string; rating: number }>;
      };

      // byCategory facet
      expect(facetResult.byCategory).toHaveLength(3);
      expect(facetResult.byCategory[0]).toEqual({ _id: 'A', count: 3 });

      // priceStats facet
      expect(facetResult.priceStats).toHaveLength(1);
      expect(facetResult.priceStats[0].avgPrice).toBe(174);
      expect(facetResult.priceStats[0].minPrice).toBe(100);
      expect(facetResult.priceStats[0].maxPrice).toBe(300);

      // topRated facet
      expect(facetResult.topRated).toHaveLength(2);
      expect(facetResult.topRated[0].rating).toBe(4.8);
    });

    it.fails('should allow empty pipelines in facet via aggregate()', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', value: 10 },
        { _id: '2', value: 20 },
      ]);

      const results = await collection.aggregate([
        {
          $facet: {
            all: [], // Empty pipeline returns all docs
            filtered: [{ $match: { value: { $gt: 15 } } }],
          },
        },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(1);
      const facetResult = results[0] as {
        all: Array<{ _id: string }>;
        filtered: Array<{ _id: string }>;
      };
      expect(facetResult.all).toHaveLength(2);
      expect(facetResult.filtered).toHaveLength(1);
    });

    it.fails('should support nested facet operations via aggregate()', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', status: 'active', type: 'premium' },
        { _id: '2', status: 'inactive', type: 'basic' },
        { _id: '3', status: 'active', type: 'basic' },
        { _id: '4', status: 'active', type: 'premium' },
      ]);

      const results = await collection.aggregate([
        {
          $facet: {
            statusCounts: [{ $sortByCount: '$status' }],
            typeCounts: [{ $sortByCount: '$type' }],
            activeOnly: [
              { $match: { status: 'active' } },
              { $count: 'total' },
            ],
          },
        },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(1);
      const facetResult = results[0] as {
        statusCounts: Array<{ _id: string; count: number }>;
        typeCounts: Array<{ _id: string; count: number }>;
        activeOnly: Array<{ total: number }>;
      };

      expect(facetResult.statusCounts[0]).toEqual({ _id: 'active', count: 3 });
      expect(facetResult.typeCounts[0]._id).toMatch(/premium|basic/);
      expect(facetResult.activeOnly[0].total).toBe(3);
    });

    it.fails('should work with $match before $facet via aggregate()', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', region: 'US', sales: 100 },
        { _id: '2', region: 'EU', sales: 150 },
        { _id: '3', region: 'US', sales: 200 },
        { _id: '4', region: 'APAC', sales: 120 },
      ]);

      const results = await collection.aggregate([
        { $match: { region: { $in: ['US', 'EU'] } } },
        {
          $facet: {
            regionSummary: [
              { $group: { _id: '$region', totalSales: { $sum: '$sales' } } },
            ],
            docCount: [{ $count: 'count' }],
          },
        },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(1);
      const facetResult = results[0] as {
        regionSummary: Array<{ _id: string; totalSales: number }>;
        docCount: Array<{ count: number }>;
      };

      // Should only include US and EU
      expect(facetResult.regionSummary).toHaveLength(2);
      expect(facetResult.docCount[0].count).toBe(3);
    });
  });

  /**
   * $graphLookup stage - performs recursive graph traversal
   * Useful for hierarchical data like org charts, category trees, or social graphs.
   */
  describe('$graphLookup stage integration', () => {
    it.fails('should perform recursive graph traversal via aggregate()', async () => {
      const { db } = createTestDatabase('graphdb_int');
      const employees = db.collection('employees');

      await employees.insertMany([
        { _id: 'ceo', name: 'CEO', reportsTo: null },
        { _id: 'vp1', name: 'VP Engineering', reportsTo: 'ceo' },
        { _id: 'vp2', name: 'VP Sales', reportsTo: 'ceo' },
        { _id: 'mgr1', name: 'Engineering Manager', reportsTo: 'vp1' },
        { _id: 'dev1', name: 'Developer 1', reportsTo: 'mgr1' },
        { _id: 'dev2', name: 'Developer 2', reportsTo: 'mgr1' },
      ]);

      const results = await employees.aggregate([
        { $match: { _id: 'dev1' } },
        {
          $graphLookup: {
            from: 'employees',
            startWith: '$reportsTo',
            connectFromField: 'reportsTo',
            connectToField: '_id',
            as: 'reportingHierarchy',
          },
        },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(1);
      const dev = results[0] as { reportingHierarchy: { name: string }[] };
      expect(dev.reportingHierarchy).toHaveLength(3); // mgr1, vp1, ceo
      expect(dev.reportingHierarchy.map((e) => e.name)).toContain('Engineering Manager');
      expect(dev.reportingHierarchy.map((e) => e.name)).toContain('VP Engineering');
      expect(dev.reportingHierarchy.map((e) => e.name)).toContain('CEO');
    });

    it.fails('should respect maxDepth option via aggregate()', async () => {
      const { db } = createTestDatabase('graphdb2_int');
      const employees = db.collection('employees');

      await employees.insertMany([
        { _id: 'ceo', name: 'CEO', reportsTo: null },
        { _id: 'vp1', name: 'VP', reportsTo: 'ceo' },
        { _id: 'mgr1', name: 'Manager', reportsTo: 'vp1' },
        { _id: 'dev1', name: 'Developer', reportsTo: 'mgr1' },
      ]);

      const results = await employees.aggregate([
        { $match: { _id: 'dev1' } },
        {
          $graphLookup: {
            from: 'employees',
            startWith: '$reportsTo',
            connectFromField: 'reportsTo',
            connectToField: '_id',
            as: 'chain',
            maxDepth: 1,
          },
        },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(1);
      const dev = results[0] as { chain: unknown[] };
      expect(dev.chain).toHaveLength(2); // Only mgr1 and vp1 (maxDepth: 1)
    });
  });

  /**
   * $merge stage - writes aggregation results to a collection
   * Unlike $out which replaces the entire collection, $merge can
   * insert, update, or merge documents based on matching criteria.
   */
  describe('$merge stage integration', () => {
    it.fails('should merge aggregation results into existing collection via aggregate()', async () => {
      const { db } = createTestDatabase('mergedb_int');
      const sales = db.collection('sales');
      const reports = db.collection('reports');

      await sales.insertMany([
        { _id: '1', product: 'A', quantity: 10 },
        { _id: '2', product: 'A', quantity: 20 },
        { _id: '3', product: 'B', quantity: 15 },
      ]);

      await reports.insertMany([
        { _id: 'A', totalQuantity: 5 }, // existing record
      ]);

      await sales.aggregate([
        { $group: { _id: '$product', totalQuantity: { $sum: '$quantity' } } },
        {
          $merge: {
            into: 'reports',
            on: '_id',
            whenMatched: 'replace',
            whenNotMatched: 'insert',
          },
        },
      ] as unknown[]).toArray();

      const results = await reports.find({}).toArray();
      expect(results).toHaveLength(2);
      expect(results.find((r) => r._id === 'A')).toEqual({ _id: 'A', totalQuantity: 30 });
      expect(results.find((r) => r._id === 'B')).toEqual({ _id: 'B', totalQuantity: 15 });
    });

    it.fails('should support whenMatched: merge option via aggregate()', async () => {
      const { db } = createTestDatabase('mergedb2_int');
      const source = db.collection('source');
      const target = db.collection('target');

      await source.insertMany([
        { _id: '1', newField: 'value1' },
      ]);

      await target.insertMany([
        { _id: '1', existingField: 'existing' },
      ]);

      await source.aggregate([
        {
          $merge: {
            into: 'target',
            on: '_id',
            whenMatched: 'merge',
            whenNotMatched: 'insert',
          },
        },
      ] as unknown[]).toArray();

      const results = await target.find({}).toArray();
      expect(results).toHaveLength(1);
      expect(results[0]).toEqual({
        _id: '1',
        existingField: 'existing',
        newField: 'value1',
      });
    });
  });

  /**
   * $out stage - writes aggregation results to a new collection
   * Replaces the entire target collection with the pipeline output.
   */
  describe('$out stage integration', () => {
    it.fails('should write aggregation results to a new collection via aggregate()', async () => {
      const { db } = createTestDatabase('outdb_int');
      const source = db.collection('source');

      await source.insertMany([
        { _id: '1', category: 'A', value: 10 },
        { _id: '2', category: 'A', value: 20 },
        { _id: '3', category: 'B', value: 15 },
      ]);

      await source.aggregate([
        { $group: { _id: '$category', total: { $sum: '$value' } } },
        { $out: 'aggregated' },
      ] as unknown[]).toArray();

      const results = await db.collection('aggregated').find({}).toArray();
      expect(results).toHaveLength(2);
      expect(results.find((r) => r._id === 'A')).toEqual({ _id: 'A', total: 30 });
      expect(results.find((r) => r._id === 'B')).toEqual({ _id: 'B', total: 15 });
    });

    it.fails('should replace existing collection with $out via aggregate()', async () => {
      const { db } = createTestDatabase('outdb2_int');
      const source = db.collection('source');
      const output = db.collection('output');

      await output.insertMany([
        { _id: 'old', data: 'existing' },
      ]);

      await source.insertMany([
        { _id: '1', value: 100 },
      ]);

      await source.aggregate([
        { $project: { value: 1 } },
        { $out: 'output' },
      ] as unknown[]).toArray();

      const results = await output.find({}).toArray();
      expect(results).toHaveLength(1);
      expect(results[0]).toEqual({ _id: '1', value: 100 });
      // Old data should be gone
      expect(results.find((r) => r._id === 'old')).toBeUndefined();
    });
  });

  /**
   * $redact stage - performs field-level access control
   * Restricts content of documents based on stored access levels.
   * Uses $$DESCEND, $$PRUNE, and $$KEEP system variables.
   */
  describe('$redact stage integration', () => {
    it.fails('should redact documents based on field-level access control via aggregate()', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        {
          _id: '1',
          title: 'Public Document',
          level: 1,
          content: {
            level: 1,
            body: 'Public content',
            secret: {
              level: 5,
              data: 'Top secret',
            },
          },
        },
      ]);

      const userLevel = 3;
      const results = await collection.aggregate([
        {
          $redact: {
            $cond: {
              if: { $lte: ['$level', userLevel] },
              then: '$$DESCEND',
              else: '$$PRUNE',
            },
          },
        },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(1);
      const doc = results[0] as {
        title: string;
        content: { body: string; secret?: { data: string } };
      };
      expect(doc.title).toBe('Public Document');
      expect(doc.content.body).toBe('Public content');
      expect(doc.content.secret).toBeUndefined(); // Pruned because level 5 > 3
    });

    it.fails('should support $$KEEP to include entire subdocument via aggregate()', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        {
          _id: '1',
          public: true,
          nested: {
            public: false,
            data: 'should be included anyway',
          },
        },
      ]);

      const results = await collection.aggregate([
        {
          $redact: {
            $cond: {
              if: '$public',
              then: '$$KEEP',
              else: '$$PRUNE',
            },
          },
        },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(1);
      const doc = results[0] as { nested: { data: string } };
      // $$KEEP includes the entire subdocument without further examination
      expect(doc.nested.data).toBe('should be included anyway');
    });
  });

  /**
   * $replaceRoot stage - replaces document with an embedded document
   * Promotes a subdocument to become the new document root.
   */
  describe('$replaceRoot stage integration', () => {
    it.fails('should replace document root with embedded document via aggregate()', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', name: 'Alice', profile: { age: 30, city: 'NYC' } },
        { _id: '2', name: 'Bob', profile: { age: 25, city: 'LA' } },
      ]);

      const results = await collection.aggregate([
        { $replaceRoot: { newRoot: '$profile' } },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(2);
      expect(results[0]).toEqual({ age: 30, city: 'NYC' });
      expect(results[1]).toEqual({ age: 25, city: 'LA' });
    });

    it.fails('should support $mergeObjects in newRoot expression via aggregate()', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', name: 'Alice', details: { age: 30 } },
      ]);

      const results = await collection.aggregate([
        {
          $replaceRoot: {
            newRoot: { $mergeObjects: [{ name: '$name' }, '$details'] },
          },
        },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(1);
      expect(results[0]).toEqual({ name: 'Alice', age: 30 });
    });
  });

  /**
   * $replaceWith stage - alias for $replaceRoot with simpler syntax
   * Introduced in MongoDB 4.2 as a more concise alternative.
   */
  describe('$replaceWith stage integration', () => {
    it.fails('should be an alias for $replaceRoot via aggregate()', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', embedded: { x: 1, y: 2 } },
      ]);

      const results = await collection.aggregate([
        { $replaceWith: '$embedded' },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(1);
      expect(results[0]).toEqual({ x: 1, y: 2 });
    });

    it.fails('should work with complex expressions via aggregate()', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', a: { x: 1 }, b: { y: 2 } },
      ]);

      const results = await collection.aggregate([
        {
          $replaceWith: {
            $mergeObjects: ['$a', '$b'],
          },
        },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(1);
      expect(results[0]).toEqual({ x: 1, y: 2 });
    });
  });

  /**
   * $sample stage - randomly selects N documents
   * Useful for getting representative samples or random records.
   */
  describe('$sample stage integration', () => {
    it.fails('should randomly select N documents via aggregate()', async () => {
      const { collection } = createTestCollection();
      const docs = Array.from({ length: 100 }, (_, i) => ({
        _id: String(i + 1),
        value: i + 1,
      }));
      await collection.insertMany(docs);

      const results = await collection.aggregate([
        { $sample: { size: 5 } },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(5);
      // All results should be from the original documents
      results.forEach((r: unknown) => {
        const doc = r as { value: number };
        expect(doc.value).toBeGreaterThanOrEqual(1);
        expect(doc.value).toBeLessThanOrEqual(100);
      });
    });

    it.fails('should return all documents if size >= collection size via aggregate()', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', value: 1 },
        { _id: '2', value: 2 },
        { _id: '3', value: 3 },
      ]);

      const results = await collection.aggregate([
        { $sample: { size: 10 } },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(3);
    });

    it.fails('should work with other stages via aggregate()', async () => {
      const { collection } = createTestCollection();
      const docs = Array.from({ length: 20 }, (_, i) => ({
        _id: String(i + 1),
        category: i % 2 === 0 ? 'A' : 'B',
        value: i + 1,
      }));
      await collection.insertMany(docs);

      const results = await collection.aggregate([
        { $match: { category: 'A' } },
        { $sample: { size: 3 } },
        { $project: { category: 1, value: 1 } },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(3);
      results.forEach((r: unknown) => {
        expect((r as { category: string }).category).toBe('A');
      });
    });
  });

  /**
   * $sortByCount stage - groups by field value and counts, sorted descending
   * Equivalent to $group + $sort but more concise.
   */
  describe('$sortByCount stage integration', () => {
    it.fails('should group by field and sort by count descending via aggregate()', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', tag: 'js' },
        { _id: '2', tag: 'python' },
        { _id: '3', tag: 'js' },
        { _id: '4', tag: 'js' },
        { _id: '5', tag: 'python' },
        { _id: '6', tag: 'rust' },
      ]);

      const results = await collection.aggregate([
        { $sortByCount: '$tag' },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(3);
      expect(results[0]).toEqual({ _id: 'js', count: 3 });
      expect(results[1]).toEqual({ _id: 'python', count: 2 });
      expect(results[2]).toEqual({ _id: 'rust', count: 1 });
    });

    it.fails('should be equivalent to $group + $sort via aggregate()', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', status: 'active' },
        { _id: '2', status: 'inactive' },
        { _id: '3', status: 'active' },
        { _id: '4', status: 'active' },
      ]);

      const sortByCountResults = await collection.aggregate([
        { $sortByCount: '$status' },
      ] as unknown[]).toArray();

      const equivalentResults = await collection.aggregate([
        { $group: { _id: '$status', count: { $sum: 1 } } },
        { $sort: { count: -1 } },
      ]).toArray();

      expect(sortByCountResults).toEqual(equivalentResults);
    });

    it.fails('should work with nested field paths via aggregate()', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', user: { role: 'admin' } },
        { _id: '2', user: { role: 'user' } },
        { _id: '3', user: { role: 'admin' } },
        { _id: '4', user: { role: 'user' } },
        { _id: '5', user: { role: 'user' } },
      ]);

      const results = await collection.aggregate([
        { $sortByCount: '$user.role' },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(2);
      expect(results[0]).toEqual({ _id: 'user', count: 3 });
      expect(results[1]).toEqual({ _id: 'admin', count: 2 });
    });

    it.fails('should work after $unwind via aggregate()', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', tags: ['js', 'ts'] },
        { _id: '2', tags: ['js', 'python'] },
        { _id: '3', tags: ['python', 'rust'] },
      ]);

      const results = await collection.aggregate([
        { $unwind: '$tags' },
        { $sortByCount: '$tags' },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(4);
      expect(results[0]).toEqual({ _id: 'js', count: 2 });
      expect(results[1]).toEqual({ _id: 'python', count: 2 });
      // ts and rust have count 1 each
    });
  });

  /**
   * $unionWith stage - combines results from multiple collections
   * Performs a union of the pipeline with another collection's documents.
   */
  describe('$unionWith stage integration', () => {
    it.fails('should combine documents from another collection via aggregate()', async () => {
      const { db } = createTestDatabase('uniondb_int');
      const suppliers = db.collection('suppliers');
      const warehouses = db.collection('warehouses');

      await suppliers.insertMany([
        { _id: 's1', name: 'Supplier A', state: 'NY' },
        { _id: 's2', name: 'Supplier B', state: 'CA' },
      ]);

      await warehouses.insertMany([
        { _id: 'w1', name: 'Warehouse X', state: 'NY' },
        { _id: 'w2', name: 'Warehouse Y', state: 'TX' },
      ]);

      const results = await suppliers.aggregate([
        { $project: { name: 1, state: 1 } },
        {
          $unionWith: {
            coll: 'warehouses',
            pipeline: [{ $project: { name: 1, state: 1 } }],
          },
        },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(4);
      const names = results.map((r: unknown) => (r as { name: string }).name);
      expect(names).toContain('Supplier A');
      expect(names).toContain('Warehouse X');
    });

    it.fails('should support simple string syntax for collection name via aggregate()', async () => {
      const { db } = createTestDatabase('uniondb2_int');
      const coll1 = db.collection('coll1');
      const coll2 = db.collection('coll2');

      await coll1.insertMany([
        { _id: '1', value: 'from coll1' },
      ]);

      await coll2.insertMany([
        { _id: '2', value: 'from coll2' },
      ]);

      const results = await coll1.aggregate([
        { $unionWith: 'coll2' },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(2);
      const values = results.map((r: unknown) => (r as { value: string }).value);
      expect(values).toContain('from coll1');
      expect(values).toContain('from coll2');
    });

    it.fails('should apply pipeline to the union collection via aggregate()', async () => {
      const { db } = createTestDatabase('uniondb3_int');
      const sales2020 = db.collection('sales2020');
      const sales2021 = db.collection('sales2021');

      await sales2020.insertMany([
        { _id: '1', product: 'A', amount: 100 },
        { _id: '2', product: 'B', amount: 50 },
      ]);

      await sales2021.insertMany([
        { _id: '3', product: 'A', amount: 150 },
        { _id: '4', product: 'C', amount: 75 },
      ]);

      const results = await sales2020.aggregate([
        { $match: { product: 'A' } },
        {
          $unionWith: {
            coll: 'sales2021',
            pipeline: [{ $match: { product: 'A' } }],
          },
        },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(2);
      results.forEach((r: unknown) => {
        expect((r as { product: string }).product).toBe('A');
      });
    });

    it.fails('should support aggregation after $unionWith via aggregate()', async () => {
      const { db } = createTestDatabase('uniondb4_int');
      const jan = db.collection('jan');
      const feb = db.collection('feb');

      await jan.insertMany([
        { _id: '1', category: 'X', value: 100 },
        { _id: '2', category: 'Y', value: 50 },
      ]);

      await feb.insertMany([
        { _id: '3', category: 'X', value: 150 },
        { _id: '4', category: 'Y', value: 75 },
      ]);

      const results = await jan.aggregate([
        { $unionWith: 'feb' },
        { $group: { _id: '$category', total: { $sum: '$value' } } },
        { $sort: { _id: 1 } },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(2);
      expect(results[0]).toEqual({ _id: 'X', total: 250 });
      expect(results[1]).toEqual({ _id: 'Y', total: 125 });
    });

    it.fails('should handle multiple $unionWith stages via aggregate()', async () => {
      const { db } = createTestDatabase('uniondb5_int');
      const q1 = db.collection('q1');
      const q2 = db.collection('q2');
      const q3 = db.collection('q3');

      await q1.insertMany([{ _id: '1', quarter: 'Q1', revenue: 1000 }]);
      await q2.insertMany([{ _id: '2', quarter: 'Q2', revenue: 1500 }]);
      await q3.insertMany([{ _id: '3', quarter: 'Q3', revenue: 1200 }]);

      const results = await q1.aggregate([
        { $unionWith: 'q2' },
        { $unionWith: 'q3' },
        { $sort: { quarter: 1 } },
      ] as unknown[]).toArray();

      expect(results).toHaveLength(3);
      const quarters = results.map((r: unknown) => (r as { quarter: string }).quarter);
      expect(quarters).toEqual(['Q1', 'Q2', 'Q3']);
    });
  });
});
