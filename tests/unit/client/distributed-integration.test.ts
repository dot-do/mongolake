/**
 * Distributed Aggregation Integration Tests
 *
 * Tests for Collection.aggregate() with distributed execution mode enabled.
 * Verifies that the integration between Collection, AggregationCursor,
 * and DistributedAggregationPlanner works correctly.
 */

import { describe, it, expect } from 'vitest';
import { createTestCollection, createTestDatabase } from './test-helpers.js';

describe('Collection.aggregate() with distributed option', () => {
  describe('basic distributed execution', () => {
    it('should produce same results as standard execution for simple $group', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', category: 'A', amount: 10 },
        { _id: '2', category: 'B', amount: 20 },
        { _id: '3', category: 'A', amount: 30 },
        { _id: '4', category: 'A', amount: 40 },
      ]);

      // Standard execution
      const standardResults = await collection.aggregate([
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
        { $sort: { _id: 1 } },
      ]).toArray();

      // Distributed execution
      const distributedResults = await collection.aggregate([
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
        { $sort: { _id: 1 } },
      ], { distributed: true }).toArray();

      expect(distributedResults).toEqual(standardResults);
    });

    it('should produce same results for $match before $group', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', status: 'active', category: 'A', amount: 10 },
        { _id: '2', status: 'inactive', category: 'A', amount: 100 },
        { _id: '3', status: 'active', category: 'A', amount: 20 },
        { _id: '4', status: 'active', category: 'B', amount: 30 },
      ]);

      const pipeline = [
        { $match: { status: 'active' } },
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
        { $sort: { _id: 1 } },
      ];

      const standardResults = await collection.aggregate(pipeline).toArray();
      const distributedResults = await collection.aggregate(pipeline, { distributed: true }).toArray();

      expect(distributedResults).toEqual(standardResults);
    });

    it('should produce same results for $group with $avg', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', category: 'A', score: 80 },
        { _id: '2', category: 'A', score: 90 },
        { _id: '3', category: 'B', score: 70 },
        { _id: '4', category: 'A', score: 100 },
      ]);

      const pipeline = [
        { $group: { _id: '$category', avgScore: { $avg: '$score' } } },
        { $sort: { _id: 1 } },
      ];

      const standardResults = await collection.aggregate(pipeline).toArray();
      const distributedResults = await collection.aggregate(pipeline, { distributed: true }).toArray();

      expect(distributedResults).toEqual(standardResults);
    });

    it('should produce same results for $group with $min and $max', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', value: 50 },
        { _id: '2', value: 100 },
        { _id: '3', value: 25 },
        { _id: '4', value: 75 },
      ]);

      const pipeline = [
        { $group: { _id: null, minValue: { $min: '$value' }, maxValue: { $max: '$value' } } },
      ];

      const standardResults = await collection.aggregate(pipeline).toArray();
      const distributedResults = await collection.aggregate(pipeline, { distributed: true }).toArray();

      expect(distributedResults).toEqual(standardResults);
    });

    it('should produce same results for $group with $count', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', category: 'A' },
        { _id: '2', category: 'A' },
        { _id: '3', category: 'B' },
        { _id: '4', category: 'A' },
      ]);

      const pipeline = [
        { $group: { _id: '$category', docCount: { $count: {} } } },
        { $sort: { _id: 1 } },
      ];

      const standardResults = await collection.aggregate(pipeline).toArray();
      const distributedResults = await collection.aggregate(pipeline, { distributed: true }).toArray();

      expect(distributedResults).toEqual(standardResults);
    });

    it('should produce same results for compound _id', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', year: 2023, month: 1, amount: 100 },
        { _id: '2', year: 2023, month: 1, amount: 200 },
        { _id: '3', year: 2023, month: 2, amount: 150 },
        { _id: '4', year: 2024, month: 1, amount: 300 },
      ]);

      const pipeline = [
        { $group: { _id: { year: '$year', month: '$month' }, total: { $sum: '$amount' } } },
        { $sort: { '_id.year': 1, '_id.month': 1 } },
      ];

      const standardResults = await collection.aggregate(pipeline).toArray();
      const distributedResults = await collection.aggregate(pipeline, { distributed: true }).toArray();

      expect(distributedResults).toEqual(standardResults);
    });
  });

  describe('reduce phase stages', () => {
    it('should apply $sort after $group', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', category: 'A', amount: 10 },
        { _id: '2', category: 'B', amount: 30 },
        { _id: '3', category: 'C', amount: 20 },
      ]);

      const pipeline = [
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
        { $sort: { total: -1 } },
      ];

      const standardResults = await collection.aggregate(pipeline).toArray();
      const distributedResults = await collection.aggregate(pipeline, { distributed: true }).toArray();

      expect(distributedResults).toEqual(standardResults);
      expect(distributedResults[0]._id).toBe('B'); // 30
      expect(distributedResults[1]._id).toBe('C'); // 20
      expect(distributedResults[2]._id).toBe('A'); // 10
    });

    it('should apply $limit after $group', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', category: 'A' },
        { _id: '2', category: 'B' },
        { _id: '3', category: 'C' },
        { _id: '4', category: 'D' },
      ]);

      const pipeline = [
        { $group: { _id: '$category', count: { $sum: 1 } } },
        { $limit: 2 },
      ];

      const standardResults = await collection.aggregate(pipeline).toArray();
      const distributedResults = await collection.aggregate(pipeline, { distributed: true }).toArray();

      expect(distributedResults).toHaveLength(2);
      expect(distributedResults).toEqual(standardResults);
    });

    it('should apply $skip after $group', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', category: 'A', amount: 100 },
        { _id: '2', category: 'B', amount: 200 },
        { _id: '3', category: 'C', amount: 300 },
      ]);

      const pipeline = [
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
        { $sort: { total: -1 } },
        { $skip: 1 },
        { $limit: 1 },
      ];

      const standardResults = await collection.aggregate(pipeline).toArray();
      const distributedResults = await collection.aggregate(pipeline, { distributed: true }).toArray();

      expect(distributedResults).toHaveLength(1);
      expect(distributedResults).toEqual(standardResults);
    });
  });

  describe('pipelines without $group', () => {
    it('should fall back to standard execution for simple pipelines', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', name: 'Alice', age: 30 },
        { _id: '2', name: 'Bob', age: 25 },
        { _id: '3', name: 'Charlie', age: 35 },
      ]);

      const pipeline = [
        { $match: { age: { $gte: 30 } } },
        { $project: { name: 1 } },
      ];

      const standardResults = await collection.aggregate(pipeline).toArray();
      const distributedResults = await collection.aggregate(pipeline, { distributed: true }).toArray();

      expect(distributedResults).toEqual(standardResults);
    });
  });

  describe('non-distributable pipelines', () => {
    it('should fall back to standard execution for $lookup pipelines', async () => {
      const { db } = createTestDatabase('lookuptest');

      const orders = db.collection('orders');
      const products = db.collection('products');

      await products.insertMany([
        { _id: 'prod1', name: 'Widget', price: 10 },
        { _id: 'prod2', name: 'Gadget', price: 20 },
      ]);

      await orders.insertMany([
        { _id: 'order1', productId: 'prod1', quantity: 5 },
        { _id: 'order2', productId: 'prod2', quantity: 3 },
      ]);

      const pipeline = [
        {
          $lookup: {
            from: 'products',
            localField: 'productId',
            foreignField: '_id',
            as: 'productDetails',
          },
        },
        { $sort: { _id: 1 } },
      ];

      // $lookup should cause fallback to standard execution
      const standardResults = await orders.aggregate(pipeline).toArray();
      const distributedResults = await orders.aggregate(pipeline, { distributed: true }).toArray();

      expect(distributedResults).toEqual(standardResults);
    });
  });

  describe('complex multi-stage pipelines', () => {
    it('should handle $match -> $group -> $sort -> $limit', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', region: 'US', sales: 100, active: true },
        { _id: '2', region: 'EU', sales: 200, active: true },
        { _id: '3', region: 'US', sales: 50, active: false },
        { _id: '4', region: 'US', sales: 150, active: true },
        { _id: '5', region: 'EU', sales: 100, active: true },
        { _id: '6', region: 'APAC', sales: 75, active: true },
      ]);

      const pipeline = [
        { $match: { active: true } },
        { $group: { _id: '$region', totalSales: { $sum: '$sales' }, count: { $sum: 1 } } },
        { $sort: { totalSales: -1 } },
        { $limit: 2 },
      ];

      const standardResults = await collection.aggregate(pipeline).toArray();
      const distributedResults = await collection.aggregate(pipeline, { distributed: true }).toArray();

      expect(distributedResults).toEqual(standardResults);
      expect(distributedResults).toHaveLength(2);
      // EU: 200 + 100 = 300
      // US: 100 + 150 = 250 (inactive 50 filtered out)
      expect(distributedResults[0]).toEqual({ _id: 'EU', totalSales: 300, count: 2 });
      expect(distributedResults[1]).toEqual({ _id: 'US', totalSales: 250, count: 2 });
    });

    it('should handle multiple accumulators in $group', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', category: 'A', price: 100, quantity: 5 },
        { _id: '2', category: 'A', price: 200, quantity: 3 },
        { _id: '3', category: 'B', price: 150, quantity: 4 },
      ]);

      const pipeline = [
        {
          $group: {
            _id: '$category',
            totalPrice: { $sum: '$price' },
            avgPrice: { $avg: '$price' },
            minPrice: { $min: '$price' },
            maxPrice: { $max: '$price' },
            count: { $sum: 1 },
          },
        },
        { $sort: { _id: 1 } },
      ];

      const standardResults = await collection.aggregate(pipeline).toArray();
      const distributedResults = await collection.aggregate(pipeline, { distributed: true }).toArray();

      expect(distributedResults).toEqual(standardResults);
    });
  });

  describe('backwards compatibility', () => {
    it('should use standard execution when distributed option is not set', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', category: 'A', amount: 10 },
        { _id: '2', category: 'B', amount: 20 },
      ]);

      // No options - should use standard execution
      const results = await collection.aggregate([
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
      ]).toArray();

      expect(results).toHaveLength(2);
    });

    it('should use standard execution when distributed is false', async () => {
      const { collection } = createTestCollection();
      await collection.insertMany([
        { _id: '1', category: 'A', amount: 10 },
        { _id: '2', category: 'B', amount: 20 },
      ]);

      const results = await collection.aggregate([
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
      ], { distributed: false }).toArray();

      expect(results).toHaveLength(2);
    });

    it('should handle empty collections', async () => {
      const { collection } = createTestCollection();

      const standardResults = await collection.aggregate([
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
      ]).toArray();

      const distributedResults = await collection.aggregate([
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
      ], { distributed: true }).toArray();

      expect(distributedResults).toEqual(standardResults);
      expect(distributedResults).toHaveLength(0);
    });
  });
});

describe('AggregateOptions', () => {
  it('should accept distributed option', async () => {
    const { collection } = createTestCollection();
    await collection.insertMany([{ _id: '1', value: 1 }]);

    // This should compile and work without errors
    const cursor = collection.aggregate(
      [{ $match: {} }],
      { distributed: true }
    );

    const results = await cursor.toArray();
    expect(results).toHaveLength(1);
  });

  it('should accept batchSize option (reserved for future use)', async () => {
    const { collection } = createTestCollection();
    await collection.insertMany([{ _id: '1', value: 1 }]);

    // batchSize is currently a no-op but should be accepted
    const cursor = collection.aggregate(
      [{ $match: {} }],
      { batchSize: 100 }
    );

    const results = await cursor.toArray();
    expect(results).toHaveLength(1);
  });
});
