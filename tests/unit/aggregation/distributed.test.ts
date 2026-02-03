/**
 * Distributed Aggregation Tests
 *
 * Tests for aggregation across multiple shards covering:
 * - $group across shards with merge
 * - $match pushed to each shard
 * - $sort with limit optimization
 * - $count across shards
 * - $lookup across shards
 * - Partial aggregation on shards, final merge
 * - Pipeline optimization (predicate pushdown)
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  DistributedAggregationPlanner,
  DistributedAggregationExecutor,
  createDistributedAggregationPlanner,
  createDistributedAggregationExecutor,
  type PartialAggregate,
} from '../../../src/client/distributed-aggregation.js';
import type { AggregationStage, WithId, Document } from '../../../src/types.js';

describe('Distributed Aggregation across Multiple Shards', () => {
  let executor: DistributedAggregationExecutor;
  let planner: DistributedAggregationPlanner;

  beforeEach(() => {
    executor = createDistributedAggregationExecutor();
    planner = createDistributedAggregationPlanner();
  });

  describe('$group across shards with merge', () => {
    it('should merge $sum aggregates from 3 shards correctly', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, [
          { _id: '1', region: 'US', amount: 100 },
          { _id: '2', region: 'EU', amount: 50 },
        ]],
        [1, [
          { _id: '3', region: 'US', amount: 200 },
          { _id: '4', region: 'APAC', amount: 150 },
        ]],
        [2, [
          { _id: '5', region: 'EU', amount: 75 },
          { _id: '6', region: 'US', amount: 125 },
        ]],
      ]);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$region', totalAmount: { $sum: '$amount' } } },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(3);

      const usResult = results.find((r) => r._id === 'US') as { totalAmount: number };
      const euResult = results.find((r) => r._id === 'EU') as { totalAmount: number };
      const apacResult = results.find((r) => r._id === 'APAC') as { totalAmount: number };

      expect(usResult.totalAmount).toBe(425); // 100 + 200 + 125
      expect(euResult.totalAmount).toBe(125); // 50 + 75
      expect(apacResult.totalAmount).toBe(150);
    });

    it('should merge $count aggregates from multiple shards', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, [
          { _id: '1', status: 'active' },
          { _id: '2', status: 'active' },
          { _id: '3', status: 'inactive' },
        ]],
        [1, [
          { _id: '4', status: 'active' },
          { _id: '5', status: 'inactive' },
        ]],
        [2, [
          { _id: '6', status: 'active' },
          { _id: '7', status: 'active' },
          { _id: '8', status: 'active' },
        ]],
      ]);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$status', count: { $count: {} } } },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(2);

      const activeResult = results.find((r) => r._id === 'active') as { count: number };
      const inactiveResult = results.find((r) => r._id === 'inactive') as { count: number };

      expect(activeResult.count).toBe(6); // 2 + 1 + 3
      expect(inactiveResult.count).toBe(2); // 1 + 1
    });

    it('should merge $avg correctly with proper sum/count tracking', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, [
          { _id: '1', department: 'eng', salary: 100000 },
          { _id: '2', department: 'eng', salary: 120000 },
        ]],
        [1, [
          { _id: '3', department: 'eng', salary: 80000 },
          { _id: '4', department: 'eng', salary: 100000 },
        ]],
        [2, [
          { _id: '5', department: 'eng', salary: 150000 },
        ]],
      ]);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$department', avgSalary: { $avg: '$salary' } } },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(1);
      // (100000 + 120000 + 80000 + 100000 + 150000) / 5 = 550000 / 5 = 110000
      expect((results[0] as { avgSalary: number }).avgSalary).toBe(110000);
    });

    it('should merge $min and $max correctly across shards', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, [
          { _id: '1', temperature: 25 },
          { _id: '2', temperature: 30 },
        ]],
        [1, [
          { _id: '3', temperature: 15 },
          { _id: '4', temperature: 22 },
        ]],
        [2, [
          { _id: '5', temperature: 35 },
          { _id: '6', temperature: 28 },
        ]],
      ]);

      const pipeline: AggregationStage[] = [
        {
          $group: {
            _id: null,
            minTemp: { $min: '$temperature' },
            maxTemp: { $max: '$temperature' },
          },
        },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(1);
      expect((results[0] as { minTemp: number }).minTemp).toBe(15);
      expect((results[0] as { maxTemp: number }).maxTemp).toBe(35);
    });

    it('should handle groups appearing on only some shards', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, [
          { _id: '1', type: 'A', value: 10 },
        ]],
        [1, [
          { _id: '2', type: 'B', value: 20 },
        ]],
        [2, [
          { _id: '3', type: 'C', value: 30 },
          { _id: '4', type: 'A', value: 40 },
        ]],
      ]);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$type', total: { $sum: '$value' } } },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(3);
      expect(results.find((r) => r._id === 'A')).toEqual({ _id: 'A', total: 50 });
      expect(results.find((r) => r._id === 'B')).toEqual({ _id: 'B', total: 20 });
      expect(results.find((r) => r._id === 'C')).toEqual({ _id: 'C', total: 30 });
    });
  });

  describe('$match pushed to each shard', () => {
    it('should apply $match filter on each shard before aggregation', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, [
          { _id: '1', active: true, category: 'A', amount: 100 },
          { _id: '2', active: false, category: 'A', amount: 500 },
          { _id: '3', active: true, category: 'B', amount: 200 },
        ]],
        [1, [
          { _id: '4', active: true, category: 'A', amount: 150 },
          { _id: '5', active: false, category: 'B', amount: 300 },
        ]],
        [2, [
          { _id: '6', active: true, category: 'B', amount: 250 },
          { _id: '7', active: true, category: 'A', amount: 50 },
        ]],
      ]);

      const pipeline: AggregationStage[] = [
        { $match: { active: true } },
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(2);

      const catA = results.find((r) => r._id === 'A') as { total: number };
      const catB = results.find((r) => r._id === 'B') as { total: number };

      // A: 100 + 150 + 50 = 300 (inactive filtered out)
      // B: 200 + 250 = 450 (inactive filtered out)
      expect(catA.total).toBe(300);
      expect(catB.total).toBe(450);
    });

    it('should push complex $match with operators to shards', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, [
          { _id: '1', price: 50, quantity: 10 },
          { _id: '2', price: 150, quantity: 5 },
        ]],
        [1, [
          { _id: '3', price: 80, quantity: 20 },
          { _id: '4', price: 200, quantity: 2 },
        ]],
      ]);

      const pipeline: AggregationStage[] = [
        { $match: { price: { $gte: 100 } } },
        { $group: { _id: null, totalQuantity: { $sum: '$quantity' } } },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(1);
      // Only price >= 100: 150 (qty 5) + 200 (qty 2) = 7
      expect((results[0] as { totalQuantity: number }).totalQuantity).toBe(7);
    });

    it('should handle $match with $in operator across shards', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, [
          { _id: '1', status: 'pending', value: 10 },
          { _id: '2', status: 'approved', value: 20 },
        ]],
        [1, [
          { _id: '3', status: 'rejected', value: 30 },
          { _id: '4', status: 'approved', value: 40 },
        ]],
        [2, [
          { _id: '5', status: 'pending', value: 50 },
        ]],
      ]);

      const pipeline: AggregationStage[] = [
        { $match: { status: { $in: ['pending', 'approved'] } } },
        { $group: { _id: null, total: { $sum: '$value' } } },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(1);
      // pending: 10, 50; approved: 20, 40 = 120
      expect((results[0] as { total: number }).total).toBe(120);
    });
  });

  describe('$sort with limit optimization', () => {
    it('should sort merged results from multiple shards', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, [
          { _id: '1', product: 'A', sales: 100 },
          { _id: '2', product: 'B', sales: 300 },
        ]],
        [1, [
          { _id: '3', product: 'C', sales: 200 },
          { _id: '4', product: 'A', sales: 150 },
        ]],
        [2, [
          { _id: '5', product: 'B', sales: 100 },
        ]],
      ]);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$product', totalSales: { $sum: '$sales' } } },
        { $sort: { totalSales: -1 } },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(3);
      expect(results[0]).toEqual({ _id: 'B', totalSales: 400 }); // 300 + 100
      expect(results[1]).toEqual({ _id: 'A', totalSales: 250 }); // 100 + 150
      expect(results[2]).toEqual({ _id: 'C', totalSales: 200 });
    });

    it('should apply $sort with $limit for top-N queries', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, [
          { _id: '1', category: 'electronics', revenue: 5000 },
          { _id: '2', category: 'books', revenue: 1000 },
        ]],
        [1, [
          { _id: '3', category: 'clothing', revenue: 3000 },
          { _id: '4', category: 'electronics', revenue: 4000 },
        ]],
        [2, [
          { _id: '5', category: 'food', revenue: 2000 },
          { _id: '6', category: 'books', revenue: 500 },
        ]],
      ]);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$category', totalRevenue: { $sum: '$revenue' } } },
        { $sort: { totalRevenue: -1 } },
        { $limit: 2 },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(2);
      expect(results[0]).toEqual({ _id: 'electronics', totalRevenue: 9000 }); // 5000 + 4000
      expect(results[1]).toEqual({ _id: 'clothing', totalRevenue: 3000 });
    });

    it('should handle ascending sort with skip', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, [
          { _id: '1', item: 'X', count: 5 },
          { _id: '2', item: 'Y', count: 10 },
        ]],
        [1, [
          { _id: '3', item: 'Z', count: 15 },
          { _id: '4', item: 'X', count: 3 },
        ]],
      ]);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$item', totalCount: { $sum: '$count' } } },
        { $sort: { totalCount: 1 } },
        { $skip: 1 },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(2);
      // Sorted ascending: X=8, Y=10, Z=15, skip first
      expect(results[0]._id).toBe('Y');
      expect(results[1]._id).toBe('Z');
    });
  });

  describe('$count across shards', () => {
    it('should count total documents across all shards', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, [
          { _id: '1', name: 'Alice' },
          { _id: '2', name: 'Bob' },
        ]],
        [1, [
          { _id: '3', name: 'Charlie' },
        ]],
        [2, [
          { _id: '4', name: 'Diana' },
          { _id: '5', name: 'Eve' },
          { _id: '6', name: 'Frank' },
        ]],
      ]);

      const pipeline: AggregationStage[] = [
        { $count: 'totalDocuments' },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(1);
      expect((results[0] as { totalDocuments: number }).totalDocuments).toBe(6);
    });

    it('should count filtered documents across shards', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, [
          { _id: '1', type: 'premium', active: true },
          { _id: '2', type: 'basic', active: true },
        ]],
        [1, [
          { _id: '3', type: 'premium', active: false },
          { _id: '4', type: 'premium', active: true },
        ]],
        [2, [
          { _id: '5', type: 'basic', active: true },
        ]],
      ]);

      const pipeline: AggregationStage[] = [
        { $match: { type: 'premium', active: true } },
        { $count: 'activePremiumCount' },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(1);
      expect((results[0] as { activePremiumCount: number }).activePremiumCount).toBe(2);
    });
  });

  describe('$lookup across shards', () => {
    it('should identify $lookup as non-distributable', () => {
      const pipeline: AggregationStage[] = [
        {
          $lookup: {
            from: 'orders',
            localField: 'customerId',
            foreignField: '_id',
            as: 'customerOrders',
          },
        },
      ];

      const analysis = planner.analyzePipeline(pipeline);

      expect(analysis.canDistribute).toBe(false);
      expect(analysis.reason).toContain('$lookup');
    });

    it('should classify $lookup as reduce phase', () => {
      const pipeline: AggregationStage[] = [
        { $match: { status: 'active' } },
        {
          $lookup: {
            from: 'products',
            localField: 'productId',
            foreignField: '_id',
            as: 'productInfo',
          },
        },
      ];

      const analysis = planner.analyzePipeline(pipeline);

      expect(analysis.stages[1].phase).toBe('reduce');
      expect(analysis.stages[1].stageType).toBe('$lookup');
    });
  });

  describe('Partial aggregation on shards, final merge', () => {
    it('should execute partial $push and merge arrays', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, [
          { _id: '1', team: 'A', player: 'Alice' },
          { _id: '2', team: 'A', player: 'Bob' },
        ]],
        [1, [
          { _id: '3', team: 'A', player: 'Charlie' },
          { _id: '4', team: 'B', player: 'Diana' },
        ]],
      ]);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$team', players: { $push: '$player' } } },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(2);

      const teamA = results.find((r) => r._id === 'A') as { players: string[] };
      const teamB = results.find((r) => r._id === 'B') as { players: string[] };

      expect(teamA.players).toHaveLength(3);
      expect(teamA.players).toContain('Alice');
      expect(teamA.players).toContain('Bob');
      expect(teamA.players).toContain('Charlie');
      expect(teamB.players).toEqual(['Diana']);
    });

    it('should execute partial $addToSet and merge unique values', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, [
          { _id: '1', product: 'P1', tag: 'sale' },
          { _id: '2', product: 'P1', tag: 'new' },
        ]],
        [1, [
          { _id: '3', product: 'P1', tag: 'sale' }, // duplicate
          { _id: '4', product: 'P1', tag: 'featured' },
        ]],
      ]);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$product', tags: { $addToSet: '$tag' } } },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(1);

      const product = results[0] as { tags: string[] };
      expect(product.tags).toHaveLength(3);
      expect(product.tags).toContain('sale');
      expect(product.tags).toContain('new');
      expect(product.tags).toContain('featured');
    });

    it('should handle multiple accumulators with partial aggregation', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, [
          { _id: '1', store: 'NYC', sales: 1000, transactions: 50 },
          { _id: '2', store: 'NYC', sales: 1500, transactions: 75 },
        ]],
        [1, [
          { _id: '3', store: 'NYC', sales: 2000, transactions: 100 },
          { _id: '4', store: 'LA', sales: 3000, transactions: 150 },
        ]],
      ]);

      const pipeline: AggregationStage[] = [
        {
          $group: {
            _id: '$store',
            totalSales: { $sum: '$sales' },
            avgTransactions: { $avg: '$transactions' },
            maxSales: { $max: '$sales' },
            transactionCount: { $count: {} },
          },
        },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(2);

      const nyc = results.find((r) => r._id === 'NYC') as {
        totalSales: number;
        avgTransactions: number;
        maxSales: number;
        transactionCount: number;
      };
      const la = results.find((r) => r._id === 'LA') as {
        totalSales: number;
        avgTransactions: number;
        maxSales: number;
        transactionCount: number;
      };

      expect(nyc.totalSales).toBe(4500);
      expect(nyc.avgTransactions).toBe(75); // (50 + 75 + 100) / 3
      expect(nyc.maxSales).toBe(2000);
      expect(nyc.transactionCount).toBe(3);

      expect(la.totalSales).toBe(3000);
      expect(la.avgTransactions).toBe(150);
      expect(la.maxSales).toBe(3000);
      expect(la.transactionCount).toBe(1);
    });

    it('should handle $first and $last across shards', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, [
          { _id: '1', group: 'G1', value: 'first-shard0' },
          { _id: '2', group: 'G1', value: 'last-shard0' },
        ]],
        [1, [
          { _id: '3', group: 'G1', value: 'first-shard1' },
          { _id: '4', group: 'G1', value: 'last-shard1' },
        ]],
      ]);

      const pipeline: AggregationStage[] = [
        {
          $group: {
            _id: '$group',
            firstValue: { $first: '$value' },
            lastValue: { $last: '$value' },
          },
        },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(1);
      // First should be from first shard, last from last shard processed
      const result = results[0] as { firstValue: string; lastValue: string };
      expect(result.firstValue).toBe('first-shard0');
      expect(result.lastValue).toBe('last-shard1');
    });
  });

  describe('Pipeline optimization (predicate pushdown)', () => {
    it('should identify $match as map phase for pushdown', () => {
      const pipeline: AggregationStage[] = [
        { $match: { status: 'active' } },
        { $project: { name: 1, email: 1 } },
        { $group: { _id: '$name', count: { $sum: 1 } } },
      ];

      const analysis = planner.analyzePipeline(pipeline);

      expect(analysis.stages[0].phase).toBe('map');
      expect(analysis.stages[0].stageType).toBe('$match');
      expect(analysis.stages[1].phase).toBe('map');
      expect(analysis.stages[2].phase).toBe('reduce');
    });

    it('should split pipeline correctly for distributed execution', () => {
      const pipeline: AggregationStage[] = [
        { $match: { active: true } },
        { $project: { category: 1, amount: 1 } },
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
        { $sort: { total: -1 } },
        { $limit: 5 },
      ];

      const split = planner.splitPipeline(pipeline);

      expect(split.mapPhase).toHaveLength(2);
      expect(split.mapPhase[0]).toEqual({ $match: { active: true } });
      expect(split.mapPhase[1]).toEqual({ $project: { category: 1, amount: 1 } });

      expect(split.groupStage).toEqual({ _id: '$category', total: { $sum: '$amount' } });

      expect(split.reducePhase).toHaveLength(2);
      expect(split.reducePhase[0]).toEqual({ $sort: { total: -1 } });
      expect(split.reducePhase[1]).toEqual({ $limit: 5 });
    });

    it('should classify $project, $addFields, $unwind as map phase', () => {
      const pipeline: AggregationStage[] = [
        { $project: { name: 1 } },
        { $addFields: { computed: '$value' } },
        { $unwind: '$tags' },
      ];

      const analysis = planner.analyzePipeline(pipeline);

      expect(analysis.stages.every((s) => s.phase === 'map')).toBe(true);
    });

    it('should execute full pipeline with map-reduce-reduce phases', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, [
          { _id: '1', year: 2023, quarter: 'Q1', revenue: 1000, active: true },
          { _id: '2', year: 2023, quarter: 'Q1', revenue: 2000, active: false },
          { _id: '3', year: 2023, quarter: 'Q2', revenue: 1500, active: true },
        ]],
        [1, [
          { _id: '4', year: 2023, quarter: 'Q1', revenue: 3000, active: true },
          { _id: '5', year: 2023, quarter: 'Q2', revenue: 2500, active: true },
          { _id: '6', year: 2023, quarter: 'Q3', revenue: 1800, active: true },
        ]],
      ]);

      const pipeline: AggregationStage[] = [
        { $match: { active: true } },
        { $group: { _id: '$quarter', totalRevenue: { $sum: '$revenue' } } },
        { $sort: { totalRevenue: -1 } },
        { $limit: 2 },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(2);
      expect(results[0]).toEqual({ _id: 'Q1', totalRevenue: 4000 }); // 1000 + 3000
      expect(results[1]).toEqual({ _id: 'Q2', totalRevenue: 4000 }); // 1500 + 2500
    });
  });

  describe('Edge cases and error handling', () => {
    it('should handle empty shards gracefully', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, []],
        [1, [{ _id: '1', category: 'A', value: 100 }]],
        [2, []],
        [3, [{ _id: '2', category: 'A', value: 200 }]],
      ]);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$category', total: { $sum: '$value' } } },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(1);
      expect(results[0]).toEqual({ _id: 'A', total: 300 });
    });

    it('should handle all shards being empty', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, []],
        [1, []],
        [2, []],
      ]);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$category', total: { $sum: '$value' } } },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(0);
    });

    it('should handle compound _id across shards', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, [
          { _id: '1', year: 2023, month: 1, sales: 100 },
          { _id: '2', year: 2023, month: 2, sales: 200 },
        ]],
        [1, [
          { _id: '3', year: 2023, month: 1, sales: 150 },
          { _id: '4', year: 2024, month: 1, sales: 300 },
        ]],
      ]);

      const pipeline: AggregationStage[] = [
        {
          $group: {
            _id: { year: '$year', month: '$month' },
            totalSales: { $sum: '$sales' },
          },
        },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(3);

      const jan2023 = results.find((r) => {
        const id = r._id as { year: number; month: number };
        return id.year === 2023 && id.month === 1;
      });
      expect(jan2023).toBeDefined();
      expect((jan2023 as { totalSales: number }).totalSales).toBe(250);
    });

    it('should handle null values in group fields', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, [
          { _id: '1', category: null, value: 10 },
          { _id: '2', category: 'A', value: 20 },
        ]],
        [1, [
          { _id: '3', category: null, value: 30 },
          { _id: '4', value: 40 }, // missing category
        ]],
      ]);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$category', total: { $sum: '$value' } } },
      ];

      const results = await executor.execute(shardData, pipeline);

      // null and undefined should be grouped together
      expect(results.length).toBeGreaterThanOrEqual(2);
      const catA = results.find((r) => r._id === 'A');
      expect(catA).toBeDefined();
      expect((catA as { total: number }).total).toBe(20);
    });

    it('should handle very large number of groups across shards', async () => {
      // Create 100 documents across 4 shards with 25 unique categories each
      const shards: Map<number, WithId<Document>[]> = new Map();
      for (let shardId = 0; shardId < 4; shardId++) {
        const docs: WithId<Document>[] = [];
        for (let i = 0; i < 25; i++) {
          docs.push({
            _id: `${shardId}-${i}`,
            category: `cat-${i}`,
            value: shardId * 100 + i,
          });
        }
        shards.set(shardId, docs);
      }

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$category', total: { $sum: '$value' }, count: { $sum: 1 } } },
      ];

      const results = await executor.execute(shards, pipeline);

      expect(results).toHaveLength(25);
      // Each category appears on all 4 shards
      for (const result of results) {
        expect((result as { count: number }).count).toBe(4);
      }
    });
  });

  describe('Pipeline without $group', () => {
    it('should execute pipeline with only map phases across shards', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, [
          { _id: '1', name: 'Alice', age: 25, status: 'active' },
          { _id: '2', name: 'Bob', age: 35, status: 'inactive' },
        ]],
        [1, [
          { _id: '3', name: 'Charlie', age: 30, status: 'active' },
        ]],
      ]);

      const pipeline: AggregationStage[] = [
        { $match: { status: 'active' } },
        { $project: { name: 1, age: 1 } },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(2);
      const names = results.map((r) => (r as { name: string }).name);
      expect(names).toContain('Alice');
      expect(names).toContain('Charlie');
    });

    it('should handle $sort and $limit without $group', async () => {
      const shardData = new Map<number, WithId<Document>[]>([
        [0, [
          { _id: '1', score: 85 },
          { _id: '2', score: 92 },
        ]],
        [1, [
          { _id: '3', score: 78 },
          { _id: '4', score: 95 },
        ]],
      ]);

      const pipeline: AggregationStage[] = [
        { $sort: { score: -1 } },
        { $limit: 2 },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(2);
      expect((results[0] as { score: number }).score).toBe(95);
      expect((results[1] as { score: number }).score).toBe(92);
    });
  });
});
