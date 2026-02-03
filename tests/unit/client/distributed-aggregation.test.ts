/**
 * Distributed Aggregation Engine Tests
 *
 * Tests for the distributed aggregation planner and executor:
 * - Pipeline analysis and classification
 * - Partial aggregation for $group
 * - Merging partial results across shards
 * - End-to-end distributed execution
 */

import { describe, it, expect } from 'vitest';
import {
  DistributedAggregationPlanner,
  DistributedAggregationExecutor,
  createDistributedAggregationPlanner,
  createDistributedAggregationExecutor,
  type PartialAggregate,
} from '../../../src/client/distributed-aggregation.js';
import type { AggregationStage, WithId, Document } from '../../../src/types.js';

describe('DistributedAggregationPlanner', () => {
  describe('analyzePipeline', () => {
    it('should classify $match as map phase', () => {
      const planner = createDistributedAggregationPlanner();
      const pipeline: AggregationStage[] = [{ $match: { status: 'active' } }];

      const analysis = planner.analyzePipeline(pipeline);

      expect(analysis.canDistribute).toBe(true);
      expect(analysis.stages[0].phase).toBe('map');
      expect(analysis.stages[0].stageType).toBe('$match');
    });

    it('should classify $project as map phase', () => {
      const planner = createDistributedAggregationPlanner();
      const pipeline: AggregationStage[] = [{ $project: { name: 1 } }];

      const analysis = planner.analyzePipeline(pipeline);

      expect(analysis.stages[0].phase).toBe('map');
      expect(analysis.stages[0].stageType).toBe('$project');
    });

    it('should classify $addFields as map phase', () => {
      const planner = createDistributedAggregationPlanner();
      const pipeline: AggregationStage[] = [{ $addFields: { newField: '$oldField' } }];

      const analysis = planner.analyzePipeline(pipeline);

      expect(analysis.stages[0].phase).toBe('map');
      expect(analysis.stages[0].stageType).toBe('$addFields');
    });

    it('should classify $unwind as map phase', () => {
      const planner = createDistributedAggregationPlanner();
      const pipeline: AggregationStage[] = [{ $unwind: '$tags' }];

      const analysis = planner.analyzePipeline(pipeline);

      expect(analysis.stages[0].phase).toBe('map');
      expect(analysis.stages[0].stageType).toBe('$unwind');
    });

    it('should classify $group as reduce phase', () => {
      const planner = createDistributedAggregationPlanner();
      const pipeline: AggregationStage[] = [
        { $group: { _id: '$category', count: { $sum: 1 } } },
      ];

      const analysis = planner.analyzePipeline(pipeline);

      expect(analysis.hasGroup).toBe(true);
      expect(analysis.groupStageIndex).toBe(0);
      expect(analysis.stages[0].phase).toBe('reduce');
      expect(analysis.stages[0].stageType).toBe('$group');
    });

    it('should classify $lookup as reduce phase and mark as not distributable', () => {
      const planner = createDistributedAggregationPlanner();
      const pipeline: AggregationStage[] = [
        {
          $lookup: {
            from: 'other',
            localField: 'id',
            foreignField: 'refId',
            as: 'joined',
          },
        },
      ];

      const analysis = planner.analyzePipeline(pipeline);

      expect(analysis.canDistribute).toBe(false);
      expect(analysis.reason).toContain('$lookup');
      expect(analysis.stages[0].phase).toBe('reduce');
    });

    it('should classify $limit, $skip, $sort as barrier phase', () => {
      const planner = createDistributedAggregationPlanner();
      const pipeline: AggregationStage[] = [
        { $sort: { name: 1 } },
        { $skip: 10 },
        { $limit: 5 },
      ];

      const analysis = planner.analyzePipeline(pipeline);

      expect(analysis.stages[0].phase).toBe('barrier');
      expect(analysis.stages[1].phase).toBe('barrier');
      expect(analysis.stages[2].phase).toBe('barrier');
    });

    it('should find $group stage index in complex pipeline', () => {
      const planner = createDistributedAggregationPlanner();
      const pipeline: AggregationStage[] = [
        { $match: { active: true } },
        { $project: { category: 1, amount: 1 } },
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
        { $sort: { total: -1 } },
      ];

      const analysis = planner.analyzePipeline(pipeline);

      expect(analysis.hasGroup).toBe(true);
      expect(analysis.groupStageIndex).toBe(2);
    });
  });

  describe('splitPipeline', () => {
    it('should split pipeline around $group stage', () => {
      const planner = createDistributedAggregationPlanner();
      const pipeline: AggregationStage[] = [
        { $match: { active: true } },
        { $group: { _id: '$category', count: { $sum: 1 } } },
        { $sort: { count: -1 } },
      ];

      const split = planner.splitPipeline(pipeline);

      expect(split.mapPhase).toHaveLength(1);
      expect(split.mapPhase[0]).toEqual({ $match: { active: true } });
      expect(split.groupStage).toEqual({ _id: '$category', count: { $sum: 1 } });
      expect(split.reducePhase).toHaveLength(1);
      expect(split.reducePhase[0]).toEqual({ $sort: { count: -1 } });
    });

    it('should handle pipeline with no $group', () => {
      const planner = createDistributedAggregationPlanner();
      const pipeline: AggregationStage[] = [
        { $match: { active: true } },
        { $limit: 10 },
      ];

      const split = planner.splitPipeline(pipeline);

      expect(split.mapPhase).toHaveLength(2);
      expect(split.groupStage).toBeUndefined();
      expect(split.reducePhase).toHaveLength(0);
    });

    it('should handle $group at start of pipeline', () => {
      const planner = createDistributedAggregationPlanner();
      const pipeline: AggregationStage[] = [
        { $group: { _id: null, total: { $sum: '$amount' } } },
      ];

      const split = planner.splitPipeline(pipeline);

      expect(split.mapPhase).toHaveLength(0);
      expect(split.groupStage).toEqual({ _id: null, total: { $sum: '$amount' } });
      expect(split.reducePhase).toHaveLength(0);
    });
  });

  describe('executePartialGroup', () => {
    const planner = new DistributedAggregationPlanner();

    it('should compute partial $sum', () => {
      const docs: WithId<Document>[] = [
        { _id: '1', category: 'A', amount: 10 },
        { _id: '2', category: 'A', amount: 20 },
        { _id: '3', category: 'B', amount: 30 },
      ];

      const groupSpec = { _id: '$category', total: { $sum: '$amount' } };
      const partials = planner.executePartialGroup(docs, groupSpec);

      expect(partials).toHaveLength(2);

      const catA = partials.find((p) => p._id === 'A');
      const catB = partials.find((p) => p._id === 'B');

      expect(catA).toBeDefined();
      expect(catA!.accumulators.total).toEqual({ type: 'sum', value: 30 });

      expect(catB).toBeDefined();
      expect(catB!.accumulators.total).toEqual({ type: 'sum', value: 30 });
    });

    it('should compute partial $sum with constant value', () => {
      const docs: WithId<Document>[] = [
        { _id: '1', category: 'A' },
        { _id: '2', category: 'A' },
        { _id: '3', category: 'A' },
      ];

      const groupSpec = { _id: '$category', count: { $sum: 1 } };
      const partials = planner.executePartialGroup(docs, groupSpec);

      expect(partials).toHaveLength(1);
      expect(partials[0].accumulators.count).toEqual({ type: 'sum', value: 3 });
    });

    it('should compute partial $avg', () => {
      const docs: WithId<Document>[] = [
        { _id: '1', category: 'A', score: 80 },
        { _id: '2', category: 'A', score: 100 },
      ];

      const groupSpec = { _id: '$category', avgScore: { $avg: '$score' } };
      const partials = planner.executePartialGroup(docs, groupSpec);

      expect(partials).toHaveLength(1);
      expect(partials[0].accumulators.avgScore).toEqual({
        type: 'avg',
        sum: 180,
        count: 2,
      });
    });

    it('should compute partial $min', () => {
      const docs: WithId<Document>[] = [
        { _id: '1', category: 'A', value: 50 },
        { _id: '2', category: 'A', value: 30 },
        { _id: '3', category: 'A', value: 70 },
      ];

      const groupSpec = { _id: '$category', minValue: { $min: '$value' } };
      const partials = planner.executePartialGroup(docs, groupSpec);

      expect(partials).toHaveLength(1);
      expect(partials[0].accumulators.minValue).toEqual({ type: 'min', value: 30 });
    });

    it('should compute partial $max', () => {
      const docs: WithId<Document>[] = [
        { _id: '1', category: 'A', value: 50 },
        { _id: '2', category: 'A', value: 30 },
        { _id: '3', category: 'A', value: 70 },
      ];

      const groupSpec = { _id: '$category', maxValue: { $max: '$value' } };
      const partials = planner.executePartialGroup(docs, groupSpec);

      expect(partials).toHaveLength(1);
      expect(partials[0].accumulators.maxValue).toEqual({ type: 'max', value: 70 });
    });

    it('should compute partial $first', () => {
      const docs: WithId<Document>[] = [
        { _id: '1', category: 'A', name: 'First' },
        { _id: '2', category: 'A', name: 'Second' },
      ];

      const groupSpec = { _id: '$category', firstName: { $first: '$name' } };
      const partials = planner.executePartialGroup(docs, groupSpec);

      expect(partials).toHaveLength(1);
      expect(partials[0].accumulators.firstName).toEqual({
        type: 'first',
        value: 'First',
        hasValue: true,
      });
    });

    it('should compute partial $last', () => {
      const docs: WithId<Document>[] = [
        { _id: '1', category: 'A', name: 'First' },
        { _id: '2', category: 'A', name: 'Second' },
      ];

      const groupSpec = { _id: '$category', lastName: { $last: '$name' } };
      const partials = planner.executePartialGroup(docs, groupSpec);

      expect(partials).toHaveLength(1);
      expect(partials[0].accumulators.lastName).toEqual({
        type: 'last',
        value: 'Second',
        hasValue: true,
      });
    });

    it('should compute partial $push', () => {
      const docs: WithId<Document>[] = [
        { _id: '1', category: 'A', tag: 'x' },
        { _id: '2', category: 'A', tag: 'y' },
      ];

      const groupSpec = { _id: '$category', tags: { $push: '$tag' } };
      const partials = planner.executePartialGroup(docs, groupSpec);

      expect(partials).toHaveLength(1);
      expect(partials[0].accumulators.tags).toEqual({
        type: 'push',
        values: ['x', 'y'],
      });
    });

    it('should compute partial $addToSet', () => {
      const docs: WithId<Document>[] = [
        { _id: '1', category: 'A', tag: 'x' },
        { _id: '2', category: 'A', tag: 'y' },
        { _id: '3', category: 'A', tag: 'x' },
      ];

      const groupSpec = { _id: '$category', tags: { $addToSet: '$tag' } };
      const partials = planner.executePartialGroup(docs, groupSpec);

      expect(partials).toHaveLength(1);
      const acc = partials[0].accumulators.tags as { type: 'addToSet'; values: Map<string, unknown> };
      expect(acc.type).toBe('addToSet');
      expect(acc.values.size).toBe(2);
      expect(Array.from(acc.values.values())).toContain('x');
      expect(Array.from(acc.values.values())).toContain('y');
    });

    it('should compute partial $count', () => {
      const docs: WithId<Document>[] = [
        { _id: '1', category: 'A' },
        { _id: '2', category: 'A' },
        { _id: '3', category: 'A' },
      ];

      const groupSpec = { _id: '$category', docCount: { $count: {} } };
      const partials = planner.executePartialGroup(docs, groupSpec);

      expect(partials).toHaveLength(1);
      expect(partials[0].accumulators.docCount).toEqual({ type: 'count', value: 3 });
    });

    it('should handle compound _id', () => {
      const docs: WithId<Document>[] = [
        { _id: '1', year: 2023, month: 1, amount: 100 },
        { _id: '2', year: 2023, month: 1, amount: 200 },
        { _id: '3', year: 2023, month: 2, amount: 150 },
      ];

      const groupSpec = {
        _id: { year: '$year', month: '$month' },
        total: { $sum: '$amount' },
      };
      const partials = planner.executePartialGroup(docs, groupSpec);

      expect(partials).toHaveLength(2);

      const jan = partials.find((p) => {
        const id = p._id as { year: number; month: number };
        return id.year === 2023 && id.month === 1;
      });
      expect(jan).toBeDefined();
      expect(jan!.accumulators.total).toEqual({ type: 'sum', value: 300 });
    });

    it('should handle _id: null (aggregate all)', () => {
      const docs: WithId<Document>[] = [
        { _id: '1', amount: 10 },
        { _id: '2', amount: 20 },
        { _id: '3', amount: 30 },
      ];

      const groupSpec = { _id: null, total: { $sum: '$amount' } };
      const partials = planner.executePartialGroup(docs, groupSpec);

      expect(partials).toHaveLength(1);
      expect(partials[0]._id).toBeNull();
      expect(partials[0].accumulators.total).toEqual({ type: 'sum', value: 60 });
    });
  });

  describe('mergePartialAggregates', () => {
    const planner = new DistributedAggregationPlanner();

    it('should merge $sum from multiple shards', () => {
      const shard1: PartialAggregate[] = [
        { _id: 'A', accumulators: { total: { type: 'sum', value: 100 } } },
      ];
      const shard2: PartialAggregate[] = [
        { _id: 'A', accumulators: { total: { type: 'sum', value: 200 } } },
      ];

      const groupSpec = { _id: '$category', total: { $sum: '$amount' } };
      const result = planner.mergePartialAggregates([shard1, shard2], groupSpec);

      expect(result).toHaveLength(1);
      expect(result[0]).toEqual({ _id: 'A', total: 300 });
    });

    it('should merge $avg correctly (sum/count)', () => {
      const shard1: PartialAggregate[] = [
        { _id: 'A', accumulators: { avgScore: { type: 'avg', sum: 180, count: 2 } } },
      ];
      const shard2: PartialAggregate[] = [
        { _id: 'A', accumulators: { avgScore: { type: 'avg', sum: 120, count: 2 } } },
      ];

      const groupSpec = { _id: '$category', avgScore: { $avg: '$score' } };
      const result = planner.mergePartialAggregates([shard1, shard2], groupSpec);

      expect(result).toHaveLength(1);
      // (180 + 120) / (2 + 2) = 300 / 4 = 75
      expect(result[0]).toEqual({ _id: 'A', avgScore: 75 });
    });

    it('should merge $min correctly', () => {
      const shard1: PartialAggregate[] = [
        { _id: 'A', accumulators: { minValue: { type: 'min', value: 50 } } },
      ];
      const shard2: PartialAggregate[] = [
        { _id: 'A', accumulators: { minValue: { type: 'min', value: 30 } } },
      ];

      const groupSpec = { _id: '$category', minValue: { $min: '$value' } };
      const result = planner.mergePartialAggregates([shard1, shard2], groupSpec);

      expect(result).toHaveLength(1);
      expect(result[0]).toEqual({ _id: 'A', minValue: 30 });
    });

    it('should merge $max correctly', () => {
      const shard1: PartialAggregate[] = [
        { _id: 'A', accumulators: { maxValue: { type: 'max', value: 50 } } },
      ];
      const shard2: PartialAggregate[] = [
        { _id: 'A', accumulators: { maxValue: { type: 'max', value: 80 } } },
      ];

      const groupSpec = { _id: '$category', maxValue: { $max: '$value' } };
      const result = planner.mergePartialAggregates([shard1, shard2], groupSpec);

      expect(result).toHaveLength(1);
      expect(result[0]).toEqual({ _id: 'A', maxValue: 80 });
    });

    it('should merge $push (concatenate arrays)', () => {
      const shard1: PartialAggregate[] = [
        { _id: 'A', accumulators: { tags: { type: 'push', values: ['x', 'y'] } } },
      ];
      const shard2: PartialAggregate[] = [
        { _id: 'A', accumulators: { tags: { type: 'push', values: ['z'] } } },
      ];

      const groupSpec = { _id: '$category', tags: { $push: '$tag' } };
      const result = planner.mergePartialAggregates([shard1, shard2], groupSpec);

      expect(result).toHaveLength(1);
      expect(result[0]).toEqual({ _id: 'A', tags: ['x', 'y', 'z'] });
    });

    it('should merge $addToSet (union of sets)', () => {
      const shard1Values = new Map<string, unknown>([
        ['"x"', 'x'],
        ['"y"', 'y'],
      ]);
      const shard2Values = new Map<string, unknown>([
        ['"y"', 'y'],
        ['"z"', 'z'],
      ]);

      const shard1: PartialAggregate[] = [
        { _id: 'A', accumulators: { tags: { type: 'addToSet', values: shard1Values } } },
      ];
      const shard2: PartialAggregate[] = [
        { _id: 'A', accumulators: { tags: { type: 'addToSet', values: shard2Values } } },
      ];

      const groupSpec = { _id: '$category', tags: { $addToSet: '$tag' } };
      const result = planner.mergePartialAggregates([shard1, shard2], groupSpec);

      expect(result).toHaveLength(1);
      const tags = (result[0] as { tags: string[] }).tags;
      expect(tags).toHaveLength(3);
      expect(tags).toContain('x');
      expect(tags).toContain('y');
      expect(tags).toContain('z');
    });

    it('should merge $count correctly', () => {
      const shard1: PartialAggregate[] = [
        { _id: 'A', accumulators: { docCount: { type: 'count', value: 5 } } },
      ];
      const shard2: PartialAggregate[] = [
        { _id: 'A', accumulators: { docCount: { type: 'count', value: 3 } } },
      ];

      const groupSpec = { _id: '$category', docCount: { $count: {} } };
      const result = planner.mergePartialAggregates([shard1, shard2], groupSpec);

      expect(result).toHaveLength(1);
      expect(result[0]).toEqual({ _id: 'A', docCount: 8 });
    });

    it('should handle groups appearing on only some shards', () => {
      const shard1: PartialAggregate[] = [
        { _id: 'A', accumulators: { total: { type: 'sum', value: 100 } } },
      ];
      const shard2: PartialAggregate[] = [
        { _id: 'B', accumulators: { total: { type: 'sum', value: 200 } } },
      ];

      const groupSpec = { _id: '$category', total: { $sum: '$amount' } };
      const result = planner.mergePartialAggregates([shard1, shard2], groupSpec);

      expect(result).toHaveLength(2);
      expect(result.find((r) => r._id === 'A')).toEqual({ _id: 'A', total: 100 });
      expect(result.find((r) => r._id === 'B')).toEqual({ _id: 'B', total: 200 });
    });

    it('should handle multiple accumulators per group', () => {
      const shard1: PartialAggregate[] = [
        {
          _id: 'A',
          accumulators: {
            total: { type: 'sum', value: 100 },
            docCount: { type: 'count', value: 2 },
          },
        },
      ];
      const shard2: PartialAggregate[] = [
        {
          _id: 'A',
          accumulators: {
            total: { type: 'sum', value: 150 },
            docCount: { type: 'count', value: 3 },
          },
        },
      ];

      const groupSpec = {
        _id: '$category',
        total: { $sum: '$amount' },
        docCount: { $count: {} },
      };
      const result = planner.mergePartialAggregates([shard1, shard2], groupSpec);

      expect(result).toHaveLength(1);
      expect(result[0]).toEqual({ _id: 'A', total: 250, docCount: 5 });
    });
  });
});

describe('DistributedAggregationExecutor', () => {
  describe('execute', () => {
    it('should execute distributed $group with $sum', async () => {
      const executor = createDistributedAggregationExecutor();

      const shardData = new Map<number, WithId<Document>[]>([
        [
          0,
          [
            { _id: '1', category: 'A', amount: 10 },
            { _id: '2', category: 'B', amount: 20 },
          ],
        ],
        [
          1,
          [
            { _id: '3', category: 'A', amount: 30 },
            { _id: '4', category: 'A', amount: 40 },
          ],
        ],
        [
          2,
          [
            { _id: '5', category: 'B', amount: 50 },
          ],
        ],
      ]);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(2);

      const catA = results.find((r) => r._id === 'A') as { total: number };
      const catB = results.find((r) => r._id === 'B') as { total: number };

      expect(catA.total).toBe(80); // 10 + 30 + 40
      expect(catB.total).toBe(70); // 20 + 50
    });

    it('should execute distributed $group with $avg', async () => {
      const executor = createDistributedAggregationExecutor();

      const shardData = new Map<number, WithId<Document>[]>([
        [
          0,
          [
            { _id: '1', category: 'A', score: 80 },
            { _id: '2', category: 'A', score: 90 },
          ],
        ],
        [
          1,
          [
            { _id: '3', category: 'A', score: 70 },
            { _id: '4', category: 'A', score: 60 },
          ],
        ],
      ]);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$category', avgScore: { $avg: '$score' } } },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(1);
      // (80 + 90 + 70 + 60) / 4 = 300 / 4 = 75
      expect((results[0] as { avgScore: number }).avgScore).toBe(75);
    });

    it('should execute distributed $group with $min and $max', async () => {
      const executor = createDistributedAggregationExecutor();

      const shardData = new Map<number, WithId<Document>[]>([
        [
          0,
          [
            { _id: '1', value: 50 },
            { _id: '2', value: 100 },
          ],
        ],
        [
          1,
          [
            { _id: '3', value: 25 },
            { _id: '4', value: 75 },
          ],
        ],
      ]);

      const pipeline: AggregationStage[] = [
        {
          $group: {
            _id: null,
            minValue: { $min: '$value' },
            maxValue: { $max: '$value' },
          },
        },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(1);
      expect((results[0] as { minValue: number }).minValue).toBe(25);
      expect((results[0] as { maxValue: number }).maxValue).toBe(100);
    });

    it('should execute $match before distributed $group', async () => {
      const executor = createDistributedAggregationExecutor();

      const shardData = new Map<number, WithId<Document>[]>([
        [
          0,
          [
            { _id: '1', status: 'active', category: 'A', amount: 10 },
            { _id: '2', status: 'inactive', category: 'A', amount: 100 },
          ],
        ],
        [
          1,
          [
            { _id: '3', status: 'active', category: 'A', amount: 20 },
            { _id: '4', status: 'active', category: 'B', amount: 30 },
          ],
        ],
      ]);

      const pipeline: AggregationStage[] = [
        { $match: { status: 'active' } },
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(2);

      const catA = results.find((r) => r._id === 'A') as { total: number };
      const catB = results.find((r) => r._id === 'B') as { total: number };

      expect(catA.total).toBe(30); // 10 + 20 (inactive filtered out)
      expect(catB.total).toBe(30);
    });

    it('should execute $sort after distributed $group', async () => {
      const executor = createDistributedAggregationExecutor();

      const shardData = new Map<number, WithId<Document>[]>([
        [
          0,
          [
            { _id: '1', category: 'A', amount: 10 },
            { _id: '2', category: 'B', amount: 30 },
          ],
        ],
        [
          1,
          [
            { _id: '3', category: 'C', amount: 20 },
          ],
        ],
      ]);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
        { $sort: { total: -1 } },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(3);
      expect(results[0]._id).toBe('B'); // 30
      expect(results[1]._id).toBe('C'); // 20
      expect(results[2]._id).toBe('A'); // 10
    });

    it('should execute $limit after distributed $group', async () => {
      const executor = createDistributedAggregationExecutor();

      const shardData = new Map<number, WithId<Document>[]>([
        [
          0,
          [
            { _id: '1', category: 'A' },
            { _id: '2', category: 'B' },
          ],
        ],
        [
          1,
          [
            { _id: '3', category: 'C' },
            { _id: '4', category: 'D' },
          ],
        ],
      ]);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$category', count: { $sum: 1 } } },
        { $limit: 2 },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(2);
    });

    it('should handle empty shards', async () => {
      const executor = createDistributedAggregationExecutor();

      const shardData = new Map<number, WithId<Document>[]>([
        [0, []],
        [
          1,
          [
            { _id: '1', category: 'A', amount: 10 },
          ],
        ],
        [2, []],
      ]);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(1);
      expect(results[0]).toEqual({ _id: 'A', total: 10 });
    });

    it('should execute complex pipeline with multiple stages', async () => {
      const executor = createDistributedAggregationExecutor();

      const shardData = new Map<number, WithId<Document>[]>([
        [
          0,
          [
            { _id: '1', region: 'US', sales: 100, active: true },
            { _id: '2', region: 'EU', sales: 200, active: true },
            { _id: '3', region: 'US', sales: 50, active: false },
          ],
        ],
        [
          1,
          [
            { _id: '4', region: 'US', sales: 150, active: true },
            { _id: '5', region: 'EU', sales: 100, active: true },
            { _id: '6', region: 'APAC', sales: 75, active: true },
          ],
        ],
      ]);

      const pipeline: AggregationStage[] = [
        { $match: { active: true } },
        { $group: { _id: '$region', totalSales: { $sum: '$sales' }, count: { $sum: 1 } } },
        { $sort: { totalSales: -1 } },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(3);

      // EU: 200 + 100 = 300
      // US: 100 + 150 = 250 (50 filtered out)
      // APAC: 75
      expect(results[0]).toEqual({ _id: 'EU', totalSales: 300, count: 2 });
      expect(results[1]).toEqual({ _id: 'US', totalSales: 250, count: 2 });
      expect(results[2]).toEqual({ _id: 'APAC', totalSales: 75, count: 1 });
    });

    it('should handle pipeline without $group', async () => {
      const executor = createDistributedAggregationExecutor();

      const shardData = new Map<number, WithId<Document>[]>([
        [
          0,
          [
            { _id: '1', name: 'Alice', age: 30 },
            { _id: '2', name: 'Bob', age: 25 },
          ],
        ],
        [
          1,
          [
            { _id: '3', name: 'Charlie', age: 35 },
          ],
        ],
      ]);

      const pipeline: AggregationStage[] = [
        { $match: { age: { $gte: 30 } } },
        { $project: { name: 1 } },
      ];

      const results = await executor.execute(shardData, pipeline);

      expect(results).toHaveLength(2);
      const names = results.map((r) => (r as { name: string }).name);
      expect(names).toContain('Alice');
      expect(names).toContain('Charlie');
    });
  });
});

describe('DistributedAggregator', () => {
  describe('execute with string shard IDs', () => {
    it('should execute distributed $group with $sum across named shards', async () => {
      const { DistributedAggregator } = await import('../../../src/client/distributed-aggregation.js');
      const aggregator = new DistributedAggregator();

      // Set up shard data
      aggregator.setShardData('shard-0', [
        { _id: '1', category: 'A', amount: 10 },
        { _id: '2', category: 'B', amount: 20 },
      ]);
      aggregator.setShardData('shard-1', [
        { _id: '3', category: 'A', amount: 30 },
        { _id: '4', category: 'A', amount: 40 },
      ]);
      aggregator.setShardData('shard-2', [
        { _id: '5', category: 'B', amount: 50 },
      ]);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
      ];

      const results = await aggregator.execute(pipeline, ['shard-0', 'shard-1', 'shard-2']);

      expect(results).toHaveLength(2);

      const catA = results.find((r) => r._id === 'A') as { total: number };
      const catB = results.find((r) => r._id === 'B') as { total: number };

      expect(catA.total).toBe(80); // 10 + 30 + 40
      expect(catB.total).toBe(70); // 20 + 50
    });

    it('should execute $match before distributed $group', async () => {
      const { DistributedAggregator } = await import('../../../src/client/distributed-aggregation.js');
      const aggregator = new DistributedAggregator();

      aggregator.setShardData('shard-0', [
        { _id: '1', status: 'active', category: 'A', amount: 10 },
        { _id: '2', status: 'inactive', category: 'A', amount: 100 },
      ]);
      aggregator.setShardData('shard-1', [
        { _id: '3', status: 'active', category: 'A', amount: 20 },
        { _id: '4', status: 'active', category: 'B', amount: 30 },
      ]);

      const pipeline: AggregationStage[] = [
        { $match: { status: 'active' } },
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
      ];

      const results = await aggregator.execute(pipeline, ['shard-0', 'shard-1']);

      expect(results).toHaveLength(2);

      const catA = results.find((r) => r._id === 'A') as { total: number };
      const catB = results.find((r) => r._id === 'B') as { total: number };

      expect(catA.total).toBe(30); // 10 + 20 (inactive filtered out)
      expect(catB.total).toBe(30);
    });

    it('should execute $sort after distributed $group', async () => {
      const { DistributedAggregator } = await import('../../../src/client/distributed-aggregation.js');
      const aggregator = new DistributedAggregator();

      aggregator.setShardData('shard-0', [
        { _id: '1', category: 'A', amount: 10 },
        { _id: '2', category: 'B', amount: 30 },
      ]);
      aggregator.setShardData('shard-1', [
        { _id: '3', category: 'C', amount: 20 },
      ]);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
        { $sort: { total: -1 } },
      ];

      const results = await aggregator.execute(pipeline, ['shard-0', 'shard-1']);

      expect(results).toHaveLength(3);
      expect(results[0]._id).toBe('B'); // 30
      expect(results[1]._id).toBe('C'); // 20
      expect(results[2]._id).toBe('A'); // 10
    });

    it('should return empty array for empty shard list', async () => {
      const { DistributedAggregator } = await import('../../../src/client/distributed-aggregation.js');
      const aggregator = new DistributedAggregator();

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$category', count: { $sum: 1 } } },
      ];

      const results = await aggregator.execute(pipeline, []);

      expect(results).toHaveLength(0);
    });

    it('should handle empty shards gracefully', async () => {
      const { DistributedAggregator } = await import('../../../src/client/distributed-aggregation.js');
      const aggregator = new DistributedAggregator();

      aggregator.setShardData('shard-0', []);
      aggregator.setShardData('shard-1', [
        { _id: '1', category: 'A', amount: 10 },
      ]);
      aggregator.setShardData('shard-2', []);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
      ];

      const results = await aggregator.execute(pipeline, ['shard-0', 'shard-1', 'shard-2']);

      expect(results).toHaveLength(1);
      expect(results[0]).toEqual({ _id: 'A', total: 10 });
    });
  });

  describe('$sort + $limit optimization', () => {
    it('should apply sort+limit optimization for pipelines without $group', async () => {
      const { DistributedAggregator } = await import('../../../src/client/distributed-aggregation.js');
      const aggregator = new DistributedAggregator({ sortLimitOptimization: true });

      // Each shard has 5 documents
      aggregator.setShardData('shard-0', [
        { _id: '1', score: 90 },
        { _id: '2', score: 85 },
        { _id: '3', score: 80 },
        { _id: '4', score: 75 },
        { _id: '5', score: 70 },
      ]);
      aggregator.setShardData('shard-1', [
        { _id: '6', score: 95 },
        { _id: '7', score: 88 },
        { _id: '8', score: 82 },
        { _id: '9', score: 77 },
        { _id: '10', score: 72 },
      ]);

      const pipeline: AggregationStage[] = [
        { $sort: { score: -1 } },
        { $limit: 3 },
      ];

      const results = await aggregator.execute(pipeline, ['shard-0', 'shard-1']);

      expect(results).toHaveLength(3);
      // Top 3 scores across both shards: 95, 90, 88
      expect((results[0] as { score: number }).score).toBe(95);
      expect((results[1] as { score: number }).score).toBe(90);
      expect((results[2] as { score: number }).score).toBe(88);
    });

    it('should handle $match before $sort + $limit', async () => {
      const { DistributedAggregator } = await import('../../../src/client/distributed-aggregation.js');
      const aggregator = new DistributedAggregator({ sortLimitOptimization: true });

      aggregator.setShardData('shard-0', [
        { _id: '1', status: 'active', score: 90 },
        { _id: '2', status: 'inactive', score: 100 },
        { _id: '3', status: 'active', score: 80 },
      ]);
      aggregator.setShardData('shard-1', [
        { _id: '4', status: 'active', score: 95 },
        { _id: '5', status: 'inactive', score: 99 },
      ]);

      const pipeline: AggregationStage[] = [
        { $match: { status: 'active' } },
        { $sort: { score: -1 } },
        { $limit: 2 },
      ];

      const results = await aggregator.execute(pipeline, ['shard-0', 'shard-1']);

      expect(results).toHaveLength(2);
      // Top 2 active scores: 95, 90
      expect((results[0] as { score: number }).score).toBe(95);
      expect((results[1] as { score: number }).score).toBe(90);
    });

    it('should work with $sort ascending order', async () => {
      const { DistributedAggregator } = await import('../../../src/client/distributed-aggregation.js');
      const aggregator = new DistributedAggregator({ sortLimitOptimization: true });

      aggregator.setShardData('shard-0', [
        { _id: '1', price: 100 },
        { _id: '2', price: 50 },
        { _id: '3', price: 75 },
      ]);
      aggregator.setShardData('shard-1', [
        { _id: '4', price: 25 },
        { _id: '5', price: 60 },
      ]);

      const pipeline: AggregationStage[] = [
        { $sort: { price: 1 } },
        { $limit: 2 },
      ];

      const results = await aggregator.execute(pipeline, ['shard-0', 'shard-1']);

      expect(results).toHaveLength(2);
      // Lowest 2 prices: 25, 50
      expect((results[0] as { price: number }).price).toBe(25);
      expect((results[1] as { price: number }).price).toBe(50);
    });

    it('should disable optimization when sortLimitOptimization is false', async () => {
      const { DistributedAggregator } = await import('../../../src/client/distributed-aggregation.js');
      const aggregator = new DistributedAggregator({ sortLimitOptimization: false });

      aggregator.setShardData('shard-0', [
        { _id: '1', score: 90 },
        { _id: '2', score: 85 },
      ]);
      aggregator.setShardData('shard-1', [
        { _id: '3', score: 95 },
        { _id: '4', score: 88 },
      ]);

      const pipeline: AggregationStage[] = [
        { $sort: { score: -1 } },
        { $limit: 2 },
      ];

      // Should still work correctly, just without the optimization
      const results = await aggregator.execute(pipeline, ['shard-0', 'shard-1']);

      expect(results).toHaveLength(2);
      expect((results[0] as { score: number }).score).toBe(95);
      expect((results[1] as { score: number }).score).toBe(90);
    });
  });

  describe('complex multi-stage pipelines', () => {
    it('should execute full pipeline with $match, $group, $sort, $limit', async () => {
      const { DistributedAggregator } = await import('../../../src/client/distributed-aggregation.js');
      const aggregator = new DistributedAggregator();

      aggregator.setShardData('shard-0', [
        { _id: '1', region: 'US', sales: 100, active: true },
        { _id: '2', region: 'EU', sales: 200, active: true },
        { _id: '3', region: 'US', sales: 50, active: false },
      ]);
      aggregator.setShardData('shard-1', [
        { _id: '4', region: 'US', sales: 150, active: true },
        { _id: '5', region: 'EU', sales: 100, active: true },
        { _id: '6', region: 'APAC', sales: 75, active: true },
      ]);

      const pipeline: AggregationStage[] = [
        { $match: { active: true } },
        { $group: { _id: '$region', totalSales: { $sum: '$sales' }, count: { $sum: 1 } } },
        { $sort: { totalSales: -1 } },
        { $limit: 2 },
      ];

      const results = await aggregator.execute(pipeline, ['shard-0', 'shard-1']);

      expect(results).toHaveLength(2);
      // EU: 200 + 100 = 300
      // US: 100 + 150 = 250 (inactive 50 filtered out)
      expect(results[0]).toEqual({ _id: 'EU', totalSales: 300, count: 2 });
      expect(results[1]).toEqual({ _id: 'US', totalSales: 250, count: 2 });
    });

    it('should handle $addFields in reduce phase', async () => {
      const { DistributedAggregator } = await import('../../../src/client/distributed-aggregation.js');
      const aggregator = new DistributedAggregator();

      aggregator.setShardData('shard-0', [
        { _id: '1', category: 'A', amount: 100 },
        { _id: '2', category: 'A', amount: 200 },
      ]);
      aggregator.setShardData('shard-1', [
        { _id: '3', category: 'B', amount: 150 },
      ]);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
        { $addFields: { category: '$_id' } },
        { $sort: { total: -1 } },
      ];

      const results = await aggregator.execute(pipeline, ['shard-0', 'shard-1']);

      expect(results).toHaveLength(2);
      expect(results[0]).toMatchObject({ _id: 'A', total: 300, category: 'A' });
      expect(results[1]).toMatchObject({ _id: 'B', total: 150, category: 'B' });
    });

    it('should handle $unset in reduce phase', async () => {
      const { DistributedAggregator } = await import('../../../src/client/distributed-aggregation.js');
      const aggregator = new DistributedAggregator();

      aggregator.setShardData('shard-0', [
        { _id: '1', category: 'A', amount: 100 },
      ]);
      aggregator.setShardData('shard-1', [
        { _id: '2', category: 'B', amount: 200 },
      ]);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
        { $unset: '_id' },
      ];

      const results = await aggregator.execute(pipeline, ['shard-0', 'shard-1']);

      expect(results).toHaveLength(2);
      for (const result of results) {
        expect(result).not.toHaveProperty('_id');
        expect(result).toHaveProperty('total');
      }
    });

    it('should handle $skip in reduce phase', async () => {
      const { DistributedAggregator } = await import('../../../src/client/distributed-aggregation.js');
      const aggregator = new DistributedAggregator();

      aggregator.setShardData('shard-0', [
        { _id: '1', category: 'A', amount: 100 },
        { _id: '2', category: 'B', amount: 200 },
      ]);
      aggregator.setShardData('shard-1', [
        { _id: '3', category: 'C', amount: 300 },
      ]);

      const pipeline: AggregationStage[] = [
        { $group: { _id: '$category', total: { $sum: '$amount' } } },
        { $sort: { total: -1 } },
        { $skip: 1 },
        { $limit: 1 },
      ];

      const results = await aggregator.execute(pipeline, ['shard-0', 'shard-1']);

      expect(results).toHaveLength(1);
      // C=300, B=200, A=100 -> skip 1 -> B=200
      expect(results[0]).toEqual({ _id: 'B', total: 200 });
    });
  });
});
