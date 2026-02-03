/**
 * MongoLake Distributed Aggregation Engine
 *
 * Implements map-reduce style distributed aggregation across shards.
 *
 * Phase Classification:
 * - Map Phase (per-shard): $match, $project, $addFields, $set, $unset, $limit, $skip, $sort, $unwind
 * - Reduce Phase (cross-shard coordination): $group, $lookup, $count
 *
 * For $group, we use partial aggregation:
 * 1. Map Phase: Each shard computes partial aggregates
 * 2. Reduce Phase: Merge partial aggregates into final results
 *
 * Accumulators have different merge strategies:
 * - $sum: sum of partial sums
 * - $avg: sum(partial_sum) / sum(partial_count)
 * - $min: min of partial mins
 * - $max: max of partial maxs
 * - $first: first of firsts (with ordering)
 * - $last: last of lasts (with ordering)
 * - $push: concat arrays
 * - $addToSet: union of sets
 * - $count: sum of counts
 */

import type {
  Document,
  WithId,
  AggregationStage,
  GroupStage,
} from '@types';
import { getNestedValue } from '@utils/nested.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Execution phase for a pipeline stage
 */
export type ExecutionPhase = 'map' | 'reduce' | 'barrier';

/**
 * Partial aggregate for a single group
 * Contains intermediate values that can be merged
 */
export interface PartialAggregate {
  _id: unknown;
  accumulators: Record<string, PartialAccumulatorValue>;
}

/**
 * Value types for partial accumulators
 */
export type PartialAccumulatorValue =
  | { type: 'sum'; value: number }
  | { type: 'avg'; sum: number; count: number }
  | { type: 'min'; value: unknown }
  | { type: 'max'; value: unknown }
  | { type: 'first'; value: unknown; hasValue: boolean }
  | { type: 'last'; value: unknown; hasValue: boolean }
  | { type: 'push'; values: unknown[] }
  | { type: 'addToSet'; values: Map<string, unknown> }
  | { type: 'count'; value: number };

/**
 * Result of pipeline analysis
 */
export interface PipelineAnalysis {
  /** Whether this pipeline can be distributed */
  canDistribute: boolean;
  /** Reason if distribution is not possible */
  reason?: string;
  /** Classified stages with execution phases */
  stages: ClassifiedStage[];
  /** Whether a $group stage is present */
  hasGroup: boolean;
  /** Index of the $group stage (if present) */
  groupStageIndex?: number;
}

/**
 * A pipeline stage with its execution phase
 */
export interface ClassifiedStage {
  stage: AggregationStage;
  phase: ExecutionPhase;
  stageType: string;
}

/**
 * Shard execution result
 */
export interface ShardResult {
  shardId: number;
  documents?: WithId<Document>[];
  partialAggregates?: PartialAggregate[];
}

/**
 * Options for distributed execution
 */
export interface DistributedAggregationOptions {
  /** Number of shards (for testing) */
  shardCount?: number;
}

// ============================================================================
// DistributedAggregationPlanner
// ============================================================================

/**
 * Plans and executes distributed aggregation pipelines.
 *
 * Supports map-reduce style execution where:
 * - Map phase runs on each shard independently
 * - Reduce phase merges results from all shards
 *
 * Key optimization: $group uses partial aggregation where each shard
 * computes intermediate results that are then merged.
 */
export class DistributedAggregationPlanner {
  /**
   * Analyze a pipeline for distributed execution
   */
  analyzePipeline(pipeline: AggregationStage[]): PipelineAnalysis {
    const stages: ClassifiedStage[] = [];
    let hasGroup = false;
    let groupStageIndex: number | undefined;
    let canDistribute = true;
    let reason: string | undefined;

    for (let i = 0; i < pipeline.length; i++) {
      const stage = pipeline[i]!;
      const classification = this.classifyStage(stage);
      stages.push(classification);

      if (classification.stageType === '$group') {
        hasGroup = true;
        groupStageIndex = i;
      }

      // Check for distribution blockers
      if (classification.stageType === '$lookup') {
        // $lookup currently loads entire foreign collection
        // This is a known limitation we're documenting
        canDistribute = false;
        reason = '$lookup loads entire collection - requires cross-shard coordination not yet implemented';
      }
    }

    return {
      canDistribute,
      reason,
      stages,
      hasGroup,
      groupStageIndex,
    };
  }

  /**
   * Classify a single pipeline stage
   */
  private classifyStage(stage: AggregationStage): ClassifiedStage {
    // Map phase stages (can run per-shard)
    if ('$match' in stage) {
      return { stage, phase: 'map', stageType: '$match' };
    }
    if ('$project' in stage) {
      return { stage, phase: 'map', stageType: '$project' };
    }
    if ('$addFields' in stage) {
      return { stage, phase: 'map', stageType: '$addFields' };
    }
    if ('$set' in stage) {
      return { stage, phase: 'map', stageType: '$set' };
    }
    if ('$unset' in stage) {
      return { stage, phase: 'map', stageType: '$unset' };
    }
    if ('$unwind' in stage) {
      return { stage, phase: 'map', stageType: '$unwind' };
    }

    // Barrier stages (require all data before continuing)
    // Note: $limit/$skip/$sort after $group need reduce phase
    if ('$limit' in stage) {
      return { stage, phase: 'barrier', stageType: '$limit' };
    }
    if ('$skip' in stage) {
      return { stage, phase: 'barrier', stageType: '$skip' };
    }
    if ('$sort' in stage) {
      return { stage, phase: 'barrier', stageType: '$sort' };
    }

    // Reduce phase stages (require cross-shard coordination)
    if ('$group' in stage) {
      return { stage, phase: 'reduce', stageType: '$group' };
    }
    if ('$lookup' in stage) {
      return { stage, phase: 'reduce', stageType: '$lookup' };
    }
    if ('$count' in stage) {
      return { stage, phase: 'reduce', stageType: '$count' };
    }

    // Default to barrier (safe choice for unknown stages)
    return { stage, phase: 'barrier', stageType: 'unknown' };
  }

  /**
   * Split pipeline into map and reduce phases around $group
   */
  splitPipeline(pipeline: AggregationStage[]): {
    mapPhase: AggregationStage[];
    groupStage?: GroupStage;
    reducePhase: AggregationStage[];
  } {
    const analysis = this.analyzePipeline(pipeline);

    if (!analysis.hasGroup || analysis.groupStageIndex === undefined) {
      // No $group - all stages run in map phase
      return {
        mapPhase: pipeline,
        reducePhase: [],
      };
    }

    const groupIndex = analysis.groupStageIndex!;
    const groupStageObj = pipeline[groupIndex]!;
    const groupStage = '$group' in groupStageObj ? groupStageObj.$group : undefined;

    return {
      mapPhase: pipeline.slice(0, groupIndex),
      groupStage,
      reducePhase: pipeline.slice(groupIndex + 1),
    };
  }

  /**
   * Execute partial $group on a set of documents (map phase)
   * Returns partial aggregates that can be merged
   */
  executePartialGroup(
    docs: WithId<Document>[],
    groupSpec: GroupStage
  ): PartialAggregate[] {
    const groups = new Map<string, PartialAggregate>();

    for (const doc of docs) {
      const groupKey = this.evaluateGroupId(doc as Record<string, unknown>, groupSpec._id);
      const groupId = this.parseGroupId(groupKey, groupSpec._id);

      if (!groups.has(groupKey)) {
        groups.set(groupKey, {
          _id: groupId,
          accumulators: {},
        });
      }

      const partial = groups.get(groupKey)!;

      // Update each accumulator with this document
      for (const [field, expr] of Object.entries(groupSpec)) {
        if (field === '_id') continue;
        const accExpr = expr as Record<string, unknown>;
        this.updatePartialAccumulator(partial, field, accExpr, doc as Record<string, unknown>);
      }
    }

    return Array.from(groups.values());
  }

  /**
   * Merge partial aggregates from multiple shards (reduce phase)
   */
  mergePartialAggregates(
    shardResults: PartialAggregate[][],
    _groupSpec: GroupStage
  ): WithId<Document>[] {
    const mergedGroups = new Map<string, PartialAggregate>();

    // Collect all partial aggregates by group key
    for (const shardPartials of shardResults) {
      for (const partial of shardPartials) {
        const key = JSON.stringify(partial._id);

        if (!mergedGroups.has(key)) {
          mergedGroups.set(key, {
            _id: partial._id,
            accumulators: {},
          });
        }

        const merged = mergedGroups.get(key)!;

        // Merge each accumulator
        for (const [field, value] of Object.entries(partial.accumulators)) {
          if (!merged.accumulators[field]) {
            merged.accumulators[field] = this.clonePartialValue(value);
          } else {
            this.mergeAccumulatorValue(merged.accumulators[field], value);
          }
        }
      }
    }

    // Finalize aggregates into result documents
    const results: WithId<Document>[] = [];
    for (const [, partial] of mergedGroups) {
      const result: Record<string, unknown> = { _id: partial._id };

      for (const [field, value] of Object.entries(partial.accumulators)) {
        result[field] = this.finalizeAccumulator(value);
      }

      results.push(result as WithId<Document>);
    }

    return results;
  }

  /**
   * Update a partial accumulator with a new document value
   */
  private updatePartialAccumulator(
    partial: PartialAggregate,
    field: string,
    accExpr: Record<string, unknown>,
    doc: Record<string, unknown>
  ): void {
    if ('$sum' in accExpr) {
      const value = this.evaluateAccumulatorInput(accExpr.$sum, doc);
      const numValue = typeof value === 'number' ? value : 0;

      if (!partial.accumulators[field]) {
        partial.accumulators[field] = { type: 'sum', value: 0 };
      }
      const acc = partial.accumulators[field] as { type: 'sum'; value: number };
      acc.value += numValue;
      return;
    }

    if ('$avg' in accExpr) {
      const value = this.evaluateAccumulatorInput(accExpr.$avg, doc);
      const numValue = typeof value === 'number' ? value : 0;

      if (!partial.accumulators[field]) {
        partial.accumulators[field] = { type: 'avg', sum: 0, count: 0 };
      }
      const acc = partial.accumulators[field] as { type: 'avg'; sum: number; count: number };
      acc.sum += numValue;
      acc.count += 1;
      return;
    }

    if ('$min' in accExpr) {
      const value = this.evaluateAccumulatorInput(accExpr.$min, doc);

      if (!partial.accumulators[field]) {
        partial.accumulators[field] = { type: 'min', value };
      } else {
        const acc = partial.accumulators[field] as { type: 'min'; value: unknown };
        if (value !== null && value !== undefined) {
          if (acc.value === null || acc.value === undefined || value < acc.value) {
            acc.value = value;
          }
        }
      }
      return;
    }

    if ('$max' in accExpr) {
      const value = this.evaluateAccumulatorInput(accExpr.$max, doc);

      if (!partial.accumulators[field]) {
        partial.accumulators[field] = { type: 'max', value };
      } else {
        const acc = partial.accumulators[field] as { type: 'max'; value: unknown };
        if (value !== null && value !== undefined) {
          if (acc.value === null || acc.value === undefined || value > acc.value) {
            acc.value = value;
          }
        }
      }
      return;
    }

    if ('$first' in accExpr) {
      if (!partial.accumulators[field]) {
        const value = this.evaluateAccumulatorInput(accExpr.$first, doc);
        partial.accumulators[field] = { type: 'first', value, hasValue: true };
      }
      // $first only takes the first value, ignore subsequent
      return;
    }

    if ('$last' in accExpr) {
      const value = this.evaluateAccumulatorInput(accExpr.$last, doc);
      if (!partial.accumulators[field]) {
        partial.accumulators[field] = { type: 'last', value, hasValue: true };
      } else {
        const acc = partial.accumulators[field] as { type: 'last'; value: unknown; hasValue: boolean };
        acc.value = value;
        acc.hasValue = true;
      }
      return;
    }

    if ('$push' in accExpr) {
      const value = this.evaluateAccumulatorInput(accExpr.$push, doc);

      if (!partial.accumulators[field]) {
        partial.accumulators[field] = { type: 'push', values: [] };
      }
      const acc = partial.accumulators[field] as { type: 'push'; values: unknown[] };
      acc.values.push(value);
      return;
    }

    if ('$addToSet' in accExpr) {
      const value = this.evaluateAccumulatorInput(accExpr.$addToSet, doc);

      if (!partial.accumulators[field]) {
        partial.accumulators[field] = { type: 'addToSet', values: new Map() };
      }
      const acc = partial.accumulators[field] as { type: 'addToSet'; values: Map<string, unknown> };
      const key = JSON.stringify(value);
      if (!acc.values.has(key)) {
        acc.values.set(key, value);
      }
      return;
    }

    if ('$count' in accExpr) {
      if (!partial.accumulators[field]) {
        partial.accumulators[field] = { type: 'count', value: 0 };
      }
      const acc = partial.accumulators[field] as { type: 'count'; value: number };
      acc.value += 1;
      return;
    }
  }

  /**
   * Evaluate an accumulator input expression
   */
  private evaluateAccumulatorInput(expr: unknown, doc: Record<string, unknown>): unknown {
    if (typeof expr === 'number') {
      return expr;
    }
    if (typeof expr === 'string' && expr.startsWith('$')) {
      return getNestedValue(doc, expr.slice(1));
    }
    return expr;
  }

  /**
   * Merge two partial accumulator values
   */
  private mergeAccumulatorValue(
    target: PartialAccumulatorValue,
    source: PartialAccumulatorValue
  ): void {
    if (target.type !== source.type) {
      throw new Error(`Cannot merge accumulators of different types: ${target.type} vs ${source.type}`);
    }

    switch (target.type) {
      case 'sum': {
        const s = source as { type: 'sum'; value: number };
        target.value += s.value;
        break;
      }

      case 'avg': {
        const s = source as { type: 'avg'; sum: number; count: number };
        target.sum += s.sum;
        target.count += s.count;
        break;
      }

      case 'min': {
        const s = source as { type: 'min'; value: unknown };
        if (s.value !== null && s.value !== undefined) {
          if (target.value === null || target.value === undefined || s.value < target.value) {
            target.value = s.value;
          }
        }
        break;
      }

      case 'max': {
        const s = source as { type: 'max'; value: unknown };
        if (s.value !== null && s.value !== undefined) {
          if (target.value === null || target.value === undefined || s.value > target.value) {
            target.value = s.value;
          }
        }
        break;
      }

      case 'first': {
        // First accumulator keeps the first value seen
        // In distributed setting, we need ordering metadata to determine true first
        // For now, keep the target (first shard's first)
        break;
      }

      case 'last': {
        // Last accumulator needs ordering metadata to determine true last
        // For now, use the source (later shard's last)
        const s = source as { type: 'last'; value: unknown; hasValue: boolean };
        if (s.hasValue) {
          target.value = s.value;
          target.hasValue = true;
        }
        break;
      }

      case 'push': {
        const s = source as { type: 'push'; values: unknown[] };
        target.values.push(...s.values);
        break;
      }

      case 'addToSet': {
        const s = source as { type: 'addToSet'; values: Map<string, unknown> };
        for (const [key, value] of s.values) {
          if (!target.values.has(key)) {
            target.values.set(key, value);
          }
        }
        break;
      }

      case 'count': {
        const s = source as { type: 'count'; value: number };
        target.value += s.value;
        break;
      }
    }
  }

  /**
   * Clone a partial accumulator value
   */
  private clonePartialValue(value: PartialAccumulatorValue): PartialAccumulatorValue {
    switch (value.type) {
      case 'sum':
        return { type: 'sum', value: value.value };
      case 'avg':
        return { type: 'avg', sum: value.sum, count: value.count };
      case 'min':
        return { type: 'min', value: value.value };
      case 'max':
        return { type: 'max', value: value.value };
      case 'first':
        return { type: 'first', value: value.value, hasValue: value.hasValue };
      case 'last':
        return { type: 'last', value: value.value, hasValue: value.hasValue };
      case 'push':
        return { type: 'push', values: [...value.values] };
      case 'addToSet':
        return { type: 'addToSet', values: new Map(value.values) };
      case 'count':
        return { type: 'count', value: value.value };
    }
  }

  /**
   * Finalize a partial accumulator into its final value
   */
  private finalizeAccumulator(value: PartialAccumulatorValue): unknown {
    switch (value.type) {
      case 'sum':
        return value.value;
      case 'avg':
        return value.count > 0 ? value.sum / value.count : 0;
      case 'min':
        return value.value;
      case 'max':
        return value.value;
      case 'first':
        return value.value;
      case 'last':
        return value.value;
      case 'push':
        return value.values;
      case 'addToSet':
        return Array.from(value.values.values());
      case 'count':
        return value.value;
    }
  }

  /**
   * Evaluate _id expression for $group stage
   */
  private evaluateGroupId(doc: Record<string, unknown>, idExpr: unknown): string {
    if (idExpr === null) {
      return '__all__';
    }

    if (typeof idExpr === 'string' && idExpr.startsWith('$')) {
      const value = getNestedValue(doc, idExpr.slice(1));
      return value === null || value === undefined ? '__null__' : String(value);
    }

    if (typeof idExpr === 'object' && idExpr !== null) {
      // Compound _id expression (e.g., { year: "$year", month: "$month" })
      const idObj: Record<string, unknown> = {};
      for (const [key, value] of Object.entries(idExpr)) {
        if (typeof value === 'string' && value.startsWith('$')) {
          idObj[key] = getNestedValue(doc, value.slice(1));
        } else {
          idObj[key] = value;
        }
      }
      return JSON.stringify(idObj);
    }

    return String(idExpr);
  }

  /**
   * Parse compound _id back to object
   */
  private parseGroupId(groupId: string, idExpr: unknown): unknown {
    if (groupId === '__all__') {
      return null;
    }
    if (groupId === '__null__') {
      return null;
    }
    if (typeof idExpr === 'object' && idExpr !== null && !Array.isArray(idExpr)) {
      try {
        return JSON.parse(groupId);
      } catch {
        return groupId;
      }
    }
    return groupId;
  }
}

// ============================================================================
// Distributed Aggregation Executor
// ============================================================================

/**
 * Executes distributed aggregation across simulated shards.
 *
 * In a real deployment, each shard would be a separate Durable Object.
 * This implementation simulates the distribution for testing and
 * demonstration purposes.
 */
export class DistributedAggregationExecutor {
  private planner: DistributedAggregationPlanner;

  constructor() {
    this.planner = new DistributedAggregationPlanner();
  }

  /**
   * Execute a distributed aggregation pipeline
   *
   * @param shardData - Map of shard ID to documents on that shard
   * @param pipeline - Aggregation pipeline to execute
   * @returns Aggregated results
   */
  async execute(
    shardData: Map<number, WithId<Document>[]>,
    pipeline: AggregationStage[]
  ): Promise<WithId<Document>[]> {
    const analysis = this.planner.analyzePipeline(pipeline);
    const split = this.planner.splitPipeline(pipeline);

    if (!analysis.hasGroup || !split.groupStage) {
      // No $group - just merge results from map phase
      // In real implementation, this would need careful handling of $limit, $sort, etc.
      let allDocs: WithId<Document>[] = [];
      for (const [, docs] of shardData) {
        allDocs = allDocs.concat(docs);
      }
      // Execute full pipeline on merged data
      return this.executeLocalPipeline(allDocs, pipeline);
    }

    // Phase 1: Execute map phase on each shard
    const shardPartials: PartialAggregate[][] = [];
    for (const [, docs] of shardData) {
      // Execute map phase stages
      const mapResult = await this.executeLocalPipeline(docs, split.mapPhase);
      // Execute partial group
      const partials = this.planner.executePartialGroup(mapResult, split.groupStage);
      shardPartials.push(partials);
    }

    // Phase 2: Merge partial aggregates
    let results = this.planner.mergePartialAggregates(shardPartials, split.groupStage);

    // Phase 3: Execute reduce phase stages
    if (split.reducePhase.length > 0) {
      results = await this.executeLocalPipeline(results, split.reducePhase);
    }

    return results;
  }

  /**
   * Execute pipeline stages locally (non-distributed)
   */
  private async executeLocalPipeline(
    docs: WithId<Document>[],
    pipeline: AggregationStage[]
  ): Promise<WithId<Document>[]> {
    let result = docs;

    for (const stage of pipeline) {
      result = await this.executeStage(result, stage);
    }

    return result;
  }

  /**
   * Execute a single pipeline stage
   */
  private async executeStage(
    docs: WithId<Document>[],
    stage: AggregationStage
  ): Promise<WithId<Document>[]> {
    if ('$match' in stage) {
      return docs.filter((doc) => this.matchesFilter(doc, stage.$match));
    }

    if ('$project' in stage) {
      return docs.map((doc) => this.applyProjection(doc, stage.$project));
    }

    if ('$sort' in stage) {
      return this.sortDocuments(docs, stage.$sort);
    }

    if ('$limit' in stage) {
      return docs.slice(0, stage.$limit);
    }

    if ('$skip' in stage) {
      return docs.slice(stage.$skip);
    }

    if ('$addFields' in stage || '$set' in stage) {
      const fields = '$addFields' in stage ? stage.$addFields : ('$set' in stage ? stage.$set : {});
      return docs.map((doc) => {
        const newDoc = { ...doc } as Record<string, unknown>;
        for (const [field, expr] of Object.entries(fields)) {
          if (typeof expr === 'string' && expr.startsWith('$')) {
            newDoc[field] = getNestedValue(doc as Record<string, unknown>, expr.slice(1));
          } else {
            newDoc[field] = expr;
          }
        }
        return newDoc as WithId<Document>;
      });
    }

    if ('$unset' in stage) {
      const fields = Array.isArray(stage.$unset) ? stage.$unset : [stage.$unset];
      return docs.map((doc) => {
        const newDoc = { ...doc } as Record<string, unknown>;
        for (const field of fields) {
          delete newDoc[field];
        }
        return newDoc as WithId<Document>;
      });
    }

    if ('$unwind' in stage) {
      return this.processUnwind(docs, stage.$unwind);
    }

    if ('$count' in stage) {
      // $count stage produces a document with _id: null and the count field.
      // MongoDB $count returns { _id: null, fieldName: count } which has null _id.
      // Double cast is required because WithId<Document> expects _id to be string|ObjectId,
      // but MongoDB's $count actually returns null for _id.
      const countResult = { _id: null, [stage.$count]: docs.length } as unknown as WithId<Document>;
      return [countResult];
    }

    // Unknown stage - pass through
    return docs;
  }

  /**
   * Simple filter matching (subset of MongoDB filter operators)
   */
  private matchesFilter(doc: WithId<Document>, filter: unknown): boolean {
    if (!filter || typeof filter !== 'object') return true;

    const filterObj = filter as Record<string, unknown>;
    for (const [key, value] of Object.entries(filterObj)) {
      const docValue = getNestedValue(doc as Record<string, unknown>, key);

      if (typeof value === 'object' && value !== null) {
        const ops = value as Record<string, unknown>;
        if ('$eq' in ops && docValue !== ops.$eq) return false;
        if ('$ne' in ops && docValue === ops.$ne) return false;
        if ('$gt' in ops) {
          const numDocValue = docValue as number;
          if (!(numDocValue !== null && numDocValue !== undefined && numDocValue > (ops.$gt as number))) return false;
        }
        if ('$gte' in ops) {
          const numDocValue = docValue as number;
          if (!(numDocValue !== null && numDocValue !== undefined && numDocValue >= (ops.$gte as number))) return false;
        }
        if ('$lt' in ops) {
          const numDocValue = docValue as number;
          if (!(numDocValue !== null && numDocValue !== undefined && numDocValue < (ops.$lt as number))) return false;
        }
        if ('$lte' in ops) {
          const numDocValue = docValue as number;
          if (!(numDocValue !== null && numDocValue !== undefined && numDocValue <= (ops.$lte as number))) return false;
        }
        if ('$in' in ops && !(ops.$in as unknown[]).includes(docValue)) return false;
      } else {
        if (docValue !== value) return false;
      }
    }

    return true;
  }

  /**
   * Apply projection to document
   */
  private applyProjection(
    doc: WithId<Document>,
    projection: { [key: string]: 0 | 1 | unknown }
  ): WithId<Document> {
    const result: Record<string, unknown> = {};
    const hasInclusions = Object.values(projection).some((v) => v === 1);

    if (hasInclusions) {
      // Include mode
      result._id = doc._id; // _id included by default
      for (const [key, value] of Object.entries(projection)) {
        if (value === 1) {
          result[key] = getNestedValue(doc as Record<string, unknown>, key);
        } else if (value === 0 && key === '_id') {
          delete result._id;
        }
      }
    } else {
      // Exclude mode
      Object.assign(result, doc);
      for (const [key, value] of Object.entries(projection)) {
        if (value === 0) {
          delete result[key];
        }
      }
    }

    return result as WithId<Document>;
  }

  /**
   * Sort documents
   */
  private sortDocuments(
    docs: WithId<Document>[],
    sortSpec: { [key: string]: 1 | -1 }
  ): WithId<Document>[] {
    return [...docs].sort((a, b) => {
      for (const [key, direction] of Object.entries(sortSpec)) {
        const aVal = getNestedValue(a as Record<string, unknown>, key);
        const bVal = getNestedValue(b as Record<string, unknown>, key);
        if (aVal != null && bVal != null) {
          if (aVal < bVal) return -direction;
          if (aVal > bVal) return direction;
        }
      }
      return 0;
    });
  }

  /**
   * Process $unwind stage
   */
  private processUnwind(
    docs: WithId<Document>[],
    unwindSpec: string | { path: string; preserveNullAndEmptyArrays?: boolean }
  ): WithId<Document>[] {
    let path: string;
    let preserveNullAndEmptyArrays = false;

    if (typeof unwindSpec === 'string') {
      path = unwindSpec.startsWith('$') ? unwindSpec.slice(1) : unwindSpec;
    } else {
      path = unwindSpec.path.startsWith('$') ? unwindSpec.path.slice(1) : unwindSpec.path;
      preserveNullAndEmptyArrays = unwindSpec.preserveNullAndEmptyArrays ?? false;
    }

    const result: WithId<Document>[] = [];

    for (const doc of docs) {
      const arrayValue = getNestedValue(doc as Record<string, unknown>, path);

      if (Array.isArray(arrayValue) && arrayValue.length > 0) {
        for (const item of arrayValue) {
          const newDoc = { ...doc } as Record<string, unknown>;
          this.setNestedValue(newDoc, path, item);
          result.push(newDoc as WithId<Document>);
        }
      } else if (preserveNullAndEmptyArrays) {
        const newDoc = { ...doc } as Record<string, unknown>;
        this.setNestedValue(newDoc, path, null);
        result.push(newDoc as WithId<Document>);
      }
    }

    return result;
  }

  /**
   * Set a nested value in an object
   */
  private setNestedValue(obj: Record<string, unknown>, path: string, value: unknown): void {
    const parts = path.split('.');
    let current = obj;

    for (let i = 0; i < parts.length - 1; i++) {
      const part = parts[i]!;
      if (!(part in current) || typeof current[part] !== 'object' || current[part] === null) {
        current[part] = {};
      }
      current = current[part] as Record<string, unknown>;
    }

    current[parts[parts.length - 1]!] = value;
  }
}

// ============================================================================
// Shard Executor Interface
// ============================================================================

/**
 * Interface for executing pipelines on a specific shard.
 * Implementations can use Durable Objects, HTTP calls, or in-memory execution.
 */
export interface ShardExecutor {
  /**
   * Execute a pipeline on a shard and return results
   */
  execute(
    shardId: string,
    pipeline: AggregationStage[]
  ): Promise<WithId<Document>[]>;

  /**
   * Execute partial aggregation on a shard and return partial aggregates
   */
  executePartialGroup(
    shardId: string,
    mapPipeline: AggregationStage[],
    groupSpec: GroupStage
  ): Promise<PartialAggregate[]>;
}

/**
 * Options for distributed aggregation execution
 */
export interface DistributedAggregatorOptions {
  /**
   * Custom shard executor. If not provided, uses in-memory executor.
   */
  executor?: ShardExecutor;

  /**
   * Enable $sort + $limit optimization.
   * When true, pushes partial sorting and limiting to shards.
   * @default true
   */
  sortLimitOptimization?: boolean;

  /**
   * Maximum documents to fetch from each shard when using sort+limit optimization.
   * This is the limit value multiplied by this factor.
   * @default 1 (exact limit pushed to shards)
   */
  shardLimitFactor?: number;
}

// ============================================================================
// DistributedAggregator
// ============================================================================

/**
 * High-level distributed aggregation engine that works across named shards.
 *
 * This class provides the main API for executing aggregation pipelines
 * across multiple shards, handling:
 * - Pipeline analysis and phase splitting
 * - $match pushdown to shards
 * - Partial $group aggregation with merge
 * - $sort + $limit optimization
 * - Result coordination and merging
 *
 * Example usage:
 * ```typescript
 * const aggregator = new DistributedAggregator();
 * const results = await aggregator.execute(
 *   [{ $match: { status: 'active' } }, { $group: { _id: '$region', count: { $sum: 1 } } }],
 *   ['shard-0', 'shard-1', 'shard-2']
 * );
 * ```
 */
export class DistributedAggregator {
  private readonly planner: DistributedAggregationPlanner;
  private readonly executor: ShardExecutor | null;
  private readonly sortLimitOptimization: boolean;
  private readonly shardLimitFactor: number;

  // In-memory shard data for testing (when no executor provided)
  private shardData: Map<string, WithId<Document>[]> = new Map();

  constructor(options: DistributedAggregatorOptions = {}) {
    this.planner = new DistributedAggregationPlanner();
    this.executor = options.executor ?? null;
    this.sortLimitOptimization = options.sortLimitOptimization ?? true;
    this.shardLimitFactor = options.shardLimitFactor ?? 1;
  }

  /**
   * Set shard data for in-memory testing (when no executor provided)
   */
  setShardData(shardId: string, docs: WithId<Document>[]): void {
    this.shardData.set(shardId, docs);
  }

  /**
   * Execute a distributed aggregation pipeline across the specified shards.
   *
   * The pipeline is analyzed and split into phases:
   * 1. Map phase: $match, $project, etc. pushed to each shard
   * 2. Partial aggregation: $group executed partially on each shard
   * 3. Merge phase: Partial results merged and finalized
   * 4. Reduce phase: Post-group stages like $sort, $limit applied
   *
   * @param pipeline - The aggregation pipeline to execute
   * @param shards - Array of shard identifiers to query
   * @returns Aggregated results
   */
  async execute(
    pipeline: AggregationStage[],
    shards: string[]
  ): Promise<WithId<Document>[]> {
    if (shards.length === 0) {
      return [];
    }

    // 1. Analyze pipeline
    const analysis = this.planner.analyzePipeline(pipeline);
    const split = this.planner.splitPipeline(pipeline);

    // 2. Detect $sort + $limit optimization opportunity
    const sortLimitOpt = this.detectSortLimitOptimization(pipeline, analysis);

    // 3. If no $group, handle simpler case
    if (!analysis.hasGroup || !split.groupStage) {
      return this.executeWithoutGroup(pipeline, shards, sortLimitOpt);
    }

    // 4. Execute partial aggregation on each shard in parallel
    const shardPartials = await Promise.all(
      shards.map((shardId) => this.executeShardPartialGroup(shardId, split.mapPhase, split.groupStage!))
    );

    // 5. Merge partial aggregates from all shards
    let results = this.planner.mergePartialAggregates(shardPartials, split.groupStage);

    // 6. Apply reduce phase stages (post-$group stages)
    if (split.reducePhase.length > 0) {
      results = this.applyReducePhase(results, split.reducePhase);
    }

    return results;
  }

  /**
   * Detect if $sort + $limit optimization can be applied
   */
  private detectSortLimitOptimization(
    pipeline: AggregationStage[],
    analysis: PipelineAnalysis
  ): { sortSpec?: { [key: string]: 1 | -1 }; limit?: number } | null {
    if (!this.sortLimitOptimization) {
      return null;
    }

    // Look for $sort followed by $limit pattern
    let sortSpec: { [key: string]: 1 | -1 } | undefined;
    let limit: number | undefined;
    let foundSort = false;

    for (let i = 0; i < pipeline.length; i++) {
      const stage = pipeline[i]!;

      if ('$sort' in stage) {
        // Check if this sort is in reduce phase (after $group if present)
        if (analysis.hasGroup && analysis.groupStageIndex !== undefined && i <= analysis.groupStageIndex) {
          continue; // Skip sorts before $group
        }
        sortSpec = stage.$sort;
        foundSort = true;
      } else if ('$limit' in stage && foundSort) {
        limit = stage.$limit;
        break;
      } else if (foundSort && !('$skip' in stage)) {
        // If we find a non-skip/limit stage after sort, optimization might not be safe
        break;
      }
    }

    if (sortSpec && limit) {
      return { sortSpec, limit };
    }

    return null;
  }

  /**
   * Execute pipeline without $group stage
   */
  private async executeWithoutGroup(
    pipeline: AggregationStage[],
    shards: string[],
    sortLimitOpt: { sortSpec?: { [key: string]: 1 | -1 }; limit?: number } | null
  ): Promise<WithId<Document>[]> {
    // Extract $sort and $limit stages for proper distributed handling
    let sortSpec: { [key: string]: 1 | -1 } | undefined;
    let limit: number | undefined;
    let skip: number | undefined;
    const sortIdx = pipeline.findIndex((s) => '$sort' in s);
    const limitIdx = pipeline.findIndex((s) => '$limit' in s);
    // Note: skipIdx calculated but not directly used - skip handling is done via iteration

    // Build shard pipeline - run stages up to (but not including) $sort/$limit/$skip on shards
    // These need to be applied after merging for correct distributed semantics
    let shardPipeline: AggregationStage[] = [];
    let postMergePipeline: AggregationStage[] = [];
    let foundBarrier = false;

    for (let i = 0; i < pipeline.length; i++) {
      const stage = pipeline[i]!;
      if ('$sort' in stage || '$limit' in stage || '$skip' in stage) {
        foundBarrier = true;
        if ('$sort' in stage) sortSpec = stage.$sort;
        if ('$limit' in stage) limit = stage.$limit;
        if ('$skip' in stage) skip = stage.$skip;
        postMergePipeline.push(stage);
      } else if (foundBarrier) {
        postMergePipeline.push(stage);
      } else {
        shardPipeline.push(stage);
      }
    }

    // If optimization is enabled and we have sort+limit, push partial sort+limit to shards
    if (sortLimitOpt?.sortSpec && sortLimitOpt?.limit && sortIdx >= 0 && limitIdx > sortIdx) {
      // Add sort and a larger limit to shard pipeline for early filtering
      const earlyLimit = sortLimitOpt.limit * this.shardLimitFactor;
      if (sortSpec) {
        shardPipeline.push({ $sort: sortSpec });
      }
      shardPipeline.push({ $limit: earlyLimit } as AggregationStage);
    }

    // Execute on all shards in parallel
    const shardResults = await Promise.all(
      shards.map((shardId) => this.executeShardPipeline(shardId, shardPipeline))
    );

    // Merge results
    let merged: WithId<Document>[] = [];
    for (const results of shardResults) {
      merged = merged.concat(results);
    }

    // Apply post-merge stages ($sort, $skip, $limit, and any stages after them)
    if (sortSpec) {
      merged = this.sortDocuments(merged, sortSpec);
    }
    if (skip !== undefined) {
      merged = merged.slice(skip);
    }
    if (limit !== undefined) {
      merged = merged.slice(0, limit);
    }

    // Apply any remaining stages after sort/skip/limit
    for (const stage of postMergePipeline) {
      if ('$sort' in stage || '$limit' in stage || '$skip' in stage) {
        continue; // Already handled above
      }
      if ('$match' in stage) {
        merged = merged.filter((doc) => this.matchesFilter(doc, stage.$match));
      } else if ('$project' in stage) {
        merged = merged.map((doc) => this.applyProjection(doc, stage.$project));
      } else if ('$addFields' in stage || '$set' in stage) {
        const fields = '$addFields' in stage ? stage.$addFields : ('$set' in stage ? stage.$set : {});
        merged = merged.map((doc) => {
          const newDoc = { ...doc } as Record<string, unknown>;
          for (const [field, expr] of Object.entries(fields)) {
            if (typeof expr === 'string' && expr.startsWith('$')) {
              newDoc[field] = getNestedValue(doc as Record<string, unknown>, expr.slice(1));
            } else {
              newDoc[field] = expr;
            }
          }
          return newDoc as WithId<Document>;
        });
      } else if ('$unset' in stage) {
        const fieldsToRemove = Array.isArray(stage.$unset) ? stage.$unset : [stage.$unset];
        merged = merged.map((doc) => {
          const newDoc = { ...doc } as Record<string, unknown>;
          for (const field of fieldsToRemove) {
            delete newDoc[field];
          }
          return newDoc as WithId<Document>;
        });
      }
    }

    return merged;
  }

  /**
   * Execute a pipeline on a specific shard
   */
  private async executeShardPipeline(
    shardId: string,
    pipeline: AggregationStage[]
  ): Promise<WithId<Document>[]> {
    if (this.executor) {
      return this.executor.execute(shardId, pipeline);
    }

    // In-memory execution for testing
    const docs = this.shardData.get(shardId) ?? [];
    return this.executeLocalPipeline(docs, pipeline);
  }

  /**
   * Execute partial group aggregation on a shard
   */
  private async executeShardPartialGroup(
    shardId: string,
    mapPipeline: AggregationStage[],
    groupSpec: GroupStage
  ): Promise<PartialAggregate[]> {
    if (this.executor) {
      return this.executor.executePartialGroup(shardId, mapPipeline, groupSpec);
    }

    // In-memory execution for testing
    const docs = this.shardData.get(shardId) ?? [];
    const mapResult = await this.executeLocalPipeline(docs, mapPipeline);
    return this.planner.executePartialGroup(mapResult, groupSpec);
  }

  /**
   * Apply reduce phase stages to merged results
   */
  private applyReducePhase(
    docs: WithId<Document>[],
    reducePhase: AggregationStage[]
  ): WithId<Document>[] {
    let result = docs;

    for (const stage of reducePhase) {
      if ('$sort' in stage) {
        result = this.sortDocuments(result, stage.$sort);
      } else if ('$limit' in stage) {
        result = result.slice(0, stage.$limit);
      } else if ('$skip' in stage) {
        result = result.slice(stage.$skip);
      } else if ('$project' in stage) {
        result = result.map((doc) => this.applyProjection(doc, stage.$project));
      } else if ('$addFields' in stage || '$set' in stage) {
        const fields = '$addFields' in stage ? stage.$addFields : ('$set' in stage ? stage.$set : {});
        result = result.map((doc) => {
          const newDoc = { ...doc } as Record<string, unknown>;
          for (const [field, expr] of Object.entries(fields)) {
            if (typeof expr === 'string' && expr.startsWith('$')) {
              newDoc[field] = getNestedValue(doc as Record<string, unknown>, expr.slice(1));
            } else {
              newDoc[field] = expr;
            }
          }
          return newDoc as WithId<Document>;
        });
      } else if ('$unset' in stage) {
        const fieldsToRemove = Array.isArray(stage.$unset) ? stage.$unset : [stage.$unset];
        result = result.map((doc) => {
          const newDoc = { ...doc } as Record<string, unknown>;
          for (const field of fieldsToRemove) {
            delete newDoc[field];
          }
          return newDoc as WithId<Document>;
        });
      } else if ('$match' in stage) {
        result = result.filter((doc) => this.matchesFilter(doc, stage.$match));
      }
    }

    return result;
  }

  /**
   * Execute pipeline stages locally
   */
  private async executeLocalPipeline(
    docs: WithId<Document>[],
    pipeline: AggregationStage[]
  ): Promise<WithId<Document>[]> {
    let result = docs;

    for (const stage of pipeline) {
      if ('$match' in stage) {
        result = result.filter((doc) => this.matchesFilter(doc, stage.$match));
      } else if ('$sort' in stage) {
        result = this.sortDocuments(result, stage.$sort);
      } else if ('$limit' in stage) {
        result = result.slice(0, stage.$limit);
      } else if ('$skip' in stage) {
        result = result.slice(stage.$skip);
      } else if ('$project' in stage) {
        result = result.map((doc) => this.applyProjection(doc, stage.$project));
      } else if ('$addFields' in stage || '$set' in stage) {
        const fields = '$addFields' in stage ? stage.$addFields : ('$set' in stage ? stage.$set : {});
        result = result.map((doc) => {
          const newDoc = { ...doc } as Record<string, unknown>;
          for (const [field, expr] of Object.entries(fields)) {
            if (typeof expr === 'string' && expr.startsWith('$')) {
              newDoc[field] = getNestedValue(doc as Record<string, unknown>, expr.slice(1));
            } else {
              newDoc[field] = expr;
            }
          }
          return newDoc as WithId<Document>;
        });
      } else if ('$unset' in stage) {
        const fieldsToRemove = Array.isArray(stage.$unset) ? stage.$unset : [stage.$unset];
        result = result.map((doc) => {
          const newDoc = { ...doc } as Record<string, unknown>;
          for (const field of fieldsToRemove) {
            delete newDoc[field];
          }
          return newDoc as WithId<Document>;
        });
      }
    }

    return result;
  }

  /**
   * Sort documents
   */
  private sortDocuments(
    docs: WithId<Document>[],
    sortSpec: { [key: string]: 1 | -1 }
  ): WithId<Document>[] {
    return [...docs].sort((a, b) => {
      for (const [key, direction] of Object.entries(sortSpec)) {
        const aVal = getNestedValue(a as Record<string, unknown>, key);
        const bVal = getNestedValue(b as Record<string, unknown>, key);
        if (aVal != null && bVal != null) {
          if (aVal < bVal) return -direction;
          if (aVal > bVal) return direction;
        }
      }
      return 0;
    });
  }

  /**
   * Simple filter matching
   */
  private matchesFilter(doc: WithId<Document>, filter: unknown): boolean {
    if (!filter || typeof filter !== 'object') return true;

    const filterObj = filter as Record<string, unknown>;
    for (const [key, value] of Object.entries(filterObj)) {
      const docValue = getNestedValue(doc as Record<string, unknown>, key);

      if (typeof value === 'object' && value !== null) {
        const ops = value as Record<string, unknown>;
        if ('$eq' in ops && docValue !== ops.$eq) return false;
        if ('$ne' in ops && docValue === ops.$ne) return false;
        if ('$gt' in ops && !(docValue !== null && docValue !== undefined && docValue > (ops.$gt as number | string))) return false;
        if ('$gte' in ops && !(docValue !== null && docValue !== undefined && docValue >= (ops.$gte as number | string))) return false;
        if ('$lt' in ops && !(docValue !== null && docValue !== undefined && docValue < (ops.$lt as number | string))) return false;
        if ('$lte' in ops && !(docValue !== null && docValue !== undefined && docValue <= (ops.$lte as number | string))) return false;
        if ('$in' in ops && !(ops.$in as unknown[]).includes(docValue)) return false;
      } else {
        if (docValue !== value) return false;
      }
    }

    return true;
  }

  /**
   * Apply projection to document
   */
  private applyProjection(
    doc: WithId<Document>,
    projection: { [key: string]: 0 | 1 | unknown }
  ): WithId<Document> {
    const result: Record<string, unknown> = {};
    const hasInclusions = Object.values(projection).some((v) => v === 1);

    if (hasInclusions) {
      result._id = doc._id;
      for (const [key, value] of Object.entries(projection)) {
        if (value === 1) {
          result[key] = getNestedValue(doc as Record<string, unknown>, key);
        } else if (value === 0 && key === '_id') {
          delete result._id;
        }
      }
    } else {
      Object.assign(result, doc);
      for (const [key, value] of Object.entries(projection)) {
        if (value === 0) {
          delete result[key];
        }
      }
    }

    return result as WithId<Document>;
  }
}

// ============================================================================
// Factory Functions
// ============================================================================

/**
 * Create a new DistributedAggregationPlanner
 */
export function createDistributedAggregationPlanner(): DistributedAggregationPlanner {
  return new DistributedAggregationPlanner();
}

/**
 * Create a new DistributedAggregationExecutor
 */
export function createDistributedAggregationExecutor(): DistributedAggregationExecutor {
  return new DistributedAggregationExecutor();
}

/**
 * Create a new DistributedAggregator
 */
export function createDistributedAggregator(options?: DistributedAggregatorOptions): DistributedAggregator {
  return new DistributedAggregator(options);
}
