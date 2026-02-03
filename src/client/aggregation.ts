/**
 * MongoLake Aggregation Cursors
 *
 * Aggregation pipeline execution cursors for Collection and TimeTravelCollection.
 * Uses a shared BaseAggregationCursor to eliminate code duplication.
 *
 * Supports two execution modes:
 * 1. Standard mode: Loads all documents, then processes pipeline stages
 * 2. Distributed mode: Uses DistributedAggregationPlanner for optimized execution
 *    with partial aggregation and cross-shard merging
 */

import type {
  Document,
  WithId,
  Filter,
  AggregationStage,
  AggregateOptions,
  BucketAutoStage,
  GraphLookupStage,
  RedactExpression,
  ReplaceRootStage,
  SampleStage,
} from '@types';
import { logger } from '../utils/logger.js';
import {
  processBucketAuto,
  processGraphLookup,
  processRedact,
  processReplaceRoot,
  processSample,
  processSortByCount,
} from '@mongolake/query/aggregation-stages.js';
import { matchesFilter } from '@utils/filter.js';
import { applyProjection } from '@utils/projection.js';
import { getNestedValue } from '@utils/nested.js';
import type { Collection } from './collection.js';
import type { TimeTravelCollection } from './time-travel.js';
import {
  DistributedAggregationPlanner,
  type PipelineAnalysis,
} from './distributed-aggregation.js';

// ============================================================================
// Collection Source Interface
// ============================================================================

/**
 * Interface for collection sources that can provide documents for aggregation.
 * Both Collection and TimeTravelCollection implement this interface.
 */
export interface AggregationSource<T extends Document = Document> {
  readDocuments(): Promise<WithId<T>[]>;
  getSiblingCollection<U extends Document = Document>(name: string): AggregationSource<U>;
}

/**
 * Result of pipeline analysis for distributed execution.
 * Exported for use in tests and external integrations.
 */
export type { PipelineAnalysis } from './distributed-aggregation.js';

// ============================================================================
// BaseAggregationCursor (Abstract)
// ============================================================================

/**
 * Abstract base class for aggregation cursors.
 * Contains shared logic for processing aggregation pipeline stages.
 *
 * Supports two execution modes:
 * 1. Standard mode (default): Loads all documents, then processes pipeline
 * 2. Distributed mode (options.distributed=true): Uses DistributedAggregationPlanner
 *    for optimized execution with partial aggregation and merging
 */
export abstract class BaseAggregationCursor<T extends Document = Document> {
  protected readonly options: AggregateOptions;
  private readonly planner: DistributedAggregationPlanner;
  private pipelineAnalysis: PipelineAnalysis | null = null;

  constructor(
    protected pipeline: AggregationStage[],
    options?: AggregateOptions
  ) {
    this.options = options ?? {};
    this.planner = new DistributedAggregationPlanner();
  }

  /**
   * Analyze the pipeline for distributed execution.
   * Results are cached for reuse.
   */
  protected analyzePipeline(): PipelineAnalysis {
    if (!this.pipelineAnalysis) {
      this.pipelineAnalysis = this.planner.analyzePipeline(this.pipeline);
    }
    return this.pipelineAnalysis;
  }

  /**
   * Check if this pipeline can benefit from distributed execution.
   * A pipeline benefits from distribution if:
   * - It contains a $group stage (for partial aggregation)
   * - OR it has $sort + $limit that can be optimized
   * - AND it doesn't contain blocking stages like $lookup
   */
  protected canUseDistributedExecution(): boolean {
    const analysis = this.analyzePipeline();
    return analysis.canDistribute && (
      analysis.hasGroup ||
      this.hasSortLimitOptimization()
    );
  }

  /**
   * Check if the pipeline has $sort followed by $limit
   */
  private hasSortLimitOptimization(): boolean {
    let foundSort = false;
    for (const stage of this.pipeline) {
      if ('$sort' in stage) {
        foundSort = true;
      } else if ('$limit' in stage && foundSort) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get the collection source for reading documents.
   * Implemented by concrete classes.
   */
  protected abstract getSource(): AggregationSource<Document>;

  /**
   * Helper to extract field value from document using $ notation
   */
  protected getFieldValue(doc: Record<string, unknown>, expr: unknown): unknown {
    if (typeof expr === 'string' && expr.startsWith('$')) {
      return getNestedValue(doc, expr.slice(1));
    }
    return expr;
  }

  /**
   * Evaluate _id expression for $group stage
   */
  protected evaluateGroupId(doc: Record<string, unknown>, idExpr: unknown): string {
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
        idObj[key] = this.getFieldValue(doc, value);
      }
      return JSON.stringify(idObj);
    }

    return String(idExpr);
  }

  /**
   * Parse compound _id back to object
   */
  protected parseGroupId(groupId: string, idExpr: unknown): unknown {
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

  /**
   * Execute and return all results.
   *
   * Execution strategy:
   * 1. If distributed mode is enabled AND pipeline can benefit from it,
   *    use distributed execution with partial aggregation
   * 2. Otherwise, use standard execution (load all docs, process sequentially)
   */
  async toArray(): Promise<T[]> {
    // Check if we should use distributed execution
    if (this.options.distributed && this.canUseDistributedExecution()) {
      return this.executeDistributed();
    }

    // Standard execution: load all documents, then process pipeline
    return this.executeStandard();
  }

  /**
   * Standard execution mode: load all documents, then process pipeline stages.
   * This is the original behavior, maintained for backwards compatibility.
   */
  protected async executeStandard(): Promise<T[]> {
    // Get all documents first
    let docs = await this.getSource().readDocuments();

    // Process pipeline stages
    for (const stage of this.pipeline) {
      docs = await this.processStage(docs, stage);
    }

    return docs as T[];
  }

  /**
   * Distributed execution mode: use DistributedAggregationPlanner for
   * optimized execution with partial aggregation and merging.
   *
   * This mode:
   * 1. Splits the pipeline into map and reduce phases
   * 2. Executes map phase to get documents
   * 3. For $group pipelines, executes partial aggregation and merges
   * 4. Applies reduce phase stages to merged results
   */
  protected async executeDistributed(): Promise<T[]> {
    const split = this.planner.splitPipeline(this.pipeline);

    // Get all documents (in a real distributed system, this would be parallelized across shards)
    let docs = await this.getSource().readDocuments();

    // Execute map phase stages (before $group)
    for (const stage of split.mapPhase) {
      docs = await this.processStage(docs, stage);
    }

    // If there's a $group stage, use partial aggregation
    if (split.groupStage) {
      // Execute partial group - in single-node mode, this is equivalent to full group
      // but the code path exercises the distributed aggregation logic
      const partials = this.planner.executePartialGroup(docs, split.groupStage);

      // Merge partial aggregates (single shard, so just one set of partials)
      docs = this.planner.mergePartialAggregates([partials], split.groupStage);
    }

    // Execute reduce phase stages (after $group)
    for (const stage of split.reducePhase) {
      docs = await this.processStage(docs, stage);
    }

    return docs as T[];
  }

  /**
   * Process a single aggregation pipeline stage
   */
  protected async processStage(docs: WithId<Document>[], stage: AggregationStage): Promise<WithId<Document>[]> {
    if ('$match' in stage) {
      return this.processMatch(docs, stage.$match);
    }

    if ('$sort' in stage) {
      return this.processSort(docs, stage.$sort);
    }

    if ('$limit' in stage) {
      return docs.slice(0, stage.$limit);
    }

    if ('$skip' in stage) {
      return docs.slice(stage.$skip);
    }

    if ('$project' in stage) {
      return docs.map((doc) => applyProjection(doc, stage.$project as { [key: string]: 0 | 1 }) as WithId<Document>);
    }

    if ('$group' in stage) {
      return this.processGroup(docs, stage.$group);
    }

    if ('$unwind' in stage) {
      return this.processUnwind(docs, stage.$unwind);
    }

    if ('$lookup' in stage) {
      return await this.processLookup(docs, stage.$lookup);
    }

    if ('$count' in stage) {
      // $count stage produces a document with _id: null and the count field.
      // MongoDB $count returns { _id: null, fieldName: count } which has null _id.
      // Double cast is required because WithId<Document> expects _id to be string|ObjectId,
      // but MongoDB's $count actually returns null for _id.
      const countResult = { _id: null, [stage.$count]: docs.length } as unknown as WithId<Document>;
      return [countResult];
    }

    if ('$addFields' in stage || '$set' in stage) {
      const fieldsToAdd = '$addFields' in stage ? stage.$addFields : ('$set' in stage ? stage.$set : {});
      return this.processAddFields(docs, fieldsToAdd);
    }

    if ('$unset' in stage) {
      return this.processUnset(docs, stage.$unset);
    }

    if ('$facet' in stage) {
      return await this.processFacet(docs, stage.$facet);
    }

    if ('$bucket' in stage) {
      return this.processBucket(docs, stage.$bucket);
    }

    if ('$bucketAuto' in stage) {
      return processBucketAuto(docs, stage.$bucketAuto as BucketAutoStage);
    }

    if ('$graphLookup' in stage) {
      return await this.processGraphLookup(docs, stage.$graphLookup as GraphLookupStage);
    }

    if ('$redact' in stage) {
      return processRedact(docs, stage.$redact as RedactExpression);
    }

    if ('$replaceRoot' in stage) {
      return processReplaceRoot(docs, stage.$replaceRoot as ReplaceRootStage);
    }

    if ('$replaceWith' in stage) {
      return processReplaceRoot(docs, stage.$replaceWith as string | ReplaceRootStage);
    }

    if ('$sample' in stage) {
      return processSample(docs, stage.$sample as SampleStage);
    }

    if ('$sortByCount' in stage) {
      return processSortByCount(docs, stage.$sortByCount);
    }

    if ('$merge' in stage) {
      // $merge is a terminal stage that writes to a collection
      // For now, we just return the documents - actual merge would be handled by the caller
      logger.warn('$merge stage is not fully implemented - documents returned without merging', {
        operation: 'aggregation',
        stage: '$merge',
        documentCount: docs.length,
      });
      return docs;
    }

    if ('$out' in stage) {
      // $out is a terminal stage that writes to a collection
      // For now, we just return the documents - actual output would be handled by the caller
      logger.warn('$out stage is not fully implemented - documents returned without output', {
        operation: 'aggregation',
        stage: '$out',
        documentCount: docs.length,
      });
      return docs;
    }

    return docs;
  }

  /**
   * Process $match stage
   */
  protected processMatch(docs: WithId<Document>[], matchFilter: unknown): WithId<Document>[] {
    return docs.filter((doc) => matchesFilter(doc, matchFilter as Filter<WithId<Document>>));
  }

  /**
   * Process $sort stage
   */
  protected processSort(docs: WithId<Document>[], sortSpec: { [key: string]: 1 | -1 }): WithId<Document>[] {
    return [...docs].sort((a, b) => {
      for (const [key, direction] of Object.entries(sortSpec)) {
        const aVal = getNestedValue(a, key) as string | number | boolean | null | undefined;
        const bVal = getNestedValue(b, key) as string | number | boolean | null | undefined;
        if (aVal != null && bVal != null && aVal < bVal) return -direction;
        if (aVal != null && bVal != null && aVal > bVal) return direction;
      }
      return 0;
    });
  }

  /**
   * Process $group stage
   */
  protected processGroup(docs: WithId<Document>[], groupSpec: { _id: unknown; [key: string]: unknown }): WithId<Document>[] {
    const groups = new Map<string, Record<string, unknown>[]>();

    for (const doc of docs) {
      const groupKey = this.evaluateGroupId(doc as Record<string, unknown>, groupSpec._id);

      if (!groups.has(groupKey)) {
        groups.set(groupKey, []);
      }
      const group = groups.get(groupKey);
      if (group) {
        group.push(doc as Record<string, unknown>);
      }
    }

    const result: WithId<Document>[] = [];
    for (const [groupId, groupDocs] of groups) {
      const groupResult: Record<string, unknown> = {
        _id: this.parseGroupId(groupId, groupSpec._id),
      };

      for (const [field, expr] of Object.entries(groupSpec)) {
        if (field === '_id') continue;
        groupResult[field] = this.evaluateAccumulator(groupDocs, expr as Record<string, unknown>);
      }

      result.push(groupResult as WithId<Document>);
    }

    return result;
  }

  /**
   * Evaluate a group accumulator expression
   */
  protected evaluateAccumulator(groupDocs: Record<string, unknown>[], accExpr: Record<string, unknown>): unknown {
    if ('$sum' in accExpr) {
      if (typeof accExpr.$sum === 'number') {
        return groupDocs.length * accExpr.$sum;
      }
      const sumField = String(accExpr.$sum).replace('$', '');
      return groupDocs.reduce((sum, d) => sum + (Number(getNestedValue(d, sumField)) || 0), 0);
    }

    if ('$avg' in accExpr) {
      const avgField = String(accExpr.$avg).replace('$', '');
      const values = groupDocs.map((d) => Number(getNestedValue(d, avgField)) || 0);
      return values.length > 0 ? values.reduce((a, b) => a + b, 0) / values.length : 0;
    }

    if ('$min' in accExpr) {
      const minField = String(accExpr.$min).replace('$', '');
      const values = groupDocs.map((d) => getNestedValue(d, minField)).filter((v) => v !== undefined && v !== null);
      return values.length > 0 ? values.reduce((min, v) => (v < min ? v : min)) : null;
    }

    if ('$max' in accExpr) {
      const maxField = String(accExpr.$max).replace('$', '');
      const values = groupDocs.map((d) => getNestedValue(d, maxField)).filter((v) => v !== undefined && v !== null);
      return values.length > 0 ? values.reduce((max, v) => (v > max ? v : max)) : null;
    }

    if ('$first' in accExpr) {
      const firstField = String(accExpr.$first).replace('$', '');
      return groupDocs.length > 0 ? getNestedValue(groupDocs[0], firstField) : null;
    }

    if ('$last' in accExpr) {
      const lastField = String(accExpr.$last).replace('$', '');
      return groupDocs.length > 0 ? getNestedValue(groupDocs[groupDocs.length - 1], lastField) : null;
    }

    if ('$push' in accExpr) {
      const pushField = String(accExpr.$push).replace('$', '');
      return groupDocs.map((d) => getNestedValue(d, pushField));
    }

    if ('$addToSet' in accExpr) {
      const addToSetField = String(accExpr.$addToSet).replace('$', '');
      const values = groupDocs.map((d) => getNestedValue(d, addToSetField));
      const seen = new Set<string>();
      const uniqueValues: unknown[] = [];
      for (const v of values) {
        const key = JSON.stringify(v);
        if (!seen.has(key)) {
          seen.add(key);
          uniqueValues.push(v);
        }
      }
      return uniqueValues;
    }

    if ('$count' in accExpr) {
      return groupDocs.length;
    }

    return null;
  }

  /**
   * Process $unwind stage
   */
  protected processUnwind(docs: WithId<Document>[], unwindSpec: string | { path: string; preserveNullAndEmptyArrays?: boolean }): WithId<Document>[] {
    let path: string;
    let preserveNullAndEmptyArrays = false;

    if (typeof unwindSpec === 'string') {
      path = unwindSpec.startsWith('$') ? unwindSpec.slice(1) : unwindSpec;
    } else {
      path = unwindSpec.path.startsWith('$') ? unwindSpec.path.slice(1) : unwindSpec.path;
      preserveNullAndEmptyArrays = unwindSpec.preserveNullAndEmptyArrays ?? false;
    }

    const unwoundDocs: WithId<Document>[] = [];

    for (const doc of docs) {
      const arrayValue = getNestedValue(doc as Record<string, unknown>, path);

      if (Array.isArray(arrayValue) && arrayValue.length > 0) {
        for (const item of arrayValue) {
          const newDoc = this.deepClone(doc) as Record<string, unknown>;
          this.setNestedValue(newDoc, path, item);
          unwoundDocs.push(newDoc as WithId<Document>);
        }
      } else if (preserveNullAndEmptyArrays) {
        const newDoc = this.deepClone(doc) as Record<string, unknown>;
        if (arrayValue === undefined || (Array.isArray(arrayValue) && arrayValue.length === 0)) {
          this.setNestedValue(newDoc, path, null);
        }
        unwoundDocs.push(newDoc as WithId<Document>);
      }
    }

    return unwoundDocs;
  }

  /**
   * Process $lookup stage
   */
  protected async processLookup(
    docs: WithId<Document>[],
    lookupSpec: { from: string; localField?: string; foreignField?: string; as: string; let?: Record<string, unknown>; pipeline?: AggregationStage[] }
  ): Promise<WithId<Document>[]> {
    const { from, localField, foreignField, as } = lookupSpec;

    const foreignCollection = this.getSource().getSiblingCollection<Document>(from);
    const foreignDocs = await foreignCollection.readDocuments();

    if (lookupSpec.pipeline) {
      return this.processLookupWithPipeline(docs, foreignDocs, lookupSpec.let || {}, lookupSpec.pipeline, as);
    } else if (localField && foreignField) {
      return this.processLookupEquality(docs, foreignDocs, localField, foreignField, as);
    } else {
      return docs.map((doc) => {
        (doc as Record<string, unknown>)[as] = [];
        return doc;
      });
    }
  }

  /**
   * Process $lookup with pipeline
   */
  protected processLookupWithPipeline(
    docs: WithId<Document>[],
    foreignDocs: WithId<Document>[],
    letVars: Record<string, unknown>,
    pipeline: AggregationStage[],
    as: string
  ): WithId<Document>[] {
    for (let i = 0; i < docs.length; i++) {
      const doc = docs[i] as Record<string, unknown>;

      const varContext: Record<string, unknown> = {};
      for (const [varName, varExpr] of Object.entries(letVars)) {
        varContext[varName] = this.getFieldValue(doc, varExpr);
      }

      let pipelineDocs = [...foreignDocs] as Document[];

      for (const pipelineStage of pipeline) {
        if ('$match' in pipelineStage) {
          const resolvedMatch = this.resolveVariables(pipelineStage.$match, varContext);
          pipelineDocs = pipelineDocs.filter((d) => matchesFilter(d as WithId<Document>, resolvedMatch as Filter<WithId<Document>>));
        }
      }

      (doc as Record<string, unknown>)[as] = pipelineDocs;
      docs[i] = doc as WithId<Document>;
    }

    return docs;
  }

  /**
   * Process $lookup with equality match
   */
  protected processLookupEquality(
    docs: WithId<Document>[],
    foreignDocs: WithId<Document>[],
    localField: string,
    foreignField: string,
    as: string
  ): WithId<Document>[] {
    for (let i = 0; i < docs.length; i++) {
      const doc = docs[i] as Record<string, unknown>;
      const localValue = getNestedValue(doc, localField);

      const matchingDocs = foreignDocs.filter((foreignDoc) => {
        const foreignValue = getNestedValue(foreignDoc as Record<string, unknown>, foreignField);
        return localValue === foreignValue;
      });

      (doc as Record<string, unknown>)[as] = matchingDocs;
      docs[i] = doc as WithId<Document>;
    }

    return docs;
  }

  /**
   * Process $addFields or $set stage
   */
  protected processAddFields(docs: WithId<Document>[], fieldsToAdd: Record<string, unknown>): WithId<Document>[] {
    return docs.map((doc) => {
      const newDoc = { ...doc } as Record<string, unknown>;
      for (const [field, expr] of Object.entries(fieldsToAdd)) {
        newDoc[field] = this.getFieldValue(newDoc, expr);
      }
      return newDoc as WithId<Document>;
    });
  }

  /**
   * Process $unset stage
   */
  protected processUnset(docs: WithId<Document>[], unsetSpec: string | string[]): WithId<Document>[] {
    const fieldsToRemove = Array.isArray(unsetSpec) ? unsetSpec : [unsetSpec];
    return docs.map((doc) => {
      const newDoc = { ...doc } as Record<string, unknown>;
      for (const field of fieldsToRemove) {
        delete newDoc[field];
      }
      return newDoc as WithId<Document>;
    });
  }

  /**
   * Process $facet stage - run multiple pipelines in parallel
   */
  protected async processFacet(
    docs: WithId<Document>[],
    facetSpec: Record<string, AggregationStage[]>
  ): Promise<WithId<Document>[]> {
    const result: Record<string, unknown> = { _id: null };

    for (const [outputField, subPipeline] of Object.entries(facetSpec)) {
      // Process each sub-pipeline independently with a copy of the input docs
      let facetDocs = [...docs];
      for (const stage of subPipeline) {
        facetDocs = await this.processStage(facetDocs, stage);
      }
      result[outputField] = facetDocs;
    }

    return [result as WithId<Document>];
  }

  /**
   * Process $bucket stage - group documents into buckets based on boundaries
   */
  protected processBucket(
    docs: WithId<Document>[],
    bucketSpec: { groupBy: string; boundaries: (number | Date)[]; default?: string; output?: Record<string, unknown> }
  ): WithId<Document>[] {
    const { groupBy, boundaries, default: defaultBucket, output } = bucketSpec;
    const buckets = new Map<string | number | Date, Record<string, unknown>[]>();

    // Initialize buckets for each boundary range
    for (let i = 0; i < boundaries.length - 1; i++) {
      buckets.set(boundaries[i]!, []);
    }
    if (defaultBucket) {
      buckets.set(defaultBucket, []);
    }

    // Distribute documents into buckets
    for (const doc of docs) {
      const fieldPath = groupBy.startsWith('$') ? groupBy.slice(1) : groupBy;
      const value = getNestedValue(doc as Record<string, unknown>, fieldPath) as number | Date | null | undefined;

      let placed = false;
      if (value !== null && value !== undefined) {
        for (let i = 0; i < boundaries.length - 1; i++) {
          const lower = boundaries[i];
          const upper = boundaries[i + 1];
          if (lower !== undefined && upper !== undefined && value >= lower && value < upper) {
            const bucket = buckets.get(lower);
            if (bucket) {
              bucket.push(doc as Record<string, unknown>);
            }
            placed = true;
            break;
          }
        }
      }

      if (!placed && defaultBucket) {
        const defaultBucketArray = buckets.get(defaultBucket);
        if (defaultBucketArray) {
          defaultBucketArray.push(doc as Record<string, unknown>);
        }
      }
    }

    // Build result documents
    const result: WithId<Document>[] = [];
    for (const [bucketId, bucketDocs] of buckets) {
      if (bucketDocs.length === 0 && bucketId !== defaultBucket) {
        continue; // Skip empty boundary buckets
      }

      const bucketResult: Record<string, unknown> = { _id: bucketId };

      if (output) {
        for (const [field, expr] of Object.entries(output)) {
          bucketResult[field] = this.evaluateAccumulator(bucketDocs, expr as Record<string, unknown>);
        }
      } else {
        // Default output is count
        bucketResult.count = bucketDocs.length;
      }

      if (bucketDocs.length > 0 || bucketId === defaultBucket) {
        result.push(bucketResult as WithId<Document>);
      }
    }

    return result;
  }

  /**
   * Process $graphLookup stage - recursive graph traversal
   */
  protected async processGraphLookup(
    docs: WithId<Document>[],
    spec: GraphLookupStage
  ): Promise<WithId<Document>[]> {
    const getCollection = async (name: string): Promise<WithId<Document>[]> => {
      const foreignCollection = this.getSource().getSiblingCollection<Document>(name);
      return foreignCollection.readDocuments();
    };

    return processGraphLookup(docs, spec, getCollection);
  }

  /**
   * Deep clone an object
   */
  protected deepClone<U>(obj: U): U {
    if (obj === null || typeof obj !== 'object') {
      return obj;
    }

    if (Array.isArray(obj)) {
      return obj.map((item) => this.deepClone(item)) as U;
    }

    if (obj instanceof Date) {
      return new Date(obj.getTime()) as U;
    }

    if (obj instanceof Uint8Array) {
      return new Uint8Array(obj) as U;
    }

    const cloned: Record<string, unknown> = {};
    for (const key in obj) {
      if (Object.prototype.hasOwnProperty.call(obj, key)) {
        cloned[key] = this.deepClone((obj as Record<string, unknown>)[key]);
      }
    }
    return cloned as U;
  }

  /**
   * Set a nested value in an object using dot notation
   */
  protected setNestedValue(obj: Record<string, unknown>, path: string, value: unknown): void {
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

  /**
   * Resolve $$variables in an expression
   */
  protected resolveVariables(expr: unknown, varContext: Record<string, unknown>): unknown {
    if (typeof expr === 'string' && expr.startsWith('$$')) {
      const varName = expr.slice(2);
      return varContext[varName];
    }

    if (typeof expr === 'object' && expr !== null) {
      if (Array.isArray(expr)) {
        return expr.map((item) => this.resolveVariables(item, varContext));
      }

      const resolved: Record<string, unknown> = {};
      for (const [key, value] of Object.entries(expr)) {
        if (key === '$expr') {
          resolved[key] = this.resolveExprVariables(value, varContext);
        } else {
          resolved[key] = this.resolveVariables(value, varContext);
        }
      }
      return resolved;
    }

    return expr;
  }

  /**
   * Resolve $$variables in $expr expressions
   */
  protected resolveExprVariables(expr: unknown, varContext: Record<string, unknown>): unknown {
    if (typeof expr === 'string' && expr.startsWith('$$')) {
      const varName = expr.slice(2);
      return varContext[varName];
    }

    if (typeof expr === 'object' && expr !== null) {
      if (Array.isArray(expr)) {
        return expr.map((item) => this.resolveExprVariables(item, varContext));
      }

      const resolved: Record<string, unknown> = {};
      for (const [key, value] of Object.entries(expr)) {
        resolved[key] = this.resolveExprVariables(value, varContext);
      }
      return resolved;
    }

    return expr;
  }

  /**
   * Execute and iterate
   */
  async *[Symbol.asyncIterator](): AsyncIterableIterator<T> {
    const results = await this.toArray();
    for (const doc of results) {
      yield doc;
    }
  }
}

// ============================================================================
// AggregationCursor
// ============================================================================

export class AggregationCursor<T extends Document = Document> extends BaseAggregationCursor<T> {
  constructor(
    private collection: Collection<Document>,
    pipeline: AggregationStage[],
    options?: AggregateOptions
  ) {
    super(pipeline, options);
  }

  protected getSource(): AggregationSource<Document> {
    return this.collection;
  }
}

// ============================================================================
// TimeTravelAggregationCursor
// ============================================================================

export class TimeTravelAggregationCursor<T extends Document = Document> extends BaseAggregationCursor<T> {
  constructor(
    private collection: TimeTravelCollection<Document>,
    pipeline: AggregationStage[],
    options?: AggregateOptions
  ) {
    super(pipeline, options);
  }

  protected getSource(): AggregationSource<Document> {
    return this.collection;
  }
}
