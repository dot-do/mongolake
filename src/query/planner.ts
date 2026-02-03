/**
 * Query Cost Estimator and Planner
 *
 * Provides cost-based query optimization for MongoLake collections.
 * Estimates the cost of different execution strategies and selects
 * the optimal approach based on:
 * - Collection statistics (document count, cardinality)
 * - Available indexes
 * - Query filter conditions
 *
 * Cost Model:
 * - Full scan: O(n) where n = document count
 * - Index lookup: O(log n) + (selectivity * n)
 * - Compound index: O(log n) + (combined selectivity * n)
 */

import type { Filter, Document } from '../types.js';
import type { IndexManager, QueryPlan as IndexQueryPlan } from '../index/index-manager.js';
import type { IndexMetadata } from '../index/btree.js';
import type { CompoundIndexMetadata } from '../index/compound.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Statistics about a collection used for cost estimation
 */
export interface CollectionStats {
  /** Total number of documents in the collection */
  documentCount: number;

  /**
   * Average document size in bytes.
   * Used for I/O cost estimation.
   */
  avgDocumentSize?: number;

  /**
   * Field cardinality estimates.
   * Maps field name to estimated number of distinct values.
   */
  fieldCardinality?: Map<string, number>;

  /**
   * Index statistics.
   * Maps index name to index-specific statistics.
   */
  indexStats?: Map<string, IndexStats>;
}

/**
 * Statistics about an index
 */
export interface IndexStats {
  /** Index name */
  name: string;

  /** Total number of entries in the index */
  entryCount: number;

  /** Number of unique keys in the index */
  uniqueKeys: number;

  /** Index depth (for B-tree indexes) */
  depth?: number;

  /** Average entries per key (for non-unique indexes) */
  avgEntriesPerKey?: number;
}

/**
 * Cost estimate for a query execution strategy
 */
export interface CostEstimate {
  /** Total estimated cost (lower is better) */
  totalCost: number;

  /**
   * Cost breakdown by component:
   * - indexSeek: Cost of traversing the index
   * - indexScan: Cost of scanning matching index entries
   * - documentFetch: Cost of fetching documents by ID
   * - filter: Cost of applying residual filters
   */
  breakdown: {
    indexSeek: number;
    indexScan: number;
    documentFetch: number;
    filter: number;
  };

  /** Estimated number of documents examined */
  documentsExamined: number;

  /** Estimated number of documents returned */
  documentsReturned: number;

  /** Estimated selectivity (0.0 to 1.0) */
  selectivity: number;

  /** Whether this estimate uses an index */
  usesIndex: boolean;

  /** Index name if using an index */
  indexName?: string;
}

/**
 * Index selection result
 */
export interface IndexSelection {
  /** Selected index name, or null for full scan */
  indexName: string | null;

  /** Type of index selected */
  indexType: 'single' | 'compound' | 'text' | 'geo' | null;

  /** Fields covered by the index */
  coveredFields: string[];

  /** Estimated selectivity of the index scan */
  selectivity: number;

  /** Reason for selection */
  reason: string;

  /** Alternative indexes considered */
  alternatives: Array<{
    indexName: string;
    cost: number;
    reason: string;
  }>;
}

/**
 * Execution plan for a query
 */
export interface ExecutionPlan {
  /** Unique identifier for this plan */
  planId: string;

  /** Strategy: 'index_scan', 'full_scan', 'index_intersection', 'covered_query' */
  strategy: 'index_scan' | 'full_scan' | 'index_intersection' | 'covered_query';

  /** Estimated cost */
  cost: CostEstimate;

  /** Index to use (if any) */
  indexName?: string;

  /** Fields used from the index */
  indexFields?: string[];

  /** Type of index operation */
  operation?: 'eq' | 'range' | 'in' | 'prefix' | 'geo' | 'text';

  /** Bounds for index scan */
  indexBounds?: {
    field: string;
    min?: unknown;
    max?: unknown;
    inclusive: { min: boolean; max: boolean };
  }[];

  /** Residual filter to apply after index scan */
  residualFilter?: Filter<Document>;

  /** Sort optimization */
  sortOptimization?: {
    canUseIndexSort: boolean;
    sortFields: string[];
  };

  /** Human-readable explanation */
  explanation: string;

  /** Timestamp when plan was created */
  createdAt: Date;
}

/**
 * Options for query planning
 */
export interface PlannerOptions {
  /**
   * Collection statistics for cost estimation.
   * If not provided, defaults are used.
   */
  stats?: CollectionStats;

  /**
   * Hint to force a specific index.
   * Bypasses cost estimation.
   */
  hint?: string | { [key: string]: 1 | -1 };

  /**
   * Maximum number of alternative plans to consider.
   * @default 5
   */
  maxAlternatives?: number;

  /**
   * Whether to consider index intersection.
   * @default true
   */
  allowIndexIntersection?: boolean;
}

// ============================================================================
// Cost Model Constants
// ============================================================================

/**
 * Cost factors for the query optimizer.
 * These are relative costs used to compare different strategies.
 */
const COST_FACTORS = {
  /** Cost per document for full collection scan */
  FULL_SCAN_PER_DOC: 1.0,

  /** Base cost for index seek (B-tree traversal) */
  INDEX_SEEK_BASE: 10.0,

  /** Cost per level of B-tree traversal */
  INDEX_SEEK_PER_LEVEL: 2.0,

  /** Cost per index entry scanned */
  INDEX_SCAN_PER_ENTRY: 0.5,

  /** Cost per document fetch by ID */
  DOCUMENT_FETCH_PER_DOC: 1.5,

  /** Cost per document for filter evaluation */
  FILTER_PER_DOC: 0.3,

  /** Multiplier for range queries vs equality */
  RANGE_SELECTIVITY_MULTIPLIER: 3.0,

  /** Multiplier for $in queries */
  IN_SELECTIVITY_MULTIPLIER: 1.5,

  /** Multiplier for compound index prefix match */
  COMPOUND_PREFIX_MULTIPLIER: 0.8,

  /** Minimum selectivity (even highly selective indexes return something) */
  MIN_SELECTIVITY: 0.001,

  /** Default selectivity when cardinality is unknown */
  DEFAULT_SELECTIVITY: 0.1,
} as const;

/**
 * Default B-tree depth estimation based on document count
 */
function estimateBTreeDepth(documentCount: number, fanout: number = 128): number {
  if (documentCount <= 1) return 1;
  return Math.ceil(Math.log(documentCount) / Math.log(fanout));
}

// ============================================================================
// Query Cost Estimator
// ============================================================================

/**
 * Estimates the cost of executing a query with different strategies.
 */
export class QueryCostEstimator {
  private defaultStats: CollectionStats = {
    documentCount: 1000, // Conservative default
    avgDocumentSize: 500,
    fieldCardinality: new Map(),
    indexStats: new Map(),
  };

  /**
   * Estimate the cost of a full collection scan
   */
  estimateFullScanCost(
    filter: Filter<Document>,
    stats: CollectionStats = this.defaultStats
  ): CostEstimate {
    const docCount = stats.documentCount;
    const filterComplexity = this.estimateFilterComplexity(filter);

    // Full scan examines all documents
    const scanCost = docCount * COST_FACTORS.FULL_SCAN_PER_DOC;
    const filterCost = docCount * COST_FACTORS.FILTER_PER_DOC * filterComplexity;

    // Estimate selectivity based on filter
    const selectivity = this.estimateFilterSelectivity(filter, stats);
    const estimatedReturned = Math.max(1, Math.ceil(docCount * selectivity));

    return {
      totalCost: scanCost + filterCost,
      breakdown: {
        indexSeek: 0,
        indexScan: 0,
        documentFetch: scanCost,
        filter: filterCost,
      },
      documentsExamined: docCount,
      documentsReturned: estimatedReturned,
      selectivity,
      usesIndex: false,
    };
  }

  /**
   * Estimate the cost of an index scan
   */
  estimateIndexScanCost(
    filter: Filter<Document>,
    indexName: string,
    indexField: string,
    operation: 'eq' | 'range' | 'in',
    stats: CollectionStats = this.defaultStats
  ): CostEstimate {
    const docCount = stats.documentCount;

    // Estimate B-tree depth
    const depth = estimateBTreeDepth(docCount);

    // Index seek cost
    const seekCost = COST_FACTORS.INDEX_SEEK_BASE + (depth * COST_FACTORS.INDEX_SEEK_PER_LEVEL);

    // Estimate selectivity based on operation and cardinality
    let selectivity = this.estimateOperationSelectivity(
      filter,
      indexField,
      operation,
      stats
    );

    // Estimate matching documents
    const matchingDocs = Math.max(1, Math.ceil(docCount * selectivity));

    // Index scan cost
    const indexScanCost = matchingDocs * COST_FACTORS.INDEX_SCAN_PER_ENTRY;

    // Document fetch cost
    const fetchCost = matchingDocs * COST_FACTORS.DOCUMENT_FETCH_PER_DOC;

    // Residual filter cost (for conditions not covered by index)
    const residualFields = this.countNonIndexedFields(filter, [indexField]);
    const residualFilterCost = residualFields > 0
      ? matchingDocs * COST_FACTORS.FILTER_PER_DOC * residualFields
      : 0;

    // Estimate final result count after residual filter
    const residualSelectivity = residualFields > 0 ? 0.5 : 1.0;
    const estimatedReturned = Math.max(1, Math.ceil(matchingDocs * residualSelectivity));

    return {
      totalCost: seekCost + indexScanCost + fetchCost + residualFilterCost,
      breakdown: {
        indexSeek: seekCost,
        indexScan: indexScanCost,
        documentFetch: fetchCost,
        filter: residualFilterCost,
      },
      documentsExamined: matchingDocs,
      documentsReturned: estimatedReturned,
      selectivity,
      usesIndex: true,
      indexName,
    };
  }

  /**
   * Estimate the cost of a compound index scan
   */
  estimateCompoundIndexCost(
    filter: Filter<Document>,
    indexName: string,
    _indexFields: string[],  // All fields in the compound index (for documentation)
    equalityFields: string[],
    rangeField?: string,
    stats: CollectionStats = this.defaultStats
  ): CostEstimate {
    const docCount = stats.documentCount;

    // Estimate B-tree depth
    const depth = estimateBTreeDepth(docCount);

    // Index seek cost
    const seekCost = COST_FACTORS.INDEX_SEEK_BASE + (depth * COST_FACTORS.INDEX_SEEK_PER_LEVEL);

    // Calculate combined selectivity
    let combinedSelectivity = 1.0;

    // Each equality field narrows the result set
    for (const field of equalityFields) {
      const fieldSelectivity = this.estimateFieldSelectivity(field, stats);
      combinedSelectivity *= fieldSelectivity;
    }

    // Range field adds less selectivity
    if (rangeField) {
      const rangeSelectivity = this.estimateFieldSelectivity(rangeField, stats) *
        COST_FACTORS.RANGE_SELECTIVITY_MULTIPLIER;
      combinedSelectivity *= Math.min(rangeSelectivity, 0.5);
    }

    // Apply compound prefix multiplier (compound indexes are often more efficient)
    combinedSelectivity *= Math.pow(
      COST_FACTORS.COMPOUND_PREFIX_MULTIPLIER,
      equalityFields.length - 1
    );

    // Ensure minimum selectivity
    combinedSelectivity = Math.max(COST_FACTORS.MIN_SELECTIVITY, combinedSelectivity);

    // Estimate matching documents
    const matchingDocs = Math.max(1, Math.ceil(docCount * combinedSelectivity));

    // Index scan cost
    const indexScanCost = matchingDocs * COST_FACTORS.INDEX_SCAN_PER_ENTRY;

    // Document fetch cost
    const fetchCost = matchingDocs * COST_FACTORS.DOCUMENT_FETCH_PER_DOC;

    // Residual filter cost
    const coveredFields = [...equalityFields];
    if (rangeField) coveredFields.push(rangeField);
    const residualFields = this.countNonIndexedFields(filter, coveredFields);
    const residualFilterCost = residualFields > 0
      ? matchingDocs * COST_FACTORS.FILTER_PER_DOC * residualFields
      : 0;

    // Estimate final result count
    const residualSelectivity = residualFields > 0 ? 0.5 : 1.0;
    const estimatedReturned = Math.max(1, Math.ceil(matchingDocs * residualSelectivity));

    return {
      totalCost: seekCost + indexScanCost + fetchCost + residualFilterCost,
      breakdown: {
        indexSeek: seekCost,
        indexScan: indexScanCost,
        documentFetch: fetchCost,
        filter: residualFilterCost,
      },
      documentsExamined: matchingDocs,
      documentsReturned: estimatedReturned,
      selectivity: combinedSelectivity,
      usesIndex: true,
      indexName,
    };
  }

  /**
   * Estimate filter complexity (number of conditions)
   */
  private estimateFilterComplexity(filter: Filter<Document>): number {
    if (!filter || Object.keys(filter).length === 0) return 0.1;

    let complexity = 0;

    for (const [key, condition] of Object.entries(filter)) {
      if (key.startsWith('$')) {
        // Logical operator
        if (key === '$and' || key === '$or' || key === '$nor') {
          const subFilters = condition as Filter<Document>[];
          for (const sub of subFilters) {
            complexity += this.estimateFilterComplexity(sub);
          }
        } else if (key === '$not') {
          complexity += this.estimateFilterComplexity(condition as Filter<Document>);
        }
      } else {
        // Field condition
        if (typeof condition === 'object' && condition !== null) {
          complexity += Object.keys(condition).length;
        } else {
          complexity += 1;
        }
      }
    }

    return Math.max(1, complexity);
  }

  /**
   * Estimate filter selectivity (fraction of documents that match)
   */
  private estimateFilterSelectivity(
    filter: Filter<Document>,
    stats: CollectionStats
  ): number {
    if (!filter || Object.keys(filter).length === 0) return 1.0;

    let selectivity = 1.0;

    for (const [key, condition] of Object.entries(filter)) {
      if (key.startsWith('$')) {
        // Handle logical operators
        if (key === '$and') {
          for (const sub of condition as Filter<Document>[]) {
            selectivity *= this.estimateFilterSelectivity(sub, stats);
          }
        } else if (key === '$or') {
          let orSelectivity = 0;
          for (const sub of condition as Filter<Document>[]) {
            orSelectivity += this.estimateFilterSelectivity(sub, stats);
          }
          selectivity *= Math.min(orSelectivity, 1.0);
        }
      } else {
        // Field condition
        selectivity *= this.estimateConditionSelectivity(key, condition, stats);
      }
    }

    return Math.max(COST_FACTORS.MIN_SELECTIVITY, selectivity);
  }

  /**
   * Estimate selectivity for a single field condition
   */
  private estimateConditionSelectivity(
    field: string,
    condition: unknown,
    stats: CollectionStats
  ): number {
    const cardinality = stats.fieldCardinality?.get(field);

    // Base selectivity from cardinality
    let baseSelectivity = cardinality
      ? 1 / cardinality
      : COST_FACTORS.DEFAULT_SELECTIVITY;

    // Adjust based on operator
    if (typeof condition === 'object' && condition !== null) {
      const ops = condition as Record<string, unknown>;

      if ('$eq' in ops || Object.keys(ops).length === 0) {
        // Equality - use base selectivity
        return baseSelectivity;
      }

      if ('$in' in ops) {
        const values = ops.$in as unknown[];
        return Math.min(baseSelectivity * values.length * COST_FACTORS.IN_SELECTIVITY_MULTIPLIER, 0.5);
      }

      if ('$gt' in ops || '$gte' in ops || '$lt' in ops || '$lte' in ops) {
        // Range - less selective
        return Math.min(baseSelectivity * COST_FACTORS.RANGE_SELECTIVITY_MULTIPLIER, 0.5);
      }

      if ('$ne' in ops) {
        // Not equal - very unselective
        return 1 - baseSelectivity;
      }

      if ('$exists' in ops) {
        return ops.$exists ? 0.9 : 0.1;
      }

      if ('$regex' in ops) {
        // Regex - moderately selective
        const pattern = ops.$regex as string;
        if (pattern.startsWith('^')) {
          return 0.1; // Anchored regex is more selective
        }
        return 0.3;
      }
    }

    // Direct equality value
    return baseSelectivity;
  }

  /**
   * Estimate selectivity for a specific operation type
   */
  private estimateOperationSelectivity(
    filter: Filter<Document>,
    field: string,
    operation: 'eq' | 'range' | 'in',
    stats: CollectionStats
  ): number {
    const condition = filter[field];
    const cardinality = stats.fieldCardinality?.get(field);
    const baseSelectivity = cardinality
      ? 1 / cardinality
      : COST_FACTORS.DEFAULT_SELECTIVITY;

    switch (operation) {
      case 'eq':
        return baseSelectivity;

      case 'in':
        if (typeof condition === 'object' && condition !== null && '$in' in condition) {
          const values = (condition as { $in: unknown[] }).$in;
          return Math.min(
            baseSelectivity * values.length * COST_FACTORS.IN_SELECTIVITY_MULTIPLIER,
            0.5
          );
        }
        return baseSelectivity;

      case 'range':
        return Math.min(baseSelectivity * COST_FACTORS.RANGE_SELECTIVITY_MULTIPLIER, 0.5);

      default:
        return baseSelectivity;
    }
  }

  /**
   * Estimate selectivity for a field
   */
  private estimateFieldSelectivity(field: string, stats: CollectionStats): number {
    const cardinality = stats.fieldCardinality?.get(field);
    if (cardinality) {
      return 1 / cardinality;
    }
    return COST_FACTORS.DEFAULT_SELECTIVITY;
  }

  /**
   * Count filter fields not covered by the index
   */
  private countNonIndexedFields(filter: Filter<Document>, indexedFields: string[]): number {
    const indexedSet = new Set(indexedFields);
    let count = 0;

    for (const key of Object.keys(filter)) {
      if (!key.startsWith('$') && !indexedSet.has(key)) {
        count++;
      }
    }

    return count;
  }
}

// ============================================================================
// Query Planner
// ============================================================================

/**
 * QueryPlanner selects optimal execution strategies for queries.
 *
 * @example
 * ```typescript
 * const planner = new QueryPlanner(indexManager);
 *
 * // Get cost estimate for a filter
 * const cost = await planner.estimateQueryCost(
 *   { age: { $gt: 25 }, status: 'active' },
 *   { documentCount: 100000 }
 * );
 *
 * // Select optimal index
 * const selection = await planner.selectOptimalIndex(
 *   { age: { $gt: 25 } },
 *   indexes
 * );
 *
 * // Create execution plan
 * const plan = await planner.createExecutionPlan(
 *   { age: { $gt: 25 }, status: 'active' },
 *   { stats: { documentCount: 100000 } }
 * );
 * ```
 */
export class QueryPlanner {
  private costEstimator: QueryCostEstimator;

  constructor(private indexManager?: IndexManager) {
    this.costEstimator = new QueryCostEstimator();
  }

  /**
   * Estimate the total cost of executing a query
   *
   * @param filter - Query filter
   * @param stats - Collection statistics
   * @returns Cost estimate for the optimal strategy
   */
  async estimateQueryCost(
    filter: Filter<Document>,
    stats?: CollectionStats
  ): Promise<CostEstimate> {
    const plan = await this.createExecutionPlan(filter, { stats });
    return plan.cost;
  }

  /**
   * Select the optimal index for a query
   *
   * @param filter - Query filter
   * @param availableIndexes - List of available indexes
   * @returns Index selection with reasoning
   */
  async selectOptimalIndex(
    filter: Filter<Document>,
    availableIndexes?: Array<IndexMetadata | CompoundIndexMetadata>
  ): Promise<IndexSelection> {
    // Empty filter - full scan
    if (!filter || Object.keys(filter).length === 0) {
      return {
        indexName: null,
        indexType: null,
        coveredFields: [],
        selectivity: 1.0,
        reason: 'Empty filter requires full collection scan',
        alternatives: [],
      };
    }

    // Get indexes from IndexManager if not provided
    let indexes = availableIndexes;
    if (!indexes && this.indexManager) {
      const indexList = await this.indexManager.listIndexes();
      indexes = indexList;
    }

    if (!indexes || indexes.length === 0) {
      return {
        indexName: null,
        indexType: null,
        coveredFields: [],
        selectivity: 1.0,
        reason: 'No indexes available',
        alternatives: [],
      };
    }

    // Extract filter fields (excluding logical operators)
    const filterFields = new Set(
      Object.keys(filter).filter((k) => !k.startsWith('$'))
    );

    // Score each index
    const scoredIndexes: Array<{
      index: IndexMetadata | CompoundIndexMetadata;
      score: number;
      coveredFields: string[];
      reason: string;
      isCompound: boolean;
    }> = [];

    for (const index of indexes) {
      const isCompound = 'fields' in index;
      let score = 0;
      let coveredFields: string[] = [];
      let reason = '';

      if (isCompound) {
        // Compound index
        const compoundIndex = index as CompoundIndexMetadata;
        const indexFields = compoundIndex.fields.map((f) => f.field);

        // Check prefix coverage
        let prefixMatch = 0;
        for (const field of indexFields) {
          if (filterFields.has(field)) {
            prefixMatch++;
            coveredFields.push(field);
          } else {
            break; // Compound index requires prefix match
          }
        }

        if (prefixMatch > 0) {
          // Score based on prefix length and equality conditions
          score = prefixMatch * 10;
          for (const field of coveredFields) {
            const condition = filter[field];
            if (this.isEqualityCondition(condition)) {
              score += 5; // Bonus for equality on prefix
            }
          }
          reason = `Compound index covers ${prefixMatch} prefix field(s)`;
        } else {
          reason = 'No prefix match for compound index';
        }
      } else {
        // Single field index
        const singleIndex = index as IndexMetadata;

        if (filterFields.has(singleIndex.field)) {
          coveredFields = [singleIndex.field];
          score = 5;

          const condition = filter[singleIndex.field];
          if (this.isEqualityCondition(condition)) {
            score += 3;
            reason = 'Equality condition on indexed field';
          } else if (this.isRangeCondition(condition)) {
            score += 1;
            reason = 'Range condition on indexed field';
          } else {
            reason = 'Index covers filter field';
          }
        } else {
          reason = 'Index field not in filter';
        }
      }

      if (score > 0) {
        scoredIndexes.push({
          index,
          score,
          coveredFields,
          reason,
          isCompound,
        });
      }
    }

    // Sort by score (descending)
    scoredIndexes.sort((a, b) => b.score - a.score);

    // Select the best index
    if (scoredIndexes.length === 0) {
      return {
        indexName: null,
        indexType: null,
        coveredFields: [],
        selectivity: 1.0,
        reason: 'No suitable index found for filter fields',
        alternatives: [],
      };
    }

    const best = scoredIndexes[0]!;
    const selectivity = Math.pow(0.1, best.coveredFields.length);

    return {
      indexName: best.index.name,
      indexType: best.isCompound ? 'compound' : 'single',
      coveredFields: best.coveredFields,
      selectivity,
      reason: best.reason,
      alternatives: scoredIndexes.slice(1, 4).map((s) => ({
        indexName: s.index.name,
        cost: 100 - s.score, // Invert score to cost
        reason: s.reason,
      })),
    };
  }

  /**
   * Create an execution plan for a query
   *
   * @param filter - Query filter
   * @param options - Planning options
   * @returns Execution plan with cost estimate
   */
  async createExecutionPlan(
    filter: Filter<Document>,
    options?: PlannerOptions
  ): Promise<ExecutionPlan> {
    const stats = options?.stats ?? { documentCount: 1000 };
    const planId = crypto.randomUUID();

    // Empty filter - full scan
    if (!filter || Object.keys(filter).length === 0) {
      const fullScanCost = this.costEstimator.estimateFullScanCost({}, stats);
      return {
        planId,
        strategy: 'full_scan',
        cost: fullScanCost,
        explanation: 'Empty filter requires full collection scan',
        createdAt: new Date(),
      };
    }

    // Check for index hint
    if (options?.hint) {
      return this.createHintedPlan(filter, options.hint, stats, planId);
    }

    // Get available indexes
    let indexQueryPlan: IndexQueryPlan | null = null;
    if (this.indexManager) {
      indexQueryPlan = await this.indexManager.analyzeQuery(filter);
    }

    // If IndexManager found an index to use
    if (indexQueryPlan?.useIndex && indexQueryPlan.indexName && indexQueryPlan.field) {
      const indexCost = this.costEstimator.estimateIndexScanCost(
        filter,
        indexQueryPlan.indexName,
        indexQueryPlan.field,
        indexQueryPlan.operation ?? 'eq',
        stats
      );

      const fullScanCost = this.costEstimator.estimateFullScanCost(filter, stats);

      // Compare costs
      if (indexCost.totalCost < fullScanCost.totalCost) {
        return this.buildIndexScanPlan(
          planId,
          filter,
          indexQueryPlan.indexName,
          indexQueryPlan.field,
          indexQueryPlan.operation ?? 'eq',
          indexCost
        );
      }
    }

    // Fall back to full scan
    const fullScanCost = this.costEstimator.estimateFullScanCost(filter, stats);
    return {
      planId,
      strategy: 'full_scan',
      cost: fullScanCost,
      residualFilter: filter,
      explanation: this.generateFullScanExplanation(filter, fullScanCost),
      createdAt: new Date(),
    };
  }

  /**
   * Explain a query plan in human-readable format
   */
  async explain(filter: Filter<Document>, options?: PlannerOptions): Promise<string> {
    const plan = await this.createExecutionPlan(filter, options);
    const lines: string[] = [];

    lines.push(`Query Plan (${plan.planId}):`);
    lines.push(`  Strategy: ${plan.strategy}`);
    lines.push(`  Total Cost: ${plan.cost.totalCost.toFixed(2)}`);
    lines.push('');
    lines.push('Cost Breakdown:');
    lines.push(`  Index Seek:     ${plan.cost.breakdown.indexSeek.toFixed(2)}`);
    lines.push(`  Index Scan:     ${plan.cost.breakdown.indexScan.toFixed(2)}`);
    lines.push(`  Document Fetch: ${plan.cost.breakdown.documentFetch.toFixed(2)}`);
    lines.push(`  Filter:         ${plan.cost.breakdown.filter.toFixed(2)}`);
    lines.push('');
    lines.push('Estimates:');
    lines.push(`  Documents Examined: ${plan.cost.documentsExamined}`);
    lines.push(`  Documents Returned: ${plan.cost.documentsReturned}`);
    lines.push(`  Selectivity:        ${(plan.cost.selectivity * 100).toFixed(2)}%`);

    if (plan.indexName) {
      lines.push('');
      lines.push(`Index: ${plan.indexName}`);
      if (plan.indexFields) {
        lines.push(`  Fields: ${plan.indexFields.join(', ')}`);
      }
      if (plan.operation) {
        lines.push(`  Operation: ${plan.operation}`);
      }
    }

    if (plan.residualFilter && Object.keys(plan.residualFilter).length > 0) {
      lines.push('');
      lines.push('Residual Filter:');
      lines.push(`  ${JSON.stringify(plan.residualFilter)}`);
    }

    lines.push('');
    lines.push(`Explanation: ${plan.explanation}`);

    return lines.join('\n');
  }

  // --------------------------------------------------------------------------
  // Private Helpers
  // --------------------------------------------------------------------------

  /**
   * Create a plan when a hint is provided
   */
  private createHintedPlan(
    filter: Filter<Document>,
    hint: string | { [key: string]: 1 | -1 },
    stats: CollectionStats,
    planId: string
  ): ExecutionPlan {
    const indexName = typeof hint === 'string' ? hint : Object.keys(hint)[0]!;

    // Estimate cost assuming the hinted index can be used
    const filterFields = Object.keys(filter).filter((k) => !k.startsWith('$'));
    const firstField = filterFields[0] ?? '';

    const cost = this.costEstimator.estimateIndexScanCost(
      filter,
      indexName,
      firstField,
      'eq',
      stats
    );

    return {
      planId,
      strategy: 'index_scan',
      cost,
      indexName,
      indexFields: [firstField],
      residualFilter: this.buildResidualFilter(filter, [firstField]),
      explanation: `Using hinted index '${indexName}'`,
      createdAt: new Date(),
    };
  }

  /**
   * Build an index scan plan
   */
  private buildIndexScanPlan(
    planId: string,
    filter: Filter<Document>,
    indexName: string,
    field: string,
    operation: 'eq' | 'range' | 'in',
    cost: CostEstimate
  ): ExecutionPlan {
    const condition = filter[field];
    const indexBounds = this.extractIndexBounds(field, condition);
    const residualFilter = this.buildResidualFilter(filter, [field]);

    return {
      planId,
      strategy: 'index_scan',
      cost,
      indexName,
      indexFields: [field],
      operation,
      indexBounds: indexBounds ? [indexBounds] : undefined,
      residualFilter: Object.keys(residualFilter).length > 0 ? residualFilter : undefined,
      explanation: `Using index '${indexName}' for ${operation} on '${field}' ` +
        `(selectivity: ${(cost.selectivity * 100).toFixed(1)}%, ` +
        `cost: ${cost.totalCost.toFixed(1)})`,
      createdAt: new Date(),
    };
  }

  /**
   * Extract index bounds from a condition
   */
  private extractIndexBounds(
    field: string,
    condition: unknown
  ): { field: string; min?: unknown; max?: unknown; inclusive: { min: boolean; max: boolean } } | null {
    if (typeof condition !== 'object' || condition === null) {
      // Direct equality
      return {
        field,
        min: condition,
        max: condition,
        inclusive: { min: true, max: true },
      };
    }

    const ops = condition as Record<string, unknown>;
    let min: unknown = undefined;
    let max: unknown = undefined;
    let minInclusive = true;
    let maxInclusive = true;

    if ('$eq' in ops) {
      return {
        field,
        min: ops.$eq,
        max: ops.$eq,
        inclusive: { min: true, max: true },
      };
    }

    if ('$gt' in ops) {
      min = ops.$gt;
      minInclusive = false;
    }
    if ('$gte' in ops) {
      min = ops.$gte;
      minInclusive = true;
    }
    if ('$lt' in ops) {
      max = ops.$lt;
      maxInclusive = false;
    }
    if ('$lte' in ops) {
      max = ops.$lte;
      maxInclusive = true;
    }

    if (min !== undefined || max !== undefined) {
      return {
        field,
        min,
        max,
        inclusive: { min: minInclusive, max: maxInclusive },
      };
    }

    return null;
  }

  /**
   * Build residual filter excluding indexed fields
   */
  private buildResidualFilter(filter: Filter<Document>, indexedFields: string[]): Filter<Document> {
    const indexedSet = new Set(indexedFields);
    const residual: Filter<Document> = {};

    for (const [key, value] of Object.entries(filter)) {
      if (!indexedSet.has(key)) {
        residual[key] = value;
      }
    }

    return residual;
  }

  /**
   * Check if a condition is an equality condition
   */
  private isEqualityCondition(condition: unknown): boolean {
    if (typeof condition !== 'object' || condition === null) {
      return true; // Direct value comparison
    }
    const ops = condition as Record<string, unknown>;
    return '$eq' in ops;
  }

  /**
   * Check if a condition is a range condition
   */
  private isRangeCondition(condition: unknown): boolean {
    if (typeof condition !== 'object' || condition === null) {
      return false;
    }
    const ops = condition as Record<string, unknown>;
    return '$gt' in ops || '$gte' in ops || '$lt' in ops || '$lte' in ops;
  }

  /**
   * Generate explanation for a full scan plan
   */
  private generateFullScanExplanation(filter: Filter<Document>, cost: CostEstimate): string {
    const filterFields = Object.keys(filter).filter((k) => !k.startsWith('$'));

    if (filterFields.length === 0) {
      return 'Full collection scan (no filter conditions)';
    }

    return `Full collection scan - no suitable index for fields: ${filterFields.join(', ')} ` +
      `(examining ${cost.documentsExamined} documents)`;
  }
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Estimate query cost without an IndexManager
 *
 * @param filter - Query filter
 * @param indexes - Available indexes
 * @param stats - Collection statistics
 * @returns Cost estimate
 */
export function estimateQueryCost(
  filter: Filter<Document>,
  indexes: Array<IndexMetadata | CompoundIndexMetadata>,
  stats?: CollectionStats
): CostEstimate {
  const estimator = new QueryCostEstimator();
  const defaultStats: CollectionStats = stats ?? { documentCount: 1000 };

  // Check for usable index
  const filterFields = Object.keys(filter).filter((k) => !k.startsWith('$'));

  for (const index of indexes) {
    const isCompound = 'fields' in index;

    if (isCompound) {
      const compoundIndex = index as CompoundIndexMetadata;
      const firstField = compoundIndex.fields[0]?.field;
      if (firstField && filterFields.includes(firstField)) {
        return estimator.estimateIndexScanCost(
          filter,
          compoundIndex.name,
          firstField,
          'eq',
          defaultStats
        );
      }
    } else {
      const singleIndex = index as IndexMetadata;
      if (filterFields.includes(singleIndex.field)) {
        return estimator.estimateIndexScanCost(
          filter,
          singleIndex.name,
          singleIndex.field,
          'eq',
          defaultStats
        );
      }
    }
  }

  return estimator.estimateFullScanCost(filter, defaultStats);
}

/**
 * Select optimal index for a filter
 *
 * @param filter - Query filter
 * @param availableIndexes - List of available indexes
 * @returns Index selection
 */
export async function selectOptimalIndex(
  filter: Filter<Document>,
  availableIndexes: Array<IndexMetadata | CompoundIndexMetadata>
): Promise<IndexSelection> {
  const planner = new QueryPlanner();
  return planner.selectOptimalIndex(filter, availableIndexes);
}

/**
 * Create execution plan for a query
 *
 * @param query - Query filter
 * @param options - Planning options
 * @returns Execution plan
 */
export async function createExecutionPlan(
  query: Filter<Document>,
  options?: PlannerOptions
): Promise<ExecutionPlan> {
  const planner = new QueryPlanner();
  return planner.createExecutionPlan(query, options);
}

// ============================================================================
// Exports
// ============================================================================

export default QueryPlanner;
