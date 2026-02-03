/**
 * Query Planner
 *
 * Analyzes query filters and determines optimal execution plans
 * by matching filters against available indexes.
 */

import type { Filter, Document } from '../types.js';
import type { IndexManager, QueryPlan } from './index-manager.js';

// ============================================================================
// Types
// ============================================================================

/** Detailed execution plan returned by the planner */
export interface ExecutionPlan {
  /** Strategy to use: 'index_scan' or 'full_scan' */
  strategy: 'index_scan' | 'full_scan';

  /** Name of the index to use (if index_scan) */
  indexName?: string;

  /** Field being indexed (if index_scan) */
  field?: string;

  /** Type of index operation */
  operation?: 'eq' | 'range' | 'in' | 'geo';

  /** Filter condition for the indexed field */
  condition?: unknown;

  /** Remaining filter conditions to apply after index scan */
  residualFilter?: Filter<Document>;

  /** Estimated selectivity (0.0-1.0, lower is better) */
  estimatedSelectivity: number;

  /** Human-readable explanation of the plan */
  explanation: string;
}

/** Plan execution result */
export interface PlanExecutionResult {
  /** Document IDs from index scan (if used) */
  docIds?: string[];

  /** Whether this is an exact match (no residual filter needed for indexed field) */
  exact: boolean;

  /** The execution plan that was used */
  plan: ExecutionPlan;
}

// ============================================================================
// Query Planner
// ============================================================================

/**
 * QueryPlanner analyzes filters and selects optimal execution strategies.
 *
 * It determines whether to use an index scan or full collection scan
 * based on available indexes and filter conditions.
 *
 * @example
 * ```typescript
 * const planner = new QueryPlanner(indexManager);
 *
 * // Analyze a filter
 * const plan = await planner.createPlan('users', { age: 25 });
 *
 * if (plan.strategy === 'index_scan') {
 *   // Execute index scan
 *   const result = await planner.executePlan('users', { age: 25 }, plan);
 *   // Use result.docIds to fetch documents
 * }
 * ```
 */
export class QueryPlanner {
  constructor(private indexManager: IndexManager) {}

  // --------------------------------------------------------------------------
  // Plan Creation
  // --------------------------------------------------------------------------

  /**
   * Create an execution plan for a filter.
   *
   * Analyzes the filter to determine if any indexed fields can be used
   * to optimize the query execution.
   *
   * @param _collection - Collection name (for context, reserved for future use)
   * @param filter - Query filter to analyze
   * @returns Execution plan with strategy and details
   */
  async createPlan(_collection: string, filter: Filter<Document>): Promise<ExecutionPlan> {
    // Empty filter = full scan
    if (!filter || Object.keys(filter).length === 0) {
      return {
        strategy: 'full_scan',
        estimatedSelectivity: 1.0,
        explanation: 'Empty filter requires full collection scan',
      };
    }

    // Analyze the filter using IndexManager
    const queryPlan = await this.indexManager.analyzeQuery(filter);

    if (!queryPlan.useIndex) {
      return this.createFullScanPlan(filter);
    }

    return this.createIndexScanPlan(filter, queryPlan);
  }

  /**
   * Create a full scan plan.
   */
  private createFullScanPlan(filter: Filter<Document>): ExecutionPlan {
    const filterKeys = Object.keys(filter).filter(k => !k.startsWith('$'));

    return {
      strategy: 'full_scan',
      residualFilter: filter,
      estimatedSelectivity: 1.0,
      explanation: filterKeys.length > 0
        ? `No index available for fields: ${filterKeys.join(', ')}`
        : 'Filter contains only logical operators, requires full scan',
    };
  }

  /**
   * Create an index scan plan.
   */
  private createIndexScanPlan(filter: Filter<Document>, queryPlan: QueryPlan): ExecutionPlan {
    const { indexName, field, operation } = queryPlan;

    // Build residual filter (conditions not covered by index)
    const residualFilter = this.buildResidualFilter(filter, field!);

    // Estimate selectivity based on operation type
    const selectivity = this.estimateSelectivity(operation!);

    // Extract the condition for the indexed field
    const condition = filter[field!];

    return {
      strategy: 'index_scan',
      indexName,
      field,
      operation,
      condition,
      residualFilter: Object.keys(residualFilter).length > 0 ? residualFilter : undefined,
      estimatedSelectivity: selectivity,
      explanation: `Using index '${indexName}' for ${operation} on '${field}'`,
    };
  }

  /**
   * Build residual filter excluding the indexed field.
   */
  private buildResidualFilter(filter: Filter<Document>, indexedField: string): Filter<Document> {
    const residual: Filter<Document> = {};

    for (const [key, value] of Object.entries(filter)) {
      if (key !== indexedField) {
        residual[key] = value;
      }
    }

    return residual;
  }

  /**
   * Estimate selectivity based on operation type.
   *
   * Lower values indicate more selective (fewer documents matched).
   */
  private estimateSelectivity(operation: 'eq' | 'range' | 'in'): number {
    switch (operation) {
      case 'eq':
        return 0.01; // Equality is highly selective
      case 'in':
        return 0.1; // IN with few values is fairly selective
      case 'range':
        return 0.3; // Range queries are less selective
      default:
        return 1.0;
    }
  }

  // --------------------------------------------------------------------------
  // Plan Execution
  // --------------------------------------------------------------------------

  /**
   * Execute an index scan based on the execution plan.
   *
   * @param _collection - Collection name (for context, reserved for future use)
   * @param filter - Original filter
   * @param plan - Execution plan from createPlan()
   * @returns Document IDs matching the index condition
   */
  async executePlan(
    _collection: string,
    filter: Filter<Document>,
    plan: ExecutionPlan
  ): Promise<PlanExecutionResult> {
    if (plan.strategy !== 'index_scan' || !plan.indexName || !plan.field) {
      return {
        exact: false,
        plan,
      };
    }

    // Execute the index scan
    const scanResult = await this.indexManager.scanIndex(
      plan.indexName,
      plan.field,
      filter
    );

    return {
      docIds: scanResult.docIds,
      exact: scanResult.exact && !plan.residualFilter,
      plan,
    };
  }

  // --------------------------------------------------------------------------
  // Plan Analysis Utilities
  // --------------------------------------------------------------------------

  /**
   * Check if a filter can use an index.
   *
   * Convenience method for quick checks without full plan creation.
   */
  async canUseIndex(filter: Filter<Document>): Promise<boolean> {
    const plan = await this.indexManager.analyzeQuery(filter);
    return plan.useIndex;
  }

  /**
   * Get the best index for a filter, if any.
   *
   * Returns the index name or null if no index can be used.
   */
  async getBestIndex(filter: Filter<Document>): Promise<string | null> {
    const plan = await this.indexManager.analyzeQuery(filter);
    return plan.indexName || null;
  }

  /**
   * Explain a query plan in human-readable format.
   *
   * Useful for debugging and query optimization.
   */
  async explain(collection: string, filter: Filter<Document>): Promise<string> {
    const plan = await this.createPlan(collection, filter);

    const lines: string[] = [
      `Query Plan for collection '${collection}':`,
      `  Strategy: ${plan.strategy}`,
    ];

    if (plan.indexName) {
      lines.push(`  Index: ${plan.indexName}`);
      lines.push(`  Field: ${plan.field}`);
      lines.push(`  Operation: ${plan.operation}`);
    }

    lines.push(`  Estimated Selectivity: ${(plan.estimatedSelectivity * 100).toFixed(1)}%`);

    if (plan.residualFilter) {
      lines.push(`  Residual Filter: ${JSON.stringify(plan.residualFilter)}`);
    }

    lines.push(`  Explanation: ${plan.explanation}`);

    return lines.join('\n');
  }
}

// ============================================================================
// Exports
// ============================================================================

export default QueryPlanner;
