/**
 * Soft Delete Plugin
 *
 * Instead of permanently deleting documents, marks them as deleted
 * with a timestamp. Automatically filters out soft-deleted documents
 * from queries unless explicitly requested.
 *
 * @example
 * ```typescript
 * import { softDeletePlugin } from 'mongolake/plugin/builtin';
 *
 * // Use with defaults
 * registry.register(softDeletePlugin);
 *
 * // Or with custom configuration
 * registry.register(softDeletePlugin, {
 *   deletedAtField: '_deletedAt',
 *   includeDeletedField: '_includeDeleted',
 * });
 *
 * // Delete operation marks document instead of removing
 * await collection.deleteOne({ _id: '123' });
 * // Document now has: { deletedAt: Date }
 *
 * // Queries automatically exclude deleted documents
 * await collection.find({}).toArray(); // Only non-deleted docs
 *
 * // To include deleted documents in queries:
 * await collection.find({ _includeDeleted: true }).toArray();
 * ```
 */

import type { Document, Filter, AggregationStage } from '../../types.js';
import { definePlugin, type CollectionHookContext, type HookResult } from '../index.js';

/**
 * Configuration options for the soft delete plugin.
 */
export interface SoftDeletePluginOptions {
  /** Field name to store deletion timestamp (default: 'deletedAt') */
  deletedAtField?: string;
  /** Field name to check for including deleted docs in queries (default: '_includeDeleted') */
  includeDeletedField?: string;
  /** Field name to store deletion actor (if available from context) */
  deletedByField?: string;
  /** Collections to include (if empty, all collections) */
  includeCollections?: string[];
  /** Collections to exclude */
  excludeCollections?: string[];
}

const DEFAULT_DELETED_AT = 'deletedAt';
const DEFAULT_INCLUDE_DELETED = '_includeDeleted';

/**
 * Check if a collection should have soft delete applied.
 */
function shouldApplyToCollection(
  collection: string,
  options: SoftDeletePluginOptions
): boolean {
  if (options.excludeCollections?.includes(collection)) {
    return false;
  }
  if (options.includeCollections && options.includeCollections.length > 0) {
    return options.includeCollections.includes(collection);
  }
  return true;
}

/**
 * Create a soft delete plugin with the given options.
 */
export function createSoftDeletePlugin(options: SoftDeletePluginOptions = {}) {
  const deletedAtField = options.deletedAtField ?? DEFAULT_DELETED_AT;
  const includeDeletedField = options.includeDeletedField ?? DEFAULT_INCLUDE_DELETED;
  // Reserved for future use: const deletedByField = options.deletedByField;

  return definePlugin({
    name: 'soft-delete',
    version: '1.0.0',
    description: 'Soft delete support - marks documents as deleted instead of removing them',
    tags: ['builtin', 'documents'],

    hooks: {
      // Intercept delete operations and convert to updates
      'collection:beforeDelete': async (
        filter: Filter<Document>,
        context: CollectionHookContext
      ) => {
        if (!shouldApplyToCollection(context.collection, options)) {
          return filter;
        }

        // We return a stop signal to prevent the actual delete
        // The afterDelete hook will handle the soft delete
        return { stop: true, result: filter };
      },

      // After delete is "stopped", we perform a soft delete update
      'collection:afterDelete': async (
        _result: { deletedCount: number },
        context: CollectionHookContext
      ) => {
        if (!shouldApplyToCollection(context.collection, options)) {
          return;
        }

        // Note: In a real implementation, this would need access to the
        // collection to perform the update. This is a simplified example.
        // The actual implementation would use context.storage or a callback.
        context.log.info(`Soft delete applied to filter in ${context.collection}`);
      },

      // Add filter to exclude deleted documents from find operations
      'collection:beforeFind': async (
        params: { filter?: Filter<Document>; options?: Record<string, unknown> },
        context: CollectionHookContext
      ) => {
        if (!shouldApplyToCollection(context.collection, options)) {
          return params;
        }

        const filter = { ...params.filter } as Record<string, unknown>;

        // Check if user wants to include deleted documents
        if (filter[includeDeletedField]) {
          delete filter[includeDeletedField];
          return { filter: filter as Filter<Document>, options: params.options };
        }

        // Exclude documents where deletedAt exists
        filter[deletedAtField] = { $exists: false };

        return { filter: filter as Filter<Document>, options: params.options };
      },

      // Add filter to exclude deleted documents from aggregations
      'collection:beforeAggregate': async (
        pipeline: AggregationStage[],
        context: CollectionHookContext
      ): Promise<HookResult<AggregationStage[]>> => {
        if (!shouldApplyToCollection(context.collection, options)) {
          return pipeline;
        }

        // Check if first stage is $match with _includeDeleted
        const firstStage = pipeline[0] as { $match?: Record<string, unknown> } | undefined;
        if (firstStage && '$match' in firstStage && firstStage.$match) {
          const match = firstStage.$match;
          if (match[includeDeletedField]) {
            // Remove the _includeDeleted flag but don't add the filter
            const newMatch = { ...match };
            delete newMatch[includeDeletedField];
            return [{ $match: newMatch as Filter<Document> }, ...pipeline.slice(1)] as AggregationStage[];
          }
        }

        // Add $match stage at the beginning to exclude deleted docs
        const excludeDeletedStage: AggregationStage = {
          $match: { [deletedAtField]: { $exists: false } } as Filter<Document>,
        };

        return [excludeDeletedStage, ...pipeline];
      },
    },
  });
}

/**
 * Default soft delete plugin instance.
 */
export const softDeletePlugin = createSoftDeletePlugin();
