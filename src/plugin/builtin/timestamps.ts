/**
 * Timestamps Plugin
 *
 * Automatically adds createdAt and updatedAt timestamps to documents.
 * This is similar to Mongoose's timestamps option but works at the
 * plugin level for all collections.
 *
 * @example
 * ```typescript
 * import { timestampsPlugin } from 'mongolake/plugin/builtin';
 *
 * // Use with default field names
 * registry.register(timestampsPlugin);
 *
 * // Or with custom field names
 * registry.register(timestampsPlugin, {
 *   createdAt: '_createdAt',
 *   updatedAt: '_modifiedAt',
 * });
 *
 * // Documents will automatically have timestamps:
 * // { name: 'Alice', createdAt: Date, updatedAt: Date }
 * ```
 */

import type { Document, Update, Filter } from '../../types.js';
import { definePlugin, type CollectionHookContext } from '../index.js';

/**
 * Configuration options for the timestamps plugin.
 */
export interface TimestampsPluginOptions {
  /** Field name for creation timestamp (default: 'createdAt') */
  createdAt?: string | false;
  /** Field name for update timestamp (default: 'updatedAt') */
  updatedAt?: string | false;
  /** Collections to include (if empty, all collections) */
  includeCollections?: string[];
  /** Collections to exclude */
  excludeCollections?: string[];
}

const DEFAULT_CREATED_AT = 'createdAt';
const DEFAULT_UPDATED_AT = 'updatedAt';

/**
 * Check if a collection should have timestamps applied.
 */
function shouldApplyToCollection(
  collection: string,
  options: TimestampsPluginOptions
): boolean {
  // Check exclusions first
  if (options.excludeCollections?.includes(collection)) {
    return false;
  }

  // If inclusions specified, only apply to those
  if (options.includeCollections && options.includeCollections.length > 0) {
    return options.includeCollections.includes(collection);
  }

  return true;
}

/**
 * Create a timestamps plugin with the given options.
 */
export function createTimestampsPlugin(options: TimestampsPluginOptions = {}) {
  const createdAtField = options.createdAt === false ? null : (options.createdAt ?? DEFAULT_CREATED_AT);
  const updatedAtField = options.updatedAt === false ? null : (options.updatedAt ?? DEFAULT_UPDATED_AT);

  return definePlugin({
    name: 'timestamps',
    version: '1.0.0',
    description: 'Automatically adds createdAt and updatedAt timestamps to documents',
    tags: ['builtin', 'documents'],

    hooks: {
      'collection:beforeInsert': async (docs: Document[], context: CollectionHookContext) => {
        if (!shouldApplyToCollection(context.collection, options)) {
          return docs;
        }

        const now = new Date();
        return docs.map((doc) => {
          const newDoc = { ...doc };
          if (createdAtField && !(createdAtField in newDoc)) {
            (newDoc as Record<string, unknown>)[createdAtField] = now;
          }
          if (updatedAtField && !(updatedAtField in newDoc)) {
            (newDoc as Record<string, unknown>)[updatedAtField] = now;
          }
          return newDoc;
        });
      },

      'collection:beforeUpdate': async (
        params: { filter: Filter<Document>; update: Update<Document> },
        context: CollectionHookContext
      ) => {
        if (!shouldApplyToCollection(context.collection, options)) {
          return params;
        }

        if (!updatedAtField) {
          return params;
        }

        const update = { ...params.update };
        const $set = { ...(update.$set || {}) } as Record<string, unknown>;
        $set[updatedAtField] = new Date();
        update.$set = $set as Partial<Document>;

        return { filter: params.filter, update };
      },
    },
  });
}

/**
 * Default timestamps plugin instance.
 * Uses 'createdAt' and 'updatedAt' field names.
 */
export const timestampsPlugin = createTimestampsPlugin();
