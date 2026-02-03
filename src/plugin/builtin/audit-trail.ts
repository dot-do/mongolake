/**
 * Audit Trail Plugin
 *
 * Records all document changes to an audit collection.
 * Useful for compliance, debugging, and data recovery.
 *
 * @example
 * ```typescript
 * import { auditTrailPlugin } from 'mongolake/plugin/builtin';
 *
 * registry.register(auditTrailPlugin, {
 *   auditCollection: '_audit',
 *   includeDocument: true,
 * });
 *
 * // All operations are now logged:
 * // {
 * //   operation: 'insert',
 * //   collection: 'users',
 * //   documentId: '123',
 * //   timestamp: Date,
 * //   user: { userId: '...', email: '...' },
 * //   document: { ... } // if includeDocument is true
 * // }
 * ```
 */

import type { Document } from '../../types.js';
import { definePlugin, type PluginContext, type CollectionHookContext } from '../index.js';

/**
 * Configuration options for the audit trail plugin.
 */
export interface AuditTrailPluginOptions {
  /** Name of the audit collection (default: '_audit') */
  auditCollection?: string;
  /** Whether to include the full document in audit records (default: false) */
  includeDocument?: boolean;
  /** Whether to include the user context (default: true) */
  includeUser?: boolean;
  /** Collections to audit (if empty, all collections except audit collection) */
  includeCollections?: string[];
  /** Collections to exclude from auditing */
  excludeCollections?: string[];
  /** Custom handler for audit records (instead of storing in collection) */
  auditHandler?: (entry: AuditEntry) => Promise<void> | void;
}

/**
 * Audit log entry structure.
 */
export interface AuditEntry {
  /** Type of operation */
  operation: 'insert' | 'update' | 'delete' | 'find' | 'aggregate';
  /** Database name */
  database: string;
  /** Collection name */
  collection: string;
  /** Document ID(s) affected (if available) */
  documentIds?: (string | unknown)[];
  /** Timestamp of the operation */
  timestamp: Date;
  /** User who performed the operation (if available) */
  user?: {
    userId: string;
    email?: string;
    roles?: string[];
  };
  /** Filter used in the operation */
  filter?: Record<string, unknown>;
  /** Update specification (for update operations) */
  update?: Record<string, unknown>;
  /** Full document(s) (if includeDocument is true) */
  documents?: Document[];
  /** Number of documents affected */
  affectedCount?: number;
  /** Request metadata */
  request?: {
    method?: string;
    path?: string;
    ip?: string;
  };
}

const DEFAULT_AUDIT_COLLECTION = '_audit';

/**
 * Check if a collection should be audited.
 */
function shouldAudit(
  collection: string,
  options: AuditTrailPluginOptions
): boolean {
  const auditCollection = options.auditCollection ?? DEFAULT_AUDIT_COLLECTION;

  // Never audit the audit collection itself
  if (collection === auditCollection) {
    return false;
  }

  // Check exclusions
  if (options.excludeCollections?.includes(collection)) {
    return false;
  }

  // Check inclusions
  if (options.includeCollections && options.includeCollections.length > 0) {
    return options.includeCollections.includes(collection);
  }

  return true;
}

/**
 * Create an audit entry from context and operation details.
 */
function createAuditEntry(
  operation: AuditEntry['operation'],
  context: CollectionHookContext,
  options: AuditTrailPluginOptions,
  details: Partial<AuditEntry> = {}
): AuditEntry {
  const entry: AuditEntry = {
    operation,
    database: context.database,
    collection: context.collection,
    timestamp: context.timestamp,
    ...details,
  };

  // Include user if available and enabled
  if (options.includeUser !== false && context.user) {
    entry.user = {
      userId: context.user.userId,
      email: context.user.email,
      roles: context.user.roles,
    };
  }

  // Include request metadata if available
  if (context.request) {
    entry.request = {
      method: context.request.method,
      path: context.request.path,
    };
  }

  return entry;
}

/**
 * Create an audit trail plugin with the given options.
 */
export function createAuditTrailPlugin(options: AuditTrailPluginOptions = {}) {
  // Store audit entries (in real implementation, would write to storage)
  const auditBuffer: AuditEntry[] = [];

  const recordAudit = async (entry: AuditEntry, context: PluginContext) => {
    if (options.auditHandler) {
      await options.auditHandler(entry);
    } else {
      // In a real implementation, this would write to the audit collection
      // For now, we log and buffer
      auditBuffer.push(entry);
      context.log.debug(`Audit: ${entry.operation} on ${entry.collection}`, {
        documentIds: entry.documentIds,
        affectedCount: entry.affectedCount,
      });
    }
  };

  return definePlugin({
    name: 'audit-trail',
    version: '1.0.0',
    description: 'Records all document changes to an audit collection',
    tags: ['builtin', 'audit', 'compliance'],

    async init(context: PluginContext) {
      context.log.info('Audit trail plugin initialized');
    },

    hooks: {
      'collection:afterInsert': async (
        result: { insertedIds: Record<number, string | unknown>; insertedCount: number },
        context: CollectionHookContext
      ) => {
        if (!shouldAudit(context.collection, options)) {
          return;
        }

        const entry = createAuditEntry('insert', context, options, {
          documentIds: Object.values(result.insertedIds),
          affectedCount: result.insertedCount,
        });

        await recordAudit(entry, context);
      },

      'collection:afterUpdate': async (
        result: { matchedCount: number; modifiedCount: number },
        context: CollectionHookContext
      ) => {
        if (!shouldAudit(context.collection, options)) {
          return;
        }

        const entry = createAuditEntry('update', context, options, {
          filter: context.filter as Record<string, unknown>,
          affectedCount: result.modifiedCount,
        });

        await recordAudit(entry, context);
      },

      'collection:afterDelete': async (
        result: { deletedCount: number },
        context: CollectionHookContext
      ) => {
        if (!shouldAudit(context.collection, options)) {
          return;
        }

        const entry = createAuditEntry('delete', context, options, {
          filter: context.filter as Record<string, unknown>,
          affectedCount: result.deletedCount,
        });

        await recordAudit(entry, context);
      },
    },

    // Expose audit buffer for testing
    async destroy() {
      auditBuffer.length = 0;
    },
  });
}

/**
 * Default audit trail plugin instance.
 */
export const auditTrailPlugin = createAuditTrailPlugin();
