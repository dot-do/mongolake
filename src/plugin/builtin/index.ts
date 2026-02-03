/**
 * Built-in Plugins
 *
 * MongoLake provides several built-in plugins for common functionality.
 * These can be used as-is or as examples for creating custom plugins.
 */

export { timestampsPlugin, type TimestampsPluginOptions } from './timestamps.js';
export { softDeletePlugin, type SoftDeletePluginOptions } from './soft-delete.js';
export { auditTrailPlugin, type AuditTrailPluginOptions } from './audit-trail.js';
export { validationPlugin, type ValidationPluginOptions } from './validation.js';
