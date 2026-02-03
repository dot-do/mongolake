/**
 * Collection Services
 *
 * Service classes for Collection composition pattern.
 * These services encapsulate specific functionality areas
 * to reduce complexity in the main Collection class.
 */

export { IndexService } from './index-service.js';
export { ChangeStreamService, type ChangeStreamNamespace } from './change-stream-service.js';
export { ValidationService, type BatchValidationOptions } from './validation-service.js';
export { AuditService, type CorruptionContext } from './audit-service.js';
