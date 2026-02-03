/**
 * Auth Module for MongoLake
 *
 * Provides production-ready authentication and authorization.
 *
 * This module exports all auth functionality from focused submodules:
 * - types.ts - Type definitions
 * - jwt-validator.ts - JWT token validation and decoding
 * - jwks-manager.ts - JWKS endpoint fetching and caching
 * - oauth-handler.ts - OAuth flows (device, authorization code)
 * - token-refresh.ts - Token refresh logic
 * - token-cache.ts - Token caching with revocation
 * - rbac-evaluator.ts - Role-based access control
 * - audit-logger.ts - Audit logging implementations
 * - keychain-storage.ts - Secure token storage
 * - provider-adapters/ - Auth provider adapters
 * - middleware.ts - Main auth middleware (coordinator)
 */

// Re-export everything from middleware (which re-exports from submodules)
export * from './middleware.js';
