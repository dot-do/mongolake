/**
 * Auth Middleware Implementation for MongoLake
 *
 * Provides production-ready authentication middleware supporting:
 * - Bearer token validation with RS256/ES256/HS256 support
 * - JWKs endpoint fetching for key rotation
 * - Token refresh handling
 * - API key authentication
 * - Token caching with LRU eviction
 * - User context extraction from JWT
 * - OAuth 2.0 authorization code flow
 * - OAuth device flow for CLI
 * - Keychain storage for credentials
 * - Multiple auth providers
 * - Role-based access control (RBAC) with database/collection permissions
 * - Audit logging for auth events
 *
 * This module coordinates the various auth subsystems and provides
 * backwards-compatible exports for all auth functionality.
 */

import {
  DEFAULT_CACHE_MAX_SIZE,
  DEFAULT_CACHE_TTL_SECONDS,
} from '../constants.js';
import { sanitizeError } from '../utils/sanitize-error.js';

// ============================================================================
// Re-export Types
// ============================================================================

export type {
  UserContext,
  TokenInfo,
  AuthConfig,
  TokenResponse,
  ApiKeyValidation,
  AuthProvider,
  AuthError,
  AuthResult,
  PermissionLevel,
  ResourcePermission,
  RoleDefinition,
  RBACConfig,
  PermissionCheckResult,
  AuthEventType,
  AuditLogEntry,
  AuditLogger,
  JwtHeader,
  JwtClaims,
  JwtVerificationOptions,
  JwtVerificationResult,
  JWK,
  JWKSet,
  ExtendedJwtVerificationOptions,
  DeviceAuthResponse,
  AuthorizationCodeResponse,
  TokenExchangeResponse,
  KeytarLike,
  StoredTokens,
  ValidateBearerTokenResult,
} from './types.js';

// ============================================================================
// Re-export from jwt-validator
// ============================================================================

export {
  decodeBase64Url,
  base64UrlToUint8Array,
  stringToUint8Array,
  parseJwtHeader,
  parseJwt,
  importHmacKey,
  importRsaPublicKey,
  importEcdsaPublicKey,
  verifyJwt,
  extractUserContext,
} from './jwt-validator.js';

// ============================================================================
// Re-export from jwks-manager
// ============================================================================

export {
  importJwk,
  clearJWKSCache,
  getCachedJWKS,
  fetchJWKS,
} from './jwks-manager.js';

// ============================================================================
// Re-export from token-refresh
// ============================================================================

export {
  isTokenExpired,
  isTokenNearExpiry,
  validateBearerToken,
} from './token-refresh.js';

// ============================================================================
// Re-export from token-cache
// ============================================================================

export {
  TokenCache,
  createTokenCache,
} from './token-cache.js';

// ============================================================================
// Re-export from oauth-handler
// ============================================================================

export {
  DeviceFlowHandler,
  AuthorizationCodeHandler,
} from './oauth-handler.js';

// ============================================================================
// Re-export from keychain-storage
// ============================================================================

export {
  resetEncryptionKey,
  encryptTokens,
  decryptTokens,
  KeychainStorage,
} from './keychain-storage.js';

// ============================================================================
// Re-export from rbac-evaluator
// ============================================================================

export {
  comparePermissionLevels,
  maxPermissionLevel,
  RBACManager,
} from './rbac-evaluator.js';

// Keep legacy export name for backwards compatibility
export { maxPermissionLevel as _maxPermissionLevel } from './rbac-evaluator.js';

// ============================================================================
// Re-export from audit-logger
// ============================================================================

export {
  ConsoleAuditLogger,
  InMemoryAuditLogger,
  CompositeAuditLogger,
} from './audit-logger.js';

// ============================================================================
// Re-export from provider-adapters
// ============================================================================

export {
  createAuthProvider,
  registerProvider,
  getProvider,
  getAllProviders,
  clearProviders,
} from './provider-adapters/index.js';

// ============================================================================
// Import for internal use
// ============================================================================

import type {
  AuthConfig,
  AuthError,
  AuthProvider,
  AuthResult,
  AuditLogger,
  AuditLogEntry,
  PermissionCheckResult,
  PermissionLevel,
  UserContext,
} from './types.js';

import { parseJwt } from './jwt-validator.js';
import { validateBearerToken } from './token-refresh.js';
import { TokenCache } from './token-cache.js';
import { RBACManager } from './rbac-evaluator.js';

// ============================================================================
// Constants
// ============================================================================

/** Minimum API key length for format validation */
const MIN_API_KEY_LENGTH = 10;

// ============================================================================
// Assertion Functions
// ============================================================================

/**
 * Assert that a UserContext is defined.
 * Used after validation checks to narrow the type from UserContext | undefined.
 */
function assertUserContext(user: UserContext | undefined): asserts user is UserContext {
  if (!user) {
    throw new Error('Expected user context but got undefined');
  }
}

// ============================================================================
// AuthMiddleware Class
// ============================================================================

export class AuthMiddleware {
  private config: AuthConfig;
  private cache: TokenCache;
  private rbacManager?: RBACManager;
  private auditLogger?: AuditLogger;

  constructor(config: AuthConfig) {
    this.config = config;
    this.cache = new TokenCache({ maxSize: DEFAULT_CACHE_MAX_SIZE, ttlSeconds: DEFAULT_CACHE_TTL_SECONDS });

    // Initialize RBAC manager if configured
    if (config.rbac) {
      this.rbacManager = new RBACManager(config.rbac);
    }

    // Initialize audit logger
    this.auditLogger = config.auditLogger;
  }

  /**
   * Get the RBAC manager for permission checks
   */
  getRBACManager(): RBACManager | undefined {
    return this.rbacManager;
  }

  /**
   * Log an audit event
   */
  private async logAuditEvent(entry: Omit<AuditLogEntry, 'timestamp'>): Promise<void> {
    if (this.auditLogger) {
      await this.auditLogger.log({
        ...entry,
        timestamp: new Date(),
      });
    }
  }

  /**
   * Extract client IP from request headers
   */
  private getClientIP(request: Request): string | undefined {
    return request.headers.get('CF-Connecting-IP')
      ?? request.headers.get('X-Forwarded-For')?.split(',')[0]?.trim()
      ?? request.headers.get('X-Real-IP')
      ?? undefined;
  }

  /**
   * Authenticate a request by checking public paths, then validating credentials
   */
  async authenticate(request: Request): Promise<AuthResult> {
    const url = new URL(request.url);
    const ipAddress = this.getClientIP(request);
    const userAgent = request.headers.get('User-Agent') ?? undefined;

    // Skip authentication for whitelisted public paths
    if (this.config.publicPaths) {
      for (const path of this.config.publicPaths) {
        if (this.matchPath(url.pathname, path)) {
          return { authenticated: true, skipped: true };
        }
      }
    }

    // Try to extract credentials from request
    const authHeader = request.headers.get('Authorization');
    const apiKey = request.headers.get('X-API-Key');

    // Must provide either Authorization header or API key
    if (!authHeader && !apiKey) {
      await this.logAuditEvent({
        eventType: 'auth_failure',
        ipAddress,
        userAgent,
        requestPath: url.pathname,
        requestMethod: request.method,
        errorCode: 'MISSING_AUTH_HEADER',
        errorMessage: 'No authorization credentials provided',
      });
      return this.createErrorResult(
        'MISSING_AUTH_HEADER',
        'Request must include either "Authorization" header or "X-API-Key" header',
        401
      );
    }

    // Prefer Authorization header if both are present
    if (authHeader) {
      return this.authenticateWithAuthHeader(authHeader, request);
    }

    // At this point apiKey must be defined since we checked (!authHeader && !apiKey) above
    return this.validateApiKey(apiKey ?? '');
  }

  private async authenticateWithAuthHeader(authHeader: string, request: Request): Promise<AuthResult> {
    // Check for API key authentication (ApiKey scheme)
    if (authHeader.startsWith('ApiKey ')) {
      return this.validateApiKey(authHeader.slice(7));
    }

    // Check for bearer token authentication (Bearer scheme)
    if (authHeader.startsWith('Bearer ')) {
      const token = authHeader.slice(7).trim();
      if (!token) {
        return this.createErrorResult('EMPTY_TOKEN', 'Bearer token is required after "Bearer"', 401);
      }
      return this.authenticateWithBearer(token, request);
    }

    // Authorization header is present but uses an unsupported scheme
    return this.createErrorResult(
      'INVALID_AUTH_FORMAT',
      'Authorization header must use "Bearer <token>" or "ApiKey <key>" format',
      401
    );
  }

  private async authenticateWithBearer(token: string, request: Request): Promise<AuthResult> {
    const url = new URL(request.url);
    const ipAddress = this.getClientIP(request);
    const userAgent = request.headers.get('User-Agent') ?? undefined;

    // If provider is explicitly specified, use it
    const providerName = request.headers.get('X-Auth-Provider');
    if (providerName) {
      const provider = this.config.providers?.find((p) => p.name === providerName);
      if (!provider) {
        await this.logAuditEvent({
          eventType: 'auth_failure',
          ipAddress,
          userAgent,
          requestPath: url.pathname,
          requestMethod: request.method,
          errorCode: 'UNKNOWN_PROVIDER',
          errorMessage: `Unknown auth provider: ${providerName}`,
        });
        return this.createErrorResult('UNKNOWN_PROVIDER', `Unknown auth provider: ${providerName}`, 401);
      }
      return this.validateWithProvider(token, provider, request);
    }

    // Return cached validation result if available and caching is enabled
    if (this.config.cacheEnabled !== false) {
      const cached = this.cache.get(token);
      if (cached) {
        return {
          authenticated: true,
          user: cached.user,
          authMethod: 'bearer',
          fromCache: true,
          request: this.enhanceRequest(request, cached.user),
        };
      }
    }

    // Validate token with signature verification if configured
    const validation = await validateBearerToken(token, this.config);

    // Handle validation failure with special case for expired tokens
    if (!validation.valid) {
      const errorCode = validation.error?.code ?? 'UNKNOWN_ERROR';
      const errorMessage = validation.error?.message ?? 'Token validation failed';

      await this.logAuditEvent({
        eventType: 'auth_failure',
        ipAddress,
        userAgent,
        requestPath: url.pathname,
        requestMethod: request.method,
        errorCode,
        errorMessage,
      });

      if (validation.isExpired) {
        return {
          authenticated: false,
          error: validation.error,
          statusCode: 401,
          headers: {
            'WWW-Authenticate': 'Bearer realm="mongolake", error="invalid_token", error_description="Token has expired"',
          },
        };
      }
      return this.createErrorResult(errorCode, errorMessage, 401);
    }

    // Check if token issuer matches a configured provider
    try {
      const claims = parseJwt(token);
      if (claims.iss && this.config.providers) {
        const provider = this.config.providers.find((p) => p.issuer === claims.iss);
        if (provider) {
          return this.validateWithProvider(token, provider, request);
        }
      }
    } catch {
      // Parse errors are expected here in some edge cases:
      // 1. Token was validated by signature but has non-standard claims format
      // 2. Token is from a provider using a different encoding scheme
      // Since the token already passed validation above, this is safe to ignore.
      // The token will be treated as valid without provider-specific handling.
    }

    // Attempt token refresh if it's nearing expiry and refresh credentials are provided
    if (validation.isNearExpiry && this.config.tokenRefresher) {
      const refreshToken = request.headers.get('X-Refresh-Token');
      if (refreshToken) {
        try {
          assertUserContext(validation.user);
          const newTokens = await this.config.tokenRefresher(refreshToken);
          this.cacheTokenIfEnabled(newTokens.accessToken, validation.user, newTokens.expiresIn);

          await this.logAuditEvent({
            eventType: 'token_refresh',
            userId: validation.user.userId,
            ipAddress,
            userAgent,
            requestPath: url.pathname,
            requestMethod: request.method,
          });

          return {
            authenticated: true,
            user: validation.user,
            authMethod: 'bearer',
            newTokens,
            headers: {
              'X-New-Access-Token': newTokens.accessToken,
              'X-New-Refresh-Token': newTokens.refreshToken,
            },
            request: this.enhanceRequest(request, validation.user),
          };
        } catch (err) {
          // Sanitize error message to prevent credential leakage
          const sanitized = sanitizeError(err);

          await this.logAuditEvent({
            eventType: 'token_refresh_failure',
            userId: validation.user?.userId,
            ipAddress,
            userAgent,
            requestPath: url.pathname,
            requestMethod: request.method,
            errorCode: 'REFRESH_FAILED',
            errorMessage: sanitized.message,
          });

          return this.createErrorResult('REFRESH_FAILED', sanitized.message, 401);
        }
      }
    }

    // Assert user context exists after successful validation
    assertUserContext(validation.user);

    // Cache validated token for future requests
    this.cacheTokenIfEnabled(token, validation.user, null);

    // Log successful authentication
    await this.logAuditEvent({
      eventType: 'auth_success',
      userId: validation.user.userId,
      ipAddress,
      userAgent,
      requestPath: url.pathname,
      requestMethod: request.method,
    });

    return {
      authenticated: true,
      user: validation.user,
      authMethod: 'bearer',
      request: this.enhanceRequest(request, validation.user),
    };
  }

  private async validateWithProvider(
    token: string,
    provider: AuthProvider,
    request: Request
  ): Promise<AuthResult> {
    const result = await provider.validateToken(token);

    if (!result || !result.valid) {
      return this.createErrorResult('INVALID_TOKEN', 'Token validation failed', 401);
    }

    assertUserContext(result.user);
    const enhancedRequest = this.enhanceRequest(request, result.user);

    return {
      authenticated: true,
      user: result.user,
      authMethod: 'bearer',
      request: enhancedRequest,
    };
  }

  private async validateApiKey(apiKey: string): Promise<AuthResult> {
    // Ensure API key validation is configured
    if (!this.config.apiKeyValidator) {
      return this.createErrorResult(
        'API_KEY_NOT_SUPPORTED',
        'API key authentication is not enabled for this service',
        401
      );
    }

    // Validate API key format before expensive validation check
    if (apiKey.includes('-') || apiKey.length < MIN_API_KEY_LENGTH) {
      return this.createErrorResult(
        'INVALID_API_KEY_FORMAT',
        `API key must be at least ${MIN_API_KEY_LENGTH} characters and contain no hyphens`,
        401
      );
    }

    // Validate the API key with the configured validator
    const validation = await this.config.apiKeyValidator(apiKey);
    if (!validation.valid) {
      return this.createErrorResult('INVALID_API_KEY', 'The provided API key is invalid', 401);
    }

    // Check if API key has required scopes
    if (this.config.requiredScopes?.length) {
      const apiKeyScopes = validation.scopes ?? [];
      const hasAllScopes = this.config.requiredScopes.every((scope) => apiKeyScopes.includes(scope));
      if (!hasAllScopes) {
        return {
          authenticated: false,
          error: {
            code: 'INSUFFICIENT_SCOPE',
            message: `API key requires scopes: ${this.config.requiredScopes.join(', ')}`,
          },
          statusCode: 403,
        };
      }
    }

    return {
      authenticated: true,
      user: validation.user,
      authMethod: 'api_key',
    };
  }

  /**
   * Cache a validated token if caching is enabled
   */
  private cacheTokenIfEnabled(token: string, user: UserContext, refreshExpiresIn: number | null): void {
    if (this.config.cacheEnabled === false) {
      return;
    }

    try {
      const claims = parseJwt(token);
      const expiresAt = refreshExpiresIn
        ? Date.now() + refreshExpiresIn * 1000
        : claims.exp
          ? claims.exp * 1000
          : Date.now() + 3600000; // Default 1 hour if no exp claim

      this.cache.set(token, {
        token,
        user,
        expiresAt,
        validatedAt: Date.now(),
      });
    } catch {
      // Caching errors are non-fatal - authentication already succeeded.
      // Possible causes:
      // 1. Token has no 'exp' claim (will use default TTL anyway)
      // 2. Cache storage is full (LRU will handle eviction on next request)
      // We intentionally don't log here to avoid noise, since caching is
      // an optimization and failure just means the next request will re-validate.
    }
  }

  private createErrorResult(code: string, message: string, statusCode: number): AuthResult {
    const error: AuthError = { code, message };
    const result: AuthResult = {
      authenticated: false,
      error,
      statusCode,
    };

    if (this.config.errorFormatter) {
      result.errorBody = this.config.errorFormatter(error);
    }

    return result;
  }

  /**
   * Add user context headers to the request for downstream handlers
   */
  private enhanceRequest(request: Request, user: UserContext): Request {
    const headers = new Headers(request.headers);
    // Always add user ID (required)
    headers.set('X-User-Id', user.userId);
    // Add optional user details if available
    if (user.email) {
      headers.set('X-User-Email', user.email);
    }
    if (user.organizationId) {
      headers.set('X-Organization-Id', user.organizationId);
    }

    return new Request(request.url, {
      method: request.method,
      headers,
      body: request.body,
      redirect: request.redirect,
      signal: request.signal,
    });
  }

  /**
   * Check if a request path matches a whitelist pattern
   * (supports exact match and prefix match with trailing *)
   */
  private matchPath(pathname: string, pattern: string): boolean {
    if (pattern.endsWith('*')) {
      // Prefix match: /api/v1/* matches /api/v1/users, /api/v1/posts, etc.
      return pathname.startsWith(pattern.slice(0, -1));
    }
    // Exact match
    return pathname === pattern;
  }

  /**
   * Create a middleware handler function for request validation
   * Returns 401 Unauthorized if authentication fails
   */
  createHandler(): (request: Request, next: () => Promise<Response>) => Promise<Response> {
    return async (request: Request, next: () => Promise<Response>): Promise<Response> => {
      const authResult = await this.authenticate(request);

      // Return error response if authentication failed and path wasn't skipped
      if (!authResult.authenticated && !authResult.skipped) {
        const body = authResult.errorBody || authResult.error;
        return new Response(JSON.stringify(body), {
          status: authResult.statusCode ?? 401,
          headers: {
            'Content-Type': 'application/json',
            ...(authResult.headers ?? {}),
          },
        });
      }

      // Continue to next middleware/handler
      return next();
    };
  }

  /**
   * Check if a user has permission to access a database/collection
   * Logs audit events for permission checks
   */
  async checkPermission(
    user: UserContext,
    database: string,
    collection: string | undefined,
    operation: 'read' | 'write' | 'admin',
    request?: Request
  ): Promise<PermissionCheckResult> {
    // If no RBAC manager, all authenticated users have full access
    if (!this.rbacManager) {
      return { allowed: true, effectivePermission: 'admin', reason: 'RBAC not configured' };
    }

    const requiredLevel: PermissionLevel = operation;
    const result = this.rbacManager.checkPermission(user, database, collection, requiredLevel);

    // Log audit event
    const ipAddress = request ? this.getClientIP(request) : undefined;
    const userAgent = request?.headers.get('User-Agent') ?? undefined;
    const url = request ? new URL(request.url) : undefined;

    if (result.allowed) {
      await this.logAuditEvent({
        eventType: 'permission_granted',
        userId: user.userId,
        ipAddress,
        userAgent,
        requestPath: url?.pathname,
        requestMethod: request?.method,
        database,
        collection,
        operation,
        metadata: {
          effectivePermission: result.effectivePermission,
          matchedRole: result.matchedRole,
        },
      });
    } else {
      await this.logAuditEvent({
        eventType: 'permission_denied',
        userId: user.userId,
        ipAddress,
        userAgent,
        requestPath: url?.pathname,
        requestMethod: request?.method,
        database,
        collection,
        operation,
        errorMessage: result.reason,
        metadata: {
          effectivePermission: result.effectivePermission,
          requiredPermission: operation,
        },
      });
    }

    return result;
  }

  /**
   * Revoke a token (add to revocation list)
   */
  async revokeToken(token: string, user?: UserContext, request?: Request): Promise<void> {
    this.cache.revoke(token);

    const ipAddress = request ? this.getClientIP(request) : undefined;
    const userAgent = request?.headers.get('User-Agent') ?? undefined;
    const url = request ? new URL(request.url) : undefined;

    await this.logAuditEvent({
      eventType: 'token_revoked',
      userId: user?.userId,
      ipAddress,
      userAgent,
      requestPath: url?.pathname,
      requestMethod: request?.method,
    });
  }
}

// ============================================================================
// Factory Function
// ============================================================================

/**
 * Create an auth middleware instance with the given configuration
 */
export function createAuthMiddleware(config: AuthConfig): AuthMiddleware {
  return new AuthMiddleware(config);
}
