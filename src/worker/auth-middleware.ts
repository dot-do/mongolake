/**
 * OAuth Service Binding Authentication Middleware
 *
 * Provides authentication middleware for Cloudflare Workers using
 * AUTH and OAUTH service bindings for minimal latency overhead.
 *
 * Features:
 * - Bearer token extraction from Authorization header
 * - Token validation via AUTH service binding
 * - User context attachment to request
 * - Token refresh handling via OAUTH service binding
 * - Optional/configurable authentication
 */

import type { AuthServiceBinding, OAuthServiceBinding, ServiceBindingEnv } from '../types.js';

// ============================================================================
// Types
// ============================================================================

/** User context extracted from validated token */
export interface AuthUserContext {
  userId: string;
  email?: string;
  roles?: string[];
  permissions?: string[];
  organizationId?: string;
  claims?: Record<string, unknown>;
}

/** Token validation result from AUTH service */
export interface TokenValidationResult {
  valid: boolean;
  user?: AuthUserContext;
  expiresAt?: number;
  error?: string;
  errorCode?: string;
}

/** Token refresh result from OAUTH service */
export interface TokenRefreshResult {
  accessToken: string;
  refreshToken?: string;
  expiresIn: number;
  error?: string;
}

/** Authentication result */
export interface AuthResult {
  authenticated: boolean;
  user?: AuthUserContext;
  error?: string;
  errorCode?: string;
  statusCode?: number;
  newAccessToken?: string;
  newRefreshToken?: string;
}

/** Auth middleware configuration */
export interface AuthMiddlewareConfig {
  /** Enable authentication (default: true when AUTH binding exists) */
  enabled?: boolean;
  /** Paths that bypass authentication (e.g., /health, /metrics) */
  publicPaths?: string[];
  /** Enable token refresh when nearing expiry (default: true) */
  enableRefresh?: boolean;
  /** Seconds before expiry to trigger refresh (default: 300) */
  refreshThresholdSeconds?: number;
  /** Custom header for refresh token (default: X-Refresh-Token) */
  refreshTokenHeader?: string;
}

/** Default configuration values */
const DEFAULT_CONFIG: Required<AuthMiddlewareConfig> = {
  enabled: true,
  publicPaths: ['/health', '/metrics'],
  enableRefresh: true,
  refreshThresholdSeconds: 300,
  refreshTokenHeader: 'X-Refresh-Token',
};

// ============================================================================
// Service Binding Validation Helpers
// ============================================================================

/**
 * Type guard to check if AUTH service binding is available and properly configured
 */
export function hasAuthBinding(env: ServiceBindingEnv): env is ServiceBindingEnv & { AUTH: AuthServiceBinding } {
  return (
    env.AUTH !== undefined &&
    typeof env.AUTH === 'object' &&
    env.AUTH !== null &&
    typeof (env.AUTH as AuthServiceBinding).fetch === 'function'
  );
}

/**
 * Type guard to check if OAUTH service binding is available and properly configured
 */
export function hasOAuthBinding(env: ServiceBindingEnv): env is ServiceBindingEnv & { OAUTH: OAuthServiceBinding } {
  return (
    env.OAUTH !== undefined &&
    typeof env.OAUTH === 'object' &&
    env.OAUTH !== null &&
    typeof (env.OAUTH as OAuthServiceBinding).fetch === 'function'
  );
}

// ============================================================================
// Token Extraction
// ============================================================================

/**
 * Extract Bearer token from Authorization header
 *
 * @param request - The incoming request
 * @returns The token string or null if not present/malformed
 */
export function extractBearerToken(request: Request): string | null {
  const authHeader = request.headers.get('Authorization');
  if (!authHeader) {
    return null;
  }

  // Check for Bearer scheme (case-insensitive)
  if (!authHeader.toLowerCase().startsWith('bearer ')) {
    return null;
  }

  const token = authHeader.slice(7).trim();
  return token.length > 0 ? token : null;
}

/**
 * Extract refresh token from custom header
 *
 * @param request - The incoming request
 * @param headerName - The header name to extract from
 * @returns The refresh token or null if not present
 */
export function extractRefreshToken(request: Request, headerName: string = 'X-Refresh-Token'): string | null {
  const refreshToken = request.headers.get(headerName);
  return refreshToken && refreshToken.trim().length > 0 ? refreshToken.trim() : null;
}

// ============================================================================
// Service Binding Calls
// ============================================================================

/**
 * Validate token via AUTH service binding
 *
 * Uses service binding for minimal latency (in-datacenter call, no network hop).
 *
 * @param authService - AUTH service binding
 * @param token - Bearer token to validate
 * @returns Validation result with user context if valid
 */
export async function validateTokenViaBinding(
  authService: AuthServiceBinding,
  token: string
): Promise<TokenValidationResult> {
  try {
    const response = await authService.fetch(
      new Request('https://auth-service/validate', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ token }),
      })
    );

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({})) as { error?: string; code?: string };
      return {
        valid: false,
        error: errorData.error || `Token validation failed with status ${response.status}`,
        errorCode: errorData.code || 'VALIDATION_FAILED',
      };
    }

    const result = await response.json() as {
      valid: boolean;
      user?: AuthUserContext;
      expiresAt?: number;
      error?: string;
      errorCode?: string;
    };

    return {
      valid: result.valid,
      user: result.user,
      expiresAt: result.expiresAt,
      error: result.error,
      errorCode: result.errorCode,
    };
  } catch (error) {
    return {
      valid: false,
      error: error instanceof Error ? error.message : 'Token validation request failed',
      errorCode: 'SERVICE_ERROR',
    };
  }
}

/**
 * Refresh token via OAUTH service binding
 *
 * Uses service binding for minimal latency refresh operations.
 *
 * @param oauthService - OAUTH service binding
 * @param refreshToken - Refresh token to use
 * @returns New tokens if refresh successful
 */
export async function refreshTokenViaBinding(
  oauthService: OAuthServiceBinding,
  refreshToken: string
): Promise<TokenRefreshResult> {
  try {
    const response = await oauthService.fetch(
      new Request('https://oauth-service/token', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          grant_type: 'refresh_token',
          refresh_token: refreshToken,
        }),
      })
    );

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({})) as { error?: string };
      return {
        accessToken: '',
        expiresIn: 0,
        error: errorData.error || `Token refresh failed with status ${response.status}`,
      };
    }

    const result = await response.json() as {
      access_token: string;
      refresh_token?: string;
      expires_in: number;
    };

    return {
      accessToken: result.access_token,
      refreshToken: result.refresh_token,
      expiresIn: result.expires_in,
    };
  } catch (error) {
    return {
      accessToken: '',
      expiresIn: 0,
      error: error instanceof Error ? error.message : 'Token refresh request failed',
    };
  }
}

// ============================================================================
// Path Matching
// ============================================================================

/**
 * Check if a path matches any of the public path patterns
 *
 * Supports:
 * - Exact match: /health matches only /health
 * - Prefix match: /api/public/* matches /api/public/anything
 *
 * @param pathname - Request pathname to check
 * @param publicPaths - List of public path patterns
 * @returns True if path is public
 */
export function isPublicPath(pathname: string, publicPaths: string[]): boolean {
  for (const pattern of publicPaths) {
    if (pattern.endsWith('*')) {
      // Prefix match
      const prefix = pattern.slice(0, -1);
      if (pathname.startsWith(prefix)) {
        return true;
      }
    } else {
      // Exact match
      if (pathname === pattern) {
        return true;
      }
    }
  }
  return false;
}

// ============================================================================
// Auth Middleware Class
// ============================================================================

/**
 * Authentication middleware using service bindings
 *
 * Provides low-latency authentication by leveraging Cloudflare service bindings
 * instead of external HTTP calls.
 */
export class ServiceBindingAuthMiddleware {
  private config: Required<AuthMiddlewareConfig>;

  constructor(config: AuthMiddlewareConfig = {}) {
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  /**
   * Authenticate a request using service bindings
   *
   * @param request - The incoming request
   * @param env - Environment with service bindings
   * @returns Authentication result
   */
  async authenticate(request: Request, env: ServiceBindingEnv): Promise<AuthResult> {
    // Check if auth is enabled and binding exists
    if (!this.config.enabled || !hasAuthBinding(env)) {
      // Auth disabled or not configured - allow request
      return { authenticated: true };
    }

    const url = new URL(request.url);

    // Check for public paths
    if (isPublicPath(url.pathname, this.config.publicPaths)) {
      return { authenticated: true };
    }

    // Extract Bearer token
    const token = extractBearerToken(request);
    if (!token) {
      return {
        authenticated: false,
        error: 'Authorization header with Bearer token required',
        errorCode: 'MISSING_TOKEN',
        statusCode: 401,
      };
    }

    // Validate token via AUTH service binding
    const validationResult = await validateTokenViaBinding(env.AUTH, token);

    if (!validationResult.valid) {
      return {
        authenticated: false,
        error: validationResult.error || 'Token validation failed',
        errorCode: validationResult.errorCode || 'INVALID_TOKEN',
        statusCode: 401,
      };
    }

    // Check if token needs refresh
    if (
      this.config.enableRefresh &&
      hasOAuthBinding(env) &&
      validationResult.expiresAt
    ) {
      const now = Math.floor(Date.now() / 1000);
      const secondsUntilExpiry = validationResult.expiresAt - now;

      if (secondsUntilExpiry <= this.config.refreshThresholdSeconds) {
        const refreshToken = extractRefreshToken(request, this.config.refreshTokenHeader);

        if (refreshToken) {
          const refreshResult = await refreshTokenViaBinding(env.OAUTH, refreshToken);

          if (refreshResult.accessToken) {
            return {
              authenticated: true,
              user: validationResult.user,
              newAccessToken: refreshResult.accessToken,
              newRefreshToken: refreshResult.refreshToken,
            };
          }
          // Refresh failed but original token still valid - continue
        }
      }
    }

    return {
      authenticated: true,
      user: validationResult.user,
    };
  }

  /**
   * Create a middleware handler function
   *
   * Returns a function that can be used in middleware chains.
   *
   * @returns Middleware handler function
   */
  createHandler(): (
    request: Request,
    env: ServiceBindingEnv,
    next: () => Promise<Response>
  ) => Promise<Response> {
    return async (
      request: Request,
      env: ServiceBindingEnv,
      next: () => Promise<Response>
    ): Promise<Response> => {
      const authResult = await this.authenticate(request, env);

      if (!authResult.authenticated) {
        const headers: Record<string, string> = {
          'Content-Type': 'application/json',
          'WWW-Authenticate': `Bearer realm="mongolake"${authResult.errorCode ? `, error="${authResult.errorCode}"` : ''}`,
        };

        return new Response(
          JSON.stringify({
            error: authResult.error,
            code: authResult.errorCode,
          }),
          {
            status: authResult.statusCode || 401,
            headers,
          }
        );
      }

      // Call next handler
      const response = await next();

      // If tokens were refreshed, add them to response headers
      if (authResult.newAccessToken) {
        const newHeaders = new Headers(response.headers);
        newHeaders.set('X-New-Access-Token', authResult.newAccessToken);
        if (authResult.newRefreshToken) {
          newHeaders.set('X-New-Refresh-Token', authResult.newRefreshToken);
        }

        return new Response(response.body, {
          status: response.status,
          statusText: response.statusText,
          headers: newHeaders,
        });
      }

      return response;
    };
  }

  /**
   * Get user context from a validated request
   *
   * Call this after authenticate() returns authenticated: true
   *
   * @param authResult - Result from authenticate()
   * @returns User context or undefined
   */
  getUserContext(authResult: AuthResult): AuthUserContext | undefined {
    return authResult.user;
  }
}

// ============================================================================
// Factory Functions
// ============================================================================

/**
 * Create a service binding auth middleware instance
 *
 * @param config - Middleware configuration
 * @returns Configured middleware instance
 */
export function createServiceBindingAuthMiddleware(
  config: AuthMiddlewareConfig = {}
): ServiceBindingAuthMiddleware {
  return new ServiceBindingAuthMiddleware(config);
}

/**
 * Quick authentication check using service bindings
 *
 * Convenience function for simple auth checks without creating a middleware instance.
 *
 * @param request - The incoming request
 * @param env - Environment with service bindings
 * @param options - Optional configuration
 * @returns Authentication result
 */
export async function authenticateRequest(
  request: Request,
  env: ServiceBindingEnv,
  options: AuthMiddlewareConfig = {}
): Promise<AuthResult> {
  const middleware = new ServiceBindingAuthMiddleware(options);
  return middleware.authenticate(request, env);
}

// ============================================================================
// Request Enhancement
// ============================================================================

/**
 * Enhance a request with user context headers
 *
 * Adds user information as headers for downstream handlers.
 *
 * @param request - Original request
 * @param user - User context to attach
 * @returns New request with user headers
 */
export function enhanceRequestWithUser(request: Request, user: AuthUserContext): Request {
  const headers = new Headers(request.headers);

  headers.set('X-User-Id', user.userId);
  if (user.email) {
    headers.set('X-User-Email', user.email);
  }
  if (user.organizationId) {
    headers.set('X-Organization-Id', user.organizationId);
  }
  if (user.roles && user.roles.length > 0) {
    headers.set('X-User-Roles', user.roles.join(','));
  }
  if (user.permissions && user.permissions.length > 0) {
    headers.set('X-User-Permissions', user.permissions.join(','));
  }

  return new Request(request.url, {
    method: request.method,
    headers,
    body: request.body,
    redirect: request.redirect,
  });
}
