/**
 * Service Binding Auth Middleware Tests
 *
 * Tests for the MongoLake Worker authentication middleware using
 * AUTH and OAUTH service bindings for minimal latency overhead.
 *
 * Features tested:
 * - Bearer token extraction from Authorization header
 * - Token validation via AUTH service binding
 * - User context attachment to request
 * - Token refresh handling via OAUTH binding
 * - Public path bypassing
 * - Configurable middleware behavior
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  ServiceBindingAuthMiddleware,
  createServiceBindingAuthMiddleware,
  authenticateRequest,
  extractBearerToken,
  extractRefreshToken,
  validateTokenViaBinding,
  refreshTokenViaBinding,
  hasAuthBinding,
  hasOAuthBinding,
  isPublicPath,
  enhanceRequestWithUser,
  type AuthResult,
  type AuthUserContext,
  type AuthMiddlewareConfig,
  type TokenValidationResult,
  type TokenRefreshResult,
} from '../../../src/worker/auth-middleware.js';
import type { AuthServiceBinding, OAuthServiceBinding, ServiceBindingEnv } from '../../../src/types.js';

// ============================================================================
// Test Fixtures
// ============================================================================

const mockUserContext: AuthUserContext = {
  userId: 'user_123',
  email: 'test@example.com',
  roles: ['user', 'admin'],
  permissions: ['read', 'write'],
  organizationId: 'org_456',
  claims: { tier: 'pro' },
};

const mockValidToken = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.valid-token-payload';
const mockExpiredToken = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.expired-token-payload';
const mockRefreshToken = 'refresh_token_xyz789';

/**
 * Create a mock AUTH service binding
 */
const createMockAuthService = (
  overrides?: Partial<{ validateResponse: TokenValidationResult; shouldFail?: boolean; statusCode?: number }>
): AuthServiceBinding => ({
  fetch: vi.fn().mockImplementation(async (request: Request) => {
    if (overrides?.shouldFail) {
      return new Response(JSON.stringify({ error: 'Service unavailable' }), {
        status: overrides.statusCode || 500,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    const body = await request.json() as { token: string };

    // Default success response
    const response: TokenValidationResult = overrides?.validateResponse || {
      valid: true,
      user: mockUserContext,
      expiresAt: Math.floor(Date.now() / 1000) + 3600,
    };

    // Simulate invalid token detection
    // Note: The implementation uses 'code' field from response, not 'errorCode'
    if (body.token === 'invalid-token') {
      return new Response(JSON.stringify({ valid: false, error: 'Invalid token', code: 'INVALID_TOKEN' }), {
        status: 401,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    if (body.token === mockExpiredToken) {
      return new Response(JSON.stringify({ valid: false, error: 'Token expired', code: 'TOKEN_EXPIRED' }), {
        status: 401,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    return new Response(JSON.stringify(response), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    });
  }),
});

/**
 * Create a mock OAUTH service binding
 */
const createMockOAuthService = (
  overrides?: Partial<{ refreshResponse: TokenRefreshResult; shouldFail?: boolean }>
): OAuthServiceBinding => ({
  fetch: vi.fn().mockImplementation(async () => {
    if (overrides?.shouldFail) {
      return new Response(JSON.stringify({ error: 'Refresh failed' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    const response: { access_token: string; refresh_token?: string; expires_in: number } = {
      access_token: overrides?.refreshResponse?.accessToken || 'new_access_token',
      refresh_token: overrides?.refreshResponse?.refreshToken || 'new_refresh_token',
      expires_in: overrides?.refreshResponse?.expiresIn || 3600,
    };

    return new Response(JSON.stringify(response), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    });
  }),
});

/**
 * Create a mock environment with service bindings
 */
const createMockEnv = (options?: {
  withAuth?: boolean;
  withOAuth?: boolean;
  authOverrides?: Parameters<typeof createMockAuthService>[0];
  oauthOverrides?: Parameters<typeof createMockOAuthService>[0];
}): ServiceBindingEnv => {
  const env: ServiceBindingEnv = {};

  if (options?.withAuth !== false) {
    env.AUTH = createMockAuthService(options?.authOverrides);
  }

  if (options?.withOAuth) {
    env.OAUTH = createMockOAuthService(options?.oauthOverrides);
  }

  return env;
};

/**
 * Create a mock request with optional headers
 */
const createMockRequest = (path: string, headers?: Record<string, string>): Request => {
  return new Request(`https://mongolake.workers.dev${path}`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
      ...headers,
    },
  });
};

// ============================================================================
// Token Extraction Tests
// ============================================================================

describe('Token Extraction', () => {
  describe('extractBearerToken', () => {
    it('should extract Bearer token from Authorization header', () => {
      const request = createMockRequest('/api/db/collection', {
        Authorization: `Bearer ${mockValidToken}`,
      });

      const token = extractBearerToken(request);

      expect(token).toBe(mockValidToken);
    });

    it('should return null when Authorization header is missing', () => {
      const request = createMockRequest('/api/db/collection');

      const token = extractBearerToken(request);

      expect(token).toBeNull();
    });

    it('should return null for non-Bearer authorization', () => {
      const request = createMockRequest('/api/db/collection', {
        Authorization: 'Basic dXNlcjpwYXNz',
      });

      const token = extractBearerToken(request);

      expect(token).toBeNull();
    });

    it('should return null for empty Bearer token', () => {
      const request = createMockRequest('/api/db/collection', {
        Authorization: 'Bearer ',
      });

      const token = extractBearerToken(request);

      expect(token).toBeNull();
    });

    it('should handle Bearer scheme case-insensitively', () => {
      const request = createMockRequest('/api/db/collection', {
        Authorization: `BEARER ${mockValidToken}`,
      });

      const token = extractBearerToken(request);

      expect(token).toBe(mockValidToken);
    });

    it('should trim whitespace from token', () => {
      const request = createMockRequest('/api/db/collection', {
        Authorization: `Bearer   ${mockValidToken}   `,
      });

      const token = extractBearerToken(request);

      expect(token).toBe(mockValidToken);
    });
  });

  describe('extractRefreshToken', () => {
    it('should extract refresh token from default header', () => {
      const request = createMockRequest('/api/db/collection', {
        'X-Refresh-Token': mockRefreshToken,
      });

      const token = extractRefreshToken(request);

      expect(token).toBe(mockRefreshToken);
    });

    it('should extract refresh token from custom header', () => {
      const request = createMockRequest('/api/db/collection', {
        'X-Custom-Refresh': mockRefreshToken,
      });

      const token = extractRefreshToken(request, 'X-Custom-Refresh');

      expect(token).toBe(mockRefreshToken);
    });

    it('should return null when header is missing', () => {
      const request = createMockRequest('/api/db/collection');

      const token = extractRefreshToken(request);

      expect(token).toBeNull();
    });

    it('should return null for empty refresh token', () => {
      const request = createMockRequest('/api/db/collection', {
        'X-Refresh-Token': '   ',
      });

      const token = extractRefreshToken(request);

      expect(token).toBeNull();
    });
  });
});

// ============================================================================
// Service Binding Type Guards Tests
// ============================================================================

describe('Service Binding Type Guards', () => {
  describe('hasAuthBinding', () => {
    it('should return true when AUTH binding exists with fetch method', () => {
      const env = createMockEnv({ withAuth: true });

      expect(hasAuthBinding(env)).toBe(true);
    });

    it('should return false when AUTH binding is undefined', () => {
      const env = createMockEnv({ withAuth: false });

      expect(hasAuthBinding(env)).toBe(false);
    });

    it('should return false when AUTH binding is null', () => {
      const env: ServiceBindingEnv = { AUTH: null as unknown as AuthServiceBinding };

      expect(hasAuthBinding(env)).toBe(false);
    });

    it('should return false when AUTH binding lacks fetch method', () => {
      const env: ServiceBindingEnv = { AUTH: {} as AuthServiceBinding };

      expect(hasAuthBinding(env)).toBe(false);
    });
  });

  describe('hasOAuthBinding', () => {
    it('should return true when OAUTH binding exists with fetch method', () => {
      const env = createMockEnv({ withAuth: false, withOAuth: true });

      expect(hasOAuthBinding(env)).toBe(true);
    });

    it('should return false when OAUTH binding is undefined', () => {
      const env = createMockEnv({ withAuth: true, withOAuth: false });

      expect(hasOAuthBinding(env)).toBe(false);
    });

    it('should return false when OAUTH binding lacks fetch method', () => {
      const env: ServiceBindingEnv = { OAUTH: {} as OAuthServiceBinding };

      expect(hasOAuthBinding(env)).toBe(false);
    });
  });
});

// ============================================================================
// Token Validation via Service Binding Tests
// ============================================================================

describe('validateTokenViaBinding', () => {
  it('should return valid result for valid token', async () => {
    const authService = createMockAuthService();

    const result = await validateTokenViaBinding(authService, mockValidToken);

    expect(result.valid).toBe(true);
    expect(result.user).toBeDefined();
    expect(result.user?.userId).toBe('user_123');
    expect(result.expiresAt).toBeDefined();
  });

  it('should call AUTH service with correct request format', async () => {
    const authService = createMockAuthService();

    await validateTokenViaBinding(authService, mockValidToken);

    expect(authService.fetch).toHaveBeenCalledWith(
      expect.objectContaining({
        method: 'POST',
        url: 'https://auth-service/validate',
      })
    );
  });

  it('should return invalid result for invalid token', async () => {
    const authService = createMockAuthService();

    const result = await validateTokenViaBinding(authService, 'invalid-token');

    expect(result.valid).toBe(false);
    expect(result.error).toBeDefined();
    expect(result.errorCode).toBe('INVALID_TOKEN');
  });

  it('should return invalid result for expired token', async () => {
    const authService = createMockAuthService();

    const result = await validateTokenViaBinding(authService, mockExpiredToken);

    expect(result.valid).toBe(false);
    expect(result.error).toContain('expired');
    expect(result.errorCode).toBe('TOKEN_EXPIRED');
  });

  it('should handle service errors gracefully', async () => {
    const authService = createMockAuthService({ shouldFail: true });

    const result = await validateTokenViaBinding(authService, mockValidToken);

    expect(result.valid).toBe(false);
    expect(result.error).toBeDefined();
    expect(result.errorCode).toBe('VALIDATION_FAILED');
  });

  it('should handle network errors gracefully', async () => {
    const authService: AuthServiceBinding = {
      fetch: vi.fn().mockRejectedValue(new Error('Network error')),
    };

    const result = await validateTokenViaBinding(authService, mockValidToken);

    expect(result.valid).toBe(false);
    expect(result.error).toContain('Network error');
    expect(result.errorCode).toBe('SERVICE_ERROR');
  });

  it('should include user context from validation response', async () => {
    const customUser: AuthUserContext = {
      userId: 'custom_user',
      email: 'custom@example.com',
      roles: ['superadmin'],
    };
    const authService = createMockAuthService({
      validateResponse: { valid: true, user: customUser, expiresAt: Date.now() + 3600 },
    });

    const result = await validateTokenViaBinding(authService, mockValidToken);

    expect(result.valid).toBe(true);
    expect(result.user?.userId).toBe('custom_user');
    expect(result.user?.roles).toContain('superadmin');
  });
});

// ============================================================================
// Token Refresh via Service Binding Tests
// ============================================================================

describe('refreshTokenViaBinding', () => {
  it('should return new tokens on successful refresh', async () => {
    const oauthService = createMockOAuthService();

    const result = await refreshTokenViaBinding(oauthService, mockRefreshToken);

    expect(result.accessToken).toBe('new_access_token');
    expect(result.refreshToken).toBe('new_refresh_token');
    expect(result.expiresIn).toBe(3600);
  });

  it('should call OAUTH service with correct grant type', async () => {
    const oauthService = createMockOAuthService();

    await refreshTokenViaBinding(oauthService, mockRefreshToken);

    const fetchCall = (oauthService.fetch as ReturnType<typeof vi.fn>).mock.calls[0][0] as Request;
    const body = await fetchCall.clone().json() as { grant_type: string; refresh_token: string };

    expect(body.grant_type).toBe('refresh_token');
    expect(body.refresh_token).toBe(mockRefreshToken);
  });

  it('should handle refresh failure', async () => {
    const oauthService = createMockOAuthService({ shouldFail: true });

    const result = await refreshTokenViaBinding(oauthService, mockRefreshToken);

    expect(result.accessToken).toBe('');
    expect(result.error).toBeDefined();
  });

  it('should handle network errors gracefully', async () => {
    const oauthService: OAuthServiceBinding = {
      fetch: vi.fn().mockRejectedValue(new Error('Network error')),
    };

    const result = await refreshTokenViaBinding(oauthService, mockRefreshToken);

    expect(result.accessToken).toBe('');
    expect(result.error).toContain('Network error');
  });
});

// ============================================================================
// Public Path Matching Tests
// ============================================================================

describe('isPublicPath', () => {
  const publicPaths = ['/health', '/metrics', '/api/public/*', '/v1/docs'];

  it('should match exact public paths', () => {
    expect(isPublicPath('/health', publicPaths)).toBe(true);
    expect(isPublicPath('/metrics', publicPaths)).toBe(true);
    expect(isPublicPath('/v1/docs', publicPaths)).toBe(true);
  });

  it('should match prefix patterns with wildcard', () => {
    expect(isPublicPath('/api/public/something', publicPaths)).toBe(true);
    expect(isPublicPath('/api/public/nested/path', publicPaths)).toBe(true);
    expect(isPublicPath('/api/public/', publicPaths)).toBe(true);
  });

  it('should not match non-public paths', () => {
    expect(isPublicPath('/api/private', publicPaths)).toBe(false);
    expect(isPublicPath('/api/db/collection', publicPaths)).toBe(false);
    expect(isPublicPath('/health-check', publicPaths)).toBe(false);
  });

  it('should not match partial prefix without wildcard', () => {
    expect(isPublicPath('/v1/docs/extra', publicPaths)).toBe(false);
    expect(isPublicPath('/metrics/detailed', publicPaths)).toBe(false);
  });

  it('should handle empty public paths array', () => {
    expect(isPublicPath('/health', [])).toBe(false);
    expect(isPublicPath('/any/path', [])).toBe(false);
  });
});

// ============================================================================
// ServiceBindingAuthMiddleware Tests
// ============================================================================

describe('ServiceBindingAuthMiddleware', () => {
  describe('constructor and configuration', () => {
    it('should use default configuration when none provided', () => {
      const middleware = new ServiceBindingAuthMiddleware();

      // Default config should be applied
      expect(middleware).toBeDefined();
    });

    it('should merge custom configuration with defaults', () => {
      const config: AuthMiddlewareConfig = {
        publicPaths: ['/custom-health'],
        refreshThresholdSeconds: 600,
      };
      const middleware = new ServiceBindingAuthMiddleware(config);

      expect(middleware).toBeDefined();
    });
  });

  describe('authenticate', () => {
    it('should authenticate request with valid Bearer token', async () => {
      const env = createMockEnv({ withAuth: true });
      const middleware = new ServiceBindingAuthMiddleware();
      const request = createMockRequest('/api/db/collection', {
        Authorization: `Bearer ${mockValidToken}`,
      });

      const result = await middleware.authenticate(request, env);

      expect(result.authenticated).toBe(true);
      expect(result.user).toBeDefined();
      expect(result.user?.userId).toBe('user_123');
    });

    it('should reject request without Bearer token', async () => {
      const env = createMockEnv({ withAuth: true });
      const middleware = new ServiceBindingAuthMiddleware();
      const request = createMockRequest('/api/db/collection');

      const result = await middleware.authenticate(request, env);

      expect(result.authenticated).toBe(false);
      expect(result.error).toContain('Bearer token required');
      expect(result.errorCode).toBe('MISSING_TOKEN');
      expect(result.statusCode).toBe(401);
    });

    it('should reject request with invalid token', async () => {
      const env = createMockEnv({ withAuth: true });
      const middleware = new ServiceBindingAuthMiddleware();
      const request = createMockRequest('/api/db/collection', {
        Authorization: 'Bearer invalid-token',
      });

      const result = await middleware.authenticate(request, env);

      expect(result.authenticated).toBe(false);
      expect(result.errorCode).toBe('INVALID_TOKEN');
      expect(result.statusCode).toBe(401);
    });

    it('should allow request when AUTH binding is not configured', async () => {
      const env = createMockEnv({ withAuth: false });
      const middleware = new ServiceBindingAuthMiddleware();
      const request = createMockRequest('/api/db/collection');

      const result = await middleware.authenticate(request, env);

      expect(result.authenticated).toBe(true);
    });

    it('should allow request when auth is disabled in config', async () => {
      const env = createMockEnv({ withAuth: true });
      const middleware = new ServiceBindingAuthMiddleware({ enabled: false });
      const request = createMockRequest('/api/db/collection');

      const result = await middleware.authenticate(request, env);

      expect(result.authenticated).toBe(true);
    });

    it('should bypass auth for public paths', async () => {
      const env = createMockEnv({ withAuth: true });
      const middleware = new ServiceBindingAuthMiddleware({
        publicPaths: ['/health', '/api/public/*'],
      });

      const healthRequest = createMockRequest('/health');
      const publicRequest = createMockRequest('/api/public/docs');

      const healthResult = await middleware.authenticate(healthRequest, env);
      const publicResult = await middleware.authenticate(publicRequest, env);

      expect(healthResult.authenticated).toBe(true);
      expect(publicResult.authenticated).toBe(true);
    });

    it('should refresh tokens when nearing expiry', async () => {
      const nearExpiryTime = Math.floor(Date.now() / 1000) + 60; // 60 seconds until expiry
      const env = createMockEnv({
        withAuth: true,
        withOAuth: true,
        authOverrides: {
          validateResponse: {
            valid: true,
            user: mockUserContext,
            expiresAt: nearExpiryTime,
          },
        },
      });

      const middleware = new ServiceBindingAuthMiddleware({
        enableRefresh: true,
        refreshThresholdSeconds: 300, // Refresh when < 5 minutes remaining
      });

      const request = createMockRequest('/api/db/collection', {
        Authorization: `Bearer ${mockValidToken}`,
        'X-Refresh-Token': mockRefreshToken,
      });

      const result = await middleware.authenticate(request, env);

      expect(result.authenticated).toBe(true);
      expect(result.newAccessToken).toBeDefined();
      expect(result.newRefreshToken).toBeDefined();
    });

    it('should not refresh tokens when not nearing expiry', async () => {
      const farExpiryTime = Math.floor(Date.now() / 1000) + 3600; // 1 hour until expiry
      const env = createMockEnv({
        withAuth: true,
        withOAuth: true,
        authOverrides: {
          validateResponse: {
            valid: true,
            user: mockUserContext,
            expiresAt: farExpiryTime,
          },
        },
      });

      const middleware = new ServiceBindingAuthMiddleware({
        enableRefresh: true,
        refreshThresholdSeconds: 300,
      });

      const request = createMockRequest('/api/db/collection', {
        Authorization: `Bearer ${mockValidToken}`,
        'X-Refresh-Token': mockRefreshToken,
      });

      const result = await middleware.authenticate(request, env);

      expect(result.authenticated).toBe(true);
      expect(result.newAccessToken).toBeUndefined();
    });

    it('should continue without refresh if OAUTH binding not available', async () => {
      const nearExpiryTime = Math.floor(Date.now() / 1000) + 60;
      const env = createMockEnv({
        withAuth: true,
        withOAuth: false, // No OAUTH binding
        authOverrides: {
          validateResponse: {
            valid: true,
            user: mockUserContext,
            expiresAt: nearExpiryTime,
          },
        },
      });

      const middleware = new ServiceBindingAuthMiddleware({
        enableRefresh: true,
        refreshThresholdSeconds: 300,
      });

      const request = createMockRequest('/api/db/collection', {
        Authorization: `Bearer ${mockValidToken}`,
        'X-Refresh-Token': mockRefreshToken,
      });

      const result = await middleware.authenticate(request, env);

      expect(result.authenticated).toBe(true);
      expect(result.newAccessToken).toBeUndefined(); // No refresh without OAUTH binding
    });

    it('should continue without refresh if no refresh token provided', async () => {
      const nearExpiryTime = Math.floor(Date.now() / 1000) + 60;
      const env = createMockEnv({
        withAuth: true,
        withOAuth: true,
        authOverrides: {
          validateResponse: {
            valid: true,
            user: mockUserContext,
            expiresAt: nearExpiryTime,
          },
        },
      });

      const middleware = new ServiceBindingAuthMiddleware({
        enableRefresh: true,
        refreshThresholdSeconds: 300,
      });

      const request = createMockRequest('/api/db/collection', {
        Authorization: `Bearer ${mockValidToken}`,
        // No X-Refresh-Token header
      });

      const result = await middleware.authenticate(request, env);

      expect(result.authenticated).toBe(true);
      expect(result.newAccessToken).toBeUndefined();
    });

    it('should continue if refresh fails but original token still valid', async () => {
      const nearExpiryTime = Math.floor(Date.now() / 1000) + 60;
      const env = createMockEnv({
        withAuth: true,
        withOAuth: true,
        authOverrides: {
          validateResponse: {
            valid: true,
            user: mockUserContext,
            expiresAt: nearExpiryTime,
          },
        },
        oauthOverrides: {
          shouldFail: true,
        },
      });

      const middleware = new ServiceBindingAuthMiddleware({
        enableRefresh: true,
        refreshThresholdSeconds: 300,
      });

      const request = createMockRequest('/api/db/collection', {
        Authorization: `Bearer ${mockValidToken}`,
        'X-Refresh-Token': mockRefreshToken,
      });

      const result = await middleware.authenticate(request, env);

      // Should still authenticate even if refresh failed
      expect(result.authenticated).toBe(true);
      expect(result.newAccessToken).toBeUndefined();
    });
  });

  describe('createHandler', () => {
    it('should return a middleware handler function', () => {
      const middleware = new ServiceBindingAuthMiddleware();
      const handler = middleware.createHandler();

      expect(typeof handler).toBe('function');
    });

    it('should call next handler when authenticated', async () => {
      const env = createMockEnv({ withAuth: true });
      const middleware = new ServiceBindingAuthMiddleware();
      const handler = middleware.createHandler();

      const request = createMockRequest('/api/db/collection', {
        Authorization: `Bearer ${mockValidToken}`,
      });

      const nextResponse = new Response(JSON.stringify({ data: 'test' }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      });
      const next = vi.fn().mockResolvedValue(nextResponse);

      const response = await handler(request, env, next);

      expect(next).toHaveBeenCalled();
      expect(response.status).toBe(200);
    });

    it('should return 401 response when not authenticated', async () => {
      const env = createMockEnv({ withAuth: true });
      const middleware = new ServiceBindingAuthMiddleware();
      const handler = middleware.createHandler();

      const request = createMockRequest('/api/db/collection');
      const next = vi.fn();

      const response = await handler(request, env, next);

      expect(next).not.toHaveBeenCalled();
      expect(response.status).toBe(401);

      const body = await response.json() as { error: string; code: string };
      expect(body.error).toBeDefined();
      expect(body.code).toBe('MISSING_TOKEN');
    });

    it('should include WWW-Authenticate header on auth failure', async () => {
      const env = createMockEnv({ withAuth: true });
      const middleware = new ServiceBindingAuthMiddleware();
      const handler = middleware.createHandler();

      const request = createMockRequest('/api/db/collection');
      const next = vi.fn();

      const response = await handler(request, env, next);

      expect(response.headers.get('WWW-Authenticate')).toContain('Bearer');
    });

    it('should add new token headers when tokens are refreshed', async () => {
      const nearExpiryTime = Math.floor(Date.now() / 1000) + 60;
      const env = createMockEnv({
        withAuth: true,
        withOAuth: true,
        authOverrides: {
          validateResponse: {
            valid: true,
            user: mockUserContext,
            expiresAt: nearExpiryTime,
          },
        },
      });

      const middleware = new ServiceBindingAuthMiddleware({
        enableRefresh: true,
        refreshThresholdSeconds: 300,
      });
      const handler = middleware.createHandler();

      const request = createMockRequest('/api/db/collection', {
        Authorization: `Bearer ${mockValidToken}`,
        'X-Refresh-Token': mockRefreshToken,
      });

      const nextResponse = new Response(JSON.stringify({ data: 'test' }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      });
      const next = vi.fn().mockResolvedValue(nextResponse);

      const response = await handler(request, env, next);

      expect(response.headers.get('X-New-Access-Token')).toBeDefined();
      expect(response.headers.get('X-New-Refresh-Token')).toBeDefined();
    });
  });

  describe('getUserContext', () => {
    it('should return user context from auth result', () => {
      const middleware = new ServiceBindingAuthMiddleware();
      const authResult: AuthResult = {
        authenticated: true,
        user: mockUserContext,
      };

      const userContext = middleware.getUserContext(authResult);

      expect(userContext).toEqual(mockUserContext);
    });

    it('should return undefined when no user in auth result', () => {
      const middleware = new ServiceBindingAuthMiddleware();
      const authResult: AuthResult = {
        authenticated: true,
      };

      const userContext = middleware.getUserContext(authResult);

      expect(userContext).toBeUndefined();
    });
  });
});

// ============================================================================
// Factory Function Tests
// ============================================================================

describe('createServiceBindingAuthMiddleware', () => {
  it('should create middleware instance with default config', () => {
    const middleware = createServiceBindingAuthMiddleware();

    expect(middleware).toBeInstanceOf(ServiceBindingAuthMiddleware);
  });

  it('should create middleware instance with custom config', () => {
    const config: AuthMiddlewareConfig = {
      publicPaths: ['/custom'],
      refreshThresholdSeconds: 120,
    };
    const middleware = createServiceBindingAuthMiddleware(config);

    expect(middleware).toBeInstanceOf(ServiceBindingAuthMiddleware);
  });
});

describe('authenticateRequest', () => {
  it('should authenticate request using convenience function', async () => {
    const env = createMockEnv({ withAuth: true });
    const request = createMockRequest('/api/db/collection', {
      Authorization: `Bearer ${mockValidToken}`,
    });

    const result = await authenticateRequest(request, env);

    expect(result.authenticated).toBe(true);
    expect(result.user).toBeDefined();
  });

  it('should accept custom options', async () => {
    const env = createMockEnv({ withAuth: true });
    const request = createMockRequest('/health');

    const result = await authenticateRequest(request, env, {
      publicPaths: ['/health'],
    });

    expect(result.authenticated).toBe(true);
  });
});

// ============================================================================
// Request Enhancement Tests
// ============================================================================

describe('enhanceRequestWithUser', () => {
  it('should add user ID header to request', () => {
    const originalRequest = createMockRequest('/api/db/collection');

    const enhancedRequest = enhanceRequestWithUser(originalRequest, mockUserContext);

    expect(enhancedRequest.headers.get('X-User-Id')).toBe('user_123');
  });

  it('should add email header when present', () => {
    const originalRequest = createMockRequest('/api/db/collection');

    const enhancedRequest = enhanceRequestWithUser(originalRequest, mockUserContext);

    expect(enhancedRequest.headers.get('X-User-Email')).toBe('test@example.com');
  });

  it('should add organization ID header when present', () => {
    const originalRequest = createMockRequest('/api/db/collection');

    const enhancedRequest = enhanceRequestWithUser(originalRequest, mockUserContext);

    expect(enhancedRequest.headers.get('X-Organization-Id')).toBe('org_456');
  });

  it('should add roles header when present', () => {
    const originalRequest = createMockRequest('/api/db/collection');

    const enhancedRequest = enhanceRequestWithUser(originalRequest, mockUserContext);

    expect(enhancedRequest.headers.get('X-User-Roles')).toBe('user,admin');
  });

  it('should add permissions header when present', () => {
    const originalRequest = createMockRequest('/api/db/collection');

    const enhancedRequest = enhanceRequestWithUser(originalRequest, mockUserContext);

    expect(enhancedRequest.headers.get('X-User-Permissions')).toBe('read,write');
  });

  it('should preserve original request properties', () => {
    const originalRequest = createMockRequest('/api/db/collection', {
      'X-Custom-Header': 'custom-value',
    });

    const enhancedRequest = enhanceRequestWithUser(originalRequest, mockUserContext);

    expect(enhancedRequest.url).toBe(originalRequest.url);
    expect(enhancedRequest.method).toBe(originalRequest.method);
    expect(enhancedRequest.headers.get('X-Custom-Header')).toBe('custom-value');
  });

  it('should handle minimal user context', () => {
    const minimalUser: AuthUserContext = {
      userId: 'minimal_user',
    };
    const originalRequest = createMockRequest('/api/db/collection');

    const enhancedRequest = enhanceRequestWithUser(originalRequest, minimalUser);

    expect(enhancedRequest.headers.get('X-User-Id')).toBe('minimal_user');
    expect(enhancedRequest.headers.get('X-User-Email')).toBeNull();
    expect(enhancedRequest.headers.get('X-Organization-Id')).toBeNull();
    expect(enhancedRequest.headers.get('X-User-Roles')).toBeNull();
  });
});

// ============================================================================
// Integration with Worker Tests
// ============================================================================

describe('Integration with Worker Environment', () => {
  it('should work with typical MongoLakeEnv structure', async () => {
    // Simulate the real environment structure from MongoLakeEnv
    interface MongoLakeEnv extends ServiceBindingEnv {
      BUCKET: unknown;
      RPC_NAMESPACE: unknown;
      REQUIRE_AUTH?: boolean;
    }

    const env: MongoLakeEnv = {
      BUCKET: {},
      RPC_NAMESPACE: {},
      REQUIRE_AUTH: true,
      AUTH: createMockAuthService(),
    };

    const middleware = new ServiceBindingAuthMiddleware();
    const request = createMockRequest('/api/db/collection', {
      Authorization: `Bearer ${mockValidToken}`,
    });

    const result = await middleware.authenticate(request, env);

    expect(result.authenticated).toBe(true);
  });

  it('should handle production-like scenario with both bindings', async () => {
    const env = createMockEnv({
      withAuth: true,
      withOAuth: true,
    });

    const middleware = new ServiceBindingAuthMiddleware({
      enabled: true,
      publicPaths: ['/health', '/metrics'],
      enableRefresh: true,
      refreshThresholdSeconds: 300,
      refreshTokenHeader: 'X-Refresh-Token',
    });

    // Protected route with valid token
    const protectedRequest = createMockRequest('/api/db/collection', {
      Authorization: `Bearer ${mockValidToken}`,
    });

    const protectedResult = await middleware.authenticate(protectedRequest, env);
    expect(protectedResult.authenticated).toBe(true);

    // Public route without token
    const publicRequest = createMockRequest('/health');
    const publicResult = await middleware.authenticate(publicRequest, env);
    expect(publicResult.authenticated).toBe(true);

    // Protected route without token
    const unauthRequest = createMockRequest('/api/db/collection');
    const unauthResult = await middleware.authenticate(unauthRequest, env);
    expect(unauthResult.authenticated).toBe(false);
  });
});
