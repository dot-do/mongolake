/**
 * Auth Middleware Request/Response Integration Tests
 *
 * Tests for middleware handler, path whitelisting, and error formatting.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  type AuthConfig,
  type AuthResult,
  mockAuthConfig,
  mockValidToken,
} from './test-helpers.js';

describe('Auth Middleware - Request/Response Integration', () => {
  let createAuthMiddleware: (config: AuthConfig) => {
    authenticate: (req: Request) => Promise<AuthResult>;
    createHandler: () => (request: Request, next: () => Promise<Response>) => Promise<Response>;
  };

  beforeEach(async () => {
    const module = await import('../../../src/auth/middleware.js');
    createAuthMiddleware = module.createAuthMiddleware;
  });

  it('should attach user context to request for downstream use', async () => {
    const middleware = createAuthMiddleware(mockAuthConfig);

    const request = new Request('https://api.mongolake.com/db/test', {
      headers: {
        Authorization: `Bearer ${mockValidToken}`,
      },
    });

    const result = await middleware.authenticate(request);
    const enhancedRequest = result.request;

    expect(enhancedRequest).toBeDefined();
    // User context should be accessible via a custom property or header
    expect(enhancedRequest?.headers.get('X-User-Id')).toBe('user_123');
  });

  it('should provide middleware handler function', async () => {
    const middleware = createAuthMiddleware(mockAuthConfig);

    const handler = middleware.createHandler();

    expect(typeof handler).toBe('function');
    expect(handler.length).toBe(2); // (request, next) => ...
  });

  it('should skip auth for whitelisted paths', async () => {
    const middlewareWithWhitelist = createAuthMiddleware({
      ...mockAuthConfig,
      publicPaths: ['/health', '/metrics', '/api/v1/public/*'],
    });

    const request = new Request('https://api.mongolake.com/health');

    const result = await middlewareWithWhitelist.authenticate(request);

    expect(result.authenticated).toBe(true);
    expect(result.skipped).toBe(true);
  });

  it('should support custom auth error response formatter', async () => {
    const customMiddleware = createAuthMiddleware({
      ...mockAuthConfig,
      errorFormatter: (error) => ({
        error: {
          type: error.code,
          description: error.message,
          timestamp: new Date().toISOString(),
        },
      }),
    });

    const request = new Request('https://api.mongolake.com/db/test');

    const result = await customMiddleware.authenticate(request);

    expect(result.errorBody).toHaveProperty('error');
    expect((result.errorBody as { error: { type: string; timestamp: string } })?.error).toHaveProperty('type');
    expect((result.errorBody as { error: { type: string; timestamp: string } })?.error).toHaveProperty('timestamp');
  });
});
