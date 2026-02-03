/**
 * Auth Middleware API Key Authentication Tests
 *
 * Tests for API key validation and scope enforcement.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  type AuthConfig,
  type AuthResult,
  mockAuthConfig,
  mockValidToken,
  mockApiKey,
  mockUserContext,
} from './test-helpers.js';

describe('Auth Middleware - API Key Authentication', () => {
  let createAuthMiddleware: (config: AuthConfig) => { authenticate: (req: Request) => Promise<AuthResult> };

  beforeEach(async () => {
    const module = await import('../../../src/auth/middleware.js');
    createAuthMiddleware = module.createAuthMiddleware;
  });

  it('should accept valid API key in X-API-Key header', async () => {
    const middleware = createAuthMiddleware({
      ...mockAuthConfig,
      apiKeyValidator: async (apiKey: string) => {
        if (apiKey === mockApiKey) {
          return {
            valid: true,
            user: mockUserContext,
            scopes: ['read', 'write'],
          };
        }
        return { valid: false };
      },
    });

    const request = new Request('https://api.mongolake.com/db/test', {
      headers: {
        'X-API-Key': mockApiKey,
      },
    });

    const result = await middleware.authenticate(request);

    expect(result.authenticated).toBe(true);
    expect(result.user).toBeDefined();
    expect(result.authMethod).toBe('api_key');
  });

  it('should accept valid API key in Authorization header with ApiKey scheme', async () => {
    const middleware = createAuthMiddleware({
      ...mockAuthConfig,
      apiKeyValidator: async (apiKey: string) => {
        if (apiKey === mockApiKey) {
          return { valid: true, user: mockUserContext, scopes: ['read', 'write'] };
        }
        return { valid: false };
      },
    });

    const request = new Request('https://api.mongolake.com/db/test', {
      headers: {
        Authorization: `ApiKey ${mockApiKey}`,
      },
    });

    const result = await middleware.authenticate(request);

    expect(result.authenticated).toBe(true);
    expect(result.authMethod).toBe('api_key');
  });

  it('should reject invalid API key', async () => {
    const middleware = createAuthMiddleware({
      ...mockAuthConfig,
      apiKeyValidator: async () => ({ valid: false }),
    });

    const request = new Request('https://api.mongolake.com/db/test', {
      headers: {
        'X-API-Key': 'invalid_api_key',
      },
    });

    const result = await middleware.authenticate(request);

    expect(result.authenticated).toBe(false);
    expect(result.error?.code).toBe('INVALID_API_KEY');
    expect(result.statusCode).toBe(401);
  });

  it('should validate API key format before checking', async () => {
    const middleware = createAuthMiddleware({
      ...mockAuthConfig,
      apiKeyValidator: async () => ({ valid: false }),
    });

    const request = new Request('https://api.mongolake.com/db/test', {
      headers: {
        'X-API-Key': 'bad-format',
      },
    });

    const result = await middleware.authenticate(request);

    expect(result.authenticated).toBe(false);
    expect(result.error?.code).toBe('INVALID_API_KEY_FORMAT');
  });

  it('should enforce API key scopes', async () => {
    const limitedMiddleware = createAuthMiddleware({
      ...mockAuthConfig,
      apiKeyValidator: async () => ({
        valid: true,
        user: mockUserContext,
        scopes: ['read'], // No write scope
      }),
      requiredScopes: ['write'],
    });

    const request = new Request('https://api.mongolake.com/db/test', {
      method: 'POST',
      headers: {
        'X-API-Key': mockApiKey,
      },
    });

    const result = await limitedMiddleware.authenticate(request);

    expect(result.authenticated).toBe(false);
    expect(result.error?.code).toBe('INSUFFICIENT_SCOPE');
    expect(result.statusCode).toBe(403);
  });

  it('should prefer Bearer token over API key when both present', async () => {
    const middleware = createAuthMiddleware({
      ...mockAuthConfig,
      apiKeyValidator: async () => ({
        valid: true,
        user: mockUserContext,
        scopes: ['read', 'write'],
      }),
    });

    const request = new Request('https://api.mongolake.com/db/test', {
      headers: {
        Authorization: `Bearer ${mockValidToken}`,
        'X-API-Key': mockApiKey,
      },
    });

    const result = await middleware.authenticate(request);

    expect(result.authenticated).toBe(true);
    expect(result.authMethod).toBe('bearer');
  });
});
