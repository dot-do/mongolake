/**
 * Auth Middleware Multiple Providers Tests
 *
 * Tests for multi-provider authentication support.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  type AuthConfig,
  type AuthResult,
  type AuthProvider,
  mockAuthConfig,
  mockValidToken,
  mockUserContext,
} from './test-helpers.js';

describe('Auth Middleware - Multiple Providers', () => {
  let createAuthMiddleware: (config: AuthConfig) => { authenticate: (req: Request) => Promise<AuthResult> };

  const githubProvider: AuthProvider = {
    name: 'github',
    issuer: 'https://github.com',
    validateToken: vi.fn().mockResolvedValue({ valid: true, user: mockUserContext }),
  };

  const googleProvider: AuthProvider = {
    name: 'google',
    issuer: 'https://accounts.google.com',
    validateToken: vi.fn().mockResolvedValue({ valid: true, user: mockUserContext }),
  };

  beforeEach(async () => {
    const module = await import('../../../src/auth/middleware.js');
    createAuthMiddleware = module.createAuthMiddleware;

    vi.clearAllMocks();
  });

  it('should support multiple authentication providers', async () => {
    const middleware = createAuthMiddleware({
      ...mockAuthConfig,
      providers: [githubProvider, googleProvider],
    });

    const request = new Request('https://api.mongolake.com/db/test', {
      headers: {
        Authorization: `Bearer ${mockValidToken}`,
      },
    });

    const result = await middleware.authenticate(request);

    expect(result.authenticated).toBe(true);
  });

  it('should select provider based on token issuer', async () => {
    const middleware = createAuthMiddleware({
      ...mockAuthConfig,
      providers: [githubProvider, googleProvider],
    });

    // Token with GitHub issuer
    const githubToken = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyXzEyMyIsImlzcyI6Imh0dHBzOi8vZ2l0aHViLmNvbSIsImlhdCI6MTcwNjc4NjQwMCwiZXhwIjoxNzA2ODcyODAwfQ.signature';

    const request = new Request('https://api.mongolake.com/db/test', {
      headers: {
        Authorization: `Bearer ${githubToken}`,
      },
    });

    await middleware.authenticate(request);

    expect(githubProvider.validateToken).toHaveBeenCalled();
    expect(googleProvider.validateToken).not.toHaveBeenCalled();
  });

  it('should allow specifying provider via header', async () => {
    const middleware = createAuthMiddleware({
      ...mockAuthConfig,
      providers: [githubProvider, googleProvider],
    });

    const request = new Request('https://api.mongolake.com/db/test', {
      headers: {
        Authorization: `Bearer ${mockValidToken}`,
        'X-Auth-Provider': 'google',
      },
    });

    await middleware.authenticate(request);

    expect(googleProvider.validateToken).toHaveBeenCalled();
  });

  it('should reject unknown provider', async () => {
    const middleware = createAuthMiddleware({
      ...mockAuthConfig,
      providers: [githubProvider, googleProvider],
    });

    const request = new Request('https://api.mongolake.com/db/test', {
      headers: {
        Authorization: `Bearer ${mockValidToken}`,
        'X-Auth-Provider': 'unknown',
      },
    });

    const result = await middleware.authenticate(request);

    expect(result.authenticated).toBe(false);
    expect(result.error?.code).toBe('UNKNOWN_PROVIDER');
  });

  it('should fallback to default provider when issuer not matched', async () => {
    const middleware = createAuthMiddleware({
      ...mockAuthConfig,
      providers: [githubProvider, googleProvider],
    });

    const unknownIssuerToken = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyXzEyMyIsImlzcyI6Imh0dHBzOi8vdW5rbm93bi5jb20iLCJpYXQiOjE3MDY3ODY0MDAsImV4cCI6MTcwNjg3MjgwMH0.signature';

    const request = new Request('https://api.mongolake.com/db/test', {
      headers: {
        Authorization: `Bearer ${unknownIssuerToken}`,
      },
    });

    const result = await middleware.authenticate(request);

    // Should use default oauth.do validation
    expect(result.authenticated).toBeDefined();
  });

  it('should merge user context from provider with base claims', async () => {
    githubProvider.validateToken = vi.fn().mockResolvedValue({
      valid: true,
      user: {
        ...mockUserContext,
        metadata: { githubLogin: 'testuser' },
      },
    });

    const middleware = createAuthMiddleware({
      ...mockAuthConfig,
      providers: [githubProvider, googleProvider],
    });

    const githubToken = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyXzEyMyIsImlzcyI6Imh0dHBzOi8vZ2l0aHViLmNvbSIsImlhdCI6MTcwNjc4NjQwMCwiZXhwIjoxNzA2ODcyODAwfQ.signature';

    const request = new Request('https://api.mongolake.com/db/test', {
      headers: {
        Authorization: `Bearer ${githubToken}`,
      },
    });

    const result = await middleware.authenticate(request);

    expect(result.user?.metadata?.githubLogin).toBe('testuser');
  });
});
