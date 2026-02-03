/**
 * Auth Middleware Token Refresh Tests
 *
 * Tests for automatic token refresh and threshold handling.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  type AuthConfig,
  type AuthResult,
  mockAuthConfig,
  mockValidToken,
  mockNearExpiryToken,
  mockRefreshToken,
  createMockTokenRefresher,
} from './test-helpers.js';

describe('Auth Middleware - Token Refresh', () => {
  let createAuthMiddleware: (config: AuthConfig) => { authenticate: (req: Request) => Promise<AuthResult> };
  let mockTokenRefresher: ReturnType<typeof vi.fn>;

  beforeEach(async () => {
    const module = await import('../../../src/auth/middleware.js');
    createAuthMiddleware = module.createAuthMiddleware;
    mockTokenRefresher = createMockTokenRefresher();
  });

  it('should refresh tokens automatically when near expiry', async () => {
    const middleware = createAuthMiddleware({
      ...mockAuthConfig,
      tokenRefresher: mockTokenRefresher,
    });

    const request = new Request('https://api.mongolake.com/db/test', {
      headers: {
        Authorization: `Bearer ${mockNearExpiryToken}`,
        'X-Refresh-Token': mockRefreshToken,
      },
    });

    const result = await middleware.authenticate(request);

    expect(result.authenticated).toBe(true);
    expect(mockTokenRefresher).toHaveBeenCalledWith(mockRefreshToken);
    expect(result.newTokens).toBeDefined();
    expect(result.newTokens?.accessToken).toBe(mockValidToken);
  });

  it('should include refreshed token in response headers', async () => {
    const middleware = createAuthMiddleware({
      ...mockAuthConfig,
      tokenRefresher: mockTokenRefresher,
    });

    const request = new Request('https://api.mongolake.com/db/test', {
      headers: {
        Authorization: `Bearer ${mockNearExpiryToken}`,
        'X-Refresh-Token': mockRefreshToken,
      },
    });

    const result = await middleware.authenticate(request);

    expect(result.headers?.['X-New-Access-Token']).toBeDefined();
    expect(result.headers?.['X-New-Refresh-Token']).toBeDefined();
  });

  it('should fail gracefully if refresh token is invalid', async () => {
    mockTokenRefresher.mockRejectedValueOnce(new Error('Invalid refresh token'));

    const middleware = createAuthMiddleware({
      ...mockAuthConfig,
      tokenRefresher: mockTokenRefresher,
    });

    const request = new Request('https://api.mongolake.com/db/test', {
      headers: {
        Authorization: `Bearer ${mockNearExpiryToken}`,
        'X-Refresh-Token': 'invalid_refresh_token',
      },
    });

    const result = await middleware.authenticate(request);

    expect(result.authenticated).toBe(false);
    expect(result.error?.code).toBe('REFRESH_FAILED');
    expect(result.statusCode).toBe(401);
  });

  it('should not refresh if token has sufficient time remaining', async () => {
    const middleware = createAuthMiddleware({
      ...mockAuthConfig,
      tokenRefresher: mockTokenRefresher,
    });

    const request = new Request('https://api.mongolake.com/db/test', {
      headers: {
        Authorization: `Bearer ${mockValidToken}`,
        'X-Refresh-Token': mockRefreshToken,
      },
    });

    await middleware.authenticate(request);

    expect(mockTokenRefresher).not.toHaveBeenCalled();
  });

  it('should configure refresh threshold', async () => {
    const customMiddleware = createAuthMiddleware({
      ...mockAuthConfig,
      tokenRefresher: mockTokenRefresher,
      refreshThresholdSeconds: 300, // 5 minutes
    });

    const request = new Request('https://api.mongolake.com/db/test', {
      headers: {
        Authorization: `Bearer ${mockNearExpiryToken}`,
        'X-Refresh-Token': mockRefreshToken,
      },
    });

    await customMiddleware.authenticate(request);

    expect(mockTokenRefresher).toHaveBeenCalled();
  });
});
