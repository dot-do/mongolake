/**
 * Auth Middleware Bearer Token Validation Tests
 *
 * Tests for Bearer token validation and expiration handling.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  type AuthConfig,
  type AuthResult,
  mockAuthConfig,
  mockValidToken,
  mockExpiredToken,
} from './test-helpers.js';

describe('Auth Middleware - Bearer Token Validation', () => {
  let createAuthMiddleware: (config: AuthConfig) => { authenticate: (req: Request) => Promise<AuthResult> };

  beforeEach(async () => {
    const module = await import('../../../src/auth/middleware.js');
    createAuthMiddleware = module.createAuthMiddleware;
  });

  describe('validateBearerToken', () => {
    it('should accept valid Bearer token in Authorization header', async () => {
      const middleware = createAuthMiddleware(mockAuthConfig);
      const request = new Request('https://api.mongolake.com/db/test', {
        headers: {
          Authorization: `Bearer ${mockValidToken}`,
        },
      });

      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(true);
      expect(result.user).toBeDefined();
      expect(result.user?.userId).toBe('user_123');
    });

    it('should reject request without Authorization header', async () => {
      const middleware = createAuthMiddleware(mockAuthConfig);
      const request = new Request('https://api.mongolake.com/db/test');

      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(false);
      expect(result.error).toBeDefined();
      expect(result.error?.code).toBe('MISSING_AUTH_HEADER');
      expect(result.statusCode).toBe(401);
    });

    it('should reject request with malformed Authorization header', async () => {
      const middleware = createAuthMiddleware(mockAuthConfig);
      const request = new Request('https://api.mongolake.com/db/test', {
        headers: {
          Authorization: 'InvalidFormat token123',
        },
      });

      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(false);
      expect(result.error?.code).toBe('INVALID_AUTH_FORMAT');
      expect(result.statusCode).toBe(401);
    });

    it('should reject request with empty Bearer token', async () => {
      const middleware = createAuthMiddleware(mockAuthConfig);
      const request = new Request('https://api.mongolake.com/db/test', {
        headers: {
          Authorization: 'Bearer ',
        },
      });

      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(false);
      // Empty token after "Bearer " is treated as invalid auth format
      expect(result.error?.code).toBe('INVALID_AUTH_FORMAT');
      expect(result.statusCode).toBe(401);
    });

    it('should reject invalid/tampered token', async () => {
      const middleware = createAuthMiddleware(mockAuthConfig);
      const request = new Request('https://api.mongolake.com/db/test', {
        headers: {
          Authorization: 'Bearer invalid.token.here',
        },
      });

      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(false);
      expect(result.error?.code).toBe('INVALID_TOKEN');
      expect(result.statusCode).toBe(401);
    });
  });

  describe('expired token handling', () => {
    it('should reject expired tokens with 401', async () => {
      const middleware = createAuthMiddleware(mockAuthConfig);
      const request = new Request('https://api.mongolake.com/db/test', {
        headers: {
          Authorization: `Bearer ${mockExpiredToken}`,
        },
      });

      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(false);
      expect(result.error?.code).toBe('TOKEN_EXPIRED');
      expect(result.statusCode).toBe(401);
      expect(result.error?.message).toContain('expired');
    });

    it('should include WWW-Authenticate header for expired tokens', async () => {
      const middleware = createAuthMiddleware(mockAuthConfig);
      const request = new Request('https://api.mongolake.com/db/test', {
        headers: {
          Authorization: `Bearer ${mockExpiredToken}`,
        },
      });

      const result = await middleware.authenticate(request);

      expect(result.headers?.['WWW-Authenticate']).toBeDefined();
      expect(result.headers?.['WWW-Authenticate']).toContain('Bearer');
      expect(result.headers?.['WWW-Authenticate']).toContain('error="invalid_token"');
    });
  });
});
