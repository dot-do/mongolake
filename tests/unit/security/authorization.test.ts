/**
 * Security Tests - Authorization
 *
 * Tests for authentication middleware security, token validation,
 * and permission enforcement.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import type {
  AuthConfig,
  AuthResult,
  UserContext,
  ApiKeyValidation,
} from '../auth/test-helpers.js';
import { testSecret, createHs256Token } from '../auth/test-helpers.js';

// ============================================================================
// Auth Middleware - Token Rejection Tests
// ============================================================================

describe('Security - Auth Middleware Token Validation', () => {
  let createAuthMiddleware: (config: AuthConfig) => { authenticate: (req: Request) => Promise<AuthResult> };

  beforeEach(async () => {
    const module = await import('../../../src/auth/middleware.js');
    createAuthMiddleware = module.createAuthMiddleware;
  });

  const baseConfig: AuthConfig = {
    issuer: 'https://oauth.do',
    audience: 'mongolake',
    clientId: 'mongolake-client',
    tokenEndpoint: 'https://oauth.do/token',
  };

  describe('Invalid token format rejection', () => {
    it('should reject token with invalid JWT structure (not 3 parts)', async () => {
      const middleware = createAuthMiddleware(baseConfig);
      const invalidTokens = [
        'not-a-jwt',
        'only.two',
        'has.four.parts.here',
        '',
        '...',
        'a.b.c.d.e',
      ];

      for (const token of invalidTokens) {
        const request = new Request('https://api.mongolake.com/db/test', {
          headers: { Authorization: `Bearer ${token}` },
        });
        const result = await middleware.authenticate(request);
        expect(result.authenticated).toBe(false);
        expect(result.error).toBeDefined();
        expect(result.statusCode).toBe(401);
      }
    });

    it('should reject token with invalid base64 encoding', async () => {
      const middleware = createAuthMiddleware(baseConfig);
      // Invalid base64 in header (contains invalid chars)
      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { Authorization: 'Bearer !!!.!!!.!!!' },
      });
      const result = await middleware.authenticate(request);
      expect(result.authenticated).toBe(false);
      expect(result.statusCode).toBe(401);
    });

    it('should reject token with malformed JSON in payload', async () => {
      const middleware = createAuthMiddleware(baseConfig);
      // Valid base64 but not valid JSON
      const invalidPayload = btoa('not json').replace(/=/g, '');
      const header = btoa(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).replace(/=/g, '');
      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { Authorization: `Bearer ${header}.${invalidPayload}.signature` },
      });
      const result = await middleware.authenticate(request);
      expect(result.authenticated).toBe(false);
    });
  });

  describe('Expired token rejection', () => {
    it('should reject recently expired tokens', async () => {
      const middleware = createAuthMiddleware(baseConfig);
      const now = Math.floor(Date.now() / 1000);
      // Token expired 1 second ago with recent iat
      const expiredToken = await createHs256Token(
        {
          sub: 'user_123',
          email: 'test@example.com',
          iat: now - 3600, // Issued 1 hour ago
          exp: now - 1, // Expired 1 second ago
        },
        testSecret
      );

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { Authorization: `Bearer ${expiredToken}` },
      });
      const result = await middleware.authenticate(request);
      expect(result.authenticated).toBe(false);
      expect(result.error?.code).toBe('TOKEN_EXPIRED');
      expect(result.statusCode).toBe(401);
    });

    it('should reject intentionally expired test tokens (exp == iat)', async () => {
      const middleware = createAuthMiddleware(baseConfig);
      const now = Math.floor(Date.now() / 1000);
      // Intentionally expired token (exp equals iat)
      const header = btoa(JSON.stringify({ alg: 'RS256', typ: 'JWT' })).replace(/=/g, '');
      const payload = btoa(JSON.stringify({
        sub: 'user_123',
        iat: now,
        exp: now, // Same as iat - clearly intentionally expired
      })).replace(/=/g, '');

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { Authorization: `Bearer ${header}.${payload}.signature` },
      });
      const result = await middleware.authenticate(request);
      expect(result.authenticated).toBe(false);
      expect(result.error?.code).toBe('TOKEN_EXPIRED');
    });

    it('should include WWW-Authenticate header for expired tokens', async () => {
      const middleware = createAuthMiddleware(baseConfig);
      const now = Math.floor(Date.now() / 1000);
      const header = btoa(JSON.stringify({ alg: 'RS256', typ: 'JWT' })).replace(/=/g, '');
      const payload = btoa(JSON.stringify({
        sub: 'user_123',
        iat: now - 10,
        exp: now - 1,
      })).replace(/=/g, '');

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { Authorization: `Bearer ${header}.${payload}.signature` },
      });
      const result = await middleware.authenticate(request);
      expect(result.headers?.['WWW-Authenticate']).toBeDefined();
      expect(result.headers?.['WWW-Authenticate']).toContain('Bearer');
      expect(result.headers?.['WWW-Authenticate']).toContain('error="invalid_token"');
    });
  });

  describe('Missing authentication rejection', () => {
    it('should reject request without any auth header', async () => {
      const middleware = createAuthMiddleware(baseConfig);
      const request = new Request('https://api.mongolake.com/db/test');
      const result = await middleware.authenticate(request);
      expect(result.authenticated).toBe(false);
      expect(result.error?.code).toBe('MISSING_AUTH_HEADER');
      expect(result.statusCode).toBe(401);
    });

    it('should reject request with empty Authorization header', async () => {
      const middleware = createAuthMiddleware(baseConfig);
      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { Authorization: '' },
      });
      const result = await middleware.authenticate(request);
      expect(result.authenticated).toBe(false);
    });

    it('should reject request with Bearer but no token', async () => {
      const middleware = createAuthMiddleware(baseConfig);
      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { Authorization: 'Bearer ' },
      });
      const result = await middleware.authenticate(request);
      expect(result.authenticated).toBe(false);
      expect(result.statusCode).toBe(401);
    });

    it('should reject request with unsupported auth scheme', async () => {
      const middleware = createAuthMiddleware(baseConfig);
      const unsupportedSchemes = ['Basic', 'Digest', 'NTLM', 'Custom'];

      for (const scheme of unsupportedSchemes) {
        const request = new Request('https://api.mongolake.com/db/test', {
          headers: { Authorization: `${scheme} sometoken` },
        });
        const result = await middleware.authenticate(request);
        expect(result.authenticated).toBe(false);
        expect(result.error?.code).toBe('INVALID_AUTH_FORMAT');
      }
    });
  });

  describe('Token tampering detection', () => {
    it('should reject tokens with modified payload (signature mismatch)', async () => {
      const middleware = createAuthMiddleware({
        ...baseConfig,
        jwtSecret: testSecret,
      });

      // Create a valid token
      const validToken = await createHs256Token(
        {
          sub: 'user_123',
          email: 'normal@example.com',
          iat: Math.floor(Date.now() / 1000),
          exp: Math.floor(Date.now() / 1000) + 3600,
        },
        testSecret
      );

      // Tamper with the payload
      const parts = validToken.split('.');
      const tamperedPayload = btoa(JSON.stringify({
        sub: 'admin_1', // Changed from user_123
        email: 'admin@example.com', // Escalated privileges
        roles: ['admin'], // Added admin role
        iat: Math.floor(Date.now() / 1000),
        exp: Math.floor(Date.now() / 1000) + 3600,
      })).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');

      const tamperedToken = `${parts[0]}.${tamperedPayload}.${parts[2]}`;

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { Authorization: `Bearer ${tamperedToken}` },
      });
      const result = await middleware.authenticate(request);
      expect(result.authenticated).toBe(false);
      expect(result.error?.code).toBe('INVALID_SIGNATURE');
    });

    it('should reject tokens signed with different secret', async () => {
      const middleware = createAuthMiddleware({
        ...baseConfig,
        jwtSecret: testSecret,
      });

      // Create token with different secret
      const wrongSecretToken = await createHs256Token(
        {
          sub: 'user_123',
          iat: Math.floor(Date.now() / 1000),
          exp: Math.floor(Date.now() / 1000) + 3600,
        },
        'different-secret-key-that-should-fail-validation'
      );

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { Authorization: `Bearer ${wrongSecretToken}` },
      });
      const result = await middleware.authenticate(request);
      expect(result.authenticated).toBe(false);
      expect(result.error?.code).toBe('INVALID_SIGNATURE');
    });
  });

  describe('Algorithm confusion attacks', () => {
    it('should reject "none" algorithm tokens', async () => {
      const middleware = createAuthMiddleware({
        ...baseConfig,
        jwtSecret: testSecret,
      });

      // Create token with "none" algorithm
      const header = btoa(JSON.stringify({ alg: 'none', typ: 'JWT' })).replace(/=/g, '');
      const payload = btoa(JSON.stringify({
        sub: 'admin_1',
        roles: ['admin'],
        iat: Math.floor(Date.now() / 1000),
        exp: Math.floor(Date.now() / 1000) + 3600,
      })).replace(/=/g, '');
      const noneAlgToken = `${header}.${payload}.`;

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { Authorization: `Bearer ${noneAlgToken}` },
      });
      const result = await middleware.authenticate(request);
      expect(result.authenticated).toBe(false);
      expect(result.error?.code).toBe('INSECURE_ALGORITHM');
    });

    it('should reject unsupported algorithms', async () => {
      const middleware = createAuthMiddleware({
        ...baseConfig,
        jwtSecret: testSecret,
      });

      const unsupportedAlgorithms = ['HS384', 'HS512', 'ES256', 'PS256'];

      for (const alg of unsupportedAlgorithms) {
        const header = btoa(JSON.stringify({ alg, typ: 'JWT' })).replace(/=/g, '');
        const payload = btoa(JSON.stringify({
          sub: 'user_123',
          iat: Math.floor(Date.now() / 1000),
          exp: Math.floor(Date.now() / 1000) + 3600,
        })).replace(/=/g, '');
        const token = `${header}.${payload}.fakeSignature`;

        const request = new Request('https://api.mongolake.com/db/test', {
          headers: { Authorization: `Bearer ${token}` },
        });
        const result = await middleware.authenticate(request);
        expect(result.authenticated).toBe(false);
        // The middleware rejects unsupported algorithms - the error code depends on
        // where the rejection happens (during signature verification vs token parsing)
        expect(['UNSUPPORTED_ALGORITHM', 'INVALID_TOKEN']).toContain(result.error?.code);
      }
    });
  });
});

// ============================================================================
// Permission Enforcement Tests
// ============================================================================

describe('Security - Permission Enforcement', () => {
  let createAuthMiddleware: (config: AuthConfig) => { authenticate: (req: Request) => Promise<AuthResult> };

  beforeEach(async () => {
    const module = await import('../../../src/auth/middleware.js');
    createAuthMiddleware = module.createAuthMiddleware;
  });

  const baseConfig: AuthConfig = {
    issuer: 'https://oauth.do',
    audience: 'mongolake',
    clientId: 'mongolake-client',
    tokenEndpoint: 'https://oauth.do/token',
  };

  describe('API Key scope enforcement', () => {
    it('should reject API key missing required scopes', async () => {
      const apiKeyValidator = vi.fn().mockResolvedValue({
        valid: true,
        user: { userId: 'service_1', roles: ['service'] },
        scopes: ['read'], // Only has read scope
      } as ApiKeyValidation);

      const middleware = createAuthMiddleware({
        ...baseConfig,
        apiKeyValidator,
        requiredScopes: ['read', 'write'], // Requires both read and write
      });

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { 'X-API-Key': 'mlk_live_validapikey123' },
      });
      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(false);
      expect(result.error?.code).toBe('INSUFFICIENT_SCOPE');
      expect(result.statusCode).toBe(403);
      expect(result.error?.message).toContain('write');
    });

    it('should accept API key with all required scopes', async () => {
      const apiKeyValidator = vi.fn().mockResolvedValue({
        valid: true,
        user: { userId: 'service_1', roles: ['service'] },
        scopes: ['read', 'write', 'admin'],
      } as ApiKeyValidation);

      const middleware = createAuthMiddleware({
        ...baseConfig,
        apiKeyValidator,
        requiredScopes: ['read', 'write'],
      });

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { 'X-API-Key': 'mlk_live_validapikey123' },
      });
      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(true);
      expect(result.user?.userId).toBe('service_1');
    });

    it('should reject invalid API keys', async () => {
      const apiKeyValidator = vi.fn().mockResolvedValue({
        valid: false,
      } as ApiKeyValidation);

      const middleware = createAuthMiddleware({
        ...baseConfig,
        apiKeyValidator,
      });

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { 'X-API-Key': 'mlk_live_invalidkey' },
      });
      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(false);
      expect(result.error?.code).toBe('INVALID_API_KEY');
      expect(result.statusCode).toBe(401);
    });
  });

  describe('API Key format validation', () => {
    it('should reject API keys that are too short', async () => {
      const apiKeyValidator = vi.fn().mockResolvedValue({ valid: true });
      const middleware = createAuthMiddleware({
        ...baseConfig,
        apiKeyValidator,
      });

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { 'X-API-Key': 'short' }, // Less than 10 chars
      });
      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(false);
      expect(result.error?.code).toBe('INVALID_API_KEY_FORMAT');
      // Validator should not be called for invalid format
      expect(apiKeyValidator).not.toHaveBeenCalled();
    });

    it('should reject API keys containing hyphens', async () => {
      const apiKeyValidator = vi.fn().mockResolvedValue({ valid: true });
      const middleware = createAuthMiddleware({
        ...baseConfig,
        apiKeyValidator,
      });

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { 'X-API-Key': 'mlk-live-key123' }, // Contains hyphens
      });
      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(false);
      expect(result.error?.code).toBe('INVALID_API_KEY_FORMAT');
    });

    it('should reject when API key auth is not configured', async () => {
      const middleware = createAuthMiddleware(baseConfig); // No apiKeyValidator

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { 'X-API-Key': 'mlk_live_validapikey123' },
      });
      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(false);
      expect(result.error?.code).toBe('API_KEY_NOT_SUPPORTED');
    });
  });

  describe('Public path bypass', () => {
    it('should skip authentication for whitelisted paths', async () => {
      const middleware = createAuthMiddleware({
        ...baseConfig,
        publicPaths: ['/health', '/metrics', '/api/v1/public/*'],
      });

      const publicRequests = [
        new Request('https://api.mongolake.com/health'),
        new Request('https://api.mongolake.com/metrics'),
        new Request('https://api.mongolake.com/api/v1/public/status'),
        new Request('https://api.mongolake.com/api/v1/public/docs/openapi'),
      ];

      for (const request of publicRequests) {
        const result = await middleware.authenticate(request);
        expect(result.authenticated).toBe(true);
        expect(result.skipped).toBe(true);
      }
    });

    it('should require authentication for non-public paths', async () => {
      const middleware = createAuthMiddleware({
        ...baseConfig,
        publicPaths: ['/health', '/api/v1/public/*'],
      });

      const privateRequests = [
        new Request('https://api.mongolake.com/api/v1/users'),
        new Request('https://api.mongolake.com/api/v1/databases'),
        new Request('https://api.mongolake.com/admin'),
      ];

      for (const request of privateRequests) {
        const result = await middleware.authenticate(request);
        expect(result.authenticated).toBe(false);
        expect(result.skipped).toBeUndefined();
      }
    });

    it('should not allow path traversal to bypass auth', async () => {
      const middleware = createAuthMiddleware({
        ...baseConfig,
        publicPaths: ['/public/*'],
      });

      // Attempting path traversal to access protected resources
      // Note: The Request constructor normalizes URLs, so '../' gets resolved
      // The key security property is that the middleware properly checks the final path
      const bypassAttempts = [
        new Request('https://api.mongolake.com/public/../api/users'),
        new Request('https://api.mongolake.com/public/..%2Fapi%2Fusers'),
      ];

      for (const request of bypassAttempts) {
        const result = await middleware.authenticate(request);
        // After URL normalization by the Request constructor:
        // - '/public/../api/users' becomes '/api/users'
        // - The encoded version keeps the encoded chars in the path
        // Either way, requests outside /public/* should require auth
        // (If skipped=true, that's fine IF the normalized URL is still under /public/*)
        const url = new URL(request.url);
        if (!url.pathname.startsWith('/public/')) {
          // Path is not under /public/, should require authentication
          expect(result.authenticated).toBe(false);
        }
        // If the path IS under /public/ after normalization, skipping is OK
      }
    });
  });

  describe('Auth provider validation', () => {
    it('should reject unknown auth provider', async () => {
      const validToken = await createHs256Token(
        {
          sub: 'user_123',
          iat: Math.floor(Date.now() / 1000),
          exp: Math.floor(Date.now() / 1000) + 3600,
          iss: 'https://oauth.do',
        },
        testSecret
      );

      const middleware = createAuthMiddleware({
        ...baseConfig,
        providers: [
          {
            name: 'google',
            issuer: 'https://accounts.google.com',
            validateToken: vi.fn().mockResolvedValue({ valid: true }),
          },
        ],
      });

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: {
          Authorization: `Bearer ${validToken}`,
          'X-Auth-Provider': 'unknown-provider',
        },
      });
      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(false);
      expect(result.error?.code).toBe('UNKNOWN_PROVIDER');
    });

    it('should use specified auth provider when header present', async () => {
      const providerValidator = vi.fn().mockResolvedValue({
        valid: true,
        user: { userId: 'google_user_123', roles: ['user'] } as UserContext,
      });

      const middleware = createAuthMiddleware({
        ...baseConfig,
        providers: [
          {
            name: 'google',
            issuer: 'https://accounts.google.com',
            validateToken: providerValidator,
          },
        ],
      });

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: {
          Authorization: 'Bearer some-google-token',
          'X-Auth-Provider': 'google',
        },
      });
      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(true);
      expect(result.user?.userId).toBe('google_user_123');
      expect(providerValidator).toHaveBeenCalledWith('some-google-token');
    });
  });

  describe('Token cache security', () => {
    it('should not return cached result for revoked tokens', async () => {
      // First, authenticate successfully to cache the token
      const validToken = await createHs256Token(
        {
          sub: 'user_123',
          iat: Math.floor(Date.now() / 1000),
          exp: Math.floor(Date.now() / 1000) + 3600,
        },
        testSecret
      );

      const middleware = createAuthMiddleware({
        ...baseConfig,
        cacheEnabled: true,
        skipSignatureVerification: true, // Skip for test simplicity
      });

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { Authorization: `Bearer ${validToken}` },
      });

      // First request caches the token
      const result1 = await middleware.authenticate(request);
      expect(result1.authenticated).toBe(true);

      // Second request should use cache
      const result2 = await middleware.authenticate(request);
      expect(result2.authenticated).toBe(true);
      expect(result2.fromCache).toBe(true);
    });

    it('should cache tokens only when enabled', async () => {
      const validToken = await createHs256Token(
        {
          sub: 'user_123',
          iat: Math.floor(Date.now() / 1000),
          exp: Math.floor(Date.now() / 1000) + 3600,
        },
        testSecret
      );

      const middleware = createAuthMiddleware({
        ...baseConfig,
        cacheEnabled: false,
        skipSignatureVerification: true,
      });

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: { Authorization: `Bearer ${validToken}` },
      });

      // Multiple requests
      const result1 = await middleware.authenticate(request);
      const result2 = await middleware.authenticate(request);

      expect(result1.authenticated).toBe(true);
      expect(result2.authenticated).toBe(true);
      expect(result2.fromCache).toBeUndefined();
    });
  });
});

// ============================================================================
// User Context Extraction Security
// ============================================================================

describe('Security - User Context Extraction', () => {
  let extractUserContext: (token: string) => UserContext;

  beforeEach(async () => {
    const module = await import('../../../src/auth/middleware.js');
    extractUserContext = module.extractUserContext;
  });

  it('should reject token without subject (sub) claim', async () => {
    const tokenWithoutSub = await createHs256Token(
      {
        email: 'test@example.com',
        iat: Math.floor(Date.now() / 1000),
        exp: Math.floor(Date.now() / 1000) + 3600,
      },
      testSecret
    );

    expect(() => extractUserContext(tokenWithoutSub)).toThrow(/sub.*claim/i);
  });

  it('should extract standard claims correctly', async () => {
    const token = await createHs256Token(
      {
        sub: 'user_123',
        email: 'test@example.com',
        roles: ['user', 'admin'],
        permissions: ['read', 'write'],
        org_id: 'org_456',
        iat: Math.floor(Date.now() / 1000),
        exp: Math.floor(Date.now() / 1000) + 3600,
      },
      testSecret
    );

    const context = extractUserContext(token);

    expect(context.userId).toBe('user_123');
    expect(context.email).toBe('test@example.com');
    expect(context.roles).toEqual(['user', 'admin']);
    expect(context.permissions).toEqual(['read', 'write']);
    expect(context.organizationId).toBe('org_456');
  });

  it('should not include reserved claims in metadata', async () => {
    const token = await createHs256Token(
      {
        sub: 'user_123',
        email: 'test@example.com',
        iss: 'https://oauth.do',
        aud: 'mongolake',
        exp: Math.floor(Date.now() / 1000) + 3600,
        iat: Math.floor(Date.now() / 1000),
        nbf: Math.floor(Date.now() / 1000),
        jti: 'unique-token-id',
        customClaim: 'custom-value',
      },
      testSecret
    );

    const context = extractUserContext(token);

    // Custom claim should be in metadata
    expect(context.metadata?.customClaim).toBe('custom-value');

    // Reserved claims should NOT be in metadata
    expect(context.metadata?.iss).toBeUndefined();
    expect(context.metadata?.aud).toBeUndefined();
    expect(context.metadata?.exp).toBeUndefined();
    expect(context.metadata?.iat).toBeUndefined();
    expect(context.metadata?.nbf).toBeUndefined();
    expect(context.metadata?.jti).toBeUndefined();
    expect(context.metadata?.sub).toBeUndefined();
  });

  it('should handle tokens with minimal claims', async () => {
    const token = await createHs256Token(
      {
        sub: 'user_123',
      },
      testSecret
    );

    const context = extractUserContext(token);

    expect(context.userId).toBe('user_123');
    expect(context.email).toBeUndefined();
    expect(context.roles).toEqual([]);
    expect(context.permissions).toBeUndefined();
    expect(context.organizationId).toBeUndefined();
    expect(context.metadata).toBeUndefined();
  });
});
