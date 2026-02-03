/**
 * Production-Ready Auth Tests
 *
 * Tests for production-ready JWT/OAuth authentication features:
 * - RS256/ES256 JWT support
 * - JWKs endpoint fetching with key rotation
 * - OAuth 2.0 authorization code flow
 * - Refresh token handling
 * - Role-based access control (RBAC)
 * - Audit logging
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  type AuthConfig,
  mockAuthConfig,
  testSecret,
  createHs256Token,
} from './test-helpers.js';

describe('Production Auth Features', () => {
  describe('ES256 JWT Support', () => {
    let verifyJwt: (token: string, options?: {
      secret?: string;
      publicKey?: string;
      skipSignatureVerification?: boolean;
      clockTolerance?: number;
    }) => Promise<{
      valid: boolean;
      header?: { alg: string; typ?: string; kid?: string };
      claims?: Record<string, unknown>;
      error?: { code: string; message: string };
    }>;

    beforeEach(async () => {
      const module = await import('../../../src/auth/middleware.js');
      verifyJwt = module.verifyJwt;
    });

    it('should reject ES256 token without public key', async () => {
      // Create a mock ES256 token (won't have valid signature, but tests error handling)
      const header = { alg: 'ES256', typ: 'JWT' };
      const payload = { sub: 'user_123', exp: Math.floor(Date.now() / 1000) + 3600 };
      const headerB64 = btoa(JSON.stringify(header)).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');
      const payloadB64 = btoa(JSON.stringify(payload)).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');
      const token = `${headerB64}.${payloadB64}.fake-signature`;

      const result = await verifyJwt(token, {});

      expect(result.valid).toBe(false);
      expect(result.error?.code).toBe('MISSING_PUBLIC_KEY');
    });

    it('should support ES256 algorithm in verification', async () => {
      // Skip signature verification to test claims parsing with ES256 header
      const header = { alg: 'ES256', typ: 'JWT' };
      const now = Math.floor(Date.now() / 1000);
      const payload = { sub: 'user_123', exp: now + 3600 };
      const headerB64 = btoa(JSON.stringify(header)).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');
      const payloadB64 = btoa(JSON.stringify(payload)).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');
      const token = `${headerB64}.${payloadB64}.fake-signature`;

      const result = await verifyJwt(token, { skipSignatureVerification: true });

      expect(result.valid).toBe(true);
      expect(result.header?.alg).toBe('ES256');
      expect(result.claims?.sub).toBe('user_123');
    });
  });

  describe('JWKs Endpoint Fetching', () => {
    let fetchJWKS: (uri: string, options?: { forceRefresh?: boolean }) => Promise<Map<string, CryptoKey>>;
    let clearJWKSCache: () => void;

    beforeEach(async () => {
      const module = await import('../../../src/auth/middleware.js');
      fetchJWKS = module.fetchJWKS;
      clearJWKSCache = module.clearJWKSCache;

      // Clear cache before each test
      clearJWKSCache();
    });

    afterEach(() => {
      vi.restoreAllMocks();
    });

    it('should fetch and parse JWKs from endpoint', async () => {
      const mockJwks = {
        keys: [
          {
            kty: 'RSA',
            kid: 'key-1',
            use: 'sig',
            n: 'sXchDaQebSXKcvLwANQcBpg',
            e: 'AQAB',
            alg: 'RS256',
          },
        ],
      };

      vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
        new Response(JSON.stringify(mockJwks), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        })
      );

      const keys = await fetchJWKS('https://example.com/.well-known/jwks.json');

      expect(keys.size).toBe(1);
      expect(keys.has('key-1')).toBe(true);
    });

    it('should cache JWKs and return cached result', async () => {
      const mockJwks = {
        keys: [
          {
            kty: 'RSA',
            kid: 'key-1',
            use: 'sig',
            n: 'sXchDaQebSXKcvLwANQcBpg',
            e: 'AQAB',
            alg: 'RS256',
          },
        ],
      };

      const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
        new Response(JSON.stringify(mockJwks), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        })
      );

      // First fetch
      await fetchJWKS('https://example.com/.well-known/jwks.json');

      // Second fetch should use cache
      await fetchJWKS('https://example.com/.well-known/jwks.json');

      expect(fetchSpy).toHaveBeenCalledTimes(1);
    });

    it('should force refresh when requested', async () => {
      const mockJwks = {
        keys: [
          {
            kty: 'RSA',
            kid: 'key-1',
            use: 'sig',
            n: 'sXchDaQebSXKcvLwANQcBpg',
            e: 'AQAB',
            alg: 'RS256',
          },
        ],
      };

      const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
        new Response(JSON.stringify(mockJwks), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        })
      );

      // First fetch
      await fetchJWKS('https://example.com/.well-known/jwks.json');

      // Force refresh
      await fetchJWKS('https://example.com/.well-known/jwks.json', { forceRefresh: true });

      expect(fetchSpy).toHaveBeenCalledTimes(2);
    });

    it('should handle JWKs fetch errors gracefully', async () => {
      vi.spyOn(globalThis, 'fetch').mockRejectedValueOnce(new Error('Network error'));

      await expect(
        fetchJWKS('https://example.com/.well-known/jwks.json')
      ).rejects.toThrow('Network error');
    });

    it('should skip encryption keys (use=enc)', async () => {
      const mockJwks = {
        keys: [
          {
            kty: 'RSA',
            kid: 'signing-key',
            use: 'sig',
            n: 'sXchDaQebSXKcvLwANQcBpg',
            e: 'AQAB',
            alg: 'RS256',
          },
          {
            kty: 'RSA',
            kid: 'encryption-key',
            use: 'enc',
            n: 'xyz123',
            e: 'AQAB',
            alg: 'RSA-OAEP',
          },
        ],
      };

      vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
        new Response(JSON.stringify(mockJwks), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        })
      );

      const keys = await fetchJWKS('https://example.com/.well-known/jwks.json');

      expect(keys.size).toBe(1);
      expect(keys.has('signing-key')).toBe(true);
      expect(keys.has('encryption-key')).toBe(false);
    });
  });

  describe('OAuth 2.0 Authorization Code Flow', () => {
    let AuthorizationCodeHandler: new (config: {
      clientId: string;
      clientSecret?: string;
      authorizationEndpoint: string;
      tokenEndpoint: string;
      redirectUri: string;
      scope?: string;
    }) => {
      generateState: () => string;
      generateCodeVerifier: () => string;
      generateCodeChallenge: (codeVerifier: string) => Promise<string>;
      getAuthorizationUrl: (options?: {
        state?: string;
        codeVerifier?: string;
        scope?: string;
      }) => Promise<{ url: string; state: string; codeVerifier?: string }>;
      exchangeCode: (code: string, codeVerifier: string) => Promise<{
        accessToken: string;
        refreshToken?: string;
        expiresIn: number;
        tokenType: string;
      }>;
      refreshTokens: (refreshToken: string) => Promise<{
        accessToken: string;
        refreshToken?: string;
        expiresIn: number;
        tokenType: string;
      }>;
    };

    beforeEach(async () => {
      const module = await import('../../../src/auth/middleware.js');
      AuthorizationCodeHandler = module.AuthorizationCodeHandler;
    });

    afterEach(() => {
      vi.restoreAllMocks();
    });

    it('should generate secure state parameter', () => {
      const handler = new AuthorizationCodeHandler({
        clientId: 'test-client',
        authorizationEndpoint: 'https://auth.example.com/authorize',
        tokenEndpoint: 'https://auth.example.com/token',
        redirectUri: 'https://app.example.com/callback',
      });

      const state1 = handler.generateState();
      const state2 = handler.generateState();

      // State should be 64 hex characters (32 bytes)
      expect(state1).toMatch(/^[a-f0-9]{64}$/);
      expect(state2).toMatch(/^[a-f0-9]{64}$/);
      expect(state1).not.toBe(state2);
    });

    it('should generate PKCE code verifier', () => {
      const handler = new AuthorizationCodeHandler({
        clientId: 'test-client',
        authorizationEndpoint: 'https://auth.example.com/authorize',
        tokenEndpoint: 'https://auth.example.com/token',
        redirectUri: 'https://app.example.com/callback',
      });

      const verifier = handler.generateCodeVerifier();

      // Code verifier should be URL-safe base64
      expect(verifier).toMatch(/^[A-Za-z0-9_-]+$/);
      expect(verifier.length).toBeGreaterThan(32);
    });

    it('should generate authorization URL with PKCE', async () => {
      const handler = new AuthorizationCodeHandler({
        clientId: 'test-client',
        authorizationEndpoint: 'https://auth.example.com/authorize',
        tokenEndpoint: 'https://auth.example.com/token',
        redirectUri: 'https://app.example.com/callback',
        scope: 'openid profile',
      });

      const { url, state, codeVerifier } = await handler.getAuthorizationUrl();

      expect(url).toContain('https://auth.example.com/authorize?');
      expect(url).toContain('response_type=code');
      expect(url).toContain('client_id=test-client');
      expect(url).toContain('redirect_uri=');
      expect(url).toContain('state=');
      expect(url).toContain('code_challenge=');
      expect(url).toContain('code_challenge_method=S256');
      expect(state).toBeDefined();
      expect(codeVerifier).toBeDefined();
    });

    it('should exchange authorization code for tokens', async () => {
      const handler = new AuthorizationCodeHandler({
        clientId: 'test-client',
        clientSecret: 'test-secret',
        authorizationEndpoint: 'https://auth.example.com/authorize',
        tokenEndpoint: 'https://auth.example.com/token',
        redirectUri: 'https://app.example.com/callback',
      });

      vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
        new Response(JSON.stringify({
          access_token: 'new-access-token',
          refresh_token: 'new-refresh-token',
          expires_in: 3600,
          token_type: 'Bearer',
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        })
      );

      const tokens = await handler.exchangeCode('auth-code-123', 'code-verifier-xyz');

      expect(tokens.accessToken).toBe('new-access-token');
      expect(tokens.refreshToken).toBe('new-refresh-token');
      expect(tokens.expiresIn).toBe(3600);
      expect(tokens.tokenType).toBe('Bearer');
    });

    it('should refresh tokens', async () => {
      const handler = new AuthorizationCodeHandler({
        clientId: 'test-client',
        authorizationEndpoint: 'https://auth.example.com/authorize',
        tokenEndpoint: 'https://auth.example.com/token',
        redirectUri: 'https://app.example.com/callback',
      });

      vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
        new Response(JSON.stringify({
          access_token: 'refreshed-access-token',
          refresh_token: 'refreshed-refresh-token',
          expires_in: 3600,
          token_type: 'Bearer',
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        })
      );

      const tokens = await handler.refreshTokens('old-refresh-token');

      expect(tokens.accessToken).toBe('refreshed-access-token');
      expect(tokens.refreshToken).toBe('refreshed-refresh-token');
    });

    it('should handle token exchange errors', async () => {
      const handler = new AuthorizationCodeHandler({
        clientId: 'test-client',
        authorizationEndpoint: 'https://auth.example.com/authorize',
        tokenEndpoint: 'https://auth.example.com/token',
        redirectUri: 'https://app.example.com/callback',
      });

      vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
        new Response(JSON.stringify({
          error: 'invalid_grant',
          error_description: 'Authorization code has expired',
        }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' },
        })
      );

      await expect(
        handler.exchangeCode('expired-code', 'verifier')
      ).rejects.toThrow('Authorization code has expired');
    });
  });

  describe('Role-Based Access Control (RBAC)', () => {
    let RBACManager: new (config: {
      roles: Array<{
        name: string;
        permissions: Array<{
          database: string;
          collection?: string;
          level: 'none' | 'read' | 'write' | 'admin';
        }>;
        inheritsFrom?: string[];
      }>;
      defaultRole?: string;
      enabled: boolean;
    }) => {
      getEffectivePermission: (user: { userId: string; roles: string[] }, database: string, collection?: string) => {
        allowed: boolean;
        effectivePermission: 'none' | 'read' | 'write' | 'admin';
        matchedRole?: string;
        reason?: string;
      };
      checkPermission: (
        user: { userId: string; roles: string[] },
        database: string,
        collection: string | undefined,
        requiredLevel: 'none' | 'read' | 'write' | 'admin'
      ) => {
        allowed: boolean;
        effectivePermission: 'none' | 'read' | 'write' | 'admin';
        matchedRole?: string;
        reason?: string;
      };
      canRead: (user: { userId: string; roles: string[] }, database: string, collection?: string) => {
        allowed: boolean;
        effectivePermission: 'none' | 'read' | 'write' | 'admin';
      };
      canWrite: (user: { userId: string; roles: string[] }, database: string, collection?: string) => {
        allowed: boolean;
        effectivePermission: 'none' | 'read' | 'write' | 'admin';
      };
      canAdmin: (user: { userId: string; roles: string[] }, database: string, collection?: string) => {
        allowed: boolean;
        effectivePermission: 'none' | 'read' | 'write' | 'admin';
      };
      listAccessibleResources: (user: { userId: string; roles: string[] }) => Array<{
        database: string;
        collection?: string;
        level: 'none' | 'read' | 'write' | 'admin';
      }>;
    };

    beforeEach(async () => {
      const module = await import('../../../src/auth/middleware.js');
      RBACManager = module.RBACManager;
    });

    it('should allow access when RBAC is disabled', () => {
      const manager = new RBACManager({
        roles: [],
        enabled: false,
      });

      const user = { userId: 'user-1', roles: [] };
      const result = manager.getEffectivePermission(user, 'testdb', 'testcol');

      expect(result.allowed).toBe(true);
      expect(result.effectivePermission).toBe('admin');
    });

    it('should check database-level permissions', () => {
      const manager = new RBACManager({
        roles: [
          {
            name: 'db-reader',
            permissions: [
              { database: 'testdb', level: 'read' },
            ],
          },
        ],
        enabled: true,
      });

      const user = { userId: 'user-1', roles: ['db-reader'] };

      const readResult = manager.canRead(user, 'testdb', 'anycollection');
      expect(readResult.allowed).toBe(true);

      const writeResult = manager.canWrite(user, 'testdb', 'anycollection');
      expect(writeResult.allowed).toBe(false);
    });

    it('should check collection-level permissions', () => {
      const manager = new RBACManager({
        roles: [
          {
            name: 'collection-writer',
            permissions: [
              { database: 'testdb', collection: 'users', level: 'write' },
              { database: 'testdb', collection: 'logs', level: 'read' },
            ],
          },
        ],
        enabled: true,
      });

      const user = { userId: 'user-1', roles: ['collection-writer'] };

      // Can write to users collection
      const writeUsersResult = manager.canWrite(user, 'testdb', 'users');
      expect(writeUsersResult.allowed).toBe(true);

      // Can only read logs collection
      const writeLogsResult = manager.canWrite(user, 'testdb', 'logs');
      expect(writeLogsResult.allowed).toBe(false);

      const readLogsResult = manager.canRead(user, 'testdb', 'logs');
      expect(readLogsResult.allowed).toBe(true);
    });

    it('should support wildcard permissions', () => {
      const manager = new RBACManager({
        roles: [
          {
            name: 'super-admin',
            permissions: [
              { database: '*', level: 'admin' },
            ],
          },
        ],
        enabled: true,
      });

      const user = { userId: 'user-1', roles: ['super-admin'] };

      expect(manager.canAdmin(user, 'anydb', 'anycol').allowed).toBe(true);
      expect(manager.canAdmin(user, 'otherdb', 'othercol').allowed).toBe(true);
    });

    it('should support role inheritance', () => {
      const manager = new RBACManager({
        roles: [
          {
            name: 'base-role',
            permissions: [
              { database: 'testdb', level: 'read' },
            ],
          },
          {
            name: 'extended-role',
            permissions: [
              { database: 'testdb', collection: 'special', level: 'write' },
            ],
            inheritsFrom: ['base-role'],
          },
        ],
        enabled: true,
      });

      const user = { userId: 'user-1', roles: ['extended-role'] };

      // Has inherited read permission
      expect(manager.canRead(user, 'testdb', 'regular').allowed).toBe(true);

      // Has own write permission for special collection
      expect(manager.canWrite(user, 'testdb', 'special').allowed).toBe(true);

      // Does not have write permission for regular collections
      expect(manager.canWrite(user, 'testdb', 'regular').allowed).toBe(false);
    });

    it('should use default role when user has no roles', () => {
      const manager = new RBACManager({
        roles: [
          {
            name: 'guest',
            permissions: [
              { database: 'public', level: 'read' },
            ],
          },
        ],
        defaultRole: 'guest',
        enabled: true,
      });

      const user = { userId: 'user-1', roles: [] };

      expect(manager.canRead(user, 'public', 'data').allowed).toBe(true);
      expect(manager.canRead(user, 'private', 'data').allowed).toBe(false);
    });

    it('should deny access when user has no matching permissions', () => {
      const manager = new RBACManager({
        roles: [
          {
            name: 'limited',
            permissions: [
              { database: 'allowed-db', level: 'read' },
            ],
          },
        ],
        enabled: true,
      });

      const user = { userId: 'user-1', roles: ['limited'] };

      const result = manager.canRead(user, 'forbidden-db', 'col');
      expect(result.allowed).toBe(false);
      expect(result.effectivePermission).toBe('none');
    });

    it('should list accessible resources for a user', () => {
      const manager = new RBACManager({
        roles: [
          {
            name: 'analyst',
            permissions: [
              { database: 'analytics', level: 'read' },
              { database: 'reports', collection: 'monthly', level: 'write' },
            ],
          },
        ],
        enabled: true,
      });

      const user = { userId: 'user-1', roles: ['analyst'] };
      const resources = manager.listAccessibleResources(user);

      expect(resources).toHaveLength(2);
      expect(resources).toContainEqual({ database: 'analytics', collection: undefined, level: 'read' });
      expect(resources).toContainEqual({ database: 'reports', collection: 'monthly', level: 'write' });
    });
  });

  describe('Audit Logging', () => {
    let InMemoryAuditLogger: new (maxEntries?: number) => {
      log: (entry: { eventType: string; timestamp: Date; userId?: string }) => void;
      getEntries: () => Array<{ eventType: string; timestamp: Date; userId?: string }>;
      getEntriesByType: (eventType: string) => Array<{ eventType: string; timestamp: Date }>;
      getEntriesByUser: (userId: string) => Array<{ eventType: string; userId: string }>;
      clear: () => void;
    };

    let ConsoleAuditLogger: new (prefix?: string) => {
      log: (entry: { eventType: string; timestamp: Date }) => void;
    };

    let CompositeAuditLogger: new (loggers: Array<{ log: (entry: unknown) => void | Promise<void> }>) => {
      log: (entry: unknown) => Promise<void>;
    };

    beforeEach(async () => {
      const module = await import('../../../src/auth/middleware.js');
      InMemoryAuditLogger = module.InMemoryAuditLogger;
      ConsoleAuditLogger = module.ConsoleAuditLogger;
      CompositeAuditLogger = module.CompositeAuditLogger;
    });

    it('should store audit entries in memory', () => {
      const logger = new InMemoryAuditLogger();

      logger.log({
        eventType: 'auth_success',
        timestamp: new Date(),
        userId: 'user-123',
      });

      logger.log({
        eventType: 'auth_failure',
        timestamp: new Date(),
      });

      const entries = logger.getEntries();
      expect(entries).toHaveLength(2);
    });

    it('should filter entries by type', () => {
      const logger = new InMemoryAuditLogger();

      logger.log({ eventType: 'auth_success', timestamp: new Date() });
      logger.log({ eventType: 'auth_failure', timestamp: new Date() });
      logger.log({ eventType: 'auth_success', timestamp: new Date() });

      const successEntries = logger.getEntriesByType('auth_success');
      expect(successEntries).toHaveLength(2);
    });

    it('should filter entries by user', () => {
      const logger = new InMemoryAuditLogger();

      logger.log({ eventType: 'auth_success', timestamp: new Date(), userId: 'user-1' });
      logger.log({ eventType: 'auth_success', timestamp: new Date(), userId: 'user-2' });
      logger.log({ eventType: 'auth_failure', timestamp: new Date(), userId: 'user-1' });

      const user1Entries = logger.getEntriesByUser('user-1');
      expect(user1Entries).toHaveLength(2);
    });

    it('should respect max entries limit', () => {
      const logger = new InMemoryAuditLogger(5);

      for (let i = 0; i < 10; i++) {
        logger.log({ eventType: 'auth_success', timestamp: new Date() });
      }

      expect(logger.getEntries()).toHaveLength(5);
    });

    it('should clear entries', () => {
      const logger = new InMemoryAuditLogger();

      logger.log({ eventType: 'auth_success', timestamp: new Date() });
      logger.log({ eventType: 'auth_failure', timestamp: new Date() });

      logger.clear();

      expect(logger.getEntries()).toHaveLength(0);
    });

    it('should log to console', () => {
      const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
      const logger = new ConsoleAuditLogger('[Test]');

      logger.log({
        eventType: 'auth_success',
        timestamp: new Date(),
      });

      expect(consoleSpy).toHaveBeenCalled();
      const logOutput = consoleSpy.mock.calls[0]?.[0] as string;
      expect(logOutput).toContain('[Test]');
      expect(logOutput).toContain('auth_success');

      consoleSpy.mockRestore();
    });

    it('should log to multiple destinations', async () => {
      const logger1 = new InMemoryAuditLogger();
      const logger2 = new InMemoryAuditLogger();

      const composite = new CompositeAuditLogger([logger1, logger2]);

      await composite.log({
        eventType: 'auth_success',
        timestamp: new Date(),
      });

      expect(logger1.getEntries()).toHaveLength(1);
      expect(logger2.getEntries()).toHaveLength(1);
    });
  });

  describe('AuthMiddleware with Audit Logging', () => {
    let createAuthMiddleware: (config: AuthConfig) => {
      authenticate: (req: Request) => Promise<{
        authenticated: boolean;
        user?: { userId: string };
        error?: { code: string };
      }>;
      checkPermission: (
        user: { userId: string; roles: string[] },
        database: string,
        collection: string | undefined,
        operation: 'read' | 'write' | 'admin',
        request?: Request
      ) => Promise<{ allowed: boolean }>;
      getRBACManager: () => unknown;
    };

    let InMemoryAuditLogger: new () => {
      log: (entry: { eventType: string; timestamp: Date }) => void;
      getEntries: () => Array<{ eventType: string }>;
      getEntriesByType: (eventType: string) => Array<{ eventType: string }>;
    };

    beforeEach(async () => {
      const module = await import('../../../src/auth/middleware.js');
      createAuthMiddleware = module.createAuthMiddleware;
      InMemoryAuditLogger = module.InMemoryAuditLogger;
    });

    it('should log successful authentication', async () => {
      const auditLogger = new InMemoryAuditLogger();

      const now = Math.floor(Date.now() / 1000);
      const token = await createHs256Token({
        sub: 'user_123',
        email: 'test@example.com',
        iat: now,
        exp: now + 3600,
      }, testSecret);

      const middleware = createAuthMiddleware({
        ...mockAuthConfig,
        jwtSecret: testSecret,
        auditLogger,
      });

      const request = new Request('https://api.example.com/test', {
        headers: { Authorization: `Bearer ${token}` },
      });

      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(true);

      const entries = auditLogger.getEntriesByType('auth_success');
      expect(entries).toHaveLength(1);
    });

    it('should log authentication failures', async () => {
      const auditLogger = new InMemoryAuditLogger();

      const middleware = createAuthMiddleware({
        ...mockAuthConfig,
        auditLogger,
      });

      const request = new Request('https://api.example.com/test');

      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(false);

      const entries = auditLogger.getEntriesByType('auth_failure');
      expect(entries).toHaveLength(1);
    });

    it('should integrate RBAC with middleware', async () => {
      const auditLogger = new InMemoryAuditLogger();

      const middleware = createAuthMiddleware({
        ...mockAuthConfig,
        rbac: {
          roles: [
            {
              name: 'reader',
              permissions: [{ database: 'testdb', level: 'read' }],
            },
          ],
          enabled: true,
        },
        auditLogger,
      });

      const rbacManager = middleware.getRBACManager();
      expect(rbacManager).toBeDefined();

      const user = { userId: 'user-1', roles: ['reader'] };

      const readResult = await middleware.checkPermission(user, 'testdb', 'testcol', 'read');
      expect(readResult.allowed).toBe(true);

      const writeResult = await middleware.checkPermission(user, 'testdb', 'testcol', 'write');
      expect(writeResult.allowed).toBe(false);

      const permissionEntries = auditLogger.getEntries().filter(
        (e) => e.eventType === 'permission_granted' || e.eventType === 'permission_denied'
      );
      expect(permissionEntries).toHaveLength(2);
    });
  });

  describe('Clock Tolerance', () => {
    let verifyJwt: (token: string, options?: {
      secret?: string;
      skipSignatureVerification?: boolean;
      clockTolerance?: number;
    }) => Promise<{
      valid: boolean;
      error?: { code: string };
    }>;

    beforeEach(async () => {
      const module = await import('../../../src/auth/middleware.js');
      verifyJwt = module.verifyJwt;
    });

    it('should handle clock skew with tolerance', async () => {
      const now = Math.floor(Date.now() / 1000);

      // Token that expired 30 seconds ago
      const token = await createHs256Token({
        sub: 'user_123',
        iat: now - 3600,
        exp: now - 30,
      }, testSecret);

      // Without tolerance, should fail
      const resultWithoutTolerance = await verifyJwt(token, {
        secret: testSecret,
        clockTolerance: 0,
      });
      expect(resultWithoutTolerance.valid).toBe(false);
      expect(resultWithoutTolerance.error?.code).toBe('TOKEN_EXPIRED');

      // With 60 second tolerance, should pass
      const resultWithTolerance = await verifyJwt(token, {
        secret: testSecret,
        clockTolerance: 60,
      });
      expect(resultWithTolerance.valid).toBe(true);
    });
  });
});
