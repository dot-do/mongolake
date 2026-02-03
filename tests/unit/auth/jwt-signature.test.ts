/**
 * Auth Middleware JWT Signature Verification Tests
 *
 * Tests for JWT signature verification and algorithm handling.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  type AuthConfig,
  type AuthResult,
  mockAuthConfig,
  mockValidToken,
  testSecret,
  createHs256Token,
} from './test-helpers.js';

describe('Auth Middleware - JWT Signature Verification', () => {
  let verifyJwt: (token: string, options?: {
    secret?: string;
    publicKey?: string;
    skipSignatureVerification?: boolean;
    clockTolerance?: number;
  }) => Promise<{
    valid: boolean;
    header?: { alg: string; typ?: string };
    claims?: Record<string, unknown>;
    error?: { code: string; message: string };
  }>;

  beforeEach(async () => {
    const module = await import('../../../src/auth/middleware.js');
    verifyJwt = module.verifyJwt;
  });

  describe('verifyJwt function', () => {
    it('should verify a valid HS256 token', async () => {
      const now = Math.floor(Date.now() / 1000);
      const token = await createHs256Token({
        sub: 'user_123',
        email: 'test@example.com',
        iat: now,
        exp: now + 3600,
      }, testSecret);

      const result = await verifyJwt(token, { secret: testSecret });

      expect(result.valid).toBe(true);
      expect(result.claims?.sub).toBe('user_123');
      expect(result.claims?.email).toBe('test@example.com');
      expect(result.header?.alg).toBe('HS256');
    });

    it('should reject token with invalid signature', async () => {
      const now = Math.floor(Date.now() / 1000);
      const token = await createHs256Token({
        sub: 'user_123',
        iat: now,
        exp: now + 3600,
      }, testSecret);

      // Verify with wrong secret
      const result = await verifyJwt(token, { secret: 'wrong-secret' });

      expect(result.valid).toBe(false);
      expect(result.error?.code).toBe('INVALID_SIGNATURE');
    });

    it('should reject tampered token', async () => {
      const now = Math.floor(Date.now() / 1000);
      const token = await createHs256Token({
        sub: 'user_123',
        iat: now,
        exp: now + 3600,
      }, testSecret);

      // Tamper with the payload by changing the user ID
      const parts = token.split('.');
      const tamperedPayload = btoa(JSON.stringify({
        sub: 'attacker_456',  // Changed!
        iat: now,
        exp: now + 3600,
      })).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');
      const tamperedToken = `${parts[0]}.${tamperedPayload}.${parts[2]}`;

      const result = await verifyJwt(tamperedToken, { secret: testSecret });

      expect(result.valid).toBe(false);
      expect(result.error?.code).toBe('INVALID_SIGNATURE');
    });

    it('should reject expired token', async () => {
      const now = Math.floor(Date.now() / 1000);
      const token = await createHs256Token({
        sub: 'user_123',
        iat: now - 7200,
        exp: now - 3600,  // Expired 1 hour ago
      }, testSecret);

      const result = await verifyJwt(token, { secret: testSecret });

      expect(result.valid).toBe(false);
      expect(result.error?.code).toBe('TOKEN_EXPIRED');
    });

    it('should reject token that is not yet valid (nbf claim)', async () => {
      const now = Math.floor(Date.now() / 1000);
      const token = await createHs256Token({
        sub: 'user_123',
        iat: now,
        nbf: now + 3600,  // Not valid until 1 hour from now
        exp: now + 7200,
      }, testSecret);

      const result = await verifyJwt(token, { secret: testSecret });

      expect(result.valid).toBe(false);
      expect(result.error?.code).toBe('TOKEN_NOT_YET_VALID');
    });

    it('should respect clock tolerance for expiration', async () => {
      const now = Math.floor(Date.now() / 1000);
      const token = await createHs256Token({
        sub: 'user_123',
        iat: now - 3600,
        exp: now - 10,  // Expired 10 seconds ago
      }, testSecret);

      // Without tolerance, should fail
      const resultNoTolerance = await verifyJwt(token, { secret: testSecret, clockTolerance: 0 });
      expect(resultNoTolerance.valid).toBe(false);

      // With 60 second tolerance, should pass
      const resultWithTolerance = await verifyJwt(token, { secret: testSecret, clockTolerance: 60 });
      expect(resultWithTolerance.valid).toBe(true);
    });

    it('should respect clock tolerance for nbf claim', async () => {
      const now = Math.floor(Date.now() / 1000);
      const token = await createHs256Token({
        sub: 'user_123',
        iat: now,
        nbf: now + 10,  // Valid in 10 seconds
        exp: now + 3600,
      }, testSecret);

      // Without tolerance, should fail
      const resultNoTolerance = await verifyJwt(token, { secret: testSecret, clockTolerance: 0 });
      expect(resultNoTolerance.valid).toBe(false);

      // With 60 second tolerance, should pass
      const resultWithTolerance = await verifyJwt(token, { secret: testSecret, clockTolerance: 60 });
      expect(resultWithTolerance.valid).toBe(true);
    });

    it('should reject algorithm "none"', async () => {
      // Manually create a token with alg: none
      const header = { alg: 'none', typ: 'JWT' };
      const payload = { sub: 'user_123', exp: Math.floor(Date.now() / 1000) + 3600 };
      const headerB64 = btoa(JSON.stringify(header)).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');
      const payloadB64 = btoa(JSON.stringify(payload)).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');
      const token = `${headerB64}.${payloadB64}.`;

      const result = await verifyJwt(token, { secret: testSecret });

      expect(result.valid).toBe(false);
      expect(result.error?.code).toBe('INSECURE_ALGORITHM');
    });

    it('should reject unsupported algorithms', async () => {
      // Create token with HS384 algorithm (not supported)
      const header = { alg: 'HS384', typ: 'JWT' };
      const payload = { sub: 'user_123', exp: Math.floor(Date.now() / 1000) + 3600 };
      const headerB64 = btoa(JSON.stringify(header)).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');
      const payloadB64 = btoa(JSON.stringify(payload)).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');
      const token = `${headerB64}.${payloadB64}.fake-signature`;

      const result = await verifyJwt(token, { secret: testSecret });

      expect(result.valid).toBe(false);
      expect(result.error?.code).toBe('UNSUPPORTED_ALGORITHM');
    });

    it('should require secret for HS256', async () => {
      const now = Math.floor(Date.now() / 1000);
      const token = await createHs256Token({
        sub: 'user_123',
        exp: now + 3600,
      }, testSecret);

      const result = await verifyJwt(token, {}); // No secret provided

      expect(result.valid).toBe(false);
      expect(result.error?.code).toBe('MISSING_SECRET');
    });

    it('should allow skipping signature verification for testing', async () => {
      const now = Math.floor(Date.now() / 1000);
      const token = await createHs256Token({
        sub: 'user_123',
        exp: now + 3600,
      }, testSecret);

      // Verify with wrong secret but skip verification
      const result = await verifyJwt(token, {
        secret: 'wrong-secret',
        skipSignatureVerification: true,
      });

      expect(result.valid).toBe(true);
      expect(result.claims?.sub).toBe('user_123');
    });

    it('should reject malformed token (wrong number of parts)', async () => {
      const result = await verifyJwt('only.two', { secret: testSecret });

      expect(result.valid).toBe(false);
      expect(result.error?.code).toBe('INVALID_TOKEN_FORMAT');
    });

    it('should reject token with invalid JSON in header', async () => {
      const invalidHeader = btoa('not-json').replace(/=/g, '');
      const payload = btoa(JSON.stringify({ sub: 'user' })).replace(/=/g, '');
      const token = `${invalidHeader}.${payload}.signature`;

      const result = await verifyJwt(token, { secret: testSecret });

      expect(result.valid).toBe(false);
      expect(result.error?.code).toBe('INVALID_TOKEN_HEADER');
    });

    it('should reject token with invalid JSON in payload', async () => {
      const header = btoa(JSON.stringify({ alg: 'HS256' })).replace(/=/g, '');
      const invalidPayload = btoa('not-json').replace(/=/g, '');
      const token = `${header}.${invalidPayload}.signature`;

      const result = await verifyJwt(token, { secret: testSecret });

      expect(result.valid).toBe(false);
      expect(result.error?.code).toBe('INVALID_TOKEN_PAYLOAD');
    });
  });

  describe('middleware with signature verification', () => {
    let createAuthMiddleware: (config: AuthConfig) => { authenticate: (req: Request) => Promise<AuthResult> };

    beforeEach(async () => {
      const module = await import('../../../src/auth/middleware.js');
      createAuthMiddleware = module.createAuthMiddleware;
    });

    it('should verify JWT signature when jwtSecret is configured', async () => {
      const now = Math.floor(Date.now() / 1000);
      const validToken = await createHs256Token({
        sub: 'user_123',
        email: 'test@example.com',
        iat: now,
        exp: now + 3600,
      }, testSecret);

      const middleware = createAuthMiddleware({
        ...mockAuthConfig,
        jwtSecret: testSecret,
      });

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: {
          Authorization: `Bearer ${validToken}`,
        },
      });

      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(true);
      expect(result.user?.userId).toBe('user_123');
    });

    it('should reject forged tokens when jwtSecret is configured', async () => {
      const now = Math.floor(Date.now() / 1000);
      // Create token with different secret (simulating an attacker)
      const forgedToken = await createHs256Token({
        sub: 'attacker_456',
        email: 'attacker@evil.com',
        iat: now,
        exp: now + 3600,
      }, 'attacker-secret');

      const middleware = createAuthMiddleware({
        ...mockAuthConfig,
        jwtSecret: testSecret,
      });

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: {
          Authorization: `Bearer ${forgedToken}`,
        },
      });

      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(false);
      expect(result.error?.code).toBe('INVALID_SIGNATURE');
    });

    it('should skip signature verification when skipSignatureVerification is true', async () => {
      const middleware = createAuthMiddleware({
        ...mockAuthConfig,
        jwtSecret: testSecret,
        skipSignatureVerification: true,
      });

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: {
          Authorization: `Bearer ${mockValidToken}`, // Uses mock token with fake signature
        },
      });

      const result = await middleware.authenticate(request);

      expect(result.authenticated).toBe(true);
    });

    it('should not require signature verification when no secret/key is configured', async () => {
      const middleware = createAuthMiddleware({
        ...mockAuthConfig,
        // No jwtSecret or jwtPublicKey
      });

      const request = new Request('https://api.mongolake.com/db/test', {
        headers: {
          Authorization: `Bearer ${mockValidToken}`,
        },
      });

      const result = await middleware.authenticate(request);

      // Should still work with legacy behavior (no signature verification)
      expect(result.authenticated).toBe(true);
    });
  });
});
