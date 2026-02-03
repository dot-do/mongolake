/**
 * Auth Middleware Token Cache Tests
 *
 * Tests for token caching, LRU eviction, and cache configuration.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  type AuthConfig,
  type AuthResult,
  type TokenInfo,
  mockAuthConfig,
  mockValidToken,
  mockExpiredToken,
  mockUserContext,
} from './test-helpers.js';

describe('Auth Middleware - Token Cache', () => {
  let TokenCache: new (options: { maxSize: number; ttlSeconds: number; revokedTokenTTLMs?: number }) => {
    set: (token: string, info: TokenInfo) => void;
    get: (token: string) => TokenInfo | undefined;
    clear: () => void;
    clearRevocationList: () => void;
    invalidate: (token: string) => void;
    revoke: (token: string) => void;
    revokeAllForUser: (userId: string) => number;
    revokeByTokenId: (tokenId: string) => void;
    isRevoked: (token: string) => boolean;
    isRevokedByTokenId: (tokenId: string) => boolean;
    getRevokedCount: () => number;
    getRevokedTokenIds: () => string[];
    getRevokedUserIds: () => string[];
  };

  beforeEach(async () => {
    vi.useFakeTimers();
    const module = await import('../../../src/auth/middleware.js');
    TokenCache = module.TokenCache;
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should cache validated tokens for performance', async () => {
    const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

    const tokenInfo: TokenInfo = {
      token: mockValidToken,
      user: mockUserContext,
      expiresAt: Date.now() + 3600000,
      validatedAt: Date.now(),
    };

    cache.set(mockValidToken, tokenInfo);

    const cached = cache.get(mockValidToken);
    expect(cached).toBeDefined();
    expect(cached?.user.userId).toBe('user_123');

    cache.clear();
  });

  it('should return undefined for non-cached tokens', async () => {
    const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

    const cached = cache.get('non_existent_token');
    expect(cached).toBeUndefined();

    cache.clear();
  });

  it('should evict expired entries from cache', async () => {
    const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

    const expiredInfo: TokenInfo = {
      token: mockExpiredToken,
      user: mockUserContext,
      expiresAt: Date.now() - 1000, // Already expired
      validatedAt: Date.now() - 2000,
    };

    cache.set(mockExpiredToken, expiredInfo);

    const cached = cache.get(mockExpiredToken);
    expect(cached).toBeUndefined();

    cache.clear();
  });

  it('should respect max cache size with LRU eviction', async () => {
    const smallCache = new TokenCache({ maxSize: 2, ttlSeconds: 300 });

    smallCache.set('token1', { token: 'token1', user: mockUserContext, expiresAt: Date.now() + 3600000, validatedAt: Date.now() });
    smallCache.set('token2', { token: 'token2', user: mockUserContext, expiresAt: Date.now() + 3600000, validatedAt: Date.now() });

    // Access token1 to make it recently used
    smallCache.get('token1');

    // Add token3, should evict token2 (least recently used)
    smallCache.set('token3', { token: 'token3', user: mockUserContext, expiresAt: Date.now() + 3600000, validatedAt: Date.now() });

    expect(smallCache.get('token1')).toBeDefined();
    expect(smallCache.get('token2')).toBeUndefined();
    expect(smallCache.get('token3')).toBeDefined();

    smallCache.clear();
  });

  it('should invalidate specific token from cache', async () => {
    const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

    const tokenInfo: TokenInfo = {
      token: mockValidToken,
      user: mockUserContext,
      expiresAt: Date.now() + 3600000,
      validatedAt: Date.now(),
    };

    cache.set(mockValidToken, tokenInfo);
    expect(cache.get(mockValidToken)).toBeDefined();

    cache.invalidate(mockValidToken);
    expect(cache.get(mockValidToken)).toBeUndefined();

    cache.clear();
  });

  it('should skip cache validation when disabled', async () => {
    const module = await import('../../../src/auth/middleware.js');
    const createAuthMiddleware = module.createAuthMiddleware as (config: AuthConfig) => { authenticate: (req: Request) => Promise<AuthResult> };

    const middleware = createAuthMiddleware({
      ...mockAuthConfig,
      cacheEnabled: false,
    });

    const request = new Request('https://api.mongolake.com/db/test', {
      headers: {
        Authorization: `Bearer ${mockValidToken}`,
      },
    });

    // First request
    await middleware.authenticate(request);
    // Second request should still validate (not use cache)
    const result = await middleware.authenticate(request);

    expect(result.fromCache).toBeFalsy();
  });

  // ============================================================================
  // Token Revocation Tests
  // ============================================================================

  describe('Token Revocation', () => {
    it('should revoke a token and prevent retrieval', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      const tokenInfo: TokenInfo = {
        token: mockValidToken,
        user: mockUserContext,
        expiresAt: Date.now() + 3600000,
        validatedAt: Date.now(),
      };

      cache.set(mockValidToken, tokenInfo);
      expect(cache.get(mockValidToken)).toBeDefined();

      cache.revoke(mockValidToken);

      // Token should no longer be retrievable
      expect(cache.get(mockValidToken)).toBeUndefined();
      expect(cache.isRevoked(mockValidToken)).toBe(true);

      cache.clear();
    });

    it('should prevent re-caching of revoked tokens', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      const tokenInfo: TokenInfo = {
        token: mockValidToken,
        user: mockUserContext,
        expiresAt: Date.now() + 3600000,
        validatedAt: Date.now(),
      };

      // Revoke token first
      cache.revoke(mockValidToken);

      // Try to cache it
      cache.set(mockValidToken, tokenInfo);

      // Should still return undefined because token is revoked
      expect(cache.get(mockValidToken)).toBeUndefined();

      cache.clear();
    });

    it('should track revoked token count', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      expect(cache.getRevokedCount()).toBe(0);

      cache.revoke('token1');
      expect(cache.getRevokedCount()).toBe(1);

      cache.revoke('token2');
      expect(cache.getRevokedCount()).toBe(2);

      // Revoking the same token again should not increase count
      cache.revoke('token1');
      expect(cache.getRevokedCount()).toBe(2);

      cache.clear();
    });

    it('should expire revocation entries after TTL', async () => {
      // Use a very short TTL (10ms) for testing
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300, revokedTokenTTLMs: 10 });

      cache.revoke(mockValidToken);
      expect(cache.isRevoked(mockValidToken)).toBe(true);

      // Wait for revocation to expire
      vi.advanceTimersByTime(20);

      // Token should no longer be revoked after TTL expires
      expect(cache.isRevoked(mockValidToken)).toBe(false);

      cache.clear();
    });

    it('should allow caching after revocation TTL expires', async () => {
      // Use a very short TTL (10ms) for testing
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300, revokedTokenTTLMs: 10 });

      const tokenInfo: TokenInfo = {
        token: mockValidToken,
        user: mockUserContext,
        expiresAt: Date.now() + 3600000,
        validatedAt: Date.now(),
      };

      cache.revoke(mockValidToken);
      expect(cache.get(mockValidToken)).toBeUndefined();

      // Wait for revocation to expire
      vi.advanceTimersByTime(20);

      // Now token can be cached again
      cache.set(mockValidToken, tokenInfo);
      expect(cache.get(mockValidToken)).toBeDefined();

      cache.clear();
    });

    it('should not affect non-revoked tokens', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      const tokenInfo1: TokenInfo = {
        token: 'token1',
        user: mockUserContext,
        expiresAt: Date.now() + 3600000,
        validatedAt: Date.now(),
      };

      const tokenInfo2: TokenInfo = {
        token: 'token2',
        user: mockUserContext,
        expiresAt: Date.now() + 3600000,
        validatedAt: Date.now(),
      };

      cache.set('token1', tokenInfo1);
      cache.set('token2', tokenInfo2);

      // Revoke only token1
      cache.revoke('token1');

      // token2 should still be accessible
      expect(cache.get('token1')).toBeUndefined();
      expect(cache.get('token2')).toBeDefined();

      cache.clear();
    });

    it('should cleanup expired revocation entries', async () => {
      // Use a very short TTL (10ms) for testing
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300, revokedTokenTTLMs: 10 });

      cache.revoke('token1');
      cache.revoke('token2');
      expect(cache.getRevokedCount()).toBe(2);

      // Wait for revocations to expire
      vi.advanceTimersByTime(20);

      // Trigger cleanup by revoking another token
      cache.revoke('token3');

      // Old entries should be cleaned up, only token3 should remain
      expect(cache.getRevokedCount()).toBe(1);

      cache.clear();
    });

    it('should revoke all tokens for a specific user', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      const user1Context = { ...mockUserContext, userId: 'user_1' };
      const user2Context = { ...mockUserContext, userId: 'user_2' };

      // Add multiple tokens for user_1
      cache.set('token1_user1', {
        token: 'token1_user1',
        user: user1Context,
        expiresAt: Date.now() + 3600000,
        validatedAt: Date.now(),
      });
      cache.set('token2_user1', {
        token: 'token2_user1',
        user: user1Context,
        expiresAt: Date.now() + 3600000,
        validatedAt: Date.now(),
      });

      // Add token for user_2
      cache.set('token1_user2', {
        token: 'token1_user2',
        user: user2Context,
        expiresAt: Date.now() + 3600000,
        validatedAt: Date.now(),
      });

      // All tokens should be accessible
      expect(cache.get('token1_user1')).toBeDefined();
      expect(cache.get('token2_user1')).toBeDefined();
      expect(cache.get('token1_user2')).toBeDefined();

      // Revoke all tokens for user_1
      const revokedCount = cache.revokeAllForUser('user_1');

      // Should return count of revoked tokens
      expect(revokedCount).toBe(2);

      // User_1's tokens should be revoked
      expect(cache.get('token1_user1')).toBeUndefined();
      expect(cache.get('token2_user1')).toBeUndefined();
      expect(cache.isRevoked('token1_user1')).toBe(true);
      expect(cache.isRevoked('token2_user1')).toBe(true);

      // User_2's token should still be accessible
      expect(cache.get('token1_user2')).toBeDefined();
      expect(cache.isRevoked('token1_user2')).toBe(false);

      cache.clear();
    });

    it('should return 0 when revoking tokens for non-existent user', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      const revokedCount = cache.revokeAllForUser('non_existent_user');
      expect(revokedCount).toBe(0);

      cache.clear();
    });

    it('should prevent re-caching of tokens revoked via revokeAllForUser', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      const userContext = { ...mockUserContext, userId: 'user_1' };

      // Add token
      cache.set('token1', {
        token: 'token1',
        user: userContext,
        expiresAt: Date.now() + 3600000,
        validatedAt: Date.now(),
      });

      // Revoke all tokens for user
      cache.revokeAllForUser('user_1');

      // Try to re-cache the revoked token
      cache.set('token1', {
        token: 'token1',
        user: userContext,
        expiresAt: Date.now() + 3600000,
        validatedAt: Date.now(),
      });

      // Token should still be revoked and not retrievable
      expect(cache.get('token1')).toBeUndefined();
      expect(cache.isRevoked('token1')).toBe(true);

      cache.clear();
    });

    it('should handle revoking user tokens with TTL expiration', async () => {
      // Use a very short TTL (10ms) for testing
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300, revokedTokenTTLMs: 10 });

      const userContext = { ...mockUserContext, userId: 'user_1' };

      // Add tokens
      cache.set('token1', {
        token: 'token1',
        user: userContext,
        expiresAt: Date.now() + 3600000,
        validatedAt: Date.now(),
      });
      cache.set('token2', {
        token: 'token2',
        user: userContext,
        expiresAt: Date.now() + 3600000,
        validatedAt: Date.now(),
      });

      // Revoke all tokens for user
      cache.revokeAllForUser('user_1');
      expect(cache.isRevoked('token1')).toBe(true);
      expect(cache.isRevoked('token2')).toBe(true);

      // Wait for revocation TTL to expire
      vi.advanceTimersByTime(20);

      // Revocations should have expired
      expect(cache.isRevoked('token1')).toBe(false);
      expect(cache.isRevoked('token2')).toBe(false);

      cache.clear();
    });
  });
});
