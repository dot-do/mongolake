/**
 * Token Revocation Tests
 *
 * RED phase tests for token revocation functionality in auth cache.
 * Tests cover:
 * - Revoked tokens being rejected even if in cache
 * - revokeToken(tokenId) adding to revocation list
 * - isRevoked(tokenId) returning true for revoked tokens
 * - cache.get() returning undefined for revoked tokens
 * - Revocation list persistence across cache operations
 * - Bulk revocation: revokeAllForUser(userId)
 * - Revocation expiry (cleanup of old revocations)
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  type TokenInfo,
  mockValidToken,
  mockUserContext,
} from './test-helpers.js';

describe('Token Revocation', () => {
  let TokenCache: new (options: {
    maxSize: number;
    ttlSeconds: number;
    revokedTokenTTLMs?: number;
  }) => {
    set: (token: string, info: TokenInfo) => void;
    get: (token: string) => TokenInfo | undefined;
    clear: () => void;
    invalidate: (token: string) => void;
    revoke: (token: string) => void;
    revokeByTokenId: (tokenId: string) => void;
    revokeAllForUser: (userId: string) => number;
    isRevoked: (token: string) => boolean;
    isRevokedByTokenId: (tokenId: string) => boolean;
    getRevokedCount: () => number;
    getRevokedTokenIds: () => string[];
    getRevokedUserIds: () => string[];
    exportRevocationList: () => { tokens: string[]; tokenIds: string[]; userIds: string[] };
    importRevocationList: (list: { tokens?: string[]; tokenIds?: string[]; userIds?: string[] }) => void;
    clearRevocationList: () => void;
  };

  beforeEach(async () => {
    vi.useFakeTimers();
    const module = await import('../../../src/auth/middleware.js');
    TokenCache = module.TokenCache;
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  // ============================================================================
  // Basic Revocation Tests
  // ============================================================================

  describe('Basic Token Revocation', () => {
    it('should reject revoked tokens even if they exist in cache', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      const tokenInfo: TokenInfo = {
        token: mockValidToken,
        user: mockUserContext,
        expiresAt: Date.now() + 3600000,
        validatedAt: Date.now(),
      };

      // Add token to cache
      cache.set(mockValidToken, tokenInfo);
      expect(cache.get(mockValidToken)).toBeDefined();

      // Revoke the token
      cache.revoke(mockValidToken);

      // Token should now be rejected even though it was cached
      expect(cache.get(mockValidToken)).toBeUndefined();
      expect(cache.isRevoked(mockValidToken)).toBe(true);

      cache.clear();
    });

    it('should prevent adding revoked tokens back to cache', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      const tokenInfo: TokenInfo = {
        token: mockValidToken,
        user: mockUserContext,
        expiresAt: Date.now() + 3600000,
        validatedAt: Date.now(),
      };

      // Revoke token first
      cache.revoke(mockValidToken);

      // Try to add it to cache
      cache.set(mockValidToken, tokenInfo);

      // Should still return undefined
      expect(cache.get(mockValidToken)).toBeUndefined();
      expect(cache.isRevoked(mockValidToken)).toBe(true);

      cache.clear();
    });

    it('should handle revoking a token that was never cached', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      // Revoke a token that was never in cache
      cache.revoke('never_cached_token');

      expect(cache.isRevoked('never_cached_token')).toBe(true);
      expect(cache.get('never_cached_token')).toBeUndefined();

      cache.clear();
    });
  });

  // ============================================================================
  // Token ID Based Revocation Tests
  // ============================================================================

  describe('Token ID Revocation (revokeByTokenId)', () => {
    it('should revoke token by its JWT ID (jti claim)', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      const tokenId = 'jti_abc123';

      // Revoke by token ID
      cache.revokeByTokenId(tokenId);

      // Check revocation status
      expect(cache.isRevokedByTokenId(tokenId)).toBe(true);

      cache.clear();
    });

    it('should reject tokens with revoked token IDs', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      const tokenId = 'jti_xyz789';
      const tokenInfo: TokenInfo = {
        token: mockValidToken,
        user: {
          ...mockUserContext,
          metadata: { jti: tokenId },
        },
        expiresAt: Date.now() + 3600000,
        validatedAt: Date.now(),
      };

      // Add token to cache
      cache.set(mockValidToken, tokenInfo);

      // Revoke by token ID
      cache.revokeByTokenId(tokenId);

      // Token should now be rejected
      expect(cache.get(mockValidToken)).toBeUndefined();

      cache.clear();
    });

    it('should track multiple revoked token IDs', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      cache.revokeByTokenId('jti_1');
      cache.revokeByTokenId('jti_2');
      cache.revokeByTokenId('jti_3');

      expect(cache.isRevokedByTokenId('jti_1')).toBe(true);
      expect(cache.isRevokedByTokenId('jti_2')).toBe(true);
      expect(cache.isRevokedByTokenId('jti_3')).toBe(true);
      expect(cache.isRevokedByTokenId('jti_4')).toBe(false);

      cache.clear();
    });

    it('should not duplicate token IDs when revoking the same ID twice', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      cache.revokeByTokenId('jti_duplicate');
      cache.revokeByTokenId('jti_duplicate');

      const revokedIds = cache.getRevokedTokenIds();
      const duplicateCount = revokedIds.filter((id) => id === 'jti_duplicate').length;

      expect(duplicateCount).toBe(1);

      cache.clear();
    });

    it('should return list of revoked token IDs', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      cache.revokeByTokenId('jti_a');
      cache.revokeByTokenId('jti_b');

      const revokedIds = cache.getRevokedTokenIds();

      expect(revokedIds).toContain('jti_a');
      expect(revokedIds).toContain('jti_b');
      expect(revokedIds.length).toBe(2);

      cache.clear();
    });
  });

  // ============================================================================
  // Bulk User Revocation Tests
  // ============================================================================

  describe('Bulk User Revocation (revokeAllForUser)', () => {
    it('should revoke all tokens for a specific user', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      const userId = 'user_to_revoke';
      const token1Info: TokenInfo = {
        token: 'token1_user',
        user: { ...mockUserContext, userId },
        expiresAt: Date.now() + 3600000,
        validatedAt: Date.now(),
      };
      const token2Info: TokenInfo = {
        token: 'token2_user',
        user: { ...mockUserContext, userId },
        expiresAt: Date.now() + 3600000,
        validatedAt: Date.now(),
      };
      const otherUserToken: TokenInfo = {
        token: 'token_other_user',
        user: { ...mockUserContext, userId: 'other_user' },
        expiresAt: Date.now() + 3600000,
        validatedAt: Date.now(),
      };

      cache.set('token1_user', token1Info);
      cache.set('token2_user', token2Info);
      cache.set('token_other_user', otherUserToken);

      // Revoke all tokens for the user
      const revokedCount = cache.revokeAllForUser(userId);

      // Should have revoked 2 tokens
      expect(revokedCount).toBe(2);

      // User's tokens should be rejected
      expect(cache.get('token1_user')).toBeUndefined();
      expect(cache.get('token2_user')).toBeUndefined();

      // Other user's token should still work
      expect(cache.get('token_other_user')).toBeDefined();

      cache.clear();
    });

    it('should prevent new tokens from revoked user from being cached', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      const userId = 'banned_user';

      // Revoke all tokens for the user
      cache.revokeAllForUser(userId);

      // Try to add a new token for this user
      const newTokenInfo: TokenInfo = {
        token: 'new_token_banned',
        user: { ...mockUserContext, userId },
        expiresAt: Date.now() + 3600000,
        validatedAt: Date.now(),
      };

      cache.set('new_token_banned', newTokenInfo);

      // Should be rejected because user is revoked
      expect(cache.get('new_token_banned')).toBeUndefined();

      cache.clear();
    });

    it('should return 0 when revoking user with no cached tokens', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      const revokedCount = cache.revokeAllForUser('non_existent_user');

      expect(revokedCount).toBe(0);

      cache.clear();
    });

    it('should track revoked user IDs', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      cache.revokeAllForUser('user_1');
      cache.revokeAllForUser('user_2');

      const revokedUserIds = cache.getRevokedUserIds();

      expect(revokedUserIds).toContain('user_1');
      expect(revokedUserIds).toContain('user_2');

      cache.clear();
    });

    it('should not duplicate user IDs when revoking the same user twice', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      cache.revokeAllForUser('duplicate_user');
      cache.revokeAllForUser('duplicate_user');

      const revokedUserIds = cache.getRevokedUserIds();
      const duplicateCount = revokedUserIds.filter((id) => id === 'duplicate_user').length;

      expect(duplicateCount).toBe(1);

      cache.clear();
    });
  });

  // ============================================================================
  // Revocation Expiry Tests
  // ============================================================================

  describe('Revocation Expiry', () => {
    it('should expire token revocations after TTL', async () => {
      const cache = new TokenCache({
        maxSize: 100,
        ttlSeconds: 300,
        revokedTokenTTLMs: 50,
      });

      cache.revoke('expiring_token');
      expect(cache.isRevoked('expiring_token')).toBe(true);

      // Wait for expiry using fake timers
      vi.advanceTimersByTime(60);

      // Revocation should have expired
      expect(cache.isRevoked('expiring_token')).toBe(false);

      cache.clear();
    });

    it('should expire token ID revocations after TTL', async () => {
      const cache = new TokenCache({
        maxSize: 100,
        ttlSeconds: 300,
        revokedTokenTTLMs: 50,
      });

      cache.revokeByTokenId('expiring_jti');
      expect(cache.isRevokedByTokenId('expiring_jti')).toBe(true);

      // Wait for expiry using fake timers
      vi.advanceTimersByTime(60);

      // Revocation should have expired
      expect(cache.isRevokedByTokenId('expiring_jti')).toBe(false);

      cache.clear();
    });

    it('should expire user revocations after TTL', async () => {
      const cache = new TokenCache({
        maxSize: 100,
        ttlSeconds: 300,
        revokedTokenTTLMs: 50,
      });

      cache.revokeAllForUser('expiring_user');

      // User tokens should be blocked initially
      const tokenInfo: TokenInfo = {
        token: 'user_token',
        user: { ...mockUserContext, userId: 'expiring_user' },
        expiresAt: Date.now() + 3600000,
        validatedAt: Date.now(),
      };
      cache.set('user_token', tokenInfo);
      expect(cache.get('user_token')).toBeUndefined();

      // Wait for expiry using fake timers
      vi.advanceTimersByTime(60);

      // Now the token should be cacheable again
      cache.set('user_token', tokenInfo);
      expect(cache.get('user_token')).toBeDefined();

      cache.clear();
    });

    it('should automatically cleanup expired revocations on revoke operations', async () => {
      const cache = new TokenCache({
        maxSize: 100,
        ttlSeconds: 300,
        revokedTokenTTLMs: 50,
      });

      // Add some revocations
      cache.revoke('old_token_1');
      cache.revoke('old_token_2');
      expect(cache.getRevokedCount()).toBe(2);

      // Wait for them to expire using fake timers
      vi.advanceTimersByTime(60);

      // Trigger cleanup by adding a new revocation
      cache.revoke('new_token');

      // Old revocations should be cleaned up
      expect(cache.getRevokedCount()).toBe(1);
      expect(cache.isRevoked('new_token')).toBe(true);
      expect(cache.isRevoked('old_token_1')).toBe(false);
      expect(cache.isRevoked('old_token_2')).toBe(false);

      cache.clear();
    });

    it('should allow recaching tokens after revocation expires', async () => {
      const cache = new TokenCache({
        maxSize: 100,
        ttlSeconds: 300,
        revokedTokenTTLMs: 50,
      });

      const tokenInfo: TokenInfo = {
        token: 'recyclable_token',
        user: mockUserContext,
        expiresAt: Date.now() + 3600000,
        validatedAt: Date.now(),
      };

      cache.set('recyclable_token', tokenInfo);
      cache.revoke('recyclable_token');
      expect(cache.get('recyclable_token')).toBeUndefined();

      // Wait for revocation to expire using fake timers
      vi.advanceTimersByTime(60);

      // Should be able to cache again
      cache.set('recyclable_token', tokenInfo);
      expect(cache.get('recyclable_token')).toBeDefined();

      cache.clear();
    });
  });

  // ============================================================================
  // Revocation List Persistence Tests
  // ============================================================================

  describe('Revocation List Persistence', () => {
    it('should export complete revocation list', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      cache.revoke('token_1');
      cache.revoke('token_2');
      cache.revokeByTokenId('jti_1');
      cache.revokeAllForUser('user_1');

      const revocationList = cache.exportRevocationList();

      expect(revocationList.tokens).toContain('token_1');
      expect(revocationList.tokens).toContain('token_2');
      expect(revocationList.tokenIds).toContain('jti_1');
      expect(revocationList.userIds).toContain('user_1');

      cache.clear();
    });

    it('should import revocation list', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      const revocationList = {
        tokens: ['imported_token_1', 'imported_token_2'],
        tokenIds: ['imported_jti_1'],
        userIds: ['imported_user_1'],
      };

      cache.importRevocationList(revocationList);

      expect(cache.isRevoked('imported_token_1')).toBe(true);
      expect(cache.isRevoked('imported_token_2')).toBe(true);
      expect(cache.isRevokedByTokenId('imported_jti_1')).toBe(true);
      expect(cache.getRevokedUserIds()).toContain('imported_user_1');

      cache.clear();
    });

    it('should merge imported revocations with existing ones', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      // Add existing revocations
      cache.revoke('existing_token');
      cache.revokeByTokenId('existing_jti');

      // Import additional revocations
      cache.importRevocationList({
        tokens: ['imported_token'],
        tokenIds: ['imported_jti'],
      });

      // Both should be present
      expect(cache.isRevoked('existing_token')).toBe(true);
      expect(cache.isRevoked('imported_token')).toBe(true);
      expect(cache.isRevokedByTokenId('existing_jti')).toBe(true);
      expect(cache.isRevokedByTokenId('imported_jti')).toBe(true);

      cache.clear();
    });

    it('should persist revocations across cache clear operations', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      cache.revoke('persistent_token');
      cache.revokeByTokenId('persistent_jti');
      cache.revokeAllForUser('persistent_user');

      // Clear the token cache (but not revocations)
      cache.clear();

      // Revocations should persist
      expect(cache.isRevoked('persistent_token')).toBe(true);
      expect(cache.isRevokedByTokenId('persistent_jti')).toBe(true);
      expect(cache.getRevokedUserIds()).toContain('persistent_user');
    });

    it('should export empty revocation list when nothing is revoked', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      const revocationList = cache.exportRevocationList();

      expect(revocationList.tokens).toEqual([]);
      expect(revocationList.tokenIds).toEqual([]);
      expect(revocationList.userIds).toEqual([]);

      cache.clear();
    });

    it('should handle importing empty revocation list', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      cache.revoke('existing_token');

      cache.importRevocationList({
        tokens: [],
        tokenIds: [],
        userIds: [],
      });

      // Existing revocations should remain
      expect(cache.isRevoked('existing_token')).toBe(true);

      cache.clear();
    });

    it('should allow clearing the revocation list explicitly', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      cache.revoke('token_to_clear');
      cache.revokeByTokenId('jti_to_clear');
      cache.revokeAllForUser('user_to_clear');

      cache.clearRevocationList();

      expect(cache.isRevoked('token_to_clear')).toBe(false);
      expect(cache.isRevokedByTokenId('jti_to_clear')).toBe(false);
      expect(cache.getRevokedUserIds()).not.toContain('user_to_clear');
      expect(cache.getRevokedCount()).toBe(0);

      cache.clear();
    });

    it('should support partial import (only tokens)', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      cache.importRevocationList({
        tokens: ['partial_token'],
      });

      expect(cache.isRevoked('partial_token')).toBe(true);

      cache.clear();
    });

    it('should support partial import (only tokenIds)', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      cache.importRevocationList({
        tokenIds: ['partial_jti'],
      });

      expect(cache.isRevokedByTokenId('partial_jti')).toBe(true);

      cache.clear();
    });

    it('should support partial import (only userIds)', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      cache.importRevocationList({
        userIds: ['partial_user'],
      });

      expect(cache.getRevokedUserIds()).toContain('partial_user');

      cache.clear();
    });
  });

  // ============================================================================
  // Edge Cases and Error Handling
  // ============================================================================

  describe('Edge Cases', () => {
    it('should handle empty token string for revocation', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      // Should not throw
      cache.revoke('');
      expect(cache.isRevoked('')).toBe(true);

      cache.clear();
    });

    it('should handle very long token strings', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      const longToken = 'x'.repeat(10000);
      cache.revoke(longToken);

      expect(cache.isRevoked(longToken)).toBe(true);

      cache.clear();
    });

    it('should handle special characters in token IDs', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      const specialTokenId = 'jti_with-special.chars/and\\slashes';
      cache.revokeByTokenId(specialTokenId);

      expect(cache.isRevokedByTokenId(specialTokenId)).toBe(true);

      cache.clear();
    });

    it('should handle concurrent revocations', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      // Simulate concurrent revocations
      const promises = [];
      for (let i = 0; i < 100; i++) {
        promises.push(Promise.resolve().then(() => cache.revoke(`concurrent_token_${i}`)));
      }

      await Promise.all(promises);

      // All should be revoked
      for (let i = 0; i < 100; i++) {
        expect(cache.isRevoked(`concurrent_token_${i}`)).toBe(true);
      }

      cache.clear();
    });

    it('should handle maximum revocation list size gracefully', async () => {
      const cache = new TokenCache({ maxSize: 100, ttlSeconds: 300 });

      // Add many revocations (should not cause memory issues)
      for (let i = 0; i < 1000; i++) {
        cache.revoke(`mass_revoke_${i}`);
      }

      // Should still function correctly
      expect(cache.isRevoked('mass_revoke_0')).toBe(true);
      expect(cache.isRevoked('mass_revoke_999')).toBe(true);
      expect(cache.getRevokedCount()).toBe(1000);

      cache.clear();
    });
  });
});
