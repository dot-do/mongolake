/**
 * Token Cache
 *
 * LRU cache for validated JWT tokens with TTL support and revocation tracking.
 */

import {
  DEFAULT_CACHE_MAX_SIZE,
  DEFAULT_CACHE_TTL_SECONDS,
} from '../constants.js';
import { LRUCache } from '../utils/lru-cache.js';
import type { TokenInfo } from './types.js';

// ============================================================================
// TokenCache
// ============================================================================

/**
 * LRU cache for validated JWT tokens with TTL support.
 *
 * Features:
 * - Stores validated token info for quick lookups
 * - Checks both token expiration and cache TTL on retrieval
 * - Evicts least-recently-used entries when capacity is reached
 * - Uses the generic LRUCache implementation with token-specific expiration checks
 * - Supports token revocation with TTL-based cleanup
 */
export class TokenCache {
  private cache: LRUCache<string, TokenInfo>;
  private ttlSeconds: number;
  /** Tracks revoked tokens with their revocation timestamp for TTL-based cleanup */
  private revokedTokens: Map<string, number> = new Map();
  /** Tracks revoked JWT token IDs (jti claim) with their revocation timestamp */
  private revokedTokenIds: Map<string, number> = new Map();
  /** Tracks revoked user IDs with their revocation timestamp */
  private revokedUserIds: Map<string, number> = new Map();
  /** Maps userId to set of tokens for bulk revocation support */
  private userTokens: Map<string, Set<string>> = new Map();
  /** TTL for revoked tokens in milliseconds (default: 1 hour) */
  private revokedTokenTTLMs: number;

  constructor(options: { maxSize: number; ttlSeconds: number; revokedTokenTTLMs?: number }) {
    this.ttlSeconds = options.ttlSeconds;
    // Default revoked token TTL to 1 hour (3600000ms) to prevent unbounded growth
    this.revokedTokenTTLMs = options.revokedTokenTTLMs ?? 3600000;
    // Use LRUCache with TTL for cache entry expiration
    this.cache = new LRUCache({
      maxSize: options.maxSize,
      ttlMs: options.ttlSeconds * 1000,
    });
  }

  /**
   * Store a validated token in the cache
   * Also tracks the token by userId for bulk revocation support
   */
  set(token: string, info: TokenInfo): void {
    this.cache.set(token, info);
    // Track token by userId for revokeAllForUser support
    const userId = info.user.userId;
    if (!this.userTokens.has(userId)) {
      this.userTokens.set(userId, new Set());
    }
    const tokenSet = this.userTokens.get(userId);
    if (tokenSet) {
      tokenSet.add(token);
    }
  }

  /**
   * Retrieve cached token info if valid and not expired
   * Returns undefined if not found, revoked, or if token/cache entry has expired
   */
  get(token: string): TokenInfo | undefined {
    // Check if token has been revoked
    if (this.isRevoked(token)) {
      // Token is revoked, remove from cache if present
      this.cache.delete(token);
      return undefined;
    }

    const info = this.cache.get(token);
    if (!info) {
      return undefined;
    }

    // Check if user is revoked
    if (this.isUserRevoked(info.user.userId)) {
      this.cache.delete(token);
      return undefined;
    }

    // Check if token ID (jti) is revoked
    const jti = info.user.metadata?.jti as string | undefined;
    if (jti && this.isRevokedByTokenId(jti)) {
      this.cache.delete(token);
      return undefined;
    }

    // Check if the JWT token itself has expired
    if (info.expiresAt <= Date.now()) {
      this.cache.delete(token);
      return undefined;
    }

    // Check if cache entry has exceeded its TTL
    const cacheAgeSeconds = (Date.now() - info.validatedAt) / 1000;
    if (cacheAgeSeconds > this.ttlSeconds) {
      this.cache.delete(token);
      return undefined;
    }

    return info;
  }

  /**
   * Revoke a token, preventing it from being retrieved from cache
   * The token will be added to a revocation list with TTL to prevent unbounded growth
   */
  revoke(token: string): void {
    // Remove from cache immediately
    this.cache.delete(token);
    // Add to revocation list with current timestamp
    this.revokedTokens.set(token, Date.now());
    // Cleanup expired revocation entries periodically
    this.cleanupRevokedTokens();
  }

  /**
   * Revoke all tokens for a specific user
   * Useful for logout-all-devices or when user permissions change
   * Also adds user to revoked users list to prevent new tokens from being cached
   */
  revokeAllForUser(userId: string): number {
    // Add user to revoked users list
    this.revokedUserIds.set(userId, Date.now());

    const userTokenSet = this.userTokens.get(userId);
    if (!userTokenSet || userTokenSet.size === 0) {
      // Still cleanup even if no tokens to revoke
      this.cleanupRevokedTokens();
      return 0;
    }

    let revokedCount = 0;
    for (const token of userTokenSet) {
      // Remove from cache
      this.cache.delete(token);
      // Add to revocation list with current timestamp
      this.revokedTokens.set(token, Date.now());
      revokedCount++;
    }

    // Clear the user's token set (but keep the revocation entries)
    userTokenSet.clear();

    // Cleanup expired revocation entries periodically
    this.cleanupRevokedTokens();

    return revokedCount;
  }

  /**
   * Revoke a token by its JWT ID (jti claim)
   * Useful when you know the token ID but not the full token string
   */
  revokeByTokenId(tokenId: string): void {
    this.revokedTokenIds.set(tokenId, Date.now());
    // Cleanup expired revocation entries periodically
    this.cleanupRevokedTokens();
  }

  /**
   * Check if a token has been revoked
   * Returns false if the revocation entry has expired (past TTL)
   */
  isRevoked(token: string): boolean {
    const revokedAt = this.revokedTokens.get(token);
    if (revokedAt === undefined) {
      return false;
    }
    // Check if revocation entry has expired
    if (Date.now() - revokedAt > this.revokedTokenTTLMs) {
      this.revokedTokens.delete(token);
      return false;
    }
    return true;
  }

  /**
   * Check if a token ID (jti claim) has been revoked
   * Returns false if the revocation entry has expired (past TTL)
   */
  isRevokedByTokenId(tokenId: string): boolean {
    const revokedAt = this.revokedTokenIds.get(tokenId);
    if (revokedAt === undefined) {
      return false;
    }
    // Check if revocation entry has expired
    if (Date.now() - revokedAt > this.revokedTokenTTLMs) {
      this.revokedTokenIds.delete(tokenId);
      return false;
    }
    return true;
  }

  /**
   * Check if a user ID has been revoked
   * Returns false if the revocation entry has expired (past TTL)
   */
  private isUserRevoked(userId: string): boolean {
    const revokedAt = this.revokedUserIds.get(userId);
    if (revokedAt === undefined) {
      return false;
    }
    // Check if revocation entry has expired
    if (Date.now() - revokedAt > this.revokedTokenTTLMs) {
      this.revokedUserIds.delete(userId);
      return false;
    }
    return true;
  }

  /**
   * Cleanup expired entries from all revocation lists
   * Called periodically to prevent unbounded growth
   */
  private cleanupRevokedTokens(): void {
    const now = Date.now();
    for (const [token, revokedAt] of this.revokedTokens) {
      if (now - revokedAt > this.revokedTokenTTLMs) {
        this.revokedTokens.delete(token);
      }
    }
    for (const [tokenId, revokedAt] of this.revokedTokenIds) {
      if (now - revokedAt > this.revokedTokenTTLMs) {
        this.revokedTokenIds.delete(tokenId);
      }
    }
    for (const [userId, revokedAt] of this.revokedUserIds) {
      if (now - revokedAt > this.revokedTokenTTLMs) {
        this.revokedUserIds.delete(userId);
      }
    }
  }

  /**
   * Get the number of revoked tokens currently tracked
   * Useful for monitoring and testing
   */
  getRevokedCount(): number {
    return this.revokedTokens.size;
  }

  /**
   * Get list of all revoked token IDs (jti claims)
   */
  getRevokedTokenIds(): string[] {
    // Clean up expired entries first
    const now = Date.now();
    const validIds: string[] = [];
    for (const [tokenId, revokedAt] of this.revokedTokenIds) {
      if (now - revokedAt <= this.revokedTokenTTLMs) {
        validIds.push(tokenId);
      }
    }
    return validIds;
  }

  /**
   * Get list of all revoked user IDs
   */
  getRevokedUserIds(): string[] {
    // Clean up expired entries first
    const now = Date.now();
    const validIds: string[] = [];
    for (const [userId, revokedAt] of this.revokedUserIds) {
      if (now - revokedAt <= this.revokedTokenTTLMs) {
        validIds.push(userId);
      }
    }
    return validIds;
  }

  /**
   * Export the complete revocation list for persistence
   */
  exportRevocationList(): { tokens: string[]; tokenIds: string[]; userIds: string[] } {
    const now = Date.now();
    const tokens: string[] = [];
    const tokenIds: string[] = [];
    const userIds: string[] = [];

    for (const [token, revokedAt] of this.revokedTokens) {
      if (now - revokedAt <= this.revokedTokenTTLMs) {
        tokens.push(token);
      }
    }
    for (const [tokenId, revokedAt] of this.revokedTokenIds) {
      if (now - revokedAt <= this.revokedTokenTTLMs) {
        tokenIds.push(tokenId);
      }
    }
    for (const [userId, revokedAt] of this.revokedUserIds) {
      if (now - revokedAt <= this.revokedTokenTTLMs) {
        userIds.push(userId);
      }
    }

    return { tokens, tokenIds, userIds };
  }

  /**
   * Import a revocation list (merges with existing revocations)
   */
  importRevocationList(list: { tokens?: string[]; tokenIds?: string[]; userIds?: string[] }): void {
    const now = Date.now();
    if (list.tokens) {
      for (const token of list.tokens) {
        this.revokedTokens.set(token, now);
      }
    }
    if (list.tokenIds) {
      for (const tokenId of list.tokenIds) {
        this.revokedTokenIds.set(tokenId, now);
      }
    }
    if (list.userIds) {
      for (const userId of list.userIds) {
        this.revokedUserIds.set(userId, now);
      }
    }
  }

  /**
   * Clear all revocation lists (but keep the cache)
   */
  clearRevocationList(): void {
    this.revokedTokens.clear();
    this.revokedTokenIds.clear();
    this.revokedUserIds.clear();
  }

  /**
   * Remove a specific token from the cache
   */
  invalidate(token: string): void {
    this.cache.delete(token);
  }

  /**
   * Clear all entries from the cache (but preserve revocation lists)
   * Use clearRevocationList() to also clear revocations
   */
  clear(): void {
    this.cache.clear();
    this.userTokens.clear();
  }
}

// ============================================================================
// Factory Functions
// ============================================================================

/**
 * Create a token cache with default settings
 */
export function createTokenCache(options?: {
  maxSize?: number;
  ttlSeconds?: number;
  revokedTokenTTLMs?: number;
}): TokenCache {
  return new TokenCache({
    maxSize: options?.maxSize ?? DEFAULT_CACHE_MAX_SIZE,
    ttlSeconds: options?.ttlSeconds ?? DEFAULT_CACHE_TTL_SECONDS,
    revokedTokenTTLMs: options?.revokedTokenTTLMs,
  });
}
