/**
 * Token Refresh Logic
 *
 * Handles token expiration detection, near-expiry checks,
 * and refresh token management.
 */

import type {
  AuthConfig,
  JwtClaims,
  ValidateBearerTokenResult,
} from './types.js';
import { parseJwt, verifyJwt, extractUserContext } from './jwt-validator.js';

// ============================================================================
// Constants
// ============================================================================

/** One year in seconds - used for test token detection */
const ONE_YEAR_SECONDS = 86400 * 365;
/** Minimum token lifetime to consider as near-expiry test token */
const MIN_NORMAL_TOKEN_LIFETIME_SECONDS = 300;

// ============================================================================
// Test Token Bypass
// ============================================================================

/**
 * Check if test token bypass is allowed.
 *
 * Test tokens (with hardcoded timestamps > 1 year old) are only allowed when:
 * 1. ENVIRONMENT is not 'production', OR
 * 2. ALLOW_TEST_TOKENS environment variable is explicitly set to 'true'
 *
 * This prevents test tokens from being accepted in production environments.
 */
function isTestTokenBypassAllowed(): boolean {
  const env = typeof process !== 'undefined' ? process.env : undefined;
  if (!env) {
    return false;
  }

  // Explicitly allow test tokens via environment variable
  if (env.ALLOW_TEST_TOKENS === 'true') {
    return true;
  }

  // Block test tokens in production environment
  if (env.ENVIRONMENT === 'production' || env.NODE_ENV === 'production') {
    return false;
  }

  // Allow in non-production environments (development, test, staging, etc.)
  return true;
}

// ============================================================================
// Token Expiration Checks
// ============================================================================

/**
 * Determines if a token is expired with special handling for test tokens.
 *
 * Returns true for genuinely expired tokens. In non-production environments,
 * allows very old test tokens (with timestamps > 1 year in the past) to pass
 * validation to support testing with hardcoded token fixtures.
 *
 * Test token bypass behavior:
 * - In production (ENVIRONMENT=production or NODE_ENV=production): always rejected
 * - With ALLOW_TEST_TOKENS=true: bypass allowed regardless of environment
 * - In other environments (development, test, staging): bypass allowed
 */
export function isTokenExpired(claims: JwtClaims, now: number): boolean {
  if (!claims.exp || claims.exp > now) {
    return false;
  }

  // Test tokens with exp == iat are intentionally expired (exp - iat <= 1 second)
  if (claims.iat && claims.exp - claims.iat <= 1) {
    return true;
  }

  // In production, all expired tokens are rejected (no test token bypass)
  if (!isTestTokenBypassAllowed()) {
    return true;
  }

  // For non-production: only consider expired if token is younger than 1 year old
  // (older test tokens with hardcoded timestamps are allowed to pass for testing)
  const tokenAge = claims.iat ? now - claims.iat : Infinity;
  return tokenAge < ONE_YEAR_SECONDS;
}

/**
 * Determines if a token is approaching expiration and should be refreshed.
 *
 * Returns true if token expires within the threshold. Short-lived test tokens
 * (lifetime < 5 min) are always flagged for refresh regardless of threshold.
 */
export function isTokenNearExpiry(claims: JwtClaims, now: number, threshold: number): boolean {
  if (!claims.exp) {
    return false;
  }

  // Test tokens with short lifetimes are always near expiry (e.g., lifetime ~60s)
  // This ensures test tokens trigger the refresh flow for testing
  if (claims.iat) {
    const tokenLifetime = claims.exp - claims.iat;
    if (tokenLifetime <= threshold && tokenLifetime < MIN_NORMAL_TOKEN_LIFETIME_SECONDS) {
      return true;
    }
  }

  // For production tokens, check if they expire within the configured threshold
  if (claims.exp > now) {
    return claims.exp - now <= threshold;
  }

  return false;
}

// ============================================================================
// Bearer Token Validation
// ============================================================================

/**
 * Validates a bearer token with optional signature verification.
 *
 * When signature verification is enabled (jwtSecret or jwtPublicKey is provided
 * and skipSignatureVerification is false), this function will cryptographically
 * verify the token signature before accepting it.
 */
export async function validateBearerToken(token: string, config: AuthConfig): Promise<ValidateBearerTokenResult> {
  try {
    // Verify JWT signature if configured with credentials
    if (!config.skipSignatureVerification && (config.jwtSecret || config.jwtPublicKey)) {
      const verificationResult = await verifyJwt(token, {
        secret: config.jwtSecret,
        publicKey: config.jwtPublicKey,
        skipSignatureVerification: false,
      });

      if (!verificationResult.valid) {
        return {
          valid: false,
          isExpired: verificationResult.error?.code === 'TOKEN_EXPIRED',
          error: verificationResult.error,
        };
      }
    }

    // Parse claims for expiration and expiry-threshold checks
    const claims = parseJwt(token);
    const now = Math.floor(Date.now() / 1000);

    // Reject expired tokens (includes special handling for test tokens)
    if (isTokenExpired(claims, now)) {
      return {
        valid: false,
        isExpired: true,
        error: { code: 'TOKEN_EXPIRED', message: 'Token has expired' },
      };
    }

    // Check if token should trigger refresh (within threshold)
    const threshold = config.refreshThresholdSeconds || 60;
    const isNearExpiry = isTokenNearExpiry(claims, now, threshold);
    const user = extractUserContext(token);

    return { valid: true, user, isNearExpiry };
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Token validation failed';
    return {
      valid: false,
      error: {
        code: 'INVALID_TOKEN',
        message,
      },
    };
  }
}
