/**
 * JWKS Manager
 *
 * Handles JWKS endpoint fetching, caching, and key rotation
 * per RFC 7517 (JSON Web Key).
 */

import type { JWK, JWKSet, AuditLogger } from './types.js';

// ============================================================================
// Constants
// ============================================================================

/** Default JWKs cache TTL in milliseconds (1 hour) */
const DEFAULT_JWKS_CACHE_TTL_MS = 60 * 60 * 1000;

/** Minimum JWKs cache TTL in milliseconds (5 minutes) */
const MIN_JWKS_CACHE_TTL_MS = 5 * 60 * 1000;

// ============================================================================
// Types
// ============================================================================

/** Cached JWKs with metadata */
interface CachedJWKS {
  keys: Map<string, CryptoKey>;
  fetchedAt: number;
  expiresAt: number;
}

// ============================================================================
// JWK Import
// ============================================================================

/**
 * Import a JWK to a CryptoKey for verification
 */
export async function importJwk(jwk: JWK, alg: string): Promise<CryptoKey> {
  if (jwk.kty === 'RSA' && (alg === 'RS256' || alg === 'RS384' || alg === 'RS512')) {
    if (!jwk.n || !jwk.e) {
      throw new Error('RSA JWK missing n or e parameter');
    }
    const hashMap: Record<string, string> = {
      RS256: 'SHA-256',
      RS384: 'SHA-384',
      RS512: 'SHA-512',
    };
    return crypto.subtle.importKey(
      'jwk',
      { kty: 'RSA', n: jwk.n, e: jwk.e, alg },
      { name: 'RSASSA-PKCS1-v1_5', hash: hashMap[alg] ?? 'SHA-256' },
      false,
      ['verify']
    );
  }

  if (jwk.kty === 'EC' && (alg === 'ES256' || alg === 'ES384' || alg === 'ES512')) {
    if (!jwk.x || !jwk.y || !jwk.crv) {
      throw new Error('EC JWK missing x, y, or crv parameter');
    }
    const curveMap: Record<string, string> = {
      ES256: 'P-256',
      ES384: 'P-384',
      ES512: 'P-521',
    };
    return crypto.subtle.importKey(
      'jwk',
      { kty: 'EC', x: jwk.x, y: jwk.y, crv: jwk.crv },
      { name: 'ECDSA', namedCurve: curveMap[alg] ?? 'P-256' },
      false,
      ['verify']
    );
  }

  throw new Error(`Unsupported JWK key type: ${jwk.kty} with algorithm: ${alg}`);
}

// ============================================================================
// JWKS Cache
// ============================================================================

/** Cache for JWKs endpoints */
const jwksCache: Map<string, CachedJWKS> = new Map();

/**
 * Clear the JWKs cache (useful for testing)
 */
export function clearJWKSCache(): void {
  jwksCache.clear();
}

/**
 * Get cached JWKS keys if still valid
 */
export function getCachedJWKS(jwksUri: string): Map<string, CryptoKey> | undefined {
  const cached = jwksCache.get(jwksUri);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.keys;
  }
  return undefined;
}

// ============================================================================
// JWKS Fetching
// ============================================================================

/**
 * Fetch and cache JWKs from a well-known endpoint
 */
export async function fetchJWKS(
  jwksUri: string,
  options: { forceRefresh?: boolean; auditLogger?: AuditLogger } = {}
): Promise<Map<string, CryptoKey>> {
  const now = Date.now();
  const cached = jwksCache.get(jwksUri);

  // Return cached keys if valid and not forcing refresh
  if (!options.forceRefresh && cached && cached.expiresAt > now) {
    return cached.keys;
  }

  try {
    const response = await fetch(jwksUri, {
      headers: { Accept: 'application/json' },
    });

    if (!response.ok) {
      throw new Error(`Failed to fetch JWKs: ${response.status} ${response.statusText}`);
    }

    const jwks: JWKSet = await response.json();

    if (!jwks.keys || !Array.isArray(jwks.keys)) {
      throw new Error('Invalid JWKs response: missing keys array');
    }

    // Parse Cache-Control header for TTL
    let ttlMs = DEFAULT_JWKS_CACHE_TTL_MS;
    const cacheControl = response.headers.get('Cache-Control');
    if (cacheControl) {
      const maxAgeMatch = cacheControl.match(/max-age=(\d+)/);
      if (maxAgeMatch?.[1]) {
        ttlMs = Math.max(parseInt(maxAgeMatch[1], 10) * 1000, MIN_JWKS_CACHE_TTL_MS);
      }
    }

    // Import all keys
    const keys = new Map<string, CryptoKey>();
    for (const jwk of jwks.keys) {
      if (jwk.kid && jwk.use !== 'enc') {
        // Only import signing keys
        const alg = jwk.alg ?? (jwk.kty === 'RSA' ? 'RS256' : jwk.kty === 'EC' ? 'ES256' : undefined);
        if (alg) {
          try {
            const cryptoKey = await importJwk(jwk, alg);
            keys.set(jwk.kid, cryptoKey);
          } catch (err) {
            // Skip invalid keys but continue processing others.
            // This is expected behavior for JWKS endpoints that contain keys
            // with unsupported algorithms or malformed key data.
            // Log at debug level if audit logger is available.
            if (options.auditLogger) {
              options.auditLogger.log({
                eventType: 'jwks_key_import_failed',
                timestamp: new Date(),
                metadata: {
                  jwksUri,
                  kid: jwk.kid,
                  alg,
                  kty: jwk.kty,
                  error: err instanceof Error ? err.message : String(err),
                },
              });
            }
          }
        }
      }
    }

    // Cache the keys
    jwksCache.set(jwksUri, {
      keys,
      fetchedAt: now,
      expiresAt: now + ttlMs,
    });

    // Log successful JWKS refresh
    if (options.auditLogger) {
      options.auditLogger.log({
        eventType: 'jwks_refresh',
        timestamp: new Date(),
        metadata: { jwksUri, keyCount: keys.size },
      });
    }

    return keys;
  } catch (err) {
    // Log JWKS refresh failure
    if (options.auditLogger) {
      options.auditLogger.log({
        eventType: 'jwks_refresh_failure',
        timestamp: new Date(),
        errorMessage: err instanceof Error ? err.message : 'Unknown error',
        metadata: { jwksUri },
      });
    }

    // Return stale cached keys if available (soft failure)
    if (cached) {
      return cached.keys;
    }

    throw err;
  }
}
