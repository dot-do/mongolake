/**
 * JWT Validator
 *
 * Provides JWT token validation, decoding, and signature verification
 * supporting HS256, RS256, and ES256 algorithms.
 */

import type {
  JwtHeader,
  JwtClaims,
  JwtVerificationOptions,
  JwtVerificationResult,
  ExtendedJwtVerificationOptions,
  UserContext,
} from './types.js';
import { fetchJWKS } from './jwks-manager.js';

// ============================================================================
// Base64URL Helpers
// ============================================================================

/**
 * Decode a base64url-encoded string to UTF-8
 * (handles padding and character replacement for URL-safe encoding)
 */
export function decodeBase64Url(input: string): string {
  let base64 = input.replace(/-/g, '+').replace(/_/g, '/');
  // Add padding (base64 requires length to be multiple of 4)
  while (base64.length % 4) {
    base64 += '=';
  }
  return atob(base64);
}

/**
 * Convert base64url-encoded string to Uint8Array for crypto operations
 */
export function base64UrlToUint8Array(input: string): Uint8Array {
  const decoded = decodeBase64Url(input);
  const bytes = new Uint8Array(decoded.length);
  for (let i = 0; i < decoded.length; i++) {
    bytes[i] = decoded.charCodeAt(i);
  }
  return bytes;
}

/**
 * Convert UTF-8 string to Uint8Array for crypto operations
 */
export function stringToUint8Array(str: string): Uint8Array {
  return new TextEncoder().encode(str);
}

// ============================================================================
// JWT Parsing
// ============================================================================

/**
 * Extract and parse the header from a JWT token
 */
export function parseJwtHeader(token: string): JwtHeader {
  const parts = token.split('.');
  if (parts.length !== 3) {
    throw new Error('JWT must contain exactly 3 parts separated by dots');
  }
  try {
    const header = decodeBase64Url(parts[0]!);
    return JSON.parse(header);
  } catch {
    throw new Error('Failed to decode/parse JWT header');
  }
}

/**
 * Extract and parse the claims/payload from a JWT token
 */
export function parseJwt(token: string): JwtClaims {
  const parts = token.split('.');
  if (parts.length !== 3) {
    throw new Error('JWT must contain exactly 3 parts separated by dots');
  }
  try {
    const payload = decodeBase64Url(parts[1]!);
    return JSON.parse(payload);
  } catch {
    throw new Error('Failed to decode/parse JWT payload');
  }
}

// ============================================================================
// Key Import Functions
// ============================================================================

export async function importHmacKey(secret: string): Promise<CryptoKey> {
  return crypto.subtle.importKey(
    'raw',
    stringToUint8Array(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['verify']
  );
}

export async function importRsaPublicKey(pem: string): Promise<CryptoKey> {
  const pemContents = pem
    .replace(/-----BEGIN (?:RSA )?PUBLIC KEY-----/g, '')
    .replace(/-----END (?:RSA )?PUBLIC KEY-----/g, '')
    .replace(/\s/g, '');

  const binaryString = atob(pemContents);
  const bytes = new Uint8Array(binaryString.length);
  for (let i = 0; i < binaryString.length; i++) {
    bytes[i] = binaryString.charCodeAt(i);
  }

  return crypto.subtle.importKey(
    'spki',
    bytes,
    { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
    false,
    ['verify']
  );
}

export async function importEcdsaPublicKey(pem: string): Promise<CryptoKey> {
  const pemContents = pem
    .replace(/-----BEGIN (?:EC )?PUBLIC KEY-----/g, '')
    .replace(/-----END (?:EC )?PUBLIC KEY-----/g, '')
    .replace(/\s/g, '');

  const binaryString = atob(pemContents);
  const bytes = new Uint8Array(binaryString.length);
  for (let i = 0; i < binaryString.length; i++) {
    bytes[i] = binaryString.charCodeAt(i);
  }

  return crypto.subtle.importKey(
    'spki',
    bytes,
    { name: 'ECDSA', namedCurve: 'P-256' },
    false,
    ['verify']
  );
}

// ============================================================================
// Signature Verification
// ============================================================================

async function verifySignature(
  algorithm: 'HMAC' | { name: 'RSASSA-PKCS1-v1_5' } | { name: 'ECDSA'; hash: { name: string } },
  key: CryptoKey,
  signature: Uint8Array,
  signingInput: string
): Promise<boolean> {
  return crypto.subtle.verify(algorithm, key, signature, stringToUint8Array(signingInput));
}

async function verifyJwtSignature(
  alg: string,
  signingInput: string,
  signature: Uint8Array,
  options: ExtendedJwtVerificationOptions,
  kid?: string
): Promise<JwtVerificationResult> {
  try {
    if (alg === 'HS256') {
      if (!options.secret) {
        return {
          valid: false,
          error: { code: 'MISSING_SECRET', message: 'JWT secret is required for HS256 verification' },
        };
      }
      const key = await importHmacKey(options.secret);
      const isValid = await verifySignature('HMAC', key, signature, signingInput);
      if (!isValid) {
        return { valid: false, error: { code: 'INVALID_SIGNATURE', message: 'JWT signature verification failed' } };
      }
    } else if (alg === 'RS256') {
      // Try JWKs first if kid is provided
      if (kid && (options.jwksKeys || options.jwksUri)) {
        const keys = options.jwksKeys ?? (options.jwksUri ? await fetchJWKS(options.jwksUri, { auditLogger: options.auditLogger }) : undefined);
        const key = keys?.get(kid);
        if (key) {
          const isValid = await verifySignature({ name: 'RSASSA-PKCS1-v1_5' }, key, signature, signingInput);
          if (!isValid) {
            return { valid: false, error: { code: 'INVALID_SIGNATURE', message: 'JWT signature verification failed' } };
          }
          return { valid: true };
        }
      }
      // Fall back to PEM public key
      if (!options.publicKey) {
        return {
          valid: false,
          error: { code: 'MISSING_PUBLIC_KEY', message: 'Public key is required for RS256 verification' },
        };
      }
      const key = await importRsaPublicKey(options.publicKey);
      const isValid = await verifySignature({ name: 'RSASSA-PKCS1-v1_5' }, key, signature, signingInput);
      if (!isValid) {
        return { valid: false, error: { code: 'INVALID_SIGNATURE', message: 'JWT signature verification failed' } };
      }
    } else if (alg === 'ES256') {
      // Try JWKs first if kid is provided
      if (kid && (options.jwksKeys || options.jwksUri)) {
        const keys = options.jwksKeys ?? (options.jwksUri ? await fetchJWKS(options.jwksUri, { auditLogger: options.auditLogger }) : undefined);
        const key = keys?.get(kid);
        if (key) {
          const isValid = await verifySignature({ name: 'ECDSA', hash: { name: 'SHA-256' } }, key, signature, signingInput);
          if (!isValid) {
            return { valid: false, error: { code: 'INVALID_SIGNATURE', message: 'JWT signature verification failed' } };
          }
          return { valid: true };
        }
      }
      // Fall back to PEM public key
      if (!options.publicKey) {
        return {
          valid: false,
          error: { code: 'MISSING_PUBLIC_KEY', message: 'Public key is required for ES256 verification' },
        };
      }
      const key = await importEcdsaPublicKey(options.publicKey);
      const isValid = await verifySignature({ name: 'ECDSA', hash: { name: 'SHA-256' } }, key, signature, signingInput);
      if (!isValid) {
        return { valid: false, error: { code: 'INVALID_SIGNATURE', message: 'JWT signature verification failed' } };
      }
    } else if (alg === 'none') {
      return { valid: false, error: { code: 'INSECURE_ALGORITHM', message: 'Algorithm "none" is not allowed' } };
    } else {
      return { valid: false, error: { code: 'UNSUPPORTED_ALGORITHM', message: `Unsupported JWT algorithm: ${alg}` } };
    }
    return { valid: true };
  } catch (err) {
    return {
      valid: false,
      error: {
        code: 'SIGNATURE_VERIFICATION_ERROR',
        message: err instanceof Error ? err.message : 'Signature verification failed',
      },
    };
  }
}

// ============================================================================
// JWT Verification
// ============================================================================

/**
 * Verify a JWT token's signature and claims
 *
 * @param token - The JWT token string
 * @param options - Verification options including secret/publicKey
 * @returns Verification result with claims if valid
 */
export async function verifyJwt(
  token: string,
  options: JwtVerificationOptions = {}
): Promise<JwtVerificationResult> {
  // Parse the token structure
  const parts = token.split('.');
  if (parts.length !== 3) {
    return {
      valid: false,
      error: {
        code: 'INVALID_TOKEN_FORMAT',
        message: 'JWT must have three parts separated by dots',
      },
    };
  }

  const [headerB64, payloadB64, signatureB64] = parts;

  // Parse header
  let header: JwtHeader;
  try {
    header = parseJwtHeader(token);
  } catch {
    return {
      valid: false,
      error: {
        code: 'INVALID_TOKEN_HEADER',
        message: 'Failed to parse JWT header',
      },
    };
  }

  // Parse claims
  let claims: JwtClaims;
  try {
    claims = parseJwt(token);
  } catch {
    return {
      valid: false,
      error: {
        code: 'INVALID_TOKEN_PAYLOAD',
        message: 'Failed to parse JWT payload',
      },
    };
  }

  // Verify signature (unless explicitly skipped for testing)
  if (!options.skipSignatureVerification) {
    const signingInput = `${headerB64}.${payloadB64}`;
    const signature = base64UrlToUint8Array(signatureB64!);

    const signatureResult = await verifyJwtSignature(header.alg, signingInput, signature, options, header.kid);
    if (!signatureResult.valid) {
      return signatureResult;
    }
  }

  // Validate time-based claims
  const now = Math.floor(Date.now() / 1000);
  const clockTolerance = options.clockTolerance || 0;

  // Check expiration (exp claim)
  if (claims.exp !== undefined) {
    if (claims.exp + clockTolerance < now) {
      return {
        valid: false,
        header,
        claims,
        error: {
          code: 'TOKEN_EXPIRED',
          message: 'Token has expired',
        },
      };
    }
  }

  // Check not-before (nbf claim)
  if (claims.nbf !== undefined) {
    if (claims.nbf - clockTolerance > now) {
      return {
        valid: false,
        header,
        claims,
        error: {
          code: 'TOKEN_NOT_YET_VALID',
          message: 'Token is not yet valid',
        },
      };
    }
  }

  return {
    valid: true,
    header,
    claims,
  };
}

// ============================================================================
// User Context Extraction
// ============================================================================

export function extractUserContext(token: string): UserContext {
  const claims = parseJwt(token);

  // User ID (subject claim) is required
  if (!claims.sub) {
    throw new Error('Token missing required "sub" (subject) claim');
  }

  const userId = claims.sub;
  const email = claims.email;
  const roles = claims.roles ?? [];
  const permissions = claims.permissions;
  const organizationId = claims.org_id;

  // Collect non-standard claims as metadata (exclude reserved JWT/auth claims)
  const reservedClaims = new Set([
    'sub', 'email', 'roles', 'permissions', 'org_id',
    'iss', 'aud', 'exp', 'iat', 'nbf', 'jti',
  ]);
  const metadata: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(claims)) {
    if (!reservedClaims.has(key)) {
      metadata[key] = value;
    }
  }

  return {
    userId,
    email,
    roles,
    permissions,
    organizationId,
    metadata: Object.keys(metadata).length > 0 ? metadata : undefined,
  };
}
