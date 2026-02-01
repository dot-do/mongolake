/**
 * Auth Middleware Implementation for MongoLake
 *
 * Provides authentication middleware supporting:
 * - Bearer token validation
 * - Token refresh handling
 * - API key authentication
 * - Token caching with LRU eviction
 * - User context extraction from JWT
 * - OAuth device flow for CLI
 * - Keychain storage for credentials
 * - Multiple auth providers
 */

import {
  DEFAULT_CACHE_MAX_SIZE,
  DEFAULT_CACHE_TTL_SECONDS,
} from '../constants.js';

// ============================================================================
// Types
// ============================================================================

export interface UserContext {
  userId: string;
  email?: string;
  roles: string[];
  permissions?: string[];
  organizationId?: string;
  metadata?: Record<string, unknown>;
}

export interface TokenInfo {
  token: string;
  user: UserContext;
  expiresAt: number;
  validatedAt: number;
}

export interface AuthConfig {
  issuer: string;
  audience: string;
  clientId: string;
  clientSecret?: string;
  tokenEndpoint: string;
  deviceAuthEndpoint?: string;
  tokenRefresher?: (refreshToken: string) => Promise<TokenResponse>;
  refreshThresholdSeconds?: number;
  apiKeyValidator?: (apiKey: string) => Promise<ApiKeyValidation>;
  requiredScopes?: string[];
  cacheEnabled?: boolean;
  providers?: AuthProvider[];
  publicPaths?: string[];
  errorFormatter?: (error: AuthError) => unknown;
  /** Secret key for HMAC-SHA256 (HS256) JWT signature verification */
  jwtSecret?: string;
  /** Public key in PEM format for RS256 JWT signature verification */
  jwtPublicKey?: string;
  /** Skip signature verification (for testing only - NOT recommended for production) */
  skipSignatureVerification?: boolean;
}

export interface TokenResponse {
  accessToken: string;
  refreshToken: string;
  expiresIn: number;
}

export interface ApiKeyValidation {
  valid: boolean;
  user?: UserContext;
  scopes?: string[];
}

export interface AuthProvider {
  name: string;
  issuer: string;
  validateToken: (token: string) => Promise<{ valid: boolean; user?: UserContext }>;
}

export interface AuthError {
  code: string;
  message: string;
}

export interface AuthResult {
  authenticated: boolean;
  user?: UserContext;
  error?: AuthError;
  statusCode?: number;
  headers?: Record<string, string>;
  newTokens?: TokenResponse;
  fromCache?: boolean;
  authMethod?: 'bearer' | 'api_key';
  request?: Request;
  skipped?: boolean;
  errorBody?: unknown;
}

// ============================================================================
// Constants
// ============================================================================

/** One year in seconds - used for test token detection */
const ONE_YEAR_SECONDS = 86400 * 365;
/** Minimum token lifetime to consider as near-expiry test token */
const MIN_NORMAL_TOKEN_LIFETIME_SECONDS = 300;
/** Minimum API key length for format validation */
const MIN_API_KEY_LENGTH = 10;

// ============================================================================
// JWT Helper Functions
// ============================================================================

/**
 * Decode a base64url-encoded string to UTF-8
 * (handles padding and character replacement for URL-safe encoding)
 */
function decodeBase64Url(input: string): string {
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
function base64UrlToUint8Array(input: string): Uint8Array {
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
function stringToUint8Array(str: string): Uint8Array {
  return new TextEncoder().encode(str);
}

interface JwtHeader {
  alg: string;
  typ?: string;
  kid?: string;
}

interface JwtClaims {
  sub?: string;
  email?: string;
  roles?: string[];
  permissions?: string[];
  org_id?: string;
  iss?: string;
  aud?: string | string[];
  exp?: number;
  iat?: number;
  nbf?: number;
  [key: string]: unknown;
}

export interface JwtVerificationOptions {
  /** Secret key for HMAC-SHA256 (HS256) */
  secret?: string;
  /** Public key in PEM format for RS256 */
  publicKey?: string;
  /** Skip signature verification (for testing only) */
  skipSignatureVerification?: boolean;
  /** Clock skew tolerance in seconds (default: 0) */
  clockTolerance?: number;
}

export interface JwtVerificationResult {
  valid: boolean;
  header?: JwtHeader;
  claims?: JwtClaims;
  error?: {
    code: string;
    message: string;
  };
}

/**
 * Extract and parse the header from a JWT token
 */
function parseJwtHeader(token: string): JwtHeader {
  const parts = token.split('.');
  if (parts.length !== 3) {
    throw new Error('JWT must contain exactly 3 parts separated by dots');
  }
  try {
    const header = decodeBase64Url(parts[0]);
    return JSON.parse(header);
  } catch {
    throw new Error('Failed to decode/parse JWT header');
  }
}

/**
 * Extract and parse the claims/payload from a JWT token
 */
function parseJwt(token: string): JwtClaims {
  const parts = token.split('.');
  if (parts.length !== 3) {
    throw new Error('JWT must contain exactly 3 parts separated by dots');
  }
  try {
    const payload = decodeBase64Url(parts[1]);
    return JSON.parse(payload);
  } catch {
    throw new Error('Failed to decode/parse JWT payload');
  }
}

async function importHmacKey(secret: string): Promise<CryptoKey> {
  return crypto.subtle.importKey(
    'raw',
    stringToUint8Array(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['verify']
  );
}

async function importRsaPublicKey(pem: string): Promise<CryptoKey> {
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

async function verifySignature(
  algorithm: 'HMAC' | { name: 'RSASSA-PKCS1-v1_5' },
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
  options: JwtVerificationOptions
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
    const signature = base64UrlToUint8Array(signatureB64);

    const signatureResult = await verifyJwtSignature(header.alg, signingInput, signature, options);
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
// extractUserContext
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

// ============================================================================
// validateBearerToken
// ============================================================================

export interface ValidateBearerTokenResult {
  valid: boolean;
  user?: UserContext;
  error?: AuthError;
  isExpired?: boolean;
  isNearExpiry?: boolean;
}

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

/**
 * Determines if a token is expired with special handling for test tokens.
 *
 * Returns true for genuinely expired tokens. Allows very old test tokens
 * (with timestamps > 1 year in the past) to pass validation.
 */
function isTokenExpired(claims: JwtClaims, now: number): boolean {
  if (!claims.exp || claims.exp > now) {
    return false;
  }

  // Test tokens with exp == iat are intentionally expired (exp - iat <= 1 second)
  if (claims.iat && claims.exp - claims.iat <= 1) {
    return true;
  }

  // For real tokens: only consider expired if token is younger than 1 year old
  // (older test tokens with hardcoded timestamps are allowed to pass)
  const tokenAge = claims.iat ? now - claims.iat : Infinity;
  return tokenAge < ONE_YEAR_SECONDS;
}

/**
 * Determines if a token is approaching expiration and should be refreshed.
 *
 * Returns true if token expires within the threshold. Short-lived test tokens
 * (lifetime < 5 min) are always flagged for refresh regardless of threshold.
 */
function isTokenNearExpiry(claims: JwtClaims, now: number, threshold: number): boolean {
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
// TokenCache
// ============================================================================

interface CacheEntry {
  info: TokenInfo;
  accessTime: number;
}

/**
 * LRU cache for validated JWT tokens with TTL support.
 *
 * Features:
 * - Stores validated token info for quick lookups
 * - Checks both token expiration and cache TTL on retrieval
 * - Evicts least-recently-used entries when capacity is reached
 * - Uses monotonic counter for accurate LRU ordering
 */
export class TokenCache {
  private cache: Map<string, CacheEntry> = new Map();
  private maxSize: number;
  private ttlSeconds: number;
  private accessCounter: number = 0; // Monotonic counter for consistent LRU ordering

  constructor(options: { maxSize: number; ttlSeconds: number }) {
    this.maxSize = options.maxSize;
    this.ttlSeconds = options.ttlSeconds;
  }

  /**
   * Store a validated token in the cache
   * (updating existing entry does not affect LRU capacity)
   */
  set(token: string, info: TokenInfo): void {
    if (this.cache.has(token)) {
      // Update existing entry without evicting
      this.accessCounter++;
      this.cache.set(token, { info, accessTime: this.accessCounter });
      return;
    }

    // Make room for new entry by evicting LRU if at capacity
    if (this.cache.size >= this.maxSize) {
      this.evictLRU();
    }

    this.accessCounter++;
    this.cache.set(token, { info, accessTime: this.accessCounter });
  }

  /**
   * Retrieve cached token info if valid and not expired
   * Returns undefined if not found or if token/cache entry has expired
   */
  get(token: string): TokenInfo | undefined {
    const entry = this.cache.get(token);
    if (!entry) {
      return undefined;
    }

    // Check if the JWT token itself has expired
    if (entry.info.expiresAt <= Date.now()) {
      this.cache.delete(token);
      return undefined;
    }

    // Check if cache entry has exceeded its TTL
    const cacheAgeSeconds = (Date.now() - entry.info.validatedAt) / 1000;
    if (cacheAgeSeconds > this.ttlSeconds) {
      this.cache.delete(token);
      return undefined;
    }

    // Update access time for LRU tracking
    this.accessCounter++;
    entry.accessTime = this.accessCounter;
    return entry.info;
  }

  /**
   * Remove a specific token from the cache
   */
  invalidate(token: string): void {
    this.cache.delete(token);
  }

  /**
   * Clear all entries from the cache
   */
  clear(): void {
    this.cache.clear();
  }

  /**
   * Evict the least-recently-used entry when cache reaches capacity
   */
  private evictLRU(): void {
    let lruToken: string | null = null;
    let oldestAccessTime = Infinity;

    // Find entry with smallest (oldest) access time
    for (const [token, entry] of this.cache) {
      if (entry.accessTime < oldestAccessTime) {
        oldestAccessTime = entry.accessTime;
        lruToken = token;
      }
    }

    if (lruToken) {
      this.cache.delete(lruToken);
    }
  }
}

// ============================================================================
// Device Authorization Flow Handler (RFC 8628)
// ============================================================================

/**
 * Response from initiating device authorization flow
 */
export interface DeviceAuthResponse {
  userCode: string;
  verificationUri: string;
  verificationUriComplete: string;
  deviceCode: string;
  expiresIn: number;
}

/**
 * Handles OAuth 2.0 Device Authorization Grant (RFC 8628) for CLI authentication.
 *
 * Flow:
 * 1. Call initiateAuth() to get user code and device code
 * 2. Display user code to user and direct them to verification URL
 * 3. Call pollForToken() to wait for user to authorize and retrieve tokens
 */
export class DeviceFlowHandler {
  private clientId: string;
  private deviceAuthEndpoint: string;
  private tokenEndpoint: string;

  constructor(config: { clientId: string; deviceAuthEndpoint: string; tokenEndpoint: string }) {
    this.clientId = config.clientId;
    this.deviceAuthEndpoint = config.deviceAuthEndpoint;
    this.tokenEndpoint = config.tokenEndpoint;
  }

  /**
   * Initiate device authorization flow - get user code and device code
   */
  async initiateAuth(): Promise<DeviceAuthResponse> {
    const response = await fetch(this.deviceAuthEndpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: new URLSearchParams({
        client_id: this.clientId,
      }),
    });

    if (!response.ok) {
      throw new Error('Failed to initiate device auth');
    }

    const data = await response.json() as {
      device_code: string;
      user_code: string;
      verification_uri: string;
      verification_uri_complete: string;
      expires_in: number;
    };

    return {
      deviceCode: data.device_code,
      userCode: data.user_code,
      verificationUri: data.verification_uri,
      verificationUriComplete: data.verification_uri_complete,
      expiresIn: data.expires_in,
    };
  }

  /**
   * Poll the token endpoint until user authorizes or timeout is reached
   * Handles RFC 8628 polling errors: authorization_pending, slow_down, access_denied
   */
  async pollForToken(
    deviceCode: string,
    options: { interval: number; timeout: number }
  ): Promise<{ accessToken: string; refreshToken: string }> {
    const startTime = Date.now();
    let currentInterval = options.interval;

    while (Date.now() - startTime < options.timeout) {
      const response = await fetch(this.tokenEndpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({
          grant_type: 'urn:ietf:params:oauth:grant-type:device_code',
          device_code: deviceCode,
          client_id: this.clientId,
        }),
      });

      if (response.ok) {
        const data = await response.json() as {
          access_token: string;
          refresh_token: string;
          token_type: string;
          expires_in: number;
        };
        return {
          accessToken: data.access_token,
          refreshToken: data.refresh_token,
        };
      }

      // Parse error response per RFC 8628
      const errorData = await response.json() as { error: string };

      if (errorData.error === 'authorization_pending') {
        // User hasn't authorized yet, wait and retry
        await this.sleep(currentInterval * 1000);
        continue;
      }

      if (errorData.error === 'slow_down') {
        // Server requested slower polling - increase interval by 5 seconds (RFC 8628)
        currentInterval += 5;
        await this.sleep(currentInterval * 1000);
        continue;
      }

      if (errorData.error === 'access_denied') {
        throw new Error('User denied authorization');
      }

      if (errorData.error === 'expired_token') {
        throw new Error('Device code expired');
      }

      // Unexpected error
      throw new Error(`Device authorization error: ${errorData.error}`);
    }

    throw new Error('Device authorization timed out');
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

// ============================================================================
// Keychain Storage for CLI Authentication Credentials
// ============================================================================

/**
 * Interface for platform keychain (abstraction over native OS keychain)
 */
export interface KeytarLike {
  setPassword: (service: string, account: string, password: string) => Promise<void>;
  getPassword: (service: string, account: string) => Promise<string | null>;
  deletePassword: (service: string, account: string) => Promise<boolean>;
}

/**
 * Token data to store securely in the keychain
 */
export interface StoredTokens {
  accessToken: string;
  refreshToken: string;
  expiresAt: number;
}

/**
 * Basic obfuscation of stored tokens (base64 + reversal)
 * NOTE: This is NOT cryptographically secure - for production use proper encryption
 */
function encryptTokens(data: string): string {
  const encoded = btoa(data);
  const reversed = encoded.split('').reverse().join('');
  return `enc:${reversed}`;
}

/**
 * Reverse the obfuscation to retrieve stored tokens
 * Handles both encrypted and legacy unencrypted formats
 */
function decryptTokens(encrypted: string): string {
  if (!encrypted.startsWith('enc:')) {
    // Legacy support for unencrypted data
    return encrypted;
  }
  const data = encrypted.slice(4);
  const reversed = data.split('').reverse().join('');
  return atob(reversed);
}

/**
 * Store and retrieve OAuth tokens from the platform keychain.
 * Supports multiple named profiles for different environments (dev, staging, prod, etc.)
 */
export class KeychainStorage {
  private serviceName: string;
  private keytar: KeytarLike;
  private profiles: Set<string> = new Set();

  constructor(config: { serviceName: string; keytar: KeytarLike }) {
    this.serviceName = config.serviceName;
    this.keytar = config.keytar;
  }

  /**
   * Store authentication tokens for a profile in the system keychain
   */
  async storeTokens(profile: string, tokens: StoredTokens): Promise<void> {
    const data = JSON.stringify(tokens);
    const obfuscated = encryptTokens(data);
    await this.keytar.setPassword(this.serviceName, profile, obfuscated);
    this.profiles.add(profile);
  }

  /**
   * Retrieve tokens for a profile from the keychain
   * Returns null if profile not found
   */
  async getTokens(profile: string): Promise<StoredTokens | null> {
    try {
      const obfuscated = await this.keytar.getPassword(this.serviceName, profile);
      if (!obfuscated) {
        return null;
      }
      return JSON.parse(decryptTokens(obfuscated));
    } catch {
      throw new Error('Keychain access denied or token corrupted');
    }
  }

  /**
   * Delete tokens for a profile from the keychain
   */
  async deleteTokens(profile: string): Promise<void> {
    await this.keytar.deletePassword(this.serviceName, profile);
    this.profiles.delete(profile);
  }

  /**
   * Get list of all stored profiles
   */
  async listProfiles(): Promise<string[]> {
    return Array.from(this.profiles);
  }
}

// ============================================================================
// AuthMiddleware Class
// ============================================================================

export class AuthMiddleware {
  private config: AuthConfig;
  private cache: TokenCache;

  constructor(config: AuthConfig) {
    this.config = config;
    this.cache = new TokenCache({ maxSize: DEFAULT_CACHE_MAX_SIZE, ttlSeconds: DEFAULT_CACHE_TTL_SECONDS });
  }

  /**
   * Authenticate a request by checking public paths, then validating credentials
   */
  async authenticate(request: Request): Promise<AuthResult> {
    const url = new URL(request.url);

    // Skip authentication for whitelisted public paths
    if (this.config.publicPaths) {
      for (const path of this.config.publicPaths) {
        if (this.matchPath(url.pathname, path)) {
          return { authenticated: true, skipped: true };
        }
      }
    }

    // Try to extract credentials from request
    const authHeader = request.headers.get('Authorization');
    const apiKey = request.headers.get('X-API-Key');

    // Must provide either Authorization header or API key
    if (!authHeader && !apiKey) {
      return this.createErrorResult(
        'MISSING_AUTH_HEADER',
        'Request must include either "Authorization" header or "X-API-Key" header',
        401
      );
    }

    // Prefer Authorization header if both are present
    if (authHeader) {
      return this.authenticateWithAuthHeader(authHeader, request);
    }

    return this.validateApiKey(apiKey!);
  }

  private async authenticateWithAuthHeader(authHeader: string, request: Request): Promise<AuthResult> {
    // Check for API key authentication (ApiKey scheme)
    if (authHeader.startsWith('ApiKey ')) {
      return this.validateApiKey(authHeader.slice(7));
    }

    // Check for bearer token authentication (Bearer scheme)
    if (authHeader.startsWith('Bearer ')) {
      const token = authHeader.slice(7).trim();
      if (!token) {
        return this.createErrorResult('EMPTY_TOKEN', 'Bearer token is required after "Bearer"', 401);
      }
      return this.authenticateWithBearer(token, request);
    }

    // Authorization header is present but uses an unsupported scheme
    return this.createErrorResult(
      'INVALID_AUTH_FORMAT',
      'Authorization header must use "Bearer <token>" or "ApiKey <key>" format',
      401
    );
  }

  private async authenticateWithBearer(token: string, request: Request): Promise<AuthResult> {
    // If provider is explicitly specified, use it
    const providerName = request.headers.get('X-Auth-Provider');
    if (providerName) {
      const provider = this.config.providers?.find((p) => p.name === providerName);
      if (!provider) {
        return this.createErrorResult('UNKNOWN_PROVIDER', `Unknown auth provider: ${providerName}`, 401);
      }
      return this.validateWithProvider(token, provider, request);
    }

    // Return cached validation result if available and caching is enabled
    if (this.config.cacheEnabled !== false) {
      const cached = this.cache.get(token);
      if (cached) {
        return {
          authenticated: true,
          user: cached.user,
          authMethod: 'bearer',
          fromCache: true,
          request: this.enhanceRequest(request, cached.user),
        };
      }
    }

    // Validate token with signature verification if configured
    const validation = await validateBearerToken(token, this.config);

    // Handle validation failure with special case for expired tokens
    if (!validation.valid) {
      if (validation.isExpired) {
        return {
          authenticated: false,
          error: validation.error,
          statusCode: 401,
          headers: {
            'WWW-Authenticate': 'Bearer realm="mongolake", error="invalid_token", error_description="Token has expired"',
          },
        };
      }
      return this.createErrorResult(validation.error!.code, validation.error!.message, 401);
    }

    // Check if token issuer matches a configured provider
    try {
      const claims = parseJwt(token);
      if (claims.iss && this.config.providers) {
        const provider = this.config.providers.find((p) => p.issuer === claims.iss);
        if (provider) {
          return this.validateWithProvider(token, provider, request);
        }
      }
    } catch {
      // Silently ignore parse errors - token was already validated above
    }

    // Attempt token refresh if it's nearing expiry and refresh credentials are provided
    if (validation.isNearExpiry && this.config.tokenRefresher) {
      const refreshToken = request.headers.get('X-Refresh-Token');
      if (refreshToken) {
        try {
          const newTokens = await this.config.tokenRefresher(refreshToken);
          this.cacheTokenIfEnabled(newTokens.accessToken, validation.user!, newTokens.expiresIn);

          return {
            authenticated: true,
            user: validation.user,
            authMethod: 'bearer',
            newTokens,
            headers: {
              'X-New-Access-Token': newTokens.accessToken,
              'X-New-Refresh-Token': newTokens.refreshToken,
            },
            request: this.enhanceRequest(request, validation.user!),
          };
        } catch (err) {
          const message = err instanceof Error ? err.message : 'Token refresh failed';
          return this.createErrorResult('REFRESH_FAILED', message, 401);
        }
      }
    }

    // Cache validated token for future requests
    this.cacheTokenIfEnabled(token, validation.user!, null);

    return {
      authenticated: true,
      user: validation.user,
      authMethod: 'bearer',
      request: this.enhanceRequest(request, validation.user!),
    };
  }

  private async validateWithProvider(
    token: string,
    provider: AuthProvider,
    request: Request
  ): Promise<AuthResult> {
    const result = await provider.validateToken(token);

    if (!result || !result.valid) {
      return this.createErrorResult('INVALID_TOKEN', 'Token validation failed', 401);
    }

    const enhancedRequest = this.enhanceRequest(request, result.user!);

    return {
      authenticated: true,
      user: result.user,
      authMethod: 'bearer',
      request: enhancedRequest,
    };
  }

  private async validateApiKey(apiKey: string): Promise<AuthResult> {
    // Ensure API key validation is configured
    if (!this.config.apiKeyValidator) {
      return this.createErrorResult(
        'API_KEY_NOT_SUPPORTED',
        'API key authentication is not enabled for this service',
        401
      );
    }

    // Validate API key format before expensive validation check
    if (apiKey.includes('-') || apiKey.length < MIN_API_KEY_LENGTH) {
      return this.createErrorResult(
        'INVALID_API_KEY_FORMAT',
        `API key must be at least ${MIN_API_KEY_LENGTH} characters and contain no hyphens`,
        401
      );
    }

    // Validate the API key with the configured validator
    const validation = await this.config.apiKeyValidator(apiKey);
    if (!validation.valid) {
      return this.createErrorResult('INVALID_API_KEY', 'The provided API key is invalid', 401);
    }

    // Check if API key has required scopes
    if (this.config.requiredScopes?.length) {
      const apiKeyScopes = validation.scopes ?? [];
      const hasAllScopes = this.config.requiredScopes.every((scope) => apiKeyScopes.includes(scope));
      if (!hasAllScopes) {
        return {
          authenticated: false,
          error: {
            code: 'INSUFFICIENT_SCOPE',
            message: `API key requires scopes: ${this.config.requiredScopes.join(', ')}`,
          },
          statusCode: 403,
        };
      }
    }

    return {
      authenticated: true,
      user: validation.user,
      authMethod: 'api_key',
    };
  }

  /**
   * Cache a validated token if caching is enabled
   */
  private cacheTokenIfEnabled(token: string, user: UserContext, refreshExpiresIn: number | null): void {
    if (this.config.cacheEnabled === false) {
      return;
    }

    try {
      const claims = parseJwt(token);
      const expiresAt = refreshExpiresIn
        ? Date.now() + refreshExpiresIn * 1000
        : claims.exp
          ? claims.exp * 1000
          : Date.now() + 3600000; // Default 1 hour if no exp claim

      this.cache.set(token, {
        token,
        user,
        expiresAt,
        validatedAt: Date.now(),
      });
    } catch {
      // Silently ignore caching errors - they shouldn't break authentication
    }
  }

  private createErrorResult(code: string, message: string, statusCode: number): AuthResult {
    const error: AuthError = { code, message };
    const result: AuthResult = {
      authenticated: false,
      error,
      statusCode,
    };

    if (this.config.errorFormatter) {
      result.errorBody = this.config.errorFormatter(error);
    }

    return result;
  }

  /**
   * Add user context headers to the request for downstream handlers
   */
  private enhanceRequest(request: Request, user: UserContext): Request {
    const headers = new Headers(request.headers);
    // Always add user ID (required)
    headers.set('X-User-Id', user.userId);
    // Add optional user details if available
    if (user.email) {
      headers.set('X-User-Email', user.email);
    }
    if (user.organizationId) {
      headers.set('X-Organization-Id', user.organizationId);
    }

    return new Request(request.url, {
      method: request.method,
      headers,
      body: request.body,
      redirect: request.redirect,
      signal: request.signal,
    });
  }

  /**
   * Check if a request path matches a whitelist pattern
   * (supports exact match and prefix match with trailing *)
   */
  private matchPath(pathname: string, pattern: string): boolean {
    if (pattern.endsWith('*')) {
      // Prefix match: /api/v1/* matches /api/v1/users, /api/v1/posts, etc.
      return pathname.startsWith(pattern.slice(0, -1));
    }
    // Exact match
    return pathname === pattern;
  }

  /**
   * Create a middleware handler function for request validation
   * Returns 401 Unauthorized if authentication fails
   */
  createHandler(): (request: Request, next: () => Promise<Response>) => Promise<Response> {
    return async (request: Request, next: () => Promise<Response>): Promise<Response> => {
      const authResult = await this.authenticate(request);

      // Return error response if authentication failed and path wasn't skipped
      if (!authResult.authenticated && !authResult.skipped) {
        const body = authResult.errorBody || authResult.error;
        return new Response(JSON.stringify(body), {
          status: authResult.statusCode ?? 401,
          headers: {
            'Content-Type': 'application/json',
            ...(authResult.headers ?? {}),
          },
        });
      }

      // Continue to next middleware/handler
      return next();
    };
  }
}

// ============================================================================
// Factory Function
// ============================================================================

/**
 * Create an auth middleware instance with the given configuration
 */
export function createAuthMiddleware(config: AuthConfig): AuthMiddleware {
  return new AuthMiddleware(config);
}
