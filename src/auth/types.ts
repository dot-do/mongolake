/**
 * Auth Types for MongoLake
 *
 * Centralized type definitions for authentication, authorization,
 * and audit logging.
 */

// ============================================================================
// Core Auth Types
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
  /** OAuth 2.0 authorization endpoint for authorization code flow */
  authorizationEndpoint?: string;
  /** Redirect URI for OAuth 2.0 authorization code flow */
  redirectUri?: string;
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
  /** Public key in PEM format for RS256/ES256 JWT signature verification */
  jwtPublicKey?: string;
  /** JWKs endpoint URL for fetching signing keys (supports key rotation) */
  jwksUri?: string;
  /** Skip signature verification (for testing only - NOT recommended for production) */
  skipSignatureVerification?: boolean;
  /** RBAC configuration for database/collection permissions */
  rbac?: RBACConfig;
  /** Audit logger for authentication events */
  auditLogger?: AuditLogger;
  /** Clock tolerance in seconds for JWT time-based claims (default: 0) */
  clockToleranceSeconds?: number;
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
// RBAC Types
// ============================================================================

/** Database/collection permission levels */
export type PermissionLevel = 'none' | 'read' | 'write' | 'admin';

/** Permission for a specific resource (database or collection) */
export interface ResourcePermission {
  /** Database name (use '*' for all databases) */
  database: string;
  /** Collection name (use '*' for all collections in database) */
  collection?: string;
  /** Permission level */
  level: PermissionLevel;
}

/** Role definition with associated permissions */
export interface RoleDefinition {
  /** Role name */
  name: string;
  /** Permissions granted by this role */
  permissions: ResourcePermission[];
  /** Inherits permissions from these roles */
  inheritsFrom?: string[];
}

/** RBAC configuration */
export interface RBACConfig {
  /** Role definitions */
  roles: RoleDefinition[];
  /** Default role for users without explicit roles */
  defaultRole?: string;
  /** Whether to enforce RBAC (if false, all authenticated users have full access) */
  enabled: boolean;
}

/** Result of permission check */
export interface PermissionCheckResult {
  allowed: boolean;
  effectivePermission: PermissionLevel;
  matchedRole?: string;
  reason?: string;
}

// ============================================================================
// Audit Logging Types
// ============================================================================

/** Types of authentication events */
export type AuthEventType =
  | 'auth_success'
  | 'auth_failure'
  | 'token_refresh'
  | 'token_refresh_failure'
  | 'permission_denied'
  | 'permission_granted'
  | 'token_revoked'
  | 'jwks_refresh'
  | 'jwks_refresh_failure'
  | 'jwks_key_import_failed';

/** Audit log entry */
export interface AuditLogEntry {
  /** Event type */
  eventType: AuthEventType;
  /** Timestamp of the event */
  timestamp: Date;
  /** User ID (if available) */
  userId?: string;
  /** IP address of the client */
  ipAddress?: string;
  /** User agent string */
  userAgent?: string;
  /** Request path */
  requestPath?: string;
  /** Request method */
  requestMethod?: string;
  /** Database being accessed */
  database?: string;
  /** Collection being accessed */
  collection?: string;
  /** Operation type */
  operation?: string;
  /** Error code (for failure events) */
  errorCode?: string;
  /** Error message (for failure events) */
  errorMessage?: string;
  /** Additional metadata */
  metadata?: Record<string, unknown>;
}

/** Audit logger interface */
export interface AuditLogger {
  /** Log an authentication event */
  log(entry: AuditLogEntry): void | Promise<void>;
}

// ============================================================================
// JWT Types
// ============================================================================

export interface JwtHeader {
  alg: string;
  typ?: string;
  kid?: string;
}

export interface JwtClaims {
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

// ============================================================================
// JWKs Types (RFC 7517)
// ============================================================================

/** JSON Web Key */
export interface JWK {
  /** Key type (RSA, EC, etc.) */
  kty: string;
  /** Key ID */
  kid?: string;
  /** Algorithm */
  alg?: string;
  /** Key usage (sig, enc) */
  use?: string;
  /** Key operations */
  key_ops?: string[];
  // RSA key parameters
  n?: string; // modulus
  e?: string; // exponent
  // EC key parameters
  crv?: string; // curve (P-256, P-384, P-521)
  x?: string; // x coordinate
  y?: string; // y coordinate
}

/** JWKs Set */
export interface JWKSet {
  keys: JWK[];
}

/** Extended verification options including JWKs support */
export interface ExtendedJwtVerificationOptions extends JwtVerificationOptions {
  /** JWKs endpoint URL for fetching signing keys */
  jwksUri?: string;
  /** Pre-fetched JWKs keys */
  jwksKeys?: Map<string, CryptoKey>;
  /** Audit logger for key fetch events */
  auditLogger?: AuditLogger;
}

// ============================================================================
// OAuth Types
// ============================================================================

/** Response from initiating device authorization flow */
export interface DeviceAuthResponse {
  userCode: string;
  verificationUri: string;
  verificationUriComplete: string;
  deviceCode: string;
  expiresIn: number;
}

/** OAuth authorization code response */
export interface AuthorizationCodeResponse {
  code: string;
  state?: string;
}

/** OAuth token exchange response */
export interface TokenExchangeResponse {
  accessToken: string;
  refreshToken?: string;
  expiresIn: number;
  tokenType: string;
  scope?: string;
}

// ============================================================================
// Keychain Types
// ============================================================================

/** Interface for platform keychain (abstraction over native OS keychain) */
export interface KeytarLike {
  setPassword: (service: string, account: string, password: string) => Promise<void>;
  getPassword: (service: string, account: string) => Promise<string | null>;
  deletePassword: (service: string, account: string) => Promise<boolean>;
}

/** Token data to store securely in the keychain */
export interface StoredTokens {
  accessToken: string;
  refreshToken: string;
  expiresAt: number;
}

// ============================================================================
// Bearer Token Validation Types
// ============================================================================

export interface ValidateBearerTokenResult {
  valid: boolean;
  user?: UserContext;
  error?: AuthError;
  isExpired?: boolean;
  isNearExpiry?: boolean;
}
