/**
 * Shared test helpers for Auth Middleware tests
 */

import { vi } from 'vitest';

// Types defined inline for TDD - will be implemented in src/auth/middleware.ts
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
  jwtSecret?: string;
  jwtPublicKey?: string;
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
// Test Fixtures
// ============================================================================

export const mockValidToken = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyXzEyMyIsImVtYWlsIjoidGVzdEBleGFtcGxlLmNvbSIsImlhdCI6MTcwNjc4NjQwMCwiZXhwIjoxNzA2ODcyODAwLCJpc3MiOiJodHRwczovL29hdXRoLmRvIiwiYXVkIjoibW9uZ29sYWtlIn0.signature';
export const mockExpiredToken = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyXzEyMyIsImVtYWlsIjoidGVzdEBleGFtcGxlLmNvbSIsImlhdCI6MTYwMDAwMDAwMCwiZXhwIjoxNjAwMDAwMDAxLCJpc3MiOiJodHRwczovL29hdXRoLmRvIiwiYXVkIjoibW9uZ29sYWtlIn0.signature';
export const mockNearExpiryToken = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyXzEyMyIsImVtYWlsIjoidGVzdEBleGFtcGxlLmNvbSIsImlhdCI6MTcwNjc4NjQwMCwiZXhwIjoxNzA2Nzg2NDYwLCJpc3MiOiJodHRwczovL29hdXRoLmRvIiwiYXVkIjoibW9uZ29sYWtlIn0.signature';
export const mockApiKey = 'mlk_live_abc123def456ghi789';
export const mockRefreshToken = 'refresh_token_xyz789';

export const mockUserContext: UserContext = {
  userId: 'user_123',
  email: 'test@example.com',
  roles: ['user'],
  permissions: ['read', 'write'],
  organizationId: 'org_456',
};

export const mockAuthConfig: AuthConfig = {
  issuer: 'https://oauth.do',
  audience: 'mongolake',
  clientId: 'mongolake-client',
  clientSecret: 'client-secret',
  tokenEndpoint: 'https://oauth.do/token',
  deviceAuthEndpoint: 'https://oauth.do/device',
};

// ============================================================================
// Helper Functions
// ============================================================================

// Test secret for HMAC-SHA256
export const testSecret = 'super-secret-key-for-testing-jwt-signatures-256-bits';

// Helper to create a valid HS256 JWT
export async function createHs256Token(payload: Record<string, unknown>, secret: string): Promise<string> {
  const header = { alg: 'HS256', typ: 'JWT' };
  const headerB64 = btoa(JSON.stringify(header)).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');
  const payloadB64 = btoa(JSON.stringify(payload)).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');
  const signingInput = `${headerB64}.${payloadB64}`;

  // Create HMAC-SHA256 signature using Web Crypto API
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    'raw',
    encoder.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );
  const signatureBuffer = await crypto.subtle.sign('HMAC', key, encoder.encode(signingInput));
  const signatureArray = new Uint8Array(signatureBuffer);
  let signatureB64 = '';
  for (const byte of signatureArray) {
    signatureB64 += String.fromCharCode(byte);
  }
  signatureB64 = btoa(signatureB64).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');

  return `${signingInput}.${signatureB64}`;
}

export function createMockTokenRefresher() {
  return vi.fn().mockResolvedValue({
    accessToken: mockValidToken,
    refreshToken: 'new_refresh_token',
    expiresIn: 3600,
  });
}

export function createMockKeytar() {
  return {
    setPassword: vi.fn().mockResolvedValue(undefined),
    getPassword: vi.fn().mockResolvedValue(null),
    deletePassword: vi.fn().mockResolvedValue(true),
  };
}
