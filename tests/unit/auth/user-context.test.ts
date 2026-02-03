/**
 * Auth Middleware User Context Extraction Tests
 *
 * Tests for extracting user context from JWT tokens.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { type UserContext, mockValidToken } from './test-helpers.js';

describe('Auth Middleware - User Context Extraction', () => {
  let extractUserContext: (token: string) => UserContext;

  beforeEach(async () => {
    const module = await import('../../../src/auth/middleware.js');
    extractUserContext = module.extractUserContext;
  });

  it('should extract user context from valid JWT token', () => {
    const context = extractUserContext(mockValidToken);

    expect(context).toBeDefined();
    expect(context.userId).toBe('user_123');
    expect(context.email).toBe('test@example.com');
  });

  it('should include roles from token claims', () => {
    const tokenWithRoles = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyXzEyMyIsImVtYWlsIjoidGVzdEBleGFtcGxlLmNvbSIsInJvbGVzIjpbImFkbWluIiwidXNlciJdLCJpYXQiOjE3MDY3ODY0MDAsImV4cCI6MTcwNjg3MjgwMH0.signature';

    const context = extractUserContext(tokenWithRoles);

    expect(context.roles).toContain('admin');
    expect(context.roles).toContain('user');
  });

  it('should extract organization ID from token', () => {
    const tokenWithOrg = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyXzEyMyIsIm9yZ19pZCI6Im9yZ180NTYiLCJpYXQiOjE3MDY3ODY0MDAsImV4cCI6MTcwNjg3MjgwMH0.signature';

    const context = extractUserContext(tokenWithOrg);

    expect(context.organizationId).toBe('org_456');
  });

  it('should handle missing optional claims gracefully', () => {
    const minimalToken = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyXzEyMyIsImlhdCI6MTcwNjc4NjQwMCwiZXhwIjoxNzA2ODcyODAwfQ.signature';

    const context = extractUserContext(minimalToken);

    expect(context.userId).toBe('user_123');
    expect(context.email).toBeUndefined();
    expect(context.roles).toEqual([]);
  });

  it('should throw for invalid token format', () => {
    expect(() => extractUserContext('not.a.valid.jwt')).toThrow();
  });

  it('should extract custom claims into metadata', () => {
    const tokenWithCustomClaims = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyXzEyMyIsInRpZXIiOiJwcm8iLCJxdW90YSI6MTAwMDAsImlhdCI6MTcwNjc4NjQwMCwiZXhwIjoxNzA2ODcyODAwfQ.signature';

    const context = extractUserContext(tokenWithCustomClaims);

    expect(context.metadata?.tier).toBe('pro');
    expect(context.metadata?.quota).toBe(10000);
  });
});
