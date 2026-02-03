/**
 * Auth Middleware Module Existence Tests
 *
 * Tests for verifying exported functions and classes exist.
 */

import { describe, it, expect } from 'vitest';

describe('Auth Middleware - Module Existence', () => {
  it('should export AuthMiddleware class', async () => {
    const module = await import('../../../src/auth/middleware.js');
    expect(module.AuthMiddleware).toBeDefined();
  });

  it('should export createAuthMiddleware factory function', async () => {
    const module = await import('../../../src/auth/middleware.js');
    expect(typeof module.createAuthMiddleware).toBe('function');
  });

  it('should export validateBearerToken function', async () => {
    const module = await import('../../../src/auth/middleware.js');
    expect(typeof module.validateBearerToken).toBe('function');
  });

  it('should export extractUserContext function', async () => {
    const module = await import('../../../src/auth/middleware.js');
    expect(typeof module.extractUserContext).toBe('function');
  });

  it('should export TokenCache class', async () => {
    const module = await import('../../../src/auth/middleware.js');
    expect(module.TokenCache).toBeDefined();
  });

  it('should export KeychainStorage class', async () => {
    const module = await import('../../../src/auth/middleware.js');
    expect(module.KeychainStorage).toBeDefined();
  });

  it('should export DeviceFlowHandler class', async () => {
    const module = await import('../../../src/auth/middleware.js');
    expect(module.DeviceFlowHandler).toBeDefined();
  });
});
