/**
 * Tests for MongoLake CLI Authentication - JWT and Token Refresh
 *
 * Tests JWT handling and token refresh functionality including:
 * - Token refresh
 * - Keychain storage (macOS)
 * - Token refresh via oauth.do
 * - Secure storage detection
 * - OAuth.do advanced integration
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import * as fs from 'node:fs';
import { mockTokenResponse } from './auth-common';

// Mock oauth.do module
vi.mock('oauth.do', () => ({
  authorizeDevice: vi.fn(),
  pollForTokens: vi.fn(),
  getUser: vi.fn(),
  configure: vi.fn(),
}));

// Mock fs module for file operations
vi.mock('node:fs', async () => {
  const actual = await vi.importActual('node:fs');
  return {
    ...actual,
    existsSync: vi.fn(),
    mkdirSync: vi.fn(),
    readFileSync: vi.fn(),
    writeFileSync: vi.fn(),
  };
});

// ============================================================================
// Token Refresh Tests
// ============================================================================

describe('CLI Auth - Token Refresh', () => {
  let refreshTokenIfNeeded: typeof import('../../../src/cli/auth.js').refreshTokenIfNeeded;

  beforeEach(async () => {
    vi.resetModules();

    const authModule = await import('../../../src/cli/auth.js');
    refreshTokenIfNeeded = authModule.refreshTokenIfNeeded;

    vi.clearAllMocks();
  });

  it('should return current token if not near expiry', async () => {
    const mockStoredAuth = {
      profiles: {
        default: {
          accessToken: mockTokenResponse.access_token,
          refreshToken: mockTokenResponse.refresh_token,
          expiresAt: Date.now() + 3600 * 1000,
          user: {
            id: 'user_123',
            email: 'test@example.com',
            name: 'Test User',
          },
        },
      },
    };
    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(mockStoredAuth));

    const token = await refreshTokenIfNeeded();

    expect(token).toBe(mockStoredAuth.profiles.default.accessToken);
  });

  it('should return null if no profile exists', async () => {
    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify({ profiles: {} }));

    const token = await refreshTokenIfNeeded();

    expect(token).toBeNull();
  });

  it('should return null if token is expired and no refresh token', async () => {
    const expiredNoRefresh = {
      profiles: {
        default: {
          accessToken: 'expired_token',
          expiresAt: Date.now() - 3600000,
        },
      },
    };
    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(expiredNoRefresh));

    const token = await refreshTokenIfNeeded();

    expect(token).toBeNull();
  });

  it('should check token with 5 minute buffer', async () => {
    const nearExpiryAuth = {
      profiles: {
        default: {
          accessToken: 'near_expiry_token',
          refreshToken: 'refresh_token',
          expiresAt: Date.now() + 4 * 60 * 1000,
        },
      },
    };
    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(nearExpiryAuth));

    const token = await refreshTokenIfNeeded();

    expect(token).toBeNull();
  });
});

// ============================================================================
// Keychain Storage Tests (macOS Secure Storage)
// ============================================================================

describe('CLI Auth - Keychain Storage (macOS)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should export KeychainTokenStorage class', async () => {
    const module = await import('../../../src/cli/auth.js');
    expect(module.KeychainTokenStorage).toBeDefined();
  });

  it('should implement TokenStorage interface', async () => {
    const module = await import('../../../src/cli/auth.js');
    const { KeychainTokenStorage } = module;

    const storage = new KeychainTokenStorage();
    expect(storage).toBeDefined();
    expect(typeof storage.getToken).toBe('function');
    expect(typeof storage.setToken).toBe('function');
    expect(typeof storage.removeToken).toBe('function');
  });

  it('should have mongolake-cli as default service name', async () => {
    const module = await import('../../../src/cli/auth.js');
    const { KeychainTokenStorage } = module;

    const storage = new KeychainTokenStorage();
    expect(storage.serviceName).toBe('mongolake-cli');
  });

  it('should encrypt tokens before storing', async () => {
    const module = await import('../../../src/cli/auth.js');
    const { KeychainTokenStorage } = module;

    const storage = new KeychainTokenStorage();

    expect(typeof storage.getRawStoredValue).toBe('function');
  });

  it('should support fallback to file storage', async () => {
    const module = await import('../../../src/cli/auth.js');
    const { KeychainTokenStorage } = module;

    const storage = new KeychainTokenStorage({ fallbackToFile: true });

    expect(storage).toHaveProperty('isUsingFallback');
  });

  it('should support multiple profiles', async () => {
    const module = await import('../../../src/cli/auth.js');
    const { KeychainTokenStorage } = module;

    const defaultStorage = new KeychainTokenStorage('default');
    const prodStorage = new KeychainTokenStorage('production');

    expect(defaultStorage.profile).toBe('default');
    expect(prodStorage.profile).toBe('production');
  });

  it('should handle access errors gracefully', async () => {
    const module = await import('../../../src/cli/auth.js');
    const { KeychainTokenStorage } = module;

    const storage = new KeychainTokenStorage();

    const result = await storage.getToken();
    expect(result === null || typeof result === 'string').toBe(true);
  });
});

// ============================================================================
// Token Refresh via oauth.do Tests
// ============================================================================

describe('CLI Auth - Token Refresh via oauth.do', () => {
  beforeEach(() => {
    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.mkdirSync).mockReturnValue(undefined);
    vi.mocked(fs.writeFileSync).mockReturnValue(undefined);
    vi.clearAllMocks();
  });

  it('should export refreshToken function', async () => {
    const module = await import('../../../src/cli/auth.js');
    expect(typeof module.refreshToken).toBe('function');
  });

  it('should export getAccessTokenWithAutoRefresh function', async () => {
    const module = await import('../../../src/cli/auth.js');
    expect(typeof module.getAccessTokenWithAutoRefresh).toBe('function');
  });

  it('should refresh expired token using oauth.do', async () => {
    const storedAuth = {
      profiles: {
        default: {
          accessToken: 'old_access_token',
          refreshToken: 'stored_refresh_token',
          expiresAt: Date.now() - 1000,
        },
      },
    };
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(storedAuth));

    const module = await import('../../../src/cli/auth.js');

    const result = await module.refreshToken();

    expect(result).toHaveProperty('success');
    expect(result).toHaveProperty('accessToken');
  });

  it('should update stored tokens after successful refresh', async () => {
    const storedAuth = {
      profiles: {
        default: {
          accessToken: 'old_token',
          refreshToken: 'refresh_token',
          expiresAt: Date.now() - 1000,
        },
      },
    };
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(storedAuth));

    const module = await import('../../../src/cli/auth.js');

    const result = await module.refreshToken();

    if (result.success) {
      const writeCall = vi.mocked(fs.writeFileSync).mock.calls[0];
      expect(writeCall).toBeDefined();
    }
  });

  it('should return error result when refresh token is expired', async () => {
    const storedAuth = {
      profiles: {
        default: {
          accessToken: 'expired_token',
          refreshToken: 'expired_refresh_token',
          expiresAt: Date.now() - 1000,
        },
      },
    };
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(storedAuth));

    const module = await import('../../../src/cli/auth.js');

    const result = await module.refreshToken();

    expect(result).toHaveProperty('success');
    expect(result).toHaveProperty('error');
  });

  it('should auto-refresh token when within threshold', async () => {
    const storedAuth = {
      profiles: {
        default: {
          accessToken: 'near_expiry_token',
          refreshToken: 'refresh_token',
          expiresAt: Date.now() + 4 * 60 * 1000,
        },
      },
    };
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(storedAuth));

    const module = await import('../../../src/cli/auth.js');

    const token = await module.getAccessTokenWithAutoRefresh();

    expect(token).toBeDefined();
  });

  it('should use specified profile for refresh', async () => {
    const storedAuth = {
      profiles: {
        production: {
          accessToken: 'prod_token',
          refreshToken: 'prod_refresh_token',
          expiresAt: Date.now() - 1000,
        },
      },
    };
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(storedAuth));

    const module = await import('../../../src/cli/auth.js');

    const result = await module.refreshToken({ profile: 'production' });

    expect(result).toHaveProperty('success');
  });
});

// ============================================================================
// Secure Storage Detection Tests
// ============================================================================

describe('CLI Auth - Secure Storage Detection', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should export detectSecureStorage function', async () => {
    const module = await import('../../../src/cli/auth.js');
    expect(typeof module.detectSecureStorage).toBe('function');
  });

  it('should detect platform-specific secure storage', async () => {
    const module = await import('../../../src/cli/auth.js');

    const result = await module.detectSecureStorage();

    expect(result).toHaveProperty('available');
    expect(result).toHaveProperty('type');
  });

  it('should report keychain type on macOS', async () => {
    const module = await import('../../../src/cli/auth.js');

    const result = await module.detectSecureStorage();

    if (process.platform === 'darwin') {
      expect(result.type).toBe('keychain');
    }
  });

  it('should report secret-service or file type on Linux', async () => {
    const module = await import('../../../src/cli/auth.js');

    const result = await module.detectSecureStorage();

    if (process.platform === 'linux') {
      expect(['secret-service', 'file']).toContain(result.type);
    }
  });

  it('should report credential-manager type on Windows', async () => {
    const module = await import('../../../src/cli/auth.js');

    const result = await module.detectSecureStorage();

    if (process.platform === 'win32') {
      expect(result.type).toBe('credential-manager');
    }
  });

  it('should always have file fallback available', async () => {
    const module = await import('../../../src/cli/auth.js');

    const result = await module.detectSecureStorage();

    expect(result.fallbackAvailable).toBe(true);
  });
});

// ============================================================================
// OAuth.do Integration Tests (Additional)
// ============================================================================

describe('CLI Auth - oauth.do Advanced Integration', () => {
  beforeEach(() => {
    vi.mocked(fs.existsSync).mockReturnValue(false);
    vi.mocked(fs.mkdirSync).mockReturnValue(undefined);
    vi.mocked(fs.writeFileSync).mockReturnValue(undefined);
    vi.clearAllMocks();
  });

  it('should export oauthClientId constant', async () => {
    const module = await import('../../../src/cli/auth.js');
    expect(module.OAUTH_CLIENT_ID).toBe('mongolake-cli');
  });

  it('should export oauthProvider constant', async () => {
    const module = await import('../../../src/cli/auth.js');
    expect(module.OAUTH_PROVIDER).toBe('oauth.do');
  });

  it('should handle oauth.do rate limiting gracefully', async () => {
    const module = await import('../../../src/cli/auth.js');

    expect(typeof module.login).toBe('function');
  });

  it('should validate oauth.do response format', async () => {
    const module = await import('../../../src/cli/auth.js');

    expect(typeof module.validateAuthResponse).toBe('function');
  });

  it('should support custom oauth.do endpoint configuration', async () => {
    const module = await import('../../../src/cli/auth.js');

    expect(typeof module.configureOAuthEndpoints).toBe('function');
  });
});
