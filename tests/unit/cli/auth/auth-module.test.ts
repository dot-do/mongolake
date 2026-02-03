/**
 * Tests for MongoLake CLI Authentication - Module Exports and Token Storage
 *
 * Tests the module exports and MongoLakeTokenStorage class.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as path from 'node:path';
import * as os from 'node:os';

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
// Test Fixtures
// ============================================================================

const mockAuthResponse = {
  device_code: 'device_code_123',
  user_code: 'ABCD-EFGH',
  verification_uri: 'https://oauth.do/device',
  verification_uri_complete: 'https://oauth.do/device?code=ABCD-EFGH',
  expires_in: 1800,
  interval: 5,
};

const mockTokenResponse = {
  access_token: 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyXzEyMyIsImVtYWlsIjoidGVzdEBleGFtcGxlLmNvbSJ9.signature',
  refresh_token: 'refresh_token_xyz789',
  token_type: 'Bearer',
  expires_in: 3600,
  user: {
    id: 'user_123',
    email: 'test@example.com',
    name: 'Test User',
  },
};

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

const mockExpiredAuth = {
  profiles: {
    default: {
      accessToken: mockTokenResponse.access_token,
      refreshToken: mockTokenResponse.refresh_token,
      expiresAt: Date.now() - 3600 * 1000, // Expired 1 hour ago
      user: {
        id: 'user_123',
        email: 'test@example.com',
        name: 'Test User',
      },
    },
  },
};

// ============================================================================
// Module Import Tests
// ============================================================================

describe('CLI Auth - Module Existence', () => {
  it('should export login function', async () => {
    const module = await import('../../../../src/cli/auth.js');
    expect(typeof module.login).toBe('function');
  });

  it('should export logout function', async () => {
    const module = await import('../../../../src/cli/auth.js');
    expect(typeof module.logout).toBe('function');
  });

  it('should export whoami function', async () => {
    const module = await import('../../../../src/cli/auth.js');
    expect(typeof module.whoami).toBe('function');
  });

  it('should export MongoLakeTokenStorage class', async () => {
    const module = await import('../../../../src/cli/auth.js');
    expect(module.MongoLakeTokenStorage).toBeDefined();
  });

  it('should export getAccessToken function', async () => {
    const module = await import('../../../../src/cli/auth.js');
    expect(typeof module.getAccessToken).toBe('function');
  });

  it('should export isAuthenticated function', async () => {
    const module = await import('../../../../src/cli/auth.js');
    expect(typeof module.isAuthenticated).toBe('function');
  });

  it('should export requireAuth function', async () => {
    const module = await import('../../../../src/cli/auth.js');
    expect(typeof module.requireAuth).toBe('function');
  });

  it('should export createAuthProvider function', async () => {
    const module = await import('../../../../src/cli/auth.js');
    expect(typeof module.createAuthProvider).toBe('function');
  });
});

// ============================================================================
// MongoLakeTokenStorage Tests
// ============================================================================

describe('CLI Auth - MongoLakeTokenStorage', () => {
  let MongoLakeTokenStorage: typeof import('../../../../src/cli/auth.js').MongoLakeTokenStorage;
  let mockExistsSync: ReturnType<typeof vi.fn>;
  let mockReadFileSync: ReturnType<typeof vi.fn>;
  let mockWriteFileSync: ReturnType<typeof vi.fn>;
  let mockMkdirSync: ReturnType<typeof vi.fn>;

  beforeEach(async () => {
    const module = await import('../../../../src/cli/auth.js');
    MongoLakeTokenStorage = module.MongoLakeTokenStorage;

    mockExistsSync = vi.mocked(fs.existsSync);
    mockReadFileSync = vi.mocked(fs.readFileSync);
    mockWriteFileSync = vi.mocked(fs.writeFileSync);
    mockMkdirSync = vi.mocked(fs.mkdirSync);

    vi.clearAllMocks();
  });

  describe('getToken', () => {
    it('should return null when no auth file exists', async () => {
      mockExistsSync.mockReturnValue(false);

      const storage = new MongoLakeTokenStorage();
      const token = await storage.getToken();

      expect(token).toBeNull();
    });

    it('should return token when auth file exists with valid data', async () => {
      mockExistsSync.mockReturnValue(true);
      mockReadFileSync.mockReturnValue(JSON.stringify(mockStoredAuth));

      const storage = new MongoLakeTokenStorage();
      const token = await storage.getToken();

      expect(token).toBe(mockStoredAuth.profiles.default.accessToken);
    });

    it('should return null when token is expired', async () => {
      mockExistsSync.mockReturnValue(true);
      mockReadFileSync.mockReturnValue(JSON.stringify(mockExpiredAuth));

      const storage = new MongoLakeTokenStorage();
      const token = await storage.getToken();

      expect(token).toBeNull();
    });

    it('should return null when profile does not exist', async () => {
      mockExistsSync.mockReturnValue(true);
      mockReadFileSync.mockReturnValue(JSON.stringify({ profiles: {} }));

      const storage = new MongoLakeTokenStorage('nonexistent');
      const token = await storage.getToken();

      expect(token).toBeNull();
    });

    it('should use specified profile', async () => {
      const multiProfileAuth = {
        profiles: {
          default: { accessToken: 'default_token', expiresAt: Date.now() + 3600000 },
          production: { accessToken: 'prod_token', expiresAt: Date.now() + 3600000 },
        },
      };
      mockExistsSync.mockReturnValue(true);
      mockReadFileSync.mockReturnValue(JSON.stringify(multiProfileAuth));

      const storage = new MongoLakeTokenStorage('production');
      const token = await storage.getToken();

      expect(token).toBe('prod_token');
    });
  });

  describe('setToken', () => {
    it('should create new profile when setting token', async () => {
      mockExistsSync.mockReturnValue(false);

      const storage = new MongoLakeTokenStorage();
      await storage.setToken('new_token');

      expect(mockMkdirSync).toHaveBeenCalled();
      expect(mockWriteFileSync).toHaveBeenCalledWith(
        expect.stringContaining('auth.json'),
        expect.stringContaining('new_token'),
        expect.any(Object)
      );
    });

    it('should update existing profile when setting token', async () => {
      mockExistsSync.mockReturnValue(true);
      mockReadFileSync.mockReturnValue(JSON.stringify(mockStoredAuth));

      const storage = new MongoLakeTokenStorage();
      await storage.setToken('updated_token');

      expect(mockWriteFileSync).toHaveBeenCalledWith(
        expect.stringContaining('auth.json'),
        expect.stringContaining('updated_token'),
        expect.any(Object)
      );
    });
  });

  describe('removeToken', () => {
    it('should remove profile from auth data', async () => {
      mockExistsSync.mockReturnValue(true);
      mockReadFileSync.mockReturnValue(JSON.stringify(mockStoredAuth));

      const storage = new MongoLakeTokenStorage();
      await storage.removeToken();

      const writeCall = mockWriteFileSync.mock.calls[0];
      const writtenData = JSON.parse(writeCall[1] as string);
      expect(writtenData.profiles.default).toBeUndefined();
    });
  });

  describe('getTokenData', () => {
    it('should return full token data', async () => {
      mockExistsSync.mockReturnValue(true);
      mockReadFileSync.mockReturnValue(JSON.stringify(mockStoredAuth));

      const storage = new MongoLakeTokenStorage();
      const tokenData = await storage.getTokenData();

      expect(tokenData).toEqual({
        accessToken: mockStoredAuth.profiles.default.accessToken,
        refreshToken: mockStoredAuth.profiles.default.refreshToken,
        expiresAt: mockStoredAuth.profiles.default.expiresAt,
      });
    });

    it('should return null when profile does not exist', async () => {
      mockExistsSync.mockReturnValue(true);
      mockReadFileSync.mockReturnValue(JSON.stringify({ profiles: {} }));

      const storage = new MongoLakeTokenStorage();
      const tokenData = await storage.getTokenData();

      expect(tokenData).toBeNull();
    });
  });

  describe('setTokenData', () => {
    it('should store full token data', async () => {
      mockExistsSync.mockReturnValue(false);

      const storage = new MongoLakeTokenStorage();
      await storage.setTokenData({
        accessToken: 'access_token',
        refreshToken: 'refresh_token',
        expiresAt: 1234567890,
      });

      const writeCall = mockWriteFileSync.mock.calls[0];
      const writtenData = JSON.parse(writeCall[1] as string);
      expect(writtenData.profiles.default.accessToken).toBe('access_token');
      expect(writtenData.profiles.default.refreshToken).toBe('refresh_token');
      expect(writtenData.profiles.default.expiresAt).toBe(1234567890);
    });
  });

  describe('getProfileData / setProfileData', () => {
    it('should store and retrieve full profile data with user info', async () => {
      mockExistsSync.mockReturnValue(true);
      mockReadFileSync.mockReturnValue(JSON.stringify(mockStoredAuth));

      const storage = new MongoLakeTokenStorage();
      const profileData = await storage.getProfileData();

      expect(profileData).toEqual(mockStoredAuth.profiles.default);
      expect(profileData?.user?.email).toBe('test@example.com');
    });

    it('should set profile data including user info', async () => {
      mockExistsSync.mockReturnValue(false);

      const storage = new MongoLakeTokenStorage();
      await storage.setProfileData({
        accessToken: 'token',
        refreshToken: 'refresh',
        expiresAt: 123456,
        user: { id: 'user_1', email: 'user@test.com', name: 'User' },
      });

      const writeCall = mockWriteFileSync.mock.calls[0];
      const writtenData = JSON.parse(writeCall[1] as string);
      expect(writtenData.profiles.default.user.email).toBe('user@test.com');
    });
  });
});

// ============================================================================
// File Path Tests
// ============================================================================

describe('CLI Auth - File Paths', () => {
  it('should use ~/.mongolake/auth.json for storage', async () => {
    vi.mocked(fs.existsSync).mockReturnValue(false);

    const authModule = await import('../../../../src/cli/auth.js');
    const storage = new authModule.MongoLakeTokenStorage();
    await storage.setToken('test_token');

    const writeCall = vi.mocked(fs.writeFileSync).mock.calls[0];
    const writtenPath = writeCall[0] as string;

    expect(writtenPath).toContain('.mongolake');
    expect(writtenPath).toContain('auth.json');
  });

  it('should create config directory with 0700 permissions', async () => {
    vi.mocked(fs.existsSync).mockReturnValue(false);

    const authModule = await import('../../../../src/cli/auth.js');
    const storage = new authModule.MongoLakeTokenStorage();
    await storage.setToken('test_token');

    expect(vi.mocked(fs.mkdirSync)).toHaveBeenCalledWith(
      expect.stringContaining('.mongolake'),
      { mode: 0o700, recursive: true }
    );
  });

  it('should write auth file with 0600 permissions', async () => {
    vi.mocked(fs.existsSync).mockReturnValue(false);

    const authModule = await import('../../../../src/cli/auth.js');
    const storage = new authModule.MongoLakeTokenStorage();
    await storage.setToken('test_token');

    expect(vi.mocked(fs.writeFileSync)).toHaveBeenCalledWith(
      expect.any(String),
      expect.any(String),
      { mode: 0o600 }
    );
  });
});
