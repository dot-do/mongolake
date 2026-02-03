/**
 * Tests for MongoLake CLI Authentication - OAuth Device Flow
 *
 * Tests the OAuth device flow authentication including:
 * - Module exports
 * - MongoLakeTokenStorage class
 * - Login flow with device authorization
 * - Logout functionality
 * - Whoami command
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import * as fs from 'node:fs';
import {
  mockAuthResponse,
  mockTokenResponse,
  mockStoredAuth,
  mockExpiredAuth,
} from './auth-common';

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
// Module Import Tests
// ============================================================================

describe('CLI Auth - Module Existence', () => {
  it('should export login function', async () => {
    const module = await import('../../../src/cli/auth.js');
    expect(typeof module.login).toBe('function');
  });

  it('should export logout function', async () => {
    const module = await import('../../../src/cli/auth.js');
    expect(typeof module.logout).toBe('function');
  });

  it('should export whoami function', async () => {
    const module = await import('../../../src/cli/auth.js');
    expect(typeof module.whoami).toBe('function');
  });

  it('should export MongoLakeTokenStorage class', async () => {
    const module = await import('../../../src/cli/auth.js');
    expect(module.MongoLakeTokenStorage).toBeDefined();
  });

  it('should export getAccessToken function', async () => {
    const module = await import('../../../src/cli/auth.js');
    expect(typeof module.getAccessToken).toBe('function');
  });

  it('should export isAuthenticated function', async () => {
    const module = await import('../../../src/cli/auth.js');
    expect(typeof module.isAuthenticated).toBe('function');
  });

  it('should export requireAuth function', async () => {
    const module = await import('../../../src/cli/auth.js');
    expect(typeof module.requireAuth).toBe('function');
  });

  it('should export createAuthProvider function', async () => {
    const module = await import('../../../src/cli/auth.js');
    expect(typeof module.createAuthProvider).toBe('function');
  });
});

// ============================================================================
// MongoLakeTokenStorage Tests
// ============================================================================

describe('CLI Auth - MongoLakeTokenStorage', () => {
  let MongoLakeTokenStorage: typeof import('../../../src/cli/auth.js').MongoLakeTokenStorage;
  let mockExistsSync: ReturnType<typeof vi.fn>;
  let mockReadFileSync: ReturnType<typeof vi.fn>;
  let mockWriteFileSync: ReturnType<typeof vi.fn>;
  let mockMkdirSync: ReturnType<typeof vi.fn>;

  beforeEach(async () => {
    const module = await import('../../../src/cli/auth.js');
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
// Login Flow Tests
// ============================================================================

describe('CLI Auth - Login Flow', () => {
  let login: typeof import('../../../src/cli/auth.js').login;
  let mockAuthorizeDevice: ReturnType<typeof vi.fn>;
  let mockPollForTokens: ReturnType<typeof vi.fn>;
  let mockConfigure: ReturnType<typeof vi.fn>;
  let consoleSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    vi.resetModules();

    const oauthModule = await import('oauth.do');
    mockAuthorizeDevice = vi.mocked(oauthModule.authorizeDevice);
    mockPollForTokens = vi.mocked(oauthModule.pollForTokens);
    mockConfigure = vi.mocked(oauthModule.configure);

    const authModule = await import('../../../src/cli/auth.js');
    login = authModule.login;

    vi.mocked(fs.existsSync).mockReturnValue(false);
    vi.mocked(fs.mkdirSync).mockReturnValue(undefined);
    vi.mocked(fs.writeFileSync).mockReturnValue(undefined);

    consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    vi.clearAllMocks();
  });

  afterEach(() => {
    consoleSpy.mockRestore();
  });

  it('should configure oauth.do with MongoLake client ID', async () => {
    mockAuthorizeDevice.mockResolvedValue(mockAuthResponse);
    mockPollForTokens.mockResolvedValue(mockTokenResponse);

    await login();

    expect(mockConfigure).toHaveBeenCalledWith({
      clientId: 'mongolake-cli',
    });
  });

  it('should initiate device authorization', async () => {
    mockAuthorizeDevice.mockResolvedValue(mockAuthResponse);
    mockPollForTokens.mockResolvedValue(mockTokenResponse);

    await login();

    expect(mockAuthorizeDevice).toHaveBeenCalled();
  });

  it('should display verification URL and user code', async () => {
    mockAuthorizeDevice.mockResolvedValue(mockAuthResponse);
    mockPollForTokens.mockResolvedValue(mockTokenResponse);

    await login();

    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('https://oauth.do/device'));
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('ABCD-EFGH'));
  });

  it('should poll for tokens with correct parameters', async () => {
    mockAuthorizeDevice.mockResolvedValue(mockAuthResponse);
    mockPollForTokens.mockResolvedValue(mockTokenResponse);

    await login();

    expect(mockPollForTokens).toHaveBeenCalledWith(
      'device_code_123',
      5,
      expect.any(Number)
    );
  });

  it('should store tokens after successful authorization', async () => {
    mockAuthorizeDevice.mockResolvedValue(mockAuthResponse);
    mockPollForTokens.mockResolvedValue(mockTokenResponse);

    const result = await login();

    expect(result.success).toBe(true);
    expect(result.user?.userId).toBe('user_123');
    expect(result.user?.email).toBe('test@example.com');
    expect(vi.mocked(fs.writeFileSync)).toHaveBeenCalled();
  });

  it('should return success result with user info', async () => {
    mockAuthorizeDevice.mockResolvedValue(mockAuthResponse);
    mockPollForTokens.mockResolvedValue(mockTokenResponse);

    const result = await login();

    expect(result.success).toBe(true);
    expect(result.user).toBeDefined();
    expect(result.user?.userId).toBe('user_123');
    expect(result.user?.email).toBe('test@example.com');
    expect(result.user?.name).toBe('Test User');
  });

  it('should handle authorization failure gracefully', async () => {
    mockAuthorizeDevice.mockRejectedValue(new Error('Network error'));

    const result = await login();

    expect(result.success).toBe(false);
    expect(result.error).toBe('Network error');
  });

  it('should handle polling timeout', async () => {
    mockAuthorizeDevice.mockResolvedValue(mockAuthResponse);
    mockPollForTokens.mockRejectedValue(new Error('Device authorization timed out'));

    const result = await login();

    expect(result.success).toBe(false);
    expect(result.error).toContain('timed out');
  });

  it('should handle user denied authorization', async () => {
    mockAuthorizeDevice.mockResolvedValue(mockAuthResponse);
    mockPollForTokens.mockRejectedValue(new Error('User denied authorization'));

    const result = await login();

    expect(result.success).toBe(false);
    expect(result.error).toContain('denied');
  });

  it('should use specified profile', async () => {
    mockAuthorizeDevice.mockResolvedValue(mockAuthResponse);
    mockPollForTokens.mockResolvedValue(mockTokenResponse);

    await login({ profile: 'production' });

    const writeCall = vi.mocked(fs.writeFileSync).mock.calls[0];
    const writtenData = JSON.parse(writeCall[1] as string);
    expect(writtenData.profiles.production).toBeDefined();
  });

  it('should show verbose output when enabled', async () => {
    mockAuthorizeDevice.mockResolvedValue(mockAuthResponse);
    mockPollForTokens.mockResolvedValue(mockTokenResponse);

    await login({ verbose: true });

    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('Initiating device authorization'));
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('Authorization successful'));
  });
});

// ============================================================================
// Logout Tests
// ============================================================================

describe('CLI Auth - Logout', () => {
  let logout: typeof import('../../../src/cli/auth.js').logout;
  let consoleSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    vi.resetModules();

    const authModule = await import('../../../src/cli/auth.js');
    logout = authModule.logout;

    consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    vi.clearAllMocks();
  });

  afterEach(() => {
    consoleSpy.mockRestore();
  });

  it('should remove credentials for default profile', async () => {
    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(mockStoredAuth));

    const result = await logout();

    expect(result).toBe(true);
    const writeCall = vi.mocked(fs.writeFileSync).mock.calls[0];
    const writtenData = JSON.parse(writeCall[1] as string);
    expect(writtenData.profiles.default).toBeUndefined();
  });

  it('should handle logout when not logged in', async () => {
    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify({ profiles: {} }));

    const result = await logout();

    expect(result).toBe(true);
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('No credentials found'));
  });

  it('should use specified profile', async () => {
    const multiProfileAuth = {
      profiles: {
        default: mockStoredAuth.profiles.default,
        production: { ...mockStoredAuth.profiles.default },
      },
    };
    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(multiProfileAuth));

    await logout({ profile: 'production' });

    const writeCall = vi.mocked(fs.writeFileSync).mock.calls[0];
    const writtenData = JSON.parse(writeCall[1] as string);
    expect(writtenData.profiles.default).toBeDefined();
    expect(writtenData.profiles.production).toBeUndefined();
  });

  it('should display success message', async () => {
    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(mockStoredAuth));

    await logout();

    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('Successfully logged out'));
  });
});

// ============================================================================
// Whoami Tests
// ============================================================================

describe('CLI Auth - Whoami', () => {
  let whoami: typeof import('../../../src/cli/auth.js').whoami;
  let mockGetUser: ReturnType<typeof vi.fn>;
  let consoleSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    vi.resetModules();

    const oauthModule = await import('oauth.do');
    mockGetUser = vi.mocked(oauthModule.getUser);

    const authModule = await import('../../../src/cli/auth.js');
    whoami = authModule.whoami;

    consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    vi.clearAllMocks();
  });

  afterEach(() => {
    consoleSpy.mockRestore();
  });

  it('should show authenticated status when logged in', async () => {
    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(mockStoredAuth));

    const result = await whoami();

    expect(result.authenticated).toBe(true);
    expect(result.user?.userId).toBe('user_123');
    expect(result.user?.email).toBe('test@example.com');
  });

  it('should show not logged in when no credentials exist', async () => {
    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify({ profiles: {} }));

    const result = await whoami();

    expect(result.authenticated).toBe(false);
    expect(result.error).toBe('Not logged in');
  });

  it('should show session expired when token is expired', async () => {
    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(mockExpiredAuth));

    const result = await whoami();

    expect(result.authenticated).toBe(false);
    expect(result.error).toBe('Session expired');
  });

  it('should display user information', async () => {
    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(mockStoredAuth));

    await whoami();

    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('Authenticated'));
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('user_123'));
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('test@example.com'));
  });

  it('should fetch fresh user info when not stored', async () => {
    const authWithoutUser = {
      profiles: {
        default: {
          accessToken: 'token',
          expiresAt: Date.now() + 3600000,
        },
      },
    };
    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(authWithoutUser));
    mockGetUser.mockResolvedValue({
      user: { id: 'fetched_user', email: 'fetched@example.com', name: 'Fetched User' },
    });

    const result = await whoami();

    expect(mockGetUser).toHaveBeenCalledWith('token');
    expect(result.user?.userId).toBe('fetched_user');
  });

  it('should include expiration information', async () => {
    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(mockStoredAuth));

    const result = await whoami();

    expect(result.expiresAt).toBeInstanceOf(Date);
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringMatching(/Session valid for \d+h \d+m/));
  });

  it('should show verbose expiration info when enabled', async () => {
    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(mockStoredAuth));

    await whoami({ verbose: true });

    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('Expires:'));
  });
});

// ============================================================================
// File Path Tests
// ============================================================================

describe('CLI Auth - File Paths', () => {
  it('should use ~/.mongolake/auth.json for storage', async () => {
    vi.mocked(fs.existsSync).mockReturnValue(false);

    const authModule = await import('../../../src/cli/auth.js');
    const storage = new authModule.MongoLakeTokenStorage();
    await storage.setToken('test_token');

    const writeCall = vi.mocked(fs.writeFileSync).mock.calls[0];
    const writtenPath = writeCall[0] as string;

    expect(writtenPath).toContain('.mongolake');
    expect(writtenPath).toContain('auth.json');
  });

  it('should create config directory with 0700 permissions', async () => {
    vi.mocked(fs.existsSync).mockReturnValue(false);

    const authModule = await import('../../../src/cli/auth.js');
    const storage = new authModule.MongoLakeTokenStorage();
    await storage.setToken('test_token');

    expect(vi.mocked(fs.mkdirSync)).toHaveBeenCalledWith(
      expect.stringContaining('.mongolake'),
      { mode: 0o700, recursive: true }
    );
  });

  it('should write auth file with 0600 permissions', async () => {
    vi.mocked(fs.existsSync).mockReturnValue(false);

    const authModule = await import('../../../src/cli/auth.js');
    const storage = new authModule.MongoLakeTokenStorage();
    await storage.setToken('test_token');

    expect(vi.mocked(fs.writeFileSync)).toHaveBeenCalledWith(
      expect.any(String),
      expect.any(String),
      { mode: 0o600 }
    );
  });
});
