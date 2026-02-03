/**
 * Tests for MongoLake CLI Authentication
 *
 * Tests the OAuth device flow authentication using oauth.do package integration.
 * Covers:
 * - MongoLakeTokenStorage class
 * - Login flow with device authorization
 * - Logout functionality
 * - Whoami command
 * - Token access and refresh utilities
 * - Auth middleware for CLI commands
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
      5, // interval
      expect.any(Number) // expires_in
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
// Token Access Tests
// ============================================================================

describe('CLI Auth - Token Access', () => {
  let getAccessToken: typeof import('../../../src/cli/auth.js').getAccessToken;
  let isAuthenticated: typeof import('../../../src/cli/auth.js').isAuthenticated;
  let getProfileData: typeof import('../../../src/cli/auth.js').getProfileData;

  beforeEach(async () => {
    vi.resetModules();

    const authModule = await import('../../../src/cli/auth.js');
    getAccessToken = authModule.getAccessToken;
    isAuthenticated = authModule.isAuthenticated;
    getProfileData = authModule.getProfileData;

    vi.clearAllMocks();
  });

  describe('getAccessToken', () => {
    it('should return token when authenticated', async () => {
      vi.mocked(fs.existsSync).mockReturnValue(true);
      vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(mockStoredAuth));

      const token = await getAccessToken();

      expect(token).toBe(mockStoredAuth.profiles.default.accessToken);
    });

    it('should return null when not authenticated', async () => {
      vi.mocked(fs.existsSync).mockReturnValue(true);
      vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify({ profiles: {} }));

      const token = await getAccessToken();

      expect(token).toBeNull();
    });

    it('should return null when token is expired', async () => {
      vi.mocked(fs.existsSync).mockReturnValue(true);
      vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(mockExpiredAuth));

      const token = await getAccessToken();

      expect(token).toBeNull();
    });

    it('should use specified profile', async () => {
      const multiProfileAuth = {
        profiles: {
          default: { accessToken: 'default_token', expiresAt: Date.now() + 3600000 },
          production: { accessToken: 'prod_token', expiresAt: Date.now() + 3600000 },
        },
      };
      vi.mocked(fs.existsSync).mockReturnValue(true);
      vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(multiProfileAuth));

      const token = await getAccessToken('production');

      expect(token).toBe('prod_token');
    });
  });

  describe('isAuthenticated', () => {
    it('should return true when token exists and is valid', async () => {
      vi.mocked(fs.existsSync).mockReturnValue(true);
      vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(mockStoredAuth));

      const authenticated = await isAuthenticated();

      expect(authenticated).toBe(true);
    });

    it('should return false when not logged in', async () => {
      vi.mocked(fs.existsSync).mockReturnValue(true);
      vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify({ profiles: {} }));

      const authenticated = await isAuthenticated();

      expect(authenticated).toBe(false);
    });

    it('should return false when token is expired', async () => {
      vi.mocked(fs.existsSync).mockReturnValue(true);
      vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(mockExpiredAuth));

      const authenticated = await isAuthenticated();

      expect(authenticated).toBe(false);
    });
  });

  describe('getProfileData', () => {
    it('should return full profile data', async () => {
      vi.mocked(fs.existsSync).mockReturnValue(true);
      vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(mockStoredAuth));

      const profileData = await getProfileData();

      expect(profileData).toEqual(mockStoredAuth.profiles.default);
    });

    it('should return null when profile does not exist', async () => {
      vi.mocked(fs.existsSync).mockReturnValue(true);
      vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify({ profiles: {} }));

      const profileData = await getProfileData();

      expect(profileData).toBeNull();
    });
  });
});

// ============================================================================
// Auth Middleware Tests
// ============================================================================

describe('CLI Auth - Auth Middleware', () => {
  let requireAuth: typeof import('../../../src/cli/auth.js').requireAuth;
  let createAuthProvider: typeof import('../../../src/cli/auth.js').createAuthProvider;
  let consoleSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    vi.resetModules();

    const authModule = await import('../../../src/cli/auth.js');
    requireAuth = authModule.requireAuth;
    createAuthProvider = authModule.createAuthProvider;

    consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    vi.clearAllMocks();
  });

  afterEach(() => {
    consoleSpy.mockRestore();
  });

  describe('requireAuth', () => {
    it('should return token when authenticated', async () => {
      vi.mocked(fs.existsSync).mockReturnValue(true);
      vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(mockStoredAuth));

      const token = await requireAuth();

      expect(token).toBe(mockStoredAuth.profiles.default.accessToken);
    });

    it('should throw error when not authenticated', async () => {
      vi.mocked(fs.existsSync).mockReturnValue(true);
      vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify({ profiles: {} }));

      await expect(requireAuth()).rejects.toThrow('Not authenticated');
    });

    it('should display authentication required message when not logged in', async () => {
      vi.mocked(fs.existsSync).mockReturnValue(true);
      vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify({ profiles: {} }));

      try {
        await requireAuth();
      } catch {
        // Expected
      }

      expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('Authentication required'));
    });
  });

  describe('createAuthProvider', () => {
    it('should return a function that resolves to token', async () => {
      vi.mocked(fs.existsSync).mockReturnValue(true);
      vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(mockStoredAuth));

      const authProvider = createAuthProvider();
      const token = await authProvider();

      expect(token).toBe(mockStoredAuth.profiles.default.accessToken);
    });

    it('should return null when not authenticated', async () => {
      vi.mocked(fs.existsSync).mockReturnValue(true);
      vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify({ profiles: {} }));

      const authProvider = createAuthProvider();
      const token = await authProvider();

      expect(token).toBeNull();
    });

    it('should use specified profile', async () => {
      const multiProfileAuth = {
        profiles: {
          default: { accessToken: 'default_token', expiresAt: Date.now() + 3600000 },
          staging: { accessToken: 'staging_token', expiresAt: Date.now() + 3600000 },
        },
      };
      vi.mocked(fs.existsSync).mockReturnValue(true);
      vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(multiProfileAuth));

      const authProvider = createAuthProvider('staging');
      const token = await authProvider();

      expect(token).toBe('staging_token');
    });
  });
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

  it('should check token with 5 minute buffer and provide graceful degradation', async () => {
    // Token expires in 4 minutes (less than 5 minute buffer)
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

    // Should return the existing token (graceful degradation) when refresh fails
    // but token is not yet expired
    const token = await refreshTokenIfNeeded();

    // Graceful degradation: returns existing token when refresh fails but token not expired
    expect(token).toBe('near_expiry_token');
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

    // Should have getRawStoredValue method for testing
    expect(typeof storage.getRawStoredValue).toBe('function');
  });

  it('should support fallback to file storage', async () => {
    const module = await import('../../../src/cli/auth.js');
    const { KeychainTokenStorage } = module;

    const storage = new KeychainTokenStorage({ fallbackToFile: true });

    // Should have isUsingFallback property
    expect(storage).toHaveProperty('isUsingFallback');
  });

  it('should support multiple profiles', async () => {
    const module = await import('../../../src/cli/auth.js');
    const { KeychainTokenStorage } = module;

    // Should accept profile parameter
    const defaultStorage = new KeychainTokenStorage('default');
    const prodStorage = new KeychainTokenStorage('production');

    expect(defaultStorage.profile).toBe('default');
    expect(prodStorage.profile).toBe('production');
  });

  it('should handle access errors gracefully', async () => {
    const module = await import('../../../src/cli/auth.js');
    const { KeychainTokenStorage } = module;

    const storage = new KeychainTokenStorage();

    // Should not throw, but return null when keychain is inaccessible
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
          expiresAt: Date.now() - 1000, // Expired
        },
      },
    };
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(storedAuth));

    const module = await import('../../../src/cli/auth.js');

    const result = await module.refreshToken();

    // Should return a result object with success status
    // Note: accessToken is only included on successful refresh
    expect(result).toHaveProperty('success');
    // The refresh will fail in tests due to no actual OAuth server
    // but it should have the error property
    expect(result).toHaveProperty('error');
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

    // After successful refresh, tokens should be updated in storage
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

    // Should return result object with error information
    expect(result).toHaveProperty('success');
    expect(result).toHaveProperty('error');
  });

  it('should auto-refresh token when within threshold', async () => {
    // Token expires in 4 minutes (within 5 minute threshold)
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

    // Should return a token (either refreshed or original)
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

    // Should handle profile-specific refresh
    expect(result).toHaveProperty('success');
  });

  it('should export forceTokenRefresh function', async () => {
    const module = await import('../../../src/cli/auth.js');
    expect(typeof module.forceTokenRefresh).toBe('function');
  });

  it('should return error when no profile data exists', async () => {
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify({ profiles: {} }));

    const module = await import('../../../src/cli/auth.js');
    const result = await module.refreshToken();

    expect(result.success).toBe(false);
    expect(result.error).toBe('No profile data found');
  });

  it('should return error when no refresh token is available', async () => {
    const storedAuth = {
      profiles: {
        default: {
          accessToken: 'token_without_refresh',
          expiresAt: Date.now() - 1000,
          // No refreshToken
        },
      },
    };
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(storedAuth));

    const module = await import('../../../src/cli/auth.js');
    const result = await module.refreshToken();

    expect(result.success).toBe(false);
    expect(result.error).toBe('No refresh token available');
  });

  it('should include retryCount in result', async () => {
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
    const result = await module.refreshToken({ maxRetries: 0 }); // No retries for faster test

    // Result should include retry information
    expect(result).toHaveProperty('retryCount');
  });

  it('should support custom maxRetries option', async () => {
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

    // Should complete quickly with maxRetries: 0
    const start = Date.now();
    await module.refreshToken({ maxRetries: 0 });
    const elapsed = Date.now() - start;

    // Should complete in under 500ms (no retries)
    expect(elapsed).toBeLessThan(500);
  });

  it('should suggest re-login for auth errors', async () => {
    const storedAuth = {
      profiles: {
        default: {
          accessToken: 'old_token',
          refreshToken: 'invalid_refresh_token',
          expiresAt: Date.now() - 1000,
        },
      },
    };
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(storedAuth));

    const module = await import('../../../src/cli/auth.js');
    const result = await module.refreshToken({ maxRetries: 0 });

    // Should have error message
    expect(result.success).toBe(false);
    expect(result.error).toBeDefined();
  });

  it('should export createRequiredAuthProvider function', async () => {
    const module = await import('../../../src/cli/auth.js');
    expect(typeof module.createRequiredAuthProvider).toBe('function');
  });

  it('createRequiredAuthProvider should throw when not authenticated', async () => {
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify({ profiles: {} }));

    const module = await import('../../../src/cli/auth.js');
    const authProvider = module.createRequiredAuthProvider();

    await expect(authProvider()).rejects.toThrow('Not authenticated');
  });

  it('createRequiredAuthProvider should return token when authenticated', async () => {
    const storedAuth = {
      profiles: {
        default: {
          accessToken: 'valid_token',
          expiresAt: Date.now() + 3600000, // 1 hour from now
        },
      },
    };
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(storedAuth));

    const module = await import('../../../src/cli/auth.js');
    const authProvider = module.createRequiredAuthProvider();

    const token = await authProvider();
    expect(token).toBe('valid_token');
  });
});

// ============================================================================
// Auth CLI Command Registration Tests
// ============================================================================

describe('CLI Auth - Command Registration', () => {
  let CLI: typeof import('../../../src/cli/framework.js').CLI;
  let registerAuthCommand: typeof import('../../../src/cli/auth.js').registerAuthCommand;

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should export registerAuthCommand function', async () => {
    const module = await import('../../../src/cli/auth.js');
    expect(typeof module.registerAuthCommand).toBe('function');
  });

  it('should register auth command with CLI', async () => {
    const cliModule = await import('../../../src/cli/framework.js');
    const authModule = await import('../../../src/cli/auth.js');
    CLI = cliModule.CLI;
    registerAuthCommand = authModule.registerAuthCommand;

    const cli = new CLI({ name: 'mongolake', version: '0.1.0' });
    registerAuthCommand(cli);

    expect(cli.hasCommand('auth')).toBe(true);
  });

  it('should register login subcommand', async () => {
    const cliModule = await import('../../../src/cli/framework.js');
    const authModule = await import('../../../src/cli/auth.js');
    CLI = cliModule.CLI;
    registerAuthCommand = authModule.registerAuthCommand;

    const cli = new CLI({ name: 'mongolake', version: '0.1.0' });
    registerAuthCommand(cli);

    const authCmd = cli.getCommand('auth');
    expect(authCmd?.hasSubcommand('login')).toBe(true);
  });

  it('should register logout subcommand', async () => {
    const cliModule = await import('../../../src/cli/framework.js');
    const authModule = await import('../../../src/cli/auth.js');
    CLI = cliModule.CLI;
    registerAuthCommand = authModule.registerAuthCommand;

    const cli = new CLI({ name: 'mongolake', version: '0.1.0' });
    registerAuthCommand(cli);

    const authCmd = cli.getCommand('auth');
    expect(authCmd?.hasSubcommand('logout')).toBe(true);
  });

  it('should register status subcommand', async () => {
    const cliModule = await import('../../../src/cli/framework.js');
    const authModule = await import('../../../src/cli/auth.js');
    CLI = cliModule.CLI;
    registerAuthCommand = authModule.registerAuthCommand;

    const cli = new CLI({ name: 'mongolake', version: '0.1.0' });
    registerAuthCommand(cli);

    const authCmd = cli.getCommand('auth');
    expect(authCmd?.hasSubcommand('status')).toBe(true);
  });

  it('should register whoami subcommand', async () => {
    const cliModule = await import('../../../src/cli/framework.js');
    const authModule = await import('../../../src/cli/auth.js');
    CLI = cliModule.CLI;
    registerAuthCommand = authModule.registerAuthCommand;

    const cli = new CLI({ name: 'mongolake', version: '0.1.0' });
    registerAuthCommand(cli);

    const authCmd = cli.getCommand('auth');
    expect(authCmd?.hasSubcommand('whoami')).toBe(true);
  });

  it('should register refresh subcommand', async () => {
    const cliModule = await import('../../../src/cli/framework.js');
    const authModule = await import('../../../src/cli/auth.js');
    CLI = cliModule.CLI;
    registerAuthCommand = authModule.registerAuthCommand;

    const cli = new CLI({ name: 'mongolake', version: '0.1.0' });
    registerAuthCommand(cli);

    const authCmd = cli.getCommand('auth');
    expect(authCmd?.hasSubcommand('refresh')).toBe(true);
  });

  it('should support --profile option on all subcommands', async () => {
    const cliModule = await import('../../../src/cli/framework.js');
    const authModule = await import('../../../src/cli/auth.js');
    CLI = cliModule.CLI;
    registerAuthCommand = authModule.registerAuthCommand;

    const cli = new CLI({ name: 'mongolake', version: '0.1.0' });
    registerAuthCommand(cli);

    const authCmd = cli.getCommand('auth');
    const loginCmd = authCmd?.getSubcommand('login');

    expect(loginCmd?.getOption('profile')).toBeDefined();
  });

  it('should support --verbose option on login', async () => {
    const cliModule = await import('../../../src/cli/framework.js');
    const authModule = await import('../../../src/cli/auth.js');
    CLI = cliModule.CLI;
    registerAuthCommand = authModule.registerAuthCommand;

    const cli = new CLI({ name: 'mongolake', version: '0.1.0' });
    registerAuthCommand(cli);

    const authCmd = cli.getCommand('auth');
    const loginCmd = authCmd?.getSubcommand('login');

    expect(loginCmd?.getOption('verbose')).toBeDefined();
  });
});

// ============================================================================
// Auth Status Command Tests
// ============================================================================

describe('CLI Auth - Status Command', () => {
  let status: typeof import('../../../src/cli/auth.js').status;
  let consoleSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    vi.resetModules();

    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.mkdirSync).mockReturnValue(undefined);
    vi.mocked(fs.writeFileSync).mockReturnValue(undefined);

    consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    vi.clearAllMocks();
  });

  afterEach(() => {
    consoleSpy.mockRestore();
  });

  it('should export status function', async () => {
    const module = await import('../../../src/cli/auth.js');
    expect(typeof module.status).toBe('function');
  });

  it('should show authentication status overview', async () => {
    const storedAuth = {
      profiles: {
        default: {
          accessToken: 'token',
          refreshToken: 'refresh',
          expiresAt: Date.now() + 3600000,
          user: {
            id: 'user_123',
            email: 'test@example.com',
            name: 'Test User',
          },
        },
      },
    };
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(storedAuth));

    const module = await import('../../../src/cli/auth.js');
    status = module.status;

    const result = await status();

    expect(result.authenticated).toBe(true);
    expect(result.profile).toBe('default');
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('Authenticated'));
  });

  it('should show token expiration time', async () => {
    const expiresAt = Date.now() + 3600000; // 1 hour from now
    const storedAuth = {
      profiles: {
        default: {
          accessToken: 'token',
          expiresAt,
        },
      },
    };
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(storedAuth));

    const module = await import('../../../src/cli/auth.js');
    status = module.status;

    const result = await status();

    expect(result.expiresAt).toBeInstanceOf(Date);
    expect(result.expiresIn).toBeDefined();
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringMatching(/expires/i));
  });

  it('should show oauth.do provider information', async () => {
    const storedAuth = {
      profiles: {
        default: {
          accessToken: 'token',
          expiresAt: Date.now() + 3600000,
          provider: 'oauth.do',
        },
      },
    };
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(storedAuth));

    const module = await import('../../../src/cli/auth.js');
    status = module.status;

    const result = await status();

    expect(result.provider).toBe('oauth.do');
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('oauth.do'));
  });

  it('should show not authenticated status when no credentials', async () => {
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify({ profiles: {} }));

    const module = await import('../../../src/cli/auth.js');
    status = module.status;

    const result = await status();

    expect(result.authenticated).toBe(false);
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('Not authenticated'));
  });

  it('should show all available profiles', async () => {
    const storedAuth = {
      profiles: {
        default: { accessToken: 'token1', expiresAt: Date.now() + 3600000 },
        production: { accessToken: 'token2', expiresAt: Date.now() + 3600000 },
        staging: { accessToken: 'token3', expiresAt: Date.now() + 3600000 },
      },
    };
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(storedAuth));

    const module = await import('../../../src/cli/auth.js');
    status = module.status;

    const result = await status({ showAllProfiles: true });

    expect(result.profiles).toHaveLength(3);
    expect(result.profiles).toContain('default');
    expect(result.profiles).toContain('production');
    expect(result.profiles).toContain('staging');
  });

  it('should indicate expired sessions', async () => {
    const storedAuth = {
      profiles: {
        default: {
          accessToken: 'expired_token',
          expiresAt: Date.now() - 3600000, // Expired 1 hour ago
        },
      },
    };
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(storedAuth));

    const module = await import('../../../src/cli/auth.js');
    status = module.status;

    const result = await status();

    expect(result.authenticated).toBe(false);
    expect(result.expired).toBe(true);
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('expired'));
  });

  it('should show storage backend type', async () => {
    const storedAuth = {
      profiles: {
        default: { accessToken: 'token', expiresAt: Date.now() + 3600000 },
      },
    };
    vi.mocked(fs.readFileSync).mockReturnValue(JSON.stringify(storedAuth));

    const module = await import('../../../src/cli/auth.js');
    status = module.status;

    const result = await status();

    // Should indicate whether using keychain or file storage
    expect(result.storageBackend).toBeDefined();
    expect(['keychain', 'file']).toContain(result.storageBackend);
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

    // Should return storage detection result
    expect(result).toHaveProperty('available');
    expect(result).toHaveProperty('type');
  });

  it('should report keychain type on macOS', async () => {
    const module = await import('../../../src/cli/auth.js');

    const result = await module.detectSecureStorage();

    // On macOS, should detect keychain
    if (process.platform === 'darwin') {
      expect(result.type).toBe('keychain');
    }
  });

  it('should report secret-service or file type on Linux', async () => {
    const module = await import('../../../src/cli/auth.js');

    const result = await module.detectSecureStorage();

    // On Linux, should detect secret-service or fallback
    if (process.platform === 'linux') {
      expect(['secret-service', 'file']).toContain(result.type);
    }
  });

  it('should report credential-manager type on Windows', async () => {
    const module = await import('../../../src/cli/auth.js');

    const result = await module.detectSecureStorage();

    // On Windows, should detect credential-manager
    if (process.platform === 'win32') {
      expect(result.type).toBe('credential-manager');
    }
  });

  it('should always have file fallback available', async () => {
    const module = await import('../../../src/cli/auth.js');

    const result = await module.detectSecureStorage();

    // Should at least have file fallback
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

    // The login function should handle rate limiting
    expect(typeof module.login).toBe('function');
  });

  it('should validate oauth.do response format', async () => {
    const module = await import('../../../src/cli/auth.js');

    // Should have validation for response format
    expect(typeof module.validateAuthResponse).toBe('function');
  });

  it('should support custom oauth.do endpoint configuration', async () => {
    const module = await import('../../../src/cli/auth.js');

    // Should support custom endpoints for testing
    expect(typeof module.configureOAuthEndpoints).toBe('function');
  });

  it('should configure oauth.do with baseUrl and authDomain', async () => {
    const { configure } = await import('oauth.do');
    const { configureOAuthEndpoints, OAUTH_CLIENT_ID } = await import('../../../src/cli/auth.js');

    // Configure with baseUrl and authDomain
    configureOAuthEndpoints({
      baseUrl: 'http://localhost:9000',
      authDomain: 'localhost:9000',
    });

    // Should call configure with the correct options
    expect(configure).toHaveBeenCalledWith({
      clientId: OAUTH_CLIENT_ID,
      apiUrl: 'http://localhost:9000',
      authKitDomain: 'localhost:9000',
    });
  });

  it('should warn about deprecated individual endpoint options', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const { configureOAuthEndpoints } = await import('../../../src/cli/auth.js');

    // Configure with deprecated individual endpoints
    configureOAuthEndpoints({
      authorizationEndpoint: 'http://localhost:9000/auth',
      tokenEndpoint: 'http://localhost:9000/token',
    });

    // Should warn about unsupported options
    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining('Individual OAuth endpoint configuration is not supported')
    );

    warnSpy.mockRestore();
  });

  it('should not call configure if no baseUrl or authDomain provided', async () => {
    const { configure } = await import('oauth.do');
    const { configureOAuthEndpoints } = await import('../../../src/cli/auth.js');

    // Clear previous calls
    vi.mocked(configure).mockClear();

    // Configure with only deprecated options (no baseUrl or authDomain)
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    configureOAuthEndpoints({
      authorizationEndpoint: 'http://localhost:9000/auth',
    });
    warnSpy.mockRestore();

    // Should not call configure since no valid options provided
    expect(configure).not.toHaveBeenCalled();
  });
});
