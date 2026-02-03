/**
 * Tests for MongoLake CLI Authentication - Login, Logout, and Whoami Flows
 *
 * Tests the OAuth device flow authentication, logout, and whoami functionality.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import * as fs from 'node:fs';

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
// Login Flow Tests
// ============================================================================

describe('CLI Auth - Login Flow', () => {
  let login: typeof import('../../../../src/cli/auth.js').login;
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

    const authModule = await import('../../../../src/cli/auth.js');
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
  let logout: typeof import('../../../../src/cli/auth.js').logout;
  let consoleSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    vi.resetModules();

    const authModule = await import('../../../../src/cli/auth.js');
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
  let whoami: typeof import('../../../../src/cli/auth.js').whoami;
  let mockGetUser: ReturnType<typeof vi.fn>;
  let consoleSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    vi.resetModules();

    const oauthModule = await import('oauth.do');
    mockGetUser = vi.mocked(oauthModule.getUser);

    const authModule = await import('../../../../src/cli/auth.js');
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
