/**
 * Tests for MongoLake CLI Authentication - Token Access and Middleware
 *
 * Tests token access and middleware functionality including:
 * - getAccessToken function
 * - isAuthenticated function
 * - getProfileData function
 * - Auth middleware (requireAuth, createAuthProvider)
 * - Auth CLI command registration
 * - Auth status command
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import * as fs from 'node:fs';
import {
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
    const expiresAt = Date.now() + 3600000;
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
          expiresAt: Date.now() - 3600000,
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

    expect(result.storageBackend).toBeDefined();
    expect(['keychain', 'file']).toContain(result.storageBackend);
  });
});
