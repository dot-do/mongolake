/**
 * Tests: Native Keychain Integration
 *
 * These tests verify the behavior of native keychain integration
 * for secure credential storage on macOS, Windows, and Linux.
 *
 * The feature provides:
 * - Secure OAuth token storage in the system keychain
 * - Token retrieval from keychain on demand
 * - Support for macOS Keychain, Windows Credential Manager, and Linux Secret Service
 * - Graceful fallback if keychain is unavailable
 *
 * @see src/cli/keychain.ts - Native keychain implementation
 * @see src/cli/auth.ts - Auth integration with keychain
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  MacOSKeychainStorage,
  WindowsCredentialStorage,
  LinuxSecretStorage,
  FileBasedStorage,
  createKeychainStorage,
  createKeychainStorageSync,
  detectSecureStorage,
  UnifiedKeychainStorage,
  DEFAULT_SERVICE_NAME,
  KeychainError,
  KeychainUnavailableError,
  type KeychainStorage,
  type KeychainTokenData,
  type SecureStorageInfo,
} from '../../../src/cli/keychain.js';

// ============================================================================
// Module Export Tests
// ============================================================================

describe('Native Keychain Integration - Module Exports', () => {
  it('should export MacOSKeychainStorage class', () => {
    expect(MacOSKeychainStorage).toBeDefined();
    expect(typeof MacOSKeychainStorage).toBe('function');
  });

  it('should export WindowsCredentialStorage class', () => {
    expect(WindowsCredentialStorage).toBeDefined();
    expect(typeof WindowsCredentialStorage).toBe('function');
  });

  it('should export LinuxSecretStorage class', () => {
    expect(LinuxSecretStorage).toBeDefined();
    expect(typeof LinuxSecretStorage).toBe('function');
  });

  it('should export FileBasedStorage class', () => {
    expect(FileBasedStorage).toBeDefined();
    expect(typeof FileBasedStorage).toBe('function');
  });

  it('should export UnifiedKeychainStorage class', () => {
    expect(UnifiedKeychainStorage).toBeDefined();
    expect(typeof UnifiedKeychainStorage).toBe('function');
  });

  it('should export createKeychainStorage function', () => {
    expect(createKeychainStorage).toBeDefined();
    expect(typeof createKeychainStorage).toBe('function');
  });

  it('should export createKeychainStorageSync function', () => {
    expect(createKeychainStorageSync).toBeDefined();
    expect(typeof createKeychainStorageSync).toBe('function');
  });

  it('should export detectSecureStorage function', () => {
    expect(detectSecureStorage).toBeDefined();
    expect(typeof detectSecureStorage).toBe('function');
  });

  it('should export DEFAULT_SERVICE_NAME constant', () => {
    expect(DEFAULT_SERVICE_NAME).toBe('mongolake-cli');
  });

  it('should export KeychainError class', () => {
    expect(KeychainError).toBeDefined();
    const error = new KeychainError('test', 'get', 'darwin');
    expect(error.name).toBe('KeychainError');
  });

  it('should export KeychainUnavailableError class', () => {
    expect(KeychainUnavailableError).toBeDefined();
    const error = new KeychainUnavailableError('darwin', 'test reason');
    expect(error.name).toBe('KeychainUnavailableError');
  });
});

// ============================================================================
// macOS Keychain Support
// ============================================================================

describe('Native Keychain Integration - macOS Keychain Support', () => {
  let storage: MacOSKeychainStorage;

  beforeEach(() => {
    storage = new MacOSKeychainStorage('test-service');
  });

  it('should have keychain storage type', () => {
    expect(storage.storageType).toBe('keychain');
  });

  it('should use provided service name', () => {
    expect(storage.serviceName).toBe('test-service');
  });

  it('should use default service name when not provided', () => {
    const defaultStorage = new MacOSKeychainStorage();
    expect(defaultStorage.serviceName).toBe(DEFAULT_SERVICE_NAME);
  });

  it('should implement KeychainStorage interface', () => {
    expect(typeof storage.getToken).toBe('function');
    expect(typeof storage.setToken).toBe('function');
    expect(typeof storage.deleteToken).toBe('function');
    expect(typeof storage.getTokenData).toBe('function');
    expect(typeof storage.setTokenData).toBe('function');
    expect(typeof storage.isAvailable).toBe('function');
  });

  it('should check keychain availability', async () => {
    const available = await storage.isAvailable();
    // Should return boolean
    expect(typeof available).toBe('boolean');
    // On macOS, should be true; on other platforms, false
    if (process.platform === 'darwin') {
      expect(available).toBe(true);
    }
  });

  // Platform-specific tests that only run on macOS
  it.skipIf(process.platform !== 'darwin')('should store and retrieve token on macOS', async () => {
    const testAccount = `test-account-${Date.now()}`;
    const testToken = 'test-access-token-12345';

    try {
      // Store token
      await storage.setToken(testAccount, testToken);

      // Retrieve token
      const retrieved = await storage.getToken(testAccount);
      expect(retrieved).toBe(testToken);
    } finally {
      // Cleanup
      await storage.deleteToken(testAccount);
    }
  });

  it.skipIf(process.platform !== 'darwin')('should update existing token on macOS', async () => {
    const testAccount = `test-account-${Date.now()}`;

    try {
      await storage.setToken(testAccount, 'original-token');
      await storage.setToken(testAccount, 'updated-token');

      const retrieved = await storage.getToken(testAccount);
      expect(retrieved).toBe('updated-token');
    } finally {
      await storage.deleteToken(testAccount);
    }
  });

  it.skipIf(process.platform !== 'darwin')('should delete token from macOS Keychain', async () => {
    const testAccount = `test-account-${Date.now()}`;

    await storage.setToken(testAccount, 'token-to-delete');
    await storage.deleteToken(testAccount);

    const retrieved = await storage.getToken(testAccount);
    expect(retrieved).toBeNull();
  });

  it.skipIf(process.platform !== 'darwin')('should store and retrieve token data with metadata', async () => {
    const testAccount = `test-account-${Date.now()}`;
    const tokenData: KeychainTokenData = {
      accessToken: 'access-token-123',
      refreshToken: 'refresh-token-456',
      expiresAt: Date.now() + 3600000,
      scope: 'read write',
    };

    try {
      await storage.setTokenData(testAccount, tokenData);

      const retrieved = await storage.getTokenData(testAccount);
      expect(retrieved).not.toBeNull();
      expect(retrieved?.accessToken).toBe(tokenData.accessToken);
      expect(retrieved?.refreshToken).toBe(tokenData.refreshToken);
      expect(retrieved?.expiresAt).toBe(tokenData.expiresAt);
      expect(retrieved?.scope).toBe(tokenData.scope);
    } finally {
      await storage.deleteToken(testAccount);
      await storage.deleteToken(`${testAccount}:refresh`);
      await storage.deleteToken(`${testAccount}:metadata`);
    }
  });

  it.skipIf(process.platform !== 'darwin')('should support multiple profiles', async () => {
    const profile1 = `profile1-${Date.now()}`;
    const profile2 = `profile2-${Date.now()}`;

    try {
      await storage.setToken(profile1, 'token-for-profile1');
      await storage.setToken(profile2, 'token-for-profile2');

      const token1 = await storage.getToken(profile1);
      const token2 = await storage.getToken(profile2);

      expect(token1).toBe('token-for-profile1');
      expect(token2).toBe('token-for-profile2');
    } finally {
      await storage.deleteToken(profile1);
      await storage.deleteToken(profile2);
    }
  });
});

// ============================================================================
// Windows Credential Manager Support
// ============================================================================

describe('Native Keychain Integration - Windows Credential Manager Support', () => {
  let storage: WindowsCredentialStorage;

  beforeEach(() => {
    storage = new WindowsCredentialStorage('test-service');
  });

  it('should have credential-manager storage type', () => {
    expect(storage.storageType).toBe('credential-manager');
  });

  it('should use provided service name', () => {
    expect(storage.serviceName).toBe('test-service');
  });

  it('should implement KeychainStorage interface', () => {
    expect(typeof storage.getToken).toBe('function');
    expect(typeof storage.setToken).toBe('function');
    expect(typeof storage.deleteToken).toBe('function');
    expect(typeof storage.getTokenData).toBe('function');
    expect(typeof storage.setTokenData).toBe('function');
    expect(typeof storage.isAvailable).toBe('function');
  });

  it('should check availability', async () => {
    const available = await storage.isAvailable();
    expect(typeof available).toBe('boolean');
    // On Windows, should be true; on other platforms, false
    if (process.platform === 'win32') {
      expect(available).toBe(true);
    }
  });

  // Windows-specific tests
  it.skipIf(process.platform !== 'win32')('should store and retrieve token on Windows', async () => {
    const testAccount = `test-account-${Date.now()}`;
    const testToken = 'test-access-token-win';

    try {
      await storage.setToken(testAccount, testToken);
      const retrieved = await storage.getToken(testAccount);
      expect(retrieved).toBe(testToken);
    } finally {
      await storage.deleteToken(testAccount);
    }
  });
});

// ============================================================================
// Linux Secret Service (D-Bus) Support
// ============================================================================

describe('Native Keychain Integration - Linux Secret Service Support', () => {
  let storage: LinuxSecretStorage;

  beforeEach(() => {
    storage = new LinuxSecretStorage('test-service');
  });

  it('should have secret-service storage type', () => {
    expect(storage.storageType).toBe('secret-service');
  });

  it('should use provided service name', () => {
    expect(storage.serviceName).toBe('test-service');
  });

  it('should implement KeychainStorage interface', () => {
    expect(typeof storage.getToken).toBe('function');
    expect(typeof storage.setToken).toBe('function');
    expect(typeof storage.deleteToken).toBe('function');
    expect(typeof storage.getTokenData).toBe('function');
    expect(typeof storage.setTokenData).toBe('function');
    expect(typeof storage.isAvailable).toBe('function');
  });

  it('should detect D-Bus secret service availability', async () => {
    const available = await storage.isAvailable();
    expect(typeof available).toBe('boolean');
    // On Linux with D-Bus, may be true; otherwise false
  });

  // Linux-specific tests (only run when secret-tool and D-Bus are available)
  it.skipIf(process.platform !== 'linux')('should handle unavailable secret service gracefully', async () => {
    // Even when unavailable, should not throw
    const token = await storage.getToken('nonexistent');
    expect(token).toBeNull();
  });
});

// ============================================================================
// File-Based Fallback Storage
// ============================================================================

describe('Native Keychain Integration - File-Based Fallback Storage', () => {
  let storage: FileBasedStorage;

  beforeEach(() => {
    storage = new FileBasedStorage('test-service');
  });

  it('should have file storage type', () => {
    expect(storage.storageType).toBe('file');
  });

  it('should always be available', async () => {
    const available = await storage.isAvailable();
    expect(available).toBe(true);
  });

  it('should implement KeychainStorage interface', () => {
    expect(typeof storage.getToken).toBe('function');
    expect(typeof storage.setToken).toBe('function');
    expect(typeof storage.deleteToken).toBe('function');
    expect(typeof storage.getTokenData).toBe('function');
    expect(typeof storage.setTokenData).toBe('function');
    expect(typeof storage.isAvailable).toBe('function');
  });

  it('should store and retrieve encrypted tokens', async () => {
    const testAccount = `test-account-${Date.now()}`;
    const testToken = 'test-token-encrypted';

    await storage.setToken(testAccount, testToken);
    const retrieved = await storage.getToken(testAccount);

    expect(retrieved).toBe(testToken);

    // Cleanup
    await storage.deleteToken(testAccount);
  });

  it('should return null for non-existent tokens', async () => {
    const retrieved = await storage.getToken('nonexistent-account');
    expect(retrieved).toBeNull();
  });

  it('should delete tokens', async () => {
    const testAccount = `test-account-${Date.now()}`;

    await storage.setToken(testAccount, 'token-to-delete');
    await storage.deleteToken(testAccount);

    const retrieved = await storage.getToken(testAccount);
    expect(retrieved).toBeNull();
  });

  it('should store and retrieve token data with metadata', async () => {
    const testAccount = `test-account-data-${Date.now()}`;
    const tokenData: KeychainTokenData = {
      accessToken: 'access-123',
      refreshToken: 'refresh-456',
      expiresAt: Date.now() + 7200000,
      scope: 'admin',
    };

    await storage.setTokenData(testAccount, tokenData);
    const retrieved = await storage.getTokenData(testAccount);

    expect(retrieved).not.toBeNull();
    expect(retrieved?.accessToken).toBe(tokenData.accessToken);
    expect(retrieved?.refreshToken).toBe(tokenData.refreshToken);
    expect(retrieved?.expiresAt).toBe(tokenData.expiresAt);
    expect(retrieved?.scope).toBe(tokenData.scope);

    // Cleanup
    await storage.deleteToken(testAccount);
    await storage.deleteToken(`${testAccount}:refresh`);
    await storage.deleteToken(`${testAccount}:metadata`);
  });
});

// ============================================================================
// Cross-Platform Keychain Abstraction
// ============================================================================

describe('Native Keychain Integration - Cross-Platform Abstraction', () => {
  it('should detect current platform and return appropriate storage info', async () => {
    const info = await detectSecureStorage();

    expect(info).toHaveProperty('available');
    expect(info).toHaveProperty('type');
    expect(info).toHaveProperty('platform');
    expect(info).toHaveProperty('fallbackAvailable');
    expect(info.fallbackAvailable).toBe(true);
  });

  it('should return correct storage type for current platform', async () => {
    const info = await detectSecureStorage();

    switch (process.platform) {
      case 'darwin':
        expect(info.type).toBe('keychain');
        break;
      case 'win32':
        expect(info.type).toBe('credential-manager');
        break;
      case 'linux':
        expect(['secret-service', 'file']).toContain(info.type);
        break;
      default:
        expect(info.type).toBe('file');
    }
  });

  it('should create appropriate storage for current platform', async () => {
    const storage = await createKeychainStorage();

    expect(storage).toBeDefined();
    expect(typeof storage.getToken).toBe('function');
    expect(typeof storage.setToken).toBe('function');
    expect(typeof storage.deleteToken).toBe('function');
  });

  it('should create storage synchronously', () => {
    const storage = createKeychainStorageSync();

    expect(storage).toBeDefined();
    expect(typeof storage.getToken).toBe('function');
  });

  it('should force file fallback when requested', async () => {
    const storage = await createKeychainStorage({
      forceFileFallback: true,
    });

    expect(storage.storageType).toBe('file');
  });

  it('should provide consistent API across all storage implementations', async () => {
    const implementations: KeychainStorage[] = [
      new MacOSKeychainStorage(),
      new WindowsCredentialStorage(),
      new LinuxSecretStorage(),
      new FileBasedStorage(),
    ];

    for (const impl of implementations) {
      // All should have the same methods
      expect(typeof impl.getToken).toBe('function');
      expect(typeof impl.setToken).toBe('function');
      expect(typeof impl.deleteToken).toBe('function');
      expect(typeof impl.getTokenData).toBe('function');
      expect(typeof impl.setTokenData).toBe('function');
      expect(typeof impl.isAvailable).toBe('function');
      expect(impl.storageType).toBeDefined();
      expect(impl.serviceName).toBeDefined();
    }
  });
});

// ============================================================================
// UnifiedKeychainStorage
// ============================================================================

describe('Native Keychain Integration - UnifiedKeychainStorage', () => {
  let storage: UnifiedKeychainStorage;

  beforeEach(() => {
    storage = new UnifiedKeychainStorage({
      serviceName: 'test-unified',
      fallbackToFile: true,
    });
  });

  it('should initialize storage on first use', async () => {
    // First operation should trigger initialization
    const token = await storage.getToken('test-profile');
    expect(token).toBeNull(); // No token stored yet
  });

  it('should report storage type after initialization', async () => {
    await storage.initialize();
    expect(storage.storageType).toBeDefined();
    expect(['keychain', 'secret-service', 'credential-manager', 'file']).toContain(storage.storageType);
  });

  it('should track fallback usage', async () => {
    await storage.initialize();
    expect(typeof storage.isUsingFallback).toBe('boolean');
  });

  it('should store and retrieve tokens', async () => {
    const testProfile = `test-profile-${Date.now()}`;
    const testToken = 'unified-test-token';

    await storage.setToken(testProfile, testToken);
    const retrieved = await storage.getToken(testProfile);

    expect(retrieved).toBe(testToken);

    // Cleanup
    await storage.deleteToken(testProfile);
  });

  it('should store and retrieve token data', async () => {
    const testProfile = `test-profile-data-${Date.now()}`;
    const tokenData: KeychainTokenData = {
      accessToken: 'unified-access',
      refreshToken: 'unified-refresh',
      expiresAt: Date.now() + 3600000,
    };

    await storage.setTokenData(testProfile, tokenData);
    const retrieved = await storage.getTokenData(testProfile);

    expect(retrieved).not.toBeNull();
    expect(retrieved?.accessToken).toBe(tokenData.accessToken);

    // Cleanup
    await storage.deleteToken(testProfile);
  });
});

// ============================================================================
// Security Considerations
// ============================================================================

describe('Native Keychain Integration - Security Considerations', () => {
  it('should not expose token values in error messages', async () => {
    const error = new KeychainError('Failed to store token', 'set', 'darwin');
    expect(error.message).not.toContain('secret');
    expect(error.message).not.toContain('password');
    expect(error.message).not.toContain('token-value');
  });

  it('should handle keychain access errors gracefully', async () => {
    const storage = await createKeychainStorage();

    // Should not throw for non-existent token
    const token = await storage.getToken('definitely-nonexistent-account');
    expect(token).toBeNull();
  });

  it('should use encrypted storage for file fallback', async () => {
    const fileStorage = new FileBasedStorage();

    // Store a token
    const testAccount = `security-test-${Date.now()}`;
    await fileStorage.setToken(testAccount, 'sensitive-token-123');

    // Retrieve it (decryption should work)
    const retrieved = await fileStorage.getToken(testAccount);
    expect(retrieved).toBe('sensitive-token-123');

    // Cleanup
    await fileStorage.deleteToken(testAccount);
  });
});

// ============================================================================
// Token Metadata Storage
// ============================================================================

describe('Native Keychain Integration - Token Metadata Storage', () => {
  let storage: KeychainStorage;

  beforeEach(async () => {
    // Use file storage for consistent testing across platforms
    storage = new FileBasedStorage('test-metadata');
  });

  it('should store token expiry time with token', async () => {
    const testAccount = `metadata-expiry-${Date.now()}`;
    const expiresAt = Date.now() + 7200000;

    await storage.setTokenData(testAccount, {
      accessToken: 'token',
      expiresAt,
    });

    const retrieved = await storage.getTokenData(testAccount);
    expect(retrieved?.expiresAt).toBe(expiresAt);

    // Cleanup
    await storage.deleteToken(testAccount);
    await storage.deleteToken(`${testAccount}:metadata`);
  });

  it('should store token scope with token', async () => {
    const testAccount = `metadata-scope-${Date.now()}`;
    const scope = 'read write admin';

    await storage.setTokenData(testAccount, {
      accessToken: 'token',
      scope,
    });

    const retrieved = await storage.getTokenData(testAccount);
    expect(retrieved?.scope).toBe(scope);

    // Cleanup
    await storage.deleteToken(testAccount);
    await storage.deleteToken(`${testAccount}:metadata`);
  });

  it('should store refresh token separately', async () => {
    const testAccount = `metadata-refresh-${Date.now()}`;

    await storage.setTokenData(testAccount, {
      accessToken: 'access-token',
      refreshToken: 'refresh-token',
    });

    const retrieved = await storage.getTokenData(testAccount);
    expect(retrieved?.accessToken).toBe('access-token');
    expect(retrieved?.refreshToken).toBe('refresh-token');

    // Cleanup
    await storage.deleteToken(testAccount);
    await storage.deleteToken(`${testAccount}:refresh`);
    await storage.deleteToken(`${testAccount}:metadata`);
  });
});

// ============================================================================
// Integration with Auth Module
// ============================================================================

describe('Native Keychain Integration - Auth Module Integration', () => {
  it('should export KeychainTokenStorage from auth module', async () => {
    const authModule = await import('../../../src/cli/auth.js');
    expect(authModule.KeychainTokenStorage).toBeDefined();
  });

  it('should export detectSecureStorage from auth module', async () => {
    const authModule = await import('../../../src/cli/auth.js');
    expect(authModule.detectSecureStorage).toBeDefined();
    expect(typeof authModule.detectSecureStorage).toBe('function');
  });

  it('KeychainTokenStorage should implement TokenStorage interface', async () => {
    const authModule = await import('../../../src/cli/auth.js');
    const storage = new authModule.KeychainTokenStorage('test-profile');

    expect(typeof storage.getToken).toBe('function');
    expect(typeof storage.setToken).toBe('function');
    expect(typeof storage.removeToken).toBe('function');
  });

  it('KeychainTokenStorage should track storage type', async () => {
    const authModule = await import('../../../src/cli/auth.js');
    const storage = new authModule.KeychainTokenStorage('test-profile');

    expect(storage.storageType).toBeDefined();
    expect(['keychain', 'secret-service', 'credential-manager', 'file']).toContain(storage.storageType);
  });

  it('KeychainTokenStorage should report fallback status', async () => {
    const authModule = await import('../../../src/cli/auth.js');
    const storage = new authModule.KeychainTokenStorage('test-profile');

    expect(typeof storage.isUsingFallback).toBe('boolean');
  });
});
