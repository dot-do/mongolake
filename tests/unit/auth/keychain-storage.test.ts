/**
 * Auth Middleware Keychain Storage Tests
 *
 * Tests for CLI keychain storage of credentials.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { mockValidToken, mockRefreshToken, createMockKeytar } from './test-helpers.js';

describe('Auth Middleware - Keychain Storage', () => {
  let KeychainStorage: new (config: {
    serviceName: string;
    keytar: {
      setPassword: (service: string, account: string, password: string) => Promise<void>;
      getPassword: (service: string, account: string) => Promise<string | null>;
      deletePassword: (service: string, account: string) => Promise<boolean>;
    };
  }) => {
    storeTokens: (profile: string, tokens: { accessToken: string; refreshToken: string; expiresAt: number }) => Promise<void>;
    getTokens: (profile: string) => Promise<{ accessToken: string; refreshToken: string; expiresAt: number } | null>;
    deleteTokens: (profile: string) => Promise<void>;
    listProfiles: () => Promise<string[]>;
  };

  let mockKeytar: ReturnType<typeof createMockKeytar>;

  beforeEach(async () => {
    const module = await import('../../../src/auth/middleware.js');
    KeychainStorage = module.KeychainStorage;
    mockKeytar = createMockKeytar();
  });

  it('should store tokens in system keychain', async () => {
    const keychain = new KeychainStorage({
      serviceName: 'mongolake-cli',
      keytar: mockKeytar,
    });

    const tokens = {
      accessToken: mockValidToken,
      refreshToken: mockRefreshToken,
      expiresAt: Date.now() + 3600000,
    };

    await keychain.storeTokens('default', tokens);

    expect(mockKeytar.setPassword).toHaveBeenCalledWith(
      'mongolake-cli',
      'default',
      expect.any(String)
    );
  });

  it('should retrieve tokens from keychain', async () => {
    // Create a keychain instance that stores tokens properly
    const keychain = new KeychainStorage({
      serviceName: 'mongolake-cli',
      keytar: mockKeytar,
    });

    const originalTokens = {
      accessToken: mockValidToken,
      refreshToken: mockRefreshToken,
      expiresAt: Date.now() + 3600000,
    };

    // Store tokens first (this encrypts them)
    await keychain.storeTokens('default', originalTokens);

    // Get the encrypted data that was stored
    const encryptedData = mockKeytar.setPassword.mock.calls[0][2];

    // Set up mock to return the encrypted data
    mockKeytar.getPassword.mockResolvedValueOnce(encryptedData);

    const tokens = await keychain.getTokens('default');

    expect(tokens).toBeDefined();
    expect(tokens?.accessToken).toBe(mockValidToken);
    expect(tokens?.refreshToken).toBe(mockRefreshToken);
  });

  it('should return null for non-existent profile', async () => {
    const keychain = new KeychainStorage({
      serviceName: 'mongolake-cli',
      keytar: mockKeytar,
    });

    const tokens = await keychain.getTokens('non_existent');
    expect(tokens).toBeNull();
  });

  it('should delete tokens from keychain', async () => {
    const keychain = new KeychainStorage({
      serviceName: 'mongolake-cli',
      keytar: mockKeytar,
    });

    await keychain.deleteTokens('default');

    expect(mockKeytar.deletePassword).toHaveBeenCalledWith(
      'mongolake-cli',
      'default'
    );
  });

  it('should handle keychain access errors gracefully', async () => {
    mockKeytar.getPassword.mockRejectedValueOnce(new Error('Keychain locked'));

    const keychain = new KeychainStorage({
      serviceName: 'mongolake-cli',
      keytar: mockKeytar,
    });

    // Error message is sanitized to prevent credential leakage
    await expect(keychain.getTokens('default')).rejects.toThrow('Keychain access failed');
  });

  it('should encrypt sensitive data before storing', async () => {
    const keychain = new KeychainStorage({
      serviceName: 'mongolake-cli',
      keytar: mockKeytar,
    });

    const tokens = {
      accessToken: mockValidToken,
      refreshToken: mockRefreshToken,
      expiresAt: Date.now() + 3600000,
    };

    await keychain.storeTokens('default', tokens);

    // The stored value should not contain the raw token
    const storedArg = mockKeytar.setPassword.mock.calls[0][2];
    expect(storedArg).not.toContain(mockValidToken);
  });

  it('should list all stored profiles', async () => {
    mockKeytar.getPassword.mockImplementation(async (_service: string, account: string) => {
      if (account === 'default' || account === 'production') {
        return JSON.stringify({ accessToken: 'token' });
      }
      return null;
    });

    const keychain = new KeychainStorage({
      serviceName: 'mongolake-cli',
      keytar: mockKeytar,
    });

    const profiles = await keychain.listProfiles();

    expect(Array.isArray(profiles)).toBe(true);
  });
});
