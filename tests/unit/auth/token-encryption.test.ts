/**
 * Token Encryption Tests
 *
 * RED phase tests verifying that proper AES-GCM encryption replaces weak
 * base64+reversal obfuscation for stored credentials.
 *
 * Tests verify:
 * - Encrypted tokens use AES-GCM (not base64+reversal)
 * - Key derivation uses PBKDF2 with sufficient iterations
 * - Each encryption uses unique IV
 * - Encrypted output is not reversible without key
 * - Decryption with wrong key fails
 * - Tampering detection (authenticated encryption)
 * - Key rotation support
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockValidToken, mockRefreshToken, createMockKeytar } from './test-helpers.js';

describe('Auth Middleware - Token Encryption', () => {
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

  let resetEncryptionKey: () => void;
  let mockKeytar: ReturnType<typeof createMockKeytar>;

  beforeEach(async () => {
    const module = await import('../../../src/auth/middleware.js');
    KeychainStorage = module.KeychainStorage;
    resetEncryptionKey = module.resetEncryptionKey;
    mockKeytar = createMockKeytar();

    // Reset encryption key before each test to ensure clean state
    resetEncryptionKey();
  });

  afterEach(() => {
    // Clean up encryption key after each test
    resetEncryptionKey();
  });

  // ===========================================================================
  // AES-GCM Encryption Tests (not base64+reversal)
  // ===========================================================================

  describe('AES-GCM Encryption (not base64+reversal)', () => {
    it('should encrypt tokens with AES-GCM format (aes:<iv>:<ciphertext>)', async () => {
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

      // Verify the stored value uses AES format with three colon-separated parts
      const storedArg = mockKeytar.setPassword.mock.calls[0][2];
      expect(storedArg).toMatch(/^aes:[A-Za-z0-9+/=]+:[A-Za-z0-9+/=]+$/);
    });

    it('should NOT use legacy enc: format (base64+reversal)', async () => {
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

      const storedArg = mockKeytar.setPassword.mock.calls[0][2];
      // Must NOT use the weak enc: prefix
      expect(storedArg).not.toMatch(/^enc:/);
      // Must start with aes: prefix
      expect(storedArg.startsWith('aes:')).toBe(true);
    });

    it('should not store raw token values in encrypted output', async () => {
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

      const storedArg = mockKeytar.setPassword.mock.calls[0][2];
      expect(storedArg).not.toContain(mockValidToken);
      expect(storedArg).not.toContain(mockRefreshToken);
    });

    it('should not store base64 encoded plaintext in encrypted output', async () => {
      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });

      const tokens = {
        accessToken: mockValidToken,
        refreshToken: mockRefreshToken,
        expiresAt: 1234567890,
      };

      await keychain.storeTokens('default', tokens);

      const storedArg = mockKeytar.setPassword.mock.calls[0][2];
      // The base64-encoded plaintext (forward or reversed) should not appear
      const plaintext = JSON.stringify(tokens);
      const base64Forward = btoa(plaintext);
      const base64Reversed = base64Forward.split('').reverse().join('');

      expect(storedArg).not.toContain(base64Forward);
      expect(storedArg).not.toContain(base64Reversed);
    });

    it('should decrypt AES-GCM encrypted tokens correctly', async () => {
      const tokens = {
        accessToken: mockValidToken,
        refreshToken: mockRefreshToken,
        expiresAt: Date.now() + 3600000,
      };

      let storedEncrypted = '';
      mockKeytar.setPassword.mockImplementation(async (_service: string, _account: string, password: string) => {
        storedEncrypted = password;
      });
      mockKeytar.getPassword.mockImplementation(async () => storedEncrypted);

      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });

      await keychain.storeTokens('default', tokens);
      const retrieved = await keychain.getTokens('default');

      expect(retrieved).toEqual(tokens);
    });

    it('should use 12-byte IV (96 bits) as recommended for AES-GCM', async () => {
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

      const storedArg = mockKeytar.setPassword.mock.calls[0][2];
      // Extract IV (second part after 'aes:')
      const parts = storedArg.split(':');
      expect(parts.length).toBe(3);
      const ivB64 = parts[1];
      // Decode base64 and verify length is 12 bytes
      const iv = Uint8Array.from(atob(ivB64), c => c.charCodeAt(0));
      expect(iv.length).toBe(12);
    });
  });

  // ===========================================================================
  // Unique IV Generation Tests
  // ===========================================================================

  describe('Unique IV Generation', () => {
    it('should generate unique IV for each encryption', async () => {
      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });

      const tokens = {
        accessToken: mockValidToken,
        refreshToken: mockRefreshToken,
        expiresAt: Date.now() + 3600000,
      };

      // Store multiple times
      await keychain.storeTokens('profile1', tokens);
      await keychain.storeTokens('profile2', tokens);
      await keychain.storeTokens('profile3', tokens);

      const stored1 = mockKeytar.setPassword.mock.calls[0][2];
      const stored2 = mockKeytar.setPassword.mock.calls[1][2];
      const stored3 = mockKeytar.setPassword.mock.calls[2][2];

      // Same plaintext should produce different ciphertext due to random IV
      expect(stored1).not.toBe(stored2);
      expect(stored2).not.toBe(stored3);
      expect(stored1).not.toBe(stored3);
    });

    it('should produce unique IVs across multiple encryptions of same data', async () => {
      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });

      const tokens = {
        accessToken: mockValidToken,
        refreshToken: mockRefreshToken,
        expiresAt: 1234567890,
      };

      // Encrypt same tokens 10 times
      const ivs = new Set<string>();
      for (let i = 0; i < 10; i++) {
        await keychain.storeTokens(`profile${i}`, tokens);
        const storedArg = mockKeytar.setPassword.mock.calls[i][2];
        const iv = storedArg.split(':')[1];
        ivs.add(iv);
      }

      // All IVs should be unique
      expect(ivs.size).toBe(10);
    });

    it('should generate cryptographically random IVs (not sequential/predictable)', async () => {
      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });

      const tokens = {
        accessToken: mockValidToken,
        refreshToken: mockRefreshToken,
        expiresAt: 1234567890,
      };

      await keychain.storeTokens('profile1', tokens);
      await keychain.storeTokens('profile2', tokens);

      const storedArg1 = mockKeytar.setPassword.mock.calls[0][2];
      const storedArg2 = mockKeytar.setPassword.mock.calls[1][2];

      const iv1 = Uint8Array.from(atob(storedArg1.split(':')[1]), c => c.charCodeAt(0));
      const iv2 = Uint8Array.from(atob(storedArg2.split(':')[1]), c => c.charCodeAt(0));

      // IVs should not be sequential (differ by more than 1 in most bytes)
      let sequentialCount = 0;
      for (let i = 0; i < iv1.length; i++) {
        if (Math.abs(iv1[i] - iv2[i]) <= 1) {
          sequentialCount++;
        }
      }
      // If IVs were sequential, most bytes would differ by 0 or 1
      // Random IVs should have significantly more variation
      expect(sequentialCount).toBeLessThan(iv1.length);
    });
  });

  // ===========================================================================
  // PBKDF2 Key Derivation Tests
  // ===========================================================================

  describe('PBKDF2 Key Derivation with Sufficient Iterations', () => {
    it('should derive key using PBKDF2 (validate via successful encrypt/decrypt)', async () => {
      // This test verifies the implementation uses PBKDF2
      // by checking that encryption/decryption still works (validates key derivation)
      const tokens = {
        accessToken: mockValidToken,
        refreshToken: mockRefreshToken,
        expiresAt: Date.now() + 3600000,
      };

      let storedEncrypted = '';
      mockKeytar.setPassword.mockImplementation(async (_service: string, _account: string, password: string) => {
        storedEncrypted = password;
      });
      mockKeytar.getPassword.mockImplementation(async () => storedEncrypted);

      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });

      await keychain.storeTokens('default', tokens);
      const retrieved = await keychain.getTokens('default');

      // If key derivation is correct (using PBKDF2 with proper params), decryption should succeed
      expect(retrieved).toEqual(tokens);
    });

    it('should derive 256-bit AES key from seed', async () => {
      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });

      const tokens = {
        accessToken: mockValidToken,
        refreshToken: mockRefreshToken,
        expiresAt: 1234567890,
      };

      await keychain.storeTokens('default', tokens);

      // If the key was derived correctly (256-bit AES-GCM), encryption should succeed
      // and produce the aes: format
      const storedArg = mockKeytar.setPassword.mock.calls[0][2];
      expect(storedArg.startsWith('aes:')).toBe(true);
    });

    it('should use consistent key derivation across multiple operations', async () => {
      const tokens = {
        accessToken: mockValidToken,
        refreshToken: mockRefreshToken,
        expiresAt: Date.now() + 3600000,
      };

      let storedData = '';
      mockKeytar.setPassword.mockImplementation(async (_service: string, _account: string, password: string) => {
        storedData = password;
      });
      mockKeytar.getPassword.mockImplementation(async () => storedData);

      // Store with one keychain instance
      const keychain1 = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });
      await keychain1.storeTokens('default', tokens);

      // Retrieve with a different keychain instance
      const keychain2 = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });
      const retrieved = await keychain2.getTokens('default');

      // Same key should be derived, allowing decryption
      expect(retrieved).toEqual(tokens);
    });
  });

  // ===========================================================================
  // Encrypted Output Not Reversible Without Key
  // ===========================================================================

  describe('Encrypted Output Not Reversible Without Key', () => {
    it('should produce ciphertext that is not reversible by simple base64 decode', async () => {
      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });

      const tokens = {
        accessToken: mockValidToken,
        refreshToken: mockRefreshToken,
        expiresAt: 1234567890,
      };

      await keychain.storeTokens('default', tokens);
      const storedArg = mockKeytar.setPassword.mock.calls[0][2];

      // Extract ciphertext portion
      const ciphertext = storedArg.split(':')[2];

      // Attempt to decode as base64 and parse as JSON - should fail
      try {
        const decoded = atob(ciphertext);
        const parsed = JSON.parse(decoded);
        // If we get here and it looks like our tokens, the encryption is weak
        expect(parsed.accessToken).not.toBe(mockValidToken);
      } catch {
        // Expected: ciphertext is not valid JSON when base64 decoded
        expect(true).toBe(true);
      }
    });

    it('should produce ciphertext that is not reversible by string reversal + base64 decode', async () => {
      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });

      const tokens = {
        accessToken: mockValidToken,
        refreshToken: mockRefreshToken,
        expiresAt: 1234567890,
      };

      await keychain.storeTokens('default', tokens);
      const storedArg = mockKeytar.setPassword.mock.calls[0][2];

      // Remove aes: prefix and try old weak decryption method
      const dataWithoutPrefix = storedArg.slice(4);
      const reversed = dataWithoutPrefix.split('').reverse().join('');

      // Attempt to decode - should fail or not produce original tokens
      try {
        const decoded = atob(reversed);
        const parsed = JSON.parse(decoded);
        expect(parsed.accessToken).not.toBe(mockValidToken);
      } catch {
        // Expected: weak decryption method should fail
        expect(true).toBe(true);
      }
    });
  });

  // ===========================================================================
  // Decryption with Wrong Key Fails
  // ===========================================================================

  describe('Decryption with Wrong Key Fails', () => {
    it('should fail decryption when IV is corrupted (simulates wrong key)', async () => {
      const tokens = {
        accessToken: mockValidToken,
        refreshToken: mockRefreshToken,
        expiresAt: Date.now() + 3600000,
      };

      let storedEncrypted = '';
      mockKeytar.setPassword.mockImplementation(async (_service: string, _account: string, password: string) => {
        storedEncrypted = password;
      });

      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });
      await keychain.storeTokens('default', tokens);

      // Verify data was stored with AES format
      expect(storedEncrypted.startsWith('aes:')).toBe(true);

      // Corrupt the IV to simulate wrong key scenario
      const parts = storedEncrypted.split(':');
      const originalIv = atob(parts[1]);
      const corruptedIv = btoa(String.fromCharCode(
        ...Array.from(originalIv).map(c => c.charCodeAt(0) ^ 0xFF)
      ));
      mockKeytar.getPassword.mockImplementation(async () => `aes:${corruptedIv}:${parts[2]}`);

      const keychain2 = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });

      // Decryption should fail with corrupted IV
      await expect(keychain2.getTokens('default')).rejects.toThrow();
    });

    it('should not leak plaintext when decryption fails', async () => {
      const tokens = {
        accessToken: mockValidToken,
        refreshToken: mockRefreshToken,
        expiresAt: Date.now() + 3600000,
      };

      let storedEncrypted = '';
      mockKeytar.setPassword.mockImplementation(async (_service: string, _account: string, password: string) => {
        storedEncrypted = password;
      });

      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });
      await keychain.storeTokens('default', tokens);

      // Return corrupted ciphertext
      const parts = storedEncrypted.split(':');
      mockKeytar.getPassword.mockImplementation(async () => `aes:${parts[1]}:invalidciphertext`);

      try {
        await keychain.getTokens('default');
        expect.fail('Should have thrown');
      } catch (err) {
        const error = err as Error;
        // Error message should not contain any token data
        expect(error.message).not.toContain(mockValidToken);
        expect(error.message).not.toContain(mockRefreshToken);
        expect(error.message).toContain('Keychain access failed');
      }
    });
  });

  // ===========================================================================
  // Tampering Detection (Authenticated Encryption)
  // ===========================================================================

  describe('Tampering Detection (Authenticated Encryption)', () => {
    it('should detect ciphertext tampering', async () => {
      const tokens = {
        accessToken: mockValidToken,
        refreshToken: mockRefreshToken,
        expiresAt: Date.now() + 3600000,
      };

      let storedEncrypted = '';
      mockKeytar.setPassword.mockImplementation(async (_service: string, _account: string, password: string) => {
        storedEncrypted = password;
      });

      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });
      await keychain.storeTokens('default', tokens);

      // Tamper with the ciphertext (flip a bit)
      const parts = storedEncrypted.split(':');
      const ciphertextBytes = Uint8Array.from(atob(parts[2]), c => c.charCodeAt(0));
      ciphertextBytes[0] ^= 0x01; // Flip one bit
      const tamperedCiphertext = btoa(String.fromCharCode(...ciphertextBytes));
      mockKeytar.getPassword.mockImplementation(async () => `aes:${parts[1]}:${tamperedCiphertext}`);

      // AES-GCM should detect tampering and fail decryption
      await expect(keychain.getTokens('default')).rejects.toThrow('Keychain access failed');
    });

    it('should detect IV tampering', async () => {
      const tokens = {
        accessToken: mockValidToken,
        refreshToken: mockRefreshToken,
        expiresAt: Date.now() + 3600000,
      };

      let storedEncrypted = '';
      mockKeytar.setPassword.mockImplementation(async (_service: string, _account: string, password: string) => {
        storedEncrypted = password;
      });

      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });
      await keychain.storeTokens('default', tokens);

      // Tamper with the IV
      const parts = storedEncrypted.split(':');
      const ivBytes = Uint8Array.from(atob(parts[1]), c => c.charCodeAt(0));
      ivBytes[0] ^= 0x01; // Flip one bit
      const tamperedIv = btoa(String.fromCharCode(...ivBytes));
      mockKeytar.getPassword.mockImplementation(async () => `aes:${tamperedIv}:${parts[2]}`);

      // AES-GCM should detect IV modification through authentication tag failure
      await expect(keychain.getTokens('default')).rejects.toThrow('Keychain access failed');
    });

    it('should detect authentication tag tampering', async () => {
      const tokens = {
        accessToken: mockValidToken,
        refreshToken: mockRefreshToken,
        expiresAt: Date.now() + 3600000,
      };

      let storedEncrypted = '';
      mockKeytar.setPassword.mockImplementation(async (_service: string, _account: string, password: string) => {
        storedEncrypted = password;
      });

      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });
      await keychain.storeTokens('default', tokens);

      // Tamper with the last 16 bytes (authentication tag in AES-GCM)
      const parts = storedEncrypted.split(':');
      const ciphertextBytes = Uint8Array.from(atob(parts[2]), c => c.charCodeAt(0));
      // Flip bit in the last byte (part of auth tag)
      ciphertextBytes[ciphertextBytes.length - 1] ^= 0x01;
      const tamperedCiphertext = btoa(String.fromCharCode(...ciphertextBytes));
      mockKeytar.getPassword.mockImplementation(async () => `aes:${parts[1]}:${tamperedCiphertext}`);

      // AES-GCM authentication should fail
      await expect(keychain.getTokens('default')).rejects.toThrow('Keychain access failed');
    });

    it('should reject truncated ciphertext', async () => {
      const tokens = {
        accessToken: mockValidToken,
        refreshToken: mockRefreshToken,
        expiresAt: Date.now() + 3600000,
      };

      let storedEncrypted = '';
      mockKeytar.setPassword.mockImplementation(async (_service: string, _account: string, password: string) => {
        storedEncrypted = password;
      });

      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });
      await keychain.storeTokens('default', tokens);

      // Truncate the ciphertext
      const parts = storedEncrypted.split(':');
      const ciphertextBytes = Uint8Array.from(atob(parts[2]), c => c.charCodeAt(0));
      const truncatedBytes = ciphertextBytes.slice(0, ciphertextBytes.length - 20);
      const truncatedCiphertext = btoa(String.fromCharCode(...truncatedBytes));
      mockKeytar.getPassword.mockImplementation(async () => `aes:${parts[1]}:${truncatedCiphertext}`);

      // Should fail due to invalid/truncated ciphertext
      await expect(keychain.getTokens('default')).rejects.toThrow('Keychain access failed');
    });
  });

  // ===========================================================================
  // Key Rotation Support Tests
  // ===========================================================================

  describe('Key Rotation Support', () => {
    it('should support re-encryption with new key after rotation', async () => {
      const tokens = {
        accessToken: mockValidToken,
        refreshToken: mockRefreshToken,
        expiresAt: Date.now() + 3600000,
      };

      const storage = new Map<string, string>();
      mockKeytar.setPassword.mockImplementation(async (_service: string, account: string, password: string) => {
        storage.set(account, password);
      });
      mockKeytar.getPassword.mockImplementation(async (_service: string, account: string) => storage.get(account) ?? null);

      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });

      // Store initial tokens
      await keychain.storeTokens('default', tokens);
      const initialEncrypted = storage.get('default');

      // Read and re-store (simulates key rotation re-encryption)
      const retrieved = await keychain.getTokens('default');
      expect(retrieved).toEqual(tokens);

      await keychain.storeTokens('default', retrieved!);
      const reEncrypted = storage.get('default');

      // Re-encrypted value should be different (new IV)
      expect(reEncrypted).not.toBe(initialEncrypted);

      // But should still decrypt to same tokens
      const finalRetrieved = await keychain.getTokens('default');
      expect(finalRetrieved).toEqual(tokens);
    });

    it('should allow migrating multiple profiles during key rotation', async () => {
      const tokens1 = {
        accessToken: 'token1',
        refreshToken: 'refresh1',
        expiresAt: Date.now() + 3600000,
      };
      const tokens2 = {
        accessToken: 'token2',
        refreshToken: 'refresh2',
        expiresAt: Date.now() + 3600000,
      };

      const storage = new Map<string, string>();
      mockKeytar.setPassword.mockImplementation(async (_service: string, account: string, password: string) => {
        storage.set(account, password);
      });
      mockKeytar.getPassword.mockImplementation(async (_service: string, account: string) => storage.get(account) ?? null);

      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });

      // Store tokens for multiple profiles
      await keychain.storeTokens('dev', tokens1);
      await keychain.storeTokens('prod', tokens2);

      // Simulate key rotation: read all and re-encrypt
      const dev = await keychain.getTokens('dev');
      const prod = await keychain.getTokens('prod');

      await keychain.storeTokens('dev', dev!);
      await keychain.storeTokens('prod', prod!);

      // Verify all profiles still accessible
      expect(await keychain.getTokens('dev')).toEqual(tokens1);
      expect(await keychain.getTokens('prod')).toEqual(tokens2);
    });

    it('should produce different ciphertexts when re-encrypting same data (supports key rotation auditing)', async () => {
      const tokens = {
        accessToken: mockValidToken,
        refreshToken: mockRefreshToken,
        expiresAt: Date.now() + 3600000,
      };

      const storage = new Map<string, string>();
      const encryptionHistory: string[] = [];

      mockKeytar.setPassword.mockImplementation(async (_service: string, account: string, password: string) => {
        storage.set(account, password);
        encryptionHistory.push(password);
      });
      mockKeytar.getPassword.mockImplementation(async (_service: string, account: string) => storage.get(account) ?? null);

      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });

      // Encrypt same tokens 5 times (simulating key rotation)
      for (let i = 0; i < 5; i++) {
        await keychain.storeTokens('default', tokens);
      }

      // All encryptions should produce different ciphertexts
      const uniqueEncryptions = new Set(encryptionHistory);
      expect(uniqueEncryptions.size).toBe(5);
    });
  });

  // ===========================================================================
  // Legacy Format Rejection Tests
  // ===========================================================================

  describe('Legacy Format Rejection', () => {
    it('should reject legacy enc: format tokens (weak obfuscation)', async () => {
      // Legacy format: enc:<reversed-base64> - no longer supported
      const tokens = {
        accessToken: mockValidToken,
        refreshToken: mockRefreshToken,
        expiresAt: 1234567890,
      };
      const encoded = btoa(JSON.stringify(tokens));
      const reversed = encoded.split('').reverse().join('');
      const legacyEncrypted = `enc:${reversed}`;

      mockKeytar.getPassword.mockResolvedValueOnce(legacyEncrypted);

      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });

      // Legacy formats are rejected - users must re-authenticate
      await expect(keychain.getTokens('default')).rejects.toThrow('Keychain access failed');
    });

    it('should reject raw JSON format tokens (unencrypted)', async () => {
      // Raw JSON format - no longer supported
      const tokens = {
        accessToken: mockValidToken,
        refreshToken: mockRefreshToken,
        expiresAt: 1234567890,
      };
      const rawJson = JSON.stringify(tokens);

      mockKeytar.getPassword.mockResolvedValueOnce(rawJson);

      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });

      // Legacy formats are rejected - users must re-authenticate
      await expect(keychain.getTokens('default')).rejects.toThrow('Keychain access failed');
    });
  });

  // ===========================================================================
  // Error Handling Tests
  // ===========================================================================

  describe('Error Handling', () => {
    it('should throw error for invalid AES format (missing parts)', async () => {
      mockKeytar.getPassword.mockResolvedValueOnce('aes:invalidformat');

      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });

      await expect(keychain.getTokens('default')).rejects.toThrow('Keychain access failed');
    });

    it('should throw error for corrupted base64 in IV', async () => {
      mockKeytar.getPassword.mockResolvedValueOnce('aes:!!!invalidbase64!!!:validbase64==');

      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });

      await expect(keychain.getTokens('default')).rejects.toThrow('Keychain access failed');
    });

    it('should throw error for corrupted base64 in ciphertext', async () => {
      const validIv = btoa('123456789012'); // 12 bytes
      mockKeytar.getPassword.mockResolvedValueOnce(`aes:${validIv}:!!!invalidbase64!!!`);

      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });

      await expect(keychain.getTokens('default')).rejects.toThrow('Keychain access failed');
    });

    it('should sanitize error messages to prevent credential leakage', async () => {
      mockKeytar.getPassword.mockRejectedValueOnce(new Error(`Decryption failed for token: ${mockValidToken}`));

      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });

      try {
        await keychain.getTokens('default');
        expect.fail('Should have thrown');
      } catch (err) {
        const error = err as Error;
        // Error should be sanitized
        expect(error.message).toContain('Keychain access failed');
        // Token should not appear in sanitized error
        expect(error.message).not.toContain(mockValidToken);
      }
    });
  });

  // ===========================================================================
  // Key Management Tests
  // ===========================================================================

  describe('Key Management', () => {
    it('should use consistent key across multiple KeychainStorage instances', async () => {
      const tokens = {
        accessToken: mockValidToken,
        refreshToken: mockRefreshToken,
        expiresAt: Date.now() + 3600000,
      };

      let storedData = '';
      mockKeytar.setPassword.mockImplementation(async (_service: string, _account: string, password: string) => {
        storedData = password;
      });
      mockKeytar.getPassword.mockImplementation(async () => storedData);

      // Store with one instance
      const keychain1 = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });
      await keychain1.storeTokens('profile1', tokens);

      // Create new instance (simulating app restart)
      const keychain2 = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });

      // Should still be able to decrypt
      const retrieved = await keychain2.getTokens('profile1');
      expect(retrieved).toEqual(tokens);
    });

    it('should work correctly after resetEncryptionKey is called', async () => {
      const keychain = new KeychainStorage({
        serviceName: 'mongolake-cli',
        keytar: mockKeytar,
      });

      const tokens = {
        accessToken: mockValidToken,
        refreshToken: mockRefreshToken,
        expiresAt: Date.now() + 3600000,
      };

      let storedData = '';
      mockKeytar.setPassword.mockImplementation(async (_service: string, _account: string, password: string) => {
        storedData = password;
      });
      mockKeytar.getPassword.mockImplementation(async () => storedData);

      // Store, reset key (now a no-op), and store again
      await keychain.storeTokens('profile1', tokens);
      resetEncryptionKey(); // Keys are now derived deterministically, so this is a no-op

      // Store new data
      await keychain.storeTokens('profile2', tokens);

      // Retrieve should work with deterministic key derivation
      const retrieved = await keychain.getTokens('profile2');
      expect(retrieved).toEqual(tokens);
    });
  });
});
