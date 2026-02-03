/**
 * Keychain Storage
 *
 * Secure storage for OAuth tokens using platform keychain
 * with AES-GCM encryption.
 */

import { sanitizeError } from '../utils/sanitize-error.js';
import type { KeytarLike, StoredTokens } from './types.js';

// ============================================================================
// Token Encryption using AES-GCM (Web Crypto API)
// ============================================================================

// Environment variable name for the encryption key (base64-encoded 256-bit key)
const ENCRYPTION_KEY_ENV = 'MONGOLAKE_ENCRYPTION_KEY';

// Default seed for deterministic key derivation when env var is not set
// This provides consistent key derivation across Worker instances
const DEFAULT_KEY_SEED = 'mongolake-default-encryption-seed-v1';

/**
 * Derive an AES-GCM encryption key deterministically from a seed string.
 * Uses PBKDF2 to derive a 256-bit key from the seed.
 */
async function deriveKeyFromSeed(seed: string): Promise<CryptoKey> {
  const encoder = new TextEncoder();
  const seedBytes = encoder.encode(seed);

  // Import the seed as a key for PBKDF2
  const baseKey = await crypto.subtle.importKey(
    'raw',
    seedBytes,
    'PBKDF2',
    false,
    ['deriveKey']
  );

  // Use a fixed salt for deterministic derivation across instances
  const salt = encoder.encode('mongolake-encryption-salt-v1');

  // Derive the AES-GCM key using PBKDF2
  return crypto.subtle.deriveKey(
    {
      name: 'PBKDF2',
      salt,
      iterations: 100000,
      hash: 'SHA-256',
    },
    baseKey,
    { name: 'AES-GCM', length: 256 },
    false,
    ['encrypt', 'decrypt']
  );
}

/**
 * Get the AES-GCM encryption key.
 *
 * Key derivation strategy (no mutable state - deterministic across Worker instances):
 * 1. Use key from MONGOLAKE_ENCRYPTION_KEY environment variable (base64-encoded 256-bit key)
 * 2. Derive key deterministically from default seed (consistent across cold starts)
 *
 * For production, set MONGOLAKE_ENCRYPTION_KEY env var with a base64-encoded 256-bit key.
 * Generate one with: node -e "console.log(require('crypto').randomBytes(32).toString('base64'))"
 */
async function getEncryptionKey(): Promise<CryptoKey> {
  // Try to get key from environment variable
  const envKey = typeof process !== 'undefined' && process.env?.[ENCRYPTION_KEY_ENV];

  if (envKey) {
    // Decode base64 key from environment
    const keyBytes = Uint8Array.from(atob(envKey), c => c.charCodeAt(0));
    if (keyBytes.length !== 32) {
      throw new Error(`${ENCRYPTION_KEY_ENV} must be a base64-encoded 256-bit (32 byte) key`);
    }
    return crypto.subtle.importKey(
      'raw',
      keyBytes,
      { name: 'AES-GCM', length: 256 },
      false,
      ['encrypt', 'decrypt']
    );
  }

  // Derive key deterministically from default seed
  // This ensures consistent key across Worker cold starts
  return deriveKeyFromSeed(DEFAULT_KEY_SEED);
}

/**
 * Reset the encryption key (no-op, kept for backward compatibility with tests)
 * @internal
 * @deprecated Keys are now derived deterministically, no mutable state to reset
 */
export function resetEncryptionKey(): void {
  // No-op: keys are now derived on each call, no mutable state
}

/**
 * Encrypt tokens using AES-GCM with a random IV.
 * Output format: aes:<base64(iv)>:<base64(ciphertext)>
 */
export async function encryptTokens(data: string): Promise<string> {
  const key = await getEncryptionKey();

  // Generate a random 12-byte IV (recommended size for AES-GCM)
  const iv = crypto.getRandomValues(new Uint8Array(12));

  // Encrypt the data
  const encoder = new TextEncoder();
  const plaintext = encoder.encode(data);

  const ciphertext = await crypto.subtle.encrypt(
    { name: 'AES-GCM', iv },
    key,
    plaintext
  );

  // Convert to base64 for storage
  const ivB64 = btoa(String.fromCharCode(...iv));
  const ciphertextB64 = btoa(String.fromCharCode(...new Uint8Array(ciphertext)));

  return `aes:${ivB64}:${ciphertextB64}`;
}

/**
 * Decrypt tokens encrypted with AES-GCM.
 *
 * Supported format:
 * - aes:<iv>:<ciphertext> - AES-GCM encrypted format
 *
 * Legacy formats (enc: obfuscation, raw JSON) are no longer supported
 * as they did not provide real security. Users with legacy tokens
 * will need to re-authenticate.
 */
export async function decryptTokens(encrypted: string): Promise<string> {
  // AES-GCM format: aes:<base64-iv>:<base64-ciphertext>
  if (encrypted.startsWith('aes:')) {
    const parts = encrypted.slice(4).split(':');
    if (parts.length !== 2) {
      throw new Error('Invalid encrypted token format');
    }

    const [ivB64, ciphertextB64] = parts;
    const iv = Uint8Array.from(atob(ivB64!), c => c.charCodeAt(0));
    const ciphertext = Uint8Array.from(atob(ciphertextB64!), c => c.charCodeAt(0));

    const key = await getEncryptionKey();

    const plaintext = await crypto.subtle.decrypt(
      { name: 'AES-GCM', iv },
      key,
      ciphertext
    );

    const decoder = new TextDecoder();
    return decoder.decode(plaintext);
  }

  // Reject legacy formats - they used weak obfuscation (not encryption)
  // Users with legacy tokens will need to re-authenticate
  throw new Error('Unsupported token format: legacy tokens must be re-encrypted');
}

// ============================================================================
// Keychain Storage
// ============================================================================

/**
 * Store and retrieve OAuth tokens from the platform keychain.
 * Supports multiple named profiles for different environments (dev, staging, prod, etc.)
 */
export class KeychainStorage {
  private serviceName: string;
  private keytar: KeytarLike;
  private profiles: Set<string> = new Set();

  constructor(config: { serviceName: string; keytar: KeytarLike }) {
    this.serviceName = config.serviceName;
    this.keytar = config.keytar;
  }

  /**
   * Store authentication tokens for a profile in the system keychain
   */
  async storeTokens(profile: string, tokens: StoredTokens): Promise<void> {
    const data = JSON.stringify(tokens);
    const encrypted = await encryptTokens(data);
    await this.keytar.setPassword(this.serviceName, profile, encrypted);
    this.profiles.add(profile);
  }

  /**
   * Retrieve tokens for a profile from the keychain
   * Returns null if profile not found
   */
  async getTokens(profile: string): Promise<StoredTokens | null> {
    try {
      const encrypted = await this.keytar.getPassword(this.serviceName, profile);
      if (!encrypted) {
        return null;
      }
      const decrypted = await decryptTokens(encrypted);
      return JSON.parse(decrypted);
    } catch (err) {
      // Never include token data in error messages
      const sanitized = sanitizeError(err);
      throw new Error(`Keychain access failed: ${sanitized.message}`);
    }
  }

  /**
   * Delete tokens for a profile from the keychain
   */
  async deleteTokens(profile: string): Promise<void> {
    await this.keytar.deletePassword(this.serviceName, profile);
    this.profiles.delete(profile);
  }

  /**
   * Get list of all stored profiles
   */
  async listProfiles(): Promise<string[]> {
    return Array.from(this.profiles);
  }
}
