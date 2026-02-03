/**
 * Native Keychain Integration for Secure Credential Storage
 *
 * Provides platform-specific secure storage for OAuth tokens:
 * - macOS: Keychain Services (via security CLI)
 * - Windows: Windows Credential Manager (via PowerShell)
 * - Linux: Secret Service (D-Bus) via secret-tool
 *
 * Falls back to file-based storage when native keychain is unavailable.
 *
 * @module cli/keychain
 */

import { execSync, exec } from 'node:child_process';
import { promisify } from 'node:util';

const execAsync = promisify(exec);

// ============================================================================
// Types and Interfaces
// ============================================================================

/**
 * Stored token data for keychain storage.
 */
export interface KeychainTokenData {
  /** The OAuth access token */
  accessToken: string;
  /** The refresh token for obtaining new access tokens */
  refreshToken?: string;
  /** Token expiration timestamp (ms since epoch) */
  expiresAt?: number;
  /** OAuth scope */
  scope?: string;
}

/**
 * Token metadata stored alongside the token.
 */
export interface TokenMetadata {
  /** Token expiration timestamp */
  expiresAt?: number;
  /** OAuth scope */
  scope?: string;
  /** Profile name */
  profile?: string;
  /** OAuth provider */
  provider?: string;
}

/**
 * Options for keychain operations.
 */
export interface KeychainOptions {
  /** Service name for keychain entries */
  serviceName?: string;
  /** Fall back to file storage if keychain unavailable */
  fallbackToFile?: boolean;
}

/**
 * Result of a keychain operation.
 */
export interface KeychainResult<T = void> {
  success: boolean;
  data?: T;
  error?: string;
  usedFallback?: boolean;
}

/**
 * Secure storage type for different platforms.
 */
export type SecureStorageType = 'keychain' | 'secret-service' | 'credential-manager' | 'file';

/**
 * Result of secure storage detection.
 */
export interface SecureStorageInfo {
  /** Whether native secure storage is available */
  available: boolean;
  /** Type of storage detected */
  type: SecureStorageType;
  /** Platform identifier */
  platform: NodeJS.Platform;
  /** Whether file-based fallback is available */
  fallbackAvailable: boolean;
}

/**
 * Abstract interface for platform-specific keychain implementations.
 */
export interface KeychainStorage {
  /** Get a token from the keychain */
  getToken(account: string): Promise<string | null>;
  /** Set a token in the keychain */
  setToken(account: string, token: string): Promise<void>;
  /** Delete a token from the keychain */
  deleteToken(account: string): Promise<void>;
  /** Get token with metadata */
  getTokenData(account: string): Promise<KeychainTokenData | null>;
  /** Set token with metadata */
  setTokenData(account: string, data: KeychainTokenData): Promise<void>;
  /** Check if keychain is available */
  isAvailable(): Promise<boolean>;
  /** Get the storage type */
  readonly storageType: SecureStorageType;
  /** Get the service name */
  readonly serviceName: string;
}

// ============================================================================
// Constants
// ============================================================================

/** Default service name for keychain entries */
export const DEFAULT_SERVICE_NAME = 'mongolake-cli';

/** Account name suffix for refresh tokens */
const REFRESH_TOKEN_SUFFIX = ':refresh';

/** Account name suffix for metadata */
const METADATA_SUFFIX = ':metadata';

/** Command timeout in milliseconds */
const COMMAND_TIMEOUT = 10000;

// ============================================================================
// Error Classes
// ============================================================================

/**
 * Error thrown when keychain operation fails.
 */
export class KeychainError extends Error {
  constructor(
    message: string,
    public readonly operation: 'get' | 'set' | 'delete' | 'check',
    public readonly platform: string,
    public readonly cause?: Error
  ) {
    super(message);
    this.name = 'KeychainError';
  }
}

/**
 * Error thrown when keychain is not available.
 */
export class KeychainUnavailableError extends Error {
  constructor(
    public readonly platform: string,
    public readonly reason: string
  ) {
    super(`Keychain unavailable on ${platform}: ${reason}`);
    this.name = 'KeychainUnavailableError';
  }
}

// ============================================================================
// Platform Detection
// ============================================================================

/**
 * Detect the current platform and available secure storage.
 */
export async function detectSecureStorage(): Promise<SecureStorageInfo> {
  const platform = process.platform;

  switch (platform) {
    case 'darwin':
      return {
        available: await checkMacOSKeychainAvailable(),
        type: 'keychain',
        platform,
        fallbackAvailable: true,
      };

    case 'win32':
      return {
        available: await checkWindowsCredentialManagerAvailable(),
        type: 'credential-manager',
        platform,
        fallbackAvailable: true,
      };

    case 'linux':
      const secretServiceAvailable = await checkLinuxSecretServiceAvailable();
      return {
        available: secretServiceAvailable,
        type: secretServiceAvailable ? 'secret-service' : 'file',
        platform,
        fallbackAvailable: true,
      };

    default:
      return {
        available: false,
        type: 'file',
        platform,
        fallbackAvailable: true,
      };
  }
}

/**
 * Check if macOS Keychain is available.
 */
async function checkMacOSKeychainAvailable(): Promise<boolean> {
  try {
    execSync('which security', { timeout: COMMAND_TIMEOUT, stdio: 'pipe' });
    return true;
  } catch {
    /* Expected when security CLI is not installed */
    return false;
  }
}

/**
 * Check if Windows Credential Manager is available.
 */
async function checkWindowsCredentialManagerAvailable(): Promise<boolean> {
  try {
    execSync('where cmdkey', { timeout: COMMAND_TIMEOUT, stdio: 'pipe' });
    return true;
  } catch {
    /* Expected when cmdkey is not available */
    return false;
  }
}

/**
 * Check if Linux Secret Service is available.
 */
async function checkLinuxSecretServiceAvailable(): Promise<boolean> {
  try {
    // Check if secret-tool is available
    execSync('which secret-tool', { timeout: COMMAND_TIMEOUT, stdio: 'pipe' });
    // Check if D-Bus session is available
    if (!process.env.DBUS_SESSION_BUS_ADDRESS) {
      return false;
    }
    return true;
  } catch {
    /* Expected when secret-tool is not installed */
    return false;
  }
}

// ============================================================================
// macOS Keychain Implementation
// ============================================================================

/**
 * macOS Keychain Services implementation using security CLI.
 */
export class MacOSKeychainStorage implements KeychainStorage {
  readonly storageType: SecureStorageType = 'keychain';
  readonly serviceName: string;

  constructor(serviceName: string = DEFAULT_SERVICE_NAME) {
    this.serviceName = serviceName;
  }

  async isAvailable(): Promise<boolean> {
    return checkMacOSKeychainAvailable();
  }

  async getToken(account: string): Promise<string | null> {
    try {
      const { stdout } = await execAsync(
        `security find-generic-password -s "${this.serviceName}" -a "${account}" -w 2>/dev/null`,
        { timeout: COMMAND_TIMEOUT }
      );
      return stdout.trim() || null;
    } catch {
      /* Expected when keychain item does not exist */
      return null;
    }
  }

  async setToken(account: string, token: string): Promise<void> {
    try {
      // First try to delete existing entry (ignore errors if not found)
      await this.deleteToken(account).catch(() => {
        /* Expected when keychain entry does not exist yet */
      });

      // Add new entry
      await execAsync(
        `security add-generic-password -s "${this.serviceName}" -a "${account}" -w "${this.escapeForShell(token)}" -U`,
        { timeout: COMMAND_TIMEOUT }
      );
    } catch (error) {
      throw new KeychainError(
        `Failed to store token in macOS Keychain`,
        'set',
        'darwin',
        error instanceof Error ? error : undefined
      );
    }
  }

  async deleteToken(account: string): Promise<void> {
    try {
      await execAsync(
        `security delete-generic-password -s "${this.serviceName}" -a "${account}" 2>/dev/null`,
        { timeout: COMMAND_TIMEOUT }
      );
    } catch {
      /* Expected when keychain item does not exist */
    }
  }

  async getTokenData(account: string): Promise<KeychainTokenData | null> {
    const accessToken = await this.getToken(account);
    if (!accessToken) {
      return null;
    }

    const refreshToken = await this.getToken(`${account}${REFRESH_TOKEN_SUFFIX}`);
    const metadataStr = await this.getToken(`${account}${METADATA_SUFFIX}`);

    let metadata: TokenMetadata = {};
    if (metadataStr) {
      try {
        metadata = JSON.parse(metadataStr);
      } catch {
        /* Expected when metadata is corrupted or in old format - use defaults */
      }
    }

    return {
      accessToken,
      refreshToken: refreshToken ?? undefined,
      expiresAt: metadata.expiresAt,
      scope: metadata.scope,
    };
  }

  async setTokenData(account: string, data: KeychainTokenData): Promise<void> {
    // Store access token
    await this.setToken(account, data.accessToken);

    // Store refresh token if present
    if (data.refreshToken) {
      await this.setToken(`${account}${REFRESH_TOKEN_SUFFIX}`, data.refreshToken);
    }

    // Store metadata
    const metadata: TokenMetadata = {
      expiresAt: data.expiresAt,
      scope: data.scope,
    };
    await this.setToken(`${account}${METADATA_SUFFIX}`, JSON.stringify(metadata));
  }

  /**
   * Escape special characters for shell command.
   */
  private escapeForShell(value: string): string {
    // Escape backslashes and double quotes
    return value.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
  }
}

// ============================================================================
// Windows Credential Manager Implementation
// ============================================================================

/**
 * Windows Credential Manager implementation using PowerShell.
 */
export class WindowsCredentialStorage implements KeychainStorage {
  readonly storageType: SecureStorageType = 'credential-manager';
  readonly serviceName: string;

  constructor(serviceName: string = DEFAULT_SERVICE_NAME) {
    this.serviceName = serviceName;
  }

  async isAvailable(): Promise<boolean> {
    return checkWindowsCredentialManagerAvailable();
  }

  async getToken(account: string): Promise<string | null> {
    try {
      const target = `${this.serviceName}:${account}`;
      const script = `
        $cred = Get-StoredCredential -Target "${target}" -ErrorAction SilentlyContinue
        if ($cred) {
          $cred.GetNetworkCredential().Password
        }
      `;
      const { stdout } = await execAsync(
        `powershell -NoProfile -Command "${script.replace(/"/g, '\\"')}"`,
        { timeout: COMMAND_TIMEOUT }
      );
      return stdout.trim() || null;
    } catch {
      /* PowerShell Get-StoredCredential failed, fallback to cmdkey approach */
      return this.getTokenViaCmdkey(account);
    }
  }

  private async getTokenViaCmdkey(account: string): Promise<string | null> {
    try {
      const target = `${this.serviceName}:${account}`;
      // Note: cmdkey can list credentials but not retrieve passwords directly
      // We use a registry-based approach or CredRead API via PowerShell
      const script = `
        Add-Type -AssemblyName System.Security
        $target = "${target}"
        try {
          $cred = [System.Net.CredentialCache]::DefaultNetworkCredentials
          $cred.Password
        } catch {
          ""
        }
      `;
      const { stdout } = await execAsync(
        `powershell -NoProfile -Command "${script.replace(/"/g, '\\"')}"`,
        { timeout: COMMAND_TIMEOUT }
      );
      return stdout.trim() || null;
    } catch {
      /* Expected when credential does not exist in Windows Credential Manager */
      return null;
    }
  }

  async setToken(account: string, token: string): Promise<void> {
    try {
      const target = `${this.serviceName}:${account}`;
      const script = `
        $target = "${target}"
        $password = ConvertTo-SecureString "${this.escapeForPowerShell(token)}" -AsPlainText -Force
        $cred = New-Object System.Management.Automation.PSCredential($target, $password)
        cmdkey /generic:$target /user:$target /pass:"${this.escapeForPowerShell(token)}"
      `;
      await execAsync(
        `powershell -NoProfile -Command "${script.replace(/\n/g, ' ')}"`,
        { timeout: COMMAND_TIMEOUT }
      );
    } catch (error) {
      throw new KeychainError(
        'Failed to store token in Windows Credential Manager',
        'set',
        'win32',
        error instanceof Error ? error : undefined
      );
    }
  }

  async deleteToken(account: string): Promise<void> {
    try {
      const target = `${this.serviceName}:${account}`;
      await execAsync(`cmdkey /delete:${target}`, { timeout: COMMAND_TIMEOUT });
    } catch {
      /* Expected when credential does not exist in Windows Credential Manager */
    }
  }

  async getTokenData(account: string): Promise<KeychainTokenData | null> {
    const accessToken = await this.getToken(account);
    if (!accessToken) {
      return null;
    }

    const refreshToken = await this.getToken(`${account}${REFRESH_TOKEN_SUFFIX}`);
    const metadataStr = await this.getToken(`${account}${METADATA_SUFFIX}`);

    let metadata: TokenMetadata = {};
    if (metadataStr) {
      try {
        metadata = JSON.parse(metadataStr);
      } catch {
        /* Expected when metadata is corrupted or in old format - use defaults */
      }
    }

    return {
      accessToken,
      refreshToken: refreshToken ?? undefined,
      expiresAt: metadata.expiresAt,
      scope: metadata.scope,
    };
  }

  async setTokenData(account: string, data: KeychainTokenData): Promise<void> {
    await this.setToken(account, data.accessToken);

    if (data.refreshToken) {
      await this.setToken(`${account}${REFRESH_TOKEN_SUFFIX}`, data.refreshToken);
    }

    const metadata: TokenMetadata = {
      expiresAt: data.expiresAt,
      scope: data.scope,
    };
    await this.setToken(`${account}${METADATA_SUFFIX}`, JSON.stringify(metadata));
  }

  /**
   * Escape special characters for PowerShell.
   */
  private escapeForPowerShell(value: string): string {
    return value.replace(/`/g, '``').replace(/"/g, '`"').replace(/\$/g, '`$');
  }
}

// ============================================================================
// Linux Secret Service Implementation
// ============================================================================

/**
 * Linux Secret Service implementation using secret-tool CLI.
 */
export class LinuxSecretStorage implements KeychainStorage {
  readonly storageType: SecureStorageType = 'secret-service';
  readonly serviceName: string;

  constructor(serviceName: string = DEFAULT_SERVICE_NAME) {
    this.serviceName = serviceName;
  }

  async isAvailable(): Promise<boolean> {
    return checkLinuxSecretServiceAvailable();
  }

  async getToken(account: string): Promise<string | null> {
    try {
      const { stdout } = await execAsync(
        `secret-tool lookup service "${this.serviceName}" account "${account}"`,
        { timeout: COMMAND_TIMEOUT }
      );
      return stdout.trim() || null;
    } catch {
      /* Expected when secret does not exist in Secret Service */
      return null;
    }
  }

  async setToken(account: string, token: string): Promise<void> {
    try {
      // Use stdin to pass the secret to avoid command line exposure
      const { exec } = await import('node:child_process');
      await new Promise<void>((resolve, reject) => {
        const proc = exec(
          `secret-tool store --label="${this.serviceName}:${account}" service "${this.serviceName}" account "${account}"`,
          { timeout: COMMAND_TIMEOUT },
          (error) => {
            if (error) reject(error);
            else resolve();
          }
        );
        proc.stdin?.write(token);
        proc.stdin?.end();
      });
    } catch (error) {
      throw new KeychainError(
        'Failed to store token in Linux Secret Service',
        'set',
        'linux',
        error instanceof Error ? error : undefined
      );
    }
  }

  async deleteToken(account: string): Promise<void> {
    try {
      await execAsync(
        `secret-tool clear service "${this.serviceName}" account "${account}"`,
        { timeout: COMMAND_TIMEOUT }
      );
    } catch {
      /* Expected when secret does not exist in Secret Service */
    }
  }

  async getTokenData(account: string): Promise<KeychainTokenData | null> {
    const accessToken = await this.getToken(account);
    if (!accessToken) {
      return null;
    }

    const refreshToken = await this.getToken(`${account}${REFRESH_TOKEN_SUFFIX}`);
    const metadataStr = await this.getToken(`${account}${METADATA_SUFFIX}`);

    let metadata: TokenMetadata = {};
    if (metadataStr) {
      try {
        metadata = JSON.parse(metadataStr);
      } catch {
        /* Expected when metadata is corrupted or in old format - use defaults */
      }
    }

    return {
      accessToken,
      refreshToken: refreshToken ?? undefined,
      expiresAt: metadata.expiresAt,
      scope: metadata.scope,
    };
  }

  async setTokenData(account: string, data: KeychainTokenData): Promise<void> {
    await this.setToken(account, data.accessToken);

    if (data.refreshToken) {
      await this.setToken(`${account}${REFRESH_TOKEN_SUFFIX}`, data.refreshToken);
    }

    const metadata: TokenMetadata = {
      expiresAt: data.expiresAt,
      scope: data.scope,
    };
    await this.setToken(`${account}${METADATA_SUFFIX}`, JSON.stringify(metadata));
  }
}

// ============================================================================
// File-Based Fallback Storage
// ============================================================================

import * as fs from 'node:fs';
import * as path from 'node:path';
import * as os from 'node:os';
import * as crypto from 'node:crypto';

/** Config directory name */
const CONFIG_DIR_NAME = '.mongolake';

/** Keychain file name */
const KEYCHAIN_FILE_NAME = 'keychain.json';

/** Directory permissions (owner read/write/execute only) */
const DIR_PERMISSIONS = 0o700;

/** File permissions (owner read/write only) */
const FILE_PERMISSIONS = 0o600;

/**
 * File-based storage as a fallback when native keychain is unavailable.
 * Uses AES-256-GCM encryption with a machine-specific key.
 */
export class FileBasedStorage implements KeychainStorage {
  readonly storageType: SecureStorageType = 'file';
  readonly serviceName: string;
  private readonly filePath: string;
  private encryptionKey: Buffer | null = null;

  constructor(serviceName: string = DEFAULT_SERVICE_NAME) {
    this.serviceName = serviceName;
    this.filePath = path.join(os.homedir(), CONFIG_DIR_NAME, KEYCHAIN_FILE_NAME);
  }

  async isAvailable(): Promise<boolean> {
    return true; // File-based storage is always available
  }

  async getToken(account: string): Promise<string | null> {
    const data = await this.readStorage();
    const entry = data[this.getKey(account)];
    if (!entry) {
      return null;
    }

    try {
      return this.decrypt(entry);
    } catch {
      /* Expected when encryption key changed (e.g., machine ID changed) or data corrupted */
      return null;
    }
  }

  async setToken(account: string, token: string): Promise<void> {
    const data = await this.readStorage();
    data[this.getKey(account)] = this.encrypt(token);
    await this.writeStorage(data);
  }

  async deleteToken(account: string): Promise<void> {
    const data = await this.readStorage();
    delete data[this.getKey(account)];
    await this.writeStorage(data);
  }

  async getTokenData(account: string): Promise<KeychainTokenData | null> {
    const accessToken = await this.getToken(account);
    if (!accessToken) {
      return null;
    }

    const refreshToken = await this.getToken(`${account}${REFRESH_TOKEN_SUFFIX}`);
    const metadataStr = await this.getToken(`${account}${METADATA_SUFFIX}`);

    let metadata: TokenMetadata = {};
    if (metadataStr) {
      try {
        metadata = JSON.parse(metadataStr);
      } catch {
        /* Expected when metadata is corrupted or in old format - use defaults */
      }
    }

    return {
      accessToken,
      refreshToken: refreshToken ?? undefined,
      expiresAt: metadata.expiresAt,
      scope: metadata.scope,
    };
  }

  async setTokenData(account: string, data: KeychainTokenData): Promise<void> {
    await this.setToken(account, data.accessToken);

    if (data.refreshToken) {
      await this.setToken(`${account}${REFRESH_TOKEN_SUFFIX}`, data.refreshToken);
    }

    const metadata: TokenMetadata = {
      expiresAt: data.expiresAt,
      scope: data.scope,
    };
    await this.setToken(`${account}${METADATA_SUFFIX}`, JSON.stringify(metadata));
  }

  /**
   * Get the storage key for an account.
   */
  private getKey(account: string): string {
    return `${this.serviceName}:${account}`;
  }

  /**
   * Get or derive the encryption key.
   */
  private getEncryptionKey(): Buffer {
    if (this.encryptionKey) {
      return this.encryptionKey;
    }

    // Derive key from machine-specific data
    const machineId = this.getMachineId();
    const salt = 'mongolake-keychain-v1';
    this.encryptionKey = crypto.pbkdf2Sync(machineId, salt, 100000, 32, 'sha256');
    return this.encryptionKey;
  }

  /**
   * Get a machine-specific identifier.
   */
  private getMachineId(): string {
    // Use combination of hostname, username, and home directory
    const hostname = os.hostname();
    const username = os.userInfo().username;
    const homedir = os.homedir();
    return `${hostname}:${username}:${homedir}`;
  }

  /**
   * Encrypt a value using AES-256-GCM.
   */
  private encrypt(value: string): string {
    const key = this.getEncryptionKey();
    const iv = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);

    let encrypted = cipher.update(value, 'utf8', 'hex');
    encrypted += cipher.final('hex');
    const authTag = cipher.getAuthTag();

    // Combine IV, auth tag, and encrypted data
    return `${iv.toString('hex')}:${authTag.toString('hex')}:${encrypted}`;
  }

  /**
   * Decrypt a value using AES-256-GCM.
   */
  private decrypt(encryptedValue: string): string {
    const key = this.getEncryptionKey();
    const parts = encryptedValue.split(':');

    if (parts.length !== 3) {
      throw new Error('Invalid encrypted value format');
    }

    const iv = Buffer.from(parts[0]!, 'hex');
    const authTag = Buffer.from(parts[1]!, 'hex');
    const encrypted = parts[2]!;

    const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
    decipher.setAuthTag(authTag);

    let decrypted = decipher.update(encrypted, 'hex', 'utf8');
    decrypted += decipher.final('utf8');
    return decrypted;
  }

  /**
   * Read storage data from file.
   */
  private async readStorage(): Promise<Record<string, string>> {
    try {
      if (fs.existsSync(this.filePath)) {
        const content = fs.readFileSync(this.filePath, 'utf8');
        return JSON.parse(content);
      }
    } catch {
      /* Expected when file is corrupted, malformed JSON, or permission issues - return empty storage */
    }
    return {};
  }

  /**
   * Write storage data to file.
   */
  private async writeStorage(data: Record<string, string>): Promise<void> {
    const configDir = path.dirname(this.filePath);
    if (!fs.existsSync(configDir)) {
      fs.mkdirSync(configDir, { mode: DIR_PERMISSIONS, recursive: true });
    }
    fs.writeFileSync(this.filePath, JSON.stringify(data, null, 2), {
      mode: FILE_PERMISSIONS,
    });
  }
}

// ============================================================================
// Unified Keychain Storage Factory
// ============================================================================

/**
 * Options for creating a keychain storage instance.
 */
export interface CreateKeychainStorageOptions {
  /** Service name for keychain entries */
  serviceName?: string;
  /** Force use of file-based storage */
  forceFileFallback?: boolean;
  /** Callback when falling back to file storage */
  onFallback?: (reason: string) => void;
}

/**
 * Create a keychain storage instance appropriate for the current platform.
 */
export async function createKeychainStorage(
  options: CreateKeychainStorageOptions = {}
): Promise<KeychainStorage> {
  const { serviceName = DEFAULT_SERVICE_NAME, forceFileFallback = false, onFallback } = options;

  if (forceFileFallback) {
    onFallback?.('File-based storage forced by configuration');
    return new FileBasedStorage(serviceName);
  }

  const platform = process.platform;

  switch (platform) {
    case 'darwin': {
      const storage = new MacOSKeychainStorage(serviceName);
      if (await storage.isAvailable()) {
        return storage;
      }
      onFallback?.('macOS Keychain is not available');
      return new FileBasedStorage(serviceName);
    }

    case 'win32': {
      const storage = new WindowsCredentialStorage(serviceName);
      if (await storage.isAvailable()) {
        return storage;
      }
      onFallback?.('Windows Credential Manager is not available');
      return new FileBasedStorage(serviceName);
    }

    case 'linux': {
      const storage = new LinuxSecretStorage(serviceName);
      if (await storage.isAvailable()) {
        return storage;
      }
      onFallback?.('Linux Secret Service is not available');
      return new FileBasedStorage(serviceName);
    }

    default:
      onFallback?.(`Unsupported platform: ${platform}`);
      return new FileBasedStorage(serviceName);
  }
}

/**
 * Create a keychain storage instance synchronously with best-effort platform detection.
 * Falls back to file-based storage if async detection is not possible.
 */
export function createKeychainStorageSync(
  options: CreateKeychainStorageOptions = {}
): KeychainStorage {
  const { serviceName = DEFAULT_SERVICE_NAME, forceFileFallback = false } = options;

  if (forceFileFallback) {
    return new FileBasedStorage(serviceName);
  }

  const platform = process.platform;

  switch (platform) {
    case 'darwin':
      // Assume macOS Keychain is available
      return new MacOSKeychainStorage(serviceName);

    case 'win32':
      // Assume Windows Credential Manager is available
      return new WindowsCredentialStorage(serviceName);

    case 'linux':
      // Check for secret-tool synchronously
      try {
        execSync('which secret-tool', { timeout: COMMAND_TIMEOUT, stdio: 'pipe' });
        if (process.env.DBUS_SESSION_BUS_ADDRESS) {
          return new LinuxSecretStorage(serviceName);
        }
      } catch {
        /* Expected when secret-tool is not installed - fall through to file storage */
      }
      return new FileBasedStorage(serviceName);

    default:
      return new FileBasedStorage(serviceName);
  }
}

// ============================================================================
// High-Level Token Storage API
// ============================================================================

/**
 * Unified token storage that wraps platform-specific keychain implementations.
 */
export class UnifiedKeychainStorage {
  private storage: KeychainStorage | null = null;
  private readonly serviceName: string;
  private readonly fallbackToFile: boolean;
  private _isUsingFallback: boolean = false;

  constructor(options: KeychainOptions = {}) {
    this.serviceName = options.serviceName ?? DEFAULT_SERVICE_NAME;
    this.fallbackToFile = options.fallbackToFile ?? true;
  }

  /**
   * Check if using fallback file storage.
   */
  get isUsingFallback(): boolean {
    return this._isUsingFallback;
  }

  /**
   * Get the underlying storage type.
   */
  get storageType(): SecureStorageType {
    return this.storage?.storageType ?? 'file';
  }

  /**
   * Initialize the storage backend.
   */
  async initialize(): Promise<void> {
    if (this.storage) {
      return;
    }

    this.storage = await createKeychainStorage({
      serviceName: this.serviceName,
      forceFileFallback: !this.fallbackToFile,
      onFallback: () => {
        this._isUsingFallback = true;
      },
    });
  }

  /**
   * Ensure storage is initialized.
   */
  private async ensureInitialized(): Promise<KeychainStorage> {
    if (!this.storage) {
      await this.initialize();
    }
    return this.storage!;
  }

  /**
   * Get a token for the given profile.
   */
  async getToken(profile: string): Promise<string | null> {
    const storage = await this.ensureInitialized();
    return storage.getToken(profile);
  }

  /**
   * Set a token for the given profile.
   */
  async setToken(profile: string, token: string): Promise<void> {
    const storage = await this.ensureInitialized();
    await storage.setToken(profile, token);
  }

  /**
   * Delete a token for the given profile.
   */
  async deleteToken(profile: string): Promise<void> {
    const storage = await this.ensureInitialized();
    await storage.deleteToken(profile);
  }

  /**
   * Get token data for the given profile.
   */
  async getTokenData(profile: string): Promise<KeychainTokenData | null> {
    const storage = await this.ensureInitialized();
    return storage.getTokenData(profile);
  }

  /**
   * Set token data for the given profile.
   */
  async setTokenData(profile: string, data: KeychainTokenData): Promise<void> {
    const storage = await this.ensureInitialized();
    await storage.setTokenData(profile, data);
  }
}
