/**
 * MongoLake CLI Authentication Commands
 *
 * Provides authentication commands for the MongoLake CLI:
 * - login: Start OAuth device flow authentication using oauth.do
 * - logout: Clear stored credentials
 * - whoami: Show current user information
 * - status: Show authentication status
 * - refresh: Refresh access token
 *
 * Uses oauth.do package for device flow authentication and token management.
 * Tokens are stored in ~/.mongolake/auth.json for secure persistence.
 *
 * @module cli/auth
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import * as os from 'node:os';
import {
  authorizeDevice,
  pollForTokens,
  getUser,
  configure,
  type TokenStorage,
  type DeviceAuthorizationResponse,
  type TokenResponse,
} from 'oauth.do';
import {
  colors,
  printSuccess,
  printError,
  printWarning,
  printInfo,
  printDim,
} from './utils.js';

// ============================================================================
// Constants
// ============================================================================

/** OAuth client ID for MongoLake CLI */
export const OAUTH_CLIENT_ID = 'mongolake-cli';

/** OAuth provider for MongoLake CLI */
export const OAUTH_PROVIDER = 'oauth.do';

/** Default profile name */
const DEFAULT_PROFILE = 'default';

/** Device flow timeout (in milliseconds) - 5 minutes */
const DEVICE_FLOW_TIMEOUT = 5 * 60 * 1000;

/** Token refresh buffer (in milliseconds) - 5 minutes before expiry */
const TOKEN_REFRESH_BUFFER_MS = 5 * 60 * 1000;

/** Config directory name */
const CONFIG_DIR_NAME = '.mongolake';

/** Auth file name */
const AUTH_FILE_NAME = 'auth.json';

/** Directory permissions (owner read/write/execute only) */
const DIR_PERMISSIONS = 0o700;

/** File permissions (owner read/write only) */
const FILE_PERMISSIONS = 0o600;

// ============================================================================
// Types
// ============================================================================

/**
 * Stored token data including refresh token and expiration.
 * Compatible with oauth.do TokenStorage interface extensions.
 */
export interface StoredTokenData {
  /** The OAuth access token */
  accessToken: string;
  /** The refresh token for obtaining new access tokens */
  refreshToken?: string;
  /** Token expiration timestamp (ms since epoch) */
  expiresAt?: number;
}

/**
 * Options for authentication commands.
 */
export interface AuthOptions {
  /** Profile name to use (default: 'default') */
  profile?: string;
  /** Enable verbose output */
  verbose?: boolean;
}

/**
 * User information returned from authentication.
 */
export interface UserInfo {
  userId: string;
  email?: string;
  name?: string;
}

/**
 * Result of a login operation.
 */
export interface LoginResult {
  success: boolean;
  user?: UserInfo;
  error?: string;
}

/**
 * Result of a whoami operation.
 */
export interface WhoamiResult {
  authenticated: boolean;
  user?: UserInfo;
  profile: string;
  expiresAt?: Date;
  error?: string;
}

/**
 * Stored authentication data structure.
 */
export interface StoredAuth {
  profiles: Record<string, ProfileData>;
}

/**
 * User data stored in a profile.
 */
export interface ProfileUser {
  id: string;
  email?: string;
  name?: string;
}

/**
 * Profile data stored in the auth file.
 */
export interface ProfileData {
  accessToken: string;
  refreshToken?: string;
  expiresAt?: number;
  user?: ProfileUser;
  /** OAuth provider used for authentication */
  provider?: string;
}

// ============================================================================
// File System Utilities
// ============================================================================

/**
 * Get the path to the MongoLake config directory.
 */
function getConfigDir(): string {
  return path.join(os.homedir(), CONFIG_DIR_NAME);
}

/**
 * Get the path to the auth config file.
 */
function getAuthFilePath(): string {
  return path.join(getConfigDir(), AUTH_FILE_NAME);
}

/**
 * Ensure the config directory exists with proper permissions.
 */
function ensureConfigDir(): void {
  const configDir = getConfigDir();
  if (!fs.existsSync(configDir)) {
    fs.mkdirSync(configDir, { mode: DIR_PERMISSIONS, recursive: true });
  }
}

/**
 * Read stored auth data from disk.
 * Returns empty profiles object if file doesn't exist or is invalid.
 */
function readAuthData(): StoredAuth {
  try {
    const authPath = getAuthFilePath();
    if (fs.existsSync(authPath)) {
      const content = fs.readFileSync(authPath, 'utf8');
      return JSON.parse(content);
    }
  } catch {
    // Return empty data on read/parse errors
  }
  return { profiles: {} };
}

/**
 * Write auth data to disk with secure permissions.
 */
function writeAuthData(data: StoredAuth): void {
  ensureConfigDir();
  const authPath = getAuthFilePath();
  fs.writeFileSync(authPath, JSON.stringify(data, null, 2), {
    mode: FILE_PERMISSIONS,
  });
}

// ============================================================================
// Token Expiration Utilities
// ============================================================================

/**
 * Check if a token is expired.
 */
function isTokenExpired(expiresAt?: number): boolean {
  return expiresAt !== undefined && expiresAt < Date.now();
}

/**
 * Check if a token is near expiry (within buffer period).
 */
function isTokenNearExpiry(expiresAt?: number): boolean {
  return expiresAt !== undefined && expiresAt < Date.now() + TOKEN_REFRESH_BUFFER_MS;
}

/**
 * Calculate expiration timestamp from expires_in seconds.
 */
function calculateExpiresAt(expiresIn?: number): number {
  const defaultExpirySeconds = 3600; // 1 hour default
  return Date.now() + (expiresIn ?? defaultExpirySeconds) * 1000;
}

/**
 * Format remaining time until expiration.
 */
function formatTimeRemaining(expiresAt: number): string {
  const timeRemaining = expiresAt - Date.now();
  const hoursRemaining = Math.floor(timeRemaining / (1000 * 60 * 60));
  const minutesRemaining = Math.floor((timeRemaining % (1000 * 60 * 60)) / (1000 * 60));
  return `${hoursRemaining}h ${minutesRemaining}m`;
}

// ============================================================================
// File-Based Token Storage
// ============================================================================

/**
 * MongoLake CLI token storage implementation.
 * Implements the oauth.do TokenStorage interface for file-based persistence.
 */
export class MongoLakeTokenStorage implements TokenStorage {
  public readonly profile: string;

  constructor(profile: string = DEFAULT_PROFILE) {
    this.profile = profile;
  }

  /**
   * Get the access token if it exists and is not expired.
   */
  async getToken(): Promise<string | null> {
    const profileData = await this.getProfileData();
    if (!profileData) {
      return null;
    }

    if (isTokenExpired(profileData.expiresAt)) {
      return null;
    }

    return profileData.accessToken;
  }

  /**
   * Set the access token.
   */
  async setToken(token: string): Promise<void> {
    const data = readAuthData();
    if (!data.profiles[this.profile]) {
      data.profiles[this.profile] = { accessToken: token };
    } else {
      data.profiles[this.profile]!.accessToken = token;
    }
    writeAuthData(data);
  }

  /**
   * Remove the profile and its tokens.
   */
  async removeToken(): Promise<void> {
    const data = readAuthData();
    delete data.profiles[this.profile];
    writeAuthData(data);
  }

  /**
   * Get token data including refresh token and expiration.
   */
  async getTokenData(): Promise<StoredTokenData | null> {
    const profileData = await this.getProfileData();
    if (!profileData) {
      return null;
    }

    return {
      accessToken: profileData.accessToken,
      refreshToken: profileData.refreshToken,
      expiresAt: profileData.expiresAt,
    };
  }

  /**
   * Set token data including refresh token and expiration.
   */
  async setTokenData(tokenData: StoredTokenData): Promise<void> {
    const data = readAuthData();
    data.profiles[this.profile] = {
      accessToken: tokenData.accessToken,
      refreshToken: tokenData.refreshToken,
      expiresAt: tokenData.expiresAt,
    };
    writeAuthData(data);
  }

  /**
   * Get full profile data including user information.
   */
  async getProfileData(): Promise<ProfileData | null> {
    const data = readAuthData();
    return data.profiles[this.profile] ?? null;
  }

  /**
   * Set full profile data including user information.
   */
  async setProfileData(profileData: ProfileData): Promise<void> {
    const data = readAuthData();
    data.profiles[this.profile] = profileData;
    writeAuthData(data);
  }
}

// ============================================================================
// OAuth Device Flow
// ============================================================================

/**
 * Configure oauth.do with MongoLake settings.
 */
function configureOAuth(): void {
  configure({
    clientId: OAUTH_CLIENT_ID,
  });
}

/**
 * Display the device authorization prompt to the user.
 */
function displayDeviceAuthPrompt(authResponse: DeviceAuthorizationResponse): void {
  console.log(`${colors.bright}To authenticate, visit:${colors.reset}`);
  console.log(`\n  ${colors.blue}${authResponse.verification_uri}${colors.reset}\n`);
  console.log(`${colors.bright}Enter code:${colors.reset} ${colors.yellow}${authResponse.user_code}${colors.reset}\n`);

  if (authResponse.verification_uri_complete) {
    printDim('Or open this URL directly:');
    console.log(`  ${colors.dim}${authResponse.verification_uri_complete}${colors.reset}\n`);
  }

  const expiresInMinutes = Math.floor(authResponse.expires_in / 60);
  printDim(`Waiting for authorization (expires in ${expiresInMinutes} minutes)...`);
}

/**
 * Fetch user information from oauth.do.
 */
async function fetchUserInfo(accessToken: string): Promise<ProfileUser | undefined> {
  try {
    const authResult = await getUser(accessToken);
    if (authResult.user) {
      return {
        id: authResult.user.id,
        email: authResult.user.email,
        name: authResult.user.name,
      };
    }
  } catch {
    // Ignore errors fetching user info
  }
  return undefined;
}

/**
 * Convert oauth.do user to ProfileUser format.
 */
function toProfileUser(user: { id: string; email?: string; name?: string } | undefined): ProfileUser | undefined {
  if (!user) return undefined;
  return {
    id: user.id,
    email: user.email,
    name: user.name,
  };
}

/**
 * Convert ProfileUser to UserInfo format for external API.
 */
function toUserInfo(user: ProfileUser | undefined): UserInfo | undefined {
  if (!user) return undefined;
  return {
    userId: user.id,
    email: user.email,
    name: user.name,
  };
}

/**
 * Get display name for a user.
 */
function getUserDisplayName(user: ProfileUser): string {
  return user.email || user.name || user.id;
}

// ============================================================================
// Login Command
// ============================================================================

/**
 * Start the OAuth device flow login process using oauth.do.
 */
export async function login(options: AuthOptions = {}): Promise<LoginResult> {
  const profile = options.profile ?? DEFAULT_PROFILE;
  const verbose = options.verbose ?? false;

  printInfo('Starting authentication...\n');

  try {
    configureOAuth();

    if (verbose) {
      printDim('Initiating device authorization...');
    }

    const authResponse: DeviceAuthorizationResponse = await authorizeDevice();
    displayDeviceAuthPrompt(authResponse);

    // Poll for tokens using oauth.do
    const pollTimeout = Math.min(authResponse.expires_in, DEVICE_FLOW_TIMEOUT / 1000);
    const tokens: TokenResponse = await pollForTokens(
      authResponse.device_code,
      authResponse.interval || 5,
      pollTimeout
    );

    if (verbose) {
      printDim('Authorization successful, storing credentials...');
    }

    // Get user information
    const user = toProfileUser(tokens.user) ?? await fetchUserInfo(tokens.access_token);

    // Store credentials
    const storage = new MongoLakeTokenStorage(profile);
    await storage.setProfileData({
      accessToken: tokens.access_token,
      refreshToken: tokens.refresh_token,
      expiresAt: calculateExpiresAt(tokens.expires_in),
      user,
      provider: OAUTH_PROVIDER,
    });

    // Display success message
    console.log('');
    printSuccess('Successfully authenticated!');
    if (user) {
      printDim(`Logged in as: ${getUserDisplayName(user)}`);
    }
    if (profile !== DEFAULT_PROFILE) {
      printDim(`Profile: ${profile}`);
    }

    return {
      success: true,
      user: toUserInfo(user),
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Authentication failed';
    console.log('');
    printError(`Authentication failed: ${message}`);

    return {
      success: false,
      error: message,
    };
  }
}

// ============================================================================
// Logout Command
// ============================================================================

/**
 * Clear stored credentials for a profile.
 */
export async function logout(options: AuthOptions = {}): Promise<boolean> {
  const profile = options.profile ?? DEFAULT_PROFILE;

  try {
    const storage = new MongoLakeTokenStorage(profile);
    const profileData = await storage.getProfileData();

    if (!profileData) {
      printWarning(`No credentials found for profile '${profile}'`);
      return true;
    }

    await storage.removeToken();

    printSuccess('Successfully logged out');
    if (profile !== DEFAULT_PROFILE) {
      printDim(`Profile: ${profile}`);
    }

    return true;
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Logout failed';
    printError(`Logout failed: ${message}`);
    return false;
  }
}

// ============================================================================
// Whoami Command
// ============================================================================

/**
 * Display user information.
 */
function displayUserInfo(user: ProfileUser, profile: string, expiresAt?: number, verbose?: boolean): void {
  printSuccess('Authenticated\n');

  console.log(`${colors.bright}User ID:${colors.reset}  ${user.id}`);
  if (user.email) {
    console.log(`${colors.bright}Email:${colors.reset}    ${user.email}`);
  }
  if (user.name) {
    console.log(`${colors.bright}Name:${colors.reset}     ${user.name}`);
  }
  if (profile !== DEFAULT_PROFILE) {
    console.log(`${colors.bright}Profile:${colors.reset}  ${profile}`);
  }

  if (expiresAt) {
    if (verbose) {
      const expiresDate = new Date(expiresAt);
      console.log(`${colors.bright}Expires:${colors.reset}  ${expiresDate.toLocaleString()}`);
    }
    printDim(`Session valid for ${formatTimeRemaining(expiresAt)}`);
  }
}

/**
 * Display not logged in message.
 */
function displayNotLoggedIn(profile: string): void {
  printWarning('Not logged in');
  if (profile !== DEFAULT_PROFILE) {
    printDim(`Profile: ${profile}`);
  }
  printDim("\nRun 'mongolake auth login' to authenticate");
}

/**
 * Display session expired message.
 */
function displaySessionExpired(): void {
  printWarning('Session expired');
  printDim("\nRun 'mongolake auth login' to re-authenticate");
}

/**
 * Show information about the currently authenticated user.
 */
export async function whoami(options: AuthOptions = {}): Promise<WhoamiResult> {
  const profile = options.profile ?? DEFAULT_PROFILE;
  const verbose = options.verbose ?? false;

  try {
    const storage = new MongoLakeTokenStorage(profile);
    const profileData = await storage.getProfileData();

    if (!profileData) {
      displayNotLoggedIn(profile);
      return {
        authenticated: false,
        profile,
        error: 'Not logged in',
      };
    }

    if (isTokenExpired(profileData.expiresAt)) {
      displaySessionExpired();
      return {
        authenticated: false,
        profile,
        error: 'Session expired',
      };
    }

    // Try to fetch fresh user info if not stored
    let user = profileData.user;
    if (!user && profileData.accessToken) {
      user = await fetchUserInfo(profileData.accessToken);
      if (user) {
        await storage.setProfileData({ ...profileData, user });
      }
    }

    if (user) {
      displayUserInfo(user, profile, profileData.expiresAt, verbose);
    } else {
      printSuccess('Authenticated\n');
      if (profile !== DEFAULT_PROFILE) {
        console.log(`${colors.bright}Profile:${colors.reset}  ${profile}`);
      }
    }

    return {
      authenticated: true,
      user: toUserInfo(user),
      profile,
      expiresAt: profileData.expiresAt ? new Date(profileData.expiresAt) : undefined,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Failed to get user info';
    printError(`Error: ${message}`);

    return {
      authenticated: false,
      profile,
      error: message,
    };
  }
}

// ============================================================================
// Token Access (for other commands that need authentication)
// ============================================================================

/**
 * Create a storage instance for the given profile.
 */
function createStorage(profile?: string): MongoLakeTokenStorage {
  return new MongoLakeTokenStorage(profile ?? DEFAULT_PROFILE);
}

/**
 * Get the access token for API requests.
 * Returns null if not authenticated or token is expired.
 */
export async function getAccessToken(profile?: string): Promise<string | null> {
  return createStorage(profile).getToken();
}

/**
 * Check if the user is authenticated with a valid token.
 */
export async function isAuthenticated(profile?: string): Promise<boolean> {
  const token = await getAccessToken(profile);
  return token !== null;
}

/**
 * Get the stored profile data.
 */
export async function getProfileData(profile?: string): Promise<ProfileData | null> {
  return createStorage(profile).getProfileData();
}

/**
 * Refresh the access token if it's near expiry.
 * Returns the access token if valid, null if refresh is needed but unavailable.
 * This is a convenience wrapper around getAccessTokenWithAutoRefresh.
 */
export async function refreshTokenIfNeeded(profile?: string): Promise<string | null> {
  return getAccessTokenWithAutoRefresh(profile);
}

// ============================================================================
// Auth Middleware for CLI Commands
// ============================================================================

/**
 * Ensure the user is authenticated before running a command.
 * Returns the access token or throws an error if not authenticated.
 * Automatically refreshes the token if it's near expiry.
 */
export async function requireAuth(options: AuthOptions = {}): Promise<string> {
  const profile = options.profile ?? DEFAULT_PROFILE;

  // Try to get token with automatic refresh
  const token = await getAccessTokenWithAutoRefresh(profile);

  if (token) {
    return token;
  }

  // No valid token, prompt for login
  printWarning('Authentication required');
  printDim("Run 'mongolake auth login' to authenticate\n");
  throw new Error('Not authenticated');
}

/**
 * Create an auth provider function for HTTP clients.
 * Compatible with oauth.do/rpc.do authentication interfaces.
 * Returns a function that resolves to a token string or null.
 * Automatically refreshes the token if it's near expiry.
 */
export function createAuthProvider(profile?: string): () => Promise<string | null> {
  return () => getAccessTokenWithAutoRefresh(profile);
}

/**
 * Create an auth provider that throws on authentication failure.
 * Useful for commands that require authentication.
 */
export function createRequiredAuthProvider(profile?: string): () => Promise<string> {
  return async () => {
    const token = await getAccessTokenWithAutoRefresh(profile);
    if (!token) {
      throw new Error('Not authenticated');
    }
    return token;
  };
}

// ============================================================================
// Token Refresh
// ============================================================================

/** Default OAuth token endpoint for oauth.do */
const DEFAULT_TOKEN_ENDPOINT = 'https://oauth.do/oauth/token';

/** Maximum number of refresh retries */
const MAX_REFRESH_RETRIES = 3;

/** Initial retry delay in milliseconds */
const INITIAL_RETRY_DELAY_MS = 1000;

/** Maximum retry delay in milliseconds */
const MAX_RETRY_DELAY_MS = 10000;

/**
 * Options for token refresh operation.
 */
export interface RefreshTokenOptions {
  profile?: string;
  /** Maximum number of retry attempts (default: 3) */
  maxRetries?: number;
  /** Whether to update stored tokens on success (default: true) */
  updateStorage?: boolean;
  /** Custom token endpoint URL */
  tokenEndpoint?: string;
}

/**
 * Result of a token refresh operation.
 */
export interface RefreshTokenResult {
  success: boolean;
  accessToken?: string;
  refreshToken?: string;
  expiresAt?: number;
  error?: string;
  /** Number of retry attempts made */
  retryCount?: number;
}

/**
 * Token refresh response from OAuth server.
 */
interface TokenRefreshResponse {
  access_token: string;
  refresh_token?: string;
  expires_in: number;
  token_type: string;
  scope?: string;
}

/**
 * OAuth error response.
 */
interface OAuthErrorResponse {
  error: string;
  error_description?: string;
}

/**
 * Check if an error is retryable (network errors, 5xx errors).
 */
function isRetryableError(error: unknown): boolean {
  if (error instanceof Error) {
    const message = error.message.toLowerCase();
    // Network errors
    if (message.includes('network') || message.includes('fetch') || message.includes('econnrefused')) {
      return true;
    }
    // Server errors (5xx)
    if (message.includes('500') || message.includes('502') || message.includes('503') || message.includes('504')) {
      return true;
    }
    // Rate limiting
    if (message.includes('429') || message.includes('rate limit')) {
      return true;
    }
  }
  return false;
}

/**
 * Calculate retry delay with exponential backoff and jitter.
 */
function calculateRetryDelay(attempt: number): number {
  const baseDelay = INITIAL_RETRY_DELAY_MS * Math.pow(2, attempt);
  const cappedDelay = Math.min(baseDelay, MAX_RETRY_DELAY_MS);
  // Add jitter (0-25% of the delay)
  const jitter = cappedDelay * Math.random() * 0.25;
  return cappedDelay + jitter;
}

/**
 * Sleep for a specified duration.
 */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Perform the actual token refresh request to the OAuth server.
 */
async function performTokenRefresh(
  refreshTokenValue: string,
  tokenEndpoint: string = DEFAULT_TOKEN_ENDPOINT
): Promise<TokenRefreshResponse> {
  const body = new URLSearchParams({
    grant_type: 'refresh_token',
    refresh_token: refreshTokenValue,
    client_id: OAUTH_CLIENT_ID,
  });

  const response = await fetch(tokenEndpoint, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      Accept: 'application/json',
    },
    body: body.toString(),
  });

  if (!response.ok) {
    let errorMessage = `Token refresh failed with status ${response.status}`;
    try {
      const errorData = (await response.json()) as OAuthErrorResponse;
      if (errorData.error_description) {
        errorMessage = errorData.error_description;
      } else if (errorData.error) {
        errorMessage = `OAuth error: ${errorData.error}`;
      }
    } catch {
      // Use default error message
    }
    throw new Error(errorMessage);
  }

  const data = (await response.json()) as TokenRefreshResponse;

  if (!data.access_token) {
    throw new Error('Invalid token response: missing access_token');
  }

  return data;
}

/**
 * Refresh the access token using the stored refresh token.
 * Implements retry logic with exponential backoff for transient failures.
 */
export async function refreshToken(options: RefreshTokenOptions = {}): Promise<RefreshTokenResult> {
  const storage = createStorage(options.profile);
  const profileData = await storage.getProfileData();
  const maxRetries = options.maxRetries ?? MAX_REFRESH_RETRIES;
  const updateStorage = options.updateStorage ?? true;
  const tokenEndpoint = options.tokenEndpoint ?? DEFAULT_TOKEN_ENDPOINT;

  if (!profileData) {
    return {
      success: false,
      error: 'No profile data found',
    };
  }

  if (!profileData.refreshToken) {
    return {
      success: false,
      error: 'No refresh token available',
    };
  }

  let lastError: Error | undefined;
  let retryCount = 0;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const tokenResponse = await performTokenRefresh(profileData.refreshToken, tokenEndpoint);

      const newExpiresAt = calculateExpiresAt(tokenResponse.expires_in);
      const newRefreshToken = tokenResponse.refresh_token ?? profileData.refreshToken;

      // Update stored tokens if requested
      if (updateStorage) {
        await storage.setProfileData({
          ...profileData,
          accessToken: tokenResponse.access_token,
          refreshToken: newRefreshToken,
          expiresAt: newExpiresAt,
        });
      }

      return {
        success: true,
        accessToken: tokenResponse.access_token,
        refreshToken: newRefreshToken,
        expiresAt: newExpiresAt,
        retryCount,
      };
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      retryCount = attempt;

      // Check if error is retryable
      if (!isRetryableError(error) || attempt >= maxRetries) {
        break;
      }

      // Wait before retrying
      const delay = calculateRetryDelay(attempt);
      await sleep(delay);
    }
  }

  // Determine if we should suggest re-login
  const errorMessage = lastError?.message ?? 'Token refresh failed';
  const isAuthError =
    errorMessage.includes('invalid_grant') ||
    errorMessage.includes('expired') ||
    errorMessage.includes('revoked') ||
    errorMessage.includes('invalid_token');

  return {
    success: false,
    error: isAuthError ? `${errorMessage} - please login again` : errorMessage,
    retryCount,
  };
}

/**
 * Get access token with automatic refresh if near expiry.
 * Performs silent token refresh when the token is within the refresh buffer period.
 */
export async function getAccessTokenWithAutoRefresh(profile?: string): Promise<string | null> {
  const storage = createStorage(profile);
  const profileData = await storage.getProfileData();

  if (!profileData) {
    return null;
  }

  // Return token if still valid and not near expiry
  if (!isTokenNearExpiry(profileData.expiresAt)) {
    return profileData.accessToken;
  }

  // Try to refresh if we have a refresh token
  if (profileData.refreshToken) {
    const result = await refreshToken({ profile });
    if (result.success && result.accessToken) {
      return result.accessToken;
    }
    // Log refresh failure for debugging (but don't throw)
    if (result.error) {
      // Silent failure - will fall through to use existing token if valid
    }
  }

  // Return current token if not expired yet (even if near expiry)
  // This provides graceful degradation when refresh fails
  if (!isTokenExpired(profileData.expiresAt)) {
    return profileData.accessToken;
  }

  return null;
}

/**
 * Force refresh the token regardless of expiry status.
 * Useful for testing or when the current token is known to be invalid.
 */
export async function forceTokenRefresh(options: RefreshTokenOptions = {}): Promise<RefreshTokenResult> {
  return refreshToken(options);
}

// ============================================================================
// Keychain Token Storage (Platform Secure Storage)
// ============================================================================

import {
  createKeychainStorageSync,
  type KeychainStorage as NativeKeychainStorage,
  type KeychainTokenData,
  type SecureStorageType,
  DEFAULT_SERVICE_NAME,
} from './keychain.js';

/**
 * Options for KeychainTokenStorage.
 */
export interface KeychainTokenStorageOptions {
  /** Fall back to file storage if keychain is unavailable */
  fallbackToFile?: boolean;
}

/**
 * Secure token storage using platform-specific secure storage.
 * Uses native keychain on macOS, Windows Credential Manager on Windows,
 * and Secret Service on Linux. Falls back to file-based storage when unavailable.
 */
export class KeychainTokenStorage implements TokenStorage {
  public readonly serviceName: string = DEFAULT_SERVICE_NAME;
  public readonly profile: string;
  private _isUsingFallback: boolean = false;

  private readonly fallbackToFile: boolean;
  private readonly fileStorage: MongoLakeTokenStorage;
  private readonly nativeStorage: NativeKeychainStorage;
  private cachedToken: string | null = null;

  constructor(profile: string = DEFAULT_PROFILE, options: KeychainTokenStorageOptions = {}) {
    this.profile = profile;
    this.fallbackToFile = options.fallbackToFile ?? true;
    this.fileStorage = new MongoLakeTokenStorage(profile);
    this.nativeStorage = createKeychainStorageSync({
      serviceName: DEFAULT_SERVICE_NAME,
      forceFileFallback: !this.fallbackToFile,
    });
    this._isUsingFallback = this.nativeStorage.storageType === 'file';
  }

  /**
   * Check if using fallback file storage.
   */
  get isUsingFallback(): boolean {
    return this._isUsingFallback;
  }

  /**
   * Get the storage type being used.
   */
  get storageType(): SecureStorageType {
    return this.nativeStorage.storageType;
  }

  async getToken(): Promise<string | null> {
    // Try native keychain first
    try {
      const token = await this.nativeStorage.getToken(this.profile);
      if (token) {
        this.cachedToken = token;
        return token;
      }
    } catch {
      // Fall through to file fallback
    }

    // Fall back to file storage if enabled
    if (this.fallbackToFile) {
      const token = await this.fileStorage.getToken();
      if (token) {
        this.cachedToken = token;
      }
      return token;
    }

    return this.cachedToken;
  }

  async setToken(token: string): Promise<void> {
    this.cachedToken = token;

    // Try native keychain first
    try {
      await this.nativeStorage.setToken(this.profile, token);
    } catch {
      this._isUsingFallback = true;
    }

    // Also store in file for compatibility if fallback is enabled
    if (this.fallbackToFile) {
      await this.fileStorage.setToken(token);
    }
  }

  async removeToken(): Promise<void> {
    this.cachedToken = null;

    // Remove from native keychain
    try {
      await this.nativeStorage.deleteToken(this.profile);
    } catch {
      // Ignore errors when deleting
    }

    // Also remove from file storage
    if (this.fallbackToFile) {
      await this.fileStorage.removeToken();
    }
  }

  /**
   * Get token data including refresh token and metadata.
   */
  async getTokenData(): Promise<StoredTokenData | null> {
    // Try native keychain first
    try {
      const data = await this.nativeStorage.getTokenData(this.profile);
      if (data) {
        return {
          accessToken: data.accessToken,
          refreshToken: data.refreshToken,
          expiresAt: data.expiresAt,
        };
      }
    } catch {
      // Fall through to file fallback
    }

    // Fall back to file storage
    if (this.fallbackToFile) {
      return this.fileStorage.getTokenData();
    }

    return null;
  }

  /**
   * Set token data including refresh token and metadata.
   */
  async setTokenData(tokenData: StoredTokenData): Promise<void> {
    this.cachedToken = tokenData.accessToken;

    // Try native keychain first
    try {
      const keychainData: KeychainTokenData = {
        accessToken: tokenData.accessToken,
        refreshToken: tokenData.refreshToken,
        expiresAt: tokenData.expiresAt,
      };
      await this.nativeStorage.setTokenData(this.profile, keychainData);
    } catch {
      this._isUsingFallback = true;
    }

    // Also store in file for compatibility
    if (this.fallbackToFile) {
      await this.fileStorage.setTokenData(tokenData);
    }
  }

  /**
   * Get the raw stored value (for testing).
   */
  getRawStoredValue(): string | null {
    return this.cachedToken;
  }
}

// ============================================================================
// Status Command
// ============================================================================

/**
 * Options for the status command.
 */
export interface StatusOptions {
  profile?: string;
  showAllProfiles?: boolean;
}

/**
 * Result of the status command.
 */
export interface StatusResult {
  authenticated: boolean;
  profile?: string;
  provider?: string;
  expiresAt?: Date;
  expiresIn?: number;
  expired?: boolean;
  profiles?: string[];
  storageBackend: 'keychain' | 'file';
}

/**
 * Display all available profiles with their status.
 */
function displayAllProfiles(authData: StoredAuth): StatusResult {
  const profiles = Object.keys(authData.profiles);

  if (profiles.length === 0) {
    printWarning('Not authenticated');
    printDim("\nRun 'mongolake auth login' to authenticate");

    return {
      authenticated: false,
      profiles: [],
      storageBackend: 'file',
    };
  }

  console.log(`${colors.bright}Available profiles:${colors.reset}`);
  for (const p of profiles) {
    const pData = authData.profiles[p]!;
    const expired = isTokenExpired(pData.expiresAt);
    const statusIcon = expired
      ? `${colors.red}(expired)${colors.reset}`
      : `${colors.green}(active)${colors.reset}`;
    console.log(`  ${p} ${statusIcon}`);
  }

  return {
    authenticated: true,
    profiles,
    storageBackend: 'file',
  };
}

/**
 * Display status for a single profile.
 */
function displayProfileStatus(profile: string, profileData: ProfileData): StatusResult {
  const now = Date.now();

  if (isTokenExpired(profileData.expiresAt)) {
    printWarning('Session expired');
    printDim("\nRun 'mongolake auth login' to re-authenticate");

    return {
      authenticated: false,
      profile,
      expired: true,
      storageBackend: 'file',
    };
  }

  printSuccess('Authenticated\n');
  console.log(`${colors.bright}Profile:${colors.reset}  ${profile}`);
  console.log(`${colors.bright}Provider:${colors.reset} ${profileData.provider ?? OAUTH_PROVIDER}`);

  if (profileData.expiresAt) {
    const expiresAt = new Date(profileData.expiresAt);
    const expiresIn = profileData.expiresAt - now;
    printDim(`Session expires at ${expiresAt.toLocaleString()}`);

    return {
      authenticated: true,
      profile,
      provider: profileData.provider ?? OAUTH_PROVIDER,
      expiresAt,
      expiresIn,
      storageBackend: 'file',
    };
  }

  return {
    authenticated: true,
    profile,
    provider: profileData.provider ?? OAUTH_PROVIDER,
    storageBackend: 'file',
  };
}

/**
 * Show authentication status.
 */
export async function status(options: StatusOptions = {}): Promise<StatusResult> {
  const profile = options.profile ?? DEFAULT_PROFILE;

  // Show all profiles if requested
  if (options.showAllProfiles) {
    const authData = readAuthData();
    return displayAllProfiles(authData);
  }

  // Show single profile status
  const storage = createStorage(profile);
  const profileData = await storage.getProfileData();

  if (!profileData) {
    printWarning('Not authenticated');
    printDim("\nRun 'mongolake auth login' to authenticate");

    return {
      authenticated: false,
      profile,
      storageBackend: 'file',
    };
  }

  return displayProfileStatus(profile, profileData);
}

// ============================================================================
// Secure Storage Detection
// ============================================================================

import { detectSecureStorage as detectNativeSecureStorage } from './keychain.js';

// Re-export SecureStorageType from keychain module
export type { SecureStorageType } from './keychain.js';

/**
 * Result of secure storage detection.
 */
export interface SecureStorageDetection {
  /** Whether native secure storage is available */
  available: boolean;
  /** Type of storage detected */
  type: SecureStorageType;
  /** Whether file-based fallback is available */
  fallbackAvailable: boolean;
}

/**
 * Detect platform-specific secure storage availability.
 * Delegates to the keychain module for actual detection.
 */
export async function detectSecureStorage(): Promise<SecureStorageDetection> {
  const result = await detectNativeSecureStorage();
  return {
    available: result.available,
    type: result.type,
    fallbackAvailable: result.fallbackAvailable,
  };
}

// ============================================================================
// OAuth Response Validation
// ============================================================================

/** Required fields for a valid device authorization response */
const REQUIRED_AUTH_RESPONSE_FIELDS = ['device_code', 'user_code', 'verification_uri'] as const;

/**
 * Validate an OAuth device authorization response.
 * Checks for required fields per RFC 8628.
 */
export function validateAuthResponse(response: unknown): boolean {
  if (!response || typeof response !== 'object') {
    return false;
  }

  const resp = response as Record<string, unknown>;

  return REQUIRED_AUTH_RESPONSE_FIELDS.every((field) => field in resp);
}

/**
 * Custom OAuth endpoint configuration (for testing).
 *
 * Note: oauth.do (v0.2.x) does not support individual endpoint configuration.
 * Instead, use `baseUrl` to redirect all OAuth traffic to a mock server,
 * which must implement the standard oauth.do API endpoints:
 * - POST /device/code (device authorization)
 * - POST /oauth/token (token exchange)
 * - GET /me (user info)
 *
 * @see https://github.com/dot-do/oauth.do for oauth.do documentation
 */
export interface OAuthEndpointsConfig {
  /**
   * Base URL for all OAuth endpoints.
   * The mock server should implement oauth.do-compatible endpoints.
   * @example 'http://localhost:9000'
   */
  baseUrl?: string;
  /**
   * Custom authorization domain (for AuthKit login page).
   * @example 'localhost:9000'
   */
  authDomain?: string;
  /**
   * @deprecated Individual endpoints not supported by oauth.do.
   * Use baseUrl instead to redirect to a mock server.
   */
  authorizationEndpoint?: string;
  /**
   * @deprecated Individual endpoints not supported by oauth.do.
   * Use baseUrl instead to redirect to a mock server.
   */
  tokenEndpoint?: string;
  /**
   * @deprecated Individual endpoints not supported by oauth.do.
   * Use baseUrl instead to redirect to a mock server.
   */
  userInfoEndpoint?: string;
}

/**
 * Configure custom OAuth endpoints for testing.
 *
 * This configures oauth.do to use custom base URLs for OAuth operations,
 * enabling testing with mock OAuth servers.
 *
 * @param endpoints - Custom endpoint configuration
 *
 * @example
 * ```ts
 * // Redirect all OAuth traffic to a local mock server
 * configureOAuthEndpoints({
 *   baseUrl: 'http://localhost:9000',
 *   authDomain: 'localhost:9000',
 * });
 * ```
 *
 * @remarks
 * oauth.do does not support configuring individual endpoints (authorization,
 * token, userInfo). Instead, use baseUrl to redirect all traffic to a mock
 * server that implements the full oauth.do API.
 *
 * For feature request to support individual endpoints, see:
 * https://github.com/dot-do/oauth.do/issues
 */
export function configureOAuthEndpoints(endpoints: OAuthEndpointsConfig): void {
  // Warn about deprecated individual endpoint options
  if (endpoints.authorizationEndpoint || endpoints.tokenEndpoint || endpoints.userInfoEndpoint) {
    console.warn(
      '[mongolake] Individual OAuth endpoint configuration is not supported by oauth.do. ' +
      'Use baseUrl to redirect to a mock server instead.'
    );
  }

  // Configure oauth.do with available options
  if (endpoints.baseUrl || endpoints.authDomain) {
    configure({
      clientId: OAUTH_CLIENT_ID,
      ...(endpoints.baseUrl && { apiUrl: endpoints.baseUrl }),
      ...(endpoints.authDomain && { authKitDomain: endpoints.authDomain }),
    });
  }
}

// ============================================================================
// CLI Command Registration
// ============================================================================

import type { CLI, Command } from './framework.js';

/** Default profile option configuration */
const PROFILE_OPTION = {
  flags: '--profile <profile>',
  description: 'Auth profile to use',
  config: { default: 'default' },
} as const;

/** Verbose option configuration */
const VERBOSE_OPTION = {
  flags: '-v, --verbose',
  description: 'Show verbose output',
} as const;

/**
 * Register all auth subcommands with the CLI.
 */
export function registerAuthCommand(cli: CLI): Command {
  const authCmd = cli.command('auth', 'Manage authentication')
    .option(PROFILE_OPTION.flags, PROFILE_OPTION.description, PROFILE_OPTION.config);

  // Login subcommand
  authCmd.subcommand('login', 'Authenticate with MongoLake')
    .option(PROFILE_OPTION.flags, PROFILE_OPTION.description, PROFILE_OPTION.config)
    .option(VERBOSE_OPTION.flags, VERBOSE_OPTION.description)
    .action(async (options) => {
      await login({
        profile: options.profile as string,
        verbose: options.verbose as boolean,
      });
    });

  // Logout subcommand
  authCmd.subcommand('logout', 'Clear stored credentials')
    .option(PROFILE_OPTION.flags, PROFILE_OPTION.description, PROFILE_OPTION.config)
    .action(async (options) => {
      await logout({ profile: options.profile as string });
    });

  // Status subcommand
  authCmd.subcommand('status', 'Show authentication status')
    .option(PROFILE_OPTION.flags, PROFILE_OPTION.description, PROFILE_OPTION.config)
    .option('--all', 'Show all profiles')
    .action(async (options) => {
      await status({
        profile: options.profile as string,
        showAllProfiles: options.all as boolean,
      });
    });

  // Whoami subcommand
  authCmd.subcommand('whoami', 'Show current user information')
    .option(PROFILE_OPTION.flags, PROFILE_OPTION.description, PROFILE_OPTION.config)
    .option(VERBOSE_OPTION.flags, VERBOSE_OPTION.description)
    .action(async (options) => {
      await whoami({
        profile: options.profile as string,
        verbose: options.verbose as boolean,
      });
    });

  // Refresh subcommand
  authCmd.subcommand('refresh', 'Refresh access token')
    .option(PROFILE_OPTION.flags, PROFILE_OPTION.description, PROFILE_OPTION.config)
    .option(VERBOSE_OPTION.flags, VERBOSE_OPTION.description)
    .option('--force', 'Force refresh even if token is not near expiry')
    .action(async (options) => {
      const profile = options.profile as string;
      const verbose = options.verbose as boolean;
      const force = options.force as boolean;

      // Check if refresh is needed (unless forced)
      if (!force) {
        const storage = createStorage(profile);
        const profileData = await storage.getProfileData();

        if (!profileData) {
          printWarning('Not authenticated');
          printDim("\nRun 'mongolake auth login' to authenticate");
          return;
        }

        if (!isTokenNearExpiry(profileData.expiresAt)) {
          printInfo('Token is still valid');
          if (verbose && profileData.expiresAt) {
            printDim(`Expires in ${formatTimeRemaining(profileData.expiresAt)}`);
          }
          return;
        }
      }

      if (verbose) {
        printDim('Refreshing access token...');
      }

      const result = await refreshToken({ profile });

      if (result.success) {
        printSuccess('Token refreshed successfully');
        if (verbose && result.expiresAt) {
          printDim(`New token expires in ${formatTimeRemaining(result.expiresAt)}`);
          if (result.retryCount && result.retryCount > 0) {
            printDim(`Succeeded after ${result.retryCount} retry attempt(s)`);
          }
        }
      } else {
        printError(`Token refresh failed: ${result.error}`);
        if (result.retryCount && result.retryCount > 0) {
          printDim(`Failed after ${result.retryCount} retry attempt(s)`);
        }
        if (result.error?.includes('please login again')) {
          printDim("\nRun 'mongolake auth login' to re-authenticate");
        }
      }
    });

  return authCmd;
}
