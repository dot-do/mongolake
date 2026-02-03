/**
 * OAuth Handler
 *
 * Implements OAuth 2.0 flows:
 * - Device Authorization Grant (RFC 8628) for CLI authentication
 * - Authorization Code flow with PKCE for web applications
 */

import type {
  DeviceAuthResponse,
  TokenExchangeResponse,
} from './types.js';

// ============================================================================
// Device Authorization Flow Handler (RFC 8628)
// ============================================================================

/**
 * Handles OAuth 2.0 Device Authorization Grant (RFC 8628) for CLI authentication.
 *
 * Flow:
 * 1. Call initiateAuth() to get user code and device code
 * 2. Display user code to user and direct them to verification URL
 * 3. Call pollForToken() to wait for user to authorize and retrieve tokens
 */
export class DeviceFlowHandler {
  private clientId: string;
  private deviceAuthEndpoint: string;
  private tokenEndpoint: string;

  constructor(config: { clientId: string; deviceAuthEndpoint: string; tokenEndpoint: string }) {
    this.clientId = config.clientId;
    this.deviceAuthEndpoint = config.deviceAuthEndpoint;
    this.tokenEndpoint = config.tokenEndpoint;
  }

  /**
   * Initiate device authorization flow - get user code and device code
   */
  async initiateAuth(): Promise<DeviceAuthResponse> {
    const response = await fetch(this.deviceAuthEndpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: new URLSearchParams({
        client_id: this.clientId,
      }),
    });

    if (!response.ok) {
      throw new Error('Failed to initiate device auth');
    }

    const data = await response.json() as {
      device_code: string;
      user_code: string;
      verification_uri: string;
      verification_uri_complete: string;
      expires_in: number;
    };

    return {
      deviceCode: data.device_code,
      userCode: data.user_code,
      verificationUri: data.verification_uri,
      verificationUriComplete: data.verification_uri_complete,
      expiresIn: data.expires_in,
    };
  }

  /**
   * Poll the token endpoint until user authorizes or timeout is reached
   * Handles RFC 8628 polling errors: authorization_pending, slow_down, access_denied
   */
  async pollForToken(
    deviceCode: string,
    options: { interval: number; timeout: number }
  ): Promise<{ accessToken: string; refreshToken: string }> {
    const startTime = Date.now();
    let currentInterval = options.interval;

    while (Date.now() - startTime < options.timeout) {
      const response = await fetch(this.tokenEndpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({
          grant_type: 'urn:ietf:params:oauth:grant-type:device_code',
          device_code: deviceCode,
          client_id: this.clientId,
        }),
      });

      if (response.ok) {
        const data = await response.json() as {
          access_token: string;
          refresh_token: string;
          token_type: string;
          expires_in: number;
        };
        return {
          accessToken: data.access_token,
          refreshToken: data.refresh_token,
        };
      }

      // Parse error response per RFC 8628
      const errorData = await response.json() as { error: string };

      if (errorData.error === 'authorization_pending') {
        // User hasn't authorized yet, wait and retry
        await this.sleep(currentInterval * 1000);
        continue;
      }

      if (errorData.error === 'slow_down') {
        // Server requested slower polling - increase interval by 5 seconds (RFC 8628)
        currentInterval += 5;
        await this.sleep(currentInterval * 1000);
        continue;
      }

      if (errorData.error === 'access_denied') {
        throw new Error('User denied authorization');
      }

      if (errorData.error === 'expired_token') {
        throw new Error('Device code expired');
      }

      // Unexpected error
      throw new Error(`Device authorization error: ${errorData.error}`);
    }

    throw new Error('Device authorization timed out');
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

// ============================================================================
// OAuth 2.0 Authorization Code Flow Handler
// ============================================================================

/**
 * Handles OAuth 2.0 Authorization Code flow for web applications.
 *
 * Flow:
 * 1. Call getAuthorizationUrl() to get the URL to redirect the user to
 * 2. User authorizes the application
 * 3. User is redirected back with authorization code
 * 4. Call exchangeCode() to exchange code for tokens
 */
export class AuthorizationCodeHandler {
  private clientId: string;
  private clientSecret?: string;
  private authorizationEndpoint: string;
  private tokenEndpoint: string;
  private redirectUri: string;
  private scope?: string;

  constructor(config: {
    clientId: string;
    clientSecret?: string;
    authorizationEndpoint: string;
    tokenEndpoint: string;
    redirectUri: string;
    scope?: string;
  }) {
    this.clientId = config.clientId;
    this.clientSecret = config.clientSecret;
    this.authorizationEndpoint = config.authorizationEndpoint;
    this.tokenEndpoint = config.tokenEndpoint;
    this.redirectUri = config.redirectUri;
    this.scope = config.scope;
  }

  /**
   * Generate a cryptographically secure state parameter
   */
  generateState(): string {
    const bytes = crypto.getRandomValues(new Uint8Array(32));
    return Array.from(bytes)
      .map((b) => b.toString(16).padStart(2, '0'))
      .join('');
  }

  /**
   * Generate PKCE code verifier
   */
  generateCodeVerifier(): string {
    const bytes = crypto.getRandomValues(new Uint8Array(32));
    return btoa(String.fromCharCode(...bytes))
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=/g, '');
  }

  /**
   * Generate PKCE code challenge from code verifier
   */
  async generateCodeChallenge(codeVerifier: string): Promise<string> {
    const encoder = new TextEncoder();
    const data = encoder.encode(codeVerifier);
    const digest = await crypto.subtle.digest('SHA-256', data);
    return btoa(String.fromCharCode(...new Uint8Array(digest)))
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=/g, '');
  }

  /**
   * Get the authorization URL to redirect the user to
   */
  async getAuthorizationUrl(options: {
    state?: string;
    codeVerifier?: string;
    scope?: string;
    additionalParams?: Record<string, string>;
  } = {}): Promise<{ url: string; state: string; codeVerifier?: string }> {
    const state = options.state ?? this.generateState();
    const codeVerifier = options.codeVerifier ?? this.generateCodeVerifier();
    const codeChallenge = await this.generateCodeChallenge(codeVerifier);

    const params = new URLSearchParams({
      response_type: 'code',
      client_id: this.clientId,
      redirect_uri: this.redirectUri,
      state,
      code_challenge: codeChallenge,
      code_challenge_method: 'S256',
    });

    if (options.scope ?? this.scope) {
      params.set('scope', options.scope ?? this.scope ?? '');
    }

    if (options.additionalParams) {
      for (const [key, value] of Object.entries(options.additionalParams)) {
        params.set(key, value);
      }
    }

    return {
      url: `${this.authorizationEndpoint}?${params.toString()}`,
      state,
      codeVerifier,
    };
  }

  /**
   * Exchange authorization code for tokens
   */
  async exchangeCode(
    code: string,
    codeVerifier: string
  ): Promise<TokenExchangeResponse> {
    const body = new URLSearchParams({
      grant_type: 'authorization_code',
      code,
      redirect_uri: this.redirectUri,
      client_id: this.clientId,
      code_verifier: codeVerifier,
    });

    if (this.clientSecret) {
      body.set('client_secret', this.clientSecret);
    }

    const response = await fetch(this.tokenEndpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body,
    });

    if (!response.ok) {
      const errorData = await response.json() as { error?: string; error_description?: string };
      throw new Error(errorData.error_description ?? errorData.error ?? 'Token exchange failed');
    }

    const data = await response.json() as {
      access_token: string;
      refresh_token?: string;
      expires_in: number;
      token_type: string;
      scope?: string;
    };

    return {
      accessToken: data.access_token,
      refreshToken: data.refresh_token,
      expiresIn: data.expires_in,
      tokenType: data.token_type,
      scope: data.scope,
    };
  }

  /**
   * Refresh tokens using a refresh token
   */
  async refreshTokens(refreshToken: string): Promise<TokenExchangeResponse> {
    const body = new URLSearchParams({
      grant_type: 'refresh_token',
      refresh_token: refreshToken,
      client_id: this.clientId,
    });

    if (this.clientSecret) {
      body.set('client_secret', this.clientSecret);
    }

    const response = await fetch(this.tokenEndpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body,
    });

    if (!response.ok) {
      const errorData = await response.json() as { error?: string; error_description?: string };
      throw new Error(errorData.error_description ?? errorData.error ?? 'Token refresh failed');
    }

    const data = await response.json() as {
      access_token: string;
      refresh_token?: string;
      expires_in: number;
      token_type: string;
      scope?: string;
    };

    return {
      accessToken: data.access_token,
      refreshToken: data.refresh_token,
      expiresIn: data.expires_in,
      tokenType: data.token_type,
      scope: data.scope,
    };
  }
}
