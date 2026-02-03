/**
 * Auth Middleware Device Flow Tests
 *
 * Tests for OAuth Device Flow (CLI authentication).
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mockAuthConfig, mockValidToken, mockRefreshToken } from './test-helpers.js';

describe('Auth Middleware - Device Flow (CLI)', () => {
  let DeviceFlowHandler: new (config: {
    clientId: string;
    deviceAuthEndpoint: string;
    tokenEndpoint: string;
  }) => {
    initiateAuth: () => Promise<{
      userCode: string;
      verificationUri: string;
      verificationUriComplete: string;
      deviceCode: string;
      expiresIn: number;
    }>;
    pollForToken: (
      deviceCode: string,
      options: { interval: number; timeout: number }
    ) => Promise<{ accessToken: string; refreshToken: string }>;
  };
  let mockFetch: ReturnType<typeof vi.fn>;

  beforeEach(async () => {
    vi.useFakeTimers();
    const module = await import('../../../src/auth/middleware.js');
    DeviceFlowHandler = module.DeviceFlowHandler;

    mockFetch = vi.fn();
    global.fetch = mockFetch;
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('should initiate device authorization flow', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        device_code: 'device_code_123',
        user_code: 'ABCD-EFGH',
        verification_uri: 'https://oauth.do/device',
        verification_uri_complete: 'https://oauth.do/device?code=ABCD-EFGH',
        expires_in: 1800,
        interval: 5,
      }),
    });

    const deviceFlow = new DeviceFlowHandler({
      clientId: mockAuthConfig.clientId,
      deviceAuthEndpoint: mockAuthConfig.deviceAuthEndpoint!,
      tokenEndpoint: mockAuthConfig.tokenEndpoint!,
    });

    const result = await deviceFlow.initiateAuth();

    expect(result.userCode).toBe('ABCD-EFGH');
    expect(result.verificationUri).toBe('https://oauth.do/device');
    expect(result.verificationUriComplete).toBe('https://oauth.do/device?code=ABCD-EFGH');
    expect(result.deviceCode).toBe('device_code_123');
    expect(result.expiresIn).toBe(1800);
  });

  it('should poll for token after user authorization', async () => {
    // First call returns pending
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 400,
      json: async () => ({ error: 'authorization_pending' }),
    });

    // Second call returns token
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        access_token: mockValidToken,
        refresh_token: mockRefreshToken,
        token_type: 'Bearer',
        expires_in: 3600,
      }),
    });

    const deviceFlow = new DeviceFlowHandler({
      clientId: mockAuthConfig.clientId,
      deviceAuthEndpoint: mockAuthConfig.deviceAuthEndpoint!,
      tokenEndpoint: mockAuthConfig.tokenEndpoint!,
    });

    const pollPromise = deviceFlow.pollForToken('device_code_123', {
      interval: 1,
      timeout: 5000,
    });

    // Advance timers to allow polling to complete
    await vi.advanceTimersByTimeAsync(2000);

    const result = await pollPromise;

    expect(result.accessToken).toBe(mockValidToken);
    expect(result.refreshToken).toBe(mockRefreshToken);
    expect(mockFetch).toHaveBeenCalledTimes(2);
  });

  it('should handle slow_down response by increasing interval', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 400,
      json: async () => ({ error: 'slow_down' }),
    });

    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        access_token: mockValidToken,
        refresh_token: mockRefreshToken,
        token_type: 'Bearer',
        expires_in: 3600,
      }),
    });

    const deviceFlow = new DeviceFlowHandler({
      clientId: mockAuthConfig.clientId,
      deviceAuthEndpoint: mockAuthConfig.deviceAuthEndpoint!,
      tokenEndpoint: mockAuthConfig.tokenEndpoint!,
    });

    const startTime = Date.now();
    const pollPromise = deviceFlow.pollForToken('device_code_123', { interval: 1, timeout: 10000 });

    // Advance timers to allow polling to complete with slow_down delay
    await vi.advanceTimersByTimeAsync(6000);

    await pollPromise;
    const elapsed = Date.now() - startTime;

    // Should have waited longer due to slow_down
    expect(elapsed).toBeGreaterThanOrEqual(5000);
  });

  it('should timeout if user does not authorize in time', async () => {
    mockFetch.mockResolvedValue({
      ok: false,
      status: 400,
      json: async () => ({ error: 'authorization_pending' }),
    });

    const deviceFlow = new DeviceFlowHandler({
      clientId: mockAuthConfig.clientId,
      deviceAuthEndpoint: mockAuthConfig.deviceAuthEndpoint!,
      tokenEndpoint: mockAuthConfig.tokenEndpoint!,
    });

    // Use a short timeout (100ms) but interval is 1 second (1000ms)
    // The first sleep will be 1000ms, so we need to advance enough for the sleep to complete
    // and then the timeout check will fail since elapsed time > 100ms
    const pollPromise = deviceFlow.pollForToken('device_code_123', { interval: 1, timeout: 100 });

    // Add a catch handler immediately to prevent unhandled rejection warning
    // The actual assertion will still work correctly
    pollPromise.catch(() => {});

    // Advance timers enough to complete at least one polling cycle (1000ms sleep)
    // After the sleep completes, Date.now() will show > 100ms elapsed, triggering timeout
    await vi.advanceTimersByTimeAsync(1100);

    await expect(pollPromise).rejects.toThrow('Device authorization timed out');
  });

  it('should handle access_denied response', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 400,
      json: async () => ({ error: 'access_denied' }),
    });

    const deviceFlow = new DeviceFlowHandler({
      clientId: mockAuthConfig.clientId,
      deviceAuthEndpoint: mockAuthConfig.deviceAuthEndpoint!,
      tokenEndpoint: mockAuthConfig.tokenEndpoint!,
    });

    // Use try/catch to avoid unhandled rejection warnings
    await expect(deviceFlow.pollForToken('device_code_123', { interval: 1, timeout: 5000 }))
      .rejects.toThrow('User denied authorization');
  });

  it('should handle expired_token response', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 400,
      json: async () => ({ error: 'expired_token' }),
    });

    const deviceFlow = new DeviceFlowHandler({
      clientId: mockAuthConfig.clientId,
      deviceAuthEndpoint: mockAuthConfig.deviceAuthEndpoint!,
      tokenEndpoint: mockAuthConfig.tokenEndpoint!,
    });

    // Use try/catch to avoid unhandled rejection warnings
    await expect(deviceFlow.pollForToken('device_code_123', { interval: 1, timeout: 5000 }))
      .rejects.toThrow('Device code expired');
  });
});
