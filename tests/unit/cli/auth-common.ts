/**
 * Shared test utilities and mocks for auth tests
 */

import { vi } from 'vitest';

// Test fixtures
export const mockAuthResponse = {
  device_code: 'device_code_123',
  user_code: 'ABCD-EFGH',
  verification_uri: 'https://oauth.do/device',
  verification_uri_complete: 'https://oauth.do/device?code=ABCD-EFGH',
  expires_in: 1800,
  interval: 5,
};

export const mockTokenResponse = {
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

export const mockStoredAuth = {
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

export const mockExpiredAuth = {
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

// Setup function for auth tests
export function setupAuthMocks() {
  vi.mock('oauth.do', () => ({
    authorizeDevice: vi.fn(),
    pollForTokens: vi.fn(),
    getUser: vi.fn(),
    configure: vi.fn(),
  }));
}
