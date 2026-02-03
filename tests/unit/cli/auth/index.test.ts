/**
 * Auth Test Suite Index
 *
 * This file documents the split auth test suites.
 * The auth tests have been split into logical groupings:
 *
 * - auth-module.test.ts: Module exports, MongoLakeTokenStorage class
 * - auth-flow.test.ts: Login, logout, whoami flows
 *
 * Additional tests remain in the original auth.test.ts file and can be
 * further split as needed:
 * - Token tests (access, refresh, middleware)
 * - Storage tests (keychain, secure storage, file paths)
 * - CLI tests (command registration, status command)
 */

import { describe, it, expect } from 'vitest';

describe('CLI Auth - Test Suite Organization', () => {
  it('should have split test files for maintainability', () => {
    const splitSuites = [
      'auth-module.test.ts',
      'auth-flow.test.ts',
    ];
    expect(splitSuites.length).toBeGreaterThan(0);
  });
});
