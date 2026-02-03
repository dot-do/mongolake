/**
 * Auth Provider Adapters
 *
 * Adapters for different authentication providers.
 * Each provider adapter implements the AuthProvider interface.
 */

import type { AuthProvider, UserContext } from '../types.js';

// ============================================================================
// Base Provider Factory
// ============================================================================

/**
 * Create a custom auth provider
 */
export function createAuthProvider(config: {
  name: string;
  issuer: string;
  validateToken: (token: string) => Promise<{ valid: boolean; user?: UserContext }>;
}): AuthProvider {
  return {
    name: config.name,
    issuer: config.issuer,
    validateToken: config.validateToken,
  };
}

// ============================================================================
// Provider Registration
// ============================================================================

const registeredProviders: Map<string, AuthProvider> = new Map();

/**
 * Register an auth provider for use in the middleware
 */
export function registerProvider(provider: AuthProvider): void {
  registeredProviders.set(provider.name, provider);
}

/**
 * Get a registered provider by name
 */
export function getProvider(name: string): AuthProvider | undefined {
  return registeredProviders.get(name);
}

/**
 * Get all registered providers
 */
export function getAllProviders(): AuthProvider[] {
  return Array.from(registeredProviders.values());
}

/**
 * Clear all registered providers (useful for testing)
 */
export function clearProviders(): void {
  registeredProviders.clear();
}

// Re-export types
export type { AuthProvider, UserContext };
