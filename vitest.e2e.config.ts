import { defineConfig, mergeConfig } from 'vitest/config';
import { baseConfig } from './vitest.base.config';

/**
 * E2E tests run against deployed workers.
 *
 * Set MONGOLAKE_E2E_URL environment variable to the deployed worker URL.
 *
 * Usage:
 *   MONGOLAKE_E2E_URL=https://mongolake.workers.dev npx vitest run --config vitest.e2e.config.ts
 */
export default mergeConfig(baseConfig, defineConfig({
  test: {
    include: ['tests/e2e/**/*.test.ts'],
    // Reporters for CI integration
    reporters: ['default', 'junit'],
    outputFile: {
      junit: './test-results/e2e-junit.xml',
    },
  },
}));
