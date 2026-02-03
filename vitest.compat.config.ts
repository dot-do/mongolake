import { defineConfig, mergeConfig } from 'vitest/config';
import { baseConfig } from './vitest.base.config';

/**
 * Compatibility tests verify MongoDB API compatibility.
 *
 * These tests run against the MongoLake wire protocol server using
 * the official MongoDB driver to validate compatibility.
 *
 * Usage:
 *   npx vitest run --config vitest.compat.config.ts
 */
export default mergeConfig(baseConfig, defineConfig({
  test: {
    include: ['tests/compat/**/*.test.ts'],
    // Longer timeout for aggregation tests
    testTimeout: 60000,
    // Reporters for CI integration
    reporters: ['default', 'junit'],
    outputFile: {
      junit: './test-results/compat-junit.xml',
    },
  },
}));
