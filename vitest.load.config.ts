import { defineConfig, mergeConfig } from 'vitest/config';
import { baseConfig } from './vitest.base.config';

/**
 * Load tests for sustained throughput, memory leak detection, etc.
 * These tests may take longer and have different requirements than unit tests.
 */
export default mergeConfig(baseConfig, defineConfig({
  test: {
    include: ['tests/load/**/*.test.ts'],
    // Load tests may take longer
    testTimeout: 60000,
    // Run sequentially to get accurate memory measurements
    poolOptions: {
      forks: {
        maxForks: 1,
        minForks: 1,
      },
    },
    // Don't bail early on load tests
    bail: 0,
    // Reporters for CI integration
    reporters: ['default', 'junit'],
    outputFile: {
      junit: './test-results/load-junit.xml',
    },
  },
}));
