import { defineConfig, mergeConfig } from 'vitest/config';
import { baseConfig } from './vitest.base.config';

/**
 * Scale tests for 100k+ document operations.
 *
 * These tests are resource-intensive and verify:
 * - Large dataset operations (100k+ documents)
 * - Multi-shard distribution and rebalancing
 * - Concurrent operations at scale
 *
 * Configuration:
 * - Extended timeouts (5+ minutes per test)
 * - Single fork for memory consistency
 * - Separate from regular test runs
 */
export default mergeConfig(baseConfig, defineConfig({
  test: {
    include: ['tests/scale/**/*.test.ts'],

    // Extended timeouts for scale tests
    // Individual tests may take several minutes with 100k+ documents
    testTimeout: 300000, // 5 minutes per test
    hookTimeout: 120000, // 2 minutes for setup/teardown

    // Single fork for memory consistency
    // Scale tests need accurate memory measurements and shared state
    pool: 'forks',
    poolOptions: {
      forks: {
        maxForks: 1,
        minForks: 1,
      },
    },

    // Sequential execution for consistent resource usage
    sequence: {
      shuffle: false,
    },

    // Don't bail early - we want all scale test results
    bail: 0,

    // Disable retries - scale tests should be deterministic
    retry: 0,

    // Reporters for CI integration
    reporters: ['default', 'junit'],
    outputFile: {
      junit: './test-results/scale-junit.xml',
    },

    // Environment configuration
    environment: 'node',

    // Increase heap size warnings threshold
    // Scale tests legitimately use more memory
    logHeapUsage: true,
  },
}));
