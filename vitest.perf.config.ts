import { defineConfig, mergeConfig } from 'vitest/config';
import { baseConfig } from './vitest.base.config';

/**
 * Performance tests run in Node.js with minimal parallelism
 * to get consistent timing measurements.
 *
 * Run with: pnpm test:perf
 */
export default mergeConfig(baseConfig, defineConfig({
  test: {
    include: [
      'tests/performance/**/*.test.ts',
    ],
    // Performance tests should run sequentially for consistent results
    poolOptions: {
      forks: {
        maxForks: 1,
        minForks: 1,
      },
    },
    // Longer timeouts for performance tests
    testTimeout: 60000,
    // Don't bail - run all performance tests
    bail: 0,
    // Reporters for CI integration
    reporters: ['default', 'junit'],
    outputFile: {
      junit: './test-results/perf-junit.xml',
    },
  },
}));
