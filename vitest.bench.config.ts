import { defineConfig, mergeConfig } from 'vitest/config';
import { baseConfig } from './vitest.base.config';

/**
 * Vitest Benchmark Configuration
 *
 * Runs performance benchmarks using vitest's bench feature.
 * Results can be used to detect regressions and track performance over time.
 *
 * Run with: pnpm run benchmark:vitest
 */
export default mergeConfig(baseConfig, defineConfig({
  test: {
    // Benchmark mode
    benchmark: {
      // Output reporters
      reporters: ['default'],

      // Include benchmark files only
      include: [
        'tests/benchmark/**/*.bench.ts',
      ],

      // Output file for JSON results (useful for CI/regression tracking)
      outputFile: {
        json: './benchmark-results.json',
      },
    },

    // Timeouts for long-running benchmarks
    testTimeout: 120000,
    hookTimeout: 60000,

    // Don't run in parallel to get consistent results
    poolOptions: {
      forks: {
        maxForks: 1,
        minForks: 1,
      },
    },
  },
}));
