import { defineConfig, mergeConfig } from 'vitest/config';
import { baseConfig } from './vitest.base.config';

/**
 * Unit tests run in Node.js with mocks.
 * These test individual functions/classes in isolation.
 */
export default mergeConfig(baseConfig, defineConfig({
  test: {
    include: [
      'src/**/*.test.ts',
      'tests/unit/**/*.test.ts',
      'tests/compat/**/*.test.ts',
    ],
    // Stop early on multiple failures to save resources
    bail: 5,
    // Reporters for CI integration
    reporters: ['default', 'junit'],
    outputFile: {
      junit: './test-results/unit-junit.xml',
    },
    coverage: {
      provider: 'v8',
      reporter: ['text', 'html', 'lcov'],
      reportsDirectory: './coverage',
      thresholds: {
        lines: 80,
        branches: 80,
        functions: 80,
        statements: 80,
      },
      exclude: [
        'node_modules/**',
        'dist/**',
        'tests/**',
        '**/*.d.ts',
        '**/*.test.ts',
      ],
    },
  },
}));
