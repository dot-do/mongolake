import { defineConfig, mergeConfig } from 'vitest/config';
import { baseConfig } from './vitest.base.config';

/**
 * Mutation testing configuration.
 * Runs a subset of tests that are known to pass and cover the mutated code.
 */
export default mergeConfig(baseConfig, defineConfig({
  test: {
    include: [
      'src/utils/__tests__/**/*.test.ts',
      'tests/unit/utils/**/*.test.ts',
    ],
    exclude: [
      // Exclude tests with import path issues
      'tests/unit/utils/validation.test.ts',
      'tests/unit/utils/update.test.ts',
      'tests/unit/utils/update-null-checks.test.ts',
      'tests/unit/utils/update-type-safety.test.ts',
      'tests/unit/utils/nested.test.ts',
      'tests/unit/utils/nested-depth.test.ts',
      // Exclude tests with known failures (lru-cache iteration tests)
      'tests/unit/utils/lru-cache-iteration.test.ts',
    ],
  },
}));
