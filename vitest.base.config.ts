import { defineConfig, type UserConfig } from 'vitest/config';
import path from 'path';

/**
 * Base Vitest configuration shared across all test configs.
 * Individual configs can extend and override these settings.
 */
export const baseConfig: UserConfig = defineConfig({
  resolve: {
    alias: {
      '@mongolake': path.resolve(__dirname, './src'),
      '@utils': path.resolve(__dirname, './src/utils'),
      '@types': path.resolve(__dirname, './src/types'),
      '@errors': path.resolve(__dirname, './src/errors'),
      '@client': path.resolve(__dirname, './src/client'),
      '@parquet': path.resolve(__dirname, './src/parquet'),
      '@storage': path.resolve(__dirname, './src/storage'),
      '@config': path.resolve(__dirname, './src/config'),
    },
  },
  test: {
    // Enable global test utilities (describe, it, expect, etc.)
    globals: true,

    // Default environment for most tests
    environment: 'node',

    // Default timeouts
    testTimeout: 30000,
    hookTimeout: 30000,

    // Default pool settings for memory optimization
    pool: 'forks',
    poolOptions: {
      forks: {
        maxForks: 2,
        minForks: 1,
      },
    },

    // Sequential execution by default for consistent results
    sequence: {
      shuffle: false,
    },

    // Default coverage configuration
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
    },
  },
});

export default baseConfig;
