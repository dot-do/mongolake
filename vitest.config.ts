import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config';

export default defineWorkersConfig({
  test: {
    globals: true,
    pool: '@cloudflare/vitest-pool-workers',
    poolOptions: {
      workers: {
        wrangler: { configPath: './wrangler.toml' },
        miniflare: {
          // Test-specific bindings (must match wrangler.toml and worker code)
          r2Buckets: ['BUCKET'],
          durableObjects: {
            RPC_NAMESPACE: { className: 'ShardDO', useSQLite: true },
          },
        },
      },
    },
    include: [
      'tests/integration/**/*.test.ts',
    ],
    exclude: [
      'tests/unit/**/*.test.ts', // Unit tests run separately in Node
      'tests/e2e/**/*.test.ts',  // E2E tests run against deployed workers
    ],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
    },
    // Reporters for CI integration
    reporters: ['default', 'junit'],
    outputFile: {
      junit: './test-results/integration-junit.xml',
    },
  },
});
