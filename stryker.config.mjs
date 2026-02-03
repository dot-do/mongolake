// @ts-check
/** @type {import('@stryker-mutator/api/core').PartialStrykerOptions} */
export default {
  packageManager: 'pnpm',
  plugins: [
    '@stryker-mutator/vitest-runner'
  ],
  testRunner: 'vitest',
  testRunnerNodeArgs: ['--experimental-vm-modules'],
  vitest: {
    configFile: 'vitest.mutation.config.ts',
    dir: '.',
  },
  reporters: ['progress', 'html', 'clear-text'],
  htmlReporter: {
    fileName: 'reports/mutation/index.html'
  },
  mutate: [
    'src/utils/**/*.ts',
    '!src/utils/**/*.test.ts',
    '!src/utils/__tests__/**',
    '!src/utils/index.ts'
  ],
  ignorePatterns: [
    '.beads/**',
    '.stryker-tmp/**',
    '*.sock'
  ],
  coverageAnalysis: 'all',
  concurrency: 4,
  timeoutMS: 30000,
  thresholds: {
    high: 85,
    low: 70,
    break: 60
  }
};
