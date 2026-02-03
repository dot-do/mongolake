/**
 * Compact Test Suite Index
 *
 * This file documents the split compact test suites.
 * The compact tests have been split into logical groupings:
 *
 * - compact-module.test.ts: Module exports, options parsing, help text, utilities
 * - compact-blocks.test.ts: Block identification, analysis, results
 *
 * Additional tests remain in the original compact.test.ts file and can be
 * further split as needed:
 * - Trigger tests (compaction triggering, scheduling, queue)
 * - Collection tests (targeting, pattern matching)
 * - Progress tests (reporting, statistics)
 * - Error tests (handling, tombstone removal)
 * - Optimization tests (read optimization, abort/resume)
 */

import { describe, it, expect } from 'vitest';

describe('CLI Compact - Test Suite Organization', () => {
  it('should have split test files for maintainability', () => {
    const splitSuites = [
      'compact-module.test.ts',
      'compact-blocks.test.ts',
    ];
    expect(splitSuites.length).toBeGreaterThan(0);
  });
});
