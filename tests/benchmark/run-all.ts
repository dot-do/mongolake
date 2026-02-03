#!/usr/bin/env npx tsx
/**
 * MongoLake Benchmark Suite Runner
 *
 * Runs all benchmark suites and produces a comprehensive report.
 *
 * Run with: npx tsx tests/benchmark/run-all.ts
 * Or use: pnpm run benchmark
 *
 * Options:
 *   --insert   Run only insert benchmarks
 *   --find     Run only find benchmarks
 *   --parquet  Run only parquet benchmarks
 *   --index    Run only index benchmarks
 */

import { spawn } from 'child_process';
import { join } from 'path';

// ============================================================================
// Configuration
// ============================================================================

const BENCHMARK_DIR = new URL('.', import.meta.url).pathname;

const BENCHMARKS = [
  { name: 'Insert', file: 'insert-benchmark.ts', flag: '--insert' },
  { name: 'Find', file: 'find-benchmark.ts', flag: '--find' },
  { name: 'Parquet', file: 'parquet-benchmark.ts', flag: '--parquet' },
  { name: 'Index', file: 'index-benchmark.ts', flag: '--index' },
];

// ============================================================================
// Utilities
// ============================================================================

function printBanner() {
  console.log(`
================================================================================
                         MongoLake Benchmark Suite
================================================================================

Date: ${new Date().toISOString()}
Node: ${process.version}
Platform: ${process.platform} ${process.arch}

`);
}

function printFooter() {
  console.log(`
================================================================================
                        All Benchmarks Complete
================================================================================
`);
}

async function runBenchmark(name: string, file: string): Promise<boolean> {
  console.log(`\n${'='.repeat(70)}`);
  console.log(`Running ${name} Benchmark...`);
  console.log('='.repeat(70) + '\n');

  return new Promise((resolve) => {
    const child = spawn('npx', ['tsx', join(BENCHMARK_DIR, file)], {
      stdio: 'inherit',
      shell: true,
    });

    child.on('close', (code) => {
      if (code !== 0) {
        console.error(`\n${name} benchmark failed with code ${code}`);
        resolve(false);
      } else {
        resolve(true);
      }
    });

    child.on('error', (err) => {
      console.error(`\nFailed to run ${name} benchmark:`, err);
      resolve(false);
    });
  });
}

// ============================================================================
// Main
// ============================================================================

async function main() {
  const args = process.argv.slice(2);

  // Determine which benchmarks to run
  let benchmarksToRun = BENCHMARKS;

  if (args.length > 0) {
    const flags = args.filter((a) => a.startsWith('--'));
    if (flags.length > 0) {
      benchmarksToRun = BENCHMARKS.filter((b) => flags.includes(b.flag));
      if (benchmarksToRun.length === 0) {
        console.error('No matching benchmarks for flags:', flags.join(', '));
        console.error('Available flags:', BENCHMARKS.map((b) => b.flag).join(', '));
        process.exit(1);
      }
    }
  }

  printBanner();

  console.log('Benchmarks to run:');
  for (const b of benchmarksToRun) {
    console.log(`  - ${b.name}`);
  }

  const startTime = Date.now();
  const results: { name: string; success: boolean }[] = [];

  for (const benchmark of benchmarksToRun) {
    const success = await runBenchmark(benchmark.name, benchmark.file);
    results.push({ name: benchmark.name, success });
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  printFooter();

  console.log('Results Summary:');
  console.log('-'.repeat(40));
  for (const r of results) {
    const status = r.success ? 'PASS' : 'FAIL';
    console.log(`  ${r.name.padEnd(15)} ${status}`);
  }
  console.log('-'.repeat(40));
  console.log(`Total time: ${elapsed}s`);

  const failed = results.filter((r) => !r.success);
  if (failed.length > 0) {
    console.error(`\n${failed.length} benchmark(s) failed.`);
    process.exit(1);
  }

  console.log('\nAll benchmarks completed successfully!');
}

main().catch((err) => {
  console.error('Benchmark runner failed:', err);
  process.exit(1);
});
