#!/usr/bin/env npx tsx
/**
 * Find Benchmark
 *
 * Measures query performance for MongoLake:
 * - Simple equality queries
 * - Range queries
 * - Filter with various operators
 * - Sorting and projection
 * - Index vs full scan comparison
 *
 * Run with: npx tsx tests/benchmark/find-benchmark.ts
 */

import {
  runBenchmark,
  runBenchmarkSync,
  formatResult,
  printHeader,
  printDivider,
  printSummaryTable,
  generateSimpleDoc,
  generateMediumDoc,
  type BenchmarkResult,
} from './utils.js';
import { BTree } from '../../src/index/btree.js';
import { matchesFilter } from '../../src/utils/filter.js';
import { sortDocuments } from '../../src/utils/sort.js';
import { applyProjection } from '../../src/utils/projection.js';
import type { Document, Filter } from '../../src/types.js';

// ============================================================================
// Test Data Setup
// ============================================================================

interface TestDocument {
  _id: string;
  name: string;
  email: string;
  age: number;
  active: boolean;
  createdAt: string;
  score?: number;
  tags?: string[];
  department?: string;
}

function generateTestData(count: number): TestDocument[] {
  const departments = ['engineering', 'sales', 'marketing', 'support', 'hr'];
  const docs: TestDocument[] = [];

  for (let i = 0; i < count; i++) {
    docs.push({
      _id: `doc-${i}`,
      name: `User ${i}`,
      email: `user${i}@example.com`,
      age: 20 + (i % 50),
      active: i % 2 === 0,
      createdAt: new Date(Date.now() - i * 86400000).toISOString(),
      score: Math.random() * 100,
      tags: [`tag${i % 10}`, `tag${(i + 1) % 10}`],
      department: departments[i % 5],
    });
  }

  return docs;
}

// ============================================================================
// Filter Benchmarks
// ============================================================================

async function benchmarkSimpleFilters(docs: TestDocument[]) {
  printDivider('Simple Filter Queries');
  const results: BenchmarkResult[] = [];

  // Equality filter
  console.log('\nEquality filter (age === 25):');
  results.push(
    runBenchmarkSync(
      'Filter: age === 25',
      () => {
        docs.filter((d) => matchesFilter(d as unknown as Document, { age: 25 }));
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Boolean filter
  console.log('\nBoolean filter (active === true):');
  results.push(
    runBenchmarkSync(
      'Filter: active === true',
      () => {
        docs.filter((d) => matchesFilter(d as unknown as Document, { active: true }));
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // String filter
  console.log('\nString equality filter:');
  results.push(
    runBenchmarkSync(
      'Filter: department === "engineering"',
      () => {
        docs.filter((d) =>
          matchesFilter(d as unknown as Document, { department: 'engineering' })
        );
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  return results;
}

async function benchmarkComparisonFilters(docs: TestDocument[]) {
  printDivider('Comparison Filter Queries');
  const results: BenchmarkResult[] = [];

  // $gt filter
  console.log('\nGreater than filter (age > 40):');
  results.push(
    runBenchmarkSync(
      'Filter: age > 40',
      () => {
        docs.filter((d) =>
          matchesFilter(d as unknown as Document, { age: { $gt: 40 } })
        );
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // $gte filter
  console.log('\nGreater than or equal filter (age >= 40):');
  results.push(
    runBenchmarkSync(
      'Filter: age >= 40',
      () => {
        docs.filter((d) =>
          matchesFilter(d as unknown as Document, { age: { $gte: 40 } })
        );
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // $lt filter
  console.log('\nLess than filter (age < 30):');
  results.push(
    runBenchmarkSync(
      'Filter: age < 30',
      () => {
        docs.filter((d) =>
          matchesFilter(d as unknown as Document, { age: { $lt: 30 } })
        );
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Range filter
  console.log('\nRange filter (25 <= age <= 35):');
  results.push(
    runBenchmarkSync(
      'Filter: 25 <= age <= 35',
      () => {
        docs.filter((d) =>
          matchesFilter(d as unknown as Document, { age: { $gte: 25, $lte: 35 } })
        );
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // $ne filter
  console.log('\nNot equal filter (department !== "hr"):');
  results.push(
    runBenchmarkSync(
      'Filter: department !== "hr"',
      () => {
        docs.filter((d) =>
          matchesFilter(d as unknown as Document, { department: { $ne: 'hr' } })
        );
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  return results;
}

async function benchmarkLogicalFilters(docs: TestDocument[]) {
  printDivider('Logical Filter Queries');
  const results: BenchmarkResult[] = [];

  // $and filter
  console.log('\n$and filter (active && age > 30):');
  results.push(
    runBenchmarkSync(
      'Filter: $and [active, age > 30]',
      () => {
        docs.filter((d) =>
          matchesFilter(d as unknown as Document, {
            $and: [{ active: true }, { age: { $gt: 30 } }],
          })
        );
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // $or filter
  console.log('\n$or filter (age < 25 || age > 45):');
  results.push(
    runBenchmarkSync(
      'Filter: $or [age < 25, age > 45]',
      () => {
        docs.filter((d) =>
          matchesFilter(d as unknown as Document, {
            $or: [{ age: { $lt: 25 } }, { age: { $gt: 45 } }],
          })
        );
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Complex compound filter
  console.log('\nComplex compound filter:');
  results.push(
    runBenchmarkSync(
      'Complex: (active && age > 30) || dept="engineering"',
      () => {
        docs.filter((d) =>
          matchesFilter(d as unknown as Document, {
            $or: [
              { $and: [{ active: true }, { age: { $gt: 30 } }] },
              { department: 'engineering' },
            ],
          })
        );
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  return results;
}

async function benchmarkArrayFilters(docs: TestDocument[]) {
  printDivider('Array Filter Queries');
  const results: BenchmarkResult[] = [];

  // $in filter
  console.log('\n$in filter (department in [engineering, sales]):');
  results.push(
    runBenchmarkSync(
      'Filter: $in [engineering, sales]',
      () => {
        docs.filter((d) =>
          matchesFilter(d as unknown as Document, {
            department: { $in: ['engineering', 'sales'] },
          })
        );
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // $nin filter
  console.log('\n$nin filter (department not in [hr, support]):');
  results.push(
    runBenchmarkSync(
      'Filter: $nin [hr, support]',
      () => {
        docs.filter((d) =>
          matchesFilter(d as unknown as Document, {
            department: { $nin: ['hr', 'support'] },
          })
        );
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  return results;
}

// ============================================================================
// Sort and Projection Benchmarks
// ============================================================================

async function benchmarkSorting(docs: TestDocument[]) {
  printDivider('Sorting Performance');
  const results: BenchmarkResult[] = [];

  // Single field sort ascending
  console.log('\nSingle field sort (age ascending):');
  results.push(
    runBenchmarkSync(
      'Sort: age ASC',
      () => {
        sortDocuments([...docs], { age: 1 });
      },
      { iterations: 50, warmup: 5 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Single field sort descending
  console.log('\nSingle field sort (age descending):');
  results.push(
    runBenchmarkSync(
      'Sort: age DESC',
      () => {
        sortDocuments([...docs], { age: -1 });
      },
      { iterations: 50, warmup: 5 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Multi-field sort
  console.log('\nMulti-field sort (department ASC, age DESC):');
  results.push(
    runBenchmarkSync(
      'Sort: department ASC, age DESC',
      () => {
        sortDocuments([...docs], { department: 1, age: -1 });
      },
      { iterations: 50, warmup: 5 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // String field sort
  console.log('\nString field sort (name ascending):');
  results.push(
    runBenchmarkSync(
      'Sort: name ASC',
      () => {
        sortDocuments([...docs], { name: 1 });
      },
      { iterations: 50, warmup: 5 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  return results;
}

async function benchmarkProjection(docs: TestDocument[]) {
  printDivider('Projection Performance');
  const results: BenchmarkResult[] = [];

  // Include projection
  console.log('\nInclude projection (_id, name, age):');
  results.push(
    runBenchmarkSync(
      'Projection: include 3 fields',
      () => {
        docs.map((d) => applyProjection(d, { _id: 1, name: 1, age: 1 }));
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Exclude projection
  console.log('\nExclude projection (exclude tags, score):');
  results.push(
    runBenchmarkSync(
      'Projection: exclude 2 fields',
      () => {
        docs.map((d) => applyProjection(d, { tags: 0, score: 0 }));
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  return results;
}

// ============================================================================
// Index vs Full Scan Comparison
// ============================================================================

async function benchmarkIndexVsScan(docs: TestDocument[]) {
  printDivider('Index vs Full Scan Comparison');
  const results: BenchmarkResult[] = [];

  // Build index
  const ageIndex = new BTree<number>('age_idx', 'age', 64);
  const departmentIndex = new BTree<string>('dept_idx', 'department', 64);

  for (const doc of docs) {
    ageIndex.insert(doc.age, doc._id);
    departmentIndex.insert(doc.department!, doc._id);
  }

  const docsMap = new Map(docs.map((d) => [d._id, d]));

  // Full scan equality
  console.log('\nFull scan - equality (age === 30):');
  results.push(
    runBenchmarkSync(
      'Full scan: age === 30',
      () => {
        docs.filter((d) => d.age === 30);
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Index lookup equality
  console.log('\nIndex lookup - equality (age === 30):');
  results.push(
    runBenchmarkSync(
      'Index lookup: age === 30',
      () => {
        const docIds = ageIndex.search(30);
        docIds.map((id) => docsMap.get(id));
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Full scan range
  console.log('\nFull scan - range (30 <= age <= 40):');
  results.push(
    runBenchmarkSync(
      'Full scan: 30 <= age <= 40',
      () => {
        docs.filter((d) => d.age >= 30 && d.age <= 40);
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Index range query
  console.log('\nIndex range - (30 <= age <= 40):');
  results.push(
    runBenchmarkSync(
      'Index range: 30 <= age <= 40',
      () => {
        const entries = ageIndex.range(30, 40);
        entries.flatMap(([_, ids]) => ids.map((id) => docsMap.get(id)));
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // String index lookup
  console.log('\nIndex lookup - string (department === "engineering"):');
  results.push(
    runBenchmarkSync(
      'Index lookup: department === "engineering"',
      () => {
        const docIds = departmentIndex.search('engineering');
        docIds.map((id) => docsMap.get(id));
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  return results;
}

// ============================================================================
// Dataset Size Benchmarks
// ============================================================================

async function benchmarkDatasetSizes() {
  printDivider('Query Performance by Dataset Size');
  const results: BenchmarkResult[] = [];

  for (const size of [1000, 10000, 50000, 100000]) {
    console.log(`\nDataset size: ${size.toLocaleString()}`);
    const docs = generateTestData(size);

    // Simple filter
    results.push(
      runBenchmarkSync(
        `Filter age > 40 (${size.toLocaleString()} docs)`,
        () => {
          docs.filter((d) =>
            matchesFilter(d as unknown as Document, { age: { $gt: 40 } })
          );
        },
        { iterations: 20, warmup: 3 }
      )
    );
    console.log(formatResult(results[results.length - 1]));
  }

  return results;
}

// ============================================================================
// Main
// ============================================================================

async function main() {
  printHeader('MongoLake Find Benchmark');
  console.log(`Date: ${new Date().toISOString()}`);

  // Generate test data
  console.log('\nGenerating test data (10,000 documents)...');
  const docs = generateTestData(10000);
  console.log(`Generated ${docs.length} documents`);

  const allResults: BenchmarkResult[] = [];

  try {
    allResults.push(...(await benchmarkSimpleFilters(docs)));
    allResults.push(...(await benchmarkComparisonFilters(docs)));
    allResults.push(...(await benchmarkLogicalFilters(docs)));
    allResults.push(...(await benchmarkArrayFilters(docs)));
    allResults.push(...(await benchmarkSorting(docs)));
    allResults.push(...(await benchmarkProjection(docs)));
    allResults.push(...(await benchmarkIndexVsScan(docs)));
    allResults.push(...(await benchmarkDatasetSizes()));
  } catch (error) {
    console.error('Benchmark failed:', error);
    process.exit(1);
  }

  printSummaryTable(allResults);
  console.log('Find benchmark complete!\n');
}

main().catch(console.error);
