#!/usr/bin/env npx tsx
/**
 * Index Benchmark
 *
 * Measures B-tree index performance for MongoLake:
 * - Insert performance at various scales
 * - Lookup performance (single key, range)
 * - Delete performance
 * - Serialization/deserialization
 * - Memory usage estimates
 *
 * Run with: npx tsx tests/benchmark/index-benchmark.ts
 */

import {
  runBenchmark,
  runBenchmarkSync,
  formatResult,
  printHeader,
  printDivider,
  printSummaryTable,
  type BenchmarkResult,
} from './utils.js';
import { BTree } from '../../src/index/btree.js';

// ============================================================================
// Index Insert Benchmarks
// ============================================================================

async function benchmarkIndexInsert() {
  printDivider('B-tree Insert Performance');
  const results: BenchmarkResult[] = [];

  // Sequential inserts at different scales
  for (const count of [1000, 10000, 50000, 100000]) {
    console.log(`\nSequential insert - ${count.toLocaleString()} keys:`);
    results.push(
      runBenchmarkSync(
        `Insert ${count.toLocaleString()} sequential keys`,
        () => {
          const tree = new BTree<number>('test_idx', 'value', 64);
          for (let i = 0; i < count; i++) {
            tree.insert(i, `doc-${i}`);
          }
        },
        { iterations: count <= 10000 ? 20 : 10, warmup: 2, batchSize: count }
      )
    );
    console.log(formatResult(results[results.length - 1]));
  }

  // Random order inserts
  console.log('\nRandom order insert - 10,000 keys:');
  const randomKeys = Array.from({ length: 10000 }, () => Math.floor(Math.random() * 1000000));
  results.push(
    runBenchmarkSync(
      'Insert 10,000 random keys',
      () => {
        const tree = new BTree<number>('test_idx', 'value', 64);
        for (let i = 0; i < randomKeys.length; i++) {
          tree.insert(randomKeys[i], `doc-${i}`);
        }
      },
      { iterations: 20, warmup: 3, batchSize: 10000 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // String key inserts
  console.log('\nString key insert - 10,000 keys:');
  results.push(
    runBenchmarkSync(
      'Insert 10,000 string keys',
      () => {
        const tree = new BTree<string>('email_idx', 'email', 64);
        for (let i = 0; i < 10000; i++) {
          tree.insert(`user${i}@example.com`, `doc-${i}`);
        }
      },
      { iterations: 20, warmup: 3, batchSize: 10000 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Multiple docIds per key (non-unique index)
  console.log('\nMultiple docIds per key (non-unique):');
  results.push(
    runBenchmarkSync(
      'Insert 10,000 keys (100 unique, 100 docs each)',
      () => {
        const tree = new BTree<number>('status_idx', 'status', 64);
        for (let i = 0; i < 10000; i++) {
          tree.insert(i % 100, `doc-${i}`);
        }
      },
      { iterations: 20, warmup: 3, batchSize: 10000 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  return results;
}

// ============================================================================
// Index Lookup Benchmarks
// ============================================================================

async function benchmarkIndexLookup() {
  printDivider('B-tree Lookup Performance');
  const results: BenchmarkResult[] = [];

  // Build trees of different sizes
  const sizes = [1000, 10000, 50000, 100000];
  const trees = new Map<number, BTree<number>>();

  for (const size of sizes) {
    const tree = new BTree<number>('test_idx', 'value', 64);
    for (let i = 0; i < size; i++) {
      tree.insert(i, `doc-${i}`);
    }
    trees.set(size, tree);
  }

  // Point lookup at different scales
  for (const size of sizes) {
    console.log(`\nPoint lookup - ${size.toLocaleString()} keys:`);
    const tree = trees.get(size)!;
    const lookupKey = Math.floor(size / 2);

    results.push(
      runBenchmarkSync(
        `Point lookup (${size.toLocaleString()} keys)`,
        () => {
          tree.search(lookupKey);
        },
        { iterations: 10000, warmup: 1000 }
      )
    );
    console.log(formatResult(results[results.length - 1]));
  }

  // has() check
  console.log('\nhas() check - 100,000 keys:');
  const largeTree = trees.get(100000)!;
  results.push(
    runBenchmarkSync(
      'has() check (100,000 keys)',
      () => {
        largeTree.has(50000);
      },
      { iterations: 10000, warmup: 1000 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Lookup non-existent key
  console.log('\nLookup non-existent key:');
  results.push(
    runBenchmarkSync(
      'Lookup non-existent key (100,000 keys)',
      () => {
        largeTree.search(999999);
      },
      { iterations: 10000, warmup: 1000 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Multiple lookups
  console.log('\nMultiple lookups (100 sequential):');
  results.push(
    runBenchmarkSync(
      '100 sequential lookups',
      () => {
        for (let i = 0; i < 100; i++) {
          largeTree.search(i * 1000);
        }
      },
      { iterations: 1000, warmup: 100, batchSize: 100 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  return results;
}

// ============================================================================
// Index Range Query Benchmarks
// ============================================================================

async function benchmarkIndexRange() {
  printDivider('B-tree Range Query Performance');
  const results: BenchmarkResult[] = [];

  // Build a large tree
  const tree = new BTree<number>('test_idx', 'value', 64);
  for (let i = 0; i < 100000; i++) {
    tree.insert(i, `doc-${i}`);
  }

  // Small range (1% of data)
  console.log('\nSmall range (1% of data):');
  results.push(
    runBenchmarkSync(
      'Range query: 0-1000 (1%)',
      () => {
        tree.range(0, 1000);
      },
      { iterations: 1000, warmup: 100 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Medium range (10% of data)
  console.log('\nMedium range (10% of data):');
  results.push(
    runBenchmarkSync(
      'Range query: 0-10000 (10%)',
      () => {
        tree.range(0, 10000);
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Large range (50% of data)
  console.log('\nLarge range (50% of data):');
  results.push(
    runBenchmarkSync(
      'Range query: 0-50000 (50%)',
      () => {
        tree.range(0, 50000);
      },
      { iterations: 20, warmup: 3 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Full scan (all entries)
  console.log('\nFull scan (entries()):');
  results.push(
    runBenchmarkSync(
      'entries() - all 100,000 keys',
      () => {
        tree.entries();
      },
      { iterations: 10, warmup: 2 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Middle range
  console.log('\nMiddle range (40000-60000):');
  results.push(
    runBenchmarkSync(
      'Range query: 40000-60000 (20%)',
      () => {
        tree.range(40000, 60000);
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // min() and max()
  console.log('\nmin() and max():');
  results.push(
    runBenchmarkSync(
      'min() lookup',
      () => {
        tree.min();
      },
      { iterations: 10000, warmup: 1000 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  results.push(
    runBenchmarkSync(
      'max() lookup',
      () => {
        tree.max();
      },
      { iterations: 10000, warmup: 1000 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  return results;
}

// ============================================================================
// Index Delete Benchmarks
// ============================================================================

async function benchmarkIndexDelete() {
  printDivider('B-tree Delete Performance');
  const results: BenchmarkResult[] = [];

  // Delete from small tree
  console.log('\nDelete from 1,000 key tree:');
  results.push(
    runBenchmarkSync(
      'Delete 100 keys from 1,000',
      () => {
        const tree = new BTree<number>('test_idx', 'value', 64);
        for (let i = 0; i < 1000; i++) {
          tree.insert(i, `doc-${i}`);
        }
        for (let i = 0; i < 100; i++) {
          tree.delete(i * 10);
        }
      },
      { iterations: 50, warmup: 5 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Delete from medium tree
  console.log('\nDelete from 10,000 key tree:');
  results.push(
    runBenchmarkSync(
      'Delete 1,000 keys from 10,000',
      () => {
        const tree = new BTree<number>('test_idx', 'value', 64);
        for (let i = 0; i < 10000; i++) {
          tree.insert(i, `doc-${i}`);
        }
        for (let i = 0; i < 1000; i++) {
          tree.delete(i * 10);
        }
      },
      { iterations: 20, warmup: 3 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Delete specific docId
  console.log('\nDelete specific docId (non-unique index):');
  results.push(
    runBenchmarkSync(
      'Delete docId from key with multiple docs',
      () => {
        const tree = new BTree<number>('status_idx', 'status', 64);
        // Insert 100 docs per status (10 statuses)
        for (let i = 0; i < 1000; i++) {
          tree.insert(i % 10, `doc-${i}`);
        }
        // Delete specific docIds
        for (let i = 0; i < 50; i++) {
          tree.delete(i % 10, `doc-${i}`);
        }
      },
      { iterations: 50, warmup: 5 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  return results;
}

// ============================================================================
// Index Serialization Benchmarks
// ============================================================================

async function benchmarkIndexSerialization() {
  printDivider('B-tree Serialization Performance');
  const results: BenchmarkResult[] = [];

  // Build trees of different sizes
  const sizes = [1000, 10000, 50000];

  for (const size of sizes) {
    const tree = new BTree<number>('test_idx', 'value', 64);
    for (let i = 0; i < size; i++) {
      tree.insert(i, `doc-${i}`);
    }

    // Serialize
    console.log(`\nSerialize ${size.toLocaleString()} keys:`);
    results.push(
      runBenchmarkSync(
        `serialize() - ${size.toLocaleString()} keys`,
        () => {
          tree.serialize();
        },
        { iterations: size <= 1000 ? 100 : 20, warmup: 5 }
      )
    );
    console.log(formatResult(results[results.length - 1]));

    // toJSON
    console.log(`toJSON ${size.toLocaleString()} keys:`);
    results.push(
      runBenchmarkSync(
        `toJSON() - ${size.toLocaleString()} keys`,
        () => {
          tree.toJSON();
        },
        { iterations: size <= 1000 ? 50 : 10, warmup: 3 }
      )
    );
    console.log(formatResult(results[results.length - 1]));

    // Deserialize
    const serialized = tree.serialize();
    console.log(`Deserialize ${size.toLocaleString()} keys:`);
    results.push(
      runBenchmarkSync(
        `deserialize() - ${size.toLocaleString()} keys`,
        () => {
          BTree.deserialize<number>(serialized);
        },
        { iterations: size <= 1000 ? 100 : 20, warmup: 5 }
      )
    );
    console.log(formatResult(results[results.length - 1]));

    // fromJSON
    const json = tree.toJSON();
    console.log(`fromJSON ${size.toLocaleString()} keys:`);
    results.push(
      runBenchmarkSync(
        `fromJSON() - ${size.toLocaleString()} keys`,
        () => {
          BTree.fromJSON<number>(json);
        },
        { iterations: size <= 1000 ? 50 : 10, warmup: 3 }
      )
    );
    console.log(formatResult(results[results.length - 1]));
  }

  return results;
}

// ============================================================================
// Index Degree Comparison
// ============================================================================

async function benchmarkIndexDegree() {
  printDivider('B-tree Minimum Degree Comparison');
  const results: BenchmarkResult[] = [];

  const degrees = [4, 16, 64, 128, 256];
  const count = 10000;

  for (const degree of degrees) {
    console.log(`\nMinimum degree: ${degree}`);

    // Insert
    results.push(
      runBenchmarkSync(
        `Insert 10,000 (degree=${degree})`,
        () => {
          const tree = new BTree<number>('test_idx', 'value', degree);
          for (let i = 0; i < count; i++) {
            tree.insert(i, `doc-${i}`);
          }
        },
        { iterations: 15, warmup: 3, batchSize: count }
      )
    );
    console.log(`  Insert: ${results[results.length - 1].opsPerSec.toFixed(2)} ops/sec`);

    // Lookup
    const tree = new BTree<number>('test_idx', 'value', degree);
    for (let i = 0; i < count; i++) {
      tree.insert(i, `doc-${i}`);
    }

    results.push(
      runBenchmarkSync(
        `Lookup (degree=${degree})`,
        () => {
          tree.search(5000);
        },
        { iterations: 10000, warmup: 1000 }
      )
    );
    console.log(`  Lookup: ${results[results.length - 1].opsPerSec.toFixed(2)} ops/sec`);

    // Range
    results.push(
      runBenchmarkSync(
        `Range 1000 keys (degree=${degree})`,
        () => {
          tree.range(4000, 5000);
        },
        { iterations: 1000, warmup: 100 }
      )
    );
    console.log(`  Range:  ${results[results.length - 1].opsPerSec.toFixed(2)} ops/sec`);

    // Serialize size
    const serialized = tree.serialize();
    const jsonSize = JSON.stringify(serialized).length;
    console.log(`  JSON size: ${(jsonSize / 1024).toFixed(1)} KB`);
  }

  return results;
}

// ============================================================================
// Memory Analysis
// ============================================================================

function analyzeMemoryUsage() {
  printDivider('Memory Usage Analysis');

  const sizes = [1000, 10000, 50000, 100000];

  console.log('\n' + 'Keys'.padEnd(15) + 'Serialized JSON'.padStart(20) + 'Keys/KB'.padStart(15));
  console.log('-'.repeat(50));

  for (const size of sizes) {
    const tree = new BTree<number>('test_idx', 'value', 64);
    for (let i = 0; i < size; i++) {
      tree.insert(i, `doc-${i}`);
    }

    const json = tree.toJSON();
    const sizeKB = json.length / 1024;
    const keysPerKB = size / sizeKB;

    console.log(
      size.toLocaleString().padEnd(15) +
        `${sizeKB.toFixed(1)} KB`.padStart(20) +
        keysPerKB.toFixed(1).padStart(15)
    );
  }

  // String keys comparison
  console.log('\nString keys comparison (10,000 keys):');
  const numTree = new BTree<number>('num_idx', 'num', 64);
  const strTree = new BTree<string>('str_idx', 'str', 64);

  for (let i = 0; i < 10000; i++) {
    numTree.insert(i, `doc-${i}`);
    strTree.insert(`user${i}@example.com`, `doc-${i}`);
  }

  const numJson = numTree.toJSON();
  const strJson = strTree.toJSON();

  console.log(`  Number keys: ${(numJson.length / 1024).toFixed(1)} KB`);
  console.log(`  String keys: ${(strJson.length / 1024).toFixed(1)} KB`);
  console.log(`  Ratio: ${(strJson.length / numJson.length).toFixed(2)}x`);
}

// ============================================================================
// Main
// ============================================================================

async function main() {
  printHeader('MongoLake Index Benchmark');
  console.log(`Date: ${new Date().toISOString()}`);

  const allResults: BenchmarkResult[] = [];

  try {
    allResults.push(...(await benchmarkIndexInsert()));
    allResults.push(...(await benchmarkIndexLookup()));
    allResults.push(...(await benchmarkIndexRange()));
    allResults.push(...(await benchmarkIndexDelete()));
    allResults.push(...(await benchmarkIndexSerialization()));
    allResults.push(...(await benchmarkIndexDegree()));

    // Memory analysis (not timed)
    analyzeMemoryUsage();
  } catch (error) {
    console.error('Benchmark failed:', error);
    process.exit(1);
  }

  printSummaryTable(allResults);
  console.log('Index benchmark complete!\n');
}

main().catch(console.error);
