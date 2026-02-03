#!/usr/bin/env npx tsx
/**
 * Parquet Benchmark
 *
 * Measures Parquet read/write performance for MongoLake:
 * - writeParquet throughput
 * - readParquet throughput
 * - Variant encoding/decoding
 * - RowGroupSerializer performance
 * - Different document sizes and batch sizes
 *
 * Run with: npx tsx tests/benchmark/parquet-benchmark.ts
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
  generateLargeDoc,
  type BenchmarkResult,
} from './utils.js';
import { writeParquet, readParquet, isParquetFile, getParquetMetadata } from '../../src/parquet/io.js';
import { encodeVariant, decodeVariant } from '../../src/parquet/variant.js';
import { RowGroupSerializer } from '../../src/parquet/row-group.js';

// ============================================================================
// Test Data Preparation
// ============================================================================

interface TestRow {
  _id: string;
  _seq: number;
  _op: 'i' | 'u' | 'd';
  doc: Record<string, unknown>;
}

function prepareTestRows(count: number, docGenerator: (i: number) => Record<string, unknown>): TestRow[] {
  return Array.from({ length: count }, (_, i) => ({
    _id: `doc-${i}`,
    _seq: i + 1,
    _op: 'i' as const,
    doc: docGenerator(i),
  }));
}

// ============================================================================
// Variant Encoding Benchmarks
// ============================================================================

async function benchmarkVariantEncoding() {
  printDivider('Variant Encoding Performance');
  const results: BenchmarkResult[] = [];

  // Simple document encoding
  console.log('\nSimple document encoding:');
  const simpleDoc = generateSimpleDoc(0);
  results.push(
    runBenchmarkSync(
      'Variant encode - simple doc',
      () => {
        encodeVariant(simpleDoc);
      },
      { iterations: 10000, warmup: 1000 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Medium document encoding
  console.log('\nMedium document encoding:');
  const mediumDoc = generateMediumDoc(0);
  results.push(
    runBenchmarkSync(
      'Variant encode - medium doc (~1KB)',
      () => {
        encodeVariant(mediumDoc);
      },
      { iterations: 5000, warmup: 500 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Large document encoding
  console.log('\nLarge document encoding:');
  const largeDoc = generateLargeDoc(0);
  results.push(
    runBenchmarkSync(
      'Variant encode - large doc (~5KB)',
      () => {
        encodeVariant(largeDoc);
      },
      { iterations: 1000, warmup: 100 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  return results;
}

async function benchmarkVariantDecoding() {
  printDivider('Variant Decoding Performance');
  const results: BenchmarkResult[] = [];

  // Simple document decoding
  console.log('\nSimple document decoding:');
  const simpleEncoded = encodeVariant(generateSimpleDoc(0));
  results.push(
    runBenchmarkSync(
      'Variant decode - simple doc',
      () => {
        decodeVariant(simpleEncoded);
      },
      { iterations: 10000, warmup: 1000 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Medium document decoding
  console.log('\nMedium document decoding:');
  const mediumEncoded = encodeVariant(generateMediumDoc(0));
  results.push(
    runBenchmarkSync(
      'Variant decode - medium doc (~1KB)',
      () => {
        decodeVariant(mediumEncoded);
      },
      { iterations: 5000, warmup: 500 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Large document decoding
  console.log('\nLarge document decoding:');
  const largeEncoded = encodeVariant(generateLargeDoc(0));
  results.push(
    runBenchmarkSync(
      'Variant decode - large doc (~5KB)',
      () => {
        decodeVariant(largeEncoded);
      },
      { iterations: 1000, warmup: 100 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  return results;
}

// ============================================================================
// WriteParquet Benchmarks
// ============================================================================

async function benchmarkWriteParquet() {
  printDivider('writeParquet Performance');
  const results: BenchmarkResult[] = [];

  // Different batch sizes with simple documents
  for (const count of [10, 100, 500, 1000, 5000]) {
    console.log(`\nwriteParquet - ${count} simple documents:`);
    const rows = prepareTestRows(count, generateSimpleDoc);
    results.push(
      await runBenchmark(
        `writeParquet - ${count} simple docs`,
        () => {
          writeParquet(rows);
        },
        { iterations: count <= 100 ? 50 : count <= 1000 ? 20 : 10, warmup: 3 }
      )
    );
    console.log(formatResult(results[results.length - 1]));
  }

  // Different document sizes with fixed batch
  console.log('\nwriteParquet - 100 medium documents (~1KB each):');
  const mediumRows = prepareTestRows(100, generateMediumDoc);
  results.push(
    await runBenchmark(
      'writeParquet - 100 medium docs',
      () => {
        writeParquet(mediumRows);
      },
      { iterations: 30, warmup: 5 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  console.log('\nwriteParquet - 100 large documents (~5KB each):');
  const largeRows = prepareTestRows(100, generateLargeDoc);
  results.push(
    await runBenchmark(
      'writeParquet - 100 large docs',
      () => {
        writeParquet(largeRows);
      },
      { iterations: 20, warmup: 3 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  return results;
}

// ============================================================================
// ReadParquet Benchmarks
// ============================================================================

async function benchmarkReadParquet() {
  printDivider('readParquet Performance');
  const results: BenchmarkResult[] = [];

  // Different batch sizes
  for (const count of [10, 100, 500, 1000]) {
    console.log(`\nreadParquet - ${count} simple documents:`);
    const rows = prepareTestRows(count, generateSimpleDoc);
    const parquetData = writeParquet(rows);
    results.push(
      await runBenchmark(
        `readParquet - ${count} simple docs`,
        async () => {
          await readParquet(parquetData);
        },
        { iterations: count <= 100 ? 50 : 20, warmup: 5 }
      )
    );
    console.log(formatResult(results[results.length - 1]));
  }

  // Different document sizes
  console.log('\nreadParquet - 100 medium documents (~1KB each):');
  const mediumRows = prepareTestRows(100, generateMediumDoc);
  const mediumParquet = writeParquet(mediumRows);
  results.push(
    await runBenchmark(
      'readParquet - 100 medium docs',
      async () => {
        await readParquet(mediumParquet);
      },
      { iterations: 30, warmup: 5 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  console.log('\nreadParquet - 100 large documents (~5KB each):');
  const largeRows = prepareTestRows(100, generateLargeDoc);
  const largeParquet = writeParquet(largeRows);
  results.push(
    await runBenchmark(
      'readParquet - 100 large docs',
      async () => {
        await readParquet(largeParquet);
      },
      { iterations: 20, warmup: 3 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  return results;
}

// ============================================================================
// RowGroupSerializer Benchmarks
// ============================================================================

async function benchmarkRowGroupSerializer() {
  printDivider('RowGroupSerializer Performance');
  const results: BenchmarkResult[] = [];

  // Prepare data in the format RowGroupSerializer expects
  function prepareRowGroupDocs(count: number, generator: (i: number) => Record<string, unknown>) {
    return Array.from({ length: count }, (_, i) => {
      const doc = generator(i);
      return {
        _id: `doc-${i}`,
        _seq: i + 1,
        _op: 'i' as const,
        ...doc,
      };
    });
  }

  // Different batch sizes
  for (const count of [100, 500, 1000, 5000]) {
    console.log(`\nRowGroupSerializer - ${count} simple documents:`);
    const docs = prepareRowGroupDocs(count, generateSimpleDoc);
    results.push(
      runBenchmarkSync(
        `RowGroupSerializer - ${count} simple docs`,
        () => {
          const serializer = new RowGroupSerializer({ compression: 'snappy' });
          serializer.serialize(docs);
        },
        { iterations: count <= 500 ? 30 : 15, warmup: 3 }
      )
    );
    console.log(formatResult(results[results.length - 1]));
  }

  // Compare compression options
  console.log('\nCompression comparison (1000 docs):');
  const docs1000 = prepareRowGroupDocs(1000, generateSimpleDoc);

  results.push(
    runBenchmarkSync(
      'RowGroupSerializer - snappy',
      () => {
        const serializer = new RowGroupSerializer({ compression: 'snappy' });
        serializer.serialize(docs1000);
      },
      { iterations: 20, warmup: 3 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  results.push(
    runBenchmarkSync(
      'RowGroupSerializer - uncompressed',
      () => {
        const serializer = new RowGroupSerializer({ compression: 'uncompressed' });
        serializer.serialize(docs1000);
      },
      { iterations: 20, warmup: 3 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  return results;
}

// ============================================================================
// Round-Trip Benchmarks
// ============================================================================

async function benchmarkRoundTrip() {
  printDivider('Round-Trip Performance (Write + Read)');
  const results: BenchmarkResult[] = [];

  for (const count of [100, 500, 1000]) {
    console.log(`\nRound-trip - ${count} simple documents:`);
    const rows = prepareTestRows(count, generateSimpleDoc);

    results.push(
      await runBenchmark(
        `Round-trip - ${count} simple docs`,
        async () => {
          const parquetData = writeParquet(rows);
          await readParquet(parquetData);
        },
        { iterations: 20, warmup: 3 }
      )
    );
    console.log(formatResult(results[results.length - 1]));
  }

  // Large documents round-trip
  console.log('\nRound-trip - 100 large documents:');
  const largeRows = prepareTestRows(100, generateLargeDoc);
  results.push(
    await runBenchmark(
      'Round-trip - 100 large docs',
      async () => {
        const parquetData = writeParquet(largeRows);
        await readParquet(parquetData);
      },
      { iterations: 15, warmup: 3 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  return results;
}

// ============================================================================
// Metadata Operations Benchmarks
// ============================================================================

async function benchmarkMetadataOperations() {
  printDivider('Metadata Operations');
  const results: BenchmarkResult[] = [];

  // Generate some test parquet files of different sizes
  const small = writeParquet(prepareTestRows(100, generateSimpleDoc));
  const medium = writeParquet(prepareTestRows(1000, generateSimpleDoc));
  const large = writeParquet(prepareTestRows(5000, generateSimpleDoc));

  // isParquetFile check
  console.log('\nisParquetFile check:');
  results.push(
    runBenchmarkSync(
      'isParquetFile - small file',
      () => {
        isParquetFile(small);
      },
      { iterations: 10000, warmup: 1000 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // getParquetMetadata
  console.log('\ngetParquetMetadata - small file (100 docs):');
  results.push(
    await runBenchmark(
      'getParquetMetadata - 100 docs',
      async () => {
        await getParquetMetadata(small);
      },
      { iterations: 100, warmup: 10 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  console.log('\ngetParquetMetadata - medium file (1000 docs):');
  results.push(
    await runBenchmark(
      'getParquetMetadata - 1000 docs',
      async () => {
        await getParquetMetadata(medium);
      },
      { iterations: 50, warmup: 5 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  console.log('\ngetParquetMetadata - large file (5000 docs):');
  results.push(
    await runBenchmark(
      'getParquetMetadata - 5000 docs',
      async () => {
        await getParquetMetadata(large);
      },
      { iterations: 30, warmup: 3 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  return results;
}

// ============================================================================
// File Size Analysis
// ============================================================================

function analyzeFileSizes() {
  printDivider('File Size Analysis');

  const sizes = [10, 100, 500, 1000, 5000];
  const docTypes = [
    { name: 'Simple (~200B)', generator: generateSimpleDoc },
    { name: 'Medium (~1KB)', generator: generateMediumDoc },
    { name: 'Large (~5KB)', generator: generateLargeDoc },
  ];

  console.log('\n' + 'Doc Type'.padEnd(20) + sizes.map((s) => `${s} docs`.padStart(12)).join(''));
  console.log('-'.repeat(80));

  for (const { name, generator } of docTypes) {
    const fileSizes = sizes.map((count) => {
      const rows = prepareTestRows(count, generator);
      const parquetData = writeParquet(rows);
      return parquetData.length;
    });

    console.log(
      name.padEnd(20) +
        fileSizes.map((size) => formatBytes(size).padStart(12)).join('')
    );
  }

  console.log('\n' + '-'.repeat(80));
  console.log('Compression ratio (Parquet size / JSON size):');
  console.log('-'.repeat(80));

  for (const { name, generator } of docTypes) {
    const ratios = sizes.map((count) => {
      const rows = prepareTestRows(count, generator);
      const parquetData = writeParquet(rows);
      const jsonSize = JSON.stringify(rows).length;
      return (parquetData.length / jsonSize).toFixed(2);
    });

    console.log(name.padEnd(20) + ratios.map((r) => r.padStart(12)).join(''));
  }
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

// ============================================================================
// Main
// ============================================================================

async function main() {
  printHeader('MongoLake Parquet Benchmark');
  console.log(`Date: ${new Date().toISOString()}`);

  const allResults: BenchmarkResult[] = [];

  try {
    allResults.push(...(await benchmarkVariantEncoding()));
    allResults.push(...(await benchmarkVariantDecoding()));
    allResults.push(...(await benchmarkWriteParquet()));
    allResults.push(...(await benchmarkReadParquet()));
    allResults.push(...(await benchmarkRowGroupSerializer()));
    allResults.push(...(await benchmarkRoundTrip()));
    allResults.push(...(await benchmarkMetadataOperations()));

    // File size analysis (not timed, just informational)
    analyzeFileSizes();
  } catch (error) {
    console.error('Benchmark failed:', error);
    process.exit(1);
  }

  printSummaryTable(allResults);
  console.log('Parquet benchmark complete!\n');
}

main().catch(console.error);
