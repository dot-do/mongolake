#!/usr/bin/env npx tsx
/**
 * Insert Benchmark
 *
 * Measures insert throughput for MongoLake:
 * - Single document inserts
 * - Batch inserts
 * - Various document sizes
 * - WAL and buffer performance
 *
 * Run with: npx tsx tests/benchmark/insert-benchmark.ts
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
  generateDocOfSize,
  type BenchmarkResult,
} from './utils.js';
import { BTree } from '../../src/index/btree.js';
import { writeParquet } from '../../src/parquet/io.js';
import { RowGroupSerializer } from '../../src/parquet/row-group.js';

// ============================================================================
// Mock ShardDO Buffer for Insert Testing
// ============================================================================

interface BufferedDoc {
  _id: string;
  _seq: number;
  _op: 'i' | 'u' | 'd';
  document: Record<string, unknown>;
}

class MockBuffer {
  private buffer: Map<string, BufferedDoc> = new Map();
  private currentLSN = 0;
  private bufferSize = 0;

  insert(doc: Record<string, unknown>): void {
    this.currentLSN++;
    const bufferedDoc: BufferedDoc = {
      _id: String(doc._id),
      _seq: this.currentLSN,
      _op: 'i',
      document: doc,
    };
    this.buffer.set(bufferedDoc._id, bufferedDoc);
    this.bufferSize += JSON.stringify(doc).length;
  }

  clear(): void {
    this.buffer.clear();
    this.currentLSN = 0;
    this.bufferSize = 0;
  }

  get size(): number {
    return this.buffer.size;
  }

  get byteSize(): number {
    return this.bufferSize;
  }

  getAll(): BufferedDoc[] {
    return Array.from(this.buffer.values());
  }
}

// ============================================================================
// Insert Benchmarks
// ============================================================================

async function benchmarkSingleInserts() {
  printDivider('Single Document Inserts');
  const results: BenchmarkResult[] = [];

  // Simple documents
  console.log('\nSimple documents (~200 bytes):');
  const simpleBuffer = new MockBuffer();
  results.push(
    await runBenchmark(
      'Single insert - simple doc',
      () => {
        const doc = generateSimpleDoc(simpleBuffer.size);
        simpleBuffer.insert(doc);
      },
      { iterations: 1000, warmup: 100, batchSize: 1 }
    )
  );
  console.log(formatResult(results[results.length - 1]));
  simpleBuffer.clear();

  // Medium documents
  console.log('\nMedium documents (~1KB):');
  const mediumBuffer = new MockBuffer();
  results.push(
    await runBenchmark(
      'Single insert - medium doc (~1KB)',
      () => {
        const doc = generateMediumDoc(mediumBuffer.size);
        mediumBuffer.insert(doc);
      },
      { iterations: 500, warmup: 50, batchSize: 1 }
    )
  );
  console.log(formatResult(results[results.length - 1]));
  mediumBuffer.clear();

  // Large documents
  console.log('\nLarge documents (~5KB):');
  const largeBuffer = new MockBuffer();
  results.push(
    await runBenchmark(
      'Single insert - large doc (~5KB)',
      () => {
        const doc = generateLargeDoc(largeBuffer.size);
        largeBuffer.insert(doc);
      },
      { iterations: 200, warmup: 20, batchSize: 1 }
    )
  );
  console.log(formatResult(results[results.length - 1]));
  largeBuffer.clear();

  return results;
}

async function benchmarkBatchInserts() {
  printDivider('Batch Inserts');
  const results: BenchmarkResult[] = [];

  for (const batchSize of [10, 100, 1000]) {
    console.log(`\nBatch size: ${batchSize}`);
    const buffer = new MockBuffer();

    results.push(
      await runBenchmark(
        `Batch insert - ${batchSize} simple docs`,
        () => {
          for (let i = 0; i < batchSize; i++) {
            const doc = generateSimpleDoc(buffer.size);
            buffer.insert(doc);
          }
        },
        { iterations: 20, warmup: 3, batchSize }
      )
    );
    console.log(formatResult(results[results.length - 1]));
    buffer.clear();
  }

  return results;
}

async function benchmarkDocumentSizes() {
  printDivider('Insert by Document Size');
  const results: BenchmarkResult[] = [];

  const sizes = [
    { label: '100 bytes', bytes: 100 },
    { label: '500 bytes', bytes: 500 },
    { label: '1 KB', bytes: 1024 },
    { label: '5 KB', bytes: 5 * 1024 },
    { label: '10 KB', bytes: 10 * 1024 },
    { label: '50 KB', bytes: 50 * 1024 },
  ];

  for (const { label, bytes } of sizes) {
    console.log(`\nDocument size: ${label}`);
    const buffer = new MockBuffer();

    results.push(
      await runBenchmark(
        `Insert ${label} document`,
        () => {
          const doc = generateDocOfSize(buffer.size, bytes);
          buffer.insert(doc);
        },
        { iterations: 100, warmup: 10, batchSize: 1 }
      )
    );
    console.log(formatResult(results[results.length - 1]));
    buffer.clear();
  }

  return results;
}

async function benchmarkInsertWithIndexing() {
  printDivider('Insert with B-tree Indexing');
  const results: BenchmarkResult[] = [];

  // Insert without index
  console.log('\nWithout index:');
  let counter = 0;
  const bufferNoIndex = new MockBuffer();
  results.push(
    await runBenchmark(
      'Insert without index',
      () => {
        const doc = generateSimpleDoc(counter++);
        bufferNoIndex.insert(doc);
      },
      { iterations: 1000, warmup: 100, batchSize: 1 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Insert with index update
  console.log('\nWith B-tree index on "age":');
  counter = 0;
  const bufferWithIndex = new MockBuffer();
  const ageIndex = new BTree<number>('age_idx', 'age', 64);
  results.push(
    await runBenchmark(
      'Insert with B-tree index',
      () => {
        const doc = generateSimpleDoc(counter);
        bufferWithIndex.insert(doc);
        ageIndex.insert(doc.age as number, `doc-${counter}`);
        counter++;
      },
      { iterations: 1000, warmup: 100, batchSize: 1 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Insert with multiple indexes
  console.log('\nWith 3 B-tree indexes:');
  counter = 0;
  const bufferMultiIndex = new MockBuffer();
  const idx1 = new BTree<number>('age_idx', 'age', 64);
  const idx2 = new BTree<string>('email_idx', 'email', 64);
  const idx3 = new BTree<boolean>('active_idx', 'active', 64);
  results.push(
    await runBenchmark(
      'Insert with 3 indexes',
      () => {
        const doc = generateSimpleDoc(counter);
        bufferMultiIndex.insert(doc);
        idx1.insert(doc.age as number, `doc-${counter}`);
        idx2.insert(doc.email as string, `doc-${counter}`);
        idx3.insert(doc.active as boolean, `doc-${counter}`);
        counter++;
      },
      { iterations: 1000, warmup: 100, batchSize: 1 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  return results;
}

async function benchmarkParquetSerialization() {
  printDivider('Parquet Serialization (Flush Simulation)');
  const results: BenchmarkResult[] = [];

  // Prepare documents
  const smallBatch = Array.from({ length: 100 }, (_, i) => ({
    _id: `doc-${i}`,
    _seq: i + 1,
    _op: 'i' as const,
    doc: generateSimpleDoc(i),
  }));

  const mediumBatch = Array.from({ length: 1000 }, (_, i) => ({
    _id: `doc-${i}`,
    _seq: i + 1,
    _op: 'i' as const,
    doc: generateSimpleDoc(i),
  }));

  const largeBatch = Array.from({ length: 5000 }, (_, i) => ({
    _id: `doc-${i}`,
    _seq: i + 1,
    _op: 'i' as const,
    doc: generateSimpleDoc(i),
  }));

  // Benchmark writeParquet with different batch sizes
  console.log('\nWriteParquet - 100 documents:');
  results.push(
    await runBenchmark(
      'writeParquet - 100 docs',
      () => {
        writeParquet(smallBatch);
      },
      { iterations: 50, warmup: 5 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  console.log('\nWriteParquet - 1000 documents:');
  results.push(
    await runBenchmark(
      'writeParquet - 1000 docs',
      () => {
        writeParquet(mediumBatch);
      },
      { iterations: 20, warmup: 3 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  console.log('\nWriteParquet - 5000 documents:');
  results.push(
    await runBenchmark(
      'writeParquet - 5000 docs',
      () => {
        writeParquet(largeBatch);
      },
      { iterations: 10, warmup: 2 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  // Benchmark RowGroupSerializer
  console.log('\nRowGroupSerializer - 1000 documents:');
  const rowGroupDocs = mediumBatch.map((r) => ({
    _id: r._id,
    _seq: r._seq,
    _op: r._op,
    ...r.doc,
  }));
  results.push(
    await runBenchmark(
      'RowGroupSerializer - 1000 docs',
      () => {
        const serializer = new RowGroupSerializer({ compression: 'snappy' });
        serializer.serialize(rowGroupDocs);
      },
      { iterations: 20, warmup: 3 }
    )
  );
  console.log(formatResult(results[results.length - 1]));

  return results;
}

// ============================================================================
// Main
// ============================================================================

async function main() {
  printHeader('MongoLake Insert Benchmark');
  console.log(`Date: ${new Date().toISOString()}`);

  const allResults: BenchmarkResult[] = [];

  try {
    allResults.push(...(await benchmarkSingleInserts()));
    allResults.push(...(await benchmarkBatchInserts()));
    allResults.push(...(await benchmarkDocumentSizes()));
    allResults.push(...(await benchmarkInsertWithIndexing()));
    allResults.push(...(await benchmarkParquetSerialization()));
  } catch (error) {
    console.error('Benchmark failed:', error);
    process.exit(1);
  }

  printSummaryTable(allResults);
  console.log('Insert benchmark complete!\n');
}

main().catch(console.error);
