/**
 * Insert Benchmark - Vitest bench suite
 *
 * Measures insert throughput for MongoLake operations.
 *
 * Run with: pnpm run benchmark:vitest
 */

import { bench, describe } from 'vitest';
import { BTree } from '../../src/index/btree.js';
import { writeParquet } from '../../src/parquet/io.js';
import { generateSimpleDoc, generateMediumDoc, generateLargeDoc } from './utils.js';

// ============================================================================
// Mock Buffer for Insert Testing
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
}

// ============================================================================
// insertOne Throughput Benchmarks
// ============================================================================

describe('insertOne throughput', () => {
  const buffer = new MockBuffer();
  let counter = 0;

  bench('simple document (~200 bytes)', () => {
    const doc = generateSimpleDoc(counter++);
    buffer.insert(doc);
  }, {
    iterations: 1000,
    warmupIterations: 100,
  });

  bench('medium document (~1KB)', () => {
    const doc = generateMediumDoc(counter++);
    buffer.insert(doc);
  }, {
    iterations: 500,
    warmupIterations: 50,
  });

  bench('large document (~5KB)', () => {
    const doc = generateLargeDoc(counter++);
    buffer.insert(doc);
  }, {
    iterations: 200,
    warmupIterations: 20,
  });
});

// ============================================================================
// insertMany Batch Benchmarks
// ============================================================================

describe('insertMany with batch sizes', () => {
  bench('batch size: 10', () => {
    const buffer = new MockBuffer();
    for (let i = 0; i < 10; i++) {
      buffer.insert(generateSimpleDoc(i));
    }
  }, {
    iterations: 100,
    warmupIterations: 10,
  });

  bench('batch size: 100', () => {
    const buffer = new MockBuffer();
    for (let i = 0; i < 100; i++) {
      buffer.insert(generateSimpleDoc(i));
    }
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('batch size: 1000', () => {
    const buffer = new MockBuffer();
    for (let i = 0; i < 1000; i++) {
      buffer.insert(generateSimpleDoc(i));
    }
  }, {
    iterations: 20,
    warmupIterations: 3,
  });
});

// ============================================================================
// Insert with Indexing Benchmarks
// ============================================================================

describe('insert with indexing', () => {
  bench('without index', () => {
    const buffer = new MockBuffer();
    for (let i = 0; i < 100; i++) {
      buffer.insert(generateSimpleDoc(i));
    }
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('with single B-tree index', () => {
    const buffer = new MockBuffer();
    const ageIndex = new BTree<number>('age_idx', 'age', 64);
    for (let i = 0; i < 100; i++) {
      const doc = generateSimpleDoc(i);
      buffer.insert(doc);
      ageIndex.insert(doc.age as number, `doc-${i}`);
    }
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('with 3 B-tree indexes', () => {
    const buffer = new MockBuffer();
    const idx1 = new BTree<number>('age_idx', 'age', 64);
    const idx2 = new BTree<string>('email_idx', 'email', 64);
    const idx3 = new BTree<boolean>('active_idx', 'active', 64);
    for (let i = 0; i < 100; i++) {
      const doc = generateSimpleDoc(i);
      buffer.insert(doc);
      idx1.insert(doc.age as number, `doc-${i}`);
      idx2.insert(doc.email as string, `doc-${i}`);
      idx3.insert(doc.active as boolean, `doc-${i}`);
    }
  }, {
    iterations: 50,
    warmupIterations: 5,
  });
});

// ============================================================================
// Parquet Serialization (Flush) Benchmarks
// ============================================================================

describe('parquet serialization', () => {
  // Pre-generate test data
  const batch100 = Array.from({ length: 100 }, (_, i) => ({
    _id: `doc-${i}`,
    _seq: i + 1,
    _op: 'i' as const,
    doc: generateSimpleDoc(i),
  }));

  const batch1000 = Array.from({ length: 1000 }, (_, i) => ({
    _id: `doc-${i}`,
    _seq: i + 1,
    _op: 'i' as const,
    doc: generateSimpleDoc(i),
  }));

  bench('writeParquet - 100 docs', () => {
    writeParquet(batch100);
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('writeParquet - 1000 docs', () => {
    writeParquet(batch1000);
  }, {
    iterations: 20,
    warmupIterations: 3,
  });
});
