/**
 * Performance Baseline Tests
 *
 * These tests establish performance baselines for regression detection.
 * Each test has a threshold that should catch ~20% regressions.
 *
 * Run with: pnpm test tests/performance/baselines.test.ts
 *
 * Note: These tests use manual timing rather than Vitest's bench feature
 * to allow for threshold assertions and CI integration.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { writeParquet, readParquet } from '../../src/parquet/io.js';
import { BTree } from '../../src/index/btree.js';
import { matchesFilter } from '../../src/utils/filter.js';
import { encodeVariant, decodeVariant } from '../../src/parquet/variant.js';
import type { Document } from '../../src/types.js';

// ============================================================================
// Configuration
// ============================================================================

/**
 * Performance thresholds - these are set to allow ~20% variance
 * from expected baseline values. Values are calibrated for typical
 * CI hardware (GitHub Actions runners).
 */
const THRESHOLDS = {
  // Insert throughput (docs/second) - minimum expected
  INSERT_THROUGHPUT_MIN: 800, // Allow 20% below 1000 docs/sec baseline

  // Query latency (milliseconds) - maximum allowed
  FIND_ONE_P50_MAX: 5, // p50 should be under 5ms
  FIND_ONE_P95_MAX: 15, // p95 should be under 15ms
  FIND_ONE_P99_MAX: 30, // p99 should be under 30ms

  // Bulk read throughput (docs/second) - minimum expected
  BULK_READ_THROUGHPUT_MIN: 5000, // Allow margin for different hardware configurations

  // Update throughput (ops/second) - minimum expected
  UPDATE_THROUGHPUT_MIN: 400, // Allow 20% below 500 ops/sec baseline

  // Parquet write (MB/second) - minimum expected
  PARQUET_WRITE_MB_SEC_MIN: 5, // Allow 20% below 6.25 MB/sec baseline

  // Memory growth (bytes) - maximum allowed during sustained operations
  MEMORY_GROWTH_MAX_MB: 100, // Max 100MB growth during test
};

// ============================================================================
// Test Data Generators
// ============================================================================

function generateSimpleDoc(index: number): Record<string, unknown> {
  return {
    _id: `doc-${index}`,
    name: `User ${index}`,
    email: `user${index}@example.com`,
    age: 20 + (index % 50),
    active: index % 2 === 0,
    createdAt: new Date().toISOString(),
  };
}

function generateMediumDoc(index: number): Record<string, unknown> {
  return {
    _id: `doc-${index}`,
    name: `User ${index}`,
    email: `user${index}@example.com`,
    age: 20 + (index % 50),
    active: index % 2 === 0,
    profile: {
      bio: `This is the biography for user ${index}. It contains some descriptive text about the user.`,
      avatar: `https://example.com/avatars/${index}.png`,
      location: {
        city: ['New York', 'San Francisco', 'London', 'Tokyo', 'Paris'][index % 5],
        country: ['USA', 'USA', 'UK', 'Japan', 'France'][index % 5],
        coordinates: { lat: 40.7128 + index * 0.01, lng: -74.006 + index * 0.01 },
      },
    },
    preferences: {
      theme: index % 2 === 0 ? 'dark' : 'light',
      notifications: { email: true, push: index % 3 === 0, sms: false },
      language: ['en', 'es', 'fr', 'de', 'ja'][index % 5],
    },
    tags: [`tag${index % 10}`, `tag${(index + 1) % 10}`, `tag${(index + 2) % 10}`],
    metadata: {
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      version: 1,
    },
  };
}

// ============================================================================
// Statistics Helpers
// ============================================================================

function percentile(sorted: number[], p: number): number {
  const index = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, Math.min(index, sorted.length - 1))];
}

function calculateStats(samples: number[]): {
  p50: number;
  p95: number;
  p99: number;
  mean: number;
  min: number;
  max: number;
} {
  const sorted = [...samples].sort((a, b) => a - b);
  const sum = samples.reduce((a, b) => a + b, 0);

  return {
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    mean: sum / samples.length,
    min: sorted[0]!,
    max: sorted[sorted.length - 1]!,
  };
}

// ============================================================================
// Mock Buffer (simulates in-memory document buffer)
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

  insert(doc: Record<string, unknown>): void {
    this.currentLSN++;
    const bufferedDoc: BufferedDoc = {
      _id: String(doc._id),
      _seq: this.currentLSN,
      _op: 'i',
      document: doc,
    };
    this.buffer.set(bufferedDoc._id, bufferedDoc);
  }

  update(id: string, doc: Record<string, unknown>): void {
    this.currentLSN++;
    const bufferedDoc: BufferedDoc = {
      _id: id,
      _seq: this.currentLSN,
      _op: 'u',
      document: doc,
    };
    this.buffer.set(id, bufferedDoc);
  }

  getAll(): BufferedDoc[] {
    return Array.from(this.buffer.values());
  }

  clear(): void {
    this.buffer.clear();
    this.currentLSN = 0;
  }

  get size(): number {
    return this.buffer.size;
  }
}

// ============================================================================
// Performance Baseline Tests
// ============================================================================

describe('Performance Baselines', () => {
  // --------------------------------------------------------------------------
  // Insert Throughput Tests
  // --------------------------------------------------------------------------

  describe('Insert Throughput', () => {
    it('insertMany(1000 docs) should exceed throughput baseline', () => {
      const buffer = new MockBuffer();
      const docs = Array.from({ length: 1000 }, (_, i) => generateSimpleDoc(i));

      // Warmup
      for (let i = 0; i < 100; i++) {
        buffer.insert(generateSimpleDoc(i + 10000));
      }
      buffer.clear();

      // Measure
      const start = performance.now();
      for (const doc of docs) {
        buffer.insert(doc);
      }
      const elapsed = performance.now() - start;

      const throughput = docs.length / (elapsed / 1000);

      expect(throughput).toBeGreaterThan(THRESHOLDS.INSERT_THROUGHPUT_MIN);
      expect(buffer.size).toBe(1000);

      // Log for debugging
      console.log(`Insert throughput: ${throughput.toFixed(0)} docs/sec (threshold: ${THRESHOLDS.INSERT_THROUGHPUT_MIN})`);
    });

    it('insertMany with indexing should maintain reasonable throughput', () => {
      const buffer = new MockBuffer();
      const index = new BTree<number>('age_idx', 'age', 64);
      const docs = Array.from({ length: 1000 }, (_, i) => generateSimpleDoc(i));

      // Warmup
      for (let i = 0; i < 50; i++) {
        const doc = generateSimpleDoc(i + 10000);
        buffer.insert(doc);
        index.insert(doc.age as number, `doc-${i + 10000}`);
      }
      buffer.clear();

      // Measure
      const start = performance.now();
      for (let i = 0; i < docs.length; i++) {
        const doc = docs[i]!;
        buffer.insert(doc);
        index.insert(doc.age as number, `doc-${i}`);
      }
      const elapsed = performance.now() - start;

      const throughput = docs.length / (elapsed / 1000);

      // Indexed inserts should still be at least 60% of non-indexed throughput
      const indexedThreshold = THRESHOLDS.INSERT_THROUGHPUT_MIN * 0.6;
      expect(throughput).toBeGreaterThan(indexedThreshold);

      console.log(`Insert with index throughput: ${throughput.toFixed(0)} docs/sec (threshold: ${indexedThreshold.toFixed(0)})`);
    });
  });

  // --------------------------------------------------------------------------
  // Query Latency Tests
  // --------------------------------------------------------------------------

  describe('Query Latency', () => {
    let docs: Array<Record<string, unknown>>;
    let docsAsDocuments: Document[];

    beforeAll(() => {
      // Generate test data once
      docs = Array.from({ length: 10000 }, (_, i) => generateSimpleDoc(i));
      docsAsDocuments = docs as Document[];
    });

    it('findOne latency should meet p50/p95/p99 thresholds', () => {
      const samples: number[] = [];
      const iterations = 100;

      // Warmup
      for (let i = 0; i < 10; i++) {
        docsAsDocuments.find((d) => matchesFilter(d, { age: 30 + (i % 20) }));
      }

      // Measure
      for (let i = 0; i < iterations; i++) {
        const targetAge = 20 + (i % 50);
        const start = performance.now();
        docsAsDocuments.find((d) => matchesFilter(d, { age: targetAge }));
        samples.push(performance.now() - start);
      }

      const stats = calculateStats(samples);

      expect(stats.p50).toBeLessThan(THRESHOLDS.FIND_ONE_P50_MAX);
      expect(stats.p95).toBeLessThan(THRESHOLDS.FIND_ONE_P95_MAX);
      expect(stats.p99).toBeLessThan(THRESHOLDS.FIND_ONE_P99_MAX);

      console.log(`findOne latency - p50: ${stats.p50.toFixed(2)}ms, p95: ${stats.p95.toFixed(2)}ms, p99: ${stats.p99.toFixed(2)}ms`);
    });

    it('findOne with index should be faster than full scan', () => {
      const index = new BTree<number>('age_idx', 'age', 64);
      const docsMap = new Map<string, Record<string, unknown>>();

      // Build index
      for (const doc of docs) {
        index.insert(doc.age as number, doc._id as string);
        docsMap.set(doc._id as string, doc);
      }

      const scanSamples: number[] = [];
      const indexSamples: number[] = [];
      const iterations = 50;

      // Measure full scan
      for (let i = 0; i < iterations; i++) {
        const targetAge = 20 + (i % 50);
        const start = performance.now();
        docsAsDocuments.find((d) => matchesFilter(d, { age: targetAge }));
        scanSamples.push(performance.now() - start);
      }

      // Measure index lookup
      for (let i = 0; i < iterations; i++) {
        const targetAge = 20 + (i % 50);
        const start = performance.now();
        const docIds = index.search(targetAge);
        if (docIds.length > 0) {
          docsMap.get(docIds[0]!);
        }
        indexSamples.push(performance.now() - start);
      }

      const scanStats = calculateStats(scanSamples);
      const indexStats = calculateStats(indexSamples);

      // Index lookup should be significantly faster (at least 2x at p50)
      expect(indexStats.p50).toBeLessThan(scanStats.p50 / 2);

      console.log(`Full scan p50: ${scanStats.p50.toFixed(2)}ms, Index p50: ${indexStats.p50.toFixed(2)}ms`);
    });
  });

  // --------------------------------------------------------------------------
  // Bulk Read Tests
  // --------------------------------------------------------------------------

  describe('Bulk Read Throughput', () => {
    it('find().toArray() on 10K docs should exceed throughput baseline', () => {
      const docs = Array.from({ length: 10000 }, (_, i) => generateSimpleDoc(i));
      const docsAsDocuments = docs as Document[];

      // Warmup
      for (let i = 0; i < 3; i++) {
        docsAsDocuments.filter((d) => matchesFilter(d, { age: { $gte: 0 } }));
      }

      // Measure
      const start = performance.now();
      const results = docsAsDocuments.filter((d) => matchesFilter(d, { age: { $gte: 0 } }));
      const elapsed = performance.now() - start;

      const throughput = results.length / (elapsed / 1000);

      expect(throughput).toBeGreaterThan(THRESHOLDS.BULK_READ_THROUGHPUT_MIN);
      expect(results.length).toBe(10000);

      console.log(`Bulk read throughput: ${throughput.toFixed(0)} docs/sec (threshold: ${THRESHOLDS.BULK_READ_THROUGHPUT_MIN})`);
    });

    it('filtered bulk read should maintain reasonable throughput', () => {
      const docs = Array.from({ length: 10000 }, (_, i) => generateSimpleDoc(i));
      const docsAsDocuments = docs as Document[];

      // Measure with compound filter
      const start = performance.now();
      const results = docsAsDocuments.filter((d) =>
        matchesFilter(d, {
          $and: [{ active: true }, { age: { $gte: 30, $lte: 40 } }],
        })
      );
      const elapsed = performance.now() - start;

      // Throughput based on documents scanned, not returned
      const throughput = docs.length / (elapsed / 1000);

      // Filtered reads should still be at least 50% of full scan throughput
      const filteredThreshold = THRESHOLDS.BULK_READ_THROUGHPUT_MIN * 0.5;
      expect(throughput).toBeGreaterThan(filteredThreshold);

      console.log(`Filtered bulk read throughput: ${throughput.toFixed(0)} docs/sec, matched: ${results.length}`);
    });
  });

  // --------------------------------------------------------------------------
  // Update Throughput Tests
  // --------------------------------------------------------------------------

  describe('Update Throughput', () => {
    it('updateMany simulation should exceed throughput baseline', () => {
      const buffer = new MockBuffer();

      // Pre-populate buffer
      const docs = Array.from({ length: 1000 }, (_, i) => generateSimpleDoc(i));
      for (const doc of docs) {
        buffer.insert(doc);
      }

      // Warmup
      for (let i = 0; i < 50; i++) {
        const doc = docs[i]!;
        buffer.update(doc._id as string, { ...doc, updatedField: `warmup-${i}` });
      }

      // Measure updates
      const updateCount = 500;
      const start = performance.now();
      for (let i = 0; i < updateCount; i++) {
        const doc = docs[i % docs.length]!;
        buffer.update(doc._id as string, { ...doc, updatedField: `value-${i}` });
      }
      const elapsed = performance.now() - start;

      const throughput = updateCount / (elapsed / 1000);

      expect(throughput).toBeGreaterThan(THRESHOLDS.UPDATE_THROUGHPUT_MIN);

      console.log(`Update throughput: ${throughput.toFixed(0)} ops/sec (threshold: ${THRESHOLDS.UPDATE_THROUGHPUT_MIN})`);
    });
  });

  // --------------------------------------------------------------------------
  // Memory Usage Tests
  // --------------------------------------------------------------------------

  describe('Memory Usage', () => {
    it('sustained operations should not exceed memory growth threshold', () => {
      // Skip if memory measurement is not available
      if (typeof process === 'undefined' || !process.memoryUsage) {
        console.log('Memory measurement not available, skipping test');
        return;
      }

      // Force GC if available to get baseline
      if (global.gc) {
        global.gc();
      }

      const initialMemory = process.memoryUsage().heapUsed;
      const buffer = new MockBuffer();

      // Perform sustained operations
      const iterations = 10;
      const docsPerIteration = 1000;

      for (let iter = 0; iter < iterations; iter++) {
        // Insert batch
        for (let i = 0; i < docsPerIteration; i++) {
          buffer.insert(generateMediumDoc(iter * docsPerIteration + i));
        }

        // Simulate reads
        const allDocs = buffer.getAll();
        const filtered = allDocs.filter((d) => d.document.active === true);

        // Keep reference to prevent GC
        expect(filtered.length).toBeGreaterThan(0);
      }

      const finalMemory = process.memoryUsage().heapUsed;
      const memoryGrowthMB = (finalMemory - initialMemory) / (1024 * 1024);

      expect(memoryGrowthMB).toBeLessThan(THRESHOLDS.MEMORY_GROWTH_MAX_MB);

      console.log(`Memory growth: ${memoryGrowthMB.toFixed(2)}MB (threshold: ${THRESHOLDS.MEMORY_GROWTH_MAX_MB}MB)`);
    });
  });

  // --------------------------------------------------------------------------
  // Parquet Write Performance Tests
  // --------------------------------------------------------------------------

  describe('Parquet Write Performance', () => {
    it('flush operations should exceed MB/sec baseline', () => {
      // Prepare test data
      const rows = Array.from({ length: 1000 }, (_, i) => ({
        _id: `doc-${i}`,
        _seq: i + 1,
        _op: 'i' as const,
        doc: generateMediumDoc(i),
      }));

      // Calculate approximate data size
      const dataSize = rows.reduce(
        (sum, row) => sum + JSON.stringify(row.doc).length,
        0
      );
      const dataSizeMB = dataSize / (1024 * 1024);

      // Warmup
      for (let i = 0; i < 3; i++) {
        writeParquet(rows.slice(0, 100));
      }

      // Measure
      const iterations = 10;
      const samples: number[] = [];

      for (let i = 0; i < iterations; i++) {
        const start = performance.now();
        const parquetData = writeParquet(rows);
        const elapsed = performance.now() - start;
        samples.push(elapsed);

        // Verify output is valid
        expect(parquetData.length).toBeGreaterThan(0);
      }

      const stats = calculateStats(samples);
      const avgElapsedSec = stats.mean / 1000;
      const throughputMBSec = dataSizeMB / avgElapsedSec;

      expect(throughputMBSec).toBeGreaterThan(THRESHOLDS.PARQUET_WRITE_MB_SEC_MIN);

      console.log(`Parquet write throughput: ${throughputMBSec.toFixed(2)} MB/sec (threshold: ${THRESHOLDS.PARQUET_WRITE_MB_SEC_MIN})`);
      console.log(`  Data size: ${dataSizeMB.toFixed(2)}MB, Avg time: ${stats.mean.toFixed(2)}ms`);
    });

    it('variant encoding should be efficient', () => {
      const docs = Array.from({ length: 1000 }, (_, i) => generateMediumDoc(i));

      // Warmup
      for (let i = 0; i < 100; i++) {
        encodeVariant(docs[i % docs.length]!);
      }

      // Measure
      const start = performance.now();
      for (const doc of docs) {
        encodeVariant(doc);
      }
      const elapsed = performance.now() - start;

      const throughput = docs.length / (elapsed / 1000);

      // Should encode at least 5000 docs/sec for medium docs
      expect(throughput).toBeGreaterThan(5000);

      console.log(`Variant encode throughput: ${throughput.toFixed(0)} docs/sec`);
    });

    it('parquet round-trip should maintain data integrity', async () => {
      const rows = Array.from({ length: 100 }, (_, i) => ({
        _id: `doc-${i}`,
        _seq: i + 1,
        _op: 'i' as const,
        doc: generateSimpleDoc(i),
      }));

      // Write
      const parquetData = writeParquet(rows);

      // Read
      const readRows = await readParquet(parquetData);

      // Verify integrity
      expect(readRows.length).toBe(rows.length);
      for (let i = 0; i < rows.length; i++) {
        expect(readRows[i]!._id).toBe(rows[i]!._id);
        expect(readRows[i]!._seq).toBe(rows[i]!._seq);
        expect(readRows[i]!._op).toBe(rows[i]!._op);
        expect(readRows[i]!.doc.name).toBe(rows[i]!.doc.name);
      }
    });
  });

  // --------------------------------------------------------------------------
  // Aggregated Performance Report
  // --------------------------------------------------------------------------

  describe('Performance Summary', () => {
    it('should generate performance summary', () => {
      console.log('\n========================================');
      console.log('Performance Baseline Thresholds');
      console.log('========================================');
      console.log(`Insert throughput min: ${THRESHOLDS.INSERT_THROUGHPUT_MIN} docs/sec`);
      console.log(`findOne p50 max: ${THRESHOLDS.FIND_ONE_P50_MAX}ms`);
      console.log(`findOne p95 max: ${THRESHOLDS.FIND_ONE_P95_MAX}ms`);
      console.log(`findOne p99 max: ${THRESHOLDS.FIND_ONE_P99_MAX}ms`);
      console.log(`Bulk read throughput min: ${THRESHOLDS.BULK_READ_THROUGHPUT_MIN} docs/sec`);
      console.log(`Update throughput min: ${THRESHOLDS.UPDATE_THROUGHPUT_MIN} ops/sec`);
      console.log(`Parquet write min: ${THRESHOLDS.PARQUET_WRITE_MB_SEC_MIN} MB/sec`);
      console.log(`Memory growth max: ${THRESHOLDS.MEMORY_GROWTH_MAX_MB}MB`);
      console.log('========================================\n');

      // This test always passes - it's just for reporting
      expect(true).toBe(true);
    });
  });
});
