/**
 * Memory Benchmark - Vitest bench suite
 *
 * Measures memory-related performance for MongoLake operations including:
 * - Memory usage baseline
 * - Memory growth over 1000 operations
 * - Large document handling (1MB, 10MB documents)
 * - Memory cleanup after operations
 *
 * Run with: pnpm run benchmark:vitest
 */

import { bench, describe, beforeAll, afterAll, beforeEach } from 'vitest';
import { ClientSession } from '../../src/session/index.js';
import { BTree } from '../../src/index/btree.js';
import { writeParquet, readParquet } from '../../src/parquet/io.js';
import {
  generateSimpleDoc,
  generateMediumDoc,
  generateLargeDoc,
  generateDocOfSize,
} from './utils.js';

// ============================================================================
// Memory Utilities
// ============================================================================

/**
 * Get current heap used memory in bytes.
 * Returns 0 if memory measurement is not available.
 */
function getHeapUsed(): number {
  if (typeof process !== 'undefined' && process.memoryUsage) {
    return process.memoryUsage().heapUsed;
  }
  return 0;
}

/**
 * Force garbage collection if available.
 */
function forceGC(): void {
  if (typeof global !== 'undefined' && typeof global.gc === 'function') {
    global.gc();
  }
}

/**
 * Generate a document with approximately the specified size in bytes.
 */
function generateLargeDocOfSize(sizeBytes: number): Record<string, unknown> {
  const baseDoc = {
    _id: `large-doc-${Date.now()}`,
    timestamp: new Date().toISOString(),
    type: 'benchmark',
  };

  // Calculate padding needed
  const baseSize = JSON.stringify(baseDoc).length;
  const paddingNeeded = Math.max(0, sizeBytes - baseSize - 20);

  // Create padding with some structure to simulate real data
  const chunkSize = 1000;
  const chunks = Math.ceil(paddingNeeded / chunkSize);
  const dataChunks: string[] = [];

  for (let i = 0; i < chunks; i++) {
    dataChunks.push('x'.repeat(Math.min(chunkSize, paddingNeeded - i * chunkSize)));
  }

  return {
    ...baseDoc,
    data: dataChunks,
    metadata: {
      size: sizeBytes,
      chunks: chunks,
    },
  };
}

// ============================================================================
// Mock Buffer for Memory Testing
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
// Memory Usage Baseline Benchmarks
// ============================================================================

describe('memory usage baseline', () => {
  bench('baseline - create empty buffer', () => {
    const buffer = new MockBuffer();
    return buffer.size;
  }, {
    iterations: 1000,
    warmupIterations: 100,
  });

  bench('baseline - create empty session', () => {
    const session = new ClientSession();
    return session.id;
  }, {
    iterations: 1000,
    warmupIterations: 100,
  });

  bench('baseline - create empty BTree index', () => {
    const index = new BTree<number>('test_idx', 'field', 64);
    return index.size;
  }, {
    iterations: 500,
    warmupIterations: 50,
  });

  bench('baseline - allocate 1KB string', () => {
    const data = 'x'.repeat(1024);
    return data.length;
  }, {
    iterations: 1000,
    warmupIterations: 100,
  });

  bench('baseline - allocate 10KB string', () => {
    const data = 'x'.repeat(10 * 1024);
    return data.length;
  }, {
    iterations: 500,
    warmupIterations: 50,
  });

  bench('baseline - allocate 100KB string', () => {
    const data = 'x'.repeat(100 * 1024);
    return data.length;
  }, {
    iterations: 100,
    warmupIterations: 10,
  });
});

// ============================================================================
// Memory Growth Over 1000 Operations
// ============================================================================

describe('memory growth over 1000 operations', () => {
  bench('1000 simple document inserts', () => {
    const buffer = new MockBuffer();
    for (let i = 0; i < 1000; i++) {
      buffer.insert(generateSimpleDoc(i));
    }
    return buffer.size;
  }, {
    iterations: 20,
    warmupIterations: 3,
  });

  bench('1000 medium document inserts', () => {
    const buffer = new MockBuffer();
    for (let i = 0; i < 1000; i++) {
      buffer.insert(generateMediumDoc(i));
    }
    return buffer.size;
  }, {
    iterations: 20,
    warmupIterations: 3,
  });

  bench('1000 large document inserts', () => {
    const buffer = new MockBuffer();
    for (let i = 0; i < 1000; i++) {
      buffer.insert(generateLargeDoc(i));
    }
    return buffer.size;
  }, {
    iterations: 10,
    warmupIterations: 2,
  });

  bench('1000 session operations with buffering', () => {
    const session = new ClientSession();
    session.startTransaction();
    for (let i = 0; i < 1000; i++) {
      session.bufferOperation({
        type: 'insert',
        database: 'testdb',
        collection: 'testcoll',
        document: generateSimpleDoc(i),
      });
    }
    return session.operationCount;
  }, {
    iterations: 20,
    warmupIterations: 3,
  });

  bench('1000 BTree index inserts', () => {
    const index = new BTree<number>('age_idx', 'age', 64);
    for (let i = 0; i < 1000; i++) {
      index.insert(20 + (i % 50), `doc-${i}`);
    }
    return index.size;
  }, {
    iterations: 20,
    warmupIterations: 3,
  });

  bench('1000 operations with index + buffer', () => {
    const buffer = new MockBuffer();
    const index = new BTree<number>('age_idx', 'age', 64);

    for (let i = 0; i < 1000; i++) {
      const doc = generateSimpleDoc(i);
      buffer.insert(doc);
      index.insert(doc.age as number, `doc-${i}`);
    }

    return buffer.size + index.size;
  }, {
    iterations: 20,
    warmupIterations: 3,
  });
});

// ============================================================================
// Large Document Handling - 1MB Documents
// ============================================================================

describe('large document handling (1MB documents)', () => {
  const ONE_MB = 1024 * 1024;

  bench('create single 1MB document', () => {
    const doc = generateLargeDocOfSize(ONE_MB);
    return JSON.stringify(doc).length;
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('insert 1MB document into buffer', () => {
    const buffer = new MockBuffer();
    const doc = generateLargeDocOfSize(ONE_MB);
    buffer.insert(doc);
    return buffer.size;
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('insert 10 x 1MB documents', () => {
    const buffer = new MockBuffer();
    for (let i = 0; i < 10; i++) {
      const doc = {
        ...generateLargeDocOfSize(ONE_MB),
        _id: `doc-${i}`,
      };
      buffer.insert(doc);
    }
    return buffer.size;
  }, {
    iterations: 10,
    warmupIterations: 2,
  });

  bench('serialize 1MB document to JSON', () => {
    const doc = generateLargeDocOfSize(ONE_MB);
    const json = JSON.stringify(doc);
    return json.length;
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('parse 1MB JSON document', () => {
    const doc = generateLargeDocOfSize(ONE_MB);
    const json = JSON.stringify(doc);
    const parsed = JSON.parse(json);
    return Object.keys(parsed).length;
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('buffer 1MB document in session', () => {
    const session = new ClientSession();
    session.startTransaction();
    const doc = generateLargeDocOfSize(ONE_MB);
    session.bufferOperation({
      type: 'insert',
      database: 'testdb',
      collection: 'testcoll',
      document: doc,
    });
    return session.operationCount;
  }, {
    iterations: 50,
    warmupIterations: 5,
  });
});

// ============================================================================
// Large Document Handling - 10MB Documents
// ============================================================================

describe('large document handling (10MB documents)', () => {
  const TEN_MB = 10 * 1024 * 1024;

  bench('create single 10MB document', () => {
    const doc = generateLargeDocOfSize(TEN_MB);
    return JSON.stringify(doc).length;
  }, {
    iterations: 10,
    warmupIterations: 2,
  });

  bench('insert 10MB document into buffer', () => {
    const buffer = new MockBuffer();
    const doc = generateLargeDocOfSize(TEN_MB);
    buffer.insert(doc);
    return buffer.size;
  }, {
    iterations: 10,
    warmupIterations: 2,
  });

  bench('serialize 10MB document to JSON', () => {
    const doc = generateLargeDocOfSize(TEN_MB);
    const json = JSON.stringify(doc);
    return json.length;
  }, {
    iterations: 10,
    warmupIterations: 2,
  });

  bench('buffer 10MB document in session', () => {
    const session = new ClientSession();
    session.startTransaction();
    const doc = generateLargeDocOfSize(TEN_MB);
    session.bufferOperation({
      type: 'insert',
      database: 'testdb',
      collection: 'testcoll',
      document: doc,
    });
    return session.operationCount;
  }, {
    iterations: 10,
    warmupIterations: 2,
  });
});

// ============================================================================
// Memory Cleanup After Operations
// ============================================================================

describe('memory cleanup after operations', () => {
  bench('buffer clear after 1000 inserts', () => {
    const buffer = new MockBuffer();
    for (let i = 0; i < 1000; i++) {
      buffer.insert(generateSimpleDoc(i));
    }
    buffer.clear();
    return buffer.size;
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('session end after 1000 buffered ops', async () => {
    const session = new ClientSession();
    session.startTransaction();
    for (let i = 0; i < 1000; i++) {
      session.bufferOperation({
        type: 'insert',
        database: 'testdb',
        collection: 'testcoll',
        document: generateSimpleDoc(i),
      });
    }
    await session.endSession();
    return session.hasEnded;
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('abort transaction with 1000 ops', async () => {
    const session = new ClientSession();
    session.startTransaction();
    for (let i = 0; i < 1000; i++) {
      session.bufferOperation({
        type: 'insert',
        database: 'testdb',
        collection: 'testcoll',
        document: generateSimpleDoc(i),
      });
    }
    await session.abortTransaction();
    return session.operationCount; // Should be 0 after abort
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('clear BTree with 1000 entries', () => {
    const index = new BTree<number>('age_idx', 'age', 64);
    for (let i = 0; i < 1000; i++) {
      index.insert(20 + (i % 50), `doc-${i}`);
    }
    index.clear();
    return index.size;
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('repeated buffer cycles (create, fill, clear)', () => {
    const buffer = new MockBuffer();
    for (let cycle = 0; cycle < 10; cycle++) {
      for (let i = 0; i < 100; i++) {
        buffer.insert(generateSimpleDoc(i));
      }
      buffer.clear();
    }
    return buffer.size;
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('repeated session cycles (start, buffer, abort)', async () => {
    const session = new ClientSession();

    for (let cycle = 0; cycle < 10; cycle++) {
      session.startTransaction();
      for (let i = 0; i < 100; i++) {
        session.bufferOperation({
          type: 'insert',
          database: 'testdb',
          collection: 'testcoll',
          document: generateSimpleDoc(i),
        });
      }
      await session.abortTransaction();
    }

    return session.txnNumber;
  }, {
    iterations: 30,
    warmupIterations: 3,
  });
});

// ============================================================================
// Parquet Memory Usage
// ============================================================================

describe('parquet memory usage', () => {
  bench('writeParquet - 100 simple docs', () => {
    const rows = Array.from({ length: 100 }, (_, i) => ({
      _id: `doc-${i}`,
      _seq: i + 1,
      _op: 'i' as const,
      doc: generateSimpleDoc(i),
    }));
    const parquet = writeParquet(rows);
    return parquet.length;
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('writeParquet - 100 medium docs', () => {
    const rows = Array.from({ length: 100 }, (_, i) => ({
      _id: `doc-${i}`,
      _seq: i + 1,
      _op: 'i' as const,
      doc: generateMediumDoc(i),
    }));
    const parquet = writeParquet(rows);
    return parquet.length;
  }, {
    iterations: 30,
    warmupIterations: 3,
  });

  bench('writeParquet - 1000 simple docs', () => {
    const rows = Array.from({ length: 1000 }, (_, i) => ({
      _id: `doc-${i}`,
      _seq: i + 1,
      _op: 'i' as const,
      doc: generateSimpleDoc(i),
    }));
    const parquet = writeParquet(rows);
    return parquet.length;
  }, {
    iterations: 20,
    warmupIterations: 3,
  });

  bench('readParquet - 100 docs round-trip', async () => {
    const rows = Array.from({ length: 100 }, (_, i) => ({
      _id: `doc-${i}`,
      _seq: i + 1,
      _op: 'i' as const,
      doc: generateSimpleDoc(i),
    }));
    const parquet = writeParquet(rows);
    const readRows = await readParquet(parquet);
    return readRows.length;
  }, {
    iterations: 30,
    warmupIterations: 3,
  });
});

// ============================================================================
// Document Size Comparison
// ============================================================================

describe('document size comparison', () => {
  bench('simple doc (~200 bytes) x 1000', () => {
    const buffer = new MockBuffer();
    for (let i = 0; i < 1000; i++) {
      buffer.insert(generateSimpleDoc(i));
    }
    return buffer.size;
  }, {
    iterations: 30,
    warmupIterations: 3,
  });

  bench('medium doc (~1KB) x 1000', () => {
    const buffer = new MockBuffer();
    for (let i = 0; i < 1000; i++) {
      buffer.insert(generateMediumDoc(i));
    }
    return buffer.size;
  }, {
    iterations: 20,
    warmupIterations: 2,
  });

  bench('large doc (~5KB) x 1000', () => {
    const buffer = new MockBuffer();
    for (let i = 0; i < 1000; i++) {
      buffer.insert(generateLargeDoc(i));
    }
    return buffer.size;
  }, {
    iterations: 10,
    warmupIterations: 2,
  });

  bench('10KB doc x 100', () => {
    const buffer = new MockBuffer();
    for (let i = 0; i < 100; i++) {
      buffer.insert(generateDocOfSize(i, 10 * 1024));
    }
    return buffer.size;
  }, {
    iterations: 20,
    warmupIterations: 2,
  });

  bench('100KB doc x 10', () => {
    const buffer = new MockBuffer();
    for (let i = 0; i < 10; i++) {
      buffer.insert(generateDocOfSize(i, 100 * 1024));
    }
    return buffer.size;
  }, {
    iterations: 20,
    warmupIterations: 2,
  });
});

// ============================================================================
// Array and Map Operations
// ============================================================================

describe('collection memory operations', () => {
  bench('Map with 10000 string keys', () => {
    const map = new Map<string, number>();
    for (let i = 0; i < 10000; i++) {
      map.set(`key-${i}`, i);
    }
    return map.size;
  }, {
    iterations: 30,
    warmupIterations: 3,
  });

  bench('Map clear after 10000 entries', () => {
    const map = new Map<string, number>();
    for (let i = 0; i < 10000; i++) {
      map.set(`key-${i}`, i);
    }
    map.clear();
    return map.size;
  }, {
    iterations: 30,
    warmupIterations: 3,
  });

  bench('Array with 10000 objects', () => {
    const arr: Array<{ id: number; value: string }> = [];
    for (let i = 0; i < 10000; i++) {
      arr.push({ id: i, value: `value-${i}` });
    }
    return arr.length;
  }, {
    iterations: 30,
    warmupIterations: 3,
  });

  bench('Array splice (clear) after 10000 entries', () => {
    const arr: Array<{ id: number; value: string }> = [];
    for (let i = 0; i < 10000; i++) {
      arr.push({ id: i, value: `value-${i}` });
    }
    arr.length = 0;
    return arr.length;
  }, {
    iterations: 30,
    warmupIterations: 3,
  });

  bench('Set with 10000 strings', () => {
    const set = new Set<string>();
    for (let i = 0; i < 10000; i++) {
      set.add(`item-${i}`);
    }
    return set.size;
  }, {
    iterations: 30,
    warmupIterations: 3,
  });
});
