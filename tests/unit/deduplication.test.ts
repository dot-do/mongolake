/**
 * Deduplication Engine Tests
 *
 * Tests for the deduplication engine that handles:
 * - Deduplicating documents by _id
 * - Keeping document with highest _seq for each _id
 * - Filtering out deleted documents (_op='d')
 * - Handling documents from multiple files
 * - Preserving document order
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  DeduplicationEngine,
  deduplicate,
  deduplicateStreaming,
  type DeduplicationOptions,
  type DeduplicationResult,
  type Document,
} from '../../src/deduplication/index.js';

// =============================================================================
// Test Document Type
// =============================================================================

interface TestDocument extends Document {
  _id: string;
  _seq: number;
  _op: 'i' | 'u' | 'd';
  name?: string;
  age?: number;
  email?: string;
  tags?: string[];
  metadata?: Record<string, unknown>;
}

// =============================================================================
// Helper Functions
// =============================================================================

function createDoc(
  id: string,
  seq: number,
  op: 'i' | 'u' | 'd' = 'i',
  data: Partial<Omit<TestDocument, '_id' | '_seq' | '_op'>> = {}
): TestDocument {
  return {
    _id: id,
    _seq: seq,
    _op: op,
    ...data,
  };
}

// =============================================================================
// No Duplicates (Pass Through)
// =============================================================================

describe('DeduplicationEngine - No Duplicates', () => {
  let engine: DeduplicationEngine;

  beforeEach(() => {
    engine = new DeduplicationEngine();
  });

  it('should pass through single document unchanged', () => {
    const docs: TestDocument[] = [createDoc('doc1', 1, 'i', { name: 'Alice' })];

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(1);
    expect(result.documents[0]._id).toBe('doc1');
    expect(result.documents[0].name).toBe('Alice');
    expect(result.stats.inputCount).toBe(1);
    expect(result.stats.outputCount).toBe(1);
    expect(result.stats.duplicatesRemoved).toBe(0);
    expect(result.stats.deletesFiltered).toBe(0);
  });

  it('should pass through multiple unique documents', () => {
    const docs: TestDocument[] = [
      createDoc('doc1', 1, 'i', { name: 'Alice' }),
      createDoc('doc2', 2, 'i', { name: 'Bob' }),
      createDoc('doc3', 3, 'i', { name: 'Charlie' }),
    ];

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(3);
    expect(result.stats.inputCount).toBe(3);
    expect(result.stats.outputCount).toBe(3);
    expect(result.stats.duplicatesRemoved).toBe(0);
  });

  it('should handle empty input', () => {
    const docs: TestDocument[] = [];

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(0);
    expect(result.stats.inputCount).toBe(0);
    expect(result.stats.outputCount).toBe(0);
  });

  it('should preserve all document fields', () => {
    const docs: TestDocument[] = [
      createDoc('doc1', 1, 'i', {
        name: 'Alice',
        age: 30,
        email: 'alice@example.com',
        tags: ['admin', 'user'],
        metadata: { role: 'manager', level: 5 },
      }),
    ];

    const result = engine.deduplicate(docs);

    expect(result.documents[0]).toEqual(docs[0]);
  });
});

// =============================================================================
// Simple Duplicates (Keep Latest _seq)
// =============================================================================

describe('DeduplicationEngine - Simple Duplicates', () => {
  let engine: DeduplicationEngine;

  beforeEach(() => {
    engine = new DeduplicationEngine();
  });

  it('should keep document with higher _seq when duplicate exists', () => {
    const docs: TestDocument[] = [
      createDoc('doc1', 1, 'i', { name: 'Alice v1' }),
      createDoc('doc1', 2, 'u', { name: 'Alice v2' }),
    ];

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(1);
    expect(result.documents[0]._seq).toBe(2);
    expect(result.documents[0].name).toBe('Alice v2');
    expect(result.stats.duplicatesRemoved).toBe(1);
  });

  it('should handle duplicates in reverse order', () => {
    const docs: TestDocument[] = [
      createDoc('doc1', 5, 'u', { name: 'Alice v5' }),
      createDoc('doc1', 2, 'i', { name: 'Alice v2' }),
    ];

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(1);
    expect(result.documents[0]._seq).toBe(5);
    expect(result.documents[0].name).toBe('Alice v5');
  });

  it('should deduplicate across multiple documents', () => {
    const docs: TestDocument[] = [
      createDoc('doc1', 1, 'i', { name: 'Alice v1' }),
      createDoc('doc2', 2, 'i', { name: 'Bob v1' }),
      createDoc('doc1', 3, 'u', { name: 'Alice v2' }),
      createDoc('doc2', 4, 'u', { name: 'Bob v2' }),
    ];

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(2);
    const doc1 = result.documents.find((d) => d._id === 'doc1');
    const doc2 = result.documents.find((d) => d._id === 'doc2');
    expect(doc1?._seq).toBe(3);
    expect(doc2?._seq).toBe(4);
    expect(result.stats.duplicatesRemoved).toBe(2);
  });

  it('should handle duplicates with same _seq (keep last encountered)', () => {
    const docs: TestDocument[] = [
      createDoc('doc1', 1, 'i', { name: 'First' }),
      createDoc('doc1', 1, 'i', { name: 'Second' }),
    ];

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(1);
    // When _seq is equal, keep the last encountered document
    expect(result.documents[0].name).toBe('Second');
  });
});

// =============================================================================
// Multiple Updates to Same Document
// =============================================================================

describe('DeduplicationEngine - Multiple Updates', () => {
  let engine: DeduplicationEngine;

  beforeEach(() => {
    engine = new DeduplicationEngine();
  });

  it('should keep only final version after many updates', () => {
    const docs: TestDocument[] = [
      createDoc('doc1', 1, 'i', { name: 'v1' }),
      createDoc('doc1', 2, 'u', { name: 'v2' }),
      createDoc('doc1', 3, 'u', { name: 'v3' }),
      createDoc('doc1', 4, 'u', { name: 'v4' }),
      createDoc('doc1', 5, 'u', { name: 'v5' }),
    ];

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(1);
    expect(result.documents[0]._seq).toBe(5);
    expect(result.documents[0].name).toBe('v5');
    expect(result.stats.duplicatesRemoved).toBe(4);
  });

  it('should handle interleaved updates to different documents', () => {
    const docs: TestDocument[] = [
      createDoc('a', 1, 'i', { name: 'a1' }),
      createDoc('b', 2, 'i', { name: 'b1' }),
      createDoc('a', 3, 'u', { name: 'a2' }),
      createDoc('c', 4, 'i', { name: 'c1' }),
      createDoc('b', 5, 'u', { name: 'b2' }),
      createDoc('a', 6, 'u', { name: 'a3' }),
      createDoc('c', 7, 'u', { name: 'c2' }),
    ];

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(3);
    const docA = result.documents.find((d) => d._id === 'a');
    const docB = result.documents.find((d) => d._id === 'b');
    const docC = result.documents.find((d) => d._id === 'c');
    expect(docA?._seq).toBe(6);
    expect(docB?._seq).toBe(5);
    expect(docC?._seq).toBe(7);
  });

  it('should handle 100+ updates to same document', () => {
    const docs: TestDocument[] = Array.from({ length: 100 }, (_, i) =>
      createDoc('doc1', i + 1, i === 0 ? 'i' : 'u', { name: `v${i + 1}` })
    );

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(1);
    expect(result.documents[0]._seq).toBe(100);
    expect(result.documents[0].name).toBe('v100');
    expect(result.stats.duplicatesRemoved).toBe(99);
  });

  it('should track field changes across updates', () => {
    const docs: TestDocument[] = [
      createDoc('doc1', 1, 'i', { name: 'Alice', age: 25 }),
      createDoc('doc1', 2, 'u', { name: 'Alice', age: 26 }), // age changed
      createDoc('doc1', 3, 'u', { name: 'Alice Smith', age: 26 }), // name changed
      createDoc('doc1', 4, 'u', { name: 'Alice Smith', age: 26, email: 'alice@test.com' }), // email added
    ];

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(1);
    expect(result.documents[0].name).toBe('Alice Smith');
    expect(result.documents[0].age).toBe(26);
    expect(result.documents[0].email).toBe('alice@test.com');
  });
});

// =============================================================================
// Delete Removes Document
// =============================================================================

describe('DeduplicationEngine - Delete Operations', () => {
  let engine: DeduplicationEngine;

  beforeEach(() => {
    engine = new DeduplicationEngine();
  });

  it('should filter out document with delete operation', () => {
    const docs: TestDocument[] = [
      createDoc('doc1', 1, 'i', { name: 'Alice' }),
      createDoc('doc1', 2, 'd'),
    ];

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(0);
    expect(result.stats.deletesFiltered).toBe(1);
  });

  it('should keep other documents when one is deleted', () => {
    const docs: TestDocument[] = [
      createDoc('doc1', 1, 'i', { name: 'Alice' }),
      createDoc('doc2', 2, 'i', { name: 'Bob' }),
      createDoc('doc1', 3, 'd'),
    ];

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(1);
    expect(result.documents[0]._id).toBe('doc2');
    expect(result.stats.deletesFiltered).toBe(1);
  });

  it('should filter multiple deleted documents', () => {
    const docs: TestDocument[] = [
      createDoc('doc1', 1, 'i'),
      createDoc('doc2', 2, 'i'),
      createDoc('doc3', 3, 'i'),
      createDoc('doc1', 4, 'd'),
      createDoc('doc2', 5, 'd'),
    ];

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(1);
    expect(result.documents[0]._id).toBe('doc3');
    expect(result.stats.deletesFiltered).toBe(2);
  });

  it('should handle delete as only operation', () => {
    const docs: TestDocument[] = [createDoc('doc1', 1, 'd')];

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(0);
    expect(result.stats.deletesFiltered).toBe(1);
  });

  it('should handle delete with updates before it', () => {
    const docs: TestDocument[] = [
      createDoc('doc1', 1, 'i', { name: 'v1' }),
      createDoc('doc1', 2, 'u', { name: 'v2' }),
      createDoc('doc1', 3, 'u', { name: 'v3' }),
      createDoc('doc1', 4, 'd'),
    ];

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(0);
    expect(result.stats.duplicatesRemoved).toBe(3); // All previous versions
    expect(result.stats.deletesFiltered).toBe(1);
  });
});

// =============================================================================
// Insert After Delete (Resurrect)
// =============================================================================

describe('DeduplicationEngine - Resurrect After Delete', () => {
  let engine: DeduplicationEngine;

  beforeEach(() => {
    engine = new DeduplicationEngine();
  });

  it('should resurrect document with insert after delete', () => {
    const docs: TestDocument[] = [
      createDoc('doc1', 1, 'i', { name: 'Original' }),
      createDoc('doc1', 2, 'd'),
      createDoc('doc1', 3, 'i', { name: 'Resurrected' }),
    ];

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(1);
    expect(result.documents[0]._seq).toBe(3);
    expect(result.documents[0].name).toBe('Resurrected');
    expect(result.documents[0]._op).toBe('i');
  });

  it('should handle multiple delete-resurrect cycles', () => {
    const docs: TestDocument[] = [
      createDoc('doc1', 1, 'i', { name: 'v1' }),
      createDoc('doc1', 2, 'd'),
      createDoc('doc1', 3, 'i', { name: 'v2' }),
      createDoc('doc1', 4, 'd'),
      createDoc('doc1', 5, 'i', { name: 'v3' }),
    ];

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(1);
    expect(result.documents[0]._seq).toBe(5);
    expect(result.documents[0].name).toBe('v3');
  });

  it('should respect final state when delete is last', () => {
    const docs: TestDocument[] = [
      createDoc('doc1', 1, 'i', { name: 'v1' }),
      createDoc('doc1', 2, 'd'),
      createDoc('doc1', 3, 'i', { name: 'v2' }),
      createDoc('doc1', 4, 'd'), // Final state is deleted
    ];

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(0);
  });

  it('should handle resurrect with updates', () => {
    const docs: TestDocument[] = [
      createDoc('doc1', 1, 'i', { name: 'v1' }),
      createDoc('doc1', 2, 'd'),
      createDoc('doc1', 3, 'i', { name: 'resurrected' }),
      createDoc('doc1', 4, 'u', { name: 'updated after resurrect' }),
    ];

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(1);
    expect(result.documents[0]._seq).toBe(4);
    expect(result.documents[0].name).toBe('updated after resurrect');
  });

  it('should handle interleaved resurrects for multiple docs', () => {
    const docs: TestDocument[] = [
      createDoc('a', 1, 'i', { name: 'a1' }),
      createDoc('b', 2, 'i', { name: 'b1' }),
      createDoc('a', 3, 'd'),
      createDoc('b', 4, 'd'),
      createDoc('a', 5, 'i', { name: 'a2' }), // a resurrected
      createDoc('c', 6, 'i', { name: 'c1' }),
      createDoc('b', 7, 'i', { name: 'b2' }), // b resurrected
    ];

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(3);
    const docA = result.documents.find((d) => d._id === 'a');
    const docB = result.documents.find((d) => d._id === 'b');
    const docC = result.documents.find((d) => d._id === 'c');
    expect(docA?.name).toBe('a2');
    expect(docB?.name).toBe('b2');
    expect(docC?.name).toBe('c1');
  });
});

// =============================================================================
// Documents from Multiple Files
// =============================================================================

describe('DeduplicationEngine - Multiple Files', () => {
  let engine: DeduplicationEngine;

  beforeEach(() => {
    engine = new DeduplicationEngine();
  });

  it('should deduplicate across file boundaries using _seq', () => {
    const file1: TestDocument[] = [
      createDoc('doc1', 1, 'i', { name: 'File1 v1' }),
      createDoc('doc2', 2, 'i', { name: 'File1 Doc2' }),
    ];
    const file2: TestDocument[] = [
      createDoc('doc1', 5, 'u', { name: 'File2 v2' }), // Higher _seq
      createDoc('doc3', 6, 'i', { name: 'File2 Doc3' }),
    ];

    const result = engine.deduplicateMultiple([file1, file2]);

    expect(result.documents).toHaveLength(3);
    const doc1 = result.documents.find((d) => d._id === 'doc1');
    expect(doc1?._seq).toBe(5);
    expect(doc1?.name).toBe('File2 v2');
  });

  it('should handle three or more files', () => {
    const file1: TestDocument[] = [createDoc('doc1', 1, 'i', { name: 'f1' })];
    const file2: TestDocument[] = [createDoc('doc1', 3, 'u', { name: 'f2' })];
    const file3: TestDocument[] = [createDoc('doc1', 2, 'u', { name: 'f3' })]; // Middle _seq

    const result = engine.deduplicateMultiple([file1, file2, file3]);

    expect(result.documents).toHaveLength(1);
    expect(result.documents[0]._seq).toBe(3); // Highest _seq wins
    expect(result.documents[0].name).toBe('f2');
  });

  it('should handle delete in later file', () => {
    const file1: TestDocument[] = [
      createDoc('doc1', 1, 'i', { name: 'Original' }),
      createDoc('doc2', 2, 'i', { name: 'Doc2' }),
    ];
    const file2: TestDocument[] = [createDoc('doc1', 10, 'd')]; // Delete in later file

    const result = engine.deduplicateMultiple([file1, file2]);

    expect(result.documents).toHaveLength(1);
    expect(result.documents[0]._id).toBe('doc2');
  });

  it('should handle empty files', () => {
    const file1: TestDocument[] = [createDoc('doc1', 1, 'i', { name: 'Doc1' })];
    const file2: TestDocument[] = [];
    const file3: TestDocument[] = [createDoc('doc2', 2, 'i', { name: 'Doc2' })];

    const result = engine.deduplicateMultiple([file1, file2, file3]);

    expect(result.documents).toHaveLength(2);
  });

  it('should track source file in metadata', () => {
    const file1: TestDocument[] = [createDoc('doc1', 1, 'i', { name: 'File1' })];
    const file2: TestDocument[] = [createDoc('doc1', 5, 'u', { name: 'File2' })];

    const options: DeduplicationOptions = { trackSourceFile: true };
    const result = engine.deduplicateMultiple([file1, file2], options);

    expect(result.documents[0]._sourceFile).toBe(1); // 0-indexed file
  });

  it('should handle 10 files with overlapping documents', () => {
    const files = Array.from({ length: 10 }, (_, fileIndex) =>
      Array.from({ length: 5 }, (_, docIndex) =>
        createDoc(`doc${docIndex}`, fileIndex * 10 + docIndex, fileIndex === 0 ? 'i' : 'u', {
          name: `f${fileIndex}-d${docIndex}`,
        })
      )
    );

    const result = engine.deduplicateMultiple(files);

    // Should have 5 unique documents, each from file 9 (highest _seq)
    expect(result.documents).toHaveLength(5);
    for (let i = 0; i < 5; i++) {
      const doc = result.documents.find((d) => d._id === `doc${i}`);
      expect(doc?._seq).toBe(90 + i); // 9 * 10 + i
    }
  });
});

// =============================================================================
// Document Order Preservation
// =============================================================================

describe('DeduplicationEngine - Order Preservation', () => {
  let engine: DeduplicationEngine;

  beforeEach(() => {
    engine = new DeduplicationEngine();
  });

  it('should preserve insertion order by default', () => {
    const docs: TestDocument[] = [
      createDoc('c', 3, 'i'),
      createDoc('a', 1, 'i'),
      createDoc('b', 2, 'i'),
    ];

    const result = engine.deduplicate(docs, { orderBy: 'insertion' });

    expect(result.documents.map((d) => d._id)).toEqual(['c', 'a', 'b']);
  });

  it('should sort by _id when specified', () => {
    const docs: TestDocument[] = [
      createDoc('c', 3, 'i'),
      createDoc('a', 1, 'i'),
      createDoc('b', 2, 'i'),
    ];

    const result = engine.deduplicate(docs, { orderBy: '_id' });

    expect(result.documents.map((d) => d._id)).toEqual(['a', 'b', 'c']);
  });

  it('should sort by _seq when specified', () => {
    const docs: TestDocument[] = [
      createDoc('c', 3, 'i'),
      createDoc('a', 1, 'i'),
      createDoc('b', 2, 'i'),
    ];

    const result = engine.deduplicate(docs, { orderBy: '_seq' });

    expect(result.documents.map((d) => d._id)).toEqual(['a', 'b', 'c']);
  });

  it('should preserve first-seen order for duplicates', () => {
    const docs: TestDocument[] = [
      createDoc('b', 1, 'i'),
      createDoc('a', 2, 'i'),
      createDoc('b', 3, 'u'), // Update to b, but b was seen first
      createDoc('c', 4, 'i'),
    ];

    const result = engine.deduplicate(docs, { orderBy: 'insertion' });

    // b was first seen, then a, then c
    expect(result.documents.map((d) => d._id)).toEqual(['b', 'a', 'c']);
  });

  it('should handle custom sort function', () => {
    const docs: TestDocument[] = [
      createDoc('doc1', 1, 'i', { name: 'Zara' }),
      createDoc('doc2', 2, 'i', { name: 'Alice' }),
      createDoc('doc3', 3, 'i', { name: 'Mike' }),
    ];

    const result = engine.deduplicate(docs, {
      orderBy: 'custom',
      compareFn: (a, b) => (a.name || '').localeCompare(b.name || ''),
    });

    expect(result.documents.map((d) => d.name)).toEqual(['Alice', 'Mike', 'Zara']);
  });

  it('should sort descending when specified', () => {
    const docs: TestDocument[] = [
      createDoc('a', 1, 'i'),
      createDoc('b', 2, 'i'),
      createDoc('c', 3, 'i'),
    ];

    const result = engine.deduplicate(docs, { orderBy: '_id', orderDirection: 'desc' });

    expect(result.documents.map((d) => d._id)).toEqual(['c', 'b', 'a']);
  });
});

// =============================================================================
// Large Dataset Deduplication
// =============================================================================

describe('DeduplicationEngine - Large Datasets', () => {
  let engine: DeduplicationEngine;

  beforeEach(() => {
    engine = new DeduplicationEngine();
  });

  it('should handle 10,000 unique documents', () => {
    const docs: TestDocument[] = Array.from({ length: 10000 }, (_, i) =>
      createDoc(`doc${i}`, i + 1, 'i', { name: `User ${i}` })
    );

    const startTime = performance.now();
    const result = engine.deduplicate(docs);
    const elapsed = performance.now() - startTime;

    expect(result.documents).toHaveLength(10000);
    expect(result.stats.duplicatesRemoved).toBe(0);
    expect(elapsed).toBeLessThan(5000); // Should complete within 5 seconds
  });

  it('should handle 10,000 documents with 50% duplicates', () => {
    const docs: TestDocument[] = Array.from({ length: 10000 }, (_, i) =>
      createDoc(`doc${i % 5000}`, i + 1, i < 5000 ? 'i' : 'u', { name: `v${i + 1}` })
    );

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(5000);
    expect(result.stats.duplicatesRemoved).toBe(5000);
  });

  it('should handle 50,000 documents with high duplication', () => {
    // 50,000 documents but only 100 unique _ids
    const docs: TestDocument[] = Array.from({ length: 50000 }, (_, i) =>
      createDoc(`doc${i % 100}`, i + 1, i < 100 ? 'i' : 'u', { name: `v${i}` })
    );

    const startTime = performance.now();
    const result = engine.deduplicate(docs);
    const elapsed = performance.now() - startTime;

    expect(result.documents).toHaveLength(100);
    expect(result.stats.duplicatesRemoved).toBe(49900);
    expect(elapsed).toBeLessThan(10000); // Should complete within 10 seconds
  });

  it('should handle documents with large payloads', () => {
    const largePayload = {
      description: 'x'.repeat(10000),
      tags: Array.from({ length: 100 }, (_, i) => `tag${i}`),
      metadata: Object.fromEntries(Array.from({ length: 50 }, (_, i) => [`key${i}`, `value${i}`])),
    };

    const docs: TestDocument[] = Array.from({ length: 1000 }, (_, i) =>
      createDoc(`doc${i % 100}`, i + 1, i < 100 ? 'i' : 'u', largePayload)
    );

    const result = engine.deduplicate(docs);

    expect(result.documents).toHaveLength(100);
  });

  it('should report memory usage for large datasets', () => {
    const docs: TestDocument[] = Array.from({ length: 10000 }, (_, i) =>
      createDoc(`doc${i}`, i + 1, 'i', { name: `User ${i}` })
    );

    const result = engine.deduplicate(docs, { trackMemory: true });

    expect(result.stats.peakMemoryBytes).toBeDefined();
    expect(result.stats.peakMemoryBytes).toBeGreaterThan(0);
  });
});

// =============================================================================
// Streaming Deduplication
// =============================================================================

describe('DeduplicationEngine - Streaming', () => {
  it('should deduplicate with async iterator input', async () => {
    async function* generateDocs(): AsyncGenerator<TestDocument> {
      yield createDoc('doc1', 1, 'i', { name: 'v1' });
      yield createDoc('doc2', 2, 'i', { name: 'Bob' });
      yield createDoc('doc1', 3, 'u', { name: 'v2' });
    }

    const result = await deduplicateStreaming(generateDocs());

    expect(result.documents).toHaveLength(2);
    const doc1 = result.documents.find((d) => d._id === 'doc1');
    expect(doc1?.name).toBe('v2');
  });

  it('should process documents in batches', async () => {
    const batchSizes: number[] = [];

    async function* generateDocs(): AsyncGenerator<TestDocument> {
      for (let i = 0; i < 1000; i++) {
        yield createDoc(`doc${i % 100}`, i + 1, i < 100 ? 'i' : 'u', { name: `v${i}` });
      }
    }

    const result = await deduplicateStreaming(generateDocs(), {
      batchSize: 100,
      onBatch: (batch) => batchSizes.push(batch.length),
    });

    expect(result.documents).toHaveLength(100);
    expect(batchSizes.length).toBeGreaterThan(0);
    expect(batchSizes.every((s) => s <= 100)).toBe(true);
  });

  it('should handle backpressure with slow consumer', async () => {
    let processedCount = 0;

    async function* generateDocs(): AsyncGenerator<TestDocument> {
      for (let i = 0; i < 100; i++) {
        yield createDoc(`doc${i}`, i + 1, 'i');
      }
    }

    const result = await deduplicateStreaming(generateDocs(), {
      batchSize: 10,
      onBatch: async () => {
        await new Promise((resolve) => setTimeout(resolve, 10));
        processedCount++;
      },
    });

    expect(result.documents).toHaveLength(100);
    expect(processedCount).toBe(10); // 100 docs / batch size 10
  });

  it('should support abort signal', async () => {
    const controller = new AbortController();

    async function* generateDocs(): AsyncGenerator<TestDocument> {
      for (let i = 0; i < 10000; i++) {
        yield createDoc(`doc${i}`, i + 1, 'i');
      }
    }

    // Abort after processing some documents
    setTimeout(() => controller.abort(), 50);

    await expect(
      deduplicateStreaming(generateDocs(), { signal: controller.signal })
    ).rejects.toThrow('Aborted');
  });

  it('should emit progress events', async () => {
    const progressEvents: Array<{ processed: number; unique: number }> = [];

    async function* generateDocs(): AsyncGenerator<TestDocument> {
      for (let i = 0; i < 500; i++) {
        yield createDoc(`doc${i % 100}`, i + 1, i < 100 ? 'i' : 'u');
      }
    }

    const result = await deduplicateStreaming(generateDocs(), {
      onProgress: (processed, unique) => progressEvents.push({ processed, unique }),
      progressInterval: 100,
    });

    expect(result.documents).toHaveLength(100);
    expect(progressEvents.length).toBeGreaterThan(0);
    expect(progressEvents[progressEvents.length - 1].processed).toBe(500);
  });

  it('should handle streaming from multiple sources', async () => {
    async function* source1(): AsyncGenerator<TestDocument> {
      yield createDoc('doc1', 1, 'i', { name: 'Source1' });
      yield createDoc('doc2', 2, 'i', { name: 'Source1 Doc2' });
    }

    async function* source2(): AsyncGenerator<TestDocument> {
      yield createDoc('doc1', 5, 'u', { name: 'Source2' }); // Higher _seq
      yield createDoc('doc3', 6, 'i', { name: 'Source2 Doc3' });
    }

    const result = await deduplicateStreaming(
      (async function* () {
        yield* source1();
        yield* source2();
      })()
    );

    expect(result.documents).toHaveLength(3);
    const doc1 = result.documents.find((d) => d._id === 'doc1');
    expect(doc1?.name).toBe('Source2');
  });

  it('should handle errors in async generator gracefully', async () => {
    async function* generateDocs(): AsyncGenerator<TestDocument> {
      yield createDoc('doc1', 1, 'i');
      throw new Error('Stream error');
    }

    await expect(deduplicateStreaming(generateDocs())).rejects.toThrow('Stream error');
  });
});

// =============================================================================
// Static Function API
// =============================================================================

describe('deduplicate - Static Function', () => {
  it('should work as a standalone function', () => {
    const docs: TestDocument[] = [
      createDoc('doc1', 1, 'i', { name: 'v1' }),
      createDoc('doc1', 2, 'u', { name: 'v2' }),
    ];

    const result = deduplicate(docs);

    expect(result.documents).toHaveLength(1);
    expect(result.documents[0].name).toBe('v2');
  });

  it('should accept options', () => {
    const docs: TestDocument[] = [
      createDoc('b', 1, 'i'),
      createDoc('a', 2, 'i'),
    ];

    const result = deduplicate(docs, { orderBy: '_id' });

    expect(result.documents.map((d) => d._id)).toEqual(['a', 'b']);
  });
});

// =============================================================================
// Error Handling
// =============================================================================

describe('DeduplicationEngine - Error Handling', () => {
  let engine: DeduplicationEngine;

  beforeEach(() => {
    engine = new DeduplicationEngine();
  });

  it('should throw on document without _id', () => {
    const docs = [{ _seq: 1, _op: 'i', name: 'Invalid' }] as unknown as TestDocument[];

    expect(() => engine.deduplicate(docs)).toThrow(/missing.*_id/i);
  });

  it('should throw on document without _seq', () => {
    const docs = [{ _id: 'doc1', _op: 'i', name: 'Invalid' }] as unknown as TestDocument[];

    expect(() => engine.deduplicate(docs)).toThrow(/missing.*_seq/i);
  });

  it('should throw on document without _op', () => {
    const docs = [{ _id: 'doc1', _seq: 1, name: 'Invalid' }] as unknown as TestDocument[];

    expect(() => engine.deduplicate(docs)).toThrow(/missing.*_op/i);
  });

  it('should throw on invalid _op value', () => {
    const docs = [{ _id: 'doc1', _seq: 1, _op: 'x', name: 'Invalid' }] as unknown as TestDocument[];

    expect(() => engine.deduplicate(docs)).toThrow(/invalid.*_op/i);
  });

  it('should throw on negative _seq', () => {
    const docs = [createDoc('doc1', -1, 'i')];

    expect(() => engine.deduplicate(docs)).toThrow(/_seq.*non-negative/i);
  });

  it('should throw on non-string _id', () => {
    const docs = [{ _id: 123, _seq: 1, _op: 'i' }] as unknown as TestDocument[];

    expect(() => engine.deduplicate(docs)).toThrow(/_id.*string/i);
  });

  it('should validate options', () => {
    const docs: TestDocument[] = [createDoc('doc1', 1, 'i')];

    expect(() => engine.deduplicate(docs, { orderBy: 'invalid' as any })).toThrow(/orderBy/i);
  });
});

// =============================================================================
// Statistics and Reporting
// =============================================================================

describe('DeduplicationEngine - Statistics', () => {
  let engine: DeduplicationEngine;

  beforeEach(() => {
    engine = new DeduplicationEngine();
  });

  it('should report complete statistics', () => {
    const docs: TestDocument[] = [
      createDoc('doc1', 1, 'i'),
      createDoc('doc2', 2, 'i'),
      createDoc('doc1', 3, 'u'),
      createDoc('doc3', 4, 'i'),
      createDoc('doc2', 5, 'd'),
    ];

    const result = engine.deduplicate(docs);

    expect(result.stats).toEqual({
      inputCount: 5,
      outputCount: 2,
      duplicatesRemoved: 2, // doc1 v1, doc2 before delete
      deletesFiltered: 1, // doc2
      processingTimeMs: expect.any(Number),
    });
  });

  it('should track processing time', () => {
    const docs: TestDocument[] = Array.from({ length: 1000 }, (_, i) =>
      createDoc(`doc${i}`, i + 1, 'i')
    );

    const result = engine.deduplicate(docs);

    expect(result.stats.processingTimeMs).toBeGreaterThan(0);
    expect(result.stats.processingTimeMs).toBeLessThan(5000);
  });

  it('should report unique _id count', () => {
    const docs: TestDocument[] = [
      createDoc('a', 1, 'i'),
      createDoc('b', 2, 'i'),
      createDoc('a', 3, 'u'),
      createDoc('c', 4, 'i'),
      createDoc('b', 5, 'u'),
    ];

    const result = engine.deduplicate(docs);

    expect(result.stats.uniqueIds).toBe(3);
    expect(result.stats.outputCount).toBe(3);
  });

  it('should report operation counts by type', () => {
    const docs: TestDocument[] = [
      createDoc('doc1', 1, 'i'),
      createDoc('doc2', 2, 'i'),
      createDoc('doc1', 3, 'u'),
      createDoc('doc3', 4, 'i'),
      createDoc('doc2', 5, 'd'),
      createDoc('doc4', 6, 'i'),
      createDoc('doc4', 7, 'u'),
    ];

    const result = engine.deduplicate(docs, { trackOperationCounts: true });

    expect(result.stats.operationCounts).toEqual({
      insert: 4,
      update: 2,
      delete: 1,
    });
  });
});
