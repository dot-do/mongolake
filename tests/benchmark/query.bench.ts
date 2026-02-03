/**
 * Query Benchmark - Vitest bench suite
 *
 * Measures query performance for MongoLake operations.
 *
 * Run with: pnpm run benchmark:vitest
 */

import { bench, describe, beforeAll } from 'vitest';
import { BTree } from '../../src/index/btree.js';
import { matchesFilter } from '../../src/utils/filter.js';
import { sortDocuments } from '../../src/utils/sort.js';
import { applyProjection } from '../../src/utils/projection.js';
import type { Document } from '../../src/types.js';

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
  score: number;
  tags: string[];
  department: string;
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
// Shared Test Data
// ============================================================================

let docs1000: TestDocument[];
let docs10000: TestDocument[];
let docsMap: Map<string, TestDocument>;
let ageIndex: BTree<number>;
let departmentIndex: BTree<string>;

beforeAll(() => {
  // Generate test datasets
  docs1000 = generateTestData(1000);
  docs10000 = generateTestData(10000);

  // Build indexes for 10k dataset
  ageIndex = new BTree<number>('age_idx', 'age', 64);
  departmentIndex = new BTree<string>('dept_idx', 'department', 64);
  docsMap = new Map();

  for (const doc of docs10000) {
    ageIndex.insert(doc.age, doc._id);
    departmentIndex.insert(doc.department, doc._id);
    docsMap.set(doc._id, doc);
  }
});

// ============================================================================
// findOne Latency Benchmarks
// ============================================================================

describe('findOne latency', () => {
  bench('equality filter (age === 30)', () => {
    docs10000.find((d) => matchesFilter(d as unknown as Document, { age: 30 }));
  }, {
    iterations: 1000,
    warmupIterations: 100,
  });

  bench('equality filter via index', () => {
    const docIds = ageIndex.search(30);
    if (docIds.length > 0) {
      docsMap.get(docIds[0]);
    }
  }, {
    iterations: 1000,
    warmupIterations: 100,
  });

  bench('compound filter (active && age > 30)', () => {
    docs10000.find((d) =>
      matchesFilter(d as unknown as Document, {
        $and: [{ active: true }, { age: { $gt: 30 } }],
      })
    );
  }, {
    iterations: 500,
    warmupIterations: 50,
  });
});

// ============================================================================
// find().toArray() Result Size Benchmarks
// ============================================================================

describe('find().toArray() result sizes', () => {
  bench('10 results (1000 doc collection)', () => {
    const results = docs1000.filter((d) =>
      matchesFilter(d as unknown as Document, { age: 25 })
    );
    return results.slice(0, 10);
  }, {
    iterations: 200,
    warmupIterations: 20,
  });

  bench('100 results (1000 doc collection)', () => {
    const results = docs1000.filter((d) =>
      matchesFilter(d as unknown as Document, { age: { $gte: 40, $lt: 50 } })
    );
    return results.slice(0, 100);
  }, {
    iterations: 100,
    warmupIterations: 10,
  });

  bench('1000 results (10000 doc collection)', () => {
    const results = docs10000.filter((d) =>
      matchesFilter(d as unknown as Document, { age: { $gte: 25, $lt: 35 } })
    );
    return results.slice(0, 1000);
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('all results (full scan 10000 docs)', () => {
    return docs10000.filter((d) =>
      matchesFilter(d as unknown as Document, { age: { $gte: 0 } })
    );
  }, {
    iterations: 20,
    warmupIterations: 3,
  });
});

// ============================================================================
// Index vs Full Scan Comparison
// ============================================================================

describe('with vs without indexes', () => {
  // Full scan queries
  bench('full scan: age === 30', () => {
    docs10000.filter((d) => d.age === 30);
  }, {
    iterations: 100,
    warmupIterations: 10,
  });

  // Index queries
  bench('index lookup: age === 30', () => {
    const docIds = ageIndex.search(30);
    docIds.map((id) => docsMap.get(id));
  }, {
    iterations: 100,
    warmupIterations: 10,
  });

  // Range queries
  bench('full scan: 30 <= age <= 40', () => {
    docs10000.filter((d) => d.age >= 30 && d.age <= 40);
  }, {
    iterations: 100,
    warmupIterations: 10,
  });

  bench('index range: 30 <= age <= 40', () => {
    const entries = ageIndex.range(30, 40);
    entries.flatMap(([_, ids]) => ids.map((id) => docsMap.get(id)));
  }, {
    iterations: 100,
    warmupIterations: 10,
  });

  // String index lookup
  bench('index lookup: department === "engineering"', () => {
    const docIds = departmentIndex.search('engineering');
    docIds.map((id) => docsMap.get(id));
  }, {
    iterations: 100,
    warmupIterations: 10,
  });
});

// ============================================================================
// Sorting Benchmarks
// ============================================================================

describe('sorting performance', () => {
  bench('sort 1000 docs by age ASC', () => {
    sortDocuments([...docs1000], { age: 1 });
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('sort 1000 docs by age DESC', () => {
    sortDocuments([...docs1000], { age: -1 });
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('sort 1000 docs multi-field (department ASC, age DESC)', () => {
    sortDocuments([...docs1000], { department: 1, age: -1 });
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('sort 10000 docs by age ASC', () => {
    sortDocuments([...docs10000], { age: 1 });
  }, {
    iterations: 10,
    warmupIterations: 2,
  });
});

// ============================================================================
// Projection Benchmarks
// ============================================================================

describe('projection performance', () => {
  bench('include projection (3 fields) - 1000 docs', () => {
    docs1000.map((d) => applyProjection(d, { _id: 1, name: 1, age: 1 }));
  }, {
    iterations: 100,
    warmupIterations: 10,
  });

  bench('exclude projection (2 fields) - 1000 docs', () => {
    docs1000.map((d) => applyProjection(d, { tags: 0, score: 0 }));
  }, {
    iterations: 100,
    warmupIterations: 10,
  });

  bench('include projection (3 fields) - 10000 docs', () => {
    docs10000.map((d) => applyProjection(d, { _id: 1, name: 1, age: 1 }));
  }, {
    iterations: 20,
    warmupIterations: 2,
  });
});

// ============================================================================
// Filter Complexity Benchmarks
// ============================================================================

describe('filter complexity', () => {
  bench('simple equality', () => {
    docs10000.filter((d) =>
      matchesFilter(d as unknown as Document, { age: 25 })
    );
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('range filter', () => {
    docs10000.filter((d) =>
      matchesFilter(d as unknown as Document, { age: { $gte: 25, $lte: 35 } })
    );
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('$or filter', () => {
    docs10000.filter((d) =>
      matchesFilter(d as unknown as Document, {
        $or: [{ age: { $lt: 25 } }, { age: { $gt: 45 } }],
      })
    );
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('$and filter', () => {
    docs10000.filter((d) =>
      matchesFilter(d as unknown as Document, {
        $and: [{ active: true }, { age: { $gt: 30 } }],
      })
    );
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('$in filter', () => {
    docs10000.filter((d) =>
      matchesFilter(d as unknown as Document, {
        department: { $in: ['engineering', 'sales'] },
      })
    );
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('complex compound filter', () => {
    docs10000.filter((d) =>
      matchesFilter(d as unknown as Document, {
        $or: [
          { $and: [{ active: true }, { age: { $gt: 30 } }] },
          { department: 'engineering' },
        ],
      })
    );
  }, {
    iterations: 50,
    warmupIterations: 5,
  });
});
