/**
 * Aggregation Benchmark - Vitest bench suite
 *
 * Measures aggregation pipeline performance for MongoLake.
 *
 * Run with: pnpm run benchmark:vitest
 */

import { bench, describe, beforeAll } from 'vitest';
import { matchesFilter } from '../../src/utils/filter.js';
import { sortDocuments } from '../../src/utils/sort.js';
import { applyProjection } from '../../src/utils/projection.js';
import type { Document } from '../../src/types.js';
import { getNestedValue } from '../../src/utils/nested.js';

// ============================================================================
// Test Data Setup
// ============================================================================

interface TestDocument {
  _id: string;
  name: string;
  department: string;
  salary: number;
  age: number;
  active: boolean;
  region: string;
  hireDate: string;
  skills: string[];
}

function generateTestData(count: number): TestDocument[] {
  const departments = ['engineering', 'sales', 'marketing', 'support', 'hr'];
  const regions = ['us-west', 'us-east', 'europe', 'asia'];
  const allSkills = ['javascript', 'python', 'go', 'rust', 'sql', 'kubernetes', 'aws', 'gcp'];
  const docs: TestDocument[] = [];

  for (let i = 0; i < count; i++) {
    docs.push({
      _id: `doc-${i}`,
      name: `Employee ${i}`,
      department: departments[i % departments.length],
      salary: 50000 + Math.floor(Math.random() * 100000),
      age: 22 + (i % 40),
      active: i % 4 !== 0, // 75% active
      region: regions[i % regions.length],
      hireDate: new Date(Date.now() - Math.random() * 5 * 365 * 86400000).toISOString(),
      skills: [
        allSkills[i % allSkills.length],
        allSkills[(i + 1) % allSkills.length],
        allSkills[(i + 2) % allSkills.length],
      ],
    });
  }

  return docs;
}

// ============================================================================
// Manual Aggregation Pipeline Implementation
// ============================================================================

/**
 * Simulates aggregation pipeline processing for benchmarking.
 * This mirrors the logic in AggregationCursor but without the async overhead.
 */
function runPipeline<T extends Record<string, unknown>>(
  docs: T[],
  pipeline: Array<Record<string, unknown>>
): unknown[] {
  let results: unknown[] = [...docs];

  for (const stage of pipeline) {
    const [[op, spec]] = Object.entries(stage);

    switch (op) {
      case '$match': {
        results = results.filter((doc) =>
          matchesFilter(doc as Document, spec as Record<string, unknown>)
        );
        break;
      }

      case '$sort': {
        results = sortDocuments(
          results as Record<string, unknown>[],
          spec as Record<string, 1 | -1>
        );
        break;
      }

      case '$limit': {
        results = results.slice(0, spec as number);
        break;
      }

      case '$skip': {
        results = results.slice(spec as number);
        break;
      }

      case '$project': {
        results = results.map((doc) =>
          applyProjection(doc as Record<string, unknown>, spec as Record<string, 0 | 1>)
        );
        break;
      }

      case '$group': {
        const groupSpec = spec as { _id: unknown; [key: string]: unknown };
        const groups = new Map<string, { docs: unknown[]; accumulators: Record<string, unknown> }>();

        for (const doc of results) {
          const record = doc as Record<string, unknown>;
          const groupKey = evaluateGroupId(record, groupSpec._id);

          if (!groups.has(groupKey)) {
            groups.set(groupKey, { docs: [], accumulators: {} });
          }
          const group = groups.get(groupKey)!;
          group.docs.push(doc);
        }

        // Apply accumulators
        results = Array.from(groups.entries()).map(([key, { docs: groupDocs, accumulators }]) => {
          const result: Record<string, unknown> = {
            _id: key === '__null__' ? null : key,
          };

          for (const [field, expr] of Object.entries(groupSpec)) {
            if (field === '_id') continue;

            const accumulator = expr as { [op: string]: unknown };
            const accOp = Object.keys(accumulator)[0];
            const accField = accumulator[accOp];

            switch (accOp) {
              case '$sum': {
                if (typeof accField === 'number') {
                  result[field] = groupDocs.length * accField;
                } else if (typeof accField === 'string' && accField.startsWith('$')) {
                  const fieldName = accField.slice(1);
                  result[field] = groupDocs.reduce((sum, d) => {
                    const val = getNestedValue(d as Record<string, unknown>, fieldName);
                    return sum + (typeof val === 'number' ? val : 0);
                  }, 0);
                }
                break;
              }
              case '$avg': {
                if (typeof accField === 'string' && accField.startsWith('$')) {
                  const fieldName = accField.slice(1);
                  const values = groupDocs.map((d) =>
                    getNestedValue(d as Record<string, unknown>, fieldName)
                  ).filter((v): v is number => typeof v === 'number');
                  result[field] = values.length > 0
                    ? values.reduce((a, b) => a + b, 0) / values.length
                    : 0;
                }
                break;
              }
              case '$min': {
                if (typeof accField === 'string' && accField.startsWith('$')) {
                  const fieldName = accField.slice(1);
                  const values = groupDocs.map((d) =>
                    getNestedValue(d as Record<string, unknown>, fieldName)
                  ).filter((v): v is number => typeof v === 'number');
                  result[field] = values.length > 0 ? Math.min(...values) : null;
                }
                break;
              }
              case '$max': {
                if (typeof accField === 'string' && accField.startsWith('$')) {
                  const fieldName = accField.slice(1);
                  const values = groupDocs.map((d) =>
                    getNestedValue(d as Record<string, unknown>, fieldName)
                  ).filter((v): v is number => typeof v === 'number');
                  result[field] = values.length > 0 ? Math.max(...values) : null;
                }
                break;
              }
              case '$count': {
                result[field] = groupDocs.length;
                break;
              }
              case '$push': {
                if (typeof accField === 'string' && accField.startsWith('$')) {
                  const fieldName = accField.slice(1);
                  result[field] = groupDocs.map((d) =>
                    getNestedValue(d as Record<string, unknown>, fieldName)
                  );
                }
                break;
              }
              case '$first': {
                if (typeof accField === 'string' && accField.startsWith('$')) {
                  const fieldName = accField.slice(1);
                  result[field] = groupDocs.length > 0
                    ? getNestedValue(groupDocs[0] as Record<string, unknown>, fieldName)
                    : null;
                }
                break;
              }
              case '$last': {
                if (typeof accField === 'string' && accField.startsWith('$')) {
                  const fieldName = accField.slice(1);
                  result[field] = groupDocs.length > 0
                    ? getNestedValue(groupDocs[groupDocs.length - 1] as Record<string, unknown>, fieldName)
                    : null;
                }
                break;
              }
            }
          }

          return result;
        });
        break;
      }

      case '$count': {
        results = [{ [spec as string]: results.length }];
        break;
      }

      case '$unwind': {
        const path = typeof spec === 'string' ? spec : (spec as { path: string }).path;
        const fieldName = path.startsWith('$') ? path.slice(1) : path;
        const preserveNull = typeof spec === 'object' && (spec as { preserveNullAndEmptyArrays?: boolean }).preserveNullAndEmptyArrays;

        const unwound: unknown[] = [];
        for (const doc of results) {
          const record = doc as Record<string, unknown>;
          const arr = getNestedValue(record, fieldName);

          if (Array.isArray(arr) && arr.length > 0) {
            for (const item of arr) {
              unwound.push({ ...record, [fieldName]: item });
            }
          } else if (preserveNull) {
            unwound.push({ ...record, [fieldName]: null });
          }
        }
        results = unwound;
        break;
      }
    }
  }

  return results;
}

function evaluateGroupId(doc: Record<string, unknown>, idExpr: unknown): string {
  if (idExpr === null) {
    return '__all__';
  }
  if (typeof idExpr === 'string' && idExpr.startsWith('$')) {
    const value = getNestedValue(doc, idExpr.slice(1));
    return value === null || value === undefined ? '__null__' : String(value);
  }
  if (typeof idExpr === 'object' && idExpr !== null) {
    // Compound group key
    const parts: string[] = [];
    for (const [k, v] of Object.entries(idExpr)) {
      if (typeof v === 'string' && v.startsWith('$')) {
        const val = getNestedValue(doc, v.slice(1));
        parts.push(`${k}:${val}`);
      } else {
        parts.push(`${k}:${v}`);
      }
    }
    return parts.join('|');
  }
  return String(idExpr);
}

// ============================================================================
// Shared Test Data
// ============================================================================

let docs1000: TestDocument[];
let docs10000: TestDocument[];

beforeAll(() => {
  docs1000 = generateTestData(1000);
  docs10000 = generateTestData(10000);
});

// ============================================================================
// Simple Pipeline Benchmarks
// ============================================================================

describe('simple aggregation pipeline', () => {
  bench('$match only (1000 docs)', () => {
    runPipeline(docs1000, [
      { $match: { active: true } },
    ]);
  }, {
    iterations: 100,
    warmupIterations: 10,
  });

  bench('$match only (10000 docs)', () => {
    runPipeline(docs10000, [
      { $match: { active: true } },
    ]);
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('$match + $sort (1000 docs)', () => {
    runPipeline(docs1000, [
      { $match: { active: true } },
      { $sort: { salary: -1 } },
    ]);
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('$match + $sort + $limit (1000 docs)', () => {
    runPipeline(docs1000, [
      { $match: { active: true } },
      { $sort: { salary: -1 } },
      { $limit: 10 },
    ]);
  }, {
    iterations: 100,
    warmupIterations: 10,
  });

  bench('$match + $project (1000 docs)', () => {
    runPipeline(docs1000, [
      { $match: { department: 'engineering' } },
      { $project: { name: 1, salary: 1, department: 1 } },
    ]);
  }, {
    iterations: 100,
    warmupIterations: 10,
  });

  bench('$count (10000 docs)', () => {
    runPipeline(docs10000, [
      { $match: { active: true } },
      { $count: 'activeCount' },
    ]);
  }, {
    iterations: 50,
    warmupIterations: 5,
  });
});

// ============================================================================
// $group with Accumulator Benchmarks
// ============================================================================

describe('$group with accumulator', () => {
  bench('$group by department - $sum (1000 docs)', () => {
    runPipeline(docs1000, [
      {
        $group: {
          _id: '$department',
          totalSalary: { $sum: '$salary' },
          count: { $sum: 1 },
        },
      },
    ]);
  }, {
    iterations: 100,
    warmupIterations: 10,
  });

  bench('$group by department - $sum (10000 docs)', () => {
    runPipeline(docs10000, [
      {
        $group: {
          _id: '$department',
          totalSalary: { $sum: '$salary' },
          count: { $sum: 1 },
        },
      },
    ]);
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('$group by department - $avg (1000 docs)', () => {
    runPipeline(docs1000, [
      {
        $group: {
          _id: '$department',
          avgSalary: { $avg: '$salary' },
          avgAge: { $avg: '$age' },
        },
      },
    ]);
  }, {
    iterations: 100,
    warmupIterations: 10,
  });

  bench('$group by department - $min/$max (1000 docs)', () => {
    runPipeline(docs1000, [
      {
        $group: {
          _id: '$department',
          minSalary: { $min: '$salary' },
          maxSalary: { $max: '$salary' },
        },
      },
    ]);
  }, {
    iterations: 100,
    warmupIterations: 10,
  });

  bench('$group all (_id: null) - $sum (10000 docs)', () => {
    runPipeline(docs10000, [
      {
        $group: {
          _id: null,
          totalSalary: { $sum: '$salary' },
          avgSalary: { $avg: '$salary' },
          count: { $sum: 1 },
        },
      },
    ]);
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('$group with $first/$last (1000 docs)', () => {
    runPipeline(docs1000, [
      { $sort: { salary: -1 } },
      {
        $group: {
          _id: '$department',
          highestPaid: { $first: '$name' },
          lowestPaid: { $last: '$name' },
        },
      },
    ]);
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('$group compound key (dept + region) (1000 docs)', () => {
    runPipeline(docs1000, [
      {
        $group: {
          _id: { department: '$department', region: '$region' },
          totalSalary: { $sum: '$salary' },
          count: { $sum: 1 },
        },
      },
    ]);
  }, {
    iterations: 50,
    warmupIterations: 5,
  });
});

// ============================================================================
// Complex Pipeline Benchmarks
// ============================================================================

describe('complex pipelines', () => {
  bench('$match + $group + $sort (1000 docs)', () => {
    runPipeline(docs1000, [
      { $match: { active: true } },
      {
        $group: {
          _id: '$department',
          avgSalary: { $avg: '$salary' },
          count: { $sum: 1 },
        },
      },
      { $sort: { avgSalary: -1 } },
    ]);
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('$match + $group + $sort (10000 docs)', () => {
    runPipeline(docs10000, [
      { $match: { active: true } },
      {
        $group: {
          _id: '$department',
          avgSalary: { $avg: '$salary' },
          count: { $sum: 1 },
        },
      },
      { $sort: { avgSalary: -1 } },
    ]);
  }, {
    iterations: 20,
    warmupIterations: 3,
  });

  bench('$unwind + $group (1000 docs)', () => {
    runPipeline(docs1000, [
      { $unwind: '$skills' },
      {
        $group: {
          _id: '$skills',
          count: { $sum: 1 },
          avgSalary: { $avg: '$salary' },
        },
      },
      { $sort: { count: -1 } },
    ]);
  }, {
    iterations: 30,
    warmupIterations: 3,
  });

  bench('multi-stage analytics pipeline (1000 docs)', () => {
    runPipeline(docs1000, [
      { $match: { active: true, salary: { $gte: 60000 } } },
      {
        $group: {
          _id: { department: '$department', region: '$region' },
          avgSalary: { $avg: '$salary' },
          maxSalary: { $max: '$salary' },
          headcount: { $sum: 1 },
        },
      },
      { $sort: { avgSalary: -1 } },
      { $limit: 10 },
    ]);
  }, {
    iterations: 50,
    warmupIterations: 5,
  });

  bench('full pipeline with projection (1000 docs)', () => {
    runPipeline(docs1000, [
      { $match: { department: { $in: ['engineering', 'sales'] } } },
      { $sort: { salary: -1 } },
      { $limit: 100 },
      { $project: { name: 1, department: 1, salary: 1 } },
    ]);
  }, {
    iterations: 100,
    warmupIterations: 10,
  });
});
