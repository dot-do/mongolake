/**
 * Query Error Scenario Tests
 *
 * Comprehensive tests for query-related error handling:
 * - Query timeout simulation
 * - Invalid aggregation pipeline errors
 * - Query execution failures
 *
 * These tests verify that query errors are properly handled with
 * informative error messages.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  validateFilter,
  validateAggregationPipeline,
  ValidationError,
} from '../../../src/validation/index.js';

// ============================================================================
// Query Timeout Simulation Tests
// ============================================================================

describe('Query Timeout Simulation', () => {
  /**
   * Simulates a query executor that can timeout
   */
  class QueryExecutor {
    private timeoutMs: number;

    constructor(timeoutMs: number = 5000) {
      this.timeoutMs = timeoutMs;
    }

    async execute<T>(
      queryFn: () => Promise<T>,
      options: { timeout?: number } = {}
    ): Promise<T> {
      const timeout = options.timeout ?? this.timeoutMs;

      return Promise.race([
        queryFn(),
        new Promise<T>((_, reject) => {
          setTimeout(() => {
            reject(new QueryTimeoutError(`Query timed out after ${timeout}ms`));
          }, timeout);
        }),
      ]);
    }
  }

  class QueryTimeoutError extends Error {
    constructor(message: string) {
      super(message);
      this.name = 'QueryTimeoutError';
    }
  }

  let executor: QueryExecutor;

  beforeEach(() => {
    vi.useFakeTimers();
    executor = new QueryExecutor(100);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should throw QueryTimeoutError when query exceeds timeout', async () => {
    const slowQuery = async () => {
      await new Promise((resolve) => setTimeout(resolve, 200));
      return { results: [] };
    };

    const queryPromise = executor.execute(slowQuery, { timeout: 50 });

    vi.advanceTimersByTime(51);

    await expect(queryPromise).rejects.toThrow(QueryTimeoutError);
    await expect(queryPromise).rejects.toThrow('timed out after 50ms');
  });

  it('should complete successfully when query finishes before timeout', async () => {
    const fastQuery = async () => {
      await new Promise((resolve) => setTimeout(resolve, 10));
      return { results: [1, 2, 3] };
    };

    const queryPromise = executor.execute(fastQuery, { timeout: 100 });

    vi.advanceTimersByTime(11);

    const result = await queryPromise;
    expect(result).toEqual({ results: [1, 2, 3] });
  });

  it('should include timeout duration in error message', async () => {
    const slowQuery = async () => {
      await new Promise((resolve) => setTimeout(resolve, 1000));
      return {};
    };

    const queryPromise = executor.execute(slowQuery, { timeout: 250 });

    vi.advanceTimersByTime(251);

    try {
      await queryPromise;
    } catch (error) {
      expect(error).toBeInstanceOf(QueryTimeoutError);
      expect((error as Error).message).toContain('250ms');
    }
  });

  it('should handle multiple concurrent queries with different timeouts', async () => {
    const slowQuery = async () => {
      await new Promise((resolve) => setTimeout(resolve, 200));
      return 'slow';
    };

    const fastQuery = async () => {
      await new Promise((resolve) => setTimeout(resolve, 50));
      return 'fast';
    };

    const slowPromise = executor.execute(slowQuery, { timeout: 100 });
    const fastPromise = executor.execute(fastQuery, { timeout: 100 });

    vi.advanceTimersByTime(60);
    await expect(fastPromise).resolves.toBe('fast');

    vi.advanceTimersByTime(50);
    await expect(slowPromise).rejects.toThrow(QueryTimeoutError);
  });

  it('should handle query that throws error before timeout', async () => {
    const errorQuery = async () => {
      await new Promise((resolve) => setTimeout(resolve, 10));
      throw new Error('Query execution failed');
    };

    const queryPromise = executor.execute(errorQuery, { timeout: 100 });

    vi.advanceTimersByTime(11);

    await expect(queryPromise).rejects.toThrow('Query execution failed');
  });
});

// ============================================================================
// Invalid Aggregation Pipeline Tests
// ============================================================================

describe('Invalid Aggregation Pipeline Errors', () => {
  describe('pipeline structure errors', () => {
    it('should throw ValidationError for non-array pipeline', () => {
      expect(() => validateAggregationPipeline({ $match: {} })).toThrow(ValidationError);

      const error = getValidationError(() => validateAggregationPipeline({ $match: {} }));
      expect(error?.message).toContain('must be an array');
    });

    it('should throw ValidationError for empty pipeline', () => {
      expect(() => validateAggregationPipeline([])).toThrow(ValidationError);

      const error = getValidationError(() => validateAggregationPipeline([]));
      expect(error?.message).toContain('cannot be empty');
    });

    it('should throw ValidationError for null/undefined stages', () => {
      expect(() => validateAggregationPipeline([null])).toThrow(ValidationError);
      expect(() => validateAggregationPipeline([undefined])).toThrow(ValidationError);
    });

    it('should throw ValidationError for non-object stages', () => {
      expect(() => validateAggregationPipeline(['$match'])).toThrow(ValidationError);
      expect(() => validateAggregationPipeline([123])).toThrow(ValidationError);
      expect(() => validateAggregationPipeline([['$match', {}]])).toThrow(ValidationError);
    });
  });

  describe('invalid stage operator errors', () => {
    it('should throw ValidationError for unknown stage operators', () => {
      expect(() => validateAggregationPipeline([{ $filter: {} }])).toThrow(ValidationError);

      const error = getValidationError(() => validateAggregationPipeline([{ $filter: {} }]));
      expect(error?.message).toContain('invalid aggregation stage');
      expect(error?.message).toContain('$filter');
    });

    it('should throw ValidationError for typos in stage operators', () => {
      expect(() => validateAggregationPipeline([{ $matches: {} }])).toThrow(ValidationError);
      expect(() => validateAggregationPipeline([{ $projects: {} }])).toThrow(ValidationError);
      expect(() => validateAggregationPipeline([{ $groupby: {} }])).toThrow(ValidationError);
    });

    it('should throw ValidationError for stages with multiple operators', () => {
      expect(() =>
        validateAggregationPipeline([{ $match: {}, $project: {} }])
      ).toThrow(ValidationError);

      const error = getValidationError(() =>
        validateAggregationPipeline([{ $match: {}, $sort: {} }])
      );
      expect(error?.message).toContain('exactly one key');
    });

    it('should throw ValidationError for empty stage object', () => {
      expect(() => validateAggregationPipeline([{}])).toThrow(ValidationError);
    });
  });

  describe('$match stage errors', () => {
    it('should throw ValidationError for invalid filter in $match', () => {
      expect(() =>
        validateAggregationPipeline([{ $match: { field: { $badOp: 1 } } }])
      ).toThrow(ValidationError);
    });

    it('should propagate nested filter validation errors', () => {
      const error = getValidationError(() =>
        validateAggregationPipeline([{ $match: { $and: 'not an array' } }])
      );
      expect(error?.message).toContain('requires an array');
    });
  });

  describe('$project stage errors', () => {
    it('should throw ValidationError for invalid projection in $project', () => {
      expect(() =>
        validateAggregationPipeline([{ $project: { name: 2 } }])
      ).toThrow(ValidationError);
    });

    it('should throw ValidationError for mixed inclusion/exclusion', () => {
      expect(() =>
        validateAggregationPipeline([{ $project: { name: 1, age: 0 } }])
      ).toThrow(ValidationError);
    });
  });

  describe('$sort stage errors', () => {
    it('should throw ValidationError for non-object $sort', () => {
      expect(() => validateAggregationPipeline([{ $sort: 'name' }])).toThrow(ValidationError);
      expect(() => validateAggregationPipeline([{ $sort: ['name'] }])).toThrow(ValidationError);
    });

    it('should throw ValidationError for invalid sort direction', () => {
      expect(() => validateAggregationPipeline([{ $sort: { name: 0 } }])).toThrow(ValidationError);
      expect(() => validateAggregationPipeline([{ $sort: { name: 2 } }])).toThrow(ValidationError);
      expect(() => validateAggregationPipeline([{ $sort: { name: 'asc' } }])).toThrow(ValidationError);

      const error = getValidationError(() =>
        validateAggregationPipeline([{ $sort: { name: 2 } }])
      );
      expect(error?.message).toContain('1 or -1');
    });
  });

  describe('$limit and $skip stage errors', () => {
    it('should throw ValidationError for negative $limit', () => {
      expect(() => validateAggregationPipeline([{ $limit: -1 }])).toThrow(ValidationError);

      const error = getValidationError(() => validateAggregationPipeline([{ $limit: -1 }]));
      expect(error?.message).toContain('non-negative integer');
    });

    it('should throw ValidationError for non-integer $limit', () => {
      expect(() => validateAggregationPipeline([{ $limit: 1.5 }])).toThrow(ValidationError);
      expect(() => validateAggregationPipeline([{ $limit: 'ten' }])).toThrow(ValidationError);
    });

    it('should throw ValidationError for negative $skip', () => {
      expect(() => validateAggregationPipeline([{ $skip: -5 }])).toThrow(ValidationError);
    });

    it('should throw ValidationError for non-integer $skip', () => {
      expect(() => validateAggregationPipeline([{ $skip: 2.5 }])).toThrow(ValidationError);
    });
  });

  describe('pipeline length errors', () => {
    it('should throw ValidationError for too many stages', () => {
      const pipeline = Array(150).fill({ $match: {} });

      expect(() => validateAggregationPipeline(pipeline, { maxStages: 100 })).toThrow(ValidationError);

      const error = getValidationError(() =>
        validateAggregationPipeline(pipeline, { maxStages: 100 })
      );
      expect(error?.message).toContain('maximum');
      expect(error?.message).toContain('100');
      expect(error?.message).toContain('stages');
    });
  });
});

// ============================================================================
// Query Execution Error Tests
// ============================================================================

describe('Query Execution Errors', () => {
  /**
   * Mock query engine that simulates various execution errors
   */
  class MockQueryEngine {
    private shouldError: Error | null = null;

    setError(error: Error | null): void {
      this.shouldError = error;
    }

    async find(collection: string, filter: Record<string, unknown>): Promise<unknown[]> {
      // Validate filter first
      validateFilter(filter);

      if (this.shouldError) {
        throw this.shouldError;
      }

      return [];
    }

    async aggregate(
      collection: string,
      pipeline: unknown[]
    ): Promise<unknown[]> {
      // Validate pipeline first
      validateAggregationPipeline(pipeline);

      if (this.shouldError) {
        throw this.shouldError;
      }

      return [];
    }
  }

  let queryEngine: MockQueryEngine;

  beforeEach(() => {
    queryEngine = new MockQueryEngine();
  });

  describe('find query errors', () => {
    it('should throw ValidationError for invalid filter operator', async () => {
      await expect(
        queryEngine.find('users', { age: { $greaterThan: 18 } })
      ).rejects.toThrow(ValidationError);
    });

    it('should throw informative error for execution failure', async () => {
      queryEngine.setError(new Error('Collection "users" does not exist'));

      await expect(queryEngine.find('users', {})).rejects.toThrow('does not exist');
    });

    it('should throw informative error for memory limit exceeded', async () => {
      queryEngine.setError(new Error('Query exceeded memory limit of 100MB'));

      await expect(queryEngine.find('users', {})).rejects.toThrow('memory limit');
    });

    it('should throw informative error for connection lost', async () => {
      queryEngine.setError(new Error('Connection to database lost'));

      await expect(queryEngine.find('users', {})).rejects.toThrow('Connection');
    });
  });

  describe('aggregate query errors', () => {
    it('should throw ValidationError for invalid pipeline', async () => {
      await expect(
        queryEngine.aggregate('users', [{ $badStage: {} }])
      ).rejects.toThrow(ValidationError);
    });

    it('should throw informative error for unsupported operation', async () => {
      queryEngine.setError(new Error('$lookup is not supported in this context'));

      await expect(
        queryEngine.aggregate('users', [{ $match: {} }])
      ).rejects.toThrow('not supported');
    });

    it('should throw informative error for index required', async () => {
      queryEngine.setError(new Error('This aggregation requires an index on field "status"'));

      await expect(
        queryEngine.aggregate('users', [{ $match: { status: 'active' } }])
      ).rejects.toThrow('requires an index');
    });
  });
});

// ============================================================================
// Complex Query Error Scenarios
// ============================================================================

describe('Complex Query Error Scenarios', () => {
  it('should validate deeply nested $and/$or combinations', () => {
    const validComplexFilter = {
      $and: [
        { status: 'active' },
        {
          $or: [
            { age: { $gte: 18 } },
            { parentConsent: true },
          ],
        },
        {
          $or: [
            { role: { $in: ['admin', 'moderator'] } },
            { verified: true },
          ],
        },
      ],
    };

    expect(() => validateFilter(validComplexFilter)).not.toThrow();
  });

  it('should throw ValidationError for invalid operator in nested structure', () => {
    const invalidNestedFilter = {
      $and: [
        { status: 'active' },
        {
          $or: [
            { age: { $greaterOrEqual: 18 } }, // Invalid operator
            { parentConsent: true },
          ],
        },
      ],
    };

    expect(() => validateFilter(invalidNestedFilter)).toThrow(ValidationError);
  });

  it('should validate complex aggregation pipeline', () => {
    // Note: Projection validation requires simple 0/1 values or valid projection operators
    // Expression values like '$_id' are not supported in the basic validateProjection
    const validPipeline = [
      { $match: { status: 'active' } },
      { $group: { _id: '$category', count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 10 },
      { $project: { count: 1, _id: 0 } },
    ];

    expect(() => validateAggregationPipeline(validPipeline)).not.toThrow();
  });

  it('should throw ValidationError for invalid match in aggregation', () => {
    const invalidPipeline = [
      { $match: { field: { $badOperator: 'value' } } },
      { $group: { _id: '$category' } },
    ];

    expect(() => validateAggregationPipeline(invalidPipeline)).toThrow(ValidationError);
  });

  it('should throw ValidationError for invalid projection in aggregation', () => {
    const invalidPipeline = [
      { $match: {} },
      { $project: { included: 1, excluded: 0 } }, // Mixed inclusion/exclusion
    ];

    expect(() => validateAggregationPipeline(invalidPipeline)).toThrow(ValidationError);
  });
});

// ============================================================================
// Error Message Quality for Query Errors
// ============================================================================

describe('Query Error Message Quality', () => {
  it('should include stage index in aggregation pipeline errors', () => {
    const error = getValidationError(() =>
      validateAggregationPipeline([
        { $match: {} },
        { $badStage: {} },
      ])
    );

    expect(error?.context?.index).toBe(1);
  });

  it('should include operator name in filter errors', () => {
    const error = getValidationError(() =>
      validateFilter({ field: { $unknownOp: 'value' } })
    );

    expect(error?.invalidValue).toBe('$unknownOp');
  });

  it('should include field name context in sort errors', () => {
    const error = getValidationError(() =>
      validateAggregationPipeline([{ $sort: { badField: 2 } }])
    );

    expect(error?.context?.field).toBe('badField');
  });

  it('should provide clear message for $in with non-array', () => {
    const error = getValidationError(() =>
      validateFilter({ status: { $in: 'active' } })
    );

    expect(error?.message).toContain('$in');
    expect(error?.message).toContain('requires an array');
  });

  it('should provide clear message for $and with non-array', () => {
    const error = getValidationError(() =>
      validateFilter({ $and: { a: 1 } })
    );

    expect(error?.message).toContain('$and');
    expect(error?.message).toContain('requires an array');
  });
});

// ============================================================================
// Helper Functions
// ============================================================================

function getValidationError(fn: () => void): ValidationError | undefined {
  try {
    fn();
    return undefined;
  } catch (e) {
    if (e instanceof ValidationError) {
      return e;
    }
    throw e;
  }
}
