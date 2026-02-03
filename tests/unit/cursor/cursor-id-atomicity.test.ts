/**
 * Cursor ID Atomicity Tests
 *
 * Tests to detect race conditions in cursor ID generation.
 * These tests verify that generateCursorId() produces unique IDs
 * even under high concurrent load.
 */

import { describe, it, expect } from 'vitest';
import { generateCursorId } from '../../../src/cursor/index.js';

// =============================================================================
// Constants
// =============================================================================

const CONCURRENT_COUNT = 1000;
const LARGE_CONCURRENT_COUNT = 5000;
const RAPID_SEQUENTIAL_COUNT = 10000;

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Extract the timestamp portion (upper 32 bits) from a cursor ID
 */
function extractTimestamp(cursorId: bigint): bigint {
  return cursorId >> 32n;
}

/**
 * Extract the random portion (lower 32 bits) from a cursor ID
 */
function extractRandomPortion(cursorId: bigint): bigint {
  return cursorId & 0xFFFFFFFFn;
}

/**
 * Check if a sequence of numbers appears to be sequential (indicates race condition)
 * Returns true if more than threshold% of values are sequential
 */
function hasSequentialPattern(values: bigint[], threshold = 0.1): boolean {
  if (values.length < 2) return false;

  const sorted = [...values].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  let sequentialCount = 0;

  for (let i = 1; i < sorted.length; i++) {
    // Check if current value is exactly 1 more than previous
    if (sorted[i] - sorted[i - 1] === 1n) {
      sequentialCount++;
    }
  }

  const sequentialRatio = sequentialCount / (sorted.length - 1);
  return sequentialRatio > threshold;
}

/**
 * Analyze distribution of random portions to detect poor randomness
 */
function analyzeDistribution(values: bigint[]): { min: bigint; max: bigint; range: bigint; variance: number } {
  const sorted = [...values].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  const min = sorted[0]!;
  const max = sorted[sorted.length - 1]!;
  const range = max - min;

  // Calculate variance (as percentage of max possible range)
  const mean = values.reduce((sum, v) => sum + v, 0n) / BigInt(values.length);
  const squaredDiffs = values.map((v) => {
    const diff = v - mean;
    return diff * diff;
  });
  const avgSquaredDiff = squaredDiffs.reduce((sum, v) => sum + v, 0n) / BigInt(values.length);
  // Convert to number for percentage calculation (normalized to max 32-bit value)
  const maxPossible = 0xFFFFFFFFn;
  const variance = Number(avgSquaredDiff) / Number(maxPossible * maxPossible);

  return { min, max, range, variance };
}

// =============================================================================
// Concurrent Generation Tests
// =============================================================================

describe('Cursor ID Atomicity', () => {
  describe('concurrent generation', () => {
    it('should generate unique IDs under concurrent load (1000 concurrent)', async () => {
      const ids = await Promise.all(
        Array.from({ length: CONCURRENT_COUNT }, () => generateCursorId())
      );

      const uniqueIds = new Set(ids.map((id) => id.toString()));
      expect(uniqueIds.size).toBe(CONCURRENT_COUNT);
    });

    it('should generate unique IDs under heavy concurrent load (5000 concurrent)', async () => {
      const ids = await Promise.all(
        Array.from({ length: LARGE_CONCURRENT_COUNT }, () => generateCursorId())
      );

      const uniqueIds = new Set(ids.map((id) => id.toString()));
      expect(uniqueIds.size).toBe(LARGE_CONCURRENT_COUNT);
    });

    it('should generate unique IDs in multiple concurrent batches', async () => {
      const allIds: bigint[] = [];
      const batchCount = 10;
      const batchSize = 500;

      // Run multiple batches concurrently
      const batchResults = await Promise.all(
        Array.from({ length: batchCount }, () =>
          Promise.all(Array.from({ length: batchSize }, () => generateCursorId()))
        )
      );

      for (const batch of batchResults) {
        allIds.push(...batch);
      }

      const uniqueIds = new Set(allIds.map((id) => id.toString()));
      expect(uniqueIds.size).toBe(batchCount * batchSize);
    });
  });

  // =============================================================================
  // Rapid Sequential Generation Tests
  // =============================================================================

  describe('rapid sequential generation', () => {
    it('should not cause collisions in rapid sequential generation', () => {
      const ids: bigint[] = [];

      for (let i = 0; i < RAPID_SEQUENTIAL_COUNT; i++) {
        ids.push(generateCursorId());
      }

      const uniqueIds = new Set(ids.map((id) => id.toString()));
      expect(uniqueIds.size).toBe(RAPID_SEQUENTIAL_COUNT);
    });

    it('should generate unique IDs in tight loop with same timestamp', () => {
      // Generate many IDs that will likely have the same timestamp (within same second)
      const ids: bigint[] = [];
      const startTime = Date.now();

      // Generate as many IDs as possible within a short time window
      while (Date.now() - startTime < 100) {
        ids.push(generateCursorId());
      }

      const uniqueIds = new Set(ids.map((id) => id.toString()));
      expect(uniqueIds.size).toBe(ids.length);

      // Verify we generated a meaningful number of IDs
      expect(ids.length).toBeGreaterThan(100);
    });
  });

  // =============================================================================
  // Sequential Pattern Detection Tests
  // =============================================================================

  describe('sequential pattern detection', () => {
    it('should not have sequential patterns in random portion (indicates non-atomic counter)', async () => {
      const ids = await Promise.all(
        Array.from({ length: CONCURRENT_COUNT }, () => generateCursorId())
      );

      // Extract the random portions
      const randomPortions = ids.map(extractRandomPortion);

      // Check for sequential patterns (would indicate race condition with counter)
      const hasSequential = hasSequentialPattern(randomPortions);
      expect(hasSequential).toBe(false);
    });

    it('should not have sequential patterns in rapid sequential generation', () => {
      const ids: bigint[] = [];

      for (let i = 0; i < 1000; i++) {
        ids.push(generateCursorId());
      }

      const randomPortions = ids.map(extractRandomPortion);
      const hasSequential = hasSequentialPattern(randomPortions);
      expect(hasSequential).toBe(false);
    });

    it('should have good distribution of random portions', async () => {
      const ids = await Promise.all(
        Array.from({ length: CONCURRENT_COUNT }, () => generateCursorId())
      );

      const randomPortions = ids.map(extractRandomPortion);
      const distribution = analyzeDistribution(randomPortions);

      // The range should be reasonably large (at least 50% of possible 32-bit range)
      // This ensures we're not seeing clustered values that indicate poor randomness
      const minExpectedRange = 0xFFFFFFFFn / 2n;
      expect(distribution.range).toBeGreaterThan(minExpectedRange);
    });
  });

  // =============================================================================
  // Timestamp Verification Tests
  // =============================================================================

  describe('timestamp verification', () => {
    it('should have correct timestamp in upper 32 bits', () => {
      const beforeTime = BigInt(Math.floor(Date.now() / 1000));
      const id = generateCursorId();
      const afterTime = BigInt(Math.floor(Date.now() / 1000));

      const timestamp = extractTimestamp(id);

      expect(timestamp).toBeGreaterThanOrEqual(beforeTime);
      expect(timestamp).toBeLessThanOrEqual(afterTime);
    });

    it('should have consistent timestamps within same second', async () => {
      const ids = await Promise.all(
        Array.from({ length: 100 }, () => generateCursorId())
      );

      const timestamps = ids.map(extractTimestamp);
      const uniqueTimestamps = new Set(timestamps.map((t) => t.toString()));

      // All IDs generated within a short time should have at most 2 different timestamps
      // (could span a second boundary)
      expect(uniqueTimestamps.size).toBeLessThanOrEqual(2);
    });

    it('should have different timestamps when generated across time', async () => {
      const id1 = generateCursorId();

      // Wait for timestamp to change
      await new Promise((resolve) => setTimeout(resolve, 1100));

      const id2 = generateCursorId();

      const timestamp1 = extractTimestamp(id1);
      const timestamp2 = extractTimestamp(id2);

      expect(timestamp2).toBeGreaterThan(timestamp1);
    });
  });

  // =============================================================================
  // Stress Tests
  // =============================================================================

  describe('stress tests', () => {
    it('should handle interleaved concurrent and sequential generation', async () => {
      const allIds: bigint[] = [];

      for (let round = 0; round < 10; round++) {
        // Concurrent batch
        const concurrentIds = await Promise.all(
          Array.from({ length: 100 }, () => generateCursorId())
        );
        allIds.push(...concurrentIds);

        // Sequential batch
        for (let i = 0; i < 100; i++) {
          allIds.push(generateCursorId());
        }
      }

      const uniqueIds = new Set(allIds.map((id) => id.toString()));
      expect(uniqueIds.size).toBe(2000);
    });

    it('should maintain uniqueness with mixed workload', async () => {
      const allIds: bigint[] = [];

      // Start multiple "workers" that generate IDs concurrently
      const workers = Array.from({ length: 10 }, async () => {
        const workerIds: bigint[] = [];
        for (let i = 0; i < 100; i++) {
          // Mix of immediate and slightly delayed generation
          if (i % 3 === 0) {
            await new Promise((resolve) => setTimeout(resolve, 1));
          }
          workerIds.push(generateCursorId());
        }
        return workerIds;
      });

      const results = await Promise.all(workers);
      for (const workerIds of results) {
        allIds.push(...workerIds);
      }

      const uniqueIds = new Set(allIds.map((id) => id.toString()));
      expect(uniqueIds.size).toBe(1000);
    });

    it('should not have duplicate IDs across multiple test runs', async () => {
      const allIds: bigint[] = [];

      // Simulate multiple "sessions" of cursor creation
      for (let session = 0; session < 5; session++) {
        const sessionIds = await Promise.all(
          Array.from({ length: 200 }, () => generateCursorId())
        );
        allIds.push(...sessionIds);
      }

      const uniqueIds = new Set(allIds.map((id) => id.toString()));
      expect(uniqueIds.size).toBe(1000);
    });
  });

  // =============================================================================
  // Edge Cases
  // =============================================================================

  describe('edge cases', () => {
    it('should generate valid IDs (non-zero)', async () => {
      const ids = await Promise.all(
        Array.from({ length: 100 }, () => generateCursorId())
      );

      for (const id of ids) {
        expect(id).not.toBe(0n);
      }
    });

    it('should generate IDs within valid 64-bit range', async () => {
      const ids = await Promise.all(
        Array.from({ length: 100 }, () => generateCursorId())
      );

      const maxInt64 = (1n << 63n) - 1n;

      for (const id of ids) {
        expect(id).toBeGreaterThan(0n);
        expect(id).toBeLessThanOrEqual(maxInt64);
      }
    });

    it('should have random portion within valid 32-bit range', async () => {
      const ids = await Promise.all(
        Array.from({ length: 100 }, () => generateCursorId())
      );

      const max32 = 0xFFFFFFFFn;

      for (const id of ids) {
        const randomPortion = extractRandomPortion(id);
        expect(randomPortion).toBeGreaterThanOrEqual(0n);
        expect(randomPortion).toBeLessThanOrEqual(max32);
      }
    });
  });
});
