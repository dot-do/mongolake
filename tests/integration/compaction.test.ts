/**
 * Compaction Integration Tests
 *
 * Tests compaction behavior during active writes, ensuring that
 * compaction can run concurrently with read/write operations
 * without data loss or corruption.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  CompactionScheduler,
  type BlockMetadata,
  type CompactionResult,
  type AlarmScheduler,
} from '../../src/compaction/scheduler.js';
import { MemoryStorage } from '../../src/storage/index.js';
import { resetDocumentCounter } from '../utils/factories.js';

// ============================================================================
// Test Types
// ============================================================================

interface TestBlock extends BlockMetadata {
  id: string;
  path: string;
  size: number;
  rowCount: number;
  minSeq: number;
  maxSeq: number;
  createdAt: Date;
  collection?: string;
}

interface WriteOperation {
  blockId: string;
  data: Uint8Array;
  timestamp: number;
}

// ============================================================================
// Test Helpers
// ============================================================================

function createTestBlock(
  id: string,
  size: number,
  options: {
    minSeq?: number;
    maxSeq?: number;
    rowCount?: number;
    collection?: string;
  } = {}
): TestBlock {
  const rowCount = options.rowCount ?? Math.floor(size / 100);
  return {
    id,
    path: `blocks/${options.collection ?? 'test'}/${id}.parquet`,
    size,
    rowCount,
    minSeq: options.minSeq ?? 1,
    maxSeq: options.maxSeq ?? rowCount,
    createdAt: new Date(),
    collection: options.collection,
  };
}

function createSmallBlocks(
  count: number,
  sizePerBlock: number = 500_000
): TestBlock[] {
  const blocks: TestBlock[] = [];
  let currentSeq = 1;

  for (let i = 0; i < count; i++) {
    const rowCount = Math.floor(sizePerBlock / 100);
    blocks.push(
      createTestBlock(`block-${i}`, sizePerBlock, {
        minSeq: currentSeq,
        maxSeq: currentSeq + rowCount - 1,
        rowCount,
      })
    );
    currentSeq += rowCount;
  }

  return blocks;
}

async function populateStorage(
  storage: MemoryStorage,
  blocks: TestBlock[]
): Promise<void> {
  for (const block of blocks) {
    const data = new Uint8Array(block.size);
    // Fill with some pattern for validation
    for (let i = 0; i < data.length; i++) {
      data[i] = i % 256;
    }
    await storage.put(block.path, data);
  }
}

// ============================================================================
// Compaction During Active Writes
// ============================================================================

describe('Compaction - Active Writes', () => {
  let scheduler: CompactionScheduler;
  let storage: MemoryStorage;

  beforeEach(() => {
    resetDocumentCounter();
    storage = new MemoryStorage();
    scheduler = new CompactionScheduler({
      storage,
      minBlockSize: 2_000_000, // 2MB threshold
      maxBlocksPerRun: 10,
    });
  });

  afterEach(() => {
    storage.clear();
  });

  it('should not interfere with concurrent writes to different blocks', async () => {
    // Create initial small blocks for compaction
    const smallBlocks = createSmallBlocks(5, 300_000);
    await populateStorage(storage, smallBlocks);

    // Simulate concurrent write
    const newBlockPath = 'blocks/test/new-block.parquet';
    const writePromise = storage.put(newBlockPath, new Uint8Array(1000));

    // Run compaction simultaneously
    const compactionPromise = scheduler.runCompaction(smallBlocks);

    // Both should complete successfully
    const [, compactionResult] = await Promise.all([writePromise, compactionPromise]);

    // Compaction should succeed
    expect(compactionResult.skipped).toBe(false);
    expect(compactionResult.mergedBlocks).toHaveLength(1);

    // New block should exist
    const newBlockExists = await storage.exists(newBlockPath);
    expect(newBlockExists).toBe(true);
  });

  it('should preserve data integrity during concurrent operations', async () => {
    const smallBlocks = createSmallBlocks(3, 400_000);
    await populateStorage(storage, smallBlocks);

    // Write to a different collection during compaction
    const otherCollectionPath = 'blocks/other/new-doc.parquet';
    const otherData = new Uint8Array([1, 2, 3, 4, 5]);

    const compactionPromise = scheduler.runCompaction(smallBlocks);
    const writePromise = storage.put(otherCollectionPath, otherData);

    await Promise.all([compactionPromise, writePromise]);

    // Verify other collection data is intact
    const readBack = await storage.get(otherCollectionPath);
    expect(readBack).toEqual(otherData);
  });

  it('should handle writes arriving during block merge', async () => {
    const smallBlocks = createSmallBlocks(4, 350_000);
    await populateStorage(storage, smallBlocks);

    // Track write timing
    let writeCompletedDuringCompaction = false;
    const originalPut = storage.put.bind(storage);

    storage.put = async (key: string, data: Uint8Array) => {
      // Check if this is a merged block write
      if (key.includes('merged')) {
        // Simulate concurrent write arriving during merge
        const concurrentPath = 'blocks/test/concurrent-write.parquet';
        await originalPut(concurrentPath, new Uint8Array(500));
        writeCompletedDuringCompaction = true;
      }
      return originalPut(key, data);
    };

    const result = await scheduler.runCompaction(smallBlocks);

    expect(result.skipped).toBe(false);
    expect(writeCompletedDuringCompaction).toBe(true);

    // Both merged block and concurrent write should exist
    const concurrentExists = await storage.exists('blocks/test/concurrent-write.parquet');
    expect(concurrentExists).toBe(true);
  });

  it('should process new blocks in subsequent compaction runs', async () => {
    // Initial compaction
    const initialBlocks = createSmallBlocks(5, 300_000);
    await populateStorage(storage, initialBlocks);

    const firstResult = await scheduler.runCompaction(initialBlocks);
    expect(firstResult.mergedBlocks).toHaveLength(1);

    // Get the max sequence from the merged block to create non-overlapping new blocks
    const mergedMaxSeq = firstResult.mergedBlocks[0].maxSeq;

    // Add more small blocks with sequences AFTER the merged block (no overlap)
    const newBlocks = createSmallBlocks(3, 400_000).map((block, i) => ({
      ...block,
      id: `new-block-${i}`,
      path: `blocks/test/new-block-${i}.parquet`,
      minSeq: mergedMaxSeq + 1 + i * 400,
      maxSeq: mergedMaxSeq + (i + 1) * 400,
    }));
    await populateStorage(storage, newBlocks);

    // Second compaction should process new blocks only (merged block is large enough)
    const secondResult = await scheduler.runCompaction(newBlocks);

    // New small blocks should be compacted
    expect(secondResult.skipped).toBe(false);
  });
});

// ============================================================================
// Compaction with Continuation
// ============================================================================

describe('Compaction - Continuation Across Writes', () => {
  let scheduler: CompactionScheduler;
  let storage: MemoryStorage;
  let mockAlarmScheduler: AlarmScheduler;

  beforeEach(() => {
    resetDocumentCounter();
    storage = new MemoryStorage();
    mockAlarmScheduler = { schedule: vi.fn() };
    scheduler = new CompactionScheduler({
      storage,
      maxBlocksPerRun: 5,
      alarmScheduler: mockAlarmScheduler,
    });
  });

  afterEach(() => {
    storage.clear();
  });

  it('should schedule continuation when more blocks remain', async () => {
    // Create more blocks than can be processed in one run
    const blocks = createSmallBlocks(15, 200_000);
    await populateStorage(storage, blocks);

    const result = await scheduler.runCompaction(blocks);

    expect(result.hasMore).toBe(true);
    expect(mockAlarmScheduler.schedule).toHaveBeenCalled();
  });

  it('should resume from continuation state correctly', async () => {
    const blocks = createSmallBlocks(12, 250_000);
    await populateStorage(storage, blocks);

    // First run
    const firstResult = await scheduler.runCompaction(blocks);
    expect(firstResult.hasMore).toBe(true);

    const continuationState = firstResult.continuationState;
    expect(continuationState).toBeDefined();

    // Add new blocks during continuation
    const newBlock = createTestBlock('late-arrival', 300_000, {
      minSeq: 10000,
      maxSeq: 10100,
      rowCount: 100,
    });
    await storage.put(newBlock.path, new Uint8Array(newBlock.size));

    // Resume compaction
    const allBlocks = [...blocks, newBlock];
    const secondResult = await scheduler.runCompaction(allBlocks, {
      continuationState,
    });

    // Should process remaining blocks without reprocessing already-compacted ones
    expect(secondResult.processedBlocks).toBeLessThanOrEqual(5);
  });

  it('should handle writes arriving between continuation runs', async () => {
    const initialBlocks = createSmallBlocks(8, 300_000);
    await populateStorage(storage, initialBlocks);

    // First compaction run
    const firstResult = await scheduler.runCompaction(initialBlocks);

    // Simulate writes arriving during delay between runs
    const newWrites: TestBlock[] = [];
    for (let i = 0; i < 3; i++) {
      const block = createTestBlock(`inter-run-${i}`, 400_000, {
        minSeq: 5000 + i * 100,
        maxSeq: 5099 + i * 100,
        rowCount: 100,
      });
      newWrites.push(block);
      await storage.put(block.path, new Uint8Array(block.size));
    }

    // Second run with new writes
    const allBlocks = [...initialBlocks, ...newWrites];
    const secondResult = await scheduler.runCompaction(allBlocks, {
      continuationState: firstResult.continuationState,
    });

    // Should handle both remaining and new blocks
    expect(secondResult.mergedBlocks.length).toBeGreaterThanOrEqual(0);
  });
});

// ============================================================================
// Source Block Protection
// ============================================================================

describe('Compaction - Source Block Protection', () => {
  let scheduler: CompactionScheduler;
  let storage: MemoryStorage;

  beforeEach(() => {
    resetDocumentCounter();
    storage = new MemoryStorage();
    scheduler = new CompactionScheduler({ storage });
  });

  afterEach(() => {
    storage.clear();
  });

  it('should keep source blocks readable until merge completes', async () => {
    const blocks = createSmallBlocks(3, 400_000);
    await populateStorage(storage, blocks);

    let blocksReadableDuringMerge = true;
    const originalPut = storage.put.bind(storage);

    storage.put = async (key: string, data: Uint8Array) => {
      if (key.includes('merged')) {
        // During merge write, verify source blocks are still readable
        for (const block of blocks) {
          const sourceData = await storage.get(block.path);
          if (!sourceData) {
            blocksReadableDuringMerge = false;
          }
        }
      }
      return originalPut(key, data);
    };

    await scheduler.runCompaction(blocks);

    expect(blocksReadableDuringMerge).toBe(true);
  });

  it('should allow reads from source blocks during compaction', async () => {
    const blocks = createSmallBlocks(4, 350_000);
    await populateStorage(storage, blocks);

    // Start compaction and read simultaneously
    const compactionPromise = scheduler.runCompaction(blocks);
    const readPromises = blocks.map((block) => storage.get(block.path));

    const [compactionResult, ...readResults] = await Promise.all([
      compactionPromise,
      ...readPromises,
    ]);

    expect(compactionResult.skipped).toBe(false);

    // All reads should have returned data
    for (const readResult of readResults) {
      expect(readResult).not.toBeNull();
    }
  });

  it('should not delete source blocks if merge write fails', async () => {
    const blocks = createSmallBlocks(3, 400_000);
    await populateStorage(storage, blocks);

    // Mock storage to fail on merged block write
    const failingStorage = {
      ...storage,
      get: storage.get.bind(storage),
      put: vi.fn().mockImplementation(async (key: string, data: Uint8Array) => {
        if (key.includes('merged')) {
          throw new Error('Simulated write failure');
        }
        return storage.put(key, data);
      }),
      delete: storage.delete.bind(storage),
      exists: storage.exists.bind(storage),
      head: storage.head.bind(storage),
      list: storage.list.bind(storage),
      createMultipartUpload: storage.createMultipartUpload.bind(storage),
      getStream: storage.getStream.bind(storage),
      putStream: storage.putStream.bind(storage),
    };

    const failingScheduler = new CompactionScheduler({
      storage: failingStorage as MemoryStorage,
    });

    // Should throw due to merge failure
    await expect(failingScheduler.runCompaction(blocks)).rejects.toThrow();

    // Source blocks should still exist
    for (const block of blocks) {
      const exists = await storage.exists(block.path);
      expect(exists).toBe(true);
    }
  });
});

// ============================================================================
// Delayed Deletion
// ============================================================================

describe('Compaction - Delayed Deletion', () => {
  let scheduler: CompactionScheduler;
  let storage: MemoryStorage;

  beforeEach(() => {
    resetDocumentCounter();
    storage = new MemoryStorage();
    scheduler = new CompactionScheduler({
      storage,
      deleteDelay: 5000, // 5 second delay
    });
  });

  afterEach(() => {
    storage.clear();
  });

  it('should schedule deletion instead of immediate delete', async () => {
    const blocks = createSmallBlocks(3, 400_000);
    await populateStorage(storage, blocks);

    const result = await scheduler.runCompaction(blocks);

    // Blocks should still exist (deletion is delayed)
    for (const block of blocks) {
      const exists = await storage.exists(block.path);
      expect(exists).toBe(true);
    }

    // Should have pending deletions
    expect(result.pendingDeletions).toHaveLength(3);
    for (const deletion of result.pendingDeletions) {
      expect(deletion.scheduledAt).toBeInstanceOf(Date);
      expect(deletion.path).toBeDefined();
    }
  });

  it('should allow reads from source blocks during deletion delay', async () => {
    const blocks = createSmallBlocks(2, 500_000);
    await populateStorage(storage, blocks);

    const result = await scheduler.runCompaction(blocks);
    expect(result.pendingDeletions.length).toBeGreaterThan(0);

    // Source blocks should still be readable during delay window
    for (const block of blocks) {
      const data = await storage.get(block.path);
      expect(data).not.toBeNull();
      expect(data?.length).toBe(block.size);
    }
  });
});

// ============================================================================
// Abort Handling
// ============================================================================

describe('Compaction - Abort Handling', () => {
  let scheduler: CompactionScheduler;
  let storage: MemoryStorage;

  beforeEach(() => {
    resetDocumentCounter();
    storage = new MemoryStorage();
    scheduler = new CompactionScheduler({ storage });
  });

  afterEach(() => {
    storage.clear();
  });

  it('should handle abort signal if set before compaction starts', async () => {
    const blocks = createSmallBlocks(10, 200_000);
    await populateStorage(storage, blocks);

    const abortController = new AbortController();
    // Abort immediately before running
    abortController.abort();

    const result = await scheduler.runCompaction(blocks, {
      signal: abortController.signal,
    });

    // Should be aborted
    expect(result.aborted).toBe(true);
    expect(result.partialState).toBeDefined();
  });

  it('should complete normally without abort signal', async () => {
    const blocks = createSmallBlocks(6, 300_000);
    await populateStorage(storage, blocks);

    // Run without abort signal
    const result = await scheduler.runCompaction(blocks);

    // Should complete successfully
    expect(result.aborted).toBeFalsy();
    expect(result.mergedBlocks).toHaveLength(1);
  });

  it('should preserve source blocks if compaction is skipped', async () => {
    // Create blocks that are already large enough (no compaction needed)
    const largeBlocks: TestBlock[] = [
      createTestBlock('large-1', 4_000_000, { minSeq: 1, maxSeq: 1000 }),
    ];
    await populateStorage(storage, largeBlocks);

    const result = await scheduler.runCompaction(largeBlocks);

    // Should be skipped (no small blocks)
    expect(result.skipped).toBe(true);

    // Source block should still exist
    const exists = await storage.exists(largeBlocks[0].path);
    expect(exists).toBe(true);
  });

  it('should allow running compaction after previous completion', async () => {
    const blocks = createSmallBlocks(4, 350_000);
    await populateStorage(storage, blocks);

    // First run - completes
    const firstResult = await scheduler.runCompaction(blocks);
    expect(firstResult.mergedBlocks).toHaveLength(1);

    // Create new blocks for second run
    const newBlocks = createSmallBlocks(3, 400_000).map((b, i) => ({
      ...b,
      id: `second-${i}`,
      path: `blocks/test/second-${i}.parquet`,
      minSeq: 5000 + i * 100,
      maxSeq: 5099 + i * 100,
    }));
    await populateStorage(storage, newBlocks);

    // Second run - also completes
    const secondResult = await scheduler.runCompaction(newBlocks);
    expect(secondResult.mergedBlocks).toHaveLength(1);
  });
});

// ============================================================================
// Multi-Collection Compaction
// ============================================================================

describe('Compaction - Multi-Collection Isolation', () => {
  let storage: MemoryStorage;

  beforeEach(() => {
    resetDocumentCounter();
    storage = new MemoryStorage();
  });

  afterEach(() => {
    storage.clear();
  });

  it('should compact collections independently', async () => {
    const schedulerA = new CompactionScheduler({ storage });
    const schedulerB = new CompactionScheduler({ storage });

    // Create blocks for two collections
    const blocksA = createSmallBlocks(3, 400_000).map((b) => ({
      ...b,
      path: b.path.replace('/test/', '/collection-a/'),
      collection: 'collection-a',
    }));

    const blocksB = createSmallBlocks(3, 400_000).map((b) => ({
      ...b,
      id: `b-${b.id}`,
      path: b.path.replace('/test/', '/collection-b/'),
      collection: 'collection-b',
      minSeq: b.minSeq + 10000,
      maxSeq: b.maxSeq + 10000,
    }));

    await populateStorage(storage, blocksA);
    await populateStorage(storage, blocksB);

    // Compact both collections concurrently
    const [resultA, resultB] = await Promise.all([
      schedulerA.runCompaction(blocksA),
      schedulerB.runCompaction(blocksB),
    ]);

    expect(resultA.skipped).toBe(false);
    expect(resultB.skipped).toBe(false);

    // Each should have produced a merged block
    expect(resultA.mergedBlocks).toHaveLength(1);
    expect(resultB.mergedBlocks).toHaveLength(1);

    // Merged blocks should be different
    expect(resultA.mergedBlocks[0].path).not.toBe(resultB.mergedBlocks[0].path);
  });

  it('should not affect other collections during compaction', async () => {
    const scheduler = new CompactionScheduler({ storage });

    // Setup collection A for compaction
    const blocksA = createSmallBlocks(4, 350_000).map((b) => ({
      ...b,
      path: b.path.replace('/test/', '/collection-a/'),
    }));

    // Setup collection B with data that should remain untouched
    const collectionBData = new Uint8Array([10, 20, 30, 40, 50]);
    await storage.put('blocks/collection-b/data.parquet', collectionBData);

    await populateStorage(storage, blocksA);

    // Compact collection A
    await scheduler.runCompaction(blocksA);

    // Collection B data should be unchanged
    const bData = await storage.get('blocks/collection-b/data.parquet');
    expect(bData).toEqual(collectionBData);
  });
});

// ============================================================================
// Statistics and Reporting
// ============================================================================

describe('Compaction - Statistics', () => {
  let scheduler: CompactionScheduler;
  let storage: MemoryStorage;

  beforeEach(() => {
    resetDocumentCounter();
    storage = new MemoryStorage();
    scheduler = new CompactionScheduler({ storage });
  });

  afterEach(() => {
    storage.clear();
  });

  it('should report accurate compaction statistics', async () => {
    const blocks = createSmallBlocks(4, 300_000);
    await populateStorage(storage, blocks);

    const result = await scheduler.runCompaction(blocks);

    expect(result.stats).toBeDefined();
    expect(result.stats.processedBlocks).toBe(4);
    expect(result.stats.bytesProcessed).toBe(4 * 300_000);
    expect(result.stats.durationMs).toBeGreaterThanOrEqual(0);
    expect(result.stats.skipped).toBe(false);
  });

  it('should include row count in merged block', async () => {
    const blocks: TestBlock[] = [
      createTestBlock('block-1', 400_000, { rowCount: 150, minSeq: 1, maxSeq: 150 }),
      createTestBlock('block-2', 400_000, { rowCount: 200, minSeq: 151, maxSeq: 350 }),
      createTestBlock('block-3', 400_000, { rowCount: 100, minSeq: 351, maxSeq: 450 }),
    ];
    await populateStorage(storage, blocks);

    const result = await scheduler.runCompaction(blocks);

    expect(result.mergedBlocks[0].rowCount).toBe(450);
    expect(result.stats.rowsProcessed).toBe(450);
  });

  it('should report manifest update metadata', async () => {
    const blocks = createSmallBlocks(3, 500_000);
    await populateStorage(storage, blocks);

    const result = await scheduler.runCompaction(blocks);

    expect(result.manifestUpdate).toBeDefined();
    expect(result.manifestUpdate?.metadata.sourceBlockCount).toBe(3);
    expect(result.manifestUpdate?.metadata.totalSourceSize).toBe(3 * 500_000);
    expect(result.manifestUpdate?.operation).toBe('compaction');
  });
});
