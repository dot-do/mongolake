/**
 * Compaction Scheduler Tests
 *
 * Tests for the compaction scheduler that handles:
 * - Merging small blocks (<2MB) into larger blocks (4MB target)
 * - Triggered by DO alarms after flush
 * - Processing incrementally (max 10 blocks per run)
 * - Updating manifest with new block references
 * - Deleting old blocks after merge
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  CompactionScheduler,
  type CompactionOptions,
  type CompactionResult,
  type CompactionState,
  type BlockMetadata,
  type ManifestUpdate,
} from '../../../src/compaction/scheduler.js';
import { MemoryStorage } from '../../../src/storage/index.js';

// ============================================================================
// Test Block Type
// ============================================================================

interface TestBlock extends BlockMetadata {
  id: string;
  path: string;
  size: number;
  rowCount: number;
  minSeq: number;
  maxSeq: number;
  createdAt: Date;
}

// ============================================================================
// Helper Functions
// ============================================================================

function createBlock(
  id: string,
  size: number,
  options: Partial<Omit<TestBlock, 'id' | 'size'>> = {}
): TestBlock {
  return {
    id,
    path: `blocks/${id}.parquet`,
    size,
    rowCount: options.rowCount ?? Math.floor(size / 100),
    minSeq: options.minSeq ?? 1,
    maxSeq: options.maxSeq ?? options.rowCount ?? Math.floor(size / 100),
    createdAt: options.createdAt ?? new Date(),
  };
}

function createSmallBlock(id: string, size: number = 500_000): TestBlock {
  // Default 500KB, well under 2MB threshold
  return createBlock(id, size);
}

function createLargeBlock(id: string, size: number = 4_000_000): TestBlock {
  // Default 4MB, at target size
  return createBlock(id, size);
}

// ============================================================================
// 1. Identify Small Blocks Needing Compaction
// ============================================================================

describe('CompactionScheduler - Identify Small Blocks', () => {
  let scheduler: CompactionScheduler;
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
    scheduler = new CompactionScheduler({ storage });
  });

  it('should identify blocks smaller than 2MB as needing compaction', async () => {
    const blocks: TestBlock[] = [
      createSmallBlock('block-1', 500_000),  // 500KB - needs compaction
      createSmallBlock('block-2', 1_000_000), // 1MB - needs compaction
      createLargeBlock('block-3', 4_000_000), // 4MB - does not need compaction
    ];

    const result = await scheduler.identifyBlocksNeedingCompaction(blocks);

    expect(result).toHaveLength(2);
    expect(result.map((b) => b.id)).toContain('block-1');
    expect(result.map((b) => b.id)).toContain('block-2');
    expect(result.map((b) => b.id)).not.toContain('block-3');
  });

  it('should respect custom size threshold', async () => {
    const customScheduler = new CompactionScheduler({
      storage,
      minBlockSize: 1_000_000, // 1MB threshold instead of 2MB
    });

    const blocks: TestBlock[] = [
      createSmallBlock('block-1', 500_000),  // 500KB - needs compaction
      createSmallBlock('block-2', 1_500_000), // 1.5MB - does NOT need compaction with custom threshold
    ];

    const result = await customScheduler.identifyBlocksNeedingCompaction(blocks);

    expect(result).toHaveLength(1);
    expect(result[0].id).toBe('block-1');
  });

  it('should return empty array when all blocks are large enough', async () => {
    const blocks: TestBlock[] = [
      createLargeBlock('block-1', 3_000_000),
      createLargeBlock('block-2', 5_000_000),
    ];

    const result = await scheduler.identifyBlocksNeedingCompaction(blocks);

    expect(result).toHaveLength(0);
  });

  it('should return empty array for empty input', async () => {
    const result = await scheduler.identifyBlocksNeedingCompaction([]);

    expect(result).toHaveLength(0);
  });

  it('should sort small blocks by sequence for optimal merging', async () => {
    const blocks: TestBlock[] = [
      createBlock('block-3', 500_000, { minSeq: 300, maxSeq: 400 }),
      createBlock('block-1', 500_000, { minSeq: 1, maxSeq: 100 }),
      createBlock('block-2', 500_000, { minSeq: 101, maxSeq: 200 }),
    ];

    const result = await scheduler.identifyBlocksNeedingCompaction(blocks);

    expect(result[0].id).toBe('block-1');
    expect(result[1].id).toBe('block-2');
    expect(result[2].id).toBe('block-3');
  });
});

// ============================================================================
// 2. Merge Multiple Blocks into Single Block
// ============================================================================

describe('CompactionScheduler - Merge Blocks', () => {
  let scheduler: CompactionScheduler;
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
    scheduler = new CompactionScheduler({ storage });
  });

  it('should merge multiple small blocks into single larger block', async () => {
    const blocks: TestBlock[] = [
      createBlock('block-1', 1_000_000, { rowCount: 1000, minSeq: 1, maxSeq: 1000 }),
      createBlock('block-2', 1_000_000, { rowCount: 1000, minSeq: 1001, maxSeq: 2000 }),
      createBlock('block-3', 1_000_000, { rowCount: 1000, minSeq: 2001, maxSeq: 3000 }),
    ];

    // Simulate block data in storage
    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.mergeBlocks(blocks);

    expect(result.mergedBlock).toBeDefined();
    expect(result.mergedBlock.rowCount).toBe(3000);
    expect(result.mergedBlock.minSeq).toBe(1);
    expect(result.mergedBlock.maxSeq).toBe(3000);
    expect(result.sourceBlocks).toHaveLength(3);
  });

  it('should target 4MB merged block size', async () => {
    const blocks: TestBlock[] = [
      createBlock('block-1', 1_000_000, { rowCount: 1000 }),
      createBlock('block-2', 1_500_000, { rowCount: 1500 }),
      createBlock('block-3', 1_200_000, { rowCount: 1200 }),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.mergeBlocks(blocks);

    // Merged block should be approximately sum of source sizes (3.7MB)
    expect(result.mergedBlock.size).toBeGreaterThan(3_000_000);
    expect(result.mergedBlock.size).toBeLessThanOrEqual(4_500_000);
  });

  it('should generate unique ID for merged block', async () => {
    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result1 = await scheduler.mergeBlocks(blocks);
    const result2 = await scheduler.mergeBlocks(blocks);

    expect(result1.mergedBlock.id).not.toBe(result2.mergedBlock.id);
  });

  it('should handle single block merge (no-op)', async () => {
    const blocks: TestBlock[] = [createSmallBlock('block-1', 1_500_000)];

    await storage.put(blocks[0].path, new Uint8Array(blocks[0].size));

    const result = await scheduler.mergeBlocks(blocks);

    // Single block merge should still produce a result
    expect(result.mergedBlock).toBeDefined();
    expect(result.sourceBlocks).toHaveLength(1);
  });

  it('should throw on empty blocks array', async () => {
    await expect(scheduler.mergeBlocks([])).rejects.toThrow(/cannot merge.*empty/i);
  });

  it('should write merged block to storage', async () => {
    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.mergeBlocks(blocks);

    const written = await storage.exists(result.mergedBlock.path);
    expect(written).toBe(true);
  });
});

// ============================================================================
// 3. Limit Blocks Processed Per Run
// ============================================================================

describe('CompactionScheduler - Processing Limits', () => {
  let scheduler: CompactionScheduler;
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
    scheduler = new CompactionScheduler({ storage, maxBlocksPerRun: 10 });
  });

  it('should process at most 10 blocks per run by default', async () => {
    // Create 20 small blocks
    const blocks: TestBlock[] = Array.from({ length: 20 }, (_, i) =>
      createSmallBlock(`block-${i}`, 300_000)
    );

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.runCompaction(blocks);

    // Should only process first 10 blocks
    expect(result.processedBlocks).toBeLessThanOrEqual(10);
    expect(result.hasMore).toBe(true);
  });

  it('should respect custom maxBlocksPerRun option', async () => {
    const customScheduler = new CompactionScheduler({
      storage,
      maxBlocksPerRun: 5,
    });

    const blocks: TestBlock[] = Array.from({ length: 10 }, (_, i) =>
      createSmallBlock(`block-${i}`, 300_000)
    );

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await customScheduler.runCompaction(blocks);

    expect(result.processedBlocks).toBeLessThanOrEqual(5);
  });

  it('should indicate no more work when all blocks processed', async () => {
    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.runCompaction(blocks);

    expect(result.hasMore).toBe(false);
  });

  it('should track total blocks remaining', async () => {
    const blocks: TestBlock[] = Array.from({ length: 25 }, (_, i) =>
      createSmallBlock(`block-${i}`, 300_000)
    );

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.runCompaction(blocks);

    expect(result.remainingBlocks).toBeGreaterThan(0);
    expect(result.remainingBlocks).toBeLessThan(25);
  });

  it('should return state for continuation', async () => {
    const blocks: TestBlock[] = Array.from({ length: 15 }, (_, i) =>
      createSmallBlock(`block-${i}`, 300_000)
    );

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.runCompaction(blocks);

    expect(result.continuationState).toBeDefined();
    expect(result.continuationState?.lastProcessedSeq).toBeGreaterThan(0);
  });
});

// ============================================================================
// 4. Schedule Continuation via Alarm
// ============================================================================

describe('CompactionScheduler - Alarm Scheduling', () => {
  let scheduler: CompactionScheduler;
  let storage: MemoryStorage;
  let mockAlarmScheduler: { schedule: ReturnType<typeof vi.fn> };

  beforeEach(() => {
    storage = new MemoryStorage();
    mockAlarmScheduler = { schedule: vi.fn() };
    scheduler = new CompactionScheduler({
      storage,
      alarmScheduler: mockAlarmScheduler,
    });
  });

  it('should schedule alarm when more work remains', async () => {
    const blocks: TestBlock[] = Array.from({ length: 15 }, (_, i) =>
      createSmallBlock(`block-${i}`, 300_000)
    );

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    await scheduler.runCompaction(blocks);

    expect(mockAlarmScheduler.schedule).toHaveBeenCalled();
  });

  it('should not schedule alarm when all work complete', async () => {
    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    await scheduler.runCompaction(blocks);

    expect(mockAlarmScheduler.schedule).not.toHaveBeenCalled();
  });

  it('should schedule alarm with continuation state', async () => {
    const blocks: TestBlock[] = Array.from({ length: 15 }, (_, i) =>
      createSmallBlock(`block-${i}`, 300_000)
    );

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    await scheduler.runCompaction(blocks);

    expect(mockAlarmScheduler.schedule).toHaveBeenCalledWith(
      expect.objectContaining({
        continuationState: expect.any(Object),
      })
    );
  });

  it('should respect minimum delay between alarms', async () => {
    const blocks: TestBlock[] = Array.from({ length: 15 }, (_, i) =>
      createSmallBlock(`block-${i}`, 300_000)
    );

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    await scheduler.runCompaction(blocks);

    expect(mockAlarmScheduler.schedule).toHaveBeenCalledWith(
      expect.objectContaining({
        delayMs: expect.any(Number),
      })
    );

    const call = mockAlarmScheduler.schedule.mock.calls[0][0];
    expect(call.delayMs).toBeGreaterThanOrEqual(100); // Minimum 100ms delay
  });

  it('should resume from continuation state', async () => {
    const blocks: TestBlock[] = Array.from({ length: 25 }, (_, i) =>
      createBlock(`block-${i}`, 300_000, { minSeq: i * 100 + 1, maxSeq: (i + 1) * 100 })
    );

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const firstRun = await scheduler.runCompaction(blocks);
    const continuationState = firstRun.continuationState;

    // Second run with continuation state
    const secondRun = await scheduler.runCompaction(blocks, { continuationState });

    // Should not reprocess already-processed blocks
    expect(secondRun.processedBlocks).toBeLessThanOrEqual(10);
    expect(secondRun.continuationState?.lastProcessedSeq).toBeGreaterThan(
      continuationState?.lastProcessedSeq ?? 0
    );
  });
});

// ============================================================================
// 5. Update Manifest Atomically
// ============================================================================

describe('CompactionScheduler - Manifest Updates', () => {
  let scheduler: CompactionScheduler;
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
    scheduler = new CompactionScheduler({ storage });
  });

  it('should add merged block to manifest', async () => {
    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.runCompaction(blocks);

    expect(result.manifestUpdate).toBeDefined();
    expect(result.manifestUpdate?.addedBlocks).toHaveLength(1);
    expect(result.manifestUpdate?.addedBlocks[0].id).toBe(result.mergedBlocks[0].id);
  });

  it('should remove source blocks from manifest', async () => {
    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
      createSmallBlock('block-3'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.runCompaction(blocks);

    expect(result.manifestUpdate?.removedBlocks).toHaveLength(3);
    expect(result.manifestUpdate?.removedBlocks.map((b) => b.id)).toContain('block-1');
    expect(result.manifestUpdate?.removedBlocks.map((b) => b.id)).toContain('block-2');
    expect(result.manifestUpdate?.removedBlocks.map((b) => b.id)).toContain('block-3');
  });

  it('should generate atomic manifest update entry', async () => {
    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.runCompaction(blocks);

    expect(result.manifestUpdate?.version).toBeDefined();
    expect(result.manifestUpdate?.timestamp).toBeDefined();
    expect(result.manifestUpdate?.operation).toBe('compaction');
  });

  it('should include compaction metadata in manifest entry', async () => {
    const blocks: TestBlock[] = [
      createSmallBlock('block-1', 500_000),
      createSmallBlock('block-2', 700_000),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.runCompaction(blocks);

    expect(result.manifestUpdate?.metadata).toBeDefined();
    expect(result.manifestUpdate?.metadata.sourceBlockCount).toBe(2);
    expect(result.manifestUpdate?.metadata.totalSourceSize).toBe(1_200_000);
    expect(result.manifestUpdate?.metadata.mergedSize).toBeGreaterThan(0);
  });

  it('should apply manifest update atomically', async () => {
    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.runCompaction(blocks);

    // Verify manifest was written
    const manifestPath = 'manifest/current.json';
    const manifestData = await storage.get(manifestPath);
    expect(manifestData).not.toBeNull();
  });

  it('should rollback on manifest update failure', async () => {
    const failingStorage = {
      ...storage,
      put: vi.fn().mockImplementation((key: string, data: Uint8Array) => {
        if (key.includes('manifest')) {
          throw new Error('Manifest write failed');
        }
        return storage.put(key, data);
      }),
    } as unknown as MemoryStorage;

    const failingScheduler = new CompactionScheduler({ storage: failingStorage });

    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    await expect(failingScheduler.runCompaction(blocks)).rejects.toThrow(/manifest/i);

    // Source blocks should still exist (no partial state)
    for (const block of blocks) {
      const exists = await storage.exists(block.path);
      expect(exists).toBe(true);
    }
  });
});

// ============================================================================
// 6. Delete Old Blocks After Successful Merge
// ============================================================================

describe('CompactionScheduler - Block Deletion', () => {
  let scheduler: CompactionScheduler;
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
    scheduler = new CompactionScheduler({ storage });
  });

  it('should delete source blocks after successful merge', async () => {
    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    await scheduler.runCompaction(blocks);

    // Source blocks should be deleted
    for (const block of blocks) {
      const exists = await storage.exists(block.path);
      expect(exists).toBe(false);
    }
  });

  it('should keep merged block after compaction', async () => {
    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.runCompaction(blocks);

    const mergedExists = await storage.exists(result.mergedBlocks[0].path);
    expect(mergedExists).toBe(true);
  });

  it('should not delete source blocks if merge fails', async () => {
    const failingStorage = {
      ...storage,
      put: vi.fn().mockImplementation((key: string, data: Uint8Array) => {
        if (key.includes('merged')) {
          throw new Error('Merge write failed');
        }
        return storage.put(key, data);
      }),
    } as unknown as MemoryStorage;

    const failingScheduler = new CompactionScheduler({ storage: failingStorage });

    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    await expect(failingScheduler.runCompaction(blocks)).rejects.toThrow();

    // Source blocks should still exist
    for (const block of blocks) {
      const exists = await storage.exists(block.path);
      expect(exists).toBe(true);
    }
  });

  it('should delete blocks in batch for efficiency', async () => {
    const deleteSpy = vi.spyOn(storage, 'delete');

    const blocks: TestBlock[] = Array.from({ length: 5 }, (_, i) =>
      createSmallBlock(`block-${i}`, 300_000)
    );

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    await scheduler.runCompaction(blocks);

    // All 5 blocks should be deleted
    expect(deleteSpy).toHaveBeenCalledTimes(5);
  });

  it('should support delayed deletion for safety', async () => {
    const delayedScheduler = new CompactionScheduler({
      storage,
      deleteDelay: 5000, // 5 second delay
    });

    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await delayedScheduler.runCompaction(blocks);

    // Blocks should still exist immediately (deletion is delayed)
    for (const block of blocks) {
      const exists = await storage.exists(block.path);
      expect(exists).toBe(true);
    }

    expect(result.pendingDeletions).toHaveLength(2);
    expect(result.pendingDeletions[0].scheduledAt).toBeDefined();
  });
});

// ============================================================================
// 7. Skip Compaction if No Small Blocks
// ============================================================================

describe('CompactionScheduler - Skip Compaction', () => {
  let scheduler: CompactionScheduler;
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
    scheduler = new CompactionScheduler({ storage });
  });

  it('should skip compaction when no small blocks exist', async () => {
    const blocks: TestBlock[] = [
      createLargeBlock('block-1', 4_000_000),
      createLargeBlock('block-2', 5_000_000),
    ];

    const result = await scheduler.runCompaction(blocks);

    expect(result.skipped).toBe(true);
    expect(result.reason).toMatch(/no.*small.*blocks/i);
    expect(result.mergedBlocks).toHaveLength(0);
  });

  it('should skip compaction on empty input', async () => {
    const result = await scheduler.runCompaction([]);

    expect(result.skipped).toBe(true);
    expect(result.reason).toMatch(/no.*blocks/i);
  });

  it('should report skip in result stats', async () => {
    const blocks: TestBlock[] = [createLargeBlock('block-1', 4_000_000)];

    const result = await scheduler.runCompaction(blocks);

    expect(result.stats.skipped).toBe(true);
    expect(result.stats.processedBlocks).toBe(0);
    expect(result.stats.bytesProcessed).toBe(0);
  });

  it('should not schedule alarm when skipped', async () => {
    const mockAlarmScheduler = { schedule: vi.fn() };
    const schedulerWithAlarm = new CompactionScheduler({
      storage,
      alarmScheduler: mockAlarmScheduler,
    });

    const blocks: TestBlock[] = [createLargeBlock('block-1', 4_000_000)];

    await schedulerWithAlarm.runCompaction(blocks);

    expect(mockAlarmScheduler.schedule).not.toHaveBeenCalled();
  });

  it('should skip if only one small block exists', async () => {
    const blocks: TestBlock[] = [
      createSmallBlock('block-1', 500_000),
      createLargeBlock('block-2', 4_000_000),
    ];

    const result = await scheduler.runCompaction(blocks);

    // Single small block is not worth compacting alone
    expect(result.skipped).toBe(true);
    expect(result.reason).toMatch(/insufficient.*blocks/i);
  });
});

// ============================================================================
// 8. Handle Concurrent Reads During Compaction
// ============================================================================

describe('CompactionScheduler - Concurrent Reads', () => {
  let scheduler: CompactionScheduler;
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
    scheduler = new CompactionScheduler({ storage });
  });

  it('should keep source blocks readable until merge completes', async () => {
    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    // Simulate concurrent read during compaction
    let readDuringCompaction = false;
    const originalPut = storage.put.bind(storage);
    storage.put = async (key: string, data: Uint8Array) => {
      if (key.includes('merged')) {
        // During merge write, check source blocks are still readable
        for (const block of blocks) {
          const data = await storage.get(block.path);
          if (data) readDuringCompaction = true;
        }
      }
      return originalPut(key, data);
    };

    await scheduler.runCompaction(blocks);

    expect(readDuringCompaction).toBe(true);
  });

  it('should use copy-on-write semantics', async () => {
    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.runCompaction(blocks);

    // New block should have different path than source blocks
    expect(result.mergedBlocks[0].path).not.toBe(blocks[0].path);
    expect(result.mergedBlocks[0].path).not.toBe(blocks[1].path);
  });

  it('should support read locks during compaction', async () => {
    const lockingScheduler = new CompactionScheduler({
      storage,
      useLocking: true,
    });

    const blocks: TestBlock[] = [
      createSmallBlock('block-1'),
      createSmallBlock('block-2'),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await lockingScheduler.runCompaction(blocks);

    expect(result.locksAcquired).toBeDefined();
    expect(result.locksReleased).toBe(true);
  });

  it('should not block reads on other collections', async () => {
    const blocks: TestBlock[] = [
      createSmallBlock('collection-a/block-1'),
      createSmallBlock('collection-a/block-2'),
    ];

    const otherCollectionBlock = 'collection-b/block-1';
    await storage.put(otherCollectionBlock, new Uint8Array(1000));

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    // During compaction of collection-a, collection-b should be readable
    let otherReadable = false;
    const originalPut = storage.put.bind(storage);
    storage.put = async (key: string, data: Uint8Array) => {
      if (key.includes('merged')) {
        const data = await storage.get(otherCollectionBlock);
        otherReadable = data !== null;
      }
      return originalPut(key, data);
    };

    await scheduler.runCompaction(blocks);

    expect(otherReadable).toBe(true);
  });

  it('should handle compaction abort gracefully', async () => {
    const abortController = new AbortController();

    const blocks: TestBlock[] = Array.from({ length: 10 }, (_, i) =>
      createSmallBlock(`block-${i}`, 300_000)
    );

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    // Abort during compaction
    setTimeout(() => abortController.abort(), 10);

    const result = await scheduler.runCompaction(blocks, {
      signal: abortController.signal,
    });

    expect(result.aborted).toBe(true);
    // Partially processed state should be consistent
    expect(result.partialState).toBeDefined();
  });
});

// ============================================================================
// 9. Preserve Document Ordering by Sequence
// ============================================================================

describe('CompactionScheduler - Document Ordering', () => {
  let scheduler: CompactionScheduler;
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
    scheduler = new CompactionScheduler({ storage });
  });

  it('should preserve document order by _seq in merged block', async () => {
    const blocks: TestBlock[] = [
      createBlock('block-1', 500_000, { minSeq: 1, maxSeq: 100, rowCount: 100 }),
      createBlock('block-2', 500_000, { minSeq: 101, maxSeq: 200, rowCount: 100 }),
      createBlock('block-3', 500_000, { minSeq: 201, maxSeq: 300, rowCount: 100 }),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.mergeBlocks(blocks);

    expect(result.mergedBlock.minSeq).toBe(1);
    expect(result.mergedBlock.maxSeq).toBe(300);
    expect(result.documentOrder).toBe('sequential');
  });

  it('should handle blocks with non-contiguous sequences', async () => {
    const blocks: TestBlock[] = [
      createBlock('block-1', 500_000, { minSeq: 1, maxSeq: 100, rowCount: 100 }),
      createBlock('block-2', 500_000, { minSeq: 201, maxSeq: 300, rowCount: 100 }), // Gap!
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.mergeBlocks(blocks);

    expect(result.mergedBlock.minSeq).toBe(1);
    expect(result.mergedBlock.maxSeq).toBe(300);
    expect(result.hasSequenceGaps).toBe(true);
  });

  it('should sort documents within merged block by _seq', async () => {
    // Blocks received out of order
    const blocks: TestBlock[] = [
      createBlock('block-3', 500_000, { minSeq: 201, maxSeq: 300, rowCount: 100 }),
      createBlock('block-1', 500_000, { minSeq: 1, maxSeq: 100, rowCount: 100 }),
      createBlock('block-2', 500_000, { minSeq: 101, maxSeq: 200, rowCount: 100 }),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.mergeBlocks(blocks);

    // Merged block should have correct ordering
    expect(result.mergedBlock.minSeq).toBe(1);
    expect(result.documentOrder).toBe('sequential');
  });

  it('should include row group ordering metadata', async () => {
    const blocks: TestBlock[] = [
      createBlock('block-1', 500_000, { minSeq: 1, maxSeq: 100, rowCount: 100 }),
      createBlock('block-2', 500_000, { minSeq: 101, maxSeq: 200, rowCount: 100 }),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.mergeBlocks(blocks);

    expect(result.mergedBlock.metadata).toBeDefined();
    expect(result.mergedBlock.metadata.sortedBy).toBe('_seq');
    expect(result.mergedBlock.metadata.sortOrder).toBe('ascending');
  });

  it('should validate sequence continuity', async () => {
    const blocks: TestBlock[] = [
      createBlock('block-1', 500_000, { minSeq: 1, maxSeq: 100, rowCount: 100 }),
      createBlock('block-2', 500_000, { minSeq: 50, maxSeq: 150, rowCount: 100 }), // Overlap!
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    await expect(scheduler.mergeBlocks(blocks)).rejects.toThrow(/overlapping.*sequence/i);
  });
});

// ============================================================================
// 10. Recompute Field Statistics for Merged Block
// ============================================================================

describe('CompactionScheduler - Statistics Recomputation', () => {
  let scheduler: CompactionScheduler;
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
    scheduler = new CompactionScheduler({ storage });
  });

  it('should compute aggregate min/max for merged block', async () => {
    const blocks: TestBlock[] = [
      createBlock('block-1', 500_000, { minSeq: 1, maxSeq: 100 }),
      createBlock('block-2', 500_000, { minSeq: 101, maxSeq: 200 }),
    ];

    // Add field statistics to blocks
    (blocks[0] as any).fieldStats = { age: { min: 20, max: 40 } };
    (blocks[1] as any).fieldStats = { age: { min: 25, max: 50 } };

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.mergeBlocks(blocks);

    expect(result.mergedBlock.fieldStats).toBeDefined();
    expect(result.mergedBlock.fieldStats.age.min).toBe(20);
    expect(result.mergedBlock.fieldStats.age.max).toBe(50);
  });

  it('should merge null counts from all source blocks', async () => {
    const blocks: TestBlock[] = [
      createBlock('block-1', 500_000, { rowCount: 100 }),
      createBlock('block-2', 500_000, { rowCount: 100 }),
    ];

    (blocks[0] as any).fieldStats = { email: { nullCount: 5 } };
    (blocks[1] as any).fieldStats = { email: { nullCount: 10 } };

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.mergeBlocks(blocks);

    expect(result.mergedBlock.fieldStats.email.nullCount).toBe(15);
  });

  it('should compute distinct count estimates', async () => {
    const blocks: TestBlock[] = [
      createBlock('block-1', 500_000, { rowCount: 100 }),
      createBlock('block-2', 500_000, { rowCount: 100 }),
    ];

    (blocks[0] as any).fieldStats = { status: { distinctCount: 3 } };
    (blocks[1] as any).fieldStats = { status: { distinctCount: 2 } };

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.mergeBlocks(blocks);

    // Distinct count is estimated (could be anywhere from 3 to 5)
    expect(result.mergedBlock.fieldStats.status.distinctCount).toBeGreaterThanOrEqual(3);
    expect(result.mergedBlock.fieldStats.status.distinctCount).toBeLessThanOrEqual(5);
  });

  it('should update row count statistics', async () => {
    const blocks: TestBlock[] = [
      createBlock('block-1', 500_000, { rowCount: 150 }),
      createBlock('block-2', 500_000, { rowCount: 200 }),
      createBlock('block-3', 500_000, { rowCount: 100 }),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.mergeBlocks(blocks);

    expect(result.mergedBlock.rowCount).toBe(450);
  });

  it('should preserve column-level statistics', async () => {
    const blocks: TestBlock[] = [
      createBlock('block-1', 500_000, { rowCount: 100 }),
      createBlock('block-2', 500_000, { rowCount: 100 }),
    ];

    (blocks[0] as any).columnStats = {
      name: { encoding: 'PLAIN_DICTIONARY', dictionarySize: 50 },
      age: { encoding: 'PLAIN', compressionRatio: 0.8 },
    };
    (blocks[1] as any).columnStats = {
      name: { encoding: 'PLAIN_DICTIONARY', dictionarySize: 30 },
      age: { encoding: 'PLAIN', compressionRatio: 0.75 },
    };

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.mergeBlocks(blocks);

    expect(result.mergedBlock.columnStats).toBeDefined();
    expect(result.mergedBlock.columnStats.name).toBeDefined();
    expect(result.mergedBlock.columnStats.age).toBeDefined();
  });

  it('should handle missing field statistics gracefully', async () => {
    const blocks: TestBlock[] = [
      createBlock('block-1', 500_000, { rowCount: 100 }),
      createBlock('block-2', 500_000, { rowCount: 100 }),
    ];

    // Only block-1 has field stats
    (blocks[0] as any).fieldStats = { name: { min: 'Alice', max: 'Zoe' } };

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.mergeBlocks(blocks);

    // Should still produce valid merged block
    expect(result.mergedBlock).toBeDefined();
    expect(result.mergedBlock.rowCount).toBe(200);
  });

  it('should update bloom filter for merged block', async () => {
    const blocks: TestBlock[] = [
      createBlock('block-1', 500_000, { rowCount: 100 }),
      createBlock('block-2', 500_000, { rowCount: 100 }),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.mergeBlocks(blocks);

    expect(result.mergedBlock.bloomFilter).toBeDefined();
    expect(result.mergedBlock.bloomFilter.estimatedFpp).toBeLessThan(0.05);
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('CompactionScheduler - Integration', () => {
  let scheduler: CompactionScheduler;
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
    scheduler = new CompactionScheduler({
      storage,
      maxBlocksPerRun: 10,
      minBlockSize: 2_000_000,
      targetBlockSize: 4_000_000,
    });
  });

  it('should perform complete compaction cycle', async () => {
    // Create 5 small blocks totaling ~2.5MB
    const blocks: TestBlock[] = [
      createBlock('block-1', 500_000, { minSeq: 1, maxSeq: 100, rowCount: 100 }),
      createBlock('block-2', 500_000, { minSeq: 101, maxSeq: 200, rowCount: 100 }),
      createBlock('block-3', 500_000, { minSeq: 201, maxSeq: 300, rowCount: 100 }),
      createBlock('block-4', 500_000, { minSeq: 301, maxSeq: 400, rowCount: 100 }),
      createBlock('block-5', 500_000, { minSeq: 401, maxSeq: 500, rowCount: 100 }),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.runCompaction(blocks);

    // Should have merged all 5 blocks into 1
    expect(result.mergedBlocks).toHaveLength(1);
    expect(result.mergedBlocks[0].rowCount).toBe(500);
    expect(result.mergedBlocks[0].minSeq).toBe(1);
    expect(result.mergedBlocks[0].maxSeq).toBe(500);

    // Source blocks should be deleted
    for (const block of blocks) {
      const exists = await storage.exists(block.path);
      expect(exists).toBe(false);
    }

    // Merged block should exist
    const mergedExists = await storage.exists(result.mergedBlocks[0].path);
    expect(mergedExists).toBe(true);
  });

  it('should handle multi-run compaction with continuation', async () => {
    // Create 25 small blocks requiring multiple runs
    const blocks: TestBlock[] = Array.from({ length: 25 }, (_, i) =>
      createBlock(`block-${i}`, 300_000, {
        minSeq: i * 100 + 1,
        maxSeq: (i + 1) * 100,
        rowCount: 100,
      })
    );

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    // First run
    const firstResult = await scheduler.runCompaction(blocks);
    expect(firstResult.hasMore).toBe(true);

    // Second run with continuation
    const secondResult = await scheduler.runCompaction(blocks, {
      continuationState: firstResult.continuationState,
    });

    // Should complete eventually
    expect(secondResult.processedBlocks).toBeGreaterThan(0);
  });

  it('should track total compaction statistics', async () => {
    const blocks: TestBlock[] = [
      createBlock('block-1', 800_000, { rowCount: 800 }),
      createBlock('block-2', 900_000, { rowCount: 900 }),
      createBlock('block-3', 700_000, { rowCount: 700 }),
    ];

    for (const block of blocks) {
      await storage.put(block.path, new Uint8Array(block.size));
    }

    const result = await scheduler.runCompaction(blocks);

    expect(result.stats).toBeDefined();
    expect(result.stats.processedBlocks).toBe(3);
    expect(result.stats.bytesProcessed).toBe(2_400_000);
    expect(result.stats.rowsProcessed).toBe(2400);
    expect(result.stats.durationMs).toBeGreaterThan(0);
    expect(result.stats.compressionRatio).toBeGreaterThan(0);
  });
});
