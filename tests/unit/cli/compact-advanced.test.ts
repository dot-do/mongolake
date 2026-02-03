/**
 * Tests for MongoLake Compact Command - Advanced Functionality
 *
 * Tests the advanced compaction functionality including:
 * - Compaction triggering
 * - Collection targeting
 * - Progress reporting
 * - Comprehensive error handling
 * - Tombstone removal
 * - Read performance optimization
 * - Compaction statistics
 * - Abort and resume
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { mockStorage, mockScheduler } from './compact-common';

// Mock the storage module
vi.mock('../../../src/storage/index.js', () => ({
  FileSystemStorage: vi.fn(() => mockStorage),
}));

// Mock the compaction scheduler
vi.mock('../../../src/compaction/scheduler.js', () => ({
  CompactionScheduler: vi.fn(() => mockScheduler),
}));

// ============================================================================
// Compaction Triggering Tests
// ============================================================================

describe('CLI Compact - Compaction Triggering', () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  it('should export triggerCompaction function', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.triggerCompaction).toBeDefined();
    expect(typeof module.triggerCompaction).toBe('function');
  });

  it('should trigger compaction immediately when called', async () => {
    const module = await import('../../../src/cli/compact.js');

    const result = await module.triggerCompaction({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      immediate: true,
    });

    expect(result).toBeDefined();
    expect(result.triggered).toBe(true);
    expect(result.startedAt).toBeInstanceOf(Date);
  });

  it('should schedule compaction for later execution', async () => {
    const module = await import('../../../src/cli/compact.js');

    const result = await module.triggerCompaction({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      immediate: false,
      delay: 60000,
    });

    expect(result).toBeDefined();
    expect(result.scheduled).toBe(true);
    expect(result.scheduledFor).toBeDefined();
    expect(result.scheduledFor.getTime()).toBeGreaterThan(Date.now());
  });

  it('should detect when compaction is already running', async () => {
    const module = await import('../../../src/cli/compact.js');

    const firstCompaction = module.triggerCompaction({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      immediate: true,
    });

    const secondResult = await module.triggerCompaction({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      immediate: true,
    });

    expect(secondResult.alreadyRunning).toBe(true);
    expect(secondResult.triggered).toBe(false);

    await firstCompaction;
  });

  it('should queue compaction when another is running', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.queueCompaction).toBeDefined();

    const queueResult = await module.queueCompaction({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
    });

    expect(queueResult).toBeDefined();
    expect(queueResult.queued).toBe(true);
    expect(queueResult.position).toBeGreaterThanOrEqual(0);
  });

  it('should support priority-based compaction queue', async () => {
    const module = await import('../../../src/cli/compact.js');

    await module.queueCompaction({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      priority: 'normal',
    });

    const highPriorityResult = await module.queueCompaction({
      database: 'testdb',
      collection: 'orders',
      path: '.mongolake',
      priority: 'high',
    });

    expect(highPriorityResult.position).toBe(0);
  });

  it('should trigger compaction via CLI command', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.handleCompactCommand).toBeDefined();

    const exitSpy = vi.spyOn(process, 'exit').mockImplementation(() => undefined as never);
    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    await module.handleCompactCommand(['testdb', 'users']);

    expect(exitSpy).not.toHaveBeenCalledWith(1);

    exitSpy.mockRestore();
    consoleSpy.mockRestore();
  });

  it('should support --schedule flag for delayed compaction', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('--schedule');
  });

  it('should support --priority flag for queue priority', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('--priority');
  });

  it('should emit compaction-started event when triggered', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.CompactionEventEmitter).toBeDefined();

    const emitter = new module.CompactionEventEmitter();
    const startEvents: unknown[] = [];

    emitter.on('compaction-started', (event: unknown) => startEvents.push(event));

    await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      eventEmitter: emitter,
    });

    expect(startEvents.length).toBe(1);
    expect(startEvents[0]).toMatchObject({
      database: 'testdb',
      collection: 'users',
      startedAt: expect.any(Date),
    });
  });

  it('should emit compaction-completed event when finished', async () => {
    const module = await import('../../../src/cli/compact.js');

    const emitter = new module.CompactionEventEmitter();
    const completeEvents: unknown[] = [];

    emitter.on('compaction-completed', (event: unknown) => completeEvents.push(event));

    await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      eventEmitter: emitter,
    });

    expect(completeEvents.length).toBe(1);
    expect(completeEvents[0]).toMatchObject({
      database: 'testdb',
      collection: 'users',
      completedAt: expect.any(Date),
      result: expect.any(Object),
    });
  });

  it('should cancel scheduled compaction', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.cancelCompaction).toBeDefined();

    const scheduled = await module.triggerCompaction({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      immediate: false,
      delay: 60000,
    });

    const cancelResult = await module.cancelCompaction(scheduled.compactionId);

    expect(cancelResult.cancelled).toBe(true);
    expect(cancelResult.compactionId).toBe(scheduled.compactionId);
  });

  it('should list pending compaction jobs', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.listPendingCompactions).toBeDefined();

    await module.queueCompaction({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
    });

    await module.queueCompaction({
      database: 'testdb',
      collection: 'orders',
      path: '.mongolake',
    });

    const pending = await module.listPendingCompactions('.mongolake');

    expect(pending).toBeDefined();
    expect(Array.isArray(pending)).toBe(true);
    expect(pending.length).toBeGreaterThanOrEqual(2);
    expect(pending[0]).toHaveProperty('database');
    expect(pending[0]).toHaveProperty('collection');
    expect(pending[0]).toHaveProperty('queuedAt');
  });
});

// ============================================================================
// Collection Targeting Tests
// ============================================================================

describe('CLI Compact - Collection Targeting', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockStorage.list.mockResolvedValue([
      'testdb/users/users_1705000000000_1.parquet',
      'testdb/users/users_1705000001000_2.parquet',
      'testdb/orders/orders_1705000000000_1.parquet',
      'testdb/products/products_1705000000000_1.parquet',
    ]);
    mockStorage.head.mockResolvedValue({ size: 1000000 });
  });

  it('should export compactCollection function', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.compactCollection).toBeDefined();
    expect(typeof module.compactCollection).toBe('function');
  });

  it('should compact a specific collection', async () => {
    const module = await import('../../../src/cli/compact.js');

    const result = await module.compactCollection({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
    });

    expect(result).toBeDefined();
    expect(result.database).toBe('testdb');
    expect(result.collection).toBe('users');
    expect(result.processedBlocks).toBeGreaterThanOrEqual(0);
  });

  it('should validate collection exists before compacting', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.validateCollection).toBeDefined();

    mockStorage.list.mockResolvedValue([]);

    const validation = await module.validateCollection('testdb', 'nonexistent', '.mongolake');

    expect(validation.exists).toBe(false);
    expect(validation.error).toContain('not found');
  });

  it('should support wildcard collection patterns', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.resolveCollectionPattern).toBeDefined();

    const collections = await module.resolveCollectionPattern('testdb', 'user*', '.mongolake');

    expect(collections).toBeDefined();
    expect(Array.isArray(collections)).toBe(true);
  });

  it('should compact multiple collections matching pattern', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.compactCollections).toBeDefined();

    const result = await module.compactCollections({
      database: 'testdb',
      pattern: '*',
      path: '.mongolake',
      dryRun: false,
    });

    expect(result).toBeDefined();
    expect(result.collectionsCompacted).toBeDefined();
    expect(Array.isArray(result.collectionsCompacted)).toBe(true);
  });

  it('should list all collections in database', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.listCollections).toBeDefined();

    const collections = await module.listCollections('testdb', '.mongolake');

    expect(collections).toBeDefined();
    expect(Array.isArray(collections)).toBe(true);
    expect(collections).toContain('users');
    expect(collections).toContain('orders');
    expect(collections).toContain('products');
  });

  it('should get collection statistics before compaction', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.getCollectionStats).toBeDefined();

    const stats = await module.getCollectionStats('testdb', 'users', '.mongolake');

    expect(stats).toBeDefined();
    expect(stats.blockCount).toBeGreaterThanOrEqual(0);
    expect(stats.totalSize).toBeGreaterThanOrEqual(0);
    expect(stats.smallBlockCount).toBeGreaterThanOrEqual(0);
    expect(stats.needsCompaction).toBeDefined();
  });

  it('should skip collections with no small blocks', async () => {
    const module = await import('../../../src/cli/compact.js');

    mockStorage.head.mockResolvedValue({ size: 10 * 1024 * 1024 });

    const result = await module.compactCollection({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
    });

    expect(result.skipped).toBe(true);
    expect(result.reason).toContain('no small blocks');
  });

  it('should support --all flag to compact all collections', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('--all');

    const result = await module.compactAllCollections({
      database: 'testdb',
      path: '.mongolake',
      dryRun: false,
    });

    expect(result).toBeDefined();
    expect(result.collectionsProcessed).toBeGreaterThan(0);
  });

  it('should support --exclude flag to skip specific collections', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('--exclude');

    const result = await module.compactCollections({
      database: 'testdb',
      pattern: '*',
      exclude: ['logs', 'metrics'],
      path: '.mongolake',
      dryRun: false,
    });

    expect(result.collectionsCompacted).not.toContain('logs');
    expect(result.collectionsCompacted).not.toContain('metrics');
  });
});

// ============================================================================
// Progress Reporting Tests
// ============================================================================

describe('CLI Compact - Detailed Progress Reporting', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockStorage.list.mockResolvedValue([
      'testdb/users/users_1.parquet',
      'testdb/users/users_2.parquet',
      'testdb/users/users_3.parquet',
    ]);
    mockStorage.head.mockResolvedValue({ size: 1000000 });
    mockStorage.get.mockResolvedValue(new Uint8Array(1000000));
  });

  it('should export createProgressReporter function', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.createProgressReporter).toBeDefined();
    expect(typeof module.createProgressReporter).toBe('function');
  });

  it('should report block-level progress', async () => {
    const module = await import('../../../src/cli/compact.js');

    const progressEvents: unknown[] = [];
    const reporter = module.createProgressReporter({
      onProgress: (event: unknown) => progressEvents.push(event),
    });

    await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      progressReporter: reporter,
    });

    expect(progressEvents.length).toBeGreaterThan(0);
    expect(progressEvents[0]).toMatchObject({
      phase: expect.any(String),
      currentBlock: expect.any(Number),
      totalBlocks: expect.any(Number),
    });
  });

  it('should calculate accurate ETA based on progress', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.calculateETA).toBeDefined();

    const progress = {
      bytesProcessed: 50 * 1024 * 1024,
      totalBytes: 200 * 1024 * 1024,
      elapsedMs: 10000,
    };

    const eta = module.calculateETA(progress);

    expect(eta).toBeDefined();
    expect(eta.remainingMs).toBeGreaterThan(0);
    expect(eta.estimatedCompletion).toBeInstanceOf(Date);
    expect(eta.remainingMs).toBeGreaterThan(20000);
    expect(eta.remainingMs).toBeLessThan(50000);
  });

  it('should report throughput in MB/s', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.calculateThroughput).toBeDefined();

    const stats = {
      bytesProcessed: 100 * 1024 * 1024,
      durationMs: 10000,
    };

    const throughput = module.calculateThroughput(stats);

    expect(throughput).toBeDefined();
    expect(throughput.bytesPerSecond).toBe(10 * 1024 * 1024);
    expect(throughput.mbPerSecond).toBe(10);
  });

  it('should format progress bar for terminal', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.formatProgressBar).toBeDefined();

    const bar = module.formatProgressBar({
      current: 75,
      total: 100,
      width: 40,
    });

    expect(bar).toBeDefined();
    expect(typeof bar).toBe('string');
    expect(bar).toContain('75%');
    expect(bar.length).toBeLessThanOrEqual(60);
  });

  it('should support JSON progress output mode', async () => {
    const module = await import('../../../src/cli/compact.js');

    const jsonOutput: string[] = [];
    const reporter = module.createProgressReporter({
      format: 'json',
      onProgress: (event: unknown) => jsonOutput.push(JSON.stringify(event)),
    });

    await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      progressReporter: reporter,
    });

    expect(jsonOutput.length).toBeGreaterThan(0);
    const parsed = JSON.parse(jsonOutput[0]);
    expect(parsed).toHaveProperty('phase');
    expect(parsed).toHaveProperty('timestamp');
  });

  it('should emit phase change events', async () => {
    const module = await import('../../../src/cli/compact.js');

    const phases: string[] = [];
    const emitter = new module.CompactEventEmitter();

    emitter.on('phase', (phase: string) => phases.push(phase));

    await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      eventEmitter: emitter,
    });

    expect(phases).toContain('analyzing');
    expect(phases).toContain('reading');
    expect(phases).toContain('merging');
    expect(phases).toContain('writing');
    expect(phases).toContain('complete');
  });
});

// ============================================================================
// Comprehensive Error Handling Tests
// ============================================================================

describe('CLI Compact - Comprehensive Error Handling', () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  it('should export CompactionError class', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.CompactionError).toBeDefined();
  });

  it('should handle storage read errors gracefully', async () => {
    const module = await import('../../../src/cli/compact.js');

    mockStorage.get.mockRejectedValue(new Error('Storage read failed'));
    mockStorage.list.mockResolvedValue(['testdb/users/data.parquet']);
    mockStorage.head.mockResolvedValue({ size: 1000000 });

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
    });

    expect(result.error).toBeDefined();
    expect(result.error.code).toBe('STORAGE_READ_ERROR');
  });

  it('should handle storage write errors gracefully', async () => {
    const module = await import('../../../src/cli/compact.js');

    mockStorage.put.mockRejectedValue(new Error('Storage write failed'));
    mockStorage.list.mockResolvedValue(['testdb/users/data.parquet']);
    mockStorage.head.mockResolvedValue({ size: 1000000 });
    mockStorage.get.mockResolvedValue(new Uint8Array(1000));

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
    });

    expect(result.error).toBeDefined();
    expect(result.error.code).toBe('STORAGE_WRITE_ERROR');
  });

  it('should handle corrupted parquet files', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.validateParquetFile).toBeDefined();

    const corruptedData = new Uint8Array([0, 0, 0, 0]);
    const validation = await module.validateParquetFile(corruptedData);

    expect(validation.valid).toBe(false);
    expect(validation.error).toContain('invalid');
  });

  it('should handle insufficient disk space', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.checkDiskSpace).toBeDefined();

    const result = await module.checkDiskSpace('.mongolake', 1000 * 1024 * 1024 * 1024);

    expect(result.sufficient).toBe(false);
    expect(result.available).toBeDefined();
    expect(result.required).toBe(1000 * 1024 * 1024 * 1024);
  });

  it('should rollback on partial failure', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.rollbackCompaction).toBeDefined();

    const partialState = {
      sourceBlocks: ['block1.parquet', 'block2.parquet'],
      mergedBlock: 'merged.parquet',
      manifestBackup: { version: 1 },
    };

    const rollbackResult = await module.rollbackCompaction(partialState, '.mongolake');

    expect(rollbackResult.success).toBe(true);
    expect(rollbackResult.restoredBlocks).toContain('block1.parquet');
    expect(rollbackResult.restoredBlocks).toContain('block2.parquet');
    expect(rollbackResult.removedMerged).toBe(true);
  });

  it('should provide actionable error messages', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.formatErrorMessage).toBeDefined();

    const error = new module.CompactionError('STORAGE_WRITE_ERROR', 'Failed to write merged block');
    const formatted = module.formatErrorMessage(error);

    expect(formatted).toContain('STORAGE_WRITE_ERROR');
    expect(formatted).toContain('suggestion');
    expect(formatted.toLowerCase()).toContain('disk space');
  });

  it('should handle interrupt signals (SIGINT)', async () => {
    const module = await import('../../../src/cli/compact.js');

    const controller = new AbortController();
    controller.abort();

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      signal: controller.signal,
    });

    expect(result.aborted).toBe(true);
    expect(result.cleanedUp).toBe(true);
  });

  it('should estimate memory required', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.estimateMemoryRequired).toBeDefined();

    const blocks = [
      { size: 1024 * 1024 * 1024 },
      { size: 1024 * 1024 * 1024 },
    ];

    const estimate = module.estimateMemoryRequired(blocks);

    expect(estimate).toBeDefined();
    expect(estimate.required).toBeGreaterThan(0);
    expect(estimate.available).toBeDefined();
    expect(estimate.sufficient).toBeDefined();
  });
});

// ============================================================================
// Tombstone Removal Tests
// ============================================================================

describe('CLI Compact - Tombstone Removal', () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  it('should export removeTombstones function', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.removeTombstones).toBeDefined();
    expect(typeof module.removeTombstones).toBe('function');
  });

  it('should identify tombstone documents during compaction', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.identifyTombstones).toBeDefined();
    expect(typeof module.identifyTombstones).toBe('function');

    const blocks = [
      {
        id: 'block1',
        path: 'testdb/users/users_1.parquet',
        size: 1000000,
        rowCount: 100,
        minSeq: 1,
        maxSeq: 100,
        createdAt: new Date(),
        tombstoneCount: 5,
      },
      {
        id: 'block2',
        path: 'testdb/users/users_2.parquet',
        size: 500000,
        rowCount: 50,
        minSeq: 101,
        maxSeq: 150,
        createdAt: new Date(),
        tombstoneCount: 10,
      },
    ];

    const tombstones = await module.identifyTombstones(blocks);
    expect(tombstones).toBeDefined();
    expect(tombstones.totalCount).toBe(15);
    expect(tombstones.blocksWithTombstones).toHaveLength(2);
  });

  it('should remove tombstones when compacting', async () => {
    const module = await import('../../../src/cli/compact.js');

    const options = {
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      removeTombstones: true,
    };

    const result = await module.runCompact(options);
    expect(result).toBeDefined();
    expect(result.tombstonesRemoved).toBeDefined();
    expect(typeof result.tombstonesRemoved).toBe('number');
  });

  it('should support --remove-tombstones flag', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('--remove-tombstones');
  });

  it('should filter out tombstone documents from merged output', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.filterTombstones).toBeDefined();

    const documents = [
      { _id: '1', name: 'Alice', _deleted: false },
      { _id: '2', name: 'Bob', _deleted: true },
      { _id: '3', name: 'Charlie', _deleted: false },
      { _id: '4', name: 'David', _deleted: true },
    ];

    const filtered = await module.filterTombstones(documents);
    expect(filtered).toHaveLength(2);
    expect(filtered.every((d: { _deleted: boolean }) => !d._deleted)).toBe(true);
  });
});

// ============================================================================
// Read Performance Optimization Tests
// ============================================================================

describe('CLI Compact - Read Performance Optimization', () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  it('should export optimizeForReads function', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.optimizeForReads).toBeDefined();
    expect(typeof module.optimizeForReads).toBe('function');
  });

  it('should support --optimize flag', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('--optimize');
  });

  it('should build zone maps for frequently queried columns', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.buildZoneMaps).toBeDefined();

    const blocks = [
      {
        id: 'block1',
        path: 'data_1.parquet',
        minValues: { age: 18, score: 0.5 },
        maxValues: { age: 35, score: 0.9 },
      },
      {
        id: 'block2',
        path: 'data_2.parquet',
        minValues: { age: 30, score: 0.7 },
        maxValues: { age: 65, score: 1.0 },
      },
    ];

    const zoneMaps = await module.buildZoneMaps(blocks, ['age', 'score']);
    expect(zoneMaps).toBeDefined();
    expect(zoneMaps.age).toBeDefined();
    expect(zoneMaps.age.globalMin).toBe(18);
    expect(zoneMaps.age.globalMax).toBe(65);
  });

  it('should calculate optimal row group size', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.calculateOptimalRowGroupSize).toBeDefined();

    const stats = {
      avgRowSize: 256,
      totalRows: 100000,
      memoryBudget: 64 * 1024 * 1024,
    };

    const optimalSize = await module.calculateOptimalRowGroupSize(stats);
    expect(optimalSize).toBeDefined();
    expect(optimalSize).toBeGreaterThan(0);
    expect(optimalSize).toBeLessThanOrEqual(1000000);
  });
});

// ============================================================================
// Compaction Statistics Tests
// ============================================================================

describe('CLI Compact - Compaction Statistics', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockStorage.list.mockResolvedValue([
      'testdb/users/users_1705000000000_1.parquet',
      'testdb/users/users_1705000001000_2.parquet',
      'testdb/users/users_1705000002000_3.parquet',
    ]);
    mockStorage.head.mockResolvedValue({ size: 1000000 });
    mockStorage.get.mockResolvedValue(new Uint8Array(1000000));
  });

  it('should export getCompactionStats function', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.getCompactionStats).toBeDefined();
    expect(typeof module.getCompactionStats).toBe('function');
  });

  it('should track space savings from compaction', async () => {
    const module = await import('../../../src/cli/compact.js');

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
    });

    expect(result.stats).toBeDefined();
    expect(result.stats.spaceSaved).toBeDefined();
    expect(result.stats.spaceSavedPercent).toBeDefined();
    expect(typeof result.stats.spaceSaved).toBe('number');
    expect(typeof result.stats.spaceSavedPercent).toBe('number');
  });

  it('should track file count reduction', async () => {
    const module = await import('../../../src/cli/compact.js');

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
    });

    expect(result.stats).toBeDefined();
    expect(result.stats.filesBefore).toBeDefined();
    expect(result.stats.filesAfter).toBeDefined();
    expect(result.stats.filesRemoved).toBeDefined();
    expect(result.stats.filesAfter).toBeLessThan(result.stats.filesBefore);
  });

  it('should generate compaction summary report', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.generateCompactionReport).toBeDefined();

    const stats = {
      filesBefore: 50,
      filesAfter: 5,
      sizeBefore: 100 * 1024 * 1024,
      sizeAfter: 80 * 1024 * 1024,
      rowsProcessed: 1000000,
      tombstonesRemoved: 5000,
      durationMs: 30000,
    };

    const report = await module.generateCompactionReport(stats);
    expect(report).toBeDefined();
    expect(typeof report).toBe('string');
    expect(report).toContain('Files');
    expect(report).toContain('Size');
    expect(report).toContain('Duration');
  });

  it('should store compaction history', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.getCompactionHistory).toBeDefined();
    expect(module.recordCompaction).toBeDefined();

    await module.recordCompaction({
      database: 'testdb',
      collection: 'users',
      timestamp: new Date(),
      stats: {
        filesBefore: 10,
        filesAfter: 2,
        duration: 5000,
      },
    });

    const history = await module.getCompactionHistory('testdb', 'users', '.mongolake');
    expect(history).toBeDefined();
    expect(Array.isArray(history)).toBe(true);
    expect(history.length).toBeGreaterThan(0);
  });
});

// ============================================================================
// Abort and Resume Tests
// ============================================================================

describe('CLI Compact - Abort and Resume', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockStorage.list.mockResolvedValue([
      'testdb/users/users_1705000000000_1.parquet',
      'testdb/users/users_1705000001000_2.parquet',
      'testdb/users/users_1705000002000_3.parquet',
    ]);
    mockStorage.head.mockResolvedValue({ size: 1000000 });
    mockStorage.get.mockResolvedValue(new Uint8Array(1000000));
  });

  it('should support graceful abort via AbortController', async () => {
    const module = await import('../../../src/cli/compact.js');

    const controller = new AbortController();
    controller.abort();

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      signal: controller.signal,
    });

    expect(result).toBeDefined();
    expect(result.aborted).toBe(true);
  });

  it('should save checkpoint state for resume', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.saveCheckpoint).toBeDefined();
    expect(module.loadCheckpoint).toBeDefined();

    const checkpoint = {
      database: 'testdb',
      collection: 'users',
      lastProcessedBlock: 'block_5',
      processedBlocks: 5,
      remainingBlocks: 10,
      startedAt: new Date(),
    };

    await module.saveCheckpoint(checkpoint, '.mongolake');
    const loaded = await module.loadCheckpoint('testdb', 'users', '.mongolake');

    expect(loaded).toBeDefined();
    expect(loaded?.lastProcessedBlock).toBe('block_5');
    expect(loaded?.processedBlocks).toBe(5);
  });

  it('should resume from checkpoint', async () => {
    const module = await import('../../../src/cli/compact.js');

    await module.saveCheckpoint({
      database: 'testdb',
      collection: 'users',
      lastProcessedBlock: 'block_5',
      processedBlocks: 5,
      remainingBlocks: 10,
      startedAt: new Date(Date.now() - 60000),
    }, '.mongolake');

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      resume: true,
    });

    expect(result).toBeDefined();
    expect(result.resumedFrom).toBe('block_5');
  });

  it('should clean up checkpoint on successful completion', async () => {
    const module = await import('../../../src/cli/compact.js');

    await module.saveCheckpoint({
      database: 'testdb',
      collection: 'users',
      lastProcessedBlock: 'block_5',
      processedBlocks: 5,
      remainingBlocks: 5,
      startedAt: new Date(),
    }, '.mongolake');

    await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      resume: true,
    });

    const checkpoint = await module.loadCheckpoint('testdb', 'users', '.mongolake');
    expect(checkpoint).toBeNull();
  });

  it('should support --force-restart to ignore checkpoint', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('--force-restart');

    await module.saveCheckpoint({
      database: 'testdb',
      collection: 'users',
      lastProcessedBlock: 'block_5',
      processedBlocks: 5,
      remainingBlocks: 10,
      startedAt: new Date(),
    }, '.mongolake');

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      forceRestart: true,
    });

    expect(result).toBeDefined();
    expect(result.resumedFrom).toBeUndefined();
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('CLI Compact - Integration', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockStorage.list.mockResolvedValue([
      'testdb/users/users_1705000000000_1.parquet',
      'testdb/users/users_1705000001000_2.parquet',
      'testdb/users/users_1705000002000_3.parquet',
    ]);
    mockStorage.head.mockResolvedValue({ size: 1000000 });
    mockStorage.get.mockResolvedValue(new Uint8Array(1000000));
  });

  it('should run end-to-end compaction', async () => {
    const module = await import('../../../src/cli/compact.js');

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
    });

    expect(result).toBeDefined();
    expect(result.success).toBe(true);
    expect(result.processedBlocks).toBeGreaterThan(0);
  });

  it('should support collection pattern matching', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.compactCollections).toBeDefined();

    const result = await module.compactCollections({
      database: 'testdb',
      pattern: 'user*',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
    });

    expect(result).toBeDefined();
    expect(result.collectionsCompacted).toBeDefined();
    expect(Array.isArray(result.collectionsCompacted)).toBe(true);
  });

  it('should support database-wide compaction', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.compactDatabase).toBeDefined();

    const result = await module.compactDatabase({
      database: 'testdb',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
    });

    expect(result).toBeDefined();
    expect(result.collectionsProcessed).toBeGreaterThan(0);
    expect(result.totalStats).toBeDefined();
  });

  it('should support --max-size option to limit compaction scope', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('--max-size');

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      maxSize: 10 * 1024 * 1024,
    });

    expect(result).toBeDefined();
    expect(result.stats.bytesProcessed).toBeLessThanOrEqual(10 * 1024 * 1024);
  });

  it('should support --min-age option to only compact old files', async () => {
    const module = await import('../../../src/cli/compact.js');
    expect(module.COMPACT_HELP_TEXT).toContain('--min-age');

    const result = await module.runCompact({
      database: 'testdb',
      collection: 'users',
      path: '.mongolake',
      dryRun: false,
      verbose: false,
      minAge: 86400000,
    });

    expect(result).toBeDefined();
    if (result.processedFiles) {
      for (const file of result.processedFiles) {
        expect(Date.now() - file.createdAt.getTime()).toBeGreaterThanOrEqual(86400000);
      }
    }
  });
});
