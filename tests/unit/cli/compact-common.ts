/**
 * Shared test utilities and mocks for compact tests
 */

import { vi } from 'vitest';

// Mock storage module
export const mockStorage = {
  get: vi.fn().mockResolvedValue(new Uint8Array([1, 2, 3])),
  put: vi.fn().mockResolvedValue(undefined),
  delete: vi.fn().mockResolvedValue(undefined),
  list: vi.fn().mockResolvedValue([]),
  head: vi.fn().mockResolvedValue({ size: 100 }),
  exists: vi.fn().mockResolvedValue(true),
};

// Mock the compaction scheduler
export const mockScheduler = {
  identifyBlocksNeedingCompaction: vi.fn().mockResolvedValue([]),
  runCompaction: vi.fn().mockResolvedValue({
    skipped: false,
    processedBlocks: 3,
    mergedBlocks: [{ path: 'merged.parquet', size: 4000000, rowCount: 1000, minSeq: 1, maxSeq: 3 }],
    stats: {
      bytesProcessed: 3000000,
      rowsProcessed: 1000,
      compressionRatio: 0.75,
    },
    hasMore: false,
    pendingDeletions: [],
  }),
};

// Shared interfaces
export interface BlockMetadata {
  id: string;
  path: string;
  size: number;
  rowCount: number;
  minSeq: number;
  maxSeq: number;
  createdAt: Date;
}

export interface CompactionResult {
  skipped: boolean;
  reason?: string;
  processedBlocks: number;
  mergedBlocks: Array<{
    path: string;
    size: number;
    rowCount: number;
    minSeq: number;
    maxSeq: number;
  }>;
  stats?: {
    bytesProcessed: number;
    rowsProcessed: number;
    compressionRatio: number;
  };
  hasMore: boolean;
  pendingDeletions?: string[];
}

// Utility functions for tests
export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const k = 1024;
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(2)} ${units[i]}`;
}

export function formatDuration(ms: number): string {
  if (ms < 1000) {
    return `${ms}ms`;
  }
  const seconds = ms / 1000;
  if (seconds < 60) {
    return `${seconds.toFixed(2)}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds % 60;
  return `${minutes}m ${remainingSeconds.toFixed(0)}s`;
}

// Setup function for compact tests
export function setupCompactMocks() {
  vi.mock('../../../src/storage/index.js', () => ({
    FileSystemStorage: vi.fn(() => mockStorage),
  }));

  vi.mock('../../../src/compaction/scheduler.js', () => ({
    CompactionScheduler: vi.fn(() => mockScheduler),
  }));
}

export function resetCompactMocks() {
  mockStorage.get.mockReset();
  mockStorage.get.mockResolvedValue(new Uint8Array([1, 2, 3]));
  mockStorage.put.mockReset();
  mockStorage.put.mockResolvedValue(undefined);
  mockStorage.list.mockReset();
  mockStorage.list.mockResolvedValue([]);
  mockStorage.head.mockReset();
  mockStorage.head.mockResolvedValue({ size: 100 });
  mockStorage.delete.mockReset();
  mockStorage.delete.mockResolvedValue(undefined);
}
