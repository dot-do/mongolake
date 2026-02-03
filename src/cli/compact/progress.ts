/**
 * Progress reporting utilities for the Compact command
 *
 * @module cli/compact/progress
 */

import type {
  ProgressReporter,
  ProgressReporterOptions,
  ProgressEvent,
  ETAInput,
  ETAResult,
  ThroughputInput,
  ThroughputResult,
  ProgressBarInput,
  ProgressSummaryInput,
} from './types.js';

/**
 * Create a progress reporter with optional throttling and formatting
 */
export function createProgressReporter(options: ProgressReporterOptions): ProgressReporter {
  let lastReportTime = 0;
  const throttleMs = options.throttleMs ?? 0;

  return {
    report(event: ProgressEvent): void {
      const now = Date.now();
      if (throttleMs > 0 && now - lastReportTime < throttleMs) {
        return;
      }
      lastReportTime = now;

      if (options.format === 'json') {
        options.onProgress({ ...event, timestamp: new Date().toISOString() });
      } else {
        options.onProgress(event);
      }
    },
  };
}

/**
 * Calculate ETA based on current progress
 */
export function calculateETA(progress: ETAInput): ETAResult {
  const { bytesProcessed, totalBytes, elapsedMs } = progress;

  if (bytesProcessed === 0) {
    return {
      remainingMs: Infinity,
      estimatedCompletion: new Date(Date.now() + 86400000), // 1 day fallback
    };
  }

  const bytesPerMs = bytesProcessed / elapsedMs;
  const remainingBytes = totalBytes - bytesProcessed;
  const remainingMs = Math.round(remainingBytes / bytesPerMs);

  return {
    remainingMs,
    estimatedCompletion: new Date(Date.now() + remainingMs),
  };
}

/**
 * Calculate throughput statistics
 */
export function calculateThroughput(stats: ThroughputInput): ThroughputResult {
  const { bytesProcessed, rowsProcessed, durationMs } = stats;

  const seconds = durationMs / 1000;
  const bytesPerSecond = Math.round(bytesProcessed / seconds);
  const mbPerSecond = bytesPerSecond / (1024 * 1024);

  const result: ThroughputResult = {
    bytesPerSecond,
    mbPerSecond: Math.round(mbPerSecond * 100) / 100,
  };

  if (rowsProcessed !== undefined) {
    result.rowsPerSecond = Math.round(rowsProcessed / seconds);
  }

  return result;
}

/**
 * Format a progress bar for terminal display
 */
export function formatProgressBar(input: ProgressBarInput): string {
  const { current, total, width } = input;
  const percent = Math.round((current / total) * 100);
  const filled = Math.round((current / total) * width);
  const empty = width - filled;

  const bar = '='.repeat(filled) + '-'.repeat(empty);
  return `[${bar}] ${percent}%`;
}

/**
 * Format a human-readable progress summary
 */
export function formatProgressSummary(input: ProgressSummaryInput): string {
  const { phase, currentBlock, totalBlocks, bytesProcessed, totalBytes } = input;
  const percent = Math.round((bytesProcessed / totalBytes) * 100);

  return `${phase}: ${currentBlock}/${totalBlocks} blocks (${percent}% complete)`;
}
