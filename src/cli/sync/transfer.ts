/**
 * Transfer Utilities for MongoLake Sync
 *
 * Bandwidth throttling, chunked transfers, retry policies, and concurrency control.
 */

import { FileSystemStorage } from '../../storage/index.js';
import { RemoteClient } from './remote-client.js';
import { computeFileHash } from './state-manager.js';
import type { FileState, UploadProgress } from './types.js';

// ============================================================================
// Transfer Rate Utilities
// ============================================================================

export function calculateTransferRate(bytes: number, durationMs: number): number {
  if (durationMs === 0) return 0;
  return Math.round((bytes / durationMs) * 1000);
}

export function formatTransferRate(bytesPerSecond: number): string {
  if (bytesPerSecond === 0) return '0 B/s';

  const units = ['B/s', 'KB/s', 'MB/s', 'GB/s'];
  const k = 1024;
  const i = Math.floor(Math.log(bytesPerSecond) / Math.log(k));

  return `${(bytesPerSecond / Math.pow(k, i)).toFixed(2)} ${units[i]}`;
}

export function estimateTimeRemaining(bytesRemaining: number, bytesPerSecond: number): number {
  if (bytesPerSecond === 0) return Infinity;
  return Math.round((bytesRemaining / bytesPerSecond) * 1000);
}

// ============================================================================
// Bandwidth Throttling
// ============================================================================

export class BandwidthThrottler {
  private maxBytesPerSecond: number;
  private bytesThisSecond: number = 0;
  private lastReset: number = Date.now();
  private totalBytes: number = 0;
  private startTime: number = Date.now();

  constructor(options: { maxBytesPerSecond: number }) {
    this.maxBytesPerSecond = options.maxBytesPerSecond;
  }

  async throttle(bytes: number): Promise<void> {
    const now = Date.now();

    // Reset counter every second
    if (now - this.lastReset >= 1000) {
      this.bytesThisSecond = 0;
      this.lastReset = now;
    }

    // If we've exceeded the limit, wait
    if (this.bytesThisSecond + bytes > this.maxBytesPerSecond) {
      const waitTime = 1000 - (now - this.lastReset);
      if (waitTime > 0) {
        await new Promise((resolve) => setTimeout(resolve, waitTime));
        this.bytesThisSecond = 0;
        this.lastReset = Date.now();
      }
    }

    this.bytesThisSecond += bytes;
    this.totalBytes += bytes;
  }

  getAverageSpeed(): number {
    const elapsed = (Date.now() - this.startTime) / 1000;
    if (elapsed === 0) return 0;
    return this.totalBytes / elapsed;
  }
}

export function parseBandwidthLimit(limit: string): number {
  const match = limit.match(/^(\d+)(KB|MB|GB)?(?:\/s)?$/i);
  if (!match) return parseInt(limit, 10);

  const value = parseInt(match[1]!, 10);
  const unit = (match[2] || '').toUpperCase();

  switch (unit) {
    case 'KB':
      return value * 1024;
    case 'MB':
      return value * 1024 * 1024;
    case 'GB':
      return value * 1024 * 1024 * 1024;
    default:
      return value;
  }
}

// ============================================================================
// Retry Policy
// ============================================================================

export class RetryPolicy {
  private maxRetries: number;
  private baseDelayMs: number;
  private maxDelayMs: number;
  private jitterPercent: number;

  constructor(options: {
    maxRetries: number;
    baseDelayMs: number;
    maxDelayMs?: number;
    jitterPercent?: number;
  }) {
    this.maxRetries = options.maxRetries;
    this.baseDelayMs = options.baseDelayMs;
    this.maxDelayMs = options.maxDelayMs ?? 30000;
    this.jitterPercent = options.jitterPercent ?? 0;
  }

  getDelay(attempt: number): number {
    const delay = Math.min(this.baseDelayMs * Math.pow(2, attempt), this.maxDelayMs);
    return delay;
  }

  getDelayWithJitter(attempt: number): number {
    const baseDelay = this.getDelay(attempt);
    const jitterRange = baseDelay * (this.jitterPercent / 100);
    const jitter = (Math.random() - 0.5) * 2 * jitterRange;
    return Math.round(baseDelay + jitter);
  }

  get maxAttempts(): number {
    return this.maxRetries + 1;
  }
}

export function isRetryableError(error: unknown): boolean {
  if (error instanceof Error) {
    const message = error.message.toLowerCase();
    return (
      message.includes('econnreset') ||
      message.includes('etimedout') ||
      message.includes('fetch')
    );
  }

  if (typeof error === 'object' && error !== null && 'status' in error) {
    const status = (error as { status: number }).status;
    return status === 429 || status >= 500;
  }

  return false;
}

export async function fetchWithRetry(
  url: string,
  init: RequestInit,
  options: { maxRetries: number; baseDelayMs: number }
): Promise<Response> {
  const policy = new RetryPolicy({
    maxRetries: options.maxRetries,
    baseDelayMs: options.baseDelayMs,
  });

  let lastError: Error | null = null;

  for (let attempt = 0; attempt <= options.maxRetries; attempt++) {
    try {
      const response = await fetch(url, init);
      if (response.ok || !isRetryableError({ status: response.status })) {
        return response;
      }
      throw new Error(`HTTP ${response.status}`);
    } catch (error) {
      lastError = error as Error;
      if (!isRetryableError(error) || attempt === options.maxRetries) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, policy.getDelay(attempt)));
    }
  }

  throw new Error(`Failed after max retries: ${lastError?.message}`);
}

export function parseRetryAfter(header: string | null): number | null {
  if (!header) return null;
  const seconds = parseInt(header, 10);
  if (isNaN(seconds)) return null;
  return seconds * 1000;
}

// ============================================================================
// Concurrency Control
// ============================================================================

export const DEFAULT_CONCURRENCY = 5;

export class ConcurrencyLimiter {
  private maxConcurrent: number;
  private active: number = 0;
  private queue: Array<() => void> = [];

  constructor(maxConcurrent: number) {
    this.maxConcurrent = maxConcurrent;
  }

  get activeCount(): number {
    return this.active;
  }

  async acquire(): Promise<void> {
    if (this.active < this.maxConcurrent) {
      this.active++;
      return;
    }

    return new Promise((resolve) => {
      this.queue.push(() => {
        this.active++;
        resolve();
      });
    });
  }

  release(): void {
    this.active--;
    const next = this.queue.shift();
    if (next) {
      next();
    }
  }
}

export async function processFilesInParallel<T>(
  files: Array<{ path: string; size: number; hash: string }>,
  processor: (file: { path: string; size: number; hash: string }) => Promise<T>,
  options: { concurrency: number }
): Promise<T[]> {
  const limiter = new ConcurrencyLimiter(options.concurrency);
  const results: T[] = [];

  await Promise.all(
    files.map(async (file) => {
      await limiter.acquire();
      try {
        const result = await processor(file);
        results.push(result);
      } finally {
        limiter.release();
      }
    })
  );

  return results;
}

// ============================================================================
// Checksum Verification
// ============================================================================

export async function verifyChecksum(data: Uint8Array, expectedHash: string): Promise<boolean> {
  const actualHash = await computeFileHash(data);
  return actualHash === expectedHash;
}

export async function downloadWithVerification(
  database: string,
  remote: string,
  file: { path: string; hash: string },
  accessToken: string | null,
  options: { maxRetries: number }
): Promise<{ data: Uint8Array; verified: boolean }> {
  const client = new RemoteClient(remote, accessToken);

  for (let attempt = 0; attempt <= options.maxRetries; attempt++) {
    const data = await client.downloadFile(database, file.path);
    const isValid = await verifyChecksum(data, file.hash);

    if (isValid) {
      return { data, verified: true };
    }

    if (attempt === options.maxRetries) {
      throw new Error(
        `Checksum verification failed for ${file.path} after ${options.maxRetries} retries`
      );
    }
  }

  throw new Error('Unexpected error in download verification');
}

// ============================================================================
// Chunked Transfers
// ============================================================================

export const MULTIPART_THRESHOLD = 100 * 1024 * 1024; // 100MB

export function splitIntoChunks(data: Uint8Array, chunkSize: number): Uint8Array[] {
  const chunks: Uint8Array[] = [];
  for (let i = 0; i < data.length; i += chunkSize) {
    chunks.push(data.slice(i, i + chunkSize));
  }
  return chunks;
}

export async function calculateChunkChecksum(chunk: Uint8Array): Promise<string> {
  return computeFileHash(chunk);
}

export class ChunkedUploader {
  private chunkSize: number;
  private database: string;
  private remote: string;
  private accessToken: string | null;
  private _isResumable: boolean;
  private uploadProgress: Map<string, { completedChunks: number; totalChunks: number }> =
    new Map();

  constructor(options: {
    chunkSize: number;
    database: string;
    remote: string;
    accessToken: string | null;
    resumable?: boolean;
  }) {
    this.chunkSize = options.chunkSize;
    this.database = options.database;
    this.remote = options.remote;
    this.accessToken = options.accessToken;
    this._isResumable = options.resumable ?? false;
  }

  get isResumable(): boolean {
    return this._isResumable;
  }

  getUploadProgress(uploadId: string): { completedChunks: number; totalChunks: number } | null {
    return this.uploadProgress.get(uploadId) || null;
  }

  async upload(
    filePath: string,
    data: Uint8Array
  ): Promise<{ success: boolean; chunksUploaded: number }> {
    const chunks = splitIntoChunks(data, this.chunkSize);
    let chunksUploaded = 0;

    for (const chunk of chunks) {
      const response = await fetch(
        `${this.remote.replace(/\/$/, '')}/api/sync/${this.database}/chunks/${encodeURIComponent(filePath)}/${chunksUploaded}`,
        {
          method: 'PUT',
          headers: {
            'Content-Type': 'application/octet-stream',
            ...(this.accessToken ? { Authorization: `Bearer ${this.accessToken}` } : {}),
          },
          body: chunk,
        }
      );

      if (!response.ok) {
        return { success: false, chunksUploaded };
      }

      chunksUploaded++;
    }

    return { success: true, chunksUploaded };
  }

  async resume(
    _uploadId: string,
    data: Uint8Array,
    previousProgress: UploadProgress
  ): Promise<{ success: boolean; resumedFrom: number; chunksUploaded: number }> {
    const chunks = splitIntoChunks(data, this.chunkSize);
    const startChunk = previousProgress.completedChunks;
    let chunksUploaded = 0;

    for (let i = startChunk; i < chunks.length; i++) {
      const response = await fetch(
        `${this.remote.replace(/\/$/, '')}/api/sync/${this.database}/chunks/${encodeURIComponent(previousProgress.filePath)}/${i}`,
        {
          method: 'PUT',
          headers: {
            'Content-Type': 'application/octet-stream',
            ...(this.accessToken ? { Authorization: `Bearer ${this.accessToken}` } : {}),
          },
          body: chunks[i],
        }
      );

      if (!response.ok) {
        return { success: false, resumedFrom: startChunk, chunksUploaded };
      }

      chunksUploaded++;
    }

    return { success: true, resumedFrom: startChunk, chunksUploaded };
  }
}

export class ChunkedDownloader {
  private database: string;
  private remote: string;
  private accessToken: string | null;

  constructor(options: { database: string; remote: string; accessToken: string | null }) {
    this.database = options.database;
    this.remote = options.remote;
    this.accessToken = options.accessToken;
  }

  async download(filePath: string): Promise<{ data: Uint8Array; chunksDownloaded: number }> {
    // First, get chunk metadata
    const metaResponse = await fetch(
      `${this.remote.replace(/\/$/, '')}/api/sync/${this.database}/chunks/${encodeURIComponent(filePath)}/meta`,
      {
        headers: this.accessToken ? { Authorization: `Bearer ${this.accessToken}` } : {},
      }
    );

    const meta = (await metaResponse.json()) as { totalChunks: number; chunkSize: number };
    const chunks: Uint8Array[] = [];

    for (let i = 0; i < meta.totalChunks; i++) {
      const response = await fetch(
        `${this.remote.replace(/\/$/, '')}/api/sync/${this.database}/chunks/${encodeURIComponent(filePath)}/${i}`,
        {
          headers: this.accessToken ? { Authorization: `Bearer ${this.accessToken}` } : {},
        }
      );

      const buffer = await response.arrayBuffer();
      chunks.push(new Uint8Array(buffer));
    }

    // Combine chunks
    const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
    const combined = new Uint8Array(totalLength);
    let offset = 0;
    for (const chunk of chunks) {
      combined.set(chunk, offset);
      offset += chunk.length;
    }

    return { data: combined, chunksDownloaded: meta.totalChunks };
  }
}

// ============================================================================
// File Operations
// ============================================================================

export async function uploadChangedFiles(
  database: string,
  remote: string,
  files: Array<{ path: string; size: number; hash: string }>,
  dataPath: string,
  accessToken: string | null
): Promise<{ success: boolean; filesUploaded: number }> {
  const storage = new FileSystemStorage(dataPath);
  const client = new RemoteClient(remote, accessToken);
  let filesUploaded = 0;

  for (const file of files) {
    const data = await storage.get(file.path);
    if (data) {
      await client.uploadFile(database, file.path, data);
      filesUploaded++;
    }
  }

  return { success: true, filesUploaded };
}

export async function downloadChangedFiles(
  database: string,
  remote: string,
  files: FileState[],
  dataPath: string,
  accessToken: string | null
): Promise<{ success: boolean; filesDownloaded: number }> {
  const storage = new FileSystemStorage(dataPath);
  const client = new RemoteClient(remote, accessToken);
  let filesDownloaded = 0;

  for (const file of files) {
    const data = await client.downloadFile(database, file.path);
    await storage.put(file.path, data);
    filesDownloaded++;
  }

  return { success: true, filesDownloaded };
}

export async function resumeSync(
  database: string,
  dataPath: string,
  remote: string,
  savedState: { operation: 'push' | 'pull'; pendingFiles: string[] },
  accessToken: string | null
): Promise<{ filesProcessed: number }> {
  const storage = new FileSystemStorage(dataPath);
  const client = new RemoteClient(remote, accessToken);
  let filesProcessed = 0;

  for (const filePath of savedState.pendingFiles) {
    if (savedState.operation === 'push') {
      const data = await storage.get(filePath);
      if (data) {
        await client.uploadFile(database, filePath, data);
        filesProcessed++;
      }
    } else {
      const data = await client.downloadFile(database, filePath);
      await storage.put(filePath, data);
      filesProcessed++;
    }
  }

  return { filesProcessed };
}
