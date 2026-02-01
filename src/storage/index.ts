/**
 * Storage Abstraction Layer
 *
 * Supports multiple backends:
 * - Local filesystem (.mongolake/)
 * - Cloudflare R2
 * - S3-compatible storage
 * - In-memory (for testing)
 */

import type { R2Bucket, R2Object, R2ObjectBody, MongoLakeConfig } from '../types.js';

// ============================================================================
// Storage Interface
// ============================================================================

export interface StorageBackend {
  /** Get object by key */
  get(key: string): Promise<Uint8Array | null>;

  /** Put object */
  put(key: string, data: Uint8Array): Promise<void>;

  /** Delete object */
  delete(key: string): Promise<void>;

  /** List objects by prefix */
  list(prefix: string): Promise<string[]>;

  /** Check if object exists */
  exists(key: string): Promise<boolean>;

  /** Get object metadata (size) */
  head(key: string): Promise<{ size: number } | null>;

  /** Multipart upload for large files */
  createMultipartUpload(key: string): Promise<MultipartUpload>;
}

export interface MultipartUpload {
  uploadPart(partNumber: number, data: Uint8Array): Promise<UploadedPart>;
  complete(parts: UploadedPart[]): Promise<void>;
  abort(): Promise<void>;
}

export interface UploadedPart {
  partNumber: number;
  etag: string;
}

// ============================================================================
// Factory
// ============================================================================

export function createStorage(config: MongoLakeConfig): StorageBackend {
  if (config.local) {
    return new FileSystemStorage(config.local);
  }

  if (config.bucket) {
    return new R2Storage(config.bucket);
  }

  if (config.endpoint) {
    return new S3Storage({
      endpoint: config.endpoint,
      accessKeyId: config.accessKeyId!,
      secretAccessKey: config.secretAccessKey!,
      bucket: config.bucketName!,
    });
  }

  // Default to local .mongolake folder
  return new FileSystemStorage('.mongolake');
}

// ============================================================================
// Filesystem Storage (Local Development)
// ============================================================================

export class FileSystemStorage implements StorageBackend {
  private basePath: string;
  private fs: typeof import('node:fs/promises') | null = null;
  private path: typeof import('node:path') | null = null;

  constructor(basePath: string) {
    this.basePath = basePath;
  }

  private async ensureModules() {
    if (!this.fs) {
      this.fs = await import('node:fs/promises');
      this.path = await import('node:path');
    }
  }

  private getFullPath(key: string): string {
    return this.path!.join(this.basePath, key);
  }

  async get(key: string): Promise<Uint8Array | null> {
    await this.ensureModules();
    try {
      const buffer = await this.fs!.readFile(this.getFullPath(key));
      return new Uint8Array(buffer);
    } catch (e: unknown) {
      if ((e as NodeJS.ErrnoException).code === 'ENOENT') return null;
      throw e;
    }
  }

  async put(key: string, data: Uint8Array): Promise<void> {
    await this.ensureModules();
    const fullPath = this.getFullPath(key);
    await this.fs!.mkdir(this.path!.dirname(fullPath), { recursive: true });
    await this.fs!.writeFile(fullPath, data);
  }

  async delete(key: string): Promise<void> {
    await this.ensureModules();
    try {
      await this.fs!.unlink(this.getFullPath(key));
    } catch (e: unknown) {
      if ((e as NodeJS.ErrnoException).code !== 'ENOENT') throw e;
    }
  }

  async list(prefix: string): Promise<string[]> {
    await this.ensureModules();
    const results: string[] = [];
    const basePath = this.getFullPath(prefix);

    async function walk(dir: string, base: string, fs: typeof import('node:fs/promises'), path: typeof import('node:path')) {
      try {
        const entries = await fs.readdir(dir, { withFileTypes: true });
        for (const entry of entries) {
          const fullPath = path.join(dir, entry.name);
          const relativePath = path.join(base, entry.name);
          if (entry.isDirectory()) {
            await walk(fullPath, relativePath, fs, path);
          } else {
            results.push(relativePath);
          }
        }
      } catch (e: unknown) {
        if ((e as NodeJS.ErrnoException).code !== 'ENOENT') throw e;
      }
    }

    await walk(basePath, prefix, this.fs!, this.path!);
    return results;
  }

  async exists(key: string): Promise<boolean> {
    await this.ensureModules();
    try {
      await this.fs!.access(this.getFullPath(key));
      return true;
    } catch {
      return false;
    }
  }

  async head(key: string): Promise<{ size: number } | null> {
    await this.ensureModules();
    try {
      const stats = await this.fs!.stat(this.getFullPath(key));
      return { size: stats.size };
    } catch (e: unknown) {
      if ((e as NodeJS.ErrnoException).code === 'ENOENT') return null;
      throw e;
    }
  }

  async createMultipartUpload(key: string): Promise<MultipartUpload> {
    await this.ensureModules();
    const parts: Map<number, Uint8Array> = new Map();
    const self = this;

    return {
      async uploadPart(partNumber: number, data: Uint8Array): Promise<UploadedPart> {
        parts.set(partNumber, data);
        return { partNumber, etag: `part-${partNumber}` };
      },

      async complete(uploadedParts: UploadedPart[]): Promise<void> {
        // Concatenate parts in order
        const sortedParts = uploadedParts.sort((a, b) => a.partNumber - b.partNumber);
        const totalSize = sortedParts.reduce((sum, p) => sum + (parts.get(p.partNumber)?.length || 0), 0);
        const combined = new Uint8Array(totalSize);
        let offset = 0;
        for (const p of sortedParts) {
          const data = parts.get(p.partNumber)!;
          combined.set(data, offset);
          offset += data.length;
        }
        await self.put(key, combined);
      },

      async abort(): Promise<void> {
        parts.clear();
      },
    };
  }
}

// ============================================================================
// R2 Storage (Cloudflare)
// ============================================================================

export class R2Storage implements StorageBackend {
  constructor(private bucket: R2Bucket) {}

  async get(key: string): Promise<Uint8Array | null> {
    const obj = await this.bucket.get(key);
    if (!obj) return null;
    const buffer = await obj.arrayBuffer();
    return new Uint8Array(buffer);
  }

  async put(key: string, data: Uint8Array): Promise<void> {
    await this.bucket.put(key, data);
  }

  async delete(key: string): Promise<void> {
    await this.bucket.delete(key);
  }

  async list(prefix: string): Promise<string[]> {
    const results: string[] = [];
    let cursor: string | undefined;

    do {
      const response = await this.bucket.list({ prefix, cursor });
      for (const obj of response.objects) {
        results.push(obj.key);
      }
      cursor = response.truncated ? response.cursor : undefined;
    } while (cursor);

    return results;
  }

  async exists(key: string): Promise<boolean> {
    const obj = await this.bucket.get(key);
    return obj !== null;
  }

  async head(key: string): Promise<{ size: number } | null> {
    const obj = await this.bucket.get(key);
    if (!obj) return null;
    // R2ObjectBody doesn't directly expose size, need to get from arrayBuffer
    const buffer = await obj.arrayBuffer();
    return { size: buffer.byteLength };
  }

  async createMultipartUpload(key: string): Promise<MultipartUpload> {
    const upload = await this.bucket.createMultipartUpload(key);

    return {
      async uploadPart(partNumber: number, data: Uint8Array): Promise<UploadedPart> {
        const part = await upload.uploadPart(partNumber, data);
        return { partNumber: part.partNumber, etag: part.etag };
      },

      async complete(parts: UploadedPart[]): Promise<void> {
        await upload.complete(parts.map((p) => ({ partNumber: p.partNumber, etag: p.etag })));
      },

      async abort(): Promise<void> {
        await upload.abort();
      },
    };
  }
}

// ============================================================================
// S3 Storage (External)
// ============================================================================

export interface S3Config {
  endpoint: string;
  accessKeyId: string;
  secretAccessKey: string;
  bucket: string;
  region?: string;
}

export class S3Storage implements StorageBackend {
  private config: S3Config;

  constructor(config: S3Config) {
    this.config = {
      ...config,
      region: config.region || 'auto',
    };
  }

  private async signRequest(
    method: string,
    key: string,
    headers: Record<string, string> = {},
    body?: Uint8Array
  ): Promise<{ url: string; headers: Record<string, string> }> {
    // Simplified S3 signing - in production, use proper AWS4 signing
    const url = `${this.config.endpoint}/${this.config.bucket}/${key}`;
    const date = new Date().toISOString().replace(/[:-]|\.\d{3}/g, '');

    // Basic auth header (works with many S3-compatible services)
    const authHeaders = {
      ...headers,
      'x-amz-date': date,
      'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
      Authorization: `AWS4-HMAC-SHA256 Credential=${this.config.accessKeyId}`,
    };

    return { url, headers: authHeaders };
  }

  async get(key: string): Promise<Uint8Array | null> {
    const { url, headers } = await this.signRequest('GET', key);
    const response = await fetch(url, { headers });
    if (response.status === 404) return null;
    if (!response.ok) throw new Error(`S3 GET failed: ${response.status}`);
    const buffer = await response.arrayBuffer();
    return new Uint8Array(buffer);
  }

  async put(key: string, data: Uint8Array): Promise<void> {
    const { url, headers } = await this.signRequest('PUT', key, {
      'Content-Type': 'application/octet-stream',
      'Content-Length': String(data.length),
    });
    const response = await fetch(url, {
      method: 'PUT',
      headers,
      body: data,
    });
    if (!response.ok) throw new Error(`S3 PUT failed: ${response.status}`);
  }

  async delete(key: string): Promise<void> {
    const { url, headers } = await this.signRequest('DELETE', key);
    const response = await fetch(url, { method: 'DELETE', headers });
    if (!response.ok && response.status !== 404) {
      throw new Error(`S3 DELETE failed: ${response.status}`);
    }
  }

  async list(prefix: string): Promise<string[]> {
    const { url, headers } = await this.signRequest('GET', '', {});
    const listUrl = `${url.replace(/\/$/, '')}?list-type=2&prefix=${encodeURIComponent(prefix)}`;
    const response = await fetch(listUrl, { headers });
    if (!response.ok) throw new Error(`S3 LIST failed: ${response.status}`);

    const text = await response.text();
    // Parse XML response (simplified)
    const keys: string[] = [];
    const keyRegex = /<Key>([^<]+)<\/Key>/g;
    let match;
    while ((match = keyRegex.exec(text)) !== null) {
      keys.push(match[1]);
    }
    return keys;
  }

  async exists(key: string): Promise<boolean> {
    const { url, headers } = await this.signRequest('HEAD', key);
    const response = await fetch(url, { method: 'HEAD', headers });
    return response.ok;
  }

  async head(key: string): Promise<{ size: number } | null> {
    const { url, headers } = await this.signRequest('HEAD', key);
    const response = await fetch(url, { method: 'HEAD', headers });
    if (!response.ok) return null;
    const size = parseInt(response.headers.get('content-length') || '0', 10);
    return { size };
  }

  async createMultipartUpload(key: string): Promise<MultipartUpload> {
    // Simplified - in production, implement proper S3 multipart
    const parts: Map<number, Uint8Array> = new Map();
    const self = this;

    return {
      async uploadPart(partNumber: number, data: Uint8Array): Promise<UploadedPart> {
        parts.set(partNumber, data);
        return { partNumber, etag: `part-${partNumber}` };
      },

      async complete(uploadedParts: UploadedPart[]): Promise<void> {
        const sortedParts = uploadedParts.sort((a, b) => a.partNumber - b.partNumber);
        const totalSize = sortedParts.reduce((sum, p) => sum + (parts.get(p.partNumber)?.length || 0), 0);
        const combined = new Uint8Array(totalSize);
        let offset = 0;
        for (const p of sortedParts) {
          const data = parts.get(p.partNumber)!;
          combined.set(data, offset);
          offset += data.length;
        }
        await self.put(key, combined);
      },

      async abort(): Promise<void> {
        parts.clear();
      },
    };
  }
}

// ============================================================================
// Memory Storage (Testing)
// ============================================================================

export class MemoryStorage implements StorageBackend {
  private data: Map<string, Uint8Array> = new Map();

  async get(key: string): Promise<Uint8Array | null> {
    return this.data.get(key) || null;
  }

  async put(key: string, data: Uint8Array): Promise<void> {
    this.data.set(key, data);
  }

  async delete(key: string): Promise<void> {
    this.data.delete(key);
  }

  async list(prefix: string): Promise<string[]> {
    return Array.from(this.data.keys()).filter((k) => k.startsWith(prefix));
  }

  async exists(key: string): Promise<boolean> {
    return this.data.has(key);
  }

  async head(key: string): Promise<{ size: number } | null> {
    const data = this.data.get(key);
    if (!data) return null;
    return { size: data.length };
  }

  async createMultipartUpload(key: string): Promise<MultipartUpload> {
    const parts: Map<number, Uint8Array> = new Map();
    const self = this;

    return {
      async uploadPart(partNumber: number, data: Uint8Array): Promise<UploadedPart> {
        parts.set(partNumber, data);
        return { partNumber, etag: `part-${partNumber}` };
      },

      async complete(uploadedParts: UploadedPart[]): Promise<void> {
        const sortedParts = uploadedParts.sort((a, b) => a.partNumber - b.partNumber);
        const totalSize = sortedParts.reduce((sum, p) => sum + (parts.get(p.partNumber)?.length || 0), 0);
        const combined = new Uint8Array(totalSize);
        let offset = 0;
        for (const p of sortedParts) {
          const data = parts.get(p.partNumber)!;
          combined.set(data, offset);
          offset += data.length;
        }
        await self.put(key, combined);
      },

      async abort(): Promise<void> {
        parts.clear();
      },
    };
  }

  /** Clear all data (for testing) */
  clear(): void {
    this.data.clear();
  }
}
