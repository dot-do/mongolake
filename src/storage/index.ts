/**
 * Storage Abstraction Layer
 *
 * Supports multiple backends:
 * - Local filesystem (.mongolake/)
 * - Cloudflare R2
 * - S3-compatible storage
 * - In-memory (for testing)
 */

import type { R2Bucket, MongoLakeConfig } from '../types.js';

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
// Helpers
// ============================================================================

/**
 * Concatenates an array of Uint8Array parts into a single Uint8Array.
 * Parts are concatenated in the order provided.
 */
export function concatenateParts(parts: Uint8Array[]): Uint8Array {
  const totalSize = parts.reduce((sum, part) => sum + part.length, 0);
  const combined = new Uint8Array(totalSize);
  let offset = 0;
  for (const part of parts) {
    combined.set(part, offset);
    offset += part.length;
  }
  return combined;
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
        const sortedData = sortedParts.map((p) => parts.get(p.partNumber)!);
        const combined = concatenateParts(sortedData);
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
    const obj = await this.bucket.head(key);
    if (!obj) return null;
    return { size: obj.size };
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

// AWS Signature Version 4 helper functions
const EMPTY_PAYLOAD_HASH = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';

/**
 * Convert a Uint8Array to a hex string
 */
function toHex(data: Uint8Array): string {
  return Array.from(data)
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
}

/**
 * Compute SHA-256 hash of data and return as hex string
 */
async function sha256Hex(data: string | Uint8Array): Promise<string> {
  const encoder = new TextEncoder();
  const dataBytes = typeof data === 'string' ? encoder.encode(data) : data;
  const hashBuffer = await crypto.subtle.digest('SHA-256', dataBytes);
  return toHex(new Uint8Array(hashBuffer));
}

/**
 * Compute HMAC-SHA256 and return raw bytes
 */
async function hmacSha256(key: Uint8Array, data: string): Promise<Uint8Array> {
  const encoder = new TextEncoder();
  const cryptoKey = await crypto.subtle.importKey(
    'raw',
    key,
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );
  const signature = await crypto.subtle.sign('HMAC', cryptoKey, encoder.encode(data));
  return new Uint8Array(signature);
}

/**
 * Derive the AWS Signature Version 4 signing key
 */
async function getSigningKey(
  secretKey: string,
  dateStamp: string,
  region: string,
  service: string
): Promise<Uint8Array> {
  const encoder = new TextEncoder();
  const kDate = await hmacSha256(encoder.encode('AWS4' + secretKey), dateStamp);
  const kRegion = await hmacSha256(kDate, region);
  const kService = await hmacSha256(kRegion, service);
  const kSigning = await hmacSha256(kService, 'aws4_request');
  return kSigning;
}

export class S3Storage implements StorageBackend {
  private config: S3Config;

  constructor(config: S3Config) {
    this.config = {
      ...config,
      region: config.region || 'auto',
    };
  }

  /**
   * Sign an S3 request using AWS Signature Version 4.
   * This implementation properly hashes the payload for request integrity.
   */
  private async signRequest(
    method: string,
    key: string,
    headers: Record<string, string> = {},
    body?: Uint8Array
  ): Promise<{ url: string; headers: Record<string, string> }> {
    const service = 's3';
    const region = this.config.region || 'auto';

    // Parse endpoint to get host
    const endpointUrl = new URL(this.config.endpoint);
    const host = endpointUrl.host;

    // Build the URL path
    const encodedKey = key
      .split('/')
      .map((segment) => encodeURIComponent(segment))
      .join('/');
    const url = `${this.config.endpoint}/${this.config.bucket}/${encodedKey}`;
    const canonicalUri = `/${this.config.bucket}/${encodedKey}`;

    // Get timestamps
    const now = new Date();
    const amzDate = now.toISOString().replace(/[:-]|\.\d{3}/g, '');
    const dateStamp = amzDate.substring(0, 8);

    // Calculate payload hash - this is the security fix
    // Previously used UNSIGNED-PAYLOAD which disables integrity checking
    const payloadHash = body ? await sha256Hex(body) : EMPTY_PAYLOAD_HASH;

    // Build canonical headers (must be sorted and lowercase)
    const canonicalHeaders: Record<string, string> = {
      host: host,
      'x-amz-content-sha256': payloadHash,
      'x-amz-date': amzDate,
      ...Object.fromEntries(
        Object.entries(headers).map(([k, v]) => [k.toLowerCase(), v])
      ),
    };

    // Sort headers alphabetically
    const sortedHeaderKeys = Object.keys(canonicalHeaders).sort();
    const canonicalHeadersString = sortedHeaderKeys
      .map((k) => `${k}:${canonicalHeaders[k]}`)
      .join('\n');
    const signedHeaders = sortedHeaderKeys.join(';');

    // Build canonical request
    const canonicalQueryString = ''; // No query params for basic operations
    const canonicalRequest = [
      method,
      canonicalUri,
      canonicalQueryString,
      canonicalHeadersString + '\n',
      signedHeaders,
      payloadHash,
    ].join('\n');

    // Calculate hash of canonical request
    const canonicalRequestHash = await sha256Hex(canonicalRequest);

    // Build string to sign
    const algorithm = 'AWS4-HMAC-SHA256';
    const credentialScope = `${dateStamp}/${region}/${service}/aws4_request`;
    const stringToSign = [
      algorithm,
      amzDate,
      credentialScope,
      canonicalRequestHash,
    ].join('\n');

    // Calculate signature
    const signingKey = await getSigningKey(
      this.config.secretAccessKey,
      dateStamp,
      region,
      service
    );
    const signatureBytes = await hmacSha256(signingKey, stringToSign);
    const signature = toHex(signatureBytes);

    // Build authorization header
    const authorization = [
      `${algorithm} Credential=${this.config.accessKeyId}/${credentialScope}`,
      `SignedHeaders=${signedHeaders}`,
      `Signature=${signature}`,
    ].join(', ');

    // Build final headers
    const authHeaders: Record<string, string> = {
      ...headers,
      host: host,
      'x-amz-date': amzDate,
      'x-amz-content-sha256': payloadHash,
      Authorization: authorization,
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
    // Pass body to signRequest for proper payload hash computation
    const { url, headers } = await this.signRequest(
      'PUT',
      key,
      {
        'Content-Type': 'application/octet-stream',
        'Content-Length': String(data.length),
      },
      data
    );
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
        const sortedData = sortedParts.map((p) => parts.get(p.partNumber)!);
        const combined = concatenateParts(sortedData);
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
        const sortedData = sortedParts.map((p) => parts.get(p.partNumber)!);
        const combined = concatenateParts(sortedData);
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
