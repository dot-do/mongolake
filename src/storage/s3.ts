/**
 * S3 Storage Backend
 *
 * Optional S3-compatible storage with AWS Signature Version 4 signing.
 * Import separately to avoid bundling S3 code when not needed:
 *
 * ```ts
 * import { S3Storage } from 'mongolake/storage/s3';
 * ```
 */

import type { StorageBackend, MultipartUpload } from './index.js';
import { createBufferedMultipartUpload, validateStorageKey, validateStoragePrefix } from './index.js';
import { createSafeErrorMessage, sanitizeConfig } from '../utils/sanitize-error.js';

// ============================================================================
// S3 Configuration
// ============================================================================

export interface S3Config {
  endpoint: string;
  accessKeyId: string;
  secretAccessKey: string;
  bucket: string;
  region?: string;
}

// ============================================================================
// AWS Signature Version 4 Helper Functions
// ============================================================================

const EMPTY_PAYLOAD_HASH = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';

/**
 * Convert a Uint8Array to a hex string
 */
export function toHex(data: Uint8Array): string {
  return Array.from(data)
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
}

/**
 * Compute SHA-256 hash of data and return as hex string
 */
export async function sha256Hex(data: string | Uint8Array): Promise<string> {
  const encoder = new TextEncoder();
  const dataBytes = typeof data === 'string' ? encoder.encode(data) : data;
  const hashBuffer = await crypto.subtle.digest('SHA-256', dataBytes);
  return toHex(new Uint8Array(hashBuffer));
}

/**
 * Compute HMAC-SHA256 and return raw bytes
 */
export async function hmacSha256(key: Uint8Array, data: string): Promise<Uint8Array> {
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
export async function getSigningKey(
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

/**
 * Format a Date to AWS date format: YYYYMMDD'T'HHMMSS'Z'
 * AWS requires ISO 8601 basic format without separators
 */
export function formatAmzDate(date: Date): string {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  const hours = String(date.getUTCHours()).padStart(2, '0');
  const minutes = String(date.getUTCMinutes()).padStart(2, '0');
  const seconds = String(date.getUTCSeconds()).padStart(2, '0');
  return `${year}${month}${day}T${hours}${minutes}${seconds}Z`;
}

/**
 * Format a Date to AWS date stamp format: YYYYMMDD
 */
export function formatDateStamp(date: Date): string {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  return `${year}${month}${day}`;
}

/**
 * AWS SigV4 URI encoding - encodes all characters except unreserved characters
 * Unreserved characters per RFC 3986: A-Z a-z 0-9 - _ . ~
 * For path segments, forward slash '/' should NOT be encoded.
 */
export function awsUriEncode(str: string, encodeSlash: boolean = true): string {
  let encoded = '';
  for (const char of str) {
    if (
      (char >= 'A' && char <= 'Z') ||
      (char >= 'a' && char <= 'z') ||
      (char >= '0' && char <= '9') ||
      char === '-' ||
      char === '_' ||
      char === '.' ||
      char === '~'
    ) {
      encoded += char;
    } else if (char === '/' && !encodeSlash) {
      encoded += char;
    } else {
      // Encode the character as %XX for each byte in UTF-8
      const bytes = new TextEncoder().encode(char);
      for (const byte of bytes) {
        encoded += '%' + byte.toString(16).toUpperCase().padStart(2, '0');
      }
    }
  }
  return encoded;
}

/**
 * Trim and normalize header value per AWS SigV4 spec:
 * - Trim leading and trailing whitespace
 * - Convert sequential spaces to a single space
 */
export function normalizeHeaderValue(value: string): string {
  return value.trim().replace(/\s+/g, ' ');
}

// ============================================================================
// S3 Error Handling
// ============================================================================

/**
 * S3 operation error with sanitized message (no credentials leaked)
 */
export class S3Error extends Error {
  readonly statusCode?: number;
  readonly operation: string;
  readonly bucket: string;
  readonly key?: string;

  constructor(operation: string, bucket: string, key: string | undefined, statusCode: number | undefined, originalError?: unknown) {
    const baseMessage = `S3 ${operation} failed`;
    const details: string[] = [];
    if (statusCode) details.push(`status=${statusCode}`);
    if (key) details.push(`key=${key}`);
    details.push(`bucket=${bucket}`);

    const message = details.length > 0 ? `${baseMessage}: ${details.join(', ')}` : baseMessage;
    super(originalError ? createSafeErrorMessage(message, originalError) : message);

    this.name = 'S3Error';
    this.statusCode = statusCode;
    this.operation = operation;
    this.bucket = bucket;
    this.key = key;
  }
}

/**
 * Create a safe S3 error that never includes credentials
 */
function createS3Error(
  operation: string,
  bucket: string,
  key: string | undefined,
  statusCode?: number,
  originalError?: unknown
): S3Error {
  return new S3Error(operation, bucket, key, statusCode, originalError);
}

// ============================================================================
// S3 Storage Class
// ============================================================================

export class S3Storage implements StorageBackend {
  private config: S3Config;

  constructor(config: S3Config) {
    this.config = {
      ...config,
      region: config.region || 'auto',
    };
  }

  /**
   * Get a safe representation of the config for logging (credentials redacted)
   */
  getSafeConfig(): Record<string, unknown> {
    // S3Config satisfies Record<string, unknown> for sanitization purposes
    const configRecord: Record<string, unknown> = {
      endpoint: this.config.endpoint,
      bucket: this.config.bucket,
      region: this.config.region,
      accessKeyId: this.config.accessKeyId,
      secretAccessKey: this.config.secretAccessKey,
    };
    return sanitizeConfig(configRecord);
  }

  /**
   * Sign an S3 request using AWS Signature Version 4.
   * This implementation properly hashes the payload for request integrity.
   *
   * @param method - HTTP method (GET, PUT, DELETE, HEAD)
   * @param key - Object key (path within the bucket)
   * @param headers - Additional headers to include in the request
   * @param body - Request body for PUT operations
   * @param queryParams - Query string parameters (for list operations)
   */
  private async signRequest(
    method: string,
    key: string,
    headers: Record<string, string> = {},
    body?: Uint8Array,
    queryParams?: Record<string, string>
  ): Promise<{ url: string; headers: Record<string, string> }> {
    const service = 's3';
    const region = this.config.region || 'auto';

    // Parse endpoint to get host
    const endpointUrl = new URL(this.config.endpoint);
    const host = endpointUrl.host;

    // Build the URL path using proper AWS URI encoding
    // For path segments, encode each segment but preserve '/'
    const encodedKey = key
      .split('/')
      .map((segment) => awsUriEncode(segment, true))
      .join('/');

    // Build canonical URI - always starts with /
    const canonicalUri = `/${this.config.bucket}${encodedKey ? '/' + encodedKey : ''}`;

    // Build the final URL
    const baseUrl = `${this.config.endpoint}${canonicalUri}`;

    // Build canonical query string (parameters must be sorted)
    let canonicalQueryString = '';
    let finalUrl = baseUrl;
    if (queryParams && Object.keys(queryParams).length > 0) {
      const sortedParams = Object.keys(queryParams).sort();
      canonicalQueryString = sortedParams
        .map((k) => `${awsUriEncode(k)}=${awsUriEncode(queryParams[k]!)}`)
        .join('&');
      finalUrl = `${baseUrl}?${canonicalQueryString}`;
    }

    // Get timestamps using proper formatting functions
    const now = new Date();
    const amzDate = formatAmzDate(now);
    const dateStamp = formatDateStamp(now);

    // Calculate payload hash - this is the security fix
    // Previously used UNSIGNED-PAYLOAD which disables integrity checking
    const payloadHash = body ? await sha256Hex(body) : EMPTY_PAYLOAD_HASH;

    // Build canonical headers (must be sorted and lowercase)
    // Normalize header values per AWS SigV4 spec
    const canonicalHeaders: Record<string, string> = {
      host: normalizeHeaderValue(host),
      'x-amz-content-sha256': payloadHash,
      'x-amz-date': amzDate,
    };

    // Add additional headers, normalizing keys to lowercase and values
    for (const [k, v] of Object.entries(headers)) {
      canonicalHeaders[k.toLowerCase()] = normalizeHeaderValue(v);
    }

    // Sort headers alphabetically
    const sortedHeaderKeys = Object.keys(canonicalHeaders).sort();
    const canonicalHeadersString = sortedHeaderKeys
      .map((k) => `${k}:${canonicalHeaders[k]}`)
      .join('\n');
    const signedHeaders = sortedHeaderKeys.join(';');

    // Build canonical request
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

    return { url: finalUrl, headers: authHeaders };
  }

  async get(key: string): Promise<Uint8Array | null> {
    validateStorageKey(key);
    try {
      const { url, headers } = await this.signRequest('GET', key);
      const response = await fetch(url, { headers });
      if (response.status === 404) return null;
      if (!response.ok) {
        throw createS3Error('GET', this.config.bucket, key, response.status);
      }
      const buffer = await response.arrayBuffer();
      return new Uint8Array(buffer);
    } catch (error) {
      if (error instanceof S3Error) throw error;
      throw createS3Error('GET', this.config.bucket, key, undefined, error);
    }
  }

  async put(key: string, data: Uint8Array): Promise<void> {
    validateStorageKey(key);
    try {
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
      if (!response.ok) {
        throw createS3Error('PUT', this.config.bucket, key, response.status);
      }
    } catch (error) {
      if (error instanceof S3Error) throw error;
      throw createS3Error('PUT', this.config.bucket, key, undefined, error);
    }
  }

  async delete(key: string): Promise<void> {
    validateStorageKey(key);
    try {
      const { url, headers } = await this.signRequest('DELETE', key);
      const response = await fetch(url, { method: 'DELETE', headers });
      if (!response.ok && response.status !== 404) {
        throw createS3Error('DELETE', this.config.bucket, key, response.status);
      }
    } catch (error) {
      if (error instanceof S3Error) throw error;
      throw createS3Error('DELETE', this.config.bucket, key, undefined, error);
    }
  }

  async list(prefix: string): Promise<string[]> {
    validateStoragePrefix(prefix);
    try {
      // Sign the request with query parameters included in the signature
      const { url, headers } = await this.signRequest(
        'GET',
        '',
        {},
        undefined,
        {
          'list-type': '2',
          prefix: prefix,
        }
      );
      const response = await fetch(url, { headers });
      if (!response.ok) {
        throw createS3Error('LIST', this.config.bucket, prefix, response.status);
      }

      const text = await response.text();
      // Parse XML response (simplified)
      const keys: string[] = [];
      const keyRegex = /<Key>([^<]+)<\/Key>/g;
      let match;
      while ((match = keyRegex.exec(text)) !== null) {
        keys.push(match[1]!);
      }
      return keys;
    } catch (error) {
      if (error instanceof S3Error) throw error;
      throw createS3Error('LIST', this.config.bucket, prefix, undefined, error);
    }
  }

  async exists(key: string): Promise<boolean> {
    validateStorageKey(key);
    try {
      const { url, headers } = await this.signRequest('HEAD', key);
      const response = await fetch(url, { method: 'HEAD', headers });
      return response.ok;
    } catch (error) {
      if (error instanceof S3Error) throw error;
      throw createS3Error('HEAD', this.config.bucket, key, undefined, error);
    }
  }

  async head(key: string): Promise<{ size: number } | null> {
    validateStorageKey(key);
    try {
      const { url, headers } = await this.signRequest('HEAD', key);
      const response = await fetch(url, { method: 'HEAD', headers });
      if (!response.ok) return null;
      const size = parseInt(response.headers.get('content-length') || '0', 10);
      return { size };
    } catch (error) {
      if (error instanceof S3Error) throw error;
      throw createS3Error('HEAD', this.config.bucket, key, undefined, error);
    }
  }

  async createMultipartUpload(key: string): Promise<MultipartUpload> {
    validateStorageKey(key);
    // Simplified - in production, implement proper S3 multipart
    return createBufferedMultipartUpload((data) => this.put(key, data));
  }

  async getStream(key: string): Promise<ReadableStream<Uint8Array> | null> {
    validateStorageKey(key);
    try {
      const { url, headers } = await this.signRequest('GET', key);
      const response = await fetch(url, { headers });
      if (response.status === 404) return null;
      if (!response.ok) {
        throw createS3Error('GET_STREAM', this.config.bucket, key, response.status);
      }
      return response.body as ReadableStream<Uint8Array>;
    } catch (error) {
      if (error instanceof S3Error) throw error;
      throw createS3Error('GET_STREAM', this.config.bucket, key, undefined, error);
    }
  }

  async putStream(key: string, stream: ReadableStream<Uint8Array>): Promise<void> {
    validateStorageKey(key);
    try {
      // For S3 streaming uploads, we use chunked transfer encoding
      // First collect the stream to compute content length and hash for signing
      const reader = stream.getReader();
      const chunks: Uint8Array[] = [];
      let totalSize = 0;

      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          chunks.push(value);
          totalSize += value.length;
        }
      } finally {
        reader.releaseLock();
      }

      // Concatenate all chunks
      const data = new Uint8Array(totalSize);
      let offset = 0;
      for (const chunk of chunks) {
        data.set(chunk, offset);
        offset += chunk.length;
      }

      // Use the standard put method with the collected data
      await this.put(key, data);
    } catch (error) {
      if (error instanceof S3Error) throw error;
      throw createS3Error('PUT_STREAM', this.config.bucket, key, undefined, error);
    }
  }
}
