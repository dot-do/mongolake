/**
 * HTTP Range Request Handler
 *
 * Parses HTTP Range headers and enables efficient partial file reads.
 * Critical for Parquet file access where we need to read footers without
 * downloading entire files.
 *
 * Supports:
 * - Byte ranges: bytes=0-1023 (first 1024 bytes)
 * - Suffix ranges: bytes=-1024 (last 1024 bytes)
 * - Multiple ranges: bytes=0-100,200-300 (not commonly used)
 *
 * RFC 7233: https://tools.ietf.org/html/rfc7233
 */

import type { StorageBackend } from './index.js';

// ============================================================================
// Types
// ============================================================================

/** Parsed byte range */
export interface ByteRange {
  /** Start byte (inclusive) */
  start: number;
  /** End byte (inclusive) */
  end: number;
}

/** Result of parsing a Range header */
export interface ParsedRange {
  /** Unit (always 'bytes') */
  unit: string;
  /** Parsed ranges */
  ranges: ByteRange[];
}

/** Range request result */
export interface RangeResponse {
  /** HTTP status code (200 or 206) */
  status: 200 | 206 | 416;
  /** Response headers */
  headers: Record<string, string>;
  /** Response body */
  body: Uint8Array | null;
  /** Content range info (for 206 responses) */
  contentRange?: {
    start: number;
    end: number;
    total: number;
  };
}

// ============================================================================
// Range Parser
// ============================================================================

/**
 * Parse an HTTP Range header
 *
 * Formats supported:
 * - bytes=start-end (e.g., "bytes=0-1023")
 * - bytes=-suffix (e.g., "bytes=-1024" for last 1024 bytes)
 * - bytes=start- (e.g., "bytes=500-" from byte 500 to end)
 * - Multiple ranges (e.g., "bytes=0-100,200-300")
 *
 * @param rangeHeader - The Range header value
 * @param fileSize - Total file size for validation
 * @returns Parsed range or null if invalid
 */
export function parseRangeHeader(
  rangeHeader: string,
  fileSize: number
): ParsedRange | null {
  const match = rangeHeader.match(/^(\w+)=(.+)$/);
  if (!match) {
    return null;
  }

  const [, unit, rangesStr] = match;

  // RFC 7233 only standardizes the "bytes" unit
  if (unit !== 'bytes') {
    return null;
  }

  // rangesStr is guaranteed by the regex match pattern
  if (!rangesStr) {
    return null;
  }

  // Parse comma-separated range parts
  // Syntactically valid ranges are returned even if unsatisfiable;
  // satisfiability is checked separately via isRangeSatisfiable()
  const rangeParts = rangesStr.split(',').map((s) => s.trim());
  const ranges: ByteRange[] = [];

  for (const part of rangeParts) {
    const range = parseRangePart(part, fileSize);
    if (range === null) {
      return null;
    }
    ranges.push(range);
  }

  if (ranges.length === 0) {
    return null;
  }

  return { unit, ranges };
}

/**
 * Parse a single range part (e.g., "0-1023", "-1024", "100-")
 */
function parseRangePart(part: string, fileSize: number): ByteRange | null {
  // Suffix range: bytes=-N (last N bytes)
  if (part.startsWith('-')) {
    const suffixStr = part.slice(1);
    // Validate that suffix contains only digits (e.g., "-10" valid, "-10-100" invalid)
    if (!/^\d+$/.test(suffixStr)) {
      return null;
    }
    const suffix = parseInt(suffixStr, 10);
    if (isNaN(suffix) || suffix <= 0) {
      return null;
    }
    // For a 1000-byte file with bytes=-500, return bytes 500-999
    const start = Math.max(0, fileSize - suffix);
    return { start, end: fileSize - 1 };
  }

  // Standard or open-ended range: bytes=N-M or bytes=N-
  const dashIndex = part.indexOf('-');
  if (dashIndex === -1) {
    return null;
  }

  const startStr = part.slice(0, dashIndex);
  const endStr = part.slice(dashIndex + 1);

  const start = parseInt(startStr, 10);
  if (isNaN(start) || start < 0) {
    return null;
  }

  // Open-ended range: bytes=N- (from N to file end)
  if (endStr === '') {
    // Return even if start is beyond file size; isRangeSatisfiable() will handle it
    return { start, end: fileSize - 1 };
  }

  // Standard range: bytes=N-M
  const end = parseInt(endStr, 10);
  if (isNaN(end) || end < start) {
    return null;
  }

  // Return even if unsatisfiable; isRangeSatisfiable() will reject it
  // Cap end at file size - 1
  const cappedEnd = Math.min(end, fileSize - 1);

  return { start, end: cappedEnd };
}

/**
 * Check if ranges are satisfiable
 */
export function isRangeSatisfiable(
  ranges: ByteRange[],
  fileSize: number
): boolean {
  if (fileSize === 0) {
    return false;
  }

  for (const range of ranges) {
    if (range.start < fileSize) {
      return true;
    }
  }

  return false;
}

/**
 * Check if a range header is syntactically valid (ignoring file size)
 * This helps differentiate between malformed syntax and unsatisfiable ranges.
 */
function isSyntacticallyValidRange(rangeHeader: string): boolean {
  const match = rangeHeader.match(/^(\w+)=(.+)$/);
  if (!match) {
    return false;
  }

  const [, unit, rangesStr] = match;

  // Only "bytes" unit is standardized per RFC 7233
  if (unit !== 'bytes') {
    return false;
  }

  // rangesStr is guaranteed by the regex match pattern
  if (!rangesStr) {
    return false;
  }

  const rangeParts = rangesStr.split(',').map((s) => s.trim());
  if (rangeParts.length === 0) {
    return false;
  }

  for (const part of rangeParts) {
    if (!isSyntacticallyValidRangePart(part)) {
      return false;
    }
  }

  return true;
}

/**
 * Check if a single range part has valid syntax (ignoring file size)
 */
function isSyntacticallyValidRangePart(part: string): boolean {
  // Suffix range: bytes=-N
  if (part.startsWith('-')) {
    const suffixStr = part.slice(1);
    // Must be only digits
    if (!/^\d+$/.test(suffixStr)) {
      return false;
    }
    const suffix = parseInt(suffixStr, 10);
    return !isNaN(suffix) && suffix > 0;
  }

  // Standard or open-ended range: bytes=N-M or bytes=N-
  const dashIndex = part.indexOf('-');
  if (dashIndex === -1) {
    return false;
  }

  const startStr = part.slice(0, dashIndex);
  const endStr = part.slice(dashIndex + 1);

  const start = parseInt(startStr, 10);
  if (isNaN(start) || start < 0) {
    return false;
  }

  // Open-ended range: bytes=N- is valid if N is valid
  if (endStr === '') {
    return true;
  }

  // Standard range: bytes=N-M
  const end = parseInt(endStr, 10);
  return !isNaN(end) && end >= start;
}

// ============================================================================
// Range Handler Class
// ============================================================================

/**
 * HTTP Range Request Handler
 *
 * Handles Range requests for efficient partial file reads.
 * Integrates with storage backends for byte-level access.
 */
export class RangeHandler {
  /**
   * Handle a range request
   *
   * @param rangeHeader - The Range header value (or null for full file)
   * @param data - The full file data
   * @returns Response with appropriate status, headers, and body
   */
  handleRange(rangeHeader: string | null, data: Uint8Array): RangeResponse {
    const fileSize = data.byteLength;

    // No Range header - return full file (200 OK)
    if (!rangeHeader) {
      return {
        status: 200,
        headers: {
          'Content-Type': 'application/octet-stream',
          'Content-Length': String(fileSize),
          'Accept-Ranges': 'bytes',
        },
        body: data,
      };
    }

    // Parse the Range header
    const parsed = parseRangeHeader(rangeHeader, fileSize);

    if (!parsed) {
      // Check if this is a syntax error (malformed) or unsatisfiable range
      // Per RFC 7233, malformed headers are ignored (treat as no Range header)
      if (isSyntacticallyValidRange(rangeHeader)) {
        // Valid syntax but unsatisfiable (e.g., start > fileSize)
        return {
          status: 416,
          headers: {
            'Content-Range': `bytes */${fileSize}`,
          },
          body: null,
        };
      }
      // Malformed Range header - return full file per RFC 7233 section 2.3
      return {
        status: 200,
        headers: {
          'Content-Type': 'application/octet-stream',
          'Content-Length': String(fileSize),
          'Accept-Ranges': 'bytes',
        },
        body: data,
      };
    }

    // Check if any requested range is satisfiable
    if (!isRangeSatisfiable(parsed.ranges, fileSize)) {
      // Return 416 Range Not Satisfiable with file size info
      return {
        status: 416,
        headers: {
          'Content-Range': `bytes */${fileSize}`,
        },
        body: null,
      };
    }

    // Use first range only (multipart responses rarely needed in practice)
    const range = parsed.ranges[0]!;
    const contentLength = range.end - range.start + 1;
    const body = data.slice(range.start, range.end + 1);

    return {
      status: 206,
      headers: {
        'Content-Type': 'application/octet-stream',
        'Content-Length': String(contentLength),
        'Content-Range': `bytes ${range.start}-${range.end}/${fileSize}`,
        'Accept-Ranges': 'bytes',
      },
      body,
      contentRange: {
        start: range.start,
        end: range.end,
        total: fileSize,
      },
    };
  }

  /**
   * Create a Response object from range handling result
   */
  toResponse(result: RangeResponse): Response {
    return new Response(result.body, {
      status: result.status,
      headers: result.headers,
    });
  }
}

// ============================================================================
// Storage-Integrated Range Handler
// ============================================================================

/**
 * Range handler that integrates with storage backends
 *
 * Enables efficient partial reads without loading full files into memory.
 * Essential for reading Parquet footers.
 */
export class StorageRangeHandler {
  constructor(private storage: StorageBackend) {}

  /**
   * Handle a range request for a storage object
   *
   * @param key - Storage object key
   * @param rangeHeader - Range header value (or null for full file)
   * @returns Range response
   */
  async handleRange(
    key: string,
    rangeHeader: string | null
  ): Promise<RangeResponse> {
    // Get object metadata to determine file size
    const metadata = await this.storage.head(key);
    if (!metadata) {
      return {
        status: 416,
        headers: {},
        body: null,
      };
    }

    const fileSize = metadata.size;

    // No Range header - return full file (200 OK)
    if (!rangeHeader) {
      const data = await this.storage.get(key);
      if (!data) {
        return {
          status: 416,
          headers: {},
          body: null,
        };
      }

      return {
        status: 200,
        headers: {
          'Content-Type': 'application/octet-stream',
          'Content-Length': String(fileSize),
          'Accept-Ranges': 'bytes',
        },
        body: data,
      };
    }

    // Parse and validate the range header
    const parsed = parseRangeHeader(rangeHeader, fileSize);

    if (!parsed || !isRangeSatisfiable(parsed.ranges, fileSize)) {
      // Return 416 Range Not Satisfiable
      return {
        status: 416,
        headers: {
          'Content-Range': `bytes */${fileSize}`,
        },
        body: null,
      };
    }

    // Fetch the object data and extract the requested range
    const data = await this.storage.get(key);
    if (!data) {
      return {
        status: 416,
        headers: {},
        body: null,
      };
    }

    // Use first range only (multiple ranges not commonly used)
    const range = parsed.ranges[0]!;
    const body = data.slice(range.start, range.end + 1);
    const contentLength = range.end - range.start + 1;

    return {
      status: 206,
      headers: {
        'Content-Type': 'application/octet-stream',
        'Content-Length': String(contentLength),
        'Content-Range': `bytes ${range.start}-${range.end}/${fileSize}`,
        'Accept-Ranges': 'bytes',
      },
      body,
      contentRange: {
        start: range.start,
        end: range.end,
        total: fileSize,
      },
    };
  }
}

// ============================================================================
// Parquet Footer Reader
// ============================================================================

/**
 * Read Parquet file footer using range requests
 *
 * Parquet files store metadata in the footer (last N bytes).
 * This enables reading metadata without downloading the full file.
 *
 * Footer structure:
 * [row groups data...] [FileMetaData] [footer length (4 bytes LE)] [magic "PAR1"]
 *
 * Reading strategy:
 * 1. Read last 8 bytes to get magic and footer length
 * 2. Read footer length bytes to get FileMetaData
 */
export class ParquetFooterReader {
  private handler: RangeHandler;
  // Parquet magic bytes: "PAR1"
  private static readonly PARQUET_MAGIC = new Uint8Array([0x50, 0x41, 0x52, 0x31]);

  constructor() {
    this.handler = new RangeHandler();
  }

  /**
   * Verify Parquet magic bytes in trailer
   */
  private verifyMagic(magic: Uint8Array): boolean {
    if (magic.byteLength !== 4) {
      return false;
    }
    for (let i = 0; i < 4; i++) {
      if (magic[i] !== ParquetFooterReader.PARQUET_MAGIC[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Read footer metadata from a Parquet file
   *
   * @param data - Full file data (for testing/local)
   * @returns Footer bytes (Thrift-encoded FileMetaData)
   */
  readFooter(data: Uint8Array): Uint8Array | null {
    const fileSize = data.byteLength;

    // Minimum Parquet file: 4 (magic) + 4 (footer length) + 4 (magic) = 12 bytes
    if (fileSize < 12) {
      return null;
    }

    // Read last 8 bytes: [footer_length (4)] [magic (4)]
    const trailer = data.slice(fileSize - 8);

    // Verify magic bytes
    const magic = trailer.slice(4);
    if (!this.verifyMagic(magic)) {
      return null;
    }

    // Read footer length (4-byte little-endian)
    const footerLengthView = new DataView(
      trailer.buffer,
      trailer.byteOffset,
      4
    );
    const footerLength = footerLengthView.getUint32(0, true);

    // Validate footer length
    if (footerLength > fileSize - 8) {
      return null;
    }

    // Calculate footer start position
    const footerStart = fileSize - 8 - footerLength;

    // Extract footer metadata
    return data.slice(footerStart, fileSize - 8);
  }

  /**
   * Read footer using range request
   *
   * Efficient two-step process:
   * 1. Read last 8 bytes to get footer length
   * 2. Read footer bytes
   *
   * @param data - Full file data
   * @returns Footer bytes
   */
  readFooterWithRanges(data: Uint8Array): Uint8Array | null {
    const fileSize = data.byteLength;

    if (fileSize < 12) {
      return null;
    }

    // Step 1: Read trailer (last 8 bytes) to extract footer length
    const trailerResponse = this.handler.handleRange('bytes=-8', data);

    if (trailerResponse.status !== 206 || !trailerResponse.body) {
      return null;
    }

    const trailer = trailerResponse.body;

    // Verify magic bytes
    const magic = trailer.slice(4);
    if (!this.verifyMagic(magic)) {
      return null;
    }

    // Read footer length (4-byte little-endian)
    const footerLengthView = new DataView(
      trailer.buffer,
      trailer.byteOffset,
      4
    );
    const footerLength = footerLengthView.getUint32(0, true);

    if (footerLength > fileSize - 8) {
      return null;
    }

    // Step 2: Read footer metadata (footer + length + magic)
    const fullFooterResponse = this.handler.handleRange(
      `bytes=-${footerLength + 8}`,
      data
    );

    if (fullFooterResponse.status !== 206 || !fullFooterResponse.body) {
      return null;
    }

    // Return just the footer metadata (slice off the trailer)
    return fullFooterResponse.body.slice(0, footerLength);
  }

  /**
   * Calculate range header for reading Parquet footer
   *
   * @param footerLength - Known footer length (or estimate)
   * @returns Range header value
   */
  static getFooterRangeHeader(footerLength: number): string {
    // Add 8 bytes for footer length field and magic
    return `bytes=-${footerLength + 8}`;
  }
}

