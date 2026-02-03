/**
 * Parquet Compression Module
 *
 * Provides a unified compression interface with Snappy and ZSTD codec implementations.
 * Centralizes compression logic previously duplicated across row-group.ts,
 * row-group-reader.ts, and column-writer.ts.
 *
 * Compression Format:
 * Both codecs use a common LZ77-style format:
 * - Header: [originalLength: 4 bytes LE]
 * - Literals: [length (1-2 bytes)] [literal bytes]
 * - Matches: [0xFF] [offset: 2 bytes LE] [length: 1 byte]
 */

import {
  SNAPPY_WINDOW_SIZE,
  SNAPPY_MIN_MATCH_LENGTH,
  MAX_MATCH_LENGTH,
  LITERAL_FLUSH_THRESHOLD,
  ZSTD_WINDOW_SIZE,
  ZSTD_MIN_MATCH_LENGTH,
  HASH_POSITION_LIMIT,
} from '../constants.js';

// ============================================================================
// Types
// ============================================================================

/** Supported compression codecs */
export type CompressionCodec = 'none' | 'snappy' | 'zstd';

/** Compression codec interface */
export interface CompressionCodecInterface {
  /** Compress data */
  compress(data: Uint8Array): Uint8Array;
  /** Decompress data given expected uncompressed size */
  decompress(data: Uint8Array, expectedSize: number): Uint8Array;
}

// ============================================================================
// Shared Utilities
// ============================================================================

/**
 * Encode original data length as 4-byte little-endian integer.
 * Used as a header for compressed data to enable decompression.
 */
function encodeOriginalLength(length: number): number[] {
  return [
    length & 0xff,
    (length >> 8) & 0xff,
    (length >> 16) & 0xff,
    (length >> 24) & 0xff,
  ];
}

/**
 * Read 4-byte little-endian length from data header.
 */
function readOriginalLength(data: Uint8Array): number {
  const view = new DataView(data.buffer, data.byteOffset, 4);
  return view.getUint32(0, true);
}

/**
 * Flush accumulated literals to output buffer.
 * Encodes length with continuation bit if length exceeds 127.
 *
 * Format:
 * - Length <= 127: [length: 1 byte] [literals]
 * - Length > 127: [0x80 | (length & 0x7F)] [length >> 7] [literals]
 */
function flushLiteralsToOutput(output: number[], literals: number[]): void {
  if (literals.length === 0) return;

  if (literals.length <= 127) {
    output.push(literals.length);
    output.push(...literals);
  } else {
    // Continuation bit indicates next byte contains additional length
    output.push(0x80 | (literals.length & 0x7f));
    output.push(literals.length >> 7);
    output.push(...literals);
  }
  literals.length = 0;
}

/**
 * Common decompression logic for LZ77-style compressed data.
 * Processes literal blocks and match references from compressed stream.
 *
 * @param data - Compressed data with 4-byte length header
 * @param expectedSize - Expected uncompressed size for validation
 * @returns Decompressed data
 */
function decompressLZ77(data: Uint8Array, expectedSize: number): Uint8Array {
  if (data.length === 0) return new Uint8Array(0);

  // If data is smaller than header or matches expected size, it wasn't compressed
  if (data.length < 4) return data;

  // Read original length from first 4 bytes
  const originalLength = readOriginalLength(data);

  // If the data matches the expected uncompressed size, it wasn't actually compressed
  if (data.length === expectedSize || originalLength !== expectedSize) {
    return data;
  }

  const output = new Uint8Array(originalLength);
  let outputPos = 0;
  let inputPos = 4; // Skip the 4-byte length header

  while (inputPos < data.length && outputPos < originalLength) {
    const byte = data[inputPos];

    if (byte === 0xff) {
      // Match: [0xFF] [offset: 2 bytes] [length: 1 byte]
      if (inputPos + 4 > data.length) break;

      const offset = data[inputPos + 1]! | (data[inputPos + 2]! << 8);
      const length = data[inputPos + 3]!;
      inputPos += 4;

      // Copy from earlier in output
      const sourcePos = outputPos - offset;
      for (let i = 0; i < length && outputPos < originalLength; i++) {
        output[outputPos++] = output[sourcePos + i]!;
      }
    } else {
      // Literal block
      let literalLength: number;

      if (byte! & 0x80) {
        // Two-byte length: lower 7 bits + next byte * 128
        if (inputPos + 1 >= data.length) break;
        literalLength = (byte! & 0x7f) | (data[inputPos + 1]! << 7);
        inputPos += 2;
      } else {
        // Single-byte length
        literalLength = byte!;
        inputPos += 1;
      }

      // Copy literal bytes
      for (let i = 0; i < literalLength && inputPos < data.length && outputPos < originalLength; i++) {
        output[outputPos++] = data[inputPos++]!;
      }
    }
  }

  return output;
}

// ============================================================================
// Snappy Codec
// ============================================================================

/**
 * Snappy compression using LZ77-style sliding window with back-references.
 * Uses linear scan for match finding (simpler but slower for large windows).
 */
function compressSnappy(data: Uint8Array): Uint8Array {
  if (data.length === 0) return new Uint8Array(0);
  if (data.length < 10) return data;

  const output: number[] = [];
  output.push(...encodeOriginalLength(data.length));

  let pos = 0;
  const literals: number[] = [];

  while (pos < data.length) {
    // Find the best match in the sliding window
    let bestMatchLength = 0;
    let bestMatchOffset = 0;

    const windowStart = Math.max(0, pos - SNAPPY_WINDOW_SIZE);
    for (let offset = windowStart; offset < pos; offset++) {
      let matchLength = 0;
      while (
        pos + matchLength < data.length &&
        matchLength < MAX_MATCH_LENGTH &&
        data[offset + matchLength] === data[pos + matchLength]
      ) {
        matchLength++;
      }
      if (matchLength > bestMatchLength) {
        bestMatchLength = matchLength;
        bestMatchOffset = pos - offset;
      }
    }

    if (bestMatchLength >= SNAPPY_MIN_MATCH_LENGTH) {
      // Flush pending literals before encoding a match
      flushLiteralsToOutput(output, literals);

      // Encode match: marker (0xFF), offset (2 bytes), length (1 byte)
      output.push(0xff);
      output.push(bestMatchOffset & 0xff);
      output.push((bestMatchOffset >> 8) & 0xff);
      output.push(bestMatchLength);
      pos += bestMatchLength;
    } else {
      literals.push(data[pos]!);
      pos++;

      // Periodically flush large literal blocks to prevent unbounded growth
      if (literals.length >= LITERAL_FLUSH_THRESHOLD) {
        flushLiteralsToOutput(output, literals);
      }
    }
  }

  // Flush any remaining literals
  flushLiteralsToOutput(output, literals);

  const result = new Uint8Array(output);

  // Return uncompressed if compression wasn't beneficial
  if (result.length < data.length) {
    return result;
  }
  return data;
}

/**
 * Snappy codec implementation
 */
export const snappyCodec: CompressionCodecInterface = {
  compress: compressSnappy,
  decompress: decompressLZ77,
};

// ============================================================================
// ZSTD Codec
// ============================================================================

/**
 * ZSTD-like compression with larger window and hash table for faster matching.
 * Uses hash table to quickly find candidate matches (faster for larger windows).
 */
function compressZstd(data: Uint8Array): Uint8Array {
  if (data.length === 0) return new Uint8Array(0);
  if (data.length < 10) return data;

  const output: number[] = [];
  output.push(...encodeOriginalLength(data.length));

  // Hash table maps 3-byte patterns to their positions for faster matching
  const hashTable = new Map<number, number[]>();

  const computeHash = (pos: number): number => {
    if (pos + 3 > data.length) return 0;
    return (data[pos]! << 16) | (data[pos + 1]! << 8) | data[pos + 2]!;
  };

  let pos = 0;
  const literals: number[] = [];

  while (pos < data.length) {
    const currentHash = computeHash(pos);
    const candidates = hashTable.get(currentHash) || [];

    // Find the best match among candidates
    let bestMatchLength = 0;
    let bestMatchOffset = 0;

    for (const candidatePos of candidates) {
      // Skip candidates outside the window
      if (pos - candidatePos > ZSTD_WINDOW_SIZE) continue;

      let matchLength = 0;
      while (
        pos + matchLength < data.length &&
        matchLength < MAX_MATCH_LENGTH &&
        data[candidatePos + matchLength] === data[pos + matchLength]
      ) {
        matchLength++;
      }
      if (matchLength > bestMatchLength) {
        bestMatchLength = matchLength;
        bestMatchOffset = pos - candidatePos;
      }
    }

    // Update hash table with current position
    if (!hashTable.has(currentHash)) {
      hashTable.set(currentHash, []);
    }
    const positionList = hashTable.get(currentHash)!;
    positionList.push(pos);

    // Keep only recent positions to limit memory usage
    if (positionList.length > HASH_POSITION_LIMIT) {
      positionList.shift();
    }

    if (bestMatchLength >= ZSTD_MIN_MATCH_LENGTH) {
      flushLiteralsToOutput(output, literals);
      output.push(0xff);
      output.push(bestMatchOffset & 0xff);
      output.push((bestMatchOffset >> 8) & 0xff);
      output.push(bestMatchLength);
      pos += bestMatchLength;
    } else {
      literals.push(data[pos]!);
      pos++;

      // Periodically flush large literal blocks to prevent unbounded growth
      if (literals.length >= LITERAL_FLUSH_THRESHOLD) {
        flushLiteralsToOutput(output, literals);
      }
    }
  }

  flushLiteralsToOutput(output, literals);

  const result = new Uint8Array(output);
  if (result.length < data.length) {
    return result;
  }
  return data;
}

/**
 * ZSTD codec implementation
 */
export const zstdCodec: CompressionCodecInterface = {
  compress: compressZstd,
  decompress: decompressLZ77,
};

// ============================================================================
// No-op Codec
// ============================================================================

/**
 * No-op codec for uncompressed data
 */
export const noneCodec: CompressionCodecInterface = {
  compress: (data: Uint8Array) => data,
  decompress: (data: Uint8Array) => data,
};

// ============================================================================
// Codec Registry
// ============================================================================

/** Map of codec names to implementations */
const codecs: Record<CompressionCodec, CompressionCodecInterface> = {
  none: noneCodec,
  snappy: snappyCodec,
  zstd: zstdCodec,
};

/**
 * Get a codec implementation by name.
 *
 * @param codec - Codec name
 * @returns Codec implementation
 * @throws Error if codec is not supported
 */
export function getCodec(codec: CompressionCodec): CompressionCodecInterface {
  const impl = codecs[codec];
  if (!impl) {
    throw new Error(`Unsupported compression codec: ${codec}`);
  }
  return impl;
}

// ============================================================================
// Convenience Functions
// ============================================================================

/**
 * Compress data using the specified codec.
 *
 * @param data - Data to compress
 * @param codec - Compression codec to use
 * @returns Compressed data
 */
export function compress(data: Uint8Array, codec: CompressionCodec): Uint8Array {
  return getCodec(codec).compress(data);
}

/**
 * Decompress data using the specified codec.
 *
 * @param data - Compressed data
 * @param codec - Compression codec used
 * @param expectedSize - Expected uncompressed size
 * @returns Decompressed data
 */
export function decompress(
  data: Uint8Array,
  codec: CompressionCodec,
  expectedSize: number
): Uint8Array {
  return getCodec(codec).decompress(data, expectedSize);
}

/**
 * List of supported compression codecs.
 */
export const supportedCodecs: CompressionCodec[] = ['none', 'snappy', 'zstd'];

/**
 * Check if a codec is supported.
 *
 * @param codec - Codec name to check
 * @returns true if supported
 */
export function isCodecSupported(codec: string): codec is CompressionCodec {
  return supportedCodecs.includes(codec as CompressionCodec);
}
