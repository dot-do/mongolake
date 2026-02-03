/**
 * Parquet I/O Module
 *
 * Provides proper binary Parquet reading and writing using hyparquet libraries.
 * Replaces the JSON fallback with actual Parquet format.
 *
 * Note: hyparquet reads BYTE_ARRAY columns as UTF-8 strings, which corrupts
 * binary data containing invalid UTF-8 sequences. To work around this, we
 * base64 encode variant data when writing and decode when reading.
 *
 * Compression: We default to UNCOMPRESSED because the binary Variant encoding
 * already provides significant space savings. Our benchmarks in parquedb showed
 * that additional compression provides minimal benefit while adding CPU overhead.
 */

import { parquetMetadataAsync, parquetReadObjects } from 'hyparquet';
import { parquetWriteBuffer } from 'hyparquet-writer';
import { encodeVariant, decodeVariant } from './variant.js';
import type { Document } from '@types';
import { DEFAULT_ROW_GROUP_SIZE } from '@mongolake/constants.js';
import { logger } from '../utils/logger.js';

// ============================================================================
// Base64 Utilities (for binary data preservation)
// ============================================================================

/**
 * Encode Uint8Array to base64 string
 */
function uint8ArrayToBase64(bytes: Uint8Array): string {
  let binary = '';
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i]!);
  }
  return btoa(binary);
}

/**
 * Decode base64 string to Uint8Array
 */
function base64ToUint8Array(base64: string): Uint8Array {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

// ============================================================================
// Types
// ============================================================================

/**
 * Row format for MongoLake Parquet files
 */
export interface ParquetRow {
  _id: string;
  _seq: number | bigint;
  _op: string; // 'i' | 'u' | 'd'
  _data: string; // Base64-encoded Variant binary
}

/**
 * Options for writing Parquet files
 */
export interface WriteOptions {
  /** Number of rows per row group (default: DEFAULT_ROW_GROUP_SIZE) */
  rowGroupSize?: number;
}

/**
 * AsyncBuffer interface for hyparquet
 */
export interface AsyncBuffer {
  byteLength: number;
  slice(start: number, end?: number): Promise<ArrayBuffer>;
}

// ============================================================================
// Writing Functions
// ============================================================================

/**
 * Write documents to Parquet binary format
 *
 * @param rows - Array of document rows with metadata
 * @param options - Write options
 * @returns Uint8Array containing valid Parquet file
 */
export function writeParquet<T extends Document>(
  rows: Array<{ _id: string; _seq: number; _op: 'i' | 'u' | 'd'; doc: T }>,
  options: WriteOptions = {}
): Uint8Array {
  const { rowGroupSize = DEFAULT_ROW_GROUP_SIZE } = options;

  // Convert documents to columnar format
  const ids: string[] = [];
  const seqs: bigint[] = [];  // INT64 requires BigInt values
  const ops: string[] = [];
  const dataStrings: string[] = [];  // Base64-encoded variant data

  for (const row of rows) {
    ids.push(row._id);
    seqs.push(BigInt(row._seq));  // Convert to BigInt for INT64
    ops.push(row._op);
    // Encode the full document as Variant binary, then base64 encode
    // to preserve binary data through hyparquet's UTF-8 string conversion
    const variantBytes = encodeVariant(row.doc);
    dataStrings.push(uint8ArrayToBase64(variantBytes));
  }

  // Use hyparquet-writer to create actual Parquet binary (uncompressed)
  // _data is stored as a regular string (base64-encoded variant binary)
  const buffer = parquetWriteBuffer({
    columnData: [
      { name: '_id', data: ids },
      { name: '_seq', data: seqs, type: 'INT64' },
      { name: '_op', data: ops },
      { name: '_data', data: dataStrings },
    ],
    rowGroupSize,
  });

  return new Uint8Array(buffer);
}

// ============================================================================
// Reading Functions
// ============================================================================

/**
 * Create an AsyncBuffer from a Uint8Array for hyparquet
 */
export function createAsyncBufferFromBytes(data: Uint8Array): AsyncBuffer {
  return {
    byteLength: data.length,
    async slice(start: number, end?: number): Promise<ArrayBuffer> {
      const effectiveEnd = end ?? data.length;
      const slice = data.slice(start, effectiveEnd);
      // Create a new ArrayBuffer to avoid SharedArrayBuffer issues
      const buffer = new ArrayBuffer(slice.length);
      new Uint8Array(buffer).set(slice);
      return buffer;
    },
  };
}

/**
 * Read documents from Parquet binary format
 *
 * @param data - Parquet file bytes
 * @returns Array of parsed rows
 */
export async function readParquet<T extends Document>(
  data: Uint8Array
): Promise<Array<{ _id: string; _seq: number; _op: string; doc: T }>> {
  // Check for valid Parquet magic bytes
  if (data.length < 4) {
    throw new Error('Invalid Parquet file: too small');
  }

  const magic = String.fromCharCode(data[0]!, data[1]!, data[2]!, data[3]!);
  if (magic !== 'PAR1') {
    // Try to parse as legacy JSON format for backwards compatibility
    return readLegacyJson<T>(data);
  }

  // Create async buffer for hyparquet
  const asyncBuffer = createAsyncBufferFromBytes(data);

  try {
    // Read Parquet metadata first
    const metadata = await parquetMetadataAsync(asyncBuffer);

    // Read all rows - note: hyparquet returns all columns by default
    // No compressors needed since we write uncompressed files
    const rows = await parquetReadObjects({
      file: asyncBuffer,
      metadata,
    });

    // Decode variant data back to documents
    const result: Array<{ _id: string; _seq: number; _op: string; doc: T }> = [];

    for (const row of rows as Array<{ _id: string; _seq: bigint | number; _op: string; _data: string | Uint8Array }>) {
      let doc: T;

      if (typeof row._data === 'string') {
        // Base64-encoded variant binary - this is the normal path
        try {
          const variantBytes = base64ToUint8Array(row._data);
          doc = decodeVariant(variantBytes) as T;
        } catch {
          // Maybe it's legacy JSON string
          try {
            doc = JSON.parse(row._data) as T;
          } catch {
            throw new Error(`Unable to decode _data field: not valid base64 or JSON`);
          }
        }
      } else if (row._data instanceof Uint8Array) {
        // Direct Uint8Array (shouldn't happen with hyparquet but handle it)
        doc = decodeVariant(row._data) as T;
      } else if (typeof row._data === 'object' && row._data !== null) {
        // Direct object (shouldn't happen but handle gracefully)
        // The object is already in the expected document shape
        doc = row._data as T;
      } else {
        throw new Error(`Unable to decode _data field: unexpected type ${typeof row._data}`);
      }

      result.push({
        _id: String(row._id),
        _seq: Number(row._seq),
        _op: String(row._op),
        doc,
      });
    }

    return result;
  } catch (error) {
    // If Parquet parsing fails, try legacy JSON format
    logger.warn('Parquet parsing failed, trying JSON fallback', {
      error: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : undefined,
    });
    return readLegacyJson<T>(data);
  }
}

/**
 * Read legacy JSON format for backwards compatibility
 */
function readLegacyJson<T extends Document>(
  data: Uint8Array
): Array<{ _id: string; _seq: number; _op: string; doc: T }> {
  try {
    const text = new TextDecoder().decode(data);
    const rows = JSON.parse(text) as Array<{
      _id: string;
      _seq: number;
      _op: string;
      _data: T;
    }>;

    return rows.map((row) => ({
      _id: row._id,
      _seq: row._seq,
      _op: row._op,
      doc: row._data,
    }));
  } catch {
    throw new Error('Unable to parse file as Parquet or JSON');
  }
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Check if data is valid Parquet format
 */
export function isParquetFile(data: Uint8Array): boolean {
  if (data.length < 8) return false;

  // Check magic bytes at start
  const startMagic = String.fromCharCode(data[0]!, data[1]!, data[2]!, data[3]!);
  if (startMagic !== 'PAR1') return false;

  // Check magic bytes at end
  const endMagic = String.fromCharCode(
    data[data.length - 4]!,
    data[data.length - 3]!,
    data[data.length - 2]!,
    data[data.length - 1]!
  );
  if (endMagic !== 'PAR1') return false;

  return true;
}

/**
 * Get Parquet file metadata without reading all data
 */
export async function getParquetMetadata(data: Uint8Array): Promise<{
  rowCount: number;
  rowGroups: number;
  columns: string[];
}> {
  const asyncBuffer = createAsyncBufferFromBytes(data);
  const metadata = await parquetMetadataAsync(asyncBuffer);

  return {
    rowCount: Number(metadata.num_rows),
    rowGroups: metadata.row_groups.length,
    columns: metadata.schema.map((s: { name: string }) => s.name).filter((n: string) => n !== 'schema'),
  };
}
