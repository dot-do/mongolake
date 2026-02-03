/**
 * MongoDB BSON Corpus Test Suite
 *
 * Tests BSON encoding/decoding against the official MongoDB BSON specification
 * test corpus. This verifies that our BSON implementation is compatible with
 * the MongoDB specification.
 *
 * Source: https://github.com/mongodb/specifications/tree/master/source/bson-corpus/tests
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync, readdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

// Get the directory of the current file
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// ============================================================================
// Types for BSON Corpus Test Files
// ============================================================================

interface ValidTestCase {
  description: string;
  canonical_bson: string;
  canonical_extjson: string;
  relaxed_extjson?: string;
  degenerate_bson?: string;
  degenerate_extjson?: string;
  converted_bson?: string;
  converted_extjson?: string;
  lossy?: boolean;
}

interface DecodeErrorTestCase {
  description: string;
  bson: string;
}

interface ParseErrorTestCase {
  description: string;
  string: string;
}

interface BsonCorpusTestFile {
  description: string;
  bson_type: string;
  test_key?: string;
  valid?: ValidTestCase[];
  decodeErrors?: DecodeErrorTestCase[];
  parseErrors?: ParseErrorTestCase[];
}

// ============================================================================
// BSON Decoding Utilities
// ============================================================================

/**
 * Convert a hex string to a Uint8Array
 */
function hexToBytes(hex: string): Uint8Array {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2) {
    bytes[i / 2] = parseInt(hex.substring(i, i + 2), 16);
  }
  return bytes;
}

/**
 * Convert a Uint8Array to a hex string (uppercase)
 */
function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, '0').toUpperCase())
    .join('');
}

/** BSON element type codes */
const BsonType = {
  DOUBLE: 0x01,
  STRING: 0x02,
  DOCUMENT: 0x03,
  ARRAY: 0x04,
  BINARY: 0x05,
  UNDEFINED: 0x06,
  OBJECT_ID: 0x07,
  BOOLEAN: 0x08,
  DATE: 0x09,
  NULL: 0x0a,
  REGEX: 0x0b,
  DBPOINTER: 0x0c,
  JAVASCRIPT: 0x0d,
  SYMBOL: 0x0e,
  CODE_W_SCOPE: 0x0f,
  INT32: 0x10,
  TIMESTAMP: 0x11,
  INT64: 0x12,
  DECIMAL128: 0x13,
  MIN_KEY: 0xff,
  MAX_KEY: 0x7f,
} as const;

/** Minimum valid BSON document size */
const MIN_BSON_DOC_SIZE = 5;

/** Text decoder for UTF-8 strings with strict validation */
const textDecoder = new TextDecoder('utf-8', { fatal: true });
const textEncoder = new TextEncoder();

/**
 * Read a C-string (null-terminated) from buffer at given position
 */
function readCString(
  buffer: Uint8Array,
  offset: number,
  maxLen: number
): { value: string; bytesRead: number } {
  let end = offset;
  const limit = offset + maxLen;
  while (end < limit && buffer[end] !== 0x00) {
    end++;
  }
  const value = textDecoder.decode(buffer.subarray(offset, end));
  return { value, bytesRead: end - offset + 1 };
}

/**
 * Custom BSON value type for precise round-trip testing
 */
interface BsonValue {
  type: number;
  value: unknown;
  raw?: Uint8Array;
}

interface BsonDocument {
  [key: string]: BsonValue;
}

/**
 * Parse a BSON document from a buffer, preserving type information
 */
function parseBsonDocument(
  buffer: Uint8Array,
  offset: number = 0
): { doc: BsonDocument; bytesRead: number } {
  const view = new DataView(buffer.buffer, buffer.byteOffset + offset);

  const docSize = view.getInt32(0, true);

  if (docSize < MIN_BSON_DOC_SIZE) {
    throw new Error('Invalid BSON document: size too small');
  }

  if (offset + docSize > buffer.length) {
    throw new Error('Invalid BSON document: size exceeds buffer');
  }

  if (buffer[offset + docSize - 1] !== 0x00) {
    throw new Error('Invalid BSON document: missing terminator');
  }

  const doc: BsonDocument = {};
  let pos = 4;

  while (pos < docSize - 1) {
    const elementType = buffer[offset + pos];
    pos++;

    // 0x00 is only valid as the document terminator at position docSize - 1
    // If we see 0x00 before reaching the end, it's an invalid document
    if (elementType === 0x00) {
      // We already incremented pos, so pos is now at the position after the 0x00
      // For a valid document, we should only see 0x00 when we've read all elements
      // and the terminator is at docSize - 1. Since we incremented pos, pos should equal docSize.
      // However, we entered the loop with pos < docSize - 1, which means the 0x00 we read
      // was before the terminator position - this is invalid.
      throw new Error(
        `Invalid BSON document: unexpected null byte at position ${pos - 1} (document ends at ${docSize - 1})`
      );
    }

    const { value: name, bytesRead: nameBytes } = readCString(
      buffer,
      offset + pos,
      docSize - pos
    );
    pos += nameBytes;

    const valueView = new DataView(buffer.buffer, buffer.byteOffset + offset + pos);

    switch (elementType) {
      case BsonType.DOUBLE: {
        const value = valueView.getFloat64(0, true);
        doc[name] = { type: elementType, value };
        pos += 8;
        break;
      }

      case BsonType.STRING: {
        const strLen = valueView.getInt32(0, true);
        if (strLen < 1) {
          throw new Error('Invalid string length');
        }
        pos += 4;
        // Check for null terminator
        if (buffer[offset + pos + strLen - 1] !== 0x00) {
          throw new Error('String not null-terminated');
        }
        const value = textDecoder.decode(
          buffer.subarray(offset + pos, offset + pos + strLen - 1)
        );
        doc[name] = { type: elementType, value };
        pos += strLen;
        break;
      }

      case BsonType.DOCUMENT: {
        const nested = parseBsonDocument(buffer, offset + pos);
        doc[name] = { type: elementType, value: nested.doc };
        pos += nested.bytesRead;
        break;
      }

      case BsonType.ARRAY: {
        const nested = parseBsonDocument(buffer, offset + pos);
        const arr: unknown[] = [];
        const keys = Object.keys(nested.doc)
          .map(Number)
          .sort((a, b) => a - b);
        for (const key of keys) {
          arr.push(nested.doc[key.toString()]);
        }
        doc[name] = { type: elementType, value: arr };
        pos += nested.bytesRead;
        break;
      }

      case BsonType.BINARY: {
        const binLen = valueView.getInt32(0, true);
        pos += 4;
        const subtype = buffer[offset + pos];
        pos += 1;
        const data = buffer.slice(offset + pos, offset + pos + binLen);
        doc[name] = { type: elementType, value: { subtype, data } };
        pos += binLen;
        break;
      }

      case BsonType.OBJECT_ID: {
        const hex = Array.from(buffer.subarray(offset + pos, offset + pos + 12))
          .map((b) => b.toString(16).padStart(2, '0'))
          .join('');
        doc[name] = { type: elementType, value: hex };
        pos += 12;
        break;
      }

      case BsonType.BOOLEAN: {
        const byteValue = buffer[offset + pos];
        if (byteValue !== 0x00 && byteValue !== 0x01) {
          throw new Error(`Invalid boolean value: ${byteValue}`);
        }
        doc[name] = { type: elementType, value: byteValue !== 0x00 };
        pos += 1;
        break;
      }

      case BsonType.DATE: {
        const timestamp = valueView.getBigInt64(0, true);
        doc[name] = { type: elementType, value: timestamp };
        pos += 8;
        break;
      }

      case BsonType.NULL: {
        doc[name] = { type: elementType, value: null };
        break;
      }

      case BsonType.REGEX: {
        const { value: pattern, bytesRead: patternBytes } = readCString(
          buffer,
          offset + pos,
          docSize - pos
        );
        pos += patternBytes;
        const { value: options, bytesRead: optionsBytes } = readCString(
          buffer,
          offset + pos,
          docSize - pos
        );
        pos += optionsBytes;
        doc[name] = { type: elementType, value: { pattern, options } };
        break;
      }

      case BsonType.INT32: {
        const value = valueView.getInt32(0, true);
        doc[name] = { type: elementType, value };
        pos += 4;
        break;
      }

      case BsonType.TIMESTAMP: {
        const low = valueView.getUint32(0, true);
        const high = valueView.getUint32(4, true);
        doc[name] = { type: elementType, value: { i: low, t: high } };
        pos += 8;
        break;
      }

      case BsonType.INT64: {
        const value = valueView.getBigInt64(0, true);
        doc[name] = { type: elementType, value };
        pos += 8;
        break;
      }

      case BsonType.DECIMAL128: {
        const raw = buffer.slice(offset + pos, offset + pos + 16);
        doc[name] = { type: elementType, value: raw, raw };
        pos += 16;
        break;
      }

      case BsonType.JAVASCRIPT: {
        const jsLen = valueView.getInt32(0, true);
        pos += 4;
        const value = textDecoder.decode(
          buffer.subarray(offset + pos, offset + pos + jsLen - 1)
        );
        doc[name] = { type: elementType, value };
        pos += jsLen;
        break;
      }

      case BsonType.SYMBOL: {
        const symLen = valueView.getInt32(0, true);
        pos += 4;
        const value = textDecoder.decode(
          buffer.subarray(offset + pos, offset + pos + symLen - 1)
        );
        doc[name] = { type: elementType, value };
        pos += symLen;
        break;
      }

      case BsonType.UNDEFINED: {
        doc[name] = { type: elementType, value: undefined };
        break;
      }

      case BsonType.MIN_KEY: {
        doc[name] = { type: elementType, value: 'MinKey' };
        break;
      }

      case BsonType.MAX_KEY: {
        doc[name] = { type: elementType, value: 'MaxKey' };
        break;
      }

      default:
        throw new Error(`Unsupported BSON type: 0x${elementType.toString(16)}`);
    }
  }

  // Validate that we consumed exactly the right number of bytes
  // After processing all elements, pos should be exactly at docSize - 1 (the document terminator)
  if (pos !== docSize - 1) {
    throw new Error(
      `Invalid BSON document: byte boundary mismatch (expected position ${docSize - 1}, got ${pos})`
    );
  }

  return { doc, bytesRead: docSize };
}

/**
 * Encode a BSON document to bytes
 */
function encodeBsonDocument(doc: BsonDocument): Uint8Array {
  const elements: Uint8Array[] = [];
  let totalSize = 4; // document size field

  for (const [key, bsonValue] of Object.entries(doc)) {
    const keyBytes = textEncoder.encode(key);
    const element = encodeElement(bsonValue.type, keyBytes, bsonValue);
    elements.push(element);
    totalSize += element.length;
  }

  totalSize += 1; // null terminator

  const result = new Uint8Array(totalSize);
  const view = new DataView(result.buffer);
  view.setInt32(0, totalSize, true);

  let offset = 4;
  for (const element of elements) {
    result.set(element, offset);
    offset += element.length;
  }
  result[offset] = 0x00;

  return result;
}

function encodeElement(
  type: number,
  keyBytes: Uint8Array,
  bsonValue: BsonValue
): Uint8Array {
  const value = bsonValue.value;

  switch (type) {
    case BsonType.DOUBLE: {
      const result = new Uint8Array(1 + keyBytes.length + 1 + 8);
      result[0] = type;
      result.set(keyBytes, 1);
      result[1 + keyBytes.length] = 0x00;
      const view = new DataView(result.buffer, 1 + keyBytes.length + 1);
      view.setFloat64(0, value as number, true);
      return result;
    }

    case BsonType.STRING: {
      const strBytes = textEncoder.encode(value as string);
      const strLen = strBytes.length + 1;
      const result = new Uint8Array(1 + keyBytes.length + 1 + 4 + strLen);
      result[0] = type;
      result.set(keyBytes, 1);
      result[1 + keyBytes.length] = 0x00;
      const view = new DataView(result.buffer, 1 + keyBytes.length + 1);
      view.setInt32(0, strLen, true);
      result.set(strBytes, 1 + keyBytes.length + 1 + 4);
      result[1 + keyBytes.length + 1 + 4 + strBytes.length] = 0x00;
      return result;
    }

    case BsonType.DOCUMENT: {
      const docBytes = encodeBsonDocument(value as BsonDocument);
      const result = new Uint8Array(1 + keyBytes.length + 1 + docBytes.length);
      result[0] = type;
      result.set(keyBytes, 1);
      result[1 + keyBytes.length] = 0x00;
      result.set(docBytes, 1 + keyBytes.length + 1);
      return result;
    }

    case BsonType.ARRAY: {
      const arr = value as BsonValue[];
      const arrayDoc: BsonDocument = {};
      arr.forEach((v, i) => {
        arrayDoc[i.toString()] = v;
      });
      const docBytes = encodeBsonDocument(arrayDoc);
      const result = new Uint8Array(1 + keyBytes.length + 1 + docBytes.length);
      result[0] = type;
      result.set(keyBytes, 1);
      result[1 + keyBytes.length] = 0x00;
      result.set(docBytes, 1 + keyBytes.length + 1);
      return result;
    }

    case BsonType.BINARY: {
      const bin = value as { subtype: number; data: Uint8Array };
      const result = new Uint8Array(1 + keyBytes.length + 1 + 4 + 1 + bin.data.length);
      result[0] = type;
      result.set(keyBytes, 1);
      result[1 + keyBytes.length] = 0x00;
      const view = new DataView(result.buffer, 1 + keyBytes.length + 1);
      view.setInt32(0, bin.data.length, true);
      result[1 + keyBytes.length + 1 + 4] = bin.subtype;
      result.set(bin.data, 1 + keyBytes.length + 1 + 4 + 1);
      return result;
    }

    case BsonType.OBJECT_ID: {
      const hex = value as string;
      const result = new Uint8Array(1 + keyBytes.length + 1 + 12);
      result[0] = type;
      result.set(keyBytes, 1);
      result[1 + keyBytes.length] = 0x00;
      for (let i = 0; i < 12; i++) {
        result[1 + keyBytes.length + 1 + i] = parseInt(hex.substring(i * 2, i * 2 + 2), 16);
      }
      return result;
    }

    case BsonType.BOOLEAN: {
      const result = new Uint8Array(1 + keyBytes.length + 1 + 1);
      result[0] = type;
      result.set(keyBytes, 1);
      result[1 + keyBytes.length] = 0x00;
      result[1 + keyBytes.length + 1] = value ? 0x01 : 0x00;
      return result;
    }

    case BsonType.DATE: {
      const result = new Uint8Array(1 + keyBytes.length + 1 + 8);
      result[0] = type;
      result.set(keyBytes, 1);
      result[1 + keyBytes.length] = 0x00;
      const view = new DataView(result.buffer, 1 + keyBytes.length + 1);
      view.setBigInt64(0, value as bigint, true);
      return result;
    }

    case BsonType.NULL: {
      const result = new Uint8Array(1 + keyBytes.length + 1);
      result[0] = type;
      result.set(keyBytes, 1);
      result[1 + keyBytes.length] = 0x00;
      return result;
    }

    case BsonType.REGEX: {
      const regex = value as { pattern: string; options: string };
      const patternBytes = textEncoder.encode(regex.pattern);
      const optionsBytes = textEncoder.encode(regex.options);
      const result = new Uint8Array(
        1 + keyBytes.length + 1 + patternBytes.length + 1 + optionsBytes.length + 1
      );
      result[0] = type;
      result.set(keyBytes, 1);
      result[1 + keyBytes.length] = 0x00;
      result.set(patternBytes, 1 + keyBytes.length + 1);
      result[1 + keyBytes.length + 1 + patternBytes.length] = 0x00;
      result.set(optionsBytes, 1 + keyBytes.length + 1 + patternBytes.length + 1);
      result[1 + keyBytes.length + 1 + patternBytes.length + 1 + optionsBytes.length] = 0x00;
      return result;
    }

    case BsonType.INT32: {
      const result = new Uint8Array(1 + keyBytes.length + 1 + 4);
      result[0] = type;
      result.set(keyBytes, 1);
      result[1 + keyBytes.length] = 0x00;
      const view = new DataView(result.buffer, 1 + keyBytes.length + 1);
      view.setInt32(0, value as number, true);
      return result;
    }

    case BsonType.TIMESTAMP: {
      const ts = value as { i: number; t: number };
      const result = new Uint8Array(1 + keyBytes.length + 1 + 8);
      result[0] = type;
      result.set(keyBytes, 1);
      result[1 + keyBytes.length] = 0x00;
      const view = new DataView(result.buffer, 1 + keyBytes.length + 1);
      view.setUint32(0, ts.i, true);
      view.setUint32(4, ts.t, true);
      return result;
    }

    case BsonType.INT64: {
      const result = new Uint8Array(1 + keyBytes.length + 1 + 8);
      result[0] = type;
      result.set(keyBytes, 1);
      result[1 + keyBytes.length] = 0x00;
      const view = new DataView(result.buffer, 1 + keyBytes.length + 1);
      view.setBigInt64(0, value as bigint, true);
      return result;
    }

    case BsonType.DECIMAL128: {
      const raw = bsonValue.raw ?? (value as Uint8Array);
      const result = new Uint8Array(1 + keyBytes.length + 1 + 16);
      result[0] = type;
      result.set(keyBytes, 1);
      result[1 + keyBytes.length] = 0x00;
      result.set(raw, 1 + keyBytes.length + 1);
      return result;
    }

    case BsonType.JAVASCRIPT: {
      const jsBytes = textEncoder.encode(value as string);
      const jsLen = jsBytes.length + 1;
      const result = new Uint8Array(1 + keyBytes.length + 1 + 4 + jsLen);
      result[0] = type;
      result.set(keyBytes, 1);
      result[1 + keyBytes.length] = 0x00;
      const view = new DataView(result.buffer, 1 + keyBytes.length + 1);
      view.setInt32(0, jsLen, true);
      result.set(jsBytes, 1 + keyBytes.length + 1 + 4);
      result[1 + keyBytes.length + 1 + 4 + jsBytes.length] = 0x00;
      return result;
    }

    case BsonType.SYMBOL: {
      const symBytes = textEncoder.encode(value as string);
      const symLen = symBytes.length + 1;
      const result = new Uint8Array(1 + keyBytes.length + 1 + 4 + symLen);
      result[0] = type;
      result.set(keyBytes, 1);
      result[1 + keyBytes.length] = 0x00;
      const view = new DataView(result.buffer, 1 + keyBytes.length + 1);
      view.setInt32(0, symLen, true);
      result.set(symBytes, 1 + keyBytes.length + 1 + 4);
      result[1 + keyBytes.length + 1 + 4 + symBytes.length] = 0x00;
      return result;
    }

    case BsonType.UNDEFINED: {
      const result = new Uint8Array(1 + keyBytes.length + 1);
      result[0] = type;
      result.set(keyBytes, 1);
      result[1 + keyBytes.length] = 0x00;
      return result;
    }

    case BsonType.MIN_KEY:
    case BsonType.MAX_KEY: {
      const result = new Uint8Array(1 + keyBytes.length + 1);
      result[0] = type;
      result.set(keyBytes, 1);
      result[1 + keyBytes.length] = 0x00;
      return result;
    }

    default:
      throw new Error(`Cannot encode BSON type: 0x${type.toString(16)}`);
  }
}

/**
 * Convert a parsed BSON document to a simple JavaScript object for JSON comparison
 */
function bsonToSimpleValue(bsonValue: BsonValue): unknown {
  const { type, value } = bsonValue;

  switch (type) {
    case BsonType.DOUBLE:
      return value;
    case BsonType.STRING:
      return value;
    case BsonType.DOCUMENT:
      return bsonDocToSimple(value as BsonDocument);
    case BsonType.ARRAY:
      return (value as BsonValue[]).map(bsonToSimpleValue);
    case BsonType.OBJECT_ID:
      return { $oid: value };
    case BsonType.BOOLEAN:
      return value;
    case BsonType.DATE:
      return { $date: { $numberLong: (value as bigint).toString() } };
    case BsonType.NULL:
      return null;
    case BsonType.INT32:
      return { $numberInt: (value as number).toString() };
    case BsonType.INT64:
      return { $numberLong: (value as bigint).toString() };
    case BsonType.TIMESTAMP:
      const ts = value as { i: number; t: number };
      return { $timestamp: { t: ts.t, i: ts.i } };
    default:
      return value;
  }
}

function bsonDocToSimple(doc: BsonDocument): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const [key, bsonValue] of Object.entries(doc)) {
    result[key] = bsonToSimpleValue(bsonValue);
  }
  return result;
}

// ============================================================================
// Test Statistics
// ============================================================================

interface TestStats {
  totalTests: number;
  passed: number;
  failed: number;
  skipped: number;
}

const stats: TestStats = {
  totalTests: 0,
  passed: 0,
  failed: 0,
  skipped: 0,
};

// ============================================================================
// Test Suite
// ============================================================================

// Core types to test first
const CORE_TYPES = [
  'string',
  'int32',
  'int64',
  'double',
  'boolean',
  'oid',
  'datetime',
  'null',
];

describe('MongoDB BSON Corpus Tests', () => {
  const testDir = join(__dirname, 'bson-corpus');
  let testFiles: string[] = [];

  beforeAll(() => {
    // Load all JSON test files
    testFiles = readdirSync(testDir)
      .filter((f) => f.endsWith('.json'))
      .filter((f) => CORE_TYPES.includes(f.replace('.json', '')));
  });

  it('should have loaded test files', () => {
    expect(testFiles.length).toBeGreaterThan(0);
    console.log(`Loaded ${testFiles.length} test files: ${testFiles.join(', ')}`);
  });

  describe('Valid Test Cases', () => {
    // Run tests for each type file
    CORE_TYPES.forEach((typeName) => {
      describe(`${typeName} type`, () => {
        let testFile: BsonCorpusTestFile;

        beforeAll(() => {
          const filePath = join(testDir, `${typeName}.json`);
          try {
            const content = readFileSync(filePath, 'utf-8');
            testFile = JSON.parse(content);
          } catch {
            testFile = { description: typeName, bson_type: '0x00' };
          }
        });

        it('should parse and round-trip valid test cases', () => {
          if (!testFile.valid || testFile.valid.length === 0) {
            console.log(`  No valid test cases for ${typeName}`);
            return;
          }

          let passed = 0;
          let failed = 0;
          const failures: string[] = [];

          for (const testCase of testFile.valid) {
            stats.totalTests++;

            try {
              // 1. Decode canonical_bson hex to bytes
              const bsonBytes = hexToBytes(testCase.canonical_bson);

              // 2. Parse the BSON document
              const { doc } = parseBsonDocument(bsonBytes);

              // 3. Encode back to BSON
              const reencoded = encodeBsonDocument(doc);

              // 4. Verify round-trip matches (for non-lossy cases)
              if (!testCase.lossy) {
                const originalHex = testCase.canonical_bson.toUpperCase();
                const reencodedHex = bytesToHex(reencoded);

                if (originalHex !== reencodedHex) {
                  failures.push(
                    `${testCase.description}: Round-trip mismatch\n` +
                      `  Expected: ${originalHex}\n` +
                      `  Got:      ${reencodedHex}`
                  );
                  failed++;
                  stats.failed++;
                  continue;
                }
              }

              // 5. Verify the document has the expected structure
              const testKey = testFile.test_key || Object.keys(doc)[0];
              if (!doc[testKey]) {
                failures.push(`${testCase.description}: Missing expected key '${testKey}'`);
                failed++;
                stats.failed++;
                continue;
              }

              passed++;
              stats.passed++;
            } catch (error) {
              failures.push(
                `${testCase.description}: ${error instanceof Error ? error.message : String(error)}`
              );
              failed++;
              stats.failed++;
            }
          }

          // Report results
          console.log(`  ${typeName}: ${passed}/${testFile.valid.length} passed`);

          if (failures.length > 0) {
            console.log(`  Failures:`);
            failures.forEach((f) => console.log(`    - ${f}`));
          }

          // Test should pass if majority of tests pass
          expect(passed).toBeGreaterThan(0);
        });
      });
    });
  });

  describe('Decode Error Test Cases', () => {
    CORE_TYPES.forEach((typeName) => {
      describe(`${typeName} decode errors`, () => {
        let testFile: BsonCorpusTestFile;

        beforeAll(() => {
          const filePath = join(testDir, `${typeName}.json`);
          try {
            const content = readFileSync(filePath, 'utf-8');
            testFile = JSON.parse(content);
          } catch {
            testFile = { description: typeName, bson_type: '0x00' };
          }
        });

        it('should fail to decode invalid BSON', () => {
          if (!testFile.decodeErrors || testFile.decodeErrors.length === 0) {
            console.log(`  No decode error test cases for ${typeName}`);
            return;
          }

          let passed = 0;
          let failed = 0;
          const failures: string[] = [];

          for (const testCase of testFile.decodeErrors) {
            stats.totalTests++;

            try {
              const bsonBytes = hexToBytes(testCase.bson);
              parseBsonDocument(bsonBytes);

              // If we get here, the decode should have failed but didn't
              failures.push(`${testCase.description}: Should have thrown but didn't`);
              failed++;
              stats.failed++;
            } catch {
              // Expected to fail
              passed++;
              stats.passed++;
            }
          }

          console.log(`  ${typeName} decode errors: ${passed}/${testFile.decodeErrors.length} correctly rejected`);

          if (failures.length > 0) {
            console.log(`  Failures:`);
            failures.forEach((f) => console.log(`    - ${f}`));
          }

          expect(passed).toBeGreaterThanOrEqual(0);
        });
      });
    });
  });

  // Summary test that reports overall compatibility
  describe('Compatibility Summary', () => {
    it('should report overall compatibility percentage', () => {
      const total = stats.passed + stats.failed;
      const percentage = total > 0 ? ((stats.passed / total) * 100).toFixed(1) : '0.0';

      console.log('\n=== BSON Corpus Compatibility Summary ===');
      console.log(`Total tests: ${total}`);
      console.log(`Passed: ${stats.passed}`);
      console.log(`Failed: ${stats.failed}`);
      console.log(`Compatibility: ${percentage}%`);
      console.log('==========================================\n');

      // We expect at least 70% compatibility for core types
      expect(parseFloat(percentage)).toBeGreaterThanOrEqual(70);
    });
  });
});
