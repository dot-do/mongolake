/**
 * Variant Encoder
 *
 * Implements Parquet Variant binary format encoding and decoding.
 * The Variant format is a self-describing binary format that can represent
 * any JSON-like value with type information embedded in the encoding.
 *
 * Format design:
 * - Lower 4 bits (0x0F) of the first byte encode the base type
 * - Upper 4 bits (0xF0) encode sub-type information (size, flags, etc.)
 *
 * Spec reference: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
 */

import { ObjectId } from '@types';

// ============================================================================
// Type Constants
// ============================================================================

/**
 * Variant type identifiers (lower 4 bits of header byte)
 */
export const VariantType = {
  NULL: 0x00,
  BOOLEAN: 0x01,
  INT: 0x02,
  FLOAT: 0x03,
  STRING: 0x04,
  ARRAY: 0x05,
  OBJECT: 0x06,
  DATE: 0x07,
  BINARY: 0x08,
  OBJECT_ID: 0x09,

  /**
   * Extract the type from an encoded variant buffer.
   * Throws if the buffer is empty.
   */
  fromEncoded(encoded: Uint8Array): number {
    if (encoded.length === 0) {
      throw new Error('Cannot extract type from empty buffer');
    }
    return encoded[0]! & 0x0f;
  },
} as const;

// Integer size sub-types (upper 4 bits when type is INT)
const INT_SIZE = {
  INT8: 0x10,
  INT16: 0x20,
  INT32: 0x30,
  INT64: 0x40,
} as const;

// Boolean value indicators (upper 4 bits when type is BOOLEAN)
const BOOL_TRUE = 0x10;
const BOOL_FALSE = 0x00;

// ============================================================================
// Encoding Functions
// ============================================================================

/**
 * Encode a JavaScript value to Parquet Variant binary format.
 *
 * Supports null, boolean, number (int/float), string, Date, Uint8Array, ObjectId,
 * Array, and plain objects. Rejects undefined, functions, symbols, and BigInt.
 *
 * @param value - The value to encode
 * @returns Uint8Array containing the encoded variant
 * @throws Error if the value type is not supported or is undefined
 */
export function encodeVariant(value: unknown): Uint8Array {
  if (value === null) {
    return encodeNull();
  }

  if (value === undefined) {
    throw new Error('Cannot encode undefined: not a valid Variant value');
  }

  if (typeof value === 'boolean') {
    return encodeBoolean(value);
  }

  if (typeof value === 'number') {
    return encodeNumber(value);
  }

  if (typeof value === 'string') {
    return encodeString(value);
  }

  if (value instanceof Date) {
    return encodeDate(value);
  }

  if (value instanceof Uint8Array) {
    return encodeBinary(value);
  }

  if (value instanceof ObjectId) {
    return encodeObjectId(value);
  }

  if (Array.isArray(value)) {
    return encodeArray(value);
  }

  if (typeof value === 'object') {
    return encodeObject(value as Record<string, unknown>);
  }

  // Unsupported types
  if (typeof value === 'function') {
    throw new Error('Cannot encode function: functions are not serializable');
  }

  if (typeof value === 'symbol') {
    throw new Error('Cannot encode symbol: symbols are not serializable');
  }

  if (typeof value === 'bigint') {
    throw new Error('Cannot encode BigInt: use safe integers instead');
  }

  throw new Error(`Unsupported value type: ${typeof value}`);
}

function encodeNull(): Uint8Array {
  return new Uint8Array([VariantType.NULL]);
}

function encodeBoolean(value: boolean): Uint8Array {
  const header = VariantType.BOOLEAN | (value ? BOOL_TRUE : BOOL_FALSE);
  return new Uint8Array([header]);
}

function encodeNumber(value: number): Uint8Array {
  // Handle -0 as 0 (they are equal in JavaScript comparisons)
  if (Object.is(value, -0)) {
    return encodeInteger(0);
  }
  // Check if it's a safe integer that can be precisely represented
  // Large values like 1e308 are "integers" but cannot be precisely stored as int64
  if (Number.isInteger(value) && Number.isSafeInteger(value)) {
    return encodeInteger(value);
  }
  // Float (double precision) - for all non-safe-integer numbers
  return encodeFloat(value);
}

function encodeInteger(value: number): Uint8Array {
  // Choose smallest integer type that can hold the value
  if (value >= -128 && value <= 127) {
    return encodeFixedInt(2, INT_SIZE.INT8, (view) => view.setInt8(1, value));
  }

  if (value >= -32768 && value <= 32767) {
    return encodeFixedInt(3, INT_SIZE.INT16, (view) => view.setInt16(1, value, true));
  }

  if (value >= -2147483648 && value <= 2147483647) {
    return encodeFixedInt(5, INT_SIZE.INT32, (view) => view.setInt32(1, value, true));
  }

  // int64: use BigInt for precision beyond JavaScript's safe integer limit
  return encodeFixedInt(9, INT_SIZE.INT64, (view) => view.setBigInt64(1, BigInt(value), true));
}

/**
 * Helper to encode a fixed-size integer with a specific size sub-type.
 * Sets the header byte and then calls the setter function to write the value.
 */
function encodeFixedInt(
  totalSize: number,
  sizeSubType: number,
  setter: (view: DataView) => void
): Uint8Array {
  const buffer = new ArrayBuffer(totalSize);
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer);

  bytes[0] = VariantType.INT | sizeSubType;
  setter(view);

  return bytes;
}

function encodeFloat(value: number): Uint8Array {
  const buffer = new ArrayBuffer(9);
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer);
  bytes[0] = VariantType.FLOAT;
  view.setFloat64(1, value, true);
  return bytes;
}

function encodeString(value: string): Uint8Array {
  const encoder = new TextEncoder();
  const stringBytes = encoder.encode(value);
  const lengthBytes = encodeVarInt(stringBytes.length);

  // Layout: [type | length-varint | string-data]
  const buffer = new Uint8Array(1 + lengthBytes.length + stringBytes.length);
  buffer[0] = VariantType.STRING;
  buffer.set(lengthBytes, 1);
  buffer.set(stringBytes, 1 + lengthBytes.length);

  return buffer;
}

function encodeDate(value: Date): Uint8Array {
  const arrayBuf = new ArrayBuffer(9);
  const bytes = new Uint8Array(arrayBuf);
  const view = new DataView(arrayBuf);
  bytes[0] = VariantType.DATE;
  view.setBigInt64(1, BigInt(value.getTime()), true);
  return bytes;
}

function encodeBinary(value: Uint8Array): Uint8Array {
  const lengthBytes = encodeVarInt(value.length);
  const buffer = new Uint8Array(1 + lengthBytes.length + value.length);
  buffer[0] = VariantType.BINARY;
  buffer.set(lengthBytes, 1);
  buffer.set(value, 1 + lengthBytes.length);
  return buffer;
}

function encodeObjectId(value: ObjectId): Uint8Array {
  // ObjectId is always 12 bytes
  const hexStr = value.toString();
  const buffer = new Uint8Array(13); // 1 header + 12 bytes
  buffer[0] = VariantType.OBJECT_ID;
  for (let i = 0; i < 12; i++) {
    buffer[i + 1] = parseInt(hexStr.slice(i * 2, i * 2 + 2), 16);
  }
  return buffer;
}

function encodeArray(value: unknown[]): Uint8Array {
  // Encode each element, converting undefined to null for sparse arrays
  // Use indexed loop to properly handle sparse arrays (holes in the array)
  const encodedElements: Uint8Array[] = [];
  for (let i = 0; i < value.length; i++) {
    const item = value[i];
    // Explicitly check for undefined (including holes in sparse arrays)
    encodedElements.push(encodeVariant(item === undefined ? null : item));
  }

  const lengthBytes = encodeVarInt(value.length);

  // Calculate total size: header + length-varint + all elements
  let totalSize = 1 + lengthBytes.length;
  for (const elem of encodedElements) {
    totalSize += elem.length;
  }

  // Build buffer: [type | length-varint | element1 | element2 | ...]
  const buffer = new Uint8Array(totalSize);
  buffer[0] = VariantType.ARRAY;
  buffer.set(lengthBytes, 1);

  let offset = 1 + lengthBytes.length;
  for (const elem of encodedElements) {
    buffer.set(elem, offset);
    offset += elem.length;
  }

  return buffer;
}

function encodeObject(value: Record<string, unknown>): Uint8Array {
  const keys = Object.keys(value);

  // Encode all key-value pairs
  const pairs = keys.map((key) => {
    const fieldValue = value[key];
    // Convert undefined to null for consistency
    const encodedValue = encodeVariant(fieldValue === undefined ? null : fieldValue);
    const encodedKey = encodeStringData(key);

    return { encodedKey, encodedValue };
  });

  const keyCountBytes = encodeVarInt(keys.length);

  // Calculate total size: header + key-count-varint + all pairs
  let totalSize = 1 + keyCountBytes.length;
  for (const pair of pairs) {
    totalSize += pair.encodedKey.length + pair.encodedValue.length;
  }

  // Build buffer: [type | key-count-varint | (key-length-varint | key-data | value)* ]
  const buffer = new Uint8Array(totalSize);
  buffer[0] = VariantType.OBJECT;
  buffer.set(keyCountBytes, 1);

  let offset = 1 + keyCountBytes.length;
  for (const pair of pairs) {
    buffer.set(pair.encodedKey, offset);
    offset += pair.encodedKey.length;
    buffer.set(pair.encodedValue, offset);
    offset += pair.encodedValue.length;
  }

  return buffer;
}

/**
 * Encode a string as length-prefixed UTF-8 data (without type marker).
 * Used for object keys and similar cases where we need string data only.
 */
function encodeStringData(value: string): Uint8Array {
  const stringBytes = new TextEncoder().encode(value);
  const lengthBytes = encodeVarInt(stringBytes.length);

  const result = new Uint8Array(lengthBytes.length + stringBytes.length);
  result.set(lengthBytes, 0);
  result.set(stringBytes, lengthBytes.length);

  return result;
}

// ============================================================================
// Decoding Functions
// ============================================================================

/**
 * Decode a Parquet Variant binary buffer to a JavaScript value.
 *
 * @param data - The encoded variant buffer
 * @returns The decoded JavaScript value
 * @throws Error if the buffer is empty or contains invalid type markers
 */
export function decodeVariant(data: Uint8Array): unknown {
  const { value } = decodeVariantAt(data, 0);
  return value;
}

/**
 * Result of decoding a variant at a given offset.
 */
interface DecodeResult {
  value: unknown;
  bytesRead: number;
}

/**
 * Decode a variant value at the specified offset within a buffer.
 *
 * @param data - The buffer containing encoded variant data
 * @param offset - The byte offset where this variant begins
 * @returns The decoded value and number of bytes consumed
 * @throws Error if buffer bounds are exceeded or type marker is invalid
 */
function decodeVariantAt(data: Uint8Array, offset: number): DecodeResult {
  if (offset >= data.length) {
    throw new Error('Unexpected end of buffer while reading variant type');
  }

  const header = data[offset]!;
  const type = header & 0x0f; // Lower nibble contains the base type

  switch (type) {
    case VariantType.NULL:
      return { value: null, bytesRead: 1 };

    case VariantType.BOOLEAN: {
      // Boolean value is encoded in the upper nibble
      const isTrue = (header & 0xf0) === BOOL_TRUE;
      return { value: isTrue, bytesRead: 1 };
    }

    case VariantType.INT: {
      return decodeInteger(data, offset, header!);
    }

    case VariantType.FLOAT: {
      return decodeFloat(data, offset);
    }

    case VariantType.STRING: {
      return decodeString(data, offset);
    }

    case VariantType.DATE: {
      return decodeDate(data, offset);
    }

    case VariantType.BINARY: {
      return decodeBinary(data, offset);
    }

    case VariantType.OBJECT_ID: {
      return decodeObjectId(data, offset);
    }

    case VariantType.ARRAY: {
      return decodeArray(data, offset);
    }

    case VariantType.OBJECT: {
      return decodeObject(data, offset);
    }

    default:
      throw new Error(`Unknown variant type: 0x${type.toString(16)}`);
  }
}

/**
 * Decode an integer value (int8, int16, int32, or int64).
 * The subtype is encoded in the upper nibble of the header.
 */
function decodeInteger(data: Uint8Array, offset: number, header: number): DecodeResult {
  const sizeMarker = header & 0xf0; // Upper nibble indicates size

  switch (sizeMarker) {
    case INT_SIZE.INT8: {
      // int8: single byte stored as two's complement
      // Arithmetic right shift converts unsigned byte to signed int
      return { value: (data[offset + 1]! << 24) >> 24, bytesRead: 2 };
    }

    case INT_SIZE.INT16: {
      // int16: 2 bytes, little-endian
      const intBytes = new Uint8Array(2);
      intBytes[0] = data[offset + 1]!;
      intBytes[1] = data[offset + 2]!;
      const view = new DataView(intBytes.buffer);
      return { value: view.getInt16(0, true), bytesRead: 3 };
    }

    case INT_SIZE.INT32: {
      // int32: 4 bytes, little-endian
      const intBytes = new Uint8Array(4);
      for (let i = 0; i < 4; i++) {
        intBytes[i] = data[offset + 1 + i]!;
      }
      const view = new DataView(intBytes.buffer);
      return { value: view.getInt32(0, true), bytesRead: 5 };
    }

    case INT_SIZE.INT64: {
      // int64: 8 bytes, little-endian
      const intBytes = new Uint8Array(8);
      for (let i = 0; i < 8; i++) {
        intBytes[i] = data[offset + 1 + i]!;
      }
      const view = new DataView(intBytes.buffer);
      const bigIntValue = view.getBigInt64(0, true);

      // Check if the value exceeds JavaScript's safe integer range
      // to prevent silent precision loss
      if (
        bigIntValue > BigInt(Number.MAX_SAFE_INTEGER) ||
        bigIntValue < BigInt(Number.MIN_SAFE_INTEGER)
      ) {
        // Return as BigInt to preserve precision
        return { value: bigIntValue, bytesRead: 9 };
      }

      return { value: Number(bigIntValue), bytesRead: 9 };
    }

    default:
      throw new Error(`Unknown integer size encoding: 0x${sizeMarker.toString(16)}`);
  }
}

/**
 * Decode a 64-bit floating point value (IEEE 754 double, little-endian).
 */
function decodeFloat(data: Uint8Array, offset: number): DecodeResult {
  // Copy 8 bytes to create a properly aligned ArrayBuffer
  const floatBytes = new Uint8Array(8);
  for (let i = 0; i < 8; i++) {
    floatBytes[i] = data[offset + 1 + i]!;
  }
  const view = new DataView(floatBytes.buffer);
  return { value: view.getFloat64(0, true), bytesRead: 9 };
}

/**
 * Decode a UTF-8 string with variable-length integer length prefix.
 */
function decodeString(data: Uint8Array, offset: number): DecodeResult {
  const { value: length, bytesRead: lengthBytes } = decodeVarIntAt(data, offset + 1);
  const stringStart = offset + 1 + lengthBytes;
  const stringEnd = stringStart + length;
  const stringBytes = data.slice(stringStart, stringEnd);
  const decoder = new TextDecoder();
  return { value: decoder.decode(stringBytes), bytesRead: 1 + lengthBytes + length };
}

/**
 * Decode a Date value stored as milliseconds since epoch (int64, little-endian).
 */
function decodeDate(data: Uint8Array, offset: number): DecodeResult {
  const dateBytes = new Uint8Array(8);
  for (let i = 0; i < 8; i++) {
    dateBytes[i] = data[offset + 1 + i]!;
  }
  const view = new DataView(dateBytes.buffer);
  const timestamp = view.getBigInt64(0, true);
  return { value: new Date(Number(timestamp)), bytesRead: 9 };
}

/**
 * Decode binary data with variable-length integer length prefix.
 */
function decodeBinary(data: Uint8Array, offset: number): DecodeResult {
  const { value: length, bytesRead: lengthBytes } = decodeVarIntAt(data, offset + 1);
  const binaryStart = offset + 1 + lengthBytes;
  const binaryEnd = binaryStart + length;
  const binary = new Uint8Array(data.slice(binaryStart, binaryEnd));
  return { value: binary, bytesRead: 1 + lengthBytes + length };
}

/**
 * Decode a MongoDB ObjectId (12 fixed bytes converted to hex string).
 */
function decodeObjectId(data: Uint8Array, offset: number): DecodeResult {
  const bytes = new Uint8Array(12);
  for (let i = 0; i < 12; i++) {
    bytes[i] = data[offset + 1 + i]!;
  }
  // Convert bytes to hex string (2 chars per byte)
  const hexStr = Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
  return { value: new ObjectId(hexStr), bytesRead: 13 };
}

/**
 * Decode an array of variant values with variable-length integer element count prefix.
 */
function decodeArray(data: Uint8Array, offset: number): DecodeResult {
  const { value: elementCount, bytesRead: countBytes } = decodeVarIntAt(data, offset + 1);
  const elements: unknown[] = [];
  let currentOffset = offset + 1 + countBytes;

  for (let i = 0; i < elementCount; i++) {
    const { value, bytesRead } = decodeVariantAt(data, currentOffset);
    elements.push(value);
    currentOffset += bytesRead;
  }

  return { value: elements, bytesRead: currentOffset - offset };
}

/**
 * Decode an object (map) with variable-length integer key count prefix.
 * Format: varint key_count, then for each key: (varint key_length, utf8 key_bytes, variant value)
 */
function decodeObject(data: Uint8Array, offset: number): DecodeResult {
  const { value: keyCount, bytesRead: countBytes } = decodeVarIntAt(data, offset + 1);
  const result: Record<string, unknown> = Object.create(null);
  let currentOffset = offset + 1 + countBytes;

  for (let i = 0; i < keyCount; i++) {
    // Decode key: varint length + utf8 bytes
    const { value: keyLength, bytesRead: keyLengthBytes } = decodeVarIntAt(data, currentOffset);
    currentOffset += keyLengthBytes;

    const keyStart = currentOffset;
    const keyEnd = keyStart + keyLength;
    const keyBytes = data.slice(keyStart, keyEnd);
    const key = new TextDecoder().decode(keyBytes);
    currentOffset = keyEnd;

    // Decode value as a variant
    const { value, bytesRead: valueBytes } = decodeVariantAt(data, currentOffset);
    result[key] = value;
    currentOffset += valueBytes;
  }

  return { value: result, bytesRead: currentOffset - offset };
}

// ============================================================================
// Variable-Length Integer Encoding (LEB128-style)
// ============================================================================

/**
 * Encode a non-negative integer as a variable-length integer (varint).
 * Uses LEB128-style encoding: lower 7 bits for value, high bit as continuation flag.
 * More efficient for small values (1 byte for 0-127).
 */
function encodeVarInt(value: number): Uint8Array {
  const bytes: number[] = [];
  let remaining = value;
  do {
    let byte = remaining & 0x7f;
    remaining >>>= 7;
    if (remaining !== 0) {
      byte |= 0x80; // Set continuation bit
    }
    bytes.push(byte);
  } while (remaining !== 0);
  return new Uint8Array(bytes);
}

/**
 * Decode a variable-length integer (varint) at the specified offset.
 * Returns both the decoded value and the number of bytes consumed.
 */
function decodeVarIntAt(data: Uint8Array, offset: number): { value: number; bytesRead: number } {
  let result = 0;
  let shift = 0;
  let bytesRead = 0;

  while (offset + bytesRead < data.length) {
    const byte = data[offset + bytesRead]!;
    bytesRead++;
    // Lower 7 bits contain data
    result |= (byte & 0x7f) << shift;
    // High bit (0x80) indicates continuation
    if ((byte & 0x80) === 0) {
      break;
    }
    shift += 7;
  }

  return { value: result, bytesRead };
}
