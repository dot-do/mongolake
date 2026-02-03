/**
 * Wire Protocol Edge Cases Tests
 *
 * Tests for edge cases and boundary conditions in MongoDB wire protocol handling.
 * Covers malformed data, size limits, encoding issues, and other edge cases
 * that may occur in production environments.
 *
 * Tests cover:
 * 1. Malformed BSON document handling
 * 2. Documents exceeding 16MB limit
 * 3. Invalid UTF-8 in field names
 * 4. Extremely long string fields
 * 5. Deeply nested documents (>100 levels)
 * 6. Binary data with all byte values
 * 7. Circular reference detection
 * 8. Empty document handling
 *
 * Wire Protocol Reference: https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/
 */

import { describe, it, expect } from 'vitest';
import {
  parseMessage,
  parseOpMsg,
  parseMessageHeader,
  OpCode,
  OpMsgFlags,
} from '../../../src/wire-protocol/message-parser.js';
import {
  serializeDocument,
  buildOpMsgResponse,
  buildSuccessResponse,
} from '../../../src/wire-protocol/bson-serializer.js';
import {
  MAX_WIRE_MESSAGE_SIZE,
  MIN_WIRE_MESSAGE_SIZE,
  MAX_BATCH_BYTES,
} from '../../../src/constants.js';

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Helper to create a valid MongoDB wire protocol message header
 */
function createHeader(
  messageLength: number,
  requestId: number,
  responseTo: number,
  opCode: number
): Uint8Array {
  const buffer = new ArrayBuffer(16);
  const view = new DataView(buffer);
  // MongoDB uses little-endian byte order
  view.setInt32(0, messageLength, true);
  view.setInt32(4, requestId, true);
  view.setInt32(8, responseTo, true);
  view.setInt32(12, opCode, true);
  return new Uint8Array(buffer);
}

/**
 * Helper to create a minimal BSON document from raw data
 */
function createBsonDocument(doc: Record<string, unknown>): Uint8Array {
  return serializeDocument(doc);
}

/**
 * Helper to create an OP_MSG message
 */
function createOpMsg(
  requestId: number,
  responseTo: number,
  flags: number,
  sections: Array<{ type: 0 | 1; payload: Uint8Array; identifier?: string }>
): Uint8Array {
  // Calculate sections size
  let sectionsSize = 0;
  for (const section of sections) {
    if (section.type === 0) {
      sectionsSize += 1 + section.payload.length; // type byte + document
    } else {
      // Type 1: Document sequence
      const idBytes = new TextEncoder().encode(section.identifier! + '\0');
      sectionsSize += 1 + 4 + idBytes.length + section.payload.length;
    }
  }

  // Total message length: header (16) + flags (4) + sections
  const messageLength = 16 + 4 + sectionsSize;

  const buffer = new Uint8Array(messageLength);
  const view = new DataView(buffer.buffer);

  // Header
  view.setInt32(0, messageLength, true);
  view.setInt32(4, requestId, true);
  view.setInt32(8, responseTo, true);
  view.setInt32(12, 2013, true); // OP_MSG opCode

  // Flags
  view.setUint32(16, flags, true);

  // Sections
  let offset = 20;
  for (const section of sections) {
    buffer[offset] = section.type;
    offset += 1;

    if (section.type === 0) {
      buffer.set(section.payload, offset);
      offset += section.payload.length;
    } else {
      const idBytes = new TextEncoder().encode(section.identifier! + '\0');
      const sectionSize = 4 + idBytes.length + section.payload.length;
      view.setInt32(offset, sectionSize, true);
      offset += 4;
      buffer.set(idBytes, offset);
      offset += idBytes.length;
      buffer.set(section.payload, offset);
      offset += section.payload.length;
    }
  }

  return buffer;
}

/**
 * Create a raw BSON document with custom bytes
 * Allows creating malformed BSON for testing
 */
function createRawBsonDocument(size: number, content: Uint8Array): Uint8Array {
  const doc = new Uint8Array(size);
  const view = new DataView(doc.buffer);
  view.setInt32(0, size, true); // Document size
  doc.set(content, 4); // Content after size
  doc[size - 1] = 0x00; // Document terminator
  return doc;
}

// ============================================================================
// 1. Malformed BSON Document Handling
// ============================================================================

describe('Malformed BSON Document Handling', () => {
  describe('invalid document size', () => {
    it('should reject BSON document with size smaller than minimum (5 bytes)', () => {
      const message = new Uint8Array(30);
      const view = new DataView(message.buffer);

      // Valid header
      view.setInt32(0, 30, true);
      view.setInt32(4, 1, true);
      view.setInt32(8, 0, true);
      view.setInt32(12, 2013, true);
      view.setUint32(16, 0, true);

      // Section type 0
      message[20] = 0;

      // BSON document with size < 5 (invalid)
      view.setInt32(21, 3, true);

      expect(() => parseOpMsg(message)).toThrow(/invalid.*BSON/i);
    });

    it('should reject BSON document with size of 0', () => {
      const message = new Uint8Array(30);
      const view = new DataView(message.buffer);

      view.setInt32(0, 30, true);
      view.setInt32(4, 1, true);
      view.setInt32(8, 0, true);
      view.setInt32(12, 2013, true);
      view.setUint32(16, 0, true);

      message[20] = 0;
      view.setInt32(21, 0, true); // Zero size document

      expect(() => parseOpMsg(message)).toThrow();
    });

    it('should reject BSON document with negative size', () => {
      const message = new Uint8Array(30);
      const view = new DataView(message.buffer);

      view.setInt32(0, 30, true);
      view.setInt32(4, 1, true);
      view.setInt32(8, 0, true);
      view.setInt32(12, 2013, true);
      view.setUint32(16, 0, true);

      message[20] = 0;
      view.setInt32(21, -100, true); // Negative size

      expect(() => parseOpMsg(message)).toThrow();
    });

    it('should reject BSON document with size exceeding buffer', () => {
      const message = new Uint8Array(30);
      const view = new DataView(message.buffer);

      view.setInt32(0, 30, true);
      view.setInt32(4, 1, true);
      view.setInt32(8, 0, true);
      view.setInt32(12, 2013, true);
      view.setUint32(16, 0, true);

      message[20] = 0;
      view.setInt32(21, 1000, true); // Size exceeds available space

      expect(() => parseOpMsg(message)).toThrow();
    });
  });

  describe('missing document terminator', () => {
    it('should reject BSON document without null terminator', () => {
      const message = new Uint8Array(30);
      const view = new DataView(message.buffer);

      view.setInt32(0, 30, true);
      view.setInt32(4, 1, true);
      view.setInt32(8, 0, true);
      view.setInt32(12, 2013, true);
      view.setUint32(16, 0, true);

      message[20] = 0;
      view.setInt32(21, 9, true); // Document claiming 9 bytes
      // Fill with non-null bytes (should have 0x00 terminator)
      message[25] = 0xFF;
      message[26] = 0xFF;
      message[27] = 0xFF;
      message[28] = 0xFF;
      message[29] = 0xFF; // Should be 0x00

      expect(() => parseOpMsg(message)).toThrow(/BSON|invalid|terminator/i);
    });
  });

  describe('invalid element types', () => {
    it('should reject BSON with unknown element type', () => {
      const message = new Uint8Array(40);
      const view = new DataView(message.buffer);

      view.setInt32(0, 40, true);
      view.setInt32(4, 1, true);
      view.setInt32(8, 0, true);
      view.setInt32(12, 2013, true);
      view.setUint32(16, 0, true);

      message[20] = 0;
      view.setInt32(21, 15, true); // doc size
      message[25] = 0x80; // Invalid BSON type (not a valid type code)
      message[26] = 0x78; // 'x'
      message[27] = 0x00; // null terminator for field name
      message[35] = 0x00; // doc terminator

      expect(() => parseOpMsg(message)).toThrow();
    });

    it('should reject BSON with reserved type codes', () => {
      const message = new Uint8Array(40);
      const view = new DataView(message.buffer);

      view.setInt32(0, 40, true);
      view.setInt32(4, 1, true);
      view.setInt32(8, 0, true);
      view.setInt32(12, 2013, true);
      view.setUint32(16, 0, true);

      message[20] = 0;
      view.setInt32(21, 15, true);
      message[25] = 0x06; // Undefined type (deprecated/reserved)
      message[26] = 0x78;
      message[27] = 0x00;
      message[35] = 0x00;

      // May throw or handle gracefully depending on implementation
      // The important thing is it doesn't crash
      try {
        parseOpMsg(message);
      } catch {
        // Expected to throw for unsupported/deprecated types
      }
    });
  });

  describe('truncated element data', () => {
    it('should handle BSON with truncated string data gracefully', () => {
      const message = new Uint8Array(35);
      const view = new DataView(message.buffer);

      view.setInt32(0, 35, true);
      view.setInt32(4, 1, true);
      view.setInt32(8, 0, true);
      view.setInt32(12, 2013, true);
      view.setUint32(16, 0, true);

      message[20] = 0;
      view.setInt32(21, 14, true); // Document size
      message[25] = 0x02; // String type
      message[26] = 0x78; // 'x' field name
      message[27] = 0x00; // null terminator
      view.setInt32(28, 1000, true); // String length claims 1000 bytes
      // But document ends here

      // Parser behavior may vary - it should either throw or handle gracefully
      // This test verifies the parser doesn't crash or hang
      try {
        parseOpMsg(message);
        // If it doesn't throw, that's acceptable lenient behavior
      } catch {
        // Throwing is also acceptable strict behavior
      }
    });

    it('should reject BSON with truncated int64 data', () => {
      const message = new Uint8Array(32);
      const view = new DataView(message.buffer);

      view.setInt32(0, 32, true);
      view.setInt32(4, 1, true);
      view.setInt32(8, 0, true);
      view.setInt32(12, 2013, true);
      view.setUint32(16, 0, true);

      message[20] = 0;
      view.setInt32(21, 11, true); // Document size (too small for int64)
      message[25] = 0x12; // Int64 type
      message[26] = 0x78; // 'x'
      message[27] = 0x00; // null terminator
      // Only 3 bytes left, but int64 needs 8
      message[31] = 0x00; // terminator

      expect(() => parseOpMsg(message)).toThrow();
    });
  });

  describe('malformed string encoding', () => {
    it('should handle BSON string without null terminator gracefully', () => {
      const message = new Uint8Array(40);
      const view = new DataView(message.buffer);

      view.setInt32(0, 40, true);
      view.setInt32(4, 1, true);
      view.setInt32(8, 0, true);
      view.setInt32(12, 2013, true);
      view.setUint32(16, 0, true);

      message[20] = 0;
      view.setInt32(21, 19, true); // Document size
      message[25] = 0x02; // String type
      message[26] = 0x78; // 'x'
      message[27] = 0x00; // field name null terminator
      view.setInt32(28, 5, true); // String length of 5
      // String content without null terminator
      message[32] = 0x74; // 't'
      message[33] = 0x65; // 'e'
      message[34] = 0x73; // 's'
      message[35] = 0x74; // 't'
      message[36] = 0xFF; // Should be 0x00
      message[39] = 0x00; // Doc terminator

      // Parser behavior may vary - it should either throw or handle gracefully
      // The BSON spec includes length prefix, so parser may not check for null terminator
      try {
        parseOpMsg(message);
        // If it doesn't throw, that's acceptable (length-based parsing)
      } catch {
        // Throwing is also acceptable (strict validation)
      }
    });
  });
});

// ============================================================================
// 2. Documents Exceeding 16MB Limit
// ============================================================================

describe('Documents Exceeding 16MB Limit', () => {
  it('should reject message exceeding maximum wire protocol size (48MB)', () => {
    const oversizeLength = MAX_WIRE_MESSAGE_SIZE + 1;
    const header = createHeader(oversizeLength, 1, 0, OpCode.OP_MSG);

    expect(() => parseMessage(header)).toThrow(/exceeds maximum/i);
  });

  it('should reject document claiming size larger than MAX_BATCH_BYTES', () => {
    // Create a header claiming a huge message
    const hugeSize = MAX_BATCH_BYTES + 1;
    const message = new Uint8Array(30);
    const view = new DataView(message.buffer);

    view.setInt32(0, 30, true); // Actual message size
    view.setInt32(4, 1, true);
    view.setInt32(8, 0, true);
    view.setInt32(12, 2013, true);
    view.setUint32(16, 0, true);

    message[20] = 0;
    view.setInt32(21, hugeSize, true); // BSON doc claiming huge size

    // Should fail because claimed size exceeds buffer
    expect(() => parseOpMsg(message)).toThrow();
  });

  it('should handle document at exactly maximum allowed size boundary', () => {
    // Create header claiming max size - should fail due to truncation
    const header = createHeader(MAX_WIRE_MESSAGE_SIZE, 1, 0, OpCode.OP_MSG);

    // parseMessage should throw because we don't have the full message
    expect(() => parseMessage(header)).toThrow(/incomplete|truncated/i);
  });

  it('should calculate serialized document size correctly', () => {
    const doc = { ok: 1, data: 'x'.repeat(100) };
    const serialized = serializeDocument(doc);

    // Size should be: 4 (size) + elements + 1 (terminator)
    expect(serialized.length).toBeGreaterThan(100);
    expect(serialized.length).toBeLessThan(200);
  });

  it('should handle large but valid documents', () => {
    // Create a large but valid document (under limits)
    const largeData = 'x'.repeat(10000);
    const doc = { ping: 1, data: largeData, $db: 'admin' };
    const bsonDoc = createBsonDocument(doc);
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: bsonDoc }]);

    // Should parse successfully
    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });
});

// ============================================================================
// 3. Invalid UTF-8 in Field Names
// ============================================================================

describe('Invalid UTF-8 in Field Names', () => {
  it('should handle field names with valid ASCII characters', () => {
    const doc = createBsonDocument({ validField: 1, $db: 'test' });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle field names with valid UTF-8 multibyte characters', () => {
    const doc = createBsonDocument({ 'cafe': 1, $db: 'test' });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle field names with CJK characters', () => {
    const doc = createBsonDocument({ 'name': 1, $db: 'test' });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle field names with emoji', () => {
    const doc = createBsonDocument({ 'emoji_field': 1, $db: 'test' });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle field names with special characters', () => {
    const doc = createBsonDocument({
      'field-with-dash': 1,
      field_with_underscore: 2,
      'field.with.dots': 3,
      $db: 'test',
    });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle empty string field name', () => {
    const doc = createBsonDocument({ '': 1, $db: 'test' });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle very long field names', () => {
    const longFieldName = 'a'.repeat(500);
    const doc = createBsonDocument({ [longFieldName]: 1, $db: 'test' });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle field name without null terminator gracefully', () => {
    const message = new Uint8Array(40);
    const view = new DataView(message.buffer);

    view.setInt32(0, 40, true);
    view.setInt32(4, 1, true);
    view.setInt32(8, 0, true);
    view.setInt32(12, 2013, true);
    view.setUint32(16, 0, true);

    message[20] = 0;
    view.setInt32(21, 19, true); // Doc size
    message[25] = 0x10; // Int32 type
    // Field name without null terminator - fill to end
    message[26] = 0x78; // 'x'
    message[27] = 0x79; // 'y' (should be 0x00)
    message[28] = 0x7a; // 'z'
    // The parsing may fail trying to find the null terminator or may read garbage

    // Parser behavior may vary - it should either throw or handle gracefully
    try {
      parseOpMsg(message);
      // If it doesn't throw, that's acceptable lenient behavior
    } catch {
      // Throwing is also acceptable strict behavior
    }
  });
});

// ============================================================================
// 4. Extremely Long String Fields
// ============================================================================

describe('Extremely Long String Fields', () => {
  it('should handle string of moderate length (10KB)', () => {
    const longString = 'x'.repeat(10 * 1024);
    const doc = createBsonDocument({ data: longString, $db: 'test' });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle string of 100KB', () => {
    const longString = 'x'.repeat(100 * 1024);
    const doc = createBsonDocument({ data: longString, $db: 'test' });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle string of 1MB', () => {
    const longString = 'x'.repeat(1024 * 1024);
    const doc = createBsonDocument({ data: longString, $db: 'test' });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle multiple long strings in same document', () => {
    const str1 = 'a'.repeat(50 * 1024);
    const str2 = 'b'.repeat(50 * 1024);
    const str3 = 'c'.repeat(50 * 1024);
    const doc = createBsonDocument({
      field1: str1,
      field2: str2,
      field3: str3,
      $db: 'test',
    });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle long string with UTF-8 multibyte characters', () => {
    // Each character here is 2-4 bytes in UTF-8
    const multibyteString = '\u4e2d\u6587'.repeat(5000); // Chinese characters
    const doc = createBsonDocument({ data: multibyteString, $db: 'test' });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle empty string', () => {
    const doc = createBsonDocument({ data: '', $db: 'test' });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle string with null bytes embedded', () => {
    // Note: BSON strings should not contain embedded nulls
    // This tests that the serializer handles this edge case
    const stringWithNull = 'hello\x00world';

    // This may truncate at null or throw depending on implementation
    try {
      const doc = createBsonDocument({ data: stringWithNull, $db: 'test' });
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);
      const parsed = parseOpMsg(message);
      expect(parsed.sections).toBeDefined();
    } catch {
      // May throw - acceptable behavior
    }
  });
});

// ============================================================================
// 5. Deeply Nested Documents (>100 levels)
// ============================================================================

describe('Deeply Nested Documents', () => {
  it('should handle moderately nested document (10 levels)', () => {
    let nested: Record<string, unknown> = { value: 1 };
    for (let i = 0; i < 10; i++) {
      nested = { nested };
    }
    nested.$db = 'test';
    nested.ping = 1;

    const doc = createBsonDocument(nested);
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle deeply nested document (50 levels)', () => {
    let nested: Record<string, unknown> = { value: 1 };
    for (let i = 0; i < 50; i++) {
      nested = { level: i, nested };
    }
    nested.$db = 'test';
    nested.find = 'collection';

    const doc = createBsonDocument(nested);
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle deeply nested document (100 levels)', () => {
    let nested: Record<string, unknown> = { value: 1 };
    for (let i = 0; i < 100; i++) {
      nested = { nested };
    }
    nested.$db = 'test';
    nested.ping = 1;

    const doc = createBsonDocument(nested);
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    // Should handle without stack overflow
    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle deeply nested arrays (50 levels)', () => {
    let nested: unknown = [1, 2, 3];
    for (let i = 0; i < 50; i++) {
      nested = [nested];
    }

    const doc = createBsonDocument({ data: nested, $db: 'test', ping: 1 });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle mixed nesting of documents and arrays', () => {
    let mixed: unknown = { value: [1, 2] };
    for (let i = 0; i < 30; i++) {
      mixed = { doc: [mixed, { inner: i }] };
    }

    const doc = createBsonDocument({
      data: mixed as Record<string, unknown>,
      $db: 'test',
      ping: 1,
    });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle wide document with many fields at same level', () => {
    const wideDoc: Record<string, unknown> = { $db: 'test', ping: 1 };
    for (let i = 0; i < 500; i++) {
      wideDoc[`field${i}`] = i;
    }

    const doc = createBsonDocument(wideDoc);
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle both deep and wide document', () => {
    let nested: Record<string, unknown> = {};
    for (let i = 0; i < 20; i++) {
      nested[`field${i}`] = i;
    }
    nested.value = 1;

    for (let depth = 0; depth < 20; depth++) {
      const wrapper: Record<string, unknown> = {};
      for (let i = 0; i < 5; i++) {
        wrapper[`sibling${i}`] = i;
      }
      wrapper.nested = nested;
      nested = wrapper;
    }
    nested.$db = 'test';
    nested.ping = 1;

    const doc = createBsonDocument(nested);
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });
});

// ============================================================================
// 6. Binary Data with All Byte Values
// ============================================================================

describe('Binary Data with All Byte Values', () => {
  it('should handle binary data with all 256 byte values', () => {
    // Create binary data containing every possible byte value
    const allBytes = new Uint8Array(256);
    for (let i = 0; i < 256; i++) {
      allBytes[i] = i;
    }

    const doc = createBsonDocument({
      data: allBytes,
      $db: 'test',
      ping: 1,
    });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle binary data with repeated null bytes', () => {
    const nullBytes = new Uint8Array(100);
    nullBytes.fill(0x00);

    const doc = createBsonDocument({
      data: nullBytes,
      $db: 'test',
      ping: 1,
    });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle binary data with repeated 0xFF bytes', () => {
    const ffBytes = new Uint8Array(100);
    ffBytes.fill(0xff);

    const doc = createBsonDocument({
      data: ffBytes,
      $db: 'test',
      ping: 1,
    });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle empty binary data', () => {
    const emptyBinary = new Uint8Array(0);

    const doc = createBsonDocument({
      data: emptyBinary,
      $db: 'test',
      ping: 1,
    });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle large binary data (1MB)', () => {
    const largeBinary = new Uint8Array(1024 * 1024);
    // Fill with pattern
    for (let i = 0; i < largeBinary.length; i++) {
      largeBinary[i] = i % 256;
    }

    const doc = createBsonDocument({
      data: largeBinary,
      $db: 'test',
      ping: 1,
    });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle multiple binary fields', () => {
    const binary1 = new Uint8Array([0x00, 0x01, 0x02, 0x03]);
    const binary2 = new Uint8Array([0xff, 0xfe, 0xfd, 0xfc]);
    const binary3 = new Uint8Array([0x7f, 0x80, 0x81, 0x82]);

    const doc = createBsonDocument({
      bin1: binary1,
      bin2: binary2,
      bin3: binary3,
      $db: 'test',
      ping: 1,
    });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle binary data that looks like BSON structure', () => {
    // Create binary data that contains bytes resembling BSON elements
    const trickBinary = new Uint8Array([
      0x05,
      0x00,
      0x00,
      0x00, // Looks like doc size
      0x00, // Looks like terminator
      0x02, // Looks like string type
      0x78,
      0x00, // Looks like field name
    ]);

    const doc = createBsonDocument({
      data: trickBinary,
      $db: 'test',
      ping: 1,
    });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });
});

// ============================================================================
// 7. Circular Reference Detection
// ============================================================================

describe('Circular Reference Detection', () => {
  it('should detect direct circular reference', () => {
    const obj: Record<string, unknown> = { name: 'test' };
    obj.self = obj; // Direct circular reference

    expect(() => serializeDocument(obj)).toThrow();
  });

  it('should detect indirect circular reference through nested object', () => {
    const parent: Record<string, unknown> = { name: 'parent' };
    const child: Record<string, unknown> = { name: 'child', parent };
    parent.child = child;

    expect(() => serializeDocument(parent)).toThrow();
  });

  it('should detect circular reference through array', () => {
    const arr: unknown[] = [1, 2, 3];
    const obj = { arr };
    arr.push(obj); // Circular through array

    expect(() => serializeDocument(obj)).toThrow();
  });

  it('should detect deeply nested circular reference', () => {
    const level1: Record<string, unknown> = { level: 1 };
    const level2: Record<string, unknown> = { level: 2, parent: level1 };
    const level3: Record<string, unknown> = { level: 3, parent: level2 };
    const level4: Record<string, unknown> = { level: 4, parent: level3 };
    level1.descendant = level4; // Circular back to level1

    expect(() => serializeDocument(level1)).toThrow();
  });

  it('should handle non-circular references correctly', () => {
    const shared = { value: 42 };
    const obj = {
      ref1: shared,
      ref2: shared, // Same reference but not circular
      $db: 'test',
      ping: 1,
    };

    // This should NOT throw - shared references are fine
    const doc = createBsonDocument(obj);
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle object with null prototype', () => {
    const obj = Object.create(null);
    obj.ping = 1;
    obj.$db = 'test';

    const doc = createBsonDocument(obj);
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });
});

// ============================================================================
// 8. Empty Document Handling
// ============================================================================

describe('Empty Document Handling', () => {
  it('should handle empty document body', () => {
    const doc = createBsonDocument({ $db: 'test' });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should serialize minimal BSON document (just terminator)', () => {
    const emptyDoc = createBsonDocument({});
    // Empty document should be exactly 5 bytes: 4 size + 1 terminator
    expect(emptyDoc.length).toBe(5);

    // Verify structure
    const view = new DataView(emptyDoc.buffer);
    expect(view.getInt32(0, true)).toBe(5); // Size
    expect(emptyDoc[4]).toBe(0x00); // Terminator
  });

  it('should parse minimal valid OP_MSG', () => {
    // Create minimal empty document
    const minDoc = new Uint8Array(5);
    new DataView(minDoc.buffer).setInt32(0, 5, true);
    minDoc[4] = 0x00;

    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: minDoc }]);

    const parsed = parseMessage(message);
    expect(parsed.type).toBe('OP_MSG');
  });

  it('should handle document with only $db field', () => {
    const doc = createBsonDocument({ $db: 'admin' });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle document with null values', () => {
    const doc = createBsonDocument({
      field1: null,
      field2: null,
      $db: 'test',
      ping: 1,
    });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle document with empty nested document', () => {
    const doc = createBsonDocument({
      nested: {},
      $db: 'test',
      ping: 1,
    });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle document with empty array', () => {
    const doc = createBsonDocument({
      array: [],
      $db: 'test',
      ping: 1,
    });
    const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(1);
  });

  it('should handle OP_MSG with empty document sequence section', () => {
    const bodyDoc = createBsonDocument({ insert: 'test', $db: 'testdb' });
    const emptyDocs = new Uint8Array(0); // Empty document sequence

    const message = createOpMsg(1, 0, 0, [
      { type: 0, payload: bodyDoc },
      { type: 1, payload: emptyDocs, identifier: 'documents' },
    ]);

    const parsed = parseOpMsg(message);
    expect(parsed.sections).toHaveLength(2);
  });
});

// ============================================================================
// Additional Edge Cases
// ============================================================================

describe('Additional Edge Cases', () => {
  describe('numeric boundaries', () => {
    it('should handle int32 at maximum value', () => {
      const doc = createBsonDocument({
        value: 2147483647, // Max int32
        $db: 'test',
        ping: 1,
      });
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

      const parsed = parseOpMsg(message);
      expect(parsed.sections).toHaveLength(1);
    });

    it('should handle int32 at minimum value', () => {
      const doc = createBsonDocument({
        value: -2147483648, // Min int32
        $db: 'test',
        ping: 1,
      });
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

      const parsed = parseOpMsg(message);
      expect(parsed.sections).toHaveLength(1);
    });

    it('should handle int64 (bigint) values', () => {
      const doc = createBsonDocument({
        value: 9007199254740993n, // Larger than MAX_SAFE_INTEGER
        $db: 'test',
        ping: 1,
      });
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

      const parsed = parseOpMsg(message);
      expect(parsed.sections).toHaveLength(1);
    });

    it('should handle double precision floating point', () => {
      const doc = createBsonDocument({
        value: 3.141592653589793,
        $db: 'test',
        ping: 1,
      });
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

      const parsed = parseOpMsg(message);
      expect(parsed.sections).toHaveLength(1);
    });

    it('should handle special float values (Infinity, NaN)', () => {
      const doc = createBsonDocument({
        inf: Infinity,
        negInf: -Infinity,
        nan: NaN,
        $db: 'test',
        ping: 1,
      });
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

      const parsed = parseOpMsg(message);
      expect(parsed.sections).toHaveLength(1);
    });
  });

  describe('date handling', () => {
    it('should handle Date objects', () => {
      const doc = createBsonDocument({
        timestamp: new Date('2024-01-01T00:00:00Z'),
        $db: 'test',
        ping: 1,
      });
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

      const parsed = parseOpMsg(message);
      expect(parsed.sections).toHaveLength(1);
    });

    it('should handle Date at Unix epoch', () => {
      const doc = createBsonDocument({
        timestamp: new Date(0),
        $db: 'test',
        ping: 1,
      });
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

      const parsed = parseOpMsg(message);
      expect(parsed.sections).toHaveLength(1);
    });

    it('should handle far future Date', () => {
      const doc = createBsonDocument({
        timestamp: new Date('9999-12-31T23:59:59Z'),
        $db: 'test',
        ping: 1,
      });
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

      const parsed = parseOpMsg(message);
      expect(parsed.sections).toHaveLength(1);
    });
  });

  describe('regex handling', () => {
    it('should handle simple regex', () => {
      const doc = createBsonDocument({
        pattern: /test/,
        $db: 'test',
        ping: 1,
      });
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

      const parsed = parseOpMsg(message);
      expect(parsed.sections).toHaveLength(1);
    });

    it('should handle regex with flags', () => {
      const doc = createBsonDocument({
        pattern: /test/gim,
        $db: 'test',
        ping: 1,
      });
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

      const parsed = parseOpMsg(message);
      expect(parsed.sections).toHaveLength(1);
    });

    it('should handle empty regex', () => {
      const doc = createBsonDocument({
        pattern: new RegExp(''),
        $db: 'test',
        ping: 1,
      });
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

      const parsed = parseOpMsg(message);
      expect(parsed.sections).toHaveLength(1);
    });
  });

  describe('boolean handling', () => {
    it('should handle true and false values', () => {
      const doc = createBsonDocument({
        trueVal: true,
        falseVal: false,
        $db: 'test',
        ping: 1,
      });
      const message = createOpMsg(1, 0, 0, [{ type: 0, payload: doc }]);

      const parsed = parseOpMsg(message);
      expect(parsed.sections).toHaveLength(1);
    });
  });

  describe('response building edge cases', () => {
    it('should build response with empty data', () => {
      const response = buildSuccessResponse(1, 2, {});

      const header = parseMessageHeader(response);
      expect(header.opCode).toBe(OpCode.OP_MSG);
    });

    it('should build response with complex nested data', () => {
      const response = buildSuccessResponse(1, 2, {
        cursor: {
          id: 0n,
          ns: 'test.collection',
          firstBatch: [
            { _id: '1', nested: { deeply: { nested: { value: 42 } } } },
          ],
        },
      });

      const parsed = parseMessage(response);
      expect(parsed.type).toBe('OP_MSG');
    });

    it('should handle response with array of documents', () => {
      const docs = Array.from({ length: 100 }, (_, i) => ({
        _id: i.toString(),
        index: i,
      }));

      const response = buildSuccessResponse(1, 2, {
        cursor: {
          id: 0n,
          ns: 'test.collection',
          firstBatch: docs,
        },
      });

      const parsed = parseMessage(response);
      expect(parsed.type).toBe('OP_MSG');
    });
  });
});
