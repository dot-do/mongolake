/**
 * Response Encoder Tests - RED Phase (TDD)
 *
 * These tests define expected behavior for response encoding in MongoDB wire protocol.
 * Tests should initially fail until the implementation passes them.
 *
 * Tests cover:
 * 1. Successful response with cursor (firstBatch, nextBatch, id, ns)
 * 2. Error response encoding (ok:0, errmsg, code)
 * 3. Write result encoding (n, nModified, upserted)
 * 4. Header construction with correct responseTo
 *
 * Wire Protocol Reference: https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/
 */

import { describe, it, expect } from 'vitest';
import {
  buildOpMsgResponse,
  buildSuccessResponse,
  buildErrorResponse,
  buildCursorResponse,
  buildWriteResultResponse,
  buildOpReply,
  ResponseBuilder,
  MongoErrorCode,
  getErrorCodeName,
  serializeDocument,
} from '../../../src/wire-protocol/bson-serializer.js';
import {
  parseMessage,
  parseOpMsg,
  OpCode,
} from '../../../src/wire-protocol/message-parser.js';

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Parse the response document from an OP_MSG message
 */
function parseResponseDocument(response: Uint8Array): Record<string, unknown> {
  const parsed = parseOpMsg(response);
  const bodySection = parsed.sections.find((s) => s.type === 0);
  if (!bodySection) {
    throw new Error('No body section found in response');
  }

  // Parse BSON document from body section
  const view = new DataView(bodySection.payload.buffer, bodySection.payload.byteOffset);
  const docSize = view.getInt32(0, true);

  // Simple BSON parser for test verification
  const doc: Record<string, unknown> = {};
  let pos = 4; // Skip size

  const textDecoder = new TextDecoder();

  while (pos < docSize - 1) {
    const type = bodySection.payload[pos];
    pos++;

    if (type === 0x00) break;

    // Read key (C-string)
    let keyEnd = pos;
    while (bodySection.payload[keyEnd] !== 0x00) keyEnd++;
    const key = textDecoder.decode(bodySection.payload.subarray(pos, keyEnd));
    pos = keyEnd + 1;

    // Read value based on type
    const valueView = new DataView(
      bodySection.payload.buffer,
      bodySection.payload.byteOffset + pos
    );

    switch (type) {
      case 0x01: // Double
        doc[key] = valueView.getFloat64(0, true);
        pos += 8;
        break;
      case 0x02: // String
        const strLen = valueView.getInt32(0, true);
        pos += 4;
        doc[key] = textDecoder.decode(bodySection.payload.subarray(pos, pos + strLen - 1));
        pos += strLen;
        break;
      case 0x03: // Document (nested)
        const nestedDocSize = valueView.getInt32(0, true);
        // For simplicity, just extract as raw bytes reference
        doc[key] = parseNestedDocument(bodySection.payload.subarray(pos, pos + nestedDocSize));
        pos += nestedDocSize;
        break;
      case 0x04: // Array
        const arrSize = valueView.getInt32(0, true);
        doc[key] = parseNestedArray(bodySection.payload.subarray(pos, pos + arrSize));
        pos += arrSize;
        break;
      case 0x08: // Boolean
        doc[key] = bodySection.payload[pos] !== 0x00;
        pos += 1;
        break;
      case 0x0a: // Null
        doc[key] = null;
        break;
      case 0x10: // Int32
        doc[key] = valueView.getInt32(0, true);
        pos += 4;
        break;
      case 0x12: // Int64
        doc[key] = valueView.getBigInt64(0, true);
        pos += 8;
        break;
      default:
        throw new Error(`Unsupported BSON type: 0x${type.toString(16)}`);
    }
  }

  return doc;
}

/**
 * Parse a nested BSON document
 */
function parseNestedDocument(buffer: Uint8Array): Record<string, unknown> {
  const doc: Record<string, unknown> = {};
  const view = new DataView(buffer.buffer, buffer.byteOffset);
  const docSize = view.getInt32(0, true);
  let pos = 4;

  const textDecoder = new TextDecoder();

  while (pos < docSize - 1) {
    const type = buffer[pos];
    pos++;

    if (type === 0x00) break;

    // Read key
    let keyEnd = pos;
    while (buffer[keyEnd] !== 0x00) keyEnd++;
    const key = textDecoder.decode(buffer.subarray(pos, keyEnd));
    pos = keyEnd + 1;

    const valueView = new DataView(buffer.buffer, buffer.byteOffset + pos);

    switch (type) {
      case 0x01: // Double
        doc[key] = valueView.getFloat64(0, true);
        pos += 8;
        break;
      case 0x02: // String
        const strLen = valueView.getInt32(0, true);
        pos += 4;
        doc[key] = textDecoder.decode(buffer.subarray(pos, pos + strLen - 1));
        pos += strLen;
        break;
      case 0x03: // Document
        const nestedSize = valueView.getInt32(0, true);
        doc[key] = parseNestedDocument(buffer.subarray(pos, pos + nestedSize));
        pos += nestedSize;
        break;
      case 0x04: // Array
        const arrSize = valueView.getInt32(0, true);
        doc[key] = parseNestedArray(buffer.subarray(pos, pos + arrSize));
        pos += arrSize;
        break;
      case 0x08: // Boolean
        doc[key] = buffer[pos] !== 0x00;
        pos += 1;
        break;
      case 0x0a: // Null
        doc[key] = null;
        break;
      case 0x10: // Int32
        doc[key] = valueView.getInt32(0, true);
        pos += 4;
        break;
      case 0x12: // Int64
        doc[key] = valueView.getBigInt64(0, true);
        pos += 8;
        break;
      default:
        // Skip unknown types for simplicity
        break;
    }
  }

  return doc;
}

/**
 * Parse a BSON array
 */
function parseNestedArray(buffer: Uint8Array): unknown[] {
  const docObj = parseNestedDocument(buffer);
  // Convert object with numeric keys to array
  const keys = Object.keys(docObj)
    .map(Number)
    .filter((k) => !isNaN(k))
    .sort((a, b) => a - b);
  return keys.map((k) => docObj[k.toString()]);
}

/**
 * Extract header fields from a wire protocol message
 */
function extractHeader(message: Uint8Array): {
  messageLength: number;
  requestId: number;
  responseTo: number;
  opCode: number;
} {
  const view = new DataView(message.buffer, message.byteOffset);
  return {
    messageLength: view.getInt32(0, true),
    requestId: view.getInt32(4, true),
    responseTo: view.getInt32(8, true),
    opCode: view.getInt32(12, true),
  };
}

// ============================================================================
// Successful Response with Cursor Tests
// ============================================================================

describe('Cursor Response Encoding', () => {
  describe('firstBatch response', () => {
    it('should encode cursor response with firstBatch', () => {
      const documents = [
        { _id: '1', name: 'Alice', age: 30 },
        { _id: '2', name: 'Bob', age: 25 },
      ];
      const cursorId = 12345n;
      const namespace = 'testdb.users';

      const response = buildCursorResponse(
        100, // requestId
        50, // responseTo
        cursorId,
        namespace,
        documents,
        'firstBatch'
      );

      const doc = parseResponseDocument(response);

      // Verify ok:1
      expect(doc.ok).toBe(1);

      // Verify cursor structure
      expect(doc.cursor).toBeDefined();
      const cursor = doc.cursor as Record<string, unknown>;

      // Verify cursor.id
      expect(cursor.id).toBe(cursorId);

      // Verify cursor.ns
      expect(cursor.ns).toBe(namespace);

      // Verify cursor.firstBatch
      expect(cursor.firstBatch).toBeDefined();
      expect(Array.isArray(cursor.firstBatch)).toBe(true);
      const firstBatch = cursor.firstBatch as unknown[];
      expect(firstBatch).toHaveLength(2);
    });

    it('should encode cursor response with empty firstBatch', () => {
      const response = buildCursorResponse(
        100,
        50,
        0n,
        'testdb.users',
        [],
        'firstBatch'
      );

      const doc = parseResponseDocument(response);

      expect(doc.ok).toBe(1);
      const cursor = doc.cursor as Record<string, unknown>;
      expect(cursor.id).toBe(0n);
      expect(cursor.firstBatch).toEqual([]);
    });

    it('should encode cursor with zero ID indicating exhausted cursor', () => {
      const response = buildCursorResponse(
        100,
        50,
        0n, // Zero means cursor exhausted
        'testdb.users',
        [{ _id: '1' }],
        'firstBatch'
      );

      const doc = parseResponseDocument(response);

      const cursor = doc.cursor as Record<string, unknown>;
      expect(cursor.id).toBe(0n);
    });

    it('should encode cursor with large cursor ID', () => {
      const largeCursorId = BigInt('9007199254740993'); // Larger than MAX_SAFE_INTEGER

      const response = buildCursorResponse(
        100,
        50,
        largeCursorId,
        'testdb.users',
        [],
        'firstBatch'
      );

      const doc = parseResponseDocument(response);

      const cursor = doc.cursor as Record<string, unknown>;
      expect(cursor.id).toBe(largeCursorId);
    });
  });

  describe('nextBatch response', () => {
    it('should encode cursor response with nextBatch', () => {
      const documents = [
        { _id: '3', name: 'Charlie' },
        { _id: '4', name: 'Diana' },
      ];
      const cursorId = 67890n;
      const namespace = 'testdb.users';

      const response = buildCursorResponse(
        101,
        100,
        cursorId,
        namespace,
        documents,
        'nextBatch'
      );

      const doc = parseResponseDocument(response);

      expect(doc.ok).toBe(1);

      const cursor = doc.cursor as Record<string, unknown>;
      expect(cursor.id).toBe(cursorId);
      expect(cursor.ns).toBe(namespace);
      expect(cursor.nextBatch).toBeDefined();
      expect(Array.isArray(cursor.nextBatch)).toBe(true);
      // firstBatch should not be present in nextBatch response
      expect(cursor.firstBatch).toBeUndefined();
    });

    it('should encode final nextBatch with cursor ID 0', () => {
      const response = buildCursorResponse(
        102,
        101,
        0n, // Final batch
        'testdb.users',
        [{ _id: '5', name: 'Eve' }],
        'nextBatch'
      );

      const doc = parseResponseDocument(response);

      const cursor = doc.cursor as Record<string, unknown>;
      expect(cursor.id).toBe(0n);
      expect(cursor.nextBatch).toBeDefined();
    });

    it('should encode getMore continuation response', () => {
      // Simulates getMore response with more data available
      const continuationCursorId = 99999n;

      const response = buildCursorResponse(
        103,
        102,
        continuationCursorId,
        'testdb.largecollection',
        Array.from({ length: 100 }, (_, i) => ({ _id: i.toString(), index: i })),
        'nextBatch'
      );

      const doc = parseResponseDocument(response);

      const cursor = doc.cursor as Record<string, unknown>;
      expect(cursor.id).toBe(continuationCursorId);
      const nextBatch = cursor.nextBatch as unknown[];
      expect(nextBatch).toHaveLength(100);
    });
  });

  describe('cursor namespace', () => {
    it('should encode namespace in database.collection format', () => {
      const response = buildCursorResponse(
        100,
        50,
        0n,
        'mydb.mycollection',
        [],
        'firstBatch'
      );

      const doc = parseResponseDocument(response);

      const cursor = doc.cursor as Record<string, unknown>;
      expect(cursor.ns).toBe('mydb.mycollection');
    });

    it('should handle namespace with dots in collection name', () => {
      const response = buildCursorResponse(
        100,
        50,
        0n,
        'mydb.my.dotted.collection',
        [],
        'firstBatch'
      );

      const doc = parseResponseDocument(response);

      const cursor = doc.cursor as Record<string, unknown>;
      expect(cursor.ns).toBe('mydb.my.dotted.collection');
    });

    it('should handle special characters in namespace', () => {
      const response = buildCursorResponse(
        100,
        50,
        0n,
        'testdb.users_archive-2024',
        [],
        'firstBatch'
      );

      const doc = parseResponseDocument(response);

      const cursor = doc.cursor as Record<string, unknown>;
      expect(cursor.ns).toBe('testdb.users_archive-2024');
    });
  });

  describe('ResponseBuilder cursor API', () => {
    it('should build cursor response using fluent builder', () => {
      const documents = [{ _id: '1', name: 'Test' }];

      const response = ResponseBuilder.create(100, 50)
        .success()
        .withCursor(12345n, 'testdb.users', documents, 'firstBatch')
        .build();

      const doc = parseResponseDocument(response);

      expect(doc.ok).toBe(1);
      const cursor = doc.cursor as Record<string, unknown>;
      expect(cursor.id).toBe(12345n);
      expect(cursor.ns).toBe('testdb.users');
      expect(cursor.firstBatch).toBeDefined();
    });

    it('should build nextBatch response using fluent builder', () => {
      const response = ResponseBuilder.create(101, 100)
        .success()
        .withCursor(12345n, 'testdb.users', [{ _id: '2' }], 'nextBatch')
        .build();

      const doc = parseResponseDocument(response);

      const cursor = doc.cursor as Record<string, unknown>;
      expect(cursor.nextBatch).toBeDefined();
    });
  });
});

// ============================================================================
// Error Response Encoding Tests
// ============================================================================

describe('Error Response Encoding', () => {
  describe('basic error structure', () => {
    it('should encode error response with ok:0', () => {
      const response = buildErrorResponse(
        100,
        50,
        MongoErrorCode.CursorNotFound,
        'cursor id 12345 not found'
      );

      const doc = parseResponseDocument(response);

      expect(doc.ok).toBe(0);
    });

    it('should encode error message in errmsg field', () => {
      const errorMessage = 'The specified namespace testdb.users does not exist';

      const response = buildErrorResponse(
        100,
        50,
        MongoErrorCode.NamespaceNotFound,
        errorMessage
      );

      const doc = parseResponseDocument(response);

      expect(doc.errmsg).toBe(errorMessage);
    });

    it('should encode error code as number', () => {
      const response = buildErrorResponse(
        100,
        50,
        MongoErrorCode.Unauthorized,
        'not authorized'
      );

      const doc = parseResponseDocument(response);

      expect(doc.code).toBe(MongoErrorCode.Unauthorized);
      expect(typeof doc.code).toBe('number');
    });

    it('should encode codeName string', () => {
      const response = buildErrorResponse(
        100,
        50,
        MongoErrorCode.DuplicateKey,
        'duplicate key error'
      );

      const doc = parseResponseDocument(response);

      expect(doc.codeName).toBe('DuplicateKey');
    });

    it('should allow custom codeName', () => {
      const response = buildErrorResponse(
        100,
        50,
        999,
        'custom error',
        'CustomErrorCode'
      );

      const doc = parseResponseDocument(response);

      expect(doc.code).toBe(999);
      expect(doc.codeName).toBe('CustomErrorCode');
    });
  });

  describe('common error codes', () => {
    it('should encode CursorNotFound error (code 43)', () => {
      const response = buildErrorResponse(
        100,
        50,
        MongoErrorCode.CursorNotFound,
        'cursor id 999 not found'
      );

      const doc = parseResponseDocument(response);

      expect(doc.ok).toBe(0);
      expect(doc.code).toBe(43);
      expect(doc.codeName).toBe('CursorNotFound');
    });

    it('should encode NamespaceNotFound error (code 26)', () => {
      const response = buildErrorResponse(
        100,
        50,
        MongoErrorCode.NamespaceNotFound,
        'ns not found'
      );

      const doc = parseResponseDocument(response);

      expect(doc.code).toBe(26);
      expect(doc.codeName).toBe('NamespaceNotFound');
    });

    it('should encode CommandNotFound error (code 59)', () => {
      const response = buildErrorResponse(
        100,
        50,
        MongoErrorCode.CommandNotFound,
        'no such command: unknownCmd'
      );

      const doc = parseResponseDocument(response);

      expect(doc.code).toBe(59);
      expect(doc.codeName).toBe('CommandNotFound');
    });

    it('should encode Unauthorized error (code 13)', () => {
      const response = buildErrorResponse(
        100,
        50,
        MongoErrorCode.Unauthorized,
        'not authorized on testdb to execute command'
      );

      const doc = parseResponseDocument(response);

      expect(doc.code).toBe(13);
      expect(doc.codeName).toBe('Unauthorized');
    });

    it('should encode DuplicateKey error (code 11000)', () => {
      const response = buildErrorResponse(
        100,
        50,
        MongoErrorCode.DuplicateKey,
        'E11000 duplicate key error collection: testdb.users index: email_1 dup key: { email: "test@example.com" }'
      );

      const doc = parseResponseDocument(response);

      expect(doc.code).toBe(11000);
      expect(doc.codeName).toBe('DuplicateKey');
    });

    it('should encode AuthenticationFailed error (code 18)', () => {
      const response = buildErrorResponse(
        100,
        50,
        MongoErrorCode.AuthenticationFailed,
        'Authentication failed.'
      );

      const doc = parseResponseDocument(response);

      expect(doc.code).toBe(18);
      expect(doc.codeName).toBe('AuthenticationFailed');
    });
  });

  describe('getErrorCodeName utility', () => {
    it('should return correct name for known error codes', () => {
      expect(getErrorCodeName(MongoErrorCode.CursorNotFound)).toBe('CursorNotFound');
      expect(getErrorCodeName(MongoErrorCode.NamespaceNotFound)).toBe('NamespaceNotFound');
      expect(getErrorCodeName(MongoErrorCode.DuplicateKey)).toBe('DuplicateKey');
      expect(getErrorCodeName(MongoErrorCode.Unauthorized)).toBe('Unauthorized');
    });

    it('should return UnknownError for unrecognized codes', () => {
      expect(getErrorCodeName(99999)).toBe('UnknownError');
    });
  });

  describe('ResponseBuilder error API', () => {
    it('should build error response using fluent builder', () => {
      const response = ResponseBuilder.create(100, 50)
        .error(MongoErrorCode.CursorNotFound, 'cursor not found')
        .build();

      const doc = parseResponseDocument(response);

      expect(doc.ok).toBe(0);
      expect(doc.code).toBe(43);
      expect(doc.errmsg).toBe('cursor not found');
    });

    it('should allow custom codeName in builder', () => {
      const response = ResponseBuilder.create(100, 50)
        .error(500, 'internal error', 'InternalServerError')
        .build();

      const doc = parseResponseDocument(response);

      expect(doc.codeName).toBe('InternalServerError');
    });

    it('should allow additional data in error response', () => {
      const response = ResponseBuilder.create(100, 50)
        .error(MongoErrorCode.DuplicateKey, 'duplicate key')
        .withData({
          keyPattern: { email: 1 },
          keyValue: { email: 'test@example.com' },
        })
        .build();

      const doc = parseResponseDocument(response);

      expect(doc.ok).toBe(0);
      expect(doc.keyPattern).toBeDefined();
      expect(doc.keyValue).toBeDefined();
    });
  });
});

// ============================================================================
// Write Result Encoding Tests
// ============================================================================

describe('Write Result Encoding', () => {
  describe('insert result', () => {
    it('should encode n field for inserted document count', () => {
      const response = buildWriteResultResponse(100, 50, 5);

      const doc = parseResponseDocument(response);

      expect(doc.ok).toBe(1);
      expect(doc.n).toBe(5);
    });

    it('should encode n=1 for single insert', () => {
      const response = buildWriteResultResponse(100, 50, 1);

      const doc = parseResponseDocument(response);

      expect(doc.n).toBe(1);
    });

    it('should encode n=0 when no documents inserted', () => {
      const response = buildWriteResultResponse(100, 50, 0);

      const doc = parseResponseDocument(response);

      expect(doc.n).toBe(0);
    });

    it('should encode large n value for bulk insert', () => {
      const response = buildWriteResultResponse(100, 50, 10000);

      const doc = parseResponseDocument(response);

      expect(doc.n).toBe(10000);
    });
  });

  describe('update result', () => {
    it('should encode nModified for update operations', () => {
      const response = buildWriteResultResponse(100, 50, 10, { nModified: 7 });

      const doc = parseResponseDocument(response);

      expect(doc.ok).toBe(1);
      expect(doc.n).toBe(10); // Documents matched
      expect(doc.nModified).toBe(7); // Documents actually modified
    });

    it('should encode n > nModified when some docs match but not modified', () => {
      // e.g., updateMany where some docs already have the new value
      const response = buildWriteResultResponse(100, 50, 100, { nModified: 50 });

      const doc = parseResponseDocument(response);

      expect(doc.n).toBe(100);
      expect(doc.nModified).toBe(50);
    });

    it('should encode nModified=0 when no modifications made', () => {
      const response = buildWriteResultResponse(100, 50, 5, { nModified: 0 });

      const doc = parseResponseDocument(response);

      expect(doc.n).toBe(5);
      expect(doc.nModified).toBe(0);
    });

    it('should omit nModified when not provided', () => {
      const response = buildWriteResultResponse(100, 50, 5);

      const doc = parseResponseDocument(response);

      expect(doc.nModified).toBeUndefined();
    });
  });

  describe('upsert result', () => {
    it('should encode upserted array when document was upserted', () => {
      const upsertedId = '507f1f77bcf86cd799439011';
      const response = buildWriteResultResponse(100, 50, 1, {
        upserted: [{ index: 0, _id: upsertedId }],
      });

      const doc = parseResponseDocument(response);

      expect(doc.ok).toBe(1);
      expect(doc.n).toBe(1);
      expect(doc.upserted).toBeDefined();
      expect(Array.isArray(doc.upserted)).toBe(true);

      const upserted = doc.upserted as Array<{ index: number; _id: string }>;
      expect(upserted).toHaveLength(1);
      expect(upserted[0].index).toBe(0);
      expect(upserted[0]._id).toBe(upsertedId);
    });

    it('should encode multiple upserted documents for bulk upsert', () => {
      const response = buildWriteResultResponse(100, 50, 3, {
        upserted: [
          { index: 0, _id: 'id1' },
          { index: 2, _id: 'id2' },
          { index: 5, _id: 'id3' },
        ],
      });

      const doc = parseResponseDocument(response);

      const upserted = doc.upserted as Array<{ index: number; _id: string }>;
      expect(upserted).toHaveLength(3);
      expect(upserted[0].index).toBe(0);
      expect(upserted[1].index).toBe(2);
      expect(upserted[2].index).toBe(5);
    });

    it('should omit upserted when empty array provided', () => {
      const response = buildWriteResultResponse(100, 50, 1, {
        upserted: [],
      });

      const doc = parseResponseDocument(response);

      expect(doc.upserted).toBeUndefined();
    });

    it('should omit upserted when not provided', () => {
      const response = buildWriteResultResponse(100, 50, 1);

      const doc = parseResponseDocument(response);

      expect(doc.upserted).toBeUndefined();
    });
  });

  describe('combined write result', () => {
    it('should encode complete update with upsert result', () => {
      const response = buildWriteResultResponse(100, 50, 1, {
        nModified: 0,
        upserted: [{ index: 0, _id: 'newId' }],
      });

      const doc = parseResponseDocument(response);

      expect(doc.ok).toBe(1);
      expect(doc.n).toBe(1);
      expect(doc.nModified).toBe(0);
      expect(doc.upserted).toBeDefined();
    });
  });

  describe('delete result', () => {
    it('should encode n for deleted document count', () => {
      const response = buildWriteResultResponse(100, 50, 10);

      const doc = parseResponseDocument(response);

      expect(doc.ok).toBe(1);
      expect(doc.n).toBe(10);
    });

    it('should encode n=0 when no documents deleted', () => {
      const response = buildWriteResultResponse(100, 50, 0);

      const doc = parseResponseDocument(response);

      expect(doc.n).toBe(0);
    });
  });

  describe('ResponseBuilder write result API', () => {
    it('should build write result using fluent builder', () => {
      const response = ResponseBuilder.create(100, 50)
        .success()
        .withWriteResult(5)
        .build();

      const doc = parseResponseDocument(response);

      expect(doc.ok).toBe(1);
      expect(doc.n).toBe(5);
    });

    it('should build update result with nModified', () => {
      const response = ResponseBuilder.create(100, 50)
        .success()
        .withWriteResult(10, { nModified: 7 })
        .build();

      const doc = parseResponseDocument(response);

      expect(doc.n).toBe(10);
      expect(doc.nModified).toBe(7);
    });

    it('should build upsert result using fluent builder', () => {
      const response = ResponseBuilder.create(100, 50)
        .success()
        .withWriteResult(1, { upserted: [{ index: 0, _id: 'newId' }] })
        .build();

      const doc = parseResponseDocument(response);

      expect(doc.upserted).toBeDefined();
    });
  });
});

// ============================================================================
// Header Construction Tests
// ============================================================================

describe('Header Construction', () => {
  describe('responseTo field', () => {
    it('should set correct responseTo matching request', () => {
      const requestId = 12345;
      const response = buildOpMsgResponse(100, requestId, { ok: 1 });

      const header = extractHeader(response);

      expect(header.responseTo).toBe(requestId);
    });

    it('should set responseTo to 0 for unprompted message', () => {
      const response = buildOpMsgResponse(100, 0, { ok: 1 });

      const header = extractHeader(response);

      expect(header.responseTo).toBe(0);
    });

    it('should handle large responseTo values', () => {
      const largeRequestId = 2147483647; // Max int32

      const response = buildOpMsgResponse(100, largeRequestId, { ok: 1 });

      const header = extractHeader(response);

      expect(header.responseTo).toBe(largeRequestId);
    });

    it('should set unique requestId for response', () => {
      const myRequestId = 999;
      const response = buildOpMsgResponse(myRequestId, 123, { ok: 1 });

      const header = extractHeader(response);

      expect(header.requestId).toBe(myRequestId);
    });
  });

  describe('messageLength field', () => {
    it('should calculate correct message length', () => {
      const response = buildOpMsgResponse(100, 50, { ok: 1 });

      const header = extractHeader(response);

      expect(header.messageLength).toBe(response.length);
    });

    it('should update message length for larger documents', () => {
      const smallResponse = buildOpMsgResponse(100, 50, { ok: 1 });
      const largeResponse = buildOpMsgResponse(100, 50, {
        ok: 1,
        data: 'x'.repeat(1000),
      });

      const smallHeader = extractHeader(smallResponse);
      const largeHeader = extractHeader(largeResponse);

      expect(largeHeader.messageLength).toBeGreaterThan(smallHeader.messageLength);
      expect(largeHeader.messageLength).toBe(largeResponse.length);
    });
  });

  describe('opCode field', () => {
    it('should set OP_MSG opcode (2013) for buildOpMsgResponse', () => {
      const response = buildOpMsgResponse(100, 50, { ok: 1 });

      const header = extractHeader(response);

      expect(header.opCode).toBe(OpCode.OP_MSG);
      expect(header.opCode).toBe(2013);
    });

    it('should set OP_REPLY opcode (1) for buildOpReply', () => {
      const response = buildOpReply(100, 50, [{ ok: 1 }]);

      const header = extractHeader(response);

      expect(header.opCode).toBe(OpCode.OP_REPLY);
      expect(header.opCode).toBe(1);
    });
  });

  describe('requestId and responseTo correlation', () => {
    it('should allow request/response matching via responseTo', () => {
      // Simulate a request
      const requestRequestId = 1001;

      // Build response with responseTo matching request
      const responseRequestId = 1002;
      const response = buildOpMsgResponse(responseRequestId, requestRequestId, { ok: 1 });

      const header = extractHeader(response);

      // Response's responseTo should match the request's requestId
      expect(header.responseTo).toBe(requestRequestId);
      // Response has its own unique requestId
      expect(header.requestId).toBe(responseRequestId);
    });

    it('should maintain correlation through multiple request/response pairs', () => {
      // First request/response
      const request1Id = 100;
      const response1 = buildOpMsgResponse(101, request1Id, { ok: 1 });

      // Second request/response
      const request2Id = 102;
      const response2 = buildOpMsgResponse(103, request2Id, { ok: 1 });

      const header1 = extractHeader(response1);
      const header2 = extractHeader(response2);

      expect(header1.responseTo).toBe(request1Id);
      expect(header2.responseTo).toBe(request2Id);
    });
  });

  describe('header byte layout', () => {
    it('should have correct header structure (16 bytes)', () => {
      const response = buildOpMsgResponse(100, 50, { ok: 1 });

      // Header is exactly 16 bytes
      expect(response.length).toBeGreaterThanOrEqual(16);

      const view = new DataView(response.buffer, response.byteOffset);

      // Verify little-endian layout
      const messageLength = view.getInt32(0, true);
      const requestId = view.getInt32(4, true);
      const responseTo = view.getInt32(8, true);
      const opCode = view.getInt32(12, true);

      expect(messageLength).toBe(response.length);
      expect(requestId).toBe(100);
      expect(responseTo).toBe(50);
      expect(opCode).toBe(OpCode.OP_MSG);
    });
  });

  describe('ResponseBuilder header handling', () => {
    it('should set correct requestId and responseTo via builder', () => {
      const response = ResponseBuilder.create(200, 150)
        .success()
        .build();

      const header = extractHeader(response);

      expect(header.requestId).toBe(200);
      expect(header.responseTo).toBe(150);
    });
  });
});

// ============================================================================
// Success Response Tests
// ============================================================================

describe('Success Response Encoding', () => {
  it('should encode basic success response with ok:1', () => {
    const response = buildSuccessResponse(100, 50);

    const doc = parseResponseDocument(response);

    expect(doc.ok).toBe(1);
  });

  it('should encode success response with additional data', () => {
    const response = buildSuccessResponse(100, 50, {
      ismaster: true,
      maxBsonObjectSize: 16777216,
      localTime: '2024-01-15T10:30:00Z',
    });

    const doc = parseResponseDocument(response);

    expect(doc.ok).toBe(1);
    expect(doc.ismaster).toBe(true);
    expect(doc.maxBsonObjectSize).toBe(16777216);
  });

  it('should not override ok field even if provided in data', () => {
    // Caller should not be able to set ok:0 via data param
    const response = buildSuccessResponse(100, 50, { ok: 0 } as Record<string, unknown>);

    const doc = parseResponseDocument(response);

    // ok should still be 1 (success response always has ok:1)
    expect(doc.ok).toBe(1);
  });
});

// ============================================================================
// OP_REPLY Legacy Response Tests
// ============================================================================

describe('OP_REPLY Encoding', () => {
  it('should encode OP_REPLY with single document', () => {
    const response = buildOpReply(100, 50, [{ ok: 1, message: 'hello' }]);

    const header = extractHeader(response);

    expect(header.opCode).toBe(OpCode.OP_REPLY);
    expect(header.responseTo).toBe(50);
  });

  it('should encode OP_REPLY with multiple documents', () => {
    const docs = [
      { _id: '1', name: 'Alice' },
      { _id: '2', name: 'Bob' },
      { _id: '3', name: 'Charlie' },
    ];

    const response = buildOpReply(100, 50, docs);

    // Verify numberReturned field in OP_REPLY body
    const view = new DataView(response.buffer, response.byteOffset);

    // OP_REPLY structure after header:
    // - 4 bytes: response flags
    // - 8 bytes: cursor ID
    // - 4 bytes: starting from
    // - 4 bytes: number returned
    const numberReturned = view.getInt32(32, true);

    expect(numberReturned).toBe(3);
  });

  it('should encode OP_REPLY with cursor ID', () => {
    const cursorId = 12345n;

    const response = buildOpReply(100, 50, [{ ok: 1 }], 0, cursorId);

    const view = new DataView(response.buffer, response.byteOffset);

    // Cursor ID is at offset 20 (16 header + 4 flags)
    const responseCursorId = view.getBigInt64(20, true);

    expect(responseCursorId).toBe(cursorId);
  });

  it('should encode OP_REPLY with starting from', () => {
    const startingFrom = 100;

    const response = buildOpReply(100, 50, [{ ok: 1 }], 0, 0n, startingFrom);

    const view = new DataView(response.buffer, response.byteOffset);

    // startingFrom is at offset 28 (16 header + 4 flags + 8 cursorId)
    const responseStartingFrom = view.getInt32(28, true);

    expect(responseStartingFrom).toBe(startingFrom);
  });

  it('should encode OP_REPLY with flags', () => {
    const flags = 0x02; // QueryFailure flag

    const response = buildOpReply(100, 50, [{ ok: 0, errmsg: 'error' }], flags);

    const view = new DataView(response.buffer, response.byteOffset);

    // Flags at offset 16 (right after header)
    const responseFlags = view.getUint32(16, true);

    expect(responseFlags).toBe(flags);
  });
});

// ============================================================================
// Document Encoding Edge Cases
// ============================================================================

describe('Document Encoding Edge Cases', () => {
  it('should encode documents with nested objects', () => {
    const response = buildSuccessResponse(100, 50, {
      cursor: {
        id: 0n,
        ns: 'test.coll',
        firstBatch: [
          {
            _id: '1',
            address: {
              street: '123 Main St',
              city: 'New York',
              geo: { lat: 40.7128, lng: -74.006 },
            },
          },
        ],
      },
    });

    const doc = parseResponseDocument(response);

    expect(doc.cursor).toBeDefined();
  });

  it('should encode documents with arrays', () => {
    const response = buildSuccessResponse(100, 50, {
      cursor: {
        id: 0n,
        ns: 'test.coll',
        firstBatch: [
          {
            _id: '1',
            tags: ['mongodb', 'database', 'nosql'],
            scores: [85, 90, 92],
          },
        ],
      },
    });

    const doc = parseResponseDocument(response);

    expect(doc.cursor).toBeDefined();
  });

  it('should encode documents with null values', () => {
    const response = buildSuccessResponse(100, 50, {
      result: null,
      error: null,
    });

    const doc = parseResponseDocument(response);

    expect(doc.result).toBeNull();
    expect(doc.error).toBeNull();
  });

  it('should encode documents with boolean values', () => {
    const response = buildSuccessResponse(100, 50, {
      isReplicaSet: true,
      isSecondary: false,
    });

    const doc = parseResponseDocument(response);

    expect(doc.isReplicaSet).toBe(true);
    expect(doc.isSecondary).toBe(false);
  });

  it('should encode documents with integer values', () => {
    const response = buildSuccessResponse(100, 50, {
      count: 42,
      maxSize: 16777216,
    });

    const doc = parseResponseDocument(response);

    expect(doc.count).toBe(42);
    expect(doc.maxSize).toBe(16777216);
  });

  it('should encode documents with bigint (int64) values', () => {
    const response = buildSuccessResponse(100, 50, {
      cursorId: 9007199254740993n, // Larger than MAX_SAFE_INTEGER
    });

    const doc = parseResponseDocument(response);

    expect(doc.cursorId).toBe(9007199254740993n);
  });

  it('should encode documents with string values', () => {
    const response = buildSuccessResponse(100, 50, {
      message: 'Hello, World!',
      namespace: 'testdb.users',
    });

    const doc = parseResponseDocument(response);

    expect(doc.message).toBe('Hello, World!');
    expect(doc.namespace).toBe('testdb.users');
  });

  it('should encode documents with unicode strings', () => {
    const response = buildSuccessResponse(100, 50, {
      greeting: 'Cafe',
      message: 'Hello World',
    });

    const doc = parseResponseDocument(response);

    expect(doc.greeting).toBeDefined();
    expect(doc.message).toBeDefined();
  });

  it('should encode empty documents', () => {
    const response = buildSuccessResponse(100, 50, {});

    const doc = parseResponseDocument(response);

    expect(doc.ok).toBe(1);
  });
});

// ============================================================================
// Round-trip Tests (encode then parse)
// ============================================================================

describe('Round-trip Encoding', () => {
  it('should produce parseable OP_MSG response', () => {
    const response = buildOpMsgResponse(100, 50, { ok: 1, test: 'value' });

    // Should not throw
    const parsed = parseMessage(response);

    expect(parsed.type).toBe('OP_MSG');
    expect(parsed.header.requestId).toBe(100);
    expect(parsed.header.responseTo).toBe(50);
  });

  it('should produce parseable cursor response', () => {
    const response = buildCursorResponse(
      100,
      50,
      12345n,
      'testdb.users',
      [{ _id: '1', name: 'Test' }],
      'firstBatch'
    );

    const parsed = parseMessage(response);

    expect(parsed.type).toBe('OP_MSG');
  });

  it('should produce parseable error response', () => {
    const response = buildErrorResponse(
      100,
      50,
      MongoErrorCode.CursorNotFound,
      'cursor not found'
    );

    const parsed = parseMessage(response);

    expect(parsed.type).toBe('OP_MSG');
  });

  it('should produce parseable write result response', () => {
    const response = buildWriteResultResponse(100, 50, 5, { nModified: 3 });

    const parsed = parseMessage(response);

    expect(parsed.type).toBe('OP_MSG');
  });
});
